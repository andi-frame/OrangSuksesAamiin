package mapping

import (
	"encoding/json"
	"math"
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
)

// Sensor Pantai (M2 Problem 5, Lampiran C) -> HazardEvent COASTAL.
//
// The sensor is a black box that lies, so every observation is checked against
// the base contract before anything is stored. The checks are about the
// contract only (types, ranges, references, time order), never about ids,
// seeds or the order of a practice profile:
//
//	observation_id       non-empty string
//	station_id           string, one of GET /v1/stations
//	observed_at          RFC 3339, not after the response's generated_at (+ tolerance)
//	sea_level_anomaly_m  finite number, |x| <= 20 m
//	wave_height_m        finite number, 0 <= x <= 30 m
//	quality              string; only "ok" is stored, any other value is the
//	                     sensor itself saying the reading is not trustworthy
//	critical             boolean
//
// A violation is a *MappingError: the poller sets the observation aside in the
// dead-letter store with that reason; it never becomes a HazardEvent.
//
// Mapping:
//
//	source_ref_id  observation_id (an observation is immutable)
//	area_name      province of the station (same naming as the other sources)
//	lat/lon        position of the station
//	occurred_at    observed_at
//	severity       critical -> AWAS; else |sea level anomaly| >= 1.0 m or wave >= 4.0 m -> SIAGA;
//	               >= 0.5 m or >= 2.5 m -> WASPADA; otherwise NORMAL
//	attributes     station_id, station_name, sea_level_anomaly_m, wave_height_m, quality, critical

// Station is one entry of GET /v1/stations.
type Station struct {
	StationID string  `json:"station_id"`
	Name      string  `json:"name"`
	Province  string  `json:"province"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// CoastalLimits bound what a plausible observation looks like.
const (
	MaxSeaLevelAnomalyM = 20.0
	MaxWaveHeightM      = 30.0
	// FutureTolerance: an observation may be stamped slightly after
	// generated_at (clock granularity), not more.
	FutureTolerance = time.Minute
)

// ErrUnknownStation is the Field of a MappingError for a station the sensor
// did not list (retryable: the station list may simply be out of date).
const ErrUnknownStation = "station_id"

// Coastal validates one observation and maps it. generatedAt is the response's
// generated_at (zero = unknown, the time-order check is skipped).
func Coastal(raw map[string]any, stations map[string]Station, generatedAt time.Time) (*domain.HazardEvent, error) {
	id, err := reqString(raw, "observation_id")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(id) == "" || len(id) > 200 {
		return nil, bad("observation_id", "empty or longer than 200 characters")
	}
	stID, err := reqString(raw, "station_id")
	if err != nil {
		return nil, err
	}
	st, ok := stations[stID]
	if !ok {
		return nil, bad(ErrUnknownStation, "unknown station %q (not in GET /v1/stations)", stID)
	}
	obsAtS, err := reqString(raw, "observed_at")
	if err != nil {
		return nil, err
	}
	obsAt, err := time.Parse(time.RFC3339Nano, obsAtS)
	if err != nil {
		return nil, bad("observed_at", "not RFC 3339: %q", obsAtS)
	}
	obsAt = obsAt.UTC()
	if !generatedAt.IsZero() && obsAt.After(generatedAt.Add(FutureTolerance)) {
		return nil, bad("observed_at", "%s is after generated_at %s", obsAt.Format(time.RFC3339), generatedAt.Format(time.RFC3339))
	}
	sea, err := finiteNumber(raw, "sea_level_anomaly_m")
	if err != nil {
		return nil, err
	}
	if math.Abs(sea) > MaxSeaLevelAnomalyM {
		return nil, bad("sea_level_anomaly_m", "%g m is outside +/-%g m", sea, MaxSeaLevelAnomalyM)
	}
	wave, err := finiteNumber(raw, "wave_height_m")
	if err != nil {
		return nil, err
	}
	if wave < 0 || wave > MaxWaveHeightM {
		return nil, bad("wave_height_m", "%g m is outside 0..%g m", wave, MaxWaveHeightM)
	}
	quality, err := reqString(raw, "quality")
	if err != nil {
		return nil, err
	}
	if quality != "ok" {
		return nil, bad("quality", "sensor reports quality %q (only \"ok\" readings are stored)", quality)
	}
	critV, ok := raw["critical"]
	if !ok || critV == nil {
		return nil, bad("critical", "missing")
	}
	critical, ok := critV.(bool)
	if !ok {
		return nil, bad("critical", "must be a boolean, got %T", critV)
	}

	lat, lon := st.Latitude, st.Longitude
	return &domain.HazardEvent{
		HazardID:    domain.HazardID(domain.SourceCoastal, id),
		Source:      domain.SourceCoastal,
		SourceRefID: id,
		HazardType:  domain.HazardCoastal,
		Severity:    CoastalSeverity(critical, sea, wave),
		AreaName:    st.Province,
		Latitude:    &lat,
		Longitude:   &lon,
		OccurredAt:  obsAt,
		Attributes: map[string]any{
			"station_id":          stID,
			"station_name":        st.Name,
			"sea_level_anomaly_m": sea,
			"wave_height_m":       wave,
			"quality":             quality,
			"critical":            critical,
		},
	}, nil
}

// CoastalSeverity maps a reading to the canonical severity.
func CoastalSeverity(critical bool, sea, wave float64) domain.Severity {
	a := math.Abs(sea)
	switch {
	case critical:
		return domain.SeverityAwas
	case a >= 1.0 || wave >= 4.0:
		return domain.SeveritySiaga
	case a >= 0.5 || wave >= 2.5:
		return domain.SeverityWaspada
	default:
		return domain.SeverityNormal
	}
}

func finiteNumber(m map[string]any, k string) (float64, error) {
	v, ok := m[k]
	if !ok || v == nil {
		return 0, bad(k, "missing")
	}
	var f float64
	switch x := v.(type) {
	case float64:
		f = x
	case json.Number:
		var err error
		if f, err = x.Float64(); err != nil {
			return 0, bad(k, "not a number: %q", x.String())
		}
	default:
		return 0, bad(k, "must be a number, got %T", v)
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, bad(k, "not a finite number")
	}
	return f, nil
}
