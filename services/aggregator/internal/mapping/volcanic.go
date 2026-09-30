package mapping

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
)

// Known top-level fields of a PVMBG VolcanicReport (contract). confidence_level
// is deliberately NOT listed: it is handled by the generic "unknown field ->
// attributes" rule, which is exactly what Problem 1 demonstrates.
var volcanicKnown = set(
	"report_id", "volcano_id", "alert_level", "eruption_count_24h",
	"ash_column_height_m", "reported_at",
)

// Volcano is one row of BNPB's static reference table.
type Volcano struct {
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// VolcanoTable maps volcano_id -> reference data (owned by BNPB).
type VolcanoTable map[string]Volcano

// LoadVolcanoTable reads the static reference table from a JSON file.
func LoadVolcanoTable(path string) (VolcanoTable, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("volcano table: %w", err)
	}
	var t VolcanoTable
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, fmt.Errorf("volcano table %s: %w", path, err)
	}
	return t, nil
}

func alertToSeverity(s string) (domain.Severity, bool) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "NORMAL":
		return domain.SeverityNormal, true
	case "WASPADA":
		return domain.SeverityWaspada, true
	case "SIAGA":
		return domain.SeveritySiaga, true
	case "AWAS":
		return domain.SeverityAwas, true
	}
	return "", false
}

// Volcanic maps a raw VolcanicReport to a HazardEvent.
//
// A volcano_id missing from the reference table is not fatal: the raw id is used
// as area_name, coordinates stay null and the raw id is kept in attributes
// (volcano_id) so the record is still usable and the gap is visible.
func Volcanic(raw map[string]any, table VolcanoTable, ingestedAt time.Time) (*domain.HazardEvent, error) {
	id, err := reqString(raw, "report_id")
	if err != nil {
		return nil, err
	}
	vid, err := reqString(raw, "volcano_id")
	if err != nil {
		return nil, err
	}
	lvl, err := reqString(raw, "alert_level")
	if err != nil {
		return nil, err
	}
	sev, ok := alertToSeverity(lvl)
	if !ok {
		return nil, bad("alert_level", "unknown enum value %q", lvl)
	}
	rep, err := reqTime(raw, "reported_at")
	if err != nil {
		return nil, err
	}

	attrs := splitExtras(raw, volcanicKnown) // includes confidence_level when present
	for _, k := range []string{"eruption_count_24h", "ash_column_height_m"} {
		if v, ok := raw[k]; ok {
			attrs[k] = v
		}
	}

	h := &domain.HazardEvent{
		HazardID:    domain.HazardID(domain.SourcePVMBG, id),
		Source:      domain.SourcePVMBG,
		SourceRefID: id,
		HazardType:  domain.HazardVolcanic,
		Severity:    sev,
		OccurredAt:  rep,
		IngestedAt:  ingestedAt.UTC().Truncate(time.Microsecond),
		Attributes:  attrs,
	}
	if v, found := table[vid]; found {
		lat, lon := v.Latitude, v.Longitude
		h.AreaName, h.Latitude, h.Longitude = v.Name, &lat, &lon
	} else {
		h.AreaName = vid
		attrs["volcano_id"] = vid
	}
	return h, nil
}
