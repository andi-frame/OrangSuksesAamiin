package mapping

import (
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
)

// Known top-level fields of a BMKG SeismicEvent (contract). Anything else goes
// to attributes (policy rule 3).
var seismicKnown = set(
	"event_id", "magnitude", "depth_km", "epicenter_lat", "epicenter_lon",
	"region_name", "occurred_at", "potential_tsunami",
)

// Known top-level fields of a TsunamiWarning (contract + the permitted
// issued_at addition).
var warningKnown = set(
	"warning_id", "related_event_id", "threat_level", "affected_zones",
	"estimated_arrival", "issued_at",
)

// SeverityFromMagnitude is the mapping rule used when no TsunamiWarning exists:
// NORMAL < 5.0, WASPADA 5.0 <= m < 6.5, SIAGA >= 6.5.
func SeverityFromMagnitude(m float64) domain.Severity {
	switch {
	case m >= 6.5:
		return domain.SeveritySiaga
	case m >= 5.0:
		return domain.SeverityWaspada
	default:
		return domain.SeverityNormal
	}
}

// Seismic maps a raw SeismicEvent to a HazardEvent WITHOUT tsunami information;
// call ApplyWarnings afterwards to fold warnings in.
func Seismic(raw map[string]any, ingestedAt time.Time) (*domain.HazardEvent, error) {
	id, err := reqString(raw, "event_id")
	if err != nil {
		return nil, err
	}
	mag, err := reqFloat(raw, "magnitude")
	if err != nil {
		return nil, err
	}
	lat, err := reqFloat(raw, "epicenter_lat")
	if err != nil {
		return nil, err
	}
	lon, err := reqFloat(raw, "epicenter_lon")
	if err != nil {
		return nil, err
	}
	if lat < -90 || lat > 90 {
		return nil, bad("epicenter_lat", "out of range [-90,90]: %v", lat)
	}
	if lon < -180 || lon > 180 {
		return nil, bad("epicenter_lon", "out of range [-180,180]: %v", lon)
	}
	region, err := reqString(raw, "region_name")
	if err != nil {
		return nil, err
	}
	occ, err := reqTime(raw, "occurred_at")
	if err != nil {
		return nil, err
	}

	attrs := splitExtras(raw, seismicKnown)
	attrs["magnitude"] = mag
	for _, k := range []string{"depth_km", "potential_tsunami"} { // attribute-only, optional
		if v, ok := raw[k]; ok {
			attrs[k] = v
		}
	}
	return &domain.HazardEvent{
		HazardID:    domain.HazardID(domain.SourceBMKG, id),
		Source:      domain.SourceBMKG,
		SourceRefID: id,
		HazardType:  domain.HazardSeismic,
		Severity:    SeverityFromMagnitude(mag),
		AreaName:    region,
		Latitude:    &lat,
		Longitude:   &lon,
		OccurredAt:  occ,
		IngestedAt:  ingestedAt.UTC().Truncate(time.Microsecond),
		Attributes:  attrs,
	}, nil
}

// Warning is a parsed TsunamiWarning ready to be folded into a HazardEvent.
type Warning struct {
	WarningID      string
	RelatedEventID string
	Severity       domain.Severity // WASPADA | SIAGA | AWAS
	IssuedAt       time.Time       // zero when the source did not provide it
	// Attrs are the attribute entries this warning contributes: the contract
	// fields under their own names (+ issued_at), unknown fields as "warning_<key>".
	Attrs map[string]any
}

func threatToSeverity(s string) (domain.Severity, bool) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "WASPADA":
		return domain.SeverityWaspada, true
	case "SIAGA":
		return domain.SeveritySiaga, true
	case "AWAS":
		return domain.SeverityAwas, true
	}
	return "", false
}

// ParseWarning validates a raw TsunamiWarning.
func ParseWarning(raw map[string]any) (Warning, error) {
	id, err := reqString(raw, "warning_id")
	if err != nil {
		return Warning{}, err
	}
	rel, err := reqString(raw, "related_event_id")
	if err != nil {
		return Warning{}, err
	}
	lvl, err := reqString(raw, "threat_level")
	if err != nil {
		return Warning{}, err
	}
	sev, ok := threatToSeverity(lvl)
	if !ok {
		return Warning{}, bad("threat_level", "unknown enum value %q", lvl)
	}
	w := Warning{WarningID: id, RelatedEventID: rel, Severity: sev, Attrs: map[string]any{
		"warning_id": id, "related_event_id": rel, "threat_level": lvl,
	}}
	for _, k := range []string{"affected_zones", "estimated_arrival", "issued_at"} {
		if v, ok := raw[k]; ok {
			w.Attrs[k] = v
		}
	}
	for k, v := range splitExtras(raw, warningKnown) {
		w.Attrs["warning_"+k] = v
	}
	if t, ok := optTime(raw, "issued_at"); ok {
		w.IssuedAt = t
	}
	return w, nil
}

// ExistingWarning reconstructs the warning previously folded into h (if any),
// so that re-mapping the event or receiving another warning never loses it.
func ExistingWarning(h *domain.HazardEvent) (Warning, bool) {
	id, ok := h.Attributes["warning_id"].(string)
	if !ok {
		return Warning{}, false
	}
	lvl, _ := h.Attributes["threat_level"].(string)
	sev, ok := threatToSeverity(lvl)
	if !ok {
		return Warning{}, false
	}
	w := Warning{WarningID: id, RelatedEventID: h.SourceRefID, Severity: sev, Attrs: map[string]any{}}
	for k, v := range h.Attributes {
		if _, known := warningKnown[k]; known || strings.HasPrefix(k, "warning_") {
			w.Attrs[k] = v
		}
	}
	if t, ok := optTime(h.Attributes, "issued_at"); ok {
		w.IssuedAt = t
	}
	return w, true
}

// better reports whether a should replace b as "the" warning of an event: the
// higher threat wins; ties go to the later-issued, then the greater id.
func better(a, b Warning) bool {
	if a.Severity.Rank() != b.Severity.Rank() {
		return a.Severity.Rank() > b.Severity.Rank()
	}
	if !a.IssuedAt.Equal(b.IssuedAt) {
		return a.IssuedAt.After(b.IssuedAt)
	}
	return a.WarningID > b.WarningID
}

// ApplyWarnings folds warnings (and the warning already stored in h, if any)
// into h and recomputes its severity per the mapping rule:
//
//	warning present -> AWAS when a warning is Awas, otherwise the threat_level
//	                   of the (highest) related warning
//	no warning      -> derived from magnitude
//
// It is idempotent, so re-applying the same warnings changes nothing.
func ApplyWarnings(h *domain.HazardEvent, ws []Warning) {
	var best *Warning
	if old, ok := ExistingWarning(h); ok {
		best = &old
	}
	for i := range ws {
		if ws[i].RelatedEventID != h.SourceRefID {
			continue
		}
		if best == nil || better(ws[i], *best) {
			best = &ws[i]
		}
	}
	if best == nil {
		if mag, ok := h.Attributes["magnitude"].(float64); ok {
			h.Severity = SeverityFromMagnitude(mag)
		}
		return
	}
	if h.Attributes == nil {
		h.Attributes = map[string]any{}
	}
	for k, v := range best.Attrs {
		h.Attributes[k] = v
	}
	h.Severity = best.Severity
}
