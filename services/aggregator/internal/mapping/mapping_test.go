package mapping

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
)

var now = time.Date(2026, 10, 1, 10, 0, 5, 0, time.UTC)

func decode(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func seismicRaw(t *testing.T, mag string, extra string) map[string]any {
	return decode(t, fmt.Sprintf(`{"event_id":"E1","magnitude":%s,"depth_km":12.5,"epicenter_lat":-6.1,"epicenter_lon":105.8,
		"region_name":"Selat Sunda","occurred_at":"2026-10-01T09:59:00Z","potential_tsunami":true%s}`, mag, extra))
}

func warnRaw(t *testing.T, id, level, issued string) map[string]any {
	return decode(t, fmt.Sprintf(`{"warning_id":%q,"related_event_id":"E1","threat_level":%q,
		"affected_zones":["Anyer","Carita"],"estimated_arrival":"2026-10-01T10:30:00Z","issued_at":%q}`, id, level, issued))
}

func mustSeismic(t *testing.T, raw map[string]any) *domain.HazardEvent {
	t.Helper()
	h, err := Seismic(raw, now)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func mustWarn(t *testing.T, raw map[string]any) Warning {
	t.Helper()
	w, err := ParseWarning(raw)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// ---- SeismicEvent -> HazardEvent (mandatory mapping rules) ----

func TestSeismicFieldMapping(t *testing.T) {
	h := mustSeismic(t, seismicRaw(t, "5.4", ""))
	if h.HazardID != "bmkg-E1" || h.Source != domain.SourceBMKG || h.SourceRefID != "E1" ||
		h.HazardType != domain.HazardSeismic || h.AreaName != "Selat Sunda" {
		t.Fatalf("unexpected mapping: %+v", h)
	}
	if *h.Latitude != -6.1 || *h.Longitude != 105.8 {
		t.Fatalf("coordinates: %v %v", *h.Latitude, *h.Longitude)
	}
	if !h.OccurredAt.Equal(time.Date(2026, 10, 1, 9, 59, 0, 0, time.UTC)) || !h.IngestedAt.Equal(now) {
		t.Fatalf("times: %v %v", h.OccurredAt, h.IngestedAt)
	}
	for _, k := range []string{"magnitude", "depth_km", "potential_tsunami"} {
		if _, ok := h.Attributes[k]; !ok {
			t.Errorf("attribute %s missing", k)
		}
	}
	if _, leaked := h.Attributes["event_id"]; leaked {
		t.Error("event_id has a canonical counterpart (source_ref_id) and must not be in attributes")
	}
}

func TestSeverityWithoutWarning(t *testing.T) {
	cases := []struct {
		mag  string
		want domain.Severity
	}{
		{"0.5", domain.SeverityNormal}, {"4.99", domain.SeverityNormal},
		{"5.0", domain.SeverityWaspada}, {"6.49", domain.SeverityWaspada},
		{"6.5", domain.SeveritySiaga}, {"8.9", domain.SeveritySiaga},
	}
	for _, c := range cases {
		h := mustSeismic(t, seismicRaw(t, c.mag, ""))
		ApplyWarnings(h, nil)
		if h.Severity != c.want {
			t.Errorf("magnitude %s: got %s want %s", c.mag, h.Severity, c.want)
		}
	}
}

func TestSeverityFollowsWarningNotMagnitude(t *testing.T) {
	cases := []struct {
		mag, level string
		want       domain.Severity
	}{
		{"7.9", "Waspada", domain.SeverityWaspada}, // low warning on a big quake: warning wins
		{"4.0", "Siaga", domain.SeveritySiaga},     // high warning on a small quake: warning wins
		{"5.5", "Awas", domain.SeverityAwas},
	}
	for _, c := range cases {
		h := mustSeismic(t, seismicRaw(t, c.mag, ""))
		ApplyWarnings(h, []Warning{mustWarn(t, warnRaw(t, "W1", c.level, "2026-10-01T10:00:00Z"))})
		if h.Severity != c.want {
			t.Errorf("mag %s + %s: got %s want %s", c.mag, c.level, h.Severity, c.want)
		}
		if h.Attributes["threat_level"] != c.level || h.Attributes["warning_id"] != "W1" {
			t.Errorf("warning fields missing from attributes: %v", h.Attributes)
		}
		for _, k := range []string{"related_event_id", "affected_zones", "estimated_arrival"} {
			if _, ok := h.Attributes[k]; !ok {
				t.Errorf("attributes lacks warning field %s", k)
			}
		}
	}
}

func TestAwasWarningWinsAmongSeveral(t *testing.T) {
	h := mustSeismic(t, seismicRaw(t, "6.0", ""))
	ApplyWarnings(h, []Warning{
		mustWarn(t, warnRaw(t, "W1", "Siaga", "2026-10-01T10:05:00Z")),
		mustWarn(t, warnRaw(t, "W2", "Awas", "2026-10-01T10:00:00Z")),
		mustWarn(t, warnRaw(t, "W3", "Waspada", "2026-10-01T10:09:00Z")),
	})
	if h.Severity != domain.SeverityAwas || h.Attributes["warning_id"] != "W2" {
		t.Fatalf("got %s / %v", h.Severity, h.Attributes["warning_id"])
	}
}

func TestLateWarningRaisesSeverityAndIsIdempotent(t *testing.T) {
	h := mustSeismic(t, seismicRaw(t, "5.8", ""))
	ApplyWarnings(h, nil)
	if h.Severity != domain.SeverityWaspada {
		t.Fatalf("before warning: %s", h.Severity)
	}
	before := *h

	ws := []Warning{mustWarn(t, warnRaw(t, "W1", "Awas", "2026-10-01T10:00:03Z"))}
	ApplyWarnings(h, ws)
	if h.Severity != domain.SeverityAwas {
		t.Fatalf("after late warning: %s", h.Severity)
	}
	if domain.SameContent(before, *h) {
		t.Fatal("late warning must change the content (=> new version)")
	}
	after := *h
	ApplyWarnings(h, ws)
	if !domain.SameContent(after, *h) {
		t.Fatal("re-applying the same warning must be a no-op")
	}
}

func TestRepolledEventKeepsStoredWarning(t *testing.T) {
	// stored hazard already carries a warning; the event is polled again (overlap
	// window) and freshly mapped without it. The stored warning must survive.
	stored := mustSeismic(t, seismicRaw(t, "5.8", ""))
	ApplyWarnings(stored, []Warning{mustWarn(t, warnRaw(t, "W1", "Siaga", "2026-10-01T10:00:03Z"))})

	fresh := mustSeismic(t, seismicRaw(t, "5.8", ""))
	var ws []Warning
	if w, ok := ExistingWarning(stored); ok {
		ws = append(ws, w)
	}
	ApplyWarnings(fresh, ws)
	if !domain.SameContent(*stored, *fresh) {
		t.Fatalf("re-polled event must equal stored one:\n%+v\n%+v", stored.Attributes, fresh.Attributes)
	}
}

func TestWarningForOtherEventIgnored(t *testing.T) {
	h := mustSeismic(t, seismicRaw(t, "5.8", ""))
	raw := warnRaw(t, "W9", "Awas", "2026-10-01T10:00:00Z")
	raw["related_event_id"] = "OTHER"
	ApplyWarnings(h, []Warning{mustWarn(t, raw)})
	if h.Severity != domain.SeverityWaspada {
		t.Fatalf("warning of another event changed severity: %s", h.Severity)
	}
}

// ---- Problem 1: schema evolution policy ----

func volcRaw(t *testing.T, extra string) map[string]any {
	return decode(t, fmt.Sprintf(`{"report_id":"R1","volcano_id":"merapi","alert_level":"Siaga",
		"eruption_count_24h":4,"ash_column_height_m":1200.5,"reported_at":"2026-10-01T09:58:00Z"%s}`, extra))
}

var testTable = VolcanoTable{"merapi": {Name: "Gunung Merapi", Latitude: -7.5407, Longitude: 110.4457}}

func TestVolcanicFieldMappingInitialSchema(t *testing.T) {
	h, err := Volcanic(volcRaw(t, ""), testTable, now)
	if err != nil {
		t.Fatal(err)
	}
	if h.HazardID != "pvmbg-R1" || h.Source != domain.SourcePVMBG || h.SourceRefID != "R1" ||
		h.HazardType != domain.HazardVolcanic || h.Severity != domain.SeveritySiaga ||
		h.AreaName != "Gunung Merapi" {
		t.Fatalf("unexpected mapping: %+v", h)
	}
	if *h.Latitude != -7.5407 || *h.Longitude != 110.4457 {
		t.Fatalf("coordinates must come from the static reference table")
	}
	if !h.OccurredAt.Equal(time.Date(2026, 10, 1, 9, 58, 0, 0, time.UTC)) {
		t.Fatalf("occurred_at must be reported_at: %v", h.OccurredAt)
	}
	if h.Attributes["eruption_count_24h"] != 4.0 || h.Attributes["ash_column_height_m"] != 1200.5 {
		t.Fatalf("attributes: %v", h.Attributes)
	}
	if _, has := h.Attributes["confidence_level"]; has {
		t.Fatal("no confidence_level in the initial schema")
	}
	if _, leaked := h.Attributes["volcano_id"]; leaked {
		t.Fatal("volcano_id is consumed by area_name/coordinates for a known volcano")
	}
}

func TestConfidenceLevelAppearsWithoutCodeChange(t *testing.T) {
	old, err := Volcanic(volcRaw(t, ""), testTable, now)
	if err != nil {
		t.Fatal(err)
	}
	neu, err := Volcanic(volcRaw(t, `,"confidence_level":0.87`), testTable, now)
	if err != nil {
		t.Fatalf("new field must not break mapping: %v", err)
	}
	if neu.Attributes["confidence_level"] != 0.87 {
		t.Fatalf("confidence_level not forwarded to attributes: %v", neu.Attributes)
	}
	if neu.Severity != old.Severity || neu.AreaName != old.AreaName {
		t.Fatal("canonical fields must be unaffected by the additive change")
	}
}

func TestUnknownFieldPolicyIsConsistent(t *testing.T) {
	// Policy: every unknown top-level field goes to attributes, verbatim, whatever
	// its JSON type, for both sources.
	extras := `,"confidence_level":0.9,"str":"x","num":3,"flag":true,"nested":{"a":[1,2,{"b":null}]},"list":["p","q"],"nothing":null`
	want := map[string]any{}
	_ = json.Unmarshal([]byte("{"+extras[1:]+"}"), &want)

	v, err := Volcanic(volcRaw(t, extras), testTable, now)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Seismic(seismicRaw(t, "5.1", extras), now)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []*domain.HazardEvent{v, s} {
		for k, wv := range want {
			gv, ok := h.Attributes[k]
			if !ok {
				t.Errorf("%s: unknown field %q was dropped", h.Source, k)
				continue
			}
			a, _ := json.Marshal(gv)
			b, _ := json.Marshal(wv)
			if string(a) != string(b) {
				t.Errorf("%s: field %q altered: %s != %s", h.Source, k, a, b)
			}
		}
	}
}

func TestUnknownWarningFieldsAreNamespaced(t *testing.T) {
	raw := warnRaw(t, "W1", "Siaga", "2026-10-01T10:00:00Z")
	raw["surge_height_m"] = 2.5
	w := mustWarn(t, raw)
	if w.Attrs["warning_surge_height_m"] != 2.5 {
		t.Fatalf("unknown warning field lost: %v", w.Attrs)
	}
}

func TestUnsupportedChangesAreQuarantinedNotGuessed(t *testing.T) {
	cases := []struct {
		name  string
		build func() (any, error)
		field string
	}{
		{"unknown alert_level enum", func() (any, error) {
			r := volcRaw(t, "")
			r["alert_level"] = "Darurat"
			return Volcanic(r, testTable, now)
		}, "alert_level"},
		{"required field removed", func() (any, error) {
			r := volcRaw(t, "")
			delete(r, "alert_level")
			return Volcanic(r, testTable, now)
		}, "alert_level"},
		{"required field renamed", func() (any, error) {
			r := volcRaw(t, "")
			r["volcano"] = r["volcano_id"]
			delete(r, "volcano_id")
			return Volcanic(r, testTable, now)
		}, "volcano_id"},
		{"type change on required field", func() (any, error) {
			r := seismicRaw(t, `"5.4"`, "")
			return Seismic(r, now)
		}, "magnitude"},
		{"bad datetime", func() (any, error) {
			r := volcRaw(t, "")
			r["reported_at"] = "yesterday"
			return Volcanic(r, testTable, now)
		}, "reported_at"},
		{"latitude out of range", func() (any, error) {
			r := seismicRaw(t, "5.0", "")
			r["epicenter_lat"] = 123.0
			return Seismic(r, now)
		}, "epicenter_lat"},
		{"unknown threat_level", func() (any, error) {
			r := warnRaw(t, "W1", "Merah", "2026-10-01T10:00:00Z")
			return ParseWarning(r)
		}, "threat_level"},
	}
	for _, c := range cases {
		_, err := c.build()
		var me *MappingError
		if !errors.As(err, &me) {
			t.Errorf("%s: expected *MappingError, got %v", c.name, err)
			continue
		}
		if me.Field != c.field {
			t.Errorf("%s: error on field %q, want %q", c.name, me.Field, c.field)
		}
	}
}

func TestAttributeOnlyFieldMayDisappear(t *testing.T) {
	raw := volcRaw(t, "")
	delete(raw, "ash_column_height_m")
	if _, err := Volcanic(raw, testTable, now); err != nil {
		t.Fatalf("removing an attribute-only field must be tolerated: %v", err)
	}
}

func TestUnknownVolcanoFallsBackWithoutCrash(t *testing.T) {
	raw := volcRaw(t, "")
	raw["volcano_id"] = "gunung-baru"
	h, err := Volcanic(raw, testTable, now)
	if err != nil {
		t.Fatal(err)
	}
	if h.AreaName != "gunung-baru" || h.Latitude != nil || h.Longitude != nil || h.Attributes["volcano_id"] != "gunung-baru" {
		t.Fatalf("fallback wrong: %+v", h)
	}
}

func TestAlertLevelEnumMapping(t *testing.T) {
	for in, want := range map[string]domain.Severity{
		"Normal": domain.SeverityNormal, "Waspada": domain.SeverityWaspada,
		"Siaga": domain.SeveritySiaga, "Awas": domain.SeverityAwas,
	} {
		raw := volcRaw(t, "")
		raw["alert_level"] = in
		h, err := Volcanic(raw, testTable, now)
		if err != nil || h.Severity != want {
			t.Errorf("%s -> %v (%v), want %s", in, h, err, want)
		}
	}
}

// A hazard must survive a JSON round-trip (what a document/JSONB store does)
// without being seen as "changed": otherwise every re-poll would bump versions.
func TestJSONRoundTripKeepsContentEqual(t *testing.T) {
	h1 := mustSeismic(t, seismicRaw(t, "6.6", `,"confidence_level":1`))
	ApplyWarnings(h1, []Warning{mustWarn(t, warnRaw(t, "W1", "Siaga", "2026-10-01T10:00:00Z"))})
	h2, _ := Volcanic(volcRaw(t, `,"confidence_level":0.87,"x":{"y":[1,"z"]}`), testTable, now)
	for _, h := range []*domain.HazardEvent{h1, h2} {
		b, _ := json.Marshal(h)
		var back domain.HazardEvent
		if err := json.Unmarshal(b, &back); err != nil {
			t.Fatal(err)
		}
		if !domain.SameContent(*h, back) {
			t.Errorf("%s changed across a JSON round-trip", h.HazardID)
		}
	}
}
