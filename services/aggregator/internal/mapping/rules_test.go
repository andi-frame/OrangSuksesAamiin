package mapping

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRulesRenameMoveAndCoerceOnlyFillAbsentFields(t *testing.T) {
	r := SchemaRules{"PVMBG": {
		Rename: map[string]string{"level": "alert_level", "location.volcano_code": "volcano_id"},
		Coerce: map[string]string{"reported_at": CoerceUnixSeconds, "eruption_count_24h": CoerceToNumber},
	}}
	raw := map[string]any{
		"report_id": "VR-1", "level": "Siaga", "location": map[string]any{"volcano_code": "merapi"},
		"reported_at": 1794650400.0, "eruption_count_24h": "12",
	}
	applied := r.Apply("PVMBG", raw)
	if len(applied) != 4 {
		t.Fatalf("applied: %v", applied)
	}
	want := time.Unix(1794650400, 0).UTC().Format(time.RFC3339Nano)
	if raw["alert_level"] != "Siaga" || raw["volcano_id"] != "merapi" || raw["reported_at"] != want ||
		raw["eruption_count_24h"] != 12.0 {
		t.Fatalf("result: %v", raw)
	}
	if _, left := raw["level"]; left {
		t.Fatal("renamed field must not remain")
	}
	if _, left := raw["location"]; left {
		t.Fatal("an emptied parent object must not remain")
	}
	h, err := Volcanic(raw, VolcanoTable{"merapi": {Name: "Gunung Merapi"}}, time.Now())
	if err != nil || h.AreaName != "Gunung Merapi" {
		t.Fatalf("mapped: %+v %v", h, err)
	}

	// a record already in the old schema is never altered
	old := map[string]any{"alert_level": "Awas", "level": "Normal", "reported_at": "2026-11-14T08:00:00Z"}
	r.Apply("PVMBG", old)
	if old["alert_level"] != "Awas" || old["level"] != "Normal" {
		t.Fatalf("existing canonical field overwritten: %v", old)
	}
	// another source is untouched
	other := map[string]any{"level": "x"}
	if r.Apply("BMKG", other) != nil || other["level"] != "x" {
		t.Fatal("rules leaked to another source")
	}
}

func TestLoadSchemaRulesValidates(t *testing.T) {
	dir := t.TempDir()
	if r, err := LoadSchemaRules(filepath.Join(dir, "missing.json")); err != nil || len(r) != 0 {
		t.Fatalf("missing file must mean no rules: %v %v", r, err)
	}
	bad := filepath.Join(dir, "bad.json")
	_ = os.WriteFile(bad, []byte(`{"PVMBG":{"coerce":{"reported_at":"guess_it"}}}`), 0o600)
	if _, err := LoadSchemaRules(bad); err == nil {
		t.Fatal("an unknown conversion must stop the start-up")
	}
	good := filepath.Join(dir, "good.json")
	_ = os.WriteFile(good, []byte(`{"PVMBG":{"rename":{"level":"alert_level"}}}`), 0o600)
	r, err := LoadSchemaRules(good)
	if err != nil || len(r.Summary()) != 1 {
		t.Fatalf("good rules: %v %v", r, err)
	}
}
