package policy_test

import (
	"encoding/json"
	"testing"

	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/policy"
)

func TestMediaStripsMentah(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{
		"hazard_id": "bmkg-x", "source": "BMKG", "hazard_type": "SEISMIC", "severity": "NORMAL",
		"area_name": "a", "occurred_at": "t", "ingested_at": "t", "version": 1, "updated_at": "t",
		"latitude": -1.0, "attributes": map[string]any{"m": 1},
	})
	out, err := policy.FilterItem("media", raw, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := out["latitude"]; ok {
		t.Fatal("media must not receive latitude")
	}
	if _, ok := out["attributes"]; ok {
		t.Fatal("media must not receive attributes")
	}
}

func TestMediaForbiddenFieldQuery(t *testing.T) {
	if err := policy.CheckQuery("media", "latitude", ""); err == nil {
		t.Fatal("expected forbidden")
	}
}

func TestMediaForbiddenInclude(t *testing.T) {
	if err := policy.CheckQuery("media", "", "mentah"); err == nil {
		t.Fatal("expected forbidden for include=mentah")
	}
}

func TestLapanganAllowsMentahQuery(t *testing.T) {
	if err := policy.CheckQuery("lapangan", "latitude", "mentah"); err != nil {
		t.Fatal("lapangan should not be blocked by media rules")
	}
}

func TestCanSourcesStatus(t *testing.T) {
	if policy.CanSourcesStatus("media") {
		t.Fatal("media must not access sources status")
	}
	if !policy.CanSourcesStatus("lapangan") || !policy.CanSourcesStatus("pusat") {
		t.Fatal("lapangan and pusat should access sources status")
	}
}
