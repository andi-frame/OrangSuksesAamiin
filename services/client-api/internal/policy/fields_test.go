package policy_test

import (
	"encoding/json"
	"net/url"
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

func q(kv ...string) url.Values {
	v := url.Values{}
	for i := 0; i < len(kv); i += 2 {
		v.Add(kv[i], kv[i+1])
	}
	return v
}

func TestMediaRejectsEveryWayOfAskingForMentah(t *testing.T) {
	for name, query := range map[string]url.Values{
		"fields=latitude":          q("fields", "latitude"),
		"fields=Latitude (case)":   q("fields", "Latitude"),
		"fields padded":            q("fields", " latitude"),
		"fields list with mentah":  q("fields", "hazard_id,attributes"),
		"fields[] form":            q("fields[]", "latitude"),
		"fields repeated":          q("fields", "severity", "fields", "source_ref_id"),
		"fields unknown name":      q("fields", "secret_thing"),
		"FIELDS key case":          q("FIELDS", "longitude"),
		"include=mentah":           q("include", "mentah"),
		"include=MENTAH":           q("include", "MENTAH"),
		"include list with mentah": q("include", "mentah,x"),
		"include=raw":              q("include", "raw"),
		"include=all":              q("include", "all"),
		"include repeated":         q("include", "summary", "include", "mentah"),
		"include[] form":           q("include[]", "mentah"),
	} {
		if err := policy.CheckQuery("media", query); err == nil {
			t.Errorf("%s: media request must be rejected", name)
		}
	}
}

func TestMediaAllowsSummaryRequests(t *testing.T) {
	for name, query := range map[string]url.Values{
		"no fields":        q(),
		"summary fields":   q("fields", "hazard_id, Severity,area_name"),
		"include=summary":  q("include", "summary"),
		"unrelated filter": q("hazard_type", "SEISMIC", "limit", "5"),
	} {
		if err := policy.CheckQuery("media", query); err != nil {
			t.Errorf("%s: should be allowed: %v", name, err)
		}
	}
}

func TestLapanganAndPusatAllowMentahQuery(t *testing.T) {
	for _, role := range []string{"lapangan", "pusat"} {
		if err := policy.CheckQuery(role, q("fields", "latitude", "include", "mentah")); err != nil {
			t.Fatalf("%s should not be blocked by media rules", role)
		}
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
