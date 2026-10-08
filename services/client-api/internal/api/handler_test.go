package api_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/aggregator"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/api"
)

var secret = []byte("test-secret-not-a-real-credential")

const hazardJSON = `{"hazard_id":"pvmbg-R1","source":"PVMBG","source_ref_id":"R1","hazard_type":"VOLCANIC",
"severity":"SIAGA","area_name":"Gunung Prikitiw","latitude":-7.5,"longitude":110.4,
"occurred_at":"2026-10-07T10:00:00Z","ingested_at":"2026-10-07T10:00:01Z","version":1,"updated_at":"2026-10-07T10:00:01Z",
"attributes":{"eruption_count_24h":3,"confidence_level":0.8}}`

// newServer wires the real handler to a stub Aggregator whose PVMBG freshness is stale when pvmbgStale is true.
func newServer(t *testing.T, pvmbgStale bool) *httptest.Server {
	t.Helper()
	staleSince := "null"
	state := `"up"`
	if pvmbgStale {
		staleSince = `"2026-10-07T09:00:00Z"`
		state = `"down"`
	}
	agg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Service-Key") != "svc-key" {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		switch {
		case r.URL.Path == "/internal/hazards":
			io.WriteString(w, `{"items":[`+hazardJSON+`],"next_cursor":"","freshness":{`+
				`"BMKG":{"state":"up","last_success_at":null,"stale_since":null},`+
				`"PVMBG":{"state":`+state+`,"last_success_at":null,"stale_since":`+staleSince+`}}}`)
		case strings.HasPrefix(r.URL.Path, "/internal/hazards/"):
			io.WriteString(w, hazardJSON)
		case r.URL.Path == "/internal/sources/status":
			io.WriteString(w, `{"sources":[{"source":"PVMBG","state":`+state+`,"stale_since":`+staleSince+`}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(agg.Close)

	log := obs.NewLogger("client-api-test")
	srv := &api.Server{Agg: aggregator.New(agg.URL, "svc-key", log, 2*time.Second), Log: log}
	ts := httptest.NewServer(srv.Handler(secret))
	t.Cleanup(ts.Close)
	return ts
}

func token(t *testing.T, role string) string {
	t.Helper()
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": role + "-client", "role": role, "exp": time.Now().Add(time.Minute).Unix(),
	}).SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func get(t *testing.T, ts *httptest.Server, path, tok string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, ts.URL+path, nil)
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

func firstItem(t *testing.T, body map[string]any) map[string]any {
	t.Helper()
	items, _ := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %v", body["items"])
	}
	return items[0].(map[string]any)
}

func TestMissingTokenIs401AndBadTokenIs403(t *testing.T) {
	ts := newServer(t, false)
	if code, _ := get(t, ts, "/v1/hazards", ""); code != http.StatusUnauthorized {
		t.Fatalf("no token: got %d want 401", code)
	}
	if code, _ := get(t, ts, "/v1/hazards", "not.a.jwt"); code != http.StatusForbidden {
		t.Fatalf("garbage token: got %d want 403", code)
	}
	expired, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"role": "pusat", "exp": time.Now().Add(-time.Minute).Unix(),
	}).SignedString(secret)
	if code, _ := get(t, ts, "/v1/hazards", expired); code != http.StatusForbidden {
		t.Fatalf("expired token: got %d want 403", code)
	}
	forged, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"role": "pusat", "exp": time.Now().Add(time.Minute).Unix(),
	}).SignedString([]byte("some-other-secret"))
	if code, _ := get(t, ts, "/v1/hazards", forged); code != http.StatusForbidden {
		t.Fatalf("token signed with another key: got %d want 403", code)
	}
	noExp, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"role": "pusat"}).SignedString(secret)
	if code, _ := get(t, ts, "/v1/hazards", noExp); code != http.StatusForbidden {
		t.Fatalf("validly signed token without exp must not be immortal: got %d want 403", code)
	}
	unknownRole, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"role": "superadmin", "exp": time.Now().Add(time.Minute).Unix(),
	}).SignedString(secret)
	if code, _ := get(t, ts, "/v1/hazards", unknownRole); code != http.StatusForbidden {
		t.Fatalf("unknown role: got %d want 403", code)
	}
	noneAlg := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{
		"role": "pusat", "exp": time.Now().Add(time.Minute).Unix(),
	})
	unsigned, _ := noneAlg.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if code, _ := get(t, ts, "/v1/hazards", unsigned); code != http.StatusForbidden {
		t.Fatalf("alg=none token: got %d want 403", code)
	}
}

func TestMediaNeverReceivesMentahOnListOrGet(t *testing.T) {
	ts := newServer(t, false)
	for _, path := range []string{"/v1/hazards", "/v1/hazards/pvmbg-R1"} {
		code, body := get(t, ts, path, token(t, "media"))
		if code != http.StatusOK {
			t.Fatalf("%s: got %d", path, code)
		}
		item := body
		if strings.HasSuffix(path, "hazards") {
			item = firstItem(t, body)
		}
		for _, k := range []string{"latitude", "longitude", "attributes", "source_ref_id"} {
			if _, leaked := item[k]; leaked {
				t.Errorf("%s: media response leaks %q", path, k)
			}
		}
		if item["severity"] != "SIAGA" || item["area_name"] != "Gunung Prikitiw" {
			t.Errorf("%s: media lost summary fields: %v", path, item)
		}
	}
}

func TestMediaExplicitMentahRequestIs403(t *testing.T) {
	ts := newServer(t, false)
	for _, q := range []string{
		"fields=latitude", "fields=Latitude", "include=mentah", "include=mentah,x", "include=raw",
		"fields%5B%5D=attributes", "fields=hazard_id,source_ref_id",
	} {
		if code, _ := get(t, ts, "/v1/hazards?"+q, token(t, "media")); code != http.StatusForbidden {
			t.Errorf("list ?%s: got %d want 403", q, code)
		}
		if code, _ := get(t, ts, "/v1/hazards/pvmbg-R1?"+q, token(t, "media")); code != http.StatusForbidden {
			t.Errorf("get ?%s: got %d want 403", q, code)
		}
	}
}

func TestLapanganAndPusatReceiveFullHazard(t *testing.T) {
	ts := newServer(t, false)
	for _, role := range []string{"lapangan", "pusat"} {
		code, body := get(t, ts, "/v1/hazards?fields=latitude&include=mentah", token(t, role))
		if code != http.StatusOK {
			t.Fatalf("%s: got %d", role, code)
		}
		item := firstItem(t, body)
		for _, k := range []string{"latitude", "longitude", "attributes", "source_ref_id"} {
			if _, ok := item[k]; !ok {
				t.Errorf("%s: missing %q", role, k)
			}
		}
	}
}

func TestSourcesStatusRoles(t *testing.T) {
	ts := newServer(t, false)
	if code, _ := get(t, ts, "/v1/sources/status", token(t, "media")); code != http.StatusForbidden {
		t.Fatalf("media: got %d want 403", code)
	}
	for _, role := range []string{"lapangan", "pusat"} {
		if code, _ := get(t, ts, "/v1/sources/status", token(t, role)); code != http.StatusOK {
			t.Fatalf("%s: got %d want 200", role, code)
		}
	}
}

// Graceful degradation (P2): while the source is down the hazard is still served, marked stale,
// for every role, and freshness.stale_since is passed through.
func TestStaleMarkerWhenSourceIsDown(t *testing.T) {
	down := newServer(t, true)
	up := newServer(t, false)
	for _, role := range []string{"media", "lapangan", "pusat"} {
		code, body := get(t, down, "/v1/hazards", token(t, role))
		if code != http.StatusOK {
			t.Fatalf("%s during outage: got %d want 200", role, code)
		}
		if firstItem(t, body)["stale"] != true {
			t.Errorf("%s: item must be stale=true during outage", role)
		}
		fresh := body["freshness"].(map[string]any)["PVMBG"].(map[string]any)
		if fresh["stale_since"] == nil {
			t.Errorf("%s: freshness.PVMBG.stale_since missing", role)
		}
		if code, one := get(t, down, "/v1/hazards/pvmbg-R1", token(t, role)); code != http.StatusOK || one["stale"] != true {
			t.Errorf("%s get-by-id during outage: code=%d stale=%v", role, code, one["stale"])
		}
		_, healthy := get(t, up, "/v1/hazards", token(t, role))
		if firstItem(t, healthy)["stale"] != false {
			t.Errorf("%s: item must be stale=false when the source is up", role)
		}
	}
}

func TestUpstreamDownIs503NotHang(t *testing.T) {
	log := obs.NewLogger("client-api-test")
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	srv := &api.Server{Agg: aggregator.New(dead.URL, "svc-key", log, time.Second), Log: log}
	ts := httptest.NewServer(srv.Handler(secret))
	defer ts.Close()
	start := time.Now()
	code, _ := get(t, ts, "/v1/hazards", token(t, "pusat"))
	if code != http.StatusBadGateway {
		t.Fatalf("aggregator unreachable: got %d want 502", code)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("request hung for %v", time.Since(start))
	}
}
