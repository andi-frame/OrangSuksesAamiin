package pvmbg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/andi-frame/orangsuksesaamiin/pkg/obs/schemacheck"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testSecret = "test-pvmbg-secret"

type env struct {
	ts    *httptest.Server
	store *Store
	state *State
	tok   string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	cfg := Config{
		Port: "0", TokenSecret: testSecret, AdminToken: "test-admin",
		DelayMin: 10 * time.Millisecond, DelayMax: 20 * time.Millisecond,
		ReportInterval: 0, SeedCount: 24, HangMax: 5 * time.Second,
	}
	log := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	store, state := NewStore(), NewState(cfg)
	gen := NewGenerator(cfg, store, state, log)
	gen.Seed(time.Now())
	ts := httptest.NewServer(NewHandler(cfg, store, state, gen, log))
	t.Cleanup(ts.Close)
	return &env{ts: ts, store: store, state: state, tok: MintToken(testSecret, "bnpb-aggregator", time.Hour, time.Now())}
}

func (e *env) do(t *testing.T, method, path, body string, hdr map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, e.ts.URL+path, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func (e *env) bearer() map[string]string {
	return map[string]string{"Authorization": "Bearer " + e.tok}
}
func admin() map[string]string { return map[string]string{HeaderAdmin: "test-admin"} }

func (e *env) reports(t *testing.T) []map[string]any {
	t.Helper()
	resp, b := e.do(t, "GET", "/volcanic-reports", "", e.bearer())
	if resp.StatusCode != 200 {
		t.Fatalf("reports: %d %s", resp.StatusCode, b)
	}
	var out []map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestTokenMintVerify(t *testing.T) {
	now := time.Now()
	tok := MintToken(testSecret, "x", time.Hour, now)
	if err := VerifyToken(testSecret, tok, now); err != nil {
		t.Fatal(err)
	}
	if err := VerifyToken("other-secret", tok, now); !errors.Is(err, ErrSignature) {
		t.Fatalf("wrong secret: %v", err)
	}
	if err := VerifyToken(testSecret, tok, now.Add(2*time.Hour)); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired: %v", err)
	}
	if err := VerifyToken(testSecret, "bmkg-api-key", now); !errors.Is(err, ErrMalformed) {
		t.Fatalf("bmkg-style key must be malformed: %v", err)
	}
}

func TestAuthMatrix(t *testing.T) {
	e := newEnv(t)
	expired := MintToken(testSecret, "x", time.Minute, time.Now().Add(-time.Hour))
	forged := MintToken("attacker-secret", "x", time.Hour, time.Now())
	cases := []struct {
		name string
		hdr  map[string]string
		want int
	}{
		{"no credential", nil, 401},
		{"BMKG key only", map[string]string{"X-BMKG-Key": "example-bmkg-key"}, 403},
		{"BMKG key as bearer", map[string]string{"Authorization": "Bearer example-bmkg-key"}, 403},
		{"token signed with another secret", map[string]string{"Authorization": "Bearer " + forged}, 403},
		{"non-bearer scheme", map[string]string{"Authorization": "Basic abc"}, 403},
		{"expired token", map[string]string{"Authorization": "Bearer " + expired}, 401},
		{"valid token", e.bearer(), 200},
	}
	for _, c := range cases {
		if resp, _ := e.do(t, "GET", "/volcanic-reports", "", c.hdr); resp.StatusCode != c.want {
			t.Errorf("%s: got %d want %d", c.name, resp.StatusCode, c.want)
		}
	}
}

func TestSeedAtLeast20WithoutConfidenceLevel(t *testing.T) {
	e := newEnv(t)
	rs := e.reports(t)
	if len(rs) < 20 {
		t.Fatalf("seed has %d reports, contract requires >= 20", len(rs))
	}
	levels := map[string]bool{}
	for _, r := range rs {
		if _, has := r["confidence_level"]; has {
			t.Fatal("initial schema must not contain confidence_level")
		}
		levels[r["alert_level"].(string)] = true
	}
	if len(levels) < 4 {
		t.Errorf("seed covers only alert levels %v", levels)
	}
}

func TestSchemaVersionChangesResponseWithoutRestart(t *testing.T) {
	e := newEnv(t)
	before := len(e.reports(t))

	// admin credentials are required
	if resp, _ := e.do(t, "POST", "/admin/schema-version", `{"confidence_level":true}`, nil); resp.StatusCode != 401 {
		t.Fatalf("schema switch without admin token: %d", resp.StatusCode)
	}
	// the gateway (Bearer) credential must not open the admin API
	if resp, _ := e.do(t, "POST", "/admin/schema-version", `{"confidence_level":true}`, e.bearer()); resp.StatusCode != 401 {
		t.Fatalf("gateway token must not work as admin: %d", resp.StatusCode)
	}

	if resp, b := e.do(t, "POST", "/admin/schema-version", `{"confidence_level":true}`, admin()); resp.StatusCode != 200 {
		t.Fatalf("switch failed: %s", b)
	}
	if resp, b := e.do(t, "POST", "/admin/generate", `{"count":2}`, admin()); resp.StatusCode != 200 {
		t.Fatalf("generate failed: %s", b)
	}
	rs := e.reports(t)
	if len(rs) != before+2 {
		t.Fatalf("got %d reports, want %d", len(rs), before+2)
	}
	withField := 0
	for i, r := range rs {
		_, has := r["confidence_level"]
		if i < before && has {
			t.Errorf("old report %v must keep the old shape", r["report_id"])
		}
		if has {
			withField++
			if c := r["confidence_level"].(float64); c < 0 || c > 1 {
				t.Errorf("confidence_level %v out of [0,1]", c)
			}
		}
	}
	if withField != 2 {
		t.Fatalf("expected the 2 new reports to carry confidence_level, got %d", withField)
	}

	// switching back removes the field from responses again
	e.do(t, "POST", "/admin/schema-version", `{"version":1}`, admin())
	for _, r := range e.reports(t) {
		if _, has := r["confidence_level"]; has {
			t.Fatal("confidence_level must disappear when the switch is off")
		}
	}
}

func TestOutageErrorModeAndRecovery(t *testing.T) {
	e := newEnv(t)
	e.do(t, "POST", "/admin/outage", `{"enabled":true}`, admin())
	if resp, _ := e.do(t, "GET", "/volcanic-reports", "", e.bearer()); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("outage: got %d want 503", resp.StatusCode)
	}
	// dead host does not check credentials either
	if resp, _ := e.do(t, "GET", "/volcanic-reports", "", nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("outage without credential: got %d want 503", resp.StatusCode)
	}
	resp, b := e.do(t, "GET", "/health", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(string(b), `"outage":true`) {
		t.Fatalf("health during outage: %d %s", resp.StatusCode, b)
	}
	e.do(t, "POST", "/admin/outage", `{"enabled":false}`, admin())
	if resp, _ := e.do(t, "GET", "/volcanic-reports", "", e.bearer()); resp.StatusCode != 200 {
		t.Fatalf("after outage: got %d want 200", resp.StatusCode)
	}
}

func TestOutageHangModeNeverAnswers(t *testing.T) {
	e := newEnv(t)
	e.do(t, "POST", "/admin/outage", `{"enabled":true,"mode":"hang"}`, admin())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", e.ts.URL+"/volcanic-reports", nil)
	req.Header.Set("Authorization", "Bearer "+e.tok)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
		t.Fatalf("hang mode answered with %d", resp.StatusCode)
	}
}

func TestDelayIsConfigurableAtRuntime(t *testing.T) {
	e := newEnv(t)
	e.do(t, "POST", "/admin/delay", `{"ms":400}`, admin())
	start := time.Now()
	e.reports(t)
	if d := time.Since(start); d < 400*time.Millisecond {
		t.Fatalf("delay not applied: %v", d)
	}
	if resp, _ := e.do(t, "POST", "/admin/delay", `{"min_ms":900,"max_ms":100}`, admin()); resp.StatusCode != 400 {
		t.Fatalf("invalid range accepted: %d", resp.StatusCode)
	}
}

func TestSinceFilter(t *testing.T) {
	e := newEnv(t)
	all := e.store.Since(time.Time{}, false)
	cut := all[len(all)-2].ReportedAt.Format(time.RFC3339Nano)
	resp, b := e.do(t, "GET", "/volcanic-reports?since="+cut, "", e.bearer())
	var got []map[string]any
	_ = json.Unmarshal(b, &got)
	if resp.StatusCode != 200 || len(got) != 1 {
		t.Fatalf("since filter: %d, %d items", resp.StatusCode, len(got))
	}
}

func TestExtraFieldsAndOverridesSimulateSchemaChanges(t *testing.T) {
	e := newEnv(t)
	before := len(e.reports(t))

	// a brand-new, unknown field appears on new reports
	e.do(t, "POST", "/admin/schema-version", `{"extra_fields":{"lava_dome_volume_m3":125000,"sensor":{"type":"seismometer"}}}`, admin())
	e.do(t, "POST", "/admin/generate", `{"count":1}`, admin())
	// and one report changes an existing field in an unsupported way
	e.do(t, "POST", "/admin/generate", `{"count":1,"override":{"alert_level":"Darurat"}}`, admin())
	// and one drops a required field
	e.do(t, "POST", "/admin/generate", `{"count":1,"override":{"volcano_id":null}}`, admin())

	rs := e.reports(t)
	if len(rs) != before+3 {
		t.Fatalf("got %d reports", len(rs))
	}
	if rs[before]["lava_dome_volume_m3"] != 125000.0 {
		t.Fatalf("extra field missing: %v", rs[before])
	}
	if rs[before+1]["alert_level"] != "Darurat" {
		t.Fatalf("override not applied: %v", rs[before+1])
	}
	if _, has := rs[before+2]["volcano_id"]; has {
		t.Fatalf("null override must remove the field: %v", rs[before+2])
	}
	if _, has := rs[before-1]["lava_dome_volume_m3"]; has {
		t.Fatal("older reports must not change")
	}

	// clearing the extra fields stops adding them
	e.do(t, "POST", "/admin/schema-version", `{"extra_fields":{}}`, admin())
	e.do(t, "POST", "/admin/generate", `{"count":1}`, admin())
	rs = e.reports(t)
	if _, has := rs[len(rs)-1]["lava_dome_volume_m3"]; has {
		t.Fatal("cleared extra fields must not be added any more")
	}
}

// The mock must honour the M1 contract in both schema versions (contracts/schemas).
func TestResponsesConformToContractSchemaBeforeAndAfterSwitch(t *testing.T) {
	sc, err := schemacheck.Load("../../../../contracts/schemas/volcanic-report.json")
	if err != nil {
		t.Fatal(err)
	}
	e := newEnv(t)
	check := func(label string, wantConfidence bool) {
		resp, b := e.do(t, "GET", "/volcanic-reports", "", e.bearer())
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d", label, resp.StatusCode)
		}
		var items []map[string]any
		_ = json.Unmarshal(b, &items)
		withField := 0
		for i, it := range items {
			if errs := sc.Validate(it); len(errs) != 0 {
				t.Fatalf("%s[%d] violates the contract: %v", label, i, errs)
			}
			if _, has := it["confidence_level"]; has {
				withField++
			}
		}
		if (withField > 0) != wantConfidence {
			t.Fatalf("%s: %d reports carry confidence_level, want present=%v", label, withField, wantConfidence)
		}
	}
	check("initial schema", false)
	e.do(t, "POST", "/admin/schema-version", `{"confidence_level":true}`, admin())
	e.do(t, "POST", "/admin/generate", `{"count":2}`, admin())
	check("after the switch", true)
}
