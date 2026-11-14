package pvmbg

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// M2 P3: the incompatible mode renames a field, changes a type and drops a
// required field, only for reports created while it is on.
func TestIncompatibleModeChangesNewReportsOnly(t *testing.T) {
	e := newEnv(t)
	before := len(e.reports(t))
	if resp, b := e.do(t, "POST", "/admin/schema-version", `{"mode":"incompatible"}`, admin()); resp.StatusCode != 200 ||
		!strings.Contains(string(b), `"schema_version":3`) {
		t.Fatalf("switch: %d %s", resp.StatusCode, b)
	}
	e.do(t, "POST", "/admin/generate", `{"count":1}`, admin())
	rs := e.reports(t)
	old, neu := rs[0], rs[len(rs)-1]
	if len(rs) != before+1 || old["alert_level"] == nil || old["volcano_id"] == nil {
		t.Fatalf("old report changed: %v", old)
	}
	if _, has := neu["alert_level"]; has || neu["level"] == nil {
		t.Fatalf("rename: %v", neu)
	}
	if _, isNum := neu["reported_at"].(float64); !isNum {
		t.Fatalf("type change: %v", neu["reported_at"])
	}
	if _, has := neu["volcano_id"]; has {
		t.Fatalf("required field still there: %v", neu)
	}
	if loc, _ := neu["location"].(map[string]any); loc["volcano_code"] == nil {
		t.Fatalf("moved field: %v", neu)
	}

	// a single change, and back to the compatible schema
	e.do(t, "POST", "/admin/schema-version", `{"incompatible":["rename"]}`, admin())
	e.do(t, "POST", "/admin/generate", `{"count":1}`, admin())
	last := e.reports(t)[before+1]
	if last["level"] == nil || last["volcano_id"] == nil {
		t.Fatalf("only rename expected: %v", last)
	}
	if resp, _ := e.do(t, "POST", "/admin/schema-version", `{"incompatible":["shuffle"]}`, admin()); resp.StatusCode != 400 {
		t.Fatal("unknown change accepted")
	}
	e.do(t, "POST", "/admin/schema-version", `{"version":1}`, admin())
	e.do(t, "POST", "/admin/generate", `{"count":1}`, admin())
	if r := e.reports(t)[before+2]; r["alert_level"] == nil {
		t.Fatalf("version 1 must restore the original schema: %v", r)
	}
}

func TestMalformedKinds(t *testing.T) {
	e := newEnv(t)
	for _, k := range []string{`{"kind":"deep","depth":50000}`, `{"kind":"invalid_utf8"}`, `{"kind":"oversize","size_bytes":3000000}`} {
		if resp, b := e.do(t, "POST", "/admin/malformed", k, admin()); resp.StatusCode != 200 {
			t.Fatalf("%s: %d %s", k, resp.StatusCode, b)
		}
	}
	_, body := e.do(t, "GET", "/volcanic-reports", "", e.bearer())
	if utf8.Valid(body) || json.Valid(body) {
		t.Fatal("body must contain the broken reports verbatim")
	}
	if !strings.Contains(string(body), strings.Repeat("[", 50000)) || len(body) < 3000000 {
		t.Fatal("deep / oversize report missing")
	}

	// truncated: exactly the next response is cut, inside its last element
	e2 := newEnv(t)
	if resp, _ := e2.do(t, "POST", "/admin/malformed", `{"kind":"truncated"}`, admin()); resp.StatusCode != 200 {
		t.Fatal("truncated")
	}
	_, cut := e2.do(t, "GET", "/volcanic-reports", "", e2.bearer())
	if json.Valid(cut) || !strings.HasPrefix(string(cut), "[{") {
		t.Fatalf("first response must be cut: %q", cut[max(0, len(cut)-60):])
	}
	if !json.Valid(e2.body(t)) {
		t.Fatal("only one response is cut")
	}
	if resp, _ := e2.do(t, "POST", "/admin/malformed", `{"kind":"gibberish"}`, admin()); resp.StatusCode != 400 {
		t.Fatal("unknown kind accepted")
	}
}

func (e *env) body(t *testing.T) []byte {
	t.Helper()
	_, b := e.do(t, "GET", "/volcanic-reports", "", e.bearer())
	return b
}

// On Kubernetes the Tester Agent calls POST /admin/schema-version {"version":2} without
// our admin token. With PVMBG_ADMIN_OPEN that works; a wrong token is still refused.
func TestAdminOpenAcceptsCallsWithoutToken(t *testing.T) {
	e := newEnv(t)
	if resp, _ := e.do(t, "POST", "/admin/schema-version", `{"version":2}`, nil); resp.StatusCode != 401 {
		t.Fatalf("closed by default: %d", resp.StatusCode)
	}
	cfg := Config{TokenSecret: testSecret, AdminToken: "test-admin", AdminOpen: true, SeedCount: 20, HangMax: time.Second}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	store, state := NewStore(), NewState(cfg)
	ts := httptest.NewServer(NewHandler(cfg, store, state, NewGenerator(cfg, store, state, log), log))
	defer ts.Close()
	open := &env{ts: ts, store: store, state: state}
	if resp, b := open.do(t, "POST", "/admin/schema-version", `{"version":2}`, nil); resp.StatusCode != 200 ||
		!strings.Contains(string(b), `"schema_version":2`) {
		t.Fatalf("open, no token: %d %s", resp.StatusCode, b)
	}
	if resp, _ := open.do(t, "POST", "/admin/schema-version", `{"version":1}`, map[string]string{HeaderAdmin: "wrong"}); resp.StatusCode != 403 {
		t.Fatalf("open, wrong token: %d", resp.StatusCode)
	}
}
