package pvmbg

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

// HeaderAdmin guards the operator endpoints (/admin/*).
const HeaderAdmin = "X-Admin-Token"

type server struct {
	cfg   Config
	store *Store
	state *State
	gen   *Generator
	log   *slog.Logger
}

// NewHandler wires the PVMBG mock routes.
func NewHandler(cfg Config, store *Store, state *State, gen *Generator, log *slog.Logger) http.Handler {
	s := &server{cfg: cfg, store: store, state: state, gen: gen, log: log}
	mux := http.NewServeMux()
	// /health stays answerable during an outage so the operator can see the flag;
	// data endpoints are the ones that "die".
	mux.HandleFunc("GET /health", obs.LivenessHandler("pvmbg-mock", func() map[string]any {
		sn := state.Snapshot()
		return map[string]any{"reports": store.Count(), "outage": sn.Outage, "schema_version": sn.SchemaVersion}
	}))
	mux.HandleFunc("GET /ready", obs.LivenessHandler("pvmbg-mock", nil))
	mux.Handle("GET /volcanic-reports", s.outage(s.withBearer(http.HandlerFunc(s.volcanicReports))))
	mux.Handle("GET /admin/state", s.withAdmin(http.HandlerFunc(s.adminState)))
	mux.Handle("POST /admin/schema-version", s.withAdmin(http.HandlerFunc(s.adminSchema)))
	mux.Handle("POST /admin/outage", s.withAdmin(http.HandlerFunc(s.adminOutage)))
	mux.Handle("POST /admin/delay", s.withAdmin(http.HandlerFunc(s.adminDelay)))
	mux.Handle("POST /admin/generate", s.withAdmin(http.HandlerFunc(s.adminGenerate)))
	return obs.Middleware(log, "/health", "/ready")(mux)
}

// outage makes the data endpoint behave like a dead service: either an
// immediate 503 ("error") or no answer at all ("hang"), until the operator
// turns the outage off. It runs before authentication, like a dead host would.
func (s *server) outage(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		on, mode := s.state.Outage()
		if !on {
			next.ServeHTTP(w, r)
			return
		}
		s.log.WarnContext(r.Context(), "outage_active", "mode", mode)
		if mode == "hang" {
			select {
			case <-r.Context().Done(): // client gave up
			case <-time.After(s.cfg.HangMax):
				obs.WriteError(w, http.StatusServiceUnavailable, "outage", "PVMBG is down")
			}
			return
		}
		obs.WriteError(w, http.StatusServiceUnavailable, "outage", "PVMBG is down")
	})
}

func tokenEqual(got, want string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// withBearer enforces "Authorization: Bearer <pvmbg-token>". No credential is
// 401; an invalid one - including BMKG's X-BMKG-Key or any token not signed with
// the PVMBG secret - is 403; an expired PVMBG token is 401.
func (s *server) withBearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" {
			if r.Header.Get("X-BMKG-Key") != "" {
				s.log.WarnContext(r.Context(), "auth_rejected", "reason", "foreign_credential")
				obs.WriteError(w, http.StatusForbidden, "forbidden", "credential does not belong to PVMBG")
				return
			}
			s.log.WarnContext(r.Context(), "auth_rejected", "reason", "missing_token")
			obs.WriteError(w, http.StatusUnauthorized, "unauthorized", "missing Authorization: Bearer <pvmbg-token>")
			return
		}
		scheme, tok, ok := strings.Cut(auth, " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || tok == "" {
			obs.WriteError(w, http.StatusForbidden, "forbidden", "expected Authorization: Bearer <pvmbg-token>")
			return
		}
		if err := VerifyToken(s.cfg.TokenSecret, strings.TrimSpace(tok), time.Now()); err != nil {
			s.log.WarnContext(r.Context(), "auth_rejected", "reason", err.Error())
			if errors.Is(err, ErrExpired) {
				obs.WriteError(w, http.StatusUnauthorized, "token_expired", "PVMBG token expired")
				return
			}
			obs.WriteError(w, http.StatusForbidden, "forbidden", "invalid PVMBG token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *server) withAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := r.Header.Get(HeaderAdmin)
		if tok == "" {
			obs.WriteError(w, http.StatusUnauthorized, "unauthorized", "missing "+HeaderAdmin+" header")
			return
		}
		if !tokenEqual(tok, s.cfg.AdminToken) {
			obs.WriteError(w, http.StatusForbidden, "forbidden", "invalid admin token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// delay reproduces PVMBG's slow, fluctuating latency: uniform in [min, max],
// both adjustable at runtime. Returns false when the client went away.
func (s *server) delay(ctx context.Context) bool {
	lo, hi := s.state.Delay()
	d := lo
	if span := hi - lo; span > 0 {
		d += time.Duration(rand.Int64N(int64(span)))
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *server) volcanicReports(w http.ResponseWriter, r *http.Request) {
	var since time.Time
	if v := r.URL.Query().Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			obs.WriteError(w, http.StatusBadRequest, "bad_request", "since must be ISO 8601 / RFC 3339")
			return
		}
		since = t
	}
	if !s.delay(r.Context()) {
		return
	}
	// The schema flag is read after the delay: it reflects the moment of the
	// response, so a switch flipped while the request was waiting is honoured.
	obs.WriteJSON(w, http.StatusOK, s.store.Since(since, s.state.Confidence()))
}

func decode(r *http.Request, v any) error {
	if r.ContentLength == 0 {
		return nil
	}
	return json.NewDecoder(r.Body).Decode(v)
}

func (s *server) adminState(w http.ResponseWriter, r *http.Request) {
	obs.WriteJSON(w, http.StatusOK, s.state.Snapshot())
}

// adminSchema switches confidence_level on/off at runtime, no restart needed.
// Body: {"confidence_level": true} or {"version": 2} (1 = original schema).
func (s *server) adminSchema(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ConfidenceLevel *bool          `json:"confidence_level"`
		Version         *int           `json:"version"`
		ExtraFields     map[string]any `json:"extra_fields"`
	}
	if err := decode(r, &body); err != nil || (body.ConfidenceLevel == nil && body.Version == nil && body.ExtraFields == nil) {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", `body must contain confidence_level (bool), version (1|2) and/or extra_fields (object)`)
		return
	}
	if body.ConfidenceLevel != nil || body.Version != nil {
		on := false
		if body.ConfidenceLevel != nil {
			on = *body.ConfidenceLevel
		} else {
			if *body.Version != 1 && *body.Version != 2 {
				obs.WriteError(w, http.StatusBadRequest, "bad_request", "version must be 1 or 2")
				return
			}
			on = *body.Version == 2
		}
		s.state.SetConfidence(on)
		s.log.WarnContext(r.Context(), "schema_version_changed", "confidence_level", on)
	}
	if body.ExtraFields != nil {
		s.state.SetExtra(body.ExtraFields)
		s.log.WarnContext(r.Context(), "extra_fields_changed", "count", len(body.ExtraFields))
	}
	obs.WriteJSON(w, http.StatusOK, s.state.Snapshot())
}

// adminOutage turns the outage on/off instantly.
// Body: {"enabled": true, "mode": "error"|"hang"} (mode defaults to "error").
func (s *server) adminOutage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Enabled *bool  `json:"enabled"`
		Mode    string `json:"mode"`
	}
	if err := decode(r, &body); err != nil || body.Enabled == nil {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", `body must be {"enabled":true|false,"mode":"error"|"hang"}`)
		return
	}
	if body.Mode != "" && body.Mode != "error" && body.Mode != "hang" {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", `mode must be "error" or "hang"`)
		return
	}
	s.state.SetOutage(*body.Enabled, body.Mode)
	s.log.WarnContext(r.Context(), "outage_changed", "enabled", *body.Enabled, "mode", body.Mode)
	obs.WriteJSON(w, http.StatusOK, s.state.Snapshot())
}

// adminDelay changes the response delay at runtime (spec: "besaran delay wajib
// dapat dikonfigurasi"). Body: {"ms":3000} (fixed) or {"min_ms":500,"max_ms":3000}.
func (s *server) adminDelay(w http.ResponseWriter, r *http.Request) {
	var body struct {
		MS    *int64 `json:"ms"`
		MinMS *int64 `json:"min_ms"`
		MaxMS *int64 `json:"max_ms"`
	}
	if err := decode(r, &body); err != nil {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "invalid JSON body")
		return
	}
	var lo, hi int64
	switch {
	case body.MS != nil:
		lo, hi = *body.MS, *body.MS
	case body.MinMS != nil && body.MaxMS != nil:
		lo, hi = *body.MinMS, *body.MaxMS
	default:
		obs.WriteError(w, http.StatusBadRequest, "bad_request", `body must be {"ms":N} or {"min_ms":N,"max_ms":M}`)
		return
	}
	if lo < 0 || hi < lo {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "need 0 <= min_ms <= max_ms")
		return
	}
	s.state.SetDelay(time.Duration(lo)*time.Millisecond, time.Duration(hi)*time.Millisecond)
	s.log.InfoContext(r.Context(), "delay_changed", "min_ms", lo, "max_ms", hi)
	obs.WriteJSON(w, http.StatusOK, s.state.Snapshot())
}

// adminGenerate creates reports on demand. Body (all optional):
// {"count":1,"volcano_id":"merapi","alert_level":"Awas","override":{"field":value}}.
// override entries are merged into the wire format of these reports (null removes a
// field), e.g. {"alert_level":"Darurat"} simulates an unsupported enum change.
func (s *server) adminGenerate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Count      int            `json:"count"`
		VolcanoID  string         `json:"volcano_id"`
		AlertLevel string         `json:"alert_level"`
		Override   map[string]any `json:"override"`
	}
	if err := decode(r, &body); err != nil {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "invalid JSON body")
		return
	}
	if body.AlertLevel != "" && levelIndex(body.AlertLevel) < 0 {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "alert_level must be Normal|Waspada|Siaga|Awas")
		return
	}
	body.Count = min(max(body.Count, 1), 500)
	obs.WriteJSON(w, http.StatusOK, map[string]any{"generated": s.gen.Generate(body.Count, body.VolcanoID, body.AlertLevel, body.Override)})
}
