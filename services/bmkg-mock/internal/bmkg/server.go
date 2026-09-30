package bmkg

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

// HeaderKey is the BMKG credential header required by the contract.
const HeaderKey = "X-BMKG-Key"

// HeaderAdmin guards the operator-only /admin endpoints (a permitted addition).
const HeaderAdmin = "X-Admin-Token"

type server struct {
	cfg   Config
	store *Store
	gen   *Generator
	log   *slog.Logger
}

// NewHandler wires the BMKG mock routes.
func NewHandler(cfg Config, store *Store, gen *Generator, log *slog.Logger) http.Handler {
	s := &server{cfg: cfg, store: store, gen: gen, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", obs.LivenessHandler("bmkg-mock", func() map[string]any {
		e, w := store.Counts()
		return map[string]any{"events": e, "warnings": w}
	}))
	mux.HandleFunc("GET /ready", obs.LivenessHandler("bmkg-mock", nil))
	mux.Handle("GET /seismic-events", s.withKey(http.HandlerFunc(s.seismicEvents)))
	mux.Handle("GET /tsunami-warnings", s.withKey(http.HandlerFunc(s.tsunamiWarnings)))
	mux.Handle("POST /admin/generate", s.withAdmin(http.HandlerFunc(s.adminGenerate)))
	return obs.Middleware(log, "/health", "/ready")(mux)
}

func tokenEqual(got, want string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// withKey enforces X-BMKG-Key. A missing credential is 401; a wrong one, or one
// that visibly belongs to another agency (PVMBG's Authorization: Bearer), is 403.
func (s *server) withKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get(HeaderKey)
		switch {
		case key == "" && r.Header.Get("Authorization") != "":
			s.log.WarnContext(r.Context(), "auth_rejected", "reason", "foreign_credential")
			obs.WriteError(w, http.StatusForbidden, "forbidden", "credential does not belong to BMKG")
		case key == "":
			s.log.WarnContext(r.Context(), "auth_rejected", "reason", "missing_key")
			obs.WriteError(w, http.StatusUnauthorized, "unauthorized", "missing "+HeaderKey+" header")
		case !tokenEqual(key, s.cfg.APIKey):
			s.log.WarnContext(r.Context(), "auth_rejected", "reason", "invalid_key")
			obs.WriteError(w, http.StatusForbidden, "forbidden", "invalid BMKG API key")
		default:
			next.ServeHTTP(w, r)
		}
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

// delay reproduces BMKG's low, consistent latency (contract: fixed 50-150 ms).
// It returns false when the client went away first.
func (s *server) delay(ctx context.Context) bool {
	d := s.cfg.DelayMin
	if span := s.cfg.DelayMax - s.cfg.DelayMin; span > 0 {
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

func parseSince(r *http.Request) (time.Time, error) {
	v := r.URL.Query().Get("since")
	if v == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, v)
}

func (s *server) seismicEvents(w http.ResponseWriter, r *http.Request) {
	since, err := parseSince(r)
	if err != nil {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "since must be ISO 8601 / RFC 3339")
		return
	}
	if !s.delay(r.Context()) {
		return
	}
	obs.WriteJSON(w, http.StatusOK, s.store.EventsSince(since))
}

func (s *server) tsunamiWarnings(w http.ResponseWriter, r *http.Request) {
	since, err := parseSince(r)
	if err != nil {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "since must be ISO 8601 / RFC 3339")
		return
	}
	if !s.delay(r.Context()) {
		return
	}
	obs.WriteJSON(w, http.StatusOK, s.store.WarningsSince(since))
}

// adminGenerate creates events on demand, so a demo does not have to wait for
// the 10-second ticker. Body (all optional): {"count":1,"magnitude":7.6,"tsunami":true}.
func (s *server) adminGenerate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Count     int     `json:"count"`
		Magnitude float64 `json:"magnitude"`
		Tsunami   *bool   `json:"tsunami"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			obs.WriteError(w, http.StatusBadRequest, "bad_request", "invalid JSON body")
			return
		}
	}
	if body.Count <= 0 {
		body.Count = 1
	}
	if body.Count > 500 {
		body.Count = 500
	}
	events := s.gen.Generate(body.Count, body.Magnitude, body.Tsunami)
	obs.WriteJSON(w, http.StatusOK, map[string]any{"generated": events})
}
