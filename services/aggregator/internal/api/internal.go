// Package api is the Aggregator's internal read API. The Canonical Store is
// private to the Aggregator: every other component (notably the Client-Facing
// API) obtains hazards only through these endpoints, guarded by a service key.
//
// Three principals, three independent credentials:
//   - X-Service-Key: the Client-Facing API (hazards, source status, leader);
//   - Bearer <ops token>: BNPB Internal Ops (/internal/ops/*: dead letters with
//     their raw bytes, reprocessing). The service key does not open these;
//   - Bearer <audit token>: the auditor (/internal/audit/*, Lampiran A).
package api

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/leader"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/resilience"
)

// HeaderServiceKey carries the internal service credential.
const HeaderServiceKey = "X-Service-Key"

// StatusSource provides the current view of every upstream.
type StatusSource interface {
	Statuses(ctx context.Context) []resilience.Status
}

type Server struct {
	Repo       ports.Repository
	Status     StatusSource
	Leader     func() leader.Status // nil when leader election is off
	Rules      []string             // active schema rules (summary), shown to Ops
	ServiceKey string
	OpsToken   string
	AuditToken string
	Log        *slog.Logger
	Now        func() time.Time
	Version    string // build version, reported on /health
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Handler wires the routes. /health and /ready are unauthenticated (probes).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", obs.LivenessHandler("aggregator", func() map[string]any {
		return map[string]any{"version": s.Version}
	}))
	mux.HandleFunc("GET /ready", obs.ReadinessHandler("aggregator", map[string]obs.Check{
		"canonical_store": func(ctx context.Context) error { return s.Repo.Ping(ctx) },
	}))
	mux.Handle("GET /internal/hazards", s.auth(http.HandlerFunc(s.list)))
	mux.Handle("GET /internal/hazards/{id}", s.auth(http.HandlerFunc(s.get)))
	mux.Handle("GET /internal/sources/status", s.auth(http.HandlerFunc(s.status)))
	mux.Handle("GET /internal/leader", s.auth(http.HandlerFunc(s.leader)))

	mux.Handle("GET /internal/ops/unprocessed", s.bearer("ops", s.OpsToken, http.HandlerFunc(s.opsList)))
	mux.Handle("GET /internal/ops/unprocessed/{id}", s.bearer("ops", s.OpsToken, http.HandlerFunc(s.opsGet)))
	mux.Handle("POST /internal/ops/unprocessed/reprocess", s.bearer("ops", s.OpsToken, http.HandlerFunc(s.opsReprocess)))
	mux.Handle("GET /internal/ops/schema-rules", s.bearer("ops", s.OpsToken, http.HandlerFunc(s.opsRules)))

	mux.Handle("GET /internal/audit/unprocessed", s.bearer("audit", s.AuditToken, http.HandlerFunc(s.auditUnprocessed)))
	return obs.Middleware(s.Log, "/health", "/ready")(mux)
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get(HeaderServiceKey)
		if key == "" {
			obs.WriteError(w, http.StatusUnauthorized, "unauthorized", "missing "+HeaderServiceKey+" header")
			return
		}
		if subtle.ConstantTimeCompare([]byte(key), []byte(s.ServiceKey)) != 1 {
			s.Log.WarnContext(r.Context(), "auth_rejected", "reason", "invalid_service_key")
			obs.WriteError(w, http.StatusForbidden, "forbidden", "invalid service key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// bearer guards an endpoint with "Authorization: Bearer <token>": missing or
// wrong token = 401 (Lampiran A), the token of another principal = 403.
func (s *Server) bearer(principal, token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, got, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(got) == "" {
			obs.WriteError(w, http.StatusUnauthorized, "unauthorized", "missing Authorization: Bearer <"+principal+"-token>")
			return
		}
		got = strings.TrimSpace(got)
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1 {
			next.ServeHTTP(w, r)
			return
		}
		for _, other := range []string{s.OpsToken, s.AuditToken, s.ServiceKey} {
			if other != "" && subtle.ConstantTimeCompare([]byte(got), []byte(other)) == 1 {
				s.Log.WarnContext(r.Context(), "auth_rejected", "reason", "wrong_principal", "required", principal)
				obs.WriteError(w, http.StatusForbidden, "forbidden", "this credential may not access "+principal+" endpoints")
				return
			}
		}
		s.Log.WarnContext(r.Context(), "auth_rejected", "reason", "invalid_token", "required", principal)
		obs.WriteError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
	})
}

// Freshness is the per-source data freshness attached to list responses, so the
// caller can mark data as stale (Problem 2) without a second round trip.
type Freshness struct {
	State         string     `json:"state"`
	LastSuccessAt *time.Time `json:"last_success_at"`
	StaleSince    *time.Time `json:"stale_since"`
}

func (s *Server) freshness(ctx context.Context) map[string]Freshness {
	out := map[string]Freshness{}
	for _, st := range s.Status.Statuses(ctx) {
		out[st.Source] = Freshness{State: st.State, LastSuccessAt: st.LastSuccessAt, StaleSince: st.StaleSince}
	}
	return out
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := ports.ListFilter{Cursor: q.Get("cursor")}
	if v := q.Get("hazard_type"); v != "" {
		switch t := domain.HazardType(strings.ToUpper(v)); t {
		case domain.HazardSeismic, domain.HazardVolcanic:
			f.HazardType = t
		default:
			obs.WriteError(w, http.StatusBadRequest, "bad_request", "hazard_type must be SEISMIC or VOLCANIC")
			return
		}
	}
	if v := q.Get("source"); v != "" {
		switch src := domain.Source(strings.ToUpper(v)); src {
		case domain.SourceBMKG, domain.SourcePVMBG:
			f.Source = src
		default:
			obs.WriteError(w, http.StatusBadRequest, "bad_request", "source must be BMKG or PVMBG")
			return
		}
	}
	if v := q.Get("severity"); v != "" {
		sev := domain.Severity(strings.ToUpper(v))
		if sev.Rank() < 0 {
			obs.WriteError(w, http.StatusBadRequest, "bad_request", "severity must be NORMAL, WASPADA, SIAGA or AWAS")
			return
		}
		f.Severity = sev
	}
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			obs.WriteError(w, http.StatusBadRequest, "bad_request", "since must be ISO 8601 / RFC 3339")
			return
		}
		f.Since = &t
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			obs.WriteError(w, http.StatusBadRequest, "bad_request", "limit must be a positive integer")
			return
		}
		f.Limit = n
	}
	page, err := s.Repo.List(r.Context(), f)
	if err != nil {
		if f.Cursor != "" && err.Error() == "invalid cursor" {
			obs.WriteError(w, http.StatusBadRequest, "bad_request", "invalid cursor")
			return
		}
		s.Log.ErrorContext(r.Context(), "list_failed", "error", err.Error())
		obs.WriteError(w, http.StatusServiceUnavailable, "store_unavailable", "canonical store unavailable")
		return
	}
	items := page.Items
	if items == nil {
		items = []domain.HazardEvent{}
	}
	obs.WriteJSON(w, http.StatusOK, map[string]any{
		"items": items, "next_cursor": page.NextCursor, "freshness": s.freshness(r.Context()),
	})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	h, err := s.Repo.Get(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, ports.ErrNotFound):
		obs.WriteError(w, http.StatusNotFound, "not_found", "hazard not found")
	case err != nil:
		s.Log.ErrorContext(r.Context(), "get_failed", "error", err.Error())
		obs.WriteError(w, http.StatusServiceUnavailable, "store_unavailable", "canonical store unavailable")
	default:
		obs.WriteJSON(w, http.StatusOK, h)
	}
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	body := map[string]any{"sources": s.Status.Statuses(r.Context())}
	if c, err := s.Repo.CountUnprocessed(r.Context()); err == nil {
		body["unprocessed"] = c // counts only: the messages themselves are for Internal Ops
	}
	obs.WriteJSON(w, http.StatusOK, body)
}

func (s *Server) leader(w http.ResponseWriter, r *http.Request) {
	if s.Leader == nil {
		obs.WriteJSON(w, http.StatusOK, map[string]any{"election": "none", "leader": true})
		return
	}
	obs.WriteJSON(w, http.StatusOK, map[string]any{"election": "kubernetes", "instance": s.Leader()})
}
