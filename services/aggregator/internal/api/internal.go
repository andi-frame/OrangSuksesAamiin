// Package api is the Aggregator's internal read API. The Canonical Store is
// private to the Aggregator: every other component (notably the Client-Facing
// API) obtains hazards only through these endpoints, guarded by a service key.
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
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/resilience"
)

// HeaderServiceKey carries the internal service credential.
const HeaderServiceKey = "X-Service-Key"

type Server struct {
	Repo       ports.Repository
	Trackers   []*resilience.Tracker
	Quarantine *ingest.Quarantine
	ServiceKey string
	Log        *slog.Logger
}

// Handler wires the routes. /health and /ready are unauthenticated (probes).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", obs.LivenessHandler("aggregator", nil))
	mux.HandleFunc("GET /ready", obs.ReadinessHandler("aggregator", map[string]obs.Check{
		"canonical_store": func(ctx context.Context) error { return s.Repo.Ping(ctx) },
	}))
	mux.Handle("GET /internal/hazards", s.auth(http.HandlerFunc(s.list)))
	mux.Handle("GET /internal/hazards/{id}", s.auth(http.HandlerFunc(s.get)))
	mux.Handle("GET /internal/sources/status", s.auth(http.HandlerFunc(s.status)))
	mux.Handle("GET /internal/quarantine", s.auth(http.HandlerFunc(s.quarantine)))
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

// Freshness is the per-source data freshness attached to list responses, so the
// caller can mark data as stale (Problem 2) without a second round trip.
type Freshness struct {
	State         string     `json:"state"`
	LastSuccessAt *time.Time `json:"last_success_at"`
	StaleSince    *time.Time `json:"stale_since"`
}

func (s *Server) statuses() []resilience.Status {
	out := make([]resilience.Status, 0, len(s.Trackers))
	for _, t := range s.Trackers {
		out = append(out, t.Snapshot())
	}
	return out
}

func (s *Server) freshness() map[string]Freshness {
	out := map[string]Freshness{}
	for _, st := range s.statuses() {
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
		"items": items, "next_cursor": page.NextCursor, "freshness": s.freshness(),
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
	obs.WriteJSON(w, http.StatusOK, map[string]any{"sources": s.statuses(), "quarantine_total": s.Quarantine.Total()})
}

func (s *Server) quarantine(w http.ResponseWriter, r *http.Request) {
	obs.WriteJSON(w, http.StatusOK, map[string]any{"total": s.Quarantine.Total(), "recent": s.Quarantine.Recent()})
}
