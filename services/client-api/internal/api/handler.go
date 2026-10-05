package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/aggregator"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/auth"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/policy"
)

type Server struct {
	Agg *aggregator.Client
	Log *slog.Logger
}

func (s *Server) Handler(jwtSecret []byte) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", obs.LivenessHandler("client-api", nil))
	mux.HandleFunc("GET /ready", obs.LivenessHandler("client-api", nil))

	secured := auth.Middleware(jwtSecret)
	mux.Handle("GET /v1/hazards", secured(http.HandlerFunc(s.listHazards)))
	mux.Handle("GET /v1/hazards/{id}", secured(http.HandlerFunc(s.getHazard)))
	mux.Handle("GET /v1/sources/status", secured(http.HandlerFunc(s.sourcesStatus)))

	return mux
}

func (s *Server) listHazards(w http.ResponseWriter, r *http.Request) {
	role := auth.Role(r.Context())
	if err := policy.CheckQuery(role, r.URL.Query().Get("fields"), r.URL.Query().Get("include")); err != nil {
		obs.WriteError(w, http.StatusForbidden, "forbidden", "field not allowed for this client role")
		return
	}

	page, err := s.Agg.List(r.Context(), cloneQuery(r.URL.Query()))
	if err != nil {
		s.writeAggErr(w, page.StatusCode, err)
		return
	}

	items := make([]map[string]any, 0, len(page.Items))
	for _, raw := range page.Items {
		src := sourceOf(raw)
		stale := isSourceStale(page.Freshness, src)
		item, err := policy.FilterItem(role, raw, stale)
		if err != nil {
			s.Log.WarnContext(r.Context(), "filter_item_failed", "error", err.Error())
			continue
		}
		items = append(items, item)
	}

	obs.WriteJSON(w, http.StatusOK, map[string]any{
		"items": items, "next_cursor": page.NextCursor, "freshness": page.Freshness,
	})
}

func (s *Server) getHazard(w http.ResponseWriter, r *http.Request) {
	role := auth.Role(r.Context())
	if err := policy.CheckQuery(role, r.URL.Query().Get("fields"), r.URL.Query().Get("include")); err != nil {
		obs.WriteError(w, http.StatusForbidden, "forbidden", "field not allowed for this client role")
		return
	}

	body, code, err := s.Agg.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeAggErr(w, code, err)
		return
	}
	if code != http.StatusOK {
		s.writeAggErr(w, code, err)
		return
	}

	stale := s.sourceStale(r.Context(), sourceOf(body))
	item, err := policy.FilterItem(role, body, stale)
	if err != nil {
		obs.WriteError(w, http.StatusInternalServerError, "internal_error", "could not shape response")
		return
	}
	obs.WriteJSON(w, http.StatusOK, item)
}

func (s *Server) sourcesStatus(w http.ResponseWriter, r *http.Request) {
	role := auth.Role(r.Context())
	if !policy.CanSourcesStatus(role) {
		obs.WriteError(w, http.StatusForbidden, "forbidden", "endpoint not allowed for this client role")
		return
	}
	body, code, err := s.Agg.SourcesStatus(r.Context())
	if err != nil {
		s.writeAggErr(w, code, err)
		return
	}
	if code != http.StatusOK {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write(body)
		return
	}
	var v any
	_ = json.Unmarshal(body, &v)
	obs.WriteJSON(w, http.StatusOK, v)
}

func (s *Server) sourceStale(ctx context.Context, source string) bool {
	if source == "" {
		return false
	}
	body, code, err := s.Agg.SourcesStatus(ctx)
	if err != nil || code != http.StatusOK {
		return false
	}
	var wrap struct {
		Sources []struct {
			Source     string  `json:"source"`
			StaleSince *string `json:"stale_since"`
		} `json:"sources"`
	}
	if json.Unmarshal(body, &wrap) != nil {
		return false
	}
	for _, row := range wrap.Sources {
		if row.Source == source && row.StaleSince != nil {
			return true
		}
	}
	return false
}

func (s *Server) writeAggErr(w http.ResponseWriter, code int, err error) {
	if code == http.StatusServiceUnavailable {
		obs.WriteError(w, http.StatusServiceUnavailable, "upstream_unavailable", "data temporarily unavailable")
		return
	}
	if code == http.StatusNotFound {
		obs.WriteError(w, http.StatusNotFound, "not_found", "hazard not found")
		return
	}
	if code == 0 {
		obs.WriteError(w, http.StatusBadGateway, "bad_gateway", "upstream aggregator unreachable")
		return
	}
	obs.WriteError(w, http.StatusBadGateway, "bad_gateway", "upstream aggregator error")
}

func cloneQuery(q url.Values) url.Values {
	out := url.Values{}
	for k, vs := range q {
		for _, v := range vs {
			out.Add(k, v)
		}
	}
	return out
}

func sourceOf(raw json.RawMessage) string {
	var m struct {
		Source string `json:"source"`
	}
	_ = json.Unmarshal(raw, &m)
	return m.Source
}

func isSourceStale(fresh map[string]aggregator.Freshness, source string) bool {
	f, ok := fresh[source]
	if !ok {
		return false
	}
	return f.StaleSince != nil
}
