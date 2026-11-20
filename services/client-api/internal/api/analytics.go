package api

import (
	_ "embed"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

// Analytics dashboard (M2 Problem 1, criterion 7). The page is static (no data
// in it); the numbers come from GET /v1/analytics/summary, which every
// authenticated role may read: they are counts over Ringkasan fields
// (hazard_type, severity, area_name, occurred_at) only.

//go:embed dashboard.html
var dashboardHTML []byte

func (s *Server) dashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'")
	_, _ = w.Write(dashboardHTML)
}

func (s *Server) analyticsSummary(w http.ResponseWriter, r *http.Request) {
	q := url.Values{}
	for _, k := range []string{"from", "to", "bucket", "ingested_before"} {
		if v := r.URL.Query().Get(k); v != "" {
			q.Set(k, v)
		}
	}
	body, code, err := s.Agg.Analytics(r.Context(), q)
	if err != nil || code == 0 {
		s.writeAggErr(w, code, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

// dashboardLogin forwards {client_id, client_secret} to the Auth Service, so the
// page can log in from its own origin (no CORS on the Auth Service needed).
func (s *Server) dashboardLogin(w http.ResponseWriter, r *http.Request) {
	s.authProxy(w, r, "/v1/token")
}

// dashboardRefresh forwards {refresh_token} (the page never keeps the secret).
func (s *Server) dashboardRefresh(w http.ResponseWriter, r *http.Request) {
	s.authProxy(w, r, "/v1/refresh")
}

func (s *Server) authProxy(w http.ResponseWriter, r *http.Request, path string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 4096))
	if err != nil {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "unreadable body")
		return
	}
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, s.AuthURL+path, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := obs.NewHTTPClient(s.Log, "auth-service", 5*time.Second).Do(req)
	if err != nil {
		obs.WriteError(w, http.StatusBadGateway, "bad_gateway", "auth service unreachable")
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, io.LimitReader(resp.Body, 1<<16))
}
