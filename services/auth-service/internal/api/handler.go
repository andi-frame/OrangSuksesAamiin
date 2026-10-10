package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/store"
	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/token"
)

type Server struct {
	Tokens *token.Service
	Store  *store.Store
	Log    *slog.Logger
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", obs.LivenessHandler("auth-service", nil))
	mux.HandleFunc("GET /ready", obs.ReadinessHandler("auth-service", map[string]obs.Check{
		"auth_db": func(ctx context.Context) error { return s.Store.Ping(ctx) },
	}))
	mux.HandleFunc("POST /v1/token", s.postToken)
	mux.HandleFunc("POST /v1/refresh", s.postRefresh)
	return obs.Middleware(s.Log, "/health", "/ready")(mux)
}

type tokenRequest struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

func (s *Server) postToken(w http.ResponseWriter, r *http.Request) {
	body, err := readJSON(r)
	if err != nil {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	var req tokenRequest
	if err := json.Unmarshal(body, &req); err != nil {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	if req.ClientID == "" || req.ClientSecret == "" {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "client_id and client_secret are required")
		return
	}

	pair, err := s.Tokens.Issue(r.Context(), req.ClientID, req.ClientSecret)
	if errors.Is(err, token.ErrInvalidClient) {
		obs.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid client credentials")
		return
	}
	if err != nil {
		s.Log.ErrorContext(r.Context(), "token_issue_failed", "error", err.Error())
		obs.WriteError(w, http.StatusInternalServerError, "internal_error", "could not issue token")
		return
	}
	obs.WriteJSON(w, http.StatusOK, pair)
}

func (s *Server) postRefresh(w http.ResponseWriter, r *http.Request) {
	body, err := readJSON(r)
	if err != nil {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	var req refreshRequest
	if err := json.Unmarshal(body, &req); err != nil {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "invalid json body")
		return
	}
	if req.RefreshToken == "" {
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "refresh_token is required")
		return
	}

	pair, err := s.Tokens.Refresh(r.Context(), req.RefreshToken)
	if errors.Is(err, token.ErrInvalidRefresh) {
		obs.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid or expired refresh token")
		return
	}
	if err != nil {
		s.Log.ErrorContext(r.Context(), "token_refresh_failed", "error", err.Error())
		obs.WriteError(w, http.StatusInternalServerError, "internal_error", "could not refresh token")
		return
	}
	obs.WriteJSON(w, http.StatusOK, pair)
}

func readJSON(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, 1<<20))
}
