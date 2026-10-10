package auth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/auth"
)

func TestMiddlewareMissingToken(t *testing.T) {
	secret := []byte("test-secret")
	h := auth.Middleware(secret)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/hazards", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d want 401", rec.Code)
	}
}

func TestMiddlewareInvalidToken(t *testing.T) {
	secret := []byte("test-secret")
	h := auth.Middleware(secret)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/hazards", nil)
	req.Header.Set("Authorization", "Bearer not.valid.jwt")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d want 401", rec.Code)
	}
	if rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatal("a 401 must carry a WWW-Authenticate challenge")
	}
}
