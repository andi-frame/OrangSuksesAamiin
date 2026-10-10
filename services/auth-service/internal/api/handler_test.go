package api_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/api"
	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/config"
	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/store"
	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/token"
)

func testServer(t *testing.T) http.Handler {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	cfg := config.Config{
		JWTSecret:  []byte("test-secret"),
		AccessTTL:  time.Minute,
		RefreshTTL: time.Hour,
		Clients: map[string]config.Client{
			"c1": {ID: "c1", Secret: "s1", Role: "media"},
		},
	}
	srv := &api.Server{
		Tokens: token.New(cfg, st),
		Store:  st,
		Log:    slog.Default(),
	}
	return srv.Handler()
}

func TestPostTokenSuccess(t *testing.T) {
	h := testServer(t)
	body, _ := json.Marshal(map[string]string{"client_id": "c1", "client_secret": "s1"})
	req := httptest.NewRequest(http.MethodPost, "/v1/token", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var pair map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &pair); err != nil {
		t.Fatal(err)
	}
	if pair["access_token"] == "" || pair["refresh_token"] == "" {
		t.Fatal("expected token pair")
	}
}

func TestPostTokenWrongSecret(t *testing.T) {
	h := testServer(t)
	body, _ := json.Marshal(map[string]string{"client_id": "c1", "client_secret": "bad"})
	req := httptest.NewRequest(http.MethodPost, "/v1/token", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestPostRefreshRejectsEmpty(t *testing.T) {
	h := testServer(t)
	body, _ := json.Marshal(map[string]string{"refresh_token": ""})
	req := httptest.NewRequest(http.MethodPost, "/v1/refresh", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rec.Code)
	}
}
