package limit_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/limit"
)

func TestShedsExcessWith429ButKeepsHealthAlive(t *testing.T) {
	h := limit.New(1, 3).Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	codes := map[int]int{}
	for i := 0; i < 20; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/hazards", nil))
		codes[rec.Code]++
		if rec.Code == http.StatusTooManyRequests && rec.Header().Get("Retry-After") == "" {
			t.Fatal("429 must carry Retry-After")
		}
	}
	if codes[http.StatusOK] < 3 || codes[http.StatusTooManyRequests] == 0 || codes[http.StatusOK]+codes[http.StatusTooManyRequests] != 20 {
		t.Fatalf("unexpected distribution (burst 3 of 20 instant requests): %v", codes)
	}
	for _, p := range []string{"/health", "/ready"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s must not be rate limited, got %d", p, rec.Code)
		}
	}
}
