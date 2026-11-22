package limit_test

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/limit"
)

type roleKey struct{}

func roleOf(ctx context.Context) string { r, _ := ctx.Value(roleKey{}).(string); return r }

// Under a Media flood that exhausts the rate, Tim Lapangan is still admitted
// (as long as Lapangan's own load fits in the reserved share).
func TestMediaFloodDoesNotStarveLapangan(t *testing.T) {
	p := limit.NewPriority(10, 100, 0.3, 1000, roleOf, "lapangan", "pusat")
	media, lapangan := map[int]int{}, map[int]int{}
	for i := 0; i < 400; i++ {
		_, code := admit(p, "media")
		media[code]++
		if i%20 == 0 {
			_, c := admit(p, "lapangan")
			lapangan[c]++
		}
	}
	if media[http.StatusTooManyRequests] == 0 {
		t.Fatalf("media must be shed under overload: %v", media)
	}
	if lapangan[http.StatusOK] != 20 {
		t.Fatalf("every Lapangan request must be served while Media is shed: %v", lapangan)
	}
}

// When requests pile up (slow upstream), Media is shed first.
func TestConcurrencyReserveKeepsSlotsForLapangan(t *testing.T) {
	p := limit.NewPriority(1_000_000, 1_000_000, 0.25, 8, roleOf, "lapangan")
	var releases []func()
	for {
		rel, code := p.Admit("media")
		if code != http.StatusOK {
			if code != http.StatusTooManyRequests {
				t.Fatalf("media shed must be 429, got %d", code)
			}
			break
		}
		releases = append(releases, rel)
	}
	if len(releases) != 6 { // 75 % of 8
		t.Fatalf("media may hold 6 of 8 slots, held %d", len(releases))
	}
	for range 2 {
		if _, code := p.Admit("lapangan"); code != http.StatusOK {
			t.Fatalf("reserved slots must go to lapangan, got %d", code)
		}
	}
	if _, code := p.Admit("lapangan"); code != http.StatusTooManyRequests {
		t.Fatalf("no capacity at all: a controlled 429 for high priority too, got %d", code)
	}
}

func TestMiddlewareAnswers429WithRetryAfter(t *testing.T) {
	p := limit.NewPriority(1, 1, 0.5, 10, roleOf, "lapangan")
	h := p.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	var mu sync.Mutex
	codes := map[int]int{}
	for range 5 {
		rec := newRec()
		req, _ := http.NewRequestWithContext(context.WithValue(context.Background(), roleKey{}, "media"), http.MethodGet, "/v1/hazards", nil)
		h.ServeHTTP(rec, req)
		mu.Lock()
		codes[rec.code]++
		mu.Unlock()
		if rec.code == 429 && rec.Header().Get("Retry-After") == "" {
			t.Fatal("429 must carry Retry-After")
		}
	}
	if codes[429] == 0 {
		t.Fatalf("expected sheds: %v", codes)
	}
}

func admit(p *limit.Priority, role string) (func(), int) {
	rel, code := p.Admit(role)
	if rel != nil {
		rel()
	}
	return rel, code
}

type rec struct {
	h    http.Header
	code int
}

func newRec() *rec                         { return &rec{h: http.Header{}, code: 200} }
func (r *rec) Header() http.Header         { return r.h }
func (r *rec) Write(b []byte) (int, error) { return len(b), nil }
func (r *rec) WriteHeader(c int)           { r.code = c }
