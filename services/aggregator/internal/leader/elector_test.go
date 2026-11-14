package leader

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/memory"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// memStore is a lease store with compare-and-swap on a version counter, like
// the Kubernetes API server.
type memStore struct {
	mu  sync.Mutex
	rec *Record
	ver int
}

func (s *memStore) Get(context.Context) (Record, string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rec == nil {
		return Record{}, "", false, nil
	}
	return *s.rec, strconv.Itoa(s.ver), true, nil
}

func (s *memStore) Create(_ context.Context, r Record) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rec != nil {
		return "", ErrConflict
	}
	s.rec, s.ver = &r, s.ver+1
	return strconv.Itoa(s.ver), nil
}

func (s *memStore) Update(_ context.Context, r Record, v string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rec == nil || v != strconv.Itoa(s.ver) {
		return "", ErrConflict
	}
	s.rec, s.ver = &r, s.ver+1
	return strconv.Itoa(s.ver), nil
}

// clock is a per-instance clock that the test moves by hand.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type candidate struct {
	*Elector
	clk   *clock
	terms chan int64
	ctxs  chan context.Context
}

func newCandidate(id string, st Store, start time.Time) *candidate {
	c := &candidate{clk: &clock{t: start}, terms: make(chan int64, 8), ctxs: make(chan context.Context, 8)}
	c.Elector = &Elector{
		Store: st, Identity: id, LeaseName: "aggregator-ingest",
		LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: c.clk.Now,
		OnLead: func(ctx context.Context, term int64) {
			c.terms <- term
			c.ctxs <- ctx
			<-ctx.Done()
		},
	}
	return c
}

func (c *candidate) leadCtx(t *testing.T) (int64, context.Context) {
	t.Helper()
	select {
	case term := <-c.terms:
		return term, <-c.ctxs
	case <-time.After(2 * time.Second):
		t.Fatalf("%s never started leading", c.Identity)
	}
	return 0, nil
}

// step moves every clock by d and lets every listed candidate run one tick.
func step(ctx context.Context, d time.Duration, cs ...*candidate) {
	for _, c := range cs {
		c.clk.Add(d)
		c.Tick(ctx)
	}
}

func waitDone(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("leadership context was not cancelled")
	}
}

var t0 = time.Date(2026, 11, 14, 8, 0, 0, 0, time.UTC)

func TestOnlyOneLeaderAndTakeoverAfterLeaseDurationWithHigherTerm(t *testing.T) {
	ctx := context.Background()
	st := &memStore{}
	a, b := newCandidate("agg-a", st, t0), newCandidate("agg-b", st, t0.Add(time.Hour)) // b's clock is an hour off

	a.Tick(ctx)
	term, aCtx := a.leadCtx(t)
	if term != 1 {
		t.Fatalf("first term = %d", term)
	}
	if f, ok := ports.FenceFrom(aCtx); !ok || f.Term != 1 || f.Holder != "agg-a" || f.Name != "aggregator-ingest" {
		t.Fatalf("leadership ctx must carry the fencing token: %+v", f)
	}

	// a renews every 2 s; b keeps seeing a new version and never takes over
	for i := range 20 {
		step(ctx, 2*time.Second, a, b)
		if b.Status().Leader {
			t.Fatalf("b took over a live leader at step %d", i)
		}
	}

	// a dies (stops ticking). b waits LeaseDuration on its own clock, then takes over.
	waited := time.Duration(0)
	for !b.Status().Leader {
		step(ctx, 2*time.Second, b)
		waited += 2 * time.Second
		if waited > time.Minute {
			t.Fatal("b never took over")
		}
	}
	if waited <= 15*time.Second {
		t.Fatalf("b took over after %v, before the lease duration", waited)
	}
	if term, _ = b.leadCtx(t); term != 2 {
		t.Fatalf("takeover term = %d, want 2", term)
	}
}

// The lease's timestamps are written with the holder's wall clock. A holder
// whose clock is +45 s or -30 s off must not make others take over early:
// decisions use only "has the object changed", timed on the observer's clock.
func TestSkewedHolderClockDoesNotCauseTakeover(t *testing.T) {
	ctx := context.Background()
	for _, skew := range []time.Duration{45 * time.Second, -30 * time.Second, -24 * time.Hour} {
		st := &memStore{}
		a, b := newCandidate("agg-a", st, t0.Add(skew)), newCandidate("agg-b", st, t0)
		a.Tick(ctx)
		a.leadCtx(t)
		for range 10 {
			step(ctx, 2*time.Second, a, b)
			if b.Status().Leader {
				t.Fatalf("skew %v: b took over a live leader", skew)
			}
		}
	}
}

// A clock that jumps on the leader's node (+45 s, then -30 s) between two
// renewals must not make the leader give up or anyone else take over.
func TestClockJumpOnLeaderNodeKeepsSingleLeader(t *testing.T) {
	ctx := context.Background()
	st := &memStore{}
	a, b := newCandidate("agg-a", st, t0), newCandidate("agg-b", st, t0)
	a.Tick(ctx)
	_, aCtx := a.leadCtx(t)
	step(ctx, 2*time.Second, a, b)
	a.clk.Add(45 * time.Second) // only a's clock jumps
	step(ctx, 2*time.Second, a, b)
	a.clk.Add(-30 * time.Second)
	for range 5 {
		step(ctx, 2*time.Second, a, b)
	}
	if !a.Status().Leader || b.Status().Leader || aCtx.Err() != nil {
		t.Fatalf("leadership changed because of a clock jump: a=%+v b=%+v", a.Status(), b.Status())
	}
}

// The freeze scenario: the leader is suspended longer than the lease, b takes
// over and raises the fence, a wakes up still believing it leads. Its writes
// are refused by the store and Confirm makes it step down before it polls.
func TestFrozenLeaderIsFencedAfterWakingUp(t *testing.T) {
	ctx := context.Background()
	st := &memStore{}
	repo := memory.New()
	a, b := newCandidate("agg-a", st, t0), newCandidate("agg-b", st, t0)

	a.Tick(ctx)
	_, aCtx := a.leadCtx(t)
	if err := repo.RaiseFence(ctx, mustFence(t, aCtx)); err != nil {
		t.Fatal(err)
	}
	h := domain.HazardEvent{HazardID: "bmkg-1", Source: domain.SourceBMKG, SourceRefID: "1",
		HazardType: domain.HazardSeismic, Severity: domain.SeverityWaspada, AreaName: "x", OccurredAt: t0, Version: 1}
	if err := repo.Save(aCtx, h, 0, ports.OutboxEntry{EventID: "bmkg-1:1", HazardID: "bmkg-1", Version: 1}); err != nil {
		t.Fatal(err)
	}

	// a freezes: neither its clock nor its loop moves. b takes over.
	for !b.Status().Leader {
		step(ctx, 2*time.Second, b)
	}
	_, bCtx := b.leadCtx(t)
	if err := repo.RaiseFence(ctx, mustFence(t, bCtx)); err != nil {
		t.Fatal(err)
	}

	// a wakes up. Before its elector notices anything, a write is attempted.
	h2 := h
	h2.HazardID, h2.SourceRefID = "bmkg-2", "2"
	err := repo.Save(aCtx, h2, 0, ports.OutboxEntry{EventID: "bmkg-2:1", HazardID: "bmkg-2", Version: 1})
	if !errors.Is(err, ports.ErrFenced) {
		t.Fatalf("woken leader's write: want ErrFenced, got %v", err)
	}
	if _, err := repo.Get(ctx, "bmkg-2"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatal("a fenced write left data behind")
	}
	// the gate before every poll re-reads the lease and steps down
	if err := a.Confirm(ctx); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Confirm after takeover: %v", err)
	}
	waitDone(t, aCtx)
	if a.Status().Leader {
		t.Fatal("a still claims leadership")
	}
	if err := b.Confirm(ctx); err != nil {
		t.Fatalf("new leader Confirm: %v", err)
	}
}

func mustFence(t *testing.T, ctx context.Context) ports.Fence {
	t.Helper()
	f, ok := ports.FenceFrom(ctx)
	if !ok {
		t.Fatal("no fence in leadership context")
	}
	return f
}

// A leader that cannot reach the API server gives up after RenewDeadline, before
// anyone else may take over (LeaseDuration), so the two never overlap.
func TestLeaderStepsDownWhenItCannotRenew(t *testing.T) {
	ctx := context.Background()
	st := &flakyStore{memStore: &memStore{}}
	a := newCandidate("agg-a", st, t0)
	a.Tick(ctx)
	_, aCtx := a.leadCtx(t)
	st.fail = true
	for range 4 {
		step(ctx, 2*time.Second, a)
	}
	if !a.Status().Leader {
		t.Fatal("stepped down before the renew deadline")
	}
	for range 2 {
		step(ctx, 2*time.Second, a)
	}
	waitDone(t, aCtx)
	if a.Status().Leader {
		t.Fatal("still leader after the renew deadline")
	}
}

type flakyStore struct {
	*memStore
	fail bool
}

func (s *flakyStore) Get(ctx context.Context) (Record, string, bool, error) {
	if s.fail {
		return Record{}, "", false, errors.New("api server unreachable")
	}
	return s.memStore.Get(ctx)
}

// Shutdown hands the lease back: the successor does not wait LeaseDuration.
func TestShutdownReleasesLeaseForImmediateTakeover(t *testing.T) {
	st := &memStore{}
	a, b := newCandidate("agg-a", st, t0), newCandidate("agg-b", st, t0)
	actx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.Run(actx); close(done) }()
	_, aLead := a.leadCtx(t)
	cancel()
	<-done
	waitDone(t, aLead)
	b.Tick(context.Background())
	if term, _ := b.leadCtx(t); term != 2 {
		t.Fatalf("takeover after release: term %d", term)
	}
}

// The store already holds a newer term than the lease (e.g. the Lease object
// was deleted and recreated). OnLead gives up when the fence is refused; the
// elector then re-acquires with ever higher terms until it passes the fence.
func TestLeaderClimbsPastAStoreFenceAfterLeaseReset(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()
	if err := repo.RaiseFence(ctx, ports.Fence{Name: "aggregator-ingest", Term: 3}); err != nil {
		t.Fatal(err)
	}
	st := &memStore{}
	a := newCandidate("agg-a", st, t0)
	accepted := make(chan int64, 1)
	a.OnLead = func(lctx context.Context, term int64) {
		f, _ := ports.FenceFrom(lctx)
		if repo.RaiseFence(lctx, f) != nil {
			return // fenced: give up this term
		}
		accepted <- term
		<-lctx.Done()
	}
	for range 10 {
		a.Tick(ctx)
		select {
		case term := <-accepted:
			if term != 3 {
				t.Fatalf("accepted term %d, want 3", term)
			}
			return
		case <-time.After(50 * time.Millisecond):
		}
		step(ctx, 2*time.Second)
	}
	t.Fatal("never got past the fence")
}
