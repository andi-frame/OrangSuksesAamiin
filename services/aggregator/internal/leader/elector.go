// Package leader elects the one Aggregator instance that ingests (M2 Problem 2).
//
// Several instances serve reads, but only the leader polls the upstream
// agencies, relays the outbox and retries dead letters, so every source is
// fetched once and stored once.
//
// Time. No decision compares wall clocks of different machines:
//   - the lease lives in one place (a Kubernetes Lease object in etcd, ordered
//     by its resourceVersion), not on any node;
//   - a candidate declares the lease expired only after the lease object has
//     not changed for LeaseDuration, measured on the candidate's own monotonic
//     clock from the moment it first saw that version. A wall clock that jumps
//     forward or backward (NTP off, +45 s / -30 s) changes neither side;
//   - the leader gives up by itself when it could not renew for RenewDeadline
//     (< LeaseDuration), again on its own monotonic clock.
//
// Safety does not rest on time at all. A frozen leader (VM suspended, laptop
// lid closed) does not notice that time passed. Each leadership has a term (the
// lease's transition count, strictly increasing); writes carry it as a fencing
// token (ports.Fence) and the store rejects a term older than one it has seen.
// The new leader raises the fence before its first write, so whatever the woken
// leader tries afterwards is refused, and Confirm re-reads the lease before
// every poll so it does not even call the upstream again.
package leader

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// ErrConflict is returned by Store.Create/Update when someone else changed the
// lease first (optimistic concurrency on the object's version).
var ErrConflict = errors.New("lease changed concurrently")

// ErrNotLeader is returned by Confirm when this instance does not hold the lease.
var ErrNotLeader = errors.New("not the leader")

// Record is the content of the lease.
type Record struct {
	Holder      string // "" = released
	DurationSec int
	AcquireTime time.Time // informational (holder's wall clock); never used for decisions
	RenewTime   time.Time // idem
	Transitions int64     // term: +1 every time the holder changes
}

// Store is where the lease lives. Version is an opaque optimistic-concurrency token.
type Store interface {
	Get(ctx context.Context) (rec Record, version string, found bool, err error)
	Create(ctx context.Context, rec Record) (version string, err error)
	Update(ctx context.Context, rec Record, version string) (newVersion string, err error)
}

// Status is a snapshot for /internal/leader, logs and metrics.
type Status struct {
	Identity       string     `json:"identity"`
	Leader         bool       `json:"leader"`
	Term           int64      `json:"term"`
	ObservedHolder string     `json:"observed_holder"`
	ObservedTerm   int64      `json:"observed_term"`
	LeaderSince    *time.Time `json:"leader_since,omitempty"`
	LeaseName      string     `json:"lease_name"`
}

// Elector runs the election loop. Zero durations get safe defaults.
type Elector struct {
	Store         Store
	Identity      string // pod name
	LeaseName     string // also the fence name in the store
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
	Log           *slog.Logger
	Now           func() time.Time // must carry a monotonic reading (time.Now does)

	// OnLead runs while this instance leads. Its ctx carries the fencing token
	// (ports.WithFence) and is cancelled when leadership ends; OnLead must return
	// promptly after that. It is never running twice at the same time.
	OnLead func(ctx context.Context, term int64)

	mu          sync.Mutex
	leading     bool
	term        int64
	since       time.Time
	lastRenew   time.Time // monotonic
	observedVer string
	observedAt  time.Time // monotonic, when observedVer was first seen
	observed    Record
	cancelLead  context.CancelFunc
	leadDone    chan struct{}
}

func (e *Elector) defaults() {
	if e.LeaseDuration <= 0 {
		e.LeaseDuration = 15 * time.Second
	}
	if e.RenewDeadline <= 0 || e.RenewDeadline >= e.LeaseDuration {
		e.RenewDeadline = e.LeaseDuration * 2 / 3
	}
	if e.RetryPeriod <= 0 {
		e.RetryPeriod = 2 * time.Second
	}
	if e.Now == nil {
		e.Now = time.Now
	}
}

// Run blocks until ctx is cancelled, then stops leading and releases the
// lease so a successor takes over within one RetryPeriod (rolling update).
func (e *Elector) Run(ctx context.Context) {
	e.defaults()
	e.Log.Info("leader_election_started", "identity", e.Identity, "lease", e.LeaseName,
		"lease_duration_s", e.LeaseDuration.Seconds(), "renew_deadline_s", e.RenewDeadline.Seconds(),
		"retry_period_s", e.RetryPeriod.Seconds())
	t := time.NewTicker(e.RetryPeriod)
	defer t.Stop()
	for {
		e.Tick(ctx)
		select {
		case <-ctx.Done():
			e.release()
			return
		case <-t.C:
		}
	}
}

// Tick performs one election step. Exported for tests.
func (e *Elector) Tick(ctx context.Context) {
	e.defaults()
	tctx, cancel := context.WithTimeout(ctx, e.RetryPeriod+e.RenewDeadline/2)
	defer cancel()
	tctx = obs.WithCorrelationID(tctx, "lease-"+e.Identity)

	rec, ver, found, err := e.Store.Get(tctx)
	now := e.Now()
	if err != nil {
		e.Log.WarnContext(tctx, "lease_read_failed", "error", err.Error())
		e.checkDeadline(now)
		return
	}
	if !found {
		if !e.canAcquire() {
			return
		}
		nr := Record{Holder: e.Identity, DurationSec: int(e.LeaseDuration.Seconds()), AcquireTime: now, RenewTime: now, Transitions: 1}
		if nv, err := e.Store.Create(tctx, nr); err == nil {
			e.observe(nr, nv, now)
			e.becomeLeader(ctx, 1, now)
		} else if !errors.Is(err, ErrConflict) {
			e.Log.WarnContext(tctx, "lease_create_failed", "error", err.Error())
		}
		return
	}
	e.observe(rec, ver, now)

	e.mu.Lock()
	leading, term := e.leading, e.term
	e.mu.Unlock()

	if leading && rec.Holder == e.Identity && rec.Transitions == term {
		rec.RenewTime = now
		nv, err := e.Store.Update(tctx, rec, ver)
		if err != nil {
			e.Log.WarnContext(tctx, "lease_renew_failed", "term", term, "error", err.Error())
			e.checkDeadline(now)
			return
		}
		e.mu.Lock()
		e.lastRenew = now
		e.mu.Unlock()
		e.observe(rec, nv, now)
		return
	}
	if leading { // the lease names someone else or another term: we lost it
		e.stepDown("lease_taken", rec)
		return
	}
	if !e.canAcquire() {
		return
	}
	e.mu.Lock()
	expired := rec.Holder == "" || rec.Holder == e.Identity || now.Sub(e.observedAt) > e.LeaseDuration
	e.mu.Unlock()
	if !expired {
		return
	}
	nr := Record{Holder: e.Identity, DurationSec: int(e.LeaseDuration.Seconds()), AcquireTime: now, RenewTime: now,
		Transitions: rec.Transitions + 1}
	nv, err := e.Store.Update(tctx, nr, ver)
	if err != nil {
		if !errors.Is(err, ErrConflict) {
			e.Log.WarnContext(tctx, "lease_acquire_failed", "error", err.Error())
		}
		return
	}
	e.Log.InfoContext(tctx, "lease_taken_over", "previous_holder", rec.Holder, "previous_term", rec.Transitions,
		"term", nr.Transitions)
	e.observe(nr, nv, now)
	e.becomeLeader(ctx, nr.Transitions, now)
}

func (e *Elector) observe(rec Record, ver string, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if ver != e.observedVer {
		e.observedVer, e.observedAt = ver, now
	}
	e.observed = rec
}

// canAcquire is false while the previous OnLead is still winding down.
func (e *Elector) canAcquire() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.leadDone == nil {
		return true
	}
	select {
	case <-e.leadDone:
		e.leadDone = nil
		return true
	default:
		return false
	}
}

func (e *Elector) checkDeadline(now time.Time) {
	e.mu.Lock()
	expired := e.leading && now.Sub(e.lastRenew) > e.RenewDeadline
	e.mu.Unlock()
	if expired {
		e.stepDown("renew_deadline_exceeded", Record{})
	}
}

func (e *Elector) becomeLeader(parent context.Context, term int64, now time.Time) {
	fence := ports.Fence{Name: e.LeaseName, Term: term, Holder: e.Identity}
	lctx, cancel := context.WithCancel(ports.WithFence(parent, fence))
	done := make(chan struct{})
	e.mu.Lock()
	e.leading, e.term, e.lastRenew, e.since = true, term, now, time.Now().UTC()
	e.cancelLead, e.leadDone = cancel, done
	e.mu.Unlock()
	e.Log.Info("leader_acquired", "identity", e.Identity, "term", term, "lease", e.LeaseName)
	go func() {
		defer close(done)
		if e.OnLead != nil {
			e.OnLead(lctx, term)
		}
		// OnLead gave up on its own (e.g. the store holds a newer term): stop
		// leading; the next tick acquires again with a higher term.
		e.mu.Lock()
		still := e.leading && e.term == term && parent.Err() == nil
		e.mu.Unlock()
		if still {
			e.stepDown("on_lead_returned", Record{})
		}
	}()
}

func (e *Elector) stepDown(reason string, seen Record) {
	e.mu.Lock()
	if !e.leading {
		e.mu.Unlock()
		return
	}
	term, cancel := e.term, e.cancelLead
	e.leading, e.cancelLead = false, nil
	e.mu.Unlock()
	cancel()
	e.Log.Warn("leader_lost", "identity", e.Identity, "term", term, "reason", reason,
		"lease_holder", seen.Holder, "lease_term", seen.Transitions)
}

// release ends leadership on shutdown and hands the lease back.
func (e *Elector) release() {
	e.mu.Lock()
	term, done := e.term, e.leadDone
	e.mu.Unlock()
	if term == 0 {
		return // never led
	}
	e.stepDown("shutdown", Record{})
	if done != nil {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	}
	// The shutdown may already have ended OnLead (its context derives from the
	// cancelled one), so do not rely on e.leading: release whatever lease still
	// names this instance.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rec, ver, found, err := e.Store.Get(ctx)
	if err != nil || !found || rec.Holder != e.Identity {
		return
	}
	rec.Holder, rec.DurationSec, rec.RenewTime = "", 1, e.Now()
	if _, err := e.Store.Update(ctx, rec, ver); err != nil {
		e.Log.Warn("lease_release_failed", "error", err.Error())
		return
	}
	e.Log.Info("lease_released", "identity", e.Identity, "term", rec.Transitions)
}

// Confirm re-reads the lease and returns nil only if this instance still holds
// it with the same term and renewed it recently. The leader calls it before
// every upstream request; a leader that was frozen and woke up learns here
// that it was replaced (and steps down) before it calls anyone.
func (e *Elector) Confirm(ctx context.Context) error {
	e.mu.Lock()
	leading, term, last := e.leading, e.term, e.lastRenew
	e.mu.Unlock()
	if !leading {
		return ErrNotLeader
	}
	if e.Now().Sub(last) > e.RenewDeadline {
		e.stepDown("renew_deadline_exceeded", Record{})
		return ErrNotLeader
	}
	rec, _, found, err := e.Store.Get(ctx)
	if err != nil {
		return err
	}
	if !found || rec.Holder != e.Identity || rec.Transitions != term {
		e.stepDown("lease_taken", rec)
		return ErrNotLeader
	}
	return nil
}

// Status returns a snapshot.
func (e *Elector) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := Status{Identity: e.Identity, Leader: e.leading, Term: e.term, LeaseName: e.LeaseName,
		ObservedHolder: e.observed.Holder, ObservedTerm: e.observed.Transitions}
	if e.leading {
		t := e.since
		s.LeaderSince = &t
	} else {
		s.Term = 0
	}
	return s
}
