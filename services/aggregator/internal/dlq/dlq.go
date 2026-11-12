// Package dlq keeps every message the Aggregator could not process, with its
// exact bytes, reason, source and time, and processes it again (M2 Problem 3).
//
// Two kinds of failure are treated differently:
//
//   - retryable (status "retrying"): the message is fine but something it
//     needs is not there yet - the store write failed, the seismic event a
//     tsunami warning refers to has not arrived, the response was cut off in
//     transit and the source will deliver it again. The Worker retries it with
//     exponential backoff and parks it when the budget is spent.
//   - not retryable (status "parked"): the bytes themselves cannot be read
//     (malformed JSON, invalid UTF-8, too deep, too large) or they break the
//     contract (renamed field, changed type, missing required field). Retrying
//     cannot help. The message waits until the system is changed (schema rules,
//     new version) and BNPB Internal Ops requeues it.
//
// Order: messages about the same record (Source + RecordKey) are processed in
// arrival order. While an older one is open, a newer one waits behind it, so a
// recovered old message can never overwrite newer data.
package dlq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// Failure is a classified processing failure.
type Failure struct {
	Reason    string
	Retryable bool
}

func (f *Failure) Error() string { return f.Reason }

// Permanent is a failure that only a change to the system can fix.
func Permanent(format string, args ...any) *Failure {
	return &Failure{Reason: fmt.Sprintf(format, args...)}
}

// Transient is a failure that may go away by itself.
func Transient(format string, args ...any) *Failure {
	return &Failure{Reason: fmt.Sprintf(format, args...), Retryable: true}
}

// Classify turns any processing error into a Failure. Errors that are not
// already classified come from our own infrastructure (store timeout,
// unreachable shard) and are retryable.
func Classify(err error) *Failure {
	var f *Failure
	if errors.As(err, &f) {
		return f
	}
	return Transient("store_error: %v", err)
}

// MessageID is deterministic: the same bytes of the same kind from the same
// source always get the same id, so a re-fetched message is never stored twice.
func MessageID(source, kind string, raw []byte) string {
	h := sha256.New()
	h.Write([]byte(source + "\x00" + kind + "\x00"))
	h.Write(raw)
	return "msg-" + hex.EncodeToString(h.Sum(nil))[:20]
}

// Queue writes to the dead-letter store.
type Queue struct {
	Repo        ports.Repository
	Log         *slog.Logger
	Now         func() time.Time
	MaxAttempts int           // retryable failures before parking; default 8
	BaseBackoff time.Duration // default 2 s, doubled per attempt
	MaxBackoff  time.Duration // default 2 min
}

func (q *Queue) now() time.Time {
	if q.Now != nil {
		return q.Now().UTC()
	}
	return time.Now().UTC()
}

func (q *Queue) maxAttempts() int {
	if q.MaxAttempts <= 0 {
		return 8
	}
	return q.MaxAttempts
}

// Backoff is the wait before attempt n+1.
func (q *Queue) Backoff(attempts int) time.Duration {
	base, maxB := q.BaseBackoff, q.MaxBackoff
	if base <= 0 {
		base = 2 * time.Second
	}
	if maxB <= 0 {
		maxB = 2 * time.Minute
	}
	d := base
	for i := 1; i < attempts && d < maxB; i++ {
		d *= 2
	}
	return min(d, maxB)
}

// Message is what a caller hands to Add.
type Message struct {
	Source    string
	Kind      string
	RecordKey string
	Raw       []byte
	Size      int64 // original size when Raw is only a prefix; 0 = len(Raw)
}

// Add stores a message that just failed for the first time. It returns the
// message id and whether it is new (false: the same bytes were already there).
func (q *Queue) Add(ctx context.Context, m Message, f *Failure) (string, bool, error) {
	now := q.now()
	u := ports.Unprocessed{
		MessageID: MessageID(m.Source, m.Kind, m.Raw), Source: m.Source, Kind: m.Kind, RecordKey: m.RecordKey,
		ReceivedAt: now, Reason: f.Reason, Retryable: f.Retryable, Attempts: 1,
		CorrelationID: obs.CorrelationID(ctx), Raw: m.Raw, RawSize: m.Size,
	}
	if f.Retryable {
		u.Status, u.NextAttemptAt = ports.StatusRetrying, now.Add(q.Backoff(1))
	} else {
		u.Status, u.NextAttemptAt = ports.StatusParked, now
	}
	created, err := q.Repo.AddUnprocessed(ctx, u)
	if err != nil {
		return "", false, err
	}
	if created {
		q.Log.WarnContext(ctx, "message_unprocessed", "message_id", u.MessageID, "source", u.Source, "kind", u.Kind,
			"record_key", u.RecordKey, "status", u.Status, "retryable", u.Retryable, "reason", u.Reason, "bytes", len(m.Raw))
	}
	return u.MessageID, created, nil
}

// OpenAhead returns the oldest open message about the same record (same
// source, kind and record key), if any. A new message for that record must
// wait behind it. Fragments have a kind of their own and never block.
func (q *Queue) OpenAhead(ctx context.Context, source, kind, key string) (*ports.Unprocessed, error) {
	if key == "" {
		return nil, nil
	}
	p, err := q.Repo.ListUnprocessed(ctx, ports.UnprocessedFilter{Source: source, Kind: kind, RecordKey: key, Limit: 1})
	if err != nil || len(p.Items) == 0 {
		return nil, err
	}
	return &p.Items[0], nil
}

// Handler processes one dead-letter message again. nil = processed.
type Handler func(ctx context.Context, m ports.Unprocessed) error

// Worker retries due messages. It runs only on the leader (its writes are fenced).
type Worker struct {
	Queue    *Queue
	Handlers map[string]Handler // by Kind
	Interval time.Duration      // default 1 s
	Batch    int                // default 100
	Log      *slog.Logger
}

// Run blocks until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	iv := w.Interval
	if iv <= 0 {
		iv = time.Second
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		if _, err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
			if errors.Is(err, ports.ErrFenced) {
				w.Log.WarnContext(ctx, "dlq_worker_fenced")
				return
			}
			w.Log.WarnContext(ctx, "dlq_worker_failed", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Result of one pass.
type Result struct {
	Resolved, Retrying, Parked, Blocked int
}

// RunOnce processes every message that is due, oldest first.
func (w *Worker) RunOnce(ctx context.Context) (Result, error) {
	var res Result
	batch := w.Batch
	if batch <= 0 {
		batch = 100
	}
	now := w.Queue.now()
	page, err := w.Queue.Repo.ListUnprocessed(ctx, ports.UnprocessedFilter{DueBefore: &now, Limit: batch})
	if err != nil {
		return res, err
	}
	for _, m := range page.Items {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		ahead, err := w.Queue.OpenAhead(ctx, m.Source, m.Kind, m.RecordKey)
		if err != nil {
			return res, err
		}
		if ahead != nil && ahead.MessageID != m.MessageID {
			res.Blocked++ // an older message about the same record goes first
			continue
		}
		if err := w.process(ctx, m, &res); err != nil {
			return res, err
		}
	}
	return res, nil
}

func (w *Worker) process(ctx context.Context, m ports.Unprocessed, res *Result) error {
	mctx := obs.WithCorrelationID(ctx, obs.NewCorrelationID())
	h, ok := w.Handlers[m.Kind]
	var perr error
	if !ok {
		perr = Permanent("no handler for kind %q", m.Kind)
	} else {
		perr = h(mctx, m)
	}
	if errors.Is(perr, ports.ErrFenced) {
		return perr
	}
	now := w.Queue.now()
	m.Attempts++
	if perr == nil {
		m.Status, m.ResolvedAt, m.NextAttemptAt = ports.StatusResolved, &now, now
		if err := w.Queue.Repo.UpdateUnprocessed(mctx, m); err != nil {
			return err
		}
		res.Resolved++
		w.Log.InfoContext(mctx, "message_recovered", "message_id", m.MessageID, "source", m.Source, "kind", m.Kind,
			"record_key", m.RecordKey, "attempts", m.Attempts, "original_cid", m.CorrelationID)
		return nil
	}
	f := Classify(perr)
	// bytes that were already found unreadable stay described by the first, more precise diagnosis
	// (e.g. "too_large" rather than "unexpected end of JSON input" for the kept prefix)
	if !(strings.HasPrefix(m.Reason, "malformed:") && strings.HasPrefix(f.Reason, "malformed:")) {
		m.Reason = f.Reason
	}
	m.Retryable = f.Retryable
	switch {
	case !f.Retryable:
		m.Status, m.NextAttemptAt = ports.StatusParked, now
		res.Parked++
	case m.Attempts >= w.Queue.maxAttempts():
		m.Status, m.NextAttemptAt, m.Reason = ports.StatusParked, now, "retry_exhausted: "+f.Reason
		res.Parked++
	default:
		m.Status, m.NextAttemptAt = ports.StatusRetrying, now.Add(w.Queue.Backoff(m.Attempts))
		res.Retrying++
	}
	if err := w.Queue.Repo.UpdateUnprocessed(mctx, m); err != nil {
		return err
	}
	w.Log.InfoContext(mctx, "message_retry_failed", "message_id", m.MessageID, "source", m.Source, "kind", m.Kind,
		"status", m.Status, "attempts", m.Attempts, "reason", m.Reason)
	return nil
}
