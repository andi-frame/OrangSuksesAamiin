package ports

import (
	"context"
	"errors"
)

// ErrFenced is returned by a write that carries a fencing token older than the
// newest one the store has already seen for the same lease. The writer has
// lost leadership (it was frozen, partitioned or replaced) and must stop.
var ErrFenced = errors.New("fenced: a newer leader term has written to the store")

// Fence is the fencing token of an ingest writer (M2 Problem 2). Term is the
// lease generation handed out by the leader election (strictly increasing per
// Name); the store keeps the highest term per Name and rejects older ones, so
// a stale leader cannot write even if it still believes it is the leader.
//
// The store is the judge, not the writer's clock: safety does not depend on
// any node's wall clock or on how long a process was frozen.
type Fence struct {
	Name   string // lease name, e.g. "aggregator-ingest"
	Term   int64  // > 0
	Holder string // pod name, for the audit trail only
}

type fenceKey struct{}

// WithFence marks every repository write made with ctx as belonging to f.
func WithFence(ctx context.Context, f Fence) context.Context {
	return context.WithValue(ctx, fenceKey{}, f)
}

// FenceFrom returns the fence carried by ctx, if any. Writes without a fence
// are not checked (single-instance runs, tests, operator actions that do not
// ingest).
func FenceFrom(ctx context.Context) (Fence, bool) {
	f, ok := ctx.Value(fenceKey{}).(Fence)
	return f, ok && f.Name != "" && f.Term > 0
}
