package poller

import (
	"context"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// Cursor names persisted through ports.Repository.
const (
	CursorBMKGEvents   = "bmkg.seismic-events"
	CursorBMKGWarnings = "bmkg.tsunami-warnings"
	CursorPVMBG        = "pvmbg.volcanic-reports"
)

// sinceFor returns the `since` value for the next request: the persisted
// watermark minus a small overlap (records that appear slightly out of order
// are not missed; the duplicates are harmless because upsert is idempotent).
// The zero time means "no watermark yet": ask for everything.
func sinceFor(ctx context.Context, repo ports.Repository, name string, overlap time.Duration) (time.Time, error) {
	c, ok, err := repo.GetCursor(ctx, name)
	if err != nil || !ok {
		return time.Time{}, err
	}
	return c.Add(-overlap), nil
}

// advance stores max(current, candidate) as the new watermark.
func advance(ctx context.Context, repo ports.Repository, name string, candidate time.Time) error {
	if candidate.IsZero() {
		return nil
	}
	cur, ok, err := repo.GetCursor(ctx, name)
	if err != nil {
		return err
	}
	if ok && !candidate.After(cur) {
		return nil
	}
	return repo.SetCursor(ctx, name, candidate)
}

func parseTime(v any) (time.Time, bool) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// reject quarantines a record and counts it in the cycle stats - once: a record
// re-delivered by the overlap window is not counted (or logged) again.
func reject(ctx context.Context, q *ingest.Quarantine, st *Stats, source, recordID, field, reason string, raw any) {
	if q.Add(ctx, source, recordID, field, reason, raw) {
		st.Quarantined++
	}
}
