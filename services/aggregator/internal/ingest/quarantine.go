package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

// QuarantineEntry documents one source record that could not be mapped.
type QuarantineEntry struct {
	At            time.Time       `json:"at"`
	Source        string          `json:"source"`
	RecordID      string          `json:"record_id"`
	Field         string          `json:"field"`
	Reason        string          `json:"reason"`
	CorrelationID string          `json:"correlation_id"`
	Raw           json.RawMessage `json:"raw"`
}

// Quarantine keeps the most recent rejected records (bounded ring) and counts
// all of them. It is deliberately in-memory: it is a diagnostic aid, not a
// store of record (documented limitation).
type Quarantine struct {
	log *slog.Logger
	max int

	mu      sync.Mutex
	entries []QuarantineEntry
	total   map[string]int64
	seen    map[string]time.Time // de-duplication of re-fetched rejected records
}

func NewQuarantine(log *slog.Logger, max int) *Quarantine {
	return &Quarantine{log: log, max: max, total: map[string]int64{}, seen: map[string]time.Time{}}
}

// dedupeTTL is how long a rejected record is remembered. The polling overlap
// window re-delivers a record for a few seconds after it was first seen; without
// this it would be rejected, counted and logged again on every cycle.
const dedupeTTL = 15 * time.Minute

// Add records a rejected record and logs it at WARN. It returns true only the
// first time a given record is rejected for a given reason; repeats caused by
// re-fetching (overlap window) are ignored and return false.
func (q *Quarantine) Add(ctx context.Context, source, recordID, field, reason string, raw any) bool {
	b, _ := json.Marshal(raw)
	key := source + "|" + recordID + "|" + field + "|" + reason
	if recordID == "" { // nothing to identify it by: the content is its identity
		key += "|" + string(b)
	}
	if len(b) > 2048 {
		b, _ = json.Marshal(map[string]any{"truncated": true, "prefix": string(b[:2048])})
	}
	now := time.Now().UTC()

	q.mu.Lock()
	if at, dup := q.seen[key]; dup && now.Sub(at) < dedupeTTL {
		q.mu.Unlock()
		return false
	}
	if len(q.seen) > 5000 { // bound memory: forget expired keys
		for k, at := range q.seen {
			if now.Sub(at) >= dedupeTTL {
				delete(q.seen, k)
			}
		}
	}
	q.seen[key] = now
	q.entries = append(q.entries, QuarantineEntry{
		At: now, Source: source, RecordID: recordID, Field: field, Reason: reason,
		CorrelationID: obs.CorrelationID(ctx), Raw: b,
	})
	if len(q.entries) > q.max {
		q.entries = q.entries[len(q.entries)-q.max:]
	}
	q.total[source]++
	q.mu.Unlock()
	q.log.WarnContext(ctx, "mapping_rejected", "source", source, "record_id", recordID, "field", field, "reason", reason)
	return true
}

// Recent returns the retained entries, newest first.
func (q *Quarantine) Recent() []QuarantineEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]QuarantineEntry, len(q.entries))
	for i, e := range q.entries {
		out[len(q.entries)-1-i] = e
	}
	return out
}

// Total returns the number of records rejected per source since start.
func (q *Quarantine) Total() map[string]int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[string]int64, len(q.total))
	for k, v := range q.total {
		out[k] = v
	}
	return out
}
