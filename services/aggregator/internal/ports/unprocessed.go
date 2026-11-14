package ports

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"
)

// Status of an unprocessed (dead-letter) message.
const (
	StatusRetrying = "retrying" // will be tried again automatically (backoff)
	StatusParked   = "parked"   // waits for a change to the system, then an operator requeue
	StatusResolved = "resolved" // processed successfully later; kept for the audit trail
)

// Unprocessed is one message the Aggregator received but could not turn into
// stored state (M2 Problem 3). Raw holds the exact bytes received, so nothing
// is guessed or lost: the message can be inspected and processed again after
// the system changes.
type Unprocessed struct {
	MessageID     string    // deterministic: same bytes from the same source => same id (re-fetches do not duplicate)
	Seq           int64     // insertion order, assigned by the store
	Source        string    // BMKG | PVMBG | ...
	Kind          string    // what Raw is, e.g. "pvmbg.volcanic-report" (selects the reprocessor)
	RecordKey     string    // source_ref_id when it could be read; "" otherwise (per-key ordering)
	ReceivedAt    time.Time // first time the message was seen
	Reason        string    // why it failed the last time
	Retryable     bool      // true: may succeed without a system change
	Attempts      int
	Status        string // StatusRetrying | StatusParked | StatusResolved
	NextAttemptAt time.Time
	ResolvedAt    *time.Time
	CorrelationID string
	Raw           []byte
	RawSize       int64 // size of the original message (> len(Raw) only when it was too large to keep whole)
}

// UnprocessedFilter selects dead-letter messages. Statuses empty = retrying and parked.
type UnprocessedFilter struct {
	Source    string
	Kind      string
	RecordKey string
	Statuses  []string
	DueBefore *time.Time // only retrying messages whose NextAttemptAt <= DueBefore
	Limit     int        // default 100, max 1000
	Cursor    string     // opaque, from the previous page
}

// Normalize applies the default status set and page sizes.
func (f *UnprocessedFilter) Normalize() {
	if len(f.Statuses) == 0 {
		f.Statuses = []string{StatusRetrying, StatusParked}
	}
	if f.Limit <= 0 {
		f.Limit = 100
	}
	if f.Limit > 1000 {
		f.Limit = 1000
	}
}

// UnprocessedPage is one page ordered by Seq ascending (= arrival order).
type UnprocessedPage struct {
	Items      []Unprocessed
	NextCursor string // "" on the last page
}

// EncodeSeqCursor / DecodeSeqCursor build the opaque dead-letter page token.
func EncodeSeqCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte("u|" + strconv.FormatInt(seq, 10)))
}

func DecodeSeqCursor(c string) (int64, error) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil || len(b) < 3 || string(b[:2]) != "u|" {
		return 0, fmt.Errorf("invalid cursor")
	}
	n, err := strconv.ParseInt(string(b[2:]), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid cursor")
	}
	return n, nil
}

// UnprocessedCounts is the number of open messages per source and status.
type UnprocessedCounts map[string]map[string]int64
