// Package ports defines the interfaces through which the Aggregator talks to
// its infrastructure. The Aggregator owns the Canonical Store (P4): nothing but
// a Repository implementation ever touches the database, and nothing but a
// Publisher implementation ever touches the broker.
//
// Backends are pluggable (see internal/adapters): memory/log for tests and
// local dev, postgres/kafka for the real system.
package ports

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
)

var (
	// ErrNotFound is returned by Get when the hazard does not exist.
	ErrNotFound = errors.New("hazard not found")
	// ErrVersionConflict is returned by Save when the stored version differs from
	// the expected one (optimistic concurrency; also what keeps M2's multiple
	// Aggregator instances from overwriting each other).
	ErrVersionConflict = errors.New("version conflict")
)

// OutboxEntry is a broker message that must be published because a hazard was
// created or changed. It is written in the SAME transaction as the hazard
// (transactional outbox), so a stored hazard can never lose its event and a
// broker outage never blocks ingestion.
type OutboxEntry struct {
	EventID       string // "<hazard_id>:<version>" - unique, idempotency key
	HazardID      string // message key => per-hazard ordering in Kafka
	Version       int
	Payload       []byte // JSON of domain.Envelope
	CorrelationID string // flow that produced the change, forwarded as message header
	CreatedAt     time.Time
}

// ListFilter selects hazards for the internal read API.
type ListFilter struct {
	HazardType domain.HazardType // "" = any
	Source     domain.Source     // "" = any
	Severity   domain.Severity   // "" = any
	Since      *time.Time        // occurred_at >= Since
	Limit      int               // default 100, max 500
	Cursor     string            // opaque, from Page.NextCursor
}

// Normalize applies default and maximum page sizes.
func (f *ListFilter) Normalize() {
	if f.Limit <= 0 {
		f.Limit = 100
	}
	if f.Limit > 500 {
		f.Limit = 500
	}
}

// Page is one page of results ordered by (occurred_at DESC, hazard_id DESC).
type Page struct {
	Items      []domain.HazardEvent
	NextCursor string // "" when there are no more results
}

// Repository is the Aggregator's view of the Canonical Store.
type Repository interface {
	Ping(ctx context.Context) error

	// Get returns the hazard or ErrNotFound.
	Get(ctx context.Context, hazardID string) (*domain.HazardEvent, error)

	// Save persists h and, atomically, the outbox entry. expectedVersion is the
	// version currently stored (0 = the hazard must not exist yet); h.Version must
	// be expectedVersion+1. A mismatch returns ErrVersionConflict and writes
	// nothing (neither hazard nor outbox entry).
	Save(ctx context.Context, h domain.HazardEvent, expectedVersion int, out OutboxEntry) error

	// List returns hazards newest-first with keyset pagination.
	List(ctx context.Context, f ListFilter) (Page, error)

	// GetCursor / SetCursor persist the per-source polling watermark so that a
	// restarted (or rebuilt) Aggregator resumes where it left off.
	GetCursor(ctx context.Context, name string) (time.Time, bool, error)
	SetCursor(ctx context.Context, name string, t time.Time) error

	// PendingOutbox returns up to limit unpublished entries, oldest first.
	PendingOutbox(ctx context.Context, limit int) ([]OutboxEntry, error)
	// MarkPublished flags entries as delivered to the broker.
	MarkPublished(ctx context.Context, eventIDs []string) error
}

// Message is what a Publisher sends to the broker.
type Message struct {
	Key     string            // hazard_id
	Value   []byte            // JSON domain.Envelope
	Headers map[string]string // includes correlation_id
}

// Publisher hands messages to the broker. Publish must return nil only once the
// broker acknowledged the message (at-least-once).
type Publisher interface {
	Publish(ctx context.Context, msg Message) error
	Close() error
}

// EncodeCursor builds the opaque pagination token for the last returned item.
func EncodeCursor(occurredAt time.Time, hazardID string) string {
	raw := occurredAt.UTC().Format(time.RFC3339Nano) + "|" + hazardID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor parses a token produced by EncodeCursor.
func DecodeCursor(c string) (time.Time, string, error) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	ts, id, ok := strings.Cut(string(b), "|")
	if !ok {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	return t, id, nil
}
