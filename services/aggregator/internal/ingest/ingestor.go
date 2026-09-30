// Package ingest turns mapped hazards into stored state plus outbox events.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// Outcome of one Upsert.
type Outcome string

const (
	Created   Outcome = "created"
	Updated   Outcome = "updated"
	Unchanged Outcome = "unchanged"
)

// Ingestor persists hazards and their outbox events.
type Ingestor struct {
	Repo ports.Repository
	Log  *slog.Logger
	Now  func() time.Time
}

func New(repo ports.Repository, log *slog.Logger) *Ingestor {
	return &Ingestor{Repo: repo, Log: log, Now: time.Now}
}

// PrepareFunc lets the caller adjust the freshly mapped hazard using what is
// already stored (e.g. carry a tsunami warning over) before it is compared.
type PrepareFunc func(existing *domain.HazardEvent, fresh *domain.HazardEvent)

// Upsert stores h idempotently:
//   - unknown hazard_id                -> version 1, event "created"
//   - same content as stored           -> nothing written (re-polled overlap window)
//   - changed content                  -> version+1, event "updated"
//
// The hazard and its outbox entry are written atomically by the repository.
func (i *Ingestor) Upsert(ctx context.Context, h *domain.HazardEvent, prepare PrepareFunc) (Outcome, error) {
	for attempt := 0; attempt < 3; attempt++ {
		existing, err := i.Repo.Get(ctx, h.HazardID)
		if err != nil && !errors.Is(err, ports.ErrNotFound) {
			return "", fmt.Errorf("get %s: %w", h.HazardID, err)
		}
		if errors.Is(err, ports.ErrNotFound) {
			existing = nil
		}
		next := *h
		if prepare != nil {
			prepare(existing, &next)
		}
		if existing != nil {
			// additive-only: never destroy what was captured before (see mergeAttributes)
			next.Attributes = mergeAttributes(existing.Attributes, next.Attributes)
		}

		now := i.Now().UTC().Truncate(time.Microsecond)
		var kind string
		var expected int
		switch {
		case existing == nil:
			kind, expected = domain.KindCreated, 0
			next.Version, next.IngestedAt, next.UpdatedAt = 1, ifZero(next.IngestedAt, now), now
		case domain.SameContent(*existing, next):
			return Unchanged, nil
		default:
			kind, expected = domain.KindUpdated, existing.Version
			next.Version, next.IngestedAt, next.UpdatedAt = existing.Version+1, existing.IngestedAt, now
		}

		env := domain.Envelope{
			EventID: domain.EventID(next.HazardID, next.Version), Kind: kind,
			EmittedAt: now, Hazard: next, SchemaName: domain.SchemaName,
		}
		payload, err := json.Marshal(env)
		if err != nil {
			return "", err
		}
		entry := ports.OutboxEntry{
			EventID: env.EventID, HazardID: next.HazardID, Version: next.Version,
			Payload: payload, CorrelationID: obs.CorrelationID(ctx), CreatedAt: now,
		}
		err = i.Repo.Save(ctx, next, expected, entry)
		if errors.Is(err, ports.ErrVersionConflict) {
			continue // someone else wrote in between: re-read and decide again
		}
		if err != nil {
			return "", fmt.Errorf("save %s: %w", h.HazardID, err)
		}
		*h = next
		i.Log.InfoContext(ctx, "hazard_stored", "hazard_id", next.HazardID, "version", next.Version,
			"outcome", kind, "severity", next.Severity, "source", next.Source)
		return Outcome(kind), nil
	}
	return "", fmt.Errorf("save %s: too many version conflicts", h.HazardID)
}

func ifZero(t, def time.Time) time.Time {
	if t.IsZero() {
		return def
	}
	return t
}

// mergeAttributes returns old overlaid with incoming: values the source sends now
// win, but a field it stops sending stays on the record. This is the storage half
// of the additive-only schema policy (Problem 1): a rolled-back schema, a partial
// response or a re-poll inside the overlap window can never erase information that
// was already captured. The cost: a field the source removes on purpose lingers on
// that one record (new records simply do not have it) - documented as a limitation.
func mergeAttributes(old, incoming map[string]any) map[string]any {
	out := make(map[string]any, len(old)+len(incoming))
	for k, v := range old {
		out[k] = v
	}
	for k, v := range incoming {
		out[k] = v
	}
	return out
}
