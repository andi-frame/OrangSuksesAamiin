// Package memory is the in-memory Repository used for unit tests and for
// running the Aggregator without a database. Data is stored as JSON bytes to
// mimic a document/JSONB store (types survive a round-trip exactly like they
// would in PostgreSQL), and it implements the same outbox semantics.
package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

func init() {
	adapters.RegisterRepository("memory", func(context.Context, *slog.Logger) (ports.Repository, error) {
		return New(), nil
	})
}

type outboxRow struct {
	ports.OutboxEntry
	published bool
}

// Repo is a thread-safe in-memory ports.Repository.
type Repo struct {
	mu        sync.Mutex
	hazards   map[string][]byte
	cursors   map[string]time.Time
	outbox    []*outboxRow
	fences    map[string]int64
	dlq       map[string]*ports.Unprocessed
	dlqSeq    int64
	status    map[string]statusRow
	satellite map[string][]byte
	failing   error // test hook: makes every call fail
}

func New() *Repo {
	return &Repo{hazards: map[string][]byte{}, cursors: map[string]time.Time{},
		fences: map[string]int64{}, dlq: map[string]*ports.Unprocessed{}, status: map[string]statusRow{}, satellite: map[string][]byte{}}
}

// checkFenceLocked enforces the fence carried by ctx: an older term than the
// newest one seen is rejected, a newer one becomes the newest.
func (r *Repo) checkFenceLocked(ctx context.Context) error {
	f, ok := ports.FenceFrom(ctx)
	if !ok {
		return nil
	}
	if f.Term < r.fences[f.Name] {
		return ports.ErrFenced
	}
	r.fences[f.Name] = f.Term
	return nil
}

// Fail makes every subsequent call return err (nil to recover). Test hook.
func (r *Repo) Fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failing = err
}

func (r *Repo) Ping(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failing
}

func (r *Repo) Get(_ context.Context, id string) (*domain.HazardEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return nil, r.failing
	}
	return r.getLocked(id)
}

func (r *Repo) getLocked(id string) (*domain.HazardEvent, error) {
	b, ok := r.hazards[id]
	if !ok {
		return nil, ports.ErrNotFound
	}
	var h domain.HazardEvent
	if err := json.Unmarshal(b, &h); err != nil {
		return nil, err
	}
	return &h, nil
}

func (r *Repo) Save(ctx context.Context, h domain.HazardEvent, expected int, out ports.OutboxEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return r.failing
	}
	if err := r.checkFenceLocked(ctx); err != nil {
		return err
	}
	cur := 0
	if old, err := r.getLocked(h.HazardID); err == nil {
		cur = old.Version
	}
	if cur != expected || h.Version != expected+1 {
		return ports.ErrVersionConflict
	}
	b, err := json.Marshal(h)
	if err != nil {
		return err
	}
	r.hazards[h.HazardID] = b
	if out.CreatedAt.IsZero() {
		out.CreatedAt = time.Now().UTC()
	}
	r.outbox = append(r.outbox, &outboxRow{OutboxEntry: out})
	return nil
}

func (r *Repo) List(_ context.Context, f ports.ListFilter) (ports.Page, error) {
	f.Normalize()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return ports.Page{}, r.failing
	}
	var all []domain.HazardEvent
	for id := range r.hazards {
		h, _ := r.getLocked(id)
		if h.Retired {
			continue
		}
		if f.HazardType != "" && h.HazardType != f.HazardType {
			continue
		}
		if f.Source != "" && h.Source != f.Source {
			continue
		}
		if f.Severity != "" && h.Severity != f.Severity {
			continue
		}
		if f.Since != nil && f.Position(*h).Before(*f.Since) {
			continue
		}
		all = append(all, *h)
	}
	sort.Slice(all, func(i, j int) bool {
		return f.Before(all[i], all[j])
	})
	if f.Cursor != "" {
		ct, cid, err := ports.DecodeCursor(f.Cursor)
		if err != nil {
			return ports.Page{}, err
		}
		i := sort.Search(len(all), func(i int) bool { // first item strictly "after" the cursor
			return f.Before(domain.HazardEvent{HazardID: cid, OccurredAt: ct, IngestedAt: ct}, all[i])
		})
		all = all[i:]
	}
	page := ports.Page{}
	if len(all) > f.Limit {
		all = all[:f.Limit]
		last := all[len(all)-1]
		page.NextCursor = ports.EncodeCursor(f.Position(last), last.HazardID)
	}
	page.Items = all
	return page, nil
}

func (r *Repo) GetCursor(_ context.Context, name string) (time.Time, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return time.Time{}, false, r.failing
	}
	t, ok := r.cursors[name]
	return t, ok, nil
}

func (r *Repo) SetCursor(ctx context.Context, name string, t time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return r.failing
	}
	if err := r.checkFenceLocked(ctx); err != nil {
		return err
	}
	r.cursors[name] = t.UTC()
	return nil
}

func (r *Repo) PendingOutbox(_ context.Context, limit int) ([]ports.OutboxEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return nil, r.failing
	}
	var out []ports.OutboxEntry
	for _, row := range r.outbox {
		if !row.published {
			out = append(out, row.OutboxEntry)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (r *Repo) MarkPublished(ctx context.Context, ids []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return r.failing
	}
	if err := r.checkFenceLocked(ctx); err != nil {
		return err
	}
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	for _, row := range r.outbox {
		if set[row.EventID] {
			row.published = true
		}
	}
	return nil
}

func (r *Repo) LoadSatellite(_ context.Context, key string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return bytes.Clone(r.satellite[key]), r.failing
}

func (r *Repo) StoreSatellite(ctx context.Context, key string, body []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return r.failing
	}
	if err := r.checkFenceLocked(ctx); err != nil {
		return err
	}
	r.satellite[key] = bytes.Clone(body)
	return nil
}

func (r *Repo) AuditReferences(_ context.Context, source domain.Source) ([]string, []string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing != nil {
		return nil, nil, r.failing
	}
	var refs []string
	for id := range r.hazards {
		h, err := r.getLocked(id)
		if err != nil {
			return nil, nil, err
		}
		if !h.Retired && (source == "" || h.Source == source) {
			refs = append(refs, h.SourceRefID)
		}
	}
	return refs, nil, nil
}
