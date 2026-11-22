// Package sharded routes canonical records across replicated PostgreSQL groups.
package sharded

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/postgres"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

const deadline = 1200 * time.Millisecond

type unit interface {
	ports.Repository
	ports.SatelliteStore
	ports.AuditReferences
}

type shard struct {
	name    string
	unit    unit
	members []*postgres.Repo
}

type Repo struct {
	unit   // Shard zero owns shared metadata, including the satellite journal.
	shards []shard
}

func init() {
	adapters.RegisterRepository("sharded", func(ctx context.Context, _ *slog.Logger) (ports.Repository, error) {
		return Open(ctx, os.Getenv("CANONICAL_SHARD_URLS"), os.Getenv("CANONICAL_MEMBER_HOSTS"))
	})
}

func Open(ctx context.Context, urls, members string) (*Repo, error) {
	if urls == "" {
		return nil, errors.New("CANONICAL_SHARD_URLS is required")
	}
	var shards []shard
	var pools []*postgres.Repo
	success := false
	defer func() {
		if !success {
			for _, pool := range pools {
				pool.Close()
			}
		}
	}()
	groups := strings.Split(members, ";")
	for i, url := range strings.Split(urls, ",") {
		primary, err := postgres.OpenShard(ctx, url)
		if err != nil {
			return nil, fmt.Errorf("invalid shard %d configuration", i)
		}
		pools = append(pools, primary)
		s := shard{name: fmt.Sprintf("shard-%d", i), unit: primary}
		if members != "" && i < len(groups) {
			for _, host := range strings.Split(groups[i], ",") {
				member, err := postgres.OpenMember(ctx, url, strings.TrimSpace(host))
				if err != nil {
					return nil, fmt.Errorf("invalid member of %s", s.name)
				}
				pools = append(pools, member)
				s.members = append(s.members, member)
			}
		}
		shards = append(shards, s)
	}
	r := &Repo{unit: shards[0].unit, shards: shards}
	if err := r.Ping(ctx); err != nil {
		return nil, err
	}
	success = true
	return r, nil
}

// Index keeps routing stable for a fixed shard count. Physical replacements
// keep their logical shard number and therefore require no key reassignment.
func Index(id string, count int) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum64() % uint64(count))
}

func (r *Repo) owner(id string) *shard {
	return &r.shards[Index(id, len(r.shards))]
}

func fanout[T any](ctx context.Context, shards []shard, fn func(context.Context, shard) T) []T {
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	out := make([]T, len(shards))
	var wg sync.WaitGroup
	for i, s := range shards {
		wg.Go(func() { out[i] = fn(ctx, s) })
	}
	wg.Wait()
	return out
}

func (r *Repo) Ping(ctx context.Context) error {
	errs := fanout(ctx, r.shards, func(ctx context.Context, s shard) error { return s.unit.Ping(ctx) })
	for _, err := range errs {
		if err == nil {
			return nil
		}
	}
	return errors.New("no canonical shard reachable")
}

func (r *Repo) Get(ctx context.Context, id string) (*domain.HazardEvent, error) {
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	return r.owner(id).unit.Get(ctx, id)
}

// shardWrites shows how the write load spreads over the shards (P1 criterion 2); together with the shard
// placement (kubectl get pods -L shard,db-role) it gives the write load per node.
var shardWrites = obs.NewCounter("aggregator_shard_writes_total",
	"HazardEvent writes routed to each shard, by outcome (ok, error).", "shard", "outcome")

func (r *Repo) Save(ctx context.Context, h domain.HazardEvent, expected int, row ports.OutboxEntry) error {
	s := r.owner(h.HazardID)
	err := s.unit.Save(ctx, h, expected, row)
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	shardWrites.Inc(s.name, outcome)
	return err
}

func (r *Repo) RaiseFence(ctx context.Context, fence ports.Fence) error {
	errs := fanout(ctx, r.shards, func(ctx context.Context, s shard) error {
		return s.unit.RaiseFence(ctx, fence)
	})
	return errors.Join(errs...)
}

func (r *Repo) List(ctx context.Context, f ports.ListFilter) (ports.Page, error) {
	f.Normalize()
	if f.Cursor != "" {
		if _, _, err := ports.DecodeCursor(f.Cursor); err != nil {
			return ports.Page{}, err
		}
	}
	type result struct {
		page    ports.Page
		err     error
		missing []string
	}
	pages := fanout(ctx, r.shards, func(ctx context.Context, s shard) result {
		p, err := s.unit.List(ctx, f)
		res := result{page: p, err: err}
		if err == nil {
			res.missing = s.unreachableMembers(ctx)
		}
		return res
	})
	out := ports.Page{}
	more := false
	for i, p := range pages {
		if p.err != nil {
			out.UnavailableUnits = append(out.UnavailableUnits, r.shards[i].name)
			continue
		}
		out.Items = append(out.Items, p.page.Items...)
		out.UnavailableUnits = append(out.UnavailableUnits, p.missing...)
		more = more || p.page.NextCursor != ""
	}
	slices.SortFunc(out.Items, func(a, b domain.HazardEvent) int {
		if f.Before(a, b) {
			return -1
		}
		if f.Before(b, a) {
			return 1
		}
		return 0
	})
	if len(out.Items) > f.Limit {
		out.Items = out.Items[:f.Limit]
		more = true
	}
	slices.Sort(out.UnavailableUnits)
	if more && len(out.Items) > 0 {
		last := out.Items[len(out.Items)-1]
		out.NextCursor = ports.EncodeCursor(f.Position(last), last.HazardID)
	}
	return out, nil
}

func (r *Repo) PendingOutbox(ctx context.Context, limit int) ([]ports.OutboxEntry, error) {
	type result struct {
		rows []ports.OutboxEntry
		err  error
	}
	pages := fanout(ctx, r.shards, func(ctx context.Context, s shard) result {
		rows, err := s.unit.PendingOutbox(ctx, limit)
		return result{rows, err}
	})
	var out []ports.OutboxEntry
	if limit <= 0 {
		return out, nil
	}
	for len(out) < limit {
		next := -1
		for i, page := range pages {
			if page.err != nil || len(page.rows) == 0 {
				continue
			}
			// equal timestamps (coarse clocks, e.g. Windows) are ordered by event ID, so the merge is deterministic
			a, b := page.rows[0], pages[max(next, 0)].rows
			if next == -1 || a.CreatedAt.Before(b[0].CreatedAt) ||
				(a.CreatedAt.Equal(b[0].CreatedAt) && a.EventID < b[0].EventID) {
				next = i
			}
		}
		if next == -1 {
			break
		}
		out = append(out, pages[next].rows[0])
		pages[next].rows = pages[next].rows[1:]
	}
	if len(out) == 0 {
		for _, p := range pages {
			if p.err != nil {
				return nil, p.err
			}
		}
	}
	return out, nil
}

func (r *Repo) MarkPublished(ctx context.Context, ids []string) error {
	byShard := make([][]string, len(r.shards))
	for _, id := range ids {
		split := strings.LastIndexByte(id, ':')
		if split < 1 {
			return fmt.Errorf("invalid outbox ID %q", id)
		}
		i := Index(id[:split], len(r.shards))
		byShard[i] = append(byShard[i], id)
	}
	var errs []error
	for i, batch := range byShard {
		if len(batch) > 0 {
			errs = append(errs, r.shards[i].unit.MarkPublished(ctx, batch))
		}
	}
	return errors.Join(errs...)
}

func (r *Repo) AuditReferences(ctx context.Context, source domain.Source) ([]string, []string, error) {
	type result struct{ refs, unavailable []string }
	snapshots := fanout(ctx, r.shards, func(ctx context.Context, s shard) result {
		refs, _, err := s.unit.AuditReferences(ctx, source)
		if err != nil {
			return result{unavailable: []string{s.name}}
		}
		return result{refs: refs, unavailable: s.unreachableMembers(ctx)}
	})
	var refs, unavailable []string
	for _, snapshot := range snapshots {
		refs = append(refs, snapshot.refs...)
		unavailable = append(unavailable, snapshot.unavailable...)
	}
	return refs, unavailable, nil
}

func (r *Repo) ListSourceStatus(ctx context.Context) ([]ports.SourceStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, 550*time.Millisecond)
	defer cancel()
	return r.unit.ListSourceStatus(ctx)
}

func (r *Repo) ConfirmHazard(ctx context.Context, id string, version int) error {
	if barrier, ok := r.owner(id).unit.(ports.HazardDurability); ok {
		return barrier.ConfirmHazard(ctx, id, version)
	}
	return nil
}
