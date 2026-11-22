package sharded

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/memory"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/postgres"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports/repotest"
)

func memoryShards() *Repo {
	shards := make([]shard, 3)
	for i := range shards {
		shards[i] = shard{name: fmt.Sprintf("shard-%d", i), unit: memory.New()}
	}
	return &Repo{shards: shards, unit: shards[0].unit}
}

func TestShardedContract(t *testing.T) {
	repotest.Run(t, func(*testing.T) ports.Repository { return memoryShards() })
}

func TestPostgresShardedContract(t *testing.T) {
	urls := os.Getenv("TEST_SHARD_DB_URLS")
	if urls == "" {
		t.Skip("TEST_SHARD_DB_URLS not set")
	}
	ctx := context.Background()
	repo, err := Open(ctx, urls, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, shard := range repo.shards {
		t.Cleanup(shard.unit.(*postgres.Repo).Close)
	}
	repotest.Run(t, func(t *testing.T) ports.Repository {
		for _, shard := range repo.shards {
			if err := shard.unit.(*postgres.Repo).Reset(ctx); err != nil {
				t.Fatal(err)
			}
		}
		return repo
	})
}

func TestShardLossStillServesAndFenceCoversEveryShard(t *testing.T) {
	r := memoryShards()
	ctx := context.Background()
	if err := r.RaiseFence(ctx, ports.Fence{Name: "lease", Term: 2}); err != nil {
		t.Fatal(err)
	}
	stale := ports.WithFence(ctx, ports.Fence{Name: "lease", Term: 1})
	at := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	for i, shard := range r.shards {
		for j := 0; ; j++ {
			id := fmt.Sprintf("bmkg-%d", j)
			if Index(id, len(r.shards)) != i {
				continue
			}
			h := domain.HazardEvent{HazardID: id, Version: 1, IngestedAt: at}
			out := ports.OutboxEntry{EventID: domain.EventID(id, 1), HazardID: id}
			if err := r.Save(stale, h, 0, out); !errors.Is(err, ports.ErrFenced) {
				t.Fatalf("%s: expected fenced error, received %v", shard.name, err)
			}
			if err := r.Save(ctx, h, 0, out); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	r.shards[0].unit.(*memory.Repo).Fail(errors.New("offline"))
	if err := r.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	f := ports.ListFilter{AuditOrder: true, Limit: 1}
	first, err := r.List(ctx, f)
	if err != nil || len(first.Items) != 1 || len(first.UnavailableUnits) != 1 || first.NextCursor == "" {
		t.Fatalf("first page: %+v, %v", first, err)
	}
	f.Cursor = first.NextCursor
	second, err := r.List(ctx, f)
	if err != nil || len(second.Items) != 1 || second.Items[0].HazardID <= first.Items[0].HazardID || second.NextCursor != "" {
		t.Fatalf("second page: %+v, %v", second, err)
	}
}

func TestLegacyMigrationIsRepeatable(t *testing.T) {
	legacy, urls := os.Getenv("TEST_LEGACY_DB_URL"), os.Getenv("TEST_SHARD_DB_URLS")
	if legacy == "" || urls == "" {
		t.Skip("PostgreSQL migration URLs are not set")
	}
	ctx := context.Background()
	from, err := postgres.New(ctx, legacy)
	if err != nil {
		t.Fatal(err)
	}
	defer from.Close()
	to, err := Open(ctx, urls, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, shard := range to.shards {
		pg := shard.unit.(*postgres.Repo)
		defer pg.Close()
		if err := pg.Reset(ctx); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	h := domain.HazardEvent{HazardID: "bmkg-legacy", SourceRefID: "legacy", Source: domain.SourceBMKG,
		OccurredAt: at, IngestedAt: at, UpdatedAt: at, Version: 1}
	entry := ports.OutboxEntry{EventID: domain.EventID(h.HazardID, h.Version), HazardID: h.HazardID}
	if err := from.Save(ctx, h, 0, entry); err != nil {
		t.Fatal(err)
	}
	if err := from.MarkPublished(ctx, []string{entry.EventID}); err != nil {
		t.Fatal(err)
	}
	if err := from.SetCursor(ctx, "legacy-cursor", at); err != nil {
		t.Fatal(err)
	}
	if err := from.RaiseFence(ctx, ports.Fence{Name: "lease", Term: 2}); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for range 2 {
		if err := MigrateSingle(ctx, legacy, urls, log); err != nil {
			t.Fatal(err)
		}
	}
	got, err := to.Get(ctx, h.HazardID)
	if err != nil || got.Version != h.Version || !domain.SameContent(*got, h) {
		t.Fatalf("migrated hazard: %+v, %v", got, err)
	}
	out, err := to.PendingOutbox(ctx, 20)
	if err != nil || len(out) != 0 {
		t.Fatalf("migrated published marker: %+v, %v", out, err)
	}
	cursor, ok, err := to.GetCursor(ctx, "legacy-cursor")
	if err != nil || !ok || !cursor.Equal(at) {
		t.Fatalf("migrated cursor: %v, %v, %v", cursor, ok, err)
	}
	for _, shard := range to.shards {
		err := shard.unit.RaiseFence(ctx, ports.Fence{Name: "lease", Term: 1})
		if !errors.Is(err, ports.ErrFenced) {
			t.Fatalf("%s did not retain migrated fence: %v", shard.name, err)
		}
	}
}
