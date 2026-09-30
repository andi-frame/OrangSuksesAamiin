package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/logpub"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/memory"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

func lg() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func store(t *testing.T, repo *memory.Repo, ids ...string) {
	t.Helper()
	ing := ingest.New(repo, lg())
	for i, id := range ids {
		ctx := obs.WithCorrelationID(context.Background(), "cid-"+id)
		h := &domain.HazardEvent{
			HazardID: id, Source: domain.SourceBMKG, SourceRefID: id, HazardType: domain.HazardSeismic,
			Severity: domain.SeverityWaspada, AreaName: "A", OccurredAt: time.Now().Add(time.Duration(i) * time.Second),
		}
		if _, err := ing.Upsert(ctx, h, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func newRelay(repo *memory.Repo, pub *logpub.Publisher) *Relay {
	return &Relay{Repo: repo, Pub: pub, Log: lg(), Interval: 10 * time.Millisecond, Batch: 10, MaxBackoff: 50 * time.Millisecond}
}

func TestFlushPublishesInOrderWithHeadersAndMarksDelivered(t *testing.T) {
	repo, pub := memory.New(), logpub.New(nil)
	store(t, repo, "a", "b", "c")
	n, err := newRelay(repo, pub).Flush(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("flush: %d %v", n, err)
	}
	msgs := pub.Published()
	for i, id := range []string{"a", "b", "c"} {
		m := msgs[i]
		if m.Key != id {
			t.Fatalf("message %d key = %s, want %s (per-hazard ordering key)", i, m.Key, id)
		}
		if m.Headers[obs.KafkaHeaderCorrelationID] != "cid-"+id || m.Headers["event_id"] != id+":1" {
			t.Fatalf("headers: %v", m.Headers)
		}
	}
	if p, _ := repo.PendingOutbox(context.Background(), 10); len(p) != 0 {
		t.Fatalf("delivered entries must not be re-sent: %d pending", len(p))
	}
}

// A broker outage must neither block storing hazards nor lose their events.
func TestBrokerOutageDoesNotStopIngestionOrLoseEvents(t *testing.T) {
	repo, pub := memory.New(), logpub.New(nil)
	pub.SetErr(errors.New("broker unreachable"))
	r := newRelay(repo, pub)

	store(t, repo, "a", "b") // ingestion keeps working with the broker down
	if _, err := r.Flush(context.Background()); err == nil {
		t.Fatal("flush must report the outage")
	}
	store(t, repo, "c")
	if p, _ := repo.PendingOutbox(context.Background(), 10); len(p) != 3 {
		t.Fatalf("events must wait in the outbox: %d", len(p))
	}

	pub.SetErr(nil) // broker is back
	n, err := r.Flush(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("after recovery: %d %v", n, err)
	}
	got := pub.Published()
	if len(got) != 3 || got[0].Key != "a" || got[2].Key != "c" {
		t.Fatalf("order after recovery: %+v", got)
	}
}

func TestPartialFailureKeepsOrderAndResumes(t *testing.T) {
	repo := memory.New()
	store(t, repo, "a", "b", "c")
	pub := &failingSecond{Publisher: logpub.New(nil)}
	r := &Relay{Repo: repo, Pub: pub, Log: lg(), Interval: time.Millisecond, Batch: 10, MaxBackoff: time.Millisecond}
	n, err := r.Flush(context.Background())
	if err == nil || n != 1 {
		t.Fatalf("expected 1 delivered then failure, got %d %v", n, err)
	}
	pend, _ := repo.PendingOutbox(context.Background(), 10)
	if len(pend) != 2 || pend[0].HazardID != "b" {
		t.Fatalf("b and c must still be pending, in order: %+v", pend)
	}
	pub.heal = true
	if n, err := r.Flush(context.Background()); err != nil || n != 2 {
		t.Fatalf("resume: %d %v", n, err)
	}
}

type failingSecond struct {
	Publisher *logpub.Publisher
	calls     int
	heal      bool
}

func (f *failingSecond) Publish(ctx context.Context, m ports.Message) error {
	f.calls++
	if !f.heal && f.calls == 2 {
		return errors.New("transient")
	}
	return f.Publisher.Publish(ctx, m)
}

func TestRunDeliversNewEntriesUntilCancelled(t *testing.T) {
	repo, pub := memory.New(), logpub.New(nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { newRelay(repo, pub).Run(ctx); close(done) }()

	store(t, repo, "x")
	deadline := time.Now().Add(2 * time.Second)
	for len(pub.Published()) < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(pub.Published()) != 1 {
		t.Fatal("relay loop did not deliver the entry")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relay did not stop on cancel")
	}
}

func (f *failingSecond) Close() error { return nil }
