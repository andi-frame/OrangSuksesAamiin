package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func quiet() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func cluster(t *testing.T) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "field-reports", "field-reports.processed"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

func produce(t *testing.T, brokers []string, ids ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic("field-reports"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for _, id := range ids {
		v, _ := json.Marshal(Incoming{ReportID: id, IdempotencyKey: "k-" + id, ClientID: "c", ReceivedAt: time.Now().UTC(),
			Report: json.RawMessage(`{"area_name":"x","description":"y"}`)})
		if err := cl.ProduceSync(context.Background(), &kgo.Record{Key: []byte(id), Value: v}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// outputs reads the committed output topic.
func outputs(t *testing.T, brokers []string) map[string]int {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics("field-reports.processed"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	got := map[string]int{}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	for ctx.Err() == nil {
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) { got[string(r.Key)]++ })
	}
	return got
}

func start(t *testing.T, brokers []string, name string) (*Processor, context.CancelFunc) {
	t.Helper()
	p := New(Config{Brokers: brokers, InTopic: "field-reports", OutTopic: "field-reports.processed",
		Group: "frp", TransactionID: "frp-txn", Instance: name}, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	return p, func() { cancel(); <-done }
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestEveryReportProcessedOnceEvenWhenSubmittedTwice(t *testing.T) {
	brokers := cluster(t)
	var ids []string
	for i := range 20 {
		ids = append(ids, fmt.Sprintf("fr-%02d", i))
	}
	produce(t, brokers, ids...)
	produce(t, brokers, ids[:5]...) // client retries with the same Idempotency-Key

	p, stop := start(t, brokers, "a")
	waitFor(t, "20 processed", func() bool { return p.Stats()["processed"].(int64) == 20 })
	waitFor(t, "5 duplicates", func() bool { return p.Stats()["duplicates_skipped"].(int64) == 5 })
	stop()

	out := outputs(t, brokers)
	if len(out) != 20 {
		t.Fatalf("want 20 processed reports, got %d", len(out))
	}
	for id, n := range out {
		if n != 1 {
			t.Fatalf("%s processed %d times", id, n)
		}
	}
}

// After a restart the dedup set comes back from the output topic: a late retry
// of an already processed report is still skipped, new reports go through.
func TestRestartRebuildsDedupSet(t *testing.T) {
	brokers := cluster(t)
	produce(t, brokers, "fr-a", "fr-b")
	p, stop := start(t, brokers, "a")
	waitFor(t, "2 processed", func() bool { return p.Stats()["processed"].(int64) == 2 })
	stop()

	produce(t, brokers, "fr-a", "fr-c") // a retry arriving after the restart, and a new one
	p2, stop2 := start(t, brokers, "b")
	defer stop2()
	if _, ok := p2.Get("fr-a"); !ok {
		t.Fatal("rebuild must know fr-a")
	}
	waitFor(t, "fr-c processed", func() bool { _, ok := p2.Get("fr-c"); return ok })
	waitFor(t, "retry skipped", func() bool { return p2.Stats()["duplicates_skipped"].(int64) == 1 })
	if n := p2.Stats()["processed"].(int64); n != 1 {
		t.Fatalf("the restarted processor must process only fr-c, processed %d", n)
	}
	out := outputs(t, brokers)
	if len(out) != 3 || out["fr-a"] != 1 {
		t.Fatalf("outputs %v", out)
	}
}

// A zombie (an old instance that is still running, e.g. paused or cut off) and a
// new instance share the transactional id: the zombie is fenced, its Run ends,
// and every report is still processed exactly once by the new one.
func TestZombieIsFencedAndNothingIsProcessedTwice(t *testing.T) {
	brokers := cluster(t)
	produce(t, brokers, "fr-1", "fr-2")
	a := New(Config{Brokers: brokers, InTopic: "field-reports", OutTopic: "field-reports.processed",
		Group: "frp", TransactionID: "frp-txn", Instance: "a"}, quiet())
	actx, acancel := context.WithCancel(context.Background())
	defer acancel()
	if err := a.Rebuild(actx); err != nil {
		t.Fatal(err)
	}
	aDone := make(chan error, 1)
	go func() { aDone <- a.Run(actx) }()
	waitFor(t, "a processed 2", func() bool { return a.Stats()["processed"].(int64) == 2 })

	b, stopB := start(t, brokers, "b") // same transactional id: fences a
	defer stopB()
	produce(t, brokers, "fr-3", "fr-1")
	waitFor(t, "fr-3 processed by someone", func() bool {
		_, okA := a.Get("fr-3")
		_, okB := b.Get("fr-3")
		return okA || okB
	})
	acancel()
	<-aDone
	out := outputs(t, brokers)
	if len(out) != 3 {
		t.Fatalf("want 3 reports, got %v", out)
	}
	for id, n := range out {
		if n != 1 {
			t.Fatalf("%s committed %d times", id, n)
		}
	}
}
