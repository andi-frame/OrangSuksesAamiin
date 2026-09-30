package kafka

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

const topic = "hazard.events"

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// Runs against franz-go's in-process fake broker: key, value and headers must
// arrive exactly as published, and Publish must only return after the ack.
func TestPublishKeyValueAndHeaders(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	p, err := New(ctx, c.ListenAddrs(), topic, quiet())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	msg := ports.Message{Key: "bmkg-1", Value: []byte(`{"event_id":"bmkg-1:1"}`),
		Headers: map[string]string{"correlation_id": "cid-1", "event_id": "bmkg-1:1"}}
	if err := p.Publish(ctx, msg); err != nil {
		t.Fatal(err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	fetches := cl.PollFetches(cctx)
	if err := fetches.Err(); err != nil {
		t.Fatal(err)
	}
	recs := fetches.Records()
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	r := recs[0]
	if string(r.Key) != msg.Key || string(r.Value) != string(msg.Value) {
		t.Fatalf("key/value mismatch: %q %q", r.Key, r.Value)
	}
	got := map[string]string{}
	for _, h := range r.Headers {
		got[h.Key] = string(h.Value)
	}
	if got["correlation_id"] != "cid-1" || got["event_id"] != "bmkg-1:1" {
		t.Fatalf("headers lost: %v", got)
	}
}

// A missing topic (kafka-init not finished) is an error the relay retries, not a silent drop.
func TestPublishToMissingTopicFails(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p, err := New(ctx, c.ListenAddrs(), topic, quiet())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Publish(ctx, ports.Message{Key: "k", Value: []byte("{}")}); err == nil {
		t.Fatal("expected an error for a topic that does not exist")
	}
}

func TestNewFailsWhenBrokerUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := New(ctx, []string{"127.0.0.1:1"}, topic, quiet()); err == nil {
		t.Fatal("expected an error")
	}
}
