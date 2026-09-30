// Package kafka is the Kafka implementation of ports.Publisher (franz-go, pure Go).
//
// Publish returns nil only after the broker acknowledged the record from all
// in-sync replicas (acks=all, idempotent producer). Retrying and ordering belong to
// the outbox relay; nothing is buffered silently here. The topic is created by the
// kafka-init job, never by the producer.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

func init() {
	adapters.RegisterPublisher("kafka", func(ctx context.Context, log *slog.Logger) (ports.Publisher, error) {
		brokers := os.Getenv("KAFKA_BROKERS")
		if brokers == "" {
			return nil, errors.New("KAFKA_BROKERS is empty (required when PUBLISHER_BACKEND=kafka)")
		}
		topic := os.Getenv("KAFKA_TOPIC")
		if topic == "" {
			topic = "hazard.events"
		}
		return New(ctx, strings.Split(brokers, ","), topic, log)
	})
}

// Publisher produces hazard events synchronously.
type Publisher struct {
	client *kgo.Client
	topic  string
	log    *slog.Logger
}

// New creates the client and verifies that a broker is reachable.
func New(ctx context.Context, brokers []string, topic string, log *slog.Logger) (*Publisher, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()), // idempotent producer is franz-go's default when acks=all
		kgo.RecordDeliveryTimeout(10*time.Second),
		kgo.ProduceRequestTimeout(8*time.Second),
		kgo.UnknownTopicRetries(2), // fail fast if the topic is missing; the relay retries
	)
	if err != nil {
		return nil, err
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cl.Ping(pctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka not reachable: %w", err)
	}
	return &Publisher{client: cl, topic: topic, log: log}, nil
}

func (p *Publisher) Publish(ctx context.Context, m ports.Message) error {
	rec := &kgo.Record{Key: []byte(m.Key), Value: m.Value}
	for k, v := range m.Headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	start := time.Now()
	res := p.client.ProduceSync(ctx, rec)
	lctx := obs.ExtractHeaders(ctx, m.Headers)
	if err := res.FirstErr(); err != nil {
		p.log.WarnContext(lctx, "event_publish_failed", "backend", "kafka", "key", m.Key, "error", err.Error())
		return err
	}
	r, _ := res.First()
	p.log.InfoContext(lctx, "event_published", "backend", "kafka", "key", m.Key, "topic", p.topic,
		"partition", r.Partition, "offset", r.Offset, "latency_ms", time.Since(start).Milliseconds())
	return nil
}

func (p *Publisher) Close() error {
	p.client.Close()
	return nil
}
