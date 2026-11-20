package reports

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka publishes field reports. The client connects lazily: the Client-Facing
// API starts and serves reads even when the broker is down; only the write
// path answers 503 then.
type Kafka struct {
	cl *kgo.Client
}

// NewKafka creates the producer (acks=all + idempotent producer: a retried
// produce is not written twice to the partition).
func NewKafka(brokers []string, topic string, timeout time.Duration) (*Kafka, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(timeout),
		kgo.ProduceRequestTimeout(timeout),
		kgo.ProducerLinger(0),
		kgo.UnknownTopicRetries(2),
	)
	if err != nil {
		return nil, err
	}
	return &Kafka{cl: cl}, nil
}

func (k *Kafka) Publish(ctx context.Context, key string, value []byte, headers map[string]string) error {
	rec := &kgo.Record{Key: []byte(key), Value: value}
	for hk, hv := range headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: hk, Value: []byte(hv)})
	}
	if err := k.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return nil
}

func (k *Kafka) Close() { k.cl.Close() }
