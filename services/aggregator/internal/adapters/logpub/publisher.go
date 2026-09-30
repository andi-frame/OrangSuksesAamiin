// Package logpub is a Publisher that only logs what it would publish. It
// stands in for the broker in unit tests and in local runs without Kafka.
package logpub

import (
	"context"
	"log/slog"
	"sync"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

func init() {
	adapters.RegisterPublisher("log", func(_ context.Context, log *slog.Logger) (ports.Publisher, error) {
		return &Publisher{log: log}, nil
	})
}

// Publisher records every message and logs it.
type Publisher struct {
	log *slog.Logger

	mu       sync.Mutex
	Messages []ports.Message
	Err      error // test hook: when set, Publish fails
}

func New(log *slog.Logger) *Publisher { return &Publisher{log: log} }

func (p *Publisher) Publish(ctx context.Context, m ports.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Err != nil {
		return p.Err
	}
	p.Messages = append(p.Messages, m)
	if p.log != nil {
		p.log.InfoContext(obs.ExtractHeaders(ctx, m.Headers), "event_published",
			"backend", "log", "key", m.Key, "bytes", len(m.Value))
	}
	return nil
}

func (p *Publisher) Close() error { return nil }

// Published returns a copy of the messages seen so far.
func (p *Publisher) Published() []ports.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ports.Message(nil), p.Messages...)
}

// SetErr makes Publish fail with err (nil to recover).
func (p *Publisher) SetErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Err = err
}
