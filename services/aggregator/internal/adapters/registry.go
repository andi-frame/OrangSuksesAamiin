// Package adapters is a tiny plug-in registry: each backend registers itself
// under a name (init()), and cmd/aggregator selects one from the environment
// (REPOSITORY_BACKEND / PUBLISHER_BACKEND). Adding the PostgreSQL repository
// or the Kafka publisher therefore means adding a package plus one blank
// import in cmd/aggregator/backends.go - no change to the Aggregator's logic.
package adapters

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// RepositoryFactory builds a Repository, reading its own settings from the
// environment (e.g. CANONICAL_DB_URL).
type RepositoryFactory func(ctx context.Context, log *slog.Logger) (ports.Repository, error)

// PublisherFactory builds a Publisher (e.g. reading KAFKA_BROKERS, KAFKA_TOPIC).
type PublisherFactory func(ctx context.Context, log *slog.Logger) (ports.Publisher, error)

var (
	mu    sync.RWMutex
	repos = map[string]RepositoryFactory{}
	pubs  = map[string]PublisherFactory{}
)

func RegisterRepository(name string, f RepositoryFactory) {
	mu.Lock()
	defer mu.Unlock()
	repos[name] = f
}

func RegisterPublisher(name string, f PublisherFactory) {
	mu.Lock()
	defer mu.Unlock()
	pubs[name] = f
}

func names[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func NewRepository(ctx context.Context, name string, log *slog.Logger) (ports.Repository, error) {
	mu.RLock()
	f, ok := repos[name]
	avail := names(repos)
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown REPOSITORY_BACKEND %q (available: %v)", name, avail)
	}
	return f(ctx, log)
}

func NewPublisher(ctx context.Context, name string, log *slog.Logger) (ports.Publisher, error) {
	mu.RLock()
	f, ok := pubs[name]
	avail := names(pubs)
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown PUBLISHER_BACKEND %q (available: %v)", name, avail)
	}
	return f(ctx, log)
}
