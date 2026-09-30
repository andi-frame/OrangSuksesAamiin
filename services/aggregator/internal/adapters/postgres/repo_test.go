package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports/repotest"
)

// Needs a real database: TEST_CANONICAL_DB_URL=postgres://... (make test-postgres
// starts a throwaway container and sets it). Skipped when unset so `make test`
// stays hermetic.
func TestPostgresRepoSatisfiesContract(t *testing.T) {
	url := os.Getenv("TEST_CANONICAL_DB_URL")
	if url == "" {
		t.Skip("TEST_CANONICAL_DB_URL not set")
	}
	ctx := context.Background()
	repo, err := New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(repo.Close)
	repotest.Run(t, func(t *testing.T) ports.Repository {
		if err := repo.Reset(ctx); err != nil {
			t.Fatal(err)
		}
		return repo
	})
}

// A second Aggregator instance (or a restart) must reuse existing tables.
func TestMigrateIsIdempotent(t *testing.T) {
	url := os.Getenv("TEST_CANONICAL_DB_URL")
	if url == "" {
		t.Skip("TEST_CANONICAL_DB_URL not set")
	}
	for i := 0; i < 2; i++ {
		r, err := New(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		r.Close()
	}
}
