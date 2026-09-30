package memory

import (
	"testing"

	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports/repotest"
)

// The in-memory repository must satisfy the same contract as the PostgreSQL one.
func TestMemoryRepoSatisfiesContract(t *testing.T) {
	repotest.Run(t, func(*testing.T) ports.Repository { return New() })
}
