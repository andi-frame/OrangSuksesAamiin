package main

// Storage and broker backends register themselves in init(); select one with
// REPOSITORY_BACKEND / PUBLISHER_BACKEND. To plug in the PostgreSQL repository
// or the Kafka publisher, add their packages here (blank import) - nothing else
// in the Aggregator changes. See docs/adapters.md.
import (
	_ "github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/logpub"
	_ "github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/memory"
	// _ "github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/postgres"
	// _ "github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/kafka"
)
