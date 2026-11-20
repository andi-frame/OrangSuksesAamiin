package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/mapping"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// importCatalog accepts newline-delimited BMKG SeismicEvent objects. It only
// inserts new IDs, so reruns preserve live updates and any folded-in warnings.
func importCatalog(ctx context.Context, input io.Reader, repo ports.Repository, log *slog.Logger) error {
	ing := ingest.New(repo, log)
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var processed, added int
	for scanner.Scan() {
		processed++
		var event map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return fmt.Errorf("catalog line %d: %w", processed, err)
		}
		h, err := mapping.Seismic(event, time.Now())
		if err != nil {
			return fmt.Errorf("catalog line %d: %w", processed, err)
		}
		outcome, err := ing.Upsert(ctx, h, func(current, fresh *domain.HazardEvent) {
			if current != nil {
				*fresh = *current
			}
		})
		if err != nil {
			return fmt.Errorf("catalog line %d: %w", processed, err)
		}
		if outcome == ingest.Created {
			added++
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	log.InfoContext(ctx, "catalog_import_complete", "input_records", processed, "inserted", added)
	return nil
}

func runCatalog() {
	ctx, stop := obs.SignalContext()
	defer stop()
	log := obs.NewLogger("catalog-import")
	repo, err := adapters.NewRepository(ctx, obs.EnvString("REPOSITORY_BACKEND", "postgres"), log)
	if err == nil {
		err = importCatalog(ctx, os.Stdin, repo, log)
	}
	if err != nil {
		fatal(log, "catalog_import", err)
	}
}
