// Command aggregator is the BNPB gateway: it polls BMKG and PVMBG, maps their
// records to the canonical HazardEvent, stores them in the Canonical Store it
// owns, publishes changes through the outbox relay and serves them on an
// internal API.
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/api"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/config"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/mapping"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/poller"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/relay"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/resilience"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

func main() {
	log := obs.NewLogger("aggregator")
	cfg := config.Load()
	ctx, stop := obs.SignalContext()
	defer stop()

	volcanoes, err := mapping.LoadVolcanoTable(cfg.VolcanoTablePath)
	if err != nil {
		fatal(log, "volcano_table", err)
	}
	repo := connect(ctx, log, "repository", func() (ports.Repository, error) {
		return adapters.NewRepository(ctx, cfg.RepositoryBackend, log)
	})
	pub := connect(ctx, log, "publisher", func() (ports.Publisher, error) {
		return adapters.NewPublisher(ctx, cfg.PublisherBackend, log)
	})
	defer pub.Close()

	ing := ingest.New(repo, log)
	quarantine := ingest.NewQuarantine(log, 200)

	bmkgBreaker := resilience.NewBreaker(cfg.BreakerThreshold, cfg.BreakerOpenFor)
	pvmbgBreaker := resilience.NewBreaker(cfg.BreakerThreshold, cfg.BreakerOpenFor)
	bmkgTracker := resilience.NewTracker("BMKG", bmkgBreaker)
	pvmbgTracker := resilience.NewTracker("PVMBG", pvmbgBreaker)

	// one loop per source: they never wait for each other (Problem 2)
	cycleTimeout := 2*cfg.SourceTimeout + time.Second
	loops := []*poller.Loop{
		{
			Source: &poller.BMKGSource{
				Client: &sources.BMKG{HTTP: obs.NewHTTPClient(log, "bmkg", cfg.SourceTimeout), BaseURL: cfg.BMKGURL, APIKey: cfg.BMKGKey},
				Repo:   repo, Ingestor: ing, Quarantine: quarantine,
				Overlap: cfg.PollOverlap, PendingTTL: cfg.PendingWarningTTL, Log: log,
			},
			Breaker: bmkgBreaker, Tracker: bmkgTracker, Interval: cfg.PollInterval, CycleTimeout: cycleTimeout, Log: log,
		},
		{
			Source: &poller.PVMBGSource{
				Client: &sources.PVMBG{HTTP: obs.NewHTTPClient(log, "pvmbg", cfg.SourceTimeout), BaseURL: cfg.PVMBGURL, Token: cfg.PVMBGToken},
				Repo:   repo, Ingestor: ing, Quarantine: quarantine, Volcanoes: volcanoes,
				Overlap: cfg.PollOverlap, Log: log,
			},
			Breaker: pvmbgBreaker, Tracker: pvmbgTracker, Interval: cfg.PollInterval, CycleTimeout: cycleTimeout, Log: log,
		},
	}
	for _, l := range loops {
		go l.Run(ctx)
	}
	go (&relay.Relay{Repo: repo, Pub: pub, Log: log, Interval: cfg.RelayInterval, Batch: cfg.RelayBatch, MaxBackoff: cfg.RelayBackoff}).Run(ctx)

	apiSrv := &api.Server{
		Repo: repo, Trackers: []*resilience.Tracker{bmkgTracker, pvmbgTracker},
		Quarantine: quarantine, ServiceKey: cfg.ServiceKey, Log: log,
	}
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           apiSrv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	log.Info("aggregator_started", "port", cfg.Port, "repository", cfg.RepositoryBackend, "publisher", cfg.PublisherBackend,
		"poll_interval_sec", cfg.PollInterval.Seconds(), "bmkg_url", cfg.BMKGURL, "pvmbg_url", cfg.PVMBGURL)
	if err := obs.Serve(ctx, srv, log); err != nil {
		fatal(log, "server", err)
	}
}

// connect retries a backend constructor for up to 90 s, so the Aggregator can
// start before its database/broker container is ready (docker compose).
func connect[T any](ctx context.Context, log *slog.Logger, what string, build func() (T, error)) T {
	deadline := time.Now().Add(90 * time.Second)
	for {
		v, err := build()
		if err == nil {
			return v
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			fatal(log, what, err)
		}
		log.Warn("backend_not_ready", "backend", what, "error", err.Error())
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
}

func fatal(log *slog.Logger, what string, err error) {
	log.Error("fatal", "what", what, "error", err.Error())
	os.Exit(1)
}
