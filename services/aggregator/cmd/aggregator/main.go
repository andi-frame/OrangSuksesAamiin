// Command aggregator is the BNPB gateway: it polls BMKG and PVMBG, maps their
// records to the canonical HazardEvent, stores them in the Canonical Store it
// owns, publishes changes through the outbox relay and serves them on an
// internal API.
//
// M2: several instances run behind one Service. All of them serve reads; one,
// elected through a Kubernetes Lease, ingests (polls, relays the outbox,
// retries dead letters), and every one of its writes carries its leadership
// term as a fencing token.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/adapters/sharded"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/api"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/config"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/dlq"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/domain"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ingest"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/leader"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/mapping"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/poller"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/relay"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/resilience"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/satellite"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/sources"
)

// version is set at build time (-ldflags "-X main.version=...", Dockerfile ARG VERSION).
var version = "dev"

func main() {
	if len(os.Args) == 2 {
		switch os.Args[1] {
		case "import-catalog":
			runCatalog()
			return
		case "migrate-single":
			ctx, stop := obs.SignalContext()
			defer stop()
			log := obs.NewLogger("canonical-migration")
			source, destinations := os.Getenv("CANONICAL_DB_URL"), os.Getenv("CANONICAL_SHARD_URLS")
			if source == "" || destinations == "" {
				fatal(log, "canonical_migration", errors.New("CANONICAL_DB_URL and CANONICAL_SHARD_URLS are required"))
			}
			if err := sharded.MigrateSingle(ctx, source, destinations, log); err != nil {
				fatal(log, "canonical_migration", err)
			}
			return
		}
	}
	cfg := config.Load()
	log := obs.NewLogger("aggregator").With("instance", cfg.Identity)
	ctx, stop := obs.SignalContext()
	defer stop()

	volcanoes, err := mapping.LoadVolcanoTable(cfg.VolcanoTablePath)
	if err != nil {
		fatal(log, "volcano_table", err)
	}
	rules, err := mapping.LoadSchemaRules(cfg.SchemaRulesPath)
	if err != nil {
		fatal(log, "schema_rules", err)
	}
	log.Info("schema_rules_loaded", "path", cfg.SchemaRulesPath, "rules", rules.Summary())

	repo := connect(ctx, log, "repository", func() (ports.Repository, error) {
		return adapters.NewRepository(ctx, cfg.RepositoryBackend, log)
	})
	pub := connect(ctx, log, "publisher", func() (ports.Publisher, error) {
		return adapters.NewPublisher(ctx, cfg.PublisherBackend, log)
	})
	defer pub.Close()

	ing := &ingestion{cfg: cfg, log: log, repo: repo, pub: pub, rules: rules, volcanoes: volcanoes}
	if cfg.SatelliteToken != "" && !strings.HasPrefix(cfg.SatelliteToken, "example-") {
		body, err := os.ReadFile(cfg.ProvinceTablePath)
		if err != nil {
			fatal(log, "province_table", err)
		}
		provinces, err := satellite.LoadProvinces(body)
		if err != nil {
			fatal(log, "province_table", err)
		}
		store, ok := repo.(ports.SatelliteStore)
		if !ok {
			fatal(log, "satellite", errors.New("repository lacks satellite checkpoints"))
		}
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.ResponseHeaderTimeout = 5 * time.Second
		ing.satellite = &satellite.Runner{Repo: repo, Store: store, Client: &http.Client{Transport: transport},
			URL: cfg.SatelliteURL, Token: cfg.SatelliteToken, Lateness: cfg.SatelliteLateness, Provinces: provinces, Log: log}
	}

	// background work ends (and the lease is released) before the process exits
	bgDone := make(chan struct{})
	var elector *leader.Elector
	switch cfg.LeaderElection {
	case "kubernetes":
		store, err := leader.NewInClusterLeases(cfg.LeaseName, cfg.Namespace)
		if err != nil {
			fatal(log, "leader_election", err)
		}
		elector = &leader.Elector{
			Store: store, Identity: cfg.Identity, LeaseName: cfg.LeaseName,
			LeaseDuration: cfg.LeaseDuration, RenewDeadline: cfg.RenewDeadline, RetryPeriod: cfg.RetryPeriod,
			Log: log,
		}
		ing.gate = elector.Confirm
		elector.OnLead = func(lctx context.Context, _ int64) { ing.run(lctx) }
		go func() { defer close(bgDone); elector.Run(ctx) }()
	case "none":
		log.Warn("leader_election_disabled", "note", "run exactly one instance (docker compose)")
		go func() { defer close(bgDone); ing.run(ctx) }()
	default:
		fatal(log, "config", errors.New("LEADER_ELECTION must be none or kubernetes"))
	}

	activeSources := []string{"BMKG", "PVMBG"}
	if ing.satellite != nil {
		activeSources = append(activeSources, string(domain.SourceSatellite))
	}
	apiSrv := &api.Server{
		Repo:   repo,
		Status: &api.StoreStatus{Repo: repo, Sources: activeSources, MaxAge: cfg.StatusMaxAge, Log: log},
		Rules:  rules.Summary(), ServiceKey: cfg.ServiceKey, OpsToken: cfg.OpsToken, AuditToken: cfg.AuditToken, Log: log,
		Version: version,
	}
	if elector != nil {
		apiSrv.Leader = elector.Status
	}
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           apiSrv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		// requests already in flight at SIGTERM finish normally (Serve drains them);
		// cancelling them with the signal would turn a rolling update into failed requests
		BaseContext: func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
	}
	log.Info("aggregator_started", "port", cfg.Port, "repository", cfg.RepositoryBackend, "publisher", cfg.PublisherBackend,
		"leader_election", cfg.LeaderElection, "poll_interval_sec", cfg.PollInterval.Seconds(),
		"bmkg_url", cfg.BMKGURL, "pvmbg_url", cfg.PVMBGURL)
	if err := obs.Serve(ctx, srv, log); err != nil {
		fatal(log, "server", err)
	}
	select {
	case <-bgDone:
	case <-time.After(15 * time.Second):
		log.Warn("shutdown_timeout", "what", "ingestion / lease release")
	}
	log.Info("aggregator_stopped")
}

// ingestion is everything only one instance may do at a time.
type ingestion struct {
	cfg       config.Config
	log       *slog.Logger
	repo      ports.Repository
	pub       ports.Publisher
	rules     mapping.SchemaRules
	volcanoes mapping.VolcanoTable
	satellite *satellite.Runner
	gate      func(context.Context) error // nil when there is no election
}

// run blocks until ctx ends (leadership lost or shutdown).
func (g *ingestion) run(ctx context.Context) {
	cfg, log, repo := g.cfg, g.log, g.repo
	if f, ok := ports.FenceFrom(ctx); ok {
		if err := raiseFence(ctx, repo, f, log); err != nil {
			return // fenced by a newer term, or shutting down: leadership ends
		}
	}

	ing := ingest.New(repo, log)
	queue := &dlq.Queue{Repo: repo, Log: log, MaxAttempts: cfg.DLQMaxAttempts,
		BaseBackoff: cfg.DLQBaseBackoff, MaxBackoff: cfg.DLQMaxBackoff}
	limits := sources.Limits{MaxBytes: cfg.MessageMaxBytes, MaxDepth: cfg.MessageMaxDepth}

	bmkgBreaker := resilience.NewBreaker(cfg.BreakerThreshold, cfg.BreakerOpenFor)
	pvmbgBreaker := resilience.NewBreaker(cfg.BreakerThreshold, cfg.BreakerOpenFor)
	bmkgTracker := resilience.NewTracker("BMKG", bmkgBreaker)
	pvmbgTracker := resilience.NewTracker("PVMBG", pvmbgBreaker)
	g.restore(ctx, bmkgTracker, pvmbgTracker)

	bmkg := &poller.BMKGSource{
		Client: &sources.BMKG{HTTP: obs.NewHTTPClient(log, "bmkg", cfg.SourceTimeout), BaseURL: cfg.BMKGURL,
			APIKey: cfg.BMKGKey, Caller: cfg.Identity, Limits: limits},
		Repo: repo, Ingestor: ing, DLQ: queue, Rules: g.rules,
		Overlap: cfg.PollOverlap, PendingTTL: cfg.PendingWarningTTL, Log: log,
	}
	pvmbg := &poller.PVMBGSource{
		Client: &sources.PVMBG{HTTP: obs.NewHTTPClient(log, "pvmbg", cfg.SourceTimeout), BaseURL: cfg.PVMBGURL,
			Token: cfg.PVMBGToken, Caller: cfg.Identity, Limits: limits},
		Repo: repo, Ingestor: ing, DLQ: queue, Rules: g.rules, Volcanoes: g.volcanoes,
		Overlap: cfg.PollOverlap, Log: log,
	}

	// one loop per source: they never wait for each other (Problem 2)
	cycleTimeout := 2*cfg.SourceTimeout + time.Second
	loops := []*poller.Loop{
		{Source: bmkg, Breaker: bmkgBreaker, Tracker: bmkgTracker},
		{Source: pvmbg, Breaker: pvmbgBreaker, Tracker: pvmbgTracker},
	}
	worker := &dlq.Worker{Queue: queue, Log: log, Handlers: map[string]dlq.Handler{
		poller.KindPVMBGReport:   pvmbg.Reprocess,
		poller.KindBMKGEvent:     bmkg.ReprocessEvent,
		poller.KindBMKGWarning:   bmkg.ReprocessWarning,
		poller.KindPVMBGFragment: poller.FragmentHandler(repo, domain.SourcePVMBG),
		poller.KindBMKGFragment:  poller.FragmentHandler(repo, domain.SourceBMKG),
	}}
	rel := &relay.Relay{Repo: repo, Pub: g.pub, Log: log, Interval: cfg.RelayInterval, Batch: cfg.RelayBatch, MaxBackoff: cfg.RelayBackoff}

	var wg sync.WaitGroup
	if g.satellite != nil {
		runner := *g.satellite
		runner.Gate = g.gate
		wg.Go(func() { runner.Run(ctx) })
	}
	for _, l := range loops {
		l.Interval, l.CycleTimeout, l.Log, l.Gate, l.Publish = cfg.PollInterval, cycleTimeout, log, g.gate, g.publish
		wg.Add(1)
		go func() { defer wg.Done(); l.Run(ctx) }()
	}
	wg.Add(2)
	go func() { defer wg.Done(); worker.Run(ctx) }()
	go func() { defer wg.Done(); rel.Run(ctx) }()
	log.InfoContext(ctx, "ingestion_started")
	wg.Wait()
	log.Info("ingestion_stopped")
}

// raiseFence makes the store reject every older term before this leader's
// first write. A store outage is waited out; a newer term ends leadership.
func raiseFence(ctx context.Context, repo ports.Repository, f ports.Fence, log *slog.Logger) error {
	for {
		err := repo.RaiseFence(ctx, f)
		if err == nil {
			log.InfoContext(ctx, "fence_raised", "lease", f.Name, "term", f.Term)
			return nil
		}
		if errors.Is(err, ports.ErrFenced) {
			log.ErrorContext(ctx, "fence_rejected", "lease", f.Name, "term", f.Term, "note", "a newer term exists in the store")
			return err
		}
		log.WarnContext(ctx, "fence_raise_failed", "error", err.Error())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// restore seeds the trackers from the previous leader's stored view.
func (g *ingestion) restore(ctx context.Context, ts ...*resilience.Tracker) {
	rows, err := g.repo.ListSourceStatus(ctx)
	if err != nil {
		return
	}
	for _, r := range rows {
		var st resilience.Status
		if json.Unmarshal(r.Doc, &st) != nil {
			continue
		}
		for _, t := range ts {
			t.Restore(st)
		}
	}
}

// publish stores the source status so every instance serves the same view.
func (g *ingestion) publish(ctx context.Context, st resilience.Status) {
	st.PolledBy = g.cfg.Identity
	b, err := json.Marshal(st)
	if err == nil {
		err = g.repo.PutSourceStatus(ctx, st.Source, b)
	}
	if err != nil && ctx.Err() == nil {
		g.log.WarnContext(ctx, "source_status_store_failed", "source", st.Source, "error", err.Error())
	}
}

// connect retries a backend constructor for up to 90 s, so the Aggregator can
// start before its database/broker is ready.
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
