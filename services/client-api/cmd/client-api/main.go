package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/aggregator"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/api"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/config"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/limit"
)

func main() {
	log := obs.NewLogger("client-api")
	cfg := config.Load()

	agg := aggregator.New(cfg.AggURL, cfg.AggKey, log, cfg.AggTimeout)
	srv := &api.Server{Agg: agg, Log: log}

	root := srv.Handler(cfg.JWTSecret)
	root = limit.New(cfg.RateRPS, cfg.RateBurst).Middleware(root)
	root = obs.Middleware(log, "/health", "/ready")(root)

	httpSrv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           root,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Info("client_api_started", "port", cfg.Port, "aggregator_url", cfg.AggURL,
		"rate_rps", cfg.RateRPS, "rate_burst", cfg.RateBurst)

	ctx, stop := obs.SignalContext()
	defer stop()
	if err := obs.Serve(ctx, httpSrv, log); err != nil {
		fatal(log, "server", err)
	}
}

func fatal(log *slog.Logger, what string, err error) {
	log.Error("fatal", "what", what, "error", err.Error())
	os.Exit(1)
}
