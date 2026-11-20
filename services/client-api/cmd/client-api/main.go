package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/aggregator"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/api"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/auth"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/config"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/limit"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/reports"
)

func main() {
	log := obs.NewLogger("client-api")
	cfg := config.Load()

	agg := aggregator.New(cfg.AggURL, cfg.AggKey, log, cfg.AggTimeout)
	srv := &api.Server{
		Agg: agg, Log: log,
		// Tim Lapangan and Pusat keep a reserved share of the capacity; Media is shed first (429)
		Limit:            limit.NewPriority(cfg.RateRPS, cfg.RateBurst, cfg.Reserve, cfg.MaxInFlight, auth.Role, "lapangan", "pusat"),
		ReportAckTimeout: cfg.ReportAckTimeout, ReportsURL: cfg.ReportsURL, AuthURL: cfg.AuthURL,
	}
	if len(cfg.KafkaBrokers) > 0 {
		k, err := reports.NewKafka(cfg.KafkaBrokers, cfg.ReportsTopic, cfg.ReportAckTimeout)
		if err != nil {
			fatal(log, "kafka", err)
		}
		defer k.Close()
		srv.Reports = k
	}

	root := obs.Middleware(log, "/health", "/ready")(srv.Handler(cfg.JWTSecret))

	httpSrv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           root,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Info("client_api_started", "port", cfg.Port, "aggregator_url", cfg.AggURL,
		"rate_rps", cfg.RateRPS, "rate_burst", cfg.RateBurst, "admission", srv.Limit.String(),
		"field_reports", srv.Reports != nil, "field_reports_topic", cfg.ReportsTopic)

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
