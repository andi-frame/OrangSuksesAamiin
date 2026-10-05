package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/consumer-hazard/internal/config"
	"github.com/andi-frame/orangsuksesaamiin/services/consumer-hazard/internal/runner"
)

func main() {
	log := obs.NewLogger("consumer-hazard")
	cfg := config.Load()

	ctx, stop := obs.SignalContext()
	defer stop()

	run, err := runner.New(ctx, cfg.Brokers, cfg.Topic, cfg.Group, cfg.Name, log)
	if err != nil {
		fatal(log, "kafka", err)
	}
	defer run.Close()
	go run.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", obs.LivenessHandler(cfg.Name, func() map[string]any {
		return map[string]any{"group": cfg.Group}
	}))
	mux.HandleFunc("GET /ready", obs.LivenessHandler(cfg.Name, nil))
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		obs.WriteJSON(w, http.StatusOK, run.Stats())
	})

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           obs.Middleware(log, "/health", "/ready")(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Info("consumer_started", "name", cfg.Name, "group", cfg.Group, "topic", cfg.Topic, "port", cfg.Port)
	if err := obs.Serve(ctx, srv, log); err != nil {
		fatal(log, "server", err)
	}
}

func fatal(log *slog.Logger, what string, err error) {
	log.Error("fatal", "what", what, "error", err.Error())
	os.Exit(1)
}
