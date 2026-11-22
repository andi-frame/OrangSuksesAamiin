package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
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

	// The broker may start after us (or restart): the consumer waits for it
	// instead of exiting, so the pod never goes through CrashLoopBackOff.
	// /health answers from the start (liveness); /ready is 503 until connected.
	var run atomic.Pointer[runner.Runner]
	go func() {
		for {
			r, err := runner.New(ctx, cfg.Brokers, cfg.Topic, cfg.Group, cfg.Name, log)
			if err == nil {
				run.Store(r)
				log.Info("kafka_connected", "group", cfg.Group, "topic", cfg.Topic)
				r.Run(ctx)
				r.Close()
				return
			}
			log.Warn("kafka_not_ready", "error", err.Error(), "retry_in_s", 3)
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", obs.LivenessHandler(cfg.Name, func() map[string]any {
		return map[string]any{"group": cfg.Group, "connected": run.Load() != nil}
	}))
	mux.HandleFunc("GET /ready", obs.ReadinessHandler(cfg.Name, map[string]obs.Check{
		"kafka": func(_ context.Context) error {
			if run.Load() == nil {
				return errors.New("not connected to the broker yet")
			}
			return nil
		},
	}))
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		if rr := run.Load(); rr != nil {
			obs.WriteJSON(w, http.StatusOK, rr.Stats())
			return
		}
		obs.WriteError(w, http.StatusServiceUnavailable, "not_ready", "not connected to the broker yet")
	})
	mux.HandleFunc("GET /alerts", func(w http.ResponseWriter, r *http.Request) {
		alerts := []runner.Alert{}
		if rr := run.Load(); rr != nil {
			alerts = rr.Alerts()
		}
		obs.WriteJSON(w, http.StatusOK, map[string]any{"consumer": cfg.Name, "alerts": alerts})
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
