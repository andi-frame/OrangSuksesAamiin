// Command bmkg-mock runs the simulated BMKG seismic/tsunami service.
package main

import (
	"net/http"
	"os"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/bmkg-mock/internal/bmkg"
)

func main() {
	log := obs.NewLogger("bmkg-mock")
	cfg := bmkg.LoadConfig()

	store := bmkg.NewStore()
	gen := bmkg.NewGenerator(cfg, store, log)
	gen.Seed(time.Now())

	ctx, stop := obs.SignalContext()
	defer stop()
	go gen.Run(ctx)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           bmkg.NewHandler(cfg, store, gen, log),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Info("bmkg_mock_started", "port", cfg.Port,
		"delay_min_ms", cfg.DelayMin.Milliseconds(), "delay_max_ms", cfg.DelayMax.Milliseconds(),
		"event_interval_sec", cfg.EventInterval.Seconds())
	if err := obs.Serve(ctx, srv, log); err != nil {
		log.Error("server_failed", "error", err.Error())
		os.Exit(1)
	}
}
