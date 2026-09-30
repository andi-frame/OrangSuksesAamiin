// Command pvmbg-mock runs the simulated PVMBG volcano monitoring service.
package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/pvmbg-mock/internal/pvmbg"
)

func main() {
	log := obs.NewLogger("pvmbg-mock")
	cfg := pvmbg.LoadConfig()

	store := pvmbg.NewStore()
	state := pvmbg.NewState(cfg)
	gen := pvmbg.NewGenerator(cfg, store, state, log)
	gen.Seed(time.Now())

	ctx, stop := obs.SignalContext()
	defer stop()
	go gen.Run(ctx)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           pvmbg.NewHandler(cfg, store, state, gen, log),
		ReadHeaderTimeout: 5 * time.Second,
		// requests inherit ctx, so a "hang" outage does not block shutdown
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	log.Info("pvmbg_mock_started", "port", cfg.Port,
		"delay_min_ms", cfg.DelayMin.Milliseconds(), "delay_max_ms", cfg.DelayMax.Milliseconds(),
		"report_interval_sec", cfg.ReportInterval.Seconds())
	if err := obs.Serve(ctx, srv, log); err != nil {
		log.Error("server_failed", "error", err.Error())
		os.Exit(1)
	}
}
