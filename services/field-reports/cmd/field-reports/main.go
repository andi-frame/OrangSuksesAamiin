// Command field-reports processes Tim Lapangan field reports exactly once
// (see internal/processor). HTTP: /health, /ready, /metrics, /stats and
// GET /reports/{id} (used by the Client-Facing API).
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/field-reports/internal/processor"
)

func main() {
	log := obs.NewLogger("field-reports")
	host, _ := os.Hostname()
	cfg := processor.Config{
		Brokers:       strings.Split(obs.MustEnv("KAFKA_BROKERS"), ","),
		InTopic:       obs.EnvString("FIELD_REPORTS_TOPIC", "field-reports"),
		OutTopic:      obs.EnvString("FIELD_REPORTS_PROCESSED_TOPIC", "field-reports.processed"),
		Group:         obs.EnvString("KAFKA_GROUP", "field-report-processor"),
		TransactionID: obs.EnvString("KAFKA_TRANSACTIONAL_ID", "field-report-processor"),
		Instance:      obs.EnvString("POD_NAME", host),
		Delay:         obs.EnvMillis("FIELD_REPORT_PROCESS_DELAY_MS", 0),
	}
	port := obs.EnvString("FIELD_REPORTS_PORT", "8093")
	p := processor.New(cfg, log)

	ctx, stop := obs.SignalContext()
	defer stop()

	// The broker may not be there yet: keep trying instead of exiting
	// (no CrashLoopBackOff); /ready stays 503 until the processor runs.
	go func() {
		for ctx.Err() == nil {
			if err := p.Rebuild(ctx); err != nil {
				log.Warn("rebuild_failed", "error", err.Error(), "retry_in_s", 3)
				sleep(ctx, 3*time.Second)
				continue
			}
			if err := p.Run(ctx); err != nil {
				log.Warn("processor_restarting", "error", err.Error(), "retry_in_s", 1)
				p.Reset()
				sleep(ctx, time.Second)
				continue
			}
			return
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", obs.LivenessHandler("field-reports", nil))
	mux.HandleFunc("GET /ready", obs.ReadinessHandler("field-reports", map[string]obs.Check{
		"processor": func(context.Context) error {
			if !p.Ready() {
				return errors.New("rebuilding the dedup set / joining the group")
			}
			return nil
		},
	}))
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) { obs.WriteJSON(w, 200, p.Stats()) })
	mux.HandleFunc("GET /reports/{id}", func(w http.ResponseWriter, r *http.Request) {
		rec, ok := p.Get(r.PathValue("id"))
		if !ok {
			obs.WriteError(w, http.StatusNotFound, "not_found", "no processed report with this id")
			return
		}
		obs.WriteJSON(w, http.StatusOK, rec)
	})
	srv := &http.Server{Addr: ":" + port, Handler: obs.Middleware(log, "/health", "/ready")(mux), ReadHeaderTimeout: 5 * time.Second}
	if err := obs.Serve(ctx, srv, log); err != nil {
		log.Error("fatal", "what", "server", "error", err.Error())
		os.Exit(1)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
