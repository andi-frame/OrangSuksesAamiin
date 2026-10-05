package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/api"
	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/config"
	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/store"
	"github.com/andi-frame/orangsuksesaamiin/services/auth-service/internal/token"
)

func main() {
	log := obs.NewLogger("auth-service")
	cfg := config.Load()

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		fatal(log, "auth_db_open", err)
	}
	defer st.Close()

	svc := token.New(cfg, st)
	srv := &api.Server{Tokens: svc, Store: st, Log: log}

	httpSrv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Info("auth_service_started", "port", cfg.Port, "db_path", cfg.DBPath,
		"access_ttl_sec", cfg.AccessTTL.Seconds(), "refresh_ttl_sec", cfg.RefreshTTL.Seconds())

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
