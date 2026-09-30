// Package pvmbg implements the PVMBG mock: a slow, flaky "legacy" volcano
// monitoring service with an operator-controlled outage and a runtime schema
// change (confidence_level), as required by the M1 specification.
package pvmbg

import (
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

// Config holds every tunable of the mock. Credentials come from the
// environment only.
type Config struct {
	Port        string
	TokenSecret string // signing secret of PVMBG access tokens (independent of BMKG)
	AdminToken  string // X-Admin-Token for /admin/*

	DelayMin time.Duration // initial random delay range (spec: 500 ms - 3 s); changeable at runtime
	DelayMax time.Duration

	ReportInterval time.Duration // one new report per interval (spec: >= 1 / 10 s)
	SeedCount      int           // historical records generated at start (spec: >= 20)
	HangMax        time.Duration // upper bound of a "hang" outage per request
}

// LoadConfig reads the configuration from environment variables.
func LoadConfig() Config {
	return Config{
		Port:           obs.EnvString("PVMBG_PORT", "8082"),
		TokenSecret:    obs.MustEnv("PVMBG_TOKEN_SECRET"),
		AdminToken:     obs.MustEnv("PVMBG_ADMIN_TOKEN"),
		DelayMin:       obs.EnvMillis("PVMBG_DELAY_MIN_MS", 500*time.Millisecond),
		DelayMax:       obs.EnvMillis("PVMBG_DELAY_MAX_MS", 3*time.Second),
		ReportInterval: obs.EnvSeconds("PVMBG_REPORT_INTERVAL_SEC", 10*time.Second),
		SeedCount:      max(obs.EnvInt("PVMBG_SEED_COUNT", 24), 20),
		HangMax:        obs.EnvSeconds("PVMBG_HANG_MAX_SEC", 60*time.Second),
	}
}
