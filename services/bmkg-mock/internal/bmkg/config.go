// Package bmkg implements the BMKG mock: a passive, low-latency seismic and
// tsunami-warning service that follows the contract of the M1 specification.
package bmkg

import (
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

// Config holds every tunable of the mock. All credentials come from the
// environment; there are no hard-coded defaults for secrets.
type Config struct {
	Port       string
	APIKey     string // X-BMKG-Key
	AdminToken string // X-Admin-Token for /admin/*

	DelayMin time.Duration // fixed response delay range (spec: 50-150 ms)
	DelayMax time.Duration

	EventInterval   time.Duration // one new event per interval (spec: >= 1 / 10 s)
	WarningDelayMin time.Duration // a tsunami warning is issued this long after its event
	WarningDelayMax time.Duration
	SeedCount       int // historical records generated at start (spec: >= 20)
}

// LoadConfig reads the configuration from environment variables.
func LoadConfig() Config {
	return Config{
		Port:            obs.EnvString("BMKG_PORT", "8081"),
		APIKey:          obs.MustEnv("BMKG_API_KEY"),
		AdminToken:      obs.MustEnv("BMKG_ADMIN_TOKEN"),
		DelayMin:        obs.EnvMillis("BMKG_DELAY_MIN_MS", 50*time.Millisecond),
		DelayMax:        obs.EnvMillis("BMKG_DELAY_MAX_MS", 150*time.Millisecond),
		EventInterval:   obs.EnvSeconds("BMKG_EVENT_INTERVAL_SEC", 10*time.Second),
		WarningDelayMin: obs.EnvSeconds("BMKG_WARNING_DELAY_MIN_SEC", 2*time.Second),
		WarningDelayMax: obs.EnvSeconds("BMKG_WARNING_DELAY_MAX_SEC", 8*time.Second),
		SeedCount:       max(obs.EnvInt("BMKG_SEED_COUNT", 24), 20),
	}
}
