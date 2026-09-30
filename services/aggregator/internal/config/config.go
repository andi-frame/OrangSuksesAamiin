// Package config reads the Aggregator's settings from the environment.
// Credentials have no defaults: a missing one stops the process with a clear
// message instead of silently running with a shared or hard-coded secret.
package config

import (
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

type Config struct {
	Port       string
	ServiceKey string // X-Service-Key guarding /internal/* (used by the Client-Facing API)

	BMKGURL    string
	BMKGKey    string // X-BMKG-Key       - valid for BMKG only
	PVMBGURL   string
	PVMBGToken string // Bearer <token>   - valid for PVMBG only

	PollInterval  time.Duration
	PollOverlap   time.Duration
	SourceTimeout time.Duration

	BreakerThreshold int
	BreakerOpenFor   time.Duration

	PendingWarningTTL time.Duration

	RepositoryBackend string // memory | postgres
	PublisherBackend  string // log | kafka
	VolcanoTablePath  string

	RelayInterval time.Duration
	RelayBatch    int
	RelayBackoff  time.Duration
}

func Load() Config {
	return Config{
		Port:              obs.EnvString("AGGREGATOR_PORT", "8083"),
		ServiceKey:        obs.MustEnv("AGGREGATOR_SERVICE_KEY"),
		BMKGURL:           obs.EnvString("BMKG_URL", "http://bmkg-mock:8081"),
		BMKGKey:           obs.MustEnv("BMKG_API_KEY"),
		PVMBGURL:          obs.EnvString("PVMBG_URL", "http://pvmbg-mock:8082"),
		PVMBGToken:        obs.MustEnv("PVMBG_GATEWAY_TOKEN"),
		PollInterval:      obs.EnvSeconds("POLL_INTERVAL_SEC", 3*time.Second),
		PollOverlap:       obs.EnvSeconds("POLL_OVERLAP_SEC", 5*time.Second),
		SourceTimeout:     obs.EnvSeconds("SOURCE_TIMEOUT_SEC", 5*time.Second),
		BreakerThreshold:  obs.EnvInt("BREAKER_FAILURE_THRESHOLD", 3),
		BreakerOpenFor:    obs.EnvSeconds("BREAKER_OPEN_SEC", 10*time.Second),
		PendingWarningTTL: obs.EnvSeconds("PENDING_WARNING_TTL_SEC", 120*time.Second),
		RepositoryBackend: obs.EnvString("REPOSITORY_BACKEND", "memory"),
		PublisherBackend:  obs.EnvString("PUBLISHER_BACKEND", "log"),
		VolcanoTablePath:  obs.EnvString("VOLCANO_TABLE_PATH", "/app/config/volcanoes.json"),
		RelayInterval:     obs.EnvMillis("RELAY_INTERVAL_MS", 500*time.Millisecond),
		RelayBatch:        obs.EnvInt("RELAY_BATCH", 100),
		RelayBackoff:      obs.EnvSeconds("RELAY_MAX_BACKOFF_SEC", 10*time.Second),
	}
}
