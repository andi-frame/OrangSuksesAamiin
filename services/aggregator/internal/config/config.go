// Package config reads the Aggregator's settings from the environment.
// Credentials have no defaults: a missing one stops the process with a clear
// message instead of silently running with a shared or hard-coded secret.
package config

import (
	"os"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

type Config struct {
	Port       string
	ServiceKey string // X-Service-Key guarding /internal/* (used by the Client-Facing API)
	OpsToken   string // Bearer token of BNPB Internal Ops: dead-letter inspection and reprocessing
	AuditToken string // Bearer token of the auditor (Lampiran A, /internal/audit/*)

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

	RepositoryBackend                               string // memory | postgres | sharded
	PublisherBackend                                string // log | kafka
	VolcanoTablePath                                string
	SchemaRulesPath                                 string
	SatelliteURL, SatelliteToken, ProvinceTablePath string
	SatelliteLateness                               time.Duration

	// M2 Problem 5: Sensor Pantai (enabled when PANTAI_AUTH_TOKEN is set)
	SensorURL          string
	SensorToken        string
	SensorPollInterval time.Duration
	SensorTimeout      time.Duration
	SensorFreezeAfter  time.Duration
	SensorMaxLag       time.Duration
	SensorPageSize     int
	SensorMaxPages     int
	SensorBreakerOpen  time.Duration

	RelayInterval time.Duration
	RelayBatch    int
	RelayBackoff  time.Duration

	// M2 Problem 2: one ingesting instance
	LeaderElection string // none | kubernetes
	Identity       string // pod name
	Namespace      string
	LeaseName      string
	LeaseDuration  time.Duration
	RenewDeadline  time.Duration
	RetryPeriod    time.Duration
	StatusMaxAge   time.Duration // a stored source status older than this is shown as stale

	// M2 Problem 3: dead letters and message limits
	MessageMaxBytes int
	MessageMaxDepth int
	DLQMaxAttempts  int
	DLQBaseBackoff  time.Duration
	DLQMaxBackoff   time.Duration
}

func Load() Config {
	host, _ := os.Hostname()
	return Config{
		SatelliteURL:       obs.EnvString("SATELLITE_FEED_URL", "https://satellite.naufarrel.dev"),
		SatelliteToken:     os.Getenv("SATELLITE_FEED_TOKEN"),
		ProvinceTablePath:  obs.EnvString("SATELLITE_PROVINCES_PATH", "/app/config/provinces.json"),
		SatelliteLateness:  obs.EnvSeconds("SATELLITE_LATENESS_SEC", 24*time.Hour),
		SensorURL:          obs.EnvString("SENSOR_PANTAI_URL", "http://sensor-pantai:8080"),
		SensorToken:        os.Getenv("PANTAI_AUTH_TOKEN"),
		SensorPollInterval: obs.EnvMillis("SENSOR_POLL_INTERVAL_MS", time.Second),
		SensorTimeout:      obs.EnvSeconds("SENSOR_TIMEOUT_SEC", 2*time.Second),
		SensorFreezeAfter:  obs.EnvSeconds("SENSOR_FREEZE_AFTER_SEC", 15*time.Second),
		SensorMaxLag:       obs.EnvSeconds("SENSOR_MAX_LAG_SEC", 2*time.Hour),
		SensorPageSize:     obs.EnvInt("SENSOR_PAGE_SIZE", 500),
		SensorMaxPages:     obs.EnvInt("SENSOR_MAX_PAGES", 20),
		SensorBreakerOpen:  obs.EnvSeconds("SENSOR_BREAKER_OPEN_SEC", 10*time.Second),
		Port:               obs.EnvString("AGGREGATOR_PORT", "8083"),
		ServiceKey:         obs.MustEnv("AGGREGATOR_SERVICE_KEY"),
		OpsToken:           obs.MustEnv("AGGREGATOR_OPS_TOKEN"),
		AuditToken:         obs.MustEnv("AUDIT_TOKEN"),
		BMKGURL:            obs.EnvString("BMKG_URL", "http://bmkg-mock:8081"),
		BMKGKey:            obs.MustEnv("BMKG_API_KEY"),
		PVMBGURL:           obs.EnvString("PVMBG_URL", "http://pvmbg-mock:8082"),
		PVMBGToken:         obs.MustEnv("PVMBG_GATEWAY_TOKEN"),
		PollInterval:       obs.EnvSeconds("POLL_INTERVAL_SEC", 3*time.Second),
		PollOverlap:        obs.EnvSeconds("POLL_OVERLAP_SEC", 5*time.Second),
		SourceTimeout:      obs.EnvSeconds("SOURCE_TIMEOUT_SEC", 5*time.Second),
		BreakerThreshold:   obs.EnvInt("BREAKER_FAILURE_THRESHOLD", 3),
		BreakerOpenFor:     obs.EnvSeconds("BREAKER_OPEN_SEC", 10*time.Second),
		PendingWarningTTL:  obs.EnvSeconds("PENDING_WARNING_TTL_SEC", 120*time.Second),
		RepositoryBackend:  obs.EnvString("REPOSITORY_BACKEND", "memory"),
		PublisherBackend:   obs.EnvString("PUBLISHER_BACKEND", "log"),
		VolcanoTablePath:   obs.EnvString("VOLCANO_TABLE_PATH", "/app/config/volcanoes.json"),
		SchemaRulesPath:    obs.EnvString("SCHEMA_RULES_PATH", "/app/rules/schema-rules.json"),
		RelayInterval:      obs.EnvMillis("RELAY_INTERVAL_MS", 500*time.Millisecond),
		RelayBatch:         obs.EnvInt("RELAY_BATCH", 100),
		RelayBackoff:       obs.EnvSeconds("RELAY_MAX_BACKOFF_SEC", 10*time.Second),
		LeaderElection:     obs.EnvString("LEADER_ELECTION", "none"),
		Identity:           obs.EnvString("POD_NAME", host),
		Namespace:          obs.EnvString("POD_NAMESPACE", ""),
		LeaseName:          obs.EnvString("LEASE_NAME", "aggregator-ingest"),
		LeaseDuration:      obs.EnvSeconds("LEASE_DURATION_SEC", 15*time.Second),
		RenewDeadline:      obs.EnvSeconds("LEASE_RENEW_DEADLINE_SEC", 10*time.Second),
		RetryPeriod:        obs.EnvSeconds("LEASE_RETRY_PERIOD_SEC", 2*time.Second),
		StatusMaxAge:       obs.EnvSeconds("SOURCE_STATUS_MAX_AGE_SEC", 30*time.Second),
		MessageMaxBytes:    obs.EnvInt("MESSAGE_MAX_BYTES", 1<<20),
		MessageMaxDepth:    obs.EnvInt("MESSAGE_MAX_DEPTH", 32),
		DLQMaxAttempts:     obs.EnvInt("DLQ_MAX_ATTEMPTS", 8),
		DLQBaseBackoff:     obs.EnvSeconds("DLQ_BASE_BACKOFF_SEC", 2*time.Second),
		DLQMaxBackoff:      obs.EnvSeconds("DLQ_MAX_BACKOFF_SEC", 120*time.Second),
	}
}
