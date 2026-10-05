package config

import (
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

type Config struct {
	Port       string
	JWTSecret  []byte
	AggURL     string
	AggKey     string
	AggTimeout time.Duration
	RateRPS    int
	RateBurst  int
}

func Load() Config {
	return Config{
		Port:       obs.EnvString("CLIENT_API_PORT", "8080"),
		JWTSecret:  []byte(obs.MustEnv("AUTH_JWT_SECRET")),
		AggURL:     obs.EnvString("AGGREGATOR_URL", "http://aggregator:8083"),
		AggKey:     obs.MustEnv("AGGREGATOR_SERVICE_KEY"),
		AggTimeout: obs.EnvSeconds("AGGREGATOR_TIMEOUT_SEC", 10*time.Second),
		RateRPS:    obs.EnvInt("CLIENT_RATE_LIMIT_RPS", 200),
		RateBurst:  obs.EnvInt("CLIENT_RATE_LIMIT_BURST", 400),
	}
}
