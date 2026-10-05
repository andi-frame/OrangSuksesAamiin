package config

import (
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

type Client struct {
	ID     string
	Secret string
	Role   string // media | lapangan | pusat
}

type Config struct {
	Port       string
	JWTSecret  []byte
	AccessTTL  time.Duration
	RefreshTTL time.Duration
	DBPath     string
	Clients    map[string]Client
}

func Load() Config {
	clients := map[string]Client{
		obs.MustEnv("AUTH_MEDIA_CLIENT_ID"): {
			ID:     obs.MustEnv("AUTH_MEDIA_CLIENT_ID"),
			Secret: obs.MustEnv("AUTH_MEDIA_CLIENT_SECRET"),
			Role:   "media",
		},
		obs.MustEnv("AUTH_LAPANGAN_CLIENT_ID"): {
			ID:     obs.MustEnv("AUTH_LAPANGAN_CLIENT_ID"),
			Secret: obs.MustEnv("AUTH_LAPANGAN_CLIENT_SECRET"),
			Role:   "lapangan",
		},
		obs.MustEnv("AUTH_PUSAT_CLIENT_ID"): {
			ID:     obs.MustEnv("AUTH_PUSAT_CLIENT_ID"),
			Secret: obs.MustEnv("AUTH_PUSAT_CLIENT_SECRET"),
			Role:   "pusat",
		},
	}
	return Config{
		Port:       obs.EnvString("AUTH_SERVICE_PORT", "8084"),
		JWTSecret:  []byte(obs.MustEnv("AUTH_JWT_SECRET")),
		AccessTTL:  time.Duration(obs.EnvInt("AUTH_ACCESS_TTL_SEC", 60)) * time.Second,
		RefreshTTL: time.Duration(obs.EnvInt("AUTH_REFRESH_TTL_SEC", 86400)) * time.Second,
		DBPath:     obs.EnvString("AUTH_DB_PATH", "/data/auth.db"),
		Clients:    clients,
	}
}
