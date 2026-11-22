package config

import (
	"strconv"
	"strings"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

type Config struct {
	Port       string
	JWTSecret  []byte
	AggURL     string
	AggKey     string
	AggTimeout time.Duration

	// admission control (P2 resource bound + P5 criterion 5 priorities)
	RateRPS     int
	RateBurst   int
	Reserve     float64 // share kept for Tim Lapangan / Pusat
	MaxInFlight int

	// field reports (P3 criterion 6)
	KafkaBrokers     []string // empty = POST /v1/field-reports answers 503
	ReportsTopic     string
	ReportAckTimeout time.Duration
	ReportsURL       string

	AuthURL string // analytics dashboard login (same origin)
}

func Load() Config {
	reserve, err := strconv.ParseFloat(obs.EnvString("CLIENT_PRIORITY_RESERVE", "0.3"), 64)
	if err != nil {
		reserve = 0.3
	}
	var brokers []string
	if b := strings.TrimSpace(obs.EnvString("KAFKA_BROKERS", "")); b != "" {
		brokers = strings.Split(b, ",")
	}
	return Config{
		Port:             obs.EnvString("CLIENT_API_PORT", "8080"),
		JWTSecret:        []byte(obs.MustEnv("AUTH_JWT_SECRET")),
		AggURL:           obs.EnvString("AGGREGATOR_URL", "http://aggregator:8083"),
		AggKey:           obs.MustEnv("AGGREGATOR_SERVICE_KEY"),
		AggTimeout:       obs.EnvSeconds("AGGREGATOR_TIMEOUT_SEC", 10*time.Second),
		RateRPS:          obs.EnvInt("CLIENT_RATE_LIMIT_RPS", 500),
		RateBurst:        obs.EnvInt("CLIENT_RATE_LIMIT_BURST", 1000),
		Reserve:          reserve,
		MaxInFlight:      obs.EnvInt("CLIENT_MAX_IN_FLIGHT", 64),
		KafkaBrokers:     brokers,
		ReportsTopic:     obs.EnvString("FIELD_REPORTS_TOPIC", "field-reports"),
		ReportAckTimeout: obs.EnvSeconds("FIELD_REPORT_ACK_TIMEOUT_SEC", 5*time.Second),
		ReportsURL:       strings.TrimRight(obs.EnvString("FIELD_REPORTS_URL", ""), "/"),
		AuthURL:          strings.TrimRight(obs.EnvString("AUTH_URL", "http://auth-service:8084"), "/"),
	}
}
