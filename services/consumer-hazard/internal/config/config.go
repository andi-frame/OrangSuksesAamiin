package config

import (
	"strings"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
)

type Config struct {
	Name    string
	Port    string
	Brokers []string
	Topic   string
	Group   string
}

func Load() Config {
	brokers := obs.MustEnv("KAFKA_BROKERS")
	return Config{
		Name:    obs.EnvString("CONSUMER_NAME", "consumer"),
		Port:    obs.EnvString("CONSUMER_PORT", "8091"),
		Brokers: strings.Split(brokers, ","),
		Topic:   obs.EnvString("KAFKA_TOPIC", "hazard.events"),
		Group:   obs.MustEnv("KAFKA_GROUP"),
	}
}
