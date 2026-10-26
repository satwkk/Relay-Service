package consumer

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	Brokers        []string
	Group          string
	PollInterval   time.Duration
	RequestTimeout time.Duration
	LeaseDuration  time.Duration
	DBTimeout      time.Duration
}

func ConfigFromEnv() (Config, error) {
	cfg := Config{Group: "relay-fanout", PollInterval: 500 * time.Millisecond,
		RequestTimeout: 10 * time.Second, LeaseDuration: 30 * time.Second, DBTimeout: 5 * time.Second}
	for _, broker := range strings.Split(os.Getenv("KAFKA_ADDR"), ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			cfg.Brokers = append(cfg.Brokers, broker)
		}
	}
	if len(cfg.Brokers) == 0 {
		return cfg, fmt.Errorf("KAFKA_ADDR is required")
	}
	if group := strings.TrimSpace(os.Getenv("KAFKA_CONSUMER_GROUP")); group != "" {
		cfg.Group = group
	}
	for name, target := range map[string]*time.Duration{
		"DELIVERY_POLL_INTERVAL":  &cfg.PollInterval,
		"WEBHOOK_TIMEOUT":         &cfg.RequestTimeout,
		"DELIVERY_LEASE_DURATION": &cfg.LeaseDuration,
	} {
		if value := os.Getenv(name); value != "" {
			duration, err := time.ParseDuration(value)
			if err != nil || duration <= 0 {
				return cfg, fmt.Errorf("%s must be a positive duration", name)
			}
			*target = duration
		}
	}
	if cfg.LeaseDuration <= cfg.RequestTimeout+2*cfg.DBTimeout {
		return cfg, fmt.Errorf("DELIVERY_LEASE_DURATION must exceed WEBHOOK_TIMEOUT plus 10s for database operations")
	}
	return cfg, nil
}
