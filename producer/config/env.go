package config

import (
	"fmt"
	"os"
	"strconv"

	"producer/internal/server"
	"producer/internal/writer"
)

type Config struct {
	HTTPServer server.Config
	Kafka      writer.Config
}

func LoadFromEnv() (*Config, error) {
	cfg := &Config{}

	port, err := strconv.Atoi(getEnv("HTTP_PORT", "8080"))
	if err != nil {
		return nil, fmt.Errorf("invalid HTTP_PORT: %w", err)
	}
	cfg.HTTPServer.HTTPPort = port

	cfg.Kafka.Broker = os.Getenv("KAFKA_BROKER")
	if cfg.Kafka.Broker == "" {
		return nil, fmt.Errorf("KAFKA_BROKER is required")
	}

	cfg.Kafka.Topic = getEnv("KAFKA_TOPIC", "my-topic")

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
