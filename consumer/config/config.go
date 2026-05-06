package config

import (
	"fmt"
	"os"
	"strings"

	"consumer/internal/reader"
	"consumer/internal/storage"
)

type Config struct {
	Kafka      reader.Config
	OpenSearch storage.Config
}

func LoadFromEnv() (*Config, error) {
	cfg := &Config{}

	cfg.Kafka.Broker = os.Getenv("KAFKA_BROKER")
	if cfg.Kafka.Broker == "" {
		return nil, fmt.Errorf("KAFKA_BROKER is required")
	}

	cfg.Kafka.Topic = getEnv("KAFKA_TOPIC", "my-topic")
	cfg.Kafka.GroupID = getEnv("KAFKA_GROUP_ID", "my-consumer-group")

	addresses := os.Getenv("OPENSEARCH_ADDRESSES")
	if addresses == "" {
		return nil, fmt.Errorf("OPENSEARCH_ADDRESSES is required")
	}
	cfg.OpenSearch.Addresses = strings.Split(addresses, ",")

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
