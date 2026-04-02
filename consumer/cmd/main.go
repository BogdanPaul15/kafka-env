package main

import (
	"context"
	"log"

	"consumer/config"
	"consumer/internal/consumer"
	"consumer/internal/reader"
	"consumer/internal/storage"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	store, err := storage.NewOpenSearchClient(cfg.OpenSearch)
	if err != nil {
		log.Fatalf("failed to create opensearch client: %v", err)
	}
	reader := reader.NewReader(cfg.Kafka)

	consumer := consumer.NewConsumer(reader, store)

	if err := consumer.Run(context.Background()); err != nil {
		log.Fatalf("consumer error: %v", err)
	}

	consumer.Close()
}
