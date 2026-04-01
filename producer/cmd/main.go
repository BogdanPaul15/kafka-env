package main

import (
	"log"

	"producer/config"
	"producer/internal/server"
	"producer/internal/writer"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	w := writer.NewWriter(cfg.Kafka)
	defer func() {
		if err := w.Close(); err != nil {
			log.Fatalf("Failed to close writer: %v", err)
		}
	}()

	srv := server.NewServer(w, cfg.HTTPServer)

	if err := srv.Run(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
