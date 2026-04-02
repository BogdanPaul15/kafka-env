package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"consumer/internal/reader"
	"consumer/internal/storage"
	"consumer/model"
)

type Consumer struct {
	reader  reader.KafkaReader
	storage storage.Storage
}

func NewConsumer(reader reader.KafkaReader, storage storage.Storage) *Consumer {
	return &Consumer{
		reader:  reader,
		storage: storage,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch message: %w", err)
		}

		var events []model.LogEvent
		if err := json.Unmarshal(msg.Value, &events); err != nil {
			log.Printf("failed to unmarshal message offset %d: %v, skipping", msg.Offset, err)
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("failed to commit malformed message: %v", err)
			}
			continue
		}

		if err := c.storage.Store(ctx, events); err != nil {
			return fmt.Errorf("failed to store events: %w", err)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("failed to commit message: %w", err)
		}
	}
}

func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("failed to close reader: %w", err)
	}
	if err := c.storage.Close(); err != nil {
		return fmt.Errorf("failed to close storage: %w", err)
	}
	return nil
}
