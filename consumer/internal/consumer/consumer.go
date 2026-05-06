package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"consumer/internal/reader"
	"consumer/internal/storage"
	"consumer/model"

	"github.com/segmentio/kafka-go"
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
	const maxBatchSize = 1000
	const flushInterval = 100 * time.Millisecond

	batch := make([]model.LogEvent, 0, maxBatchSize)
	messages := make([]kafka.Message, 0, maxBatchSize)

	flush := func(flushCtx context.Context) error {
		if len(batch) == 0 {
			return nil
		}

		if err := c.storage.Store(flushCtx, batch); err != nil {
			return fmt.Errorf("failed to store events: %w", err)
		}

		if err := c.reader.CommitMessages(flushCtx, messages...); err != nil {
			return fmt.Errorf("failed to commit messages: %w", err)
		}

		batch = batch[:0]
		messages = messages[:0]
		return nil
	}

	for {
		fetchCtx, cancel := context.WithTimeout(ctx, flushInterval)
		msg, err := c.reader.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if fetchCtx.Err() == context.DeadlineExceeded {
				if err := flush(ctx); err != nil {
					return err
				}
				continue
			}

			if flushErr := flush(context.Background()); flushErr != nil {
				return flushErr
			}
			return fmt.Errorf("failed to fetch message: %w", err)
		}

		var event model.LogEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("failed to unmarshal message offset %d: %v, skipping", msg.Offset, err)
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("failed to commit malformed message: %v", err)
			}
			continue
		}

		batch = append(batch, event)
		messages = append(messages, msg)

		if len(batch) >= maxBatchSize {
			if err := flush(ctx); err != nil {
				return err
			}
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
