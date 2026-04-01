package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"producer/internal/model"

	kafka "github.com/segmentio/kafka-go"
)

type Config struct {
	Broker string
	Topic  string
}

type MessageWriter interface {
	Write(ctx context.Context, events []model.LogEvent) error
	Close() error
}

var _ MessageWriter = (*Writer)(nil)

type Writer struct {
	kw *kafka.Writer
}

func NewWriter(cfg Config) *Writer {
	return &Writer{
		kw: &kafka.Writer{
			Addr:     kafka.TCP(cfg.Broker),
			Topic:    cfg.Topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (w *Writer) Write(ctx context.Context, events []model.LogEvent) error {
	msgs := make([]kafka.Message, len(events))

	for i, event := range events {
		value, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		msgs[i] = kafka.Message{
			Key:   []byte(event.Level),
			Value: value,
			Time:  time.Now(),
		}
	}

	if err := w.kw.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("failed to write messages: %w", err)
	}
	return nil
}

func (w *Writer) Close() error {
	if err := w.kw.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return nil
}
