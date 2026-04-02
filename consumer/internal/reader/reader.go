package reader

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Broker  string
	Topic   string
	GroupID string
}

type KafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

func NewReader(cfg Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.Broker},
		Topic:   cfg.Topic,
		GroupID: cfg.GroupID,
	})
}
