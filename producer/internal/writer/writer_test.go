package writer

import (
	"context"
	"errors"
	"producer/internal/model"
	"testing"

	"github.com/segmentio/kafka-go"
)

type mockKafkaWriter struct {
	messages []kafka.Message
	err      error
}

func (m *mockKafkaWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	if m.err != nil {
		return m.err
	}
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockKafkaWriter) Close() error { return nil }

func TestWrite_Success(t *testing.T) {
	mock := &mockKafkaWriter{}
	m := NewKafkaWriter(mock)

	events := []model.LogEvent{
		{
			Timestamp: "2026-04-01 22:24:11.483837841",
			Level:     "INFO",
			Service:   "payment-service",
			TraceID:   "abcd-efgh-ijkl-mnop",
			Message:   "test payment",
		},
		{
			Timestamp: "2026-04-01 22:25:13.712384989",
			Level:     "ERROR",
			Service:   "ad-service",
			TraceID:   "abcd-efgh-ijkl-mnop",
			Message:   "test payment",
		},
		{
			Timestamp: "2026-04-01 22:26:59.812347929",
			Level:     "FATAL",
			Service:   "payment-service",
			TraceID:   "abcd-efgh-ijkl-mnop",
			Message:   "test payment",
		},
	}

	if err := m.Write(context.Background(), events); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(mock.messages))
	}
}

func TestWrite_KafkaError(t *testing.T) {
	mock := &mockKafkaWriter{err: errors.New("broker down")}
	w := NewKafkaWriter(mock)

	err := w.Write(context.Background(), []model.LogEvent{
		{
			Timestamp: "2026-04-01 22:24:11.483837841",
			Level:     "INFO",
			Service:   "payment-service",
			TraceID:   "abcd-efgh-ijkl-mnop",
			Message:   "test payment",
		},
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
