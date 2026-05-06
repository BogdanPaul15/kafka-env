package storage

import (
	"context"

	"consumer/model"
)

type Storage interface {
	Store(ctx context.Context, events []model.LogEvent) error
	Close() error
}
