package core

import (
	"context"

	"landing-connector/internal/model"
)

type Source interface {
	Poll(ctx context.Context, limit int) ([]model.KafkaMessage, error)
	Commit(ctx context.Context, window model.BatchWindow) error
	Close() error
}

type Sink interface {
	WriteWindow(ctx context.Context, window model.BatchWindow) (string, error)
}
