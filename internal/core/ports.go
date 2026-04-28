package core

import (
	"context"
	"io"

	"landing-connector/internal/model"
)

type Source interface {
	Poll(ctx context.Context, limit int) ([]model.KafkaMessage, error)
	Commit(ctx context.Context, window model.BatchWindow) error
	Close() error
}

type Sink interface {
	UploadWindowStream(ctx context.Context, window model.BatchWindow, reader io.Reader) (string, error)
}
