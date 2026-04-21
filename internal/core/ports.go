package core

import (
	"context"
	"os"

	"landing-connector/internal/model"
)

type Source interface {
	Poll(ctx context.Context, limit int) ([]model.KafkaMessage, error)
	Commit(ctx context.Context, window model.BatchWindow) error
	Close() error
}

type Sink interface {
	UploadWindowFile(ctx context.Context, window model.BatchWindow, file *os.File, size int64) (string, error)
}
