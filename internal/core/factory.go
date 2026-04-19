package core

import (
	"context"
	"fmt"
	"strings"

	"landing-connector/internal/adapters/sink/adls"
	"landing-connector/internal/adapters/sink/minio"
	"landing-connector/internal/adapters/source/kafka"
	"landing-connector/internal/config"
)

func BuildSource(cfg config.Config) (Source, error) {
	switch strings.ToLower(cfg.Source.Type) {
	case "", "kafka":
		return kafka.NewSource(cfg.Kafka)
	default:
		return nil, fmt.Errorf("unsupported source type: %s", cfg.Source.Type)
	}
}

func BuildSink(ctx context.Context, cfg config.Config) (Sink, error) {
	switch strings.ToLower(cfg.Sink.Type) {
	case "", "adls":
		return adls.New(ctx, cfg)
	case "minio":
		return minio.New(ctx, cfg)
	default:
		return nil, fmt.Errorf("unsupported sink type: %s", cfg.Sink.Type)
	}
}
