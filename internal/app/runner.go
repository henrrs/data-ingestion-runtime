package app

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"landing-connector/internal/batch"
	"landing-connector/internal/config"
	"landing-connector/internal/core"
)

type Runner struct {
	cfg    config.Config
	logger *zap.Logger
	source core.Source
	sink   core.Sink
}

func NewRunner(cfg config.Config, logger *zap.Logger) (*Runner, error) {
	source, err := core.BuildSource(cfg)
	if err != nil {
		return nil, err
	}

	sink, err := core.BuildSink(context.Background(), cfg)
	if err != nil {
		_ = source.Close()
		return nil, err
	}

	return &Runner{
		cfg:    cfg,
		logger: logger,
		source: source,
		sink:   sink,
	}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	defer func() { _ = r.source.Close() }()

	runID := time.Now().UTC().Format("20060102T150405Z")
	r.logger.Info("pipeline started",
		zap.String("pipeline_id", r.cfg.PipelineID),
		zap.String("run_id", runID),
		zap.String("topic", r.cfg.Kafka.Topic),
	)

	assembler := batch.NewAssembler(r.cfg.Batch, runID, time.Now().UTC())

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		messages, err := r.source.Poll(ctx, 1000)
		if err != nil {
			return fmt.Errorf("poll kafka: %w", err)
		}

		if len(messages) == 0 {
			break
		}

		for _, msg := range messages {
			if err := assembler.Add(msg, r.cfg.Output.IncludeKey, r.cfg.Output.IncludeHeaders); err != nil {
				return err
			}
			if assembler.ShouldFlush(time.Now().UTC()) {
				if err := r.flush(ctx, assembler); err != nil {
					return err
				}
				assembler = batch.NewAssembler(r.cfg.Batch, runID, time.Now().UTC())
			}
		}
	}

	if len(assembler.Window(time.Now().UTC()).Records) > 0 {
		if err := r.flush(ctx, assembler); err != nil {
			return err
		}
	}

	r.logger.Info("pipeline finished",
		zap.String("pipeline_id", r.cfg.PipelineID),
		zap.String("run_id", runID),
	)
	return nil
}

func (r *Runner) flush(ctx context.Context, assembler *batch.Assembler) error {
	window := assembler.Window(time.Now().UTC())
	filePath, err := r.sink.WriteWindow(ctx, window)
	if err != nil {
		return fmt.Errorf("write sink: %w", err)
	}
	if err := r.source.Commit(ctx, window); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}

	r.logger.Info("batch flushed",
		zap.String("pipeline_id", r.cfg.PipelineID),
		zap.String("run_id", window.RunID),
		zap.String("file_path", filePath),
		zap.Int("records", len(window.Records)),
		zap.Int("bytes_approx", window.BytesApprox),
		zap.Time("started_at", window.StartedAt),
		zap.Time("ended_at", window.EndedAt),
	)
	return nil
}
