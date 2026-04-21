package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"landing-connector/internal/batch"
	"landing-connector/internal/config"
	"landing-connector/internal/core"
	"landing-connector/internal/model"
)

type Runner struct {
	cfg    config.Config
	logger *zap.Logger
	source core.Source
	sink   core.Sink
}

const idlePollTimeout = 2 * time.Second

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

	runID := time.Now().UTC().Format("20060102T150405.000000000Z")
	r.logger.Info("pipeline started",
		zap.String("pipeline_id", r.cfg.PipelineID),
		zap.String("run_id", runID),
		zap.String("topic", r.cfg.Kafka.Topic),
	)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		workersMu sync.Mutex
		workers   = make(map[int32]chan model.KafkaMessage)
		wg        sync.WaitGroup
		commitMu  sync.Mutex
		errOnce   sync.Once
		closeOnce sync.Once
	)

	errCh := make(chan error, 1)
	flushLimiter := make(chan struct{}, r.cfg.Runtime.MaxParallelFlushes)

	reportErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			errCh <- err
			cancel()
		})
	}

	closeWorkers := func() {
		closeOnce.Do(func() {
			workersMu.Lock()
			defer workersMu.Unlock()
			for _, messages := range workers {
				close(messages)
			}
		})
	}
	defer closeWorkers()

	getPartitionQueue := func(partition int32) chan model.KafkaMessage {
		workersMu.Lock()
		defer workersMu.Unlock()

		if messages, exists := workers[partition]; exists {
			return messages
		}

		messages := make(chan model.KafkaMessage, r.cfg.Runtime.PartitionQueueSize)
		workers[partition] = messages
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.runPartitionWorker(runCtx, runID, partition, messages, flushLimiter, &commitMu); err != nil && !errors.Is(err, context.Canceled) {
				reportErr(err)
			}
		}()

		return messages
	}

	for {
		select {
		case err := <-errCh:
			wg.Wait()
			return err
		default:
		}

		if err := ctx.Err(); err != nil {
			cancel()
			wg.Wait()
			return err
		}

		pollCtx, pollCancel := context.WithTimeout(runCtx, idlePollTimeout)
		messages, err := r.source.Poll(pollCtx, 1000)
		pollCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				break
			}
			if runCtx.Err() != nil {
				select {
				case reportedErr := <-errCh:
					wg.Wait()
					return reportedErr
				default:
				}
			}
			cancel()
			wg.Wait()
			return fmt.Errorf("poll kafka: %w", err)
		}

		if len(messages) == 0 {
			break
		}

		for _, msg := range messages {
			queue := getPartitionQueue(msg.Partition)
			select {
			case queue <- msg:
			case err := <-errCh:
				wg.Wait()
				return err
			case <-runCtx.Done():
				select {
				case reportedErr := <-errCh:
					wg.Wait()
					return reportedErr
				default:
					wg.Wait()
					return runCtx.Err()
				}
			}
		}
	}

	closeWorkers()
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}

	r.logger.Info("pipeline finished",
		zap.String("pipeline_id", r.cfg.PipelineID),
		zap.String("run_id", runID),
	)
	return nil
}

func (r *Runner) runPartitionWorker(
	ctx context.Context,
	runID string,
	partition int32,
	messages <-chan model.KafkaMessage,
	flushLimiter chan struct{},
	commitMu *sync.Mutex,
) error {
	assembler := batch.NewAssembler(r.cfg.Batch, runID, time.Now().UTC())

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				window := assembler.Window(time.Now().UTC())
				if len(window.Records) == 0 {
					return nil
				}
				return r.flush(ctx, window, partition, flushLimiter, commitMu)
			}

			if err := assembler.Add(msg, r.cfg.Output.IncludeKey, r.cfg.Output.IncludeHeaders); err != nil {
				return err
			}
			if assembler.ShouldFlush(time.Now().UTC()) {
				window := assembler.Window(time.Now().UTC())
				if err := r.flush(ctx, window, partition, flushLimiter, commitMu); err != nil {
					return err
				}
				assembler = batch.NewAssembler(r.cfg.Batch, runID, time.Now().UTC())
			}
		}
	}
}

func (r *Runner) flush(
	ctx context.Context,
	window model.BatchWindow,
	partition int32,
	flushLimiter chan struct{},
	commitMu *sync.Mutex,
) error {
	select {
	case flushLimiter <- struct{}{}:
		defer func() { <-flushLimiter }()
	case <-ctx.Done():
		return ctx.Err()
	}

	filePath, err := r.sink.WriteWindow(ctx, window)
	if err != nil {
		return fmt.Errorf("write sink: %w", err)
	}
	commitMu.Lock()
	err = r.source.Commit(ctx, window)
	commitMu.Unlock()
	if err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}

	r.logger.Info("batch flushed",
		zap.String("pipeline_id", r.cfg.PipelineID),
		zap.String("run_id", window.RunID),
		zap.String("file_path", filePath),
		zap.Int32("partition", partition),
		zap.Int("records", len(window.Records)),
		zap.Int("bytes_approx", window.BytesApprox),
		zap.Time("started_at", window.StartedAt),
		zap.Time("ended_at", window.EndedAt),
	)
	return nil
}
