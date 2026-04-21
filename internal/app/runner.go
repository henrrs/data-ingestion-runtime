package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"landing-connector/internal/adapters/sink/fileformat"
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

type flushTask struct {
	partition int32
	sequence  int64
	window    model.BatchWindow
}

type encodedTask struct {
	flushTask
	file *os.File
	size int64
}

type uploadedTask struct {
	flushTask
	filePath string
}

type partitionCommitState struct {
	nextSequence int64
	pending      map[int64]uploadedTask
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
		partitionsMu sync.Mutex
		partitions   = make(map[int32]chan model.KafkaMessage)
		closeOnce    sync.Once
		partitionsWG sync.WaitGroup
		encodesWG    sync.WaitGroup
		uploadsWG    sync.WaitGroup
		commitWG     sync.WaitGroup
		errOnce      sync.Once
	)

	errCh := make(chan error, 1)
	flushTasks := make(chan flushTask, r.cfg.Runtime.FlushQueueSize)
	encodedTasks := make(chan encodedTask, r.cfg.Runtime.FlushQueueSize)
	uploadedTasks := make(chan uploadedTask, r.cfg.Runtime.FlushQueueSize)

	reportErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			errCh <- err
			cancel()
		})
	}

	closePartitions := func() {
		closeOnce.Do(func() {
			partitionsMu.Lock()
			defer partitionsMu.Unlock()
			for _, messages := range partitions {
				close(messages)
			}
		})
	}
	defer closePartitions()

	for i := 0; i < r.cfg.Runtime.MaxParallelEncodes; i++ {
		encodesWG.Add(1)
		go func() {
			defer encodesWG.Done()
			if err := r.runEncodeWorker(runCtx, flushTasks, encodedTasks); err != nil && !errors.Is(err, context.Canceled) {
				reportErr(err)
			}
		}()
	}

	for i := 0; i < r.cfg.Runtime.MaxParallelUploads; i++ {
		uploadsWG.Add(1)
		go func() {
			defer uploadsWG.Done()
			if err := r.runUploadWorker(runCtx, encodedTasks, uploadedTasks); err != nil && !errors.Is(err, context.Canceled) {
				reportErr(err)
			}
		}()
	}

	commitWG.Add(1)
	go func() {
		defer commitWG.Done()
		if err := r.runCommitCoordinator(runCtx, uploadedTasks); err != nil && !errors.Is(err, context.Canceled) {
			reportErr(err)
		}
	}()

	commitDone := make(chan struct{})
	go func() {
		commitWG.Wait()
		close(commitDone)
	}()

	getPartitionQueue := func(partition int32) chan model.KafkaMessage {
		partitionsMu.Lock()
		defer partitionsMu.Unlock()

		if messages, exists := partitions[partition]; exists {
			return messages
		}

		messages := make(chan model.KafkaMessage, r.cfg.Runtime.PartitionQueueSize)
		partitions[partition] = messages
		partitionsWG.Add(1)
		go func() {
			defer partitionsWG.Done()
			if err := r.runPartitionWorker(runCtx, runID, partition, messages, flushTasks); err != nil && !errors.Is(err, context.Canceled) {
				reportErr(err)
			}
		}()

		return messages
	}

	var runErr error

pollLoop:
	for {
		select {
		case err := <-errCh:
			runErr = err
			break pollLoop
		default:
		}

		if err := ctx.Err(); err != nil {
			runErr = err
			cancel()
			break
		}

		pollCtx, pollCancel := context.WithTimeout(runCtx, idlePollTimeout)
		messages, err := r.source.Poll(pollCtx, 1000)
		pollCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && runCtx.Err() == nil && ctx.Err() == nil {
				break
			}
			if runCtx.Err() != nil {
				break
			}
			runErr = fmt.Errorf("poll kafka: %w", err)
			cancel()
			break
		}

		if len(messages) == 0 {
			break
		}

		for _, msg := range messages {
			queue := getPartitionQueue(msg.Partition)
			if err := sendWithContext(runCtx, queue, msg); err != nil {
				if runErr == nil {
					runErr = err
				}
				break pollLoop
			}
		}
	}

	closePartitions()
	partitionsWG.Wait()
	close(flushTasks)
	encodesWG.Wait()
	close(encodedTasks)
	uploadsWG.Wait()
	close(uploadedTasks)
	<-commitDone

	if runErr == nil {
		select {
		case err := <-errCh:
			runErr = err
		default:
		}
	}

	if runErr != nil {
		return runErr
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
	flushTasks chan<- flushTask,
) error {
	assembler := batch.NewAssembler(r.cfg.Batch, runID, time.Now().UTC())
	var sequence int64

	flushWindow := func(now time.Time) error {
		window := assembler.Window(now)
		if len(window.Records) == 0 {
			return nil
		}

		task := flushTask{
			partition: partition,
			sequence:  sequence,
			window:    window,
		}
		if err := sendWithContext(ctx, flushTasks, task); err != nil {
			return err
		}

		sequence++
		assembler = batch.NewAssembler(r.cfg.Batch, runID, now)
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return flushWindow(time.Now().UTC())
			}

			if err := assembler.Add(msg, r.cfg.Output.IncludeKey, r.cfg.Output.IncludeHeaders); err != nil {
				return err
			}
			if assembler.ShouldFlush(time.Now().UTC()) {
				if err := flushWindow(time.Now().UTC()); err != nil {
					return err
				}
			}
		}
	}
}

func (r *Runner) runEncodeWorker(
	ctx context.Context,
	flushTasks <-chan flushTask,
	encodedTasks chan<- encodedTask,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok := <-flushTasks:
			if !ok {
				return nil
			}

			file, size, err := fileformat.WriteRecordsToTempFile(task.window.Records, r.cfg.Output)
			if err != nil {
				return fmt.Errorf("encode window partition=%d sequence=%d: %w", task.partition, task.sequence, err)
			}

			encoded := encodedTask{
				flushTask: task,
				file:      file,
				size:      size,
			}
			if err := sendWithContext(ctx, encodedTasks, encoded); err != nil {
				_ = cleanupEncodedTask(encoded)
				return err
			}
		}
	}
}

func (r *Runner) runUploadWorker(
	ctx context.Context,
	encodedTasks <-chan encodedTask,
	uploadedTasks chan<- uploadedTask,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok := <-encodedTasks:
			if !ok {
				return nil
			}

			filePath, err := r.sink.UploadWindowFile(ctx, task.window, task.file, task.size)
			cleanupErr := cleanupEncodedTask(task)
			if err != nil {
				return fmt.Errorf("upload window partition=%d sequence=%d: %w", task.partition, task.sequence, err)
			}
			if cleanupErr != nil {
				return fmt.Errorf("cleanup encoded window partition=%d sequence=%d: %w", task.partition, task.sequence, cleanupErr)
			}

			uploaded := uploadedTask{
				flushTask: task.flushTask,
				filePath:  filePath,
			}
			if err := sendWithContext(ctx, uploadedTasks, uploaded); err != nil {
				return err
			}
		}
	}
}

func (r *Runner) runCommitCoordinator(ctx context.Context, uploadedTasks <-chan uploadedTask) error {
	states := make(map[int32]*partitionCommitState)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok := <-uploadedTasks:
			if !ok {
				return nil
			}

			state := states[task.partition]
			if state == nil {
				state = &partitionCommitState{
					pending: make(map[int64]uploadedTask),
				}
				states[task.partition] = state
			}

			state.pending[task.sequence] = task
			for {
				nextTask, exists := state.pending[state.nextSequence]
				if !exists {
					break
				}

				if err := r.source.Commit(ctx, nextTask.window); err != nil {
					return fmt.Errorf("commit offsets partition=%d sequence=%d: %w", nextTask.partition, nextTask.sequence, err)
				}

				r.logger.Info("batch flushed",
					zap.String("pipeline_id", r.cfg.PipelineID),
					zap.String("run_id", nextTask.window.RunID),
					zap.String("file_path", nextTask.filePath),
					zap.Int32("partition", nextTask.partition),
					zap.Int64("sequence", nextTask.sequence),
					zap.Int("records", len(nextTask.window.Records)),
					zap.Int("bytes_approx", nextTask.window.BytesApprox),
					zap.Time("started_at", nextTask.window.StartedAt),
					zap.Time("ended_at", nextTask.window.EndedAt),
				)

				delete(state.pending, state.nextSequence)
				state.nextSequence++
			}
		}
	}
}

func cleanupEncodedTask(task encodedTask) error {
	var err error
	if task.file == nil {
		return nil
	}
	if closeErr := task.file.Close(); closeErr != nil {
		err = errors.Join(err, closeErr)
	}
	if removeErr := os.Remove(task.file.Name()); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		err = errors.Join(err, removeErr)
	}
	return err
}

func sendWithContext[T any](ctx context.Context, target chan<- T, value T) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				err = ctxErr
				return
			}
			err = fmt.Errorf("send on closed channel: %v", recovered)
		}
	}()

	select {
	case target <- value:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
