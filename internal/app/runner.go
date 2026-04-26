package app

import (
	"context"
	"errors"
	"fmt"
	"os"
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

type uploadTask struct {
	window         model.BatchWindow
	partition      int32
	sequence       int64
	file           *os.File
	size           int64
	sealedAt       time.Time
	releaseFlush   func()
	assemblyMicros int64
}

type uploadedTask struct {
	window         model.BatchWindow
	partition      int32
	sequence       int64
	filePath       string
	sealedAt       time.Time
	uploadedAt     time.Time
	uploadDuration time.Duration
	releaseFlush   func()
	assemblyMicros int64
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

	runStartedAt := time.Now().UTC()
	recorder := newRuntimeStatsRecorder(runStartedAt)
	runID := runStartedAt.Format("20060102T150405.000000000Z")
	defer recorder.Log(r.logger, r.cfg.PipelineID, runID)

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
		uploadsWG    sync.WaitGroup
		commitWG     sync.WaitGroup
		errOnce      sync.Once
	)

	errCh := make(chan error, 1)
	uploadTasks := make(chan uploadTask, r.cfg.Runtime.FlushQueueSize)
	uploadedTasks := make(chan uploadedTask, r.cfg.Runtime.FlushQueueSize)
	flushLimiter := make(chan struct{}, r.cfg.Runtime.MaxParallelFlushes)
	encodeLimiter := make(chan struct{}, r.cfg.Runtime.MaxParallelEncodes)

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

	for i := 0; i < r.cfg.Runtime.MaxParallelUploads; i++ {
		uploadsWG.Add(1)
		go func() {
			defer uploadsWG.Done()
			if err := r.runUploadWorker(runCtx, uploadTasks, uploadedTasks); err != nil && !errors.Is(err, context.Canceled) {
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
			if err := r.runPartitionWorker(runCtx, runID, partition, messages, uploadTasks, flushLimiter, encodeLimiter); err != nil && !errors.Is(err, context.Canceled) {
				reportErr(err)
			}
		}()

		return messages
	}

	hasLocalWork := func() bool {
		partitionsMu.Lock()
		defer partitionsMu.Unlock()

		for _, messages := range partitions {
			if len(messages) > 0 {
				return true
			}
		}

		return len(uploadTasks) > 0 || len(uploadedTasks) > 0 || len(flushLimiter) > 0
	}

	var runErr error
	seenMessages := false
	consecutiveIdlePolls := 0

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
				if !seenMessages {
					break
				}
				if hasLocalWork() {
					consecutiveIdlePolls = 0
					continue
				}
				consecutiveIdlePolls++
				if consecutiveIdlePolls >= 3 {
					break
				}
				continue
			}
			if runCtx.Err() != nil {
				break
			}
			runErr = fmt.Errorf("poll kafka: %w", err)
			cancel()
			break
		}

		if len(messages) == 0 {
			if !seenMessages {
				break
			}
			if hasLocalWork() {
				consecutiveIdlePolls = 0
				continue
			}
			consecutiveIdlePolls++
			if consecutiveIdlePolls >= 3 {
				break
			}
			continue
		}

		seenMessages = true
		consecutiveIdlePolls = 0

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
	close(uploadTasks)
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
	uploadTasks chan<- uploadTask,
	flushLimiter chan struct{},
	encodeLimiter chan struct{},
) error {
	assembler := batch.NewAssembler(r.cfg.Batch, r.cfg.Output, r.cfg.Runtime.TempDir, runID, time.Now().UTC())
	defer func() { _ = assembler.Abort() }()

	var sequence int64

	flushWindow := func(now time.Time) error {
		prepared, err := assembler.Window(now)
		if err != nil {
			return err
		}
		if prepared.Window.RecordCount == 0 {
			return nil
		}

		releaseFlush, err := acquireLimiter(ctx, flushLimiter)
		if err != nil {
			_ = cleanupTempFile(prepared.File)
			return err
		}

		task := uploadTask{
			window:         prepared.Window,
			partition:      partition,
			sequence:       sequence,
			file:           prepared.File,
			size:           prepared.Size,
			sealedAt:       now,
			releaseFlush:   releaseFlush,
			assemblyMicros: prepared.Window.EndedAt.Sub(prepared.Window.StartedAt).Microseconds(),
		}
		if err := sendWithContext(ctx, uploadTasks, task); err != nil {
			task.releaseFlush()
			_ = cleanupTempFile(task.file)
			return err
		}

		sequence++
		assembler = batch.NewAssembler(r.cfg.Batch, r.cfg.Output, r.cfg.Runtime.TempDir, runID, now)
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

			if err := withLimiter(ctx, encodeLimiter, func() error {
				return assembler.Add(msg)
			}); err != nil {
				return err
			}
			if assembler.ShouldFlush(msg.IngestionTime) {
				if err := flushWindow(msg.IngestionTime); err != nil {
					return err
				}
			}
		}
	}
}

func (r *Runner) runUploadWorker(
	ctx context.Context,
	uploadTasks <-chan uploadTask,
	uploadedTasks chan<- uploadedTask,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok := <-uploadTasks:
			if !ok {
				return nil
			}

			startedAt := time.Now()
			filePath, err := r.sink.UploadWindowFile(ctx, task.window, task.file, task.size)
			cleanupErr := cleanupTempFile(task.file)
			if err != nil {
				task.releaseFlush()
				return fmt.Errorf("upload window partition=%d sequence=%d: %w", task.partition, task.sequence, err)
			}
			if cleanupErr != nil {
				task.releaseFlush()
				return fmt.Errorf("cleanup temp file partition=%d sequence=%d: %w", task.partition, task.sequence, cleanupErr)
			}

			uploaded := uploadedTask{
				window:         task.window,
				partition:      task.partition,
				sequence:       task.sequence,
				filePath:       filePath,
				sealedAt:       task.sealedAt,
				uploadedAt:     time.Now(),
				uploadDuration: time.Since(startedAt),
				releaseFlush:   task.releaseFlush,
				assemblyMicros: task.assemblyMicros,
			}
			if err := sendWithContext(ctx, uploadedTasks, uploaded); err != nil {
				uploaded.releaseFlush()
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

			readyTasks := make([]uploadedTask, 0, 2)
			nextSequence := state.nextSequence
			for {
				nextTask, exists := state.pending[nextSequence]
				if !exists {
					break
				}
				readyTasks = append(readyTasks, nextTask)
				delete(state.pending, nextSequence)
				nextSequence++
			}
			if len(readyTasks) == 0 {
				continue
			}

			finalTask := readyTasks[len(readyTasks)-1]
			if err := r.source.Commit(ctx, finalTask.window); err != nil {
				for _, readyTask := range readyTasks {
					readyTask.releaseFlush()
				}
				return fmt.Errorf("commit offsets partition=%d sequence=%d: %w", finalTask.partition, finalTask.sequence, err)
			}

			state.nextSequence = nextSequence
			committedAt := time.Now()
			for _, readyTask := range readyTasks {
				readyTask.releaseFlush()
				logBatchFlushed(r.logger, r.cfg.PipelineID, readyTask, committedAt)
			}
		}
	}
}

func logBatchFlushed(logger *zap.Logger, pipelineID string, task uploadedTask, committedAt time.Time) {
	var recordsPerSecond float64
	if task.assemblyMicros > 0 {
		recordsPerSecond = float64(task.window.RecordCount) / (float64(task.assemblyMicros) / float64(time.Second/time.Microsecond))
	}

	var bytesPerSecond float64
	if task.uploadDuration > 0 {
		bytesPerSecond = float64(task.window.BytesApprox) / task.uploadDuration.Seconds()
	}

	logger.Info("batch flushed",
		zap.String("pipeline_id", pipelineID),
		zap.String("run_id", task.window.RunID),
		zap.String("topic", task.window.Topic),
		zap.String("file_path", task.filePath),
		zap.Int32("partition", task.partition),
		zap.Int64("sequence", task.sequence),
		zap.Int("records", task.window.RecordCount),
		zap.Int("bytes_approx", task.window.BytesApprox),
		zap.Float64("records_per_sec", recordsPerSecond),
		zap.Float64("bytes_per_sec", bytesPerSecond),
		zap.Duration("assembly_duration", time.Duration(task.assemblyMicros)*time.Microsecond),
		zap.Duration("upload_duration", task.uploadDuration),
		zap.Duration("time_to_commit", committedAt.Sub(task.sealedAt)),
		zap.Time("started_at", task.window.StartedAt),
		zap.Time("ended_at", task.window.EndedAt),
	)
}

func cleanupTempFile(file *os.File) error {
	if file == nil {
		return nil
	}

	var err error
	if closeErr := file.Close(); closeErr != nil {
		err = errors.Join(err, closeErr)
	}
	if removeErr := os.Remove(file.Name()); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		err = errors.Join(err, removeErr)
	}
	return err
}

func acquireLimiter(ctx context.Context, limiter chan struct{}) (func(), error) {
	select {
	case limiter <- struct{}{}:
		return func() { <-limiter }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func withLimiter(ctx context.Context, limiter chan struct{}, fn func() error) error {
	release, err := acquireLimiter(ctx, limiter)
	if err != nil {
		return err
	}
	defer release()
	return fn()
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
