package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
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

type streamUploadResult struct {
	filePath                 string
	duration                 time.Duration
	activeDuration           time.Duration
	waitForFirstByteDuration time.Duration
	tailFinalizeDuration     time.Duration
	readBytes                int64
	err                      error
}

type activeStream struct {
	writer      fileformat.StreamWriter
	pipeWriter  *io.PipeWriter
	resultCh    <-chan streamUploadResult
	byteCounter *countingWriter
}

type uploadedTask struct {
	window                     model.BatchWindow
	partition                  int32
	sequence                   int64
	filePath                   string
	streamSize                 int64
	sealedAt                   time.Time
	uploadedAt                 time.Time
	uploadDuration             time.Duration
	uploadActiveDuration       time.Duration
	uploadWaitFirstByte        time.Duration
	uploadTailFinalizeDuration time.Duration
	uploadReadBytes            int64
	releaseFlush               func()
	releaseUpload              func()
	encodeDuration             time.Duration
	uploadErr                  error
}

type partitionCommitState struct {
	nextSequence int64
	pending      map[int64]uploadedTask
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
		finalizeWG   sync.WaitGroup
		commitWG     sync.WaitGroup
		errOnce      sync.Once
	)

	errCh := make(chan error, 1)
	uploadedTasks := make(chan uploadedTask, r.cfg.Runtime.FlushQueueSize)
	flushLimiter := make(chan struct{}, r.cfg.Runtime.MaxParallelFlushes)
	encodeLimiter := make(chan struct{}, r.cfg.Runtime.MaxParallelEncodes)
	uploadLimiter := make(chan struct{}, r.cfg.Runtime.MaxParallelUploads)

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
			if err := r.runPartitionWorker(
				runCtx,
				runID,
				partition,
				messages,
				uploadedTasks,
				flushLimiter,
				encodeLimiter,
				uploadLimiter,
				&finalizeWG,
			); err != nil && !errors.Is(err, context.Canceled) {
				reportErr(err)
			}
		}()

		return messages
	}

	var runErr error
	executionMode := r.resolvedExecutionMode()
	basePollLimit := r.initialPollLimit()
	maxPollLimit := clampPollLimit(basePollLimit * 4)
	pollLimit := basePollLimit
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

		pollCtx, pollCancel := context.WithTimeout(runCtx, r.idlePollTimeout())
		messages, err := r.source.Poll(pollCtx, pollLimit)
		pollCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && runCtx.Err() == nil && ctx.Err() == nil {
				consecutiveIdlePolls++
				pollLimit = reducePollLimit(pollLimit, basePollLimit)
				if r.shouldStopOnIdle(executionMode, seenMessages, consecutiveIdlePolls) {
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
			consecutiveIdlePolls++
			pollLimit = reducePollLimit(pollLimit, basePollLimit)
			if r.shouldStopOnIdle(executionMode, seenMessages, consecutiveIdlePolls) {
				break
			}
			continue
		}

		seenMessages = true
		consecutiveIdlePolls = 0
		if len(messages) >= pollLimit && pollLimit < maxPollLimit {
			pollLimit = clampPollLimit(minInt(maxPollLimit, pollLimit*2))
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
	finalizeWG.Wait()
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

func (r *Runner) idlePollTimeout() time.Duration {
	if r.cfg.Runtime.IdlePollTimeout > 0 {
		return r.cfg.Runtime.IdlePollTimeout
	}
	return 2 * time.Second
}

func (r *Runner) idlePollCount() int {
	if r.cfg.Runtime.IdlePollCount > 0 {
		return r.cfg.Runtime.IdlePollCount
	}
	return 15
}

func (r *Runner) drainIdlePollCount() int {
	if r.cfg.Runtime.DrainIdlePollCount > 0 {
		return r.cfg.Runtime.DrainIdlePollCount
	}
	return 2
}

func (r *Runner) resolvedExecutionMode() string {
	mode := r.cfg.Runtime.ExecutionMode
	if mode == "" || mode == "finite" {
		return "finite"
	}
	if mode == "continuous" {
		return "continuous"
	}
	if mode == "auto" {
		if r.cfg.RunTimeout > 0 {
			return "finite"
		}
		return "continuous"
	}
	return "finite"
}

func (r *Runner) shouldStopOnIdle(mode string, seenMessages bool, consecutiveIdlePolls int) bool {
	if mode == "continuous" {
		return false
	}
	if !seenMessages {
		return true
	}
	return consecutiveIdlePolls >= r.drainIdlePollCount()
}

func (r *Runner) initialPollLimit() int {
	if r.cfg.Kafka.PollRecords > 0 {
		return clampPollLimit(r.cfg.Kafka.PollRecords)
	}
	return 1000
}

func reducePollLimit(current, base int) int {
	if current <= base {
		return base
	}
	return maxInt(base, current/2)
}

func clampPollLimit(limit int) int {
	if limit < 1 {
		return 1
	}
	if limit > 32000 {
		return 32000
	}
	return limit
}

func (r *Runner) runPartitionWorker(
	ctx context.Context,
	runID string,
	partition int32,
	messages <-chan model.KafkaMessage,
	uploadedTasks chan<- uploadedTask,
	flushLimiter chan struct{},
	encodeLimiter chan struct{},
	uploadLimiter chan struct{},
	finalizeWG *sync.WaitGroup,
) error {
	assembler := batch.NewAssembler(r.cfg.Batch, r.cfg.Output, runID, time.Now().UTC())
	var stream *activeStream
	var sequence int64

	defer func() {
		abortActiveStream(stream, errors.New("partition worker stopped"))
		_ = assembler.Abort()
	}()

	startStream := func(msg model.KafkaMessage) (*activeStream, error) {
		pipeReader, pipeWriter := io.Pipe()
		counter := &countingWriter{writer: pipeWriter}

		windowForPath := model.BatchWindow{
			RunID:       runID,
			Topic:       msg.Topic,
			StartedAt:   assembler.StartedAt(),
			RecordCount: 1,
			OffsetsByPart: map[int32]model.OffsetRange{
				partition: {
					Partition:   partition,
					StartOffset: msg.Offset,
					EndOffset:   msg.Offset,
					RecordCount: 1,
				},
			},
		}

		resultCh := make(chan streamUploadResult, 1)
		go func() {
			defer close(resultCh)

			startedAt := time.Now()
			readTracker := &uploadReadTracker{reader: pipeReader}
			filePath, uploadErr := r.sink.UploadWindowStream(ctx, windowForPath, readTracker)
			finishedAt := time.Now()
			_ = pipeReader.Close()
			uploadRead := readTracker.Snapshot()
			waitForFirstByte := time.Duration(0)
			activeDuration := time.Duration(0)
			tailFinalizeDuration := time.Duration(0)
			if uploadRead.FirstReadAt.IsZero() {
				waitForFirstByte = finishedAt.Sub(startedAt)
			} else {
				waitForFirstByte = uploadRead.FirstReadAt.Sub(startedAt)
				activeDuration = uploadRead.LastReadAt.Sub(uploadRead.FirstReadAt)
				tailFinalizeDuration = finishedAt.Sub(uploadRead.LastReadAt)
			}
			resultCh <- streamUploadResult{
				filePath:                 filePath,
				duration:                 finishedAt.Sub(startedAt),
				activeDuration:           activeDuration,
				waitForFirstByteDuration: maxDuration(0, waitForFirstByte),
				tailFinalizeDuration:     maxDuration(0, tailFinalizeDuration),
				readBytes:                uploadRead.BytesRead,
				err:                      uploadErr,
			}
		}()

		streamWriter, err := fileformat.NewStreamWriter(counter, r.cfg.Output)
		if err != nil {
			_ = pipeWriter.CloseWithError(err)
			return nil, fmt.Errorf("create stream writer: %w", err)
		}

		return &activeStream{
			writer:      streamWriter,
			pipeWriter:  pipeWriter,
			resultCh:    resultCh,
			byteCounter: counter,
		}, nil
	}

	dispatchUploadResult := func(task uploadedTask, resultCh <-chan streamUploadResult) {
		finalizeWG.Add(1)
		go func() {
			defer finalizeWG.Done()

			result, ok := <-resultCh
			if !ok {
				task.uploadErr = errors.New("upload stream result channel closed unexpectedly")
			} else {
				task.filePath = result.filePath
				task.uploadDuration = result.duration
				task.uploadActiveDuration = result.activeDuration
				task.uploadWaitFirstByte = result.waitForFirstByteDuration
				task.uploadTailFinalizeDuration = result.tailFinalizeDuration
				task.uploadReadBytes = result.readBytes
				task.uploadErr = result.err
			}
			if task.releaseUpload != nil {
				task.releaseUpload()
			}
			task.uploadedAt = time.Now()

			if err := sendWithContext(ctx, uploadedTasks, task); err != nil {
				task.releaseFlush()
			}
		}()
	}

	flushWindow := func(now time.Time) error {
		prepared, err := assembler.Window(now)
		if err != nil {
			return err
		}
		if prepared.Window.RecordCount == 0 {
			return nil
		}
		if stream == nil {
			return errors.New("sealed non-empty window without active stream")
		}

		releaseFlush, err := acquireLimiter(ctx, flushLimiter)
		if err != nil {
			abortActiveStream(stream, err)
			return err
		}
		releaseFlush = singleRelease(releaseFlush)

		releaseUpload, err := acquireLimiter(ctx, uploadLimiter)
		if err != nil {
			releaseFlush()
			abortActiveStream(stream, err)
			return err
		}
		releaseUpload = singleRelease(releaseUpload)

		if err := stream.writer.Close(); err != nil {
			releaseFlush()
			releaseUpload()
			abortActiveStream(stream, err)
			return fmt.Errorf("close stream writer: %w", err)
		}
		if err := stream.pipeWriter.Close(); err != nil {
			releaseFlush()
			releaseUpload()
			abortActiveStream(stream, err)
			return fmt.Errorf("close stream pipe: %w", err)
		}

		task := uploadedTask{
			window:         prepared.Window,
			partition:      partition,
			sequence:       sequence,
			streamSize:     stream.byteCounter.Bytes(),
			sealedAt:       now,
			releaseFlush:   releaseFlush,
			releaseUpload:  releaseUpload,
			encodeDuration: prepared.Window.EndedAt.Sub(prepared.Window.StartedAt),
		}
		dispatchUploadResult(task, stream.resultCh)

		sequence++
		stream = nil
		assembler = batch.NewAssembler(r.cfg.Batch, r.cfg.Output, runID, now)
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

			record := batch.BuildLandingRecord(msg, runID, r.cfg.Output.IncludeKey)

			if stream == nil {
				var err error
				stream, err = startStream(msg)
				if err != nil {
					return err
				}
			}

			releaseEncode, err := acquireLimiter(ctx, encodeLimiter)
			if err != nil {
				abortActiveStream(stream, err)
				return err
			}
			releaseEncode = singleRelease(releaseEncode)

			if err := stream.writer.WriteRecord(record); err != nil {
				releaseEncode()
				abortActiveStream(stream, err)
				return fmt.Errorf("encode stream record: %w", err)
			}
			if err := assembler.Add(msg); err != nil {
				releaseEncode()
				abortActiveStream(stream, err)
				return err
			}
			releaseEncode()

			if assembler.ShouldFlush(msg.IngestionTime) {
				if err := flushWindow(msg.IngestionTime); err != nil {
					return err
				}
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

			if task.uploadErr != nil {
				task.releaseFlush()
				return fmt.Errorf("upload stream partition=%d sequence=%d: %w", task.partition, task.sequence, task.uploadErr)
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
			commitStartedAt := time.Now()
			if err := r.source.Commit(ctx, finalTask.window); err != nil {
				for _, readyTask := range readyTasks {
					readyTask.releaseFlush()
				}
				return fmt.Errorf("commit offsets partition=%d sequence=%d: %w", finalTask.partition, finalTask.sequence, err)
			}

			state.nextSequence = nextSequence
			committedAt := time.Now()
			commitDuration := committedAt.Sub(commitStartedAt)
			for _, readyTask := range readyTasks {
				readyTask.releaseFlush()
				logBatchFlushed(r.logger, r.cfg, readyTask, committedAt, commitDuration)
			}
		}
	}
}

func logBatchFlushed(logger *zap.Logger, cfg config.Config, task uploadedTask, committedAt time.Time, commitDuration time.Duration) {
	var recordsPerSecond float64
	if task.encodeDuration > 0 {
		recordsPerSecond = float64(task.window.RecordCount) / task.encodeDuration.Seconds()
	}

	var bytesPerSecond float64
	if task.uploadActiveDuration > 0 {
		bytesPerSecond = float64(task.uploadReadBytes) / task.uploadActiveDuration.Seconds()
	} else if task.uploadDuration > 0 {
		bytesPerSecond = float64(task.window.BytesApprox) / task.uploadDuration.Seconds()
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	logger.Info("batch flushed",
		zap.String("pipeline_id", cfg.PipelineID),
		zap.String("run_id", task.window.RunID),
		zap.String("topic", task.window.Topic),
		zap.String("format", cfg.Output.Format),
		zap.String("compression", cfg.Output.Compression),
		zap.String("upload_mode", cfg.Output.UploadMode),
		zap.String("file_path", task.filePath),
		zap.Int32("partition", task.partition),
		zap.Int64("sequence", task.sequence),
		zap.Int("records", task.window.RecordCount),
		zap.Int("approx_input_bytes", task.window.BytesApprox),
		zap.Int64("stream_size", task.streamSize),
		zap.Int64("upload_active_bytes", task.uploadReadBytes),
		zap.Float64("records_per_sec", recordsPerSecond),
		zap.Float64("upload_active_bytes_per_sec", bytesPerSecond),
		zap.Duration("encode_duration", task.encodeDuration),
		zap.Duration("upload_duration", task.uploadDuration),
		zap.Duration("upload_stream_open_duration", task.uploadDuration),
		zap.Duration("upload_active_duration", task.uploadActiveDuration),
		zap.Duration("upload_wait_for_first_byte", task.uploadWaitFirstByte),
		zap.Duration("upload_tail_finalize_duration", task.uploadTailFinalizeDuration),
		zap.Duration("commit_duration", commitDuration),
		zap.Duration("total_batch_duration", committedAt.Sub(task.window.StartedAt)),
		zap.Duration("time_to_commit", committedAt.Sub(task.sealedAt)),
		zap.Uint64("heap_alloc_bytes", mem.HeapAlloc),
		zap.Uint64("heap_sys_bytes", mem.HeapSys),
		zap.Uint32("gc_cycles", mem.NumGC),
		zap.Uint64("gc_pause_total_ns", mem.PauseTotalNs),
		zap.Time("started_at", task.window.StartedAt),
		zap.Time("ended_at", task.window.EndedAt),
	)
}

func abortActiveStream(stream *activeStream, reason error) {
	if stream == nil {
		return
	}
	_ = stream.pipeWriter.CloseWithError(reason)
	_ = stream.writer.Close()
}

func acquireLimiter(ctx context.Context, limiter chan struct{}) (func(), error) {
	select {
	case limiter <- struct{}{}:
		return func() { <-limiter }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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

func singleRelease(release func()) func() {
	var once sync.Once
	return func() {
		once.Do(release)
	}
}

type countingWriter struct {
	writer io.Writer
	total  int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.writer.Write(p)
	atomic.AddInt64(&c.total, int64(n))
	return n, err
}

func (c *countingWriter) Bytes() int64 {
	return atomic.LoadInt64(&c.total)
}

type uploadReadTracker struct {
	reader io.Reader

	mu          sync.Mutex
	firstReadAt time.Time
	lastReadAt  time.Time
	bytesRead   int64
}

type uploadReadSnapshot struct {
	FirstReadAt time.Time
	LastReadAt  time.Time
	BytesRead   int64
}

func (t *uploadReadTracker) Read(p []byte) (int, error) {
	n, err := t.reader.Read(p)
	if n > 0 {
		now := time.Now()
		t.mu.Lock()
		if t.firstReadAt.IsZero() {
			t.firstReadAt = now
		}
		t.lastReadAt = now
		t.bytesRead += int64(n)
		t.mu.Unlock()
	}
	return n, err
}

func (t *uploadReadTracker) Snapshot() uploadReadSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return uploadReadSnapshot{
		FirstReadAt: t.firstReadAt,
		LastReadAt:  t.lastReadAt,
		BytesRead:   t.bytesRead,
	}
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
