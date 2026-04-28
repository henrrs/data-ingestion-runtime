package app

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
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
	maxWorkers := r.cfg.Runtime.AutotuneMaxWorkers
	flushLimiter := newAdaptiveLimiter(r.cfg.Runtime.MaxParallelFlushes, maxWorkers)
	encodeLimiter := newAdaptiveLimiter(r.cfg.Runtime.MaxParallelEncodes, maxWorkers)
	uploadLimiter := newAdaptiveLimiter(r.cfg.Runtime.MaxParallelUploads, maxWorkers)
	autotuner := newRuntimeAutoTuner(r.cfg, r.logger, flushLimiter, encodeLimiter, uploadLimiter)
	autotuner.Start(runCtx)

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
		if err := r.runCommitCoordinator(runCtx, uploadedTasks, autotuner); err != nil && !errors.Is(err, context.Canceled) {
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
		autotuner.RecordActivePartitionCount(len(partitions))
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
				autotuner,
				&finalizeWG,
			); err != nil && !errors.Is(err, context.Canceled) {
				reportErr(err)
			}
		}()

		return messages
	}

	var runErr error
	executionMode := r.resolvedExecutionMode()
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

		pollLimit := autotuner.CurrentPollLimit()
		pollCtx, pollCancel := context.WithTimeout(runCtx, r.idlePollTimeout())
		messages, err := r.source.Poll(pollCtx, pollLimit)
		pollCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && runCtx.Err() == nil && ctx.Err() == nil {
				consecutiveIdlePolls++
				autotuner.RecordPoll(0, true)
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
			autotuner.RecordPoll(0, true)
			if r.shouldStopOnIdle(executionMode, seenMessages, consecutiveIdlePolls) {
				break
			}
			continue
		}

		autotuner.RecordPoll(len(messages), false)
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
	flushLimiter *adaptiveLimiter,
	encodeLimiter *adaptiveLimiter,
	uploadLimiter *adaptiveLimiter,
	autotuner *runtimeAutoTuner,
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

		releaseFlush, err := flushLimiter.Acquire(ctx)
		if err != nil {
			abortActiveStream(stream, err)
			return err
		}
		releaseFlush = singleRelease(releaseFlush)

		releaseUpload, err := uploadLimiter.Acquire(ctx)
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

			releaseEncode, err := encodeLimiter.Acquire(ctx)
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
			if autotuner != nil {
				autotuner.RecordEncodedRecord()
			}

			if assembler.ShouldFlush(msg.IngestionTime) {
				if err := flushWindow(msg.IngestionTime); err != nil {
					return err
				}
			}
		}
	}
}

func (r *Runner) runCommitCoordinator(ctx context.Context, uploadedTasks <-chan uploadedTask, autotuner *runtimeAutoTuner) error {
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
				if autotuner != nil {
					autotuner.RecordBatch(readyTask, commitDuration)
				}
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

type adaptiveLimiter struct {
	max atomic.Int64

	target atomic.Int64
	inUse  atomic.Int64
}

func newAdaptiveLimiter(initial, max int) *adaptiveLimiter {
	maxValue := int64(max)
	if maxValue <= 0 {
		maxValue = 1
	}
	target := int64(initial)
	if target <= 0 {
		target = 1
	}
	if target > maxValue {
		target = maxValue
	}

	limiter := &adaptiveLimiter{}
	limiter.max.Store(maxValue)
	limiter.target.Store(target)
	return limiter
}

func (l *adaptiveLimiter) Acquire(ctx context.Context) (func(), error) {
	for {
		target := l.target.Load()
		inUse := l.inUse.Load()
		if inUse < target {
			if l.inUse.CompareAndSwap(inUse, inUse+1) {
				return func() {
					l.inUse.Add(-1)
				}, nil
			}
			continue
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (l *adaptiveLimiter) SetTarget(value int) {
	target := int64(value)
	if target < 1 {
		target = 1
	}
	maxValue := l.max.Load()
	if target > maxValue {
		target = maxValue
	}
	l.target.Store(target)
}

func (l *adaptiveLimiter) SetMax(value int) {
	maxValue := int64(value)
	if maxValue < 1 {
		maxValue = 1
	}
	l.max.Store(maxValue)
	current := l.target.Load()
	if current > maxValue {
		l.target.Store(maxValue)
	}
}

func (l *adaptiveLimiter) Target() int {
	return int(l.target.Load())
}

func (l *adaptiveLimiter) Max() int {
	return int(l.max.Load())
}

type autotuneChange struct {
	dimension string
	previous  int
}

type runtimeAutoTuner struct {
	cfg    config.Config
	logger *zap.Logger

	enabled bool

	pollMin int
	pollMax int

	pollLimit atomic.Int64

	flushLimiter   *adaptiveLimiter
	encodeLimiter  *adaptiveLimiter
	uploadLimiter  *adaptiveLimiter
	exploreWorkers bool
	maxWorkersHard int

	pollCalls   atomic.Int64
	idlePolls   atomic.Int64
	polledMsgs  atomic.Int64
	encodedRecs atomic.Int64
	batchCount  atomic.Int64
	waitNs      atomic.Int64
	activeNs    atomic.Int64
	commitNs    atomic.Int64
	activeParts atomic.Int64

	mu                sync.Mutex
	lastScore         float64
	phase             string
	sourceWindows     int
	workerDimensionIx int
	pendingChange     *autotuneChange
	prevPauseTotalNs  uint64
	heapBudgetBytes   uint64
}

func newRuntimeAutoTuner(
	cfg config.Config,
	logger *zap.Logger,
	flushLimiter *adaptiveLimiter,
	encodeLimiter *adaptiveLimiter,
	uploadLimiter *adaptiveLimiter,
) *runtimeAutoTuner {
	pollMin := clampPollLimit(cfg.Kafka.PollRecords)
	if pollMin <= 0 {
		pollMin = 1000
	}
	pollMax := clampPollLimit(cfg.Runtime.AutotunePollMax)
	if pollMax < pollMin {
		pollMax = pollMin
	}

	mode := strings.ToLower(cfg.Runtime.AutotuneMode)
	enabled := mode != "off"

	t := &runtimeAutoTuner{
		cfg:             cfg,
		logger:          logger,
		enabled:         enabled,
		pollMin:         pollMin,
		pollMax:         pollMax,
		flushLimiter:    flushLimiter,
		encodeLimiter:   encodeLimiter,
		uploadLimiter:   uploadLimiter,
		exploreWorkers:  shouldExploreWorkers(cfg),
		maxWorkersHard:  maxInt(1, cfg.Runtime.AutotuneMaxWorkers),
		phase:           "source",
		heapBudgetBytes: detectHeapBudgetBytes(),
	}
	t.pollLimit.Store(int64(pollMin))
	return t
}

func (t *runtimeAutoTuner) Start(ctx context.Context) {
	if !t.enabled {
		return
	}

	interval := t.cfg.Runtime.AutotuneInterval
	if interval <= 0 {
		interval = 20 * time.Second
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				t.evaluate(interval)
			}
		}
	}()
}

func (t *runtimeAutoTuner) CurrentPollLimit() int {
	limit := int(t.pollLimit.Load())
	if limit <= 0 {
		return t.pollMin
	}
	return clampPollLimit(limit)
}

func (t *runtimeAutoTuner) RecordPoll(messageCount int, idle bool) {
	if !t.enabled {
		return
	}
	t.pollCalls.Add(1)
	if idle {
		t.idlePolls.Add(1)
		return
	}
	t.polledMsgs.Add(int64(messageCount))
}

func (t *runtimeAutoTuner) RecordEncodedRecord() {
	if !t.enabled {
		return
	}
	t.encodedRecs.Add(1)
}

func (t *runtimeAutoTuner) RecordBatch(task uploadedTask, commitDuration time.Duration) {
	if !t.enabled {
		return
	}
	t.batchCount.Add(1)
	t.waitNs.Add(task.uploadWaitFirstByte.Nanoseconds())
	t.activeNs.Add(task.uploadActiveDuration.Nanoseconds())
	t.commitNs.Add(commitDuration.Nanoseconds())
}

func (t *runtimeAutoTuner) RecordActivePartitionCount(count int) {
	if !t.enabled {
		return
	}
	if count < 1 {
		count = 1
	}
	t.activeParts.Store(int64(count))
}

func (t *runtimeAutoTuner) evaluate(interval time.Duration) {
	pollCalls := t.pollCalls.Swap(0)
	idlePolls := t.idlePolls.Swap(0)
	polledMsgs := t.polledMsgs.Swap(0)
	encodedRecs := t.encodedRecs.Swap(0)
	batchCount := t.batchCount.Swap(0)
	waitNs := t.waitNs.Swap(0)
	activeNs := t.activeNs.Swap(0)
	commitNs := t.commitNs.Swap(0)

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	gcPauseDeltaNs := int64(mem.PauseTotalNs - t.prevPauseTotalNs)
	t.prevPauseTotalNs = mem.PauseTotalNs

	seconds := interval.Seconds()
	if seconds <= 0 {
		seconds = 1
	}
	throughput := float64(encodedRecs) / seconds
	idleRatio := float64(idlePolls) / float64(maxInt64(1, pollCalls))
	avgWait := float64(waitNs) / float64(maxInt64(1, batchCount))
	avgActive := float64(activeNs) / float64(maxInt64(1, batchCount))
	avgCommit := float64(commitNs) / float64(maxInt64(1, batchCount))
	activeParts := int(t.activeParts.Load())

	score := throughput
	if t.heapBudgetBytes > 0 && mem.HeapAlloc > t.heapBudgetBytes {
		overflowMiB := float64(mem.HeapAlloc-t.heapBudgetBytes) / (1024.0 * 1024.0)
		score -= overflowMiB * 25
	}
	score -= float64(gcPauseDeltaNs) / float64(time.Millisecond) * 0.8

	t.mu.Lock()
	defer t.mu.Unlock()

	t.applyDynamicWorkerMax(activeParts)

	if t.phase == "source" {
		t.tuneSource(idleRatio, polledMsgs, throughput)
		t.sourceWindows++
		if t.sourceWindows >= 3 {
			t.phase = "workers"
		}
		t.lastScore = score
		return
	}

	if t.pendingChange != nil {
		rollback := score < (t.lastScore * 0.97)
		if t.heapBudgetBytes > 0 && mem.HeapAlloc > t.heapBudgetBytes {
			rollback = true
		}
		if rollback {
			t.setDimensionTarget(t.pendingChange.dimension, t.pendingChange.previous)
			t.logger.Info("autotune rollback",
				zap.String("dimension", t.pendingChange.dimension),
				zap.Int("target", t.pendingChange.previous),
				zap.Float64("score", score),
				zap.Float64("last_score", t.lastScore),
			)
		}
		t.pendingChange = nil
		t.lastScore = score
		return
	}

	if t.heapBudgetBytes > 0 && mem.HeapAlloc > t.heapBudgetBytes {
		if t.reduceWorkers() {
			t.logger.Info("autotune reduce workers due heap pressure",
				zap.Uint64("heap_alloc_bytes", mem.HeapAlloc),
				zap.Uint64("heap_budget_bytes", t.heapBudgetBytes),
			)
		}
		t.lastScore = score
		return
	}

	if !t.exploreWorkers {
		t.lastScore = score
		return
	}

	dimension := []string{"flush", "encode", "upload"}[t.workerDimensionIx%3]
	t.workerDimensionIx++
	current := t.dimensionTarget(dimension)
	maxTarget := t.dimensionMax(dimension)
	if current < maxTarget {
		t.setDimensionTarget(dimension, current+1)
		t.pendingChange = &autotuneChange{
			dimension: dimension,
			previous:  current,
		}
		t.logger.Info("autotune explore worker",
			zap.String("dimension", dimension),
			zap.Int("from", current),
			zap.Int("to", current+1),
			zap.Int("active_partitions", activeParts),
			zap.Float64("throughput_rec_s", throughput),
			zap.Float64("idle_ratio", idleRatio),
			zap.Float64("avg_upload_wait_s", avgWait/float64(time.Second)),
			zap.Float64("avg_upload_active_s", avgActive/float64(time.Second)),
			zap.Float64("avg_commit_s", avgCommit/float64(time.Second)),
		)
	}
	t.lastScore = score
}

func (t *runtimeAutoTuner) tuneSource(idleRatio float64, polledMsgs int64, throughput float64) {
	current := t.CurrentPollLimit()
	target := current

	switch {
	case idleRatio > 0.30 && current < t.pollMax:
		target = minInt(current*2, t.pollMax)
	case idleRatio < 0.03 && current > t.pollMin:
		target = maxInt(current/2, t.pollMin)
	}

	if target == current {
		return
	}
	t.pollLimit.Store(int64(target))
	t.logger.Info("autotune adjust source",
		zap.Int("poll_limit_from", current),
		zap.Int("poll_limit_to", target),
		zap.Float64("idle_ratio", idleRatio),
		zap.Int64("polled_messages", polledMsgs),
		zap.Float64("throughput_rec_s", throughput),
	)
}

func (t *runtimeAutoTuner) dimensionTarget(dimension string) int {
	switch dimension {
	case "flush":
		return t.flushLimiter.Target()
	case "encode":
		return t.encodeLimiter.Target()
	default:
		return t.uploadLimiter.Target()
	}
}

func (t *runtimeAutoTuner) dimensionMax(dimension string) int {
	switch dimension {
	case "flush":
		return t.flushLimiter.Max()
	case "encode":
		return t.encodeLimiter.Max()
	default:
		return t.uploadLimiter.Max()
	}
}

func (t *runtimeAutoTuner) setDimensionTarget(dimension string, target int) {
	switch dimension {
	case "flush":
		t.flushLimiter.SetTarget(target)
	case "encode":
		t.encodeLimiter.SetTarget(target)
	default:
		t.uploadLimiter.SetTarget(target)
	}
}

func (t *runtimeAutoTuner) applyDynamicWorkerMax(activePartitions int) {
	if activePartitions <= 0 {
		return
	}
	dynamicMax := minInt(t.maxWorkersHard, maxInt(1, activePartitions))
	t.flushLimiter.SetMax(dynamicMax)
	t.encodeLimiter.SetMax(dynamicMax)
	t.uploadLimiter.SetMax(dynamicMax)
}

func (t *runtimeAutoTuner) reduceWorkers() bool {
	for _, dimension := range []string{"upload", "encode", "flush"} {
		current := t.dimensionTarget(dimension)
		if current > 1 {
			t.setDimensionTarget(dimension, current-1)
			return true
		}
	}
	return false
}

func detectHeapBudgetBytes() uint64 {
	const (
		miB = 1024 * 1024
		giB = 1024 * miB
	)

	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 900 * miB
	}
	defer file.Close()

	var totalBytes uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		kb, parseErr := strconv.ParseUint(fields[1], 10, 64)
		if parseErr != nil {
			break
		}
		totalBytes = kb * 1024
		break
	}

	if totalBytes == 0 {
		return 900 * miB
	}

	budget := totalBytes / 3
	if budget < 600*miB {
		budget = 600 * miB
	}
	if budget > 3*giB {
		budget = 3 * giB
	}
	return budget
}

func shouldExploreWorkers(cfg config.Config) bool {
	mode := strings.ToLower(cfg.Runtime.ExecutionMode)
	switch mode {
	case "continuous":
		return true
	case "auto":
		return cfg.RunTimeout <= 0
	default:
		return false
	}
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

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
