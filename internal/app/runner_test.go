package app

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

type stubSource struct {
	mu          sync.Mutex
	pollResults [][]model.KafkaMessage
	pollErrors  []error
	pollCalls   int
	commits     []model.BatchWindow
	closed      bool
}

func (s *stubSource) Poll(_ context.Context, _ int) ([]model.KafkaMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	index := s.pollCalls
	s.pollCalls++

	var messages []model.KafkaMessage
	if index < len(s.pollResults) {
		messages = s.pollResults[index]
	}

	var err error
	if index < len(s.pollErrors) {
		err = s.pollErrors[index]
	}

	return messages, err
}

func (s *stubSource) Commit(_ context.Context, window model.BatchWindow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commits = append(s.commits, window)
	return nil
}

func (s *stubSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

type stubSink struct {
	mu            sync.Mutex
	uploads       []model.BatchWindow
	started       chan struct{}
	copied        chan struct{}
	releaseWrites chan struct{}
}

func (s *stubSink) UploadWindowStream(_ context.Context, window model.BatchWindow, reader io.Reader) (string, error) {
	if s.started != nil {
		s.started <- struct{}{}
	}
	_, _ = io.Copy(io.Discard, reader)
	if s.copied != nil {
		s.copied <- struct{}{}
	}
	if s.releaseWrites != nil {
		<-s.releaseWrites
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.uploads = append(s.uploads, window)
	return "s3://landing/test.avro", nil
}

func TestRunnerFlushesPartialBatchAndStopsCleanlyOnIdlePollTimeout(t *testing.T) {
	source := &stubSource{
		pollResults: [][]model.KafkaMessage{
			{
				{
					Topic:         "orders",
					Partition:     0,
					Offset:        10,
					EventTime:     time.Date(2026, time.April, 20, 1, 30, 0, 0, time.UTC),
					IngestionTime: time.Date(2026, time.April, 20, 1, 30, 1, 0, time.UTC),
					Key:           []byte("order-10"),
					Value:         []byte(`{"id":10}`),
					HeadersJSON:   []byte(`{"source":"test"}`),
				},
			},
		},
		pollErrors: []error{nil, context.DeadlineExceeded},
	}

	sink := &stubSink{}
	runner := newTestRunner(source, sink, func(cfg *config.Config) {
		cfg.Output.IncludeHeaders = true
		cfg.Output.IncludeKey = true
	})

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	if !source.closed {
		t.Fatal("expected source to be closed")
	}
	if len(sink.uploads) != 1 {
		t.Fatalf("expected 1 sink upload, got %d", len(sink.uploads))
	}
	if len(source.commits) != 1 {
		t.Fatalf("expected 1 commit, got %d", len(source.commits))
	}
	if got := sink.uploads[0].RecordCount; got != 1 {
		t.Fatalf("expected flushed window to contain 1 record, got %d", got)
	}
}

func TestRunnerStopsCleanlyWhenIdleBeforeReceivingMessages(t *testing.T) {
	source := &stubSource{
		pollErrors: []error{context.DeadlineExceeded},
	}

	sink := &stubSink{}
	runner := newTestRunner(source, sink, nil)

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	if len(sink.uploads) != 0 {
		t.Fatalf("expected no sink uploads, got %d", len(sink.uploads))
	}
	if len(source.commits) != 0 {
		t.Fatalf("expected no commits, got %d", len(source.commits))
	}
}

func TestRunnerDoesNotStopOnFirstIdleTimeoutAfterSeeingMessages(t *testing.T) {
	source := &stubSource{
		pollResults: [][]model.KafkaMessage{
			{
				{Topic: "orders", Partition: 0, Offset: 10, Value: []byte(`{"id":10}`)},
			},
			nil,
			{
				{Topic: "orders", Partition: 0, Offset: 11, Value: []byte(`{"id":11}`)},
			},
		},
		pollErrors: []error{
			nil,
			context.DeadlineExceeded,
			nil,
			context.DeadlineExceeded,
			context.DeadlineExceeded,
			context.DeadlineExceeded,
		},
	}

	sink := &stubSink{}
	runner := newTestRunner(source, sink, func(cfg *config.Config) {
		cfg.Batch.MaxRecords = 1
	})

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	if got := len(sink.uploads); got != 2 {
		t.Fatalf("expected 2 uploads after intermittent idle timeout, got %d", got)
	}
	if got := len(source.commits); got != 2 {
		t.Fatalf("expected 2 commits after intermittent idle timeout, got %d", got)
	}
}

func TestRunnerFiniteModeUsesDrainIdlePollCountAfterSeeingMessages(t *testing.T) {
	source := &stubSource{
		pollResults: [][]model.KafkaMessage{
			{
				{Topic: "orders", Partition: 0, Offset: 10, Value: []byte(`{"id":10}`)},
			},
		},
		pollErrors: []error{
			nil,
			context.DeadlineExceeded,
			context.DeadlineExceeded,
			context.DeadlineExceeded,
		},
	}

	sink := &stubSink{}
	runner := newTestRunner(source, sink, func(cfg *config.Config) {
		cfg.Runtime.ExecutionMode = "finite"
		cfg.Runtime.DrainIdlePollCount = 2
	})

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	if got := source.pollCalls; got != 3 {
		t.Fatalf("expected runner to stop after 2 drain idle polls, got poll calls %d", got)
	}
	if got := len(source.commits); got != 1 {
		t.Fatalf("expected 1 commit, got %d", got)
	}
}

func TestRunnerAutoModeWithoutRunTimeoutBehavesAsContinuous(t *testing.T) {
	source := &stubSource{
		pollErrors: []error{
			context.DeadlineExceeded,
			context.DeadlineExceeded,
			context.DeadlineExceeded,
			context.DeadlineExceeded,
		},
	}

	sink := &stubSink{}
	runner := newTestRunner(source, sink, func(cfg *config.Config) {
		cfg.Runtime.ExecutionMode = "auto"
		cfg.Runtime.DrainIdlePollCount = 1
		cfg.Runtime.IdlePollTimeout = 10 * time.Millisecond
	})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	err := runner.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded in continuous mode, got %v", err)
	}

	if source.pollCalls < 2 {
		t.Fatalf("expected multiple polls before context timeout, got %d", source.pollCalls)
	}
}

func TestRunnerFlushesDifferentPartitionsConcurrently(t *testing.T) {
	source := &stubSource{
		pollResults: [][]model.KafkaMessage{
			{
				{Topic: "orders", Partition: 0, Offset: 10, Value: []byte(`{"id":10}`)},
				{Topic: "orders", Partition: 1, Offset: 20, Value: []byte(`{"id":20}`)},
			},
		},
		pollErrors: []error{nil, context.DeadlineExceeded},
	}

	started := make(chan struct{}, 2)
	copied := make(chan struct{}, 2)
	releaseWrites := make(chan struct{})
	sink := &stubSink{
		started:       started,
		copied:        copied,
		releaseWrites: releaseWrites,
	}
	runner := newTestRunner(source, sink, func(cfg *config.Config) {
		cfg.Batch.MaxRecords = 1
		cfg.Runtime.MaxParallelFlushes = 2
		cfg.Runtime.MaxParallelEncodes = 2
		cfg.Runtime.MaxParallelUploads = 2
		cfg.Runtime.FlushQueueSize = 2
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(context.Background())
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for concurrent uploads to start")
		}
	}

	close(releaseWrites)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runner to finish")
	}

	if len(sink.uploads) != 2 {
		t.Fatalf("expected 2 sink uploads, got %d", len(sink.uploads))
	}
	if len(source.commits) != 2 {
		t.Fatalf("expected 2 commits, got %d", len(source.commits))
	}
}

func TestRunnerAppliesMaxParallelFlushes(t *testing.T) {
	source := &stubSource{
		pollResults: [][]model.KafkaMessage{
			{
				{Topic: "orders", Partition: 0, Offset: 10, Value: []byte(`{"id":10}`)},
				{Topic: "orders", Partition: 1, Offset: 20, Value: []byte(`{"id":20}`)},
			},
		},
		pollErrors: []error{nil, context.DeadlineExceeded},
	}

	started := make(chan struct{}, 2)
	copied := make(chan struct{}, 2)
	releaseWrites := make(chan struct{})
	sink := &stubSink{
		started:       started,
		copied:        copied,
		releaseWrites: releaseWrites,
	}
	runner := newTestRunner(source, sink, func(cfg *config.Config) {
		cfg.Batch.MaxRecords = 1
		cfg.Runtime.MaxParallelFlushes = 1
		cfg.Runtime.MaxParallelEncodes = 2
		cfg.Runtime.MaxParallelUploads = 2
		cfg.Runtime.FlushQueueSize = 2
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(context.Background())
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first upload to start")
	}

	select {
	case <-started:
	case <-time.After(150 * time.Millisecond):
		t.Fatal("timed out waiting for second upload to start")
	}

	select {
	case <-copied:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first upload stream to drain")
	}

	select {
	case <-copied:
		t.Fatal("expected second upload stream to remain blocked by max_parallel_flushes")
	case <-time.After(150 * time.Millisecond):
	}

	close(releaseWrites)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runner to finish")
	}
}

func TestRunnerAppliesMaxParallelUploads(t *testing.T) {
	source := &stubSource{
		pollResults: [][]model.KafkaMessage{
			{
				{Topic: "orders", Partition: 0, Offset: 10, Value: []byte(`{"id":10}`)},
				{Topic: "orders", Partition: 1, Offset: 20, Value: []byte(`{"id":20}`)},
			},
		},
		pollErrors: []error{nil, context.DeadlineExceeded},
	}

	started := make(chan struct{}, 2)
	copied := make(chan struct{}, 2)
	releaseWrites := make(chan struct{})
	sink := &stubSink{
		started:       started,
		copied:        copied,
		releaseWrites: releaseWrites,
	}

	runner := newTestRunner(source, sink, func(cfg *config.Config) {
		cfg.Batch.MaxRecords = 1
		cfg.Runtime.MaxParallelFlushes = 2
		cfg.Runtime.MaxParallelEncodes = 2
		cfg.Runtime.MaxParallelUploads = 1
		cfg.Runtime.FlushQueueSize = 2
		cfg.Runtime.PartitionQueueSize = 2
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(context.Background())
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first upload to start")
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second upload to start")
	}

	select {
	case <-copied:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first upload stream to drain")
	}

	select {
	case <-copied:
		t.Fatal("expected second upload stream to remain blocked by max_parallel_uploads")
	case <-time.After(200 * time.Millisecond):
	}

	close(releaseWrites)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for runner to finish")
	}

	if got := len(source.commits); got != 2 {
		t.Fatalf("expected 2 commits, got %d", got)
	}
}

type outOfOrderSink struct {
	mu           sync.Mutex
	started      chan int64
	releaseFirst chan struct{}
}

func (s *outOfOrderSink) UploadWindowStream(_ context.Context, window model.BatchWindow, reader io.Reader) (string, error) {
	offset := window.OffsetsByPart[0].StartOffset
	if s.started != nil {
		s.started <- offset
	}
	_, _ = io.Copy(io.Discard, reader)
	if offset == 10 {
		<-s.releaseFirst
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return "s3://landing/test.avro", nil
}

func TestRunnerCommitsSamePartitionInOrderWhenUploadsFinishOutOfOrder(t *testing.T) {
	source := &stubSource{
		pollResults: [][]model.KafkaMessage{
			{
				{Topic: "orders", Partition: 0, Offset: 10, Value: []byte(`{"id":10}`)},
				{Topic: "orders", Partition: 0, Offset: 11, Value: []byte(`{"id":11}`)},
			},
		},
		pollErrors: []error{nil, context.DeadlineExceeded},
	}

	started := make(chan int64, 2)
	releaseFirst := make(chan struct{})
	sink := &outOfOrderSink{
		started:      started,
		releaseFirst: releaseFirst,
	}
	runner := newTestRunner(source, sink, func(cfg *config.Config) {
		cfg.Batch.MaxRecords = 1
		cfg.Runtime.MaxParallelFlushes = 2
		cfg.Runtime.MaxParallelEncodes = 2
		cfg.Runtime.MaxParallelUploads = 2
		cfg.Runtime.FlushQueueSize = 2
		cfg.Runtime.PartitionQueueSize = 2
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(context.Background())
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for uploads to start")
		}
	}

	time.Sleep(150 * time.Millisecond)
	if got := len(source.commits); got != 0 {
		t.Fatalf("expected no commits before first upload completes, got %d", got)
	}

	close(releaseFirst)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runner to finish")
	}

	if len(source.commits) != 1 {
		t.Fatalf("expected coalesced commit count 1, got %d", len(source.commits))
	}
	if got := source.commits[0].OffsetsByPart[0].EndOffset; got != 11 {
		t.Fatalf("expected committed end offset 11, got %d", got)
	}
}

func TestRunnerDoesNotCommitOnUploadFailure(t *testing.T) {
	source := &stubSource{
		pollResults: [][]model.KafkaMessage{
			{
				{Topic: "orders", Partition: 0, Offset: 10, Value: []byte(`{"id":10}`)},
			},
		},
		pollErrors: []error{nil},
	}

	sink := &failingSink{err: errors.New("sink exploded")}
	runner := newTestRunner(source, sink, func(cfg *config.Config) {
		cfg.Batch.MaxRecords = 1
	})

	err := runner.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "sink exploded") {
		t.Fatalf("expected sink error, got %v", err)
	}
	if len(source.commits) != 0 {
		t.Fatalf("expected no commits after upload failure, got %d", len(source.commits))
	}
}

type failingSink struct {
	err error
}

func (s *failingSink) UploadWindowStream(_ context.Context, _ model.BatchWindow, reader io.Reader) (string, error) {
	_, _ = io.Copy(io.Discard, reader)
	return "", s.err
}

func newTestRunner(source coreSource, sink coreSink, mutate func(*config.Config)) *Runner {
	cfg := config.Config{
		PipelineID: "orders-landing-test",
		Kafka:      config.KafkaConfig{Topic: "orders"},
		Batch: config.BatchConfig{
			MaxRecords:  10,
			MaxBytes:    1024,
			MaxDuration: time.Minute,
		},
		Output: config.OutputConfig{
			Format:      "avro",
			Compression: "snappy",
			UploadMode:  "streaming",
		},
		Runtime: config.RuntimeConfig{
			MaxParallelFlushes: 1,
			MaxParallelEncodes: 1,
			MaxParallelUploads: 1,
			FlushQueueSize:     1,
			PartitionQueueSize: 1,
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}

	return &Runner{
		cfg:    cfg,
		logger: zap.NewNop(),
		source: source,
		sink:   sink,
	}
}

type coreSource interface {
	Poll(ctx context.Context, limit int) ([]model.KafkaMessage, error)
	Commit(ctx context.Context, window model.BatchWindow) error
	Close() error
}

type coreSink interface {
	UploadWindowStream(ctx context.Context, window model.BatchWindow, reader io.Reader) (string, error)
}
