package app

import (
	"context"
	"errors"
	"os"
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
	releaseWrites chan struct{}
}

func (s *stubSink) UploadWindowFile(_ context.Context, window model.BatchWindow, _ *os.File, _ int64) (string, error) {
	if s.started != nil {
		s.started <- struct{}{}
	}
	if s.releaseWrites != nil {
		<-s.releaseWrites
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.uploads = append(s.uploads, window)
	return "s3://landing/test.parquet", nil
}

func TestRunnerFlushesPartialBatchAndStopsCleanlyOnIdlePollTimeout(t *testing.T) {
	source := &stubSource{
		pollResults: [][]model.KafkaMessage{
			{
				{
					Topic:     "orders",
					Partition: 0,
					Offset:    10,
					EventTime: time.Date(2026, time.April, 20, 1, 30, 0, 0, time.UTC),
					Key:       []byte("order-10"),
					Value:     []byte(`{"id":10}`),
					Headers: map[string]string{
						"source": "test",
					},
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
	if got := len(sink.uploads[0].Records); got != 1 {
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
	releaseWrites := make(chan struct{})
	sink := &stubSink{
		started:       started,
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

func TestRunnerAllowsSamePartitionUploadsInParallel(t *testing.T) {
	source := &stubSource{
		pollResults: [][]model.KafkaMessage{
			{
				{Topic: "orders", Partition: 0, Offset: 10, Value: []byte(`{"id":10}`)},
				{Topic: "orders", Partition: 0, Offset: 11, Value: []byte(`{"id":11}`)},
			},
		},
		pollErrors: []error{nil, context.DeadlineExceeded},
	}

	started := make(chan struct{}, 2)
	releaseWrites := make(chan struct{})
	sink := &stubSink{
		started:       started,
		releaseWrites: releaseWrites,
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
			t.Fatal("timed out waiting for same-partition uploads to start")
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
		t.Fatalf("expected 2 uploads, got %d", len(sink.uploads))
	}
}

type outOfOrderSink struct {
	mu           sync.Mutex
	started      chan int64
	releaseFirst chan struct{}
}

func (s *outOfOrderSink) UploadWindowFile(_ context.Context, window model.BatchWindow, _ *os.File, _ int64) (string, error) {
	offset := window.Records[0].Offset
	if s.started != nil {
		s.started <- offset
	}
	if offset == 10 {
		<-s.releaseFirst
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return "s3://landing/test.parquet", nil
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

	if len(source.commits) != 2 {
		t.Fatalf("expected 2 commits, got %d", len(source.commits))
	}
	if got := source.commits[0].Records[0].Offset; got != 10 {
		t.Fatalf("expected first commit offset 10, got %d", got)
	}
	if got := source.commits[1].Records[0].Offset; got != 11 {
		t.Fatalf("expected second commit offset 11, got %d", got)
	}
}

func TestRunnerPropagatesSinkError(t *testing.T) {
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
}

type failingSink struct {
	err error
}

func (s *failingSink) UploadWindowFile(_ context.Context, _ model.BatchWindow, _ *os.File, _ int64) (string, error) {
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
			Format:      "parquet",
			Compression: "snappy",
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
	UploadWindowFile(ctx context.Context, window model.BatchWindow, file *os.File, size int64) (string, error)
}
