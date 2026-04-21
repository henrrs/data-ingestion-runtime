package app

import (
	"context"
	"errors"
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
	writes        []model.BatchWindow
	started       chan struct{}
	releaseWrites chan struct{}
}

func (s *stubSink) WriteWindow(_ context.Context, window model.BatchWindow) (string, error) {
	if s.started != nil {
		s.started <- struct{}{}
	}
	if s.releaseWrites != nil {
		<-s.releaseWrites
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes = append(s.writes, window)
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
	runner := &Runner{
		cfg: config.Config{
			PipelineID: "orders-landing-test",
			Kafka:      config.KafkaConfig{Topic: "orders"},
			Batch: config.BatchConfig{
				MaxRecords:  10,
				MaxBytes:    1024,
				MaxDuration: time.Minute,
			},
			Output: config.OutputConfig{
				IncludeHeaders: true,
				IncludeKey:     true,
			},
			Runtime: config.RuntimeConfig{
				MaxParallelFlushes: 1,
				PartitionQueueSize: 1,
			},
		},
		logger: zap.NewNop(),
		source: source,
		sink:   sink,
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	if !source.closed {
		t.Fatal("expected source to be closed")
	}
	if len(sink.writes) != 1 {
		t.Fatalf("expected 1 sink write, got %d", len(sink.writes))
	}
	if len(source.commits) != 1 {
		t.Fatalf("expected 1 commit, got %d", len(source.commits))
	}
	if got := len(sink.writes[0].Records); got != 1 {
		t.Fatalf("expected flushed window to contain 1 record, got %d", got)
	}
}

func TestRunnerStopsCleanlyWhenIdleBeforeReceivingMessages(t *testing.T) {
	source := &stubSource{
		pollErrors: []error{context.DeadlineExceeded},
	}

	sink := &stubSink{}
	runner := &Runner{
		cfg: config.Config{
			PipelineID: "orders-landing-test",
			Kafka:      config.KafkaConfig{Topic: "orders"},
			Batch: config.BatchConfig{
				MaxRecords:  10,
				MaxBytes:    1024,
				MaxDuration: time.Minute,
			},
			Runtime: config.RuntimeConfig{
				MaxParallelFlushes: 1,
				PartitionQueueSize: 1,
			},
		},
		logger: zap.NewNop(),
		source: source,
		sink:   sink,
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	if len(sink.writes) != 0 {
		t.Fatalf("expected no sink writes, got %d", len(sink.writes))
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
	runner := &Runner{
		cfg: config.Config{
			PipelineID: "orders-landing-test",
			Kafka:      config.KafkaConfig{Topic: "orders"},
			Batch: config.BatchConfig{
				MaxRecords:  1,
				MaxBytes:    1024,
				MaxDuration: time.Minute,
			},
			Runtime: config.RuntimeConfig{
				MaxParallelFlushes: 2,
				PartitionQueueSize: 1,
			},
		},
		logger: zap.NewNop(),
		source: source,
		sink:   sink,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(context.Background())
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for concurrent writes to start")
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

	if len(sink.writes) != 2 {
		t.Fatalf("expected 2 sink writes, got %d", len(sink.writes))
	}
	if len(source.commits) != 2 {
		t.Fatalf("expected 2 commits, got %d", len(source.commits))
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
	runner := &Runner{
		cfg: config.Config{
			PipelineID: "orders-landing-test",
			Kafka:      config.KafkaConfig{Topic: "orders"},
			Batch: config.BatchConfig{
				MaxRecords:  1,
				MaxBytes:    1024,
				MaxDuration: time.Minute,
			},
			Runtime: config.RuntimeConfig{
				MaxParallelFlushes: 1,
				PartitionQueueSize: 1,
			},
		},
		logger: zap.NewNop(),
		source: source,
		sink:   sink,
	}

	err := runner.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "sink exploded") {
		t.Fatalf("expected sink error, got %v", err)
	}
}

type failingSink struct {
	err error
}

func (s *failingSink) WriteWindow(_ context.Context, _ model.BatchWindow) (string, error) {
	return "", s.err
}
