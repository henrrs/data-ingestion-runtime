package app

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

type stubSource struct {
	pollResults [][]model.KafkaMessage
	pollErrors  []error
	pollCalls   int
	commits     []model.BatchWindow
	closed      bool
}

func (s *stubSource) Poll(_ context.Context, _ int) ([]model.KafkaMessage, error) {
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
	s.commits = append(s.commits, window)
	return nil
}

func (s *stubSource) Close() error {
	s.closed = true
	return nil
}

type stubSink struct {
	writes []model.BatchWindow
}

func (s *stubSink) WriteWindow(_ context.Context, window model.BatchWindow) (string, error) {
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
