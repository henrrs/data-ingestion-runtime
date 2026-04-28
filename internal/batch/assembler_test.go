package batch

import (
	"testing"
	"time"

	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

func TestAssemblerTracksOffsets(t *testing.T) {
	assembler := NewAssembler(config.BatchConfig{
		MaxRecords:  10,
		MaxBytes:    1024,
		MaxDuration: time.Minute,
	}, config.OutputConfig{
		Format:      "avro",
		Compression: "snappy",
	}, "run-1", time.Now().UTC())
	defer func() { _ = assembler.Abort() }()

	messages := []model.KafkaMessage{
		{Topic: "orders", Partition: 0, Offset: 100, IngestionTime: time.Now().UTC(), Value: []byte(`{"id":1}`)},
		{Topic: "orders", Partition: 0, Offset: 101, IngestionTime: time.Now().UTC(), Value: []byte(`{"id":2}`)},
	}

	for _, msg := range messages {
		if err := assembler.Add(msg); err != nil {
			t.Fatalf("add message: %v", err)
		}
	}

	prepared, err := assembler.Window(time.Now().UTC())
	if err != nil {
		t.Fatalf("prepare window: %v", err)
	}

	offsets := prepared.Window.OffsetsByPart[0]
	if offsets.StartOffset != 100 || offsets.EndOffset != 101 || offsets.RecordCount != 2 {
		t.Fatalf("unexpected offsets: %+v", offsets)
	}
	if prepared.Window.RecordCount != 2 {
		t.Fatalf("expected record_count=2, got %d", prepared.Window.RecordCount)
	}
	if prepared.Window.Topic != "orders" {
		t.Fatalf("expected topic=orders, got %q", prepared.Window.Topic)
	}
}

func TestAssemblerStreamsRecordDataImmediately(t *testing.T) {
	assembler := NewAssembler(config.BatchConfig{
		MaxRecords:  10,
		MaxBytes:    1024,
		MaxDuration: time.Minute,
	}, config.OutputConfig{
		Format:      "avro",
		Compression: "snappy",
		IncludeKey:  true,
	}, "run-1", time.Now().UTC())

	key := []byte("key-1")
	payload := []byte(`{"id":1}`)
	msg := model.KafkaMessage{
		Topic:         "orders",
		Partition:     0,
		Offset:        100,
		IngestionTime: time.Date(2026, time.April, 25, 22, 0, 0, 0, time.UTC),
		Key:           key,
		Value:         payload,
	}

	record := BuildLandingRecord(msg, "run-1", true)
	if string(record.KeyRaw) != "key-1" {
		t.Fatalf("expected key_raw=key-1, got %q", string(record.KeyRaw))
	}
	if string(record.PayloadRaw) != `{"id":1}` {
		t.Fatalf("expected payload_raw={\"id\":1}, got %q", string(record.PayloadRaw))
	}

	if err := assembler.Add(msg); err != nil {
		t.Fatalf("add message: %v", err)
	}
}
