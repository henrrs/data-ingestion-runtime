package batch

import (
	"os"
	"testing"
	"time"

	"github.com/hamba/avro/v2/ocf"

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
	}, "", "run-1", time.Now().UTC())
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
	defer func() {
		_ = prepared.File.Close()
		_ = os.Remove(prepared.File.Name())
	}()

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
	}, "", "run-1", time.Now().UTC())
	defer func() { _ = assembler.Abort() }()

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

	if err := assembler.Add(msg); err != nil {
		t.Fatalf("add message: %v", err)
	}

	key[0] = 'X'
	payload[0] = 'X'

	prepared, err := assembler.Window(time.Now().UTC())
	if err != nil {
		t.Fatalf("prepare window: %v", err)
	}
	defer func() {
		_ = prepared.File.Close()
		_ = os.Remove(prepared.File.Name())
	}()

	decoder, err := ocf.NewDecoder(prepared.File)
	if err != nil {
		t.Fatalf("create avro decoder: %v", err)
	}
	if !decoder.HasNext() {
		t.Fatal("expected avro file to contain one record")
	}

	var got struct {
		KeyRaw     []byte `avro:"key_raw"`
		PayloadRaw []byte `avro:"payload_raw"`
	}
	if err := decoder.Decode(&got); err != nil {
		t.Fatalf("decode avro record: %v", err)
	}

	if string(got.KeyRaw) != "key-1" {
		t.Fatalf("expected encoded key to be stable, got %q", string(got.KeyRaw))
	}
	if string(got.PayloadRaw) != `{"id":1}` {
		t.Fatalf("expected encoded payload to be stable, got %q", string(got.PayloadRaw))
	}
}
