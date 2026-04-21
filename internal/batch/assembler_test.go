package batch

import (
	"bytes"
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
	}, "run-1", time.Now().UTC())

	messages := []model.KafkaMessage{
		{Topic: "orders", Partition: 0, Offset: 100, Value: []byte(`{"id":1}`)},
		{Topic: "orders", Partition: 0, Offset: 101, Value: []byte(`{"id":2}`)},
	}

	for _, msg := range messages {
		if err := assembler.Add(msg, false, false); err != nil {
			t.Fatalf("add message: %v", err)
		}
	}

	window := assembler.Window(time.Now().UTC())
	offsets := window.OffsetsByPart[0]
	if offsets.StartOffset != 100 || offsets.EndOffset != 101 || offsets.RecordCount != 2 {
		t.Fatalf("unexpected offsets: %+v", offsets)
	}
}

func TestAssemblerCopiesPayloadAndKeyBuffers(t *testing.T) {
	assembler := NewAssembler(config.BatchConfig{
		MaxRecords:  10,
		MaxBytes:    1024,
		MaxDuration: time.Minute,
	}, "run-1", time.Now().UTC())

	key := []byte("key-1")
	payload := []byte(`{"id":1}`)
	msg := model.KafkaMessage{
		Topic:     "orders",
		Partition: 0,
		Offset:    100,
		Key:       key,
		Value:     payload,
	}

	if err := assembler.Add(msg, true, false); err != nil {
		t.Fatalf("add message: %v", err)
	}

	key[0] = 'X'
	payload[0] = 'X'

	window := assembler.Window(time.Now().UTC())
	record := window.Records[0]
	if !bytes.Equal(record.KeyRaw, []byte("key-1")) {
		t.Fatalf("expected copied key, got %q", string(record.KeyRaw))
	}
	if !bytes.Equal(record.PayloadRaw, []byte(`{"id":1}`)) {
		t.Fatalf("expected copied payload, got %q", string(record.PayloadRaw))
	}
}
