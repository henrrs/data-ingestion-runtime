package batch

import (
	"testing"
	"time"

	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

func TestStringifyPayloadJSON(t *testing.T) {
	payload, err := stringifyPayload([]byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if payload != `{"id":1}` {
		t.Fatalf("unexpected payload: %s", payload)
	}
}

func TestStringifyPayloadBinary(t *testing.T) {
	payload, err := stringifyPayload([]byte{0, 1, 2, 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := `{"base64_payload":"AAECAw=="}`
	if payload != expected {
		t.Fatalf("expected %s, got %s", expected, payload)
	}
}

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
