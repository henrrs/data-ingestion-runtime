package kafka

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestExtractSchemaID(t *testing.T) {
	value := []byte{0, 0, 0, 0, 25, 1, 2, 3}
	if got := extractSchemaID(value); got != 25 {
		t.Fatalf("expected schema id 25, got %d", got)
	}
}

func TestExtractSchemaIDWithoutWireFormat(t *testing.T) {
	value := []byte(`{"id":1}`)
	if got := extractSchemaID(value); got != 0 {
		t.Fatalf("expected schema id 0, got %d", got)
	}
}

func TestNewKafkaMessageSkipsHeadersWhenDisabled(t *testing.T) {
	record := &kgo.Record{
		Topic:     "orders",
		Partition: 0,
		Offset:    10,
		Value:     []byte(`{"id":1}`),
		Headers: []kgo.RecordHeader{
			{Key: "source", Value: []byte("test")},
		},
	}

	msg := newKafkaMessage(record, time.Date(2026, time.April, 25, 22, 0, 0, 0, time.UTC), false)
	if msg.HeadersJSON != nil {
		t.Fatalf("expected headers to stay nil when disabled, got %q", string(msg.HeadersJSON))
	}
}

func TestNewKafkaMessageSerializesHeadersWhenEnabled(t *testing.T) {
	record := &kgo.Record{
		Topic:     "orders",
		Partition: 0,
		Offset:    10,
		Value:     []byte(`{"id":1}`),
		Headers: []kgo.RecordHeader{
			{Key: "source", Value: []byte("test")},
		},
	}

	msg := newKafkaMessage(record, time.Date(2026, time.April, 25, 22, 0, 0, 0, time.UTC), true)
	if got := string(msg.HeadersJSON); got != `{"source":"test"}` {
		t.Fatalf("expected serialized headers, got %q", got)
	}
}
