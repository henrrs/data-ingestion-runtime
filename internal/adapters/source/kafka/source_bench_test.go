package kafka

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func BenchmarkNewKafkaMessageHeadersOff(b *testing.B) {
	b.ReportAllocs()

	record := &kgo.Record{
		Topic:     "orders",
		Partition: 0,
		Offset:    10,
		Timestamp: time.Date(2026, time.April, 25, 22, 0, 0, 0, time.UTC),
		Key:       []byte("order-1"),
		Value:     samplePayload(4096),
		Headers: []kgo.RecordHeader{
			{Key: "source", Value: []byte("bench")},
			{Key: "tenant", Value: []byte("acme")},
		},
	}
	ingestionTime := time.Date(2026, time.April, 25, 22, 0, 0, 0, time.UTC)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = newKafkaMessage(record, ingestionTime, false)
	}
}

func BenchmarkNewKafkaMessageHeadersOn(b *testing.B) {
	b.ReportAllocs()

	record := &kgo.Record{
		Topic:     "orders",
		Partition: 0,
		Offset:    10,
		Timestamp: time.Date(2026, time.April, 25, 22, 0, 0, 0, time.UTC),
		Key:       []byte("order-1"),
		Value:     samplePayload(4096),
		Headers: []kgo.RecordHeader{
			{Key: "source", Value: []byte("bench")},
			{Key: "tenant", Value: []byte("acme")},
		},
	}
	ingestionTime := time.Date(2026, time.April, 25, 22, 0, 0, 0, time.UTC)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = newKafkaMessage(record, ingestionTime, true)
	}
}

func samplePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + (i % 26))
	}
	return payload
}
