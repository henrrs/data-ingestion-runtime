package avroutil

import (
	"io"
	"testing"
	"time"

	"landing-connector/internal/model"
)

func BenchmarkStreamWriterSnappy(b *testing.B) {
	benchmarkStreamWriter(b, "snappy")
}

func BenchmarkStreamWriterNull(b *testing.B) {
	benchmarkStreamWriter(b, "null")
}

func benchmarkStreamWriter(b *testing.B, compression string) {
	b.ReportAllocs()

	writer, err := NewStreamWriter(io.Discard, compression)
	if err != nil {
		b.Fatalf("new stream writer: %v", err)
	}

	record := model.LandingRecord{
		IngestionTime: time.Date(2026, time.April, 25, 22, 0, 0, 0, time.UTC),
		RunID:         "run-1",
		Topic:         "orders",
		Partition:     0,
		Offset:        10,
		KeyRaw:        []byte("order-1"),
		PayloadRaw:    benchmarkPayload(4096),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record.Offset = int64(i)
		if err := writer.WriteRecord(record); err != nil {
			b.Fatalf("write record: %v", err)
		}
	}
	b.StopTimer()

	if err := writer.Close(); err != nil {
		b.Fatalf("close writer: %v", err)
	}
}

func benchmarkPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + (i % 26))
	}
	return payload
}
