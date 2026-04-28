package batch

import (
	"testing"
	"time"

	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

func BenchmarkAssemblerAddHeadersOffKeyOff(b *testing.B) {
	benchmarkAssemblerAdd(b, false, false)
}

func BenchmarkAssemblerAddHeadersOffKeyOn(b *testing.B) {
	benchmarkAssemblerAdd(b, false, true)
}

func BenchmarkAssemblerAddHeadersOnKeyOff(b *testing.B) {
	benchmarkAssemblerAdd(b, true, false)
}

func BenchmarkAssemblerAddHeadersOnKeyOn(b *testing.B) {
	benchmarkAssemblerAdd(b, true, true)
}

func benchmarkAssemblerAdd(b *testing.B, includeHeaders bool, includeKey bool) {
	b.ReportAllocs()
	b.SetBytes(1024)

	cfg := config.BatchConfig{
		MaxRecords:  b.N + 1,
		MaxBytes:    b.N * 1024 * 2,
		MaxDuration: time.Hour,
	}
	outputCfg := config.OutputConfig{
		Format:         "avro",
		Compression:    "null",
		UploadMode:     "streaming",
		IncludeHeaders: includeHeaders,
		IncludeKey:     includeKey,
	}

	now := time.Date(2026, time.April, 26, 0, 0, 0, 0, time.UTC)
	assembler := NewAssembler(cfg, outputCfg, "bench-run", now)
	defer func() { _ = assembler.Abort() }()

	msg := model.KafkaMessage{
		Topic:         "orders",
		Partition:     0,
		Offset:        1,
		IngestionTime: now,
		Key:           []byte("order-1"),
		Value:         samplePayload(1024),
	}
	if includeHeaders {
		msg.HeadersJSON = []byte(`{"source":"bench","tenant":"acme"}`)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.Offset = int64(i)
		if err := assembler.Add(msg); err != nil {
			b.Fatal(err)
		}
	}
}

func samplePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	return payload
}
