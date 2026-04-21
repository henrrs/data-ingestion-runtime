package parquetutil

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"landing-connector/internal/model"
)

func TestWriteRecordsZstdWithOptionalTimestamp(t *testing.T) {
	eventMicros := time.Date(2026, time.April, 20, 1, 18, 0, 0, time.UTC).UnixMicro()
	headers := `{"source":"e2e"}`
	schemaID := int32(25)

	records := []model.LandingRecord{
		{
			IngestionTime: time.Date(2026, time.April, 20, 1, 18, 1, 0, time.UTC),
			RunID:         "run-1",
			Topic:         "orders",
			Partition:     0,
			Offset:        1,
			EventTime:     &eventMicros,
			KeyRaw:        []byte("order-1"),
			HeadersJSON:   &headers,
			SchemaID:      &schemaID,
			PayloadRaw:    []byte(`{"id":1}`),
		},
	}

	var buffer bytes.Buffer
	if err := WriteRecords(&buffer, records, "zstd"); err != nil {
		t.Fatalf("write records: %v", err)
	}
	if buffer.Len() == 0 {
		t.Fatal("expected parquet buffer to be non-empty")
	}
}

func TestWriteRecordsHandlesLargeBatchInChunks(t *testing.T) {
	t.Parallel()

	records := make([]model.LandingRecord, 0, parquetWriteChunkSize+17)
	for i := 0; i < parquetWriteChunkSize+17; i++ {
		records = append(records, model.LandingRecord{
			IngestionTime: time.Date(2026, time.April, 20, 1, 18, 1, 0, time.UTC),
			RunID:         "run-1",
			Topic:         "orders",
			Partition:     0,
			Offset:        int64(i),
			PayloadRaw:    []byte(`{"id":1}`),
		})
	}

	var buffer bytes.Buffer
	if err := WriteRecords(&buffer, records, "zstd"); err != nil {
		t.Fatalf("write large batch: %v", err)
	}
	if buffer.Len() == 0 {
		t.Fatal("expected parquet buffer to be non-empty")
	}
}

func TestWriteRecordsPropagatesWriterError(t *testing.T) {
	t.Parallel()

	writer := &failingWriter{failAfter: 1}
	err := WriteRecords(writer, []model.LandingRecord{
		{
			IngestionTime: time.Date(2026, time.April, 20, 1, 18, 1, 0, time.UTC),
			RunID:         "run-1",
			Topic:         "orders",
			Partition:     0,
			Offset:        1,
			PayloadRaw:    []byte(`{"id":1}`),
		},
	}, "zstd")
	if err == nil {
		t.Fatal("expected writer error")
	}
	if !errors.Is(err, errWriterFailed) {
		t.Fatalf("expected errWriterFailed, got %v", err)
	}
}

var errWriterFailed = errors.New("writer failed")

type failingWriter struct {
	writes    int
	failAfter int
}

func (w *failingWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes >= w.failAfter {
		return 0, errWriterFailed
	}
	return len(p), nil
}
