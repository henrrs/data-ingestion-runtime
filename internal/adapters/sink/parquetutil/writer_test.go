package parquetutil

import (
	"bytes"
	"testing"
	"time"

	"landing-connector/internal/model"
)

func TestWriteRecordsZstdWithOptionalTimestamp(t *testing.T) {
	eventMicros := time.Date(2026, time.April, 20, 1, 18, 0, 0, time.UTC).UnixMicro()
	key := "order-1"
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
			KeyString:     &key,
			HeadersJSON:   &headers,
			SchemaID:      &schemaID,
			PayloadJSON:   `{"id":1}`,
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
