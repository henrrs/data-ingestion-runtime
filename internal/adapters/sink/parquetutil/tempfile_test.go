package parquetutil

import (
	"io"
	"os"
	"testing"
	"time"

	"landing-connector/internal/model"
)

func TestWriteRecordsToTempFileReturnsRewoundFile(t *testing.T) {
	t.Parallel()

	file, size, err := WriteRecordsToTempFile([]model.LandingRecord{
		{
			IngestionTime: time.Date(2026, time.April, 20, 1, 18, 1, 0, time.UTC),
			RunID:         "run-1",
			Topic:         "orders",
			Partition:     0,
			Offset:        1,
			PayloadRaw:    []byte(`{"id":1}`),
		},
	}, "zstd")
	if err != nil {
		t.Fatalf("write temp parquet file: %v", err)
	}
	defer func() {
		_ = file.Close()
		_ = os.Remove(file.Name())
	}()

	if size <= 0 {
		t.Fatalf("expected positive parquet size, got %d", size)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("read rewound temp parquet file: %v", err)
	}
	if int64(len(content)) != size {
		t.Fatalf("expected read size %d, got %d", size, len(content))
	}
}
