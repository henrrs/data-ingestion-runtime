package fileformat

import (
	"io"
	"os"
	"testing"
	"time"

	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

func TestWriteRecordsToTempFileParquet(t *testing.T) {
	t.Parallel()

	file, size, err := WriteRecordsToTempFile(sampleRecords(), config.OutputConfig{
		Format:      "parquet",
		Compression: "zstd",
	})
	if err != nil {
		t.Fatalf("write parquet temp output file: %v", err)
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
		t.Fatalf("read temp parquet file: %v", err)
	}
	if int64(len(content)) != size {
		t.Fatalf("expected read size %d, got %d", size, len(content))
	}
}

func TestWriteRecordsToTempFileAvro(t *testing.T) {
	t.Parallel()

	file, size, err := WriteRecordsToTempFile(sampleRecords(), config.OutputConfig{
		Format:      "avro",
		Compression: "snappy",
	})
	if err != nil {
		t.Fatalf("write avro temp output file: %v", err)
	}
	defer func() {
		_ = file.Close()
		_ = os.Remove(file.Name())
	}()

	if size <= 0 {
		t.Fatalf("expected positive avro size, got %d", size)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("read temp avro file: %v", err)
	}
	if int64(len(content)) != size {
		t.Fatalf("expected read size %d, got %d", size, len(content))
	}
}

func sampleRecords() []model.LandingRecord {
	return []model.LandingRecord{
		{
			IngestionTime: time.Date(2026, time.April, 21, 1, 18, 1, 0, time.UTC),
			RunID:         "run-1",
			Topic:         "orders",
			Partition:     0,
			Offset:        1,
			PayloadRaw:    []byte(`{"id":1}`),
		},
	}
}
