package fileformat

import (
	"io"
	"os"
	"testing"
	"time"

	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

func TestWriteRecordsToTempFileAvro(t *testing.T) {
	t.Parallel()

	file, size, err := WriteRecordsToTempFile(sampleRecords(), config.OutputConfig{
		Format:      "avro",
		Compression: "snappy",
	}, "")
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

func TestTempFileWriterAbortRemovesFile(t *testing.T) {
	t.Parallel()

	writer, err := NewTempFileWriter(config.OutputConfig{
		Format:      "avro",
		Compression: "snappy",
	}, "")
	if err != nil {
		t.Fatalf("create temp writer: %v", err)
	}

	record := sampleRecords()[0]
	if err := writer.AppendRecord(record); err != nil {
		t.Fatalf("append record: %v", err)
	}

	tempWriter := writer.(*tempFileWriter)
	fileName := tempWriter.file.Name()
	if err := writer.Abort(); err != nil {
		t.Fatalf("abort writer: %v", err)
	}
	if _, err := os.Stat(fileName); !os.IsNotExist(err) {
		t.Fatalf("expected temp file to be removed, got %v", err)
	}
}
