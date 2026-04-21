package parquetutil

import (
	"fmt"
	"io"
	"os"

	"landing-connector/internal/model"
)

func WriteRecordsToTempFile(records []model.LandingRecord, compression string) (*os.File, int64, error) {
	tempFile, err := os.CreateTemp("", "landing-connector-*.parquet")
	if err != nil {
		return nil, 0, fmt.Errorf("create temp parquet file: %w", err)
	}

	cleanupOnError := func(cause error) (*os.File, int64, error) {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
		return nil, 0, cause
	}

	if err := WriteRecords(tempFile, records, compression); err != nil {
		return cleanupOnError(fmt.Errorf("write parquet temp file: %w", err))
	}

	info, err := tempFile.Stat()
	if err != nil {
		return cleanupOnError(fmt.Errorf("stat parquet temp file: %w", err))
	}

	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return cleanupOnError(fmt.Errorf("rewind parquet temp file: %w", err))
	}

	return tempFile, info.Size(), nil
}
