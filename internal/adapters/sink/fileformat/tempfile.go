package fileformat

import (
	"fmt"
	"io"
	"os"

	"landing-connector/internal/adapters/sink/avroutil"
	"landing-connector/internal/adapters/sink/parquetutil"
	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

func WriteRecordsToTempFile(records []model.LandingRecord, outputCfg config.OutputConfig) (*os.File, int64, error) {
	tempFile, err := os.CreateTemp("", "landing-connector-*."+outputCfg.FileExtension())
	if err != nil {
		return nil, 0, fmt.Errorf("create temp output file: %w", err)
	}

	cleanupOnError := func(cause error) (*os.File, int64, error) {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
		return nil, 0, cause
	}

	switch outputCfg.Format {
	case "parquet":
		if err := parquetutil.WriteRecords(tempFile, records, outputCfg.Compression); err != nil {
			return cleanupOnError(fmt.Errorf("write parquet temp file: %w", err))
		}
	case "avro":
		if err := avroutil.WriteRecords(tempFile, records, outputCfg.Compression); err != nil {
			return cleanupOnError(fmt.Errorf("write avro temp file: %w", err))
		}
	default:
		return cleanupOnError(fmt.Errorf("unsupported output format: %s", outputCfg.Format))
	}

	info, err := tempFile.Stat()
	if err != nil {
		return cleanupOnError(fmt.Errorf("stat temp output file: %w", err))
	}

	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return cleanupOnError(fmt.Errorf("rewind temp output file: %w", err))
	}

	return tempFile, info.Size(), nil
}
