package parquetutil

import (
	"fmt"
	"io"
	"time"

	"github.com/parquet-go/parquet-go"

	"landing-connector/internal/model"
)

type landingRecordUncompressed struct {
	IngestionTime time.Time `parquet:"ingestion_time,uncompressed"`
	RunID         string    `parquet:"run_id,uncompressed"`
	Topic         string    `parquet:"topic,uncompressed"`
	Partition     int32     `parquet:"partition,uncompressed"`
	Offset        int64     `parquet:"offset,uncompressed"`
	EventTime     *int64    `parquet:"event_time,optional,uncompressed"`
	KeyRaw        []byte    `parquet:"key_raw,optional,uncompressed"`
	HeadersJSON   *string   `parquet:"headers_json,optional,uncompressed"`
	SchemaID      *int32    `parquet:"schema_id,optional,uncompressed"`
	PayloadRaw    []byte    `parquet:"payload_raw,uncompressed"`
}

type landingRecordSnappy struct {
	IngestionTime time.Time `parquet:"ingestion_time,snappy"`
	RunID         string    `parquet:"run_id,snappy"`
	Topic         string    `parquet:"topic,snappy"`
	Partition     int32     `parquet:"partition,snappy"`
	Offset        int64     `parquet:"offset,snappy"`
	EventTime     *int64    `parquet:"event_time,optional,snappy"`
	KeyRaw        []byte    `parquet:"key_raw,optional,snappy"`
	HeadersJSON   *string   `parquet:"headers_json,optional,snappy"`
	SchemaID      *int32    `parquet:"schema_id,optional,snappy"`
	PayloadRaw    []byte    `parquet:"payload_raw,snappy"`
}

type landingRecordGzip struct {
	IngestionTime time.Time `parquet:"ingestion_time,gzip"`
	RunID         string    `parquet:"run_id,gzip"`
	Topic         string    `parquet:"topic,gzip"`
	Partition     int32     `parquet:"partition,gzip"`
	Offset        int64     `parquet:"offset,gzip"`
	EventTime     *int64    `parquet:"event_time,optional,gzip"`
	KeyRaw        []byte    `parquet:"key_raw,optional,gzip"`
	HeadersJSON   *string   `parquet:"headers_json,optional,gzip"`
	SchemaID      *int32    `parquet:"schema_id,optional,gzip"`
	PayloadRaw    []byte    `parquet:"payload_raw,gzip"`
}

type landingRecordBrotli struct {
	IngestionTime time.Time `parquet:"ingestion_time,brotli"`
	RunID         string    `parquet:"run_id,brotli"`
	Topic         string    `parquet:"topic,brotli"`
	Partition     int32     `parquet:"partition,brotli"`
	Offset        int64     `parquet:"offset,brotli"`
	EventTime     *int64    `parquet:"event_time,optional,brotli"`
	KeyRaw        []byte    `parquet:"key_raw,optional,brotli"`
	HeadersJSON   *string   `parquet:"headers_json,optional,brotli"`
	SchemaID      *int32    `parquet:"schema_id,optional,brotli"`
	PayloadRaw    []byte    `parquet:"payload_raw,brotli"`
}

type landingRecordLz4 struct {
	IngestionTime time.Time `parquet:"ingestion_time,lz4"`
	RunID         string    `parquet:"run_id,lz4"`
	Topic         string    `parquet:"topic,lz4"`
	Partition     int32     `parquet:"partition,lz4"`
	Offset        int64     `parquet:"offset,lz4"`
	EventTime     *int64    `parquet:"event_time,optional,lz4"`
	KeyRaw        []byte    `parquet:"key_raw,optional,lz4"`
	HeadersJSON   *string   `parquet:"headers_json,optional,lz4"`
	SchemaID      *int32    `parquet:"schema_id,optional,lz4"`
	PayloadRaw    []byte    `parquet:"payload_raw,lz4"`
}

type landingRecordZstd struct {
	IngestionTime time.Time `parquet:"ingestion_time,zstd"`
	RunID         string    `parquet:"run_id,zstd"`
	Topic         string    `parquet:"topic,zstd"`
	Partition     int32     `parquet:"partition,zstd"`
	Offset        int64     `parquet:"offset,zstd"`
	EventTime     *int64    `parquet:"event_time,optional,zstd"`
	KeyRaw        []byte    `parquet:"key_raw,optional,zstd"`
	HeadersJSON   *string   `parquet:"headers_json,optional,zstd"`
	SchemaID      *int32    `parquet:"schema_id,optional,zstd"`
	PayloadRaw    []byte    `parquet:"payload_raw,zstd"`
}

const parquetWriteChunkSize = 1024

func WriteRecords(output io.Writer, records []model.LandingRecord, compression string) error {
	switch compression {
	case "uncompressed":
		return write(output, records, func(record model.LandingRecord) landingRecordUncompressed {
			return landingRecordUncompressed{
				IngestionTime: record.IngestionTime,
				RunID:         record.RunID,
				Topic:         record.Topic,
				Partition:     record.Partition,
				Offset:        record.Offset,
				EventTime:     record.EventTime,
				KeyRaw:        record.KeyRaw,
				HeadersJSON:   record.HeadersJSON,
				SchemaID:      record.SchemaID,
				PayloadRaw:    record.PayloadRaw,
			}
		})
	case "snappy":
		return write(output, records, func(record model.LandingRecord) landingRecordSnappy {
			return landingRecordSnappy{
				IngestionTime: record.IngestionTime,
				RunID:         record.RunID,
				Topic:         record.Topic,
				Partition:     record.Partition,
				Offset:        record.Offset,
				EventTime:     record.EventTime,
				KeyRaw:        record.KeyRaw,
				HeadersJSON:   record.HeadersJSON,
				SchemaID:      record.SchemaID,
				PayloadRaw:    record.PayloadRaw,
			}
		})
	case "gzip":
		return write(output, records, func(record model.LandingRecord) landingRecordGzip {
			return landingRecordGzip{
				IngestionTime: record.IngestionTime,
				RunID:         record.RunID,
				Topic:         record.Topic,
				Partition:     record.Partition,
				Offset:        record.Offset,
				EventTime:     record.EventTime,
				KeyRaw:        record.KeyRaw,
				HeadersJSON:   record.HeadersJSON,
				SchemaID:      record.SchemaID,
				PayloadRaw:    record.PayloadRaw,
			}
		})
	case "brotli":
		return write(output, records, func(record model.LandingRecord) landingRecordBrotli {
			return landingRecordBrotli{
				IngestionTime: record.IngestionTime,
				RunID:         record.RunID,
				Topic:         record.Topic,
				Partition:     record.Partition,
				Offset:        record.Offset,
				EventTime:     record.EventTime,
				KeyRaw:        record.KeyRaw,
				HeadersJSON:   record.HeadersJSON,
				SchemaID:      record.SchemaID,
				PayloadRaw:    record.PayloadRaw,
			}
		})
	case "lz4":
		return write(output, records, func(record model.LandingRecord) landingRecordLz4 {
			return landingRecordLz4{
				IngestionTime: record.IngestionTime,
				RunID:         record.RunID,
				Topic:         record.Topic,
				Partition:     record.Partition,
				Offset:        record.Offset,
				EventTime:     record.EventTime,
				KeyRaw:        record.KeyRaw,
				HeadersJSON:   record.HeadersJSON,
				SchemaID:      record.SchemaID,
				PayloadRaw:    record.PayloadRaw,
			}
		})
	case "zstd":
		return write(output, records, func(record model.LandingRecord) landingRecordZstd {
			return landingRecordZstd{
				IngestionTime: record.IngestionTime,
				RunID:         record.RunID,
				Topic:         record.Topic,
				Partition:     record.Partition,
				Offset:        record.Offset,
				EventTime:     record.EventTime,
				KeyRaw:        record.KeyRaw,
				HeadersJSON:   record.HeadersJSON,
				SchemaID:      record.SchemaID,
				PayloadRaw:    record.PayloadRaw,
			}
		})
	default:
		return fmt.Errorf("unsupported parquet compression: %s", compression)
	}
}

func write[T any](output io.Writer, records []model.LandingRecord, mapper func(model.LandingRecord) T) error {
	writer := parquet.NewGenericWriter[T](output)
	for start := 0; start < len(records); start += parquetWriteChunkSize {
		end := start + parquetWriteChunkSize
		if end > len(records) {
			end = len(records)
		}

		chunk := make([]T, 0, end-start)
		for _, record := range records[start:end] {
			chunk = append(chunk, mapper(record))
		}
		if _, err := writer.Write(chunk); err != nil {
			return fmt.Errorf("write parquet rows: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close parquet writer: %w", err)
	}
	return nil
}
