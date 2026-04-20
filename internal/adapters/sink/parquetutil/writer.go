package parquetutil

import (
	"bytes"
	"fmt"
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
	KeyString     *string   `parquet:"key_string,optional,uncompressed"`
	HeadersJSON   *string   `parquet:"headers_json,optional,uncompressed"`
	SchemaID      *int32    `parquet:"schema_id,optional,uncompressed"`
	PayloadJSON   string    `parquet:"payload_json,uncompressed"`
}

type landingRecordSnappy struct {
	IngestionTime time.Time `parquet:"ingestion_time,snappy"`
	RunID         string    `parquet:"run_id,snappy"`
	Topic         string    `parquet:"topic,snappy"`
	Partition     int32     `parquet:"partition,snappy"`
	Offset        int64     `parquet:"offset,snappy"`
	EventTime     *int64    `parquet:"event_time,optional,snappy"`
	KeyString     *string   `parquet:"key_string,optional,snappy"`
	HeadersJSON   *string   `parquet:"headers_json,optional,snappy"`
	SchemaID      *int32    `parquet:"schema_id,optional,snappy"`
	PayloadJSON   string    `parquet:"payload_json,snappy"`
}

type landingRecordGzip struct {
	IngestionTime time.Time `parquet:"ingestion_time,gzip"`
	RunID         string    `parquet:"run_id,gzip"`
	Topic         string    `parquet:"topic,gzip"`
	Partition     int32     `parquet:"partition,gzip"`
	Offset        int64     `parquet:"offset,gzip"`
	EventTime     *int64    `parquet:"event_time,optional,gzip"`
	KeyString     *string   `parquet:"key_string,optional,gzip"`
	HeadersJSON   *string   `parquet:"headers_json,optional,gzip"`
	SchemaID      *int32    `parquet:"schema_id,optional,gzip"`
	PayloadJSON   string    `parquet:"payload_json,gzip"`
}

type landingRecordBrotli struct {
	IngestionTime time.Time `parquet:"ingestion_time,brotli"`
	RunID         string    `parquet:"run_id,brotli"`
	Topic         string    `parquet:"topic,brotli"`
	Partition     int32     `parquet:"partition,brotli"`
	Offset        int64     `parquet:"offset,brotli"`
	EventTime     *int64    `parquet:"event_time,optional,brotli"`
	KeyString     *string   `parquet:"key_string,optional,brotli"`
	HeadersJSON   *string   `parquet:"headers_json,optional,brotli"`
	SchemaID      *int32    `parquet:"schema_id,optional,brotli"`
	PayloadJSON   string    `parquet:"payload_json,brotli"`
}

type landingRecordLz4 struct {
	IngestionTime time.Time `parquet:"ingestion_time,lz4"`
	RunID         string    `parquet:"run_id,lz4"`
	Topic         string    `parquet:"topic,lz4"`
	Partition     int32     `parquet:"partition,lz4"`
	Offset        int64     `parquet:"offset,lz4"`
	EventTime     *int64    `parquet:"event_time,optional,lz4"`
	KeyString     *string   `parquet:"key_string,optional,lz4"`
	HeadersJSON   *string   `parquet:"headers_json,optional,lz4"`
	SchemaID      *int32    `parquet:"schema_id,optional,lz4"`
	PayloadJSON   string    `parquet:"payload_json,lz4"`
}

type landingRecordZstd struct {
	IngestionTime time.Time `parquet:"ingestion_time,zstd"`
	RunID         string    `parquet:"run_id,zstd"`
	Topic         string    `parquet:"topic,zstd"`
	Partition     int32     `parquet:"partition,zstd"`
	Offset        int64     `parquet:"offset,zstd"`
	EventTime     *int64    `parquet:"event_time,optional,zstd"`
	KeyString     *string   `parquet:"key_string,optional,zstd"`
	HeadersJSON   *string   `parquet:"headers_json,optional,zstd"`
	SchemaID      *int32    `parquet:"schema_id,optional,zstd"`
	PayloadJSON   string    `parquet:"payload_json,zstd"`
}

func WriteRecords(buffer *bytes.Buffer, records []model.LandingRecord, compression string) error {
	switch compression {
	case "uncompressed":
		return write(buffer, convert(records, func(record model.LandingRecord) landingRecordUncompressed {
			return landingRecordUncompressed{
				IngestionTime: record.IngestionTime,
				RunID:         record.RunID,
				Topic:         record.Topic,
				Partition:     record.Partition,
				Offset:        record.Offset,
				EventTime:     record.EventTime,
				KeyString:     record.KeyString,
				HeadersJSON:   record.HeadersJSON,
				SchemaID:      record.SchemaID,
				PayloadJSON:   record.PayloadJSON,
			}
		}))
	case "snappy":
		return write(buffer, convert(records, func(record model.LandingRecord) landingRecordSnappy {
			return landingRecordSnappy{
				IngestionTime: record.IngestionTime,
				RunID:         record.RunID,
				Topic:         record.Topic,
				Partition:     record.Partition,
				Offset:        record.Offset,
				EventTime:     record.EventTime,
				KeyString:     record.KeyString,
				HeadersJSON:   record.HeadersJSON,
				SchemaID:      record.SchemaID,
				PayloadJSON:   record.PayloadJSON,
			}
		}))
	case "gzip":
		return write(buffer, convert(records, func(record model.LandingRecord) landingRecordGzip {
			return landingRecordGzip{
				IngestionTime: record.IngestionTime,
				RunID:         record.RunID,
				Topic:         record.Topic,
				Partition:     record.Partition,
				Offset:        record.Offset,
				EventTime:     record.EventTime,
				KeyString:     record.KeyString,
				HeadersJSON:   record.HeadersJSON,
				SchemaID:      record.SchemaID,
				PayloadJSON:   record.PayloadJSON,
			}
		}))
	case "brotli":
		return write(buffer, convert(records, func(record model.LandingRecord) landingRecordBrotli {
			return landingRecordBrotli{
				IngestionTime: record.IngestionTime,
				RunID:         record.RunID,
				Topic:         record.Topic,
				Partition:     record.Partition,
				Offset:        record.Offset,
				EventTime:     record.EventTime,
				KeyString:     record.KeyString,
				HeadersJSON:   record.HeadersJSON,
				SchemaID:      record.SchemaID,
				PayloadJSON:   record.PayloadJSON,
			}
		}))
	case "lz4":
		return write(buffer, convert(records, func(record model.LandingRecord) landingRecordLz4 {
			return landingRecordLz4{
				IngestionTime: record.IngestionTime,
				RunID:         record.RunID,
				Topic:         record.Topic,
				Partition:     record.Partition,
				Offset:        record.Offset,
				EventTime:     record.EventTime,
				KeyString:     record.KeyString,
				HeadersJSON:   record.HeadersJSON,
				SchemaID:      record.SchemaID,
				PayloadJSON:   record.PayloadJSON,
			}
		}))
	case "zstd":
		return write(buffer, convert(records, func(record model.LandingRecord) landingRecordZstd {
			return landingRecordZstd{
				IngestionTime: record.IngestionTime,
				RunID:         record.RunID,
				Topic:         record.Topic,
				Partition:     record.Partition,
				Offset:        record.Offset,
				EventTime:     record.EventTime,
				KeyString:     record.KeyString,
				HeadersJSON:   record.HeadersJSON,
				SchemaID:      record.SchemaID,
				PayloadJSON:   record.PayloadJSON,
			}
		}))
	default:
		return fmt.Errorf("unsupported parquet compression: %s", compression)
	}
}

func write[T any](buffer *bytes.Buffer, records []T) error {
	writer := parquet.NewGenericWriter[T](buffer)
	if _, err := writer.Write(records); err != nil {
		return fmt.Errorf("write parquet rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close parquet writer: %w", err)
	}
	return nil
}

func convert[T any](records []model.LandingRecord, mapper func(model.LandingRecord) T) []T {
	out := make([]T, 0, len(records))
	for _, record := range records {
		out = append(out, mapper(record))
	}
	return out
}
