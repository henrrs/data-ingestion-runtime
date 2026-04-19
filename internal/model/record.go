package model

import "time"

type LandingRecord struct {
	IngestionTime time.Time `parquet:"ingestion_time,zstd"`
	RunID         string    `parquet:"run_id,zstd"`
	Topic         string    `parquet:"topic,zstd"`
	Partition     int32     `parquet:"partition,zstd"`
	Offset        int64     `parquet:"offset,zstd"`
	EventTime     *int64    `parquet:"event_time,optional,timestamp(microsecond),zstd"`
	KeyString     *string   `parquet:"key_string,optional,zstd"`
	HeadersJSON   *string   `parquet:"headers_json,optional,zstd"`
	SchemaID      *int32    `parquet:"schema_id,optional,zstd"`
	PayloadJSON   string    `parquet:"payload_json,zstd"`
}

type KafkaMessage struct {
	Topic      string
	Partition  int32
	Offset     int64
	EventTime  time.Time
	Key        []byte
	Value      []byte
	Headers    map[string]string
	SchemaID   int32
}

type BatchWindow struct {
	RunID         string
	StartedAt     time.Time
	EndedAt       time.Time
	Records       []LandingRecord
	OffsetsByPart map[int32]OffsetRange
	BytesApprox   int
}

type OffsetRange struct {
	Partition   int32
	StartOffset int64
	EndOffset   int64
	RecordCount int
}
