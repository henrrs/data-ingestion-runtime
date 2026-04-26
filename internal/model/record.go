package model

import "time"

type LandingRecord struct {
	IngestionTime time.Time
	RunID         string
	Topic         string
	Partition     int32
	Offset        int64
	EventTime     *int64
	KeyRaw        []byte
	HeadersJSON   *string
	SchemaID      *int32
	PayloadRaw    []byte
}

type KafkaMessage struct {
	Topic         string
	Partition     int32
	Offset        int64
	EventTime     time.Time
	IngestionTime time.Time
	Key           []byte
	Value         []byte
	HeadersJSON   []byte
	SchemaID      int32
}

type BatchWindow struct {
	RunID         string
	Topic         string
	StartedAt     time.Time
	EndedAt       time.Time
	OffsetsByPart map[int32]OffsetRange
	RecordCount   int
	BytesApprox   int
}

type OffsetRange struct {
	Partition   int32
	StartOffset int64
	EndOffset   int64
	RecordCount int
}
