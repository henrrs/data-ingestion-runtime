package avroutil

import (
	"fmt"
	"io"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"

	"landing-connector/internal/model"
)

const (
	landingRecordSchema = `{
  "type": "record",
  "name": "LandingRecord",
  "namespace": "landing.connector",
  "fields": [
    {"name": "ingestion_time", "type": "long"},
    {"name": "run_id", "type": "string"},
    {"name": "topic", "type": "string"},
    {"name": "partition", "type": "int"},
    {"name": "offset", "type": "long"},
    {"name": "event_time", "type": ["null", "long"], "default": null},
    {"name": "key_raw", "type": ["null", "bytes"], "default": null},
    {"name": "headers_json", "type": ["null", "string"], "default": null},
    {"name": "schema_id", "type": ["null", "int"], "default": null},
    {"name": "payload_raw", "type": "bytes"}
  ]
}`
	avroOCFBlockSize = 16 * 1024 * 1024
)

var parsedLandingRecordSchema = avro.MustParse(landingRecordSchema)

type avroLandingRecord struct {
	IngestionTime int64   `avro:"ingestion_time"`
	RunID         string  `avro:"run_id"`
	Topic         string  `avro:"topic"`
	Partition     int32   `avro:"partition"`
	Offset        int64   `avro:"offset"`
	EventTime     *int64  `avro:"event_time"`
	KeyRaw        []byte  `avro:"key_raw"`
	HeadersJSON   *string `avro:"headers_json"`
	SchemaID      *int32  `avro:"schema_id"`
	PayloadRaw    []byte  `avro:"payload_raw"`
}

type StreamWriter struct {
	encoder *ocf.Encoder
}

func NewStreamWriter(output io.Writer, compression string) (*StreamWriter, error) {
	codec, err := toOCFCodec(compression)
	if err != nil {
		return nil, err
	}

	encoder, err := ocf.NewEncoderWithSchema(
		parsedLandingRecordSchema,
		output,
		ocf.WithCodec(codec),
		ocf.WithBlockLength(0),
		ocf.WithBlockSize(avroOCFBlockSize),
	)
	if err != nil {
		return nil, fmt.Errorf("create avro writer: %w", err)
	}

	return &StreamWriter{encoder: encoder}, nil
}

func (w *StreamWriter) WriteRecord(record model.LandingRecord) error {
	if err := w.encoder.Encode(toAvroRecord(record)); err != nil {
		return fmt.Errorf("encode avro row: %w", err)
	}
	return nil
}

func (w *StreamWriter) Close() error {
	if err := w.encoder.Close(); err != nil {
		return fmt.Errorf("flush avro writer: %w", err)
	}
	return nil
}

func WriteRecords(output io.Writer, records []model.LandingRecord, compression string) error {
	writer, err := NewStreamWriter(output, compression)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := writer.WriteRecord(record); err != nil {
			return err
		}
	}
	return writer.Close()
}

func toOCFCodec(compression string) (ocf.CodecName, error) {
	switch compression {
	case "null":
		return ocf.Null, nil
	case "snappy":
		return ocf.Snappy, nil
	case "deflate":
		return ocf.Deflate, nil
	default:
		return "", fmt.Errorf("unsupported avro compression: %s", compression)
	}
}

func toAvroRecord(record model.LandingRecord) avroLandingRecord {
	return avroLandingRecord{
		IngestionTime: record.IngestionTime.UTC().UnixMicro(),
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
}
