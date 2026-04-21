package avroutil

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/hamba/avro/v2/ocf"

	"landing-connector/internal/model"
)

func TestWriteRecordsSnappy(t *testing.T) {
	t.Parallel()

	headers := `{"source":"e2e"}`
	schemaID := int32(25)
	eventMicros := time.Date(2026, time.April, 21, 1, 18, 0, 0, time.UTC).UnixMicro()

	records := []model.LandingRecord{
		{
			IngestionTime: time.Date(2026, time.April, 21, 1, 18, 1, 0, time.UTC),
			RunID:         "run-1",
			Topic:         "orders",
			Partition:     0,
			Offset:        1,
			EventTime:     &eventMicros,
			KeyRaw:        []byte("order-1"),
			HeadersJSON:   &headers,
			SchemaID:      &schemaID,
			PayloadRaw:    []byte(`{"id":1}`),
		},
	}

	var buffer bytes.Buffer
	if err := WriteRecords(&buffer, records, "snappy"); err != nil {
		t.Fatalf("write records: %v", err)
	}
	if buffer.Len() == 0 {
		t.Fatal("expected avro buffer to be non-empty")
	}

	decoder, err := ocf.NewDecoder(bytes.NewReader(buffer.Bytes()))
	if err != nil {
		t.Fatalf("create avro decoder: %v", err)
	}

	if !decoder.HasNext() {
		t.Fatal("expected a record in avro container")
	}

	var got avroLandingRecord
	if err := decoder.Decode(&got); err != nil {
		t.Fatalf("decode avro record: %v", err)
	}
	if err := decoder.Error(); err != nil {
		t.Fatalf("decoder error: %v", err)
	}

	if got.RunID != "run-1" {
		t.Fatalf("expected run_id=run-1, got %q", got.RunID)
	}
	if got.Topic != "orders" {
		t.Fatalf("expected topic=orders, got %q", got.Topic)
	}
	if got.EventTime == nil || *got.EventTime != eventMicros {
		t.Fatalf("expected event_time=%d, got %#v", eventMicros, got.EventTime)
	}
	if got.SchemaID == nil || *got.SchemaID != schemaID {
		t.Fatalf("expected schema_id=%d, got %#v", schemaID, got.SchemaID)
	}
	if string(got.PayloadRaw) != `{"id":1}` {
		t.Fatalf("expected payload to roundtrip, got %q", string(got.PayloadRaw))
	}
}

func TestWriteRecordsRejectsUnsupportedCompression(t *testing.T) {
	t.Parallel()

	err := WriteRecords(&bytes.Buffer{}, []model.LandingRecord{
		{
			IngestionTime: time.Date(2026, time.April, 21, 1, 18, 1, 0, time.UTC),
			RunID:         "run-1",
			Topic:         "orders",
			Partition:     0,
			Offset:        1,
			PayloadRaw:    []byte(`{"id":1}`),
		},
	}, "zstd")
	if err == nil || !strings.Contains(err.Error(), "compression") {
		t.Fatalf("expected avro compression error, got %v", err)
	}
}
