package batch

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

type Assembler struct {
	cfg       config.BatchConfig
	runID     string
	startedAt time.Time
	window    model.BatchWindow
}

func NewAssembler(cfg config.BatchConfig, runID string, now time.Time) *Assembler {
	return &Assembler{
		cfg:       cfg,
		runID:     runID,
		startedAt: now,
		window: model.BatchWindow{
			RunID:         runID,
			StartedAt:     now,
			OffsetsByPart: make(map[int32]model.OffsetRange),
		},
	}
}

func (a *Assembler) Add(msg model.KafkaMessage, includeKey bool, includeHeaders bool) error {
	payloadJSON, err := stringifyPayload(msg.Value)
	if err != nil {
		return fmt.Errorf("stringify payload at offset %d: %w", msg.Offset, err)
	}

	record := model.LandingRecord{
		IngestionTime: time.Now().UTC(),
		RunID:         a.runID,
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		PayloadJSON:   payloadJSON,
	}

	if !msg.EventTime.IsZero() {
		eventTime := msg.EventTime.UTC().UnixMicro()
		record.EventTime = &eventTime
	}
	if includeKey {
		key := string(msg.Key)
		record.KeyString = &key
	}
	if includeHeaders {
		headersJSON, err := json.Marshal(msg.Headers)
		if err != nil {
			return fmt.Errorf("marshal headers at offset %d: %w", msg.Offset, err)
		}
		headers := string(headersJSON)
		record.HeadersJSON = &headers
	}
	if msg.SchemaID > 0 {
		schemaID := msg.SchemaID
		record.SchemaID = &schemaID
	}

	a.window.Records = append(a.window.Records, record)
	a.window.BytesApprox += len(msg.Value) + len(msg.Key) + derefLen(record.HeadersJSON)

	offsetRange := a.window.OffsetsByPart[msg.Partition]
	if offsetRange.RecordCount == 0 {
		offsetRange = model.OffsetRange{
			Partition:   msg.Partition,
			StartOffset: msg.Offset,
			EndOffset:   msg.Offset,
			RecordCount: 1,
		}
	} else {
		offsetRange.EndOffset = msg.Offset
		offsetRange.RecordCount++
	}
	a.window.OffsetsByPart[msg.Partition] = offsetRange

	return nil
}

func (a *Assembler) ShouldFlush(now time.Time) bool {
	return len(a.window.Records) >= a.cfg.MaxRecords ||
		a.window.BytesApprox >= a.cfg.MaxBytes ||
		now.Sub(a.startedAt) >= a.cfg.MaxDuration
}

func (a *Assembler) Window(now time.Time) model.BatchWindow {
	w := a.window
	w.EndedAt = now
	return w
}

func stringifyPayload(raw []byte) (string, error) {
	if len(raw) == 0 {
		return "null", nil
	}
	if json.Valid(raw) {
		return string(raw), nil
	}
	encoded, err := json.Marshal(map[string]string{
		"base64_payload": base64.StdEncoding.EncodeToString(raw),
	})
	if err != nil {
		return "", fmt.Errorf("marshal binary payload wrapper: %w", err)
	}
	return string(encoded), nil
}

func derefLen(value *string) int {
	if value == nil {
		return 0
	}
	return len(*value)
}
