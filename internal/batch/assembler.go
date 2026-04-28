package batch

import (
	"time"
	"unsafe"

	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

type PreparedWindow struct {
	Window model.BatchWindow
}

type Assembler struct {
	cfg     config.BatchConfig
	output  config.OutputConfig
	runID   string
	started time.Time
	window  model.BatchWindow
}

func NewAssembler(cfg config.BatchConfig, outputCfg config.OutputConfig, runID string, now time.Time) *Assembler {
	return &Assembler{
		cfg:     cfg,
		output:  outputCfg,
		runID:   runID,
		started: now,
		window: model.BatchWindow{
			RunID:         runID,
			StartedAt:     now,
			OffsetsByPart: make(map[int32]model.OffsetRange),
		},
	}
}

func BuildLandingRecord(msg model.KafkaMessage, runID string, includeKey bool) model.LandingRecord {
	record := model.LandingRecord{
		IngestionTime: msg.IngestionTime,
		RunID:         runID,
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		PayloadRaw:    msg.Value,
	}

	if !msg.EventTime.IsZero() {
		eventTime := msg.EventTime.UTC().UnixMicro()
		record.EventTime = &eventTime
	}
	if includeKey {
		record.KeyRaw = msg.Key
	}
	if len(msg.HeadersJSON) > 0 {
		// Safe because msg.HeadersJSON is produced per message and never mutated afterwards.
		headers := bytesToImmutableString(msg.HeadersJSON)
		record.HeadersJSON = &headers
	}
	if msg.SchemaID > 0 {
		schemaID := msg.SchemaID
		record.SchemaID = &schemaID
	}

	return record
}

func (a *Assembler) Add(msg model.KafkaMessage) error {
	a.window.Topic = msg.Topic
	a.window.RecordCount++
	approx := len(msg.Value) + len(msg.HeadersJSON)
	if a.output.IncludeKey {
		approx += len(msg.Key)
	}
	a.window.BytesApprox += approx

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
	return a.window.RecordCount >= a.cfg.MaxRecords ||
		a.window.BytesApprox >= a.cfg.MaxBytes ||
		now.Sub(a.started) >= a.cfg.MaxDuration
}

func (a *Assembler) Window(now time.Time) (PreparedWindow, error) {
	if a.window.RecordCount == 0 {
		return PreparedWindow{}, nil
	}

	a.window.EndedAt = now
	return PreparedWindow{
		Window: a.window,
	}, nil
}

func (a *Assembler) StartedAt() time.Time {
	return a.started
}

func (a *Assembler) Abort() error {
	return nil
}

func bytesToImmutableString(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(value), len(value))
}
