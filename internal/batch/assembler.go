package batch

import (
	"fmt"
	"os"
	"time"

	"landing-connector/internal/adapters/sink/fileformat"
	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

type PreparedWindow struct {
	Window model.BatchWindow
	File   *os.File
	Size   int64
}

type Assembler struct {
	cfg       config.BatchConfig
	outputCfg config.OutputConfig
	tempDir   string
	runID     string
	startedAt time.Time
	window    model.BatchWindow
	writer    fileformat.TempFileWriter
}

func NewAssembler(cfg config.BatchConfig, outputCfg config.OutputConfig, tempDir string, runID string, now time.Time) *Assembler {
	return &Assembler{
		cfg:       cfg,
		outputCfg: outputCfg,
		tempDir:   tempDir,
		runID:     runID,
		startedAt: now,
		window: model.BatchWindow{
			RunID:         runID,
			StartedAt:     now,
			OffsetsByPart: make(map[int32]model.OffsetRange),
		},
	}
}

func (a *Assembler) Add(msg model.KafkaMessage) error {
	record := model.LandingRecord{
		IngestionTime: msg.IngestionTime,
		RunID:         a.runID,
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		PayloadRaw:    msg.Value,
	}

	if !msg.EventTime.IsZero() {
		eventTime := msg.EventTime.UTC().UnixMicro()
		record.EventTime = &eventTime
	}
	if a.outputCfg.IncludeKey {
		record.KeyRaw = msg.Key
	}
	if len(msg.HeadersJSON) > 0 {
		headers := string(msg.HeadersJSON)
		record.HeadersJSON = &headers
	}
	if msg.SchemaID > 0 {
		schemaID := msg.SchemaID
		record.SchemaID = &schemaID
	}

	if err := a.appendRecord(record); err != nil {
		return err
	}

	a.window.Topic = msg.Topic
	a.window.RecordCount++
	a.window.BytesApprox += len(msg.Value) + len(record.KeyRaw) + len(msg.HeadersJSON)

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
		now.Sub(a.startedAt) >= a.cfg.MaxDuration
}

func (a *Assembler) Window(now time.Time) (PreparedWindow, error) {
	if a.window.RecordCount == 0 {
		return PreparedWindow{}, nil
	}

	a.window.EndedAt = now
	file, size, err := a.writer.Close()
	if err != nil {
		return PreparedWindow{}, fmt.Errorf("close temp writer: %w", err)
	}
	a.writer = nil

	return PreparedWindow{
		Window: a.window,
		File:   file,
		Size:   size,
	}, nil
}

func (a *Assembler) Abort() error {
	if a.writer == nil {
		return nil
	}
	err := a.writer.Abort()
	a.writer = nil
	return err
}

func (a *Assembler) appendRecord(record model.LandingRecord) error {
	if a.writer == nil {
		writer, err := fileformat.NewTempFileWriter(a.outputCfg, a.tempDir)
		if err != nil {
			return fmt.Errorf("create temp writer: %w", err)
		}
		a.writer = writer
	}

	if err := a.writer.AppendRecord(record); err != nil {
		return fmt.Errorf("append record to temp writer: %w", err)
	}
	return nil
}
