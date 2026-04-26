package fileformat

import (
	"fmt"
	"io"
	"os"

	"landing-connector/internal/adapters/sink/avroutil"
	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

type TempFileWriter interface {
	AppendRecord(record model.LandingRecord) error
	Close() (*os.File, int64, error)
	Abort() error
}

type formatWriter interface {
	WriteRecord(record model.LandingRecord) error
	Close() error
}

type tempFileWriter struct {
	file   *os.File
	writer formatWriter
	closed bool
}

func NewTempFileWriter(outputCfg config.OutputConfig, tempDir string) (TempFileWriter, error) {
	tempFile, err := os.CreateTemp(tempDir, "landing-connector-*."+outputCfg.FileExtension())
	if err != nil {
		return nil, fmt.Errorf("create temp output file: %w", err)
	}

	writer, err := newFormatWriter(tempFile, outputCfg)
	if err != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
		return nil, err
	}

	return &tempFileWriter{
		file:   tempFile,
		writer: writer,
	}, nil
}

func WriteRecordsToTempFile(records []model.LandingRecord, outputCfg config.OutputConfig, tempDir string) (*os.File, int64, error) {
	writer, err := NewTempFileWriter(outputCfg, tempDir)
	if err != nil {
		return nil, 0, err
	}

	for _, record := range records {
		if err := writer.AppendRecord(record); err != nil {
			_ = writer.Abort()
			return nil, 0, err
		}
	}

	return writer.Close()
}

func (w *tempFileWriter) AppendRecord(record model.LandingRecord) error {
	if w.closed {
		return errorsf("append record on closed temp file writer")
	}
	if err := w.writer.WriteRecord(record); err != nil {
		return err
	}
	return nil
}

func (w *tempFileWriter) Close() (*os.File, int64, error) {
	if w.closed {
		return nil, 0, errorsf("close temp file writer twice")
	}
	w.closed = true

	if err := w.writer.Close(); err != nil {
		_ = w.file.Close()
		_ = os.Remove(w.file.Name())
		return nil, 0, err
	}

	info, err := w.file.Stat()
	if err != nil {
		_ = w.file.Close()
		_ = os.Remove(w.file.Name())
		return nil, 0, fmt.Errorf("stat temp output file: %w", err)
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		_ = w.file.Close()
		_ = os.Remove(w.file.Name())
		return nil, 0, fmt.Errorf("rewind temp output file: %w", err)
	}

	return w.file, info.Size(), nil
}

func (w *tempFileWriter) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true

	var err error
	if closeErr := w.file.Close(); closeErr != nil {
		err = errorsJoin(err, closeErr)
	}
	if removeErr := os.Remove(w.file.Name()); removeErr != nil && !os.IsNotExist(removeErr) {
		err = errorsJoin(err, removeErr)
	}
	return err
}

func newFormatWriter(output io.Writer, outputCfg config.OutputConfig) (formatWriter, error) {
	switch outputCfg.Format {
	case "avro":
		return avroutil.NewStreamWriter(output, outputCfg.Compression)
	default:
		return nil, fmt.Errorf("unsupported output format: %s", outputCfg.Format)
	}
}

func errorsJoin(base error, next error) error {
	if base == nil {
		return next
	}
	return fmt.Errorf("%v: %w", base, next)
}

func errorsf(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}
