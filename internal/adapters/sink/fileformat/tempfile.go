package fileformat

import (
	"fmt"
	"io"

	"landing-connector/internal/adapters/sink/avroutil"
	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

type StreamWriter interface {
	WriteRecord(record model.LandingRecord) error
	Close() error
}

type formatWriter interface {
	WriteRecord(record model.LandingRecord) error
	Close() error
}

func NewStreamWriter(output io.Writer, outputCfg config.OutputConfig) (StreamWriter, error) {
	return newFormatWriter(output, outputCfg)
}

func newFormatWriter(output io.Writer, outputCfg config.OutputConfig) (formatWriter, error) {
	switch outputCfg.Format {
	case "avro":
		return avroutil.NewStreamWriter(output, outputCfg.Compression)
	default:
		return nil, fmt.Errorf("unsupported output format: %s", outputCfg.Format)
	}
}
