package kafka

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

type Source struct {
	client         *kgo.Client
	includeHeaders bool
}

func NewSource(cfg config.KafkaConfig, includeHeaders bool) (*Source, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.DisableAutoCommit(),
	}

	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	if cfg.Security.TLSEnabled {
		opts = append(opts, kgo.DialTLSConfig(new(tls.Config)))
	}
	if cfg.Security.SASLEnabled {
		mechanism := strings.ToUpper(cfg.Security.Mechanism)
		switch mechanism {
		case "", "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: cfg.Security.Username,
				Pass: cfg.Security.Password,
			}.AsMechanism()))
		default:
			return nil, fmt.Errorf("unsupported sasl mechanism: %s", cfg.Security.Mechanism)
		}
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	return &Source{
		client:         client,
		includeHeaders: includeHeaders,
	}, nil
}

func (s *Source) Close() error {
	s.client.Close()
	return nil
}

func (s *Source) Poll(ctx context.Context, limit int) ([]model.KafkaMessage, error) {
	fetches := s.client.PollRecords(ctx, limit)
	if errs := fetches.Errors(); len(errs) > 0 {
		return nil, fmt.Errorf("fetch topic %s partition %d: %w", errs[0].Topic, errs[0].Partition, errs[0].Err)
	}

	ingestionTime := timeNowUTC()
	messages := make([]model.KafkaMessage, 0, limit)
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		for _, record := range partition.Records {
			messages = append(messages, newKafkaMessage(record, ingestionTime, s.includeHeaders))
		}
	})

	return messages, nil
}

func (s *Source) Commit(ctx context.Context, window model.BatchWindow) error {
	records := make([]*kgo.Record, 0, len(window.OffsetsByPart))
	for _, offsetRange := range window.OffsetsByPart {
		records = append(records, &kgo.Record{
			Topic:     window.Topic,
			Partition: offsetRange.Partition,
			Offset:    offsetRange.EndOffset + 1,
		})
	}
	return s.client.CommitRecords(ctx, records...)
}

func extractSchemaID(value []byte) int32 {
	if len(value) < 5 || value[0] != 0 {
		return 0
	}
	return int32(binary.BigEndian.Uint32(value[1:5]))
}

func newKafkaMessage(record *kgo.Record, ingestionTime time.Time, includeHeaders bool) model.KafkaMessage {
	var headersJSON []byte
	if includeHeaders {
		headersJSON = marshalHeadersJSON(record.Headers)
	}

	return model.KafkaMessage{
		Topic:         record.Topic,
		Partition:     record.Partition,
		Offset:        record.Offset,
		EventTime:     record.Timestamp,
		IngestionTime: ingestionTime,
		Key:           record.Key,
		Value:         record.Value,
		HeadersJSON:   headersJSON,
		SchemaID:      extractSchemaID(record.Value),
	}
}

var timeNowUTC = func() time.Time {
	return time.Now().UTC()
}

func marshalHeadersJSON(headers []kgo.RecordHeader) []byte {
	if len(headers) == 0 {
		return []byte("{}")
	}

	var buffer bytes.Buffer
	buffer.Grow(len(headers) * 16)
	buffer.WriteByte('{')
	for i, header := range headers {
		if i > 0 {
			buffer.WriteByte(',')
		}
		writeJSONString(&buffer, header.Key)
		buffer.WriteByte(':')
		writeJSONString(&buffer, string(header.Value))
	}
	buffer.WriteByte('}')
	return buffer.Bytes()
}

func writeJSONString(buffer *bytes.Buffer, value string) {
	buffer.WriteByte('"')
	for _, r := range value {
		switch r {
		case '\\', '"':
			buffer.WriteByte('\\')
			buffer.WriteRune(r)
		case '\b':
			buffer.WriteString(`\b`)
		case '\f':
			buffer.WriteString(`\f`)
		case '\n':
			buffer.WriteString(`\n`)
		case '\r':
			buffer.WriteString(`\r`)
		case '\t':
			buffer.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(buffer, "\\u%04x", r)
				continue
			}
			buffer.WriteRune(r)
		}
	}
	buffer.WriteByte('"')
}
