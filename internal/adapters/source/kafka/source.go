package kafka

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

type Source struct {
	client *kgo.Client
}

func NewSource(cfg config.KafkaConfig) (*Source, error) {
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

	return &Source{client: client}, nil
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

	messages := make([]model.KafkaMessage, 0, limit)
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		for _, record := range partition.Records {
			headers := make(map[string]string, len(record.Headers))
			for _, h := range record.Headers {
				headers[h.Key] = string(h.Value)
			}
			messages = append(messages, model.KafkaMessage{
				Topic:     record.Topic,
				Partition: record.Partition,
				Offset:    record.Offset,
				EventTime: record.Timestamp,
				Key:       record.Key,
				Value:     record.Value,
				Headers:   headers,
				SchemaID:  extractSchemaID(record.Value),
			})
		}
	})

	return messages, nil
}

func (s *Source) Commit(ctx context.Context, window model.BatchWindow) error {
	records := make([]*kgo.Record, 0, len(window.OffsetsByPart))
	for _, offsetRange := range window.OffsetsByPart {
		records = append(records, &kgo.Record{
			Topic:     inferTopic(window),
			Partition: offsetRange.Partition,
			Offset:    offsetRange.EndOffset + 1,
		})
	}
	return s.client.CommitRecords(ctx, records...)
}

func inferTopic(window model.BatchWindow) string {
	if len(window.Records) == 0 {
		return ""
	}
	return window.Records[0].Topic
}

func extractSchemaID(value []byte) int32 {
	if len(value) < 5 || value[0] != 0 {
		return 0
	}
	return int32(binary.BigEndian.Uint32(value[1:5]))
}
