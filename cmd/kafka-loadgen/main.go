package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var (
		brokersFlag  = flag.String("brokers", "localhost:19092", "Comma-separated Kafka brokers")
		topic        = flag.String("topic", "", "Kafka topic to produce to")
		count        = flag.Int("count", 0, "Number of events to produce")
		payloadBytes = flag.Int("payload-bytes", 4096, "Exact payload size in bytes")
		payloadMode  = flag.String("payload-mode", "repetitive", "Payload generator mode: repetitive or pseudo-random")
		randomSeed   = flag.Uint64("random-seed", 42, "Seed used by pseudo-random payload generation")
		batchSize    = flag.Int("batch-size", 1000, "Number of records per synchronous produce batch")
		reportEvery  = flag.Int("report-every", 50000, "Progress interval in number of produced records")
		keyPrefix    = flag.String("key-prefix", "order", "Prefix used when generating keys")
	)
	flag.Parse()

	switch {
	case *topic == "":
		fail("missing required -topic")
	case *count <= 0:
		fail("count must be > 0")
	case *payloadBytes < 128:
		fail("payload-bytes must be >= 128 to fit the JSON envelope")
	case *batchSize <= 0:
		fail("batch-size must be > 0")
	case *reportEvery <= 0:
		fail("report-every must be > 0")
	}
	if !isSupportedPayloadMode(*payloadMode) {
		fail("payload-mode must be one of: repetitive, pseudo-random")
	}

	brokers := splitNonEmpty(*brokersFlag)
	if len(brokers) == 0 {
		fail("brokers must not be empty")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
	if err != nil {
		fail("create kafka producer: %v", err)
	}
	defer client.Close()

	idWidth := len(strconv.Itoa(*count))
	start := time.Now()
	produced := 0
	totalBytes := 0

	for produced < *count {
		remaining := *count - produced
		size := *batchSize
		if remaining < size {
			size = remaining
		}

		records := make([]*kgo.Record, 0, size)
		for i := 0; i < size; i++ {
			sequence := produced + i + 1
			value := buildPayload(sequence, idWidth, *payloadBytes, *payloadMode, *randomSeed)
			key := []byte(fmt.Sprintf("%s-%0*d", *keyPrefix, idWidth, sequence))
			records = append(records, &kgo.Record{
				Topic: topicOrPanic(*topic),
				Key:   key,
				Value: value,
				Headers: []kgo.RecordHeader{
					{Key: "source", Value: []byte("load-test")},
					{Key: "type", Value: []byte("bulk")},
				},
			})
			totalBytes += len(value)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		err := client.ProduceSync(ctx, records...).FirstErr()
		cancel()
		if err != nil {
			fail("produce batch ending at message %d: %v", produced+size, err)
		}

		produced += size
		if produced%*reportEvery == 0 || produced == *count {
			elapsed := time.Since(start)
			throughput := float64(produced) / elapsed.Seconds()
			mbPerSec := (float64(totalBytes) / (1024 * 1024)) / elapsed.Seconds()
			fmt.Printf(
				"produced=%d/%d elapsed=%s records_per_sec=%.0f mb_per_sec=%.2f\n",
				produced,
				*count,
				elapsed.Round(time.Millisecond),
				throughput,
				mbPerSec,
			)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf(
		"completed topic=%s count=%d payload_bytes=%d payload_mode=%s total_payload_mb=%.2f elapsed=%s records_per_sec=%.0f mb_per_sec=%.2f\n",
		*topic,
		*count,
		*payloadBytes,
		*payloadMode,
		float64(totalBytes)/(1024*1024),
		elapsed.Round(time.Millisecond),
		float64(produced)/elapsed.Seconds(),
		(float64(totalBytes)/(1024*1024))/elapsed.Seconds(),
	)
}

func buildPayload(sequence int, idWidth int, targetBytes int, payloadMode string, randomSeed uint64) []byte {
	id := fmt.Sprintf("%0*d", idWidth, sequence)
	prefix := fmt.Sprintf(`{"id":"%s","status":"bulk","source":"load-test","pad":"`, id)
	suffix := `"}` // closes pad string and JSON object

	paddingLength := targetBytes - len(prefix) - len(suffix)
	if paddingLength < 0 {
		fail("payload-bytes=%d is too small for generated JSON envelope", targetBytes)
	}

	payload := make([]byte, targetBytes)
	position := copy(payload, prefix)
	switch payloadMode {
	case "repetitive":
		for i := 0; i < paddingLength; i++ {
			payload[position+i] = 'x'
		}
	case "pseudo-random":
		fillPseudoRandomASCII(payload[position:position+paddingLength], sequence, randomSeed)
	default:
		fail("unsupported payload mode: %s", payloadMode)
	}
	position += paddingLength
	copy(payload[position:], suffix)
	return payload
}

func isSupportedPayloadMode(mode string) bool {
	switch mode {
	case "repetitive", "pseudo-random":
		return true
	default:
		return false
	}
}

const pseudoRandomAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"

func fillPseudoRandomASCII(dst []byte, sequence int, randomSeed uint64) {
	rng := splitMix64{
		state: mixedSeed(randomSeed, uint64(sequence)),
	}
	for i := range dst {
		dst[i] = pseudoRandomAlphabet[rng.next()&63]
	}
}

func mixedSeed(randomSeed uint64, sequence uint64) uint64 {
	seed := randomSeed ^ (sequence * 0x9e3779b97f4a7c15)
	if seed == 0 {
		return 0x6a09e667f3bcc909
	}
	return seed
}

type splitMix64 struct {
	state uint64
}

func (s *splitMix64) next() uint64 {
	s.state += 0x9e3779b97f4a7c15
	z := s.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func splitNonEmpty(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func topicOrPanic(topic string) string {
	if topic == "" {
		panic("topic must not be empty")
	}
	return topic
}

func fail(message string, args ...any) {
	fmt.Fprintf(os.Stderr, message+"\n", args...)
	os.Exit(1)
}
