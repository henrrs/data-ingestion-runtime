package config

import (
	"strings"
	"testing"
	"time"
)

func TestApplyDefaultsNormalizesCase(t *testing.T) {
	cfg := Config{
		Source: SourceConfig{Type: "KaFkA"},
		Sink:   SinkConfig{Type: "MiNiO"},
		Kafka: KafkaConfig{
			Security: KafkaSecurity{Mechanism: "plain"},
		},
		Output: OutputConfig{ParquetCompression: "ZSTD"},
		ADLS: ADLSConfig{
			Credential: CredentialSpec{Mode: "DEFAULT_AZURE_CREDENTIAL"},
		},
	}

	cfg.applyDefaults()

	if cfg.Source.Type != "kafka" {
		t.Fatalf("expected source.type to be normalized, got %q", cfg.Source.Type)
	}
	if cfg.Sink.Type != "minio" {
		t.Fatalf("expected sink.type to be normalized, got %q", cfg.Sink.Type)
	}
	if cfg.Output.ParquetCompression != "zstd" {
		t.Fatalf("expected parquet compression to be normalized, got %q", cfg.Output.ParquetCompression)
	}
	if cfg.ADLS.Credential.Mode != "default_azure_credential" {
		t.Fatalf("expected credential mode to be normalized, got %q", cfg.ADLS.Credential.Mode)
	}
	if cfg.Kafka.Security.Mechanism != "PLAIN" {
		t.Fatalf("expected mechanism to be normalized, got %q", cfg.Kafka.Security.Mechanism)
	}
	if cfg.Runtime.MaxParallelFlushes <= 0 {
		t.Fatalf("expected runtime.max_parallel_flushes default to be > 0, got %d", cfg.Runtime.MaxParallelFlushes)
	}
	if cfg.Runtime.PartitionQueueSize <= 0 {
		t.Fatalf("expected runtime.partition_queue_size default to be > 0, got %d", cfg.Runtime.PartitionQueueSize)
	}
	if cfg.Runtime.PprofAddr != "127.0.0.1:6060" {
		t.Fatalf("expected runtime.pprof_addr default, got %q", cfg.Runtime.PprofAddr)
	}
}

func TestValidateRejectsNonZeroCommitInterval(t *testing.T) {
	cfg := validMinIOConfig()
	cfg.Kafka.CommitInterval = time.Second

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "commit_interval") {
		t.Fatalf("expected commit_interval validation error, got %v", err)
	}
}

func TestValidateRejectsUnsupportedCompression(t *testing.T) {
	cfg := validMinIOConfig()
	cfg.Output.ParquetCompression = "zip"

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "parquet_compression") {
		t.Fatalf("expected parquet_compression validation error, got %v", err)
	}
}

func TestValidateRejectsInvalidRuntimeSettings(t *testing.T) {
	cfg := validMinIOConfig()
	cfg.Runtime.MaxParallelFlushes = 0

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "max_parallel_flushes") {
		t.Fatalf("expected max_parallel_flushes validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Runtime.PartitionQueueSize = 0
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "partition_queue_size") {
		t.Fatalf("expected partition_queue_size validation error, got %v", err)
	}
}

func validMinIOConfig() Config {
	return Config{
		PipelineID: "orders-landing",
		Source:     SourceConfig{Type: "kafka"},
		Sink:       SinkConfig{Type: "minio"},
		Kafka: KafkaConfig{
			Brokers:       []string{"localhost:19092"},
			Topic:         "orders",
			ConsumerGroup: "orders-landing-batch",
		},
		Batch: BatchConfig{
			MaxRecords:  10,
			MaxBytes:    1024,
			MaxDuration: time.Minute,
		},
		MinIO: MinIOConfig{
			Endpoint:  "localhost:9000",
			AccessKey: "minioadmin",
			SecretKey: "minioadmin",
			Bucket:    "landing",
		},
		Output: OutputConfig{
			ParquetCompression: "zstd",
		},
		Runtime: RuntimeConfig{
			MaxParallelFlushes: 1,
			PartitionQueueSize: 1,
		},
	}
}
