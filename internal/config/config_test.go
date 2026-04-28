package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func stubHostProfile(t *testing.T, profile hostProfile) {
	t.Helper()
	original := hostProfileDetector
	hostProfileDetector = func() hostProfile {
		return profile
	}
	t.Cleanup(func() {
		hostProfileDetector = original
	})
}

func TestApplyDefaultsNormalizesCase(t *testing.T) {
	stubHostProfile(t, hostProfile{CPUCores: 6, TotalMemoryBytes: 8 * 1024 * 1024 * 1024})

	cfg := Config{
		Source: SourceConfig{Type: "KaFkA"},
		Sink:   SinkConfig{Type: "MiNiO"},
		Kafka: KafkaConfig{
			Security: KafkaSecurity{Mechanism: "plain"},
		},
		Output: OutputConfig{
			Format:      "AvRo",
			Compression: "SnApPy",
		},
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
	if cfg.Output.Format != "avro" {
		t.Fatalf("expected output.format to be normalized, got %q", cfg.Output.Format)
	}
	if cfg.Output.Compression != "snappy" {
		t.Fatalf("expected output.compression to be normalized, got %q", cfg.Output.Compression)
	}
	if cfg.Kafka.PollRecords != 2000 {
		t.Fatalf("expected kafka.poll_records default, got %d", cfg.Kafka.PollRecords)
	}
	if cfg.Kafka.FetchMaxBytes != 32*1024*1024 {
		t.Fatalf("expected kafka.fetch_max_bytes default, got %d", cfg.Kafka.FetchMaxBytes)
	}
	if cfg.Kafka.FetchMaxPartitionBytes != 8*1024*1024 {
		t.Fatalf("expected kafka.fetch_max_partition_bytes default, got %d", cfg.Kafka.FetchMaxPartitionBytes)
	}
	if cfg.Kafka.FetchMinBytes != 262144 {
		t.Fatalf("expected kafka.fetch_min_bytes default, got %d", cfg.Kafka.FetchMinBytes)
	}
	if cfg.Kafka.FetchMaxWait != 25*time.Millisecond {
		t.Fatalf("expected kafka.fetch_max_wait default, got %s", cfg.Kafka.FetchMaxWait)
	}
	if cfg.MinIO.MultipartPartSizeMiB != 16 {
		t.Fatalf("expected minio.multipart_part_size_mib default, got %d", cfg.MinIO.MultipartPartSizeMiB)
	}
	if cfg.ADLS.Credential.Mode != "default_azure_credential" {
		t.Fatalf("expected credential mode to be normalized, got %q", cfg.ADLS.Credential.Mode)
	}
	if cfg.Kafka.Security.Mechanism != "PLAIN" {
		t.Fatalf("expected mechanism to be normalized, got %q", cfg.Kafka.Security.Mechanism)
	}
	if cfg.Runtime.MaxParallelFlushes != 2 {
		t.Fatalf("expected runtime.max_parallel_flushes default, got %d", cfg.Runtime.MaxParallelFlushes)
	}
	if cfg.Runtime.MaxParallelEncodes != 2 {
		t.Fatalf("expected runtime.max_parallel_encodes default, got %d", cfg.Runtime.MaxParallelEncodes)
	}
	if cfg.Runtime.MaxParallelUploads != 2 {
		t.Fatalf("expected runtime.max_parallel_uploads default, got %d", cfg.Runtime.MaxParallelUploads)
	}
	if cfg.Runtime.FlushQueueSize != 8 {
		t.Fatalf("expected runtime.flush_queue_size default, got %d", cfg.Runtime.FlushQueueSize)
	}
	if cfg.Runtime.PartitionQueueSize != 64 {
		t.Fatalf("expected runtime.partition_queue_size default, got %d", cfg.Runtime.PartitionQueueSize)
	}
	if cfg.Runtime.ExecutionMode != "auto" {
		t.Fatalf("expected runtime.execution_mode default, got %q", cfg.Runtime.ExecutionMode)
	}
	if cfg.Runtime.AutotuneMode != "auto" {
		t.Fatalf("expected runtime.autotune_mode default, got %q", cfg.Runtime.AutotuneMode)
	}
	if cfg.Runtime.AutotuneInterval != 20*time.Second {
		t.Fatalf("expected runtime.autotune_interval default, got %s", cfg.Runtime.AutotuneInterval)
	}
	if cfg.Runtime.AutotuneMaxWorkers != 12 {
		t.Fatalf("expected runtime.autotune_max_workers default, got %d", cfg.Runtime.AutotuneMaxWorkers)
	}
	if cfg.Runtime.AutotunePollMax != 8000 {
		t.Fatalf("expected runtime.autotune_poll_max default, got %d", cfg.Runtime.AutotunePollMax)
	}
	if cfg.Runtime.DrainIdlePollCount != 2 {
		t.Fatalf("expected runtime.drain_idle_poll_count default, got %d", cfg.Runtime.DrainIdlePollCount)
	}
	if cfg.Runtime.IdlePollTimeout != 2*time.Second {
		t.Fatalf("expected runtime.idle_poll_timeout default, got %s", cfg.Runtime.IdlePollTimeout)
	}
	if cfg.Runtime.PprofAddr != "127.0.0.1:6060" {
		t.Fatalf("expected runtime.pprof_addr default, got %q", cfg.Runtime.PprofAddr)
	}
}

func TestApplyDefaultsKeepsExplicitKnobs(t *testing.T) {
	stubHostProfile(t, hostProfile{CPUCores: 32, TotalMemoryBytes: 64 * 1024 * 1024 * 1024})

	cfg := Config{
		Kafka: KafkaConfig{
			PollRecords:            777,
			FetchMaxBytes:          111,
			FetchMaxPartitionBytes: 222,
			FetchMinBytes:          333,
			FetchMaxWait:           444 * time.Millisecond,
		},
		MinIO: MinIOConfig{
			MultipartPartSizeMiB: 32,
		},
		Runtime: RuntimeConfig{
			MaxParallelFlushes: 9,
			MaxParallelEncodes: 8,
			MaxParallelUploads: 7,
			FlushQueueSize:     6,
			PartitionQueueSize: 5,
		},
	}

	cfg.applyDefaults()

	if cfg.Kafka.PollRecords != 777 {
		t.Fatalf("expected kafka.poll_records to remain explicit, got %d", cfg.Kafka.PollRecords)
	}
	if cfg.Kafka.FetchMaxBytes != 111 {
		t.Fatalf("expected kafka.fetch_max_bytes to remain explicit, got %d", cfg.Kafka.FetchMaxBytes)
	}
	if cfg.Kafka.FetchMaxPartitionBytes != 222 {
		t.Fatalf("expected kafka.fetch_max_partition_bytes to remain explicit, got %d", cfg.Kafka.FetchMaxPartitionBytes)
	}
	if cfg.Kafka.FetchMinBytes != 333 {
		t.Fatalf("expected kafka.fetch_min_bytes to remain explicit, got %d", cfg.Kafka.FetchMinBytes)
	}
	if cfg.Kafka.FetchMaxWait != 444*time.Millisecond {
		t.Fatalf("expected kafka.fetch_max_wait to remain explicit, got %s", cfg.Kafka.FetchMaxWait)
	}
	if cfg.Runtime.MaxParallelFlushes != 9 {
		t.Fatalf("expected runtime.max_parallel_flushes to remain explicit, got %d", cfg.Runtime.MaxParallelFlushes)
	}
	if cfg.Runtime.MaxParallelEncodes != 8 {
		t.Fatalf("expected runtime.max_parallel_encodes to remain explicit, got %d", cfg.Runtime.MaxParallelEncodes)
	}
	if cfg.Runtime.MaxParallelUploads != 7 {
		t.Fatalf("expected runtime.max_parallel_uploads to remain explicit, got %d", cfg.Runtime.MaxParallelUploads)
	}
	if cfg.Runtime.FlushQueueSize != 6 {
		t.Fatalf("expected runtime.flush_queue_size to remain explicit, got %d", cfg.Runtime.FlushQueueSize)
	}
	if cfg.Runtime.PartitionQueueSize != 5 {
		t.Fatalf("expected runtime.partition_queue_size to remain explicit, got %d", cfg.Runtime.PartitionQueueSize)
	}
	if cfg.Runtime.AutotuneMode != "auto" {
		t.Fatalf("expected runtime.autotune_mode default, got %q", cfg.Runtime.AutotuneMode)
	}
	if cfg.Runtime.AutotuneInterval != 20*time.Second {
		t.Fatalf("expected runtime.autotune_interval default, got %s", cfg.Runtime.AutotuneInterval)
	}
	if cfg.Runtime.AutotuneMaxWorkers != 32 {
		t.Fatalf("expected runtime.autotune_max_workers default from explicit parallelism, got %d", cfg.Runtime.AutotuneMaxWorkers)
	}
	if cfg.Runtime.AutotunePollMax != 3108 {
		t.Fatalf("expected runtime.autotune_poll_max default from explicit poll, got %d", cfg.Runtime.AutotunePollMax)
	}
	if cfg.MinIO.MultipartPartSizeMiB != 32 {
		t.Fatalf("expected minio.multipart_part_size_mib to remain explicit, got %d", cfg.MinIO.MultipartPartSizeMiB)
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
	cfg.Output.Compression = "zip"

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "output.compression") {
		t.Fatalf("expected output.compression validation error, got %v", err)
	}
}

func TestValidateAcceptsAvroCompression(t *testing.T) {
	cfg := validMinIOConfig()
	cfg.Output.Format = "avro"
	cfg.Output.Compression = "null"

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected avro config to be valid, got %v", err)
	}
}

func TestValidateRejectsUnsupportedAvroCompression(t *testing.T) {
	cfg := validMinIOConfig()
	cfg.Output.Format = "avro"
	cfg.Output.Compression = "zstd"

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "format=avro") {
		t.Fatalf("expected avro compression validation error, got %v", err)
	}
}

func TestValidateRejectsUnsupportedOutputFormat(t *testing.T) {
	cfg := validMinIOConfig()
	cfg.Output.Format = "orc"

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "unsupported output.format") {
		t.Fatalf("expected unsupported output.format error, got %v", err)
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
	cfg.Runtime.MaxParallelEncodes = 0
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "max_parallel_encodes") {
		t.Fatalf("expected max_parallel_encodes validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Runtime.MaxParallelUploads = 0
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "max_parallel_uploads") {
		t.Fatalf("expected max_parallel_uploads validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Runtime.FlushQueueSize = 0
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "flush_queue_size") {
		t.Fatalf("expected flush_queue_size validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Runtime.PartitionQueueSize = 0
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "partition_queue_size") {
		t.Fatalf("expected partition_queue_size validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Runtime.ExecutionMode = ""
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "execution_mode") {
		t.Fatalf("expected execution_mode validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Runtime.ExecutionMode = "invalid"
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "unsupported runtime.execution_mode") {
		t.Fatalf("expected unsupported runtime.execution_mode validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Runtime.AutotuneMode = ""
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "autotune_mode") {
		t.Fatalf("expected autotune_mode validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Runtime.AutotuneMode = "invalid"
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "unsupported runtime.autotune_mode") {
		t.Fatalf("expected unsupported runtime.autotune_mode validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Runtime.AutotuneInterval = 0
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "autotune_interval") {
		t.Fatalf("expected autotune_interval validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Runtime.AutotuneMaxWorkers = 0
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "autotune_max_workers") {
		t.Fatalf("expected autotune_max_workers validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Runtime.AutotunePollMax = 0
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "autotune_poll_max") {
		t.Fatalf("expected autotune_poll_max validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Runtime.DrainIdlePollCount = 0
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "drain_idle_poll_count") {
		t.Fatalf("expected drain_idle_poll_count validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Kafka.PollRecords = 0
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "poll_records") {
		t.Fatalf("expected poll_records validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Kafka.FetchMaxBytes = -1
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "fetch_max_bytes") {
		t.Fatalf("expected fetch_max_bytes validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.MinIO.MultipartPartSizeMiB = 4
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "multipart_part_size_mib") {
		t.Fatalf("expected multipart_part_size_mib validation error, got %v", err)
	}

	cfg = validMinIOConfig()
	cfg.Runtime.IdlePollTimeout = 0
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "idle_poll_timeout") {
		t.Fatalf("expected idle_poll_timeout validation error, got %v", err)
	}
}

func TestReadLinuxMemTotalBytes(t *testing.T) {
	meminfo := "MemTotal:       8176368 kB\nMemFree:         188764 kB\n"
	path := writeTempFile(t, meminfo)

	got, err := readLinuxMemTotalBytes(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := uint64(8176368 * 1024)
	if got != want {
		t.Fatalf("expected %d, got %d", want, got)
	}
}

func TestReadLinuxMemTotalBytesRejectsInvalidFormat(t *testing.T) {
	path := writeTempFile(t, "MemTotal: not-a-number kB\n")

	if _, err := readLinuxMemTotalBytes(path); err == nil {
		t.Fatal("expected parse error")
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
			PollRecords:   1000,
		},
		Batch: BatchConfig{
			MaxRecords:  10,
			MaxBytes:    1024,
			MaxDuration: time.Minute,
		},
		MinIO: MinIOConfig{
			Endpoint:             "localhost:9000",
			AccessKey:            "minioadmin",
			SecretKey:            "minioadmin",
			Bucket:               "landing",
			MultipartPartSizeMiB: 16,
		},
		Output: OutputConfig{
			Format:      "avro",
			Compression: "snappy",
		},
		Runtime: RuntimeConfig{
			MaxParallelFlushes: 1,
			MaxParallelEncodes: 1,
			MaxParallelUploads: 1,
			FlushQueueSize:     1,
			PartitionQueueSize: 1,
			ExecutionMode:      "finite",
			AutotuneMode:       "auto",
			AutotuneInterval:   20 * time.Second,
			AutotuneMaxWorkers: 2,
			AutotunePollMax:    4000,
			DrainIdlePollCount: 2,
			IdlePollTimeout:    2 * time.Second,
		},
	}
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()

	file, err := os.CreateTemp(t.TempDir(), "meminfo-*.txt")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := file.WriteString(content); err != nil {
		_ = file.Close()
		t.Fatalf("write temp file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	return file.Name()
}
