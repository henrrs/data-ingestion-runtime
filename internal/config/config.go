package config

import (
	"errors"
	"fmt"
	"os"
	goRuntime "runtime"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	PipelineID string        `yaml:"pipeline_id"`
	RunTimeout time.Duration `yaml:"run_timeout"`
	Source     SourceConfig  `yaml:"source"`
	Sink       SinkConfig    `yaml:"sink"`
	Kafka      KafkaConfig   `yaml:"kafka"`
	Batch      BatchConfig   `yaml:"batch"`
	ADLS       ADLSConfig    `yaml:"adls"`
	MinIO      MinIOConfig   `yaml:"minio"`
	Output     OutputConfig  `yaml:"output"`
	Runtime    RuntimeConfig `yaml:"runtime"`
}

type SourceConfig struct {
	Type string `yaml:"type"`
}

type SinkConfig struct {
	Type string `yaml:"type"`
}

type KafkaConfig struct {
	Brokers        []string      `yaml:"brokers"`
	Topic          string        `yaml:"topic"`
	ConsumerGroup  string        `yaml:"consumer_group"`
	ClientID       string        `yaml:"client_id"`
	CommitInterval time.Duration `yaml:"commit_interval"`
	Security       KafkaSecurity `yaml:"security"`
}

type KafkaSecurity struct {
	SASLEnabled bool   `yaml:"sasl_enabled"`
	TLSEnabled  bool   `yaml:"tls_enabled"`
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
	Mechanism   string `yaml:"mechanism"`
}

type BatchConfig struct {
	MaxRecords  int           `yaml:"max_records"`
	MaxBytes    int           `yaml:"max_bytes"`
	MaxDuration time.Duration `yaml:"max_duration"`
}

type ADLSConfig struct {
	AccountName string         `yaml:"account_name"`
	Filesystem  string         `yaml:"filesystem"`
	BasePath    string         `yaml:"base_path"`
	Credential  CredentialSpec `yaml:"credential"`
}

type MinIOConfig struct {
	Endpoint       string `yaml:"endpoint"`
	AccessKey      string `yaml:"access_key"`
	SecretKey      string `yaml:"secret_key"`
	Bucket         string `yaml:"bucket"`
	BasePath       string `yaml:"base_path"`
	UseSSL         bool   `yaml:"use_ssl"`
	ForcePathStyle bool   `yaml:"force_path_style"`
}

type CredentialSpec struct {
	Mode         string `yaml:"mode"`
	TenantID     string `yaml:"tenant_id"`
	ClientID     string `yaml:"client_id"`
	ClientSecret string `yaml:"client_secret"`
}

type OutputConfig struct {
	ParquetCompression string `yaml:"parquet_compression"`
	IncludeHeaders     bool   `yaml:"include_headers"`
	IncludeKey         bool   `yaml:"include_key"`
	FilePrefix         string `yaml:"file_prefix"`
}

type RuntimeConfig struct {
	MaxParallelFlushes int    `yaml:"max_parallel_flushes"`
	PartitionQueueSize int    `yaml:"partition_queue_size"`
	PprofEnabled       bool   `yaml:"pprof_enabled"`
	PprofAddr          string `yaml:"pprof_addr"`
}

func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	cfg.applyDefaults()

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Source.Type == "" {
		c.Source.Type = "kafka"
	}
	if c.Sink.Type == "" {
		c.Sink.Type = "adls"
	}
	if c.Output.ParquetCompression == "" {
		c.Output.ParquetCompression = "zstd"
	}
	if c.Output.FilePrefix == "" {
		c.Output.FilePrefix = "part"
	}
	if c.ADLS.Credential.Mode == "" {
		c.ADLS.Credential.Mode = "default_azure_credential"
	}
	if c.Runtime.MaxParallelFlushes == 0 {
		c.Runtime.MaxParallelFlushes = min(goRuntime.GOMAXPROCS(0), 4)
	}
	if c.Runtime.PartitionQueueSize == 0 {
		c.Runtime.PartitionQueueSize = c.Runtime.MaxParallelFlushes * 2
	}
	if c.Runtime.PprofAddr == "" {
		c.Runtime.PprofAddr = "127.0.0.1:6060"
	}

	c.Source.Type = strings.ToLower(c.Source.Type)
	c.Sink.Type = strings.ToLower(c.Sink.Type)
	c.Output.ParquetCompression = strings.ToLower(c.Output.ParquetCompression)
	c.ADLS.Credential.Mode = strings.ToLower(c.ADLS.Credential.Mode)
	c.Kafka.Security.Mechanism = strings.ToUpper(c.Kafka.Security.Mechanism)
}

func (c Config) Validate() error {
	switch {
	case c.PipelineID == "":
		return errors.New("pipeline_id is required")
	case c.Source.Type == "":
		return errors.New("source.type is required")
	case c.Sink.Type == "":
		return errors.New("sink.type is required")
	case len(c.Kafka.Brokers) == 0:
		return errors.New("kafka.brokers is required")
	case c.Kafka.Topic == "":
		return errors.New("kafka.topic is required")
	case c.Kafka.ConsumerGroup == "":
		return errors.New("kafka.consumer_group is required")
	case c.Kafka.CommitInterval != 0:
		return errors.New("kafka.commit_interval must be 0 because offsets are committed only after sink write confirmation")
	case c.Batch.MaxRecords <= 0:
		return errors.New("batch.max_records must be > 0")
	case c.Batch.MaxBytes <= 0:
		return errors.New("batch.max_bytes must be > 0")
	case c.Batch.MaxDuration <= 0:
		return errors.New("batch.max_duration must be > 0")
	case c.Runtime.MaxParallelFlushes <= 0:
		return errors.New("runtime.max_parallel_flushes must be > 0")
	case c.Runtime.PartitionQueueSize <= 0:
		return errors.New("runtime.partition_queue_size must be > 0")
	}

	switch c.Output.ParquetCompression {
	case "uncompressed", "snappy", "gzip", "brotli", "lz4", "zstd":
	default:
		return fmt.Errorf("unsupported output.parquet_compression: %s", c.Output.ParquetCompression)
	}

	switch c.Source.Type {
	case "kafka":
	default:
		return fmt.Errorf("unsupported source.type: %s", c.Source.Type)
	}

	switch c.Sink.Type {
	case "adls":
		switch {
		case c.ADLS.AccountName == "":
			return errors.New("adls.account_name is required for sink.type=adls")
		case c.ADLS.Filesystem == "":
			return errors.New("adls.filesystem is required for sink.type=adls")
		case c.ADLS.BasePath == "":
			return errors.New("adls.base_path is required for sink.type=adls")
		}
	case "minio":
		switch {
		case c.MinIO.Endpoint == "":
			return errors.New("minio.endpoint is required for sink.type=minio")
		case c.MinIO.AccessKey == "":
			return errors.New("minio.access_key is required for sink.type=minio")
		case c.MinIO.SecretKey == "":
			return errors.New("minio.secret_key is required for sink.type=minio")
		case c.MinIO.Bucket == "":
			return errors.New("minio.bucket is required for sink.type=minio")
		}
	default:
		return fmt.Errorf("unsupported sink.type: %s", c.Sink.Type)
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
