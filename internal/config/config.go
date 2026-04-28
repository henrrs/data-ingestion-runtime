package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	goRuntime "runtime"
	"strconv"
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
	Brokers                []string      `yaml:"brokers"`
	Topic                  string        `yaml:"topic"`
	ConsumerGroup          string        `yaml:"consumer_group"`
	ClientID               string        `yaml:"client_id"`
	CommitInterval         time.Duration `yaml:"commit_interval"`
	PollRecords            int           `yaml:"poll_records"`
	FetchMaxBytes          int32         `yaml:"fetch_max_bytes"`
	FetchMaxPartitionBytes int32         `yaml:"fetch_max_partition_bytes"`
	FetchMinBytes          int32         `yaml:"fetch_min_bytes"`
	FetchMaxWait           time.Duration `yaml:"fetch_max_wait"`
	Security               KafkaSecurity `yaml:"security"`
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
	Endpoint             string `yaml:"endpoint"`
	AccessKey            string `yaml:"access_key"`
	SecretKey            string `yaml:"secret_key"`
	Bucket               string `yaml:"bucket"`
	BasePath             string `yaml:"base_path"`
	UseSSL               bool   `yaml:"use_ssl"`
	ForcePathStyle       bool   `yaml:"force_path_style"`
	MultipartPartSizeMiB int    `yaml:"multipart_part_size_mib"`
}

type CredentialSpec struct {
	Mode         string `yaml:"mode"`
	TenantID     string `yaml:"tenant_id"`
	ClientID     string `yaml:"client_id"`
	ClientSecret string `yaml:"client_secret"`
}

type OutputConfig struct {
	Format         string `yaml:"format"`
	Compression    string `yaml:"compression"`
	IncludeHeaders bool   `yaml:"include_headers"`
	IncludeKey     bool   `yaml:"include_key"`
	FilePrefix     string `yaml:"file_prefix"`
}

type RuntimeConfig struct {
	MaxParallelFlushes int           `yaml:"max_parallel_flushes"`
	MaxParallelEncodes int           `yaml:"max_parallel_encodes"`
	MaxParallelUploads int           `yaml:"max_parallel_uploads"`
	FlushQueueSize     int           `yaml:"flush_queue_size"`
	PartitionQueueSize int           `yaml:"partition_queue_size"`
	ExecutionMode      string        `yaml:"execution_mode"`
	AutotuneMode       string        `yaml:"autotune_mode"`
	AutotuneInterval   time.Duration `yaml:"autotune_interval"`
	AutotuneMaxWorkers int           `yaml:"autotune_max_workers"`
	AutotunePollMax    int           `yaml:"autotune_poll_max"`
	DrainIdlePollCount int           `yaml:"drain_idle_poll_count"`
	IdlePollTimeout    time.Duration `yaml:"idle_poll_timeout"`
	PprofEnabled       bool          `yaml:"pprof_enabled"`
	PprofAddr          string        `yaml:"pprof_addr"`
}

type hostProfile struct {
	CPUCores         int
	TotalMemoryBytes uint64
}

type tunedDefaults struct {
	PollRecords            int
	FetchMaxBytes          int32
	FetchMaxPartitionBytes int32
	FetchMinBytes          int32
	FetchMaxWait           time.Duration
	MaxParallelism         int
	AutotuneMaxWorkers     int
	FlushQueueSize         int
	PartitionQueueSize     int
}

var hostProfileDetector = detectHostProfile

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
	profile := hostProfileDetector()
	defaults := tuneDefaults(profile)
	scenarioRecordBytes := estimateScenarioRecordBytes(c.Batch, c.Output)
	scenarioFetchMaxBytes, scenarioFetchMaxPartitionBytes, scenarioFetchMinBytes := tuneFetchFromScenario(defaults, scenarioRecordBytes)

	if c.Source.Type == "" {
		c.Source.Type = "kafka"
	}
	if c.Sink.Type == "" {
		c.Sink.Type = "adls"
	}
	if c.Output.Format == "" {
		c.Output.Format = "avro"
	}
	if c.Output.Compression == "" {
		c.Output.Compression = "null"
	}
	if c.Output.FilePrefix == "" {
		c.Output.FilePrefix = "part"
	}
	if c.Kafka.PollRecords == 0 {
		c.Kafka.PollRecords = defaults.PollRecords
	}
	if c.Kafka.FetchMaxBytes == 0 {
		c.Kafka.FetchMaxBytes = scenarioFetchMaxBytes
	}
	if c.Kafka.FetchMaxPartitionBytes == 0 {
		c.Kafka.FetchMaxPartitionBytes = scenarioFetchMaxPartitionBytes
	}
	if c.Kafka.FetchMinBytes == 0 {
		c.Kafka.FetchMinBytes = scenarioFetchMinBytes
	}
	if c.Kafka.FetchMaxWait == 0 {
		c.Kafka.FetchMaxWait = defaults.FetchMaxWait
	}
	if c.ADLS.Credential.Mode == "" {
		c.ADLS.Credential.Mode = "default_azure_credential"
	}
	if c.MinIO.MultipartPartSizeMiB == 0 {
		c.MinIO.MultipartPartSizeMiB = 16
	}
	if c.Runtime.MaxParallelFlushes == 0 {
		c.Runtime.MaxParallelFlushes = defaults.MaxParallelism
	}
	if c.Runtime.MaxParallelEncodes == 0 {
		c.Runtime.MaxParallelEncodes = c.Runtime.MaxParallelFlushes
	}
	if c.Runtime.MaxParallelUploads == 0 {
		c.Runtime.MaxParallelUploads = c.Runtime.MaxParallelFlushes
	}
	if c.Runtime.FlushQueueSize == 0 {
		minQueue := max(c.Runtime.MaxParallelEncodes, c.Runtime.MaxParallelUploads) * 2
		c.Runtime.FlushQueueSize = max(minQueue, defaults.FlushQueueSize)
	}
	if c.Runtime.PartitionQueueSize == 0 {
		minQueue := c.Runtime.MaxParallelUploads * 2
		c.Runtime.PartitionQueueSize = max(minQueue, defaults.PartitionQueueSize)
	}
	if c.Runtime.ExecutionMode == "" {
		c.Runtime.ExecutionMode = "auto"
	}
	if c.Runtime.AutotuneMode == "" {
		c.Runtime.AutotuneMode = "auto"
	}
	if c.Runtime.AutotuneInterval == 0 {
		c.Runtime.AutotuneInterval = 20 * time.Second
	}
	if c.Runtime.AutotuneMaxWorkers == 0 {
		c.Runtime.AutotuneMaxWorkers = defaults.AutotuneMaxWorkers
		c.Runtime.AutotuneMaxWorkers = max(c.Runtime.AutotuneMaxWorkers, c.Runtime.MaxParallelFlushes*2)
		c.Runtime.AutotuneMaxWorkers = min(c.Runtime.AutotuneMaxWorkers, 64)
	}
	if c.Runtime.AutotunePollMax == 0 {
		c.Runtime.AutotunePollMax = clampAutoPoll(c.Kafka.PollRecords * 4)
	}
	if c.Runtime.DrainIdlePollCount == 0 {
		c.Runtime.DrainIdlePollCount = 2
	}
	if c.Runtime.IdlePollTimeout == 0 {
		c.Runtime.IdlePollTimeout = 2 * time.Second
	}
	if c.Runtime.PprofAddr == "" {
		c.Runtime.PprofAddr = "127.0.0.1:6060"
	}

	c.Source.Type = strings.ToLower(c.Source.Type)
	c.Sink.Type = strings.ToLower(c.Sink.Type)
	c.Output.Format = strings.ToLower(c.Output.Format)
	c.Output.Compression = strings.ToLower(c.Output.Compression)
	c.Runtime.ExecutionMode = strings.ToLower(c.Runtime.ExecutionMode)
	c.Runtime.AutotuneMode = strings.ToLower(c.Runtime.AutotuneMode)
	c.ADLS.Credential.Mode = strings.ToLower(c.ADLS.Credential.Mode)
	c.Kafka.Security.Mechanism = strings.ToUpper(c.Kafka.Security.Mechanism)
}

func detectHostProfile() hostProfile {
	profile := hostProfile{
		CPUCores:         goRuntime.GOMAXPROCS(0),
		TotalMemoryBytes: 0,
	}
	if profile.CPUCores <= 0 {
		profile.CPUCores = 1
	}

	memBytes, err := readLinuxMemTotalBytes("/proc/meminfo")
	if err == nil && memBytes > 0 {
		profile.TotalMemoryBytes = memBytes
	}
	return profile
}

func readLinuxMemTotalBytes(path string) (uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, fmt.Errorf("invalid MemTotal line: %q", line)
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse MemTotal kB: %w", err)
		}
		return kb * 1024, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, errors.New("MemTotal not found")
}

func tuneDefaults(profile hostProfile) tunedDefaults {
	cpu := profile.CPUCores
	if cpu <= 0 {
		cpu = goRuntime.GOMAXPROCS(0)
	}
	if cpu <= 0 {
		cpu = 1
	}

	const (
		kiB = 1024
		miB = 1024 * kiB
		giB = 1024 * miB
	)

	mem := profile.TotalMemoryBytes
	switch {
	case mem == 0:
		maxParallel := min(max(1, cpu/2), 3)
		return tunedDefaults{
			PollRecords:            2000,
			FetchMaxBytes:          32 * miB,
			FetchMaxPartitionBytes: 8 * miB,
			FetchMinBytes:          512 * kiB,
			FetchMaxWait:           50 * time.Millisecond,
			MaxParallelism:         maxParallel,
			AutotuneMaxWorkers:     12,
			FlushQueueSize:         max(8, maxParallel*4),
			PartitionQueueSize:     64,
		}
	case mem > 0 && mem <= 8*giB:
		maxParallel := min(max(1, cpu/2), 2)
		return tunedDefaults{
			PollRecords:            2000,
			FetchMaxBytes:          32 * miB,
			FetchMaxPartitionBytes: 8 * miB,
			FetchMinBytes:          256 * kiB,
			FetchMaxWait:           25 * time.Millisecond,
			MaxParallelism:         maxParallel,
			AutotuneMaxWorkers:     12,
			FlushQueueSize:         max(8, maxParallel*4),
			PartitionQueueSize:     64,
		}
	case mem > 0 && mem <= 16*giB:
		maxParallel := min(max(2, cpu/2), 4)
		return tunedDefaults{
			PollRecords:            4000,
			FetchMaxBytes:          64 * miB,
			FetchMaxPartitionBytes: 16 * miB,
			FetchMinBytes:          1 * miB,
			FetchMaxWait:           50 * time.Millisecond,
			MaxParallelism:         maxParallel,
			AutotuneMaxWorkers:     20,
			FlushQueueSize:         max(16, maxParallel*4),
			PartitionQueueSize:     128,
		}
	default:
		maxParallel := min(max(2, cpu/2), 6)
		return tunedDefaults{
			PollRecords:            8000,
			FetchMaxBytes:          128 * miB,
			FetchMaxPartitionBytes: 32 * miB,
			FetchMinBytes:          2 * miB,
			FetchMaxWait:           75 * time.Millisecond,
			MaxParallelism:         maxParallel,
			AutotuneMaxWorkers:     32,
			FlushQueueSize:         max(24, maxParallel*4),
			PartitionQueueSize:     256,
		}
	}
}

func estimateScenarioRecordBytes(batch BatchConfig, output OutputConfig) int {
	const (
		minBytesPerRecord = 1024
		maxBytesPerRecord = 512 * 1024
		defaultRecordSize = 8 * 1024
	)

	recordBytes := defaultRecordSize
	if batch.MaxRecords > 0 && batch.MaxBytes > 0 {
		candidate := batch.MaxBytes / batch.MaxRecords
		if candidate > 0 {
			recordBytes = candidate
		}
	}
	if output.IncludeHeaders {
		recordBytes += 256
	}
	if output.IncludeKey {
		recordBytes += 128
	}

	if recordBytes < minBytesPerRecord {
		return minBytesPerRecord
	}
	if recordBytes > maxBytesPerRecord {
		return maxBytesPerRecord
	}
	return recordBytes
}

func tuneFetchFromScenario(defaults tunedDefaults, recordBytes int) (int32, int32, int32) {
	const (
		miB             = 1024 * 1024
		minFetchMax     = 8 * miB
		maxFetchMax     = 256 * miB
		minFetchPartMax = 1 * miB
		maxFetchPartMax = 64 * miB
	)

	scenarioPart := recordBytes * 384
	fetchMaxPartition := max(int(defaults.FetchMaxPartitionBytes), scenarioPart)
	fetchMaxPartition = min(max(fetchMaxPartition, minFetchPartMax), maxFetchPartMax)

	scenarioMax := fetchMaxPartition * 4
	fetchMax := max(int(defaults.FetchMaxBytes), scenarioMax)
	fetchMax = min(max(fetchMax, minFetchMax), maxFetchMax)

	scenarioMin := recordBytes * 32
	fetchMin := max(int(defaults.FetchMinBytes), scenarioMin)
	if fetchMin > fetchMax/4 {
		fetchMin = fetchMax / 4
	}

	return int32(fetchMax), int32(fetchMaxPartition), int32(fetchMin)
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
	case c.Kafka.PollRecords <= 0:
		return errors.New("kafka.poll_records must be > 0")
	case c.Kafka.FetchMaxBytes < 0:
		return errors.New("kafka.fetch_max_bytes must be >= 0")
	case c.Kafka.FetchMaxPartitionBytes < 0:
		return errors.New("kafka.fetch_max_partition_bytes must be >= 0")
	case c.Kafka.FetchMinBytes < 0:
		return errors.New("kafka.fetch_min_bytes must be >= 0")
	case c.Kafka.FetchMaxWait < 0:
		return errors.New("kafka.fetch_max_wait must be >= 0")
	case c.MinIO.MultipartPartSizeMiB < 0:
		return errors.New("minio.multipart_part_size_mib must be >= 0")
	case c.Batch.MaxRecords <= 0:
		return errors.New("batch.max_records must be > 0")
	case c.Batch.MaxBytes <= 0:
		return errors.New("batch.max_bytes must be > 0")
	case c.Batch.MaxDuration <= 0:
		return errors.New("batch.max_duration must be > 0")
	case c.Runtime.MaxParallelFlushes <= 0:
		return errors.New("runtime.max_parallel_flushes must be > 0")
	case c.Runtime.MaxParallelEncodes <= 0:
		return errors.New("runtime.max_parallel_encodes must be > 0")
	case c.Runtime.MaxParallelUploads <= 0:
		return errors.New("runtime.max_parallel_uploads must be > 0")
	case c.Runtime.FlushQueueSize <= 0:
		return errors.New("runtime.flush_queue_size must be > 0")
	case c.Runtime.PartitionQueueSize <= 0:
		return errors.New("runtime.partition_queue_size must be > 0")
	case c.Runtime.ExecutionMode == "":
		return errors.New("runtime.execution_mode is required")
	case c.Runtime.AutotuneMode == "":
		return errors.New("runtime.autotune_mode is required")
	case c.Runtime.AutotuneInterval <= 0:
		return errors.New("runtime.autotune_interval must be > 0")
	case c.Runtime.AutotuneMaxWorkers <= 0:
		return errors.New("runtime.autotune_max_workers must be > 0")
	case c.Runtime.AutotunePollMax <= 0:
		return errors.New("runtime.autotune_poll_max must be > 0")
	case c.Runtime.DrainIdlePollCount <= 0:
		return errors.New("runtime.drain_idle_poll_count must be > 0")
	case c.Runtime.IdlePollTimeout <= 0:
		return errors.New("runtime.idle_poll_timeout must be > 0")
	}

	switch c.Runtime.ExecutionMode {
	case "auto", "finite", "continuous":
	default:
		return fmt.Errorf("unsupported runtime.execution_mode: %s", c.Runtime.ExecutionMode)
	}

	switch c.Runtime.AutotuneMode {
	case "auto", "off":
	default:
		return fmt.Errorf("unsupported runtime.autotune_mode: %s", c.Runtime.AutotuneMode)
	}

	switch c.Output.Format {
	case "avro":
		switch c.Output.Compression {
		case "null", "snappy", "deflate":
		default:
			return fmt.Errorf("unsupported output.compression for format=avro: %s", c.Output.Compression)
		}
	default:
		return fmt.Errorf("unsupported output.format: %s", c.Output.Format)
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
		case c.MinIO.MultipartPartSizeMiB < 5:
			return errors.New("minio.multipart_part_size_mib must be >= 5")
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func clampAutoPoll(limit int) int {
	if limit < 1 {
		return 1
	}
	if limit > 32000 {
		return 32000
	}
	return limit
}

func (o OutputConfig) FileExtension() string {
	return "avro"
}
