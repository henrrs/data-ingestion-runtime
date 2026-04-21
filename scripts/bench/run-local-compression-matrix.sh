#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT_DIR/.artifacts/perf/$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUTPUT_DIR"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/deploy/docker-compose.local.yml}"

BROKERS="${BROKERS:-localhost:19092}"
PARTITIONS="${PARTITIONS:-6}"
EVENT_COUNT="${EVENT_COUNT:-1000000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-8192}"
PAYLOAD_MODE="${PAYLOAD_MODE:-pseudo-random}"
RANDOM_SEED="${RANDOM_SEED:-20260419}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
REPORT_EVERY="${REPORT_EVERY:-100000}"
COMPRESSIONS="${COMPRESSIONS:-zstd snappy uncompressed}"
OUTPUT_FORMAT="${OUTPUT_FORMAT:-parquet}"
PIPELINE_ID="${PIPELINE_ID:-orders-stress}"
CONSUMER_GROUP_PREFIX="${CONSUMER_GROUP_PREFIX:-orders-stress-cg}"
TOPIC_PREFIX="${TOPIC_PREFIX:-orders-stress}"
MINIO_BUCKET="${MINIO_BUCKET:-landing}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_BASE_PATH_PREFIX="${MINIO_BASE_PATH_PREFIX:-source=kafka}"
FLUSH_WORKERS="${FLUSH_WORKERS:-2}"
ENCODE_WORKERS="${ENCODE_WORKERS:-$FLUSH_WORKERS}"
UPLOAD_WORKERS="${UPLOAD_WORKERS:-$FLUSH_WORKERS}"
PARTITION_QUEUE_SIZE="${PARTITION_QUEUE_SIZE:-4}"
FLUSH_QUEUE_SIZE="${FLUSH_QUEUE_SIZE:-0}"
RUN_TIMEOUT="${RUN_TIMEOUT:-30m}"
KEEP_TOPICS="${KEEP_TOPICS:-false}"
BATCH_MAX_RECORDS="${BATCH_MAX_RECORDS:-10000}"
BATCH_MAX_BYTES="${BATCH_MAX_BYTES:-104857600}"
BATCH_MAX_DURATION="${BATCH_MAX_DURATION:-10m}"

if [[ "$FLUSH_QUEUE_SIZE" == "0" ]]; then
  if (( ENCODE_WORKERS > UPLOAD_WORKERS )); then
    FLUSH_QUEUE_SIZE=$((ENCODE_WORKERS * 2))
  else
    FLUSH_QUEUE_SIZE=$((UPLOAD_WORKERS * 2))
  fi
fi

if ! command -v docker-compose >/dev/null 2>&1; then
  echo "docker-compose is required" >&2
  exit 1
fi

if ! command -v go >/dev/null 2>&1; then
  echo "go is required" >&2
  exit 1
fi

if [[ ! -x /usr/bin/time ]]; then
  echo "/usr/bin/time is required" >&2
  exit 1
fi

echo "Benchmark output: $OUTPUT_DIR"

for compression in $COMPRESSIONS; do
  run_id="$(date -u +%Y%m%dT%H%M%SZ)"
  topic="${TOPIC_PREFIX}-${compression}-${run_id}"
  consumer_group="${CONSUMER_GROUP_PREFIX}-${compression}-${run_id}"
  config_file="$OUTPUT_DIR/${compression}.yaml"

  echo
  echo "==> Running compression=$compression topic=$topic"

  cat >"$config_file" <<EOF
pipeline_id: ${PIPELINE_ID}-${compression}
run_timeout: ${RUN_TIMEOUT}
source:
  type: kafka
sink:
  type: minio
kafka:
  brokers:
    - ${BROKERS}
  topic: ${topic}
  consumer_group: ${consumer_group}
batch:
  max_records: ${BATCH_MAX_RECORDS}
  max_bytes: ${BATCH_MAX_BYTES}
  max_duration: ${BATCH_MAX_DURATION}
minio:
  endpoint: ${MINIO_ENDPOINT}
  access_key: ${MINIO_ACCESS_KEY}
  secret_key: ${MINIO_SECRET_KEY}
  bucket: ${MINIO_BUCKET}
  base_path: ${MINIO_BASE_PATH_PREFIX}/topic=${topic}
  use_ssl: false
  force_path_style: true
output:
  format: ${OUTPUT_FORMAT}
  compression: ${compression}
  include_headers: true
  include_key: true
  file_prefix: part
runtime:
  max_parallel_flushes: ${FLUSH_WORKERS}
  max_parallel_encodes: ${ENCODE_WORKERS}
  max_parallel_uploads: ${UPLOAD_WORKERS}
  flush_queue_size: ${FLUSH_QUEUE_SIZE}
  partition_queue_size: ${PARTITION_QUEUE_SIZE}
  pprof_enabled: false
  pprof_addr: 127.0.0.1:6060
EOF

  /usr/bin/time -f "producer_elapsed_sec=%e producer_maxrss_kb=%M" \
    -o "$OUTPUT_DIR/${compression}.producer.time" \
    go run ./cmd/kafka-loadgen \
      -brokers "$BROKERS" \
      -topic "$topic" \
      -create-topic \
      -topic-partitions "$PARTITIONS" \
      -topic-replicas 1 \
      -count "$EVENT_COUNT" \
      -payload-bytes "$PAYLOAD_BYTES" \
      -payload-mode "$PAYLOAD_MODE" \
      -random-seed "$RANDOM_SEED" \
      -batch-size "$BATCH_SIZE" \
      -report-every "$REPORT_EVERY" \
      >"$OUTPUT_DIR/${compression}.producer.log" 2>&1

  /usr/bin/time -f "connector_elapsed_sec=%e connector_maxrss_kb=%M" \
    -o "$OUTPUT_DIR/${compression}.connector.time" \
    go run ./cmd/landing-connector -config "$config_file" \
    >"$OUTPUT_DIR/${compression}.connector.log" 2>&1

  if [[ "$KEEP_TOPICS" != "true" ]]; then
    docker-compose -f "$COMPOSE_FILE" exec -T redpanda rpk topic delete "$topic" >/dev/null 2>&1 || {
      echo "warning: unable to delete topic $topic automatically" >&2
    }
  fi
done

echo
echo "Completed compression matrix. Results saved under $OUTPUT_DIR"
