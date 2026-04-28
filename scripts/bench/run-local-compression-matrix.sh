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
COMPRESSIONS="${COMPRESSIONS:-null}"
OUTPUT_FORMAT="${OUTPUT_FORMAT:-avro}"
PIPELINE_ID="${PIPELINE_ID:-orders-stress}"
CONSUMER_GROUP_PREFIX="${CONSUMER_GROUP_PREFIX:-orders-stress-cg}"
TOPIC_PREFIX="${TOPIC_PREFIX:-orders-stress}"
MINIO_BUCKET="${MINIO_BUCKET:-landing}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_BASE_PATH_PREFIX="${MINIO_BASE_PATH_PREFIX:-source=kafka}"
MINIO_MULTIPART_PART_SIZE_MIB="${MINIO_MULTIPART_PART_SIZE_MIB:-16}"
FLUSH_WORKERS="${FLUSH_WORKERS:-2}"
ENCODE_WORKERS="${ENCODE_WORKERS:-$FLUSH_WORKERS}"
UPLOAD_WORKERS="${UPLOAD_WORKERS:-$FLUSH_WORKERS}"
PARTITION_QUEUE_SIZE="${PARTITION_QUEUE_SIZE:-4}"
FLUSH_QUEUE_SIZE="${FLUSH_QUEUE_SIZE:-0}"
IDLE_POLL_TIMEOUT="${IDLE_POLL_TIMEOUT:-2s}"
RUN_TIMEOUT="${RUN_TIMEOUT:-30m}"
KEEP_TOPICS="${KEEP_TOPICS:-false}"
BATCH_MAX_RECORDS="${BATCH_MAX_RECORDS:-10000}"
BATCH_MAX_BYTES="${BATCH_MAX_BYTES:-104857600}"
BATCH_MAX_DURATION="${BATCH_MAX_DURATION:-10m}"
INCLUDE_HEADERS="${INCLUDE_HEADERS:-true}"
INCLUDE_KEY="${INCLUDE_KEY:-true}"
KAFKA_POLL_RECORDS="${KAFKA_POLL_RECORDS:-1000}"
KAFKA_FETCH_MAX_BYTES="${KAFKA_FETCH_MAX_BYTES:-0}"
KAFKA_FETCH_MAX_PARTITION_BYTES="${KAFKA_FETCH_MAX_PARTITION_BYTES:-0}"
KAFKA_FETCH_MIN_BYTES="${KAFKA_FETCH_MIN_BYTES:-0}"
KAFKA_FETCH_MAX_WAIT="${KAFKA_FETCH_MAX_WAIT:-0s}"
GODEBUG_VALUE="${GODEBUG_VALUE:-}"
TIME_VERBOSE="${TIME_VERBOSE:-false}"

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
  poll_records: ${KAFKA_POLL_RECORDS}
  fetch_max_bytes: ${KAFKA_FETCH_MAX_BYTES}
  fetch_max_partition_bytes: ${KAFKA_FETCH_MAX_PARTITION_BYTES}
  fetch_min_bytes: ${KAFKA_FETCH_MIN_BYTES}
  fetch_max_wait: ${KAFKA_FETCH_MAX_WAIT}
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
  multipart_part_size_mib: ${MINIO_MULTIPART_PART_SIZE_MIB}
output:
  format: ${OUTPUT_FORMAT}
  compression: "${compression}"
  include_headers: ${INCLUDE_HEADERS}
  include_key: ${INCLUDE_KEY}
  file_prefix: part
runtime:
  max_parallel_flushes: ${FLUSH_WORKERS}
  max_parallel_encodes: ${ENCODE_WORKERS}
  max_parallel_uploads: ${UPLOAD_WORKERS}
  flush_queue_size: ${FLUSH_QUEUE_SIZE}
  partition_queue_size: ${PARTITION_QUEUE_SIZE}
  idle_poll_timeout: ${IDLE_POLL_TIMEOUT}
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

  if [[ "$TIME_VERBOSE" == "true" ]]; then
    env GODEBUG="$GODEBUG_VALUE" /usr/bin/time -v \
      -o "$OUTPUT_DIR/${compression}.connector.time.verbose" \
      go run ./cmd/landing-connector -config "$config_file" \
      >"$OUTPUT_DIR/${compression}.connector.log" 2>&1
    python3 - "$OUTPUT_DIR/${compression}.connector.time.verbose" >"$OUTPUT_DIR/${compression}.connector.time" <<'PY'
import re
import sys

content = open(sys.argv[1], "r", encoding="utf-8").read()
elapsed = re.search(r"Elapsed \(wall clock\) time .*: (?:(\d+):)?(\d+):(\d+(?:\.\d+)?)", content)
rss = re.search(r"Maximum resident set size \(kbytes\): (\d+)", content)
if not elapsed or not rss:
    raise SystemExit("unable to parse verbose time output")
hours = int(elapsed.group(1) or 0)
minutes = int(elapsed.group(2))
seconds = float(elapsed.group(3))
print(f"connector_elapsed_sec={hours*3600 + minutes*60 + seconds:.2f} connector_maxrss_kb={rss.group(1)}")
PY
  else
    env GODEBUG="$GODEBUG_VALUE" /usr/bin/time -f "connector_elapsed_sec=%e connector_maxrss_kb=%M" \
      -o "$OUTPUT_DIR/${compression}.connector.time" \
      go run ./cmd/landing-connector -config "$config_file" \
      >"$OUTPUT_DIR/${compression}.connector.log" 2>&1
  fi

  if [[ "$KEEP_TOPICS" != "true" ]]; then
    docker-compose -f "$COMPOSE_FILE" exec -T redpanda rpk topic delete "$topic" >/dev/null 2>&1 || {
      echo "warning: unable to delete topic $topic automatically" >&2
    }
  fi
done

echo
echo "Completed compression matrix. Results saved under $OUTPUT_DIR"
