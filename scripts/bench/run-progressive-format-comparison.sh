#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/deploy/docker-compose.local.yml}"
OUTPUT_ROOT="${OUTPUT_ROOT:-$ROOT_DIR/.artifacts/perf/progressive-format-comparison-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUTPUT_ROOT"

COUNTS="${COUNTS:-100000 200000 300000 400000 500000}"
FORMATS="${FORMATS:-parquet avro}"
COMPRESSION="${COMPRESSION:-snappy}"
PARTITIONS="${PARTITIONS:-6}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-8192}"
PAYLOAD_MODE="${PAYLOAD_MODE:-pseudo-random}"
RANDOM_SEED="${RANDOM_SEED:-20260419}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
REPORT_EVERY="${REPORT_EVERY:-100000}"
FLUSH_WORKERS="${FLUSH_WORKERS:-2}"
PARTITION_QUEUE_SIZE="${PARTITION_QUEUE_SIZE:-4}"
BATCH_MAX_RECORDS="${BATCH_MAX_RECORDS:-10000}"
BATCH_MAX_BYTES="${BATCH_MAX_BYTES:-104857600}"
BATCH_MAX_DURATION="${BATCH_MAX_DURATION:-10m}"
RUN_TIMEOUT="${RUN_TIMEOUT:-30m}"

SUMMARY_CSV="$OUTPUT_ROOT/summary.csv"
echo "event_count,format,compression,producer_elapsed_sec,producer_maxrss_kb,connector_elapsed_sec,connector_maxrss_kb,lag,objects,size_bytes,free_kb_after_cleanup,scenario_dir" >"$SUMMARY_CSV"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required" >&2
    exit 1
  fi
}

require_cmd docker-compose
require_cmd docker
require_cmd go
require_cmd python3

stop_stack() {
  docker-compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
}

wait_for_port() {
  local port="$1"
  local attempts=0
  until bash -lc ":</dev/tcp/127.0.0.1/$port" >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if (( attempts > 60 )); then
      echo "timed out waiting for port $port" >&2
      exit 1
    fi
    sleep 1
  done
}

start_stack() {
  docker-compose -f "$COMPOSE_FILE" up -d redpanda minio >/dev/null
  wait_for_port 19092
  wait_for_port 9000
}

extract_value() {
  local file="$1"
  local key="$2"
  python3 - "$file" "$key" <<'PY'
import sys
path, key = sys.argv[1], sys.argv[2]
with open(path, "r", encoding="utf-8") as fh:
    content = fh.read().strip().split()
for token in content:
    if token.startswith(key + "="):
        print(token.split("=", 1)[1])
        raise SystemExit(0)
raise SystemExit(1)
PY
}

collect_minio_stats() {
  local topic="$1"
  local json
  json="$(docker run --rm --network host --entrypoint /bin/sh minio/mc -c "mc alias set local http://127.0.0.1:9000 minioadmin minioadmin >/dev/null && mc du --recursive --json local/landing/source=kafka/topic=${topic} | tail -n 1")"
  python3 - "$json" <<'PY'
import json, sys
payload = json.loads(sys.argv[1])
print(f"{payload.get('objects', 0)},{payload.get('size', 0)}")
PY
}

collect_lag() {
  local consumer_group="$1"
  local attempts=0
  local lag=""
  while (( attempts < 6 )); do
    lag="$(docker-compose -f "$COMPOSE_FILE" exec -T redpanda rpk group describe "$consumer_group" | awk '$1=="TOTAL-LAG" {print $2; exit}')"
    if [[ -n "$lag" && "$lag" == "0" ]]; then
      echo "$lag"
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 5
  done

  echo "${lag:-unknown}"
}

free_kb() {
  df -Pk "$ROOT_DIR" | awk 'NR==2 {print $4}'
}

cleanup_and_record_free_space() {
  stop_stack
  free_kb
}

run_scenario() {
  local event_count="$1"
  local format="$2"
  local scenario_dir="$OUTPUT_ROOT/${event_count}/${format}"
  mkdir -p "$scenario_dir"

  echo
  echo "==> event_count=$event_count format=$format compression=$COMPRESSION"

  start_stack

  OUTPUT_DIR="$scenario_dir" \
  EVENT_COUNT="$event_count" \
  PAYLOAD_BYTES="$PAYLOAD_BYTES" \
  PARTITIONS="$PARTITIONS" \
  COMPRESSIONS="$COMPRESSION" \
  OUTPUT_FORMAT="$format" \
  RANDOM_SEED="$RANDOM_SEED" \
  PAYLOAD_MODE="$PAYLOAD_MODE" \
  BATCH_SIZE="$BATCH_SIZE" \
  REPORT_EVERY="$REPORT_EVERY" \
  KEEP_TOPICS=true \
  FLUSH_WORKERS="$FLUSH_WORKERS" \
  PARTITION_QUEUE_SIZE="$PARTITION_QUEUE_SIZE" \
  BATCH_MAX_RECORDS="$BATCH_MAX_RECORDS" \
  BATCH_MAX_BYTES="$BATCH_MAX_BYTES" \
  BATCH_MAX_DURATION="$BATCH_MAX_DURATION" \
  RUN_TIMEOUT="$RUN_TIMEOUT" \
  "$ROOT_DIR/scripts/bench/run-local-compression-matrix.sh"

  local config_file="$scenario_dir/${COMPRESSION}.yaml"
  local topic
  local consumer_group
  topic="$(awk '$1=="topic:" {print $2; exit}' "$config_file")"
  consumer_group="$(awk '$1=="consumer_group:" {print $2; exit}' "$config_file")"

  local producer_elapsed
  local producer_rss
  local connector_elapsed
  local connector_rss
  producer_elapsed="$(extract_value "$scenario_dir/${COMPRESSION}.producer.time" "producer_elapsed_sec")"
  producer_rss="$(extract_value "$scenario_dir/${COMPRESSION}.producer.time" "producer_maxrss_kb")"
  connector_elapsed="$(extract_value "$scenario_dir/${COMPRESSION}.connector.time" "connector_elapsed_sec")"
  connector_rss="$(extract_value "$scenario_dir/${COMPRESSION}.connector.time" "connector_maxrss_kb")"

  local lag
  lag="$(collect_lag "$consumer_group")"

  local minio_stats
  local objects
  local size_bytes
  minio_stats="$(collect_minio_stats "$topic")"
  objects="${minio_stats%%,*}"
  size_bytes="${minio_stats##*,}"

  local free_after_cleanup
  free_after_cleanup="$(cleanup_and_record_free_space)"

  echo "${event_count},${format},${COMPRESSION},${producer_elapsed},${producer_rss},${connector_elapsed},${connector_rss},${lag},${objects},${size_bytes},${free_after_cleanup},${scenario_dir}" >>"$SUMMARY_CSV"
}

trap stop_stack EXIT

stop_stack

for event_count in $COUNTS; do
  for format in $FORMATS; do
    run_scenario "$event_count" "$format"
  done
done

echo
echo "Completed progressive comparison. Summary: $SUMMARY_CSV"
