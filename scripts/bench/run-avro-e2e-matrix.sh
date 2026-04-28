#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/deploy/docker-compose.local.yml}"
OUTPUT_ROOT="${OUTPUT_ROOT:-$ROOT_DIR/.artifacts/perf/avro-e2e-matrix-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUTPUT_ROOT"

COUNTS="${COUNTS:-100000 300000 500000}"
WORKER_MATRIX="${WORKER_MATRIX:-1:1:1 2:2:2 4:2:2 2:4:2 2:2:4 4:4:4}"
COMPRESSION="${COMPRESSION:-null}"
PARTITIONS="${PARTITIONS:-6}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-8192}"
PAYLOAD_MODE="${PAYLOAD_MODE:-pseudo-random}"
RANDOM_SEED="${RANDOM_SEED:-20260419}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
REPORT_EVERY="${REPORT_EVERY:-100000}"
PARTITION_QUEUE_SIZE="${PARTITION_QUEUE_SIZE:-8}"
BATCH_MAX_RECORDS="${BATCH_MAX_RECORDS:-10000}"
BATCH_MAX_BYTES="${BATCH_MAX_BYTES:-104857600}"
BATCH_MAX_DURATION="${BATCH_MAX_DURATION:-10m}"
RUN_TIMEOUT="${RUN_TIMEOUT:-45m}"
INCLUDE_HEADERS="${INCLUDE_HEADERS:-true}"
INCLUDE_KEY="${INCLUDE_KEY:-true}"
IDLE_POLL_TIMEOUT="${IDLE_POLL_TIMEOUT:-2s}"
IDLE_POLL_COUNT="${IDLE_POLL_COUNT:-15}"
KAFKA_POLL_RECORDS="${KAFKA_POLL_RECORDS:-1000}"
KAFKA_FETCH_MAX_BYTES="${KAFKA_FETCH_MAX_BYTES:-0}"
KAFKA_FETCH_MAX_PARTITION_BYTES="${KAFKA_FETCH_MAX_PARTITION_BYTES:-0}"
KAFKA_FETCH_MIN_BYTES="${KAFKA_FETCH_MIN_BYTES:-0}"
KAFKA_FETCH_MAX_WAIT="${KAFKA_FETCH_MAX_WAIT:-0s}"
TIME_VERBOSE="${TIME_VERBOSE:-false}"
GODEBUG_VALUE="${GODEBUG_VALUE:-}"

SUMMARY_CSV="$OUTPUT_ROOT/summary.csv"
echo "event_count,compression,flush_workers,encode_workers,upload_workers,producer_elapsed_sec,producer_maxrss_kb,connector_elapsed_sec,connector_maxrss_kb,throughput_records_per_sec,lag,objects,size_bytes,heap_alloc_bytes,heap_inuse_bytes,heap_sys_bytes,total_alloc_bytes,alloc_delta_bytes,alloc_rate_bytes_per_sec,gc_cycles,gc_pause_total_ns,page_cache_before_kb,page_cache_after_connector_kb,page_cache_after_cleanup_kb,free_kb_after_cleanup,scenario_dir" >"$SUMMARY_CSV"

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

extract_runtime_stat() {
  local file="$1"
  local key="$2"
  python3 - "$file" "$key" <<'PY'
import json
import sys

path, key = sys.argv[1], sys.argv[2]
value = ""
with open(path, "r", encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("msg") != "runtime stats":
            continue
        if key in payload:
            value = payload[key]
if value == "":
    raise SystemExit(1)
print(value)
PY
}

page_cache_kb() {
  awk '
    /^Cached:/  { cached=$2 }
    /^Buffers:/ { buffers=$2 }
    END { print cached + buffers }
  ' /proc/meminfo
}

collect_minio_stats() {
  local topic="$1"
  local json
  json="$(docker run --rm --network host --entrypoint /bin/sh minio/mc -c "mc alias set local http://127.0.0.1:9000 minioadmin minioadmin >/dev/null && mc du --recursive --json local/landing/source=kafka/topic=${topic} | tail -n 1")"
  python3 - "$json" <<'PY'
import json
import sys
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

cleanup_before_scenario() {
  stop_stack
  sleep 2
}

cleanup_after_scenario() {
  stop_stack
  free_kb
}

run_scenario() {
  local event_count="$1"
  local flush_workers="$2"
  local encode_workers="$3"
  local upload_workers="$4"

  local scenario_dir="$OUTPUT_ROOT/count=${event_count}/flush=${flush_workers}-encode=${encode_workers}-upload=${upload_workers}"
  local temp_dir="$scenario_dir/tmp"
  mkdir -p "$scenario_dir" "$temp_dir"

  echo
  echo "==> event_count=$event_count compression=$COMPRESSION flush=$flush_workers encode=$encode_workers upload=$upload_workers"

  cleanup_before_scenario
  local page_cache_before
  page_cache_before="$(page_cache_kb)"

  start_stack

  OUTPUT_DIR="$scenario_dir" \
  EVENT_COUNT="$event_count" \
  PAYLOAD_BYTES="$PAYLOAD_BYTES" \
  PARTITIONS="$PARTITIONS" \
  COMPRESSIONS="$COMPRESSION" \
  OUTPUT_FORMAT="avro" \
  RANDOM_SEED="$RANDOM_SEED" \
  PAYLOAD_MODE="$PAYLOAD_MODE" \
  BATCH_SIZE="$BATCH_SIZE" \
  REPORT_EVERY="$REPORT_EVERY" \
  KEEP_TOPICS=true \
  FLUSH_WORKERS="$flush_workers" \
  ENCODE_WORKERS="$encode_workers" \
  UPLOAD_WORKERS="$upload_workers" \
  PARTITION_QUEUE_SIZE="$PARTITION_QUEUE_SIZE" \
  BATCH_MAX_RECORDS="$BATCH_MAX_RECORDS" \
  BATCH_MAX_BYTES="$BATCH_MAX_BYTES" \
  BATCH_MAX_DURATION="$BATCH_MAX_DURATION" \
  RUN_TIMEOUT="$RUN_TIMEOUT" \
  TEMP_DIR="$temp_dir" \
  INCLUDE_HEADERS="$INCLUDE_HEADERS" \
  INCLUDE_KEY="$INCLUDE_KEY" \
  IDLE_POLL_TIMEOUT="$IDLE_POLL_TIMEOUT" \
  IDLE_POLL_COUNT="$IDLE_POLL_COUNT" \
  KAFKA_POLL_RECORDS="$KAFKA_POLL_RECORDS" \
  KAFKA_FETCH_MAX_BYTES="$KAFKA_FETCH_MAX_BYTES" \
  KAFKA_FETCH_MAX_PARTITION_BYTES="$KAFKA_FETCH_MAX_PARTITION_BYTES" \
  KAFKA_FETCH_MIN_BYTES="$KAFKA_FETCH_MIN_BYTES" \
  KAFKA_FETCH_MAX_WAIT="$KAFKA_FETCH_MAX_WAIT" \
  TIME_VERBOSE="$TIME_VERBOSE" \
  GODEBUG_VALUE="$GODEBUG_VALUE" \
  "$ROOT_DIR/scripts/bench/run-local-compression-matrix.sh"

  local page_cache_after_connector
  page_cache_after_connector="$(page_cache_kb)"

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

  local throughput_records_per_sec
  throughput_records_per_sec="$(python3 - "$event_count" "$connector_elapsed" <<'PY'
import sys
count = int(sys.argv[1])
elapsed = float(sys.argv[2])
print(f"{count / elapsed:.2f}")
PY
)"

  local lag
  lag="$(collect_lag "$consumer_group")"

  local minio_stats
  local objects
  local size_bytes
  minio_stats="$(collect_minio_stats "$topic")"
  objects="${minio_stats%%,*}"
  size_bytes="${minio_stats##*,}"

  local heap_alloc_bytes
  local heap_inuse_bytes
  local heap_sys_bytes
  local total_alloc_bytes
  local alloc_delta_bytes
  local alloc_rate_bytes_per_sec
  local gc_cycles
  local gc_pause_total_ns
  heap_alloc_bytes="$(extract_runtime_stat "$scenario_dir/${COMPRESSION}.connector.log" "heap_alloc_bytes")"
  heap_inuse_bytes="$(extract_runtime_stat "$scenario_dir/${COMPRESSION}.connector.log" "heap_inuse_bytes")"
  heap_sys_bytes="$(extract_runtime_stat "$scenario_dir/${COMPRESSION}.connector.log" "heap_sys_bytes")"
  total_alloc_bytes="$(extract_runtime_stat "$scenario_dir/${COMPRESSION}.connector.log" "total_alloc_bytes")"
  alloc_delta_bytes="$(extract_runtime_stat "$scenario_dir/${COMPRESSION}.connector.log" "alloc_delta_bytes")"
  alloc_rate_bytes_per_sec="$(extract_runtime_stat "$scenario_dir/${COMPRESSION}.connector.log" "alloc_rate_bytes_per_sec")"
  gc_cycles="$(extract_runtime_stat "$scenario_dir/${COMPRESSION}.connector.log" "gc_cycles")"
  gc_pause_total_ns="$(extract_runtime_stat "$scenario_dir/${COMPRESSION}.connector.log" "gc_pause_total_ns")"

  local free_after_cleanup
  free_after_cleanup="$(cleanup_after_scenario)"
  local page_cache_after_cleanup
  page_cache_after_cleanup="$(page_cache_kb)"

  echo "${event_count},${COMPRESSION},${flush_workers},${encode_workers},${upload_workers},${producer_elapsed},${producer_rss},${connector_elapsed},${connector_rss},${throughput_records_per_sec},${lag},${objects},${size_bytes},${heap_alloc_bytes},${heap_inuse_bytes},${heap_sys_bytes},${total_alloc_bytes},${alloc_delta_bytes},${alloc_rate_bytes_per_sec},${gc_cycles},${gc_pause_total_ns},${page_cache_before},${page_cache_after_connector},${page_cache_after_cleanup},${free_after_cleanup},${scenario_dir}" >>"$SUMMARY_CSV"
}

trap stop_stack EXIT

stop_stack

for event_count in $COUNTS; do
  for tuple in $WORKER_MATRIX; do
    IFS=: read -r flush_workers encode_workers upload_workers <<<"$tuple"
    run_scenario "$event_count" "$flush_workers" "$encode_workers" "$upload_workers"
  done
done

echo
echo "Completed Avro E2E matrix. Summary: $SUMMARY_CSV"
