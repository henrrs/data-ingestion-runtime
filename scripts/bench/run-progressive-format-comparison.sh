#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "run-progressive-format-comparison.sh foi convergido para o benchmark Avro-only."
echo "Encaminhando para scripts/bench/run-avro-e2e-matrix.sh"

exec "$ROOT_DIR/scripts/bench/run-avro-e2e-matrix.sh" "$@"
