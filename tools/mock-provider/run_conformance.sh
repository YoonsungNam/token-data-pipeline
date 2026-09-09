#!/usr/bin/env bash
# mock-provider를 기동하고 vendored conformance_check로 계약 준수를 검증한다.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PORT="${PORT:-8000}"
DATE_ARG="${1:-$(TZ=Asia/Seoul date -d "yesterday" +%F)}"
PYTHON="${PYTHON:-python3}"
export MOCK_USERS="${MOCK_USERS:-600}"

"${PYTHON}" -m uvicorn app.main:app --host 127.0.0.1 --port "${PORT}" &
UVICORN_PID=$!
trap 'kill "${UVICORN_PID}" 2>/dev/null || true' EXIT

for _ in $(seq 1 50); do
  if curl -sf "http://127.0.0.1:${PORT}/healthz" > /dev/null; then break; fi
  sleep 0.2
done

"${PYTHON}" contract/tests/conformance_check.py --base-url "http://127.0.0.1:${PORT}" --date "${DATE_ARG}"
echo "CONFORMANCE PASS (date=${DATE_ARG})"

# /v1/metrics — token-metric-api @6a552d2 자가 검사 (같은 uvicorn 프로세스; FAIL이 있으면 exit 1)
"${PYTHON}" contract/tests/check_metrics_api.py --base-url "http://127.0.0.1:${PORT}" --date "${DATE_ARG}"
echo "METRICS CONFORMANCE PASS (date=${DATE_ARG})"
