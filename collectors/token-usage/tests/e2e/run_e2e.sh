#!/usr/bin/env bash
# CI/로컬(도커 필요) E2E: CH 컨테이너 + mock-provider 컨테이너 → DDL(단일노드 변환)
# → 수집 2회(멱등성) → verify --expect-empty → 시나리오 케이스(identity drift WARN)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."     # collectors/token-usage

DATE_ARG="${1:-$(date -d "yesterday" +%F)}"
SEED="e2e-seed-1"

docker network create tokene2e 2>/dev/null || true
trap 'docker rm -f ch-e2e mock-e2e >/dev/null 2>&1 || true; docker network rm tokene2e >/dev/null 2>&1 || true' EXIT

docker run -d --rm --name ch-e2e --network tokene2e -p 18123:8123 \
  clickhouse/clickhouse-server:24.8
docker run -d --rm --name mock-e2e --network tokene2e -p 18000:8000 \
  -e MOCK_SERVICE_GROUP="Mock Group" -e MOCK_SERVICE="Mock Service A" \
  -e MOCK_SEED="${SEED}" -e MOCK_USERS=30 -e MOCK_ANON_USERS=5 \
  token-mock-provider:e2e

for _ in $(seq 1 60); do
  curl -sf http://127.0.0.1:18123/ping >/dev/null && \
  curl -sf http://127.0.0.1:18000/healthz >/dev/null && break
  sleep 1
done

# DDL: 초안(company)을 단일노드용으로 변환 — Replicated 제거, ON CLUSTER 제거, dist→local 뷰 없이
python3 - <<'PY'
import re, pathlib, urllib.request

sql = pathlib.Path("ddl/company/raw_token_usage.sql").read_text()
sql += "\nCREATE DATABASE IF NOT EXISTS gpu_data;\n"

# dim ddl 파일명: PR #3 결정 반영 후 dim_token_service.sql로 개명됨 —
# 신명 파일이 없으면(초안 병합 전) 구명 파일로 폴백
dim_path = pathlib.Path("ddl/company/dim_token_service.sql")
if not dim_path.exists():
    dim_path = pathlib.Path("ddl/company/dim_service.sql")  # PR #3 결정 반영 전 초안 호환
sql += dim_path.read_text()

sql = re.sub(r"ON CLUSTER 'gpu-monitoring'", "", sql)
sql = re.sub(r"ENGINE = ReplicatedMergeTree\([^)]*\)", "ENGINE = MergeTree", sql, flags=re.S)
sql = re.sub(r"ENGINE = Distributed\('gpu-monitoring', '(\w+)', '(\w+)',[\s\S]*?\);",
             r"ENGINE = Distributed('default', '\1', '\2', rand());", sql)

# PR #3 결정 반영 전 초안 호환: 파일 내용이 구명(token_fact DB / dim_service_local·dist
# 테이블)을 쓰고 있어도 신명(fact / dim_token_service_local·dist)으로 치환해 생성
sql = re.sub(r"\btoken_fact\b", "fact", sql)
sql = re.sub(r"\bdim_service_(local|dist)\b", r"dim_token_service_\1", sql)

for stmt in sql.split(";"):
    if stmt.strip():
        req = urllib.request.Request("http://127.0.0.1:18123/", data=(stmt + ";").encode())
        urllib.request.urlopen(req).read()
print("DDL applied (single-node transformed)")
PY

export CH_HOST=127.0.0.1 CH_PORT=18123 CH_CLUSTER="" VM_PUSH_URL=""
export ENDPOINTS_FILE=tests/e2e/endpoints.e2e.yaml
cat > tests/e2e/endpoints.e2e.yaml <<EOF
services:
  - serviceGroup: "Mock Group"
    service: "Mock Service A"
    baseUrl: "http://127.0.0.1:18000"
    enabled: true
EOF

# 수집 2회 — 멱등성(delete-then-insert) 검증
# batch_time = target_date 다음날 02:00 (target_date = batch_time − 1일, §5.1)
NEXT_DAY=$(date -d "${DATE_ARG} +1 day" +%F)
python3 -m app.main "${NEXT_DAY}T02:00:00+09:00"
python3 -m app.main "${NEXT_DAY}T02:00:00+09:00"

read -r EXP <<<"$(python3 tests/e2e/ci_expectations.py "${DATE_ARG}" "${SEED}" 30 5 \
  "claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5")"
EXP_ROWS=$(sed -E 's/.*rows=([0-9]+).*/\1/' <<<"$EXP")
EXP_INPUT=$(sed -E 's/.*input_sum=([0-9]+).*/\1/' <<<"$EXP")
EXP_REQ=$(sed -E 's/.*requests_sum=([0-9]+).*/\1/' <<<"$EXP")

sed -e "s/{DATE}/${DATE_ARG}/g" -e "s/{SERVICE}/Mock Service A/g" \
    -e "s/{EXP_ROWS}/${EXP_ROWS}/g" -e "s/{EXP_INPUT}/${EXP_INPUT}/g" \
    -e "s/{EXP_REQ}/${EXP_REQ}/g" tests/e2e/verify_expected_results.sql \
  | curl -sf --data-binary @- "http://127.0.0.1:18123/?default_format=TSV" > /tmp/verify_out.tsv
if [ -s /tmp/verify_out.tsv ]; then
  echo "E2E VERIFY FAILED:"; cat /tmp/verify_out.tsv; exit 1
fi

# 시나리오: 서비스명 드리프트 → CHECK WARN + 적재는 진행 (§5.0)
curl -sf -X POST http://127.0.0.1:18000/__mock/scenario \
  -H 'content-type: application/json' -d '{"name_drift": " "}' >/dev/null
OUT=$(python3 -m app.main "${NEXT_DAY}T02:00:00+09:00" 2>&1) || true
grep -q "identity_drift" <<<"$OUT" || { echo "identity drift WARN missing"; exit 1; }
grep -q "BATCH_RESULT status=SUCCESS" <<<"$OUT" || { echo "drift must not fail batch"; exit 1; }

echo "E2E PASS (date=${DATE_ARG}, rows=${EXP_ROWS})"
