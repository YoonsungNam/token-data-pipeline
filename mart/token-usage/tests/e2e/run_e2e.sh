#!/usr/bin/env bash
# CI/로컬(도커 필요) E2E: CH 컨테이너(CLICKHOUSE_SKIP_USER_SETUP=1) → DDL(단일노드 변환:
# collectors fact/dim_token_service + mart/view + 신규 ddl_test_dims.sql) → dim_token_service
# 4서비스 시드 → seed_fact.py(mock datagen 직접 호출 — mock 서버 불필요) → app.batch 2회(멱등)
# → verify_expected_results.sql(--expect-empty) → 마커 grep → 5월 고정일 배치 + 귀속 검증.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."     # mart/token-usage

# 결정적 기대값(E2E 제목의 "결정적") — dim_token_user_org 시드(2026-06-01/06-15 이관·퇴사) 이후로
# 고정한 날짜. 실제 벽시계(date -d yesterday)에 의존하면 CI 실행 시점에 따라 이관 전/후
# 경계가 흔들려 재현성이 깨지므로 collectors E2E와 달리 고정값을 기본으로 쓴다.
DATE_ARG="${1:-2026-07-10}"
MAY_DATE="2026-05-15"
SEED="token-mock-1"
USERS=50
ANON=10
MODELS="claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5"
CH_PORT_HOST=18123

docker network create tokene2e-mart >/dev/null 2>&1 || true
trap 'docker rm -f ch-e2e-mart >/dev/null 2>&1 || true; docker network rm tokene2e-mart >/dev/null 2>&1 || true' EXIT

# CLICKHOUSE_SKIP_USER_SETUP=1: 공식 이미지는 비밀번호 미설정 시 default 유저의 네트워크
# 접근을 차단(localhost 전용) — published port 경유(브리지 IP) 쿼리가 403이 됨 (collectors
# E2E와 동일 이슈, Plan 2a E2E 교훈).
docker run -d --rm --name ch-e2e-mart --network tokene2e-mart -p "${CH_PORT_HOST}:8123" \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  clickhouse/clickhouse-server:24.8

for _ in $(seq 1 60); do
  curl -sf "http://127.0.0.1:${CH_PORT_HOST}/ping" >/dev/null && break
  sleep 1
done

# DDL: collectors(raw_token_usage/dim_token_service, 그대로) + mart(mart_tables/
# view_token_usage — accounts.sql 제외, admin 수동 GRANT라 E2E 대상 아님) + 신규
# ddl_test_dims.sql(dim_token_user_org/dim_token_model 단일노드 정본) — 전부 단일노드 변환.
# collectors run_e2e.sh의 변환 파이썬 블록을 확장(DB 3개 프리펜드 + 5개 파일 결합).
python3 - <<'PY'
import re, pathlib, urllib.request

sql = "CREATE DATABASE IF NOT EXISTS fact;\n"
sql += "CREATE DATABASE IF NOT EXISTS gpu_data;\n"
sql += "CREATE DATABASE IF NOT EXISTS mart;\n"

sql += pathlib.Path("../../collectors/token-usage/ddl/company/raw_token_usage.sql").read_text()
sql += "\n" + pathlib.Path("../../collectors/token-usage/ddl/company/dim_token_service.sql").read_text()
sql += "\n" + pathlib.Path("ddl/company/mart_tables.sql").read_text()
sql += "\n" + pathlib.Path("ddl/company/view_token_usage.sql").read_text()
sql += "\n" + pathlib.Path("tests/e2e/ddl_test_dims.sql").read_text()

# 단일노드 변환 — Replicated/ON CLUSTER 제거, Distributed 재작성(중첩 괄호 안전 regex).
# ddl_test_dims.sql은 이미 단일노드 MergeTree라 이 3개 패턴에 매칭될 게 없어 그대로 통과.
sql = re.sub(r"ON CLUSTER 'gpu-monitoring'", "", sql)
sql = re.sub(r"ENGINE = ReplicatedMergeTree\([^)]*\)", "ENGINE = MergeTree", sql, flags=re.S)
sql = re.sub(r"ENGINE = Distributed\('gpu-monitoring', '(\w+)', '(\w+)',[\s\S]*?\);",
             r"ENGINE = Distributed('default', '\1', '\2', rand());", sql)

for stmt in sql.split(";"):
    if stmt.strip():
        # CH_PORT_HOST(위 bash 변수)와 동일 포트를 리터럴로 고정 — heredoc 프로세스는
        # export되지 않은 셸 변수를 보지 못하므로 os.environ 경유 대신 직접 명시.
        req = urllib.request.Request("http://127.0.0.1:18123/", data=(stmt + ";").encode())
        try:
            urllib.request.urlopen(req).read()
        except urllib.error.HTTPError as e:
            print(f"DDL failed (HTTP {e.code}): {e.read().decode(errors='replace')}")
            print(f"statement: {stmt.strip()[:200]}")
            raise SystemExit(1)
print("DDL applied (single-node transformed)")
PY

# dim_token_service: 4서비스 전부 enabled=1 (A/B/C/D — D 포함, coverage 분모=4)
curl -sf --data-binary @- "http://127.0.0.1:${CH_PORT_HOST}/" <<'SQL'
INSERT INTO gpu_data.dim_token_service_dist
    (service_group, service, base_url, enabled, source_type, note, updated_at) VALUES
('Mock Group','Mock Service A','http://mock-a.invalid',1,'usage-api-v1','','2026-01-01 00:00:00'),
('Mock Group','Mock Service B','http://mock-b.invalid',1,'usage-api-v1','','2026-01-01 00:00:00'),
('Mock Group','Mock Service C','http://mock-c.invalid',1,'usage-api-v1','','2026-01-01 00:00:00'),
('Mock Group','Mock Service D','http://mock-d.invalid',1,'usage-api-v1','','2026-01-01 00:00:00');
SQL

export CH_HOST=127.0.0.1 CH_PORT="${CH_PORT_HOST}" CH_CLUSTER=""

# fact 시드 — mock 서버 불필요(datagen 직접 호출). 메인 날짜 + 5월 고정 날짜.
python3 tests/e2e/seed_fact.py "${DATE_ARG}" "${MAY_DATE}"

# 수집 2회 — 멱등성(delete-then-insert, insert_deduplicate=0) 검증.
# batch_time = target_date 다음날 04:00 KST(mart cron 창).
NEXT_DAY=$(date -d "${DATE_ARG} +1 day" +%F)
RUN1=$(python3 -m app.batch "${NEXT_DAY}T04:00:00+09:00" 2>&1) || true
echo "$RUN1"
grep -qF "BATCH_RESULT status=SUCCESS" <<<"$RUN1" || { echo "E2E FAILED: run1 status != SUCCESS"; exit 1; }

RUN2=$(python3 -m app.batch "${NEXT_DAY}T04:00:00+09:00" 2>&1) || true
echo "$RUN2"
grep -qF "BATCH_RESULT status=SUCCESS" <<<"$RUN2" || { echo "E2E FAILED: run2(재실행) status != SUCCESS"; exit 1; }

# 마커 검증 — coverage=3/4(A/C/D summary 존재, B 누락), missing_services는 쌍따옴표
# 포함 콤마 목록이라 grep -F로 리터럴 매칭(서비스명 공백 대응).
grep -qF 'coverage=3/4' <<<"$RUN2" || { echo "E2E FAILED: coverage marker != 3/4"; exit 1; }
grep -qF 'missing_services="Mock Service B"' <<<"$RUN2" || {
  echo "E2E FAILED: missing_services marker != \"Mock Service B\""; exit 1; }

# 5월 고정 날짜 배치 — user-0005 조직 이동(2026-06-01) 발생일 기준 귀속 검증용.
RUN_MAY=$(python3 -m app.batch --from "${MAY_DATE}" --to "${MAY_DATE}" 2>&1) || true
echo "$RUN_MAY"
grep -qF "BATCH_RESULT status=SUCCESS" <<<"$RUN_MAY" || { echo "E2E FAILED: 5월 배치 status != SUCCESS"; exit 1; }

# 기대값 산출 — 메인 날짜(num_services=3, A+B+C) / 5월(num_services=1, A만 시드).
declare -A EXP
while IFS='=' read -r k v; do EXP["main_$k"]="$v"; done < <(
  python3 tests/e2e/mart_expectations.py "${DATE_ARG}" "${SEED}" "${USERS}" "${ANON}" "${MODELS}" 3)
while IFS='=' read -r k v; do EXP["may_$k"]="$v"; done < <(
  python3 tests/e2e/mart_expectations.py "${MAY_DATE}" "${SEED}" "${USERS}" "${ANON}" "${MODELS}" 1)

sed -e "s/{DATE}/${DATE_ARG}/g" \
    -e "s/{EXP_DETAIL_ROWS}/${EXP[main_detail_rows]}/g" \
    -e "s/{EXP_DETAIL_TOTAL_INPUT}/${EXP[main_detail_total_input]}/g" \
    -e "s/{EXP_ORG_X}/${EXP[main_org_x_total]}/g" \
    -e "s/{EXP_ORG_Y}/${EXP[main_org_y_total]}/g" \
    -e "s/{EXP_ORG_Z}/${EXP[main_org_z_total]}/g" \
    -e "s/{EXP_UNKNOWN_ROWS}/${EXP[main_unknown_rows]}/g" \
    -e "s/{EXP_HAIKU_NULL_ROWS}/${EXP[main_haiku_null_rows]}/g" \
    -e "s/{EXP_UNKNOWN_MODEL_ROWS}/${EXP[main_unknown_model_rows]}/g" \
    -e "s/{EXP_COST_SUM}/${EXP[main_cost_sum]}/g" \
    -e "s/{EXP_MAIN_U5_ROWS}/${EXP[main_user5_rows]}/g" \
    -e "s/{EXP_MAY_U5_ROWS}/${EXP[may_user5_rows]}/g" \
    -e "s/{EXP_ANON_HANDLE_ROWS}/${EXP[main_anon_handle_rows]}/g" \
    tests/e2e/verify_expected_results.sql > /tmp/verify_query_mart.sql
# -f 대신 HTTP 코드 캡처 — 서버 오류 본문을 그대로 노출 (진단 가능성, Plan 2a DDL 진단과 동일 원칙)
VERIFY_HTTP=$(curl -s -o /tmp/verify_out_mart.tsv -w '%{http_code}' \
  --data-binary @/tmp/verify_query_mart.sql "http://127.0.0.1:${CH_PORT_HOST}/?default_format=TSV")
if [ "${VERIFY_HTTP}" != "200" ]; then
  echo "E2E VERIFY QUERY FAILED (HTTP ${VERIFY_HTTP}):"; cat /tmp/verify_out_mart.tsv; exit 1
fi
if [ -s /tmp/verify_out_mart.tsv ]; then
  echo "E2E VERIFY FAILED:"; cat /tmp/verify_out_mart.tsv; exit 1
fi

echo "E2E PASS (date=${DATE_ARG}, may_date=${MAY_DATE}, detail_rows=${EXP[main_detail_rows]})"
