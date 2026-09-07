#!/usr/bin/env bash
# CI/로컬(도커 필요) E2E — Plan 6c T10:
#   CH 24.8 컨테이너(CLICKHOUSE_SKIP_USER_SETUP=1) → DDL(단일노드 변환: token-usage dim_token_service/mart_tables
#   + Plan 6a raw_token_metrics/dim_token_metrics_service + 6c mart_metrics_tables + tests/e2e/ddl_test_dims.sql)
#   → seed_metrics.py(시나리오 A/B/C/D) → app.batch --date 2회(멱등) → 마커·CHECK 라인 grep
#   → verify_expected_results.sql(expect-empty 21검사, R5 — m1_no_gpu_cost_null 포함)
#   → tools/verify/run_invariants.py --sql invariants_metrics.sql
#   → no-metrics day(D-1) 배치 = SUCCESS coverage 0/3.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."     # mart/token-metrics

# 결정적 기대값 — 시드는 날짜에 의존하지 않지만(build_seed(date)) dim 유효기간(effective_from 2026-01-01,
# registry coverage_since 2026-08-26) 이후여야 하므로 고정 기본값을 쓴다(원형 mart E2E와 동일 원칙).
DATE_ARG="${1:-2026-09-03}"
NO_METRICS_DATE=$(date -d "${DATE_ARG} -1 day" +%F)
CH_PORT_HOST=18124

docker network create tokene2e-mart-metrics >/dev/null 2>&1 || true
trap 'docker rm -f ch-e2e-mart-metrics >/dev/null 2>&1 || true; docker network rm tokene2e-mart-metrics >/dev/null 2>&1 || true' EXIT

# CLICKHOUSE_SKIP_USER_SETUP=1: 비밀번호 미설정 시 default 유저의 네트워크 접근 차단(403) 회피 — 원형과 동일.
docker run -d --rm --name ch-e2e-mart-metrics --network tokene2e-mart-metrics -p "${CH_PORT_HOST}:8123" \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  clickhouse/clickhouse-server:24.8

for _ in $(seq 1 60); do
  curl -sf "http://127.0.0.1:${CH_PORT_HOST}/ping" >/dev/null && break
  sleep 1
done
curl -sf "http://127.0.0.1:${CH_PORT_HOST}/ping" >/dev/null || { echo "E2E FAILED: ClickHouse not reachable on ${CH_PORT_HOST}"; exit 1; }

# DDL 결합(DB 3개 프리펜드) — 읽기 계약 2파일(token-usage, 읽기만) + Plan 6a 2파일 + 6c mart + e2e dim 대역.
# 단일노드 변환 regex 3종은 원형 mart/token-usage/tests/e2e/run_e2e.sh와 동일. '--' 주석 줄은 분리 전에 제거해
# 주석 속 세미콜론이 문장 분리를 깨지 못하게 하고(ddl_test_dims.sql은 애초에 세미콜론 없는 주석만 쓴다),
# 문장 분리는 Plan 6b run_e2e.sh와 같은 split_statements(단일따옴표 문자열 안의 ';' 무시)로 한다 —
# 6a 테스트가 COMMENT 문자열의 ';'를 금지하지만 로더는 그 계약에 기대지 않는다.
python3 - <<'PY'
import re, pathlib, urllib.request, urllib.error


def split_statements(text):
    """';' 문장 분할 — 단일따옴표 문자열 안의 ';' 는 무시 (6a 컬럼 COMMENT 에 ';' 가 있다; '' 이스케이프는 토글 2회로 처리)."""
    out, buf, in_str = [], [], False
    for ch in text:
        if ch == "'":
            in_str = not in_str
        if ch == ";" and not in_str:
            out.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    out.append("".join(buf))
    return [s for s in out if s.strip()]


sql = "CREATE DATABASE IF NOT EXISTS fact;\n"
sql += "CREATE DATABASE IF NOT EXISTS gpu_data;\n"
sql += "CREATE DATABASE IF NOT EXISTS mart;\n"
for path in (
    "../../collectors/token-usage/ddl/company/dim_token_service.sql",
    "../../mart/token-usage/ddl/company/mart_tables.sql",
    "../../collectors/token-metrics/ddl/company/raw_token_metrics.sql",
    "../../collectors/token-metrics/ddl/company/dim_token_metrics_service.sql",
    "ddl/company/mart_metrics_tables.sql",
    "tests/e2e/ddl_test_dims.sql",
):
    sql += "\n" + pathlib.Path(path).read_text(encoding="utf-8")

sql = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
sql = re.sub(r"ON CLUSTER 'gpu-monitoring'", "", sql)
sql = re.sub(r"ENGINE = ReplicatedMergeTree\([^)]*\)", "ENGINE = MergeTree", sql, flags=re.S)
sql = re.sub(r"ENGINE = Distributed\('gpu-monitoring', '(\w+)', '(\w+)',[\s\S]*?\);",
             r"ENGINE = Distributed('default', '\1', '\2', rand());", sql)

applied = 0
for stmt in split_statements(sql):
    if stmt.strip():
        # 포트는 CH_PORT_HOST(bash 변수)와 동일 값을 리터럴로 고정 — heredoc은 export 안 된 셸 변수를 못 본다.
        req = urllib.request.Request("http://127.0.0.1:18124/", data=(stmt + ";").encode("utf-8"))
        try:
            urllib.request.urlopen(req).read()
        except urllib.error.HTTPError as e:
            print(f"DDL failed (HTTP {e.code}): {e.read().decode(errors='replace')}")
            print(f"statement: {stmt.strip()[:200]}")
            raise SystemExit(1)
        applied += 1
print(f"DDL applied (single-node transformed, statements={applied})")
PY

export CH_HOST=127.0.0.1 CH_PORT="${CH_PORT_HOST}" CH_CLUSTER=""
export CH_DB_FACT=fact CH_DB_DIM=gpu_data CH_DB_MART=mart

# 시드 — registry(A/B/C/D + A/B/C), fact gpu 6/serving 2/summary 2, token_usage 6/agg_service 3.
python3 tests/e2e/seed_metrics.py "${DATE_ARG}"

# 기대값(CH 불필요) — 마커 grep과 verify 치환에 사용.
declare -A EXP
while IFS='=' read -r k v; do EXP["$k"]="$v"; done < <(python3 tests/e2e/mart_expectations.py "${DATE_ARG}")
echo "expectations: m1_rows=${EXP[EXP_M1_ROWS]} m4_rows=${EXP[EXP_M4_ROWS]} coverage=${EXP[EXP_COVERAGE]}"

# 배치 2회 — 멱등성(DELETE → INSERT, insert_deduplicate=0) 검증. 마커·CHECK 라인은 2회차 출력으로 단정.
# R4(T3 review carry): 1회차 실행이 SQL_M1/SQL_M3/SQL_M4/SQL_M2를 실서버에 처음 파싱시키는 지점 —
# 벽시계를 재 informational 로그만 남긴다(임계값 없음; 파싱 실패는 run1 status 체크가 그대로 잡는다).
T0=$(date +%s)
RUN1=$(python3 -m app.batch --date "${DATE_ARG}" 2>&1) || true
echo "batch_wall_s=$(( $(date +%s) - T0 ))"
echo "$RUN1"
grep -qF "BATCH_RESULT status=SUCCESS module=mart-metrics" <<<"$RUN1" || { echo "E2E FAILED: run1 status != SUCCESS"; exit 1; }

RUN2=$(python3 -m app.batch --date "${DATE_ARG}" 2>&1) || true
echo "$RUN2"
grep -qF "BATCH_RESULT status=SUCCESS module=mart-metrics" <<<"$RUN2" || { echo "E2E FAILED: run2(재실행) status != SUCCESS"; exit 1; }
grep -qF "metrics_coverage=${EXP[EXP_COVERAGE]} " <<<"$RUN2" || { echo "E2E FAILED: coverage marker != ${EXP[EXP_COVERAGE]}"; exit 1; }
grep -qF 'missing_services="Mock Service C"' <<<"$RUN2" || { echo "E2E FAILED: missing_services marker != \"Mock Service C\""; exit 1; }
grep -qF "rows_mart=${EXP[EXP_M1_ROWS]} " <<<"$RUN2" || { echo "E2E FAILED: rows_mart marker != ${EXP[EXP_M1_ROWS]}"; exit 1; }
grep -qF "rows_share=${EXP[EXP_M4_ROWS]} " <<<"$RUN2" || { echo "E2E FAILED: rows_share marker != ${EXP[EXP_M4_ROWS]}"; exit 1; }
grep -qF "CHECK WARN metrics_missing severity=FAIL count=1" <<<"$RUN2" || { echo "E2E FAILED: metrics_missing CHECK line missing"; exit 1; }
grep -qF "CHECK WARN hours_over_count severity=FAIL count=1" <<<"$RUN2" || { echo "E2E FAILED: hours_over_count CHECK line missing"; exit 1; }
grep -qF "CHECK INFO manual_source severity=INFO count=1" <<<"$RUN2" || { echo "E2E FAILED: manual_source CHECK INFO line missing"; exit 1; }

# verify — {DATE} + {EXP_*} 8토큰 치환 후 expect-empty. -f 대신 HTTP 코드 캡처(서버 오류 본문 노출 — 원형과 동일 원칙).
sed -e "s/{DATE}/${DATE_ARG}/g" \
    -e "s/{EXP_M1_ROWS}/${EXP[EXP_M1_ROWS]}/g" \
    -e "s/{EXP_M1_QWEN_COST}/${EXP[EXP_M1_QWEN_COST]}/g" \
    -e "s/{EXP_M3_FAIL_ROWS}/${EXP[EXP_M3_FAIL_ROWS]}/g" \
    -e "s/{EXP_M3_WARN_ROWS}/${EXP[EXP_M3_WARN_ROWS]}/g" \
    -e "s/{EXP_M4_ROWS}/${EXP[EXP_M4_ROWS]}/g" \
    -e "s/{EXP_M4_QWEN_SUM}/${EXP[EXP_M4_QWEN_SUM]}/g" \
    -e "s/{EXP_M2_ROWS}/${EXP[EXP_M2_ROWS]}/g" \
    -e "s/{EXP_M2_IDLE_H100}/${EXP[EXP_M2_IDLE_H100]}/g" \
    tests/e2e/verify_expected_results.sql > /tmp/verify_query_mart_metrics.sql
if grep -q '{EXP_' /tmp/verify_query_mart_metrics.sql; then
  echo "E2E FAILED: unreplaced token in verify query:"; grep -n '{EXP_' /tmp/verify_query_mart_metrics.sql; exit 1
fi
VERIFY_HTTP=$(curl -s -o /tmp/verify_out_mart_metrics.tsv -w '%{http_code}' \
  --data-binary @/tmp/verify_query_mart_metrics.sql "http://127.0.0.1:${CH_PORT_HOST}/?default_format=TSV")
if [ "${VERIFY_HTTP}" != "200" ]; then
  echo "E2E VERIFY QUERY FAILED (HTTP ${VERIFY_HTTP}):"; cat /tmp/verify_out_mart_metrics.tsv; exit 1
fi
if [ -s /tmp/verify_out_mart_metrics.tsv ]; then
  echo "E2E VERIFY FAILED:"; cat /tmp/verify_out_mart_metrics.tsv; exit 1
fi
echo "verify_expected_results.sql: 21 checks empty (PASS)"

# 불변식(T9) — 같은 컨테이너의 fact/gpu_data/mart 기본 DB명, 읽기 전용 SELECT.
INV=$(python3 ../../tools/verify/run_invariants.py --sql ../../tools/verify/invariants_metrics.sql --date "${DATE_ARG}" 2>&1) || true
echo "$INV"
grep -qF "ALL INVARIANTS PASS (date=${DATE_ARG}, DBs=fact/gpu_data/mart, sql=invariants_metrics.sql)" <<<"$INV" || { echo "E2E FAILED: invariants_metrics"; exit 1; }

# no-metrics day(D-1): 앵커 0 + 토큰 0 → FAILURE 아님(설계 §6.1) — coverage 0/3, 3서비스 전부 missing.
RUN_EMPTY=$(python3 -m app.batch --date "${NO_METRICS_DATE}" 2>&1) || true
echo "$RUN_EMPTY"
grep -qF "BATCH_RESULT status=SUCCESS module=mart-metrics metrics_coverage=0/3" <<<"$RUN_EMPTY" || {
  echo "E2E FAILED: no-metrics day(${NO_METRICS_DATE}) status/coverage != SUCCESS 0/3"; exit 1; }
grep -qF 'missing_services="Mock Service A,Mock Service B,Mock Service C"' <<<"$RUN_EMPTY" || {
  echo "E2E FAILED: no-metrics day missing_services != A,B,C"; exit 1; }
grep -qF "CHECK WARN token_mart_absent date=${NO_METRICS_DATE}" <<<"$RUN_EMPTY" || {
  echo "E2E FAILED: no-metrics day token_mart_absent WARN missing"; exit 1; }

echo "E2E PASS (date=${DATE_ARG}, m1_rows=${EXP[EXP_M1_ROWS]}, coverage=${EXP[EXP_COVERAGE]})"
