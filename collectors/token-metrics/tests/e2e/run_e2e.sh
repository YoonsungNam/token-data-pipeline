#!/usr/bin/env bash
# CI/로컬(도커 필요) E2E — collectors/token-metrics (Plan 6b T11 · 설계 §7.3, §4.0 뮤테이션 장부 실측)
# CH 24.8 + mock-provider 컨테이너 → 6a DDL 2파일 단일노드 변환 + 토큰 레지스트리 최소 twin
# → 정기 2회(2회차 already_loaded · system.mutations 0) → verify --expect-empty
# → --replace 재수집(mutations 3 · 감사 1) → 시나리오 A(hours_over WARN)/B(gpu 빈 배열)/C(409 not_ready)
# → manual-v0 1회(MANUAL_INPUT · manual-v0 앵커) + 재호출 already_loaded
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."     # collectors/token-metrics

DATE_ARG="${1:-$(TZ=Asia/Seoul date -d "yesterday" +%F)}"  # KST 의 어제 (mock 게이트가 KST) — 러너 로컬 TZ 와 무관하게 고정
NEXT_DAY=$(date -d "${DATE_ARG} +1 day" +%F)   # 정기 batch_time 의 날짜 (target_date = batch_time − 1일 = DATE_ARG)
DATE2=$(date -d "${DATE_ARG} -1 day" +%F)      # 시나리오 C · manual 용 두 번째 날짜 (MOCK_METRICS_RETENTION_DAYS=14 안)
SEED="e2e-seed-1"
SVC="Mock Service A"
CH_URL="http://127.0.0.1:18125/"
MOCK_URL="http://127.0.0.1:18001"
TMP=tests/e2e/.tmp                              # gitignore — sed 치환 CSV · verify 출력

chq() {            # chq <sql> — CH HTTP 스칼라 질의 (TSV 1값, 개행은 $(...) 가 제거)
  curl -s --fail-with-body "${CH_URL}" --data-binary "$1"
}
expect_eq() {      # expect_eq <what> <got> <want>
  if [ "$2" != "$3" ]; then echo "$1: expected $3, got $2"; exit 1; fi
}
run_collector() {  # run_collector <args...> — OUT / RC 설정 (set -e 아래에서 비정상 종료도 값으로 받는다)
  set +e
  OUT=$(python3 -m app.main "$@" 2>&1)
  RC=$?
  set -e
}
need_line() {      # need_line <what> <fixed-string> — $OUT 에 문자열이 없으면 실패 + 전체 출력 덤프
  grep -qF -- "$2" <<<"$OUT" || { echo "$1 missing"; echo "$OUT"; exit 1; }
}
scenario() {       # scenario '<json>' — mock 시나리오 키별 병합 (T1 set_scenario; 나머지 키는 유지)
  curl -s -f -X POST "${MOCK_URL}/__mock/scenario" -H 'content-type: application/json' -d "$1" >/dev/null
}

# (1) 컨테이너 — 기존 token-usage e2e 와 포트·컨테이너·네트워크 이름이 다르다 (병렬 실행 충돌 없음)
docker network create tokenmetricse2e 2>/dev/null || true
trap 'docker rm -f ch-e2e-metrics mock-e2e-metrics >/dev/null 2>&1 || true; docker network rm tokenmetricse2e >/dev/null 2>&1 || true' EXIT

# 이전 실행이 SIGKILL 등으로 trap 을 못 타면 컨테이너 이름이 남는다 — 새로 띄우기 전에 정리
docker rm -f ch-e2e-metrics mock-e2e-metrics >/dev/null 2>&1 || true

# CLICKHOUSE_SKIP_USER_SETUP=1: 공식 이미지는 비밀번호 미설정 시 default 유저의
# 네트워크 접근을 차단(localhost 전용) — published port 경유(브리지 IP) 쿼리가 403이 됨
docker run -d --rm --name ch-e2e-metrics --network tokenmetricse2e -p 18125:8123 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  clickhouse/clickhouse-server:24.8
docker run -d --rm --name mock-e2e-metrics --network tokenmetricse2e -p 18001:8000 \
  -e MOCK_SERVICE_GROUP="Mock Group" -e MOCK_SERVICE="${SVC}" \
  -e MOCK_SEED="${SEED}" -e MOCK_METRICS_RETENTION_DAYS=14 \
  token-mock-provider:e2e

for _ in $(seq 1 60); do
  curl -s -f http://127.0.0.1:18125/ping >/dev/null && \
  curl -s -f "${MOCK_URL}/healthz" >/dev/null && break
  sleep 1
done
curl -s -f http://127.0.0.1:18125/ping >/dev/null || { echo "clickhouse not healthy after 60s"; exit 1; }
curl -s -f "${MOCK_URL}/healthz" >/dev/null || { echo "mock-provider not healthy after 60s"; exit 1; }

# (2) DDL: 6a 초안(company)을 단일노드용으로 변환 — ON CLUSTER 제거, Replicated → MergeTree,
#     Distributed('gpu-monitoring', …) → Distributed('default', …, rand()); dist→local 뷰 없이.
#     tests/e2e/ddl_test_dims.sql(토큰 레지스트리 최소 twin)은 변환 없이 이어 붙인다.
python3 - <<'PY'
import pathlib, re, urllib.error, urllib.request

CH = "http://127.0.0.1:18125/"
# 단일노드 E2E 는 admin 수동 절차(accounts.sql)가 없으므로 이 스크립트가 fact DB 를 대신 생성
sql = "CREATE DATABASE IF NOT EXISTS fact;\nCREATE DATABASE IF NOT EXISTS gpu_data;\n"
sql += pathlib.Path("ddl/company/raw_token_metrics.sql").read_text(encoding="utf-8")
sql += "\n" + pathlib.Path("ddl/company/dim_token_metrics_service.sql").read_text(encoding="utf-8")

# 6a DDL 머리말 주석에 ';' 가 있는 줄이 있어 문장 분할 전에 주석 줄('--' 로 시작)을 제거
sql = re.sub(r"^[ \t]*--.*$", "", sql, flags=re.M)
sql = re.sub(r"ON CLUSTER 'gpu-monitoring'", "", sql)
sql = re.sub(r"ENGINE = ReplicatedMergeTree\([^)]*\)", "ENGINE = MergeTree", sql, flags=re.S)
sql = re.sub(r"ENGINE = Distributed\('gpu-monitoring', '(\w+)', '(\w+)',[\s\S]*?\);",
             r"ENGINE = Distributed('default', '\1', '\2', rand());", sql)
if "gpu-monitoring" in sql or "Replicated" in sql:
    print("single-node transform incomplete (gpu-monitoring / Replicated still present)")
    raise SystemExit(1)

dims = pathlib.Path("tests/e2e/ddl_test_dims.sql").read_text(encoding="utf-8")
sql += "\n" + re.sub(r"^[ \t]*--.*$", "", dims, flags=re.M)

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

n = 0
for stmt in split_statements(sql):
    req = urllib.request.Request(CH, data=(stmt.strip() + ";").encode("utf-8"))
    try:
        urllib.request.urlopen(req).read()
    except urllib.error.HTTPError as e:
        print(f"DDL failed (HTTP {e.code}): {e.read().decode(errors='replace')}")
        print(f"statement: {stmt.strip()[:200]}")
        raise SystemExit(1)
    n += 1
print(f"DDL applied (single-node transformed, {n} statements)")
PY
expect_eq "preflight dim_token_service twin" "$(chq 'SELECT count() FROM gpu_data.dim_token_service_dist')" "1"
expect_eq "fact raw_token_metrics tables" \
  "$(chq "SELECT count() FROM system.tables WHERE database = 'fact' AND name LIKE 'raw_token_metrics_%'")" "6"
expect_eq "fact audit tables" \
  "$(chq "SELECT count() FROM system.tables WHERE database = 'fact' AND name LIKE 'collect_audit_metrics_1d_%'")" "2"
expect_eq "gpu_data metrics registry tables" \
  "$(chq "SELECT count() FROM system.tables WHERE database = 'gpu_data' AND name LIKE 'dim_token_metrics_service_%'")" "2"

export CH_HOST=127.0.0.1 CH_PORT=18125 CH_USER=default CH_PASSWORD= CH_CLUSTER=""
export ENDPOINTS_FILE=tests/e2e/endpoints.e2e.yaml
cat > tests/e2e/endpoints.e2e.yaml <<EOF
services:
  - serviceGroup: "Mock Group"
    service: "${SVC}"
    baseUrl: "${MOCK_URL}"
    enabled: true
    apiSince: "2026-01-01"        # 정기 게이트(before_since) 통과 — T2 기본값 2026-09-09 보다 과거
    coverageSince: "2026-01-01"
EOF
mkdir -p "${TMP}"

# 기대치 — mock datagen 결정성 (같은 seed·date·models); 일반 대입이라야 set -e 가 실패를 잡는다
# (여기·서브셸 없는 <<< here-string 대입은 실패해도 무시된다)
EXP=$(python3 tests/e2e/ci_expectations.py "${DATE_ARG}" "${SEED}" \
  "claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5")
EXP_GPU_ROWS=$(sed -E 's/.*rows_gpu=([0-9]+).*/\1/' <<<"$EXP")
EXP_SERVING_ROWS=$(sed -E 's/.*rows_serving=([0-9]+).*/\1/' <<<"$EXP")
EXP_GPU_HOURS=$(sed -E 's/.*gpu_hours_sum=([0-9.]+).*/\1/' <<<"$EXP")
EXP_ROWS=$((EXP_GPU_ROWS + EXP_SERVING_ROWS))
expect_eq "expected rows (5 gpu + 9 serving)" "${EXP_ROWS}" "14"

MUT_Q="SELECT count() FROM system.mutations WHERE database IN ('fact', 'gpu_data')"
MUT_FACT_Q="SELECT count() FROM system.mutations WHERE database = 'fact'"
WHERE_D1="WHERE date = '${DATE_ARG}' AND service = '${SVC}'"
WHERE_D2="WHERE date = '${DATE2}' AND service = '${SVC}'"

# (3) 정기 1회 — batch_time = DATE_ARG 다음날 02:05 KST → target_date = DATE_ARG, slot=02(비최종)
run_collector "${NEXT_DAY}T02:05:00+09:00"
expect_eq "regular run 1 exit code" "${RC}" "0"
need_line "regular run 1 SERVICE_RESULT" "SERVICE_RESULT status=SUCCESS module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=${EXP_ROWS} pages=1 warn=0 rejected=0"
need_line "regular run 1 BATCH_RESULT" "BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 services_failed=0 services_skipped=0 rows=${EXP_ROWS} "
need_line "regular run 1 slot/final" "slot=02 final=0"
expect_eq "BATCH_RESULT lines in run 1" "$(grep -c '^BATCH_RESULT ' <<<"$OUT")" "1"

# (4) 정기 2회 — 앵커 존재 → already_loaded (fetch 없음) · 뮤테이션 0 (§4.0 정기 = 0; 레지스트리 diff 도 변경 없음)
run_collector "${NEXT_DAY}T02:05:00+09:00"
expect_eq "regular run 2 exit code" "${RC}" "0"
need_line "regular run 2 already_loaded" "SERVICE_RESULT status=SKIPPED module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=already_loaded"
need_line "regular run 2 BATCH_RESULT" "BATCH_RESULT status=SUCCESS module=token-metrics services_ok=0 services_failed=0 services_skipped=1 rows=0 "
expect_eq "mutations after 2 regular runs (fact + gpu_data)" "$(chq "${MUT_Q}")" "0"

# (5) verify --expect-empty — 정기 2회 직후 상태 (행수·합·앵커·감사 0·레지스트리·flags 빈 배열)
sed -e "s/{DATE}/${DATE_ARG}/g" -e "s/{SERVICE}/${SVC}/g" \
    -e "s/{EXP_GPU_ROWS}/${EXP_GPU_ROWS}/g" -e "s/{EXP_SERVING_ROWS}/${EXP_SERVING_ROWS}/g" \
    -e "s/{EXP_GPU_HOURS}/${EXP_GPU_HOURS}/g" tests/e2e/verify_expected_results.sql \
  | curl -s --fail-with-body --data-binary @- "${CH_URL}?default_format=TSV" > "${TMP}/verify_out.tsv" \
  || { echo "E2E VERIFY QUERY FAILED:"; cat "${TMP}/verify_out.tsv"; exit 1; }
if [ -s "${TMP}/verify_out.tsv" ]; then
  echo "E2E VERIFY FAILED:"; cat "${TMP}/verify_out.tsv"; exit 1
fi

# (6) --replace 재수집 — 앵커 존재 → 감사 INSERT 1 + DELETE 3(summary·gpu·serving _local) + INSERT (§5.4 · §4.0 날짜당 ≤3)
run_collector --from "${DATE_ARG}" --to "${DATE_ARG}" --replace
expect_eq "replace run exit code" "${RC}" "0"
need_line "replace SERVICE_RESULT" "SERVICE_RESULT status=SUCCESS module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=${EXP_ROWS} pages=1 warn=0 rejected=0"
need_line "replace BATCH_RESULT" "BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 services_failed=0 services_skipped=0 rows=${EXP_ROWS} "
expect_eq "fact mutations after --replace" "$(chq "${MUT_FACT_Q}")" "3"
expect_eq "gpu_data mutations after --replace (rerun never syncs registry)" \
  "$(chq "SELECT count() FROM system.mutations WHERE database = 'gpu_data'")" "0"
expect_eq "audit rows after --replace" "$(chq "SELECT count() FROM fact.collect_audit_metrics_1d_dist ${WHERE_D1}")" "1"
expect_eq "audit prev_source_type" \
  "$(chq "SELECT prev_source_type FROM fact.collect_audit_metrics_1d_dist ${WHERE_D1}")" "metrics-api-v1"
expect_eq "audit prev_gpu_rows" "$(chq "SELECT prev_gpu_rows FROM fact.collect_audit_metrics_1d_dist ${WHERE_D1}")" "${EXP_GPU_ROWS}"
expect_eq "summary anchor still once after --replace" \
  "$(chq "SELECT count() FROM fact.raw_token_metrics_summary_1d_dist ${WHERE_D1}")" "1"
expect_eq "gpu rows after --replace" "$(chq "SELECT count() FROM fact.raw_token_metrics_gpu_1d_dist ${WHERE_D1}")" "${EXP_GPU_ROWS}"
expect_eq "serving rows after --replace" \
  "$(chq "SELECT count() FROM fact.raw_token_metrics_serving_1d_dist ${WHERE_D1}")" "${EXP_SERVING_ROWS}"

# (7) 시나리오 A — gpuHours > gpuCount×24 → CHECK WARN hours_over_count=1, 행은 flags 로 적재·SUCCESS (§5.3 계층 2)
scenario '{"metrics_gpu_hours_over": 1}'
run_collector --from "${DATE_ARG}" --to "${DATE_ARG}" --replace
expect_eq "scenario A exit code" "${RC}" "0"
need_line "scenario A CHECK WARN" "CHECK WARN service=${SVC} hours_over_count=1"
need_line "scenario A SERVICE_RESULT" "SERVICE_RESULT status=SUCCESS module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=${EXP_ROWS} pages=1 warn=1 rejected=0"
need_line "scenario A BATCH_RESULT" "BATCH_RESULT status=SUCCESS module=token-metrics"
expect_eq "scenario A flagged gpu rows" \
  "$(chq "SELECT count() FROM fact.raw_token_metrics_gpu_1d_dist ${WHERE_D1} AND has(flags, 'hours_over_count')")" "1"

# 시나리오 B — gpu 빈 배열 + serving 행 = 케이스 E → SUCCESS rows=serving 만, summary gpu_rows=0 (§5.2 표 200 행)
scenario '{"metrics_gpu_hours_over": 0, "metrics_empty_gpu": 1}'
run_collector --from "${DATE_ARG}" --to "${DATE_ARG}" --replace
expect_eq "scenario B exit code" "${RC}" "0"
need_line "scenario B SERVICE_RESULT" "SERVICE_RESULT status=SUCCESS module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=${EXP_SERVING_ROWS} "
need_line "scenario B BATCH_RESULT" "BATCH_RESULT status=SUCCESS module=token-metrics"
expect_eq "scenario B summary gpu_rows" "$(chq "SELECT gpu_rows FROM fact.raw_token_metrics_summary_1d_dist ${WHERE_D1}")" "0"
expect_eq "scenario B gpu rows" "$(chq "SELECT count() FROM fact.raw_token_metrics_gpu_1d_dist ${WHERE_D1}")" "0"
expect_eq "scenario B serving rows" \
  "$(chq "SELECT count() FROM fact.raw_token_metrics_serving_1d_dist ${WHERE_D1}")" "${EXP_SERVING_ROWS}"
expect_eq "fact mutations after 3 replaces (3 per date per replace)" "$(chq "${MUT_FACT_Q}")" "9"
expect_eq "audit rows after 3 replaces (append-only)" \
  "$(chq "SELECT count() FROM fact.collect_audit_metrics_1d_dist ${WHERE_D1}")" "3"

# 시나리오 C — 409 data_not_ready: 정기 호출은 앵커 존재로 already_loaded 가 먼저이므로 다른 날짜(DATE2) rerun.
#   rerun 모드 규칙(§5.2): Retry-After 뒤 1회 재방문 → 재차 409 = FAILURE reason=not_ready, exit 1
scenario '{"metrics_empty_gpu": 0, "not_ready_until_uptime_s": 100000, "retry_after_s": 1}'
run_collector --from "${DATE2}" --to "${DATE2}"
expect_eq "scenario C exit code" "${RC}" "1"
need_line "scenario C SERVICE_RESULT" "SERVICE_RESULT status=FAILURE module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=not_ready"
need_line "scenario C BATCH_RESULT" "BATCH_RESULT status=FAILURE module=token-metrics services_ok=0 services_failed=1 services_skipped=0 rows=0 "
expect_eq "scenario C loaded nothing" "$(chq "SELECT count() FROM fact.raw_token_metrics_summary_1d_dist ${WHERE_D2}")" "0"
scenario '{"not_ready_until_uptime_s": 0}'      # reset (retry_after_s 는 ≥1 이어야 하므로 그대로 둔다)

# (8) manual-v0 — {DATE} → DATE2 치환본을 .tmp/ 에 생성, 같은 normalize+replace 경로 (§5.5)
for f in gpu serving engine; do
  sed "s/{DATE}/${DATE2}/g" "tests/e2e/manual_e2e/e2e_manual_v0_${f}.csv" > "${TMP}/e2e_manual_v0_${f}.csv"
done
run_collector --manual-gpu "${TMP}/e2e_manual_v0_gpu.csv" --manual-serving "${TMP}/e2e_manual_v0_serving.csv" \
  --manual-engine "${TMP}/e2e_manual_v0_engine.csv" --from "${DATE2}" --to "${DATE2}" \
  --generated-at "${NEXT_DAY}T09:00:00+09:00"
expect_eq "manual run exit code" "${RC}" "0"
need_line "manual MANUAL_INPUT" "MANUAL_INPUT module=token-metrics rows_gpu=2 rows_serving=3 rows_engine=1 rows_outside_range=0 rows_other_service=0"
need_line "manual SERVICE_RESULT" "SERVICE_RESULT status=SUCCESS module=token-metrics service=${SVC} source_type=manual-v0 rows=5 pages=1"
need_line "manual BATCH_RESULT" "BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 services_failed=0 services_skipped=0 rows=5 "
expect_eq "manual anchor (manual-v0)" \
  "$(chq "SELECT count() FROM fact.raw_token_metrics_summary_1d_dist ${WHERE_D2} AND source_type = 'manual-v0'")" "1"
expect_eq "manual engine_version" \
  "$(chq "SELECT engine_version FROM fact.raw_token_metrics_summary_1d_dist ${WHERE_D2}")" "0.8.4"
expect_eq "manual gpu rows" "$(chq "SELECT count() FROM fact.raw_token_metrics_gpu_1d_dist ${WHERE_D2}")" "2"
expect_eq "manual serving rows" "$(chq "SELECT count() FROM fact.raw_token_metrics_serving_1d_dist ${WHERE_D2}")" "3"
expect_eq "manual source_type on gpu rows" \
  "$(chq "SELECT countIf(source_type != 'manual-v0') FROM fact.raw_token_metrics_gpu_1d_dist ${WHERE_D2}")" "0"
expect_eq "fact mutations after manual first load (no anchor → INSERT only)" "$(chq "${MUT_FACT_Q}")" "9"

# manual 앵커가 있는 날짜의 rerun(--replace 없음) → already_loaded, 뮤테이션 0
run_collector --from "${DATE2}" --to "${DATE2}"
expect_eq "rerun after manual exit code" "${RC}" "0"
need_line "rerun after manual already_loaded" "SERVICE_RESULT status=SKIPPED module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=already_loaded"
expect_eq "fact mutations unchanged after already_loaded" "$(chq "${MUT_FACT_Q}")" "9"

# (9)
echo "E2E PASS (date=${DATE_ARG}, gpu=${EXP_GPU_ROWS}, serving=${EXP_SERVING_ROWS})"
