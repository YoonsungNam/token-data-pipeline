#!/usr/bin/env bash
# CI/로컬(도커 필요) E2E: CH 컨테이너(CLICKHOUSE_SKIP_USER_SETUP=1) → dim DDL 2종(단일노드
# 변환: user-org/model-catalog — accounts.sql은 admin 수동 GRANT라 애초에 읽지 않아 변환
# 대상에서 자연 제외) → ①모델 시드 적용→재실행(멱등)→count==4→검증부 --expect-empty
# → ②T2 도구로 fixture→INSERT SQL 생성→적용→재실행(멱등)→count==fixture 행수→검증부
# --expect-empty → ③mart 소비 계약 스모크(argMax 이력 조인 + user-0005 이동 후 조직 단정
# — fixture에서 산출, 하드코딩 금지). mart/token-usage/tests/e2e/run_e2e.sh 골격 축소판
# (Plan 4 T3). HTTP 오류 본문 출력 진단(curl -f 금지 — 코드 캡처)을 전 단계에 적용.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."     # assets/user-org

CH_PORT_HOST=18124
CH_URL="http://127.0.0.1:${CH_PORT_HOST}/"
# 시드/T2 SQL 공통 리터럴 — 적용부(INSERT)와 검증부(SELECT) 분리 앵커 (T3 sed 앵커,
# csv_to_dim_user_org_insert.py가 동일 리터럴 사용 — 이중 정의 아님, 고정 계약 문자열).
ANCHOR='-- 검증: 결과가 비어야 정상 ------------------------------------------------'
# user-0005 조직 이동(fixture effective_from=2026-06-01) 이후로 고정한 기준일 —
# 브리프 §Step1.4 지정값. mart E2E와 달리 이관 전/후 경계만 확인하면 되므로 고정.
MOVE_CHECK_DATE="2026-07-01"

docker network create tokene2e-assets >/dev/null 2>&1 || true
trap 'docker rm -f ch-e2e-assets >/dev/null 2>&1 || true; docker network rm tokene2e-assets >/dev/null 2>&1 || true' EXIT

# CLICKHOUSE_SKIP_USER_SETUP=1: 공식 이미지는 비밀번호 미설정 시 default 유저의 네트워크
# 접근을 차단(localhost 전용) — published port 경유(브리지 IP) 쿼리가 403이 됨 (mart/
# collectors E2E와 동일 이슈, Plan 2a E2E 교훈).
docker run -d --rm --name ch-e2e-assets --network tokene2e-assets -p "${CH_PORT_HOST}:8123" \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  clickhouse/clickhouse-server:24.8

for _ in $(seq 1 60); do
  curl -sf "http://127.0.0.1:${CH_PORT_HOST}/ping" >/dev/null && break
  sleep 1
done

# ---------------------------------------------------------------------------
# 공용 헬퍼(런타임 생성, 레포 반입 아님) — 문장(';') 단위 POST + HTTP 오류 본문 출력
# 진단(DDL 변환 블록과 동일 urllib 패턴 — curl -f 금지). 인자는 전부 argv로 전달해
# heredoc의 셸 변수 미노출 문제(mart run_e2e.sh 교훈)를 애초에 회피한다.
# ---------------------------------------------------------------------------
cat > /tmp/assets_e2e_lib.py <<'PYEOF'
"""assets E2E 헬퍼 (Plan 4 T3) — SQL 파일 분할/적용/검증. stdlib(urllib/csv)만 사용."""
import csv
import sys
from datetime import datetime
import urllib.error
import urllib.request


def _post(url: str, stmt: str) -> bytes:
    req = urllib.request.Request(url, data=stmt.encode())
    try:
        return urllib.request.urlopen(req).read()
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        print(f"CH QUERY FAILED (HTTP {e.code}): {body}")
        print(f"statement: {stmt.strip()[:300]}")
        raise SystemExit(1)


def _statements(text: str):
    for stmt in text.split(";"):
        s = stmt.strip()
        if s:
            yield s + ";"


def cmd_split(argv):
    """앵커 리터럴 기준 파일을 (적용부, 검증부)로 분할 — 네트워크 없음."""
    anchor, path, out_pre, out_post = argv
    text = open(path, encoding="utf-8").read()
    idx = text.index(anchor)
    open(out_pre, "w", encoding="utf-8").write(text[:idx])
    open(out_post, "w", encoding="utf-8").write(text[idx:])


def cmd_apply(argv):
    """파일을 ';' 단위 문장으로 POST — 전부 성공해야 통과."""
    url, path, label = argv
    text = open(path, encoding="utf-8").read()
    n = 0
    for stmt in _statements(text):
        _post(url, stmt)
        n += 1
    print(f"{label}: {n}건 적용")


def cmd_apply_expect_empty(argv):
    """파일을 ';' 단위 문장으로 POST — 각 문장 결과가 전부 비어야 통과
    (T2 도구 검증부는 dup_key/missing_key/key_conflict가 개별 문장 — max_query_size
    회피를 위한 설계, 시드 검증부는 단일 UNION ALL 문장 1개인 특수 케이스로 자연 포함)."""
    url, path, label = argv
    text = open(path, encoding="utf-8").read()
    n = 0
    for stmt in _statements(text):
        body = _post(url, stmt).decode(errors="replace")
        if body.strip():
            print(f"{label} FAILED — 결과가 비어야 하는데 존재:\n{body}")
            print(f"statement: {stmt.strip()[:300]}")
            raise SystemExit(1)
        n += 1
    print(f"{label}: {n}건 검증 통과(전부 빈 결과)")


def cmd_count_eq(argv):
    url, table, expected, label = argv
    body = _post(url + "?default_format=TSV", f"SELECT count() FROM {table};").decode().strip()
    if body != expected:
        print(f"{label} FAILED — count={body} expected={expected}")
        raise SystemExit(1)
    print(f"{label}: count={body} (OK)")


def _resolve_org_path(csv_path: str, user_id: str, as_of: str):
    """fixture CSV에서 user_id의 as_of 시점 유효 조직(effective_from<=as_of 중 최신)을
    산출 — mart의 argMax(org_path, effective_from) 의미론과 동일 (하드코딩 금지)."""
    fmt = "%Y-%m-%d"
    as_of_dt = datetime.strptime(as_of, fmt)
    best_row, best_dt = None, None
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["user_id"] != user_id:
                continue
            eff = (row.get("effective_from") or "").strip()
            if not eff:
                continue
            eff_dt = datetime.strptime(eff, fmt)
            if eff_dt > as_of_dt:
                continue
            if best_dt is None or eff_dt > best_dt:
                best_dt, best_row = eff_dt, row
    if best_row is None:
        print(f"mart-smoke FAILED — fixture에 {user_id}의 {as_of} 이전 유효 행이 없음")
        raise SystemExit(1)
    return [seg.strip() for seg in best_row["org"].split(">")]


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _array_literal(segments) -> str:
    return "[" + ",".join(f"'{_escape(s)}'" for s in segments) + "]"


def cmd_mart_smoke(argv):
    """mart 소비 계약 스모크: (a) STEP1 이력 조인과 동형인 argMax 쿼리 실행 성공(T1
    개명 확인 겸) + (b) user_id의 as_of 기준 org_path가 fixture 산출값과 일치 단정."""
    url, csv_path, as_of, user_id, dim_table, label = argv

    general_sql = (
        f"SELECT user_id, argMax(org_path, effective_from) AS org_path\n"
        f"FROM {dim_table}\n"
        f"WHERE effective_from <= '{as_of}'\n"
        f"GROUP BY user_id;"
    )
    _post(url, general_sql)
    print(f"{label}: argMax 이력 조인 쿼리 실행 성공")

    expected_literal = _array_literal(_resolve_org_path(csv_path, user_id, as_of))
    check_sql = (
        f"SELECT '{user_id}_org_mismatch_or_missing' AS check_name, count() AS actual, 1 AS expected\n"
        f"FROM (\n"
        f"    SELECT user_id, argMax(org_path, effective_from) AS org_path\n"
        f"    FROM {dim_table}\n"
        f"    WHERE effective_from <= '{as_of}'\n"
        f"    GROUP BY user_id\n"
        f")\n"
        f"WHERE user_id = '{user_id}' AND org_path = {expected_literal}\n"
        f"HAVING count() != 1;"
    )
    body = _post(url, check_sql).decode(errors="replace")
    if body.strip():
        print(f"{label} FAILED — {user_id} org_path 불일치(기대={expected_literal}):\n{body}")
        raise SystemExit(1)
    print(f"{label}: {user_id} org_path == {expected_literal} ({as_of} 기준, fixture 산출) 확인")


COMMANDS = {
    "split": cmd_split,
    "apply": cmd_apply,
    "apply-expect-empty": cmd_apply_expect_empty,
    "count-eq": cmd_count_eq,
    "mart-smoke": cmd_mart_smoke,
}

if __name__ == "__main__":
    _cmd, *_rest = sys.argv[1:]
    COMMANDS[_cmd](_rest)
PYEOF

# ---------------------------------------------------------------------------
# Step 1: dim DDL 2종 적용 — 단일노드 변환(Replicated→MergeTree·ON CLUSTER 제거·
# Distributed 재작성, 중첩 괄호 안전 regex — mart run_e2e.sh 블록 재사용).
# accounts.sql(GRANT, admin 수동)은 읽지 않아 변환 대상에서 자연 제외.
# ---------------------------------------------------------------------------
python3 - <<'PY'
import re, pathlib, urllib.request

sql = "CREATE DATABASE IF NOT EXISTS gpu_data;\n"
sql += pathlib.Path("ddl/company/dim_token_user_org.sql").read_text()
sql += "\n" + pathlib.Path("../model-catalog/ddl/company/dim_token_model.sql").read_text()

sql = re.sub(r"ON CLUSTER 'gpu-monitoring'", "", sql)
sql = re.sub(r"ENGINE = ReplicatedMergeTree\([^)]*\)", "ENGINE = MergeTree", sql, flags=re.S)
sql = re.sub(r"ENGINE = Distributed\('gpu-monitoring', '(\w+)', '(\w+)',[\s\S]*?\);",
             r"ENGINE = Distributed('default', '\1', '\2', rand());", sql)

for stmt in sql.split(";"):
    if stmt.strip():
        # CH_PORT_HOST(위 bash 변수)와 동일 포트를 리터럴로 고정 — heredoc 프로세스는
        # export되지 않은 셸 변수를 보지 못하므로 os.environ 경유 대신 직접 명시
        # (mart run_e2e.sh와 동일 이유).
        req = urllib.request.Request("http://127.0.0.1:18124/", data=(stmt + ";").encode())
        try:
            urllib.request.urlopen(req).read()
        except urllib.error.HTTPError as e:
            print(f"DDL failed (HTTP {e.code}): {e.read().decode(errors='replace')}")
            print(f"statement: {stmt.strip()[:200]}")
            raise SystemExit(1)
print("DDL applied (single-node transformed)")
PY

# ---------------------------------------------------------------------------
# Step 2: 모델 시드 — 적용(INSERT부)→재실행(멱등)→count==4→검증부(SELECT부) --expect-empty
# ---------------------------------------------------------------------------
SEED_FILE="../model-catalog/ddl/company/seed_dim_token_model.sql"
python3 /tmp/assets_e2e_lib.py split "${ANCHOR}" "${SEED_FILE}" \
  /tmp/assets_e2e_seed_insert.sql /tmp/assets_e2e_seed_verify.sql
python3 /tmp/assets_e2e_lib.py apply "${CH_URL}" /tmp/assets_e2e_seed_insert.sql \
  "seed_dim_token_model INSERT(1회차)"
python3 /tmp/assets_e2e_lib.py apply "${CH_URL}" /tmp/assets_e2e_seed_insert.sql \
  "seed_dim_token_model INSERT(재실행·멱등)"
python3 /tmp/assets_e2e_lib.py count-eq "${CH_URL}" gpu_data.dim_token_model_dist 4 \
  "dim_token_model count"
python3 /tmp/assets_e2e_lib.py apply-expect-empty "${CH_URL}" /tmp/assets_e2e_seed_verify.sql \
  "seed_dim_token_model 검증부"

# ---------------------------------------------------------------------------
# Step 3: T2 도구 왕복 — fixture→SQL 생성(--out /tmp/...)→적용→재실행(멱등)
# →count==fixture 행수(wc -l 동적 산출 — 스크립트 상수화로 인한 드리프트 방지)
# →검증부 --expect-empty
# ---------------------------------------------------------------------------
FIXTURE_CSV="fixtures/synthetic_org_members.csv"
FIXTURE_ROWS=$(($(wc -l < "${FIXTURE_CSV}") - 1))   # 헤더 제외
echo "fixture 행수(동적 산출): ${FIXTURE_ROWS}"

GEN_SQL=/tmp/assets_e2e_dim_user_org_insert.sql
python3 csv_to_dim_user_org_insert.py --csv "${FIXTURE_CSV}" --out "${GEN_SQL}"

python3 /tmp/assets_e2e_lib.py split "${ANCHOR}" "${GEN_SQL}" \
  /tmp/assets_e2e_org_insert.sql /tmp/assets_e2e_org_verify.sql
python3 /tmp/assets_e2e_lib.py apply "${CH_URL}" /tmp/assets_e2e_org_insert.sql \
  "dim_token_user_org INSERT(1회차)"
python3 /tmp/assets_e2e_lib.py apply "${CH_URL}" /tmp/assets_e2e_org_insert.sql \
  "dim_token_user_org INSERT(재실행·멱등)"
python3 /tmp/assets_e2e_lib.py count-eq "${CH_URL}" gpu_data.dim_token_user_org_dist \
  "${FIXTURE_ROWS}" "dim_token_user_org count"
python3 /tmp/assets_e2e_lib.py apply-expect-empty "${CH_URL}" /tmp/assets_e2e_org_verify.sql \
  "dim_token_user_org 검증부"

# ---------------------------------------------------------------------------
# Step 4: mart 소비 계약 스모크 — STEP1과 동형인 argMax 이력 조인 쿼리 실행 성공
# (T1 개명 확인 겸) + user-0005의 기준일 org_path가 fixture 이동 후 조직과 일치
# (fixture에서 산출 — 하드코딩 금지)
# ---------------------------------------------------------------------------
python3 /tmp/assets_e2e_lib.py mart-smoke "${CH_URL}" "${FIXTURE_CSV}" "${MOVE_CHECK_DATE}" \
  user-0005 gpu_data.dim_token_user_org_dist "mart 소비 계약 스모크"

echo "E2E PASS (dim_token_model=4, dim_token_user_org=${FIXTURE_ROWS}, move_check_date=${MOVE_CHECK_DATE})"
