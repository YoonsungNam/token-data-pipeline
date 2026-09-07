"""app/steps.py 단위 테스트 — SQL 문자열 계약(서버 바인딩·컬럼 순서·비용 술어·우선순위) +
_run_table 시퀀스(FakeGate). ClickHouse 없이 돈다(SQL 실행은 T10 e2e·CI가 담당).

FakeGate는 mart/token-usage/tests/test_steps.py의 것을 복제하되 테이블 키를 mart-metrics
4테이블로 바꿨다(_TABLE_KEYS 부분 문자열 라우팅 — 서로 부분 문자열이 아니어야 함, 테스트로 고정).
"""
import os
import pathlib
import re
import subprocess
import sys

import pytest

from app import steps
from app.ch import DB_DIM, DB_FACT, DB_MART, DB_TOKEN_DIM, DB_TOKEN_MART
from app.mart import FAIL_FLAGS, W_CACHE, W_OUT, W_UNC
from app.preflight import READ_CONTRACT

MODULE_ROOT = pathlib.Path(__file__).resolve().parents[1]
DDL_PATH = MODULE_ROOT / "ddl" / "company" / "mart_metrics_tables.sql"
DATE = "2026-09-01"   # 러너 테스트용 날짜 상수 — T4(M3)가 같은 파일에서 재사용, T6/T7은 자체 M4_DATE/M2_DATE

# 읽기 계약(설계 §5.4 / app.preflight.READ_CONTRACT) — token_usage_1d 9컬럼.
# (컨트롤러 결정 D3) 하드코딩하지 않고 실제 계약에서 파생 — Task 1이 계약을 바꾸면 여기서도 잡힌다.
READ_CONTRACT_TOKEN_USAGE_1D = READ_CONTRACT[f"{DB_TOKEN_MART}.token_usage_1d"]


# ----------------------------------------------------------------------------
# 헬퍼 — DDL 컬럼 파서(T4/T6/T7이 재사용) + INSERT 컬럼 목록 파서
# ----------------------------------------------------------------------------

def ddl_columns(table_local: str) -> list:
    """mart_metrics_tables.sql에서 `CREATE TABLE IF NOT EXISTS mart.<table_local>` 블록의
    컬럼 이름을 선언 순서대로 반환. CONSTRAINT 행·빈 줄·주석은 제외."""
    text = DDL_PATH.read_text(encoding="utf-8")
    head = f"CREATE TABLE IF NOT EXISTS mart.{table_local}\n"
    start = text.index(head)
    body_start = text.index("\n(\n", start) + len("\n(\n")
    body_end = text.index("\n)\n", body_start)
    cols = []
    for raw in text[body_start:body_end].splitlines():
        line = raw.strip()
        if not line or line.startswith("--") or line.startswith("CONSTRAINT"):
            continue
        cols.append(line.split()[0])
    return cols


def insert_columns(sql: str) -> list:
    """`INSERT INTO <table> (c1, c2, ...)`의 컬럼 목록."""
    m = re.search(r"INSERT INTO\s+\S+\s*\((.*?)\)", sql, re.S)
    assert m, "INSERT INTO <table> (cols) 형태가 아님"
    return [c.strip() for c in m.group(1).split(",") if c.strip()]


def sql_constants() -> dict:
    """steps 모듈의 SQL_*/EXPECTED_SQL_* 상수 전부(T4~T7이 추가하는 것도 자동 포함)."""
    return {n: v for n, v in vars(steps).items()
            if (n.startswith("SQL_") or n.startswith("EXPECTED_SQL_")) and isinstance(v, str)}


# ----------------------------------------------------------------------------
# FakeGate — 토큰 mart 테스트 더블 복제(테이블 키만 교체)
# ----------------------------------------------------------------------------

class FakeGate:
    """CHGate 더블. 호출 순서(order)·삭제 술어·INSERT SQL·query SQL·verify 인자를 기록한다.
    `_short()`는 dist/SQL 문자열에서 테이블 키를 찾아 짧은 이름(m1/m3/m4/m2)으로 라우팅한다."""

    _TABLE_KEYS = [
        ("agg_token_model_cost_1d", "m1"),
        ("token_metrics_check_1d", "m3"),
        ("agg_token_model_share_1d", "m4"),
        ("agg_token_gpu_group_1d", "m2"),
    ]

    def __init__(self, exists=True, verify_ok=True, verify_actual=None, expected_overrides=None):
        self._exists = exists
        self._verify_ok = verify_ok
        self._verify_actual = verify_actual
        self._expected_overrides = expected_overrides or {}
        self.order = []
        self.delete_preds = []
        self.written = []
        self.query_calls = []
        self.verify_calls = []
        self._current_short = None

    def _short(self, s: str) -> str:
        for key, short in sorted(self._TABLE_KEYS, key=lambda kv: -len(kv[0])):
            if key in s:
                return short
        raise AssertionError(f"unknown table in: {s[:120]!r}")

    def exists(self, table_dist, date):
        self.order.append(("exists", self._short(table_dist)))
        return self._exists

    def delete_day(self, table_local, date, extra_pred=""):
        self.order.append(("delete", self._short(table_local)))
        self.delete_preds.append((self._short(table_local), extra_pred))

    def insert_select(self, sql, params=None):
        short = self._short(sql)
        self._current_short = short
        self.order.append(("insert", short))
        self.written.append((short, sql, params))
        return 7

    def query(self, sql, params=None):
        self.order.append(("query", self._current_short))
        self.query_calls.append((self._current_short, sql, params))
        return [(self._expected_overrides.get(self._current_short, 3),)]

    def verify_count(self, table_dist, date, expected):
        short = self._short(table_dist)
        self.order.append(("verify", short))
        self.verify_calls.append((short, date, expected))
        actual = expected if self._verify_actual is None else self._verify_actual
        return (self._verify_ok, actual)


# ----------------------------------------------------------------------------
# 전역 SQL 계약
# ----------------------------------------------------------------------------

def test_all_sql_constants_use_date_binding_and_no_percent_format():
    consts = sql_constants()
    assert {"SQL_M1", "EXPECTED_SQL_M1"} <= set(consts)
    for name, sql in consts.items():
        assert "{d:Date}" in sql, name
        assert "%(" not in sql and "%s" not in sql, name
        assert "{{" not in sql and "}}" not in sql, name      # f-string 이스케이프 잔재 금지
        assert ".format(" not in sql, name


def test_no_coalesce_anywhere_in_sql():
    for name, sql in sql_constants().items():
        assert "coalesce(" not in sql.lower(), name


def test_created_by_is_token_metrics_pipeline():
    assert steps.CREATED_BY == "token-metrics-pipeline"
    assert re.search(r"'token-metrics-pipeline'\s+AS created_by", steps.SQL_M1)
    assert "'token-pipeline'" not in steps.SQL_M1


def test_canon_expression_identical_in_insert_and_expected():
    assert steps.canon("u.model") == "if(a.canonical = '', u.model, a.canonical)"
    for x in ("u.model", "g.model"):
        assert steps.canon(x) in steps.SQL_M1
        assert steps.canon(x) in steps.EXPECTED_SQL_M1
    # 키 조각 자체가 두 SQL에 그대로 들어간다(파생 오차 0)
    for frag in (steps._TOK_KEYS, steps._GPU_KEYS):
        assert frag in steps.SQL_M1
        assert frag in steps.EXPECTED_SQL_M1
    assert "UNION DISTINCT" in steps.SQL_M1
    assert "UNION ALL" in steps.EXPECTED_SQL_M1
    assert "uniqExact((service, model))" in steps.EXPECTED_SQL_M1


def test_sub_queries_shared_between_insert_and_expected():
    # SUB_* 문자열 상수 = 괄호로 감싼 서브쿼리, 호출측이 AS 별칭을 붙인다
    subs = {n: v for n, v in vars(steps).items() if n.startswith("SUB_")}
    assert {"SUB_EFF_ALIAS", "SUB_EFF_TCO", "SUB_EFF_ALLOC", "SUB_EFF_PRICE",
            "SUB_REG", "SUB_USAGE_SVC", "SUB_ANCHOR", "SUB_GPU_CNT", "SUB_SERVING_CNT"} <= set(subs)
    for name, sub in subs.items():
        assert sub.startswith("(SELECT") and sub.endswith(")"), name
        assert " AS " not in sub[-6:], name
    for name in ("SUB_EFF_ALIAS", "SUB_EFF_TCO", "SUB_EFF_ALLOC", "SUB_EFF_PRICE",
                 "SUB_ANCHOR", "SUB_GPU_CNT", "SUB_SERVING_CNT"):
        assert "{d:Date}" in subs[name], name
    assert f"{DB_DIM}.dim_token_model_alias_dist" in steps.SUB_EFF_ALIAS
    assert f"{DB_DIM}.dim_token_gpu_tco_dist" in steps.SUB_EFF_TCO
    assert f"{DB_DIM}.dim_token_gpu_allocation_dist" in steps.SUB_EFF_ALLOC
    assert f"{DB_DIM}.dim_token_vendor_price_dist" in steps.SUB_EFF_PRICE
    assert f"{DB_DIM}.dim_token_metrics_service_dist" in steps.SUB_REG
    assert f"{DB_TOKEN_DIM}.dim_token_service_dist" in steps.SUB_USAGE_SVC
    assert f"{DB_FACT}.raw_token_metrics_summary_1d_dist" in steps.SUB_ANCHOR
    assert f"{DB_FACT}.raw_token_metrics_gpu_1d_dist" in steps.SUB_GPU_CNT
    assert f"{DB_FACT}.raw_token_metrics_serving_1d_dist" in steps.SUB_SERVING_CNT
    # serving_rows 앵커값은 serving[] 원소 수 — custom 전개 행(metric='custom')은 실측에서 제외
    assert "countIf(metric != 'custom') AS n" in steps.SUB_SERVING_CNT
    assert "count() AS n" in steps.SUB_GPU_CNT
    # T4/T6/T7 계약: alloc은 unknown 제외 + allocated_gpu_count 별칭, price는 standard 고정 + p_* 별칭
    assert "gpu_type != 'unknown'" in steps.SUB_EFF_ALLOC
    assert "AS allocated_gpu_count" in steps.SUB_EFF_ALLOC
    assert "tier = 'standard'" in steps.SUB_EFF_PRICE
    for alias in ("AS p_in", "AS p_cached", "AS p_cc", "AS p_out"):
        assert alias in steps.SUB_EFF_PRICE
    # 최신 이력 행의 NULL 전파(설계 해석 2) — argMax(ifNull(x, -1)) + nullIf(..., -1)
    for name in ("SUB_EFF_TCO", "SUB_EFF_ALLOC", "SUB_EFF_PRICE"):
        assert "nullIf(argMax(ifNull(" in subs[name], name
    assert "effective_from <= {d:Date}" in steps.SUB_EFF_ALIAS
    assert "enabled = 1" in steps.SUB_USAGE_SVC
    # SQL_M1에서 실제로 조인되는 조각들
    for name in ("SUB_EFF_ALIAS", "SUB_EFF_TCO", "SUB_REG", "SUB_USAGE_SVC", "SUB_ANCHOR",
                 "SUB_GPU_CNT", "SUB_SERVING_CNT"):
        assert subs[name] in steps.SQL_M1, name


def test_global_join_and_global_in_only():
    for name, sql in sql_constants().items():
        for m in re.finditer(r"\bLEFT JOIN\b", sql):
            assert sql[max(0, m.start() - 7):m.start()] == "GLOBAL ", (name, sql[m.start() - 40:m.end()])
        for m in re.finditer(r"\bIN\s*\(SELECT", sql):
            assert sql[max(0, m.start() - 7):m.start()] == "GLOBAL ", (name, sql[m.start() - 40:m.end()])
        if not name.endswith("_SUMMARY"):   # SQL_M3_SUMMARY(T4)는 단일 테이블 GROUP BY — 서브쿼리 없음
            assert "GLOBAL IN" in sql, name
        assert " JOIN " not in sql.replace("GLOBAL LEFT JOIN", ""), name   # INNER/CROSS 금지


# ----------------------------------------------------------------------------
# M1 — agg_token_model_cost_1d
# ----------------------------------------------------------------------------

def test_m1_insert_column_list_matches_ddl_order():
    cols = ddl_columns("agg_token_model_cost_1d_local")
    assert len(cols) == 28
    assert cols[0] == "date" and cols[-1] == "created_by"
    assert insert_columns(steps.SQL_M1) == cols
    # SELECT 절 alias도 같은 순서(위치 기반 INSERT 금지 원칙의 2중 방어)
    outer = steps.SQL_M1[steps.SQL_M1.rindex("\nSELECT\n"):steps.SQL_M1.index("\nFROM keys AS k")]
    aliases = re.findall(r"\bAS (\w+)\s*,?\s*$", outer, re.M)
    assert aliases == cols


def test_m1_cost_predicate_serving_standby_not_fail():
    sql = steps.SQL_M1
    fail = "hasAny(g.flags, ['hours_over_count','unknown_violation'])"
    assert steps.FAIL_PRED == fail
    assert tuple(FAIL_FLAGS) == ("hours_over_count", "unknown_violation")
    assert f"g.category IN ('serving','standby') AND NOT {fail}" in sql
    # cost_sum sumIf 슬라이스에 'test'가 없어야 한다(테스트 GPU 시간은 C 불포함)
    cost_slice = sql[sql.index("sumIf(g.gpu_hours * t.tco"):sql.index("AS cost_sum")]
    assert "'test'" not in cost_slice
    assert f"NOT {fail} AND isNotNull(t.tco)" in cost_slice
    # 기종 하나라도 TCO NULL → C NULL(부분 합 금지), 행 없음 → NULL, 그 외 NULL 합은 0
    assert "if(ga.has_rows = 0 OR ga.tco_null_cnt > 0, NULL, ifNull(ga.cost_sum, 0))" in sql
    assert f"countIf(g.category IN ('serving','standby') AND NOT {fail} AND isNull(t.tco))" in sql
    # 시간 4분류
    assert f"sumIf(g.gpu_hours, g.category = 'serving' AND NOT {fail})  AS serving_gpu_hours" in sql
    assert f"sumIf(g.gpu_hours, g.category = 'standby' AND NOT {fail})  AS standby_gpu_hours" in sql
    assert f"sumIf(g.gpu_hours, g.category = 'test' AND NOT {fail})     AS test_gpu_hours" in sql
    assert f"sumIf(g.gpu_hours, {fail})" in sql and "AS flagged_gpu_hours" in sql
    assert "/ 24" in sql and "AS equiv_gpu_count" in sql
    assert "0                                                                 AS scaled_intraday" in sql
    assert "if(ga.serving_gpu_hours > 0, total_tokens / ga.serving_gpu_hours, NULL)" in sql
    assert "arraySort(groupUniqArray(g.gpu_type))" in sql


def test_m1_weight_constants_inlined():
    assert (W_UNC, W_CACHE, W_OUT) == (1.0, 0.1, 4.0)
    assert "1.0 * (" in steps.SQL_M1
    assert "0.1 * " in steps.SQL_M1
    assert "4.0 * " in steps.SQL_M1
    assert steps._WTOK_EXPR == ("1.0 * (input_tokens + cache_creation_tokens)"
                                " + 0.1 * cache_read_tokens + 4.0 * output_tokens")
    assert f"{steps._WTOK_EXPR}" in steps.SQL_M1
    assert "input_tokens + cache_creation_tokens                              AS uncached_tokens" in steps.SQL_M1
    assert "cache_read_tokens                                                 AS cached_tokens" in steps.SQL_M1
    assert "input_tokens + cache_read_tokens + cache_creation_tokens + output_tokens" in steps.SQL_M1


def test_m1_reads_token_side_only_via_token_db_constants():
    sql = steps.SQL_M1
    used = set(re.findall(r"\bu\.(\w+)", sql))
    assert used <= set(READ_CONTRACT_TOKEN_USAGE_1D), used - set(READ_CONTRACT_TOKEN_USAGE_1D)
    assert f"{DB_TOKEN_MART}.token_usage_1d_dist" in sql
    assert f"{DB_TOKEN_DIM}.dim_token_service_dist" in sql
    # 토큰 측 테이블은 DB_TOKEN_* 접두로만 등장(격리 DB 검증 시 운영 DB를 가리키는 유일한 경로)
    assert re.search(r"\b\w+\.token_usage_1d_dist", sql).group(0) == f"{DB_TOKEN_MART}.token_usage_1d_dist"
    for m in re.finditer(r"(\w+)\.token_usage_1d_dist", sql):
        assert m.group(1) == DB_TOKEN_MART
    for m in re.finditer(r"(\w+)\.dim_token_service_dist", sql):
        assert m.group(1) == DB_TOKEN_DIM
    assert "agg_token_service_1d" not in sql          # M1은 token_usage_1d만 읽는다
    assert f"INSERT INTO {DB_MART}.agg_token_model_cost_1d_dist" in sql
    assert "u.user_id" not in sql and "u.org" not in sql


def test_db_env_override_isolates_token_side_in_sql_m1():
    """company-verify 격리(설계 §5.4/§6.1): 메트릭 fact/dim/mart는 token_verify_* DB, 토큰 측 읽기만
    운영 DB(CH_DB_TOKEN_MART/CH_DB_TOKEN_DIM). DB명은 import 시점에 f-string으로 고정되므로
    자식 프로세스에서 확인한다(원형 mart/token-usage/tests/test_ch.py::test_db_names_env_override)."""
    env = {"PATH": os.environ.get("PATH", ""),
           "CH_DB_FACT": "token_verify_fact", "CH_DB_DIM": "token_verify_gpu_data",
           "CH_DB_MART": "token_verify_mart",
           "CH_DB_TOKEN_MART": "mart", "CH_DB_TOKEN_DIM": "gpu_data"}
    result = subprocess.run(
        [sys.executable, "-c", "from app import steps; print(steps.SQL_M1)"],
        cwd=str(MODULE_ROOT), env=env, capture_output=True, text=True, check=True)
    sql = result.stdout
    assert "INSERT INTO token_verify_mart.agg_token_model_cost_1d_dist" in sql
    assert "token_verify_fact.raw_token_metrics_gpu_1d_dist" in sql
    assert "token_verify_fact.raw_token_metrics_summary_1d_dist" in sql
    assert "token_verify_gpu_data.dim_token_model_alias_dist" in sql
    assert "token_verify_gpu_data.dim_token_metrics_service_dist" in sql
    assert "mart.token_usage_1d_dist" in sql and "token_verify_mart.token_usage_1d_dist" not in sql
    assert "gpu_data.dim_token_service_dist" in sql and "token_verify_gpu_data.dim_token_service_dist" not in sql
    # CH_DB_TOKEN_* 미지정이면 DB_MART/DB_DIM을 따라간다(단일 DB 운영 기본값)
    env2 = {k: v for k, v in env.items() if not k.startswith("CH_DB_TOKEN_")}
    result2 = subprocess.run(
        [sys.executable, "-c", "from app import steps; print(steps.SQL_M1)"],
        cwd=str(MODULE_ROOT), env=env2, capture_output=True, text=True, check=True)
    assert "token_verify_mart.token_usage_1d_dist" in result2.stdout
    assert "token_verify_gpu_data.dim_token_service_dist" in result2.stdout


def test_m1_quality_flag_priority_order_in_sql():
    sql = steps.SQL_M1
    order = ["'partial'", "'no_tco'", "'flagged'", "'manual'", "'no_metrics'", "'consumer_only'", "'normal'"]
    qf = sql[sql.rindex("multiIf(", 0, sql.index("AS quality_flag")):sql.index("AS quality_flag")]
    positions = [qf.index(tok) for tok in order]
    assert positions == sorted(positions)
    # 판정 술어(설계 해석 4: partial = 앵커 있음 AND (gpu_rows 또는 serving_rows 실측 불일치))
    assert "an.service != '' AND (an.gpu_rows != gc.n OR an.serving_rows != sc.n)" in qf
    assert "ga.has_rows = 1 AND ga.tco_null_cnt > 0" in qf
    assert "ga.flagged_gpu_hours > 0" in qf
    assert "an.source_type = 'manual-v0'" in qf
    assert ("r.service != '' AND r.enabled = 1 AND r.coverage_since <= {d:Date}"
            "\n            AND (isNull(r.until) OR {d:Date} <= r.until) AND an.service = ''") in qf
    assert "r.service = '', " in qf
    # 플래그·has 컬럼
    assert "greatest(tk.registered, ga.registered)" in sql
    assert "max(a.canonical != '')" in sql
    assert "max(isNull(t.tco))" in sql and "AS tco_missing" in sql
    assert "tk.has_rows                                                       AS has_token_rows" in sql
    assert "ga.has_rows                                                       AS has_gpu_rows" in sql


def test_m1_service_group_fallback_order():
    # reg > gpu fact > token mart (설계 §6.1 — 레지스트리 우선, 미등록 서비스는 소스 값)
    assert ("multiIf(r.service_group != '', r.service_group,\n"
            "            ga.service_group != '', ga.service_group,\n"
            "            tk.service_group)") in steps.SQL_M1


def test_mart_tables_order_and_names():
    assert steps.T_M1 == "agg_token_model_cost_1d"
    assert steps.T_M3 == "token_metrics_check_1d"
    assert steps.T_M4 == "agg_token_model_share_1d"
    assert steps.T_M2 == "agg_token_gpu_group_1d"
    assert steps.MART_TABLES == (steps.T_M1, steps.T_M3, steps.T_M4, steps.T_M2)


# ----------------------------------------------------------------------------
# FakeGate 자체 계약 + _run_table 시퀀스
# ----------------------------------------------------------------------------

def test_fake_gate_table_keys_are_not_substrings_of_each_other():
    keys = [k for k, _ in FakeGate._TABLE_KEYS]
    for a in keys:
        for b in keys:
            if a != b:
                assert a not in b, (a, b)


def test_run_table_sequence_exists_delete_insert_expected_verify():
    g = FakeGate(exists=True)
    warns = []
    n = steps._run_table(g, "2026-09-01", f"{DB_MART}.{steps.T_M1}_dist",
                         f"{DB_MART}.{steps.T_M1}_local", steps.SQL_M1, steps.EXPECTED_SQL_M1, warns)
    assert g.order == [("exists", "m1"), ("delete", "m1"), ("insert", "m1"),
                       ("query", "m1"), ("verify", "m1")]
    assert g.delete_preds == [("m1", "")]
    assert g.written[0][2] == {"d": "2026-09-01"}
    assert g.query_calls[0][1] is steps.EXPECTED_SQL_M1
    assert g.query_calls[0][2] == {"d": "2026-09-01"}
    assert g.verify_calls == [("m1", "2026-09-01", 3)]
    assert n == 3 and warns == []          # 반환은 actual(소스 카운트), written_rows(7) 아님


def test_run_table_skips_delete_when_not_exists():
    g = FakeGate(exists=False)
    steps._run_table(g, "2026-09-01", f"{DB_MART}.{steps.T_M1}_dist",
                     f"{DB_MART}.{steps.T_M1}_local", steps.SQL_M1, steps.EXPECTED_SQL_M1, [])
    assert [o for o, _ in g.order] == ["exists", "insert", "query", "verify"]


def test_run_table_raises_step_error_on_verify_fail():
    g = FakeGate(verify_ok=False, verify_actual=1)
    with pytest.raises(steps.StepError) as ei:
        steps._run_table(g, "2026-09-01", f"{DB_MART}.{steps.T_M1}_dist",
                         f"{DB_MART}.{steps.T_M1}_local", steps.SQL_M1, steps.EXPECTED_SQL_M1, [])
    msg = str(ei.value)
    assert "verify_count failed" in msg and "expected=3" in msg and "actual=1" in msg
    assert "written_rows=7" in msg


def test_run_table_dup_suspect_warn_when_actual_gt_expected():
    g = FakeGate(verify_ok=True, verify_actual=5)
    warns = []
    n = steps._run_table(g, "2026-09-01", f"{DB_MART}.{steps.T_M1}_dist",
                         f"{DB_MART}.{steps.T_M1}_local", steps.SQL_M1, steps.EXPECTED_SQL_M1, warns)
    assert n == 5
    assert warns == [f"dup_suspect:{DB_MART}.agg_token_model_cost_1d_dist"]


def test_run_m1_returns_rows_mart_from_verify_actual():
    g = FakeGate(expected_overrides={"m1": 11})
    out = steps.run_m1(g, "2026-09-01")
    assert out == {"rows_mart": 11, "warns": []}
    assert g.verify_calls == [("m1", "2026-09-01", 11)]
    assert g.written[0][1] is steps.SQL_M1


# ============================================================================
# M3 token_metrics_check_1d — 핵심 13블록·빌더 계약 (Plan 6c T4)
# ============================================================================

M3_CORE_NAMES = [
    "metrics_missing", "partial_load", "rows_rejected", "unregistered_model",
    "hours_over_count", "unknown_violation", "pct_non_monotone", "gpu_type_no_tco",
    "serving_missing_for_gpu_model", "serving_without_gpu_serving_row", "identity_drift",
    "service_not_in_usage_registry", "manual_source",
]

M3_SEVERITY = {
    "metrics_missing": "FAIL", "partial_load": "FAIL", "rows_rejected": "WARN",
    "unregistered_model": "WARN", "hours_over_count": "FAIL", "unknown_violation": "FAIL",
    "pct_non_monotone": "FAIL", "gpu_type_no_tco": "WARN", "serving_missing_for_gpu_model": "WARN",
    "serving_without_gpu_serving_row": "WARN", "identity_drift": "WARN",
    "service_not_in_usage_registry": "WARN", "manual_source": "INFO",
}

# 블록이 model / gpu_type 컬럼을 실제 값으로 채워야 하는 검사 (키 단위가 model/gpu_type인 것)
M3_KEYED_MODEL = {"unregistered_model", "hours_over_count", "unknown_violation", "pct_non_monotone",
                  "serving_missing_for_gpu_model", "serving_without_gpu_serving_row"}
M3_KEYED_GPU_TYPE = {"hours_over_count", "unknown_violation", "gpu_type_no_tco"}


def _m3_select_header_aliases(block_sql: str) -> list[str]:
    """블록 SELECT 헤더(FROM 직전까지)의 'AS <alias>' 목록 — 12컬럼 순서 확인용."""
    header = block_sql.split("\nFROM", 1)[0]
    return [ln.strip().rstrip(",").rsplit(" AS ", 1)[1] for ln in header.splitlines()[1:]]


def test_m3_core_block_names_exact():
    assert [name for name, _ in steps.M3_BLOCKS_CORE] == M3_CORE_NAMES
    assert len(M3_CORE_NAMES) == 13
    assert steps.M3_BLOCKS_STRETCH == []


def test_m3_every_block_has_twelve_columns_and_own_name():
    for name, sql in steps.M3_BLOCKS_CORE:
        assert sql.startswith("SELECT\n"), name
        assert _m3_select_header_aliases(sql) == list(steps.M3_COLUMNS), name
        assert f"'{name}' AS check_name" in sql, name
        assert "'token-metrics-pipeline' AS created_by" in sql, name
        assert "    {d:Date} AS date," in sql, name


def test_m3_insert_column_list_matches_ddl_order():
    sql = steps.build_m3_sql(steps.M3_BLOCKS_CORE)
    assert sql.startswith(f"INSERT INTO {DB_MART}.token_metrics_check_1d_dist (")
    cols = insert_columns(sql)
    assert cols == ddl_columns("token_metrics_check_1d_local")
    assert len(cols) == 12
    assert cols == list(steps.M3_COLUMNS)


def test_m3_model_and_gpu_type_columns_populated_where_keyed():
    for name, sql in steps.M3_BLOCKS_CORE:
        header = sql.split("\nFROM", 1)[0]
        model_line = next(ln for ln in header.splitlines() if ln.endswith(" AS model,"))
        gpu_line = next(ln for ln in header.splitlines() if ln.endswith(" AS gpu_type,"))
        if name in M3_KEYED_MODEL:
            assert "''" not in model_line, name
        else:
            assert model_line.strip() == "'' AS model,", name
        if name in M3_KEYED_GPU_TYPE:
            assert "''" not in gpu_line, name
        else:
            assert gpu_line.strip() == "'' AS gpu_type,", name


def test_m3_severity_map():
    for name, sql in steps.M3_BLOCKS_CORE:
        assert f"'{M3_SEVERITY[name]}' AS severity" in sql, name
    assert sorted(set(M3_SEVERITY.values())) == ["FAIL", "INFO", "WARN"]
    with pytest.raises(ValueError):
        steps._m3_select("x", "ERROR", service_group="''", service="''", observed="0",
                         threshold="0", detail="''", body="FROM system.one")


def test_m3_expected_is_count_of_same_union():
    sql = steps.build_m3_sql(steps.M3_BLOCKS_CORE)
    expected = steps.build_m3_expected(steps.M3_BLOCKS_CORE)
    body = sql.split("\n", 1)[1]
    assert expected.startswith("SELECT count() FROM (\n")
    assert expected.endswith("\n)")
    assert body in expected


def test_m3_identity_drift_detail_has_no_reported_values():
    sql = dict(steps.M3_BLOCKS_CORE)["identity_drift"]
    detail_line = next(ln for ln in sql.splitlines() if ln.endswith(" AS detail,"))
    # detail은 불일치 여부(toString(toUInt8(비교식)))만 — reported_* 원문을 문자열로 싣지 않는다
    assert "toString(an.reported_service)" not in detail_line
    assert "toString(an.reported_service_group)" not in detail_line
    assert re.search(r"concat\(.*'svc_diff=', toString\(toUInt8\(an\.reported_service != an\.service\)\)", detail_line)
    assert "' group_diff=', toString(toUInt8(an.reported_service_group != r.service_group))" in detail_line
    assert "an.source_type = 'metrics-api-v1'" in sql
    assert "'identity_drift' AS check_name" in sql


def test_m3_builder_with_subset_blocks():
    two = steps.build_m3_sql(steps.M3_BLOCKS_CORE[:2])
    assert two.count("\nUNION ALL\n") == 1
    assert "'metrics_missing' AS check_name" in two and "'partial_load' AS check_name" in two
    assert "'rows_rejected' AS check_name" not in two
    full = steps.build_m3_sql(steps.M3_BLOCKS_CORE)
    assert full.count("\nUNION ALL\n") == 12
    assert steps.build_m3_expected(steps.M3_BLOCKS_CORE[:2]).count("\nUNION ALL\n") == 1
    with pytest.raises(ValueError):
        steps.build_m3_sql([])
    with pytest.raises(ValueError):
        steps.build_m3_expected([])


def test_m3_inner_union_all_never_at_column_zero():
    # 블록 내부 UNION ALL은 들여쓰기 — 최상위 조립 토큰 "\nUNION ALL\n"과 충돌하지 않는다
    for name, sql in steps.M3_BLOCKS_CORE:
        assert "\nUNION ALL\n" not in sql, name
        assert "\nUNION DISTINCT\n" not in sql, name


def test_m3_sql_contract_date_binding_no_percent_no_coalesce_no_star():
    for s in (steps.build_m3_sql(steps.M3_BLOCKS_CORE), steps.build_m3_expected(steps.M3_BLOCKS_CORE),
              steps.SQL_M3_SUMMARY):
        assert "{d:Date}" in s
        assert "%(" not in s
        assert "coalesce(" not in s.lower()
        assert "SELECT *" not in s
    for name, sql in steps.M3_BLOCKS_CORE:
        assert "{d:Date}" in sql, name


def test_m3_fact_blocks_anchored_and_partial_load_unanchored():
    anchored = {"unregistered_model", "hours_over_count", "unknown_violation", "pct_non_monotone",
                "gpu_type_no_tco", "serving_missing_for_gpu_model", "serving_without_gpu_serving_row"}
    for name, sql in steps.M3_BLOCKS_CORE:
        if name in anchored:
            assert f"GLOBAL IN {steps._M3_ANCHORED}" in sql, name
    partial = dict(steps.M3_BLOCKS_CORE)["partial_load"]
    assert "GLOBAL IN" not in partial
    assert "countIf(metric != 'custom') AS serving_n" in partial
    assert "custom_rows" not in partial
    assert "an.gpu_rows != c.actual_gpu" in partial and "an.serving_rows != c.actual_serving" in partial
    assert "UNION DISTINCT" in partial


def test_m3_token_side_columns_within_read_contract():
    sql = steps.build_m3_sql(steps.M3_BLOCKS_CORE)
    assert f"{DB_TOKEN_MART}.token_usage_1d_dist AS u" in sql
    used = set(re.findall(r"\bu\.(\w+)", sql))
    assert used <= set(READ_CONTRACT[f"{DB_TOKEN_MART}.token_usage_1d"])
    assert "agg_token_service_1d" not in sql


def test_m3_reg_expectation_predicate_matches_m0():
    missing = dict(steps.M3_BLOCKS_CORE)["metrics_missing"]
    assert "r.enabled = 1" in missing
    assert "r.coverage_since <= {d:Date}" in missing
    assert "(isNull(r.until) OR {d:Date} <= r.until)" in missing
    assert "an.service = ''" in missing
    reg_gap = dict(steps.M3_BLOCKS_CORE)["service_not_in_usage_registry"]
    assert f"r.service GLOBAL NOT IN {steps.SUB_USAGE_SVC}" in reg_gap


def test_m3_gpu_type_no_tco_excludes_fail_rows_and_uses_cost_categories():
    sql = dict(steps.M3_BLOCKS_CORE)["gpu_type_no_tco"]
    assert f"NOT {steps.FAIL_PRED}" in sql
    assert "g.category IN ('serving', 'standby')" in sql
    assert "isNull(t.tco)" in sql
    assert steps.SUB_EFF_TCO in sql


def test_m3_canon_used_for_model_keys():
    for name in ("unregistered_model", "hours_over_count", "pct_non_monotone",
                 "serving_missing_for_gpu_model", "serving_without_gpu_serving_row"):
        sql = dict(steps.M3_BLOCKS_CORE)[name]
        assert steps.SUB_EFF_ALIAS in sql, name
        assert "AS canon_model" in sql, name
    unknown = dict(steps.M3_BLOCKS_CORE)["unknown_violation"]
    assert steps.SUB_EFF_ALIAS not in unknown  # 미지 항목은 원문 모델명 그대로 (검출 대상 식별)
    # 9) 토큰 측(tk)도 canon으로 키를 맞춘다 — 원문 alias(u.model)와 gpu canon을 직접 비교하면 alias 모델이 전부 미스
    smissing = dict(steps.M3_BLOCKS_CORE)["serving_missing_for_gpu_model"]
    assert steps._TOK_SRC in smissing and steps._TOK_TAIL in smissing
    assert f"{steps.canon('u.model')} AS canon_model" in smissing
    assert "tk.canon_model = gk.canon_model" in smissing
    assert "tk.model = gk.canon_model" not in smissing


def test_m3_child_counts_shares_serving_count_expression_with_t3():
    # 컨트롤러 D1: _M3_CHILD_COUNTS가 T3 SUB_SERVING_CNT(Plan 6b n_serving 정의)와
    # 동일한 집계식을 쓴다는 것을 고정 — 중복 자체는 설계상 허용(D1), 표현 불일치만 방지.
    assert "countIf(metric != 'custom')" in steps._M3_CHILD_COUNTS
    assert "countIf(metric != 'custom')" in steps.SUB_SERVING_CNT


def test_m3_blocks_core_dist_joins_and_in_are_global():
    # nit(12): M3_BLOCKS_CORE 각 블록 안에서 _dist 테이블을 향한 JOIN/IN은 전부 GLOBAL
    # (§4.0 분산 조인 표준) — GLOBAL LEFT JOIN이 아닌 " JOIN "이 없고, GLOBAL(NOT) IN (SELECT가
    # 아닌 "IN (SELECT"가 없다.
    for name, sql in steps.M3_BLOCKS_CORE:
        assert " JOIN " not in sql.replace("GLOBAL LEFT JOIN", ""), name
        for m in re.finditer(r"\bIN\s*\(SELECT", sql):
            prefix = sql[:m.start()]
            assert prefix.endswith("GLOBAL ") or prefix.endswith("GLOBAL NOT "), \
                (name, sql[max(0, m.start() - 40):m.end()])


# --- run_m3: _run_table 시퀀스 + 검사 요약 라인 ------------------------------------
class M3Gate(FakeGate):
    """FakeGate + M3 요약(GROUP BY check_name, severity) 조회 응답·적재 행수 고정."""

    def __init__(self, summary_rows, rows=3, **kw):
        super().__init__(**kw)
        self.summary_rows = summary_rows
        self.rows = rows
        self.deleted = []
        self.inserted = []

    def delete_day(self, table_local, date, extra_pred=""):
        self.deleted.append((table_local, date, extra_pred))
        super().delete_day(table_local, date, extra_pred)

    def insert_select(self, sql, params=None):
        self.inserted.append((sql, params))
        return self.rows

    def verify_count(self, table_dist, date, expected):
        self.verify_calls.append((table_dist, expected))
        return True, self.rows

    def query(self, sql, params=None):
        self.query_calls.append((sql, params))
        if "GROUP BY check_name, severity" in sql:
            return list(self.summary_rows)
        if sql.startswith("SELECT count() FROM ("):
            return [(self.rows,)]
        return super().query(sql, params)


def test_run_m3_appends_check_warn_lines(capsys):
    gate = M3Gate([("rows_rejected", "WARN", 2)], rows=2)
    out = steps.run_m3(gate, DATE)
    assert out["warns"] == ["CHECK WARN rows_rejected severity=WARN count=2"]
    assert out["rows_check"] == 2
    assert "CHECK WARN rows_rejected severity=WARN count=2" in capsys.readouterr().out


def test_run_m3_fail_is_warn_line_and_info_is_info_line():
    gate = M3Gate([("hours_over_count", "FAIL", 1), ("manual_source", "INFO", 4)], rows=5)
    out = steps.run_m3(gate, DATE)
    assert out["warns"] == [
        "CHECK WARN hours_over_count severity=FAIL count=1",
        "CHECK INFO manual_source severity=INFO count=4",
    ]
    assert len([w for w in out["warns"] if w.startswith("CHECK WARN ")]) == 1


def test_run_m3_sequence_delete_insert_expected_verify_summary():
    gate = M3Gate([], rows=7)
    out = steps.run_m3(gate, DATE)
    assert out == {"rows_check": 7, "warns": []}
    # FakeGate.order는 (op, short) 튜플 — M3Gate가 insert/query/verify를 덮어써 exists·delete만 기록된다
    assert gate.order == [("exists", "m3"), ("delete", "m3")]
    assert gate.deleted == [(f"{DB_MART}.token_metrics_check_1d_local", DATE, "")]
    assert len(gate.inserted) == 1
    sql, params = gate.inserted[0]
    assert sql.startswith(f"INSERT INTO {DB_MART}.token_metrics_check_1d_dist (")
    assert params == {"d": DATE}
    assert gate.verify_calls == [(f"{DB_MART}.token_metrics_check_1d_dist", 7)]
    # blocks=None 기본은 CORE + STRETCH — T6/T7이 STRETCH를 extend한 뒤에도 성립
    assert gate.query_calls[0][0] == steps.build_m3_expected(steps.M3_BLOCKS_CORE + steps.M3_BLOCKS_STRETCH)
    assert gate.query_calls[-1] == (steps.SQL_M3_SUMMARY, {"d": DATE})


def test_run_m3_blocks_default_is_core_plus_stretch(monkeypatch):
    extra = ("stretch_probe", steps._m3_select(
        "stretch_probe", "INFO", service_group="''", service="''", observed="0",
        threshold="0", detail="''", body="FROM system.one WHERE 0 AND {d:Date} = {d:Date}"))
    monkeypatch.setattr(steps, "M3_BLOCKS_STRETCH", [extra])
    gate = M3Gate([], rows=0)
    steps.run_m3(gate, DATE)
    sql = gate.inserted[0][0]
    assert sql.count("\nUNION ALL\n") == 13
    assert "'stretch_probe' AS check_name" in sql
    gate2 = M3Gate([], rows=0)
    steps.run_m3(gate2, DATE, blocks=steps.M3_BLOCKS_CORE[:1])
    assert "UNION ALL" not in gate2.inserted[0][0]


def test_run_m3_raises_step_error_when_verify_fails():
    class Failing(M3Gate):
        def verify_count(self, table_dist, date, expected):
            return False, 0

    with pytest.raises(steps.StepError):
        steps.run_m3(Failing([], rows=3), DATE)
