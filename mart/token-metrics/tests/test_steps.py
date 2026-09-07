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
