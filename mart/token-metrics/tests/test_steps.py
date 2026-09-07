"""app/steps.py 단위 테스트 — SQL 문자열 계약(서버 바인딩·컬럼 순서·비용 술어·우선순위) +
_run_table 시퀀스(FakeGate). ClickHouse 없이 돈다(SQL 실행은 T10 e2e·CI가 담당).

FakeGate는 mart/token-usage/tests/test_steps.py의 것을 복제하되 테이블 키를 mart-metrics
4테이블로 바꿨다(_TABLE_KEYS 부분 문자열 라우팅 — 서로 부분 문자열이 아니어야 함, 테스트로 고정).
"""
from __future__ import annotations

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
        # fix1 M15 (추가 전용) — exists/delete_day/verify_count에 실제로 전달된 전체 테이블명
        # (op, table_name) 그대로 기록. 기존 order/delete_preds/verify_calls·_short 라우팅은 불변
        # (M3Gate가 그대로 계속 동작).
        self.full_names = []

    def _short(self, s: str) -> str:
        for key, short in sorted(self._TABLE_KEYS, key=lambda kv: -len(kv[0])):
            if key in s:
                return short
        raise AssertionError(f"unknown table in: {s[:120]!r}")

    def exists(self, table_dist, date):
        self.order.append(("exists", self._short(table_dist)))
        self.full_names.append(("exists", table_dist))
        return self._exists

    def delete_day(self, table_local, date, extra_pred=""):
        self.order.append(("delete", self._short(table_local)))
        self.delete_preds.append((self._short(table_local), extra_pred))
        self.full_names.append(("delete", table_local))

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
        self.full_names.append(("verify", table_dist))
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


def test_sub_reg_is_unique_per_service():
    # fix1 finding 1 — dim_token_metrics_service_local은 평범한 ORDER BY (service) MergeTree라
    # 부분 재동기화로 (service) 중복 행이 남을 수 있다. EXPECTED_SQL_M1은 SUB_REG를 공유하지
    # 않으므로(키는 tok/gpu 쪽에서만 옴) 중복이 생겨도 dup_suspect로는 드러나지 않는다 — SUB_REG
    # 자체가 서비스당 1행만 반환하도록 강제해 조인 팬아웃을 원천 차단한다.
    assert "LIMIT 1 BY service" in steps.SUB_REG


def test_global_join_and_global_in_only():
    for name, sql in sql_constants().items():
        for m in re.finditer(r"\bLEFT JOIN\b", sql):
            assert sql[max(0, m.start() - 7):m.start()] == "GLOBAL ", (name, sql[m.start() - 40:m.end()])
        for m in re.finditer(r"\bIN\s*\(SELECT", sql):
            assert sql[max(0, m.start() - 7):m.start()] == "GLOBAL ", (name, sql[m.start() - 40:m.end()])
        if not name.endswith("_SUMMARY"):   # SQL_M3_SUMMARY(T4)는 단일 테이블 GROUP BY — 서브쿼리 없음
            assert "GLOBAL IN" in sql, name
        stripped = sql.replace("GLOBAL LEFT JOIN", "")
        assert re.search(r"(^|\s)JOIN\s", stripped, re.M) is None, name   # INNER/CROSS 금지(줄 시작 JOIN 포함)


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
    assert [n for n, _ in steps.M3_BLOCKS_STRETCH][:3] == [
        "provider_ambiguous", "vendor_price_missing", "consumer_tokens_exceed_provider"]


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
    # fix1 SHOULD-2: stretch 블록(T6)도 core와 같은 규율을 지킨다 — CORE + STRETCH 전부 순회.
    for name, sql in steps.M3_BLOCKS_CORE + steps.M3_BLOCKS_STRETCH:
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


def test_m3_blocks_dist_joins_and_in_are_global():
    # nit(12): M3 블록 각각 안에서 _dist 테이블을 향한 JOIN/IN은 전부 GLOBAL
    # (§4.0 분산 조인 표준) — GLOBAL LEFT JOIN이 아닌 " JOIN "이 없고, GLOBAL(NOT) IN (SELECT가
    # 아닌 "IN (SELECT"가 없다.
    # fix1 SHOULD-2: stretch 블록(T6)도 core와 같은 규율을 지킨다 — CORE + STRETCH 전부 순회.
    for name, sql in steps.M3_BLOCKS_CORE + steps.M3_BLOCKS_STRETCH:
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


# --- T3 fix round 1: 의미 고정 테스트 ---
# 컨트롤러 리뷰(90aaf60) fix round 1 — 뮤테이션 스윕이 잡아내지 못한 6종(M7/M8/M9/M13/M15/M16)을
# 개별 고정한다. 여기부터는 새 테스트만 추가하고 기존 T3/M3 단언은 건드리지 않는다(M3 리전은 T4 소유).

def test_m1_token_side_date_filter_and_binding_counts():
    # M7 — u.date = {d:Date} 게이트가 사라지면 그날 전체가 아니라 누적 전체를 읽어버린다.
    assert "WHERE u.date = {d:Date}" in steps.SQL_M1
    assert steps.SQL_M1.count("{d:Date}") == 17
    assert steps.EXPECTED_SQL_M1.count("{d:Date}") == 5


def test_m1_gpu_rows_gated_by_anchor_presence():
    # M13 — 앵커 없는 서비스의 gpu 행이 게이트 없이 들어오면 검증되지 않은 데이터가 섞인다.
    frag = "g.service GLOBAL IN (SELECT service FROM"
    assert frag in steps.SQL_M1
    assert frag in steps.EXPECTED_SQL_M1


def test_m1_outer_join_keys_pair_same_named_columns():
    # M8/M9 — 조인 키가 뒤바뀌면(예: an.service = k.model) 전부 미스가 나거나 오조인된다.
    sql = steps.SQL_M1
    assert "GLOBAL LEFT JOIN tok_agg AS tk ON tk.service = k.service AND tk.model = k.model" in sql
    assert "GLOBAL LEFT JOIN gpu_agg AS ga ON ga.service = k.service AND ga.model = k.model" in sql
    assert f"GLOBAL LEFT JOIN {steps.SUB_ANCHOR} AS an ON an.service = k.service" in sql
    assert f"GLOBAL LEFT JOIN {steps.SUB_REG} AS r ON r.service = k.service" in sql
    for bad in ("an.service = k.model", "r.service = k.model",
                "tk.service = k.model", "ga.service = k.model",
                "tk.model = k.service", "ga.model = k.service"):
        assert bad not in sql, bad


def test_run_m1_uses_dist_for_exists_verify_and_local_for_delete():
    # M15 — run_m1이 dist/local을 뒤바꿔 넘기면 exists/verify가 로컬 테이블을 보거나 DELETE가
    # ON CLUSTER 분산 테이블을 때리게 된다.
    g = FakeGate()
    steps.run_m1(g, DATE)
    prefix = f"{DB_MART}.{steps.T_M1}"
    exists_names = [name for op, name in g.full_names if op == "exists"]
    delete_names = [name for op, name in g.full_names if op == "delete"]
    verify_names = [name for op, name in g.full_names if op == "verify"]
    assert exists_names == [f"{prefix}_dist"]
    assert delete_names == [f"{prefix}_local"]
    assert verify_names == [f"{prefix}_dist"]


def test_run_table_forwards_extra_pred_to_delete_day():
    # M16 — _run_table이 extra_pred를 delete_day에 넘기지 않으면 공유 테이블 DELETE가
    # created_by 등 소유자 조건 없이 전체 삭제된다.
    g = FakeGate()
    warns: list = []
    dist = f"{DB_MART}.{steps.T_M1}_dist"
    local = f"{DB_MART}.{steps.T_M1}_local"
    sql = f"SELECT 1 /* {steps.T_M1} */"
    expected_sql = f"SELECT 1 /* {steps.T_M1} */"
    steps._run_table(g, DATE, dist, local, sql, expected_sql, warns,
                      extra_pred="created_by = 'x'")
    assert g.delete_preds == [("m1", "created_by = 'x'")]


# --- T4 fix round 1: 등록부 부재/커버리지 창·검출 술어 고정·따옴표 가드 ---
# 컨트롤러 리뷰(86682d4) fix round 1 findings 1~4. M3 리전 테스트만 추가한다(T3 fix round 1
# 섹션 다음에 이어붙임 — 기존 T3/T4 단언은 건드리지 않는다).

def test_m3_identity_drift_excludes_service_absent_from_registry():
    # finding 1 — 등록부에서 빠진 서비스는(조인 미스 → join_use_nulls=0으로 service_group='')
    # 비교할 대상이 없으므로 identity_drift 오탐을 내지 않는다.
    sql = " ".join(dict(steps.M3_BLOCKS_CORE)["identity_drift"].split())
    assert "r.service != ''" in sql


def test_m3_service_not_in_usage_registry_reuses_metrics_missing_coverage_window():
    # finding 2 — 커버리지 창(coverage_since/until)은 레지스트리 기반 모든 기대의 공통 게이트다.
    # metrics_missing이 쓰는 것과 정확히 같은 두 술어를 재사용한다(공백 정규화 비교).
    missing = " ".join(dict(steps.M3_BLOCKS_CORE)["metrics_missing"].split())
    reg_gap = " ".join(dict(steps.M3_BLOCKS_CORE)["service_not_in_usage_registry"].split())
    for pred in ("r.coverage_since <= {d:Date}", "(isNull(r.until) OR {d:Date} <= r.until)"):
        assert pred in missing, pred
        assert pred in reg_gap, pred


def test_m3_flag_and_source_predicates_pinned():
    # finding 3 — 뮤테이션 스윕이 잡아내지 못하는 6개 검출 술어를 블록별로 고정한다.
    blocks = {name: " ".join(sql.split()) for name, sql in steps.M3_BLOCKS_CORE}
    pinned = {
        "hours_over_count": "hasAny(g.flags, ['hours_over_count'])",
        "unknown_violation": "hasAny(g.flags, ['unknown_violation'])",
        "pct_non_monotone": "hasAny(s.flags, ['pct_non_monotone'])",
        "serving_without_gpu_serving_row": "r.expect_gpu = 1",
        "manual_source": "an.source_type = 'manual-v0'",
        "serving_missing_for_gpu_model": "sk.has_rows = 0 AND tk.requests > 0",
    }
    for name, pred in pinned.items():
        assert pred in blocks[name], name


def test_m3_select_rejects_quote_in_check_name_or_severity():
    # finding 4 — check_name/severity를 단일따옴표 SQL 리터럴로 그대로 보간하므로, 따옴표가
    # 섞이면 생성된 SQL 문자열 자체가 깨진다(가드 없으면 조용히 잘못된 SQL을 만든다).
    with pytest.raises(ValueError):
        steps._m3_select("x'y", "WARN", service_group="''", service="''", observed="0",
                         threshold="0", detail="''", body="FROM system.one")


# ============================================================================
# M4 agg_token_model_share_1d — 분모 6모드·외부 API 단가·M1 산출 소비 (Plan 6c T6)
# ============================================================================
from app.mart import DENOMINATOR_MODES  # noqa: E402 — T6 import (T2 상수)

M4_DATE = "2026-09-01"
M4_STRETCH_NAMES = ["provider_ambiguous", "vendor_price_missing", "consumer_tokens_exceed_provider"]


def test_m4_insert_column_list_matches_ddl_order():
    cols = ddl_columns("agg_token_model_share_1d_local")
    assert len(cols) == 14
    assert cols == ["date", "model", "service", "service_group", "provider_service", "is_provider",
                    "denominator_mode", "service_wtokens", "model_total_wtokens", "share",
                    "model_cost_krw", "allocated_cost_krw", "quality_flag", "created_by"]
    assert insert_columns(steps.SQL_M4) == cols
    outer = steps.SQL_M4[steps.SQL_M4.rindex("\nSELECT\n"):steps.SQL_M4.index("\nFROM keys AS k")]
    aliases = re.findall(r"\bAS (\w+)\s*,?\s*$", outer, re.M)
    assert aliases == cols
    assert re.search(r"'token-metrics-pipeline'\s+AS created_by", steps.SQL_M4)


def test_m4_denominator_modes_all_six_literals_present():
    assert DENOMINATOR_MODES == ("all_services", "provider_reported", "token_not_reported",
                                 "no_provider", "provider_ambiguous", "external_api")
    for m in DENOMINATOR_MODES:
        assert f"'{m}'" in steps.SQL_M4, m
    # 판정 multiIf(mode CTE) 분기 순서 = 설계 §6.4 (4): ambiguous > no_provider > external > tnr > reported > all
    seg = steps.SQL_M4[steps.SQL_M4.index("AS uic"):steps.SQL_M4.index("AS denominator_mode")]
    order = ["'provider_ambiguous'", "'no_provider'", "'external_api'", "'token_not_reported'",
             "'provider_reported'", "'all_services'"]
    positions = [seg.index(tok) for tok in order]
    assert positions == sorted(positions)
    assert "n_prov >= 2," in seg
    assert "n_prov = 0 AND has_gpu = 1," in seg
    assert "w_m = 0 AND ifNull(model_cost_krw, 0) > 0," in seg
    assert "uic = 1," in seg
    assert "if(uic = 1, greatest(w_prov, w_all - w_prov), w_all)" in seg  # C4: provider_reported 분모 보정


def test_m4_provider_reported_denominator_uses_greatest_c4():
    """컨트롤러 결정 C4 — provider_reported 분모는 w_p가 아니라 greatest(w_p, w_all - w_p)
    (소비자 W 합이 제공자 자기보고를 초과해도 Sigma share = 1을 유지). numerator(제공자 자기분
    greatest(w.wtok - (md.w_all - w.wtok), 0.0))는 그대로(test_m4_weight_expr_shared_with_m1)."""
    snippet = "if(uic = 1, greatest(w_prov, w_all - w_prov), w_all)"
    assert " ".join(snippet.split()) in " ".join(steps.SQL_M4.split())


def test_m4_reads_m1_output_not_fact_for_cost():
    assert f"{DB_MART}.agg_token_model_cost_1d_dist" in steps.SQL_M4
    assert "has_gpu_rows = 1" in steps.SQL_M4
    assert "tco_krw_per_gpu_hour" not in steps.SQL_M4
    assert steps.SUB_EFF_TCO not in steps.SQL_M4
    assert "dim_token_gpu_tco" not in steps.SQL_M4
    # 제공자(candidate) 행 = FAIL 없는 serving/standby gpu 행(설계 §6.4 (4)) — test·FAIL 행은 후보가 아니다
    assert "g.category IN ('serving', 'standby') AND NOT " + steps.FAIL_PRED in steps._M4_PROV_ROWS
    assert steps._M4_PROV_ROWS in steps.SQL_M4


def test_m4_external_api_formula_divides_by_1e6_and_tier_standard():
    sql = steps.SQL_M4
    assert "/ 1e6" in sql
    assert "tier = 'standard'" in sql
    assert steps.SUB_EFF_PRICE in sql
    for p in ("p_in", "p_cached", "p_cc", "p_out"):
        assert f"AS {p}" in sql, p
    # D8: 공백 결합 없는 정규화 비교(간접 산식 배치는 자유)
    formula = ("(w.input_tokens * md.p_in + w.cache_read_tokens * md.p_cached "
               "+ w.cache_creation_tokens * md.p_cc + w.output_tokens * md.p_out) / 1e6")
    assert " ".join(formula.split()) in " ".join(sql.split())
    # 단가 행 부재/NULL → allocated NULL → vendor_price_missing (모델별 벤더 1행 — fan-out 방지)
    assert "nullIf(argMin(ifNull(p_in, -1), provider), -1)" in steps._M4_VENDOR
    # 집계 별칭이 소스 컬럼 provider를 가리면 argMin(…, provider)가 중첩 집계가 된다 — 별칭은 vendor
    assert "min(provider) AS vendor" in re.sub(r"\s+", " ", steps._M4_VENDOR)
    assert "AS provider" not in steps._M4_VENDOR
    assert "v.vendor AS vendor" in re.sub(r"\s+", " ", steps.SQL_M4)
    assert "md.denominator_mode = 'external_api' AND isNull(allocated_cost_krw)" in sql
    # 사외 API 행의 provider_service = 벤더 표기(없으면 '')
    assert "md.denominator_mode = 'external_api', md.vendor," in steps._M4_WT_KEYS


def test_m4_weight_expr_shared_with_m1():
    assert steps._WTOK_EXPR in steps.SQL_M1
    assert steps._WTOK_EXPR in steps.SQL_M4
    # D8: 공백 결합 없는 정규화 비교
    assert " ".join((steps._WTOK_EXPR + " AS wtok").split()) in " ".join(steps._M4_WT.split())
    assert (W_UNC, W_CACHE, W_OUT) == (1.0, 0.1, 4.0)
    # provider_reported 제공자 자기분 = max(W(p) − Σ_{s≠p} W(s), 0) (설계 §6.4 (4) 분모 모드 보정)
    assert "greatest(w.wtok - (md.w_all - w.wtok), 0.0)" in steps.SQL_M4
    assert "k.is_provider = 1 AND md.denominator_mode = 'provider_reported'" in steps.SQL_M4


def test_m4_quality_priority_order():
    sql = steps.SQL_M4
    order = ["'partial'", "'no_tco'", "'provider_ambiguous'", "'vendor_price_missing'",
             "'token_not_reported'", "'normal'"]
    qf = sql[sql.rindex("multiIf(", 0, sql.index("AS quality_flag")):sql.index("AS quality_flag")]
    positions = [qf.index(tok) for tok in order]
    assert positions == sorted(positions)
    assert "mc.quality_flag = 'partial'" in qf and "mc.quality_flag = 'no_tco'" in qf
    # share/allocated 특례(설계 §6.1 M4): ambiguous NULL, token_not_reported 제공자 행 1·전액, 분모 0 NULL
    # D8: 공백 결합 없는 정규화 비교
    share_formula = ("multiIf(md.denominator_mode = 'provider_ambiguous', NULL, "
                     "md.denominator_mode = 'token_not_reported', if(k.is_provider = 1, 1.0, NULL), "
                     "md.w_m = 0, NULL, "
                     "service_wtokens / md.w_m)")
    assert " ".join(share_formula.split()) in " ".join(sql.split())
    assert "md.denominator_mode = 'token_not_reported', if(k.is_provider = 1, model_cost_krw, NULL)," in sql
    assert "model_cost_krw * share)" in sql
    # fix1 SHOULD-4: 이 리터럴은 model_cost_krw·allocated_cost_krw 두 multiIf 분기에 각각
    # 등장한다(twin-string) — 한쪽이 지워져도 in 단언은 살아남으므로 개수까지 고정한다.
    assert sql.count("md.denominator_mode = 'no_provider', toNullable(0.0),") == 2


def test_m4_expected_key_tuple():
    assert "uniqExact((model, service, provider_service))" in steps.EXPECTED_SQL_M4
    assert steps._M4_CTES in steps.SQL_M4 and steps._M4_CTES in steps.EXPECTED_SQL_M4
    for frag in (steps._M4_WT_KEYS, steps._M4_PROV_KEYS):
        assert frag in steps.SQL_M4
        assert frag in steps.EXPECTED_SQL_M4
    assert "UNION DISTINCT" in steps.SQL_M4
    assert "\n    UNION ALL\n" in steps.EXPECTED_SQL_M4
    assert "INSERT INTO" not in steps.EXPECTED_SQL_M4
    for x in ("u.model", "g.model"):
        assert steps.canon(x) in steps.SQL_M4 and steps.canon(x) in steps.EXPECTED_SQL_M4
    # 서브쿼리 조각 재사용(설계 Consumes): 단가·레지스트리·앵커·usage_svc·alias
    for name in ("SUB_EFF_PRICE", "SUB_REG", "SUB_ANCHOR", "SUB_USAGE_SVC", "SUB_EFF_ALIAS"):
        assert getattr(steps, name) in steps.SQL_M4, name
    assert "ARRAY JOIN" not in steps.SQL_M4 and "NOT IN (" not in steps.SQL_M4


def test_run_m4_returns_rows_share_from_verify_actual_and_routes_to_m4():
    g = FakeGate(expected_overrides={"m4": 9})
    out = steps.run_m4(g, M4_DATE)
    assert out == {"rows_share": 9, "warns": []}
    # SQL_M4는 M1 테이블명도 포함(m1c) — FakeGate는 가장 긴 키(agg_token_model_share_1d)로 m4 라우팅
    assert g.order == [("exists", "m4"), ("delete", "m4"), ("insert", "m4"), ("query", "m4"), ("verify", "m4")]
    assert g.verify_calls == [("m4", M4_DATE, 9)]
    assert g.written[0][1] is steps.SQL_M4
    assert g.query_calls[0][1] is steps.EXPECTED_SQL_M4
    assert g.delete_preds == [("m4", "")]


def test_run_m4_dup_suspect_warn_and_step_error():
    g = FakeGate(expected_overrides={"m4": 4}, verify_actual=5)
    out = steps.run_m4(g, M4_DATE)
    assert out["rows_share"] == 5
    assert out["warns"] == [f"dup_suspect:{DB_MART}.agg_token_model_share_1d_dist"]
    with pytest.raises(steps.StepError):
        steps.run_m4(FakeGate(verify_ok=False), M4_DATE)


def test_run_m4_uses_dist_for_exists_verify_and_local_for_delete():
    # fix1 SHOULD-3 — run_m1과 같은 M15 규율: run_m4가 dist/local을 뒤바꿔 넘기면 exists/verify가
    # 로컬 테이블을 보거나 DELETE가 ON CLUSTER 분산 테이블을 때리게 된다.
    g = FakeGate()
    steps.run_m4(g, M4_DATE)
    prefix = f"{DB_MART}.{steps.T_M4}"
    exists_names = [name for op, name in g.full_names if op == "exists"]
    delete_names = [name for op, name in g.full_names if op == "delete"]
    verify_names = [name for op, name in g.full_names if op == "verify"]
    assert exists_names == [f"{prefix}_dist"]
    assert delete_names == [f"{prefix}_local"]
    assert verify_names == [f"{prefix}_dist"]


def test_m4_surviving_mutation_exact_strings():
    """fix1 SHOULD-5 — 뮤테이션 테스트에서 살아남는 핵심 상수 문자열 4종을 개별 고정한다:
    (1) model_total_wtokens 출력 = md.w_m, (2) 소비자 행도 제공자의 M1 비용을 읽는 outer m1c 조인,
    (3) provider_service 키의 제공자 판별식, (4) _M4_M1C의 date 바인딩 술어(조각 자체에서 확인)."""
    sql = steps.SQL_M4
    assert "md.w_m                                                            AS model_total_wtokens," in sql
    assert "GLOBAL LEFT JOIN m1c AS mc ON mc.model = k.model AND mc.service = k.provider_service" in sql
    assert "toUInt8(md.n_prov = 1 AND w.service = md.provider)" in steps._M4_WT_KEYS
    assert "WHERE date = {d:Date} AND has_gpu_rows = 1" in steps._M4_M1C


# --- M3 stretch 3블록 (share 경고) ---------------------------------------------------

def test_m3_stretch_names_after_t6():
    assert [n for n, _ in steps.M3_BLOCKS_STRETCH][:3] == M4_STRETCH_NAMES
    blocks = steps.M3_BLOCKS_CORE + steps.M3_BLOCKS_STRETCH
    assert len(blocks) >= 16
    sql = steps.build_m3_sql(blocks)
    assert sql.count("\nUNION ALL\n") == len(blocks) - 1
    assert len(set(n for n, _ in blocks)) == len(blocks)      # 이름 중복 없음(core와 겹치지 않음)
    for name in M4_STRETCH_NAMES:
        assert name not in M3_CORE_NAMES


def test_m3_stretch_blocks_follow_core_discipline():
    stretch = dict(steps.M3_BLOCKS_STRETCH)
    for name in M4_STRETCH_NAMES:
        sql = stretch[name]
        assert sql.startswith("SELECT\n"), name
        assert _m3_select_header_aliases(sql) == list(steps.M3_COLUMNS), name
        assert f"'{name}' AS check_name" in sql, name
        assert "'WARN' AS severity" in sql, name
        assert "    {d:Date} AS date," in sql, name
        assert "'token-metrics-pipeline' AS created_by" in sql, name
        assert "\nUNION ALL\n" not in sql and "\nUNION DISTINCT\n" not in sql, name
        assert "coalesce(" not in sql.lower() and "SELECT *" not in sql, name
        assert "%(" not in sql, name
        header = sql.split("\nFROM", 1)[0]
        model_line = next(ln for ln in header.splitlines() if ln.endswith(" AS model,"))
        assert "''" not in model_line, name                     # 모델 단위 검사 — model 컬럼 채움
        gpu_line = next(ln for ln in header.splitlines() if ln.endswith(" AS gpu_type,"))
        assert gpu_line.strip() == "'' AS gpu_type,", name
        assert "concat('model=', " in sql, name
        assert "reported_" not in sql.split("\nFROM", 1)[0], name   # detail에 응답 원문 없음(§5.6)


def test_m3_provider_ambiguous_block_uses_m4_provider_rows():
    sql = dict(steps.M3_BLOCKS_STRETCH)["provider_ambiguous"]
    assert steps._M4_PROV in sql
    assert "WHERE p.n_prov >= 2" in sql
    assert "toNullable(toFloat64(p.n_prov)) AS observed" in sql
    assert "toNullable(toFloat64(1)) AS threshold" in sql


def test_m3_vendor_price_missing_block_external_api_only():
    sql = dict(steps.M3_BLOCKS_STRETCH)["vendor_price_missing"]
    assert steps._M4_GPU_ANY in sql              # gpu 행이 전혀 없는 모델만(no_provider 미발화)
    assert steps._M4_VENDOR in sql
    assert "WHERE ga.has_gpu = 0" in sql
    assert ("(v.has_price = 0 OR isNull(v.p_in) OR isNull(v.p_cached)"
            " OR isNull(v.p_cc) OR isNull(v.p_out))") in sql
    assert f"u.service GLOBAL IN {steps.SUB_USAGE_SVC}" in sql


def test_m3_consumer_tokens_exceed_provider_block_provider_reported_only():
    sql = dict(steps.M3_BLOCKS_STRETCH)["consumer_tokens_exceed_provider"]
    assert steps._M4_PROV in sql and steps._M4_WT in sql and steps._M4_WT_TOTAL in sql
    assert "r.usage_includes_consumers = 1" in sql
    assert "WHERE p.n_prov = 1" in sql
    assert "(t.w_all - wp.wtok) > wp.wtok" in sql
    assert "toNullable(toFloat64(t.w_all - wp.wtok)) AS observed" in sql
    assert "toNullable(toFloat64(wp.wtok)) AS threshold" in sql


def test_run_m3_default_includes_t6_stretch_blocks():
    gate = M3Gate([], rows=1)
    steps.run_m3(gate, M4_DATE)
    inserted_sql = gate.inserted[0][0]
    for name in M4_STRETCH_NAMES:
        assert f"'{name}' AS check_name" in inserted_sql, name


# ============================================================================
# T7 — M2 agg_token_gpu_group_1d(할당×24·idle 클램프·identity_gap·unattributed=flagged+other) + M3 stretch 4블록(20블록 완성)
# ============================================================================

M2_DATE = "2026-09-03"
M3_STRETCH_NAMES_T7 = ["provider_ambiguous", "vendor_price_missing", "consumer_tokens_exceed_provider",
                       "no_allocation", "sum_hours_over_allocation",
                       "gpu_block_empty_unexpected", "serving_block_empty_unexpected"]
T7_GROUP_BLOCKS = {"no_allocation", "sum_hours_over_allocation"}          # 그룹×기종 단위(gpu_type 채움)
T7_ANCHOR_BLOCKS = {"gpu_block_empty_unexpected", "serving_block_empty_unexpected"}   # 서비스 단위


def _m2_outer_select() -> str:
    return steps.SQL_M2[steps.SQL_M2.rindex("\nSELECT\n"):steps.SQL_M2.index("\nFROM keys AS k")]


def test_m2_insert_column_list_matches_ddl_order():
    cols = ddl_columns("agg_token_gpu_group_1d_local")
    assert len(cols) == 23                                   # Plan 6a DDL 정본(설계 §6.1 컬럼 목록 23)
    assert cols[:3] == ["date", "service_group", "gpu_type"] and cols[-1] == "created_by"
    assert insert_columns(steps.SQL_M2) == cols
    aliases = re.findall(r"\bAS (\w+)\s*,?\s*$", _m2_outer_select(), re.M)
    assert aliases == cols
    assert re.search(r"'token-metrics-pipeline'\s+AS created_by", steps.SQL_M2)
    assert steps.SQL_M2.lstrip().startswith(f"INSERT INTO {DB_MART}.agg_token_gpu_group_1d_dist")


def test_m2_allocated_hours_is_count_times_24():
    assert "al.allocated_gpu_count * 24" in steps.SQL_M2
    assert re.search(r"allocated_gpu_count \* 24\s+AS allocated_gpu_hours", steps.SQL_M2)
    assert "allocated_gpu_hours * t.tco" in steps.SQL_M2       # 그룹 총비용 = 할당 × TCO (정의서 3.4)
    assert "reported_gpu_hours_total / 24" in steps.SQL_M2     # equiv_gpu_count


def test_m2_idle_clamped_with_greatest_zero():
    assert "greatest(" in steps.SQL_M2
    assert "- reported_gpu_hours_total, 0)" in steps.SQL_M2
    assert "reported_gpu_hours_total > allocated_gpu_hours" in steps.SQL_M2          # over_report (I1)
    assert re.search(r"toUInt8\(ifNull\(reported_gpu_hours_total > allocated_gpu_hours, 0\)\)\s+AS over_report", steps.SQL_M2)
    assert re.search(r"if\(isNull\(allocated_gpu_hours\), NULL,\s+greatest\(allocated_gpu_hours - reported_gpu_hours_total, 0\)\)\s+AS idle_gpu_hours",
                     steps.SQL_M2)
    assert "idle_gpu_hours * t.tco" in steps.SQL_M2


def test_m2_identity_gap_from_loaded_columns():
    # I2: gap = group_total − model_cost_sum − test_cost − idle_cost − unattributed — 적재되는 별칭끼리 계산
    assert ("group_total_cost_krw - model_cost_sum_krw - test_cost_krw - idle_cost_krw - unattributed_cost_krw"
            in steps.SQL_M2)
    assert "(serving_gpu_hours + standby_gpu_hours) * t.tco" in steps.SQL_M2   # Σ C = (serving+standby)×TCO
    assert "test_gpu_hours * t.tco" in steps.SQL_M2                            # 실험 비용(그룹 귀속)
    # R1(scan-B D3): unattributed = (flagged + other) × TCO — other = 비FAIL 행 중 category ∉ {serving,standby,test}
    assert "(flagged_gpu_hours + gp.other_gpu_hours) * t.tco" in steps.SQL_M2
    assert "flagged_gpu_hours * t.tco" not in steps.SQL_M2
    assert re.search(r"toUInt8\(isNull\(t\.tco\)\)\s+AS tco_missing", steps.SQL_M2)
    assert "reported_gpu_hours_total / allocated_gpu_hours" in steps.SQL_M2    # utilization
    assert "allocated_gpu_hours = 0, NULL" in steps.SQL_M2                     # 0 할당 → NULL(0 나눗셈 방지)


def test_m2_cost_from_fact_by_gpu_type_not_m1():
    assert "agg_token_model_cost_1d" not in steps.SQL_M2
    assert "agg_token_model_share_1d" not in steps.SQL_M2 and "token_metrics_check_1d" not in steps.SQL_M2
    assert "tco" in steps.SQL_M2
    assert steps.SUB_EFF_TCO in steps.SQL_M2
    assert f"{DB_FACT}.raw_token_metrics_gpu_1d_dist AS g" in steps.SQL_M2
    assert "token_usage_1d" not in steps.SQL_M2                # 토큰 측 무관(GPU-only 테이블)


def test_m2_excludes_unknown_gpu_type_allocation():
    assert "gpu_type != 'unknown'" in steps.SUB_EFF_ALLOC and steps.SUB_EFF_ALLOC in steps.SQL_M2
    # alloc 키 = 앵커 서비스가 있는 그룹만 (설계 §6.1 "그룹 내 서비스 앵커 ≥1")
    assert f"al.service_group GLOBAL IN (SELECT service_group FROM {steps.SUB_ANCHOR})" in steps.SQL_M2
    assert f"g.service GLOBAL IN {steps._M3_ANCHORED}" in steps.SQL_M2


def test_m2_hours_five_way_split_and_fail_pred():
    grp = steps._M2_GRP
    assert grp in steps.SQL_M2
    assert f"sumIf(g.gpu_hours, g.category = 'serving' AND NOT {steps.FAIL_PRED})" in grp
    assert f"sumIf(g.gpu_hours, g.category = 'standby' AND NOT {steps.FAIL_PRED})" in grp
    assert f"sumIf(g.gpu_hours, g.category = 'test' AND NOT {steps.FAIL_PRED})" in grp
    assert "sum(g.gpu_hours)" in grp and "AS reported_gpu_hours_total" in grp   # 플래그 포함 전체
    assert f"sumIf(g.gpu_hours, {steps.FAIL_PRED})" in grp and "AS flagged_gpu_hours" in grp
    # R1(scan-B D3): other = 비FAIL 행 중 category ∉ {serving,standby,test} — unattributed_cost_krw에 합류
    assert (f"sumIf(g.gpu_hours, g.category NOT IN ('serving', 'standby', 'test') AND NOT {steps.FAIL_PRED})"
            in grp and "AS other_gpu_hours" in grp)
    assert "GROUP BY g.service_group, g.gpu_type" in grp


def test_m2_quality_priority_order():
    sql = steps.SQL_M2
    order = [sql.index(f"'{f}'") for f in ("over_report", "no_tco", "no_allocation", "flagged", "normal")]
    assert order == sorted(order)
    assert "multiIf(" in sql and "'normal')" in sql
    assert "'partial'" not in sql and "'manual'" not in sql       # M1 전용 플래그 없음


def test_m2_expected_key_tuple():
    assert "uniqExact((service_group, gpu_type))" in steps.EXPECTED_SQL_M2
    assert "\n    UNION ALL\n" in steps.EXPECTED_SQL_M2
    assert "UNION DISTINCT" in steps.SQL_M2
    for frag in (steps._M2_GPU_KEYS, steps._M2_ALLOC_KEYS):
        assert frag in steps.SQL_M2 and frag in steps.EXPECTED_SQL_M2
    assert "{d:Date}" in steps.EXPECTED_SQL_M2 and "GLOBAL IN" in steps.EXPECTED_SQL_M2


def test_run_m2_returns_rows_group_from_verify_actual_and_routes_to_m2():
    gate = FakeGate(exists=True, verify_actual=4, expected_overrides={"m2": 4})
    out = steps.run_m2(gate, M2_DATE)
    assert out == {"rows_group": 4, "warns": []}
    assert gate.order == [("exists", "m2"), ("delete", "m2"), ("insert", "m2"), ("query", "m2"), ("verify", "m2")]
    assert gate.delete_preds == [("m2", "")]
    assert gate.written[0][1] == steps.SQL_M2 and gate.written[0][2] == {"d": M2_DATE}
    assert gate.query_calls[0][1] == steps.EXPECTED_SQL_M2
    assert gate.verify_calls == [("m2", M2_DATE, 4)]


def test_run_m2_zero_rows_day_is_success_and_dup_or_verify_paths():
    empty = FakeGate(exists=False, verify_actual=0, expected_overrides={"m2": 0})
    assert steps.run_m2(empty, M2_DATE) == {"rows_group": 0, "warns": []}   # gpu·할당 모두 없는 날
    assert ("delete", "m2") not in empty.order
    dup = FakeGate(exists=True, verify_actual=5, expected_overrides={"m2": 3})
    out = steps.run_m2(dup, M2_DATE)
    assert out["rows_group"] == 5 and out["warns"] == [f"dup_suspect:{DB_MART}.agg_token_gpu_group_1d_dist"]
    with pytest.raises(steps.StepError):
        steps.run_m2(FakeGate(exists=True, verify_ok=False, verify_actual=1), M2_DATE)


def test_run_m2_uses_dist_for_exists_verify_and_local_for_delete():
    # fix1 MUST-1 — run_m1/run_m4와 같은 M15 규율: run_m2가 dist/local을 뒤바꿔 넘기면 exists/verify가
    # 로컬 테이블을 보거나 DELETE가 ON CLUSTER 분산 테이블을 때리게 된다.
    g = FakeGate()
    steps.run_m2(g, M2_DATE)
    prefix = f"{DB_MART}.{steps.T_M2}"
    exists_names = [name for op, name in g.full_names if op == "exists"]
    delete_names = [name for op, name in g.full_names if op == "delete"]
    verify_names = [name for op, name in g.full_names if op == "verify"]
    assert exists_names == [f"{prefix}_dist"]
    assert delete_names == [f"{prefix}_local"]
    assert verify_names == [f"{prefix}_dist"]


def test_m3_stretch_seven_names_after_t7():
    assert [n for n, _ in steps.M3_BLOCKS_STRETCH] == M3_STRETCH_NAMES_T7
    blocks = steps.M3_BLOCKS_CORE + steps.M3_BLOCKS_STRETCH
    assert len(blocks) == 20
    assert len(set(n for n, _ in blocks)) == 20
    assert steps.build_m3_sql(blocks).count("\nUNION ALL\n") == 19
    assert steps.build_m3_expected(blocks).count("\nUNION ALL\n") == 19
    stretch = dict(steps.M3_BLOCKS_STRETCH)
    assert "'FAIL' AS severity" in stretch["sum_hours_over_allocation"]
    for name in ("no_allocation", "gpu_block_empty_unexpected", "serving_block_empty_unexpected"):
        assert "'WARN' AS severity" in stretch[name], name


def test_m3_t7_blocks_follow_core_discipline():
    stretch = dict(steps.M3_BLOCKS_STRETCH)
    for name in T7_GROUP_BLOCKS | T7_ANCHOR_BLOCKS:
        sql = stretch[name]
        assert sql.startswith("SELECT\n"), name
        assert _m3_select_header_aliases(sql) == list(steps.M3_COLUMNS), name
        assert f"'{name}' AS check_name" in sql, name
        assert "    {d:Date} AS date," in sql, name
        assert "'token-metrics-pipeline' AS created_by" in sql, name
        assert "\nUNION ALL\n" not in sql and "\nUNION DISTINCT\n" not in sql, name
        assert "coalesce(" not in sql.lower() and "SELECT *" not in sql and "%(" not in sql, name
        header = sql.split("\nFROM", 1)[0]
        assert next(ln for ln in header.splitlines() if ln.endswith(" AS model,")).strip() == "'' AS model,", name
        gpu_line = next(ln for ln in header.splitlines() if ln.endswith(" AS gpu_type,")).strip()
        svc_line = next(ln for ln in header.splitlines() if ln.endswith(" AS service,")).strip()
        if name in T7_GROUP_BLOCKS:
            assert gpu_line == "x.gpu_type AS gpu_type," and svc_line == "'' AS service,", name
            assert "x.service_group AS service_group," in sql and "concat('gpu_type=', x.gpu_type) AS detail" in sql, name
            assert steps._M2_GRP in sql and steps.SUB_EFF_ALLOC in sql, name
            assert "toNullable(toFloat64(x.reported_gpu_hours_total)) AS observed" in sql, name
        else:
            assert gpu_line == "'' AS gpu_type," and svc_line == "an.service AS service,", name
            assert "an.service_group AS service_group," in sql and "an.source_type AS source_type," in sql, name
            assert steps.SUB_ANCHOR in sql and steps.SUB_REG in sql, name
            assert "toNullable(toFloat64(1)) AS threshold" in sql, name
        assert "reported_service" not in header, name          # detail/헤더에 응답 원문 없음(§5.6)


def test_m3_no_allocation_and_over_allocation_predicates():
    stretch = dict(steps.M3_BLOCKS_STRETCH)
    no_alloc = stretch["no_allocation"]
    assert "WHERE isNull(al.allocated_gpu_count)" in no_alloc            # = M2 allocated_gpu_hours NULL
    assert "toNullable(toFloat64(0)) AS threshold" in no_alloc
    over = stretch["sum_hours_over_allocation"]
    assert "WHERE x.reported_gpu_hours_total > al.allocated_gpu_count * 24" in over
    assert "toNullable(toFloat64(al.allocated_gpu_count * 24)) AS threshold" in over
    for sql in (no_alloc, over):
        assert "GLOBAL LEFT JOIN" in sql and "al.service_group = x.service_group AND al.gpu_type = x.gpu_type" in sql


def test_m3_block_empty_unexpected_pair_uses_registry_expectation():
    stretch = dict(steps.M3_BLOCKS_STRETCH)
    gpu = stretch["gpu_block_empty_unexpected"]
    assert "WHERE r.expect_gpu = 1 AND an.gpu_rows = 0" in gpu
    assert "'expect_gpu=1' AS detail" in gpu and "toNullable(toFloat64(an.gpu_rows)) AS observed" in gpu
    serving = stretch["serving_block_empty_unexpected"]
    assert "WHERE r.expect_serving = 1 AND an.serving_rows = 0" in serving
    assert "'expect_serving=1' AS detail" in serving and "toNullable(toFloat64(an.serving_rows)) AS observed" in serving
    for sql in (gpu, serving):
        assert f"FROM {steps.SUB_ANCHOR} AS an" in sql
        assert f"GLOBAL LEFT JOIN {steps.SUB_REG} AS r ON r.service = an.service" in sql


def test_run_m3_default_includes_t7_stretch_blocks():
    gate = M3Gate([], rows=1)
    steps.run_m3(gate, M2_DATE)
    inserted_sql = gate.inserted[0][0]
    for name in M3_STRETCH_NAMES_T7:
        assert f"'{name}' AS check_name" in inserted_sql, name
    assert inserted_sql.count("\nUNION ALL\n") == 19
