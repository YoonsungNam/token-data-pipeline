"""tools/verify/tests/test_run_invariants_metrics.py — Plan 6c T9
run_invariants.py --sql 라우팅(additive) + invariants_metrics.sql 정적 계약.

- 라우팅: --sql 미지정 → SQL_PATH(기존 동작 불변), 지정 → 그 파일, 파일 없음 → exit 2.
- 정적 계약: 8블록 순서·3컬럼·토큰 4종·M1 동일 술어·coalesce/사용자 식별자 0·
  created_by 4테이블·stretch 3블록 술어·신규 테이블만 참조·SELECT 전용·GLOBAL 명시.
- CH 접속 없음(FakeCH). 기존 tests/test_run_invariants.py는 import하지 않는다(무수정 원칙).
"""
import pathlib
import re

import pytest

import run_invariants as ri

METRICS_SQL = pathlib.Path(ri.__file__).resolve().parent / "invariants_metrics.sql"

EXPECTED_BLOCKS = [
    "metrics_anchor_missing",
    "metrics_gpu_dup_key",
    "metrics_serving_dup_key",
    "metrics_cost_sum_mismatch",
    "created_by_wrong_metrics",
    "share_sum_mismatch",
    "group_identity_gap",
    "idle_negative",
]
MART_TABLES = [
    "agg_token_model_cost_1d",
    "token_metrics_check_1d",
    "agg_token_model_share_1d",
    "agg_token_gpu_group_1d",
]
MARKER_RE = re.compile(r"'(\w+)' AS check_name")

# T3 SQL_M1 gpu_agg 와 문자 그대로 같아야 하는 조각(도구 독립성 — import 대신 문자열 대조)
M1_COST_PRED = ("g.category IN ('serving','standby') "
                "AND NOT hasAny(g.flags, ['hours_over_count','unknown_violation'])")
M1_CANON = "if(a.canonical = '', g.model, a.canonical)"
M1_TCO = "nullIf(argMax(ifNull(tco_krw_per_gpu_hour, -1), effective_from), -1) AS tco"


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeCH:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.queries = []
        self.last_settings = None

    def query(self, sql, parameters=None, settings=None):
        self.queries.append(sql)
        self.last_settings = settings
        return FakeResult(self.rows)


@pytest.fixture(autouse=True)
def clean_ch_env(monkeypatch):
    for k in ("CH_DB_FACT", "CH_DB_DIM", "CH_DB_MART",
              "CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_CLUSTER"):
        monkeypatch.delenv(k, raising=False)


def metrics_sql() -> str:
    return METRICS_SQL.read_text(encoding="utf-8")


def _code(sql: str) -> str:
    """`--` 주석 줄을 제거한 SQL 본문."""
    return "\n".join(line for line in sql.splitlines()
                     if not line.lstrip().startswith("--"))


def _block(sql: str, name: str) -> str:
    """이름의 첫 마커부터 다른 이름의 다음 마커 직전까지(같은 이름의 연속 SELECT는 한 블록)."""
    markers = list(MARKER_RE.finditer(sql))
    starts = [m for m in markers if m.group(1) == name]
    assert starts, f"block marker not found: {name}"
    start = starts[0].start()
    end = len(sql)
    for m in markers:
        if m.start() > start and m.group(1) != name:
            end = m.start()
            break
    return sql[start:end]


# ---------------------------------------------------------------------------
# 1) --sql 라우팅 (run_invariants.py additive)
# ---------------------------------------------------------------------------

def test_sql_flag_default_is_invariants_sql(monkeypatch):
    assert ri.build_arg_parser().parse_args([]).sql is None
    seen = []

    def fake_load(path=ri.SQL_PATH):
        seen.append(path)
        return ("SELECT 'x' AS check_name, 'd' AS detail, toUInt64(1) AS bad_count "
                "FROM {FACT}.t WHERE date = '{DATE}'")

    monkeypatch.setattr(ri, "load_sql", fake_load)
    assert ri.main(["--date", "2026-09-03"], client=FakeCH([])) == 0
    assert seen == [ri.SQL_PATH]


def test_sql_flag_loads_metrics_file(capsys):
    fake = FakeCH([])
    rc = ri.main(["--sql", str(METRICS_SQL), "--date", "2026-09-03"], client=fake)
    assert rc == 0
    assert len(fake.queries) == 1
    sent = fake.queries[0]
    assert "'metrics_anchor_missing' AS check_name" in sent
    assert "'idle_negative' AS check_name" in sent
    for tok in ("{FACT}", "{DIM}", "{MART}", "{DATE}"):
        assert tok not in sent
    assert "date = '2026-09-03'" in sent
    assert "fact.raw_token_metrics_summary_1d_dist" in sent
    assert "gpu_data.dim_token_gpu_tco_dist" in sent
    assert "mart.agg_token_gpu_group_1d_dist" in sent
    assert fake.last_settings == {"distributed_product_mode": "global"}
    out = capsys.readouterr().out
    assert "ALL INVARIANTS PASS" in out
    assert "sql=invariants_metrics.sql" in out


def test_sql_flag_relative_path_resolved_from_cwd(tmp_path, monkeypatch, capsys):
    body = ("SELECT 'x' AS check_name, 'd' AS detail, toUInt64(1) AS bad_count\n"
            "FROM {MART}.t WHERE date = '{DATE}'\n")
    (tmp_path / "custom.sql").write_text(body, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    fake = FakeCH([])
    assert ri.main(["--sql", "custom.sql", "--date", "2026-09-03"], client=fake) == 0
    assert fake.queries == [
        "SELECT 'x' AS check_name, 'd' AS detail, toUInt64(1) AS bad_count\n"
        "FROM mart.t WHERE date = '2026-09-03'\n"
    ]
    assert "sql=custom.sql" in capsys.readouterr().out


def test_sql_flag_missing_file_exit2(tmp_path, capsys):
    missing = tmp_path / "nonexistent.sql"
    with pytest.raises(SystemExit) as e:
        ri.main(["--sql", str(missing), "--date", "2026-09-03"], client=FakeCH([]))
    assert e.value.code == 2
    err = capsys.readouterr().err
    assert "--sql 파일을 찾을 수 없습니다" in err
    assert str(missing) in err


def test_default_run_message_names_default_sql(monkeypatch, capsys):
    monkeypatch.setattr(ri, "load_sql", lambda path=ri.SQL_PATH: "SELECT 1")
    assert ri.main(["--date", "2026-09-03"], client=FakeCH([])) == 0
    out = capsys.readouterr().out
    assert ("ALL INVARIANTS PASS (date=2026-09-03, DBs=fact/gpu_data/mart, "
            "sql=invariants.sql)") in out


def test_violation_rows_printed_and_exit1(capsys):
    fake = FakeCH([("idle_negative", "grpA/H100 reported=30 allocated=24", 1),
                   ("group_identity_gap", "grpB/A100 gap=12.5", 1)])
    rc = ri.main(["--sql", str(METRICS_SQL), "--date", "2026-09-03"], client=fake)
    assert rc == 1
    out = capsys.readouterr().out
    assert ("[FAIL] 2건의 불변식 위반 발견 (date=2026-09-03, DBs=fact/gpu_data/mart, "
            "sql=invariants_metrics.sql)") in out
    assert "idle_negative" in out
    assert "grpA/H100 reported=30 allocated=24" in out
    assert "group_identity_gap" in out


# ---------------------------------------------------------------------------
# 2) invariants_metrics.sql 정적 계약
# ---------------------------------------------------------------------------

def test_metrics_sql_has_eight_blocks_in_order():
    sql = _code(metrics_sql())
    names = MARKER_RE.findall(sql)
    assert list(dict.fromkeys(names)) == EXPECTED_BLOCKS
    assert names.count("created_by_wrong_metrics") == 4
    assert len(names) == 11
    assert sql.count("UNION ALL") == 10
    assert not sql.rstrip().endswith(";")


def test_metrics_sql_tokens_only_known_four():
    sql = metrics_sql()
    tokens = set(re.findall(r"\{[A-Za-z_:]+\}", sql))
    assert tokens == {"{FACT}", "{DIM}", "{MART}", "{DATE}"}
    rendered = ri.render(sql, fact="vf", dim="vd", mart="vm", date="2026-09-03")
    assert re.findall(r"\{[A-Za-z_:]+\}", rendered) == []
    assert "CH_DB_TOKEN" not in sql


def test_metrics_sql_three_column_contract():
    sql = _code(metrics_sql())
    selects = sql.split("UNION ALL")
    assert len(selects) == 11
    for sel in selects:
        head = sel[:sel.index("\nFROM")]
        assert MARKER_RE.search(head), head
        assert " AS detail" in head, head
        assert re.search(r"toUInt64\(.+\) AS bad_count", head, re.S), head
        assert "'{DATE}'" in sel, sel


def test_metrics_cost_predicate_matches_m1():
    blk = _block(_code(metrics_sql()), "metrics_cost_sum_mismatch")
    assert blk.count(M1_COST_PRED) == 2
    assert M1_CANON in blk
    assert f"GROUP BY g.service, {M1_CANON}" in blk
    assert M1_TCO in blk
    assert "argMax(canonical, effective_from) AS canonical" in blk
    assert blk.count("effective_from <= '{DATE}'") == 2
    assert "has_gpu_rows = 1" in blk
    cost_slice = blk[blk.index("sumIf(g.gpu_hours * t.tco"):blk.index("AS fact_cost")]
    assert "'test'" not in cost_slice
    assert "isNull(t.tco)" in blk
    assert "isNotNull(t.tco)" in blk
    assert "raw_token_metrics_summary_1d_dist" in blk
    assert "abs(ifNull(m.model_cost_krw, 0) - ifNull(f.fact_cost, 0)) > 1" in blk
    assert "isNull(m.model_cost_krw) != (f.tco_null_cnt > 0)" in blk


def test_metrics_sql_no_coalesce_and_no_user_id():
    sql = metrics_sql()
    assert "coalesce(" not in sql.lower()
    assert "user_id" not in sql
    assert "user_name" not in sql


def test_created_by_block_covers_four_mart_tables():
    code = _code(metrics_sql())
    blk = _block(code, "created_by_wrong_metrics")
    assert blk.count("'created_by_wrong_metrics' AS check_name") == 4
    for t in MART_TABLES:
        assert f"FROM {{MART}}.{t}_dist" in blk
        assert f"concat('table={t} created_by=', created_by)" in blk
    positions = [blk.index(f"FROM {{MART}}.{t}_dist") for t in MART_TABLES]
    assert positions == sorted(positions)
    assert blk.count("created_by != 'token-metrics-pipeline'") == 4
    assert blk.count("GROUP BY created_by") == 4
    assert "'token-pipeline'" not in code


def test_group_identity_gap_excludes_only_tco_missing():
    blk = _block(_code(metrics_sql()), "group_identity_gap")
    assert "FROM {MART}.agg_token_gpu_group_1d_dist" in blk
    assert "over_report" not in blk          # 설계 §7.1: I2 = abs(gap) > 1, over_report 면제 없음
    assert "tco_missing = 0" in blk
    assert "isNotNull(identity_gap_krw)" in blk
    assert "abs(identity_gap_krw) > 1" in blk


def test_idle_negative_is_over_report_rows():
    blk = _block(_code(metrics_sql()), "idle_negative")
    assert "FROM {MART}.agg_token_gpu_group_1d_dist" in blk
    assert "over_report = 1" in blk
    assert "reported=" in blk
    assert "allocated=" in blk


def test_share_sum_mismatch_modes_and_null_rule():
    blk = _block(_code(metrics_sql()), "share_sum_mismatch")
    assert "FROM {MART}.agg_token_model_share_1d_dist" in blk
    assert "denominator_mode IN ('all_services','provider_reported','token_not_reported')" in blk
    assert "isNotNull(model_cost_krw)" in blk
    assert "GROUP BY model" in blk
    assert ("HAVING abs(ifNull(sum(allocated_cost_krw), 0) "
            "- ifNull(any(model_cost_krw), 0)) > 1") in blk


def test_dup_key_blocks_group_by_full_order_by_key():
    code = _code(metrics_sql())
    gpu = _block(code, "metrics_gpu_dup_key")
    srv = _block(code, "metrics_serving_dup_key")
    assert "FROM {FACT}.raw_token_metrics_gpu_1d_dist" in gpu
    assert "GROUP BY service, model, gpu_type, category" in gpu
    assert "FROM {FACT}.raw_token_metrics_serving_1d_dist" in srv
    assert "GROUP BY service, model, metric, name" in srv
    for blk in (gpu, srv):
        assert "HAVING n > 1" in blk
        assert "HAVING count() > 0" in blk


def test_anchor_block_unions_children_and_global_not_in():
    blk = _block(_code(metrics_sql()), "metrics_anchor_missing")
    assert "FROM {FACT}.raw_token_metrics_gpu_1d_dist WHERE date = '{DATE}'" in blk
    assert "FROM {FACT}.raw_token_metrics_serving_1d_dist WHERE date = '{DATE}'" in blk
    assert "UNION DISTINCT" in blk
    assert "GLOBAL NOT IN" in blk
    assert "FROM {FACT}.raw_token_metrics_summary_1d_dist WHERE date = '{DATE}'" in blk
    assert "HAVING count() > 0" in blk


def test_metrics_sql_reads_only_new_tables():
    code = _code(metrics_sql())
    tables = set(re.findall(r"\{(?:FACT|DIM|MART)\}\.([a-z_0-9]+)", code))
    assert tables == {
        "raw_token_metrics_gpu_1d_dist",
        "raw_token_metrics_serving_1d_dist",
        "raw_token_metrics_summary_1d_dist",
        "dim_token_model_alias_dist",
        "dim_token_gpu_tco_dist",
        "agg_token_model_cost_1d_dist",
        "token_metrics_check_1d_dist",
        "agg_token_model_share_1d_dist",
        "agg_token_gpu_group_1d_dist",
    }
    for legacy in ("token_usage_1d", "view_token_usage",
                   "dim_token_service_dist", "dim_token_model_dist"):
        assert legacy not in code


def test_metrics_sql_is_select_only():
    code = _code(metrics_sql())
    assert re.search(r"\b(INSERT|ALTER|DELETE|DROP|TRUNCATE|OPTIMIZE)\b", code, re.I) is None
    assert code.lstrip().startswith("SELECT")


def test_global_join_discipline():
    code = _code(metrics_sql())
    assert re.search(r"(?<!GLOBAL )LEFT JOIN", code) is None
    assert re.search(r"(?<!GLOBAL )(?<!GLOBAL NOT )\bIN \(\s*SELECT", code) is None
    assert code.count("GLOBAL LEFT JOIN") == 3
    assert code.count("GLOBAL IN (") == 1
    assert code.count("GLOBAL NOT IN (") == 1
