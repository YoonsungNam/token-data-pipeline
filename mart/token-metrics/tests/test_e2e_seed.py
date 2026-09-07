"""E2E 시드(tests/e2e/seed_metrics.py)·기대값(tests/e2e/mart_expectations.py)의 CH 불필요 부분 검증 (Plan 6c T10).

- build_seed(date): 키·컬럼 폭·결정성(sha256 user_id, random 미사용)·시나리오 값(gpu/serving/summary/token/registry)
- DDL 파일(collectors/token-metrics, mart/token-metrics, 기존 token-usage DDL, ddl_test_dims.sql)에 SEED_TABLES
  각 테이블의 실제 정의가 있는지 교차 대조(R3 D-2 — 원래 초안의 `table.split(".")[0] in (DB_FACT, DB_DIM, DB_MART)`
  단정은 이 3 상수의 정의상 항상 참인 동어반복이라 대체한다)
- ddl_test_dims.sql 시드 값 == seed_metrics.TCO_KRW/ALLOCATION/ALIASES (두 정본 교차 대조 — 드리프트 방지)
- expect(date): 9키 값 검산(정의서 §5.1/§5.3·설계 §6.4 — 아웃라인 T10 Step 3의 검산표)·m3_counts 20블록 분해
- M3_SEVERITY(R6): app.steps.M3_BLOCKS_CORE + M3_BLOCKS_STRETCH(name -> severity)와 정확히 일치하는지 대조
- M4 share/allocation 항등식(R1): all_services/provider_reported 등 배분 모드에서 Sigma share == 1,
  Sigma allocated == model_cost_krw(코스트가 있는 경우) — app.mart.allocate_shared/provider_self_weight 경유
- M2 "other" 카테고리(R2): SQL_M2의 unattributed = (flagged + other) x TCO를 파이썬 쪽도 반영하는지
  합성 시드로 확인(identity_gap_krw == 0 유지)
"""
import hashlib
import importlib.util
import pathlib
import re
import sys
from datetime import date as date_cls

import pytest

import app.steps as steps

E2E_DIR = pathlib.Path(__file__).resolve().parent / "e2e"
ROOT = pathlib.Path(__file__).resolve().parents[3]        # tests -> token-metrics -> mart -> repo root
DATE = "2026-09-03"

# R3 D-2: SEED_TABLES 각 테이블(bare 이름, `<db>.` 접두·`_dist` 접미 제거)이 CREATE TABLE 정의로
# 존재하는지 대조할 DDL 파일 집합(전부 읽기 전용 — zero-diff 대상 아님, 값을 여기서 바꾸지 않는다).
DDL_FILES = (
    sorted((ROOT / "collectors" / "token-metrics" / "ddl" / "company").glob("*.sql"))
    + sorted((ROOT / "mart" / "token-metrics" / "ddl" / "company").glob("*.sql"))
    + [
        ROOT / "collectors" / "token-usage" / "ddl" / "company" / "dim_token_service.sql",
        ROOT / "mart" / "token-usage" / "ddl" / "company" / "mart_tables.sql",
        E2E_DIR / "ddl_test_dims.sql",
    ]
)


def _load(name: str):
    path = E2E_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def sm():
    return _load("seed_metrics")


@pytest.fixture(scope="module")
def me(sm):
    return _load("mart_expectations")


@pytest.fixture(scope="module")
def seed(sm):
    return sm.build_seed(DATE)


@pytest.fixture(scope="module")
def ddl_text():
    return "\n".join(p.read_text(encoding="utf-8") for p in DDL_FILES)


# ---------------------------------------------------------------- build_seed 구조·결정성

def test_build_seed_keys_follow_seed_tables_order_and_column_width(sm, seed, ddl_text):
    assert list(seed.keys()) == list(sm.SEED_TABLES.keys())
    assert list(sm.SEED_TABLES.keys()) == ["dim_token_service", "dim_metrics_service", "gpu", "serving",
                                           "summary", "token_usage", "agg_service"]
    for key, rows in seed.items():
        table, cols = sm.SEED_TABLES[key]
        assert table.endswith("_dist")
        # R3 D-2: 동어반복 단정(`table.split(".")[0] in (DB_FACT, DB_DIM, DB_MART)`) 대신 DDL 파일
        # 집합에 이 테이블의 실제 정의(`<bare>_local` 또는 ddl_test_dims.sql의 단일노드 `<bare>`)가
        # 있는지 정규식으로 확인한다.
        bare = table.split(".", 1)[1][: -len("_dist")]
        assert re.search(rf"CREATE TABLE IF NOT EXISTS \S*\.{re.escape(bare)}(_local)?\b", ddl_text), (key, bare)
        assert rows, key
        for row in rows:
            assert isinstance(row, tuple) and len(row) == len(cols), (key, row)


def test_build_seed_is_deterministic_and_uses_sha256_not_random(sm, seed):
    assert sm.build_seed(DATE) == seed
    src = (E2E_DIR / "seed_metrics.py").read_text(encoding="utf-8")
    assert "import random" not in src and "hashlib" in src
    cols = sm.SEED_TABLES["token_usage"][1]
    uid_idx, svc_idx = cols.index("user_id"), cols.index("service")
    for row in seed["token_usage"]:
        uid, svc = row[uid_idx], row[svc_idx]
        assert re.fullmatch(r"u-[0-9a-f]{12}", uid)
        assert uid in {sm.synthetic_user_id(svc, DATE, 0), sm.synthetic_user_id(svc, DATE, 1)}
    assert sm.synthetic_user_id("Mock Service A", DATE, 0) == \
        "u-" + hashlib.sha256(f"Mock Service A|{DATE}|0".encode()).hexdigest()[:12]


def test_row_counts_match_scenario(seed):
    assert {k: len(v) for k, v in seed.items()} == {
        "dim_token_service": 4, "dim_metrics_service": 3, "gpu": 6, "serving": 2,
        "summary": 2, "token_usage": 6, "agg_service": 3}


def test_dates_are_date_objects_and_datetimes_are_kst_aware(sm, seed):
    for key, rows in seed.items():
        cols = sm.SEED_TABLES[key][1]
        for row in rows:
            for name, val in zip(cols, row):
                if name == "date":
                    assert type(val) is date_cls and val == date_cls(2026, 9, 3)
                if name in ("api_since", "coverage_since"):
                    assert type(val) is date_cls
                if name in ("generated_at", "collected_at", "updated_at"):
                    assert val.tzinfo is not None and val.utcoffset().total_seconds() == 9 * 3600


# ---------------------------------------------------------------- 시나리오 값

def test_gpu_and_serving_and_anchor_values(sm, seed):
    gcols = sm.SEED_TABLES["gpu"][1]
    g = [dict(zip(gcols, r)) for r in seed["gpu"]]
    by_key = {(r["service"], r["model"], r["gpu_type"], r["category"]): r for r in g}
    assert len(by_key) == 6                                             # metrics_gpu_dup_key 0건
    assert by_key[(sm.SVC_A, sm.MODEL_QWEN, "H100", "serving")]["gpu_hours"] == 40.0
    assert by_key[(sm.SVC_A, sm.MODEL_QWEN, "H100", "standby")]["gpu_hours"] == 8.0
    assert by_key[(sm.SVC_A, sm.MODEL_QWEN, "H100", "test")]["gpu_hours"] == 2.0
    assert by_key[(sm.SVC_B, sm.MODEL_SONNET, "B200", "serving")]["gpu_hours"] == 4.0
    flagged = by_key[(sm.SVC_B, sm.MODEL_SONNET, "H100", "standby")]
    assert flagged["gpu_count"] == 2.0 and flagged["gpu_hours"] == 50.0 and flagged["flags"] == ["hours_over_count"]
    assert all(r["flags"] == [] for k, r in by_key.items() if k != (sm.SVC_B, sm.MODEL_SONNET, "H100", "standby"))
    assert {r["source_type"] for r in g if r["service"] == sm.SVC_B} == {"manual-v0"}

    scols = sm.SEED_TABLES["serving"][1]
    s = [dict(zip(scols, r)) for r in seed["serving"]]
    assert {(r["service"], r["model"], r["metric"], r["name"]) for r in s} == {
        (sm.SVC_A, sm.MODEL_QWEN, "ttft_ms", ""), (sm.SVC_A, sm.MODEL_QWEN, "output_tps", "")}
    ttft = next(r for r in s if r["metric"] == "ttft_ms")
    assert (ttft["p50"], ttft["p90"], ttft["p95"], ttft["p99"], ttft["unit"]) == (120.0, 240.0, 300.0, 450.0, "ms")
    tps = next(r for r in s if r["metric"] == "output_tps")
    assert (tps["p50"], tps["p90"], tps["p95"], tps["p99"], tps["unit"]) == (80.0, 60.0, 55.0, 40.0, "tokens/s")

    acols = sm.SEED_TABLES["summary"][1]
    a = {r[acols.index("service")]: dict(zip(acols, r)) for r in seed["summary"]}
    assert set(a) == {sm.SVC_A, sm.SVC_B}                               # C 앵커 없음
    assert (a[sm.SVC_A]["gpu_rows"], a[sm.SVC_A]["serving_rows"], a[sm.SVC_A]["rejected_rows"]) == (3, 2, 1)
    assert (a[sm.SVC_B]["gpu_rows"], a[sm.SVC_B]["serving_rows"], a[sm.SVC_B]["rejected_rows"]) == (3, 0, 0)
    for r in a.values():                                                 # identity_drift 0건
        assert r["reported_service"] == r["service"] and r["reported_service_group"] == sm.SERVICE_GROUP


def test_token_side_values(sm, seed):
    cols = sm.SEED_TABLES["token_usage"][1]
    rows = [dict(zip(cols, r)) for r in seed["token_usage"]]
    sums = {}
    for r in rows:
        assert r["model"] == sm.MODEL_QWEN and r["created_by"] == "token-pipeline"
        assert r["total_input_tokens"] == r["input_tokens"] + r["cache_read_tokens"] + r["cache_creation_tokens"]
        assert r["org_path"] == ["unknown"] and r["cost"] is None and r["user_type"] == "identified"
        acc = sums.setdefault(r["service"], [0, 0, 0, 0, 0])
        for i, f in enumerate(("input_tokens", "cache_read_tokens", "cache_creation_tokens",
                               "output_tokens", "requests")):
            acc[i] += r[f]
    assert {k: tuple(v) for k, v in sums.items()} == {
        sm.SVC_A: (2_000_000, 5_000_000, 0, 250_000, 100),
        sm.SVC_B: (4_000_000, 10_000_000, 0, 500_000, 200),
        sm.SVC_D: (500_000, 0, 0, 0, 10)}
    acols = sm.SEED_TABLES["agg_service"][1]
    agg = {r[acols.index("service")]: dict(zip(acols, r)) for r in seed["agg_service"]}
    assert set(agg) == {sm.SVC_A, sm.SVC_B, sm.SVC_D}
    assert agg[sm.SVC_A]["input_tokens"] == 2_000_000 and agg[sm.SVC_A]["created_by"] == "token-pipeline"


def test_registries(sm, seed):
    ucols = sm.SEED_TABLES["dim_token_service"][1]
    usage = {r[ucols.index("service")]: dict(zip(ucols, r)) for r in seed["dim_token_service"]}
    assert set(usage) == {sm.SVC_A, sm.SVC_B, sm.SVC_C, sm.SVC_D}
    assert all(r["enabled"] == 1 and r["service_group"] == sm.SERVICE_GROUP for r in usage.values())
    mcols = sm.SEED_TABLES["dim_metrics_service"][1]
    reg = {r[mcols.index("service")]: dict(zip(mcols, r)) for r in seed["dim_metrics_service"]}
    assert set(reg) == {sm.SVC_A, sm.SVC_B, sm.SVC_C}
    assert {(k, r["expect_gpu"], r["expect_serving"], r["usage_includes_consumers"]) for k, r in reg.items()} == {
        (sm.SVC_A, 1, 1, 0), (sm.SVC_B, 1, 0, 0), (sm.SVC_C, 1, 1, 0)}
    assert all(r["enabled"] == 1 and r["coverage_since"] == date_cls(2026, 8, 26) and r["until"] is None
               for r in reg.values())


# ---------------------------------------------------------------- ddl_test_dims.sql 정본 교차 대조

def _sql_insert_values(sql: str, table: str) -> list[str]:
    """INSERT INTO <table> (cols) VALUES (...),(...); 의 각 값 튜플 문자열을 돌려준다(주석·세미콜론 없음 전제)."""
    m = re.search(rf"INSERT INTO {re.escape(table)} \([^)]*\) VALUES\s*(.*?);", sql, flags=re.S)
    assert m, table
    return re.findall(r"\(([^()]*)\)", m.group(1))


def test_ddl_test_dims_matches_python_twin_constants(sm):
    sql = (E2E_DIR / "ddl_test_dims.sql").read_text(encoding="utf-8")
    tco_rows = _sql_insert_values(sql, "gpu_data.dim_token_gpu_tco_dist")
    tco = {}
    for r in tco_rows:
        parts = [p.strip().strip("'") for p in r.split(",")]
        tco[parts[0]] = None if parts[2] == "NULL" else float(parts[2])
    assert tco == sm.TCO_KRW                                             # {'unknown': None, 'H100': 4200.0, ...}
    alloc_rows = _sql_insert_values(sql, "gpu_data.dim_token_gpu_allocation_dist")
    alloc = {}
    for r in alloc_rows:
        parts = [p.strip().strip("'") for p in r.split(",")]
        alloc[(parts[0], parts[1])] = None if parts[3] == "NULL" else float(parts[3])
    assert alloc == sm.ALLOCATION
    alias_rows = _sql_insert_values(sql, "gpu_data.dim_token_model_alias_dist")
    aliases = {}
    for r in alias_rows:
        parts = [p.strip().strip("'") for p in r.split(",")]
        aliases[parts[0]] = parts[2]
    assert aliases == sm.ALIASES
    assert len(_sql_insert_values(sql, "gpu_data.dim_token_vendor_price_dist")) == len(sm.VENDOR_PRICE)


# ---------------------------------------------------------------- R6: M3 이름/severity == app.steps 정본

def test_m3_severity_matches_steps_module(me):
    """R6: mart_expectations.M3_SEVERITY(name -> severity)는 app.steps.M3_BLOCKS_CORE +
    M3_BLOCKS_STRETCH(브랜치 실물)와 정확히 같아야 한다 — 20블록, core 13 + stretch 7(T6 3 + T7 4)."""
    branch_blocks = dict(_m3_block_severities())
    assert len(branch_blocks) == 20
    assert branch_blocks == me.M3_SEVERITY
    assert list(branch_blocks) == [name for name, _sql in steps.M3_BLOCKS_CORE + steps.M3_BLOCKS_STRETCH]


def _m3_block_severities():
    """(name, severity) — 각 블록 SQL 문자열의 `AS severity` 리터럴에서 severity를 뽑는다(브랜치 실물 대조)."""
    for name, sql in steps.M3_BLOCKS_CORE + steps.M3_BLOCKS_STRETCH:
        m = re.search(r"'(FAIL|WARN|INFO)'\s+AS severity", sql)
        assert m, name
        yield name, m.group(1)


# ---------------------------------------------------------------- 기대값 검산 (아웃라인 T10 Step 3의 검산표)

def test_expect_values(me):
    exp = me.expect(DATE)
    assert list(exp.keys()) == ["EXP_M1_ROWS", "EXP_M1_QWEN_COST", "EXP_M3_FAIL_ROWS", "EXP_M3_WARN_ROWS",
                                "EXP_M4_ROWS", "EXP_M4_QWEN_SUM", "EXP_M2_ROWS", "EXP_M2_IDLE_H100",
                                "EXP_COVERAGE"]
    assert exp["EXP_M1_ROWS"] == 4                                       # (A,Qwen) (B,Qwen) (B,sonnet) (D,Qwen)
    assert exp["EXP_M1_QWEN_COST"] == pytest.approx(201_600.0)            # (40+8)h x 4200
    assert exp["EXP_M3_FAIL_ROWS"] == 2                                   # metrics_missing(C) + hours_over_count(B)
    assert exp["EXP_M3_WARN_ROWS"] == 3                                   # rows_rejected(A) gpu_type_no_tco(B/B200) no_allocation(B200)
    assert exp["EXP_M4_ROWS"] == 4                                        # Qwen x {A,B,D} + sonnet(B)
    assert exp["EXP_M4_QWEN_SUM"] == pytest.approx(201_600.0)             # 배분 합 == 원가 (share_sum_mismatch 0)
    assert exp["EXP_M2_ROWS"] == 3                                        # H100 / B200 / A100(할당만)
    assert exp["EXP_M2_IDLE_H100"] == pytest.approx(72.0)                 # 8x24 - (48 + 2 + 50 + 20)
    assert exp["EXP_COVERAGE"] == "2/3"


def test_m3_counts_breakdown(sm, me):
    counts = me.m3_counts(sm.build_seed(DATE))
    assert counts == {
        "metrics_missing": 1, "partial_load": 0, "rows_rejected": 1, "unregistered_model": 0,
        "hours_over_count": 1, "unknown_violation": 0, "pct_non_monotone": 0, "gpu_type_no_tco": 1,
        "serving_missing_for_gpu_model": 0, "serving_without_gpu_serving_row": 0, "identity_drift": 0,
        "service_not_in_usage_registry": 0, "manual_source": 1,
        "provider_ambiguous": 0, "consumer_tokens_exceed_provider": 0, "vendor_price_missing": 0,
        "no_allocation": 1, "sum_hours_over_allocation": 0,
        "gpu_block_empty_unexpected": 0, "serving_block_empty_unexpected": 0}
    assert sum(counts.values()) == 6


def test_m2_identity_gap_zero_where_tco_present(sm, me):
    rows = me.m2_rows(sm.build_seed(DATE))
    g = sm.SERVICE_GROUP
    assert set(rows) == {(g, "H100"), (g, "B200"), (g, "A100")}
    h100, b200, a100 = rows[(g, "H100")], rows[(g, "B200")], rows[(g, "A100")]
    assert h100["quality_flag"] == "flagged" and h100["identity_gap_krw"] == pytest.approx(0.0)
    assert h100["reported_gpu_hours_total"] == pytest.approx(120.0) and h100["idle_gpu_hours"] == pytest.approx(72.0)
    assert a100["quality_flag"] == "normal" and a100["idle_gpu_hours"] == pytest.approx(96.0)
    assert a100["identity_gap_krw"] == pytest.approx(0.0)
    assert b200["quality_flag"] == "no_tco" and b200["group_total_cost_krw"] is None


def test_main_prints_nine_key_value_lines(me, capsys):
    assert me.main([DATE]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 9 and all(re.fullmatch(r"EXP_[A-Z0-9_]+=[^\s]+", ln) for ln in lines)
    assert "EXP_M1_QWEN_COST=201600.0000" in lines and "EXP_COVERAGE=2/3" in lines


# ---------------------------------------------------------------- R1: M4 share/allocation 항등식

def test_m4_share_sum_and_allocation_identity_r1(sm, me):
    """R1(scan-C N-5 / scan-B C4): provider_reported/all_services의 share·배분은
    app.mart.allocate_shared / provider_self_weight 로만 계산한다(인라인 재구현 금지) — 검증:
    provider_ambiguous/no_provider/external_api를 제외한 모든 모드에서 모델별 Sigma share == 1.0(+-1e-9),
    all_services/provider_reported이고 model_cost_krw가 있는 모델은 Sigma allocated == model_cost_krw."""
    rows = me.m4_rows(sm.build_seed(DATE))
    models = {m for m, _s in rows}
    assert models                                        # 시나리오에 최소 1개 모델(Qwen3-32B) 존재
    checked_all_services_or_provider_reported = 0
    for model in models:
        model_rows = {s: r for (m, s), r in rows.items() if m == model}
        mode = next(iter(model_rows.values()))["denominator_mode"]
        assert all(r["denominator_mode"] == mode for r in model_rows.values())
        if mode in ("provider_ambiguous", "no_provider", "external_api"):
            continue
        shares = [r["share"] for r in model_rows.values() if r["share"] is not None]
        if not shares:
            continue        # ë¶ëª¨(w_all/D) 0 â ìë¬´ë ì´ ëª¨ë¸ì ë³´ê³ íì§ ìì(share ì ë¶ NULL, ì ì)
        assert sum(shares) == pytest.approx(1.0, abs=1e-9), (model, mode, shares)
        if mode in ("all_services", "provider_reported"):
            cost = next(iter(model_rows.values()))["model_cost_krw"]
            if cost:
                allocated_sum = sum(r["allocated_cost_krw"] for r in model_rows.values()
                                    if r["allocated_cost_krw"] is not None)
                assert allocated_sum == pytest.approx(cost, abs=1e-6), (model, mode, allocated_sum, cost)
                checked_all_services_or_provider_reported += 1
    assert checked_all_services_or_provider_reported >= 1     # Qwen3-32B(all_services, cost=201,600)는 반드시 통과


# ---------------------------------------------------------------- R2: M2 "other" 카테고리 identity_gap

def test_m2_rows_handles_other_category_and_keeps_identity_gap_zero(sm, me):
    """R2(scan-B D3-secondary): SQL_M2의 unattributed = (flagged + other) x TCO를 파이썬도 반영한다 —
    category가 {serving,standby,test}에 속하지 않는 비FAIL 행("other")을 추가해도 identity_gap_krw == 0
    (group_total_cost_krw는 allocated_gpu_hours x TCO로 other/flagged 증가와 무관하게 고정)."""
    seed = sm.build_seed(DATE)
    gcols = sm.SEED_TABLES["gpu"][1]
    idx = {c: i for i, c in enumerate(gcols)}
    extra = [None] * len(gcols)
    template = seed["gpu"][0]                              # (A, Qwen3-32B, H100, serving, ...) 행을 복제해 변형
    extra = list(template)
    extra[idx["category"]] = "other"                       # 정상 3분류 밖의 카테고리 — SQL_M2 other_gpu_hours 경로
    extra[idx["gpu_hours"]] = 10.0
    extra[idx["gpu_count"]] = 1.0
    extra[idx["flags"]] = []
    seed["gpu"] = list(seed["gpu"]) + [tuple(extra)]

    rows = me.m2_rows(seed)
    g = sm.SERVICE_GROUP
    h100 = rows[(g, "H100")]
    assert h100["reported_gpu_hours_total"] == pytest.approx(130.0)      # 기존 120 + other 10
    assert h100["idle_gpu_hours"] == pytest.approx(62.0)                 # 192 - 130
    assert h100["identity_gap_krw"] == pytest.approx(0.0, abs=1e-6)
    assert h100["flagged_gpu_hours"] == pytest.approx(50.0)              # FAIL 전용 — other는 섞이지 않는다
