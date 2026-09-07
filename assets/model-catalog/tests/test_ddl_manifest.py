"""설계 2026-08-31 §4.0 P0 DDL 매니페스트(14파일) 컨벤션 lint (Plan 6a T2).

목적: 두 생성기(tools/gen_stage_ddl.py, tools/gen_verify_ddl.py)와 e2e 단일노드 변환
정규식이 그대로 먹히는 형식인지, §4.0 물리 표·§4.2 GRANT 표·시드 3요소가 파일에
그대로 반영됐는지를 기계로 검증한다. ClickHouse 문법 검증은 6b/6c e2e가 담당.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
CLUSTER = "gpu-monitoring"

COLLECTORS_DIR = "collectors/token-metrics/ddl/company"
MART_DIR = "mart/token-metrics/ddl/company"
ASSETS_DIR = "assets/model-catalog/ddl/company"
FIXTURES_DIR = "assets/model-catalog/fixtures"

MANIFEST = [
    f"{COLLECTORS_DIR}/raw_token_metrics.sql",
    f"{COLLECTORS_DIR}/dim_token_metrics_service.sql",
    f"{COLLECTORS_DIR}/accounts.sql",
    f"{MART_DIR}/mart_metrics_tables.sql",
    f"{MART_DIR}/accounts.sql",
    f"{ASSETS_DIR}/dim_token_model_alias.sql",
    f"{ASSETS_DIR}/dim_token_gpu_tco.sql",
    f"{ASSETS_DIR}/dim_token_gpu_allocation.sql",
    f"{ASSETS_DIR}/dim_token_vendor_price.sql",
    f"{ASSETS_DIR}/seed_dim_token_model_alias.sql",
    f"{ASSETS_DIR}/seed_dim_token_gpu_tco.sql",
    f"{ASSETS_DIR}/seed_dim_token_gpu_allocation.sql",
    f"{ASSETS_DIR}/seed_dim_token_vendor_price.sql",
    f"{ASSETS_DIR}/accounts_metrics.sql",
]

# (id, rel_path, db, table, partition, order_by, sharding, kind)  kind: fact | dim | mart
TABLES = [
    ("collectors_gpu", f"{COLLECTORS_DIR}/raw_token_metrics.sql", "fact", "raw_token_metrics_gpu_1d",
     "toYYYYMM(date)", "(date, service, model, gpu_type, category)", "cityHash64(service)", "fact"),
    ("collectors_serving", f"{COLLECTORS_DIR}/raw_token_metrics.sql", "fact", "raw_token_metrics_serving_1d",
     "toYYYYMM(date)", "(date, service, model, metric, name)", "cityHash64(service)", "fact"),
    ("collectors_summary", f"{COLLECTORS_DIR}/raw_token_metrics.sql", "fact", "raw_token_metrics_summary_1d",
     "toYYYYMM(date)", "(date, service)", "cityHash64(service)", "fact"),
    ("collectors_audit", f"{COLLECTORS_DIR}/raw_token_metrics.sql", "fact", "collect_audit_metrics_1d",
     "toYYYYMM(date)", "(date, service, replaced_at)", "cityHash64(service)", "fact"),
    ("collectors_registry", f"{COLLECTORS_DIR}/dim_token_metrics_service.sql", "gpu_data", "dim_token_metrics_service",
     None, "(service)", "rand()", "dim"),
    ("mart_cost", f"{MART_DIR}/mart_metrics_tables.sql", "mart", "agg_token_model_cost_1d",
     "toYYYYMM(date)", "(date, service, model)", "cityHash64(service)", "mart"),
    ("mart_check", f"{MART_DIR}/mart_metrics_tables.sql", "mart", "token_metrics_check_1d",
     "toYYYYMM(date)", "(date, service, check_name, model, gpu_type)", "cityHash64(service)", "mart"),
    ("mart_share", f"{MART_DIR}/mart_metrics_tables.sql", "mart", "agg_token_model_share_1d",
     "toYYYYMM(date)", "(date, model, service, provider_service)", "cityHash64(model)", "mart"),
    ("mart_group", f"{MART_DIR}/mart_metrics_tables.sql", "mart", "agg_token_gpu_group_1d",
     "toYYYYMM(date)", "(date, service_group, gpu_type)", "cityHash64(service_group)", "mart"),
    ("assets_dim_alias", f"{ASSETS_DIR}/dim_token_model_alias.sql", "gpu_data", "dim_token_model_alias",
     None, "(alias, effective_from)", "cityHash64(alias)", "dim"),
    ("assets_dim_tco", f"{ASSETS_DIR}/dim_token_gpu_tco.sql", "gpu_data", "dim_token_gpu_tco",
     None, "(gpu_type, effective_from)", "cityHash64(gpu_type)", "dim"),
    ("assets_dim_allocation", f"{ASSETS_DIR}/dim_token_gpu_allocation.sql", "gpu_data", "dim_token_gpu_allocation",
     None, "(service_group, gpu_type, effective_from)", "cityHash64(service_group)", "dim"),
    ("assets_dim_vendor_price", f"{ASSETS_DIR}/dim_token_vendor_price.sql", "gpu_data", "dim_token_vendor_price",
     None, "(provider, model, tier, effective_from)", "cityHash64(model)", "dim"),
]

# 설계 §4.1/§4.2/§4.3/§6.1 컬럼 목록 (순서 포함) — _local 컬럼명은 이 목록과 정확히 같아야 한다
COLUMNS = {
    "raw_token_metrics_gpu_1d": [
        "date", "service_group", "service", "model", "gpu_type", "category", "gpu_count", "gpu_hours",
        "flags", "source_type", "generated_at", "collected_at"],
    "raw_token_metrics_serving_1d": [
        "date", "service_group", "service", "model", "metric", "name", "unit", "p50", "p90", "p95", "p99",
        "flags", "source_type", "generated_at", "collected_at"],
    "raw_token_metrics_summary_1d": [
        "date", "service_group", "service", "reported_service_group", "reported_service", "engine_type",
        "engine_version", "gpu_rows", "serving_rows", "custom_rows", "rejected_rows", "merged_dups",
        "source_type", "generated_at", "collected_at"],
    "collect_audit_metrics_1d": [
        "date", "service", "prev_generated_at", "prev_collected_at", "prev_source_type", "prev_gpu_rows",
        "prev_gpu_hours_sum", "prev_serving_rows", "replaced_at"],
    "dim_token_metrics_service": [
        "service_group", "service", "base_url", "enabled", "api_since", "coverage_since", "until",
        "expect_gpu", "expect_serving", "usage_includes_consumers", "note", "updated_at"],
    "agg_token_model_cost_1d": [
        "date", "service_group", "service", "model", "serving_gpu_hours", "standby_gpu_hours", "test_gpu_hours",
        "flagged_gpu_hours", "equiv_gpu_count", "scaled_intraday", "model_cost_krw", "input_tokens",
        "cache_read_tokens", "cache_creation_tokens", "output_tokens", "requests", "uncached_tokens",
        "cached_tokens", "total_tokens", "weighted_tokens", "tokens_per_gpu_hour", "gpu_type_mix",
        "model_registered", "tco_missing", "has_token_rows", "has_gpu_rows", "quality_flag", "created_by"],
    "token_metrics_check_1d": [
        "date", "service_group", "service", "check_name", "model", "gpu_type", "severity", "observed",
        "threshold", "detail", "source_type", "created_by"],
    "agg_token_model_share_1d": [
        "date", "model", "service", "service_group", "provider_service", "is_provider", "denominator_mode",
        "service_wtokens", "model_total_wtokens", "share", "model_cost_krw", "allocated_cost_krw",
        "quality_flag", "created_by"],
    "agg_token_gpu_group_1d": [
        "date", "service_group", "gpu_type", "allocated_gpu_hours", "group_total_cost_krw", "serving_gpu_hours",
        "standby_gpu_hours", "test_gpu_hours", "reported_gpu_hours_total", "flagged_gpu_hours",
        "model_cost_sum_krw", "test_cost_krw", "idle_gpu_hours", "idle_cost_krw", "unattributed_cost_krw",
        "identity_gap_krw", "utilization", "over_report", "equiv_gpu_count", "tco_missing",
        "allocation_source", "quality_flag", "created_by"],
    "dim_token_model_alias": ["alias", "effective_from", "canonical", "defining_service", "source", "note"],
    "dim_token_gpu_tco": ["gpu_type", "effective_from", "tco_krw_per_gpu_hour", "currency", "basis", "note"],
    "dim_token_gpu_allocation": ["service_group", "gpu_type", "effective_from", "allocated_gpu_count", "source", "note"],
    "dim_token_vendor_price": [
        "provider", "model", "tier", "effective_from", "krw_per_mtok_input", "krw_per_mtok_cached",
        "krw_per_mtok_cache_creation", "krw_per_mtok_output", "note"],
}

_CREATE_RE = re.compile(
    r"CREATE TABLE IF NOT EXISTS (?P<name>[a-z_]+\.[a-z0-9_]+)\n"
    r"ON CLUSTER 'gpu-monitoring'\n"
    r"\(\n(?P<cols>.*?)\n\)\n"
    r"(?P<engine>ENGINE = .*?;)",
    re.S,
)
# e2e 단일노드 변환 (collectors/mart/assets run_e2e.sh 공통) — 이 두 정규식이 전 ENGINE에 매치해야 한다
_E2E_REPL_RE = re.compile(r"ENGINE = ReplicatedMergeTree\([^)]*\)", re.S)
_E2E_DIST_RE = re.compile(r"ENGINE = Distributed\('gpu-monitoring', '(\w+)', '(\w+)',[\s\S]*?\);")

# GRANT 두 형식 (collectors 형식 / 정규형)
_GRANT_COLLECTORS_RE = re.compile(r"^GRANT (?P<priv>.+?) ON (?P<tbl>\S+)\s+TO mart ON CLUSTER 'gpu-monitoring';$")
_GRANT_CANONICAL_RE = re.compile(r"^GRANT ON CLUSTER 'gpu-monitoring' (?P<priv>.+?) ON (?P<tbl>\S+)\s+TO mart;$")


def _read(rel: str) -> str:
    path = REPO_ROOT / rel
    assert path.exists(), f"매니페스트 파일 부재: {rel}"
    return path.read_text(encoding="utf-8")


def _blocks(text: str) -> dict:
    return {m.group("name"): m for m in _CREATE_RE.finditer(text)}


def _columns(cols_text: str) -> list:
    """(name, type) 목록 — CONSTRAINT·주석·빈 줄 제외. 타입은 두 번째 토큰(콤마 제거)."""
    out = []
    for raw in cols_text.split("\n"):
        line = raw.strip()
        if not line or line.startswith("--") or line.startswith("CONSTRAINT"):
            continue
        tokens = line.split()
        out.append((tokens[0], tokens[1].rstrip(",")))
    return out


def _grants(text: str, form: str) -> set:
    pat = _GRANT_COLLECTORS_RE if form == "collectors" else _GRANT_CANONICAL_RE
    found = set()
    for raw in text.split("\n"):
        line = raw.strip()
        if not line or line.startswith("--"):
            continue
        m = pat.match(line)
        assert m, f"GRANT 형식 위반({form}): {line}"
        privs = frozenset(p.strip() for p in m.group("priv").split(","))
        found.add((privs, m.group("tbl")))
    return found


# ---------------------------------------------------------------- 파일 공통

def _code_lines(text: str) -> str:
    return "\n".join(l for l in text.split("\n") if not l.lstrip().startswith("--"))


@pytest.mark.parametrize("rel", MANIFEST, ids=[p.split("/")[-1] + "@" + p.split("/")[0] for p in MANIFEST])
def test_manifest_file_hygiene(rel):
    text = _read(rel)
    code = _code_lines(text)
    assert "\t" not in text, "탭 금지"
    assert text.endswith("\n"), "개행으로 종료"
    assert "CREATE DATABASE" not in code, "신규 파일은 DB를 만들지 않는다 (fact/gpu_data/mart 존재 전제)"
    assert "CREATE USER" not in code
    assert "harbor." not in text and "@" not in text, "공개 레포 — 사내 주소·이메일 금지"


# ---------------------------------------------------------------- 테이블 쌍

@pytest.mark.parametrize("case", TABLES, ids=[c[0] for c in TABLES])
def test_table_pair_conventions(case):
    _, rel, db, table, partition, order_by, sharding, kind = case
    text = _read(rel)
    blocks = _blocks(text)
    local_name = f"{db}.{table}_local"
    dist_name = f"{db}.{table}_dist"
    assert local_name in blocks, f"{local_name} CREATE 블록 부재 (ON CLUSTER 단독 줄·'(' ')' 단독 줄 형식 확인)"
    assert dist_name in blocks, f"{dist_name} CREATE 블록 부재"
    local = blocks[local_name]
    dist = blocks[dist_name]

    engine_local = local.group("engine")
    expected_repl = (
        "ENGINE = ReplicatedMergeTree(\n"
        f"    '/clickhouse/tables/{{shard}}/{db}/{table}_local',\n"
        "    '{replica}'\n"
        ")"
    )
    assert engine_local.startswith(expected_repl), f"{local_name}: ReplicatedMergeTree/ZK 경로 형식"
    assert f"ORDER BY {order_by}" in engine_local, f"{local_name}: ORDER BY {order_by}"
    assert "SETTINGS index_granularity = 8192;" in engine_local
    if kind == "dim":
        assert "PARTITION BY" not in engine_local, f"{local_name}: dim은 파티션 없음"
        assert "TTL" not in engine_local, f"{local_name}: dim은 TTL 없음"
    else:
        assert f"PARTITION BY {partition}" in engine_local
        assert "TTL date + INTERVAL 25 MONTH" in engine_local

    engine_dist = dist.group("engine")
    expected_dist = re.compile(
        r"^ENGINE = Distributed\('gpu-monitoring', '" + re.escape(db) + r"', '" + re.escape(table)
        + r"_local',\s*" + re.escape(sharding) + r"\);$"
    )
    assert expected_dist.match(engine_dist), f"{dist_name}: Distributed 인자 형식 — {engine_dist!r}"

    local_cols = _columns(local.group("cols"))
    dist_cols = _columns(dist.group("cols"))
    assert [c[0] for c in local_cols] == COLUMNS[table], f"{local_name}: 컬럼 목록·순서가 설계와 다름"
    assert local_cols == dist_cols, f"{table}: _local/_dist (컬럼, 타입) 불일치"
    assert "COMMENT" not in dist.group("cols"), f"{dist_name}: _dist에는 COMMENT 없음"
    assert "DEFAULT" not in dist.group("cols"), f"{dist_name}: _dist에는 DEFAULT 없음"
    for m in re.finditer(r"COMMENT '([^']*)'", local.group("cols")):
        assert ";" not in m.group(1), f"{local_name}: COMMENT 문자열에 ';' 금지 (e2e run_e2e.sh가 ';'로 문장을 분리)"

    constraint = "CONSTRAINT check_created_by CHECK created_by != ''"
    if kind == "mart":
        assert constraint in local.group("cols") and constraint in dist.group("cols")
        created_by_line = [l for l in local.group("cols").split("\n") if l.strip().startswith("created_by ")]
        assert created_by_line and "DEFAULT" not in created_by_line[0], "created_by는 DEFAULT 없음"
    else:
        assert "created_by" not in local.group("cols")

    for name, typ in local_cols:
        if name.endswith("_at"):
            assert typ == "DateTime('Asia/Seoul')", f"{table}.{name}: KST DateTime"
        assert not typ.startswith("Nullable(String"), f"{table}.{name}: 문자열은 NOT NULL('')"


@pytest.mark.parametrize("rel", [t for t in MANIFEST if "accounts" not in t and "seed_" not in t],
                         ids=lambda p: p.split("/")[-1])
def test_e2e_single_node_conversion_leaves_no_residual(rel):
    text = _read(rel)
    n_local = text.count("_local\nON CLUSTER")
    n_dist = text.count("_dist\nON CLUSTER")
    assert n_local == n_dist and n_local > 0
    converted = _E2E_REPL_RE.sub("ENGINE = MergeTree", text)
    converted = _E2E_DIST_RE.sub(r"ENGINE = Distributed('default', '\1', '\2', rand());", converted)
    code = _code_lines(converted)
    assert "ReplicatedMergeTree" not in code
    assert "Distributed('gpu-monitoring'" not in code
    assert converted.count("ENGINE = MergeTree") == n_local
    assert converted.count("Distributed('default'") == n_dist


# ---------------------------------------------------------------- GRANT (설계 §4.2 표)

def _rw(db, t):
    return {(frozenset({"SELECT", "INSERT"}), f"{db}.{t}_dist"),
            (frozenset({"SELECT", "INSERT"}), f"{db}.{t}_local"),
            (frozenset({"ALTER DELETE"}), f"{db}.{t}_local")}


def test_collectors_accounts_grants():
    found = _grants(_read(f"{COLLECTORS_DIR}/accounts.sql"), "collectors")
    expected = set()
    for t in ("raw_token_metrics_gpu_1d", "raw_token_metrics_serving_1d", "raw_token_metrics_summary_1d"):
        expected |= _rw("fact", t)
    expected |= {(frozenset({"SELECT", "INSERT"}), "fact.collect_audit_metrics_1d_dist"),
                 (frozenset({"SELECT", "INSERT"}), "fact.collect_audit_metrics_1d_local")}
    expected |= _rw("gpu_data", "dim_token_metrics_service")
    expected.add((frozenset({"SELECT"}), "gpu_data.dim_token_service_dist"))
    assert found == expected
    assert not any("collect_audit_metrics_1d" in tbl and "ALTER DELETE" in privs for privs, tbl in found)


def test_mart_accounts_grants():
    found = _grants(_read(f"{MART_DIR}/accounts.sql"), "canonical")
    expected = set()
    for t in ("agg_token_model_cost_1d", "token_metrics_check_1d", "agg_token_model_share_1d", "agg_token_gpu_group_1d"):
        expected.add((frozenset({"SELECT", "INSERT"}), f"mart.{t}_dist"))
        expected.add((frozenset({"ALTER DELETE"}), f"mart.{t}_local"))
    for t in ("dim_token_model_alias", "dim_token_gpu_tco", "dim_token_gpu_allocation", "dim_token_vendor_price",
              "dim_token_metrics_service", "dim_token_service"):
        expected.add((frozenset({"SELECT"}), f"gpu_data.{t}_dist"))
    for t in ("raw_token_metrics_gpu_1d", "raw_token_metrics_serving_1d", "raw_token_metrics_summary_1d"):
        expected.add((frozenset({"SELECT"}), f"fact.{t}_dist"))
    expected.add((frozenset({"SELECT"}), "mart.token_usage_1d_dist"))
    expected.add((frozenset({"SELECT"}), "mart.agg_token_service_1d_dist"))
    expected.add((frozenset({"CREATE TEMPORARY TABLE"}), "*.*"))
    expected.add((frozenset({"SELECT"}), "system.mutations"))
    assert found == expected


def test_assets_accounts_metrics_grants():
    found = _grants(_read(f"{ASSETS_DIR}/accounts_metrics.sql"), "canonical")
    expected = {(frozenset({"SELECT"}), f"gpu_data.{t}_dist") for t in (
        "dim_token_model_alias", "dim_token_gpu_tco", "dim_token_gpu_allocation", "dim_token_vendor_price")}
    assert found == expected


# ---------------------------------------------------------------- 시드 (dim_holiday 3요소 + 플레이스홀더 규칙)

SEEDS = {
    "seed_dim_token_model_alias.sql": ("dim_token_model_alias", "(alias, effective_from)",
                                       ["dup_key", "alias_maps_to_two_canonicals", "alias_loop", "empty_canonical",
                                        "missing_identity_row", "service_not_in_registry"]),
    "seed_dim_token_gpu_tco.sql": ("dim_token_gpu_tco", "(gpu_type, effective_from)",
                                   ["dup_key", "unknown_row_state", "basis_domain"]),
    "seed_dim_token_gpu_allocation.sql": ("dim_token_gpu_allocation", "(service_group, gpu_type, effective_from)",
                                          ["dup_key", "unknown_row_state"]),
    "seed_dim_token_vendor_price.sql": ("dim_token_vendor_price", "(provider, model, tier, effective_from)",
                                        ["dup_key", "unknown_row_state", "tier_domain"]),
}
ANCHOR = "-- 검증: 결과가 비어야 정상 ------------------------------------------------"


@pytest.mark.parametrize("fname", sorted(SEEDS), ids=lambda f: "seed_" + f[len("seed_dim_token_"):-4])
def test_seed_three_elements_and_placeholders(fname):
    table, key, checks = SEEDS[fname]
    text = _read(f"{ASSETS_DIR}/{fname}")
    assert f"INSERT INTO gpu_data.{table}_dist" in text
    assert f"WHERE {key} NOT IN (" in text, "NOT IN 멱등 가드 (키 튜플 형식 그대로)"
    assert "SETTINGS insert_distributed_sync = 1;" in text
    assert ANCHOR in text
    for c in checks:
        assert f"'{c}' AS check_name" in text or f"SELECT '{c}'," in text, f"검증 {c} 부재"
    assert "'unknown'" in text, "unknown 플레이스홀더 행 필수"
    code = _code_lines(text)
    assert "합성" not in code and "toNullable(" not in code, "사내 시드는 NULL 플레이스홀더만 (합성 수치 금지 — 코드 라인 기준)"
    if fname == "seed_dim_token_gpu_tco.sql":
        for g in ("'H100'", "'A100'", "'H200'", "'L40S'"):
            assert g in text
        assert "'KRW'" in text
    if fname == "seed_dim_token_vendor_price.sql":
        assert "'standard'" in text


@pytest.mark.parametrize("fname", sorted(SEEDS), ids=lambda f: "stage_fixture_" + f[len("seed_dim_token_"):-4])
def test_stage_fixture_exists_and_is_synthetic(fname):
    table, key, _ = SEEDS[fname]
    text = _read(f"{FIXTURES_DIR}/stage_{fname}")
    assert "합성" in text.split("\n")[1], "둘째 줄 헤더에 '합성' 표기 (사내 적용 금지 경고)"
    assert f"INSERT INTO gpu_data.{table}_dist" in text
    assert f"WHERE {key} NOT IN (" in text
    assert "SETTINGS insert_distributed_sync = 1;" in text
    assert ANCHOR in text
    for raw in _code_lines(text).split("\n"):
        if "toDate('2026-01-01')" in raw:
            assert "'unknown'" in raw, (
                "fixture 실값 행은 플레이스홀더 키 날짜(2026-01-01)를 재사용하지 않는다 — "
                "시드가 먼저 적용되면 NOT IN 가드가 실값 행을 무음 skip (Self-Review #4)")
