"""Tests for csv_to_layer_c_dim_insert.py — 순수 로직(parse_rows/render_sql) + CLI, 3테이블 공통.

TDD (Plan 6a T9): 이 파일을 먼저 작성 → FAIL 확인 → 구현 → 통과.
검증 실패 경로도 데이터 행(기종·그룹·모델·단가 원문)을 절대 에코하지 않는다 — 행 번호·필드명·검증명만.
"""
import subprocess
import sys
from pathlib import Path

import pytest

from csv_to_layer_c_dim_insert import (
    PLACEHOLDER_EFFECTIVE_FROM,
    TABLE_SPECS,
    DimRow,
    LayerCError,
    insert_columns,
    parse_rows,
    render_sql,
    required_headers,
)

MODULE_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = MODULE_ROOT / "csv_to_layer_c_dim_insert.py"
FIXTURES = {
    "gpu_tco": MODULE_ROOT / "fixtures" / "synthetic_layer_c_tco.csv",
    "gpu_allocation": MODULE_ROOT / "fixtures" / "synthetic_layer_c_allocation.csv",
    "vendor_price": MODULE_ROOT / "fixtures" / "synthetic_layer_c_price.csv",
}
EF = "2026-08-26"

TCO = TABLE_SPECS["gpu_tco"]
ALLOC = TABLE_SPECS["gpu_allocation"]
PRICE = TABLE_SPECS["vendor_price"]


def _tco(**o):
    base = {"gpu_type": "H100", "effective_from": EF, "tco_krw_per_gpu_hour": "4300", "basis": "tco", "note": "합성"}
    base.update(o)
    return base


def _alloc(**o):
    base = {"service_group": "Mock Group", "gpu_type": "H100", "effective_from": EF,
            "allocated_gpu_count": "8", "source": "", "note": "합성"}
    base.update(o)
    return base


def _price(**o):
    base = {"provider": "anthropic", "model": "claude-sonnet-5", "tier": "", "effective_from": EF,
            "krw_per_mtok_input": "4050", "krw_per_mtok_cached": "405",
            "krw_per_mtok_cache_creation": "5062.5", "krw_per_mtok_output": "20250", "note": "합성"}
    base.update(o)
    return base


# ---------------------------------------------------------------- 스펙 계약

def test_table_specs_match_ddl_column_order():
    assert insert_columns(TCO) == ("gpu_type", "effective_from", "tco_krw_per_gpu_hour", "currency", "basis", "note")
    assert insert_columns(ALLOC) == ("service_group", "gpu_type", "effective_from", "allocated_gpu_count", "source", "note")
    assert insert_columns(PRICE) == (
        "provider", "model", "tier", "effective_from", "krw_per_mtok_input", "krw_per_mtok_cached",
        "krw_per_mtok_cache_creation", "krw_per_mtok_output", "note",
    )


def test_required_headers_and_default_out_names_are_gitignored_patterns():
    assert required_headers(TCO) == ("gpu_type", "tco_krw_per_gpu_hour")
    assert required_headers(ALLOC) == ("service_group", "gpu_type", "allocated_gpu_count")
    assert required_headers(PRICE) == ("provider", "model", "krw_per_mtok_input", "krw_per_mtok_output")
    assert TCO.default_out == "dim_token_gpu_tco_insert.sql"
    assert ALLOC.default_out == "dim_token_gpu_allocation_insert.sql"
    assert PRICE.default_out == "dim_token_vendor_price_insert.sql"


# ---------------------------------------------------------------- parse_rows (공통 규칙)

def test_tco_row_parsed_with_krw_and_float():
    rows = parse_rows(TCO, [_tco()], None)
    assert isinstance(rows[0], DimRow)
    assert rows[0].values == {"gpu_type": "H100", "effective_from": EF, "tco_krw_per_gpu_hour": 4300.0,
                              "currency": "KRW", "basis": "tco", "note": "합성"}


def test_alloc_defaults_source_manual_and_allows_zero():
    rows = parse_rows(ALLOC, [_alloc(allocated_gpu_count="0")], None)
    assert rows[0].values["source"] == "manual"
    assert rows[0].values["allocated_gpu_count"] == 0.0


def test_price_defaults_tier_standard_and_optional_null():
    rows = parse_rows(PRICE, [_price(krw_per_mtok_cached="", krw_per_mtok_cache_creation="")], None)
    assert rows[0].values["tier"] == "standard"
    assert rows[0].values["krw_per_mtok_cached"] is None
    assert rows[0].values["krw_per_mtok_cache_creation"] is None
    assert rows[0].values["krw_per_mtok_input"] == 4050.0


def test_empty_numeric_becomes_null_placeholder():
    rows = parse_rows(TCO, [_tco(tco_krw_per_gpu_hour="")], None)
    assert rows[0].values["tco_krw_per_gpu_hour"] is None


def test_no_auto_correction_only_strip():
    rows = parse_rows(TCO, [_tco(gpu_type="  h100-sxm ")], None)
    assert rows[0].values["gpu_type"] == "h100-sxm"


def test_thousands_separator_accepted():
    rows = parse_rows(PRICE, [_price(krw_per_mtok_output="20,250")], None)
    assert rows[0].values["krw_per_mtok_output"] == 20250.0


@pytest.mark.parametrize("spec,row", [(TCO, _tco(gpu_type=" ")), (ALLOC, _alloc(service_group="")),
                                      (PRICE, _price(model=""))])
def test_empty_key_rejected(spec, row):
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(spec, [row], None)
    assert "empty_key" in str(excinfo.value)


@pytest.mark.parametrize("spec,row", [(TCO, _tco(gpu_type="unknown")), (ALLOC, _alloc(gpu_type="unknown")),
                                      (PRICE, _price(provider="unknown"))])
def test_unknown_reserved_rejected(spec, row):
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(spec, [row], None)
    assert "unknown_reserved" in str(excinfo.value)


@pytest.mark.parametrize("bad", ["abc", "nan", "inf", "4300원"])
def test_bad_number_rejected(bad):
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(TCO, [_tco(tco_krw_per_gpu_hour=bad)], None)
    assert "bad_number" in str(excinfo.value)


@pytest.mark.parametrize("spec,row", [(TCO, _tco(tco_krw_per_gpu_hour="-1")), (ALLOC, _alloc(allocated_gpu_count="-8")),
                                      (PRICE, _price(krw_per_mtok_cached="-0.5"))])
def test_negative_value_rejected(spec, row):
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(spec, [row], None)
    assert "negative_value" in str(excinfo.value)


def test_basis_domain_rejected():
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(TCO, [_tco(basis="rental")], None)
    assert "basis_domain" in str(excinfo.value)
    assert "rental" not in str(excinfo.value)


def test_tier_domain_rejected():
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(PRICE, [_price(tier="premium")], None)
    assert "tier_domain" in str(excinfo.value)
    assert "premium" not in str(excinfo.value)


def test_currency_column_must_be_krw_if_present():
    rows = parse_rows(TCO, [_tco(currency="KRW")], None)
    assert rows[0].values["currency"] == "KRW"
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(TCO, [_tco(currency="USD")], None)
    assert "currency_krw" in str(excinfo.value)


def test_dup_key_rejected_with_row_numbers():
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(ALLOC, [_alloc(), _alloc(allocated_gpu_count="4")], None)
    msg = str(excinfo.value)
    assert "dup_key" in msg and "2번째" in msg and "1번째" in msg
    assert "Mock Group" not in msg


def test_same_key_different_effective_from_allowed():
    rows = parse_rows(ALLOC, [_alloc(), _alloc(effective_from="2026-09-01", allocated_gpu_count="0")], None)
    assert len(rows) == 2


def test_blank_effective_from_uses_default():
    rows = parse_rows(TCO, [_tco(effective_from="")], "2026-09-01")
    assert rows[0].values["effective_from"] == "2026-09-01"


def test_blank_effective_from_without_default_rejected():
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(TCO, [_tco(effective_from="")], None)
    assert "--effective-from" in str(excinfo.value)


def test_bad_date_rejected():
    with pytest.raises(LayerCError):
        parse_rows(TCO, [_tco(effective_from="26/08/2026")], None)


@pytest.mark.parametrize("kw", [{"effective_from": PLACEHOLDER_EFFECTIVE_FROM}, {"effective_from": ""}])
def test_placeholder_effective_from_rejected(kw):
    default = PLACEHOLDER_EFFECTIVE_FROM if kw["effective_from"] == "" else None
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(TCO, [_tco(**kw)], default)
    assert "effective_from_is_placeholder_date" in str(excinfo.value)


# ---------------------------------------------------------------- render_sql

def test_render_sql_tco_three_elements_and_checks():
    rows = parse_rows(TCO, [_tco(), _tco(gpu_type="H200", tco_krw_per_gpu_hour="")], None)
    sql = render_sql(TCO, rows, 500, "synthetic_layer_c_tco.csv", None)
    assert "INSERT INTO gpu_data.dim_token_gpu_tco_dist" in sql
    assert "(gpu_type, effective_from, tco_krw_per_gpu_hour, currency, basis, note)" in sql
    assert "SELECT 'H100' AS gpu_type, toDate('2026-08-26') AS effective_from, CAST(4300.0 AS Nullable(Float64)) AS tco_krw_per_gpu_hour, 'KRW' AS currency, 'tco' AS basis, '합성' AS note" in sql
    assert "SELECT 'H200', toDate('2026-08-26'), CAST(NULL AS Nullable(Float64)), 'KRW', 'tco', '합성'" in sql
    assert "WHERE (gpu_type, effective_from) NOT IN (" in sql
    assert "SETTINGS insert_distributed_sync = 1;" in sql
    assert "-- 검증: 결과가 비어야 정상" in sql
    assert "synthetic_layer_c_tco.csv" in sql
    tail = sql.split("-- 검증: 결과가 비어야 정상", 1)[1]
    positions = [tail.index(f"'{name}'") for name in TCO.check_names]
    assert positions == sorted(positions)
    assert tail.count("UNION ALL") == 3


def test_render_sql_alloc_checks_and_concat_key():
    rows = parse_rows(ALLOC, [_alloc()], None)
    sql = render_sql(ALLOC, rows, 500, "a.csv", None)
    assert "WHERE (service_group, gpu_type, effective_from) NOT IN (" in sql
    tail = sql.split("-- 검증: 결과가 비어야 정상", 1)[1]
    assert "concat(service_group, '/', gpu_type) AS key" in tail
    assert all(f"'{name}'" in tail for name in ALLOC.check_names)
    assert tail.count("UNION ALL") == 2


def test_render_sql_price_checks_and_concat_key():
    rows = parse_rows(PRICE, [_price()], None)
    sql = render_sql(PRICE, rows, 500, "p.csv", None)
    assert "WHERE (provider, model, tier, effective_from) NOT IN (" in sql
    assert "'standard' AS tier" in sql
    tail = sql.split("-- 검증: 결과가 비어야 정상", 1)[1]
    assert "concat(provider, '/', model, '/', tier) AS key" in tail
    assert all(f"'{name}'" in tail for name in PRICE.check_names)
    assert tail.count("UNION ALL") == 2


def test_render_sql_deterministic():
    rows = parse_rows(TCO, [_tco(), _tco(gpu_type="A100", tco_krw_per_gpu_hour="2100")], None)
    assert render_sql(TCO, rows, 500, "t.csv", None) == render_sql(TCO, rows, 500, "t.csv", None)


def test_render_sql_quote_and_backslash_escape():
    rows = parse_rows(ALLOC, [_alloc(service_group="O'Brien\\Group")], None)
    sql = render_sql(ALLOC, rows, 500, "a.csv", None)
    assert "O\\'Brien\\\\Group" in sql


def test_render_sql_chunking():
    rows = parse_rows(TCO, [_tco(gpu_type=f"G{i}") for i in range(5)], None)
    sql = render_sql(TCO, rows, 2, "t.csv", None)
    assert sql.count("INSERT INTO gpu_data.dim_token_gpu_tco_dist") == 3
    assert sql.count("'dup_key' AS check_name") == 1


def test_render_sql_target_db_override():
    rows = parse_rows(PRICE, [_price()], None)
    sql = render_sql(PRICE, rows, 500, "p.csv", None, "token_verify_dim")
    assert "token_verify_dim.dim_token_vendor_price_dist" in sql
    assert "gpu_data." not in sql


# ---------------------------------------------------------------- CLI

def _run(table, *extra, csv_path=None, out_path):
    return subprocess.run(
        [sys.executable, str(TOOL_PATH), "--table", table, "--csv", str(csv_path or FIXTURES[table]),
         "--out", str(out_path), *extra],
        capture_output=True, text=True,
    )


@pytest.mark.parametrize("table,expected_rows,expected_nulls", [
    ("gpu_tco", 3, 1), ("gpu_allocation", 3, 0), ("vendor_price", 2, 2),
])
def test_cli_roundtrip_fixtures(tmp_path, table, expected_rows, expected_nulls):
    out_path = tmp_path / "out.sql"
    result = _run(table, "--effective-from", EF, out_path=out_path)
    assert result.returncode == 0, result.stderr
    body = out_path.read_text(encoding="utf-8")
    assert body.count(f"INSERT INTO gpu_data.{TABLE_SPECS[table].table}_dist") == 1
    assert body.count("CAST(NULL AS Nullable(Float64))") == expected_nulls
    assert f"출력 행수: {expected_rows} (NULL 숫자 셀 {expected_nulls})" in result.stdout
    assert result.stderr == ""


def test_cli_alloc_fixture_source_default_and_zero_row(tmp_path):
    out_path = tmp_path / "out.sql"
    assert _run("gpu_allocation", out_path=out_path).returncode == 0
    body = out_path.read_text(encoding="utf-8")
    assert "'A100', toDate('2026-08-26'), CAST(4.0 AS Nullable(Float64)), 'manual', " in body
    assert "'H100', toDate('2026-09-01'), CAST(0.0 AS Nullable(Float64)), 'quota-sheet', " in body


def test_cli_target_db_option(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run("gpu_tco", "--effective-from", EF, "--target-db", "token_verify_dim", out_path=out_path)
    assert result.returncode == 0, result.stderr
    body = out_path.read_text(encoding="utf-8")
    assert "INSERT INTO token_verify_dim.dim_token_gpu_tco_dist" in body
    assert "gpu_data." not in body


def test_cli_invalid_table_and_target_db_are_usage_errors(tmp_path):
    bad_table = subprocess.run(
        [sys.executable, str(TOOL_PATH), "--table", "gpu_cost", "--csv", str(FIXTURES["gpu_tco"]),
         "--out", str(tmp_path / "o.sql")], capture_output=True, text=True,
    )
    assert bad_table.returncode == 2
    assert _run("gpu_tco", "--target-db", "mart", out_path=tmp_path / "o.sql").returncode == 2


def test_cli_missing_default_effective_from_fails_on_blank_row(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run("gpu_tco", out_path=out_path)
    assert result.returncode == 1
    assert "--effective-from" in result.stderr
    assert not out_path.exists()


def test_cli_stdout_summary_only(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run("vendor_price", out_path=out_path)
    assert result.returncode == 0
    assert "INSERT INTO" not in result.stdout
    assert "claude-" not in result.stdout and "anthropic" not in result.stdout
    assert "4050" not in result.stdout


def test_cli_error_output_has_no_data_rows(tmp_path):
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(
        "gpu_type,effective_from,tco_krw_per_gpu_hour,basis,note\n"
        "SECRET-GPU,2026-08-26,99999,secret-basis,\n",
        encoding="utf-8",
    )
    out_path = tmp_path / "out.sql"
    result = _run("gpu_tco", csv_path=bad_csv, out_path=out_path)
    assert result.returncode == 1
    assert "basis_domain" in result.stderr
    for secret in ("SECRET-GPU", "99999", "secret-basis"):
        assert secret not in result.stderr and secret not in result.stdout
    assert not out_path.exists()


def test_cli_missing_required_header(tmp_path):
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text("gpu,cost\nH100,1\n", encoding="utf-8")
    result = _run("gpu_tco", csv_path=bad_csv, out_path=tmp_path / "o.sql")
    assert result.returncode == 1
    assert "필수 컬럼 없음" in result.stderr
