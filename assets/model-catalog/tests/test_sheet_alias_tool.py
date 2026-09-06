"""Tests for sheet_to_dim_token_model_alias_insert.py — 순수 로직(parse_sheet/render_sql/load_services) + CLI.

TDD (Plan 6a T8): 이 파일을 먼저 작성 → FAIL 확인 → 구현 → 통과.
검증 실패 경로도 데이터 행(모델명·서비스명 원문)을 절대 에코하지 않는다 — 행 번호·필드명·검증명만.
"""
import subprocess
import sys
from pathlib import Path

import pytest

from sheet_to_dim_token_model_alias_insert import (
    CHECK_NAMES,
    PLACEHOLDER_EFFECTIVE_FROM,
    AliasRow,
    SheetError,
    load_services,
    parse_sheet,
    render_sql,
)

MODULE_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = MODULE_ROOT / "sheet_to_dim_token_model_alias_insert.py"
FIXTURE_CSV = MODULE_ROOT / "fixtures" / "synthetic_model_sheet.csv"
FIXTURE_YAML = MODULE_ROOT / "fixtures" / "synthetic_endpoints_metrics.yaml"

SERVICES = {"Mock Service A", "Mock Service B"}
EF = "2026-08-26"


def _row(**overrides):
    base = {
        "canonical": "claude-sonnet-5",
        "aliases": "claude-sonnet-5-20260101",
        "defining_service": "Mock Service A",
        "effective_from": EF,
        "note": "합성",
    }
    base.update(overrides)
    return base


def _keys(rows):
    return [(r.alias, r.effective_from, r.canonical, r.defining_service, r.source) for r in rows]


# ---------------------------------------------------------------- parse_sheet

def test_identity_row_first_then_alias_rows():
    rows = parse_sheet([_row(aliases="claude-sonnet-5-20260101, sonnet-5")], None, SERVICES)
    assert _keys(rows) == [
        ("claude-sonnet-5", EF, "claude-sonnet-5", "", "metadata-sheet"),
        ("claude-sonnet-5-20260101", EF, "claude-sonnet-5", "Mock Service A", "metadata-sheet"),
        ("sonnet-5", EF, "claude-sonnet-5", "Mock Service A", "metadata-sheet"),
    ]
    assert all(isinstance(r, AliasRow) for r in rows)


def test_canonical_only_row_emits_identity_with_empty_service():
    rows = parse_sheet([_row(aliases="", defining_service="")], None, SERVICES)
    assert _keys(rows) == [("claude-sonnet-5", EF, "claude-sonnet-5", "", "metadata-sheet")]


def test_no_auto_correction_only_strip():
    rows = parse_sheet([_row(aliases="  Claude-Sonnet-5.v2 ")], None, SERVICES)
    assert rows[1].alias == "Claude-Sonnet-5.v2"


@pytest.mark.parametrize("bad", ["a,,b", "a,", ",a", " , a"])
def test_empty_alias_segment_rejected(bad):
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(aliases=bad)], None, SERVICES)
    assert "empty_alias_segment" in str(excinfo.value)


def test_empty_canonical_rejected():
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(canonical="  ")], None, SERVICES)
    assert "empty_canonical" in str(excinfo.value)


def test_alias_maps_to_two_canonicals_rejected():
    rows_in = [
        _row(canonical="claude-sonnet-5", aliases="sonnet"),
        _row(canonical="claude-opus-4-8", aliases="sonnet", defining_service="Mock Service B"),
    ]
    with pytest.raises(SheetError) as excinfo:
        parse_sheet(rows_in, None, SERVICES)
    msg = str(excinfo.value)
    assert "alias_maps_to_two_canonicals" in msg
    assert "2번째" in msg and "1번째" in msg
    assert "sonnet" not in msg


def test_remap_across_effective_from_allowed():
    rows_in = [
        _row(canonical="claude-sonnet-5", aliases="sonnet", effective_from="2026-08-26"),
        _row(canonical="claude-opus-4-8", aliases="sonnet", defining_service="Mock Service B",
             effective_from="2026-09-01"),
    ]
    rows = parse_sheet(rows_in, None, SERVICES)
    assert len(rows) == 4


def test_alias_loop_rejected():
    rows_in = [
        _row(canonical="claude-sonnet-5", aliases="sonnet-5"),
        _row(canonical="sonnet-5", aliases="sonnet-5-legacy", defining_service="Mock Service B"),
    ]
    with pytest.raises(SheetError) as excinfo:
        parse_sheet(rows_in, None, SERVICES)
    msg = str(excinfo.value)
    assert "alias_loop" in msg
    assert "sonnet" not in msg


def test_dup_key_identity_rejected():
    rows_in = [_row(aliases=""), _row(aliases="")]
    with pytest.raises(SheetError) as excinfo:
        parse_sheet(rows_in, None, SERVICES)
    assert "dup_key" in str(excinfo.value)


def test_dup_key_alias_equals_own_canonical_rejected():
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(aliases="claude-sonnet-5, sonnet-5")], None, SERVICES)
    assert "dup_key" in str(excinfo.value)


def test_service_not_in_registry_rejected():
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(defining_service="Mock Service Z")], None, SERVICES)
    msg = str(excinfo.value)
    assert "service_not_in_registry" in msg
    assert "Mock Service Z" not in msg


def test_alias_row_without_service_rejected():
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(defining_service="")], None, SERVICES)
    assert "service_not_in_registry" in str(excinfo.value)


def test_blank_effective_from_uses_default():
    rows = parse_sheet([_row(effective_from="")], "2026-09-01", SERVICES)
    assert {r.effective_from for r in rows} == {"2026-09-01"}


def test_blank_effective_from_without_default_rejected():
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(effective_from="")], None, SERVICES)
    assert "--effective-from" in str(excinfo.value)


def test_bad_date_rejected():
    with pytest.raises(SheetError):
        parse_sheet([_row(effective_from="2026/08/26")], None, SERVICES)


@pytest.mark.parametrize("kw", [{"effective_from": PLACEHOLDER_EFFECTIVE_FROM}, {"effective_from": ""}])
def test_placeholder_effective_from_rejected(kw):
    default = PLACEHOLDER_EFFECTIVE_FROM if kw["effective_from"] == "" else None
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(**kw)], default, SERVICES)
    assert "effective_from_is_placeholder_date" in str(excinfo.value)


# ---------------------------------------------------------------- load_services

def test_load_services_from_fixture_yaml():
    assert load_services([FIXTURE_YAML]) == SERVICES


def test_load_services_multiple_files_union(tmp_path):
    other = tmp_path / "endpoints-extra.yaml"
    other.write_text("services:\n  - service: \"Mock Service C\"   # 주석\n    enabled: true\n", encoding="utf-8")
    assert load_services([FIXTURE_YAML, other]) == SERVICES | {"Mock Service C"}


def test_load_services_ignores_comments_and_rejects_empty(tmp_path):
    empty = tmp_path / "endpoints-empty.yaml"
    empty.write_text("# service: \"Commented Out\"\nservices: []\n", encoding="utf-8")
    with pytest.raises(SheetError):
        load_services([empty])


# ---------------------------------------------------------------- render_sql

def test_render_sql_three_elements_and_six_checks():
    rows = parse_sheet([_row()], None, SERVICES)
    sql = render_sql(rows, 500, "synthetic_model_sheet.csv", None)
    assert "INSERT INTO gpu_data.dim_token_model_alias_dist" in sql
    assert "(alias, effective_from, canonical, defining_service, source, note)" in sql
    assert "WHERE (alias, effective_from) NOT IN (" in sql
    assert "SETTINGS insert_distributed_sync = 1;" in sql
    assert "-- 검증: 결과가 비어야 정상" in sql
    assert "synthetic_model_sheet.csv" in sql
    tail = sql.split("-- 검증: 결과가 비어야 정상", 1)[1]
    positions = [tail.index(f"'{name}'") for name in CHECK_NAMES]
    assert positions == sorted(positions), "검증 6종 순서 = CHECK_NAMES (service_not_in_registry 마지막)"
    assert "SELECT service FROM gpu_data.dim_token_metrics_service_dist" in tail
    assert "GLOBAL IN" in tail and "GLOBAL NOT IN" in tail
    assert tail.count("UNION ALL") == 5


def test_render_sql_deterministic():
    rows = parse_sheet([_row(), _row(canonical="claude-opus-4-8", aliases="opus-4.8",
                                     defining_service="Mock Service B")], None, SERVICES)
    assert render_sql(rows, 500, "s.csv", None) == render_sql(rows, 500, "s.csv", None)


def test_render_sql_quote_and_backslash_escape():
    rows = parse_sheet([_row(aliases="o'brien\\v2")], None, SERVICES)
    sql = render_sql(rows, 500, "s.csv", None)
    assert "o\\'brien\\\\v2" in sql


def test_render_sql_chunking():
    rows_in = [_row(canonical=f"model-{i}", aliases=f"m{i}-a, m{i}-b") for i in range(5)]
    rows = parse_sheet(rows_in, None, SERVICES)
    assert len(rows) == 15
    sql = render_sql(rows, 4, "s.csv", None)
    assert sql.count("INSERT INTO gpu_data.dim_token_model_alias_dist") == 4
    assert sql.count("'dup_key' AS check_name") == 1


def test_render_sql_target_db_override():
    rows = parse_sheet([_row()], None, SERVICES)
    sql = render_sql(rows, 500, "s.csv", None, "token_verify_dim")
    assert "token_verify_dim.dim_token_model_alias_dist" in sql
    assert "token_verify_dim.dim_token_metrics_service_dist" in sql
    assert "gpu_data." not in sql


# ---------------------------------------------------------------- CLI

def _run(*extra, csv_path=FIXTURE_CSV, out_path):
    return subprocess.run(
        [sys.executable, str(TOOL_PATH), "--csv", str(csv_path), "--out", str(out_path),
         "--services", str(FIXTURE_YAML), *extra],
        capture_output=True, text=True,
    )


def test_cli_roundtrip_fixture(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run("--effective-from", EF, out_path=out_path)
    assert result.returncode == 0, result.stderr
    body = out_path.read_text(encoding="utf-8")
    assert body.count("INSERT INTO gpu_data.dim_token_model_alias_dist") == 1
    # 3 canonical(identity 3) + alias 3 = 6행
    assert body.count(" AS alias, ") == 6
    assert "'claude-haiku-4-5' AS alias, toDate('2026-08-26') AS effective_from, 'claude-haiku-4-5' AS canonical, '' AS defining_service" in body
    assert "출력 행수: 6 (identity 3, alias 3)" in result.stdout
    assert "레지스트리 서비스 수: 2" in result.stdout


def test_cli_target_db_option(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run("--effective-from", EF, "--target-db", "token_verify_dim", out_path=out_path)
    assert result.returncode == 0, result.stderr
    body = out_path.read_text(encoding="utf-8")
    assert "INSERT INTO token_verify_dim.dim_token_model_alias_dist" in body
    assert "gpu_data." not in body


def test_cli_target_db_invalid_is_usage_error(tmp_path):
    result = _run("--effective-from", EF, "--target-db", "mart", out_path=tmp_path / "o.sql")
    assert result.returncode == 2


def test_cli_missing_default_effective_from_fails_on_blank_row(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run(out_path=out_path)
    assert result.returncode == 1
    assert "--effective-from" in result.stderr
    assert not out_path.exists()


def test_cli_stdout_summary_only(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run("--effective-from", EF, out_path=out_path)
    assert result.returncode == 0
    assert "INSERT INTO" not in result.stdout
    assert "claude-" not in result.stdout
    assert "Mock Service" not in result.stdout
    assert result.stderr == ""


def test_cli_error_output_has_no_data_rows(tmp_path):
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(
        "canonical,aliases,defining_service,effective_from,note\n"
        "secret-model-name,secret-alias,Mock Service Q,2026-08-26,\n",
        encoding="utf-8",
    )
    out_path = tmp_path / "out.sql"
    result = _run(csv_path=bad_csv, out_path=out_path)
    assert result.returncode == 1
    assert "service_not_in_registry" in result.stderr
    for secret in ("secret-model-name", "secret-alias", "Mock Service Q"):
        assert secret not in result.stderr and secret not in result.stdout
    assert not out_path.exists()


def test_cli_missing_required_header(tmp_path):
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text("model,alias\nx,y\n", encoding="utf-8")
    result = _run(csv_path=bad_csv, out_path=tmp_path / "o.sql")
    assert result.returncode == 1
    assert "필수 컬럼 없음" in result.stderr


def test_cli_accepts_utf8_bom_csv(tmp_path):
    bom_csv = tmp_path / "bom.csv"
    bom_csv.write_bytes(b"\xef\xbb\xbf" + FIXTURE_CSV.read_bytes())
    out_path = tmp_path / "out.sql"
    result = _run("--effective-from", EF, csv_path=bom_csv, out_path=out_path)
    assert result.returncode == 0, result.stderr
    assert "출력 행수: 6 (identity 3, alias 3)" in result.stdout


def test_cli_rejects_non_utf8_csv(tmp_path):
    marker = "가나다"
    text = FIXTURE_CSV.read_text(encoding="utf-8")
    assert marker not in text
    text = text.replace("—", "-")  # cp949로 인코딩 불가한 em dash 제거
    text = text.replace("합성 - 날짜 접미 alias 2종", f"합성 - 날짜 접미 alias 2종 {marker}")
    bad_csv = tmp_path / "bad_cp949.csv"
    bad_csv.write_bytes(text.encode("cp949"))
    out_path = tmp_path / "out.sql"
    result = _run(csv_path=bad_csv, out_path=out_path)
    assert result.returncode == 1
    assert "UTF-8이 아님" in result.stderr
    assert marker not in result.stderr
    assert marker not in result.stdout
    assert "claude-sonnet-5" not in result.stderr
    assert not out_path.exists()


def test_cli_unwritable_out_reports_cleanly(tmp_path):
    out_path = tmp_path / "no_such_dir" / "x.sql"
    result = _run("--effective-from", EF, out_path=out_path)
    assert result.returncode == 1
    assert "--out 파일을 쓸 수 없음" in result.stderr
    assert "Traceback" not in result.stderr
