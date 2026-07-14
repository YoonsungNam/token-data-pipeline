"""Tests for csv_to_dim_user_org_insert.py — pure logic (parse_roster/render_sql).

TDD (Plan 4 T2): 이 파일을 먼저 작성 → FAIL 확인 → 구현 → 통과.
검증 실패 경로도 데이터 행(user_name/org 원문)을 절대 에코하지 않는다 — 행 번호·필드명만.
"""
import subprocess
import sys
from pathlib import Path

import pytest

from csv_to_dim_user_org_insert import (
    Row,
    RosterError,
    parse_roster,
    render_sql,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = Path(__file__).resolve().parents[1] / "csv_to_dim_user_org_insert.py"
FIXTURE_CSV = Path(__file__).resolve().parents[1] / "fixtures" / "synthetic_org_members.csv"


def _row(**overrides):
    base = {
        "user_id": "user-0001",
        "user_name": "합성-0001",
        "org": "A부문>X팀",
        "is_active": "1",
        "effective_from": "2026-01-01",
    }
    base.update(overrides)
    return base


def test_org_path_split_and_depth():
    rows = parse_roster(
        [
            {
                "user_id": "user-0001",
                "user_name": "합성A",
                "org": "A부문>X팀",
                "is_active": "1",
                "effective_from": "2026-01-01",
            }
        ],
        "2020-01-01",
    )
    assert rows[0].org_path == ["A부문", "X팀"]
    assert rows[0].org_depth == 2


@pytest.mark.parametrize("bad_org", ["A>>B", ">A", "A>"])
def test_empty_segment_rejected(bad_org):
    with pytest.raises(RosterError):
        parse_roster([_row(org=bad_org)], "2020-01-01")


def test_duplicate_key_rejected():
    rows_in = [
        _row(user_id="user-0002", effective_from="2026-01-01"),
        _row(user_id="user-0002", effective_from="2026-01-01"),
    ]
    with pytest.raises(RosterError) as excinfo:
        parse_roster(rows_in, "2020-01-01")
    # 행 번호 포함 확인 (데이터 값 에코는 아님)
    assert "2" in str(excinfo.value)


def test_anon_user_name_preserved_with_notice(tmp_path):
    # 2026-07-14 결정: anon-* user_name 강제 빈 문자열 로직 폐지 — 값 보존 + stderr 안내
    # (카운트만, 이름 원문은 미출력 — §7.2 데이터 경계 원칙 유지).
    rows = parse_roster(
        [_row(user_id="anon-0001", user_name="합성핸들-테스트")],
        "2020-01-01",
    )
    assert rows[0].user_name == "합성핸들-테스트"

    csv_path = tmp_path / "anon.csv"
    csv_path.write_text(
        "user_id,user_name,org,is_active,effective_from\n"
        "anon-0001,합성핸들-테스트,A부문>X팀,1,2026-01-01\n",
        encoding="utf-8",
    )
    out_path = tmp_path / "out.sql"
    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--csv",
            str(csv_path),
            "--out",
            str(out_path),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    sql_text = out_path.read_text(encoding="utf-8")
    assert "합성핸들-테스트" in sql_text            # 강제 빈 문자열 아님 — 값 보존
    assert "anon 1행" in result.stderr               # 카운트 안내(stderr)
    assert "비실명 핸들명" in result.stderr
    assert "합성핸들-테스트" not in result.stderr    # 데이터 원문은 stderr에도 미출력
    assert "합성핸들-테스트" not in result.stdout


def test_default_effective_from_applied():
    rows = parse_roster([_row(effective_from="")], "2020-01-01")
    assert rows[0].effective_from == "2020-01-01"


def test_render_sql_three_elements():
    rows = parse_roster([_row(user_id="user-0003")], "2020-01-01")
    sql = render_sql(rows, 500, "synthetic_org_members.csv", "2020-01-01")
    assert "NOT IN" in sql
    assert "SETTINGS insert_distributed_sync = 1" in sql
    assert "-- 검증: 결과가 비어야 정상" in sql
    assert "synthetic_org_members.csv" in sql
    assert "1" in sql.splitlines()[0] or any("행수: 1" in line for line in sql.splitlines())


def test_render_sql_deterministic():
    rows = parse_roster(
        [_row(user_id="user-0004"), _row(user_id="user-0005")], "2020-01-01"
    )
    sql1 = render_sql(rows, 500, "synthetic_org_members.csv", "2020-01-01")
    sql2 = render_sql(rows, 500, "synthetic_org_members.csv", "2020-01-01")
    assert sql1 == sql2


def test_render_sql_quote_escape():
    rows = parse_roster([_row(user_id="user-0006", user_name="O'Brien")], "2020-01-01")
    sql = render_sql(rows, 500, "synthetic_org_members.csv", "2020-01-01")
    assert "O\\'Brien" in sql
    assert "O'Brien" not in sql.replace("O\\'Brien", "")


def test_render_sql_backslash_escape():
    rows = parse_roster(
        [_row(user_id="user-0007", user_name="back\\slash")], "2020-01-01"
    )
    sql = render_sql(rows, 500, "synthetic_org_members.csv", "2020-01-01")
    assert "back\\\\slash" in sql


def test_chunking():
    rows_in = [_row(user_id=f"user-000{i}", effective_from="2026-01-01") for i in range(5)]
    rows = parse_roster(rows_in, "2020-01-01")
    sql = render_sql(rows, 2, "synthetic_org_members.csv", "2020-01-01")
    assert sql.count("INSERT INTO gpu_data.dim_token_user_org_dist") == 3
    assert sql.count("'missing_key' AS check_name") == 3
    assert sql.count("'key_conflict' AS check_name") == 3
    assert sql.count("'dup_key' AS check_name") == 1


def test_stdout_summary_only(capsys, tmp_path):
    out_path = tmp_path / "out.sql"
    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--csv",
            str(FIXTURE_CSV),
            "--out",
            str(out_path),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "INSERT INTO" not in result.stdout
    assert "합성-" not in result.stdout
    assert "부문" not in result.stdout


def test_error_output_has_no_data_rows(tmp_path):
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(
        "user_id,user_name,org,is_active,effective_from\n"
        "user-9001,합성극비이름,A>>B,1,2026-01-01\n",
        encoding="utf-8",
    )
    out_path = tmp_path / "out.sql"
    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--csv",
            str(bad_csv),
            "--out",
            str(out_path),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "합성극비이름" not in result.stderr
    assert "A>>B" not in result.stderr
    assert "합성극비이름" not in result.stdout
    assert not out_path.exists()
