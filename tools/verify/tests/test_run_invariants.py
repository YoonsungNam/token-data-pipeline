"""tools/verify/run_invariants.py 테스트.

FakeCH는 tools/data-admin/tests/test_delete_data.py 관례를 따른다 — query() 호출을
기록하고 미리 준비된 result_rows를 반환한다. render()는 부수효과 없는 순수 함수라
파일 I/O 없이 문자열만으로 검증한다.
"""
import datetime as dt
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest

import run_invariants as ri

MODULE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(autouse=True)
def clean_ch_env(monkeypatch):
    """CH_*/CH_DB_* env가 로컬 개발 셸에 남아있어도 테스트가 결정론적이도록 매 테스트 전 제거."""
    for name in ("CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_CLUSTER",
                 "CH_DB_FACT", "CH_DB_DIM", "CH_DB_MART"):
        monkeypatch.delenv(name, raising=False)


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeCH:
    """query() 호출 이력 기록 + 미리 준비된 위반 행 반환."""

    def __init__(self, rows=None):
        self.rows = rows if rows is not None else []
        self.queries = []

    def query(self, sql, parameters=None):
        self.queries.append(sql)
        return FakeResult(self.rows)


# ---------------------------------------------------------------------------
# render() — 순수 치환 함수: 토큰 전부 치환, 잔존 0
# ---------------------------------------------------------------------------

def test_render_substitutes_all_tokens():
    sql = "SELECT * FROM {FACT}.t WHERE date='{DATE}' -- {DIM} {MART}"
    out = ri.render(sql, fact="f1", dim="d1", mart="m1", date="2026-07-14")
    assert out == "SELECT * FROM f1.t WHERE date='2026-07-14' -- d1 m1"


def test_render_leaves_no_token_residue_on_real_sql_file():
    sql = ri.load_sql()
    out = ri.render(sql, fact="token_verify_fact", dim="token_verify_dim",
                     mart="token_verify_mart", date="2026-07-14")
    for token in ("{FACT}", "{DIM}", "{MART}", "{DATE}"):
        assert token not in out, f"{token} 잔존"


def test_render_handles_repeated_token_occurrences():
    sql = "{FACT}.a JOIN {FACT}.b ON {FACT}.a.x = {FACT}.b.x WHERE d='{DATE}' AND d2='{DATE}'"
    out = ri.render(sql, fact="fact", dim="gpu_data", mart="mart", date="2026-07-01")
    assert "{FACT}" not in out and "{DATE}" not in out
    assert out.count("fact.") == 4                                    # a/b × 2 occurrences each
    assert out.count("2026-07-01") == 2


def test_render_is_pure_no_file_or_state_side_effects():
    sql = "{FACT}{DIM}{MART}{DATE}"
    ri.render(sql, fact="a", dim="b", mart="c", date="2026-01-01")
    # 두 번째 호출도 첫 호출과 동일한 입력에 대해 동일한 출력 (순수성)
    out2 = ri.render(sql, fact="a", dim="b", mart="c", date="2026-01-01")
    assert out2 == "abc2026-01-01"


# ---------------------------------------------------------------------------
# invariants.sql — 로드 가능 + 컬럼 계약(3컬럼) 정적 점검
# ---------------------------------------------------------------------------

def test_load_sql_reads_committed_file_and_contains_all_tokens():
    sql = ri.load_sql()
    for token in ("{FACT}", "{DIM}", "{MART}", "{DATE}"):
        assert token in sql
    assert "check_name" in sql
    assert "bad_count" in sql


def _select_list(sql: str, check_name: str) -> str:
    """check_name 검사의 SELECT 컬럼 리스트(check_name/detail/bad_count 산출식)만
    추출한다 — FROM 절 이전까지."""
    marker = f"'{check_name}' AS check_name"
    start = sql.index(marker)
    end = sql.index("\nFROM ", start)
    return sql[start:end]


def _strip_string_literals(fragment: str) -> str:
    """단일따옴표 문자열 리터럴 내용을 지운다 — 사람이 읽는 진단 라벨 텍스트에
    'user_name' 같은 단어가 포함돼도(예: 'identified rows with non-empty
    user_name: 3') 실제 컬럼 원문 참조와 혼동하지 않기 위함."""
    return re.sub(r"'[^']*'", "''", fragment)


def test_identified_name_leak_select_never_references_user_id_or_user_name_column():
    """§5.6/v1.4 로깅 계약 회귀(High 픽스): "모든 로그에 레코드 페이로드와 user_id
    원문을 남기지 않는다"는 계약을 identified_name_leak 검사 자체가 스스로
    위반해선 안 된다. 과거 리뷰에서 detail에 concat('user_id=', user_id, ...)로
    user_id 원문을 그대로 심어 stdout에 노출한 결함이 있었다(재발 방지) —
    SELECT 컬럼 리스트에 user_id·user_name 컬럼 원문 참조가 전혀 없어야 한다."""
    select_list = _select_list(ri.load_sql(), "identified_name_leak")
    stripped = _strip_string_literals(select_list)
    assert "user_id" not in stripped
    assert "user_name" not in stripped


def test_cost_null_contract_covers_unregistered_non_unknown_models():
    """cost_null_contract 커버리지 갭 회귀(Medium 픽스, §4.2 리뷰 #15 "$0 위장"
    버그와 동일 계열): 과거 WHERE는 `model='unknown' OR model IN (등록집합)`만
    봐서 dim_token_model에 아직 등록되지 않은 신규 비-unknown 모델이 검사
    대상에서 통째로 빠졌다. 수정 후에는 해당 검사 블록에 모델을 사전 필터링하는
    WHERE 절이 없어야 한다(전 모델이 registered 플래그로 판정된다)."""
    sql = ri.load_sql()
    start = sql.index("'cost_null_contract' AS check_name")
    end = sql.index("UNION ALL", start)
    block = sql[start:end]
    # 등록 판정(registered)은 남아있어야 하고, model 자체를 걸러내는
    # `model = 'unknown' OR model IN (...)` 식의 사전 WHERE 필터는 없어야 한다.
    assert "registered" in block
    assert "model = 'unknown'\n        OR model IN" not in block
    assert re.search(r"WHERE\s+date = '\{DATE\}'\s*\)", block), (
        "가장 안쪽 서브쿼리 WHERE가 date 필터만 남아 있어야 한다(모델 전건 대상)")


# ---------------------------------------------------------------------------
# 위반 행 판정 — FakeCH: 위반 행 반환 시 exit 1, 빈 결과 시 exit 0
# ---------------------------------------------------------------------------

def test_main_exits_1_and_prints_violations_when_rows_returned(capsys):
    ch = FakeCH(rows=[("layer3_sum_mismatch", "raw=1 mart=2 view=2", 1)])
    rc = ri.main(["--date", "2026-07-14"], client=ch)
    assert rc == 1
    out = capsys.readouterr().out
    assert "[FAIL]" in out
    assert "layer3_sum_mismatch" in out
    assert "raw=1 mart=2 view=2" in out
    assert len(ch.queries) == 1


def test_main_exits_0_and_prints_pass_when_no_rows(capsys):
    ch = FakeCH(rows=[])
    rc = ri.main(["--date", "2026-07-14"], client=ch)
    assert rc == 0
    out = capsys.readouterr().out
    assert "ALL INVARIANTS PASS" in out
    assert "date=2026-07-14" in out
    assert "DBs=fact/gpu_data/mart" in out


def test_main_multiple_violation_rows_all_reported(capsys):
    ch = FakeCH(rows=[
        ("identified_name_leak", "identified rows with non-empty user_name: 1", 1),
        ("created_by_wrong", "table=mart_detail created_by=other", 5),
    ])
    rc = ri.main(["--date", "2026-07-14"], client=ch)
    assert rc == 1
    out = capsys.readouterr().out
    assert "2건" in out
    assert "identified_name_leak" in out
    assert "created_by_wrong" in out


def test_main_output_contains_no_user_id_pattern_on_violations(capsys):
    """§5.6 PII 미노출 회귀: identified_name_leak이 위반 행을 반환해도(픽스 후
    detail은 건수 집계 요약뿐) 최종 stdout에 user_id 원문 형태(예: user-1234)가
    전혀 없어야 한다. _print_violations()는 SQL이 만든 detail을 verbatim
    출력하므로, 이 테스트는 러너가 아니라 계약이 지켜지는지의 종단 확인이다."""
    ch = FakeCH(rows=[
        ("identified_name_leak", "identified rows with non-empty user_name: 3", 3),
        ("created_by_wrong", "table=mart_detail created_by=other", 5),
    ])
    rc = ri.main(["--date", "2026-07-14"], client=ch)
    assert rc == 1
    out = capsys.readouterr().out
    assert not re.search(r"user-\d+", out), "user_id 원문 패턴이 출력에 노출됨(§5.6 위반)"


def test_main_uses_db_env_overrides_in_query_and_message(monkeypatch, capsys):
    monkeypatch.setenv("CH_DB_FACT", "token_verify_fact")
    monkeypatch.setenv("CH_DB_DIM", "token_verify_dim")
    monkeypatch.setenv("CH_DB_MART", "token_verify_mart")
    ch = FakeCH(rows=[])
    rc = ri.main(["--date", "2026-07-14"], client=ch)
    assert rc == 0
    out = capsys.readouterr().out
    assert "DBs=token_verify_fact/token_verify_dim/token_verify_mart" in out
    sent_sql = ch.queries[0]
    assert "token_verify_fact." in sent_sql
    assert "token_verify_dim." in sent_sql
    assert "token_verify_mart." in sent_sql
    assert "{FACT}" not in sent_sql and "{DATE}" not in sent_sql


# ---------------------------------------------------------------------------
# --date 기본값 — 어제(KST), aware datetime
# ---------------------------------------------------------------------------

def test_default_date_kst_is_previous_day():
    now = datetime(2026, 7, 15, 3, 0, 0, tzinfo=ri.KST)
    assert ri.default_date_kst(now) == "2026-07-14"


def test_default_date_kst_requires_aware_now_and_converts_timezone():
    # UTC 자정 직후(=KST 오전 9시)라도 KST 기준 날짜로 변환 후 하루 전이어야 한다
    now_utc = datetime(2026, 7, 15, 0, 30, 0, tzinfo=timezone.utc)  # KST 09:30
    assert ri.default_date_kst(now_utc) == "2026-07-14"


def test_now_kst_returns_aware_datetime():
    now = ri.now_kst()
    assert now.tzinfo is not None
    assert now.utcoffset() == timedelta(hours=9)


def test_main_without_date_arg_uses_default_date_kst(capsys):
    fixed_now = datetime(2026, 7, 15, 3, 0, 0, tzinfo=ri.KST)
    ch = FakeCH(rows=[])
    rc = ri.main([], client=ch, now_fn=lambda: fixed_now)
    assert rc == 0
    out = capsys.readouterr().out
    assert "date=2026-07-14" in out
    assert "2026-07-14" in ch.queries[0]


# ---------------------------------------------------------------------------
# DB명 env 기본값 (mart/token-usage/app/ch.py와 동일 계약)
# ---------------------------------------------------------------------------

def test_db_name_defaults():
    dbs = ri.load_db_config()
    assert dbs == {"fact": "fact", "dim": "gpu_data", "mart": "mart"}


def test_db_name_env_override_via_subprocess():
    # DB_*_DEFAULT 상수 자체는 재평가되지 않지만 load_db_config()는 매 호출 시
    # os.environ을 다시 읽는다 — 자식 프로세스로 격리해 결정론적으로 검증한다
    # (tools/data-admin/tests/test_delete_data.py test_db_name_env_override_via_subprocess와 동일 관례).
    env = {"PATH": os.environ.get("PATH", ""),
           "CH_DB_FACT": "f2", "CH_DB_DIM": "d2", "CH_DB_MART": "m2"}
    result = subprocess.run(
        [sys.executable, "-c",
         "import run_invariants as ri; d = ri.load_db_config(); "
         "print(d['fact']); print(d['dim']); print(d['mart'])"],
        cwd=MODULE_ROOT, env=env, capture_output=True, text=True, check=True)
    assert result.stdout.strip().splitlines() == ["f2", "d2", "m2"]


# ---------------------------------------------------------------------------
# 인자 오류 — exit 2
# ---------------------------------------------------------------------------

def test_exit2_malformed_date():
    with pytest.raises(SystemExit) as e:
        ri.main(["--date", "2026/07/14"], client=FakeCH())
    assert e.value.code == 2


def test_help_exits_0():
    with pytest.raises(SystemExit) as e:
        ri.main(["--help"], client=FakeCH())
    assert e.value.code == 0
