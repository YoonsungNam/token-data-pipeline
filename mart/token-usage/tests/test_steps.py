"""Tests for app/steps.py — STEP 1/2 서버사이드 SQL 상수 + 실행 함수 (Plan 3 T3).

SQL은 E2E가 검증하므로 단위 테스트는 계약 형태(서버 바인딩·필수 마커·실행 시퀀스)만 고정한다.
FakeGate는 CHGate 인터페이스(exists/delete_day/insert_select/verify_count/query)를 흉내내며
호출 순서·인자를 기록한다 (T1 FakeCH와 별도 — steps.py는 CHGate 인스턴스 자체가 아니라
그 인터페이스만 요구하므로 여기서는 더 얇은 더블을 쓴다).
"""
import pytest

from app import steps

DATE = "2026-07-10"

# steps.py가 실제로 참조하는 테이블명 조각 → FakeGate 내부 짧은 키.
# 길이가 긴 키부터 매칭해야 "token_usage_1d"가 "view_token_usage_1d"의 부분 문자열이라
# 오매칭되는 것을 피할 수 있다.
_TABLE_KEYS = [
    ("view_token_usage_1d", "view_detail"),
    ("view_token_usage_service_1d", "view_service"),
    ("view_token_usage_org_1d", "view_org"),
    ("view_token_usage_model_1d", "view_model"),
    ("token_usage_1d", "detail"),
    ("agg_token_service_1d", "agg_service"),
    ("agg_token_org_1d", "agg_org"),
    ("agg_token_model_1d", "agg_model"),
]


def _short(table_or_sql: str) -> str:
    for key, short in sorted(_TABLE_KEYS, key=lambda kv: -len(kv[0])):
        if key in table_or_sql:
            return short
    raise AssertionError(f"unmapped table/sql in FakeGate: {table_or_sql[:80]!r}")


class FakeGate:
    """CHGate 인터페이스 더블 — exists/delete_day/insert_select/verify_count/query
    호출을 전부 기록한다.

    - order: exists() 호출 순서(테이블 처리 순서 — 짧은 키).
    - delete_preds: delete_day에 전달된 extra_pred 전부.
    - written: insert_select가 반환한 written_rows(짧은 키 기준).
    - exists()는 기본 True를 반환해 delete_day가 실제로 호출되도록 한다(그래야
      view의 extra_pred 계약을 의미 있게 검증할 수 있다 — exists=False면 delete
      자체가 스킵되어 delete_preds가 공허하게 통과해버린다).
    """

    def __init__(self, exists=True, verify_ok=True, verify_actual=None):
        self.order = []
        self.delete_preds = []
        self.written = {}
        self.query_calls = []
        self._exists = exists
        self._verify_ok = verify_ok
        self._verify_actual = verify_actual

    def exists(self, table_dist, date):
        short = _short(table_dist)
        self.order.append(short)
        return self._exists

    def delete_day(self, table_local, date, extra_pred=""):
        self.delete_preds.append(extra_pred)

    def insert_select(self, sql, params=None):
        short = _short(sql)
        n = self.written.get(short, 1)
        self.written[short] = n
        return n

    def verify_count(self, table_dist, date, expected):
        actual = expected if self._verify_actual is None else self._verify_actual
        return self._verify_ok, actual

    def query(self, sql, params=None):
        self.query_calls.append((sql, params))
        return []


@pytest.fixture
def fake_gate():
    return FakeGate()


@pytest.fixture
def fake_gate_failing():
    return FakeGate(verify_ok=False, verify_actual=0)


def test_sql_constants_bind_date_not_fstring():
    # 날짜는 서버사이드 바인딩만 — SQL 인젝션·타입 사고 방지 (§7.1)
    for sql in [steps.SQL_DETAIL, steps.SQL_AGG_SERVICE, steps.SQL_AGG_ORG,
                steps.SQL_AGG_MODEL, *steps.SQL_VIEWS.values()]:
        assert "{d:Date}" in sql
        assert "%(" not in sql                       # 파이썬 포맷 흔적 금지


def test_sql_detail_contract_markers():
    s = steps.SQL_DETAIL
    assert "GLOBAL LEFT JOIN" in s                   # §4.0 분산 조인 표준
    assert "'token-pipeline'" in s                   # created_by 명시 삽입
    assert "['unknown']" in s                        # §6.1 미매핑 규칙
    assert "argMax" in s                             # date 기준 유효 dim 행


def test_sql_agg_service_null_semantics():
    s = steps.SQL_AGG_SERVICE
    assert s.count("has_summary") >= 2               # 부재→NULL 분기 (v1.9)
    assert "is_derived" in s


def test_sql_inserts_have_explicit_column_lists():
    # 위치 기반 INSERT 금지 — 모든 INSERT가 대상 컬럼을 명시 (DDL 컬럼 추가 시 조용한 어긋남 방지)
    for sql in [steps.SQL_DETAIL, steps.SQL_AGG_SERVICE, steps.SQL_AGG_ORG,
                steps.SQL_AGG_MODEL, *steps.SQL_VIEWS.values()]:
        assert "INSERT INTO" in sql
        # "INSERT INTO <table>\n    (col, col, ...)" 형태 — SELECT 앞에 여는 괄호가 있어야 한다
        insert_idx = sql.index("INSERT INTO")
        select_idx = sql.index("SELECT")
        assert "(" in sql[insert_idx:select_idx]


def test_sql_views_no_select_star():
    for sql in steps.SQL_VIEWS.values():
        assert "SELECT *" not in sql                 # 컬럼 드리프트 조기 검출


def test_run_step1_sequence(fake_gate):
    # FakeGate가 호출 순서를 기록: exists→(delete)→insert_select→verify_count ×4테이블
    r = steps.run_step1(fake_gate, DATE)
    assert fake_gate.order == ["detail", "agg_service", "agg_org", "agg_model"]
    assert r["rows_detail"] == fake_gate.written["detail"]
    assert set(r.keys()) == {"rows_detail", "rows_svc", "rows_org", "rows_model", "warns"}
    assert r["warns"] == []


def test_run_step1_deletes_without_created_by_pred(fake_gate):
    # STEP 1(mart 전용 테이블)은 view와 달리 created_by 술어가 붙지 않는다
    steps.run_step1(fake_gate, DATE)
    assert fake_gate.delete_preds == ["", "", "", ""]


def test_run_step2_deletes_view_with_created_by_pred(fake_gate):
    steps.run_step2(fake_gate, DATE)
    assert all("created_by" in p for p in fake_gate.delete_preds)   # view만의 술어 (§7.1)
    assert fake_gate.delete_preds                                  # 공허 통과 방지 — 실제 호출됨


def test_run_step2_returns_rows_view_detail_and_total(fake_gate):
    r = steps.run_step2(fake_gate, DATE)
    assert set(r.keys()) == {"rows_view_detail", "rows_view_total", "warns"}
    assert r["rows_view_detail"] == fake_gate.written["view_detail"]
    assert r["rows_view_total"] == sum(fake_gate.written.values())
    assert r["warns"] == []


def test_verify_count_failure_raises_step_error(fake_gate_failing):
    with pytest.raises(steps.StepError):
        steps.run_step1(fake_gate_failing, DATE)


def test_verify_count_overshoot_warns_dup_suspect():
    gate = FakeGate(verify_ok=True, verify_actual=999)   # actual > written → dup_suspect
    r = steps.run_step1(gate, DATE)
    assert any(w.startswith("dup_suspect:") for w in r["warns"])
    assert len(r["warns"]) == 4                          # 4테이블 전부 초과 판정
