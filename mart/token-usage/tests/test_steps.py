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
    - verify_calls: verify_count에 실제로 전달된 (table_dist, expected) 전부 —
      expected가 written_rows가 아니라 query() 소스 카운트에서 왔는지 단정용.
    - exists()는 기본 True를 반환해 delete_day가 실제로 호출되도록 한다(그래야
      view의 extra_pred 계약을 의미 있게 검증할 수 있다 — exists=False면 delete
      자체가 스킵되어 delete_preds가 공허하게 통과해버린다).

    query()는 EXPECTED_SQL의 실제 SQL 문자열을 파싱하지 않는다(agg_service의
    expected_sql처럼 대상 테이블이 아니라 소스 테이블명을 참조하는 SQL은
    `_short(sql)`로 오매칭되기 때문 — 예: EXPECTED_SQL_AGG_SERVICE는
    "agg_token_service_1d"가 아니라 "token_usage_1d"/summary 테이블을 참조).
    대신 exists()가 마지막으로 기록한 짧은 키(_current_short)를 "현재 처리 중인
    테이블"로 추적해 그 테이블의 expected를 반환한다 — 운영 코드의 실제
    EXPECTED_SQL 정확성은 clickhouse-format 파싱 검증(E2E 범위)이 담당한다.
    """

    def __init__(self, exists=True, verify_ok=True, verify_actual=None, expected_overrides=None):
        self.order = []
        self.delete_preds = []
        self.written = {}
        self.query_calls = []
        self.verify_calls = []
        self._exists = exists
        self._verify_ok = verify_ok
        self._verify_actual = verify_actual
        self._current_short = None
        self._expected_overrides = expected_overrides or {}

    def exists(self, table_dist, date):
        short = _short(table_dist)
        self.order.append(short)
        self._current_short = short
        return self._exists

    def delete_day(self, table_local, date, extra_pred=""):
        self.delete_preds.append(extra_pred)

    def insert_select(self, sql, params=None):
        short = _short(sql)
        n = self.written.get(short, 1)
        self.written[short] = n
        return n

    def verify_count(self, table_dist, date, expected):
        self.verify_calls.append((table_dist, expected))
        actual = expected if self._verify_actual is None else self._verify_actual
        return self._verify_ok, actual

    def query(self, sql, params=None):
        self.query_calls.append((sql, params))
        short = self._current_short
        expected = self._expected_overrides.get(short, self.written.get(short, 1))
        return [(expected,)]


@pytest.fixture
def fake_gate():
    return FakeGate()


@pytest.fixture
def fake_gate_failing():
    return FakeGate(verify_ok=False, verify_actual=0)


_ALL_EXPECTED_SQL = [
    steps.EXPECTED_SQL_DETAIL, steps.EXPECTED_SQL_AGG_SERVICE, steps.EXPECTED_SQL_AGG_ORG,
    steps.EXPECTED_SQL_AGG_MODEL, steps.EXPECTED_SQL_VIEW_DETAIL, steps.EXPECTED_SQL_VIEW_SERVICE,
    steps.EXPECTED_SQL_VIEW_ORG, steps.EXPECTED_SQL_VIEW_MODEL,
]


def test_sql_constants_bind_date_not_fstring():
    # 날짜는 서버사이드 바인딩만 — SQL 인젝션·타입 사고 방지 (§7.1)
    for sql in [steps.SQL_DETAIL, steps.SQL_AGG_SERVICE, steps.SQL_AGG_ORG,
                steps.SQL_AGG_MODEL, *steps.SQL_VIEWS.values()]:
        assert "{d:Date}" in sql
        assert "%(" not in sql                       # 파이썬 포맷 흔적 금지


def test_expected_sql_bind_date_not_fstring():
    # written_rows 대신 쓰는 소스 카운트 쿼리 8종도 동일 바인딩 규칙을 지킨다.
    assert len(_ALL_EXPECTED_SQL) == 8
    for sql in _ALL_EXPECTED_SQL:
        assert "{d:Date}" in sql
        assert "%(" not in sql


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
    # FakeGate가 호출 순서를 기록: exists→(delete)→insert_select→query(expected)→
    # verify_count ×4테이블
    r = steps.run_step1(fake_gate, DATE)
    assert fake_gate.order == ["detail", "agg_service", "agg_org", "agg_model"]
    assert r["rows_detail"] == fake_gate.written["detail"]
    assert set(r.keys()) == {"rows_detail", "rows_svc", "rows_org", "rows_model", "warns"}
    assert r["warns"] == []


def test_run_step1_queries_expected_per_table_after_insert(fake_gate):
    # insert→expected query 순서: 4테이블 전부 query()가 정확히 1회씩(총 4회) 호출되고
    # 매번 서버 바인딩 날짜 파라미터로 호출된다.
    steps.run_step1(fake_gate, DATE)
    assert len(fake_gate.query_calls) == 4
    assert all(params == {"d": DATE} for _, params in fake_gate.query_calls)


def test_run_step2_queries_expected_per_table(fake_gate):
    steps.run_step2(fake_gate, DATE)
    assert len(fake_gate.query_calls) == 4
    assert all(params == {"d": DATE} for _, params in fake_gate.query_calls)


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


def test_verify_uses_source_count_not_written_rows():
    # 회귀 계약: written_rows가 실제 적재량의 2배(Distributed insert_distributed_sync=1
    # 이중 계상 시나리오)여도 verify_count는 항상 gate.query() 소스 카운트(expected)를
    # 쓴다 — written_rows는 텔레메트리로 강등되었다(app/steps.py EXPECTED_SQL docstring).
    shorts = ("detail", "agg_service", "agg_org", "agg_model")
    gate = FakeGate(expected_overrides={s: 3 for s in shorts})
    for s in shorts:
        gate.written[s] = 6   # insert_select가 이중 계상된 written_rows=6(진짜는 3)을 반환

    r = steps.run_step1(gate, DATE)

    # verify_count에 실제로 전달된 expected는 written_rows(6)가 아니라 소스 카운트(3).
    assert gate.verify_calls
    assert all(expected == 3 for _, expected in gate.verify_calls)
    # actual(=expected=3, FakeGate 기본) == expected(3) → 초과 아님, dup_suspect 없음.
    assert r["warns"] == []
    # written_rows 자체는 여전히 반환값(텔레메트리)으로 보존된다.
    assert r["rows_detail"] == 6
