"""Tests for app/batch.py — 읽기 계약 프리플라이트 → 변이 예산 프리체크 → M0/M0b →
RUNNERS(M1→M3) 오케스트레이션 → BATCH_RESULT 마커 · SIGTERM (Plan 6c T5, fix round 1).

FakeGate는 mart/token-usage/tests/test_batch.py의 더블(SQL 부분문자열 라우팅)을 클론하되
실행 표면(insert_select/verify_count)은 호출 자체를 금지한다 — M1/M3 러너는
`batch.RUNNERS`를 monkeypatch한 스텁으로 대체한다(steps.py 계약은 test_steps.py가 고정).

컨트롤러 결정 D1(service_not_in_usage_registry): batch는 서비스별 CHECK WARN을 직접 찍지
않는다 — M3(steps.run_m3)의 집계 1줄(`CHECK WARN service_not_in_usage_registry severity=<S>
count=<n>`)이 유일한 출력 소스다. SQL_M0_REG_NOT_IN_USAGE(steps.SUB_REG/SUB_USAGE_SVC로
조립)는 조용히 실행되어 이름만 마커 missing_services에 병합된다.
컨트롤러 결정 D2: 이 쿼리는 날짜 무관이라 `{d:Date}` 바인딩 계약(date-bound 집합)에서 제외한다.

fix round 1(리뷰 6항목) 반영:
1. RUNNERS 루프 — 러너가 이미 `CHECK WARN `/`CHECK INFO ` 접두로 반환한 줄(생산자가 이미
   찍은 M3 요약 줄)은 batch가 재출력하지 않는다(집계만).
2. `_status` 캐시 — `run_batch`의 try 진입 직후 FAILURE/coverage=0/0/reason=sigterm으로
   리셋해, 이전 날짜의 SUCCESS 캐시가 다음 날짜 M0 진행 중까지 남지 않게 한다.
3. `main` — `--from`이 `--to`보다 늦어 `target_dates`가 `([], True)`를 반환하면(오늘은 `None`만
   처리) stderr 1줄 + exit 2, `CHGate`는 생성되지 않는다.
4. `_merge_not_in_usage` — T2 `compute_coverage()`가 반환한 `Coverage`를 변경하지 않고
   `dataclasses.replace`로 새 객체를 만든다(합집합 ≠ 원본 치환).
5. `_check_m0`/`_merge_not_in_usage` 분리 — 레지스트리 조회 실패 시에도 이미 계산된 M0
   수치(metrics_coverage=<present>/<enabled>)를 마커에 보존한다.
6. 리뷰에서 생존한 뮤테이션(worst=max, RUNNERS 루프 내 상태 갱신, SIGTERM 핸들러 등록) 각각에
   대한 표적 테스트 추가.
"""
import re
import signal

import pytest

from app import batch, steps
from app.ch import DB_DIM, DB_FACT, DB_MART, DB_TOKEN_DIM, DB_TOKEN_MART
from app.config import Config
from app.mart import Coverage, batch_line
from app.preflight import READ_CONTRACT
from app.steps import MART_TABLES, StepError, run_m1, run_m3, run_m4

DATE = "2026-09-03"
DATE2 = "2026-09-04"

# 설계 §6.1 마커 형식 그대로 — 필드 순서·따옴표·elapsed 소수 1자리·선택 reason 접미
MARKER_RE = re.compile(
    r'^BATCH_RESULT status=(?P<status>SUCCESS|FAILURE) module=mart-metrics '
    r'metrics_coverage=(?P<present>\d+)/(?P<enabled>\d+) missing_services="(?P<missing>[^"]*)" '
    r'rows_mart=(?P<rows_mart>\d+) rows_check=(?P<rows_check>\d+) rows_share=(?P<rows_share>\d+) '
    r'warn=(?P<warn>\d+) elapsed=\d+\.\d(?: reason=(?P<reason>[A-Za-z0-9_]+))?$')


class FakeGate:
    """CHGate 더블 — describe/exists/delete_day/query만 응답. insert_select·verify_count는
    RUNNERS 스텁이 대체하므로 호출되면 AssertionError(오케스트레이션이 steps를 우회해
    직접 쓰지 않는다는 계약)."""

    def __init__(self, expected=None, anchors=None, not_in_usage=None, token_mart_rows=1,
                 describe_missing=None, exists_always=False):
        self.expected = list(expected or [])            # SQL_M0_EXPECTED_SERVICES 응답
        self.anchors = list(anchors or [])              # SQL_M0_ANCHOR_SERVICES 응답
        self.not_in_usage = list(not_in_usage or [])    # SQL_M0_REG_NOT_IN_USAGE 응답
        self.token_mart_rows = token_mart_rows          # SQL_M0B_TOKEN_MART_ROWS 응답
        self.describe_missing = describe_missing or {}  # {"<db.table>_dist": {"col", ...}}
        self.exists_always = exists_always
        self.describe_calls = []
        self.exists_calls = []
        self.delete_calls = []
        self.query_calls = []

    def describe(self, table_dist):
        self.describe_calls.append(table_dist)
        base = table_dist[:-len("_dist")]
        drop = self.describe_missing.get(table_dist, set())
        return [c for c in READ_CONTRACT[base] if c not in drop]

    def exists(self, table_dist, date):
        self.exists_calls.append((table_dist, date))
        return self.exists_always

    def delete_day(self, table_local, date, extra_pred=""):
        self.delete_calls.append((table_local, date))

    def wait_for_mutations(self, table_local):
        return None

    def insert_select(self, sql, params=None):
        raise AssertionError("insert_select must go through steps.run_m* (RUNNERS are stubbed)")

    def verify_count(self, table_dist, date, expected):
        raise AssertionError("verify_count must go through steps.run_m* (RUNNERS are stubbed)")

    def query(self, sql, params=None):
        self.query_calls.append((sql, params))
        if "GLOBAL NOT IN" in sql:
            return [(s,) for s in self.not_in_usage]
        if "dim_token_metrics_service_dist" in sql and "coverage_since" in sql:
            return [(s,) for s in self.expected]
        if "raw_token_metrics_summary_1d_dist" in sql:
            return [(s,) for s in self.anchors]
        if "agg_token_service_1d_dist" in sql and "count()" in sql:
            return [(self.token_mart_rows,)]
        raise AssertionError(f"unmapped query in FakeGate: {sql[:80]!r}")


def stub_runners(monkeypatch, rows_mart=3, rows_check=5, warns_m1=None, warns_m3=None,
                 m1_raises=None, fail_dates=()):
    """batch.RUNNERS를 [("rows_mart", m1), ("rows_check", m3)] 스텁으로 교체. 반환 = 호출 기록.
    m1_raises가 주어지면 fail_dates(비어 있으면 모든 날짜)에서 M1이 그 예외를 던진다."""
    calls = []

    def run_m1(gate, date):
        calls.append(("rows_mart", date))
        if m1_raises is not None and (not fail_dates or date in fail_dates):
            raise m1_raises
        return {"rows_mart": rows_mart, "warns": list(warns_m1 or [])}

    def run_m3(gate, date):
        calls.append(("rows_check", date))
        return {"rows_check": rows_check, "warns": list(warns_m3 or [])}

    monkeypatch.setattr(batch, "RUNNERS", [("rows_mart", run_m1), ("rows_check", run_m3)])
    return calls


def full_gate(**kw):
    """기대 2 서비스 = 앵커 2 서비스 (coverage 2/2, 경고 없음)."""
    return FakeGate(expected=["Mock Service A", "Mock Service B"],
                    anchors=["Mock Service A", "Mock Service B"], **kw)


def wire_main(monkeypatch, gate, **cfg_overrides):
    """main()이 실제 CH에 붙지 않도록 CHGate/load_config를 치환."""
    monkeypatch.setattr(batch, "CHGate", lambda cfg: gate)
    monkeypatch.setattr(batch, "load_config", lambda: Config(**cfg_overrides))


def marker_lines(out: str) -> list[str]:
    return [line for line in out.splitlines() if line.startswith("BATCH_RESULT")]


# ============================================================================
# SQL 상수 계약 — DB 상수 5종만·{d:Date} 바인딩·읽기 계약 테이블 (§6.1, §7.1)
# ============================================================================

def test_sql_constants_bind_date_and_use_db_constants():
    date_bound = [batch.SQL_M0_EXPECTED_SERVICES, batch.SQL_M0_ANCHOR_SERVICES,
                  batch.SQL_M0B_TOKEN_MART_ROWS]
    for sql in date_bound:
        assert "{d:Date}" in sql
    for sql in [batch.SQL_M0_REG_NOT_IN_USAGE, *date_bound]:
        assert "%(" not in sql and "coalesce(" not in sql.lower()
    assert f"{DB_DIM}.dim_token_metrics_service_dist" in batch.SQL_M0_EXPECTED_SERVICES
    assert "coverage_since <= {d:Date}" in batch.SQL_M0_EXPECTED_SERVICES
    assert "isNull(until) OR {d:Date} <= until" in batch.SQL_M0_EXPECTED_SERVICES
    assert f"{DB_FACT}.raw_token_metrics_summary_1d_dist" in batch.SQL_M0_ANCHOR_SERVICES
    assert f"{DB_TOKEN_DIM}.dim_token_service_dist" in batch.SQL_M0_REG_NOT_IN_USAGE
    assert "GLOBAL NOT IN" in batch.SQL_M0_REG_NOT_IN_USAGE
    # M0b = 읽기 계약 agg_token_service_1d(date, service)만 — 설계 §6.1 "agg_token_service_1d에 D 행 없음"
    assert f"{DB_TOKEN_MART}.agg_token_service_1d_dist" in batch.SQL_M0B_TOKEN_MART_ROWS
    assert "count()" in batch.SQL_M0B_TOKEN_MART_ROWS
    assert "token_usage_1d" not in batch.SQL_M0B_TOKEN_MART_ROWS


# ============================================================================
# SQL_M0_REG_NOT_IN_USAGE — 컨트롤러 결정 D1(조용함)·D2(날짜 무관) — steps.py 조각 재사용
# ============================================================================

def test_sql_m0_reg_not_in_usage_composed_from_steps_fragments_and_date_free():
    """컨트롤러 결정 D1/D2: steps.SUB_REG/SUB_USAGE_SVC(§4.3 조인 키 전제와 동일 정본)로 조립하고,
    레지스트리끼리의 정합성 검사라 {d:Date} 규칙(app.steps SQL 대상)에서 제외된다."""
    assert steps.SUB_REG in batch.SQL_M0_REG_NOT_IN_USAGE
    assert steps.SUB_USAGE_SVC in batch.SQL_M0_REG_NOT_IN_USAGE
    assert "{d:Date}" not in batch.SQL_M0_REG_NOT_IN_USAGE


def test_reg_not_in_usage_feeds_missing_services_without_printing(monkeypatch, capsys):
    """컨트롤러 결정 D1 — batch는 이 검사에 대해 어떤 줄도 찍지 않는다(M3의 집계 1줄이 유일
    소스, 여기선 RUNNERS가 스텁이라 아예 나타나지 않는다). 이름은 missing_services에만 실린다."""
    stub_runners(monkeypatch)
    gate = full_gate(not_in_usage=["Mock Service C", "Mock Service D"])
    out = batch.run_batch(Config(), DATE, gate=gate)
    printed = capsys.readouterr().out
    assert "service_not_in_usage_registry" not in printed
    assert "Mock Service C" not in printed and "Mock Service D" not in printed
    m = MARKER_RE.match(out.line)
    assert m.group("status") == "SUCCESS"
    assert m.group("missing") == "Mock Service C,Mock Service D"
    assert m.group("warn") == "0"        # 이 검사 자체의 CHECK WARN은 M3(스텁이라 없음)에서만 남


def test_reg_not_in_usage_merges_with_m0_missing_sorted_dedup(monkeypatch, capsys):
    """fix1-4: M0 커버리지 결손(present<enabled)과 레지스트리 결손을 합칠 때 정렬·중복
    제거하되(합집합 ≠ 치환 — M0 missing=["b"], not-in-usage={"c","a"} → missing_services=a,b,c),
    metrics_coverage=N/M과 CHECK WARN metrics_coverage의 missing=<n> 카운트는 병합 전
    M0-only 값(=1)을 유지한다."""
    stub_runners(monkeypatch)
    gate = FakeGate(expected=["a", "b"], anchors=["a"], not_in_usage=["c", "a"])
    out = batch.run_batch(Config(), DATE, gate=gate)
    printed = capsys.readouterr().out
    m = MARKER_RE.match(out.line)
    assert m.group("missing") == "a,b,c"                        # dedup + 정렬(합집합)
    assert (m.group("present"), m.group("enabled")) == ("1", "2")  # M0-only, 병합 영향 없음
    assert "CHECK WARN metrics_coverage missing=1" in printed    # 레지스트리 이름 제외한 카운트


def test_compute_coverage_return_value_not_mutated_by_merge(monkeypatch):
    """fix1-4: T2 compute_coverage()가 반환한 Coverage 객체는 병합 후에도 원본 그대로다
    (dataclasses.replace로 새 객체를 만들 뿐, in-place 변경 금지)."""
    captured = {}
    real_compute_coverage = batch.compute_coverage

    def spy(*a, **kw):
        cov = real_compute_coverage(*a, **kw)
        captured["cov"] = cov
        return cov

    monkeypatch.setattr(batch, "compute_coverage", spy)
    gate = FakeGate(expected=["a", "b"], anchors=["a"], not_in_usage=["c", "a"])
    warns: list = []
    coverage = batch._check_m0(gate, DATE, warns)
    merged = batch._merge_not_in_usage(gate, coverage)
    assert captured["cov"] is coverage
    assert coverage.missing == ["b"]             # 원본 불변
    assert merged.missing == ["a", "b", "c"]     # 새 객체(합집합)
    assert merged is not coverage


def test_reg_not_in_usage_query_has_no_date_param(monkeypatch):
    stub_runners(monkeypatch)
    gate = full_gate()
    batch.run_batch(Config(), DATE, gate=gate)
    calls = [c for c in gate.query_calls if "GLOBAL NOT IN" in c[0]]
    assert len(calls) == 1 and calls[0][1] is None


def test_reg_query_failure_after_good_m0_preserves_coverage_numbers(monkeypatch):
    """fix1-5: 레지스트리 조회(SQL_M0_REG_NOT_IN_USAGE) 실패는 exception 경로로 가지만, 이미
    계산된 M0 수치(metrics_coverage=<present>/<enabled>)를 마커에 그대로 보존한다."""
    stub_runners(monkeypatch)

    class RegFailsGate(FakeGate):
        def query(self, sql, params=None):
            if "GLOBAL NOT IN" in sql:
                raise RuntimeError("registry lookup failed")
            return super().query(sql, params)

    gate = RegFailsGate(expected=["Mock Service A", "Mock Service B"],
                        anchors=["Mock Service A", "Mock Service B"])
    out = batch.run_batch(Config(), DATE, gate=gate)
    m = MARKER_RE.match(out.line)
    assert out.exit_code == 1 and m.group("reason") == "exception"
    assert (m.group("present"), m.group("enabled")) == ("2", "2")


# ============================================================================
# run_batch — 마커·M0·M0b·러너 경고 집계 (마커 출력은 main의 몫)
# ============================================================================

def test_marker_success_full_coverage(monkeypatch, capsys):
    stub_runners(monkeypatch, rows_mart=3, rows_check=5)
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    m = MARKER_RE.match(out.line)
    assert m, out.line
    assert out.exit_code == 0 and out.skip_share is False
    assert m.group("status") == "SUCCESS"
    assert (m.group("present"), m.group("enabled"), m.group("missing")) == ("2", "2", "-")
    assert (m.group("rows_mart"), m.group("rows_check"), m.group("rows_share"),
            m.group("warn")) == ("3", "5", "0", "0")
    assert m.group("reason") is None
    assert out.rows == {"rows_mart": 3, "rows_check": 5, "rows_share": 0}
    assert "BATCH_RESULT" not in capsys.readouterr().out   # 날짜당 정확히 1줄 — 출력은 main()


def test_no_metrics_day_is_success_with_warn(monkeypatch, capsys):
    stub_runners(monkeypatch, rows_mart=2, rows_check=1)
    gate = FakeGate(expected=["Mock Service A", "Mock Service B"], anchors=[])
    out = batch.run_batch(Config(), DATE, gate=gate)
    m = MARKER_RE.match(out.line)
    assert m.group("status") == "SUCCESS" and out.exit_code == 0     # 절대 FAILURE 아님 (§6.1)
    assert (m.group("present"), m.group("enabled")) == ("0", "2")
    assert m.group("missing") == "Mock Service A,Mock Service B"
    assert m.group("warn") == "1"
    printed = capsys.readouterr().out
    assert "CHECK WARN metrics_coverage missing=2" in printed
    assert "Mock Service" not in printed          # 서비스명은 마커 missing_services에만


def test_token_mart_absent_warn_and_flag(monkeypatch, capsys):
    stub_runners(monkeypatch)
    out = batch.run_batch(Config(), DATE, gate=full_gate(), token_mart_present=False)
    assert out.skip_share is True and out.exit_code == 0
    assert f"CHECK WARN token_mart_absent date={DATE}" in capsys.readouterr().out
    m = MARKER_RE.match(out.line)
    assert m.group("status") == "SUCCESS" and m.group("warn") == "1" and m.group("rows_share") == "0"


def test_token_mart_presence_queried_when_not_given(monkeypatch):
    stub_runners(monkeypatch)
    assert batch.run_batch(Config(), DATE, gate=full_gate(token_mart_rows=0)).skip_share is True
    assert batch.run_batch(Config(), DATE, gate=full_gate(token_mart_rows=7)).skip_share is False
    given = full_gate(token_mart_rows=0)
    assert batch.run_batch(Config(), DATE, gate=given, token_mart_present=True).skip_share is False
    assert not any("agg_token_service_1d_dist" in sql for sql, _ in given.query_calls)


def test_m3_check_lines_not_reprinted_by_batch(monkeypatch, capsys):
    """fix1-1: 러너가 이미 `CHECK WARN `/`CHECK INFO ` 접두로 반환한 줄(생산자가 이미 찍었다는
    계약)은 batch가 재출력하지 않는다 — 접두 없는 코드(dup_suspect:<dist>)만 batch가 1회 출력."""
    stub_runners(monkeypatch, warns_m3=[
        "dup_suspect:mart.x_dist",
        "CHECK WARN foo severity=WARN count=1",
        "CHECK INFO bar severity=INFO count=2",
    ])
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    printed = capsys.readouterr().out
    assert printed.count("CHECK WARN dup_suspect:mart.x_dist") == 1
    assert "CHECK WARN foo severity=WARN count=1" not in printed
    assert "CHECK INFO bar severity=INFO count=2" not in printed
    assert MARKER_RE.match(out.line).group("warn") == "2"     # dup_suspect + foo(INFO bar는 제외)


def test_step_warns_are_normalized_and_counted(monkeypatch, capsys):
    stub_runners(monkeypatch,
                 warns_m1=[f"dup_suspect:{DB_MART}.agg_token_model_cost_1d_dist"],
                 warns_m3=["CHECK WARN rows_rejected severity=WARN count=2",
                           "CHECK INFO manual_source severity=INFO count=1"])
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    printed = capsys.readouterr().out
    # fix1-1: CHECK WARN|INFO 접두 줄은 생산자(steps.run_m3)가 이미 찍는다는 계약이므로 batch는
    # 재출력하지 않는다(스텁이라 여기선 아무도 찍지 않음 — printed에 나타나지 않아야 한다).
    assert f"CHECK WARN dup_suspect:{DB_MART}.agg_token_model_cost_1d_dist" in printed
    assert "CHECK WARN rows_rejected severity=WARN count=2" not in printed
    assert "CHECK INFO manual_source severity=INFO count=1" not in printed
    assert MARKER_RE.match(out.line).group("warn") == "2"      # INFO는 제외, 재출력 여부와 무관


def test_step_error_marks_failure_with_reason(monkeypatch):
    calls = stub_runners(monkeypatch, m1_raises=StepError("verify_count"))
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    m = MARKER_RE.match(out.line)
    assert out.exit_code == 1
    assert m.group("status") == "FAILURE" and m.group("reason") == "verify_count"
    assert calls == [("rows_mart", DATE)]          # M1 실패 → M3 미실행


def test_step_error_reason_is_first_token_of_message(monkeypatch, capsys):
    msg = (f"verify_count failed: {DB_MART}.agg_token_model_cost_1d_dist date={DATE} "
           f"written_rows=0 expected=3 actual=0")
    stub_runners(monkeypatch, m1_raises=StepError(msg))
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    assert MARKER_RE.match(out.line).group("reason") == "verify_count"
    assert "verify_count failed" in capsys.readouterr().err     # 상세는 stderr(마커 오염 금지)


def test_generic_exception_marks_failure_reason_exception(monkeypatch, capsys):
    stub_runners(monkeypatch, m1_raises=RuntimeError("connection reset"))
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    m = MARKER_RE.match(out.line)
    assert out.exit_code == 1 and m.group("reason") == "exception"
    assert "RuntimeError" in capsys.readouterr().err


def test_m0_query_failure_is_failure_with_zero_coverage(monkeypatch):
    stub_runners(monkeypatch)

    class BrokenGate(FakeGate):
        def query(self, sql, params=None):
            raise TimeoutError("read timeout")

    out = batch.run_batch(Config(), DATE, gate=BrokenGate())
    m = MARKER_RE.match(out.line)
    assert m.group("status") == "FAILURE" and m.group("reason") == "exception"
    assert (m.group("present"), m.group("enabled"), m.group("missing")) == ("0", "0", "-")


def test_marker_never_contains_user_id_or_payload(monkeypatch):
    stub_runners(monkeypatch, warns_m1=["user_id=abc"])
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    assert "user_id" not in out.line and "abc" not in out.line    # warns는 카운트만 마커에 반영
    assert MARKER_RE.match(out.line).group("warn") == "1"


def test_status_cache_is_failure_sigterm_while_in_progress(monkeypatch):
    seen = {}

    def run_m1(gate, date):
        seen["line"] = batch._status["line"]
        return {"rows_mart": 1, "warns": []}

    monkeypatch.setattr(batch, "RUNNERS", [("rows_mart", run_m1)])
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    m = MARKER_RE.match(seen["line"])
    assert m.group("status") == "FAILURE" and m.group("reason") == "sigterm"
    assert m.group("present") == "2"                # M0 결과가 이미 반영된 캐시
    assert batch._status["line"] == out.line        # 완료 후 캐시 = 최종 줄


def test_status_cache_reset_before_next_dates_m0(monkeypatch):
    """fix1-2: 이전 날짜의 SUCCESS 캐시가 다음 날짜 M0 진행 중까지 남아있으면 SIGTERM이 실행되지
    않은 날짜의 SUCCESS 마커를 잘못 재출력한다 — try 진입 직후 FAILURE reason=sigterm으로
    캐시를 리셋해야 한다."""
    stub_runners(monkeypatch, rows_mart=3, rows_check=5)
    batch.run_batch(Config(), DATE, gate=full_gate())          # 날짜 A → SUCCESS(rows_mart=3) 캐시
    assert "status=SUCCESS" in batch._status["line"]

    snapshot = []

    class SnapshotGate(FakeGate):
        def query(self, sql, params=None):
            if "dim_token_metrics_service_dist" in sql and "coverage_since" in sql:
                snapshot.append(batch._status["line"])
            return super().query(sql, params)

    batch.run_batch(Config(), DATE2, gate=SnapshotGate(
        expected=["Mock Service A", "Mock Service B"],
        anchors=["Mock Service A", "Mock Service B"]))
    assert snapshot, "SQL_M0_EXPECTED_SERVICES query was not observed"
    m = MARKER_RE.match(snapshot[0])
    assert m.group("status") == "FAILURE" and m.group("reason") == "sigterm"
    assert m.group("rows_mart") == "0"
    assert "SUCCESS" not in snapshot[0]


def test_status_updated_inside_runners_loop_between_m1_and_m3(monkeypatch):
    """fix1-6(a): RUNNERS 루프 내부의 `_status` 갱신이 살아있는지 — M1 완료 직후(M3 실행 전)
    캐시에 rows_mart가 이미 반영돼 있어야 한다."""
    seen = {}

    def run_m1(gate, date):
        return {"rows_mart": 9, "warns": []}

    def run_m3(gate, date):
        seen["line"] = batch._status["line"]
        return {"rows_check": 5, "warns": []}

    monkeypatch.setattr(batch, "RUNNERS", [("rows_mart", run_m1), ("rows_check", run_m3)])
    batch.run_batch(Config(), DATE, gate=full_gate())
    m = MARKER_RE.match(seen["line"])
    assert m.group("status") == "FAILURE"
    assert m.group("rows_mart") == "9"


# ============================================================================
# 프리플라이트(읽기 계약 13컬럼) · 변이 예산 프리체크 — 첫 _run_table 전, 변이 0
# ============================================================================

def test_preflight_or_fail_reports_missing_and_describes_three_tables(capsys):
    gate = full_gate(describe_missing={f"{DB_TOKEN_MART}.agg_token_service_1d_dist": {"service"}})
    missing = batch.preflight_or_fail(gate)
    assert missing == [f"{DB_TOKEN_MART}.agg_token_service_1d.service"]
    assert sorted(gate.describe_calls) == sorted([
        f"{DB_TOKEN_MART}.token_usage_1d_dist",
        f"{DB_TOKEN_MART}.agg_token_service_1d_dist",
        f"{DB_TOKEN_DIM}.dim_token_service_dist",
    ])
    printed = capsys.readouterr().out
    assert f"PREFLIGHT FAIL read_contract missing={DB_TOKEN_MART}.agg_token_service_1d.service" in printed
    assert batch.preflight_or_fail(full_gate()) == []


def test_preflight_describe_exception_counts_as_missing():
    class NoTableGate(FakeGate):
        def describe(self, table_dist):
            if table_dist.endswith("dim_token_service_dist"):
                raise RuntimeError("Table does not exist")
            return super().describe(table_dist)

    missing = batch.preflight_or_fail(NoTableGate())
    assert missing
    assert all(m.startswith(f"{DB_TOKEN_DIM}.dim_token_service.") for m in missing)


def test_read_contract_missing_fails_all_dates_without_mutation(monkeypatch, capsys):
    calls = stub_runners(monkeypatch)
    gate = full_gate(exists_always=True,
                     describe_missing={f"{DB_TOKEN_MART}.agg_token_service_1d_dist": {"service"}})
    wire_main(monkeypatch, gate)
    code = batch.main(["--from", "2026-09-01", "--to", "2026-09-03"])
    lines = marker_lines(capsys.readouterr().out)
    assert code == 1 and len(lines) == 3
    for line in lines:
        m = MARKER_RE.match(line)
        assert m.group("status") == "FAILURE" and m.group("reason") == "read_contract"
        assert (m.group("present"), m.group("enabled")) == ("0", "0")
    assert gate.delete_calls == [] and gate.exists_calls == [] and calls == []


def test_exists_exception_before_date_loop_fails_all_dates_reason_exception(monkeypatch, capsys):
    """B4(M-3) — plan_mutations의 exists()가 raise하면(마트 테이블 부재·서버 unreachable) date
    루프 진입 전이라 예전에는 트레이스백만 남고 BATCH_RESULT가 한 줄도 안 나갔다(마커 기반 알림
    침묵). 이제 date 루프 밖 예외를 잡아 날짜당 정확히 1줄 FAILURE reason=exception 마커로
    바꾼다 — INSERT/DELETE는 issue되지 않는다(변이 0)."""
    calls = stub_runners(monkeypatch)

    class RaisingExistsGate(FakeGate):
        def exists(self, table_dist, date):
            raise RuntimeError("connection refused")

    gate = RaisingExistsGate()
    wire_main(monkeypatch, gate)
    code = batch.main(["--from", "2026-09-01", "--to", "2026-09-03"])
    lines = marker_lines(capsys.readouterr().out)
    assert code == 1 and len(lines) == 3
    for line in lines:
        m = MARKER_RE.match(line)
        assert m.group("status") == "FAILURE" and m.group("reason") == "exception"
        assert (m.group("present"), m.group("enabled")) == ("0", "0")
    assert gate.delete_calls == [] and calls == []


def test_plan_mutations_counts_existing_date_table_pairs():
    gate = full_gate(exists_always=True)
    assert batch.plan_mutations(gate, ["2026-09-01", "2026-09-02"]) == 8
    assert {t for t, _ in gate.exists_calls} == {f"{DB_MART}.{t}_dist" for t in MART_TABLES}
    assert batch.plan_mutations(full_gate(exists_always=False), ["2026-09-01"]) == 0


def test_mutation_budget_precheck_fails_before_any_delete(monkeypatch, capsys):
    calls = stub_runners(monkeypatch)
    gate = full_gate(exists_always=True)
    wire_main(monkeypatch, gate)
    code = batch.main(["--from", "2026-08-01", "--to", "2026-08-17"])     # 17일 × 4 = 68 > 64
    lines = marker_lines(capsys.readouterr().out)
    assert code == 1 and len(lines) == 17
    assert all(MARKER_RE.match(line).group("reason") == "mutation_budget" for line in lines)
    assert gate.delete_calls == [] and calls == []

    gate2 = full_gate(exists_always=True)
    wire_main(monkeypatch, gate2)
    code = batch.main(["--from", "2026-08-01", "--to", "2026-08-16"])     # 16일 × 4 = 64 = 예산
    lines = marker_lines(capsys.readouterr().out)
    assert code == 0 and len(lines) == 16
    assert all(MARKER_RE.match(line).group("status") == "SUCCESS" for line in lines)
    assert len(calls) == 32                                                # 16일 × (M1 + M3)


def test_mutation_budget_env_override(monkeypatch, capsys):
    stub_runners(monkeypatch)
    wire_main(monkeypatch, full_gate(exists_always=True), max_mutations_per_run=4)
    code = batch.main(["--from", "2026-09-01", "--to", "2026-09-02"])     # 2일 × 4 = 8 > 4
    assert code == 1
    assert capsys.readouterr().out.count("reason=mutation_budget") == 2


# ============================================================================
# main / CLI — 날짜당 마커 1줄, worst exit, --date 별칭, 인자 오류 2
# ============================================================================

def test_main_prints_one_marker_per_date_and_worst_exit(monkeypatch, capsys):
    """fix1-6(c): FAILURE 날짜를 먼저, SUCCESS 날짜를 나중에 두어 `worst = max(worst, …)`가
    아니라 `worst = outcome.exit_code`(마지막 값으로 덮어쓰기)로 퇴화하면 실패하도록 만든다."""
    stub_runners(monkeypatch, m1_raises=StepError("verify_count"), fail_dates=(DATE,))
    wire_main(monkeypatch, full_gate())
    code = batch.main(["--from", DATE, "--to", DATE2])
    out = capsys.readouterr().out
    lines = marker_lines(out)
    assert code == 1 and len(lines) == 2
    assert MARKER_RE.match(lines[0]).group("status") == "FAILURE"
    assert MARKER_RE.match(lines[1]).group("status") == "SUCCESS"
    assert "user_id" not in out


def test_main_date_alias_single_day(monkeypatch, capsys):
    stub_runners(monkeypatch)
    wire_main(monkeypatch, full_gate())
    code = batch.main(["--date", DATE])
    lines = marker_lines(capsys.readouterr().out)
    assert code == 0 and len(lines) == 1


def test_main_from_without_to_exits_2(monkeypatch):
    wire_main(monkeypatch, full_gate())
    assert batch.main(["--from", DATE]) == 2


def test_main_inverted_from_to_exits_2_without_marker_or_chgate(monkeypatch, capsys):
    """fix1-3: --from이 --to보다 늦으면 target_dates가 ([], True)를 반환한다(오늘은 None만
    처리) — stderr 1줄 + exit 2, 마커 0줄, CHGate는 생성되지 않는다."""
    def boom(cfg):
        raise AssertionError("CHGate must not be constructed for an inverted date range")

    monkeypatch.setattr(batch, "CHGate", boom)
    monkeypatch.setattr(batch, "load_config", lambda: Config())
    code = batch.main(["--from", "2026-09-05", "--to", "2026-09-01"])
    out = capsys.readouterr()
    assert code == 2
    assert marker_lines(out.out) == []
    assert "대상 날짜 없음" in out.err


def test_main_registers_sigterm_handler(monkeypatch):
    """fix1-6(d): main()이 SIGTERM 핸들러 등록을 빠뜨리면 실패한다."""
    calls = []
    monkeypatch.setattr(batch.signal, "signal", lambda *a: calls.append(a))
    stub_runners(monkeypatch)
    wire_main(monkeypatch, full_gate())
    batch.main(["--date", DATE])
    assert calls == [(signal.SIGTERM, batch._sigterm_handler)]


def test_main_help_exits_zero():
    with pytest.raises(SystemExit) as exc:
        batch.main(["--help"])
    assert exc.value.code == 0


# ============================================================================
# SIGTERM — 캐시 줄 재출력 + note=sigterm, exit 143
# ============================================================================

def test_sigterm_handler_prints_cached_line_and_exits_143(capsys):
    cached = batch_line("FAILURE", Coverage(2, 1, ["Mock Service B"], ["Mock Service B"]),
                        3, 0, 0, 1, 4.2, reason="sigterm")
    batch._status["line"] = cached
    with pytest.raises(SystemExit) as exc:
        batch._sigterm_handler(15, None)
    assert exc.value.code == 143
    out = capsys.readouterr().out.strip()
    assert out == cached + " note=sigterm"
    assert "reason=sigterm note=sigterm" in out


# ============================================================================
# T6 — M4 러너 연결: RUNNERS 3항·M0b token_mart_absent 시 M4 스킵(rows_share=0)·마커 rows_share
# ============================================================================
# D6/D7: Config·run_m4는 top-level import를 재사용(중복 import 없음) — 아래는 T6 전용 나머지.
T6_DATE = "2026-09-03"


class _T6Gate:
    """M0/M0b 조회만 응답하는 최소 게이트(러너는 monkeypatch 스텁이라 테이블 접근 없음)."""

    def __init__(self, expected=("Mock Service A", "Mock Service B"), anchors=None, token_rows=1):
        self.expected = list(expected)
        self.anchors = self.expected if anchors is None else list(anchors)
        self.token_rows = token_rows
        self.queries = []

    def query(self, sql, params=None):
        self.queries.append(sql)
        if "GLOBAL NOT IN" in sql:
            return []
        if "raw_token_metrics_summary_1d_dist" in sql:
            return [(s,) for s in self.anchors]
        if "dim_token_metrics_service_dist" in sql:
            return [(s,) for s in self.expected]
        if "agg_token_service_1d_dist" in sql:
            return [(self.token_rows,)]
        raise AssertionError(f"unexpected query: {sql[:80]!r}")

    def describe(self, table):
        raise AssertionError("describe must not be called from run_batch")

    def exists(self, table_dist, date):
        raise AssertionError("exists must not be called (runners are stubbed)")

    def delete_day(self, table_local, date, extra_pred=""):
        raise AssertionError("delete_day must not be called (runners are stubbed)")


def _stub_runners(monkeypatch, m4_rows=7):
    calls = {"m1": [], "m3": [], "m4": []}

    def m1(gate, date):
        calls["m1"].append(date)
        return {"rows_mart": 3, "warns": []}

    def m3(gate, date):
        calls["m3"].append(date)
        return {"rows_check": 5, "warns": []}

    def m4(gate, date):
        calls["m4"].append(date)
        return {"rows_share": m4_rows, "warns": []}

    monkeypatch.setattr(batch, "RUNNERS", [("rows_mart", m1), ("rows_check", m3), ("rows_share", m4)])
    return calls


def test_token_mart_absent_skips_m4_rows_share_zero(monkeypatch):
    calls = _stub_runners(monkeypatch, m4_rows=7)
    out = batch.run_batch(Config(), T6_DATE, gate=_T6Gate(), token_mart_present=False)
    assert calls["m1"] == [T6_DATE] and calls["m3"] == [T6_DATE]
    assert calls["m4"] == []                       # M4 러너 호출 0회
    assert out.skip_share is True
    assert out.rows["rows_share"] == 0
    assert "status=SUCCESS" in out.line
    assert "rows_mart=3 rows_check=5 rows_share=0 warn=1" in out.line
    assert out.exit_code == 0


def test_marker_rows_share_filled(monkeypatch):
    calls = _stub_runners(monkeypatch, m4_rows=7)
    out = batch.run_batch(Config(), T6_DATE, gate=_T6Gate(), token_mart_present=True)
    assert calls["m4"] == [T6_DATE]
    assert out.skip_share is False
    assert out.rows["rows_share"] == 7
    assert "rows_mart=3 rows_check=5 rows_share=7 warn=0" in out.line
    assert "status=SUCCESS" in out.line


def test_m0b_query_decides_skip_share_when_flag_not_given(monkeypatch):
    calls = _stub_runners(monkeypatch, m4_rows=2)
    gate = _T6Gate(token_rows=0)
    out = batch.run_batch(Config(), T6_DATE, gate=gate)
    assert any("agg_token_service_1d_dist" in q for q in gate.queries)
    assert out.skip_share is True and calls["m4"] == [] and "rows_share=0 warn=1" in out.line
    gate2 = _T6Gate(token_rows=12)
    out2 = batch.run_batch(Config(), T6_DATE, gate=gate2)
    assert out2.skip_share is False and calls["m4"] == [T6_DATE] and "rows_share=2 warn=0" in out2.line


# ============================================================================
# T7 — RUNNERS 4개 완성(M1→M3→M4→M2) · rows_group 마커 미포함(로그만) · M2 실패 = FAILURE
# ============================================================================
from app.steps import run_m2  # noqa: E402

T7_DATE = "2026-09-03"


def _stub_four_runners(monkeypatch, rows_group=4, m2_raises=None):
    """RUNNERS를 4개 스텁으로 교체(M1 3행·M3 5행·M4 7행·M2 rows_group행). 반환 = 호출 순서 기록."""
    calls = []

    def m1(gate, date):
        calls.append("rows_mart")
        return {"rows_mart": 3, "warns": []}

    def m3(gate, date):
        calls.append("rows_check")
        return {"rows_check": 5, "warns": []}

    def m4(gate, date):
        calls.append("rows_share")
        return {"rows_share": 7, "warns": []}

    def m2(gate, date):
        calls.append("rows_group")
        if m2_raises is not None:
            raise m2_raises
        return {"rows_group": rows_group, "warns": []}

    monkeypatch.setattr(batch, "RUNNERS", [("rows_mart", m1), ("rows_check", m3),
                                           ("rows_share", m4), ("rows_group", m2)])
    return calls


def test_runners_final_order_four():
    assert [k for k, _ in batch.RUNNERS] == ["rows_mart", "rows_check", "rows_share", "rows_group"]
    assert [fn.__name__ for _, fn in batch.RUNNERS] == ["run_m1", "run_m3", "run_m4", "run_m2"]
    # fix1 SHOULD-2 — 함수 동일성까지 고정(이름만으로는 재바인딩된 동명 함수를 놓칠 수 있다)
    assert batch.RUNNERS[0][1] is run_m1
    assert batch.RUNNERS[1][1] is run_m3
    assert batch.RUNNERS[2][1] is run_m4
    assert batch.RUNNERS[3][1] is run_m2 and batch.RUNNERS[3][1] is steps.run_m2
    assert batch._MARKER_ROW_KEYS == ("rows_mart", "rows_check", "rows_share")   # Plan 6a H 고정
    assert "rows_group" not in batch._MARKER_ROW_KEYS


def test_marker_has_no_rows_group_field(monkeypatch, caplog):
    calls = _stub_four_runners(monkeypatch, rows_group=4)
    with caplog.at_level("INFO", logger="app.batch"):
        out = batch.run_batch(Config(), T7_DATE, gate=full_gate(), token_mart_present=True)
    assert calls == ["rows_mart", "rows_check", "rows_share", "rows_group"]
    m = MARKER_RE.match(out.line)
    assert m, out.line
    assert m.group("status") == "SUCCESS" and out.exit_code == 0
    assert (m.group("rows_mart"), m.group("rows_check"), m.group("rows_share")) == ("3", "5", "7")
    assert "rows_group" not in out.line                       # 마커 필드 고정(Plan 6a H)
    assert out.rows == {"rows_mart": 3, "rows_check": 5, "rows_share": 7, "rows_group": 4}
    assert "M2 rows_group=4" in caplog.text                    # 로그로만 노출


def test_m2_runs_even_when_token_mart_absent(monkeypatch):
    # M0b token_mart_absent는 M4만 스킵 — M2는 GPU-only라 실행된다(설계 §6.1 M0b)
    calls = _stub_four_runners(monkeypatch, rows_group=2)
    out = batch.run_batch(Config(), T7_DATE, gate=full_gate(), token_mart_present=False)
    assert calls == ["rows_mart", "rows_check", "rows_group"]
    assert out.skip_share is True and out.rows["rows_share"] == 0 and out.rows["rows_group"] == 2
    assert "rows_mart=3 rows_check=5 rows_share=0 warn=1" in out.line and "status=SUCCESS" in out.line


def test_m2_failure_marks_batch_failure(monkeypatch, capsys):
    msg = (f"verify_count failed: {DB_MART}.agg_token_gpu_group_1d_dist date={T7_DATE} "
           "written_rows=0 expected=3 actual=1")
    calls = _stub_four_runners(monkeypatch, m2_raises=StepError(msg))
    out = batch.run_batch(Config(), T7_DATE, gate=full_gate(), token_mart_present=True)
    assert calls == ["rows_mart", "rows_check", "rows_share", "rows_group"]   # M2가 마지막 러너
    m = MARKER_RE.match(out.line)
    assert m, out.line
    assert out.exit_code == 1 and m.group("status") == "FAILURE" and m.group("reason") == "verify_count"
    assert (m.group("rows_mart"), m.group("rows_check"), m.group("rows_share")) == ("3", "5", "7")   # 선행 3개 반영
    assert out.rows["rows_group"] == 0
    assert "verify_count failed" in capsys.readouterr().err   # 상세는 stderr(마커 오염 금지)
