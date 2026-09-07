"""Tests for app/batch.py — 읽기 계약 프리플라이트 → 변이 예산 프리체크 → M0/M0b →
RUNNERS(M1→M3) 오케스트레이션 → BATCH_RESULT 마커 · SIGTERM (Plan 6c T5).

FakeGate는 mart/token-usage/tests/test_batch.py의 더블(SQL 부분문자열 라우팅)을 클론하되
실행 표면(insert_select/verify_count)은 호출 자체를 금지한다 — M1/M3 러너는
`batch.RUNNERS`를 monkeypatch한 스텁으로 대체한다(steps.py 계약은 test_steps.py가 고정).

컨트롤러 결정 D1(service_not_in_usage_registry): batch는 서비스별 CHECK WARN을 직접 찍지
않는다 — M3(steps.run_m3)의 집계 1줄(`CHECK WARN service_not_in_usage_registry severity=<S>
count=<n>`)이 유일한 출력 소스다. SQL_M0_REG_NOT_IN_USAGE(steps.SUB_REG/SUB_USAGE_SVC로
조립)는 조용히 실행되어 이름만 마커 missing_services에 병합된다. 따라서 원 아웃라인의
"서비스별 CHECK WARN 줄" 기대 테스트는 이 파일에서 제거·재작성됐다(아래 D1 관련 절 참고).
컨트롤러 결정 D2: 이 쿼리는 날짜 무관이라 `{d:Date}` 바인딩 계약(date-bound 집합)에서 제외한다.
"""
import re

import pytest

from app import batch, steps
from app.ch import DB_DIM, DB_FACT, DB_MART, DB_TOKEN_DIM, DB_TOKEN_MART
from app.config import Config
from app.mart import Coverage, batch_line
from app.preflight import READ_CONTRACT
from app.steps import MART_TABLES, StepError

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


def test_runners_order_m1_then_m3():
    assert [k for k, _ in batch.RUNNERS] == ["rows_mart", "rows_check"]
    assert batch.RUNNERS[0][1] is steps.run_m1 and batch.RUNNERS[1][1] is steps.run_m3


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


def test_reg_not_in_usage_merges_with_m0_missing_sorted_dedup():
    """M0 커버리지 결손(present<enabled)과 레지스트리 결손을 합칠 때 정렬·중복 제거하되,
    metrics_coverage=N/M은 병합 전 M0-only 값을 유지한다(원인이 다른 두 카운트를 섞지 않음)."""
    gate = FakeGate(expected=["Mock Service A", "Mock Service B"], anchors=["Mock Service A"],
                    not_in_usage=["Mock Service B", "Mock Service C"])
    warns: list = []
    coverage = batch._check_m0_coverage(gate, DATE, warns)
    assert coverage.missing == ["Mock Service B", "Mock Service C"]     # dedup + 정렬
    assert (coverage.present, coverage.enabled) == (1, 2)               # M0-only, 병합 영향 없음


def test_reg_not_in_usage_query_has_no_date_param(monkeypatch):
    stub_runners(monkeypatch)
    gate = full_gate()
    batch.run_batch(Config(), DATE, gate=gate)
    calls = [c for c in gate.query_calls if "GLOBAL NOT IN" in c[0]]
    assert len(calls) == 1 and calls[0][1] is None


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


def test_step_warns_are_normalized_and_counted(monkeypatch, capsys):
    stub_runners(monkeypatch,
                 warns_m1=[f"dup_suspect:{DB_MART}.agg_token_model_cost_1d_dist"],
                 warns_m3=["CHECK WARN rows_rejected severity=WARN count=2",
                           "CHECK INFO manual_source severity=INFO count=1"])
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    printed = capsys.readouterr().out
    assert f"CHECK WARN dup_suspect:{DB_MART}.agg_token_model_cost_1d_dist" in printed
    assert "CHECK WARN rows_rejected severity=WARN count=2" in printed
    assert "CHECK INFO manual_source severity=INFO count=1" in printed
    assert MARKER_RE.match(out.line).group("warn") == "2"      # INFO는 warn 카운트 제외


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
    stub_runners(monkeypatch, m1_raises=StepError("verify_count"), fail_dates=(DATE2,))
    wire_main(monkeypatch, full_gate())
    code = batch.main(["--from", DATE, "--to", DATE2])
    out = capsys.readouterr().out
    lines = marker_lines(out)
    assert code == 1 and len(lines) == 2
    assert MARKER_RE.match(lines[0]).group("status") == "SUCCESS"
    assert MARKER_RE.match(lines[1]).group("status") == "FAILURE"
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
