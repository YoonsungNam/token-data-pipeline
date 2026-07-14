"""Tests for app/batch.py — STEP 0 게이트 + run_step1/run_step2 오케스트레이션 +
인라인 검증 4종 + 마커/CLI (Plan 3 T4).

FakeGate는 T3(test_steps.py)의 더블과 같은 패턴(짧은 키 매핑)을 STEP1/2 실행 표면에
재사용하고, STEP 0·인라인 검증 4종이 쓰는 gate.query()에는 SQL 텍스트의 고유
부분문자열로 분기하는 매핑을 추가한다 (T1/T3 report의 "파일별 자체 FakeGate" 관례).
"""
import pytest

from app import batch, steps
from app.config import Config


DATE = "2026-07-10"
DATE2 = "2026-07-11"

# steps.py가 참조하는 테이블/SQL 조각 → FakeGate 내부 짧은 키 (test_steps.py와 동일 관례).
# 긴 키부터 매칭 — "token_usage_1d"가 "view_token_usage_1d"의 부분문자열이라 오매칭되는 것을 피한다.
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


# steps.py의 EXPECTED_SQL_* 상수(소스 카운트 쿼리, written_rows 이중 계상 회귀 수정) →
# FakeGate 짧은 키. 부분문자열 매칭(_short)은 여기서 쓸 수 없다 — 예를 들어
# EXPECTED_SQL_AGG_SERVICE는 대상 테이블(agg_token_service_1d)이 아니라 소스 테이블
# (token_usage_1d/raw_token_usage_summary_1d_dist)을 참조하므로 아래 STEP 0/인라인
# 검증용 substring 라우팅과 충돌한다. 상수 객체 자체로 정확히 식별한다.
_EXPECTED_SQL_SHORT = {
    steps.EXPECTED_SQL_DETAIL: "detail",
    steps.EXPECTED_SQL_AGG_SERVICE: "agg_service",
    steps.EXPECTED_SQL_AGG_ORG: "agg_org",
    steps.EXPECTED_SQL_AGG_MODEL: "agg_model",
    steps.EXPECTED_SQL_VIEW_DETAIL: "view_detail",
    steps.EXPECTED_SQL_VIEW_SERVICE: "view_service",
    steps.EXPECTED_SQL_VIEW_ORG: "view_org",
    steps.EXPECTED_SQL_VIEW_MODEL: "view_model",
}


class FakeGate:
    """CHGate 인터페이스 전체 더블 — STEP1/2 실행 표면(exists/delete_day/insert_select/
    verify_count)은 test_steps.py 패턴을 재사용하고, STEP 0·인라인 검증이 쓰는
    query()는 SQL 텍스트의 고유 부분문자열로 응답을 분기한다.

    기본값은 전부 "경고 없음" 상태(raw=mart=view 합계 동일, diff 불일치 없음, org 매핑
    실패율 0%, 미등록 모델 없음) — 개별 테스트가 필요한 파라미터만 오버라이드한다.
    """

    def __init__(self, enabled=None, summary=None, step1_fails=False, step2_fails=False,
                 totals=None, diff_mismatch_services=None, org_counts=None,
                 unregistered_models=None, query_raises=None):
        self.enabled = enabled if enabled is not None else []
        self.summary = summary if summary is not None else set()
        self.step1_fails = step1_fails
        self.step2_fails = step2_fails
        self.totals = totals or {"raw": 100, "mart": 100, "view": 100}
        self.diff_mismatch_services = diff_mismatch_services or []
        self.org_counts = org_counts or (0, 0)   # (unknown_cnt, identified_cnt)
        self.unregistered_models = unregistered_models or []
        self.written = {}
        self.query_calls = []
        self.query_raises = query_raises  # (exc_class, msg) tuple or None

    # ---- STEP1/2 실행 표면 (steps.py가 호출) ----
    def exists(self, table_dist, date):
        return True

    def delete_day(self, table_local, date, extra_pred=""):
        pass

    def insert_select(self, sql, params=None):
        key = _short(sql)
        n = self.written.get(key, 1)
        self.written[key] = n
        return n

    def verify_count(self, table_dist, date, expected):
        key = _short(table_dist)
        if self.step1_fails and key == "detail":
            return False, 0
        if self.step2_fails and key == "view_detail":
            return False, 0
        return True, expected

    # ---- STEP 0 + 인라인 검증 4종 + STEP1/2 expected 소스 카운트가 쓰는 query() ----
    def query(self, sql, params=None):
        self.query_calls.append((sql, params))
        if self.query_raises is not None:
            exc_class, msg = self.query_raises
            raise exc_class(msg)
        # steps.py의 EXPECTED_SQL_* — 이중 계상 회귀 수정: expected는 written_rows와
        # 동일 기본값을 돌려줘 이 파일의 기존 verify_count 계약(= expected 그대로
        # 통과/step1_fails·step2_fails만 실패)을 보존한다.
        if sql in _EXPECTED_SQL_SHORT:
            short = _EXPECTED_SQL_SHORT[sql]
            return [(self.written.get(short, 1),)]
        if "dim_token_service_dist" in sql:
            return [(s,) for s in self.enabled]
        if "raw_token_usage_summary_1d_dist" in sql:
            return [(s,) for s in self.summary]
        if "view_token_usage_1d_dist" in sql:
            return [(self.totals["view"],)]
        if "raw_token_usage_1d_dist" in sql:
            return [(self.totals["raw"],)]
        if "cost IS NULL" in sql:
            return [(m,) for m in self.unregistered_models]
        if "org_path = ['unknown']" in sql:
            return [self.org_counts]
        if "agg_token_service_1d_dist" in sql:
            return [(s,) for s in self.diff_mismatch_services]
        if "token_usage_1d_dist" in sql:
            return [(self.totals["mart"],)]
        raise AssertionError(f"unmapped query in FakeGate: {sql[:80]!r}")


def cfg(**overrides):
    return Config(**overrides)


# ============================================================================
# SQL 상수 계약 — 서버 바인딩만 사용 (§7.1)
# ============================================================================

def test_sql_constants_bind_date_not_fstring():
    date_bound = [
        batch.SQL_SUMMARY_SERVICES,
        batch.SQL_VALIDATE_RAW_TOTAL,
        batch.SQL_VALIDATE_MART_TOTAL,
        batch.SQL_VALIDATE_VIEW_TOTAL,
        batch.SQL_VALIDATE_DIFF_MISMATCH,
        batch.SQL_VALIDATE_ORG_MAP_FAIL,
        batch.SQL_VALIDATE_UNREGISTERED_MODELS,
    ]
    for sql in date_bound:
        assert "{d:Date}" in sql
    for sql in [batch.SQL_ENABLED_SERVICES, *date_bound]:
        assert "%(" not in sql


# ============================================================================
# 브리프 Step 1 테스트
# ============================================================================

def test_step0_coverage_in_marker_and_proceeds(capsys):
    # enabled 3, summary 1 → 적재는 진행 + coverage=1/3 + missing 노출 (§7.1 "조용함 금지")
    gate = FakeGate(enabled=["S1", "S2", "S3"], summary={"S1"})
    code = batch.run_batch(cfg(), DATE, gate=gate)
    out = capsys.readouterr().out
    assert code == 0
    assert "coverage=1/3" in out and 'missing_services="S2,S3"' in out


def test_step_error_marks_failure_exit_1(capsys):
    gate = FakeGate(enabled=["S1"], summary={"S1"}, step1_fails=True)
    code = batch.run_batch(cfg(), DATE, gate=gate)
    out = capsys.readouterr().out
    assert code == 1
    assert "BATCH_RESULT status=FAILURE" in out


def test_range_rerun_repeats_full_pipeline_per_date(capsys, monkeypatch):
    # --from/--to 2일 → BATCH_RESULT 2줄, 날짜별 STEP 0→2 독립 (§7.1 — collectors와 다른 계약)
    gate = FakeGate(enabled=["S1"], summary={"S1"})
    monkeypatch.setattr(batch, "CHGate", lambda cfg: gate)
    code = batch.main(["--from", DATE, "--to", DATE2])
    out = capsys.readouterr().out
    assert out.count("BATCH_RESULT") == 2
    assert code == 0


def test_marker_single_line_per_date_and_no_user_ids(capsys):
    gate = FakeGate(enabled=["S1"], summary={"S1"})
    code = batch.run_batch(cfg(), DATE, gate=gate)
    out = capsys.readouterr().out
    assert code == 0
    assert out.count("BATCH_RESULT") == 1
    assert "user_id" not in out


# ============================================================================
# 인라인 검증 4종 — 각각 독립적으로 WARN 발화 (배치는 SUCCESS 유지)
# ============================================================================

def test_inline_validation_totals_mismatch_warns(capsys):
    gate = FakeGate(enabled=["S1"], summary={"S1"},
                    totals={"raw": 100, "mart": 100, "view": 99})
    code = batch.run_batch(cfg(), DATE, gate=gate)
    out = capsys.readouterr().out
    assert code == 0
    assert "CHECK WARN" in out and "totals_mismatch" in out
    assert "warn=1" in out


def test_inline_validation_diff_mismatch_warns(capsys):
    gate = FakeGate(enabled=["S1"], summary={"S1"}, diff_mismatch_services=["S2"])
    code = batch.run_batch(cfg(), DATE, gate=gate)
    out = capsys.readouterr().out
    assert code == 0
    assert "CHECK WARN" in out and "diff_mismatch" in out and "S2" in out


def test_inline_validation_org_map_fail_rate_warns(capsys):
    # unknown 30 / identified 100 = 30% > 기본 임계 20%
    gate = FakeGate(enabled=["S1"], summary={"S1"}, org_counts=(30, 100))
    code = batch.run_batch(cfg(org_map_warn_threshold=0.2), DATE, gate=gate)
    out = capsys.readouterr().out
    assert code == 0
    assert "CHECK WARN" in out and "org_map_fail_rate" in out


def test_inline_validation_org_map_fail_rate_below_threshold_no_warn(capsys):
    # unknown 10 / identified 100 = 10% < 임계 20% → WARN 없음
    gate = FakeGate(enabled=["S1"], summary={"S1"}, org_counts=(10, 100))
    code = batch.run_batch(cfg(org_map_warn_threshold=0.2), DATE, gate=gate)
    out = capsys.readouterr().out
    assert code == 0
    assert "org_map_fail_rate" not in out
    assert "warn=0" in out


def test_inline_validation_unregistered_models_warns(capsys):
    gate = FakeGate(enabled=["S1"], summary={"S1"}, unregistered_models=["gpt-mystery"])
    code = batch.run_batch(cfg(), DATE, gate=gate)
    out = capsys.readouterr().out
    assert code == 0
    assert "CHECK WARN" in out and "unregistered_models" in out and "gpt-mystery" in out


def test_inline_validations_all_clean_warn_zero(capsys):
    gate = FakeGate(enabled=["S1"], summary={"S1"})
    code = batch.run_batch(cfg(), DATE, gate=gate)
    out = capsys.readouterr().out
    assert code == 0
    assert "CHECK WARN" not in out
    assert "warn=0" in out


# ============================================================================
# 광역 가드 예외 처리 — T4 리뷰 Fix 1 (Medium)
# ============================================================================

def test_unexpected_exception_still_emits_failure_marker(capsys):
    """TimeoutError 등 비-StepError 예외도 BATCH_RESULT status=FAILURE 마커 + return 1."""
    gate = FakeGate(enabled=["S1"], summary={"S1"}, query_raises=(TimeoutError, "connection timeout"))
    code = batch.run_batch(cfg(), DATE, gate=gate)
    captured = capsys.readouterr()
    assert code == 1
    assert "BATCH_RESULT status=FAILURE" in captured.out
    assert "ERROR in run_batch" in captured.err


def test_multi_date_continues_after_exception(capsys, monkeypatch):
    """1일차 예외 → 2일차 정상 실행, BATCH_RESULT 2줄 (FAILURE + SUCCESS), exit 1."""
    date1_gate = FakeGate(enabled=["S1"], summary={"S1"}, query_raises=(RuntimeError, "db error"))
    date2_gate = FakeGate(enabled=["S1"], summary={"S1"})
    gates_by_date = {DATE: date1_gate, DATE2: date2_gate}

    original_run_batch = batch.run_batch

    def run_batch_with_date_gate(cfg, date, gate=None):
        """Override run_batch to use date-specific gate."""
        specific_gate = gates_by_date.get(date)
        if specific_gate is not None:
            gate = specific_gate
        return original_run_batch(cfg, date, gate=gate)

    monkeypatch.setattr(batch, "run_batch", run_batch_with_date_gate)
    # Mock CHGate to avoid real connection
    monkeypatch.setattr(batch, "CHGate", lambda cfg: None)
    code = batch.main(["--from", DATE, "--to", DATE2])
    out = capsys.readouterr().out
    assert out.count("BATCH_RESULT") == 2
    assert out.count("status=FAILURE") == 1
    assert out.count("status=SUCCESS") == 1
    assert code == 1  # 첫 날짜 실패로 worst=1


def test_step2_step_error_marks_failure(capsys):
    """FakeGate.step2_fails 경로 커버 (리뷰 지적 — 정의만 되고 미사용)."""
    gate = FakeGate(enabled=["S1"], summary={"S1"}, step2_fails=True)
    code = batch.run_batch(cfg(), DATE, gate=gate)
    out = capsys.readouterr().out
    assert code == 1
    assert "BATCH_RESULT status=FAILURE" in out


# ============================================================================
# CLI
# ============================================================================

def test_main_help_exits_zero():
    with pytest.raises(SystemExit) as exc:
        batch.main(["--help"])
    assert exc.value.code == 0


def test_main_from_without_to_exits_2(capsys):
    code = batch.main(["--from", DATE])
    assert code == 2
