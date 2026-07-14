"""배치 오케스트레이터 — STEP 0 커버리지 게이트 → STEP 1(run_step1) → STEP 2(run_step2)
→ 인라인 검증 4종 → 마커 (Plan 3 T4).

CLI/SIGTERM/aware-KST 처리는 collectors/token-usage/app/main.py의 확정 패턴을 이식한다.
로깅 계약(§5.6): 어떤 로그에도 user_id 원문을 남기지 않는다(서비스명·모델명·집계값만).

인라인 검증 4종의 SQL은 이 모듈이 정의한다(T3의 steps.py는 STEP1/2 SQL만 소유) —
전부 gate.query()로 실행하고 날짜는 서버 바인딩(`{d:Date}`)만 사용한다(§7.1).
"""
import argparse
import signal
import sys
import time

from app.ch import CHGate, DB_DIM, DB_FACT, DB_MART
from app.config import Config, load_config
from app.mart import Coverage, Warn, batch_line, compute_coverage, target_dates
from app.steps import StepError, run_step1, run_step2

# =============================================================================
# STEP 0 — 커버리지 SQL (날짜 무관한 enabled 목록 + 날짜별 summary 서비스 목록)
# =============================================================================

SQL_ENABLED_SERVICES = f"SELECT service FROM {DB_DIM}.dim_token_service_dist WHERE enabled = 1"

SQL_SUMMARY_SERVICES = f"""
SELECT DISTINCT service FROM {DB_FACT}.raw_token_usage_summary_1d_dist
WHERE date = {{d:Date}}
"""

# =============================================================================
# 인라인 검증 4종 — 컨트롤러 사인오프(브리프 미정의분) SQL 정본
# =============================================================================

# (a) 3계층 합계 비교 — raw에는 total_input_tokens 컬럼이 없어 3필드 합으로 재구성
SQL_VALIDATE_RAW_TOTAL = f"""
SELECT sum(input_tokens + cache_read_tokens + cache_creation_tokens)
FROM {DB_FACT}.raw_token_usage_1d_dist
WHERE date = {{d:Date}}
"""

SQL_VALIDATE_MART_TOTAL = f"""
SELECT sum(total_input_tokens)
FROM {DB_MART}.token_usage_1d_dist
WHERE date = {{d:Date}}
"""

SQL_VALIDATE_VIEW_TOTAL = f"""
SELECT sum(total_input_tokens)
FROM {DB_DIM}.view_token_usage_1d_dist
WHERE date = {{d:Date}}
"""

# (b) detail-vs-summary 대사 불일치 서비스 목록 — is_derived=0 한정, diff NULL(파생/미보고)은 제외
SQL_VALIDATE_DIFF_MISMATCH = f"""
SELECT DISTINCT service
FROM {DB_MART}.agg_token_service_1d_dist
WHERE date = {{d:Date}}
  AND is_derived = 0
  AND (
    (diff_input_tokens IS NOT NULL AND diff_input_tokens != 0)
    OR (diff_cache_read_tokens IS NOT NULL AND diff_cache_read_tokens != 0)
    OR (diff_cache_creation_tokens IS NOT NULL AND diff_cache_creation_tokens != 0)
    OR (diff_output_tokens IS NOT NULL AND diff_output_tokens != 0)
    OR (diff_requests IS NOT NULL AND diff_requests != 0)
  )
"""

# (c) org 매핑 실패율 — user_type='identified' 한정, org_path=['unknown'] 비율
SQL_VALIDATE_ORG_MAP_FAIL = f"""
SELECT
    countIf(user_type = 'identified' AND org_path = ['unknown']) AS unknown_cnt,
    countIf(user_type = 'identified') AS identified_cnt
FROM {DB_MART}.token_usage_1d_dist
WHERE date = {{d:Date}}
"""

# (d) dim_model 미등록 모델 집합 — cost IS NULL, 'unknown' 자체는 제외(전 단가 NULL 시드 정상)
SQL_VALIDATE_UNREGISTERED_MODELS = f"""
SELECT DISTINCT model
FROM {DB_MART}.token_usage_1d_dist
WHERE date = {{d:Date}}
  AND cost IS NULL
  AND model != 'unknown'
"""


_status = {"line": batch_line("FAILURE", Coverage(0, 0, [], []), 0, 0, 0, 0.0)}


def _sigterm_handler(signum, frame):
    print(_status["line"] + " note=sigterm", flush=True)   # 진행 중 날짜의 마커 보장 (수집기 §5.6 교훈)
    sys.exit(1)


def _scalar(rows: list[tuple]):
    return rows[0][0] if rows else None


def _check_step0_coverage(gate, cfg: Config, date: str) -> tuple[Coverage, Warn]:
    enabled = [r[0] for r in gate.query(SQL_ENABLED_SERVICES)]
    summary = {r[0] for r in gate.query(SQL_SUMMARY_SERVICES, {"d": date})}
    coverage = compute_coverage(enabled, summary, cfg.expected_late_services)
    if coverage.warn_targets:
        text = f'CHECK WARN coverage missing_services="{",".join(coverage.warn_targets)}"'
        print(text, flush=True)
        return coverage, Warn(1, text)
    return coverage, Warn(0, "")


def _validate_totals(gate, date: str) -> Warn:
    """(a) 3계층 합계 비교 — raw(원본 3필드 합) == mart 상세 == view 상세."""
    raw = _scalar(gate.query(SQL_VALIDATE_RAW_TOTAL, {"d": date}))
    mart = _scalar(gate.query(SQL_VALIDATE_MART_TOTAL, {"d": date}))
    view = _scalar(gate.query(SQL_VALIDATE_VIEW_TOTAL, {"d": date}))
    if raw == mart == view:
        return Warn(0, "")
    text = f"CHECK WARN totals_mismatch raw={raw} mart={mart} view={view}"
    print(text, flush=True)
    return Warn(1, text)


def _validate_diff_mismatch(gate, date: str) -> Warn:
    """(b) detail-vs-summary 대사 불일치 서비스 목록."""
    services = sorted({r[0] for r in gate.query(SQL_VALIDATE_DIFF_MISMATCH, {"d": date})})
    if not services:
        return Warn(0, "")
    text = f'CHECK WARN diff_mismatch services="{",".join(services)}"'
    print(text, flush=True)
    return Warn(1, text)


def _validate_org_mapping(gate, cfg: Config, date: str) -> Warn:
    """(c) org 매핑 실패율 — identified 행 중 org_path=['unknown'] 비율 > 임계."""
    rows = gate.query(SQL_VALIDATE_ORG_MAP_FAIL, {"d": date})
    unknown_cnt, identified_cnt = rows[0] if rows else (0, 0)
    if not identified_cnt:
        return Warn(0, "")
    rate = unknown_cnt / identified_cnt
    if rate <= cfg.org_map_warn_threshold:
        return Warn(0, "")
    text = f"CHECK WARN org_map_fail_rate={rate:.3f} threshold={cfg.org_map_warn_threshold}"
    print(text, flush=True)
    return Warn(1, text)


def _validate_unregistered_models(gate, date: str) -> Warn:
    """(d) dim_model 미등록 모델 집합 — cost IS NULL('unknown' 제외)."""
    models = sorted({r[0] for r in gate.query(SQL_VALIDATE_UNREGISTERED_MODELS, {"d": date})})
    if not models:
        return Warn(0, "")
    text = f'CHECK WARN unregistered_models="{",".join(models)}"'
    print(text, flush=True)
    return Warn(1, text)


def _emit_step_warns(warns: list[str]) -> Warn:
    """steps.py의 dup_suspect 등 STEP1/2 경고를 출력하고 Warn으로 집계 (§7.1 조용함 금지)."""
    for w in warns:
        print(f"CHECK WARN {w}", flush=True)
    return Warn(len(warns), "\n".join(warns))


def run_batch(cfg: Config, date: str, gate=None) -> int:
    """날짜 1개의 STEP 0→2 전체 + 인라인 검증 4종 + 마커 1줄. 0=SUCCESS, 1=FAILURE.

    광역 가드: STEP 0(쿼리·검증)·STEP 1·STEP 2·인라인 검증에서 발생하는 모든 예외
    → BATCH_RESULT status=FAILURE 마커 + return 1 (§5.6 마커 계약, §7.1 날짜별 독립).
    """
    gate = gate or CHGate(cfg)
    started = time.monotonic()
    warn = Warn(0, "")
    coverage = None
    rows_mart = 0
    rows_view = 0

    try:
        coverage, w0 = _check_step0_coverage(gate, cfg, date)
        warn = warn + w0
        _status["line"] = batch_line("FAILURE", coverage, 0, 0, warn.count, time.monotonic() - started)

        try:
            step1 = run_step1(gate, date)
        except StepError:
            elapsed = time.monotonic() - started
            line = batch_line("FAILURE", coverage, 0, 0, warn.count, elapsed)
            _status["line"] = line
            print(line, flush=True)
            return 1
        warn = warn + _emit_step_warns(step1["warns"])
        rows_mart = step1["rows_detail"]
        _status["line"] = batch_line("FAILURE", coverage, rows_mart, 0, warn.count, time.monotonic() - started)

        try:
            step2 = run_step2(gate, date)
        except StepError:
            elapsed = time.monotonic() - started
            line = batch_line("FAILURE", coverage, rows_mart, 0, warn.count, elapsed)
            _status["line"] = line
            print(line, flush=True)
            return 1
        warn = warn + _emit_step_warns(step2["warns"])
        rows_view = step2["rows_view_detail"]      # T3 인터페이스와 동일 정의 (rows_view_detail)
        _status["line"] = batch_line("FAILURE", coverage, rows_mart, rows_view, warn.count,
                                     time.monotonic() - started)

        warn = warn + _validate_totals(gate, date)
        warn = warn + _validate_diff_mismatch(gate, date)
        warn = warn + _validate_org_mapping(gate, cfg, date)
        warn = warn + _validate_unregistered_models(gate, date)

        elapsed = time.monotonic() - started
        line = batch_line("SUCCESS", coverage, rows_mart, rows_view, warn.count, elapsed)
        _status["line"] = line
        print(line, flush=True)
        return 0

    except Exception as exc:
        # 예상 밖 예외(TimeoutError, RuntimeError 등) — 마커 보장 + 날짜 독립 진행
        # 예외 메시지는 stderr로(마커 형식 오염 금지, user_id 원문 금지)
        exc_name = type(exc).__name__
        import sys
        print(f"ERROR in run_batch(date={date}): {exc_name}: {str(exc)[:200]}", file=sys.stderr, flush=True)

        elapsed = time.monotonic() - started
        if coverage is None:
            # STEP 0 실패 시 coverage 미초기화
            coverage = Coverage(0, 0, [], [])
        line = batch_line("FAILURE", coverage, rows_mart, rows_view, warn.count, elapsed)
        _status["line"] = line
        print(line, flush=True)
        return 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_time", nargs="?", default=None,
                        help="ISO8601 (기본 now, KST) — target_date = batch_time - 1일")
    parser.add_argument("--from", dest="from_date", default=None)
    parser.add_argument("--to", dest="to_date", default=None)
    args = parser.parse_args(argv)

    signal.signal(signal.SIGTERM, _sigterm_handler)
    cfg = load_config()
    dates, is_rerun = target_dates(args)
    if dates is None:
        return 2

    gate = CHGate(cfg)
    worst = 0
    for d in dates:            # 다중 날짜에도 날짜별 마커 독립 출력 — emit_batch 집계 없음 (§7.1)
        worst = max(worst, run_batch(cfg, d, gate=gate))
    return worst


if __name__ == "__main__":
    sys.exit(main())
