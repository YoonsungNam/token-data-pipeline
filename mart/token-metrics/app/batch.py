"""배치 오케스트레이터 — 읽기 계약 프리플라이트 → 변이 예산 프리체크 → (날짜별) M0 커버리지
→ M0b 토큰 mart 존재 확인 → RUNNERS(M1→M3; T6 M4·T7 M2 append) → BATCH_RESULT 마커 (Plan 6c T5).

원형 = mart/token-usage/app/batch.py(Plan 3 T4). 델타:
- 인라인 검증 4종 대신 M0/M0b(설계 §6.1) — 데이터 품질 검사는 M3 테이블(steps.py)이 담당.
- 첫 _run_table 전 프리플라이트(§7.5 읽기 계약 3테이블/13컬럼 DESCRIBE) + 예산 선검사(§4.0 장부 —
  대상 날짜 전체 × 4테이블 exists 합산 > MART_METRICS_MAX_MUTATIONS_PER_RUN → 전 날짜
  FAILURE reason=mutation_budget, 변이 0).
- 마커는 run_batch가 만들고(BatchOutcome.line) main()이 날짜당 정확히 1줄 출력한다.
- 메트릭 fact가 없는 날(앵커 0)은 토큰-only 행 + WARN — 절대 FAILURE 아님(§6.1).

로깅 계약(마스터 §5.6): 어떤 로그에도 user_id 원문·레코드 페이로드를 남기지 않는다
(서비스명·검사 이름·카운트만). 마커의 reason은 `[A-Za-z0-9_]+` 토큰 하나.
날짜는 서버 바인딩(`{d:Date}`)만 사용한다(§7.1) — 단, 컨트롤러 결정 D2: SQL_M0_REG_NOT_IN_USAGE는
날짜 무관(레지스트리끼리의 정합성 검사)이라 이 규칙에서 제외된다. DB명은 app.ch 상수 5종만(import
시 1회 보간).

service_not_in_usage_registry 경고(컨트롤러 결정 D1/fix1-1): 이 검사의 CHECK WARN 출력은
M3(steps.run_m3)이 생산자로서 이미 찍는다(`CHECK WARN service_not_in_usage_registry
severity=<S> count=<n>`) — batch는 M3(및 다른 러너)가 반환한 `CHECK WARN `/`CHECK INFO `
접두 줄을 재출력하지 않고 집계에만 반영한다(생산자가 찍는다·소비자는 재출력하지 않는다는
계약, RUNNERS 루프 참고). SQL_M0_REG_NOT_IN_USAGE(steps.SUB_REG/SUB_USAGE_SVC로 조립)는
조용히 실행해 서비스명만 마커 missing_services에 병합한다(fix1-4/5 — `_merge_not_in_usage`).
따라서 warn=은 `CHECK WARN ` 접두 줄 수(체크당 1줄)이고 `CHECK INFO`는 제외한다.
"""
from __future__ import annotations

import argparse
import dataclasses
import logging
import re
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Callable

from app.ch import CHGate, DB_DIM, DB_FACT, DB_MART, DB_TOKEN_DIM, DB_TOKEN_MART
from app.config import Config, load_config
from app.mart import Coverage, batch_line, compute_coverage, mutation_budget_exceeded, target_dates
from app.preflight import contract_tables, missing_columns
from app.steps import MART_TABLES, StepError, SUB_REG, SUB_USAGE_SVC, run_m1, run_m3

log = logging.getLogger("app.batch")

# =============================================================================
# M0 / M0b SQL — DB 상수 5종만, 날짜는 {d:Date} 바인딩 (§6.1)
# =============================================================================

# M0 기대 집합: 레지스트리 enabled + coverage 창(coverage_since ≤ d ≤ until | until IS NULL)
SQL_M0_EXPECTED_SERVICES = f"""
SELECT service
FROM {DB_DIM}.dim_token_metrics_service_dist
WHERE enabled = 1
  AND coverage_since <= {{d:Date}}
  AND (isNull(until) OR {{d:Date}} <= until)
ORDER BY service
"""

# M0 실제 집합: 앵커(summary) 서비스 — 메트릭 측 소스는 앵커가 있는 (date, service)만
SQL_M0_ANCHOR_SERVICES = f"""
SELECT service
FROM {DB_FACT}.raw_token_metrics_summary_1d_dist
WHERE date = {{d:Date}}
ORDER BY service
"""

# reg.service ∉ usage_svc — 이름만 missing_services에 조용히 병합한다(컨트롤러 결정 D1:
# CHECK WARN 출력은 M3(steps.run_m3)의 집계 1줄이 유일 소스 — 여기선 찍지 않는다). SUB_REG/
# SUB_USAGE_SVC는 steps.py 정본(§4.3 조인 키 전제와 동일 조건)을 그대로 재사용해 조립한다.
# 날짜 무관 — {d:Date} 규칙 예외(컨트롤러 결정 D2: app.steps SQL만 그 규칙의 대상).
SQL_M0_REG_NOT_IN_USAGE = f"""
SELECT r.service
FROM {SUB_REG} AS r
WHERE r.enabled = 1 AND r.service GLOBAL NOT IN {SUB_USAGE_SVC}
ORDER BY r.service
"""

# M0b: 토큰 mart에 D 행이 있는가 — 읽기 계약 agg_token_service_1d(date, service)만 참조 (§6.1)
SQL_M0B_TOKEN_MART_ROWS = f"""
SELECT count()
FROM {DB_TOKEN_MART}.agg_token_service_1d_dist
WHERE date = {{d:Date}}
"""

# 실행 순서 고정: M1 → M3. T6가 ("rows_share", run_m4), T7이 ("rows_group", run_m2)를 append.
# 각 러너: fn(gate, date) -> {"<key>": int, "warns": list[str]}.
RUNNERS: list[tuple[str, Callable]] = [("rows_mart", run_m1), ("rows_check", run_m3)]

# 마커 필드(Plan 6a H 고정) — rows_group(T7)은 마커에 싣지 않고 로그만
_MARKER_ROW_KEYS = ("rows_mart", "rows_check", "rows_share")
_REASON_RE = re.compile(r"[A-Za-z0-9_]+")
_EMPTY_COVERAGE = Coverage(0, 0, [], [])


@dataclass
class BatchOutcome:
    """run_batch 결과 — exit_code 0/1, 마커 1줄(line), M0b 스킵 플래그, 러너별 행수."""
    exit_code: int
    line: str
    skip_share: bool
    rows: dict = field(default_factory=dict)


# SIGTERM 캐시 줄 — 진행 중엔 status=FAILURE reason=sigterm(부분 진행 반영), 완료 후엔 최종 줄
_status = {"line": batch_line("FAILURE", _EMPTY_COVERAGE, 0, 0, 0, 0, 0.0, reason="sigterm")}


def _sigterm_handler(signum, frame):
    print(_status["line"] + " note=sigterm", flush=True)   # 진행 중 날짜의 마커 보장 (수집기 §5.6 교훈)
    sys.exit(143)


def _scalar(rows: list[tuple]):
    return rows[0][0] if rows else None


def _normalize_warn(w: str) -> str:
    """steps.py 경고(`dup_suspect:<dist>` 등 접두 없는 코드)에는 `CHECK WARN ` 접두를 붙이고,
    이미 `CHECK WARN `/`CHECK INFO `로 시작하는 M3 요약 줄(run_m3)은 그대로 둔다."""
    if w.startswith("CHECK WARN ") or w.startswith("CHECK INFO "):
        return w
    return f"CHECK WARN {w}"


def _warn(warns: list[str], text: str) -> None:
    """경고 1줄 즉시 stdout(§7.1 조용함 금지) + 집계 목록에 추가."""
    print(text, flush=True)
    warns.append(text)


def _warn_count(warns: list[str]) -> int:
    """마커 warn= — `CHECK WARN ` 접두 줄만 센다(`CHECK INFO`는 제외)."""
    return len([w for w in warns if w.startswith("CHECK WARN ")])


def _step_reason(exc: BaseException) -> str:
    """StepError 메시지의 첫 토큰만 reason으로 — 'verify_count failed: …' → 'verify_count'.
    마커에는 테이블명·카운트를 싣지 않는다(상세는 stderr)."""
    m = _REASON_RE.match(str(exc).strip())
    return m.group(0) if m else "step_error"


def _line(status: str, coverage: Coverage, rows: dict, warns: list[str], started: float,
          reason: str = "") -> str:
    return batch_line(status, coverage, rows["rows_mart"], rows["rows_check"], rows["rows_share"],
                      _warn_count(warns), time.monotonic() - started, reason)


def preflight_or_fail(gate) -> list[str]:
    """읽기 계약(§6.1 3테이블/13컬럼) DESCRIBE 프리플라이트 — install.sh [3/6]과 같은 계약을
    배치 기동 시 다시 확인한다(사내 스키마 드리프트 방어). 반환 = 누락 `db.table.col` 정렬 목록.
    DESCRIBE 자체가 실패(테이블 부재·권한)하면 그 테이블의 컬럼 전부를 누락으로 본다."""
    described: dict[str, list[str]] = {}
    for table in contract_tables():
        try:
            described[table] = list(gate.describe(f"{table}_dist"))
        except Exception as exc:
            log.error("DESCRIBE failed: %s_dist %s", table, type(exc).__name__)
            described[table] = []
    missing = missing_columns(described)
    if missing:
        print(f"PREFLIGHT FAIL read_contract missing={','.join(missing)}", flush=True)
    return missing


def plan_mutations(gate, dates: list[str]) -> int:
    """예정 DELETE 수 = (대상 날짜 × MART_TABLES 4테이블) 중 exists(_dist, d)인 쌍의 수.
    첫 _run_table 전에 한 번만 호출한다(§4.0 장부 — 실행당 가드)."""
    planned = 0
    for d in dates:
        for table in MART_TABLES:
            if gate.exists(f"{DB_MART}.{table}_dist", d):
                planned += 1
    return planned


def _fail_all(dates: list[str], reason: str) -> int:
    """프리플라이트·예산 실패: 모든 대상 날짜에 FAILURE 마커(coverage 0/0, rows 0) — 변이 0."""
    for d in dates:
        line = batch_line("FAILURE", _EMPTY_COVERAGE, 0, 0, 0, 0, 0.0, reason)
        _status["line"] = line
        print(line, flush=True)
    return 1


# =============================================================================
# M0 커버리지 · M0b 토큰 mart 존재 (§6.1)
# =============================================================================

def _check_m0(gate, date: str, warns: list[str]) -> Coverage:
    """M0 — 기대(레지스트리 coverage 창) vs 실제(앵커). EXPECTED_LATE 없음(설계 §6.1 — 빈 목록).
    누락 서비스명은 마커 missing_services에만 싣고 WARN 줄에는 카운트만 쓴다.

    레지스트리 정합성 검사(SQL_M0_REG_NOT_IN_USAGE)는 이 함수가 아니라 `_merge_not_in_usage`가
    담당한다(컨트롤러 결정 fix1-5 — M0과 분리해, 레지스트리 조회가 실패해도 여기서 이미 계산한
    coverage(present/enabled)를 호출자가 그대로 마커에 보존할 수 있게 한다)."""
    expected = [row[0] for row in gate.query(SQL_M0_EXPECTED_SERVICES, {"d": date})]
    anchors = {row[0] for row in gate.query(SQL_M0_ANCHOR_SERVICES, {"d": date})}
    coverage = compute_coverage(expected, anchors, [])
    if coverage.missing:
        _warn(warns, f"CHECK WARN metrics_coverage missing={len(coverage.missing)}")
    return coverage


def _merge_not_in_usage(gate, coverage: Coverage) -> Coverage:
    """reg.service ∉ usage_svc(SQL_M0_REG_NOT_IN_USAGE)는 컨트롤러 결정 D1에 따라 조용히
    실행한다 — CHECK WARN은 찍지 않고(M3의 집계 1줄이 유일 소스), 서비스명만 missing_services에
    병합한다. 마커 missing_services(합집합) ⊇ M0 커버리지 결손(coverage.missing) — 이 함수는
    T2 `compute_coverage()`가 반환한 `coverage` 객체를 변경하지 않고(컨트롤러 결정 fix1-4)
    `dataclasses.replace`로 새 Coverage를 만들어 반환한다. metrics_coverage=N/M(present/enabled)과
    `CHECK WARN metrics_coverage`의 missing=<n> 카운트는 이 병합 이전(M0-only)의 값을 그대로
    유지한다 — "그날 커버리지 결손"과 "레지스트리 정합성 결손"은 원인이 달라 카운트를 섞지 않는다."""
    not_in_usage = {row[0] for row in gate.query(SQL_M0_REG_NOT_IN_USAGE)}
    if not_in_usage:
        return dataclasses.replace(coverage, missing=sorted(set(coverage.missing) | not_in_usage))
    return coverage


def _token_mart_present(gate, date: str, token_mart_present: bool | None) -> bool:
    """M0b — 호출자가 명시하지 않으면 agg_token_service_1d의 D 행수로 판정."""
    if token_mart_present is None:
        return int(_scalar(gate.query(SQL_M0B_TOKEN_MART_ROWS, {"d": date})) or 0) > 0
    return bool(token_mart_present)


# =============================================================================
# run_batch — 날짜 1개: M0 → M0b → RUNNERS → 마커(반환) (§6.1, §7.1 날짜별 독립)
# =============================================================================

def run_batch(cfg: Config, date: str, gate=None, *, token_mart_present: bool | None = None) -> BatchOutcome:
    """날짜 1개의 M0→M0b→RUNNERS 전체 + 마커 1줄(반환, 출력은 main). exit_code 0=SUCCESS, 1=FAILURE.

    광역 가드: M0·M0b·러너에서 발생하는 모든 예외 → status=FAILURE 마커 + exit_code 1
    (StepError → reason=<첫 토큰>, 그 외 → reason=exception). 앵커 0건(no-metrics day)은
    예외가 아니라 WARN — M1은 토큰-only 행(no_metrics/consumer_only)을 적재하고 SUCCESS.
    _status["line"]은 단계마다 갱신해 SIGTERM 시 부분 진행이 담긴 마커가 나가게 한다 —
    try 진입 직후 첫 문장이 FAILURE/coverage=0/0/reason=sigterm으로 캐시를 리셋한다
    (컨트롤러 결정 fix1-2: 이전 날짜의 SUCCESS 캐시가 이번 날짜 M0 진행 중까지 남는 것을 방지).
    """
    gate = gate or CHGate(cfg)
    started = time.monotonic()
    warns: list[str] = []
    coverage = _EMPTY_COVERAGE
    rows = {k: 0 for k in _MARKER_ROW_KEYS}
    skip_share = False

    try:
        _status["line"] = _line("FAILURE", _EMPTY_COVERAGE, rows, warns, started, "sigterm")

        coverage = _check_m0(gate, date, warns)
        _status["line"] = _line("FAILURE", coverage, rows, warns, started, "sigterm")

        coverage = _merge_not_in_usage(gate, coverage)
        _status["line"] = _line("FAILURE", coverage, rows, warns, started, "sigterm")

        if not _token_mart_present(gate, date, token_mart_present):
            _warn(warns, f"CHECK WARN token_mart_absent date={date}")
            skip_share = True            # T6: rows_share(M4) 러너 스킵 근거 — T5는 플래그만 기록
            _status["line"] = _line("FAILURE", coverage, rows, warns, started, "sigterm")

        for key, fn in RUNNERS:
            result = fn(gate, date)
            rows[key] = int(result[key])
            for w in result["warns"]:
                # 컨트롤러 결정 fix1-1: 생산자(steps.run_m3 등)가 이미 CHECK WARN|INFO 접두 줄을
                # 찍었다면 재출력하지 않고 집계만 한다 — 접두 없는 코드(dup_suspect:<dist> 등,
                # 생산자가 찍지 않음)만 batch가 정규화해 1회 출력한다.
                if w.startswith("CHECK WARN ") or w.startswith("CHECK INFO "):
                    warns.append(w)
                else:
                    _warn(warns, _normalize_warn(w))
            _status["line"] = _line("FAILURE", coverage, rows, warns, started, "sigterm")

        line = _line("SUCCESS", coverage, rows, warns, started)
        _status["line"] = line
        return BatchOutcome(0, line, skip_share, dict(rows))

    except StepError as exc:
        print(f"ERROR in run_batch(date={date}): StepError: {str(exc)[:200]}", file=sys.stderr, flush=True)
        reason = _step_reason(exc)
    except Exception as exc:
        # 예상 밖 예외(TimeoutError, RuntimeError 등) — 마커 보장 + 날짜 독립 진행.
        # 예외 메시지는 stderr로(마커 형식 오염 금지, user_id 원문 금지 — 이름·200자 요약만)
        print(f"ERROR in run_batch(date={date}): {type(exc).__name__}: {str(exc)[:200]}",
              file=sys.stderr, flush=True)
        reason = "exception"

    line = _line("FAILURE", coverage, rows, warns, started, reason)
    _status["line"] = line
    return BatchOutcome(1, line, skip_share, dict(rows))


# =============================================================================
# CLI — [batch_time] | --date D | --from D --to D ; --log-level
# =============================================================================

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="app.batch", description="token-mart-metrics 일배치 (M0→M0b→M1→M3→M4→M2)")
    parser.add_argument("batch_time", nargs="?", default=None,
                        help="ISO8601 (기본 now, KST) — target_date = batch_time - 1일")
    parser.add_argument("--date", default=None, help="단일 날짜 YYYY-MM-DD (= --from D --to D)")
    parser.add_argument("--from", dest="from_date", default=None)
    parser.add_argument("--to", dest="to_date", default=None)
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    args = parser.parse_args(argv)
    if args.date:
        args.from_date = args.to_date = args.date
    logging.basicConfig(level=getattr(logging, args.log_level), stream=sys.stderr,
                        format="%(levelname)s %(name)s %(message)s", force=True)

    signal.signal(signal.SIGTERM, _sigterm_handler)
    cfg = load_config()
    dates, _is_rerun = target_dates(args)
    if dates is None:
        return 2
    if not dates:
        # target_dates()는 --from/--to 미쌍(None) 케이스만 자체 stderr 메시지를 찍는다 — 역전
        # 범위(--from이 --to보다 늦음)는 빈 리스트를 반환하며 아무 메시지도 없으므로 여기서
        # 보충한다(컨트롤러 결정 fix1-3; target_dates의 메시지와 중복 출력하지 않는다).
        print("[ERROR] 대상 날짜 없음 — --from은 --to보다 늦을 수 없습니다", file=sys.stderr, flush=True)
        return 2

    gate = CHGate(cfg)
    if preflight_or_fail(gate):                    # 읽기 계약 불일치 — 첫 날짜 처리 전, 변이 0
        return _fail_all(dates, "read_contract")

    planned = plan_mutations(gate, dates)          # 첫 _run_table 전 한 번 (§4.0 장부)
    log.info("mutation budget: planned=%d budget=%d dates=%d",
             planned, cfg.max_mutations_per_run, len(dates))
    if mutation_budget_exceeded(planned, cfg.max_mutations_per_run):
        print(f"BUDGET FAIL mutation_budget planned={planned} "
              f"budget={cfg.max_mutations_per_run} dates={len(dates)}", flush=True)
        return _fail_all(dates, "mutation_budget")

    worst = 0
    for d in dates:            # 날짜별 마커 독립 출력 — 한 날짜 FAILURE여도 나머지 계속 (§7.1)
        outcome = run_batch(cfg, d, gate=gate)
        print(outcome.line, flush=True)
        worst = max(worst, outcome.exit_code)
    return worst


if __name__ == "__main__":
    sys.exit(main())
