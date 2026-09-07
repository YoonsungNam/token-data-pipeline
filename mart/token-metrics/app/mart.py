"""순수 로직 — coverage 게이트, 마커, 날짜 윈도우, 뮤테이션 예산 판정, 비용 모델 참조 구현.

I/O 금지 (클라이언트, 네트워크, 시계 부작용 없음). 마스터 §5.6 로깅 계약: user_id 원문·페이로드 미포함.

원형 mart/token-usage/app/mart.py 클론 — 6c 델타:
  - 마커(설계 §6.1): `module=mart-metrics`, `metrics_coverage=N/M`, `rows_mart/rows_check/rows_share`,
    말미 선택 ` reason=<r>`(mutation_budget / read_contract / <StepError.reason> / exception / sigterm).
  - `mutation_budget_exceeded(planned, budget)`: 첫 `_run_table` 전 예정 DELETE 합산 > 예산(기본 64)이면
    실행 없이 `FAILURE reason=mutation_budget`(설계 §6.1, §4.0 뮤테이션 장부 — 날짜당 ≤4).
  - 비용 모델 참조 구현(설계 §6.4 = 정의서 docs/cost-model-spec.md §3/§9): steps.py의 SQL과 동일 규칙을
    Python으로 적은 것. e2e mart_expectations.py가 이 함수들로 기대값을 만들어 SQL 결과와 대조한다.

컨트롤러 결정(스코프 아웃/추가 — footer Self-Review에 병합):
  - `Warn` 데이터클래스와 그 테스트는 원형에서 클론하지 않는다. 이후 태스크(steps.py/batch.py)는
    warn을 `list[str]`로 직접 다루며 인라인 WARN 라인을 직접 출력하므로 이 모듈은 `Warn`을 export하지 않는다.
  - `provider_self_weight(w_provider, consumer_total)`: §6.4 (4) `provider_reported` 분모 모드에서
    제공자의 자기 보고 가중치 중 소비자 몫을 뺀 나머지를 구하는 헬퍼(아래 정의 참조) — "미결 M4" 해소.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date as date_cls, datetime, timedelta

from app.ch import KST


@dataclass
class Coverage:
    """Coverage state: enabled count, present count, missing services, warn targets."""
    enabled: int             # M0 기대 서비스 수 (reg enabled=1 AND coverage_since ≤ d AND (until IS NULL OR d ≤ until))
    present: int             # 기대 중 앵커(summary)에 있는 서비스 수
    missing: list[str]       # 기대 중 앵커에 없는 서비스 (정렬) — 마커 missing_services에 전부 노출
    warn_targets: list[str]  # missing 중 expected_late에 없는 것 (정렬) — 6c batch는 expected_late=[]


def compute_coverage(
    expected_services,
    anchor_services,
    expected_late,
) -> Coverage:
    """
    Compute coverage state (설계 §6.1 M0).

    expected_services: reg에서 그날 커버리지가 기대되는 서비스 (list/set 무관)
    anchor_services:   그날 앵커(raw_token_metrics_summary_1d)에 있는 서비스 (list/set 무관)
    missing = expected - anchor (sorted)
    warn_targets = missing - expected_late (sorted)
    """
    expected_set = set(expected_services)
    anchor_set = set(anchor_services)
    missing_set = expected_set - anchor_set
    missing = sorted(missing_set)
    expected_late_set = set(expected_late)
    warn_targets = sorted(missing_set - expected_late_set)

    return Coverage(
        enabled=len(expected_set),
        present=len(expected_set & anchor_set),
        missing=missing,
        warn_targets=warn_targets,
    )


def batch_line(
    status: str,
    coverage: Coverage,
    rows_mart: int,
    rows_check: int,
    rows_share: int,
    warn_count: int,
    elapsed_s: float,
    reason: str = "",
) -> str:
    """
    Format batch result marker line (설계 §6.1 — 날짜당 정확히 1줄).

    Format: BATCH_RESULT status=<S> module=mart-metrics metrics_coverage=N/M
    missing_services="..." rows_mart=<n> rows_check=<n> rows_share=<n> warn=<n> elapsed=<sec, 1 decimal>
    [ reason=<r>]

    missing_services value is always double-quoted (마스터 §5.6 v1.10 — 서비스명 공백 보호).
    Empty missing list renders as "-". reason은 비어있지 않을 때만 말미에 붙는다.
    """
    if coverage.missing:
        missing_str = ",".join(coverage.missing)
    else:
        missing_str = "-"

    # metrics_coverage=N/M where N = present (앵커에 있는 기대 서비스), M = enabled (기대 서비스 전체)
    coverage_display = f"{coverage.present}/{coverage.enabled}"

    # Format elapsed to 1 decimal place
    elapsed_display = f"{elapsed_s:.1f}"

    line = (
        f"BATCH_RESULT status={status} module=mart-metrics metrics_coverage={coverage_display} "
        f'missing_services="{missing_str}" rows_mart={rows_mart} rows_check={rows_check} '
        f"rows_share={rows_share} warn={warn_count} elapsed={elapsed_display}"
    )
    if reason:
        line += f" reason={reason}"
    return line


def target_dates(args) -> tuple[list[str] | None, bool]:
    """
    Parse CLI args for target date(s).

    Returns (dates, is_rerun) where:
    - dates: list of YYYY-MM-DD strings (inclusive range), or None if args invalid
    - is_rerun: True if multi-date range (--from/--to), False otherwise

    Contract matches collectors' _target_dates:
    - --from/--to must be paired, YYYY-MM-DD, inclusive
    - naive datetime interpreted as KST
    - aware datetime converted to KST
    - default: batch_time = now(KST), target_date = yesterday
    """
    if args.from_date or args.to_date:
        # --from/--to must be paired
        if not (args.from_date and args.to_date):
            print("--from/--to는 쌍으로 지정 (KST, YYYY-MM-DD)", file=sys.stderr)
            return None, False

        d0 = date_cls.fromisoformat(args.from_date)
        d1 = date_cls.fromisoformat(args.to_date)
        # Inclusive range: (d1 - d0).days + 1
        dates = [str(d0 + timedelta(days=i)) for i in range((d1 - d0).days + 1)]
        return dates, True

    # Parse batch_time (default to now(KST))
    if args.batch_time:
        parsed = datetime.fromisoformat(args.batch_time)
        if parsed.tzinfo is None:
            # naive input is interpreted as KST (§5.1)
            parsed = parsed.replace(tzinfo=KST)
        batch_time = parsed.astimezone(KST)
    else:
        batch_time = datetime.now(KST)

    # target_date = batch_time - 1 day
    target_date = batch_time.date() - timedelta(days=1)
    return [str(target_date)], False


def mutation_budget_exceeded(planned: int, budget: int) -> bool:
    """
    뮤테이션 예산 선검사 (설계 §6.1, §4.0 장부).

    planned = 대상 날짜 전체 × MART_TABLES 4테이블의 `exists` 합산(= 예정 DELETE 수, 날짜당 ≤4).
    budget  = Config.max_mutations_per_run (MART_METRICS_MAX_MUTATIONS_PER_RUN, 기본 64 → 16일 rerun까지 통과).
    초과(planned > budget)면 batch는 변이 0으로 모든 날짜 `FAILURE reason=mutation_budget`(rerun은 --chunk-days 7).
    """
    return planned > budget


# ============================================================================
# 비용 모델 참조 구현 — 설계 §6.4 (1)~(7) = 정의서 docs/cost-model-spec.md §3/§9
#
# steps.py의 SQL과 동일 규칙(단위 테스트 = 정의서 §5 워크 예시, e2e = mart_expectations.py 기대값).
# 정의서 §9 의사코드 대비 파이프라인 보정: FAIL 플래그 행 제외(→ unattributed), TCO NULL 전파(부분 합 금지),
# idle 음수 클램프 0 + over_report(I1), 사외 API ③ /1e6, M1 quality_flag 우선순위, M4 분모 모드 6종.
# ============================================================================

# §6.4 (1) 파이프라인 보정 — 물리적으로 불가능(hours_over_count)하거나 모델 귀속 불가(unknown_violation)인 행은
# C에서 제외하고 그룹 행 unattributed_cost_krw로 노출. steps.FAIL_PRED가 이 튜플로 hasAny(...) 문자열을 만든다.
FAIL_FLAGS = ("hours_over_count", "unknown_violation")

# §6.4 (3) 가중 토큰 W — TCO 팀 승인값 정본(변경 시 상수 교체 + mart rerun). steps.py가 SQL 문자열에 삽입.
W_UNC = 1.0
W_CACHE = 0.1
W_OUT = 4.0

# §6.1 M1 quality_flag 우선순위 (multiIf 분기 순서와 동일, 마지막이 기본값)
M1_FLAG_PRIORITY = ("partial", "no_tco", "flagged", "manual", "no_metrics", "consumer_only", "normal")

# §6.1 M4 denominator_mode 6종 (순서 고정 — steps.py multiIf·DDL COMMENT와 동일)
DENOMINATOR_MODES = (
    "all_services",        # W(m) = Σ usage_svc 전 서비스 (기본)
    "provider_reported",   # usage_includes_consumers=1: W(m) = W(provider), 제공자 자기분 = max(W(m) − Σ_{s≠p} W(s), 0)
    "token_not_reported",  # W(m)=0 AND C>0: 제공자 행 share=1 전액 (I8 — 호스팅 그룹 귀속)
    "no_provider",         # gpu 행은 있으나 test뿐: C=0, 배분 없음
    "provider_ambiguous",  # 제공자 후보 다중: 후보별 행, share NULL (배부 보류)
    "external_api",        # gpu 행 전혀 없음: 벤더 단가 ③ (dim_token_vendor_price tier='standard')
)


def is_fail(flags) -> bool:
    """FAIL 플래그 판정 — SQL `hasAny(flags, ['hours_over_count','unknown_violation'])`와 동일."""
    return any(f in FAIL_FLAGS for f in flags)


def weighted_tokens(input_tokens, cache_read, cache_creation, output) -> float:
    """
    §6.4 (3) / 정의서 3.5: W(s, m, d) = 1·uncached + 0.1·cached + 4·output,
    uncached = input_tokens + cache_creation_tokens, cached = cache_read_tokens.
    """
    uncached = float(input_tokens) + float(cache_creation)
    return W_UNC * uncached + W_CACHE * float(cache_read) + W_OUT * float(output)


def model_cost(gpu_rows, tco) -> float | None:
    """
    §6.4 (1) / 정의서 3.2: C(m, d) = Σ_gpu_type (serving + standby) gpu_hours × TCO(gpu_type, d).

    gpu_rows: [(category, gpu_type, gpu_hours, flags)] — 한 (date, service, canon(model))의 gpu fact 행.
    tco:      {gpu_type: 원/GPU·h | None} — date 유효 이력 행(최신 행이 NULL이면 None).

    규칙(SQL_M1 model_cost_krw와 동일):
      - test는 C 불포함(그룹 귀속 — 정의서 3.3), FAIL 행 제외(→ unattributed).
      - 합산 대상 행의 기종 하나라도 TCO 미등록/None → None (부분 합 금지).
      - gpu 행이 전혀 없으면 None (has_gpu_rows=0 → NULL); test-only면 0.0 (no_provider).
    """
    if not gpu_rows:
        return None
    total = 0.0
    for category, gpu_type, gpu_hours, flags in gpu_rows:
        if category not in ("serving", "standby") or is_fail(flags):
            continue
        rate = tco.get(gpu_type)
        if rate is None:
            return None
        total += float(gpu_hours) * float(rate)
    return total


def allocate_shared(cost, wtokens) -> dict[str, float]:
    """
    §6.4 (4) / 정의서 3.6: 부담(s, m) = C(m) × W(s) / W(m), W(m) = Σ_s W(s).

    wtokens: {service: W(s, m, d)} — 모집단 = dim_token_service enabled=1 전 서비스.
    W(m) == 0 → {} (I8: 호출측이 token_not_reported로 제공자 행 share=1 전액 귀속).
    전용 모델(서비스 1개)은 자동으로 전액(I4); Σ_s 부담 = C (I3, ±1원).
    """
    total = sum(float(w) for w in wtokens.values())
    if total == 0:
        return {}
    return {s: float(cost) * float(w) / total for s, w in wtokens.items()}


def provider_self_weight(w_provider: float, consumer_total: float) -> float:
    """
    §6.4 (4) `provider_reported` 분모 모드의 제공자 자기분 헬퍼(컨트롤러 결정 — "미결 M4" 해소).

    usage_includes_consumers=1인 제공자 모델은 W(m) = W(provider)(자기 보고에 소비자 몫이 포함된
    값)로 잡는다. 이 UNCHANGED `allocate_shared(cost, wtokens)`에 넣을 제공자 자기분은
    max(W(provider) − Σ_{s≠p} W(s), 0) — 이렇게 만든 wtokens로 호출하면 share 합 = 1,
    배분 합 = C가 그대로 성립한다(I3). 소비자 토큰 합이 제공자 보고를 초과해도(비정상 입력 —
    consumer_tokens_exceed_provider WARN 대상, 이 헬퍼의 책임 밖) 제공자 배분이 0으로 클램프될
    뿐 항등식은 깨지지 않는다.
    """
    return max(float(w_provider) - float(consumer_total), 0.0)


def external_api_cost(input_tokens, cache_read, cache_creation, output, price) -> float | None:
    """
    §6.4 (6) / 정의서 3.9: ③ = (input × p_in + cache_read × p_cached + cache_creation × p_cc + output × p_out) / 1e6.

    price: (krw_per_mtok_input, krw_per_mtok_cached, krw_per_mtok_cache_creation, krw_per_mtok_output) 원/1M,
           dim_token_vendor_price tier='standard' date 유효 행. 하나라도 None → None (+ vendor_price_missing).
    input_tokens는 cache_creation을 제외한 순수 입력(3.5의 uncached와 혼용 금지 — 이중 계산 방지).
    """
    if any(p is None for p in price):
        return None
    p_in, p_cached, p_cc, p_out = (float(p) for p in price)
    return (
        float(input_tokens) * p_in
        + float(cache_read) * p_cached
        + float(cache_creation) * p_cc
        + float(output) * p_out
    ) / 1e6


def group_overhead(allocated_gpu_hours, reported_total, serving, standby, test, flagged, tco) -> dict:
    """
    §6.4 (2)/(7) / 정의서 3.1·3.3·3.4 — M2 agg_token_gpu_group_1d 한 (date, service_group, gpu_type) 행.

    allocated_gpu_hours: 할당표 allocated_gpu_count × 24 (없으면 None)
    reported_total:      Σ 보고 gpu_hours 전체(플래그 포함) = serving + standby + test + flagged
    serving/standby/test: 비FAIL 카테고리별 합, flagged: FAIL 행 합
    tco:                 원/GPU·h (None이면 비용 키 전부 None)

    idle = max(allocated − reported_total, 0) (I1: 음수면 over_report=1 + 0 클램프)
    그룹 총비용 = allocated × TCO (I2: = Σ C + test + idle + unattributed ± 오차 → identity_gap_krw)
    """
    result = {}
    if allocated_gpu_hours is None:
        idle = None
        utilization = None
        over_report = 0
    else:
        allocated = float(allocated_gpu_hours)
        reported = float(reported_total)
        idle = max(allocated - reported, 0.0)
        utilization = (reported / allocated) if allocated > 0 else None
        over_report = int(reported > allocated)
    result["idle_gpu_hours"] = idle
    result["utilization"] = utilization
    result["over_report"] = over_report

    if tco is None:
        for key in ("group_total_cost_krw", "model_cost_sum_krw", "test_cost_krw",
                    "idle_cost_krw", "unattributed_cost_krw", "identity_gap_krw"):
            result[key] = None
        return result

    rate = float(tco)
    result["model_cost_sum_krw"] = (float(serving) + float(standby)) * rate   # Σ 그룹 호스팅 모델 C
    result["test_cost_krw"] = float(test) * rate                              # 실험 비용 (그룹 귀속)
    result["unattributed_cost_krw"] = float(flagged) * rate                   # FAIL 행 × TCO (§6.4 (1) 보정)
    if allocated_gpu_hours is None:
        result["group_total_cost_krw"] = None
        result["idle_cost_krw"] = None
        result["identity_gap_krw"] = None
    else:
        result["group_total_cost_krw"] = float(allocated_gpu_hours) * rate    # 할당 × TCO (정의서 3.4, (7))
        result["idle_cost_krw"] = idle * rate                                 # 유휴 비용
        result["identity_gap_krw"] = (
            result["group_total_cost_krw"]
            - result["model_cost_sum_krw"]
            - result["test_cost_krw"]
            - result["idle_cost_krw"]
            - result["unattributed_cost_krw"]
        )
    return result


def quality_flag_m1(partial, no_tco, flagged, manual, no_metrics, consumer_only) -> str:
    """§6.1 M1 quality_flag — M1_FLAG_PRIORITY 순서의 첫 True, 전부 False면 'normal' (SQL multiIf와 동일)."""
    truth = (partial, no_tco, flagged, manual, no_metrics, consumer_only)
    for name, hit in zip(M1_FLAG_PRIORITY, truth):
        if hit:
            return name
    return M1_FLAG_PRIORITY[-1]
