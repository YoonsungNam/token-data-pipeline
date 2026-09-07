"""Tests for app/mart.py — 순수 로직 (커버리지·마커·날짜창·뮤테이션 예산 + 비용 모델 참조 구현).

원형: mart/token-usage/tests/test_mart.py (마커 필드명·module 값·reason 접미가 6c 델타).
정의서 docs/cost-model-spec.md §5.1/§5.2/§5.3 워크 예시 값을 그대로 재현한다.

컨트롤러 결정: `Warn` 데이터클래스는 원형에서 클론하지 않는다(app/mart.py가 export하지 않음) —
이 테스트 모듈도 `Warn`을 import/테스트하지 않는다. `provider_self_weight` 테스트를 추가한다
(§6.4 (4) provider_reported 분모 모드 — "미결 M4" 해소).
"""
from __future__ import annotations

import argparse
from datetime import date as date_cls, datetime, timedelta, timezone

import pytest

from app.ch import KST
from app.mart import (
    Coverage,
    batch_line,
    compute_coverage,
    mutation_budget_exceeded,
    target_dates,
)


# ============================================================================
# compute_coverage
# ============================================================================

def test_compute_coverage_missing_sorted_and_present_count():
    """missing = expected − anchor (정렬), present = |expected ∩ anchor|; anchor는 list여도 된다."""
    c = compute_coverage(["Mock Service B", "Mock Service A", "Mock Service C"],
                         ["Mock Service A"], [])
    assert c.enabled == 3
    assert c.present == 1
    assert c.missing == ["Mock Service B", "Mock Service C"]
    assert c.warn_targets == ["Mock Service B", "Mock Service C"]


def test_compute_coverage_expected_late_excluded_from_warn_targets_only():
    """expected_late는 warn_targets에서만 제외 — 마커 missing에는 전부 노출 (batch는 []을 넘긴다)."""
    c = compute_coverage(["S1", "S2", "S3"], {"S1"}, expected_late=["S3"])
    assert (c.enabled, c.present) == (3, 1)
    assert c.missing == ["S2", "S3"]
    assert c.warn_targets == ["S2"]


def test_compute_coverage_empty_expected_is_zero_over_zero():
    """기대 집합이 비면 0/0 — no-metrics day라도 예외 없이 Coverage를 만든다 (§6.1: 절대 FAILURE 아님)."""
    c = compute_coverage([], [], [])
    assert (c.enabled, c.present, c.missing, c.warn_targets) == (0, 0, [], [])


# ============================================================================
# batch_line — 설계 §6.1 306 마커 형식 (날짜당 정확히 1줄)
# ============================================================================

def test_batch_line_exact_format():
    """필드 순서·이름·module 값이 설계 §6.1과 문자 단위로 일치한다."""
    cov = Coverage(enabled=2, present=1, missing=["Mock Service B"], warn_targets=["Mock Service B"])
    line = batch_line("SUCCESS", cov, 3, 5, 0, 1, 12.34)
    assert line == (
        'BATCH_RESULT status=SUCCESS module=mart-metrics metrics_coverage=1/2 '
        'missing_services="Mock Service B" rows_mart=3 rows_check=5 rows_share=0 warn=1 elapsed=12.3'
    )


def test_batch_line_reason_suffix():
    """reason이 있으면 말미에 ` reason=<r>`; missing 없음은 `missing_services="-"`."""
    cov = Coverage(enabled=2, present=2, missing=[], warn_targets=[])
    line = batch_line("FAILURE", cov, 0, 0, 0, 0, 0.04, reason="mutation_budget")
    assert line.endswith(" reason=mutation_budget")
    assert 'missing_services="-"' in line
    assert "status=FAILURE" in line and "metrics_coverage=2/2" in line
    assert "elapsed=0.0 reason=mutation_budget" in line
    # reason 미지정이면 접미가 붙지 않는다
    assert " reason=" not in batch_line("SUCCESS", cov, 1, 1, 1, 0, 1.0)


def test_batch_line_missing_with_spaces_quoted_and_comma_joined():
    """서비스명 공백은 쌍따옴표로 보호, 복수는 콤마 결합 (마스터 §5.6 v1.10)."""
    cov = Coverage(enabled=3, present=1, missing=["Mock Service B", "S3"], warn_targets=["Mock Service B", "S3"])
    line = batch_line("SUCCESS", cov, 0, 0, 0, 0, 1.0)
    assert 'missing_services="Mock Service B,S3"' in line


def test_batch_line_never_contains_user_id_or_payload():
    """마스터 §5.6 로깅 계약 — 마커에는 카운트·서비스명만 (user_id 원문·페이로드 부재)."""
    cov = Coverage(enabled=1, present=1, missing=[], warn_targets=[])
    line = batch_line("SUCCESS", cov, 10, 20, 30, 2, 99.99, reason="")
    assert "user_id" not in line
    assert "payload" not in line
    assert line.count("BATCH_RESULT") == 1
    assert "elapsed=100.0" in line


# ============================================================================
# target_dates — 원형 계약 (수집기 _target_dates와 동일)
# ============================================================================

def test_target_dates_range_inclusive_and_default_yesterday_kst():
    """--from/--to inclusive; 인자 없음 = 어제(KST); naive batch_time은 KST로 해석."""
    args = argparse.Namespace(from_date="2026-09-01", to_date="2026-09-03", batch_time=None)
    assert target_dates(args) == (["2026-09-01", "2026-09-02", "2026-09-03"], True)

    args = argparse.Namespace(from_date=None, to_date=None, batch_time="2026-09-04T10:20:00")
    assert target_dates(args) == (["2026-09-03"], False)

    before = datetime.now(KST).date() - timedelta(days=1)
    dates, is_rerun = target_dates(argparse.Namespace(from_date=None, to_date=None, batch_time=None))
    after = datetime.now(KST).date() - timedelta(days=1)
    assert is_rerun is False and len(dates) == 1
    assert dates[0] in {str(before), str(after)}
    assert isinstance(date_cls.fromisoformat(dates[0]), date_cls)


def test_target_dates_pair_required_and_aware_utc_converted_to_kst():
    """--from 단독은 (None, False); aware UTC는 KST로 변환 후 어제."""
    assert target_dates(argparse.Namespace(from_date="2026-09-01", to_date=None, batch_time=None)) == (None, False)
    assert target_dates(argparse.Namespace(from_date=None, to_date="2026-09-03", batch_time=None)) == (None, False)
    dt_utc = datetime(2026, 9, 4, 1, 20, tzinfo=timezone.utc)  # = 2026-09-04 10:20 KST
    args = argparse.Namespace(from_date=None, to_date=None, batch_time=dt_utc.isoformat())
    assert target_dates(args) == (["2026-09-03"], False)


# ============================================================================
# mutation_budget_exceeded — 설계 §4.0 129 (기본 64, 날짜당 ≤4 → 16일 = 64 통과, 17일 = 68 초과)
# ============================================================================

def test_mutation_budget_exceeded_boundary():
    assert mutation_budget_exceeded(64, 64) is False
    assert mutation_budget_exceeded(65, 64) is True
    assert mutation_budget_exceeded(0, 64) is False
    assert mutation_budget_exceeded(17 * 4, 64) is True
    assert mutation_budget_exceeded(16 * 4, 64) is False


# ============================================================================
# 비용 모델 참조 구현 — 설계 §6.4 (1)~(7) = 정의서 §3 수식 / §5 워크 예시 / §8 불변식 / §9 의사코드
# ============================================================================

from app.mart import (  # noqa: E402 — 2부 import (1부와 분리해 RED 메시지 고정)
    FAIL_FLAGS,
    DENOMINATOR_MODES,
    M1_FLAG_PRIORITY,
    W_CACHE,
    W_OUT,
    W_UNC,
    allocate_shared,
    external_api_cost,
    group_overhead,
    is_fail,
    model_cost,
    provider_self_weight,
    quality_flag_m1,
    weighted_tokens,
)


def test_weight_constants_are_1_0_1_4():
    """§6.4 (3): TCO 팀 승인값 정본 1 : 0.1 : 4 — steps.py가 SQL 문자열에 그대로 삽입한다."""
    assert (W_UNC, W_CACHE, W_OUT) == (1.0, 0.1, 4.0)
    assert all(isinstance(w, float) for w in (W_UNC, W_CACHE, W_OUT))
    # §6.1 M4 denominator_mode 6종 — 순서 고정 (steps.py의 multiIf 분기 순서·DDL COMMENT와 동일)
    assert DENOMINATOR_MODES == (
        "all_services", "provider_reported", "token_not_reported",
        "no_provider", "provider_ambiguous", "external_api",
    )


def test_is_fail_flags_hours_over_count_and_unknown_violation_only():
    """§6.4 (1) 파이프라인 보정: FAIL 플래그 2종만 C에서 제외 — 다른 플래그(pct_non_monotone 등)는 C 포함."""
    assert FAIL_FLAGS == ("hours_over_count", "unknown_violation")
    assert is_fail(["hours_over_count"]) is True
    assert is_fail(["pct_non_monotone", "unknown_violation"]) is True
    assert is_fail(["pct_non_monotone"]) is False
    assert is_fail([]) is False


def test_cost_spec_5_1_qwen_allocation_preserves_total():
    """정의서 §5.1: Qwen3-32B A100×2, serving 44 + standby 4, 단가 5,000원 → C = 240,000원.
    W: HR 챗봇 14M, 문서 요약 28M, 코딩 도우미 2M → 배분 76,364 / 152,727 / 10,909 (합 240,000 — I3)."""
    gpu_rows = [("serving", "A100", 44.0, []), ("standby", "A100", 4.0, [])]
    cost = model_cost(gpu_rows, {"A100": 5000.0})
    assert cost == 240000.0

    wtokens = {
        "HR 챗봇": weighted_tokens(10e6, 0, 0, 1e6),
        "문서 요약": weighted_tokens(20e6, 0, 0, 2e6),
        "코딩 도우미": weighted_tokens(1e6, 0, 0, 0.25e6),
    }
    assert wtokens == {"HR 챗봇": 14e6, "문서 요약": 28e6, "코딩 도우미": 2e6}

    alloc = allocate_shared(cost, wtokens)
    assert {s: round(v) for s, v in alloc.items()} == {"HR 챗봇": 76364, "문서 요약": 152727, "코딩 도우미": 10909}
    assert abs(sum(alloc.values()) - 240000.0) < 1  # I3: Σ_s 부담 = C (±1원)


def test_cost_spec_5_2_token_price_derivation():
    """정의서 §5.2: Llama-70B H100×4, 96 GPU·h, 단가 5,000원 → C = 480,000원;
    uncached 50M, cached 30M, output 10M → W = 50 + 3 + 40 = 93M; p = C/W ≈ 0.00516원/가중토큰.
    정의서 표기(5,160 / 516 / 20,600 원/1M)는 근사 — 허용오차로 단언, 검산은 정확히."""
    cost = model_cost([("serving", "H100", 96.0, [])], {"H100": 5000.0})
    assert cost == 480000.0
    w_model = weighted_tokens(50e6, 30e6, 0, 10e6)
    assert w_model == 93e6

    p = cost / w_model
    assert abs(p * 1e6 - 5160) < 2          # p_uncached ≈ 5,160원/1M (정확값 5161.29)
    assert abs(0.1 * p * 1e6 - 516) < 1     # p_cached ≈ 516원/1M
    assert abs(4 * p * 1e6 - 20600) < 50    # p_output ≈ 20,600원/1M (정확값 20645.16)
    # 검산: p × 토큰을 다시 더하면 C (순환 — 비용 입력이 아님, §6.4 (5))
    assert abs(50e6 * p + 30e6 * 0.1 * p + 10e6 * 4 * p - 480000.0) < 1e-6


def test_cost_spec_5_3_group_idle_zero_then_sixteen():
    """정의서 §5.3: 할당 H100 120 GPU·h/일. serving 96 + standby 24 → idle 0, 그룹 총비용 = C.
    다음 날 serving 80 + standby 24 → idle 16 → 유휴 비용 16 × 5,000 = 80,000원 (I2 항등식 gap 0)."""
    day1 = group_overhead(120, 120, 96, 24, 0, 0, 5000)
    assert day1["idle_gpu_hours"] == 0.0
    assert day1["identity_gap_krw"] == 0.0
    assert day1["over_report"] == 0
    assert day1["group_total_cost_krw"] == 600000.0
    assert day1["model_cost_sum_krw"] == 600000.0
    assert day1["utilization"] == 1.0

    day2 = group_overhead(120, 104, 80, 24, 0, 0, 5000)
    assert day2["idle_gpu_hours"] == 16.0
    assert day2["idle_cost_krw"] == 80000.0
    assert day2["identity_gap_krw"] == 0.0
    assert day2["test_cost_krw"] == 0.0 and day2["unattributed_cost_krw"] == 0.0
    assert abs(day2["utilization"] - 104 / 120) < 1e-12


def test_model_cost_null_when_any_tco_missing_and_excludes_test_and_fail():
    """§6.4 (1): serving+standby만, FAIL 행 제외, TCO 기종 하나라도 NULL이면 C NULL(부분 합 금지)."""
    rows = [
        ("serving", "H100", 10, []),
        ("serving", "B200", 1, []),                    # TCO 미등록 → 전체 NULL
        ("test", "H100", 5, []),                       # test는 C 불포함 (그룹 귀속)
        ("serving", "H100", 7, ["hours_over_count"]),  # FAIL → C 제외 (unattributed로)
    ]
    assert model_cost(rows, {"H100": 4200}) is None
    assert model_cost([r for r in rows if r[1] != "B200"], {"H100": 4200}) == 42000.0
    # 명시적 None 단가도 NULL 전파 (dim 최신 이력 행이 NULL인 경우)
    assert model_cost(rows, {"H100": 4200, "B200": None}) is None
    # gpu 행 없음 → NULL (has_gpu_rows=0); test-only → 0.0 (no_provider — C=0)
    assert model_cost([], {"H100": 4200}) is None
    assert model_cost([("test", "H100", 5, [])], {"H100": 4200}) == 0.0


def test_external_api_cost_formula_and_null():
    """§6.4 (6)/정의서 3.9: ③ = (input×p_in + cache_read×p_cached + cache_creation×p_cc + output×p_out)/1e6;
    여기서 input은 cache_creation을 제외한 순수 입력(3.5 uncached와 혼용 금지). 단가 하나라도 NULL → NULL."""
    price = (4050.0, 405.0, 5062.5, 20250.0)  # 원/1M — tier='standard'
    assert external_api_cost(1e6, 2e6, 0.5e6, 1e6, price) == 4050 + 810 + 2531.25 + 20250
    assert external_api_cost(1e6, 2e6, 0.5e6, 1e6, price) == 27641.25
    assert external_api_cost(1e6, 2e6, 0.5e6, 1e6, (4050.0, None, 5062.5, 20250.0)) is None
    assert external_api_cost(0, 0, 0, 0, price) == 0.0


def test_allocate_shared_dedicated_share_one_and_zero_total_empty():
    """I4: 전용 모델은 전액 귀속(share=1); I8: W(m)=0이면 {} — 호출측이 token_not_reported 처리(호스팅 그룹 귀속)."""
    assert allocate_shared(240000.0, {"only": 7e6}) == {"only": 240000.0}
    assert allocate_shared(240000.0, {}) == {}
    assert allocate_shared(240000.0, {"a": 0.0, "b": 0.0}) == {}
    assert allocate_shared(0.0, {"a": 1e6, "b": 3e6}) == {"a": 0.0, "b": 0.0}


def test_provider_self_weight_and_provider_reported_allocation_sums_to_cost():
    """scan-B C4 / §6.4 (4) provider_reported: 제공자 자기분 = max(W(provider) − Σ 소비자, 0)("미결 M4" 해소).
    이 자기분을 UNCHANGED allocate_shared(cost, wtokens)에 넣으면 share 합 = 1, 배분 합 = C가 유지된다(I3) —
    소비자 토큰 합이 제공자 보고를 초과해도(비정상 입력) 제공자 배분은 0으로 클램프될 뿐 항등식은 깨지지 않는다
    (consumer_tokens_exceed_provider WARN 발행은 이 헬퍼의 관심사가 아니다)."""
    assert provider_self_weight(100.0, 40.0) == 60.0
    assert provider_self_weight(100.0, 100.0) == 0.0
    assert provider_self_weight(100.0, 150.0) == 0.0  # 소비자 합이 제공자 보고 초과 → 0 클램프

    # 정상 케이스: 제공자 보고 W=100, 소비자 A=30/B=20 → 제공자 자기분 50, 배분 합 = C
    w_provider, consumers = 100.0, {"A": 30.0, "B": 20.0}
    wtokens = {"provider": provider_self_weight(w_provider, sum(consumers.values())), **consumers}
    cost = 10000.0
    alloc = allocate_shared(cost, wtokens)
    assert sum(alloc.values()) == pytest.approx(cost)
    assert alloc["provider"] == pytest.approx(5000.0)

    # exceed 케이스: 소비자 합(150)이 제공자 보고(100) 초과 → 제공자 자기분 0, 배분 합은 여전히 C
    consumers_exceed = {"A": 90.0, "B": 60.0}
    wtokens_exceed = {
        "provider": provider_self_weight(w_provider, sum(consumers_exceed.values())),
        **consumers_exceed,
    }
    alloc_exceed = allocate_shared(cost, wtokens_exceed)
    assert sum(alloc_exceed.values()) == pytest.approx(cost)
    assert alloc_exceed["provider"] == 0.0


def test_group_overhead_over_report_clamps_idle_and_null_propagation():
    """I1: 보고 > 할당이면 idle 0 클램프 + over_report=1(identity_gap ≠ 0로 드러남);
    할당 없음 → idle/utilization/group_total/identity_gap NULL; TCO 없음 → 비용 키 전부 NULL."""
    over = group_overhead(100, 120, 96, 24, 0, 0, 5000)
    assert over["idle_gpu_hours"] == 0.0 and over["over_report"] == 1
    assert over["identity_gap_krw"] == -100000.0
    assert abs(over["utilization"] - 1.2) < 1e-12

    no_alloc = group_overhead(None, 120, 96, 24, 0, 0, 5000)
    assert (no_alloc["idle_gpu_hours"], no_alloc["utilization"], no_alloc["group_total_cost_krw"],
            no_alloc["identity_gap_krw"], no_alloc["idle_cost_krw"]) == (None, None, None, None, None)
    assert no_alloc["over_report"] == 0
    assert no_alloc["model_cost_sum_krw"] == 600000.0  # C 합은 할당 없이도 계산된다

    no_tco = group_overhead(120, 120, 96, 24, 0, 0, None)
    for key in ("group_total_cost_krw", "model_cost_sum_krw", "test_cost_krw",
                "idle_cost_krw", "unattributed_cost_krw", "identity_gap_krw"):
        assert no_tco[key] is None
    assert no_tco["idle_gpu_hours"] == 0.0 and no_tco["over_report"] == 0

    # e2e 시드 값(T10): H100 그룹 — 할당 192, 보고 120 = serving 60 + standby 8 + test 2 + flagged 50
    e2e = group_overhead(192.0, 120.0, 60.0, 8.0, 2.0, 50.0, 4200.0)
    assert e2e["idle_gpu_hours"] == 72.0
    assert e2e["identity_gap_krw"] == 0.0  # 806,400 − 285,600 − 8,400 − 302,400 − 210,000
    assert set(e2e) == {
        "group_total_cost_krw", "model_cost_sum_krw", "test_cost_krw", "idle_gpu_hours",
        "idle_cost_krw", "unattributed_cost_krw", "identity_gap_krw", "utilization", "over_report",
    }


def test_quality_flag_m1_priority():
    """§6.1 M1 quality_flag 우선순위 고정: partial > no_tco > flagged > manual > no_metrics > consumer_only > normal."""
    assert M1_FLAG_PRIORITY == ("partial", "no_tco", "flagged", "manual", "no_metrics", "consumer_only", "normal")
    assert M1_FLAG_PRIORITY[-1] == "normal"
    assert quality_flag_m1(True, True, True, True, True, True) == "partial"
    assert quality_flag_m1(False, True, True, True, True, True) == "no_tco"
    assert quality_flag_m1(False, False, True, True, True, True) == "flagged"
    assert quality_flag_m1(False, False, False, True, True, False) == "manual"
    assert quality_flag_m1(False, False, False, False, True, True) == "no_metrics"
    assert quality_flag_m1(False, False, False, False, False, True) == "consumer_only"
    assert quality_flag_m1(False, False, False, False, False, False) == "normal"


# ============================================================================
# T6 — allocate_shared ↔ M4 all_services 산식 일치(설계 §6.1 share = W(s)/ΣW, 비용 = C(m)×share)
# ============================================================================
def test_allocate_shared_matches_m4_all_services_semantics():
    from app.mart import allocate_shared

    wt = {"A": 76364, "B": 152727, "C": 10909}     # W 가중 토큰(1·in + 0.1·cache_read + 4·out 합)
    total = 240000.0
    out = allocate_shared(total, wt)
    assert set(out) == set(wt)
    assert abs(sum(out.values()) - total) < 0.01
    w_sum = sum(wt.values())
    for s, w in wt.items():
        assert abs(out[s] - total * w / w_sum) < 0.01
    assert allocate_shared(total, {"A": 240000.0}) == {"A": 240000.0}
    assert allocate_shared(total, {}) == {}


# ============================================================================
# fix1 MUST-1(c) — C4 provider_reported 분모: SQL_M4 mode CTE 산식 = 파이썬 참조 구현 고정
# ============================================================================
def test_c4_provider_reported_denominator_matches_sql_formula_and_python_reference():
    """C4 — steps.py SQL_M4의 mode CTE 산식(D = max(W(p), Σ_{s≠p}W(s)); 제공자 자기분 share =
    max(W(p)−Σc, 0)/D; 소비자 share = W(s)/D)을 파이썬으로 그대로 재현해 매 케이스
    Σ share = 1·Σ 배분 = C를 확인하고, app.mart.provider_self_weight + allocate_shared
    (기존 참조 구현)이 같은 입력에서 같은 share를 내는지 대조한다 — SQL 산식과 파이썬 참조
    구현이 서로 고정(pinned)된다. w_p > Σc / w_p = Σc / w_p < Σc / w_p = 0(Σc > 0)/
    소비자 없음 5가지 케이스를 순회한다."""
    C = 12345.6
    cases = [
        (100.0, {"A": 30.0, "B": 20.0}),   # w_p(100) > Σc(50)
        (100.0, {"A": 60.0, "B": 40.0}),   # w_p(100) == Σc(100)
        (100.0, {"A": 90.0, "B": 60.0}),   # w_p(100) < Σc(150) — consumer_tokens_exceed_provider
        (0.0, {"A": 30.0, "B": 20.0}),     # w_p == 0, Σc > 0
        (100.0, {}),                        # 소비자 없음
    ]
    for w_p, consumers in cases:
        sum_c = sum(consumers.values())

        # --- SQL_M4 mode CTE의 세 식(D = max(w_p, Σc))을 파이썬으로 직접 재현 ---
        d = max(w_p, sum_c)
        assert d > 0, (w_p, consumers)   # 이 표의 케이스는 전부 provider_reported로 도달(D=0은 token_not_reported)
        provider_share = max(w_p - sum_c, 0.0) / d
        consumer_shares = {s: c / d for s, c in consumers.items()}
        shares = [provider_share] + list(consumer_shares.values())
        assert abs(sum(shares) - 1.0) < 1e-9, (w_p, consumers, shares)
        allocated = [s * C for s in shares]
        assert abs(sum(allocated) - C) < 1e-9, (w_p, consumers, allocated)

        # --- 파이썬 참조 구현(app.mart) 대조 — provider_self_weight + UNCHANGED allocate_shared가
        #     같은 입력에서 SQL과 같은 share를 내야 한다(총합 = D가 항상 성립하므로 자동으로 맞는다) ---
        wtokens = {"provider": provider_self_weight(w_p, sum_c), **consumers}
        alloc_ref = allocate_shared(C, wtokens)
        assert alloc_ref["provider"] / C == pytest.approx(provider_share)
        for s in consumers:
            assert alloc_ref[s] / C == pytest.approx(consumer_shares[s])

