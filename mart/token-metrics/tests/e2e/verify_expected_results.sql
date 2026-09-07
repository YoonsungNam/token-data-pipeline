-- expect-empty 방식: 기대와 다른(또는 필요한 행이 없는) 경우만 SELECT — 출력 없으면 통과
-- (mart/token-usage/tests/e2e/verify_expected_results.sql 패턴 재사용, Plan 6c T10).
-- 실행 전 치환: {DATE} {EXP_M1_ROWS} {EXP_M1_QWEN_COST} {EXP_M3_FAIL_ROWS} {EXP_M3_WARN_ROWS}
--              {EXP_M4_ROWS} {EXP_M4_QWEN_SUM} {EXP_M2_ROWS} {EXP_M2_IDLE_H100}
-- 모든 actual/expected는 toInt64 — UNION 체인의 UInt64/Float64 supertype 부재(NO_COMMON_TYPE, CH 24.8) 회피.
-- run_e2e.sh는 배치를 2회 실행한 뒤 이 파일을 1회 실행한다(행수 == 기대 = 멱등 검증).
-- 검사 21종(R5 — scan-C 컨트롤러 룰링: 20종 + m1_no_gpu_cost_null LEFT JOIN 미스 검사 1종 추가).

-- === 1) M1 agg_token_model_cost_1d ===

SELECT 'm1_rows' AS check_name, toInt64(count()) AS actual, toInt64({EXP_M1_ROWS}) AS expected
FROM mart.agg_token_model_cost_1d_dist
WHERE date = '{DATE}'
HAVING count() != {EXP_M1_ROWS}

UNION ALL

-- (A, Qwen3-32B) C = (serving 40h + standby 8h) × 4200 — test 2h 제외, 행 정확히 1개
SELECT 'm1_qwen_cost_a', toInt64(round(ifNull(sum(model_cost_krw), -1))), toInt64(round({EXP_M1_QWEN_COST}))
FROM mart.agg_token_model_cost_1d_dist
WHERE date = '{DATE}' AND service = 'Mock Service A' AND model = 'Qwen3-32B'
HAVING count() != 1 OR abs(ifNull(sum(model_cost_krw), -1) - {EXP_M1_QWEN_COST}) > 0.5

UNION ALL

-- quality_flag 4행 전부: normal / manual / no_tco / consumer_only (M1_FLAG_PRIORITY)
SELECT 'm1_flags',
       toInt64(countIf((service, model, quality_flag) IN (
           ('Mock Service A', 'Qwen3-32B', 'normal'),
           ('Mock Service B', 'Qwen3-32B', 'manual'),
           ('Mock Service B', 'claude-sonnet-5', 'no_tco'),
           ('Mock Service D', 'Qwen3-32B', 'consumer_only')))),
       toInt64(4)
FROM mart.agg_token_model_cost_1d_dist
WHERE date = '{DATE}'
HAVING countIf((service, model, quality_flag) IN (
           ('Mock Service A', 'Qwen3-32B', 'normal'),
           ('Mock Service B', 'Qwen3-32B', 'manual'),
           ('Mock Service B', 'claude-sonnet-5', 'no_tco'),
           ('Mock Service D', 'Qwen3-32B', 'consumer_only'))) != 4

UNION ALL

-- C는 토큰도 gpu 행도 없다 — M1 행 0 (metrics_missing은 M3에서만)
SELECT 'm1_c_absent', toInt64(countIf(service = 'Mock Service C')), toInt64(0)
FROM mart.agg_token_model_cost_1d_dist
WHERE date = '{DATE}'
HAVING countIf(service = 'Mock Service C') != 0

UNION ALL

-- R5(scan-C 컨트롤러 룰링): LEFT JOIN 미스 — (B, Qwen3-32B)·(D, Qwen3-32B)는 토큰 행은 있으나
-- gpu 행이 없다(join_use_nulls=0) — model_cost_krw NULL + has_gpu_rows=0 이 정확히 2건이어야 한다.
SELECT 'm1_no_gpu_cost_null',
       toInt64(countIf(model_cost_krw IS NULL AND has_gpu_rows = 0)),
       toInt64(2)
FROM mart.agg_token_model_cost_1d_dist
WHERE date = '{DATE}'
HAVING countIf(model_cost_krw IS NULL AND has_gpu_rows = 0) != 2

UNION ALL

-- === 2) M3 token_metrics_check_1d ===

SELECT 'm3_fail_rows', toInt64(countIf(severity = 'FAIL')), toInt64({EXP_M3_FAIL_ROWS})
FROM mart.token_metrics_check_1d_dist
WHERE date = '{DATE}'
HAVING countIf(severity = 'FAIL') != {EXP_M3_FAIL_ROWS}

UNION ALL

SELECT 'm3_warn_rows', toInt64(countIf(severity = 'WARN')), toInt64({EXP_M3_WARN_ROWS})
FROM mart.token_metrics_check_1d_dist
WHERE date = '{DATE}'
HAVING countIf(severity = 'WARN') != {EXP_M3_WARN_ROWS}

UNION ALL

SELECT 'm3_manual_source_b_info',
       toInt64(countIf(check_name = 'manual_source' AND service = 'Mock Service B' AND severity = 'INFO'
                       AND source_type = 'manual-v0')),
       toInt64(1)
FROM mart.token_metrics_check_1d_dist
WHERE date = '{DATE}'
HAVING countIf(check_name = 'manual_source' AND service = 'Mock Service B' AND severity = 'INFO'
               AND source_type = 'manual-v0') != 1

UNION ALL

SELECT 'm3_metrics_missing_c',
       toInt64(countIf(check_name = 'metrics_missing' AND service = 'Mock Service C' AND severity = 'FAIL')),
       toInt64(1)
FROM mart.token_metrics_check_1d_dist
WHERE date = '{DATE}'
HAVING countIf(check_name = 'metrics_missing' AND service = 'Mock Service C' AND severity = 'FAIL') != 1

UNION ALL

-- === 3) M4 agg_token_model_share_1d ===

SELECT 'm4_rows', toInt64(count()), toInt64({EXP_M4_ROWS})
FROM mart.agg_token_model_share_1d_dist
WHERE date = '{DATE}'
HAVING count() != {EXP_M4_ROWS}

UNION ALL

-- Qwen 3행(A/B/D) 전부 all_services, 제공자 A (A 행만 is_provider=1)
SELECT 'm4_qwen_mode_all_services',
       toInt64(countIf(model = 'Qwen3-32B' AND denominator_mode = 'all_services'
                       AND provider_service = 'Mock Service A'
                       AND is_provider = toUInt8(service = 'Mock Service A'))),
       toInt64(3)
FROM mart.agg_token_model_share_1d_dist
WHERE date = '{DATE}'
HAVING countIf(model = 'Qwen3-32B' AND denominator_mode = 'all_services'
               AND provider_service = 'Mock Service A'
               AND is_provider = toUInt8(service = 'Mock Service A')) != 3

UNION ALL

-- I3: Σ allocated(Qwen) == C(Qwen) (±0.5원)
SELECT 'm4_qwen_allocated_sum',
       toInt64(round(ifNull(sumIf(allocated_cost_krw, model = 'Qwen3-32B'), -1))),
       toInt64(round({EXP_M4_QWEN_SUM}))
FROM mart.agg_token_model_share_1d_dist
WHERE date = '{DATE}'
HAVING abs(ifNull(sumIf(allocated_cost_krw, model = 'Qwen3-32B'), -1) - {EXP_M4_QWEN_SUM}) > 0.5

UNION ALL

-- sonnet: 토큰 0·C NULL → 제공자 행 1개, share NULL, quality no_tco(M1 제공자 행 상속)
SELECT 'm4_sonnet_share_null',
       toInt64(countIf(model = 'claude-sonnet-5' AND service = 'Mock Service B' AND is_provider = 1
                       AND isNull(share) AND isNull(allocated_cost_krw) AND quality_flag = 'no_tco')),
       toInt64(1)
FROM mart.agg_token_model_share_1d_dist
WHERE date = '{DATE}'
HAVING countIf(model = 'claude-sonnet-5' AND service = 'Mock Service B' AND is_provider = 1
               AND isNull(share) AND isNull(allocated_cost_krw) AND quality_flag = 'no_tco') != 1
       OR countIf(model = 'claude-sonnet-5') != 1

UNION ALL

-- === 4) M2 agg_token_gpu_group_1d ===

SELECT 'm2_rows', toInt64(count()), toInt64({EXP_M2_ROWS})
FROM mart.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}'
HAVING count() != {EXP_M2_ROWS}

UNION ALL

-- H100: idle = 8×24 − 120 = 72h, over_report 0, FAIL 50h → quality flagged, 정체성 gap 0 (I2)
SELECT 'm2_h100_idle',
       toInt64(round(ifNull(sumIf(idle_gpu_hours, gpu_type = 'H100'), -1) * 1000)),
       toInt64(round({EXP_M2_IDLE_H100} * 1000))
FROM mart.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}' AND service_group = 'Mock Group'
HAVING abs(ifNull(sumIf(idle_gpu_hours, gpu_type = 'H100'), -1) - {EXP_M2_IDLE_H100}) > 0.0005
       OR countIf(gpu_type = 'H100' AND quality_flag = 'flagged' AND over_report = 0 AND tco_missing = 0
                  AND abs(ifNull(identity_gap_krw, 1)) < 0.5) != 1

UNION ALL

-- B200: 할당 없음 + TCO 없음 → no_tco(우선순위 no_tco > no_allocation), 비용 컬럼 NULL
SELECT 'm2_b200_no_tco',
       toInt64(countIf(gpu_type = 'B200' AND quality_flag = 'no_tco' AND tco_missing = 1
                       AND isNull(allocated_gpu_hours) AND isNull(group_total_cost_krw)
                       AND isNull(idle_gpu_hours))),
       toInt64(1)
FROM mart.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}' AND service_group = 'Mock Group'
HAVING countIf(gpu_type = 'B200' AND quality_flag = 'no_tco' AND tco_missing = 1
               AND isNull(allocated_gpu_hours) AND isNull(group_total_cost_krw)
               AND isNull(idle_gpu_hours)) != 1

UNION ALL

-- A100: gpu 행 0 + 할당 4 → alloc-only 행, idle 96h = 전액 유휴, gap 0, normal
SELECT 'm2_a100_alloc_only_normal',
       toInt64(countIf(gpu_type = 'A100' AND quality_flag = 'normal' AND reported_gpu_hours_total = 0
                       AND abs(ifNull(idle_gpu_hours, -1) - 96) < 0.0005
                       AND abs(ifNull(identity_gap_krw, 1)) < 0.5)),
       toInt64(1)
FROM mart.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}' AND service_group = 'Mock Group'
HAVING countIf(gpu_type = 'A100' AND quality_flag = 'normal' AND reported_gpu_hours_total = 0
               AND abs(ifNull(idle_gpu_hours, -1) - 96) < 0.0005
               AND abs(ifNull(identity_gap_krw, 1)) < 0.5) != 1

UNION ALL

-- === 5) created_by 전행 'token-metrics-pipeline' (mart 4테이블) ===

SELECT 'created_by_all_tables', toInt64(sum(bad)), toInt64(0)
FROM
(
    SELECT countIf(created_by != 'token-metrics-pipeline') AS bad
    FROM mart.agg_token_model_cost_1d_dist WHERE date = '{DATE}'
    UNION ALL
    SELECT countIf(created_by != 'token-metrics-pipeline')
    FROM mart.token_metrics_check_1d_dist WHERE date = '{DATE}'
    UNION ALL
    SELECT countIf(created_by != 'token-metrics-pipeline')
    FROM mart.agg_token_model_share_1d_dist WHERE date = '{DATE}'
    UNION ALL
    SELECT countIf(created_by != 'token-metrics-pipeline')
    FROM mart.agg_token_gpu_group_1d_dist WHERE date = '{DATE}'
)
HAVING sum(bad) != 0

UNION ALL

-- === 6) 2회 실행 후 키 중복 0 (DELETE → INSERT 원자 교체, insert_deduplicate=0) ===

SELECT 'idempotent_no_dup_m1', toInt64(count()), toInt64(uniqExact((service, model)))
FROM mart.agg_token_model_cost_1d_dist
WHERE date = '{DATE}'
HAVING count() != uniqExact((service, model))

UNION ALL

SELECT 'idempotent_no_dup_m2', toInt64(count()), toInt64(uniqExact((service_group, gpu_type)))
FROM mart.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}'
HAVING count() != uniqExact((service_group, gpu_type))

UNION ALL

SELECT 'idempotent_no_dup_m4', toInt64(count()), toInt64(uniqExact((model, service, provider_service)))
FROM mart.agg_token_model_share_1d_dist
WHERE date = '{DATE}'
HAVING count() != uniqExact((model, service, provider_service))
