-- --expect-empty 방식: 기대와 다른 행만 SELECT — 출력 없으면 통과 (기존 token-usage e2e 와 같은 형식)
-- 실행 전 치환: {DATE} {SERVICE} {EXP_GPU_ROWS} {EXP_SERVING_ROWS} {EXP_GPU_HOURS}
-- 실행 시점: 정기 2회(2회차 already_loaded) 직후 — 감사 0행·flags 빈 배열·앵커 1행이 전제 (§4.0 정기 = 뮤테이션 0)

SELECT 'gpu_row_count' AS check_name, count() AS actual, {EXP_GPU_ROWS} AS expected
FROM fact.raw_token_metrics_gpu_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != {EXP_GPU_ROWS}

UNION ALL

SELECT 'serving_row_count', count(), {EXP_SERVING_ROWS}
FROM fact.raw_token_metrics_serving_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != {EXP_SERVING_ROWS}

UNION ALL

SELECT 'gpu_hours_sum', sum(gpu_hours), {EXP_GPU_HOURS}
FROM fact.raw_token_metrics_gpu_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING abs(sum(gpu_hours) - {EXP_GPU_HOURS}) > 0.05

UNION ALL

SELECT 'summary_anchor_once', count(), 1
FROM fact.raw_token_metrics_summary_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != 1

UNION ALL

SELECT 'summary_source_type', count(), 0
FROM fact.raw_token_metrics_summary_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}' AND source_type != 'metrics-api-v1'
HAVING count() != 0

UNION ALL

SELECT 'summary_engine', count(), 0
FROM fact.raw_token_metrics_summary_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
  AND (engine_type != 'vllm' OR engine_version != '0.10.1')
HAVING count() != 0

UNION ALL

SELECT 'summary_counts', count(), 0
FROM fact.raw_token_metrics_summary_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
  AND (gpu_rows != {EXP_GPU_ROWS} OR serving_rows != {EXP_SERVING_ROWS})
HAVING count() != 0

UNION ALL

-- 2회 실행은 already_loaded 로 DELETE·감사 INSERT 가 없다 (§5.2 표 · §5.4 (2))
SELECT 'audit_empty', count(), 0
FROM fact.collect_audit_metrics_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != 0

UNION ALL

-- 정기 실행의 레지스트리 diff-sync 결과 (§4.3) — endpoints 1건 = 행 1건
SELECT 'registry_synced', count(), 1
FROM gpu_data.dim_token_metrics_service_dist
WHERE service = '{SERVICE}'
HAVING count() != 1

UNION ALL

-- toHour(collected_at) NOT BETWEEN 0 AND 23 는 항상 거짓(아무 것도 걸러내지 않음)이라 제외했다 —
-- 남은 두 시간창 조건만으로 "적재 시각이 지금 근방(과거 2시간 ~ 미래 10분)" 을 검사한다 (컨트롤러 판정).
SELECT 'collected_at_kst_sane', count(), 0
FROM fact.raw_token_metrics_gpu_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
  AND (collected_at < now('Asia/Seoul') - INTERVAL 2 HOUR
       OR collected_at > now('Asia/Seoul') + INTERVAL 10 MINUTE)
HAVING count() != 0

UNION ALL

-- 시나리오 OFF 기본 데이터 — gpu·serving 모두 flags 빈 배열 (T3)
SELECT 'no_flags_on_clean_run', count(), 0
FROM (
    SELECT flags FROM fact.raw_token_metrics_gpu_1d_dist
    WHERE date = '{DATE}' AND service = '{SERVICE}'
    UNION ALL
    SELECT flags FROM fact.raw_token_metrics_serving_1d_dist
    WHERE date = '{DATE}' AND service = '{SERVICE}'
)
WHERE length(flags) != 0
HAVING count() != 0
