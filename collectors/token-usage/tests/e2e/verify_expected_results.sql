-- --expect-empty 방식: 기대와 다른 행만 SELECT — 출력 없으면 통과 (동료 s2job 패턴)
-- 실행 전 치환: {DATE} {SERVICE} {EXP_ROWS} {EXP_INPUT} {EXP_REQ}

SELECT 'detail_row_count_mismatch' AS check_name, count() AS actual, {EXP_ROWS} AS expected
FROM fact.raw_token_usage_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != {EXP_ROWS}

UNION ALL

SELECT 'detail_input_sum_mismatch', sum(input_tokens), {EXP_INPUT}
FROM fact.raw_token_usage_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING sum(input_tokens) != {EXP_INPUT}

UNION ALL

SELECT 'summary_row_missing', count(), 1
FROM fact.raw_token_usage_summary_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != 1

UNION ALL

SELECT 'summary_matches_detail', s.requests, {EXP_REQ}
FROM fact.raw_token_usage_summary_1d_dist AS s
WHERE s.date = '{DATE}' AND s.service = '{SERVICE}' AND s.requests != {EXP_REQ}

UNION ALL

SELECT 'dim_token_service_registered', count(), 1
FROM gpu_data.dim_token_service_dist
WHERE service = '{SERVICE}' AND source_type = 'usage-api-v1'
HAVING count() != 1

UNION ALL

-- 재수집 멱등성: 2회 실행 후에도 행수 동일 (E2E 스크립트가 2회 실행)
SELECT 'no_duplicate_after_rerun', count(), {EXP_ROWS}
FROM fact.raw_token_usage_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != {EXP_ROWS}
