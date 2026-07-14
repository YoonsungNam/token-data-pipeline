-- --expect-empty 방식: 기대와 다른(또는 필요한 행이 존재하지 않는) 경우만 SELECT —
-- 출력 없으면 통과 (collectors/token-usage/tests/e2e/verify_expected_results.sql 패턴 재사용).
-- 실행 전 치환: {DATE} {EXP_DETAIL_ROWS} {EXP_DETAIL_TOTAL_INPUT} {EXP_ORG_X} {EXP_ORG_Y}
--              {EXP_ORG_Z} {EXP_UNKNOWN_ROWS} {EXP_HAIKU_NULL_ROWS} {EXP_UNKNOWN_MODEL_ROWS}
--              {EXP_COST_SUM} {EXP_MAIN_U5_ROWS} {EXP_MAY_U5_ROWS}
-- 2026-05-15는 5월 고정 시드 날짜(brief §Step2) — 토큰화하지 않고 리터럴로 고정.
--
-- Service B/C/D 체크는 "행 존재 + 필드 상태"를 하나의 COUNT(...)==1 조건으로 합쳐
-- 단정한다 — 대상 행이 아예 없어도(위반행-빈결과 공허 통과) count()가 0이 되어 실패로
-- 잡히도록 설계했다(브리프의 "행 존재 단정 포함" 요구).

-- === 1) 상세 행수/합계 == mart_expectations 산출값(A+B+C 3서비스 합), 2-run 후 행수 동일 ===

SELECT 'detail_row_count_mismatch' AS check_name, count() AS actual, {EXP_DETAIL_ROWS} AS expected
FROM mart.token_usage_1d_dist
WHERE date = '{DATE}'
HAVING count() != {EXP_DETAIL_ROWS}

UNION ALL

SELECT 'detail_total_input_mismatch', sum(total_input_tokens), {EXP_DETAIL_TOTAL_INPUT}
FROM mart.token_usage_1d_dist
WHERE date = '{DATE}'
HAVING sum(total_input_tokens) != {EXP_DETAIL_TOTAL_INPUT}

UNION ALL

-- raw == mart (3계층 비교의 1층 — raw는 total_input_tokens 컬럼이 없어 3필드 합으로 재구성)
SELECT 'raw_total_mismatch', sum(input_tokens + cache_read_tokens + cache_creation_tokens),
       {EXP_DETAIL_TOTAL_INPUT}
FROM fact.raw_token_usage_1d_dist
WHERE date = '{DATE}'
HAVING sum(input_tokens + cache_read_tokens + cache_creation_tokens) != {EXP_DETAIL_TOTAL_INPUT}

UNION ALL

-- view == mart (3계층 비교의 3층)
SELECT 'view_total_mismatch', sum(total_input_tokens), {EXP_DETAIL_TOTAL_INPUT}
FROM gpu_data.view_token_usage_1d_dist
WHERE date = '{DATE}'
HAVING sum(total_input_tokens) != {EXP_DETAIL_TOTAL_INPUT}

UNION ALL

SELECT 'view_row_count_mismatch', count(), {EXP_DETAIL_ROWS}
FROM gpu_data.view_token_usage_1d_dist
WHERE date = '{DATE}'
HAVING count() != {EXP_DETAIL_ROWS}

UNION ALL

-- 재수집 멱등성: run_e2e.sh가 배치를 2회 실행한 뒤 이 verify를 1회 실행 —
-- 단일노드 E2E는 MergeTree라 블록 중복제거 미발생 — insert_deduplicate=0의 실검증은 test_ch.py 설정 단정 + accounts.sql ALTER USER + Plan 5 stage 2-run
SELECT 'no_duplicate_after_rerun', count(), {EXP_DETAIL_ROWS}
FROM mart.token_usage_1d_dist
WHERE date = '{DATE}'
HAVING count() != {EXP_DETAIL_ROWS}

UNION ALL

-- === 2) org 버킷 합계 + unknown 버킷 존재(user-0020~ 미등록분) ===

SELECT 'org_x_total_mismatch', sum(total_input_tokens), {EXP_ORG_X}
FROM mart.token_usage_1d_dist
WHERE date = '{DATE}' AND org_path = ['A부문','X팀']
HAVING sum(total_input_tokens) != {EXP_ORG_X}

UNION ALL

SELECT 'org_y_total_mismatch', sum(total_input_tokens), {EXP_ORG_Y}
FROM mart.token_usage_1d_dist
WHERE date = '{DATE}' AND org_path = ['A부문','Y팀']
HAVING sum(total_input_tokens) != {EXP_ORG_Y}

UNION ALL

SELECT 'org_z_total_mismatch', sum(total_input_tokens), {EXP_ORG_Z}
FROM mart.token_usage_1d_dist
WHERE date = '{DATE}' AND org_path = ['B부문','Z팀']
HAVING sum(total_input_tokens) != {EXP_ORG_Z}

UNION ALL

SELECT 'unknown_bucket_row_count_mismatch', count(), {EXP_UNKNOWN_ROWS}
FROM mart.token_usage_1d_dist
WHERE date = '{DATE}' AND org_path = ['unknown']
HAVING count() != {EXP_UNKNOWN_ROWS}

UNION ALL

-- === 3) 5월 검증 — user-0005 발생일 기준 조직 귀속(이관 전 X팀 / 이관 후 Z팀) ===

SELECT 'may_user5_pre_move_org_row_count_mismatch', count(), {EXP_MAY_U5_ROWS}
FROM mart.token_usage_1d_dist
WHERE date = '2026-05-15' AND user_id = 'user-0005' AND org_path = ['A부문','X팀']
HAVING count() != {EXP_MAY_U5_ROWS}

UNION ALL

SELECT 'main_user5_post_move_org_row_count_mismatch', count(), {EXP_MAIN_U5_ROWS}
FROM mart.token_usage_1d_dist
WHERE date = '{DATE}' AND user_id = 'user-0005' AND org_path = ['B부문','Z팀']
HAVING count() != {EXP_MAIN_U5_ROWS}

UNION ALL

-- === 4) cost 기대값(opus+sonnet, ±1e-6) + haiku/unknown 모델 cost NULL 전파 ===

SELECT 'cost_sum_mismatch', sum(cost), {EXP_COST_SUM}
FROM mart.token_usage_1d_dist
WHERE date = '{DATE}' AND model IN ('claude-opus-4-8', 'claude-sonnet-5')
HAVING abs(sum(cost) - {EXP_COST_SUM}) > 1e-6

UNION ALL

SELECT 'haiku_cost_not_null_leak', countIf(cost IS NOT NULL), 0
FROM mart.token_usage_1d_dist
WHERE date = '{DATE}' AND model = 'claude-haiku-4-5'
HAVING countIf(cost IS NOT NULL) != 0

UNION ALL

SELECT 'haiku_null_row_count_mismatch', countIf(cost IS NULL), {EXP_HAIKU_NULL_ROWS}
FROM mart.token_usage_1d_dist
WHERE date = '{DATE}' AND model = 'claude-haiku-4-5'
HAVING countIf(cost IS NULL) != {EXP_HAIKU_NULL_ROWS}

UNION ALL

SELECT 'unknown_model_cost_not_null_leak', countIf(cost IS NOT NULL), 0
FROM mart.token_usage_1d_dist
WHERE date = '{DATE}' AND model = 'unknown'
HAVING countIf(cost IS NOT NULL) != 0

UNION ALL

SELECT 'unknown_model_row_count_mismatch', count(), {EXP_UNKNOWN_MODEL_ROWS}
FROM mart.token_usage_1d_dist
WHERE date = '{DATE}' AND model = 'unknown'
HAVING count() != {EXP_UNKNOWN_MODEL_ROWS}

UNION ALL

-- === 5) Service B/C/D — agg_token_service_1d 상태(행 존재 + 필드 상태 동시 단정) ===

-- B: records만(summary 미적재) — reported_*/diff_* 전부 NULL
SELECT 'service_b_agg_state_wrong', count(), 1
FROM mart.agg_token_service_1d_dist
WHERE date = '{DATE}' AND service = 'Mock Service B'
  AND reported_input_tokens IS NULL AND reported_cache_read_tokens IS NULL
  AND reported_cache_creation_tokens IS NULL AND reported_output_tokens IS NULL
  AND reported_requests IS NULL
  AND diff_input_tokens IS NULL AND diff_cache_read_tokens IS NULL
  AND diff_cache_creation_tokens IS NULL AND diff_output_tokens IS NULL
  AND diff_requests IS NULL
HAVING count() != 1

UNION ALL

-- C: records+summary(is_derived=1) — reported_* 유지, diff_* NULL(파생 시맨틱)
SELECT 'service_c_agg_state_wrong', count(), 1
FROM mart.agg_token_service_1d_dist
WHERE date = '{DATE}' AND service = 'Mock Service C'
  AND is_derived = 1
  AND reported_input_tokens IS NOT NULL
  AND diff_input_tokens IS NULL AND diff_cache_read_tokens IS NULL
  AND diff_cache_creation_tokens IS NULL AND diff_output_tokens IS NULL
  AND diff_requests IS NULL
HAVING count() != 1

UNION ALL

-- D: summary만(NODATA) — summary-only 보강 행, sums 전부 0 + diff_*=0
SELECT 'service_d_agg_state_wrong', count(), 1
FROM mart.agg_token_service_1d_dist
WHERE date = '{DATE}' AND service = 'Mock Service D'
  AND input_tokens = 0 AND cache_read_tokens = 0 AND cache_creation_tokens = 0
  AND output_tokens = 0 AND total_input_tokens = 0 AND requests = 0
  AND diff_input_tokens = 0 AND diff_cache_read_tokens = 0
  AND diff_cache_creation_tokens = 0 AND diff_output_tokens = 0 AND diff_requests = 0
HAVING count() != 1

UNION ALL

-- === 6) created_by 전행 'token-pipeline' (mart 4 + view 4 테이블) ===

SELECT 'created_by_leak_mart_detail', countIf(created_by != 'token-pipeline'), 0
FROM mart.token_usage_1d_dist WHERE date = '{DATE}'
HAVING countIf(created_by != 'token-pipeline') != 0

UNION ALL

SELECT 'created_by_leak_mart_agg_service', countIf(created_by != 'token-pipeline'), 0
FROM mart.agg_token_service_1d_dist WHERE date = '{DATE}'
HAVING countIf(created_by != 'token-pipeline') != 0

UNION ALL

SELECT 'created_by_leak_mart_agg_org', countIf(created_by != 'token-pipeline'), 0
FROM mart.agg_token_org_1d_dist WHERE date = '{DATE}'
HAVING countIf(created_by != 'token-pipeline') != 0

UNION ALL

SELECT 'created_by_leak_mart_agg_model', countIf(created_by != 'token-pipeline'), 0
FROM mart.agg_token_model_1d_dist WHERE date = '{DATE}'
HAVING countIf(created_by != 'token-pipeline') != 0

UNION ALL

SELECT 'created_by_leak_view_detail', countIf(created_by != 'token-pipeline'), 0
FROM gpu_data.view_token_usage_1d_dist WHERE date = '{DATE}'
HAVING countIf(created_by != 'token-pipeline') != 0

UNION ALL

SELECT 'created_by_leak_view_service', countIf(created_by != 'token-pipeline'), 0
FROM gpu_data.view_token_usage_service_1d_dist WHERE date = '{DATE}'
HAVING countIf(created_by != 'token-pipeline') != 0

UNION ALL

SELECT 'created_by_leak_view_org', countIf(created_by != 'token-pipeline'), 0
FROM gpu_data.view_token_usage_org_1d_dist WHERE date = '{DATE}'
HAVING countIf(created_by != 'token-pipeline') != 0

UNION ALL

SELECT 'created_by_leak_view_model', countIf(created_by != 'token-pipeline'), 0
FROM gpu_data.view_token_usage_model_1d_dist WHERE date = '{DATE}'
HAVING countIf(created_by != 'token-pipeline') != 0
