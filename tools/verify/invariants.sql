-- =============================================================
-- company 검증 성공 기준 — 실행형 불변식 (docs/operations/company-verify.md
-- "성공 기준 체크리스트"의 재현형, tools/verify/run_invariants.py가 렌더링·실행)
--
-- 실데이터 검증이므로 고정 기대값이 아니라 불변식으로 판정한다 —
-- mart/token-usage/tests/e2e/verify_expected_results.sql과 동일 패턴:
-- **빈 출력 = 전건 통과.** 어떤 SELECT든 1행이라도 나오면 그 행이 위반이다.
--
-- 치환 토큰 (run_invariants.py가 render()로 치환, 잔존 시 실행 실패):
--   {FACT}  — fact DB명 (기본 fact,     1단계 격리 시 token_verify_fact)
--   {DIM}   — dim  DB명 (기본 gpu_data, 1단계 격리 시 token_verify_dim)
--   {MART}  — mart DB명 (기본 mart,     1단계 격리 시 token_verify_mart)
--   {DATE}  — 대상일 'YYYY-MM-DD' (단일 날짜, 리터럴 문자열로 치환)
--
-- 사용법:
--   python3 tools/verify/run_invariants.py --date 2026-07-14
--   (또는 치환 후 clickhouse-client --multiquery로 직접 실행 — 읽기 전용 SELECT만)
--
-- 컬럼 계약: 전 SELECT가 3컬럼(check_name String, detail String, bad_count UInt64)
-- 로 통일되어 있다 — UNION ALL 체인에서 컬럼 타입이 갈라지면 NO_COMMON_TYPE으로
-- 실패하므로(Plan 3 전례, CH 24.8+ 계열 공통), bad_count는 전부 toUInt64(...)로,
-- detail은 전부 String(리터럴 또는 concat/toString)으로 캐스팅을 통일했다.
-- 원본 컬럼이 이미 해당 타입이어도(count() 등) 명시 캐스팅을 유지한다.
--
-- 이 파일은 검증(SELECT)만 수행한다 — INSERT/ALTER/DELETE 없음.
-- =============================================================

-- 1) layer3_sum_mismatch — raw(input+cache_read+cache_creation 합) == mart 상세
--    total_input 합 == view 상세 total_input 합. 하나라도 다르면 3계층 대사 실패
--    (§4.1). date={DATE} 단일일 스칼라 3개를 비교 — 위반 시 정확히 1행 노출.
SELECT
    'layer3_sum_mismatch' AS check_name,
    concat('raw=', toString(t.raw_sum), ' mart=', toString(t.mart_sum),
           ' view=', toString(t.view_sum)) AS detail,
    toUInt64(1) AS bad_count
FROM
(
    SELECT
        (SELECT sum(input_tokens + cache_read_tokens + cache_creation_tokens)
         FROM {FACT}.raw_token_usage_1d_dist WHERE date = '{DATE}') AS raw_sum,
        (SELECT sum(total_input_tokens)
         FROM {MART}.token_usage_1d_dist WHERE date = '{DATE}') AS mart_sum,
        (SELECT sum(total_input_tokens)
         FROM {DIM}.view_token_usage_1d_dist WHERE date = '{DATE}') AS view_sum
) AS t
WHERE t.raw_sum != t.mart_sum OR t.mart_sum != t.view_sum OR t.raw_sum != t.view_sum

UNION ALL

-- 2) reconcile_diff_nonzero — is_derived=0 AND reported_input_tokens IS NOT NULL
--    인데 diff_* 중 하나라도 !=0인 서비스({MART}.agg_token_service_1d_dist).
--    Σdetail == reported여야 정상(§4.1 대사 불일치 없음) — is_derived=1(파생)
--    서비스는 diff_*가 원래 NULL이므로 대상에서 자연 제외된다.
SELECT
    'reconcile_diff_nonzero' AS check_name,
    concat('service=', service,
           ' diff_input=', toString(coalesce(diff_input_tokens, 0)),
           ' diff_cache_read=', toString(coalesce(diff_cache_read_tokens, 0)),
           ' diff_cache_creation=', toString(coalesce(diff_cache_creation_tokens, 0)),
           ' diff_output=', toString(coalesce(diff_output_tokens, 0)),
           ' diff_requests=', toString(coalesce(diff_requests, 0))) AS detail,
    toUInt64(abs(coalesce(diff_input_tokens, 0)) + abs(coalesce(diff_cache_read_tokens, 0)) +
             abs(coalesce(diff_cache_creation_tokens, 0)) + abs(coalesce(diff_output_tokens, 0)) +
             abs(coalesce(diff_requests, 0))) AS bad_count
FROM {MART}.agg_token_service_1d_dist
WHERE date = '{DATE}'
  AND is_derived = 0
  AND reported_input_tokens IS NOT NULL
  AND (coalesce(diff_input_tokens, 0) != 0 OR coalesce(diff_cache_read_tokens, 0) != 0
       OR coalesce(diff_cache_creation_tokens, 0) != 0 OR coalesce(diff_output_tokens, 0) != 0
       OR coalesce(diff_requests, 0) != 0)

UNION ALL

-- 3) adoption_rate_over_1 — {DIM}.view_token_usage_org_1d_dist에서
--    adoption_rate > 1.0 (distinct_users > headcount는 물리적으로 불가능해야 함)
--    또는 org_path=[](빈 배열, 미매핑은 반드시 ['unknown']이어야 하므로 위반).
SELECT
    'adoption_rate_over_1' AS check_name,
    concat('org_path=', arrayStringConcat(org_path, '>'),
           ' distinct_users=', toString(distinct_users),
           ' headcount=', toString(headcount),
           ' adoption_rate=', toString(coalesce(adoption_rate, -1))) AS detail,
    toUInt64(distinct_users) AS bad_count
FROM {DIM}.view_token_usage_org_1d_dist
WHERE date = '{DATE}'
  AND (adoption_rate > 1.0 OR length(org_path) = 0)

UNION ALL

-- 4) cost_null_contract — model='unknown'인데 cost IS NOT NULL(위반) 또는
--    dim_token_model에 등록된 모델(unknown 제외)인데 cost IS NULL(위반).
--    등록 여부는 {DIM}.dim_token_model_dist에 (model, effective_from<=date) 존재로
--    판정 — dim에 'unknown' 행이 있어도(가격 NULL 캐치올) 그 행은 "등록"으로
--    치지 않는다($0/가격 위장 없음, §4.2 리뷰 #15). {MART}.token_usage_1d_dist ×
--    {DIM}.dim_token_model_dist, model 단위로 GROUP BY해 위반 모델당 1행.
SELECT
    'cost_null_contract' AS check_name,
    concat('model=', model,
           ' unknown_cost_leak=', toString(countIf(model = 'unknown' AND cost IS NOT NULL)),
           ' registered_cost_null=', toString(countIf(cost IS NULL))) AS detail,
    toUInt64(countIf(model = 'unknown' AND cost IS NOT NULL) + countIf(cost IS NULL)) AS bad_count
FROM {MART}.token_usage_1d_dist
WHERE date = '{DATE}'
  AND (
        model = 'unknown'
        OR model IN (
            SELECT model FROM {DIM}.dim_token_model_dist
            WHERE effective_from <= '{DATE}' AND model != 'unknown'
        )
      )
GROUP BY model
HAVING (model = 'unknown' AND countIf(cost IS NOT NULL) > 0)
    OR (model != 'unknown' AND countIf(cost IS NULL) > 0)

UNION ALL

-- 5) identified_name_leak — {DIM}.view_token_usage_1d_dist에서 user_type='identified'
--    AND user_name != '' (실명 노출 경계, §9-1 보류 — 0이어야 함).
--    detail에는 user_name 원문 대신 길이만 노출한다(진단은 가능하되 실명 자체를
--    추가로 유출하지 않기 위함 — §8.3 delete_data.py의 원문 비노출 관례와 동일).
SELECT
    'identified_name_leak' AS check_name,
    concat('user_id=', user_id, ' service=', service,
           ' user_name_len=', toString(length(user_name))) AS detail,
    toUInt64(1) AS bad_count
FROM {DIM}.view_token_usage_1d_dist
WHERE date = '{DATE}' AND user_type = 'identified' AND user_name != ''

UNION ALL

-- 6) created_by_wrong — {MART}.token_usage_1d_dist·{DIM}.view_token_usage_1d_dist에서
--    created_by != 'token-pipeline'(공유 쓰기 계약 위반, §4.2 리뷰 #22). created_by
--    값별로 GROUP BY해 위반 값당 1행.
SELECT
    'created_by_wrong' AS check_name,
    concat('table=mart_detail created_by=', created_by) AS detail,
    toUInt64(count()) AS bad_count
FROM {MART}.token_usage_1d_dist
WHERE date = '{DATE}' AND created_by != 'token-pipeline'
GROUP BY created_by

UNION ALL

SELECT
    'created_by_wrong' AS check_name,
    concat('table=view_detail created_by=', created_by) AS detail,
    toUInt64(count()) AS bad_count
FROM {DIM}.view_token_usage_1d_dist
WHERE date = '{DATE}' AND created_by != 'token-pipeline'
GROUP BY created_by

UNION ALL

-- 7) coverage_gap — {DIM}.dim_token_service_dist에 enabled=1인데 그 date의
--    {DIM}.view_token_usage_service_1d_dist에 없는 service(누락 커버리지).
--    주의: expected-late 예외(§5.9 계약 9조 — 늦게 도착 가능 서비스 화이트리스트)
--    판정은 이 SQL의 범위 밖이다. 이 검사는 순수 존재 여부만 보므로, expected-late
--    서비스가 아직 도착 전이면 여기서도 위반으로 잡힌다 — 그 경우 사람이
--    EXPECTED_LATE_SERVICES 목록과 대조해 예외 여부를 판단해야 한다(자동 면제 없음).
SELECT
    'coverage_gap' AS check_name,
    concat('service_group=', service_group, ' service=', service) AS detail,
    toUInt64(1) AS bad_count
FROM {DIM}.dim_token_service_dist
WHERE enabled = 1
  AND service NOT IN (
      SELECT DISTINCT service FROM {DIM}.view_token_usage_service_1d_dist
      WHERE date = '{DATE}'
  )
