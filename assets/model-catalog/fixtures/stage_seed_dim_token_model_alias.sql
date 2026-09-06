-- =============================================================
-- [stage 전용] gpu_data.dim_token_model_alias 합성 시드 — 사내 적용 금지 (설계 §4.0: stage 합성값은 생성기 밖 fixture)
-- 합성 데이터: tools/mock-provider 모델 3종(claude-opus-4-8 / claude-sonnet-5 / claude-haiku-4-5)의 identity + 별칭 2건.
-- 적용: clickhouse-client --multiquery < assets/model-catalog/fixtures/stage_seed_dim_token_model_alias.sql
--   (stage 런북 절차 — docs/operations/token-metrics-deploy.md, Plan 6c). 3요소(NOT IN 가드·동기 삽입·말미 검증) 동일.
-- 적용 순서: ddl/stage/seed_dim_token_*.sql(플레이스홀더 행, effective_from 2026-01-01) **이후**에 적용한다. 실값 행은
--   effective_from 2026-08-01 — 시드 키(2026-01-01)를 재사용하지 않는다(NOT IN 가드가 시드 행과 충돌해 무음 skip 되는 사고 방지).
-- =============================================================

INSERT INTO gpu_data.dim_token_model_alias_dist
    (alias, effective_from, canonical, defining_service, source, note)
SELECT *
FROM (
    SELECT 'unknown' AS alias, toDate('2026-01-01') AS effective_from, 'unknown' AS canonical,
           '' AS defining_service, 'seed' AS source, '합성 — identity' AS note
    UNION ALL
    SELECT 'claude-opus-4-8', toDate('2026-08-01'), 'claude-opus-4-8', '', 'seed', '합성 — identity'
    UNION ALL
    SELECT 'claude-sonnet-5', toDate('2026-08-01'), 'claude-sonnet-5', '', 'seed', '합성 — identity'
    UNION ALL
    SELECT 'claude-haiku-4-5', toDate('2026-08-01'), 'claude-haiku-4-5', '', 'seed', '합성 — identity'
    UNION ALL
    SELECT 'claude-sonnet-5-20260101', toDate('2026-08-01'), 'claude-sonnet-5', 'Mock Service A', 'seed', '합성 — 날짜 접미 별칭'
    UNION ALL
    SELECT 'opus-4.8', toDate('2026-08-01'), 'claude-opus-4-8', 'Mock Service B', 'seed', '합성 — 축약 별칭'
)
WHERE (alias, effective_from) NOT IN (
    SELECT alias, effective_from FROM gpu_data.dim_token_model_alias_dist
)
SETTINGS insert_distributed_sync = 1;

-- 검증: 결과가 비어야 정상 ------------------------------------------------
SELECT 'dup_key' AS check_name, alias AS key, effective_from, count() AS cnt
FROM gpu_data.dim_token_model_alias_dist
GROUP BY alias, effective_from
HAVING count() > 1

UNION ALL

SELECT 'alias_maps_to_two_canonicals', alias, effective_from, uniqExact(canonical)
FROM gpu_data.dim_token_model_alias_dist
GROUP BY alias, effective_from
HAVING uniqExact(canonical) > 1

UNION ALL

SELECT 'alias_loop', alias, effective_from, toUInt64(1)
FROM gpu_data.dim_token_model_alias_dist
WHERE alias != canonical
  AND canonical GLOBAL IN (
      SELECT alias FROM gpu_data.dim_token_model_alias_dist WHERE alias != canonical
  )

UNION ALL

SELECT 'empty_canonical', alias, effective_from, toUInt64(1)
FROM gpu_data.dim_token_model_alias_dist
WHERE canonical = ''

UNION ALL

SELECT 'missing_identity_row', canonical, min(effective_from), count()
FROM gpu_data.dim_token_model_alias_dist
WHERE canonical != ''
  AND canonical GLOBAL NOT IN (
      SELECT alias FROM gpu_data.dim_token_model_alias_dist WHERE alias = canonical
  )
GROUP BY canonical

UNION ALL

SELECT 'service_not_in_registry', alias, effective_from, toUInt64(1)
FROM gpu_data.dim_token_model_alias_dist
WHERE defining_service != ''
  AND defining_service GLOBAL NOT IN (
      SELECT service FROM gpu_data.dim_token_metrics_service_dist
  );
