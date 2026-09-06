-- =============================================================
-- [stage 전용] gpu_data.dim_token_vendor_price 합성 시드 — 사내 적용 금지 (설계 §4.0)
-- 합성 데이터: seed_dim_token_model.sql 공표 USD 단가 × 합성 환율 1350 (실제 환율·계약가 아님) — M4 external_api 경로 검증.
-- 적용: clickhouse-client --multiquery < assets/model-catalog/fixtures/stage_seed_dim_token_vendor_price.sql
-- 적용 순서: ddl/stage/seed_dim_token_*.sql(플레이스홀더 행, effective_from 2026-01-01) **이후**에 적용한다. 실값 행은
--   effective_from 2026-08-01 — 시드 키(2026-01-01)를 재사용하지 않는다(NOT IN 가드가 시드 행과 충돌해 무음 skip 되는 사고 방지).
-- =============================================================

INSERT INTO gpu_data.dim_token_vendor_price_dist
    (provider, model, tier, effective_from, krw_per_mtok_input, krw_per_mtok_cached,
     krw_per_mtok_cache_creation, krw_per_mtok_output, note)
SELECT *
FROM (
    SELECT 'unknown' AS provider, 'unknown' AS model, 'standard' AS tier, toDate('2026-01-01') AS effective_from,
           CAST(NULL AS Nullable(Float64)) AS krw_per_mtok_input,
           CAST(NULL AS Nullable(Float64)) AS krw_per_mtok_cached,
           CAST(NULL AS Nullable(Float64)) AS krw_per_mtok_cache_creation,
           CAST(NULL AS Nullable(Float64)) AS krw_per_mtok_output,
           '합성 — 플레이스홀더' AS note
    UNION ALL
    SELECT 'anthropic', 'claude-opus-4-8', 'standard', toDate('2026-08-01'),
           toNullable(6750.0), toNullable(675.0), toNullable(8437.5), toNullable(33750.0), '합성값 — USD×1350'
    UNION ALL
    SELECT 'anthropic', 'claude-sonnet-5', 'standard', toDate('2026-08-01'),
           toNullable(4050.0), toNullable(405.0), toNullable(5062.5), toNullable(20250.0), '합성값 — USD×1350'
    UNION ALL
    SELECT 'anthropic', 'claude-haiku-4-5', 'standard', toDate('2026-08-01'),
           toNullable(1350.0), toNullable(135.0), toNullable(1687.5), toNullable(6750.0), '합성값 — USD×1350'
)
WHERE (provider, model, tier, effective_from) NOT IN (
    SELECT provider, model, tier, effective_from FROM gpu_data.dim_token_vendor_price_dist
)
SETTINGS insert_distributed_sync = 1;

-- 검증: 결과가 비어야 정상 ------------------------------------------------
SELECT 'dup_key' AS check_name, concat(provider, '/', model, '/', tier) AS key, effective_from, count() AS cnt
FROM gpu_data.dim_token_vendor_price_dist
GROUP BY provider, model, tier, effective_from
HAVING count() > 1

UNION ALL

SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'),
       countIf(krw_per_mtok_input IS NOT NULL OR krw_per_mtok_cached IS NOT NULL
               OR krw_per_mtok_cache_creation IS NOT NULL OR krw_per_mtok_output IS NOT NULL)
FROM gpu_data.dim_token_vendor_price_dist
WHERE provider = 'unknown' AND model = 'unknown'
HAVING count() = 0
    OR countIf(krw_per_mtok_input IS NOT NULL OR krw_per_mtok_cached IS NOT NULL
               OR krw_per_mtok_cache_creation IS NOT NULL OR krw_per_mtok_output IS NOT NULL) > 0

UNION ALL

SELECT 'tier_domain', concat(provider, '/', model, '/', tier), effective_from, toUInt64(1)
FROM gpu_data.dim_token_vendor_price_dist
WHERE tier NOT IN ('standard', 'batch', 'flex', 'priority');
