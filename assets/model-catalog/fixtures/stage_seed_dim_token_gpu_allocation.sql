-- =============================================================
-- [stage 전용] gpu_data.dim_token_gpu_allocation 합성 시드 — 사내 적용 금지 (설계 §4.0)
-- 합성 데이터: Mock Group(tools/mock-provider·endpoints.yaml serviceGroup) H100 8장·A100 4장 — M2 그룹 행 경로 검증.
-- 적용: clickhouse-client --multiquery < assets/model-catalog/fixtures/stage_seed_dim_token_gpu_allocation.sql
-- 적용 순서: ddl/stage/seed_dim_token_*.sql(플레이스홀더 행, effective_from 2026-01-01) **이후**에 적용한다. 실값 행은
--   effective_from 2026-08-01 — 시드 키(2026-01-01)를 재사용하지 않는다(NOT IN 가드가 시드 행과 충돌해 무음 skip 되는 사고 방지).
-- =============================================================

INSERT INTO gpu_data.dim_token_gpu_allocation_dist
    (service_group, gpu_type, effective_from, allocated_gpu_count, source, note)
SELECT *
FROM (
    SELECT 'unknown' AS service_group, 'unknown' AS gpu_type, toDate('2026-01-01') AS effective_from,
           CAST(NULL AS Nullable(Float64)) AS allocated_gpu_count,
           'seed' AS source, '합성 — 플레이스홀더' AS note
    UNION ALL
    SELECT 'Mock Group', 'H100', toDate('2026-08-01'), toNullable(8.0), 'seed', '합성값 — stage 전용'
    UNION ALL
    SELECT 'Mock Group', 'A100', toDate('2026-08-01'), toNullable(4.0), 'seed', '합성값 — stage 전용'
)
WHERE (service_group, gpu_type, effective_from) NOT IN (
    SELECT service_group, gpu_type, effective_from FROM gpu_data.dim_token_gpu_allocation_dist
)
SETTINGS insert_distributed_sync = 1;

-- 검증: 결과가 비어야 정상 ------------------------------------------------
SELECT 'dup_key' AS check_name, concat(service_group, '/', gpu_type) AS key, effective_from, count() AS cnt
FROM gpu_data.dim_token_gpu_allocation_dist
GROUP BY service_group, gpu_type, effective_from
HAVING count() > 1

UNION ALL

SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'), countIf(allocated_gpu_count IS NOT NULL)
FROM gpu_data.dim_token_gpu_allocation_dist
WHERE gpu_type = 'unknown'
HAVING count() = 0 OR countIf(allocated_gpu_count IS NOT NULL) > 0

UNION ALL

SELECT 'negative_count', concat(service_group, '/', gpu_type), effective_from, toUInt64(1)
FROM gpu_data.dim_token_gpu_allocation_dist
WHERE allocated_gpu_count < 0;
