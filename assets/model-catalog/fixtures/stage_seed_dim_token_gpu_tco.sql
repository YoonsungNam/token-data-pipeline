-- =============================================================
-- [stage 전용] gpu_data.dim_token_gpu_tco 합성 시드 — 사내 적용 금지 (설계 §4.0)
-- 합성 데이터: KRW/GPU-h 임의값(실제 TCO 아님) — mart-metrics stage e2e에서 비용 산식 경로만 검증.
-- 적용: clickhouse-client --multiquery < assets/model-catalog/fixtures/stage_seed_dim_token_gpu_tco.sql
-- 적용 순서: ddl/stage/seed_dim_token_*.sql(플레이스홀더 행, effective_from 2026-01-01) **이후**에 적용한다. 실값 행은
--   effective_from 2026-08-01 — 시드 키(2026-01-01)를 재사용하지 않는다(NOT IN 가드가 시드 행과 충돌해 무음 skip 되는 사고 방지).
-- =============================================================

INSERT INTO gpu_data.dim_token_gpu_tco_dist
    (gpu_type, effective_from, tco_krw_per_gpu_hour, currency, basis, note)
SELECT *
FROM (
    SELECT 'unknown' AS gpu_type, toDate('2026-01-01') AS effective_from,
           CAST(NULL AS Nullable(Float64)) AS tco_krw_per_gpu_hour,
           'KRW' AS currency, '' AS basis, '합성 — TCO 산정 불가' AS note
    UNION ALL
    -- 실값 행은 2026-08-01: 시드(ddl/stage/seed_dim_token_gpu_tco.sql)의 H100/A100/H200/L40S NULL 행 키(2026-01-01)와 겹치면
    -- NOT IN 가드가 이 행을 무음 skip 한다(시드가 먼저 적용됨) — 그래서 시드 키를 재사용하지 않는다
    SELECT 'H100', toDate('2026-08-01'), toNullable(4200.0), 'KRW', 'tco', '합성값 — stage 전용'
    UNION ALL
    SELECT 'A100', toDate('2026-08-01'), toNullable(2100.0), 'KRW', 'tco', '합성값 — stage 전용'
    UNION ALL
    SELECT 'H200', toDate('2026-08-01'), toNullable(5300.0), 'KRW', 'tco', '합성값 — stage 전용'
    UNION ALL
    SELECT 'L40S', toDate('2026-08-01'), toNullable(1300.0), 'KRW', 'tco', '합성값 — stage 전용'
    UNION ALL
    -- TCO 미등록 기종 경로(gpu_type_no_tco·tco_missing=1) 검증용: 행 자체를 두지 않는다 — B200은 의도적 부재
    SELECT 'H100', toDate('2026-08-26'), toNullable(4300.0), 'KRW', 'tco', '합성값 — 이력 2행째(effective_from 갱신 경로)'
)
WHERE (gpu_type, effective_from) NOT IN (
    SELECT gpu_type, effective_from FROM gpu_data.dim_token_gpu_tco_dist
)
SETTINGS insert_distributed_sync = 1;

-- 검증: 결과가 비어야 정상 ------------------------------------------------
SELECT 'dup_key' AS check_name, gpu_type AS key, effective_from, count() AS cnt
FROM gpu_data.dim_token_gpu_tco_dist
GROUP BY gpu_type, effective_from
HAVING count() > 1

UNION ALL

SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'), countIf(tco_krw_per_gpu_hour IS NOT NULL)
FROM gpu_data.dim_token_gpu_tco_dist
WHERE gpu_type = 'unknown'
HAVING count() = 0 OR countIf(tco_krw_per_gpu_hour IS NOT NULL) > 0

UNION ALL

SELECT 'basis_domain', gpu_type, effective_from, toUInt64(1)
FROM gpu_data.dim_token_gpu_tco_dist
WHERE basis NOT IN ('', 'depreciation', 'lease', 'power-inclusive', 'tco')

UNION ALL

SELECT 'currency_krw', gpu_type, effective_from, toUInt64(1)
FROM gpu_data.dim_token_gpu_tco_dist
WHERE currency != 'KRW';
