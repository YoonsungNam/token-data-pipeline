-- =============================================================
-- gpu_data.dim_token_gpu_tco 시드 (설계 2026-08-31 §4.2 — dim_holiday 3요소 패턴)
-- (a) 출처·기준일: 플레이스홀더만 — unknown + {H100, A100, H200, L40S} 전부 NULL(2026-01-01).
--     실값(TCO팀 확정, KRW/GPU-h)은 assets/model-catalog/csv_to_layer_c_dim_insert.py --table gpu_tco
--     생성 SQL(gitignore: dim_token_gpu_*_insert*.sql)을 admin이 append 적용 — effective_from = 소급 시작일(> 2026-01-01).
-- (b) NOT IN 멱등 가드 — 재실행 안전.
-- (c) 말미 검증 SELECT — 결과가 비어야 정상.
-- 실행 주체: admin 수동. mart는 SELECT만. stage 합성값은 fixtures/stage_seed_dim_token_gpu_tco.sql.
-- =============================================================

INSERT INTO gpu_data.dim_token_gpu_tco_dist
    (gpu_type, effective_from, tco_krw_per_gpu_hour, currency, basis, note)
SELECT *
FROM (
    -- 계약 표준값 unknown: TCO NULL — 미등록 기종 비용이 0원으로 위장되지 않게
    SELECT 'unknown' AS gpu_type, toDate('2026-01-01') AS effective_from,
           CAST(NULL AS Nullable(Float64)) AS tco_krw_per_gpu_hour,
           'KRW' AS currency, '' AS basis, '계약 표준 값 — TCO 산정 불가' AS note
    UNION ALL
    SELECT 'H100', toDate('2026-01-01'), CAST(NULL AS Nullable(Float64)), 'KRW', '',
           '플레이스홀더 — 실값은 Layer C 생성 SQL로 append (gpu_type_no_tco 발화 대상)'
    UNION ALL
    SELECT 'A100', toDate('2026-01-01'), CAST(NULL AS Nullable(Float64)), 'KRW', '',
           '플레이스홀더 — 실값은 Layer C 생성 SQL로 append'
    UNION ALL
    SELECT 'H200', toDate('2026-01-01'), CAST(NULL AS Nullable(Float64)), 'KRW', '',
           '플레이스홀더 — 실값은 Layer C 생성 SQL로 append'
    UNION ALL
    SELECT 'L40S', toDate('2026-01-01'), CAST(NULL AS Nullable(Float64)), 'KRW', '',
           '플레이스홀더 — 실값은 Layer C 생성 SQL로 append'
)
WHERE (gpu_type, effective_from) NOT IN (
    SELECT gpu_type, effective_from FROM gpu_data.dim_token_gpu_tco_dist
)
SETTINGS insert_distributed_sync = 1;

-- 검증: 결과가 비어야 정상 ------------------------------------------------
-- 1) (gpu_type, effective_from) 키 중복 없음
SELECT 'dup_key' AS check_name, gpu_type AS key, effective_from, count() AS cnt
FROM gpu_data.dim_token_gpu_tco_dist
GROUP BY gpu_type, effective_from
HAVING count() > 1

UNION ALL

-- 2) unknown 행 존재 + TCO 전부 NULL (행 부재 또는 값 오염 시 1행 노출)
SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'), countIf(tco_krw_per_gpu_hour IS NOT NULL)
FROM gpu_data.dim_token_gpu_tco_dist
WHERE gpu_type = 'unknown'
HAVING count() = 0 OR countIf(tco_krw_per_gpu_hour IS NOT NULL) > 0

UNION ALL

-- 3) basis 도메인
SELECT 'basis_domain', gpu_type, effective_from, toUInt64(1)
FROM gpu_data.dim_token_gpu_tco_dist
WHERE basis NOT IN ('', 'depreciation', 'lease', 'power-inclusive', 'tco')

UNION ALL

-- 4) 통화 KRW 고정
SELECT 'currency_krw', gpu_type, effective_from, toUInt64(1)
FROM gpu_data.dim_token_gpu_tco_dist
WHERE currency != 'KRW';
