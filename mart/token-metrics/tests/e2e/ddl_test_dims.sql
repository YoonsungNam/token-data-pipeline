-- =============================================================
-- [E2E 전용] 기준정보 dim 4종의 단일노드 대역 — gpu_data.dim_token_{model_alias,gpu_tco,gpu_allocation,vendor_price}_dist
-- 이름만 _dist(steps.py가 {DB_DIM}.<table>_dist를 읽음), 엔진은 MergeTree(단일노드).
-- 컬럼·ORDER BY = assets/model-catalog/ddl/company/dim_token_*.sql (Plan 6a D 표).
-- 시드 = assets/model-catalog/fixtures/stage_seed_dim_token_*.sql 값 재현(디폴트 컬럼 포함 전부 명시).
--   델타 1: TCO의 H100 2026-08-26 4300 이력 행은 넣지 않는다 — 2026-09-03 유효 TCO를 4200으로 고정해
--           C(Qwen3-32B) = (40+8)×4200 = 201,600 검산을 유지(이력 argMax 경로는 T3 단위 테스트가 검증).
--   델타 2: alias에 e2e 전용 identity 1행(Qwen3-32B) 추가 — unregistered_model WARN 0건 유지.
-- 주의: run_e2e.sh가 세미콜론으로 문장을 나눈다 — 주석·문자열에 세미콜론 금지(이 주석 포함).
-- 이 파일의 파이썬 재현(TCO_KRW/ALLOCATION/ALIASES/VENDOR_PRICE)은 tests/e2e/seed_metrics.py 상단 상수 —
-- tests/test_e2e_seed.py가 두 정본을 교차 대조한다(값을 고치면 둘 다 고친다).
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_model_alias_dist
(
    alias            String,
    effective_from   Date,
    canonical        String,
    defining_service LowCardinality(String) DEFAULT '',
    source           LowCardinality(String) DEFAULT 'metadata-sheet',
    note             String DEFAULT ''
)
ENGINE = MergeTree
ORDER BY (alias, effective_from);

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_gpu_tco_dist
(
    gpu_type             String,
    effective_from       Date,
    tco_krw_per_gpu_hour Nullable(Float64),
    currency             LowCardinality(String) DEFAULT 'KRW',
    basis                LowCardinality(String) DEFAULT '',
    note                 String DEFAULT ''
)
ENGINE = MergeTree
ORDER BY (gpu_type, effective_from);

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_gpu_allocation_dist
(
    service_group       LowCardinality(String),
    gpu_type            String,
    effective_from      Date,
    allocated_gpu_count Nullable(Float64),
    source              LowCardinality(String) DEFAULT 'manual',
    note                String DEFAULT ''
)
ENGINE = MergeTree
ORDER BY (service_group, gpu_type, effective_from);

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_vendor_price_dist
(
    provider                   LowCardinality(String),
    model                      String,
    tier                       LowCardinality(String) DEFAULT 'standard',
    effective_from             Date,
    krw_per_mtok_input         Nullable(Float64),
    krw_per_mtok_cached        Nullable(Float64),
    krw_per_mtok_cache_creation Nullable(Float64),
    krw_per_mtok_output        Nullable(Float64),
    note                       String DEFAULT ''
)
ENGINE = MergeTree
ORDER BY (provider, model, tier, effective_from);

-- alias: fixture 6행 + e2e identity 1행 = 7행
INSERT INTO gpu_data.dim_token_model_alias_dist (alias, effective_from, canonical, defining_service, source, note) VALUES
('unknown', '2026-01-01', 'unknown', '', 'seed', 'synthetic identity'),
('claude-opus-4-8', '2026-01-01', 'claude-opus-4-8', '', 'seed', 'synthetic identity'),
('claude-sonnet-5', '2026-01-01', 'claude-sonnet-5', '', 'seed', 'synthetic identity'),
('claude-haiku-4-5', '2026-01-01', 'claude-haiku-4-5', '', 'seed', 'synthetic identity'),
('claude-sonnet-5-20260101', '2026-01-01', 'claude-sonnet-5', 'Mock Service A', 'seed', 'synthetic dated alias'),
('opus-4.8', '2026-01-01', 'claude-opus-4-8', 'Mock Service B', 'seed', 'synthetic short alias'),
('Qwen3-32B', '2026-01-01', 'Qwen3-32B', 'Mock Service A', 'seed', 'e2e');

-- tco: fixture 중 2026-01-01 행 5개(B200 부재 = gpu_type_no_tco 경로)
INSERT INTO gpu_data.dim_token_gpu_tco_dist (gpu_type, effective_from, tco_krw_per_gpu_hour, currency, basis, note) VALUES
('unknown', '2026-01-01', NULL, 'KRW', '', 'synthetic placeholder'),
('H100', '2026-01-01', 4200.0, 'KRW', 'tco', 'synthetic stage value'),
('A100', '2026-01-01', 2100.0, 'KRW', 'tco', 'synthetic stage value'),
('H200', '2026-01-01', 5300.0, 'KRW', 'tco', 'synthetic stage value'),
('L40S', '2026-01-01', 1300.0, 'KRW', 'tco', 'synthetic stage value');

-- allocation: fixture 3행 그대로(Mock Group H100 8 / A100 4)
INSERT INTO gpu_data.dim_token_gpu_allocation_dist (service_group, gpu_type, effective_from, allocated_gpu_count, source, note) VALUES
('unknown', 'unknown', '2026-01-01', NULL, 'seed', 'synthetic placeholder'),
('Mock Group', 'H100', '2026-01-01', 8.0, 'seed', 'synthetic stage value'),
('Mock Group', 'A100', '2026-01-01', 4.0, 'seed', 'synthetic stage value');

-- vendor_price: fixture 4행 그대로(unknown 플레이스홀더 + anthropic 3)
INSERT INTO gpu_data.dim_token_vendor_price_dist (provider, model, tier, effective_from, krw_per_mtok_input, krw_per_mtok_cached, krw_per_mtok_cache_creation, krw_per_mtok_output, note) VALUES
('unknown', 'unknown', 'standard', '2026-01-01', NULL, NULL, NULL, NULL, 'synthetic placeholder'),
('anthropic', 'claude-opus-4-8', 'standard', '2026-01-01', 6750.0, 675.0, 8437.5, 33750.0, 'synthetic USD x 1350'),
('anthropic', 'claude-sonnet-5', 'standard', '2026-01-01', 4050.0, 405.0, 5062.5, 20250.0, 'synthetic USD x 1350'),
('anthropic', 'claude-haiku-4-5', 'standard', '2026-01-01', 1350.0, 135.0, 1687.5, 6750.0, 'synthetic USD x 1350');
