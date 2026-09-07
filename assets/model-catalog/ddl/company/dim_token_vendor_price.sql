-- =============================================================
-- Company/Stage ClickHouse DDL — gpu_data.dim_token_vendor_price (설계 2026-08-31 §4.2, P0-stretch — 사외 API 비용, 정의서 3.9)
-- Target cluster: gpu-monitoring
-- Writer: admin 수동 (seed_dim_token_vendor_price.sql 플레이스홀더 + csv_to_layer_c_dim_insert.py --table vendor_price) /
--         Reader: mart (공유 계정 — mart-metrics M4 external_api: allocated_cost_krw = Σ tokens × krw_per_mtok / 1e6)
-- 주의: gpu_data는 기존(동료 소유) DB — DB 생성문 없음.
-- 단가 의미 (§4.2): input = 캐시 생성·읽기를 제외한 순수 입력 단가, cache_creation = 벤더 공표 전체 write 단가
--   (입력 대비 할증분 아님; TTL별 차이는 note + 최고 단가 — 미결 M21). tier는 'standard' 기본(미결 M18).
-- 이력 규약: (provider, model, tier, effective_from) — 변경은 새 effective_from 행 append. KRW 고정.
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_vendor_price_local
ON CLUSTER 'gpu-monitoring'
(
    provider                    LowCardinality(String)                  COMMENT '벤더 (anthropic 등) — M4 provider_service 표기',
    model                       String                                  COMMENT 'canonical 모델명',
    tier                        LowCardinality(String) DEFAULT 'standard' COMMENT 'standard | batch | flex | priority (tier_domain)',
    effective_from              Date                                    COMMENT '단가 적용 시작일 (이력 키)',
    krw_per_mtok_input          Nullable(Float64)                       COMMENT '순수 입력 단가 (KRW per MTok)',
    krw_per_mtok_cached         Nullable(Float64)                       COMMENT '캐시 읽기 단가',
    krw_per_mtok_cache_creation Nullable(Float64)                       COMMENT '캐시 생성(write) 전체 단가',
    krw_per_mtok_output         Nullable(Float64)                       COMMENT '출력 단가',
    note                        String DEFAULT ''                       COMMENT '출처·환율 기준일 (예: 공표가 × 환율 2026-08)'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/gpu_data/dim_token_vendor_price_local',
    '{replica}'
)
ORDER BY (provider, model, tier, effective_from)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_vendor_price_dist
ON CLUSTER 'gpu-monitoring'
(
    provider                    LowCardinality(String),
    model                       String,
    tier                        LowCardinality(String),
    effective_from              Date,
    krw_per_mtok_input          Nullable(Float64),
    krw_per_mtok_cached         Nullable(Float64),
    krw_per_mtok_cache_creation Nullable(Float64),
    krw_per_mtok_output         Nullable(Float64),
    note                        String
)
ENGINE = Distributed('gpu-monitoring', 'gpu_data', 'dim_token_vendor_price_local', cityHash64(model));
