-- =============================================================
-- Company/Stage ClickHouse DDL — gpu_data.dim_token_model
-- Target cluster: gpu-monitoring
-- Writer: admin 수동 (시드 SQL — seed_dim_token_model.sql, §6.2) /
--         Reader: mart (공유 계정, 계정 공유 결정 2026-07-14 — STEP 1 단가 조인)
-- 주의: gpu_data는 기존(동료 소유) DB — CREATE DATABASE 하지 않음.
-- 네이밍: dim_token_* 접두사 규칙 적용 (dim_token_service.sql 헤더 참조).
-- 이력 규약 (§4.2·§6.2): (model, effective_from) — 단가 변경은 새
--   effective_from 행 추가 (기존 행 불변 — 소급 정정도 이력 정정 후
--   해당 기간 mart rerun, §4.3). 단가는 Nullable — 미등록/unknown은
--   NULL로 cost 산식에 자연 전파 ($0 위장 금지, 리뷰 #15).
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_model_local
ON CLUSTER 'gpu-monitoring'
(
    model                       String                 COMMENT 'token-usage-api의 model 문자열 정본 — unknown 포함',
    effective_from              Date                   COMMENT '단가 적용 시작일 (이력 키)',
    provider                    LowCardinality(String) COMMENT '모델 제공자 (anthropic 등)',
    serving_type                LowCardinality(String) COMMENT 'internal | external — §4.4 Layer C 대상 판별',
    input_usd_per_mtok          Nullable(Float64)      COMMENT 'USD per MTok — 미등록·unknown은 NULL (cost NULL 전파)',
    cache_read_usd_per_mtok     Nullable(Float64),
    cache_creation_usd_per_mtok Nullable(Float64),
    output_usd_per_mtok         Nullable(Float64),
    currency                    LowCardinality(String) COMMENT 'USD 고정 (§9-5 미결 상속 — 참고 지표)',
    note                        String                 DEFAULT ''
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/gpu_data/dim_token_model_local',
    '{replica}'
)
ORDER BY (model, effective_from)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_model_dist
ON CLUSTER 'gpu-monitoring'
(
    model                       String,
    effective_from              Date,
    provider                    LowCardinality(String),
    serving_type                LowCardinality(String),
    input_usd_per_mtok          Nullable(Float64),
    cache_read_usd_per_mtok     Nullable(Float64),
    cache_creation_usd_per_mtok Nullable(Float64),
    output_usd_per_mtok         Nullable(Float64),
    currency                    LowCardinality(String),
    note                        String
)
ENGINE = Distributed('gpu-monitoring', 'gpu_data', 'dim_token_model_local', cityHash64(model));
