-- =============================================================
-- Company/Stage ClickHouse DDL — gpu_data.dim_token_gpu_allocation (설계 2026-08-31 §4.2, P0-stretch — Layer C)
-- Target cluster: gpu-monitoring
-- Writer: admin 수동 (seed_dim_token_gpu_allocation.sql 플레이스홀더 + csv_to_layer_c_dim_insert.py --table gpu_allocation) /
--         Reader: mart (공유 계정 — mart-metrics M2 allocated_gpu_hours = allocated_gpu_count × 24)
-- 주의: gpu_data는 기존(동료 소유) DB — DB 생성문 없음.
-- 할당표는 serviceGroup 단위 (정의서 2.3 — 쿼터 보유 단위). 단위는 장수(count).
-- 이력 규약 (§4.2): (service_group, gpu_type, effective_from) — 변경은 새 effective_from 행 append; 철회는 0 행 append.
-- 플레이스홀더는 gpu_type='unknown' (M2 무시 — HAVING gpu_type != 'unknown', no_allocation으로만 노출).
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_gpu_allocation_local
ON CLUSTER 'gpu-monitoring'
(
    service_group       LowCardinality(String)                COMMENT '정본 = endpoints.yaml serviceGroup (바이트 동일)',
    gpu_type            String                                COMMENT 'TCO표 키 — 플레이스홀더 unknown',
    effective_from      Date                                  COMMENT '할당 적용 시작일 00:00 KST (이력 키)',
    allocated_gpu_count Nullable(Float64)                     COMMENT '할당 장수(분수 허용) — 플레이스홀더는 NULL, 철회는 0',
    source              LowCardinality(String) DEFAULT 'manual' COMMENT 'manual | quota-sheet | seed',
    note                String DEFAULT ''                     COMMENT '출처·기준 (자유 텍스트)'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/gpu_data/dim_token_gpu_allocation_local',
    '{replica}'
)
ORDER BY (service_group, gpu_type, effective_from)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_gpu_allocation_dist
ON CLUSTER 'gpu-monitoring'
(
    service_group       LowCardinality(String),
    gpu_type            String,
    effective_from      Date,
    allocated_gpu_count Nullable(Float64),
    source              LowCardinality(String),
    note                String
)
ENGINE = Distributed('gpu-monitoring', 'gpu_data', 'dim_token_gpu_allocation_local', cityHash64(service_group));
