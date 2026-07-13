-- =============================================================
-- Company/Stage ClickHouse DDL — gpu_data.dim_service
-- Target cluster: gpu-monitoring
-- Writer: token_collector — 각 수집 모듈이 자기 source_type 범위만
--         원자 교체 (DELETE WHERE source_type='<유형>' → INSERT,
--         mutations_sync=2 — 스펙 §4.2·§5.9 계약 6조)
-- 주의: gpu_data는 기존(동료 소유) DB — CREATE DATABASE 하지 않음.
--       이슈 #1에서 dim·view의 gpu_data 배치 확정.
-- 협의 지점: 테이블명 접두사(dim_token_service?) — ddl/README.md
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_service_local
ON CLUSTER 'gpu-monitoring'
(
    service_group LowCardinality(String) COMMENT '과제명 (endpoints.yaml 정본)',
    service       LowCardinality(String) COMMENT '서비스 식별자 (endpoints.yaml 정본)',
    base_url      String                 COMMENT '수집 대상 base URL',
    enabled       UInt8                  COMMENT '0 = 수집 제외 (폐기 서비스도 행 유지 — §4.2)',
    source_type   LowCardinality(String) DEFAULT 'usage-api-v1' COMMENT '수집 경로 유형 (§5.9)',
    note          String                 DEFAULT '' COMMENT '운영 메모',
    updated_at    DateTime('Asia/Seoul') COMMENT '레지스트리 교체 시각'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/gpu_data/dim_service_local',
    '{replica}'
)
ORDER BY (service)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS gpu_data.dim_service_dist
ON CLUSTER 'gpu-monitoring'
(
    service_group LowCardinality(String),
    service       LowCardinality(String),
    base_url      String,
    enabled       UInt8,
    source_type   LowCardinality(String),
    note          String,
    updated_at    DateTime('Asia/Seoul')
)
ENGINE = Distributed('gpu-monitoring', 'gpu_data', 'dim_service_local', rand());
