-- =============================================================
-- Company/Stage ClickHouse DDL — gpu_data.dim_token_service
-- Target cluster: gpu-monitoring
-- Writer: mart (공유 계정, 구 token_collector — 계정 공유 결정 2026-07-14) — 각 수집 모듈이 자기 source_type 범위만
--         원자 교체 (DELETE WHERE source_type='<유형>' → INSERT,
--         mutations_sync=2 — 스펙 §4.2·§5.9 계약 6조)
-- 주의: gpu_data는 기존(동료 소유) DB — CREATE DATABASE 하지 않음.
--       이슈 #1에서 dim·view의 gpu_data 배치 확정.
-- 네이밍(확정): dim_token_* 접두사 사용 — gpu_data는 공유 DB이므로
--   (1) 기존/향후 GPU 파이프라인 테이블과의 이름 충돌 예방,
--   (2) 테이블명만으로 토큰 파이프라인 소유임을 식별(정리·마이그레이션 시
--       오조작 방지)이 목적. 토큰 파이프라인이 gpu_data에 만드는 모든
--   테이블(dim_token_*, view_token_usage_*)이 같은 규칙을 따른다.
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_service_local
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
    '/clickhouse/tables/{shard}/gpu_data/dim_token_service_local',
    '{replica}'
)
ORDER BY (service)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_service_dist
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
ENGINE = Distributed('gpu-monitoring', 'gpu_data', 'dim_token_service_local', rand());
