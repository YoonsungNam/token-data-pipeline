-- =============================================================
-- Company/Stage ClickHouse DDL — gpu_data.dim_token_model_alias (설계 2026-08-31 §4.2, P0)
-- Target cluster: gpu-monitoring
-- Writer: admin 수동 (seed_dim_token_model_alias.sql + sheet_to_dim_token_model_alias_insert.py 생성 SQL) /
--         Reader: mart (공유 계정 — mart-metrics M1 canon(x) = if(a.canonical = '', x, a.canonical))
-- 주의: gpu_data는 기존(동료 소유) DB — DB 생성문 없음. dim_token_* 접두사 규칙 적용.
-- 이력 규약 (§4.2): (alias, effective_from) — 재매핑은 새 effective_from 행 append (기존 행 불변).
--   identity 행(canonical→canonical)·unknown→unknown 필수. alias 없는 canonical-only 행도 identity 생성.
-- model_registered 판정: canonical이 dim_token_model_alias에 identity 행(생성기 자동 생성)으로 존재하면 1 — gpu_data.dim_token_model 조회 없음(6c 읽기 계약 3테이블 고정).
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_model_alias_local
ON CLUSTER 'gpu-monitoring'
(
    alias            String                                    COMMENT 'API/수기 원문 모델 문자열 (≤128) — identity 행은 canonical과 동일',
    effective_from   Date                                      COMMENT '매핑 적용 시작일 (이력 키)',
    canonical        String                                    COMMENT '정규화 대상 모델명 — 빈 문자열 금지(empty_canonical)',
    defining_service LowCardinality(String) DEFAULT ''         COMMENT '이 alias를 정의한(보고한) 서비스 — 레지스트리 service와 일치해야 함(service_not_in_registry)',
    source           LowCardinality(String) DEFAULT 'metadata-sheet' COMMENT 'metadata-sheet | manual | seed',
    note             String DEFAULT ''                         COMMENT '시트 비고 원문 (자유 텍스트)'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/gpu_data/dim_token_model_alias_local',
    '{replica}'
)
ORDER BY (alias, effective_from)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_model_alias_dist
ON CLUSTER 'gpu-monitoring'
(
    alias            String,
    effective_from   Date,
    canonical        String,
    defining_service LowCardinality(String),
    source           LowCardinality(String),
    note             String
)
ENGINE = Distributed('gpu-monitoring', 'gpu_data', 'dim_token_model_alias_local', cityHash64(alias));
