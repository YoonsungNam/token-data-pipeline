-- =============================================================
-- Company/Stage ClickHouse DDL — gpu_data.dim_token_user_org
-- Target cluster: gpu-monitoring
-- Writer: admin 수동 (1단계 — csv_to_dim_user_org_insert.py가 생성한
--         INSERT SQL을 사내 절차로 투입·리뷰 후 실행, §6.1) /
--         Reader: mart (공유 계정, 계정 공유 결정 2026-07-14 — STEP 1 이력 조인)
-- 주의: gpu_data는 기존(동료 소유) DB — CREATE DATABASE 하지 않음.
-- 네이밍: dim_token_* 접두사 규칙 적용 (dim_token_service.sql 헤더의
--   확정 규칙 — "토큰 파이프라인이 gpu_data에 만드는 모든 테이블").
--   스펙 §4.2 표기는 v1.11에서 정리 완료.
-- 이력 규약 (§4.2·§6.1): (user_id, effective_from) 이력 append —
--   조직 이동/퇴사는 새 effective_from 행 추가, 기존 행 불변
--   (파기·가명화는 §6.1 보존 규칙의 예외 경로). mart STEP 1은
--   date 기준 유효 행(effective_from <= date 최신, argMax)을 조인.
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_user_org_local
ON CLUSTER 'gpu-monitoring'
(
    user_id        String                 COMMENT '사내 id (fact.user_id와 동일 체계)',
    effective_from Date                   COMMENT '이력 키 — 조직 이동/퇴사 시 신규 행 추가 (기존 행 불변)',
    user_name      String                 COMMENT '식별 사용자는 실명, anonymous 매핑 행은 비실명 핸들명만 허용(실명 기입 금지 — 투입 리뷰 확인, §6.1, 2026-07-14 개정)',
    org_path       Array(String)          COMMENT '최상위→말단 가변 깊이 — 미매핑 귀속은 조인측(mart)이 [''unknown''] 처리 (§6.1)',
    org_depth      UInt8                  COMMENT '= length(org_path)',
    is_active      UInt8                  COMMENT '0 = 퇴사/비활성 — headcount 산정 제외 (§4.3)',
    updated_at     DateTime('Asia/Seoul') COMMENT '행 투입 시각'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/gpu_data/dim_token_user_org_local',
    '{replica}'
)
ORDER BY (user_id, effective_from)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_user_org_dist
ON CLUSTER 'gpu-monitoring'
(
    user_id        String,
    effective_from Date,
    user_name      String,
    org_path       Array(String),
    org_depth      UInt8,
    is_active      UInt8,
    updated_at     DateTime('Asia/Seoul')
)
ENGINE = Distributed('gpu-monitoring', 'gpu_data', 'dim_token_user_org_local', cityHash64(user_id));
