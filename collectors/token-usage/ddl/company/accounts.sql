-- =============================================================
-- 계정 3종 + 테이블 레벨 GRANT (스펙 v1.6 §7.2 계정·GRANT 경계)
-- 실행 주체: admin 수동 (install.sh 자동 적용 대상 아님 — §7.2)
-- 실행 전 CHANGE_ME_* 를 실제 비밀번호로 치환
--
-- 원칙 (§7.2):
--  - 계정명 token_ 접두사 — 동료의 collector/mart 계정과 분리
--    (CREATE USER IF NOT EXISTS는 이름 충돌 시 조용히 공유되므로)
--  - GRANT는 전부 자기 테이블에 **테이블 레벨** — DB 레벨 금지
--  - 신규 테이블 추가 시 GRANT 추가는 migrate_add_*.sql 절차의 일부
-- =============================================================

-- 1) 수집기 계정 -----------------------------------------------
CREATE USER IF NOT EXISTS token_collector
ON CLUSTER 'gpu-monitoring'
IDENTIFIED WITH sha256_password BY 'CHANGE_ME_COLLECTOR';

-- 수집 원본: 존재 확인 SELECT(§4.0 no-op 스킵) + INSERT + 멱등 DELETE(local만)
GRANT SELECT, INSERT ON token_fact.raw_token_usage_1d_dist          TO token_collector ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON token_fact.raw_token_usage_1d_local         TO token_collector ON CLUSTER 'gpu-monitoring';
GRANT ALTER DELETE   ON token_fact.raw_token_usage_1d_local         TO token_collector ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON token_fact.raw_token_usage_summary_1d_dist  TO token_collector ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON token_fact.raw_token_usage_summary_1d_local TO token_collector ON CLUSTER 'gpu-monitoring';
GRANT ALTER DELETE   ON token_fact.raw_token_usage_summary_1d_local TO token_collector ON CLUSTER 'gpu-monitoring';
-- 교체 감사(append-only): INSERT만 — DELETE 권한 없음 (감사 불변성)
GRANT SELECT, INSERT ON token_fact.collect_audit_1d_dist            TO token_collector ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON token_fact.collect_audit_1d_local           TO token_collector ON CLUSTER 'gpu-monitoring';
-- 서비스 레지스트리: 자기 source_type 범위 교체 (§5.9 계약 6조)
GRANT SELECT, INSERT ON gpu_data.dim_service_dist                   TO token_collector ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON gpu_data.dim_service_local                  TO token_collector ON CLUSTER 'gpu-monitoring';
GRANT ALTER DELETE   ON gpu_data.dim_service_local                  TO token_collector ON CLUSTER 'gpu-monitoring';
-- mutations_sync=2 방식이므로 system.mutations 권한 불요 (§7.2)

-- 2) mart 배치 계정 --------------------------------------------
-- (mart·view 테이블 GRANT는 Plan 3 DDL에서 추가 — 여기는 읽기 원천만)
CREATE USER IF NOT EXISTS token_mart
ON CLUSTER 'gpu-monitoring'
IDENTIFIED WITH sha256_password BY 'CHANGE_ME_MART';

GRANT SELECT ON token_fact.raw_token_usage_1d_dist         TO token_mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT ON token_fact.raw_token_usage_summary_1d_dist TO token_mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT ON token_fact.collect_audit_1d_dist           TO token_mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT ON gpu_data.dim_service_dist                  TO token_mart ON CLUSTER 'gpu-monitoring';
-- clusterAllReplicas 폴링(wait_for_mutations) + GLOBAL JOIN에 필요 (§7.2, 동료 mart 관례와 동일)
GRANT SELECT ON system.mutations TO token_mart ON CLUSTER 'gpu-monitoring';
GRANT CREATE TEMPORARY TABLE ON *.* TO token_mart ON CLUSTER 'gpu-monitoring';

-- 3) 대시보드 읽기 전용 계정 ------------------------------------
-- 실효 GRANT는 view 테이블 생성 시(Plan 3) 부여: SELECT ON gpu_data.view_token_usage_* 한정
-- (fact·mart·dim 직접 조회 차단 — per-user 데이터 접근 통제, §7.2)
CREATE USER IF NOT EXISTS token_dashboard_reader
ON CLUSTER 'gpu-monitoring'
IDENTIFIED WITH sha256_password BY 'CHANGE_ME_READER';
