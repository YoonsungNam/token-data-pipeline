-- =============================================================
-- user-org 모듈 GRANT 추가분 (스펙 §7.2 계정·GRANT 경계)
-- 실행 주체: admin 수동. 계정 생성은 collectors accounts.sql에서 완료.
--
-- 쓰기 주체 확정 (§6.1 1단계 + §8.3 문구의 해소): dim_token_user_org의
--   INSERT(이력 append)·파기/가명화(ALTER DELETE/UPDATE)는 전부 admin
--   수동 실행 — 전용 쓰기 계정을 두지 않는다. 근거: 1단계 투입은
--   "사내 절차로만 투입·리뷰"(§6.1)이고 빈도가 낮으며, 개인정보 테이블의
--   쓰기 권한을 상시 계정에 부여하지 않는 것이 최소 권한이다.
--   2단계 sync CronJob 도입 시(§9-2) 전용 계정을 그때 신설한다.
-- =============================================================

-- token_mart — STEP 1 이력 조인 읽기 (_dist만 — _local 우회 차단, dim_token_service 선례) (mart accounts.sql 헤더의 Plan 4 귀속 명시분)
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.dim_token_user_org_dist TO token_mart;