-- =============================================================
-- model-catalog 모듈 GRANT 추가분 (스펙 v1.12 §7.2 계정·GRANT 경계)
-- 실행 주체: admin 수동. 계정은 동료 소유의 기존 운영계정 `mart`를 공유
--   한다(전용 계정 폐지, 계정 공유 합의 2026-07-14 — 이슈 #1). 이 파일은
--   CREATE USER를 하지 않는다.
-- 쓰기 주체: 시드/단가 갱신 SQL은 admin 수동 (§6.2 — 전용 계정 없음).
-- =============================================================

-- mart(공유 계정) — STEP 1 단가 조인 읽기 (_dist만 — _local 우회 차단) (mart accounts.sql 헤더의 Plan 4 귀속 명시분)
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.dim_token_model_dist  TO mart;