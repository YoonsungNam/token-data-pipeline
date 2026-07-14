-- =============================================================
-- model-catalog 모듈 GRANT 추가분 (스펙 §7.2 계정·GRANT 경계)
-- 실행 주체: admin 수동. 계정 생성은 collectors accounts.sql에서 완료.
-- 쓰기 주체: 시드/단가 갱신 SQL은 admin 수동 (§6.2 — 전용 계정 없음).
-- =============================================================

-- token_mart — STEP 1 단가 조인 읽기 (_dist만 — _local 우회 차단) (mart accounts.sql 헤더의 Plan 4 귀속 명시분)
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.dim_token_model_dist  TO token_mart;