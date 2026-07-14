-- =============================================================
-- mart 모듈 GRANT 추가분 (스펙 §7.2 계정·GRANT 경계)
-- 실행 주체: admin 수동 (install.sh 자동 적용 대상 아님 — §7.2)
--
-- 계정 생성은 collectors/token-usage/ddl/company/accounts.sql에서 완료
-- (token_mart / token_dashboard_reader) — 이 파일은 Plan 3 신규 테이블에
-- 대한 GRANT만 추가한다 (migrate_add_* 관행의 mart 초기분).
-- 원칙 (§7.2):
--  - 자기 테이블에 테이블 레벨 GRANT만 — DB 레벨 금지
--  - INSERT는 _dist 경유만 (fact와의 co-location 라우팅 일관성 — §4.0);
--    _local에는 멱등 DELETE용 ALTER DELETE만
--  - GRANT 문은 정규형 `GRANT ON CLUSTER ... <priv> ON <table> TO <user>`
-- 주의: STEP 1이 조인하는 gpu_data.dim_token_user_org / dim_token_model의 SELECT GRANT는
--   해당 테이블을 만드는 assets DDL(Plan 4)의 accounts에 귀속 —
--   mart 가동 전 Plan 4 적용이 전제다 (ddl/README.md 적용 순서 참조).
-- =============================================================

-- 1) token_mart — mart 자기 테이블 (STEP 1) ---------------------

GRANT ON CLUSTER 'gpu-monitoring' SELECT, INSERT ON mart.token_usage_1d_dist        TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' ALTER DELETE   ON mart.token_usage_1d_local       TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT, INSERT ON mart.agg_token_service_1d_dist  TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' ALTER DELETE   ON mart.agg_token_service_1d_local TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT, INSERT ON mart.agg_token_org_1d_dist      TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' ALTER DELETE   ON mart.agg_token_org_1d_local     TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT, INSERT ON mart.agg_token_model_1d_dist    TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' ALTER DELETE   ON mart.agg_token_model_1d_local   TO token_mart;

-- 2) token_mart — view 테이블 (STEP 2) --------------------------

GRANT ON CLUSTER 'gpu-monitoring' SELECT, INSERT ON gpu_data.view_token_usage_1d_dist          TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' ALTER DELETE   ON gpu_data.view_token_usage_1d_local         TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT, INSERT ON gpu_data.view_token_usage_service_1d_dist  TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' ALTER DELETE   ON gpu_data.view_token_usage_service_1d_local TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT, INSERT ON gpu_data.view_token_usage_org_1d_dist      TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' ALTER DELETE   ON gpu_data.view_token_usage_org_1d_local     TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT, INSERT ON gpu_data.view_token_usage_model_1d_dist    TO token_mart;
GRANT ON CLUSTER 'gpu-monitoring' ALTER DELETE   ON gpu_data.view_token_usage_model_1d_local   TO token_mart;

-- 3) token_dashboard_reader — view 읽기 전용 --------------------
--    _dist만 GRANT: fact·mart·dim 직접 조회 차단 + _local 우회 차단 (§7.2).
--    주의: 이것은 "테이블 경계" 통제다 — per-user 상세(view_token_usage_1d)의
--    노출 grain·조직 부착 여부는 §9-1/§9-3 미결 (ROW POLICY/계정 분리로
--    별도 확정 — 이 GRANT가 per-user 접근 통제를 대신하지 않는다)

GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.view_token_usage_1d_dist         TO token_dashboard_reader;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.view_token_usage_service_1d_dist TO token_dashboard_reader;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.view_token_usage_org_1d_dist     TO token_dashboard_reader;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.view_token_usage_model_1d_dist   TO token_dashboard_reader;

-- 4) token_mart 서버측 설정 — 멱등 rerun 보호 ------------------
--    DELETE→동일 데이터 재INSERT가 ReplicatedMergeTree 블록 중복제거에
--    걸려 조용히 폐기되는 것을 서버측에서도 차단 (클라이언트 설정
--    insert_deduplicate=0의 Distributed 전파 불완전 사례 대비)
ALTER USER token_mart SETTINGS insert_deduplicate = 0;
