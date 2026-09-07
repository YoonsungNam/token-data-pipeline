-- =============================================================
-- collectors/token-metrics GRANT 추가분 (설계 2026-08-31 §4.2 GRANT 표 · 마스터 v1.12 §7.2 계정·GRANT 경계)
-- 실행 주체: admin 수동 (install.sh 자동 적용 대상 아님 — §7.2)
-- 계정: 공유 운영계정 mart (계정 생성·비밀번호는 동료 소유 — 이 파일은 사용자를 만들지 않는다)
-- 원칙: 자기 테이블에 테이블 레벨 GRANT만(DB 레벨 금지). 이미 있는 권한은 no-op.
--   fact/gpu_data DB는 기존 DB — 이 파일은 DB를 만들지 않는다 (6b install.sh 프리플라이트가 존재 확인).
-- 기존 collectors/token-usage/ddl/company/accounts.sql 무수정 — 신규 테이블 몫만 여기에.
-- =============================================================

-- 1) 수집기 몫 --------------------------------------------------
-- 수집 원본 3테이블: 존재 확인 SELECT(앵커) + INSERT(_dist 경유) + 멱등 DELETE(_local만, --replace·크래시 복구)
GRANT SELECT, INSERT ON fact.raw_token_metrics_gpu_1d_dist      TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON fact.raw_token_metrics_gpu_1d_local     TO mart ON CLUSTER 'gpu-monitoring';
GRANT ALTER DELETE   ON fact.raw_token_metrics_gpu_1d_local     TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON fact.raw_token_metrics_serving_1d_dist  TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON fact.raw_token_metrics_serving_1d_local TO mart ON CLUSTER 'gpu-monitoring';
GRANT ALTER DELETE   ON fact.raw_token_metrics_serving_1d_local TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON fact.raw_token_metrics_summary_1d_dist  TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON fact.raw_token_metrics_summary_1d_local TO mart ON CLUSTER 'gpu-monitoring';
GRANT ALTER DELETE   ON fact.raw_token_metrics_summary_1d_local TO mart ON CLUSTER 'gpu-monitoring';
-- 교체 감사(append-only): INSERT만 — DELETE 권한 없음 (감사 불변성)
GRANT SELECT, INSERT ON fact.collect_audit_metrics_1d_dist      TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON fact.collect_audit_metrics_1d_local     TO mart ON CLUSTER 'gpu-monitoring';
-- 메트릭 레지스트리: 정기 실행 diff 동기화 (ALTER DELETE 전체 + INSERT)
GRANT SELECT, INSERT ON gpu_data.dim_token_metrics_service_dist  TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON gpu_data.dim_token_metrics_service_local TO mart ON CLUSTER 'gpu-monitoring';
GRANT ALTER DELETE   ON gpu_data.dim_token_metrics_service_local TO mart ON CLUSTER 'gpu-monitoring';
-- 프리플라이트·M0 (토큰 레지스트리 읽기 전용 — 기존 권한이면 no-op)
GRANT SELECT ON gpu_data.dim_token_service_dist TO mart ON CLUSTER 'gpu-monitoring';
