-- E2E 전용: 토큰 레지스트리(gpu_data.dim_token_service) 최소 twin — 단일노드 CH 24.8.
-- 컬럼 7종은 collectors/token-usage/ddl/company/dim_token_service.sql 과 이름·타입 동일(§7.3 "최소 twin").
-- install.sh 프리플라이트가 같은 테이블을 SELECT count() 하므로 e2e 도 같은 질의로 1을 확인한다.
-- run_e2e.sh 가 6a DDL 2파일을 단일노드로 변환한 뒤 이 파일을 그대로(변환 없이) 이어 붙여 실행한다.
CREATE DATABASE IF NOT EXISTS gpu_data;

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_service_local
(
    service_group LowCardinality(String),
    service       LowCardinality(String),
    base_url      String,
    enabled       UInt8,
    source_type   LowCardinality(String),
    note          String DEFAULT '',
    updated_at    DateTime('Asia/Seoul')
)
ENGINE = MergeTree
ORDER BY (service);

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_service_dist
AS gpu_data.dim_token_service_local
ENGINE = Distributed('default', 'gpu_data', 'dim_token_service_local', rand());

INSERT INTO gpu_data.dim_token_service_dist VALUES
    ('Mock Group', 'Mock Service A', 'http://127.0.0.1:18001', 1, 'usage-api-v1', '', now());
