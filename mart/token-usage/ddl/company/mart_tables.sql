-- =============================================================
-- Company/Stage ClickHouse DDL — mart 1차 집계 4테이블 (스펙 §4.3)
-- Target cluster: gpu-monitoring (company 2s×2r / stage 1s×1r)
-- Writer: mart (공유 계정, 구 token_mart — STEP 1, §7.1)
-- 주의: mart DB는 동료 소유 공유 DB(기본안 — ddl/README.md 협의 지점 1) —
--       CREATE DATABASE 하지 않음. 테이블 DDL만 install.sh 자동 적용.
-- 공유 쓰기 계약: created_by는 DEFAULT 없음 — 모든 작성자가 INSERT 시
--   명시 삽입(본 파이프라인 'token-pipeline' 고정), CHECK로 생략을
--   조기 검출 (§4.2 리뷰 #22)
-- =============================================================

-- -------------------------------------------------------------
-- 1) mart.token_usage_1d — 사용자×모델 일별 상세 + 조직/비용 부착
--    Grain: date × service × user_id × user_type × model
--    fact.raw_token_usage_1d와 co-location (동일 파티션/ORDER BY/샤딩키)
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.token_usage_1d_local
ON CLUSTER 'gpu-monitoring'
(
    date                  Date                   COMMENT '사용량 발생 일자 (KST)',
    service_group         LowCardinality(String) COMMENT '정본 = endpoints.yaml (§5.0)',
    service               LowCardinality(String) COMMENT '정본 = endpoints.yaml (§5.0)',
    user_id               String                 COMMENT 'unclassified는 빈 문자열 (§5.4)',
    user_type             LowCardinality(String) COMMENT 'identified | anonymous | unclassified',
    model                 LowCardinality(String) COMMENT 'unknown 허용',
    org_path              Array(String)          COMMENT '최상위→말단 가변 깊이 — 미매핑은 [''unknown''] (§6.1)',
    org_top               LowCardinality(String) COMMENT '편의 파생 = org_path[1]',
    org_leaf              LowCardinality(String) COMMENT '편의 파생 = org_path 말단',
    input_tokens          UInt64                 COMMENT '순수 input (cache 제외)',
    cache_read_tokens     UInt64,
    cache_creation_tokens UInt64,
    output_tokens         UInt64,
    total_input_tokens    UInt64                 COMMENT '= input + cache_read + cache_creation (§4.3)',
    requests              UInt64,
    cost                  Nullable(Float64)      COMMENT 'Σ(단가×양)/1e6, date 기준 유효 단가 — dim_token_model 미등록은 NULL (§4.3)',
    created_by            LowCardinality(String) COMMENT '공유 쓰기 계약 — 본 파이프라인은 token-pipeline 고정',
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/mart/token_usage_1d_local',
    '{replica}'
)
PARTITION BY toYYYYMMDD(date)
ORDER BY (date, service, user_type, user_id, model)
TTL date + INTERVAL 25 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mart.token_usage_1d_dist
ON CLUSTER 'gpu-monitoring'
(
    date                  Date,
    service_group         LowCardinality(String),
    service               LowCardinality(String),
    user_id               String,
    user_type             LowCardinality(String),
    model                 LowCardinality(String),
    org_path              Array(String),
    org_top               LowCardinality(String),
    org_leaf              LowCardinality(String),
    input_tokens          UInt64,
    cache_read_tokens     UInt64,
    cache_creation_tokens UInt64,
    output_tokens         UInt64,
    total_input_tokens    UInt64,
    requests              UInt64,
    cost                  Nullable(Float64),
    created_by            LowCardinality(String),
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = Distributed('gpu-monitoring', 'mart', 'token_usage_1d_local',
                     cityHash64(service, user_id));

-- -------------------------------------------------------------
-- 2) mart.agg_token_service_1d — 서비스 일별 집계 + 보고값 대사
--    Grain: date × service_group × service
--    reported_*는 fact.raw_token_usage_summary_1d 조인값 —
--    diff_* = Σdetail − reported, is_derived=1이면 NULL (§4.1 파생 시맨틱)
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.agg_token_service_1d_local
ON CLUSTER 'gpu-monitoring'
(
    date                               Date,
    service_group                      LowCardinality(String),
    service                            LowCardinality(String),
    input_tokens                       UInt64,
    cache_read_tokens                  UInt64,
    cache_creation_tokens              UInt64,
    output_tokens                      UInt64,
    total_input_tokens                 UInt64,
    requests                           UInt64,
    distinct_users                     UInt64            COMMENT 'detail uniqExact, user_id != '''' (§4.3)',
    cost                               Nullable(Float64),
    is_derived                         UInt8             COMMENT 'summary가 detail 합산 파생이면 1 (§4.1)',
    reported_input_tokens              Nullable(UInt64)            COMMENT '서비스 보고값 (summary 조인)',
    reported_cache_read_tokens         Nullable(UInt64),
    reported_cache_creation_tokens     Nullable(UInt64),
    reported_output_tokens             Nullable(UInt64),
    reported_requests                  Nullable(UInt64),
    reported_distinct_users            Nullable(UInt32)            COMMENT '비가산 — 교차 합산 금지',
    reported_distinct_identified_users Nullable(UInt32),
    diff_input_tokens                  Nullable(Int64)   COMMENT 'Σdetail − reported. is_derived=1이면 NULL',
    diff_cache_read_tokens             Nullable(Int64),
    diff_cache_creation_tokens         Nullable(Int64),
    diff_output_tokens                 Nullable(Int64),
    diff_requests                      Nullable(Int64),
    created_by                         LowCardinality(String),
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/mart/agg_token_service_1d_local',
    '{replica}'
)
PARTITION BY toYYYYMM(date)
ORDER BY (date, service)
TTL date + INTERVAL 25 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mart.agg_token_service_1d_dist
ON CLUSTER 'gpu-monitoring'
(
    date                               Date,
    service_group                      LowCardinality(String),
    service                            LowCardinality(String),
    input_tokens                       UInt64,
    cache_read_tokens                  UInt64,
    cache_creation_tokens              UInt64,
    output_tokens                      UInt64,
    total_input_tokens                 UInt64,
    requests                           UInt64,
    distinct_users                     UInt64,
    cost                               Nullable(Float64),
    is_derived                         UInt8,
    reported_input_tokens              Nullable(UInt64),
    reported_cache_read_tokens         Nullable(UInt64),
    reported_cache_creation_tokens     Nullable(UInt64),
    reported_output_tokens             Nullable(UInt64),
    reported_requests                  Nullable(UInt64),
    reported_distinct_users            Nullable(UInt32),
    reported_distinct_identified_users Nullable(UInt32),
    diff_input_tokens                  Nullable(Int64),
    diff_cache_read_tokens             Nullable(Int64),
    diff_cache_creation_tokens         Nullable(Int64),
    diff_output_tokens                 Nullable(Int64),
    diff_requests                      Nullable(Int64),
    created_by                         LowCardinality(String),
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = Distributed('gpu-monitoring', 'mart', 'agg_token_service_1d_local',
                     cityHash64(service));

-- -------------------------------------------------------------
-- 3) mart.agg_token_org_1d — 조직 일별 집계 (말단 경로 단위)
--    Grain: date × org_path — 상위 롤업은 쿼리 시 arraySlice GROUP BY,
--    서브트리 질의 표준 = prefix 비교 (§4.3)
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.agg_token_org_1d_local
ON CLUSTER 'gpu-monitoring'
(
    date                  Date,
    org_path              Array(String)          COMMENT '말단 경로 — 미매핑 버킷은 [''unknown'']',
    input_tokens          UInt64,
    cache_read_tokens     UInt64,
    cache_creation_tokens UInt64,
    output_tokens         UInt64,
    total_input_tokens    UInt64,
    requests              UInt64,
    distinct_users        UInt64                 COMMENT 'detail uniqExact, user_id != ''''',
    headcount             UInt32                 COMMENT '로스터 정원(해당 경로 소속) — dim_token_user_org 부재 시 0',
    adoption_rate         Nullable(Float64)      COMMENT 'distinct_users / headcount — headcount 0이면 NULL',
    cost                  Nullable(Float64),
    created_by            LowCardinality(String),
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/mart/agg_token_org_1d_local',
    '{replica}'
)
PARTITION BY toYYYYMM(date)
ORDER BY (date, org_path)
TTL date + INTERVAL 25 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mart.agg_token_org_1d_dist
ON CLUSTER 'gpu-monitoring'
(
    date                  Date,
    org_path              Array(String),
    input_tokens          UInt64,
    cache_read_tokens     UInt64,
    cache_creation_tokens UInt64,
    output_tokens         UInt64,
    total_input_tokens    UInt64,
    requests              UInt64,
    distinct_users        UInt64,
    headcount             UInt32,
    adoption_rate         Nullable(Float64),
    cost                  Nullable(Float64),
    created_by            LowCardinality(String),
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = Distributed('gpu-monitoring', 'mart', 'agg_token_org_1d_local',
                     cityHash64(arrayStringConcat(org_path, '>')));

-- -------------------------------------------------------------
-- 4) mart.agg_token_model_1d — 모델 일별 집계
--    Grain: date × model × provider (§4.3)
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.agg_token_model_1d_local
ON CLUSTER 'gpu-monitoring'
(
    date                  Date,
    model                 LowCardinality(String) COMMENT 'unknown 포함',
    provider              LowCardinality(String) COMMENT 'dim_token_model 조인 — 미등록은 빈 문자열',
    input_tokens          UInt64,
    cache_read_tokens     UInt64,
    cache_creation_tokens UInt64,
    output_tokens         UInt64,
    total_input_tokens    UInt64,
    requests              UInt64,
    distinct_services     UInt32                 COMMENT '해당 모델을 쓴 서비스 수',
    cost                  Nullable(Float64)      COMMENT 'dim_token_model 미등록 모델은 NULL',
    created_by            LowCardinality(String),
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/mart/agg_token_model_1d_local',
    '{replica}'
)
PARTITION BY toYYYYMM(date)
ORDER BY (date, model, provider)
TTL date + INTERVAL 25 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mart.agg_token_model_1d_dist
ON CLUSTER 'gpu-monitoring'
(
    date                  Date,
    model                 LowCardinality(String),
    provider              LowCardinality(String),
    input_tokens          UInt64,
    cache_read_tokens     UInt64,
    cache_creation_tokens UInt64,
    output_tokens         UInt64,
    total_input_tokens    UInt64,
    requests              UInt64,
    distinct_services     UInt32,
    cost                  Nullable(Float64),
    created_by            LowCardinality(String),
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = Distributed('gpu-monitoring', 'mart', 'agg_token_model_1d_local',
                     cityHash64(model));
