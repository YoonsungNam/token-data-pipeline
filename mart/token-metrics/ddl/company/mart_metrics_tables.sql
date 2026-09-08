-- =============================================================
-- Company/Stage ClickHouse DDL — mart-metrics 4테이블 (설계 2026-08-31 §4.0 매니페스트 · §6.1 M1/M3/M4/M2)
-- Target cluster: gpu-monitoring (company 2s×2r / stage 1s×1r)
-- Writer: mart (공유 계정 — mart/token-metrics CronJob token-mart-metrics 10:20 KST)
-- 주의: mart DB는 기존(공유) DB — DB 생성문 없음. 테이블 DDL만 install.sh 자동 적용.
-- 공유 쓰기 계약: created_by는 DEFAULT 없음 — 본 파이프라인은 'token-metrics-pipeline' 고정
--   (기존 'token-pipeline'과 구분; 불변식 created_by_wrong_metrics가 검사), CHECK로 생략 조기 검출.
-- 비용 정본 = docs/cost-model-spec.md (설계 §6.4 매핑). 통화 KRW 고정.
-- 뮤테이션: 정기 10:20 실행은 날짜당 ≤4(존재 시 DELETE — 첫 적재일 0), rerun은 날짜당 ≤4
--   (MART_METRICS_MAX_MUTATIONS_PER_RUN=64 = 4×16, --chunk-days 7).
-- 행 규칙: 메트릭 fact가 없는 날은 토큰-only 행 + NULL + WARN (절대 FAILURE 아님).
-- =============================================================

-- -------------------------------------------------------------
-- 1) mart.agg_token_model_cost_1d — M1 모델 비용 C (grain: date × service × model(canon))
--    keys = tok_agg ∪ gpu_agg (UNION DISTINCT) 구동, 양쪽 GLOBAL LEFT JOIN
--    model_cost_krw = Σ (serving+standby, 비FAIL) gpu_hours × TCO — 기종 하나라도 TCO NULL이면 NULL
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.agg_token_model_cost_1d_local
ON CLUSTER 'gpu-monitoring'
(
    date                  Date                    COMMENT 'KST 집계일',
    service_group         LowCardinality(String)  COMMENT '레지스트리(메트릭) 우선, 없으면 토큰 mart 값',
    service               LowCardinality(String),
    model                 LowCardinality(String)  COMMENT 'canonical — dim_token_model_alias 적용 후 (미등록은 원문 그대로)',
    serving_gpu_hours     Float64                 COMMENT 'category=serving, 비FAIL 행 합',
    standby_gpu_hours     Float64                 COMMENT 'category=standby, 비FAIL 행 합',
    test_gpu_hours        Float64                 COMMENT 'category=test, 비FAIL 행 합 — C 불포함(그룹 귀속)',
    flagged_gpu_hours     Float64                 COMMENT 'FAIL 플래그(hours_over_count·unknown_violation) 행 합 — C 제외, 그룹 unattributed로',
    equiv_gpu_count       Float64                 COMMENT 'Σ(serving+standby+test) / 24',
    scaled_intraday       UInt8 DEFAULT 0         COMMENT '당일(부분 구간) 집계에서 24h 환산했으면 1 — P0는 항상 0',
    model_cost_krw        Nullable(Float64)       COMMENT 'C = Σ(serving+standby, 비FAIL) gpu_hours × tco_krw_per_gpu_hour — TCO 부재 기종 있으면 NULL',
    input_tokens          UInt64                  COMMENT 'mart.token_usage_1d 합 (usage_svc 전 서비스)',
    cache_read_tokens     UInt64,
    cache_creation_tokens UInt64,
    output_tokens         UInt64,
    requests              UInt64,
    uncached_tokens       UInt64                  COMMENT '= input + cache_creation (정의서 3.5)',
    cached_tokens         UInt64                  COMMENT '= cache_read',
    total_tokens          UInt64                  COMMENT '= 4합 (표시 3분류는 뷰 규칙 — D5)',
    weighted_tokens       Float64                 COMMENT 'W(s,m) = 1·uncached + 0.1·cached + 4·output (정의서 3.5)',
    tokens_per_gpu_hour   Nullable(Float64)       COMMENT '= total_tokens / serving_gpu_hours — 분모 0이면 NULL',
    gpu_type_mix          Array(String)           COMMENT '그날 이 (service, model)에 관측된 gpu_type 정렬 목록',
    model_registered      UInt8                   COMMENT 'canonical이 gpu_data.dim_token_model_alias에 identity/alias 행으로 존재(canon 히트)하면 1, 미등록 0 (설계 §2·§6.1)',
    tco_missing           UInt8                   COMMENT 'gpu_type_mix 중 date 유효 TCO 부재 기종이 있으면 1',
    has_token_rows        UInt8                   COMMENT 'tok_agg 키 존재',
    has_gpu_rows          UInt8                   COMMENT 'gpu_agg 키 존재 (FAIL 행 포함)',
    quality_flag          LowCardinality(String)  COMMENT 'partial > no_tco > flagged > manual > no_metrics > consumer_only > normal (우선순위 고정)',
    created_by            LowCardinality(String)  COMMENT '공유 쓰기 계약 — 본 파이프라인은 token-metrics-pipeline 고정',
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/mart/agg_token_model_cost_1d_local',
    '{replica}'
)
PARTITION BY toYYYYMM(date)
ORDER BY (date, service, model)
TTL date + INTERVAL 25 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mart.agg_token_model_cost_1d_dist
ON CLUSTER 'gpu-monitoring'
(
    date                  Date,
    service_group         LowCardinality(String),
    service               LowCardinality(String),
    model                 LowCardinality(String),
    serving_gpu_hours     Float64,
    standby_gpu_hours     Float64,
    test_gpu_hours        Float64,
    flagged_gpu_hours     Float64,
    equiv_gpu_count       Float64,
    scaled_intraday       UInt8,
    model_cost_krw        Nullable(Float64),
    input_tokens          UInt64,
    cache_read_tokens     UInt64,
    cache_creation_tokens UInt64,
    output_tokens         UInt64,
    requests              UInt64,
    uncached_tokens       UInt64,
    cached_tokens         UInt64,
    total_tokens          UInt64,
    weighted_tokens       Float64,
    tokens_per_gpu_hour   Nullable(Float64),
    gpu_type_mix          Array(String),
    model_registered      UInt8,
    tco_missing           UInt8,
    has_token_rows        UInt8,
    has_gpu_rows          UInt8,
    quality_flag          LowCardinality(String),
    created_by            LowCardinality(String),
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = Distributed('gpu-monitoring', 'mart', 'agg_token_model_cost_1d_local',
                     cityHash64(service));

-- -------------------------------------------------------------
-- 2) mart.token_metrics_check_1d — M3 데이터 품질 검사 (9/14 알림 표면)
--    grain: date × service × check_name × model × gpu_type (model/gpu_type은 해당 없으면 '')
--    detail은 수·이름만 (데이터 원문·사용자 식별자 금지 — §5.6)
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.token_metrics_check_1d_local
ON CLUSTER 'gpu-monitoring'
(
    date          Date                              COMMENT 'KST 집계일',
    service_group LowCardinality(String),
    service       LowCardinality(String),
    check_name    LowCardinality(String)            COMMENT 'metrics_missing | partial_load | rows_rejected | unregistered_model | hours_over_count | unknown_violation | pct_non_monotone | gpu_type_no_tco | serving_missing_for_gpu_model | serving_without_gpu_serving_row | identity_drift | service_not_in_usage_registry | manual_source | (stretch) provider_ambiguous | consumer_tokens_exceed_provider | vendor_price_missing | no_allocation | sum_hours_over_allocation | gpu_block_empty_unexpected | serving_block_empty_unexpected',
    model         LowCardinality(String) DEFAULT '' COMMENT '해당 없으면 빈 문자열',
    gpu_type      LowCardinality(String) DEFAULT '' COMMENT '해당 없으면 빈 문자열',
    severity      LowCardinality(String)            COMMENT 'FAIL | WARN | INFO',
    observed      Nullable(Float64)                 COMMENT '관측값 (수치가 없는 검사는 NULL)',
    threshold     Nullable(Float64)                 COMMENT '기준값 (없으면 NULL)',
    detail        String DEFAULT ''                 COMMENT '수·이름만 (예: rejected=3, gpu_type=H100)',
    source_type   LowCardinality(String) DEFAULT '' COMMENT 'metrics-api-v1 | manual-v0 | 빈 문자열(앵커 없음)',
    created_by    LowCardinality(String)            COMMENT 'token-metrics-pipeline 고정',
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/mart/token_metrics_check_1d_local',
    '{replica}'
)
PARTITION BY toYYYYMM(date)
ORDER BY (date, service, check_name, model, gpu_type)
TTL date + INTERVAL 25 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mart.token_metrics_check_1d_dist
ON CLUSTER 'gpu-monitoring'
(
    date          Date,
    service_group LowCardinality(String),
    service       LowCardinality(String),
    check_name    LowCardinality(String),
    model         LowCardinality(String),
    gpu_type      LowCardinality(String),
    severity      LowCardinality(String),
    observed      Nullable(Float64),
    threshold     Nullable(Float64),
    detail        String,
    source_type   LowCardinality(String),
    created_by    LowCardinality(String),
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = Distributed('gpu-monitoring', 'mart', 'token_metrics_check_1d_local',
                     cityHash64(service));

-- -------------------------------------------------------------
-- 3) mart.agg_token_model_share_1d — M4 공유 모델 배분 (P0-stretch; 정의서 3.6/3.9)
--    grain: date × model(canon) × service × provider_service
--    share = W(s)/W(m); allocated_cost_krw = model_cost × share (내부) / 벤더 단가 (external_api)
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.agg_token_model_share_1d_local
ON CLUSTER 'gpu-monitoring'
(
    date                Date                    COMMENT 'KST 집계일',
    model               LowCardinality(String)  COMMENT 'canonical',
    service             LowCardinality(String)  COMMENT '사용(소비) 서비스',
    service_group       LowCardinality(String),
    provider_service    LowCardinality(String)  COMMENT '제공자 서비스 — 사외 API 모델은 벤더 표기, 없으면 빈 문자열',
    is_provider         UInt8                   COMMENT 'service == provider_service',
    denominator_mode    LowCardinality(String)  COMMENT 'all_services | provider_reported | token_not_reported | no_provider | provider_ambiguous | external_api',
    service_wtokens     Float64                 COMMENT 'W(s,m) — 가중 1/0.1/4',
    model_total_wtokens Float64                 COMMENT 'W(m) — 분모 (denominator_mode에 따라)',
    share               Nullable(Float64)       COMMENT 'W(s)/W(m) — provider_ambiguous·분모 0이면 NULL, token_not_reported 제공자 행은 1',
    model_cost_krw      Nullable(Float64)       COMMENT 'C(m) = M1 제공자 행의 model_cost_krw',
    allocated_cost_krw  Nullable(Float64)       COMMENT '내부: C × share / external_api: 벤더 단가 합 / 1e6 (단가 행 부재 시 NULL)',
    quality_flag        LowCardinality(String)  COMMENT 'normal | token_not_reported | provider_ambiguous | vendor_price_missing | no_tco | partial',
    created_by          LowCardinality(String)  COMMENT 'token-metrics-pipeline 고정',
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/mart/agg_token_model_share_1d_local',
    '{replica}'
)
PARTITION BY toYYYYMM(date)
ORDER BY (date, model, service, provider_service)
TTL date + INTERVAL 25 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mart.agg_token_model_share_1d_dist
ON CLUSTER 'gpu-monitoring'
(
    date                Date,
    model               LowCardinality(String),
    service             LowCardinality(String),
    service_group       LowCardinality(String),
    provider_service    LowCardinality(String),
    is_provider         UInt8,
    denominator_mode    LowCardinality(String),
    service_wtokens     Float64,
    model_total_wtokens Float64,
    share               Nullable(Float64),
    model_cost_krw      Nullable(Float64),
    allocated_cost_krw  Nullable(Float64),
    quality_flag        LowCardinality(String),
    created_by          LowCardinality(String),
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = Distributed('gpu-monitoring', 'mart', 'agg_token_model_share_1d_local',
                     cityHash64(model));

-- -------------------------------------------------------------
-- 4) mart.agg_token_gpu_group_1d — M2 그룹 귀속·유휴 (P0-stretch; 정의서 3.1/3.3/3.4)
--    grain: date × service_group × gpu_type (쿼터 보유 단위)
--    I1: idle ≥ 0 (음수면 over_report=1 + 0 클램프) / I2: group_total = ΣC + test + idle + unattributed ± gap
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mart.agg_token_gpu_group_1d_local
ON CLUSTER 'gpu-monitoring'
(
    date                     Date                    COMMENT 'KST 집계일',
    service_group            LowCardinality(String),
    gpu_type                 LowCardinality(String),
    allocated_gpu_hours      Nullable(Float64)       COMMENT 'dim_token_gpu_allocation date 유효 행 allocated_gpu_count × 24 — 할당 행 없으면 NULL',
    group_total_cost_krw     Nullable(Float64)       COMMENT '= allocated_gpu_hours × TCO (정의서 3.4)',
    serving_gpu_hours        Float64                 COMMENT '그룹 합, 비FAIL',
    standby_gpu_hours        Float64                 COMMENT '그룹 합, 비FAIL',
    test_gpu_hours           Float64                 COMMENT '그룹 합, 비FAIL',
    reported_gpu_hours_total Float64                 COMMENT '플래그 포함 전체 보고 합',
    flagged_gpu_hours        Float64                 COMMENT 'FAIL 플래그 행 합',
    model_cost_sum_krw       Nullable(Float64)       COMMENT 'Σ (serving+standby) gpu_hours × TCO — 기종별 계산 (한 모델이 TCO 결측 기종에 걸치면 Σ M1과 다를 수 있음, N6)',
    test_cost_krw            Nullable(Float64)       COMMENT '= Σ test × TCO — 그룹 귀속, 배분 안 함',
    idle_gpu_hours           Nullable(Float64)       COMMENT '= allocated − reported_total (음수면 0)',
    idle_cost_krw            Nullable(Float64)       COMMENT '= idle_gpu_hours × TCO',
    unattributed_cost_krw    Nullable(Float64)       COMMENT '= (flagged_gpu_hours + other_gpu_hours) × TCO — other = 비FAIL·category ∉ {serving,standby,test}',
    identity_gap_krw         Nullable(Float64)       COMMENT '= group_total − model_cost_sum − test_cost − idle_cost − unattributed (I2)',
    utilization              Nullable(Float64)       COMMENT '= reported_total / allocated',
    over_report              UInt8                   COMMENT 'reported_total > allocated 이면 1 (I1 FAIL)',
    equiv_gpu_count          Float64                 COMMENT '= reported_total / 24',
    tco_missing              UInt8                   COMMENT 'date 유효 TCO 부재면 1 (비용 컬럼 전부 NULL)',
    allocation_source        LowCardinality(String)  COMMENT 'dim_token_gpu_allocation.source — 할당 행 없으면 빈 문자열',
    quality_flag             LowCardinality(String)  COMMENT 'normal | no_allocation | no_tco | over_report | flagged',
    created_by               LowCardinality(String)  COMMENT 'token-metrics-pipeline 고정',
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/mart/agg_token_gpu_group_1d_local',
    '{replica}'
)
PARTITION BY toYYYYMM(date)
ORDER BY (date, service_group, gpu_type)
TTL date + INTERVAL 25 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mart.agg_token_gpu_group_1d_dist
ON CLUSTER 'gpu-monitoring'
(
    date                     Date,
    service_group            LowCardinality(String),
    gpu_type                 LowCardinality(String),
    allocated_gpu_hours      Nullable(Float64),
    group_total_cost_krw     Nullable(Float64),
    serving_gpu_hours        Float64,
    standby_gpu_hours        Float64,
    test_gpu_hours           Float64,
    reported_gpu_hours_total Float64,
    flagged_gpu_hours        Float64,
    model_cost_sum_krw       Nullable(Float64),
    test_cost_krw            Nullable(Float64),
    idle_gpu_hours           Nullable(Float64),
    idle_cost_krw            Nullable(Float64),
    unattributed_cost_krw    Nullable(Float64),
    identity_gap_krw         Nullable(Float64),
    utilization              Nullable(Float64),
    over_report              UInt8,
    equiv_gpu_count          Float64,
    tco_missing              UInt8,
    allocation_source        LowCardinality(String),
    quality_flag             LowCardinality(String),
    created_by               LowCardinality(String),
    CONSTRAINT check_created_by CHECK created_by != ''
)
ENGINE = Distributed('gpu-monitoring', 'mart', 'agg_token_gpu_group_1d_local',
                     cityHash64(service_group));
