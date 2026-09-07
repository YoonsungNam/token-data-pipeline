-- =============================================================
-- Company/Stage ClickHouse DDL — fact.raw_token_metrics_* + fact.collect_audit_metrics_1d
-- (설계 2026-08-31 §4.0 매니페스트 · §4.1 fact 4테이블)
-- Target cluster: gpu-monitoring (company 2s×2r / stage 1s×1r)
-- Writer: mart (공유 계정 — collectors/token-metrics 수집기, 8슬롯 CronJob)
-- 주의: fact DB는 기존(공유) DB — DB 생성문 없음. 테이블 DDL만 install.sh 자동 적용.
-- 원칙 (§4.0): <이름>_local + <이름>_dist 쌍, DateTime('Asia/Seoul'), 문자열 NOT NULL(''),
--   숫자 부재는 Nullable, index_granularity 8192, 25개월 TTL, toYYYYMM 파티션(소행수).
-- 뮤테이션 (§4.0 장부 — ddl/README.md): 정기 8슬롯 실행은 0(앵커 존재→스킵, 미존재→INSERT만);
--   재수집 --replace는 날짜당 fact ≤3(gpu·serving·summary — 감사는 append-only).
-- 적재 순서 (§4.1): summary 앵커 DELETE 첫 번째 · INSERT 마지막 (앵커 존재 = 적재 완료).
-- =============================================================

-- -------------------------------------------------------------
-- 1) fact.raw_token_metrics_gpu_1d — GPU 점유 (grain: date × service × model × gpu_type × category)
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact.raw_token_metrics_gpu_1d_local
ON CLUSTER 'gpu-monitoring'
(
    date          Date                   COMMENT 'KST 집계일',
    service_group LowCardinality(String) COMMENT '정본 = collectors/token-metrics/endpoints.yaml',
    service       LowCardinality(String) COMMENT '정본 = collectors/token-metrics/endpoints.yaml',
    model         LowCardinality(String) COMMENT 'API 문자열 그대로(≤128, 정규화는 mart) — unknown은 category=test만 정상',
    gpu_type      LowCardinality(String) COMMENT 'TCO표 키 (정확 일치, ≤64)',
    category      LowCardinality(String) COMMENT 'serving | standby | test',
    gpu_count     Float64                COMMENT '그날 최대 장수 (분수 허용) — 비용 미사용',
    gpu_hours     Float64                COMMENT '장수×시간 적분 — 비용의 유일한 근거',
    flags         Array(String)          COMMENT 'hours_over_count(FAIL) | unknown_violation(FAIL) | dup_merged(WARN) — 빈 배열이 정상',
    source_type   LowCardinality(String) COMMENT 'metrics-api-v1 | manual-v0',
    generated_at  DateTime('Asia/Seoul') COMMENT '응답 generatedAt (KST 변환)',
    collected_at  DateTime('Asia/Seoul') COMMENT '적재 시각 (KST)'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/fact/raw_token_metrics_gpu_1d_local',
    '{replica}'
)
PARTITION BY toYYYYMM(date)
ORDER BY (date, service, model, gpu_type, category)
TTL date + INTERVAL 25 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS fact.raw_token_metrics_gpu_1d_dist
ON CLUSTER 'gpu-monitoring'
(
    date          Date,
    service_group LowCardinality(String),
    service       LowCardinality(String),
    model         LowCardinality(String),
    gpu_type      LowCardinality(String),
    category      LowCardinality(String),
    gpu_count     Float64,
    gpu_hours     Float64,
    flags         Array(String),
    source_type   LowCardinality(String),
    generated_at  DateTime('Asia/Seoul'),
    collected_at  DateTime('Asia/Seoul')
)
ENGINE = Distributed('gpu-monitoring', 'fact', 'raw_token_metrics_gpu_1d_local',
                     cityHash64(service));

-- -------------------------------------------------------------
-- 2) fact.raw_token_metrics_serving_1d — 서빙 성능 long form
--    (grain: date × service × model × metric × name; 유일성은 정규화기가 (model)·(model, custom.name) 중복 제거 후 성립)
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact.raw_token_metrics_serving_1d_local
ON CLUSTER 'gpu-monitoring'
(
    date          Date                   COMMENT 'KST 집계일',
    service_group LowCardinality(String) COMMENT '정본 = collectors/token-metrics/endpoints.yaml',
    service       LowCardinality(String) COMMENT '정본 = collectors/token-metrics/endpoints.yaml',
    model         LowCardinality(String) COMMENT 'API 문자열 그대로 (정규화는 mart)',
    metric        LowCardinality(String) COMMENT 'ttft_ms | itl_ms | e2e_ms | output_tps | custom',
    name          String DEFAULT ''      COMMENT '표준 지표는 빈 문자열 / custom 지표명 (≤64)',
    unit          LowCardinality(String) COMMENT 'ms / tokens/s / custom 단위 (≤32)',
    p50           Nullable(Float64)      COMMENT '부재 = NULL',
    p90           Nullable(Float64)      COMMENT '부재 = NULL (output_tps는 p50만)',
    p95           Nullable(Float64),
    p99           Nullable(Float64),
    flags         Array(String)          COMMENT 'pct_non_monotone(FAIL) | unknown_violation(FAIL) | dup_model_kept_first | dup_custom_kept_first',
    source_type   LowCardinality(String) COMMENT 'metrics-api-v1 | manual-v0',
    generated_at  DateTime('Asia/Seoul') COMMENT '응답 generatedAt (KST 변환)',
    collected_at  DateTime('Asia/Seoul') COMMENT '적재 시각 (KST)'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/fact/raw_token_metrics_serving_1d_local',
    '{replica}'
)
PARTITION BY toYYYYMM(date)
ORDER BY (date, service, model, metric, name)
TTL date + INTERVAL 25 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS fact.raw_token_metrics_serving_1d_dist
ON CLUSTER 'gpu-monitoring'
(
    date          Date,
    service_group LowCardinality(String),
    service       LowCardinality(String),
    model         LowCardinality(String),
    metric        LowCardinality(String),
    name          String,
    unit          LowCardinality(String),
    p50           Nullable(Float64),
    p90           Nullable(Float64),
    p95           Nullable(Float64),
    p99           Nullable(Float64),
    flags         Array(String),
    source_type   LowCardinality(String),
    generated_at  DateTime('Asia/Seoul'),
    collected_at  DateTime('Asia/Seoul')
)
ENGINE = Distributed('gpu-monitoring', 'fact', 'raw_token_metrics_serving_1d_local',
                     cityHash64(service));

-- -------------------------------------------------------------
-- 3) fact.raw_token_metrics_summary_1d — 응답당 정확히 1행(앵커), NODATA(rows==0)도 기록
--    (grain: date × service). 앵커 존재 = 적재 완료 — 정기 실행의 스킵 판정·M0 커버리지 근거.
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact.raw_token_metrics_summary_1d_local
ON CLUSTER 'gpu-monitoring'
(
    date                   Date                              COMMENT 'KST 집계일',
    service_group          LowCardinality(String)            COMMENT '정본 = collectors/token-metrics/endpoints.yaml',
    service                LowCardinality(String)            COMMENT '정본 = collectors/token-metrics/endpoints.yaml',
    reported_service_group String                            COMMENT 'API 응답 원문 — manual-v0는 레지스트리 값',
    reported_service       String                            COMMENT 'API 응답 원문 — identity_drift 검사는 metrics-api-v1만',
    engine_type            LowCardinality(String) DEFAULT '' COMMENT 'null·형태 불량이면 빈 문자열 (+engine_malformed WARN)',
    engine_version         String DEFAULT ''                 COMMENT '엔진 버전 원문 (없으면 빈 문자열)',
    gpu_rows               UInt32                            COMMENT '정규화 통과 gpu 행수',
    serving_rows           UInt32                            COMMENT '정규화 통과 serving 행수 (표준 지표)',
    custom_rows            UInt32                            COMMENT 'custom 지표 행수',
    rejected_rows          UInt32                            COMMENT '정규화 거부 행수',
    merged_dups            UInt16                            COMMENT '중복 병합 건수',
    source_type            LowCardinality(String)            COMMENT 'metrics-api-v1 | manual-v0',
    generated_at           DateTime('Asia/Seoul')            COMMENT '파싱 실패 → now(KST)+WARN, 오프셋≠+09:00 → KST 변환 + generated_at_offset_mismatch WARN',
    collected_at           DateTime('Asia/Seoul')            COMMENT '적재 시각 (KST)'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/fact/raw_token_metrics_summary_1d_local',
    '{replica}'
)
PARTITION BY toYYYYMM(date)
ORDER BY (date, service)
TTL date + INTERVAL 25 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS fact.raw_token_metrics_summary_1d_dist
ON CLUSTER 'gpu-monitoring'
(
    date                   Date,
    service_group          LowCardinality(String),
    service                LowCardinality(String),
    reported_service_group String,
    reported_service       String,
    engine_type            LowCardinality(String),
    engine_version         String,
    gpu_rows               UInt32,
    serving_rows           UInt32,
    custom_rows            UInt32,
    rejected_rows          UInt32,
    merged_dups            UInt16,
    source_type            LowCardinality(String),
    generated_at           DateTime('Asia/Seoul'),
    collected_at           DateTime('Asia/Seoul')
)
ENGINE = Distributed('gpu-monitoring', 'fact', 'raw_token_metrics_summary_1d_local',
                     cityHash64(service));

-- -------------------------------------------------------------
-- 4) fact.collect_audit_metrics_1d — 교체 감사 (append-only: 절대 DELETE 안 함, GRANT도 INSERT만)
--    --replace 재수집이 기존 세대를 지우기 직전 요약을 보존 (마스터 §8.4 상속)
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact.collect_audit_metrics_1d_local
ON CLUSTER 'gpu-monitoring'
(
    date               Date                   COMMENT '교체된 데이터의 대상 일자',
    service            LowCardinality(String) COMMENT '정본 서비스명',
    prev_generated_at  DateTime('Asia/Seoul') COMMENT '교체 전 세대의 generated_at',
    prev_collected_at  DateTime('Asia/Seoul') COMMENT '교체 전 세대의 적재 시각',
    prev_source_type   LowCardinality(String) COMMENT '교체 전 세대의 source_type (manual-v0 → metrics-api-v1 전환 추적)',
    prev_gpu_rows      UInt32                 COMMENT '교체 전 세대 gpu 행수',
    prev_gpu_hours_sum Float64                COMMENT '교체 전 세대 gpu_hours 합',
    prev_serving_rows  UInt32                 COMMENT '교체 전 세대 serving 행수',
    replaced_at        DateTime('Asia/Seoul') COMMENT '교체(재수집) 시각'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/fact/collect_audit_metrics_1d_local',
    '{replica}'
)
PARTITION BY toYYYYMM(date)
ORDER BY (date, service, replaced_at)
TTL date + INTERVAL 25 MONTH
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS fact.collect_audit_metrics_1d_dist
ON CLUSTER 'gpu-monitoring'
(
    date               Date,
    service            LowCardinality(String),
    prev_generated_at  DateTime('Asia/Seoul'),
    prev_collected_at  DateTime('Asia/Seoul'),
    prev_source_type   LowCardinality(String),
    prev_gpu_rows      UInt32,
    prev_gpu_hours_sum Float64,
    prev_serving_rows  UInt32,
    replaced_at        DateTime('Asia/Seoul')
)
ENGINE = Distributed('gpu-monitoring', 'fact', 'collect_audit_metrics_1d_local',
                     cityHash64(service));
