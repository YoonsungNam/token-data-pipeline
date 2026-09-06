# mart/token-metrics DDL

> 상태: Plan 6a 초안 — 설계 `docs/superpowers/specs/2026-08-31-token-metrics-ingest-design.md` §4.0·§4.2·§6.1·§6.4.
> 비용 정본 = `docs/cost-model-spec.md`(Draft v0.1). 기존 `mart/token-usage/ddl/**` 무수정.

## 파일

| 파일 | 내용 | 적용 주체 |
|---|---|---|
| `company/mart_metrics_tables.sql` | M1 `agg_token_model_cost_1d`(모델 비용 C), M3 `token_metrics_check_1d`(품질 검사·9/14 알림 표면), M4 `agg_token_model_share_1d`(공유 모델 배분·stretch), M2 `agg_token_gpu_group_1d`(그룹 귀속·유휴·stretch) | `install.sh company`(6c) |
| `company/accounts.sql` | 공유 계정 `mart` GRANT (설계 §4.2 표 2행 — 자기 4테이블 + 읽기 계약 + `CREATE TEMPORARY TABLE`·`system.mutations`) | admin 수동 |
| `stage/*.sql` | `tools/gen_stage_ddl.py` 생성물 — 직접 수정 금지 | `install.sh stage` |
| `company-verify/*.sql` | `tools/gen_verify_ddl.py` 생성물(`token_verify_mart`) — 직접 수정 금지. 단, GRANT의 `mart.token_usage_1d_dist`·`mart.agg_token_service_1d_dist`는 `token_verify_mart.*`로 치환되므로 격리 검증 시 `CH_DB_TOKEN_MART`/`CH_DB_TOKEN_DIM`을 운영 DB로 가리키고 해당 SELECT 권한은 운영 GRANT에 의존한다(설계 §6.1) | `install.sh company-verify` |

## 쓰기 계약

- `created_by` DEFAULT 없음 + `CONSTRAINT check_created_by CHECK created_by != ''` — 6c `steps.py`가 `'token-metrics-pipeline'` 고정 삽입(기존 `token-pipeline`과 구분). 불변식 `created_by_wrong_metrics`(6c `tools/verify/invariants_metrics.sql`)가 검사.
- INSERT는 `_dist` 경유만, 멱등 DELETE는 `_local`(`ON CLUSTER`) — 날짜당 4 뮤테이션(존재할 때만).
- 읽기 계약(설계 §6.1, install.sh 프리플라이트 `DESCRIBE`): `mart.token_usage_1d` 9컬럼, `mart.agg_token_service_1d` 2컬럼, `gpu_data.dim_token_service` 2컬럼 — 그 외 기존 테이블·컬럼 의존 없음.

## 타입 규약 (설계 해석 — 설계가 타입을 적지 않은 컬럼)

| 부류 | 타입 | 예 |
|---|---|---|
| gpu 시간 합 | `Float64` | `serving_gpu_hours`, `reported_gpu_hours_total` |
| 비용·비율(부재 가능) | `Nullable(Float64)` | `model_cost_krw`, `share`, `utilization`, `idle_cost_krw` |
| 토큰·요청 카운트 | `UInt64` | `input_tokens`, `requests`, `total_tokens` |
| 가중 토큰 | `Float64` | `weighted_tokens`, `service_wtokens`, `model_total_wtokens` |
| 0/1 플래그 | `UInt8` | `model_registered`, `tco_missing`, `over_report`, `is_provider`, `scaled_intraday`(DEFAULT 0) |
| 분류 문자열 | `LowCardinality(String)` | `quality_flag`, `denominator_mode`, `severity`, `check_name`, `allocation_source` |
| 자유 문자열 | `String DEFAULT ''` | `detail`(수·이름만) |

## 뮤테이션 (설계 §4.0 장부 — mart 몫)

- 정기 10:20 실행: 날짜당 ≤4(M1·M3·M4·M2 — 첫 적재일 존재확인 no-op → 0).
- rerun: 날짜당 ≤4, `MART_METRICS_MAX_MUTATIONS_PER_RUN=64`(4×16), `--chunk-days 7`. 첫 `_run_table` 전 날짜 전체 × 4테이블 `exists` 선조회 → 초과 시 `FAILURE reason=mutation_budget`.
- 창: 10:50 KST 이후 + 활성 Job 0 확인(토큰 mart 04:00·메트릭 마지막 슬롯과 비중첩).

## 적용 순서

1. `collectors/token-metrics/ddl/company/*.sql` + `assets/model-catalog/ddl/company/{dim_token_*.sql,seed_*.sql,accounts_metrics.sql}` 선적용 (읽기 대상).
2. admin: `company/accounts.sql`.
3. `mart/token-metrics/install.sh company …`(6c) → `apply_sql` = `mart_metrics_tables.sql`.
