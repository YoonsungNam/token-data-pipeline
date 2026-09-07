# mart/token-metrics — `/v1/metrics` 비용 mart 배치 (Plan 6c)

서비스가 보고한 GPU 시간(`fact.raw_token_metrics_*`)과 기존 토큰 mart(`mart.token_usage_1d`)를 결합해 **모델 비용 C·검사
결과·모델 비용 배분·그룹 GPU 정체성** 4테이블을 하루 단위로 만든다. `mart/token-usage` 의 클론이며 그 모듈과 코드·DDL·CronJob·
Secret 을 공유하지 않는다(기존 모듈 무수정 — 설계 2026-08-31 §6.2). 비용 정의의 정본은 `docs/cost-model-spec.md` 다.

## 요약

| 구분 | 테이블 |
|---|---|
| 읽기 — fact 3 | `fact.raw_token_metrics_gpu_1d_dist`, `fact.raw_token_metrics_serving_1d_dist`, `fact.raw_token_metrics_summary_1d_dist`(앵커) |
| 읽기 — dim 6 | `gpu_data.dim_token_metrics_service_dist`(레지스트리), `dim_token_service_dist`(토큰 레지스트리), `dim_token_model_alias_dist`, `dim_token_gpu_tco_dist`, `dim_token_gpu_allocation_dist`, `dim_token_vendor_price_dist` |
| 읽기 — 토큰 mart 2 | `mart.token_usage_1d_dist`, `mart.agg_token_service_1d_dist` (읽기 계약 13컬럼 — `app/preflight.py` `READ_CONTRACT`) |
| 쓰기 — mart 4 | `mart.agg_token_model_cost_1d`(M1)·28컬럼, `mart.token_metrics_check_1d`(M3)·12컬럼, `mart.agg_token_model_share_1d`(M4)·14컬럼, `mart.agg_token_gpu_group_1d`(M2)·23컬럼 — `created_by='token-metrics-pipeline'` |

DDL: `ddl/company/mart_metrics_tables.sql`(4테이블 `_local`/`_dist`, Plan 6a) · GRANT `ddl/company/accounts.sql`(admin 수동) ·
stage 미러 `ddl/stage/`(`tools/gen_stage_ddl.py`) · 격리 미러 `ddl/company-verify/`(`tools/gen_verify_ddl.py`).

## 실행

```bash
cd mart/token-metrics
python -m app.batch --date 2026-09-04             # 하루 (기본값: 어제 KST)
python -m app.batch --from 2026-09-01 --to 2026-09-07   # 범위 — 날짜별 순차, 날짜당 뮤테이션 ≤4
python -m app.batch --date 2026-09-04 --log-level DEBUG
```

컨테이너(`Dockerfile`, `build.sh`)·CronJob `token-mart-metrics`(`20 10 * * *` KST — 수집기 마지막 슬롯 09:05 KST 이후)·
Secret `token-mart-metrics-ch-secret` 은 `install.sh` 가 설치한다(§배포). 날짜 범위 재실행은 `tools/rerun.py`
(`--context --from --to [--chunk-days 1..16] [--force]`, 창 ≥10:50 KST, 활성 `token-mart-*` Job 0 — `--force`로도 우회 불가).

## 환경변수

| 이름 | 기본값 | 의미 |
|---|---|---|
| `CH_HOST` | `localhost` | ClickHouse 호스트 (클러스터 내부 서비스 주소) |
| `CH_PORT` | `8123` | HTTP 포트 |
| `CH_USER` | `default` | 접속 계정 — 운영은 공유 계정 `mart` |
| `CH_PASSWORD` | (빈 값) | 비밀번호 (Secret) |
| `CH_CLUSTER` | (빈 값) | `ON CLUSTER` 이름 — 뮤테이션(`ALTER … DELETE`) 대상. 빈 값 = 단일노드 |
| `RETRY_COUNT` | `10` | 접속·쿼리 재시도 횟수 |
| `RETRY_INTERVAL_S` | `5` | 재시도 간격(초) |
| `MUTATION_POLL_S` | `3` | `system.mutations` 폴링 간격(초) |
| `MUTATION_TIMEOUT_S` | `300` | 뮤테이션 완료 대기 한도(초) |
| `INSERT_QUORUM` | (빈 값) | `insert_quorum` 설정값 (stage 1s×1r 은 빈 값, company/-verify 는 install.sh 가 `auto` 주입) |
| `MART_METRICS_MAX_MUTATIONS_PER_RUN` | `64` | 실행당 뮤테이션 예산 = 16일 × 4테이블 — 초과 시 `reason=mutation_budget` |
| `CH_DB_FACT` | `fact` | 메트릭 fact DB |
| `CH_DB_DIM` | `gpu_data` | dim DB (레지스트리·기준정보) |
| `CH_DB_MART` | `mart` | 쓰기 mart DB |
| `CH_DB_TOKEN_MART` | `mart` | 읽는 토큰 mart DB (격리 검증 시 운영 DB 지정 가능) |
| `CH_DB_TOKEN_DIM` | `gpu_data` | 읽는 토큰 레지스트리 DB |

`EXPECTED_LATE_SERVICES` 와 `ORG_MAP_WARN_THRESHOLD` 는 이 모듈에 **없다**(토큰 mart 전용 — 메트릭 지연 판정은 M3 `metrics_missing` 검사가 대신한다).
앞 11개는 `app/config.py`, `CH_DB_*` 5개는 `app/ch.py` 가 읽는다. install.sh `[2/6]` 가 만드는 Secret 키는 11개(`CH_HOST … CH_DB_TOKEN_DIM`,
`MART_METRICS_MAX_MUTATIONS_PER_RUN` — company/-verify 는 `INSERT_QUORUM=auto` 추가) — `RETRY_*`/`MUTATION_*` 는 Secret에 없고 Python 기본값을 쓴다.

## 마커·WARN 코드

실행 끝에 한 줄(마스터 §5.6 — `user_id`·payload 는 싣지 않는다, `missing_services` 는 따옴표):

```
BATCH_RESULT status=SUCCESS|FAILURE module=mart-metrics metrics_coverage=<present>/<enabled> missing_services="a,b|-" rows_mart=<n> rows_check=<n> rows_share=<n> warn=<n> elapsed=<s.s> [reason=<r>]
```

`app.mart.batch_line(status, coverage, rows_mart, rows_check, rows_share, warn_count, elapsed_s, reason="")` 이 만든다.
`rows_mart` = M1 행수, `rows_check` = M3, `rows_share` = M4(스킵 시 0, M2 `rows_group` 은 마커에 없고 로그만). `reason` 은 FAILURE 에만: `read_contract`(토큰 mart 읽기
계약 불일치 — `PREFLIGHT FAIL read_contract missing=<…>` 선행), `mutation_budget`, `verify_count`(적재 후 재조회 ≠ EXPECTED),
`sigterm`(+ `note=sigterm`), `exception`. mart 마커는 `status=SUCCESS|FAILURE` 만 있다 — `NODATA` 상태는 6b 수집기 마커 전용이며 이 모듈에는 없다.

| WARN 코드(로그 `CHECK WARN …`) | 의미 | 상태 |
|---|---|---|
| `metrics_coverage missing=<n>` | enabled 서비스 중 앵커(summary) 없는 수 — 같은 서비스는 M3 `metrics_missing` FAIL 행 | SUCCESS 유지 |
| `service_not_in_usage_registry severity=WARN count=<n>` | 메트릭 레지스트리에만 있는 서비스 수(토큰 레지스트리 `dim_token_service` 부재) | SUCCESS 유지 |
| `token_mart_absent date=<d>` | 그 날짜 토큰 mart 0행 — M4 스킵(`rows_share=0`), M1 은 GPU-only 행 | SUCCESS 유지 |
| `dup_suspect:<table>` | 적재 후 키 중복 의심(재조회 uniqExact < count) | SUCCESS 유지 — `invariants_metrics` 로 확인 |

그 밖에 M3 검사마다 결과 행이 있으면 `CHECK WARN <check_name> severity=<FAIL|WARN> count=<n>` 1줄이 나오며, 마커의
`warn=` 은 그 줄 전부를 센다(`CHECK INFO` 제외).

## 실행 순서

M0 → M0b → M1 → M3 → M4 → M2 (날짜마다):

1. **M0** 읽기 계약 프리플라이트 — `DESCRIBE` 3테이블/13컬럼(`app/preflight.py`); 실패 시 적재 없이 `FAILURE reason=read_contract`.
2. **M0b** 커버리지 — 레지스트리 `enabled=1` ∩ 앵커 `raw_token_metrics_summary_1d` → `metrics_coverage`·`missing_services`.
3. **M1** `agg_token_model_cost_1d` — keys = 토큰 집계 ∪ GPU 집계, `model` 은 `dim_token_model_alias` 로 canonical 화, C = Σ(serving+standby, 비FAIL) gpu_hours × TCO(`dim_token_gpu_tco`, `effective_from <= date` 최신), 토큰 4종·`weighted_tokens`·`tokens_per_gpu_hour`, `quality_flag`(partial > no_tco > flagged > manual > no_metrics > consumer_only > normal).
4. **M3** `token_metrics_check_1d` — 검사 행(FAIL/WARN/INFO, 핵심 13 + stretch 7 = 20블록): `metrics_missing`, 플래그·TCO 부재·중복 등(`app/steps.py` `M3_BLOCKS_CORE`/`M3_BLOCKS_STRETCH`).
5. **M4** `agg_token_model_share_1d` — 공유 모델 비용 배분 share = W(s,m)/W(m), `denominator_mode` 6종(`all_services, provider_reported, token_not_reported, no_provider, provider_ambiguous, external_api`), 사외 API 는 `dim_token_vendor_price` 단가로 추정. 토큰 mart 부재일은 M4 자체를 건너뛴다(`token_mart_absent`).
6. **M2** `agg_token_gpu_group_1d` — 그룹×기종 정체성: allocated(`dim_token_gpu_allocation`×24) = Σ model cost + test + idle + unattributed, `identity_gap_krw`, `utilization`, `over_report`.

날짜마다 각 테이블 `DELETE WHERE date = …`(존재 시) 후 INSERT — 뮤테이션 ≤4/일, 예산 `MART_METRICS_MAX_MUTATIONS_PER_RUN`.
전 단계는 `insert_distributed_sync=1`·`distributed_product_mode=global` 로 실행하고 적재 후 재조회 행수를 EXPECTED 와 대조한다.

## 비용 모델 요약

정본은 `docs/cost-model-spec.md` §3(정의)·§7(표시·라벨) — 여기서는 이 모듈이 구현한 식만 적는다.

- **C(모델 비용)** = Σ(category ∈ serving, standby; FAIL 플래그 없는 행) `gpu_hours × tco_krw_per_gpu_hour`. `test` 는 C 에 넣지 않고 그룹에 귀속, FAIL 플래그 행은 `flagged_gpu_hours` 로 분리(그룹 `unattributed`). 기종 하나라도 TCO 가 없으면 C = NULL(`tco_missing=1`, `no_tco`) — 0 이 아니다.
- **W(가중 토큰)** = `1 × uncached + 0.1 × cached + 4 × output` (`app.mart.W_UNC=1.0, W_CACHE=0.1, W_OUT=4.0`; uncached = input + cache_creation, cached = cache_read).
- **share(s, m)** = W(s,m) / W(m), **allocated_cost_krw** = C × share(배분). 사외 API 모델은 `(input×p_in + cache_read×p_cached + cache_creation×p_write + output×p_out)/1e6`(추정). 토큰 미보고 모델(W(m)=0, C>0)은 제공자 서비스 행 share=1 전액(그룹 귀속).
- **그룹 정체성(I2)**: `allocated_gpu_hours × TCO = model_cost_sum + test_cost + idle_cost + unattributed_cost`, `identity_gap_krw` ≈ 0 이 정상; `utilization = reported_gpu_hours_total / allocated_gpu_hours`, 보고 > 배정이면 `over_report=1`(I1).
- **M2 `model_cost_sum_krw` 는 M1 C 의 합이 아니다**: (service_group, gpu_type) 단위로 fact 를 재집계한 `(serving + standby, FAIL 제외) × 그 기종 TCO` 다. 한 모델이 TCO 있는 기종과 없는 기종에 걸쳐 있으면 M1 C 는 NULL(부분 합 금지)이지만 M2 는 TCO 있는 기종 행에 그 모델의 시간을 그대로 포함한다 — 그룹의 모든 기종에 TCO 가 있을 때만 `Σ_gpu_type model_cost_sum_krw = Σ_model M1 C` 가 성립하며, 대시보드는 두 값을 같은 패널에서 합산·대조하지 않는다(M2 는 그룹 정체성용, M1 은 모델 비용용).
- 라벨: 측정(GPU 시간×TCO) / 배분(가중 토큰 비율) / 추정(벤더 단가) / 그룹 귀속(토큰 미보고 모델 전액) — 대시보드 `cost_label` 컬럼, 통화 KRW 고정.

## 테스트

```bash
cd mart/token-metrics
python -m pytest -q                       # 단위 (ClickHouse 불필요) — tests/test_docs_contract.py 포함(문서 ↔ 코드 계약)
bash tests/e2e/run_e2e.sh                 # E2E — 로컬 ClickHouse + 합성 fact/dim/토큰 mart 적재 → 배치 → 기대 결과 SQL
```

CI: `.github/workflows/test-mart-metrics.yml`(단위 + E2E), `.github/workflows/release-images-metrics.yml`(이미지).
불변식: `python3 tools/verify/run_invariants.py --sql tools/verify/invariants_metrics.sql --date <YYYY-MM-DD>` → `ALL INVARIANTS PASS`.

## 배포

절차 전체(기준정보 dim → 6b 수집기 → `install.sh` 프리플라이트 → 첫 배치 → 불변식 → 대시보드 → 재실행 → 격리 검증 → 트러블슈팅·롤백)는
`docs/operations/token-metrics-deploy.md`. 대시보드 `docs/monitoring/grafana_dashboard_token_metrics.json`(`docs/monitoring/README.md` §7),
stage 공통 환경 `docs/operations/stage-runbook.md`, 기존 모듈 재실행 규칙 `docs/operations/rerun.md`.

## 설계 해석

이 모듈이 설계 문서가 명시적으로 정하지 않은 지점에서 고른 것 — 근거는 `app/steps.py`·`app/mart.py`·`app/batch.py` 주석 참조.

1. **C4(`provider_reported` 분모)**: `D = max(W(p), Σ_{s≠p}W(s))`; 소비자 share = `W(s)/D`, 제공자 자기분 = `max(W(p) − Σ_{s≠p}W(s), 0)/D` — Σ share = 1, Σ 배분 = C (I3 예외 없음). `W(p)=0` 이고 소비자 토큰 > 0 이면 모드는 `provider_reported` 로 남고 소비자가 C 전액을 자기 비중대로 가져간다(M3 `consumer_tokens_exceed_provider` WARN 이 그 날을 표시). 파이썬 참조 구현은 `app.mart.provider_self_weight`/`allocate_shared`; M4 컬럼 `model_total_wtokens` = D.
2. **`skip_share=True`(토큰 mart 부재)인 날은 M4 `delete_day` 도 건너뛴다** — 전날 적재분이 그대로 남을 수 있으므로, 토큰 mart 가 채워진 뒤에는 `rerun.py` 로 같은 날짜를 다시 돌려야 M4 가 갱신된다.
3. **메트릭 미보고일**: 내부 서빙 모델이라도 그날 gpu 행이 없으면 `external_api`/`no_provider` 모드로 판정될 수 있다 — M3 `metrics_missing` FAIL 과 함께 읽는다(분모 모드 자체는 오류가 아니다).
4. **D3 (over_report 일)**: `idle_negative`(I1) 와 `group_identity_gap`(I2) 가 함께 FAIL 한다 — 면제 없음(데이터 품질 실패로 취급). gpu 행의 category 가 `{serving, standby, test}` 밖(비FAIL)이면 `unattributed_cost_krw` 에 `(flagged_gpu_hours + other_gpu_hours) × TCO` 로 귀속돼 그룹 항등식이 유지된다(`flagged_gpu_hours` 컬럼 자체는 FAIL 행만 담는다).
5. **N6**: M2 `model_cost_sum_krw`(기종별 fact × TCO 재집계)는 한 모델이 TCO 결측 기종에 걸치면 Σ M1 `model_cost_krw`(기종 하나라도 NULL → C NULL)와 값이 달라질 수 있다 — 불변식 `group_identity_gap`(I2)은 M2 자체 컬럼끼리만 계산해 이 편차와 무관하다.
6. **M1 `delete_day`는 extra_pred 없이 그 날짜 전체를 삭제**한다 — 단일 작성자(CronJob 1개 + rerun 상호배제) 전제이며, 부분 삭제 조건을 두지 않는다.
7. **마커 `missing_services`(합집합)는 M0 커버리지 결손을 포함하되 그보다 넓다** — 사용량(토큰) 레지스트리에 없는 메트릭 레지스트리 서비스명도 함께 나열한다(M3 `service_not_in_usage_registry` 가 세는 집합과 같음). 반면 `metrics_coverage=<present>/<enabled>`(및 `CHECK WARN metrics_coverage missing=<n>`)의 `<n>`은 M0 커버리지 결손만 센 값이다 — 두 결손은 원인이 달라 카운트를 섞지 않는다.
