# Token Metrics Mart (Plan 6c/6) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `/v1/metrics` 반입의 **mart 계층**을 신규 모듈 `mart/token-metrics/`로 완성한다 — Plan 6a가 고정한 mart 4테이블(M1 `agg_token_model_cost_1d`·M3 `token_metrics_check_1d`·M4 `agg_token_model_share_1d`·M2 `agg_token_gpu_group_1d`)을 채우는 일배치(M0→M0b→M1→M3→M4→M2, 뮤테이션 예산 선검사, `module=mart-metrics` 마커), 읽기 계약 프리플라이트(3테이블/13컬럼 DESCRIBE), 배포 계층(Dockerfile·build.sh·k8s CronJob `token-mart-metrics` 10:20 KST·install.sh·rerun.py `--chunk-days`), 불변식 `tools/verify/invariants_metrics.sql` + `run_invariants.py --sql`, 결정적 E2E + CI, Grafana 대시보드 JSON + 운영 문서. 비용 모델은 `docs/cost-model-spec.md`(정본)의 §6.4 매핑을 SQL로 옮기고, 단위 테스트가 정의서 §5.1/§5.2 워크 예시를 재현한다.

**Architecture:** `mart/token-usage`를 **클론**한 독립 모듈(공용 패키지 추출 없음 — 설계 §7.4 "3번째 중복 시"). `app/config.py`(env) · `app/ch.py`(CHGate 멱등 프리미티브 + DB 상수 5종) · `app/mart.py`(순수 로직: 커버리지·마커·날짜창·예산·**비용 모델 참조 구현**) · `app/steps.py`(서버사이드 `INSERT INTO … SELECT` SQL 상수 + `_run_table` 실행) · `app/batch.py`(오케스트레이션·CLI·SIGTERM). 변환은 전부 ClickHouse 서버사이드(GLOBAL LEFT JOIN, `{d:Date}` 바인딩, `distributed_product_mode=global`), 멱등 시퀀스 = `exists → delete_day(_local, ON CLUSTER) → wait_for_mutations → insert_select(_dist) → EXPECTED_SQL 소스 카운트 → verify_count`. 토큰 측 읽기는 `DB_TOKEN_MART`/`DB_TOKEN_DIM` 상수로만(격리 검증 시 운영 DB로 향함).

**Tech Stack:** Python 3.10+ 표준 라이브러리 + `clickhouse-connect>=0.7,<1` + `pytest>=8`; bash(install/build/e2e); kustomize; GitHub Actions(CH 24.8 컨테이너 E2E); Grafana 11.6 + grafana-clickhouse-datasource 4.19 JSON.

**Spec:**
- 설계(자매 스펙): `docs/superpowers/specs/2026-08-31-token-metrics-ingest-design.md` — §4.0(82-131) 물리·매니페스트·뮤테이션 장부, §4.1(132-176) fact, §4.2(177-195) dim/GRANT, §4.3(196-230) 레지스트리, §5.2(239-259) 수집기 마커, §5.3(260-267) 플래그, §5.4(268-275) 부분 적재, **§6.1(293-306) 배치**, §6.2(308-310) 대시보드, §6.3(312-314) rerun, **§6.4(316-336) 비용 모델 매핑**, §7.1(340-342) 불변식, §7.3(350-354) 테스트·문서, §7.5(361-371) 배포.
- 비용 정본: `docs/cost-model-spec.md` (Draft v0.1) — §3 수식, §5.1/§5.2 워크 예시, §8 불변식 I1~I8, §9 의사코드.
- 스키마 정본: `docs/superpowers/plans/2026-09-04-token-metrics-schema.md`(Plan 6a) — "6b/6c가 소비하는 인터페이스"(4950-5049) + T4 mart DDL(1076-1481, `created_by=token-metrics-pipeline`) + T6 stage fixture(2015-2226): A fact / B 레지스트리 / C mart / D dim / H 공유 도구.
- 마스터 스펙 `docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md` v1.14(Plan 6a T11이 개정 — **이 플랜은 마스터 스펙을 수정하지 않는다**).

## Global Constraints

- **Zero-diff(설계 §7.5 — 절대 편집 금지)**: `collectors/token-usage/**`, `mart/token-usage/**`, `assets/user-org/**`, `assets/model-catalog/`의 기존 파일, `tools/verify/invariants.sql`, `docs/operations/{company-verify,stage-runbook,rerun}.md`, `docs/monitoring/grafana_dashboard_token_usage.json`, `.github/workflows/{release-images,test-collector,test-mart}.yml`. 태스크 종료 시 `git diff --stat -- <위 경로>`가 비어 있어야 한다.
- **허용된 additive 편집만**: `tools/verify/run_invariants.py`(`--sql` 옵션 추가 — 기본 경로·기존 동작 불변), `.github/workflows/release-images-metrics.yml`(Plan 6b 생성 — mart 항목 **추가**; 부재 시 collectors+mart 2항목으로 신규 생성), `docs/monitoring/README.md`(신규 절 append — 기존 절 무수정), `tools/verify/tests/`(신규 테스트 파일 추가). 이 플랜은 `tools/gen_stage_ddl.py`·`tools/gen_verify_ddl.py`·`test-assets.yml`·`.gitignore`·`tools/mock-provider/**`·마스터 스펙을 건드리지 않는다(각각 6a·6b가 담당).
- **공개 레포 경계**: 사내 호스트/주소 금지(플레이스홀더 `harbor.example.internal`, `chi-<cluster>.<ns>.svc`), 사내 프로젝트 코드명 금지, 소유자 이메일 금지, 실데이터 파일 금지(설계 §7.2 gitignore — Plan 6a T1).
- **이름은 설계·Plan 6a 그대로**(§4.0·§4.3·§5.2·§5.5·§5.6·§6.1·§6.3·§7.1): 테이블 `mart.agg_token_model_cost_1d`·`mart.token_metrics_check_1d`·`mart.agg_token_model_share_1d`·`mart.agg_token_gpu_group_1d`(`_local`/`_dist`), `created_by='token-metrics-pipeline'`, CronJob/컨테이너/이미지 `token-mart-metrics`, Secret `token-mart-metrics-ch-secret`(company-verify: `-verify` 접미), `imagePullSecrets: registry-pull-secret`(없을 때만 생성), 마커 `module=mart-metrics`, env `CH_DB_TOKEN_MART`/`CH_DB_TOKEN_DIM`/`MART_METRICS_MAX_MUTATIONS_PER_RUN=64`. 모호한 항목은 하나로 정해 footer "설계 해석"에 기록한다.
- **CronJob 계약 수치(임의 변경 금지)**: `schedule: "20 10 * * *"`, `timeZone: Asia/Seoul`, `concurrencyPolicy: Forbid`, `startingDeadlineSeconds: 1800`, `activeDeadlineSeconds: 1800`, `backoffLimit: 1`, `restartPolicy: Never`, resources requests 100m/256Mi · limits 1/1Gi, `successfulJobsHistoryLimit/failedJobsHistoryLimit: 3`.
- **DB 상수 5종만**(`app/ch.py`): `DB_FACT=CH_DB_FACT|fact`, `DB_DIM=CH_DB_DIM|gpu_data`, `DB_MART=CH_DB_MART|mart`, `DB_TOKEN_MART=CH_DB_TOKEN_MART|DB_MART`, `DB_TOKEN_DIM=CH_DB_TOKEN_DIM|DB_DIM`. steps.py의 모든 테이블 참조는 `f"{DB_x}.<table>_dist"`(INSERT/SELECT) · `_local`(DELETE)뿐. 토큰 측 3테이블(`token_usage_1d`, `agg_token_service_1d` → `DB_TOKEN_MART`; `dim_token_service` → `DB_TOKEN_DIM`)은 **읽기 계약 13컬럼만** 참조한다.
- **SQL 계약(테스트로 고정)**: 모든 SQL/EXPECTED_SQL 상수에 `{d:Date}` 포함·`%(` 부재·`'coalesce('` 부재(대문자 포함 — `coalesce(` 소문자로 검사, 작성 시 대문자도 쓰지 않는다)·INSERT 컬럼 목록 명시·`SELECT *` 금지·`'token-metrics-pipeline' AS created_by`. canon 식은 `steps.canon(x)`가 만든 **동일 문자열**을 INSERT와 EXPECTED 양쪽에 사용. LEFT JOIN 미스는 `''`(join_use_nulls=0) — `if(a.canonical = '', x, a.canonical)`.
- **멱등 시퀀스·뮤테이션**: 테이블당 `exists → delete_day → insert_select(insert_distributed_sync=1, insert_deduplicate=0, distributed_product_mode=global[, insert_quorum]) → EXPECTED_SQL → verify_count(재시도 RETRY_COUNT×RETRY_INTERVAL_S)`; written_rows는 텔레메트리. 날짜당 뮤테이션 ≤4(M1·M3·M4·M2). **예산 선검사**: 첫 `_run_table` 전에 (대상 날짜 전체 × 4테이블) `exists` 합산 > `MART_METRICS_MAX_MUTATIONS_PER_RUN`(기본 64)이면 실행 없이 날짜별 `BATCH_RESULT status=FAILURE … reason=mutation_budget`(exit 1). rerun은 `--chunk-days 7`로 분할.
- **마커(§6.1)**: 날짜당 정확히 1줄 `BATCH_RESULT status=<SUCCESS|FAILURE> module=mart-metrics metrics_coverage=N/M missing_services="<a,b>|-" rows_mart=<n> rows_check=<n> rows_share=<n> warn=<n> elapsed=<s.s>[ reason=<r>]`; 인라인 검증은 `CHECK WARN <code> …`(카운트·이름만, 페이로드·user_id 금지); SIGTERM 시 캐시 줄 재출력 + ` note=sigterm`. **메트릭 fact가 없는 날 = 토큰-only 행 + NULL + WARN, 절대 FAILURE 아님.**
- **비용 모델(§6.4 = 정의서)**: C = Σ(serving+standby, 비FAIL) gpu_hours × TCO(date 유효, 기종 하나라도 NULL → C NULL); FAIL 플래그 = `hasAny(flags, ['hours_over_count','unknown_violation'])`; `W_UNC=1, W_CACHE=0.1, W_OUT=4`(steps.py 상수); uncached = input + cache_creation; W(m) 모집단 = `dim_token_service enabled=1` 전 서비스; 분모 모드 6종 `all_services|provider_reported|token_not_reported|no_provider|provider_ambiguous|external_api`; 사외 ③ = `(input×krw_per_mtok_input + cache_read×krw_per_mtok_cached + cache_creation×krw_per_mtok_cache_creation + output×krw_per_mtok_output)/1e6`(tier='standard'); M2 `allocated_gpu_hours = allocated_gpu_count × 24`, idle 클램프 0 + `over_report`.
- **Python 3.10+**: `from __future__ import annotations`, StrEnum/tomllib/match/`datetime.UTC` 금지, `random` 금지(결정적 시드는 sha256), aware KST datetime만(`KST = timezone(timedelta(hours=9))`), 테스트는 `cd mart/token-metrics && python -m pytest -q`(루트 `conftest.py` + `tests/__init__.py`). 로컬에 docker 없음(CI가 E2E), 공유 클러스터 kubectl 변형 금지.
- **커밋 관례**: `type(scope): 한국어 설명 (Plan 6c Tn)` — scope는 `mart-metrics`(모듈 코드·E2E·문서)/`verify`(tools/verify — 기존 히스토리 `feat(verify)` 관례)/`tools`/`ddl`/`ci`/`docs`; 트레일러 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` + `Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54`. 태스크당 1커밋 이상, 커밋 전 `git diff --stat`으로 zero-diff 목록 무변경 확인.

## File Structure

전부 신규 파일(Create) — 예외는 `Modify (additive)`로 표기한 3파일뿐. `mart/token-metrics/ddl/**`는 Plan 6a T4·T7 산출물(읽기만, 무수정).

```
mart/token-metrics/                                          # 신규 모듈 — mart/token-usage 클론 (공용 패키지 없음)
├── app/
│   ├── __init__.py                                          # Create: 빈 파일(패키지)
│   ├── config.py                                            # Create: Config dataclass + load_config() — CH_*, RETRY_*, MUTATION_*, INSERT_QUORUM, MART_METRICS_MAX_MUTATIONS_PER_RUN
│   ├── ch.py                                                # Create: DB 상수 5종(DB_FACT/DB_DIM/DB_MART/DB_TOKEN_MART/DB_TOKEN_DIM) + KST + CHGate(exists/delete_day/wait_for_mutations/insert_select/verify_count/query/describe)
│   ├── preflight.py                                         # Create: READ_CONTRACT(3테이블/13컬럼) + missing_columns() 순수 함수 (batch 기동 시·install.sh 대조 테스트)
│   ├── mart.py                                              # Create: 순수 로직 — Coverage/Warn/batch_line/target_dates/plan_mutations 판정 + 비용 모델 참조 구현(model_cost/weighted_tokens/allocate_shared/external_api_cost/group_overhead/quality_flag_m1)
│   ├── steps.py                                             # Create: 공통 서브쿼리 SUB_*(eff_alias/eff_tco/eff_alloc/eff_price/reg/usage_svc/anchor) + canon() + SQL_M1/M3/M4/M2 + EXPECTED_SQL_* + W_UNC/W_CACHE/W_OUT + _run_table/run_m1/run_m3/run_m4/run_m2
│   └── batch.py                                             # Create: 오케스트레이션 M0→M0b→M1→M3→M4→M2, 프리플라이트·뮤테이션 예산 선검사·마커·SIGTERM·CLI(--from/--to/batch_time)
├── tests/
│   ├── __init__.py                                          # Create
│   ├── test_config.py                                       # Create: env 파싱(예산 기본 64, DB 상수 fallback)
│   ├── test_ch.py                                           # Create: CHGate settings(insert_distributed_sync/insert_deduplicate/distributed_product_mode)·describe·DB 상수 5종
│   ├── test_preflight.py                                    # Create: 13컬럼 계약·누락 판정·install.sh 배열 대조
│   ├── test_mart.py                                         # Create: 마커·커버리지·예산 + 정의서 §5.1(Qwen3-32B 240,000원)·§5.2(p≈0.00516)·§5.3(idle 0/16) 재현
│   ├── test_steps.py                                        # Create: SQL 계약({d:Date}·coalesce 부재·created_by·canon 동일 문자열·가중치 상수·분모 모드 6종 문자열·_run_table 시퀀스)
│   ├── test_batch.py                                        # Create: FakeGate 라우팅 — 순서·마커 1줄·no-metrics day SUCCESS·mutation_budget FAILURE·read_contract FAILURE·M0b 스킵·SIGTERM
│   ├── test_rerun.py                                        # Create: CRONJOB 상수·chunk_ranges·window_ok(10:50)·deadline
│   ├── test_install_contract.py                             # Create: install.sh READ_CONTRACT == preflight.READ_CONTRACT·[3/6]<[4/6]·cronjob.yaml 계약 grep
│   ├── test_e2e_seed.py                                     # Create(T10): CH 없이 seed_metrics.build_seed/mart_expectations.expect 결정성·시나리오 값·ddl_test_dims.sql 상수 교차 검증
│   ├── test_docs_contract.py                                # Create(T11): 대시보드 JSON 구조·FROM 허용집합·시간 매크로·컬럼 존재(DDL 대조)·README §7·배포 문서 절·플레이스홀더 계약
│   └── e2e/
│       ├── run_e2e.sh                                       # Create: CH 24.8 단일노드 — DDL 변환(6a fact/dim/mart DDL + ddl_test_dims.sql) → seed → 2회 실행(멱등) → verify expect-empty → 불변식 --sql
│       ├── seed_metrics.py                                  # Create: 결정적 시드(sha256) — fact 3테이블 + 토큰 읽기 계약 3테이블 + dim 4 + 레지스트리 (정의서 §5.1/§5.3 값 포함)
│       ├── ddl_test_dims.sql                                # Create: 단일노드 twin — dim 4종·dim_token_metrics_service·읽기 계약 3테이블 최소 컬럼(`_dist` 이름의 MergeTree)
│       ├── mart_expectations.py                             # Create: app.mart 참조 구현으로 기대값 산출(key=value 출력)
│       └── verify_expected_results.sql                      # Create: expect-empty 검증(M1/M3/M4/M2 행수·비용·share 합·identity_gap·created_by)
├── tools/
│   └── rerun.py                                             # Create: CRONJOB=token-mart-metrics, --chunk-days 7, 창 10:50+·활성 token-mart-* Job 0 확인, --chain 없음(수신 측)
├── k8s/
│   ├── base/cronjob.yaml                                    # Create: CronJob token-mart-metrics "20 10 * * *" (계약 수치 고정)
│   ├── base/kustomization.yaml                              # Create
│   ├── overlays/stage/kustomization.yaml                    # Create: ghcr.io/yoonsungnam/token-mart-metrics
│   ├── overlays/company/kustomization.yaml                  # Create: harbor.example.internal 플레이스홀더
│   └── overlays/company-verify/kustomization.yaml           # Create: name suffix -verify, Secret token-mart-metrics-ch-secret-verify
├── ddl/                                                     # (Plan 6a T4/T7 산출 — 무수정) company/{mart_metrics_tables,accounts}.sql, stage/, company-verify/, README.md
├── Dockerfile                                               # Create: python:3.12-slim, app/ 복사, ENTRYPOINT python -m app.batch
├── build.sh                                                 # Create: IMAGE=token-mart-metrics (stage=ghcr, company=--registry)
├── install.sh                                               # Create: [1/6] registry-pull-secret(없을 때만) [2/6] Secret [3/6] 읽기 계약 DESCRIBE 프리플라이트 [4/6] DDL apply_sql(mart_metrics_tables.sql) [5/6] kustomize apply [6/6] set image/env
├── conftest.py                                              # Create: sys.path(모듈 루트)
├── requirements.txt                                         # Create: clickhouse-connect>=0.7,<1
├── requirements-dev.txt                                     # Create: -r requirements.txt + pytest>=8
└── README.md                                                # Create: 실행/환경변수/마커/배포/비용 모델 포인터

tools/verify/
├── invariants_metrics.sql                                   # Create: 5 P0 블록 + 3 stretch 블록 (3컬럼 계약, {FACT}/{DIM}/{MART}/{DATE})
├── run_invariants.py                                        # Modify (additive): --sql <path> (기본 SQL_PATH), 메시지에 sql 파일명 표기
└── tests/test_run_invariants_metrics.py                     # Create: --sql 라우팅·기본값 불변·metrics 블록 8종 정적 점검

.github/workflows/
├── test-mart-metrics.yml                                    # Create: paths mart/token-metrics/**, collectors/token-metrics/ddl/**, assets/model-catalog/ddl/**, tools/verify/invariants_metrics.sql — image/manifests/unit/e2e
└── release-images-metrics.yml                               # Modify (additive; 부재 시 Create): matrix += {context: mart/token-metrics, image: token-mart-metrics}

docs/monitoring/
├── grafana_dashboard_token_metrics.json                     # Create: uid token-metrics-stage, 10 데이터 패널 + 텍스트 1 (측정/배분/추정 라벨)
└── README.md                                                # Modify (additive): "## 7. token-metrics 대시보드" 절 append

docs/operations/token-metrics-deploy.md                      # Create: §7.5 배포 절차·프리플라이트·rerun 창 10:50·--chunk-days·부분 적재 복구·롤백
```

---
### Task 1: 모듈 스캐폴드 — app/{__init__,config,ch,preflight}.py + 테스트 인프라 (DB 상수 5종·describe·읽기 계약 13컬럼)

**설계 근거**: 설계 §6.1 295-297(Secret 키 `CH_DB_TOKEN_MART`/`CH_DB_TOKEN_DIM` — 기본 = `CH_DB_MART`/`CH_DB_DIM`; `steps.py`의 DB명은 `DB_FACT/DB_DIM/DB_MART/DB_TOKEN_MART/DB_TOKEN_DIM`만; 읽기 계약 **3테이블/13컬럼** — `mart.token_usage_1d` 9 · `mart.agg_token_service_1d` 2 · `gpu_data.dim_token_service` 2, 그 외 컬럼·테이블 의존 없음), §4.0 117-131(`distributed_product_mode=global` 분산 조인 표준, `MART_METRICS_MAX_MUTATIONS_PER_RUN` 기본 **64** = 4×16), §7.5 370(사내 프리플라이트 — mart install.sh가 13컬럼을 `DESCRIBE`로 확인), Plan 6a `ddl/README.md` "쓰기 계약"(읽기 계약 9/2/2 재확인), Plan 6a H(공유 도구 등록 — 이 태스크는 공유 파일을 건드리지 않는다).

**원형(클론 후 델타)**: `mart/token-usage/app/config.py:1-50`, `mart/token-usage/app/ch.py:1-126`(이미 `distributed_product_mode: "global"` 포함 — 원본 100-101행 확인), `mart/token-usage/tests/test_config.py:1-70`, `mart/token-usage/tests/test_ch.py:1-240`, `mart/token-usage/conftest.py`(빈 파일 — 6c는 `tools/verify/conftest.py` 관례대로 sys.path를 명시), `mart/token-usage/requirements.txt`·`requirements-dev.txt`. 원본 파일은 **읽기만** 한다(zero-diff).

**Files:**
- Create: `mart/token-metrics/app/__init__.py`(빈 파일)
- Create: `mart/token-metrics/app/config.py` — `Config` dataclass + `load_config()`
- Create: `mart/token-metrics/app/ch.py` — DB 상수 5종 + `KST`/`now_kst()` + `CHGate`(`describe` 신규)
- Create: `mart/token-metrics/app/preflight.py` — `READ_CONTRACT`(3테이블/13컬럼) + `contract_tables()` + `missing_columns()`
- Create: `mart/token-metrics/conftest.py`, `mart/token-metrics/tests/__init__.py`(빈 파일)
- Create: `mart/token-metrics/requirements.txt`, `mart/token-metrics/requirements-dev.txt`
- Test: `mart/token-metrics/tests/test_config.py`, `mart/token-metrics/tests/test_ch.py`, `mart/token-metrics/tests/test_preflight.py`

**Interfaces:**
- Consumes: 없음(기존 코드 import 없음 — 외부 의존은 `clickhouse-connect>=0.7,<1`뿐). 원형 시그니처는 `mart/token-usage/app/ch.py`의 `CHGate`와 동일하게 유지한다(T3 `_run_table`·T5 `run_batch`가 같은 호출 규약을 클론).
- Produces (이후 태스크·플랜이 이름 그대로 사용):
  - `app.config.Config` (dataclass) — 필드·기본값: `ch_host: str = "localhost"`, `ch_port: int = 8123`, `ch_user: str = "default"`, `ch_password: str = ""`, `ch_cluster: str = ""`, `retry_count: int = 10`, `retry_interval_s: int = 5`, `mutation_poll_s: int = 3`, `mutation_timeout_s: int = 300`, `insert_quorum: str = ""`, `max_mutations_per_run: int = 64`. **`expected_late_services`·`org_map_warn_threshold` 없음**(토큰 mart 전용).
  - `app.config.load_config() -> Config` — env `CH_HOST CH_PORT CH_USER CH_PASSWORD CH_CLUSTER RETRY_COUNT RETRY_INTERVAL_S MUTATION_POLL_S MUTATION_TIMEOUT_S INSERT_QUORUM MART_METRICS_MAX_MUTATIONS_PER_RUN`(빈 문자열 = 기본값).
  - `app.ch.DB_FACT = os.getenv("CH_DB_FACT", "fact")`, `DB_DIM = os.getenv("CH_DB_DIM", "gpu_data")`, `DB_MART = os.getenv("CH_DB_MART", "mart")`, `DB_TOKEN_MART = os.getenv("CH_DB_TOKEN_MART", DB_MART)`, `DB_TOKEN_DIM = os.getenv("CH_DB_TOKEN_DIM", DB_DIM)` — 전부 **모듈 로드 시 1회 평가**(T3 `steps.py`의 SQL f-string이 import 시점에 고정됨).
  - `app.ch.KST = timezone(timedelta(hours=9))`, `app.ch.now_kst() -> datetime`(aware KST).
  - `app.ch.CHGate(cfg: Config, client=None, clock=time.monotonic, sleeper=time.sleep)` — 메서드 `exists(table_dist: str, date: str) -> bool`, `delete_day(table_local: str, date: str, extra_pred: str = "") -> None`, `wait_for_mutations(table_local: str) -> None`, `insert_select(sql: str, params: dict | None = None) -> int`(settings = `{"insert_distributed_sync": 1, "insert_deduplicate": 0, "distributed_product_mode": "global"}` + `cfg.insert_quorum`이 비어있지 않으면 `"insert_quorum"`), `verify_count(table_dist: str, date: str, expected: int) -> tuple[bool, int]`, `query(sql: str, params: dict | None = None) -> list[tuple]`, **`describe(table_dist: str) -> list[str]`**(신규 — `EXISTS TABLE` 선조회 0이면 `[]`, 아니면 `DESCRIBE TABLE`의 첫 컬럼(컬럼명) 리스트, 선언 순).
  - `app.preflight.READ_CONTRACT: dict[str, tuple[str, ...]]` — 키 `f"{DB_TOKEN_MART}.token_usage_1d"`(9컬럼 `date, service_group, service, model, input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, requests`), `f"{DB_TOKEN_MART}.agg_token_service_1d"`(`date, service`), `f"{DB_TOKEN_DIM}.dim_token_service"`(`service, enabled`) — 키는 `db.table`(**`_dist` 접미 없음**).
  - `app.preflight.contract_tables() -> list[str]` — `READ_CONTRACT` 키 3개, 선언 순.
  - `app.preflight.missing_columns(described: dict[str, list[str]]) -> list[str]` — 순수 함수. 입력 키 = `db.table`(`_dist` 없이; 호출자가 `f"{t}_dist"`로 DESCRIBE). 반환 = `"<db.table>.<col>"` **정렬** 목록; 테이블 키 부재 또는 빈 리스트(테이블 부재)는 `"<db.table>.*"` 1항목. 여분 컬럼·계약 밖 테이블 키는 무시.
  - 테스트 인프라: `cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q`(루트 `conftest.py`가 모듈 루트를 `sys.path`에 삽입, `tests/__init__.py` 존재). `tests/test_ch.py`의 `FakeCH(existing_count=0, mutations_left=0, count_sequence=None, insert_written_rows=0, rows=None, table_exists=1)`·`FakeResult`·`FakeSummary`·`FakeClock`은 T3·T5 테스트가 같은 형태로 복제한다(공유 import 없음).

- [ ] **Step 1: 디렉터리·패키지·requirements·conftest 생성 (테스트 인프라)**

```bash
cd /home/mini/github/token-data-pipeline
mkdir -p mart/token-metrics/app mart/token-metrics/tests
: > mart/token-metrics/app/__init__.py
: > mart/token-metrics/tests/__init__.py
cat > mart/token-metrics/requirements.txt <<'REQ'
clickhouse-connect>=0.7,<1
REQ
cat > mart/token-metrics/requirements-dev.txt <<'REQ'
-r requirements.txt
pytest>=8
REQ
```

`mart/token-metrics/conftest.py` (신규 — `tools/verify/conftest.py` 관례):

```python
import pathlib
import sys

# tests/는 패키지(tests/__init__.py)이지만, `python -m pytest`를 다른 cwd에서 호출하거나
# rootdir 추론이 바뀌어도 `import app`이 항상 되도록 모듈 루트(mart/token-metrics)를
# sys.path에 명시적으로 얹는다 (tools/verify/conftest.py와 동일 관례).
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
```

검증:

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics
ls app tests conftest.py requirements.txt requirements-dev.txt
diff requirements.txt ../token-usage/requirements.txt && diff requirements-dev.txt ../token-usage/requirements-dev.txt && echo REQ_SAME
python -m pytest -q; echo "exit=$?"
```

기대: `ls`가 `app: __init__.py`, `tests: __init__.py`, 파일 3개를 나열; `REQ_SAME`; pytest는 `no tests ran` + `exit=5`(수집 대상 없음 — 인프라만 준비된 상태).

- [ ] **Step 2: 실패하는 테스트 — `mart/token-metrics/tests/test_config.py`** (원형 `mart/token-usage/tests/test_config.py:1-70` 클론 후 델타: `EXPECTED_LATE_SERVICES`·`ORG_MAP_WARN_THRESHOLD` 단언 제거, 예산 단언 추가)

```python
from app.config import Config, load_config

ENV_KEYS = (
    "CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_CLUSTER",
    "RETRY_COUNT", "RETRY_INTERVAL_S", "MUTATION_POLL_S", "MUTATION_TIMEOUT_S",
    "INSERT_QUORUM", "MART_METRICS_MAX_MUTATIONS_PER_RUN",
    # 토큰 mart(mart/token-usage) 전용 — 6c에는 없어야 한다 (설계 §6.1: late 목록은 레지스트리
    # coverage_since/until로 대체). 잔존 env가 있어도 무시되는지 확인용으로 함께 지운다.
    "EXPECTED_LATE_SERVICES", "ORG_MAP_WARN_THRESHOLD",
)


def _clear_env(monkeypatch):
    for k in ENV_KEYS:
        monkeypatch.delenv(k, raising=False)


def test_config_defaults(monkeypatch):
    _clear_env(monkeypatch)
    cfg = load_config()
    assert cfg.ch_host == "localhost"
    assert cfg.ch_port == 8123
    assert cfg.ch_user == "default"
    assert cfg.ch_password == ""
    assert cfg.ch_cluster == ""                  # 빈 값 = 단일노드, ON CLUSTER 생략
    assert cfg.retry_count == 10
    assert cfg.retry_interval_s == 5
    assert cfg.mutation_poll_s == 3
    assert cfg.mutation_timeout_s == 300
    assert cfg.insert_quorum == ""                # 빈 값 = 미적용
    assert cfg.max_mutations_per_run == 64        # 설계 §4.0 — 4테이블 × 16일


def test_env_parsing(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("CH_HOST", "ch.internal")
    monkeypatch.setenv("CH_PORT", "9000")
    monkeypatch.setenv("CH_USER", "mart")
    monkeypatch.setenv("CH_PASSWORD", "secret")
    monkeypatch.setenv("CH_CLUSTER", "gpu-monitoring")
    monkeypatch.setenv("RETRY_COUNT", "3")
    monkeypatch.setenv("RETRY_INTERVAL_S", "2")
    monkeypatch.setenv("MUTATION_POLL_S", "1")
    monkeypatch.setenv("MUTATION_TIMEOUT_S", "60")
    monkeypatch.setenv("INSERT_QUORUM", "auto")
    monkeypatch.setenv("MART_METRICS_MAX_MUTATIONS_PER_RUN", "12")
    cfg = load_config()
    assert cfg.ch_host == "ch.internal"
    assert cfg.ch_port == 9000
    assert cfg.ch_user == "mart"
    assert cfg.ch_password == "secret"
    assert cfg.ch_cluster == "gpu-monitoring"
    assert cfg.retry_count == 3
    assert cfg.retry_interval_s == 2
    assert cfg.mutation_poll_s == 1
    assert cfg.mutation_timeout_s == 60
    assert cfg.insert_quorum == "auto"
    assert cfg.max_mutations_per_run == 12


def test_defaults_budget_64_and_no_expected_late(monkeypatch):
    """6c 델타: 예산 필드는 기본 64, 토큰 mart 전용 필드 2개는 존재하지 않는다(잔존 env가
    주입돼도 무시 — company Secret에 EXPECTED_LATE_SERVICES가 남아 있어도 무해)."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("EXPECTED_LATE_SERVICES", "svc-a,svc-b")
    monkeypatch.setenv("ORG_MAP_WARN_THRESHOLD", "0.5")
    cfg = Config()
    assert cfg.max_mutations_per_run == 64
    assert hasattr(cfg, "expected_late_services") is False
    assert hasattr(cfg, "org_map_warn_threshold") is False
    loaded = load_config()
    assert loaded == Config()                     # 잔존 env 무시 → 전부 기본값
    assert hasattr(loaded, "expected_late_services") is False


def test_env_override_budget(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("MART_METRICS_MAX_MUTATIONS_PER_RUN", "8")
    assert load_config().max_mutations_per_run == 8
    monkeypatch.setenv("MART_METRICS_MAX_MUTATIONS_PER_RUN", "  ")   # 공백 = 미설정 → 기본값
    assert load_config().max_mutations_per_run == 64
```

- [ ] **Step 3: RED 확인**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_config.py 2>&1 | grep -E "No module named|[0-9]+ error"
```

기대(3줄): `E   ModuleNotFoundError: No module named 'app.config'`(`app/__init__.py`만 있고 `config.py`가 없음) / `Interrupted: 1 error during collection` / `1 error in …s`.

- [ ] **Step 4: 구현 — `mart/token-metrics/app/config.py`** (원형 `mart/token-usage/app/config.py:1-50` 클론 후 델타: `_float_env`·`_csv_env`·`expected_late_services`·`org_map_warn_threshold` 제거, `max_mutations_per_run` 추가)

```python
"""mart-metrics 배치 환경변수 (설계 §6.1) — mart/token-usage/app/config.py 클론 + 델타.

델타: EXPECTED_LATE_SERVICES·ORG_MAP_WARN_THRESHOLD 제거(토큰 mart 전용 — 6c의 M0 커버리지
기대 집합은 레지스트리 dim_token_metrics_service의 coverage_since/until로 계산하므로 late
목록이 없다), 뮤테이션 예산 MART_METRICS_MAX_MUTATIONS_PER_RUN 추가(설계 §4.0 장부 —
기본 64 = 4테이블 × 16일; batch.py가 첫 DELETE 전 exists 선조회 합산과 비교).
CH_DB_* 는 config가 아니라 app.ch 모듈 상수(로드 시 1회 평가)로 읽는다.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    return int(raw) if raw.strip() else default


@dataclass
class Config:
    ch_host: str = "localhost"
    ch_port: int = 8123
    ch_user: str = "default"
    ch_password: str = ""              # 운영 계정 mart(공유)는 Secret 주입
    ch_cluster: str = ""               # 빈 값 = 단일노드 (ON CLUSTER·clusterAllReplicas 생략, §4.0)
    retry_count: int = 10               # count 검증 재시도 횟수
    retry_interval_s: int = 5
    mutation_poll_s: int = 3            # wait_for_mutations 폴링 주기
    mutation_timeout_s: int = 300
    insert_quorum: str = ""             # 빈 값 = 미적용, company는 install.sh가 'auto' 주입
    max_mutations_per_run: int = 64     # 설계 §4.0 — 초과 시 FAILURE reason=mutation_budget (T5)


def load_config() -> Config:
    return Config(
        ch_host=os.getenv("CH_HOST", "localhost"),
        ch_port=_int_env("CH_PORT", 8123),
        ch_user=os.getenv("CH_USER", "default"),
        ch_password=os.getenv("CH_PASSWORD", ""),
        ch_cluster=os.getenv("CH_CLUSTER", ""),
        retry_count=_int_env("RETRY_COUNT", 10),
        retry_interval_s=_int_env("RETRY_INTERVAL_S", 5),
        mutation_poll_s=_int_env("MUTATION_POLL_S", 3),
        mutation_timeout_s=_int_env("MUTATION_TIMEOUT_S", 300),
        insert_quorum=os.getenv("INSERT_QUORUM", ""),
        max_mutations_per_run=_int_env("MART_METRICS_MAX_MUTATIONS_PER_RUN", 64),
    )
```

- [ ] **Step 5: GREEN 확인**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_config.py 2>&1 | tail -n 1
```

기대: `4 passed`.

- [ ] **Step 6: 실패하는 테스트 — `mart/token-metrics/tests/test_ch.py`** (원형 `mart/token-usage/tests/test_ch.py:1-240` 클론 후 델타: `FakeCH`에 `table_exists`·`EXISTS TABLE` 라우팅 추가, `created_by` 값 `token-metrics-pipeline`, DB 상수 5종 테스트 2개 교체, settings 정확 일치·`describe` 테스트 추가; 원형의 `steps.SQL_DETAIL` 단언은 T3가 `SQL_M1`로 추가)

```python
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest

from app.ch import (CHGate, DB_DIM, DB_FACT, DB_MART, DB_TOKEN_DIM, DB_TOKEN_MART, KST,
                    now_kst)
from app.config import Config

MODULE_ROOT = Path(__file__).resolve().parent.parent

DATE = "2026-09-01"


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeSummary:
    """clickhouse-connect QuerySummary 흉내 — written_rows만 필요."""

    def __init__(self, written_rows):
        self.written_rows = written_rows


class FakeCH:
    """mart/token-usage FakeCH 클론 — command/query 호출 이력을 전부 기록한다.

    - commands: [(sql, parameters, settings), ...]  (client.command 호출)
    - queries:  [(sql, parameters), ...]             (client.query 호출)
    - mutations_left: system.mutations/clusterAllReplicas 폴링 응답용 카운트다운.
      None이면 항상 pending(count=1) — 타임아웃 테스트 전용. 0이면 즉시 완료(no-op 대기).
    - count_sequence: exists/verify_count의 count() 응답을 순서대로 흉내(마지막 값은 유지).
    - rows: query()의 일반 SELECT/DESCRIBE 응답(list[list]) — count()/mutations 분기보다 우선.
    - table_exists: `EXISTS TABLE …` 응답(1/0) — describe()의 선조회 (6c 델타).
    """

    def __init__(self, existing_count=0, mutations_left=0, count_sequence=None,
                 insert_written_rows=0, rows=None, table_exists=1):
        self.commands = []
        self.queries = []
        self.existing_count = existing_count
        self.mutations_left = mutations_left
        self.count_sequence = list(count_sequence) if count_sequence is not None else None
        self.insert_written_rows = insert_written_rows
        self.rows = rows
        self.table_exists = table_exists

    def command(self, sql, parameters=None, settings=None):
        self.commands.append((" ".join(sql.split()), parameters, settings))
        return FakeSummary(self.insert_written_rows)

    def query(self, sql, parameters=None):
        sql_n = " ".join(sql.split())
        self.queries.append((sql_n, parameters))
        if sql_n.startswith("EXISTS TABLE"):
            return FakeResult([[self.table_exists]])
        if "system.mutations" in sql_n:
            if self.mutations_left is None:
                return FakeResult([[1]])                      # 항상 pending
            if self.mutations_left > 0:
                self.mutations_left -= 1
                return FakeResult([[self.mutations_left + 1]])
            return FakeResult([[0]])
        if self.rows is not None:
            return FakeResult(self.rows)
        if self.count_sequence:
            val = self.count_sequence[0]
            if len(self.count_sequence) > 1:
                self.count_sequence.pop(0)
            return FakeResult([[val]])
        return FakeResult([[self.existing_count]])


class FakeClock:
    """sleeper가 clock을 전진시키는 결정론적 시계 (실 sleep 유입 차단)."""

    def __init__(self):
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_exists_skips_delete_when_absent():      # §4.0 no-op 뮤테이션 스킵
    ch = FakeCH(existing_count=0)
    g = CHGate(Config(), client=ch)
    assert g.exists("mart.agg_token_model_cost_1d_dist", DATE) is False


def test_exists_true_when_present():
    ch = FakeCH(existing_count=5)
    g = CHGate(Config(), client=ch)
    assert g.exists("mart.agg_token_model_cost_1d_dist", DATE) is True


def test_delete_day_on_cluster_and_waits():
    ch = FakeCH(existing_count=5)
    g = CHGate(Config(ch_cluster="gpu-monitoring"), client=ch)
    g.delete_day("mart.agg_token_model_cost_1d_local", DATE)
    cmd = ch.commands[0][0]
    assert "ON CLUSTER 'gpu-monitoring'" in cmd and "DELETE WHERE date =" in cmd
    assert any("clusterAllReplicas" in q for q, _ in ch.queries)   # 전 레플리카 폴링


def test_delete_day_no_on_cluster_when_cluster_empty():
    ch = FakeCH(existing_count=5)
    g = CHGate(Config(ch_cluster=""), client=ch)
    g.delete_day("mart.agg_token_model_cost_1d_local", DATE)
    cmd = ch.commands[0][0]
    assert "ON CLUSTER" not in cmd
    assert all("clusterAllReplicas" not in q for q, _ in ch.queries)
    assert any("system.mutations" in q for q, _ in ch.queries)


def test_delete_day_extra_pred_created_by():     # 추가 술어 형식 'AND …' (§7.1)
    ch = FakeCH(existing_count=5)
    CHGate(Config(), client=ch).delete_day(
        "mart.token_metrics_check_1d_local", DATE,
        extra_pred="AND created_by = 'token-metrics-pipeline'")
    assert "AND created_by = 'token-metrics-pipeline'" in ch.commands[0][0]
    assert ch.commands[0][1] == {"d": DATE}


def test_wait_for_mutations_timeout_raises():    # sleeper가 clock을 전진
    ch = FakeCH(mutations_left=None)              # 항상 pending — 타임아웃 강제
    fc = FakeClock()
    g = CHGate(Config(mutation_timeout_s=9, mutation_poll_s=3), client=ch,
               clock=fc.time, sleeper=fc.sleep)
    with pytest.raises(TimeoutError):
        g.wait_for_mutations("mart.agg_token_model_cost_1d_local")


def test_wait_for_mutations_returns_when_pending_reaches_zero():
    ch = FakeCH(mutations_left=2)                 # 2회 pending 후 완료
    fc = FakeClock()
    g = CHGate(Config(), client=ch, clock=fc.time, sleeper=fc.sleep)
    g.wait_for_mutations("mart.agg_token_model_cost_1d_local")
    assert fc.now == 6.0                          # 2 * mutation_poll_s(3) 만큼 전진


def test_verify_count_retries_then_passes():     # 재시도 중 카운트 도달
    ch = FakeCH(count_sequence=[3, 3, 7])
    g = CHGate(Config(retry_count=5, retry_interval_s=0), client=ch)
    ok, actual = g.verify_count("mart.agg_token_model_cost_1d_dist", DATE, expected=7)
    assert ok is True and actual == 7


def test_verify_count_actual_over_expected_passes_with_flag():  # 초과=통과(중복 징후는 호출자가 WARN)
    ch = FakeCH(existing_count=10)
    g = CHGate(Config(), client=ch)
    ok, actual = g.verify_count("mart.agg_token_model_cost_1d_dist", DATE, expected=7)
    assert ok is True and actual == 10


def test_verify_count_exhausted_fails():
    ch = FakeCH(existing_count=3)
    g = CHGate(Config(retry_count=3, retry_interval_s=0), client=ch)
    ok, actual = g.verify_count("mart.agg_token_model_cost_1d_dist", DATE, expected=7)
    assert ok is False and actual == 3


def test_insert_select_settings_contract():
    # settings **정확 일치**: insert_distributed_sync=1 AND insert_deduplicate=0(재삽입 폐기 차단)
    # AND distributed_product_mode='global'(§4.0 분산 조인 — 각 샤드 전역 조회). 그 외 키 없음.
    ch = FakeCH(insert_written_rows=42)
    g = CHGate(Config(), client=ch)
    n = g.insert_select("INSERT INTO mart.agg_token_model_cost_1d_dist SELECT ...", {"d": DATE})
    assert n == 42
    sql, params, settings = ch.commands[0]
    assert sql.startswith("INSERT INTO mart.agg_token_model_cost_1d_dist")
    assert params == {"d": DATE}
    assert settings == {"insert_distributed_sync": 1, "insert_deduplicate": 0,
                        "distributed_product_mode": "global"}


def test_insert_select_quorum_only_when_configured():
    # cfg.insert_quorum='' → settings에 insert_quorum 없음; 'auto' → insert_quorum='auto' 추가
    ch1 = FakeCH(insert_written_rows=5)
    CHGate(Config(insert_quorum=""), client=ch1).insert_select("INSERT ... SELECT ...")
    assert "insert_quorum" not in ch1.commands[0][2]

    ch2 = FakeCH(insert_written_rows=5)
    CHGate(Config(insert_quorum="auto"), client=ch2).insert_select("INSERT ... SELECT ...")
    assert ch2.commands[0][2] == {"insert_distributed_sync": 1, "insert_deduplicate": 0,
                                  "distributed_product_mode": "global", "insert_quorum": "auto"}


def test_insert_select_without_written_rows_raises():
    # 재실행 폴백 금지 — 이중 적재 위험
    class FakeCHNoWrittenRows:
        def command(self, sql, parameters=None, settings=None):
            return FakeSummary(None)               # written_rows 없는 요약 반환

    ch = FakeCHNoWrittenRows()
    g = CHGate(Config(), client=ch)
    with pytest.raises(RuntimeError, match="insert_select: written_rows 미획득"):
        g.insert_select("INSERT INTO mart.agg_token_model_cost_1d_dist SELECT ...")


def test_query_returns_rows():
    # 범용 SELECT 프리미티브 — M0 커버리지·예산 선조회·인라인 검증이 사용
    ch = FakeCH(rows=[["svc-a", 3], ["svc-b", 1]])
    g = CHGate(Config(), client=ch)
    result = g.query("SELECT service, count() FROM mart.agg_token_model_cost_1d_dist GROUP BY service")
    assert result == [("svc-a", 3), ("svc-b", 1)]


def test_now_kst_is_aware():
    assert now_kst().tzinfo is not None
    assert now_kst().utcoffset() == timedelta(hours=9)
    assert KST.utcoffset(None) == timedelta(hours=9)


def test_db_names_default_five():
    """설계 §6.1 DB 상수 5종 — 미설정 시 기존 배포·E2E 무변경 기본값. 토큰 측 2종은
    CH_DB_TOKEN_* 미설정이면 CH_DB_MART/CH_DB_DIM(여기서는 기본값)을 따른다."""
    assert DB_FACT == "fact"
    assert DB_DIM == "gpu_data"
    assert DB_MART == "mart"
    assert DB_TOKEN_MART == "mart"
    assert DB_TOKEN_DIM == "gpu_data"


def _child_db_constants(env: dict) -> list[str]:
    """자식 프로세스에서 app.ch를 import — 상수는 모듈 로드 시 1회 결정되므로(CronJob env
    주입 전제) 이미 import된 프로세스에서 os.environ만 바꿔서는 재평가되지 않는다."""
    result = subprocess.run(
        [sys.executable, "-c",
         "from app.ch import DB_FACT, DB_DIM, DB_MART, DB_TOKEN_MART, DB_TOKEN_DIM; "
         "print('\\n'.join([DB_FACT, DB_DIM, DB_MART, DB_TOKEN_MART, DB_TOKEN_DIM]))"],
        cwd=str(MODULE_ROOT), env=env, capture_output=True, text=True, check=True)
    return result.stdout.splitlines()


def test_db_constants_five_with_token_fallback():
    """company-verify 격리(설계 §6.1·§7.5): CH_DB_TOKEN_MART/CH_DB_TOKEN_DIM 미설정 →
    CH_DB_MART/CH_DB_DIM 값을 그대로 따른다(fallback); 설정 시 그 값(운영 DB로 토큰 측
    읽기 유지)."""
    base = {"PATH": os.environ.get("PATH", ""),
            "CH_DB_FACT": "token_verify_fact", "CH_DB_DIM": "token_verify_dim",
            "CH_DB_MART": "token_verify_mart"}
    assert _child_db_constants(base) == [
        "token_verify_fact", "token_verify_dim", "token_verify_mart",
        "token_verify_mart", "token_verify_dim"]                 # fallback = CH_DB_MART/CH_DB_DIM
    assert _child_db_constants({**base, "CH_DB_TOKEN_MART": "mart",
                                "CH_DB_TOKEN_DIM": "gpu_data"}) == [
        "token_verify_fact", "token_verify_dim", "token_verify_mart", "mart", "gpu_data"]


def test_describe_returns_column_names():
    # DESCRIBE TABLE의 첫 컬럼(name)만 선언 순으로 — 프리플라이트(app.preflight)가 대조
    ch = FakeCH(rows=[["date", "Date", "", "", "", "", ""],
                      ["service", "LowCardinality(String)", "", "", "", "", ""]])
    g = CHGate(Config(), client=ch)
    assert g.describe("mart.token_usage_1d_dist") == ["date", "service"]
    assert [q for q, _ in ch.queries] == ["EXISTS TABLE mart.token_usage_1d_dist",
                                          "DESCRIBE TABLE mart.token_usage_1d_dist"]


def test_describe_absent_table_returns_empty_without_describe():
    # 테이블 부재 = [] (EXISTS TABLE 선조회 — 드라이버 예외 파싱 없이 부재 판정; DESCRIBE 미실행)
    ch = FakeCH(rows=[["date", "Date"]], table_exists=0)
    g = CHGate(Config(), client=ch)
    assert g.describe("mart.token_usage_1d_dist") == []
    assert [q for q, _ in ch.queries] == ["EXISTS TABLE mart.token_usage_1d_dist"]
```

- [ ] **Step 7: RED 확인**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_ch.py 2>&1 | grep -E "No module named|[0-9]+ error"
```

기대(3줄): `E   ModuleNotFoundError: No module named 'app.ch'` / `Interrupted: 1 error during collection` / `1 error in …s`.

- [ ] **Step 8: 구현 — `mart/token-metrics/app/ch.py`** (원형 `mart/token-usage/app/ch.py:1-126` 클론 후 델타: docstring의 DB 상수 설명 5종, `DB_TOKEN_MART`/`DB_TOKEN_DIM` 추가(원본 25-27행 뒤), `describe()` 신규(원본 `query()` 뒤). `insert_select` settings는 원본 100-101행 그대로 — `distributed_product_mode: "global"` 이미 포함)

```python
"""ClickHouse 멱등 시퀀스 프리미티브 — CHGate (mart/token-usage/app/ch.py 클론, 설계 §6.1).

호출자(steps.py/batch.py — T3~T7)의 시퀀스: exists(존재 확인 — 없으면 delete 스킵,
§4.0 뮤테이션 절감) → delete_day(ALTER TABLE ... DELETE, ON CLUSTER + wait_for_mutations 내장)
→ insert_select(항상 _dist 경유, insert_distributed_sync=1 + insert_deduplicate=0 — 재삽입
중복제거 차단 + distributed_product_mode=global — §4.0 분산 조인 표준)
→ verify_count(재시도 RETRY_COUNT×RETRY_INTERVAL_S).

DB명은 아래 상수 **5종만** 쓴다(테이블명 하드코딩 금지, f"{DB_MART}.agg_token_model_cost_1d_dist"
형식). 토큰 측 읽기 계약 3테이블(token_usage_1d·agg_token_service_1d → DB_TOKEN_MART,
dim_token_service → DB_TOKEN_DIM)은 CH_DB_TOKEN_* 로 분리돼, company-verify 격리 DB
(token_verify_*)에서 검증할 때 운영 DB를 가리키게 할 수 있다(설계 §6.1·§7.5 — 미설정이면
CH_DB_MART/CH_DB_DIM을 따른다).

상수는 모듈 import 시점에 1회 평가된다. steps.py의 SQL 상수는 이 모듈을 import하는
시점에 f-string으로 DB명이 보간되어 문자열로 고정된다 — env는 프로세스 시작 시 1회
읽히므로(CronJob env 주입 전제) 런타임 중 재평가되지 않는다. 이는 의도된 동작이다.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

import clickhouse_connect

from app.config import Config

DB_FACT = os.getenv("CH_DB_FACT", "fact")                 # raw_token_metrics_*_1d (Plan 6a A)
DB_DIM = os.getenv("CH_DB_DIM", "gpu_data")               # dim_token_* 4종 + dim_token_metrics_service (6a B/D)
DB_MART = os.getenv("CH_DB_MART", "mart")                 # 6c가 쓰는 mart 4테이블 (6a C)
DB_TOKEN_MART = os.getenv("CH_DB_TOKEN_MART", DB_MART)    # 읽기 계약: token_usage_1d·agg_token_service_1d
DB_TOKEN_DIM = os.getenv("CH_DB_TOKEN_DIM", DB_DIM)       # 읽기 계약: dim_token_service

KST = timezone(timedelta(hours=9))


def now_kst() -> datetime:
    """CH DateTime('Asia/Seoul') 컬럼용 — aware KST. naive datetime을 clickhouse-connect가
    호스트 TZ로 해석하면 KST 벽시계와 어긋난다 (항상 tzinfo 유지)."""
    return datetime.now(KST)


class CHGate:
    def __init__(self, cfg: Config, client=None, clock=time.monotonic, sleeper=time.sleep):
        self.cfg = cfg
        self.client = client or clickhouse_connect.get_client(
            host=cfg.ch_host, port=cfg.ch_port, username=cfg.ch_user,
            password=cfg.ch_password)
        self.clock = clock
        self.sleeper = sleeper

    def _on_cluster(self) -> str:
        return f" ON CLUSTER '{self.cfg.ch_cluster}'" if self.cfg.ch_cluster else ""

    def _mutation_scope(self) -> str:
        if self.cfg.ch_cluster:
            return f"clusterAllReplicas('{self.cfg.ch_cluster}', system.mutations)"
        return "system.mutations"

    def _count(self, table_dist: str, date: str) -> int:
        r = self.client.query(
            f"SELECT count() FROM {table_dist} WHERE date = %(d)s",
            parameters={"d": date})
        return int(r.result_rows[0][0]) if r.result_rows else 0

    def exists(self, table_dist: str, date: str) -> bool:
        """존재 확인 SELECT — False면 호출자가 delete_day를 스킵한다 (§4.0 뮤테이션 절감).
        batch.plan_mutations(T5)가 날짜 전체 × 4테이블에 대해 이 값을 합산해 예산과 비교한다."""
        return self._count(table_dist, date) > 0

    def delete_day(self, table_local: str, date: str, extra_pred: str = "") -> None:
        """local 테이블에 ON CLUSTER DELETE 후 전 레플리카 뮤테이션 완료까지 대기.
        extra_pred: created_by 등 추가 조건 — 'AND ...' 형태로 전달."""
        pred = "date = %(d)s" + (f" {extra_pred}" if extra_pred else "")
        self.client.command(
            f"ALTER TABLE {table_local}{self._on_cluster()} DELETE WHERE {pred}",
            parameters={"d": date})
        self.wait_for_mutations(table_local)

    def wait_for_mutations(self, table_local: str) -> None:
        """CH_CLUSTER 설정 시 clusterAllReplicas(cluster, system.mutations)로 전 레플리카를
        폴링(3s), 300s 초과 시 TimeoutError. table_local은 "database.table" 형식."""
        db, tbl = table_local.split(".", 1)
        scope = self._mutation_scope()
        start = self.clock()
        while True:
            r = self.client.query(
                f"SELECT count() FROM {scope} "
                f"WHERE database = %(db)s AND table = %(tbl)s AND is_done = 0",
                parameters={"db": db, "tbl": tbl})
            pending = int(r.result_rows[0][0]) if r.result_rows else 0
            if not pending:
                return
            if self.clock() - start >= self.cfg.mutation_timeout_s:
                raise TimeoutError(
                    f"wait_for_mutations timeout ({self.cfg.mutation_timeout_s}s): "
                    f"{table_local} pending={pending}")
            self.sleeper(self.cfg.mutation_poll_s)

    def insert_select(self, sql: str, params: dict | None = None) -> int:
        """INSERT INTO ... SELECT 실행 — 항상 _dist 경유(co-location).
        insert_deduplicate=0 필수(재삽입 중복제거 차단 — Global Constraints).
        cfg.insert_quorum이 설정된 경우만 insert_quorum 설정을 포함한다.
        distributed_product_mode='global': GLOBAL LEFT JOIN이 각 샤드에서 dim을
        전역 조회하도록 강제(로컬 샤드만 보고 부분 조인하는 사고 방지 — §4.0 분산 조인 표준)."""
        settings = {"insert_distributed_sync": 1, "insert_deduplicate": 0,
                    "distributed_product_mode": "global"}
        if self.cfg.insert_quorum:
            settings["insert_quorum"] = self.cfg.insert_quorum
        result = self.client.command(sql, parameters=params, settings=settings)
        written = getattr(result, "written_rows", None)
        if written is None:
            raise RuntimeError(
                "insert_select: written_rows 미획득 — 드라이버 반환형 확인 필요 "
                "(재실행 폴백은 이중 적재 위험으로 금지)")
        return int(written)

    def verify_count(self, table_dist: str, date: str, expected: int) -> tuple[bool, int]:
        """RETRY_COUNT회(간격 RETRY_INTERVAL_S)까지 재시도. actual>=expected면 통과
        (초과 시에도 통과 — 중복 적재 징후 WARN 판단은 호출자 책임)."""
        actual = self._count(table_dist, date)
        attempt = 1
        while actual < expected and attempt < self.cfg.retry_count:
            self.sleeper(self.cfg.retry_interval_s)
            actual = self._count(table_dist, date)
            attempt += 1
        return actual >= expected, actual

    def query(self, sql: str, params: dict | None = None) -> list[tuple]:
        """M0 커버리지·예산 선조회·인라인 검증용 범용 SELECT."""
        r = self.client.query(sql, parameters=params)
        return [tuple(row) for row in (r.result_rows or [])]

    def describe(self, table_dist: str) -> list[str]:
        """읽기 계약 프리플라이트용(설계 §6.1·§7.5) — DESCRIBE TABLE의 컬럼명을 선언 순으로.
        테이블 부재는 [] (EXISTS TABLE 선조회 — 드라이버 예외 메시지 파싱 없이 부재를 구분;
        preflight.missing_columns가 `<table>.*`로 보고). SELECT 권한은 SHOW TABLES/COLUMNS를
        함의하므로 GRANT(Plan 6a accounts.sql)로 충분하다."""
        found = self.query(f"EXISTS TABLE {table_dist}")
        if not found or not int(found[0][0]):
            return []
        return [str(row[0]) for row in self.query(f"DESCRIBE TABLE {table_dist}")]
```

- [ ] **Step 9: GREEN 확인**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_ch.py 2>&1 | tail -n 1
```

기대: `19 passed`.

- [ ] **Step 10: 실패하는 테스트 — `mart/token-metrics/tests/test_preflight.py`** (신규 — 설계 §6.1 읽기 계약 3테이블/13컬럼; DB명은 env 없이 기본값 `mart`/`gpu_data` 전제 — `test_ch.py::test_db_names_default_five`와 같은 전제)

```python
from app.ch import DB_TOKEN_DIM, DB_TOKEN_MART
from app.preflight import READ_CONTRACT, contract_tables, missing_columns


def _full() -> dict[str, list[str]]:
    """계약 그대로의 DESCRIBE 결과 흉내(사내 여분 컬럼 없음)."""
    return {table: list(cols) for table, cols in READ_CONTRACT.items()}


def test_contract_is_three_tables_thirteen_columns():
    assert len(READ_CONTRACT) == 3
    assert sum(len(v) for v in READ_CONTRACT.values()) == 13
    assert READ_CONTRACT["mart.token_usage_1d"] == (
        "date", "service_group", "service", "model",
        "input_tokens", "cache_read_tokens", "cache_creation_tokens", "output_tokens",
        "requests")
    assert READ_CONTRACT["mart.agg_token_service_1d"] == ("date", "service")
    assert READ_CONTRACT["gpu_data.dim_token_service"] == ("service", "enabled")
    # §5.6 로깅 계약과 무관하게, 계약에 user_id/user_type 등 개인 식별 컬럼은 없어야 한다
    assert not any(c.startswith("user_") for cols in READ_CONTRACT.values() for c in cols)


def test_contract_tables_use_token_db_constants_without_dist_suffix():
    tables = contract_tables()
    assert tables == [f"{DB_TOKEN_MART}.token_usage_1d",
                      f"{DB_TOKEN_MART}.agg_token_service_1d",
                      f"{DB_TOKEN_DIM}.dim_token_service"]
    assert all(t.count(".") == 1 for t in tables)                       # 'db.table'
    assert not any(t.endswith("_dist") or t.endswith("_local") for t in tables)
    assert tables == list(READ_CONTRACT)                                # 선언 순서 유지


def test_missing_columns_empty_when_superset():
    described = _full()
    described["mart.token_usage_1d"] += ["user_id", "user_type", "batch_time"]   # 사내 여분 컬럼
    described["gpu_data.dim_token_service"] = ["service_group", "service", "base_url",
                                               "enabled", "note"]               # 순서 무관
    described["mart.some_other_table"] = ["x"]                                   # 계약 밖 테이블 무시
    assert missing_columns(described) == []
    assert missing_columns(_full()) == []


def test_missing_columns_reports_table_and_column():
    described = _full()
    described["mart.token_usage_1d"].remove("requests")
    assert missing_columns(described) == ["mart.token_usage_1d.requests"]

    described = _full()
    del described["gpu_data.dim_token_service"]                # 테이블 키 부재
    assert missing_columns(described) == ["gpu_data.dim_token_service.*"]

    described = _full()
    described["gpu_data.dim_token_service"] = []               # CHGate.describe()의 부재 응답 []
    described["mart.agg_token_service_1d"].remove("service")
    described["mart.token_usage_1d"].remove("cache_read_tokens")
    assert missing_columns(described) == [                     # 정렬(테이블 → 컬럼)
        "gpu_data.dim_token_service.*",
        "mart.agg_token_service_1d.service",
        "mart.token_usage_1d.cache_read_tokens",
    ]
    assert missing_columns({}) == [f"{t}.*" for t in sorted(READ_CONTRACT)]
```

- [ ] **Step 11: RED 확인**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_preflight.py 2>&1 | grep -E "No module named|[0-9]+ error"
```

기대(3줄): `E   ModuleNotFoundError: No module named 'app.preflight'` / `Interrupted: 1 error during collection` / `1 error in …s`.

- [ ] **Step 12: 구현 — `mart/token-metrics/app/preflight.py`** (신규 — 순수 함수, I/O 없음)

```python
"""읽기 계약 프리플라이트 (설계 §6.1·§7.5) — 순수 함수(I/O 없음).

토큰 측 3테이블/13컬럼만 의존한다(그 외 기존 테이블·컬럼 의존 없음). 키는 `db.table`
(`_dist` 접미 없음) — 호출자(batch.preflight_or_fail(T5) / install.sh [3/6](T8))가
`f"{table}_dist"`로 DESCRIBE 한 결과를 이 키로 넘긴다. DB명은 app.ch의
DB_TOKEN_MART/DB_TOKEN_DIM(모듈 로드 시 1회 평가 — company-verify 격리 시 운영 DB).
install.sh의 bash 배열 READ_CONTRACT(13항목 `db.table_dist:column`)는
tests/test_install_contract.py(T8)가 이 dict와 동일함을 단언한다.
"""
from __future__ import annotations

from app.ch import DB_TOKEN_DIM, DB_TOKEN_MART

READ_CONTRACT: dict[str, tuple[str, ...]] = {
    f"{DB_TOKEN_MART}.token_usage_1d": (
        "date", "service_group", "service", "model",
        "input_tokens", "cache_read_tokens", "cache_creation_tokens", "output_tokens",
        "requests"),                                          # 9
    f"{DB_TOKEN_MART}.agg_token_service_1d": ("date", "service"),   # 2 — M0b 토큰 mart 존재 확인
    f"{DB_TOKEN_DIM}.dim_token_service": ("service", "enabled"),    # 2 — usage_svc 모집단
}


def contract_tables() -> list[str]:
    """계약 테이블 3개 — `db.table`(`_dist` 없음), READ_CONTRACT 선언 순."""
    return list(READ_CONTRACT)


def missing_columns(described: dict[str, list[str]]) -> list[str]:
    """DESCRIBE 결과 대조 → 누락 목록(정렬). 여분 컬럼·계약 밖 테이블 키는 무시한다.
    described[table]이 없거나 빈 리스트(테이블 부재 — CHGate.describe()가 []를 반환)면
    `<table>.*` 1항목으로 보고한다. 비어 있지 않은 반환 = 계약 위반 → 호출자가
    `PREFLIGHT FAIL read_contract missing=<a,b,...>` 로그 후 중단(설치 exit 3 / 배치 FAILURE)."""
    missing: list[str] = []
    for table, cols in READ_CONTRACT.items():
        have = described.get(table)
        if not have:
            missing.append(f"{table}.*")
            continue
        have_set = set(have)
        missing.extend(f"{table}.{col}" for col in cols if col not in have_set)
    return sorted(missing)
```

- [ ] **Step 13: 전체 GREEN + 문법·zero-diff 게이트**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q 2>&1 | tail -n 1
cd /home/mini/github/token-data-pipeline
python - <<'PY'
import ast, pathlib
for p in sorted(pathlib.Path("mart/token-metrics").rglob("*.py")):
    ast.parse(p.read_text(encoding="utf-8"), filename=str(p))
    src = p.read_text(encoding="utf-8")
    assert "import random" not in src and "StrEnum" not in src and "tomllib" not in src, p
print("AST_OK")
PY
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-mart.yml .github/workflows/test-collector.yml
git status --short mart/token-metrics
```

기대: `27 passed`(config 4 + ch 19 + preflight 4); `AST_OK`; `git diff --stat` 출력 없음(zero-diff); `git status --short`에는 이 태스크가 만든 경로의 untracked 항목만 — `?? mart/token-metrics/app/`, `?? mart/token-metrics/tests/`, `?? mart/token-metrics/conftest.py`, `?? mart/token-metrics/requirements.txt`, `?? mart/token-metrics/requirements-dev.txt` — 이고 ` M` 행은 0개(`mart/token-metrics/ddl/**`은 Plan 6a가 이미 커밋한 추적 파일이라 디렉터리 전체가 `?? mart/token-metrics/`로 뭉쳐 나오지 않는다).

- [ ] **Step 14: 커밋**

```bash
cd /home/mini/github/token-data-pipeline
git add mart/token-metrics/app/__init__.py mart/token-metrics/app/config.py mart/token-metrics/app/ch.py mart/token-metrics/app/preflight.py \
        mart/token-metrics/conftest.py mart/token-metrics/requirements.txt mart/token-metrics/requirements-dev.txt \
        mart/token-metrics/tests/__init__.py mart/token-metrics/tests/test_config.py mart/token-metrics/tests/test_ch.py mart/token-metrics/tests/test_preflight.py
git commit -m "feat(mart-metrics): 모듈 스캐폴드 — config/ch(DB 상수 5종·describe)/preflight 읽기 계약 13컬럼 (Plan 6c T1)

mart/token-usage 클론 + 델타: Config에서 EXPECTED_LATE_SERVICES·ORG_MAP_WARN_THRESHOLD 제거, MART_METRICS_MAX_MUTATIONS_PER_RUN(기본 64) 추가(설계 §4.0). app.ch에 DB_TOKEN_MART/DB_TOKEN_DIM(CH_DB_TOKEN_* 미설정 시 CH_DB_MART/CH_DB_DIM fallback — 설계 §6.1)과 describe(EXISTS TABLE 선조회 + DESCRIBE TABLE) 추가, insert_select settings 정확 일치 고정(distributed_product_mode=global). app.preflight READ_CONTRACT 3테이블/13컬럼 + missing_columns 순수 함수(§7.5 사내 프리플라이트).

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

### Task 2: app/mart.py 순수 로직 — 마커·커버리지·변이 예산 + 비용 모델 참조 구현 (정의서 §5.1/§5.2/§5.3 재현)

**설계 근거**: 설계 §6.1 306(마커 형식 `BATCH_RESULT status=… module=mart-metrics metrics_coverage=N/M missing_services="…" rows_mart= rows_check= rows_share= warn= elapsed=`), 300(M0 기대 집합 = `reg` enabled·coverage 유효 vs 앵커), 295(예산 초과 시 `FAILURE reason=mutation_budget`), §6.4 316-336((1) C = serving+standby × TCO·FAIL 제외·TCO 하나라도 NULL → NULL, (2) idle 클램프 0 + `over_report`·I2 항등식, (3) `W_UNC=1, W_CACHE=0.1, W_OUT=4`·uncached = input + cache_creation, (4) 부담 = C × W(s)/W(m)·I3/I4/I8, (5) p = C/W 파생 전용, (6) 사외 ③ `/1e6`, (7) 그룹 총비용 = 할당 × TCO); 정의서 `docs/cost-model-spec.md` §3 수식(57-150), §5 워크 예시(188-221: 5.1 Qwen3-32B 240,000원 배분, 5.2 p ≈ 0.00516원, 5.3 idle 0 → 16), §8 불변식 I1~I8(257-271), §9 의사코드(272-310); 마스터 §5.6 398-419(로그에 user_id 원문·페이로드 금지, `missing_services` 쌍따옴표).
**읽을 원형**: `mart/token-usage/app/mart.py:1-137`(digest §3 — `Coverage`/`Warn`/`compute_coverage`/`batch_line`/`target_dates`를 클론 후 델타), `mart/token-usage/tests/test_mart.py`(원본 직접 읽기 — 마커·`target_dates` 테스트 원형), Task 1 `app/ch.py`(`KST`).

이 태스크의 모듈은 **I/O가 없는 순수 로직**이다. `batch.py`(T5)가 마커·커버리지·예산 판정을 소비하고, `steps.py`(T3/T4/T6/T7)가 `W_*`/`FAIL_FLAGS`/`DENOMINATOR_MODES` 상수를 SQL 문자열에 삽입하며, e2e `mart_expectations.py`(T10)가 **비용 모델 참조 구현**(`model_cost`/`allocate_shared`/`group_overhead` …)으로 기대값을 계산해 SQL 결과와 대조한다 — 즉 참조 구현은 SQL과 **동일한 규칙**을 Python으로 적은 것이며(정의서 §9 의사코드의 파이프라인 보정판), 단위 테스트가 정의서 §5 워크 예시 값을 그대로 재현한다.

**Files:**
- Create: `mart/token-metrics/app/mart.py`
- Test: `mart/token-metrics/tests/test_mart.py`
- (읽기만) `mart/token-usage/app/mart.py`, `mart/token-usage/tests/test_mart.py` — zero-diff 대상, 절대 수정하지 않는다.

**Interfaces:**
- Consumes:
  - `app.ch.KST`(Task 1 — `timezone(timedelta(hours=9))`; `mart.py`는 자체 `KST`를 정의하지 않고 이것을 import 한다).
  - Task 1 `conftest.py`(모듈 루트 `sys.path`) + `tests/__init__.py` — `cd mart/token-metrics && python -m pytest -q` 실행 전제.
- Produces (`app.mart` — 크로스 태스크 표 그대로; 아래 시그니처가 정본):
  - `Coverage(enabled: int, present: int, missing: list[str], warn_targets: list[str])` — dataclass.
  - `Warn(count: int, text: str = "")` — dataclass, `__add__` 지원(원형 클론; 표의 `Warn(count)`는 `text` 기본값 `""`로 성립).
  - `compute_coverage(expected_services, anchor_services, expected_late) -> Coverage` — `missing = sorted(set(expected) - set(anchor))`, `present = len(set(expected) & set(anchor))`, `warn_targets = sorted(missing - set(expected_late))`(batch는 `expected_late=[]`).
  - `batch_line(status: str, coverage: Coverage, rows_mart: int, rows_check: int, rows_share: int, warn_count: int, elapsed_s: float, reason: str = "") -> str` — 정확히 `BATCH_RESULT status={status} module=mart-metrics metrics_coverage={present}/{enabled} missing_services="{a,b|-}" rows_mart={n} rows_check={n} rows_share={n} warn={n} elapsed={s:.1f}` + (`reason` 비어있지 않으면) ` reason={reason}`.
  - `target_dates(args) -> tuple[list[str] | None, bool]` — 원형 그대로(`--from/--to` 쌍·inclusive, naive `batch_time`은 KST, 기본 = 어제 KST).
  - `mutation_budget_exceeded(planned: int, budget: int) -> bool` — `planned > budget`(64 == 64 → False, 65 > 64 → True).
  - `FAIL_FLAGS = ("hours_over_count", "unknown_violation")`, `is_fail(flags) -> bool`.
  - `W_UNC = 1.0`, `W_CACHE = 0.1`, `W_OUT = 4.0`, `weighted_tokens(input_tokens, cache_read, cache_creation, output) -> float` = `W_UNC * (input + cache_creation) + W_CACHE * cache_read + W_OUT * output`.
  - `model_cost(gpu_rows: list[tuple[str, str, float, list[str]]], tco: dict[str, float | None]) -> float | None` — `gpu_rows` 원소 = `(category, gpu_type, gpu_hours, flags)`; serving/standby AND not FAIL 행만 합산; 그 행 중 `tco.get(gpu_type) is None`이 하나라도 있으면 `None`; `gpu_rows`가 비어 있으면 `None`(SQL `has_gpu_rows = 0 → NULL`과 동일).
  - `allocate_shared(cost: float, wtokens: dict[str, float]) -> dict[str, float]` — `total == 0 → {}`(I8: 호출측이 `token_not_reported` 처리), 아니면 `{s: cost * w / total}`(I3/I4).
  - `external_api_cost(input_tokens, cache_read, cache_creation, output, price: tuple[float | None, float | None, float | None, float | None]) -> float | None` — `price = (p_in, p_cached, p_cc, p_out)` 원/1M; 하나라도 None → None; 아니면 `(i*p_in + cr*p_cached + cc*p_cc + o*p_out) / 1e6`.
  - `group_overhead(allocated_gpu_hours: float | None, reported_total: float, serving: float, standby: float, test: float, flagged: float, tco: float | None) -> dict` — 키 9종 `group_total_cost_krw, model_cost_sum_krw, test_cost_krw, idle_gpu_hours, idle_cost_krw, unattributed_cost_krw, identity_gap_krw, utilization, over_report`.
  - `M1_FLAG_PRIORITY = ("partial", "no_tco", "flagged", "manual", "no_metrics", "consumer_only", "normal")`, `quality_flag_m1(partial, no_tco, flagged, manual, no_metrics, consumer_only) -> str`.
  - `DENOMINATOR_MODES = ("all_services", "provider_reported", "token_not_reported", "no_provider", "provider_ambiguous", "external_api")`.

- [ ] **Step 1: 원형 확인 + RED — 마커·커버리지·날짜창·예산 테스트 작성**

원형을 먼저 읽어 델타를 눈으로 확인한다(수정 금지):

```bash
sed -n 1,137p /home/mini/github/token-data-pipeline/mart/token-usage/app/mart.py
sed -n 100,135p /home/mini/github/token-data-pipeline/mart/token-usage/tests/test_mart.py
```

기대: 원형 `batch_line(status, coverage, rows_mart, rows_view, warn_count, elapsed_s)`가 `module=mart-token coverage=N/M … rows_view=`를 출력한다 — 6c 델타는 `module=mart-metrics metrics_coverage=N/M … rows_check= rows_share=` + 말미 `reason=`(선택)이다.

`mart/token-metrics/tests/test_mart.py`를 아래 내용으로 생성한다(1부 — 마커·커버리지·날짜창·예산; 2부는 Step 4에서 append):

```bash
cat > /home/mini/github/token-data-pipeline/mart/token-metrics/tests/test_mart.py <<'PYEOF'
"""Tests for app/mart.py — 순수 로직 (커버리지·마커·날짜창·뮤테이션 예산 + 비용 모델 참조 구현).

원형: mart/token-usage/tests/test_mart.py (마커 필드명·module 값·reason 접미가 6c 델타).
정의서 docs/cost-model-spec.md §5.1/§5.2/§5.3 워크 예시 값을 그대로 재현한다.
"""
import argparse
from datetime import date as date_cls, datetime, timedelta, timezone

import pytest

from app.ch import KST
from app.mart import (
    Coverage,
    Warn,
    batch_line,
    compute_coverage,
    mutation_budget_exceeded,
    target_dates,
)


# ============================================================================
# compute_coverage
# ============================================================================

def test_compute_coverage_missing_sorted_and_present_count():
    """missing = expected − anchor (정렬), present = |expected ∩ anchor|; anchor는 list여도 된다."""
    c = compute_coverage(["Mock Service B", "Mock Service A", "Mock Service C"],
                         ["Mock Service A"], [])
    assert c.enabled == 3
    assert c.present == 1
    assert c.missing == ["Mock Service B", "Mock Service C"]
    assert c.warn_targets == ["Mock Service B", "Mock Service C"]


def test_compute_coverage_expected_late_excluded_from_warn_targets_only():
    """expected_late는 warn_targets에서만 제외 — 마커 missing에는 전부 노출 (batch는 []을 넘긴다)."""
    c = compute_coverage(["S1", "S2", "S3"], {"S1"}, expected_late=["S3"])
    assert (c.enabled, c.present) == (3, 1)
    assert c.missing == ["S2", "S3"]
    assert c.warn_targets == ["S2"]


def test_compute_coverage_empty_expected_is_zero_over_zero():
    """기대 집합이 비면 0/0 — no-metrics day라도 예외 없이 Coverage를 만든다 (§6.1: 절대 FAILURE 아님)."""
    c = compute_coverage([], [], [])
    assert (c.enabled, c.present, c.missing, c.warn_targets) == (0, 0, [], [])


# ============================================================================
# batch_line — 설계 §6.1 306 마커 형식 (날짜당 정확히 1줄)
# ============================================================================

def test_batch_line_exact_format():
    """필드 순서·이름·module 값이 설계 §6.1과 문자 단위로 일치한다."""
    cov = Coverage(enabled=2, present=1, missing=["Mock Service B"], warn_targets=["Mock Service B"])
    line = batch_line("SUCCESS", cov, 3, 5, 0, 1, 12.34)
    assert line == (
        'BATCH_RESULT status=SUCCESS module=mart-metrics metrics_coverage=1/2 '
        'missing_services="Mock Service B" rows_mart=3 rows_check=5 rows_share=0 warn=1 elapsed=12.3'
    )


def test_batch_line_reason_suffix():
    """reason이 있으면 말미에 ` reason=<r>`; missing 없음은 `missing_services="-"`."""
    cov = Coverage(enabled=2, present=2, missing=[], warn_targets=[])
    line = batch_line("FAILURE", cov, 0, 0, 0, 0, 0.04, reason="mutation_budget")
    assert line.endswith(" reason=mutation_budget")
    assert 'missing_services="-"' in line
    assert "status=FAILURE" in line and "metrics_coverage=2/2" in line
    assert "elapsed=0.0 reason=mutation_budget" in line
    # reason 미지정이면 접미가 붙지 않는다
    assert " reason=" not in batch_line("SUCCESS", cov, 1, 1, 1, 0, 1.0)


def test_batch_line_missing_with_spaces_quoted_and_comma_joined():
    """서비스명 공백은 쌍따옴표로 보호, 복수는 콤마 결합 (마스터 §5.6 v1.10)."""
    cov = Coverage(enabled=3, present=1, missing=["Mock Service B", "S3"], warn_targets=["Mock Service B", "S3"])
    line = batch_line("SUCCESS", cov, 0, 0, 0, 0, 1.0)
    assert 'missing_services="Mock Service B,S3"' in line


def test_batch_line_never_contains_user_id_or_payload():
    """마스터 §5.6 로깅 계약 — 마커에는 카운트·서비스명만 (user_id 원문·페이로드 부재)."""
    cov = Coverage(enabled=1, present=1, missing=[], warn_targets=[])
    line = batch_line("SUCCESS", cov, 10, 20, 30, 2, 99.99, reason="")
    assert "user_id" not in line
    assert "payload" not in line
    assert line.count("BATCH_RESULT") == 1
    assert "elapsed=100.0" in line


# ============================================================================
# target_dates — 원형 계약 (수집기 _target_dates와 동일)
# ============================================================================

def test_target_dates_range_inclusive_and_default_yesterday_kst():
    """--from/--to inclusive; 인자 없음 = 어제(KST); naive batch_time은 KST로 해석."""
    args = argparse.Namespace(from_date="2026-09-01", to_date="2026-09-03", batch_time=None)
    assert target_dates(args) == (["2026-09-01", "2026-09-02", "2026-09-03"], True)

    args = argparse.Namespace(from_date=None, to_date=None, batch_time="2026-09-04T10:20:00")
    assert target_dates(args) == (["2026-09-03"], False)

    before = datetime.now(KST).date() - timedelta(days=1)
    dates, is_rerun = target_dates(argparse.Namespace(from_date=None, to_date=None, batch_time=None))
    after = datetime.now(KST).date() - timedelta(days=1)
    assert is_rerun is False and len(dates) == 1
    assert dates[0] in {str(before), str(after)}
    assert isinstance(date_cls.fromisoformat(dates[0]), date_cls)


def test_target_dates_pair_required_and_aware_utc_converted_to_kst():
    """--from 단독은 (None, False); aware UTC는 KST로 변환 후 어제."""
    assert target_dates(argparse.Namespace(from_date="2026-09-01", to_date=None, batch_time=None)) == (None, False)
    assert target_dates(argparse.Namespace(from_date=None, to_date="2026-09-03", batch_time=None)) == (None, False)
    dt_utc = datetime(2026, 9, 4, 1, 20, tzinfo=timezone.utc)  # = 2026-09-04 10:20 KST
    args = argparse.Namespace(from_date=None, to_date=None, batch_time=dt_utc.isoformat())
    assert target_dates(args) == (["2026-09-03"], False)


# ============================================================================
# mutation_budget_exceeded — 설계 §4.0 129 (기본 64, 날짜당 ≤4 → 16일 = 64 통과, 17일 = 68 초과)
# ============================================================================

def test_mutation_budget_exceeded_boundary():
    assert mutation_budget_exceeded(64, 64) is False
    assert mutation_budget_exceeded(65, 64) is True
    assert mutation_budget_exceeded(0, 64) is False
    assert mutation_budget_exceeded(17 * 4, 64) is True
    assert mutation_budget_exceeded(16 * 4, 64) is False


# ============================================================================
# Warn — 원형 클론 (count + text, text 기본 "")
# ============================================================================

def test_warn_add_and_default_text_and_no_user_id():
    w = Warn(count=2, text="CHECK WARN metrics_coverage missing=2") + Warn(count=1)
    assert w.count == 3
    assert w.text == "CHECK WARN metrics_coverage missing=2"
    assert (Warn(count=1, text="a") + Warn(count=1, text="b")).text == "a\nb"
    assert "user_id" not in w.text
PYEOF
```

실행:

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_mart.py
```

기대 출력(RED — 모듈 부재):

```
ImportError while importing test module '.../mart/token-metrics/tests/test_mart.py'.
...
ModuleNotFoundError: No module named 'app.mart'
```

- [ ] **Step 2: GREEN — `app/mart.py` 1부(원형 클론 + 델타: 마커·`mutation_budget_exceeded`)**

`mart/token-metrics/app/mart.py`를 아래 내용으로 생성한다(원형 `mart/token-usage/app/mart.py:1-137` 클론; 델타 = 모듈 docstring, `from __future__ import annotations`, `KST`를 `app.ch`에서 import, `Warn.text` 기본값 `""`, `compute_coverage` 인자명·`set()` 정규화, `batch_line` 서식, `mutation_budget_exceeded` 신설. 비용 모델 2부는 Step 5에서 append):

```bash
mkdir -p /home/mini/github/token-data-pipeline/mart/token-metrics/app
cat > /home/mini/github/token-data-pipeline/mart/token-metrics/app/mart.py <<'PYEOF'
"""순수 로직 — coverage 게이트, 마커, 날짜 윈도우, 뮤테이션 예산 판정, 비용 모델 참조 구현.

I/O 금지 (클라이언트, 네트워크, 시계 부작용 없음). 마스터 §5.6 로깅 계약: user_id 원문·페이로드 미포함.

원형 mart/token-usage/app/mart.py 클론 — 6c 델타:
  - 마커(설계 §6.1): `module=mart-metrics`, `metrics_coverage=N/M`, `rows_mart/rows_check/rows_share`,
    말미 선택 ` reason=<r>`(mutation_budget / read_contract / <StepError.reason> / exception / sigterm).
  - `mutation_budget_exceeded(planned, budget)`: 첫 `_run_table` 전 예정 DELETE 합산 > 예산(기본 64)이면
    실행 없이 `FAILURE reason=mutation_budget`(설계 §6.1, §4.0 뮤테이션 장부 — 날짜당 ≤4).
  - 비용 모델 참조 구현(설계 §6.4 = 정의서 docs/cost-model-spec.md §3/§9): steps.py의 SQL과 동일 규칙을
    Python으로 적은 것. e2e mart_expectations.py가 이 함수들로 기대값을 만들어 SQL 결과와 대조한다.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date as date_cls, datetime, timedelta

from app.ch import KST


@dataclass
class Coverage:
    """Coverage state: enabled count, present count, missing services, warn targets."""
    enabled: int             # M0 기대 서비스 수 (reg enabled=1 AND coverage_since ≤ d AND (until IS NULL OR d ≤ until))
    present: int             # 기대 중 앵커(summary)에 있는 서비스 수
    missing: list[str]       # 기대 중 앵커에 없는 서비스 (정렬) — 마커 missing_services에 전부 노출
    warn_targets: list[str]  # missing 중 expected_late에 없는 것 (정렬) — 6c batch는 expected_late=[]


@dataclass
class Warn:
    """Warning aggregator: count + text (no user_id in text per §5.6)."""
    count: int
    text: str = ""

    def __add__(self, other: "Warn") -> "Warn":
        """Combine two Warn objects."""
        if not self.text:
            combined_text = other.text
        elif not other.text:
            combined_text = self.text
        else:
            combined_text = f"{self.text}\n{other.text}"
        return Warn(count=self.count + other.count, text=combined_text)


def compute_coverage(
    expected_services,
    anchor_services,
    expected_late,
) -> Coverage:
    """
    Compute coverage state (설계 §6.1 M0).

    expected_services: reg에서 그날 커버리지가 기대되는 서비스 (list/set 무관)
    anchor_services:   그날 앵커(raw_token_metrics_summary_1d)에 있는 서비스 (list/set 무관)
    missing = expected - anchor (sorted)
    warn_targets = missing - expected_late (sorted)
    """
    expected_set = set(expected_services)
    anchor_set = set(anchor_services)
    missing_set = expected_set - anchor_set
    missing = sorted(missing_set)
    expected_late_set = set(expected_late)
    warn_targets = sorted(missing_set - expected_late_set)

    return Coverage(
        enabled=len(expected_set),
        present=len(expected_set & anchor_set),
        missing=missing,
        warn_targets=warn_targets,
    )


def batch_line(
    status: str,
    coverage: Coverage,
    rows_mart: int,
    rows_check: int,
    rows_share: int,
    warn_count: int,
    elapsed_s: float,
    reason: str = "",
) -> str:
    """
    Format batch result marker line (설계 §6.1 — 날짜당 정확히 1줄).

    Format: BATCH_RESULT status=<S> module=mart-metrics metrics_coverage=N/M
    missing_services="..." rows_mart=<n> rows_check=<n> rows_share=<n> warn=<n> elapsed=<sec, 1 decimal>
    [ reason=<r>]

    missing_services value is always double-quoted (마스터 §5.6 v1.10 — 서비스명 공백 보호).
    Empty missing list renders as "-". reason은 비어있지 않을 때만 말미에 붙는다.
    """
    if coverage.missing:
        missing_str = ",".join(coverage.missing)
    else:
        missing_str = "-"

    # metrics_coverage=N/M where N = present (앵커에 있는 기대 서비스), M = enabled (기대 서비스 전체)
    coverage_display = f"{coverage.present}/{coverage.enabled}"

    # Format elapsed to 1 decimal place
    elapsed_display = f"{elapsed_s:.1f}"

    line = (
        f"BATCH_RESULT status={status} module=mart-metrics metrics_coverage={coverage_display} "
        f'missing_services="{missing_str}" rows_mart={rows_mart} rows_check={rows_check} '
        f"rows_share={rows_share} warn={warn_count} elapsed={elapsed_display}"
    )
    if reason:
        line += f" reason={reason}"
    return line


def target_dates(args) -> tuple[list[str] | None, bool]:
    """
    Parse CLI args for target date(s).

    Returns (dates, is_rerun) where:
    - dates: list of YYYY-MM-DD strings (inclusive range), or None if args invalid
    - is_rerun: True if multi-date range (--from/--to), False otherwise

    Contract matches collectors' _target_dates:
    - --from/--to must be paired, YYYY-MM-DD, inclusive
    - naive datetime interpreted as KST
    - aware datetime converted to KST
    - default: batch_time = now(KST), target_date = yesterday
    """
    if args.from_date or args.to_date:
        # --from/--to must be paired
        if not (args.from_date and args.to_date):
            print("--from/--to는 쌍으로 지정 (KST, YYYY-MM-DD)", file=sys.stderr)
            return None, False

        d0 = date_cls.fromisoformat(args.from_date)
        d1 = date_cls.fromisoformat(args.to_date)
        # Inclusive range: (d1 - d0).days + 1
        dates = [str(d0 + timedelta(days=i)) for i in range((d1 - d0).days + 1)]
        return dates, True

    # Parse batch_time (default to now(KST))
    if args.batch_time:
        parsed = datetime.fromisoformat(args.batch_time)
        if parsed.tzinfo is None:
            # naive input is interpreted as KST (§5.1)
            parsed = parsed.replace(tzinfo=KST)
        batch_time = parsed.astimezone(KST)
    else:
        batch_time = datetime.now(KST)

    # target_date = batch_time - 1 day
    target_date = batch_time.date() - timedelta(days=1)
    return [str(target_date)], False


def mutation_budget_exceeded(planned: int, budget: int) -> bool:
    """
    뮤테이션 예산 선검사 (설계 §6.1, §4.0 장부).

    planned = 대상 날짜 전체 × MART_TABLES 4테이블의 `exists` 합산(= 예정 DELETE 수, 날짜당 ≤4).
    budget  = Config.max_mutations_per_run (MART_METRICS_MAX_MUTATIONS_PER_RUN, 기본 64 → 16일 rerun까지 통과).
    초과(planned > budget)면 batch는 변이 0으로 모든 날짜 `FAILURE reason=mutation_budget` (rerun은 --chunk-days 7).
    """
    return planned > budget
PYEOF
```

실행:

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_mart.py
```

기대 출력(GREEN):

```
...........                                                              [100%]
11 passed in 0.0Xs
```

- [ ] **Step 3: zero-diff 확인 + 1부 커밋**

```bash
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-mart.yml .github/workflows/test-collector.yml
```

기대 출력: (비어 있음 — zero-diff 목록 무변경).

```bash
cd /home/mini/github/token-data-pipeline && git add mart/token-metrics/app/mart.py mart/token-metrics/tests/test_mart.py && git commit -m "feat(mart-metrics): mart.py 1부 — 마커 module=mart-metrics·metrics_coverage·reason 접미 + mutation_budget_exceeded (Plan 6c T2)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

- [ ] **Step 4: RED — 비용 모델 참조 구현 테스트 append (정의서 §5.1/§5.2/§5.3 재현)**

`tests/test_mart.py` 말미에 2부를 append 한다(별도 import 문 — 첫 미정의 이름이 `FAIL_FLAGS`이므로 RED 메시지가 고정된다):

```bash
cat >> /home/mini/github/token-data-pipeline/mart/token-metrics/tests/test_mart.py <<'PYEOF'


# ============================================================================
# 비용 모델 참조 구현 — 설계 §6.4 (1)~(7) = 정의서 §3 수식 / §5 워크 예시 / §8 불변식 / §9 의사코드
# ============================================================================

from app.mart import (  # noqa: E402 — 2부 import (1부와 분리해 RED 메시지 고정)
    FAIL_FLAGS,
    DENOMINATOR_MODES,
    M1_FLAG_PRIORITY,
    W_CACHE,
    W_OUT,
    W_UNC,
    allocate_shared,
    external_api_cost,
    group_overhead,
    is_fail,
    model_cost,
    quality_flag_m1,
    weighted_tokens,
)


def test_weight_constants_are_1_0_1_4():
    """§6.4 (3): TCO 팀 승인값 정본 1 : 0.1 : 4 — steps.py가 SQL 문자열에 그대로 삽입한다."""
    assert (W_UNC, W_CACHE, W_OUT) == (1.0, 0.1, 4.0)
    assert all(isinstance(w, float) for w in (W_UNC, W_CACHE, W_OUT))
    # §6.1 M4 denominator_mode 6종 — 순서 고정 (steps.py의 multiIf 분기 순서·DDL COMMENT와 동일)
    assert DENOMINATOR_MODES == (
        "all_services", "provider_reported", "token_not_reported",
        "no_provider", "provider_ambiguous", "external_api",
    )


def test_is_fail_flags_hours_over_count_and_unknown_violation_only():
    """§6.4 (1) 파이프라인 보정: FAIL 플래그 2종만 C에서 제외 — 다른 플래그(pct_non_monotone 등)는 C 포함."""
    assert FAIL_FLAGS == ("hours_over_count", "unknown_violation")
    assert is_fail(["hours_over_count"]) is True
    assert is_fail(["pct_non_monotone", "unknown_violation"]) is True
    assert is_fail(["pct_non_monotone"]) is False
    assert is_fail([]) is False


def test_cost_spec_5_1_qwen_allocation_preserves_total():
    """정의서 §5.1: Qwen3-32B A100×2, serving 44 + standby 4, 단가 5,000원 → C = 240,000원.
    W: HR 챗봇 14M, 문서 요약 28M, 코딩 도우미 2M → 배분 76,364 / 152,727 / 10,909 (합 240,000 — I3)."""
    gpu_rows = [("serving", "A100", 44.0, []), ("standby", "A100", 4.0, [])]
    cost = model_cost(gpu_rows, {"A100": 5000.0})
    assert cost == 240000.0

    wtokens = {
        "HR 챗봇": weighted_tokens(10e6, 0, 0, 1e6),
        "문서 요약": weighted_tokens(20e6, 0, 0, 2e6),
        "코딩 도우미": weighted_tokens(1e6, 0, 0, 0.25e6),
    }
    assert wtokens == {"HR 챗봇": 14e6, "문서 요약": 28e6, "코딩 도우미": 2e6}

    alloc = allocate_shared(cost, wtokens)
    assert {s: round(v) for s, v in alloc.items()} == {"HR 챗봇": 76364, "문서 요약": 152727, "코딩 도우미": 10909}
    assert abs(sum(alloc.values()) - 240000.0) < 1  # I3: Σ_s 부담 = C (±1원)


def test_cost_spec_5_2_token_price_derivation():
    """정의서 §5.2: Llama-70B H100×4, 96 GPU·h, 단가 5,000원 → C = 480,000원;
    uncached 50M, cached 30M, output 10M → W = 50 + 3 + 40 = 93M; p = C/W ≈ 0.00516원/가중토큰.
    정의서 표기(5,160 / 516 / 20,600 원/1M)는 근사 — 허용오차로 단언, 검산은 정확히."""
    cost = model_cost([("serving", "H100", 96.0, [])], {"H100": 5000.0})
    assert cost == 480000.0
    w_model = weighted_tokens(50e6, 30e6, 0, 10e6)
    assert w_model == 93e6

    p = cost / w_model
    assert abs(p * 1e6 - 5160) < 2          # p_uncached ≈ 5,160원/1M (정확값 5161.29)
    assert abs(0.1 * p * 1e6 - 516) < 1     # p_cached ≈ 516원/1M
    assert abs(4 * p * 1e6 - 20600) < 50    # p_output ≈ 20,600원/1M (정확값 20645.16)
    # 검산: p × 토큰을 다시 더하면 C (순환 — 비용 입력이 아님, §6.4 (5))
    assert abs(50e6 * p + 30e6 * 0.1 * p + 10e6 * 4 * p - 480000.0) < 1e-6


def test_cost_spec_5_3_group_idle_zero_then_sixteen():
    """정의서 §5.3: 할당 H100 120 GPU·h/일. serving 96 + standby 24 → idle 0, 그룹 총비용 = C.
    다음 날 serving 80 + standby 24 → idle 16 → 유휴 비용 16 × 5,000 = 80,000원 (I2 항등식 gap 0)."""
    day1 = group_overhead(120, 120, 96, 24, 0, 0, 5000)
    assert day1["idle_gpu_hours"] == 0.0
    assert day1["identity_gap_krw"] == 0.0
    assert day1["over_report"] == 0
    assert day1["group_total_cost_krw"] == 600000.0
    assert day1["model_cost_sum_krw"] == 600000.0
    assert day1["utilization"] == 1.0

    day2 = group_overhead(120, 104, 80, 24, 0, 0, 5000)
    assert day2["idle_gpu_hours"] == 16.0
    assert day2["idle_cost_krw"] == 80000.0
    assert day2["identity_gap_krw"] == 0.0
    assert day2["test_cost_krw"] == 0.0 and day2["unattributed_cost_krw"] == 0.0
    assert abs(day2["utilization"] - 104 / 120) < 1e-12


def test_model_cost_null_when_any_tco_missing_and_excludes_test_and_fail():
    """§6.4 (1): serving+standby만, FAIL 행 제외, TCO 기종 하나라도 NULL이면 C NULL(부분 합 금지)."""
    rows = [
        ("serving", "H100", 10, []),
        ("serving", "B200", 1, []),                    # TCO 미등록 → 전체 NULL
        ("test", "H100", 5, []),                       # test는 C 불포함 (그룹 귀속)
        ("serving", "H100", 7, ["hours_over_count"]),  # FAIL → C 제외 (unattributed로)
    ]
    assert model_cost(rows, {"H100": 4200}) is None
    assert model_cost([r for r in rows if r[1] != "B200"], {"H100": 4200}) == 42000.0
    # 명시적 None 단가도 NULL 전파 (dim 최신 이력 행이 NULL인 경우)
    assert model_cost(rows, {"H100": 4200, "B200": None}) is None
    # gpu 행 없음 → NULL (has_gpu_rows=0); test-only → 0.0 (no_provider — C=0)
    assert model_cost([], {"H100": 4200}) is None
    assert model_cost([("test", "H100", 5, [])], {"H100": 4200}) == 0.0


def test_external_api_cost_formula_and_null():
    """§6.4 (6)/정의서 3.9: ③ = (input×p_in + cache_read×p_cached + cache_creation×p_cc + output×p_out)/1e6;
    여기서 input은 cache_creation을 제외한 순수 입력(3.5 uncached와 혼용 금지). 단가 하나라도 NULL → NULL."""
    price = (4050.0, 405.0, 5062.5, 20250.0)  # 원/1M — tier='standard'
    assert external_api_cost(1e6, 2e6, 0.5e6, 1e6, price) == 4050 + 810 + 2531.25 + 20250
    assert external_api_cost(1e6, 2e6, 0.5e6, 1e6, price) == 27641.25
    assert external_api_cost(1e6, 2e6, 0.5e6, 1e6, (4050.0, None, 5062.5, 20250.0)) is None
    assert external_api_cost(0, 0, 0, 0, price) == 0.0


def test_allocate_shared_dedicated_share_one_and_zero_total_empty():
    """I4: 전용 모델은 전액 귀속(share=1); I8: W(m)=0이면 {} — 호출측이 token_not_reported 처리(호스팅 그룹 귀속)."""
    assert allocate_shared(240000.0, {"only": 7e6}) == {"only": 240000.0}
    assert allocate_shared(240000.0, {}) == {}
    assert allocate_shared(240000.0, {"a": 0.0, "b": 0.0}) == {}
    assert allocate_shared(0.0, {"a": 1e6, "b": 3e6}) == {"a": 0.0, "b": 0.0}


def test_group_overhead_over_report_clamps_idle_and_null_propagation():
    """I1: 보고 > 할당이면 idle 0 클램프 + over_report=1(identity_gap ≠ 0로 드러남);
    할당 없음 → idle/utilization/group_total/identity_gap NULL; TCO 없음 → 비용 키 전부 NULL."""
    over = group_overhead(100, 120, 96, 24, 0, 0, 5000)
    assert over["idle_gpu_hours"] == 0.0 and over["over_report"] == 1
    assert over["identity_gap_krw"] == -100000.0
    assert abs(over["utilization"] - 1.2) < 1e-12

    no_alloc = group_overhead(None, 120, 96, 24, 0, 0, 5000)
    assert (no_alloc["idle_gpu_hours"], no_alloc["utilization"], no_alloc["group_total_cost_krw"],
            no_alloc["identity_gap_krw"], no_alloc["idle_cost_krw"]) == (None, None, None, None, None)
    assert no_alloc["over_report"] == 0
    assert no_alloc["model_cost_sum_krw"] == 600000.0  # C 합은 할당 없이도 계산된다

    no_tco = group_overhead(120, 120, 96, 24, 0, 0, None)
    for key in ("group_total_cost_krw", "model_cost_sum_krw", "test_cost_krw",
                "idle_cost_krw", "unattributed_cost_krw", "identity_gap_krw"):
        assert no_tco[key] is None
    assert no_tco["idle_gpu_hours"] == 0.0 and no_tco["over_report"] == 0

    # e2e 시드 값(T10): H100 그룹 — 할당 192, 보고 120 = serving 60 + standby 8 + test 2 + flagged 50
    e2e = group_overhead(192.0, 120.0, 60.0, 8.0, 2.0, 50.0, 4200.0)
    assert e2e["idle_gpu_hours"] == 72.0
    assert e2e["identity_gap_krw"] == 0.0  # 806,400 − 285,600 − 8,400 − 302,400 − 210,000
    assert set(e2e) == {
        "group_total_cost_krw", "model_cost_sum_krw", "test_cost_krw", "idle_gpu_hours",
        "idle_cost_krw", "unattributed_cost_krw", "identity_gap_krw", "utilization", "over_report",
    }


def test_quality_flag_m1_priority():
    """§6.1 M1 quality_flag 우선순위 고정: partial > no_tco > flagged > manual > no_metrics > consumer_only > normal."""
    assert M1_FLAG_PRIORITY == ("partial", "no_tco", "flagged", "manual", "no_metrics", "consumer_only", "normal")
    assert M1_FLAG_PRIORITY[-1] == "normal"
    assert quality_flag_m1(True, True, True, True, True, True) == "partial"
    assert quality_flag_m1(False, True, True, True, True, True) == "no_tco"
    assert quality_flag_m1(False, False, True, True, True, True) == "flagged"
    assert quality_flag_m1(False, False, False, True, True, False) == "manual"
    assert quality_flag_m1(False, False, False, False, True, True) == "no_metrics"
    assert quality_flag_m1(False, False, False, False, False, True) == "consumer_only"
    assert quality_flag_m1(False, False, False, False, False, False) == "normal"
PYEOF
```

실행:

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_mart.py
```

기대 출력(RED — 2부 이름 부재):

```
ImportError while importing test module '.../mart/token-metrics/tests/test_mart.py'.
...
ImportError: cannot import name 'FAIL_FLAGS' from 'app.mart' (.../mart/token-metrics/app/mart.py)
```

- [ ] **Step 5: GREEN — `app/mart.py` 2부 append (비용 모델 참조 구현 = 정의서 §9 의사코드의 파이프라인 보정판)**

`app/mart.py` 말미에 append 한다(정의서 §9 `model_cost`/`group_overhead`/`weighted_tokens`/`allocate_shared`를 설계 §6.4의 보정 — FAIL 제외·TCO NULL 전파·idle 클램프·unattributed — 을 넣어 SQL(T3 `SQL_M1`, T6 `SQL_M4`, T7 `SQL_M2`)과 동일 규칙으로 옮긴 것):

```bash
cat >> /home/mini/github/token-data-pipeline/mart/token-metrics/app/mart.py <<'PYEOF'


# ============================================================================
# 비용 모델 참조 구현 — 설계 §6.4 (1)~(7) = 정의서 docs/cost-model-spec.md §3/§9
#
# steps.py의 SQL과 동일 규칙(단위 테스트 = 정의서 §5 워크 예시, e2e = mart_expectations.py 기대값).
# 정의서 §9 의사코드 대비 파이프라인 보정: FAIL 플래그 행 제외(→ unattributed), TCO NULL 전파(부분 합 금지),
# idle 음수 클램프 0 + over_report(I1), 사외 API ③ /1e6, M1 quality_flag 우선순위, M4 분모 모드 6종.
# ============================================================================

# §6.4 (1) 파이프라인 보정 — 물리적으로 불가능(hours_over_count)하거나 모델 귀속 불가(unknown_violation)인 행은
# C에서 제외하고 그룹 행 unattributed_cost_krw로 노출. steps.FAIL_PRED가 이 튜플로 hasAny(...) 문자열을 만든다.
FAIL_FLAGS = ("hours_over_count", "unknown_violation")

# §6.4 (3) 가중 토큰 W — TCO 팀 승인값 정본(변경 시 상수 교체 + mart rerun). steps.py가 SQL 문자열에 삽입.
W_UNC = 1.0
W_CACHE = 0.1
W_OUT = 4.0

# §6.1 M1 quality_flag 우선순위 (multiIf 분기 순서와 동일, 마지막이 기본값)
M1_FLAG_PRIORITY = ("partial", "no_tco", "flagged", "manual", "no_metrics", "consumer_only", "normal")

# §6.1 M4 denominator_mode 6종 (순서 고정 — steps.py multiIf·DDL COMMENT와 동일)
DENOMINATOR_MODES = (
    "all_services",        # W(m) = Σ usage_svc 전 서비스 (기본)
    "provider_reported",   # usage_includes_consumers=1: W(m) = W(provider), 제공자 자기분 = max(W(m) − Σ_{s≠p} W(s), 0)
    "token_not_reported",  # W(m)=0 AND C>0: 제공자 행 share=1 전액 (I8 — 호스팅 그룹 귀속)
    "no_provider",         # gpu 행은 있으나 test뿐: C=0, 배분 없음
    "provider_ambiguous",  # 제공자 후보 다중: 후보별 행, share NULL (배부 보류)
    "external_api",        # gpu 행 전혀 없음: 벤더 단가 ③ (dim_token_vendor_price tier='standard')
)


def is_fail(flags) -> bool:
    """FAIL 플래그 판정 — SQL `hasAny(flags, ['hours_over_count','unknown_violation'])`와 동일."""
    return any(f in FAIL_FLAGS for f in flags)


def weighted_tokens(input_tokens, cache_read, cache_creation, output) -> float:
    """
    §6.4 (3) / 정의서 3.5: W(s, m, d) = 1·uncached + 0.1·cached + 4·output,
    uncached = input_tokens + cache_creation_tokens, cached = cache_read_tokens.
    """
    uncached = float(input_tokens) + float(cache_creation)
    return W_UNC * uncached + W_CACHE * float(cache_read) + W_OUT * float(output)


def model_cost(gpu_rows, tco) -> float | None:
    """
    §6.4 (1) / 정의서 3.2: C(m, d) = Σ_gpu_type (serving + standby) gpu_hours × TCO(gpu_type, d).

    gpu_rows: [(category, gpu_type, gpu_hours, flags)] — 한 (date, service, canon(model))의 gpu fact 행.
    tco:      {gpu_type: 원/GPU·h | None} — date 유효 이력 행(최신 행이 NULL이면 None).

    규칙(SQL_M1 model_cost_krw와 동일):
      - test는 C 불포함(그룹 귀속 — 정의서 3.3), FAIL 행 제외(→ unattributed).
      - 합산 대상 행의 기종 하나라도 TCO 미등록/None → None (부분 합 금지).
      - gpu 행이 전혀 없으면 None (has_gpu_rows=0 → NULL); test-only면 0.0 (no_provider).
    """
    if not gpu_rows:
        return None
    total = 0.0
    for category, gpu_type, gpu_hours, flags in gpu_rows:
        if category not in ("serving", "standby") or is_fail(flags):
            continue
        rate = tco.get(gpu_type)
        if rate is None:
            return None
        total += float(gpu_hours) * float(rate)
    return total


def allocate_shared(cost, wtokens) -> dict[str, float]:
    """
    §6.4 (4) / 정의서 3.6: 부담(s, m) = C(m) × W(s) / W(m), W(m) = Σ_s W(s).

    wtokens: {service: W(s, m, d)} — 모집단 = dim_token_service enabled=1 전 서비스.
    W(m) == 0 → {} (I8: 호출측이 token_not_reported로 제공자 행 share=1 전액 귀속).
    전용 모델(서비스 1개)은 자동으로 전액(I4); Σ_s 부담 = C (I3, ±1원).
    """
    total = sum(float(w) for w in wtokens.values())
    if total == 0:
        return {}
    return {s: float(cost) * float(w) / total for s, w in wtokens.items()}


def external_api_cost(input_tokens, cache_read, cache_creation, output, price) -> float | None:
    """
    §6.4 (6) / 정의서 3.9: ③ = (input × p_in + cache_read × p_cached + cache_creation × p_cc + output × p_out) / 1e6.

    price: (krw_per_mtok_input, krw_per_mtok_cached, krw_per_mtok_cache_creation, krw_per_mtok_output) 원/1M,
           dim_token_vendor_price tier='standard' date 유효 행. 하나라도 None → None (+ vendor_price_missing).
    input_tokens는 cache_creation을 제외한 순수 입력(3.5의 uncached와 혼용 금지 — 이중 계산 방지).
    """
    if any(p is None for p in price):
        return None
    p_in, p_cached, p_cc, p_out = (float(p) for p in price)
    return (
        float(input_tokens) * p_in
        + float(cache_read) * p_cached
        + float(cache_creation) * p_cc
        + float(output) * p_out
    ) / 1e6


def group_overhead(allocated_gpu_hours, reported_total, serving, standby, test, flagged, tco) -> dict:
    """
    §6.4 (2)/(7) / 정의서 3.1·3.3·3.4 — M2 agg_token_gpu_group_1d 한 (date, service_group, gpu_type) 행.

    allocated_gpu_hours: 할당표 allocated_gpu_count × 24 (없으면 None)
    reported_total:      Σ 보고 gpu_hours 전체(플래그 포함) = serving + standby + test + flagged
    serving/standby/test: 비FAIL 카테고리별 합, flagged: FAIL 행 합
    tco:                 원/GPU·h (None이면 비용 키 전부 None)

    idle = max(allocated − reported_total, 0) (I1: 음수면 over_report=1 + 0 클램프)
    그룹 총비용 = allocated × TCO (I2: = Σ C + test + idle + unattributed ± 오차 → identity_gap_krw)
    """
    result = {}
    if allocated_gpu_hours is None:
        idle = None
        utilization = None
        over_report = 0
    else:
        allocated = float(allocated_gpu_hours)
        reported = float(reported_total)
        idle = max(allocated - reported, 0.0)
        utilization = (reported / allocated) if allocated > 0 else None
        over_report = int(reported > allocated)
    result["idle_gpu_hours"] = idle
    result["utilization"] = utilization
    result["over_report"] = over_report

    if tco is None:
        for key in ("group_total_cost_krw", "model_cost_sum_krw", "test_cost_krw",
                    "idle_cost_krw", "unattributed_cost_krw", "identity_gap_krw"):
            result[key] = None
        return result

    rate = float(tco)
    result["model_cost_sum_krw"] = (float(serving) + float(standby)) * rate   # Σ 그룹 호스팅 모델 C
    result["test_cost_krw"] = float(test) * rate                              # 실험 비용 (그룹 귀속)
    result["unattributed_cost_krw"] = float(flagged) * rate                   # FAIL 행 × TCO (§6.4 (1) 보정)
    if allocated_gpu_hours is None:
        result["group_total_cost_krw"] = None
        result["idle_cost_krw"] = None
        result["identity_gap_krw"] = None
    else:
        result["group_total_cost_krw"] = float(allocated_gpu_hours) * rate    # 할당 × TCO (정의서 3.4, (7))
        result["idle_cost_krw"] = idle * rate                                 # 유휴 비용
        result["identity_gap_krw"] = (
            result["group_total_cost_krw"]
            - result["model_cost_sum_krw"]
            - result["test_cost_krw"]
            - result["idle_cost_krw"]
            - result["unattributed_cost_krw"]
        )
    return result


def quality_flag_m1(partial, no_tco, flagged, manual, no_metrics, consumer_only) -> str:
    """§6.1 M1 quality_flag — M1_FLAG_PRIORITY 순서의 첫 True, 전부 False면 'normal' (SQL multiIf와 동일)."""
    truth = (partial, no_tco, flagged, manual, no_metrics, consumer_only)
    for name, hit in zip(M1_FLAG_PRIORITY, truth):
        if hit:
            return name
    return M1_FLAG_PRIORITY[-1]
PYEOF
```

실행:

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_mart.py
```

기대 출력(GREEN):

```
.....................                                                    [100%]
21 passed in 0.0Xs
```

Python 3.10 호환 확인(`from __future__ import annotations` + PEP 604 표기만, StrEnum/match 없음)과 순수성(`random`·네트워크 import 없음):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python3 -c "import ast, sys; ast.parse(open('app/mart.py').read()); print('ast ok')" && ! grep -n "import random\|StrEnum\|tomllib\|datetime.UTC\|^match " app/mart.py && echo "py310 ok"
```

기대 출력: `ast ok` 다음 줄 `py310 ok`.

- [ ] **Step 6: 모듈 전체 테스트 + Produces 이름 존재 확인 + zero-diff 게이트 + 커밋**

Task 1 테스트(config/ch/preflight)와 함께 모듈 전체를 돌린다:

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q
```

기대 출력: `48 passed`(T1 27 + T2 21 — config 4 + ch 19 + preflight 4 + mart 21).

Produces 목록의 이름이 전부 모듈에 존재하는지 기계적으로 확인한다(T3/T5/T6/T7/T10이 import 하는 이름):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python3 -c "
from app import mart
names = ['Coverage', 'Warn', 'compute_coverage', 'batch_line', 'target_dates', 'mutation_budget_exceeded',
         'FAIL_FLAGS', 'is_fail', 'W_UNC', 'W_CACHE', 'W_OUT', 'weighted_tokens', 'model_cost',
         'allocate_shared', 'external_api_cost', 'group_overhead', 'quality_flag_m1',
         'M1_FLAG_PRIORITY', 'DENOMINATOR_MODES']
missing = [n for n in names if not hasattr(mart, n)]
assert not missing, missing
assert mart.batch_line('SUCCESS', mart.Coverage(2, 1, ['Mock Service B'], ['Mock Service B']), 3, 5, 0, 1, 12.34) == 'BATCH_RESULT status=SUCCESS module=mart-metrics metrics_coverage=1/2 missing_services=\"Mock Service B\" rows_mart=3 rows_check=5 rows_share=0 warn=1 elapsed=12.3'
print('produces ok', len(names))
"
```

기대 출력: `produces ok 19`.

zero-diff 게이트(기존 자산 무변경):

```bash
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-mart.yml .github/workflows/test-collector.yml
```

기대 출력: (비어 있음).

커밋:

```bash
cd /home/mini/github/token-data-pipeline && git add mart/token-metrics/app/mart.py mart/token-metrics/tests/test_mart.py && git commit -m "feat(mart-metrics): mart.py 순수 로직 — 마커·예산·비용 모델 참조 구현(정의서 §5.1/§5.2/§5.3 재현) (Plan 6c T2)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

기대: `git log --oneline -2`에 T2 커밋 2건(1부·2부)이 보이고, `git status --porcelain -- mart/token-metrics/app/mart.py mart/token-metrics/tests/test_mart.py`가 비어 있다.

**설계 해석 (T2 — footer "Self-Review 노트"에 병합)**:
- `Warn(count: int, text: str = "")` — 크로스 태스크 표의 `Warn(count)`는 `text` 기본값 `""`로 성립(원형 `__add__` 유지). T5는 warns를 `list[str]`로 다루므로 `Warn`을 생성하지 않는다.
- `batch_line`의 빈 missing은 `missing_services="-"`(원형·마스터 §5.6 v1.10 그대로). T5 아웃라인의 `test_marker_success_full_coverage` 예시 문자열 `missing_services=""`는 T2가 정의한 `"-"`로 읽는다(T5 writer는 `'missing_services="-"'`로 단언).
- `compute_coverage(expected_services, anchor_services, expected_late)`는 list/set 무관(`set()` 정규화) — T5가 `query()` 결과 리스트를 그대로 넘긴다.
- `model_cost`: `gpu_rows == []` → `None`(SQL `has_gpu_rows = 0 → NULL`), test-only 행만 있으면 `0.0`(`no_provider` — C=0). SQL_M1의 `if(ga.has_rows = 0 OR ga.tco_null_cnt > 0, NULL, ga.cost_sum)`과 동일.
- `group_overhead`: `allocated_gpu_hours is None`이어도 TCO가 있으면 `model_cost_sum_krw/test_cost_krw/unattributed_cost_krw`는 계산한다(M2 "그룹에 gpu 행은 있으나 할당 행 없음" → `no_allocation` WARN 행에서도 C 합은 표시). `allocated == 0`이면 `utilization = None`(0 나눗셈 회피, `idle = 0`, `over_report = int(reported > 0)`).
- 테스트 명령은 아웃라인 규칙대로 `python -m pytest -q`, 한 줄 스크립트는 Plan 6a 관례대로 `python3 -c`(개발 머신에 `python` alias가 없어도 동작).
### Task 3: app/steps.py — 공통 서브쿼리 SUB_*·canon()·M1 agg_token_model_cost_1d SQL/EXPECTED·_run_table/run_m1

**설계 근거**: 설계 §6.1 299(공통 CTE `eff_alias`/`eff_tco`/`eff_alloc`/`eff_price`/`reg`/`usage_svc`/`anchor` — 이력 dim은 `effective_from <= d` 최신 행 argMax, alloc은 `unknown` 제외, price는 `tier='standard'`), 301(M1 `agg_token_model_cost_1d` — grain date×service×model(canon), 소스 = 토큰 `token_usage_1d`(usage_svc 서비스) ∪ 메트릭 gpu fact(앵커 있는 서비스), 컬럼 28개, `quality_flag` 우선순위 `partial > no_tco > flagged > manual > no_metrics > consumer_only > normal`, `created_by='token-metrics-pipeline'`), §6.4 318-326((1) C = Σ(serving+standby, 비FAIL) gpu_hours × TCO — FAIL = `hours_over_count|unknown_violation`, test 제외, 기종 하나라도 TCO NULL → C NULL; (3) `W_UNC=1, W_CACHE=0.1, W_OUT=4`, uncached = input + cache_creation, `tokens_per_gpu_hour = total/serving`(serving 0 → NULL)), §4.0(GLOBAL LEFT JOIN 표준·`distributed_product_mode=global`·join_use_nulls=0 규약: 미스 = ''/0/NULL(Nullable)/[]), §5.4(토큰 측 읽기 계약 13컬럼 — 이 태스크는 `token_usage_1d` 9컬럼 + `dim_token_service` 2컬럼만), §7.1(서버 바인딩 `{d:Date}`만); Plan 6a C(M1 DDL `docs/superpowers/plans/2026-09-04-token-metrics-schema.md:1098-1175` — 28컬럼 순서가 INSERT 컬럼 목록의 정본), D(조회 규약: `_dist`로 읽고 `_local`에 DELETE, 이력 dim argMax); 정의서 `docs/cost-model-spec.md` §3.1-3.5.
**읽을 원형**: `mart/token-usage/app/steps.py:1-25`(docstring·`StepError`), `86-193`(`SQL_AGG_SERVICE`의 `UNION ALL` 도메인 합집합·`SQL_AGG_ORG`의 argMax 이력 패턴), `366-452`(`_run_table` — 그대로 클론), `mart/token-usage/tests/test_steps.py:1-158`(`FakeGate` — 테이블 키만 교체해 클론), Task 1 `app/ch.py`(DB 상수 5종·`CHGate` 호출 규약), Task 2 `app/mart.py`(`FAIL_FLAGS`·`W_*`·`M1_FLAG_PRIORITY`).

이 태스크는 `steps.py`의 **1부**다 — 공통 조각(`canon()`·`FAIL_PRED`·`_WTOK_EXPR`·`SUB_*` 9종)과 M1 한 테이블의 `SQL_M1`/`EXPECTED_SQL_M1`, 공용 시퀀스 `_run_table`, `run_m1`까지. M3/M4/M2(`SQL_M3`/`SQL_M4`/`SQL_M2`·`run_m3`/`run_m4`/`run_m2`)는 T4/T6/T7이 같은 파일에 append 하며 여기서 만든 `SUB_*`/`FAIL_PRED`/`_WTOK_EXPR`/`canon`/`_run_table`/`T_M*`를 그대로 쓴다. 핵심 원칙 셋:
1. **INSERT와 EXPECTED가 같은 문자열 조각을 공유**한다 — 키 서브쿼리 `_TOK_KEYS`/`_GPU_KEYS`(각각 `SUB_EFF_ALIAS`·`SUB_USAGE_SVC`/`SUB_ANCHOR` 포함)를 `SQL_M1`의 `keys` CTE(`UNION DISTINCT`)와 `EXPECTED_SQL_M1`(`UNION ALL` + `uniqExact((service, model))`)이 그대로 삽입한다. 파생 오차 0(원형 `EXPECTED_SQL_AGG_SERVICE`의 `UNION ALL` + `uniqExact` 패턴).
2. **`written_rows`를 expected로 쓰지 않는다** — `_run_table`은 `gate.query(expected_sql)` 소스 카운트를 `verify_count`의 expected로 넘기고, 반환값은 `verify_count`의 actual(마커 `rows_mart`의 소스)이다(원형 docstring의 Distributed 이중 계상 전례).
3. **DB 접두 규약** — 메트릭 fact `DB_FACT`, 메트릭 dim·레지스트리 `DB_DIM`, mart `DB_MART`, **토큰 측 읽기만 `DB_TOKEN_MART`/`DB_TOKEN_DIM`**(company-verify 격리 시 운영 DB를 가리키는 유일한 경로). 토큰 측은 읽기 계약 컬럼(`u.<col>`)만 참조한다.

**Files:**
- Create: `mart/token-metrics/app/steps.py`(M1까지 — T4/T6/T7이 append)
- Create: `mart/token-metrics/tests/test_steps.py`(FakeGate + SQL 계약 + `_run_table` 시퀀스 — T4/T6/T7이 append)
- Test: `mart/token-metrics/tests/test_steps.py`
- (읽기만) `mart/token-metrics/ddl/company/mart_metrics_tables.sql`(Plan 6a T4 산출물 — 테스트가 파싱해 INSERT 컬럼 목록과 대조; 수정하지 않는다), `mart/token-usage/app/steps.py`, `mart/token-usage/tests/test_steps.py` — zero-diff 대상, 절대 수정하지 않는다.

**Interfaces:**
- Consumes:
  - `app.ch.{DB_FACT, DB_DIM, DB_MART, DB_TOKEN_MART, DB_TOKEN_DIM}`(Task 1 — 모듈 로드 시 1회 평가된 str; `DB_TOKEN_MART` 기본값 = `DB_MART`, `DB_TOKEN_DIM` 기본값 = `DB_DIM`). `CHGate` 호출 규약(Task 1): `exists(table_dist, date) -> bool`, `delete_day(table_local, date, extra_pred="")`, `insert_select(sql, params=None) -> int`, `query(sql, params=None) -> list[tuple]`, `verify_count(table_dist, date, expected) -> tuple[bool, int]` — `_run_table`은 이 5개만 호출한다.
  - `app.mart.{W_UNC, W_CACHE, W_OUT, FAIL_FLAGS}`(Task 2 — `1.0`/`0.1`/`4.0`/`("hours_over_count", "unknown_violation")`; SQL 문자열에 f-string으로 박힌다).
  - Plan 6a C M1 DDL(`mart/token-metrics/ddl/company/mart_metrics_tables.sql`의 `mart.agg_token_model_cost_1d_local` 28컬럼) — 테스트 `ddl_columns()`가 파싱.
  - Plan 6a 테이블 이름(전부 `_dist`로 읽음): `{DB_FACT}.raw_token_metrics_gpu_1d_dist`(`date, service_group, service, model, gpu_type, category, gpu_hours, flags`), `{DB_FACT}.raw_token_metrics_serving_1d_dist`(행 수만), `{DB_FACT}.raw_token_metrics_summary_1d_dist`(앵커: `service, service_group, reported_service_group, reported_service, source_type, gpu_rows, serving_rows, rejected_rows`), `{DB_DIM}.dim_token_metrics_service_dist`(`service, service_group, enabled, coverage_since, until, expect_gpu, expect_serving, usage_includes_consumers`), `{DB_DIM}.dim_token_model_alias_dist`(`alias, effective_from, canonical`), `{DB_DIM}.dim_token_gpu_tco_dist`(`gpu_type, effective_from, tco_krw_per_gpu_hour`), `{DB_DIM}.dim_token_gpu_allocation_dist`(`service_group, gpu_type, effective_from, allocated_gpu_count, source`), `{DB_DIM}.dim_token_vendor_price_dist`(`provider, model, tier, effective_from, krw_per_mtok_{input,cached,cache_creation,output}`); 토큰 측 `{DB_TOKEN_MART}.token_usage_1d_dist`(9컬럼), `{DB_TOKEN_DIM}.dim_token_service_dist`(`service, enabled`).
- Produces (`app.steps` — 크로스 태스크 표 그대로; 아래가 정본):
  - `CREATED_BY = "token-metrics-pipeline"`.
  - `T_M1 = "agg_token_model_cost_1d"`, `T_M3 = "token_metrics_check_1d"`, `T_M4 = "agg_token_model_share_1d"`, `T_M2 = "agg_token_gpu_group_1d"`, `MART_TABLES = (T_M1, T_M3, T_M4, T_M2)`(= 배치 실행 순서 M1→M3→M4→M2; 접두 없이 — 호출측이 `f"{DB_MART}.{T_M1}_dist"`/`_local`로 조립).
  - `class StepError(Exception)` — verify 실패(재시도 소진 후 actual < expected).
  - `canon(x: str) -> str` = `f"if(a.canonical = '', {x}, a.canonical)"` — 호출측이 `SUB_EFF_ALIAS`를 `AS a`로 조인해 둔 전제.
  - `FAIL_PRED = "hasAny(g.flags, ['hours_over_count','unknown_violation'])"`(`FAIL_FLAGS`에서 생성; gpu fact alias 항상 `g`).
  - `_WTOK_EXPR = "1.0 * (input_tokens + cache_creation_tokens) + 0.1 * cache_read_tokens + 4.0 * output_tokens"`(같은 SELECT의 alias 4개를 전제; M1 outer SELECT·M4 wt 서브쿼리 공유).
  - 괄호 포함 서브쿼리 문자열 상수(호출측이 `AS <alias>` 부착): `SUB_EFF_ALIAS`(`alias, canonical`), `SUB_EFF_TCO`(`gpu_type, tco`), `SUB_EFF_ALLOC`(`service_group, gpu_type, allocated_gpu_count, source`; `HAVING gpu_type != 'unknown'`), `SUB_EFF_PRICE`(`provider, model, p_in, p_cached, p_cc, p_out`; `WHERE tier = 'standard'`), `SUB_REG`(레지스트리 8컬럼), `SUB_USAGE_SVC`(`service`; `enabled = 1`), `SUB_ANCHOR`(앵커 8컬럼, `date = {d:Date}`), `SUB_GPU_CNT`/`SUB_SERVING_CNT`(`service, n` — 자식 fact 행 수, partial 판정용).
  - 키 조각 `_TOK_KEYS`/`_GPU_KEYS`(`SELECT service, model … GROUP BY`; INSERT `keys` CTE와 EXPECTED 공유), `SQL_M1`(28컬럼 명시 INSERT … SELECT), `EXPECTED_SQL_M1`(`uniqExact((service, model))`).
  - `_run_table(gate, date: str, dist: str, local: str, sql: str, expected_sql: str, warns: list, extra_pred: str = "") -> int` — `exists → delete_day(extra_pred) → insert_select({"d": date}) → query(expected_sql, {"d": date}) → verify_count`; 실패 `StepError`; `actual > expected`면 `warns.append(f"dup_suspect:{dist}")`; 반환 actual.
  - `run_m1(gate, date: str) -> dict` = `{"rows_mart": int, "warns": list[str]}`.
  - 테스트 헬퍼(`tests/test_steps.py` — T4/T6/T7이 같은 파일에서 재사용): `ddl_columns(table_local: str) -> list[str]`, `insert_columns(sql: str) -> list[str]`, `sql_constants() -> dict`, `FakeGate(exists=True, verify_ok=True, verify_actual=None, expected_overrides=None)`(`_TABLE_KEYS` 4테이블 → `m1/m3/m4/m2`, `order/delete_preds/written/query_calls/verify_calls` 기록, `insert_select`는 7 반환, `query`는 `expected_overrides.get(short, 3)`).

- [ ] **Step 1: 원형·DDL 확인 (수정 금지)**

원형의 `_run_table`(그대로 클론할 대상)과 `EXPECTED_SQL_*` 패턴, FakeGate를 눈으로 확인한다:

```bash
sed -n 1,25p /home/mini/github/token-data-pipeline/mart/token-usage/app/steps.py
sed -n 310,340p /home/mini/github/token-data-pipeline/mart/token-usage/app/steps.py
sed -n 366,452p /home/mini/github/token-data-pipeline/mart/token-usage/app/steps.py
sed -n 1,80p /home/mini/github/token-data-pipeline/mart/token-usage/tests/test_steps.py
```

기대: `_run_table(gate, date, dist, local, sql, expected_sql, warns, extra_pred="")`가 `exists → delete_day → insert_select → query(expected_sql) → verify_count` 순서로 호출하고 `actual`을 반환한다; `EXPECTED_SQL_AGG_SERVICE`가 `SELECT uniqExact(service) FROM ( … UNION ALL … )` 형태다; `FakeGate._TABLE_KEYS`가 `(테이블 키, 짧은 이름)` 목록이다.

Plan 6a T4가 만든 M1 DDL의 컬럼 28개를 확인한다(테스트 `ddl_columns()`가 이 파일을 파싱한다):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && grep -c "CREATE TABLE" ddl/company/mart_metrics_tables.sql && grep -n "CREATE TABLE IF NOT EXISTS mart.agg_token_model_cost_1d_local" ddl/company/mart_metrics_tables.sql && python3 -c "
import pathlib, re
t = pathlib.Path('ddl/company/mart_metrics_tables.sql').read_text(encoding='utf-8')
s = t.index('CREATE TABLE IF NOT EXISTS mart.agg_token_model_cost_1d_local\n')
b = t.index('\n(\n', s) + 3; e = t.index('\n)\n', b)
cols = [l.strip().split()[0] for l in t[b:e].splitlines() if l.strip() and not l.strip().startswith(('--', 'CONSTRAINT'))]
print(len(cols)); print(', '.join(cols))
"
```

기대 출력: 첫 줄 `8`(CREATE TABLE 8개 = 4테이블 × `_local`/`_dist`), 둘째 줄 `<n>:CREATE TABLE IF NOT EXISTS mart.agg_token_model_cost_1d_local`, 셋째 줄 `28`, 넷째 줄 `date, service_group, service, model, serving_gpu_hours, standby_gpu_hours, test_gpu_hours, flagged_gpu_hours, equiv_gpu_count, scaled_intraday, model_cost_krw, input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, requests, uncached_tokens, cached_tokens, total_tokens, weighted_tokens, tokens_per_gpu_hour, gpu_type_mix, model_registered, tco_missing, has_token_rows, has_gpu_rows, quality_flag, created_by`.

- [ ] **Step 2: 실패하는 테스트 — `mart/token-metrics/tests/test_steps.py`** (원형 `mart/token-usage/tests/test_steps.py:1-158` FakeGate 클론 + 테이블 키 교체; SQL 계약 테스트는 신규. T4/T6/T7이 같은 파일에 M3/M4/M2 테스트를 append 한다)

`mart/token-metrics/tests/test_steps.py`를 아래 내용 **전체**로 생성한다:

```python
"""app/steps.py 단위 테스트 — SQL 문자열 계약(서버 바인딩·컬럼 순서·비용 술어·우선순위) +
_run_table 시퀀스(FakeGate). ClickHouse 없이 돈다(SQL 실행은 T10 e2e·CI가 담당).

FakeGate는 mart/token-usage/tests/test_steps.py의 것을 복제하되 테이블 키를 mart-metrics
4테이블로 바꿨다(_TABLE_KEYS 부분 문자열 라우팅 — 서로 부분 문자열이 아니어야 함, 테스트로 고정).
"""
import os
import pathlib
import re
import subprocess
import sys

import pytest

from app import steps
from app.ch import DB_DIM, DB_FACT, DB_MART, DB_TOKEN_DIM, DB_TOKEN_MART
from app.mart import FAIL_FLAGS, W_CACHE, W_OUT, W_UNC

MODULE_ROOT = pathlib.Path(__file__).resolve().parents[1]
DDL_PATH = MODULE_ROOT / "ddl" / "company" / "mart_metrics_tables.sql"
DATE = "2026-09-01"   # 러너 테스트용 날짜 상수 — T4(M3)가 같은 파일에서 재사용, T6/T7은 자체 M4_DATE/M2_DATE

# 읽기 계약(설계 §5.4 / app.preflight.READ_CONTRACT) — token_usage_1d 9컬럼
READ_CONTRACT_TOKEN_USAGE_1D = (
    "date", "service_group", "service", "model",
    "input_tokens", "cache_read_tokens", "cache_creation_tokens", "output_tokens", "requests",
)


# ----------------------------------------------------------------------------
# 헬퍼 — DDL 컬럼 파서(T4/T6/T7이 재사용) + INSERT 컬럼 목록 파서
# ----------------------------------------------------------------------------

def ddl_columns(table_local: str) -> list:
    """mart_metrics_tables.sql에서 `CREATE TABLE IF NOT EXISTS mart.<table_local>` 블록의
    컬럼 이름을 선언 순서대로 반환. CONSTRAINT 행·빈 줄·주석은 제외."""
    text = DDL_PATH.read_text(encoding="utf-8")
    head = f"CREATE TABLE IF NOT EXISTS mart.{table_local}\n"
    start = text.index(head)
    body_start = text.index("\n(\n", start) + len("\n(\n")
    body_end = text.index("\n)\n", body_start)
    cols = []
    for raw in text[body_start:body_end].splitlines():
        line = raw.strip()
        if not line or line.startswith("--") or line.startswith("CONSTRAINT"):
            continue
        cols.append(line.split()[0])
    return cols


def insert_columns(sql: str) -> list:
    """`INSERT INTO <table> (c1, c2, ...)`의 컬럼 목록."""
    m = re.search(r"INSERT INTO\s+\S+\s*\((.*?)\)", sql, re.S)
    assert m, "INSERT INTO <table> (cols) 형태가 아님"
    return [c.strip() for c in m.group(1).split(",") if c.strip()]


def sql_constants() -> dict:
    """steps 모듈의 SQL_*/EXPECTED_SQL_* 상수 전부(T4~T7이 추가하는 것도 자동 포함)."""
    return {n: v for n, v in vars(steps).items()
            if (n.startswith("SQL_") or n.startswith("EXPECTED_SQL_")) and isinstance(v, str)}


# ----------------------------------------------------------------------------
# FakeGate — 토큰 mart 테스트 더블 복제(테이블 키만 교체)
# ----------------------------------------------------------------------------

class FakeGate:
    """CHGate 더블. 호출 순서(order)·삭제 술어·INSERT SQL·query SQL·verify 인자를 기록한다.
    `_short()`는 dist/SQL 문자열에서 테이블 키를 찾아 짧은 이름(m1/m3/m4/m2)으로 라우팅한다."""

    _TABLE_KEYS = [
        ("agg_token_model_cost_1d", "m1"),
        ("token_metrics_check_1d", "m3"),
        ("agg_token_model_share_1d", "m4"),
        ("agg_token_gpu_group_1d", "m2"),
    ]

    def __init__(self, exists=True, verify_ok=True, verify_actual=None, expected_overrides=None):
        self._exists = exists
        self._verify_ok = verify_ok
        self._verify_actual = verify_actual
        self._expected_overrides = expected_overrides or {}
        self.order = []
        self.delete_preds = []
        self.written = []
        self.query_calls = []
        self.verify_calls = []
        self._current_short = None

    def _short(self, s: str) -> str:
        for key, short in sorted(self._TABLE_KEYS, key=lambda kv: -len(kv[0])):
            if key in s:
                return short
        raise AssertionError(f"unknown table in: {s[:120]!r}")

    def exists(self, table_dist, date):
        self.order.append(("exists", self._short(table_dist)))
        return self._exists

    def delete_day(self, table_local, date, extra_pred=""):
        self.order.append(("delete", self._short(table_local)))
        self.delete_preds.append((self._short(table_local), extra_pred))

    def insert_select(self, sql, params=None):
        short = self._short(sql)
        self._current_short = short
        self.order.append(("insert", short))
        self.written.append((short, sql, params))
        return 7

    def query(self, sql, params=None):
        self.order.append(("query", self._current_short))
        self.query_calls.append((self._current_short, sql, params))
        return [(self._expected_overrides.get(self._current_short, 3),)]

    def verify_count(self, table_dist, date, expected):
        short = self._short(table_dist)
        self.order.append(("verify", short))
        self.verify_calls.append((short, date, expected))
        actual = expected if self._verify_actual is None else self._verify_actual
        return (self._verify_ok, actual)


# ----------------------------------------------------------------------------
# 전역 SQL 계약
# ----------------------------------------------------------------------------

def test_all_sql_constants_use_date_binding_and_no_percent_format():
    consts = sql_constants()
    assert {"SQL_M1", "EXPECTED_SQL_M1"} <= set(consts)
    for name, sql in consts.items():
        assert "{d:Date}" in sql, name
        assert "%(" not in sql and "%s" not in sql, name
        assert "{{" not in sql and "}}" not in sql, name      # f-string 이스케이프 잔재 금지
        assert ".format(" not in sql, name


def test_no_coalesce_anywhere_in_sql():
    for name, sql in sql_constants().items():
        assert "coalesce(" not in sql.lower(), name


def test_created_by_is_token_metrics_pipeline():
    assert steps.CREATED_BY == "token-metrics-pipeline"
    assert re.search(r"'token-metrics-pipeline'\s+AS created_by", steps.SQL_M1)
    assert "'token-pipeline'" not in steps.SQL_M1


def test_canon_expression_identical_in_insert_and_expected():
    assert steps.canon("u.model") == "if(a.canonical = '', u.model, a.canonical)"
    for x in ("u.model", "g.model"):
        assert steps.canon(x) in steps.SQL_M1
        assert steps.canon(x) in steps.EXPECTED_SQL_M1
    # 키 조각 자체가 두 SQL에 그대로 들어간다(파생 오차 0)
    for frag in (steps._TOK_KEYS, steps._GPU_KEYS):
        assert frag in steps.SQL_M1
        assert frag in steps.EXPECTED_SQL_M1
    assert "UNION DISTINCT" in steps.SQL_M1
    assert "UNION ALL" in steps.EXPECTED_SQL_M1
    assert "uniqExact((service, model))" in steps.EXPECTED_SQL_M1


def test_sub_queries_shared_between_insert_and_expected():
    # SUB_* 문자열 상수 = 괄호로 감싼 서브쿼리, 호출측이 AS 별칭을 붙인다
    subs = {n: v for n, v in vars(steps).items() if n.startswith("SUB_")}
    assert {"SUB_EFF_ALIAS", "SUB_EFF_TCO", "SUB_EFF_ALLOC", "SUB_EFF_PRICE",
            "SUB_REG", "SUB_USAGE_SVC", "SUB_ANCHOR", "SUB_GPU_CNT", "SUB_SERVING_CNT"} <= set(subs)
    for name, sub in subs.items():
        assert sub.startswith("(SELECT") and sub.endswith(")"), name
        assert " AS " not in sub[-6:], name
    for name in ("SUB_EFF_ALIAS", "SUB_EFF_TCO", "SUB_EFF_ALLOC", "SUB_EFF_PRICE",
                 "SUB_ANCHOR", "SUB_GPU_CNT", "SUB_SERVING_CNT"):
        assert "{d:Date}" in subs[name], name
    assert f"{DB_DIM}.dim_token_model_alias_dist" in steps.SUB_EFF_ALIAS
    assert f"{DB_DIM}.dim_token_gpu_tco_dist" in steps.SUB_EFF_TCO
    assert f"{DB_DIM}.dim_token_gpu_allocation_dist" in steps.SUB_EFF_ALLOC
    assert f"{DB_DIM}.dim_token_vendor_price_dist" in steps.SUB_EFF_PRICE
    assert f"{DB_DIM}.dim_token_metrics_service_dist" in steps.SUB_REG
    assert f"{DB_TOKEN_DIM}.dim_token_service_dist" in steps.SUB_USAGE_SVC
    assert f"{DB_FACT}.raw_token_metrics_summary_1d_dist" in steps.SUB_ANCHOR
    assert f"{DB_FACT}.raw_token_metrics_gpu_1d_dist" in steps.SUB_GPU_CNT
    assert f"{DB_FACT}.raw_token_metrics_serving_1d_dist" in steps.SUB_SERVING_CNT
    # serving_rows 앵커값은 serving[] 원소 수 — custom 전개 행(metric='custom')은 실측에서 제외
    assert "countIf(metric != 'custom') AS n" in steps.SUB_SERVING_CNT
    assert "count() AS n" in steps.SUB_GPU_CNT
    # T4/T6/T7 계약: alloc은 unknown 제외 + allocated_gpu_count 별칭, price는 standard 고정 + p_* 별칭
    assert "gpu_type != 'unknown'" in steps.SUB_EFF_ALLOC
    assert "AS allocated_gpu_count" in steps.SUB_EFF_ALLOC
    assert "tier = 'standard'" in steps.SUB_EFF_PRICE
    for alias in ("AS p_in", "AS p_cached", "AS p_cc", "AS p_out"):
        assert alias in steps.SUB_EFF_PRICE
    # 최신 이력 행의 NULL 전파(설계 해석 2) — argMax(ifNull(x, -1)) + nullIf(..., -1)
    for name in ("SUB_EFF_TCO", "SUB_EFF_ALLOC", "SUB_EFF_PRICE"):
        assert "nullIf(argMax(ifNull(" in subs[name], name
    assert "effective_from <= {d:Date}" in steps.SUB_EFF_ALIAS
    assert "enabled = 1" in steps.SUB_USAGE_SVC
    # SQL_M1에서 실제로 조인되는 조각들
    for name in ("SUB_EFF_ALIAS", "SUB_EFF_TCO", "SUB_REG", "SUB_USAGE_SVC", "SUB_ANCHOR",
                 "SUB_GPU_CNT", "SUB_SERVING_CNT"):
        assert subs[name] in steps.SQL_M1, name


def test_global_join_and_global_in_only():
    for name, sql in sql_constants().items():
        for m in re.finditer(r"\bLEFT JOIN\b", sql):
            assert sql[max(0, m.start() - 7):m.start()] == "GLOBAL ", (name, sql[m.start() - 40:m.end()])
        for m in re.finditer(r"\bIN\s*\(SELECT", sql):
            assert sql[max(0, m.start() - 7):m.start()] == "GLOBAL ", (name, sql[m.start() - 40:m.end()])
        if not name.endswith("_SUMMARY"):   # SQL_M3_SUMMARY(T4)는 단일 테이블 GROUP BY — 서브쿼리 없음
            assert "GLOBAL IN" in sql, name
        assert " JOIN " not in sql.replace("GLOBAL LEFT JOIN", ""), name   # INNER/CROSS 금지


# ----------------------------------------------------------------------------
# M1 — agg_token_model_cost_1d
# ----------------------------------------------------------------------------

def test_m1_insert_column_list_matches_ddl_order():
    cols = ddl_columns("agg_token_model_cost_1d_local")
    assert len(cols) == 28
    assert cols[0] == "date" and cols[-1] == "created_by"
    assert insert_columns(steps.SQL_M1) == cols
    # SELECT 절 alias도 같은 순서(위치 기반 INSERT 금지 원칙의 2중 방어)
    outer = steps.SQL_M1[steps.SQL_M1.rindex("\nSELECT\n"):steps.SQL_M1.index("\nFROM keys AS k")]
    aliases = re.findall(r"\bAS (\w+)\s*,?\s*$", outer, re.M)
    assert aliases == cols


def test_m1_cost_predicate_serving_standby_not_fail():
    sql = steps.SQL_M1
    fail = "hasAny(g.flags, ['hours_over_count','unknown_violation'])"
    assert steps.FAIL_PRED == fail
    assert tuple(FAIL_FLAGS) == ("hours_over_count", "unknown_violation")
    assert f"g.category IN ('serving','standby') AND NOT {fail}" in sql
    # cost_sum sumIf 슬라이스에 'test'가 없어야 한다(테스트 GPU 시간은 C 불포함)
    cost_slice = sql[sql.index("sumIf(g.gpu_hours * t.tco"):sql.index("AS cost_sum")]
    assert "'test'" not in cost_slice
    assert f"NOT {fail} AND isNotNull(t.tco)" in cost_slice
    # 기종 하나라도 TCO NULL → C NULL(부분 합 금지), 행 없음 → NULL, 그 외 NULL 합은 0
    assert "if(ga.has_rows = 0 OR ga.tco_null_cnt > 0, NULL, ifNull(ga.cost_sum, 0))" in sql
    assert f"countIf(g.category IN ('serving','standby') AND NOT {fail} AND isNull(t.tco))" in sql
    # 시간 4분류
    assert f"sumIf(g.gpu_hours, g.category = 'serving' AND NOT {fail})  AS serving_gpu_hours" in sql
    assert f"sumIf(g.gpu_hours, g.category = 'standby' AND NOT {fail})  AS standby_gpu_hours" in sql
    assert f"sumIf(g.gpu_hours, g.category = 'test' AND NOT {fail})     AS test_gpu_hours" in sql
    assert f"sumIf(g.gpu_hours, {fail})" in sql and "AS flagged_gpu_hours" in sql
    assert "/ 24" in sql and "AS equiv_gpu_count" in sql
    assert "0                                                                 AS scaled_intraday" in sql
    assert "if(ga.serving_gpu_hours > 0, total_tokens / ga.serving_gpu_hours, NULL)" in sql
    assert "arraySort(groupUniqArray(g.gpu_type))" in sql


def test_m1_weight_constants_inlined():
    assert (W_UNC, W_CACHE, W_OUT) == (1.0, 0.1, 4.0)
    assert "1.0 * (" in steps.SQL_M1
    assert "0.1 * " in steps.SQL_M1
    assert "4.0 * " in steps.SQL_M1
    assert steps._WTOK_EXPR == ("1.0 * (input_tokens + cache_creation_tokens)"
                                " + 0.1 * cache_read_tokens + 4.0 * output_tokens")
    assert f"{steps._WTOK_EXPR}" in steps.SQL_M1
    assert "input_tokens + cache_creation_tokens                              AS uncached_tokens" in steps.SQL_M1
    assert "cache_read_tokens                                                 AS cached_tokens" in steps.SQL_M1
    assert "input_tokens + cache_read_tokens + cache_creation_tokens + output_tokens" in steps.SQL_M1


def test_m1_reads_token_side_only_via_token_db_constants():
    sql = steps.SQL_M1
    used = set(re.findall(r"\bu\.(\w+)", sql))
    assert used <= set(READ_CONTRACT_TOKEN_USAGE_1D), used - set(READ_CONTRACT_TOKEN_USAGE_1D)
    assert f"{DB_TOKEN_MART}.token_usage_1d_dist" in sql
    assert f"{DB_TOKEN_DIM}.dim_token_service_dist" in sql
    # 토큰 측 테이블은 DB_TOKEN_* 접두로만 등장(격리 DB 검증 시 운영 DB를 가리키는 유일한 경로)
    assert re.search(r"\b\w+\.token_usage_1d_dist", sql).group(0) == f"{DB_TOKEN_MART}.token_usage_1d_dist"
    for m in re.finditer(r"(\w+)\.token_usage_1d_dist", sql):
        assert m.group(1) == DB_TOKEN_MART
    for m in re.finditer(r"(\w+)\.dim_token_service_dist", sql):
        assert m.group(1) == DB_TOKEN_DIM
    assert "agg_token_service_1d" not in sql          # M1은 token_usage_1d만 읽는다
    assert f"INSERT INTO {DB_MART}.agg_token_model_cost_1d_dist" in sql
    assert "u.user_id" not in sql and "u.org" not in sql


def test_db_env_override_isolates_token_side_in_sql_m1():
    """company-verify 격리(설계 §5.4/§6.1): 메트릭 fact/dim/mart는 token_verify_* DB, 토큰 측 읽기만
    운영 DB(CH_DB_TOKEN_MART/CH_DB_TOKEN_DIM). DB명은 import 시점에 f-string으로 고정되므로
    자식 프로세스에서 확인한다(원형 mart/token-usage/tests/test_ch.py::test_db_names_env_override)."""
    env = {"PATH": os.environ.get("PATH", ""),
           "CH_DB_FACT": "token_verify_fact", "CH_DB_DIM": "token_verify_gpu_data",
           "CH_DB_MART": "token_verify_mart",
           "CH_DB_TOKEN_MART": "mart", "CH_DB_TOKEN_DIM": "gpu_data"}
    result = subprocess.run(
        [sys.executable, "-c", "from app import steps; print(steps.SQL_M1)"],
        cwd=str(MODULE_ROOT), env=env, capture_output=True, text=True, check=True)
    sql = result.stdout
    assert "INSERT INTO token_verify_mart.agg_token_model_cost_1d_dist" in sql
    assert "token_verify_fact.raw_token_metrics_gpu_1d_dist" in sql
    assert "token_verify_fact.raw_token_metrics_summary_1d_dist" in sql
    assert "token_verify_gpu_data.dim_token_model_alias_dist" in sql
    assert "token_verify_gpu_data.dim_token_metrics_service_dist" in sql
    assert "mart.token_usage_1d_dist" in sql and "token_verify_mart.token_usage_1d_dist" not in sql
    assert "gpu_data.dim_token_service_dist" in sql and "token_verify_gpu_data.dim_token_service_dist" not in sql
    # CH_DB_TOKEN_* 미지정이면 DB_MART/DB_DIM을 따라간다(단일 DB 운영 기본값)
    env2 = {k: v for k, v in env.items() if not k.startswith("CH_DB_TOKEN_")}
    result2 = subprocess.run(
        [sys.executable, "-c", "from app import steps; print(steps.SQL_M1)"],
        cwd=str(MODULE_ROOT), env=env2, capture_output=True, text=True, check=True)
    assert "token_verify_mart.token_usage_1d_dist" in result2.stdout
    assert "token_verify_gpu_data.dim_token_service_dist" in result2.stdout


def test_m1_quality_flag_priority_order_in_sql():
    sql = steps.SQL_M1
    order = ["'partial'", "'no_tco'", "'flagged'", "'manual'", "'no_metrics'", "'consumer_only'", "'normal'"]
    qf = sql[sql.rindex("multiIf(", 0, sql.index("AS quality_flag")):sql.index("AS quality_flag")]
    positions = [qf.index(tok) for tok in order]
    assert positions == sorted(positions)
    # 판정 술어(설계 해석 4: partial = 앵커 있음 AND (gpu_rows 또는 serving_rows 실측 불일치))
    assert "an.service != '' AND (an.gpu_rows != gc.n OR an.serving_rows != sc.n)" in qf
    assert "ga.has_rows = 1 AND ga.tco_null_cnt > 0" in qf
    assert "ga.flagged_gpu_hours > 0" in qf
    assert "an.source_type = 'manual-v0'" in qf
    assert ("r.service != '' AND r.enabled = 1 AND r.coverage_since <= {d:Date}"
            "\n            AND (isNull(r.until) OR {d:Date} <= r.until) AND an.service = ''") in qf
    assert "r.service = '', " in qf
    # 플래그·has 컬럼
    assert "greatest(tk.registered, ga.registered)" in sql
    assert "max(a.canonical != '')" in sql
    assert "max(isNull(t.tco))" in sql and "AS tco_missing" in sql
    assert "tk.has_rows                                                       AS has_token_rows" in sql
    assert "ga.has_rows                                                       AS has_gpu_rows" in sql


def test_m1_service_group_fallback_order():
    # reg > gpu fact > token mart (설계 §6.1 — 레지스트리 우선, 미등록 서비스는 소스 값)
    assert ("multiIf(r.service_group != '', r.service_group,\n"
            "            ga.service_group != '', ga.service_group,\n"
            "            tk.service_group)") in steps.SQL_M1


def test_mart_tables_order_and_names():
    assert steps.T_M1 == "agg_token_model_cost_1d"
    assert steps.T_M3 == "token_metrics_check_1d"
    assert steps.T_M4 == "agg_token_model_share_1d"
    assert steps.T_M2 == "agg_token_gpu_group_1d"
    assert steps.MART_TABLES == (steps.T_M1, steps.T_M3, steps.T_M4, steps.T_M2)


# ----------------------------------------------------------------------------
# FakeGate 자체 계약 + _run_table 시퀀스
# ----------------------------------------------------------------------------

def test_fake_gate_table_keys_are_not_substrings_of_each_other():
    keys = [k for k, _ in FakeGate._TABLE_KEYS]
    for a in keys:
        for b in keys:
            if a != b:
                assert a not in b, (a, b)


def test_run_table_sequence_exists_delete_insert_expected_verify():
    g = FakeGate(exists=True)
    warns = []
    n = steps._run_table(g, "2026-09-01", f"{DB_MART}.{steps.T_M1}_dist",
                         f"{DB_MART}.{steps.T_M1}_local", steps.SQL_M1, steps.EXPECTED_SQL_M1, warns)
    assert g.order == [("exists", "m1"), ("delete", "m1"), ("insert", "m1"),
                       ("query", "m1"), ("verify", "m1")]
    assert g.delete_preds == [("m1", "")]
    assert g.written[0][2] == {"d": "2026-09-01"}
    assert g.query_calls[0][1] is steps.EXPECTED_SQL_M1
    assert g.query_calls[0][2] == {"d": "2026-09-01"}
    assert g.verify_calls == [("m1", "2026-09-01", 3)]
    assert n == 3 and warns == []          # 반환은 actual(소스 카운트), written_rows(7) 아님


def test_run_table_skips_delete_when_not_exists():
    g = FakeGate(exists=False)
    steps._run_table(g, "2026-09-01", f"{DB_MART}.{steps.T_M1}_dist",
                     f"{DB_MART}.{steps.T_M1}_local", steps.SQL_M1, steps.EXPECTED_SQL_M1, [])
    assert [o for o, _ in g.order] == ["exists", "insert", "query", "verify"]


def test_run_table_raises_step_error_on_verify_fail():
    g = FakeGate(verify_ok=False, verify_actual=1)
    with pytest.raises(steps.StepError) as ei:
        steps._run_table(g, "2026-09-01", f"{DB_MART}.{steps.T_M1}_dist",
                         f"{DB_MART}.{steps.T_M1}_local", steps.SQL_M1, steps.EXPECTED_SQL_M1, [])
    msg = str(ei.value)
    assert "verify_count failed" in msg and "expected=3" in msg and "actual=1" in msg
    assert "written_rows=7" in msg


def test_run_table_dup_suspect_warn_when_actual_gt_expected():
    g = FakeGate(verify_ok=True, verify_actual=5)
    warns = []
    n = steps._run_table(g, "2026-09-01", f"{DB_MART}.{steps.T_M1}_dist",
                         f"{DB_MART}.{steps.T_M1}_local", steps.SQL_M1, steps.EXPECTED_SQL_M1, warns)
    assert n == 5
    assert warns == [f"dup_suspect:{DB_MART}.agg_token_model_cost_1d_dist"]


def test_run_m1_returns_rows_mart_from_verify_actual():
    g = FakeGate(expected_overrides={"m1": 11})
    out = steps.run_m1(g, "2026-09-01")
    assert out == {"rows_mart": 11, "warns": []}
    assert g.verify_calls == [("m1", "2026-09-01", 11)]
    assert g.written[0][1] is steps.SQL_M1
```

- [ ] **Step 3: RED 확인**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_steps.py 2>&1 | tail -n 4
```

기대 출력(RED — `app/steps.py` 부재; `app/__init__.py`는 T1이 만들어 두었으므로 `ModuleNotFoundError`가 아니라 `ImportError`다):

```
E   ImportError: cannot import name 'steps' from 'app' (/home/mini/github/token-data-pipeline/mart/token-metrics/app/__init__.py)
=========================== short test summary info ============================
ERROR tests/test_steps.py
!!!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
1 error in 0.50s
```

- [ ] **Step 4: 구현 — `mart/token-metrics/app/steps.py`** (원형 `mart/token-usage/app/steps.py:1-25`의 docstring 골격·`StepError`, `366-452`의 `_run_table`을 클론; SQL 조각·`SQL_M1`·`EXPECTED_SQL_M1`·`run_m1`은 신규. `SUB_*`는 CTE가 아니라 **괄호 포함 서브쿼리 문자열**이며, `{{d:Date}}`는 f-string 이스케이프라 최종 문자열에는 `{d:Date}`로 남는다)

`mart/token-metrics/app/steps.py`를 아래 내용 **전체**로 생성한다(T4/T6/T7이 M3/M4/M2를 이 파일 끝에 append 한다):

```python
"""mart-metrics M1/M3/M4/M2 서버사이드 SQL + 실행 함수 (Plan 6c T3~T7; 이 파일은 T3 시점 = 공통 조각 + M1).

전부 서버사이드 INSERT INTO ... SELECT — GLOBAL LEFT JOIN 표준(설계 §4.0,
distributed_product_mode=global은 CHGate.insert_select settings), 날짜는 ClickHouse 서버
바인딩(`{d:Date}`)만 사용한다(SQL 인젝션·타입 사고 방지, 마스터 §7.1).

이스케이프 규칙: 아래 SQL 상수는 모듈 로드 시 f-string으로 DB명(DB_FACT/DB_DIM/DB_MART/
DB_TOKEN_MART/DB_TOKEN_DIM)과 가중치 상수(W_*)만 보간하고, 서버 바인딩 자리는 소스에서
`{{d:Date}}`로 이중 중괄호 이스케이프해 최종 문자열에는 `{d:Date}`가 그대로 남는다.

DB명 규약(설계 §6.1): 메트릭 fact는 DB_FACT, 메트릭 dim·레지스트리는 DB_DIM, mart 4테이블은
DB_MART, **토큰 측 읽기(token_usage_1d·agg_token_service_1d → DB_TOKEN_MART, dim_token_service
→ DB_TOKEN_DIM)는 읽기 계약 13컬럼만**(app/preflight.py READ_CONTRACT) — company-verify 격리
검증 시 토큰 측만 운영 DB를 가리킨다.

공통 서브쿼리(설계 §6.1 "공통 CTE" eff_alias/eff_tco/eff_alloc/eff_price/reg/usage_svc/anchor):
CTE 대신 **괄호 포함 서브쿼리 문자열 상수 SUB_***로 두고 호출측이 `AS a/t/al/p/r/u/an`을
붙인다 — INSERT…SELECT와 EXPECTED_SQL이 같은 문자열 조각을 공유해 파생 오차를 0으로 만들기
위함(설계 해석 3). 이력 조회는 `effective_from <= {d:Date}` 행 중 argMax(최신) — 최신 행의
NULL은 NULL로 전파(`nullIf(argMax(ifNull(x, -1), effective_from), -1)`; argMax가 NULL arg를
건너뛰어 과거 실값이 되살아나는 문제 회피, 설계 해석 2).

컬럼 정본: mart/token-metrics/ddl/company/mart_metrics_tables.sql(Plan 6a T4) — 아래 INSERT
컬럼 목록·SELECT 순서는 DDL 컬럼 순서를 그대로 옮긴 것. 위치 기반 INSERT 금지 — 모든 INSERT가
대상 컬럼을 명시한다(tests/test_steps.py가 DDL 파일을 파싱해 동일 리스트를 단언).

verify expected: insert_select의 written_rows는 verify_count의 expected로 쓰지 않는다 —
Distributed(insert_distributed_sync=1) 경로에서 written_rows가 이중 계상되어 영원히 통과
불가능한 expected를 만든 토큰 mart CI 실패 전례(mart/token-usage/app/steps.py docstring).
대신 테이블별 EXPECTED_SQL 소스 카운트(같은 키 서브쿼리 문자열의 UNION ALL + uniqExact)를 쓴다.

비용 모델(설계 §6.4 = docs/cost-model-spec.md): C = Σ(serving+standby, 비FAIL) gpu_hours × TCO
(기종 하나라도 TCO NULL → NULL, test 제외), FAIL 플래그 = FAIL_PRED, W = W_UNC·(input +
cache_creation) + W_CACHE·cache_read + W_OUT·output(app/mart.py 상수 정본). 참조 구현은
app/mart.py(model_cost/weighted_tokens …) — SQL과 동일 규칙, e2e가 대조한다.
"""
from app.ch import DB_DIM, DB_FACT, DB_MART, DB_TOKEN_DIM, DB_TOKEN_MART
from app.mart import FAIL_FLAGS, W_CACHE, W_OUT, W_UNC


class StepError(Exception):
    """verify_count 실패(재시도 소진 후 actual < expected) 시 발생 — 호출자(batch.py)가
    BATCH_RESULT status=FAILURE로 전파한다."""


# 공유 쓰기 계약(Plan 6a C) — 기존 토큰 mart의 'token-pipeline'과 구분, 불변식
# created_by_wrong_metrics가 이 값을 검사한다.
CREATED_BY = "token-metrics-pipeline"

# mart 4테이블(DB_MART 접두 없이 — 호출측이 f"{DB_MART}.{T_M1}_dist"/_local로 조립).
# MART_TABLES 순서 = 배치 실행 순서 M1 → M3 → M4 → M2(설계 §6.1) = 뮤테이션 예산 선검사 순회 순서.
T_M1 = "agg_token_model_cost_1d"
T_M3 = "token_metrics_check_1d"
T_M4 = "agg_token_model_share_1d"
T_M2 = "agg_token_gpu_group_1d"
MART_TABLES = (T_M1, T_M3, T_M4, T_M2)


# =============================================================================
# 공통 조각 — canon()·FAIL_PRED·SUB_* (설계 §6.1 299)
# =============================================================================

def canon(x: str) -> str:
    """canonical 정규화 식 — `dim_token_model_alias` 히트(a.canonical != '')면 canonical, 아니면
    원문 그대로(LEFT JOIN 미스는 join_use_nulls=0 규약으로 ''). INSERT와 EXPECTED가 **같은
    문자열**을 쓰도록 반드시 이 함수로 만든다(테스트가 동일성 단언). 호출측은 alias 서브쿼리를
    `AS a`로 조인해 두어야 한다."""
    return f"if(a.canonical = '', {x}, a.canonical)"


# FAIL 플래그 술어(설계 §6.4 (1) 파이프라인 보정) — app.mart.FAIL_FLAGS에서 생성해 참조 구현과
# 문자열 정본을 공유한다. gpu fact alias는 항상 `g`.
FAIL_PRED = "hasAny(g.flags, [" + ",".join(f"'{f}'" for f in FAIL_FLAGS) + "])"

# 가중 토큰 식 조각(설계 §6.4 (3), 정의서 3.5) — M1(outer SELECT)·M4(wt 서브쿼리)가 공유.
# 피연산자는 **같은 SELECT 안의 alias**(input_tokens/cache_read_tokens/cache_creation_tokens/
# output_tokens)를 가리키므로, 사용측은 이 조각 앞에서 4컬럼을 그 이름으로 alias 해 둔다.
_WTOK_EXPR = (
    f"{W_UNC} * (input_tokens + cache_creation_tokens)"
    f" + {W_CACHE} * cache_read_tokens + {W_OUT} * output_tokens"
)

# eff_alias — alias별 date 유효 최신 canonical (String, NULL 없음 → 그냥 argMax)
SUB_EFF_ALIAS = f"""(SELECT alias, argMax(canonical, effective_from) AS canonical
        FROM {DB_DIM}.dim_token_model_alias_dist
        WHERE effective_from <= {{d:Date}}
        GROUP BY alias)"""

# eff_tco — gpu_type별 date 유효 최신 TCO(원/GPU·h). 최신 이력 행이 NULL이면 NULL(설계 해석 2).
SUB_EFF_TCO = f"""(SELECT gpu_type,
               nullIf(argMax(ifNull(tco_krw_per_gpu_hour, -1), effective_from), -1) AS tco
        FROM {DB_DIM}.dim_token_gpu_tco_dist
        WHERE effective_from <= {{d:Date}}
        GROUP BY gpu_type)"""

# eff_alloc — (service_group, gpu_type)별 date 유효 최신 할당 GPU 수. `unknown` 기종 제외(설계 §6.1).
SUB_EFF_ALLOC = f"""(SELECT service_group, gpu_type,
               nullIf(argMax(ifNull(allocated_gpu_count, -1), effective_from), -1) AS allocated_gpu_count,
               argMax(source, effective_from) AS source
        FROM {DB_DIM}.dim_token_gpu_allocation_dist
        WHERE effective_from <= {{d:Date}}
        GROUP BY service_group, gpu_type
        HAVING gpu_type != 'unknown')"""

# eff_price — (provider, model)별 date 유효 최신 벤더 단가 4종(원/1M 토큰), 처리등급 standard 고정
# (설계 §6.4 (6)). 단가 NULL은 NULL 그대로(비용 NULL 전파 + M3 vendor_price_missing).
SUB_EFF_PRICE = f"""(SELECT provider, model,
               nullIf(argMax(ifNull(krw_per_mtok_input, -1), effective_from), -1)          AS p_in,
               nullIf(argMax(ifNull(krw_per_mtok_cached, -1), effective_from), -1)         AS p_cached,
               nullIf(argMax(ifNull(krw_per_mtok_cache_creation, -1), effective_from), -1) AS p_cc,
               nullIf(argMax(ifNull(krw_per_mtok_output, -1), effective_from), -1)         AS p_out
        FROM {DB_DIM}.dim_token_vendor_price_dist
        WHERE tier = 'standard' AND effective_from <= {{d:Date}}
        GROUP BY provider, model)"""

# reg — 메트릭 레지스트리 전체 행(설계 §4.3; 6b가 원자 교체). until은 Nullable(Date).
SUB_REG = f"""(SELECT service, service_group, enabled, coverage_since, until,
               expect_gpu, expect_serving, usage_includes_consumers
        FROM {DB_DIM}.dim_token_metrics_service_dist)"""

# usage_svc — 토큰 측 모집단(dim_token_service enabled=1; 읽기 계약 2컬럼 service/enabled).
SUB_USAGE_SVC = f"""(SELECT service FROM {DB_TOKEN_DIM}.dim_token_service_dist WHERE enabled = 1)"""

# anchor — 그날 앵커(summary, 응답당 1행). 메트릭 측 소스는 앵커가 있는 (date, service)만(설계 §6.1).
SUB_ANCHOR = f"""(SELECT service, service_group, reported_service_group, reported_service, source_type,
               gpu_rows, serving_rows, rejected_rows
        FROM {DB_FACT}.raw_token_metrics_summary_1d_dist
        WHERE date = {{d:Date}})"""

# 자식 행 실측 수(서비스 단위) — 앵커 gpu_rows/serving_rows와 대조(partial 판정, 설계 해석 4).
# 앵커 serving_rows = 표준 지표 행 수(metric != 'custom', Plan 6b NormalizeResult.n_serving)이고 custom 행은
# custom_rows로 따로 기록되므로(설계 §4.1 long form) serving 실측은 metric != 'custom' 행만 센다.
SUB_GPU_CNT = f"""(SELECT service, count() AS n
        FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist
        WHERE date = {{d:Date}}
        GROUP BY service)"""
SUB_SERVING_CNT = f"""(SELECT service, countIf(metric != 'custom') AS n
        FROM {DB_FACT}.raw_token_metrics_serving_1d_dist
        WHERE date = {{d:Date}}
        GROUP BY service)"""


# =============================================================================
# M1 — mart.agg_token_model_cost_1d (설계 §6.1 302, §6.4 (1)(3))
#   grain: date × service × model(canon). keys = tok 키 ∪ gpu 키(UNION DISTINCT)를 구동 테이블로
#   tok_agg/gpu_agg/reg/anchor/자식 카운트를 GLOBAL LEFT JOIN. INSERT와 EXPECTED가 같은 키 조각
#   (_TOK_KEYS/_GPU_KEYS)을 공유한다.
# =============================================================================

# 토큰 측 소스 = token_usage_1d(읽기 계약 9컬럼) 중 usage_svc 서비스 전부(소비 전용 포함).
_TOK_SRC = f"""FROM {DB_TOKEN_MART}.token_usage_1d_dist AS u
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = u.model"""
_TOK_TAIL = f"""WHERE u.date = {{d:Date}} AND u.service GLOBAL IN {SUB_USAGE_SVC}
    GROUP BY u.service, {canon('u.model')}"""

# 메트릭 측 소스 = gpu fact 중 앵커가 있는 서비스(FAIL 행 포함해 키 유지 — 시간은 NOT FAIL_PRED로 분리).
_GPU_SRC = f"""FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = g.model"""
_GPU_TAIL = f"""WHERE g.date = {{d:Date}} AND g.service GLOBAL IN (SELECT service FROM {SUB_ANCHOR})
    GROUP BY g.service, {canon('g.model')}"""

# 키 서브쿼리 조각 — SQL_M1의 keys(UNION DISTINCT)와 EXPECTED_SQL_M1(UNION ALL + uniqExact) 공유.
_TOK_KEYS = f"""SELECT u.service AS service, {canon('u.model')} AS model
    {_TOK_SRC}
    {_TOK_TAIL}"""
_GPU_KEYS = f"""SELECT g.service AS service, {canon('g.model')} AS model
    {_GPU_SRC}
    {_GPU_TAIL}"""

SQL_M1 = f"""
INSERT INTO {DB_MART}.{T_M1}_dist
    (date, service_group, service, model,
     serving_gpu_hours, standby_gpu_hours, test_gpu_hours, flagged_gpu_hours,
     equiv_gpu_count, scaled_intraday, model_cost_krw,
     input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, requests,
     uncached_tokens, cached_tokens, total_tokens, weighted_tokens, tokens_per_gpu_hour,
     gpu_type_mix, model_registered, tco_missing, has_token_rows, has_gpu_rows,
     quality_flag, created_by)
WITH
    tok_agg AS (
        SELECT u.service                     AS service,
               any(u.service_group)          AS service_group,
               {canon('u.model')}            AS model,
               sum(u.input_tokens)           AS input_tokens,
               sum(u.cache_read_tokens)      AS cache_read_tokens,
               sum(u.cache_creation_tokens)  AS cache_creation_tokens,
               sum(u.output_tokens)          AS output_tokens,
               sum(u.requests)               AS requests,
               max(a.canonical != '')        AS registered,
               1                             AS has_rows
        {_TOK_SRC}
        {_TOK_TAIL}
    ),
    gpu_agg AS (
        -- 시간 4분류: serving/standby/test는 비FAIL 행만, flagged는 FAIL 행 전체(카테고리 무관).
        -- C = Σ(serving+standby, 비FAIL) gpu_hours × TCO — 그 행 중 TCO NULL 기종이 하나라도 있으면
        -- (tco_null_cnt > 0) outer에서 NULL(부분 합 금지, 설계 §6.4 (1)). test 시간은 C 불포함.
        SELECT g.service                     AS service,
               any(g.service_group)          AS service_group,
               {canon('g.model')}            AS model,
               sumIf(g.gpu_hours, g.category = 'serving' AND NOT {FAIL_PRED})  AS serving_gpu_hours,
               sumIf(g.gpu_hours, g.category = 'standby' AND NOT {FAIL_PRED})  AS standby_gpu_hours,
               sumIf(g.gpu_hours, g.category = 'test' AND NOT {FAIL_PRED})     AS test_gpu_hours,
               sumIf(g.gpu_hours, {FAIL_PRED})                                 AS flagged_gpu_hours,
               countIf(g.category IN ('serving','standby') AND NOT {FAIL_PRED} AND isNull(t.tco))
                                                                               AS tco_null_cnt,
               sumIf(g.gpu_hours * t.tco,
                     g.category IN ('serving','standby') AND NOT {FAIL_PRED} AND isNotNull(t.tco))
                                                                               AS cost_sum,
               arraySort(groupUniqArray(g.gpu_type))                           AS gpu_type_mix,
               max(isNull(t.tco))                                              AS tco_missing,
               max(a.canonical != '')                                          AS registered,
               1                                                               AS has_rows
        {_GPU_SRC}
        GLOBAL LEFT JOIN {SUB_EFF_TCO} AS t ON t.gpu_type = g.gpu_type
        {_GPU_TAIL}
    ),
    keys AS (
        {_TOK_KEYS}
        UNION DISTINCT
        {_GPU_KEYS}
    )
SELECT
    {{d:Date}}                                                        AS date,
    multiIf(r.service_group != '', r.service_group,
            ga.service_group != '', ga.service_group,
            tk.service_group)                                         AS service_group,
    k.service                                                         AS service,
    k.model                                                           AS model,
    ga.serving_gpu_hours                                              AS serving_gpu_hours,
    ga.standby_gpu_hours                                              AS standby_gpu_hours,
    ga.test_gpu_hours                                                 AS test_gpu_hours,
    ga.flagged_gpu_hours                                              AS flagged_gpu_hours,
    (ga.serving_gpu_hours + ga.standby_gpu_hours + ga.test_gpu_hours) / 24
                                                                      AS equiv_gpu_count,
    0                                                                 AS scaled_intraday,
    if(ga.has_rows = 0 OR ga.tco_null_cnt > 0, NULL, ifNull(ga.cost_sum, 0))
                                                                      AS model_cost_krw,
    tk.input_tokens                                                   AS input_tokens,
    tk.cache_read_tokens                                              AS cache_read_tokens,
    tk.cache_creation_tokens                                          AS cache_creation_tokens,
    tk.output_tokens                                                  AS output_tokens,
    tk.requests                                                       AS requests,
    input_tokens + cache_creation_tokens                              AS uncached_tokens,
    cache_read_tokens                                                 AS cached_tokens,
    input_tokens + cache_read_tokens + cache_creation_tokens + output_tokens
                                                                      AS total_tokens,
    {_WTOK_EXPR}                                                      AS weighted_tokens,
    if(ga.serving_gpu_hours > 0, total_tokens / ga.serving_gpu_hours, NULL)
                                                                      AS tokens_per_gpu_hour,
    ga.gpu_type_mix                                                   AS gpu_type_mix,
    greatest(tk.registered, ga.registered)                            AS model_registered,
    ga.tco_missing                                                    AS tco_missing,
    tk.has_rows                                                       AS has_token_rows,
    ga.has_rows                                                       AS has_gpu_rows,
    -- 우선순위 고정(설계 §6.1): partial > no_tco > flagged > manual > no_metrics > consumer_only > normal
    multiIf(
        an.service != '' AND (an.gpu_rows != gc.n OR an.serving_rows != sc.n),          'partial',
        ga.has_rows = 1 AND ga.tco_null_cnt > 0,                                        'no_tco',
        ga.flagged_gpu_hours > 0,                                                       'flagged',
        an.source_type = 'manual-v0',                                                   'manual',
        r.service != '' AND r.enabled = 1 AND r.coverage_since <= {{d:Date}}
            AND (isNull(r.until) OR {{d:Date}} <= r.until) AND an.service = '',         'no_metrics',
        r.service = '',                                                                 'consumer_only',
        'normal')                                                     AS quality_flag,
    '{CREATED_BY}'                                                    AS created_by
FROM keys AS k
GLOBAL LEFT JOIN tok_agg AS tk ON tk.service = k.service AND tk.model = k.model
GLOBAL LEFT JOIN gpu_agg AS ga ON ga.service = k.service AND ga.model = k.model
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = k.service
GLOBAL LEFT JOIN {SUB_ANCHOR} AS an ON an.service = k.service
GLOBAL LEFT JOIN {SUB_GPU_CNT} AS gc ON gc.service = k.service
GLOBAL LEFT JOIN {SUB_SERVING_CNT} AS sc ON sc.service = k.service
"""

EXPECTED_SQL_M1 = f"""
SELECT uniqExact((service, model)) FROM (
    {_TOK_KEYS}
    UNION ALL
    {_GPU_KEYS}
)
"""
# ↑ M1 행 그레인은 date×service×model(canon) — keys(UNION DISTINCT)의 distinct 키 수와 같다.
# 좌측 keys에 붙는 tok_agg/gpu_agg(GROUP BY 키 유니크)·reg(ORDER BY service 유일)·anchor
# (date×service 1행)·자식 카운트(GROUP BY service)는 전부 키 유니크라 fan-out이 없다.


# =============================================================================
# 실행 함수 — 공용 시퀀스 _run_table + run_m1 (run_m3/run_m4/run_m2는 T4/T6/T7)
# =============================================================================

def _run_table(gate, date: str, dist: str, local: str, sql: str, expected_sql: str,
                warns: list, extra_pred: str = "") -> int:
    """공용 시퀀스: exists → (delete_day) → insert_select → expected 소스 카운트
    조회(gate.query) → verify_count.

    verify_count의 expected는 insert_select의 written_rows가 아니라 expected_sql의
    소스 카운트 결과를 쓴다(Distributed 이중 계상 회피 — 모듈 상단 docstring 참조).
    written_rows는 텔레메트리로만 로그에 남긴다.

    verify_count 실패는 StepError(FAILURE 전파). 초과분(actual > expected)은
    "dup_suspect:<table>" 경고를 warns에 추가한다.

    반환은 **verify_count의 actual**(실제 적재 행수 — 소스 카운트 기반)이다.
    written_rows는 Distributed 경로에서 신뢰 불가(단일노드에서 0, 다샤드에서 이중
    계상)라 마커 행수로 쓸 수 없다 — 텔레메트리로 로그에만 남긴다."""
    if gate.exists(dist, date):
        gate.delete_day(local, date, extra_pred=extra_pred)
    written = gate.insert_select(sql, {"d": date})
    expected_rows = gate.query(expected_sql, {"d": date})
    expected = int(expected_rows[0][0]) if expected_rows else 0
    ok, actual = gate.verify_count(dist, date, expected)
    if not ok:
        raise StepError(
            f"verify_count failed: {dist} date={date} "
            f"written_rows={written} expected={expected} actual={actual}")
    if actual > expected:
        warns.append(f"dup_suspect:{dist}")
    print(
        f"STEP table ok: {dist} date={date} written_rows={written} "
        f"expected={expected} actual={actual}",
        flush=True)
    return actual


def run_m1(gate, date: str) -> dict:
    """M1 — mart.agg_token_model_cost_1d 1테이블. 반환 {"rows_mart": actual, "warns": [...]}
    (마커 rows_mart의 소스). 메트릭 fact가 없는 날도 토큰-only 행이 적재되므로 실행을 건너뛰지
    않는다(설계 §6.1 — 절대 FAILURE 아님)."""
    warns: list = []
    rows = _run_table(gate, date, f"{DB_MART}.{T_M1}_dist", f"{DB_MART}.{T_M1}_local",
                      SQL_M1, EXPECTED_SQL_M1, warns)
    return {"rows_mart": rows, "warns": warns}
```

- [ ] **Step 5: GREEN 확인**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_steps.py 2>&1 | tail -n 1
```

기대 출력: `20 passed`.

- [ ] **Step 6: 보간 결과 눈검토 + Produces 이름 존재 확인 + 문법·3.10 게이트**

f-string 보간이 끝난 **실제 SQL**을 한 번 출력해 눈으로 본다(ClickHouse는 로컬에 없으므로 실행은 T10 e2e·CI가 담당; 여기서는 `{d:Date}`가 그대로 남았는지, `{{`/`}}` 잔재·`coalesce`가 없는지, INSERT 컬럼 목록과 outer SELECT alias 순서가 같은지를 확인한다):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python3 -c "
from app import steps
print(steps.SQL_M1)
print('=====')
print(steps.EXPECTED_SQL_M1)
" | sed -n 1,12p
```

기대 출력(앞 12줄 — 기본 DB명 `mart` 전제):

```

INSERT INTO mart.agg_token_model_cost_1d_dist
    (date, service_group, service, model,
     serving_gpu_hours, standby_gpu_hours, test_gpu_hours, flagged_gpu_hours,
     equiv_gpu_count, scaled_intraday, model_cost_krw,
     input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, requests,
     uncached_tokens, cached_tokens, total_tokens, weighted_tokens, tokens_per_gpu_hour,
     gpu_type_mix, model_registered, tco_missing, has_token_rows, has_gpu_rows,
     quality_flag, created_by)
WITH
    tok_agg AS (
        SELECT u.service                     AS service,
```

Produces 목록의 이름이 전부 모듈에 존재하는지 + 바인딩/조인 개수 고정값을 기계적으로 확인한다(T4/T5/T6/T7/T10이 import 하는 이름):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python3 -c "
from app import steps
names = ['CREATED_BY', 'T_M1', 'T_M3', 'T_M4', 'T_M2', 'MART_TABLES', 'StepError', 'canon', 'FAIL_PRED',
         '_WTOK_EXPR', 'SUB_EFF_ALIAS', 'SUB_EFF_TCO', 'SUB_EFF_ALLOC', 'SUB_EFF_PRICE', 'SUB_REG',
         'SUB_USAGE_SVC', 'SUB_ANCHOR', 'SUB_GPU_CNT', 'SUB_SERVING_CNT', '_TOK_KEYS', '_GPU_KEYS',
         'SQL_M1', 'EXPECTED_SQL_M1', '_run_table', 'run_m1']
missing = [n for n in names if not hasattr(steps, n)]
assert not missing, missing
assert steps.MART_TABLES == ('agg_token_model_cost_1d', 'token_metrics_check_1d', 'agg_token_model_share_1d', 'agg_token_gpu_group_1d')
print('produces ok', len(names))
print('d:Date', steps.SQL_M1.count('{d:Date}'), steps.EXPECTED_SQL_M1.count('{d:Date}'))
print('GLOBAL LEFT JOIN', steps.SQL_M1.count('GLOBAL LEFT JOIN'), 'GLOBAL IN', steps.SQL_M1.count('GLOBAL IN'))
print('braces', '{{' in steps.SQL_M1, '}}' in steps.SQL_M1, 'coalesce', 'coalesce' in steps.SQL_M1.lower())
"
```

기대 출력(4줄):

```
produces ok 25
d:Date 17 5
GLOBAL LEFT JOIN 11 GLOBAL IN 4
braces False False coalesce False
```

(`{d:Date}` 17 = tok_agg 2(alias·`u.date`) + gpu_agg 4(alias·tco·`g.date`·앵커 GLOBAL IN) + keys 5(`_TOK_KEYS` 2 + `_GPU_KEYS` 3) + outer 6(`AS date` 1 + quality_flag 2 + SUB_ANCHOR·SUB_GPU_CNT·SUB_SERVING_CNT 각 1); EXPECTED 5 = `_TOK_KEYS` 2 + `_GPU_KEYS` 3. GLOBAL LEFT JOIN 11 = alias 4(tok_agg·gpu_agg·keys 2) + tco 1 + outer 6; GLOBAL IN 4 = usage_svc 2 + 앵커 2.)

문법·Python 3.10 게이트(`match`/`StrEnum`/`tomllib`/`datetime.UTC`/`random` 금지):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python3 -c "import ast; ast.parse(open('app/steps.py').read()); ast.parse(open('tests/test_steps.py').read()); print('AST_OK')" && ! grep -n "import random\|StrEnum\|tomllib\|datetime.UTC\|^match " app/steps.py tests/test_steps.py && echo "py310 ok"
```

기대 출력: `AST_OK` 다음 줄 `py310 ok`.

- [ ] **Step 7: 모듈 전체 테스트 + zero-diff 게이트 + 커밋**

Task 1·2 테스트(config/ch/preflight/mart)와 함께 모듈 전체를 돌린다:

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q 2>&1 | tail -n 1
```

기대 출력: `68 passed`(T1 27 + T2 21 + T3 20; T1/T2가 실제로 작성한 테스트 수가 다르면 그 합 + 20). `tests/test_ch.py`의 서브프로세스 env 테스트는 `app.steps`를 import 하지 않으므로(T1이 원형의 `steps.SQL_DETAIL` 단언을 제거) 이 태스크가 그 역할을 `test_db_env_override_isolates_token_side_in_sql_m1`로 대신한다.

zero-diff 게이트(기존 자산 무변경):

```bash
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-mart.yml .github/workflows/test-collector.yml && git status --short -- mart/token-metrics/app/steps.py mart/token-metrics/tests/test_steps.py
```

기대 출력: `git diff --stat` 출력 없음(비어 있음); `git status --short`에 `?? mart/token-metrics/app/steps.py`와 `?? mart/token-metrics/tests/test_steps.py` 두 줄(또는 디렉터리 단위 `?? mart/token-metrics/`가 이미 T1/T2 커밋으로 해소되어 파일 2건만).

커밋:

```bash
cd /home/mini/github/token-data-pipeline && git add mart/token-metrics/app/steps.py mart/token-metrics/tests/test_steps.py && git commit -m "feat(mart-metrics): steps.py 공통 서브쿼리·canon·M1 agg_token_model_cost_1d SQL·_run_table (Plan 6c T3)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

기대: `git log --oneline -1`이 `feat(mart-metrics): steps.py 공통 서브쿼리·canon·M1 agg_token_model_cost_1d SQL·_run_table (Plan 6c T3)`를 보이고, `git status --porcelain -- mart/token-metrics/app/steps.py mart/token-metrics/tests/test_steps.py`가 비어 있다.

**설계 해석 (T3 — footer "Self-Review 노트"에 병합)**:
- `model_registered` = **alias 테이블 히트**(`dim_token_model_alias`에 `effective_from <= d` 행이 있어 `a.canonical != ''`)로 판정한다 — 토큰 측·gpu 측 어느 쪽이든 히트면 1(`greatest(tk.registered, ga.registered)`). `dim_token_model`(토큰 mart dim)은 읽기 계약 13컬럼 밖이라 참조하지 않는다(footer 해석 1과 동일).
- 이력 dim 최신 행의 NULL 전파: `nullIf(argMax(ifNull(x, -1), effective_from), -1)` — `argMax`가 NULL 인자를 건너뛰어 과거 실값이 되살아나는 문제를 막는다(footer 해석 2). TCO·할당 GPU 수·단가는 전부 음수가 아니므로 `-1` 센티널이 실값과 충돌하지 않는다.
- `SUB_*`는 CTE가 아니라 **괄호 포함 서브쿼리 문자열 상수**로 두고 호출측이 `AS a/t/al/p/r/u/an` 별칭을 붙인다(footer 해석 3) — INSERT와 EXPECTED가 같은 조각을 공유해야 하는데 CTE는 두 SQL에 따로 선언해야 해 파생 오차가 생기기 때문. `SUB_EFF_ALLOC`·`SUB_EFF_PRICE`는 M1이 쓰지 않지만 T6(M4)·T7(M2)이 같은 별칭(`allocated_gpu_count`, `p_in/p_cached/p_cc/p_out`)으로 소비하므로 여기서 정의·테스트한다.
- `partial` 판정(footer 해석 4를 따름 — 아웃라인의 "gpu_rows만" 문구보다 우선): 앵커가 있고 **`an.gpu_rows != 실측 gpu 행수 OR an.serving_rows != 실측 serving 행수`**(서비스 단위, T4 M3 `partial_load`와 같은 술어). 실측은 `SUB_GPU_CNT`/`SUB_SERVING_CNT`(fact `_dist` `GROUP BY service`)로 붙인다. serving 실측은 `countIf(metric != 'custom')` — 수집기(Plan 6b T5)가 앵커 `serving_rows`에 `NormalizeResult.n_serving`(= `metric != "custom"` 행 수)을, custom 행은 `custom_rows`에 따로 기록하므로 `count()`로 세면 custom 지표가 있는 서비스가 전부 partial로 오판된다. 앵커 미스는 `an.service = ''`라 partial 후보에서 빠지고, 자식 카운트 미스는 `gc.n = 0`이라 앵커의 `gpu_rows > 0`과 불일치 → partial(설계 의도: 자식 적재 누락 = partial).
- `model_cost_krw`: `sumIf(g.gpu_hours * t.tco, …)`는 `t.tco`가 Nullable이라 조건에 맞는 행이 없으면 NULL을 돌려준다(예: test 카테고리 행만 있는 모델). 설계 §6.4 (1)의 "C=0(`no_provider`)"를 지키기 위해 outer에서 `if(ga.has_rows = 0 OR ga.tco_null_cnt > 0, NULL, ifNull(ga.cost_sum, 0))` — gpu 행 자체가 없으면 NULL, serving/standby 비FAIL 행 중 TCO NULL 기종이 하나라도 있으면 NULL(부분 합 금지), 그 외 NULL 합은 0. `ifNull`은 허용(금지는 `coalesce(`뿐 — Nullable 조인 미스 은폐 방지 규칙의 취지대로, 여기서는 집계 결과의 NULL만 0으로 바꾼다). T2 `model_cost([])`가 `None`, test-only 행이 `0.0`을 돌려주는 참조 구현과 일치.
- `EXPECTED_SQL_M1`의 키는 `(service, model)`만 — `date`는 `{d:Date}` 상수라 키에서 제외해도 그날 행수와 같다. 좌측 `keys`에 붙는 `tok_agg`/`gpu_agg`(GROUP BY 키 유니크)·`reg`(레지스트리 ORDER BY service 유일)·`anchor`(date×service 1행 — 응답당 1행이 Plan 6a 앵커 계약)·자식 카운트(GROUP BY service)는 전부 키 유니크라 fan-out이 없으므로 `uniqExact((service, model))` = 적재 행수다. 앵커가 같은 (date, service)에 2행이면 M1이 2배가 되어 `verify_count`의 `actual > expected` 분기가 `dup_suspect:<table>` WARN으로 드러낸다(수집기 재적재는 날짜 단위 DELETE 후 INSERT라 정상 경로에서는 발생하지 않는다).
- `service_group` 폴백 순서: 레지스트리 `r.service_group` > gpu fact `any(g.service_group)` > 토큰 mart `any(u.service_group)` — 설계 §6.1 "레지스트리(메트릭) 우선, 없으면 토큰 mart 값"의 M1 DDL COMMENT를 따르되, 레지스트리 미등록·gpu 행 있음(앵커만 있는 수동 반입) 케이스를 위해 fact 값을 중간에 둔다.
- `quality_flag`의 `no_metrics`: 레지스트리 `enabled = 1 AND coverage_since <= d AND (until IS NULL OR d <= until)`인데 그날 앵커가 없는 서비스의 토큰-only 행. `consumer_only`: 레지스트리 미등록(`r.service = ''`). 둘 다 M0 커버리지(T5)와 같은 "기대 집합" 술어를 쓴다.
- `_WTOK_EXPR`의 피연산자는 **같은 SELECT의 alias**(`input_tokens` 등 — `tk.input_tokens AS input_tokens` 뒤에 위치)라 ClickHouse alias 참조 규칙으로 해석된다; `uncached_tokens`/`total_tokens`/`tokens_per_gpu_hour`도 같은 방식으로 앞선 alias를 재사용한다(원형 `SQL_AGG_ORG`의 `distinct_users` alias 재사용 패턴).
- `test_db_env_override_isolates_token_side_in_sql_m1`은 서브프로세스 2회(`CH_DB_TOKEN_*` 지정/미지정)로 company-verify 격리 DB 규약을 SQL 문자열 수준에서 고정한다 — T1 `test_ch.py`가 원형의 `steps.SQL_DETAIL` 서브프로세스 단언을 뺐으므로 그 역할을 이 파일이 맡는다.
- 테스트 명령은 아웃라인 규칙대로 `python -m pytest -q`, 한 줄 스크립트는 Plan 6a 관례대로 `python3 -c`(개발 머신에 `python` alias가 없어도 동작).
### Task 4: app/steps.py — M3 token_metrics_check_1d 핵심 13블록 + build_m3_sql/build_m3_expected/run_m3

**설계 근거**: 설계 §6.1 303(M3 = 검사별 독립 SELECT를 UNION ALL로 적재, EXPECTED = 같은 UNION의 count; 메트릭 측 소스는 앵커가 있는 (date, service)만 읽는다), §4.3 196-213(등록부 `dim_token_metrics_service` 컬럼 — `enabled/coverage_since/until/expect_gpu/expect_serving`의 의미, M0 기대 집합 술어), §4.2 표 162-172(`hours_over_count`·`unknown_violation`·`pct_non_monotone` 정의와 alias 시드 규칙 — 미등록 모델은 canon = 원문), §5.3 262-270(구조 거부 `rejected_rows`, 정규화 플래그, 앵커 vs 자식 행수), §4.1 167(`identity_drift` = `source_type='metrics-api-v1'`이고 응답 자기신고 `reported_*`가 헤더/등록부와 다름), §5.2(수동 반입 `manual-v0` 표기), Plan 6a C M3 12컬럼 DDL `token_metrics_check_1d_local`(`date, service_group, service, check_name, model, gpu_type, severity, observed, threshold, detail, source_type, created_by`, ORDER BY `(date, service, check_name, model, gpu_type)`, `observed/threshold Nullable(Float64)`, `detail` "수·이름만"), 마스터 §5.6 398-419(로그·검사 표에 user_id 원문·페이로드 금지 — `identity_drift.detail`은 불일치 여부만 싣고 `reported_*` 원문은 싣지 않는다).
**읽을 원형**: digest §4 `mart/token-usage/app/steps.py:366-452`(EXPECTED 조립 원칙·`_run_table` — T3가 이미 클론), §24 `tools/verify/invariants.sql:1-189`(헤더 + 6블록을 `UNION ALL`로 잇는 블록 리스트 스타일 — M3도 "블록 리스트 → UNION ALL" 조립), T3 산출 `mart/token-metrics/app/steps.py`(`SUB_*`·`canon`·`FAIL_PRED`·`_run_table`·`T_M3`·`CREATED_BY` 재사용), T3 산출 `mart/token-metrics/tests/test_steps.py`(`FakeGate`·`DATE` 재사용), Plan 6a `mart/token-metrics/ddl/company/mart_metrics_tables.sql`(M3 DDL — 테스트가 파일을 파싱해 INSERT 컬럼 순서를 대조).

M3는 T5 `batch.py`가 `rows_check`·`warn=` 마커 소스로 소비하고(`CHECK WARN ` 접두 라인 수), T6/T7이 `M3_BLOCKS_STRETCH`에 확장 블록을 append 한다. 이 태스크는 **핵심 13블록**과 조립기·러너까지만 만든다 — 확장 블록 이름은 DDL COMMENT에만 예고되어 있고 여기서는 빈 리스트다.

**Files:**
- Modify: `mart/token-metrics/app/steps.py` — T3 산출 파일의 **끝에** M3 절을 append(기존 T3 코드는 손대지 않는다).
- Modify: `mart/token-metrics/tests/test_steps.py` — T3 산출 파일의 **끝에** M3 테스트를 append(`FakeGate`·`DATE`·`steps` import를 그대로 재사용).
- Test: `mart/token-metrics/tests/test_steps.py`
- (읽기만) `mart/token-metrics/ddl/company/mart_metrics_tables.sql`(Plan 6a — 테스트 참조), `tools/verify/invariants.sql`, `mart/token-usage/app/steps.py` — zero-diff 대상, 절대 수정하지 않는다.

**Interfaces:**
- Consumes (T3 `app/steps.py`):
  - `CREATED_BY = "token-metrics-pipeline"`, `T_M3 = "token_metrics_check_1d"`, `StepError`.
  - `canon(x: str) -> str` = `f"if(a.canonical = '', {x}, a.canonical)"` — alias 서브쿼리 별칭이 `a`일 때만 유효.
  - `FAIL_PRED` = `hasAny(g.flags, ['hours_over_count','unknown_violation'])` — gpu 팩트 별칭 `g` 전제.
  - `SUB_EFF_ALIAS`(컬럼 `alias, canonical`), `SUB_EFF_TCO`(`gpu_type, tco` — `tco`는 Nullable), `SUB_REG`(`service, service_group, enabled, coverage_since, until, expect_gpu, expect_serving, usage_includes_consumers`), `SUB_USAGE_SVC`(`service`, enabled=1), `SUB_ANCHOR`(`service, service_group, reported_service_group, reported_service, source_type, gpu_rows, serving_rows, rejected_rows`; `WHERE date = {d:Date}` 내장).
  - `_run_table(gate, date, dist, local, sql, expected_sql, warns, extra_pred="") -> int` — exists → delete_day → insert_select(params `{"d": date}`) → `gate.query(expected_sql, {"d": date})` → verify_count; 실패 시 `StepError`; `dup_suspect:<dist>` warn.
  - `app.ch.DB_FACT / DB_DIM / DB_MART / DB_TOKEN_MART / DB_TOKEN_DIM`(T1), `app.preflight.READ_CONTRACT`(T1 — 테스트에서 토큰 측 컬럼 검증), `app.mart.FAIL_FLAGS`(T2 — T3의 `FAIL_PRED` 경유).
  - 테스트: T3 `tests/test_steps.py`의 `FakeGate(exists=True, verify_ok=True, verify_actual=None, expected_overrides=None)`(`order/delete_preds/written/query_calls/verify_calls` 기록, `_TABLE_KEYS` 부분문자열 라우팅에 `token_metrics_check_1d` 포함)와 `DATE`, `from app import steps`.
- Produces (`app.steps` — 아래 시그니처가 정본):
  - `M3_COLUMNS: tuple[str, ...]` — 12컬럼 DDL 순서 `("date", "service_group", "service", "check_name", "model", "gpu_type", "severity", "observed", "threshold", "detail", "source_type", "created_by")`; `M3_INSERT_COLUMNS = ", ".join(M3_COLUMNS)`.
  - `_m3_select(check_name: str, severity: str, *, service_group: str, service: str, observed: str, threshold: str, detail: str, body: str, model: str = "''", gpu_type: str = "''", source_type: str = "''") -> str` — 12컬럼 SELECT 헤더(`{d:Date} AS date` … `'token-metrics-pipeline' AS created_by`) + `body`(FROM 절부터). 값 인자는 SQL 식 문자열. `severity ∉ {FAIL, WARN, INFO}` → `ValueError`. T6/T7의 확장 블록도 이 헬퍼로 만든다.
  - `_M3_ANCHORED: str` = `(SELECT service FROM {SUB_ANCHOR})` — 팩트 블록의 `g.service GLOBAL IN {_M3_ANCHORED}` 우변.
  - `_M3_CHILD_COUNTS: str` — 서비스별 `(service, service_group, actual_gpu, actual_serving)`; `actual_serving = countIf(metric != 'custom')`(Plan 6b `n_serving`과 동일 정의).
  - `M3_BLOCKS_CORE: list[tuple[str, str]]` — 13개 `(check_name, select_sql)`, 순서 고정: `metrics_missing`(FAIL), `partial_load`(FAIL), `rows_rejected`(WARN), `unregistered_model`(WARN), `hours_over_count`(FAIL), `unknown_violation`(FAIL), `pct_non_monotone`(FAIL), `gpu_type_no_tco`(WARN), `serving_missing_for_gpu_model`(WARN), `serving_without_gpu_serving_row`(WARN), `identity_drift`(WARN), `service_not_in_usage_registry`(WARN), `manual_source`(INFO).
  - `M3_BLOCKS_STRETCH: list[tuple[str, str]] = []` — T4 시점 빈 리스트(T6/T7 append).
  - `build_m3_sql(blocks: list[tuple[str, str]]) -> str` = `f"INSERT INTO {DB_MART}.{T_M3}_dist ({M3_INSERT_COLUMNS})\n" + "\nUNION ALL\n".join(sql for _, sql in blocks)`; 빈 리스트 → `ValueError`.
  - `build_m3_expected(blocks: list[tuple[str, str]]) -> str` = `"SELECT count() FROM (\n" + <같은 join 문자열> + "\n)"` — INSERT 본문과 문자 단위 동일.
  - `SQL_M3_SUMMARY: str` = `SELECT check_name, severity, count() AS n FROM {DB_MART}.{T_M3}_dist WHERE date = {d:Date} GROUP BY check_name, severity ORDER BY check_name, severity`.
  - `run_m3(gate, date: str, blocks: list[tuple[str, str]] | None = None) -> dict` — `blocks=None`이면 `M3_BLOCKS_CORE + M3_BLOCKS_STRETCH`; `_run_table(gate, date, f"{DB_MART}.{T_M3}_dist", f"{DB_MART}.{T_M3}_local", build_m3_sql(blocks), build_m3_expected(blocks), warns)` 후 `gate.query(SQL_M3_SUMMARY, {"d": date})`의 각 `(check_name, severity, n)`을 `CHECK WARN <check_name> severity=<sev> count=<n>`(FAIL/WARN) 또는 `CHECK INFO <check_name> severity=INFO count=<n>`(INFO)로 `print(..., flush=True)`하고 `warns`에 append; 반환 `{"rows_check": int, "warns": list[str]}`. **T5 `batch.py`는 이 라인을 다시 출력하지 않고 `startswith("CHECK WARN ")`만 센다.**
  - 테스트 헬퍼(`tests/test_steps.py`): `_m3_insert_columns(sql) -> list[str]`, `_m3_ddl_columns(table_local) -> list[str]`, `_m3_select_header_aliases(block_sql) -> list[str]`, `M3Gate(FakeGate)`(`summary_rows`, `rows`, `deleted`, `inserted`).

- [ ] **Step 1: RED — M3 블록·빌더 계약 테스트 추가 (`tests/test_steps.py` 끝에 append)**

T3의 `FakeGate`·`DATE`·`from app import steps`가 같은 파일 위에 이미 있다. 모든 접근은 `steps.M3_BLOCKS_CORE`처럼 **속성 접근**으로 써서(모듈 레벨 `from app.steps import M3_BLOCKS_CORE` 금지) 아직 없는 이름 때문에 T3 테스트까지 import 단계에서 죽지 않게 한다.

```bash
cd /home/mini/github/token-data-pipeline
cat >> mart/token-metrics/tests/test_steps.py <<'PYEOF'
# ============================================================================
# M3 token_metrics_check_1d — 핵심 13블록·빌더 계약 (Plan 6c T4)
# ============================================================================
import re
from pathlib import Path

from app.ch import DB_MART, DB_TOKEN_MART
from app.preflight import READ_CONTRACT

_M3_DDL_PATH = Path(__file__).resolve().parents[1] / "ddl" / "company" / "mart_metrics_tables.sql"

M3_CORE_NAMES = [
    "metrics_missing", "partial_load", "rows_rejected", "unregistered_model",
    "hours_over_count", "unknown_violation", "pct_non_monotone", "gpu_type_no_tco",
    "serving_missing_for_gpu_model", "serving_without_gpu_serving_row", "identity_drift",
    "service_not_in_usage_registry", "manual_source",
]

M3_SEVERITY = {
    "metrics_missing": "FAIL", "partial_load": "FAIL", "rows_rejected": "WARN",
    "unregistered_model": "WARN", "hours_over_count": "FAIL", "unknown_violation": "FAIL",
    "pct_non_monotone": "FAIL", "gpu_type_no_tco": "WARN", "serving_missing_for_gpu_model": "WARN",
    "serving_without_gpu_serving_row": "WARN", "identity_drift": "WARN",
    "service_not_in_usage_registry": "WARN", "manual_source": "INFO",
}

# 블록이 model / gpu_type 컬럼을 실제 값으로 채워야 하는 검사 (키 단위가 model/gpu_type인 것)
M3_KEYED_MODEL = {"unregistered_model", "hours_over_count", "unknown_violation", "pct_non_monotone",
                  "serving_missing_for_gpu_model", "serving_without_gpu_serving_row"}
M3_KEYED_GPU_TYPE = {"hours_over_count", "unknown_violation", "gpu_type_no_tco"}


def _m3_insert_columns(sql: str) -> list[str]:
    """'INSERT INTO db.tbl (a, b, c)\\nSELECT' 첫 줄의 괄호 안 컬럼 목록."""
    head = sql.split("\n", 1)[0]
    inner = head[head.index("(") + 1: head.rindex(")")]
    return [c.strip() for c in inner.split(",")]


def _m3_ddl_columns(table_local: str) -> list[str]:
    """Plan 6a DDL 파일에서 CREATE TABLE IF NOT EXISTS <table_local> 의 컬럼 선언 순서(CONSTRAINT 제외)."""
    lines = _M3_DDL_PATH.read_text(encoding="utf-8").splitlines()
    cols, state = [], "search"          # search → (CREATE 매칭) header → ("(" 라인) columns
    for line in lines:
        s = line.strip()
        if state == "search":
            if s.startswith("CREATE TABLE IF NOT EXISTS") and s.split()[-1].split(".")[-1] == table_local:
                state = "header"
            continue
        if state == "header":
            if s.startswith("("):
                state = "columns"
            continue
        if s.startswith(")"):
            break
        if not s or s.startswith("--") or s.startswith("CONSTRAINT"):
            continue
        cols.append(s.split()[0].strip("`"))
    assert cols, f"table not found in DDL: {table_local}"
    return cols


def _m3_select_header_aliases(block_sql: str) -> list[str]:
    """블록 SELECT 헤더(FROM 직전까지)의 'AS <alias>' 목록 — 12컬럼 순서 확인용."""
    header = block_sql.split("\nFROM", 1)[0]
    return [ln.strip().rstrip(",").rsplit(" AS ", 1)[1] for ln in header.splitlines()[1:]]


def test_m3_core_block_names_exact():
    assert [name for name, _ in steps.M3_BLOCKS_CORE] == M3_CORE_NAMES
    assert len(M3_CORE_NAMES) == 13
    assert steps.M3_BLOCKS_STRETCH == []


def test_m3_every_block_has_twelve_columns_and_own_name():
    for name, sql in steps.M3_BLOCKS_CORE:
        assert sql.startswith("SELECT\n"), name
        assert _m3_select_header_aliases(sql) == list(steps.M3_COLUMNS), name
        assert f"'{name}' AS check_name" in sql, name
        assert "'token-metrics-pipeline' AS created_by" in sql, name
        assert "    {d:Date} AS date," in sql, name


def test_m3_insert_column_list_matches_ddl_order():
    sql = steps.build_m3_sql(steps.M3_BLOCKS_CORE)
    assert sql.startswith(f"INSERT INTO {DB_MART}.token_metrics_check_1d_dist (")
    cols = _m3_insert_columns(sql)
    assert cols == _m3_ddl_columns("token_metrics_check_1d_local")
    assert len(cols) == 12
    assert cols == list(steps.M3_COLUMNS)


def test_m3_model_and_gpu_type_columns_populated_where_keyed():
    for name, sql in steps.M3_BLOCKS_CORE:
        header = sql.split("\nFROM", 1)[0]
        model_line = next(ln for ln in header.splitlines() if ln.endswith(" AS model,"))
        gpu_line = next(ln for ln in header.splitlines() if ln.endswith(" AS gpu_type,"))
        if name in M3_KEYED_MODEL:
            assert "''" not in model_line, name
        else:
            assert model_line.strip() == "'' AS model,", name
        if name in M3_KEYED_GPU_TYPE:
            assert "''" not in gpu_line, name
        else:
            assert gpu_line.strip() == "'' AS gpu_type,", name


def test_m3_severity_map():
    for name, sql in steps.M3_BLOCKS_CORE:
        assert f"'{M3_SEVERITY[name]}' AS severity" in sql, name
    assert sorted(set(M3_SEVERITY.values())) == ["FAIL", "INFO", "WARN"]
    with pytest.raises(ValueError):
        steps._m3_select("x", "ERROR", service_group="''", service="''", observed="0",
                         threshold="0", detail="''", body="FROM system.one")


def test_m3_expected_is_count_of_same_union():
    sql = steps.build_m3_sql(steps.M3_BLOCKS_CORE)
    expected = steps.build_m3_expected(steps.M3_BLOCKS_CORE)
    body = sql.split("\n", 1)[1]
    assert expected.startswith("SELECT count() FROM (\n")
    assert expected.endswith("\n)")
    assert body in expected
def test_m3_identity_drift_detail_has_no_reported_values():
    sql = dict(steps.M3_BLOCKS_CORE)["identity_drift"]
    detail_line = next(ln for ln in sql.splitlines() if ln.endswith(" AS detail,"))
    # detail은 불일치 여부(toString(toUInt8(비교식)))만 — reported_* 원문을 문자열로 싣지 않는다
    assert "toString(an.reported_service)" not in detail_line
    assert "toString(an.reported_service_group)" not in detail_line
    assert re.search(r"concat\(.*'svc_diff=', toString\(toUInt8\(an\.reported_service != an\.service\)\)", detail_line)
    assert "' group_diff=', toString(toUInt8(an.reported_service_group != r.service_group))" in detail_line
    assert "an.source_type = 'metrics-api-v1'" in sql
    assert "'identity_drift' AS check_name" in sql


def test_m3_builder_with_subset_blocks():
    two = steps.build_m3_sql(steps.M3_BLOCKS_CORE[:2])
    assert two.count("\nUNION ALL\n") == 1
    assert "'metrics_missing' AS check_name" in two and "'partial_load' AS check_name" in two
    assert "'rows_rejected' AS check_name" not in two
    full = steps.build_m3_sql(steps.M3_BLOCKS_CORE)
    assert full.count("\nUNION ALL\n") == 12
    assert steps.build_m3_expected(steps.M3_BLOCKS_CORE[:2]).count("\nUNION ALL\n") == 1
    with pytest.raises(ValueError):
        steps.build_m3_sql([])
    with pytest.raises(ValueError):
        steps.build_m3_expected([])


def test_m3_inner_union_all_never_at_column_zero():
    # 블록 내부 UNION ALL은 들여쓰기 — 최상위 조립 토큰 "\nUNION ALL\n"과 충돌하지 않는다
    for name, sql in steps.M3_BLOCKS_CORE:
        assert "\nUNION ALL\n" not in sql, name
        assert "\nUNION DISTINCT\n" not in sql, name


def test_m3_sql_contract_date_binding_no_percent_no_coalesce_no_star():
    for s in (steps.build_m3_sql(steps.M3_BLOCKS_CORE), steps.build_m3_expected(steps.M3_BLOCKS_CORE),
              steps.SQL_M3_SUMMARY):
        assert "{d:Date}" in s
        assert "%(" not in s
        assert "coalesce(" not in s.lower()
        assert "SELECT *" not in s
    for name, sql in steps.M3_BLOCKS_CORE:
        assert "{d:Date}" in sql, name


def test_m3_fact_blocks_anchored_and_partial_load_unanchored():
    anchored = {"unregistered_model", "hours_over_count", "unknown_violation", "pct_non_monotone",
                "gpu_type_no_tco", "serving_missing_for_gpu_model", "serving_without_gpu_serving_row"}
    for name, sql in steps.M3_BLOCKS_CORE:
        if name in anchored:
            assert f"GLOBAL IN {steps._M3_ANCHORED}" in sql, name
    partial = dict(steps.M3_BLOCKS_CORE)["partial_load"]
    assert "GLOBAL IN" not in partial
    assert "countIf(metric != 'custom') AS serving_n" in partial
    assert "custom_rows" not in partial
    assert "an.gpu_rows != c.actual_gpu" in partial and "an.serving_rows != c.actual_serving" in partial
    assert "UNION DISTINCT" in partial


def test_m3_token_side_columns_within_read_contract():
    sql = steps.build_m3_sql(steps.M3_BLOCKS_CORE)
    assert f"{DB_TOKEN_MART}.token_usage_1d_dist AS u" in sql
    used = set(re.findall(r"\bu\.(\w+)", sql))
    assert used <= set(READ_CONTRACT[f"{DB_TOKEN_MART}.token_usage_1d"])
    assert "agg_token_service_1d" not in sql


def test_m3_reg_expectation_predicate_matches_m0():
    missing = dict(steps.M3_BLOCKS_CORE)["metrics_missing"]
    assert "r.enabled = 1" in missing
    assert "r.coverage_since <= {d:Date}" in missing
    assert "(isNull(r.until) OR {d:Date} <= r.until)" in missing
    assert "an.service = ''" in missing
    reg_gap = dict(steps.M3_BLOCKS_CORE)["service_not_in_usage_registry"]
    assert f"r.service GLOBAL NOT IN {steps.SUB_USAGE_SVC}" in reg_gap


def test_m3_gpu_type_no_tco_excludes_fail_rows_and_uses_cost_categories():
    sql = dict(steps.M3_BLOCKS_CORE)["gpu_type_no_tco"]
    assert f"NOT {steps.FAIL_PRED}" in sql
    assert "g.category IN ('serving', 'standby')" in sql
    assert "isNull(t.tco)" in sql
    assert steps.SUB_EFF_TCO in sql


def test_m3_canon_used_for_model_keys():
    for name in ("unregistered_model", "hours_over_count", "pct_non_monotone",
                 "serving_missing_for_gpu_model", "serving_without_gpu_serving_row"):
        sql = dict(steps.M3_BLOCKS_CORE)[name]
        assert steps.SUB_EFF_ALIAS in sql, name
        assert "AS canon_model" in sql, name
    unknown = dict(steps.M3_BLOCKS_CORE)["unknown_violation"]
    assert steps.SUB_EFF_ALIAS not in unknown  # 미지 항목은 원문 모델명 그대로 (검출 대상 식별)
    # 9) 토큰 측(tk)도 canon으로 키를 맞춘다 — 원문 alias(u.model)와 gpu canon을 직접 비교하면 alias 모델이 전부 미스
    smissing = dict(steps.M3_BLOCKS_CORE)["serving_missing_for_gpu_model"]
    assert steps._TOK_SRC in smissing and steps._TOK_TAIL in smissing
    assert f"{steps.canon('u.model')} AS canon_model" in smissing
    assert "tk.canon_model = gk.canon_model" in smissing
    assert "tk.model = gk.canon_model" not in smissing
PYEOF
```

- [ ] **Step 2: 실패 확인 — 15개 모두 `M3_BLOCKS_CORE` 부재로 실패, T3 테스트는 그대로 통과**

Run: `cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_steps.py -k "m3"`
Expected: `15 failed, N deselected`(N = T3 테스트 수) — 각 실패의 오류가 `AttributeError: module 'app.steps' has no attribute 'M3_BLOCKS_CORE'`.
Run: `python -m pytest -q tests/test_steps.py -k "not m3"`
Expected: T3 테스트 전부 `passed`(M3 이름을 속성 접근으로만 썼으므로 수집 단계 오류 없음).

- [ ] **Step 3: 구현 — 헤더 헬퍼·앵커 집합·자식 카운트·블록 1~5 (`app/steps.py` 끝에 append)**

```bash
cd /home/mini/github/token-data-pipeline
cat >> mart/token-metrics/app/steps.py <<'PYEOF'


# ============================================================================
# M3 token_metrics_check_1d — 데이터 품질 검사 (설계 §6.1 M3, §4.3, §5.3-3, §5.4; Plan 6c T4)
#
# 각 검사 = 독립 SELECT 블록 (check_name, select_sql). build_m3_sql()이 블록을
# "\nUNION ALL\n"으로 이어 INSERT를 만들고, build_m3_expected()가 **같은 블록 문자열**의
# count()를 EXPECTED로 쓴다(파생 오차 0 — tools/verify/invariants.sql의 블록 리스트 방식).
# 12컬럼 순서는 DDL 선언 순서(Plan 6a mart_metrics_tables.sql token_metrics_check_1d_local)로
# 고정 — _m3_select()만이 헤더를 만든다. 블록 내부 UNION ALL은 반드시 들여쓰기(4칸 이상)해서
# 최상위 조립 토큰 "\nUNION ALL\n"과 구분한다.
#
# detail 규약(마스터 §5.6 로그·검사 표 비노출): 수·이름(model/gpu_type/카운트)만 —
# 응답 원문(reported_*)·user_id·페이로드는 싣지 않는다.
# 메트릭 측 소스는 앵커가 있는 (date, service)만 읽는다(§6.1) — partial_load만 예외
# (앵커 없는 잔여물 자체가 검출 대상).
# ============================================================================

M3_COLUMNS = ("date", "service_group", "service", "check_name", "model", "gpu_type",
              "severity", "observed", "threshold", "detail", "source_type", "created_by")

# 앵커가 있는 서비스 집합 — 팩트 블록의 GLOBAL IN 우변
_M3_ANCHORED = f"(SELECT service FROM {SUB_ANCHOR})"

# 앵커 vs 자식 행수(partial_load) — serving은 표준 지표 행(metric != 'custom')만
# (Plan 6b NormalizeResult.n_serving과 동일 정의; custom_rows는 비교하지 않는다 — 설계 해석)
_M3_CHILD_COUNTS = f"""(
    SELECT service, any(service_group) AS service_group,
           sum(gpu_n) AS actual_gpu, sum(serving_n) AS actual_serving
    FROM
    (
        SELECT service, any(service_group) AS service_group, count() AS gpu_n, 0 AS serving_n
        FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist
        WHERE date = {{d:Date}}
        GROUP BY service
        UNION ALL
        SELECT service, any(service_group) AS service_group, 0 AS gpu_n,
               countIf(metric != 'custom') AS serving_n
        FROM {DB_FACT}.raw_token_metrics_serving_1d_dist
        WHERE date = {{d:Date}}
        GROUP BY service
    )
    GROUP BY service
)"""


def _m3_select(check_name: str, severity: str, *, service_group: str, service: str,
               observed: str, threshold: str, detail: str, body: str,
               model: str = "''", gpu_type: str = "''", source_type: str = "''") -> str:
    """12컬럼(DDL 순서) SELECT 헤더 + FROM 본문. 값 인자는 SQL 식 문자열이다."""
    if severity not in ("FAIL", "WARN", "INFO"):
        raise ValueError(f"M3 severity must be FAIL|WARN|INFO: {check_name}={severity}")
    return (
        "SELECT\n"
        "    {d:Date} AS date,\n"
        f"    {service_group} AS service_group,\n"
        f"    {service} AS service,\n"
        f"    '{check_name}' AS check_name,\n"
        f"    {model} AS model,\n"
        f"    {gpu_type} AS gpu_type,\n"
        f"    '{severity}' AS severity,\n"
        f"    toNullable(toFloat64({observed})) AS observed,\n"
        f"    toNullable(toFloat64({threshold})) AS threshold,\n"
        f"    {detail} AS detail,\n"
        f"    {source_type} AS source_type,\n"
        f"    '{CREATED_BY}' AS created_by\n"
        f"{body}"
    )


# --- 1) metrics_missing FAIL — reg 기대(enabled·coverage 유효)인데 앵커 부재 (§4.3 M0 기대 집합)
_M3_METRICS_MISSING = _m3_select(
    "metrics_missing", "FAIL",
    service_group="r.service_group", service="r.service",
    observed="0", threshold="1", detail="'no summary row'",
    body=f"""FROM {SUB_REG} AS r
GLOBAL LEFT JOIN {SUB_ANCHOR} AS an ON an.service = r.service
WHERE r.enabled = 1
  AND r.coverage_since <= {{d:Date}}
  AND (isNull(r.until) OR {{d:Date}} <= r.until)
  AND an.service = ''""")

# --- 2) partial_load FAIL — (a) 자식 행은 있으나 앵커 부재, (b) 앵커 카운트 ≠ 실제 자식 행수 (§5.4)
_M3_PARTIAL_LOAD = _m3_select(
    "partial_load", "FAIL",
    service_group="if(an.service != '', an.service_group, c.service_group)", service="k.service",
    observed="c.actual_gpu + c.actual_serving", threshold="an.gpu_rows + an.serving_rows",
    detail=("concat('gpu=', toString(c.actual_gpu), '/', toString(an.gpu_rows), "
            "' serving=', toString(c.actual_serving), '/', toString(an.serving_rows))"),
    source_type="an.source_type",
    body=f"""FROM
(
    SELECT service FROM {_M3_CHILD_COUNTS} AS cc
    UNION DISTINCT
    SELECT service FROM {SUB_ANCHOR} AS aa
) AS k
GLOBAL LEFT JOIN {_M3_CHILD_COUNTS} AS c ON c.service = k.service
GLOBAL LEFT JOIN {SUB_ANCHOR} AS an ON an.service = k.service
WHERE an.service = ''
   OR an.gpu_rows != c.actual_gpu
   OR an.serving_rows != c.actual_serving""")

# --- 3) rows_rejected WARN — 앵커 rejected_rows > 0 (§5.3-1 구조 거부 카운트)
_M3_ROWS_REJECTED = _m3_select(
    "rows_rejected", "WARN",
    service_group="an.service_group", service="an.service",
    observed="an.rejected_rows", threshold="0", detail="'rejected rows in summary'",
    source_type="an.source_type",
    body=f"""FROM {SUB_ANCHOR} AS an
WHERE an.rejected_rows > 0""")

# --- 4) unregistered_model WARN — gpu 팩트 모델이 alias 표에 없음(canon = 원문) (§4.2 alias 시드 규칙)
_M3_UNREGISTERED_MODEL = _m3_select(
    "unregistered_model", "WARN",
    service_group="x.service_group", service="x.service", model="x.canon_model",
    observed="x.n", threshold="0", detail="''", source_type="x.source_type",
    body=f"""FROM
(
    SELECT g.service, any(g.service_group) AS service_group,
           {canon('g.model')} AS canon_model, count() AS n, any(g.source_type) AS source_type
    FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = g.model
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN {_M3_ANCHORED} AND a.canonical = ''
    GROUP BY g.service, {canon('g.model')}
) AS x""")

# --- 5) hours_over_count FAIL — 행 플래그(§5.3-2, gpuHours > gpuCount×24) 집계 (service, canon, gpu_type)
_M3_HOURS_OVER_COUNT = _m3_select(
    "hours_over_count", "FAIL",
    service_group="x.service_group", service="x.service", model="x.canon_model", gpu_type="x.gpu_type",
    observed="x.hours", threshold="x.hours_cap",
    detail="concat('model=', x.canon_model, ' gpu_type=', x.gpu_type)", source_type="x.source_type",
    body=f"""FROM
(
    SELECT g.service, any(g.service_group) AS service_group,
           {canon('g.model')} AS canon_model, g.gpu_type,
           sum(g.gpu_hours) AS hours, sum(g.gpu_count) * 24 AS hours_cap,
           any(g.source_type) AS source_type
    FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = g.model
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN {_M3_ANCHORED}
      AND hasAny(g.flags, ['hours_over_count'])
    GROUP BY g.service, {canon('g.model')}, g.gpu_type
) AS x""")
PYEOF
```

- [ ] **Step 4: 구현 — 블록 6~10 (`app/steps.py` 끝에 append)**

```bash
cd /home/mini/github/token-data-pipeline
cat >> mart/token-metrics/app/steps.py <<'PYEOF'
# --- 6) unknown_violation FAIL — 정규화가 플래그한 미지 항목(§5.3-2): gpu·serving 팩트 양쪽, 모델 원문 그대로
_M3_UNKNOWN_VIOLATION = _m3_select(
    "unknown_violation", "FAIL",
    service_group="x.service_group", service="x.service", model="x.model", gpu_type="x.gpu_type",
    observed="x.n", threshold="0", detail="concat('part=', x.part)", source_type="x.source_type",
    body=f"""FROM
(
    SELECT service, any(service_group) AS service_group, model, gpu_type, part,
           count() AS n, any(source_type) AS source_type
    FROM
    (
        SELECT g.service, g.service_group, g.model, g.gpu_type, 'gpu' AS part, g.source_type
        FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
        WHERE g.date = {{d:Date}} AND g.service GLOBAL IN {_M3_ANCHORED}
          AND hasAny(g.flags, ['unknown_violation'])
        UNION ALL
        SELECT s.service, s.service_group, s.model, '' AS gpu_type, 'serving' AS part, s.source_type
        FROM {DB_FACT}.raw_token_metrics_serving_1d_dist AS s
        WHERE s.date = {{d:Date}} AND s.service GLOBAL IN {_M3_ANCHORED}
          AND hasAny(s.flags, ['unknown_violation'])
    )
    GROUP BY service, model, gpu_type, part
) AS x""")

# --- 7) pct_non_monotone FAIL — serving 행 플래그(§4.1 158 FAIL 플래그, p50>p90>... §5.3-2) 집계 (service, canon)
_M3_PCT_NON_MONOTONE = _m3_select(
    "pct_non_monotone", "FAIL",
    service_group="x.service_group", service="x.service", model="x.canon_model",
    observed="x.n", threshold="0", detail="concat('metrics=', x.metrics)", source_type="x.source_type",
    body=f"""FROM
(
    SELECT s.service, any(s.service_group) AS service_group,
           {canon('s.model')} AS canon_model, count() AS n,
           arrayStringConcat(arraySort(groupUniqArray(toString(s.metric))), ',') AS metrics,
           any(s.source_type) AS source_type
    FROM {DB_FACT}.raw_token_metrics_serving_1d_dist AS s
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = s.model
    WHERE s.date = {{d:Date}} AND s.service GLOBAL IN {_M3_ANCHORED}
      AND hasAny(s.flags, ['pct_non_monotone'])
    GROUP BY s.service, {canon('s.model')}
) AS x""")

# --- 8) gpu_type_no_tco WARN — 비용 계산 대상(serving/standby, FAIL 제외) gpu_type에 유효 TCO 없음 (§4.2 M1 cost NULL 사유)
_M3_GPU_TYPE_NO_TCO = _m3_select(
    "gpu_type_no_tco", "WARN",
    service_group="x.service_group", service="x.service", gpu_type="x.gpu_type",
    observed="x.hours", threshold="0", detail="'no effective tco'", source_type="x.source_type",
    body=f"""FROM
(
    SELECT g.service, any(g.service_group) AS service_group, g.gpu_type,
           sum(g.gpu_hours) AS hours, any(g.source_type) AS source_type
    FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    GLOBAL LEFT JOIN {SUB_EFF_TCO} AS t ON t.gpu_type = g.gpu_type
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN {_M3_ANCHORED}
      AND g.category IN ('serving', 'standby') AND NOT {FAIL_PRED} AND isNull(t.tco)
    GROUP BY g.service, g.gpu_type
) AS x""")

# --- 9) serving_missing_for_gpu_model WARN — gpu serving 행이 있는 (service, canon)에 serving 지표 행이 없고
#        token_usage_1d 요청은 있음 (§6.1 M4 share 분모 결손 사전 경고)
_M3_SERVING_MISSING_FOR_GPU_MODEL = _m3_select(
    "serving_missing_for_gpu_model", "WARN",
    service_group="gk.service_group", service="gk.service", model="gk.canon_model",
    observed="tk.requests", threshold="0", detail="'gpu serving row without serving metrics'",
    source_type="gk.source_type",
    body=f"""FROM
(
    SELECT g.service, any(g.service_group) AS service_group, {canon('g.model')} AS canon_model,
           any(g.source_type) AS source_type
    FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = g.model
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN {_M3_ANCHORED} AND g.category = 'serving'
    GROUP BY g.service, {canon('g.model')}
) AS gk
GLOBAL LEFT JOIN
(
    SELECT s.service, {canon('s.model')} AS canon_model, 1 AS has_rows
    FROM {DB_FACT}.raw_token_metrics_serving_1d_dist AS s
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = s.model
    WHERE s.date = {{d:Date}} AND s.metric != 'custom'
    GROUP BY s.service, {canon('s.model')}
) AS sk ON sk.service = gk.service AND sk.canon_model = gk.canon_model
GLOBAL LEFT JOIN
(
    SELECT u.service, {canon('u.model')} AS canon_model, sum(u.requests) AS requests
    {_TOK_SRC}
    {_TOK_TAIL}
) AS tk ON tk.service = gk.service AND tk.canon_model = gk.canon_model
WHERE sk.has_rows = 0 AND tk.requests > 0""")

# --- 10) serving_without_gpu_serving_row WARN — serving 지표 행은 있으나 gpu serving 행 없음 (reg expect_gpu=1인 서비스만)
_M3_SERVING_WITHOUT_GPU_SERVING_ROW = _m3_select(
    "serving_without_gpu_serving_row", "WARN",
    service_group="sk.service_group", service="sk.service", model="sk.canon_model",
    observed="1", threshold="0", detail="'serving metrics without gpu serving row'",
    source_type="sk.source_type",
    body=f"""FROM
(
    SELECT s.service, any(s.service_group) AS service_group, {canon('s.model')} AS canon_model,
           any(s.source_type) AS source_type
    FROM {DB_FACT}.raw_token_metrics_serving_1d_dist AS s
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = s.model
    WHERE s.date = {{d:Date}} AND s.service GLOBAL IN {_M3_ANCHORED} AND s.metric != 'custom'
    GROUP BY s.service, {canon('s.model')}
) AS sk
GLOBAL LEFT JOIN
(
    SELECT g.service, {canon('g.model')} AS canon_model, 1 AS has_rows
    FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = g.model
    WHERE g.date = {{d:Date}} AND g.category = 'serving'
    GROUP BY g.service, {canon('g.model')}
) AS gk ON gk.service = sk.service AND gk.canon_model = sk.canon_model
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = sk.service
WHERE gk.has_rows = 0 AND r.expect_gpu = 1""")
PYEOF
```

- [ ] **Step 5: 구현 — 블록 11~13 · `M3_BLOCKS_CORE`/`M3_BLOCKS_STRETCH` · 빌더 · `SQL_M3_SUMMARY` (`app/steps.py` 끝에 append)**

```bash
cd /home/mini/github/token-data-pipeline
cat >> mart/token-metrics/app/steps.py <<'PYEOF'

# --- 11) identity_drift WARN — API 응답 자기신고(reported_*)가 헤더/레지스트리와 다름 (§5.3-3)
#         detail은 불일치 여부(0/1)만 — reported_* 원문은 싣지 않는다 (마스터 §5.6)
_M3_IDENTITY_DRIFT = _m3_select(
    "identity_drift", "WARN",
    service_group="r.service_group", service="an.service",
    observed="toUInt8(an.reported_service != an.service) + toUInt8(an.reported_service_group != r.service_group)",
    threshold="0",
    detail=("concat('svc_diff=', toString(toUInt8(an.reported_service != an.service)), "
            "' group_diff=', toString(toUInt8(an.reported_service_group != r.service_group)))"),
    source_type="an.source_type",
    body=f"""FROM {SUB_ANCHOR} AS an
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = an.service
WHERE an.source_type = 'metrics-api-v1'
  AND (an.reported_service != an.service OR an.reported_service_group != r.service_group)""")

# --- 12) service_not_in_usage_registry WARN — 메트릭 레지스트리 서비스가 token_usage 레지스트리에 없음 (§4.3 조인 키 전제)
_M3_SERVICE_NOT_IN_USAGE_REGISTRY = _m3_select(
    "service_not_in_usage_registry", "WARN",
    service_group="r.service_group", service="r.service",
    observed="1", threshold="0", detail="'not in dim_token_service'",
    body=f"""FROM {SUB_REG} AS r
WHERE r.enabled = 1 AND r.service GLOBAL NOT IN {SUB_USAGE_SVC}""")

# --- 13) manual_source INFO — 앵커 source_type = 'manual-v0' (§5.2 수동 반입 표기, 정보성)
_M3_MANUAL_SOURCE = _m3_select(
    "manual_source", "INFO",
    service_group="an.service_group", service="an.service",
    observed="1", threshold="0", detail="'manual-v0'", source_type="an.source_type",
    body=f"""FROM {SUB_ANCHOR} AS an
WHERE an.source_type = 'manual-v0'""")

# 핵심 13블록 — 순서는 설계 §6.1 M3 표 순서 (T5 batch·문서가 이 순서를 인용)
M3_BLOCKS_CORE: list[tuple[str, str]] = [
    ("metrics_missing", _M3_METRICS_MISSING),
    ("partial_load", _M3_PARTIAL_LOAD),
    ("rows_rejected", _M3_ROWS_REJECTED),
    ("unregistered_model", _M3_UNREGISTERED_MODEL),
    ("hours_over_count", _M3_HOURS_OVER_COUNT),
    ("unknown_violation", _M3_UNKNOWN_VIOLATION),
    ("pct_non_monotone", _M3_PCT_NON_MONOTONE),
    ("gpu_type_no_tco", _M3_GPU_TYPE_NO_TCO),
    ("serving_missing_for_gpu_model", _M3_SERVING_MISSING_FOR_GPU_MODEL),
    ("serving_without_gpu_serving_row", _M3_SERVING_WITHOUT_GPU_SERVING_ROW),
    ("identity_drift", _M3_IDENTITY_DRIFT),
    ("service_not_in_usage_registry", _M3_SERVICE_NOT_IN_USAGE_REGISTRY),
    ("manual_source", _M3_MANUAL_SOURCE),
]

# 확장 블록 — T4 시점 비어 있음. T6(share 경고)·T7(gpu 그룹 경고)이 append한다.
M3_BLOCKS_STRETCH: list[tuple[str, str]] = []

M3_INSERT_COLUMNS = ", ".join(M3_COLUMNS)


def _m3_union(blocks: list[tuple[str, str]]) -> str:
    if not blocks:
        raise ValueError("build_m3: blocks must not be empty")
    return "\nUNION ALL\n".join(sql for _, sql in blocks)


def build_m3_sql(blocks: list[tuple[str, str]]) -> str:
    """블록 리스트 → INSERT INTO {DB_MART}.token_metrics_check_1d_dist (12컬럼) + UNION ALL 본문."""
    return f"INSERT INTO {DB_MART}.{T_M3}_dist ({M3_INSERT_COLUMNS})\n" + _m3_union(blocks)


def build_m3_expected(blocks: list[tuple[str, str]]) -> str:
    """같은 블록 문자열의 count() — INSERT 본문과 문자 단위로 동일한 UNION을 감싼다."""
    return "SELECT count() FROM (\n" + _m3_union(blocks) + "\n)"


SQL_M3_SUMMARY = f"""SELECT check_name, severity, count() AS n
FROM {DB_MART}.{T_M3}_dist
WHERE date = {{d:Date}}
GROUP BY check_name, severity
ORDER BY check_name, severity"""
PYEOF
```

- [ ] **Step 6: GREEN 확인 — 블록·빌더 15개 통과**

Run: `cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_steps.py -k "m3"`
Expected: `15 passed, N deselected`
Run: `python -c "import app.steps as s; sql = s.build_m3_sql(s.M3_BLOCKS_CORE); print(len(s.M3_BLOCKS_CORE), sql.count(chr(10)+'UNION ALL'+chr(10)), 'coalesce(' in sql.lower(), '%(' in sql)"`
Expected: `13 12 False False`

- [ ] **Step 7: RED — `run_m3` 시퀀스·요약 라인 테스트 추가 (`tests/test_steps.py` 끝에 append)**

`M3Gate`는 T3 `FakeGate`를 상속해 (a) M3 요약 조회(`GROUP BY check_name, severity`)에 준비된 행을, (b) EXPECTED 조회(`SELECT count() FROM (`)에 고정 행수를 돌려주고, (c) delete/insert 호출을 인자째 기록한다. `FakeGate._short` 라우팅은 INSERT 문에 `token_metrics_check_1d`가 들어 있어 그대로 동작한다.

```bash
cd /home/mini/github/token-data-pipeline
cat >> mart/token-metrics/tests/test_steps.py <<'PYEOF'
# --- run_m3: _run_table 시퀀스 + 검사 요약 라인 ------------------------------------
class M3Gate(FakeGate):
    """FakeGate + M3 요약(GROUP BY check_name, severity) 조회 응답·적재 행수 고정."""

    def __init__(self, summary_rows, rows=3, **kw):
        super().__init__(**kw)
        self.summary_rows = summary_rows
        self.rows = rows
        self.deleted = []
        self.inserted = []

    def delete_day(self, table_local, date, extra_pred=""):
        self.deleted.append((table_local, date, extra_pred))
        super().delete_day(table_local, date, extra_pred)

    def insert_select(self, sql, params=None):
        self.inserted.append((sql, params))
        return self.rows

    def verify_count(self, table_dist, date, expected):
        self.verify_calls.append((table_dist, expected))
        return True, self.rows

    def query(self, sql, params=None):
        self.query_calls.append((sql, params))
        if "GROUP BY check_name, severity" in sql:
            return list(self.summary_rows)
        if sql.startswith("SELECT count() FROM ("):
            return [(self.rows,)]
        return super().query(sql, params)


def test_run_m3_appends_check_warn_lines(capsys):
    gate = M3Gate([("rows_rejected", "WARN", 2)], rows=2)
    out = steps.run_m3(gate, DATE)
    assert out["warns"] == ["CHECK WARN rows_rejected severity=WARN count=2"]
    assert out["rows_check"] == 2
    assert "CHECK WARN rows_rejected severity=WARN count=2" in capsys.readouterr().out


def test_run_m3_fail_is_warn_line_and_info_is_info_line():
    gate = M3Gate([("hours_over_count", "FAIL", 1), ("manual_source", "INFO", 4)], rows=5)
    out = steps.run_m3(gate, DATE)
    assert out["warns"] == [
        "CHECK WARN hours_over_count severity=FAIL count=1",
        "CHECK INFO manual_source severity=INFO count=4",
    ]
    assert len([w for w in out["warns"] if w.startswith("CHECK WARN ")]) == 1


def test_run_m3_sequence_delete_insert_expected_verify_summary():
    gate = M3Gate([], rows=7)
    out = steps.run_m3(gate, DATE)
    assert out == {"rows_check": 7, "warns": []}
    # FakeGate.order는 (op, short) 튜플 — M3Gate가 insert/query/verify를 덮어써 exists·delete만 기록된다
    assert gate.order == [("exists", "m3"), ("delete", "m3")]
    assert gate.deleted == [(f"{DB_MART}.token_metrics_check_1d_local", DATE, "")]
    assert len(gate.inserted) == 1
    sql, params = gate.inserted[0]
    assert sql.startswith(f"INSERT INTO {DB_MART}.token_metrics_check_1d_dist (")
    assert params == {"d": DATE}
    assert gate.verify_calls == [(f"{DB_MART}.token_metrics_check_1d_dist", 7)]
    # blocks=None 기본은 CORE + STRETCH — T6/T7이 STRETCH를 extend한 뒤에도 성립
    assert gate.query_calls[0][0] == steps.build_m3_expected(steps.M3_BLOCKS_CORE + steps.M3_BLOCKS_STRETCH)
    assert gate.query_calls[-1] == (steps.SQL_M3_SUMMARY, {"d": DATE})


def test_run_m3_blocks_default_is_core_plus_stretch(monkeypatch):
    extra = ("stretch_probe", steps._m3_select(
        "stretch_probe", "INFO", service_group="''", service="''", observed="0",
        threshold="0", detail="''", body="FROM system.one WHERE 0 AND {d:Date} = {d:Date}"))
    monkeypatch.setattr(steps, "M3_BLOCKS_STRETCH", [extra])
    gate = M3Gate([], rows=0)
    steps.run_m3(gate, DATE)
    sql = gate.inserted[0][0]
    assert sql.count("\nUNION ALL\n") == 13
    assert "'stretch_probe' AS check_name" in sql
    gate2 = M3Gate([], rows=0)
    steps.run_m3(gate2, DATE, blocks=steps.M3_BLOCKS_CORE[:1])
    assert "UNION ALL" not in gate2.inserted[0][0]


def test_run_m3_raises_step_error_when_verify_fails():
    class Failing(M3Gate):
        def verify_count(self, table_dist, date, expected):
            return False, 0

    with pytest.raises(steps.StepError):
        steps.run_m3(Failing([], rows=3), DATE)
PYEOF
```

- [ ] **Step 8: 실패 확인 — `run_m3` 부재**

Run: `cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_steps.py -k "run_m3"`
Expected: `5 failed, 15 deselected`(+ T3 테스트 수) — 각 실패가 `AttributeError: module 'app.steps' has no attribute 'run_m3'. Did you mean: 'run_m1'?`

- [ ] **Step 9: 구현 — `run_m3` (`app/steps.py` 끝에 append)**

```bash
cd /home/mini/github/token-data-pipeline
cat >> mart/token-metrics/app/steps.py <<'PYEOF'

def run_m3(gate, date: str, blocks: list[tuple[str, str]] | None = None) -> dict:
    """M3: 검사 블록 UNION ALL 적재 → 검사별 건수를 'CHECK WARN|INFO <check_name> severity=<sev> count=<n>'
    로 출력·warns에 추가. 검사 행이 있어도 STEP은 성공이다(FAIL은 severity 값일 뿐 — 실패 처리는
    _run_table의 verify 불일치·예외만). T5 batch는 'CHECK WARN ' 접두 라인 수를 warn= 마커에 센다."""
    if blocks is None:
        blocks = M3_BLOCKS_CORE + M3_BLOCKS_STRETCH
    warns: list[str] = []
    rows = _run_table(gate, date, f"{DB_MART}.{T_M3}_dist", f"{DB_MART}.{T_M3}_local",
                      build_m3_sql(blocks), build_m3_expected(blocks), warns)
    for check_name, severity, n in gate.query(SQL_M3_SUMMARY, {"d": date}):
        level = "INFO" if severity == "INFO" else "WARN"
        line = f"CHECK {level} {check_name} severity={severity} count={int(n)}"
        print(line, flush=True)
        warns.append(line)
    return {"rows_check": rows, "warns": warns}
PYEOF
```

- [ ] **Step 10: GREEN — `run_m3` 5개 통과 → 모듈 전체 스위트 → zero-diff 확인**

Run: `cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_steps.py -k "run_m3"`
Expected: `5 passed, 15 deselected`(+ T3 테스트 수)
Run: `python -m pytest -q`
Expected: 마지막 줄이 `N passed`(T1~T3 테스트 + 이 태스크의 20개; `failed`·`error` 0). `test_run_m3_appends_check_warn_lines`가 `capsys`로 stdout의 `CHECK WARN rows_rejected severity=WARN count=2` 라인을 확인하므로 `-s` 없이 실행한다.
Run: `git status --short mart/token-metrics && git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-mart.yml .github/workflows/test-collector.yml`
Expected: `git status`에는 ` M mart/token-metrics/app/steps.py`와 ` M mart/token-metrics/tests/test_steps.py` 두 줄만; `git diff --stat`은 **빈 출력**(zero-diff).
Run: `sed -n '/^# M3 token_metrics_check_1d/,$p' mart/token-metrics/app/steps.py | grep -c "GLOBAL LEFT JOIN\|GLOBAL IN\|GLOBAL NOT IN" && ! sed -n '/^# M3 token_metrics_check_1d/,$p' mart/token-metrics/app/steps.py | grep -n "LEFT JOIN" | grep -v GLOBAL; echo "exit=$?"`
Expected: 첫 줄 `26`, 둘째 줄 `exit=0`(M3 절에 GLOBAL 없는 LEFT JOIN이 하나도 없다 — §4.0 분산 조인 표준).

- [ ] **Step 11: 커밋**

```bash
cd /home/mini/github/token-data-pipeline
git add mart/token-metrics/app/steps.py mart/token-metrics/tests/test_steps.py
git commit -m "feat(mart-metrics): M3 token_metrics_check_1d 핵심 13블록 — 블록 리스트 UNION ALL 조립·EXPECTED=count (Plan 6c T4)

- _m3_select: 12컬럼(DDL 순서) SELECT 헤더 헬퍼, severity FAIL|WARN|INFO 검증
- M3_BLOCKS_CORE 13블록(metrics_missing … manual_source), M3_BLOCKS_STRETCH=[] (T6/T7 append)
- build_m3_sql / build_m3_expected: 같은 블록 문자열을 UNION ALL로 조립, EXPECTED=count()
- run_m3: _run_table 후 SQL_M3_SUMMARY로 CHECK WARN|INFO <check_name> severity= count= 라인 출력·warns
- identity_drift.detail은 불일치 여부(0/1)만 — reported_* 원문 비노출 (마스터 §5.6)
- 팩트 블록은 앵커 있는 (date, service)만 조회 (GLOBAL IN), partial_load만 비앵커 잔여물 검출

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
git log --oneline -1
```

**Self-Review / 설계 해석 (T4)**:
- **설계 해석 — `partial_load` 비교 대상**: 앵커 `gpu_rows`는 gpu 팩트 행수, `serving_rows`는 표준 지표 행수(`metric != 'custom'`, Plan 6b `NormalizeResult.n_serving`과 같은 정의)와 비교한다. `custom_rows`는 비교하지 않는다(설계 §5.4는 gpu/serving 카운트만 앵커 대사 대상으로 명시). 앵커가 없는데 자식 행이 있는 서비스도 `partial_load`(source_type `''`)로 잡는다 — 이 블록만 `GLOBAL IN {_M3_ANCHORED}` 앵커 필터를 걸지 않는 이유.
- **설계 해석 — `canon_model` 내부 별칭**: 블록 내부 서브쿼리에서 `if(a.canonical = '', g.model, a.canonical) AS canon_model`(GROUP BY는 같은 식)로 계산하고 바깥 12컬럼 헤더에서 `x.canon_model AS model`로 올린다. `model`이라는 별칭을 안쪽에서 쓰면 팩트 컬럼 `g.model`과 이름이 겹쳐 ClickHouse 별칭 해석이 순환하므로 피했다. `unknown_violation`만 alias 조인 없이 **원문** `model`을 싣는다(미지 항목의 식별이 목적 — canon으로 접으면 어떤 원문이 문제였는지 사라진다).
- **설계 해석 — `gpu_type_no_tco` 범위**: M1 비용 산식(§6.4 (1))과 같은 행 집합 — `category IN ('serving','standby') AND NOT FAIL_PRED` — 에 한해 유효 TCO 부재를 경고한다. FAIL 플래그 행은 어차피 비용에서 제외되므로 TCO 경고를 겹쳐 내지 않는다. `observed` = 해당 gpu_hours 합(영향 크기).
- **설계 해석 — `pct_non_monotone` severity = FAIL**: 설계 §4.1 158이 serving 행 플래그 `pct_non_monotone`을 `unknown_violation`과 같은 **FAIL** 플래그로 정의하므로 M3 블록도 FAIL이다(`hours_over_count`/`unknown_violation`과 동급). 다만 이 플래그는 serving 팩트(백분위 지표)에만 붙고 gpu 팩트 비용 산식과 무관하므로 T2 `FAIL_FLAGS`(C 제외 술어)에는 들어가지 않는다 — M3 severity와 C 제외 집합은 별개 축.
- **설계 해석 — `serving_missing_for_gpu_model` 토큰 측 키**: `tk` 서브쿼리도 `_TOK_SRC`/`_TOK_TAIL`(alias 조인 + usage_svc 모집단)로 `canon(u.model)`을 만들어 `gk.canon_model`과 비교한다. `token_usage_1d.model`은 원문 alias라 gpu 측 canon과 직접 비교하면 alias로 보고된 모델의 요청이 전부 미스(`tk.requests = 0`)가 돼 경고가 영구 누락된다. 같은 canon으로 접힌 alias 여러 개의 요청은 `sum`으로 합쳐진다(M1·M4와 같은 모집단).
- **설계 해석 — `hours_over_count` observed/threshold**: 행 플래그를 (service, canon, gpu_type)으로 묶어 `observed = sum(gpu_hours)`, `threshold = sum(gpu_count) * 24`. 플래그 자체는 정규화(Plan 6b)가 행 단위로 이미 붙였고 M3는 집계·노출만 한다.
- **설계 해석 — `identity_drift`**: `SUB_ANCHOR`의 `reported_service`는 헤더 `service`와, `reported_service_group`은 등록부 `r.service_group`과 비교한다(§5.4). `detail`은 `svc_diff=<0|1> group_diff=<0|1>`만 — 응답 원문(`reported_*`)은 검사 표에 싣지 않는다(마스터 §5.6). `observed`는 불일치 개수(0~2), `threshold` 0.
- **설계 해석 — `serving_missing_for_gpu_model` / `serving_without_gpu_serving_row` 방향**: 전자는 gpu `category='serving'` 행이 있는 (service, canon)에 서빙 지표 행이 없고 `token_usage_1d` 요청이 있는 경우(M4 share 분모 결손 예고, `observed = sum(u.requests)`); 후자는 서빙 지표 행이 있는데 gpu serving 행이 없고 등록부 `expect_gpu = 1`인 경우. `expect_serving`은 이 두 블록에서 쓰지 않는다(gpu 행 존재 자체가 서빙 기대의 근거).
- **설계 해석 — `service_not_in_usage_registry`**: 등록부 `enabled = 1`인 서비스 중 `dim_token_service`(enabled=1)에 없는 것. T5 M0의 같은 이름 `CHECK WARN service_not_in_usage_registry service=<s>` 로그와 **이름은 같고 표면이 다르다**(M0는 로그 라인, M3는 검사 표 행) — 둘 다 설계 §6.1 표기 그대로.
- **설계 해석 — `run_m3` 출력**: 요약 라인을 `run_m3`가 직접 `print(flush=True)`하고 `warns`에도 넣는다. T5 `batch.py`는 `warns`를 **재출력하지 않고** `startswith("CHECK WARN ")`만 세어 `warn=` 마커를 만든다(INFO 라인은 `CHECK INFO` 접두라 세지 않는다). 검사 표에 FAIL 행이 있어도 STEP 실패가 아니다(§6.1 — 실패는 `_run_table`의 verify 불일치·예외만).
- **`{d:Date}` 서버 바인딩**: 모든 블록·EXPECTED·요약이 `{d:Date}`(f-string 안에서는 `{{d:Date}}`)만 쓴다. `_m3_select` 헤더는 f-string이 아닌 일반 문자열 `"    {d:Date} AS date,\n"`이므로 중괄호를 겹치지 않는다.
- **블록 내부 UNION**: `_M3_CHILD_COUNTS`의 `UNION ALL`, `partial_load`의 `UNION DISTINCT`, `unknown_violation`의 `UNION ALL`은 모두 4칸 이상 들여쓰기 — 최상위 조립 토큰 `"\nUNION ALL\n"`과 절대 겹치지 않는다(`test_m3_inner_union_all_never_at_column_zero`가 고정).
- **테스트 헬퍼 이름**: T3가 `SQL_M1` 컬럼 파싱용 헬퍼를 어떤 이름으로 두었든 이 태스크는 `_m3_` 접두의 자체 헬퍼(`_m3_insert_columns`/`_m3_ddl_columns`/`_m3_select_header_aliases`, 상수 `_M3_DDL_PATH`·`M3_CORE_NAMES`·`M3_SEVERITY`·`M3_KEYED_MODEL`·`M3_KEYED_GPU_TYPE`)만 추가해 T3 정의를 덮어쓰지 않는다. `_m3_ddl_columns`는 `CREATE TABLE IF NOT EXISTS <db>.<table_local>` 줄을 마지막 토큰의 `.` 뒤 이름으로 매칭 → `(` 줄 이후 컬럼 수집 → `)` 줄에서 종료, `CONSTRAINT`·주석·빈 줄 제외(Plan 6a DDL의 `ON CLUSTER` 줄이 `(` 앞에 있어도 컬럼으로 오인하지 않는다). 파일 중간의 `import re`·`from pathlib import Path`·`from app.ch import …`·`from app.preflight import READ_CONTRACT`는 T3와 중복돼도 무해하다.
- **Zero-diff**: 수정 파일은 `mart/token-metrics/app/steps.py`·`tests/test_steps.py` 둘뿐. `mart/token-usage/**`, `tools/verify/invariants.sql`은 읽기만.
- **공개 레포 규율**: 사내 호스트·코드명·이메일 없음. DB명은 `app.ch` 상수(env `CH_DB_*`)로만 보간.
### Task 5: app/batch.py — 읽기 계약 프리플라이트·M0/M0b·변이 예산 프리체크·M1→M3 오케스트레이션·BATCH_RESULT 마커·SIGTERM

**설계 근거**: 설계 §6.1 301(M0 커버리지 기대 = `reg enabled=1 AND coverage_since ≤ d AND (until IS NULL OR d ≤ until)`, 실제 = 앵커; `reg.service ∉ usage_svc` → `CHECK WARN service_not_in_usage_registry`; M0b `agg_token_service_1d`에 D 행 없음 → `CHECK WARN token_mart_absent`; 첫 `_run_table` 전 날짜 전체 × 4테이블 `exists` 선조회 → 초과 시 `FAILURE reason=mutation_budget`; 메트릭 fact가 없는 날 = **절대 FAILURE 아님**), §6.1 306(마커 `BATCH_RESULT status=… module=mart-metrics metrics_coverage=N/M missing_services="…" rows_mart= rows_check= rows_share= warn= elapsed=`), §4.0 117-131(변이 장부 — mart 날짜당 ≤4, `MART_METRICS_MAX_MUTATIONS_PER_RUN` 기본 64), §7.5 370(읽기 계약 13컬럼 프리플라이트), 마스터 §5.6(로그에 user_id 원문·페이로드 금지, SIGTERM 시에도 마커 출력), Plan 6a H(마커 필드 고정 — `rows_group`는 마커 미포함).
**읽을 원형**: `mart/token-usage/app/batch.py:1-29`(docstring·import·STEP 0 SQL), `:88-108`(`_status`·`_sigterm_handler`·`_scalar`·`_check_step0_coverage`), `:147-257`(`_emit_step_warns`·`run_batch`·`main`) — digest §5; `mart/token-usage/tests/test_batch.py:1-197`(FakeGate query 부분문자열 라우팅·마커 테스트) — digest §7; T1 `app/preflight.py`(`READ_CONTRACT`·`contract_tables()`·`missing_columns()`), T2 `app/mart.py`(`Coverage`·`compute_coverage`·`batch_line(…, reason="")`·`target_dates`·`mutation_budget_exceeded`), T3/T4 `app/steps.py`(`run_m1`·`run_m3`·`MART_TABLES`·`StepError`).

**Files:**
- Create: `mart/token-metrics/app/batch.py`
- Test: `mart/token-metrics/tests/test_batch.py`

**Interfaces:**
- Consumes:
  - `app.ch`: `CHGate(cfg)`, `DB_FACT`, `DB_DIM`, `DB_MART`, `DB_TOKEN_MART`, `DB_TOKEN_DIM` (T1). 게이트 메서드 `describe(table_dist) -> list[str]`, `exists(table_dist, date) -> bool`, `query(sql, params=None) -> list[tuple]`.
  - `app.config`: `Config`(필드 `max_mutations_per_run: int = 64`), `load_config() -> Config` (T1).
  - `app.preflight`: `contract_tables() -> list[str]`(`db.table` 3개, `_dist` 접미 없음), `missing_columns(described: dict[str, list[str]]) -> list[str]` (T1).
  - `app.mart`: `Coverage(enabled, present, missing, warn_targets)`, `compute_coverage(expected_services: list[str], anchor_services: set[str], expected_late: list[str]) -> Coverage`, `batch_line(status, coverage, rows_mart, rows_check, rows_share, warn_count, elapsed_s, reason="") -> str`, `target_dates(args) -> tuple[list[str] | None, bool]`(`args.batch_time`/`args.from_date`/`args.to_date`), `mutation_budget_exceeded(planned: int, budget: int) -> bool` (T2).
  - `app.steps`: `run_m1(gate, date) -> dict`(`{"rows_mart": int, "warns": list[str]}`), `run_m3(gate, date, blocks=None) -> dict`(`{"rows_check": int, "warns": list[str]}` — warns 항목은 `CHECK WARN <check_name> severity=<S> count=<n>` / `CHECK INFO …`), `MART_TABLES = (T_M1, T_M3, T_M4, T_M2)`(테이블명, db·접미 없음), `StepError` (T3/T4). `_run_table`의 경고 항목은 접두 없는 `dup_suspect:<dist>`.
- Produces (`mart/token-metrics/app/batch.py`):
  - SQL 상수(모두 `str`): `SQL_M0_EXPECTED_SERVICES`(`{d:Date}` 바인딩), `SQL_M0_ANCHOR_SERVICES`(`{d:Date}`), `SQL_M0_REG_NOT_IN_USAGE`(날짜 무관), `SQL_M0B_TOKEN_MART_ROWS`(`{d:Date}`; `SELECT count() FROM {DB_TOKEN_MART}.agg_token_service_1d_dist WHERE date = {d:Date}`).
  - `RUNNERS: list[tuple[str, Callable]]` — T5 시점 `[("rows_mart", run_m1), ("rows_check", run_m3)]`(T6가 `("rows_share", run_m4)`, T7이 `("rows_group", run_m2)`를 append). 각 러너는 `fn(gate, date) -> dict`이며 결과 dict에 `key`와 `"warns"`를 담는다.
  - `@dataclass BatchOutcome(exit_code: int, line: str, skip_share: bool, rows: dict)` — `rows` 키는 러너 키 전부(`rows_share`는 T5에서 항상 0).
  - `preflight_or_fail(gate) -> list[str]` — 누락 `"<db.table>.<col>"` 정렬 목록(빈 리스트 = 통과); 누락 시 stdout `PREFLIGHT FAIL read_contract missing=<a,b,…>` 1줄.
  - `plan_mutations(gate, dates: list[str]) -> int` — 예정 DELETE 수(= `exists(f"{DB_MART}.{t}_dist", d)`가 True인 (d, t) 쌍 수, t ∈ `MART_TABLES`).
  - `run_batch(cfg: Config, date: str, gate=None, *, token_mart_present: bool | None = None) -> BatchOutcome` — 마커는 **출력하지 않고** `line`으로 반환(출력은 `main`). `CHECK WARN …` 줄은 즉시 stdout.
  - `main(argv=None) -> int` — CLI `[batch_time] [--date D] [--from D --to D] [--log-level L]`; exit 0(전 날짜 SUCCESS) / 1(FAILURE 1개 이상 — 프리플라이트·예산 실패 포함) / 2(인자 오류).
  - `_sigterm_handler(signum, frame)` — `_status["line"] + " note=sigterm"` 출력 후 `sys.exit(143)`; `_status: dict[str, str]`(캐시 줄 — 진행 중은 `status=FAILURE … reason=sigterm`, 완료 후는 최종 줄).
  - 마커/WARN 문자열 계약: `CHECK WARN metrics_coverage missing=<n>`(서비스명은 마커 `missing_services`에만), `CHECK WARN service_not_in_usage_registry service=<s>`, `CHECK WARN token_mart_absent date=<d>`, `CHECK WARN dup_suspect:<dist>`; `reason=read_contract` / `reason=mutation_budget` / `reason=<StepError 첫 토큰>`(예 `verify_count`) / `reason=exception`; `warn=` = `CHECK WARN ` 접두 줄 수(`CHECK INFO` 제외).

- [ ] **Step 1: 실패하는 테스트 (FakeGate·RUNNERS 스텁·SQL 계약·마커·프리플라이트·예산·CLI·SIGTERM)** — `mart/token-metrics/tests/test_batch.py`

```python
"""Tests for app/batch.py — 읽기 계약 프리플라이트 → 변이 예산 프리체크 → M0/M0b →
RUNNERS(M1→M3) 오케스트레이션 → BATCH_RESULT 마커 · SIGTERM (Plan 6c T5).

FakeGate는 mart/token-usage/tests/test_batch.py의 더블(SQL 부분문자열 라우팅)을 클론하되
실행 표면(insert_select/verify_count)은 호출 자체를 금지한다 — M1/M3 러너는
`batch.RUNNERS`를 monkeypatch한 스텁으로 대체한다(steps.py 계약은 test_steps.py가 고정).
"""
import re

import pytest

from app import batch, steps
from app.ch import DB_DIM, DB_FACT, DB_MART, DB_TOKEN_DIM, DB_TOKEN_MART
from app.config import Config
from app.mart import Coverage, batch_line
from app.preflight import READ_CONTRACT
from app.steps import MART_TABLES, StepError

DATE = "2026-09-03"
DATE2 = "2026-09-04"

# 설계 §6.1 마커 형식 그대로 — 필드 순서·따옴표·elapsed 소수 1자리·선택 reason 접미
MARKER_RE = re.compile(
    r'^BATCH_RESULT status=(?P<status>SUCCESS|FAILURE) module=mart-metrics '
    r'metrics_coverage=(?P<present>\d+)/(?P<enabled>\d+) missing_services="(?P<missing>[^"]*)" '
    r'rows_mart=(?P<rows_mart>\d+) rows_check=(?P<rows_check>\d+) rows_share=(?P<rows_share>\d+) '
    r'warn=(?P<warn>\d+) elapsed=\d+\.\d(?: reason=(?P<reason>[A-Za-z0-9_]+))?$')


class FakeGate:
    """CHGate 더블 — describe/exists/delete_day/query만 응답. insert_select·verify_count는
    RUNNERS 스텁이 대체하므로 호출되면 AssertionError(오케스트레이션이 steps를 우회해
    직접 쓰지 않는다는 계약)."""

    def __init__(self, expected=None, anchors=None, not_in_usage=None, token_mart_rows=1,
                 describe_missing=None, exists_always=False):
        self.expected = list(expected or [])            # SQL_M0_EXPECTED_SERVICES 응답
        self.anchors = list(anchors or [])              # SQL_M0_ANCHOR_SERVICES 응답
        self.not_in_usage = list(not_in_usage or [])    # SQL_M0_REG_NOT_IN_USAGE 응답
        self.token_mart_rows = token_mart_rows          # SQL_M0B_TOKEN_MART_ROWS 응답
        self.describe_missing = describe_missing or {}  # {"<db.table>_dist": {"col", ...}}
        self.exists_always = exists_always
        self.describe_calls = []
        self.exists_calls = []
        self.delete_calls = []
        self.query_calls = []

    def describe(self, table_dist):
        self.describe_calls.append(table_dist)
        base = table_dist[:-len("_dist")]
        drop = self.describe_missing.get(table_dist, set())
        return [c for c in READ_CONTRACT[base] if c not in drop]

    def exists(self, table_dist, date):
        self.exists_calls.append((table_dist, date))
        return self.exists_always

    def delete_day(self, table_local, date, extra_pred=""):
        self.delete_calls.append((table_local, date))

    def wait_for_mutations(self, table_local):
        return None

    def insert_select(self, sql, params=None):
        raise AssertionError("insert_select must go through steps.run_m* (RUNNERS are stubbed)")

    def verify_count(self, table_dist, date, expected):
        raise AssertionError("verify_count must go through steps.run_m* (RUNNERS are stubbed)")

    def query(self, sql, params=None):
        self.query_calls.append((sql, params))
        if "GLOBAL NOT IN" in sql:
            return [(s,) for s in self.not_in_usage]
        if "dim_token_metrics_service_dist" in sql and "coverage_since" in sql:
            return [(s,) for s in self.expected]
        if "raw_token_metrics_summary_1d_dist" in sql:
            return [(s,) for s in self.anchors]
        if "agg_token_service_1d_dist" in sql and "count()" in sql:
            return [(self.token_mart_rows,)]
        raise AssertionError(f"unmapped query in FakeGate: {sql[:80]!r}")


def stub_runners(monkeypatch, rows_mart=3, rows_check=5, warns_m1=None, warns_m3=None,
                 m1_raises=None, fail_dates=()):
    """batch.RUNNERS를 [("rows_mart", m1), ("rows_check", m3)] 스텁으로 교체. 반환 = 호출 기록.
    m1_raises가 주어지면 fail_dates(비어 있으면 모든 날짜)에서 M1이 그 예외를 던진다."""
    calls = []

    def run_m1(gate, date):
        calls.append(("rows_mart", date))
        if m1_raises is not None and (not fail_dates or date in fail_dates):
            raise m1_raises
        return {"rows_mart": rows_mart, "warns": list(warns_m1 or [])}

    def run_m3(gate, date):
        calls.append(("rows_check", date))
        return {"rows_check": rows_check, "warns": list(warns_m3 or [])}

    monkeypatch.setattr(batch, "RUNNERS", [("rows_mart", run_m1), ("rows_check", run_m3)])
    return calls


def full_gate(**kw):
    """기대 2 서비스 = 앵커 2 서비스 (coverage 2/2, 경고 없음)."""
    return FakeGate(expected=["Mock Service A", "Mock Service B"],
                    anchors=["Mock Service A", "Mock Service B"], **kw)


def wire_main(monkeypatch, gate, **cfg_overrides):
    """main()이 실제 CH에 붙지 않도록 CHGate/load_config를 치환."""
    monkeypatch.setattr(batch, "CHGate", lambda cfg: gate)
    monkeypatch.setattr(batch, "load_config", lambda: Config(**cfg_overrides))


def marker_lines(out: str) -> list[str]:
    return [line for line in out.splitlines() if line.startswith("BATCH_RESULT")]


# ============================================================================
# SQL 상수 계약 — DB 상수 5종만·{d:Date} 바인딩·읽기 계약 테이블 (§6.1, §7.1)
# ============================================================================

def test_sql_constants_bind_date_and_use_db_constants():
    date_bound = [batch.SQL_M0_EXPECTED_SERVICES, batch.SQL_M0_ANCHOR_SERVICES,
                  batch.SQL_M0B_TOKEN_MART_ROWS]
    for sql in date_bound:
        assert "{d:Date}" in sql
    for sql in [batch.SQL_M0_REG_NOT_IN_USAGE, *date_bound]:
        assert "%(" not in sql and "coalesce(" not in sql.lower()
    assert f"{DB_DIM}.dim_token_metrics_service_dist" in batch.SQL_M0_EXPECTED_SERVICES
    assert "coverage_since <= {d:Date}" in batch.SQL_M0_EXPECTED_SERVICES
    assert "isNull(until) OR {d:Date} <= until" in batch.SQL_M0_EXPECTED_SERVICES
    assert f"{DB_FACT}.raw_token_metrics_summary_1d_dist" in batch.SQL_M0_ANCHOR_SERVICES
    assert f"{DB_TOKEN_DIM}.dim_token_service_dist" in batch.SQL_M0_REG_NOT_IN_USAGE
    assert "GLOBAL NOT IN" in batch.SQL_M0_REG_NOT_IN_USAGE
    # M0b = 읽기 계약 agg_token_service_1d(date, service)만 — 설계 §6.1 "agg_token_service_1d에 D 행 없음"
    assert f"{DB_TOKEN_MART}.agg_token_service_1d_dist" in batch.SQL_M0B_TOKEN_MART_ROWS
    assert "count()" in batch.SQL_M0B_TOKEN_MART_ROWS
    assert "token_usage_1d" not in batch.SQL_M0B_TOKEN_MART_ROWS


def test_runners_order_m1_then_m3():
    assert [k for k, _ in batch.RUNNERS] == ["rows_mart", "rows_check"]
    assert batch.RUNNERS[0][1] is steps.run_m1 and batch.RUNNERS[1][1] is steps.run_m3


# ============================================================================
# run_batch — 마커·M0·M0b·러너 경고 집계 (마커 출력은 main의 몫)
# ============================================================================

def test_marker_success_full_coverage(monkeypatch, capsys):
    stub_runners(monkeypatch, rows_mart=3, rows_check=5)
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    m = MARKER_RE.match(out.line)
    assert m, out.line
    assert out.exit_code == 0 and out.skip_share is False
    assert m.group("status") == "SUCCESS"
    assert (m.group("present"), m.group("enabled"), m.group("missing")) == ("2", "2", "-")
    assert (m.group("rows_mart"), m.group("rows_check"), m.group("rows_share"),
            m.group("warn")) == ("3", "5", "0", "0")
    assert m.group("reason") is None
    assert out.rows == {"rows_mart": 3, "rows_check": 5, "rows_share": 0}
    assert "BATCH_RESULT" not in capsys.readouterr().out   # 날짜당 정확히 1줄 — 출력은 main()


def test_no_metrics_day_is_success_with_warn(monkeypatch, capsys):
    stub_runners(monkeypatch, rows_mart=2, rows_check=1)
    gate = FakeGate(expected=["Mock Service A", "Mock Service B"], anchors=[])
    out = batch.run_batch(Config(), DATE, gate=gate)
    m = MARKER_RE.match(out.line)
    assert m.group("status") == "SUCCESS" and out.exit_code == 0     # 절대 FAILURE 아님 (§6.1)
    assert (m.group("present"), m.group("enabled")) == ("0", "2")
    assert m.group("missing") == "Mock Service A,Mock Service B"
    assert m.group("warn") == "1"
    printed = capsys.readouterr().out
    assert "CHECK WARN metrics_coverage missing=2" in printed
    assert "Mock Service" not in printed          # 서비스명은 마커 missing_services에만


def test_service_not_in_usage_registry_warn_per_service(monkeypatch, capsys):
    stub_runners(monkeypatch)
    gate = full_gate(not_in_usage=["Mock Service C", "Mock Service D"])
    out = batch.run_batch(Config(), DATE, gate=gate)
    printed = capsys.readouterr().out
    assert "CHECK WARN service_not_in_usage_registry service=Mock Service C" in printed
    assert "CHECK WARN service_not_in_usage_registry service=Mock Service D" in printed
    m = MARKER_RE.match(out.line)
    assert m.group("status") == "SUCCESS" and m.group("warn") == "2"


def test_token_mart_absent_warn_and_flag(monkeypatch, capsys):
    stub_runners(monkeypatch)
    out = batch.run_batch(Config(), DATE, gate=full_gate(), token_mart_present=False)
    assert out.skip_share is True and out.exit_code == 0
    assert f"CHECK WARN token_mart_absent date={DATE}" in capsys.readouterr().out
    m = MARKER_RE.match(out.line)
    assert m.group("status") == "SUCCESS" and m.group("warn") == "1" and m.group("rows_share") == "0"


def test_token_mart_presence_queried_when_not_given(monkeypatch):
    stub_runners(monkeypatch)
    assert batch.run_batch(Config(), DATE, gate=full_gate(token_mart_rows=0)).skip_share is True
    assert batch.run_batch(Config(), DATE, gate=full_gate(token_mart_rows=7)).skip_share is False
    given = full_gate(token_mart_rows=0)
    assert batch.run_batch(Config(), DATE, gate=given, token_mart_present=True).skip_share is False
    assert not any("agg_token_service_1d_dist" in sql for sql, _ in given.query_calls)


def test_step_warns_are_normalized_and_counted(monkeypatch, capsys):
    stub_runners(monkeypatch,
                 warns_m1=[f"dup_suspect:{DB_MART}.agg_token_model_cost_1d_dist"],
                 warns_m3=["CHECK WARN rows_rejected severity=WARN count=2",
                           "CHECK INFO manual_source severity=INFO count=1"])
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    printed = capsys.readouterr().out
    assert f"CHECK WARN dup_suspect:{DB_MART}.agg_token_model_cost_1d_dist" in printed
    assert "CHECK WARN rows_rejected severity=WARN count=2" in printed
    assert "CHECK INFO manual_source severity=INFO count=1" in printed
    assert MARKER_RE.match(out.line).group("warn") == "2"      # INFO는 warn 카운트 제외


def test_step_error_marks_failure_with_reason(monkeypatch):
    calls = stub_runners(monkeypatch, m1_raises=StepError("verify_count"))
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    m = MARKER_RE.match(out.line)
    assert out.exit_code == 1
    assert m.group("status") == "FAILURE" and m.group("reason") == "verify_count"
    assert calls == [("rows_mart", DATE)]          # M1 실패 → M3 미실행


def test_step_error_reason_is_first_token_of_message(monkeypatch, capsys):
    msg = (f"verify_count failed: {DB_MART}.agg_token_model_cost_1d_dist date={DATE} "
           f"written_rows=0 expected=3 actual=0")
    stub_runners(monkeypatch, m1_raises=StepError(msg))
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    assert MARKER_RE.match(out.line).group("reason") == "verify_count"
    assert "verify_count failed" in capsys.readouterr().err     # 상세는 stderr(마커 오염 금지)


def test_generic_exception_marks_failure_reason_exception(monkeypatch, capsys):
    stub_runners(monkeypatch, m1_raises=RuntimeError("connection reset"))
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    m = MARKER_RE.match(out.line)
    assert out.exit_code == 1 and m.group("reason") == "exception"
    assert "RuntimeError" in capsys.readouterr().err


def test_m0_query_failure_is_failure_with_zero_coverage(monkeypatch):
    stub_runners(monkeypatch)

    class BrokenGate(FakeGate):
        def query(self, sql, params=None):
            raise TimeoutError("read timeout")

    out = batch.run_batch(Config(), DATE, gate=BrokenGate())
    m = MARKER_RE.match(out.line)
    assert m.group("status") == "FAILURE" and m.group("reason") == "exception"
    assert (m.group("present"), m.group("enabled"), m.group("missing")) == ("0", "0", "-")


def test_marker_never_contains_user_id_or_payload(monkeypatch):
    stub_runners(monkeypatch, warns_m1=["user_id=abc"])
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    assert "user_id" not in out.line and "abc" not in out.line    # warns는 카운트만 마커에 반영
    assert MARKER_RE.match(out.line).group("warn") == "1"


def test_status_cache_is_failure_sigterm_while_in_progress(monkeypatch):
    seen = {}

    def run_m1(gate, date):
        seen["line"] = batch._status["line"]
        return {"rows_mart": 1, "warns": []}

    monkeypatch.setattr(batch, "RUNNERS", [("rows_mart", run_m1)])
    out = batch.run_batch(Config(), DATE, gate=full_gate())
    m = MARKER_RE.match(seen["line"])
    assert m.group("status") == "FAILURE" and m.group("reason") == "sigterm"
    assert m.group("present") == "2"                # M0 결과가 이미 반영된 캐시
    assert batch._status["line"] == out.line        # 완료 후 캐시 = 최종 줄


# ============================================================================
# 프리플라이트(읽기 계약 13컬럼) · 변이 예산 프리체크 — 첫 _run_table 전, 변이 0
# ============================================================================

def test_preflight_or_fail_reports_missing_and_describes_three_tables(capsys):
    gate = full_gate(describe_missing={f"{DB_TOKEN_MART}.agg_token_service_1d_dist": {"service"}})
    missing = batch.preflight_or_fail(gate)
    assert missing == [f"{DB_TOKEN_MART}.agg_token_service_1d.service"]
    assert sorted(gate.describe_calls) == sorted([
        f"{DB_TOKEN_MART}.token_usage_1d_dist",
        f"{DB_TOKEN_MART}.agg_token_service_1d_dist",
        f"{DB_TOKEN_DIM}.dim_token_service_dist",
    ])
    printed = capsys.readouterr().out
    assert f"PREFLIGHT FAIL read_contract missing={DB_TOKEN_MART}.agg_token_service_1d.service" in printed
    assert batch.preflight_or_fail(full_gate()) == []


def test_preflight_describe_exception_counts_as_missing():
    class NoTableGate(FakeGate):
        def describe(self, table_dist):
            if table_dist.endswith("dim_token_service_dist"):
                raise RuntimeError("Table does not exist")
            return super().describe(table_dist)

    missing = batch.preflight_or_fail(NoTableGate())
    assert missing
    assert all(m.startswith(f"{DB_TOKEN_DIM}.dim_token_service.") for m in missing)


def test_read_contract_missing_fails_all_dates_without_mutation(monkeypatch, capsys):
    calls = stub_runners(monkeypatch)
    gate = full_gate(exists_always=True,
                     describe_missing={f"{DB_TOKEN_MART}.agg_token_service_1d_dist": {"service"}})
    wire_main(monkeypatch, gate)
    code = batch.main(["--from", "2026-09-01", "--to", "2026-09-03"])
    lines = marker_lines(capsys.readouterr().out)
    assert code == 1 and len(lines) == 3
    for line in lines:
        m = MARKER_RE.match(line)
        assert m.group("status") == "FAILURE" and m.group("reason") == "read_contract"
        assert (m.group("present"), m.group("enabled")) == ("0", "0")
    assert gate.delete_calls == [] and gate.exists_calls == [] and calls == []


def test_plan_mutations_counts_existing_date_table_pairs():
    gate = full_gate(exists_always=True)
    assert batch.plan_mutations(gate, ["2026-09-01", "2026-09-02"]) == 8
    assert {t for t, _ in gate.exists_calls} == {f"{DB_MART}.{t}_dist" for t in MART_TABLES}
    assert batch.plan_mutations(full_gate(exists_always=False), ["2026-09-01"]) == 0


def test_mutation_budget_precheck_fails_before_any_delete(monkeypatch, capsys):
    calls = stub_runners(monkeypatch)
    gate = full_gate(exists_always=True)
    wire_main(monkeypatch, gate)
    code = batch.main(["--from", "2026-08-01", "--to", "2026-08-17"])     # 17일 × 4 = 68 > 64
    lines = marker_lines(capsys.readouterr().out)
    assert code == 1 and len(lines) == 17
    assert all(MARKER_RE.match(line).group("reason") == "mutation_budget" for line in lines)
    assert gate.delete_calls == [] and calls == []

    gate2 = full_gate(exists_always=True)
    wire_main(monkeypatch, gate2)
    code = batch.main(["--from", "2026-08-01", "--to", "2026-08-16"])     # 16일 × 4 = 64 = 예산
    lines = marker_lines(capsys.readouterr().out)
    assert code == 0 and len(lines) == 16
    assert all(MARKER_RE.match(line).group("status") == "SUCCESS" for line in lines)
    assert len(calls) == 32                                                # 16일 × (M1 + M3)


def test_mutation_budget_env_override(monkeypatch, capsys):
    stub_runners(monkeypatch)
    wire_main(monkeypatch, full_gate(exists_always=True), max_mutations_per_run=4)
    code = batch.main(["--from", "2026-09-01", "--to", "2026-09-02"])     # 2일 × 4 = 8 > 4
    assert code == 1
    assert capsys.readouterr().out.count("reason=mutation_budget") == 2


# ============================================================================
# main / CLI — 날짜당 마커 1줄, worst exit, --date 별칭, 인자 오류 2
# ============================================================================

def test_main_prints_one_marker_per_date_and_worst_exit(monkeypatch, capsys):
    stub_runners(monkeypatch, m1_raises=StepError("verify_count"), fail_dates=(DATE2,))
    wire_main(monkeypatch, full_gate())
    code = batch.main(["--from", DATE, "--to", DATE2])
    out = capsys.readouterr().out
    lines = marker_lines(out)
    assert code == 1 and len(lines) == 2
    assert MARKER_RE.match(lines[0]).group("status") == "SUCCESS"
    assert MARKER_RE.match(lines[1]).group("status") == "FAILURE"
    assert "user_id" not in out


def test_main_date_alias_single_day(monkeypatch, capsys):
    stub_runners(monkeypatch)
    wire_main(monkeypatch, full_gate())
    code = batch.main(["--date", DATE])
    lines = marker_lines(capsys.readouterr().out)
    assert code == 0 and len(lines) == 1


def test_main_from_without_to_exits_2(monkeypatch):
    wire_main(monkeypatch, full_gate())
    assert batch.main(["--from", DATE]) == 2


def test_main_help_exits_zero():
    with pytest.raises(SystemExit) as exc:
        batch.main(["--help"])
    assert exc.value.code == 0


# ============================================================================
# SIGTERM — 캐시 줄 재출력 + note=sigterm, exit 143
# ============================================================================

def test_sigterm_handler_prints_cached_line_and_exits_143(capsys):
    cached = batch_line("FAILURE", Coverage(2, 1, ["Mock Service B"], ["Mock Service B"]),
                        3, 0, 0, 1, 4.2, reason="sigterm")
    batch._status["line"] = cached
    with pytest.raises(SystemExit) as exc:
        batch._sigterm_handler(15, None)
    assert exc.value.code == 143
    out = capsys.readouterr().out.strip()
    assert out == cached + " note=sigterm"
    assert "reason=sigterm note=sigterm" in out
```

- [ ] **Step 2: 실패 확인**

Run: `cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_batch.py`
Expected: 수집 단계 ERROR 1건 — `ImportError: cannot import name 'batch' from 'app' (/home/mini/github/token-data-pipeline/mart/token-metrics/app/__init__.py)` (`from app import batch, steps` 줄 — `app/__init__.py`가 이미 있으므로 `ModuleNotFoundError`가 아니라 `ImportError`다; T3 Step 3과 같은 형태).

- [ ] **Step 3: 구현 (1/2 — SQL 상수·RUNNERS·BatchOutcome·SIGTERM·프리플라이트·예산)** — `mart/token-metrics/app/batch.py` (원형 `mart/token-usage/app/batch.py:1-29, 88-108` 클론 후 델타: 인라인 검증 4종 제거 → M0/M0b, `_status` 초기 줄에 `reason="sigterm"`, exit 143, 프리플라이트·`plan_mutations` 신설)

```python
"""배치 오케스트레이터 — 읽기 계약 프리플라이트 → 변이 예산 프리체크 → (날짜별) M0 커버리지
→ M0b 토큰 mart 존재 확인 → RUNNERS(M1→M3; T6 M4·T7 M2 append) → BATCH_RESULT 마커 (Plan 6c T5).

원형 = mart/token-usage/app/batch.py(Plan 3 T4). 델타:
- 인라인 검증 4종 대신 M0/M0b(설계 §6.1) — 데이터 품질 검사는 M3 테이블(steps.py)이 담당.
- 첫 _run_table 전 프리플라이트(§7.5 읽기 계약 3테이블/13컬럼 DESCRIBE) + 예산 선검사(§4.0 장부 —
  대상 날짜 전체 × 4테이블 exists 합산 > MART_METRICS_MAX_MUTATIONS_PER_RUN → 전 날짜
  FAILURE reason=mutation_budget, 변이 0).
- 마커는 run_batch가 만들고(BatchOutcome.line) main()이 날짜당 정확히 1줄 출력한다.
- 메트릭 fact가 없는 날(앵커 0)은 토큰-only 행 + WARN — 절대 FAILURE 아님(§6.1).

로깅 계약(마스터 §5.6): 어떤 로그에도 user_id 원문·레코드 페이로드를 남기지 않는다
(서비스명·검사 이름·카운트만). 마커의 reason은 `[A-Za-z0-9_]+` 토큰 하나.
날짜는 서버 바인딩(`{d:Date}`)만 사용한다(§7.1). DB명은 app.ch 상수 5종만(import 시 1회 보간).
"""
from __future__ import annotations

import argparse
import logging
import re
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Callable

from app.ch import CHGate, DB_DIM, DB_FACT, DB_MART, DB_TOKEN_DIM, DB_TOKEN_MART
from app.config import Config, load_config
from app.mart import Coverage, batch_line, compute_coverage, mutation_budget_exceeded, target_dates
from app.preflight import contract_tables, missing_columns
from app.steps import MART_TABLES, StepError, run_m1, run_m3

log = logging.getLogger("app.batch")

# =============================================================================
# M0 / M0b SQL — DB 상수 5종만, 날짜는 {d:Date} 바인딩 (§6.1)
# =============================================================================

# M0 기대 집합: 레지스트리 enabled + coverage 창(coverage_since ≤ d ≤ until | until IS NULL)
SQL_M0_EXPECTED_SERVICES = f"""
SELECT service
FROM {DB_DIM}.dim_token_metrics_service_dist
WHERE enabled = 1
  AND coverage_since <= {{d:Date}}
  AND (isNull(until) OR {{d:Date}} <= until)
ORDER BY service
"""

# M0 실제 집합: 앵커(summary) 서비스 — 메트릭 측 소스는 앵커가 있는 (date, service)만
SQL_M0_ANCHOR_SERVICES = f"""
SELECT service
FROM {DB_FACT}.raw_token_metrics_summary_1d_dist
WHERE date = {{d:Date}}
ORDER BY service
"""

# reg.service ∉ usage_svc → CHECK WARN service_not_in_usage_registry (날짜 무관, 토큰 측은 DB_TOKEN_DIM)
SQL_M0_REG_NOT_IN_USAGE = f"""
SELECT service
FROM {DB_DIM}.dim_token_metrics_service_dist
WHERE enabled = 1
  AND service GLOBAL NOT IN (SELECT service FROM {DB_TOKEN_DIM}.dim_token_service_dist WHERE enabled = 1)
ORDER BY service
"""

# M0b: 토큰 mart에 D 행이 있는가 — 읽기 계약 agg_token_service_1d(date, service)만 참조 (§6.1)
SQL_M0B_TOKEN_MART_ROWS = f"""
SELECT count()
FROM {DB_TOKEN_MART}.agg_token_service_1d_dist
WHERE date = {{d:Date}}
"""

# 실행 순서 고정: M1 → M3. T6가 ("rows_share", run_m4), T7이 ("rows_group", run_m2)를 append.
# 각 러너: fn(gate, date) -> {"<key>": int, "warns": list[str]}.
RUNNERS: list[tuple[str, Callable]] = [("rows_mart", run_m1), ("rows_check", run_m3)]

# 마커 필드(Plan 6a H 고정) — rows_group(T7)은 마커에 싣지 않고 로그만
_MARKER_ROW_KEYS = ("rows_mart", "rows_check", "rows_share")
_REASON_RE = re.compile(r"[A-Za-z0-9_]+")
_EMPTY_COVERAGE = Coverage(0, 0, [], [])


@dataclass
class BatchOutcome:
    """run_batch 결과 — exit_code 0/1, 마커 1줄(line), M0b 스킵 플래그, 러너별 행수."""
    exit_code: int
    line: str
    skip_share: bool
    rows: dict = field(default_factory=dict)


# SIGTERM 캐시 줄 — 진행 중엔 status=FAILURE reason=sigterm(부분 진행 반영), 완료 후엔 최종 줄
_status = {"line": batch_line("FAILURE", _EMPTY_COVERAGE, 0, 0, 0, 0, 0.0, reason="sigterm")}


def _sigterm_handler(signum, frame):
    print(_status["line"] + " note=sigterm", flush=True)   # 진행 중 날짜의 마커 보장 (수집기 §5.6 교훈)
    sys.exit(143)


def _scalar(rows: list[tuple]):
    return rows[0][0] if rows else None


def _normalize_warn(w: str) -> str:
    """steps.py 경고(`dup_suspect:<dist>` 등 접두 없는 코드)에는 `CHECK WARN ` 접두를 붙이고,
    이미 `CHECK WARN `/`CHECK INFO `로 시작하는 M3 요약 줄(run_m3)은 그대로 둔다."""
    if w.startswith("CHECK WARN ") or w.startswith("CHECK INFO "):
        return w
    return f"CHECK WARN {w}"


def _warn(warns: list[str], text: str) -> None:
    """경고 1줄 즉시 stdout(§7.1 조용함 금지) + 집계 목록에 추가."""
    print(text, flush=True)
    warns.append(text)


def _warn_count(warns: list[str]) -> int:
    """마커 warn= — `CHECK WARN ` 접두 줄만 센다(`CHECK INFO`는 제외)."""
    return len([w for w in warns if w.startswith("CHECK WARN ")])


def _step_reason(exc: BaseException) -> str:
    """StepError 메시지의 첫 토큰만 reason으로 — 'verify_count failed: …' → 'verify_count'.
    마커에는 테이블명·카운트를 싣지 않는다(상세는 stderr)."""
    m = _REASON_RE.match(str(exc).strip())
    return m.group(0) if m else "step_error"


def _line(status: str, coverage: Coverage, rows: dict, warns: list[str], started: float,
          reason: str = "") -> str:
    return batch_line(status, coverage, rows["rows_mart"], rows["rows_check"], rows["rows_share"],
                      _warn_count(warns), time.monotonic() - started, reason)


def preflight_or_fail(gate) -> list[str]:
    """읽기 계약(§6.1 3테이블/13컬럼) DESCRIBE 프리플라이트 — install.sh [3/6]과 같은 계약을
    배치 기동 시 다시 확인한다(사내 스키마 드리프트 방어). 반환 = 누락 `db.table.col` 정렬 목록.
    DESCRIBE 자체가 실패(테이블 부재·권한)하면 그 테이블의 컬럼 전부를 누락으로 본다."""
    described: dict[str, list[str]] = {}
    for table in contract_tables():
        try:
            described[table] = list(gate.describe(f"{table}_dist"))
        except Exception as exc:
            log.error("DESCRIBE failed: %s_dist %s", table, type(exc).__name__)
            described[table] = []
    missing = missing_columns(described)
    if missing:
        print(f"PREFLIGHT FAIL read_contract missing={','.join(missing)}", flush=True)
    return missing


def plan_mutations(gate, dates: list[str]) -> int:
    """예정 DELETE 수 = (대상 날짜 × MART_TABLES 4테이블) 중 exists(_dist, d)인 쌍의 수.
    첫 _run_table 전에 한 번만 호출한다(§4.0 장부 — 실행당 가드)."""
    planned = 0
    for d in dates:
        for table in MART_TABLES:
            if gate.exists(f"{DB_MART}.{table}_dist", d):
                planned += 1
    return planned


def _fail_all(dates: list[str], reason: str) -> int:
    """프리플라이트·예산 실패: 모든 대상 날짜에 FAILURE 마커(coverage 0/0, rows 0) — 변이 0."""
    for d in dates:
        line = batch_line("FAILURE", _EMPTY_COVERAGE, 0, 0, 0, 0, 0.0, reason)
        _status["line"] = line
        print(line, flush=True)
    return 1
```

- [ ] **Step 4: 구현 (2/2 — M0/M0b·run_batch·main)** — `mart/token-metrics/app/batch.py` 이어서 (원형 `:147-257`의 `_emit_step_warns`/`run_batch`/`main` 클론 후 델타: 러너 루프 일반화, `BatchOutcome` 반환, 예외 → `reason`, `main`에 프리플라이트·예산·`--date`/`--log-level`)

```python
# =============================================================================
# M0 커버리지 · M0b 토큰 mart 존재 (§6.1)
# =============================================================================

def _check_m0_coverage(gate, date: str, warns: list[str]) -> Coverage:
    """M0 — 기대(레지스트리 coverage 창) vs 실제(앵커). EXPECTED_LATE 없음(설계 §6.1 — 빈 목록).
    누락 서비스명은 마커 missing_services에만 싣고 WARN 줄에는 카운트만 쓴다."""
    expected = [row[0] for row in gate.query(SQL_M0_EXPECTED_SERVICES, {"d": date})]
    anchors = {row[0] for row in gate.query(SQL_M0_ANCHOR_SERVICES, {"d": date})}
    coverage = compute_coverage(expected, anchors, [])
    if coverage.missing:
        _warn(warns, f"CHECK WARN metrics_coverage missing={len(coverage.missing)}")
    for row in gate.query(SQL_M0_REG_NOT_IN_USAGE):
        _warn(warns, f"CHECK WARN service_not_in_usage_registry service={row[0]}")
    return coverage


def _token_mart_present(gate, date: str, token_mart_present: bool | None) -> bool:
    """M0b — 호출자가 명시하지 않으면 agg_token_service_1d의 D 행수로 판정."""
    if token_mart_present is None:
        return int(_scalar(gate.query(SQL_M0B_TOKEN_MART_ROWS, {"d": date})) or 0) > 0
    return bool(token_mart_present)


# =============================================================================
# run_batch — 날짜 1개: M0 → M0b → RUNNERS → 마커(반환) (§6.1, §7.1 날짜별 독립)
# =============================================================================

def run_batch(cfg: Config, date: str, gate=None, *, token_mart_present: bool | None = None) -> BatchOutcome:
    """날짜 1개의 M0→M0b→RUNNERS 전체 + 마커 1줄(반환, 출력은 main). exit_code 0=SUCCESS, 1=FAILURE.

    광역 가드: M0·M0b·러너에서 발생하는 모든 예외 → status=FAILURE 마커 + exit_code 1
    (StepError → reason=<첫 토큰>, 그 외 → reason=exception). 앵커 0건(no-metrics day)은
    예외가 아니라 WARN — M1은 토큰-only 행(no_metrics/consumer_only)을 적재하고 SUCCESS.
    _status["line"]은 단계마다 갱신해 SIGTERM 시 부분 진행이 담긴 마커가 나가게 한다.
    """
    gate = gate or CHGate(cfg)
    started = time.monotonic()
    warns: list[str] = []
    coverage = _EMPTY_COVERAGE
    rows = {k: 0 for k in _MARKER_ROW_KEYS}
    skip_share = False

    try:
        coverage = _check_m0_coverage(gate, date, warns)
        _status["line"] = _line("FAILURE", coverage, rows, warns, started, "sigterm")

        if not _token_mart_present(gate, date, token_mart_present):
            _warn(warns, f"CHECK WARN token_mart_absent date={date}")
            skip_share = True            # T6: rows_share(M4) 러너 스킵 근거 — T5는 플래그만 기록
            _status["line"] = _line("FAILURE", coverage, rows, warns, started, "sigterm")

        for key, fn in RUNNERS:
            result = fn(gate, date)
            rows[key] = int(result[key])
            for w in result["warns"]:
                _warn(warns, _normalize_warn(w))
            _status["line"] = _line("FAILURE", coverage, rows, warns, started, "sigterm")

        line = _line("SUCCESS", coverage, rows, warns, started)
        _status["line"] = line
        return BatchOutcome(0, line, skip_share, dict(rows))

    except StepError as exc:
        print(f"ERROR in run_batch(date={date}): StepError: {str(exc)[:200]}", file=sys.stderr, flush=True)
        reason = _step_reason(exc)
    except Exception as exc:
        # 예상 밖 예외(TimeoutError, RuntimeError 등) — 마커 보장 + 날짜 독립 진행.
        # 예외 메시지는 stderr로(마커 형식 오염 금지, user_id 원문 금지 — 이름·200자 요약만)
        print(f"ERROR in run_batch(date={date}): {type(exc).__name__}: {str(exc)[:200]}",
              file=sys.stderr, flush=True)
        reason = "exception"

    line = _line("FAILURE", coverage, rows, warns, started, reason)
    _status["line"] = line
    return BatchOutcome(1, line, skip_share, dict(rows))


# =============================================================================
# CLI — [batch_time] | --date D | --from D --to D ; --log-level
# =============================================================================

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="app.batch", description="token-mart-metrics 일배치 (M0→M0b→M1→M3→M4→M2)")
    parser.add_argument("batch_time", nargs="?", default=None,
                        help="ISO8601 (기본 now, KST) — target_date = batch_time - 1일")
    parser.add_argument("--date", default=None, help="단일 날짜 YYYY-MM-DD (= --from D --to D)")
    parser.add_argument("--from", dest="from_date", default=None)
    parser.add_argument("--to", dest="to_date", default=None)
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    args = parser.parse_args(argv)
    if args.date:
        args.from_date = args.to_date = args.date
    logging.basicConfig(level=getattr(logging, args.log_level), stream=sys.stderr,
                        format="%(levelname)s %(name)s %(message)s", force=True)

    signal.signal(signal.SIGTERM, _sigterm_handler)
    cfg = load_config()
    dates, _is_rerun = target_dates(args)
    if dates is None:
        return 2

    gate = CHGate(cfg)
    if preflight_or_fail(gate):                    # 읽기 계약 불일치 — 첫 날짜 처리 전, 변이 0
        return _fail_all(dates, "read_contract")

    planned = plan_mutations(gate, dates)          # 첫 _run_table 전 한 번 (§4.0 장부)
    log.info("mutation budget: planned=%d budget=%d dates=%d",
             planned, cfg.max_mutations_per_run, len(dates))
    if mutation_budget_exceeded(planned, cfg.max_mutations_per_run):
        print(f"BUDGET FAIL mutation_budget planned={planned} "
              f"budget={cfg.max_mutations_per_run} dates={len(dates)}", flush=True)
        return _fail_all(dates, "mutation_budget")

    worst = 0
    for d in dates:            # 날짜별 마커 독립 출력 — 한 날짜 FAILURE여도 나머지 계속 (§7.1)
        outcome = run_batch(cfg, d, gate=gate)
        print(outcome.line, flush=True)
        worst = max(worst, outcome.exit_code)
    return worst


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 5: 통과 확인** (파일 단위 → 전체 회귀 → CLI 스모크)

Run: `cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_batch.py`
Expected: `25 passed`

Run: `cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q`
Expected: 전부 통과 — T1~T4 누계 + 25 (`… passed`, failed 0).

Run: `cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m app.batch --help`
Expected: usage 줄에 `[batch_time]`, 옵션 `--date DATE`, `--from FROM_DATE`, `--to TO_DATE`, `--log-level {DEBUG,INFO,WARNING,ERROR}` 표시, exit 0.

Run: `cd /home/mini/github/token-data-pipeline/mart/token-metrics && python - <<'PY'
import re
from app import batch
for name in ("SQL_M0_EXPECTED_SERVICES", "SQL_M0_ANCHOR_SERVICES", "SQL_M0_REG_NOT_IN_USAGE", "SQL_M0B_TOKEN_MART_ROWS"):
    sql = getattr(batch, name)
    assert "%(" not in sql and "coalesce(" not in sql.lower() and "SELECT *" not in sql, name
print("sql-contract ok", [k for k, _ in batch.RUNNERS])
PY`
Expected: `sql-contract ok ['rows_mart', 'rows_check']`

- [ ] **Step 6: zero-diff 게이트 + 커밋**

Run: `git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-mart.yml .github/workflows/test-collector.yml`
Expected: 출력 없음(빈 diff).

```bash
cd /home/mini/github/token-data-pipeline
git add mart/token-metrics/app/batch.py mart/token-metrics/tests/test_batch.py
git commit -m "feat(mart-metrics): batch.py — 읽기 계약 프리플라이트·M0/M0b·변이 예산 프리체크·M1→M3 오케스트레이션·BATCH_RESULT 마커 (Plan 6c T5)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

**T5 설계 해석 (footer Self-Review 노트로 이관)**:
- M0b 판정 테이블 = `{DB_TOKEN_MART}.agg_token_service_1d_dist`(설계 §6.1 "`mart.agg_token_service_1d`에 D 행 없음" + 읽기 계약 `(date, service)` 2컬럼의 존재 이유). 상수 이름은 아웃라인의 `SQL_M0B_TOKEN_MART_ROWS` 유지.
- `reason=` 값은 `StepError` 메시지의 첫 `[A-Za-z0-9_]+` 토큰(`_run_table` 원형 메시지 `verify_count failed: …` → `verify_count`) — 마커에 테이블명·카운트 비노출(§5.6), 상세는 stderr.
- SIGTERM: 캐시 줄은 진행 중 `status=FAILURE … reason=sigterm`(M0·M0b·러너마다 갱신), 핸들러가 ` note=sigterm`을 덧붙여 출력하고 **exit 143**(128+SIGTERM; 원형 exit 1에서 변경 — 아웃라인 지정). 날짜 완료 후 캐시 = 최종 줄(원형과 동일).
- 마커 `warn=`은 `CHECK WARN ` 접두 줄 수(`CHECK INFO manual_source`는 제외); `_run_table`의 `dup_suspect:<dist>`는 `CHECK WARN ` 접두를 붙여 출력·집계(원형 `_emit_step_warns`와 동일 규칙).
- `missing_services` 빈 값은 T2 `batch_line` 규칙대로 `"-"`(원형과 동일 — 아웃라인 T5 예시의 `""`는 오기).
- CLI: 원형의 위치 인자 `batch_time`·`--from/--to`(T2 `target_dates` 원형 그대로 소비)에 `--date D`(= `--from D --to D` 별칭)·`--log-level`을 추가; `cfg`는 T1 `load_config()`로 읽는다(아웃라인의 `Config.from_env()` 표기는 T1 인터페이스 `load_config()`로 통일).
- 프리플라이트 DESCRIBE가 예외(테이블 부재·권한)면 그 테이블을 빈 컬럼 목록으로 `missing_columns()`에 넘긴다 — T1 구현이 `"<table>.*"`를 내든 컬럼별 항목을 내든 비어 있지 않으므로 FAILURE로 수렴.
### Task 6: M4 agg_token_model_share_1d(stretch, 분모 6모드·외부 API 단가) + M3 stretch 3블록 + batch rows_share 연결

**설계 근거**: 설계 §6.1 304(M4 컬럼·`denominator_mode` 6종·`provider_ambiguous` 후보행·`external_api` 벤더 단가·행 집합 정의·EXPECTED = 동일 키 집합 uniqExact), §6.4 323-331((3) W 가중치 1/0.1/4·모집단 usage_svc, (4) `provider(m)` = FAIL 없는 serving/standby gpu 행 서비스·다중 → `provider_ambiguous`·0개 → `external_api`/`no_provider`·`usage_includes_consumers=1` → `W(m)=W(p,m)`, 제공자 자기분 `max(W(m)−Σ_{s≠p}W(s),0)`·`consumer_tokens_exceed_provider` WARN·`token_not_reported`(W(m)=0·C>0) share=1), §6.4 (6)(외부 API 단가 ③ `/1e6`, `dim_token_vendor_price` tier='standard'), §4.3 213(`usage_includes_consumers` 의미), 정의서 §3.3(share)·§3.5(W)·§3.6(부담 = C×W(s)/W(m), I3·I4)·§3.9(③ 식)·§5.1(Qwen 240,000원 배분 예제 — `test_mart` 재현), Plan 6a `mart_metrics_tables.sql` `agg_token_model_share_1d_local` DDL **14컬럼**(date, model, service, service_group, provider_service, is_provider, denominator_mode, service_wtokens, model_total_wtokens, share, model_cost_krw, allocated_cost_krw, quality_flag, created_by — 아웃라인의 "15컬럼"은 오기, DDL이 정본), 마스터 §5.6(M3 detail 비노출).
**읽을 원형**: T3 `app/steps.py`(`canon`·`FAIL_PRED`·`_WTOK_EXPR`·`SUB_EFF_ALIAS`·`SUB_EFF_PRICE`·`SUB_REG`·`SUB_USAGE_SVC`·`SUB_ANCHOR`·`_TOK_SRC`/`_TOK_TAIL`/`_GPU_SRC`·`SQL_M1`·`_run_table`·`run_m1`), T4 `app/steps.py`(`_m3_select`·`M3_BLOCKS_CORE`·`M3_BLOCKS_STRETCH`·`build_m3_sql`·`run_m3`), T3/T4 `tests/test_steps.py`(`ddl_columns`·`insert_columns`·`sql_constants`·`FakeGate`·`M3_CORE_NAMES`·`_m3_select_header_aliases`·`M3Gate`), T5 `app/batch.py`(`RUNNERS`·`run_batch`·`BatchOutcome`·`skip_share`), T2 `app/mart.py`(`DENOMINATOR_MODES`·`allocate_shared`), T1 `app/config.py`(`Config`).

**Files:**
- Modify: `mart/token-metrics/app/steps.py` — 말미 append: `_M4_WT`·`_M4_WT_TOTAL`·`_M4_PROV_ROWS`·`_M4_PROV`·`_M4_GPU_ANY`·`_M4_VENDOR`·`_M4_M1C`·`_M4_CTES`·`_M4_WT_KEYS`·`_M4_PROV_KEYS`·`SQL_M4`·`EXPECTED_SQL_M4`·`run_m4`·M3 stretch 3블록 + `M3_BLOCKS_STRETCH.extend(...)`
- Modify: `mart/token-metrics/app/batch.py` — import에 `run_m4`, `RUNNERS` 3항, 러너 루프 `skip_share` 가드 (3 hunks)
- Modify: `mart/token-metrics/tests/test_steps.py` — T4 단언 1줄 교체 + T6 섹션 append (M4 SQL 계약 7 + `run_m4` 2 + M3 stretch 6)
- Modify: `mart/token-metrics/tests/test_batch.py` — T5 `test_runners_order_m1_then_m3` 제거(→ `test_runners_order_m1_m3_m4`) + T6 섹션 append (4)
- Modify: `mart/token-metrics/tests/test_mart.py` — `test_allocate_shared_matches_m4_all_services_semantics` append
- (읽기만) `mart/token-metrics/ddl/company/mart_metrics_tables.sql`(Plan 6a — M4 DDL 컬럼 순서 정본), `docs/superpowers/specs/2026-08-31-token-metrics-ingest-design.md`, `docs/cost-model-spec.md`. `collectors/token-usage/**`·`mart/token-usage/**`·`assets/**` 등 zero-diff 대상은 건드리지 않는다.

**Interfaces:**
- Produces:
  - `app.steps.SQL_M4: str` — `INSERT INTO {DB_MART}.agg_token_model_share_1d_dist (14컬럼 DDL 순서) WITH wt, wt_total, prov, gpu_any, m1c, models, mode, keys SELECT …`; `{d:Date}` 서버 바인딩, GLOBAL LEFT JOIN/GLOBAL IN만, `coalesce`·`ARRAY JOIN`·`NOT IN (` 없음.
  - `app.steps.EXPECTED_SQL_M4: str` — 같은 `_M4_CTES` + `SELECT uniqExact((model, service, provider_service)) FROM (<_M4_WT_KEYS> UNION ALL <_M4_PROV_KEYS>)`.
  - `app.steps.run_m4(gate, date: str) -> dict` — `{"rows_share": int, "warns": list[str]}` (`_run_table` 시퀀스, dist/local = `{DB_MART}.agg_token_model_share_1d_{dist,local}`).
  - `app.steps.M3_BLOCKS_STRETCH` 항목 3개(순서 고정): `("provider_ambiguous", …)` WARN · `("vendor_price_missing", …)` WARN · `("consumer_tokens_exceed_provider", …)` WARN — 모두 `_m3_select(...)` 12컬럼, model 컬럼 채움, gpu_type `''`.
  - 조각 상수(모듈 private, M3 stretch·M4가 공유): `_M4_WT`, `_M4_WT_TOTAL`, `_M4_PROV_ROWS`, `_M4_PROV`, `_M4_GPU_ANY`, `_M4_VENDOR`(괄호 서브쿼리 — 모델별 벤더 단가 1행 + `has_price`), `_M4_M1C`, `_M4_CTES`, `_M4_WT_KEYS`, `_M4_PROV_KEYS`.
  - `app.batch.RUNNERS == [("rows_mart", run_m1), ("rows_check", run_m3), ("rows_share", run_m4)]`; `run_batch`는 `skip_share=True`(M0b `token_mart_absent`)면 `rows_share` 러너를 호출하지 않고 `rows["rows_share"] = 0`.
  - 테스트: `test_m4_insert_column_list_matches_ddl_order`, `test_m4_denominator_modes_all_six_literals_present`, `test_m4_reads_m1_output_not_fact_for_cost`, `test_m4_external_api_formula_divides_by_1e6_and_tier_standard`, `test_m4_weight_expr_shared_with_m1`, `test_m4_quality_priority_order`, `test_m4_expected_key_tuple`, `test_run_m4_returns_rows_share_from_verify_actual_and_routes_to_m4`, `test_run_m4_dup_suspect_warn_and_step_error`, `test_m3_stretch_names_after_t6`, `test_m3_stretch_blocks_follow_core_discipline`, `test_m3_provider_ambiguous_block_uses_m4_provider_rows`, `test_m3_vendor_price_missing_block_external_api_only`, `test_m3_consumer_tokens_exceed_provider_block_provider_reported_only`, `test_run_m3_default_includes_t6_stretch_blocks`; `test_batch.py`: `test_runners_order_m1_m3_m4`, `test_token_mart_absent_skips_m4_rows_share_zero`, `test_marker_rows_share_filled`, `test_m0b_query_decides_skip_share_when_flag_not_given`; `test_mart.py`: `test_allocate_shared_matches_m4_all_services_semantics`.
- Consumes: `{DB_MART}.agg_token_model_cost_1d_dist`(같은 배치 M1 산출 — `has_gpu_rows = 1` 행의 `model_cost_krw`·`quality_flag`; 실행 순서 M1→M3→M4는 `RUNNERS` 순서가 보장), `app.steps.{canon, FAIL_PRED, _WTOK_EXPR, SUB_EFF_ALIAS, SUB_EFF_PRICE, SUB_REG, SUB_USAGE_SVC, SUB_ANCHOR, _TOK_SRC, _TOK_TAIL, _GPU_SRC, _run_table, _m3_select, M3_BLOCKS_STRETCH, DB_MART, T_M1, T_M4, CREATED_BY}`, `app.mart.{DENOMINATOR_MODES, allocate_shared}`, `app.config.Config`, `app.batch.{RUNNERS, run_batch, BatchOutcome}`(T5), 테스트 헬퍼 `tests/test_steps.py::{ddl_columns, insert_columns, FakeGate, M3Gate, M3_CORE_NAMES, _m3_select_header_aliases}`.

**M4 판정 규칙 요약**(SQL_M4 `mode` CTE의 multiIf 순서 = 설계 §6.4 (4); 테스트가 이 순서를 단언):

| 순서 | 조건(모델 m, 날짜 d) | denominator_mode | 행·값 |
|---|---|---|---|
| 1 | 제공자 후보 `n_prov >= 2` | `provider_ambiguous` | 후보별 행 `(m, p, p)` is_provider=1 + 소비자 행 `(m, s, '')`; share·allocated NULL |
| 2 | `n_prov = 0` AND 그날 gpu 행 있음(test·FAIL뿐) | `no_provider` | 소비자 행만, provider_service ''; model_cost 0·allocated 0·share = W(s)/W(m) 정보용 |
| 3 | `n_prov = 0` AND gpu 행 전무 | `external_api` | provider_service = 벤더(단가 행 부재 시 ''); allocated = ③ / 1e6, 단가 NULL이면 NULL + `vendor_price_missing` |
| 4 | `W(m) = 0` AND `C(m) > 0` | `token_not_reported` | 제공자 행 share=1·allocated=C; (provider_reported·W(p)=0 특례로 소비자 행이 있으면 share NULL) |
| 5 | 제공자 `usage_includes_consumers = 1` | `provider_reported` | W(m)=W(p,m); 제공자 행 service_wtokens = max(W(p)−Σ_{s≠p}W(s), 0) |
| 6 | 기본 | `all_services` | W(m)=Σ_s W(s,m); 제공자 행은 토큰이 없어도 존재(share 0) |

C(m) = M1 제공자 행 `(service = p, model = m, has_gpu_rows = 1)`의 `model_cost_krw`(NULL이면 NULL 전파 — `no_tco`). quality_flag 우선순위: `partial` > `no_tco`(둘 다 M1 제공자 행 값 상속) > `provider_ambiguous` > `vendor_price_missing` > `token_not_reported` > `normal`.

- [ ] **Step 1: 실패하는 테스트 — `tests/test_steps.py` 말미에 M4 SQL 계약 테스트 append (파트 A)**

T3 헬퍼(`ddl_columns`·`insert_columns`·`FakeGate`)와 T4 상수(`M3_CORE_NAMES`·`_m3_select_header_aliases`·`M3Gate`)를 그대로 쓴다. 파일 **끝**에 빈 줄 2개를 두고 아래를 append한다(테스트용 날짜 상수는 T6 자체 `M4_DATE`로 둔다 — T3 파일의 `DATE`는 T4가 쓰고, T6는 M4 케이스 날짜를 독립적으로 고정한다).

```python
# ============================================================================
# M4 agg_token_model_share_1d — 분모 6모드·외부 API 단가·M1 산출 소비 (Plan 6c T6)
# ============================================================================
from app.mart import DENOMINATOR_MODES  # noqa: E402 — T6 import (T2 상수)

M4_DATE = "2026-09-01"
M4_STRETCH_NAMES = ["provider_ambiguous", "vendor_price_missing", "consumer_tokens_exceed_provider"]


def test_m4_insert_column_list_matches_ddl_order():
    cols = ddl_columns("agg_token_model_share_1d_local")
    assert len(cols) == 14
    assert cols == ["date", "model", "service", "service_group", "provider_service", "is_provider",
                    "denominator_mode", "service_wtokens", "model_total_wtokens", "share",
                    "model_cost_krw", "allocated_cost_krw", "quality_flag", "created_by"]
    assert insert_columns(steps.SQL_M4) == cols
    outer = steps.SQL_M4[steps.SQL_M4.rindex("\nSELECT\n"):steps.SQL_M4.index("\nFROM keys AS k")]
    aliases = re.findall(r"\bAS (\w+)\s*,?\s*$", outer, re.M)
    assert aliases == cols
    assert re.search(r"'token-metrics-pipeline'\s+AS created_by", steps.SQL_M4)


def test_m4_denominator_modes_all_six_literals_present():
    assert DENOMINATOR_MODES == ("all_services", "provider_reported", "token_not_reported",
                                 "no_provider", "provider_ambiguous", "external_api")
    for m in DENOMINATOR_MODES:
        assert f"'{m}'" in steps.SQL_M4, m
    # 판정 multiIf(mode CTE) 분기 순서 = 설계 §6.4 (4): ambiguous > no_provider > external > tnr > reported > all
    seg = steps.SQL_M4[steps.SQL_M4.index("AS uic"):steps.SQL_M4.index("AS denominator_mode")]
    order = ["'provider_ambiguous'", "'no_provider'", "'external_api'", "'token_not_reported'",
             "'provider_reported'", "'all_services'"]
    positions = [seg.index(tok) for tok in order]
    assert positions == sorted(positions)
    assert "n_prov >= 2," in seg
    assert "n_prov = 0 AND has_gpu = 1," in seg
    assert "w_m = 0 AND ifNull(model_cost_krw, 0) > 0," in seg
    assert "uic = 1," in seg
    assert "if(uic = 1, w_prov, w_all)" in seg


def test_m4_reads_m1_output_not_fact_for_cost():
    assert f"{DB_MART}.agg_token_model_cost_1d_dist" in steps.SQL_M4
    assert "has_gpu_rows = 1" in steps.SQL_M4
    assert "tco_krw_per_gpu_hour" not in steps.SQL_M4
    assert steps.SUB_EFF_TCO not in steps.SQL_M4
    assert "dim_token_gpu_tco" not in steps.SQL_M4
    # 제공자(candidate) 행 = FAIL 없는 serving/standby gpu 행(설계 §6.4 (4)) — test·FAIL 행은 후보가 아니다
    assert "g.category IN ('serving', 'standby') AND NOT " + steps.FAIL_PRED in steps._M4_PROV_ROWS
    assert steps._M4_PROV_ROWS in steps.SQL_M4


def test_m4_external_api_formula_divides_by_1e6_and_tier_standard():
    sql = steps.SQL_M4
    assert "/ 1e6" in sql
    assert "tier = 'standard'" in sql
    assert steps.SUB_EFF_PRICE in sql
    for p in ("p_in", "p_cached", "p_cc", "p_out"):
        assert f"AS {p}" in sql, p
    assert ("(w.input_tokens * md.p_in + w.cache_read_tokens * md.p_cached\n"
            "                 + w.cache_creation_tokens * md.p_cc + w.output_tokens * md.p_out) / 1e6") in sql
    # 단가 행 부재/NULL → allocated NULL → vendor_price_missing (모델별 벤더 1행 — fan-out 방지)
    assert "nullIf(argMin(ifNull(p_in, -1), provider), -1)" in steps._M4_VENDOR
    # 집계 별칭이 소스 컬럼 provider를 가리면 argMin(…, provider)가 중첩 집계가 된다 — 별칭은 vendor
    assert "min(provider) AS vendor" in re.sub(r"\s+", " ", steps._M4_VENDOR)
    assert "AS provider" not in steps._M4_VENDOR
    assert "v.vendor AS vendor" in re.sub(r"\s+", " ", steps.SQL_M4)
    assert "md.denominator_mode = 'external_api' AND isNull(allocated_cost_krw)" in sql
    # 사외 API 행의 provider_service = 벤더 표기(없으면 '')
    assert "md.denominator_mode = 'external_api', md.vendor," in steps._M4_WT_KEYS


def test_m4_weight_expr_shared_with_m1():
    assert steps._WTOK_EXPR in steps.SQL_M1
    assert steps._WTOK_EXPR in steps.SQL_M4
    assert steps._WTOK_EXPR + "                  AS wtok" in steps._M4_WT
    assert (W_UNC, W_CACHE, W_OUT) == (1.0, 0.1, 4.0)
    # provider_reported 제공자 자기분 = max(W(p) − Σ_{s≠p} W(s), 0) (설계 §6.4 (4) 분모 모드 보정)
    assert "greatest(w.wtok - (md.w_all - w.wtok), 0.0)" in steps.SQL_M4
    assert "k.is_provider = 1 AND md.denominator_mode = 'provider_reported'" in steps.SQL_M4


def test_m4_quality_priority_order():
    sql = steps.SQL_M4
    order = ["'partial'", "'no_tco'", "'provider_ambiguous'", "'vendor_price_missing'",
             "'token_not_reported'", "'normal'"]
    qf = sql[sql.rindex("multiIf(", 0, sql.index("AS quality_flag")):sql.index("AS quality_flag")]
    positions = [qf.index(tok) for tok in order]
    assert positions == sorted(positions)
    assert "mc.quality_flag = 'partial'" in qf and "mc.quality_flag = 'no_tco'" in qf
    # share/allocated 특례(설계 §6.1 M4): ambiguous NULL, token_not_reported 제공자 행 1·전액, 분모 0 NULL
    assert ("multiIf(md.denominator_mode = 'provider_ambiguous', NULL,\n"
            "            md.denominator_mode = 'token_not_reported', if(k.is_provider = 1, 1.0, NULL),\n"
            "            md.w_m = 0, NULL,\n"
            "            service_wtokens / md.w_m)") in sql
    assert "md.denominator_mode = 'token_not_reported', if(k.is_provider = 1, model_cost_krw, NULL)," in sql
    assert "model_cost_krw * share)" in sql
    assert "md.denominator_mode = 'no_provider', toNullable(0.0)," in sql


def test_m4_expected_key_tuple():
    assert "uniqExact((model, service, provider_service))" in steps.EXPECTED_SQL_M4
    assert steps._M4_CTES in steps.SQL_M4 and steps._M4_CTES in steps.EXPECTED_SQL_M4
    for frag in (steps._M4_WT_KEYS, steps._M4_PROV_KEYS):
        assert frag in steps.SQL_M4
        assert frag in steps.EXPECTED_SQL_M4
    assert "UNION DISTINCT" in steps.SQL_M4
    assert "\n    UNION ALL\n" in steps.EXPECTED_SQL_M4
    assert "INSERT INTO" not in steps.EXPECTED_SQL_M4
    for x in ("u.model", "g.model"):
        assert steps.canon(x) in steps.SQL_M4 and steps.canon(x) in steps.EXPECTED_SQL_M4
    # 서브쿼리 조각 재사용(설계 Consumes): 단가·레지스트리·앵커·usage_svc·alias
    for name in ("SUB_EFF_PRICE", "SUB_REG", "SUB_ANCHOR", "SUB_USAGE_SVC", "SUB_EFF_ALIAS"):
        assert getattr(steps, name) in steps.SQL_M4, name
    assert "ARRAY JOIN" not in steps.SQL_M4 and "NOT IN (" not in steps.SQL_M4
```

- [ ] **Step 2: 실패하는 테스트 — `run_m4`·M3 stretch 테스트 append (파트 B) + T4 단언 1줄 교체**

먼저 T4의 `test_m3_core_block_names_exact` 안 한 줄을 교체한다(T4가 "T6에서 갱신"으로 예고한 단언).

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && grep -n "assert steps.M3_BLOCKS_STRETCH == \[\]" tests/test_steps.py
```

기대: 정확히 1행(`test_m3_core_block_names_exact` 본문). 그 행을 다음으로 바꾼다.

교체 전(함수 본문 내 4칸 들여쓰기 유지):
```text
    assert steps.M3_BLOCKS_STRETCH == []
```
교체 후:
```text
    assert [n for n, _ in steps.M3_BLOCKS_STRETCH][:3] == ["provider_ambiguous", "vendor_price_missing", "consumer_tokens_exceed_provider"]
```

이어서 Step 1에서 append한 블록 **바로 뒤**(파일 끝)에 빈 줄 2개를 두고 append한다.

```python
def test_run_m4_returns_rows_share_from_verify_actual_and_routes_to_m4():
    g = FakeGate(expected_overrides={"m4": 9})
    out = steps.run_m4(g, M4_DATE)
    assert out == {"rows_share": 9, "warns": []}
    # SQL_M4는 M1 테이블명도 포함(m1c) — FakeGate는 가장 긴 키(agg_token_model_share_1d)로 m4 라우팅
    assert g.order == [("exists", "m4"), ("delete", "m4"), ("insert", "m4"), ("query", "m4"), ("verify", "m4")]
    assert g.verify_calls == [("m4", M4_DATE, 9)]
    assert g.written[0][1] is steps.SQL_M4
    assert g.query_calls[0][1] is steps.EXPECTED_SQL_M4
    assert g.delete_preds == [("m4", "")]


def test_run_m4_dup_suspect_warn_and_step_error():
    g = FakeGate(expected_overrides={"m4": 4}, verify_actual=5)
    out = steps.run_m4(g, M4_DATE)
    assert out["rows_share"] == 5
    assert out["warns"] == [f"dup_suspect:{DB_MART}.agg_token_model_share_1d_dist"]
    with pytest.raises(steps.StepError):
        steps.run_m4(FakeGate(verify_ok=False), M4_DATE)


# --- M3 stretch 3블록 (share 경고) ---------------------------------------------------

def test_m3_stretch_names_after_t6():
    assert [n for n, _ in steps.M3_BLOCKS_STRETCH][:3] == M4_STRETCH_NAMES
    blocks = steps.M3_BLOCKS_CORE + steps.M3_BLOCKS_STRETCH
    assert len(blocks) >= 16
    sql = steps.build_m3_sql(blocks)
    assert sql.count("\nUNION ALL\n") == len(blocks) - 1
    assert len(set(n for n, _ in blocks)) == len(blocks)      # 이름 중복 없음(core와 겹치지 않음)
    for name in M4_STRETCH_NAMES:
        assert name not in M3_CORE_NAMES


def test_m3_stretch_blocks_follow_core_discipline():
    stretch = dict(steps.M3_BLOCKS_STRETCH)
    for name in M4_STRETCH_NAMES:
        sql = stretch[name]
        assert sql.startswith("SELECT\n"), name
        assert _m3_select_header_aliases(sql) == list(steps.M3_COLUMNS), name
        assert f"'{name}' AS check_name" in sql, name
        assert "'WARN' AS severity" in sql, name
        assert "    {d:Date} AS date," in sql, name
        assert "'token-metrics-pipeline' AS created_by" in sql, name
        assert "\nUNION ALL\n" not in sql and "\nUNION DISTINCT\n" not in sql, name
        assert "coalesce(" not in sql.lower() and "SELECT *" not in sql, name
        assert "%(" not in sql, name
        header = sql.split("\nFROM", 1)[0]
        model_line = next(ln for ln in header.splitlines() if ln.endswith(" AS model,"))
        assert "''" not in model_line, name                     # 모델 단위 검사 — model 컬럼 채움
        gpu_line = next(ln for ln in header.splitlines() if ln.endswith(" AS gpu_type,"))
        assert gpu_line.strip() == "'' AS gpu_type,", name
        assert "concat('model=', " in sql, name
        assert "reported_" not in sql.split("\nFROM", 1)[0], name   # detail에 응답 원문 없음(§5.6)


def test_m3_provider_ambiguous_block_uses_m4_provider_rows():
    sql = dict(steps.M3_BLOCKS_STRETCH)["provider_ambiguous"]
    assert steps._M4_PROV in sql
    assert "WHERE p.n_prov >= 2" in sql
    assert "toNullable(toFloat64(p.n_prov)) AS observed" in sql
    assert "toNullable(toFloat64(1)) AS threshold" in sql


def test_m3_vendor_price_missing_block_external_api_only():
    sql = dict(steps.M3_BLOCKS_STRETCH)["vendor_price_missing"]
    assert steps._M4_GPU_ANY in sql              # gpu 행이 전혀 없는 모델만(no_provider 미발화)
    assert steps._M4_VENDOR in sql
    assert "WHERE ga.has_gpu = 0" in sql
    assert ("(v.has_price = 0 OR isNull(v.p_in) OR isNull(v.p_cached)"
            " OR isNull(v.p_cc) OR isNull(v.p_out))") in sql
    assert f"u.service GLOBAL IN {steps.SUB_USAGE_SVC}" in sql


def test_m3_consumer_tokens_exceed_provider_block_provider_reported_only():
    sql = dict(steps.M3_BLOCKS_STRETCH)["consumer_tokens_exceed_provider"]
    assert steps._M4_PROV in sql and steps._M4_WT in sql and steps._M4_WT_TOTAL in sql
    assert "r.usage_includes_consumers = 1" in sql
    assert "WHERE p.n_prov = 1" in sql
    assert "(t.w_all - wp.wtok) > wp.wtok" in sql
    assert "toNullable(toFloat64(t.w_all - wp.wtok)) AS observed" in sql
    assert "toNullable(toFloat64(wp.wtok)) AS threshold" in sql


def test_run_m3_default_includes_t6_stretch_blocks():
    gate = M3Gate([], rows=1)
    steps.run_m3(gate, M4_DATE)
    inserted_sql = gate.inserted[0][0]
    for name in M4_STRETCH_NAMES:
        assert f"'{name}' AS check_name" in inserted_sql, name
```

- [ ] **Step 3: RED 확인**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_steps.py 2>&1 | tail -n 20
```

기대(요지): T6가 추가한 15개 + 교체한 1개가 실패한다 — `FAILED tests/test_steps.py::test_m4_insert_column_list_matches_ddl_order - AttributeError: module 'app.steps' has no attribute 'SQL_M4'. Did you mean: 'SQL_M1'?`(M4 7개·`run_m4` 2개 동일 원인), `FAILED tests/test_steps.py::test_m3_core_block_names_exact - AssertionError: assert [] == ['provider_am...eed_provider']`, `test_m3_stretch_names_after_t6`(같은 단언), `test_m3_stretch_blocks_follow_core_discipline - KeyError: 'provider_ambiguous'`(블록 3개 테스트 동일), `test_run_m3_default_includes_t6_stretch_blocks`(check_name 부재). 마지막 줄 `16 failed, N passed`(N = T3+T4 기존 통과 수).

- [ ] **Step 4: 구현 — `app/steps.py` 말미 append (파트 1: M4 조각 상수)**

`app/steps.py`의 **끝**(`run_m3` 정의 뒤)에 빈 줄 2개를 두고 append한다. 조각은 M1 정본을 재사용한다: `canon`·`FAIL_PRED`·`_WTOK_EXPR`·`_TOK_SRC`/`_TOK_TAIL`(usage_svc 모집단)·`_GPU_SRC`·`SUB_ANCHOR`·`SUB_EFF_PRICE`·`SUB_REG`. 새 `SUB_*` 이름은 만들지 않는다(T3의 `test_sub_queries_shared_between_insert_and_expected`가 `SUB_` 접두 전부를 검사) — M4 전용 조각은 `_M4_*`.

```python
# ============================================================================
# M4 agg_token_model_share_1d — 공유 모델 비용 배분 (설계 §6.1 M4, §6.4 (3)~(6); Plan 6c T6)
#
# grain: date × model(canon) × service × provider_service. 행 = (그날 그 모델에 토큰이 있는
# usage_svc 서비스: wt) ∪ (제공자 후보 전부: prov_rows — 다중이면 후보별 행, share NULL).
# 모델 단위 판정(mode CTE)은 설계 §6.4 (4) 순서로 고정:
#   n_prov >= 2                       → provider_ambiguous (후보 행 is_provider=1, share·allocated NULL)
#   n_prov = 0 AND gpu 행 있음(test뿐) → no_provider        (C=0, allocated 0, share는 정보용)
#   n_prov = 0 AND gpu 행 전혀 없음    → external_api       (벤더 단가 ③ / 1e6, tier='standard')
#   W(m) = 0 AND C > 0                → token_not_reported (제공자 행 share=1 전액, I8)
#   usage_includes_consumers = 1      → provider_reported  (W(m)=W(p,m), 제공자 자기분 = max(W(p)−Σ_{s≠p}W(s), 0))
#   기본                               → all_services       (W(m)=Σ_s W(s,m), 정의서 3.6)
# C(m)은 같은 배치에서 선행 적재된 M1 제공자 행(has_gpu_rows=1)의 model_cost_krw를 읽는다
# (설계 해석 — TCO 재계산 없음: 실행 순서 M1 → M3 → M4는 batch.RUNNERS가 보장).
# quality_flag 우선순위: partial > no_tco > provider_ambiguous > vendor_price_missing
#                       > token_not_reported > normal (partial/no_tco = M1 제공자 행 값 상속).
# ============================================================================

# wt — (service, model) 가중 토큰 + 토큰 4성분(외부 API 단가식용). 모집단 = usage_svc(_TOK_TAIL).
_M4_WT = f"""SELECT u.service                     AS service,
           any(u.service_group)          AS service_group,
           {canon('u.model')}            AS model,
           sum(u.input_tokens)           AS input_tokens,
           sum(u.cache_read_tokens)      AS cache_read_tokens,
           sum(u.cache_creation_tokens)  AS cache_creation_tokens,
           sum(u.output_tokens)          AS output_tokens,
           {_WTOK_EXPR}                  AS wtok
    {_TOK_SRC}
    {_TOK_TAIL}"""

# wt_total — 모델별 Σ_s W(s,m) (all_services 분모)
_M4_WT_TOTAL = f"""SELECT model, sum(wtok) AS w_all
    FROM
    (
        {_M4_WT}
    )
    GROUP BY model"""

# prov_rows — 제공자 후보 (model, service): 앵커 서비스의 FAIL 없는 serving/standby gpu 행 (C>0 성립 행)
_M4_PROV_ROWS = f"""SELECT {canon('g.model')} AS model, g.service AS service
    {_GPU_SRC}
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN (SELECT service FROM {SUB_ANCHOR})
      AND g.category IN ('serving', 'standby') AND NOT {FAIL_PRED}
    GROUP BY g.service, {canon('g.model')}"""

# prov — 모델별 후보 배열·수·단일 제공자(다중/0이면 '')
_M4_PROV = f"""SELECT model,
           arraySort(groupUniqArray(service))  AS providers,
           length(providers)                   AS n_prov,
           if(n_prov = 1, providers[1], '')    AS provider
    FROM
    (
        {_M4_PROV_ROWS}
    )
    GROUP BY model"""

# gpu_any — 그날 gpu 행이 하나라도 있는 모델(카테고리·FAIL 무관): no_provider vs external_api 판별
_M4_GPU_ANY = f"""SELECT {canon('g.model')} AS model, 1 AS has_gpu
    {_GPU_SRC}
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN (SELECT service FROM {SUB_ANCHOR})
    GROUP BY {canon('g.model')}"""

# vendor — 모델별 벤더 단가 1행(provider 최소값으로 고정 — (provider, model) 다중 등록 시 fan-out 방지).
# 단가 NULL은 -1 sentinel로 argMin을 통과시켜 NULL 그대로 돌려준다(SUB_EFF_* 규약과 동일).
# 별칭은 `vendor`(≠ 소스 컬럼 provider) — `AS provider`로 두면 같은 SELECT의 argMin(…, provider)가
# 컬럼 대신 집계 별칭을 가리켜(prefer_column_name_to_alias=0) 중첩 집계 오류가 난다.
_M4_VENDOR = f"""(SELECT model,
               min(provider)                                        AS vendor,
               nullIf(argMin(ifNull(p_in, -1), provider), -1)       AS p_in,
               nullIf(argMin(ifNull(p_cached, -1), provider), -1)   AS p_cached,
               nullIf(argMin(ifNull(p_cc, -1), provider), -1)       AS p_cc,
               nullIf(argMin(ifNull(p_out, -1), provider), -1)      AS p_out,
               1                                                    AS has_price
        FROM {SUB_EFF_PRICE} AS ep
        GROUP BY model)"""

# m1c — 같은 배치 M1 제공자 행: C(m)·품질 플래그 (has_gpu_rows=1 행만)
_M4_M1C = f"""SELECT model, service, model_cost_krw, quality_flag, 1 AS has_m1
    FROM {DB_MART}.{T_M1}_dist
    WHERE date = {{d:Date}} AND has_gpu_rows = 1"""
```

- [ ] **Step 5: 구현 — `app/steps.py` 말미 append (파트 2: `_M4_CTES`·키 조각·`SQL_M4`·`EXPECTED_SQL_M4`·`run_m4`)**

Step 4 블록 바로 뒤에 append한다. INSERT 컬럼 목록은 Plan 6a DDL `agg_token_model_share_1d_local` 선언 순서 14개 그대로(위치 기반 INSERT 금지). `mode` CTE의 multiIf 분기 순서가 판정 규칙표의 1→6이다.

```python
# 공통 CTE 블록 — SQL_M4와 EXPECTED_SQL_M4가 문자 단위로 공유(파생 오차 0). keys는 INSERT만 붙인다.
_M4_CTES = f"""WITH
    wt AS (
        {_M4_WT}
    ),
    wt_total AS (
        {_M4_WT_TOTAL}
    ),
    prov AS (
        {_M4_PROV}
    ),
    gpu_any AS (
        {_M4_GPU_ANY}
    ),
    m1c AS (
        {_M4_M1C}
    ),
    models AS (
        SELECT model FROM wt
        UNION DISTINCT
        SELECT model FROM prov
    ),
    mode AS (
        -- 모델 단위 판정(모듈 상단 주석 순서). 미스 값: n_prov 0, has_gpu 0, w_all 0, uic 0, C NULL.
        SELECT m.model                                   AS model,
               p.n_prov                                  AS n_prov,
               p.provider                                AS provider,
               ga.has_gpu                                AS has_gpu,
               mt.w_all                                  AS w_all,
               wp.wtok                                   AS w_prov,
               r.usage_includes_consumers                AS uic,
               if(uic = 1, w_prov, w_all)                AS w_m,
               mc.model_cost_krw                         AS model_cost_krw,
               v.vendor                                  AS vendor,
               v.p_in                                    AS p_in,
               v.p_cached                                AS p_cached,
               v.p_cc                                    AS p_cc,
               v.p_out                                   AS p_out,
               multiIf(n_prov >= 2,                                 'provider_ambiguous',
                       n_prov = 0 AND has_gpu = 1,                  'no_provider',
                       n_prov = 0,                                  'external_api',
                       w_m = 0 AND ifNull(model_cost_krw, 0) > 0,   'token_not_reported',
                       uic = 1,                                     'provider_reported',
                       'all_services')                   AS denominator_mode
        FROM models AS m
        GLOBAL LEFT JOIN prov AS p ON p.model = m.model
        GLOBAL LEFT JOIN gpu_any AS ga ON ga.model = m.model
        GLOBAL LEFT JOIN wt_total AS mt ON mt.model = m.model
        GLOBAL LEFT JOIN wt AS wp ON wp.model = p.model AND wp.service = p.provider
        GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = p.provider
        GLOBAL LEFT JOIN m1c AS mc ON mc.model = p.model AND mc.service = p.provider
        GLOBAL LEFT JOIN {_M4_VENDOR} AS v ON v.model = m.model
    )"""

# 키 조각 — SQL_M4 keys(UNION DISTINCT)와 EXPECTED_SQL_M4(UNION ALL + uniqExact) 공유.
# wt 행의 provider_service: 단일 제공자면 p, external_api면 벤더 표기(없으면 ''), 그 외 ''.
_M4_WT_KEYS = """SELECT w.model AS model, w.service AS service,
           multiIf(md.n_prov = 1, md.provider,
                   md.denominator_mode = 'external_api', md.vendor,
                   '')                                          AS provider_service,
           toUInt8(md.n_prov = 1 AND w.service = md.provider)   AS is_provider
    FROM wt AS w
    GLOBAL LEFT JOIN mode AS md ON md.model = w.model"""
_M4_PROV_KEYS = f"""SELECT model, service, service AS provider_service, toUInt8(1) AS is_provider
    FROM
    (
        {_M4_PROV_ROWS}
    )"""

SQL_M4 = f"""
INSERT INTO {DB_MART}.{T_M4}_dist
    (date, model, service, service_group, provider_service, is_provider, denominator_mode,
     service_wtokens, model_total_wtokens, share, model_cost_krw, allocated_cost_krw,
     quality_flag, created_by)
{_M4_CTES},
    keys AS (
        {_M4_WT_KEYS}
        UNION DISTINCT
        {_M4_PROV_KEYS}
    )
SELECT
    {{d:Date}}                                                        AS date,
    k.model                                                           AS model,
    k.service                                                         AS service,
    multiIf(r.service_group != '', r.service_group,
            an.service_group != '', an.service_group,
            w.service_group)                                          AS service_group,
    k.provider_service                                                AS provider_service,
    k.is_provider                                                     AS is_provider,
    md.denominator_mode                                               AS denominator_mode,
    -- provider_reported 제공자 자기분 = max(W(p) − Σ 소비자 W(s), 0) = max(2·W(p) − W_all, 0)
    if(k.is_provider = 1 AND md.denominator_mode = 'provider_reported',
       greatest(w.wtok - (md.w_all - w.wtok), 0.0), w.wtok)           AS service_wtokens,
    md.w_m                                                            AS model_total_wtokens,
    multiIf(md.denominator_mode = 'provider_ambiguous', NULL,
            md.denominator_mode = 'token_not_reported', if(k.is_provider = 1, 1.0, NULL),
            md.w_m = 0, NULL,
            service_wtokens / md.w_m)                                 AS share,
    multiIf(md.denominator_mode = 'no_provider', toNullable(0.0),
            mc.has_m1 = 1, mc.model_cost_krw,
            NULL)                                                     AS model_cost_krw,
    multiIf(md.denominator_mode = 'external_api',
                (w.input_tokens * md.p_in + w.cache_read_tokens * md.p_cached
                 + w.cache_creation_tokens * md.p_cc + w.output_tokens * md.p_out) / 1e6,
            md.denominator_mode = 'provider_ambiguous', NULL,
            md.denominator_mode = 'no_provider', toNullable(0.0),
            md.denominator_mode = 'token_not_reported', if(k.is_provider = 1, model_cost_krw, NULL),
            model_cost_krw * share)                                   AS allocated_cost_krw,
    -- 우선순위 고정(설계 §6.1 M4): partial > no_tco > provider_ambiguous > vendor_price_missing
    --                             > token_not_reported > normal
    multiIf(
        mc.quality_flag = 'partial',                                              'partial',
        mc.quality_flag = 'no_tco',                                               'no_tco',
        md.denominator_mode = 'provider_ambiguous',                               'provider_ambiguous',
        md.denominator_mode = 'external_api' AND isNull(allocated_cost_krw),      'vendor_price_missing',
        md.denominator_mode = 'token_not_reported',                               'token_not_reported',
        'normal')                                                     AS quality_flag,
    '{CREATED_BY}'                                                    AS created_by
FROM keys AS k
GLOBAL LEFT JOIN mode AS md ON md.model = k.model
GLOBAL LEFT JOIN wt AS w ON w.model = k.model AND w.service = k.service
GLOBAL LEFT JOIN m1c AS mc ON mc.model = k.model AND mc.service = k.provider_service
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = k.service
GLOBAL LEFT JOIN {SUB_ANCHOR} AS an ON an.service = k.service
"""

EXPECTED_SQL_M4 = f"""
{_M4_CTES}
SELECT uniqExact((model, service, provider_service)) FROM (
    {_M4_WT_KEYS}
    UNION ALL
    {_M4_PROV_KEYS}
)
"""
# ↑ M4 행 그레인은 date×model×service×provider_service — keys(UNION DISTINCT)의 distinct 키 수.
# 좌측 keys에 붙는 mode(GROUP BY model 유니크)·wt(GROUP BY service, model)·m1c(M1 그레인
# date×service×model)·reg·anchor(service 유니크)는 전부 키 유니크라 fan-out이 없다.


def run_m4(gate, date: str) -> dict:
    """M4 — mart.agg_token_model_share_1d 1테이블. 반환 {"rows_share": actual, "warns": [...]}
    (마커 rows_share의 소스). 토큰 mart 부재일(M0b token_mart_absent)은 batch가 이 러너를
    건너뛰고 rows_share=0을 기록한다 — 여기서는 판단하지 않는다(설계 §6.1 M0b)."""
    warns: list = []
    rows = _run_table(gate, date, f"{DB_MART}.{T_M4}_dist", f"{DB_MART}.{T_M4}_local",
                      SQL_M4, EXPECTED_SQL_M4, warns)
    return {"rows_share": rows, "warns": warns}
```

- [ ] **Step 6: 구현 — `app/steps.py` 말미 append (파트 3: M3 stretch 3블록 + `M3_BLOCKS_STRETCH.extend`)**

Step 5 블록 바로 뒤에 append한다. `M3_BLOCKS_STRETCH`는 T4가 `[]`로 선언한 리스트 객체를 **그대로 extend**한다(재대입 금지 — `run_m3(blocks=None)`이 호출 시점에 `M3_BLOCKS_CORE + M3_BLOCKS_STRETCH`를 평가하므로 모듈 말미 extend가 반영된다). 블록 본문의 UNION은 없고, 내부 서브쿼리는 전부 4칸 이상 들여쓰기(최상위 조립 토큰 `"\nUNION ALL\n"`과 충돌 없음).

```python
# ============================================================================
# M3 stretch — share 경고 3블록 (설계 §6.1 M3 stretch, §6.4 (4)(6); Plan 6c T6)
#   M4와 같은 조각(_M4_PROV/_M4_WT/_M4_WT_TOTAL/_M4_GPU_ANY/_M4_VENDOR)을 쓴다 — M4 판정과
#   검사 검출이 문자 단위로 같은 집합을 본다. 3블록 모두 모델 단위(model 컬럼 채움, gpu_type '').
# ============================================================================

# --- 14) provider_ambiguous WARN — 제공자 후보 다중 모델(M4 후보별 행·share NULL·배부 보류)
_M3_PROVIDER_AMBIGUOUS = _m3_select(
    "provider_ambiguous", "WARN",
    service_group="''", service="''", model="p.model",
    observed="p.n_prov", threshold="1",
    detail="concat('model=', p.model, ' providers=', toString(p.n_prov))",
    body=f"""FROM
(
    {_M4_PROV}
) AS p
WHERE p.n_prov >= 2""")

# --- 15) vendor_price_missing WARN — external_api 모델(gpu 행 전무·토큰 사용 있음) 중 유효 단가 부재/NULL
#         no_provider(test 전용 gpu 행) 모델은 gpu_any에 잡혀 발화하지 않는다(설계 §6.4 (4)).
_M3_VENDOR_PRICE_MISSING = _m3_select(
    "vendor_price_missing", "WARN",
    service_group="''", service="''", model="x.model",
    observed="1", threshold="0", detail="concat('model=', x.model)",
    body=f"""FROM
(
    SELECT {canon('u.model')} AS model
    {_TOK_SRC}
    WHERE u.date = {{d:Date}} AND u.service GLOBAL IN {SUB_USAGE_SVC}
    GROUP BY {canon('u.model')}
) AS x
GLOBAL LEFT JOIN
(
    {_M4_GPU_ANY}
) AS ga ON ga.model = x.model
GLOBAL LEFT JOIN {_M4_VENDOR} AS v ON v.model = x.model
WHERE ga.has_gpu = 0
  AND (v.has_price = 0 OR isNull(v.p_in) OR isNull(v.p_cached) OR isNull(v.p_cc) OR isNull(v.p_out))""")

# --- 16) consumer_tokens_exceed_provider WARN — provider_reported(usage_includes_consumers=1) 모델에서
#         Σ_{s≠p} W(s,m) > W(p,m) (제공자 자기분 0 클램프 발생 — 설계 §6.4 (4) 분모 모드 보정)
_M3_CONSUMER_TOKENS_EXCEED_PROVIDER = _m3_select(
    "consumer_tokens_exceed_provider", "WARN",
    service_group="r.service_group", service="p.provider", model="p.model",
    observed="t.w_all - wp.wtok", threshold="wp.wtok",
    detail="concat('model=', p.model)",
    body=f"""FROM
(
    {_M4_PROV}
) AS p
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = p.provider
GLOBAL LEFT JOIN
(
    {_M4_WT_TOTAL}
) AS t ON t.model = p.model
GLOBAL LEFT JOIN
(
    {_M4_WT}
) AS wp ON wp.model = p.model AND wp.service = p.provider
WHERE p.n_prov = 1 AND r.usage_includes_consumers = 1 AND (t.w_all - wp.wtok) > wp.wtok""")

M3_BLOCKS_STRETCH.extend([
    ("provider_ambiguous", _M3_PROVIDER_AMBIGUOUS),
    ("vendor_price_missing", _M3_VENDOR_PRICE_MISSING),
    ("consumer_tokens_exceed_provider", _M3_CONSUMER_TOKENS_EXCEED_PROVIDER),
])
```

- [ ] **Step 7: GREEN 확인 — `tests/test_steps.py`**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_steps.py 2>&1 | tail -n 3
```

기대: 마지막 줄에 `failed` 없음, `passed` 수 = Step 3의 `N + 16`(T6 추가 15 + 교체 1). T3 전역 계약 테스트(`test_all_sql_constants_use_date_binding_and_no_percent_format`·`test_no_coalesce_anywhere_in_sql`·`test_global_join_and_global_in_only`·`test_sub_queries_shared_between_insert_and_expected`)가 `SQL_M4`·`EXPECTED_SQL_M4`를 자동 포함해 통과해야 한다(`{d:Date}` 포함·`{{`/`}}` 잔재 없음·`GLOBAL LEFT JOIN`/`GLOBAL IN`만·`coalesce` 없음).

추가 확인(문자열 계약을 눈으로):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -c "
from app import steps
print(len(steps.M3_BLOCKS_CORE + steps.M3_BLOCKS_STRETCH))
print(steps.build_m3_sql(steps.M3_BLOCKS_CORE + steps.M3_BLOCKS_STRETCH).count(chr(10) + 'UNION ALL' + chr(10)))
print(steps.SQL_M4.count('GLOBAL LEFT JOIN'), 'ARRAY JOIN' in steps.SQL_M4, 'NOT IN (' in steps.SQL_M4)
"
```

기대 출력: `16` / `15` / `18 False False`(GLOBAL LEFT JOIN 18회 = CTE wt 1 + wt_total 1 + prov 1 + gpu_any 1 + mode 7 + keys 2(wt_keys의 mode 조인 1 + prov_keys의 alias 조인 1) + 최종 SELECT 5; `ARRAY JOIN`·`NOT IN (` 없음).

- [ ] **Step 8: 실패하는 테스트 — `tests/test_batch.py`: T5 러너 순서 테스트 교체 + T6 섹션 append**

T5의 `test_runners_order_m1_then_m3`(`["rows_mart","rows_check"]` 단언 — T5가 "T6/T7에서 갱신" 예고)를 **함수째 삭제**하고, 아래 T6 섹션(`test_runners_order_m1_m3_m4` 포함)을 파일 끝에 append한다. 러너는 전부 `monkeypatch`로 스텁하므로 게이트는 M0/M0b 조회만 응답하는 최소 더블(`_T6Gate`)을 T6 섹션 안에 자체 정의한다(T5 FakeGate 이름·시그니처에 의존하지 않음).

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && grep -n "def test_runners_order_m1_then_m3" tests/test_batch.py
```

기대: 1행. 그 `def` 행부터 다음 빈 줄 2개 전까지(함수 본문 2줄 — `assert [k for k, _ in batch.RUNNERS] == ["rows_mart", "rows_check"]`와 러너 함수 튜플 단언)를 삭제한다.

이어서 파일 끝에 빈 줄 2개를 두고 append한다.

```python
# ============================================================================
# T6 — M4 러너 연결: RUNNERS 3항·M0b token_mart_absent 시 M4 스킵(rows_share=0)·마커 rows_share
# ============================================================================
from app.config import Config  # noqa: E402 — T1 Config 기본값 인스턴스
from app.steps import run_m4  # noqa: E402

T6_DATE = "2026-09-03"


class _T6Gate:
    """M0/M0b 조회만 응답하는 최소 게이트(러너는 monkeypatch 스텁이라 테이블 접근 없음)."""

    def __init__(self, expected=("Mock Service A", "Mock Service B"), anchors=None, token_rows=1):
        self.expected = list(expected)
        self.anchors = self.expected if anchors is None else list(anchors)
        self.token_rows = token_rows
        self.queries = []

    def query(self, sql, params=None):
        self.queries.append(sql)
        if "GLOBAL NOT IN" in sql:
            return []
        if "raw_token_metrics_summary_1d_dist" in sql:
            return [(s,) for s in self.anchors]
        if "dim_token_metrics_service_dist" in sql:
            return [(s,) for s in self.expected]
        if "agg_token_service_1d_dist" in sql:
            return [(self.token_rows,)]
        raise AssertionError(f"unexpected query: {sql[:80]!r}")

    def describe(self, table):
        raise AssertionError("describe must not be called from run_batch")

    def exists(self, table_dist, date):
        raise AssertionError("exists must not be called (runners are stubbed)")

    def delete_day(self, table_local, date, extra_pred=""):
        raise AssertionError("delete_day must not be called (runners are stubbed)")


def _stub_runners(monkeypatch, m4_rows=7):
    calls = {"m1": [], "m3": [], "m4": []}

    def m1(gate, date):
        calls["m1"].append(date)
        return {"rows_mart": 3, "warns": []}

    def m3(gate, date):
        calls["m3"].append(date)
        return {"rows_check": 5, "warns": []}

    def m4(gate, date):
        calls["m4"].append(date)
        return {"rows_share": m4_rows, "warns": []}

    monkeypatch.setattr(batch, "RUNNERS", [("rows_mart", m1), ("rows_check", m3), ("rows_share", m4)])
    return calls


def test_runners_order_m1_m3_m4():
    assert [k for k, _ in batch.RUNNERS] == ["rows_mart", "rows_check", "rows_share"]
    assert batch.RUNNERS[2][1] is run_m4
    assert [fn.__name__ for _, fn in batch.RUNNERS] == ["run_m1", "run_m3", "run_m4"]


def test_token_mart_absent_skips_m4_rows_share_zero(monkeypatch):
    calls = _stub_runners(monkeypatch, m4_rows=7)
    out = batch.run_batch(Config(), T6_DATE, gate=_T6Gate(), token_mart_present=False)
    assert calls["m1"] == [T6_DATE] and calls["m3"] == [T6_DATE]
    assert calls["m4"] == []                       # M4 러너 호출 0회
    assert out.skip_share is True
    assert out.rows["rows_share"] == 0
    assert "status=SUCCESS" in out.line
    assert "rows_mart=3 rows_check=5 rows_share=0 warn=1" in out.line
    assert out.exit_code == 0


def test_marker_rows_share_filled(monkeypatch):
    calls = _stub_runners(monkeypatch, m4_rows=7)
    out = batch.run_batch(Config(), T6_DATE, gate=_T6Gate(), token_mart_present=True)
    assert calls["m4"] == [T6_DATE]
    assert out.skip_share is False
    assert out.rows["rows_share"] == 7
    assert "rows_mart=3 rows_check=5 rows_share=7 warn=0" in out.line
    assert "status=SUCCESS" in out.line


def test_m0b_query_decides_skip_share_when_flag_not_given(monkeypatch):
    calls = _stub_runners(monkeypatch, m4_rows=2)
    gate = _T6Gate(token_rows=0)
    out = batch.run_batch(Config(), T6_DATE, gate=gate)
    assert any("agg_token_service_1d_dist" in q for q in gate.queries)
    assert out.skip_share is True and calls["m4"] == [] and "rows_share=0 warn=1" in out.line
    gate2 = _T6Gate(token_rows=12)
    out2 = batch.run_batch(Config(), T6_DATE, gate=gate2)
    assert out2.skip_share is False and calls["m4"] == [T6_DATE] and "rows_share=2 warn=0" in out2.line
```

- [ ] **Step 9: RED 확인 — `tests/test_batch.py`**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_batch.py 2>&1 | tail -n 12
```

기대(요지): `FAILED tests/test_batch.py::test_runners_order_m1_m3_m4 - AssertionError: assert ['rows_mart', 'rows_check'] == ['rows_mart',... 'rows_share']`, `FAILED tests/test_batch.py::test_token_mart_absent_skips_m4_rows_share_zero - AssertionError: assert ['2026-09-03'] == []`(스킵 가드가 없어 M4 스텁이 호출됨), `FAILED tests/test_batch.py::test_m0b_query_decides_skip_share_when_flag_not_given`(같은 원인). `test_marker_rows_share_filled`는 러너 루프가 스텁 3개를 모두 도는 T5 구조상 이미 통과한다(정상 — 가드 도입 후에도 통과해야 함). 마지막 줄 `3 failed, N passed`.

- [ ] **Step 10: 구현 — `app/batch.py` 3개 헌크(import·RUNNERS·스킵 가드)**

T5 산출물의 정확한 행 번호는 T5 실행 결과에 따르므로, 아래 명령으로 앵커 3곳을 찾은 뒤 헌크를 적용한다(앵커 문자열은 T5 아웃라인이 고정한 것).

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && grep -n "from app.steps import\|^RUNNERS: list\[tuple\[str, Callable\]\] = \|for key, fn in RUNNERS" app/batch.py
```

기대: 정확히 3행(import 1, `RUNNERS: list[tuple[str, Callable]] =` 1, `for key, fn in RUNNERS:` 1).

헌크 1 — import(파일 상단):

```python
# before
from app.steps import MART_TABLES, StepError, run_m1, run_m3
# after
from app.steps import MART_TABLES, StepError, run_m1, run_m3, run_m4
```

헌크 2 — 러너 목록(모듈 상수):

```python
# before
# 실행 순서 고정: M1 → M3. T6가 ("rows_share", run_m4), T7이 ("rows_group", run_m2)를 append.
# 각 러너: fn(gate, date) -> {"<key>": int, "warns": list[str]}.
RUNNERS: list[tuple[str, Callable]] = [("rows_mart", run_m1), ("rows_check", run_m3)]
# after  (주석 2행은 T5 그대로 — T7 헌크 2의 앵커)
# 실행 순서 고정: M1 → M3. T6가 ("rows_share", run_m4), T7이 ("rows_group", run_m2)를 append.
# 각 러너: fn(gate, date) -> {"<key>": int, "warns": list[str]}.
RUNNERS: list[tuple[str, Callable]] = [("rows_mart", run_m1), ("rows_check", run_m3), ("rows_share", run_m4)]
```

헌크 3 — `run_batch` 러너 루프(`for key, fn in RUNNERS:` 바로 다음, `result = fn(gate, date)` 앞에 3행 삽입). `skip_share`는 T5가 M0b(`agg_token_service_1d_dist` 행 수 0 또는 `token_mart_present=False`)로 세팅해 두는 지역 변수다.

```text
# before
        for key, fn in RUNNERS:
            result = fn(gate, date)
            rows[key] = int(result[key])
# after
        for key, fn in RUNNERS:
            if key == "rows_share" and skip_share:
                rows[key] = 0        # M0b token_mart_absent — M4 스킵(설계 §6.1), 마커 rows_share=0
                continue
            result = fn(gate, date)
            rows[key] = int(result[key])
```

`rows` 딕셔너리는 T5가 `{"rows_mart": 0, "rows_check": 0, "rows_share": 0}`로 초기화하고 `batch_line(...)`이 `rows_share=` 토큰을 항상 출력하므로 마커 포맷 변경은 없다(스킵 시 `rows_share=0`, 정상 시 M4 반환값).

- [ ] **Step 11: GREEN 확인 — `tests/test_batch.py`**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_batch.py 2>&1 | tail -n 3
```

기대: 마지막 줄 `N passed`(Step 9의 3 failed 전부 통과, T5 기존 테스트 회귀 0).

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python - <<'PY'
import app.batch as b
print([k for k, _ in b.RUNNERS], b.RUNNERS[2][1].__name__)
PY
```

기대 출력: `['rows_mart', 'rows_check', 'rows_share'] run_m4`

- [ ] **Step 12: `tests/test_mart.py` — allocate_shared ↔ M4 all_services 의미 일치 테스트(append → 바로 GREEN)**

M4 `all_services` 모드의 `allocated_cost_krw = C(m) × share`는 T2 `allocate_shared`(`cost-model-spec.md` 5.2 W 가중 안분)와 같은 산식이어야 한다. 아래를 `tests/test_mart.py` 끝에 append한다(T2 구현이 이미 만족하므로 RED 단계 없이 회귀 고정용).

```python


# ============================================================================
# T6 — allocate_shared ↔ M4 all_services 산식 일치(설계 §6.1 share = W(s)/ΣW, 비용 = C(m)×share)
# ============================================================================
def test_allocate_shared_matches_m4_all_services_semantics():
    from app.mart import allocate_shared

    wt = {"A": 76364, "B": 152727, "C": 10909}     # W 가중 토큰(1·in + 0.1·cache_read + 4·out 합)
    total = 240000.0
    out = allocate_shared(total, wt)
    assert set(out) == set(wt)
    assert abs(sum(out.values()) - total) < 0.01
    w_sum = sum(wt.values())
    for s, w in wt.items():
        assert abs(out[s] - total * w / w_sum) < 0.01
    assert allocate_shared(total, {"A": 240000.0}) == {"A": 240000.0}
    assert allocate_shared(total, {}) == {}
```

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_mart.py 2>&1 | tail -n 2
```

기대: 마지막 줄 `N passed`(신규 1건 포함, 실패 0).

- [ ] **Step 13: 전체 테스트 + zero-diff + 공개 레포 점검**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q 2>&1 | tail -n 3
```

기대: 마지막 줄 `N passed`(실패 0. T1~T5 회귀 0 + T6 신규 = test_steps 16 + test_batch 4 + test_mart 1).

```bash
git status --porcelain
```

기대 출력(정확히 5행, 전부 `mart/token-metrics/` 아래 — zero-diff 영역 `collectors/token-usage/**`, `mart/token-usage/**`, `assets/user-org/**`, `tools/verify/invariants.sql`, `.github/workflows/*`, `docs/operations/*` 변경 0):

```
 M mart/token-metrics/app/batch.py
 M mart/token-metrics/app/steps.py
 M mart/token-metrics/tests/test_batch.py
 M mart/token-metrics/tests/test_mart.py
 M mart/token-metrics/tests/test_steps.py
```

```bash
cd /home/mini/github/token-data-pipeline && git diff --name-only | grep -v '^mart/token-metrics/' | wc -l
```

기대 출력: `0`

공개 레포 점검(사내 호스트명·코드명·소유자 이메일 0):

```bash
cd /home/mini/github/token-data-pipeline && git diff -U0 -- mart/token-metrics | grep -E '^\+' | grep -E -i -c '\.(corp|internal)\.[a-z]+[^ ]*svc|@[a-z0-9.-]+\.(com|co\.kr|net)\b|harbor\.[a-z0-9-]+\.(com|co\.kr)' ; echo "exit=$?"
```

기대 출력: `0` 그리고 `exit=1`(grep -c 매치 0 → 종료 코드 1이 정상). 이 태스크의 추가 코드는 DB/테이블명(`{DB_MART}` 등 T1 상수)만 참조하고 클러스터 주소·레지스트리 주소를 문자열로 갖지 않는다. `harbor.example.internal` / `chi-<cluster>.<ns>.svc` 플레이스홀더도 이 태스크에서는 새로 쓰지 않는다.

- [ ] **Step 14: 커밋**

```bash
cd /home/mini/github/token-data-pipeline && git add mart/token-metrics/app/steps.py mart/token-metrics/app/batch.py mart/token-metrics/tests/test_steps.py mart/token-metrics/tests/test_batch.py mart/token-metrics/tests/test_mart.py && git commit -q -F - <<'MSG'
feat(mart-metrics): M4 agg_token_model_share_1d(분모 6모드·외부 API 단가) + M3 stretch 3블록 + batch rows_share (Plan 6c T6)

- app/steps.py: SQL_M4/EXPECTED_SQL_M4/run_m4 — W 가중(1·in+1·cache_creation+0.1·cache_read+4·out) 분모 6모드
  (all_services/provider_reported/token_not_reported/no_provider/provider_ambiguous/external_api),
  external_api는 dim_token_vendor_price 단가로 allocated_cost_krw 직접 계산, no_provider는 0 확정,
  provider_ambiguous는 share/비용 NULL. 키 = usage_svc 토큰 보고 행 ∪ 제공자 후보 self 행(share 0 허용).
  quality_flag 우선순위 partial > no_tco > provider_ambiguous > vendor_price_missing > token_not_reported > normal.
- app/steps.py: M3_BLOCKS_STRETCH += provider_ambiguous / vendor_price_missing / consumer_tokens_exceed_provider
  (build_m3_sql이 자동 UNION ALL — M3 컬럼/마커 변경 없음).
- app/batch.py: RUNNERS 3항(rows_mart, rows_check, rows_share) + M0b token_mart_absent 시 M4 스킵(rows_share=0).
- tests: test_steps 16건(모드 판정·share/비용 산식·GLOBAL 규율·run_m4 시퀀스·EXPECTED 키 합집합·M3 stretch 블록),
  test_batch 4건(러너 순서·스킵 가드·마커 rows_share·M0b 자동 판정), test_mart 1건(allocate_shared ↔ M4 all_services).

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54
MSG
git log --oneline -1
```

기대: `<sha> feat(mart-metrics): M4 agg_token_model_share_1d(분모 6모드·외부 API 단가) + M3 stretch 3블록 + batch rows_share (Plan 6c T6)`

**Self-Review (Task 6)**

설계 해석(설계서가 명시하지 않아 이 태스크가 하나로 고정한 것 — 스테이지 실행 시 설계 소유자 확인 항목):

1. **M4 DDL 컬럼 수 = 14**(`date, model, service, service_group, provider_service, is_provider, denominator_mode, service_wtokens, model_total_wtokens, share, model_cost_krw, allocated_cost_krw, quality_flag, created_by`). 설계 §6.1 표의 컬럼을 Plan 6a DDL(`agg_token_model_share_1d`) 순서대로 옮겼고 INSERT 컬럼 리스트를 명시해 순서 의존을 제거했다. Plan 6a DDL이 컬럼을 추가/삭제했다면 `SQL_M4`의 INSERT 컬럼 리스트와 `test_m4_insert_column_list_matches_ddl_order`이 함께 실패하도록 두었다(테스트가 DDL 파일을 파싱).
2. **C(m)은 같은 배치의 M1 결과**(`agg_token_model_cost_1d_dist` 당일 `has_gpu_rows = 1` 행의 `model_cost_krw`·`quality_flag`, 제공자 행 `service = p`로 조인)에서 읽는다(`_M4_M1C`). 순서 보장은 `RUNNERS` 순서(M1 → M3 → M4)로 한다. M1이 해당 날짜에 제공자 행을 쓰지 않은 모델(`has_m1=0`)은 `model_cost_krw=NULL`, `quality_flag=no_tco`.
3. **행 집합** = `usage_svc`에서 당일 토큰을 보고한 (model, service) 행 ∪ 제공자 후보 self 행 `(m, p, p)`. 제공자가 토큰을 보고하지 않아도 self 행은 존재한다(`service_wtokens=0`, share 0 또는 모드별 NULL). `provider_ambiguous`는 후보 제공자 전부의 self 행을 남기고 share/비용 NULL — 하류에서 "후보가 누구였는지" 조회 가능.
4. **`no_provider`**(GPU 원가는 있으나 제공자 미판정)는 `model_cost_krw = 0`, `allocated_cost_krw = 0`, `share = W(s)/ΣW`(분모는 all_services와 동일)로 고정 — 설계 §6.1 "비용 0 확정, 토큰 점유율은 유지" 해석.
5. **`external_api` 단가**는 `dim_token_vendor_price`(`SUB_EFF_PRICE`, tier='standard')에서 `_M4_VENDOR`가 vendor를 `min(provider) AS vendor`로 1개 고정 후(별칭을 `provider`로 두면 같은 SELECT의 `argMin(…, provider)`가 집계 별칭을 참조해 중첩 집계 오류 — 소스 컬럼명과 다른 별칭 필수) `nullIf(argMin(ifNull(x,-1),provider),-1)` 센티널로 NULL 단가를 보존한다. 단가 4종 중 하나라도 NULL이면 `vendor_price_missing`(allocated_cost_krw는 ClickHouse Nullable 산술로 NULL).
6. **M3 stretch 3블록은 `model` 컬럼을 채운다**(T4 필수 블록 중 model 차원이 있는 블록과 동일 스키마). `provider_ambiguous`/`vendor_price_missing`은 M4의 `denominator_mode`/`quality_flag`를 재조회하지 않고 M4와 같은 CTE 조각(`_M4_PROV`, `_M4_VENDOR`)을 재사용해 **M3가 M4보다 먼저 돌아도** 같은 답을 내도록 했다. `consumer_tokens_exceed_provider`는 `provider_reported` 모드에서 Σ 소비자 W > 제공자 W인 (model, provider) — M4가 `greatest(…, 0.0)`으로 클램프하는 케이스를 M3가 경고로 드러낸다.
7. **`M4_DATE` 지역 상수**(`tests/test_steps.py` T6 섹션)를 두어 T4 `DATE` 상수 정의 여부와 무관하게 동작한다.
8. **batch.py 헌크는 행 번호 대신 앵커 문자열**(`from app.steps import …`, `^RUNNERS = `, `for key, fn in RUNNERS:`)로 위치를 잡는다 — 세 문자열 모두 T5 아웃라인이 고정한 인터페이스. T5의 `test_runners_order_m1_then_m3`는 삭제하고 `test_runners_order_m1_m3_m4`로 대체(T5 본문에 예고됨).
9. **`_WTOK_EXPR`를 GROUP BY 집계 alias 위에 다시 계산**하는 CTE(`wt_total`, `mode`)는 ClickHouse가 SELECT alias를 같은 SELECT 안에서 참조 가능하다는 점(`prefer_column_name_to_alias=0` 기본)에 기댄다. 스테이지 1회 실행에서 `EXPECTED_SQL_M4` 행 수 = INSERT 행 수 대조(`_run_table` verify_count)로 확인한다.

이 태스크가 발견한 앞 태스크 결함(리뷰 라운드 1에서 T3/T4 본문에 반영 완료 — 아래는 이력):

- **T4 `SQL_M3_SUMMARY`에 `GLOBAL IN`이 없어** T3 `test_global_join_and_global_in_only`가 `SQL_*` 전수 검사에서 실패하던 문제 → T3 테스트가 `name.endswith("_SUMMARY")`를 `GLOBAL IN` 필수 검사에서 제외하도록 수정됨(`SQL_M3_SUMMARY`는 단일 테이블 집계라 서브쿼리 IN 자체가 없음; `JOIN` 금지 검사는 그대로 적용). 이 태스크의 `SQL_M4`는 규칙을 충족한다(`GLOBAL LEFT JOIN` 18회, `GLOBAL IN` 포함, `ARRAY JOIN`·`NOT IN (SELECT` 0회).
- **T4 `run_m3` 테스트가 `DATE` 상수를 쓰지만 정의가 없던 문제** → T3 테스트 파일 상단에 `DATE = "2026-09-01"`이 정의돼 T4가 재사용한다.
- **T4 `test_run_m3_sequence_delete_insert_expected_verify_summary`가 `gate.order == ["m3"]`를 단언하던 문제** → T3 FakeGate `order`는 `(op, key)` 튜플이므로 `[("exists", "m3"), ("delete", "m3")]`로 수정됨(M3Gate가 insert/query/verify를 덮어써 두 항목만 기록). 이 태스크의 `test_run_m4_sequence_*`는 튜플 기록 기준으로 작성했다.

체크리스트:

- [x] Files/Interfaces(Consumes/Produces 정확한 시그니처) 명시.
- [x] 모든 스텝이 실패 테스트 → 실패 확인 명령·문구 → 구현(전문) → 통과 확인 → 커밋 순.
- [x] 플레이스홀더 0(금지어 grep 결과 0행).
- [x] 호출 함수 전부 존재: `_run_table`, `SUB_REG`, `SUB_ANCHOR`, `_WTOK_EXPR`, `M3_BLOCKS_STRETCH`, `_m3_select`, `build_m3_sql`, `MART_TABLES`, `DB_*`, `T_M4`(T1/T3/T4), `allocate_shared`(T2), `batch_line`/`compute_coverage`(T2), `run_batch`/`RUNNERS`/`BatchOutcome`(T5), `DENOMINATOR_MODES`(app.mart, T2).
- [x] Python 3.10 호환(match 문·`Self` 미사용), `random` 미사용, 날짜는 `{d:Date}` 바인딩만(KST 규율은 T1 `Config`/T5 `run_batch`가 담당).
- [x] ClickHouse 관용구: `INSERT … SELECT` + CTE, `GLOBAL LEFT JOIN`/`GLOBAL IN`, `argMin`/`groupUniqArray`/`arraySort`/`multiIf`/`toNullable`/`greatest`, join_use_nulls=0 전제 miss 값 처리.
- [x] 커밋 메시지·트레일러 아웃라인 그대로.
- [x] zero-diff 영역 무변경, 사내 호스트명·코드명·소유자 이메일 0.
### Task 7: M2 agg_token_gpu_group_1d(stretch, 할당×24·idle 클램프·identity_gap) + M3 stretch 4블록 + batch 연결 (RUNNERS 4개 완성)

**설계 근거**: 설계 §6.1 305(M2 컬럼·grain `date × service_group × gpu_type`·행 집합 = "그룹에 gpu 행이 있거나 (`unknown` 아닌 할당 행 AND 그룹 내 앵커 서비스 ≥ 1)인 쌍만"·`allocated_gpu_hours = allocated_gpu_count × 24`·idle 클램프 0 + `over_report`·`identity_gap_krw`·`tco_missing`이면 비용 컬럼 NULL·EXPECTED = 동일 키 집합 uniqExact), §6.4 (2)(idle = 할당 − Σ보고, 음수면 0 + `over_report` FAIL, I2 항등)·(7)(그룹 비용은 할당 기준 `allocated × TCO`), 정의서 §3.1(idle ≥ 0)·§3.4(항등식 `group_total = ΣC + test + idle + unattributed ± gap`)·§5.3(idle 0→16 예제)·§8 I1/I2, Plan 6a `mart_metrics_tables.sql` `agg_token_gpu_group_1d_local` DDL **23컬럼**(date, service_group, gpu_type, allocated_gpu_hours, group_total_cost_krw, serving_gpu_hours, standby_gpu_hours, test_gpu_hours, reported_gpu_hours_total, flagged_gpu_hours, model_cost_sum_krw, test_cost_krw, idle_gpu_hours, idle_cost_krw, unattributed_cost_krw, identity_gap_krw, utilization, over_report, equiv_gpu_count, tco_missing, allocation_source, quality_flag, created_by — 아웃라인의 "24컬럼"은 오기, DDL이 정본·`quality_flag` COMMENT `normal | no_allocation | no_tco | over_report | flagged`), Plan 6a `dim_token_gpu_allocation` DDL(`allocated_gpu_count Nullable(Float64)` 플레이스홀더 NULL·철회 0, `source` `manual | quota-sheet | seed`)·T6 stage fixture(`unknown/unknown` NULL 플레이스홀더 행 — `SUB_EFF_ALLOC`의 `HAVING gpu_type != 'unknown'`가 제외), Plan 6a H(마커 필드 고정 — `rows_group` 미포함, 로그만), 마스터 §5.6(M3 detail 비노출 — gpu_type·expect 플래그만).
**읽을 원형**: T3 `app/steps.py`(`DB_FACT`·`DB_MART`·`T_M2`·`CREATED_BY`·`FAIL_PRED`·`SUB_EFF_TCO`·`SUB_EFF_ALLOC`·`SUB_REG`·`SUB_ANCHOR`·`_run_table`·`run_m1`·`SQL_M1`의 `{{d:Date}}`/`GLOBAL LEFT JOIN`/alias 재사용 관용구), T4 `app/steps.py`(`_m3_select` 12컬럼 헤더·`_M3_ANCHORED`·`M3_BLOCKS_CORE`·`M3_BLOCKS_STRETCH`·`build_m3_sql`·`run_m3`), T6 `app/steps.py` 말미(`M3_BLOCKS_STRETCH.extend([...])` 3항 — T7은 그 **뒤**에 4항을 extend), T3/T4/T6 `tests/test_steps.py`(`ddl_columns`·`insert_columns`·`sql_constants`·`FakeGate`·`M3_CORE_NAMES`·`_m3_select_header_aliases`·`M3Gate`·`M4_STRETCH_NAMES`), T5 `app/batch.py`(`RUNNERS`·`_MARKER_ROW_KEYS`·`log`·`run_batch` 러너 루프·`BatchOutcome.rows`), T6 `app/batch.py` 헌크(import `run_m4`·`RUNNERS` 3항·`skip_share` 가드), T5/T6 `tests/test_batch.py`(`FakeGate`·`full_gate`·`MARKER_RE`·`DATE`·`test_runners_order_m1_m3_m4`), T2 `app/mart.py`(`group_overhead`).

**Files:**
- Modify: `mart/token-metrics/app/steps.py` — 말미 append(T6 `M3_BLOCKS_STRETCH.extend` 뒤): M2 섹션(`_M2_GPU_TAIL`·`_M2_ALLOC_TAIL`·`_M2_GPU_KEYS`·`_M2_ALLOC_KEYS`·`_M2_GRP`·`SQL_M2`·`EXPECTED_SQL_M2`·`run_m2`) + M3 stretch 4블록(`_M3_NO_ALLOCATION`·`_M3_SUM_HOURS_OVER_ALLOCATION`·`_M3_GPU_BLOCK_EMPTY_UNEXPECTED`·`_M3_SERVING_BLOCK_EMPTY_UNEXPECTED`) + `M3_BLOCKS_STRETCH.extend([...4항])`
- Modify: `mart/token-metrics/app/batch.py` — import에 `run_m2`, `RUNNERS` 4항, `rows` 초기화에 러너 키 보강, 러너 루프에 비마커 키 로그 (4 hunks; 결정적 패치 스크립트 동봉)
- Modify: `mart/token-metrics/tests/test_steps.py` — T6 stretch 이름 단언이 정확 일치형이면 `[:3]` 접두 일치형으로 치환(가드 스크립트, 0 또는 1회) + T7 섹션 append(M2 SQL 계약 9 + `run_m2` 2 + M3 stretch 5 = 16)
- Modify: `mart/token-metrics/tests/test_batch.py` — T6 `test_runners_order_m1_m3_m4` 제거(→ `test_runners_final_order_four`) + T7 섹션 append(4)
- Modify: `mart/token-metrics/tests/test_mart.py` — `test_group_overhead_reference_matches_m2_semantics` append
- (읽기만) `mart/token-metrics/ddl/company/mart_metrics_tables.sql`(Plan 6a — M2 DDL 컬럼 순서 정본), `mart/token-metrics/ddl/company/dim_token_gpu_allocation.sql`·`mart/token-metrics/ddl/company/seed_dim_token_gpu_allocation.sql`(Plan 6a — `allocated_gpu_count`·`source` 규약), `docs/superpowers/specs/2026-08-31-token-metrics-ingest-design.md`, `docs/cost-model-spec.md`. `collectors/token-usage/**`·`mart/token-usage/**`·`assets/**`·`tools/verify/invariants.sql`·`docs/operations/**`·`docs/monitoring/**`·`.github/workflows/**` 등 zero-diff 대상은 건드리지 않는다.

**Interfaces:**
- Produces:
  - `app.steps.SQL_M2: str` — `INSERT INTO {DB_MART}.agg_token_gpu_group_1d_dist (23컬럼 DDL 순서) WITH grp AS (…), keys AS (<_M2_GPU_KEYS> UNION DISTINCT <_M2_ALLOC_KEYS>) SELECT … FROM keys AS k GLOBAL LEFT JOIN grp AS gp … GLOBAL LEFT JOIN {SUB_EFF_ALLOC} AS al … GLOBAL LEFT JOIN {SUB_EFF_TCO} AS t ON t.gpu_type = k.gpu_type`; `{d:Date}` 서버 바인딩, `GLOBAL LEFT JOIN`/`GLOBAL IN`만, `coalesce`·`agg_token_model_cost_1d`(M1 읽기) 없음.
  - `app.steps.EXPECTED_SQL_M2: str` — `SELECT uniqExact((service_group, gpu_type)) FROM (<_M2_GPU_KEYS> UNION ALL <_M2_ALLOC_KEYS>)`.
  - `app.steps.run_m2(gate, date: str) -> dict` — `{"rows_group": int, "warns": list[str]}` (`_run_table` 시퀀스 exists→delete→insert→expected→verify, dist/local = `{DB_MART}.agg_token_gpu_group_1d_{dist,local}`; 0행 날도 SUCCESS).
  - `app.steps.M3_BLOCKS_STRETCH` 항목 4개 append(순서 고정, T6 3항 뒤): `("no_allocation", …)` WARN · `("sum_hours_over_allocation", …)` FAIL · `("gpu_block_empty_unexpected", …)` WARN · `("serving_block_empty_unexpected", …)` WARN — 최종 7항, `build_m3_sql()` 기본 = CORE 13 + STRETCH 7 = **20블록**(`UNION ALL` 19회). 모두 `_m3_select(...)` 12컬럼, model `''`; 앞 2개는 gpu_type 채움·service `''`, 뒤 2개는 service·source_type 채움.
  - 조각 상수(모듈 private, M3 stretch 2블록과 M2가 공유): `_M2_GPU_TAIL`(gpu fact 앵커 필터 + `GROUP BY g.service_group, g.gpu_type`), `_M2_ALLOC_TAIL`(`SUB_EFF_ALLOC` + 앵커 그룹 필터), `_M2_GPU_KEYS`, `_M2_ALLOC_KEYS`, `_M2_GRP`(시간 5분류 + `gpu_rows`), `_M3_NO_ALLOCATION`, `_M3_SUM_HOURS_OVER_ALLOCATION`, `_M3_GPU_BLOCK_EMPTY_UNEXPECTED`, `_M3_SERVING_BLOCK_EMPTY_UNEXPECTED`.
  - `app.batch.RUNNERS == [("rows_mart", run_m1), ("rows_check", run_m3), ("rows_share", run_m4), ("rows_group", run_m2)]`(최종 4개, M0→M0b→M1→M3→M4→M2); `run_batch`는 `rows_group`를 마커에 싣지 않고 `log.info("M2 rows_group=%d")`만 남기며, `BatchOutcome.rows`에는 실패 시에도 `rows_group` 키가 존재(0 초기화); `skip_share=True`여도 M2는 실행(GPU-only 그룹 집계).
  - 테스트 `tests/test_steps.py`: `test_m2_insert_column_list_matches_ddl_order`, `test_m2_allocated_hours_is_count_times_24`, `test_m2_idle_clamped_with_greatest_zero`, `test_m2_identity_gap_from_loaded_columns`, `test_m2_cost_from_fact_by_gpu_type_not_m1`, `test_m2_excludes_unknown_gpu_type_allocation`, `test_m2_hours_five_way_split_and_fail_pred`, `test_m2_quality_priority_order`, `test_m2_expected_key_tuple`, `test_run_m2_returns_rows_group_from_verify_actual_and_routes_to_m2`, `test_run_m2_zero_rows_day_is_success_and_dup_or_verify_paths`, `test_m3_stretch_seven_names_after_t7`, `test_m3_t7_blocks_follow_core_discipline`, `test_m3_no_allocation_and_over_allocation_predicates`, `test_m3_block_empty_unexpected_pair_uses_registry_expectation`, `test_run_m3_default_includes_t7_stretch_blocks`; `tests/test_batch.py`: `test_runners_final_order_four`, `test_marker_has_no_rows_group_field`, `test_m2_runs_even_when_token_mart_absent`, `test_m2_failure_marks_batch_failure`; `tests/test_mart.py`: `test_group_overhead_reference_matches_m2_semantics`.
- Consumes: `{DB_FACT}.raw_token_metrics_gpu_1d_dist`(당일·앵커 서비스 행 — date, service_group, service, gpu_type, category, gpu_hours, flags), `{DB_FACT}.raw_token_metrics_summary_1d_dist`(`SUB_ANCHOR` 경유 — service_group·gpu_rows·serving_rows·source_type), `{DB_DIM}.dim_token_gpu_allocation_dist`(`SUB_EFF_ALLOC` 경유 — allocated_gpu_count·source), `{DB_DIM}.dim_token_gpu_tco_dist`(`SUB_EFF_TCO` 경유 — tco), `{DB_DIM}.dim_token_metrics_service_dist`(`SUB_REG` 경유 — expect_gpu·expect_serving), `app.steps.{DB_FACT, DB_MART, T_M2, CREATED_BY, FAIL_PRED, SUB_EFF_TCO, SUB_EFF_ALLOC, SUB_REG, SUB_ANCHOR, _M3_ANCHORED, _run_table, _m3_select, M3_BLOCKS_STRETCH, build_m3_sql, run_m3, StepError}`(T3/T4), `app.batch.{RUNNERS, _MARKER_ROW_KEYS, log, run_batch, BatchOutcome}`(T5/T6), `app.mart.group_overhead`(T2), 테스트 헬퍼 `tests/test_steps.py::{ddl_columns, insert_columns, FakeGate, M3Gate, M3_CORE_NAMES, _m3_select_header_aliases, M4_STRETCH_NAMES}`(T3/T4/T6), `tests/test_batch.py::{FakeGate, full_gate, MARKER_RE, DATE}`(T5).

**M2 산식 요약**(`SQL_M2` 바깥 SELECT의 별칭 = 적재 컬럼; 뒤 별칭이 앞 별칭을 재사용 — T3 `SQL_M1`과 같은 관용구. `t.tco`는 `keys.gpu_type`으로 1회 조인한 date 유효 TCO, `al.*`는 `SUB_EFF_ALLOC`, `gp.*`는 `grp` CTE):

| 컬럼 | 식 | NULL 규칙 |
|---|---|---|
| `allocated_gpu_hours` | `al.allocated_gpu_count * 24` | 할당 행 없음(alloc-miss) → NULL |
| `group_total_cost_krw` | `allocated_gpu_hours * t.tco` | 할당 NULL 또는 TCO NULL → NULL |
| `serving/standby/test_gpu_hours` | `sumIf(gpu_hours, category = '<c>' AND NOT FAIL_PRED)` | grp-miss(할당만 있는 키) → 0 |
| `reported_gpu_hours_total` / `flagged_gpu_hours` | `sum(gpu_hours)` / `sumIf(gpu_hours, FAIL_PRED)` | grp-miss → 0 |
| `model_cost_sum_krw` | `(serving_gpu_hours + standby_gpu_hours) * t.tco` | TCO NULL → NULL |
| `test_cost_krw` / `unattributed_cost_krw` | `test_gpu_hours * t.tco` / `flagged_gpu_hours * t.tco` | TCO NULL → NULL |
| `idle_gpu_hours` | `if(isNull(allocated_gpu_hours), NULL, greatest(allocated_gpu_hours - reported_gpu_hours_total, 0))` | I1 클램프; 할당 NULL → NULL |
| `idle_cost_krw` | `idle_gpu_hours * t.tco` | 둘 중 하나 NULL → NULL |
| `identity_gap_krw` | `group_total − model_cost_sum − test_cost − idle_cost − unattributed` | 하나라도 NULL → NULL (I2) |
| `utilization` | `if(isNull(allocated) OR allocated = 0, NULL, reported_total / allocated)` | 0 할당 → NULL |
| `over_report` | `toUInt8(ifNull(reported_gpu_hours_total > allocated_gpu_hours, 0))` | 할당 NULL → 0 |
| `equiv_gpu_count` | `reported_gpu_hours_total / 24` | — |
| `tco_missing` | `toUInt8(isNull(t.tco))` | — |
| `allocation_source` | `al.source` | alloc-miss → `''`(join_use_nulls=0) |
| `quality_flag` | `multiIf(over_report=1,'over_report', tco_missing=1,'no_tco', isNull(allocated),'no_allocation', flagged>0,'flagged', 'normal')` | 우선순위 고정(설계 해석 T7-2) |

- [ ] **Step 1: 실패하는 테스트 — `tests/test_steps.py`(T6 단언 가드 + T7 섹션 append)**

T6가 `M3_BLOCKS_STRETCH` 이름 목록을 **정확 일치**(`== [...]` 3항)로 단언했다면 T7의 4항 append로 그 단언이 깨진다. T6 본문은 `[:3] ==` 접두 일치형으로 쓰여 있지만(0회 치환이 정상), 실행 결과가 다를 수 있으므로 아래 가드를 먼저 돌린다(정확 일치형만 접두 일치형으로 바꾸며, 이미 접두형이면 아무것도 바꾸지 않는다).

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python - <<'PY'
import re, pathlib
p = pathlib.Path("tests/test_steps.py")
s = p.read_text(encoding="utf-8")
pat = re.compile(r"assert \[n for n, _ in steps\.M3_BLOCKS_STRETCH\] == "
                 r"(\[\"provider_ambiguous\", \"vendor_price_missing\", \"consumer_tokens_exceed_provider\"\]|M4_STRETCH_NAMES)")
s2, n = pat.subn(r"assert [n for n, _ in steps.M3_BLOCKS_STRETCH][:3] == \1", s)
assert n in (0, 1), n
p.write_text(s2, encoding="utf-8")
print("t6 exact-equality asserts rewritten:", n)
PY
```

기대 출력: `t6 exact-equality asserts rewritten: 0`(T6가 접두형으로 썼을 때) 또는 `1`. 그 뒤 `grep -n "M3_BLOCKS_STRETCH\] ==" tests/test_steps.py`가 0행이어야 한다(`[:3] ==`만 남음).

이어서 아래 T7 섹션을 `tests/test_steps.py` **끝**에 append한다. T3 `ddl_columns`·`insert_columns`·`FakeGate`, T4 `_m3_select_header_aliases`·`M3Gate`·`M3_CORE_NAMES`, 모듈 상단 `re`·`steps`·`DB_MART`·`DB_FACT` import를 그대로 쓴다(T3~T6 파일 상단에 이미 있음).

```python


# ============================================================================
# T7 — M2 agg_token_gpu_group_1d(할당×24·idle 클램프·identity_gap) + M3 stretch 4블록(20블록 완성)
# ============================================================================

M2_DATE = "2026-09-03"
M3_STRETCH_NAMES_T7 = ["provider_ambiguous", "vendor_price_missing", "consumer_tokens_exceed_provider",
                       "no_allocation", "sum_hours_over_allocation",
                       "gpu_block_empty_unexpected", "serving_block_empty_unexpected"]
T7_GROUP_BLOCKS = {"no_allocation", "sum_hours_over_allocation"}          # 그룹×기종 단위(gpu_type 채움)
T7_ANCHOR_BLOCKS = {"gpu_block_empty_unexpected", "serving_block_empty_unexpected"}   # 서비스 단위


def _m2_outer_select() -> str:
    return steps.SQL_M2[steps.SQL_M2.rindex("\nSELECT\n"):steps.SQL_M2.index("\nFROM keys AS k")]


def test_m2_insert_column_list_matches_ddl_order():
    cols = ddl_columns("agg_token_gpu_group_1d_local")
    assert len(cols) == 23                                   # Plan 6a DDL 정본(설계 §6.1 컬럼 목록 23)
    assert cols[:3] == ["date", "service_group", "gpu_type"] and cols[-1] == "created_by"
    assert insert_columns(steps.SQL_M2) == cols
    aliases = re.findall(r"\bAS (\w+)\s*,?\s*$", _m2_outer_select(), re.M)
    assert aliases == cols
    assert re.search(r"'token-metrics-pipeline'\s+AS created_by", steps.SQL_M2)
    assert steps.SQL_M2.lstrip().startswith(f"INSERT INTO {DB_MART}.agg_token_gpu_group_1d_dist")


def test_m2_allocated_hours_is_count_times_24():
    assert "al.allocated_gpu_count * 24" in steps.SQL_M2
    assert re.search(r"allocated_gpu_count \* 24\s+AS allocated_gpu_hours", steps.SQL_M2)
    assert "allocated_gpu_hours * t.tco" in steps.SQL_M2       # 그룹 총비용 = 할당 × TCO (정의서 3.4)
    assert "reported_gpu_hours_total / 24" in steps.SQL_M2     # equiv_gpu_count


def test_m2_idle_clamped_with_greatest_zero():
    assert "greatest(" in steps.SQL_M2
    assert "- reported_gpu_hours_total, 0)" in steps.SQL_M2
    assert "reported_gpu_hours_total > allocated_gpu_hours" in steps.SQL_M2          # over_report (I1)
    assert re.search(r"toUInt8\(ifNull\(reported_gpu_hours_total > allocated_gpu_hours, 0\)\)\s+AS over_report", steps.SQL_M2)
    assert re.search(r"if\(isNull\(allocated_gpu_hours\), NULL,\s+greatest\(allocated_gpu_hours - reported_gpu_hours_total, 0\)\)\s+AS idle_gpu_hours",
                     steps.SQL_M2)
    assert "idle_gpu_hours * t.tco" in steps.SQL_M2


def test_m2_identity_gap_from_loaded_columns():
    # I2: gap = group_total − model_cost_sum − test_cost − idle_cost − unattributed — 적재되는 별칭끼리 계산
    assert ("group_total_cost_krw - model_cost_sum_krw - test_cost_krw - idle_cost_krw - unattributed_cost_krw"
            in steps.SQL_M2)
    assert "(serving_gpu_hours + standby_gpu_hours) * t.tco" in steps.SQL_M2   # Σ C = (serving+standby)×TCO
    assert "test_gpu_hours * t.tco" in steps.SQL_M2                            # 실험 비용(그룹 귀속)
    assert "flagged_gpu_hours * t.tco" in steps.SQL_M2                         # unattributed(FAIL 행 × TCO)
    assert re.search(r"toUInt8\(isNull\(t\.tco\)\)\s+AS tco_missing", steps.SQL_M2)
    assert "reported_gpu_hours_total / allocated_gpu_hours" in steps.SQL_M2    # utilization
    assert "allocated_gpu_hours = 0, NULL" in steps.SQL_M2                     # 0 할당 → NULL(0 나눗셈 방지)


def test_m2_cost_from_fact_by_gpu_type_not_m1():
    assert "agg_token_model_cost_1d" not in steps.SQL_M2
    assert "agg_token_model_share_1d" not in steps.SQL_M2 and "token_metrics_check_1d" not in steps.SQL_M2
    assert "tco" in steps.SQL_M2
    assert steps.SUB_EFF_TCO in steps.SQL_M2
    assert f"{DB_FACT}.raw_token_metrics_gpu_1d_dist AS g" in steps.SQL_M2
    assert "token_usage_1d" not in steps.SQL_M2                # 토큰 측 무관(GPU-only 테이블)


def test_m2_excludes_unknown_gpu_type_allocation():
    assert "gpu_type != 'unknown'" in steps.SUB_EFF_ALLOC and steps.SUB_EFF_ALLOC in steps.SQL_M2
    # alloc 키 = 앵커 서비스가 있는 그룹만 (설계 §6.1 "그룹 내 서비스 앵커 ≥1")
    assert f"al.service_group GLOBAL IN (SELECT service_group FROM {steps.SUB_ANCHOR})" in steps.SQL_M2
    assert f"g.service GLOBAL IN {steps._M3_ANCHORED}" in steps.SQL_M2


def test_m2_hours_five_way_split_and_fail_pred():
    grp = steps._M2_GRP
    assert grp in steps.SQL_M2
    assert f"sumIf(g.gpu_hours, g.category = 'serving' AND NOT {steps.FAIL_PRED})" in grp
    assert f"sumIf(g.gpu_hours, g.category = 'standby' AND NOT {steps.FAIL_PRED})" in grp
    assert f"sumIf(g.gpu_hours, g.category = 'test' AND NOT {steps.FAIL_PRED})" in grp
    assert "sum(g.gpu_hours)" in grp and "AS reported_gpu_hours_total" in grp   # 플래그 포함 전체
    assert f"sumIf(g.gpu_hours, {steps.FAIL_PRED})" in grp and "AS flagged_gpu_hours" in grp
    assert "GROUP BY g.service_group, g.gpu_type" in grp


def test_m2_quality_priority_order():
    sql = steps.SQL_M2
    order = [sql.index(f"'{f}'") for f in ("over_report", "no_tco", "no_allocation", "flagged", "normal")]
    assert order == sorted(order)
    assert "multiIf(" in sql and "'normal')" in sql
    assert "'partial'" not in sql and "'manual'" not in sql       # M1 전용 플래그 없음


def test_m2_expected_key_tuple():
    assert "uniqExact((service_group, gpu_type))" in steps.EXPECTED_SQL_M2
    assert "\n    UNION ALL\n" in steps.EXPECTED_SQL_M2
    assert "UNION DISTINCT" in steps.SQL_M2
    for frag in (steps._M2_GPU_KEYS, steps._M2_ALLOC_KEYS):
        assert frag in steps.SQL_M2 and frag in steps.EXPECTED_SQL_M2
    assert "{d:Date}" in steps.EXPECTED_SQL_M2 and "GLOBAL IN" in steps.EXPECTED_SQL_M2


def test_run_m2_returns_rows_group_from_verify_actual_and_routes_to_m2():
    gate = FakeGate(exists=True, verify_actual=4, expected_overrides={"m2": 4})
    out = steps.run_m2(gate, M2_DATE)
    assert out == {"rows_group": 4, "warns": []}
    assert gate.order == [("exists", "m2"), ("delete", "m2"), ("insert", "m2"), ("query", "m2"), ("verify", "m2")]
    assert gate.delete_preds == [("m2", "")]
    assert gate.written[0][1] == steps.SQL_M2 and gate.written[0][2] == {"d": M2_DATE}
    assert gate.query_calls[0][1] == steps.EXPECTED_SQL_M2
    assert gate.verify_calls == [("m2", M2_DATE, 4)]


def test_run_m2_zero_rows_day_is_success_and_dup_or_verify_paths():
    empty = FakeGate(exists=False, verify_actual=0, expected_overrides={"m2": 0})
    assert steps.run_m2(empty, M2_DATE) == {"rows_group": 0, "warns": []}   # gpu·할당 모두 없는 날
    assert ("delete", "m2") not in empty.order
    dup = FakeGate(exists=True, verify_actual=5, expected_overrides={"m2": 3})
    out = steps.run_m2(dup, M2_DATE)
    assert out["rows_group"] == 5 and out["warns"] == [f"dup_suspect:{DB_MART}.agg_token_gpu_group_1d_dist"]
    with pytest.raises(steps.StepError):
        steps.run_m2(FakeGate(exists=True, verify_ok=False, verify_actual=1), M2_DATE)


def test_m3_stretch_seven_names_after_t7():
    assert [n for n, _ in steps.M3_BLOCKS_STRETCH] == M3_STRETCH_NAMES_T7
    blocks = steps.M3_BLOCKS_CORE + steps.M3_BLOCKS_STRETCH
    assert len(blocks) == 20
    assert len(set(n for n, _ in blocks)) == 20
    assert steps.build_m3_sql(blocks).count("\nUNION ALL\n") == 19
    assert steps.build_m3_expected(blocks).count("\nUNION ALL\n") == 19
    stretch = dict(steps.M3_BLOCKS_STRETCH)
    assert "'FAIL' AS severity" in stretch["sum_hours_over_allocation"]
    for name in ("no_allocation", "gpu_block_empty_unexpected", "serving_block_empty_unexpected"):
        assert "'WARN' AS severity" in stretch[name], name


def test_m3_t7_blocks_follow_core_discipline():
    stretch = dict(steps.M3_BLOCKS_STRETCH)
    for name in T7_GROUP_BLOCKS | T7_ANCHOR_BLOCKS:
        sql = stretch[name]
        assert sql.startswith("SELECT\n"), name
        assert _m3_select_header_aliases(sql) == list(steps.M3_COLUMNS), name
        assert f"'{name}' AS check_name" in sql, name
        assert "    {d:Date} AS date," in sql, name
        assert "'token-metrics-pipeline' AS created_by" in sql, name
        assert "\nUNION ALL\n" not in sql and "\nUNION DISTINCT\n" not in sql, name
        assert "coalesce(" not in sql.lower() and "SELECT *" not in sql and "%(" not in sql, name
        header = sql.split("\nFROM", 1)[0]
        assert next(ln for ln in header.splitlines() if ln.endswith(" AS model,")).strip() == "'' AS model,", name
        gpu_line = next(ln for ln in header.splitlines() if ln.endswith(" AS gpu_type,")).strip()
        svc_line = next(ln for ln in header.splitlines() if ln.endswith(" AS service,")).strip()
        if name in T7_GROUP_BLOCKS:
            assert gpu_line == "x.gpu_type AS gpu_type," and svc_line == "'' AS service,", name
            assert "x.service_group AS service_group," in sql and "concat('gpu_type=', x.gpu_type) AS detail" in sql, name
            assert steps._M2_GRP in sql and steps.SUB_EFF_ALLOC in sql, name
            assert "toNullable(toFloat64(x.reported_gpu_hours_total)) AS observed" in sql, name
        else:
            assert gpu_line == "'' AS gpu_type," and svc_line == "an.service AS service,", name
            assert "an.service_group AS service_group," in sql and "an.source_type AS source_type," in sql, name
            assert steps.SUB_ANCHOR in sql and steps.SUB_REG in sql, name
            assert "toNullable(toFloat64(1)) AS threshold" in sql, name
        assert "reported_service" not in header, name          # detail/헤더에 응답 원문 없음(§5.6)


def test_m3_no_allocation_and_over_allocation_predicates():
    stretch = dict(steps.M3_BLOCKS_STRETCH)
    no_alloc = stretch["no_allocation"]
    assert "WHERE isNull(al.allocated_gpu_count)" in no_alloc            # = M2 allocated_gpu_hours NULL
    assert "toNullable(toFloat64(0)) AS threshold" in no_alloc
    over = stretch["sum_hours_over_allocation"]
    assert "WHERE x.reported_gpu_hours_total > al.allocated_gpu_count * 24" in over
    assert "toNullable(toFloat64(al.allocated_gpu_count * 24)) AS threshold" in over
    for sql in (no_alloc, over):
        assert "GLOBAL LEFT JOIN" in sql and "al.service_group = x.service_group AND al.gpu_type = x.gpu_type" in sql


def test_m3_block_empty_unexpected_pair_uses_registry_expectation():
    stretch = dict(steps.M3_BLOCKS_STRETCH)
    gpu = stretch["gpu_block_empty_unexpected"]
    assert "WHERE r.expect_gpu = 1 AND an.gpu_rows = 0" in gpu
    assert "'expect_gpu=1' AS detail" in gpu and "toNullable(toFloat64(an.gpu_rows)) AS observed" in gpu
    serving = stretch["serving_block_empty_unexpected"]
    assert "WHERE r.expect_serving = 1 AND an.serving_rows = 0" in serving
    assert "'expect_serving=1' AS detail" in serving and "toNullable(toFloat64(an.serving_rows)) AS observed" in serving
    for sql in (gpu, serving):
        assert f"FROM {steps.SUB_ANCHOR} AS an" in sql
        assert f"GLOBAL LEFT JOIN {steps.SUB_REG} AS r ON r.service = an.service" in sql


def test_run_m3_default_includes_t7_stretch_blocks():
    gate = M3Gate([], rows=1)
    steps.run_m3(gate, M2_DATE)
    inserted_sql = gate.inserted[0][0]
    for name in M3_STRETCH_NAMES_T7:
        assert f"'{name}' AS check_name" in inserted_sql, name
    assert inserted_sql.count("\nUNION ALL\n") == 19
```

- [ ] **Step 2: RED 확인 — `tests/test_steps.py`**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_steps.py 2>&1 | grep -E "^(FAILED|ERROR)|^E   |passed|failed" | head -n 40
```

기대(요지): T7 16개 전부 FAILED, T3~T6 기존 테스트는 통과.
- `test_m2_*` 9개·`test_run_m2_*` 2개: `AttributeError: module 'app.steps' has no attribute 'SQL_M2'. Did you mean: 'SQL_M1'?` / `… has no attribute '_M2_GRP'` / `… 'EXPECTED_SQL_M2'` / `… 'run_m2'. Did you mean: 'run_m1'?`
- `test_m3_stretch_seven_names_after_t7`: `AssertionError: assert ['provider_am...eed_provider'] == ['provider_am...xpected', ...]` + `Right contains 4 more items, first extra item: 'no_allocation'`
- `test_m3_t7_blocks_follow_core_discipline`·`test_m3_no_allocation_and_over_allocation_predicates`: `KeyError: 'no_allocation'`; `test_m3_block_empty_unexpected_pair_uses_registry_expectation`: `KeyError: 'gpu_block_empty_unexpected'`
- `test_run_m3_default_includes_t7_stretch_blocks`: `AssertionError: no_allocation` + `assert "'no_allocation' AS check_name" in 'INSERT INTO mart.token_metrics_check_1d_dist (…'`
- 마지막 줄 `16 failed, N passed`.

- [ ] **Step 3: 구현 — `app/steps.py` 말미 append(M2 섹션 + M3 stretch 4블록)**

append 위치는 T6의 `M3_BLOCKS_STRETCH.extend([...])` 3항 블록 **뒤**(파일 끝). 먼저 앞 상태를 확인한다.

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && grep -n "^M3_BLOCKS_STRETCH.extend\|^_M3_ANCHORED = \|^SUB_EFF_ALLOC = \|^SUB_EFF_TCO = \|^SUB_ANCHOR = \|^SUB_REG = \|^def _run_table\|^def _m3_select\|^T_M2 = " app/steps.py; grep -c "SQL_M2" app/steps.py
```

기대: 앞 grep은 9행(`M3_BLOCKS_STRETCH.extend` 1회 — T6 것), 뒤 `grep -c`는 `0`. 그 다음 아래 내용을 그대로 `app/steps.py` 끝에 append한다(f-string 안의 `{{d:Date}}`는 렌더 후 `{d:Date}` — 서버 바인딩, 클라이언트 포맷 금지).

```python


# ============================================================================
# M2 agg_token_gpu_group_1d — 그룹 귀속·유휴 (설계 §6.1 M2, §6.4 (2)(7); 정의서 3.1/3.3/3.4, I1/I2; Plan 6c T7)
#
# grain: date × service_group × gpu_type (쿼터 보유 단위). 행 = grp 키(앵커 서비스의 gpu 행이 있는
# (그룹, 기종)) ∪ alloc 키(`unknown` 아닌 date 유효 할당 행 AND 그 그룹에 앵커 서비스 ≥ 1) — UNION DISTINCT.
# 비용은 M1을 읽지 않고 fact 시간 × 그 기종 TCO를 outer에서 직접 곱한다(그레인이 기종 단위라
# Σ_model (serving+standby)×TCO = 그룹 (serving+standby)×TCO — M1 합과 같되 M1의 "기종 하나라도 NULL이면
# 모델 C NULL" 규칙과 무관하게 기종별로 닫힌다; 설계 해석 T7-1). TCO NULL이면 Nullable 산술로
# 비용 6컬럼(group_total/model_cost_sum/test_cost/idle_cost/unattributed/identity_gap)이 전부 NULL.
#   allocated_gpu_hours = allocated_gpu_count × 24 (할당 행 없음 → NULL)
#   idle_gpu_hours      = greatest(allocated − reported_total, 0)  (I1 클램프; over_report = reported > allocated)
#   identity_gap_krw    = group_total − model_cost_sum − test_cost − idle_cost − unattributed  (I2, 적재 컬럼끼리 계산)
# 참조 구현 app/mart.py group_overhead()와 같은 규칙 — tests/test_mart.py가 대조한다.
# ============================================================================

# gpu fact(앵커 서비스만) → (그룹, 기종) 집계 — grp CTE와 키 조각이 같은 꼬리를 공유
_M2_GPU_TAIL = f"""FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN {_M3_ANCHORED}
    GROUP BY g.service_group, g.gpu_type"""

# 할당 행(unknown 제외, date 유효 최신) 중 앵커 서비스가 1개 이상인 그룹만
_M2_ALLOC_TAIL = f"""FROM {SUB_EFF_ALLOC} AS al
    WHERE al.service_group GLOBAL IN (SELECT service_group FROM {SUB_ANCHOR})"""

# 키 조각 — SQL_M2의 keys(UNION DISTINCT)와 EXPECTED_SQL_M2(UNION ALL + uniqExact) 공유
_M2_GPU_KEYS = f"""SELECT g.service_group AS service_group, g.gpu_type AS gpu_type
    {_M2_GPU_TAIL}"""
_M2_ALLOC_KEYS = f"""SELECT al.service_group AS service_group, al.gpu_type AS gpu_type
    {_M2_ALLOC_TAIL}"""

# grp — 시간 5분류(그룹 합): serving/standby/test는 비FAIL 행만, reported_total은 플래그 포함 전체,
# flagged는 FAIL 행 전체(카테고리 무관). M3 no_allocation/sum_hours_over_allocation 블록도 같은 조각을 쓴다.
_M2_GRP = f"""SELECT g.service_group                                                  AS service_group,
           g.gpu_type                                                       AS gpu_type,
           sumIf(g.gpu_hours, g.category = 'serving' AND NOT {FAIL_PRED})   AS serving_gpu_hours,
           sumIf(g.gpu_hours, g.category = 'standby' AND NOT {FAIL_PRED})   AS standby_gpu_hours,
           sumIf(g.gpu_hours, g.category = 'test' AND NOT {FAIL_PRED})      AS test_gpu_hours,
           sum(g.gpu_hours)                                                 AS reported_gpu_hours_total,
           sumIf(g.gpu_hours, {FAIL_PRED})                                  AS flagged_gpu_hours,
           count()                                                          AS gpu_rows
    {_M2_GPU_TAIL}"""

SQL_M2 = f"""
INSERT INTO {DB_MART}.{T_M2}_dist
    (date, service_group, gpu_type,
     allocated_gpu_hours, group_total_cost_krw,
     serving_gpu_hours, standby_gpu_hours, test_gpu_hours, reported_gpu_hours_total, flagged_gpu_hours,
     model_cost_sum_krw, test_cost_krw, idle_gpu_hours, idle_cost_krw, unattributed_cost_krw,
     identity_gap_krw, utilization, over_report, equiv_gpu_count, tco_missing,
     allocation_source, quality_flag, created_by)
WITH
    grp AS (
        {_M2_GRP}
    ),
    keys AS (
        {_M2_GPU_KEYS}
        UNION DISTINCT
        {_M2_ALLOC_KEYS}
    )
SELECT
    {{d:Date}}                                                        AS date,
    k.service_group                                                   AS service_group,
    k.gpu_type                                                        AS gpu_type,
    al.allocated_gpu_count * 24                                       AS allocated_gpu_hours,
    allocated_gpu_hours * t.tco                                       AS group_total_cost_krw,
    gp.serving_gpu_hours                                              AS serving_gpu_hours,
    gp.standby_gpu_hours                                              AS standby_gpu_hours,
    gp.test_gpu_hours                                                 AS test_gpu_hours,
    gp.reported_gpu_hours_total                                       AS reported_gpu_hours_total,
    gp.flagged_gpu_hours                                              AS flagged_gpu_hours,
    (serving_gpu_hours + standby_gpu_hours) * t.tco                   AS model_cost_sum_krw,
    test_gpu_hours * t.tco                                            AS test_cost_krw,
    if(isNull(allocated_gpu_hours), NULL,
       greatest(allocated_gpu_hours - reported_gpu_hours_total, 0))   AS idle_gpu_hours,
    idle_gpu_hours * t.tco                                            AS idle_cost_krw,
    flagged_gpu_hours * t.tco                                         AS unattributed_cost_krw,
    group_total_cost_krw - model_cost_sum_krw - test_cost_krw - idle_cost_krw - unattributed_cost_krw
                                                                      AS identity_gap_krw,
    if(isNull(allocated_gpu_hours) OR allocated_gpu_hours = 0, NULL,
       reported_gpu_hours_total / allocated_gpu_hours)                AS utilization,
    toUInt8(ifNull(reported_gpu_hours_total > allocated_gpu_hours, 0)) AS over_report,
    reported_gpu_hours_total / 24                                     AS equiv_gpu_count,
    toUInt8(isNull(t.tco))                                            AS tco_missing,
    al.source                                                         AS allocation_source,
    -- 우선순위 고정(설계 해석 T7-2): over_report > no_tco > no_allocation > flagged > normal
    multiIf(
        over_report = 1,                 'over_report',
        tco_missing = 1,                 'no_tco',
        isNull(allocated_gpu_hours),     'no_allocation',
        flagged_gpu_hours > 0,           'flagged',
        'normal')                                                     AS quality_flag,
    '{CREATED_BY}'                                                    AS created_by
FROM keys AS k
GLOBAL LEFT JOIN grp AS gp ON gp.service_group = k.service_group AND gp.gpu_type = k.gpu_type
GLOBAL LEFT JOIN {SUB_EFF_ALLOC} AS al ON al.service_group = k.service_group AND al.gpu_type = k.gpu_type
GLOBAL LEFT JOIN {SUB_EFF_TCO} AS t ON t.gpu_type = k.gpu_type
"""

EXPECTED_SQL_M2 = f"""
SELECT uniqExact((service_group, gpu_type)) FROM (
    {_M2_GPU_KEYS}
    UNION ALL
    {_M2_ALLOC_KEYS}
)
"""
# ↑ grp(GROUP BY 키 유니크)·eff_alloc(GROUP BY service_group, gpu_type)·eff_tco(GROUP BY gpu_type)는
# 전부 키 유니크라 keys 좌측에 fan-out이 없다 — 적재 행수 = keys의 distinct 키 수.


def run_m2(gate, date: str) -> dict:
    """M2 — mart.agg_token_gpu_group_1d 1테이블. 반환 {"rows_group": actual, "warns": [...]}.
    rows_group는 마커 필드가 아니다(Plan 6a H 고정) — batch.py가 로그 `M2 rows_group=<n>`만 남긴다.
    gpu 행도 할당 행도 없는 날은 0행 적재(expected 0 = actual 0)로 성공 — 절대 FAILURE 아님."""
    warns: list = []
    rows = _run_table(gate, date, f"{DB_MART}.{T_M2}_dist", f"{DB_MART}.{T_M2}_local",
                      SQL_M2, EXPECTED_SQL_M2, warns)
    return {"rows_group": rows, "warns": warns}


# ============================================================================
# M3 stretch — 그룹·앵커 4블록 (설계 §6.1 M3 stretch, §6.4 (2) I1; Plan 6c T7)
#   17·18은 M2와 같은 조각(_M2_GRP·SUB_EFF_ALLOC)을 써서 M2 quality_flag 판정과 문자 단위로 같은 집합을
#   본다(그룹 단위 — service '', gpu_type 채움). 19·20은 앵커(summary) × 레지스트리 기대(expect_*)
#   — 서비스 단위(service 채움, gpu_type ''). 4블록 모두 model ''. detail은 이름·수만(§5.6).
# ============================================================================

# --- 17) no_allocation WARN — gpu 행이 있는 (그룹, 기종)에 date 유효 할당(unknown 제외)이 없거나 NULL
#         (= M2 allocated_gpu_hours NULL·quality_flag no_allocation과 같은 술어 isNull(al.allocated_gpu_count))
_M3_NO_ALLOCATION = _m3_select(
    "no_allocation", "WARN",
    service_group="x.service_group", service="''", gpu_type="x.gpu_type",
    observed="x.reported_gpu_hours_total", threshold="0",
    detail="concat('gpu_type=', x.gpu_type)",
    body=f"""FROM
(
    {_M2_GRP}
) AS x
GLOBAL LEFT JOIN {SUB_EFF_ALLOC} AS al ON al.service_group = x.service_group AND al.gpu_type = x.gpu_type
WHERE isNull(al.allocated_gpu_count)""")

# --- 18) sum_hours_over_allocation FAIL — 보고 합(플래그 포함) > 할당 × 24 (I1 idle < 0 → M2 over_report=1·idle 0 클램프)
_M3_SUM_HOURS_OVER_ALLOCATION = _m3_select(
    "sum_hours_over_allocation", "FAIL",
    service_group="x.service_group", service="''", gpu_type="x.gpu_type",
    observed="x.reported_gpu_hours_total", threshold="al.allocated_gpu_count * 24",
    detail="concat('gpu_type=', x.gpu_type)",
    body=f"""FROM
(
    {_M2_GRP}
) AS x
GLOBAL LEFT JOIN {SUB_EFF_ALLOC} AS al ON al.service_group = x.service_group AND al.gpu_type = x.gpu_type
WHERE x.reported_gpu_hours_total > al.allocated_gpu_count * 24""")

# --- 19) gpu_block_empty_unexpected WARN — 앵커는 있는데 gpu 블록 0행이고 레지스트리가 expect_gpu=1
_M3_GPU_BLOCK_EMPTY_UNEXPECTED = _m3_select(
    "gpu_block_empty_unexpected", "WARN",
    service_group="an.service_group", service="an.service",
    observed="an.gpu_rows", threshold="1", detail="'expect_gpu=1'", source_type="an.source_type",
    body=f"""FROM {SUB_ANCHOR} AS an
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = an.service
WHERE r.expect_gpu = 1 AND an.gpu_rows = 0""")

# --- 20) serving_block_empty_unexpected WARN — 앵커는 있는데 serving 블록 0행이고 레지스트리가 expect_serving=1
_M3_SERVING_BLOCK_EMPTY_UNEXPECTED = _m3_select(
    "serving_block_empty_unexpected", "WARN",
    service_group="an.service_group", service="an.service",
    observed="an.serving_rows", threshold="1", detail="'expect_serving=1'", source_type="an.source_type",
    body=f"""FROM {SUB_ANCHOR} AS an
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = an.service
WHERE r.expect_serving = 1 AND an.serving_rows = 0""")

M3_BLOCKS_STRETCH.extend([
    ("no_allocation", _M3_NO_ALLOCATION),
    ("sum_hours_over_allocation", _M3_SUM_HOURS_OVER_ALLOCATION),
    ("gpu_block_empty_unexpected", _M3_GPU_BLOCK_EMPTY_UNEXPECTED),
    ("serving_block_empty_unexpected", _M3_SERVING_BLOCK_EMPTY_UNEXPECTED),
])
# ↑ 20블록 완성: core 13 + stretch 7(T6 3 + T7 4). run_m3 기본 = M3_BLOCKS_CORE + M3_BLOCKS_STRETCH.
```

`_m3_select`(T4)는 `observed`·`threshold`를 `toNullable(toFloat64(<식>))`로, `date`를 `{d:Date}`로, `created_by`를 `'token-metrics-pipeline'`로 감싸 12컬럼 헤더를 만든다 — 그래서 위 4블록의 관측값 식은 원시 식(`x.reported_gpu_hours_total`, `an.gpu_rows`, `0`, `1`, `al.allocated_gpu_count * 24`)만 넘긴다.

- [ ] **Step 4: GREEN 확인 — `tests/test_steps.py` + SQL 전역 규율**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_steps.py 2>&1 | tail -n 3
```

기대: 마지막 줄 `N passed`(Step 2의 16 failed 전부 통과, T3~T6 기존 테스트 회귀 0 — T3 전역 테스트(`test_all_sql_constants_use_date_binding_and_no_percent_format`·`test_no_coalesce_anywhere_in_sql`·`test_created_by_is_token_metrics_pipeline` 등)가 `SQL_M2`·`EXPECTED_SQL_M2`를 `sql_constants()`로 자동 수집해 `{d:Date}` 바인딩·`{{` 잔재 없음·`coalesce` 없음·`%(` 없음·`created_by` 리터럴을 함께 검사한다).

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python - <<'PY'
import re
import app.steps as s
cols = re.search(r"\(([^)]*)\)\s*\nWITH", s.SQL_M2, re.S).group(1)
print(len([c.strip() for c in cols.replace("\n", " ").split(",")]),
      len(s.M3_BLOCKS_CORE) + len(s.M3_BLOCKS_STRETCH),
      [n for n, _ in s.M3_BLOCKS_STRETCH][3:],
      s.build_m3_sql(s.M3_BLOCKS_CORE + s.M3_BLOCKS_STRETCH).count("\nUNION ALL\n"),
      "{{" in s.SQL_M2, "{d:Date}" in s.SQL_M2 and "{d:Date}" in s.EXPECTED_SQL_M2)
PY
```

기대 출력: `23 20 ['no_allocation', 'sum_hours_over_allocation', 'gpu_block_empty_unexpected', 'serving_block_empty_unexpected'] 19 False True`

- [ ] **Step 5: 실패하는 테스트 — `tests/test_batch.py`(T6 순서 테스트 교체 + T7 섹션 append) · `tests/test_mart.py`(참조 구현 대조 append)**

T6의 `test_runners_order_m1_m3_m4`(`["rows_mart","rows_check","rows_share"]` 정확 일치 단언)는 T7의 4항 완성으로 깨지므로 **함수째 삭제**하고 `test_runners_final_order_four`로 대체한다. 결정적 삭제 스크립트:

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python - <<'PY'
import re, pathlib
p = pathlib.Path("tests/test_batch.py")
s = p.read_text(encoding="utf-8")
pat = re.compile(r"def test_runners_order_m1_m3_m4\(\):\n(?:    .*\n)+\n\n")
s2, n = pat.subn("", s)
assert n == 1, n
p.write_text(s2, encoding="utf-8")
print("removed test_runners_order_m1_m3_m4:", n)
PY
```

기대 출력: `removed test_runners_order_m1_m3_m4: 1`. 이어 아래 T7 섹션을 `tests/test_batch.py` **끝**에 append한다. T5의 `full_gate`·`MARKER_RE`·`Config`·`StepError`·`DB_MART`·`batch`·`steps` import를 그대로 쓴다(파일 상단에 이미 있음). `caplog.at_level("INFO", logger="app.batch")`는 T5 `log = logging.getLogger("app.batch")`를 잡는다(`logging` import 불필요).

```python


# ============================================================================
# T7 — RUNNERS 4개 완성(M1→M3→M4→M2) · rows_group 마커 미포함(로그만) · M2 실패 = FAILURE
# ============================================================================
from app.steps import run_m2  # noqa: E402

T7_DATE = "2026-09-03"


def _stub_four_runners(monkeypatch, rows_group=4, m2_raises=None):
    """RUNNERS를 4개 스텁으로 교체(M1 3행·M3 5행·M4 7행·M2 rows_group행). 반환 = 호출 순서 기록."""
    calls = []

    def m1(gate, date):
        calls.append("rows_mart")
        return {"rows_mart": 3, "warns": []}

    def m3(gate, date):
        calls.append("rows_check")
        return {"rows_check": 5, "warns": []}

    def m4(gate, date):
        calls.append("rows_share")
        return {"rows_share": 7, "warns": []}

    def m2(gate, date):
        calls.append("rows_group")
        if m2_raises is not None:
            raise m2_raises
        return {"rows_group": rows_group, "warns": []}

    monkeypatch.setattr(batch, "RUNNERS", [("rows_mart", m1), ("rows_check", m3),
                                           ("rows_share", m4), ("rows_group", m2)])
    return calls


def test_runners_final_order_four():
    assert [k for k, _ in batch.RUNNERS] == ["rows_mart", "rows_check", "rows_share", "rows_group"]
    assert [fn.__name__ for _, fn in batch.RUNNERS] == ["run_m1", "run_m3", "run_m4", "run_m2"]
    assert batch.RUNNERS[3][1] is run_m2 and batch.RUNNERS[3][1] is steps.run_m2
    assert batch._MARKER_ROW_KEYS == ("rows_mart", "rows_check", "rows_share")   # Plan 6a H 고정
    assert "rows_group" not in batch._MARKER_ROW_KEYS


def test_marker_has_no_rows_group_field(monkeypatch, caplog):
    calls = _stub_four_runners(monkeypatch, rows_group=4)
    with caplog.at_level("INFO", logger="app.batch"):
        out = batch.run_batch(Config(), T7_DATE, gate=full_gate(), token_mart_present=True)
    assert calls == ["rows_mart", "rows_check", "rows_share", "rows_group"]
    m = MARKER_RE.match(out.line)
    assert m, out.line
    assert m.group("status") == "SUCCESS" and out.exit_code == 0
    assert (m.group("rows_mart"), m.group("rows_check"), m.group("rows_share")) == ("3", "5", "7")
    assert "rows_group" not in out.line                       # 마커 필드 고정(Plan 6a H)
    assert out.rows == {"rows_mart": 3, "rows_check": 5, "rows_share": 7, "rows_group": 4}
    assert "M2 rows_group=4" in caplog.text                    # 로그로만 노출


def test_m2_runs_even_when_token_mart_absent(monkeypatch):
    # M0b token_mart_absent는 M4만 스킵 — M2는 GPU-only라 실행된다(설계 §6.1 M0b)
    calls = _stub_four_runners(monkeypatch, rows_group=2)
    out = batch.run_batch(Config(), T7_DATE, gate=full_gate(), token_mart_present=False)
    assert calls == ["rows_mart", "rows_check", "rows_group"]
    assert out.skip_share is True and out.rows["rows_share"] == 0 and out.rows["rows_group"] == 2
    assert "rows_mart=3 rows_check=5 rows_share=0 warn=1" in out.line and "status=SUCCESS" in out.line


def test_m2_failure_marks_batch_failure(monkeypatch, capsys):
    msg = (f"verify_count failed: {DB_MART}.agg_token_gpu_group_1d_dist date={T7_DATE} "
           "written_rows=0 expected=3 actual=1")
    calls = _stub_four_runners(monkeypatch, m2_raises=StepError(msg))
    out = batch.run_batch(Config(), T7_DATE, gate=full_gate(), token_mart_present=True)
    assert calls == ["rows_mart", "rows_check", "rows_share", "rows_group"]   # M2가 마지막 러너
    m = MARKER_RE.match(out.line)
    assert m, out.line
    assert out.exit_code == 1 and m.group("status") == "FAILURE" and m.group("reason") == "verify_count"
    assert (m.group("rows_mart"), m.group("rows_check"), m.group("rows_share")) == ("3", "5", "7")   # 선행 3개 반영
    assert out.rows["rows_group"] == 0
    assert "verify_count failed" in capsys.readouterr().err   # 상세는 stderr(마커 오염 금지)
```

`tests/test_mart.py` 끝에는 아래를 append한다(T2 `group_overhead`는 2부 import 블록에 이미 있음). T2 구현이 이미 이 규칙을 만족하므로 이 테스트는 append 직후 통과한다 — `SQL_M2`의 NULL 규칙·클램프·항등식을 파이썬 참조 구현에 고정하는 회귀 가드다(RED 단계 없음, Self-Review 7).

```python


def test_group_overhead_reference_matches_m2_semantics():
    """Plan 6c T7: SQL_M2 규칙 = group_overhead — allocated×24 입력, idle = greatest(allocated − reported, 0),
    over_report = reported > allocated, gap = group_total − Σ C − test − idle − unattributed (I2)."""
    ok = group_overhead(48.0, 32.0, 24.0, 8.0, 0.0, 0.0, 5000.0)     # 할당 2장×24h, 보고 32h
    assert ok["idle_gpu_hours"] == 16.0 and ok["over_report"] == 0
    assert ok["group_total_cost_krw"] == 240000.0 and ok["model_cost_sum_krw"] == 160000.0
    assert ok["idle_cost_krw"] == 80000.0 and ok["identity_gap_krw"] == 0.0
    assert abs(ok["utilization"] - 32.0 / 48.0) < 1e-12
    over = group_overhead(24.0, 32.0, 24.0, 8.0, 0.0, 0.0, 5000.0)   # 할당 1장×24h < 보고 32h (I1 위반)
    assert over["idle_gpu_hours"] == 0.0 and over["over_report"] == 1
    assert over["identity_gap_krw"] == -40000.0                      # 120000 − 160000 − 0 − 0 − 0
    # SQL_M2와 같은 NULL 규칙: 할당 없음 → idle/utilization/group_total/gap None, 비용 3종은 산출
    no_alloc = group_overhead(None, 32.0, 24.0, 8.0, 0.0, 0.0, 5000.0)
    assert no_alloc["idle_gpu_hours"] is None and no_alloc["over_report"] == 0
    assert no_alloc["group_total_cost_krw"] is None and no_alloc["identity_gap_krw"] is None
    assert no_alloc["model_cost_sum_krw"] == 160000.0
    # TCO 없음 → 비용 6키 전부 None (SQL_M2: Nullable 산술 전파 + tco_missing=1)
    no_tco = group_overhead(48.0, 32.0, 24.0, 8.0, 0.0, 0.0, None)
    for key in ("group_total_cost_krw", "model_cost_sum_krw", "test_cost_krw",
                "idle_cost_krw", "unattributed_cost_krw", "identity_gap_krw"):
        assert no_tco[key] is None, key
    assert no_tco["idle_gpu_hours"] == 16.0
```

- [ ] **Step 6: RED 확인 — `tests/test_batch.py` · `tests/test_mart.py`**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_batch.py tests/test_mart.py 2>&1 | grep -E "^(FAILED|ERROR)|^E   |passed|failed" | head -n 20
```

기대(요지): T7 batch 4개 중 3개 FAILED, 나머지는 통과.
- `test_runners_final_order_four`: `AssertionError: assert ['rows_mart',... 'rows_share'] == ['rows_mart',... 'rows_group']` + `Right contains one more item: 'rows_group'`
- `test_marker_has_no_rows_group_field`: `AssertionError: assert 'M2 rows_group=4' in ''`(스텁 4개는 T5 루프가 모두 돌지만 로그 분기가 없음)
- `test_m2_failure_marks_batch_failure`: `KeyError: 'rows_group'`(`out.rows`에 러너 키 초기화가 없음; stderr에는 `ERROR in run_batch(date=2026-09-03): StepError: verify_count failed: …`가 이미 찍힌다)
- `test_m2_runs_even_when_token_mart_absent`는 T5 루프 + T6 `skip_share` 가드 구조상 **이미 통과**한다(정상 — M2에 스킵 가드를 달지 않았다는 회귀 가드로 남긴다). `test_group_overhead_reference_matches_m2_semantics`도 즉시 통과(Step 5 설명).
- 마지막 줄 `3 failed, N passed`.

- [ ] **Step 7: 구현 — `app/batch.py` 4개 헌크(import·RUNNERS·rows 초기화·비마커 키 로그)**

앞 상태 확인(앵커 4곳; T6 실행 결과에 따라 표기가 조금 달라도 아래 패치 스크립트가 흡수한다):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && grep -n "^from app.steps import\|^RUNNERS\|^_MARKER_ROW_KEYS\|rows = {k: 0 for k in _MARKER_ROW_KEYS}\|rows\[key\] = \|^log = " app/batch.py
```

기대: 6행 안팎(import 1, `RUNNERS` 대입 1, `_MARKER_ROW_KEYS = ("rows_mart", "rows_check", "rows_share")` 1, `rows = {k: 0 …}` 1, `rows[key] = …` 2 — T6 스킵 가드의 `rows[key] = 0`과 러너 결과 반영 1, `log = logging.getLogger("app.batch")` 1).

헌크 1 — import(파일 상단, T6가 `run_m4`를 추가한 행):

```python
# before
from app.steps import MART_TABLES, StepError, run_m1, run_m3, run_m4
# after
from app.steps import MART_TABLES, StepError, run_m1, run_m2, run_m3, run_m4
```

헌크 2 — 러너 목록(모듈 상수; T5 주석 `# 실행 순서 고정: M1 → M3. T6가 …` 1행을 교체, `# 각 러너: …` 주석은 유지):

```python
# before
# 실행 순서 고정: M1 → M3. T6가 ("rows_share", run_m4), T7이 ("rows_group", run_m2)를 append.
# 각 러너: fn(gate, date) -> {"<key>": int, "warns": list[str]}.
RUNNERS: list[tuple[str, Callable]] = [("rows_mart", run_m1), ("rows_check", run_m3), ("rows_share", run_m4)]
# after
# 각 러너: fn(gate, date) -> {"<key>": int, "warns": list[str]}.
# 실행 순서 고정(설계 §6.1 §4.0 변이 순서 M1·M3·M4·M2): M1 → M3 → M4 → M2. RUNNERS 4개 완성(Plan 6c T7).
RUNNERS: list[tuple[str, Callable]] = [("rows_mart", run_m1), ("rows_check", run_m3),
                                       ("rows_share", run_m4), ("rows_group", run_m2)]
```

`_MARKER_ROW_KEYS = ("rows_mart", "rows_check", "rows_share")`는 **변경하지 않는다**(마커 필드 고정 — Plan 6a H).

헌크 3 — `run_batch` 지역 `rows` 초기화 직후 1행 삽입(실패·SIGTERM 경로에서도 `BatchOutcome.rows["rows_group"]`가 존재하도록):

```text
# before
    rows = {k: 0 for k in _MARKER_ROW_KEYS}
    skip_share = False
# after
    rows = {k: 0 for k in _MARKER_ROW_KEYS}
    rows.update({k: 0 for k, _ in RUNNERS})   # 마커 밖 러너 키(rows_group)도 0 초기화 — 실패 시에도 존재(T7)
    skip_share = False
```

헌크 4 — 러너 루프, 결과 반영 직후 2행 삽입(T6 스킵 가드는 그대로):

```text
# before
        for key, fn in RUNNERS:
            if key == "rows_share" and skip_share:
                rows[key] = 0        # M0b token_mart_absent — M4 스킵(설계 §6.1), 마커 rows_share=0
                continue
            result = fn(gate, date)
            rows[key] = int(result[key])
            for w in result["warns"]:
                _warn(warns, _normalize_warn(w))
# after
        for key, fn in RUNNERS:
            if key == "rows_share" and skip_share:
                rows[key] = 0        # M0b token_mart_absent — M4 스킵(설계 §6.1), 마커 rows_share=0
                continue
            result = fn(gate, date)
            rows[key] = int(result[key])
            if key not in _MARKER_ROW_KEYS:
                log.info("M2 %s=%d", key, rows[key])   # rows_group — 마커 미포함(Plan 6a H), 로그만
            for w in result["warns"]:
                _warn(warns, _normalize_warn(w))
```

`_line(...)`(마커 포맷터)는 `_MARKER_ROW_KEYS`만 읽으므로 `rows["rows_group"]`가 있어도 마커 문자열은 변하지 않는다. 위 4헌크를 한 번에 적용하는 결정적 스크립트(앵커마다 `assert` — T6 실행 결과가 `RUNNERS = [...]`(타입 주석 없음)·`r = fn(gate, date)`/`rows[key] = r[key]` 표기였어도 같은 결과):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python - <<'PY'
import re, pathlib
p = pathlib.Path("app/batch.py")
s = p.read_text(encoding="utf-8")

# 헌크 1 — import: run_m2 추가(이름 집합으로 재구성 — T6가 run_m4를 어떤 순서로 넣었든 동일 결과)
m = re.search(r"^from app\.steps import ([^\n]+)$", s, re.M)
assert m, "import anchor"
names = [n.strip() for n in m.group(1).split(",")]
for n in ("run_m2", "run_m4"):
    if n not in names:
        names.append(n)
fixed = [n for n in names if not n.startswith("run_m")] + sorted(n for n in names if n.startswith("run_m"))
s = s[:m.start()] + "from app.steps import " + ", ".join(fixed) + s[m.end():]

# 헌크 2 — RUNNERS 4항: T5 주석 1행(`# 실행 순서 고정: M1 → M3. …`) 교체 + 대입 교체(T5 타입 주석 유무 무관)
s, n = re.subn(r"^# 실행 순서 고정[^\n]*\n", "", s, count=1, flags=re.M)
assert n in (0, 1), "order comment"
pat = re.compile(r"^RUNNERS(: list\[tuple\[str, Callable\]\])? = \[.*?\]\n", re.M | re.S)
new_runners = ("# 실행 순서 고정(설계 §6.1 §4.0 변이 순서 M1·M3·M4·M2): M1 → M3 → M4 → M2. RUNNERS 4개 완성(Plan 6c T7).\n"
               "RUNNERS: list[tuple[str, Callable]] = [(\"rows_mart\", run_m1), (\"rows_check\", run_m3),\n"
               "                                       (\"rows_share\", run_m4), (\"rows_group\", run_m2)]\n")
s, n = pat.subn(new_runners, s, count=1)
assert n == 1, "RUNNERS anchor"

# 헌크 3 — rows 초기화 직후: 러너 키(rows_group) 0 초기화
old = "    rows = {k: 0 for k in _MARKER_ROW_KEYS}\n"
assert s.count(old) == 1, "rows init anchor"
s = s.replace(old, old + "    rows.update({k: 0 for k, _ in RUNNERS})   # 마커 밖 러너 키(rows_group)도 0 초기화 — 실패 시에도 존재(T7)\n")

# 헌크 4 — 러너 루프: 결과 반영 직후 비마커 키 로그(T5 `int(result[key])` / T6 표기 `r[key]` 모두 수용)
pat = re.compile(r"^(?P<ind>[ ]+)rows\[key\] = (?:int\()?(?:result|r)\[key\]\)?[ ]*\n", re.M)
ms = list(pat.finditer(s))
assert len(ms) == 1, "loop anchor"
ind = ms[0].group("ind")
s = s[:ms[0].end()] + (f"{ind}if key not in _MARKER_ROW_KEYS:\n"
                       f"{ind}    log.info(\"M2 %s=%d\", key, rows[key])   # rows_group — 마커 미포함(Plan 6a H), 로그만\n") + s[ms[0].end():]
p.write_text(s, encoding="utf-8")
print("batch.py patched: import, RUNNERS(4), rows.update, log.info")
PY
```

기대 출력: `batch.py patched: import, RUNNERS(4), rows.update, log.info`. 적용 후 확인:

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python - <<'PY'
import app.batch as b
print([k for k, _ in b.RUNNERS], [fn.__name__ for _, fn in b.RUNNERS], b._MARKER_ROW_KEYS)
PY
```

기대 출력: `['rows_mart', 'rows_check', 'rows_share', 'rows_group'] ['run_m1', 'run_m3', 'run_m4', 'run_m2'] ('rows_mart', 'rows_check', 'rows_share')`

- [ ] **Step 8: GREEN 확인 — 전체 스위트 + 구문/3.10 호환 + zero-diff 게이트**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q 2>&1 | tail -n 3
```

기대: 마지막 줄 `N passed`(Step 2의 16개 + Step 6의 3개 전부 통과, T1~T6 기존 테스트 회귀 0; T7이 더한 테스트는 총 21개 = steps 16 + batch 4 + mart 1).

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python - <<'PY'
import ast, pathlib
for f in ("app/steps.py", "app/batch.py", "tests/test_steps.py", "tests/test_batch.py", "tests/test_mart.py"):
    src = pathlib.Path(f).read_text(encoding="utf-8")
    ast.parse(src, f)
    for bad in ("StrEnum", "match ", "tomllib", "datetime.UTC", "import random"):
        assert bad not in src, (f, bad)
print("ast ok / py310 ok")
PY
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python - <<'PY'
import app.steps as s
sql = s.build_m3_sql(s.M3_BLOCKS_CORE + s.M3_BLOCKS_STRETCH)
assert sql.count("\nUNION ALL\n") == 19 and "{d:Date}" in sql and "{{" not in sql
for name in ("no_allocation", "sum_hours_over_allocation", "gpu_block_empty_unexpected", "serving_block_empty_unexpected"):
    assert f"'{name}' AS check_name" in sql
assert "harbor" not in s.SQL_M2 and "@" not in s.SQL_M2       # 공개 레포 경계 — 사내 주소·이메일 0
print("m3 20 blocks ok / public-repo boundary ok")
PY
```

기대 출력: `ast ok / py310 ok` / `m3 20 blocks ok / public-repo boundary ok`.

zero-diff 게이트(설계 §7 — 토큰 mart·수집기·assets·검증 SQL·운영 문서·워크플로 무변경):

```bash
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-mart.yml .github/workflows/test-collector.yml
cd /home/mini/github/token-data-pipeline && git status --short mart/token-metrics
```

기대: 첫 명령 출력 **없음**(zero-diff). 둘째 명령은 정확히 5행 — ` M mart/token-metrics/app/steps.py`, ` M mart/token-metrics/app/batch.py`, ` M mart/token-metrics/tests/test_steps.py`, ` M mart/token-metrics/tests/test_batch.py`, ` M mart/token-metrics/tests/test_mart.py`(다른 파일이 보이면 커밋 전에 되돌린다).

- [ ] **Step 9: 커밋**

```bash
cd /home/mini/github/token-data-pipeline && git add mart/token-metrics/app/steps.py mart/token-metrics/app/batch.py mart/token-metrics/tests/test_steps.py mart/token-metrics/tests/test_batch.py mart/token-metrics/tests/test_mart.py && git commit -m "feat(mart-metrics): M2 agg_token_gpu_group_1d(할당×24·idle 클램프·정체성 gap) + M3 stretch 4블록 — 20블록 완성 (Plan 6c T7)" -m "- app/steps.py: SQL_M2/EXPECTED_SQL_M2/run_m2 — grain date×service_group×gpu_type, 행 = gpu 행 ∪ (unknown 아닌 할당 × 앵커 그룹), allocated=count×24, idle=greatest(allocated−reported,0)+over_report, gap=group_total−ΣC−test−idle−unattributed, TCO NULL→비용 6컬럼 NULL, quality over_report>no_tco>no_allocation>flagged>normal.
- app/steps.py: M3 stretch no_allocation(WARN)·sum_hours_over_allocation(FAIL)·gpu_block_empty_unexpected(WARN)·serving_block_empty_unexpected(WARN) — core 13 + stretch 7 = 20블록.
- app/batch.py: RUNNERS 4개 완성(rows_mart, rows_check, rows_share, rows_group); rows_group는 마커 미포함(Plan 6a H), log.info만.
- tests: test_steps 16 · test_batch 4(test_runners_order_m1_m3_m4 → test_runners_final_order_four) · test_mart 1." -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>" -m "Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

기대: `[main <sha>] feat(mart-metrics): M2 agg_token_gpu_group_1d(할당×24·idle 클램프·정체성 gap) + M3 stretch 4블록 — 20블록 완성 (Plan 6c T7)` · `5 files changed`.

**Self-Review (T7 설계 해석 — 실행자가 재판단하지 말 것)**:
1. **T7-1 비용 계산 위치**: 아웃라인은 `grp` CTE 안에서 `sumIf(gpu_hours * t.tco, …)`로 `model_cost_sum_krw`·`test_cost_krw`를 만들고 `tco_missing = max(isNull(t.tco))`로 잡지만, 그레인이 `(service_group, gpu_type)`이라 TCO는 행마다 하나뿐이다. 그래서 `SQL_M2`는 TCO를 `keys.gpu_type`으로 **outer에서 1회** 조인하고 `(serving + standby) × t.tco`·`test × t.tco`·`flagged × t.tco`로 곱한다 — 값은 아웃라인 식과 항등이고, TCO NULL이면 Nullable 산술 전파로 `group_total/model_cost_sum/test_cost/idle_cost/unattributed/identity_gap` 6컬럼이 전부 NULL이 된다(아웃라인의 `if(tco_missing = 1, NULL, …)` 래핑과 같은 결과, 래핑 없이). M1의 `agg_token_model_cost_1d`는 읽지 않는다(`test_m2_cost_from_fact_by_gpu_type_not_m1`) — M1은 "기종 하나라도 TCO NULL이면 모델 C NULL"(설계 §6.4 (1) 부분 합 금지)이지만 M2는 기종별로 닫히므로 **`model_cost_sum_krw`는 Σ M1 C와 같지 않을 수 있다** — TCO 결손 기종에 걸친 모델의 TCO 있는 기종 시간이 M2에는 포함되고 M1 C에는(NULL이라) 빠진다. 그룹의 모든 기종에 TCO가 있을 때만 `Σ_gpu_type model_cost_sum_krw = Σ_model M1 C`. 이 편차는 의도적(I2는 기종별 항등이 목적)이며 모듈 README "비용 모델 요약"과 footer 조립 검증 6에 명시한다; 그 경우 M1 C가 NULL이라 M1 기준 총비용 패널(T11)에는 `no_tco`로 드러난다.
2. **T7-2 quality_flag 우선순위**: DDL COMMENT는 5값만 나열하고 우선순위를 말하지 않는다. `over_report > no_tco > no_allocation > flagged > normal`로 고정(정합성 FAIL이 가장 먼저, 비용 계산 불가 다음, 정보성 순). `over_report=1`이면 `allocated`가 있으므로 `no_allocation`과 겹치지 않고, `no_tco`와 `over_report`가 겹치면 `over_report`(FAIL 근거)가 이긴다.
3. **T7-3 컬럼 수 23**: 아웃라인 "24컬럼"은 오기 — Plan 6a `mart_metrics_tables.sql` DDL(및 그 파일의 "23컬럼" 자체 주석)이 정본. `test_m2_insert_column_list_matches_ddl_order`가 `ddl_columns()`로 DDL을 파싱해 INSERT 컬럼 목록·outer SELECT 별칭 순서와 3중 대조한다.
4. **T7-4 그룹 키 = fact `g.service_group`**: gpu fact 행의 `service_group`(수집기가 레지스트리로 정규화해 적재한 값)을 그대로 GROUP BY 키로 쓴다. 레지스트리 재조인은 하지 않는다(앵커 필터 `g.service GLOBAL IN (SELECT service FROM SUB_ANCHOR)`만 — T4 `_M3_ANCHORED` 재사용).
5. **T7-5 할당 NULL 행**: `dim_token_gpu_allocation.allocated_gpu_count`가 NULL인 행(플레이스홀더)은 `SUB_EFF_ALLOC`의 `nullIf(argMax(ifNull(…, -1)), -1)`로 NULL이 되고, `SQL_M2`에서는 `allocated_gpu_hours = NULL × 24 = NULL` → `no_allocation`; 할당 행 자체가 없는 키도 조인 miss로 NULL → 같은 판정. M3 `no_allocation` 블록의 `WHERE isNull(al.allocated_gpu_count)`가 두 경우를 모두 잡아 M2 `quality_flag='no_allocation'` 집합과 문자 단위로 일치한다(단, alloc-only 키는 gpu 행이 없으므로 M3 대상이 아님 — M3는 `_M2_GRP` 기반). `allocation_source`는 조인 miss 시 join_use_nulls=0으로 `''`(DDL COMMENT "할당 행 없으면 빈 문자열") — 아웃라인의 `ifNull(al.source, '')`와 같은 값이며 `ifNull` 없이 동작하므로 쓰지 않았다(전역 `coalesce` 금지 테스트와 무관하지만 일관성 유지).
6. **T7-6 `rows.update`**: `BatchOutcome.rows`가 실패·SIGTERM 경로에서도 `rows_group` 키를 갖도록 `RUNNERS` 키 전체를 0으로 보강한다(`test_m2_failure_marks_batch_failure`). `_MARKER_ROW_KEYS`·`_line` 미변경 → 마커 문자열 불변(Plan 6a H).
7. **T7-7 로그 분기 일반화**: `if key not in _MARKER_ROW_KEYS: log.info("M2 %s=%d", key, rows[key])` — 현재 비마커 키는 `rows_group` 하나라 로그는 정확히 `M2 rows_group=<n>`(아웃라인 문구). 러너 키로 분기하므로 batch.py에 M2 이름을 하드코딩한 조건문이 없다.
8. **T7-8 `test_mart.py` 테스트는 특성화(characterization)**: T2 `group_overhead`가 이미 같은 규칙이라 append 즉시 통과한다. 목적은 `SQL_M2`의 NULL 규칙·클램프·항등식을 파이썬 참조 구현에 고정해 두 구현이 서로 어긋나면 드러나게 하는 것(정의서 §5.3 idle 0→16 예제와 같은 수치).
9. **T7-9 `skip_share`와 M2**: M0b `token_mart_absent`는 토큰 mart 부재라 M4만 스킵하고, M2는 GPU fact·할당·TCO만 읽으므로 실행한다(`test_m2_runs_even_when_token_mart_absent`). 실행 순서 M1→M3→M4→M2는 설계 §4.0 변이 순서(`plan_mutations`가 이미 `MART_TABLES = (T_M1, T_M3, T_M4, T_M2)` 순).
10. **T7-10 EXPECTED = 키 집합**: `grp`·`SUB_EFF_ALLOC`·`SUB_EFF_TCO`가 모두 키 유니크(GROUP BY)라 `keys` 좌측 fan-out이 없고, INSERT 행수 = `keys`의 distinct 키 수 = `EXPECTED_SQL_M2`의 `uniqExact((service_group, gpu_type))`(UNION ALL 위 uniqExact — T3/T6과 같은 관용구). gpu 행·할당 행 모두 없는 날은 0=0으로 SUCCESS.
11. **T7-11 T6 산출물 흡수**: T6가 `M3_BLOCKS_STRETCH` 이름 단언을 정확 일치형으로 썼거나 `RUNNERS`를 타입 주석 없이/루프를 `r = fn(...)` 표기로 적용했더라도, Step 1 가드 스크립트와 Step 7 패치 스크립트가 앵커 정규식으로 흡수한다(각 앵커 `assert`로 0/1회 보장). T6 `test_runners_order_m1_m3_m4`는 T7에서 삭제·대체(T6 본문에 예고된 갱신).
12. **하드 룰 점검**: zero-diff 대상 무변경(Step 8 게이트) · 사내 주소/코드명/이메일 0(`SQL_M2`에 `harbor`·`@` 부재 확인) · 테이블/컬럼/플래그 이름은 DDL·설계 §6.1 그대로 · `{d:Date}` 서버 바인딩·`GLOBAL LEFT JOIN`/`GLOBAL IN`만·`coalesce` 없음 · Python 3.10 호환(Step 8 검사) · `random` 미사용 · 날짜는 문자열 `YYYY-MM-DD`로만 다룸(KST 규율은 T5 `main`이 담당).
### Task 8: 배포 — Dockerfile/build.sh/k8s CronJob token-mart-metrics(20 10 * * *)/install.sh DESCRIBE 프리플라이트/tools/rerun.py(--chunk-days 7·10:50 창)/release-images-metrics.yml

**설계 근거**: 설계 §6.1 295(CronJob `token-mart-metrics` — `"20 10 * * *"`, `timeZone: Asia/Seoul`, Forbid, `startingDeadlineSeconds: 1800`, `activeDeadlineSeconds: 1800`, 컨테이너/이미지 `token-mart-metrics`, Secret `token-mart-metrics-ch-secret`(CH_USER=mart, `CH_DB_FACT/CH_DB_DIM/CH_DB_MART` + 토큰 측 읽기 전용 `CH_DB_TOKEN_MART/CH_DB_TOKEN_DIM`), env `MART_METRICS_MAX_MUTATIONS_PER_RUN=64`, `imagePullSecrets: registry-pull-secret`), §6.1 297(읽기 계약 3테이블/13컬럼 — install.sh `DESCRIBE`, 불일치 시 설치 중단), §6.3 314(rerun: CRONJOB `token-mart-metrics`, `--chunk-days` 기본 7, 창 **10:50 이후** + 활성 Job 0), §4.0 뮤테이션 장부 119-131(mart-metrics rerun 날짜당 ≤4, `MART_METRICS_MAX_MUTATIONS_PER_RUN` 기본 64 = 4×16 ↔ 청크 7일×4 = 28), §7.5 361-371(독립 이미지 2개·`registry-pull-secret`은 **없을 때만** 생성·`release-images-metrics.yml` 분리·기존 3워크플로 무수정·격리 검증 시 `CH_DB_TOKEN_*`은 운영 DB), Plan 6a ddl/README "적용 순서"(`install.sh` → `apply_sql mart_metrics_tables.sql`, `accounts.sql`은 admin) + "뮤테이션"(창 10:50·활성 Job 0).
**읽을 원형**(digest 번호): §11 `mart/token-usage/build.sh:1-64`·`Dockerfile:1-10`, §12 k8s 5파일(`base/cronjob.yaml:1-44`, `base/kustomization.yaml:1-4`, `overlays/{stage,company,company-verify}/kustomization.yaml`), §10 `install.sh:1-226`(usage 32, company-verify 55-58, Secret 119-154, chi-* 탐색 160-166, `apply_sql()` 170-176, DDL_DIR 181-183, set image/env 211-218), §8 `tools/rerun.py:1-165`(`kubectl()`·`build_job_spec`·`wait_job`·`main`), §9 `tests/test_rerun.py:1-60`(importlib 로딩·`cronjob_obj()`), §25.2 `release-images.yml:1-52`. 전부 **클론 후 델타** — 원형 파일은 무수정(zero-diff).

**Files:**
- Create: `mart/token-metrics/Dockerfile`, `mart/token-metrics/build.sh`(chmod +x)
- Create: `mart/token-metrics/k8s/base/cronjob.yaml`, `mart/token-metrics/k8s/base/kustomization.yaml`, `mart/token-metrics/k8s/overlays/stage/kustomization.yaml`, `mart/token-metrics/k8s/overlays/company/kustomization.yaml`, `mart/token-metrics/k8s/overlays/company-verify/kustomization.yaml`
- Create: `mart/token-metrics/install.sh`(chmod +x)
- Create: `mart/token-metrics/tools/rerun.py`
- Modify (additive; **부재 시 Create**): `.github/workflows/release-images-metrics.yml` — Plan 6b가 먼저 만들었으면 `paths` 1줄 + matrix 1항목 추가, 없으면 2항목(collectors/token-metrics·mart/token-metrics)으로 신규 생성. 기존 `.github/workflows/release-images.yml`은 무수정.
- 존재 확인만(T1 산출): `mart/token-metrics/requirements.txt`, `requirements-dev.txt`, `conftest.py` — Dockerfile이 `requirements.txt`를 COPY 하므로 부재 시 T1로 되돌아간다.
- Test: `mart/token-metrics/tests/test_rerun.py`, `mart/token-metrics/tests/test_install_contract.py`

**Interfaces:**
- Consumes: T1 `app.preflight.READ_CONTRACT: dict[str, tuple[str, ...]]`(키 `"{DB_TOKEN_MART}.token_usage_1d"`·`"{DB_TOKEN_MART}.agg_token_service_1d"`·`"{DB_TOKEN_DIM}.dim_token_service"`, env 미설정 시 `mart.`/`gpu_data.` 접두) — `tests/test_install_contract.py`가 install.sh 배열과 대조; T5 `app.batch.main(argv)`의 CLI `--from YYYY-MM-DD --to YYYY-MM-DD`(inclusive) — 컨테이너 `ENTRYPOINT ["python", "-m", "app.batch"]` 뒤에 `args`로 붙는다; T1 `app.config.Config`의 env 이름(`CH_HOST CH_PORT CH_USER CH_PASSWORD CH_CLUSTER INSERT_QUORUM MART_METRICS_MAX_MUTATIONS_PER_RUN`)과 `app.ch`의 `CH_DB_FACT CH_DB_DIM CH_DB_MART CH_DB_TOKEN_MART CH_DB_TOKEN_DIM`; Plan 6a `mart/token-metrics/ddl/{company,stage,company-verify}/mart_metrics_tables.sql`(install.sh `apply_sql` 대상), `ddl/*/accounts.sql`(admin 수동 — 적용하지 않음).
- Produces:
  - 이미지 `token-mart-metrics`(`build.sh [--registry R] [--tag T] <stage|company>`, 태그 기본 git sha7, stage 레지스트리 `ghcr.io/yoonsungnam`), **`ENTRYPOINT ["python", "-m", "app.batch"]`** — 이미지 스모크는 `docker run --rm token-mart-metrics:ci --help`(T10 CI `image` 잡이 이 형태를 쓴다; `python -m app.batch`를 다시 붙이면 argparse 오류).
  - CronJob `token-mart-metrics`(container `token-mart-metrics`, image `token-mart-metrics:latest`, `schedule: "20 10 * * *"`, `timeZone: Asia/Seoul`, `concurrencyPolicy: Forbid`, `startingDeadlineSeconds: 1800`, `activeDeadlineSeconds: 1800`, `backoffLimit: 1`, `restartPolicy: Never`, history 3/3, requests 100m/256Mi·limits 1/1Gi, `imagePullSecrets: registry-pull-secret`, `envFrom secretRef token-mart-metrics-ch-secret`); overlays stage(`newName: ghcr.io/yoonsungnam/token-mart-metrics`)·company(주석만)·company-verify(`nameSuffix: -verify` → CronJob `token-mart-metrics-verify`, secretRef `token-mart-metrics-ch-secret-verify`).
  - Secret `token-mart-metrics-ch-secret[-verify]` 키 **11개 항상 존재**: `CH_HOST CH_PORT CH_USER CH_PASSWORD CH_CLUSTER CH_DB_FACT CH_DB_DIM CH_DB_MART CH_DB_TOKEN_MART CH_DB_TOKEN_DIM MART_METRICS_MAX_MUTATIONS_PER_RUN` (+ company/company-verify에서만 `INSERT_QUORUM=auto`). `EXPECTED_LATE_SERVICES`·`--target-db` 류 없음. CH_HOST는 Secret 키(원형의 `kubectl set env` 대신 — 정적 env 금지 주석과 정합).
  - install.sh: 인자 `[--registry R] [--tag T] [--context C] [-n|--namespace NS] <stage|company|company-verify>`(환경은 위치 인자 또는 `--overlay <env>` — T11 배포 문서의 `install.sh --overlay stage …` 표기와 호환); 단계 `[1/6] registry-pull-secret(없을 때만)` → `[2/6] Secret` → `[3/6] 읽기 계약 프리플라이트` → `[4/6] DDL` → `[5/6] kustomize apply` → `[6/6] set image`; bash 배열 `READ_CONTRACT`(13항목 `"<db>.<table>_dist:<column>"`, db 접두는 `${CH_DB_TOKEN_MART}`/`${CH_DB_TOKEN_DIM}` 변수); 실패 라인 `PREFLIGHT FAIL read_contract missing=<t.c,...>` + `exit 3`; 성공 라인 `PREFLIGHT OK read_contract tables=3 columns=13`.
  - `tools/rerun.py`(모듈 상수·순수 함수 — `tests/test_rerun.py`가 importlib로 로드): `CRONJOB = "token-mart-metrics"`, `NAMESPACE_DEFAULT = "monitoring"`, `WINDOW_HHMM = (10, 50)`, `CHUNK_DAYS_DEFAULT = 7`, `CHUNK_DAYS_MAX = 16`, `ACTIVE_JOB_PREFIX = "token-mart-"`, `DEADLINE_PER_CHUNK_S = 1800`, `TIMEOUT_RANGE_S = 7200`, `KST`; `chunk_ranges(from_d: date, to_d: date, chunk_days: int) -> list[tuple[date, date]]`, `window_ok(now: datetime, force: bool = False) -> bool`, `active_mart_jobs(kubectl_json: dict) -> int`, `build_batch_command(from_d, to_d) -> list[str]`(= `["--from", "<from>", "--to", "<to>"]`), `range_deadline_s(n_days: int) -> int`(= `min(1800 × ceil(n_days/7), 7200)`), `job_name(cronjob: str, from_d: date, to_d: date, epoch: int) -> str`, `build_job_spec(cronjob_obj, name, args, active_deadline_s=None) -> dict`(containers[0].**args** override — command는 ENTRYPOINT 유지), `wait_job(context, namespace, job_name, timeout_s) -> bool`, `build_arg_parser()`, `main(argv=None) -> int`(exit 0 성공 / 1 Job 실패 / 2 사용법·창·활성 Job 거부). CLI: `python3 mart/token-metrics/tools/rerun.py --context C [-n NS] [--cronjob token-mart-metrics[-verify]] [--from D --to D] [--chunk-days 7] [--force]` — `--chain*`/`--service`/`--replace`/`--push-vm` 없음(체인 종단). 거부 메시지 `RERUN REFUSED window (>=10:50 KST) — use --force`, `RERUN REFUSED active_jobs=<n> (token-mart-* running)`.
  - `.github/workflows/release-images-metrics.yml` matrix 항목 `{context: mart/token-metrics, image: token-mart-metrics}` → `ghcr.io/yoonsungnam/token-mart-metrics:{latest,sha7}`.
  - 설계 해석(리뷰어 확인 항목): (1) rerun은 `kubectl create job --overrides`가 아니라 원형의 `build_job_spec` + `kubectl apply -f -`로 청크 Job을 만든다(`kubectl create job`에는 `--overrides` 플래그가 없다); Dockerfile을 `ENTRYPOINT`로 바꿔 `args`만 override 한다. (2) `range_deadline_s`의 "일수 비례"는 청크(≤7일)당 1800s로 해석 — 원형(일당 1800s)을 그대로 두면 7일 청크가 3.5h가 되어 CronJob 창과 무관해진다. (3) 창(10:50) 게이트는 수동 1회 모드에도 적용(일일 실행과의 겹침 차단이 목적), `--force`는 창만 무시하고 활성 Job 게이트는 무시 불가. (4) `CH_HOST`를 Secret 키로 넣고 `[6/6]`에서는 이미지만 주입한다(원형의 정적 env 주입 제거 — cronjob.yaml 주석 "정적 env 주입 없음"과 정합). (5) `chunk_ranges`는 마지막 청크를 `to_d`에서 자른다(8/1..8/17, 7 → 7/7/3일).

- [ ] **Step 1: T1 산출물 존재 확인 + Dockerfile 작성** — 원형 `mart/token-usage/Dockerfile:1-10`(digest §11) 클론, 델타 = `CMD` → `ENTRYPOINT`(rerun의 `args` override·CI 스모크 `--help` 계약) + 주석.

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics
ls requirements.txt requirements-dev.txt conftest.py app/batch.py app/preflight.py
# 기대: 5개 파일 전부 출력 (T1·T5 산출물). 하나라도 "No such file"이면 T1/T5로 되돌아간다.
```

`mart/token-metrics/Dockerfile`:

```dockerfile
# 설계 §7.5 독립 이미지 token-mart-metrics — mart/token-usage/Dockerfile 클론 (python:3.12-slim,
# requirements 선복사 캐시). BASE_IMAGE는 company 빌드에서 Harbor proxy로 치환된다 (build.sh --registry 경로).
ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
# ENTRYPOINT(원형은 CMD): rerun.py가 containers[0].args = ["--from", D, "--to", D]만 덮는다 (설계 §6.3).
# 스모크는 `docker run --rm token-mart-metrics:ci --help` — 뒤에 `python -m app.batch`를 다시 붙이지 말 것.
# mart는 endpoints ConfigMap 불요 — dim_token_metrics_service/앵커가 게이트 기준 (설계 §6.1 M0)
ENTRYPOINT ["python", "-m", "app.batch"]
```

- [ ] **Step 2: build.sh 작성** — 원형 `mart/token-usage/build.sh:1-64` 클론, 델타 = `IMAGE_NAME`·usage 경로·다음 단계 안내 3곳.

`mart/token-metrics/build.sh`:

```bash
#!/usr/bin/env bash
# token-mart-metrics 이미지 빌드/푸시 (설계 §7.5 — mart/token-usage/build.sh 클론, 이미지명만 델타)
#
# 사용법:
#   ./mart/token-metrics/build.sh [--registry <registry>] [--tag <tag>] <stage|company>
#
#   stage:   REGISTRY 기본 ghcr.io/yoonsungnam
#   company: --registry 필수 (사내 Harbor) — BASE_IMAGE를 Harbor proxy로 치환
#   태그 기본: git short SHA (git 밖이면 latest)
set -euo pipefail
SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
cd "$(dirname "${SCRIPT_PATH}")"

IMAGE_NAME="token-mart-metrics"
REGISTRY=""
TAG=""
ENV=""

usage() {
  grep '^# ' "${SCRIPT_PATH}" | head -8; exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry) REGISTRY="$2"; shift 2 ;;
    --tag)      TAG="$2"; shift 2 ;;
    stage|company) ENV="$1"; shift ;;
    *) echo "[ERROR] unknown arg: $1"; usage ;;
  esac
done
[[ -n "${ENV}" ]] || usage

if [[ -z "${REGISTRY}" ]]; then
  case "${ENV}" in
    stage)   REGISTRY="ghcr.io/yoonsungnam" ;;
    company) echo "[ERROR] company 환경에서는 --registry 옵션이 필수입니다."; usage ;;
  esac
fi

if [[ -z "${TAG}" ]]; then
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    TAG="$(git rev-parse --short HEAD)"
  else
    TAG="latest"
  fi
fi

BUILD_ARGS=()
if [[ "${ENV}" == "company" ]]; then
  # 사내망은 docker hub 직접 pull 불가 — Harbor pull-through proxy 경유 (동료 레포 관례; 주소는 --registry 인자로만)
  BUILD_ARGS+=(--build-arg "BASE_IMAGE=${REGISTRY%%/*}/proxy-docker-registry-1.docker.io/python:3.12-slim")
fi

docker buildx build --platform linux/amd64 "${BUILD_ARGS[@]}" \
  -t "${IMAGE_NAME}:${TAG}" . --load

FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}:${TAG}"
docker tag "${IMAGE_NAME}:${TAG}" "${FULL_IMAGE}"
docker push "${FULL_IMAGE}"

echo ""
echo "[OK] pushed ${FULL_IMAGE}"
echo "다음 단계:"
echo "  ./mart/token-metrics/install.sh --registry ${REGISTRY} --tag ${TAG} ${ENV}"
```

- [ ] **Step 3: Dockerfile/build.sh 검증** — 로컬에 docker 없음(CI가 빌드). 문법·계약만 확인.

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics
chmod +x build.sh
bash -n build.sh && echo "build.sh syntax OK"
./build.sh; echo "exit=$?"
# 기대: usage 6줄(`^# ` 주석 — 빈 `#` 행 2개는 제외, head -8 상한 미달) 출력 후 exit=1
./build.sh company; echo "exit=$?"
# 기대: "[ERROR] company 환경에서는 --registry 옵션이 필수입니다." + usage, exit=1
grep -c '^ENTRYPOINT \["python", "-m", "app.batch"\]$' Dockerfile
# 기대: 1
grep -c '^CMD' Dockerfile
# 기대: 0
grep -n "IMAGE_NAME=" build.sh
# 기대: IMAGE_NAME="token-mart-metrics"
```

- [ ] **Step 4: k8s base + overlays 작성** — 원형 digest §12 5파일 클론, 델타 = 이름·schedule·startingDeadlineSeconds·Secret·주석(계약 수치는 Global Constraints "CronJob 계약 수치" 그대로).

`mart/token-metrics/k8s/base/cronjob.yaml`:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: token-mart-metrics
spec:
  # 매일 10:20 KST (설계 §6.1) — 6b 수집기 마지막 슬롯 09:05 + 적재 마감 후, 토큰 mart(04:00) 이후.
  # 산식과 연동된 값 — 단독 수정 금지 (rerun 창 10:50 = 이 실행의 activeDeadline 1800s 종료 후)
  schedule: "20 10 * * *"
  timeZone: Asia/Seoul
  # §4.0 no-op DELETE 스킵 규칙의 전제: 단일 작성자 (경합 금지)
  concurrencyPolicy: Forbid
  # 컨트롤러 일시 중단 등으로 10:20을 놓쳐도 30분 내에는 따라잡는다 (설계 §6.1 — 10:50 rerun 창과 비중첩)
  startingDeadlineSeconds: 1800
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      backoffLimit: 1
      # 설계 §6.1 — 서버사이드 SQL 경량, 30분 타임아웃. rerun.py range_deadline_s 산식(1800 × 청크 수)과 연동 — 단독 수정 금지
      activeDeadlineSeconds: 1800
      template:
        spec:
          # Never: 실패 시 새 파드로 1회 재시도(backoffLimit) — 파드 로그가 실행 단위와 1:1 → BATCH_RESULT 1줄 소비 (§5.6)
          restartPolicy: Never
          imagePullSecrets:
            - name: registry-pull-secret
          containers:
            - name: token-mart-metrics
              image: token-mart-metrics:latest
              imagePullPolicy: Always
              # 이미지 ENTRYPOINT = python -m app.batch; rerun.py는 args만 override (설계 §6.3)
              envFrom:
                - secretRef:
                    name: token-mart-metrics-ch-secret
              # 앱 env(CH_*, CH_DB_* 5종, MART_METRICS_MAX_MUTATIONS_PER_RUN)는 전부 envFrom(Secret) 경유 —
              # 정적 env는 Secret 값을 덮어쓰므로 금지 (install.sh도 set env를 쓰지 않는다)
              resources:
                requests:
                  cpu: 100m
                  memory: 256Mi
                limits:
                  cpu: "1"
                  # limits 없는 배포 금지 (토큰 mart v1.6 OOM 실경험 상속)
                  memory: 1Gi
```

`mart/token-metrics/k8s/base/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - cronjob.yaml
```

`mart/token-metrics/k8s/overlays/stage/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
# namespace는 매니페스트에 고정하지 않는다 — install.sh의 -n ${NAMESPACE}(기본 monitoring)에 일원화
# (고정 시 --namespace 옵션과 apply -k가 충돌)
resources:
  - ../../base
images:
  - name: token-mart-metrics
    newName: ghcr.io/yoonsungnam/token-mart-metrics
    # 실제 태그는 install.sh가 kubectl set image로 덮는다 (build.sh 태그와 일치)
    newTag: latest
```

`mart/token-metrics/k8s/overlays/company/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
# namespace 고정 없음 — install.sh -n 일원화 (stage overlay와 동일 사유)
resources:
  - ../../base
# 이미지 주소는 install.sh가 --registry/--tag로 kubectl set image 주입 (사내 Harbor 주소 커밋 금지 — 설계 §7.2)
```

`mart/token-metrics/k8s/overlays/company-verify/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
# namespace 고정 없음 — install.sh -n 일원화 (company overlay와 동일 사유)
# 격리 검증 전용(선택 — 설계 §7.5): company와 동일 이미지·리소스, CronJob/Secret 이름만 -verify 접미로
# 분리해 production 리소스(token-mart-metrics)와 공존시킨다. 토큰 측 읽기(CH_DB_TOKEN_*)는 Secret 값으로 운영 DB 유지.
nameSuffix: -verify
resources:
  - ../../base
patches:
  - target:
      kind: CronJob
      name: token-mart-metrics
    patch: |-
      - op: replace
        path: /spec/jobTemplate/spec/template/spec/containers/0/envFrom/0/secretRef/name
        value: token-mart-metrics-ch-secret-verify
# 이미지 주소는 install.sh가 --registry/--tag로 kubectl set image 주입 (company overlay와 동일)
```

- [ ] **Step 5: 매니페스트 렌더 검증** — kubectl은 로컬에 있음(클러스터 접근 불필요).

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics
OUT="$(mktemp -d)"
for o in stage company company-verify; do kubectl kustomize "k8s/overlays/${o}" > "${OUT}/mm-${o}.yaml" && echo "render ${o} OK"; done
# 기대: render stage OK / render company OK / render company-verify OK
for f in "${OUT}"/mm-stage.yaml "${OUT}"/mm-company.yaml "${OUT}"/mm-company-verify.yaml; do
  grep -q 'schedule: 20 10 \* \* \*' "$f" && grep -q 'timeZone: Asia/Seoul' "$f" \
  && grep -q 'concurrencyPolicy: Forbid' "$f" && grep -q 'startingDeadlineSeconds: 1800' "$f" \
  && grep -q 'activeDeadlineSeconds: 1800' "$f" && grep -q 'backoffLimit: 1' "$f" \
  && grep -q 'memory: 1Gi' "$f" && grep -q 'memory: 256Mi' "$f" \
  && grep -q 'name: registry-pull-secret' "$f" && ! grep -q 'EXPECTED_LATE_SERVICES' "$f" && echo "contract $f OK"
done
# 기대: contract … OK 3줄
grep -c 'ghcr.io/yoonsungnam/token-mart-metrics:latest' "${OUT}/mm-stage.yaml"            # 기대: 1
grep -c 'name: token-mart-metrics-ch-secret$' "${OUT}/mm-company.yaml"                     # 기대: 1
grep -c 'name: token-mart-metrics-verify$' "${OUT}/mm-company-verify.yaml"                 # 기대: 1
grep -c 'name: token-mart-metrics-ch-secret-verify$' "${OUT}/mm-company-verify.yaml"       # 기대: 1
```

- [ ] **Step 6: install.sh 계약 테스트 작성(RED)** — install.sh의 bash 배열 `READ_CONTRACT`가 T1 `app.preflight.READ_CONTRACT`와 **같은 13컬럼**임을 텍스트 파싱으로 단언(설계 §6.1 "3테이블/13컬럼 — 그 외 의존 없음"의 단일 정본화), 단계 순서 `[3/6] < [4/6]`, Secret 키 11개, cronjob.yaml 계약 grep, kustomize 렌더(kubectl 있을 때만).

`mart/token-metrics/tests/test_install_contract.py`:

```python
"""install.sh / k8s 매니페스트 계약 테스트 (Plan 6c T8).

install.sh는 bash라 단위 실행이 불가 — 텍스트 파싱으로 (1) 읽기 계약 배열이 app/preflight.py와
동일한지, (2) 프리플라이트가 DDL 적용 전에 오는지(설계 §6.1 "불일치 시 설치 중단"), (3) Secret 키
목록(설계 §6.1 — 11개, EXPECTED_LATE_SERVICES 없음), (4) CronJob 계약 수치(설계 §6.1)를 고정한다.
"""
from __future__ import annotations

import pathlib
import re
import shutil
import subprocess

import pytest

from app.preflight import READ_CONTRACT

ROOT = pathlib.Path(__file__).resolve().parents[1]
INSTALL = ROOT / "install.sh"
CRONJOB_YAML = ROOT / "k8s" / "base" / "cronjob.yaml"
STAGE_KUST = ROOT / "k8s" / "overlays" / "stage" / "kustomization.yaml"
VERIFY_KUST = ROOT / "k8s" / "overlays" / "company-verify" / "kustomization.yaml"

SECRET_KEYS = ("CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_CLUSTER",
               "CH_DB_FACT", "CH_DB_DIM", "CH_DB_MART", "CH_DB_TOKEN_MART", "CH_DB_TOKEN_DIM",
               "MART_METRICS_MAX_MUTATIONS_PER_RUN")


def _install_text() -> str:
    return INSTALL.read_text(encoding="utf-8")


def _install_contract() -> dict[str, list[str]]:
    """install.sh의 READ_CONTRACT=( "db.table_dist:col" … ) → {"db.table": [col, …]} (선언 순서 유지)."""
    m = re.search(r"^READ_CONTRACT=\((.*?)^\)", _install_text(), re.S | re.M)
    assert m, "install.sh에 READ_CONTRACT=( … ) 배열이 없다"
    entries = re.findall(r'"([^"]+)"', m.group(1))
    out: dict[str, list[str]] = {}
    for entry in entries:
        entry = entry.replace("${CH_DB_TOKEN_MART}", "mart").replace("${CH_DB_TOKEN_DIM}", "gpu_data")
        table, col = entry.split(":")
        assert table.endswith("_dist"), f"프리플라이트는 _dist 테이블을 DESCRIBE 한다: {entry}"
        out.setdefault(table[: -len("_dist")], []).append(col)
    return out


def test_install_read_contract_equals_preflight():
    got = _install_contract()
    assert got == {k: list(v) for k, v in READ_CONTRACT.items()}
    assert sum(len(v) for v in got.values()) == 13
    assert len(got) == 3


def test_install_steps_six_and_preflight_before_ddl():
    text = _install_text()
    idx = [text.index(f'"[{k}/6]') for k in range(1, 7)]
    assert idx == sorted(idx), "단계 [1/6]..[6/6]가 순서대로 나타나야 한다"
    assert text.index('"[3/6]') < text.index('"[4/6]')                  # 프리플라이트 → DDL
    assert "PREFLIGHT FAIL read_contract missing=" in text
    assert re.search(r"^\s*exit 3\s*$", text, re.M), "프리플라이트 실패는 exit 3"
    assert "DESCRIBE TABLE" in text
    assert "mart_metrics_tables.sql" in text
    assert re.search(r"\bset env\b", text) is None                      # 정적 env 주입 없음 (CH_HOST도 Secret 키)


def test_install_secret_keys_eleven_and_no_expected_late():
    text = _install_text()
    found = set(re.findall(r'--from-literal="([A-Z_]+)=', text))
    assert set(SECRET_KEYS) <= found, sorted(set(SECRET_KEYS) - found)
    assert "INSERT_QUORUM" in found                                       # company/company-verify 조건부
    assert "EXPECTED_LATE_SERVICES" not in found
    assert "EXPECTED_LATE_SERVICES" not in text
    assert "target-db" not in text


def test_install_pull_secret_created_only_when_absent():
    text = _install_text()
    start = text.index('"[1/6]')
    end = text.index('"[2/6]')
    block = text[start:end]
    assert "create secret docker-registry" in block
    assert "갱신" not in block, "registry-pull-secret은 없을 때만 생성 — 갱신 프롬프트 금지 (설계 §7.5)"


def test_install_usage_range_ends_before_set_euo():
    lines = _install_text().splitlines()
    m = re.search(r"sed -n '2,(\d+)p'", lines[[i for i, l in enumerate(lines) if l.startswith("usage()")][0]])
    assert m, "usage()는 sed -n '2,Np' 형식"
    last = int(m.group(1))
    assert all(lines[i].startswith("#") for i in range(1, last))        # 2..N 행은 전부 주석
    assert lines[last].startswith("set -euo pipefail")                    # N+1 행(0-based last)이 set -euo


def test_cronjob_yaml_contract():
    text = CRONJOB_YAML.read_text(encoding="utf-8")
    for needle in ('name: token-mart-metrics', 'schedule: "20 10 * * *"', "timeZone: Asia/Seoul",
                   "concurrencyPolicy: Forbid", "startingDeadlineSeconds: 1800",
                   "activeDeadlineSeconds: 1800", "backoffLimit: 1", "restartPolicy: Never",
                   "successfulJobsHistoryLimit: 3", "failedJobsHistoryLimit: 3",
                   "name: registry-pull-secret", "name: token-mart-metrics-ch-secret",
                   "image: token-mart-metrics:latest", "memory: 256Mi", "memory: 1Gi", "cpu: 100m"):
        assert needle in text, needle
    assert "EXPECTED_LATE_SERVICES" not in text
    assert "token-mart-daily" not in text                                 # 원형 이름 잔재 금지
    assert "ghcr.io/yoonsungnam/token-mart-metrics" in STAGE_KUST.read_text(encoding="utf-8")
    verify = VERIFY_KUST.read_text(encoding="utf-8")
    assert "nameSuffix: -verify" in verify
    assert "value: token-mart-metrics-ch-secret-verify" in verify


@pytest.mark.skipif(shutil.which("kubectl") is None, reason="kubectl 없음 — CI manifests 잡이 대신 검증")
def test_kustomize_overlays_render_contract():
    for overlay, needles in (
        ("stage", ("ghcr.io/yoonsungnam/token-mart-metrics:latest", "name: token-mart-metrics-ch-secret\n")),
        ("company", ("image: token-mart-metrics:latest", "name: token-mart-metrics-ch-secret\n")),
        ("company-verify", ("name: token-mart-metrics-verify\n", "name: token-mart-metrics-ch-secret-verify\n")),
    ):
        out = subprocess.run(["kubectl", "kustomize", str(ROOT / "k8s" / "overlays" / overlay)],
                             check=True, capture_output=True, text=True).stdout
        assert "schedule: 20 10 * * *" in out, overlay
        assert "startingDeadlineSeconds: 1800" in out and "activeDeadlineSeconds: 1800" in out, overlay
        for needle in needles:
            assert needle in out, (overlay, needle)
```

실행(RED):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics
python -m pytest -q tests/test_install_contract.py 2>&1 | tail -n 4
```

기대: `test_cronjob_yaml_contract`·`test_kustomize_overlays_render_contract` 2개 PASS(Step 4 산출), 나머지 5개 `FileNotFoundError: … install.sh` FAIL → `5 failed, 2 passed` (kubectl 부재 환경은 `5 failed, 1 passed, 1 skipped`).


- [ ] **Step 7: install.sh 작성 — 6단계·DESCRIBE 프리플라이트(exit 3)·11키 Secret**

`mart/token-usage/install.sh`(226행)를 클론하되 다음 델타를 반영한다(설계 §6.1/§7.5 — 자세한 근거는 아래 "설계 해석").

| 항목 | 원형(token-usage) | token-metrics(이 Task) |
|---|---|---|
| 이름 3종 | `token-mart-daily`/`token-mart-ch-secret` | `IMAGE_NAME`·`CRONJOB_NAME`=`token-mart-metrics`, `SECRET_NAME`=`token-mart-metrics-ch-secret` (verify는 `-verify` 접미) |
| 단계 수 | 5단계 | **6단계** — `[3/6]` 읽기 계약 프리플라이트가 DDL 앞에 삽입 |
| Secret 키 | 8키 + `EXPECTED_LATE_SERVICES` | **11키 항상**(`CH_HOST` 포함) + company/-verify만 `INSERT_QUORUM=auto`; 지연 서비스 허용 목록 env 없음 |
| `[6/6]` | `set image` + 정적 env 주입 | `set image`만 — `CH_HOST`가 Secret 키이므로 정적 env 주입 없음 |
| chi-* 파드 탐색 | DDL 단계 내부 | `[1/6]` 앞 프리앰블(`ch_pod`·`ch_host`를 `[2/6]`·`[3/6]`·`[4/6]`이 공유) |
| pull secret | 존재 시 갱신 프롬프트 | **없을 때만 생성**, 있으면 손대지 않음(네임스페이스 공유 Secret — 설계 §7.5) |
| `--target-db` | (없음 — 원형에도 없음) | 없음 — 격리 검증은 `CH_DB_*` Secret 값으로만 분기(Step 6 테스트가 부재를 고정) |
| 헤더 | 2–14행 주석 | 2–21행 주석, 22행 `set -euo pipefail`, `usage()`는 `sed -n '2,21p'` (Step 6 테스트가 경계 검증) |
| 환경 인자 | 위치 인자만 | 위치 인자 **또는** `--overlay <env>` (T11 배포 문서 표기 `install.sh --overlay stage …`와 호환) |

`install.sh` 본문에는 "set env"라는 문구가 주석에도 있으면 안 된다(Step 6 `test_install_steps_six_and_preflight_before_ddl`가 `\bset env\b` 부재를 단언). `mart/token-metrics/install.sh` 전문(원형과 동일한 부분도 생략 없이 적는다):

```bash
#!/usr/bin/env bash
# token-mart-metrics 배치 설치 (설계 §7.5 "새 코드만 새로 배포" — mart/token-usage/install.sh 클론)
#
# 사용법:
#   ./mart/token-metrics/install.sh [--registry <registry>] [--tag <tag>] \
#     [--context <kube-context>] [-n|--namespace <ns>] <stage|company|company-verify>
#   (환경은 위치 인자 또는 --overlay <stage|company|company-verify> — 배포 문서(T11) 표기와 동일)
#
#   stage:           context 기본 homelab, registry 기본 ghcr.io/yoonsungnam
#   company:         --context/--registry 필수 (사내 Harbor 주소는 인자로만 — 커밋 금지)
#   company-verify:  격리 검증(선택 — 설계 §7.5). Secret/CronJob 이름 -verify 접미, DDL은 ddl/company-verify/,
#                    CH_DB_FACT/DIM/MART = token_verify_*; CH_DB_TOKEN_MART/CH_DB_TOKEN_DIM은 운영 DB(mart/gpu_data)
#                    유지(토큰 측 읽기 — 운영 GRANT 의존, Plan 6a ddl/README).
#
# 수행 순서:
#   [1/6] registry-pull-secret — 없을 때만 생성 (네임스페이스 공유 Secret, 있으면 손대지 않음 — 설계 §7.5)
#   [2/6] token-mart-metrics-ch-secret[-verify] 멱등 생성 (envFrom — 키 11개, CH_HOST 포함)
#   [3/6] 읽기 계약 프리플라이트 — DESCRIBE 3테이블/13컬럼 (설계 §6.1; 불일치 시 exit 3, DDL 적용 전 중단)
#   [4/6] 테이블 DDL 적용 (mart_metrics_tables.sql — accounts.sql은 admin 수동)
#   [5/6] CronJob 배포 (kustomize overlay)
#   [6/6] 이미지 주소 주입 + 수동 테스트 커맨드 안내
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

IMAGE_NAME="token-mart-metrics"
CRONJOB_NAME="token-mart-metrics"
SECRET_NAME="token-mart-metrics-ch-secret"
PULL_SECRET_NAME="registry-pull-secret"
CH_NAMESPACE="clickhouse"

REGISTRY=""; TAG=""; KUBE_CONTEXT=""; NAMESPACE="monitoring"; ENV=""

usage() { sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry)      REGISTRY="$2"; shift 2 ;;
    --tag)           TAG="$2"; shift 2 ;;
    --context)       KUBE_CONTEXT="$2"; shift 2 ;;
    -n|--namespace)  NAMESPACE="$2"; shift 2 ;;
    --overlay)       ENV="$2"; shift 2 ;;
    stage|company|company-verify) ENV="$1"; shift ;;
    *) echo "[ERROR] unknown arg: $1"; usage ;;
  esac
done
case "${ENV}" in stage|company|company-verify) ;; *) echo "[ERROR] env must be stage|company|company-verify: '${ENV}'"; usage ;; esac

# DB명 5종 (설계 §6.1 Secret 키 CH_DB_* — [2/6] Secret 값이자 [3/6] 프리플라이트의 DESCRIBE 대상 접두).
# 토큰 측 읽기 전용 CH_DB_TOKEN_MART/CH_DB_TOKEN_DIM 기본 = CH_DB_MART/CH_DB_DIM (app/ch.py fallback과 동일).
CH_DB_FACT="fact"; CH_DB_DIM="gpu_data"; CH_DB_MART="mart"
CH_DB_TOKEN_MART="mart"; CH_DB_TOKEN_DIM="gpu_data"
MAX_MUTATIONS="64"

case "${ENV}" in
  stage)
    KUBE_CONTEXT="${KUBE_CONTEXT:-homelab}"
    REGISTRY="${REGISTRY:-ghcr.io/yoonsungnam}"
    ;;
  company)
    [[ -n "${KUBE_CONTEXT}" ]] || { echo "[ERROR] company 환경에서는 --context 옵션이 필수입니다."; usage; }
    [[ -n "${REGISTRY}" ]]     || { echo "[ERROR] company 환경에서는 --registry 옵션이 필수입니다."; usage; }
    ;;
  company-verify)
    [[ -n "${KUBE_CONTEXT}" ]] || { echo "[ERROR] company-verify 환경에서는 --context 옵션이 필수입니다."; usage; }
    [[ -n "${REGISTRY}" ]]     || { echo "[ERROR] company-verify 환경에서는 --registry 옵션이 필수입니다."; usage; }
    SECRET_NAME="${SECRET_NAME}-verify"
    CRONJOB_NAME="${CRONJOB_NAME}-verify"
    # 격리 DB 3종(tools/gen_verify_ddl.py 기본안) — 토큰 측 읽기(CH_DB_TOKEN_*)는 운영 DB 유지
    CH_DB_FACT="token_verify_fact"; CH_DB_DIM="token_verify_dim"; CH_DB_MART="token_verify_mart"
    ;;
esac

if [[ -z "${TAG}" ]]; then
  if git -C "${HERE}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    TAG="$(git -C "${HERE}" rev-parse --short HEAD)"
  else
    TAG="latest"
  fi
fi

KUBECTL="kubectl --context=${KUBE_CONTEXT} --insecure-skip-tls-verify"

# kube API 서버 호스트를 NO_PROXY에 자동 추가 (사내 프록시 환경에서 kubectl 통신 보존 — 원형과 동일)
api_server="$(${KUBECTL} config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || true)"
if [[ -n "${api_server}" ]]; then
  api_host="$(printf '%s' "${api_server}" | sed -E 's#^https?://##; s#:[0-9]+$##')"
  export NO_PROXY="${NO_PROXY:+${NO_PROXY},}${api_host}"
  export no_proxy="${no_proxy:+${no_proxy},}${api_host}"
fi

echo "=== token-mart-metrics install (${ENV}) ==="
echo "context=${KUBE_CONTEXT} namespace=${NAMESPACE} image=${REGISTRY}/${IMAGE_NAME}:${TAG}"

# ── ClickHouse 파드 탐색 (chi-*; [2/6] CH_HOST 값·[3/6] DESCRIBE·[4/6] DDL이 공유) ──────────
ch_pod="$(${KUBECTL} get pods -n "${CH_NAMESPACE}" -o name 2>/dev/null \
  | sed 's#^pod/##' | grep '^chi-' | head -1 || true)"
if [[ -z "${ch_pod}" ]]; then
  echo "[ERROR] ${CH_NAMESPACE} 네임스페이스에서 chi-* ClickHouse 파드를 찾지 못했습니다."
  exit 1
fi
# 파드명 말미 ordinal을 잘라 헤드리스 서비스명 유도 (예: chi-<cluster>-<cluster>-0-0-0 → chi-<cluster>-<cluster>-0-0)
ch_host="${ch_pod%-*}.${CH_NAMESPACE}.svc"
echo "  ClickHouse pod: ${ch_pod} (CH_HOST=${ch_host})"

# ── [1/6] registry pull secret — 없을 때만 생성 (설계 §7.5 공유 Secret 예외) ───────────────
echo ""
echo "[1/6] image pull secret '${PULL_SECRET_NAME}'"
if ${KUBECTL} get secret "${PULL_SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  echo "  이미 존재합니다 — 네임스페이스 공유 Secret이므로 손대지 않습니다 (기존 token-usage 배포 소유)."
else
  read -r -p "  registry server [${REGISTRY%%/*}]: " reg_server
  reg_server="${reg_server:-${REGISTRY%%/*}}"
  read -r -p "  registry username: " reg_user
  read -r -s -p "  registry password/token: " reg_pass; echo ""
  ${KUBECTL} create secret docker-registry "${PULL_SECRET_NAME}" \
    --docker-server="${reg_server}" --docker-username="${reg_user}" \
    --docker-password="${reg_pass}" -n "${NAMESPACE}"
fi

# ── [2/6] app secret (envFrom — 키 이름이 곧 컨테이너 env 이름, 설계 §6.1) ─────────────────
# 원형과 델타: 지연 서비스 허용 목록 env 없음(M0 커버리지는 레지스트리 coverage_since/until이 결정),
# CH_DB_* 5종 + MART_METRICS_MAX_MUTATIONS_PER_RUN 항상 포함, CH_HOST도 Secret 키(정적 env 주입 금지).
# INSERT_QUORUM=auto는 company·company-verify 자동 포함(2s×2r 물리 클러스터 — 레플리카 지연 게이트).
echo ""
echo "[2/6] app secret '${SECRET_NAME}'"
if ${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  read -r -p "  이미 존재합니다. 갱신하시겠습니까? [y/N] " ans
else
  ans="y"
fi
if [[ "${ans}" == "y" || "${ans}" == "Y" ]]; then
  ch_user_default="mart"
  [[ "${ENV}" == "company-verify" ]] && ch_user_default="token_verify"
  read -r -p "  CH_USER [${ch_user_default}]: " ch_user
  ch_user="${ch_user:-${ch_user_default}}"
  read -r -s -p "  CH_PASSWORD: " ch_pass; echo ""
  read -r -p "  CH_DB_FACT [${CH_DB_FACT}]: " v;        CH_DB_FACT="${v:-${CH_DB_FACT}}"
  read -r -p "  CH_DB_DIM [${CH_DB_DIM}]: " v;          CH_DB_DIM="${v:-${CH_DB_DIM}}"
  read -r -p "  CH_DB_MART [${CH_DB_MART}]: " v;        CH_DB_MART="${v:-${CH_DB_MART}}"
  read -r -p "  CH_DB_TOKEN_MART (토큰 mart 읽기 — 격리 검증 시 운영 DB) [${CH_DB_TOKEN_MART}]: " v
  CH_DB_TOKEN_MART="${v:-${CH_DB_TOKEN_MART}}"
  read -r -p "  CH_DB_TOKEN_DIM (dim_token_service 읽기 — 격리 검증 시 운영 DB) [${CH_DB_TOKEN_DIM}]: " v
  CH_DB_TOKEN_DIM="${v:-${CH_DB_TOKEN_DIM}}"
  read -r -p "  MART_METRICS_MAX_MUTATIONS_PER_RUN [${MAX_MUTATIONS}]: " v; MAX_MUTATIONS="${v:-${MAX_MUTATIONS}}"
  # stage 홈랩 CHI는 ZK 없음 — ON CLUSTER 불가하므로 단일노드 모드(빈 값). company/-verify는 클러스터명 주입
  # (CH_CLUSTER와 DDL의 ON CLUSTER 리터럴 일치 전제)
  CH_CLUSTER_VALUE="gpu-monitoring"
  [[ "${ENV}" == "stage" ]] && CH_CLUSTER_VALUE=""
  args=(--from-literal="CH_HOST=${ch_host}"
        --from-literal="CH_PORT=8123"
        --from-literal="CH_USER=${ch_user}"
        --from-literal="CH_PASSWORD=${ch_pass}"
        --from-literal="CH_CLUSTER=${CH_CLUSTER_VALUE}"
        --from-literal="CH_DB_FACT=${CH_DB_FACT}"
        --from-literal="CH_DB_DIM=${CH_DB_DIM}"
        --from-literal="CH_DB_MART=${CH_DB_MART}"
        --from-literal="CH_DB_TOKEN_MART=${CH_DB_TOKEN_MART}"
        --from-literal="CH_DB_TOKEN_DIM=${CH_DB_TOKEN_DIM}"
        --from-literal="MART_METRICS_MAX_MUTATIONS_PER_RUN=${MAX_MUTATIONS}")
  if [[ "${ENV}" == "company" || "${ENV}" == "company-verify" ]]; then
    args+=(--from-literal="INSERT_QUORUM=auto")
  fi
  ${KUBECTL} create secret generic "${SECRET_NAME}" "${args[@]}" -n "${NAMESPACE}" \
    --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -
fi

# ── [3/6] 읽기 계약 프리플라이트 (설계 §6.1 3테이블/13컬럼 — DESCRIBE 대조, 불일치 시 설치 중단) ──
# 항목 형식 "<db>.<table>_dist:<column>" — tests/test_install_contract.py가 app/preflight.py READ_CONTRACT와
# 동일함을 단언한다(정본 2곳의 드리프트 차단). 여분 컬럼은 허용(계약 = 부분집합).
READ_CONTRACT=(
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:date"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:service_group"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:service"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:model"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:input_tokens"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:cache_read_tokens"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:cache_creation_tokens"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:output_tokens"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:requests"
  "${CH_DB_TOKEN_MART}.agg_token_service_1d_dist:date"
  "${CH_DB_TOKEN_MART}.agg_token_service_1d_dist:service"
  "${CH_DB_TOKEN_DIM}.dim_token_service_dist:service"
  "${CH_DB_TOKEN_DIM}.dim_token_service_dist:enabled"
)
echo ""
echo "[3/6] read-contract preflight (DESCRIBE — ${#READ_CONTRACT[@]} columns)"
missing=()
prev_table=""; cols=""
for entry in "${READ_CONTRACT[@]}"; do
  table="${entry%%:*}"; col="${entry##*:}"
  if [[ "${table}" != "${prev_table}" ]]; then
    prev_table="${table}"
    cols="$(${KUBECTL} exec -n "${CH_NAMESPACE}" "${ch_pod}" -- \
      clickhouse-client -q "DESCRIBE TABLE ${table}" 2>/dev/null | cut -f1 || true)"
    if [[ -z "${cols}" ]]; then
      missing+=("${table}.*")
      echo "  ${table}: 테이블 부재(또는 DESCRIBE 권한 없음)"
    else
      echo "  ${table}: $(printf '%s\n' "${cols}" | wc -l) columns"
    fi
  fi
  if [[ -n "${cols}" ]] && ! grep -qx "${col}" <<<"${cols}"; then
    missing+=("${table}.${col}")
  fi
done
if [[ ${#missing[@]} -gt 0 ]]; then
  echo "PREFLIGHT FAIL read_contract missing=$(IFS=,; echo "${missing[*]}")"
  echo "  (설계 §6.1 읽기 계약 불일치 — DDL/CronJob 적용 전 중단. 사내 스키마·GRANT 확인 후 재실행)"
  exit 3
fi
echo "  PREFLIGHT OK read_contract tables=3 columns=${#READ_CONTRACT[@]}"

# ── [4/6] 테이블 DDL (kubectl cp + clickhouse-client — 원형 apply_sql 그대로) ────────────────
echo ""
echo "[4/6] table DDL"
apply_sql() {
  local sql_file="$1" base tmp_pod_path
  base="$(basename "${sql_file}")"
  tmp_pod_path="/tmp/${base}"
  ${KUBECTL} cp "${sql_file}" "${CH_NAMESPACE}/${ch_pod}:${tmp_pod_path}"
  ${KUBECTL} exec -n "${CH_NAMESPACE}" "${ch_pod}" -- \
    sh -c "clickhouse-client --multiquery < ${tmp_pod_path}"
  ${KUBECTL} exec -n "${CH_NAMESPACE}" "${ch_pod}" -- rm -f "${tmp_pod_path}"
  echo "  applied: ${base}"
}
DDL_DIR="ddl/company"
[[ "${ENV}" == "company-verify" ]] && DDL_DIR="ddl/company-verify"
# stage 홈랩 CHI는 ZK 없음 — Replicated/ON CLUSTER 불가, 생성 변형 사용 (tools/gen_stage_ddl.py)
[[ "${ENV}" == "stage" ]] && DDL_DIR="ddl/stage"
echo "  (GRANT는 admin 수동: ${DDL_DIR}/accounts.sql — Plan 6a ddl/README 적용 순서 2. 읽기 대상 DDL"
echo "   collectors/token-metrics/ddl·assets/model-catalog/ddl은 6b install.sh/admin이 먼저 적용해야 한다)"
apply_sql "${HERE}/${DDL_DIR}/mart_metrics_tables.sql"
echo "  (accounts.sql은 적용하지 않았습니다 — GRANT/계정은 admin 수동, 설계 §7.5 DDL/GRANT)"

# ── [5/6] CronJob 배포 ────────────────────────────────────────────────────────────────────
echo ""
echo "[5/6] CronJob apply (overlay: ${ENV})"
${KUBECTL} apply -k "${HERE}/k8s/overlays/${ENV}" -n "${NAMESPACE}"

# ── [6/6] 이미지 주소 주입 (env는 전부 Secret — 정적 env 주입 없음) ──────────────────────────
echo ""
echo "[6/6] set image"
${KUBECTL} set image "cronjob/${CRONJOB_NAME}" \
  "${IMAGE_NAME}=${REGISTRY}/${IMAGE_NAME}:${TAG}" -n "${NAMESPACE}"

rerun_hint="python3 ${HERE}/tools/rerun.py --context ${KUBE_CONTEXT} --namespace ${NAMESPACE}"
[[ "${ENV}" == "company-verify" ]] && rerun_hint="${rerun_hint} --cronjob ${CRONJOB_NAME}"
echo ""
echo "[OK] 설치 완료. 수동 테스트(창 10:50 KST 이후 — 설계 §6.3):"
echo "  ${rerun_hint}"
echo "  (범위 재수행: ${rerun_hint} --from YYYY-MM-DD --to YYYY-MM-DD [--chunk-days 7])"
echo "  (또는 kubectl 직접: ${KUBECTL} create job --from=cronjob/${CRONJOB_NAME} ${CRONJOB_NAME}-manual-\$(date +%s) -n ${NAMESPACE})"
```

```bash
chmod +x /home/mini/github/token-data-pipeline/mart/token-metrics/install.sh
```

검증(클러스터 없이 — 문법·usage·헤더 경계·인자 검증):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics
bash -n install.sh && echo SYNTAX_OK
./install.sh; echo "exit=$?"
./install.sh company; echo "exit=$?"
./install.sh --overlay company; echo "exit=$?"
./install.sh --overlay bogus; echo "exit=$?"
sed -n '21,22p' install.sh
grep -c '"\[[1-6]/6\]' install.sh
grep -n 'PREFLIGHT FAIL read_contract missing=\|PREFLIGHT OK read_contract\|^  exit 3$' install.sh
grep -c -- '--from-literal="' install.sh
grep -n 'set env\|EXPECTED_LATE_SERVICES\|target-db' install.sh; echo "forbidden=$?"
```

기대:
- `SYNTAX_OK`
- `./install.sh` → `[ERROR] env must be stage|company|company-verify: ''` 뒤에 2–21행의 헤더 주석이 `# ` 제거된 형태로 출력되고 `exit=1`(usage; 마지막 출력 행은 `[6/6] 이미지 주소 주입 + 수동 테스트 커맨드 안내`, `set -euo pipefail` 행은 출력되지 않는다)
- `./install.sh company` → `[ERROR] company 환경에서는 --context 옵션이 필수입니다.` + usage, `exit=1`
- `./install.sh --overlay company` → 위와 같은 `--context` 오류 + usage, `exit=1` (`--overlay`가 위치 인자와 동치)
- `./install.sh --overlay bogus` → `[ERROR] env must be stage|company|company-verify: 'bogus'` + usage, `exit=1`
- `sed -n '21,22p'` → 21행 `#   [6/6] 이미지 주소 주입 + 수동 테스트 커맨드 안내`, 22행 `set -euo pipefail`
- `grep -c '"\[[1-6]/6\]'` → `6` (echo 6행; 주석의 `[k/6]`는 따옴표 없음)
- 3행 매치: `PREFLIGHT FAIL read_contract missing=…`, `exit 3`, `PREFLIGHT OK read_contract tables=3 columns=…`
- `--from-literal="` 개수 → `12` (11키 + 조건부 `INSERT_QUORUM`)
- 금지 문구 grep → 출력 없음, `forbidden=1`

- [ ] **Step 8: Step 6 테스트 GREEN 확인**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics
python -m pytest -q tests/test_install_contract.py 2>&1 | tail -n 3
```

기대: `7 passed` (`test_kustomize_overlays_render_contract`는 kubectl 부재 시 `6 passed, 1 skipped`).

`test_install_read_contract_equals_preflight`가 실패하면 원인은 둘 중 하나다 — (a) T1 `app/preflight.py`의 `READ_CONTRACT` 키가 `f"{DB_TOKEN_MART}.token_usage_1d"` 형식이 아니거나 컬럼 순서가 설계 §6.1(9/2/2)과 다름 → T1이 정본이므로 install.sh의 `READ_CONTRACT=( … )` 순서를 T1에 맞춘다(테이블·컬럼 집합 자체는 설계 §6.1이 고정하므로 집합이 다르면 T1 수정이 먼저), (b) install.sh 배열의 `_dist` 접미 누락 → 접미를 붙인다. 두 정본이 일치할 때까지 커밋하지 않는다.

Dockerfile/build.sh/k8s/install.sh/테스트는 여기서 커밋하지 않는다 — T8 산출물 전체(rerun.py·워크플로 포함)를 Step 13에서 outline 지정 메시지로 한 번에 커밋한다(중간 커밋이 있으면 outline의 커밋 메시지가 산출물 목록과 어긋난다).

- [ ] **Step 9: rerun.py 테스트 작성(RED)** — 원형 `mart/token-usage/tests/test_rerun.py`(123행) 클론 + 델타: 상수(`token-mart-metrics`/창/청크), 순수 함수 5종(`chunk_ranges`/`window_ok`/`active_mart_jobs`/`build_batch_command`/`range_deadline_s`), `args` override(`command` 불변), 게이트 2종(창·활성 Job)과 순차 청크 실행을 `kubectl`/`wait_job`/`_now_kst` 대체로 검증. `random` 미사용, 모든 datetime은 aware KST.

`mart/token-metrics/tests/test_rerun.py` 전문:

```python
import datetime as dt
import importlib.util
import json
import pathlib
import types

import pytest

_RERUN_PATH = pathlib.Path(__file__).resolve().parents[1] / "tools" / "rerun.py"
spec = importlib.util.spec_from_file_location("rerun_metrics", _RERUN_PATH)
rerun = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rerun)

D = dt.date


def cronjob_obj():
    return {
        "metadata": {"name": "token-mart-metrics", "namespace": "monitoring",
                     "resourceVersion": "123", "uid": "x"},
        "spec": {"jobTemplate": {"spec": {
            "backoffLimit": 1,
            "activeDeadlineSeconds": 1800,
            "template": {"spec": {"restartPolicy": "Never", "containers": [
                {"name": "token-mart-metrics", "image": "img:tag"}]}},
        }}},
    }


def jobs_json(*items):
    return {"items": [{"metadata": {"name": n}, "status": s} for n, s in items]}


def kst(hh, mm):
    return dt.datetime(2026, 9, 5, hh, mm, tzinfo=rerun.KST)


# ── 상수·심볼 ────────────────────────────────────────────────────────────────────────────

def test_cronjob_and_window_constants():
    # 델타 누락 시 token-usage CronJob(token-mart-daily)을 오트리거하는 치명 오류가 된다
    assert rerun.CRONJOB == "token-mart-metrics"
    assert rerun.WINDOW_HHMM == (10, 50)
    assert rerun.NAMESPACE_DEFAULT == "monitoring"
    assert rerun.CHUNK_DAYS_DEFAULT == 7
    assert rerun.CHUNK_DAYS_MAX == 16                                     # 64 = 16일 × 4 변이
    assert rerun.ACTIVE_JOB_PREFIX == "token-mart-"
    assert rerun.KST.utcoffset(None) == dt.timedelta(hours=9)


def test_no_chain_or_downstream_symbols():
    # 체인의 종단 — 하류 심볼(MART_RERUN/build_mart_command)·원형 command 빌더 없음
    for sym in ("MART_RERUN", "build_mart_command", "build_collect_command"):
        assert not hasattr(rerun, sym), sym


def test_no_chain_flag():
    parser = rerun.build_arg_parser()
    for flag in (["--chain"], ["--chain-mart"], ["--service", "S"], ["--push-vm"],
                 ["--replace"], ["--target-db", "x"]):
        with pytest.raises(SystemExit):
            parser.parse_args(["--context", "c"] + flag)


def test_cli_defaults_and_overrides():
    args = rerun.build_arg_parser().parse_args(["--context", "homelab"])
    assert args.namespace == "monitoring" and args.cronjob == "token-mart-metrics"
    assert args.chunk_days == 7 and args.force is False
    args = rerun.build_arg_parser().parse_args(
        ["--context", "c", "-n", "ns", "--cronjob", "token-mart-metrics-verify",
         "--from", "2026-08-01", "--to", "2026-08-17", "--chunk-days", "3", "--force"])
    assert (args.namespace, args.cronjob, args.chunk_days, args.force) == \
        ("ns", "token-mart-metrics-verify", 3, True)


# ── 순수 함수 ────────────────────────────────────────────────────────────────────────────

def test_chunk_ranges_seven_day_split():
    assert rerun.chunk_ranges(D(2026, 8, 1), D(2026, 8, 17), 7) == \
        [(D(2026, 8, 1), D(2026, 8, 7)), (D(2026, 8, 8), D(2026, 8, 14)), (D(2026, 8, 15), D(2026, 8, 17))]


def test_chunk_ranges_single_day():
    assert rerun.chunk_ranges(D(2026, 8, 1), D(2026, 8, 1), 7) == [(D(2026, 8, 1), D(2026, 8, 1))]
    assert rerun.chunk_ranges(D(2026, 8, 1), D(2026, 8, 7), 7) == [(D(2026, 8, 1), D(2026, 8, 7))]
    with pytest.raises(ValueError):
        rerun.chunk_ranges(D(2026, 8, 1), D(2026, 8, 7), 0)


def test_window_ok_boundary():
    assert rerun.window_ok(kst(10, 49)) is False
    assert rerun.window_ok(kst(10, 50)) is True
    assert rerun.window_ok(kst(23, 59)) is True
    assert rerun.window_ok(kst(0, 0)) is False                            # 자정 직후도 창 밖
    assert rerun.window_ok(kst(10, 49), force=True) is True


def test_active_mart_jobs_counts_only_active_token_mart_prefix():
    fixture = jobs_json(("token-mart-metrics-x", {"active": 1}),
                        ("token-mart-daily-y", {"active": 1}),
                        ("token-usage-collector-z", {"active": 1}),
                        ("token-mart-metrics-old", {"succeeded": 1}))
    assert rerun.active_mart_jobs(fixture) == 2
    assert rerun.active_mart_jobs({"items": []}) == 0
    assert rerun.active_mart_jobs({}) == 0


def test_build_batch_command_args():
    # ENTRYPOINT(python -m app.batch) 뒤 args만 — "python"/"-m"이 들어가면 인자가 중복된다
    assert rerun.build_batch_command(D(2026, 8, 1), D(2026, 8, 7)) == \
        ["--from", "2026-08-01", "--to", "2026-08-07"]


def test_range_deadline_seven_days_1800():
    assert rerun.range_deadline_s(1) == 1800
    assert rerun.range_deadline_s(7) == 1800
    assert rerun.range_deadline_s(8) == 3600
    assert rerun.range_deadline_s(16) == 5400
    assert rerun.range_deadline_s(100) == rerun.TIMEOUT_RANGE_S == 7200


def test_job_name_format_and_length():
    name = rerun.job_name("token-mart-metrics-verify", D(2026, 8, 1), D(2026, 8, 7), 1756000000)
    assert name == "token-mart-metrics-verify-rerun-20260801-20260807-1756000000"
    assert len(name) <= 63


def test_build_job_spec_overrides_args_not_command_and_strips_cron_metadata():
    job = rerun.build_job_spec(cronjob_obj(), "token-mart-metrics-rerun-1",
                               ["--from", "2026-08-01", "--to", "2026-08-07"])
    assert job["kind"] == "Job"
    assert job["metadata"] == {"name": "token-mart-metrics-rerun-1"}      # uid/resourceVersion 제거
    c0 = job["spec"]["template"]["spec"]["containers"][0]
    assert c0["args"] == ["--from", "2026-08-01", "--to", "2026-08-07"]
    assert "command" not in c0                                             # ENTRYPOINT 유지
    assert job["spec"]["activeDeadlineSeconds"] == 1800                    # 기본: CronJob 값 상속
    job = rerun.build_job_spec(cronjob_obj(), "j", ["--from", "a", "--to", "b"], active_deadline_s=3600)
    assert job["spec"]["activeDeadlineSeconds"] == 3600


# ── main: 인자 오류(exit 2) ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("argv", [
    ["--context", "homelab", "--from", "2026-08-05", "--to", "2026-08-01"],
    ["--context", "homelab", "--from", "2026/08/01", "--to", "2026-08-02"],
    ["--context", "homelab", "--from", "2026-08-01"],
    ["--context", "homelab", "--chunk-days", "17"],
    ["--context", "homelab", "--chunk-days", "0"],
    [],
])
def test_usage_errors_exit_2(argv):
    with pytest.raises(SystemExit) as e:
        rerun.main(argv)
    assert e.value.code == 2


# ── main: 게이트·순차 청크 실행 (kubectl/wait_job/_now_kst 대체) ──────────────────────────

class FakeKubectl:
    """kubectl(context, args, capture=…, input_data=…) 대체 — 호출 기록 + 고정 응답."""

    def __init__(self, jobs=None, cronjob=None):
        self.calls = []
        self.jobs = jobs if jobs is not None else {"items": []}
        self.cronjob = cronjob if cronjob is not None else cronjob_obj()

    def __call__(self, context, args, capture=False, input_data=None):
        self.calls.append((list(args), input_data))
        if args[:2] == ["get", "jobs"]:
            return types.SimpleNamespace(stdout=json.dumps(self.jobs))
        if args[:2] == ["get", "cronjob"]:
            return types.SimpleNamespace(stdout=json.dumps(self.cronjob))
        return types.SimpleNamespace(stdout="")

    def applied(self):
        return [json.loads(inp) for a, inp in self.calls if a[:1] == ["apply"]]


def test_window_refused_exit_2_without_kubectl(monkeypatch):
    fake = FakeKubectl()
    monkeypatch.setattr(rerun, "kubectl", fake)
    monkeypatch.setattr(rerun, "_now_kst", lambda: kst(10, 49))
    assert rerun.main(["--context", "c"]) == 2
    assert rerun.main(["--context", "c", "--from", "2026-08-01", "--to", "2026-08-02"]) == 2
    assert fake.calls == []                                                # 창 밖이면 kubectl 미호출


def test_active_jobs_refused_exit_2_even_with_force(monkeypatch):
    fake = FakeKubectl(jobs=jobs_json(("token-mart-daily-abc", {"active": 1})))
    monkeypatch.setattr(rerun, "kubectl", fake)
    monkeypatch.setattr(rerun, "_now_kst", lambda: kst(10, 49))
    assert rerun.main(["--context", "c", "--force"]) == 2
    assert [a[:2] for a, _ in fake.calls] == [["get", "jobs"]]            # 활성 Job 조회 후 중단


def test_manual_mode_creates_job_from_cronjob(monkeypatch):
    fake = FakeKubectl()
    waited = []
    monkeypatch.setattr(rerun, "kubectl", fake)
    monkeypatch.setattr(rerun, "_now_kst", lambda: kst(11, 0))
    monkeypatch.setattr(rerun, "wait_job", lambda ctx, ns, name, t: waited.append((name, t)) or True)
    assert rerun.main(["--context", "c", "-n", "ns", "--cronjob", "token-mart-metrics-verify"]) == 0
    create = [a for a, _ in fake.calls if a[:2] == ["create", "job"]]
    assert len(create) == 1 and create[0][2] == "--from=cronjob/token-mart-metrics-verify"
    assert create[0][3].startswith("token-mart-metrics-verify-manual-")
    assert waited[0][1] == rerun.TIMEOUT_SINGLE_S == 2400


def test_range_mode_runs_chunks_sequentially(monkeypatch):
    fake = FakeKubectl()
    waited = []
    monkeypatch.setattr(rerun, "kubectl", fake)
    monkeypatch.setattr(rerun, "_now_kst", lambda: kst(10, 50))
    monkeypatch.setattr(rerun, "wait_job", lambda ctx, ns, name, t: waited.append((name, t)) or True)
    assert rerun.main(["--context", "c", "--from", "2026-08-01", "--to", "2026-08-17"]) == 0
    jobs = fake.applied()
    assert [j["spec"]["template"]["spec"]["containers"][0]["args"] for j in jobs] == [
        ["--from", "2026-08-01", "--to", "2026-08-07"],
        ["--from", "2026-08-08", "--to", "2026-08-14"],
        ["--from", "2026-08-15", "--to", "2026-08-17"],
    ]
    assert [j["spec"]["activeDeadlineSeconds"] for j in jobs] == [1800, 1800, 1800]
    assert [j["metadata"]["name"][:42] for j in jobs] == [
        "token-mart-metrics-rerun-20260801-20260807",
        "token-mart-metrics-rerun-20260808-20260814",
        "token-mart-metrics-rerun-20260815-20260817",
    ]
    # apply → wait → apply → wait … (순차: 다음 apply 전에 wait가 끝난다)
    order = [a[0] for a, _ in fake.calls]
    assert order == ["get", "get", "apply", "apply", "apply"]
    assert [n[:42] for n, _ in waited] == [j["metadata"]["name"][:42] for j in jobs]
    assert all(t == 1800 + 600 for _, t in waited)


def test_range_mode_stops_at_failed_chunk(monkeypatch, capsys):
    fake = FakeKubectl()
    results = iter([True, False])
    monkeypatch.setattr(rerun, "kubectl", fake)
    monkeypatch.setattr(rerun, "_now_kst", lambda: kst(12, 0))
    monkeypatch.setattr(rerun, "wait_job", lambda ctx, ns, name, t: next(results))
    assert rerun.main(["--context", "c", "--from", "2026-08-01", "--to", "2026-08-17"]) == 1
    assert len(fake.applied()) == 2                                        # 3번째 청크 미실행
    err = capsys.readouterr().err
    assert "--from 2026-08-15 --to 2026-08-17" in err                      # 남은 범위 재실행 안내


def test_range_mode_chunk_days_16_deadline_5400(monkeypatch):
    fake = FakeKubectl()
    monkeypatch.setattr(rerun, "kubectl", fake)
    monkeypatch.setattr(rerun, "_now_kst", lambda: kst(12, 0))
    monkeypatch.setattr(rerun, "wait_job", lambda ctx, ns, name, t: True)
    assert rerun.main(["--context", "c", "--from", "2026-08-01", "--to", "2026-08-16",
                       "--chunk-days", "16"]) == 0
    jobs = fake.applied()
    assert len(jobs) == 1 and jobs[0]["spec"]["activeDeadlineSeconds"] == 5400
```

실행(RED):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics
python -m pytest -q tests/test_rerun.py 2>&1 | tail -n 3
```

기대: 모듈 최상단 `spec.loader.exec_module(rerun)`이 `FileNotFoundError: [Errno 2] No such file or directory: '…/mart/token-metrics/tools/rerun.py'` → `1 error in …` (수집 단계 에러 — tools/rerun.py 부재).

- [ ] **Step 10: tools/rerun.py 작성** — 원형 `mart/token-usage/tools/rerun.py:1-165` 클론 + 델타: `CRONJOB`/`NAMESPACE_DEFAULT`/`WINDOW_HHMM`/`CHUNK_DAYS_*`/`ACTIVE_JOB_PREFIX`/`KST` 상수, `_now_kst()`, `chunk_ranges`/`window_ok`/`active_mart_jobs`/`job_name` 신규, `build_collect_command` → `build_batch_command`(args만), `build_job_spec`은 `args` override(`command` 미변경), `range_deadline_s`는 7일당 1800s(상한 7200), `main`은 창·활성 Job 게이트 후 청크 순차 실행. `wait_job`/`kubectl`은 원형 그대로.

`mart/token-metrics/tools/rerun.py` 전문:

```python
"""mart/token-metrics 재수행 도구 (설계 §7.5 / §4.0 뮤테이션 장부) — 체인의 **종단**(하류 없음).

두 가지 모드:
  1) 1회 수동 트리거(기본) — CronJob token-mart-metrics에서 Job 생성 (실행 시점 기준
     어제 KST 집계, app.batch의 기본 target_date 계약과 동일)
  2) 날짜 범위 재수행(--from/--to, **inclusive** — app.batch 계약과 동일) — 범위를
     --chunk-days(기본 7)일 단위 청크로 나눠 청크마다 CronJob 스펙에서 Job을 만들되
     containers[0].args만 override(이미지 ENTRYPOINT = python -m app.batch), **순차** 실행

두 모드 공통 게이트(설계 §7.5 재실행 절차):
  - 창: 현재 KST가 10:50 이후여야 한다 (일일 CronJob 10:20 + activeDeadlineSeconds 1800
    = 10:50 — 일일 실행과의 겹침 차단). --force로 무시 가능.
  - 활성 Job: 네임스페이스에 실행 중(status.active>0)인 token-mart-* Job이 0이어야 한다
    (token-mart-daily/token-mart-metrics[-verify] 모두 — 동일 mart DB 변이 직렬화). --force로도
    무시할 수 없다.

변이 예산(설계 §4.0 뮤테이션 장부): 배치 1회 변이 = 날짜당 4(mart 4테이블 delete_day) → 청크 7일 = 28
≤ MART_METRICS_MAX_MUTATIONS_PER_RUN 기본 64(= 16일×4). 따라서 --chunk-days 상한 16.

사용법:
  python3 mart/token-metrics/tools/rerun.py --context homelab
  python3 mart/token-metrics/tools/rerun.py --context homelab \
      --from 2026-08-01 --to 2026-08-17 [--chunk-days 7] [--force]

옵션:
  --context       kubectl context (필수)
  -n/--namespace  기본 monitoring
  --cronjob       대상 CronJob 이름 (기본 token-mart-metrics — company-verify는
                  token-mart-metrics-verify 지정)
  --from/--to     YYYY-MM-DD, KST, 둘 다 inclusive. 반드시 쌍으로.
  --chunk-days    청크 일수 (기본 7, 1..16)
  --force         창(10:50 KST) 게이트 무시 (활성 Job 게이트는 무시 불가)

--service/--push-vm/--chain-mart/--chain/--replace/--target-db 플래그는 없다 — mart-metrics는
서비스 단위 재수집 개념이 없고(mart 재집계), 이 모듈 자체가 체인의 종단이며, 격리 검증은
Secret의 CH_DB_*로만 분기한다.
"""
import argparse
import copy
import datetime as dt
import json
import subprocess
import sys
import time

CRONJOB = "token-mart-metrics"
NAMESPACE_DEFAULT = "monitoring"
WINDOW_HHMM = (10, 50)            # 설계 §7.5: 10:20 CronJob + activeDeadlineSeconds 1800
CHUNK_DAYS_DEFAULT = 7            # 설계 §7.5 --chunk-days 7 (7×4 = 28 변이 ≤ 64)
CHUNK_DAYS_MAX = 16               # 예산 64 = 16일 × 4 변이/일 (설계 §4.0)
ACTIVE_JOB_PREFIX = "token-mart-"
DEADLINE_PER_CHUNK_S = 1800       # CronJob activeDeadlineSeconds — 청크 7일당 30분
TIMEOUT_RANGE_S = 7200            # 청크 Job activeDeadlineSeconds 상한
POLL_S = 10
TIMEOUT_SINGLE_S = DEADLINE_PER_CHUNK_S + 600     # 서버 데드라인 + 폴링 마진
KST = dt.timezone(dt.timedelta(hours=9), "KST")


def _now_kst():
    """테스트에서 monkeypatch 하는 현재 시각(aware KST)."""
    return dt.datetime.now(KST)


def kubectl(context, args, *, capture=False, input_data=None):
    cmd = ["kubectl", f"--context={context}", "--insecure-skip-tls-verify"] + args
    return subprocess.run(cmd, check=True, text=True, input=input_data,
                          capture_output=capture)


def chunk_ranges(from_d, to_d, chunk_days):
    """[from_d, to_d] inclusive를 chunk_days일 단위 (start, end) inclusive 목록으로 분할."""
    if chunk_days < 1:
        raise ValueError(f"chunk_days must be >= 1: {chunk_days}")
    out = []
    start = from_d
    while start <= to_d:
        end = min(start + dt.timedelta(days=chunk_days - 1), to_d)
        out.append((start, end))
        start = end + dt.timedelta(days=1)
    return out


def window_ok(now, force=False):
    """now(aware KST)가 WINDOW_HHMM(10:50) 이후면 True. force=True면 항상 True."""
    if force:
        return True
    hh, mm = WINDOW_HHMM
    return now.hour * 60 + now.minute >= hh * 60 + mm


def active_mart_jobs(kubectl_json):
    """`kubectl get jobs -o json` 결과에서 이름이 token-mart-* 이고 status.active > 0 인 Job 수."""
    n = 0
    for item in kubectl_json.get("items", []):
        name = item.get("metadata", {}).get("name", "")
        active = item.get("status", {}).get("active", 0) or 0
        if name.startswith(ACTIVE_JOB_PREFIX) and active > 0:
            n += 1
    return n


def build_batch_command(from_d, to_d):
    """컨테이너 args override — ENTRYPOINT(python -m app.batch) 뒤에 붙는 인자만."""
    return ["--from", from_d.isoformat(), "--to", to_d.isoformat()]


def range_deadline_s(n_days):
    """청크 Job activeDeadlineSeconds — 7일당 1800s(설계 해석: 일일 계약 1800s는 '7일 이하 청크'
    산식), 상한 TIMEOUT_RANGE_S. range_deadline_s(7)=1800, (8)=3600, (100)=7200."""
    return min(DEADLINE_PER_CHUNK_S * ((n_days + 6) // 7), TIMEOUT_RANGE_S)


def job_name(cronjob, from_d, to_d, epoch):
    """<cronjob>-rerun-YYYYMMDD-YYYYMMDD-<epoch> (token-mart-metrics-verify 포함 63자 이내)."""
    return f"{cronjob}-rerun-{from_d:%Y%m%d}-{to_d:%Y%m%d}-{epoch}"


def build_job_spec(cronjob_obj, name, args, active_deadline_s=None):
    """CronJob 오브젝트에서 1회성 Job 스펙 생성 + containers[0].args override.

    command는 건드리지 않는다(이미지 ENTRYPOINT python -m app.batch 유지). metadata는 name만
    남긴다(uid/resourceVersion 등 서버 필드 제거). active_deadline_s=None이면 jobTemplate.spec
    값(일일 계약 1800) 상속, 값이 있으면 override."""
    spec = copy.deepcopy(cronjob_obj["spec"]["jobTemplate"]["spec"])
    spec["template"]["spec"]["containers"][0]["args"] = list(args)
    if active_deadline_s is not None:
        spec["activeDeadlineSeconds"] = active_deadline_s
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": name}, "spec": spec}


def wait_job(context, namespace, job_name, timeout_s):
    """Job 완료 폴링 + 파드별 로그 스트리밍. 성공 True / 실패 False.

    backoffLimit=1 재시도 파드까지 각각 스트리밍한다 — 마커 라인(BATCH_RESULT …, 설계 §6.3)이
    운영 기록이므로 가공 없이 그대로 출력. 청크 Job은 날짜별 BATCH_RESULT가 독립 출력되므로
    한 Job 로그 안에 여러 줄이 순서대로 나타난다."""
    deadline = time.monotonic() + timeout_s
    seen_pods = set()
    while time.monotonic() < deadline:
        res = kubectl(context, ["get", "job", job_name, "-n", namespace, "-o", "json"],
                      capture=True)
        status = json.loads(res.stdout).get("status", {})
        conds = {c["type"]: c["status"] for c in status.get("conditions", [])}
        pods = kubectl(context, ["get", "pods", "-l", f"job-name={job_name}", "-n", namespace,
                                 "-o", "jsonpath={.items[*].metadata.name}"],
                       capture=True).stdout.split()
        for pod in pods:
            if pod not in seen_pods:
                seen_pods.add(pod)
                subprocess.run(["kubectl", f"--context={context}", "--insecure-skip-tls-verify",
                                "logs", "-f", f"pod/{pod}", "-n", namespace,
                                "--pod-running-timeout=5m"], check=False)
        if conds.get("Complete") == "True":
            print(f"[INFO] 전체 로그 재조회: kubectl --context={context} logs job/{job_name} "
                  f"-n {namespace} --prefix --tail=-1")
            return True
        if conds.get("Failed") == "True":
            print(f"[ERROR] job {job_name} failed — 전체 로그: kubectl --context={context} "
                  f"logs job/{job_name} -n {namespace} --prefix --tail=-1", file=sys.stderr)
            return False
        time.sleep(POLL_S)
    print(f"[ERROR] job {job_name} timeout ({timeout_s}s)", file=sys.stderr)
    return False


def build_arg_parser():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--context", required=True)
    p.add_argument("-n", "--namespace", default=NAMESPACE_DEFAULT)
    p.add_argument("--cronjob", default=CRONJOB,
                   help=f"대상 CronJob 이름 (기본 {CRONJOB})")
    p.add_argument("--from", dest="from_d", default=None)
    p.add_argument("--to", dest="to_d", default=None)
    p.add_argument("--chunk-days", dest="chunk_days", type=int, default=CHUNK_DAYS_DEFAULT,
                   help=f"청크 일수 (기본 {CHUNK_DAYS_DEFAULT}, 1..{CHUNK_DAYS_MAX})")
    p.add_argument("--force", action="store_true",
                   help="창(10:50 KST) 게이트 무시 — 활성 Job 게이트는 무시 불가")
    return p


def main(argv=None):
    p = build_arg_parser()
    args = p.parse_args(argv)

    if not 1 <= args.chunk_days <= CHUNK_DAYS_MAX:
        p.exit(2, f"--chunk-days는 1..{CHUNK_DAYS_MAX} (변이 예산 64 = 16일×4, 설계 §4.0)\n")
    if bool(args.from_d) != bool(args.to_d):
        p.exit(2, "--from/--to는 쌍으로 지정 (YYYY-MM-DD, KST, inclusive)\n")
    d0 = d1 = None
    if args.from_d:
        try:
            d0, d1 = dt.date.fromisoformat(args.from_d), dt.date.fromisoformat(args.to_d)
        except ValueError:
            p.exit(2, "--from/--to는 YYYY-MM-DD 형식\n")
        if d0 > d1:
            p.exit(2, f"--from({d0}) > --to({d1})\n")

    now = _now_kst()
    if not window_ok(now, args.force):
        print(f"RERUN REFUSED window (>=10:50 KST) — use --force (now={now:%H:%M} KST)",
              file=sys.stderr)
        return 2
    res = kubectl(args.context, ["get", "jobs", "-n", args.namespace, "-o", "json"], capture=True)
    n_active = active_mart_jobs(json.loads(res.stdout))
    if n_active > 0:
        print(f"RERUN REFUSED active_jobs={n_active} ({ACTIVE_JOB_PREFIX}* running)", file=sys.stderr)
        return 2

    epoch = int(time.time())
    if d0 is None:
        name = f"{args.cronjob}-manual-{epoch}"
        # args override 없음 — 컨테이너 ENTRYPOINT(python -m app.batch, 인자 없음)가
        # target_date = 실행 시점 기준 어제(KST)를 산정 (app.batch 계약)
        kubectl(args.context, ["create", "job", f"--from=cronjob/{args.cronjob}",
                               name, "-n", args.namespace])
        return 0 if wait_job(args.context, args.namespace, name, TIMEOUT_SINGLE_S) else 1

    chunks = chunk_ranges(d0, d1, args.chunk_days)
    print(f"[INFO] range {d0}..{d1} → {len(chunks)} chunk(s) × ≤{args.chunk_days}d "
          f"(≤{args.chunk_days * 4} mutations/chunk) — sequential")
    res = kubectl(args.context, ["get", "cronjob", args.cronjob, "-n", args.namespace,
                                 "-o", "json"], capture=True)
    cronjob_obj = json.loads(res.stdout)
    for i, (c0, c1) in enumerate(chunks, 1):
        n_days = (c1 - c0).days + 1
        name = job_name(args.cronjob, c0, c1, epoch)
        deadline = range_deadline_s(n_days)
        job = build_job_spec(cronjob_obj, name, build_batch_command(c0, c1),
                             active_deadline_s=deadline)
        print(f"[INFO] chunk {i}/{len(chunks)} {c0}..{c1} job={name} deadline={deadline}s")
        kubectl(args.context, ["apply", "-n", args.namespace, "-f", "-"],
                input_data=json.dumps(job))
        if not wait_job(args.context, args.namespace, name, deadline + 600):
            remaining = chunks[i:]
            if remaining:
                print(f"[ERROR] chunk {i} failed — 남은 청크 {len(remaining)}개 미실행: "
                      f"{remaining[0][0]}..{remaining[-1][1]} (재실행: --from {remaining[0][0]} "
                      f"--to {remaining[-1][1]})", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 11: rerun 테스트 GREEN + 모듈 전체 스위트**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics
python -m pytest -q tests/test_rerun.py 2>&1 | tail -n 3
python tools/rerun.py --help | head -n 3
python tools/rerun.py --context c --chunk-days 17; echo "exit=$?"
python -m pytest -q 2>&1 | tail -n 3
```

기대:
- `24 passed`
- `--help` 첫 행 `usage: rerun.py [-h] --context CONTEXT [-n NAMESPACE] [--cronjob CRONJOB]`, 이어서 `[--from FROM_D] [--to TO_D] [--chunk-days CHUNK_DAYS]`, `[--force]` (`--chain*`/`--service`/`--push-vm`/`--replace`/`--target-db` 없음)
- `--chunk-days 17` → stderr `--chunk-days는 1..16 (변이 예산 64 = 16일×4, 설계 §4.0)`, `exit=2` (kubectl 호출 전 종료 — 클러스터 불필요)
- 모듈 전체: T1–T7 테스트 + `test_install_contract.py` 7 + `test_rerun.py` 24 모두 PASS(`… passed`, `failed` 0; kubectl 부재 시 `1 skipped`)

- [ ] **Step 12: release-images-metrics.yml — collectors/mart token-metrics 이미지 릴리스 워크플로**

기존 `.github/workflows/release-images.yml`(digest §25, 52행)은 **무수정**(zero-diff). 새 워크플로는 원형의 job 골격(checkout → buildx → sha7 → ghcr 로그인 → build-push)을 그대로 쓰고 matrix만 token-metrics 2종으로 바꾼다. Plan 6b(collectors/token-metrics)가 이 파일을 먼저 만들었을 수 있으므로 두 경우로 나눈다.

(a) 파일이 없을 때 — `.github/workflows/release-images-metrics.yml` 전문(원형 `release-images.yml:13-52`의 job `release`·step 5개를 그대로, `name`/`paths`/`matrix`만 델타):

```yaml
name: release-images-metrics

on:
  push:
    branches: [main]
    paths:
      - "collectors/token-metrics/**"
      - "mart/token-metrics/**"
      - ".github/workflows/release-images-metrics.yml"
  workflow_dispatch:

jobs:
  release:
    runs-on: ubuntu-latest
    permissions:
      packages: write
      contents: read
    strategy:
      matrix:
        include:
          - context: collectors/token-metrics
            image: token-metrics-collector
          - context: mart/token-metrics
            image: token-mart-metrics
    steps:
      - uses: actions/checkout@v4

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Extract SHA7
        id: sha7
        run: echo "sha7=${GITHUB_SHA::7}" >> "$GITHUB_OUTPUT"

      - name: Login to GHCR
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Build and push
        uses: docker/build-push-action@v5
        with:
          context: ${{ matrix.context }}
          push: true
          tags: |
            ghcr.io/yoonsungnam/${{ matrix.image }}:latest
            ghcr.io/yoonsungnam/${{ matrix.image }}:${{ steps.sha7.outputs.sha7 }}
```

태그 계약: `:latest`(stage overlay `newTag: latest`)와 `:<sha7>`(`build.sh`/`install.sh`의 기본 `TAG=$(git rev-parse --short HEAD)`와 동일 7자) — 원형과 같다.

(b) Plan 6b가 이미 만들어 둔 경우(`collectors/token-metrics` 항목만 있는 상태) — `paths`와 `matrix.include`에 mart 항목만 **추가**한다(다른 행 무수정):

```yaml
      - "mart/token-metrics/**"
```

```yaml
          - context: mart/token-metrics
            image: token-mart-metrics
```

검증:

```bash
cd /home/mini/github/token-data-pipeline
python3 - <<'PY'
import yaml
wf = yaml.safe_load(open(".github/workflows/release-images-metrics.yml", encoding="utf-8"))
inc = wf["jobs"]["release"]["strategy"]["matrix"]["include"]
assert {"context": "mart/token-metrics", "image": "token-mart-metrics"} in inc, inc
assert {"context": "collectors/token-metrics", "image": "token-metrics-collector"} in inc, inc
paths = wf[True]["push"]["paths"]                      # YAML 1.1: 키 `on` → True
assert "mart/token-metrics/**" in paths and "collectors/token-metrics/**" in paths, paths
assert not any("token-usage" in p for p in paths), paths
print("WORKFLOW_OK", [i["image"] for i in inc])
PY
git diff --stat main -- .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml
```

기대: `WORKFLOW_OK ['token-metrics-collector', 'token-mart-metrics']`; `git diff --stat` 출력 없음(기존 3개 워크플로 zero-diff).

CI 이미지 스모크(T10에서 `test-mart-metrics.yml`이 수행)는 `docker run --rm token-mart-metrics:ci --help`여야 한다 — 이미지가 `ENTRYPOINT ["python", "-m", "app.batch"]`이므로 `python -m app.batch --help`를 통째로 넘기면 `python`이 첫 위치 인자(`batch_time`)로 파싱되어 실패한다(T10 Consumes 계약).

- [ ] **Step 13: zero-diff 확인 + 커밋**

기존 모듈·문서·워크플로에 변경이 없는지 확인한 뒤 T8 산출물 전체를 한 번에 커밋한다.

```bash
cd /home/mini/github/token-data-pipeline
git status --porcelain -- mart/token-usage collectors/token-usage assets/user-org tools/verify/invariants.sql \
  docs/operations/company-verify.md docs/operations/stage-runbook.md docs/operations/rerun.md \
  docs/monitoring/grafana_dashboard_token_usage.json \
  .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml
git status --porcelain -- mart/token-metrics .github/workflows/release-images-metrics.yml
grep -rn "harbor\.\|@.*\.com" mart/token-metrics/install.sh mart/token-metrics/build.sh mart/token-metrics/k8s \
  mart/token-metrics/tools/rerun.py .github/workflows/release-images-metrics.yml | grep -v "example.internal\|noreply@anthropic.com" ; echo "hosts=$?"
test -x mart/token-metrics/build.sh && test -x mart/token-metrics/install.sh && echo EXEC_OK
```

기대: 첫 `git status` 출력 없음(기존 파일 zero-diff); 둘째는 T8 신규 파일들만(`??`/`A`/`M` — `Dockerfile build.sh k8s/… install.sh tools/rerun.py tests/test_rerun.py tests/test_install_contract.py`, 워크플로); 호스트 grep은 출력 없음 + `hosts=1`(사내 주소는 `--registry`/`--context` 인자로만 — 커밋 0); `EXEC_OK`.

```bash
cd /home/mini/github/token-data-pipeline
git add mart/token-metrics/Dockerfile mart/token-metrics/build.sh mart/token-metrics/k8s \
  mart/token-metrics/install.sh mart/token-metrics/tools/rerun.py \
  mart/token-metrics/tests/test_rerun.py mart/token-metrics/tests/test_install_contract.py \
  .github/workflows/release-images-metrics.yml
git commit -m "feat(mart-metrics): 배포 — Dockerfile/build.sh/k8s CronJob token-mart-metrics(10:20)/install.sh DESCRIBE 프리플라이트/rerun --chunk-days·10:50 창/release-images-metrics (Plan 6c T8)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

커밋 후 `python -m pytest -q`(모듈 전체)가 여전히 PASS이고 `git diff --stat main -- mart/token-usage .github/workflows/release-images.yml`이 비어 있으면 T8 완료. 다음 Task(T9 invariants_metrics)는 이 Task의 CronJob 이름·Secret 키·rerun CLI를 문서(T11)와 CI(T10)가 그대로 인용하므로, 이름을 바꿔야 할 사유가 생기면 여기서 바꾸지 말고 outline 정정 후 T8부터 다시 한다.
### Task 9: tools/verify/invariants_metrics.sql(P0 5 + stretch 3) + run_invariants.py --sql(additive) + 테스트

**설계 근거**: 설계 §7.1 340-342(불변식은 **별도 파일** `tools/verify/invariants_metrics.sql` — 기존 `invariants.sql` 무수정; P0 5블록 이름 `metrics_anchor_missing`/`metrics_gpu_dup_key`/`metrics_serving_dup_key`/`metrics_cost_sum_mismatch`/`created_by_wrong_metrics`(mart 4테이블), stretch 3블록 `share_sum_mismatch`/`group_identity_gap`/`idle_negative`; `metrics_cost_sum_mismatch`는 M1과 **동일 술어**로 재계산; `run_invariants.py --sql` additive — 기본 경로·동작 불변, 사내 분기본에는 `--sql`이 없음), §4.0(`distributed_product_mode=global` + `_dist` 서브쿼리/조인 `GLOBAL` 명시, join_use_nulls=0 규약: 미스 = ''/0/NULL(Nullable)), §7.1(`created_by='token-metrics-pipeline'`), 정의서 `docs/cost-model-spec.md` §8 257-271(I1 idle ≥ 0 → `idle_negative`(over_report=1), I2 그룹 항등식 → `group_identity_gap`(abs(identity_gap_krw) > 1원), I3 Σ_s allocated = C → `share_sum_mismatch`(mode 3종, C NOT NULL, ±1원; I4 `token_not_reported`는 I3의 특수형), Plan 6a ddl/README "쓰기 계약"(digest §19 — `created_by` DEFAULT 없음 + `CONSTRAINT check_created_by CHECK created_by != ''`; 불변식 `created_by_wrong_metrics`가 값 자체를 검사), Plan 6a 인터페이스 표(digest §18 A–H — fact 3 `raw_token_metrics_{gpu,serving,summary}_1d`(ORDER BY `(date, service, model, gpu_type, category)` / `(date, service, model, metric, name)` / `(date, service)`), dim `dim_token_model_alias(alias, effective_from, canonical)`·`dim_token_gpu_tco(gpu_type, effective_from, tco_krw_per_gpu_hour Nullable)`, mart M1 `agg_token_model_cost_1d(model_cost_krw Nullable, has_gpu_rows, created_by)`·M3 `token_metrics_check_1d`·M4 `agg_token_model_share_1d(model, denominator_mode, model_cost_krw Nullable, allocated_cost_krw Nullable)`·M2 `agg_token_gpu_group_1d(service_group, gpu_type, allocated_gpu_hours Nullable, reported_gpu_hours_total, identity_gap_krw Nullable, over_report, tco_missing)`), 마스터 §5.6(detail에 user_id·페이로드 원문 0 — 메트릭 테이블에는 사용자 식별 컬럼 자체가 없다).
**읽을 원형**: `tools/verify/run_invariants.py:1-155`(digest §24.1 — `SQL_PATH`, `render`, `load_sql(path=SQL_PATH)`, `build_arg_parser`, `_print_violations`, `main(argv, client, now_fn)`), `tools/verify/invariants.sql:1-189`(digest §24.2 — 189행 전체: 헤더 주석 형식·치환 토큰 4종·3컬럼 계약·`toUInt64(...)`·`UNION ALL` 체인·마지막 블록 세미콜론 없음), `:141-170`(`identified_name_leak`의 `HAVING count() > 0` 단일 집계 패턴, `created_by_wrong`의 테이블별 2 SELECT `GROUP BY created_by` 패턴), `tools/verify/tests/test_run_invariants.py:1-120`(digest §24.3 — `FakeCH`/`FakeResult`, autouse `clean_ch_env`, `_select_list` 헬퍼, 출력 단언은 부분 문자열), `tools/verify/conftest.py`(sys.path에 모듈 디렉터리 삽입 → `import run_invariants as ri`), `tools/verify/requirements-dev.txt`(`clickhouse-connect>=0.7,<1`, `pytest>=8`), `.github/workflows/test-tools.yml` job `verify-unit`(paths `tools/verify/**`, `python -m pytest tests/ -v` — 신규 파일 자동 커버, 워크플로 무수정), Plan 6c T3 `mart/token-metrics/app/steps.py`의 `canon()`/`FAIL_PRED`/`SUB_EFF_ALIAS`/`SUB_EFF_TCO`/`gpu_agg`(`tco_null_cnt`·`cost_sum`·`has_gpu_rows` — 이 태스크의 재계산 술어가 **문자 그대로** 복제하는 정본; `tools/verify`는 `mart/token-metrics/app`을 import하지 않는다 — 도구 독립성).

이 태스크의 산출물은 셋이다: (1) 불변식 SQL 파일(신규, 11 SELECT = 8 이름 순서 고정), (2) 러너의 `--sql` 옵션(additive 4 hunk — 기본 경로 `SQL_PATH`·`render` 서명·settings·기존 출력 부분 문자열 전부 불변), (3) 정적 계약 테스트(신규 파일 1개, 기존 테스트 파일 무수정). 핵심 원칙:
1. **빈 출력 = 통과** — 모든 블록이 `WHERE`/`HAVING`으로 위반 0건일 때 행 자체를 없앤다. 단일 집계 블록(`count()`만 있는 것)은 반드시 `HAVING count() > 0`.
2. **M1과 같은 문자열 술어** — `metrics_cost_sum_mismatch`의 비용 행 술어 `g.category IN ('serving','standby') AND NOT hasAny(g.flags, ['hours_over_count','unknown_violation'])`, canon `if(a.canonical = '', g.model, a.canonical)`, TCO `nullIf(argMax(ifNull(tco_krw_per_gpu_hour, -1), effective_from), -1)`는 T3 `SQL_M1`의 것을 그대로 옮긴다(테스트가 문자열 존재를 단언). `{d:Date}` 바인딩 대신 러너 토큰 `'{DATE}'` 리터럴을 쓴다(러너 계약 — 서버 바인딩 없음).
3. **신규 테이블만 읽는다** — 토큰 측 `token_usage_1d`·`dim_token_service`·`view_*`는 참조하지 않으므로 `CH_DB_TOKEN_*` 토큰이 필요 없다. 토큰은 기존 4종 `{FACT}/{DIM}/{MART}/{DATE}`뿐.

**Files:**
- Create: `tools/verify/invariants_metrics.sql`
- Create: `tools/verify/tests/test_run_invariants_metrics.py`
- Modify(additive): `tools/verify/run_invariants.py` — 4 hunk: 모듈 docstring 사용법(`:19-21` → `--sql` 용례 2줄 추가), `build_arg_parser()`(`:95-100` → `--sql` 인자 추가), `_print_violations()`(`:103-110` → 선택 인자 `sql_name` 추가·`[FAIL]` 메시지 괄호 안에 `, sql=<파일명>`), `main()`(`:132-133` → `sql_path` 결정·존재 검사·`load_sql(sql_path)`; `:146-150` → `_print_violations(..., sql_path.name)`·PASS 메시지 괄호 안에 `, sql=<파일명>`). `SQL_PATH`·`render`·`load_sql` 서명·`settings={"distributed_product_mode": "global"}`·exit 계약(0/1/2) 불변 — 기존 `tests/test_run_invariants.py` 21건 무수정 통과.
- Test: `tools/verify/tests/test_run_invariants_metrics.py`(신규 20건), `tools/verify/tests/test_run_invariants.py`(기존 21건 — 회귀 확인만, 무수정)
- (읽기만·수정 금지) `tools/verify/invariants.sql`(zero-diff 대상), `tools/verify/conftest.py`, `tools/verify/requirements-dev.txt`, `.github/workflows/test-tools.yml`(job `verify-unit`이 `tools/verify/**`를 이미 커버 — 신규 워크플로·수정 없음)

**Interfaces:**
- Consumes:
  - `run_invariants.py`(기존): `SQL_PATH = pathlib.Path(__file__).resolve().parent / "invariants.sql"`, `render(sql, fact, dim, mart, date) -> str`(`{FACT}/{DIM}/{MART}/{DATE}` 단순 치환), `load_sql(path: pathlib.Path = SQL_PATH) -> str`, `build_arg_parser() -> argparse.ArgumentParser`(`--date`), `main(argv=None, client=None, now_fn=now_kst) -> int`(`client.query(sql, settings={"distributed_product_mode": "global"})` → `result.result_rows` 3튜플 리스트; 위반 시 `_print_violations` 후 1, 아니면 `ALL INVARIANTS PASS (...)` 후 0; 인자 오류 2), `load_db_config()`(`CH_DB_FACT/DIM/MART`, 기본 `fact/gpu_data/mart`).
  - 치환 토큰 4종만: `{FACT}` → `raw_token_metrics_gpu_1d_dist`·`raw_token_metrics_serving_1d_dist`·`raw_token_metrics_summary_1d_dist`; `{DIM}` → `dim_token_model_alias_dist`·`dim_token_gpu_tco_dist`; `{MART}` → `agg_token_model_cost_1d_dist`·`token_metrics_check_1d_dist`·`agg_token_model_share_1d_dist`·`agg_token_gpu_group_1d_dist`; `{DATE}` → `'YYYY-MM-DD'` 리터럴. `CH_DB_TOKEN_*` 토큰은 **추가하지 않는다**(불변식은 신규 테이블만 본다).
  - Plan 6a 컬럼(위 설계 근거의 표 그대로): gpu fact `date, service, model, gpu_type, category, gpu_hours Float64, flags Array(String)`; serving fact `date, service, model, metric, name`; 앵커 `date, service`; alias dim `alias, effective_from, canonical`; TCO dim `gpu_type, effective_from, tco_krw_per_gpu_hour Nullable(Float64)`; M1 `date, service, model, model_cost_krw Nullable(Float64), has_gpu_rows UInt8, created_by`; M3 `date, created_by`; M4 `date, model, denominator_mode, model_cost_krw Nullable, allocated_cost_krw Nullable, created_by`; M2 `date, service_group, gpu_type, allocated_gpu_hours Nullable, reported_gpu_hours_total Float64, identity_gap_krw Nullable, over_report UInt8, tco_missing UInt8, created_by`.
  - 테스트 원형: `tests/test_run_invariants.py`의 `FakeCH(rows)`/`FakeResult(rows)`(`query(sql, parameters=None, settings=None)`가 `queries`·`last_settings` 기록)와 autouse `clean_ch_env` — 이 태스크의 테스트 파일이 **자체 복제**한다(기존 파일 import 없음, 무수정).
- Produces:
  - `tools/verify/invariants_metrics.sql` — 11 SELECT를 `UNION ALL` 10개로 잇는 단일 쿼리, 마지막 SELECT 뒤 세미콜론 없음. 블록 이름과 순서(고정): `metrics_anchor_missing`, `metrics_gpu_dup_key`, `metrics_serving_dup_key`, `metrics_cost_sum_mismatch`, `created_by_wrong_metrics`(×4: `agg_token_model_cost_1d` → `token_metrics_check_1d` → `agg_token_model_share_1d` → `agg_token_gpu_group_1d` 순), `share_sum_mismatch`, `group_identity_gap`, `idle_negative`. 각 SELECT 3컬럼 `'<name>' AS check_name, <String> AS detail, toUInt64(...) AS bad_count`. `_dist` 대상 서브쿼리는 `GLOBAL NOT IN`/`GLOBAL IN`/`GLOBAL LEFT JOIN` 명시. `coalesce(` 0회(NULL 처리는 `ifNull`/`isNull`/`isNotNull`/`nullIf`), `user_id`/`user_name` 0회.
  - `run_invariants.py` 옵션 `--sql <PATH>`(`default=None`, `metavar="PATH"`; 미지정 → `SQL_PATH`; 파일 없음 → `parser.exit(2, f"--sql 파일을 찾을 수 없습니다: {sql_path}\n")`); `_print_violations(rows, date_str, dbs, sql_name: str = "invariants.sql") -> None`(`[FAIL] N건의 불변식 위반 발견 (date=…, DBs=…, sql=<sql_name>)`); PASS 메시지 `ALL INVARIANTS PASS (date=…, DBs=…, sql=<sql_name>)`.
  - `tests/test_run_invariants_metrics.py` 모듈 상수·헬퍼: `METRICS_SQL: pathlib.Path`, `EXPECTED_BLOCKS: list[str]`(8이름), `MART_TABLES: list[str]`(4), `MARKER_RE`(`'(\w+)' AS check_name`), `FakeResult`, `FakeCH`, `clean_ch_env`(autouse), `metrics_sql() -> str`, `_code(sql) -> str`(`--` 주석 줄 제거), `_block(sql, name) -> str`(이름 첫 마커부터 다른 이름의 다음 마커 직전까지). 테스트 20건(이름은 Step 2 코드가 정본).

- [ ] **Step 1: 원형 확인 (수정 금지) + 기존 테스트 기준선**

러너의 `load_sql`/`build_arg_parser`/`main`과 원형 SQL의 3컬럼·`HAVING` 패턴을 눈으로 확인하고, 기존 21건이 통과하는 기준선을 잡는다.

```bash
cd /home/mini/github/token-data-pipeline
sed -n '38p;91,100p;103,110p;132,133p;146,151p' tools/verify/run_invariants.py
sed -n '19,25p;141,147p;154,160p;186,189p' tools/verify/invariants.sql
grep -n "class FakeCH\|class FakeResult\|def clean_ch_env\|last_settings" tools/verify/tests/test_run_invariants.py
grep -n "tools/verify/\*\*\|python -m pytest tests/ -v" .github/workflows/test-tools.yml
cd tools/verify && python -m pytest -q
```

기대: `SQL_PATH = pathlib.Path(__file__).resolve().parent / "invariants.sql"`, `def load_sql(path: pathlib.Path = SQL_PATH) -> str:`, `p.add_argument("--date", default=None,`, `def _print_violations(rows: list[tuple], date_str: str, dbs: dict) -> None:`, `sql = render(load_sql(), fact=dbs["fact"], ...)`, `print(f"ALL INVARIANTS PASS (date={date_str}, DBs=...")`; 원형 SQL에서 `-- 컬럼 계약: 전 SELECT가 3컬럼(check_name String, detail String, bad_count UInt64)`, `HAVING count() > 0`, `GROUP BY created_by`, 마지막 블록 끝에 세미콜론 없음; 테스트 파일에서 `class FakeResult`/`class FakeCH`/`def clean_ch_env`/`self.last_settings = settings`; 워크플로에서 `- "tools/verify/**"` 와 `python -m pytest tests/ -v`(job `verify-unit` — 신규 파일이 자동 포함); 마지막 줄 `21 passed`.

- [ ] **Step 2: 실패하는 테스트 — `tools/verify/tests/test_run_invariants_metrics.py` 신규(20건, 전체 RED)**

기존 `tests/test_run_invariants.py`는 건드리지 않고, 같은 스타일의 `FakeCH`/`FakeResult`/`clean_ch_env`를 이 파일 안에 복제한다(파일 간 import 없음). 정적 검사는 `_code()`로 `--` 주석 줄을 뗀 본문에 대해서만 수행한다(주석 속 단어가 계약 검사에 섞이지 않도록). 아래 전체를 새 파일로 쓴다.

```python
"""tools/verify/tests/test_run_invariants_metrics.py — Plan 6c T9
run_invariants.py --sql 라우팅(additive) + invariants_metrics.sql 정적 계약.

- 라우팅: --sql 미지정 → SQL_PATH(기존 동작 불변), 지정 → 그 파일, 파일 없음 → exit 2.
- 정적 계약: 8블록 순서·3컬럼·토큰 4종·M1 동일 술어·coalesce/사용자 식별자 0·
  created_by 4테이블·stretch 3블록 술어·신규 테이블만 참조·SELECT 전용·GLOBAL 명시.
- CH 접속 없음(FakeCH). 기존 tests/test_run_invariants.py는 import하지 않는다(무수정 원칙).
"""
import pathlib
import re

import pytest

import run_invariants as ri

METRICS_SQL = pathlib.Path(ri.__file__).resolve().parent / "invariants_metrics.sql"

EXPECTED_BLOCKS = [
    "metrics_anchor_missing",
    "metrics_gpu_dup_key",
    "metrics_serving_dup_key",
    "metrics_cost_sum_mismatch",
    "created_by_wrong_metrics",
    "share_sum_mismatch",
    "group_identity_gap",
    "idle_negative",
]
MART_TABLES = [
    "agg_token_model_cost_1d",
    "token_metrics_check_1d",
    "agg_token_model_share_1d",
    "agg_token_gpu_group_1d",
]
MARKER_RE = re.compile(r"'(\w+)' AS check_name")

# T3 SQL_M1 gpu_agg 와 문자 그대로 같아야 하는 조각(도구 독립성 — import 대신 문자열 대조)
M1_COST_PRED = ("g.category IN ('serving','standby') "
                "AND NOT hasAny(g.flags, ['hours_over_count','unknown_violation'])")
M1_CANON = "if(a.canonical = '', g.model, a.canonical)"
M1_TCO = "nullIf(argMax(ifNull(tco_krw_per_gpu_hour, -1), effective_from), -1) AS tco"


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeCH:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.queries = []
        self.last_settings = None

    def query(self, sql, parameters=None, settings=None):
        self.queries.append(sql)
        self.last_settings = settings
        return FakeResult(self.rows)


@pytest.fixture(autouse=True)
def clean_ch_env(monkeypatch):
    for k in ("CH_DB_FACT", "CH_DB_DIM", "CH_DB_MART",
              "CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_CLUSTER"):
        monkeypatch.delenv(k, raising=False)


def metrics_sql() -> str:
    return METRICS_SQL.read_text(encoding="utf-8")


def _code(sql: str) -> str:
    """`--` 주석 줄을 제거한 SQL 본문."""
    return "\n".join(line for line in sql.splitlines()
                     if not line.lstrip().startswith("--"))


def _block(sql: str, name: str) -> str:
    """이름의 첫 마커부터 다른 이름의 다음 마커 직전까지(같은 이름의 연속 SELECT는 한 블록)."""
    markers = list(MARKER_RE.finditer(sql))
    starts = [m for m in markers if m.group(1) == name]
    assert starts, f"block marker not found: {name}"
    start = starts[0].start()
    end = len(sql)
    for m in markers:
        if m.start() > start and m.group(1) != name:
            end = m.start()
            break
    return sql[start:end]


# ---------------------------------------------------------------------------
# 1) --sql 라우팅 (run_invariants.py additive)
# ---------------------------------------------------------------------------

def test_sql_flag_default_is_invariants_sql(monkeypatch):
    assert ri.build_arg_parser().parse_args([]).sql is None
    seen = []

    def fake_load(path=ri.SQL_PATH):
        seen.append(path)
        return ("SELECT 'x' AS check_name, 'd' AS detail, toUInt64(1) AS bad_count "
                "FROM {FACT}.t WHERE date = '{DATE}'")

    monkeypatch.setattr(ri, "load_sql", fake_load)
    assert ri.main(["--date", "2026-09-03"], client=FakeCH([])) == 0
    assert seen == [ri.SQL_PATH]


def test_sql_flag_loads_metrics_file(capsys):
    fake = FakeCH([])
    rc = ri.main(["--sql", str(METRICS_SQL), "--date", "2026-09-03"], client=fake)
    assert rc == 0
    assert len(fake.queries) == 1
    sent = fake.queries[0]
    assert "'metrics_anchor_missing' AS check_name" in sent
    assert "'idle_negative' AS check_name" in sent
    for tok in ("{FACT}", "{DIM}", "{MART}", "{DATE}"):
        assert tok not in sent
    assert "date = '2026-09-03'" in sent
    assert "fact.raw_token_metrics_summary_1d_dist" in sent
    assert "gpu_data.dim_token_gpu_tco_dist" in sent
    assert "mart.agg_token_gpu_group_1d_dist" in sent
    assert fake.last_settings == {"distributed_product_mode": "global"}
    out = capsys.readouterr().out
    assert "ALL INVARIANTS PASS" in out
    assert "sql=invariants_metrics.sql" in out


def test_sql_flag_relative_path_resolved_from_cwd(tmp_path, monkeypatch, capsys):
    body = ("SELECT 'x' AS check_name, 'd' AS detail, toUInt64(1) AS bad_count\n"
            "FROM {MART}.t WHERE date = '{DATE}'\n")
    (tmp_path / "custom.sql").write_text(body, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    fake = FakeCH([])
    assert ri.main(["--sql", "custom.sql", "--date", "2026-09-03"], client=fake) == 0
    assert fake.queries == [
        "SELECT 'x' AS check_name, 'd' AS detail, toUInt64(1) AS bad_count\n"
        "FROM mart.t WHERE date = '2026-09-03'\n"
    ]
    assert "sql=custom.sql" in capsys.readouterr().out


def test_sql_flag_missing_file_exit2(tmp_path, capsys):
    missing = tmp_path / "nonexistent.sql"
    with pytest.raises(SystemExit) as e:
        ri.main(["--sql", str(missing), "--date", "2026-09-03"], client=FakeCH([]))
    assert e.value.code == 2
    err = capsys.readouterr().err
    assert "--sql 파일을 찾을 수 없습니다" in err
    assert str(missing) in err


def test_default_run_message_names_default_sql(monkeypatch, capsys):
    monkeypatch.setattr(ri, "load_sql", lambda path=ri.SQL_PATH: "SELECT 1")
    assert ri.main(["--date", "2026-09-03"], client=FakeCH([])) == 0
    out = capsys.readouterr().out
    assert ("ALL INVARIANTS PASS (date=2026-09-03, DBs=fact/gpu_data/mart, "
            "sql=invariants.sql)") in out


def test_violation_rows_printed_and_exit1(capsys):
    fake = FakeCH([("idle_negative", "grpA/H100 reported=30 allocated=24", 1),
                   ("group_identity_gap", "grpB/A100 gap=12.5", 1)])
    rc = ri.main(["--sql", str(METRICS_SQL), "--date", "2026-09-03"], client=fake)
    assert rc == 1
    out = capsys.readouterr().out
    assert ("[FAIL] 2건의 불변식 위반 발견 (date=2026-09-03, DBs=fact/gpu_data/mart, "
            "sql=invariants_metrics.sql)") in out
    assert "idle_negative" in out
    assert "grpA/H100 reported=30 allocated=24" in out
    assert "group_identity_gap" in out


# ---------------------------------------------------------------------------
# 2) invariants_metrics.sql 정적 계약
# ---------------------------------------------------------------------------

def test_metrics_sql_has_eight_blocks_in_order():
    sql = _code(metrics_sql())
    names = MARKER_RE.findall(sql)
    assert list(dict.fromkeys(names)) == EXPECTED_BLOCKS
    assert names.count("created_by_wrong_metrics") == 4
    assert len(names) == 11
    assert sql.count("UNION ALL") == 10
    assert not sql.rstrip().endswith(";")


def test_metrics_sql_tokens_only_known_four():
    sql = metrics_sql()
    tokens = set(re.findall(r"\{[A-Za-z_:]+\}", sql))
    assert tokens == {"{FACT}", "{DIM}", "{MART}", "{DATE}"}
    rendered = ri.render(sql, fact="vf", dim="vd", mart="vm", date="2026-09-03")
    assert re.findall(r"\{[A-Za-z_:]+\}", rendered) == []
    assert "CH_DB_TOKEN" not in sql


def test_metrics_sql_three_column_contract():
    sql = _code(metrics_sql())
    selects = sql.split("UNION ALL")
    assert len(selects) == 11
    for sel in selects:
        head = sel[:sel.index("\nFROM")]
        assert MARKER_RE.search(head), head
        assert " AS detail" in head, head
        assert re.search(r"toUInt64\(.+\) AS bad_count", head, re.S), head
        assert "'{DATE}'" in sel, sel


def test_metrics_cost_predicate_matches_m1():
    blk = _block(_code(metrics_sql()), "metrics_cost_sum_mismatch")
    assert blk.count(M1_COST_PRED) == 2
    assert M1_CANON in blk
    assert f"GROUP BY g.service, {M1_CANON}" in blk
    assert M1_TCO in blk
    assert "argMax(canonical, effective_from) AS canonical" in blk
    assert blk.count("effective_from <= '{DATE}'") == 2
    assert "has_gpu_rows = 1" in blk
    cost_slice = blk[blk.index("sumIf(g.gpu_hours * t.tco"):blk.index("AS fact_cost")]
    assert "'test'" not in cost_slice
    assert "isNull(t.tco)" in blk
    assert "isNotNull(t.tco)" in blk
    assert "raw_token_metrics_summary_1d_dist" in blk
    assert "abs(ifNull(m.model_cost_krw, 0) - ifNull(f.fact_cost, 0)) > 1" in blk
    assert "isNull(m.model_cost_krw) != (f.tco_null_cnt > 0)" in blk


def test_metrics_sql_no_coalesce_and_no_user_id():
    sql = metrics_sql()
    assert "coalesce(" not in sql.lower()
    assert "user_id" not in sql
    assert "user_name" not in sql


def test_created_by_block_covers_four_mart_tables():
    code = _code(metrics_sql())
    blk = _block(code, "created_by_wrong_metrics")
    assert blk.count("'created_by_wrong_metrics' AS check_name") == 4
    for t in MART_TABLES:
        assert f"FROM {{MART}}.{t}_dist" in blk
        assert f"concat('table={t} created_by=', created_by)" in blk
    positions = [blk.index(f"FROM {{MART}}.{t}_dist") for t in MART_TABLES]
    assert positions == sorted(positions)
    assert blk.count("created_by != 'token-metrics-pipeline'") == 4
    assert blk.count("GROUP BY created_by") == 4
    assert "'token-pipeline'" not in code


def test_group_identity_gap_excludes_only_tco_missing():
    blk = _block(_code(metrics_sql()), "group_identity_gap")
    assert "FROM {MART}.agg_token_gpu_group_1d_dist" in blk
    assert "over_report" not in blk          # 설계 §7.1: I2 = abs(gap) > 1, over_report 면제 없음
    assert "tco_missing = 0" in blk
    assert "isNotNull(identity_gap_krw)" in blk
    assert "abs(identity_gap_krw) > 1" in blk


def test_idle_negative_is_over_report_rows():
    blk = _block(_code(metrics_sql()), "idle_negative")
    assert "FROM {MART}.agg_token_gpu_group_1d_dist" in blk
    assert "over_report = 1" in blk
    assert "reported=" in blk
    assert "allocated=" in blk


def test_share_sum_mismatch_modes_and_null_rule():
    blk = _block(_code(metrics_sql()), "share_sum_mismatch")
    assert "FROM {MART}.agg_token_model_share_1d_dist" in blk
    assert "denominator_mode IN ('all_services','provider_reported','token_not_reported')" in blk
    assert "isNotNull(model_cost_krw)" in blk
    assert "GROUP BY model" in blk
    assert ("HAVING abs(ifNull(sum(allocated_cost_krw), 0) "
            "- ifNull(any(model_cost_krw), 0)) > 1") in blk


def test_dup_key_blocks_group_by_full_order_by_key():
    code = _code(metrics_sql())
    gpu = _block(code, "metrics_gpu_dup_key")
    srv = _block(code, "metrics_serving_dup_key")
    assert "FROM {FACT}.raw_token_metrics_gpu_1d_dist" in gpu
    assert "GROUP BY service, model, gpu_type, category" in gpu
    assert "FROM {FACT}.raw_token_metrics_serving_1d_dist" in srv
    assert "GROUP BY service, model, metric, name" in srv
    for blk in (gpu, srv):
        assert "HAVING n > 1" in blk
        assert "HAVING count() > 0" in blk


def test_anchor_block_unions_children_and_global_not_in():
    blk = _block(_code(metrics_sql()), "metrics_anchor_missing")
    assert "FROM {FACT}.raw_token_metrics_gpu_1d_dist WHERE date = '{DATE}'" in blk
    assert "FROM {FACT}.raw_token_metrics_serving_1d_dist WHERE date = '{DATE}'" in blk
    assert "UNION DISTINCT" in blk
    assert "GLOBAL NOT IN" in blk
    assert "FROM {FACT}.raw_token_metrics_summary_1d_dist WHERE date = '{DATE}'" in blk
    assert "HAVING count() > 0" in blk


def test_metrics_sql_reads_only_new_tables():
    code = _code(metrics_sql())
    tables = set(re.findall(r"\{(?:FACT|DIM|MART)\}\.([a-z_0-9]+)", code))
    assert tables == {
        "raw_token_metrics_gpu_1d_dist",
        "raw_token_metrics_serving_1d_dist",
        "raw_token_metrics_summary_1d_dist",
        "dim_token_model_alias_dist",
        "dim_token_gpu_tco_dist",
        "agg_token_model_cost_1d_dist",
        "token_metrics_check_1d_dist",
        "agg_token_model_share_1d_dist",
        "agg_token_gpu_group_1d_dist",
    }
    for legacy in ("token_usage_1d", "view_token_usage",
                   "dim_token_service_dist", "dim_token_model_dist"):
        assert legacy not in code


def test_metrics_sql_is_select_only():
    code = _code(metrics_sql())
    assert re.search(r"\b(INSERT|ALTER|DELETE|DROP|TRUNCATE|OPTIMIZE)\b", code, re.I) is None
    assert code.lstrip().startswith("SELECT")


def test_global_join_discipline():
    code = _code(metrics_sql())
    assert re.search(r"(?<!GLOBAL )LEFT JOIN", code) is None
    assert re.search(r"(?<!GLOBAL )(?<!GLOBAL NOT )\bIN \(\s*SELECT", code) is None
    assert code.count("GLOBAL LEFT JOIN") == 3
    assert code.count("GLOBAL IN (") == 1
    assert code.count("GLOBAL NOT IN (") == 1
```

```bash
cd /home/mini/github/token-data-pipeline/tools/verify && python -m pytest tests/test_run_invariants_metrics.py -q 2>&1 | tail -25
```

기대: `20 failed` — 라우팅 6건은 `AttributeError: 'Namespace' object has no attribute 'sql'`(기본값 테스트), `SystemExit: 2`(`--sql` 미인식 — `unrecognized arguments: --sql`; `test_sql_flag_missing_file_exit2`는 exit 코드 2는 맞지만 stderr에 `--sql 파일을 찾을 수 없습니다`가 없어 실패), `assert (...sql=invariants.sql)) in out`(메시지에 `sql=` 없음); 정적 14건은 `FileNotFoundError: [Errno 2] No such file or directory: '.../tools/verify/invariants_metrics.sql'`. 마지막 줄 `20 failed in …`.

- [ ] **Step 3: `tools/verify/invariants_metrics.sql` — 헤더 + P0 5블록(SELECT 8개)**

원형 `invariants.sql` 헤더 형식을 복제하고 P0 5블록을 쓴다. 규칙: 바깥 `FROM`은 항상 열 0(테스트가 `\nFROM`으로 헤더를 자른다), 서브쿼리 `FROM`은 들여쓰기; 코드 줄 끝에 인라인 주석을 두지 않는다(주석은 `--`로 시작하는 독립 줄만 — 테스트 `_code()`가 그 줄만 제거한다); 주석에도 `coalesce(`·사용자 식별 컬럼명·따옴표 친 옛 파이프라인 이름을 쓰지 않는다. 아래 전체를 새 파일로 쓴다(Step 4가 stretch 3블록을 파일 끝에 이어 붙인다).

```sql
-- =============================================================
-- 메트릭 파이프라인(/v1/metrics 반입, Plan 6a~6c) 검증 불변식 — 설계 §7.3
-- tools/verify/invariants.sql(토큰 파이프라인)의 메트릭판. 기존 파일은 손대지 않고
-- run_invariants.py --sql 로 이 파일을 지정해 실행한다.
--
-- 실데이터 검증이므로 고정 기대값이 아니라 불변식으로 판정한다 —
-- tools/verify/invariants.sql·mart/token-usage/tests/e2e/verify_expected_results.sql과
-- 동일 패턴: **빈 출력 = 전건 통과.** 어떤 SELECT든 1행이라도 나오면 그 행이 위반이다.
--
-- 치환 토큰 (run_invariants.py가 render()로 치환, 잔존 시 실행 실패) — 기존 4종만 쓴다:
--   {FACT}  — 메트릭 fact DB명 (기본 fact,     1단계 격리 시 token_verify_fact)
--             raw_token_metrics_gpu_1d / raw_token_metrics_serving_1d / raw_token_metrics_summary_1d
--   {DIM}   — 메트릭 dim  DB명 (기본 gpu_data, 1단계 격리 시 token_verify_dim)
--             dim_token_model_alias / dim_token_gpu_tco
--   {MART}  — 메트릭 mart DB명 (기본 mart,     1단계 격리 시 token_verify_mart)
--             agg_token_model_cost_1d / token_metrics_check_1d /
--             agg_token_model_share_1d / agg_token_gpu_group_1d
--   {DATE}  — 대상일 'YYYY-MM-DD' (단일 날짜, 리터럴 문자열로 치환)
--   토큰 측 DB 토큰은 없다 — 이 파일은 메트릭 파이프라인의 신규 테이블만 읽는다.
--
-- 사용법 (GitHub 체크아웃 기준):
--   python3 tools/verify/run_invariants.py --sql tools/verify/invariants_metrics.sql --date 2026-09-03
--   사내 분기본 run_invariants.py에는 --sql 이 없다 — 이 파일은 GitHub본 러너로만 실행한다.
--   (또는 치환 후 clickhouse-client --multiquery로 직접 실행 — 읽기 전용 SELECT만)
--
-- 컬럼 계약: 전 SELECT가 3컬럼(check_name String, detail String, bad_count UInt64)
-- 로 통일 — bad_count는 전부 toUInt64(...), detail은 전부 String(리터럴 또는
-- concat/toString). UNION ALL 체인에서 타입이 갈라지면 NO_COMMON_TYPE으로 실패한다.
-- 단일 집계 블록은 HAVING count() > 0 으로 위반 0건일 때 행 자체를 없앤다.
--
-- 분산 규약(설계 §4.0): _dist 대상 서브쿼리/조인은 GLOBAL IN / GLOBAL NOT IN /
-- GLOBAL LEFT JOIN 을 명시한다(러너의 distributed_product_mode=global 과 이중 안전).
-- join_use_nulls=0 규약: LEFT JOIN 미스는 ''/0, Nullable 컬럼은 NULL.
-- NULL 처리는 ifNull / isNull / isNotNull / nullIf 만 쓴다(Plan 6c 공통 규칙).
-- detail에는 서비스·모델·기종·그룹명과 집계값만 싣는다 — 사용자 식별자·페이로드 원문 0
-- (마스터 §5.6; 메트릭 테이블에는 사용자 식별 컬럼 자체가 없다).
--
-- 블록(순서 고정, UNION ALL 로 이은 11 SELECT = 8 이름; created_by_wrong_metrics 는 테이블별 4 SELECT):
--   P0      1) metrics_anchor_missing   2) metrics_gpu_dup_key   3) metrics_serving_dup_key
--           4) metrics_cost_sum_mismatch   5) created_by_wrong_metrics x4
--   stretch 6) share_sum_mismatch(정의서 §8 I3·I4)   7) group_identity_gap(I2)   8) idle_negative(I1)
--
-- 이 파일은 검증(SELECT)만 수행한다 — 쓰기 구문 없음.
-- =============================================================

-- 1) metrics_anchor_missing — 자식 팩트(gpu ∪ serving)에 (date, service)가 있는데 앵커
--    {FACT}.raw_token_metrics_summary_1d_dist 에 같은 (date, service) 행이 없다.
--    Plan 6a 쓰기 계약: 앵커는 서비스당 정확히 1행이고 자식 행은 앵커 없이 존재할 수 없다 —
--    M1/M3 의 앵커 필터가 이런 서비스를 조용히 제외하므로 여기서 잡는다.
--    서비스명은 사용자 식별자가 아니므로 detail 에 정렬 나열한다.
SELECT
    'metrics_anchor_missing' AS check_name,
    concat('services=', arrayStringConcat(arraySort(groupUniqArray(service)), ',')) AS detail,
    toUInt64(uniqExact(service)) AS bad_count
FROM
(
    SELECT service FROM {FACT}.raw_token_metrics_gpu_1d_dist WHERE date = '{DATE}'
    UNION DISTINCT
    SELECT service FROM {FACT}.raw_token_metrics_serving_1d_dist WHERE date = '{DATE}'
) AS c
WHERE service GLOBAL NOT IN (
    SELECT service FROM {FACT}.raw_token_metrics_summary_1d_dist WHERE date = '{DATE}'
)
HAVING count() > 0

UNION ALL

-- 2) metrics_gpu_dup_key — gpu 팩트 ORDER BY 키 (date, service, model, gpu_type, category)
--    중복. Plan 6a 재적재 계약(삭제 → 적재)이 깨지면 같은 키가 2행 이상 남는다.
--    안쪽에서 키별 행 수 n > 1 을 세고 바깥에서 1행으로 접는다
--    (dup_keys = 중복 키 수, extra_rows = 키당 초과 행 수의 합).
SELECT
    'metrics_gpu_dup_key' AS check_name,
    concat('dup_keys=', toString(count()), ' extra_rows=', toString(sum(n - 1))) AS detail,
    toUInt64(count()) AS bad_count
FROM
(
    SELECT service, model, gpu_type, category, count() AS n
    FROM {FACT}.raw_token_metrics_gpu_1d_dist
    WHERE date = '{DATE}'
    GROUP BY service, model, gpu_type, category
    HAVING n > 1
) AS d
HAVING count() > 0

UNION ALL

-- 3) metrics_serving_dup_key — serving 팩트 ORDER BY 키 (date, service, model, metric, name)
--    중복. 한 (service, model)에 여러 metric/name 행이 있는 것은 정상이므로 키 전체로 본다.
SELECT
    'metrics_serving_dup_key' AS check_name,
    concat('dup_keys=', toString(count()), ' extra_rows=', toString(sum(n - 1))) AS detail,
    toUInt64(count()) AS bad_count
FROM
(
    SELECT service, model, metric, name, count() AS n
    FROM {FACT}.raw_token_metrics_serving_1d_dist
    WHERE date = '{DATE}'
    GROUP BY service, model, metric, name
    HAVING n > 1
) AS d
HAVING count() > 0

UNION ALL

-- 4) metrics_cost_sum_mismatch — M1 {MART}.agg_token_model_cost_1d_dist 의 model_cost_krw
--    (has_gpu_rows = 1 행)와 gpu 팩트 재계산 C 의 대사. 재계산 술어는 mart/token-metrics/app/
--    steps.py SQL_M1 gpu_agg 와 문자 그대로 같다(도구 독립성 — import 대신 문자열 복제):
--      canon = if(a.canonical = '', g.model, a.canonical),
--      eff_alias / eff_tco = effective_from <= date 최신 행 argMax(TCO 최신 행 NULL → NULL),
--      비용 행 = category IN ('serving','standby') AND NOT hasAny(g.flags, FAIL 2종),
--      앵커 있는 서비스만, test 시간 불포함.
--    NULL 규칙(설계 §6.4 (1) 부분 합 금지)까지 대칭으로 대사한다:
--      null_mismatch  = mart 의 NULL 여부 != (재계산 tco_null_cnt > 0)
--      value_mismatch = 둘 다 값이 있는데 abs 차이 > 1원
--    mart 주도 GLOBAL LEFT JOIN(키 누락은 M1 자체 verify_count 가 담당). 위반 pair 수를 1행으로.
SELECT
    'metrics_cost_sum_mismatch' AS check_name,
    concat('pairs=', toString(count()),
           ' null_rule=', toString(countIf(null_mismatch = 1)),
           ' value=', toString(countIf(value_mismatch = 1))) AS detail,
    toUInt64(count()) AS bad_count
FROM
(
    SELECT
        toUInt8(isNull(m.model_cost_krw) != (f.tco_null_cnt > 0)) AS null_mismatch,
        toUInt8(isNotNull(m.model_cost_krw) AND f.tco_null_cnt = 0
                AND abs(ifNull(m.model_cost_krw, 0) - ifNull(f.fact_cost, 0)) > 1) AS value_mismatch
    FROM
    (
        SELECT service, model, model_cost_krw
        FROM {MART}.agg_token_model_cost_1d_dist
        WHERE date = '{DATE}' AND has_gpu_rows = 1
    ) AS m
    GLOBAL LEFT JOIN
    (
        SELECT
            g.service AS service,
            if(a.canonical = '', g.model, a.canonical) AS canon_model,
            countIf(g.category IN ('serving','standby') AND NOT hasAny(g.flags, ['hours_over_count','unknown_violation']) AND isNull(t.tco)) AS tco_null_cnt,
            sumIf(g.gpu_hours * t.tco, g.category IN ('serving','standby') AND NOT hasAny(g.flags, ['hours_over_count','unknown_violation']) AND isNotNull(t.tco)) AS fact_cost
        FROM {FACT}.raw_token_metrics_gpu_1d_dist AS g
        GLOBAL LEFT JOIN
        (
            SELECT alias, argMax(canonical, effective_from) AS canonical
            FROM {DIM}.dim_token_model_alias_dist
            WHERE effective_from <= '{DATE}'
            GROUP BY alias
        ) AS a ON a.alias = g.model
        GLOBAL LEFT JOIN
        (
            SELECT gpu_type,
                   nullIf(argMax(ifNull(tco_krw_per_gpu_hour, -1), effective_from), -1) AS tco
            FROM {DIM}.dim_token_gpu_tco_dist
            WHERE effective_from <= '{DATE}'
            GROUP BY gpu_type
        ) AS t ON t.gpu_type = g.gpu_type
        WHERE g.date = '{DATE}'
          AND g.service GLOBAL IN (
              SELECT service FROM {FACT}.raw_token_metrics_summary_1d_dist WHERE date = '{DATE}'
          )
        GROUP BY g.service, if(a.canonical = '', g.model, a.canonical)
    ) AS f ON f.service = m.service AND f.canon_model = m.model
) AS x
WHERE null_mismatch = 1 OR value_mismatch = 1
HAVING count() > 0

UNION ALL

-- 5) created_by_wrong_metrics — mart 4테이블에서 created_by != 'token-metrics-pipeline'
--    (Plan 6a 쓰기 계약: DEFAULT 없음 + CHECK created_by != '' — 값 자체는 이 불변식이 검사;
--    설계 §7.1). 테이블별 SELECT 4개, created_by 값별 GROUP BY 로 위반 값당 1행.
SELECT
    'created_by_wrong_metrics' AS check_name,
    concat('table=agg_token_model_cost_1d created_by=', created_by) AS detail,
    toUInt64(count()) AS bad_count
FROM {MART}.agg_token_model_cost_1d_dist
WHERE date = '{DATE}' AND created_by != 'token-metrics-pipeline'
GROUP BY created_by

UNION ALL

SELECT
    'created_by_wrong_metrics' AS check_name,
    concat('table=token_metrics_check_1d created_by=', created_by) AS detail,
    toUInt64(count()) AS bad_count
FROM {MART}.token_metrics_check_1d_dist
WHERE date = '{DATE}' AND created_by != 'token-metrics-pipeline'
GROUP BY created_by

UNION ALL

SELECT
    'created_by_wrong_metrics' AS check_name,
    concat('table=agg_token_model_share_1d created_by=', created_by) AS detail,
    toUInt64(count()) AS bad_count
FROM {MART}.agg_token_model_share_1d_dist
WHERE date = '{DATE}' AND created_by != 'token-metrics-pipeline'
GROUP BY created_by

UNION ALL

SELECT
    'created_by_wrong_metrics' AS check_name,
    concat('table=agg_token_gpu_group_1d created_by=', created_by) AS detail,
    toUInt64(count()) AS bad_count
FROM {MART}.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}' AND created_by != 'token-metrics-pipeline'
GROUP BY created_by
```

```bash
cd /home/mini/github/token-data-pipeline/tools/verify
grep -c "AS check_name" invariants_metrics.sql
grep -c "^UNION ALL$" invariants_metrics.sql
grep -n "^FROM\|^HAVING count() > 0" invariants_metrics.sql | wc -l
python -m pytest tests/test_run_invariants_metrics.py -q 2>&1 | tail -12
```

기대: `8`, `7`, `12`(바깥 `FROM` 8 + `HAVING count() > 0` 4); pytest 마지막 줄 `11 failed, 9 passed` — 통과 9 = `test_metrics_sql_tokens_only_known_four`, `test_metrics_cost_predicate_matches_m1`, `test_metrics_sql_no_coalesce_and_no_user_id`, `test_created_by_block_covers_four_mart_tables`, `test_dup_key_blocks_group_by_full_order_by_key`, `test_anchor_block_unions_children_and_global_not_in`, `test_metrics_sql_reads_only_new_tables`, `test_metrics_sql_is_select_only`, `test_global_join_discipline`; 실패 11 = 라우팅 6건(Step 2와 같은 사유) + stretch 블록 부재로 인한 5건 (`test_metrics_sql_has_eight_blocks_in_order` — 이름 5개 != `EXPECTED_BLOCKS` 8개, `test_metrics_sql_three_column_contract` — `len(selects) == 11`인데 현재 8, `test_group_identity_gap_excludes_only_tco_missing`/`test_idle_negative_is_over_report_rows`/`test_share_sum_mismatch_modes_and_null_rule` — `block marker not found`).

- [ ] **Step 4: `invariants_metrics.sql` — stretch 3블록 append(정의서 §8 I3 → I2 → I1)**

Step 3 파일의 **끝**(마지막 `GROUP BY created_by` 줄 뒤)에 아래를 이어 붙인다. 첫 줄은 빈 줄, 그 다음 `UNION ALL`. 마지막 SELECT 뒤에 세미콜론을 두지 않는다.

```sql

UNION ALL

-- 6) share_sum_mismatch (stretch, 정의서 §8 I3: 모델 m 의 Σ_s allocated_cost(s, m) = C(m),
--    서비스 미보고분 보정 없음; I4 token_not_reported 는 제공자 행 share=1·allocated=C 인 특수형).
--    {MART}.agg_token_model_share_1d_dist 는 (date, model, service) grain 이고 denominator_mode
--    는 모델당 하나이므로 model 로 GROUP BY 하면 그 모델의 서비스 행이 모두 접힌다.
--    대상: mode 3종(all_services / provider_reported / token_not_reported) × model_cost_krw
--    NOT NULL(C NULL 모델은 배분값도 NULL 로 정의상 합이 없다). provider_ambiguous 는 배분
--    NULL, no_provider 는 C=0·배분 0, external_api 는 벤더 단가식이라 I3 대상이 아니다.
--    provider_reported 에서 소비자 토큰이 제공자 보고분을 넘으면(M3 consumer_tokens_exceed_provider
--    WARN) Σ가 C 를 넘어 여기서도 잡힌다 — 그 경우 M3 WARN 과 대조해 같은 모델인지 확인한다.
SELECT
    'share_sum_mismatch' AS check_name,
    concat('model=', model,
           ' mode=', any(denominator_mode),
           ' sum_allocated=', toString(round(ifNull(sum(allocated_cost_krw), 0), 2)),
           ' model_cost=', toString(round(ifNull(any(model_cost_krw), 0), 2))) AS detail,
    toUInt64(count()) AS bad_count
FROM {MART}.agg_token_model_share_1d_dist
WHERE date = '{DATE}'
  AND denominator_mode IN ('all_services','provider_reported','token_not_reported')
  AND isNotNull(model_cost_krw)
GROUP BY model
HAVING abs(ifNull(sum(allocated_cost_krw), 0) - ifNull(any(model_cost_krw), 0)) > 1

UNION ALL

-- 7) group_identity_gap (stretch, 정의서 §8 I2: 그룹 총비용 = Σ 모델비용 + 테스트 + 유휴 +
--    미귀속). {MART}.agg_token_gpu_group_1d_dist 의 identity_gap_krw(M2 가 계산한 좌우변 차)
--    가 ±1원을 넘으면 위반 — 설계 §7.1 그대로, over_report 면제 없음. over_report = 1 행은
--    idle 클램프(정의서 I1)로 gap 이 (할당 − 보고) × TCO 만큼 생기므로 여기와 8) idle_negative
--    에 함께 잡힌다(둘 다 위반이 맞다 — 항등식과 idle ≥ 0 이 동시에 깨진 상태). tco_missing = 1
--    행은 항등식의 항 자체가 NULL 이라 판정 불가(M2 quality_flag no_tco)이므로 제외.
SELECT
    'group_identity_gap' AS check_name,
    concat(service_group, '/', gpu_type,
           ' gap=', toString(round(ifNull(identity_gap_krw, 0), 2))) AS detail,
    toUInt64(1) AS bad_count
FROM {MART}.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}'
  AND tco_missing = 0
  AND isNotNull(identity_gap_krw)
  AND abs(identity_gap_krw) > 1

UNION ALL

-- 8) idle_negative (stretch, 정의서 §8 I1: idle_gpu_hours ≥ 0). M2 는 보고 시간이 할당 시간을
--    넘으면 idle 을 0 으로 클램프하고 over_report = 1 을 세운다 — 그 행이 곧 I1 위반 후보
--    (할당 dim 이 실제보다 작거나 팩트 시간이 과다 보고). 행당 1건, 보고/할당 시간을 노출.
SELECT
    'idle_negative' AS check_name,
    concat(service_group, '/', gpu_type,
           ' reported=', toString(round(reported_gpu_hours_total, 2)),
           ' allocated=', toString(round(ifNull(allocated_gpu_hours, 0), 2))) AS detail,
    toUInt64(1) AS bad_count
FROM {MART}.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}'
  AND over_report = 1
```

```bash
cd /home/mini/github/token-data-pipeline/tools/verify
grep -c "AS check_name" invariants_metrics.sql
grep -c "^UNION ALL$" invariants_metrics.sql
tail -c 1 invariants_metrics.sql | od -c | head -1
grep -o "'[a-z_]*' AS check_name" invariants_metrics.sql | awk '!seen[$0]++' | tr '\n' ' '; echo
python -m pytest tests/test_run_invariants_metrics.py -q 2>&1 | tail -8
```

기대: `11`, `10`, `0000000  \n`(파일 끝은 개행, 세미콜론 없음), 이름 순서 `'metrics_anchor_missing' AS check_name 'metrics_gpu_dup_key' AS check_name 'metrics_serving_dup_key' AS check_name 'metrics_cost_sum_mismatch' AS check_name 'created_by_wrong_metrics' AS check_name 'share_sum_mismatch' AS check_name 'group_identity_gap' AS check_name 'idle_negative' AS check_name`; pytest 마지막 줄 `6 failed, 14 passed` — 정적 14건 전부 통과, 남은 실패는 라우팅 6건(`test_sql_flag_default_is_invariants_sql`, `test_sql_flag_loads_metrics_file`, `test_sql_flag_relative_path_resolved_from_cwd`, `test_sql_flag_missing_file_exit2`, `test_default_run_message_names_default_sql`, `test_violation_rows_printed_and_exit1`)뿐.

- [ ] **Step 5: `run_invariants.py` — `--sql` additive 4 hunk (기본 경로·출력 계약 불변)**

`SQL_PATH`·`render`·`load_sql` 서명·settings·exit 계약은 그대로 두고 아래 4곳만 바꾼다. 줄 번호는 현재 파일(155줄) 기준.

Hunk A — 모듈 docstring 사용법(`:19-21`). before:

```text
사용법:
  python3 tools/verify/run_invariants.py --date 2026-07-14
  python3 tools/verify/run_invariants.py            # --date 생략 시 어제(KST)
```

after:

```text
사용법:
  python3 tools/verify/run_invariants.py --date 2026-07-14
  python3 tools/verify/run_invariants.py            # --date 생략 시 어제(KST)
  python3 tools/verify/run_invariants.py --sql tools/verify/invariants_metrics.sql --date 2026-09-03
                                                    # --sql: 다른 불변식 파일(기본 invariants.sql)
```

Hunk B — `build_arg_parser()`(`:98-100`). before:

```text
    p.add_argument("--date", default=None,
                    help="YYYY-MM-DD (기본: 어제, KST). 검증 대상 단일일.")
    return p
```

after:

```text
    p.add_argument("--date", default=None,
                    help="YYYY-MM-DD (기본: 어제, KST). 검증 대상 단일일.")
    p.add_argument("--sql", default=None, metavar="PATH",
                    help="불변식 SQL 파일 경로 (기본: tools/verify/invariants.sql — "
                         "메트릭 파이프라인은 tools/verify/invariants_metrics.sql)")
    return p
```

Hunk C — `_print_violations()` 서명(`:103`)과 `[FAIL]` 메시지(`:109-110`). before:

```text
def _print_violations(rows: list[tuple], date_str: str, dbs: dict) -> None:
```

```text
    print(f"[FAIL] {len(rows)}건의 불변식 위반 발견 "
          f"(date={date_str}, DBs={dbs['fact']}/{dbs['dim']}/{dbs['mart']})")
```

after:

```text
def _print_violations(rows: list[tuple], date_str: str, dbs: dict,
                      sql_name: str = "invariants.sql") -> None:
```

```text
    print(f"[FAIL] {len(rows)}건의 불변식 위반 발견 "
          f"(date={date_str}, DBs={dbs['fact']}/{dbs['dim']}/{dbs['mart']}, sql={sql_name})")
```

Hunk D — `main()`의 SQL 로드(`:132-133`)와 결과 출력(`:146-150`). before:

```text
    dbs = load_db_config()
    sql = render(load_sql(), fact=dbs["fact"], dim=dbs["dim"], mart=dbs["mart"], date=date_str)
```

```text
    if rows:
        _print_violations(rows, date_str, dbs)
        return 1

    print(f"ALL INVARIANTS PASS (date={date_str}, DBs={dbs['fact']}/{dbs['dim']}/{dbs['mart']})")
```

after:

```text
    # --sql: 다른 불변식 파일(메트릭 파이프라인 invariants_metrics.sql). 미지정 시 SQL_PATH —
    # 기존 호출 경로·출력은 그대로(additive, 설계 §7.3). 상대 경로는 cwd 기준.
    sql_path = pathlib.Path(args.sql) if args.sql else SQL_PATH
    if not sql_path.is_file():
        parser.exit(2, f"--sql 파일을 찾을 수 없습니다: {sql_path}\n")

    dbs = load_db_config()
    sql = render(load_sql(sql_path), fact=dbs["fact"], dim=dbs["dim"], mart=dbs["mart"],
                 date=date_str)
```

```text
    if rows:
        _print_violations(rows, date_str, dbs, sql_path.name)
        return 1

    print(f"ALL INVARIANTS PASS (date={date_str}, DBs={dbs['fact']}/{dbs['dim']}/{dbs['mart']}, "
          f"sql={sql_path.name})")
```

`test_sql_flag_default_is_invariants_sql`은 `ri.load_sql`을 monkeypatch 해 경로를 캡처하므로 기본 경로에서도 `load_sql(sql_path)`처럼 **인자를 명시**해 호출한다(캡처 결과 `== ri.SQL_PATH`). 아래 스크립트가 4 hunk를 정확 일치로 적용한다(원문이 다르면 `AssertionError`로 중단 — 손으로 위 before/after를 적용해도 결과는 같아야 한다).

```bash
python3 - <<'PY'
import pathlib
p = pathlib.Path("tools/verify/run_invariants.py")
s = p.read_text(encoding="utf-8")

def rep(old, new):
    global s
    assert s.count(old) == 1, old
    s = s.replace(old, new)

rep('  python3 tools/verify/run_invariants.py            # --date 생략 시 어제(KST)\n',
    '  python3 tools/verify/run_invariants.py            # --date 생략 시 어제(KST)\n'
    '  python3 tools/verify/run_invariants.py --sql tools/verify/invariants_metrics.sql --date 2026-09-03\n'
    '                                                    # --sql: 다른 불변식 파일(기본 invariants.sql)\n')
rep('                    help="YYYY-MM-DD (기본: 어제, KST). 검증 대상 단일일.")\n    return p\n',
    '                    help="YYYY-MM-DD (기본: 어제, KST). 검증 대상 단일일.")\n'
    '    p.add_argument("--sql", default=None, metavar="PATH",\n'
    '                    help="불변식 SQL 파일 경로 (기본: tools/verify/invariants.sql — "\n'
    '                         "메트릭 파이프라인은 tools/verify/invariants_metrics.sql)")\n'
    '    return p\n')
rep('def _print_violations(rows: list[tuple], date_str: str, dbs: dict) -> None:\n',
    'def _print_violations(rows: list[tuple], date_str: str, dbs: dict,\n'
    '                      sql_name: str = "invariants.sql") -> None:\n')
rep('          f"(date={date_str}, DBs={dbs[\'fact\']}/{dbs[\'dim\']}/{dbs[\'mart\']})")\n    header',
    '          f"(date={date_str}, DBs={dbs[\'fact\']}/{dbs[\'dim\']}/{dbs[\'mart\']}, sql={sql_name})")\n    header')
rep('    dbs = load_db_config()\n'
    '    sql = render(load_sql(), fact=dbs["fact"], dim=dbs["dim"], mart=dbs["mart"], date=date_str)\n',
    '    # --sql: 다른 불변식 파일(메트릭 파이프라인 invariants_metrics.sql). 미지정 시 SQL_PATH —\n'
    '    # 기존 호출 경로·출력은 그대로(additive, 설계 §7.3). 상대 경로는 cwd 기준.\n'
    '    sql_path = pathlib.Path(args.sql) if args.sql else SQL_PATH\n'
    '    if not sql_path.is_file():\n'
    '        parser.exit(2, f"--sql 파일을 찾을 수 없습니다: {sql_path}\\n")\n'
    '\n'
    '    dbs = load_db_config()\n'
    '    sql = render(load_sql(sql_path), fact=dbs["fact"], dim=dbs["dim"], mart=dbs["mart"],\n'
    '                 date=date_str)\n')
rep('        _print_violations(rows, date_str, dbs)\n',
    '        _print_violations(rows, date_str, dbs, sql_path.name)\n')
rep('    print(f"ALL INVARIANTS PASS (date={date_str}, DBs={dbs[\'fact\']}/{dbs[\'dim\']}/{dbs[\'mart\']})")\n',
    '    print(f"ALL INVARIANTS PASS (date={date_str}, DBs={dbs[\'fact\']}/{dbs[\'dim\']}/{dbs[\'mart\']}, "\n'
    '          f"sql={sql_path.name})")\n')
p.write_text(s, encoding="utf-8")
print("APPLIED", s.count("--sql"), len(s.splitlines()))
PY
cd tools/verify && python -m pytest tests/test_run_invariants_metrics.py -q 2>&1 | tail -3 && python -m pytest -q 2>&1 | tail -3
python3 run_invariants.py --help | grep -A1 "^  --sql PATH"
python3 run_invariants.py --sql /nonexistent/x.sql --date 2026-09-03; echo "rc=$?"
```

기대: `APPLIED 5 169`(`--sql` 문자열 5회: docstring 2·인자 정의 1·main 주석 1·exit 메시지 1; 155 → 169줄); 첫 pytest `20 passed`; 전체 `41 passed`(기존 21 + 신규 20 — 기존 파일 무수정); `--help`는 두 줄 `  --sql PATH   불변식 SQL 파일 경로 (기본: tools/verify/invariants.sql — 메트릭 파이프라인은` / `               tools/verify/invariants_metrics.sql)`; 마지막 명령은 stderr `--sql 파일을 찾을 수 없습니다: /nonexistent/x.sql` + `rc=2`(CH 접속 없이 인자 검사 단계에서 종료).

- [ ] **Step 6: 렌더 스모크 + CI 커버·공개 레포 점검**

CH 없이 격리 DB명(`token_verify_*`)으로 렌더해 토큰 잔존 0을 확인하고, 신규 파일이 기존 워크플로 `verify-unit`에 자동 포함되는지, 사내 주소·이메일이 없는지 본다.

```bash
cd /home/mini/github/token-data-pipeline
python3 - <<'PY'
import pathlib, re, sys
sys.path.insert(0, "tools/verify")
import run_invariants as ri
sql = pathlib.Path("tools/verify/invariants_metrics.sql").read_text(encoding="utf-8")
r = ri.render(sql, fact="token_verify_fact", dim="token_verify_dim",
              mart="token_verify_mart", date="2026-09-03")
assert re.findall(r"\{[A-Za-z_:]+\}", r) == []
print("RENDER_OK", r.count("token_verify_fact."), r.count("token_verify_dim."),
      r.count("token_verify_mart."), r.count("'2026-09-03'"))
PY
grep -n "tools/verify/\*\*" .github/workflows/test-tools.yml
grep -rn "harbor\.\|\.svc\|@.*\.com" tools/verify/invariants_metrics.sql \
  tools/verify/tests/test_run_invariants_metrics.py tools/verify/run_invariants.py; echo "hosts=$?"
wc -l tools/verify/invariants_metrics.sql tools/verify/tests/test_run_invariants_metrics.py
```

기대: `RENDER_OK 8 2 11 17`(주석 포함 치환 횟수 — fact 8·dim 2·mart 11·날짜 17, 토큰 잔존 0); 워크플로 6·8행에 `"tools/verify/**"`(push·pull_request 양쪽 paths — job `verify-unit`이 `python -m pytest tests/ -v`로 신규 파일을 자동 실행, 워크플로 무수정); 호스트 grep 출력 없음 + `hosts=1`; `268 tools/verify/invariants_metrics.sql`, `329 tools/verify/tests/test_run_invariants_metrics.py`.

- [ ] **Step 7: zero-diff 확인 + 커밋**

기존 모듈·문서·워크플로·`invariants.sql`에 변경이 없는지 확인한 뒤 T9 산출물 3파일을 한 번에 커밋한다.

```bash
cd /home/mini/github/token-data-pipeline
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql \
  docs/operations docs/monitoring/grafana_dashboard_token_usage.json \
  .github/workflows/release-images.yml .github/workflows/test-mart.yml .github/workflows/test-collector.yml \
  .github/workflows/test-tools.yml tools/verify/tests/test_run_invariants.py tools/verify/conftest.py \
  tools/verify/requirements-dev.txt
git status --porcelain -- tools/verify
```

기대: `git diff --stat` 출력 없음(기존 파일 zero-diff — `invariants.sql`·기존 테스트·conftest·requirements·`test-tools.yml` 포함); `git status`는 정확히 3줄 ` M tools/verify/run_invariants.py`, `?? tools/verify/invariants_metrics.sql`, `?? tools/verify/tests/test_run_invariants_metrics.py`.

```bash
cd /home/mini/github/token-data-pipeline
git add tools/verify/invariants_metrics.sql tools/verify/run_invariants.py \
  tools/verify/tests/test_run_invariants_metrics.py
git commit -m "feat(verify): invariants_metrics.sql 8블록(P0 5 + stretch 3) + run_invariants.py --sql additive (Plan 6c T9)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
git show --stat HEAD | tail -5
```

기대: `git show --stat`에 3파일 — `tools/verify/invariants_metrics.sql | 267 +`, `tools/verify/run_invariants.py | 24 +++++++++++++++++++-----`(+19/-5 — 빈 줄 1 포함), `tools/verify/tests/test_run_invariants_metrics.py | 329 +`; 커밋 후 `cd tools/verify && python -m pytest -q`가 여전히 `41 passed`. 다음 Task(T10 CI `test-mart-metrics.yml`)는 이 파일들을 참조하지 않는다(`tools/verify/**`는 기존 `test-tools.yml`이 담당) — T11 문서는 이 Task의 실행 커맨드 `python3 tools/verify/run_invariants.py --sql tools/verify/invariants_metrics.sql --date YYYY-MM-DD`와 8블록 이름을 그대로 인용하므로, 이름을 바꿔야 할 사유가 생기면 여기서 바꾸지 말고 outline 정정 후 T9부터 다시 한다.

**설계 해석 (T9 — footer "Self-Review 노트"에 병합)**:
- **`metrics_serving_dup_key`의 GROUP BY 키**: outline은 `GROUP BY service, model`이었으나 serving 팩트는 한 (service, model)에 여러 `metric`/`name` 행이 정상적으로 존재한다(ORDER BY `(date, service, model, metric, name)`). 그 키로 세면 정상 데이터에서도 항상 위반이 나오므로 **ORDER BY 키 전체** `service, model, metric, name`으로 중복을 판정한다. gpu 팩트도 같은 원칙으로 `service, model, gpu_type, category`.
- **`metrics_cost_sum_mismatch`의 NULL 규칙 대칭 대사**: outline은 `model_cost_krw IS NOT NULL` 행만 값 비교했지만, M1의 "TCO NULL 기종이 하나라도 있으면 C NULL(부분 합 금지)" 규칙이 깨진 경우(비FAIL 행에 TCO NULL인데 값이 채워짐, 또는 그 반대)는 값 비교로는 잡히지 않는다. 그래서 `null_mismatch`(NULL 여부 != `tco_null_cnt > 0`)와 `value_mismatch`(둘 다 값 있을 때 ±1원)를 함께 본다. 재계산은 M1과 같은 집합(앵커 있는 서비스 × 비FAIL serving/standby × test 제외)이고 **mart 주도 LEFT JOIN**이다 — fact에는 있는데 mart에 없는 키(M1 누락)는 M1 자체의 `verify_count`(EXPECTED = uniqExact 키)가 담당하므로 여기서 FULL JOIN으로 이중 검사하지 않는다.
- **`share_sum_mismatch`의 `provider_reported` 포함**: 정의서 I3는 mode 무관 항등식이지만 `provider_reported`에서 소비자 토큰이 제공자 보고분을 넘으면 Σ가 C를 넘는다(M3 `consumer_tokens_exceed_provider` WARN). 이를 자동 면제하지 않고 위반으로 노출한다(불변식은 "정의상 성립해야 할 것"을 보고, 면제 판단은 사람이 M3 WARN과 대조 — `coverage_gap`의 expected-late 처리와 같은 관례). `GROUP BY model`은 M4의 `denominator_mode`가 모델당 하나라는 T6 설계에 의존한다(detail에 `any(denominator_mode)`를 실어 대조 가능하게 함).
- **`group_identity_gap`은 `tco_missing = 1`만 제외, `over_report` 면제 없음**: 설계 §7.1이 I2를 `abs(identity_gap_krw) > 1`로만 정의하므로 over_report 행도 그대로 본다 — 그 행은 idle 클램프로 gap = (할당 − 보고) × TCO가 생겨 `idle_negative`(I1)와 **함께** 보고된다(항등식과 idle ≥ 0이 동시에 깨진 상태라 두 위반 모두 사실). tco_missing 행은 항등식의 항이 NULL이라 판정 불가(`isNotNull(identity_gap_krw)`와 이중 안전).
- **출력 메시지에 `sql=<파일명>` 추가**: header의 "메시지에 sql 파일명 표기"와 outline의 "출력 형식 무변경"이 충돌한다. PASS/`[FAIL]` 메시지의 괄호 **안쪽 끝**에 `, sql=<파일명>`을 덧붙이는 것으로 절충 — 기존 테스트가 단언하는 부분 문자열(`ALL INVARIANTS PASS`, `DBs=fact/gpu_data/mart`, `[FAIL]`, `2건`)은 모두 그대로 남아 21건 무수정 통과. `_print_violations`는 선택 인자 `sql_name="invariants.sql"`로 additive.
- **`--sql` 상대 경로는 cwd 기준**(`pathlib.Path(args.sql)` 그대로, `resolve()`·모듈 디렉터리 기준 해석 없음) — 사용법 예시가 레포 루트 기준 `tools/verify/invariants_metrics.sql`이므로 그 관례를 따른다. 존재 검사는 `is_file()`로 DB 접속 전에 수행해 exit 2.
- **detail의 서비스·모델·기종·그룹명 나열 허용**: 마스터 §5.6이 금지하는 것은 사용자 식별자·페이로드 원문이며 메트릭 테이블에는 그런 컬럼이 없다. `metrics_anchor_missing`은 서비스명을 정렬 나열, 나머지는 집계값만.
- **`created_by_wrong_metrics`의 `token_metrics_check_1d` 포함**: M3는 검사 결과 테이블이지만 Plan 6a 쓰기 계약(created_by CHECK)이 4테이블 공통이므로 4테이블 모두 검사한다(설계 §7.1 342 "mart 4테이블").
### Task 10: E2E 단일노드 — seed_metrics.py/ddl_test_dims.sql/mart_expectations.py/run_e2e.sh/verify_expected_results.sql(2회 멱등 + invariants_metrics) + .github/workflows/test-mart-metrics.yml

**설계 근거**: 설계 §6.1 306(멱등 2회 실행·EXPECTED=count·no-metrics day는 SUCCESS), §7.3 350-354(단위·e2e 테스트 범위 — mart `test_{steps,batch}.py`·e2e) + §7.5 368(권장 절차 stage(mock) → 운영 직접 설치 → `invariants_metrics.sql` — E2E는 이 절차의 단일노드 재현), §6.4 (1)~(7)(C = Σ(serving+standby, 비FAIL) × TCO·W 1/0.1/4·share = W(s)/W(m)·idle 클램프·정체성 항등), §4.0(insert_deduplicate=0 — 2회 실행 후 행수 동일), 정의서 §5.1(전용/공유 모델 배분이 총액을 보존 — I3)·§5.3(idle)·§8 I1~I3, Plan 6a T6 stage fixture 값(digest §20: alias 6행, TCO unknown NULL/H100 4200/A100 2100/H200 5300/L40S 1300 — **H100 2026-08-26 4300 이력 행은 의도적으로 제외**(아웃라인 "TCO 5행" — 포함하면 2026-09-03 유효 TCO가 4300이 되어 C(Qwen3-32B) 201,600 검산이 깨짐), allocation unknown/unknown NULL + Mock Group H100 8 / A100 4, vendor_price unknown + anthropic 3행), T9 `invariants_metrics.sql` 8블록(`metrics_anchor_missing`, `metrics_gpu_dup_key`, `metrics_serving_dup_key`, `metrics_cost_sum_mismatch`, `created_by_wrong_metrics`, `share_sum_mismatch`, `group_identity_gap`, `idle_negative`) + `run_invariants.py --sql`, T5 마커 정규식(`BATCH_RESULT status=… module=mart-metrics metrics_coverage=N/M missing_services="…" …`), T8 이미지 `ENTRYPOINT ["python", "-m", "app.batch"]`(이미지 스모크는 `docker run --rm token-mart-metrics:ci --help` — `python -m app.batch`를 다시 붙이면 argparse 오류·T8 명시), T8 CronJob 계약 수치·overlay 이름.
**읽을 원형**: digest §13 `mart/token-usage/tests/e2e/run_e2e.sh:1-141`(컨테이너·단일노드 변환 python 블록의 regex 3종·HTTP DDL 적재·2회 실행·`declare -A EXP` 파싱·`sed` 토큰 치환·HTTP 코드 캡처 expect-empty), §14 `seed_fact.py`(`_client()` = `clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT)`·`client.insert(table, rows, column_names=COLS)`·date 객체·aware KST datetime), §15 `ddl_test_dims.sql`(`_dist` 이름의 단일노드 MergeTree 대역), §16 `verify_expected_results.sql`(`SELECT '<check>' AS check_name, <actual>, <expected> FROM … HAVING <불일치>` UNION ALL, UInt64/Float64 supertype 회피 캐스팅), §17 `mart_expectations.py`(`key=value` 출력·`sys.path` 삽입), §24.1 `tools/verify/run_invariants.py`(env `CH_HOST/CH_PORT/CH_USER/CH_PASSWORD`, `CH_DB_FACT/CH_DB_DIM/CH_DB_MART`, 출력 `ALL INVARIANTS PASS (date=…, DBs=fact/gpu_data/mart)` — T9 `--sql` 적용 후에는 괄호 끝에 `, sql=<파일명>`이 붙는다), §25.1 `.github/workflows/test-mart.yml`(4 job 골격), T2 `app.mart`(`model_cost`·`weighted_tokens`·`allocate_shared`·`group_overhead`·`compute_coverage`·`quality_flag_m1`·`FAIL_FLAGS`), T3 `SUB_ANCHOR`/`_TOK_TAIL`(토큰 측 = `dim_token_service enabled=1` 서비스 전부, 메트릭 측 = 앵커 있는 서비스만), T4/T6/T7 M3 20블록 술어, T5 `app.batch.main(argv)`(`--date D`).

**Files:**
- Create: `mart/token-metrics/tests/e2e/ddl_test_dims.sql`, `mart/token-metrics/tests/e2e/seed_metrics.py`, `mart/token-metrics/tests/e2e/mart_expectations.py`, `mart/token-metrics/tests/e2e/verify_expected_results.sql`, `mart/token-metrics/tests/e2e/run_e2e.sh`, `.github/workflows/test-mart-metrics.yml`
- Test: `mart/token-metrics/tests/test_e2e_seed.py`(CH 없이 실행 — `build_seed`/`expect` 결정성·시나리오 값·`ddl_test_dims.sql` 상수 교차 검증; `python -m pytest -q`가 자동 수집)
- 읽기만(zero-diff): `collectors/token-usage/ddl/company/dim_token_service.sql`, `mart/token-usage/ddl/company/mart_tables.sql`, `tools/verify/run_invariants.py`(T9가 `--sql` 추가 완료 상태), `.github/workflows/test-mart.yml`

**Interfaces:**
- Produces:
  - `seed_metrics.build_seed(date: str) -> dict[str, list[tuple]]` — 키 순서 고정 `dim_token_service`, `dim_metrics_service`, `gpu`, `serving`, `summary`, `token_usage`, `agg_service`; 값은 `SEED_TABLES[key]`의 `(table, cols)` 컬럼 순서와 1:1인 튜플 리스트. 결정적 — 모든 수치는 모듈 상수, 합성 user_id는 `sha256(f"{service}|{date}|{k}")` 앞 12자(`random` 미사용).
  - `seed_metrics.SEED_TABLES: dict[str, tuple[str, tuple[str, ...]]]`(키 → `(<db>.<table>_dist, column_names)`), `seed_metrics.TCO_KRW: dict[str, float | None]`, `seed_metrics.ALLOCATION: dict[tuple[str, str], float | None]`, `seed_metrics.ALIASES: dict[str, str]`, `seed_metrics.VENDOR_PRICE: dict[tuple[str, str], tuple[float | None, float | None, float | None, float | None]]`(`(provider, model)` → KRW/MTok (input, cached, cache_creation, output); 위 4 상수 = `ddl_test_dims.sql` 시드의 파이썬 재현 — 교차 검증 테스트가 SQL 파일과 대조), `seed_metrics.synthetic_user_id(service: str, date: str, k: int) -> str`(`"u-" + sha256(f"{service}|{date}|{k}")[:12]`), `seed_metrics.seed_all(client, date: str) -> dict[str, int]`, `seed_metrics.main(argv=None) -> int`(`python3 tests/e2e/seed_metrics.py <date>`; env `CH_HOST/CH_PORT/CH_DB_FACT/CH_DB_DIM/CH_DB_MART`).
  - `mart_expectations.expect(date: str) -> dict[str, float | int | str]` — 키 순서 고정 `EXP_M1_ROWS, EXP_M1_QWEN_COST, EXP_M3_FAIL_ROWS, EXP_M3_WARN_ROWS, EXP_M4_ROWS, EXP_M4_QWEN_SUM, EXP_M2_ROWS, EXP_M2_IDLE_H100, EXP_COVERAGE`; `mart_expectations.EXP_KEYS: tuple[str, ...]`(위 9키 순서), `mart_expectations.M3_SEVERITY: dict[str, str]`(20블록 이름 → FAIL/WARN/INFO — T4/T6/T7 `M3_BLOCKS` severity 재현), `mart_expectations.m1_rows(seed) -> dict[tuple[str, str], dict]`(키 `(service, canonical_model)` → `model_cost_krw, weighted_tokens, requests, has_gpu_rows, has_token_rows, quality_flag`), `mart_expectations.m4_rows(seed) -> dict[tuple[str, str], dict]`(키 `(model, service)` → `provider_service, is_provider, denominator_mode, share, allocated_cost_krw, quality_flag`), `mart_expectations.m2_rows(seed) -> dict[tuple[str, str], dict]`(키 `(service_group, gpu_type)` → `mart.group_overhead(...)` 9키 + `allocated_gpu_hours, reported_gpu_hours_total, flagged_gpu_hours, tco_missing, quality_flag`), `mart_expectations.m3_counts(seed) -> dict[str, int]`(20블록 이름 → 기대 행수), `mart_expectations.main(argv=None) -> int`(stdout `key=value` 9줄; 실수는 `.4f`).
  - `verify_expected_results.sql` 토큰 `{DATE}` + `{EXP_M1_ROWS} {EXP_M1_QWEN_COST} {EXP_M3_FAIL_ROWS} {EXP_M3_WARN_ROWS} {EXP_M4_ROWS} {EXP_M4_QWEN_SUM} {EXP_M2_ROWS} {EXP_M2_IDLE_H100}`; 검사 20종 `m1_rows, m1_qwen_cost_a, m1_flags, m1_c_absent, m3_fail_rows, m3_warn_rows, m3_manual_source_b_info, m3_metrics_missing_c, m4_rows, m4_qwen_mode_all_services, m4_qwen_allocated_sum, m4_sonnet_share_null, m2_rows, m2_h100_idle, m2_b200_no_tco, m2_a100_alloc_only_normal, created_by_all_tables, idempotent_no_dup_m1, idempotent_no_dup_m2, idempotent_no_dup_m4`.
  - `run_e2e.sh [<date>]`(기본 `2026-09-03`; 컨테이너 `ch-e2e-mart-metrics`, 네트워크 `tokene2e-mart-metrics`, 호스트 포트 `18124`); 종료 메시지 `E2E PASS (date=<d>, m1_rows=<n>, coverage=<N/M>)`.
  - `.github/workflows/test-mart-metrics.yml` — `name: test-mart-metrics`, jobs `image`·`manifests`·`unit`·`e2e`.
- Consumes: Plan 6a DDL `collectors/token-metrics/ddl/company/raw_token_metrics.sql`(fact 4테이블)·`collectors/token-metrics/ddl/company/dim_token_metrics_service.sql`, `mart/token-metrics/ddl/company/mart_metrics_tables.sql`(M1 28컬럼·M3 12컬럼·M4 14컬럼·M2 23컬럼), 기존 `collectors/token-usage/ddl/company/dim_token_service.sql`·`mart/token-usage/ddl/company/mart_tables.sql`(읽기 계약 대역 — `token_usage_1d`는 `created_by != ''` CHECK가 있어 시드가 `created_by='token-pipeline'`을 넣는다), `tools/verify/invariants_metrics.sql`(T9)·`run_invariants.py --sql`(T9), `app.batch.main`(T5, `--date`), `app.mart`(T2), T8 `k8s/overlays/{stage,company,company-verify}`·`Dockerfile`.

**전제(선행 태스크 산출물이 체크아웃에 있어야 한다)**: T1–T9 커밋 완료(`mart/token-metrics/app/{config,ch,preflight,mart,steps,batch}.py`, `tools/verify/invariants_metrics.sql`, `run_invariants.py --sql`), Plan 6a DDL 3파일, T8 `k8s/`·`Dockerfile`. 로컬 E2E는 도커 데몬 필요(CI `e2e` job과 동일). 이 태스크의 어떤 스텝도 `collectors/token-usage/**`, `mart/token-usage/**`, `assets/**`, `tools/verify/invariants.sql`, `docs/**`, 기존 워크플로 3파일을 건드리지 않는다.

**시나리오 요약(시드 → 기대값 검산 — 스텝 4·5의 코드가 이 표를 계산으로 재현한다)**:

| 대상 | 시드 | 기대 |
|---|---|---|
| Mock Service A (`metrics-api-v1`, 앵커 gpu 3/serving 2/rejected 1) | gpu Qwen3-32B/H100 serving 2×40h, standby 1×8h, test 1×2h; serving ttft_ms 120/240/300/450·output_tps 80/60/55/40; 토큰 Qwen (2,000,000, 5,000,000, 0, 250,000, 100) | M1 (A,Qwen) `normal`, C = (40+8)×4200 = 201,600; W = 3.5M; M3 `rows_rejected` WARN |
| Mock Service B (`manual-v0`, expect_serving 0, 앵커 gpu 3/serving 0) | gpu claude-sonnet-5/H100 serving 1×20h, /B200 serving 1×4h, /H100 standby 2×50h flags `['hours_over_count']`; 토큰 Qwen (4,000,000, 10,000,000, 0, 500,000, 200) | M1 (B,Qwen) `manual`, (B,sonnet) `no_tco`(B200 TCO 부재 → C NULL); W(B,Qwen) = 7.0M; M3 `hours_over_count` FAIL, `gpu_type_no_tco` WARN, `manual_source` INFO |
| Mock Service C | 메트릭 레지스트리만 | M3 `metrics_missing` FAIL; coverage 2/3; M1 행 없음 |
| Mock Service D | `dim_token_service`만 + 토큰 Qwen (500,000, 0, 0, 0, 10) | M1 (D,Qwen) `consumer_only`; W = 0.5M |
| M4 Qwen3-32B | W(m) = 11.0M, 제공자 A(serving gpu 행) | 3행 `all_services`(A is_provider=1), Σ allocated = 201,600 (I3) |
| M4 claude-sonnet-5 | 토큰 0, 제공자 B, C NULL | 1행 (B,B) `all_services`, share NULL, quality `no_tco` |
| M2 Mock Group/H100 | allocated 8×24 = 192, reported 40+8+2+20+50 = 120 | idle 72, gap 806,400 − 285,600 − 8,400 − 302,400 − 210,000 = 0, `flagged`(FAIL 행 50h > 0 — T7-2 우선순위 over_report > no_tco > no_allocation > flagged > normal) |
| M2 Mock Group/B200 | 할당 없음, TCO 없음 | `no_tco`(우선순위 no_tco > no_allocation), M3 `no_allocation` WARN |
| M2 Mock Group/A100 | 할당 4×24 = 96, gpu 행 0 | alloc-only, idle 96, gap 0, `normal` |

- [ ] **Step 1: `tests/e2e/ddl_test_dims.sql` — dim 4종 단일노드 대역 + 시드(fixture 값 재현 + e2e alias identity 1행)**

원형 §15와 같은 방식: 이름은 `_dist`지만 엔진은 MergeTree(단일노드). 컬럼·ORDER BY는 Plan 6a D 표 그대로. `run_e2e.sh`가 `;`로 문장을 분리하므로 **주석·문자열 안에 세미콜론을 쓰지 않는다**. 값은 digest §20 fixture와 동일하되 TCO의 `H100 2026-08-26 4300` 이력 행만 제외(설계 근거 참조), alias에 e2e 전용 `Qwen3-32B` identity 1행 추가.

```bash
# 전제 가드 — run_e2e.sh 가 읽는 Plan 6a 산출물이 체크아웃에 없으면 FileNotFoundError 대신 여기서 멈춘다 (6a 머지 후 6c 브랜치 시작)
R=/home/mini/github/token-data-pipeline; test -f "$R/mart/token-metrics/ddl/company/mart_metrics_tables.sql" && test -f "$R/collectors/token-metrics/ddl/company/raw_token_metrics.sql" && test -f "$R/collectors/token-metrics/ddl/company/dim_token_metrics_service.sql" || { echo "Plan 6a/6b not merged — mart_metrics_tables.sql / raw_token_metrics.sql / dim_token_metrics_service.sql 필요"; exit 1; }
mkdir -p /home/mini/github/token-data-pipeline/mart/token-metrics/tests/e2e
cat > /home/mini/github/token-data-pipeline/mart/token-metrics/tests/e2e/ddl_test_dims.sql <<'SQL'
-- =============================================================
-- [E2E 전용] 기준정보 dim 4종의 단일노드 대역 — gpu_data.dim_token_{model_alias,gpu_tco,gpu_allocation,vendor_price}_dist
-- 이름만 _dist(steps.py가 {DB_DIM}.<table>_dist를 읽음), 엔진은 MergeTree(단일노드).
-- 컬럼·ORDER BY = assets/model-catalog/ddl/company/dim_token_*.sql (Plan 6a D 표).
-- 시드 = assets/model-catalog/fixtures/stage_seed_dim_token_*.sql 값 재현(디폴트 컬럼 포함 전부 명시).
--   델타 1: TCO의 H100 2026-08-26 4300 이력 행은 넣지 않는다 — 2026-09-03 유효 TCO를 4200으로 고정해
--           C(Qwen3-32B) = (40+8)×4200 = 201,600 검산을 유지(이력 argMax 경로는 T3 단위 테스트가 검증).
--   델타 2: alias에 e2e 전용 identity 1행(Qwen3-32B) 추가 — unregistered_model WARN 0건 유지.
-- 주의: run_e2e.sh가 세미콜론으로 문장을 나눈다 — 주석·문자열에 세미콜론 금지(이 주석 포함).
-- 이 파일의 파이썬 재현(TCO_KRW/ALLOCATION/ALIASES/VENDOR_PRICE)은 tests/e2e/seed_metrics.py 상단 상수 —
-- tests/test_e2e_seed.py가 두 정본을 교차 대조한다(값을 고치면 둘 다 고친다).
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_model_alias_dist
(
    alias            String,
    effective_from   Date,
    canonical        String,
    defining_service LowCardinality(String) DEFAULT '',
    source           LowCardinality(String) DEFAULT 'metadata-sheet',
    note             String DEFAULT ''
)
ENGINE = MergeTree
ORDER BY (alias, effective_from);

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_gpu_tco_dist
(
    gpu_type             String,
    effective_from       Date,
    tco_krw_per_gpu_hour Nullable(Float64),
    currency             LowCardinality(String) DEFAULT 'KRW',
    basis                LowCardinality(String) DEFAULT '',
    note                 String DEFAULT ''
)
ENGINE = MergeTree
ORDER BY (gpu_type, effective_from);

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_gpu_allocation_dist
(
    service_group       LowCardinality(String),
    gpu_type            String,
    effective_from      Date,
    allocated_gpu_count Nullable(Float64),
    source              LowCardinality(String) DEFAULT 'manual',
    note                String DEFAULT ''
)
ENGINE = MergeTree
ORDER BY (service_group, gpu_type, effective_from);

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_vendor_price_dist
(
    provider                   LowCardinality(String),
    model                      String,
    tier                       LowCardinality(String) DEFAULT 'standard',
    effective_from             Date,
    krw_per_mtok_input         Nullable(Float64),
    krw_per_mtok_cached        Nullable(Float64),
    krw_per_mtok_cache_creation Nullable(Float64),
    krw_per_mtok_output        Nullable(Float64),
    note                       String DEFAULT ''
)
ENGINE = MergeTree
ORDER BY (provider, model, tier, effective_from);

-- alias: fixture 6행 + e2e identity 1행 = 7행
INSERT INTO gpu_data.dim_token_model_alias_dist (alias, effective_from, canonical, defining_service, source, note) VALUES
('unknown', '2026-01-01', 'unknown', '', 'seed', 'synthetic identity'),
('claude-opus-4-8', '2026-01-01', 'claude-opus-4-8', '', 'seed', 'synthetic identity'),
('claude-sonnet-5', '2026-01-01', 'claude-sonnet-5', '', 'seed', 'synthetic identity'),
('claude-haiku-4-5', '2026-01-01', 'claude-haiku-4-5', '', 'seed', 'synthetic identity'),
('claude-sonnet-5-20260101', '2026-01-01', 'claude-sonnet-5', 'Mock Service A', 'seed', 'synthetic dated alias'),
('opus-4.8', '2026-01-01', 'claude-opus-4-8', 'Mock Service B', 'seed', 'synthetic short alias'),
('Qwen3-32B', '2026-01-01', 'Qwen3-32B', 'Mock Service A', 'seed', 'e2e');

-- tco: fixture 중 2026-01-01 행 5개(B200 부재 = gpu_type_no_tco 경로)
INSERT INTO gpu_data.dim_token_gpu_tco_dist (gpu_type, effective_from, tco_krw_per_gpu_hour, currency, basis, note) VALUES
('unknown', '2026-01-01', NULL, 'KRW', '', 'synthetic placeholder'),
('H100', '2026-01-01', 4200.0, 'KRW', 'tco', 'synthetic stage value'),
('A100', '2026-01-01', 2100.0, 'KRW', 'tco', 'synthetic stage value'),
('H200', '2026-01-01', 5300.0, 'KRW', 'tco', 'synthetic stage value'),
('L40S', '2026-01-01', 1300.0, 'KRW', 'tco', 'synthetic stage value');

-- allocation: fixture 3행 그대로(Mock Group H100 8 / A100 4)
INSERT INTO gpu_data.dim_token_gpu_allocation_dist (service_group, gpu_type, effective_from, allocated_gpu_count, source, note) VALUES
('unknown', 'unknown', '2026-01-01', NULL, 'seed', 'synthetic placeholder'),
('Mock Group', 'H100', '2026-01-01', 8.0, 'seed', 'synthetic stage value'),
('Mock Group', 'A100', '2026-01-01', 4.0, 'seed', 'synthetic stage value');

-- vendor_price: fixture 4행 그대로(unknown 플레이스홀더 + anthropic 3)
INSERT INTO gpu_data.dim_token_vendor_price_dist (provider, model, tier, effective_from, krw_per_mtok_input, krw_per_mtok_cached, krw_per_mtok_cache_creation, krw_per_mtok_output, note) VALUES
('unknown', 'unknown', 'standard', '2026-01-01', NULL, NULL, NULL, NULL, 'synthetic placeholder'),
('anthropic', 'claude-opus-4-8', 'standard', '2026-01-01', 6750.0, 675.0, 8437.5, 33750.0, 'synthetic USD x 1350'),
('anthropic', 'claude-sonnet-5', 'standard', '2026-01-01', 4050.0, 405.0, 5062.5, 20250.0, 'synthetic USD x 1350'),
('anthropic', 'claude-haiku-4-5', 'standard', '2026-01-01', 1350.0, 135.0, 1687.5, 6750.0, 'synthetic USD x 1350');
SQL
```

검증(CH 불필요 — 문장 분리·테이블 4개·행수·단일노드 여부):

```bash
python3 - <<'PY'
import re, pathlib
sql = pathlib.Path("mart/token-metrics/tests/e2e/ddl_test_dims.sql").read_text(encoding="utf-8")
stmts = [s.strip() for s in sql.split(";") if s.strip()]
creates = [s for s in stmts if "CREATE TABLE" in s]
inserts = [s for s in stmts if "INSERT INTO" in s]
print(len(stmts), len(creates), len(inserts))
print(re.findall(r"CREATE TABLE IF NOT EXISTS (gpu_data\.\w+)", sql))
print("ON CLUSTER" in sql, "Replicated" in sql, "Distributed(" in sql)
for s in inserts:
    table = re.search(r"INSERT INTO (\S+)", s).group(1)
    print(table, sum(1 for line in s.splitlines() if line.startswith("(")))
PY
```

기대 출력:

```
8 4 4
['gpu_data.dim_token_model_alias_dist', 'gpu_data.dim_token_gpu_tco_dist', 'gpu_data.dim_token_gpu_allocation_dist', 'gpu_data.dim_token_vendor_price_dist']
False False False
gpu_data.dim_token_model_alias_dist 7
gpu_data.dim_token_gpu_tco_dist 5
gpu_data.dim_token_gpu_allocation_dist 3
gpu_data.dim_token_vendor_price_dist 4
```

- [ ] **Step 2: 실패 테스트 — `tests/test_e2e_seed.py`(CH 없이 시드·기대값 스크립트의 계산부 검증)**

`tests/e2e/`는 패키지가 아니므로 `importlib`로 파일 경로에서 로드한다(원형 `tests/test_rerun.py`의 importlib 관례). `seed_metrics.py`는 `clickhouse_connect`를 `_client()` 안에서만 import 하므로 로드 시 CH 드라이버가 필요 없다.

```bash
cat > /home/mini/github/token-data-pipeline/mart/token-metrics/tests/test_e2e_seed.py <<'PYEOF'
"""E2E 시드(tests/e2e/seed_metrics.py)·기대값(tests/e2e/mart_expectations.py)의 CH 불필요 부분 검증 (Plan 6c T10).

- build_seed(date): 키·컬럼 폭·결정성(sha256 user_id, random 미사용)·시나리오 값(gpu/serving/summary/token/registry)
- ddl_test_dims.sql 시드 값 == seed_metrics.TCO_KRW/ALLOCATION/ALIASES (두 정본 교차 대조 — 드리프트 방지)
- expect(date): 9키 값 검산(정의서 §5.1/§5.3·설계 §6.4 — 아웃라인 T10 Step 3의 검산표)·m3_counts 20블록 분해
"""
import hashlib
import importlib.util
import pathlib
import re
import sys
from datetime import date as date_cls

import pytest

E2E_DIR = pathlib.Path(__file__).resolve().parent / "e2e"
DATE = "2026-09-03"


def _load(name: str):
    path = E2E_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def sm():
    return _load("seed_metrics")


@pytest.fixture(scope="module")
def me(sm):
    return _load("mart_expectations")


@pytest.fixture(scope="module")
def seed(sm):
    return sm.build_seed(DATE)


# ---------------------------------------------------------------- build_seed 구조·결정성

def test_build_seed_keys_follow_seed_tables_order_and_column_width(sm, seed):
    assert list(seed.keys()) == list(sm.SEED_TABLES.keys())
    assert list(sm.SEED_TABLES.keys()) == ["dim_token_service", "dim_metrics_service", "gpu", "serving",
                                           "summary", "token_usage", "agg_service"]
    for key, rows in seed.items():
        table, cols = sm.SEED_TABLES[key]
        assert table.endswith("_dist") and table.split(".")[0] in (sm.DB_FACT, sm.DB_DIM, sm.DB_MART)
        assert rows, key
        for row in rows:
            assert isinstance(row, tuple) and len(row) == len(cols), (key, row)


def test_build_seed_is_deterministic_and_uses_sha256_not_random(sm, seed):
    assert sm.build_seed(DATE) == seed
    src = (E2E_DIR / "seed_metrics.py").read_text(encoding="utf-8")
    assert "import random" not in src and "hashlib" in src
    cols = sm.SEED_TABLES["token_usage"][1]
    uid_idx, svc_idx = cols.index("user_id"), cols.index("service")
    for row in seed["token_usage"]:
        uid, svc = row[uid_idx], row[svc_idx]
        assert re.fullmatch(r"u-[0-9a-f]{12}", uid)
        assert uid in {sm.synthetic_user_id(svc, DATE, 0), sm.synthetic_user_id(svc, DATE, 1)}
    assert sm.synthetic_user_id("Mock Service A", DATE, 0) == \
        "u-" + hashlib.sha256(f"Mock Service A|{DATE}|0".encode()).hexdigest()[:12]


def test_row_counts_match_scenario(seed):
    assert {k: len(v) for k, v in seed.items()} == {
        "dim_token_service": 4, "dim_metrics_service": 3, "gpu": 6, "serving": 2,
        "summary": 2, "token_usage": 6, "agg_service": 3}


def test_dates_are_date_objects_and_datetimes_are_kst_aware(sm, seed):
    for key, rows in seed.items():
        cols = sm.SEED_TABLES[key][1]
        for row in rows:
            for name, val in zip(cols, row):
                if name == "date":
                    assert type(val) is date_cls and val == date_cls(2026, 9, 3)
                if name in ("api_since", "coverage_since"):
                    assert type(val) is date_cls
                if name in ("generated_at", "collected_at", "updated_at"):
                    assert val.tzinfo is not None and val.utcoffset().total_seconds() == 9 * 3600


# ---------------------------------------------------------------- 시나리오 값

def test_gpu_and_serving_and_anchor_values(sm, seed):
    gcols = sm.SEED_TABLES["gpu"][1]
    g = [dict(zip(gcols, r)) for r in seed["gpu"]]
    by_key = {(r["service"], r["model"], r["gpu_type"], r["category"]): r for r in g}
    assert len(by_key) == 6                                             # metrics_gpu_dup_key 0건
    assert by_key[(sm.SVC_A, sm.MODEL_QWEN, "H100", "serving")]["gpu_hours"] == 40.0
    assert by_key[(sm.SVC_A, sm.MODEL_QWEN, "H100", "standby")]["gpu_hours"] == 8.0
    assert by_key[(sm.SVC_A, sm.MODEL_QWEN, "H100", "test")]["gpu_hours"] == 2.0
    assert by_key[(sm.SVC_B, sm.MODEL_SONNET, "B200", "serving")]["gpu_hours"] == 4.0
    flagged = by_key[(sm.SVC_B, sm.MODEL_SONNET, "H100", "standby")]
    assert flagged["gpu_count"] == 2.0 and flagged["gpu_hours"] == 50.0 and flagged["flags"] == ["hours_over_count"]
    assert all(r["flags"] == [] for k, r in by_key.items() if k != (sm.SVC_B, sm.MODEL_SONNET, "H100", "standby"))
    assert {r["source_type"] for r in g if r["service"] == sm.SVC_B} == {"manual-v0"}

    scols = sm.SEED_TABLES["serving"][1]
    s = [dict(zip(scols, r)) for r in seed["serving"]]
    assert {(r["service"], r["model"], r["metric"], r["name"]) for r in s} == {
        (sm.SVC_A, sm.MODEL_QWEN, "ttft_ms", ""), (sm.SVC_A, sm.MODEL_QWEN, "output_tps", "")}
    ttft = next(r for r in s if r["metric"] == "ttft_ms")
    assert (ttft["p50"], ttft["p90"], ttft["p95"], ttft["p99"], ttft["unit"]) == (120.0, 240.0, 300.0, 450.0, "ms")
    tps = next(r for r in s if r["metric"] == "output_tps")
    assert (tps["p50"], tps["p90"], tps["p95"], tps["p99"], tps["unit"]) == (80.0, 60.0, 55.0, 40.0, "tokens/s")

    acols = sm.SEED_TABLES["summary"][1]
    a = {r[acols.index("service")]: dict(zip(acols, r)) for r in seed["summary"]}
    assert set(a) == {sm.SVC_A, sm.SVC_B}                               # C 앵커 없음
    assert (a[sm.SVC_A]["gpu_rows"], a[sm.SVC_A]["serving_rows"], a[sm.SVC_A]["rejected_rows"]) == (3, 2, 1)
    assert (a[sm.SVC_B]["gpu_rows"], a[sm.SVC_B]["serving_rows"], a[sm.SVC_B]["rejected_rows"]) == (3, 0, 0)
    for r in a.values():                                                 # identity_drift 0건
        assert r["reported_service"] == r["service"] and r["reported_service_group"] == sm.SERVICE_GROUP


def test_token_side_values(sm, seed):
    cols = sm.SEED_TABLES["token_usage"][1]
    rows = [dict(zip(cols, r)) for r in seed["token_usage"]]
    sums = {}
    for r in rows:
        assert r["model"] == sm.MODEL_QWEN and r["created_by"] == "token-pipeline"
        assert r["total_input_tokens"] == r["input_tokens"] + r["cache_read_tokens"] + r["cache_creation_tokens"]
        assert r["org_path"] == ["unknown"] and r["cost"] is None and r["user_type"] == "identified"
        acc = sums.setdefault(r["service"], [0, 0, 0, 0, 0])
        for i, f in enumerate(("input_tokens", "cache_read_tokens", "cache_creation_tokens",
                               "output_tokens", "requests")):
            acc[i] += r[f]
    assert {k: tuple(v) for k, v in sums.items()} == {
        sm.SVC_A: (2_000_000, 5_000_000, 0, 250_000, 100),
        sm.SVC_B: (4_000_000, 10_000_000, 0, 500_000, 200),
        sm.SVC_D: (500_000, 0, 0, 0, 10)}
    acols = sm.SEED_TABLES["agg_service"][1]
    agg = {r[acols.index("service")]: dict(zip(acols, r)) for r in seed["agg_service"]}
    assert set(agg) == {sm.SVC_A, sm.SVC_B, sm.SVC_D}
    assert agg[sm.SVC_A]["input_tokens"] == 2_000_000 and agg[sm.SVC_A]["created_by"] == "token-pipeline"


def test_registries(sm, seed):
    ucols = sm.SEED_TABLES["dim_token_service"][1]
    usage = {r[ucols.index("service")]: dict(zip(ucols, r)) for r in seed["dim_token_service"]}
    assert set(usage) == {sm.SVC_A, sm.SVC_B, sm.SVC_C, sm.SVC_D}
    assert all(r["enabled"] == 1 and r["service_group"] == sm.SERVICE_GROUP for r in usage.values())
    mcols = sm.SEED_TABLES["dim_metrics_service"][1]
    reg = {r[mcols.index("service")]: dict(zip(mcols, r)) for r in seed["dim_metrics_service"]}
    assert set(reg) == {sm.SVC_A, sm.SVC_B, sm.SVC_C}
    assert {(k, r["expect_gpu"], r["expect_serving"], r["usage_includes_consumers"]) for k, r in reg.items()} == {
        (sm.SVC_A, 1, 1, 0), (sm.SVC_B, 1, 0, 0), (sm.SVC_C, 1, 1, 0)}
    assert all(r["enabled"] == 1 and r["coverage_since"] == date_cls(2026, 8, 26) and r["until"] is None
               for r in reg.values())


# ---------------------------------------------------------------- ddl_test_dims.sql 정본 교차 대조

def _sql_insert_values(sql: str, table: str) -> list[str]:
    """INSERT INTO <table> (cols) VALUES (...),(...); 의 각 값 튜플 문자열을 돌려준다(주석·세미콜론 없음 전제)."""
    m = re.search(rf"INSERT INTO {re.escape(table)} \([^)]*\) VALUES\s*(.*?);", sql, flags=re.S)
    assert m, table
    return re.findall(r"\(([^()]*)\)", m.group(1))


def test_ddl_test_dims_matches_python_twin_constants(sm):
    sql = (E2E_DIR / "ddl_test_dims.sql").read_text(encoding="utf-8")
    tco_rows = _sql_insert_values(sql, "gpu_data.dim_token_gpu_tco_dist")
    tco = {}
    for r in tco_rows:
        parts = [p.strip().strip("'") for p in r.split(",")]
        tco[parts[0]] = None if parts[2] == "NULL" else float(parts[2])
    assert tco == sm.TCO_KRW                                             # {'unknown': None, 'H100': 4200.0, ...}
    alloc_rows = _sql_insert_values(sql, "gpu_data.dim_token_gpu_allocation_dist")
    alloc = {}
    for r in alloc_rows:
        parts = [p.strip().strip("'") for p in r.split(",")]
        alloc[(parts[0], parts[1])] = None if parts[3] == "NULL" else float(parts[3])
    assert alloc == sm.ALLOCATION
    alias_rows = _sql_insert_values(sql, "gpu_data.dim_token_model_alias_dist")
    aliases = {}
    for r in alias_rows:
        parts = [p.strip().strip("'") for p in r.split(",")]
        aliases[parts[0]] = parts[2]
    assert aliases == sm.ALIASES
    assert len(_sql_insert_values(sql, "gpu_data.dim_token_vendor_price_dist")) == len(sm.VENDOR_PRICE)


# ---------------------------------------------------------------- 기대값 검산 (아웃라인 T10 Step 3 검산표)

def test_expect_values(me):
    exp = me.expect(DATE)
    assert list(exp.keys()) == ["EXP_M1_ROWS", "EXP_M1_QWEN_COST", "EXP_M3_FAIL_ROWS", "EXP_M3_WARN_ROWS",
                                "EXP_M4_ROWS", "EXP_M4_QWEN_SUM", "EXP_M2_ROWS", "EXP_M2_IDLE_H100",
                                "EXP_COVERAGE"]
    assert exp["EXP_M1_ROWS"] == 4                                       # (A,Qwen) (B,Qwen) (B,sonnet) (D,Qwen)
    assert exp["EXP_M1_QWEN_COST"] == pytest.approx(201_600.0)            # (40+8)h x 4200
    assert exp["EXP_M3_FAIL_ROWS"] == 2                                   # metrics_missing(C) + hours_over_count(B)
    assert exp["EXP_M3_WARN_ROWS"] == 3                                   # rows_rejected(A) gpu_type_no_tco(B/B200) no_allocation(B200)
    assert exp["EXP_M4_ROWS"] == 4                                        # Qwen x {A,B,D} + sonnet(B)
    assert exp["EXP_M4_QWEN_SUM"] == pytest.approx(201_600.0)             # 배분 합 == 원가 (share_sum_mismatch 0)
    assert exp["EXP_M2_ROWS"] == 3                                        # H100 / B200 / A100(할당만)
    assert exp["EXP_M2_IDLE_H100"] == pytest.approx(72.0)                 # 8x24 - (48 + 2 + 50 + 20)
    assert exp["EXP_COVERAGE"] == "2/3"


def test_m3_counts_breakdown(sm, me):
    counts = me.m3_counts(sm.build_seed(DATE))
    assert counts == {
        "metrics_missing": 1, "partial_load": 0, "rows_rejected": 1, "unregistered_model": 0,
        "hours_over_count": 1, "unknown_violation": 0, "pct_non_monotone": 0, "gpu_type_no_tco": 1,
        "serving_missing_for_gpu_model": 0, "serving_without_gpu_serving_row": 0, "identity_drift": 0,
        "service_not_in_usage_registry": 0, "manual_source": 1,
        "provider_ambiguous": 0, "consumer_tokens_exceed_provider": 0, "vendor_price_missing": 0,
        "no_allocation": 1, "sum_hours_over_allocation": 0,
        "gpu_block_empty_unexpected": 0, "serving_block_empty_unexpected": 0}
    assert sum(counts.values()) == 6


def test_m2_identity_gap_zero_where_tco_present(sm, me):
    rows = me.m2_rows(sm.build_seed(DATE))
    g = sm.SERVICE_GROUP
    assert set(rows) == {(g, "H100"), (g, "B200"), (g, "A100")}
    h100, b200, a100 = rows[(g, "H100")], rows[(g, "B200")], rows[(g, "A100")]
    assert h100["quality_flag"] == "flagged" and h100["identity_gap_krw"] == pytest.approx(0.0)
    assert h100["reported_gpu_hours_total"] == pytest.approx(120.0) and h100["idle_gpu_hours"] == pytest.approx(72.0)
    assert a100["quality_flag"] == "normal" and a100["idle_gpu_hours"] == pytest.approx(96.0)
    assert a100["identity_gap_krw"] == pytest.approx(0.0)
    assert b200["quality_flag"] == "no_tco" and b200["group_total_cost_krw"] is None


def test_main_prints_nine_key_value_lines(me, capsys):
    assert me.main([DATE]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 9 and all(re.fullmatch(r"EXP_[A-Z0-9_]+=[^\s]+", ln) for ln in lines)
    assert "EXP_M1_QWEN_COST=201600.0000" in lines and "EXP_COVERAGE=2/3" in lines
PYEOF
```

RED 실행 — 아직 시드 스크립트가 없으므로 fixture 로드에서 실패한다:

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_e2e_seed.py 2>&1 | tail -5
```

기대 출력(요지):

```
E   FileNotFoundError: [Errno 2] No such file or directory: '/home/mini/github/token-data-pipeline/mart/token-metrics/tests/e2e/seed_metrics.py'
...
12 errors in 0.2s
```

- [ ] **Step 3: `tests/e2e/seed_metrics.py` — 시나리오 시드(결정적, sha256 user_id, aware KST)**

원형 §14 `seed_fact.py`의 관례(`_client()` 지연 import·`client.insert(table, rows, column_names=…)`·date 객체·aware KST)를 따른다. 컬럼 튜플은 Plan 6a DDL(fact 3테이블·registry)과 기존 `dim_token_service.sql`·`mart_tables.sql`의 컬럼 순서 그대로다. `agg_token_service_1d`는 Nullable인 `reported_*`/`diff_*` 컬럼을 생략한 부분 컬럼 INSERT(기본 NULL).

```bash
cat > /home/mini/github/token-data-pipeline/mart/token-metrics/tests/e2e/seed_metrics.py <<'PYEOF'
#!/usr/bin/env python3
"""E2E 시드 — Plan 6c T10 시나리오(Mock Service A/B/C/D)를 단일노드 CH에 적재한다.

적재 대상(모두 <db>.<table>_dist — run_e2e.sh가 단일노드 MergeTree로 만든 대역):
  gpu_data.dim_token_service_dist          토큰 측 레지스트리(A/B/C/D enabled=1)
  gpu_data.dim_token_metrics_service_dist  메트릭 레지스트리(A/B/C — C는 앵커 없음 → metrics_missing FAIL, coverage 2/3)
  fact.raw_token_metrics_gpu_1d_dist       gpu 6행(A: Qwen3-32B/H100 serving·standby·test, B: claude-sonnet-5 3행 + FAIL 1)
  fact.raw_token_metrics_serving_1d_dist   serving 2행(A: ttft_ms·output_tps)
  fact.raw_token_metrics_summary_1d_dist   앵커 2행(A metrics-api-v1 rejected 1, B manual-v0)
  mart.token_usage_1d_dist                 토큰 6행(A/B/D × 합성 사용자 2 — Qwen3-32B)
  mart.agg_token_service_1d_dist           서비스 집계 3행(A/B/D)

결정성: 수치는 전부 모듈 상수, 합성 user_id는 sha256(f"{service}|{date}|{k}") 앞 12자 — random 미사용.
정본 이원화: TCO_KRW/ALLOCATION/ALIASES/VENDOR_PRICE는 tests/e2e/ddl_test_dims.sql 시드의 파이썬 재현이며
tests/test_e2e_seed.py가 두 파일을 교차 대조한다(값을 고치면 둘 다 고친다).

사용법: CH_HOST=127.0.0.1 CH_PORT=18124 python3 tests/e2e/seed_metrics.py 2026-09-03
"""
import hashlib
import os
import sys
from datetime import date as date_cls
from datetime import datetime, time, timedelta, timezone

KST = timezone(timedelta(hours=9))

DB_FACT = os.getenv("CH_DB_FACT", "fact")
DB_DIM = os.getenv("CH_DB_DIM", "gpu_data")
DB_MART = os.getenv("CH_DB_MART", "mart")

SERVICE_GROUP = "Mock Group"
SVC_A = "Mock Service A"
SVC_B = "Mock Service B"
SVC_C = "Mock Service C"
SVC_D = "Mock Service D"
MODEL_QWEN = "Qwen3-32B"
MODEL_SONNET = "claude-sonnet-5"
SOURCE_API = "metrics-api-v1"
SOURCE_MANUAL = "manual-v0"
CREATED_BY_TOKEN_MART = "token-pipeline"       # mart.token_usage_1d CHECK created_by != '' (token-usage 배치의 값)
REGISTRY_SINCE = date_cls(2026, 8, 26)          # api_since = coverage_since (digest §20 fixture와 동일)

# ---- ddl_test_dims.sql 시드의 파이썬 재현(교차 대조 대상) ----
TCO_KRW = {"unknown": None, "H100": 4200.0, "A100": 2100.0, "H200": 5300.0, "L40S": 1300.0}
ALLOCATION = {("unknown", "unknown"): None, ("Mock Group", "H100"): 8.0, ("Mock Group", "A100"): 4.0}
ALIASES = {
    "unknown": "unknown",
    "claude-opus-4-8": "claude-opus-4-8",
    "claude-sonnet-5": "claude-sonnet-5",
    "claude-haiku-4-5": "claude-haiku-4-5",
    "claude-sonnet-5-20260101": "claude-sonnet-5",
    "opus-4.8": "claude-opus-4-8",
    "Qwen3-32B": "Qwen3-32B",
}
VENDOR_PRICE = {
    ("unknown", "unknown"): (None, None, None, None),
    ("anthropic", "claude-opus-4-8"): (6750.0, 675.0, 8437.5, 33750.0),
    ("anthropic", "claude-sonnet-5"): (4050.0, 405.0, 5062.5, 20250.0),
    ("anthropic", "claude-haiku-4-5"): (1350.0, 135.0, 1687.5, 6750.0),
}

# ---- 시나리오 상수 ----
USAGE_REGISTRY = (SVC_A, SVC_B, SVC_C, SVC_D)
# (service, expect_gpu, expect_serving, usage_includes_consumers)
METRICS_REGISTRY = ((SVC_A, 1, 1, 0), (SVC_B, 1, 0, 0), (SVC_C, 1, 1, 0))
# (service, model, gpu_type, category, gpu_count, gpu_hours, flags, source_type)
GPU_ROWS = (
    (SVC_A, MODEL_QWEN, "H100", "serving", 2.0, 40.0, (), SOURCE_API),
    (SVC_A, MODEL_QWEN, "H100", "standby", 1.0, 8.0, (), SOURCE_API),
    (SVC_A, MODEL_QWEN, "H100", "test", 1.0, 2.0, (), SOURCE_API),
    (SVC_B, MODEL_SONNET, "H100", "serving", 1.0, 20.0, (), SOURCE_MANUAL),
    (SVC_B, MODEL_SONNET, "B200", "serving", 1.0, 4.0, (), SOURCE_MANUAL),
    (SVC_B, MODEL_SONNET, "H100", "standby", 2.0, 50.0, ("hours_over_count",), SOURCE_MANUAL),
)
# (service, model, metric, name, unit, p50, p90, p95, p99, flags, source_type)
SERVING_ROWS = (
    (SVC_A, MODEL_QWEN, "ttft_ms", "", "ms", 120.0, 240.0, 300.0, 450.0, (), SOURCE_API),
    (SVC_A, MODEL_QWEN, "output_tps", "", "tokens/s", 80.0, 60.0, 55.0, 40.0, (), SOURCE_API),
)
# (service, gpu_rows, serving_rows, custom_rows, rejected_rows, merged_dups, source_type)
SUMMARY_ROWS = ((SVC_A, 3, 2, 0, 1, 0, SOURCE_API), (SVC_B, 3, 0, 0, 0, 0, SOURCE_MANUAL))
# (service, input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, requests) — 모델은 전부 Qwen3-32B
TOKEN_SCENARIO = (
    (SVC_A, 2_000_000, 5_000_000, 0, 250_000, 100),
    (SVC_B, 4_000_000, 10_000_000, 0, 500_000, 200),
    (SVC_D, 500_000, 0, 0, 0, 10),
)

# ---- 컬럼 순서(DDL 정본과 1:1) ----
DIM_TOKEN_SERVICE_COLS = ("service_group", "service", "base_url", "enabled", "source_type", "note", "updated_at")
DIM_METRICS_SERVICE_COLS = ("service_group", "service", "base_url", "enabled", "api_since", "coverage_since", "until",
                            "expect_gpu", "expect_serving", "usage_includes_consumers", "note", "updated_at")
GPU_COLS = ("date", "service_group", "service", "model", "gpu_type", "category", "gpu_count", "gpu_hours", "flags",
            "source_type", "generated_at", "collected_at")
SERVING_COLS = ("date", "service_group", "service", "model", "metric", "name", "unit", "p50", "p90", "p95", "p99",
                "flags", "source_type", "generated_at", "collected_at")
SUMMARY_COLS = ("date", "service_group", "service", "reported_service_group", "reported_service", "engine_type",
                "engine_version", "gpu_rows", "serving_rows", "custom_rows", "rejected_rows", "merged_dups",
                "source_type", "generated_at", "collected_at")
TOKEN_USAGE_COLS = ("date", "service_group", "service", "user_id", "user_type", "user_name", "model", "org_path",
                    "org_top", "org_leaf", "input_tokens", "cache_read_tokens", "cache_creation_tokens",
                    "output_tokens", "total_input_tokens", "requests", "cost", "created_by")
AGG_SERVICE_COLS = ("date", "service_group", "service", "input_tokens", "cache_read_tokens", "cache_creation_tokens",
                    "output_tokens", "total_input_tokens", "requests", "distinct_users", "cost", "is_derived",
                    "created_by")

SEED_TABLES = {
    "dim_token_service": (f"{DB_DIM}.dim_token_service_dist", DIM_TOKEN_SERVICE_COLS),
    "dim_metrics_service": (f"{DB_DIM}.dim_token_metrics_service_dist", DIM_METRICS_SERVICE_COLS),
    "gpu": (f"{DB_FACT}.raw_token_metrics_gpu_1d_dist", GPU_COLS),
    "serving": (f"{DB_FACT}.raw_token_metrics_serving_1d_dist", SERVING_COLS),
    "summary": (f"{DB_FACT}.raw_token_metrics_summary_1d_dist", SUMMARY_COLS),
    "token_usage": (f"{DB_MART}.token_usage_1d_dist", TOKEN_USAGE_COLS),
    "agg_service": (f"{DB_MART}.agg_token_service_1d_dist", AGG_SERVICE_COLS),
}


def synthetic_user_id(service: str, date: str, k: int) -> str:
    """합성 user_id — 결정적(sha256), 실제 사번/이메일 형태 아님."""
    return "u-" + hashlib.sha256(f"{service}|{date}|{k}".encode("utf-8")).hexdigest()[:12]


def _split_two(value: int) -> tuple:
    """서비스 합계를 합성 사용자 2명으로 나눈다(합이 보존되도록 floor/나머지)."""
    return (value // 2, value - value // 2)


def _base_url(service: str) -> str:
    return f"http://mock-{service[-1].lower()}.invalid"


def build_seed(date: str) -> dict:
    """date(YYYY-MM-DD) 하루치 시드 — SEED_TABLES 키 순서의 {key: [tuple, ...]} (컬럼 순서 = SEED_TABLES[key][1])."""
    d = date_cls.fromisoformat(date)
    next_day = d + timedelta(days=1)
    generated_at = datetime.combine(next_day, time(2, 5), tzinfo=KST)   # 수집기 계약: D+1 02:05 KST 생성
    collected_at = datetime.combine(next_day, time(4, 5), tzinfo=KST)   # D+1 04:05 KST 수집
    registry_updated_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=KST)

    seed = {}
    seed["dim_token_service"] = [
        (SERVICE_GROUP, svc, _base_url(svc), 1, "usage-api-v1", "e2e", registry_updated_at)
        for svc in USAGE_REGISTRY]
    seed["dim_metrics_service"] = [
        (SERVICE_GROUP, svc, _base_url(svc), 1, REGISTRY_SINCE, REGISTRY_SINCE, None,
         expect_gpu, expect_serving, uic, "e2e", registry_updated_at)
        for svc, expect_gpu, expect_serving, uic in METRICS_REGISTRY]
    seed["gpu"] = [
        (d, SERVICE_GROUP, svc, model, gpu_type, category, gpu_count, gpu_hours, list(flags), source_type,
         generated_at, collected_at)
        for svc, model, gpu_type, category, gpu_count, gpu_hours, flags, source_type in GPU_ROWS]
    seed["serving"] = [
        (d, SERVICE_GROUP, svc, model, metric, name, unit, p50, p90, p95, p99, list(flags), source_type,
         generated_at, collected_at)
        for svc, model, metric, name, unit, p50, p90, p95, p99, flags, source_type in SERVING_ROWS]
    seed["summary"] = [
        (d, SERVICE_GROUP, svc, SERVICE_GROUP, svc, "", "", gpu_rows, serving_rows, custom_rows, rejected_rows,
         merged_dups, source_type, generated_at, collected_at)
        for svc, gpu_rows, serving_rows, custom_rows, rejected_rows, merged_dups, source_type in SUMMARY_ROWS]

    token_rows = []
    agg_rows = []
    for svc, input_tokens, cache_read, cache_creation, output_tokens, requests in TOKEN_SCENARIO:
        for k in (0, 1):
            i, cr, cc, o, r = (_split_two(v)[k] for v in (input_tokens, cache_read, cache_creation,
                                                          output_tokens, requests))
            token_rows.append((d, SERVICE_GROUP, svc, synthetic_user_id(svc, date, k), "identified", "",
                               MODEL_QWEN, ["unknown"], "unknown", "unknown",
                               i, cr, cc, o, i + cr + cc, r, None, CREATED_BY_TOKEN_MART))
        agg_rows.append((d, SERVICE_GROUP, svc, input_tokens, cache_read, cache_creation, output_tokens,
                         input_tokens + cache_read + cache_creation, requests, 2, None, 0, CREATED_BY_TOKEN_MART))
    seed["token_usage"] = token_rows
    seed["agg_service"] = agg_rows
    return seed


def seed_all(client, date: str) -> dict:
    """build_seed(date)를 SEED_TABLES 순서로 INSERT — {key: rows}. 멱등 아님(run_e2e.sh가 새 컨테이너에 1회 호출)."""
    counts = {}
    for key, rows in build_seed(date).items():
        table, cols = SEED_TABLES[key]
        client.insert(table, rows, column_names=list(cols))
        counts[key] = len(rows)
    return counts


def _client():
    """clickhouse_connect는 여기서만 import — 단위 테스트(tests/test_e2e_seed.py)는 드라이버 없이 build_seed만 쓴다."""
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", "127.0.0.1"),
        port=int(os.getenv("CH_PORT", "8123")),
        username=os.getenv("CH_USER", "default"),
        password=os.getenv("CH_PASSWORD", ""),
    )


def main(argv=None) -> int:
    args = sys.argv[1:] if argv is None else list(argv)
    if len(args) != 1:
        print("usage: seed_metrics.py <YYYY-MM-DD>", file=sys.stderr)
        return 2
    try:
        date_cls.fromisoformat(args[0])
    except ValueError:
        print("date must be YYYY-MM-DD", file=sys.stderr)
        return 2
    counts = seed_all(_client(), args[0])
    for key, n in counts.items():
        print(f"seeded {SEED_TABLES[key][0]} rows={n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
PYEOF
```

부분 확인(스크립트만 로드 — 아직 `mart_expectations.py`가 없어 GREEN은 스텝 5에서):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python3 -c "
import importlib.util, sys
spec = importlib.util.spec_from_file_location('seed_metrics', 'tests/e2e/seed_metrics.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
s = m.build_seed('2026-09-03')
print({k: len(v) for k, v in s.items()})
print(s['token_usage'][0][3], s['gpu'][5][8], s['gpu'][0][10].isoformat())
"
```

기대 출력:

```
{'dim_token_service': 4, 'dim_metrics_service': 3, 'gpu': 6, 'serving': 2, 'summary': 2, 'token_usage': 6, 'agg_service': 3}
u-80eda9e5384d ['hours_over_count'] 2026-09-04T02:05:00+09:00
```

- [ ] **Step 4: `tests/e2e/mart_expectations.py` — 시드를 `app.mart`의 비용 함수로 재계산해 `key=value` 9줄 출력**

원형 §17과 같은 역할이되, mock datagen 대신 `seed_metrics.build_seed(date)`를 입력으로 M1/M3/M4/M2 규칙(T2 함수 + T4/T6/T7 술어)을 파이썬으로 재현한다. 값이 SQL과 어긋나면 `verify_expected_results.sql`이 잡는다(정본 이원화는 원형과 동일한 의도된 교차 검증).

```bash
cat > /home/mini/github/token-data-pipeline/mart/token-metrics/tests/e2e/mart_expectations.py <<'PYEOF'
#!/usr/bin/env python3
"""E2E 기대값 산출 — seed_metrics.build_seed(date)를 app.mart(T2)의 비용 함수와 M3/M4/M2 규칙(T4/T6/T7)으로
재계산해 `key=value` 9줄을 출력한다(줄마다 1개). run_e2e.sh가 셸 연관배열 EXP[...]에 담아
verify_expected_results.sql의 {EXP_*} 토큰을 sed로 치환한다 (Plan 6c T10).

    python3 tests/e2e/mart_expectations.py 2026-09-03
    EXP_M1_ROWS=4
    EXP_M1_QWEN_COST=201600.0000
    ...
    EXP_COVERAGE=2/3

정본 이원화 주의: 여기의 판정 규칙은 app/steps.py SQL의 파이썬 재현이다 — SQL 술어를 고치면 여기도 고친다
(어긋나면 verify_expected_results.sql expect-empty가 실패한다 — 그것이 이 파일의 존재 이유).
"""
import pathlib
import sys
from types import SimpleNamespace

HERE = pathlib.Path(__file__).resolve().parent            # mart/token-metrics/tests/e2e
sys.path.insert(0, str(HERE.parents[1]))                   # mart/token-metrics  → `from app import mart`
sys.path.insert(0, str(HERE))                              # tests/e2e           → `import seed_metrics`

from app import mart              # noqa: E402
import seed_metrics as sm         # noqa: E402

EXP_KEYS = ("EXP_M1_ROWS", "EXP_M1_QWEN_COST", "EXP_M3_FAIL_ROWS", "EXP_M3_WARN_ROWS", "EXP_M4_ROWS",
            "EXP_M4_QWEN_SUM", "EXP_M2_ROWS", "EXP_M2_IDLE_H100", "EXP_COVERAGE")
TOKEN_FIELDS = ("input_tokens", "cache_read_tokens", "cache_creation_tokens", "output_tokens", "requests")
# T4 core 13 + T6 stretch 3 + T7 stretch 4 = 20블록, severity는 steps.M3_BLOCKS와 동일
M3_SEVERITY = {
    "metrics_missing": "FAIL", "partial_load": "FAIL", "rows_rejected": "WARN", "unregistered_model": "WARN",
    "hours_over_count": "FAIL", "unknown_violation": "FAIL", "pct_non_monotone": "FAIL",
    "gpu_type_no_tco": "WARN", "serving_missing_for_gpu_model": "WARN",
    "serving_without_gpu_serving_row": "WARN", "identity_drift": "WARN",
    "service_not_in_usage_registry": "WARN", "manual_source": "INFO",
    "provider_ambiguous": "WARN", "consumer_tokens_exceed_provider": "WARN", "vendor_price_missing": "WARN",
    "no_allocation": "WARN", "sum_hours_over_allocation": "FAIL",
    "gpu_block_empty_unexpected": "WARN", "serving_block_empty_unexpected": "WARN",
}
M2_HOURS_PER_DAY = 24.0


def _rows(seed, key):
    cols = sm.SEED_TABLES[key][1]
    return [dict(zip(cols, r)) for r in seed[key]]


def _canon(model: str) -> str:
    """T3 canon(x) = if(alias.canonical = '', x, alias.canonical)."""
    return sm.ALIASES.get(model, model)


def _ctx(seed) -> SimpleNamespace:
    """시드에서 파생 구조를 한 번만 만든다(T3 SUB_* 서브쿼리의 파이썬 대응)."""
    dated = [r for key in ("summary", "gpu", "serving", "token_usage") for r in _rows(seed, key)]
    d = dated[0]["date"]
    reg = {r["service"]: r for r in _rows(seed, "dim_metrics_service")}
    reg_enabled = {s for s, r in reg.items() if r["enabled"] == 1}
    expected = {s for s in reg_enabled
                if reg[s]["coverage_since"] <= d and (reg[s]["until"] is None or d <= reg[s]["until"])}
    usage_svc = {r["service"] for r in _rows(seed, "dim_token_service") if r["enabled"] == 1}
    anchors = {r["service"]: r for r in _rows(seed, "summary") if r["date"] == d}
    gpu_raw = [r for r in _rows(seed, "gpu") if r["date"] == d]
    serving_raw = [r for r in _rows(seed, "serving") if r["date"] == d]
    gpu = {}                                                   # (service, canon) -> [(category, gpu_type, hours, flags)]
    for r in gpu_raw:
        if r["service"] in anchors:
            gpu.setdefault((r["service"], _canon(r["model"])), []).append(
                (r["category"], r["gpu_type"], r["gpu_hours"], list(r["flags"])))
    serving = {}                                               # (service, canon) -> [row]
    for r in serving_raw:
        if r["service"] in anchors:
            serving.setdefault((r["service"], _canon(r["model"])), []).append(r)
    tokens = {}                                                # (service, canon) -> [input, cache_read, cache_creation, output, requests]
    for r in _rows(seed, "token_usage"):
        if r["date"] == d and r["service"] in usage_svc:
            acc = tokens.setdefault((r["service"], _canon(r["model"])), [0, 0, 0, 0, 0])
            for i, f in enumerate(TOKEN_FIELDS):
                acc[i] += r[f]
    tco = {k: v for k, v in sm.TCO_KRW.items()}                # 전 행 effective_from 2026-01-01 ≤ d
    alloc = {k: v for k, v in sm.ALLOCATION.items() if k[1] != "unknown"}   # SUB_EFF_ALLOC HAVING gpu_type != 'unknown'
    group_of = {r["service"]: r["service_group"] for r in _rows(seed, "dim_token_service")}
    group_of.update({s: r["service_group"] for s, r in anchors.items()})
    return SimpleNamespace(d=d, reg=reg, reg_enabled=reg_enabled, expected=expected, usage_svc=usage_svc,
                           anchors=anchors, gpu_raw=gpu_raw, serving_raw=serving_raw, gpu=gpu, serving=serving,
                           tokens=tokens, tco=tco, alloc=alloc, group_of=group_of)


def _partial(c, svc: str) -> bool:
    """partial_load: 앵커 gpu_rows/serving_rows가 실제 fact 행수와 다르면 True(serving은 metric != 'custom')."""
    a = c.anchors[svc]
    n_gpu = sum(1 for r in c.gpu_raw if r["service"] == svc)
    n_serving = sum(1 for r in c.serving_raw if r["service"] == svc and r["metric"] != "custom")
    return a["gpu_rows"] != n_gpu or a["serving_rows"] != n_serving


def m1_rows(seed) -> dict:
    """M1 agg_token_model_cost_1d — (service, canon) -> {model_cost_krw, weighted_tokens, requests, has_*, quality_flag}."""
    c = _ctx(seed)
    out = {}
    for key in sorted(set(c.gpu) | set(c.tokens)):
        svc, _model = key
        gpu_rows = c.gpu.get(key, [])
        tok = c.tokens.get(key)
        cost = mart.model_cost(gpu_rows, c.tco)
        wt = mart.weighted_tokens(tok[0], tok[1], tok[2], tok[3]) if tok else 0.0
        anchor = c.anchors.get(svc)
        partial = anchor is not None and _partial(c, svc)
        no_tco = bool(gpu_rows) and cost is None
        flagged = any(mart.is_fail(flags) for _, _, _, flags in gpu_rows)
        manual = anchor is not None and anchor["source_type"] == sm.SOURCE_MANUAL
        no_metrics = svc in c.expected and anchor is None
        consumer_only = tok is not None and not gpu_rows
        out[key] = {
            "model_cost_krw": cost, "weighted_tokens": wt, "requests": tok[4] if tok else 0,
            "has_gpu_rows": int(bool(gpu_rows)), "has_token_rows": int(tok is not None),
            "quality_flag": mart.quality_flag_m1(partial, no_tco, flagged, manual, no_metrics, consumer_only),
        }
    return out


def _providers(c, model: str) -> list:
    """§6.4 (4) provider(m) = FAIL 없는 serving/standby gpu 행이 있는 (앵커) 서비스 — 정렬."""
    return sorted({svc for (svc, m), rows in c.gpu.items() if m == model
                   and any(cat in ("serving", "standby") and not mart.is_fail(flags) for cat, _, _, flags in rows)})


def _vendor_price(model: str):
    for (_provider, m), price in sm.VENDOR_PRICE.items():
        if m == model:
            return _provider, price
    return "", (None, None, None, None)


def m4_rows(seed) -> dict:
    """M4 agg_token_model_share_1d — (model, service) -> {provider_service, is_provider, denominator_mode, share,
    allocated_cost_krw, quality_flag}. 모드 판정 순서 = SQL_M4 mode CTE(T6)."""
    c = _ctx(seed)
    m1 = m1_rows(seed)
    out = {}
    for model in sorted({m for _, m in m1}):
        providers = _providers(c, model)
        has_gpu = any(m == model for _, m in c.gpu)
        wtokens = {svc: m1[(svc, m)]["weighted_tokens"] for (svc, m) in m1 if m == model and m1[(svc, m)]["has_token_rows"]}
        w_all = sum(wtokens.values())
        provider = providers[0] if len(providers) == 1 else ""
        prov_m1 = m1.get((provider, model)) if provider else None
        cost = prov_m1["model_cost_krw"] if prov_m1 else None
        prov_quality = prov_m1["quality_flag"] if prov_m1 else ("no_tco" if provider else "")
        uic = bool(provider) and c.reg.get(provider, {}).get("usage_includes_consumers", 0) == 1
        w_prov = wtokens.get(provider, 0.0) if provider else 0.0
        w_m = w_prov if uic else w_all
        if len(providers) >= 2:
            mode = "provider_ambiguous"
        elif not providers and has_gpu:
            mode = "no_provider"
        elif not providers:
            mode = "external_api"
        elif w_m == 0 and (cost or 0.0) > 0:
            mode = "token_not_reported"
        elif uic:
            mode = "provider_reported"
        else:
            mode = "all_services"
        vendor, price = _vendor_price(model) if mode == "external_api" else ("", (None, None, None, None))
        others = sum(w for s, w in wtokens.items() if s != provider)
        for svc in sorted(set(wtokens) | set(providers)):
            is_provider = int(svc in providers) if mode == "provider_ambiguous" else int(svc == provider)
            w_s = wtokens.get(svc, 0.0)
            if mode == "provider_reported" and is_provider:
                w_s = max(w_prov - others, 0.0)
            share = (w_s / w_m) if w_m > 0 else None
            allocated = None
            if mode == "provider_ambiguous":
                share = None
            elif mode == "no_provider":
                allocated = 0.0
            elif mode == "external_api":
                tok = c.tokens.get((svc, model), [0, 0, 0, 0, 0])
                allocated = mart.external_api_cost(tok[0], tok[1], tok[2], tok[3], price)
            elif mode == "token_not_reported":
                share = 1.0 if is_provider else None
                allocated = cost if is_provider else None
            elif cost is not None and w_m > 0:
                allocated = cost * w_s / w_m
            quality = ("partial" if prov_quality == "partial" else "no_tco" if prov_quality == "no_tco"
                       else "provider_ambiguous" if mode == "provider_ambiguous"
                       else "vendor_price_missing" if mode == "external_api" and allocated is None
                       else "token_not_reported" if mode == "token_not_reported" else "normal")
            provider_service = provider if provider else (vendor if mode == "external_api" else "")
            if mode == "provider_ambiguous" and is_provider:
                provider_service = svc
            out[(model, svc)] = {"provider_service": provider_service, "is_provider": is_provider,
                                 "denominator_mode": mode, "share": share, "allocated_cost_krw": allocated,
                                 "quality_flag": quality, "model_cost_krw": cost if mode != "no_provider" else 0.0}
    return out


def m2_rows(seed) -> dict:
    """M2 agg_token_gpu_group_1d — (service_group, gpu_type) -> group_overhead(...) + reported/allocated/quality.
    행 집합 = 앵커 서비스의 gpu 행이 있는 그룹 ∪ (unknown 아닌 할당 행 AND 그룹 내 앵커 서비스 ≥ 1)."""
    c = _ctx(seed)
    sums = {}
    for (svc, _m), rows in c.gpu.items():
        group = c.group_of[svc]
        for category, gpu_type, hours, flags in rows:
            acc = sums.setdefault((group, gpu_type), {"serving": 0.0, "standby": 0.0, "test": 0.0, "flagged": 0.0})
            if mart.is_fail(flags):
                acc["flagged"] += float(hours)
            else:
                acc[category] += float(hours)
    anchor_groups = {c.group_of[s] for s in c.anchors}
    keys = set(sums) | {k for k, cnt in c.alloc.items() if cnt is not None and k[0] in anchor_groups}
    out = {}
    for key in sorted(keys):
        acc = sums.get(key, {"serving": 0.0, "standby": 0.0, "test": 0.0, "flagged": 0.0})
        count = c.alloc.get(key)
        allocated_hours = count * M2_HOURS_PER_DAY if count is not None else None
        reported = acc["serving"] + acc["standby"] + acc["test"] + acc["flagged"]
        tco = c.tco.get(key[1])
        row = mart.group_overhead(allocated_hours, reported, acc["serving"], acc["standby"], acc["test"],
                                  acc["flagged"], tco)
        row["allocated_gpu_hours"] = allocated_hours
        row["reported_gpu_hours_total"] = reported
        row["flagged_gpu_hours"] = acc["flagged"]
        row["tco_missing"] = int(tco is None)
        # T7-2 우선순위: over_report > no_tco > no_allocation > flagged > normal
        row["quality_flag"] = ("over_report" if row["over_report"] == 1 else "no_tco" if tco is None
                               else "no_allocation" if allocated_hours is None
                               else "flagged" if acc["flagged"] > 0 else "normal")
        out[key] = row
    return out


def m3_counts(seed) -> dict:
    """M3 check_token_metrics_1d — 20블록 이름 -> 기대 행수(T4/T6/T7 술어의 파이썬 재현)."""
    c = _ctx(seed)
    n = {name: 0 for name in M3_SEVERITY}
    fact_svcs = {r["service"] for r in c.gpu_raw} | {r["service"] for r in c.serving_raw}
    n["metrics_missing"] = len([s for s in c.expected if s not in c.anchors])
    n["partial_load"] = (sum(1 for s in c.anchors if _partial(c, s)) + len(fact_svcs - set(c.anchors)))
    n["rows_rejected"] = sum(1 for a in c.anchors.values() if a["rejected_rows"] > 0)
    models_seen = ({(r["service"], r["model"]) for r in c.gpu_raw if r["service"] in c.anchors}
                   | {(r["service"], r["model"]) for r in c.serving_raw if r["service"] in c.anchors}
                   | {(r["service"], r["model"]) for r in _rows(seed, "token_usage") if r["service"] in c.usage_svc})
    n["unregistered_model"] = len({k for k in models_seen if k[1] not in sm.ALIASES})
    for flag in ("hours_over_count", "unknown_violation"):
        n[flag] = len({(r["service"], _canon(r["model"]), r["gpu_type"]) for r in c.gpu_raw
                       if r["service"] in c.anchors and flag in r["flags"]})
    n["pct_non_monotone"] = sum(1 for r in c.serving_raw if r["service"] in c.anchors and "pct_non_monotone" in r["flags"])
    n["gpu_type_no_tco"] = len({(svc, gpu_type) for (svc, _m), rows in c.gpu.items()
                                for cat, gpu_type, _h, flags in rows
                                if cat in ("serving", "standby") and not mart.is_fail(flags)
                                and c.tco.get(gpu_type) is None})
    for key, rows in c.gpu.items():
        has_serving_gpu = any(cat == "serving" for cat, _g, _h, _f in rows)
        requests = c.tokens.get(key, [0, 0, 0, 0, 0])[4]
        if has_serving_gpu and key not in c.serving and requests > 0:
            n["serving_missing_for_gpu_model"] += 1
    for key in c.serving:
        has_serving_gpu = any(cat == "serving" for cat, _g, _h, _f in c.gpu.get(key, []))
        if not has_serving_gpu and c.reg.get(key[0], {}).get("expect_gpu", 0) == 1:
            n["serving_without_gpu_serving_row"] += 1
    n["identity_drift"] = sum(1 for a in c.anchors.values() if a["source_type"] == sm.SOURCE_API
                              and (a["reported_service"] != a["service"]
                                   or a["reported_service_group"] != a["service_group"]))
    n["service_not_in_usage_registry"] = len(c.reg_enabled - c.usage_svc)
    n["manual_source"] = sum(1 for a in c.anchors.values() if a["source_type"] == sm.SOURCE_MANUAL)
    m4 = m4_rows(seed)
    models = {m for m, _s in m4}
    n["provider_ambiguous"] = sum(1 for m in models if len(_providers(c, m)) >= 2)
    for m in models:
        rows = [r for (mm, _s), r in m4.items() if mm == m]
        if rows and rows[0]["denominator_mode"] == "provider_reported":
            provider = rows[0]["provider_service"]
            m1 = m1_rows(seed)
            w_prov = m1.get((provider, m), {"weighted_tokens": 0.0})["weighted_tokens"]
            others = sum(m1[(s, mm)]["weighted_tokens"] for (s, mm) in m1 if mm == m and s != provider)
            n["consumer_tokens_exceed_provider"] += int(others > w_prov)
    n["vendor_price_missing"] = sum(1 for r in m4.values() if r["quality_flag"] == "vendor_price_missing")
    m2 = m2_rows(seed)
    for key, row in m2.items():
        has_gpu_rows = any(c.group_of[svc] == key[0] and gpu_type == key[1]
                           for (svc, _m), rows in c.gpu.items() for _c, gpu_type, _h, _f in rows)
        if has_gpu_rows and row["allocated_gpu_hours"] is None:
            n["no_allocation"] += 1
        if row["allocated_gpu_hours"] is not None and row["reported_gpu_hours_total"] > row["allocated_gpu_hours"]:
            n["sum_hours_over_allocation"] += 1
    for svc, a in c.anchors.items():
        reg = c.reg.get(svc, {})
        n["gpu_block_empty_unexpected"] += int(a["gpu_rows"] == 0 and reg.get("expect_gpu", 0) == 1)
        n["serving_block_empty_unexpected"] += int(a["serving_rows"] == 0 and reg.get("expect_serving", 0) == 1)
    return n


def expect(date: str) -> dict:
    """run_e2e.sh 계약 — EXP_KEYS 순서의 9키."""
    seed = sm.build_seed(date)
    c = _ctx(seed)
    m1, m3, m4, m2 = m1_rows(seed), m3_counts(seed), m4_rows(seed), m2_rows(seed)
    coverage = mart.compute_coverage(c.expected, set(c.anchors), [])
    return {
        "EXP_M1_ROWS": len(m1),
        "EXP_M1_QWEN_COST": m1[(sm.SVC_A, sm.MODEL_QWEN)]["model_cost_krw"],
        "EXP_M3_FAIL_ROWS": sum(v for k, v in m3.items() if M3_SEVERITY[k] == "FAIL"),
        "EXP_M3_WARN_ROWS": sum(v for k, v in m3.items() if M3_SEVERITY[k] == "WARN"),
        "EXP_M4_ROWS": len(m4),
        "EXP_M4_QWEN_SUM": sum(r["allocated_cost_krw"] for (m, _s), r in m4.items()
                               if m == sm.MODEL_QWEN and r["allocated_cost_krw"] is not None),
        "EXP_M2_ROWS": len(m2),
        "EXP_M2_IDLE_H100": m2[(sm.SERVICE_GROUP, "H100")]["idle_gpu_hours"],
        "EXP_COVERAGE": f"{coverage.present}/{coverage.enabled}",
    }


def _fmt(value) -> str:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"unexpected expectation value: {value!r}")
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def main(argv=None) -> int:
    args = sys.argv[1:] if argv is None else list(argv)
    if len(args) != 1:
        print("usage: mart_expectations.py <YYYY-MM-DD>", file=sys.stderr)
        return 2
    for key, value in expect(args[0]).items():
        print(f"{key}={_fmt(value)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
PYEOF
```

- [ ] **Step 5: GREEN — 단위 테스트 통과 + 기대값 출력 확인**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_e2e_seed.py
```

기대 출력: `12 passed`.

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python3 tests/e2e/mart_expectations.py 2026-09-03
```

기대 출력(9줄, 순서 고정):

```
EXP_M1_ROWS=4
EXP_M1_QWEN_COST=201600.0000
EXP_M3_FAIL_ROWS=2
EXP_M3_WARN_ROWS=3
EXP_M4_ROWS=4
EXP_M4_QWEN_SUM=201600.0000
EXP_M2_ROWS=3
EXP_M2_IDLE_H100=72.0000
EXP_COVERAGE=2/3
```

전체 스위트도 회귀 없음: `cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q` → 기대 `… passed`(T1–T9 테스트 수 + 12).

- [ ] **Step 6: `tests/e2e/verify_expected_results.sql` — expect-empty 20검사(M1/M3/M4/M2 + created_by + 2회 실행 무중복)**

원형 §16 패턴: 불일치 행만 SELECT, 출력 없으면 통과. UNION 체인의 supertype 문제(UInt64/Float64, CH 24.8 NO_COMMON_TYPE)를 피하려고 **모든 검사의 actual/expected를 `toInt64(...)`로 통일**한다(원화는 `round`, 시간은 ×1000 정수). 존재 단정은 `countIf(...) = 1` 형태로 합쳐 대상 행이 없어도 실패하게 한다. 토큰: `{DATE}` + `{EXP_M1_ROWS} {EXP_M1_QWEN_COST} {EXP_M3_FAIL_ROWS} {EXP_M3_WARN_ROWS} {EXP_M4_ROWS} {EXP_M4_QWEN_SUM} {EXP_M2_ROWS} {EXP_M2_IDLE_H100}`(`EXP_COVERAGE`는 마커 grep으로 검증 — SQL 미사용). 세미콜론 없음(HTTP 단일 쿼리).

```bash
cat > /home/mini/github/token-data-pipeline/mart/token-metrics/tests/e2e/verify_expected_results.sql <<'SQL'
-- expect-empty 방식: 기대와 다른(또는 필요한 행이 없는) 경우만 SELECT — 출력 없으면 통과
-- (mart/token-usage/tests/e2e/verify_expected_results.sql 패턴 재사용, Plan 6c T10).
-- 실행 전 치환: {DATE} {EXP_M1_ROWS} {EXP_M1_QWEN_COST} {EXP_M3_FAIL_ROWS} {EXP_M3_WARN_ROWS}
--              {EXP_M4_ROWS} {EXP_M4_QWEN_SUM} {EXP_M2_ROWS} {EXP_M2_IDLE_H100}
-- 모든 actual/expected는 toInt64 — UNION 체인의 UInt64/Float64 supertype 부재(NO_COMMON_TYPE, CH 24.8) 회피.
-- run_e2e.sh는 배치를 2회 실행한 뒤 이 파일을 1회 실행한다(행수 == 기대 = 멱등 검증).

-- === 1) M1 agg_token_model_cost_1d ===

SELECT 'm1_rows' AS check_name, toInt64(count()) AS actual, toInt64({EXP_M1_ROWS}) AS expected
FROM mart.agg_token_model_cost_1d_dist
WHERE date = '{DATE}'
HAVING count() != {EXP_M1_ROWS}

UNION ALL

-- (A, Qwen3-32B) C = (serving 40h + standby 8h) × 4200 — test 2h 제외, 행 정확히 1개
SELECT 'm1_qwen_cost_a', toInt64(round(ifNull(sum(model_cost_krw), -1))), toInt64(round({EXP_M1_QWEN_COST}))
FROM mart.agg_token_model_cost_1d_dist
WHERE date = '{DATE}' AND service = 'Mock Service A' AND model = 'Qwen3-32B'
HAVING count() != 1 OR abs(ifNull(sum(model_cost_krw), -1) - {EXP_M1_QWEN_COST}) > 0.5

UNION ALL

-- quality_flag 4행 전부: normal / manual / no_tco / consumer_only (M1_FLAG_PRIORITY)
SELECT 'm1_flags',
       toInt64(countIf((service, model, quality_flag) IN (
           ('Mock Service A', 'Qwen3-32B', 'normal'),
           ('Mock Service B', 'Qwen3-32B', 'manual'),
           ('Mock Service B', 'claude-sonnet-5', 'no_tco'),
           ('Mock Service D', 'Qwen3-32B', 'consumer_only')))),
       toInt64(4)
FROM mart.agg_token_model_cost_1d_dist
WHERE date = '{DATE}'
HAVING countIf((service, model, quality_flag) IN (
           ('Mock Service A', 'Qwen3-32B', 'normal'),
           ('Mock Service B', 'Qwen3-32B', 'manual'),
           ('Mock Service B', 'claude-sonnet-5', 'no_tco'),
           ('Mock Service D', 'Qwen3-32B', 'consumer_only'))) != 4

UNION ALL

-- C는 토큰도 gpu 행도 없다 — M1 행 0 (metrics_missing은 M3에서만)
SELECT 'm1_c_absent', toInt64(countIf(service = 'Mock Service C')), toInt64(0)
FROM mart.agg_token_model_cost_1d_dist
WHERE date = '{DATE}'
HAVING countIf(service = 'Mock Service C') != 0

UNION ALL

-- === 2) M3 token_metrics_check_1d ===

SELECT 'm3_fail_rows', toInt64(countIf(severity = 'FAIL')), toInt64({EXP_M3_FAIL_ROWS})
FROM mart.token_metrics_check_1d_dist
WHERE date = '{DATE}'
HAVING countIf(severity = 'FAIL') != {EXP_M3_FAIL_ROWS}

UNION ALL

SELECT 'm3_warn_rows', toInt64(countIf(severity = 'WARN')), toInt64({EXP_M3_WARN_ROWS})
FROM mart.token_metrics_check_1d_dist
WHERE date = '{DATE}'
HAVING countIf(severity = 'WARN') != {EXP_M3_WARN_ROWS}

UNION ALL

SELECT 'm3_manual_source_b_info',
       toInt64(countIf(check_name = 'manual_source' AND service = 'Mock Service B' AND severity = 'INFO'
                       AND source_type = 'manual-v0')),
       toInt64(1)
FROM mart.token_metrics_check_1d_dist
WHERE date = '{DATE}'
HAVING countIf(check_name = 'manual_source' AND service = 'Mock Service B' AND severity = 'INFO'
               AND source_type = 'manual-v0') != 1

UNION ALL

SELECT 'm3_metrics_missing_c',
       toInt64(countIf(check_name = 'metrics_missing' AND service = 'Mock Service C' AND severity = 'FAIL')),
       toInt64(1)
FROM mart.token_metrics_check_1d_dist
WHERE date = '{DATE}'
HAVING countIf(check_name = 'metrics_missing' AND service = 'Mock Service C' AND severity = 'FAIL') != 1

UNION ALL

-- === 3) M4 agg_token_model_share_1d ===

SELECT 'm4_rows', toInt64(count()), toInt64({EXP_M4_ROWS})
FROM mart.agg_token_model_share_1d_dist
WHERE date = '{DATE}'
HAVING count() != {EXP_M4_ROWS}

UNION ALL

-- Qwen 3행(A/B/D) 전부 all_services, 제공자 A (A 행만 is_provider=1)
SELECT 'm4_qwen_mode_all_services',
       toInt64(countIf(model = 'Qwen3-32B' AND denominator_mode = 'all_services'
                       AND provider_service = 'Mock Service A'
                       AND is_provider = toUInt8(service = 'Mock Service A'))),
       toInt64(3)
FROM mart.agg_token_model_share_1d_dist
WHERE date = '{DATE}'
HAVING countIf(model = 'Qwen3-32B' AND denominator_mode = 'all_services'
               AND provider_service = 'Mock Service A'
               AND is_provider = toUInt8(service = 'Mock Service A')) != 3

UNION ALL

-- I3: Σ allocated(Qwen) == C(Qwen) (±0.5원)
SELECT 'm4_qwen_allocated_sum',
       toInt64(round(ifNull(sumIf(allocated_cost_krw, model = 'Qwen3-32B'), -1))),
       toInt64(round({EXP_M4_QWEN_SUM}))
FROM mart.agg_token_model_share_1d_dist
WHERE date = '{DATE}'
HAVING abs(ifNull(sumIf(allocated_cost_krw, model = 'Qwen3-32B'), -1) - {EXP_M4_QWEN_SUM}) > 0.5

UNION ALL

-- sonnet: 토큰 0·C NULL → 제공자 행 1개, share NULL, quality no_tco(M1 제공자 행 상속)
SELECT 'm4_sonnet_share_null',
       toInt64(countIf(model = 'claude-sonnet-5' AND service = 'Mock Service B' AND is_provider = 1
                       AND isNull(share) AND isNull(allocated_cost_krw) AND quality_flag = 'no_tco')),
       toInt64(1)
FROM mart.agg_token_model_share_1d_dist
WHERE date = '{DATE}'
HAVING countIf(model = 'claude-sonnet-5' AND service = 'Mock Service B' AND is_provider = 1
               AND isNull(share) AND isNull(allocated_cost_krw) AND quality_flag = 'no_tco') != 1
       OR countIf(model = 'claude-sonnet-5') != 1

UNION ALL

-- === 4) M2 agg_token_gpu_group_1d ===

SELECT 'm2_rows', toInt64(count()), toInt64({EXP_M2_ROWS})
FROM mart.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}'
HAVING count() != {EXP_M2_ROWS}

UNION ALL

-- H100: idle = 8×24 − 120 = 72h, over_report 0, FAIL 50h → quality flagged, 정체성 gap 0 (I2)
SELECT 'm2_h100_idle',
       toInt64(round(ifNull(sumIf(idle_gpu_hours, gpu_type = 'H100'), -1) * 1000)),
       toInt64(round({EXP_M2_IDLE_H100} * 1000))
FROM mart.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}' AND service_group = 'Mock Group'
HAVING abs(ifNull(sumIf(idle_gpu_hours, gpu_type = 'H100'), -1) - {EXP_M2_IDLE_H100}) > 0.0005
       OR countIf(gpu_type = 'H100' AND quality_flag = 'flagged' AND over_report = 0 AND tco_missing = 0
                  AND abs(ifNull(identity_gap_krw, 1)) < 0.5) != 1

UNION ALL

-- B200: 할당 없음 + TCO 없음 → no_tco(우선순위 no_tco > no_allocation), 비용 컬럼 NULL
SELECT 'm2_b200_no_tco',
       toInt64(countIf(gpu_type = 'B200' AND quality_flag = 'no_tco' AND tco_missing = 1
                       AND isNull(allocated_gpu_hours) AND isNull(group_total_cost_krw)
                       AND isNull(idle_gpu_hours))),
       toInt64(1)
FROM mart.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}' AND service_group = 'Mock Group'
HAVING countIf(gpu_type = 'B200' AND quality_flag = 'no_tco' AND tco_missing = 1
               AND isNull(allocated_gpu_hours) AND isNull(group_total_cost_krw)
               AND isNull(idle_gpu_hours)) != 1

UNION ALL

-- A100: gpu 행 0 + 할당 4 → alloc-only 행, idle 96h = 전액 유휴, gap 0, normal
SELECT 'm2_a100_alloc_only_normal',
       toInt64(countIf(gpu_type = 'A100' AND quality_flag = 'normal' AND reported_gpu_hours_total = 0
                       AND abs(ifNull(idle_gpu_hours, -1) - 96) < 0.0005
                       AND abs(ifNull(identity_gap_krw, 1)) < 0.5)),
       toInt64(1)
FROM mart.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}' AND service_group = 'Mock Group'
HAVING countIf(gpu_type = 'A100' AND quality_flag = 'normal' AND reported_gpu_hours_total = 0
               AND abs(ifNull(idle_gpu_hours, -1) - 96) < 0.0005
               AND abs(ifNull(identity_gap_krw, 1)) < 0.5) != 1

UNION ALL

-- === 5) created_by 전행 'token-metrics-pipeline' (mart 4테이블) ===

SELECT 'created_by_all_tables', toInt64(sum(bad)), toInt64(0)
FROM
(
    SELECT countIf(created_by != 'token-metrics-pipeline') AS bad
    FROM mart.agg_token_model_cost_1d_dist WHERE date = '{DATE}'
    UNION ALL
    SELECT countIf(created_by != 'token-metrics-pipeline')
    FROM mart.token_metrics_check_1d_dist WHERE date = '{DATE}'
    UNION ALL
    SELECT countIf(created_by != 'token-metrics-pipeline')
    FROM mart.agg_token_model_share_1d_dist WHERE date = '{DATE}'
    UNION ALL
    SELECT countIf(created_by != 'token-metrics-pipeline')
    FROM mart.agg_token_gpu_group_1d_dist WHERE date = '{DATE}'
)
HAVING sum(bad) != 0

UNION ALL

-- === 6) 2회 실행 후 키 중복 0 (DELETE → INSERT 원자 교체, insert_deduplicate=0) ===

SELECT 'idempotent_no_dup_m1', toInt64(count()), toInt64(uniqExact((service, model)))
FROM mart.agg_token_model_cost_1d_dist
WHERE date = '{DATE}'
HAVING count() != uniqExact((service, model))

UNION ALL

SELECT 'idempotent_no_dup_m2', toInt64(count()), toInt64(uniqExact((service_group, gpu_type)))
FROM mart.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}'
HAVING count() != uniqExact((service_group, gpu_type))

UNION ALL

SELECT 'idempotent_no_dup_m4', toInt64(count()), toInt64(uniqExact((model, service, provider_service)))
FROM mart.agg_token_model_share_1d_dist
WHERE date = '{DATE}'
HAVING count() != uniqExact((model, service, provider_service))
SQL
```

검증(CH 불필요 — 검사 20종·토큰 8종·세미콜론 0):

```bash
python3 - <<'PY'
import re, pathlib
sql = pathlib.Path("mart/token-metrics/tests/e2e/verify_expected_results.sql").read_text(encoding="utf-8")
names = re.findall(r"^SELECT '(\w+)'", sql, flags=re.M)
print(len(names), names)
print(sorted(set(re.findall(r"\{(EXP_\w+)\}", sql))))
print(";" in sql, sql.count("UNION ALL"), len(re.findall(r"^HAVING", sql, flags=re.M)))
PY
```

기대 출력:

```
20 ['m1_rows', 'm1_qwen_cost_a', 'm1_flags', 'm1_c_absent', 'm3_fail_rows', 'm3_warn_rows', 'm3_manual_source_b_info', 'm3_metrics_missing_c', 'm4_rows', 'm4_qwen_mode_all_services', 'm4_qwen_allocated_sum', 'm4_sonnet_share_null', 'm2_rows', 'm2_h100_idle', 'm2_b200_no_tco', 'm2_a100_alloc_only_normal', 'created_by_all_tables', 'idempotent_no_dup_m1', 'idempotent_no_dup_m2', 'idempotent_no_dup_m4']
['EXP_M1_QWEN_COST', 'EXP_M1_ROWS', 'EXP_M2_IDLE_H100', 'EXP_M2_ROWS', 'EXP_M3_FAIL_ROWS', 'EXP_M3_WARN_ROWS', 'EXP_M4_QWEN_SUM', 'EXP_M4_ROWS']
False 22 20
```

- [ ] **Step 7: `tests/e2e/run_e2e.sh` — CH 24.8 단일노드 컨테이너 → DDL 변환 적재 → 시드 → 배치 2회 → verify expect-empty → invariants_metrics → no-metrics day**

원형 §13을 그대로 따르되 (1) 컨테이너·네트워크·포트를 `ch-e2e-mart-metrics`/`tokene2e-mart-metrics`/`18124`로 바꿔 token-usage E2E와 동시 실행 가능하게 하고, (2) DDL 결합 목록을 6c 읽기 계약 + Plan 6a + 6c로 바꾸고, (3) 배치 호출을 `--date`로, (4) verify 뒤에 T9 `run_invariants.py --sql invariants_metrics.sql`과 no-metrics day 실행을 추가한다. python 블록의 포트 `18124`는 원형과 같은 이유(heredoc이 export 안 된 셸 변수를 못 봄)로 리터럴이다.

```bash
cat > /home/mini/github/token-data-pipeline/mart/token-metrics/tests/e2e/run_e2e.sh <<'BASHEOF'
#!/usr/bin/env bash
# CI/로컬(도커 필요) E2E — Plan 6c T10:
#   CH 24.8 컨테이너(CLICKHOUSE_SKIP_USER_SETUP=1) → DDL(단일노드 변환: token-usage dim_token_service/mart_tables
#   + Plan 6a raw_token_metrics/dim_token_metrics_service + 6c mart_metrics_tables + tests/e2e/ddl_test_dims.sql)
#   → seed_metrics.py(시나리오 A/B/C/D) → app.batch --date 2회(멱등) → 마커·CHECK 라인 grep
#   → verify_expected_results.sql(expect-empty 20검사) → tools/verify/run_invariants.py --sql invariants_metrics.sql
#   → no-metrics day(D-1) 배치 = SUCCESS coverage 0/3.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."     # mart/token-metrics

# 결정적 기대값 — 시드는 날짜에 의존하지 않지만(build_seed(date)) dim 유효기간(effective_from 2026-01-01,
# registry coverage_since 2026-08-26) 이후여야 하므로 고정 기본값을 쓴다(원형 mart E2E와 동일 원칙).
DATE_ARG="${1:-2026-09-03}"
NO_METRICS_DATE=$(date -d "${DATE_ARG} -1 day" +%F)
CH_PORT_HOST=18124

docker network create tokene2e-mart-metrics >/dev/null 2>&1 || true
trap 'docker rm -f ch-e2e-mart-metrics >/dev/null 2>&1 || true; docker network rm tokene2e-mart-metrics >/dev/null 2>&1 || true' EXIT

# CLICKHOUSE_SKIP_USER_SETUP=1: 비밀번호 미설정 시 default 유저의 네트워크 접근 차단(403) 회피 — 원형과 동일.
docker run -d --rm --name ch-e2e-mart-metrics --network tokene2e-mart-metrics -p "${CH_PORT_HOST}:8123" \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  clickhouse/clickhouse-server:24.8

for _ in $(seq 1 60); do
  curl -sf "http://127.0.0.1:${CH_PORT_HOST}/ping" >/dev/null && break
  sleep 1
done
curl -sf "http://127.0.0.1:${CH_PORT_HOST}/ping" >/dev/null || { echo "E2E FAILED: ClickHouse not reachable on ${CH_PORT_HOST}"; exit 1; }

# DDL 결합(DB 3개 프리펜드) — 읽기 계약 2파일(token-usage, 읽기만) + Plan 6a 2파일 + 6c mart + e2e dim 대역.
# 단일노드 변환 regex 3종은 원형 mart/token-usage/tests/e2e/run_e2e.sh와 동일. '--' 주석 줄은 분리 전에 제거해
# 주석 속 세미콜론이 문장 분리를 깨지 못하게 하고(ddl_test_dims.sql은 애초에 세미콜론 없는 주석만 쓴다),
# 문장 분리는 Plan 6b run_e2e.sh와 같은 split_statements(단일따옴표 문자열 안의 ';' 무시)로 한다 —
# 6a 테스트가 COMMENT 문자열의 ';'를 금지하지만 로더는 그 계약에 기대지 않는다.
python3 - <<'PY'
import re, pathlib, urllib.request, urllib.error


def split_statements(text):
    """';' 문장 분할 — 단일따옴표 문자열 안의 ';' 는 무시 (6a 컬럼 COMMENT 에 ';' 가 있다; '' 이스케이프는 토글 2회로 처리)."""
    out, buf, in_str = [], [], False
    for ch in text:
        if ch == "'":
            in_str = not in_str
        if ch == ";" and not in_str:
            out.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    out.append("".join(buf))
    return [s for s in out if s.strip()]


sql = "CREATE DATABASE IF NOT EXISTS fact;\n"
sql += "CREATE DATABASE IF NOT EXISTS gpu_data;\n"
sql += "CREATE DATABASE IF NOT EXISTS mart;\n"
for path in (
    "../../collectors/token-usage/ddl/company/dim_token_service.sql",
    "../../mart/token-usage/ddl/company/mart_tables.sql",
    "../../collectors/token-metrics/ddl/company/raw_token_metrics.sql",
    "../../collectors/token-metrics/ddl/company/dim_token_metrics_service.sql",
    "ddl/company/mart_metrics_tables.sql",
    "tests/e2e/ddl_test_dims.sql",
):
    sql += "\n" + pathlib.Path(path).read_text(encoding="utf-8")

sql = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
sql = re.sub(r"ON CLUSTER 'gpu-monitoring'", "", sql)
sql = re.sub(r"ENGINE = ReplicatedMergeTree\([^)]*\)", "ENGINE = MergeTree", sql, flags=re.S)
sql = re.sub(r"ENGINE = Distributed\('gpu-monitoring', '(\w+)', '(\w+)',[\s\S]*?\);",
             r"ENGINE = Distributed('default', '\1', '\2', rand());", sql)

applied = 0
for stmt in split_statements(sql):
    if stmt.strip():
        # 포트는 CH_PORT_HOST(bash 변수)와 동일 값을 리터럴로 고정 — heredoc은 export 안 된 셸 변수를 못 본다.
        req = urllib.request.Request("http://127.0.0.1:18124/", data=(stmt + ";").encode("utf-8"))
        try:
            urllib.request.urlopen(req).read()
        except urllib.error.HTTPError as e:
            print(f"DDL failed (HTTP {e.code}): {e.read().decode(errors='replace')}")
            print(f"statement: {stmt.strip()[:200]}")
            raise SystemExit(1)
        applied += 1
print(f"DDL applied (single-node transformed, statements={applied})")
PY

export CH_HOST=127.0.0.1 CH_PORT="${CH_PORT_HOST}" CH_CLUSTER=""
export CH_DB_FACT=fact CH_DB_DIM=gpu_data CH_DB_MART=mart

# 시드 — registry(A/B/C/D + A/B/C), fact gpu 6/serving 2/summary 2, token_usage 6/agg_service 3.
python3 tests/e2e/seed_metrics.py "${DATE_ARG}"

# 기대값(CH 불필요) — 마커 grep과 verify 치환에 사용.
declare -A EXP
while IFS='=' read -r k v; do EXP["$k"]="$v"; done < <(python3 tests/e2e/mart_expectations.py "${DATE_ARG}")
echo "expectations: m1_rows=${EXP[EXP_M1_ROWS]} m4_rows=${EXP[EXP_M4_ROWS]} coverage=${EXP[EXP_COVERAGE]}"

# 배치 2회 — 멱등성(DELETE → INSERT, insert_deduplicate=0) 검증. 마커·CHECK 라인은 2회차 출력으로 단정.
RUN1=$(python3 -m app.batch --date "${DATE_ARG}" 2>&1) || true
echo "$RUN1"
grep -qF "BATCH_RESULT status=SUCCESS module=mart-metrics" <<<"$RUN1" || { echo "E2E FAILED: run1 status != SUCCESS"; exit 1; }

RUN2=$(python3 -m app.batch --date "${DATE_ARG}" 2>&1) || true
echo "$RUN2"
grep -qF "BATCH_RESULT status=SUCCESS module=mart-metrics" <<<"$RUN2" || { echo "E2E FAILED: run2(재실행) status != SUCCESS"; exit 1; }
grep -qF "metrics_coverage=${EXP[EXP_COVERAGE]} " <<<"$RUN2" || { echo "E2E FAILED: coverage marker != ${EXP[EXP_COVERAGE]}"; exit 1; }
grep -qF 'missing_services="Mock Service C"' <<<"$RUN2" || { echo "E2E FAILED: missing_services marker != \"Mock Service C\""; exit 1; }
grep -qF "rows_mart=${EXP[EXP_M1_ROWS]} " <<<"$RUN2" || { echo "E2E FAILED: rows_mart marker != ${EXP[EXP_M1_ROWS]}"; exit 1; }
grep -qF "rows_share=${EXP[EXP_M4_ROWS]} " <<<"$RUN2" || { echo "E2E FAILED: rows_share marker != ${EXP[EXP_M4_ROWS]}"; exit 1; }
grep -qF "CHECK WARN metrics_missing severity=FAIL count=1" <<<"$RUN2" || { echo "E2E FAILED: metrics_missing CHECK line missing"; exit 1; }
grep -qF "CHECK WARN hours_over_count severity=FAIL count=1" <<<"$RUN2" || { echo "E2E FAILED: hours_over_count CHECK line missing"; exit 1; }
grep -qF "CHECK INFO manual_source severity=INFO count=1" <<<"$RUN2" || { echo "E2E FAILED: manual_source CHECK INFO line missing"; exit 1; }

# verify — {DATE} + {EXP_*} 8토큰 치환 후 expect-empty. -f 대신 HTTP 코드 캡처(서버 오류 본문 노출 — 원형과 동일 원칙).
sed -e "s/{DATE}/${DATE_ARG}/g" \
    -e "s/{EXP_M1_ROWS}/${EXP[EXP_M1_ROWS]}/g" \
    -e "s/{EXP_M1_QWEN_COST}/${EXP[EXP_M1_QWEN_COST]}/g" \
    -e "s/{EXP_M3_FAIL_ROWS}/${EXP[EXP_M3_FAIL_ROWS]}/g" \
    -e "s/{EXP_M3_WARN_ROWS}/${EXP[EXP_M3_WARN_ROWS]}/g" \
    -e "s/{EXP_M4_ROWS}/${EXP[EXP_M4_ROWS]}/g" \
    -e "s/{EXP_M4_QWEN_SUM}/${EXP[EXP_M4_QWEN_SUM]}/g" \
    -e "s/{EXP_M2_ROWS}/${EXP[EXP_M2_ROWS]}/g" \
    -e "s/{EXP_M2_IDLE_H100}/${EXP[EXP_M2_IDLE_H100]}/g" \
    tests/e2e/verify_expected_results.sql > /tmp/verify_query_mart_metrics.sql
if grep -q '{EXP_' /tmp/verify_query_mart_metrics.sql; then
  echo "E2E FAILED: unreplaced token in verify query:"; grep -n '{EXP_' /tmp/verify_query_mart_metrics.sql; exit 1
fi
VERIFY_HTTP=$(curl -s -o /tmp/verify_out_mart_metrics.tsv -w '%{http_code}' \
  --data-binary @/tmp/verify_query_mart_metrics.sql "http://127.0.0.1:${CH_PORT_HOST}/?default_format=TSV")
if [ "${VERIFY_HTTP}" != "200" ]; then
  echo "E2E VERIFY QUERY FAILED (HTTP ${VERIFY_HTTP}):"; cat /tmp/verify_out_mart_metrics.tsv; exit 1
fi
if [ -s /tmp/verify_out_mart_metrics.tsv ]; then
  echo "E2E VERIFY FAILED:"; cat /tmp/verify_out_mart_metrics.tsv; exit 1
fi
echo "verify_expected_results.sql: 20 checks empty (PASS)"

# 불변식(T9) — 같은 컨테이너의 fact/gpu_data/mart 기본 DB명, 읽기 전용 SELECT.
INV=$(python3 ../../tools/verify/run_invariants.py --sql ../../tools/verify/invariants_metrics.sql --date "${DATE_ARG}" 2>&1) || true
echo "$INV"
grep -qF "ALL INVARIANTS PASS (date=${DATE_ARG}, DBs=fact/gpu_data/mart, sql=invariants_metrics.sql)" <<<"$INV" || { echo "E2E FAILED: invariants_metrics"; exit 1; }

# no-metrics day(D-1): 앵커 0 + 토큰 0 → FAILURE 아님(설계 §6.1) — coverage 0/3, 3서비스 전부 missing.
RUN_EMPTY=$(python3 -m app.batch --date "${NO_METRICS_DATE}" 2>&1) || true
echo "$RUN_EMPTY"
grep -qF "BATCH_RESULT status=SUCCESS module=mart-metrics metrics_coverage=0/3" <<<"$RUN_EMPTY" || {
  echo "E2E FAILED: no-metrics day(${NO_METRICS_DATE}) status/coverage != SUCCESS 0/3"; exit 1; }
grep -qF 'missing_services="Mock Service A,Mock Service B,Mock Service C"' <<<"$RUN_EMPTY" || {
  echo "E2E FAILED: no-metrics day missing_services != A,B,C"; exit 1; }
grep -qF "CHECK WARN token_mart_absent date=${NO_METRICS_DATE}" <<<"$RUN_EMPTY" || {
  echo "E2E FAILED: no-metrics day token_mart_absent WARN missing"; exit 1; }

echo "E2E PASS (date=${DATE_ARG}, m1_rows=${EXP[EXP_M1_ROWS]}, coverage=${EXP[EXP_COVERAGE]})"
BASHEOF
chmod +x /home/mini/github/token-data-pipeline/mart/token-metrics/tests/e2e/run_e2e.sh
```

정적 검증(도커 없이 — 문법·실행 비트·고정값):

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && bash -n tests/e2e/run_e2e.sh && echo SYNTAX_OK
test -x tests/e2e/run_e2e.sh && echo EXEC_OK
grep -c 'python3 -m app.batch --date' tests/e2e/run_e2e.sh
grep -o 'CH_PORT_HOST=18124\|http://127.0.0.1:18124/\|ch-e2e-mart-metrics\|tokene2e-mart-metrics\|invariants_metrics.sql' tests/e2e/run_e2e.sh | LC_ALL=C sort | uniq -c
```

기대 출력:

```
SYNTAX_OK
EXEC_OK
3
      1 CH_PORT_HOST=18124
      2 ch-e2e-mart-metrics
      1 http://127.0.0.1:18124/
      3 invariants_metrics.sql
      3 tokene2e-mart-metrics
```

- [ ] **Step 8: T9 `invariants_metrics.sql`의 `metrics_serving_dup_key` 키가 자연키(metric, name 포함)인지 선확인 — 시드의 serving 2행(ttft_ms·output_tps, 같은 (service, model))이 오탐되지 않아야 한다**

serving fact의 ORDER BY는 `(date, service, model, metric, name)`(Plan 6a)이고 이 시드는 같은 (A, Qwen3-32B)에 metric이 다른 2행을 넣는다. T9 아웃라인은 이 블록의 GROUP BY를 `service, model`로 적었으나 그러면 정상 데이터가 위반으로 잡힌다(아웃라인 T10의 "serving 2행" 요구와 충돌 — 자연키가 정본).

```bash
# 앵커는 SELECT 별칭 줄(파일 머리 주석의 이름 나열은 건너뛴다 — 주석에서 시작하면 첫 GROUP BY가 gpu 블록의 것이 된다)
awk '/metrics_serving_dup_key. AS check_name/{f=1} f&&/GROUP BY/{print NR": "$0; exit}' tools/verify/invariants_metrics.sql
```

기대 출력(줄번호는 T9 파일 기준, `GROUP BY` 앞 공백 4칸은 SQL 원문의 서브쿼리 들여쓰기 — 핵심은 `metric`과 `name`이 키에 포함):

```
<N>:     GROUP BY service, model, metric, name
```

출력에 `metric`이 없으면(예: `GROUP BY service, model`) 해당 GROUP BY 절 한 줄을 `GROUP BY service, model, metric, name`으로 고치고(`HAVING count() > 1`은 그대로), 같은 파일의 블록 주석에 "자연키 = ORDER BY (date, service, model, metric, name)"를 한 줄 추가한 뒤, T9 단위 테스트가 여전히 통과하는지 확인한다:

```bash
cd /home/mini/github/token-data-pipeline/tools/verify && python -m pytest -q
```

기대 출력: `… passed`(T9가 추가한 `--sql`/렌더 테스트 포함, 실패 0). 수정이 발생했으면 `tools/verify/invariants_metrics.sql`을 스텝 11 커밋에 포함한다(조건부 Modify — 커밋 메시지 본문에 "invariants_metrics: serving dup key = natural key" 한 줄 추가).

- [ ] **Step 9: 로컬 E2E 실행(도커 필요) — 전 구간 PASS**

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && pip install -q -r requirements-dev.txt
bash tests/e2e/run_e2e.sh > /tmp/e2e_mart_metrics.log 2>&1; echo "exit=$?"
grep -E '^(DDL applied|seeded|expectations:|BATCH_RESULT|CHECK (WARN|INFO)|verify_expected_results|ALL INVARIANTS PASS|E2E )' /tmp/e2e_mart_metrics.log
```

기대 출력(순서 고정 — `CHECK` 줄은 M0 → M0b → M3 `SQL_M3_SUMMARY`의 `ORDER BY check_name, severity` 순; `elapsed=`만 환경값):

```
exit=0
DDL applied (single-node transformed, statements=<n>)
seeded gpu_data.dim_token_service_dist rows=4
seeded gpu_data.dim_token_metrics_service_dist rows=3
seeded fact.raw_token_metrics_gpu_1d_dist rows=6
seeded fact.raw_token_metrics_serving_1d_dist rows=2
seeded fact.raw_token_metrics_summary_1d_dist rows=2
seeded mart.token_usage_1d_dist rows=6
seeded mart.agg_token_service_1d_dist rows=3
expectations: m1_rows=4 m4_rows=4 coverage=2/3
CHECK WARN metrics_coverage missing=1
CHECK WARN gpu_type_no_tco severity=WARN count=1
CHECK WARN hours_over_count severity=FAIL count=1
CHECK INFO manual_source severity=INFO count=1
CHECK WARN metrics_missing severity=FAIL count=1
CHECK WARN no_allocation severity=WARN count=1
CHECK WARN rows_rejected severity=WARN count=1
BATCH_RESULT status=SUCCESS module=mart-metrics metrics_coverage=2/3 missing_services="Mock Service C" rows_mart=4 rows_check=6 rows_share=4 warn=6 elapsed=<s>
CHECK WARN metrics_coverage missing=1
CHECK WARN gpu_type_no_tco severity=WARN count=1
CHECK WARN hours_over_count severity=FAIL count=1
CHECK INFO manual_source severity=INFO count=1
CHECK WARN metrics_missing severity=FAIL count=1
CHECK WARN no_allocation severity=WARN count=1
CHECK WARN rows_rejected severity=WARN count=1
BATCH_RESULT status=SUCCESS module=mart-metrics metrics_coverage=2/3 missing_services="Mock Service C" rows_mart=4 rows_check=6 rows_share=4 warn=6 elapsed=<s>
verify_expected_results.sql: 20 checks empty (PASS)
ALL INVARIANTS PASS (date=2026-09-03, DBs=fact/gpu_data/mart, sql=invariants_metrics.sql)
CHECK WARN metrics_coverage missing=3
CHECK WARN token_mart_absent date=2026-09-02
CHECK WARN metrics_missing severity=FAIL count=3
BATCH_RESULT status=SUCCESS module=mart-metrics metrics_coverage=0/3 missing_services="Mock Service A,Mock Service B,Mock Service C" rows_mart=0 rows_check=3 rows_share=0 warn=3 elapsed=<s>
E2E PASS (date=2026-09-03, m1_rows=4, coverage=2/3)
```

`warn=6` = `CHECK WARN` 접두 줄 수(coverage 1 + FAIL/WARN 블록 5, `CHECK INFO` 제외 — T5 계약). 실패 시 로그의 `E2E FAILED:`/`E2E VERIFY FAILED:` 다음 줄(검사명·actual·expected TSV)을 보고 시드(스텝 3)·기대값(스텝 4)·SQL(T3–T7) 중 어긋난 쪽을 고친다 — 기대값 파일은 SQL의 재현이므로 **SQL이 설계와 맞다면 기대값을 고치고, SQL이 틀렸다면 해당 T의 단위 테스트부터 추가**한다.


- [ ] **Step 10: `.github/workflows/test-mart-metrics.yml` — 원형 `test-mart.yml` 클론(4 job) + 델타**

델타: (1) `paths`를 6c 트리거 5종으로(`mart/token-metrics/**`, `collectors/token-metrics/ddl/**`, `assets/model-catalog/ddl/**`, `tools/verify/invariants_metrics.sql`, 자기 자신), (2) `image` 스모크는 `docker run --rm token-mart-metrics:ci --help`(T8 `ENTRYPOINT ["python","-m","app.batch"]` — 원형처럼 `python -m app.batch --help`를 붙이면 argparse가 `python`을 알 수 없는 인자로 거부), (3) `manifests`는 overlay 3개(stage/company/company-verify) + T8 계약 grep(`schedule: 20 10`, `startingDeadlineSeconds: 1800` 추가, Secret 이름 `token-mart-metrics-ch-secret`) + verify overlay 이름 2종, (4) `unit`은 `python -m pytest -q`(T1 관례 — `tests/e2e/`에는 `test_*.py`가 없어 수집되지 않고, `tests/test_e2e_seed.py`는 스텝 2의 정적 테스트라 도커 불필요), (5) `e2e`는 `bash mart/token-metrics/tests/e2e/run_e2e.sh`. 기존 `test-mart.yml`·`test-collector.yml`·`release-images.yml`은 무수정(zero-diff).

`.github/workflows/test-mart-metrics.yml` (신규):

```yaml
name: test-mart-metrics

# Plan 6c mart/token-metrics 전용 워크플로 — 기존 test-mart.yml(mart/token-usage)과 분리 (설계 §7.5).
# 트리거 경로: 모듈 자체 + 읽는 DDL(6a fact/dim, model-catalog dim) + T9 불변식 SQL + 이 파일.
on:
  push:
    branches: [main]
    paths: ["mart/token-metrics/**", "collectors/token-metrics/ddl/**", "assets/model-catalog/ddl/**",
            "tools/verify/invariants_metrics.sql", ".github/workflows/test-mart-metrics.yml"]
  pull_request:
    paths: ["mart/token-metrics/**", "collectors/token-metrics/ddl/**", "assets/model-catalog/ddl/**",
            "tools/verify/invariants_metrics.sql", ".github/workflows/test-mart-metrics.yml"]

jobs:
  image:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - name: Build image
        run: |
          docker buildx build --platform linux/amd64 \
            -t token-mart-metrics:ci mart/token-metrics --load
      # T8 Dockerfile은 ENTRYPOINT ["python", "-m", "app.batch"] — 인자만 붙인다 (python -m app.batch 재지정 금지)
      - name: Test image
        run: docker run --rm token-mart-metrics:ci --help

  manifests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Kustomize stage
        run: kubectl kustomize mart/token-metrics/k8s/overlays/stage > /tmp/mm-stage.yaml
      - name: Kustomize company
        run: kubectl kustomize mart/token-metrics/k8s/overlays/company > /tmp/mm-company.yaml
      - name: Kustomize company-verify
        run: kubectl kustomize mart/token-metrics/k8s/overlays/company-verify > /tmp/mm-company-verify.yaml
      - name: Contract fields locked
        run: |
          for f in /tmp/mm-stage.yaml /tmp/mm-company.yaml /tmp/mm-company-verify.yaml; do
            grep -q 'schedule: 20 10 \* \* \*' "$f"
            grep -q 'timeZone: Asia/Seoul' "$f"
            grep -q 'concurrencyPolicy: Forbid' "$f"
            grep -q 'startingDeadlineSeconds: 1800' "$f"
            grep -q 'activeDeadlineSeconds: 1800' "$f"
            grep -q 'backoffLimit: 1' "$f"
            grep -q 'memory: 1Gi' "$f"
            grep -q 'memory: 256Mi' "$f"
            grep -q 'name: registry-pull-secret' "$f"
            grep -q 'name: token-mart-metrics-ch-secret' "$f"
            ! grep -q 'EXPECTED_LATE_SERVICES' "$f"
          done
          grep -q 'ghcr.io/yoonsungnam/token-mart-metrics' /tmp/mm-stage.yaml
          # company-verify: nameSuffix -verify (CronJob) + Secret 이름 패치 (설계 §7.5 격리 검증)
          grep -q 'name: token-mart-metrics-verify$' /tmp/mm-company-verify.yaml
          grep -q 'name: token-mart-metrics-ch-secret-verify$' /tmp/mm-company-verify.yaml
          # production 이름은 verify 렌더에 남지 않는다 (공존 보장). CronJob은 metadata.name(2칸 들여쓰기)만 검사 —
          # 컨테이너 이름 `- name: token-mart-metrics`(nameSuffix 미적용, 12칸 들여쓰기)은 verify 렌더에도 남는 것이 정상.
          ! grep -qE '^  name: token-mart-metrics$' /tmp/mm-company-verify.yaml
          # Secret은 envFrom secretRef.name(overlay 패치 대상)이 production 이름이면 안 된다 — 들여쓰기 무관
          ! grep -q 'name: token-mart-metrics-ch-secret$' /tmp/mm-company-verify.yaml

  unit:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: mart/token-metrics
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -r requirements-dev.txt
      # tests/e2e/ 에는 test_*.py 가 없다(시드·기대값 스크립트만) — 별도 --ignore 불필요
      - run: python -m pytest -q

  e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      # clickhouse-connect(seed_metrics.py·run_invariants.py)는 requirements.txt 경유
      - run: pip install -r mart/token-metrics/requirements-dev.txt
      - name: Run E2E
        run: bash mart/token-metrics/tests/e2e/run_e2e.sh
```

검증(로컬 — 워크플로 자체는 CI에서만 실행되므로 정적 검사):

```bash
cd /home/mini/github/token-data-pipeline
python3 - <<'PY'
import yaml
d = yaml.safe_load(open(".github/workflows/test-mart-metrics.yml", encoding="utf-8"))
on = d[True]                      # PyYAML은 키 `on`을 불리언 True로 읽는다
print(d["name"], sorted(d["jobs"]))
print(on["push"]["branches"], on["push"]["paths"] == on["pull_request"]["paths"], len(on["push"]["paths"]))
img = d["jobs"]["image"]["steps"]
print([s["run"].strip() for s in img if s.get("name") == "Test image"])
print(d["jobs"]["unit"]["defaults"]["run"]["working-directory"], d["jobs"]["unit"]["steps"][-1]["run"])
print([s["run"].strip() for s in d["jobs"]["e2e"]["steps"] if s.get("name") == "Run E2E"])
lock = [s for s in d["jobs"]["manifests"]["steps"] if s.get("name") == "Contract fields locked"][0]["run"]
print(lock.count("grep -q"), "startingDeadlineSeconds: 1800" in lock, "token-mart-metrics-ch-secret-verify$" in lock)
PY
# 기존 3워크플로 무수정 확인 (zero-diff)
git diff --stat main -- .github/workflows/release-images.yml .github/workflows/test-mart.yml .github/workflows/test-collector.yml
```

기대:

```
test-mart-metrics ['e2e', 'image', 'manifests', 'unit']
['main'] True 5
['docker run --rm token-mart-metrics:ci --help']
mart/token-metrics python -m pytest -q
['bash mart/token-metrics/tests/e2e/run_e2e.sh']
16 True True
```

마지막 `git diff --stat` 출력은 **빈 줄**(기존 워크플로 3파일 변경 0). `grep -q` 16 = 루프 안 11(부정 1 포함) + 루프 밖 5(부정 2 포함). `kubectl`이 로컬에 있으면 `manifests` 잡의 `run` 블록을 그대로 셸에 붙여 넣어(경로 `/tmp/mm-*.yaml`) 사전 실행할 수 있다 — T8 Step 5 검증과 동일 결과(`exit 0`).

- [ ] **Step 11: 커밋 전 게이트(zero-diff·공개 레포·플레이스홀더) → 커밋**

```bash
cd /home/mini/github/token-data-pipeline
# (1) zero-diff 게이트 — 출력이 비어 있어야 한다
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql \
  docs/operations docs/monitoring/grafana_dashboard_token_usage.json \
  .github/workflows/release-images.yml .github/workflows/test-mart.yml .github/workflows/test-collector.yml
# (2) 공개 레포 규칙 — 신규 7파일에 사내 호스트/이메일/실데이터 흔적 0 (허용 도메인은 *.invalid·example.internal·ghcr.io 뿐)
grep -nE 'https?://[A-Za-z0-9.-]+' mart/token-metrics/tests/e2e/*.py mart/token-metrics/tests/e2e/*.sql \
  mart/token-metrics/tests/e2e/run_e2e.sh mart/token-metrics/tests/test_e2e_seed.py .github/workflows/test-mart-metrics.yml \
  | grep -vE '127\.0\.0\.1|\.invalid|example\.internal|ghcr\.io' || echo "HOSTS_OK"
grep -nE '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}' mart/token-metrics/tests/e2e mart/token-metrics/tests/test_e2e_seed.py \
  .github/workflows/test-mart-metrics.yml || echo "EMAILS_OK"
# (3) 시드 user_id는 전부 합성(u- + sha256 12자) — 실명/사번 형태 0
python3 - <<'PY'
import sys, re
sys.path.insert(0, "mart/token-metrics/tests/e2e")
import seed_metrics as sm
ids = {r[3] for r in sm.build_seed("2026-09-03")["token_usage"]}
print(len(ids), all(re.fullmatch(r"u-[0-9a-f]{12}", i) for i in ids))
PY
# (4) 신규 파일 목록 확인
git status --short -- mart/token-metrics/tests .github/workflows/test-mart-metrics.yml tools/verify/invariants_metrics.sql
```

기대: (1) 빈 출력, (2) `HOSTS_OK` / `EMAILS_OK`, (3) `6 True`(서비스 3 × 사용자 2), (4) `?? mart/token-metrics/tests/e2e/`(디렉터리 5파일) + `?? mart/token-metrics/tests/test_e2e_seed.py` + `?? .github/workflows/test-mart-metrics.yml`(스텝 8에서 T9 파일을 고쳤을 때만 ` M tools/verify/invariants_metrics.sql`이 추가로 보인다).

커밋(스텝 8 수정이 **없었으면** 첫 명령의 `tools/verify/invariants_metrics.sql`과 메시지 본문 마지막 줄을 뺀다):

```bash
cd /home/mini/github/token-data-pipeline
git add mart/token-metrics/tests/e2e/ddl_test_dims.sql mart/token-metrics/tests/e2e/seed_metrics.py \
        mart/token-metrics/tests/e2e/mart_expectations.py mart/token-metrics/tests/e2e/verify_expected_results.sql \
        mart/token-metrics/tests/e2e/run_e2e.sh mart/token-metrics/tests/test_e2e_seed.py \
        .github/workflows/test-mart-metrics.yml tools/verify/invariants_metrics.sql
git commit -m "test(mart-metrics): E2E 단일노드 — seed_metrics/ddl_test_dims/mart_expectations/verify expect-empty 2회 멱등 + invariants_metrics + test-mart-metrics.yml (Plan 6c T10)" \
  -m "시드(sha256 합성 user_id·aware KST) → app.batch --date 2회(insert_deduplicate=0 멱등) → verify_expected_results.sql 20검사 expect-empty → run_invariants.py --sql invariants_metrics.sql → no-metrics day(SUCCESS, coverage 0/3). 기대값은 seed_metrics.build_seed를 app.mart 참조 구현으로 재계산(C(Qwen3-32B)=201,600·H100 idle 72·gap 0·I3 합 보존). 워크플로 test-mart-metrics.yml(image/manifests/unit/e2e), 기존 3워크플로 무수정.
invariants_metrics: serving dup key = natural key" \
  -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>" \
  -m "Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
git log -1 --stat --format='%s%n%b' | head -25
```

기대: 첫 줄 `test(mart-metrics): E2E 단일노드 — … (Plan 6c T10)`, 본문 뒤에 트레일러 2줄, `--stat`에 신규 7파일(+ 조건부 1파일) — `mart/token-usage/`·`collectors/token-usage/`·`tools/verify/invariants.sql`·기존 워크플로 파일명이 stat에 **없다**.

**설계 해석(리뷰어 확인 항목)**:
1. **TCO H100 2026-08-26 4300 이력 행 제외** — 아웃라인 "TCO 5행"을 fixture digest §20의 4행(unknown NULL/H100 4200/A100 2100/H200 5300) + L40S 1300으로 채웠다. 이력 행을 넣으면 2026-09-03 유효 TCO가 4300이 되어 정의서 검산값 C(Qwen3-32B)=201,600(= 48h × 4200)이 깨지므로, "이력 행의 effective_from 선택"은 T3 단위 테스트(`SUB_TCO` — 최신 effective_from ≤ date)에 맡기고 E2E는 단일 유효값으로 고정했다.
2. **읽기 계약 대역 = 실제 token-usage DDL 파일(읽기만)** — `token_usage_1d`/`agg_token_service_1d`/`dim_token_service`는 `mart/token-usage/ddl/company/mart_tables.sql`·`collectors/token-usage/ddl/company/dim_token_service.sql`을 run_e2e.sh가 그대로 읽어 단일노드 변환한다(복사본 금지 — 복사하면 컬럼 계약이 두 곳에 갈라진다). 따라서 토큰 시드는 `token_usage_1d`의 `CHECK created_by != ''`를 통과하도록 `created_by='token-pipeline'`(token-usage 배치 값)을 넣는다 — 6c의 `created_by_wrong_metrics`는 mart 4테이블만 검사하므로 충돌 없음.
3. **vendor_price 4행(아웃라인 3행)** — fixture digest §20의 `unknown` 1행 + anthropic 3행(opus/sonnet/haiku)을 그대로 재현했다. `claude-sonnet-5`는 이 시드에서 GPU 제공자 B가 있어 `external_api` 모드가 아니며(M4 mode CTE 순서: provider_reported/all_services가 먼저), vendor_price는 `_vendor_price()`가 "모드가 external_api일 때만" 참조한다 — E2E 기대값에 영향 없음, 4행은 `dim_token_vendor_price_dist` DDL·INSERT 컬럼 계약 검증용.
4. **이미지 스모크 `docker run --rm token-mart-metrics:ci --help`** — T8 Dockerfile이 `ENTRYPOINT ["python", "-m", "app.batch"]`라 원형(`CMD`)의 `python -m app.batch --help` 인자를 붙이면 argparse `unrecognized arguments: python -m app.batch`로 실패한다.
5. **`metrics_serving_dup_key` 자연키 선확인(스텝 8)** — 시드가 같은 (A, Qwen3-32B)에 metric 2종(ttft_ms/output_tps) 행을 넣으므로 T9 블록이 `(service, model, metric, name)`로 묶여 있어야 오탐이 없다. T9 본문은 이미 자연키이지만 awk 1줄로 커밋 전에 확인한다(앵커는 SELECT 별칭 줄 — 파일 머리 주석의 이름 나열에서 시작하면 첫 `GROUP BY`가 gpu 블록 것으로 잡힌다).
6. **M2 H100 그룹 quality = `flagged`** — 아웃라인 시나리오 표는 이 그룹을 "정상"으로 적었으나 T7-2의 우선순위(`over_report > no_tco > no_allocation > flagged > normal`)에서 `flagged_gpu_hours`(B standby 50h, `hours_over_count`)가 0보다 크면 `flagged`다. 기대값·verify SQL(`m2_h100_idle`은 idle/gap만 검사, quality는 `m2_rows`와 `test_e2e_seed.py`에서 `flagged`로 단언)을 T7 SQL에 맞췄다 — idle 72·gap 0 검산은 그대로 성립한다(FAIL 행은 `flagged_gpu_hours`로 항등식에 포함).
7. **M2 23컬럼·M4 14컬럼 계약은 DDL(Plan 6a)에서 읽는다** — `verify_expected_results.sql`은 컬럼명만 참조하고 컬럼 수는 단언하지 않는다(컬럼 수 계약은 T6/T7 단위 테스트가 `SQL_M4`/`SQL_M2`의 INSERT 컬럼 목록으로 고정).
8. **DDL 로더의 `--` 주석 줄 제거 + 따옴표 인식 문장 분리** — Plan 6a·6c DDL 파일 주석에 세미콜론이 들어 있을 수 있어(`;`로 문장을 나누는 원형 로더가 주석 안의 `;`에서 잘린다) 결합 전에 `--`로 시작하는 줄을 지우고, 문장 분리는 Plan 6b run_e2e.sh의 `split_statements`(단일따옴표 문자열 안의 `;` 무시, `''` 이스케이프는 토글 2회)를 그대로 가져와 쓴다. 6a T1 테스트가 COMMENT 문자열의 `;`를 금지하므로 지금은 `split(";")`와 결과가 같지만, 6a·6b·6c 로더의 규칙을 하나로 맞춰 COMMENT 문구가 바뀌어도 로더가 깨지지 않게 했다. 원형 3 regex(ON CLUSTER 제거·ReplicatedMergeTree→MergeTree·Distributed 재작성)는 그대로.
9. **no-metrics day = D-1(2026-09-02)** — 시드가 없는 날짜의 배치가 `status=SUCCESS metrics_coverage=0/3`·`missing_services`에 A/B/C 전부·`CHECK WARN token_mart_absent date=…`(M0b — `agg_token_service_1d`에 그 날짜 행 없음 → M4 스킵 `rows_share=0`)·`metrics_missing` FAIL 3행을 남기는지 본다. 원형의 "5월 고정일" 2차 배치를 이 케이스로 대체했다(6c에는 조직 이관 같은 날짜 의존 로직이 없다).
10. **포트 18124·이름 `*-mart-metrics`** — token-usage E2E(18123, `ch-e2e-mart`, `tokene2e-mart`)와 로컬에서 동시 실행 가능. python heredoc의 포트는 원형과 같은 이유(heredoc 프로세스는 export 안 된 셸 변수를 못 본다)로 리터럴이며, 스텝 7 정적 검증이 `CH_PORT_HOST=18124` 1회·`http://127.0.0.1:18124/` 1회로 두 값의 일치를 고정한다.
### Task 11: 문서 — docs/monitoring/grafana_dashboard_token_metrics.json(uid token-metrics-stage, 16패널) + docs/monitoring/README.md §7(additive) + docs/operations/token-metrics-deploy.md + mart/token-metrics/README.md

**Files:**
- Create: `docs/monitoring/grafana_dashboard_token_metrics.json` — Grafana 11.6.0 대시보드(uid `token-metrics-stage`, 데이터 패널 15 + 텍스트 패널 1 = 16 — 설계 §6.2가 나열한 내용 전부: 모델별 C(serving+standby 분해)·서비스별 총비용(P0-core = Σ M1 by service, stretch = M4 합산)·그룹 행·토큰 단가 p 파생(기준월·가동률 병기)·토큰/GPU-h·요청당 원가·TTFT/ITL 추이·출처(manual-v0 vs API)·데이터 품질). 기존 `docs/monitoring/grafana_dashboard_token_usage.json`과 같은 직렬화(`json.dumps(indent=2, ensure_ascii=False)` + 개행). 본 태스크의 Python 생성 스크립트(Step 3)로 1회 생성하고, 생성 스크립트 자체는 커밋하지 않는다(재작성 시 플랜의 Step 3을 다시 실행).
- Create: `docs/operations/token-metrics-deploy.md` — 설계 §7.3/§7.5 배포 런북(0단계 전제 → 9단계 트러블슈팅, 롤백 = CronJob 2개 suspend + 신규 테이블 DROP, 부분 적재 복구, `manual_load.py` 절차 링크). 사내 주소는 전부 플레이스홀더(`harbor.example.internal`, `chi-<cluster>.<ns>.svc`).
- Create: `mart/token-metrics/README.md` — 모듈 README(읽기/쓰기 테이블, 실행, 환경변수 16개(config 11 + `CH_DB_*` 5), 마커·WARN 코드, 실행 순서, 비용 모델 요약, 테스트, 배포 문서 링크).
- Create: `mart/token-metrics/tests/test_docs_contract.py` — 문서 계약 테스트(ClickHouse 불필요, 파일만 읽음). 대시보드 JSON 구조·FROM 허용집합·시간 매크로·컬럼 존재(DDL 대조)·`user_id` 부재·gridPos 비중첩, README §7 헤딩 수, 배포 문서 절·플레이스홀더·CLI 플래그 실재(install.sh/rerun.py/run_invariants.py 대조), 모듈 README 환경변수 16개(`app/config.py` 11 + `app/ch.py` 5 대조)·마커 필드(`app.mart.batch_line` 대조).
- Modify (additive — 파일 말미 append만, 기존 117행 무수정): `docs/monitoring/README.md` — `## 7. token-metrics 대시보드` 절 추가(117행 뒤).
- Test: `mart/token-metrics/tests/test_docs_contract.py`

**Interfaces:**
- Produces (문서·JSON):
  - `docs/monitoring/grafana_dashboard_token_metrics.json`: `uid = "token-metrics-stage"`, `title = "Token Metrics — Stage Tester"`, `tags = ["token-metrics", "stage"]`, `schemaVersion = 41`, `timezone = "Asia/Seoul"`, `time = {"from": "now-30d", "to": "now"}`, `__inputs[0].name = "DS_CLICKHOUSE"`, `__requires` = grafana 11.6.0 / grafana-clickhouse-datasource 4.19.0 / timeseries / table / text; `panels[0..15].id = 1..16`; 패널 1~15 `datasource = {"type": "grafana-clickhouse-datasource", "uid": "${DS_CLICKHOUSE}"}` + `targets[0] = {editorType: "sql", format: 1, pluginVersion: "4.19.0", queryType: "sql", rawSql, refId: "A"}`; 패널 16 `type = "text"`, `datasource = {"type": "datasource", "uid": "grafana"}`; `templating.list[*].name = ["service_group", "service"]`.
  - `docs/monitoring/README.md` `## 7. token-metrics 대시보드` (7번째 `## ` 헤딩; 1~6절 바이트 무수정).
  - `docs/operations/token-metrics-deploy.md` 절 헤딩 `## 0. 전제`, `## 1. 기준정보 dim 4종`, `## 2. collectors-metrics(6b)`, `## 3. mart-metrics install.sh`, `## 4. 첫 배치·마커`, `## 5. invariants_metrics`, `## 6. 대시보드`, `## 7. 재실행(rerun --chunk-days 7)`, `## 8. company-verify 격리(선택)`, `## 9. 트러블슈팅`.
  - `mart/token-metrics/README.md` 절 `## 요약`, `## 실행`, `## 환경변수`, `## 마커·WARN 코드`, `## 실행 순서`, `## 비용 모델 요약`, `## 테스트`, `## 배포`.
  - `mart/token-metrics/tests/test_docs_contract.py`: `REPO`, `DASH`, `MON_README`, `DEPLOY_DOC`, `MOD_README`, `DDL`, `TIME_MACRO`, `ALLOWED_FROM`, `PANEL_SPEC`, `GRIDPOS`, `GROUP_FILTER_PANELS`, `SERVICE_FILTER_PANELS`, `ENV_VARS`, `load_dash() -> dict`, `data_panels(d) -> list[dict]`, `from_tables(sql: str) -> set[str]`, `ddl_columns(table_local: str) -> set[str]`, `argparse_flags(path: Path) -> set[str]`, `cli_flags_in_doc(text: str, script: str) -> set[str]`, 테스트 함수 `test_dashboard_identity`, `test_panel_ids_titles_types_from`, `test_panel_columns_exist_in_rawsql_and_ddl`, `test_time_macro_and_datasource`, `test_from_tables_are_dist_and_allowed`, `test_no_user_identifiers`, `test_gridpos_fixed_and_non_overlapping`, `test_templating_and_requires`, `test_text_panel_marker_note`, `test_design_required_panels`, `test_monitoring_readme_section_7`, `test_deploy_doc_sections_and_placeholders`, `test_deploy_doc_cli_flags_exist`, `test_module_readme_env_and_marker`.
- Consumes:
  - T3–T7 mart 4테이블 컬럼(`mart/token-metrics/ddl/company/mart_metrics_tables.sql` — Plan 6a T4 DDL, 6c는 읽기만): M1 `mart.agg_token_model_cost_1d` (`date, service_group, service, model, serving_gpu_hours, standby_gpu_hours, test_gpu_hours, flagged_gpu_hours, equiv_gpu_count, scaled_intraday, model_cost_krw, input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, requests, uncached_tokens, cached_tokens, total_tokens, weighted_tokens, tokens_per_gpu_hour, gpu_type_mix, model_registered, tco_missing, has_token_rows, has_gpu_rows, quality_flag, created_by`), M3 `mart.token_metrics_check_1d` (`date, service_group, service, check_name, model, gpu_type, severity, observed, threshold, detail, source_type, created_by`), M4 `mart.agg_token_model_share_1d` (`date, model, service, service_group, provider_service, is_provider, denominator_mode, service_wtokens, model_total_wtokens, share, model_cost_krw, allocated_cost_krw, quality_flag, created_by`), M2 `mart.agg_token_gpu_group_1d` (`date, service_group, gpu_type, allocated_gpu_hours, group_total_cost_krw, serving_gpu_hours, standby_gpu_hours, test_gpu_hours, reported_gpu_hours_total, flagged_gpu_hours, model_cost_sum_krw, test_cost_krw, idle_gpu_hours, idle_cost_krw, unattributed_cost_krw, identity_gap_krw, utilization, over_report, equiv_gpu_count, tco_missing, allocation_source, quality_flag, created_by`).
  - 6b fact `fact.raw_token_metrics_summary_1d_dist` (`date, service, service_group, gpu_rows, serving_rows, rejected_rows, source_type`), `fact.raw_token_metrics_serving_1d_dist` (`date, service_group, service, model, metric, name, p50, p95, source_type` — 설계 §6.2 "성능 — service×model 단위만; 출처는 source_type"; 6c는 읽기만), 레지스트리 `gpu_data.dim_token_metrics_service_dist` (`service, enabled, coverage_since, until` — 커버리지 패널 분모를 T5 M0 술어와 같게 날짜별로 계산).
  - T5 마커 `app.mart.batch_line(status, coverage, rows_mart, rows_check, rows_share, warn_count, elapsed_s, reason="")` → `BATCH_RESULT status=… module=mart-metrics metrics_coverage=<p>/<e> missing_services="…" rows_mart=… rows_check=… rows_share=… warn=… elapsed=… [reason=…]`; WARN 코드 `CHECK WARN metrics_coverage missing=<n>`, `CHECK WARN service_not_in_usage_registry service=<s>`, `CHECK WARN token_mart_absent date=<d>`, `CHECK WARN dup_suspect:<table>`; reason `mutation_budget|read_contract|verify_count|sigterm|exception`; `PREFLIGHT FAIL read_contract missing=<…>`.
  - T5 환경변수(`app/config.py`: `CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_CLUSTER, RETRY_COUNT, RETRY_INTERVAL_S, MUTATION_POLL_S, MUTATION_TIMEOUT_S, INSERT_QUORUM, MART_METRICS_MAX_MUTATIONS_PER_RUN` / `app/ch.py`: `CH_DB_FACT, CH_DB_DIM, CH_DB_MART, CH_DB_TOKEN_MART, CH_DB_TOKEN_DIM`), `app.steps.CREATED_BY = "token-metrics-pipeline"`.
  - T8 `mart/token-metrics/install.sh` (`--overlay stage|company|company-verify`, `--context`, `--registry`, `--tag`, `-n`), CronJob `token-mart-metrics`(`20 10 * * *` KST), Secret `token-mart-metrics-ch-secret`(격리: `token-mart-metrics-ch-secret-verify`), `mart/token-metrics/tools/rerun.py` (`--from --to --context [--chunk-days 7] [--force] [-n NAMESPACE]`, 창 ≥10:50 KST, 활성 `token-mart-*` Job 0, 예산 64 = 16일 × 4).
  - T9 `python3 tools/verify/run_invariants.py --sql tools/verify/invariants_metrics.sql --date YYYY-MM-DD` → `ALL INVARIANTS PASS`.
  - T10 `bash mart/token-metrics/tests/e2e/run_e2e.sh`.
  - 6b(Plan 6b): CronJob `token-metrics-collector`(`5 2-9 * * *`), `collectors/token-metrics/install.sh`, `collectors/token-metrics/tools/manual_load.py --from --to --gpu --serving [--engine] [--replace]`, `collectors/token-metrics/tools/rerun.py --chain-mart`.
  - 기준정보 dim(Plan 6a): `assets/model-catalog/ddl/{company,stage}/dim_token_{model_alias,gpu_tco,gpu_allocation,vendor_price}.sql`, `seed_dim_token_*.sql`, `accounts_metrics.sql`, stage 합성 시드 `assets/model-catalog/fixtures/stage_seed_dim_token_*.sql`, 생성기 `assets/model-catalog/sheet_to_dim_token_model_alias_insert.py`, `assets/model-catalog/csv_to_layer_c_dim_insert.py --table gpu_tco|gpu_allocation|vendor_price`.

- [ ] **Step 1: 실패 테스트 작성 — `mart/token-metrics/tests/test_docs_contract.py` (문서 ↔ 코드 계약)**

`mart/token-metrics/tests/test_docs_contract.py` 전체 내용:

```python
"""Plan 6c T11 문서 계약 테스트 — 대시보드 JSON·README·배포 문서가 코드(app/, tools/, DDL)와 일치하는지 검사.

ClickHouse 없이 파일만 읽는다. 레포 루트 = mart/token-metrics/tests/ 의 세 단계 위.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
DASH = REPO / "docs" / "monitoring" / "grafana_dashboard_token_metrics.json"
MON_README = REPO / "docs" / "monitoring" / "README.md"
DEPLOY_DOC = REPO / "docs" / "operations" / "token-metrics-deploy.md"
MOD_README = REPO / "mart" / "token-metrics" / "README.md"
DDL = REPO / "mart" / "token-metrics" / "ddl" / "company" / "mart_metrics_tables.sql"
INSTALL_SH = REPO / "mart" / "token-metrics" / "install.sh"
RERUN_PY = REPO / "mart" / "token-metrics" / "tools" / "rerun.py"
RUN_INV = REPO / "tools" / "verify" / "run_invariants.py"
CONFIG_PY = REPO / "mart" / "token-metrics" / "app" / "config.py"
CH_PY = REPO / "mart" / "token-metrics" / "app" / "ch.py"

TIME_MACRO = "date BETWEEN toDate($__fromTime) AND toDate($__toTime)"
DS_CH = {"type": "grafana-clickhouse-datasource", "uid": "${DS_CLICKHOUSE}"}
DS_GRAFANA = {"type": "datasource", "uid": "grafana"}

# 대시보드가 읽어도 되는 테이블 — mart 계정 GRANT(Plan 6a mart accounts.sql) 안의 _dist 만
ALLOWED_FROM = {
    "mart.agg_token_model_cost_1d_dist",
    "mart.token_metrics_check_1d_dist",
    "mart.agg_token_model_share_1d_dist",
    "mart.agg_token_gpu_group_1d_dist",
    "fact.raw_token_metrics_summary_1d_dist",
    "fact.raw_token_metrics_serving_1d_dist",   # 설계 §6.2 성능(TTFT/ITL) — service×model 단위, source_type 병기
    "gpu_data.dim_token_metrics_service_dist",
}

# (id, title, type, 주 FROM, rawSql·DDL 양쪽에 있어야 하는 컬럼) — 설계 §6.2 내용 전부(아웃라인 11패널 + 설계 §6.2가 P0 산출물로 적은 5패널)
PANEL_SPEC = [
    (1, "1. 모델별 일별 model_cost_krw (serving/standby 분해)", "timeseries", "mart.agg_token_model_cost_1d_dist",
     ["model", "model_cost_krw", "serving_gpu_hours", "standby_gpu_hours"]),
    (2, "2. 서비스별 총비용 (측정, 배부 미적용)", "timeseries", "mart.agg_token_model_cost_1d_dist",
     ["service", "model_cost_krw"]),
    (3, "3. 서비스×모델 GPU 시간·비용 (당일)", "table", "mart.agg_token_model_cost_1d_dist",
     ["service", "model", "serving_gpu_hours", "standby_gpu_hours", "test_gpu_hours",
      "flagged_gpu_hours", "model_cost_krw", "tokens_per_gpu_hour", "quality_flag"]),
    (4, "4. 서비스별 tokens_per_gpu_hour 추이", "timeseries", "mart.agg_token_model_cost_1d_dist",
     ["service", "total_tokens", "serving_gpu_hours"]),
    (5, "5. 토큰 단가 p (파생 — 기준월·가동률 병기)", "table", "mart.agg_token_model_cost_1d_dist",
     ["service_group", "model", "model_cost_krw", "weighted_tokens"]),
    (6, "6. quality_flag 분포", "table", "mart.agg_token_model_cost_1d_dist",
     ["quality_flag"]),
    (7, "7. 검사 결과 (FAIL/WARN)", "table", "mart.token_metrics_check_1d_dist",
     ["date", "service", "check_name", "severity", "model", "gpu_type", "observed", "threshold",
      "detail", "source_type"]),
    (8, "8. 일별 FAIL/WARN 건수", "timeseries", "mart.token_metrics_check_1d_dist",
     ["severity"]),
    (9, "9. 모델 비용 배분 (share)", "table", "mart.agg_token_model_share_1d_dist",
     ["model", "service", "provider_service", "denominator_mode", "share", "allocated_cost_krw",
      "quality_flag"]),
    (10, "10. 서비스별 배분 총비용 (M4 합산, stretch)", "table", "mart.agg_token_model_share_1d_dist",
     ["service", "denominator_mode", "allocated_cost_krw"]),
    (11, "11. 그룹 GPU 정체성 (I2)", "table", "mart.agg_token_gpu_group_1d_dist",
     ["service_group", "gpu_type", "allocated_gpu_hours", "reported_gpu_hours_total",
      "model_cost_sum_krw", "test_cost_krw", "idle_gpu_hours", "idle_cost_krw", "unattributed_cost_krw",
      "utilization", "identity_gap_krw", "over_report", "quality_flag"]),
    (12, "12. 그룹 utilization 추이", "timeseries", "mart.agg_token_gpu_group_1d_dist",
     ["service_group", "gpu_type", "utilization"]),
    (13, "13. TTFT/ITL 추이 (p50/p95)", "timeseries", "fact.raw_token_metrics_serving_1d_dist",
     ["service", "model", "metric", "p50", "p95", "source_type"]),
    (14, "14. 출처 (manual-v0 vs API)", "timeseries", "fact.raw_token_metrics_summary_1d_dist",
     ["service", "source_type"]),
    (15, "15. 일별 메트릭 커버리지", "table", "fact.raw_token_metrics_summary_1d_dist",
     ["service", "rejected_rows"]),
]
GRIDPOS = {
    1: (0, 0, 12, 8), 2: (12, 0, 12, 8), 3: (0, 8, 12, 8), 4: (12, 8, 12, 8), 5: (0, 16, 12, 8),
    6: (12, 16, 12, 8), 7: (0, 24, 16, 8), 8: (16, 24, 8, 8), 9: (0, 32, 12, 8), 10: (12, 32, 12, 8),
    11: (0, 40, 12, 8), 12: (12, 40, 12, 8), 13: (0, 48, 12, 8), 14: (12, 48, 6, 8), 15: (18, 48, 6, 8),
    16: (0, 56, 24, 4),
}
# 템플릿 변수 사용 패널 — service_group: 커버리지(15) 제외 전부; service: p 파생(5, 모델 단위 C÷W 라 서비스 필터 무의미)·M2(11·12, service 컬럼 없음)·커버리지(15) 제외
GROUP_FILTER_PANELS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}
SERVICE_FILTER_PANELS = {1, 2, 3, 4, 6, 7, 8, 9, 10, 13, 14}
# 모듈 README 환경변수 표 — app/config.py 또는 app/ch.py 에 문자열로 존재해야 한다 (EXPECTED_LATE_SERVICES 없음)
ENV_VARS = [
    "CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_CLUSTER",
    "RETRY_COUNT", "RETRY_INTERVAL_S", "MUTATION_POLL_S", "MUTATION_TIMEOUT_S", "INSERT_QUORUM",
    "MART_METRICS_MAX_MUTATIONS_PER_RUN",
]
DB_ENV_VARS = ["CH_DB_FACT", "CH_DB_DIM", "CH_DB_MART", "CH_DB_TOKEN_MART", "CH_DB_TOKEN_DIM"]
MARKER_FIELDS = ["status=", "module=mart-metrics", "metrics_coverage=", "missing_services=",
                 "rows_mart=", "rows_check=", "rows_share=", "warn=", "elapsed="]


def load_dash() -> dict:
    return json.loads(DASH.read_text(encoding="utf-8"))


def data_panels(d: dict) -> list[dict]:
    return [p for p in d["panels"] if p["type"] != "text"]


def from_tables(sql: str) -> set[str]:
    """rawSql 안의 FROM/JOIN 뒤 `db.table` 식별자 집합 (서브쿼리 포함)."""
    return set(re.findall(r"\b(?:FROM|JOIN)\s+([a-z_]+\.[a-z_0-9]+)", sql))


def ddl_columns(table_local: str) -> set[str]:
    """mart_metrics_tables.sql 에서 `CREATE TABLE … <table_local>` 블록의 컬럼명 집합."""
    text = DDL.read_text(encoding="utf-8")
    m = re.search(
        r"CREATE TABLE IF NOT EXISTS " + re.escape(table_local) + r"\s*\n.*?\n\((.*?)\n\)\s*\nENGINE",
        text, re.S,
    )
    assert m is not None, f"DDL block not found: {table_local}"
    cols = set()
    for line in m.group(1).splitlines():
        mm = re.match(r"^\s{4}([a-z_0-9]+)\s+[A-Z]", line)
        if mm and mm.group(1) != "CONSTRAINT":
            cols.add(mm.group(1))
    assert cols, f"no columns parsed for {table_local}"
    return cols


def argparse_flags(path: Path) -> set[str]:
    """스크립트 원문에서 `--flag`/`-n` 형태 옵션 정의 문자열을 모은다 (bash·python 공통)."""
    text = path.read_text(encoding="utf-8")
    return set(re.findall(r"(?<![\w-])(--?[a-z][a-z0-9-]*)\b", text))


def cli_flags_in_doc(text: str, script: str) -> set[str]:
    """문서의 펜스 코드 블록 안에서 `script` 를 호출하는 줄에 쓰인 옵션 플래그 집합 (산문 줄은 제외)."""
    flags = set()
    in_fence = False
    for line in text.splitlines():
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence and script in line and not line.lstrip().startswith("#"):
            flags.update(re.findall(r"(?<![\w-])(--?[a-z][a-z0-9-]*)\b", line.split(script, 1)[1]))
    return flags
```

```python
# ---------------------------------------------------------------- 대시보드 JSON

def test_dashboard_identity():
    d = load_dash()
    assert d["uid"] == "token-metrics-stage"
    assert d["title"] == "Token Metrics — Stage Tester"
    assert d["tags"] == ["token-metrics", "stage"]
    assert d["schemaVersion"] == 41
    assert d["timezone"] == "Asia/Seoul"
    assert d["time"] == {"from": "now-30d", "to": "now"}
    assert d["__inputs"][0]["name"] == "DS_CLICKHOUSE"
    assert d["__inputs"][0]["pluginId"] == "grafana-clickhouse-datasource"
    assert len(d["panels"]) == 16
    assert [p["id"] for p in d["panels"]] == list(range(1, 17))
    # 직렬화 규약: 기존 token_usage JSON 과 동일 (indent=2, ensure_ascii=False, 개행 종료)
    raw = DASH.read_text(encoding="utf-8")
    assert raw == json.dumps(d, indent=2, ensure_ascii=False) + "\n"


def test_panel_ids_titles_types_from():
    by_id = {p["id"]: p for p in load_dash()["panels"]}
    for pid, title, ptype, main_from, _cols in PANEL_SPEC:
        p = by_id[pid]
        assert p["title"] == title, (pid, p["title"])
        assert p["type"] == ptype, (pid, p["type"])
        assert p["pluginVersion"] == "11.6.0"
        assert main_from in from_tables(p["targets"][0]["rawSql"]), (pid, main_from)
    assert by_id[16]["type"] == "text"
    assert by_id[16]["title"] == "참고: BATCH_RESULT 마커 패널"


def test_panel_columns_exist_in_rawsql_and_ddl():
    by_id = {p["id"]: p for p in load_dash()["panels"]}
    ddl_local = {
        "mart.agg_token_model_cost_1d_dist": "mart.agg_token_model_cost_1d_local",
        "mart.token_metrics_check_1d_dist": "mart.token_metrics_check_1d_local",
        "mart.agg_token_model_share_1d_dist": "mart.agg_token_model_share_1d_local",
        "mart.agg_token_gpu_group_1d_dist": "mart.agg_token_gpu_group_1d_local",
    }
    for pid, _title, _ptype, main_from, cols in PANEL_SPEC:
        sql = by_id[pid]["targets"][0]["rawSql"]
        for c in cols:
            assert re.search(r"\b" + re.escape(c) + r"\b", sql), (pid, c)
        if main_from in ddl_local:
            ddl_cols = ddl_columns(ddl_local[main_from])
            missing = [c for c in cols if c not in ddl_cols]
            assert not missing, (pid, main_from, missing)


def test_time_macro_and_datasource():
    d = load_dash()
    for p in data_panels(d):
        assert p["datasource"] == DS_CH, p["id"]
        t = p["targets"][0]
        assert t["editorType"] == "sql" and t["queryType"] == "sql" and t["format"] == 1
        assert t["pluginVersion"] == "4.19.0" and t["refId"] == "A"
        assert t["datasource"] == DS_CH
        assert TIME_MACRO in t["rawSql"], p["id"]
        assert "$__timeFilter" not in t["rawSql"], p["id"]
    text = d["panels"][15]
    assert text["datasource"] == DS_GRAFANA
    assert text["options"]["mode"] == "markdown"


def test_from_tables_are_dist_and_allowed():
    for p in data_panels(load_dash()):
        tables = from_tables(p["targets"][0]["rawSql"])
        assert tables, p["id"]
        for t in tables:
            assert t.endswith("_dist"), (p["id"], t)
            assert t in ALLOWED_FROM, (p["id"], t)


def test_no_user_identifiers():
    raw = DASH.read_text(encoding="utf-8")
    assert "user_id" not in raw
    assert "user_name" not in raw
    assert "user_email" not in raw


def test_gridpos_fixed_and_non_overlapping():
    panels = load_dash()["panels"]
    rects = []
    for p in panels:
        g = p["gridPos"]
        assert (g["x"], g["y"], g["w"], g["h"]) == GRIDPOS[p["id"]], p["id"]
        assert g["x"] + g["w"] <= 24
        rects.append((p["id"], g["x"], g["y"], g["x"] + g["w"], g["y"] + g["h"]))
    for i, (ia, ax1, ay1, ax2, ay2) in enumerate(rects):
        for ib, bx1, by1, bx2, by2 in rects[i + 1:]:
            overlap = ax1 < bx2 and bx1 < ax2 and ay1 < by2 and by1 < ay2
            assert not overlap, (ia, ib)


def test_templating_and_requires():
    d = load_dash()
    names = [v["name"] for v in d["templating"]["list"]]
    assert names == ["service_group", "service"]
    for v in d["templating"]["list"]:
        assert v["type"] == "query" and v["multi"] is True and v["includeAll"] is True
        assert v["datasource"] == DS_CH
        assert "mart.agg_token_model_cost_1d_dist" in v["query"]
    assert "${service_group:singlequote}" in d["templating"]["list"][1]["query"]
    req = {(r["type"], r["id"]): r["version"] for r in d["__requires"]}
    assert req[("grafana", "grafana")] == "11.6.0"
    assert req[("datasource", "grafana-clickhouse-datasource")] == "4.19.0"
    assert {("panel", "timeseries"), ("panel", "table"), ("panel", "text")} <= set(req)
    # 변수 사용 패널: GROUP_FILTER_PANELS 는 service_group 필터, SERVICE_FILTER_PANELS 는 service 필터 — 나머지는 그 변수를 쓰지 않는다
    by_id = {p["id"]: p for p in d["panels"]}
    for pid, _t, _ty, _f, _c in PANEL_SPEC:
        sql = by_id[pid]["targets"][0]["rawSql"]
        assert ("${service_group:singlequote}" in sql) == (pid in GROUP_FILTER_PANELS), pid
        assert ("${service:singlequote}" in sql) == (pid in SERVICE_FILTER_PANELS), pid


def test_text_panel_marker_note():
    content = load_dash()["panels"][15]["options"]["content"]
    for f in MARKER_FIELDS:
        assert f in content, f
    assert "측정" in content and "배분" in content and "추정" in content


def test_design_required_panels():
    """설계 §6.2 가 grafana_dashboard_token_metrics.json 내용으로 명시한 항목이 실제 rawSql 에 있는지 (정의서 §7 라벨 포함)."""
    by_id = {p["id"]: p for p in load_dash()["panels"]}
    sql = {pid: by_id[pid]["targets"][0]["rawSql"] for pid, *_ in PANEL_SPEC}
    # 1) 모델별 C 의 serving+standby 분해 — C × serving/(serving+standby) 비례 분해
    assert "AS serving_cost_krw" in sql[1] and "AS standby_cost_krw" in sql[1]
    assert "nullIf(serving_gpu_hours + standby_gpu_hours, 0)" in sql[1]
    # 2) 서비스별 총비용 P0-core = Σ M1 model_cost_krw by service, '배부 미적용' 라벨
    assert "'측정 (배부 미적용)' AS cost_label" in sql[2]
    assert "GROUP BY time, service" in sql[2] and "sum(model_cost_krw)" in sql[2]
    # 5) 토큰 단가 p = Σ C / Σ W (정의서 3.7) — 기준월(toStartOfMonth)·가동률(M2 Σ reported / Σ allocated) 병기, 라벨 '파생'
    assert "toStartOfMonth(date) AS base_month" in sql[5]
    assert "AS p_krw_per_m_wtoken" in sql[5] and "AS utilization_pct" in sql[5]
    assert "mart.agg_token_gpu_group_1d_dist" in from_tables(sql[5])
    assert "'파생' AS cost_label" in sql[5]
    # 10) stretch = M4 합산 by service — 라벨은 denominator_mode 에서 파생(배분/추정/그룹 귀속)
    assert "multiIf(denominator_mode = 'external_api', '추정'" in sql[10]
    assert "sum(allocated_cost_krw)" in sql[10] and "GROUP BY date, service, cost_label" in sql[10]
    # 11) 그룹 행 = ΣC + 실험 + 유휴 + 미귀속 — 네 항 모두 표시
    for col in ("model_cost_sum_krw", "test_cost_krw", "idle_cost_krw", "unattributed_cost_krw"):
        assert col in sql[11], col
    # 13) TTFT/ITL — 표준 지표 2종만, source_type 병기
    assert "metric IN ('ttft_ms', 'itl_ms')" in sql[13] and "source_type" in sql[13]
    # 14) 출처 — source_type 별 서비스 수
    assert "GROUP BY time, source_type" in sql[14]
    # 15) 커버리지 분모 = 마커 metrics_coverage 분모와 같은 술어(T5 M0: enabled=1 AND coverage_since <= d AND (until IS NULL OR d <= until))
    assert "r.enabled = 1 AND r.coverage_since <= d.date AND (isNull(r.until) OR d.date <= r.until)" in sql[15]
    assert "AS expected_services" in sql[15] and "registered_services" not in sql[15]
    # 비용 라벨 컬럼이 있는 패널은 정의서 §7 의 네 라벨(+ 파생) 밖의 값을 쓰지 않는다
    for pid in (2, 3, 5, 9, 10, 11):
        assert "AS cost_label" in sql[pid], pid
```

```python
# ---------------------------------------------------------------- README / 배포 문서

def test_monitoring_readme_section_7():
    text = MON_README.read_text(encoding="utf-8")
    heads = [ln for ln in text.splitlines() if ln.startswith("## ")]
    assert len(heads) == 7, heads
    assert heads[6] == "## 7. token-metrics 대시보드"
    sec7 = text.split("## 7. token-metrics 대시보드", 1)[1]
    assert "docs/monitoring/grafana_dashboard_token_metrics.json" in sec7
    assert "token-metrics-stage" in sec7
    assert TIME_MACRO in sec7
    # 16 패널 표: `| 1 |` … `| 16 |` 행
    for n in range(1, 17):
        assert re.search(r"^\| " + str(n) + r" \|", sec7, re.M), n
    assert "len(d['panels'])==16" in sec7
    # 기존 1~6절은 손대지 않는다 (git diff 로도 확인 — Step 6)
    assert heads[0].startswith("## 1. 전제") and heads[5].startswith("## 6. JSON 검증")


def test_deploy_doc_sections_and_placeholders():
    text = DEPLOY_DOC.read_text(encoding="utf-8")
    expected = [
        "## 0. 전제", "## 1. 기준정보 dim 4종", "## 2. collectors-metrics(6b)",
        "## 3. mart-metrics install.sh", "## 4. 첫 배치·마커", "## 5. invariants_metrics",
        "## 6. 대시보드", "## 7. 재실행(rerun --chunk-days 7)", "## 8. company-verify 격리(선택)",
        "## 9. 트러블슈팅",
    ]
    heads = [ln for ln in text.splitlines() if ln.startswith("## ")]
    assert heads == expected, heads
    # 공개 레포: 사내 호스트 0 — harbor 는 플레이스홀더만, chi 서비스 주소는 <cluster>.<ns> 형태만
    for host in re.findall(r"harbor\.[A-Za-z0-9.\-]+", text):
        assert host == "harbor.example.internal", host
    for svc in re.findall(r"chi-[A-Za-z0-9<>.\-]+\.svc", text):
        assert svc == "chi-<cluster>.<ns>.svc", svc
    assert not re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text), "email in public doc"
    # 핵심 계약 문자열
    for needle in [
        "BATCH_RESULT status=SUCCESS module=mart-metrics",
        "PREFLIGHT FAIL read_contract missing=",
        "ALL INVARIANTS PASS",
        "RERUN REFUSED window (>=10:50 KST)",
        "token-mart-metrics-ch-secret-verify",
        "MART_METRICS_MAX_MUTATIONS_PER_RUN=64",
        "reason=read_contract", "reason=mutation_budget", "token_mart_absent", "metrics_missing", "no_tco",
        "stage_seed_dim_token_",
        "manual_load.py",
    ]:
        assert needle in text, needle


def test_deploy_doc_cli_flags_exist():
    text = DEPLOY_DOC.read_text(encoding="utf-8")
    for script, path in [
        ("mart/token-metrics/install.sh", INSTALL_SH),
        ("mart/token-metrics/tools/rerun.py", RERUN_PY),
        ("tools/verify/run_invariants.py", RUN_INV),
    ]:
        used = cli_flags_in_doc(text, script)
        assert used, script
        defined = argparse_flags(path)
        missing = sorted(used - defined)
        assert not missing, (script, missing)


def test_module_readme_env_and_marker():
    text = MOD_README.read_text(encoding="utf-8")
    code = CONFIG_PY.read_text(encoding="utf-8") + CH_PY.read_text(encoding="utf-8")
    rows = [ln for ln in text.splitlines() if re.match(r"^\| `[A-Z_]+` \|", ln)]
    names = [re.match(r"^\| `([A-Z_]+)` \|", ln).group(1) for ln in rows]
    assert names == ENV_VARS + DB_ENV_VARS, names
    for n in names:
        assert f'"{n}"' in code, n
    assert "| `EXPECTED_LATE_SERVICES` |" not in text
    assert "EXPECTED_LATE_SERVICES" in text  # "없음" 을 명시하는 문장
    for f in MARKER_FIELDS:
        assert f in text, f
    for code_name in ["metrics_coverage missing=", "service_not_in_usage_registry", "token_mart_absent",
                      "dup_suspect:"]:
        assert code_name in text, code_name
    assert "M0 → M0b → M1 → M3 → M4 → M2" in text
    assert "docs/cost-model-spec.md" in text
    assert "docs/operations/token-metrics-deploy.md" in text
    assert "bash tests/e2e/run_e2e.sh" in text
```

- [ ] **Step 2: 실패 확인 (RED)**

실행(첫 줄은 전제 가드 — `test_panel_columns_exist_in_rawsql_and_ddl` 이 읽는 Plan 6a DDL 이 없으면 `DDL block not found` 가 아니라 여기서 멈춘다):

```bash
cd /home/mini/github/token-data-pipeline && test -f mart/token-metrics/ddl/company/mart_metrics_tables.sql || { echo "Plan 6a not merged — mart/token-metrics/ddl/company/mart_metrics_tables.sql 필요"; exit 1; }
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_docs_contract.py 2>&1 | tail -20
```

기대 결과 — 대시보드 JSON·배포 문서·모듈 README 가 아직 없으므로 14개 테스트 전부 실패(`FileNotFoundError: [Errno 2] No such file or directory: '/home/mini/github/token-data-pipeline/docs/monitoring/grafana_dashboard_token_metrics.json'` 등), 마지막 줄:

```
14 failed in 0.3s
```

(`test_monitoring_readme_section_7` 는 파일이 있으므로 `AssertionError: assert 6 == 7` 로 실패한다.)

- [ ] **Step 3: 대시보드 JSON 생성 — `docs/monitoring/grafana_dashboard_token_metrics.json` (생성 스크립트 1회 실행)**

아래 스크립트를 `${TMPDIR:-/tmp}/gen_token_metrics_dashboard.py`(실행 세션의 임시 디렉터리 — 레포 밖) 로 저장하고 레포 루트에서 1회 실행한다(스크립트는 커밋하지 않는다 — 출력 JSON만 커밋). 골격·패널 보일러플레이트는 기존 `docs/monitoring/grafana_dashboard_token_usage.json`(digest §26)과 동일하고, 직렬화도 동일(`json.dumps(indent=2, ensure_ascii=False)` + 개행).

```python
import json

OUT = "docs/monitoring/grafana_dashboard_token_metrics.json"
DS_CH = {"type": "grafana-clickhouse-datasource", "uid": "${DS_CLICKHOUSE}"}
DS_GRAFANA = {"type": "datasource", "uid": "grafana"}
TIME = "date BETWEEN toDate($__fromTime) AND toDate($__toTime)"
F_GROUP = "service_group IN (${service_group:singlequote})"
F_SERVICE = "service IN (${service:singlequote})"


def target(raw_sql):
    return {"datasource": DS_CH, "editorType": "sql", "format": 1, "pluginVersion": "4.19.0",
            "queryType": "sql", "rawSql": raw_sql, "refId": "A"}


def grid(x, y, w, h):
    return {"x": x, "y": y, "w": w, "h": h}


def timeseries(pid, title, desc, gp, raw_sql, unit="short"):
    return {
        "id": pid, "type": "timeseries", "title": title, "description": desc,
        "datasource": DS_CH, "gridPos": grid(*gp),
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic"},
                "custom": {
                    "axisBorderShow": False, "axisCenteredZero": False, "axisColorMode": "text",
                    "axisPlacement": "auto", "barAlignment": 0, "drawStyle": "line", "fillOpacity": 10,
                    "gradientMode": "none", "hideFrom": {"legend": False, "tooltip": False, "viz": False},
                    "insertNulls": False, "lineInterpolation": "linear", "lineWidth": 1, "pointSize": 5,
                    "scaleDistribution": {"type": "linear"}, "showPoints": "auto", "spanNulls": False,
                    "stacking": {"group": "A", "mode": "none"}, "thresholdsStyle": {"mode": "off"},
                },
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": 0}]},
                "unit": unit,
            },
            "overrides": [],
        },
        "options": {"legend": {"calcs": [], "displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "none"}},
        "pluginVersion": "11.6.0",
        "targets": [target(raw_sql)],
    }


def table(pid, title, desc, gp, raw_sql):
    return {
        "id": pid, "type": "table", "title": title, "description": desc,
        "datasource": DS_CH, "gridPos": grid(*gp),
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "custom": {"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False},
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": 0}]},
            },
            "overrides": [],
        },
        "options": {"cellHeight": "sm",
                    "footer": {"countRows": False, "fields": "", "reducer": ["sum"], "show": False},
                    "showHeader": True, "sortBy": []},
        "pluginVersion": "11.6.0",
        "targets": [target(raw_sql)],
    }


def text(pid, title, gp, content):
    return {
        "id": pid, "type": "text", "title": title, "datasource": DS_GRAFANA, "gridPos": grid(*gp),
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {"mode": "markdown",
                    "code": {"language": "plaintext", "showLineNumbers": False, "showMiniMap": False},
                    "content": content},
        "pluginVersion": "11.6.0", "transparent": False,
    }


SQL1 = f"""SELECT
    date AS time,
    model,
    if(countIf(model_cost_krw IS NULL) > 0, NULL, sum(model_cost_krw)) AS model_cost_krw,
    if(countIf(model_cost_krw IS NULL) > 0, NULL,
       sum(ifNull(model_cost_krw * serving_gpu_hours / nullIf(serving_gpu_hours + standby_gpu_hours, 0), 0))) AS serving_cost_krw,
    if(countIf(model_cost_krw IS NULL) > 0, NULL,
       sum(ifNull(model_cost_krw * standby_gpu_hours / nullIf(serving_gpu_hours + standby_gpu_hours, 0), 0))) AS standby_cost_krw
FROM mart.agg_token_model_cost_1d_dist
WHERE {TIME}
  AND {F_GROUP}
  AND {F_SERVICE}
GROUP BY time, model
ORDER BY time, model"""

SQL2 = f"""SELECT
    date AS time,
    service,
    if(countIf(model_cost_krw IS NULL) > 0, NULL, sum(model_cost_krw)) AS service_cost_krw,
    '측정 (배부 미적용)' AS cost_label
FROM mart.agg_token_model_cost_1d_dist
WHERE {TIME}
  AND {F_GROUP}
  AND {F_SERVICE}
GROUP BY time, service
ORDER BY time, service"""

SQL3 = f"""SELECT
    date,
    service_group,
    service,
    model,
    serving_gpu_hours,
    standby_gpu_hours,
    test_gpu_hours,
    flagged_gpu_hours,
    equiv_gpu_count,
    model_cost_krw,
    requests,
    round(model_cost_krw / nullIf(requests, 0), 2) AS krw_per_request,
    tokens_per_gpu_hour,
    quality_flag,
    '측정' AS cost_label
FROM mart.agg_token_model_cost_1d_dist
WHERE {TIME}
  AND date = (SELECT max(date) FROM mart.agg_token_model_cost_1d_dist WHERE {TIME})
  AND {F_GROUP}
  AND {F_SERVICE}
ORDER BY model_cost_krw DESC NULLS LAST, service, model"""

SQL4 = f"""SELECT
    date AS time,
    service,
    sum(total_tokens) / nullIf(sum(serving_gpu_hours), 0) AS tokens_per_gpu_hour
FROM mart.agg_token_model_cost_1d_dist
WHERE {TIME}
  AND {F_GROUP}
  AND {F_SERVICE}
GROUP BY time, service
ORDER BY time, service"""

SQL5 = f"""SELECT
    c.base_month AS base_month,
    c.service_group AS service_group,
    c.model AS model,
    c.model_cost_krw AS model_cost_krw,
    c.weighted_tokens AS weighted_tokens,
    round(c.model_cost_krw / nullIf(c.weighted_tokens, 0) * 1000000, 2) AS p_krw_per_m_wtoken,
    round(c.model_cost_krw / nullIf(c.weighted_tokens, 0) * 1000000 * 0.1, 2) AS p_cached_krw_per_m,
    round(c.model_cost_krw / nullIf(c.weighted_tokens, 0) * 1000000 * 4, 2) AS p_output_krw_per_m,
    round(g.utilization * 100, 1) AS utilization_pct,
    '파생' AS cost_label
FROM (
    SELECT toStartOfMonth(date) AS base_month,
           service_group,
           model,
           if(countIf(model_cost_krw IS NULL) > 0, NULL, sum(model_cost_krw)) AS model_cost_krw,
           sum(weighted_tokens) AS weighted_tokens
    FROM mart.agg_token_model_cost_1d_dist
    WHERE {TIME}
      AND {F_GROUP}
    GROUP BY base_month, service_group, model
) AS c
LEFT JOIN (
    SELECT toStartOfMonth(date) AS base_month,
           service_group,
           sum(reported_gpu_hours_total) / nullIf(sum(allocated_gpu_hours), 0) AS utilization
    FROM mart.agg_token_gpu_group_1d_dist
    WHERE {TIME}
      AND {F_GROUP}
    GROUP BY base_month, service_group
) AS g ON g.base_month = c.base_month AND g.service_group = c.service_group
ORDER BY base_month DESC, service_group, model"""

SQL6 = f"""SELECT
    quality_flag,
    count() AS row_count,
    countIf(model_cost_krw IS NULL) AS cost_null_rows,
    countIf(has_gpu_rows = 0) AS no_gpu_rows,
    countIf(has_token_rows = 0) AS no_token_rows
FROM mart.agg_token_model_cost_1d_dist
WHERE {TIME}
  AND {F_GROUP}
  AND {F_SERVICE}
GROUP BY quality_flag
ORDER BY row_count DESC"""

SQL7 = f"""SELECT
    date,
    service_group,
    service,
    check_name,
    severity,
    model,
    gpu_type,
    observed,
    threshold,
    detail,
    source_type
FROM mart.token_metrics_check_1d_dist
WHERE {TIME}
  AND severity IN ('FAIL', 'WARN')
  AND {F_GROUP}
  AND {F_SERVICE}
ORDER BY date DESC, severity, service, check_name
LIMIT 500"""

SQL8 = f"""SELECT
    date AS time,
    severity,
    count() AS checks
FROM mart.token_metrics_check_1d_dist
WHERE {TIME}
  AND severity IN ('FAIL', 'WARN')
  AND {F_GROUP}
  AND {F_SERVICE}
GROUP BY time, severity
ORDER BY time, severity"""

SQL9 = f"""SELECT
    date,
    model,
    service,
    service_group,
    provider_service,
    is_provider,
    denominator_mode,
    share,
    model_cost_krw,
    allocated_cost_krw,
    quality_flag,
    multiIf(denominator_mode = 'external_api', '추정',
            denominator_mode = 'token_not_reported', '그룹 귀속',
            '배분') AS cost_label
FROM mart.agg_token_model_share_1d_dist
WHERE {TIME}
  AND {F_GROUP}
  AND {F_SERVICE}
ORDER BY date DESC, model, share DESC NULLS LAST
LIMIT 500"""

SQL10 = f"""SELECT
    date,
    service,
    multiIf(denominator_mode = 'external_api', '추정',
            denominator_mode = 'token_not_reported', '그룹 귀속',
            '배분') AS cost_label,
    if(countIf(allocated_cost_krw IS NULL) > 0, NULL, sum(allocated_cost_krw)) AS service_allocated_cost_krw,
    count() AS model_rows
FROM mart.agg_token_model_share_1d_dist
WHERE {TIME}
  AND {F_GROUP}
  AND {F_SERVICE}
GROUP BY date, service, cost_label
ORDER BY date DESC, service, cost_label
LIMIT 500"""

SQL11 = f"""SELECT
    date,
    service_group,
    gpu_type,
    allocated_gpu_hours,
    reported_gpu_hours_total,
    serving_gpu_hours,
    standby_gpu_hours,
    test_gpu_hours,
    flagged_gpu_hours,
    idle_gpu_hours,
    utilization,
    group_total_cost_krw,
    model_cost_sum_krw,
    test_cost_krw,
    idle_cost_krw,
    unattributed_cost_krw,
    identity_gap_krw,
    over_report,
    allocation_source,
    quality_flag,
    '측정' AS cost_label
FROM mart.agg_token_gpu_group_1d_dist
WHERE {TIME}
  AND {F_GROUP}
ORDER BY date DESC, service_group, gpu_type
LIMIT 500"""

SQL12 = f"""SELECT
    date AS time,
    concat(service_group, '/', gpu_type) AS group_gpu,
    avg(utilization) AS utilization
FROM mart.agg_token_gpu_group_1d_dist
WHERE {TIME}
  AND {F_GROUP}
GROUP BY time, group_gpu
ORDER BY time, group_gpu"""

SQL13 = f"""SELECT
    date AS time,
    concat(service, '/', model, ' ', metric) AS series,
    source_type,
    avg(p50) AS p50,
    avg(p95) AS p95
FROM fact.raw_token_metrics_serving_1d_dist
WHERE {TIME}
  AND metric IN ('ttft_ms', 'itl_ms')
  AND name = ''
  AND {F_GROUP}
  AND {F_SERVICE}
GROUP BY time, series, source_type
ORDER BY time, series"""

SQL14 = f"""SELECT
    date AS time,
    source_type,
    countDistinct(service) AS services
FROM fact.raw_token_metrics_summary_1d_dist
WHERE {TIME}
  AND {F_GROUP}
  AND {F_SERVICE}
GROUP BY time, source_type
ORDER BY time, source_type"""

SQL15 = f"""SELECT
    f.date AS date,
    f.reported_services AS reported_services,
    e.expected_services AS expected_services,
    f.gpu_rows AS gpu_rows,
    f.serving_rows AS serving_rows,
    f.rejected_rows AS rejected_rows,
    f.manual_services AS manual_services
FROM (
    SELECT date,
           countDistinct(service) AS reported_services,
           sum(gpu_rows) AS gpu_rows,
           sum(serving_rows) AS serving_rows,
           sum(rejected_rows) AS rejected_rows,
           countIf(source_type = 'manual-v0') AS manual_services
    FROM fact.raw_token_metrics_summary_1d_dist
    WHERE {TIME}
    GROUP BY date
) AS f
LEFT JOIN (
    SELECT d.date AS date,
           countIf(r.enabled = 1 AND r.coverage_since <= d.date AND (isNull(r.until) OR d.date <= r.until)) AS expected_services
    FROM (SELECT DISTINCT date FROM fact.raw_token_metrics_summary_1d_dist WHERE {TIME}) AS d
    CROSS JOIN gpu_data.dim_token_metrics_service_dist AS r
    GROUP BY d.date
) AS e ON e.date = f.date
ORDER BY date DESC"""

TEXT16 = (
    "**참고 — BATCH_RESULT 마커 패널 (미포함)**\n\n"
    "이 대시보드는 mart 4테이블(`mart.agg_token_model_cost_1d_dist` 등)과 앵커 fact "
    "`fact.raw_token_metrics_summary_1d_dist`·성능 fact `fact.raw_token_metrics_serving_1d_dist`의 실적 행만 관측합니다. "
    "CronJob `token-mart-metrics`(10:20 KST) 실행 자체의 판정은 로그 마커 기반입니다:\n\n"
    "`BATCH_RESULT status=SUCCESS|FAILURE module=mart-metrics metrics_coverage=<present>/<enabled> "
    "missing_services=\"a,b|-\" rows_mart=<n> rows_check=<n> rows_share=<n> warn=<n> elapsed=<s> [reason=<r>]`\n\n"
    "마커 조회에는 VictoriaLogs 데이터소스가 필요합니다 — 홈랩(stage)에는 없어 이 대시보드에는 포함하지 않았고, "
    "company 단계에서 기존 `batch_result` 대시보드에 module `mart-metrics`로 편입됩니다 "
    "(`kubectl -n monitoring logs job/<job> | grep BATCH_RESULT` 로 대체 가능).\n\n"
    "**라벨 규칙 (docs/cost-model-spec.md §7)** — `cost_label` 컬럼: `측정` = GPU 시간 × TCO(M1·M2; 패널 2의 `측정 (배부 미적용)` = "
    "서비스별 Σ M1 C, 공유 모델 배분 전), `배분` = 가중 토큰 비율(1/0.1/4)로 나눈 공유 모델 비용(M4), "
    "`추정` = 사외 API 벤더 단가 기반(M4 `external_api`), `그룹 귀속` = 토큰 미보고 모델의 호스팅 그룹 전액 귀속, "
    "`파생` = 토큰 단가 p = C ÷ W(정의서 3.7 — 비용 입력이 아니라 배분의 결과, 기준월·가동률 병기). "
    "비용 NULL = TCO/단가 부재(`no_tco`·`vendor_price_missing`) — 0이 아니라 '측정 불가'입니다."
)

panels = [
    timeseries(1, "1. 모델별 일별 model_cost_krw (serving/standby 분해)",
               "M1 agg_token_model_cost_1d_dist — 모델별 C = Σ(serving+standby, 비FAIL) gpu_hours × TCO (측정). "
               "serving_cost/standby_cost = C × 시간 비례 분해(기종 혼합 행은 근사). TCO 부재 행이 하나라도 있으면 그 날·모델은 NULL.",
               (0, 0, 12, 8), SQL1, unit="currencyKRW"),
    timeseries(2, "2. 서비스별 총비용 (측정, 배부 미적용)",
               "설계 §6.2 P0-core — 서비스별 Σ M1 model_cost_krw (그 서비스가 호스팅한 모델의 C 합, 공유 모델 배분 전). "
               "배분 후 서비스 비용은 패널 10(M4 합산).",
               (12, 0, 12, 8), SQL2, unit="currencyKRW"),
    table(3, "3. 서비스×모델 GPU 시간·비용 (당일)",
          "M1 — 선택 범위 안의 최신 집계일(max(date)) 한 날의 service×model 행. krw_per_request = model_cost_krw / requests (요청당 원가, 정의서 §7).",
          (0, 8, 12, 8), SQL3),
    timeseries(4, "4. 서비스별 tokens_per_gpu_hour 추이",
               "M1 — 서비스별 Σ total_tokens / Σ serving_gpu_hours (분모 0이면 NULL).",
               (12, 8, 12, 8), SQL4),
    table(5, "5. 토큰 단가 p (파생 — 기준월·가동률 병기)",
          "정의서 3.7 — p = Σ C(model) / Σ W(model) (원/가중토큰 → ×1e6 = 원/1M 가중토큰), p_cached = 0.1p, p_output = 4p. "
          "기준월 = base_month(toStartOfMonth), 가동률 = utilization_pct(M2 Σ reported / Σ allocated, 같은 달·같은 그룹; 할당표 없으면 NULL). "
          "배분의 결과이지 비용 입력이 아니다(순환) — 정보용.",
          (0, 16, 12, 8), SQL5),
    table(6, "6. quality_flag 분포",
          "M1 — 선택 범위의 quality_flag 별 행수 (partial > no_tco > flagged > manual > no_metrics > consumer_only > normal).",
          (12, 16, 12, 8), SQL6),
    table(7, "7. 검사 결과 (FAIL/WARN)",
          "M3 token_metrics_check_1d_dist — severity FAIL/WARN 행 (INFO 제외). detail 은 사용자 식별자·payload 를 담지 않는다(마스터 §5.6).",
          (0, 24, 16, 8), SQL7),
    timeseries(8, "8. 일별 FAIL/WARN 건수",
               "M3 — 일별 severity 별 검사 건수.",
               (16, 24, 8, 8), SQL8),
    table(9, "9. 모델 비용 배분 (share)",
          "M4 agg_token_model_share_1d_dist — share = W(s,m)/W(m), allocated_cost_krw = model_cost_krw × share (배분) / 사외 API 는 벤더 단가 (추정).",
          (0, 32, 12, 8), SQL9),
    table(10, "10. 서비스별 배분 총비용 (M4 합산, stretch)",
          "설계 §6.2 stretch — 서비스별 Σ allocated_cost_krw 를 cost_label(배분/추정/그룹 귀속) 별로 합산(§6.4 (6) ①②③). "
          "패널 2(배부 미적용)와 나란히 본다. NULL 행이 하나라도 있으면 그 (날, 서비스, 라벨) 합은 NULL.",
          (12, 32, 12, 8), SQL10),
    table(11, "11. 그룹 GPU 정체성 (I2)",
          "M2 agg_token_gpu_group_1d_dist — 그룹 총비용(allocated×TCO) = model_cost_sum(ΣC) + test_cost(실험) + idle_cost(유휴) + unattributed(미귀속) "
          "(identity_gap_krw ≈ 0 이 정상, over_report=1 은 보고 > 배정).",
          (0, 40, 12, 8), SQL11),
    timeseries(12, "12. 그룹 utilization 추이",
               "M2 — service_group/gpu_type 별 utilization = reported_gpu_hours_total / allocated_gpu_hours.",
               (12, 40, 12, 8), SQL12, unit="percentunit"),
    timeseries(13, "13. TTFT/ITL 추이 (p50/p95)",
               "성능 fact raw_token_metrics_serving_1d_dist — service×model 단위 ttft_ms/itl_ms 의 p50·p95(ms). source_type(metrics-api-v1 | manual-v0) 병기. "
               "custom 지표·e2e/output_tps 는 제외.",
               (0, 48, 12, 8), SQL13, unit="ms"),
    timeseries(14, "14. 출처 (manual-v0 vs API)",
               "앵커 fact raw_token_metrics_summary_1d_dist — 날짜별 source_type 별 보고 서비스 수 (manual-v0 = 수기 CSV, metrics-api-v1 = API).",
               (12, 48, 6, 8), SQL14),
    table(15, "15. 일별 메트릭 커버리지",
          "앵커 fact raw_token_metrics_summary_1d_dist — 보고 서비스 수(reported_services) vs 기대 서비스 수(expected_services = 마커 metrics_coverage 분모: "
          "레지스트리 enabled=1 AND coverage_since ≤ date AND (until IS NULL OR date ≤ until), 날짜별). rejected_rows > 0 이면 6b 정규화 거부 행 존재.",
          (18, 48, 6, 8), SQL15),
    text(16, "참고: BATCH_RESULT 마커 패널", (0, 56, 24, 4), TEXT16),
]


def query_var(name, label, sql):
    return {
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
        "datasource": DS_CH, "definition": sql, "hide": 0, "includeAll": True, "label": label,
        "multi": True, "name": name, "options": [], "query": sql, "refresh": 1, "regex": "",
        "skipUrlSync": False, "sort": 1, "type": "query",
    }


dashboard = {
    "__inputs": [{
        "name": "DS_CLICKHOUSE", "label": "ClickHouse (mart)",
        "description": "mart.agg_token_model_cost_1d_dist 등 mart-metrics 4테이블 + fact.raw_token_metrics_{summary,serving}_1d_dist 조회용 — 공유 계정 mart (docs/monitoring/README.md §7 참조)",
        "type": "datasource", "pluginId": "grafana-clickhouse-datasource", "pluginName": "ClickHouse",
    }],
    "__requires": [
        {"type": "grafana", "id": "grafana", "name": "Grafana", "version": "11.6.0"},
        {"type": "datasource", "id": "grafana-clickhouse-datasource", "name": "ClickHouse", "version": "4.19.0"},
        {"type": "panel", "id": "timeseries", "name": "Time series", "version": ""},
        {"type": "panel", "id": "table", "name": "Table", "version": ""},
        {"type": "panel", "id": "text", "name": "Text", "version": ""},
    ],
    "annotations": {"list": [{
        "builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True, "hide": True,
        "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts", "type": "dashboard",
    }]},
    "editable": True, "fiscalYearStartMonth": 0, "graphTooltip": 0, "id": None, "links": [], "liveNow": False,
    "panels": panels, "refresh": "", "schemaVersion": 41,
    "tags": ["token-metrics", "stage"],
    "templating": {"list": [
        query_var("service_group", "service_group",
                  "SELECT DISTINCT service_group FROM mart.agg_token_model_cost_1d_dist ORDER BY 1"),
        query_var("service", "service",
                  "SELECT DISTINCT service FROM mart.agg_token_model_cost_1d_dist "
                  "WHERE service_group IN (${service_group:singlequote}) ORDER BY 1"),
    ]},
    "time": {"from": "now-30d", "to": "now"}, "timepicker": {}, "timezone": "Asia/Seoul",
    "title": "Token Metrics — Stage Tester", "uid": "token-metrics-stage", "version": 1, "weekStart": "",
}

with open(OUT, "w", encoding="utf-8") as f:
    f.write(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n")
print("wrote", OUT, "panels", len(dashboard["panels"]))
```

실행:

```bash
cd /home/mini/github/token-data-pipeline && python3 "${TMPDIR:-/tmp}/gen_token_metrics_dashboard.py" && python3 -c "import json;d=json.load(open('docs/monitoring/grafana_dashboard_token_metrics.json'));assert d['uid']=='token-metrics-stage';assert len(d['panels'])==16;print('ok')"
```

기대 출력:

```
wrote docs/monitoring/grafana_dashboard_token_metrics.json panels 16
ok
```

- [ ] **Step 4: 대시보드 테스트 통과 확인 (GREEN — JSON 10개)**

실행:

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_docs_contract.py -k "dashboard or panel or time_macro or from_tables or user_identifiers or gridpos or templating or text_panel" 2>&1 | tail -3
```

기대 결과(마지막 줄):

```
10 passed, 4 deselected in 0.1s
```

`test_panel_columns_exist_in_rawsql_and_ddl` 이 `DDL block not found` 로 실패하면 T3의 `mart/token-metrics/ddl/company/mart_metrics_tables.sql` 이 체크아웃에 없는 것이다(Plan 6a T4 산출물 — 6c 브랜치는 6a 머지 후 시작).

- [ ] **Step 5: `docs/monitoring/README.md` 말미에 `## 7. token-metrics 대시보드` 추가 (additive — append only)**

기존 117행(`   읽기 전용 커맨드(\`clickhouse-format\`)이며 클러스터에 아무 것도 쓰지 않는다.`) 뒤에 아래 블록을 그대로 append 한다(1~6절 무수정). 레포 루트에서:

````bash
cat >> docs/monitoring/README.md <<'MD'

## 7. token-metrics 대시보드

`grafana_dashboard_token_metrics.json`(uid `token-metrics-stage`, title `Token Metrics — Stage Tester`,
tags `token-metrics`/`stage`)은 Plan 6c(mart/token-metrics)의 stage 검증용 대시보드다(설계
2026-08-31 §6.2). 기존 `grafana_dashboard_token_usage.json`(uid `token-usage-stage`)과는 **별개
파일·별개 uid**이며 기존 JSON은 무수정이다. 전제(§1 플러그인 v4·§2 데이터소스·§3 임포트 절차)는
그대로 — 같은 `mart` 계정 데이터소스를 쓰고, 6c 계정 GRANT(`mart/token-metrics/ddl/company/accounts.sql`)가
mart 4테이블·fact 앵커·레지스트리 SELECT를 포함하므로 추가 설정은 없다.

조회 대상은 mart-metrics 4테이블(`mart.agg_token_model_cost_1d_dist` M1, `mart.token_metrics_check_1d_dist` M3,
`mart.agg_token_model_share_1d_dist` M4, `mart.agg_token_gpu_group_1d_dist` M2) + 앵커 fact
`fact.raw_token_metrics_summary_1d_dist` + 성능 fact `fact.raw_token_metrics_serving_1d_dist`(service×model 단위만)
+ 레지스트리 `gpu_data.dim_token_metrics_service_dist`로, 데이터 패널 15개 + 텍스트 패널 1개 = 16개다(설계 §6.2가
나열한 내용 전부). `user_id`/`user_name` 컬럼은 어떤 패널에도 없다(§5.6).

| # | 패널 | FROM (물리 `_dist` 테이블) | 목적 |
|---|---|---|---|
| 1 | 모델별 일별 model_cost_krw (serving/standby 분해) | `mart.agg_token_model_cost_1d_dist` | 시계열 — 모델별 C(측정) + `serving_cost_krw`/`standby_cost_krw`(C × 시간 비례 분해). TCO 부재 행이 있으면 NULL(0 아님) |
| 2 | 서비스별 총비용 (측정, 배부 미적용) | `mart.agg_token_model_cost_1d_dist` | 시계열 — 설계 §6.2 P0-core: 서비스별 Σ M1 `model_cost_krw`, `cost_label` = `측정 (배부 미적용)` |
| 3 | 서비스×모델 GPU 시간·비용 (당일) | `mart.agg_token_model_cost_1d_dist` | 범위 내 최신 집계일 한 날의 service×model — serving/standby/test/flagged 시간, C, `krw_per_request`(요청당 원가), `tokens_per_gpu_hour`, `quality_flag` |
| 4 | 서비스별 tokens_per_gpu_hour 추이 | `mart.agg_token_model_cost_1d_dist` | 시계열 — Σ total_tokens / Σ serving_gpu_hours |
| 5 | 토큰 단가 p (파생 — 기준월·가동률 병기) | `mart.agg_token_model_cost_1d_dist` (+ `mart.agg_token_gpu_group_1d_dist` 조인) | 정의서 3.7 — 기준월(`base_month`)·그룹·모델별 p = Σ C / Σ W(원/1M 가중토큰), p_cached = 0.1p, p_output = 4p, `utilization_pct`(M2 월 가동률) 병기, `cost_label` = `파생` |
| 6 | quality_flag 분포 | `mart.agg_token_model_cost_1d_dist` | 플래그별 행수 + 비용 NULL·GPU 무·토큰 무 행수 |
| 7 | 검사 결과 (FAIL/WARN) | `mart.token_metrics_check_1d_dist` | M3 severity FAIL/WARN 행 — `check_name`·`observed`·`threshold`·`detail`·`source_type` |
| 8 | 일별 FAIL/WARN 건수 | `mart.token_metrics_check_1d_dist` | 시계열 — severity 별 건수 |
| 9 | 모델 비용 배분 (share) | `mart.agg_token_model_share_1d_dist` | M4 — `denominator_mode`, `share`, `allocated_cost_krw`; `cost_label` = 배분/추정(external_api)/그룹 귀속(token_not_reported) |
| 10 | 서비스별 배분 총비용 (M4 합산, stretch) | `mart.agg_token_model_share_1d_dist` | 설계 §6.2 stretch — 서비스별 Σ `allocated_cost_krw` 를 `cost_label` 별로 합산(§6.4 (6) ①②③); 패널 2와 대비 |
| 11 | 그룹 GPU 정체성 (I2) | `mart.agg_token_gpu_group_1d_dist` | M2 그룹 행 — 그룹 총비용 = `model_cost_sum_krw`(ΣC) + `test_cost_krw`(실험) + `idle_cost_krw`(유휴) + `unattributed_cost_krw`(미귀속), `identity_gap_krw`(≈0 정상), `over_report` |
| 12 | 그룹 utilization 추이 | `mart.agg_token_gpu_group_1d_dist` | 시계열 — service_group/gpu_type 별 utilization |
| 13 | TTFT/ITL 추이 (p50/p95) | `fact.raw_token_metrics_serving_1d_dist` | 시계열 — service×model 별 `ttft_ms`/`itl_ms` p50·p95(ms), `source_type` 병기 |
| 14 | 출처 (manual-v0 vs API) | `fact.raw_token_metrics_summary_1d_dist` | 시계열 — 날짜별 `source_type` 별 보고 서비스 수 |
| 15 | 일별 메트릭 커버리지 | `fact.raw_token_metrics_summary_1d_dist` (+ `gpu_data.dim_token_metrics_service_dist` 조인) | 보고 서비스 수(`reported_services`) vs 기대 서비스 수(`expected_services` — 마커 `metrics_coverage` 분모와 같은 술어를 날짜별로 계산), `rejected_rows`, `manual_services` |
| 16 | (텍스트) 참고: BATCH_RESULT 마커 패널 | 없음(쿼리 없음) | `BATCH_RESULT … module=mart-metrics` 마커 형식·VictoriaLogs 안내 + 라벨 규칙(측정/배분/추정/그룹 귀속/파생) |

템플릿 변수 2개: `service_group`(`SELECT DISTINCT service_group FROM mart.agg_token_model_cost_1d_dist ORDER BY 1`,
multi/All)과 `service`(같은 테이블, `WHERE service_group IN (${service_group:singlequote})`). 패널 1~14는
`service_group`, 패널 1~4·6~10·13·14는 `service` 변수로 필터한다(패널 5는 모델 단위 C÷W라 서비스 필터가 무의미,
M2 패널 11·12는 service 컬럼이 없고, 커버리지 패널 15는 필터 없음).

시간 필터는 §5 규칙 그대로 — 모든 데이터 패널이 `date BETWEEN toDate($__fromTime) AND toDate($__toTime)`
를 건다. 비용 표시는 `docs/cost-model-spec.md` §7 라벨 규칙을 따른다: `cost_label` 컬럼(측정 = GPU 시간×TCO,
배분 = 가중 토큰 비율 1/0.1/4, 추정 = 사외 API 벤더 단가, 파생 = 토큰 단가 p — 기준월·가동률 병기), 비용 NULL은
"측정 불가"(TCO/단가 부재)이지 0이 아니다. 패널 13(TTFT/ITL)은 `custom` 지표와 `e2e_ms`/`output_tps`를 보여주지
않는다 — 성능 패널은 설계 §6.2대로 service×model 단위의 표준 지연 지표 2종만.

JSON 검증(재작성 시 재실행 — §6 절차 + 아래 한 줄; 계약 테스트는
`cd mart/token-metrics && python -m pytest -q tests/test_docs_contract.py`):

```bash
python3 -c "import json;d=json.load(open('docs/monitoring/grafana_dashboard_token_metrics.json'));assert d['uid']=='token-metrics-stage';assert len(d['panels'])==16;print('ok')"
```
MD
````

- [ ] **Step 6: README §7 검증 — 헤딩 7개·추가만(삭제 0)**

실행:

```bash
grep -c "^## " docs/monitoring/README.md && git diff --numstat -- docs/monitoring/README.md && cd mart/token-metrics && python -m pytest -q tests/test_docs_contract.py -k monitoring_readme 2>&1 | tail -1
```

기대 출력(`git diff --numstat` 의 둘째 열 = 삭제 0):

```
7
52	0	docs/monitoring/README.md
1 passed, 13 deselected in 0.1s
```

- [ ] **Step 7: `docs/operations/token-metrics-deploy.md` 작성 (설계 §7.1/§7.3/§7.5 배포 런북 — 절 0~9)**

레포 루트에서 아래 heredoc 을 그대로 실행해 파일을 생성한다(사내 호스트는 전부 플레이스홀더 — `harbor.example.internal`, `chi-<cluster>.<ns>.svc`; 코드명·이메일 0).

````bash
cat > docs/operations/token-metrics-deploy.md <<'MD'
# token-metrics 배포 런북 (Plan 6a/6b/6c — 설계 2026-08-31 §7.1·§7.3·§7.5)

`/v1/metrics` 반입 파이프라인(collectors/token-metrics = 6b, mart/token-metrics = 6c)과 기준정보
dim 4종(assets/model-catalog = 6a)을 **기존 토큰 파이프라인(collectors/token-usage·mart/token-usage) 무수정**으로
얹는 절차다. 순서는 설계 §4.0 매니페스트·§7.5 그대로 — ① 기준정보 dim → ② collectors-metrics → ③ mart-metrics `install.sh`
(읽기 계약 프리플라이트) → ④ 첫 배치·마커 → ⑤ `invariants_metrics` → ⑥ 대시보드. 재실행(§7)·격리 검증(§8)·
트러블슈팅(§9)·롤백(§9 끝)은 뒤에 있다. stage 공통 환경(홈랩 컨텍스트·CH 파드 탐색·apply_sql)은
`docs/operations/stage-runbook.md` §2, 사내 2단계 검증 전략은 `docs/operations/company-verify.md`, 기존 모듈의
재실행 규칙은 `docs/operations/rerun.md` 를 따른다.

## 0. 전제

- Plan 6a 산출물이 머지돼 있다: `assets/model-catalog/ddl/{company,stage}/dim_token_{model_alias,gpu_tco,gpu_allocation,vendor_price}.sql`
  + `seed_dim_token_*.sql` + `accounts_metrics.sql`, `collectors/token-metrics/ddl/`, `mart/token-metrics/ddl/`.
- 기존 토큰 파이프라인이 설치·가동 중이다: `mart.token_usage_1d_dist`·`mart.agg_token_service_1d_dist`·
  `gpu_data.dim_token_service_dist` 가 존재해야 6c `install.sh` 의 읽기 계약 프리플라이트(3테이블/13컬럼)를 통과한다.
  없으면 §3 이 `PREFLIGHT FAIL read_contract missing=<db.table_dist:column,…>` 로 `exit 3` 한다(GPU-only 검증은 §8).
- admin 권한으로 `clickhouse-client` 를 실행할 수 있는 kube 컨텍스트가 있다(DDL·GRANT 는 admin 수동, 설계 §7.1).
  사내 클러스터 서비스 주소는 문서상 `chi-<cluster>.<ns>.svc` 로만 적는다(실값은 사내 문서).
- 컨테이너 레지스트리: stage 는 `ghcr.io/yoonsungnam/token-mart-metrics`(`.github/workflows/release-images-metrics.yml`
  이 push), company 는 `harbor.example.internal/<project>/token-mart-metrics:<sha7>` (Harbor 빌드 절차는 `company-verify.md` §0).
- 아래 셸 변수를 세션마다 잡는다(`stage-runbook.md` §2 와 같은 규칙 — 파드 이름은 `chi-` 접두로 탐색):

```bash
export KUBE_CONTEXT=<ctx>              # stage: homelab / company: 사내 컨텍스트 이름
export CH_NS=clickhouse
export CH_POD="$(kubectl --context "$KUBE_CONTEXT" -n "$CH_NS" get pods -o name | grep '^pod/chi-' | head -1 | cut -d/ -f2)"
apply_sql() { kubectl --context "$KUBE_CONTEXT" -n "$CH_NS" exec -i "$CH_POD" -- clickhouse-client --multiquery < "$1"; }
echo "$CH_POD"                          # 예: chi-<cluster>-<cluster>-0-0-0
```

## 1. 기준정보 dim 4종

`gpu_data.dim_token_model_alias`(별칭→canonical), `dim_token_gpu_tco`(기종별 TCO 원/GPU시간), `dim_token_gpu_allocation`
(그룹×기종 배정 GPU 수), `dim_token_vendor_price`(사외 API 단가 원/1M 토큰). 이력 조회 키는 `effective_from <= date` 의
최신 행이며 시드의 `2026-01-01` 플레이스홀더 행(값 NULL·`unknown`)은 항상 실값 행에 밀린다(Plan 6a D).

stage(홈랩) — DDL 미러 `ddl/stage/` + 플레이스홀더 시드 + **합성 실값 fixture**(`assets/model-catalog/fixtures/`):

```bash
for t in dim_token_model_alias dim_token_gpu_tco dim_token_gpu_allocation dim_token_vendor_price; do
  apply_sql "assets/model-catalog/ddl/stage/$t.sql"
done
for t in model_alias gpu_tco gpu_allocation vendor_price; do
  apply_sql "assets/model-catalog/ddl/stage/seed_dim_token_$t.sql"           # 플레이스홀더 행 (NULL·unknown)
  apply_sql "assets/model-catalog/fixtures/stage_seed_dim_token_$t.sql"      # 합성 실값 — 파일 헤더의 clickhouse-client --multiquery < 와 동일
done
apply_sql assets/model-catalog/ddl/stage/accounts_metrics.sql                # mart 계정 4테이블 _dist SELECT (기존 accounts.sql 무수정)
```

company — DDL `ddl/company/` + 플레이스홀더 시드 + **실값은 생성기 출력(gitignore)** 을 admin 이 적용한다:

```bash
for t in dim_token_model_alias dim_token_gpu_tco dim_token_gpu_allocation dim_token_vendor_price; do
  apply_sql "assets/model-catalog/ddl/company/$t.sql"
done
for t in model_alias gpu_tco gpu_allocation vendor_price; do
  apply_sql "assets/model-catalog/ddl/company/seed_dim_token_$t.sql"
done
apply_sql assets/model-catalog/ddl/company/accounts_metrics.sql
# 실값 (레포 밖 CSV → gitignore 된 *_insert.sql — 커밋 금지, 설계 §7.2)
python3 assets/model-catalog/sheet_to_dim_token_model_alias_insert.py \
  --csv <모델탭.csv> --services collectors/token-metrics/endpoints-metrics.company.yaml \
  --effective-from <YYYY-MM-DD> --out dim_token_model_alias_insert.sql
for t in gpu_tco gpu_allocation vendor_price; do
  python3 assets/model-catalog/csv_to_layer_c_dim_insert.py --table "$t" --csv "<${t}.csv>" \
    --effective-from <YYYY-MM-DD> --out "dim_token_${t}_insert.sql"
done
for f in dim_token_model_alias_insert.sql dim_token_gpu_tco_insert.sql dim_token_gpu_allocation_insert.sql dim_token_vendor_price_insert.sql; do
  apply_sql "$f"        # 각 파일 끝 "-- 검증: 결과가 비어야 정상" 이후 SELECT 가 0행이어야 한다 (check_name, key, effective_from, cnt)
done
```

확인(4테이블 행수 — stage 는 fixture 행수, company 는 시드 + 실값):

```bash
kubectl --context "$KUBE_CONTEXT" -n "$CH_NS" exec "$CH_POD" -- clickhouse-client -q "
SELECT 'model_alias', count() FROM gpu_data.dim_token_model_alias_dist
UNION ALL SELECT 'gpu_tco', count() FROM gpu_data.dim_token_gpu_tco_dist
UNION ALL SELECT 'gpu_allocation', count() FROM gpu_data.dim_token_gpu_allocation_dist
UNION ALL SELECT 'vendor_price', count() FROM gpu_data.dim_token_vendor_price_dist"
```

TCO 가 NULL 인 기종이 남아 있으면 그 기종을 쓰는 (service, model) 의 `model_cost_krw` 는 NULL(`quality_flag=no_tco`)로
적재된다 — 0 이 아니라 "측정 불가" 다(`docs/cost-model-spec.md` §7).

## 2. collectors-metrics(6b)

수집기(CronJob `token-metrics-collector`, `5 2-9 * * *` KST — 마지막 슬롯 ≤10:04, mart 는 10:20)는 Plan 6b 산출물의
`collectors/token-metrics/install.sh` 로 설치한다 — 절차·Secret·`endpoints-metrics.company.yaml`(gitignore) 은
`collectors/token-metrics/README.md` 와 `collectors/token-metrics/ddl/README.md` 를 따른다(이 문서는 링크만).
6c 가 읽는 것은 fact 3테이블(`fact.raw_token_metrics_{gpu,serving,summary}_1d_dist`)과 레지스트리
`gpu_data.dim_token_metrics_service_dist` 다. API 가 없는 서비스의 수기 제출(manual-v0)은
`collectors/token-metrics/tools/manual_load.py --from <A> --to <B> --gpu <gpu.csv> --serving <serving.csv> [--engine <engine.csv>] [--replace]`
(템플릿 `docs/templates/token_metrics_manual_v0_*.csv`) 로 적재하며 `source_type='manual-v0'` 로 구분된다(§9 의 `manual` 플래그).

수집기가 아직 없어도 6c 는 설치·실행된다(fact 0행 → 토큰-only 행 + `CHECK WARN metrics_coverage missing=<n>`,
`status=SUCCESS`) — 다만 GPU 시간·비용이 전부 0/NULL 이라 검증은 무의미하다.

## 3. mart-metrics install.sh

이미지 빌드 후 `mart/token-metrics/install.sh` 를 실행한다. 단계는 6개이며 **[3/6] 프리플라이트가 DDL 적용([4/6]) 앞**에 있다:

| 단계 | 내용 |
|---|---|
| `[1/6]` | `registry-pull-secret` — **없을 때만** 생성(있으면 그대로 사용, 대화형) |
| `[2/6]` | Secret `token-mart-metrics-ch-secret`(격리 overlay 는 `-verify` 접미) — 키 11개 `CH_HOST CH_PORT CH_USER CH_PASSWORD CH_CLUSTER CH_DB_FACT CH_DB_DIM CH_DB_MART CH_DB_TOKEN_MART CH_DB_TOKEN_DIM MART_METRICS_MAX_MUTATIONS_PER_RUN` |
| `[3/6]` | 읽기 계약 DESCRIBE 프리플라이트 — `${CH_DB_TOKEN_MART}.token_usage_1d_dist` 9컬럼, `${CH_DB_TOKEN_MART}.agg_token_service_1d_dist` 2컬럼, `${CH_DB_TOKEN_DIM}.dim_token_service_dist` 2컬럼(=13). 누락 시 `PREFLIGHT FAIL read_contract missing=<db.table_dist:column,…>` 출력 후 `exit 3` — 테이블은 만들지 않는다 |
| `[4/6]` | `mart/token-metrics/ddl/<overlay>/mart_metrics_tables.sql` 적용(4테이블 `_local`/`_dist`; `accounts.sql` 은 admin 수동) |
| `[5/6]` | `kubectl apply -k mart/token-metrics/k8s/overlays/<overlay>` — CronJob `token-mart-metrics`(`20 10 * * *` KST, `activeDeadlineSeconds 1800`) |
| `[6/6]` | 이미지 주소 주입(`kubectl set image cronjob/token-mart-metrics token-mart-metrics=<registry>/token-mart-metrics:<tag>`) + 수동 실행 커맨드 안내 — `CH_HOST` 는 [2/6] Secret 의 키(envFrom)이지 이 단계가 넣는 정적 env 가 아니다 |

```bash
# admin — GRANT (mart 계정: mart 4테이블 INSERT/SELECT + _local ALTER DELETE, 읽기 dim 6·fact 3·토큰 mart 2)
apply_sql mart/token-metrics/ddl/company/accounts.sql          # stage 는 ddl/stage/accounts.sql

# stage
./mart/token-metrics/build.sh <sha7>
./mart/token-metrics/install.sh --overlay stage --context homelab --registry ghcr.io/yoonsungnam --tag <sha7> -n monitoring

# company
./mart/token-metrics/install.sh --overlay company --context "$KUBE_CONTEXT" --registry harbor.example.internal/<project> --tag <sha7> -n monitoring
```

Secret 값: `CH_HOST` 는 클러스터 내부 서비스 주소(사내: `chi-<cluster>.<ns>.svc`, stage: `stage-runbook.md` §2 의 값),
`CH_DB_TOKEN_MART=mart`·`CH_DB_TOKEN_DIM=gpu_data`(기존 토큰 파이프라인 DB — 격리 검증은 §8),
`MART_METRICS_MAX_MUTATIONS_PER_RUN=64`(= 16일 × 4테이블; 정기 실행은 날짜당 ≤4).

설치 확인:

```bash
kubectl --context "$KUBE_CONTEXT" -n monitoring get cronjob token-mart-metrics -o jsonpath='{.spec.schedule}{"\n"}'   # 20 10 * * *
kubectl --context "$KUBE_CONTEXT" -n monitoring get secret token-mart-metrics-ch-secret -o jsonpath='{.data}' | python3 -c "import json,sys;print(sorted(json.load(sys.stdin)))"   # 키 11개
kubectl --context "$KUBE_CONTEXT" -n "$CH_NS" exec "$CH_POD" -- clickhouse-client -q "SHOW TABLES FROM mart LIKE '%token_%'"   # agg_token_model_cost_1d_*, token_metrics_check_1d_*, agg_token_model_share_1d_*, agg_token_gpu_group_1d_* (+ 기존 token_usage_1d_*)
```

## 4. 첫 배치·마커

CronJob 을 기다리지 않고 수동 Job 으로 1회 실행한다(인자 없음 = 어제(KST) 1일 — 특정 날짜·범위는 §7 `rerun.py`):

```bash
JOB="token-mart-metrics-manual-$(TZ=Asia/Seoul date +%Y%m%d)"
kubectl --context "$KUBE_CONTEXT" -n monitoring create job --from=cronjob/token-mart-metrics "$JOB"
kubectl --context "$KUBE_CONTEXT" -n monitoring wait --for=condition=complete --timeout=1800s "job/$JOB"
kubectl --context "$KUBE_CONTEXT" -n monitoring logs "job/$JOB" | grep -E "PREFLIGHT|CHECK WARN|BATCH_RESULT"
```

성공 마커(한 줄, 값은 예시):

```
BATCH_RESULT status=SUCCESS module=mart-metrics metrics_coverage=3/3 missing_services="-" rows_mart=42 rows_check=7 rows_share=39 warn=0 elapsed=12.4
```

| 필드 | 의미 |
|---|---|
| `status` | `SUCCESS` / `FAILURE`(`reason=` 동반) — 메트릭이 없는 날도 `SUCCESS`(rows 0, `metrics_coverage` WARN)이며 별도 NODATA 상태는 없다(설계 §6.1 306) |
| `module=mart-metrics` | 고정(기존 `token-usage`·`mart-token` 과 구분 — VictoriaLogs 대시보드 필터 키) |
| `metrics_coverage=<present>/<enabled>` | 레지스트리 `enabled=1` 서비스 중 그날 앵커(`raw_token_metrics_summary_1d`)가 있는 수 |
| `missing_services="a,b"` | 앵커 없는 enabled 서비스(없으면 `"-"`) — `user_id`·payload 는 절대 마커에 싣지 않는다(마스터 §5.6) |
| `rows_mart` / `rows_check` / `rows_share` | M1 / M3 / M4 적재 행수(M2 는 `rows_mart` 에 포함되지 않음 — 로그 `STEP M2` 줄) |
| `warn` | `CHECK WARN` 건수 — `metrics_coverage missing=<n>`, `service_not_in_usage_registry service=<s>`, `token_mart_absent date=<d>`, `dup_suspect:<table>` |
| `reason` | `read_contract` / `mutation_budget` / `verify_count` / `sigterm` / `exception` (§9) |

`status=SUCCESS warn>0` 은 정상 종료다(적재됨) — WARN 코드를 §9 표로 해석한다. 첫 실행이 `FAILURE reason=read_contract` 면
§3 프리플라이트가 통과했더라도 런타임에 토큰 mart 컬럼이 바뀐 것이므로 `mart/token-metrics/app/preflight.py` 의
`READ_CONTRACT` 와 실제 `DESCRIBE` 를 대조한다.

## 5. invariants_metrics

GitHub 체크아웃의 `tools/verify/run_invariants.py` 는 `--sql` 옵션(Plan 6c T9 additive)으로 `invariants_metrics.sql`
을 실행한다 — **사내 분기본의 `run_invariants.py` 에는 `--sql` 이 없으므로** 반드시 이 체크아웃에서 실행한다.
8블록: `metrics_anchor_missing, metrics_gpu_dup_key, metrics_serving_dup_key, metrics_cost_sum_mismatch,
created_by_wrong_metrics, share_sum_mismatch, group_identity_gap, idle_negative`.

```bash
kubectl --context "$KUBE_CONTEXT" -n "$CH_NS" port-forward "$CH_POD" 18123:8123 >/dev/null 2>&1 &
PF=$!
CH_HOST=127.0.0.1 CH_PORT=18123 CH_USER=mart CH_PASSWORD=<mart-password> \
  python3 tools/verify/run_invariants.py --sql tools/verify/invariants_metrics.sql --date <YYYY-MM-DD>
kill $PF
```

기대 출력: `ALL INVARIANTS PASS (date=<YYYY-MM-DD>, DBs=fact/gpu_data/mart, sql=invariants_metrics.sql)` (exit 0 — `sql=` 접미는 T9 가 `--sql` 과 함께 추가한 출력). 위반이 있으면 `[FAIL] n건` 과
`check_name / bad_count / detail` 표가 출력되고 exit 1 — `metrics_cost_sum_mismatch` 는 M1 C 와 fact 재계산 불일치
(T3 술어 변경 여부), `share_sum_mismatch` 는 Σ allocated ≠ C ±1원(I3), `group_identity_gap` 은 `abs(identity_gap_krw) > 1`(I2),
`idle_negative` 는 `over_report=1`(I1 — 보고 > 배정, `dim_token_gpu_allocation` 갱신 대상).

## 6. 대시보드

`docs/monitoring/grafana_dashboard_token_metrics.json`(uid `token-metrics-stage`, 16패널)을 `docs/monitoring/README.md`
§3 절차로 임포트한다(데이터소스는 기존 `mart` 계정 ClickHouse 데이터소스 그대로 — §7 참조). 첫 배치 후 확인 순서:
패널 15(커버리지 `reported_services` = `expected_services` — 마커 `metrics_coverage` 와 같은 분모) → 패널 7(FAIL/WARN 0 또는 §9 해석)
→ 패널 1·2·3(비용 NULL 은 `no_tco` — §1 TCO 갱신) → 패널 11(`identity_gap_krw` ≈ 0) → 패널 13(TTFT/ITL 값이 있으면 6b serving
블록 적재 정상). `BATCH_RESULT` 마커 패널은 VictoriaLogs 가 있는 company 단계에서
기존 `batch_result` 대시보드에 module `mart-metrics` 로 편입한다(패널 16 텍스트).

## 7. 재실행(rerun --chunk-days 7)

`mart/token-metrics/tools/rerun.py` 는 CronJob `token-mart-metrics` 로부터 수동 Job 을 만들어 날짜 범위를 **7일 청크**로
순차 실행한다(청크당 `activeDeadlineSeconds` = 30분, 최대 2시간). 규칙(설계 §7.5):

- **창**: 현재 KST 가 10:50 이전이면 `RERUN REFUSED window (>=10:50 KST) — use --force` 로 exit 2 — 정기 실행(10:20)과 겹치지
  않게 한다. 단일 날짜 즉시 재실행 등 의도된 경우만 `--force`.
- **활성 Job 0**: `token-mart-*` 접두 Job(정기 `token-mart-metrics-*`·기존 `token-mart-daily-*`) 이 실행 중이면 exit 2 — 같은 mart DB
  파티션에 동시 뮤테이션을 넣지 않는다.
- **예산**: 날짜당 4 뮤테이션(M1·M3·M4·M2 DELETE) × 16일 = `MART_METRICS_MAX_MUTATIONS_PER_RUN=64`. 청크 7일 = 28 변이 ≤ 64.
  범위가 16일을 넘으면 청크가 알아서 나누므로 예산 초과(`reason=mutation_budget`)는 청크 없이 `--from/--to` 를 직접 넘긴 수동 Job 에서만 난다.
- 재실행은 날짜별 `DELETE WHERE date = …` 후 재적재(멱등) — 부분 적재(예: M1·M3 만 들어가고 M4 에서 실패) 도 같은 날짜를 다시 돌리면 4테이블 모두 정합된다.
- **토큰 mart 와 같은 구간을 backfill 할 때는 순서가 있다(설계 §6.3)**: 토큰 mart(`token-mart-daily`, 사내 스케줄은 M15 에서 확인 — GitHub 기준 04:00)
  재수행이 **끝난 뒤** mart-metrics `rerun.py` 를 실행한다. M1 의 토큰 컬럼(`input_tokens … weighted_tokens`)과 M4 전체가
  `mart.token_usage_1d_dist`/`mart.agg_token_service_1d_dist` 를 읽으므로, 토큰 mart 가 아직 옛 값이면 mart-metrics 결과도 옛 값으로 굳는다.

```bash
# 범위 재실행 (기본 청크 7일 — 예: 16일 = 7+7+2 청크 3개)
python3 mart/token-metrics/tools/rerun.py --from 2026-09-01 --to 2026-09-16 --context "$KUBE_CONTEXT" -n monitoring --chunk-days 7
# 특정 하루 즉시 재실행 (창 무시)
python3 mart/token-metrics/tools/rerun.py --from 2026-09-04 --to 2026-09-04 --context "$KUBE_CONTEXT" --force
# 진행 확인
kubectl --context "$KUBE_CONTEXT" -n monitoring get jobs -l app=token-mart-metrics
```

수집기(6b) 쪽을 다시 받은 뒤 mart 까지 이어 돌릴 때는 `collectors/token-metrics/tools/rerun.py --from <A> --to <B> --chain-mart`
가 같은 날짜로 이 스크립트를 호출한다(6c 는 체인 수신 측 — `--chain` 옵션 없음). 수기 CSV 를 `manual_load.py --replace` 로 갈아끼운 날짜도
반드시 mart 를 재실행한다(fact 만 바뀌고 mart 는 그대로이므로).

## 8. company-verify 격리(선택)

`docs/operations/company-verify.md` 1단계(격리 DB `token_verify_fact/token_verify_dim/token_verify_mart`) 에 6c 를 얹는다:

```bash
./mart/token-metrics/install.sh --overlay company-verify --context "$KUBE_CONTEXT" --registry harbor.example.internal/<project> --tag <sha7> -n monitoring
```

- Secret 이름 `token-mart-metrics-ch-secret-verify`, CronJob `token-mart-metrics-verify`, DDL 은 `mart/token-metrics/ddl/company-verify/`
  (`tools/gen_verify_ddl.py` 출력 — DB 3종 치환).
- Secret 의 `CH_DB_FACT=token_verify_fact CH_DB_DIM=token_verify_dim CH_DB_MART=token_verify_mart`. 토큰 mart 참조
  `CH_DB_TOKEN_MART`/`CH_DB_TOKEN_DIM` 은 **운영 DB(`mart`/`gpu_data`) 로 지정**해 실제 토큰 집계와 결합한다(읽기 전용 —
  6c 는 토큰 mart 에 쓰지 않는다). 운영 토큰 mart 가 아직 없으면 격리 토큰 mart(`token_verify_mart`/`token_verify_dim` —
  company-verify 1단계가 만든 빈 테이블)를 가리켜 프리플라이트를 통과시키고 **GPU-only 검증** 으로 진행한다: 매 날짜
  `CHECK WARN token_mart_absent date=<d>` + M4 스킵(`rows_share=0`)이 정상이다.
- 격리 dim 4종은 §1 의 생성기에 `--target-db token_verify_dim` 을 붙여 만든다. 불변식은 §5 명령에
  `CH_DB_FACT=token_verify_fact CH_DB_DIM=token_verify_dim CH_DB_MART=token_verify_mart` 를 앞세워 실행한다.
- 2단계(정규) 전환 = §3 `--overlay company` 재설치 + 격리 CronJob `suspend`(§9 롤백과 같은 명령).

## 9. 트러블슈팅

| 증상 | 원인 | 조치 |
|---|---|---|
| `PREFLIGHT FAIL read_contract missing=…` (install `exit 3`) 또는 `BATCH_RESULT status=FAILURE … reason=read_contract` | 토큰 mart 미설치 / `CH_DB_TOKEN_MART`·`CH_DB_TOKEN_DIM` 오기 / 토큰 mart 컬럼 변경 | 기존 파이프라인 설치 확인, Secret DB명 확인, `app/preflight.py` `READ_CONTRACT` vs `DESCRIBE` 대조 — 계약 변경이면 6c 코드 수정(기존 모듈 무수정) |
| `reason=mutation_budget` | 한 Job 에 16일 초과 범위(> 64 변이) | 범위 축소 — §7 `rerun.py --chunk-days 7` 로 청크 실행 |
| `reason=verify_count` | 적재 후 재조회 행수 ≠ 기대(EXPECTED) — 동시 쓰기·복제 지연 | 활성 Job 0 확인 후 해당 날짜 재실행; 반복되면 `dup_suspect` WARN·`invariants_metrics` 확인 |
| `reason=sigterm` (`note=sigterm`) | `activeDeadlineSeconds`(1800) 초과·노드 축출 | 부분 적재 상태 — 같은 날짜 재실행(§7 멱등). 반복되면 범위 축소 |
| `CHECK WARN token_mart_absent date=<d>` | 그 날짜 토큰 mart 행 0(토큰 배치 미완·GPU-only 격리) | 정상 — M4 스킵. 토큰 배치(`token-mart-daily`) 완료 후 §7 로 재실행하면 M4 채워짐 |
| `CHECK WARN metrics_coverage missing=<n>` / M3 `metrics_missing` FAIL | enabled 서비스의 앵커(summary) 없음 — 6b 수집 실패·API 미응답·수기 미제출 | 6b 수집 로그(`token-metrics-collector` Job) 확인 → 수집 재실행(`--chain-mart`) 또는 `manual_load.py` 수기 적재 후 §7 |
| `quality_flag=no_tco` / 비용 NULL | `dim_token_gpu_tco` 에 그 기종·날짜 유효 TCO 없음 | §1 생성기로 TCO dim 갱신(`--effective-from` 은 실제 적용일) 후 해당 범위 §7 재실행 |
| `quality_flag=flagged` / `flagged_gpu_hours>0` | 6b 정규화 FAIL 플래그(`hours_over_count`·`unknown_violation`) 행 — C 에서 제외, 그룹 `unattributed` 로 | 서비스 제공 데이터 교정 요청 → `manual_load.py --replace` 또는 재수집 후 §7 |
| `CHECK WARN service_not_in_usage_registry service=<s>` | 메트릭 레지스트리에만 있고 토큰 레지스트리(`dim_token_service`)에 없는 서비스 | 토큰 파이프라인 endpoints 등록 여부 확인(정상일 수 있음 — GPU 만 보고하는 서비스) |
| 패널 11 `over_report=1` / 불변식 `idle_negative` | 보고 GPU 시간 > 배정 × 24 | `dim_token_gpu_allocation` 갱신 또는 서비스 보고값 교정 후 §7 |
| 대시보드 변수 `service_group` 비어 있음 | M1 0행(첫 배치 전) 또는 데이터소스 계정 GRANT 누락 | §4 첫 배치, `accounts.sql` 적용 확인 |

**롤백(설계 §7.5)** — CronJob 2개 `suspend` + 신규 테이블 DROP. 기존 토큰 파이프라인·`gpu_data.dim_token_model` 은 건드리지 않는다:

```bash
kubectl --context "$KUBE_CONTEXT" -n monitoring patch cronjob token-mart-metrics -p '{"spec":{"suspend":true}}'
kubectl --context "$KUBE_CONTEXT" -n monitoring patch cronjob token-metrics-collector -p '{"spec":{"suspend":true}}'
# 필요 시 테이블 제거 (admin — mart 4 + fact 4 + dim 5; ON CLUSTER 는 DDL 파일의 클러스터명과 동일)
kubectl --context "$KUBE_CONTEXT" -n "$CH_NS" exec "$CH_POD" -- clickhouse-client --multiquery -q "
DROP TABLE IF EXISTS mart.agg_token_model_cost_1d_dist ON CLUSTER 'gpu-monitoring';  DROP TABLE IF EXISTS mart.agg_token_model_cost_1d_local ON CLUSTER 'gpu-monitoring';
DROP TABLE IF EXISTS mart.token_metrics_check_1d_dist ON CLUSTER 'gpu-monitoring';   DROP TABLE IF EXISTS mart.token_metrics_check_1d_local ON CLUSTER 'gpu-monitoring';
DROP TABLE IF EXISTS mart.agg_token_model_share_1d_dist ON CLUSTER 'gpu-monitoring'; DROP TABLE IF EXISTS mart.agg_token_model_share_1d_local ON CLUSTER 'gpu-monitoring';
DROP TABLE IF EXISTS mart.agg_token_gpu_group_1d_dist ON CLUSTER 'gpu-monitoring';   DROP TABLE IF EXISTS mart.agg_token_gpu_group_1d_local ON CLUSTER 'gpu-monitoring';"
```

fact 4테이블(`fact.raw_token_metrics_*`, `fact.collect_audit_metrics_1d`)과 dim 5테이블(`gpu_data.dim_token_metrics_service`,
`dim_token_{model_alias,gpu_tco,gpu_allocation,vendor_price}`)의 DROP 은 각 모듈 `ddl/README.md` 의 목록대로 같은 형식으로 실행한다.
재설치는 §1 부터.
MD
````

- [ ] **Step 8: 배포 문서 검증 — 절 10개·플레이스홀더만·이메일 0·CLI 플래그 실재**

실행:

```bash
grep -c "^## " docs/operations/token-metrics-deploy.md && grep -o "harbor\.[A-Za-z0-9.-]*" docs/operations/token-metrics-deploy.md | sort -u && grep -c -E "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}" docs/operations/token-metrics-deploy.md; cd mart/token-metrics && python -m pytest -q tests/test_docs_contract.py -k deploy_doc 2>&1 | tail -1
```

기대 출력:

```
10
harbor.example.internal
0
2 passed, 12 deselected in 0.1s
```

(`grep -c` 는 일치 0건이면 `0` 을 출력하고 exit 1 을 반환한다 — `;` 로 이어 두어 pytest 는 그대로 실행된다.) `test_deploy_doc_cli_flags_exist` 가 `('mart/token-metrics/install.sh', ['-n'])` 처럼 실패하면 T8 스크립트에 그 플래그가 없는 것이다 — **문서를 스크립트에 맞춘다**(스크립트는 T8 계약대로 두고, 문서의 해당 명령줄만 실제 옵션명으로 고친다).

- [ ] **Step 9: `mart/token-metrics/README.md` 작성 (모듈 README)**

레포 루트에서:

````bash
cat > mart/token-metrics/README.md <<'MD'
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
| 쓰기 — mart 4 | `mart.agg_token_model_cost_1d`(M1), `mart.token_metrics_check_1d`(M3), `mart.agg_token_model_share_1d`(M4), `mart.agg_token_gpu_group_1d`(M2) — `created_by='token-metrics-pipeline'` |

DDL: `ddl/company/mart_metrics_tables.sql`(4테이블 `_local`/`_dist`, Plan 6a) · GRANT `ddl/company/accounts.sql`(admin 수동) ·
stage 미러 `ddl/stage/`(`tools/gen_stage_ddl.py`) · 격리 미러 `ddl/company-verify/`(`tools/gen_verify_ddl.py`).

## 실행

```bash
cd mart/token-metrics
python -m app.batch --date 2026-09-04             # 하루 (기본값: 어제 KST)
python -m app.batch --from 2026-09-01 --to 2026-09-07   # 범위 — 날짜별 순차, 날짜당 뮤테이션 ≤4
python -m app.batch --date 2026-09-04 --log-level DEBUG
```

컨테이너(`Dockerfile`, `build.sh`)·CronJob `token-mart-metrics`(`20 10 * * *` KST — 수집기 마지막 슬롯 10:04 이후)·
Secret `token-mart-metrics-ch-secret` 은 `install.sh` 가 설치한다(§배포). 날짜 범위 재실행은 `tools/rerun.py`
(`--from --to [--chunk-days 7] [--force]`, 창 ≥10:50 KST, 활성 `token-mart-*` Job 0).

## 환경변수

| 이름 | 기본값 | 의미 |
|---|---|---|
| `CH_HOST` | `localhost` | ClickHouse 호스트 (클러스터 내부 서비스 주소) |
| `CH_PORT` | `8123` | HTTP 포트 |
| `CH_USER` | `default` | 접속 계정 — 운영은 공유 계정 `mart` |
| `CH_PASSWORD` | (빈 값) | 비밀번호 (Secret) |
| `CH_CLUSTER` | (빈 값) | `ON CLUSTER` 이름 — 뮤테이션(`ALTER … DELETE`) 대상 |
| `RETRY_COUNT` | `10` | 접속·쿼리 재시도 횟수 |
| `RETRY_INTERVAL_S` | `5` | 재시도 간격(초) |
| `MUTATION_POLL_S` | `3` | `system.mutations` 폴링 간격(초) |
| `MUTATION_TIMEOUT_S` | `300` | 뮤테이션 완료 대기 한도(초) |
| `INSERT_QUORUM` | (빈 값) | `insert_quorum` 설정값 (stage 1s×1r 은 빈 값) |
| `MART_METRICS_MAX_MUTATIONS_PER_RUN` | `64` | 실행당 뮤테이션 예산 = 16일 × 4테이블 — 초과 시 `reason=mutation_budget` |
| `CH_DB_FACT` | `fact` | 메트릭 fact DB |
| `CH_DB_DIM` | `gpu_data` | dim DB (레지스트리·기준정보) |
| `CH_DB_MART` | `mart` | 쓰기 mart DB |
| `CH_DB_TOKEN_MART` | `mart` | 읽는 토큰 mart DB (격리 검증 시 운영 DB 지정 가능) |
| `CH_DB_TOKEN_DIM` | `gpu_data` | 읽는 토큰 레지스트리 DB |

`EXPECTED_LATE_SERVICES` 와 `ORG_MAP_WARN_THRESHOLD` 는 이 모듈에 **없다**(토큰 mart 전용 — 메트릭 지연 판정은 M3 `metrics_missing` 검사가 대신한다).
앞 11개는 `app/config.py`, `CH_DB_*` 5개는 `app/ch.py` 가 읽는다. Secret 키 = 11개(`CH_HOST … CH_DB_TOKEN_DIM`, `MART_METRICS_MAX_MUTATIONS_PER_RUN`).

## 마커·WARN 코드

실행 끝에 한 줄(마스터 §5.6 — `user_id`·payload 는 싣지 않는다, `missing_services` 는 따옴표):

```
BATCH_RESULT status=SUCCESS|FAILURE module=mart-metrics metrics_coverage=<present>/<enabled> missing_services="a,b|-" rows_mart=<n> rows_check=<n> rows_share=<n> warn=<n> elapsed=<s.s> [reason=<r>]
```

`app.mart.batch_line(status, coverage, rows_mart, rows_check, rows_share, warn_count, elapsed_s, reason="")` 이 만든다.
`rows_mart` = M1 행수, `rows_check` = M3, `rows_share` = M4(스킵 시 0). `reason` 은 FAILURE 에만: `read_contract`(토큰 mart 읽기
계약 불일치 — `PREFLIGHT FAIL read_contract missing=<…>` 선행), `mutation_budget`, `verify_count`(적재 후 재조회 ≠ EXPECTED),
`sigterm`(+ `note=sigterm`), `exception`.

| WARN 코드(로그 `CHECK WARN …`) | 의미 | 상태 |
|---|---|---|
| `metrics_coverage missing=<n>` | enabled 서비스 중 앵커(summary) 없는 수 — 같은 서비스는 M3 `metrics_missing` FAIL 행 | SUCCESS 유지 |
| `service_not_in_usage_registry service=<s>` | 메트릭 레지스트리에만 있는 서비스(토큰 레지스트리 `dim_token_service` 부재) | SUCCESS 유지 |
| `token_mart_absent date=<d>` | 그 날짜 토큰 mart 0행 — M4 스킵(`rows_share=0`), M1 은 GPU-only 행 | SUCCESS 유지 |
| `dup_suspect:<table>` | 적재 후 키 중복 의심(재조회 uniqExact < count) | SUCCESS 유지 — `invariants_metrics` 로 확인 |

## 실행 순서

M0 → M0b → M1 → M3 → M4 → M2 (날짜마다):

1. **M0** 읽기 계약 프리플라이트 — `DESCRIBE` 3테이블/13컬럼(`app/preflight.py`); 실패 시 적재 없이 `FAILURE reason=read_contract`.
2. **M0b** 커버리지 — 레지스트리 `enabled=1` ∩ 앵커 `raw_token_metrics_summary_1d` → `metrics_coverage`·`missing_services`.
3. **M1** `agg_token_model_cost_1d` — keys = 토큰 집계 ∪ GPU 집계, `model` 은 `dim_token_model_alias` 로 canonical 화, C = Σ(serving+standby, 비FAIL) gpu_hours × TCO(`dim_token_gpu_tco`, `effective_from <= date` 최신), 토큰 4종·`weighted_tokens`·`tokens_per_gpu_hour`, `quality_flag`(partial > no_tco > flagged > manual > no_metrics > consumer_only > normal).
4. **M3** `token_metrics_check_1d` — 검사 행(FAIL/WARN/INFO): `metrics_missing`, 플래그·TCO 부재·중복 등(`app/steps.py`).
5. **M4** `agg_token_model_share_1d` — 공유 모델 비용 배분 share = W(s,m)/W(m), `denominator_mode` 6종(`all_services, provider_reported, token_not_reported, no_provider, provider_ambiguous, external_api`), 사외 API 는 `dim_token_vendor_price` 단가로 추정.
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
- 라벨: 측정(GPU 시간×TCO) / 배분(가중 토큰 비율) / 추정(벤더 단가) — 대시보드 `cost_label` 컬럼, 통화 KRW 고정.

## 테스트

```bash
cd mart/token-metrics
python -m pytest -q                       # 단위 (ClickHouse 불필요) — tests/test_docs_contract.py 포함(문서 ↔ 코드 계약)
bash tests/e2e/run_e2e.sh                 # E2E — 로컬 ClickHouse(포트 18124) + 합성 fact/dim/토큰 mart 적재 → 배치 → 기대 결과 SQL
```

CI: `.github/workflows/test-mart-metrics.yml`(단위 + E2E), `.github/workflows/release-images-metrics.yml`(이미지).
불변식: `python3 tools/verify/run_invariants.py --sql tools/verify/invariants_metrics.sql --date <YYYY-MM-DD>` → `ALL INVARIANTS PASS`.

## 배포

절차 전체(기준정보 dim → 6b 수집기 → `install.sh` 프리플라이트 → 첫 배치 → 불변식 → 대시보드 → 재실행 → 격리 검증 → 트러블슈팅·롤백)는
`docs/operations/token-metrics-deploy.md`. 대시보드 `docs/monitoring/grafana_dashboard_token_metrics.json`(`docs/monitoring/README.md` §7),
stage 공통 환경 `docs/operations/stage-runbook.md`, 기존 모듈 재실행 규칙 `docs/operations/rerun.md`.
MD
````

- [ ] **Step 10: 문서-코드 대조 + 전체 단위 테스트 통과 (GREEN — 14개)**

문서에 적은 이름을 코드에서 다시 확인한다(모두 1건 이상 나와야 한다):

```bash
grep -c "token-metrics-pipeline" mart/token-metrics/app/steps.py && grep -c "module=mart-metrics" mart/token-metrics/app/mart.py && grep -c "MART_METRICS_MAX_MUTATIONS_PER_RUN" mart/token-metrics/app/config.py && grep -c "CH_DB_TOKEN_DIM" mart/token-metrics/app/ch.py && grep -c "token_mart_absent" mart/token-metrics/app/batch.py && grep -c "chunk-days" mart/token-metrics/tools/rerun.py && grep -c '"--sql"' tools/verify/run_invariants.py && grep -c "\[3/6\]" mart/token-metrics/install.sh
```

기대: 8줄 모두 `1` 이상(0 이 나오면 문서의 해당 이름을 코드 쪽 실제 이름으로 고친다 — 코드가 아니라 문서를 수정). 이어서 전체 단위 테스트:

```bash
cd /home/mini/github/token-data-pipeline/mart/token-metrics && python -m pytest -q tests/test_docs_contract.py 2>&1 | tail -1 && python -m pytest -q 2>&1 | tail -1
```

기대 출력(둘째 줄의 N 은 T1~T10 누적 테스트 수 + 14):

```
14 passed in 0.3s
N passed in …s
```

- [ ] **Step 11: 제로-diff 게이트 + 커밋**

기존 모듈·기존 문서 무수정 확인(출력이 비어야 한다 — `docs/monitoring/README.md` 는 additive 라 목록에 없고 Step 6 의 numstat 삭제 0 으로 확인했다):

```bash
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations/company-verify.md docs/operations/stage-runbook.md docs/operations/rerun.md docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-mart.yml .github/workflows/test-collector.yml && git status --short -- docs/monitoring docs/operations mart/token-metrics/README.md mart/token-metrics/tests/test_docs_contract.py
```

기대 출력(첫 명령은 출력 없음, 둘째는 신규 4 + 수정 1):

```
?? docs/monitoring/grafana_dashboard_token_metrics.json
 M docs/monitoring/README.md
?? docs/operations/token-metrics-deploy.md
?? mart/token-metrics/README.md
?? mart/token-metrics/tests/test_docs_contract.py
```

커밋:

```bash
cd /home/mini/github/token-data-pipeline && git add docs/monitoring/grafana_dashboard_token_metrics.json docs/monitoring/README.md docs/operations/token-metrics-deploy.md mart/token-metrics/README.md mart/token-metrics/tests/test_docs_contract.py && git commit -m "docs(mart-metrics): Grafana token-metrics 대시보드(16패널) + monitoring README §7 + token-metrics-deploy 런북 + 모듈 README (Plan 6c T11)

- docs/monitoring/grafana_dashboard_token_metrics.json: uid token-metrics-stage, 데이터 패널 15 + 텍스트 1(설계 §6.2 항목 전부 — 서비스별 총비용 P0-core/M4 합산, p 파생(기준월·가동률), TTFT/ITL, 출처, 그룹 행 4항), mart 4테이블·fact 2(summary·serving)·레지스트리만 조회, 시간 매크로·cost_label(측정/배분/추정/그룹 귀속/파생) 규칙
- docs/monitoring/README.md §7 (additive — 1~6절 무수정)
- docs/operations/token-metrics-deploy.md: §7.1 순서(dim 4 → 6b → install.sh 프리플라이트 → 첫 배치·마커 → invariants_metrics → 대시보드), §7.5 rerun --chunk-days 7·창 10:50·예산 64, company-verify 격리, 트러블슈팅·롤백 — 사내 주소는 플레이스홀더
- mart/token-metrics/README.md: 읽기/쓰기 테이블, 환경변수 16개(EXPECTED_LATE_SERVICES 없음), 마커·WARN 코드, M0→M0b→M1→M3→M4→M2, 비용 모델 요약
- tests/test_docs_contract.py: 문서 ↔ 코드 계약 14개 (JSON 구조·FROM·컬럼(DDL 대조)·gridPos·변수·설계 §6.2 필수 패널 / README 헤딩 / 배포 문서 절·플레이스홀더·CLI 플래그 실재 / 모듈 README env·마커)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

**Self-Review 노트 (설계 해석 — T11 범위)**

- 설계 해석 1: 패널 3 "당일" = 선택 시간 범위 안의 **최신 집계일**(`date = (SELECT max(date) … WHERE <시간 매크로>)`) — Grafana 는 항상 범위 조회이므로 "오늘" 고정 대신 범위 내 최신일로 해석했다(제목은 아웃라인 고정 문자열 유지).
- 설계 해석 2: 설계 §6.2 는 `grafana_dashboard_token_metrics.json` 의 내용으로 서비스별 총비용(P0-core = Σ M1 by service "배부 미적용" / stretch = M4 합산)·모델별 C 의 serving+standby 분해·그룹 행(ΣC+실험+유휴+미귀속)·토큰 단가 p(기준월·가동률 병기)·TTFT/ITL 추이·출처(manual-v0 vs API) 를 **stretch 표시 없이** 나열하므로(stretch 는 M4·M2 테이블에만 붙어 있다) 전부 패널로 넣었다(리뷰 라운드 1 — 아웃라인의 11패널 고정은 설계보다 우선하지 않는다). p 는 (기준월, service_group, model) 단위 Σ C / Σ W 로, 가동률은 같은 달·같은 그룹의 M2 `Σ reported_gpu_hours_total / Σ allocated_gpu_hours` 를 LEFT JOIN 해 병기한다(할당표 없으면 NULL — `join_use_nulls=0` 이어도 Nullable 컬럼의 기본값은 NULL). serving/standby 분해는 M1 이 C 를 한 컬럼으로만 갖고 있어 `C × serving/(serving+standby)` 비례 분해(기종 혼합 행은 근사 — 패널 설명에 명시). 요청당 원가(`krw_per_request`)는 패널 3 파생 컬럼.
- 설계 해석 3: 라벨 규칙(정의서 §7)은 각 패널의 `cost_label` 컬럼(측정/배분/추정/그룹 귀속)으로 구현 — `token_not_reported` 는 "그룹 귀속" 라벨(설계 §6.4 (4) I8 "서비스 비용 패널에서는 '그룹 귀속' 라벨로 분리 표시").
- 설계 해석 4: 템플릿 변수는 `${var:singlequote}` 포맷 + `includeAll`(allValue 미지정 → 전체 값 전개) 로 `IN (…)` 에 넣는다. 패널 5(p — 모델 단위 C÷W)·11·12(M2 — `service` 컬럼 없음)는 `service_group` 만, 커버리지 패널 15 는 필터 없음(`GROUP_FILTER_PANELS`/`SERVICE_FILTER_PANELS` 로 고정). 커버리지 분모 `expected_services` 는 T5 M0 술어(`enabled = 1 AND coverage_since <= d AND (until IS NULL OR d <= until)`)를 날짜별 CROSS JOIN 으로 계산해 마커 `metrics_coverage` 와 같은 수가 나온다(`enabled = 1` 만 세던 `registered_services` 는 coverage_since 이전·until 이후 날짜에서 마커와 어긋났다).
- 설계 해석 5: `install.sh`·`rerun.py` 명령줄은 아웃라인 T8 옵션(`--overlay`, `--registry`, `--tag`, `-n`, `--chunk-days`, `--force`)에 원형 클론이 갖는 `--context` 를 더해 적었다. 실제 스크립트와 어긋나면 `test_deploy_doc_cli_flags_exist` 가 잡고 **문서를 스크립트에 맞춘다**(Step 8).
- 설계 해석 6: company-verify 에서 운영 토큰 mart 가 없을 때의 "GPU-only 검증"(설계 §7.1)은 `CH_DB_TOKEN_MART/CH_DB_TOKEN_DIM` 을 격리 토큰 mart(`token_verify_mart/token_verify_dim`, company-verify 1단계의 빈 테이블)로 가리켜 프리플라이트를 통과시키고 `WARN token_mart_absent` + M4 스킵으로 진행하는 것으로 해석했다(프리플라이트는 컬럼 존재만 본다).
- 설계 해석 7: 모듈 README 환경변수 표는 config 11 + `CH_DB_*` 5 = 16행(Secret 키 11개는 별도 문장). 아웃라인의 "11 vars" 는 Secret 키 수와 일치.
- 롤백(§7.5)의 DROP 은 mart 4테이블만 명령으로 적고 fact 4·dim 5 는 각 모듈 `ddl/README.md` 목록을 가리킨다 — 6c 문서가 6a/6b 테이블 목록을 복제하지 않기 위해서다.
- 공개 레포 규칙: 문서에 사내 호스트·코드명·이메일 없음(`harbor.example.internal`, `chi-<cluster>.<ns>.svc`, `<ctx>`, `<project>`, `<mart-password>` 플레이스홀더) — `test_deploy_doc_sections_and_placeholders` 가 강제.
- 기존 파일 무수정: `docs/monitoring/README.md` 만 append(Step 6 numstat 삭제 0), 나머지는 신규 파일. 제로-diff 게이트 Step 11.
---

## 완료 기준 (Plan 6c)

- [ ] `mart/token-metrics/` 모듈이 독립 이미지 `token-mart-metrics`로 빌드되고, `cd mart/token-metrics && python -m pytest -q` 전부 통과(T1–T8 단위 테스트: config/ch/preflight/mart/steps/batch/rerun/install 계약).
- [ ] `app/preflight.READ_CONTRACT`가 정확히 3테이블/13컬럼(`token_usage_1d` 9 + `agg_token_service_1d` 2 + `dim_token_service` 2)이고 `install.sh [3/6]`의 bash 배열과 테스트로 동일 확인; 계약 누락 시 batch는 모든 날짜 `FAILURE reason=read_contract`(변이 0), install.sh는 `exit 3`.
- [ ] 단위 테스트가 정의서 §5.1(Qwen3-32B 240,000원 → 76,364/152,727/10,909, 합 보존 I3)·§5.2(p×1e6 ≈ 5,160, 0.1p ≈ 516, 4p ≈ 20,600)·§5.3(idle 0 → 16) 값을 `app.mart` 참조 구현으로 재현.
- [ ] `SQL_M1/M3/M4/M2`: INSERT 컬럼 목록이 Plan 6a DDL 선언 순서와 동일(28/12/14/23), `{d:Date}` 바인딩, `coalesce(` 부재, `created_by='token-metrics-pipeline'`, DB 접두는 `DB_FACT/DB_DIM/DB_MART/DB_TOKEN_MART/DB_TOKEN_DIM` 5종만, EXPECTED_SQL은 같은 키 조각으로 조립(M3는 같은 블록 UNION의 `count()`).
- [ ] M3 블록 20종(핵심 13 + stretch 7) 이름·severity가 DDL COMMENT 목록과 일치; `identity_drift` detail에 reported_* 원문 없음.
- [ ] `batch.py` 실행 순서 M0→M0b→M1→M3→M4→M2, 변이 예산 프리체크(`MART_METRICS_MAX_MUTATIONS_PER_RUN=64`, 날짜당 ≤4)가 첫 `_run_table` 전에 판정, 마커 1실행 1줄 `BATCH_RESULT status=<S> module=mart-metrics metrics_coverage=N/M missing_services="…" rows_mart= rows_check= rows_share= warn= elapsed=[ reason=]`, no-metrics day = SUCCESS + WARN, `token_mart_absent` = M4 스킵(`rows_share=0`), 마커·로그에 user_id/payload 0.
- [ ] CronJob `token-mart-metrics`(`"20 10 * * *"`, `Asia/Seoul`, Forbid, starting/activeDeadline 1800, backoffLimit 1, Secret `token-mart-metrics-ch-secret`, `-verify` overlay, `registry-pull-secret` 부재 시만 생성) — `test-mart-metrics.yml` manifests job grep 전부 통과.
- [ ] `tools/rerun.py`: `--chunk-days 7` 순차 Job, 창 ≥10:50 KST(`--force` 예외), 활성 `token-mart-*` Job 0 검사, `--chain` 없음.
- [ ] `tools/verify/invariants_metrics.sql` 8블록(P0 5 + stretch 3) + `run_invariants.py --sql`(기본 경로 불변) — `cd tools/verify && python -m pytest -q` 기존+신규 전부 통과.
- [ ] E2E(`tests/e2e/run_e2e.sh`)가 CI에서 통과: 단일노드 CH 24.8, 시드 결정적, 배치 2회 멱등(`idempotent_no_dup_*` 0), expect-empty 검증, `invariants_metrics.sql` `ALL INVARIANTS PASS`, no-metrics day `metrics_coverage=0/3` SUCCESS.
- [ ] 문서: `docs/monitoring/grafana_dashboard_token_metrics.json`(uid `token-metrics-stage`, 16패널 — 설계 §6.2 항목 전부, JSON 검증 통과), `docs/monitoring/README.md` §7 추가만, `docs/operations/token-metrics-deploy.md`(플레이스홀더 호스트만), 모듈 README.
- [ ] zero-diff 게이트: `git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations/company-verify.md docs/operations/stage-runbook.md docs/operations/rerun.md docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml` 출력 없음; `assets/model-catalog/` 기존 파일 무수정; 공개 레포 grep(사내 호스트·코드명·이메일) 0.

## 인터페이스 요약

| 산출물 | 이름/서명 | 정의 Task | 소비 Task |
|---|---|---|---|
| DB 상수 | `app.ch.DB_FACT/DB_DIM/DB_MART/DB_TOKEN_MART/DB_TOKEN_DIM`(env `CH_DB_*`, TOKEN_* 기본 = MART/DIM) | T1 | T3–T7, T9(토큰 이름), T10 |
| CHGate | `exists/delete_day/wait_for_mutations/insert_select/verify_count/query/describe` | T1 | T3–T7 |
| Config | `Config(... max_mutations_per_run=64)`, `load_config()` | T1 | T5, T8(Secret 키) |
| 읽기 계약 | `preflight.READ_CONTRACT`(3/13), `missing_columns()`, `contract_tables()` | T1 | T5, T8(install.sh 배열·테스트) |
| 순수 로직 | `mart.compute_coverage/batch_line/target_dates/mutation_budget_exceeded`, `FAIL_FLAGS/W_*`, `weighted_tokens/model_cost/allocate_shared/external_api_cost/group_overhead/quality_flag_m1`, `M1_FLAG_PRIORITY`, `DENOMINATOR_MODES` | T2 | T3(W 상수·FAIL_FLAGS), T5(마커), T6/T7(테스트), T10(기대값) |
| SQL 조각 | `steps.SUB_EFF_ALIAS/SUB_EFF_TCO/SUB_EFF_ALLOC/SUB_EFF_PRICE/SUB_REG/SUB_USAGE_SVC/SUB_ANCHOR`, `canon()`, `FAIL_PRED`, `_WTOK_EXPR`, `_TOK_KEYS/_GPU_KEYS` | T3 | T4, T6, T7, T9(문자열 동일 술어) |
| M1 | `SQL_M1/EXPECTED_SQL_M1`, `run_m1(gate, date) -> {"rows_mart", "warns"}`, `T_M1` | T3 | T5, T6(M1 산출 읽기), T10 |
| `_run_table` | `_run_table(gate, date, dist, local, sql, expected_sql, warns, extra_pred="") -> int`, `StepError` | T3 | T4, T6, T7 |
| M3 | `M3_BLOCKS_CORE`(13), `M3_BLOCKS_STRETCH`(T6 +3, T7 +4), `build_m3_sql/build_m3_expected`, `run_m3(gate, date, blocks=None)` | T4 | T5, T6, T7, T10 |
| batch | `BatchOutcome`, `run_batch(cfg, date, gate=None, *, token_mart_present=None)`, `RUNNERS`, `preflight_or_fail`, `plan_mutations`, `main`, `SQL_M0_*`, `SQL_M0B_TOKEN_MART_ROWS` | T5 | T6/T7(RUNNERS append), T8(ENTRYPOINT), T10 |
| M4 | `SQL_M4/EXPECTED_SQL_M4`, `run_m4 -> {"rows_share", "warns"}`, `T_M4` | T6 | T10, T11 |
| M2 | `SQL_M2/EXPECTED_SQL_M2`, `run_m2 -> {"rows_group", "warns"}`, `T_M2` | T7 | T10, T11 |
| 배포 | CronJob/Secret/overlay 이름, `install.sh [1/6]…[6/6]`, `rerun.py` 상수·함수, `release-images-metrics.yml` 항목 | T8 | T10(CI grep), T11(런북) |
| 불변식 | `invariants_metrics.sql` 8블록, `run_invariants.py --sql` | T9 | T10(E2E 마지막 단계), T11 |
| E2E | `seed_metrics.build_seed`, `mart_expectations.expect`, `ddl_test_dims.sql`, `verify_expected_results.sql` 토큰, `run_e2e.sh`, `test-mart-metrics.yml` | T10 | T11(README 테스트 절) |
| 문서 | 대시보드 uid `token-metrics-stage`, README §7, deploy 런북, 모듈 README | T11 | — |

## Self-Review 노트

### 스펙 커버리지 (설계 절 → Task)

| 설계 절 (행) | 내용 | Task |
|---|---|---|
| §6.1 295-297, 301, 306 | 모듈 클론·Secret/DB 상수 5종·읽기 계약 13컬럼·M0 coverage·M0b 토큰 mart 존재 확인·no-metrics day 규칙 | T1(상수·READ_CONTRACT), T5(M0/M0b) |
| §6.1 299 | 공통 CTE(`eff_alias/eff_tco/eff_alloc/eff_price`)·`canon(x)`·fail_flag·argMax 이력 조회 | T3 |
| §6.1 302 | M1 `agg_token_model_cost_1d` 컬럼·quality_flag 우선순위·EXPECTED | T3 |
| §6.1 304 | M4 `agg_token_model_share_1d`·분모 6모드·provider_ambiguous·external_api 단가 (stretch) | T6 |
| §6.1 303 | M3 `token_metrics_check_1d` 13+7 검사·EXPECTED=count | T4(핵심 13), T6(stretch 3), T7(stretch 4) |
| §6.1 305 | M2 `agg_token_gpu_group_1d`·할당×24·idle 클램프·identity_gap (stretch) | T7 |
| §6.1 306 | 멱등(delete_day→insert_select→verify_count)·2회 실행 검증 | T3(`_run_table`), T10(E2E 2회) |
| §6.1 295 · §7.5 363-369 | 독립 이미지·CronJob 10:20·Secret·-verify·pull-secret | T8(배포) |
| §6.2 308-310 | 대시보드 읽기 계약(공유 계정 `mart`, mart 4 `_dist` + serving fact)·uid `token-metrics-stage` 분리 | T11(대시보드·README §7) |
| §6.1 306 · 마스터 §5.6 | 마커 형식·WARN 코드·로그 비노출 | T2(`batch_line`), T5(오케스트레이션), T4(CHECK WARN) |
| §6.4 316-336 | 비용 모델(C·W 가중·share·external_api·그룹 TCO)·정의서 §5 재현 | T2(참조 구현+테스트), T3/T6/T7(SQL) |
| §4.0 129-130 | 변이 예산 64·날짜당 ≤4·`--chunk-days 7` | T2(`mutation_budget_exceeded`), T5(프리체크), T8(rerun) |
| §7.1 340-342 | `invariants_metrics.sql` 5+3·`created_by_wrong_metrics`(mart 4테이블)·`run_invariants.py --sql` additive·기존 파일 무수정 | T9 |
| §7.3 350-354 | 단위·e2e 테스트 범위(mart `test_{steps,batch}.py`·canon 동일·`coalesce` 부재·2회 멱등)·운영 문서(`token-metrics-deploy.md`·모듈 README·`docs/monitoring/README.md` 절 추가) | T1–T7(단위), T10(E2E), T11(문서) |
| §7.5 361-371 | zero-diff 목록·`registry-pull-secret` 없을 때만·독립 배포 단위·company-verify 선택·롤백(suspend + DROP)·DESCRIBE 프리플라이트·권장 절차 stage→운영→invariants | T8(install.sh/overlay), T10(E2E가 절차 재현), T11(런북) |
| §6.3 312-314 · §4.0 130 · §7.3 354 | 재실행 절차·`--chunk-days 7`·창 ≥10:50·활성 Job 0·SIGTERM·`--force` | T8(rerun.py), T5(SIGTERM), T11(런북) |
| §4.0 117-131 | 변이 대장(mart 4테이블 delete_day)·`distributed_product_mode=global`·insert 설정 | T1(ch.py 설정 테스트), T5(예산), T9(settings) |
| §4.3 196-213 | 레지스트리 컬럼(`enabled/coverage_since/until/expect_gpu/expect_serving/usage_includes_consumers`) | T4(검사 술어), T5(M0), T6(제공자 자체분) |
| §4.1 145, 158 · §5.3 265 | `hours_over_count/unknown_violation/pct_non_monotone` 플래그 의미 | T2(`FAIL_FLAGS`), T4 |
| §4.1 167 · §5.3 265 | `identity_drift` 조건(`metrics-api-v1` + reported_* ≠ 등록부) | T4 |
| 마스터 §5.6 | 로그·마커·detail에 user_id/payload 원문 금지 | T2/T4/T5/T9 테스트 |
| 정의서 §3/§5/§8 | 수식·워크 예시·불변식 I1–I4 | T2, T6, T7, T9, T10 |
| §7.2 344 | 메타데이터 시트 반입(`assets/model-catalog/` 신규 파일·실값 gitignore) | Plan 6a 담당 — 6c는 런북(T11 `token-metrics-deploy.md` §1)에서 절차만 인용, 파일 생성 없음 |
| §7.4 356 | 명시적 보류(P1 뷰·공용 패키지 추출 등) | 없음 — T1이 `mart/token-usage`를 클론(공용 패키지 추출 안 함, 헤더 Architecture) |

### 설계 해석 (설계가 열어둔 지점에서 이 플랜이 고른 것)

1. **`model_registered` = alias 테이블 히트 여부**(`dim_token_model_alias`에 `alias = model` 이력 존재). 설계 §6.1이 "등록 여부는 `model_registered`"라고만 적어 `dim_token_model`(토큰 파이프라인 소유, 6c 읽기 계약 밖) 조회 대신 alias 히트로 고정 — 읽기 계약 13컬럼을 넓히지 않기 위함.
2. **이력 조회에서 최신 행의 NULL은 NULL로 전파**: `nullIf(argMax(ifNull(x, -1), effective_from), -1)`. ClickHouse `argMax`가 NULL arg를 건너뛰어 과거 실값이 되살아나는 문제를 막는다(사내 시드 플레이스홀더 `2026-01-01 NULL` 행이 유일 이력이면 NULL, 이후 실값 행이 있으면 실값 — 설계 D 표 규약과 일치).
3. **CTE 대신 모듈 상수 서브쿼리 문자열(`SUB_*`)**: 설계 §6.1의 `WITH eff_alias …` CTE는 의미 규약으로 두고, INSERT…SELECT와 EXPECTED_SQL이 **같은 문자열 조각**을 공유하도록 조립(파생 오차 0, 테스트로 동일성 단언).
4. **`partial` 판정** = 앵커 존재 AND (`an.gpu_rows` ≠ 실제 gpu 행수 OR `an.serving_rows` ≠ 실제 serving 행수) — 서비스 단위; M3 `partial_load`와 동일 술어.
5. **M4는 같은 배치의 M1 산출(`agg_token_model_cost_1d_dist`)에서 C(m)을 읽는다**(fact 재계산 대신). 실행 순서 M1→M3→M4를 batch가 보장하므로 안전하고, "C(m) = M1 제공자 행의 model_cost_krw"라는 DDL COMMENT와 정확히 일치.
6. **M2 `model_cost_sum_krw`는 fact를 (service_group, gpu_type)로 재집계**(Σ 비FAIL serving+standby 시간 × 그 기종 TCO). M1은 모델 단위라 gpu_type 분해가 없어 M1 합으로는 gpu_type별 정체성(I2)을 만들 수 없다. **설계 §6.4 (1)과의 의도적 편차**: M1은 "기종 하나라도 TCO NULL이면 모델 C NULL"(부분 합 금지)이지만 M2는 기종별로 닫히므로, TCO 결손 기종에 걸친 모델이 있는 그룹에서는 `Σ_gpu_type model_cost_sum_krw ≠ Σ_model M1 C`(M2가 더 크다). 그룹의 모든 기종에 TCO가 있을 때만 두 합이 항등이며, 이 사실을 T7 자체 리뷰 1과 모듈 README "비용 모델 요약"에 적었다. 불변식 `metrics_cost_sum_mismatch`(T9)는 M1 술어로 M1만 재계산하므로 이 편차와 무관하고, `group_identity_gap`은 M2 내부 항등만 본다.
7. **`no_provider` 모드(test-only 모델)의 cost = 0, allocated = 0**, share는 정보용 W(s)/W(m) — 정의서 §3.6 "test는 그룹 귀속, 배분 안 함".
8. **`group_identity_gap` 불변식은 `tco_missing = 0`만 한정, `over_report` 면제 없음**(설계 §7.1 I2 = `abs(identity_gap_krw) > 1` 그대로) — over_report 행은 idle 클램프로 gap이 생겨 `idle_negative`(I1)와 함께 두 번 보고되며, 둘 다 위반이 맞다.
9. **읽기 계약 프리플라이트 이중화**: install.sh `[3/6]`(설치 시) + batch 시작 시 `gate.describe`(런타임) — 토큰 mart가 나중에 스키마를 바꿔도 변이 0으로 실패.
10. **`EXPECTED_LATE_SERVICES` env 없음**(설계 §6.1 M0가 지연 서비스 개념을 6c에 두지 않음; `compute_coverage(expected, anchors, [])`로 호출). CI grep으로 부재 강제.
11. **M4/M2(stretch)는 항상 같은 배치에서 실행**(플래그 env 없음). `token_mart_absent`일 때만 M4 스킵; M2는 GPU-only라 항상 실행. `rows_group`은 마커 필드가 아니므로(Plan 6a H 고정) 로그로만 남김.
12. **M3 stretch 7블록의 소속**: 제공자/단가 관련 3개는 M4 태스크(T6), 할당/블록 부재 관련 4개는 M2 태스크(T7)에서 추가 — 각 stretch 테이블의 서브쿼리를 재사용하기 위함. `serving_block_empty_unexpected`는 `expect_serving=1 AND an.serving_rows=0`, `gpu_block_empty_unexpected`는 `expect_gpu=1 AND an.gpu_rows=0`으로 고정.
13. **M3 `service_not_in_usage_registry`·`manual_source`·`identity_drift` detail은 수·플래그만**(원문 서비스명은 컬럼에, reported_* 원문은 어디에도 없음). `invariants_metrics.sql`의 detail에는 서비스명 나열을 허용(서비스명은 PII 아님; 금지 대상은 user_id/payload).
14. **`test-mart-metrics.yml` 트리거 paths** = `mart/token-metrics/**`, `collectors/token-metrics/ddl/**`, `assets/model-catalog/ddl/**`, `tools/verify/invariants_metrics.sql`, 워크플로 자신 — 기존 모듈 경로는 포함하지 않음(zero-diff 게이트와 정합). `release-images-metrics.yml`은 6b가 만든 파일이면 mart 항목만 추가, 없으면 collectors+mart 2항목으로 신규 생성(6b와의 머지 충돌 시 항목 병합).
15. **rerun `range_deadline_s(n) = min(1800 × ceil(n/7), 7200)`** — 청크 7일 = CronJob activeDeadline 1800과 동일, 그 이상은 상한 2시간.
16. **E2E 포트 18124·컨테이너 `ch-e2e-mart-metrics`** — 기존 token-usage E2E(18123)와 같은 러너에서 병렬 실행돼도 충돌 없음. 토큰 mart 대역은 `mart/token-usage/ddl/company/mart_tables.sql`을 **읽어** 단일노드 변환(파일 무수정).
17. **E2E 시드 값은 fixture TCO(H100 4200)로 산출**(C(Qwen3-32B) = 201,600원). 정의서 §5.1의 240,000원 예제는 T2 단위 테스트가 그대로 재현하고, E2E는 같은 참조 구현(`app.mart`)으로 기대값을 만들어 I3 합 보존을 검증한다.
18. **Grafana 16패널 구성·gridPos**는 설계가 정하지 않아 기존 대시보드 골격을 따라 이 플랜이 고정(uid `token-metrics-stage`, 데이터 15 + 텍스트 1 — 패널 내용은 설계 §6.2 목록 전부, 배치만 플랜 결정).
19. **mart 마커에 `NODATA` 상태 없음** — 설계 §6.1 306은 "메트릭이 없는 날도 SUCCESS(rows 0, `metrics_coverage` WARN)"이고 T5 `batch.py`도 `SUCCESS|FAILURE`만 낸다. 컬렉터(6b)의 `NODATA`는 6b 마커 전용이며, T11 문서(모듈 README·런북)는 mart 마커를 `status=SUCCESS|FAILURE`로만 적는다(조립 시 3곳 정정).
20. **`tools/verify/` 커밋 scope는 `feat(verify)`** — 헤더 규약의 `tools` 대신 기존 히스토리(`fix(verify): identified_name_leak …`) 관례를 따른다(T9). 나머지 10커밋은 `mart-metrics` scope(모듈 코드·E2E·문서). T2는 커밋 2개(1부 마커·예산 / 2부 비용 모델)로 나뉘며 둘 다 `(Plan 6c T2)` 접미를 갖는다.
21. **T11 대시보드 생성기는 레포 밖(`${TMPDIR:-/tmp}/gen_token_metrics_dashboard.py`)에서 1회 실행** — 산출 JSON만 커밋하고 생성기는 커밋하지 않는다(기존 `grafana_dashboard_token_usage.json`도 생성기 없이 JSON만 존재하는 관례). 재생성이 필요하면 플랜의 스크립트를 다시 저장해 실행한다.
22. **§7.2(시트 반입)·§7.4(보류 항목)는 6c 범위 밖** — §7.2는 Plan 6a가 파일을 만들고 6c는 런북에서 절차만 인용, §7.4는 어떤 Task도 착수하지 않는다(공용 패키지 추출 없이 클론 — 헤더 Architecture).

### 조립 검증 (assembler self-review — 2026-09-05)

**스펙 커버리지 표 검증.** 위 표의 모든 행을 설계 v0.5.1(`docs/superpowers/specs/2026-08-31-token-metrics-ingest-design.md`) 실제 행 번호와 대조해 정정했다: §7.1(340-342 = 불변식) ↔ §7.3(350-354 = 테스트·운영 문서) 뒤바뀜 교정(T9·T10·T11 본문 인용 포함), §6.1 세부 행(295 CronJob/Secret · 297 읽기 계약 · 299 CTE · 301 M0/M0b · 302 M1 · 303 M3 · 304 M4 · 305 M2 · 306 마커/no-metrics day)에 맞춰 T1/T2/T3/T5/T6/T7/T8 본문의 `§6.1 NNN` 인용 정정, 변이 예산 근거를 `§6.1 295`→`§4.0 129`로, 플래그 의미·`identity_drift`를 `§4.2/§5.4`→`§4.1 145,158,167 · §5.3 265`로, `§7.5 371`→`370`(DESCRIBE 프리플라이트)으로 정정. 재실행(§6.3 312-314 · §4.0 130 · §7.3 354)·§7.2·§7.4 행을 추가해 설계 §4.0–§7.5 전 절이 Task 또는 "범위 밖" 판정으로 매핑된다.

**플레이스홀더 스캔.** 오케스트레이터가 지정한 미완성 표식 6종(미정 약어 2종·"나중에 구현"·"Task N과 유사"·"적절히"·"엣지 케이스"의 영문 표현 — 대소문자 무시 grep)에 대한 검색 → 0건(이 문장이 스스로 히트하지 않도록 영문 원어는 적지 않음). 공개 레포 경계: 호스트는 `harbor.example.internal`(11)·`chi-<cluster>.<ns>.svc`(8)·`ch.internal`(테스트용 가짜 호스트 2)만, 이메일은 커밋 트레일러 `noreply@anthropic.com`만, IP는 E2E `127.0.0.1`만, 사내 코드명 0건. 레포 작업 트리 변경 0(`git status --short`에 플랜 파일 3종 `??`만 — Plan 6a/6b/6c).

**타입·이름 일관성 — 조립 시 교정 목록.**
- T6 M0b 라우팅: 토큰 mart 존재 확인이 `agg_token_service_1d_dist`(READ_CONTRACT 테이블)를 보도록 정정; T6 hunk 3 본문·앵커를 T5 실제 코드(`result = fn(gate, date)` / `int(result[key])`)에 맞춤(`r = fn(...)` 잔재 제거), RUNNERS 주석·T5 주석 문자열 일치.
- T2 누계 테스트 수 `48 passed`(T1 27 + T2 21)로 정정; 완료 기준의 mart 컬럼 수 `28/12/15/24`→`28/12/14/23`(Plan 6a DDL: M1 28·M3 12·M4 14·M2 23).
- T11 문서·모듈 README의 마커 `status=SUCCESS|NODATA|FAILURE`→`SUCCESS|FAILURE`(3곳, 해석 19).
- 헤더 File Structure에 `tests/test_e2e_seed.py`(T10)·`tests/test_docs_contract.py`(T11) 추가; 커밋 scope 목록에 `verify` 추가(해석 20); T11 생성기 저장 경로를 세션 임시 디렉터리에서 `${TMPDIR:-/tmp}`로(해석 21).
- 교차 확인(수정 불필요): READ_CONTRACT 13컬럼(T1 preflight ↔ T8 install.sh 배열 ↔ T8 테스트 파서), Secret 11키 ↔ T1 `Config` getenv 이름, CronJob `token-mart-metrics`/`"20 10 * * *"`/`startingDeadlineSeconds 1800`, rerun 상수·플래그(`--context --from --to -n --cronjob --chunk-days --force`) ↔ T11 런북, WARN 코드 4종·`missing_services="-"` 관례 ↔ T2/T5/T11, T6/T7가 쓰는 T3/T4 식별자(`SUB_*`, `canon`, `_run_table`, `_m3_select`, `M3_BLOCKS`, `build_m3_sql/expected`) 정의 존재, Plan 6a DDL/fixture 경로(`collectors/token-metrics/ddl/**`, `assets/model-catalog/ddl/**`, `tests/fixtures/**`) 일치, 12커밋 전부 `type(scope): 한국어 (Plan 6c Tn)` 형식·Tn 일치.

**시뮬레이션(스크래치 디렉터리에서 실행, 레포 무변경).** (1) T9 `run_invariants.py` 패치 스크립트를 원본 사본에 적용 → `APPLIED 5 169`, AST OK, `git diff --numstat` +19/-5(빈 줄 1 포함, `--stat` 24). (2) T5→T6→T7 `batch.py` 패치 체인 → T7 스크립트 출력 `batch.py patched: import, RUNNERS(4), rows.update, log.info`, AST OK, 최종 import `from app.steps import MART_TABLES, StepError, run_m1, run_m2, run_m3, run_m4`. (3) T11 생성기 → 16패널(데이터 15 + 텍스트 1)·uid `token-metrics-stage`·schemaVersion 41·Asia/Seoul; 계약 테스트 `test_docs_contract.py` 14개를 생성 JSON + README §7 사본 + 런북·모듈 README heredoc + T1 `config.py/ch.py`·T8 `install.sh/rerun.py`·T9 `run_invariants.py` 시뮬 사본 + Plan 6a DDL 사본으로 실행 → `14 passed`(Step 4 `-k` = `10 passed, 4 deselected`, Step 6 = `1 passed, 13 deselected`·`git diff --no-index --numstat` = `52 0`·헤딩 7, Step 8 = 절 10·`harbor.example.internal`만·이메일 0·`2 passed, 12 deselected`). (4) T2 `app/mart.py`(heredoc 2부 결합, 332줄) + T1 `config.py/ch.py` + T10 `seed_metrics.py/mart_expectations.py/ddl_test_dims.sql`로 `pytest tests/test_e2e_seed.py` → `12 passed`(T10 기대값과 일치 — C(Qwen3-32B)=201,600·H100 idle 72·gap 0·I3 합 보존이 참조 구현과 정합).

**기계 검사(최종 — 리뷰 라운드 1 반영 후).** `wc -l` = 11769; `### Task` 11개(T1 98 … T11 10137); 펜스 블록 227개(bash 136·python 35·text 17·yaml 9·sql 2·dockerfile 1·무언어 27) 중 python `ast.parse`·yaml `safe_load` 실패 0(json 블록 없음 — 대시보드 JSON은 생성기가 만든다); heredoc 18개(py 13·sql 2·sh 1·기타 2) 파싱 실패 0; 미완성 표식 6종 grep 0건.

### 리뷰 라운드 1 (2026-09-06)

리뷰 라운드 1: 42건 반영, 2건 기각(사유: `model_registered` — Plan 6a 1157행 DDL COMMENT 가 이미 "alias 테이블 히트" 의미로 6c 와 일치; `share_sum_mismatch` — 설계 §7.1 342행이 `provider_reported` 를 I3 대상 분모 모드 집합에 명시하므로 불변식이 M3 `consumer_tokens_exceed_provider` 와 함께 두 번 보고하는 것이 설계대로).

44건은 4개 렌즈(coverage 11·placeholders 12·consistency 12·adversarial 9)에 걸쳐 같은 결함이 중복 보고된 것이 많아, 결함 단위로는 다음과 같이 고쳤다(각 항목의 근거는 해당 Task 본문·footer 해석에 적었다).
- **T3/T4 (M1 `partial` ↔ 앵커 `serving_rows`)**: `SUB_SERVING_CNT`·`_M3_CHILD_COUNTS` 를 `countIf(metric != 'custom')` 로 — 앵커가 세지 않는 custom 행을 실제 행수에서도 제외(T3 테스트 단언 추가).
- **T4 블록 9 `serving_missing_for_gpu_model`**: `token_usage_1d.model` 도 `canon()` 서브쿼리로 정규화한 뒤 gpu 쪽 canonical 과 비교. `pct_non_monotone` 는 설계 §4.1 158·§5.3 265 대로 FAIL.
- **T4 테스트 5개 (blocker)**: `DATE` 상수를 T3 `tests/test_steps.py` 에 정의(T4 가 재사용); `SQL_M3_SUMMARY` 는 단일 테이블이라 T3 `test_global_join_and_global_in_only` 스윕에서 `_SUMMARY` 접미로 제외; `gate.order` 단언을 `(op, key)` 튜플 목록으로; EXPECTED 는 `run_m3` 기본값과 같은 `M3_BLOCKS_CORE + M3_BLOCKS_STRETCH` 로 고정.
- **T5**: Step 2 RED 기대 예외를 `ImportError`(`cannot import name`) 로. **T6**: `_M4_VENDOR` 의 `min(provider) AS provider` 자기참조를 `AS vendor` 로 분리(argMin 키는 원 컬럼); Step 8 삭제 대상 함수 본문 줄수 정정; 커밋·Self-Review 의 `dim_token_metrics_model_price` → 실제 이름 `dim_token_vendor_price`. **T7**: M2 `model_cost_sum_krw` 가 fact 재집계(§6.1 305 문구와의 차이·이유) 를 본문에 명시.
- **T8**: build.sh usage 6줄, install.sh 델타 표의 `--target-db` 행(원형에 없음) 정정, rerun.py 예산 인용 §6.4 → §4.0 129-130. **T9**: `group_identity_gap` 에서 `over_report = 0` 제거(설계 §7.1 I2 그대로), `--stat` 24(+19/-5), 원형 인용 `invariants.sql:1-189`. **T1**: `distributed_product_mode` 인용 100-101행, Step 13 `git status` 기대 문구.
- **T10**: DDL 로더를 Plan 6b `split_statements`(따옴표 인식) 로 통일; `uniq -c` 기대 `3 invariants_metrics.sql`; 매니페스트 잡의 negated grep 을 `^  name: token-mart-metrics$`(metadata.name 만) + Secret 이름 grep 으로; Step 1·T11 Step 2 에 Plan 6a/6b 산출물 존재 가드.
- **T11 (설계 §6.2 커버리지)**: 11패널 → 16패널 — 서비스별 총비용(M1 Σ, `측정 (배부 미적용)` 라벨) + M4 합산(stretch, `추정/배분` 라벨), 모델별 C 의 serving/standby 비례 분해, 토큰 단가 p(기준월·가동률 병기, `파생` 라벨), TTFT/ITL 추이(serving fact), 출처(manual-v0 vs API), 그룹 행에 `test_cost_krw` 추가; 커버리지 분모를 마커 `metrics_coverage` 와 같은 술어(`enabled=1 AND coverage_since<=d AND (until IS NULL OR d<=until)`, 날짜 상관) 로. 런북: `[6/6]` 은 `kubectl set image` 만(`CH_HOST` 는 Secret 키), §5 기대 출력에 `sql=` 접미, §6.3 재실행 순서(토큰 mart backfill 완료 후 mart-metrics rerun) 추가, `over_report=1` 표기.
