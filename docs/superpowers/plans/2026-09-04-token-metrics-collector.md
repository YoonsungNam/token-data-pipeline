# Token Metrics Collector (Plan 6b/6) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `/v1/metrics` 반입의 **수집 계층**을 신규 모듈 `collectors/token-metrics/`로 완성한다 — Plan 6a가 고정한 fact 4테이블(`raw_token_metrics_{gpu,serving,summary}_1d` + `collect_audit_metrics_1d`)과 레지스트리 `gpu_data.dim_token_metrics_service`를 채우는 시간별 CronJob(`token-metrics-collector`, 02:05~09:05 KST 8슬롯, `module=token-metrics` 마커 `slot=HH final=0|1`), 3계층 정규화(구조 거부 / 의미 플래그 / 응답 WARN), 크래시 안전 적재 시퀀스(존재확인 3종 → 감사 → DELETE summary→gpu→serving → INSERT gpu→serving→summary), 레지스트리 diff-sync, 수기(manual-v0) CSV 로더(`--manual-gpu/--manual-serving/--manual-engine`), 배포 계층(Dockerfile·build.sh·k8s CronJob·overlays·install.sh 프리플라이트), 운영 도구(`tools/rerun.py --chunk-days/--chain-mart`, `tools/manual_load.py`), mock-provider `GET /v1/metrics` + 계약 벤더링(@6a552d2), 결정적 E2E + CI(`test-collector-metrics.yml`, `release-images-metrics.yml`), 모듈 README.

**Architecture:** `collectors/token-usage`를 **클론**한 독립 모듈(설계 §5.1 — 기존 모듈에서 import 0, 공용 패키지 추출 없음). `app/config.py`(env + endpoints.yaml 로더 §4.3 필드) · `app/events.py`(Event enum + CollectError) · `app/api_client.py`(단건 `GET /v1/metrics?date=` + HTTP→Event 번역 + 재시도 3회) · `app/normalize.py`(순수 함수 — `MetricsPayload` → `NormalizeResult`, API/CSV 공통) · `app/writer.py`(`MetricsWriter`: 존재확인 3종·감사·DELETE/INSERT 순서·`_delete_day_in` 배칭·레지스트리 diff-sync·뮤테이션 가드) · `app/manual.py`(CSV 파서 → `MetricsPayload`) · `app/main.py`(모드×게이트 매트릭스·409 재방문·소프트 데드라인·마커·SIGTERM). 적재는 `_dist`로 INSERT(`insert_distributed_sync=1`, `insert_deduplicate=0`), 삭제는 `_local` ON CLUSTER(`mutations_sync=2`). DB명은 모듈 상수 `DB_FACT`/`DB_DIM` 2종만. VM push 없음.

**Tech Stack:** Python 3.10+ 표준 라이브러리 + `requests>=2.31,<3` + `pyyaml>=6,<7` + `clickhouse-connect>=0.7,<1` + `pytest>=8`; FastAPI mock-provider(기존 앱에 additive); bash(install/build/e2e); kustomize; GitHub Actions(clickhouse-server 24.8 컨테이너 E2E + mock 이미지).

**Spec:**
- 설계(자매 스펙): `docs/superpowers/specs/2026-08-31-token-metrics-ingest-design.md`(436행) — §3(58-78) 전제 표(11·12·13행 마커/rerun/매니페스트), §4.0(82-130) 물리·매니페스트·**뮤테이션 장부(119-128)**, §4.3(196-229) 레지스트리·endpoints.yaml, **§5.1(233-237) 토폴로지**, **§5.2(239-258) 스케줄·모드×게이트 표·마커**, **§5.3(260-266) 3계층 정규화**, **§5.4(268-274) 적재 시퀀스·배칭**, **§5.5(276-280) manual-v0 로더·manual_load.py**, **§5.6(282-289) 배포·rerun·CI**, §6.3(312-314) rerun 체인, §7.3(350-354) mock·테스트, §7.5(361-370) 배포 전략·zero-diff 목록, §10(415-436) 작업 컨벤션.
- 스키마 정본: `docs/superpowers/plans/2026-09-04-token-metrics-schema.md`(Plan 6a) — "6b/6c가 소비하는 인터페이스"(4950-5048): A fact 4테이블 컬럼 순서 / B 레지스트리 12컬럼 + endpoints 키 / F manual-v0 템플릿 헤더·파서 규칙 / G `.gitignore` 패턴 / H 공유 도구 등록 상태. DDL 원본: `collectors/token-metrics/ddl/company/{raw_token_metrics,dim_token_metrics_service,accounts}.sql`(Plan 6a T3~T5 산출).
- API 계약: `token-metric-api.yaml` @6a552d2(스크래치 클론 `token-metric-api-spec/`; 벤더링 대상 `token-metric-api.yaml` 490행 + `scripts/check_metrics_api.py` 569행).
- 마스터 스펙 `docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md` v1.14(Plan 6a T11이 개정 — **이 플랜은 마스터 스펙을 수정하지 않는다**).
- **전제**: Plan 6a 산출물(DDL 3파일·템플릿 CSV 3개·`synthetic_endpoints_metrics.yaml`·`.gitignore` 14~26행·`gen_*_ddl.py` 등록)이 브랜치에 병합돼 있다. 부재 시 해당 태스크 첫 단계에서 `ls`로 확인하고 중단·보고한다(6a 파일을 대신 만들지 않는다).

## Global Constraints

- **Zero-diff(설계 §7.5 — 절대 편집 금지)**: `collectors/token-usage/**`, `mart/token-usage/**`, `assets/user-org/**`, `assets/model-catalog/`의 기존 파일, `tools/verify/invariants.sql`, `docs/operations/{company-verify,stage-runbook,rerun}.md`, `docs/monitoring/grafana_dashboard_token_usage.json`, `.github/workflows/{release-images,test-collector,test-mart}.yml`. 태스크 종료 시 `git diff --stat -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml`이 비어 있어야 한다.
- **허용된 additive 편집만**: `tools/mock-provider/**`(새 엔드포인트·datagen 함수·시나리오 필드·contract 파일 추가 — 기존 `/v1/usage`·`/v1/usage/summary` 응답 바이트 불변, 기존 테스트 무수정 통과), `.github/workflows/test-mock-provider.yml`(metrics conformance step 추가), `.gitignore`(2행 append: `collectors/token-metrics/tests/e2e/endpoints.e2e.yaml`, `collectors/token-metrics/tests/e2e/.tmp/`), 신규 워크플로 `test-collector-metrics.yml`·`release-images-metrics.yml`(신규 파일). 이 플랜은 `tools/gen_stage_ddl.py`·`tools/gen_verify_ddl.py`·`test-assets.yml`·`test-tools.yml`·`tools/verify/**`·`docs/monitoring/README.md`·마스터 스펙·`mart/**`를 건드리지 않는다(각각 6a·6c 담당). `release-images-metrics.yml`의 matrix는 **`collectors/token-metrics` 1항목만**(mart 항목은 6c가 추가).
- **공개 레포 경계**: 사내 호스트/주소 금지(플레이스홀더 `harbor.example.internal`, `chi-<cluster>.<ns>.svc`, `token-mock-provider-a.monitoring.svc:8000`), 사내 프로젝트 코드명 금지, 소유자 이메일 금지, 실데이터 파일 금지(`endpoints-metrics.company.yaml`·`*manual_metrics*.csv`는 gitignore — 커밋되는 합성 파일은 `endpoints.yaml`·`synthetic_endpoints_metrics.yaml`·`token_metrics_manual_v0_*.csv` 이름만).
- **이름은 설계·Plan 6a 그대로**: 테이블 `fact.raw_token_metrics_gpu_1d`·`fact.raw_token_metrics_serving_1d`·`fact.raw_token_metrics_summary_1d`(앵커)·`fact.collect_audit_metrics_1d`·`gpu_data.dim_token_metrics_service`(`_local`/`_dist`; INSERT는 컬럼 목록 명시), CronJob/컨테이너/이미지 `token-metrics-collector`, Secret `token-metrics-ch-secret`, ConfigMap `token-metrics-endpoints`, 선택 ConfigMap `token-metrics-ca-bundle`(마운트 `/etc/token-metrics-ca/ca-bundle.pem`), `imagePullSecrets: registry-pull-secret`(**없을 때만 생성**), company-verify 접미 `-verify`, `source_type ∈ metrics-api-v1 | manual-v0`, 마커 `module=token-metrics`. 모호한 항목은 하나로 정해 footer "설계 해석"에 기록한다.
- **CronJob 계약 수치(임의 변경 금지; manifests contract-lock 테스트가 grep)**: `schedule: "5 2-9 * * *"`, `timeZone: Asia/Seoul`, `concurrencyPolicy: Forbid`, `startingDeadlineSeconds: 540`, `backoffLimit: 0`, `restartPolicy: Never`, `activeDeadlineSeconds: 3000`, `successfulJobsHistoryLimit: 3`, `failedJobsHistoryLimit: 3`, resources requests 100m/256Mi · limits 1/1Gi. env: `CH_HOST`(install.sh `set env`), `CH_PORT=8123`, `CH_USER`/`CH_PASSWORD`/`CH_CLUSTER`/`CH_DB_FACT`/`CH_DB_DIM`(secretKeyRef `token-metrics-ch-secret`), `ENDPOINTS_FILE=/etc/token-metrics/endpoints.yaml`, `SOFT_DEADLINE_MINUTES=40`, `LOAD_BUDGET_S=1200`, `FINAL_HOUR_KST=9`, `MAX_RESPONSE_BYTES=5000000`, `METRICS_MAX_MUTATIONS_PER_RUN=45`, `COLLECTOR_HTTPS_PROXY`/`COLLECTOR_HTTP_PROXY`/`COLLECTOR_API_CA_BUNDLE`(선택, secretKeyRef optional). 불변식 `SOFT_DEADLINE_MINUTES×60 > LOAD_BUDGET_S`(2400 > 1200)를 `test_config.py`가 고정.
- **DB 상수 2종만**(`app/writer.py` 모듈 로드 시 1회): `DB_FACT = os.getenv("CH_DB_FACT", "fact")`, `DB_DIM = os.getenv("CH_DB_DIM", "gpu_data")`. 모든 테이블 참조는 `f"{DB_FACT}.<table>_dist"`(SELECT/INSERT) · `f"{DB_FACT}.<table>_local"`(ALTER DELETE)뿐. company-verify는 env로 `token_verify_fact`/`token_verify_dim`.
- **적재 시퀀스·뮤테이션(§5.4)**: fetch·normalize·예산 가드 이후 (1) 존재 SELECT 3종(summary/gpu/serving `_dist`, `WHERE date = %(d)s AND service IN %(ss)s`) → (2) 하나라도 있으면 앵커(summary) 존재 시에만 감사 INSERT(`collect_audit_metrics_1d_dist`, `prev_source_type` 포함) → DELETE 순서 **summary → gpu → serving**(`_local`, `ON CLUSTER` when `cfg.ch_cluster`, `settings={"mutations_sync": 2}`) → (3) INSERT 순서 **gpu → serving → summary 마지막**(`insert_distributed_sync=1`, `insert_deduplicate=0`). 정기 실행은 앵커 존재 시 스킵 → 뮤테이션 0. `--replace`는 (A) 전 서비스 fetch/normalize → (B) 테이블당 `_delete_day_in(table_local, date, services)` 1회(=날짜당 ≤3) → (C) 서비스별 INSERT. 가드: 예정 DELETE 수 > `METRICS_MAX_MUTATIONS_PER_RUN` → 적재 없이 `BATCH_RESULT status=FAILURE … reason=mutation_budget` exit 1. 감사 테이블은 append-only(DELETE 금지).
- **레지스트리 동기화(§4.3)**: 정기 실행에서만(rerun·manual 모드는 읽기만). endpoints 집합 vs `dim_token_metrics_service_dist` 현재 행을 `updated_at` 제외 11컬럼으로 비교 → 다를 때만 `ALTER TABLE … _local DELETE WHERE 1`(현재 집합이 비면 생략) + INSERT(전 행, 컬럼 목록 명시). 동기화 실패는 WARN(`CHECK WARN service=- registry_sync_failed=1`)이며 수집 계속.
- **마커(§5.2 — 정확한 키 순서, 공백 구분, 값에 공백 금지)**: 서비스당 1줄 `SERVICE_RESULT status=<SUCCESS|NODATA|SKIPPED|FAILURE> module=token-metrics service=<정본> source_type=<metrics-api-v1|manual-v0> rows=<gpu+serving+custom> pages=1 warn=<n> rejected=<n>[ reason=<r>]`; 실행당 1줄 `BATCH_RESULT status=<SUCCESS|NODATA|FAILURE> module=token-metrics services_ok=<n> services_failed=<n> services_skipped=<n> rows=<n> elapsed=<n>s slot=<HH> final=<0|1>[ reason=<r>]`(elapsed는 기존 모듈과 같은 정수초+`s`); 인라인 검증 `CHECK WARN service=<svc> <code>=<count>`; SIGTERM 시 캐시된 BATCH_RESULT 줄 + ` note=sigterm` 재출력. 로그에 페이로드·행 원문 금지(카운트·이름·코드만). reason 어휘: `disabled | before_since | after_until | already_loaded | not_ready | not_ready_at_0900 | retention | retryable | permanent_error | mutation_budget | load_budget | deadline | unknown_service | invariant_broken | unexpected:<ExceptionType>`(`retryable`·`permanent_error`는 T4 번역표의 `Event.value`, `deadline`·`unexpected:<Type>`은 T6 run loop — README 모드와 게이트 표·인터페이스 B와 동일 목록).
- **Python 3.10+**: `from __future__ import annotations`, StrEnum/tomllib/match/`datetime.UTC` 금지, `random` 금지(mock은 sha256 `_det_int`), aware KST datetime만(`KST = timezone(timedelta(hours=9))`), 테스트는 `cd collectors/token-metrics && python3 -m pytest -q`(루트 `conftest.py` + `tests/__init__.py`), mock은 `cd tools/mock-provider && python3 -m pytest -q`. **개발 머신에는 `python` 바이너리가 없다** — 이 플랜의 모든 실행(Run:) 커맨드는 `python3`; `python`은 컨테이너 안(Dockerfile `CMD`, T9/T10이 만드는 Job command), CI 워크플로(`setup-python` 러너), README 사용 예시에만 쓴다. 로컬에 docker 없음(CI가 E2E). 공유 클러스터 kubectl 변형 금지.
- **커밋 관례**: `type(scope): 한국어 설명 (Plan 6b Tn)` — scope는 `collectors-metrics`/`mock`/`tools`/`ci`/`docs`; 트레일러 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` + `Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54`. 태스크당 1커밋 이상, 커밋 전 `git diff --stat`으로 zero-diff 목록 무변경 확인.

## File Structure

```
collectors/token-metrics/                        # 신규 모듈 (Plan 6a가 ddl/ 만 먼저 생성)
├── README.md                                    # T12 — 모듈 개요·모드×게이트·마커·manual 절차·rerun 창
├── Dockerfile                                   # T8 — python:3.12-slim, app/ 복사, CMD ["python","-m","app.main"]
├── build.sh                                     # T8 — IMAGE_NAME="token-metrics-collector"
├── install.sh                                   # T8 — <stage|company|company-verify> 프리플라이트·Secret·ConfigMap·DDL·kustomize·set image/env
├── requirements.txt                             # T2 — requests, pyyaml, clickhouse-connect
├── requirements-dev.txt                         # T2 — pytest
├── conftest.py                                  # T2 — sys.path 루트 고정
├── endpoints.yaml                               # T2 — 합성 기본 파일(§4.3 예시 그대로, Mock Service A)
├── app/
│   ├── __init__.py                              # T2
│   ├── config.py                                # T2 — Config(env) · ServiceEntry(§4.3 — dim 12컬럼 중 updated_at 제외 11필드) · load_endpoints()
│   ├── events.py                                # T2 — Event(str, Enum) 6종 · CollectError
│   ├── api_client.py                            # T4 — fetch_metrics(entry, date, cfg, session) → MetricsPayload · _translate_error · _get_with_retry
│   ├── normalize.py                             # T3 — MetricsPayload · GpuRow · ServingRow · NormalizeResult · normalize_payload()
│   ├── writer.py                                # T5 — DB_FACT/DB_DIM · MetricsWriter(exists/audit/delete/insert/registry sync/guard)
│   ├── manual.py                                # T7 — load_manual_csvs() → {(date, service): MetricsPayload}
│   └── main.py                                  # T6(+T7 manual 분기) — run()/main() · 모드×게이트 · 마커 · SIGTERM
├── tools/
│   ├── rerun.py                                 # T9 — CRONJOB="token-metrics-collector" · --chunk-days · --chain-mart · 창 검사
│   └── manual_load.py                           # T10 — ConfigMap token-metrics-manual-<ts> → Job(/manual) → 로그 → 삭제
├── k8s/
│   ├── base/
│   │   ├── cronjob.yaml                         # T8 — §5.2 수치 그대로
│   │   └── kustomization.yaml                   # T8
│   └── overlays/
│       ├── stage/kustomization.yaml             # T8 — images newName ghcr.io/yoonsungnam/token-metrics-collector
│       ├── company/kustomization.yaml           # T8 — resources만
│       └── company-verify/kustomization.yaml    # T8 — nameSuffix: -verify + secretRef/configMap JSON 패치
├── ddl/                                         # Plan 6a 산출(무수정): README.md, company/, stage/, company-verify/
└── tests/
    ├── __init__.py                              # T2
    ├── test_config.py                           # T2 — SOFT×60 > LOAD_BUDGET, endpoints 로더 11필드·기본값·오류
    ├── test_events.py                           # T2
    ├── test_api_client.py                       # T4 — FakeSession 스크립트: 200/400/404/409/429/5xx/네트워크/5MB/date 에코
    ├── test_normalize.py                        # T3 — §5.3 전 규칙 + 케이스 A/D/E/F
    ├── test_writer.py                           # T5 — FakeCH: 존재확인 3종·DELETE/INSERT 순서·_delete_day_in·감사·가드·registry diff
    ├── test_main.py                             # T6 — 모드×게이트 매트릭스·409 재방문·final 슬롯·마커·SIGTERM·데드라인 (+T7 manual 모드 main() 8개 append)
    ├── test_manual.py                           # T7 — CSV 파서·'#' 주석·헤더 바이트 일치·MetricsPayload 조립(manual 모드 main() 테스트는 test_main.py 에 append)
    ├── test_rerun.py                            # T9 — importlib 로드·CRONJOB 상수·chunk 분할·chain-mart 커맨드·창 검사
    ├── test_manual_load.py                      # T10 — ConfigMap 본문·Job spec(/manual 볼륨·command)·삭제 보장
    ├── test_manifests.py                        # T8 — contract-lock grep(base + overlays 렌더 문자열)
    └── e2e/
        ├── run_e2e.sh                           # T11 — CH 24.8 + mock:e2e, DDL 2파일 변환, twin dim, 수집기 2회, 시나리오, manual 1회
        ├── ci_expectations.py                   # T11 — datagen.build_metrics 기반 기대치 출력(key=value)
        ├── verify_expected_results.sql          # T11 — expect-empty 검증(플레이스홀더 {DATE}/{SERVICE}/{EXP_*})
        ├── ddl_test_dims.sql                    # T11 — gpu_data.dim_token_service_dist 최소 twin(MergeTree)
        └── manual_e2e/                          # T11 — E2E 전용 합성 CSV 3개(파일명은 gitignore 패턴 밖: e2e_manual_v0_{gpu,serving,engine}.csv)

tools/mock-provider/                             # additive (T1)
├── app/config.py                                # + metrics_retention_days: int = 14 (env MOCK_METRICS_RETENTION_DAYS)
├── app/scenarios.py                             # + 6 int 필드(metrics_*) + _SCENARIO_RULES 항목
├── app/datagen.py                               # + build_metrics(cfg, date, scn) · _pct() · METRICS_ENGINE · METRICS_GPU_TYPE
├── app/main.py                                  # + GET /v1/metrics (_date_gate 재사용, retention=14, 시나리오 분기)
├── tests/test_metrics_api.py                    # 신규
├── contract/token-metric-api.yaml               # 벤더링 @6a552d2 (그대로 복사)
├── contract/tests/check_metrics_api.py          # 벤더링 @6a552d2 (그대로 복사, 수정 금지)
├── contract/SOURCE.md                           # + metrics 계약 pin 절
├── run_conformance.sh                           # + metrics 단계(같은 uvicorn 프로세스, check_metrics_api.py 실행)
└── README.md                                    # + /v1/metrics·시나리오 절

.github/workflows/
├── test-collector-metrics.yml                   # T11 신규 — unit / e2e / image / manifests
├── release-images-metrics.yml                   # T11 신규 — matrix: collectors/token-metrics 1항목
└── test-mock-provider.yml                       # T1 additive — metrics conformance 실행 + image smoke curl /v1/metrics

.gitignore                                       # T11 — +2행 collectors/token-metrics/tests/e2e/endpoints.e2e.yaml · collectors/token-metrics/tests/e2e/.tmp/
```

---

### Task 1: mock-provider GET /v1/metrics + datagen.build_metrics + 시나리오 6종 + 계약 벤더링(@6a552d2) + run_conformance additive

수집기(T4)·E2E(T11)가 붙을 데이터 소스를 먼저 만든다. 기존 `tools/mock-provider` 앱에 **additive**로 `GET /v1/metrics`(token-metric-api @6a552d2)를 추가하고, 결정적 생성기 `datagen.build_metrics`, `/v1/metrics` 전용 시나리오 int 플래그 6종, 계약 파일 2개 벤더링, `run_conformance.sh` metrics 단계, CI image smoke 1줄을 넣는다. 기존 `/v1/usage`·`/v1/usage/summary`의 응답 바이트·오류 메시지는 불변이며 기존 테스트 40개는 무수정 통과해야 한다(설계 §7.3 350-351행, §5.2 409/404/400 행).

**Files:**
- Modify (additive): `tools/mock-provider/app/config.py`(20행 뒤 필드 1개, 34행 뒤 env 파싱 1행, 39행 뒤 검증 2행), `tools/mock-provider/app/scenarios.py`(15행 뒤 6필드 append), `tools/mock-provider/app/datagen.py`(10행 뒤 import 1행, 100행 뒤 `METRICS_ENGINE`·`METRICS_GPU_TYPE`·`_pct`·`build_metrics` append), `tools/mock-provider/app/main.py`(12행 import, 55-73행 `_date_gate` 시그니처·메시지, 144행(`get_usage_summary` 끝) 뒤 `get_metrics` 엔드포인트, 156행 뒤 `_SCENARIO_RULES` 6항목), `tools/mock-provider/run_conformance.sh`(20-21행 뒤 metrics 단계), `tools/mock-provider/contract/SOURCE.md`(끝에 절 추가), `tools/mock-provider/README.md`(소개·실행·env 표·시나리오·검증 절 additive), `.github/workflows/test-mock-provider.yml`(46행 뒤 smoke curl 2행)
- Create: `tools/mock-provider/contract/token-metric-api.yaml`(490행, 스크래치 클론 루트 `token-metric-api.yaml` 바이트 복사), `tools/mock-provider/contract/tests/check_metrics_api.py`(569행, 스크래치 클론 `scripts/check_metrics_api.py` 바이트 복사, `chmod +x`), `tools/mock-provider/tests/test_metrics_api.py`
- Test: `tools/mock-provider/tests/test_metrics_api.py`(18개 신규); 기존 `tests/test_api.py`·`tests/test_datagen.py`·`tests/test_config.py`·`tests/test_scenarios.py`·`tests/test_cursors.py` 40개 무수정 통과(합계 58)

**Interfaces:**
- Consumes (기존 코드 — 시그니처 그대로):
  - `app/config.py`: `_int_env(name: str, default: int) -> int`, `@dataclass Config(service_group, service, seed, users, anon_users, models, retention_days)`, `load_config() -> Config`
  - `app/scenarios.py`: `@dataclass ScenarioState` 9필드(`not_ready_until_uptime_s: float=0.0`, `retry_after_s: int=5`, `rate_limit_every`, `error_503_every`, `summary_extra_pct`, `name_drift: str=""`, `generated_at_change_at_page`, `not_ready_at_page`, `request_count`)
  - `app/datagen.py`: `_det_int(seed: str, *parts: str, lo: int, hi: int) -> int`(sha256 결정적), `generated_at(date: str) -> str`(`"<date+1>T02:05:00+09:00"`), `build_records(cfg, date)`, `build_summary(records) -> dict`
  - `app/main.py`: `_err(status, code, message, retry_after=None) -> JSONResponse`, `_shared_gate() -> JSONResponse | None`(요청 카운터 공유·429/503), `_date_gate(raw_date) -> tuple[date | None, JSONResponse | None]`, `_identity() -> tuple[str, str]`(`name_drift` 접미), `_SCENARIO_RULES: dict[str, tuple[type, int | float]]`, `set_scenario`(`type(value) is want` 비교 → bool 거부), `reset_scenario`(`global SCN` 재생성), 모듈 전역 `CFG`, `SCN`, `STARTED_AT`, `now_kst()`
  - 테스트 fixture 패턴(`tests/test_api.py` 12-21행): `monkeypatch.setattr(main, "CFG", Config(...))`, `monkeypatch.setattr(main, "SCN", ScenarioState())`, `TestClient(main.app)`, `yday()`
  - 계약 검사기 `check_metrics_api.py`(표준 라이브러리만; 스펙 yaml을 읽지 않음): 종료코드 FAIL 있으면 1 / WARN만 0 / `--date` 오류 2; 동작 검사 C1 당일→400, C2 미래→400, C3 30일 전→404(200이면 WARN), C4 같은 date 재호출 동일 본문, C5 `2026-13-99`→400; B7 `gpuHours > gpuCount*24+1e-6` FAIL; B8 (model,gpuType,category) 중복 WARN; B10 gpu serving 모델마다 serving 행 없으면 WARN; B11 engine 부재 WARN
- Produces (6b T3·T4·T11이 소비):
  - `Config.metrics_retention_days: int = 14` ← env `MOCK_METRICS_RETENTION_DAYS`(`_int_env`; `< 1`이면 `ValueError("MOCK_METRICS_RETENTION_DAYS must be >= 1")`). 기존 `retention_days`(usage, 90)와 독립.
  - `ScenarioState` 신규 int 필드 6종(기존 9필드 뒤에 append, 기본 0=OFF): `metrics_gpu_hours_over`, `metrics_unknown_serving`, `metrics_pct_non_monotone`, `metrics_dup_gpu_rows`, `metrics_empty_gpu`, `metrics_engine_null`. `_SCENARIO_RULES`에 6항목 `(int, 0)`(최대값 검사 없음 — 0/1만 의미; bool·음수는 400 `invalid_scenario`).
  - `datagen.METRICS_ENGINE: dict = {"type": "vllm", "version": "0.10.1"}`, `datagen.METRICS_GPU_TYPE = "H100"`, `datagen._pct(seed: str, date: str, model: str, key: str) -> dict`(`{"p50","p90","p95","p99"}` 전부 float, 누적합으로 단조 보장: p50∈[50,500], p90=p50+[1,200], p95=p90+[1,100], p99=p95+[1,300]), `datagen.build_metrics(cfg: Config, date: str, scn: ScenarioState | None = None) -> dict` — 반환 키 순서 정확히 `["date", "serviceGroup", "service", "generatedAt", "engine", "gpu", "serving"]`. gpu 행 키 순서 `{"model","gpuType","category","gpuCount","gpuHours"}`: 모델당 `serving` 1행(`gpuType="H100"`, `gpuCount=_det_int(seed,date,model,"gc",lo=1,hi=8)`(int), `gpuHours=round(gpuCount*_det_int(seed,date,model,"gh",lo=6,hi=24)*1.0,1)`(float ≤ gpuCount×24)) + 첫 모델만 `standby` 1행(`gpuCount=1, gpuHours=24.0`) + `{"model":"unknown","gpuType":"H100","category":"test","gpuCount":1,"gpuHours":float(_det_int(seed,date,"unk","th",lo=1,hi=12))}` → 기본 3모델 = gpu 5행. serving = 모델당 `{"model", "ttftMs": _pct(seed,date,model,"ttft"), "itlMs": _pct(seed,date,model,"itl"), "outputTps": {"p50": float(_det_int(seed,date,model,"tps",lo=5,hi=200))}}` → 3행. `engine`은 `dict(METRICS_ENGINE)` 복사본. `serviceGroup/service`는 `cfg.service_group/cfg.service`(호출자가 `_identity()`로 덮어씀). 시나리오 적용 순서: dup(첫 gpu 행 복제본을 **인덱스 1에 삽입** → 첫 2행 동일) → hours_over(`gpu[0]["gpuHours"] = float(gpuCount*24+10)`) → unknown_serving(`{"model":"unknown","gpuType":"H100","category":"serving","gpuCount":1,"gpuHours":24.0}` append) → pct_non_monotone(`serving[0]["ttftMs"]["p90"] = p50 - 1`) → empty_gpu(`gpu = []`) → engine_null(`engine = None`). 같은 (seed, date, scn)이면 dict 동일.
  - `main._date_gate(raw_date: str, retention_days: int | None = None, subject: str = "usage")` — kwargs 2개 additive(기본값이면 기존 usage 동작·메시지 바이트 동일; `retention_days=None` → `CFG.retention_days`). 409 메시지 `f"{subject} for the requested date is not finalized yet; retry later"`, 404 메시지 `f"{subject} data for the requested date is past the retention window"`.
  - `GET /v1/metrics?date=YYYY-MM-DD`(`main.get_metrics`): `_shared_gate()` → `date is None`이면 400 `{"code":"invalid_date","message":"date query parameter is required"}` → `_date_gate(date, retention_days=CFG.metrics_retention_days, subject="metrics")` → `payload = build_metrics(CFG, date, SCN)`; `payload["serviceGroup"], payload["service"] = _identity()` → 200 JSON(`application/json`). 같은 (seed, date, SCN)에서 `resp.content` 바이트 동일. 404 본문 `{"code":"data_not_retained","message":"metrics data for the requested date is past the retention window"}`, 409 본문 `{"code":"data_not_ready","message":"metrics for the requested date is not finalized yet; retry later"}` + `Retry-After: <retry_after_s>`.
  - `run_conformance.sh`: usage 단계 뒤 `"${PYTHON}" contract/tests/check_metrics_api.py --base-url "http://127.0.0.1:${PORT}" --date "${DATE_ARG}"`(같은 uvicorn 프로세스; `set -e`라 FAIL→exit 1) + `echo "METRICS CONFORMANCE PASS (date=${DATE_ARG})"`.
  - `contract/SOURCE.md` 절 "token-metric-api (`GET /v1/metrics`) — Plan 6b T1 추가": origin `https://github.com/YoonsungNam/token-metric-api-spec`, commit `6a552d2`(2026-08-31), 파일 2개 + sha256, reason "메트릭 계약(케이스 A~F) 준수 검증", 갱신 절차.
  - `.github/workflows/test-mock-provider.yml` job `image` smoke: `curl -fsS "http://127.0.0.1:18000/v1/metrics?date=${YDAY}" | grep -q '"gpu"'`(컨테이너 포트 매핑 18000:8000 — 기존 smoke와 동일 포트). job `test`는 `./run_conformance.sh`가 이미 실행되므로 step 추가 없음.
  - 테스트 fixture 상수(T3·T4·T11 공통): `Config(users=8, anon_users=2, seed="metrics-t")` = 모델 기본 3종 `["claude-opus-4-8", "claude-sonnet-5", "claude-haiku-4-5"]`, `DATE="2026-09-10"`, `generatedAt="2026-09-11T02:05:00+09:00"`, `ENGINE={"type": "vllm", "version": "0.10.1"}`, `GPU_TYPE="H100"`.

- [ ] **Step 0: 전제 확인 — 브랜치·기존 테스트·스크래치 클론 pin**

```bash
cd /home/mini/github/token-data-pipeline
git branch --show-current                                   # 기대: feat/token-metrics-design
cd tools/mock-provider && python3 -m pytest -q               # 기대: "40 passed"
SPEC=/tmp/claude-1000/-home-mini-github-token-data-pipeline/8a0025e9-e64c-4caf-972c-788ea90abe0e/scratchpad/token-metric-api-spec
test -d "$SPEC" || git clone https://github.com/YoonsungNam/token-metric-api-spec.git "$SPEC"
git -C "$SPEC" checkout -q 6a552d2 && git -C "$SPEC" log --oneline -1   # 기대: "6a552d2 Move build deadline ..."
wc -l "$SPEC/token-metric-api.yaml" "$SPEC/scripts/check_metrics_api.py"   # 기대: 490 / 569
```

- [ ] **Step 1: 실패 테스트 — `Config.metrics_retention_days` (파일 신규 생성)**

`tools/mock-provider/tests/test_metrics_api.py` 신규(헤더 + Step 1 테스트 2개; 뒤 Step에서 append):

```python
"""GET /v1/metrics (token-metric-api @6a552d2) — config·시나리오·datagen·엔드포인트 테스트.

기존 tests/test_api.py의 fixture 패턴(monkeypatch CFG/SCN + TestClient)을 그대로 복제한다.
"""
import json
from dataclasses import fields as dc_fields
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

import app.main as main
from app.config import Config, load_config
from app.datagen import build_records, build_summary, generated_at
from app.scenarios import ScenarioState

METRICS_FLAGS = ("metrics_gpu_hours_over", "metrics_unknown_serving", "metrics_pct_non_monotone",
                 "metrics_dup_gpu_rows", "metrics_empty_gpu", "metrics_engine_null")
DATE = "2026-09-10"
CFG3 = Config(users=8, anon_users=2, seed="metrics-t")   # models 기본 3종 → gpu 5행 / serving 3행


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(main, "CFG", Config(users=8, anon_users=2, seed="metrics-api-t"))
    monkeypatch.setattr(main, "SCN", ScenarioState())
    return TestClient(main.app)


def yday() -> str:
    return (main.now_kst().date() - timedelta(days=1)).isoformat()


def days_ago(n: int) -> str:
    return (main.now_kst().date() - timedelta(days=n)).isoformat()


# ---------------------------------------------------------------- Step 1: config
def test_metrics_retention_default_14(monkeypatch):
    for k in ("MOCK_RETENTION_DAYS", "MOCK_METRICS_RETENTION_DAYS"):
        monkeypatch.delenv(k, raising=False)
    cfg = load_config()
    assert cfg.metrics_retention_days == 14
    assert cfg.retention_days == 90            # 기존 usage 보존 기본값 불변


def test_metrics_retention_rejects_zero(monkeypatch):
    monkeypatch.setenv("MOCK_METRICS_RETENTION_DAYS", "0")
    with pytest.raises(ValueError, match="MOCK_METRICS_RETENTION_DAYS must be >= 1"):
        load_config()
    monkeypatch.setenv("MOCK_METRICS_RETENTION_DAYS", "30")
    assert load_config().metrics_retention_days == 30
```

- [ ] **Step 2: 실패 확인**

```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider && python3 -m pytest -q tests/test_metrics_api.py
```
기대: `2 failed` — `AttributeError: 'Config' object has no attribute 'metrics_retention_days'`, `Failed: DID NOT RAISE <class 'ValueError'>`.

- [ ] **Step 3: 구현 — `app/config.py` 3개 hunk (additive)**

`tools/mock-provider/app/config.py` 20행 뒤(필드), 34행 뒤(env 파싱), 39행 뒤(검증):

```diff
@@ -18,6 +18,7 @@ class Config:
         default_factory=lambda: ["claude-opus-4-8", "claude-sonnet-5", "claude-haiku-4-5"]
     )
     retention_days: int = 90
+    metrics_retention_days: int = 14        # /v1/metrics 보존 일수 (계약: 14일 초과 → 404)
 
 
 def load_config() -> Config:
@@ -32,9 +33,12 @@ def load_config() -> Config:
         anon_users=_int_env("MOCK_ANON_USERS", 10),
         models=models or ["claude-opus-4-8", "claude-sonnet-5", "claude-haiku-4-5"],
         retention_days=_int_env("MOCK_RETENTION_DAYS", 90),
+        metrics_retention_days=_int_env("MOCK_METRICS_RETENTION_DAYS", 14),
     )
     if cfg.users < 0 or cfg.anon_users < 0:
         raise ValueError("MOCK_USERS/MOCK_ANON_USERS must be >= 0")
     if cfg.retention_days < 1:
         raise ValueError("MOCK_RETENTION_DAYS must be >= 1")
+    if cfg.metrics_retention_days < 1:
+        raise ValueError("MOCK_METRICS_RETENTION_DAYS must be >= 1")
     return cfg
```

적용 후 `app/config.py` 전문(44행):

```python
import os
from dataclasses import dataclass, field


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    return int(raw) if raw.strip() else default


@dataclass
class Config:
    service_group: str = "Mock Group"
    service: str = "Mock Service A"
    seed: str = "token-mock-1"
    users: int = 50
    anon_users: int = 10
    models: list[str] = field(
        default_factory=lambda: ["claude-opus-4-8", "claude-sonnet-5", "claude-haiku-4-5"]
    )
    retention_days: int = 90
    metrics_retention_days: int = 14        # /v1/metrics 보존 일수 (계약: 14일 초과 → 404)


def load_config() -> Config:
    models_raw = os.getenv("MOCK_MODELS", "")
    models = [m.strip() for m in models_raw.split(",") if m.strip()]
    models = list(dict.fromkeys(models))
    cfg = Config(
        service_group=os.getenv("MOCK_SERVICE_GROUP", "Mock Group"),
        service=os.getenv("MOCK_SERVICE", "Mock Service A"),
        seed=os.getenv("MOCK_SEED", "token-mock-1"),
        users=_int_env("MOCK_USERS", 50),
        anon_users=_int_env("MOCK_ANON_USERS", 10),
        models=models or ["claude-opus-4-8", "claude-sonnet-5", "claude-haiku-4-5"],
        retention_days=_int_env("MOCK_RETENTION_DAYS", 90),
        metrics_retention_days=_int_env("MOCK_METRICS_RETENTION_DAYS", 14),
    )
    if cfg.users < 0 or cfg.anon_users < 0:
        raise ValueError("MOCK_USERS/MOCK_ANON_USERS must be >= 0")
    if cfg.retention_days < 1:
        raise ValueError("MOCK_RETENTION_DAYS must be >= 1")
    if cfg.metrics_retention_days < 1:
        raise ValueError("MOCK_METRICS_RETENTION_DAYS must be >= 1")
    return cfg
```

- [ ] **Step 4: 통과 확인**

```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider && python3 -m pytest -q tests/test_metrics_api.py tests/test_config.py
```
기대: `6 passed`(신규 2 + 기존 config 4).

- [ ] **Step 5: 실패 테스트 — 시나리오 int 플래그 6종 (`tests/test_metrics_api.py`에 append)**

```python


# ---------------------------------------------------------------- Step 2: 시나리오 플래그 6종
def test_scenario_metrics_flags_reject_bool(client):
    r = client.post("/__mock/scenario", json={"metrics_empty_gpu": True})
    assert r.status_code == 400
    assert r.json() == {"code": "invalid_scenario", "message": "metrics_empty_gpu must be int"}


def test_scenario_metrics_flags_reject_negative(client):
    r = client.post("/__mock/scenario", json={"metrics_dup_gpu_rows": -1})
    assert r.status_code == 400
    assert r.json() == {"code": "invalid_scenario", "message": "metrics_dup_gpu_rows must be >= 0"}


def test_scenario_reset_clears_metrics_flags(client):
    r = client.post("/__mock/scenario", json={f: 1 for f in METRICS_FLAGS})
    assert r.status_code == 200
    assert all(r.json()[f] == 1 for f in METRICS_FLAGS)
    assert all(getattr(main.SCN, f) == 1 for f in METRICS_FLAGS)
    client.post("/__mock/reset")
    assert all(getattr(main.SCN, f) == 0 for f in METRICS_FLAGS)
    # 기존 9필드 순서 불변, 신규 6필드는 dataclass 끝에 append
    names = [f.name for f in dc_fields(ScenarioState)]
    assert names[:9] == ["not_ready_until_uptime_s", "retry_after_s", "rate_limit_every",
                         "error_503_every", "summary_extra_pct", "name_drift",
                         "generated_at_change_at_page", "not_ready_at_page", "request_count"]
    assert names[9:] == list(METRICS_FLAGS)
```

- [ ] **Step 6: 실패 확인**

```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider && python3 -m pytest -q tests/test_metrics_api.py
```
기대: `3 failed, 2 passed` — 앞 2개는 `{'message': "unknown scenario fields: ['metrics_empty_gpu']"} != {'message': 'metrics_empty_gpu must be int'}`(미등록 필드라 400은 나오지만 메시지가 다름), 3번째는 `assert 400 == 200`.

- [ ] **Step 7: 구현 — `app/scenarios.py` 6필드 append + `app/main.py` `_SCENARIO_RULES` 6항목**

`tools/mock-provider/app/scenarios.py` 15행(`request_count`) 뒤 append — 적용 후 전문(22행):

```python
from dataclasses import dataclass


@dataclass
class ScenarioState:
    """계약 밖 일탈 주입 상태 — 전부 기본값(OFF)이면 완전한 계약 준수 동작."""
    not_ready_until_uptime_s: float = 0.0   # 앱 가동 N초 전까지, 과거 유효 date 요청에 대해 409 (당일/미래 400은 그대로)
    retry_after_s: int = 5                  # 409/429 응답의 Retry-After 값
    rate_limit_every: int = 0               # N번째 요청마다 429 (0=off)
    error_503_every: int = 0                # N번째 요청마다 503 (0=off)
    summary_extra_pct: int = 0              # summary inputTokens를 +N% 왜곡 (§5.1-3-4 검증용)
    name_drift: str = ""                    # 응답 serviceGroup/service 뒤에 붙일 문자열 (§5.0 검증용)
    generated_at_change_at_page: int = 0    # N페이지부터 generatedAt 변경 (§5.3 검증용)
    not_ready_at_page: int = 0              # N페이지에서 409 (§5.2 검증용)
    request_count: int = 0                  # 429/503 주기 판정용 카운터
    # --- /v1/metrics 전용 (int 0/1 — 0=OFF; 수집기 §5.3 계층 2 플래그·케이스 E·engine null 검증용) ---
    metrics_gpu_hours_over: int = 0         # 1이면 첫 gpu 행 gpuHours = gpuCount*24 + 10 (hours_over_count)
    metrics_unknown_serving: int = 0        # 1이면 model="unknown", category="serving" 행 1개 추가 (unknown_violation)
    metrics_pct_non_monotone: int = 0       # 1이면 첫 serving 행 ttftMs p90 = p50 - 1 (pct_non_monotone)
    metrics_dup_gpu_rows: int = 0           # 1이면 첫 gpu 행 복제본을 인덱스 1에 삽입 — 인접 중복 (dup_merged)
    metrics_empty_gpu: int = 0              # 1이면 gpu: [] (케이스 E — serving만 있는 응답)
    metrics_engine_null: int = 0            # 1이면 engine: null (engine 부재 허용 검증)
```

`tools/mock-provider/app/main.py` 147-157행 `_SCENARIO_RULES` 끝(`"not_ready_at_page": (int, 0),` 뒤)에 6항목:

```diff
@@ -154,6 +154,13 @@ _SCENARIO_RULES: dict[str, tuple[type, int | float]] = {
     "name_drift": (str, 0),
     "generated_at_change_at_page": (int, 0),
     "not_ready_at_page": (int, 0),
+    # /v1/metrics 전용 int 플래그 6종 (0=OFF, 1=ON; 최대값 검사 없음 — 0/1만 의미)
+    "metrics_gpu_hours_over": (int, 0),
+    "metrics_unknown_serving": (int, 0),
+    "metrics_pct_non_monotone": (int, 0),
+    "metrics_dup_gpu_rows": (int, 0),
+    "metrics_empty_gpu": (int, 0),
+    "metrics_engine_null": (int, 0),
 }
```

(`set_scenario`는 무수정 — `type(value) is int` 비교가 bool을 거부하고 `value < 0`이 음수를 거부하며, `reset_scenario`의 `ScenarioState()` 재생성이 6필드를 0으로 되돌린다.)

- [ ] **Step 8: 통과 확인**

```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider && python3 -m pytest -q tests/test_metrics_api.py tests/test_scenarios.py
```
기대: `15 passed`(신규 5 + 기존 scenarios 10).

- [ ] **Step 9: 실패 테스트 — `datagen.build_metrics` (import 1행 교체 + 테스트 3개 append)**

`tests/test_metrics_api.py` 14행의 datagen import를 교체:

```diff
@@ -11,6 +11,7 @@ from fastapi.testclient import TestClient
 
 import app.main as main
 from app.config import Config, load_config
-from app.datagen import build_records, build_summary, generated_at
+from app.datagen import (METRICS_ENGINE, build_metrics, build_records, build_summary,
+                         generated_at)
 from app.scenarios import ScenarioState
 
```

파일 끝에 append:

```python


# ---------------------------------------------------------------- Step 3: datagen.build_metrics
def test_build_metrics_deterministic():
    a, b = build_metrics(CFG3, DATE), build_metrics(CFG3, DATE)
    assert a == b
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
    assert build_metrics(CFG3, "2026-09-11") != a                      # 날짜가 다르면 데이터가 다름
    assert build_metrics(CFG3, DATE, ScenarioState()) == a              # 전 플래그 OFF == scn 생략
    assert a["engine"] == METRICS_ENGINE and a["engine"] is not METRICS_ENGINE   # 복사본(호출자 mutate 안전)


def test_build_metrics_shape():
    p = build_metrics(CFG3, DATE)
    assert set(p) == {"date", "serviceGroup", "service", "generatedAt", "engine", "gpu", "serving"}
    assert list(p) == ["date", "serviceGroup", "service", "generatedAt", "engine", "gpu", "serving"]
    assert p["date"] == DATE and p["generatedAt"] == generated_at(DATE) == "2026-09-11T02:05:00+09:00"
    assert p["serviceGroup"] == CFG3.service_group and p["service"] == CFG3.service
    assert p["engine"] == {"type": "vllm", "version": "0.10.1"}
    gpu, serving = p["gpu"], p["serving"]
    assert len(gpu) == 5 and len(serving) == 3
    assert {r["category"] for r in gpu} == {"serving", "standby", "test"}
    assert all(set(r) == {"model", "gpuType", "category", "gpuCount", "gpuHours"} for r in gpu)
    assert all(r["gpuType"] == "H100" for r in gpu)
    assert [r["model"] for r in gpu if r["category"] == "serving"] == CFG3.models
    assert [r["model"] for r in gpu if r["category"] == "standby"] == [CFG3.models[0]]
    assert [r["category"] for r in gpu if r["model"] == "unknown"] == ["test"]
    assert all(isinstance(r["gpuCount"], int) and r["gpuCount"] >= 1 for r in gpu)
    assert all(isinstance(r["gpuHours"], float) and 0 < r["gpuHours"] <= r["gpuCount"] * 24 for r in gpu)
    keys = [(r["model"], r["gpuType"], r["category"]) for r in gpu]
    assert len(keys) == len(set(keys))                                  # 기본 응답은 중복 행 없음
    assert [r["model"] for r in serving] == CFG3.models
    for r in serving:
        assert set(r) == {"model", "ttftMs", "itlMs", "outputTps"}
        assert set(r["outputTps"]) == {"p50"} and isinstance(r["outputTps"]["p50"], float)
        for block in ("ttftMs", "itlMs"):
            pc = r[block]
            assert list(pc) == ["p50", "p90", "p95", "p99"]
            assert all(isinstance(pc[k], float) and pc[k] >= 0 for k in pc)
            assert pc["p50"] <= pc["p90"] <= pc["p95"] <= pc["p99"]


def test_build_metrics_scenarios():
    base = build_metrics(CFG3, DATE)

    def with_flag(name: str) -> dict:
        scn = ScenarioState()
        setattr(scn, name, 1)
        return build_metrics(CFG3, DATE, scn)

    dup = with_flag("metrics_dup_gpu_rows")
    assert len(dup["gpu"]) == 6 and dup["gpu"][0] == dup["gpu"][1] == base["gpu"][0]
    assert dup["gpu"][2:] == base["gpu"][1:]
    over = with_flag("metrics_gpu_hours_over")
    assert over["gpu"][0]["gpuHours"] == over["gpu"][0]["gpuCount"] * 24 + 10
    assert over["gpu"][1:] == base["gpu"][1:]
    unk = with_flag("metrics_unknown_serving")
    assert len(unk["gpu"]) == 6
    assert unk["gpu"][-1] == {"model": "unknown", "gpuType": "H100", "category": "serving",
                              "gpuCount": 1, "gpuHours": 24.0}
    mono = with_flag("metrics_pct_non_monotone")
    assert mono["serving"][0]["ttftMs"]["p90"] == mono["serving"][0]["ttftMs"]["p50"] - 1
    assert mono["serving"][0]["itlMs"] == base["serving"][0]["itlMs"]
    assert mono["serving"][1:] == base["serving"][1:]
    empty = with_flag("metrics_empty_gpu")
    assert empty["gpu"] == [] and empty["serving"] == base["serving"]
    null_engine = with_flag("metrics_engine_null")
    assert null_engine["engine"] is None and null_engine["gpu"] == base["gpu"]
    assert base == build_metrics(CFG3, DATE)                            # 시나리오가 기본 결과를 오염시키지 않음
```

- [ ] **Step 10: 실패 확인**

```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider && python3 -m pytest -q tests/test_metrics_api.py
```
기대: 수집 단계 오류 `ImportError: cannot import name 'METRICS_ENGINE' from 'app.datagen'`.

- [ ] **Step 11: 구현 — `app/datagen.py` import 1행 + 파일 끝 append**

`tools/mock-provider/app/datagen.py` 10행 `from app.config import Config` 뒤에 import 1행(순환 없음 — `scenarios.py`는 `dataclasses`만 import):

```diff
@@ -8,6 +8,7 @@ from dataclasses import dataclass
 from datetime import date as date_cls, timedelta
 
 from app.config import Config
+from app.scenarios import ScenarioState
 
 
 @dataclass(frozen=True)
```

100행(`generated_at` 끝) 뒤에 append:

```python


# ---------------------------------------------------------------------------
# /v1/metrics (token-metric-api @6a552d2) — 결정적 GPU Hour·성능 메트릭 생성
# ---------------------------------------------------------------------------
METRICS_ENGINE: dict = {"type": "vllm", "version": "0.10.1"}   # 고정 자기신고 (계약 Engine)
METRICS_GPU_TYPE = "H100"


def _pct(seed: str, date: str, model: str, key: str) -> dict:
    """LatencyPercentiles — p50≤p90≤p95≤p99 단조를 누적합으로 보장 (전부 float)."""
    p50 = _det_int(seed, date, model, key, "p50", lo=50, hi=500)
    p90 = p50 + _det_int(seed, date, model, key, "d90", lo=1, hi=200)
    p95 = p90 + _det_int(seed, date, model, key, "d95", lo=1, hi=100)
    p99 = p95 + _det_int(seed, date, model, key, "d99", lo=1, hi=300)
    return {"p50": float(p50), "p90": float(p90), "p95": float(p95), "p99": float(p99)}


def build_metrics(cfg: Config, date: str, scn: ScenarioState | None = None) -> dict:
    """같은 (seed, date, scn)이면 항상 같은 dict (키 순서 포함) — C4 멱등성·CI 기대치의 근거.

    gpu = 모델당 serving 1행 + 첫 모델 standby 1행 + model="unknown" test 1행 (기본 3모델 → 5행),
    serving = 모델당 1행(ttftMs·itlMs·outputTps{p50}). serviceGroup/service는 cfg 값이며
    호출자(main.get_metrics)가 _identity()로 덮어쓴다. 시나리오 적용 순서:
    dup → hours_over → unknown_serving → pct_non_monotone → empty_gpu → engine_null.
    """
    seed = cfg.seed
    gpu: list[dict] = []
    for model in cfg.models:
        gpu_count = _det_int(seed, date, model, "gc", lo=1, hi=8)
        hours_per_gpu = _det_int(seed, date, model, "gh", lo=6, hi=24)
        gpu.append({"model": model, "gpuType": METRICS_GPU_TYPE, "category": "serving",
                    "gpuCount": gpu_count, "gpuHours": round(gpu_count * hours_per_gpu * 1.0, 1)})
    if cfg.models:
        gpu.append({"model": cfg.models[0], "gpuType": METRICS_GPU_TYPE, "category": "standby",
                    "gpuCount": 1, "gpuHours": 24.0})
    gpu.append({"model": "unknown", "gpuType": METRICS_GPU_TYPE, "category": "test",
                "gpuCount": 1, "gpuHours": float(_det_int(seed, date, "unk", "th", lo=1, hi=12))})
    serving: list[dict] = []
    for model in cfg.models:
        serving.append({
            "model": model,
            "ttftMs": _pct(seed, date, model, "ttft"),
            "itlMs": _pct(seed, date, model, "itl"),
            "outputTps": {"p50": float(_det_int(seed, date, model, "tps", lo=5, hi=200))},
        })
    engine: dict | None = dict(METRICS_ENGINE)

    if scn is not None:
        if scn.metrics_dup_gpu_rows and gpu:
            gpu.insert(1, dict(gpu[0]))                       # 첫 행 복제 — 인접 중복 (dup_merged)
        if scn.metrics_gpu_hours_over and gpu:
            gpu[0]["gpuHours"] = float(gpu[0]["gpuCount"] * 24 + 10)   # hours_over_count
        if scn.metrics_unknown_serving:
            gpu.append({"model": "unknown", "gpuType": METRICS_GPU_TYPE, "category": "serving",
                        "gpuCount": 1, "gpuHours": 24.0})     # unknown_violation
        if scn.metrics_pct_non_monotone and serving:
            serving[0]["ttftMs"]["p90"] = serving[0]["ttftMs"]["p50"] - 1   # pct_non_monotone
        if scn.metrics_empty_gpu:
            gpu = []                                          # 케이스 E
        if scn.metrics_engine_null:
            engine = None

    return {
        "date": date,
        "serviceGroup": cfg.service_group,
        "service": cfg.service,
        "generatedAt": generated_at(date),
        "engine": engine,
        "gpu": gpu,
        "serving": serving,
    }
```

- [ ] **Step 12: 통과 확인**

```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider && python3 -m pytest -q tests/test_metrics_api.py tests/test_datagen.py
```
기대: `15 passed`(신규 8 + 기존 datagen 7). 결정성 확인용 1회성 출력(값은 seed 의존 — 기록만):

```bash
python3 -c "from app.config import Config; from app.datagen import build_metrics; import json; p = build_metrics(Config(seed='e2e-seed-1'), '2026-09-10'); print(len(p['gpu']), len(p['serving']), round(sum(r['gpuHours'] for r in p['gpu']), 1))"
```
기대: `5 3 331.0`(seed `e2e-seed-1`·2026-09-10 고정값 — T11 `ci_expectations.py`가 같은 식으로 기대치를 만든다).

- [ ] **Step 13: 실패 테스트 — `GET /v1/metrics` 엔드포인트 (`tests/test_metrics_api.py`에 append)**

```python


# ---------------------------------------------------------------- Step 4: GET /v1/metrics
def test_metrics_ok_shape(client):
    d = yday()
    resp = client.get("/v1/metrics", params={"date": d})
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")
    body = resp.json()
    assert list(body) == ["date", "serviceGroup", "service", "generatedAt", "engine", "gpu", "serving"]
    assert body["date"] == d
    assert body["serviceGroup"] == "Mock Group" and body["service"] == "Mock Service A"
    assert body["generatedAt"] == f"{main.now_kst().date().isoformat()}T02:05:00+09:00"
    assert body["engine"] == METRICS_ENGINE
    assert len(body["gpu"]) == 5 and len(body["serving"]) == 3
    assert body == build_metrics(main.CFG, d)                           # 시나리오 OFF == 순수 생성기 출력


def test_metrics_today_400(client):
    today = main.now_kst().date()
    for d in (today.isoformat(), (today + timedelta(days=7)).isoformat(), "2026-13-99", "2026/09/10"):
        r = client.get("/v1/metrics", params={"date": d})
        assert r.status_code == 400 and r.json()["code"] == "invalid_date"


def test_metrics_missing_date_400(client):
    r = client.get("/v1/metrics")
    assert r.status_code == 400
    assert r.json() == {"code": "invalid_date", "message": "date query parameter is required"}


def test_metrics_retention_404_at_15_days(client):
    r = client.get("/v1/metrics", params={"date": days_ago(15)})
    assert r.status_code == 404
    assert r.json() == {"code": "data_not_retained",
                        "message": "metrics data for the requested date is past the retention window"}
    assert client.get("/v1/metrics", params={"date": days_ago(14)}).status_code == 200
    # usage 보존(90일)과 독립 — 같은 15일 전 date가 usage에서는 200
    assert client.get("/v1/usage/summary", params={"date": days_ago(15)}).status_code == 200


def test_metrics_not_ready_409_with_retry_after(client):
    main.SCN.not_ready_until_uptime_s = 10 ** 9   # 사실상 항상 미확정
    r = client.get("/v1/metrics", params={"date": yday()})
    assert r.status_code == 409 and r.headers["Retry-After"] == "5"
    assert r.json() == {"code": "data_not_ready",
                        "message": "metrics for the requested date is not finalized yet; retry later"}
    # 당일/미래 400은 409보다 우선
    assert client.get("/v1/metrics", params={"date": main.now_kst().date().isoformat()}).status_code == 400


def test_metrics_shares_request_counter_with_usage(client):
    main.SCN.rate_limit_every = 2
    assert client.get("/v1/usage/summary", params={"date": yday()}).status_code == 200
    r = client.get("/v1/metrics", params={"date": yday()})
    assert r.status_code == 429 and r.headers["Retry-After"] == "5"
    assert client.get("/v1/metrics", params={"date": yday()}).status_code == 200


def test_metrics_identity_drift(client):
    main.SCN.name_drift = " "
    body = client.get("/v1/metrics", params={"date": yday()}).json()
    assert body["service"] == "Mock Service A " and body["serviceGroup"] == "Mock Group "


def test_metrics_same_date_same_body(client):
    a = client.get("/v1/metrics", params={"date": yday()})
    b = client.get("/v1/metrics", params={"date": yday()})
    assert a.status_code == b.status_code == 200 and a.content == b.content


def test_metrics_scenario_flags_via_endpoint(client):
    assert client.post("/__mock/scenario", json={"metrics_empty_gpu": 1, "metrics_engine_null": 1}).status_code == 200
    body = client.get("/v1/metrics", params={"date": yday()}).json()
    assert body["gpu"] == [] and body["engine"] is None and len(body["serving"]) == 3
    client.post("/__mock/reset")
    body = client.get("/v1/metrics", params={"date": yday()}).json()
    assert len(body["gpu"]) == 5 and body["engine"] == METRICS_ENGINE


def test_usage_endpoints_unchanged_bytes(client):
    d = yday()
    expected = {"serviceGroup": main.CFG.service_group, "service": main.CFG.service, "date": d,
                "generatedAt": generated_at(d), **build_summary(build_records(main.CFG, d))}
    r = client.get("/v1/usage/summary", params={"date": d})
    assert r.status_code == 200
    assert r.content == json.dumps(expected, ensure_ascii=False, separators=(",", ":")).encode()
    # _date_gate 기본 인자 경로: usage 메시지 문자열 불변
    old = days_ago(main.CFG.retention_days + 1)
    assert client.get("/v1/usage/summary", params={"date": old}).json() == {
        "code": "data_not_retained",
        "message": "usage data for the requested date is past the retention window"}
    main.SCN.not_ready_until_uptime_s = 10 ** 9
    assert client.get("/v1/usage", params={"date": d}).json() == {
        "code": "data_not_ready",
        "message": "usage for the requested date is not finalized yet; retry later"}
```

- [ ] **Step 14: 실패 확인**

```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider && python3 -m pytest -q tests/test_metrics_api.py
```
기대: `9 failed, 9 passed` — 라우트 부재로 `assert 404 == 200`, `assert 404 == 400`, `{'detail': 'Not Found'} == {...}`(`test_usage_endpoints_unchanged_bytes`는 usage만 호출하므로 이미 통과).


- [ ] **Step 15: 구현 — `app/main.py` import 1행 + `_date_gate` additive kwargs 2개 + `GET /v1/metrics` 엔드포인트 + `_SCENARIO_RULES` 6항목**

`tools/mock-provider/app/main.py` 편집 4곳 (기존 `/v1/usage`·`/v1/usage/summary` 핸들러 본문은 한 글자도 바꾸지 않음):

(a) 12행 import — `build_metrics` 추가:

```diff
@@ -9,7 +9,7 @@ from fastapi.responses import JSONResponse
 
 from app.config import load_config
 from app.cursors import CursorError, decode_cursor, encode_cursor
-from app.datagen import build_records, build_summary, generated_at, to_api_dict
+from app.datagen import build_metrics, build_records, build_summary, generated_at, to_api_dict
 from app.scenarios import ScenarioState
 
 KST = timezone(timedelta(hours=9))
```

(b) 55~73행 `_date_gate` — 시그니처에 `retention_days=None, subject="usage"` 추가. 기본값 호출(기존 usage 경로)이면 `CFG.retention_days`·`"usage ..."` 메시지 그대로 → 바이트 불변:

```diff
@@ -52,8 +52,14 @@ def _shared_gate() -> JSONResponse | None:
     return None
 
 
-def _date_gate(raw_date: str) -> tuple[date_cls | None, JSONResponse | None]:
-    """계약의 date 규칙: 당일/미래 400, 보존 초과 404, 미확정 409."""
+def _date_gate(raw_date: str, retention_days: int | None = None,
+               subject: str = "usage") -> tuple[date_cls | None, JSONResponse | None]:
+    """계약의 date 규칙: 당일/미래 400, 보존 초과 404, 미확정 409.
+
+    retention_days/subject는 /v1/metrics용 additive 인자 — 기본값이면 기존 usage 동작·메시지와 바이트 동일.
+    """
+    if retention_days is None:
+        retention_days = CFG.retention_days
     if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_date):
         return None, _err(400, "invalid_date", "date must be YYYY-MM-DD")
     try:
@@ -65,11 +71,11 @@ def _date_gate(raw_date: str) -> tuple[date_cls | None, JSONResponse | None]:
         return None, _err(400, "invalid_date", "date must be a past day (KST)")
     if time.monotonic() - STARTED_AT < SCN.not_ready_until_uptime_s:
         return None, _err(409, "data_not_ready",
-                          "usage for the requested date is not finalized yet; retry later",
+                          f"{subject} for the requested date is not finalized yet; retry later",
                           retry_after=SCN.retry_after_s)
-    if d < today - timedelta(days=CFG.retention_days):
+    if d < today - timedelta(days=retention_days):
         return None, _err(404, "data_not_retained",
-                          "usage data for the requested date is past the retention window")
+                          f"{subject} data for the requested date is past the retention window")
     return d, None
```

(c) `get_usage_summary`가 끝나는 144행(`"generatedAt": generated_at(date), **summary}`) 다음, `_SCENARIO_RULES` 정의 앞에 새 엔드포인트 삽입 (빈 줄 2개로 분리):

```diff
@@ -144,6 +150,21 @@ def get_usage_summary(date: str | None = Query(None)):
             "generatedAt": generated_at(date), **summary}
 
 
+@app.get("/v1/metrics")
+def get_metrics(date: str | None = Query(None)):
+    """token-metric-api @6a552d2 GET /v1/metrics — 단건, 보존 CFG.metrics_retention_days(기본 14)."""
+    if (gate := _shared_gate()) is not None:
+        return gate
+    if date is None:
+        return _err(400, "invalid_date", "date query parameter is required")
+    _, date_err = _date_gate(date, retention_days=CFG.metrics_retention_days, subject="metrics")
+    if date_err is not None:
+        return date_err
+    payload = build_metrics(CFG, date, SCN)
+    payload["serviceGroup"], payload["service"] = _identity()
+    return payload
+
+
 _SCENARIO_RULES: dict[str, tuple[type, int | float]] = {
     # field: (required type, minimum)
     "not_ready_until_uptime_s": (float, 0),
```

삽입되는 함수 전문(참조용, 위 hunk와 동일):

```python
@app.get("/v1/metrics")
def get_metrics(date: str | None = Query(None)):
    """token-metric-api @6a552d2 GET /v1/metrics — 단건, 보존 CFG.metrics_retention_days(기본 14)."""
    if (gate := _shared_gate()) is not None:
        return gate
    if date is None:
        return _err(400, "invalid_date", "date query parameter is required")
    _, date_err = _date_gate(date, retention_days=CFG.metrics_retention_days, subject="metrics")
    if date_err is not None:
        return date_err
    payload = build_metrics(CFG, date, SCN)
    payload["serviceGroup"], payload["service"] = _identity()
    return payload
```

동작 근거: `_shared_gate()`가 `SCN.request_count`를 증가시키므로 429/503 카운터가 usage와 공유됨(계약 §"429/503 shared counter"); `_identity()`가 `name_drift` 시나리오를 usage와 동일하게 반영; `SCN`은 `reset_scenario`가 `global SCN`으로 재바인딩하므로 매 요청 시 모듈 전역을 참조해야 한다(캐싱 금지).

(d) `_SCENARIO_RULES`는 Step 7에서 이미 6항목 추가됨 — 이 단계에서 추가 편집 없음(확인만: `grep -c '"metrics_' app/main.py` → `6`).

- [ ] **Step 16: 통과 확인 — 전체 테스트**

```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider && python3 -m pytest -q
```
기대: `58 passed` (기존 40 + `test_metrics_api.py` 18). 기존 40개 테스트 파일은 미수정(`git status --porcelain tests/` 출력에 `?? tests/test_metrics_api.py` 1행만).

바이트 불변 교차 확인(선택, 같은 셸):
```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider && python3 -m pytest -q tests/test_metrics_api.py -k "unchanged_bytes or shares_request_counter"
```
기대: `2 passed`.

- [ ] **Step 17: 계약 벤더링 — `contract/token-metric-api.yaml` + `contract/tests/check_metrics_api.py` 바이트 복사 + `contract/SOURCE.md` 절 추가**

Step 0에서 pin한 스크래치 clone(`$SPEC`, HEAD `6a552d2`)에서 두 파일을 그대로 복사한다. 원본은 실행 비트가 없으므로(`-rw-rw-r--`) 체커에만 `chmod +x` — 내용 바이트는 변하지 않으므로 sha256 동일:

```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider
SPEC=/tmp/claude-1000/-home-mini-github-token-data-pipeline/8a0025e9-e64c-4caf-972c-788ea90abe0e/scratchpad/token-metric-api-spec
git -C "$SPEC" rev-parse --short HEAD
cp "$SPEC/token-metric-api.yaml" contract/token-metric-api.yaml
cp "$SPEC/scripts/check_metrics_api.py" contract/tests/check_metrics_api.py
chmod +x contract/tests/check_metrics_api.py
sha256sum contract/token-metric-api.yaml contract/tests/check_metrics_api.py
wc -l contract/token-metric-api.yaml contract/tests/check_metrics_api.py
```
기대 출력(순서대로):
```
6a552d2
a7961c71370ba5bcc7cefe60bf71249090aca8a9e20ed60d8d1f27c9a8d4dc27  contract/token-metric-api.yaml
7173ca982c1bcbc0255e02c81a7a35486837a597d0ac5ad90df7885099525a0e  contract/tests/check_metrics_api.py
  490 contract/token-metric-api.yaml
  569 contract/tests/check_metrics_api.py
 1059 total
```
해시가 다르면 clone이 `6a552d2`가 아닌 것 — `git -C "$SPEC" checkout 6a552d28bbc35d30b51b83caad5b51f6705563c7` 후 재복사. 체커 스크립트는 수정 금지(드리프트는 해시로 추적).

`contract/SOURCE.md`(기존 9행, usage 절)의 끝에 아래 절을 그대로 추가한다 (기존 usage 절 무수정):

```markdown

## token-metric-api (`GET /v1/metrics`) — Plan 6b T1 추가

- 출처: https://github.com/YoonsungNam/token-metric-api-spec @ commit `6a552d2` (2026-08-31, spec `version: 0.1.0`)
- 파일: `token-metric-api.yaml` (490행) ← 원본 루트 `token-metric-api.yaml`,
  `tests/check_metrics_api.py` (569행, 실행 비트) ← 원본 `scripts/check_metrics_api.py`
- sha256 (바이트 동일 복사 — `sha256sum contract/token-metric-api.yaml contract/tests/check_metrics_api.py`로 재확인):
  - `a7961c71370ba5bcc7cefe60bf71249090aca8a9e20ed60d8d1f27c9a8d4dc27  token-metric-api.yaml`
  - `7173ca982c1bcbc0255e02c81a7a35486837a597d0ac5ad90df7885099525a0e  tests/check_metrics_api.py`
- 이유: 메트릭 계약(케이스 A~F) 준수 검증 — `run_conformance.sh`가 usage 단계 뒤에 같은 uvicorn
  프로세스로 `check_metrics_api.py`를 실행한다 (FAIL 있으면 exit 1, WARN만이면 exit 0, `--date` 오류 exit 2).
- 갱신 절차: 원본 레포의 새 커밋을 스크래치에 clone → 두 파일을 `cp`로 바이트 복사(`chmod +x` 유지) →
  `sha256sum`으로 본 절의 해시·커밋·날짜 갱신 → `./run_conformance.sh`로 재검증. 스크립트 수정 금지(드리프트는 해시로 추적).
- 주의: `check_metrics_api.py`는 표준 라이브러리만 쓰고 스펙 yaml을 읽지 않는다(상대 경로 의존 없음) —
  `contract/tests/` 배치는 usage `conformance_check.py`와의 일관성 목적. 동작 검사 C3는 "30일 전 → 404"를
  기대하므로 `MOCK_METRICS_RETENTION_DAYS`를 30 이상으로 올리면 WARN(비치명)이 난다.
```

검증:
```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider
sha256sum -c <(printf '%s\n' \
  "a7961c71370ba5bcc7cefe60bf71249090aca8a9e20ed60d8d1f27c9a8d4dc27  contract/token-metric-api.yaml" \
  "7173ca982c1bcbc0255e02c81a7a35486837a597d0ac5ad90df7885099525a0e  contract/tests/check_metrics_api.py")
grep -c "^## token-metric-api" contract/SOURCE.md
python3 -c "import yaml; d=yaml.safe_load(open('contract/token-metric-api.yaml')); print(d['info']['version'], sorted(d['paths']))"
python3 contract/tests/check_metrics_api.py --help | head -n 1
```
기대:
```
contract/token-metric-api.yaml: OK
contract/tests/check_metrics_api.py: OK
1
0.1.0 ['/v1/metrics']
usage: check_metrics_api.py [-h] --base-url BASE_URL [--date DATE]
```
(`--help` 1행은 argparse 줄바꿈 폭에 따라 `[--skip-behavior]`까지 이어질 수 있음 — `usage: check_metrics_api.py` 접두만 확인.)

- [ ] **Step 18: `run_conformance.sh` — metrics 자가 검사 단계 추가(같은 uvicorn 프로세스) + 실행 검증**

`tools/mock-provider/run_conformance.sh` 20~21행(usage conformance 실행 + `echo "CONFORMANCE PASS ..."`) 뒤에 3행 추가. uvicorn 기동·trap·healthz 대기·usage 단계는 무수정:

```diff
@@ -20,2 +20,6 @@ done
 "${PYTHON}" contract/tests/conformance_check.py --base-url "http://127.0.0.1:${PORT}" --date "${DATE_ARG}"
 echo "CONFORMANCE PASS (date=${DATE_ARG})"
+
+# /v1/metrics — token-metric-api @6a552d2 자가 검사 (같은 uvicorn 프로세스; FAIL이 있으면 exit 1)
+"${PYTHON}" contract/tests/check_metrics_api.py --base-url "http://127.0.0.1:${PORT}" --date "${DATE_ARG}"
+echo "METRICS CONFORMANCE PASS (date=${DATE_ARG})"
```

`set -euo pipefail`이 이미 걸려 있으므로 체커가 FAIL(exit 1)이면 마지막 echo에 도달하지 못하고 스크립트가 비0으로 종료된다. WARN만(exit 0)이면 통과 — WARN 건수는 로그 `결과:` 행으로 확인.

검증(포트 충돌 회피용 `PORT=18777`; `DATE_ARG` 기본 = KST 어제):
```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider && bash -n run_conformance.sh && echo SYNTAX_OK
cd /home/mini/github/token-data-pipeline/tools/mock-provider && PORT=18777 ./run_conformance.sh 2>&1 | grep -E "^(CONFORMANCE PASS|결과:|METRICS CONFORMANCE PASS)"
```
기대(날짜는 실행일 KST 어제 — 2026-09-04 실행 시):
```
SYNTAX_OK
CONFORMANCE PASS (date=2026-09-03)
결과: PASS 15 · WARN 0 · FAIL 0
METRICS CONFORMANCE PASS (date=2026-09-03)
```
`결과:` 행의 15 = check_metrics_api.py의 구조 검사(케이스 A~F·B7~B11) + 동작 검사 C1·C2·C3·C5 (C4 409는 mock 기본 상태에서 미발생이라 검사 목록에 미포함) — WARN이 0이 아니면 기본 fixture가 B8(중복)·B10/B11 규칙을 건드린 것이므로 `build_metrics` 기본 경로를 의심할 것.

역검증(플래그가 실제로 체커에 잡히는지 — 한 번만 수동 확인, 커밋 대상 아님):
```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider
PORT=18779 python3 -m uvicorn app.main:app --host 127.0.0.1 --port 18779 >/dev/null 2>&1 &
UV=$!; for _ in $(seq 1 50); do curl -sf http://127.0.0.1:18779/healthz >/dev/null && break; sleep 0.2; done
curl -sS -X POST http://127.0.0.1:18779/__mock/scenario -H 'content-type: application/json' -d '{"metrics_gpu_hours_over":1}' >/dev/null
python3 contract/tests/check_metrics_api.py --base-url http://127.0.0.1:18779 --no-fix-guide 2>&1 | grep -E "\[FAIL\]|^결과:"; echo "exit=${PIPESTATUS[0]}"
kill "$UV"
```
기대: `[FAIL] B7     gpu[0]: gpuHours(178.0) > gpuCount×24(168) — 검증 규칙 위반` 1행(2026-09-04 실행·기본 seed 기준 gpuCount 7 — 실행일이 다르면 date가 달라져 숫자도 달라짐; ANSI 색 코드 포함) + `결과: PASS 15 · WARN 0 · FAIL 1` + `exit=1`.

- [ ] **Step 19: `README.md` — `/v1/metrics` 절·환경변수 행·플래그 표·검증 라인 (hunk 4개)**

`tools/mock-provider/README.md`(82행) 편집. 기존 문장은 삭제하지 않고 2행(`MOCK_RETENTION_DAYS` 주석·검증 라인 주석)만 보강:

```diff
@@ -2,12 +2,15 @@
 
 `token-usage-api` 계약(v1.1.0, `contract/` vendored @6c32650)을 구현한 결정적 mock 서비스.
 수집기·mart의 CI E2E와 stage(홈랩) 통합 테스트의 데이터 소스 역할 (스펙 §8.1).
+자매 계약 `token-metric-api`(`contract/token-metric-api.yaml` vendored @6a552d2)의 `GET /v1/metrics`도
+같은 앱에서 제공한다 — 아래 "/v1/metrics" 절.
 
 ## 실행
 
     pip install -r requirements-dev.txt
     uvicorn app.main:app --port 8000
     curl "http://127.0.0.1:8000/v1/usage?date=$(date -d yesterday +%F)&limit=100"
+    curl "http://127.0.0.1:8000/v1/metrics?date=$(date -d yesterday +%F)"
 
 ## 설정 (환경변수)
 
@@ -17,7 +20,8 @@
 | MOCK_SEED | token-mock-1 | 결정적 데이터 시드 — 같은 seed+date = 같은 데이터 |
 | MOCK_USERS / MOCK_ANON_USERS | 50 / 10 | identified/anonymous 사용자 수 |
 | MOCK_MODELS | claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5 | 모델 목록 |
-| MOCK_RETENTION_DAYS | 90 | 이보다 오래된 date 요청 → 404 |
+| MOCK_RETENTION_DAYS | 90 | 이보다 오래된 date 요청 → 404 (`/v1/usage*`) |
+| MOCK_METRICS_RETENTION_DAYS | 14 | `/v1/metrics`: 이보다 오래된 date 요청 → 404 (계약 보존 14일) |
 
 ## 시나리오 주입 (계약 밖, 테스트 전용)
 
@@ -29,10 +33,36 @@
 summary_extra_pct · name_drift · generated_at_change_at_page · not_ready_at_page
 (전부 OFF = 완전한 계약 준수 — CI conformance가 이 불변식을 검증)
 
+`/v1/metrics` 전용 int 플래그 6종(0=OFF, 1=ON; `_shared_gate`의 429/503·`not_ready_until_uptime_s`·
+`retry_after_s`·`name_drift`는 두 계약이 공유):
+
+| 플래그 | 1이면 | 수집기 §5.3 검증 항목 |
+|---|---|---|
+| metrics_gpu_hours_over | 첫 gpu 행 `gpuHours = gpuCount*24 + 10` | `hours_over_count` |
+| metrics_unknown_serving | `model="unknown", category="serving"` 행 1개 추가 | `unknown_violation` |
+| metrics_pct_non_monotone | 첫 serving 행 `ttftMs.p90 = p50 - 1` | `pct_non_monotone` |
+| metrics_dup_gpu_rows | 첫 gpu 행 복제본을 인덱스 1에 삽입(인접 중복) | `dup_merged` |
+| metrics_empty_gpu | `gpu: []` (serving만 있는 응답) | 케이스 E — `NODATA` 아님 |
+| metrics_engine_null | `engine: null` | engine 부재 허용 |
+
+## /v1/metrics (token-metric-api @6a552d2)
+
+    curl "http://127.0.0.1:8000/v1/metrics?date=$(date -d yesterday +%F)"
+
+- 단건 응답 `{date, serviceGroup, service, generatedAt, engine, gpu, serving}` — `app/datagen.py::build_metrics`가
+  같은 (seed, date, 시나리오)에서 항상 같은 본문을 만든다 (계약 C4 멱등성 · 수집기 E2E 기대치 산출의 근거 — Plan 6b T11
+  `collectors/token-metrics/tests/e2e/ci_expectations.py`가 이 함수를 import한다).
+- 기본 데이터(모델 3종): `gpu` 5행 = 모델당 `serving` 1행(H100, `gpuCount` 1..8, `gpuHours ≤ gpuCount×24`) +
+  첫 모델 `standby` 1행(1장·24.0h) + `model="unknown"` `test` 1행; `serving` 3행 = 모델당 `ttftMs`·`itlMs`(p50≤p90≤p95≤p99)·
+  `outputTps{p50}`; `engine` 고정 `{"type": "vllm", "version": "0.10.1"}`; `generatedAt` = 다음날 `T02:05:00+09:00`.
+- 응답 코드는 usage와 같은 `_date_gate` 규칙: 당일/미래/형식 오류 400 `invalid_date`, `date` 누락 400,
+  `MOCK_METRICS_RETENTION_DAYS`(14) 초과 404 `data_not_retained`, `not_ready_until_uptime_s` 안이면 409 `data_not_ready` + `Retry-After`,
+  429/503은 usage와 **같은 요청 카운터**를 공유한다.
+
 ## 검증
 
     python -m pytest tests/ -v      # 단위/계약 시맨틱
-    ./run_conformance.sh            # 스펙 레포의 conformance_check 통과
+    ./run_conformance.sh            # usage conformance_check 통과 + metrics check_metrics_api "FAIL 0"
 
 이미지 빌드·컨테이너 스모크는 CI의 image job에서 검증 (로컬 개발 머신에는 docker 없음).
```

검증:
```bash
cd /home/mini/github/token-data-pipeline/tools/mock-provider
wc -l README.md
grep -n "MOCK_METRICS_RETENTION_DAYS" README.md
grep -c "^| metrics_" README.md
grep -n "^## /v1/metrics" README.md
```
기대:
```
112 README.md
24:| MOCK_METRICS_RETENTION_DAYS | 14 | `/v1/metrics`: 이보다 오래된 date 요청 → 404 (계약 보존 14일) |
59:  `MOCK_METRICS_RETENTION_DAYS`(14) 초과 404 `data_not_retained`, `not_ready_until_uptime_s` 안이면 409 `data_not_ready` + `Retry-After`,
6
48:## /v1/metrics (token-metric-api @6a552d2)
```

- [ ] **Step 20: `.github/workflows/test-mock-provider.yml` — image job 컨테이너 스모크에 `/v1/metrics` curl 추가**

job `test`는 `./run_conformance.sh` step이 이미 있어 metrics 자가 검사가 자동 포함된다(Step 18) — step 추가 없음. job `image`의 `Container smoke test` run 블록(46행 usage summary curl과 47행 `docker stop mock-smoke` 사이)에 2행 삽입. 컨테이너는 `-p 18000:8000`으로 기동되므로 호스트 포트 18000:

```diff
@@ -44,4 +44,6 @@ jobs:
           done
           curl -sf http://127.0.0.1:18000/healthz
           curl -sf "http://127.0.0.1:18000/v1/usage/summary?date=$(TZ=Asia/Seoul date -d yesterday +%F)" | grep -q "CI Smoke Svc"
+          YDAY="$(TZ=Asia/Seoul date -d yesterday +%F)"
+          curl -fsS "http://127.0.0.1:18000/v1/metrics?date=${YDAY}" | grep -q '"gpu"'
           docker stop mock-smoke
```

`.dockerignore`가 `tests/`·`contract/`를 제외하므로 이미지에는 `app/`만 들어간다 — `build_metrics`는 `app/datagen.py`에 있으므로 컨테이너에서 그대로 응답한다(체커·yaml은 이미지에 불필요). `on.paths`는 이미 `tools/mock-provider/**`·이 워크플로 파일을 포함하므로 트리거 변경 없음.

검증(로컬 — 도커 없음이므로 yaml 구조·문자열만):
```bash
cd /home/mini/github/token-data-pipeline
python3 -c "import yaml; d=yaml.safe_load(open('.github/workflows/test-mock-provider.yml')); print(sorted(d['jobs']), len(d['jobs']['image']['steps']))"
grep -n "v1/metrics" .github/workflows/test-mock-provider.yml
bash -n <(python3 -c "import yaml; print(yaml.safe_load(open('.github/workflows/test-mock-provider.yml'))['jobs']['image']['steps'][2]['run'])") && echo RUN_BLOCK_SYNTAX_OK
```
기대:
```
['image', 'test'] 3
48:          curl -fsS "http://127.0.0.1:18000/v1/metrics?date=${YDAY}" | grep -q '"gpu"'
RUN_BLOCK_SYNTAX_OK
```

- [ ] **Step 21: 제로-디프 확인 + 커밋**

허용 경로 밖 무변경·기존 테스트 무수정을 확인한 뒤 한 커밋으로 묶는다:

```bash
cd /home/mini/github/token-data-pipeline
git diff --stat -- collectors mart assets tools/verify docs .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml
git status --porcelain -- tools .github .gitignore collectors
```
기대: 첫 명령 출력 없음(빈 줄 0). 두 번째 출력(이 태스크의 허용 경로로 한정 — `docs/` 아래의 무관한 untracked 파일은 제외)은 정확히 아래 11행(순서는 git 정렬):
```
 M .github/workflows/test-mock-provider.yml
 M tools/mock-provider/README.md
 M tools/mock-provider/app/config.py
 M tools/mock-provider/app/datagen.py
 M tools/mock-provider/app/main.py
 M tools/mock-provider/app/scenarios.py
 M tools/mock-provider/contract/SOURCE.md
 M tools/mock-provider/run_conformance.sh
?? tools/mock-provider/contract/tests/check_metrics_api.py
?? tools/mock-provider/contract/token-metric-api.yaml
?? tools/mock-provider/tests/test_metrics_api.py
```
(`M` 8행 + `??` 3행. 경로 한정이 없는 `git status --porcelain`에는 세션 시작 시 이미 있던 `?? docs/superpowers/specs/2026-08-31-token-metrics-ingest-design.md` 같은 무관한 행이 섞일 수 있다 — 그것은 이 태스크와 무관하며 커밋에 포함하지 않는다.) 기존 테스트 파일 4개(`tests/test_api.py`·`test_config.py`·`test_scenarios.py`·`test_datagen.py`)와 `tests/conftest.py`가 목록에 없어야 한다.

공개 레포 규칙 최종 grep(사내 호스트명·코드명·개인 이메일 0건 — 새로 추가·수정한 파일만 대상):
```bash
cd /home/mini/github/token-data-pipeline
git diff -- tools/mock-provider .github/workflows/test-mock-provider.yml | grep -n "^+" | grep -i -E "@[a-z0-9.-]+\.(com|net|io)|\.internal|\.corp" ; echo "grep_exit=$?"
grep -n -i -E "@[a-z0-9.-]+\.(com|net|io)|\.corp" tools/mock-provider/tests/test_metrics_api.py tools/mock-provider/contract/SOURCE.md; echo "grep_exit=$?"
```
기대: 두 번 모두 매치 행 없이 `grep_exit=1`. (`check_metrics_api.py` 원본 docstring의 `http://my-service.internal:8080` 예시는 스펙 레포 원문의 플레이스홀더이며 바이트 고정 복사 대상이므로 대상에서 제외 — 실제 사내 주소 아님.)

커밋:
```bash
cd /home/mini/github/token-data-pipeline
git add tools/mock-provider .github/workflows/test-mock-provider.yml
git commit -m "feat(mock): GET /v1/metrics 결정적 생성기·시나리오 6종·계약 벤더링 @6a552d2·conformance 추가 (Plan 6b T1)" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>" -m "Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
git show --stat --format="%s" HEAD
```
기대: 제목 1행 + `11 files changed` 요약(신규 3: `contract/tests/check_metrics_api.py` `contract/token-metric-api.yaml` `tests/test_metrics_api.py`; 수정 8). 커밋 후 `python3 -m pytest -q`(`58 passed`)와 `PORT=18777 ./run_conformance.sh`가 Step 16·18과 같은 출력을 내는지 마지막으로 한 번 더 확인.

**T1 설계 해석(footer "설계 해석"에 합칠 것 — outline이 정하지 않아 이 태스크에서 하나로 정한 항목):**
- `metrics_dup_gpu_rows=1`의 복제 행은 **인덱스 1에 삽입**(첫 2행 동일, 인접) — append가 아니다. 체커 B8은 위치 무관하게 (model,gpuType,category) 중복을 WARN으로 잡고, 수집기 T3 `dup_merged`는 합산이므로 인접이 테스트 단언(`gpu[0] == gpu[1]`)에 가장 단순하다.
- CI image smoke curl은 **호스트 포트 18000**(`docker run -p 18000:8000`의 호스트 쪽; outline 문구의 8000은 컨테이너 쪽) — 기존 usage smoke와 같은 포트.
- 409 테스트는 `not_ready_until_uptime_s = 10 ** 9`(outline의 3600 대신) — 느린 CI 러너에서도 uptime이 임계를 넘지 않게 하는 안전 여유이며 의미는 같다.
- `datagen.METRICS_GPU_TYPE = "H100"` 상수를 추가(outline은 문자열 리터럴만 언급) — gpu 행 5곳·시나리오 1곳이 같은 값을 쓰므로 상수화. `outputTps.p50` 범위는 `[5, 200]`로 고정(계약은 양수 float만 요구).
- 테스트 18개(outline 17 + `test_metrics_scenario_flags_via_endpoint` 1개 추가) — `set_scenario` 경로로 6플래그가 실제 응답에 반영되는지를 엔드포인트 레벨에서 한 번 더 고정한다.
- gpu 행 키 순서 `model, gpuType, category, gpuCount, gpuHours`(계약 yaml `GpuRecord` 프로퍼티 순서는 `model, gpuType, gpuCount, gpuHours, category` — 식별 3키를 앞에 모으는 쪽을 택했다; JSON 객체 키 순서는 계약상 의미 없음). T11 `ci_expectations.py`는 `json.dumps` 바이트 비교를 하지 않으므로 순서는 가독성·C4 바이트 동일성에만 영향.
- 벤더링 체커 실행 비트: 원본 `scripts/check_metrics_api.py`는 `-rw-rw-r--`이나 `contract/tests/`의 usage `conformance_check.py` 관례와 달리 `chmod +x`를 준다(`run_conformance.sh`는 `"${PYTHON}"`으로 호출하므로 비트와 무관; 내용 바이트·sha256은 동일).

---

### Task 2: 모듈 스캐폴드 — app/config.py(env + endpoints §4.3 로더) · app/events.py · requirements · conftest · endpoints.yaml

설계 §5.1(233-237) "클론" 규칙: `collectors/token-usage/app/{config,events}.py`를 **복사·개명**해 신규 모듈 안에 만든다(기존 모듈 import 0, 공용 패키지 추출 없음). §5.2(239-246) env 목록에서 `VM_PUSH_URL`·`MAX_PAGES`·`MAX_BUFFER_ROWS`·`NOT_READY_BUDGET_MINUTES`는 없고 `LOAD_BUDGET_S`·`FINAL_HOUR_KST`·`MAX_RESPONSE_BYTES`·`METRICS_MAX_MUTATIONS_PER_RUN`이 새로 들어간다. `CH_DB_FACT`/`CH_DB_DIM`은 `Config`가 아니라 T5 `app/writer.py`의 모듈 상수(`DB_FACT`/`DB_DIM`)가 읽는다(Global Constraints "DB 상수 2종만"). endpoints 로더는 §4.3(196-229) 레지스트리 12컬럼 중 `updated_at`을 뺀 11필드를 `ServiceEntry`로 싣고, T5 레지스트리 diff-sync가 그대로 쓰는 `dim_row`/`dim_key`를 제공한다.

**Files:**
- Create: `collectors/token-metrics/requirements.txt`, `collectors/token-metrics/requirements-dev.txt`, `collectors/token-metrics/conftest.py`, `collectors/token-metrics/endpoints.yaml`
- Create: `collectors/token-metrics/app/__init__.py`(빈 파일), `collectors/token-metrics/app/events.py`, `collectors/token-metrics/app/config.py`
- Create: `collectors/token-metrics/tests/__init__.py`(빈 파일)
- Test: `collectors/token-metrics/tests/test_events.py`, `collectors/token-metrics/tests/test_config.py`
- Modify: 없음 (`collectors/token-usage/**`는 zero-diff — 읽기만)

**Interfaces:**
- Consumes:
  - Plan 6a B — `gpu_data.dim_token_metrics_service` 12컬럼 순서 `service_group, service, base_url, enabled, api_since, coverage_since, until, expect_gpu, expect_serving, usage_includes_consumers, note, updated_at`(`collectors/token-metrics/ddl/company/dim_token_metrics_service.sql`), endpoints 키 10종 `serviceGroup, service, baseUrl, enabled, apiSince, coverageSince, until, expectGpu, expectServing, usageIncludesConsumers`(+ 선택 `note`).
  - Plan 6a T8 fixture `assets/model-catalog/fixtures/synthetic_endpoints_metrics.yaml`(서비스 2 — 로더 테스트 입력으로 재사용).
  - 기존 모듈 `collectors/token-usage/app/config.py`(`_int_env`·`load_config` 프록시/CA 3의미·`load_endpoints` 검증 순서)와 `app/events.py`(`Event(str, Enum)`·`CollectError`) 관용구 — 복제만, import 없음.
- Produces (T3~T11 전부 소비):
  - `app/config.py`
    - `_int_env(name: str, default: int) -> int` — 빈 문자열/미설정 = 기본값.
    - `@dataclass class Config`: `ch_host: str = "localhost"`, `ch_port: int = 8123`, `ch_user: str = "default"`, `ch_password: str = ""`, `ch_cluster: str = ""`, `endpoints_file: str = "endpoints.yaml"`, `soft_deadline_minutes: int = 40`, `load_budget_s: int = 1200`, `final_hour_kst: int = 9`, `max_response_bytes: int = 5_000_000`, `max_mutations_per_run: int = 45`, `https_proxy: str | None = None`, `api_verify: bool | str = True`. (`vm_push_url`·`max_pages`·`max_buffer_rows`·`not_ready_budget_minutes` 없음.)
    - `load_config() -> Config` — env `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD`, `CH_CLUSTER`, `ENDPOINTS_FILE`, `SOFT_DEADLINE_MINUTES`, `LOAD_BUDGET_S`, `FINAL_HOUR_KST`, `MAX_RESPONSE_BYTES`, `METRICS_MAX_MUTATIONS_PER_RUN`, `COLLECTOR_HTTPS_PROXY`(`os.environ.get` — 미설정 None=시스템 상속 / `""`=직접 연결 / 값=전용 프록시), `COLLECTOR_API_VERIFY`(`"false"` → `False`), `COLLECTOR_API_CA_BUNDLE`(경로 → `api_verify`). 로드 직후 불변식: `soft_deadline_minutes * 60 <= load_budget_s`이면 `ValueError("SOFT_DEADLINE_MINUTES*60 must exceed LOAD_BUDGET_S")`.
    - `@dataclass(frozen=True) class ServiceEntry`: `service_group: str`, `service: str`, `base_url: str`, `enabled: bool`, `api_since: date`, `coverage_since: date`, `until: date | None`, `expect_gpu: bool = True`, `expect_serving: bool = True`, `usage_includes_consumers: bool = False`, `note: str = ""`. 메서드 `dim_key(self) -> tuple` = `(service_group, service, base_url, int(enabled), api_since, coverage_since, until, int(expect_gpu), int(expect_serving), int(usage_includes_consumers), note)`(문자열은 strip, Date는 `date` 객체, `until` None 유지 — T5가 `_dist` 현재 행과 비교하는 11컬럼 키), `dim_row(self, updated_at: datetime) -> list` = `list(dim_key()) + [updated_at]`(12개 = DDL 컬럼 순서, T5 INSERT 값 행).
    - `load_endpoints(path: str) -> list[ServiceEntry]` — 필수 키 `serviceGroup, service, baseUrl, enabled`(부재 → `ValueError(f"services[{i}]: missing keys {missing}")`), 빈 값 → `ValueError(f"services[{i}]: empty serviceGroup/service/baseUrl")`, 중복 → `ValueError(f"services[{i}]: duplicate service '{service}'")`, `apiSince` 기본 `"2026-09-09"`, `coverageSince` 기본 `"2026-08-26"`, `until` 기본/`null` → `None`, 날짜는 `date.fromisoformat(str(v))`(실패 → `ValueError(f"services[{i}]: bad date {key}")`), `until < coverage_since` → `ValueError(f"services[{i}]: until before coverageSince")`, `expectGpu/expectServing` 기본 True·`usageIncludesConsumers` 기본 False(bool 아닌 값은 `bool(v)`), `note` 기본 `""`, `baseUrl`은 `rstrip("/")`, 알 수 없는 키(`type` 등) 무시, 빈 목록 → `ValueError("endpoints file has no services")`. `coverage_since > api_since`는 검증하지 않는다(허용).
    - 모듈 상수 `DEFAULT_API_SINCE = "2026-09-09"`, `DEFAULT_COVERAGE_SINCE = "2026-08-26"`(§4.3 기본값 — `load_endpoints`가 쓰는 유일한 정의처; 테스트가 인용).
  - `app/events.py`
    - `class Event(str, Enum)`: `NOT_READY = "not_ready"`, `RETRYABLE = "retryable"`, `PERMANENT_ERROR = "permanent_error"`, `RETENTION = "retention"`, `EMPTY = "empty"`, `INVARIANT_BROKEN = "invariant_broken"` — 값이 소문자인 이유: T6이 `FAILURE reason=<err.event.value>`로 마커 reason 어휘(`not_ready`·`retention`·`permanent_error`…)에 그대로 쓴다(기존 모듈은 대문자 값 — 클론이라도 값은 이 모듈 마커 계약을 따른다).
    - `class CollectError(Exception)`: `__init__(self, event: Event, message: str = "", retry_after_s: int = 0)`, 속성 `event`, `message`, `retry_after_s`; `str(err)`는 `f"{event.value}: {message}"`.
  - `conftest.py`: `sys.path.insert(0, str(Path(__file__).resolve().parent))` — `cd collectors/token-metrics && python3 -m pytest -q`에서 `from app.config import …`를 고정(T9·T10의 `tools/*.py` importlib 로드도 이 루트 기준).
  - `endpoints.yaml`: 설계 §4.3 217-229행 블록 그대로(Mock Service A 1항목 — ConfigMap `token-metrics-endpoints`의 기본 본문, T8 install.sh가 `--from-file=endpoints.yaml=`로 싣는다).
  - `requirements.txt`: `requests>=2.31,<3` · `pyyaml>=6,<7` · `clickhouse-connect>=0.7,<1`; `requirements-dev.txt`: `-r requirements.txt` · `pytest>=8`.

- [ ] **Step 1: 전제 확인(Plan 6a 산출물) + 의존성·conftest·패키지 파일**

Plan 6a 산출물 2개(레지스트리 DDL·합성 endpoints fixture)가 브랜치에 있어야 한다. 없으면 **중단·보고**(6a 파일을 대신 만들지 않는다).

Run:
```bash
cd /home/mini/github/token-data-pipeline
ls -l collectors/token-metrics/ddl/company/dim_token_metrics_service.sql assets/model-catalog/fixtures/synthetic_endpoints_metrics.yaml
grep -c "^    service: " assets/model-catalog/fixtures/synthetic_endpoints_metrics.yaml
ls collectors/token-metrics/app collectors/token-metrics/tests 2>&1 | head -2
```
Expected:
```text
-rw-r--r-- … collectors/token-metrics/ddl/company/dim_token_metrics_service.sql
-rw-r--r-- … assets/model-catalog/fixtures/synthetic_endpoints_metrics.yaml
2
ls: cannot access 'collectors/token-metrics/app': No such file or directory
```
(마지막 줄 = 아직 app/·tests/가 없음 — 이 태스크가 만든다.)

`collectors/token-metrics/requirements.txt`:

```text
requests>=2.31,<3
pyyaml>=6,<7
clickhouse-connect>=0.7,<1
```

`collectors/token-metrics/requirements-dev.txt`:

```text
-r requirements.txt
pytest>=8
```

`collectors/token-metrics/conftest.py`(모듈 루트를 sys.path 앞에 고정 — `from app.config import …`가 cwd·import-mode와 무관하게 동작):

```python
"""pytest 루트 conftest — collectors/token-metrics/ 를 import 루트로 고정 (tests/__init__.py 와 한 쌍)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
```

빈 패키지 파일 2개: `collectors/token-metrics/app/__init__.py`, `collectors/token-metrics/tests/__init__.py`(둘 다 0바이트 — 기존 모듈과 동일).

Run:
```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
mkdir -p app tests
: > app/__init__.py
: > tests/__init__.py
pip install -r requirements-dev.txt -q
python3 -c "import yaml, requests, clickhouse_connect, pytest; print('deps OK')"
wc -c app/__init__.py tests/__init__.py
```
Expected:
```text
deps OK
0 app/__init__.py
0 tests/__init__.py
0 total
```

- [ ] **Step 2: 합성 `endpoints.yaml`(설계 §4.3 217-229행 블록 그대로)**

`collectors/token-metrics/endpoints.yaml`:

```yaml
# 메트릭 레지스트리 정본 (설계 2026-08-31 §4.3) — ConfigMap token-metrics-endpoints 의 기본 본문.
# 사내 실파일은 collectors/token-metrics/endpoints-metrics.company.yaml (gitignore) — 이 파일은 합성 예시만 담는다.
# 폐기 서비스는 enabled: false 로 유지, 항목 제거 금지 (until 로 마지막 데이터 날짜를 고정).
services:
  - serviceGroup: "Mock Group"          # 토큰 레지스트리와 바이트 동일
    service: "Mock Service A"
    baseUrl: "http://token-mock-provider-a.monitoring.svc:8000"
    enabled: true
    apiSince: "2026-09-09"              # 정기 API 수집 시작 데이터 날짜
    coverageSince: "2026-08-26"         # 커버리지 기대 시작(manual 포함)
    until: null
    expectGpu: true
    expectServing: true
    usageIncludesConsumers: false       # 플랫폼 제공자만 true
```

Run:
```bash
cd /home/mini/github/token-data-pipeline
python3 -c "
import yaml; d = yaml.safe_load(open('collectors/token-metrics/endpoints.yaml', encoding='utf-8'))
s = d['services']; assert len(s) == 1 and s[0]['service'] == 'Mock Service A' and s[0]['until'] is None
assert sorted(s[0]) == ['apiSince', 'baseUrl', 'coverageSince', 'enabled', 'expectGpu', 'expectServing', 'service', 'serviceGroup', 'until', 'usageIncludesConsumers']
print('endpoints.yaml OK', len(s))"
git check-ignore -v collectors/token-metrics/endpoints.yaml; echo "check-ignore exit=$? (1 = 무시 안 됨 = 커밋 대상)"
```
Expected:
```text
endpoints.yaml OK 1
check-ignore exit=1 (1 = 무시 안 됨 = 커밋 대상)
```

- [ ] **Step 3: 실패하는 테스트 — `collectors/token-metrics/tests/test_events.py`**

```python
import pytest

from app.events import CollectError, Event


def test_event_values():
    # T6 이 FAILURE reason=<event.value> 로 마커에 그대로 쓴다 — 소문자 어휘 고정
    assert Event.NOT_READY.value == "not_ready"
    assert Event.RETRYABLE.value == "retryable"
    assert Event.PERMANENT_ERROR.value == "permanent_error"
    assert Event.RETENTION.value == "retention"
    assert Event.EMPTY.value == "empty"
    assert Event.INVARIANT_BROKEN.value == "invariant_broken"
    assert len(Event) == 6
    assert isinstance(Event.NOT_READY, str)          # str 혼합 Enum (StrEnum 미사용 — 3.10 호환)
    assert Event("retention") is Event.RETENTION


def test_collect_error_defaults():
    err = CollectError(Event.RETRYABLE)
    assert err.event is Event.RETRYABLE
    assert err.message == ""
    assert err.retry_after_s == 0
    assert "retryable" in str(err)
    assert isinstance(err, Exception)


def test_collect_error_carries_message_and_retry_after():
    err = CollectError(Event.NOT_READY, "data_not_ready", retry_after_s=900)
    assert err.retry_after_s == 900
    assert str(err) == "not_ready: data_not_ready"
    with pytest.raises(CollectError) as ei:
        raise err
    assert ei.value.event is Event.NOT_READY
```

- [ ] **Step 4: 실패 확인**

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -m pytest tests/test_events.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.events'`

- [ ] **Step 5: 구현 — `collectors/token-metrics/app/events.py`**(기존 모듈 D5.2 복제 — docstring·값 어휘만 이 모듈 계약으로)

```python
"""공통 이벤트 분류 (설계 2026-08-31 §5.2 모드×게이트 표의 분류 축 — token-metrics 클론).

정책(대기열·재방문·final 판정·status 매핑·exit 영향)은 main.py 오케스트레이터에 1벌만 존재한다.
api_client 는 HTTP 신호를 이 분류로 번역만 한다. 값은 소문자 — main 이 `FAILURE reason=<value>` 로
마커 reason 어휘(not_ready · retention · permanent_error …)에 그대로 쓴다.
"""
from enum import Enum


class Event(str, Enum):  # StrEnum 은 3.11+ — 3.10 호환 형태 사용
    NOT_READY = "not_ready"                # 409: 큐 끝 1회 재방문, 재차 409 → 비최종 SKIPPED / 최종 FAILURE
    RETRYABLE = "retryable"                # 429/5xx/네트워크: 내부 재시도 3회 소진 후 FAILURE
    PERMANENT_ERROR = "permanent_error"    # 400 / >5MB / date 에코 불일치 / non-JSON / 구조 위반: 즉시 FAILURE
    RETENTION = "retention"                # 404: 정기 FAILURE / rerun SKIPPED
    EMPTY = "empty"                        # gpu:[] AND serving:[] — NODATA (summary 앵커는 적재)
    INVARIANT_BROKEN = "invariant_broken"  # 적재 중 불변식 위반 — 폐기 후 재시작


class CollectError(Exception):
    def __init__(self, event: Event, message: str = "", retry_after_s: int = 0):
        super().__init__(f"{event.value}: {message}")
        self.event = event
        self.message = message
        self.retry_after_s = retry_after_s
```

- [ ] **Step 6: 통과 확인**

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -m pytest tests/test_events.py -q`
Expected: `3 passed`

- [ ] **Step 7: 실패하는 테스트 — `collectors/token-metrics/tests/test_config.py`**(기존 D6.1의 4테스트 관용구 + §4.3 필드·§5.2 불변식)

```python
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.config import (
    DEFAULT_API_SINCE, DEFAULT_COVERAGE_SINCE, Config, ServiceEntry, load_config, load_endpoints,
)

KST = timezone(timedelta(hours=9))
REPO_ROOT = Path(__file__).resolve().parents[3]           # tests/ → token-metrics/ → collectors/ → 레포 루트
FIXTURE = REPO_ROOT / "assets" / "model-catalog" / "fixtures" / "synthetic_endpoints_metrics.yaml"
ENV_KEYS = ("CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_CLUSTER", "ENDPOINTS_FILE",
            "SOFT_DEADLINE_MINUTES", "LOAD_BUDGET_S", "FINAL_HOUR_KST", "MAX_RESPONSE_BYTES",
            "METRICS_MAX_MUTATIONS_PER_RUN",
            "COLLECTOR_HTTPS_PROXY", "COLLECTOR_API_VERIFY", "COLLECTOR_API_CA_BUNDLE")
MINIMAL = (
    "services:\n"
    "  - serviceGroup: \"Mock Group\"\n    service: \"Mock Service A\"\n"
    "    baseUrl: \"http://mock\"\n    enabled: true\n"
)


def _clear_env(monkeypatch):
    for k in ENV_KEYS:
        monkeypatch.delenv(k, raising=False)


def test_config_defaults(monkeypatch):
    _clear_env(monkeypatch)
    cfg = load_config()
    assert cfg.ch_host == "localhost" and cfg.ch_port == 8123 and cfg.ch_user == "default"
    assert cfg.ch_password == ""
    assert cfg.ch_cluster == ""              # 빈 값 = ON CLUSTER 생략
    assert cfg.endpoints_file == "endpoints.yaml"
    assert cfg.soft_deadline_minutes == 40   # §5.2
    assert cfg.load_budget_s == 1200         # §5.2
    assert cfg.final_hour_kst == 9           # §5.2 최종 슬롯
    assert cfg.max_response_bytes == 5_000_000
    assert cfg.max_mutations_per_run == 45   # §4.0 뮤테이션 장부
    assert cfg.https_proxy is None           # 미설정 = 시스템 상속
    assert cfg.api_verify is True
    # 클론에서 제거된 필드 — VM push·페이지네이션·버퍼·NOT_READY 예산 없음 (§5.1·§5.2)
    for gone in ("vm_push_url", "max_pages", "max_buffer_rows", "not_ready_budget_minutes"):
        assert not hasattr(cfg, gone), gone


def test_soft_deadline_exceeds_load_budget(monkeypatch):
    # §5.2 불변식 SOFT×60 > LOAD_BUDGET — 기본값(2400 > 1200)과 dataclass 기본값 양쪽에서 고정
    _clear_env(monkeypatch)
    cfg = load_config()
    assert cfg.soft_deadline_minutes * 60 > cfg.load_budget_s
    assert cfg.soft_deadline_minutes * 60 == 2400 and cfg.load_budget_s == 1200
    assert Config().soft_deadline_minutes * 60 > Config().load_budget_s


def test_load_config_rejects_budget_over_deadline(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("SOFT_DEADLINE_MINUTES", "10")
    monkeypatch.setenv("LOAD_BUDGET_S", "1200")
    with pytest.raises(ValueError, match="SOFT_DEADLINE_MINUTES\\*60 must exceed LOAD_BUDGET_S"):
        load_config()
    monkeypatch.setenv("SOFT_DEADLINE_MINUTES", "20")     # 1200 == 1200 도 거부 (<=)
    with pytest.raises(ValueError):
        load_config()


def test_env_override(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("METRICS_MAX_MUTATIONS_PER_RUN", "3")
    monkeypatch.setenv("FINAL_HOUR_KST", "8")
    monkeypatch.setenv("MAX_RESPONSE_BYTES", "100")
    monkeypatch.setenv("CH_HOST", "chi-gpu-monitoring.clickhouse.svc")
    monkeypatch.setenv("CH_PORT", "8124")
    monkeypatch.setenv("CH_USER", "mart")
    monkeypatch.setenv("CH_CLUSTER", "gpu-monitoring")
    monkeypatch.setenv("ENDPOINTS_FILE", "/etc/token-metrics/endpoints.yaml")
    monkeypatch.setenv("SOFT_DEADLINE_MINUTES", "")         # 빈 문자열 = 기본값 (_int_env)
    cfg = load_config()
    assert cfg.max_mutations_per_run == 3
    assert cfg.final_hour_kst == 8
    assert cfg.max_response_bytes == 100
    assert cfg.ch_host == "chi-gpu-monitoring.clickhouse.svc" and cfg.ch_port == 8124
    assert cfg.ch_user == "mart" and cfg.ch_cluster == "gpu-monitoring"
    assert cfg.endpoints_file == "/etc/token-metrics/endpoints.yaml"
    assert cfg.soft_deadline_minutes == 40


def test_proxy_and_verify_semantics(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("COLLECTOR_HTTPS_PROXY", "")      # 빈 문자열 = 프록시 무시(직접)
    monkeypatch.setenv("COLLECTOR_API_CA_BUNDLE", "/etc/ca.pem")
    cfg = load_config()
    assert cfg.https_proxy == ""
    assert cfg.api_verify == "/etc/ca.pem"
    monkeypatch.setenv("COLLECTOR_API_VERIFY", "false")
    assert load_config().api_verify is False
    monkeypatch.setenv("COLLECTOR_HTTPS_PROXY", "http://proxy.example.internal:3128")
    assert load_config().https_proxy == "http://proxy.example.internal:3128"


def test_load_endpoints_defaults(tmp_path):
    p = tmp_path / "eps.yaml"
    p.write_text(MINIMAL, encoding="utf-8")
    eps = load_endpoints(str(p))
    assert len(eps) == 1
    e = eps[0]
    assert e.service_group == "Mock Group" and e.service == "Mock Service A"
    assert e.base_url == "http://mock" and e.enabled is True
    assert e.api_since == date(2026, 9, 9) == date.fromisoformat(DEFAULT_API_SINCE)          # §4.3 기본
    assert e.coverage_since == date(2026, 8, 26) == date.fromisoformat(DEFAULT_COVERAGE_SINCE)
    assert e.until is None
    assert e.expect_gpu and e.expect_serving
    assert not e.usage_includes_consumers
    assert e.note == ""
    assert isinstance(e, ServiceEntry)


def test_load_endpoints_full_fields(tmp_path):
    p = tmp_path / "eps.yaml"
    p.write_text(
        "services:\n"
        "  - serviceGroup: \"Mock Group\"          # 토큰 레지스트리와 바이트 동일\n"
        "    service: \"Mock Service A\"\n"
        "    baseUrl: \"http://token-mock-provider-a.monitoring.svc:8000/\"\n"
        "    enabled: true\n"
        "    apiSince: 2026-09-09\n"                 # 따옴표 없는 YAML 날짜도 허용
        "    coverageSince: \"2026-08-26\"\n"
        "    until: \"2026-12-31\"\n"
        "    expectGpu: false\n"
        "    expectServing: true\n"
        "    usageIncludesConsumers: true\n"
        "    note: \"platform provider\"\n",
        encoding="utf-8",
    )
    e = load_endpoints(str(p))[0]
    assert e.base_url == "http://token-mock-provider-a.monitoring.svc:8000"   # trailing '/' 제거
    assert e.api_since == date(2026, 9, 9) and e.coverage_since == date(2026, 8, 26)
    assert e.until == date(2026, 12, 31)
    assert e.expect_gpu is False and e.expect_serving is True
    assert e.usage_includes_consumers is True
    assert e.note == "platform provider"


def test_load_endpoints_synthetic_fixture():
    # Plan 6a T8 산출물 — 6b 로더가 그대로 읽어야 한다 (설계 §4.3 형식의 정본 예시)
    assert FIXTURE.exists(), f"Plan 6a T8 fixture 부재: {FIXTURE}"
    eps = load_endpoints(str(FIXTURE))
    assert len(eps) >= 1
    services = [e.service for e in eps]
    assert len(services) == len(set(services))
    assert all(e.service_group == "Mock Group" for e in eps)
    assert all(e.api_since == date(2026, 9, 9) and e.until is None for e in eps)


def test_load_endpoints_rejects_bad(tmp_path):
    def load(text: str):
        p = tmp_path / "bad.yaml"
        p.write_text(text, encoding="utf-8")
        return load_endpoints(str(p))

    with pytest.raises(ValueError, match=r"services\[0\]: missing keys \['service'\]"):
        load("services:\n  - {serviceGroup: G, baseUrl: 'http://a', enabled: true}\n")
    with pytest.raises(ValueError, match="duplicate service 'S'"):
        load("services:\n"
             "  - {serviceGroup: G, service: S, baseUrl: 'http://a', enabled: true}\n"
             "  - {serviceGroup: G, service: S, baseUrl: 'http://b', enabled: true}\n")
    with pytest.raises(ValueError, match="empty serviceGroup/service/baseUrl"):
        load("services:\n  - {serviceGroup: '', service: S, baseUrl: 'http://a', enabled: true}\n")
    with pytest.raises(ValueError, match=r"services\[0\]: bad date apiSince"):
        load("services:\n  - {serviceGroup: G, service: S, baseUrl: 'http://a', enabled: true, apiSince: '2026-13-01'}\n")
    with pytest.raises(ValueError, match=r"services\[0\]: bad date until"):
        load("services:\n  - {serviceGroup: G, service: S, baseUrl: 'http://a', enabled: true, until: 'soon'}\n")
    with pytest.raises(ValueError, match=r"services\[0\]: until before coverageSince"):
        load("services:\n  - {serviceGroup: G, service: S, baseUrl: 'http://a', enabled: true,"
             " coverageSince: '2026-08-26', until: '2026-08-25'}\n")
    with pytest.raises(ValueError, match=r"services\[1\]: not a mapping"):
        load("services:\n  - {serviceGroup: G, service: S, baseUrl: 'http://a', enabled: true}\n  - just-a-string\n")
    with pytest.raises(ValueError, match="endpoints file has no services"):
        load("services: []\n")
    with pytest.raises(ValueError, match="endpoints file has no services"):
        load("# 빈 문서\n")


def test_load_endpoints_unknown_keys_ignored_and_coverage_after_since_allowed(tmp_path):
    p = tmp_path / "eps.yaml"
    p.write_text(
        "services:\n"
        "  - {serviceGroup: G, service: S, baseUrl: 'http://a', enabled: true,"
        " type: usage-api-v1, foo: 1, apiSince: '2026-09-01', coverageSince: '2026-09-15'}\n",
        encoding="utf-8",
    )
    e = load_endpoints(str(p))[0]
    assert not hasattr(e, "source_type")             # 기존 모듈의 type→source_type 은 클론에서 제거
    assert e.coverage_since > e.api_since             # 허용 — 검증 항목 아님


def test_dim_row_and_key_shapes():
    entry = ServiceEntry(
        service_group=" Mock Group ", service="Mock Service A", base_url="http://mock", enabled=True,
        api_since=date(2026, 9, 9), coverage_since=date(2026, 8, 26), until=None,
        expect_gpu=True, expect_serving=False, usage_includes_consumers=False, note=" ops ",
    )
    now = datetime(2026, 9, 11, 2, 5, tzinfo=KST)
    row = entry.dim_row(now)
    assert len(row) == 12                                  # DDL 12컬럼 순서
    assert row[0] == "Mock Group" and row[1] == "Mock Service A" and row[2] == "http://mock"
    assert row[3] == 1                                     # enabled → UInt8
    assert row[4] == date(2026, 9, 9) and row[5] == date(2026, 8, 26) and row[6] is None
    assert row[7] == 1 and row[8] == 0 and row[9] == 0     # expect_gpu / expect_serving / usage_includes_consumers
    assert row[10] == "ops"
    assert row[11] is now                                  # updated_at 마지막
    key = entry.dim_key()
    assert len(key) == 11
    assert all(not isinstance(v, datetime) for v in key)   # updated_at 미포함
    assert key == tuple(row[:11])
    assert isinstance(key, tuple)
```

- [ ] **Step 8: 실패 확인**

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -m pytest tests/test_config.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.config'`

- [ ] **Step 9: 구현 — `collectors/token-metrics/app/config.py`**(기존 D5.1 복제 — `_int_env`·프록시/CA 3의미·검증 순서 그대로, 필드만 §4.3·§5.2)

```python
"""env + endpoints.yaml 로더 — collectors/token-usage/app/config.py 의 클론 (설계 2026-08-31 §5.1).

Config      : §5.2 env 목록. VM push·페이지네이션·버퍼·NOT_READY 예산 항목은 없고
              LOAD_BUDGET_S / FINAL_HOUR_KST / MAX_RESPONSE_BYTES / METRICS_MAX_MUTATIONS_PER_RUN 이 추가.
              불변식 SOFT_DEADLINE_MINUTES*60 > LOAD_BUDGET_S 를 load_config 가 강제한다.
              DB 명(CH_DB_FACT / CH_DB_DIM)은 여기 없다 — app/writer.py 모듈 상수 DB_FACT / DB_DIM 이 읽는다.
ServiceEntry: 레지스트리 gpu_data.dim_token_metrics_service 의 updated_at 제외 11컬럼 (§4.3).
              dim_key() = diff-sync 비교 키, dim_row(updated_at) = INSERT 값 행 12개 (DDL 컬럼 순서).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime

import yaml

DEFAULT_API_SINCE = "2026-09-09"        # §4.3 — go-live 첫 데이터 날짜 (정기 API 수집 게이트)
DEFAULT_COVERAGE_SINCE = "2026-08-26"   # §4.3 — M0 커버리지 기대 시작일 (manual 포함)
_REQUIRED_KEYS = ("serviceGroup", "service", "baseUrl", "enabled")


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    return int(raw) if raw.strip() else default


@dataclass
class Config:
    ch_host: str = "localhost"
    ch_port: int = 8123
    ch_user: str = "default"
    ch_password: str = ""
    ch_cluster: str = ""                 # 빈 값 = 단일노드, ON CLUSTER 생략 (§4.0)
    endpoints_file: str = "endpoints.yaml"
    soft_deadline_minutes: int = 40      # §5.2 — 2400s = 신규 착수·409 재방문 창 + 예약된 적재 예산
    load_budget_s: int = 1200            # §5.2 — 데드라인 앞에 예약된 적재 예산 (SOFT*60 > LOAD 불변식)
    final_hour_kst: int = 9              # §5.2 — batch_time KST hour >= 값 이면 최종 슬롯 (final=1)
    max_response_bytes: int = 5_000_000  # §5.2 — 본문 > 5MB 는 PERMANENT_ERROR
    max_mutations_per_run: int = 45      # §4.0 뮤테이션 장부 — 예정 DELETE 수 초과 시 reason=mutation_budget
    https_proxy: str | None = None       # None=상속, ''=직접 연결, 값=전용 프록시
    api_verify: bool | str = True        # False | True | CA bundle 경로


def load_config() -> Config:
    verify_raw = os.getenv("COLLECTOR_API_VERIFY", "")
    ca_bundle = os.getenv("COLLECTOR_API_CA_BUNDLE", "")
    api_verify: bool | str = True
    if verify_raw.strip().lower() == "false":
        api_verify = False
    elif ca_bundle.strip():
        api_verify = ca_bundle.strip()
    cfg = Config(
        ch_host=os.getenv("CH_HOST", "localhost"),
        ch_port=_int_env("CH_PORT", 8123),
        ch_user=os.getenv("CH_USER", "default"),
        ch_password=os.getenv("CH_PASSWORD", ""),
        ch_cluster=os.getenv("CH_CLUSTER", ""),
        endpoints_file=os.getenv("ENDPOINTS_FILE", "endpoints.yaml"),
        soft_deadline_minutes=_int_env("SOFT_DEADLINE_MINUTES", 40),
        load_budget_s=_int_env("LOAD_BUDGET_S", 1200),
        final_hour_kst=_int_env("FINAL_HOUR_KST", 9),
        max_response_bytes=_int_env("MAX_RESPONSE_BYTES", 5_000_000),
        max_mutations_per_run=_int_env("METRICS_MAX_MUTATIONS_PER_RUN", 45),
        https_proxy=os.environ.get("COLLECTOR_HTTPS_PROXY"),
        api_verify=api_verify,
    )
    # §5.2 불변식: 소프트 데드라인(신규 착수·409 재방문 창)이 적재 예산보다 커야 예산 예약이 성립한다.
    if cfg.soft_deadline_minutes * 60 <= cfg.load_budget_s:
        raise ValueError("SOFT_DEADLINE_MINUTES*60 must exceed LOAD_BUDGET_S")
    return cfg


@dataclass(frozen=True)
class ServiceEntry:
    service_group: str
    service: str
    base_url: str
    enabled: bool
    api_since: date
    coverage_since: date
    until: date | None
    expect_gpu: bool = True
    expect_serving: bool = True
    usage_includes_consumers: bool = False
    note: str = ""

    def dim_key(self) -> tuple:
        """레지스트리 diff 비교 키 = updated_at 제외 11컬럼 (§4.3) — DDL 컬럼 순서·타입(UInt8→int, Date→date)."""
        return (
            self.service_group.strip(), self.service.strip(), self.base_url.strip(), int(self.enabled),
            self.api_since, self.coverage_since, self.until,
            int(self.expect_gpu), int(self.expect_serving), int(self.usage_includes_consumers),
            self.note.strip(),
        )

    def dim_row(self, updated_at: datetime) -> list:
        """INSERT 값 행 12개 (DDL 컬럼 순서) — updated_at 은 aware KST datetime (writer 의 now_kst())."""
        return list(self.dim_key()) + [updated_at]


def _date_field(item: dict, key: str, default: str | None, i: int) -> date | None:
    """YYYY-MM-DD 문자열 또는 YAML date → date. 부재·null·빈 문자열은 default (None 이면 None)."""
    raw = item.get(key)
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        raw = default
    if raw is None:
        return None
    try:
        return date.fromisoformat(str(raw).strip())
    except ValueError as exc:
        raise ValueError(f"services[{i}]: bad date {key}") from exc


def load_endpoints(path: str) -> list[ServiceEntry]:
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    entries: list[ServiceEntry] = []
    seen: set[str] = set()
    for i, item in enumerate((doc or {}).get("services") or []):
        if not isinstance(item, dict):
            raise ValueError(f"services[{i}]: not a mapping")
        missing = [k for k in _REQUIRED_KEYS if k not in item]
        if missing:
            raise ValueError(f"services[{i}]: missing keys {missing}")
        group = str(item["serviceGroup"]).strip()
        service = str(item["service"]).strip()
        base_url = str(item["baseUrl"]).strip()
        if not group or not service or not base_url:
            raise ValueError(f"services[{i}]: empty serviceGroup/service/baseUrl")
        if service in seen:
            raise ValueError(f"services[{i}]: duplicate service '{service}'")
        seen.add(service)
        api_since = _date_field(item, "apiSince", DEFAULT_API_SINCE, i)
        coverage_since = _date_field(item, "coverageSince", DEFAULT_COVERAGE_SINCE, i)
        until = _date_field(item, "until", None, i)
        if until is not None and until < coverage_since:
            raise ValueError(f"services[{i}]: until before coverageSince")
        # coverage_since > api_since 는 허용 (검증하지 않음 — §4.3 두 날짜는 독립 게이트)
        entries.append(ServiceEntry(
            service_group=group, service=service, base_url=base_url.rstrip("/"),
            enabled=bool(item["enabled"]),
            api_since=api_since, coverage_since=coverage_since, until=until,
            expect_gpu=bool(item.get("expectGpu", True)),
            expect_serving=bool(item.get("expectServing", True)),
            usage_includes_consumers=bool(item.get("usageIncludesConsumers", False)),
            note=str(item.get("note") or "").strip(),
        ))   # 알 수 없는 키(type 등)는 무시
    if not entries:
        raise ValueError("endpoints file has no services")
    return entries
```

- [ ] **Step 10: 통과 확인(모듈 전체 회귀 + 합성 파일 2종 로드)**

Run:
```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
python3 -m pytest -q
python3 -c "
from app.config import load_endpoints
a = load_endpoints('endpoints.yaml'); b = load_endpoints('../../assets/model-catalog/fixtures/synthetic_endpoints_metrics.yaml')
print('endpoints.yaml', [e.service for e in a], a[0].api_since, a[0].coverage_since, a[0].until)
print('fixture', [(e.service, e.expect_serving) for e in b])"
```
Expected:
```text
14 passed
endpoints.yaml ['Mock Service A'] 2026-09-09 2026-08-26 None
fixture [('Mock Service A', True), ('Mock Service B', False)]
```

- [ ] **Step 11: zero-diff 확인 + Commit**

```bash
cd /home/mini/github/token-data-pipeline
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml
# 기대: 출력 없음 (zero-diff)
git status --short collectors/token-metrics
# 기대: ?? 항목만 — app/ tests/ conftest.py endpoints.yaml requirements.txt requirements-dev.txt (ddl/ 은 Plan 6a 커밋분이라 표시 없음)
git add collectors/token-metrics/requirements.txt collectors/token-metrics/requirements-dev.txt \
        collectors/token-metrics/conftest.py collectors/token-metrics/endpoints.yaml \
        collectors/token-metrics/app/__init__.py collectors/token-metrics/app/config.py collectors/token-metrics/app/events.py \
        collectors/token-metrics/tests/__init__.py collectors/token-metrics/tests/test_config.py collectors/token-metrics/tests/test_events.py
git commit -m "feat(collectors-metrics): 모듈 스캐폴드 — Config(env·예산 불변식)·endpoints §4.3 로더·Event/CollectError (Plan 6b T2)

collectors/token-usage 클론(설계 §5.1, import 0). Config 는 §5.2 env 14종 + SOFT_DEADLINE_MINUTES*60 > LOAD_BUDGET_S 불변식,
ServiceEntry 는 레지스트리 dim_token_metrics_service 의 updated_at 제외 11컬럼(dim_key/dim_row). Event 값은 소문자 마커 reason 어휘.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

**T2 설계 해석(footer "Self-Review 노트"에 합칠 것):**
- `Event` 값은 기존 모듈(대문자)과 달리 **소문자** — T6 마커 `reason=<event.value>`가 §5.2 reason 어휘(`not_ready`·`retention`…)와 바이트 일치해야 하므로 클론에서 값만 바꿨다(이름·형태·`CollectError` 시그니처는 동일, `message` 기본값 `""`만 추가).
- `conftest.py`는 기존 모듈(0바이트)과 달리 `sys.path.insert` 1줄 — 기존 모듈은 pytest prepend import-mode의 rootdir 삽입에 의존하는데, T9·T10이 `tools/*.py`를 importlib로 읽는 테스트를 같은 루트에서 돌리므로 명시 고정을 택했다(동작은 상위 호환).
- endpoints 날짜 필드의 빈 문자열(`until: ""`)은 `null`과 동일하게 기본값 처리(설계는 `null`만 예시) — 사내 파일 편집 실수를 오류 대신 기본값으로 흡수; 형식 오류(`"soon"`, `"2026-13-01"`)는 그대로 `bad date`.
- `CH_DB_FACT`/`CH_DB_DIM`은 `Config` 밖(T5 writer 모듈 상수) — Global Constraints "DB 상수 2종만"과 기존 `clickhouse_client.py:16-17` 관용구를 따랐다.

---

### Task 3: app/normalize.py — 3계층 정규화(순수 함수, TDD) · MetricsPayload · long-form serving 행

**Files:**
- Create: `collectors/token-metrics/app/normalize.py`
- Test: `collectors/token-metrics/tests/test_normalize.py`
- Modify: 없음 (기존 모듈 `collectors/token-usage/app/normalize.py`는 스타일 참고만 — import·복사 없음)

**설계 근거:** §5.3(260-266) 3계층 전 규칙 · §5.2 표 "200" 행(`NODATA` = rows==0 AND rejected==0, 케이스 E = gpu:[] + serving 행 → SUCCESS, rows==0 AND rejected>0 → `all_rows_rejected`는 T6가 붙임) · §4.1 fact 컬럼 도메인(Plan 6a A — `metric` API 키→`ttft_ms|itl_ms|e2e_ms|output_tps|custom`, `unit ∈ ms|tokens/s|<custom ≤32>`, gpu `flags ∈ hours_over_count|unknown_violation|dup_merged`, serving `flags ∈ pct_non_monotone|unknown_violation|dup_model_kept_first|dup_custom_kept_first`) · §5.5 manual 규칙(같은 normalize 경로, `source_type='manual-v0'`, `generated_at` 빈 값 = 적재 시각, `identity_drift`는 API만) · 계약 스키마 `token-metric-api.yaml` @6a552d2 268-490행(GpuRecord 필수키·maxLength 128/64, LatencyPercentiles 4키 필수·`additionalProperties: false`·`minimum: 0`, OutputTps p50만, CustomMetric name≤64·unit≤32·p키≥1·`additionalProperties: false`(p값 `minimum` 없음 → 음수 허용), ServingRecord `additionalProperties: false`·`minProperties: 2`, Engine type≤64/version≤64, MetricsReport 필수키 6종).

**Interfaces:**
- Consumes:
  - T2 `app.config.ServiceEntry`(frozen dataclass: `service_group: str, service: str, base_url: str, enabled: bool, api_since: date, coverage_since: date, until: date | None, expect_gpu: bool = True, expect_serving: bool = True, usage_includes_consumers: bool = False, note: str = ""`) — `normalize_payload`는 `service_group`/`service`만 읽는다(reported_* 대조). 런타임 import 없음(`TYPE_CHECKING` 가드) → 이 모듈은 `pyyaml`·`requests`·`clickhouse_connect` 없이 import 된다.
  - T2 `conftest.py`(루트 `sys.path`) + `tests/__init__.py`.
  - Plan 6a A fact 컬럼 도메인(위 설계 근거), T1 벤더링 계약 스키마(키·maxLength — 이 태스크는 상수로 고정하며 파일을 읽지 않는다).
- Produces (T4 `check_report_structure/PayloadError/MetricsPayload` · T5 `MetricsPayload/NormalizeResult/GpuRow/ServingRow/KST/SOURCE_API` · T6 `normalize_payload/check_report_structure/PayloadError` + `NormalizeResult.is_nodata/rows/warn_total/rejected/warns` · T7 `MetricsPayload/normalize_payload/SOURCE_MANUAL/LATENCY_KEYS/PCT_KEYS` 소비):
  - 상수: `EPS = 1e-6`, `KST = timezone(timedelta(hours=9))`, `CATEGORIES = ("serving", "standby", "test")`, `PCT_KEYS = ("p50", "p90", "p95", "p99")`, `LATENCY_KEYS = {"ttftMs": "ttft_ms", "itlMs": "itl_ms", "e2eMs": "e2e_ms"}`, `SERVING_ALLOWED_KEYS = {"model", "ttftMs", "itlMs", "e2eMs", "outputTps", "custom"}`, `CUSTOM_ALLOWED_KEYS = {"name", "unit"} | set(PCT_KEYS)`, `REPORT_REQUIRED_KEYS = ("date", "serviceGroup", "service", "generatedAt", "gpu", "serving")`, `REPORT_KNOWN_KEYS = set(REPORT_REQUIRED_KEYS) | {"engine"}`, `SOURCE_API = "metrics-api-v1"`, `SOURCE_MANUAL = "manual-v0"`, 길이 상한 `MAX_MODEL_LEN = 128`, `MAX_GPU_TYPE_LEN = 64`, `MAX_CUSTOM_NAME_LEN = 64`, `MAX_CUSTOM_UNIT_LEN = 32`, `MAX_ENGINE_LEN = 64`.
  - 행 플래그 문자열: `F_HOURS_OVER = "hours_over_count"`, `F_UNKNOWN = "unknown_violation"`, `F_PCT = "pct_non_monotone"`, `F_DUP_MERGED = "dup_merged"`, `F_DUP_MODEL = "dup_model_kept_first"`, `F_DUP_CUSTOM = "dup_custom_kept_first"`; 출력 순서 `GPU_FLAG_ORDER = (F_HOURS_OVER, F_UNKNOWN, F_DUP_MERGED)`, `SERVING_FLAG_ORDER = (F_PCT, F_UNKNOWN, F_DUP_MODEL, F_DUP_CUSTOM)`(flags 리스트는 항상 이 순서·중복 없음).
  - 응답 WARN 코드(`CHECK WARN service=<svc> <code>=<count>`의 `<code>`): `W_IDENTITY = "identity_drift"`, `W_GEN_PARSE = "generated_at_parse_failed"`, `W_GEN_OFFSET = "generated_at_offset_mismatch"`, `W_ENGINE = "engine_malformed"`, `W_EXTRA_KEYS = "extra_top_keys"`.
  - `class PayloadError(ValueError)` — 응답 단위 구조 위반. 메시지 = 코드 문자열 `not_object | missing_keys:<k1,k2,…> | date_mismatch | gpu_not_array | serving_not_array`(호출자 T4/T6가 `CollectError(Event.PERMANENT_ERROR, f"report structure: {e}")`로 번역).
  - `@dataclass class MetricsPayload`: `date: str`, `reported_service_group: str`, `reported_service: str`, `generated_at_raw: str`(ISO 원문; `""` = 적재 시각·WARN 없음), `engine: object`(API 원문 dict | None | 기타), `gpu: list`, `serving: list`, `source_type: str`, `extra_top_keys: list[str] = field(default_factory=list)`.
  - `@dataclass class GpuRow`: `model: str, gpu_type: str, category: str, gpu_count: float, gpu_hours: float, flags: list[str]`.
  - `@dataclass class ServingRow`: `model: str, metric: str, name: str, unit: str, p50: float | None, p90: float | None, p95: float | None, p99: float | None, flags: list[str]`.
  - `@dataclass class NormalizeResult`: **필드 순서** `generated_at: datetime`(필수, aware KST — 첫 자리), `gpu_rows: list[GpuRow] = []`, `serving_rows: list[ServingRow] = []`(표준 + custom, long form), `rejected: int = 0`, `merged_dups: int = 0`, `warns: dict[str, int] = {}`(행 플래그 카운트 + 응답 WARN — 0인 코드는 키 없음), `engine_type: str = ""`, `engine_version: str = ""`; 프로퍼티 `n_gpu`(=len(gpu_rows)), `n_serving`(metric != "custom"), `n_custom`(metric == "custom"), `rows`(= n_gpu + n_serving + n_custom), `warn_total`(= sum(warns.values())), `is_nodata`(= rows == 0 and rejected == 0). T5 테스트 헬퍼는 `NormalizeResult(generated_at=datetime(2026, 9, 11, 2, 5, tzinfo=KST), gpu_rows=[…], …)`처럼 키워드로 직접 조립한다.
  - `def _is_num(v: object) -> bool` — `isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)`.
  - `def check_report_structure(body: object, expected_date: str) -> MetricsPayload` — dict 아님 → `not_object`; `REPORT_REQUIRED_KEYS` 누락 → `missing_keys:<쉼표 목록>`; `body["date"] != expected_date` → `date_mismatch`; gpu/serving list 아님 → `gpu_not_array`/`serving_not_array`. 통과 시 `MetricsPayload(date=expected_date, reported_service_group=str(body["serviceGroup"]), reported_service=str(body["service"]), generated_at_raw=str(body["generatedAt"]), engine=body.get("engine"), gpu=body["gpu"], serving=body["serving"], source_type=SOURCE_API, extra_top_keys=sorted(set(body) - REPORT_KNOWN_KEYS))`.
  - `def parse_generated_at(raw: str, now: datetime) -> tuple[datetime, str | None]` — `strip()==""` → `(now, None)`; 끝 `Z` → `+00:00` 치환 후 `datetime.fromisoformat`; 실패 또는 naive → `(now, W_GEN_PARSE)`; 오프셋 ≠ +09:00 → `(dt.astimezone(KST), W_GEN_OFFSET)`; 그 외 `(dt.astimezone(KST), None)`. 반환 datetime은 항상 aware KST.
  - `def parse_engine(engine: object) -> tuple[str, str, bool]` — `None` → `("", "", False)`; dict이고 `type`이 공백 아닌 str ≤64, `version`이 부재/None/str ≤64 → `(type, version or "", False)`; 그 외(비dict·type 부재/빈값/비str/초과·version 비str/초과) → `("", "", True)`. 추가 키는 무시.
  - `def normalize_gpu(rows: list) -> tuple[list[GpuRow], int, int, dict[str, int]]` → `(rows, rejected, merged_dups, flag_counts)`. 거부: 비dict, `model`/`gpuType` 부재·비str·strip 후 빈값·길이 초과(128/64), `category ∉ CATEGORIES`, `gpuCount`/`gpuHours` 비숫자(bool·NaN·inf 포함)·`gpuCount <= 0`·`gpuHours < 0`. 통과 행에 원행 기준 `hours_over_count`(`gpuHours > gpuCount*24 + EPS`)·`unknown_violation`(`model == "unknown" and category in ("serving","standby")`) 후 키 `(model, gpu_type, category)` 병합: `gpu_hours` SUM, `gpu_count` MAX, flags 합집합 + `dup_merged`(2행 이상), `merged_dups += n-1`. 출력 순서 = 첫 등장 순서, 값은 `float`. 문자열은 원문 유지(strip 안 함 — 정규화는 mart). 추가 키 무시. `flag_counts`: `hours_over_count`/`unknown_violation` = 플래그가 붙은 **출력(병합 후) 행 수**, `dup_merged` = 병합된 원행 수(= `merged_dups`).
  - `def normalize_serving(records: list) -> tuple[list[ServingRow], int, dict[str, int]]` → `(rows, rejected, flag_counts)`. 레코드 단위 거부(레코드 1 = rejected 1): 비dict, `set(record) - SERVING_ALLOWED_KEYS` 비어있지 않음, `model` 부재·비str·빈값·>128, 지표 키 0개(`ttftMs/itlMs/e2eMs/outputTps/custom` 모두 부재), `ttftMs/itlMs/e2eMs`가 dict 아님·키 집합 ≠ `{p50,p90,p95,p99}`·값 비숫자·음수, `outputTps`가 dict 아님·키 집합 ≠ `{p50}`·비숫자·음수, `custom`이 list 아님·원소 dict 아님·`name` 부재/비str/빈값/>64·`unit` 부재/비str/빈값/>32·p키 0개·p값 비숫자·`CUSTOM_ALLOWED_KEYS` 밖 키(p값 음수는 허용). `custom: []`은 유효(행 0, rejected 0). 거부 판정이 중복 판정보다 먼저. 중복 `model`(2번째 이후 레코드) → 버리고 첫 레코드의 모든 행에 `dup_model_kept_first`(카운트 = 버린 레코드 수); 레코드 안 custom `name` 중복 → 첫 것 유지 + 그 행에 `dup_custom_kept_first`(카운트 = 버린 항목 수); `unknown_violation` = `model == "unknown"`인 레코드의 모든 행; `pct_non_monotone` = 존재하는 p값을 p50→p90→p95→p99 순으로 비교해 `next < prev - EPS`(행별). long form: 표준 `ServingRow(metric=LATENCY_KEYS[k], name="", unit="ms", p50..p99)`, `outputTps` → `(metric="output_tps", name="", unit="tokens/s", p50=v, p90=p95=p99=None)`, custom → `(metric="custom", name, unit, 부재 p = None)`. 행 순서: 레코드 순 × (ttftMs, itlMs, e2eMs, outputTps, custom 순). `flag_counts`: `pct_non_monotone`/`unknown_violation` = 플래그 붙은 출력 행 수, `dup_model_kept_first`/`dup_custom_kept_first` = 버린 수.
  - `def normalize_payload(payload: MetricsPayload, entry: ServiceEntry, now: datetime | None = None) -> NormalizeResult` — `now` 미주입 시 `datetime.now(KST)`; gpu/serving list 아님 → `PayloadError("gpu_not_array"|"serving_not_array")`; `rejected` = gpu + serving 거부 합; `warns` = gpu·serving `flag_counts` 합산 + (`source_type == SOURCE_API` 이고 `(reported_service_group, reported_service) != (entry.service_group, entry.service)` → `identity_drift: 1`) + generated_at WARN(1) + `engine_malformed: 1` + `extra_top_keys: len(payload.extra_top_keys)`(>0일 때만).

- [ ] **Step 1: 전제 확인 — T2 산출물·순수 모듈 경계**

Run: `cd collectors/token-metrics && ls app/__init__.py app/config.py app/events.py conftest.py tests/__init__.py && grep -n "class ServiceEntry" app/config.py && grep -c "def test_" tests/test_config.py tests/test_events.py`
Expected: 5개 파일 경로가 출력되고 `app/config.py:<n>:class ServiceEntry:` 1줄, `tests/test_config.py:11` `tests/test_events.py:3`(T2 Step 3 `test_events.py` 3개 + Step 7 `test_config.py` 11개; 정확한 수는 T2 커밋 기준). 파일이 없으면 T2가 병합되지 않은 것 — 중단하고 보고한다(대신 만들지 않는다).

Run: `sed -n 260,266p docs/superpowers/specs/2026-08-31-token-metrics-ingest-design.md | grep -c "hours_over_count\|pct_non_monotone\|dup_model_kept_first"`
Expected: `1`(§5.3-2 줄 1개에 세 어휘가 모두 있다 — 규칙 위치 확인 — 이 태스크의 플래그·WARN 어휘는 이 절과 §4.1에서 온다).

- [ ] **Step 2: 실패하는 테스트 1 — 응답 구조(`check_report_structure`)·`parse_generated_at`·`parse_engine`** — `collectors/token-metrics/tests/test_normalize.py` 신규(전체 내용)

```python
"""normalize 3계층(§5.3) 테스트 — 순수 함수만, DB/HTTP 없음. 공통 fixture 상수는 Plan 6b 전 태스크 공통."""
from datetime import date, datetime

import pytest

from app.config import ServiceEntry
from app.normalize import (KST, SOURCE_API, W_GEN_OFFSET, W_GEN_PARSE, MetricsPayload,
                           PayloadError, check_report_structure, parse_engine,
                           parse_generated_at)

SERVICE_GROUP = "Mock Group"
SERVICE = "Mock Service A"
BASE_URL = "http://mock"
DATE = "2026-09-10"
GPU_TYPE = "H100"
ENGINE = {"type": "vllm", "version": "0.10.1"}
GENERATED_AT = "2026-09-11T02:05:00+09:00"
NOW = datetime(2026, 9, 11, 3, 0, tzinfo=KST)

ENTRY = ServiceEntry(service_group=SERVICE_GROUP, service=SERVICE, base_url=BASE_URL,
                     enabled=True, api_since=date(2026, 9, 9),
                     coverage_since=date(2026, 8, 26), until=None)


def report(**kw) -> dict:
    base = {"date": DATE, "serviceGroup": SERVICE_GROUP, "service": SERVICE,
            "generatedAt": GENERATED_AT, "engine": ENGINE, "gpu": [], "serving": []}
    base.update(kw)
    return base


# ---------- check_report_structure (응답 단위 → PayloadError) ----------

def test_check_report_missing_keys():
    with pytest.raises(PayloadError) as ei:
        check_report_structure({"date": DATE}, DATE)
    assert str(ei.value).startswith("missing_keys:")
    assert "serviceGroup" in str(ei.value) and "serving" in str(ei.value)


def test_check_report_not_object():
    with pytest.raises(PayloadError) as ei:
        check_report_structure([report()], DATE)
    assert str(ei.value) == "not_object"


def test_check_report_date_mismatch():
    with pytest.raises(PayloadError) as ei:
        check_report_structure(report(date="2026-09-09"), DATE)
    assert str(ei.value) == "date_mismatch"


def test_check_report_non_array():
    with pytest.raises(PayloadError) as ei:
        check_report_structure(report(gpu={}), DATE)
    assert str(ei.value) == "gpu_not_array"
    with pytest.raises(PayloadError) as ei:
        check_report_structure(report(serving="x"), DATE)
    assert str(ei.value) == "serving_not_array"


def test_check_report_ok_builds_api_payload():
    p = check_report_structure(report(gpu=[{"model": "m"}], engine=None), DATE)
    assert isinstance(p, MetricsPayload)
    assert p.source_type == SOURCE_API
    assert (p.date, p.reported_service_group, p.reported_service) == (DATE, SERVICE_GROUP, SERVICE)
    assert p.generated_at_raw == GENERATED_AT and p.engine is None
    assert p.gpu == [{"model": "m"}] and p.serving == [] and p.extra_top_keys == []


def test_check_report_extra_keys_recorded():
    p = check_report_structure(report(foo=1, bar=2), DATE)
    assert p.extra_top_keys == ["bar", "foo"]        # 정렬·무시(적재 안 함)


# ---------- parse_generated_at / parse_engine ----------

def test_generated_at_kst_ok():
    dt, warn = parse_generated_at(GENERATED_AT, NOW)
    assert dt == datetime(2026, 9, 11, 2, 5, tzinfo=KST) and warn is None
    assert dt.tzinfo is not None and dt.utcoffset().total_seconds() == 9 * 3600


def test_generated_at_offset_mismatch():
    dt, warn = parse_generated_at("2026-09-10T17:05:00+00:00", NOW)
    assert warn == W_GEN_OFFSET
    assert dt == datetime(2026, 9, 11, 2, 5, tzinfo=KST)       # KST 변환 (다음날 02:05)
    assert dt.utcoffset().total_seconds() == 9 * 3600


def test_generated_at_z_suffix():
    dt, warn = parse_generated_at("2026-09-10T17:05:00Z", NOW)
    assert warn == W_GEN_OFFSET                                # 파싱 성공 + 오프셋 불일치
    assert dt == datetime(2026, 9, 11, 2, 5, tzinfo=KST)


def test_generated_at_parse_failed_uses_now():
    assert parse_generated_at("nope", NOW) == (NOW, W_GEN_PARSE)
    assert parse_generated_at("2026-09-11T02:05:00", NOW) == (NOW, W_GEN_PARSE)   # naive
    assert parse_generated_at("None", NOW) == (NOW, W_GEN_PARSE)                  # str(None)


def test_generated_at_empty_is_now_without_warn():
    assert parse_generated_at("", NOW) == (NOW, None)
    assert parse_generated_at("   ", NOW) == (NOW, None)


def test_engine_variants():
    assert parse_engine(None) == ("", "", False)
    assert parse_engine(ENGINE) == ("vllm", "0.10.1", False)
    assert parse_engine({"type": "sglang"}) == ("sglang", "", False)          # version 부재
    assert parse_engine({"type": "custom", "version": None}) == ("custom", "", False)
    for bad in ({"type": ""}, {"version": "x"}, "vllm", {"type": "a" * 65},
                {"type": "vllm", "version": "v" * 65}, {"type": 7}, ["vllm"]):
        assert parse_engine(bad) == ("", "", True), bad

```

- [ ] **Step 3: 실패 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_normalize.py`
Expected: `ImportError while importing test module … E   ModuleNotFoundError: No module named 'app.normalize'` → `Interrupted: 1 error during collection`

- [ ] **Step 4: 구현 1 — 골격·상수·dataclass·`PayloadError`·`check_report_structure`·`parse_generated_at`·`parse_engine`** — `collectors/token-metrics/app/normalize.py` 신규(전체 내용, 192행)

```python
"""3계층 정규화·검증 (설계 §5.3) — DB/HTTP 무접촉 순수 함수. API 응답·manual-v0 CSV 공통 경로 (§5.5).

계층 1 = 스키마 형태 위반 → 거부(rejected 카운트만) / 응답 단위 위반 → PayloadError(호출자가 PERMANENT_ERROR로 번역)
계층 2 = 형태는 맞으나 운영자 검증·단조성 위반 → 적재 + 행 플래그(flags) 또는 응답 WARN(warns)
계층 3 = 교차 행·교차 소스 → mart-metrics(M3)·불변식 — 이 모듈 밖.
숫자 판정은 bool 제외·유한값만. 로깅 계약: 예외 메시지·warns 키에 행 원문을 넣지 않는다(코드·카운트만).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:                       # 순수 모듈 유지 — 런타임 import 없음(pyyaml 불필요)
    from app.config import ServiceEntry

EPS = 1e-6
KST = timezone(timedelta(hours=9))
CATEGORIES = ("serving", "standby", "test")
PCT_KEYS = ("p50", "p90", "p95", "p99")
LATENCY_KEYS = {"ttftMs": "ttft_ms", "itlMs": "itl_ms", "e2eMs": "e2e_ms"}
SERVING_ALLOWED_KEYS = {"model", "ttftMs", "itlMs", "e2eMs", "outputTps", "custom"}
CUSTOM_ALLOWED_KEYS = {"name", "unit"} | set(PCT_KEYS)
REPORT_REQUIRED_KEYS = ("date", "serviceGroup", "service", "generatedAt", "gpu", "serving")
REPORT_KNOWN_KEYS = set(REPORT_REQUIRED_KEYS) | {"engine"}
SOURCE_API = "metrics-api-v1"
SOURCE_MANUAL = "manual-v0"

MAX_MODEL_LEN = 128          # GpuRecord.model / ServingRecord.model maxLength
MAX_GPU_TYPE_LEN = 64        # GpuRecord.gpuType maxLength
MAX_CUSTOM_NAME_LEN = 64     # CustomMetric.name maxLength
MAX_CUSTOM_UNIT_LEN = 32     # CustomMetric.unit maxLength
MAX_ENGINE_LEN = 64          # Engine.type / Engine.version maxLength

# 행 플래그 (fact.flags 어휘 — Plan 6a A)
F_HOURS_OVER = "hours_over_count"
F_UNKNOWN = "unknown_violation"
F_PCT = "pct_non_monotone"
F_DUP_MERGED = "dup_merged"
F_DUP_MODEL = "dup_model_kept_first"
F_DUP_CUSTOM = "dup_custom_kept_first"
GPU_FLAG_ORDER = (F_HOURS_OVER, F_UNKNOWN, F_DUP_MERGED)
SERVING_FLAG_ORDER = (F_PCT, F_UNKNOWN, F_DUP_MODEL, F_DUP_CUSTOM)

# 응답 WARN 코드 (CHECK WARN service=<svc> <code>=<count>)
W_IDENTITY = "identity_drift"
W_GEN_PARSE = "generated_at_parse_failed"
W_GEN_OFFSET = "generated_at_offset_mismatch"
W_ENGINE = "engine_malformed"
W_EXTRA_KEYS = "extra_top_keys"


class PayloadError(ValueError):
    """응답 단위 구조 위반 코드: not_object | missing_keys:<k,..> | date_mismatch | gpu_not_array | serving_not_array."""


@dataclass
class MetricsPayload:
    date: str
    reported_service_group: str
    reported_service: str
    generated_at_raw: str            # ISO 문자열 원문; "" = 적재 시각 사용(manual 기본, WARN 없음)
    engine: object                   # API 원문 (dict | None | 기타)
    gpu: list                        # API 형태 dict 목록 (비배열이면 normalize_payload가 PayloadError)
    serving: list
    source_type: str                 # SOURCE_API | SOURCE_MANUAL
    extra_top_keys: list[str] = field(default_factory=list)


@dataclass
class GpuRow:
    model: str
    gpu_type: str
    category: str
    gpu_count: float
    gpu_hours: float
    flags: list[str]


@dataclass
class ServingRow:
    model: str
    metric: str                      # ttft_ms | itl_ms | e2e_ms | output_tps | custom
    name: str                        # 표준 지표 '' / custom 지표명
    unit: str                        # 'ms' / 'tokens/s' / custom 단위
    p50: float | None
    p90: float | None
    p95: float | None
    p99: float | None
    flags: list[str]


@dataclass
class NormalizeResult:
    generated_at: datetime           # aware KST (필수 — 앞자리: 나머지 필드는 기본값)
    gpu_rows: list[GpuRow] = field(default_factory=list)
    serving_rows: list[ServingRow] = field(default_factory=list)   # 표준 + custom 모두 (long form)
    rejected: int = 0
    merged_dups: int = 0
    warns: dict[str, int] = field(default_factory=dict)            # 행 플래그 카운트 + 응답 WARN (0인 코드는 키 없음)
    engine_type: str = ""
    engine_version: str = ""

    @property
    def n_gpu(self) -> int:
        return len(self.gpu_rows)

    @property
    def n_serving(self) -> int:
        return sum(1 for r in self.serving_rows if r.metric != "custom")

    @property
    def n_custom(self) -> int:
        return sum(1 for r in self.serving_rows if r.metric == "custom")

    @property
    def rows(self) -> int:
        return self.n_gpu + self.n_serving + self.n_custom

    @property
    def warn_total(self) -> int:
        return sum(self.warns.values())

    @property
    def is_nodata(self) -> bool:
        return self.rows == 0 and self.rejected == 0


def _is_num(v: object) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def check_report_structure(body: object, expected_date: str) -> MetricsPayload:
    """응답 단위 구조 검사(§5.3-1) — 위반 시 PayloadError(코드). 통과 시 API 페이로드(source_type=SOURCE_API)."""
    if not isinstance(body, dict):
        raise PayloadError("not_object")
    missing = [k for k in REPORT_REQUIRED_KEYS if k not in body]
    if missing:
        raise PayloadError("missing_keys:" + ",".join(missing))
    if body["date"] != expected_date:
        raise PayloadError("date_mismatch")
    if not isinstance(body["gpu"], list):
        raise PayloadError("gpu_not_array")
    if not isinstance(body["serving"], list):
        raise PayloadError("serving_not_array")
    return MetricsPayload(
        date=expected_date,
        reported_service_group=str(body["serviceGroup"]),
        reported_service=str(body["service"]),
        generated_at_raw=str(body["generatedAt"]),
        engine=body.get("engine"),
        gpu=body["gpu"],
        serving=body["serving"],
        source_type=SOURCE_API,
        extra_top_keys=sorted(set(body) - REPORT_KNOWN_KEYS),
    )


def parse_generated_at(raw: str, now: datetime) -> tuple[datetime, str | None]:
    """generatedAt → aware KST. ''→(now, None) / 파싱 실패·naive→(now, W_GEN_PARSE) / 오프셋≠+09:00→(KST 변환, W_GEN_OFFSET)."""
    s = raw.strip() if isinstance(raw, str) else str(raw).strip()
    if s == "":
        return now, None
    if s.endswith("Z"):                 # 3.10 fromisoformat은 'Z' 미지원
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return now, W_GEN_PARSE
    if dt.tzinfo is None or dt.utcoffset() is None:
        return now, W_GEN_PARSE
    if dt.utcoffset() != timedelta(hours=9):
        return dt.astimezone(KST), W_GEN_OFFSET
    return dt.astimezone(KST), None


def parse_engine(engine: object) -> tuple[str, str, bool]:
    """Engine 블록 → (engine_type, engine_version, malformed). None은 정상 부재('' , '', False)."""
    if engine is None:
        return "", "", False
    if not isinstance(engine, dict):
        return "", "", True
    etype = engine.get("type")
    if not isinstance(etype, str) or not etype.strip() or len(etype) > MAX_ENGINE_LEN:
        return "", "", True
    version = engine.get("version")
    if version is None:
        version = ""
    if not isinstance(version, str) or len(version) > MAX_ENGINE_LEN:
        return "", "", True
    return etype, version, False
```

- [ ] **Step 5: 통과 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_normalize.py`
Expected: `12 passed`

- [ ] **Step 6: 실패하는 테스트 2 — `normalize_gpu`(거부 규칙·원행 기준 플래그·병합)** — `collectors/token-metrics/tests/test_normalize.py` **끝에 추가**(모듈 수준 import를 청크 머리에 두는 것은 의도 — 각 단계가 독립적으로 red가 되도록)

```python

# ---------- normalize_gpu ----------

from app.normalize import F_DUP_MERGED, F_HOURS_OVER, F_UNKNOWN, GpuRow, normalize_gpu  # noqa: E402


def G(**kw) -> dict:
    base = {"model": "claude-sonnet-5", "gpuType": GPU_TYPE, "category": "serving",
            "gpuCount": 4, "gpuHours": 96.0}
    base.update(kw)
    return base


def test_gpu_reject_rules():
    bad = [
        "not-a-dict",                    # 비dict
        G(model=""),                     # model 빈값
        G(model="   "),                  # model 공백만
        G(model="m" * 129),              # model 129자
        G(gpuType="g" * 65),             # gpuType 65자
        G(category="prod"),              # category ∉ enum
        G(gpuCount=True),                # bool은 숫자 아님
        G(gpuCount=0),                   # gpuCount ≤ 0
        G(gpuHours=-1),                  # gpuHours 음수
        G(gpuHours="24"),                # 문자열 숫자
        G(gpuHours=float("nan")),        # 비유한
    ]
    for raw in bad:
        rows, rejected, merged, counts = normalize_gpu([raw])
        assert (rows, rejected, merged, counts) == ([], 1, 0, {}), raw
    missing = dict(G()); del missing["gpuHours"]
    assert normalize_gpu([missing])[1] == 1


def test_gpu_ok_row_shape():
    rows, rejected, merged, counts = normalize_gpu([G(gpuCount=2, gpuHours=48)])
    assert rejected == 0 and merged == 0 and counts == {}
    assert rows == [GpuRow(model="claude-sonnet-5", gpu_type=GPU_TYPE, category="serving",
                           gpu_count=2.0, gpu_hours=48.0, flags=[])]
    assert isinstance(rows[0].gpu_count, float) and isinstance(rows[0].gpu_hours, float)


def test_gpu_hours_over_count_on_original_rows():
    rows, _, _, counts = normalize_gpu([G(gpuCount=2, gpuHours=49)])
    assert rows[0].flags == [F_HOURS_OVER] and counts == {F_HOURS_OVER: 1}
    rows, _, _, counts = normalize_gpu([G(gpuCount=2, gpuHours=48.0000001)])   # EPS 안
    assert rows[0].flags == [] and counts == {}


def test_gpu_unknown_violation():
    rows, _, _, counts = normalize_gpu([G(model="unknown", category="serving"),
                                        G(model="unknown", category="standby"),
                                        G(model="unknown", category="test")])
    assert [r.flags for r in rows] == [[F_UNKNOWN], [F_UNKNOWN], []]
    assert counts == {F_UNKNOWN: 2}


def test_gpu_dup_merged_sum_hours_max_count():
    rows, rejected, merged, counts = normalize_gpu([G(gpuCount=2, gpuHours=10), G(gpuCount=4, gpuHours=20)])
    assert rejected == 0 and merged == 1
    assert rows == [GpuRow(model="claude-sonnet-5", gpu_type=GPU_TYPE, category="serving",
                           gpu_count=4.0, gpu_hours=30.0, flags=[F_DUP_MERGED])]
    assert counts == {F_DUP_MERGED: 1}
    # 원행 중 하나가 over → 병합행에 hours_over_count도 (병합 전 원행 기준)
    rows, _, merged, counts = normalize_gpu([G(gpuCount=1, gpuHours=30), G(gpuCount=4, gpuHours=20)])
    assert rows[0].flags == [F_HOURS_OVER, F_DUP_MERGED] and rows[0].gpu_hours == 50.0
    assert counts == {F_HOURS_OVER: 1, F_DUP_MERGED: 1}
    # 3행 같은 키 → merged 2, 순서는 첫 등장
    rows, _, merged, _ = normalize_gpu([G(category="test"), G(), G(), G()])
    assert merged == 2 and [r.category for r in rows] == ["test", "serving"]


def test_gpu_extra_keys_ignored():
    rows, rejected, _, _ = normalize_gpu([G(note="x", replicas=3)])
    assert rejected == 0 and len(rows) == 1

```

- [ ] **Step 7: 실패 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_normalize.py`
Expected: `E   ImportError: cannot import name 'normalize_gpu' from 'app.normalize' (…/collectors/token-metrics/app/normalize.py)` → `Interrupted: 1 error during collection`

- [ ] **Step 8: 구현 2 — `_str_field`·`_ordered_flags`·`_count_flags`·`_validate_gpu`·`normalize_gpu`** — `collectors/token-metrics/app/normalize.py` **끝에 추가**(마지막 줄 `    return etype, version, False` 뒤, 빈 줄 2개 포함 — 추가 후 266행)

```python


def _str_field(obj: dict, key: str, max_len: int) -> str | None:
    """문자열 필드 검사 — 부재·비str·strip 후 빈값·길이 초과면 None. 통과 값은 원문 그대로(정규화는 mart)."""
    v = obj.get(key)
    if not isinstance(v, str) or not v.strip() or len(v) > max_len:
        return None
    return v


def _ordered_flags(flags: set[str], order: tuple[str, ...]) -> list[str]:
    return [f for f in order if f in flags]


def _count_flags(rows: list, counts: dict[str, int]) -> None:
    for r in rows:
        for f in r.flags:
            counts[f] = counts.get(f, 0) + 1


def _validate_gpu(raw: object) -> GpuRow | None:
    """계층 1(gpu 행): 형태 위반 → None. 추가 키는 무시(GpuRecord에 additionalProperties 없음)."""
    if not isinstance(raw, dict):
        return None
    model = _str_field(raw, "model", MAX_MODEL_LEN)
    gpu_type = _str_field(raw, "gpuType", MAX_GPU_TYPE_LEN)
    if model is None or gpu_type is None:
        return None
    category = raw.get("category")
    if category not in CATEGORIES:
        return None
    count, hours = raw.get("gpuCount"), raw.get("gpuHours")
    if not _is_num(count) or not _is_num(hours) or count <= 0 or hours < 0:
        return None
    return GpuRow(model=model, gpu_type=gpu_type, category=category,
                  gpu_count=float(count), gpu_hours=float(hours), flags=[])


def normalize_gpu(rows: list) -> tuple[list[GpuRow], int, int, dict[str, int]]:
    """gpu 배열 → (병합 행, rejected, merged_dups, flag_counts).

    계층 2 플래그는 병합 전 원행 기준(hours_over_count·unknown_violation) → 키 (model, gpu_type, category)로
    병합: gpu_hours=SUM, gpu_count=MAX, flags=합집합 + dup_merged. 출력 순서 = 첫 등장 순서.
    flag_counts: hours_over_count·unknown_violation = 플래그가 붙은 출력 행 수, dup_merged = 병합된 원행 수(= merged_dups).
    """
    merged: dict[tuple[str, str, str], GpuRow] = {}
    rejected = 0
    merged_dups = 0
    for raw in rows:
        row = _validate_gpu(raw)
        if row is None:
            rejected += 1
            continue
        flags: set[str] = set()
        if row.gpu_hours > row.gpu_count * 24 + EPS:
            flags.add(F_HOURS_OVER)
        if row.model == "unknown" and row.category in ("serving", "standby"):
            flags.add(F_UNKNOWN)
        key = (row.model, row.gpu_type, row.category)
        prev = merged.get(key)
        if prev is None:
            row.flags = _ordered_flags(flags, GPU_FLAG_ORDER)
            merged[key] = row
            continue
        prev.gpu_hours += row.gpu_hours
        prev.gpu_count = max(prev.gpu_count, row.gpu_count)
        prev.flags = _ordered_flags(set(prev.flags) | flags | {F_DUP_MERGED}, GPU_FLAG_ORDER)
        merged_dups += 1
    out = list(merged.values())
    counts: dict[str, int] = {}
    _count_flags(out, counts)
    if merged_dups:
        counts[F_DUP_MERGED] = merged_dups
    return out, rejected, merged_dups, counts
```

- [ ] **Step 9: 통과 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_normalize.py`
Expected: `18 passed`

- [ ] **Step 10: 실패하는 테스트 3 — `normalize_serving`(거부 규칙·long form 케이스 A/F·단조성·중복·unknown)** — `collectors/token-metrics/tests/test_normalize.py` **끝에 추가**

```python

# ---------- normalize_serving ----------

from app.normalize import F_DUP_CUSTOM, F_DUP_MODEL, F_PCT, ServingRow, normalize_serving  # noqa: E402

TTFT = {"p50": 280, "p90": 560, "p95": 720, "p99": 1200}
ITL = {"p50": 24, "p90": 38, "p95": 47, "p99": 80}
E2E = {"p50": 1400, "p90": 2600, "p95": 3300, "p99": 5200}


def S(**kw) -> dict:
    base = {"model": "claude-sonnet-5", "ttftMs": dict(TTFT), "itlMs": dict(ITL), "outputTps": {"p50": 41.0}}
    base.update(kw)
    return base


def test_serving_reject_rules():
    bad = [
        "not-a-dict",                                        # 비dict
        S(model=""),                                         # model 빈값
        S(model="m" * 129),                                  # model 129자
        {"model": "m", "foo": 1},                            # 허용 외 키
        {"model": "m"},                                      # 지표 0개
        S(ttftMs={"p50": 1, "p90": 2, "p95": 3}),            # p99 누락
        S(ttftMs={**TTFT, "p999": 9}),                       # 추가 키
        S(ttftMs={**TTFT, "p50": -1}),                       # 음수
        S(ttftMs={**TTFT, "p50": "280"}),                    # 비숫자
        S(ttftMs={**TTFT, "p50": True}),                     # bool
        S(ttftMs=[280, 560, 720, 1200]),                     # dict 아님
        S(outputTps={"p50": 1, "p90": 2}),                   # p50 외 키
        S(outputTps={}),                                     # p50 누락
        S(outputTps={"p50": -0.5}),                          # 음수
        S(custom=[{"unit": "ms", "p50": 1}]),                # custom name 누락
        S(custom=[{"name": "q", "unit": "u" * 33, "p50": 1}]),   # unit 33자
        S(custom=[{"name": "n" * 65, "unit": "ms", "p50": 1}]),  # name 65자
        S(custom=[{"name": "q", "unit": "ms"}]),             # p키 0개
        S(custom=[{"name": "q", "unit": "ms", "p50": "x"}]), # p값 비숫자
        S(custom=[{"name": "q", "unit": "ms", "p50": 1, "avg": 2}]),   # 허용 외 키
        S(custom={"name": "q", "unit": "ms", "p50": 1}),     # custom이 list 아님
        S(custom=["q"]),                                     # 원소 dict 아님
    ]
    for rec in bad:
        rows, rejected, counts = normalize_serving([rec])
        assert (rows, rejected, counts) == ([], 1, {}), rec


def test_serving_long_form_case_a():
    rows, rejected, counts = normalize_serving([S()])
    assert rejected == 0 and counts == {}
    assert [(r.metric, r.name, r.unit) for r in rows] == [("ttft_ms", "", "ms"), ("itl_ms", "", "ms"),
                                                          ("output_tps", "", "tokens/s")]
    assert rows[0] == ServingRow(model="claude-sonnet-5", metric="ttft_ms", name="", unit="ms",
                                 p50=280.0, p90=560.0, p95=720.0, p99=1200.0, flags=[])
    assert (rows[2].p50, rows[2].p90, rows[2].p95, rows[2].p99) == (41.0, None, None, None)


def test_serving_case_f_e2e_and_custom():
    rec = {"model": "claude-haiku-4-5", "e2eMs": dict(E2E),
           "custom": [{"name": "queueWaitMs", "unit": "ms", "p50": 120, "p90": 300},
                      {"name": "batchSize", "unit": "requests", "p50": 8}]}
    rows, rejected, counts = normalize_serving([rec])
    assert rejected == 0 and counts == {} and len(rows) == 3
    assert (rows[0].metric, rows[0].unit, rows[0].p99) == ("e2e_ms", "ms", 5200.0)
    assert (rows[1].metric, rows[1].name, rows[1].unit, rows[1].p50, rows[1].p90, rows[1].p95, rows[1].p99) == \
        ("custom", "queueWaitMs", "ms", 120.0, 300.0, None, None)
    assert (rows[2].metric, rows[2].name, rows[2].unit, rows[2].p50, rows[2].p95) == \
        ("custom", "batchSize", "requests", 8.0, None)


def test_serving_row_order_across_records():
    rows, _, _ = normalize_serving([{"model": "b", "outputTps": {"p50": 1}, "ttftMs": dict(TTFT)},
                                    {"model": "a", "e2eMs": dict(E2E)}])
    assert [(r.model, r.metric) for r in rows] == [("b", "ttft_ms"), ("b", "output_tps"), ("a", "e2e_ms")]


def test_serving_empty_custom_list_is_valid_zero_rows():
    rows, rejected, counts = normalize_serving([{"model": "m", "custom": []}])
    assert (rows, rejected, counts) == ([], 0, {})


def test_serving_pct_non_monotone():
    rows, _, counts = normalize_serving([S(ttftMs={**TTFT, "p90": 100})])          # p90 < p50
    assert rows[0].flags == [F_PCT] and rows[1].flags == [] and rows[2].flags == []
    assert counts == {F_PCT: 1}
    rows, _, counts = normalize_serving([S(ttftMs={**TTFT, "p90": 280 - 1e-7})])   # EPS 안
    assert rows[0].flags == [] and counts == {}
    rows, _, counts = normalize_serving([S(custom=[{"name": "q", "unit": "ms", "p50": 5, "p90": 4}])])
    assert rows[3].metric == "custom" and rows[3].flags == [F_PCT] and counts == {F_PCT: 1}
    rows, _, counts = normalize_serving([S(custom=[{"name": "q", "unit": "ms", "p50": 5, "p99": 4}])])
    assert rows[3].flags == [F_PCT]                                                  # 부재 p는 건너뛰고 비교


def test_serving_dup_model_kept_first():
    rows, rejected, counts = normalize_serving([S(), S(ttftMs={**TTFT, "p50": 1})])
    assert rejected == 0 and len(rows) == 3
    assert rows[0].p50 == 280.0                                # 첫 레코드 값 유지
    assert all(r.flags == [F_DUP_MODEL] for r in rows)
    assert counts == {F_DUP_MODEL: 1}
    rows, rejected, counts = normalize_serving([S(), S(), {"model": "other", "e2eMs": dict(E2E)}])
    assert len(rows) == 4 and counts == {F_DUP_MODEL: 1} and rows[3].flags == []


def test_serving_dup_model_after_rejected_record_is_reject_not_dup():
    rows, rejected, counts = normalize_serving([S(), S(ttftMs={"p50": 1})])       # 2번째는 형태 위반
    assert rejected == 1 and counts == {} and all(r.flags == [] for r in rows)


def test_serving_dup_custom_kept_first():
    rec = S(custom=[{"name": "q", "unit": "ms", "p50": 1}, {"name": "q", "unit": "ms", "p50": 2},
                    {"name": "r", "unit": "ms", "p50": 3}])
    rows, rejected, counts = normalize_serving([rec])
    customs = [r for r in rows if r.metric == "custom"]
    assert rejected == 0 and [(c.name, c.p50) for c in customs] == [("q", 1.0), ("r", 3.0)]
    assert customs[0].flags == [F_DUP_CUSTOM] and customs[1].flags == []
    assert counts == {F_DUP_CUSTOM: 1}


def test_serving_unknown_violation():
    rows, _, counts = normalize_serving([S(model="unknown", custom=[{"name": "q", "unit": "ms", "p50": 1}])])
    assert len(rows) == 4 and all(r.flags == [F_UNKNOWN] for r in rows)
    assert counts == {F_UNKNOWN: 4}
    rows, _, _ = normalize_serving([S(model="unknown", ttftMs={**TTFT, "p90": 1})])
    assert rows[0].flags == [F_PCT, F_UNKNOWN]                 # 고정 순서


def test_serving_custom_negative_allowed():
    rows, rejected, _ = normalize_serving([{"model": "m", "custom": [{"name": "delta", "unit": "ms", "p50": -1}]}])
    assert rejected == 0 and rows[0].p50 == -1.0

```

- [ ] **Step 11: 실패 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_normalize.py`
Expected: `E   ImportError: cannot import name 'normalize_serving' from 'app.normalize' (…)` → `Interrupted: 1 error during collection`

- [ ] **Step 12: 구현 3 — `_pct_block`·`_custom_item`·`_is_non_monotone`·`_expand_record`·`normalize_serving`** — `collectors/token-metrics/app/normalize.py` **끝에 추가**(마지막 줄 `    return out, rejected, merged_dups, counts` 뒤, 빈 줄 2개 포함 — 추가 후 398행)

```python


def _pct_block(block: object, keys: tuple[str, ...]) -> dict[str, float] | None:
    """ttftMs/itlMs/e2eMs(키 집합 == p50..p99) · outputTps(키 집합 == p50) — 값은 숫자·≥0. 위반 → None."""
    if not isinstance(block, dict) or set(block) != set(keys):
        return None
    if not all(_is_num(block[k]) and block[k] >= 0 for k in keys):
        return None
    return {k: float(block[k]) for k in keys}


def _custom_item(item: object) -> tuple[str, str, dict[str, float]] | None:
    """CustomMetric: name(≤64)·unit(≤32) 필수, 허용 키 {name, unit, p50..p99}, p키 ≥1, p값 숫자(음수 허용). 위반 → None."""
    if not isinstance(item, dict) or set(item) - CUSTOM_ALLOWED_KEYS:
        return None
    name = _str_field(item, "name", MAX_CUSTOM_NAME_LEN)
    unit = _str_field(item, "unit", MAX_CUSTOM_UNIT_LEN)
    if name is None or unit is None:
        return None
    present = [k for k in PCT_KEYS if k in item]
    if not present or not all(_is_num(item[k]) for k in present):
        return None
    return name, unit, {k: float(item[k]) for k in present}


def _is_non_monotone(p: dict[str, float]) -> bool:
    """존재하는 p값을 p50→p90→p95→p99 순으로 비교, next < prev - EPS 이면 True."""
    prev: float | None = None
    for k in PCT_KEYS:
        if k not in p:
            continue
        if prev is not None and p[k] < prev - EPS:
            return True
        prev = p[k]
    return False


def _expand_record(record: dict) -> tuple[list[ServingRow], int] | None:
    """계층 1(serving 레코드) 검사 + long-form 전개. 위반 → None(레코드 1개 = rejected 1).
    반환 (rows, dup_custom_discarded). 행 순서: ttftMs, itlMs, e2eMs, outputTps, custom.
    """
    if not isinstance(record, dict) or set(record) - SERVING_ALLOWED_KEYS:
        return None
    model = _str_field(record, "model", MAX_MODEL_LEN)
    if model is None:
        return None
    metric_keys = [k for k in ("ttftMs", "itlMs", "e2eMs", "outputTps", "custom") if k in record]
    if not metric_keys:                                  # 지표 0개 (minProperties 2)
        return None
    unknown = model == "unknown"
    rows: list[ServingRow] = []
    dup_custom = 0

    def _row(metric: str, name: str, unit: str, p: dict[str, float]) -> ServingRow:
        flags: set[str] = set()
        if _is_non_monotone(p):
            flags.add(F_PCT)
        if unknown:
            flags.add(F_UNKNOWN)
        return ServingRow(model=model, metric=metric, name=name, unit=unit,
                          p50=p.get("p50"), p90=p.get("p90"), p95=p.get("p95"), p99=p.get("p99"),
                          flags=_ordered_flags(flags, SERVING_FLAG_ORDER))

    for api_key, metric in LATENCY_KEYS.items():
        if api_key in record:
            p = _pct_block(record[api_key], PCT_KEYS)
            if p is None:
                return None
            rows.append(_row(metric, "", "ms", p))
    if "outputTps" in record:
        p = _pct_block(record["outputTps"], ("p50",))
        if p is None:
            return None
        rows.append(_row("output_tps", "", "tokens/s", p))
    if "custom" in record:
        customs = record["custom"]
        if not isinstance(customs, list):
            return None
        seen: dict[str, ServingRow] = {}
        for item in customs:
            parsed = _custom_item(item)
            if parsed is None:
                return None
            name, unit, p = parsed
            first = seen.get(name)
            if first is not None:                        # 같은 name 중복 → 첫 것 유지 + 플래그
                first.flags = _ordered_flags(set(first.flags) | {F_DUP_CUSTOM}, SERVING_FLAG_ORDER)
                dup_custom += 1
                continue
            seen[name] = _row("custom", name, unit, p)
        rows.extend(seen.values())
    return rows, dup_custom


def normalize_serving(records: list) -> tuple[list[ServingRow], int, dict[str, int]]:
    """serving 배열 → (long-form 행, rejected, flag_counts).

    레코드 단위 거부(계층 1)가 중복 판정보다 먼저. 중복 model(2번째 이후 레코드)은 버리고 첫 레코드의 모든 행에
    dup_model_kept_first. flag_counts: pct_non_monotone·unknown_violation = 플래그가 붙은 출력 행 수,
    dup_model_kept_first = 버린 레코드 수, dup_custom_kept_first = 버린 custom 항목 수.
    """
    out: list[ServingRow] = []
    by_model: dict[str, list[ServingRow]] = {}
    rejected = 0
    dup_model = 0
    dup_custom = 0
    for record in records:
        expanded = _expand_record(record)
        if expanded is None:
            rejected += 1
            continue
        rows, n_dup_custom = expanded
        model = record["model"]
        first = by_model.get(model)
        if first is not None:
            for r in first:
                r.flags = _ordered_flags(set(r.flags) | {F_DUP_MODEL}, SERVING_FLAG_ORDER)
            dup_model += 1
            continue
        by_model[model] = rows
        out.extend(rows)
        dup_custom += n_dup_custom
    counts: dict[str, int] = {}
    for r in out:
        for f in r.flags:
            if f in (F_PCT, F_UNKNOWN):
                counts[f] = counts.get(f, 0) + 1
    if dup_model:
        counts[F_DUP_MODEL] = dup_model
    if dup_custom:
        counts[F_DUP_CUSTOM] = dup_custom
    return out, rejected, counts
```

- [ ] **Step 13: 통과 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_normalize.py`
Expected: `29 passed`

- [ ] **Step 14: 실패하는 테스트 4 — `normalize_payload`(identity_drift API 한정·카운트·NODATA·케이스 E·warn_total·응답 WARN·now 주입)** — `collectors/token-metrics/tests/test_normalize.py` **끝에 추가**

```python

# ---------- normalize_payload ----------

from app.normalize import (SOURCE_MANUAL, W_ENGINE, W_EXTRA_KEYS, W_IDENTITY,  # noqa: E402
                           NormalizeResult, normalize_payload)


def payload(**kw) -> MetricsPayload:
    base = dict(date=DATE, reported_service_group=SERVICE_GROUP, reported_service=SERVICE,
                generated_at_raw=GENERATED_AT, engine=dict(ENGINE), gpu=[], serving=[],
                source_type=SOURCE_API)
    base.update(kw)
    return MetricsPayload(**base)


def test_payload_identity_drift_api_only():
    r = normalize_payload(payload(reported_service="Mock Service A "), ENTRY, now=NOW)
    assert r.warns == {W_IDENTITY: 1} and r.warn_total == 1
    r = normalize_payload(payload(reported_service_group="Other"), ENTRY, now=NOW)
    assert r.warns == {W_IDENTITY: 1}
    r = normalize_payload(payload(reported_service="Mock Service A ", source_type=SOURCE_MANUAL), ENTRY, now=NOW)
    assert r.warns == {}


def test_payload_counts_and_nodata():
    r = normalize_payload(payload(gpu=[G(), G(category="standby", gpuCount=1, gpuHours=24)],
                                  serving=[S(custom=[{"name": "q", "unit": "ms", "p50": 1}])]), ENTRY, now=NOW)
    assert (r.n_gpu, r.n_serving, r.n_custom, r.rows) == (2, 3, 1, 6)
    assert r.rejected == 0 and r.merged_dups == 0 and r.warns == {} and not r.is_nodata
    assert (r.engine_type, r.engine_version) == ("vllm", "0.10.1")
    assert r.generated_at == datetime(2026, 9, 11, 2, 5, tzinfo=KST)
    assert normalize_payload(payload(), ENTRY, now=NOW).is_nodata                        # gpu:[] serving:[]
    r = normalize_payload(payload(serving=[S()]), ENTRY, now=NOW)                          # 케이스 E
    assert not r.is_nodata and r.rows == 3
    r = normalize_payload(payload(gpu=[G(category="prod")]), ENTRY, now=NOW)               # 전량 거부
    assert (r.rows, r.rejected, r.is_nodata) == (0, 1, False)


def test_payload_rejected_and_merged_sum_both_blocks():
    r = normalize_payload(payload(gpu=[G(), G(), G(gpuCount=0)], serving=[S(), {"model": "m"}]), ENTRY, now=NOW)
    assert (r.n_gpu, r.merged_dups, r.rejected) == (1, 1, 2)
    assert r.warns == {F_DUP_MERGED: 1}


def test_payload_warn_total_sums_flags_and_response_warns():
    r = normalize_payload(payload(gpu=[G(gpuCount=2, gpuHours=49)], reported_service="X"), ENTRY, now=NOW)
    assert r.warns == {F_HOURS_OVER: 1, W_IDENTITY: 1} and r.warn_total == 2
    assert r.gpu_rows[0].flags == [F_HOURS_OVER]


def test_payload_response_warns_generated_at_engine_extra_keys():
    r = normalize_payload(payload(generated_at_raw="nope", engine="vllm", extra_top_keys=["a", "b"]), ENTRY, now=NOW)
    assert r.warns == {W_GEN_PARSE: 1, W_ENGINE: 1, W_EXTRA_KEYS: 2}
    assert r.generated_at == NOW and (r.engine_type, r.engine_version) == ("", "")
    r = normalize_payload(payload(generated_at_raw="2026-09-10T17:05:00Z"), ENTRY, now=NOW)
    assert r.warns == {W_GEN_OFFSET: 1} and r.generated_at == datetime(2026, 9, 11, 2, 5, tzinfo=KST)


def test_payload_generated_at_now_injected():
    r = normalize_payload(payload(generated_at_raw="", source_type=SOURCE_MANUAL), ENTRY, now=NOW)
    assert r.generated_at is NOW and r.warns == {}
    r = normalize_payload(payload(generated_at_raw=""), ENTRY)                              # now 미주입 → aware KST
    assert r.generated_at.tzinfo is not None and r.generated_at.utcoffset().total_seconds() == 9 * 3600


def test_payload_engine_null_no_warn():
    r = normalize_payload(payload(engine=None), ENTRY, now=NOW)
    assert r.warns == {} and (r.engine_type, r.engine_version) == ("", "")


def test_payload_non_array_raises():
    with pytest.raises(PayloadError) as ei:
        normalize_payload(payload(gpu={}), ENTRY, now=NOW)
    assert str(ei.value) == "gpu_not_array"
    with pytest.raises(PayloadError) as ei:
        normalize_payload(payload(serving=None), ENTRY, now=NOW)
    assert str(ei.value) == "serving_not_array"


def test_normalize_result_direct_construction_defaults():
    r = NormalizeResult(generated_at=NOW)
    assert (r.rows, r.rejected, r.merged_dups, r.warns, r.engine_type, r.engine_version) == (0, 0, 0, {}, "", "")
    assert r.is_nodata and r.warn_total == 0
```

- [ ] **Step 15: 실패 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_normalize.py`
Expected: `E   ImportError: cannot import name 'normalize_payload' from 'app.normalize' (…)` → `Interrupted: 1 error during collection`

- [ ] **Step 16: 구현 4 — `normalize_payload`** — `collectors/token-metrics/app/normalize.py` **끝에 추가**(마지막 줄 `    return out, rejected, counts` 뒤, 빈 줄 2개 포함 — 추가 후 440행)

```python


def normalize_payload(payload: MetricsPayload, entry: ServiceEntry,
                      now: datetime | None = None) -> NormalizeResult:
    """페이로드(API·manual 공통) → NormalizeResult. gpu/serving이 list가 아니면 PayloadError.

    warns = gpu·serving flag_counts + identity_drift(API만: reported_* ≠ 레지스트리 정본)
            + generated_at WARN + engine_malformed + extra_top_keys(개수, >0일 때만).
    """
    if now is None:
        now = datetime.now(KST)
    if not isinstance(payload.gpu, list):
        raise PayloadError("gpu_not_array")
    if not isinstance(payload.serving, list):
        raise PayloadError("serving_not_array")
    gpu_rows, gpu_rejected, merged_dups, gpu_counts = normalize_gpu(payload.gpu)
    serving_rows, serving_rejected, serving_counts = normalize_serving(payload.serving)
    warns: dict[str, int] = {}
    for counts in (gpu_counts, serving_counts):
        for code, n in counts.items():
            warns[code] = warns.get(code, 0) + n
    if payload.source_type == SOURCE_API and \
       (payload.reported_service_group, payload.reported_service) != (entry.service_group, entry.service):
        warns[W_IDENTITY] = 1
    generated_at, gen_warn = parse_generated_at(payload.generated_at_raw, now)
    if gen_warn is not None:
        warns[gen_warn] = 1
    engine_type, engine_version, malformed = parse_engine(payload.engine)
    if malformed:
        warns[W_ENGINE] = 1
    if payload.extra_top_keys:
        warns[W_EXTRA_KEYS] = len(payload.extra_top_keys)
    return NormalizeResult(
        generated_at=generated_at,
        gpu_rows=gpu_rows,
        serving_rows=serving_rows,
        rejected=gpu_rejected + serving_rejected,
        merged_dups=merged_dups,
        warns=warns,
        engine_type=engine_type,
        engine_version=engine_version,
    )
```

- [ ] **Step 17: 통과 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_normalize.py`
Expected: `38 passed`

- [ ] **Step 18: 순수 모듈 확인 + 전체 회귀 + zero-diff**

Run(순수성 — `requests`·`clickhouse_connect`·`yaml` 없이 import 되는지를 import 훅으로 강제):
```bash
cd collectors/token-metrics && python3 - <<'PY'
import builtins, sys
sys.path.insert(0, ".")
real = builtins.__import__
def guard(name, *a, **k):
    if name.split(".")[0] in ("requests", "clickhouse_connect", "yaml"):
        raise ImportError("forbidden at runtime: " + name)
    return real(name, *a, **k)
builtins.__import__ = guard
import app.normalize as n
builtins.__import__ = real
assert n.SOURCE_API == "metrics-api-v1" and n.SOURCE_MANUAL == "manual-v0"
assert n.LATENCY_KEYS == {"ttftMs": "ttft_ms", "itlMs": "itl_ms", "e2eMs": "e2e_ms"}
print("pure import ok")
PY
```
Expected: `pure import ok`(모듈 import는 `math`·`dataclasses`·`datetime`·`typing`뿐 — `app.config`는 `TYPE_CHECKING` 가드 안).

Run: `cd collectors/token-metrics && grep -n "^import\|^from" app/normalize.py`
Expected: 정확히 5줄 — `from __future__ import annotations`, `import math`, `from dataclasses import dataclass, field`, `from datetime import datetime, timedelta, timezone`, `from typing import TYPE_CHECKING`.

Run: `cd collectors/token-metrics && python3 -m pytest -q`
Expected: T2 통과 수 + 38 → `52 passed`(T2 outline 기준 14 + 38; T2 실제 커밋의 테스트 수가 다르면 그 수 + 38)

Run: `git status --short collectors/token-metrics && git diff --stat -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml`
Expected: `?? collectors/token-metrics/app/normalize.py`·`?? collectors/token-metrics/tests/test_normalize.py` 2줄만, diff --stat 출력 없음(zero-diff 유지).

- [ ] **Step 19: Commit**

```bash
git add collectors/token-metrics/app/normalize.py collectors/token-metrics/tests/test_normalize.py
git commit -m "feat(collectors-metrics): normalize 3계층 — 구조 거부·의미 플래그·응답 WARN·long-form serving·MetricsPayload (Plan 6b T3)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

**설계 해석(이 태스크에서 하나로 정한 항목 — footer Self-Review에 옮겨 적는다):**
- `NormalizeResult` 필드 순서는 outline 나열(gpu_rows … generated_at)과 달리 `generated_at`을 첫 자리·필수로 두고 나머지에 기본값을 준다 — `datetime.now()` 기본값(숨은 시계)을 피하면서 T5 헬퍼의 키워드 조립을 쉽게 하기 위함. 키워드 조립만 쓰므로 소비자 코드에 영향 없음.
- `warns` 카운트 의미: `hours_over_count`·`unknown_violation`·`pct_non_monotone` = 플래그가 붙은 **출력 행 수**(gpu는 병합 후), `dup_merged` = 병합된 원행 수(= `merged_dups`), `dup_model_kept_first` = 버린 레코드 수, `dup_custom_kept_first` = 버린 custom 항목 수. `warn_total`(= 마커 `warn=`)은 이 합 + 응답 WARN(각 1, `extra_top_keys`는 키 개수).
- 문자열 필드는 strip 없이 **원문 유지**(§4.1 "API 문자열 그대로") — 빈값 판정만 strip 기준, 길이 판정은 원문 기준. 따라서 `" unknown "`은 `unknown_violation`에 걸리지 않고 mart alias 대조(`unregistered_model`)로 넘어간다.
- serving 레코드의 거부(계층 1)는 중복 model 판정보다 먼저 — 형태 위반 레코드는 `rejected`, 중복이 아니다(테스트 `test_serving_dup_model_after_rejected_record_is_reject_not_dup`).
- 응답 최상위 추가 키: 설계 §5.3 표 원문은 "최상위 추가 키(무시)"(WARN 없음)이나, 이 플랜은 **의도적 편차**로 적재는 그대로 하되 `warns["extra_top_keys"] = 키 수`를 남긴다(`W_EXTRA_KEYS`) — 제공자 스키마 드리프트를 로그로 관측하기 위함이고 거부·실패가 아니다. 6c는 이 코드를 오류로 취급하지 않는다(footer 설계 해석 7).
- `{"model": m, "custom": []}`은 스키마상 유효(`minProperties: 2`) → 행 0·rejected 0. `{"model": m}`만은 지표 0개 → 거부.
- `check_report_structure`의 `serviceGroup`/`service`/`generatedAt`은 `str()`로 담는다(비문자열 응답은 `identity_drift`·`generated_at_parse_failed`로 드러남 — 응답 단위 거부 목록(§5.3-1)에 없으므로 PERMANENT_ERROR로 올리지 않는다).
- `parse_generated_at`은 +09:00 입력도 `astimezone(KST)`로 통일해 반환(`tzinfo`가 항상 모듈 `KST` 객체와 동일 오프셋) — writer가 `DateTime('Asia/Seoul')`에 그대로 INSERT.

---

### Task 4: app/api_client.py — 단건 GET /v1/metrics?date= · HTTP→Event 번역표 · RETRYABLE 3회 · MAX_RESPONSE_BYTES 가드

**Files:**
- Create: `collectors/token-metrics/app/api_client.py`
- Test: `collectors/token-metrics/tests/test_api_client.py`
- Modify: 없음 (기존 모듈 `collectors/token-usage/app/api_client.py`는 관용구 복제 원본 — import·수정 없음)

**설계 근거:** §5.2 모드×게이트 표의 응답 행(250-256) — `409` → NOT_READY(큐 끝 1회 재방문은 T6 `main`이 담당, 여기서는 `retry_after_s = min(Retry-After, 300)`을 실어 즉시 던진다), `404` → RETENTION, `429/5xx/네트워크` → RETRYABLE 3회(5/25/125s, 캡 300s — **이 계층에서 소진**), `400 / 본문 > 5MB / date 에코 불일치 / non-JSON / 필수키 누락 / 비배열` → PERMANENT_ERROR · §5.3-1 "응답 단위(PERMANENT_ERROR)" 5종은 `_get_with_retry`(본문 크기·non-JSON)와 T3 `check_report_structure`(필수키·date 에코·비배열)가 나눠 맡는다 · §5.2 env `MAX_RESPONSE_BYTES=5000000`(T2 `Config.max_response_bytes`) · §5.1 클론 규칙(기존 `api_client.py` 124행에서 `UsagePayload`·summary 호출·페이지네이션·`invalid_cursor`→INVARIANT_BROKEN 분기·`PAGE_LIMIT`·`cfg.max_pages`를 제거하고 단건 GET 1회로 축소) · 계약 `token-metric-api.yaml` @6a552d2 `GET /v1/metrics?date=YYYY-MM-DD`(응답 `MetricsReport` 1건, 커서 없음).

**Interfaces:**
- Consumes:
  - T2 `app.config.Config`(`max_response_bytes: int = 5_000_000` — 본문 상한; 그 외 필드는 이 모듈이 읽지 않는다), `app.config.ServiceEntry`(`base_url: str` — `rstrip("/")` 완료 상태; frozen dataclass 위치 인자 순서 `service_group, service, base_url, enabled, api_since, coverage_since, until, …`).
  - T2 `app.events.Event`(`NOT_READY | RETRYABLE | PERMANENT_ERROR | RETENTION` 4종 사용 — 값은 소문자 `not_ready | retryable | permanent_error | retention`), `app.events.CollectError(event, message="", retry_after_s=0)`.
  - T3 `app.normalize.check_report_structure(body: object, expected_date: str) -> MetricsPayload`(위반 시 `PayloadError` — 메시지 코드 `not_object | missing_keys:<k1,k2,…> | date_mismatch | gpu_not_array | serving_not_array`), `app.normalize.PayloadError`, `app.normalize.MetricsPayload`(`source_type == "metrics-api-v1"` = `SOURCE_API`).
  - `requests`(`requests.RequestException` — 네트워크 예외 기반 클래스; `requests.ConnectionError`·`requests.Timeout`이 하위), 세션은 **주입**(운영: T6 `_session(cfg)`가 프록시/CA를 설정한 `requests.Session`; 테스트: `FakeSession`). 세션 대역이 갖춰야 할 계약: `get(url, params=None, timeout=None)` → 응답 객체(`status_code: int`, `headers: Mapping`, `content: bytes`, `json() -> object`).
- Produces (T6 `fetch_metrics` 소비 — `run_collection(..., fetcher=api_client.fetch_metrics)`; 나머지는 이 모듈 내부·테스트 계약):
  - 상수: `RETRY_AFTER_CAP_S = 300`, `RETRYABLE_ATTEMPTS = 3`, `BACKOFF_S = (5, 25, 125)`, `HTTP_TIMEOUT_S = 60`, `METRICS_PATH = "/v1/metrics"`.
  - `def _capped_retry_after(resp) -> int` — `min(int(resp.headers.get("Retry-After", "5")), RETRY_AFTER_CAP_S)`; 정수 아님 → 5 (기존 모듈 복제).
  - `def _error_code(resp) -> str` — `str(resp.json().get("code", ""))`; 어떤 예외든 `""` (복제).
  - `def _translate_error(resp) -> CollectError` — **번역표**: `409` → `CollectError(Event.NOT_READY, f"data_not_ready ({code})", retry_after_s=_capped_retry_after(resp))`; `404` → `CollectError(Event.RETENTION, f"data_not_retained ({code})")`; `429` 또는 `>= 500` → `CollectError(Event.RETRYABLE, f"http {sc} ({code})", retry_after_s=_capped_retry_after(resp) if "Retry-After" in resp.headers else 0)`; **그 외 전부(400 포함)** → `CollectError(Event.PERMANENT_ERROR, f"http {sc} ({code})")`. `invalid_cursor`·INVARIANT_BROKEN 분기 없음.
  - `def _get_with_retry(session, url: str, params: dict, max_bytes: int) -> object` — `session.get(url, params=params, timeout=HTTP_TIMEOUT_S)` 최대 `RETRYABLE_ATTEMPTS`회. `requests.RequestException` → `CollectError(Event.RETRYABLE, f"network: {type(exc).__name__}")`(재시도 대상). 200이면 **먼저** `len(resp.content) > max_bytes` → `CollectError(Event.PERMANENT_ERROR, f"body too large: {n} > {max_bytes}")`, 다음 `resp.json()` 실패(어떤 예외든) → `CollectError(Event.PERMANENT_ERROR, "malformed json body (http 200)")`, 성공 시 파싱 결과(`object` — dict가 아닐 수 있음, 판정은 `check_report_structure`)를 반환. 비-200은 `_translate_error` → RETRYABLE이 아니면 즉시 raise; RETRYABLE은 `time.sleep(min(err.retry_after_s or BACKOFF_S[attempt], RETRY_AFTER_CAP_S))` 후 재시도(마지막 시도 뒤에는 대기 없음), 3회 소진 시 마지막 RETRYABLE raise.
  - `def fetch_metrics(entry: ServiceEntry, date: str, cfg: Config, session) -> MetricsPayload` — **GET 1회** `f"{entry.base_url}{METRICS_PATH}"`, `params={"date": date}`(limit·cursor 없음) → `check_report_structure(body, date)`; `PayloadError as e` → `CollectError(Event.PERMANENT_ERROR, f"report structure: {e}")`(`from e`). `CollectError`는 그대로 전파. 로그 출력 없음(페이로드 금지 — 카운트·코드만 남기는 것은 T6 마커).
  - 테스트 대역(T6 `test_main.py`가 같은 모양을 재사용할 수 있도록 형태 고정): `FakeResponse(status_code, body=None, headers=None, content=None)`(`content` 미지정 시 `json.dumps(body).encode()`; `body=None` → `{}`), `FakeSession(script)`(`script = [(url_substr, response | callable)]`, `calls: list[(url, params)]`, `timeouts: list`).

- [ ] **Step 1: 전제 확인 — T2·T3 산출물과 `requests` 가용성**

Run: `cd collectors/token-metrics && ls app/__init__.py app/config.py app/events.py app/normalize.py conftest.py tests/__init__.py && grep -n "max_response_bytes: int" app/config.py && grep -n "^def check_report_structure\|^class PayloadError\|^class MetricsPayload" app/normalize.py && grep -n "RETRYABLE = \|PERMANENT_ERROR = " app/events.py`
Expected: 6개 파일 경로가 출력되고, `app/config.py:<n>:    max_response_bytes: int = 5_000_000`, `app/normalize.py` 3줄(`class PayloadError(ValueError):`·`class MetricsPayload:`·`def check_report_structure(body: object, expected_date: str) -> MetricsPayload:`), `app/events.py` 2줄(`RETRYABLE = "retryable"`, `PERMANENT_ERROR = "permanent_error"`). 하나라도 없으면 T2/T3가 병합되지 않은 것 — 중단하고 보고한다(대신 만들지 않는다).

Run: `cd collectors/token-metrics && python3 -c "import requests; print(issubclass(requests.ConnectionError, requests.RequestException))" && python3 -m pytest -q 2>&1 | tail -1`
Expected: `True` 1줄(네트워크 예외 계층 확인 — `_get_with_retry`가 `requests.RequestException` 하나로 잡는다) + 현재 통과 수 `52 passed`(T2 14 + T3 38; T2/T3 커밋의 실제 수가 다르면 그 합 — 이 수를 Step 7의 누적 기대값에 쓴다).

Run: `sed -n 250,256p docs/superpowers/specs/2026-08-31-token-metrics-ingest-design.md | grep -c "409\|404\|429/5xx\|400 / 본문"`
Expected: `5`(§5.2 표의 응답 행 409·404·429/5xx·400 4개 + "앵커 존재 & --replace" 행의 "rerun의 404" 언급 1개 — 이 태스크의 번역표가 여기서 온다).

- [ ] **Step 2: 실패하는 테스트 1 — 대역·fixture·HTTP→Event 번역표·RETRYABLE 3회·본문 가드** — `collectors/token-metrics/tests/test_api_client.py` 신규(전체 내용)

```python
"""api_client 테스트 — FakeSession 스크립트 패턴(기존 모듈 test_api_client.py 관용구 복제).

모든 테스트는 `time.sleep`을 패치한다(autouse fixture `sl`) — 재시도 대기 스케줄은 호출 인자로 검증.
공통 fixture 상수는 Plan 6b 전 태스크 공통.
"""
import json
from datetime import date
from unittest.mock import call, patch

import pytest
import requests

from app.api_client import (BACKOFF_S, HTTP_TIMEOUT_S, METRICS_PATH, RETRY_AFTER_CAP_S,
                            RETRYABLE_ATTEMPTS, fetch_metrics)
from app.config import Config, ServiceEntry
from app.events import CollectError, Event
from app.normalize import SOURCE_API, MetricsPayload

SERVICE_GROUP = "Mock Group"
SERVICE = "Mock Service A"
DATE = "2026-09-10"
GPU_TYPE = "H100"
ENGINE = {"type": "vllm", "version": "0.10.1"}
GENERATED_AT = "2026-09-11T02:05:00+09:00"

ENTRY = ServiceEntry(SERVICE_GROUP, SERVICE, "http://svc", True,
                     date(2026, 9, 9), date(2026, 8, 26), None)
CFG = Config()
URL = "http://svc/v1/metrics"

REPORT = {
    "date": DATE, "serviceGroup": SERVICE_GROUP, "service": SERVICE,
    "generatedAt": GENERATED_AT, "engine": ENGINE,
    "gpu": [{"model": "claude-opus-4-8", "gpuType": GPU_TYPE, "category": "serving",
             "gpuCount": 8, "gpuHours": 192}],
    "serving": [{"model": "claude-opus-4-8",
                 "ttftMs": {"p50": 100, "p90": 200, "p95": 250, "p99": 400},
                 "outputTps": {"p50": 50}}],
}


class FakeResponse:
    """requests.Response 대역 — status_code / headers / content(bytes) / json()."""

    def __init__(self, status_code, body=None, headers=None, content=None):
        self.status_code = status_code
        self._body = {} if body is None else body          # `[]`도 그대로 보존(not_object 검증용)
        self.headers = headers or {}
        self.content = json.dumps(self._body).encode() if content is None else content

    def json(self):
        return self._body


class BadJsonResponse(FakeResponse):
    def json(self):
        raise ValueError("not json")


class FakeSession:
    """스크립트된 응답 시퀀스를 돌려주는 requests.Session 대역 — 호출 순서·params·timeout 기록."""

    def __init__(self, script):
        self.script = list(script)   # (url_substr, response | callable) — 순서 검증
        self.calls = []
        self.timeouts = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        self.timeouts.append(timeout)
        assert self.script, f"unexpected extra call {url}"
        expect_substr, resp = self.script.pop(0)
        assert expect_substr in url, f"unexpected call {url}, expected {expect_substr}"
        return resp() if callable(resp) else resp


def _raise_conn_error():
    raise requests.ConnectionError("boom")


@pytest.fixture(autouse=True)
def sl():
    """time.sleep 대역 — 실제 대기 없이 호출 인자만 기록."""
    with patch("app.api_client.time.sleep") as m:
        yield m


# ---------- 상수 (§5.2 "재시도 3회(5/25/125s, 캡 300s)") ----------

def test_constants_match_design():
    assert RETRY_AFTER_CAP_S == 300
    assert RETRYABLE_ATTEMPTS == 3
    assert BACKOFF_S == (5, 25, 125)
    assert HTTP_TIMEOUT_S == 60
    assert METRICS_PATH == "/v1/metrics"


# ---------- 번역표: 409 / 404 / 400 / 그 외 4xx ----------

def test_409_not_ready_capped_retry_after(sl):
    s = FakeSession([(METRICS_PATH, FakeResponse(409, {"code": "data_not_ready", "message": "x"},
                                                 headers={"Retry-After": "900"}))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.NOT_READY
    assert ei.value.retry_after_s == 300           # min(Retry-After, 300) 캡 (§5.2)
    assert "data_not_ready" in ei.value.message
    assert sl.call_count == 0                       # NOT_READY는 즉시 던진다 — 재방문은 main 큐 담당
    assert len(s.calls) == 1


def test_409_without_retry_after_defaults_to_5():
    s = FakeSession([(METRICS_PATH, FakeResponse(409, {"code": "data_not_ready"}))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.NOT_READY and ei.value.retry_after_s == 5


def test_404_retention():
    s = FakeSession([(METRICS_PATH, FakeResponse(404, {"code": "data_not_retained", "message": "x"}))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.RETENTION
    assert ei.value.retry_after_s == 0


def test_400_permanent():
    s = FakeSession([(METRICS_PATH, FakeResponse(400, {"code": "invalid_date", "message": "x"}))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "http 400 (invalid_date)"


def test_418_permanent_no_retry(sl):
    # 429·5xx 외의 비-200은 전부 PERMANENT_ERROR — 재시도·대기 없음
    s = FakeSession([(METRICS_PATH, FakeResponse(418, {"code": "teapot"}))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert sl.call_count == 0 and len(s.calls) == 1
```

- [ ] **Step 3: 실패하는 테스트 2 — RETRYABLE 3회 소진·대기 스케줄·본문 가드·구조 위반·단건 호출 계약** — `collectors/token-metrics/tests/test_api_client.py` **끝에 추가**

```python


# ---------- RETRYABLE: 429 / 5xx / 네트워크 — 이 계층에서 3회 소진 ----------

def test_429_then_200_retries_with_retry_after(sl):
    s = FakeSession([
        (METRICS_PATH, FakeResponse(429, {"code": "rate_limited"}, headers={"Retry-After": "7"})),
        (METRICS_PATH, FakeResponse(200, REPORT)),
    ])
    payload = fetch_metrics(ENTRY, DATE, CFG, s)
    assert isinstance(payload, MetricsPayload)
    assert sl.call_args_list == [call(7)]          # Retry-After 우선, 백오프 대신
    assert len(s.calls) == 2


def test_429_retry_after_capped_at_300(sl):
    s = FakeSession([
        (METRICS_PATH, FakeResponse(429, {"code": "rate_limited"}, headers={"Retry-After": "9999"})),
        (METRICS_PATH, FakeResponse(200, REPORT)),
    ])
    fetch_metrics(ENTRY, DATE, CFG, s)
    assert sl.call_args_list == [call(RETRY_AFTER_CAP_S)]


def test_503_three_times_exhausts(sl):
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        return FakeResponse(503, {"code": "service_unavailable", "message": "x"})

    s = FakeSession([(METRICS_PATH, flaky)] * 3)
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.RETRYABLE
    assert ei.value.message == "http 503 (service_unavailable)"
    assert calls["n"] == RETRYABLE_ATTEMPTS
    assert sl.call_args_list == [call(5), call(25)]   # 마지막 시도 뒤에는 대기 없음 (§5.2 5/25/125)


def test_5xx_retry_after_zero_falls_back_to_backoff(sl):
    # Retry-After: 0 은 "대기값 없음"과 같다 — 백오프 스케줄 사용
    s = FakeSession([(METRICS_PATH, FakeResponse(502, {"code": "bad_gateway"},
                                                 headers={"Retry-After": "0"}))] * 3)
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.RETRYABLE
    assert sl.call_args_list == [call(5), call(25)]


def test_network_error_retryable(sl):
    s = FakeSession([(METRICS_PATH, _raise_conn_error)] * 3)
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.RETRYABLE
    assert ei.value.message == "network: ConnectionError"
    assert len(s.calls) == 3 and sl.call_args_list == [call(5), call(25)]


def test_network_error_then_200_recovers(sl):
    s = FakeSession([(METRICS_PATH, _raise_conn_error), (METRICS_PATH, FakeResponse(200, REPORT))])
    payload = fetch_metrics(ENTRY, DATE, CFG, s)
    assert payload.reported_service == SERVICE and sl.call_args_list == [call(5)]


# ---------- 200 본문 가드: MAX_RESPONSE_BYTES / non-JSON (§5.2 PERMANENT_ERROR 행) ----------

def test_body_over_max_bytes_permanent(sl):
    small_cfg = Config(max_response_bytes=10)
    s = FakeSession([(METRICS_PATH, FakeResponse(200, REPORT, content=b"x" * 11))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, small_cfg, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "body too large: 11 > 10"
    assert sl.call_count == 0                       # 재시도 대상 아님


def test_body_at_max_bytes_is_accepted():
    exact = json.dumps(REPORT).encode()
    s = FakeSession([(METRICS_PATH, FakeResponse(200, REPORT, content=exact))])
    payload = fetch_metrics(ENTRY, DATE, Config(max_response_bytes=len(exact)), s)
    assert payload.reported_service == SERVICE      # 경계값(== max)은 통과 — 초과(>)만 거부


def test_malformed_json_permanent(sl):
    s = FakeSession([(METRICS_PATH, BadJsonResponse(200, {}))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "malformed json body (http 200)"
    assert sl.call_count == 0


# ---------- 구조 위반 → PERMANENT_ERROR "report structure: <코드>" (§5.3-1 응답 단위) ----------

def test_date_echo_mismatch_permanent():
    s = FakeSession([(METRICS_PATH, FakeResponse(200, dict(REPORT, date="2026-09-09")))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "report structure: date_mismatch"


def test_missing_required_key_permanent():
    body = {k: v for k, v in REPORT.items() if k != "gpu"}
    s = FakeSession([(METRICS_PATH, FakeResponse(200, body))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "report structure: missing_keys:gpu"


def test_non_array_permanent():
    s = FakeSession([(METRICS_PATH, FakeResponse(200, dict(REPORT, serving={})))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "report structure: serving_not_array"


def test_not_object_permanent():
    s = FakeSession([(METRICS_PATH, FakeResponse(200, []))])   # JSON 배열 최상위
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "report structure: not_object"


# ---------- 정상 경로·단건 호출 계약 ----------

def test_happy_path_single_get(sl):
    s = FakeSession([(METRICS_PATH, FakeResponse(200, REPORT))])
    payload = fetch_metrics(ENTRY, DATE, CFG, s)
    assert isinstance(payload, MetricsPayload)
    assert payload.source_type == SOURCE_API == "metrics-api-v1"
    assert payload.date == DATE
    assert payload.reported_service_group == SERVICE_GROUP
    assert payload.reported_service == SERVICE
    assert payload.generated_at_raw == GENERATED_AT
    assert payload.engine == ENGINE
    assert len(payload.gpu) == 1 and len(payload.serving) == 1
    assert payload.extra_top_keys == []
    assert s.calls == [(URL, {"date": DATE})]       # 호출 1회, params는 date만 (limit·cursor 없음)
    assert s.timeouts == [HTTP_TIMEOUT_S]
    assert sl.call_count == 0


def test_no_summary_or_pagination_calls():
    s = FakeSession([(METRICS_PATH, FakeResponse(200, REPORT))])
    fetch_metrics(ENTRY, DATE, CFG, s)
    assert s.script == [] and len(s.calls) == 1     # summary·다음 페이지 호출 없음
    assert all("/v1/usage" not in url for url, _ in s.calls)


def test_base_url_without_trailing_slash_joins_path():
    entry = ServiceEntry(SERVICE_GROUP, SERVICE, "http://svc:8000/root", True,
                         date(2026, 9, 9), date(2026, 8, 26), None)
    s = FakeSession([("http://svc:8000/root/v1/metrics", FakeResponse(200, REPORT))])
    fetch_metrics(entry, DATE, CFG, s)
    assert s.calls[0][0] == "http://svc:8000/root/v1/metrics"
```

- [ ] **Step 4: 실패 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_api_client.py`
Expected: `ImportError while importing test module '…/collectors/token-metrics/tests/test_api_client.py'` … `E   ModuleNotFoundError: No module named 'app.api_client'` → `Interrupted: 1 error during collection` (마지막 줄 `1 error in …s`).

- [ ] **Step 5: 구현 — 상수·번역표·`_get_with_retry`(본문 가드)·`fetch_metrics`** — `collectors/token-metrics/app/api_client.py` 신규(전체 내용, 108행)

```python
"""metrics-api-v1 클라이언트 — HTTP 신호를 공통 이벤트 분류로 번역 (설계 2026-08-31 §5.2 응답 행).

기존 usage-api-v1 수집기 api_client 의 클론(§5.1 — 원본 모듈은 zero-diff, import 없음) — summary 호출·
페이지네이션·`invalid_cursor` 분기를 제거하고 `GET /v1/metrics?date=` **단건 1회**로 축소했다.

번역표 (§5.2):
    409                       → NOT_READY  (retry_after_s = min(Retry-After, 300); 큐 끝 1회 재방문은 main 담당)
    404                       → RETENTION  (정기 FAILURE / rerun SKIPPED 은 main 담당)
    429 / 5xx / 네트워크 예외 → RETRYABLE  (이 계층에서 3회 소진: 5/25/125s, Retry-After 우선, 캡 300s)
    400 / 그 외 비-200        → PERMANENT_ERROR
    200 이지만 본문 > MAX_RESPONSE_BYTES / non-JSON → PERMANENT_ERROR
    200 이지만 필수키 누락 / date 에코 불일치 / gpu·serving 비배열 → PERMANENT_ERROR (normalize.check_report_structure)

세션은 주입받는다(테스트: FakeSession, 운영: main 이 프록시/CA 를 설정한 requests.Session).
로그 출력 없음 — 페이로드·행 원문은 어디에도 남기지 않는다(마커는 main 이 카운트·코드만 출력).
"""
from __future__ import annotations

import time

import requests

from app.config import Config, ServiceEntry
from app.events import CollectError, Event
from app.normalize import MetricsPayload, PayloadError, check_report_structure

RETRY_AFTER_CAP_S = 300          # min(Retry-After, 300s) (§5.2)
RETRYABLE_ATTEMPTS = 3
BACKOFF_S = (5, 25, 125)         # 지수 백오프 (§5.2) — 마지막 시도 뒤에는 대기하지 않으므로 125 는 예비값
HTTP_TIMEOUT_S = 60
METRICS_PATH = "/v1/metrics"     # 계약 @6a552d2 — 단건, 커서 없음


def _capped_retry_after(resp) -> int:
    try:
        return min(int(resp.headers.get("Retry-After", "5")), RETRY_AFTER_CAP_S)
    except ValueError:
        return 5


def _error_code(resp) -> str:
    try:
        return str(resp.json().get("code", ""))
    except Exception:
        return ""


def _translate_error(resp) -> CollectError:
    """비-200 응답 → CollectError (§5.2 metrics-api-v1 번역표). 페이지 재시작 분기 없음(응답 1건)."""
    sc = resp.status_code
    code = _error_code(resp)
    if sc == 409:
        return CollectError(Event.NOT_READY, f"data_not_ready ({code})",
                            retry_after_s=_capped_retry_after(resp))
    if sc == 404:
        return CollectError(Event.RETENTION, f"data_not_retained ({code})")
    if sc == 429 or sc >= 500:
        return CollectError(Event.RETRYABLE, f"http {sc} ({code})",
                            retry_after_s=_capped_retry_after(resp)
                            if "Retry-After" in resp.headers else 0)
    return CollectError(Event.PERMANENT_ERROR, f"http {sc} ({code})")   # 400 포함


def _get_with_retry(session, url: str, params: dict, max_bytes: int) -> object:
    """GET 1회 의미 단위 — RETRYABLE 만 내부 소진(≤3회), 그 외 즉시 번역해 던짐.

    200 은 본문 크기 → JSON 파싱 순으로 검사한다(둘 다 PERMANENT_ERROR, 재시도 없음).
    반환은 파싱된 JSON 값(dict 가 아닐 수 있음 — 구조 판정은 check_report_structure).
    """
    last: CollectError | None = None
    for attempt in range(RETRYABLE_ATTEMPTS):
        try:
            resp = session.get(url, params=params, timeout=HTTP_TIMEOUT_S)
        except requests.RequestException as exc:
            last = CollectError(Event.RETRYABLE, f"network: {type(exc).__name__}")
            if attempt < RETRYABLE_ATTEMPTS - 1:
                time.sleep(BACKOFF_S[attempt])
            continue
        if resp.status_code == 200:
            n = len(resp.content)
            if n > max_bytes:
                raise CollectError(Event.PERMANENT_ERROR, f"body too large: {n} > {max_bytes}")
            try:
                return resp.json()
            except Exception:
                raise CollectError(Event.PERMANENT_ERROR, "malformed json body (http 200)")
        err = _translate_error(resp)
        if err.event is not Event.RETRYABLE:
            raise err
        last = err
        if attempt < RETRYABLE_ATTEMPTS - 1:
            time.sleep(min(err.retry_after_s or BACKOFF_S[attempt], RETRY_AFTER_CAP_S))
    raise last  # type: ignore[misc]


def fetch_metrics(entry: ServiceEntry, date: str, cfg: Config, session) -> MetricsPayload:
    """(date, service) 스냅샷 1건: GET {base_url}/v1/metrics?date=<date> → 응답 단위 구조 검사.

    페이지 불변성 검사는 없다(응답 1건). 구조 위반(PayloadError)은 PERMANENT_ERROR 로 번역한다 —
    메시지 `report structure: <코드>` 의 코드는 normalize 의 어휘 그대로(not_object / missing_keys:… /
    date_mismatch / gpu_not_array / serving_not_array).
    """
    body = _get_with_retry(session, f"{entry.base_url}{METRICS_PATH}", {"date": date},
                           cfg.max_response_bytes)
    try:
        return check_report_structure(body, date)
    except PayloadError as e:
        raise CollectError(Event.PERMANENT_ERROR, f"report structure: {e}") from e
```

- [ ] **Step 6: 통과 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_api_client.py`
Expected: `22 passed`

- [ ] **Step 7: 전체 회귀 + 자기완결 경계 + 3.10 호환 + zero-diff**

Run: `cd collectors/token-metrics && python3 -m pytest -q`
Expected: Step 1에서 확인한 수 + 22 → `74 passed`(T2 14 + T3 38 + T4 22; T2/T3 실제 커밋의 수가 다르면 그 합 + 22).

Run: `cd collectors/token-metrics && grep -n "^import\|^from" app/api_client.py`
Expected: 정확히 6줄 — `from __future__ import annotations`, `import time`, `import requests`, `from app.config import Config, ServiceEntry`, `from app.events import CollectError, Event`, `from app.normalize import MetricsPayload, PayloadError, check_report_structure`(자기 모듈 `app.*`만 — 기존 수집기 import 0).

Run: `cd collectors/token-metrics && grep -rn "token_usage\|token-usage\|vm_push\|VM_PUSH_URL\|random\|max_pages\|PAGE_LIMIT\|nextCursor\|/v1/usage\|INVARIANT_BROKEN" app/api_client.py; echo "exit=$?"`
Expected: 출력 없음 + `exit=1`(footer 자기완결 grep 기준 — 원본 경로 문자열·페이지네이션 잔재·`random` 없음).

Run: `cd collectors/token-metrics && python3 -m py_compile app/api_client.py tests/test_api_client.py && python3 -c "import app.api_client as m; print(m.RETRY_AFTER_CAP_S, m.RETRYABLE_ATTEMPTS, m.BACKOFF_S, m.HTTP_TIMEOUT_S, m.METRICS_PATH)"`
Expected: `300 3 (5, 25, 125) 60 /v1/metrics`(개발기 3.10에서 컴파일·import 성공 — `from __future__ import annotations`로 `CollectError | None` 표기 허용).

Run: `git status --short collectors/token-metrics && git diff --stat -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml`
Expected: `?? collectors/token-metrics/app/api_client.py`·`?? collectors/token-metrics/tests/test_api_client.py` 2줄만, diff --stat 출력 없음(zero-diff 유지 — 기존 `collectors/token-usage/app/api_client.py`·`tests/test_api_client.py` 무변경).

- [ ] **Step 8: Commit**

```bash
git add collectors/token-metrics/app/api_client.py collectors/token-metrics/tests/test_api_client.py
git commit -m "feat(collectors-metrics): api_client — GET /v1/metrics 단건·번역표·RETRYABLE 3회·5MB 가드 (Plan 6b T4)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

**설계 해석(이 태스크에서 하나로 정한 항목 — footer Self-Review에 옮겨 적는다):**
- **본문 크기 가드는 응답 수신 후 `len(resp.content)` 기준**(스트리밍·`Content-Length` 선검사 없음). 설계 §5.2 "본문 > 5MB → PERMANENT_ERROR"의 판정 기준을 실제 수신 바이트로 고정한다 — `Content-Length`는 프록시·압축에 따라 부재/불일치할 수 있고, 5MB 상한은 메모리 보호가 아니라 계약 위반 판정이므로 사후 검사로 충분하다. 경계값 `== max_bytes`는 통과(초과 `>`만 거부, `test_body_at_max_bytes_is_accepted`).
- **검사 순서 200 → 크기 → JSON → 구조**: 크기 초과 본문은 파싱하지 않는다(메시지 `body too large: <n> > <max>`); 파싱 실패는 `malformed json body (http 200)`; 구조 위반은 `report structure: <normalize 코드>`. 세 메시지 모두 페이로드 원문을 포함하지 않는다(로그 페이로드 금지 — 카운트·코드만).
- **`_get_with_retry` 반환 타입 `object`**: 최상위가 JSON 배열/스칼라인 200 응답도 여기서 거부하지 않고 `check_report_structure`가 `not_object`로 판정한다 — 구조 판정 1벌(T3)을 유지하기 위함.
- **`Retry-After: 0`은 "값 없음"으로 취급**(`err.retry_after_s or BACKOFF_S[attempt]`) — 기존 모듈 관용구 복제. 409의 `retry_after_s`는 헤더 부재·비정수 시 5초(기존 관용구) — 큐 재방문 대기의 하한(`max(retry_after_s, 1)`)은 T6가 적용한다.
- **`429`와 `>= 500`만 RETRYABLE**; `4xx`의 나머지(400·401·403·418…)는 전부 PERMANENT_ERROR — 설계 표에 없는 코드를 재시도로 오해하지 않도록 보수적으로 고정. 네트워크 예외는 `requests.RequestException` 하나로 잡는다(`ConnectionError`·`Timeout`·`SSLError` 포함; 메시지 `network: <예외 클래스명>`).
- **URL 결합은 단순 문자열 연결** `f"{entry.base_url}{METRICS_PATH}"` — T2 `load_endpoints`가 `baseUrl.rstrip("/")`를 보장하므로 `urljoin`을 쓰지 않는다(`urljoin`은 `http://svc/root` + `/v1/metrics`에서 `root`를 버린다 — `test_base_url_without_trailing_slash_joins_path`가 하위 경로 보존을 고정).
- **테스트 대역은 `time.sleep`을 autouse fixture로 패치** — outline의 "전부 `with patch(...)`"와 같은 효과이며 대기 인자를 `call(n)`으로 검증한다. `FakeResponse.content`는 `json.dumps(body).encode()` 기본값 — 크기 가드 테스트만 `content=` 명시.

---

### Task 5: app/writer.py — 존재확인 3종 → 감사 → DELETE(summary→gpu→serving) → INSERT(gpu→serving→summary) · IN 배칭 · 뮤테이션 가드 · 레지스트리 diff-sync

**Files:**
- Create: `collectors/token-metrics/app/writer.py`
- Test: `collectors/token-metrics/tests/test_writer.py`
- Modify: 없음 (`collectors/token-usage/app/clickhouse_client.py`는 관용구 원본 — import·복사 없음, zero-diff)

**설계 근거:** §5.4(268-274) 적재 시퀀스 — (1) 존재 SELECT 3종(summary/gpu/serving)은 fetch·normalize·예산 가드 이후 DELETE 직전, 셋 다 없으면 DELETE 생략 / (2) 하나라도 있으면 앵커가 있을 때만 감사 INSERT → DELETE 순서 고정 summary(앵커)→gpu→serving(`_local`, ON CLUSTER, `mutations_sync=2`; 3 뮤테이션) / (3) INSERT 순서 gpu→serving→summary 마지막(`insert_distributed_sync=1`, `insert_deduplicate=0`) / 배칭 (B) 테이블당 `_delete_day_in(table, date, services)` 1회 → 날짜당 ≤3 · §4.0 뮤테이션 장부(119-128) — 정기 0, `--replace` 날짜당 fact ≤3, 감사 append-only, 실행당 가드 `METRICS_MAX_MUTATIONS_PER_RUN`(기본 45 = 3×15) "첫 DELETE 전 존재확인 선조회로 합산, 초과 시 `FAILURE reason=mutation_budget`" · §4.3(227)·D2.2 머리말 — 레지스트리 동기화는 endpoints 집합 vs 현재 행 diff(비교 키 = `updated_at` 제외 전 컬럼) → 다를 때만 ALTER DELETE(전체) + INSERT, 현재 집합이 비면 DELETE 생략(최초 배포 뮤테이션 0) · §5.2 표 "앵커 존재 … `SKIPPED reason=already_loaded`"(사전 판정 = summary만) / "앵커 없음 & 자식 행 있음 → 확장 존재확인이 DELETE×3 강제" / "manual-v0 앵커면 정기 경로에서 `CHECK WARN manual_row_present`"(→ `anchor_source_type`) · Plan 6a A(fact 4테이블 컬럼 순서·INSERT 컬럼 명시)·B(레지스트리 12컬럼) · D2.3 GRANT 범위(감사는 INSERT만 — 이 모듈은 감사 테이블에 DELETE를 보내지 않는다; ALTER DELETE는 gpu/serving/summary/dim `_local` 4개만).

**Interfaces:**
- Consumes:
  - T2 `app.config.Config`(`ch_host: str`, `ch_port: int`, `ch_user: str`, `ch_password: str`, `ch_cluster: str`(빈 값 = ON CLUSTER 생략), `max_mutations_per_run: int = 45`) — 나머지 필드는 읽지 않는다.
  - T2 `app.config.ServiceEntry`(frozen dataclass) — `service_group`, `service`, `dim_key() -> tuple`(11개: `service_group, service, base_url, int(enabled), api_since, coverage_since, until, int(expect_gpu), int(expect_serving), int(usage_includes_consumers), note`), `dim_row(updated_at: datetime) -> list`(= `list(dim_key()) + [updated_at]`, 12개).
  - T3 `app.normalize.MetricsPayload`(`reported_service_group: str`, `reported_service: str`, `source_type: str`), `GpuRow`(`model, gpu_type, category, gpu_count, gpu_hours, flags`), `ServingRow`(`model, metric, name, unit, p50, p90, p95, p99, flags`), `NormalizeResult`(`generated_at: datetime`(aware KST), `gpu_rows`, `serving_rows`, `rejected`, `merged_dups`, `engine_type`, `engine_version`; 프로퍼티 `n_gpu`, `n_serving`, `n_custom`, `rows`) — 테스트는 `NormalizeResult(generated_at=…, gpu_rows=[…], serving_rows=[…], …)` 키워드 조립. T3 `KST`·`SOURCE_API`(테스트 fixture).
  - Plan 6a A·B DDL(`collectors/token-metrics/ddl/company/raw_token_metrics.sql`·`dim_token_metrics_service.sql`) — 컬럼 순서의 정본; 테스트가 `_dist` DDL을 파싱해 컬럼 튜플과 대조한다.
  - `clickhouse_connect.get_client(host, port, username, password, settings)` → `client.query(sql, parameters=)`(`.result_rows`), `client.command(sql, parameters=, settings=)`, `client.insert(table, data, column_names=)`.
- Produces (T6 `MetricsWriter.anchor_exists/anchor_source_type/replace_batch/sync_registry`·`MutationBudgetExceeded` · T7 `insert_service_day` 행 규칙(manual은 `payload.reported_* = 레지스트리 값`) · T11 E2E가 `DB_FACT/DB_DIM`·컬럼 튜플·`CLIENT_SETTINGS`를 전제):
  - 모듈 상수: `DB_FACT = os.getenv("CH_DB_FACT", "fact")`, `DB_DIM = os.getenv("CH_DB_DIM", "gpu_data")`(모듈 로드 시 1회), `KST = timezone(timedelta(hours=9))`, `CLIENT_SETTINGS = {"insert_distributed_sync": 1, "insert_deduplicate": 0}`, 테이블명 `T_GPU = "raw_token_metrics_gpu_1d"`, `T_SERVING = "raw_token_metrics_serving_1d"`, `T_SUMMARY = "raw_token_metrics_summary_1d"`, `T_AUDIT = "collect_audit_metrics_1d"`, `T_DIM = "dim_token_metrics_service"`, `DELETE_ORDER = (T_SUMMARY, T_GPU, T_SERVING)`, `INSERT_ORDER = (T_GPU, T_SERVING, T_SUMMARY)`, 컬럼 튜플 `GPU_COLS`(12: `date, service_group, service, model, gpu_type, category, gpu_count, gpu_hours, flags, source_type, generated_at, collected_at`), `SERVING_COLS`(15: `date, service_group, service, model, metric, name, unit, p50, p90, p95, p99, flags, source_type, generated_at, collected_at`), `SUMMARY_COLS`(15: `date, service_group, service, reported_service_group, reported_service, engine_type, engine_version, gpu_rows, serving_rows, custom_rows, rejected_rows, merged_dups, source_type, generated_at, collected_at`), `AUDIT_COLS`(9: `date, service, prev_generated_at, prev_collected_at, prev_source_type, prev_gpu_rows, prev_gpu_hours_sum, prev_serving_rows, replaced_at`), `DIM_COLS`(12: `service_group, service, base_url, enabled, api_since, coverage_since, until, expect_gpu, expect_serving, usage_includes_consumers, note, updated_at`), `MUTATIONS_SYNC = {"mutations_sync": 2}`.
  - `def now_kst() -> datetime` — `datetime.now(KST)`(aware; 기존 모듈 C2 회귀 방지 관용구 복제).
  - `class MutationBudgetExceeded(Exception)`: `__init__(self, planned: int, done: int, limit: int)`, 속성 `planned/done/limit`, `str()` = `f"planned={planned} done={done} limit={limit}"`.
  - `class MetricsWriter`:
    - `__init__(self, cfg: Config, client=None)` — `client or clickhouse_connect.get_client(host=cfg.ch_host, port=cfg.ch_port, username=cfg.ch_user, password=cfg.ch_password, settings=CLIENT_SETTINGS)`; `self.mutations_done: int = 0`(실행 누적 — fact DELETE·레지스트리 DELETE마다 +1).
    - `_on_cluster(self) -> str` — `f" ON CLUSTER '{cfg.ch_cluster}'"` 또는 `""`; `_dist(self, name: str) -> str` = `f"{DB_FACT}.{name}_dist"`; `_local(self, name: str) -> str` = `f"{DB_FACT}.{name}_local"`(fact 전용 — 레지스트리는 `DB_DIM` 직접 조합).
    - `anchor_exists(self, date: str, service: str) -> bool` — `SELECT count() FROM {self._dist(T_SUMMARY)} WHERE date = %(d)s AND service = %(s)s`(사전 `already_loaded` 판정 — summary만).
    - `anchor_source_type(self, date: str, service: str) -> str | None` — `SELECT source_type FROM {self._dist(T_SUMMARY)} WHERE date = %(d)s AND service = %(s)s ORDER BY collected_at DESC LIMIT 1`(없으면 None; T6 `manual_row_present` WARN용).
    - `existing_services(self, date: str, services: list[str]) -> dict[str, set[str]]` — 3테이블 각각 `SELECT DISTINCT service FROM {dist} WHERE date = %(d)s AND service IN %(ss)s`(`parameters={"d": date, "ss": sorted(set(services))}`) → `{T_SUMMARY: {...}, T_GPU: {...}, T_SERVING: {...}}`(빈 `services` → 쿼리 없이 빈 집합 3개).
    - `fetch_prev_summary(self, date: str, service: str) -> dict | None` — summary `SELECT generated_at, collected_at, source_type … ORDER BY collected_at DESC LIMIT 1`(없으면 None) + gpu `SELECT count(), sum(gpu_hours)` + serving `SELECT count()` → 키 `prev_generated_at, prev_collected_at, prev_source_type, prev_gpu_rows, prev_gpu_hours_sum`(None→`0.0`), `prev_serving_rows`.
    - `_delete_day_in(self, table_local: str, date: str, services: list[str]) -> None` — `ALTER TABLE {table_local}{on_cluster} DELETE WHERE date = %(d)s AND service IN %(ss)s`, `parameters={"d": date, "ss": sorted(services)}`, `settings=MUTATIONS_SYNC`; `self.mutations_done += 1`.
    - `insert_service_day(self, entry: ServiceEntry, date: str, payload: MetricsPayload, result: NormalizeResult, collected_at: datetime) -> int` — `date_v = date_t.fromisoformat(date)`; INSERT `self._dist(T_GPU)`(행 `[date_v, entry.service_group, entry.service, r.model, r.gpu_type, r.category, r.gpu_count, r.gpu_hours, list(r.flags), payload.source_type, result.generated_at, collected_at]`, 0행이면 INSERT 생략) → `self._dist(T_SERVING)`(행 `[date_v, entry.service_group, entry.service, r.model, r.metric, r.name, r.unit, r.p50, r.p90, r.p95, r.p99, list(r.flags), payload.source_type, result.generated_at, collected_at]`, None 그대로, 0행이면 생략) → `self._dist(T_SUMMARY)` 1행 `[date_v, entry.service_group, entry.service, payload.reported_service_group, payload.reported_service, result.engine_type, result.engine_version, result.n_gpu, result.n_serving, result.n_custom, result.rejected, result.merged_dups, payload.source_type, result.generated_at, collected_at]`(항상 — NODATA 앵커 포함); 반환 `result.rows`. 모든 INSERT는 `column_names=` 명시.
    - `replace_batch(self, date: str, items: list[tuple[ServiceEntry, MetricsPayload, NormalizeResult]]) -> dict[str, int]` — (1) `existing = existing_services(date, [e.service for e, _, _ in items])`, `affected = sorted(set().union(*existing.values()))`; (2) `planned = 3 if affected else 0`, `if self.mutations_done + planned > cfg.max_mutations_per_run: raise MutationBudgetExceeded(planned, self.mutations_done, cfg.max_mutations_per_run)`(DELETE·INSERT 전 — 모든 모드 공통); (3) `for svc in sorted(existing[T_SUMMARY])`: `prev = fetch_prev_summary(date, svc)` → prev가 있으면 `insert(audit_dist, [[date_v, svc, prev[6종], now_kst()]], column_names=AUDIT_COLS)`; (4) `if affected: for t in DELETE_ORDER: _delete_day_in(_local(t), date, affected)`(정확히 3회 — 자식 행만 남은 잔여물도 3회 강제); (5) `collected_at = now_kst()` 1회 → 각 item `insert_service_day(...)` → `{service: rows}`. `items == []` → 쿼리 없이 `{}`. 정기 경로(T6)는 item 1개로 서비스별 호출, `--replace`/manual은 날짜당 1회(배칭).
    - `sync_registry(self, entries: list[ServiceEntry]) -> bool` — `desired = [e.dim_key() for e in entries]`, `current = SELECT <DIM_COLS[:11] 11컬럼 명시> FROM {DB_DIM}.{T_DIM}_dist` 튜플 목록; 둘 다 `_dim_sort_key`(원소를 문자열로 바꾼 튜플 — `until` None 혼합 정렬 안전)로 정렬해 `==` 비교 → 같으면 `False`(쿼리 1회, 뮤테이션 0); 다르면 `current`가 비어있지 않을 때만 `ALTER TABLE {DB_DIM}.{T_DIM}_local{on_cluster} DELETE WHERE 1`(`settings=MUTATIONS_SYNC`, `mutations_done += 1`) → `insert(f"{DB_DIM}.{T_DIM}_dist", [e.dim_row(now) for e in entries], column_names=DIM_COLS)`(`now = now_kst()` 1회) → `True`.
  - 모듈 함수 `def _dim_sort_key(row: tuple) -> tuple[str, ...]` — `tuple("" if v is None else str(v) for v in row)`.

- [ ] **Step 1: 전제 확인 — T2·T3 산출물 + Plan 6a DDL(컬럼 순서 정본)**

Run: `cd collectors/token-metrics && ls app/config.py app/normalize.py conftest.py tests/__init__.py ddl/company/raw_token_metrics.sql ddl/company/dim_token_metrics_service.sql ddl/company/accounts.sql && grep -n "def dim_key\|def dim_row\|max_mutations_per_run: int" app/config.py && grep -n "^class NormalizeResult\|^class GpuRow\|^class ServingRow\|^class MetricsPayload" app/normalize.py`
Expected: 7개 파일 경로가 출력되고, `app/config.py`에서 `max_mutations_per_run: int = 45`·`def dim_key`·`def dim_row` 3줄, `app/normalize.py`에서 dataclass 4줄. 파일이 없으면 T2/T3 또는 Plan 6a가 병합되지 않은 것 — 중단하고 보고한다(대신 만들지 않는다).

Run: `cd collectors/token-metrics && grep -c "GRANT ALTER DELETE" ddl/company/accounts.sql && grep -n "GRANT ALTER DELETE" ddl/company/accounts.sql | grep -c "collect_audit"`
Expected: `4` 다음 줄 `0` — ALTER DELETE 권한은 gpu/serving/summary/dim `_local` 4개뿐, 감사 테이블에는 없다(이 태스크가 감사 테이블에 DELETE를 보내지 않는 근거).

Run: `cd collectors/token-metrics && awk '/CREATE TABLE IF NOT EXISTS fact.raw_token_metrics_summary_1d_dist/,/^\)/' ddl/company/raw_token_metrics.sql | grep -c "^    [a-z_]"`
Expected: `15`(summary `_dist` 컬럼 수 — `SUMMARY_COLS` 길이와 같아야 한다; gpu 12·serving 15·audit 9·dim 12는 Step 2 테스트가 파싱해 대조).

- [ ] **Step 2: 실패하는 테스트 1 — 상수·컬럼 튜플(DDL 대조)·`now_kst`·존재확인 3종·`fetch_prev_summary`·`_delete_day_in`·`MutationBudgetExceeded`·DB env** — `collectors/token-metrics/tests/test_writer.py` 신규(전체 내용)

```python
"""writer(§5.4 적재 시퀀스 · §4.0 뮤테이션 장부 · §4.3 레지스트리 diff-sync) 테스트 — FakeCH, 실제 CH 없음.
공통 fixture 상수는 Plan 6b 전 태스크 공통(Mock Group / Mock Service A / 2026-09-10)."""
import subprocess
import sys
from datetime import date as date_t, datetime
from pathlib import Path

import pytest

from app.config import Config, ServiceEntry
from app.normalize import KST, SOURCE_API, GpuRow, MetricsPayload, NormalizeResult, ServingRow
from app.writer import (AUDIT_COLS, CLIENT_SETTINGS, DB_DIM, DB_FACT, DELETE_ORDER, DIM_COLS,
                        GPU_COLS, INSERT_ORDER, MUTATIONS_SYNC, SERVING_COLS, SUMMARY_COLS,
                        T_AUDIT, T_DIM, T_GPU, T_SERVING, T_SUMMARY, MetricsWriter,
                        MutationBudgetExceeded, now_kst)

MODULE_ROOT = Path(__file__).resolve().parent.parent
DDL_FACT = MODULE_ROOT / "ddl" / "company" / "raw_token_metrics.sql"
DDL_DIM = MODULE_ROOT / "ddl" / "company" / "dim_token_metrics_service.sql"

SERVICE_GROUP = "Mock Group"
SERVICE = "Mock Service A"
BASE_URL = "http://mock"
DATE = "2026-09-10"
DATE_V = date_t(2026, 9, 10)
GENERATED_AT = datetime(2026, 9, 11, 2, 5, tzinfo=KST)
PREV_GEN = datetime(2026, 8, 27, 9, 0, tzinfo=KST)
PREV_COL = datetime(2026, 8, 27, 9, 10, tzinfo=KST)
ALL_TABLES = (T_SUMMARY, T_GPU, T_SERVING)


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeCH:
    """존재확인 3종 · prev summary · gpu/serving 집계 · 레지스트리 SELECT를 테이블명 부분문자열로 라우팅.
    existing: {T_SUMMARY: {svc,…}, T_GPU: {…}, T_SERVING: {…}} (없는 키 = 빈 집합).
    prev_summary: (generated_at, collected_at, source_type) | None. gpu_agg: (count, sum(gpu_hours)).
    dim_rows: 레지스트리 현재 행(updated_at 제외 11컬럼 튜플) 목록."""

    def __init__(self, existing=None, prev_summary=None, gpu_agg=(0, None), serving_count=0,
                 dim_rows=None):
        existing = existing or {}
        self.existing = {t: set(existing.get(t, ())) for t in ALL_TABLES}
        self.prev_summary = prev_summary
        self.gpu_agg = gpu_agg
        self.serving_count = serving_count
        self.dim_rows = [tuple(r) for r in (dim_rows or [])]
        self.events = []        # ("command", 정규화 sql) | ("insert", table) — 호출 순서
        self.queries = []       # (정규화 sql, parameters)
        self.commands = []      # (정규화 sql, parameters, settings)
        self.inserts = []       # (table, row_count, column_names)
        self.insert_rows = []   # (table, data)

    def query(self, sql, parameters=None):
        norm = " ".join(sql.split())
        self.queries.append((norm, parameters))
        p = parameters or {}
        if "DISTINCT service" in norm:
            for t in ALL_TABLES:
                if t + "_dist" in norm:
                    hit = self.existing[t] & set(p.get("ss", ()))
                    return FakeResult([[s] for s in sorted(hit)])
            return FakeResult([])
        if T_DIM + "_dist" in norm:
            return FakeResult([list(r) for r in self.dim_rows])
        if T_SUMMARY + "_dist" in norm and "generated_at" in norm:
            return FakeResult([list(self.prev_summary)] if self.prev_summary else [])
        if T_SUMMARY + "_dist" in norm and "source_type" in norm:
            return FakeResult([[self.prev_summary[2]]] if self.prev_summary else [])
        if T_SUMMARY + "_dist" in norm and "count()" in norm:
            return FakeResult([[1 if p.get("s") in self.existing[T_SUMMARY] else 0]])
        if T_GPU + "_dist" in norm and "sum(gpu_hours)" in norm:
            return FakeResult([list(self.gpu_agg)])
        if T_SERVING + "_dist" in norm and "count()" in norm:
            return FakeResult([[self.serving_count]])
        return FakeResult([])

    def command(self, sql, parameters=None, settings=None):
        norm = " ".join(sql.split())
        self.commands.append((norm, parameters, settings))
        self.events.append(("command", norm))

    def insert(self, table, data, column_names=None):
        self.inserts.append((table, len(data), tuple(column_names or ())))
        self.insert_rows.append((table, data))
        self.events.append(("insert", table))


def entry(service=SERVICE, **kw) -> ServiceEntry:
    base = dict(service_group=SERVICE_GROUP, service=service, base_url=BASE_URL, enabled=True,
                api_since=date_t(2026, 9, 9), coverage_since=date_t(2026, 8, 26), until=None)
    base.update(kw)
    return ServiceEntry(**base)


def payload(service=SERVICE) -> MetricsPayload:
    return MetricsPayload(date=DATE, reported_service_group=SERVICE_GROUP, reported_service=service,
                          generated_at_raw="2026-09-11T02:05:00+09:00",
                          engine={"type": "vllm", "version": "0.10.1"},
                          gpu=[], serving=[], source_type=SOURCE_API)


_STD = (("ttft_ms", "ms", 280.0, 560.0, 720.0, 1200.0),
        ("itl_ms", "ms", 24.0, 38.0, 47.0, 80.0),
        ("output_tps", "tokens/s", 41.0, None, None, None))   # output_tps는 p50만 (p90..p99 None)


def result(n_gpu=2, n_serving=3, n_custom=0) -> NormalizeResult:
    """T3 dataclass 직접 조립 (n_serving <= 3: ttft_ms, itl_ms, output_tps 순)."""
    gpu = [GpuRow(model=f"m{i}", gpu_type="H100", category="serving", gpu_count=2.0,
                  gpu_hours=48.0, flags=[]) for i in range(n_gpu)]
    serving = [ServingRow(model="m0", metric=m, name="", unit=u, p50=a, p90=b, p95=c, p99=d, flags=[])
               for m, u, a, b, c, d in _STD[:n_serving]]
    serving += [ServingRow(model="m0", metric="custom", name=f"c{i}", unit="ms", p50=1.0, p90=None,
                           p95=None, p99=None, flags=[]) for i in range(n_custom)]
    return NormalizeResult(generated_at=GENERATED_AT, gpu_rows=gpu, serving_rows=serving,
                           engine_type="vllm", engine_version="0.10.1")


def writer(ch: FakeCH, **cfg_kw) -> MetricsWriter:
    return MetricsWriter(Config(**cfg_kw), client=ch)


def _ddl_columns(path: Path, table: str) -> list[str]:
    """`CREATE TABLE IF NOT EXISTS <table>` 의 컬럼 목록(첫 토큰)을 선언 순서로 — `_dist` 전용(COMMENT·DEFAULT 없음)."""
    text = path.read_text(encoding="utf-8")
    body = text[text.index(f"CREATE TABLE IF NOT EXISTS {table}"):]
    body = body[body.index("(") + 1:]
    body = body[:body.index("\n)")]
    return [line.strip().split()[0] for line in body.splitlines()
            if line.strip() and not line.strip().startswith("--")]


# ---- 상수·컬럼 튜플 -----------------------------------------------------------

def test_client_settings_constant():
    assert CLIENT_SETTINGS == {"insert_distributed_sync": 1, "insert_deduplicate": 0}   # §5.4 (3)
    assert MUTATIONS_SYNC == {"mutations_sync": 2}                                       # §5.4 (2)


def test_table_names_and_orders():
    assert (T_GPU, T_SERVING, T_SUMMARY, T_AUDIT, T_DIM) == (
        "raw_token_metrics_gpu_1d", "raw_token_metrics_serving_1d", "raw_token_metrics_summary_1d",
        "collect_audit_metrics_1d", "dim_token_metrics_service")
    assert DELETE_ORDER == (T_SUMMARY, T_GPU, T_SERVING)     # 앵커 먼저 지운다
    assert INSERT_ORDER == (T_GPU, T_SERVING, T_SUMMARY)     # 앵커 마지막에 넣는다
    assert T_AUDIT not in DELETE_ORDER                       # 감사는 append-only


def test_column_tuples_match_dist_ddl():
    """INSERT는 컬럼 목록 명시(Plan 6a) — 튜플이 DDL `_dist` 선언 순서와 바이트 단위로 같아야 한다."""
    assert list(GPU_COLS) == _ddl_columns(DDL_FACT, f"fact.{T_GPU}_dist")
    assert list(SERVING_COLS) == _ddl_columns(DDL_FACT, f"fact.{T_SERVING}_dist")
    assert list(SUMMARY_COLS) == _ddl_columns(DDL_FACT, f"fact.{T_SUMMARY}_dist")
    assert list(AUDIT_COLS) == _ddl_columns(DDL_FACT, f"fact.{T_AUDIT}_dist")
    assert list(DIM_COLS) == _ddl_columns(DDL_DIM, f"gpu_data.{T_DIM}_dist")
    assert (len(GPU_COLS), len(SERVING_COLS), len(SUMMARY_COLS), len(AUDIT_COLS), len(DIM_COLS)) == (12, 15, 15, 9, 12)


def test_dim_cols_prefix_is_service_entry_key():
    assert DIM_COLS[-1] == "updated_at"
    assert len(entry().dim_key()) == len(DIM_COLS) - 1 == 11           # diff 비교 키 = updated_at 제외


def test_now_kst_is_aware():
    assert now_kst().tzinfo is not None
    assert now_kst().utcoffset().total_seconds() == 9 * 3600


# ---- 존재확인 3종 · prev summary · _delete_day_in · 가드 예외 ------------------

def test_anchor_exists_and_source_type():
    ch = FakeCH(existing={T_SUMMARY: {SERVICE}}, prev_summary=(PREV_GEN, PREV_COL, "manual-v0"))
    w = writer(ch)
    assert w.anchor_exists(DATE, SERVICE) is True
    assert w.anchor_exists(DATE, "Mock Service B") is False
    sql, params = ch.queries[0]
    assert sql == (f"SELECT count() FROM {DB_FACT}.{T_SUMMARY}_dist "
                   f"WHERE date = %(d)s AND service = %(s)s")
    assert params == {"d": DATE, "s": SERVICE}
    assert w.anchor_source_type(DATE, SERVICE) == "manual-v0"
    sql, params = ch.queries[-1]
    assert sql == (f"SELECT source_type FROM {DB_FACT}.{T_SUMMARY}_dist "
                   f"WHERE date = %(d)s AND service = %(s)s ORDER BY collected_at DESC LIMIT 1")
    assert params == {"d": DATE, "s": SERVICE}
    assert writer(FakeCH()).anchor_source_type(DATE, SERVICE) is None
    assert ch.commands == [] and ch.inserts == []                       # 읽기 전용 — 뮤테이션 0


def test_existing_services_three_tables_in_clause():
    ch = FakeCH(existing={T_SUMMARY: {"A", "C"}, T_SERVING: {"B"}, T_GPU: {"Z"}})
    got = writer(ch).existing_services(DATE, ["C", "A", "B", "A"])
    assert got == {T_SUMMARY: {"A", "C"}, T_GPU: set(), T_SERVING: {"B"}}   # Z는 요청 밖 → 제외
    assert len(ch.queries) == 3
    assert [q[0] for q in ch.queries] == [
        f"SELECT DISTINCT service FROM {DB_FACT}.{t}_dist WHERE date = %(d)s AND service IN %(ss)s"
        for t in (T_SUMMARY, T_GPU, T_SERVING)]
    assert all(q[1] == {"d": DATE, "ss": ["A", "B", "C"]} for q in ch.queries)   # 중복 제거·정렬
    assert writer(FakeCH()).existing_services(DATE, []) == {T_SUMMARY: set(), T_GPU: set(), T_SERVING: set()}


def test_fetch_prev_summary_values_and_none():
    ch = FakeCH(prev_summary=(PREV_GEN, PREV_COL, "manual-v0"), gpu_agg=(5, 120.5), serving_count=7)
    prev = writer(ch).fetch_prev_summary(DATE, SERVICE)
    assert prev == {"prev_generated_at": PREV_GEN, "prev_collected_at": PREV_COL,
                    "prev_source_type": "manual-v0", "prev_gpu_rows": 5,
                    "prev_gpu_hours_sum": 120.5, "prev_serving_rows": 7}
    assert len(ch.queries) == 3 and all(q[1] == {"d": DATE, "s": SERVICE} for q in ch.queries)
    ch2 = FakeCH(prev_summary=(PREV_GEN, PREV_COL, "metrics-api-v1"), gpu_agg=(0, None), serving_count=0)
    prev2 = writer(ch2).fetch_prev_summary(DATE, SERVICE)
    assert prev2["prev_gpu_rows"] == 0 and prev2["prev_gpu_hours_sum"] == 0.0     # NODATA 세대도 감사 대상
    assert prev2["prev_serving_rows"] == 0
    ch3 = FakeCH(prev_summary=None, gpu_agg=(3, 10.0))
    assert writer(ch3).fetch_prev_summary(DATE, SERVICE) is None                 # 앵커 없음 → 감사 없음
    assert len(ch3.queries) == 1                                                 # summary만 조회하고 중단


def test_delete_day_in_sql_settings_and_counter():
    ch = FakeCH()
    w = writer(ch, ch_cluster="gpu-monitoring")
    w._delete_day_in(w._local(T_SUMMARY), DATE, ["C", "A"])
    assert w.mutations_done == 1
    sql, params, settings = ch.commands[0]
    assert sql == (f"ALTER TABLE {DB_FACT}.{T_SUMMARY}_local ON CLUSTER 'gpu-monitoring' "
                   f"DELETE WHERE date = %(d)s AND service IN %(ss)s")
    assert params == {"d": DATE, "ss": ["A", "C"]}
    assert settings == {"mutations_sync": 2}
    w._delete_day_in(w._local(T_GPU), DATE, ["A"])
    assert w.mutations_done == 2


def test_mutation_budget_exceeded_attrs():
    e = MutationBudgetExceeded(3, 6, 8)
    assert (e.planned, e.done, e.limit) == (3, 6, 8)
    assert str(e) == "planned=3 done=6 limit=8"
    assert isinstance(e, Exception)


def test_writer_starts_with_zero_mutations():
    w = writer(FakeCH(), ch_cluster="")
    assert w.mutations_done == 0
    assert w._on_cluster() == ""
    assert w._dist(T_GPU) == f"{DB_FACT}.raw_token_metrics_gpu_1d_dist"
    assert w._local(T_GPU) == f"{DB_FACT}.raw_token_metrics_gpu_1d_local"
    assert writer(FakeCH(), ch_cluster="gpu-monitoring")._on_cluster() == " ON CLUSTER 'gpu-monitoring'"


# ---- DB명 상수 (company-verify 격리 DB) ------------------------------------------

def test_db_names_default():
    """CH_DB_FACT/CH_DB_DIM 미설정 시 기본값 — 기존 배포·E2E 무변경."""
    assert DB_FACT == "fact"
    assert DB_DIM == "gpu_data"


def test_db_names_env_override():
    """모듈 로드 시 1회 결정(CronJob env 주입 전제) — 이미 import된 프로세스에서 os.environ을 바꿔도
    재평가되지 않으므로 자식 프로세스를 띄워 import 시점 반영을 검증한다(기존 모듈 D6.2 관용구)."""
    env = {"PATH": subprocess.os.environ.get("PATH", ""),
           "CH_DB_FACT": "token_verify_fact", "CH_DB_DIM": "token_verify_dim"}
    res = subprocess.run(
        [sys.executable, "-c", "from app.writer import DB_FACT, DB_DIM; print(DB_FACT); print(DB_DIM)"],
        cwd=str(MODULE_ROOT), env=env, capture_output=True, text=True, check=True)
    assert res.stdout.strip().splitlines() == ["token_verify_fact", "token_verify_dim"]
```

- [ ] **Step 3: 실패 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_writer.py`
Expected: `ImportError while importing test module … E   ModuleNotFoundError: No module named 'app.writer'` → `Interrupted: 1 error during collection`

- [ ] **Step 4: 구현 1 — 상수·컬럼 튜플·`now_kst`·`MutationBudgetExceeded`·`MetricsWriter` 읽기 메서드·`_delete_day_in`** — `collectors/token-metrics/app/writer.py` 신규(전체 내용)

```python
"""ClickHouse 멱등 적재 — 설계 2026-08-31 §5.4 적재 시퀀스 · §4.0 뮤테이션 장부 · §4.3 레지스트리 diff-sync.

시퀀스(§5.4, 크래시 안전):
  (1) 존재 SELECT 3종 (summary/gpu/serving `_dist`, `WHERE date = … AND service IN (…)`) — fetch·normalize 이후, DELETE 직전
  (2) 하나라도 있으면: 앵커(summary)가 있는 서비스만 감사 INSERT(append-only)
      → DELETE 순서 고정 summary(앵커) → gpu → serving (`_local`[+ON CLUSTER], mutations_sync=2; 테이블당 1회 = 날짜당 ≤3)
  (3) INSERT 순서 gpu → serving → summary 마지막 (insert_distributed_sync=1, insert_deduplicate=0)
      — 앵커(summary) 존재 = 적재 완료. 자식 행만 남은 잔여물은 다음 실행의 (1)이 잡아 DELETE×3 을 강제한다.
뮤테이션 가드(§4.0): 예정 DELETE 수 + 실행 누적(mutations_done) > METRICS_MAX_MUTATIONS_PER_RUN 이면
  DELETE·INSERT 전에 MutationBudgetExceeded — 호출자(main)가 FAILURE reason=mutation_budget 으로 번역.
DB명은 모듈 상수 2종만 (company-verify 격리 DB는 env CH_DB_FACT/CH_DB_DIM — 모듈 로드 시 1회 결정).
기존 collectors/token-usage/app/clickhouse_client.py 의 관용구(ON CLUSTER·mutations_sync=2·aware KST)를 복제 — import 없음.
"""
from __future__ import annotations

import os
from datetime import date as date_t, datetime, timedelta, timezone

import clickhouse_connect

from app.config import Config, ServiceEntry
from app.normalize import MetricsPayload, NormalizeResult

# company-verify 격리 DB 검증용 — 모듈 로드 시 1회 결정(CronJob env 주입 전제). 미설정 = 공유 DB.
DB_FACT = os.getenv("CH_DB_FACT", "fact")
DB_DIM = os.getenv("CH_DB_DIM", "gpu_data")

KST = timezone(timedelta(hours=9))
CLIENT_SETTINGS = {"insert_distributed_sync": 1, "insert_deduplicate": 0}   # §5.4 (3)
MUTATIONS_SYNC = {"mutations_sync": 2}                                       # §5.4 (2)

T_GPU = "raw_token_metrics_gpu_1d"
T_SERVING = "raw_token_metrics_serving_1d"
T_SUMMARY = "raw_token_metrics_summary_1d"       # 앵커 — 응답당 1행, NODATA 포함
T_AUDIT = "collect_audit_metrics_1d"             # append-only (GRANT도 INSERT만)
T_DIM = "dim_token_metrics_service"              # gpu_data 레지스트리 (정기 실행 diff-sync)
DELETE_ORDER = (T_SUMMARY, T_GPU, T_SERVING)     # 앵커 먼저
INSERT_ORDER = (T_GPU, T_SERVING, T_SUMMARY)     # 앵커 마지막

# 컬럼 튜플 = Plan 6a DDL `_dist` 선언 순서 (tests/test_writer.py 가 DDL을 파싱해 대조). INSERT는 항상 column_names 명시.
GPU_COLS = ("date", "service_group", "service", "model", "gpu_type", "category",
            "gpu_count", "gpu_hours", "flags", "source_type", "generated_at", "collected_at")
SERVING_COLS = ("date", "service_group", "service", "model", "metric", "name", "unit",
                "p50", "p90", "p95", "p99", "flags", "source_type", "generated_at", "collected_at")
SUMMARY_COLS = ("date", "service_group", "service", "reported_service_group", "reported_service",
                "engine_type", "engine_version", "gpu_rows", "serving_rows", "custom_rows",
                "rejected_rows", "merged_dups", "source_type", "generated_at", "collected_at")
AUDIT_COLS = ("date", "service", "prev_generated_at", "prev_collected_at", "prev_source_type",
              "prev_gpu_rows", "prev_gpu_hours_sum", "prev_serving_rows", "replaced_at")
DIM_COLS = ("service_group", "service", "base_url", "enabled", "api_since", "coverage_since", "until",
            "expect_gpu", "expect_serving", "usage_includes_consumers", "note", "updated_at")


def now_kst() -> datetime:
    """CH DateTime('Asia/Seoul') 컬럼용 — aware KST. naive datetime을 드라이버가 int(x.timestamp())로 다루면
    호스트 TZ로 해석되어 KST 벽시계와 어긋난다 — 항상 tzinfo를 유지한 채 넘긴다(기존 모듈 C2 회귀 방지)."""
    return datetime.now(KST)


class MutationBudgetExceeded(Exception):
    """예정 DELETE 수 + 실행 누적이 METRICS_MAX_MUTATIONS_PER_RUN 을 넘음 — 적재 없이 FAILURE reason=mutation_budget."""

    def __init__(self, planned: int, done: int, limit: int):
        super().__init__(f"planned={planned} done={done} limit={limit}")
        self.planned = planned
        self.done = done
        self.limit = limit


def _dim_sort_key(row: tuple) -> tuple[str, ...]:
    """레지스트리 diff 정렬 키 — `until` 이 None/date 로 섞여도 비교 가능하도록 전 원소를 문자열화(동치 판정은 원 튜플로)."""
    return tuple("" if v is None else str(v) for v in row)


class MetricsWriter:
    def __init__(self, cfg: Config, client=None):
        self.cfg = cfg
        self.client = client or clickhouse_connect.get_client(
            host=cfg.ch_host, port=cfg.ch_port, username=cfg.ch_user,
            password=cfg.ch_password, settings=CLIENT_SETTINGS)
        self.mutations_done = 0     # 실행 누적 — fact DELETE·레지스트리 DELETE 마다 +1 (§4.0 가드 합산 기준)

    # ---- 이름 조합 -------------------------------------------------------------------

    def _on_cluster(self) -> str:
        return f" ON CLUSTER '{self.cfg.ch_cluster}'" if self.cfg.ch_cluster else ""

    def _dist(self, name: str) -> str:
        return f"{DB_FACT}.{name}_dist"

    def _local(self, name: str) -> str:
        return f"{DB_FACT}.{name}_local"

    # ---- 읽기 (뮤테이션 0) --------------------------------------------------------------

    def anchor_exists(self, date: str, service: str) -> bool:
        """사전 already_loaded 판정(§5.2 표) — 앵커(summary)만 본다."""
        r = self.client.query(
            f"SELECT count() FROM {self._dist(T_SUMMARY)} WHERE date = %(d)s AND service = %(s)s",
            parameters={"d": date, "s": service})
        return bool(r.result_rows and r.result_rows[0][0])

    def anchor_source_type(self, date: str, service: str) -> str | None:
        """앵커의 source_type — 정기 경로에서 manual-v0 앵커면 CHECK WARN manual_row_present (§5.2 표)."""
        r = self.client.query(
            f"SELECT source_type FROM {self._dist(T_SUMMARY)} "
            f"WHERE date = %(d)s AND service = %(s)s ORDER BY collected_at DESC LIMIT 1",
            parameters={"d": date, "s": service})
        return r.result_rows[0][0] if r.result_rows else None

    def existing_services(self, date: str, services: list[str]) -> dict[str, set[str]]:
        """§5.4 (1) 존재 SELECT 3종 — 테이블별로 '이 날짜에 행이 있는 서비스' 집합.
        clickhouse-connect 는 list 파라미터를 배열 리터럴로 직렬화하고 CH 는 `service IN [...]` 를 허용한다."""
        ss = sorted(set(services))
        if not ss:
            return {T_SUMMARY: set(), T_GPU: set(), T_SERVING: set()}
        out: dict[str, set[str]] = {}
        for t in (T_SUMMARY, T_GPU, T_SERVING):
            r = self.client.query(
                f"SELECT DISTINCT service FROM {self._dist(t)} "
                f"WHERE date = %(d)s AND service IN %(ss)s",
                parameters={"d": date, "ss": ss})
            out[t] = {row[0] for row in r.result_rows}
        return out

    def fetch_prev_summary(self, date: str, service: str) -> dict | None:
        """교체 전 세대 요약 — 감사(append-only)용. 앵커(summary)가 없으면 None(NODATA 세대는 앵커가 있으므로 감사 대상)."""
        s = self.client.query(
            f"SELECT generated_at, collected_at, source_type FROM {self._dist(T_SUMMARY)} "
            f"WHERE date = %(d)s AND service = %(s)s ORDER BY collected_at DESC LIMIT 1",
            parameters={"d": date, "s": service})
        if not s.result_rows:
            return None
        gen, col, stype = s.result_rows[0]
        g = self.client.query(
            f"SELECT count(), sum(gpu_hours) FROM {self._dist(T_GPU)} "
            f"WHERE date = %(d)s AND service = %(s)s",
            parameters={"d": date, "s": service})
        gpu_rows, gpu_hours = (g.result_rows[0] if g.result_rows else (0, None))
        v = self.client.query(
            f"SELECT count() FROM {self._dist(T_SERVING)} WHERE date = %(d)s AND service = %(s)s",
            parameters={"d": date, "s": service})
        serving_rows = v.result_rows[0][0] if v.result_rows else 0
        return {"prev_generated_at": gen, "prev_collected_at": col, "prev_source_type": stype,
                "prev_gpu_rows": int(gpu_rows or 0), "prev_gpu_hours_sum": float(gpu_hours or 0.0),
                "prev_serving_rows": int(serving_rows or 0)}

    # ---- 뮤테이션 (각 호출 = 1 뮤테이션) -----------------------------------------------

    def _delete_day_in(self, table_local: str, date: str, services: list[str]) -> None:
        """§5.4 배칭 (B) — 테이블당 1회 `service IN (...)` DELETE (`_local`[+ON CLUSTER], mutations_sync=2)."""
        self.client.command(
            f"ALTER TABLE {table_local}{self._on_cluster()} "
            f"DELETE WHERE date = %(d)s AND service IN %(ss)s",
            parameters={"d": date, "ss": sorted(services)},
            settings=MUTATIONS_SYNC)
        self.mutations_done += 1
```

- [ ] **Step 5: 통과 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_writer.py`
Expected: `13 passed`

- [ ] **Step 6: 실패하는 테스트 2 — `insert_service_day`·`replace_batch`(첫 적재·INSERT 순서·감사→DELETE 순서→INSERT·잔여물 3회 강제·IN 배칭·가드·누적·행 모양)** — `collectors/token-metrics/tests/test_writer.py` **끝에 추가**

```python


# ---- 적재 시퀀스 (§5.4) ------------------------------------------------------------

def _tables(ch: FakeCH, kind: str) -> list[str]:
    return [t for k, t in ch.events if k == kind]


def test_first_load_no_delete_no_audit():
    ch = FakeCH()                                              # 존재 3종 전부 빈 집합
    got = writer(ch).replace_batch(DATE, [(entry(), payload(), result())])
    assert got == {SERVICE: 5}                                 # gpu 2 + serving 3
    assert ch.commands == []                                   # 뮤테이션 0 (§4.0 정기 = INSERT만)
    assert [t.rsplit(".", 1)[1] for t, _, _ in ch.inserts] == [f"{T_GPU}_dist", f"{T_SERVING}_dist", f"{T_SUMMARY}_dist"]
    assert not any(t.endswith(f"{T_AUDIT}_dist") for t, _, _ in ch.inserts)
    assert [n for _, n, _ in ch.inserts] == [2, 3, 1]
    assert [c for _, _, c in ch.inserts] == [GPU_COLS, SERVING_COLS, SUMMARY_COLS]   # 컬럼 목록 명시
    assert len(ch.queries) == 3                                # 존재 SELECT 3종만


def test_insert_order_summary_last():
    ch = FakeCH()
    got = writer(ch).replace_batch(DATE, [(entry(), payload(), result(n_gpu=0, n_serving=1))])
    assert got == {SERVICE: 1}
    tables = [t for t, _, _ in ch.inserts]
    assert tables[-1].endswith(f"{T_SUMMARY}_dist")            # 앵커 마지막
    assert not any(t.endswith(f"{T_GPU}_dist") for t in tables)   # 0행 gpu INSERT 생략
    assert [t.rsplit(".", 1)[1] for t in tables] == [f"{T_SERVING}_dist", f"{T_SUMMARY}_dist"]
    ch2 = FakeCH()
    assert writer(ch2).replace_batch(DATE, [(entry(), payload(), result(n_gpu=0, n_serving=0))]) == {SERVICE: 0}
    assert [t.rsplit(".", 1)[1] for t, _, _ in ch2.inserts] == [f"{T_SUMMARY}_dist"]   # NODATA 앵커 1행


def test_reload_audit_then_delete_order_then_insert():
    ch = FakeCH(existing={T_SUMMARY: {SERVICE}, T_GPU: {SERVICE}, T_SERVING: {SERVICE}},
                prev_summary=(PREV_GEN, PREV_COL, "manual-v0"), gpu_agg=(5, 120.5), serving_count=7)
    w = writer(ch, ch_cluster="gpu-monitoring")
    got = w.replace_batch(DATE, [(entry(), payload(), result())])
    assert got == {SERVICE: 5}
    kinds = [k for k, _ in ch.events]
    assert kinds == ["insert", "command", "command", "command", "insert", "insert", "insert"]
    assert ch.events[0][1].endswith(f"{T_AUDIT}_dist")                     # 감사 먼저
    deletes = [c for c in ch.commands if "DELETE" in c[0]]
    assert len(deletes) == 3
    assert [c[0] for c in deletes] == [
        f"ALTER TABLE {DB_FACT}.{t}_local ON CLUSTER 'gpu-monitoring' "
        f"DELETE WHERE date = %(d)s AND service IN %(ss)s" for t in (T_SUMMARY, T_GPU, T_SERVING)]
    assert all(c[1] == {"d": DATE, "ss": [SERVICE]} for c in deletes)
    assert all(c[2] == {"mutations_sync": 2} for c in deletes)
    assert [t.rsplit(".", 1)[1] for t in _tables(ch, "insert")[1:]] == [f"{T_GPU}_dist", f"{T_SERVING}_dist", f"{T_SUMMARY}_dist"]
    audit_table, audit_rows = ch.insert_rows[0]
    assert audit_table == f"{DB_FACT}.{T_AUDIT}_dist" and ch.inserts[0][2] == AUDIT_COLS
    assert len(audit_rows) == 1 and len(audit_rows[0]) == 9
    assert audit_rows[0][:8] == [DATE_V, SERVICE, PREV_GEN, PREV_COL, "manual-v0", 5, 120.5, 7]
    assert audit_rows[0][8].tzinfo is not None                              # replaced_at aware KST
    assert w.mutations_done == 3


def test_children_only_forces_three_deletes_without_audit():
    """앵커 없이 gpu 행만 남은 부분 적재 잔여물(§5.2 표) — 확장 존재확인이 DELETE×3 강제, 감사는 없음."""
    ch = FakeCH(existing={T_GPU: {SERVICE}})
    w = writer(ch)
    w.replace_batch(DATE, [(entry(), payload(), result())])
    assert len([c for c in ch.commands if "DELETE" in c[0]]) == 3
    assert not any(t.endswith(f"{T_AUDIT}_dist") for t, _, _ in ch.inserts)
    assert len(ch.queries) == 3                                # prev summary 조회 없음(앵커 집합이 빔)
    assert w.mutations_done == 3


def test_no_on_cluster_when_cluster_empty():
    ch = FakeCH(existing={T_SUMMARY: {SERVICE}}, prev_summary=(PREV_GEN, PREV_COL, "metrics-api-v1"))
    writer(ch, ch_cluster="").replace_batch(DATE, [(entry(), payload(), result())])
    deletes = [c for c in ch.commands if "DELETE" in c[0]]
    assert len(deletes) == 3 and all("ON CLUSTER" not in c[0] for c in deletes)
    assert all("_local" in c[0] for c in deletes)


def test_batch_in_clause_and_single_delete_set():
    """--replace 배칭(§5.4 B): 서비스 3개, 테이블마다 다른 존재 집합 → 합집합 1개로 DELETE 3회(6·9 아님)."""
    ch = FakeCH(existing={T_SUMMARY: {"A", "C"}, T_SERVING: {"B"}},
                prev_summary=(PREV_GEN, PREV_COL, "metrics-api-v1"), gpu_agg=(1, 24.0), serving_count=1)
    items = [(entry(s), payload(s), result()) for s in ("C", "A", "B")]
    got = writer(ch).replace_batch(DATE, items)
    assert got == {"C": 5, "A": 5, "B": 5}
    deletes = [c for c in ch.commands if "DELETE" in c[0]]
    assert len(deletes) == 3
    assert all(c[1] == {"d": DATE, "ss": ["A", "B", "C"]} for c in deletes)
    audits = [(t, n) for t, n, _ in ch.inserts if t.endswith(f"{T_AUDIT}_dist")]
    assert audits == [(f"{DB_FACT}.{T_AUDIT}_dist", 1)] * 2                # 앵커 있는 A·C 만 감사
    assert [r[0][1] for t, r in ch.insert_rows if t.endswith(f"{T_AUDIT}_dist")] == ["A", "C"]   # 정렬 순
    fact = [t.rsplit(".", 1)[1] for t, _, _ in ch.inserts if not t.endswith(f"{T_AUDIT}_dist")]
    assert fact == [f"{T_GPU}_dist", f"{T_SERVING}_dist", f"{T_SUMMARY}_dist"] * 3   # 서비스별 gpu→serving→summary
    assert [r[0][2] for t, r in ch.insert_rows if t.endswith(f"{T_SUMMARY}_dist")] == ["C", "A", "B"]   # items 순서
    # 감사 → DELETE 3 → INSERT 순서 (전 서비스 DELETE 뒤에야 첫 INSERT)
    kinds = [k for k, _ in ch.events]
    assert kinds[:5] == ["insert", "insert", "command", "command", "command"] and set(kinds[5:]) == {"insert"}


def test_mutation_budget_guard_before_any_write():
    ch = FakeCH(existing={T_GPU: {SERVICE}}, prev_summary=(PREV_GEN, PREV_COL, "metrics-api-v1"))
    w = writer(ch, max_mutations_per_run=2)
    with pytest.raises(MutationBudgetExceeded) as ei:
        w.replace_batch(DATE, [(entry(), payload(), result())])
    assert (ei.value.planned, ei.value.done, ei.value.limit) == (3, 0, 2)
    assert ch.commands == [] and ch.inserts == []              # DELETE·INSERT·감사 전부 없음
    assert len(ch.queries) == 3                                # 존재 선조회는 수행됨(가드 합산 근거)
    assert w.mutations_done == 0


def test_budget_zero_allows_first_load():
    """정기 경로(존재 0) 는 예정 DELETE 0 → 예산 0 이어도 통과 (뮤테이션 장부 '정기 0')."""
    ch = FakeCH()
    assert writer(ch, max_mutations_per_run=0).replace_batch(DATE, [(entry(), payload(), result())]) == {SERVICE: 5}
    assert ch.commands == []


def test_mutations_done_accumulates_across_batches():
    ch = FakeCH(existing={T_SUMMARY: {SERVICE}}, prev_summary=(PREV_GEN, PREV_COL, "metrics-api-v1"))
    w = writer(ch, max_mutations_per_run=8)
    w.replace_batch(DATE, [(entry(), payload(), result())])
    assert w.mutations_done == 3
    w.replace_batch("2026-09-11", [(entry(), payload(), result())])
    assert w.mutations_done == 6
    with pytest.raises(MutationBudgetExceeded) as ei:
        w.replace_batch("2026-09-12", [(entry(), payload(), result())])
    assert (ei.value.planned, ei.value.done, ei.value.limit) == (3, 6, 8)
    assert w.mutations_done == 6                               # 예외 후 누적 불변
    assert len([c for c in ch.commands if "DELETE" in c[0]]) == 6


def test_row_shapes():
    ch = FakeCH()
    w = writer(ch)
    collected = datetime(2026, 9, 11, 2, 7, tzinfo=KST)
    n = w.insert_service_day(entry(), DATE, payload(), result(n_gpu=2, n_serving=3, n_custom=1), collected)
    assert n == 6
    rows = {t.rsplit(".", 1)[1]: data for t, data in ch.insert_rows}
    gpu = rows[f"{T_GPU}_dist"]
    assert len(gpu) == 2 and all(len(r) == len(GPU_COLS) == 12 for r in gpu)
    assert type(gpu[0][0]) is date_t and gpu[0][0] == DATE_V            # 드라이버 Date 직렬화 요건 (str 불가)
    assert gpu[0][1:6] == [SERVICE_GROUP, SERVICE, "m0", "H100", "serving"]
    assert gpu[0][6:8] == [2.0, 48.0]
    assert isinstance(gpu[0][8], list) and gpu[0][8] == []               # flags Array(String)
    assert gpu[0][9] == "metrics-api-v1"
    assert gpu[0][10] == GENERATED_AT and gpu[0][10].tzinfo is not None
    assert gpu[0][11] is collected
    serving = rows[f"{T_SERVING}_dist"]
    assert len(serving) == 4 and all(len(r) == len(SERVING_COLS) == 15 for r in serving)
    assert serving[0][3:7] == ["m0", "ttft_ms", "", "ms"] and serving[0][7:11] == [280.0, 560.0, 720.0, 1200.0]
    assert serving[2][4:7] == ["output_tps", "", "tokens/s"] and serving[2][7] == 41.0
    assert serving[2][8] is None and serving[2][9] is None and serving[2][10] is None   # p90..p99 None 유지
    assert serving[3][4:7] == ["custom", "c0", "ms"]
    assert all(r[12] == "metrics-api-v1" and r[13] == GENERATED_AT and r[14] is collected for r in serving)
    summary = rows[f"{T_SUMMARY}_dist"]
    assert len(summary) == 1 and len(summary[0]) == len(SUMMARY_COLS) == 15
    assert summary[0][:7] == [DATE_V, SERVICE_GROUP, SERVICE, SERVICE_GROUP, SERVICE, "vllm", "0.10.1"]
    assert summary[0][7:12] == [2, 3, 1, 0, 0]                           # gpu_rows serving_rows custom_rows rejected merged_dups
    assert summary[0][12:] == ["metrics-api-v1", GENERATED_AT, collected]


def test_summary_carries_reported_identity_and_rejected_counts():
    """summary 의 reported_* 는 payload 원문(identity drift 추적), rejected/merged_dups 는 result 값."""
    ch = FakeCH()
    p = payload()
    p.reported_service_group, p.reported_service = "Mock Group ", "mock service a"
    r = result(n_gpu=1, n_serving=1)
    r.rejected, r.merged_dups = 4, 2
    writer(ch).insert_service_day(entry(), DATE, p, r, now_kst())
    summary = [d for t, d in ch.insert_rows if t.endswith(f"{T_SUMMARY}_dist")][0][0]
    assert summary[3:5] == ["Mock Group ", "mock service a"]
    assert summary[7:12] == [1, 1, 0, 4, 2]


def test_replace_batch_empty_items_is_noop():
    ch = FakeCH(existing={T_SUMMARY: {SERVICE}})
    assert writer(ch).replace_batch(DATE, []) == {}
    assert ch.queries == [] and ch.commands == [] and ch.inserts == []
```

- [ ] **Step 7: 실패 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_writer.py`
Expected: 13 passed, 12 failed — 실패는 전부 `AttributeError: 'MetricsWriter' object has no attribute 'replace_batch'`(또는 `insert_service_day`).

- [ ] **Step 8: 구현 2 — `insert_service_day`·`replace_batch`** — `collectors/token-metrics/app/writer.py` 클래스 `MetricsWriter` **끝에 추가**(마지막 메서드 `_delete_day_in`의 `self.mutations_done += 1` 뒤, 빈 줄 1개 포함 — 4칸 들여쓰기 메서드)

```python

    # ---- 적재 (§5.4 (3) INSERT 순서 gpu → serving → summary 마지막) ----------------------

    def insert_service_day(self, entry: ServiceEntry, date: str, payload: MetricsPayload,
                           result: NormalizeResult, collected_at: datetime) -> int:
        """서비스 1개·날짜 1개 INSERT — 0행 자식 테이블은 INSERT 생략, summary(앵커)는 항상 1행(NODATA 포함).
        manual-v0 는 호출자(T7)가 payload.reported_* 에 레지스트리 값을 넣어 온다. 반환 = result.rows."""
        date_v = date_t.fromisoformat(date)   # 네이티브 INSERT 의 Date 직렬화는 date 객체 필요
        stype = payload.source_type
        gen = result.generated_at
        gpu_rows = [[date_v, entry.service_group, entry.service, r.model, r.gpu_type, r.category,
                     r.gpu_count, r.gpu_hours, list(r.flags), stype, gen, collected_at]
                    for r in result.gpu_rows]
        if gpu_rows:
            self.client.insert(self._dist(T_GPU), gpu_rows, column_names=GPU_COLS)
        serving_rows = [[date_v, entry.service_group, entry.service, r.model, r.metric, r.name, r.unit,
                         r.p50, r.p90, r.p95, r.p99, list(r.flags), stype, gen, collected_at]
                        for r in result.serving_rows]
        if serving_rows:
            self.client.insert(self._dist(T_SERVING), serving_rows, column_names=SERVING_COLS)
        self.client.insert(
            self._dist(T_SUMMARY),
            [[date_v, entry.service_group, entry.service,
              payload.reported_service_group, payload.reported_service,
              result.engine_type, result.engine_version,
              result.n_gpu, result.n_serving, result.n_custom, result.rejected, result.merged_dups,
              stype, gen, collected_at]],
            column_names=SUMMARY_COLS)
        return result.rows

    def replace_batch(self, date: str,
                      items: list[tuple[ServiceEntry, MetricsPayload, NormalizeResult]]) -> dict[str, int]:
        """§5.4 (1)~(3) + 배칭 (B): 날짜 1개·서비스 N개.
        (1) 존재 SELECT 3종 → affected = 합집합 → (2) 가드 → 앵커 있는 서비스 감사 INSERT → DELETE summary→gpu→serving
        (테이블당 1회, 서비스 IN 배칭 = 날짜당 ≤3 뮤테이션) → (3) 서비스별 INSERT gpu→serving→summary.
        정기 경로는 item 1개로 호출(서비스별 순차), --replace/manual 은 날짜당 1회 호출. 반환 {service: rows}."""
        if not items:
            return {}
        existing = self.existing_services(date, [e.service for e, _, _ in items])
        affected = sorted(set().union(*existing.values()))
        planned = 3 if affected else 0
        limit = self.cfg.max_mutations_per_run
        if self.mutations_done + planned > limit:               # §4.0 가드 — DELETE·INSERT·감사 전
            raise MutationBudgetExceeded(planned, self.mutations_done, limit)
        date_v = date_t.fromisoformat(date)
        for svc in sorted(existing[T_SUMMARY]):                 # 앵커가 있는 서비스만 감사(append-only)
            prev = self.fetch_prev_summary(date, svc)
            if prev:
                self.client.insert(
                    self._dist(T_AUDIT),
                    [[date_v, svc, prev["prev_generated_at"], prev["prev_collected_at"],
                      prev["prev_source_type"], prev["prev_gpu_rows"], prev["prev_gpu_hours_sum"],
                      prev["prev_serving_rows"], now_kst()]],
                    column_names=AUDIT_COLS)
        if affected:                                            # 자식 행만 남은 잔여물도 3회 강제(§5.2 표)
            for t in DELETE_ORDER:
                self._delete_day_in(self._local(t), date, affected)
        collected_at = now_kst()                                # 배치 1회 — 같은 실행의 서비스는 같은 적재 시각
        out: dict[str, int] = {}
        for entry, payload, result in items:
            out[entry.service] = self.insert_service_day(entry, date, payload, result, collected_at)
        return out
```

- [ ] **Step 9: 통과 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_writer.py`
Expected: `25 passed`

- [ ] **Step 10: 실패하는 테스트 3 — `sync_registry`(diff 동일 → no-op / 다름 → DELETE WHERE 1 + INSERT / 현재 집합 비면 DELETE 생략 / 순서·None until 안전 / 뮤테이션 누적)** — `collectors/token-metrics/tests/test_writer.py` **끝에 추가**

```python


# ---- 레지스트리 diff-sync (§4.3) ----------------------------------------------------

ENTRIES = [
    entry("Mock Service A"),
    entry("Mock Service B", base_url="http://mock-b/", expect_gpu=False, until=date_t(2026, 12, 31),
          note="b"),
    entry("Mock Service C", enabled=False, usage_includes_consumers=True),
]
DIM_SELECT = (f"SELECT service_group, service, base_url, enabled, api_since, coverage_since, until, "
              f"expect_gpu, expect_serving, usage_includes_consumers, note FROM {DB_DIM}.{T_DIM}_dist")


def test_sync_registry_noop_when_equal():
    ch = FakeCH(dim_rows=[e.dim_key() for e in ENTRIES])      # 현재 행 = 원하는 집합 (updated_at 은 비교 밖)
    w = writer(ch, ch_cluster="gpu-monitoring")
    assert w.sync_registry(ENTRIES) is False
    assert ch.commands == [] and ch.inserts == []              # 쿼리 1회, 뮤테이션 0 (§4.0 장부 '정기 0')
    assert len(ch.queries) == 1 and ch.queries[0] == (DIM_SELECT, None)
    assert "updated_at" not in ch.queries[0][0]
    assert w.mutations_done == 0


def test_sync_registry_order_independent_and_none_until_safe():
    """현재 행 순서가 달라도 같은 집합이면 no-op; until 이 None/date 로 섞인 정렬이 TypeError 없이 동작."""
    rows = [ENTRIES[2].dim_key(), ENTRIES[0].dim_key(), ENTRIES[1].dim_key()]
    assert writer(FakeCH(dim_rows=rows)).sync_registry(ENTRIES) is False
    twins = [entry("Same", until=None), entry("Same", until=date_t(2026, 12, 31))]   # service 동일·until 만 다름
    ch = FakeCH(dim_rows=[twins[1].dim_key(), twins[0].dim_key()])
    assert writer(ch).sync_registry(twins) is False


def test_sync_registry_replaces_when_diff():
    changed = [ENTRIES[0], entry("Mock Service B", base_url="http://mock-b/", expect_gpu=True), ENTRIES[2]]
    ch = FakeCH(dim_rows=[e.dim_key() for e in changed])      # B 의 expect_gpu 가 DB 와 다름
    w = writer(ch, ch_cluster="gpu-monitoring")
    assert w.sync_registry(ENTRIES) is True
    assert len(ch.commands) == 1
    sql, params, settings = ch.commands[0]
    assert sql == f"ALTER TABLE {DB_DIM}.{T_DIM}_local ON CLUSTER 'gpu-monitoring' DELETE WHERE 1"
    assert params is None and settings == {"mutations_sync": 2}
    assert ch.inserts == [(f"{DB_DIM}.{T_DIM}_dist", len(ENTRIES), DIM_COLS)]
    assert [k for k, _ in ch.events] == ["command", "insert"]            # DELETE 뒤 INSERT
    data = ch.insert_rows[0][1]
    assert all(len(r) == 12 for r in data)
    assert [r[1] for r in data] == ["Mock Service A", "Mock Service B", "Mock Service C"]
    assert data[1][:11] == list(ENTRIES[1].dim_key()) and data[1][2] == "http://mock-b/"
    assert data[1][6] == date_t(2026, 12, 31) and data[0][6] is None      # until: Nullable(Date)
    assert data[2][3] == 0 and data[2][9] == 1                            # enabled / usage_includes_consumers UInt8
    assert len({r[11] for r in data}) == 1 and data[0][11].tzinfo is not None   # updated_at 동일·aware
    assert w.mutations_done == 1                                          # 레지스트리 DELETE 도 장부 합산


def test_sync_registry_skips_delete_when_current_empty():
    """최초 배포(현재 집합 빔) — DELETE 생략 → 뮤테이션 0 (§4.0 장부)."""
    ch = FakeCH(dim_rows=[])
    w = writer(ch, ch_cluster="gpu-monitoring")
    assert w.sync_registry(ENTRIES) is True
    assert ch.commands == []
    assert ch.inserts == [(f"{DB_DIM}.{T_DIM}_dist", 3, DIM_COLS)]
    assert w.mutations_done == 0


def test_sync_registry_removed_service_triggers_replace():
    ch = FakeCH(dim_rows=[e.dim_key() for e in ENTRIES])
    w = writer(ch)
    assert w.sync_registry(ENTRIES[:2]) is True                          # endpoints 에서 C 제거
    assert len(ch.commands) == 1 and "ON CLUSTER" not in ch.commands[0][0]
    assert ch.inserts[0][1] == 2
    assert w.mutations_done == 1
```

- [ ] **Step 11: 실패 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_writer.py`
Expected: 25 passed, 5 failed — 실패는 전부 `AttributeError: 'MetricsWriter' object has no attribute 'sync_registry'`.

- [ ] **Step 12: 구현 3 — `sync_registry`** — `collectors/token-metrics/app/writer.py` 클래스 `MetricsWriter` **끝에 추가**(`replace_batch`의 `return out` 뒤, 빈 줄 1개 포함)

```python

    # ---- 레지스트리 diff-sync (§4.3 — 정기 실행에서만 호출; rerun·manual 은 호출하지 않는다) --------

    def sync_registry(self, entries: list[ServiceEntry]) -> bool:
        """endpoints 집합(원하는 상태) vs gpu_data.dim_token_metrics_service 현재 행 — updated_at 제외 11컬럼 비교.
        같으면 False(SELECT 1회, 뮤테이션 0). 다르면 현재 집합이 비어있지 않을 때만 ALTER DELETE(전체, 1 뮤테이션)
        → 전 행 INSERT(컬럼 명시) → True. 실패는 호출자가 CHECK WARN registry_sync_failed 로 다룬다."""
        desired = sorted((e.dim_key() for e in entries), key=_dim_sort_key)
        cols = ", ".join(DIM_COLS[:-1])                          # updated_at 제외 — 비교 키만 읽는다
        r = self.client.query(f"SELECT {cols} FROM {DB_DIM}.{T_DIM}_dist")
        current = sorted((tuple(row) for row in r.result_rows), key=_dim_sort_key)
        if desired == current:
            return False
        if current:                                              # 최초 배포(빈 테이블)는 DELETE 생략 → 뮤테이션 0
            self.client.command(
                f"ALTER TABLE {DB_DIM}.{T_DIM}_local{self._on_cluster()} DELETE WHERE 1",
                settings=MUTATIONS_SYNC)
            self.mutations_done += 1
        now = now_kst()
        self.client.insert(f"{DB_DIM}.{T_DIM}_dist",
                           [e.dim_row(now) for e in entries], column_names=DIM_COLS)
        return True
```

- [ ] **Step 13: 통과 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_writer.py`
Expected: `30 passed`

- [ ] **Step 14: 경계 확인 — 뮤테이션 대상 테이블·import 경계·전체 회귀·zero-diff**

Run(ALTER DELETE 는 fact 3테이블 `_local` + 레지스트리 `_local` 뿐이고 감사 테이블은 INSERT만 — D2.3 GRANT 범위와 일치):
```bash
cd collectors/token-metrics && grep -c "ALTER TABLE" app/writer.py && grep -n "T_AUDIT" app/writer.py | grep -v "^[0-9]*:T_AUDIT = \|DELETE_ORDER\|INSERT_ORDER\|self._dist(T_AUDIT)" ; echo "audit-delete-free"
```
Expected: `2`(`_delete_day_in`·`sync_registry` 각 1) 다음 `audit-delete-free`만 — `T_AUDIT`가 `_local`/ALTER 문맥에 등장하지 않는다.

Run: `cd collectors/token-metrics && grep -n "^import\|^from" app/writer.py`
Expected: 정확히 6줄 — `from __future__ import annotations`, `import os`, `from datetime import date as date_t, datetime, timedelta, timezone`, `import clickhouse_connect`, `from app.config import Config, ServiceEntry`, `from app.normalize import MetricsPayload, NormalizeResult`(기존 모듈 `collectors/token-usage`에서의 import 0).

Run(가드는 DELETE 전 — 예외 시 커맨드 0, `insert_distributed_sync`/`insert_deduplicate` 는 클라이언트 세션 설정):
```bash
cd collectors/token-metrics && python3 - <<'PY'
import sys
sys.path.insert(0, ".")
import app.writer as w
assert w.CLIENT_SETTINGS == {"insert_distributed_sync": 1, "insert_deduplicate": 0}
assert w.DELETE_ORDER == (w.T_SUMMARY, w.T_GPU, w.T_SERVING) and w.INSERT_ORDER == (w.T_GPU, w.T_SERVING, w.T_SUMMARY)
assert (len(w.GPU_COLS), len(w.SERVING_COLS), len(w.SUMMARY_COLS), len(w.AUDIT_COLS), len(w.DIM_COLS)) == (12, 15, 15, 9, 12)
assert w.DB_FACT == "fact" and w.DB_DIM == "gpu_data"
print("writer constants ok")
PY
```
Expected: `writer constants ok`

Run: `cd collectors/token-metrics && python3 -m pytest -q`
Expected: T2~T4 통과 수 + 30 → `<T4 종료 시점 passed 수> + 30 passed`(outline 기준 T2 14 + T3 38 + T4 커밋의 테스트 수 + 30; 실패·에러 0)

Run: `git status --short collectors/token-metrics && git diff --stat -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml`
Expected: `?? collectors/token-metrics/app/writer.py`·`?? collectors/token-metrics/tests/test_writer.py` 2줄만, diff --stat 출력 없음(zero-diff 유지).

- [ ] **Step 15: Commit**

```bash
git add collectors/token-metrics/app/writer.py collectors/token-metrics/tests/test_writer.py
git commit -m "feat(collectors-metrics): writer — 존재확인 3종·감사·DELETE/INSERT 순서·IN 배칭·뮤테이션 가드·레지스트리 diff-sync (Plan 6b T5)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

**설계 해석(이 태스크에서 하나로 정한 항목 — footer Self-Review에 옮겨 적는다):**
- 감사 INSERT는 **앵커(summary)가 있는 서비스마다 1회**(`sorted(existing[T_SUMMARY])` 순, 서비스당 1행 INSERT) — outline 문구 그대로. 감사는 뮤테이션이 아니므로 장부에 합산하지 않는다.
- `existing_services`·`_delete_day_in`의 `IN %(ss)s` 파라미터는 **정렬·중복 제거한 list**(clickhouse-connect가 배열 리터럴로 직렬화, CH는 `service IN [...]` 허용) — 테스트가 `["A", "B", "C"]` 정확 일치를 고정한다. 빈 서비스 목록은 쿼리 없이 빈 집합 3개.
- 가드 `planned`는 **affected 비어있지 않으면 3 고정**(테이블별 존재 여부와 무관 — 잔여물 3회 강제와 일관), 예외 시 `mutations_done` 불변·감사 INSERT도 없음(가드가 감사보다 앞).
- `fetch_prev_summary(date, service)` 인자 순서는 outline(=신규 모듈 규칙 date 먼저)을 따른다 — 기존 모듈 `(service, date)`와 다름(클론이지 공용 아님). `prev_gpu_rows`·`prev_serving_rows`는 `int()`, `prev_gpu_hours_sum`은 `float()`(빈 집계 None → 0.0).
- `sync_registry` 비교는 `updated_at` 제외 11컬럼을 **SELECT에 명시**(`SELECT *` 금지 — 컬럼 추가 시 비교 키가 조용히 바뀌지 않도록) 하고, 정렬 키는 전 원소 문자열화(`_dim_sort_key`) — `until` None/date 혼합 정렬 안전, 동치 판정은 원 튜플. 서비스 제거(endpoints에서 삭제)도 diff → 전체 교체.
- 레지스트리 DELETE는 `mutations_done`에 **합산**(§4.0 "실행당 가드 … 합산"). 정기 실행에서 sync(≤1) 뒤 서비스별 `replace_batch`(존재 0 → planned 0)이므로 실무상 영향 없음.
- `replace_batch`의 `collected_at`은 **배치당 1회**(`now_kst()`), 감사 `replaced_at`은 감사 행마다 `now_kst()` — 같은 실행의 서비스가 같은 적재 시각을 갖는다(T11 E2E 기대치 산정 단순화).
- `MUTATIONS_SYNC` 상수를 outline 목록에 추가로 노출(`{"mutations_sync": 2}` — 테스트가 settings 객체 동치로 고정).

---

### Task 6: app/main.py — 모드×게이트 매트릭스 · 409 큐 끝 재방문 1회 · 최종 슬롯 not_ready_at_0900 · 소프트 데드라인·LOAD_BUDGET · SERVICE_RESULT/BATCH_RESULT/CHECK WARN 마커 · SIGTERM

**Files:**
- Create: `collectors/token-metrics/app/main.py`
- Test: `collectors/token-metrics/tests/test_main.py`
- Modify: 없음 (`collectors/token-usage/app/main.py`·`tests/test_main.py`는 골격 원본 — import·복사 없음, zero-diff. T7이 이 파일에 manual 분기(argparse 4개 + `_run_manual`)를 추가한다)

**설계 근거:** §5.2(239-258) — env(`SOFT_DEADLINE_MINUTES=40`·`LOAD_BUDGET_S=1200`·`FINAL_HOUR_KST=9`), "실행 모드 × 게이트": `api_since`/`until` 게이트와 최종 슬롯 판정(프로세스 batch_time KST hour ≥ `FINAL_HOUR_KST`)은 **정기 실행(target_date = KST 오늘−1)에만**, `--from/--to`·manual 은 무시(rerun 409 = `FAILURE reason=not_ready`), `enabled=0`은 모든 모드 `SKIPPED reason=disabled`, endpoints 에 없는 `--service`는 exit 2 · 표: 앵커 존재 & (정기 또는 without `--replace`) → 스킵 0 뮤테이션(`SKIPPED reason=already_loaded`, manual-v0 앵커면 정기 경로에서 `CHECK WARN manual_row_present`) / 200 → `SUCCESS`(rows>0) · `NODATA`(rows==0 AND rejected==0) · rows==0 AND rejected>0 → `SUCCESS rows=0 rejected=<n>` + `CHECK WARN all_rows_rejected` / 409 → 큐 끝 1회 재방문(`min(Retry-After, 300)s`), 재차 409: 비최종 `SKIPPED reason=not_ready`, 최종 `FAILURE reason=not_ready_at_0900` → exit 1 = 스펙 09:00 알림 / 404 RETENTION 정기 FAILURE·rerun SKIPPED / 그 외 `FAILURE` · 마커 어휘 정확(`module=token-metrics`, `pages=1`, `slot=HH final=0|1`, SIGTERM 캐시 줄 재출력, `CHECK WARN service=<svc> <code>=<count>`) · §5.4(268-274) — "적재 착수 전 `deadline − now < LOAD_BUDGET_S`면 미착수 FAILURE. 가드는 (2) 직전 — 모든 모드 공통", 배칭 (A) 전 서비스 fetch/normalize·가드 → (B)(C)는 writer 1회 호출(`replace_batch`), 정기 경로는 서비스별 순차 · §4.3(227) 레지스트리 동기화는 정기 실행에서만 · §3 전제 7(적재 예산은 소프트 데드라인 안에 예약 — 클론 원본 `main.py:110/158/177`)·11(잡당 BATCH_RESULT 1줄, 로그 페이로드 금지) · §7.5 "final=1 부재 = FAILURE"(최종 슬롯 줄은 반드시 `final=1`) · digest D5.5(큐 루프·`_sigterm_handler`·`_session` 관용구)·D6.4(Clock/FakeWriter/run 헬퍼 테스트 스타일).

**Interfaces:**
- Consumes:
  - T2 `app.config.Config`(`soft_deadline_minutes`, `load_budget_s`, `final_hour_kst`, `endpoints_file`, `https_proxy`, `api_verify`), `ServiceEntry`(`service_group, service, base_url, enabled, api_since: date, coverage_since, until: date | None, …`), `load_config() -> Config`, `load_endpoints(path) -> list[ServiceEntry]`(오류는 `ValueError`/`OSError`/yaml 예외).
  - T2 `app.events.Event`(값 소문자 `not_ready | retryable | permanent_error | retention | empty | invariant_broken`), `CollectError(event, message="", retry_after_s=0)`.
  - T3 `app.normalize.MetricsPayload`(`source_type`), `NormalizeResult`(`rows`, `rejected`, `warn_total`, `warns: dict[str, int]`, `is_nodata`), `PayloadError`, `normalize_payload(payload, entry, now=None) -> NormalizeResult`, `SOURCE_API = "metrics-api-v1"`, `SOURCE_MANUAL = "manual-v0"`, `check_report_structure(body, date)`(테스트 fixture 조립용).
  - T4 `app.api_client.fetch_metrics(entry, date, cfg, session) -> MetricsPayload`(기본 fetcher; `CollectError` 던짐).
  - T5 `app.writer.MetricsWriter(cfg)` — `anchor_exists(date, service) -> bool`, `anchor_source_type(date, service) -> str | None`, `replace_batch(date, items: list[tuple[ServiceEntry, MetricsPayload, NormalizeResult]]) -> dict[str, int]`(`MutationBudgetExceeded` 던짐), `sync_registry(entries) -> bool`; `MutationBudgetExceeded(planned, done, limit)`.
- Produces (T7 `_run_manual`·argparse 확장 / T9 rerun command / T10 manual_load command / T11 E2E 마커 grep / T12 README 표가 소비):
  - CLI: `python -m app.main [batch_time_iso]`(정기; 기본 `datetime.now(KST)`, naive 입력은 KST 해석, target_date = batch_time.date() − 1일) / `python -m app.main --from D0 --to D1 [--service S] [--replace]`(rerun; `--from/--to` 쌍 필수·`D0 <= D1`, 위반 stderr + exit 2). 알 수 없는 `--service` → stderr `unknown service: <S>` + exit 2. `--replace`는 `--from/--to` 없이 쓰면 exit 2(정기 실행은 뮤테이션 0 보장). `--push-vm` 없음. manual 4플래그는 T7.
  - 상수: `MODULE = "token-metrics"`, `MODE_REGULAR = "regular"`, `MODE_RERUN = "rerun"`, `MODE_MANUAL = "manual"`, `KST = timezone(timedelta(hours=9))`, `NOT_READY_REVISIT_CAP_S = 300`, `REASON_DEADLINE = "deadline"`, `REASON_LOAD_BUDGET = "load_budget"`, `REASON_MUTATION_BUDGET = "mutation_budget"`, `_batch_status = {"line": "BATCH_RESULT status=FAILURE module=token-metrics services_ok=0 services_failed=0 services_skipped=0 rows=0 elapsed=0s slot=-- final=0"}`.
  - `def _sigterm_handler(signum, frame) -> None` — `print(_batch_status["line"] + " note=sigterm", flush=True); sys.exit(1)`.
  - `@dataclass class RunContext`: `mode: str`, `replace: bool`, `batch_time: datetime`(aware KST), `slot: str = ""`(`__post_init__`가 비어 있으면 `batch_time.strftime("%H")`로 채움), `final: bool = False`, `source_type: str = SOURCE_API`. 팩토리 `def make_context(cfg: Config, mode: str, batch_time: datetime, replace: bool = False, source_type: str = SOURCE_API) -> RunContext` — `final = (mode == MODE_REGULAR and batch_time.hour >= cfg.final_hour_kst)`. T7 manual 은 `RunContext(mode=MODE_MANUAL, replace=args.replace, batch_time=now, source_type=SOURCE_MANUAL)`(final=False 기본) 또는 `make_context(cfg, MODE_MANUAL, now, args.replace, SOURCE_MANUAL)` 둘 다 가능.
  - `@dataclass class ServiceOutcome`: `service: str`, `status: str = "FAILURE"`, `source_type: str = SOURCE_API`, `rows: int = 0`, `warn: int = 0`, `rejected: int = 0`, `reason: str = ""`, `checks: dict[str, int] = field(default_factory=dict)`.
  - `@dataclass class _QueueItem`: `entry: ServiceEntry`, `resume_at: float = 0.0`, `revisited: bool = False`.
  - `def _service_line(o: ServiceOutcome) -> str` = `f"SERVICE_RESULT status={o.status} module=token-metrics service={o.service} source_type={o.source_type} rows={o.rows} pages=1 warn={o.warn} rejected={o.rejected}" + (f" reason={o.reason}" if o.reason else "")`.
  - `def _check_lines(service: str, checks: dict[str, int]) -> list[str]` = `[f"CHECK WARN service={service} {code}={n}" for code, n in sorted(checks.items()) if n]`(SERVICE_RESULT 직전 출력 — 코드·카운트만).
  - `def _batch_reason(outcomes: list[ServiceOutcome]) -> str` — 어느 outcome 이든 `reason == "mutation_budget"`이면 `"mutation_budget"`, 아니면 `""`.
  - `def _batch_line(outcomes: list[ServiceOutcome], started: float, clock, ctx: RunContext, reason: str = "") -> str` = `f"BATCH_RESULT status={status} module=token-metrics services_ok={ok} services_failed={failed} services_skipped={skipped} rows={rows} elapsed={int(clock() - started)}s slot={ctx.slot} final={int(ctx.final)}" + (f" reason={reason}" if reason else "")`; status: failed>0 → `FAILURE`; outcomes 비어있지 않고 전부 `NODATA` → `NODATA`; 그 외(전부 SKIPPED 포함) `SUCCESS`; `ok` = SUCCESS+NODATA 수.
  - `def _gate(entry: ServiceEntry, target_date: str, ctx: RunContext) -> str | None` — `not entry.enabled` → `"disabled"`(모든 모드); `ctx.mode == MODE_REGULAR`에서만 `date.fromisoformat(target_date) < entry.api_since` → `"before_since"`, `entry.until is not None and date.fromisoformat(target_date) > entry.until` → `"after_until"`; 그 외 `None`.
  - `def _outcome_from_error(entry: ServiceEntry, err: CollectError, ctx: RunContext, revisited: bool) -> ServiceOutcome | None`(`None` = 큐 끝 재방문 필요) — `NOT_READY` & not revisited → `None`; `NOT_READY` & revisited → 정기 비최종 `SKIPPED not_ready` / 정기 최종 `FAILURE not_ready_at_0900` / rerun·manual `FAILURE not_ready`; `RETENTION` → 정기 `FAILURE retention` / rerun·manual `SKIPPED retention`; 그 외 `FAILURE reason=err.event.value`(`retryable`·`permanent_error`·…).
  - `def _prepare_one(cfg: Config, entry: ServiceEntry, target_date: str, ctx: RunContext, writer, fetcher, session) -> ServiceOutcome | tuple[ServiceEntry, MetricsPayload, NormalizeResult]` — gate → `SKIPPED reason=<gate>`; `not ctx.replace and writer.anchor_exists(target_date, entry.service)` → `SKIPPED already_loaded`(+ `ctx.mode == MODE_REGULAR`이고 `writer.anchor_source_type(...) == SOURCE_MANUAL`이면 `checks={"manual_row_present": 1}`); `payload = fetcher(entry, target_date, cfg, session)`(`CollectError` 전파); `normalize_payload(payload, entry)`의 `PayloadError`는 `CollectError(Event.PERMANENT_ERROR, f"report structure: {e}")`로 번역; 성공 시 `(entry, payload, result)` 반환.
  - `def _load_items(cfg: Config, target_date: str, items: list[tuple[ServiceEntry, MetricsPayload, NormalizeResult]], writer, clock, deadline: float | None, ctx: RunContext) -> list[ServiceOutcome]` — `deadline is not None and deadline - clock() < cfg.load_budget_s` → 전 item `FAILURE reason=load_budget`(writer 호출 없음); `writer.replace_batch(target_date, items)` → `MutationBudgetExceeded` → 전 item `FAILURE reason=mutation_budget`; 성공 시 item 별 `status = "NODATA" if result.is_nodata else "SUCCESS"`, `rows = result.rows`, `warn = result.warn_total`, `rejected = result.rejected`, `checks = dict(result.warns)` + (`rows == 0 and rejected > 0` → `checks["all_rows_rejected"] = 1`, status SUCCESS 유지); `source_type = ctx.source_type`.
  - `def run_collection(cfg: Config, entries: list[ServiceEntry], target_date: str, ctx: RunContext, *, clock=time.monotonic, sleeper=time.sleep, fetcher=api_client.fetch_metrics, writer=None, session=None, register_dims: bool = True, dim_entries: list[ServiceEntry] | None = None, emit_batch: bool = True, outcomes_sink: list[ServiceOutcome] | None = None, started: float | None = None) -> int` — `started = clock() if started is None else started`, `deadline = started + cfg.soft_deadline_minutes * 60`, `writer = writer if writer is not None else MetricsWriter(cfg)`, `session = session if session is not None else _session(cfg)`; `register_dims and ctx.mode == MODE_REGULAR` → `writer.sync_registry(dim_entries if dim_entries is not None else entries)`(예외 → `print("CHECK WARN service=- registry_sync_failed=1")` 후 계속); 큐 = 전 entries(disabled 포함 — gate 가 SKIPPED 기록); 루프: 데드라인 초과·잔여 < `load_budget_s` → 잔여 전부 `FAILURE reason=deadline`; ready 없음 → `sleeper(min(wake - now, deadline - now))`; 정기 모드는 item 마다 `_load_items([item])`, rerun·manual 모드는 `pending`에 모아 큐 소진 후 `_load_items(pending)` 1회; `CollectError` → `_outcome_from_error`(None 이면 `item.resume_at = clock() + min(max(err.retry_after_s, 1), NOT_READY_REVISIT_CAP_S)`, `item.revisited = True`, 큐 끝 append); 예상 밖 예외(fetch·normalize·writer) → `FAILURE reason=unexpected:<Type>`; outcome 기록마다 `_check_lines` → `_service_line` 순 print + `_batch_status["line"]` 갱신(`outcomes_sink`가 있으면 누적 전체 기준); 종료 시 `emit_batch`면 `_batch_line(outcomes, started, clock, ctx, reason=_batch_reason(outcomes))` print; 반환 `1 if any FAILURE else 0`.
  - `def _run_dates(cfg: Config, entries: list[ServiceEntry], dim_entries: list[ServiceEntry], dates: list[str], ctx: RunContext, fetcher, *, writer=None, clock=time.monotonic, sleeper=time.sleep, started: float | None = None, register_dims: bool = True) -> int` — writer 1개 공유(뮤테이션 누적), 날짜별 `run_collection(..., register_dims=(register_dims and i == 0), dim_entries=dim_entries, emit_batch=False, outcomes_sink=all_outcomes, started=started)` → 실행당 BATCH_RESULT 1줄(`reason=_batch_reason(all_outcomes)` 승격) → `return worst`.
  - `def _session(cfg: Config)` — 기존 모듈 `collectors/token-usage/app/main.py:134-147` 복제(`requests.Session`, `cfg.https_proxy` None=상속/''=직접/값=전용, `trust_env`, `verify = cfg.api_verify`; `(https_proxy, str(api_verify))` 키로 캐시).
  - `def _parse_batch_time(raw: str | None) -> datetime` — 없음 → `datetime.now(KST)`; naive → `replace(tzinfo=KST)`; aware → `astimezone(KST)`.
  - `def _target_dates(args, batch_time: datetime) -> tuple[list[str], str]` — `--from/--to` 있으면 `([D0..D1], MODE_RERUN)`(쌍 누락 → `ValueError("--from/--to must be given together (KST, YYYY-MM-DD)")`, `D0 > D1` → `ValueError("--from must not be after --to")`, 형식 오류는 `date.fromisoformat`의 `ValueError`); 없으면 `([str(batch_time.date() - timedelta(days=1))], MODE_REGULAR)`.
  - `def main(argv=None) -> int` — argparse(`batch_time` nargs="?", `--from`(dest `from_date`), `--to`(dest `to_date`), `--service`, `--replace` store_true) → `signal.signal(SIGTERM, _sigterm_handler)` → `load_config()`/`load_endpoints(cfg.endpoints_file)`(예외 → stderr `config error: <Type>: <msg>` + exit 2) → `--service` 필터(`dim_entries`는 필터 전 전체) → `_parse_batch_time`/`_target_dates`(`ValueError` → stderr + 2) → `--replace` without range → stderr + 2 → `ctx = make_context(cfg, mode, batch_time, replace=args.replace)` → `_run_dates(cfg, entries, all_entries, dates, ctx, api_client.fetch_metrics)`.

- [ ] **Step 1: 전제 확인 — T2~T5 산출물·시그니처**

Run: `cd collectors/token-metrics && ls app/config.py app/events.py app/normalize.py app/api_client.py app/writer.py conftest.py tests/__init__.py && grep -n "^def fetch_metrics" app/api_client.py && grep -n "    def anchor_exists\|    def anchor_source_type\|    def replace_batch\|    def sync_registry\|^class MutationBudgetExceeded" app/writer.py && grep -n "NOT_READY = \|RETENTION = " app/events.py && grep -n "final_hour_kst: int\|load_budget_s: int\|soft_deadline_minutes: int" app/config.py`
Expected: 7개 파일 경로, `app/api_client.py:<n>:def fetch_metrics(entry: ServiceEntry, date: str, cfg: Config, session) -> MetricsPayload:` 1줄, `app/writer.py` 5줄(`anchor_exists`·`anchor_source_type`·`replace_batch`·`sync_registry`·`class MutationBudgetExceeded`), `app/events.py` 2줄(`NOT_READY = "not_ready"`, `RETENTION = "retention"`), `app/config.py` 3줄(`soft_deadline_minutes: int = 40`, `load_budget_s: int = 1200`, `final_hour_kst: int = 9`). 하나라도 없으면 T2~T5가 병합되지 않은 것 — 중단하고 보고한다(대신 만들지 않는다).

Run: `cd collectors/token-metrics && python3 -m pytest -q 2>&1 | tail -1`
Expected: 현재 통과 수 `104 passed`(T2 14 + T3 38 + T4 22 + T5 30 — T5 Step 14의 `30 passed`; 실제 커밋의 수가 다르면 그 합 — Step 9의 누적 기대값에 쓴다).

Run: `sed -n 247,258p docs/superpowers/specs/2026-08-31-token-metrics-ingest-design.md | grep -c "already_loaded\|not_ready_at_0900\|all_rows_rejected\|slot=HH final=0|1"`
Expected: `4`(§5.2 표·마커 문단의 어휘 4개 — 이 태스크의 reason·마커 어휘가 여기서 온다).

- [ ] **Step 2: 실패하는 테스트 1 — 마커 문자열·상태 규칙·게이트·오류→outcome 매트릭스·RunContext·날짜 산출·SIGTERM** — `collectors/token-metrics/tests/test_main.py` 신규(전체 내용; run_collection 시나리오는 Step 6이 끝에 추가)

```python
"""main 오케스트레이터(§5.2 모드×게이트 · §5.4 예산 가드 · 마커 · SIGTERM) 테스트 — Fake writer/fetcher, DB·HTTP 없음.
공통 fixture 상수는 Plan 6b 전 태스크 공통(Mock Group / Mock Service A / 2026-09-10 / 2026-09-11T02:05+09:00)."""
import argparse
from datetime import date, datetime

import pytest

from app.config import Config, ServiceEntry
from app.events import CollectError, Event
from app.main import (KST, MODE_MANUAL, MODE_REGULAR, MODE_RERUN, MODULE, NOT_READY_REVISIT_CAP_S,
                      RunContext, ServiceOutcome, _batch_line, _batch_reason, _batch_status,
                      _check_lines, _gate, _outcome_from_error, _parse_batch_time, _service_line,
                      _sigterm_handler, _target_dates, make_context)

SERVICE_GROUP = "Mock Group"
SERVICE = "Mock Service A"
SERVICE_B = "Mock Service B"
SERVICE_C = "Mock Service C"
BASE_URL = "http://mock"
DATE = "2026-09-10"
GPU_TYPE = "H100"
ENGINE = {"type": "vllm", "version": "0.10.1"}
GENERATED_AT = "2026-09-11T02:05:00+09:00"
MODELS = ["claude-opus-4-8", "claude-sonnet-5", "claude-haiku-4-5"]


def entry(service=SERVICE, enabled=True, api_since="2026-09-09", until=None, base_url=BASE_URL) -> ServiceEntry:
    return ServiceEntry(service_group=SERVICE_GROUP, service=service, base_url=base_url, enabled=enabled,
                        api_since=date.fromisoformat(api_since), coverage_since=date(2026, 8, 26),
                        until=None if until is None else date.fromisoformat(until))


ENTRY = entry()
ENTRY_B = entry(SERVICE_B, base_url="http://mock-b")
ENTRY_C = entry(SERVICE_C, base_url="http://mock-c")


class Clock:
    def __init__(self, t=0.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s


def ctx(mode=MODE_REGULAR, hour=2, replace=False, cfg=None) -> RunContext:
    """batch_time = 2026-09-11 HH:05 KST — 정기 target_date 는 DATE(2026-09-10)."""
    return make_context(cfg or Config(), mode, datetime(2026, 9, 11, hour, 5, tzinfo=KST), replace=replace)


# ---------- 상수·RunContext ----------

def test_module_constants_and_initial_batch_line():
    assert MODULE == "token-metrics"
    assert (MODE_REGULAR, MODE_RERUN, MODE_MANUAL) == ("regular", "rerun", "manual")
    assert NOT_READY_REVISIT_CAP_S == 300
    assert _batch_status["line"].startswith("BATCH_RESULT status=FAILURE module=token-metrics ")
    assert _batch_status["line"].endswith(" rows=0 elapsed=0s slot=-- final=0")


def test_make_context_slot_and_final():
    c = ctx(hour=2)
    assert (c.mode, c.replace, c.slot, c.final, c.source_type) == ("regular", False, "02", False, "metrics-api-v1")
    assert ctx(hour=9).final is True                                   # FINAL_HOUR_KST 기본 9
    assert ctx(hour=8).final is False
    assert ctx(hour=8, cfg=Config(final_hour_kst=8)).final is True     # env 조정 반영
    assert ctx(mode=MODE_RERUN, hour=9).final is False                 # 최종 판정은 정기 전용
    assert ctx(mode=MODE_MANUAL, hour=9).final is False
    manual = RunContext(mode=MODE_MANUAL, replace=True, batch_time=datetime(2026, 9, 11, 14, 0, tzinfo=KST),
                        source_type="manual-v0")                       # T7 조립 형태
    assert (manual.slot, manual.final, manual.source_type) == ("14", False, "manual-v0")


# ---------- 마커 문자열 ----------

def test_service_line_format_exact():
    o = ServiceOutcome(service=SERVICE, status="SUCCESS", rows=5, warn=2, rejected=1)
    assert _service_line(o) == ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A "
                                "source_type=metrics-api-v1 rows=5 pages=1 warn=2 rejected=1")
    o = ServiceOutcome(service=SERVICE, status="SKIPPED", source_type="manual-v0", reason="already_loaded")
    assert _service_line(o) == ("SERVICE_RESULT status=SKIPPED module=token-metrics service=Mock Service A "
                                "source_type=manual-v0 rows=0 pages=1 warn=0 rejected=0 reason=already_loaded")
    assert ServiceOutcome(service=SERVICE).status == "FAILURE"          # 기본 = FAILURE


def test_check_lines_sorted_nonzero_only_no_payload():
    lines = _check_lines(SERVICE, {"identity_drift": 1, "hours_over_count": 2, "engine_malformed": 0})
    assert lines == ["CHECK WARN service=Mock Service A hours_over_count=2",
                     "CHECK WARN service=Mock Service A identity_drift=1"]
    assert _check_lines(SERVICE, {}) == []


def test_batch_line_status_rules_slot_final_reason():
    clock = Clock(7.9)
    ok = ServiceOutcome(service=SERVICE, status="SUCCESS", rows=5)
    nodata = ServiceOutcome(service=SERVICE_B, status="NODATA")
    skipped = ServiceOutcome(service=SERVICE_C, status="SKIPPED", reason="disabled")
    failed = ServiceOutcome(service=SERVICE_C, reason="retention")
    assert _batch_line([ok, nodata, skipped], 0.0, clock, ctx(hour=2)) == (
        "BATCH_RESULT status=SUCCESS module=token-metrics services_ok=2 services_failed=0 "
        "services_skipped=1 rows=5 elapsed=7s slot=02 final=0")
    assert _batch_line([ok, failed], 0.0, clock, ctx(hour=9)).startswith(
        "BATCH_RESULT status=FAILURE module=token-metrics services_ok=1 services_failed=1 services_skipped=0 ")
    assert _batch_line([ok, failed], 0.0, clock, ctx(hour=9)).endswith(" slot=09 final=1")
    assert "status=NODATA " in _batch_line([nodata, nodata], 0.0, clock, ctx())      # 전부 NODATA
    assert "status=SUCCESS " in _batch_line([skipped], 0.0, clock, ctx())           # 전부 SKIPPED = SUCCESS
    assert "status=SUCCESS " in _batch_line([], 0.0, clock, ctx())
    assert _batch_line([failed], 0.0, clock, ctx(), reason="mutation_budget").endswith(
        " slot=02 final=0 reason=mutation_budget")
    assert _batch_reason([ok, ServiceOutcome(service=SERVICE_B, reason="mutation_budget")]) == "mutation_budget"
    assert _batch_reason([ok, failed]) == ""


def test_sigterm_reemits_cached_line(capsys):
    cached = ("BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 services_failed=0 "
              "services_skipped=0 rows=5 elapsed=3s slot=02 final=0")
    _batch_status["line"] = cached
    with pytest.raises(SystemExit) as ei:
        _sigterm_handler(15, None)
    assert ei.value.code == 1
    assert capsys.readouterr().out.rstrip("\n").splitlines()[-1] == cached + " note=sigterm"


# ---------- 게이트 매트릭스 (§5.2 모드×게이트) ----------

def test_gate_disabled_all_modes():
    e = entry(enabled=False)
    for mode in (MODE_REGULAR, MODE_RERUN, MODE_MANUAL):
        assert _gate(e, DATE, ctx(mode=mode)) == "disabled"


def test_gate_before_since_after_until_regular_only():
    e = entry(api_since="2026-09-09", until="2026-09-30")
    assert _gate(e, "2026-09-08", ctx()) == "before_since"
    assert _gate(e, "2026-09-09", ctx()) is None                        # 경계 = 포함
    assert _gate(e, "2026-09-30", ctx()) is None
    assert _gate(e, "2026-10-01", ctx()) == "after_until"
    assert _gate(entry(until=None), "2099-01-01", ctx()) is None       # until 없음 = 열림
    for mode in (MODE_RERUN, MODE_MANUAL):
        assert _gate(e, "2026-09-08", ctx(mode=mode)) is None
        assert _gate(e, "2026-10-01", ctx(mode=mode)) is None


# ---------- 오류 → outcome 매트릭스 ----------

def test_outcome_from_error_not_ready_matrix():
    err = CollectError(Event.NOT_READY, "409", retry_after_s=900)
    assert _outcome_from_error(ENTRY, err, ctx(), revisited=False) is None          # 큐 끝 재방문
    o = _outcome_from_error(ENTRY, err, ctx(hour=2), revisited=True)
    assert (o.status, o.reason) == ("SKIPPED", "not_ready")
    o = _outcome_from_error(ENTRY, err, ctx(hour=9), revisited=True)
    assert (o.status, o.reason) == ("FAILURE", "not_ready_at_0900")
    o = _outcome_from_error(ENTRY, err, ctx(mode=MODE_RERUN, hour=9), revisited=True)
    assert (o.status, o.reason) == ("FAILURE", "not_ready")
    assert _outcome_from_error(ENTRY, err, ctx(mode=MODE_RERUN), revisited=False) is None


def test_outcome_from_error_retention_and_others():
    ret = CollectError(Event.RETENTION, "404")
    assert (_outcome_from_error(ENTRY, ret, ctx(), False).status,
            _outcome_from_error(ENTRY, ret, ctx(), False).reason) == ("FAILURE", "retention")
    o = _outcome_from_error(ENTRY, ret, ctx(mode=MODE_RERUN), False)
    assert (o.status, o.reason) == ("SKIPPED", "retention")
    for ev in (Event.PERMANENT_ERROR, Event.RETRYABLE, Event.INVARIANT_BROKEN):
        o = _outcome_from_error(ENTRY, CollectError(ev, "x"), ctx(), True)
        assert (o.status, o.reason, o.service) == ("FAILURE", ev.value, SERVICE)
    manual = RunContext(mode=MODE_MANUAL, replace=False, batch_time=datetime(2026, 9, 11, 14, 0, tzinfo=KST),
                        source_type="manual-v0")
    o = _outcome_from_error(ENTRY, CollectError(Event.PERMANENT_ERROR, "x"), manual, False)
    assert o.source_type == "manual-v0"                                 # ctx.source_type 복사


# ---------- 날짜 산출·batch_time 해석 ----------

def _args(**kw) -> argparse.Namespace:
    base = {"batch_time": None, "from_date": None, "to_date": None, "service": None, "replace": False}
    base.update(kw)
    return argparse.Namespace(**base)


def test_parse_batch_time_naive_is_kst_and_aware_converted():
    bt = _parse_batch_time("2026-09-11T02:05:00")
    assert bt == datetime(2026, 9, 11, 2, 5, tzinfo=KST) and bt.tzinfo is not None
    assert _parse_batch_time("2026-09-10T17:05:00+00:00") == datetime(2026, 9, 11, 2, 5, tzinfo=KST)
    assert _parse_batch_time(None).tzinfo is not None                  # now(KST)


def test_target_dates_regular_is_yesterday_rerun_is_range():
    bt = _parse_batch_time("2026-09-11T02:05:00+09:00")
    assert _target_dates(_args(), bt) == (["2026-09-10"], MODE_REGULAR)
    assert _target_dates(_args(from_date="2026-09-01", to_date="2026-09-03"), bt) == (
        ["2026-09-01", "2026-09-02", "2026-09-03"], MODE_RERUN)
    assert _target_dates(_args(from_date="2026-09-03", to_date="2026-09-03"), bt)[0] == ["2026-09-03"]


def test_target_dates_rejects_half_pair_and_reversed():
    bt = _parse_batch_time(None)
    with pytest.raises(ValueError, match="--from/--to"):
        _target_dates(_args(from_date="2026-09-01"), bt)
    with pytest.raises(ValueError, match="--from/--to"):
        _target_dates(_args(to_date="2026-09-01"), bt)
    with pytest.raises(ValueError, match="after"):
        _target_dates(_args(from_date="2026-09-03", to_date="2026-09-01"), bt)
    with pytest.raises(ValueError):
        _target_dates(_args(from_date="2026-13-01", to_date="2026-09-01"), bt)
```

- [ ] **Step 3: 실패 확인**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_main.py`
Expected: `ImportError while importing test module '…/collectors/token-metrics/tests/test_main.py'` … `E   ModuleNotFoundError: No module named 'app.main'` → `Interrupted: 1 error during collection`(마지막 줄 `1 error in …s`).

- [ ] **Step 4: 구현 1 — 상수·`_sigterm_handler`·`RunContext`/`make_context`·`ServiceOutcome`·`_QueueItem`·마커 3함수·`_gate`·`_outcome_from_error`·`_session`·`_parse_batch_time`·`_target_dates`** — `collectors/token-metrics/app/main.py` 신규(전체 내용; `run_collection`·`_prepare_one`·`_load_items`·`_run_dates`·`main`은 Step 8이 끝에 추가)

```python
"""수집 오케스트레이터 — collectors/token-usage/app/main.py 의 클론 (설계 2026-08-31 §5.1).

정책(§5.2 모드×게이트 표 · 409 큐 끝 재방문 1회 · 최종 슬롯 판정 · §5.4 적재 예산 가드 · 마커)은
이 파일에 1벌만 존재한다. api_client 는 HTTP→Event 번역, normalize 는 순수 함수, writer 는 적재 시퀀스만.

모드(RunContext.mode):
  regular — target_date = KST 오늘−1. api_since/until 게이트·최종 슬롯 판정(batch_time.hour >= FINAL_HOUR_KST)
            ·레지스트리 동기화·manual_row_present WARN 은 이 모드에만. 앵커 존재 = SKIPPED already_loaded(뮤테이션 0).
  rerun   — --from/--to. 게이트·final 무시(409 재차 = FAILURE not_ready, 404 = SKIPPED retention).
            --replace 없으면 앵커 존재 = already_loaded. 날짜당 replace_batch 1회(§5.4 배칭 (A)→(B)(C)).
  manual  — T7 이 추가(CSV → MetricsPayload, source_type manual-v0). rerun 과 같은 정책, 동기화 없음.

로깅 계약(§3 전제 11·마스터 §5.6): 어떤 로그에도 페이로드·행 원문을 남기지 않는다 — 카운트·서비스명·코드만.
마커: SERVICE_RESULT(서비스당 1줄) / BATCH_RESULT(실행당 1줄, slot=HH final=0|1) / CHECK WARN(코드=카운트).
"""
from __future__ import annotations

import argparse
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import date as date_cls, datetime, timedelta, timezone

import requests

from app import api_client
from app.config import Config, ServiceEntry, load_config, load_endpoints
from app.events import CollectError, Event
from app.normalize import (SOURCE_API, SOURCE_MANUAL, MetricsPayload, NormalizeResult, PayloadError,
                           normalize_payload)
from app.writer import MetricsWriter, MutationBudgetExceeded

KST = timezone(timedelta(hours=9))
MODULE = "token-metrics"
MODE_REGULAR = "regular"
MODE_RERUN = "rerun"
MODE_MANUAL = "manual"
NOT_READY_REVISIT_CAP_S = 300            # §5.2 409: 큐 끝 1회 재방문, 대기 = min(Retry-After, 300)s
REASON_DEADLINE = "deadline"
REASON_LOAD_BUDGET = "load_budget"
REASON_MUTATION_BUDGET = "mutation_budget"

_batch_status = {"line": f"BATCH_RESULT status=FAILURE module={MODULE} services_ok=0 services_failed=0 "
                         "services_skipped=0 rows=0 elapsed=0s slot=-- final=0"}


def _sigterm_handler(signum, frame):
    print(_batch_status["line"] + " note=sigterm", flush=True)     # 마커 보장 (§5.2 SIGTERM 캐시 줄 재출력)
    sys.exit(1)


@dataclass
class RunContext:
    mode: str                              # MODE_REGULAR | MODE_RERUN | MODE_MANUAL
    replace: bool                          # --replace (rerun·manual) — 앵커 존재 시 교체 허용
    batch_time: datetime                   # aware KST
    slot: str = ""                         # batch_time.strftime("%H") — BATCH_RESULT slot=HH
    final: bool = False                    # 정기 & batch_time.hour >= FINAL_HOUR_KST (make_context 가 계산)
    source_type: str = SOURCE_API          # 모든 ServiceOutcome.source_type 에 복사 (manual 은 SOURCE_MANUAL)

    def __post_init__(self) -> None:
        if not self.slot:
            self.slot = self.batch_time.strftime("%H")


def make_context(cfg: Config, mode: str, batch_time: datetime, replace: bool = False,
                 source_type: str = SOURCE_API) -> RunContext:
    """최종 슬롯 판정은 정기 실행에만 (§5.2 '실행 모드 × 게이트') — rerun·manual 은 항상 final=0."""
    final = mode == MODE_REGULAR and batch_time.hour >= cfg.final_hour_kst
    return RunContext(mode=mode, replace=replace, batch_time=batch_time, final=final, source_type=source_type)


@dataclass
class ServiceOutcome:
    service: str
    status: str = "FAILURE"                # SUCCESS | NODATA | SKIPPED | FAILURE
    source_type: str = SOURCE_API
    rows: int = 0                          # gpu + serving + custom (normalize 통과 행)
    warn: int = 0                          # NormalizeResult.warn_total (행 플래그 + 응답 WARN)
    rejected: int = 0
    reason: str = ""
    checks: dict[str, int] = field(default_factory=dict)   # CHECK WARN <code>=<count> (SERVICE_RESULT 직전 출력)


@dataclass
class _QueueItem:
    entry: ServiceEntry
    resume_at: float = 0.0
    revisited: bool = False                # 409 큐 끝 재방문은 1회 — 재차 409 는 최종 판정


def _service_line(o: ServiceOutcome) -> str:
    return (f"SERVICE_RESULT status={o.status} module={MODULE} service={o.service} "
            f"source_type={o.source_type} rows={o.rows} pages=1 warn={o.warn} rejected={o.rejected}"
            + (f" reason={o.reason}" if o.reason else ""))


def _check_lines(service: str, checks: dict[str, int]) -> list[str]:
    """인라인 검증 마커 — 코드·카운트만 (페이로드 없음). 0 인 코드는 출력하지 않는다."""
    return [f"CHECK WARN service={service} {code}={n}" for code, n in sorted(checks.items()) if n]


def _batch_reason(outcomes: list[ServiceOutcome]) -> str:
    """§4.0 뮤테이션 가드 — 어느 서비스든 mutation_budget 이면 배치 reason 으로 승격 (exit 1 과 함께 알림 근거)."""
    return REASON_MUTATION_BUDGET if any(o.reason == REASON_MUTATION_BUDGET for o in outcomes) else ""


def _batch_line(outcomes: list[ServiceOutcome], started: float, clock, ctx: RunContext,
                reason: str = "") -> str:
    ok = sum(1 for o in outcomes if o.status in ("SUCCESS", "NODATA"))
    failed = sum(1 for o in outcomes if o.status == "FAILURE")
    skipped = sum(1 for o in outcomes if o.status == "SKIPPED")
    total_rows = sum(o.rows for o in outcomes)
    if failed:
        status = "FAILURE"
    elif outcomes and all(o.status == "NODATA" for o in outcomes):
        status = "NODATA"
    else:
        status = "SUCCESS"                 # 전부 SKIPPED(게이트·already_loaded)도 SUCCESS — 뮤테이션 0 정상 종료
    return (f"BATCH_RESULT status={status} module={MODULE} services_ok={ok} "
            f"services_failed={failed} services_skipped={skipped} rows={total_rows} "
            f"elapsed={int(clock() - started)}s slot={ctx.slot} final={int(ctx.final)}"
            + (f" reason={reason}" if reason else ""))


def _gate(entry: ServiceEntry, target_date: str, ctx: RunContext) -> str | None:
    """§5.2 게이트 — disabled 는 모든 모드, api_since/until 은 정기 실행에만. 반환 = SKIPPED reason 또는 None."""
    if not entry.enabled:
        return "disabled"
    if ctx.mode != MODE_REGULAR:
        return None
    target = date_cls.fromisoformat(target_date)
    if target < entry.api_since:
        return "before_since"
    if entry.until is not None and target > entry.until:
        return "after_until"
    return None


def _outcome_from_error(entry: ServiceEntry, err: CollectError, ctx: RunContext,
                        revisited: bool) -> ServiceOutcome | None:
    """CollectError → ServiceOutcome. None = 409 첫 방문: 호출자가 큐 끝에 재삽입한다 (§5.2 409 행)."""
    st = ctx.source_type
    if err.event is Event.NOT_READY:
        if not revisited:
            return None
        if ctx.mode == MODE_REGULAR:
            if ctx.final:                  # 최종 슬롯 재차 409 → exit 1 → BATCH FAILURE = 스펙 09:00 알림
                return ServiceOutcome(service=entry.service, source_type=st, reason="not_ready_at_0900")
            return ServiceOutcome(service=entry.service, status="SKIPPED", source_type=st, reason="not_ready")
        return ServiceOutcome(service=entry.service, source_type=st, reason="not_ready")
    if err.event is Event.RETENTION:
        if ctx.mode == MODE_REGULAR:
            return ServiceOutcome(service=entry.service, source_type=st, reason="retention")
        return ServiceOutcome(service=entry.service, status="SKIPPED", source_type=st, reason="retention")
    return ServiceOutcome(service=entry.service, source_type=st, reason=err.event.value)


_sessions: dict = {}


def _session(cfg: Config):
    """프록시/CA 의미는 기존 모듈과 동일 (§5.2 프록시/CA 3종): None=상속, ''=직접 연결, 값=전용 프록시."""
    key = (cfg.https_proxy, str(cfg.api_verify))
    if key not in _sessions:
        sess = requests.Session()
        if cfg.https_proxy is not None:
            sess.proxies = {"http": cfg.https_proxy or None,
                            "https": cfg.https_proxy or None}
            sess.trust_env = bool(cfg.https_proxy)
        sess.verify = cfg.api_verify
        _sessions[key] = sess
    return _sessions[key]


def _parse_batch_time(raw: str | None) -> datetime:
    """naive 입력은 KST 로 해석(호스트 TZ 무관), aware 는 KST 로 변환 — 슬롯(HH)·final 판정의 기준."""
    if not raw:
        return datetime.now(KST)
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=KST)
    return parsed.astimezone(KST)


def _target_dates(args, batch_time: datetime) -> tuple[list[str], str]:
    """--from/--to → ([D0..D1], rerun) / 없으면 ([batch_time.date() − 1], regular). 위반은 ValueError (main 이 exit 2)."""
    if args.from_date or args.to_date:
        if not (args.from_date and args.to_date):
            raise ValueError("--from/--to must be given together (KST, YYYY-MM-DD)")
        d0 = date_cls.fromisoformat(args.from_date)
        d1 = date_cls.fromisoformat(args.to_date)
        if d0 > d1:
            raise ValueError("--from must not be after --to")
        return [str(d0 + timedelta(days=i)) for i in range((d1 - d0).days + 1)], MODE_RERUN
    return [str(batch_time.date() - timedelta(days=1))], MODE_REGULAR
```

- [ ] **Step 5: 통과 확인(구현 1)**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_main.py`
Expected: `13 passed`

Run: `cd collectors/token-metrics && python3 -c "import app.main as m; print(m.MODULE, m.MODE_REGULAR, m.MODE_RERUN, m.MODE_MANUAL, m.NOT_READY_REVISIT_CAP_S); print(m._batch_status['line'])"`
Expected: `token-metrics regular rerun manual 300` 다음 줄 `BATCH_RESULT status=FAILURE module=token-metrics services_ok=0 services_failed=0 services_skipped=0 rows=0 elapsed=0s slot=-- final=0`(개발기 3.10 import 성공 — `from __future__ import annotations`로 `str | None` 표기 허용).

- [ ] **Step 6: 실패하는 테스트 2 — `run_collection` 시나리오(게이트·앵커·200 상태값·409 재방문·오류 매트릭스·예산·배칭·레지스트리)·`main()`** — `collectors/token-metrics/tests/test_main.py` **끝에 추가**(모듈 수준 import를 청크 머리에 두는 것은 의도 — 이 청크가 독립적으로 red가 되도록)

```python
# ---------- run_collection / main (Step 6 추가) ----------
from app.main import _run_dates, main, run_collection   # noqa: E402
from app.normalize import check_report_structure         # noqa: E402
from app.writer import MutationBudgetExceeded            # noqa: E402


def G(**kw) -> dict:
    base = {"model": MODELS[1], "gpuType": GPU_TYPE, "category": "serving", "gpuCount": 4, "gpuHours": 96.0}
    base.update(kw)
    return base


TTFT = {"p50": 280, "p90": 560, "p95": 720, "p99": 1200}
ITL = {"p50": 24, "p90": 38, "p95": 47, "p99": 80}


def S(**kw) -> dict:
    base = {"model": MODELS[1], "ttftMs": dict(TTFT), "itlMs": dict(ITL), "outputTps": {"p50": 41.0}}
    base.update(kw)
    return base


def report(d=DATE, service=SERVICE, gpu=None, serving=None, **top) -> dict:
    """기본 = gpu 2행(opus·sonnet) + serving 1레코드(ttftMs·itlMs·outputTps → long form 3행) → rows 5."""
    body = {"date": d, "serviceGroup": SERVICE_GROUP, "service": service, "generatedAt": GENERATED_AT,
            "engine": ENGINE,
            "gpu": [G(model=MODELS[0]), G(model=MODELS[1])] if gpu is None else gpu,
            "serving": [S()] if serving is None else serving}
    body.update(top)
    return body


class FakeWriter:
    """T5 MetricsWriter 대역 — main 이 부르는 4메서드만. batches = replace_batch 호출 기록 [(date, [services])]."""

    def __init__(self, anchors=(), anchor_types=None, raise_budget=False, raise_sync=False):
        self.anchors = set(anchors)                      # {(date, service)} — anchor_exists
        self.anchor_types = dict(anchor_types or {})     # {(date, service): source_type} — anchor_source_type
        self.batches = []
        self.sync_calls = 0
        self.sync_entries = None
        self.raise_budget = raise_budget
        self.raise_sync = raise_sync

    def anchor_exists(self, date, service):
        return (date, service) in self.anchors

    def anchor_source_type(self, date, service):
        return self.anchor_types.get((date, service))

    def replace_batch(self, date, items):
        if self.raise_budget:
            raise MutationBudgetExceeded(3, 0, 2)
        self.batches.append((date, [e.service for e, _, _ in items]))
        return {e.service: r.rows for e, _, r in items}

    def sync_registry(self, entries):
        self.sync_calls += 1
        self.sync_entries = [e.service for e in entries]
        if self.raise_sync:
            raise RuntimeError("ch down")
        return True


def fetcher_ok(**overrides):
    """항상 200 — report(**overrides) 를 서비스명·날짜만 바꿔 MetricsPayload 로 반환. f.calls = [(service, date)]."""
    calls = []

    def f(entry, d, cfg, session):
        calls.append((entry.service, d))
        return check_report_structure(report(d=d, service=entry.service, **overrides), d)
    f.calls = calls
    return f


def fetcher_script(script):
    """서비스별 순서 스크립트 {service: [CollectError | None, ...]} — CollectError 는 raise, None 은 기본 200.
    소진 후 마지막 항목 반복. 스크립트에 없는 서비스는 항상 200. f.calls = [service, ...] (호출 순서)."""
    calls = []
    cursor = {}

    def f(entry, d, cfg, session):
        calls.append(entry.service)
        seq = script.get(entry.service, [None])
        i = cursor.get(entry.service, 0)
        cursor[entry.service] = i + 1
        step = seq[min(i, len(seq) - 1)]
        if isinstance(step, BaseException):
            raise step
        return check_report_structure(report(d=d, service=entry.service), d)
    f.calls = calls
    return f


def nr(retry=900) -> CollectError:
    return CollectError(Event.NOT_READY, "409", retry_after_s=retry)


def run(capsys, entries, fetcher, *, c=None, cfg=None, writer=None, clock=None, sleeps=None,
        dim_entries=None, register_dims=True, target=DATE):
    """run_collection 1회 → (exit code, writer, stdout 줄 목록). sleeper 는 시계를 전진시킨다(재방문 도달)."""
    clock = clock or Clock()
    w = writer if writer is not None else FakeWriter()

    def sleeper(s):
        if sleeps is not None:
            sleeps.append(s)
        clock.advance(s)

    code = run_collection(cfg or Config(), entries, target, c or ctx(), clock=clock, sleeper=sleeper,
                          fetcher=fetcher, writer=w, session=object(), register_dims=register_dims,
                          dim_entries=dim_entries)
    return code, w, capsys.readouterr().out.rstrip("\n").splitlines()


def _first(lines, prefix):
    return next(i for i, l in enumerate(lines) if l.startswith(prefix))


# ---------- 게이트·앵커 (§5.2 표 1~4행) ----------

def test_gate_disabled_all_modes_no_fetch(capsys):
    f = fetcher_ok()
    for mode in (MODE_REGULAR, MODE_RERUN):
        code, w, out = run(capsys, [entry(enabled=False)], f, c=ctx(mode=mode))
        assert code == 0 and f.calls == [] and w.batches == []
        assert ("SERVICE_RESULT status=SKIPPED module=token-metrics service=Mock Service A "
                "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=disabled") in out


def test_gate_before_since_after_until_regular_only_in_run(capsys):
    e = entry(api_since="2026-09-09", until="2026-09-30")
    f = fetcher_ok()
    _, _, out = run(capsys, [e], f, target="2026-09-08")
    assert any(l.endswith("reason=before_since") for l in out) and f.calls == []
    _, _, out = run(capsys, [e], f, target="2026-10-01")
    assert any(l.endswith("reason=after_until") for l in out) and f.calls == []
    code, w, out = run(capsys, [e], f, c=ctx(mode=MODE_RERUN), target="2026-09-08")
    assert code == 0 and f.calls == [(SERVICE, "2026-09-08")] and w.batches == [("2026-09-08", [SERVICE])]


def test_already_loaded_skips_without_fetch(capsys):
    f = fetcher_ok()
    code, w, out = run(capsys, [ENTRY], f, writer=FakeWriter(anchors={(DATE, SERVICE)}))
    assert code == 0 and f.calls == [] and w.batches == []
    assert ("SERVICE_RESULT status=SKIPPED module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=already_loaded") in out
    code, w, out = run(capsys, [ENTRY], f, c=ctx(mode=MODE_RERUN), writer=FakeWriter(anchors={(DATE, SERVICE)}))
    assert code == 0 and f.calls == [] and any(l.endswith("reason=already_loaded") for l in out)   # rerun w/o --replace


def test_manual_row_present_warn_regular_only(capsys):
    mk = lambda: FakeWriter(anchors={(DATE, SERVICE)}, anchor_types={(DATE, SERVICE): "manual-v0"})  # noqa: E731
    _, _, out = run(capsys, [ENTRY], fetcher_ok(), writer=mk())
    assert out.index("CHECK WARN service=Mock Service A manual_row_present=1") < _first(out, "SERVICE_RESULT")
    assert any(l.endswith("reason=already_loaded") for l in out)
    _, _, out = run(capsys, [ENTRY], fetcher_ok(), c=ctx(mode=MODE_RERUN), writer=mk())
    assert not any(l.startswith("CHECK WARN") for l in out)
    assert any(l.endswith("reason=already_loaded") for l in out)


def test_replace_bypasses_anchor(capsys):
    f = fetcher_ok()
    code, w, out = run(capsys, [ENTRY], f, c=ctx(mode=MODE_RERUN, replace=True),
                       writer=FakeWriter(anchors={(DATE, SERVICE)}))
    assert code == 0 and len(f.calls) == 1 and w.batches == [(DATE, [SERVICE])]
    assert any(l.startswith("SERVICE_RESULT status=SUCCESS") for l in out)


# ---------- 200 → 상태값 (§5.2 표 "200" 행) ----------

def test_success_nodata_and_case_e(capsys):
    _, w, out = run(capsys, [ENTRY], fetcher_ok())
    assert ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=5 pages=1 warn=0 rejected=0") in out
    assert w.batches == [(DATE, [SERVICE])]
    code, w, out = run(capsys, [ENTRY], fetcher_ok(gpu=[], serving=[]))
    assert code == 0 and w.batches == [(DATE, [SERVICE])]               # NODATA 도 앵커(summary) 적재
    assert ("SERVICE_RESULT status=NODATA module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0") in out
    assert out[-1].startswith("BATCH_RESULT status=NODATA ")
    _, _, out = run(capsys, [ENTRY], fetcher_ok(gpu=[]))                # 케이스 E: gpu:[] + serving 행 = SUCCESS
    assert ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=3 pages=1 warn=0 rejected=0") in out


def test_all_rows_rejected_warn(capsys):
    _, _, out = run(capsys, [ENTRY], fetcher_ok(gpu=[G(category="prod")], serving=[]))
    assert "CHECK WARN service=Mock Service A all_rows_rejected=1" in out
    assert ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=1") in out
    assert out[-1].startswith("BATCH_RESULT status=SUCCESS ")


def test_warn_and_check_lines_from_flags(capsys):
    _, _, out = run(capsys, [ENTRY], fetcher_ok(gpu=[G(gpuCount=2, gpuHours=49)], serving=[],
                                                 serviceGroup="Drift Group"))
    assert [l for l in out if l.startswith("CHECK WARN")] == [
        "CHECK WARN service=Mock Service A hours_over_count=1",
        "CHECK WARN service=Mock Service A identity_drift=1"]         # 코드 정렬, 카운트만
    assert out.index("CHECK WARN service=Mock Service A identity_drift=1") < _first(out, "SERVICE_RESULT")
    assert ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=1 pages=1 warn=2 rejected=0") in out


def test_no_payload_in_logs(capsys):
    secret = "secret-model-xyz"
    _, _, out = run(capsys, [ENTRY], fetcher_ok(gpu=[G(model=secret, gpuCount=1, gpuHours=99)],
                                                 serving=[S(model=secret)]))
    assert secret not in "\n".join(out)                                 # 로그 페이로드 금지 (§3 전제 11)
    assert "CHECK WARN service=Mock Service A hours_over_count=1" in out
```

(이어서 — 같은 파일 끝에 계속 append)

```python
# ---------- 409 not_ready: 큐 끝 재방문 1회 · 최종 슬롯 · rerun (§5.2) ----------

def test_409_revisit_once_then_skip_non_final(capsys):
    f = fetcher_script({SERVICE: [nr(900), nr(900)]})
    sleeps = []
    code, w, out = run(capsys, [ENTRY], f, sleeps=sleeps)
    assert code == 0 and f.calls == [SERVICE, SERVICE] and w.batches == []
    assert sleeps == [300]                                              # min(max(900,1), 300)
    assert ("SERVICE_RESULT status=SKIPPED module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=not_ready") in out
    assert out[-1].startswith("BATCH_RESULT status=SUCCESS module=token-metrics services_ok=0 "
                              "services_failed=0 services_skipped=1 rows=0 ")
    assert out[-1].endswith(" slot=02 final=0")


def test_409_twice_final_slot_failure_exit1(capsys):
    f = fetcher_script({SERVICE: [nr(60), nr(60)]})
    code, w, out = run(capsys, [ENTRY], f, c=ctx(hour=9))
    assert code == 1 and f.calls == [SERVICE, SERVICE] and w.batches == []
    assert any(l.startswith("SERVICE_RESULT status=FAILURE") and l.endswith("reason=not_ready_at_0900") for l in out)
    assert out[-1].startswith("BATCH_RESULT status=FAILURE module=token-metrics services_ok=0 "
                              "services_failed=1 services_skipped=0 ")
    assert out[-1].endswith(" slot=09 final=1")


def test_409_revisit_at_queue_end(capsys):
    f = fetcher_script({SERVICE: [nr(60), None], SERVICE_B: [None]})
    sleeps = []
    code, w, _ = run(capsys, [ENTRY, ENTRY_B], f, sleeps=sleeps)
    assert code == 0 and f.calls == [SERVICE, SERVICE_B, SERVICE]       # A 는 큐 끝으로
    assert sleeps == [60]                                               # B 처리 후 A 의 resume 까지 대기
    assert w.batches == [(DATE, [SERVICE_B]), (DATE, [SERVICE])]        # 정기: 서비스별 순차 적재


def test_409_retry_after_zero_waits_at_least_1s(capsys):
    sleeps = []
    code, _, _ = run(capsys, [ENTRY], fetcher_script({SERVICE: [nr(0), None]}), sleeps=sleeps)
    assert code == 0 and sleeps == [1]


def test_409_in_rerun_is_failure_not_ready(capsys):
    code, _, out = run(capsys, [ENTRY], fetcher_script({SERVICE: [nr(60), nr(60)]}),
                       c=ctx(mode=MODE_RERUN, hour=9))
    assert code == 1
    assert any(l.startswith("SERVICE_RESULT status=FAILURE") and l.endswith("reason=not_ready") for l in out)
    assert out[-1].endswith(" slot=09 final=0")                         # rerun: final 판정 없음


# ---------- 404·4xx·503·구조 위반·예상 밖 예외 ----------

def test_retention_regular_failure_rerun_skipped(capsys):
    ret = {SERVICE: [CollectError(Event.RETENTION, "404")]}
    code, _, out = run(capsys, [ENTRY], fetcher_script(ret))
    assert code == 1 and any(l.startswith("SERVICE_RESULT status=FAILURE") and l.endswith("reason=retention")
                             for l in out)
    code, _, out = run(capsys, [ENTRY], fetcher_script(ret), c=ctx(mode=MODE_RERUN, replace=True))
    assert code == 0 and any(l.startswith("SERVICE_RESULT status=SKIPPED") and l.endswith("reason=retention")
                             for l in out)


def test_permanent_error_failure_isolated(capsys):
    f = fetcher_script({SERVICE: [CollectError(Event.PERMANENT_ERROR, "http 400")], SERVICE_B: [None]})
    code, w, out = run(capsys, [ENTRY, ENTRY_B], f)
    assert code == 1 and w.batches == [(DATE, [SERVICE_B])]
    assert ("SERVICE_RESULT status=FAILURE module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=permanent_error") in out
    assert any(l.startswith("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service B") for l in out)
    assert out[-1].startswith("BATCH_RESULT status=FAILURE ") and out[-1].endswith(" slot=02 final=0")


def test_retryable_exhausted_reason(capsys):
    code, _, out = run(capsys, [ENTRY], fetcher_script({SERVICE: [CollectError(Event.RETRYABLE, "503")]}))
    assert code == 1 and any(l.endswith("reason=retryable") for l in out)


def test_normalize_payload_error_is_permanent_error(capsys):
    def f(entry, d, cfg, session):
        p = check_report_structure(report(d=d, service=entry.service), d)
        p.gpu = {"not": "a list"}                                       # normalize 단계 구조 위반 유발
        return p
    code, w, out = run(capsys, [ENTRY], f)
    assert code == 1 and w.batches == [] and any(l.endswith("reason=permanent_error") for l in out)


def test_unexpected_exception_isolated(capsys):
    def f(entry, d, cfg, session):
        raise KeyError("boom")
    code, w, out = run(capsys, [ENTRY, ENTRY_B], f)
    assert code == 1 and w.batches == []
    assert sum(1 for l in out if l.endswith("reason=unexpected:KeyError")) == 2
    assert out[-1].startswith("BATCH_RESULT status=FAILURE ")


def test_writer_exception_isolated_as_unexpected(capsys):
    class Boom(FakeWriter):
        def replace_batch(self, date, items):
            raise RuntimeError("ch down")
    code, _, out = run(capsys, [ENTRY], fetcher_ok(), writer=Boom())
    assert code == 1 and any(l.endswith("reason=unexpected:RuntimeError") for l in out)


# ---------- 소프트 데드라인 · LOAD_BUDGET · 뮤테이션 예산 (§5.2 마지막 행, §5.4) ----------

def test_load_budget_reservation(capsys):
    clock = Clock()

    def f(entry, d, cfg, session):
        clock.advance(2400 - 1199)                                      # 잔여 1199s < LOAD_BUDGET_S 1200
        return check_report_structure(report(d=d, service=entry.service), d)
    code, w, out = run(capsys, [ENTRY], f, clock=clock, cfg=Config(soft_deadline_minutes=40, load_budget_s=1200))
    assert code == 1 and w.batches == []
    assert ("SERVICE_RESULT status=FAILURE module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=load_budget") in out


def test_load_budget_boundary_exactly_budget_loads(capsys):
    clock = Clock()

    def f(entry, d, cfg, session):
        clock.advance(2400 - 1200)                                      # 잔여 == 1200 → 착수
        return check_report_structure(report(d=d, service=entry.service), d)
    code, w, _ = run(capsys, [ENTRY], f, clock=clock, cfg=Config(soft_deadline_minutes=40, load_budget_s=1200))
    assert code == 0 and w.batches == [(DATE, [SERVICE])]


def test_deadline_remaining_queue_failure(capsys):
    clock = Clock()

    def f(entry, d, cfg, session):
        clock.advance(2500)                                             # 첫 fetch 중 2400s 경과
        return check_report_structure(report(d=d, service=entry.service), d)
    code, w, out = run(capsys, [ENTRY, ENTRY_B, ENTRY_C], f, clock=clock)
    assert code == 1 and w.batches == []
    assert any(l.startswith("SERVICE_RESULT status=FAILURE module=token-metrics service=Mock Service A")
               and l.endswith("reason=load_budget") for l in out)
    assert sum(1 for l in out if l.endswith("reason=deadline")) == 2   # B·C 는 fetch 없이 deadline
    assert out[-1].startswith("BATCH_RESULT status=FAILURE ")           # 정상 종료 + 마커 보장


def test_deadline_before_start_marks_all_failed_no_fetch(capsys):
    def f(entry, d, cfg, session):
        raise AssertionError("데드라인 소진 후에는 fetch 가 호출되면 안 됨")
    code, w, out = run(capsys, [ENTRY, ENTRY_B], f, cfg=Config(soft_deadline_minutes=10, load_budget_s=1200))
    assert code == 1 and w.batches == [] and sum(1 for l in out if l.endswith("reason=deadline")) == 2


def test_mutation_budget_failure_reason_promoted(capsys):
    code, w, out = run(capsys, [ENTRY, ENTRY_B], fetcher_ok(), c=ctx(mode=MODE_RERUN, replace=True),
                       writer=FakeWriter(raise_budget=True))
    assert code == 1 and w.batches == []
    assert sum(1 for l in out if l.startswith("SERVICE_RESULT status=FAILURE")
               and l.endswith("reason=mutation_budget")) == 2
    assert out[-1].endswith(" slot=02 final=0 reason=mutation_budget")


# ---------- 배칭 (§5.4 (A)(B)(C)) · 레지스트리 동기화 ----------

def test_rerun_batches_per_date_single_replace_batch(capsys):
    three = [ENTRY, ENTRY_B, ENTRY_C]
    _, w, out = run(capsys, three, fetcher_ok(), c=ctx(mode=MODE_RERUN, replace=True))
    assert w.batches == [(DATE, [SERVICE, SERVICE_B, SERVICE_C])]      # (A) 전부 fetch → (B)(C) 1회
    assert sum(1 for l in out if l.startswith("SERVICE_RESULT status=SUCCESS")) == 3
    _, w, _ = run(capsys, three, fetcher_ok())
    assert w.batches == [(DATE, [SERVICE]), (DATE, [SERVICE_B]), (DATE, [SERVICE_C])]   # 정기: 서비스별


def test_rerun_batch_excludes_skipped_and_failed(capsys):
    f = fetcher_script({SERVICE: [CollectError(Event.PERMANENT_ERROR, "400")]})
    code, w, _ = run(capsys, [ENTRY, ENTRY_B, ENTRY_C], f, c=ctx(mode=MODE_RERUN),
                     writer=FakeWriter(anchors={(DATE, SERVICE_C)}))
    assert code == 1 and w.batches == [(DATE, [SERVICE_B])]            # A=FAILURE, C=already_loaded(--replace 없음)


def test_registry_sync_regular_only_with_full_dim_entries(capsys):
    _, w, _ = run(capsys, [ENTRY], fetcher_ok(), dim_entries=[ENTRY, ENTRY_B])
    assert w.sync_calls == 1 and w.sync_entries == [SERVICE, SERVICE_B]   # --service 필터 전 전체
    _, w, _ = run(capsys, [ENTRY], fetcher_ok())
    assert w.sync_calls == 1 and w.sync_entries == [SERVICE]               # dim_entries 미지정 → entries
    _, w, _ = run(capsys, [ENTRY], fetcher_ok(), c=ctx(mode=MODE_RERUN, replace=True))
    assert w.sync_calls == 0
    _, w, _ = run(capsys, [ENTRY], fetcher_ok(), register_dims=False)
    assert w.sync_calls == 0


def test_registry_sync_failure_is_warn_not_fatal(capsys):
    code, w, out = run(capsys, [ENTRY], fetcher_ok(), writer=FakeWriter(raise_sync=True))
    assert code == 0 and w.batches == [(DATE, [SERVICE])]
    assert out[0] == "CHECK WARN service=- registry_sync_failed=1"
    assert any(l.startswith("SERVICE_RESULT status=SUCCESS") for l in out)


def test_batch_line_format_and_final_in_run(capsys):
    _, _, out = run(capsys, [ENTRY], fetcher_ok())
    assert out[-1] == ("BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 services_failed=0 "
                       "services_skipped=0 rows=5 elapsed=0s slot=02 final=0")
    assert _batch_status["line"] == out[-1]                             # SIGTERM 캐시 = 마지막 줄
    _, _, out = run(capsys, [ENTRY], fetcher_ok(), c=ctx(hour=9))
    assert out[-1].endswith(" rows=5 elapsed=0s slot=09 final=1")
    cfg8 = Config(final_hour_kst=8)
    _, _, out = run(capsys, [ENTRY], fetcher_ok(), c=ctx(hour=8, cfg=cfg8), cfg=cfg8)
    assert out[-1].endswith(" slot=08 final=1") and sum(1 for l in out if l.startswith("BATCH_RESULT")) == 1
```

(이어서 — 같은 파일 끝에 계속 append)

```python
# ---------- main(): CLI · 대상일 · 단일 BATCH_RESULT ----------

class _NoSignal:
    SIGTERM = 15

    @staticmethod
    def signal(signum, handler):
        return None


def _patch_main(monkeypatch, entries, writer, fetcher, cfg=None):
    monkeypatch.setattr("app.main.load_config", lambda: cfg or Config())
    monkeypatch.setattr("app.main.load_endpoints", lambda p: entries)
    monkeypatch.setattr("app.main.MetricsWriter", lambda c: writer)
    monkeypatch.setattr("app.main.api_client", type("M", (), {"fetch_metrics": staticmethod(fetcher)}))
    monkeypatch.setattr("app.main.signal", _NoSignal)


def test_main_unknown_service_exit_2(monkeypatch, capsys):
    _patch_main(monkeypatch, [ENTRY], FakeWriter(), fetcher_ok())
    assert main(["--service", "nope"]) == 2
    assert "unknown service: nope" in capsys.readouterr().err


def test_main_from_to_pair_required_and_ordered(monkeypatch, capsys):
    f = fetcher_ok()
    _patch_main(monkeypatch, [ENTRY], FakeWriter(), f)
    assert main(["--from", "2026-09-01"]) == 2
    assert main(["--to", "2026-09-01"]) == 2
    assert main(["--from", "2026-09-03", "--to", "2026-09-01"]) == 2
    err = capsys.readouterr().err
    assert "--from/--to must be given together" in err and "--from must not be after --to" in err
    assert f.calls == []


def test_main_replace_requires_range(monkeypatch, capsys):
    f = fetcher_ok()
    _patch_main(monkeypatch, [ENTRY], FakeWriter(), f)
    assert main(["--replace"]) == 2
    assert "--replace requires --from/--to" in capsys.readouterr().err and f.calls == []


def test_main_config_error_exit_2(monkeypatch, capsys):
    def bad(p):
        raise ValueError("endpoints file has no services")
    monkeypatch.setattr("app.main.load_config", lambda: Config())
    monkeypatch.setattr("app.main.load_endpoints", bad)
    monkeypatch.setattr("app.main.signal", _NoSignal)
    assert main([]) == 2
    assert "config error: ValueError: endpoints file has no services" in capsys.readouterr().err


def test_main_regular_target_is_yesterday(monkeypatch, capsys):
    f = fetcher_ok()
    w = FakeWriter()
    _patch_main(monkeypatch, [ENTRY, ENTRY_B], w, f)
    code = main(["2026-09-11T02:05:00+09:00", "--service", SERVICE])
    out = capsys.readouterr().out.rstrip("\n").splitlines()
    assert code == 0 and f.calls == [(SERVICE, "2026-09-10")]
    assert w.sync_entries == [SERVICE, SERVICE_B]                       # --service 필터 전 전체로 동기화
    assert w.batches == [("2026-09-10", [SERVICE])]
    assert out[-1].startswith("BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 "
                              "services_failed=0 services_skipped=0 rows=5 elapsed=")
    assert out[-1].endswith("s slot=02 final=0")


def test_main_final_slot_from_batch_time(monkeypatch, capsys):
    _patch_main(monkeypatch, [ENTRY], FakeWriter(), fetcher_ok())
    assert main(["2026-09-11T09:05:00+09:00"]) == 0
    assert capsys.readouterr().out.rstrip("\n").splitlines()[-1].endswith(" slot=09 final=1")


def test_main_emits_single_batch_line_for_range(monkeypatch, capsys):
    f = fetcher_ok()
    w = FakeWriter()
    _patch_main(monkeypatch, [ENTRY], w, f)
    code = main(["--from", "2026-09-01", "--to", "2026-09-03"])
    out = capsys.readouterr().out.rstrip("\n").splitlines()
    assert code == 0 and w.sync_calls == 0                              # rerun: 동기화 없음, api_since 무시
    assert [d for _, d in f.calls] == ["2026-09-01", "2026-09-02", "2026-09-03"]
    assert sum(1 for l in out if l.startswith("BATCH_RESULT")) == 1
    assert sum(1 for l in out if l.startswith("SERVICE_RESULT status=SUCCESS")) == 3
    assert out[-1].startswith("BATCH_RESULT status=SUCCESS module=token-metrics services_ok=3 "
                              "services_failed=0 services_skipped=0 rows=15 ")
    assert out[-1].endswith(" final=0")
    assert w.batches == [("2026-09-01", [SERVICE]), ("2026-09-02", [SERVICE]), ("2026-09-03", [SERVICE])]


def test_main_range_mutation_budget_reason_in_aggregate_line(monkeypatch, capsys):
    _patch_main(monkeypatch, [ENTRY], FakeWriter(raise_budget=True), fetcher_ok())
    code = main(["--from", "2026-09-01", "--to", "2026-09-02", "--replace"])
    out = capsys.readouterr().out.rstrip("\n").splitlines()
    assert code == 1 and out[-1].startswith("BATCH_RESULT status=FAILURE module=token-metrics services_ok=0 "
                                            "services_failed=2 services_skipped=0 rows=0 ")
    assert out[-1].endswith(" final=0 reason=mutation_budget")
    assert _batch_status["line"] == out[-1]
```

- [ ] **Step 7: 실패 확인 2 (Step 6 청크는 import 단계에서 실패)**

Run:
```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -m pytest tests/test_main.py -q 2>&1 | tail -5
```
Expected (수집 오류 — 파일 전체가 1 error):
```
E   ImportError: cannot import name '_run_dates' from 'app.main' (.../collectors/token-metrics/app/main.py)
...
!!!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
1 error in 0.XXs
```

- [ ] **Step 8: 구현 2 — `_prepare_one`·`_load_items`·`run_collection`·`_run_dates`·`main`** — `collectors/token-metrics/app/main.py` **끝에 추가**(`_target_dates` 다음)

```python
def _prepare_one(cfg: Config, entry: ServiceEntry, target_date: str, ctx: RunContext, writer, fetcher, session):
    """게이트 → 앵커(already_loaded·manual_row_present) → fetch → normalize. 반환 = ServiceOutcome(SKIPPED) 또는
    적재 대기 튜플 (entry, payload, result). CollectError 는 호출자(run_collection)가 §5.2 표로 판정."""
    gate = _gate(entry, target_date, ctx)
    if gate is not None:
        return ServiceOutcome(service=entry.service, status="SKIPPED", source_type=ctx.source_type, reason=gate)
    if not ctx.replace and writer.anchor_exists(target_date, entry.service):
        o = ServiceOutcome(service=entry.service, status="SKIPPED", source_type=ctx.source_type,
                           reason="already_loaded")
        if ctx.mode == MODE_REGULAR and writer.anchor_source_type(target_date, entry.service) == SOURCE_MANUAL:
            o.checks["manual_row_present"] = 1      # 정기 실행이 수동 앵커를 만남 — 덮어쓰지 않고 WARN 만 (§5.2)
        return o
    payload: MetricsPayload = fetcher(entry, target_date, cfg, session)
    try:
        result: NormalizeResult = normalize_payload(payload, entry)
    except PayloadError as exc:                     # 구조 위반 = 4xx 와 같은 급 (재시도 무의미)
        raise CollectError(Event.PERMANENT_ERROR, f"report structure: {exc}") from exc
    return entry, payload, result


def _load_items(cfg: Config, target_date: str, items: list, writer, clock, deadline: float | None,
                ctx: RunContext) -> list[ServiceOutcome]:
    """§5.2 마지막 행 + §5.4: 적재 착수 전 잔여 시간 < LOAD_BUDGET_S 면 착수하지 않고 FAILURE load_budget.
    replace_batch 1회(정기 = item 1개, rerun/manual = 날짜당 N개) → SUCCESS/NODATA. MutationBudgetExceeded → mutation_budget."""
    st = ctx.source_type
    if deadline is not None and deadline - clock() < cfg.load_budget_s:   # T7 manual 은 None (데드라인 없음)
        return [ServiceOutcome(service=e.service, source_type=st, reason=REASON_LOAD_BUDGET) for e, _, _ in items]
    try:
        writer.replace_batch(target_date, items)
    except MutationBudgetExceeded:
        return [ServiceOutcome(service=e.service, source_type=st, reason=REASON_MUTATION_BUDGET)
                for e, _, _ in items]
    outs: list[ServiceOutcome] = []
    for e, _, r in items:
        o = ServiceOutcome(service=e.service, status="NODATA" if r.is_nodata else "SUCCESS", source_type=st,
                           rows=r.rows, warn=r.warn_total, rejected=r.rejected, checks=dict(r.warns))
        if o.rows == 0 and o.rejected > 0:
            o.checks["all_rows_rejected"] = 1       # 앵커는 적재됐지만 행이 전부 거부됨 — 마트 단위 WARN
        outs.append(o)
    return outs


def run_collection(cfg: Config, entries: list[ServiceEntry], target_date: str, ctx: RunContext, *,
                   clock=time.monotonic, sleeper=time.sleep, fetcher=api_client.fetch_metrics,
                   writer=None, session=None, register_dims: bool = True, dim_entries=None,
                   emit_batch: bool = True, outcomes_sink=None, started: float | None = None) -> int:
    """날짜 1개 수집. 반환 = exit code(0 | 1 = FAILURE 1개 이상).
    큐 루프: 데드라인/적재 예산 검사 → 준비된 항목(resume_at ≤ now) → _prepare_one → 409 첫 방문은 큐 끝 재삽입.
    정기 = 서비스별 즉시 적재, rerun/manual = 큐 소진 후 날짜당 replace_batch 1회 (§5.4 (A)→(B)(C)).
    outcomes_sink 가 주어지면 그 누적 목록으로 SIGTERM 캐시 줄을 갱신하고(_run_dates 집계), emit_batch=False 면
    BATCH_RESULT 를 출력하지 않는다(집계 줄은 _run_dates 가 1회 출력)."""
    started = clock() if started is None else started
    deadline = started + cfg.soft_deadline_minutes * 60
    writer = writer if writer is not None else MetricsWriter(cfg)
    session = session if session is not None else _session(cfg)
    if register_dims and ctx.mode == MODE_REGULAR:
        try:
            writer.sync_registry(dim_entries if dim_entries is not None else entries)
        except Exception:                           # 레지스트리 동기화 실패는 수집을 막지 않는다 — WARN 마커만
            print("CHECK WARN service=- registry_sync_failed=1", flush=True)

    queue = [_QueueItem(entry=e) for e in entries]
    outcomes: list[ServiceOutcome] = []
    pending: list = []                              # rerun/manual: 적재 대기 (entry, payload, result)
    scope = outcomes_sink if outcomes_sink is not None else outcomes

    def _record(o: ServiceOutcome) -> None:
        outcomes.append(o)
        if outcomes_sink is not None:
            outcomes_sink.append(o)
        for line in _check_lines(o.service, o.checks):
            print(line, flush=True)
        print(_service_line(o), flush=True)
        _batch_status["line"] = _batch_line(scope, started, clock, ctx, reason=_batch_reason(scope))

    def _load(items: list) -> None:
        try:
            for o in _load_items(cfg, target_date, items, writer, clock, deadline, ctx):
                _record(o)
        except Exception as exc:                    # CH 연결 오류 등 — 항목 단위 격리, 마커 보장
            for e, _, _ in items:
                _record(ServiceOutcome(service=e.service, source_type=ctx.source_type,
                                       reason=f"unexpected:{type(exc).__name__}"))

    while queue:
        now = clock()
        if deadline - now < cfg.load_budget_s:      # 신규 착수 창 종료 — 남은 큐는 fetch 없이 FAILURE deadline
            for item in queue:
                _record(ServiceOutcome(service=item.entry.service, source_type=ctx.source_type,
                                       reason=REASON_DEADLINE))
            queue.clear()
            break
        ready = [q for q in queue if q.resume_at <= now]
        if not ready:
            wake = min(q.resume_at for q in queue)
            sleeper(min(wake - now, deadline - now))
            continue
        item = ready[0]
        queue.remove(item)
        try:
            prepared = _prepare_one(cfg, item.entry, target_date, ctx, writer, fetcher, session)
        except CollectError as err:
            o = _outcome_from_error(item.entry, err, ctx, item.revisited)
            if o is None:                           # 409 첫 방문 → 큐 끝 재삽입 1회, 대기 = min(max(Retry-After,1),300)
                item.resume_at = clock() + min(max(err.retry_after_s, 1), NOT_READY_REVISIT_CAP_S)
                item.revisited = True
                queue.append(item)
            else:
                _record(o)
            continue
        except Exception as exc:                    # 예상 밖 예외 — 서비스 단위 격리
            _record(ServiceOutcome(service=item.entry.service, source_type=ctx.source_type,
                                   reason=f"unexpected:{type(exc).__name__}"))
            continue
        if isinstance(prepared, ServiceOutcome):
            _record(prepared)
        elif ctx.mode == MODE_REGULAR:
            _load([prepared])
        else:
            pending.append(prepared)

    if pending:
        _load(pending)

    if emit_batch:
        line = _batch_line(outcomes, started, clock, ctx, reason=_batch_reason(outcomes))
        _batch_status["line"] = line
        print(line, flush=True)
    return 1 if any(o.status == "FAILURE" for o in outcomes) else 0


def _run_dates(cfg: Config, entries: list[ServiceEntry], dim_entries: list[ServiceEntry], dates: list[str],
               ctx: RunContext, fetcher, *, writer=None, clock=time.monotonic, sleeper=time.sleep,
               started: float | None = None, register_dims: bool = True) -> int:
    """날짜 N개(정기 = 1개) → BATCH_RESULT 1줄. writer 1개 공유(뮤테이션 장부 누적), started 1개(소프트 데드라인은
    실행 전체), 레지스트리 동기화는 첫 날짜에서만. 반환 = 날짜별 exit code 의 최댓값."""
    started = clock() if started is None else started
    writer = writer if writer is not None else MetricsWriter(cfg)
    all_outcomes: list[ServiceOutcome] = []
    worst = 0
    for i, d in enumerate(dates):
        code = run_collection(cfg, entries, d, ctx, clock=clock, sleeper=sleeper, fetcher=fetcher,
                              writer=writer, register_dims=(register_dims and i == 0), dim_entries=dim_entries,
                              emit_batch=False, outcomes_sink=all_outcomes, started=started)
        worst = max(worst, code)
    line = _batch_line(all_outcomes, started, clock, ctx, reason=_batch_reason(all_outcomes))
    _batch_status["line"] = line
    print(line, flush=True)
    return worst


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="token-metrics-collector")
    parser.add_argument("batch_time", nargs="?", default=None,
                        help="ISO8601 (기본 now, KST) — 정기 target_date = batch_time − 1일, slot=HH·final 판정 기준")
    parser.add_argument("--from", dest="from_date", default=None, help="rerun 시작일 (KST, YYYY-MM-DD) — --to 와 쌍")
    parser.add_argument("--to", dest="to_date", default=None, help="rerun 종료일 (포함)")
    parser.add_argument("--service", default=None, help="단일 서비스만 (레지스트리 동기화는 전체 기준)")
    parser.add_argument("--replace", action="store_true",
                        help="앵커가 있어도 교체 (rerun 전용 — 정기 실행은 뮤테이션 0 보장)")
    args = parser.parse_args(argv)

    signal.signal(signal.SIGTERM, _sigterm_handler)
    try:
        cfg = load_config()
        all_entries = load_endpoints(cfg.endpoints_file)
    except Exception as exc:                        # env 불변식·파일 부재·YAML/스키마 오류 — 적재 전 종료
        print(f"config error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    entries = all_entries
    if args.service:
        entries = [e for e in all_entries if e.service == args.service]
        if not entries:
            print(f"unknown service: {args.service}", file=sys.stderr)
            return 2
    try:
        batch_time = _parse_batch_time(args.batch_time)
        dates, mode = _target_dates(args, batch_time)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.replace and mode != MODE_RERUN:
        print("--replace requires --from/--to (regular run keeps mutations at 0)", file=sys.stderr)
        return 2
    ctx = make_context(cfg, mode, batch_time, replace=args.replace)
    return _run_dates(cfg, entries, all_entries, dates, ctx, api_client.fetch_metrics)


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 9: 통과 확인(구현 2) + 회귀·격리·금지 문자열·제로 diff**

Run: `cd collectors/token-metrics && python3 -m pytest -q tests/test_main.py 2>&1 | tail -1`
Expected: `51 passed`(Step 2 13개 + Step 6 38개)

Run: `cd collectors/token-metrics && python3 -m pytest -q 2>&1 | tail -1`
Expected: `155 passed`(Step 1의 `104` + 51 — Step 1에서 확인한 실제 수가 다르면 그 수 + 51)

Run: `cd collectors/token-metrics && grep -n "^from \|^import " app/main.py`
Expected(표준 라이브러리 + `requests` + `app.*` 만 — 기존 모듈·`vm_push`·`clickhouse_connect` 직접 import 없음):
```text
...:from __future__ import annotations
...:import argparse
...:import signal
...:import sys
...:import time
...:from dataclasses import dataclass, field
...:from datetime import date as date_cls, datetime, timedelta, timezone
...:import requests
...:from app import api_client
...:from app.config import Config, ServiceEntry, load_config, load_endpoints
...:from app.events import CollectError, Event
...:from app.normalize import (SOURCE_API, SOURCE_MANUAL, MetricsPayload, NormalizeResult, PayloadError,
...:from app.writer import MetricsWriter, MutationBudgetExceeded
```

Run: `cd collectors/token-metrics && grep -n -E "token_usage|token-usage|vm_push|push_vm|import random|max_pages|not_ready_budget|INVARIANT_RESTARTS|replace_service_day|replace_dim_services" app/main.py tests/test_main.py | grep -v "^app/main.py:1:\|클론"`
Expected: 출력 없음(모듈 docstring의 클론 출처 문구 1줄만 제외 — 기존 모듈 심볼·VM push·`random` 사용 0)

Run: `cd collectors/token-metrics && grep -n "print(" app/main.py | grep -v "flush=True\|file=sys.stderr"`
Expected: 출력 없음(stdout 마커는 전부 `flush=True` — SIGTERM 직전 줄 손실 방지; 진단은 stderr)

Run: `cd collectors/token-metrics && python3 -m py_compile app/main.py tests/test_main.py && echo OK && python3 -m app.main --help | head -3`
Expected: `OK` 다음에 usage 3줄 — 첫 줄 `usage: token-metrics-collector [-h] [--from FROM_DATE] [--to TO_DATE]`, 이어서 `[--service SERVICE] [--replace]` 와 `[batch_time]`(80열 줄바꿈; `--help`는 `signal`·`load_config` 이전에 종료 — env 불필요)

Run: `git diff --stat -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations/company-verify.md docs/operations/stage-runbook.md docs/operations/rerun.md docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml && git status --short collectors/token-metrics`
Expected(제로 diff 목록 §7.5 — diff 없음, 신규 2파일만):
```text
?? collectors/token-metrics/app/main.py
?? collectors/token-metrics/tests/test_main.py
```

- [ ] **Step 10: 커밋**

```bash
cd /home/mini/github/token-data-pipeline
git add collectors/token-metrics/app/main.py collectors/token-metrics/tests/test_main.py
git commit -m "feat(collectors-metrics): main — 모드×게이트·409 재방문 1회·최종 슬롯·소프트 데드라인·마커 slot/final·SIGTERM (Plan 6b T6)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

**설계 해석(이 태스크에서 결정한 것 — 설계 §5.2/§5.4/§5.6 문면에 없는 부분):**
- (a) `RunContext`는 `slot`/`final`에 기본값을 두고(`__post_init__`이 `slot`을 `batch_time`에서 채움) `make_context(cfg, mode, batch_time, replace, source_type)`가 `final = mode == regular and batch_time.hour >= cfg.final_hour_kst`를 계산한다. T7(manual)은 `make_context(cfg, MODE_MANUAL, now, replace=..., source_type=SOURCE_MANUAL)` 또는 `RunContext(...)` 직접 생성 어느 쪽이든 된다 — manual은 final=0 고정.
- (b) `_load_items(cfg, target_date, items, writer, clock, deadline, ctx)`가 `ctx`를 받는 이유: 모든 `ServiceOutcome.source_type`은 `ctx.source_type`에서 복사한다(정기·rerun = `metrics-api-v1`, manual = `manual-v0`). T7이 `run_collection(..., ctx=RunContext(mode=MODE_MANUAL, source_type=SOURCE_MANUAL))`와 CSV용 `fetcher`만 바꿔 같은 루프를 쓴다.
- (c) `normalize_payload`의 `PayloadError`(`gpu_not_array`/`serving_not_array`)는 `_prepare_one`이 `CollectError(Event.PERMANENT_ERROR, f"report structure: {e}")`로 번역 → `FAILURE reason=permanent_error`(T4가 `check_report_structure` 단계에서 같은 번역을 쓰므로 reason 어휘가 하나로 유지된다).
- (d) `SERVICE_RESULT warn=`은 `NormalizeResult.warn_total`(행 플래그 + 응답 WARN)만 센다. `all_rows_rejected`·`manual_row_present`·`registry_sync_failed`는 CHECK WARN 줄로만 나가고 `warn=`에 더하지 않는다(outline T6 예시 `warn=0 rejected=1`과 일치). CHECK WARN은 `service=<svc> <code>=<count>` 1코드 1줄, 코드 정렬, 항상 해당 SERVICE_RESULT 앞. 레지스트리 동기화 실패는 서비스가 없으므로 `service=-`.
- (e) SIGTERM 캐시 줄(`_batch_status["line"]`)은 `outcomes_sink`가 주어지면 누적 목록(rerun N일 집계) 기준으로 갱신한다 — `_run_dates` 실행 중 SIGTERM이 오면 그때까지의 전체 집계 + ` note=sigterm`이 나간다.
- (f) `--replace`는 `--from/--to` 없이 쓰면 exit 2(정기 실행은 뮤테이션 0 보장 — §5.2 "정기 실행은 덮어쓰기 없음"을 CLI 단계에서 고정). `--replace` 없는 rerun은 앵커 존재 = `SKIPPED already_loaded`(안전 기본값).
- (g) `load_config()`/`load_endpoints()`의 예외(env 불변식 `ValueError`, 파일 부재 `FileNotFoundError`, YAML/스키마 오류)는 `config error: <Type>: <msg>`를 stderr에 쓰고 exit 2 — BATCH_RESULT 없이 끝나며 CronJob `backoffLimit: 0`이라 그 슬롯은 알림 규칙(마커 부재)으로 잡힌다. `--service` 미등록도 exit 2.
- (h) `_run_dates`는 writer 1개(뮤테이션 장부가 날짜를 넘어 누적 — `METRICS_MAX_MUTATIONS_PER_RUN`은 실행 단위)·`started` 1개(소프트 데드라인은 실행 전체)를 공유하고 레지스트리 동기화는 첫 날짜에서만(정기 모드 한정 — rerun/manual은 호출 자체가 없음) 한다. BATCH_RESULT는 날짜 수와 무관하게 1줄.
- (i) 적재 중 `MutationBudgetExceeded` 이외의 예외(CH 연결 오류 등)와 fetch/normalize 중 `CollectError` 이외의 예외는 항목 단위로 `FAILURE reason=unexpected:<ExceptionType>`로 격리한다 — 어떤 경우에도 BATCH_RESULT는 나간다(마커 보장).
- (j) 409 대기 = `min(max(retry_after_s, 1), NOT_READY_REVISIT_CAP_S=300)`초. `Retry-After` 부재(0) 도 1초는 기다려 같은 초 재호출을 막는다. 큐 끝 재방문은 `_QueueItem.revisited` 1비트 — 재차 409는 정기·비최종 `SKIPPED not_ready` / 정기·최종 `FAILURE not_ready_at_0900` / rerun·manual `FAILURE not_ready`.
- (k) 데드라인 루프 검사는 `deadline − now < LOAD_BUDGET_S`(기존 `main.py:177`의 `now >= deadline or …`와 동치) — 신규 착수·409 재방문 창은 `SOFT×60 − LOAD_BUDGET = 1200s`, 그 뒤 남은 큐는 fetch 없이 `FAILURE reason=deadline`. 적재 직전 검사(`_load_items`)는 `main.py:110`과 같은 산식으로 `FAILURE reason=load_budget`.
- (l) `Config(soft_deadline_minutes=…, load_budget_s=…)` 직접 생성은 불변식을 검사하지 않는다(T2: 불변식은 `load_config()`에서만) — 테스트가 `Config(soft_deadline_minutes=10, load_budget_s=1200)`로 "시작 전 데드라인" 경로를 만드는 근거.

---

### Task 7: app/manual.py — manual-v0 CSV 파서 → MetricsPayload · app/main.py manual 모드(--manual-gpu/--manual-serving/--manual-engine/--generated-at) · MANUAL_INPUT 마커

**Files:**
- Create: `collectors/token-metrics/app/manual.py`
- Create: `collectors/token-metrics/tests/test_manual.py`
- Modify: `collectors/token-metrics/app/main.py` (T6 산출 — import 1줄·상수 1줄·`_add_manual_args()`·`_manual_args_error()`·`_run_manual()` 함수 추가, `main()`에 3줄 삽입(argparse 등록 1줄·검증 4줄·분기 2줄); grep 앵커 기준 — 아래 Step 8)
- Modify: `collectors/token-metrics/tests/test_main.py` (T6 산출 — 파일 끝에 manual 모드 테스트 8개 append)
- Test fixture(읽기 전용, Plan 6a T10 산출·무수정): `docs/templates/token_metrics_manual_v0_gpu.csv`, `docs/templates/token_metrics_manual_v0_serving.csv`, `docs/templates/token_metrics_manual_v0_engine.csv`

**설계 근거:** §5.5(276-280) — CLI `python -m app.main --manual-gpu /manual/gpu.csv --manual-serving /manual/serving.csv [--manual-engine /manual/engine.csv] --from D0 --to D1 [--service S] [--replace] [--generated-at ISO]`; 템플릿 3파일 헤더(gpu `date,service,model,gpuType,category,gpuCount,gpuHours` / serving `date,service,model,metric,name,unit,p50,p90,p95,p99` / engine `service,engine_type,engine_version`); "(date, service)별 `MetricsPayload`로 묶어 **동일 normalize + replace 경로**(§5.4; 가드·배칭 포함), `source_type='manual-v0'`, `reported_*` = 레지스트리 값, `generated_at` = `--generated-at` 또는 적재 시각. 서비스는 endpoints에 등록(`enabled=1`)돼 있어야 함(api_since 무시); **날짜 제약 없음**; 기존 앵커(API·manual 불문)가 있으면 `--replace` 없이는 `already_loaded` 스킵. 레지스트리 동기화는 하지 않음" · §5.2 표 manual 행(앵커 존재 & manual without `--replace` → `SKIPPED reason=already_loaded`; `enabled=0`은 모든 모드 `SKIPPED reason=disabled`) · §5.3 "숫자 판정은 bool 제외"·행 단위 거부는 normalize 한 곳(파서는 형태만 만든다) · Plan 6a F(digest D1-F): "`#`로 시작하는 줄은 건너뛴다(주석 안의 쉼표 무시), 첫 비주석 줄이 헤더(바이트 동일 요구), 빈 셀 = 부재, 인코딩 UTF-8, 날짜 `YYYY-MM-DD`(KST)"; serving `metric`은 API 키(`ttftMs|itlMs|outputTps|e2eMs|custom`) — fact `metric` 변환은 normalize · digest D3 템플릿 3파일 원문(예시 행 = Mock Service A/B 합성값) · §3 전제 11(로그에 페이로드·행 원문 금지 → 파서 오류 메시지는 `경로:줄번호: 필드명`만).

**Interfaces:**
- Consumes:
  - T2 `app.config.ServiceEntry`(frozen dataclass: `service_group`, `service`, `enabled`, `api_since`, `coverage_since`, `until`, …), `Config`(테스트 fixture `Config()` 기본값).
  - T3 `app.normalize.MetricsPayload(date, reported_service_group, reported_service, generated_at_raw, engine, gpu, serving, source_type, extra_top_keys=[])`, `SOURCE_MANUAL = "manual-v0"`, `PCT_KEYS = ("p50", "p90", "p95", "p99")`, `LATENCY_KEYS`(키 `ttftMs/itlMs/e2eMs`), `normalize_payload(payload, entry, now=None) -> NormalizeResult`(`rows`, `rejected`, `warns`, `engine_type`, `engine_version`), `KST`.
  - T6 `app.main`: `MODE_MANUAL = "manual"`, `make_context(cfg, mode, batch_time, replace=False, source_type=SOURCE_API) -> RunContext`, `_run_dates(cfg, entries, dim_entries, dates, ctx, fetcher, *, writer=None, clock=time.monotonic, sleeper=time.sleep, started=None, register_dims=True) -> int`(writer 1개 공유·날짜별 `run_collection`·실행당 BATCH_RESULT 1줄·`mutation_budget` reason 승격), `main(argv=None)`의 argparse(`--from` dest `from_date`, `--to` dest `to_date`, `--service`, `--replace`), `MetricsWriter` 이름(테스트가 `app.main.MetricsWriter`를 monkeypatch), `_batch_status`, `KST`. T6 `tests/test_main.py`의 `FakeWriter`(`anchors: set[tuple[str, str]]`, `batches: list[tuple[str, list[str]]]`, `sync_calls: int`; `FakeWriter()` 기본 생성).
  - 템플릿 3파일(Plan 6a T10): 주석 `#` 줄 + 헤더 + 합성 예시 행(gpu 4행·serving 5행·engine 2행, 전부 `2026-08-26`, 서비스 `Mock Service A`/`Mock Service B`, 그룹 `Mock Group`).
- Produces (T10 `manual_load.py`의 Job command·T11 E2E manual 1회·T12 README manual 절이 소비):
  - `app/manual.py` 상수: `GPU_HEADER = "date,service,model,gpuType,category,gpuCount,gpuHours"`, `SERVING_HEADER = "date,service,model,metric,name,unit,p50,p90,p95,p99"`, `ENGINE_HEADER = "service,engine_type,engine_version"`, `SERVING_METRICS = ("ttftMs", "itlMs", "e2eMs", "outputTps", "custom")`, `STANDARD_METRICS = ("ttftMs", "itlMs", "e2eMs", "outputTps")`, `COUNT_KEYS = ("rows_gpu", "rows_serving", "rows_engine", "rows_outside_range", "rows_other_service")`.
  - `class ManualCsvError(ValueError)`: `__init__(self, path: str, lineno: int, what: str)`; `str()` = `f"{path}:{lineno}: {what}"`(행 원문·서비스명·모델명 미포함 — 필드명·줄 번호만); 속성 `path/lineno/what`.
  - `def date_range(from_date: str, to_date: str) -> list[str]`: `YYYY-MM-DD` 포함 범위 목록; 형식 오류는 `date.fromisoformat`의 `ValueError`, `to < from` → `ValueError("--from must not be after --to")`.
  - `def read_csv_rows(path: str, expected_header: str) -> list[tuple[int, dict[str, str]]]`: `open(path, encoding="utf-8-sig", newline="")`; `#`로 시작하는 줄(좌측 공백 무시)·빈 줄 건너뜀; 첫 비주석 줄 `rstrip("\r\n") != expected_header` → `ManualCsvError(path, lineno, "header mismatch")`; 이후 줄은 `csv.reader` 1줄 파싱, 셀 수 ≠ 헤더 컬럼 수 → `ManualCsvError(path, lineno, "column count")`; 반환 `(lineno, {col: cell.strip()})`(줄 번호는 파일 물리 줄 1-base). 헤더가 끝내 없으면 `ManualCsvError(path, 0, "header missing")`.
  - `def _num(cell: str) -> object`: `""` → `None`, `float(cell)` 성공 → `float`, 실패 → 원문 `str`(normalize `_is_num`이 비숫자로 거부).
  - `def load_manual_csvs(gpu_path: str, serving_path: str, engine_path: str | None, from_date: str, to_date: str, entries: list[ServiceEntry], only_service: str | None, generated_at_raw: str) -> tuple[dict[tuple[str, str], MetricsPayload], dict[str, int]]`:
    - 등록 대조: `registry = {e.service: e for e in entries}`; CSV `service ∉ registry` → `ManualCsvError(path, lineno, "unknown service (not in endpoints)")`(필터·범위와 무관하게 항상 검사); `date` 파싱 실패 또는 `str(date.fromisoformat(v)) != v` → `ManualCsvError(path, lineno, "bad date")`; serving `metric ∉ SERVING_METRICS` → `ManualCsvError(path, lineno, "bad metric")`(이 3종은 무시 행에도 적용 — 파일 전체가 깨끗해야 적재).
    - 필터(검증 통과 행에만, 순서대로): `only_service`가 있고 `service != only_service` → 무시 + `rows_other_service += 1`; `not (from_date <= date <= to_date)` → 무시 + `rows_outside_range += 1`.
    - gpu 행 → `{"model", "gpuType", "category", "gpuCount": _num, "gpuHours": _num}`(빈 model/gpuType/category는 `""` 그대로 → normalize 거부); `rows_gpu` = 채택 행 수.
    - serving 행 → `(date, service)`별 `{model: record}`(첫 등장 순), `record = {"model": model}`; `STANDARD_METRICS`: 이미 키 존재 → `ManualCsvError(path, lineno, "duplicate (model, metric)")`, 값 `{p: _num(cell) for p in PCT_KEYS if cell != ""}`(빈 p는 키 부재 → ttftMs 등 4키 미달은 normalize가 거부); `custom`: `record.setdefault("custom", []).append({"name": name, "unit": unit, **비어있지 않은 p})`(빈 name/unit은 `""` → normalize 거부); 표준 지표 행의 `name`/`unit` 셀은 무시; `rows_serving` = 채택 행 수.
    - engine 파일(선택): 행마다 `service ∉ registry` → `ManualCsvError`, 중복 → `ManualCsvError(path, lineno, "duplicate service")`, `engine_map[service] = {"type": engine_type, "version": engine_version}`(빈 version → `""`); `rows_engine` = 파일 행 수 전부(날짜·`--service` 필터 없음 — 서비스 단위 자기신고).
    - 대상 집합: `only_service`가 레지스트리 밖이면 `ValueError(f"unknown service: {only_service}")`(필터 전 검사); `dates = date_range(from_date, to_date)`(역순이면 `ValueError`); 페이로드는 **채택된 gpu∪serving 행이 1건 이상인 `(date, service)`에만** `MetricsPayload(date=d, reported_service_group=entry.service_group, reported_service=entry.service, generated_at_raw=generated_at_raw, engine=engine_map.get(service), gpu=[…], serving=[…], source_type=SOURCE_MANUAL)` — 키 순서 = `sorted(set(gpu_by_key) | set(serving_by_key))`; 행 없는 `(date, service)`는 키 자체가 없다(페이로드·앵커 없음 → 6c `metrics_missing`). 반환 `(payloads, counts)`, `counts` 키 = `COUNT_KEYS` 전부(0 포함).
  - `app/main.py` 추가분:
    - `def _add_manual_args(parser: argparse.ArgumentParser) -> None`: `--manual-gpu`(dest `manual_gpu`), `--manual-serving`(dest `manual_serving`), `--manual-engine`(dest `manual_engine`), `--generated-at`(dest `generated_at`) — 전부 `default=None`; `main()`이 `--replace` 등록 직후 호출. `def _manual_args_error(args) -> str`: 오류 메시지 또는 `""`; `main()`이 `parse_args` 직후 호출해 비어 있지 않으면 stderr + return 2(설정 로드 전): `--manual-gpu`/`--manual-serving` 중 하나만 → `"--manual-gpu/--manual-serving must be given together"`; 쌍이 있는데 `--from/--to` 미지정 → `"manual mode requires --from/--to (KST, YYYY-MM-DD)"`; `--manual-engine`/`--generated-at`이 쌍 없이 단독 → `"--manual-engine/--generated-at require --manual-gpu/--manual-serving"`.
    - `MANUAL_INPUT_PREFIX = "MANUAL_INPUT module=token-metrics"`; 정보 마커 1줄(페이로드 없음·카운트만) `MANUAL_INPUT module=token-metrics rows_gpu=<n> rows_serving=<n> rows_engine=<n> rows_outside_range=<n> rows_other_service=<n>` — SERVICE_RESULT 줄들보다 앞, 실행당 1줄.
    - `def _run_manual(cfg: Config, args, entries: list[ServiceEntry], all_entries: list[ServiceEntry], started: float, clock=time.monotonic) -> int`: `dates = date_range(...)` + `load_manual_csvs(args.manual_gpu, args.manual_serving, args.manual_engine, args.from_date, args.to_date, all_entries, args.service, args.generated_at or "")`(`ManualCsvError`/`ValueError` → stderr `manual input error: <msg>` + return 2, 적재 없음) → MANUAL_INPUT 1줄 → `ctx = make_context(cfg, MODE_MANUAL, datetime.now(KST), replace=args.replace, source_type=SOURCE_MANUAL)`(final=False, slot=현재 KST 시) → `writer = MetricsWriter(cfg)` 1개·`all_outcomes` 1개 공유, 날짜 `d`마다 `targets = [e for e in entries if (d, e.service) in payloads]`(`--service` 필터 후 `entries` 기준; 비어 있으면 그 날짜는 건너뜀 — fetch·앵커 없음) → `run_collection(cfg, targets, d, ctx, clock=clock, fetcher=fetcher, writer=writer, register_dims=False, emit_batch=False, outcomes_sink=all_outcomes, started=started)` → 마지막에 `_batch_line(all_outcomes, started, clock, ctx, reason=_batch_reason(all_outcomes))` 1줄(`_batch_status["line"]` 갱신) → `return worst`; `fetcher = lambda entry, d, _cfg, _session: payloads[(d, entry.service)]`. `_run_dates`를 쓰지 않는 이유: 날짜마다 대상 서비스 집합이 다르다. 레지스트리 동기화 0, api_since/until 게이트 없음(`MODE_MANUAL`), 앵커 있으면 `--replace` 없이는 `SKIPPED reason=already_loaded`(T6 `_prepare_one`), `--generated-at`의 오프셋 ≠ +09:00 WARN·파싱 실패 WARN은 T3 `parse_generated_at`이 normalize 단계에서 낸다.
    - `main()`: `--service` 필터 직후 `if args.manual_gpu: return _run_manual(cfg, args, entries, all_entries, started=time.monotonic())`(정기·rerun 경로 무변경).

- [ ] **Step 1: 전제 확인 — 템플릿 3파일(Plan 6a T10)·T3·T6 산출물**

템플릿 3파일이 없으면 Plan 6a가 병합되지 않은 것 — **중단·보고**(대신 만들지 않는다). `app/main.py`에 `make_context`·`_run_dates`가 없으면 T6 미완 — 중단·보고.

Run:
```bash
cd /home/mini/github/token-data-pipeline
ls docs/templates/token_metrics_manual_v0_gpu.csv docs/templates/token_metrics_manual_v0_serving.csv docs/templates/token_metrics_manual_v0_engine.csv
grep -v '^#' docs/templates/token_metrics_manual_v0_gpu.csv | head -1
grep -v '^#' docs/templates/token_metrics_manual_v0_serving.csv | head -1
grep -v '^#' docs/templates/token_metrics_manual_v0_engine.csv | head -1
grep -c "Mock Service" docs/templates/token_metrics_manual_v0_gpu.csv docs/templates/token_metrics_manual_v0_serving.csv docs/templates/token_metrics_manual_v0_engine.csv
cd collectors/token-metrics
grep -n "^SOURCE_MANUAL\|^PCT_KEYS\|^LATENCY_KEYS\|^class MetricsPayload\|^def normalize_payload" app/normalize.py
grep -n "^MODE_MANUAL\|^def make_context\|^def _run_dates\|^def main\|^from app.writer import\|add_argument(\"--replace\"" app/main.py
grep -n "^class FakeWriter\|self.anchors\|self.batches\|self.sync_calls" tests/test_main.py | head -6
```
Expected:
```text
docs/templates/token_metrics_manual_v0_gpu.csv
docs/templates/token_metrics_manual_v0_serving.csv
docs/templates/token_metrics_manual_v0_engine.csv
date,service,model,gpuType,category,gpuCount,gpuHours
date,service,model,metric,name,unit,p50,p90,p95,p99
service,engine_type,engine_version
docs/templates/token_metrics_manual_v0_gpu.csv:4
docs/templates/token_metrics_manual_v0_serving.csv:5
docs/templates/token_metrics_manual_v0_engine.csv:2
```
이어서 `app/normalize.py` 5줄(`SOURCE_MANUAL = "manual-v0"`, `PCT_KEYS = …`, `LATENCY_KEYS = …`, `class MetricsPayload`, `def normalize_payload`), `app/main.py` 6줄(`MODE_MANUAL = "manual"`, `def make_context`, `def _run_dates`, `def main`, `from app.writer import …`, `--replace`), `tests/test_main.py`에서 `class FakeWriter`와 `self.anchors`/`self.batches`/`self.sync_calls` 줄이 보여야 한다. 주석 줄에도 "Mock Service"가 없으므로 grep -c 값 4/5/2 = 예시 행 수 그대로(주석 줄에 그 문구가 있으면 값이 커진다 — 그 경우 `grep -v '^#' … | grep -c "Mock Service"`로 다시 세어 4/5/2 확인).

- [ ] **Step 2: 실패 테스트 — CSV 읽기 기초(주석·빈 줄·헤더 바이트 일치·BOM·셀 수·`_num`·`date_range`)**

`collectors/token-metrics/tests/test_manual.py` 생성(1부 — 3부까지 이 파일에 append):

```python
"""manual-v0 CSV 파서 (설계 §5.5 · Plan 6a F) — 템플릿 3파일을 fixture로 그대로 사용."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.config import ServiceEntry
from app.manual import (
    ENGINE_HEADER, GPU_HEADER, SERVING_HEADER, ManualCsvError, _num, date_range, read_csv_rows,
)

TEMPLATES = Path(__file__).resolve().parents[3] / "docs" / "templates"
T_GPU = str(TEMPLATES / "token_metrics_manual_v0_gpu.csv")
T_SERVING = str(TEMPLATES / "token_metrics_manual_v0_serving.csv")
T_ENGINE = str(TEMPLATES / "token_metrics_manual_v0_engine.csv")
TDATE = "2026-08-26"                       # 템플릿 예시 행의 날짜

ENTRY_A = ServiceEntry(service_group="Mock Group", service="Mock Service A", base_url="http://mock",
                       enabled=True, api_since=date(2026, 9, 9), coverage_since=date(2026, 8, 26), until=None)
ENTRY_B = ServiceEntry(service_group="Mock Group", service="Mock Service B", base_url="http://mock-b",
                       enabled=True, api_since=date(2026, 9, 9), coverage_since=date(2026, 8, 26), until=None)
ENTRIES = [ENTRY_A, ENTRY_B]


def write(tmp_path: Path, name: str, text: str, encoding: str = "utf-8") -> str:
    p = tmp_path / name
    p.write_text(text, encoding=encoding)
    return str(p)


# ---- read_csv_rows ------------------------------------------------------------------

def test_templates_headers_and_row_counts():
    assert [r for _, r in read_csv_rows(T_GPU, GPU_HEADER)] and len(read_csv_rows(T_GPU, GPU_HEADER)) == 4
    assert len(read_csv_rows(T_SERVING, SERVING_HEADER)) == 5
    assert len(read_csv_rows(T_ENGINE, ENGINE_HEADER)) == 2
    lineno, first = read_csv_rows(T_GPU, GPU_HEADER)[0]
    assert lineno == 11                                   # 주석 9줄 + 헤더 1줄 → 첫 데이터 행은 물리 11행
    assert first == {"date": TDATE, "service": "Mock Service A", "model": "claude-sonnet-5",
                     "gpuType": "H100", "category": "serving", "gpuCount": "4", "gpuHours": "96.0"}


def test_comment_and_blank_lines_skipped(tmp_path):
    path = write(tmp_path, "gpu.csv",
                 "# 주석, 안의, 쉼표는, 무시\n"
                 "\n"
                 "   # 좌측 공백 뒤 주석도 무시\n"
                 f"{GPU_HEADER}\n"
                 "\n"
                 f"{TDATE},Mock Service A,claude-sonnet-5,H100,serving,4,96.0\n"
                 "# 끝 주석\n")
    rows = read_csv_rows(path, GPU_HEADER)
    assert [ln for ln, _ in rows] == [6]
    assert rows[0][1]["gpuCount"] == "4"


def test_header_mismatch_error_reports_real_line(tmp_path):
    path = write(tmp_path, "gpu.csv", "# c1\n# c2\ndate,service,model\n")
    with pytest.raises(ManualCsvError) as ei:
        read_csv_rows(path, GPU_HEADER)
    msg = str(ei.value)
    assert msg.endswith(":3: header mismatch") and ":1:" not in msg


def test_header_missing_error(tmp_path):
    path = write(tmp_path, "gpu.csv", "# only comments\n\n")
    with pytest.raises(ManualCsvError, match=r":0: header missing"):
        read_csv_rows(path, GPU_HEADER)


def test_bom_tolerated(tmp_path):
    path = write(tmp_path, "gpu.csv", f"{GPU_HEADER}\n{TDATE},Mock Service A,m,H100,serving,1,2\n",
                 encoding="utf-8-sig")
    assert read_csv_rows(path, GPU_HEADER)[0][1]["date"] == TDATE


def test_crlf_and_cell_strip(tmp_path):
    path = write(tmp_path, "gpu.csv", f"{GPU_HEADER}\r\n{TDATE}, Mock Service A ,m,H100,serving,1,2\r\n")
    assert read_csv_rows(path, GPU_HEADER)[0][1]["service"] == "Mock Service A"


def test_column_count_error(tmp_path):
    path = write(tmp_path, "gpu.csv", f"{GPU_HEADER}\n{TDATE},Mock Service A,m,H100,serving,1\n")
    with pytest.raises(ManualCsvError, match=r":2: column count"):
        read_csv_rows(path, GPU_HEADER)


def test_quoted_cell_with_comma(tmp_path):
    path = write(tmp_path, "gpu.csv", f"{GPU_HEADER}\n{TDATE},\"Mock Service A\",\"m,v2\",H100,serving,1,2\n")
    assert read_csv_rows(path, GPU_HEADER)[0][1]["model"] == "m,v2"


# ---- _num / date_range --------------------------------------------------------------

def test_num_conversion():
    assert _num("") is None
    assert _num("4") == 4.0 and isinstance(_num("4"), float)
    assert _num("96.0") == 96.0
    assert _num("abc") == "abc"                           # 비숫자는 원문 유지 → normalize 가 거부


def test_date_range_inclusive_and_errors():
    assert date_range("2026-08-26", "2026-08-28") == ["2026-08-26", "2026-08-27", "2026-08-28"]
    assert date_range("2026-08-26", "2026-08-26") == ["2026-08-26"]
    with pytest.raises(ValueError, match="--from must not be after --to"):
        date_range("2026-08-27", "2026-08-26")
    with pytest.raises(ValueError):
        date_range("2026-13-01", "2026-13-02")
```

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -m pytest -q tests/test_manual.py`
Expected: `ERROR tests/test_manual.py` — `ModuleNotFoundError: No module named 'app.manual'` (수집 단계 실패, 1 error).

- [ ] **Step 3: `app/manual.py` 1부 — 상수·`ManualCsvError`·`date_range`·`read_csv_rows`·`_num`**

`collectors/token-metrics/app/manual.py` 생성(전체 내용 — `load_manual_csvs`는 Step 5에서 파일 끝에 append):

```python
"""manual-v0 CSV 로더 (설계 §5.5) — 템플릿 3파일(docs/templates/token_metrics_manual_v0_{gpu,serving,engine}.csv)을
(date, service)별 MetricsPayload로 묶는다. 값 검증(형태 거부·의미 플래그)은 하지 않는다 — API 경로와 동일하게
normalize_payload가 한 곳에서 한다(§5.3). 이 모듈은 '파일이 계약대로 생겼는가'(헤더·컬럼 수·등록 서비스·날짜·metric 키·중복)만 본다.

파서 규칙(Plan 6a F): '#'로 시작하는 줄은 주석(안의 쉼표 무시), 빈 줄 무시, 첫 비주석 줄이 헤더(바이트 동일), 빈 셀 = 부재,
UTF-8(BOM 허용), 날짜 YYYY-MM-DD(KST). 오류 메시지는 '경로:줄번호: 무엇' — 행 원문·서비스명·모델명은 넣지 않는다(§3 전제 11).
"""
from __future__ import annotations

import csv
from datetime import date, timedelta

from app.config import ServiceEntry
from app.normalize import PCT_KEYS, SOURCE_MANUAL, MetricsPayload

GPU_HEADER = "date,service,model,gpuType,category,gpuCount,gpuHours"
SERVING_HEADER = "date,service,model,metric,name,unit,p50,p90,p95,p99"
ENGINE_HEADER = "service,engine_type,engine_version"
STANDARD_METRICS = ("ttftMs", "itlMs", "e2eMs", "outputTps")      # API 키 그대로 — fact metric 변환은 normalize
SERVING_METRICS = STANDARD_METRICS + ("custom",)
COUNT_KEYS = ("rows_gpu", "rows_serving", "rows_engine", "rows_outside_range", "rows_other_service")


class ManualCsvError(ValueError):
    """파일 계약 위반 — 적재 전 전체 거부(main이 stderr + exit 2). 메시지에 행 원문을 담지 않는다."""

    def __init__(self, path: str, lineno: int, what: str):
        super().__init__(f"{path}:{lineno}: {what}")
        self.path = path
        self.lineno = lineno
        self.what = what


def date_range(from_date: str, to_date: str) -> list[str]:
    """--from/--to 포함 범위 (YYYY-MM-DD, KST). 형식 오류는 date.fromisoformat의 ValueError."""
    d0 = date.fromisoformat(from_date)
    d1 = date.fromisoformat(to_date)
    if d1 < d0:
        raise ValueError("--from must not be after --to")
    return [str(d0 + timedelta(days=i)) for i in range((d1 - d0).days + 1)]


def read_csv_rows(path: str, expected_header: str) -> list[tuple[int, dict[str, str]]]:
    """주석·빈 줄을 건너뛰고 헤더(바이트 동일)를 확인한 뒤 (물리 줄 번호, {컬럼: strip 된 셀}) 목록을 돌려준다.
    셀 안 개행은 지원하지 않는다(줄 단위 파싱 — 템플릿 계약 밖)."""
    columns = expected_header.split(",")
    rows: list[tuple[int, dict[str, str]]] = []
    header_seen = False
    with open(path, encoding="utf-8-sig", newline="") as f:
        for lineno, raw in enumerate(f, start=1):
            line = raw.rstrip("\r\n")
            if line.strip() == "" or line.lstrip().startswith("#"):
                continue
            if not header_seen:
                if line != expected_header:
                    raise ManualCsvError(path, lineno, "header mismatch")
                header_seen = True
                continue
            cells = next(csv.reader([line]))
            if len(cells) != len(columns):
                raise ManualCsvError(path, lineno, f"column count {len(cells)} != {len(columns)}")
            rows.append((lineno, {col: cell.strip() for col, cell in zip(columns, cells)}))
    if not header_seen:
        raise ManualCsvError(path, 0, "header missing")
    return rows


def _num(cell: str) -> object:
    """빈 셀 → None(부재), 숫자 → float, 그 외 → 원문 str(normalize _is_num 이 비숫자로 거부)."""
    if cell == "":
        return None
    try:
        return float(cell)
    except ValueError:
        return cell
```

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -m pytest -q tests/test_manual.py`
Expected: `10 passed`.

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -c "import ast,sys; ast.parse(open('app/manual.py').read()); print('ok')" && python3 -c "import app.manual as m; print(m.GPU_HEADER); print(m.SERVING_METRICS)"`
Expected:
```text
ok
date,service,model,gpuType,category,gpuCount,gpuHours
('ttftMs', 'itlMs', 'e2eMs', 'outputTps', 'custom')
```

- [ ] **Step 4: 실패 테스트 — `load_manual_csvs`(템플릿 그대로 → payload · normalize 통과 · 오류 5종 · 필터 카운트 · 행 없는 (date, service) 키 없음 · 비숫자 통과 · generated_at)**

`collectors/token-metrics/tests/test_manual.py` 끝에 append(2부). 파일 상단 import 블록을 아래처럼 **교체**(`load_manual_csvs`·`normalize_payload`·`KST`·`datetime` 추가):

```python
from datetime import date, datetime
from pathlib import Path

import pytest

from app.config import ServiceEntry
from app.manual import (
    ENGINE_HEADER, GPU_HEADER, SERVING_HEADER, ManualCsvError, _num, date_range, load_manual_csvs,
    read_csv_rows,
)
from app.normalize import KST, normalize_payload
```

append 본문:

```python
# ---- load_manual_csvs ---------------------------------------------------------------

def gpu_csv(tmp_path: Path, *rows: str) -> str:
    return write(tmp_path, "gpu.csv", "\n".join(("# gpu", GPU_HEADER) + rows) + "\n")


def serving_csv(tmp_path: Path, *rows: str) -> str:
    return write(tmp_path, "serving.csv", "\n".join(("# serving", SERVING_HEADER) + rows) + "\n")


def engine_csv(tmp_path: Path, *rows: str) -> str:
    return write(tmp_path, "engine.csv", "\n".join(("# engine", ENGINE_HEADER) + rows) + "\n")


def load(gpu: str, serving: str, engine: str | None = None, *, frm: str = TDATE, to: str = TDATE,
         entries=ENTRIES, only: str | None = None, gen: str = ""):
    return load_manual_csvs(gpu, serving, engine, frm, to, entries, only, gen)


def test_templates_parse_as_is():
    payloads, counts = load(T_GPU, T_SERVING, T_ENGINE)
    assert set(payloads) == {(TDATE, "Mock Service A"), (TDATE, "Mock Service B")}
    a = payloads[(TDATE, "Mock Service A")]
    assert a.source_type == "manual-v0" and a.generated_at_raw == ""
    assert (a.reported_service_group, a.reported_service) == ("Mock Group", "Mock Service A")
    assert a.gpu == [
        {"model": "claude-sonnet-5", "gpuType": "H100", "category": "serving", "gpuCount": 4.0, "gpuHours": 96.0},
        {"model": "claude-sonnet-5", "gpuType": "H100", "category": "standby", "gpuCount": 1.0, "gpuHours": 24.0},
    ]
    assert len(a.serving) == 1 and set(a.serving[0]) == {"model", "ttftMs", "itlMs", "outputTps"}
    assert a.serving[0]["ttftMs"] == {"p50": 280.0, "p90": 560.0, "p95": 720.0, "p99": 1200.0}
    assert a.serving[0]["outputTps"] == {"p50": 41.0}
    assert a.engine == {"type": "vllm", "version": "0.8.4"}
    b = payloads[(TDATE, "Mock Service B")]
    assert [r["model"] for r in b.gpu] == ["claude-haiku-4-5", "unknown"]
    assert len(b.serving) == 1 and set(b.serving[0]) == {"model", "e2eMs", "custom"}
    assert set(b.serving[0]["e2eMs"]) == {"p50", "p90", "p95", "p99"}
    assert b.serving[0]["custom"] == [{"name": "queueWaitMs", "unit": "ms", "p50": 120.0, "p99": 900.0}]
    assert b.engine == {"type": "custom", "version": ""}
    assert counts == {"rows_gpu": 4, "rows_serving": 5, "rows_engine": 2,
                      "rows_outside_range": 0, "rows_other_service": 0}


def test_templates_then_normalize_clean():
    payloads, _ = load(T_GPU, T_SERVING, T_ENGINE)
    ra = normalize_payload(payloads[(TDATE, "Mock Service A")], ENTRY_A)
    assert (ra.rows, ra.rejected, ra.warns) == (5, 0, {})               # gpu 2 + ttft/itl/outputTps 3
    assert (ra.engine_type, ra.engine_version) == ("vllm", "0.8.4")
    rb = normalize_payload(payloads[(TDATE, "Mock Service B")], ENTRY_B)
    assert (rb.rows, rb.rejected, rb.warns) == (4, 0, {})               # gpu 2 + e2e 1 + custom 1
    assert (rb.engine_type, rb.engine_version) == ("custom", "")


def test_unknown_service_error_has_no_row_content(tmp_path):
    g = gpu_csv(tmp_path, f"{TDATE},secret-svc-x,secret-model-xyz,H100,serving,1,2")
    s = serving_csv(tmp_path)
    with pytest.raises(ManualCsvError) as ei:
        load(g, s)
    msg = str(ei.value)
    assert msg.endswith(":3: unknown service (not in endpoints)")
    assert "secret-svc-x" not in msg and "secret-model-xyz" not in msg


def test_validation_applies_to_ignored_rows(tmp_path):
    g = gpu_csv(tmp_path, "2026-01-01,secret-svc-x,m,H100,serving,1,2")     # 범위 밖이어도 미등록은 오류
    with pytest.raises(ManualCsvError, match="unknown service"):
        load(g, serving_csv(tmp_path))
    s = serving_csv(tmp_path, f"{TDATE},Mock Service B,m,ttft_ms,,,1,2,3,4")  # --service A 필터 밖이어도 metric 오류
    with pytest.raises(ManualCsvError, match="bad metric"):
        load(gpu_csv(tmp_path), s, only="Mock Service A")


def test_bad_date_error(tmp_path):
    for bad in ("2026/08/26", "26-08-26", "2026-8-26", "20260826"):
        g = gpu_csv(tmp_path, f"{bad},Mock Service A,m,H100,serving,1,2")
        with pytest.raises(ManualCsvError, match=r":3: bad date"):
            load(g, serving_csv(tmp_path))


def test_bad_metric_error(tmp_path):
    s = serving_csv(tmp_path, f"{TDATE},Mock Service A,m,ttft_ms,,,1,2,3,4")
    with pytest.raises(ManualCsvError, match=r":3: bad metric"):
        load(gpu_csv(tmp_path), s)


def test_duplicate_model_metric_error(tmp_path):
    s = serving_csv(tmp_path,
                    f"{TDATE},Mock Service A,m,ttftMs,,,1,2,3,4",
                    f"{TDATE},Mock Service A,m,ttftMs,,,5,6,7,8")
    with pytest.raises(ManualCsvError, match=r":4: duplicate \(model, metric\)"):
        load(gpu_csv(tmp_path), s)
    # 같은 model 의 서로 다른 지표는 한 레코드로 합쳐진다(long form → API 레코드)
    s2 = serving_csv(tmp_path,
                     f"{TDATE},Mock Service A,m,ttftMs,,,1,2,3,4",
                     f"{TDATE},Mock Service A,m,outputTps,,,9,,,")
    payloads, _ = load(gpu_csv(tmp_path), s2)
    rec = payloads[(TDATE, "Mock Service A")].serving
    assert len(rec) == 1 and set(rec[0]) == {"model", "ttftMs", "outputTps"}
    # custom name 중복은 파서 오류가 아니다 — normalize 가 dup_custom_kept_first 로 처리
    s3 = serving_csv(tmp_path,
                     f"{TDATE},Mock Service A,m,custom,q,ms,1,,,",
                     f"{TDATE},Mock Service A,m,custom,q,ms,2,,,")
    payloads, _ = load(gpu_csv(tmp_path), s3)
    assert len(payloads[(TDATE, "Mock Service A")].serving[0]["custom"]) == 2


def test_engine_errors_and_optional(tmp_path):
    e_dup = engine_csv(tmp_path, "Mock Service A,vllm,0.8.4", "Mock Service A,sglang,")
    with pytest.raises(ManualCsvError, match=r":4: duplicate service"):
        load(gpu_csv(tmp_path), serving_csv(tmp_path), e_dup, only="Mock Service A")
    e_unknown = engine_csv(tmp_path, "secret-svc-x,vllm,")
    with pytest.raises(ManualCsvError, match=r":3: unknown service"):
        load(gpu_csv(tmp_path), serving_csv(tmp_path), e_unknown, only="Mock Service A")
    payloads, counts = load(gpu_csv(tmp_path), serving_csv(tmp_path), None, only="Mock Service A")
    assert payloads[(TDATE, "Mock Service A")].engine is None and counts["rows_engine"] == 0
    e_ok = engine_csv(tmp_path, "Mock Service B,custom,")          # --service A 필터와 무관하게 파일 전체를 센다
    payloads, counts = load(gpu_csv(tmp_path), serving_csv(tmp_path), e_ok, only="Mock Service A")
    assert payloads[(TDATE, "Mock Service A")].engine is None and counts["rows_engine"] == 1


def test_outside_range_and_other_service_counted(tmp_path):
    g = gpu_csv(tmp_path,
                f"{TDATE},Mock Service A,m,H100,serving,1,2",
                "2026-08-27,Mock Service A,m,H100,serving,1,2",          # 범위 밖
                f"{TDATE},Mock Service B,m,H100,serving,1,2")             # 다른 서비스
    payloads, counts = load(g, serving_csv(tmp_path), only="Mock Service A")
    assert set(payloads) == {(TDATE, "Mock Service A")}
    assert len(payloads[(TDATE, "Mock Service A")].gpu) == 1
    assert counts == {"rows_gpu": 1, "rows_serving": 0, "rows_engine": 0,
                      "rows_outside_range": 1, "rows_other_service": 1}


def test_empty_service_day_yields_no_payload(tmp_path):
    g = gpu_csv(tmp_path, f"{TDATE},Mock Service A,m,H100,serving,1,2")
    payloads, counts = load(g, serving_csv(tmp_path), to="2026-08-27")
    assert set(payloads) == {(TDATE, "Mock Service A")}                  # 08-27 은 행 없음 → 키 없음(앵커 없음)
    assert ("2026-08-27", "Mock Service A") not in payloads
    assert counts["rows_gpu"] == 1 and counts["rows_outside_range"] == 0  # 범위 안(08-26..08-27)이므로 outside 아님
    assert normalize_payload(payloads[(TDATE, "Mock Service A")], ENTRY_A).is_nodata is False


def test_only_service_without_rows_yields_no_payload(tmp_path):
    g = gpu_csv(tmp_path, f"{TDATE},Mock Service B,m,H100,serving,1,2")
    payloads, counts = load(g, serving_csv(tmp_path), only="Mock Service A")
    assert payloads == {} and counts["rows_other_service"] == 1           # 행 0건 → 페이로드 0개(NODATA 앵커 아님)
    with pytest.raises(ValueError, match="unknown service: nope"):
        load(g, serving_csv(tmp_path), only="nope")


def test_service_only_in_serving_gets_payload(tmp_path):
    s = serving_csv(tmp_path, f"{TDATE},Mock Service B,m,ttftMs,,,1,2,3,4",
                    f"{TDATE},Mock Service B,m,itlMs,,,1,2,3,4",
                    f"{TDATE},Mock Service B,m,outputTps,,,9,,,")
    payloads, _ = load(gpu_csv(tmp_path), s)
    assert set(payloads) == {(TDATE, "Mock Service B")}
    r = normalize_payload(payloads[(TDATE, "Mock Service B")], ENTRY_B)
    assert r.rows == 3 and r.is_nodata is False                          # 케이스 E: gpu:[] + serving 행 → SUCCESS


def test_bad_number_kept_for_normalize(tmp_path):
    g = gpu_csv(tmp_path, f"{TDATE},Mock Service A,m,H100,serving,abc,2")
    payloads, _ = load(g, serving_csv(tmp_path))
    assert payloads[(TDATE, "Mock Service A")].gpu[0]["gpuCount"] == "abc"
    r = normalize_payload(payloads[(TDATE, "Mock Service A")], ENTRY_A)
    assert (r.rows, r.rejected) == (0, 1)


def test_blank_pct_omitted_and_blank_custom_name_kept(tmp_path):
    s = serving_csv(tmp_path,
                    f"{TDATE},Mock Service A,m,ttftMs,,,1,2,3,",           # p99 부재 → 키 없음 → normalize 거부
                    f"{TDATE},Mock Service A,m2,custom,,ms,1,,,")          # name 빈값 → "" → normalize 거부
    payloads, _ = load(gpu_csv(tmp_path), s)
    recs = payloads[(TDATE, "Mock Service A")].serving
    assert recs[0]["ttftMs"] == {"p50": 1.0, "p90": 2.0, "p95": 3.0}
    assert recs[1]["custom"] == [{"name": "", "unit": "ms", "p50": 1.0}]
    r = normalize_payload(payloads[(TDATE, "Mock Service A")], ENTRY_A)
    assert (r.rows, r.rejected) == (0, 2)


def test_generated_at_passthrough(tmp_path):
    g = gpu_csv(tmp_path, f"{TDATE},Mock Service A,m,H100,serving,1,2")
    payloads, _ = load(g, serving_csv(tmp_path), gen="2026-08-27T09:00:00+09:00")
    p = payloads[(TDATE, "Mock Service A")]
    assert p.generated_at_raw == "2026-08-27T09:00:00+09:00"
    r = normalize_payload(p, ENTRY_A)
    assert r.generated_at == datetime(2026, 8, 27, 9, 0, tzinfo=KST) and r.warns == {}
    payloads, _ = load(g, serving_csv(tmp_path), gen="")
    assert payloads[(TDATE, "Mock Service A")].generated_at_raw == ""
```

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -m pytest -q tests/test_manual.py`
Expected: `ERROR tests/test_manual.py` — `ImportError: cannot import name 'load_manual_csvs' from 'app.manual'`.

- [ ] **Step 5: `app/manual.py` 2부 — `load_manual_csvs`(등록 대조·날짜·metric 검증 → 필터 카운트 → (date, service) payload)**

`collectors/token-metrics/app/manual.py` 끝(`_num` 뒤)에 append:

```python


def _check_row(path: str, lineno: int, row: dict[str, str], registry: dict[str, ServiceEntry]) -> str:
    """등록·날짜 형식 검증(필터와 무관하게 모든 행) — 통과 시 정규 날짜 문자열 반환."""
    if row["service"] not in registry:
        raise ManualCsvError(path, lineno, "unknown service (not in endpoints)")
    raw_date = row["date"]
    try:
        parsed = date.fromisoformat(raw_date)
    except ValueError:
        raise ManualCsvError(path, lineno, "bad date") from None
    if str(parsed) != raw_date:                      # YYYY-MM-DD 만 (3.11+ 의 YYYYMMDD 완화 형식 거부)
        raise ManualCsvError(path, lineno, "bad date")
    return raw_date


def load_manual_csvs(gpu_path: str, serving_path: str, engine_path: str | None,
                     from_date: str, to_date: str, entries: list[ServiceEntry],
                     only_service: str | None, generated_at_raw: str,
                     ) -> tuple[dict[tuple[str, str], MetricsPayload], dict[str, int]]:
    """템플릿 3파일 → {(date, service): MetricsPayload} + 카운트(COUNT_KEYS).

    - 파일 계약 위반(헤더·컬럼 수·미등록 서비스·날짜 형식·metric 키·(model, metric) 중복·engine 서비스 중복)은
      ManualCsvError — 아무것도 적재하지 않는다. 값 검증은 normalize_payload 몫(빈 셀·비숫자는 그대로 전달).
    - 필터: --service 밖 행 → rows_other_service, --from/--to 밖 행 → rows_outside_range (둘 다면 앞 것만).
    - payload 는 채택된 gpu∪serving 행이 1건 이상인 (date, service) 에만 만든다 — 행 없는 (date, service) 는
      키 없음(페이로드·앵커 없음 → 6c metrics_missing 이 '수기 입력 없음'으로 본다).
    """
    registry = {e.service: e for e in entries}
    if only_service is not None and only_service not in registry:
        raise ValueError(f"unknown service: {only_service}")
    dates = date_range(from_date, to_date)
    counts = {k: 0 for k in COUNT_KEYS}
    gpu_by_key: dict[tuple[str, str], list[dict]] = {}
    serving_by_key: dict[tuple[str, str], dict[str, dict]] = {}     # (date, service) → {model: record}

    def _target(row: dict[str, str], day: str) -> bool:
        if only_service is not None and row["service"] != only_service:
            counts["rows_other_service"] += 1
            return False
        if not (dates[0] <= day <= dates[-1]):
            counts["rows_outside_range"] += 1
            return False
        return True

    for lineno, row in read_csv_rows(gpu_path, GPU_HEADER):
        day = _check_row(gpu_path, lineno, row, registry)
        if not _target(row, day):
            continue
        counts["rows_gpu"] += 1
        gpu_by_key.setdefault((day, row["service"]), []).append({
            "model": row["model"], "gpuType": row["gpuType"], "category": row["category"],
            "gpuCount": _num(row["gpuCount"]), "gpuHours": _num(row["gpuHours"]),
        })

    for lineno, row in read_csv_rows(serving_path, SERVING_HEADER):
        day = _check_row(serving_path, lineno, row, registry)
        metric = row["metric"]
        if metric not in SERVING_METRICS:
            raise ManualCsvError(serving_path, lineno, "bad metric")
        if not _target(row, day):
            continue
        counts["rows_serving"] += 1
        records = serving_by_key.setdefault((day, row["service"]), {})
        record = records.setdefault(row["model"], {"model": row["model"]})
        pcts = {p: _num(row[p]) for p in PCT_KEYS if row[p] != ""}     # 빈 p = 키 부재(normalize 가 필수키 판정)
        if metric == "custom":
            record.setdefault("custom", []).append({"name": row["name"], "unit": row["unit"], **pcts})
        else:
            if metric in record:
                raise ManualCsvError(serving_path, lineno, "duplicate (model, metric)")
            record[metric] = pcts                                       # 표준 지표 행의 name/unit 셀은 무시

    engine_map: dict[str, dict] = {}
    if engine_path is not None:
        for lineno, row in read_csv_rows(engine_path, ENGINE_HEADER):
            service = row["service"]
            if service not in registry:
                raise ManualCsvError(engine_path, lineno, "unknown service (not in endpoints)")
            if service in engine_map:
                raise ManualCsvError(engine_path, lineno, "duplicate service")
            engine_map[service] = {"type": row["engine_type"], "version": row["engine_version"]}
            counts["rows_engine"] += 1

    payloads: dict[tuple[str, str], MetricsPayload] = {}
    for day, service in sorted(set(gpu_by_key) | set(serving_by_key)):   # 행이 1건 이상인 (date, service) 만
        entry = registry[service]
        payloads[(day, service)] = MetricsPayload(
            date=day,
            reported_service_group=entry.service_group,      # §5.5 reported_* = 레지스트리 값
            reported_service=entry.service,
            generated_at_raw=generated_at_raw,               # "" = 적재 시각 (normalize, WARN 없음)
            engine=engine_map.get(service),                  # 파일 없음/행 없음 → None → engine_type ''
            gpu=gpu_by_key.get((day, service), []),
            serving=list(serving_by_key.get((day, service), {}).values()),
            source_type=SOURCE_MANUAL,
        )
    return payloads, counts
```

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -m pytest -q tests/test_manual.py`
Expected: `25 passed`.

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -m pytest -q`
Expected: T2~T6 테스트 포함 전부 `passed`, 실패 0(기존 테스트 무영향 — `app/manual.py`는 신규 파일이고 `app/normalize.py`·`app/config.py`를 읽기만 한다).

- [ ] **Step 6: 커밋 1 — 파서**

```bash
cd /home/mini/github/token-data-pipeline
git diff --stat -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml
git status --porcelain -- collectors/token-metrics
git add collectors/token-metrics/app/manual.py collectors/token-metrics/tests/test_manual.py
git commit -m "feat(collectors-metrics): manual-v0 CSV 파서 — 템플릿 3파일 계약(주석·헤더 바이트 일치·등록 대조·metric 키)·(date, service) MetricsPayload (Plan 6b T7)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```
Expected: 첫 `git diff --stat` 출력 없음(zero-diff), `git status`는 `?? collectors/token-metrics/app/manual.py`·`?? collectors/token-metrics/tests/test_manual.py` 2줄, 커밋 `2 files changed`.

- [ ] **Step 7: 실패 테스트 — `main()` manual 모드(마커·동기화 0·배칭·인자 검증·already_loaded/--replace·disabled·--service·다중 날짜(행 없는 날 앵커 없음)·뮤테이션 예산 승격)**

`collectors/token-metrics/tests/test_main.py`(T6 산출) **끝에 append**. T6의 `FakeWriter`(`FakeWriter()` 기본 생성, `anchors: set`, `batches: list[(date, [services])]`, `sync_calls: int`, `FakeWriter(raise_budget=True)`)·`Config`·`ServiceEntry`·`main`을 그대로 쓴다(이미 import 됨 — 아래 import 2줄은 신규 이름만).

```python


# ---- manual 모드 (T7 — 설계 §5.5 · §5.2 표 manual 행) -------------------------------------
from pathlib import Path

from app.main import MANUAL_INPUT_PREFIX

TEMPLATES = Path(__file__).resolve().parents[3] / "docs" / "templates"
MANUAL_ARGS = ["--manual-gpu", str(TEMPLATES / "token_metrics_manual_v0_gpu.csv"),
               "--manual-serving", str(TEMPLATES / "token_metrics_manual_v0_serving.csv"),
               "--manual-engine", str(TEMPLATES / "token_metrics_manual_v0_engine.csv")]
MANUAL_DATE = "2026-08-26"                      # 템플릿 예시 행의 날짜 (api_since 2026-09-09 보다 앞 — manual 은 게이트 없음)
RANGE = ["--from", MANUAL_DATE, "--to", MANUAL_DATE]
M_A = ServiceEntry(service_group="Mock Group", service="Mock Service A", base_url="http://mock",
                   enabled=True, api_since=date(2026, 9, 9), coverage_since=date(2026, 8, 26), until=None)
M_B = ServiceEntry(service_group="Mock Group", service="Mock Service B", base_url="http://mock-b",
                   enabled=True, api_since=date(2026, 9, 9), coverage_since=date(2026, 8, 26), until=None)
LINE_A = ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A "
          "source_type=manual-v0 rows=5 pages=1 warn=0 rejected=0")
LINE_B = ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service B "
          "source_type=manual-v0 rows=4 pages=1 warn=0 rejected=0")


def _manual_env(monkeypatch, writer, entries=None):
    monkeypatch.setattr("app.main.load_config", lambda: Config())
    monkeypatch.setattr("app.main.load_endpoints", lambda p: entries if entries is not None else [M_A, M_B])
    monkeypatch.setattr("app.main.MetricsWriter", lambda cfg: writer)


def test_manual_mode_markers_and_no_registry_sync(capsys, monkeypatch):
    w = FakeWriter()
    _manual_env(monkeypatch, w)
    code = main(MANUAL_ARGS + RANGE)
    out = capsys.readouterr().out
    lines = out.splitlines()
    assert code == 0
    assert lines[0] == (f"{MANUAL_INPUT_PREFIX} rows_gpu=4 rows_serving=5 rows_engine=2 "
                        "rows_outside_range=0 rows_other_service=0")
    assert LINE_A in lines and LINE_B in lines                     # api_since 이전 날짜여도 before_since 없음
    assert out.count("MANUAL_INPUT") == 1 and out.count("BATCH_RESULT") == 1
    batch = lines[-1]
    assert batch.startswith("BATCH_RESULT status=SUCCESS module=token-metrics services_ok=2 "
                            "services_failed=0 services_skipped=0 rows=9 elapsed=")
    assert " slot=" in batch and batch.endswith(" final=0")         # manual 은 최종 슬롯 판정 없음
    assert w.sync_calls == 0                                        # §5.5 레지스트리 동기화 없음
    assert len(w.batches) == 1 and sorted(w.batches[0][1]) == ["Mock Service A", "Mock Service B"]  # 날짜당 replace_batch 1회
    assert "CHECK WARN" not in out
    assert "claude-sonnet-5" not in out and "queueWaitMs" not in out  # 페이로드·행 원문 금지 (§3 전제 11)


def test_manual_mode_requires_pair_and_range(capsys, monkeypatch):
    w = FakeWriter()
    _manual_env(monkeypatch, w)
    assert main(MANUAL_ARGS[:2] + RANGE) == 2                       # --manual-gpu 만
    assert "--manual-gpu/--manual-serving must be given together" in capsys.readouterr().err
    assert main(MANUAL_ARGS[2:4] + RANGE) == 2                      # --manual-serving 만
    assert "must be given together" in capsys.readouterr().err
    assert main(MANUAL_ARGS) == 2                                   # --from/--to 없음
    assert "manual mode requires --from/--to" in capsys.readouterr().err
    assert main(["--generated-at", "2026-08-27T09:00:00+09:00"] + RANGE) == 2
    assert "require --manual-gpu/--manual-serving" in capsys.readouterr().err
    assert main(MANUAL_ARGS[4:] + RANGE) == 2                       # --manual-engine 단독
    assert "require --manual-gpu/--manual-serving" in capsys.readouterr().err
    assert w.batches == [] and w.sync_calls == 0


def test_manual_input_error_exits_2_without_load(capsys, monkeypatch, tmp_path):
    w = FakeWriter()
    _manual_env(monkeypatch, w)
    bad = tmp_path / "gpu.csv"
    bad.write_text("date,service,model\n", encoding="utf-8")
    code = main(["--manual-gpu", str(bad), "--manual-serving", MANUAL_ARGS[3]] + RANGE)
    captured = capsys.readouterr()
    assert code == 2
    assert "manual input error:" in captured.err and ":1: header mismatch" in captured.err
    assert "MANUAL_INPUT" not in captured.out and "BATCH_RESULT" not in captured.out
    assert w.batches == []
    assert main(MANUAL_ARGS + ["--from", "2026-08-27", "--to", "2026-08-26"]) == 2     # 역순 범위
    assert "--from must not be after --to" in capsys.readouterr().err
    missing = str(tmp_path / "absent.csv")                                              # 파일 없음(OSError) → 2
    assert main(["--manual-gpu", missing, "--manual-serving", MANUAL_ARGS[3]] + RANGE) == 2
    assert "manual input error:" in capsys.readouterr().err
    assert w.batches == []


def test_manual_already_loaded_without_replace(capsys, monkeypatch):
    w = FakeWriter()
    w.anchors.add((MANUAL_DATE, "Mock Service A"))
    _manual_env(monkeypatch, w)
    code = main(MANUAL_ARGS + RANGE)
    out = capsys.readouterr().out
    assert code == 0
    assert ("SERVICE_RESULT status=SKIPPED module=token-metrics service=Mock Service A "
            "source_type=manual-v0 rows=0 pages=1 warn=0 rejected=0 reason=already_loaded") in out
    assert LINE_B in out
    assert len(w.batches) == 1 and w.batches[0][1] == ["Mock Service B"]
    assert "services_ok=1 services_failed=0 services_skipped=1 rows=4" in out

    w2 = FakeWriter()
    w2.anchors.add((MANUAL_DATE, "Mock Service A"))
    _manual_env(monkeypatch, w2)
    code = main(MANUAL_ARGS + RANGE + ["--replace"])
    out = capsys.readouterr().out
    assert code == 0 and "reason=already_loaded" not in out and LINE_A in out
    assert len(w2.batches) == 1 and sorted(w2.batches[0][1]) == ["Mock Service A", "Mock Service B"]
    assert w2.sync_calls == 0


def test_manual_disabled_gate_and_service_filter(capsys, monkeypatch):
    disabled_b = ServiceEntry(service_group="Mock Group", service="Mock Service B", base_url="http://mock-b",
                              enabled=False, api_since=date(2026, 9, 9), coverage_since=date(2026, 8, 26),
                              until=None)
    w = FakeWriter()
    _manual_env(monkeypatch, w, entries=[M_A, disabled_b])
    assert main(MANUAL_ARGS + RANGE) == 0
    out = capsys.readouterr().out
    assert ("SERVICE_RESULT status=SKIPPED module=token-metrics service=Mock Service B "
            "source_type=manual-v0 rows=0 pages=1 warn=0 rejected=0 reason=disabled") in out   # 모든 모드 공통
    assert w.batches[0][1] == ["Mock Service A"]

    w2 = FakeWriter()
    _manual_env(monkeypatch, w2)
    assert main(MANUAL_ARGS + RANGE + ["--service", "Mock Service A"]) == 0
    out = capsys.readouterr().out
    assert (f"{MANUAL_INPUT_PREFIX} rows_gpu=2 rows_serving=3 rows_engine=2 "
            "rows_outside_range=0 rows_other_service=4") in out         # B 행 gpu 2 + serving 2 는 무시·카운트
    assert LINE_A in out and "service=Mock Service B" not in out
    assert w2.batches[0][1] == ["Mock Service A"]

    w3 = FakeWriter()
    _manual_env(monkeypatch, w3)
    assert main(MANUAL_ARGS + RANGE + ["--service", "nope"]) == 2       # T6 필터: unknown service → exit 2
    assert "unknown service: nope" in capsys.readouterr().err
    assert w3.batches == []


def test_manual_multi_day_no_rows_no_anchor_and_single_batch_line(capsys, monkeypatch):
    w = FakeWriter()
    _manual_env(monkeypatch, w)
    code = main(MANUAL_ARGS + ["--from", MANUAL_DATE, "--to", "2026-08-27",
                               "--generated-at", "2026-08-28T09:00:00+09:00"])
    out = capsys.readouterr().out
    assert code == 0
    assert out.count("BATCH_RESULT") == 1 and out.count("MANUAL_INPUT") == 1
    assert out.count("SERVICE_RESULT status=SUCCESS") == 2                 # 08-26 서비스 2개만
    assert "status=NODATA" not in out                                      # 08-27 행 없음 → 페이로드·앵커 없음(NODATA 아님)
    assert "services_ok=2 services_failed=0 services_skipped=0 rows=9" in out
    assert [d for d, _ in w.batches] == [MANUAL_DATE]                      # 행 있는 날짜만 replace_batch, 08-27 은 호출 없음
    assert sorted(w.batches[0][1]) == ["Mock Service A", "Mock Service B"]


def test_manual_generated_at_offset_mismatch_is_warn(capsys, monkeypatch):
    w = FakeWriter()
    _manual_env(monkeypatch, w)
    assert main(MANUAL_ARGS + RANGE + ["--generated-at", "2026-08-27T00:00:00+00:00"]) == 0
    out = capsys.readouterr().out
    assert "CHECK WARN service=Mock Service A generated_at_offset_mismatch=1" in out
    assert "service=Mock Service A source_type=manual-v0 rows=5 pages=1 warn=1 rejected=0" in out


def test_manual_mutation_budget_promoted_to_batch_reason(capsys, monkeypatch):
    w = FakeWriter(raise_budget=True)
    _manual_env(monkeypatch, w)
    code = main(MANUAL_ARGS + RANGE + ["--replace"])
    out = capsys.readouterr().out
    assert code == 1
    assert out.count("reason=mutation_budget") == 3                    # 서비스 2줄 + BATCH 1줄
    assert out.splitlines()[-1].startswith("BATCH_RESULT status=FAILURE module=token-metrics services_ok=0 services_failed=2")
    assert out.splitlines()[-1].endswith(" final=0 reason=mutation_budget")
```

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -m pytest -q tests/test_main.py -k manual`
Expected: 수집 단계 `ImportError: cannot import name 'MANUAL_INPUT_PREFIX' from 'app.main'` (1 error). (`MANUAL_INPUT_PREFIX` import 줄을 잠시 지우고 돌리면 T7 의 8개가 전부 실패 — `SystemExit: 2` from argparse `unrecognized arguments: --manual-gpu …`; `-k manual` 에는 T6 의 `test_manual_row_present_warn_regular_only` 도 잡히는데 그것은 정기 경로 테스트라 통과한다.)

- [ ] **Step 8: `app/main.py` manual 분기 — import·상수·헬퍼 2개·`_run_manual`·`main()` 삽입 3곳(T6 파일에 5개 hunk)**

T6 산출 `collectors/token-metrics/app/main.py`를 편집한다. 줄 번호는 T6 최종본에 따라 다르므로 **grep 앵커** 기준으로 넣는다(각 앵커는 T6 인터페이스에 고정된 문자열). 편집 전 앵커 5개가 정확히 1번씩 있는지 확인:

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && grep -c "^from app.writer import MetricsWriter, MutationBudgetExceeded" app/main.py; grep -c "^MODE_MANUAL = \"manual\"" app/main.py; grep -c "^def main(argv" app/main.py; grep -c "args = parser.parse_args(argv)" app/main.py; grep -c "batch_time = _parse_batch_time(args.batch_time)" app/main.py; grep -n "SOURCE_MANUAL" app/main.py | head -3`
Expected: `1` 5번, 이어서 `SOURCE_MANUAL`이 `from app.normalize import (` 블록(T6 는 2줄 괄호 import)과 `_prepare_one`(manual_row_present 판정)에 등장. `SOURCE_MANUAL`이 그 블록에 없으면 hunk (a)에서 괄호 안 목록 끝에 `, SOURCE_MANUAL`을 덧붙인다.

**(a) import** — `from app.writer import MetricsWriter, MutationBudgetExceeded` 줄 **바로 아래**(`from app.normalize import (…)` 2줄 블록의 **안이 아니라** 그 블록 뒤 writer import 다음)에 추가(기존 줄은 그대로; 알파벳순 `app.manual` < `app.normalize` 는 무시 — 앵커 단순성 우선):

```python
from app.manual import COUNT_KEYS, ManualCsvError, date_range, load_manual_csvs
```

**(b) 상수** — `MODE_MANUAL = "manual"` 줄 **바로 아래**에 추가(`argparse`는 T6 `main()`이 이미 import; 없으면 `import argparse`를 표준 import 블록에 추가):

```python
MANUAL_INPUT_PREFIX = "MANUAL_INPUT module=token-metrics"   # §5.5 수기 입력 정보 마커(실행당 1줄, 카운트만)
```

**(c) `_run_manual`** — `def main(argv: list[str] | None = None) -> int:` 줄(T6 시그니처 그대로; grep 앵커 `^def main(argv`) **바로 위**(빈 줄 2개 유지; (d)의 헬퍼 2개보다 아래)에 추가:

```python
def _run_manual(cfg: Config, args, entries: list[ServiceEntry], all_entries: list[ServiceEntry],
                started: float, clock=time.monotonic) -> int:
    """manual-v0 (§5.5): CSV 3파일 → (date, service) MetricsPayload → API 와 동일한 normalize/replace 경로.

    - 레지스트리 동기화 없음(register_dims=False — 정기 실행 전용 §4.3), api_since/until 게이트 없음(MODE_MANUAL),
      enabled=0 은 SKIPPED disabled(모든 모드), 앵커 있으면 --replace 없이는 SKIPPED already_loaded(_prepare_one).
    - 페이로드가 있는 (date, service) 만 대상 — 행 없는 (date, service) 는 fetch 하지 않고 앵커도 남기지 않는다
      (6c metrics_missing). 날짜마다 대상 서비스 집합이 달라 _run_dates(고정 entries) 대신 날짜별 run_collection 을
      직접 돌리되 writer·started·outcomes 를 공유해 뮤테이션 장부·소프트 데드라인·BATCH_RESULT 1줄은 _run_dates 와 같다.
    - 뮤테이션 예산 가드·날짜당 replace_batch 1회 배칭·mutation_budget 승격은 run_collection/writer 공통.
    - 파일 계약 위반(ManualCsvError)·날짜 인자 오류·파일 없음은 적재 없이 stderr + exit 2.
    """
    try:
        dates = date_range(args.from_date, args.to_date)
        payloads, counts = load_manual_csvs(
            args.manual_gpu, args.manual_serving, args.manual_engine,
            args.from_date, args.to_date, all_entries, args.service, args.generated_at or "")
    except (ManualCsvError, ValueError, OSError) as exc:
        print(f"manual input error: {exc}", file=sys.stderr)
        return 2
    print(f"{MANUAL_INPUT_PREFIX} " + " ".join(f"{k}={counts[k]}" for k in COUNT_KEYS), flush=True)
    ctx = make_context(cfg, MODE_MANUAL, datetime.now(KST), replace=args.replace, source_type=SOURCE_MANUAL)

    def fetcher(entry: ServiceEntry, target_date: str, _cfg: Config, _session) -> MetricsPayload:
        return payloads[(target_date, entry.service)]                # 대상은 키가 있는 (date, service) 로만 좁힌다

    writer = MetricsWriter(cfg)                                      # 날짜 전체가 1개 writer 공유(뮤테이션 장부 누적)
    all_outcomes: list[ServiceOutcome] = []
    worst = 0
    for d in dates:
        targets = [e for e in entries if (d, e.service) in payloads]  # --service 필터 후 entries 기준(disabled 포함 → gate)
        if not targets:                                              # 그날 행 없음 → fetch·적재·앵커 없음
            continue
        code = run_collection(cfg, targets, d, ctx, clock=clock, fetcher=fetcher, writer=writer,
                              register_dims=False, emit_batch=False, outcomes_sink=all_outcomes, started=started)
        worst = max(worst, code)
    line = _batch_line(all_outcomes, started, clock, ctx, reason=_batch_reason(all_outcomes))
    _batch_status["line"] = line
    print(line, flush=True)
    return worst


```

**(d) argparse 헬퍼** — `_run_manual` 위(`MANUAL_INPUT_PREFIX` 상수 아래, 모듈 레벨)에 추가:

```python
def _add_manual_args(parser: argparse.ArgumentParser) -> None:
    """manual-v0 (§5.5) 전용 인자 4개 — 정기·rerun 인자(batch_time/--from/--to/--service/--replace)는 T6 그대로."""
    parser.add_argument("--manual-gpu", dest="manual_gpu", default=None,
                        help="manual-v0 gpu CSV (§5.5) — --manual-serving 과 쌍, --from/--to 필수")
    parser.add_argument("--manual-serving", dest="manual_serving", default=None,
                        help="manual-v0 serving CSV")
    parser.add_argument("--manual-engine", dest="manual_engine", default=None,
                        help="manual-v0 engine CSV (선택)")
    parser.add_argument("--generated-at", dest="generated_at", default=None,
                        help="manual-v0 generated_at ISO8601 (권장 +09:00; 없으면 적재 시각)")


def _manual_args_error(args: argparse.Namespace) -> str:
    """manual 인자 조합 검증 — 오류 메시지 또는 빈 문자열. 설정 로드·DB 접근 전에 호출된다."""
    if bool(args.manual_gpu) != bool(args.manual_serving):
        return "--manual-gpu/--manual-serving must be given together"
    if args.manual_gpu and not (args.from_date and args.to_date):
        return "manual mode requires --from/--to (KST, YYYY-MM-DD)"
    if (args.manual_engine or args.generated_at) and not args.manual_gpu:
        return "--manual-engine/--generated-at require --manual-gpu/--manual-serving"
    return ""


```

`main()` 안 `--replace`를 추가하는 `parser.add_argument(...)` 문 **바로 아래**(`args = parser.parse_args(argv)` 위)에 한 줄 삽입(`main()` 본문 들여쓰기 4칸): `_add_manual_args(parser)`

**(e) 인자 검증 + 분기** — `args = parser.parse_args(argv)` 줄 **바로 아래**에 삽입(설정 로드·시그널 등록 전 — 잘못된 인자는 DB 를 건드리지 않는다). 아래 블록은 `main()` 본문 들여쓰기(4칸)를 붙여 넣는다:

```python
manual_err = _manual_args_error(args)
if manual_err:
    print(manual_err, file=sys.stderr)
    return 2
```

그리고 `--service` 필터 블록(`unknown service:` stderr + `return 2`) **뒤**, `batch_time = _parse_batch_time(args.batch_time)` 을 감싸는 **`try:` 줄 바로 위**에 분기 삽입(T6 는 `_parse_batch_time`·`_target_dates` 호출을 `try: … except ValueError` 로 감싼다 — `try:` 안(8칸)이 아니라 그 앞, `main()` 본문 들여쓰기 4칸; 정기·rerun 경로는 한 줄도 바뀌지 않는다):

```python
if args.manual_gpu:
    return _run_manual(cfg, args, entries, all_entries, started=time.monotonic())
```

`main()`이 `time.monotonic()`을 이미 `started`에 담아 두었다면 그 변수를 넘긴다(`started=started`). T6 `main()`의 `datetime`·`time`·`sys` import는 이미 있다(`_parse_batch_time`·`_run_dates`·stderr 출력이 쓴다).

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -c "import ast; ast.parse(open('app/main.py').read()); print('ok')" && grep -n "^from app.manual import\|^MANUAL_INPUT_PREFIX\|^def _add_manual_args\|^def _manual_args_error\|^def _run_manual\|    _add_manual_args(parser)\|    manual_err = _manual_args_error(args)\|    if args.manual_gpu:\|return _run_manual" app/main.py`
Expected: `ok` 다음 9줄(import 1·상수 1·def 3·`main()` 삽입 4) — 순서: import < 상수 < `_add_manual_args` < `_manual_args_error` < `_run_manual` < `_add_manual_args(parser)` < `manual_err = …` < `if args.manual_gpu:` < `return _run_manual`.

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -m pytest -q tests/test_main.py -k manual`
Expected: `9 passed`(T7 의 8개 + T6 `test_manual_row_present_warn_regular_only` — 이름에 `manual`이 들어 `-k manual` 에 잡힌다).

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -m app.main --help | grep -c -- "^  --manual-gpu \|^  --manual-serving \|^  --manual-engine \|^  --generated-at " && python3 -m app.main --manual-gpu x.csv; echo "exit=$?"`
Expected: `4`(옵션 정의 줄만 — 앵커 없이 세면 usage 줄·다른 옵션의 help 문장에 든 이름까지 잡혀 4를 넘는다) 다음 stderr `--manual-gpu/--manual-serving must be given together` 와 `exit=2`(설정·DB 접근 없이 종료 — `CH_HOST` 없어도 동작).

- [ ] **Step 9: 전체 테스트 · zero-diff 확인 · 커밋 2 (main manual 모드)**

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 -m pytest -q`
Expected: 전부 PASS(T1~T6 테스트 + `tests/test_manual.py` 25 + `tests/test_main.py` manual 8 추가). 실패가 있으면 이 Task 의 hunk (a)~(e) 만 되짚는다 — 정기·rerun 경로(`_target_dates`·`_run_dates`·`run_collection`)는 이 Task 에서 한 줄도 바꾸지 않았으므로 T6 테스트가 깨졌다면 앵커 위치가 틀린 것이다(분기가 `_parse_batch_time` 뒤로 갔거나 검증이 `parse_args` 앞에 갔을 때).

Run: `git status --porcelain -- collectors/token-usage mart/token-usage assets/user-org assets/model-catalog tools/verify/invariants.sql docs/operations/company-verify.md docs/operations/stage-runbook.md docs/operations/rerun.md docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml docs/templates; git status --porcelain -- collectors/token-metrics`
Expected: 첫 명령 출력 없음(zero-diff 목록·템플릿 3파일 무변경). 둘째 명령은 ` M collectors/token-metrics/app/main.py` 와 ` M collectors/token-metrics/tests/test_main.py` 2줄만.

Run: `cd /home/mini/github/token-data-pipeline && grep -rn "harbor\.\|@" collectors/token-metrics/app/manual.py collectors/token-metrics/app/main.py collectors/token-metrics/tests/test_manual.py | grep -v "example.internal\|noreply@anthropic.com\|@pytest\|@dataclass\|@staticmethod" ; echo "grep exit=$?"`
Expected: 출력 없음 + `grep exit=1`(사내 호스트명·메일 주소 0 — 공개 레포 규칙).

Run:
```bash
cd /home/mini/github/token-data-pipeline && git add collectors/token-metrics/app/main.py collectors/token-metrics/tests/test_main.py && git commit -q -F - <<'MSG'
feat(collectors-metrics): main manual 모드 — --manual-gpu/--manual-serving/--manual-engine/--generated-at·MANUAL_INPUT 마커·동일 replace 경로·동기화 없음 (Plan 6b T7)

- _run_manual: date_range + load_manual_csvs → MANUAL_INPUT 1줄(카운트만) → make_context(MODE_MANUAL, SOURCE_MANUAL)
  → 날짜별 run_collection(register_dims=False, 행 있는 서비스만) + _batch_line: 정기 실행과 같은 normalize·replace_batch·뮤테이션 예산·BATCH_RESULT 경로
- _add_manual_args/_manual_args_error: gpu/serving 쌍 필수, --from/--to 필수, engine/generated-at 단독 → exit 2 (설정·DB 접근 전)
- 파일 계약 위반·파일 없음·역순 범위 → stderr "manual input error: …" + exit 2, 적재 0
- enabled=0 → SKIPPED disabled, 앵커 존재 → --replace 없이는 SKIPPED already_loaded, 행 없는 (date, service) → 페이로드·앵커 없음(6c metrics_missing)
- 정기·rerun 경로 무변경, 레지스트리 동기화 없음(§4.3), 로그에 페이로드·행 내용 없음

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54
MSG
git log --oneline -2
```
Expected: 최상단 2줄이 이 Task 의 커밋 2개(`feat(collectors-metrics): main manual 모드 …`, `feat(collectors-metrics): manual-v0 CSV 파서 …`).

**Task 7 Self-Review 메모 (설계 해석 — 설계서 §5.5/§5.2/§5.6·Plan 6a F 에 명시되지 않아 이 Task 가 확정한 것):**

1. **줄 단위 CSV 파싱**: Plan 6a F 계약(`#` 주석 줄은 콤마 무시·첫 비주석 줄이 헤더·헤더 바이트 일치)은 "줄"을 단위로 정의하므로 셀 안 개행(따옴표로 감싼 줄바꿈)은 지원하지 않는다 — 한 줄을 `csv.reader([line])`로 읽어 따옴표·이스케이프는 처리하되 개행은 행 구분자로만 본다. 템플릿 3파일에는 셀 안 개행이 없다.
2. **검증 순서 = 등록 대조 → 날짜 형식 → metric 키 → 중복(model, metric) → 범위/서비스 필터**: `--from/--to`·`--service`로 걸러지는 행도 형식 검증은 통과해야 한다(파일 계약 위반은 행 위치와 무관하게 파일 전체를 거부, 적재 0). `rows_other_service`가 `rows_outside_range`보다 먼저 판정된다(한 행은 한 카운터에만 들어간다).
3. **카운터 의미**: `rows_gpu/rows_serving` = 필터를 통과해 페이로드에 들어간 행 수, `rows_engine` = engine 파일의 데이터 행 수 전체(engine 파일은 날짜가 없고 서비스별 1행이므로 필터 대상이 아니다), `rows_outside_range/rows_other_service` = gpu+serving 합계. 로그에는 카운트만 싣고 행 내용·페이로드는 싣지 않는다(§5.6).
4. **값 검증은 normalize 에 위임**: 숫자 셀은 `_num`이 int/float 로 바꾸되 실패하면 원문 문자열을 그대로 넘겨 `normalize_payload`가 API 응답과 같은 규칙(T3 `normalize_gpu`/`normalize_serving` — 비숫자·음수·`gpuCount ≤ 0`·p값 누락 등은 이름 붙은 규칙 코드 없이 `NormalizeResult.rejected` 카운터로만 집계된다)으로 rejected 처리한다 — CSV 경로에 별도 값 규칙을 두지 않는다(§5.5 "동일 normalize 경로"). 빈 pct 셀은 키 자체를 넣지 않고(부재), 빈 `name`은 `""`로 넘겨 custom 이름 필수 규칙을 normalize 가 판정한다.
5. **표준 metric 중복 = 파일 오류, custom 중복 = 허용**: 같은 (date, service, model) 에서 `ttftMs`가 두 번 나오면 어느 쪽이 맞는지 알 수 없으므로 파일을 거부한다. `custom`은 `name`이 다르면 여러 행이 정상이고, `name`까지 같은 중복은 normalize 가 첫 항목을 유지하고 나머지를 버리며 그 행에 `dup_custom_kept_first` 플래그(T3 `F_DUP_CUSTOM`, 카운트 = 버린 항목 수)를 남긴다 — 거부가 아니다.
6. **`--manual-engine`·`--generated-at` 단독 지정 → exit 2**: manual 페어 없이 넘어온 manual 전용 인자는 오타로 보고 정기 실행으로 빠지지 않게 막는다(정기 실행 중 실수로 적재 시각을 덮어쓰는 사고 방지).
7. **`(date, service)` 페이로드는 행이 있는 쌍에만 생성**: 범위 안 날짜 × 서비스 조합 중 채택된 gpu∪serving 행이 1건 이상인 `(date, service)`만 키를 만든다 — 행 없는 (date, service) 는 페이로드도 앵커도 없다(6c `metrics_missing` 불변식이 "수기 입력 없음"으로 본다, §4.3). CSV 에 없는 날짜에 NODATA 앵커를 심지 않는 이유: 수기 CSV 에서는 "그날 서비스가 실제로 0행"과 "담당자가 그날을 제출하지 않음"을 구분할 수 없고, `--from/--to` 범위만으로 전 조합에 앵커를 만들면 미제출 날짜가 완결(NODATA)로 굳어 이후 API 정기 실행이 `already_loaded` 로 건너뛰게 된다. 정말로 0행인 날을 완결로 남기는 것은 API 경로(NODATA 응답)의 몫이다.
8. **writer·컨텍스트는 T6 공용**: `make_context(MODE_MANUAL, SOURCE_MANUAL)` + 날짜별 `run_collection(register_dims=False, emit_batch=False, outcomes_sink=…)` 로 T6 의 gate(disabled·already_loaded)·배칭·예산 승격을 그대로 쓰고, BATCH_RESULT 1줄은 `_run_dates`와 같은 `_batch_line(all_outcomes, …)` 호출로 찍는다(`_run_dates`를 그대로 쓰지 않는 이유: 날짜마다 대상 서비스 집합이 다르다 — 행이 있는 서비스만). manual 모드가 `_prepare_one`·`run_collection`·`_run_dates`에 새 분기를 추가하지 않으므로 정기·rerun 경로는 zero-diff.
9. **`--replace` 는 manual 에서도 T6 의미 그대로**: 앵커 존재 시 `--replace` 없으면 `SKIPPED already_loaded`, 있으면 `replace_batch` 로 (date, service) 전체 재적재. 수기 → API 전환일에는 API 정기 실행이 manual 앵커를 덮지 않으므로(§5.5 "재적재는 --replace 명시") 운영 절차(Plan 6c 문서)가 `--replace` 를 명시한다.

---

### Task 8: 배포 계층 — Dockerfile·build.sh·k8s base CronJob §5.2·overlays(stage/company/company-verify)·install.sh 7단계 프리플라이트·tests/test_manifests.py

**Files:**
- Create: `collectors/token-metrics/Dockerfile`
- Create: `collectors/token-metrics/build.sh` (chmod +x)
- Create: `collectors/token-metrics/install.sh` (chmod +x)
- Create: `collectors/token-metrics/k8s/base/cronjob.yaml`
- Create: `collectors/token-metrics/k8s/base/kustomization.yaml`
- Create: `collectors/token-metrics/k8s/overlays/stage/kustomization.yaml`
- Create: `collectors/token-metrics/k8s/overlays/company/kustomization.yaml`
- Create: `collectors/token-metrics/k8s/overlays/company-verify/kustomization.yaml`
- Modify: 없음 (기존 모듈·워크플로 무수정 — zero-diff §7.5)
- Test: `collectors/token-metrics/tests/test_manifests.py` (pytest — kubectl 없이 `yaml.safe_load` + 문자열 검사; kubectl이 PATH에 있으면 `kubectl kustomize` 렌더 검사 추가, 없으면 그 테스트만 skip)

**Interfaces:**
- Consumes:
  - T2 `app/config.py::load_config()`가 읽는 env 이름: `CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_CLUSTER, ENDPOINTS_FILE, SOFT_DEADLINE_MINUTES, LOAD_BUDGET_S, FINAL_HOUR_KST, MAX_RESPONSE_BYTES, METRICS_MAX_MUTATIONS_PER_RUN, COLLECTOR_HTTPS_PROXY, COLLECTOR_API_VERIFY, COLLECTOR_API_CA_BUNDLE`(매니페스트는 `COLLECTOR_API_VERIFY`를 설정하지 않는다 — 기본 True); T5 `app/writer.py` 모듈 상수 env `CH_DB_FACT`(기본 `fact`)/`CH_DB_DIM`(기본 `gpu_data`).
  - T6 `python -m app.main` CLI(인자 없이 = 정기 모드; Dockerfile `CMD`), T9 `tools/rerun.py --context C --namespace N [--cronjob token-metrics-collector] --from D0 --to D1`, T10 `tools/manual_load.py --context C --namespace N --from D0 --to D1 --gpu F --serving F [--engine F]`(install.sh [7/7] 안내 문구만 — 이 태스크는 두 파일을 만들지 않는다).
  - Plan 6a T3~T7 산출 DDL: `collectors/token-metrics/ddl/{company,stage,company-verify}/{raw_token_metrics,dim_token_metrics_service,accounts}.sql`(install.sh [5/7]는 앞 2파일만 `apply_sql`; `accounts.sql`은 admin 수동 — 설계 §4.0 매니페스트).
  - 기존 클러스터 자산(읽기 전용 접점, §5.1): `gpu_data.dim_token_service_dist`(company-verify는 `token_verify_dim.dim_token_service_dist`) — 프리플라이트 SELECT 대상.
- Produces (T9·T10·T11·T12·Plan 6c 소비):
  - 이미지 `token-metrics-collector`(`build.sh` `IMAGE_NAME="token-metrics-collector"`; stage 레지스트리 `ghcr.io/yoonsungnam/token-metrics-collector`). 컨테이너 `name: token-metrics-collector`, base 이미지 `token-metrics-collector:latest`(kustomize `images` 치환 대상; `kubectl set image cronjob/token-metrics-collector token-metrics-collector=<image>` 계약).
  - CronJob `token-metrics-collector`(namespace는 매니페스트에 고정하지 않음 — install.sh `-n`, 기본 `monitoring`): `schedule: "5 2-9 * * *"`, `timeZone: Asia/Seoul`, `concurrencyPolicy: Forbid`, `startingDeadlineSeconds: 540`, `successfulJobsHistoryLimit: 3`, `failedJobsHistoryLimit: 3`, `jobTemplate.spec.backoffLimit: 0`, `jobTemplate.spec.activeDeadlineSeconds: 3000`, `restartPolicy: Never`, `imagePullSecrets: [{name: registry-pull-secret}]`, `resources: requests {cpu: 100m, memory: 256Mi} limits {cpu: "1", memory: 1Gi}`. 라벨 `app: token-metrics-collector`를 CronJob·`jobTemplate.metadata`·`template.metadata` 3곳에(T9/T10이 `kubectl get jobs -l app=token-metrics-collector`로 조회 가능).
  - 컨테이너 `envFrom: [{secretRef: {name: token-metrics-ch-secret}}]`; `env`(리터럴, 순서 고정): `ENDPOINTS_FILE=/etc/token-metrics/endpoints.yaml`, `SOFT_DEADLINE_MINUTES="40"`, `LOAD_BUDGET_S="1200"`, `FINAL_HOUR_KST="9"`, `MAX_RESPONSE_BYTES="5000000"`, `METRICS_MAX_MUTATIONS_PER_RUN="45"`. `CH_HOST`는 매니페스트에 없음 — install.sh [7/7]가 `kubectl set env`로 주입.
  - volumes 순서 계약: `[0] endpoints`(configMap `token-metrics-endpoints`, mount `/etc/token-metrics`, readOnly) · `[1] ca-bundle`(configMap `token-metrics-ca-bundle`, `optional: true`, mount `/etc/token-metrics-ca`, readOnly). T10 `manual_load.py`는 `[2] manual`(mount `/manual`)을 **append**한다.
  - Secret `token-metrics-ch-secret` 키: `CH_USER`(프롬프트 기본 `mart`; company-verify `token_verify`), `CH_PASSWORD`, `CH_PORT=8123`, `CH_CLUSTER`(company·company-verify `gpu-monitoring`, stage `""`), company-verify에서만 `CH_DB_FACT`/`CH_DB_DIM`(프롬프트 기본 `token_verify_fact`/`token_verify_dim`; stage/company는 키 없음 = 앱 기본 `fact`/`gpu_data`), `COLLECTOR_HTTPS_PROXY`(`none` → 빈 값 = 직접 연결 / enter → 키 미생성 = 시스템 상속 / 값), CA 파일을 입력했을 때만 `COLLECTOR_API_CA_BUNDLE=/etc/token-metrics-ca/ca-bundle.pem` + ConfigMap `token-metrics-ca-bundle`(`--from-file=ca-bundle.pem=<file>`).
  - ConfigMap `token-metrics-endpoints` 키 `endpoints.yaml`(`--from-file=endpoints.yaml=<src>`; stage 기본 `${HERE}/endpoints.yaml`, company·company-verify 기본 `${HERE}/endpoints-metrics.company.yaml`(gitignored — Plan 6a G)).
  - company-verify overlay: `nameSuffix: -verify` → CronJob `token-metrics-collector-verify`; JSON 패치 2건으로 secretRef → `token-metrics-ch-secret-verify`, `volumes[0].configMap` → `token-metrics-endpoints-verify`. ca-bundle ConfigMap은 접미 없이 `token-metrics-ca-bundle` 공용.
  - `./collectors/token-metrics/build.sh [--registry R] [--tag T] <stage|company>`(기존 모듈과 동일 규약 — 항상 build+push; company-verify는 company 이미지 그대로 사용).
  - `./collectors/token-metrics/install.sh [--registry R] [--tag T] [--context C] [--namespace N] [--endpoints F] <stage|company|company-verify>` — 단계 `[1/7]`~`[7/7]`: [1/7] `registry-pull-secret` **없을 때만** 생성(있으면 `"  이미 존재합니다 — 네임스페이스 공유 Secret이므로 갱신하지 않습니다"` 출력, 프롬프트 없음) → [2/7] app Secret(있으면 갱신 여부 프롬프트) → [3/7] endpoints ConfigMap → [4/7] **프리플라이트**(chi-* 파드 탐색 → `clickhouse-client --user "${ch_user}" --password "${ch_pass}"` 즉 앱 계정으로 `SELECT name FROM system.databases WHERE name IN ('${DB_FACT}','${DB_DIM}')` 행 수 2 아니면 `[ERROR] 프리플라이트 실패: DB 부재 또는 GRANT 누락 — admin이 <ddl dir>/accounts.sql 실행 필요` exit 1; `SELECT count() FROM ${DB_DIM}.dim_token_service_dist` 실패 시 `[ERROR] 프리플라이트 실패: 토큰 레지스트리 SELECT 불가(GRANT 누락) — admin이 <ddl dir>/accounts.sql 실행 필요` exit 1; [2/7]을 건너뛴 재설치에서는 기존 Secret의 `CH_USER`/`CH_PASSWORD`를 읽어 쓴다) → [5/7] `apply_sql "${HERE}/${DDL_DIR}/raw_token_metrics.sql"` + `apply_sql "${HERE}/${DDL_DIR}/dim_token_metrics_service.sql"`(`DDL_DIR` = `ddl/company` | `ddl/stage` | `ddl/company-verify`) → [6/7] `kubectl apply -k "${HERE}/k8s/overlays/${ENV}" -n NS` → [7/7] `set image cronjob/${CRONJOB_NAME} token-metrics-collector=${REGISTRY}/token-metrics-collector:${TAG}` + `set env cronjob/${CRONJOB_NAME} CH_HOST=${ch_pod%-*}.clickhouse.svc` + 수동 테스트 안내. VM 관련 단계 없음.
  - `install.sh` 상수: `IMAGE_NAME="token-metrics-collector"`, `CRONJOB_NAME="token-metrics-collector"`, `SECRET_NAME="token-metrics-ch-secret"`, `PULL_SECRET_NAME="registry-pull-secret"`, `CONFIGMAP_NAME="token-metrics-endpoints"`, `CA_CONFIGMAP_NAME="token-metrics-ca-bundle"`, `CH_NAMESPACE="clickhouse"`; 프리플라이트 DB 변수 `DB_FACT`/`DB_DIM`(기본 `fact`/`gpu_data`, company-verify `token_verify_fact`/`token_verify_dim`, Secret 입력값·기존 Secret 값이 있으면 그 값).
  - CI 계약(T11 `test-collector-metrics.yml` manifests job이 렌더 후 grep): `schedule: 5 2-9 \* \* \*`, `timeZone: Asia/Seoul`, `startingDeadlineSeconds: 540`, `activeDeadlineSeconds: 3000`, `backoffLimit: 0`, `concurrencyPolicy: Forbid`, `memory: 1Gi`, `memory: 256Mi`, `name: registry-pull-secret`, `name: token-metrics-ch-secret`, `name: token-metrics-endpoints`, `name: token-metrics-ca-bundle`, `METRICS_MAX_MUTATIONS_PER_RUN`; company-verify 렌더 `name: token-metrics-collector-verify`·`token-metrics-ch-secret-verify`·`token-metrics-endpoints-verify`; stage 렌더 `ghcr.io/yoonsungnam/token-metrics-collector`; 모든 렌더 `token-usage` 0건.

- [ ] **Step 0: 전제 확인 — Plan 6a 산출물(모듈 골격·DDL)과 T2/T6 산출물이 브랜치에 있는지**

```bash
cd /home/mini/github/token-data-pipeline
ls collectors/token-metrics/requirements.txt collectors/token-metrics/app/main.py collectors/token-metrics/app/config.py
ls collectors/token-metrics/ddl/company/raw_token_metrics.sql collectors/token-metrics/ddl/company/dim_token_metrics_service.sql collectors/token-metrics/ddl/company/accounts.sql
ls collectors/token-metrics/ddl/stage/raw_token_metrics.sql collectors/token-metrics/ddl/stage/dim_token_metrics_service.sql
ls collectors/token-metrics/ddl/company-verify/raw_token_metrics.sql collectors/token-metrics/ddl/company-verify/dim_token_metrics_service.sql collectors/token-metrics/ddl/company-verify/accounts.sql
ls collectors/token-metrics/k8s collectors/token-metrics/Dockerfile collectors/token-metrics/build.sh collectors/token-metrics/install.sh 2>&1 | head -5
```

기대: 앞 네 `ls`는 파일 경로를 그대로 출력(모두 존재). 마지막 `ls`는 `No such file or directory` 4건 — 이 태스크가 만드는 파일이 아직 없음을 확인한다. 앞 네 `ls` 중 하나라도 `No such file or directory`가 나오면 **여기서 중단**(Plan 6a 미머지 또는 T2/T6 미완료 — 이 태스크는 그 산출물 위에서만 진행).

- [ ] **Step 1: 실패하는 매니페스트 계약 테스트 작성 — `tests/test_manifests.py`**

배포 파일이 하나도 없는 상태에서 먼저 테스트 전체를 쓴다. kubectl 없이도 돌아가는 부분(`yaml.safe_load` + 문자열 계약)이 본체이고, `kubectl`이 PATH에 있을 때만 `kubectl kustomize` 렌더 검사를 추가로 수행한다(없으면 그 1개만 skip).

```python
"""배포 계층 계약 잠금 (설계 §5.2 CronJob 수치·§5.6 이름·§7.5 zero-diff).

kubectl 없이 YAML 텍스트를 직접 파싱해 검사한다(CI unit job). kubectl이 PATH에 있으면
kustomize 렌더 결과도 추가로 검사한다(없으면 그 테스트만 skip — CI manifests job이 동일 grep을 수행).
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

HERE = Path(__file__).resolve().parent.parent
BASE_CRONJOB = HERE / "k8s" / "base" / "cronjob.yaml"
BASE_KUSTOMIZATION = HERE / "k8s" / "base" / "kustomization.yaml"
OVERLAYS = HERE / "k8s" / "overlays"
APP = "token-metrics-collector"

EXPECTED_ENV = {
    "ENDPOINTS_FILE": "/etc/token-metrics/endpoints.yaml",
    "SOFT_DEADLINE_MINUTES": "40",
    "LOAD_BUDGET_S": "1200",
    "FINAL_HOUR_KST": "9",
    "MAX_RESPONSE_BYTES": "5000000",
    "METRICS_MAX_MUTATIONS_PER_RUN": "45",
}


def _load(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _cronjob() -> dict:
    return _load(BASE_CRONJOB)


def _pod_spec(cj: dict) -> dict:
    return cj["spec"]["jobTemplate"]["spec"]["template"]["spec"]


def _container(cj: dict) -> dict:
    containers = _pod_spec(cj)["containers"]
    assert len(containers) == 1
    return containers[0]


def _env(cj: dict) -> dict:
    return {e["name"]: e["value"] for e in _container(cj)["env"]}


def _resolve_pointer(obj, pointer: str):
    """JSON pointer(/a/0/b)를 dict/list에 적용 — overlay 패치 경로가 base 구조와 맞는지 검사용."""
    for part in pointer.strip("/").split("/"):
        obj = obj[int(part)] if isinstance(obj, list) else obj[part]
    return obj


def test_cronjob_spec_values():
    cj = _cronjob()
    assert cj["apiVersion"] == "batch/v1" and cj["kind"] == "CronJob"
    assert cj["metadata"]["name"] == APP
    assert cj["metadata"]["labels"] == {"app": APP}
    spec = cj["spec"]
    assert spec["schedule"] == "5 2-9 * * *"
    assert spec["timeZone"] == "Asia/Seoul"
    assert spec["concurrencyPolicy"] == "Forbid"
    assert spec["startingDeadlineSeconds"] == 540
    assert spec["successfulJobsHistoryLimit"] == 3
    assert spec["failedJobsHistoryLimit"] == 3
    assert spec["jobTemplate"]["metadata"]["labels"] == {"app": APP}
    job = spec["jobTemplate"]["spec"]
    assert job["backoffLimit"] == 0
    assert job["activeDeadlineSeconds"] == 3000
    assert job["template"]["metadata"]["labels"] == {"app": APP}
    assert _pod_spec(cj)["restartPolicy"] == "Never"


def test_slot_arithmetic_locked():
    # §5.2 슬롯 산식: 지연 시작 ≤540 + activeDeadlineSeconds 3000 + grace 30 = 3570 < 3600
    cj = _cronjob()
    starting = cj["spec"]["startingDeadlineSeconds"]
    active = cj["spec"]["jobTemplate"]["spec"]["activeDeadlineSeconds"]
    assert starting + active + 30 < 3600
    # activeDeadlineSeconds = SOFT_DEADLINE_MINUTES×60 + 종료 마진 600; SOFT×60 > LOAD_BUDGET_S (test_config.py와 동일 불변식)
    env = _env(cj)
    assert int(env["SOFT_DEADLINE_MINUTES"]) * 60 + 600 == active
    assert int(env["SOFT_DEADLINE_MINUTES"]) * 60 > int(env["LOAD_BUDGET_S"])


def test_container_name_image_and_env():
    cj = _cronjob()
    c = _container(cj)
    assert c["name"] == APP
    assert c["image"].split(":")[0] == APP  # kustomize images 치환 대상 이름
    assert c["imagePullPolicy"] == "Always"
    assert c["envFrom"] == [{"secretRef": {"name": "token-metrics-ch-secret"}}]
    assert [e["name"] for e in c["env"]] == list(EXPECTED_ENV)  # 순서 고정
    assert _env(cj) == EXPECTED_ENV
    assert all(isinstance(e["value"], str) for e in c["env"])  # k8s env value는 문자열
    assert "CH_HOST" not in _env(cj)  # install.sh [7/7] set env가 주입
    assert "VM_PUSH_URL" not in _env(cj)  # VM push 없음 (§5.2)


def test_volumes_order_and_names():
    cj = _cronjob()
    ps = _pod_spec(cj)
    assert ps["volumes"] == [
        {"name": "endpoints", "configMap": {"name": "token-metrics-endpoints"}},
        {"name": "ca-bundle", "configMap": {"name": "token-metrics-ca-bundle", "optional": True}},
    ]
    assert _container(cj)["volumeMounts"] == [
        {"name": "endpoints", "mountPath": "/etc/token-metrics", "readOnly": True},
        {"name": "ca-bundle", "mountPath": "/etc/token-metrics-ca", "readOnly": True},
    ]
    assert ps["imagePullSecrets"] == [{"name": "registry-pull-secret"}]
    assert _container(cj)["resources"] == {
        "requests": {"cpu": "100m", "memory": "256Mi"},
        "limits": {"cpu": "1", "memory": "1Gi"},
    }


def test_base_kustomization():
    assert _load(BASE_KUSTOMIZATION)["resources"] == ["cronjob.yaml"]


def test_stage_overlay_image():
    k = _load(OVERLAYS / "stage" / "kustomization.yaml")
    assert k["resources"] == ["../../base"]
    assert "namespace" not in k
    assert k["images"] == [{
        "name": APP,
        "newName": "ghcr.io/yoonsungnam/token-metrics-collector",
        "newTag": "latest",
    }]


def test_company_overlay_is_resources_only():
    k = _load(OVERLAYS / "company" / "kustomization.yaml")
    assert k["resources"] == ["../../base"]
    assert "namespace" not in k and "images" not in k and "nameSuffix" not in k and "patches" not in k


def test_company_verify_overlay_names():
    k = _load(OVERLAYS / "company-verify" / "kustomization.yaml")
    assert k["nameSuffix"] == "-verify"
    assert k["resources"] == ["../../base"]
    assert "namespace" not in k and "images" not in k
    assert len(k["patches"]) == 1
    assert k["patches"][0]["target"] == {"kind": "CronJob", "name": APP}
    ops = yaml.safe_load(k["patches"][0]["patch"])
    assert ops == [
        {"op": "replace",
         "path": "/spec/jobTemplate/spec/template/spec/containers/0/envFrom/0/secretRef/name",
         "value": "token-metrics-ch-secret-verify"},
        {"op": "replace",
         "path": "/spec/jobTemplate/spec/template/spec/volumes/0/configMap/name",
         "value": "token-metrics-endpoints-verify"},
    ]
    # 패치 경로가 base 구조를 정확히 가리킨다 (volumes[0]=endpoints 계약)
    cj = _cronjob()
    assert _resolve_pointer(cj, ops[0]["path"]) == "token-metrics-ch-secret"
    assert _resolve_pointer(cj, ops[1]["path"]) == "token-metrics-endpoints"


def _deploy_files() -> list:
    yamls = sorted((HERE / "k8s").rglob("*.yaml"))
    assert len(yamls) == 5, yamls  # base 2 + overlays 3
    return [HERE / "Dockerfile", HERE / "build.sh", HERE / "install.sh", *yamls]


def test_no_token_usage_names_anywhere():
    # 기존 모듈 이름·리소스 참조 0 (§7.5 zero-diff / §5.1 이름 전면 교체). 공유는 registry-pull-secret뿐.
    for path in _deploy_files():
        text = path.read_text(encoding="utf-8")
        for forbidden in ("token-usage", "VM_PUSH_URL", "vminsert", "vmsingle", "raw_token_usage"):
            assert forbidden not in text, f"{path.name}: {forbidden}"
    assert "registry-pull-secret" in BASE_CRONJOB.read_text(encoding="utf-8")


def test_dockerfile_and_build_sh_contract():
    d = (HERE / "Dockerfile").read_text(encoding="utf-8")
    assert "ARG BASE_IMAGE=python:3.12-slim" in d
    assert "FROM ${BASE_IMAGE}" in d
    assert "COPY requirements.txt ." in d and "COPY app/ ./app/" in d
    assert 'CMD ["python", "-m", "app.main"]' in d
    assert not any(l.startswith("COPY") and "endpoints" in l for l in d.splitlines())  # ConfigMap이 정본
    b = (HERE / "build.sh").read_text(encoding="utf-8")
    assert 'IMAGE_NAME="token-metrics-collector"' in b
    assert 'REGISTRY="ghcr.io/yoonsungnam"' in b
    assert "harbor.example.internal" in b  # 공개 레포: 사내 주소는 플레이스홀더만
    assert "./collectors/token-metrics/install.sh" in b


def test_install_sh_contract():
    text = (HERE / "install.sh").read_text(encoding="utf-8")
    for needle in (
        'IMAGE_NAME="token-metrics-collector"',
        'CRONJOB_NAME="token-metrics-collector"',
        'SECRET_NAME="token-metrics-ch-secret"',
        'PULL_SECRET_NAME="registry-pull-secret"',
        'CONFIGMAP_NAME="token-metrics-endpoints"',
        'CA_CONFIGMAP_NAME="token-metrics-ca-bundle"',
        'CH_NAMESPACE="clickhouse"',
        "set -euo pipefail",
        "[1/7]", "[2/7]", "[3/7]", "[4/7]", "[5/7]", "[6/7]", "[7/7]",
        "이미 존재합니다 — 네임스페이스 공유 Secret이므로 갱신하지 않습니다",
        "system.databases",
        "프리플라이트 실패: DB 부재",
        "dim_token_service_dist",
        "프리플라이트 실패: 토큰 레지스트리 SELECT 불가",
        'clickhouse-client --user "${ch_user}" --password "${ch_pass}" --query "$1"',   # 프리플라이트는 앱 계정으로(GRANT 검증)
        "jsonpath='{.data.CH_USER}'",                                                   # [2/7] 건너뛴 재설치도 앱 계정을 읽는다
        'apply_sql "${HERE}/${DDL_DIR}/raw_token_metrics.sql"',
        'apply_sql "${HERE}/${DDL_DIR}/dim_token_metrics_service.sql"',
        "endpoints-metrics.company.yaml",
        "token_verify_fact", "token_verify_dim",
        "COLLECTOR_API_CA_BUNDLE=/etc/token-metrics-ca/ca-bundle.pem",
        'ch_host="${ch_pod%-*}.${CH_NAMESPACE}.svc"',
        "harbor.example.internal",
    ):
        assert needle in text, needle
    assert re.search(r'apply_sql\s+"[^"]*accounts\.sql"', text) is None  # accounts.sql은 admin 수동
    assert text.count('apply_sql "${HERE}/${DDL_DIR}/') == 2
    assert "[1/6]" not in text and "[8/7]" not in text
    assert "dim_token_service.sql" not in text  # 기존 모듈 DDL 파일 — 여기서 적용하지 않음
    lines = text.splitlines()
    assert lines[0] == "#!/usr/bin/env bash"
    assert all(l.startswith("#") for l in lines[1:14])  # usage()의 sed -n '2,14p' 범위와 정합
    assert "sed -n '2,14p'" in text


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not on PATH")
def test_scripts_executable_and_bash_syntax():
    for name in ("build.sh", "install.sh"):
        path = HERE / name
        assert os.access(path, os.X_OK), f"{name} must be chmod +x"
        subprocess.run(["bash", "-n", str(path)], check=True)
    # 인자 없이 → usage + exit 1 (kubectl 불필요 경로)
    r = subprocess.run(["bash", str(HERE / "install.sh")], capture_output=True, text=True)
    assert r.returncode == 1 and "사용법" in r.stdout
    r = subprocess.run(["bash", str(HERE / "install.sh"), "company"], capture_output=True, text=True)
    assert r.returncode == 1 and "--context" in r.stdout


@pytest.mark.skipif(shutil.which("kubectl") is None, reason="kubectl not on PATH")
def test_kustomize_render_if_available():
    def render(overlay: str) -> str:
        return subprocess.run(["kubectl", "kustomize", str(OVERLAYS / overlay)],
                              check=True, capture_output=True, text=True).stdout

    rendered = {o: render(o) for o in ("stage", "company", "company-verify")}
    for overlay, out in rendered.items():
        for needle in (
            "schedule: 5 2-9 * * *", "timeZone: Asia/Seoul", "concurrencyPolicy: Forbid",
            "startingDeadlineSeconds: 540", "activeDeadlineSeconds: 3000", "backoffLimit: 0",
            "successfulJobsHistoryLimit: 3", "memory: 1Gi", "memory: 256Mi",
            "name: registry-pull-secret", "name: token-metrics-ca-bundle",
            "name: METRICS_MAX_MUTATIONS_PER_RUN",
        ):
            assert needle in out, f"{overlay}: {needle}"
        assert "token-usage" not in out, overlay
    assert "ghcr.io/yoonsungnam/token-metrics-collector:latest" in rendered["stage"]
    assert "image: token-metrics-collector:latest" in rendered["company"]
    assert "name: token-metrics-ch-secret\n" in rendered["company"]
    assert "name: token-metrics-endpoints\n" in rendered["company"]
    verify = rendered["company-verify"]
    assert "name: token-metrics-collector-verify" in verify
    assert "name: token-metrics-ch-secret-verify" in verify
    assert "name: token-metrics-endpoints-verify" in verify
    assert "name: token-metrics-ca-bundle-verify" not in verify  # ca-bundle은 접미 없이 공용
```

- [ ] **Step 1-실행: 테스트가 "파일 없음"으로 실패하는지 확인**

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
python3 -m pytest -q tests/test_manifests.py 2>&1 | tail -n 20
```

기대(요약 줄): `13 failed in …` — 개별 실패 사유는 `FileNotFoundError: [Errno 2] No such file or directory: '…/k8s/base/cronjob.yaml'`(cronjob 계열 7건), `AssertionError: []`(`_deploy_files` — yaml 5개 기대), `FileNotFoundError … Dockerfile`/`install.sh`, `AssertionError: build.sh must be chmod +x`, `subprocess.CalledProcessError … 'kubectl', 'kustomize'`(kubectl 있는 환경) 또는 `12 failed, 1 skipped`(kubectl 없는 환경). `passed`가 1건이라도 있으면 테스트가 파일 부재를 잡지 못하는 것이므로 여기서 멈추고 테스트를 고친다.

- [ ] **Step 2: Dockerfile + build.sh — 기존 수집기와 동일 패턴(§5.1 클론), 이름만 전면 교체**

`collectors/token-metrics/Dockerfile`:

```dockerfile
# token-metrics-collector 이미지 (설계 §5.1 클론 규칙 — 기존 수집기 Dockerfile과 동일 패턴).
# python:3.12-slim, requirements 선복사 캐시, 이미지 1개 + CronJob command 교체(rerun/manual Job).
# BASE_IMAGE는 company 빌드에서 Harbor proxy로 치환된다 (build.sh --registry 경로).
ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
# endpoints.yaml은 이미지에 굽지 않는다 — ConfigMap 마운트 + ENDPOINTS_FILE env가 정본 (§5.6)
CMD ["python", "-m", "app.main"]
```

`collectors/token-metrics/build.sh` — 기존 모듈 build.sh와 같은 규약(`--registry/--tag` + `stage|company`, 항상 build+push). company-verify는 별도 이미지가 없다(company 이미지를 install.sh `company-verify`가 그대로 사용).

```bash
#!/usr/bin/env bash
# token-metrics collector 이미지 빌드/푸시 (설계 §7.5 — 신규 이미지 token-metrics-collector만; 기존 이미지 재빌드 없음)
#
# 사용법:
#   ./collectors/token-metrics/build.sh [--registry <registry>] [--tag <tag>] <stage|company>
#
#   stage:   REGISTRY 기본 ghcr.io/yoonsungnam
#   company: --registry 필수 (사내 Harbor, 예: harbor.example.internal/gpu-monitoring) — BASE_IMAGE를 Harbor proxy로 치환
#   태그 기본: git short SHA (git 밖이면 latest). company-verify는 company 이미지를 그대로 쓴다.
set -euo pipefail
SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
cd "$(dirname "${SCRIPT_PATH}")"

IMAGE_NAME="token-metrics-collector"
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
  # 사내망은 docker hub 직접 pull 불가 — Harbor pull-through proxy 경유 (기존 모듈과 동일 관례)
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
echo "  ./collectors/token-metrics/install.sh --registry ${REGISTRY} --tag ${TAG} ${ENV}"
```

실행 권한과 문법·usage 스모크:

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
chmod +x build.sh
bash -n build.sh && echo SYNTAX_OK
./build.sh; echo "rc=$?"
./build.sh company; echo "rc=$?"
grep -c "token-usage" Dockerfile build.sh
```

기대: `SYNTAX_OK`; 인자 없는 호출은 헤더 주석(사용법 6줄, `# token-metrics collector 이미지 빌드/푸시 …`부터)을 출력하고 `rc=1`; `./build.sh company`는 `[ERROR] company 환경에서는 --registry 옵션이 필수입니다.` + 사용법 후 `rc=1`; 마지막 grep은 `Dockerfile:0` `build.sh:0`.

- [ ] **Step 3: k8s base — CronJob §5.2 계약 + kustomization**

`collectors/token-metrics/k8s/base/cronjob.yaml` — 수치는 설계 §5.2 그대로. `activeDeadlineSeconds`·`startingDeadlineSeconds`·env 리터럴은 서로 산식으로 묶여 있으므로(테스트 `test_slot_arithmetic_locked`) 하나만 바꾸면 테스트가 깨진다.

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: token-metrics-collector
  labels:
    app: token-metrics-collector
spec:
  # 매시 05분, 02~09시 KST = 하루 8슬롯 (설계 §5.2) — 02:05 첫 슬롯, 09:05 최종 슬롯(FINAL_HOUR_KST=9)
  schedule: "5 2-9 * * *"
  timeZone: Asia/Seoul
  # §5.4 존재확인 → DELETE → INSERT 시퀀스의 전제: 단일 작성자 (경합 금지)
  concurrencyPolicy: Forbid
  # 슬롯 산식(§5.2): 지연 시작 ≤540 + activeDeadlineSeconds 3000 + grace 30 = 3570s < 3600s
  # → Forbid가 다음 슬롯을 건너뛰지 않는다. 산식과 연동된 값 — 단독 수정 금지
  startingDeadlineSeconds: 540
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    metadata:
      labels:
        app: token-metrics-collector
    spec:
      # 0: 슬롯당 시도 1회 — 재시도는 다음 슬롯이 담당 (§5.2; 1 실행 = 1 BATCH_RESULT 줄 slot=HH)
      backoffLimit: 0
      # §5.2 산식 계약: SOFT_DEADLINE_MINUTES 40×60 = 2400s(그 안에 LOAD_BUDGET_S 1200 예약) + 종료 마진 600s
      # 산식과 연동된 값 — 단독 수정 금지 (env 리터럴과 함께 tests/test_manifests.py가 고정)
      activeDeadlineSeconds: 3000
      template:
        metadata:
          labels:
            app: token-metrics-collector
        spec:
          # Never: 파드 로그가 실행 단위와 1:1 — BATCH_RESULT '1 실행 = 1 마커 라인' 소비 (§5.2)
          restartPolicy: Never
          imagePullSecrets:
            # 네임스페이스 공유 Secret — install.sh는 없을 때만 생성 (§7.5)
            - name: registry-pull-secret
          containers:
            - name: token-metrics-collector
              image: token-metrics-collector:latest
              imagePullPolicy: Always
              envFrom:
                # CH_PORT/CH_USER/CH_PASSWORD/CH_CLUSTER[/CH_DB_FACT/CH_DB_DIM][/COLLECTOR_HTTPS_PROXY/COLLECTOR_API_CA_BUNDLE]
                # — Secret 키 이름이 곧 컨테이너 env 이름. CH_HOST는 install.sh [7/7]가 set env로 주입.
                - secretRef:
                    name: token-metrics-ch-secret
              env:
                - name: ENDPOINTS_FILE
                  value: /etc/token-metrics/endpoints.yaml
                - name: SOFT_DEADLINE_MINUTES
                  value: "40"
                - name: LOAD_BUDGET_S
                  value: "1200"
                - name: FINAL_HOUR_KST
                  value: "9"
                - name: MAX_RESPONSE_BYTES
                  value: "5000000"
                - name: METRICS_MAX_MUTATIONS_PER_RUN
                  value: "45"
              volumeMounts:
                - name: endpoints
                  mountPath: /etc/token-metrics
                  readOnly: true
                - name: ca-bundle
                  mountPath: /etc/token-metrics-ca
                  readOnly: true
              resources:
                requests:
                  cpu: 100m
                  memory: 256Mi
                limits:
                  cpu: "1"
                  # limits 없는 배포 금지 (기존 모듈 OOM 실경험) — 응답 ≤5MB 단건 + normalize 여유
                  memory: 1Gi
              # CA 번들은 install.sh [2/7]가 ConfigMap으로 주입(선택) — 있으면
              # COLLECTOR_API_CA_BUNDLE=/etc/token-metrics-ca/ca-bundle.pem (Secret 키)
          # 볼륨 index 순서는 계약: [0] endpoints, [1] ca-bundle — company-verify 패치·tools/manual_load.py([2] manual)가 의존
          volumes:
            - name: endpoints
              configMap:
                name: token-metrics-endpoints
            - name: ca-bundle
              configMap:
                name: token-metrics-ca-bundle
                optional: true
```

`collectors/token-metrics/k8s/base/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - cronjob.yaml
```

파싱 확인:

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
python3 -c "
import yaml
cj = yaml.safe_load(open('k8s/base/cronjob.yaml'))
job = cj['spec']['jobTemplate']['spec']
print(cj['spec']['schedule'], cj['spec']['timeZone'], cj['spec']['startingDeadlineSeconds'], job['activeDeadlineSeconds'], job['backoffLimit'])
print([v['name'] for v in job['template']['spec']['volumes']])
"
```

기대 출력 2줄:

```
5 2-9 * * * Asia/Seoul 540 3000 0
['endpoints', 'ca-bundle']
```

- [ ] **Step 4: overlays — stage(이미지 치환) / company(resources-only) / company-verify(nameSuffix + 패치)**

`collectors/token-metrics/k8s/overlays/stage/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
# namespace는 매니페스트에 고정하지 않는다 — install.sh의 -n ${NAMESPACE}(기본 monitoring)에 일원화
# (고정 시 --namespace 옵션과 apply -k가 충돌)
resources:
  - ../../base
images:
  - name: token-metrics-collector
    newName: ghcr.io/yoonsungnam/token-metrics-collector
    # 실제 태그는 install.sh가 kubectl set image로 덮는다 (build.sh 태그와 일치)
    newTag: latest
```

`collectors/token-metrics/k8s/overlays/company/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
# namespace 고정 없음 — install.sh -n 일원화 (stage overlay와 동일 사유)
resources:
  - ../../base
# 이미지 주소는 install.sh가 --registry/--tag로 kubectl set image 주입 (사내 Harbor 주소 커밋 금지 — 설계 §7.2)
```

`collectors/token-metrics/k8s/overlays/company-verify/kustomization.yaml` — `nameSuffix`는 CronJob 이름에만 붙고 참조 이름(secretRef/configMap)은 kustomize가 자동 치환하지 않으므로 JSON 패치로 명시한다. 패치 index `containers/0`·`envFrom/0`·`volumes/0`은 base 구조 계약(테스트 `_resolve_pointer`가 base에서 실제 값을 확인).

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
# namespace 고정 없음 — install.sh -n 일원화 (company overlay와 동일 사유)
# 격리 검증 전용(설계 §7.5 — 선택) — company와 동일 이미지·리소스,
# CronJob/Secret/ConfigMap 이름만 -verify 접미로 분리해 production 리소스와 공존시킨다.
# ca-bundle ConfigMap(token-metrics-ca-bundle)은 접미 없이 공용 (base volumes[1], optional).
nameSuffix: -verify
resources:
  - ../../base
patches:
  - target:
      kind: CronJob
      name: token-metrics-collector
    patch: |-
      - op: replace
        path: /spec/jobTemplate/spec/template/spec/containers/0/envFrom/0/secretRef/name
        value: token-metrics-ch-secret-verify
      - op: replace
        path: /spec/jobTemplate/spec/template/spec/volumes/0/configMap/name
        value: token-metrics-endpoints-verify
# 이미지 주소는 install.sh가 --registry/--tag로 kubectl set image 주입 (company overlay와 동일)
```

- [ ] **Step 5: 중간 실행 — 매니페스트 테스트 통과, install.sh 계열 3건만 남는지**

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
python3 -m pytest -q tests/test_manifests.py 2>&1 | tail -n 6
```

기대: `3 failed, 10 passed in …`(kubectl 없는 환경은 `3 failed, 9 passed, 1 skipped`). 실패 3건은 정확히 `test_no_token_usage_names_anywhere`(`FileNotFoundError … install.sh`), `test_install_sh_contract`(`FileNotFoundError … install.sh`), `test_scripts_executable_and_bash_syntax`(`AssertionError: install.sh must be chmod +x`). 그 외 실패가 있으면 Step 3/4의 YAML을 다시 대조한다.

- [ ] **Step 6: install.sh — 7단계(프리플라이트 [4/7] 포함), 기존 모듈 install.sh의 VM 블록은 클론하지 않음**

설계 §5.6 그대로: [1/7] `registry-pull-secret`은 **없을 때만** 생성(§7.5 네임스페이스 공유), [2/7] app Secret(`CH_USER` 기본 `mart`, company-verify는 `token_verify` + `CH_DB_FACT`/`CH_DB_DIM` 프롬프트), [3/7] endpoints ConfigMap, [4/7] 프리플라이트(앱 계정 `CH_USER`/`CH_PASSWORD`로 DB 2개 존재 + `${DB_DIM}.dim_token_service_dist` SELECT — accounts.sql GRANT 검증), [5/7] DDL 2파일(`accounts.sql`은 admin), [6/7] `apply -k`, [7/7] `set image` + `set env CH_HOST`.

`collectors/token-metrics/install.sh`:

```bash
#!/usr/bin/env bash
# token-metrics collector 설치 (설계 §5.6 배포 · §7.5 "새 코드만 새로 배포")
#
# 사용법:
#   ./collectors/token-metrics/install.sh [--registry <registry>] [--tag <tag>] \
#     [--context <kube-context>] [--namespace <ns>] [--endpoints <file>] <stage|company|company-verify>
#
#   stage:           context 기본 homelab, registry 기본 ghcr.io/yoonsungnam, endpoints 기본 endpoints.yaml (mock)
#   company:         --context/--registry 필수, endpoints 기본 endpoints-metrics.company.yaml (gitignored)
#   company-verify:  격리 검증(선택 — 설계 §7.5). --context/--registry 필수(company와 동일 요건).
#                     Secret/ConfigMap/CronJob 이름 -verify 접미, DDL은 ddl/company-verify/, CH_USER 기본
#                     token_verify, CH_DB_FACT/CH_DB_DIM 프롬프트(기본 token_verify_fact/token_verify_dim)
#   예: ./collectors/token-metrics/install.sh company --context <ctx> --registry harbor.example.internal/gpu-monitoring --tag <sha7>
#   기존 토큰 수집기 모듈의 Secret/ConfigMap/CronJob은 건드리지 않는다 — registry-pull-secret만 공유(없을 때만 생성)
#
# 수행 순서:
#   [1/7] registry-pull-secret — 없을 때만 생성 (네임스페이스 공유 Secret; 있으면 갱신하지 않음, §7.5)
#   [2/7] token-metrics-ch-secret[-verify] 멱등 생성 (대화형 — envFrom으로 컨테이너 env가 됨)
#   [3/7] token-metrics-endpoints[-verify] ConfigMap 생성/갱신
#   [4/7] 프리플라이트: chi-* 파드 탐색 → 앱 계정(CH_USER)으로 fact/gpu_data DB 존재 + 토큰 레지스트리 dim_token_service_dist SELECT (GRANT 검증, §5.6)
#   [5/7] 테이블 DDL 적용: raw_token_metrics.sql + dim_token_metrics_service.sql (accounts.sql은 admin 수동 — §4.0)
#   [6/7] CronJob 배포 (kustomize overlay)
#   [7/7] 이미지 주소/CH_HOST 주입 + 수동 테스트 커맨드 안내 (VM push 없음 — §5.2)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

IMAGE_NAME="token-metrics-collector"
CRONJOB_NAME="token-metrics-collector"
SECRET_NAME="token-metrics-ch-secret"
PULL_SECRET_NAME="registry-pull-secret"
CONFIGMAP_NAME="token-metrics-endpoints"
CA_CONFIGMAP_NAME="token-metrics-ca-bundle"
CH_NAMESPACE="clickhouse"

REGISTRY=""; TAG=""; KUBE_CONTEXT=""; NAMESPACE="monitoring"; ENDPOINTS_SRC=""; ENV=""
# 프리플라이트·DDL 안내가 쓰는 DB명 — 앱 기본값(fact/gpu_data). company-verify는 token_verify_*,
# Secret에 CH_DB_FACT/CH_DB_DIM이 있으면 [2/7]에서 그 값으로 덮어쓴다.
DB_FACT="fact"; DB_DIM="gpu_data"
DDL_DIR="ddl/company"

usage() { sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry)  REGISTRY="$2"; shift 2 ;;
    --tag)       TAG="$2"; shift 2 ;;
    --context)   KUBE_CONTEXT="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --endpoints) ENDPOINTS_SRC="$2"; shift 2 ;;
    stage|company|company-verify) ENV="$1"; shift ;;
    *) echo "[ERROR] unknown arg: $1"; usage ;;
  esac
done
[[ -n "${ENV}" ]] || usage

case "${ENV}" in
  stage)
    KUBE_CONTEXT="${KUBE_CONTEXT:-homelab}"
    REGISTRY="${REGISTRY:-ghcr.io/yoonsungnam}"
    ENDPOINTS_SRC="${ENDPOINTS_SRC:-${HERE}/endpoints.yaml}"
    # stage 홈랩 CHI는 ZK 없음 — Replicated/ON CLUSTER 불가, 생성 변형 사용 (tools/gen_stage_ddl.py)
    DDL_DIR="ddl/stage"
    ;;
  company)
    [[ -n "${KUBE_CONTEXT}" ]] || { echo "[ERROR] company 환경에서는 --context 옵션이 필수입니다."; usage; }
    [[ -n "${REGISTRY}" ]]     || { echo "[ERROR] company 환경에서는 --registry 옵션이 필수입니다."; usage; }
    ENDPOINTS_SRC="${ENDPOINTS_SRC:-${HERE}/endpoints-metrics.company.yaml}"
    ;;
  company-verify)
    # 격리 검증 — company와 동일 요건(--context/--registry 필수). 수집 대상은 동일한 실 서비스 API
    # (endpoints-metrics.company.yaml). DB는 token_verify_fact/token_verify_dim (tools/gen_verify_ddl.py 규칙).
    [[ -n "${KUBE_CONTEXT}" ]] || { echo "[ERROR] company-verify 환경에서는 --context 옵션이 필수입니다."; usage; }
    [[ -n "${REGISTRY}" ]]     || { echo "[ERROR] company-verify 환경에서는 --registry 옵션이 필수입니다."; usage; }
    ENDPOINTS_SRC="${ENDPOINTS_SRC:-${HERE}/endpoints-metrics.company.yaml}"
    SECRET_NAME="${SECRET_NAME}-verify"
    CONFIGMAP_NAME="${CONFIGMAP_NAME}-verify"
    CRONJOB_NAME="${CRONJOB_NAME}-verify"
    DB_FACT="token_verify_fact"; DB_DIM="token_verify_dim"
    DDL_DIR="ddl/company-verify"
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

# kube API 서버 호스트를 NO_PROXY에 자동 추가 (사내 프록시 환경에서 kubectl 통신 보존 — 기존 모듈과 동일)
api_server="$(${KUBECTL} config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || true)"
if [[ -n "${api_server}" ]]; then
  api_host="$(printf '%s' "${api_server}" | sed -E 's#^https?://##; s#:[0-9]+$##')"
  export NO_PROXY="${NO_PROXY:+${NO_PROXY},}${api_host}"
  export no_proxy="${no_proxy:+${no_proxy},}${api_host}"
fi

echo "=== token-metrics collector install (${ENV}) ==="
echo "context=${KUBE_CONTEXT} namespace=${NAMESPACE} image=${REGISTRY}/${IMAGE_NAME}:${TAG}"
echo "endpoints=${ENDPOINTS_SRC} ddl=${DDL_DIR} db=${DB_FACT}/${DB_DIM}"

# ── [1/7] registry pull secret (네임스페이스 공유 — 없을 때만 생성, §7.5) ───────
echo ""
echo "[1/7] image pull secret '${PULL_SECRET_NAME}'"
if ${KUBECTL} get secret "${PULL_SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  echo "  이미 존재합니다 — 네임스페이스 공유 Secret이므로 갱신하지 않습니다"
else
  read -r -p "  registry server [${REGISTRY%%/*}]: " reg_server
  reg_server="${reg_server:-${REGISTRY%%/*}}"
  read -r -p "  registry username: " reg_user
  read -r -s -p "  registry password/token: " reg_pass; echo ""
  ${KUBECTL} create secret docker-registry "${PULL_SECRET_NAME}" \
    --docker-server="${reg_server}" --docker-username="${reg_user}" \
    --docker-password="${reg_pass}" -n "${NAMESPACE}" \
    --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -
fi

# ── [2/7] app secret (envFrom — 키 이름이 곧 컨테이너 env 이름) ─────────────────
echo ""
echo "[2/7] app secret '${SECRET_NAME}'"
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
  read -r -p "  COLLECTOR_HTTPS_PROXY ('none'=직접 연결, enter=시스템 상속, 값=프록시 URL): " http_proxy_v
  read -r -p "  사내 CA 번들 파일 경로 (없으면 enter): " ca_bundle_v
  # CH_DB_FACT/CH_DB_DIM — 격리 검증(company-verify) 전용 프롬프트(기본 token_verify_fact/token_verify_dim).
  # stage/company는 키를 만들지 않는다(앱 기본값 fact/gpu_data — app/writer.py DB_FACT/DB_DIM).
  ch_db_fact_v=""; ch_db_dim_v=""
  if [[ "${ENV}" == "company-verify" ]]; then
    read -r -p "  CH_DB_FACT [${DB_FACT}]: " ch_db_fact_v
    read -r -p "  CH_DB_DIM [${DB_DIM}]: " ch_db_dim_v
    ch_db_fact_v="${ch_db_fact_v:-${DB_FACT}}"
    ch_db_dim_v="${ch_db_dim_v:-${DB_DIM}}"
    DB_FACT="${ch_db_fact_v}"; DB_DIM="${ch_db_dim_v}"
  fi
  # stage 홈랩 CHI는 ZK 없음 — ON CLUSTER 불가하므로 단일노드 모드(빈 값).
  # company/company-verify는 클러스터명 주입 (CH_CLUSTER와 DDL의 ON CLUSTER 리터럴 일치 전제)
  CH_CLUSTER_VALUE="gpu-monitoring"
  [[ "${ENV}" == "stage" ]] && CH_CLUSTER_VALUE=""
  args=(--from-literal="CH_USER=${ch_user}"
        --from-literal="CH_PASSWORD=${ch_pass}"
        --from-literal="CH_PORT=8123"
        --from-literal="CH_CLUSTER=${CH_CLUSTER_VALUE}")
  [[ -n "${ch_db_fact_v}" ]] && args+=(--from-literal="CH_DB_FACT=${ch_db_fact_v}")
  [[ -n "${ch_db_dim_v}" ]] && args+=(--from-literal="CH_DB_DIM=${ch_db_dim_v}")
  # 프록시 3분기: 키 미설정=시스템 상속 / 빈 값=직접 연결 / 값=전용 프록시 (app/config.py load_config).
  # read로는 미입력과 빈 값이 구분되지 않으므로 'none' 센티널로 빈 값을 받는다.
  case "${http_proxy_v}" in
    "")   ;;                                                         # enter → 키 미설정 = 상속
    none) args+=(--from-literal="COLLECTOR_HTTPS_PROXY=") ;;         # 빈 값 = 직접 연결
    *)    args+=(--from-literal="COLLECTOR_HTTPS_PROXY=${http_proxy_v}") ;;
  esac
  if [[ -n "${ca_bundle_v}" ]]; then
    # CA '파일'을 파드에 전달해야 한다 — 경로 문자열만 Secret에 넣으면 컨테이너에서 열 수 없음.
    # base cronjob.yaml의 optional ConfigMap 볼륨(/etc/token-metrics-ca)에 실어 보낸다.
    [[ -f "${ca_bundle_v}" ]] || { echo "[ERROR] CA 파일이 없습니다: ${ca_bundle_v}"; exit 1; }
    ${KUBECTL} create configmap "${CA_CONFIGMAP_NAME}" \
      --from-file="ca-bundle.pem=${ca_bundle_v}" -n "${NAMESPACE}" \
      --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -
    args+=(--from-literal="COLLECTOR_API_CA_BUNDLE=/etc/token-metrics-ca/ca-bundle.pem")
  fi
  ${KUBECTL} create secret generic "${SECRET_NAME}" "${args[@]}" -n "${NAMESPACE}" \
    --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -
else
  # 갱신하지 않으면 기존 Secret의 CH_DB_FACT/CH_DB_DIM(있을 때만)을 프리플라이트 대상 DB로 쓰고,
  # CH_USER/CH_PASSWORD 는 프리플라이트가 앱 계정으로 접속(GRANT 검증)하는 데 쓴다
  existing_fact="$(${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.data.CH_DB_FACT}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  existing_dim="$(${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.data.CH_DB_DIM}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  [[ -n "${existing_fact}" ]] && DB_FACT="${existing_fact}"
  [[ -n "${existing_dim}" ]] && DB_DIM="${existing_dim}"
  ch_user="$(${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.data.CH_USER}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  ch_pass="$(${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.data.CH_PASSWORD}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  if [[ -z "${ch_user}" ]]; then
    echo "[ERROR] 기존 Secret '${SECRET_NAME}'에 CH_USER가 없습니다 — 갱신(y)으로 다시 만드세요."
    exit 1
  fi
fi

# ── [3/7] endpoints ConfigMap (endpoints.yaml 분리 원칙 — 이미지에 굽지 않음) ────
echo ""
echo "[3/7] endpoints ConfigMap '${CONFIGMAP_NAME}'"
if [[ ! -f "${ENDPOINTS_SRC}" ]]; then
  echo "[ERROR] endpoints 파일이 없습니다: ${ENDPOINTS_SRC}"
  echo "        company는 사내 URL 목록을 endpoints-metrics.company.yaml(gitignored)로 준비하세요 (설계 §4.3 키)."
  exit 1
fi
${KUBECTL} create configmap "${CONFIGMAP_NAME}" \
  --from-file="endpoints.yaml=${ENDPOINTS_SRC}" -n "${NAMESPACE}" \
  --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -

# ── [4/7] 프리플라이트 (chi-* 자동 탐색 → DB 존재 + 토큰 레지스트리 SELECT, §5.6·§7.5) ─
echo ""
echo "[4/7] preflight (db=${DB_FACT}/${DB_DIM}, registry=${DB_DIM}.dim_token_service_dist)"
ch_pod="$(${KUBECTL} get pods -n "${CH_NAMESPACE}" -o name 2>/dev/null \
  | sed 's#^pod/##' | grep '^chi-' | head -1 || true)"
if [[ -z "${ch_pod}" ]]; then
  echo "[ERROR] ${CH_NAMESPACE} 네임스페이스에서 chi-* ClickHouse 파드를 찾지 못했습니다."
  exit 1
fi
echo "  ClickHouse pod: ${ch_pod}"
ch_query() {
  # 파드 안의 clickhouse-client(로컬 접속)로 단일 쿼리 — 접속 계정은 컨테이너가 쓸 앱 계정(CH_USER/CH_PASSWORD).
  # default 계정으로 돌리면 GRANT 누락을 잡지 못하므로(accounts.sql의 GRANT SELECT ON dim_token_service_dist TO mart)
  # 앱 계정으로 같은 SELECT 를 실행해 "DB 존재 + 앱 계정이 실제로 읽을 수 있음"을 함께 확인한다 (§5.6).
  # (system.databases 는 계정에 권한이 있는 DB만 보여 준다 — 행 수 2 미만이면 DB 부재이거나 GRANT 누락)
  ${KUBECTL} exec -n "${CH_NAMESPACE}" "${ch_pod}" -- \
    clickhouse-client --user "${ch_user}" --password "${ch_pass}" --query "$1"
}
db_rows="$(ch_query "SELECT name FROM system.databases WHERE name IN ('${DB_FACT}','${DB_DIM}')" || true)"
db_count="$(printf '%s\n' "${db_rows}" | grep -c . || true)"
if [[ "${db_count}" != "2" ]]; then
  echo "[ERROR] 프리플라이트 실패: DB 부재 또는 GRANT 누락 — admin이 ${HERE}/${DDL_DIR}/accounts.sql 실행 필요"
  echo "        계정 ${ch_user} 기준 필요: ${DB_FACT}, ${DB_DIM} / 발견: ${db_rows:-<없음>}"
  exit 1
fi
echo "  DB OK (as ${ch_user}): ${DB_FACT}, ${DB_DIM}"
if ! registry_rows="$(ch_query "SELECT count() FROM ${DB_DIM}.dim_token_service_dist")"; then
  echo "[ERROR] 프리플라이트 실패: 토큰 레지스트리 SELECT 불가(GRANT 누락) — admin이 ${HERE}/${DDL_DIR}/accounts.sql 실행 필요"
  echo "        대상: ${DB_DIM}.dim_token_service_dist (계정 ${ch_user})"
  echo "        기존 토큰 수집기 모듈이 같은 클러스터에 설치돼 있어야 합니다 (설계 §5.1 — 유일한 접점, 읽기 전용)"
  exit 1
fi
echo "  registry OK (as ${ch_user}): ${DB_DIM}.dim_token_service_dist rows=${registry_rows}"

# ── [5/7] 테이블 DDL (kubectl cp + clickhouse-client — §4.0 매니페스트의 collectors 2파일) ─
echo ""
echo "[5/7] table DDL (${DDL_DIR})"
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
if [[ "${ENV}" == "company-verify" ]]; then
  echo "  (격리 검증 — ${DDL_DIR}/의 테이블 DDL만 적용. DB·전용 계정은 admin이 ${DDL_DIR}/accounts.sql로"
  echo "   먼저 생성(프리플라이트가 DB 존재를 확인함): python3 tools/gen_verify_ddl.py로 재생성 가능)"
else
  echo "  (DB 존재·앱 계정 GRANT는 프리플라이트가 확인함 — 계정/GRANT 자체는 admin이 ${DDL_DIR}/accounts.sql로 적용, 설계 §4.0)"
fi
apply_sql "${HERE}/${DDL_DIR}/raw_token_metrics.sql"
apply_sql "${HERE}/${DDL_DIR}/dim_token_metrics_service.sql"
echo "  (accounts.sql은 적용하지 않았습니다 — CREATE DATABASE/CREATE USER/GRANT는 admin 수동 실행, 설계 §4.0)"

# ── [6/7] CronJob 배포 ────────────────────────────────────────────────────────
echo ""
echo "[6/7] CronJob apply (overlay: ${ENV})"
${KUBECTL} apply -k "${HERE}/k8s/overlays/${ENV}" -n "${NAMESPACE}"

# ── [7/7] 이미지/CH_HOST 주입 ────────────────────────────────────────────────
echo ""
echo "[7/7] set image / set env"
${KUBECTL} set image "cronjob/${CRONJOB_NAME}" \
  "${IMAGE_NAME}=${REGISTRY}/${IMAGE_NAME}:${TAG}" -n "${NAMESPACE}"

# CH_HOST: [4/7]에서 찾은 chi-* 파드명에서 말미 ordinal을 잘라 헤드리스 서비스명 유도
# (예: chi-<cluster>-<cluster>-0-0-0 → chi-<cluster>-<cluster>-0-0.clickhouse.svc)
ch_host="${ch_pod%-*}.${CH_NAMESPACE}.svc"
${KUBECTL} set env "cronjob/${CRONJOB_NAME}" "CH_HOST=${ch_host}" -n "${NAMESPACE}"
echo "  CH_HOST=${ch_host}"

echo ""
echo "[OK] 설치 완료. 정기 실행은 02:05~09:05 KST 8슬롯 (BATCH_RESULT ... slot=HH final=0|1). 수동 테스트:"
echo "  ${KUBECTL} create job --from=cronjob/${CRONJOB_NAME} ${CRONJOB_NAME}-manual-\$(date +%s) -n ${NAMESPACE}"
echo "  (날짜 범위 재수집: python3 ${HERE}/tools/rerun.py --context ${KUBE_CONTEXT} --namespace ${NAMESPACE} --cronjob ${CRONJOB_NAME} --from <D0> --to <D1>)"
echo "  (수기 CSV 적재: python3 ${HERE}/tools/manual_load.py --context ${KUBE_CONTEXT} --namespace ${NAMESPACE} --from <D0> --to <D1> --gpu <gpu.csv> --serving <serving.csv>)"
```

실행 권한·문법·usage 경로 스모크(kubectl 접속 없이 끝나는 경로만):

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
chmod +x install.sh
bash -n install.sh && echo SYNTAX_OK
out=$(./install.sh 2>&1); echo "rc=$?"; head -3 <<<"$out"
out=$(./install.sh company 2>&1); echo "rc=$?"; head -1 <<<"$out"
out=$(./install.sh company --context x 2>&1); echo "rc=$?"; head -1 <<<"$out"
grep -c '\[[1-7]/7\]' install.sh
grep -n 'apply_sql "' install.sh
grep -c 'token-usage\|VM_PUSH_URL\|vminsert' install.sh
```

기대:

```
SYNTAX_OK
rc=1
token-metrics collector 설치 (설계 §5.6 배포 · §7.5 "새 코드만 새로 배포")

사용법:
rc=1
[ERROR] company 환경에서는 --context 옵션이 필수입니다.
rc=1
[ERROR] company 환경에서는 --registry 옵션이 필수입니다.
23
243:apply_sql "${HERE}/${DDL_DIR}/raw_token_metrics.sql"
244:apply_sql "${HERE}/${DDL_DIR}/dim_token_metrics_service.sql"
0
```

(`out=$(…); echo "rc=$?"` 로 잡는 이유: `./install.sh | head -1; echo "rc=${PIPESTATUS[0]}"` 꼴은 `head`가 먼저 닫혀 스크립트가 SIGPIPE(rc=141)로 죽을 수 있어 exit 1 을 안정적으로 볼 수 없다 — 출력을 변수에 모두 받은 뒤 잘라 본다. `rc=` 줄이 텍스트보다 먼저 찍힌다. `grep -c '\[[1-7]/7\]'`의 23 = 헤더 수행 순서 주석 7 + 구획 주석 `# ── [x/7]` 7 + 단계 배너 `echo "[x/7] …"` 7 + 교차 참조 주석 2(`[2/7]에서 그 값으로`, `[4/7]에서 찾은`) — 단계 번호가 `[x/6]`로 남아 있으면 이 수가 달라진다. `apply_sql "` 줄 번호 243/244는 파일을 그대로 붙여넣었을 때의 값이며, 핵심은 **정확히 2줄**이고 `accounts.sql`이 없다는 점.)

- [ ] **Step 6-실행: 전체 매니페스트 테스트 PASS**

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
python3 -m pytest -q tests/test_manifests.py 2>&1 | tail -n 3
```

기대: `13 passed in …`(kubectl 없는 환경은 `12 passed, 1 skipped`).

- [ ] **Step 7: kustomize 렌더 검증 — T11 CI manifests job과 동일한 grep을 로컬에서 먼저 통과**

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
for o in stage company company-verify; do
  kubectl kustomize "k8s/overlays/${o}" > "/tmp/claude-1000/-home-mini-github-token-data-pipeline/8a0025e9-e64c-4caf-972c-788ea90abe0e/scratchpad/render-${o}.yaml" || { echo "RENDER_FAIL ${o}"; exit 1; }
done
R=/tmp/claude-1000/-home-mini-github-token-data-pipeline/8a0025e9-e64c-4caf-972c-788ea90abe0e/scratchpad
for o in stage company company-verify; do
  echo "== ${o}"
  grep -c 'schedule: 5 2-9 \* \* \*\|timeZone: Asia/Seoul\|startingDeadlineSeconds: 540\|activeDeadlineSeconds: 3000\|backoffLimit: 0\|concurrencyPolicy: Forbid\|memory: 1Gi\|memory: 256Mi\|name: registry-pull-secret\|name: token-metrics-ca-bundle\|name: METRICS_MAX_MUTATIONS_PER_RUN' "${R}/render-${o}.yaml"
  grep -c 'token-usage' "${R}/render-${o}.yaml"
done
grep -n '^  name: \|image: ' "${R}/render-stage.yaml" "${R}/render-company.yaml"
grep -n 'name: token-metrics-collector-verify\|name: token-metrics-ch-secret-verify\|name: token-metrics-endpoints-verify\|name: token-metrics-ca-bundle' "${R}/render-company-verify.yaml"
```

기대: 세 overlay 모두 첫 grep `11`, 두 번째 grep `0`. stage 렌더는 `image: ghcr.io/yoonsungnam/token-metrics-collector:latest`, company 렌더는 `image: token-metrics-collector:latest`(install.sh [7/7]가 덮음), 둘 다 `  name: token-metrics-collector`. company-verify 렌더는 `name: token-metrics-collector-verify`·`name: token-metrics-ch-secret-verify`·`name: token-metrics-endpoints-verify` 각 1줄 + `name: token-metrics-ca-bundle`(접미 없음) 1줄.

- [ ] **Step 8: zero-diff 확인 + 커밋**

```bash
cd /home/mini/github/token-data-pipeline
git status --porcelain -- collectors/token-usage mart/token-usage assets/user-org assets/model-catalog tools/verify/invariants.sql docs/operations/company-verify.md docs/operations/stage-runbook.md docs/operations/rerun.md docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml | wc -l
git status --porcelain -- collectors/token-metrics
grep -rn "harbor\.\(company\|corp\|internal\)\|@.*\.com" collectors/token-metrics/Dockerfile collectors/token-metrics/build.sh collectors/token-metrics/install.sh collectors/token-metrics/k8s | grep -v "harbor.example.internal" | wc -l
```

기대: 첫 줄 `0`(기존 모듈·워크플로 무수정 — §7.5 zero-diff). 둘째 블록은 `?? collectors/token-metrics/Dockerfile`, `?? collectors/token-metrics/build.sh`, `?? collectors/token-metrics/install.sh`, `?? collectors/token-metrics/k8s/`, `?? collectors/token-metrics/tests/test_manifests.py`(T2~T7 산출물이 이미 커밋돼 있다면 이 5개만). 셋째 줄 `0`(플레이스홀더 `harbor.example.internal` 외 사내 주소·이메일 0 — 설계 §7.2).

```bash
cd /home/mini/github/token-data-pipeline
git add collectors/token-metrics/Dockerfile collectors/token-metrics/build.sh collectors/token-metrics/install.sh collectors/token-metrics/k8s collectors/token-metrics/tests/test_manifests.py
git commit -m "$(cat <<'MSG'
feat(collectors-metrics): 배포 계층 — Dockerfile·build.sh·CronJob §5.2·overlays·install.sh 프리플라이트·매니페스트 계약 테스트 (Plan 6b T8)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54
MSG
)"
git show --stat HEAD | tail -n 12
```

기대: `git show --stat`에 아래 9개 파일만(모두 신규): `collectors/token-metrics/Dockerfile`, `build.sh`, `install.sh`, `k8s/base/cronjob.yaml`, `k8s/base/kustomization.yaml`, `k8s/overlays/company-verify/kustomization.yaml`, `k8s/overlays/company/kustomization.yaml`, `k8s/overlays/stage/kustomization.yaml`, `tests/test_manifests.py` — `9 files changed, … insertions(+)` 이고 `deletions` 없음.

**Task 8 Self-Review 메모 (설계 해석 — 설계가 명시하지 않은 지점의 선택):**
- 설계 해석: Secret은 개별 `secretKeyRef`가 아니라 `envFrom.secretRef`(Secret 키 이름 = 컨테이너 env 이름) — 기존 모듈과 동일 패턴이며, company-verify 패치 경로 `/…/containers/0/envFrom/0/secretRef/name`이 이 구조를 전제한다. 선택 키(`COLLECTOR_HTTPS_PROXY`, `COLLECTOR_API_CA_BUNDLE`, `CH_DB_FACT`, `CH_DB_DIM`)를 "키 없음 = 앱 기본값"으로 다루려면 `envFrom`이 필요하다.
- 설계 해석: base 이미지 참조는 `token-metrics-collector:latest`. stage는 kustomize `images`로 `ghcr.io/yoonsungnam/token-metrics-collector`로 치환하고, company·company-verify는 매니페스트에 사내 주소를 두지 않고 install.sh [7/7] `kubectl set image`가 `--registry/--tag`로 덮는다(§7.2 공개 레포 규칙). 테스트는 이름 부분(`image.split(":")[0]`)만 고정한다.
- 설계 해석: `build.sh`는 기존 모듈 규약 그대로 `--registry/--tag` + `stage|company` 뿐(항상 build+push; `--push`/`--base-image` 같은 신규 옵션 없음). company-verify 전용 이미지는 없다(§7.5 "이미지 1개").
- 설계 해석: `CH_DB_FACT`/`CH_DB_DIM` 프롬프트는 company-verify에서만(기본 `token_verify_fact`/`token_verify_dim`); stage/company는 키를 만들지 않아 앱 기본 `fact`/`gpu_data`를 쓴다. [2/7]을 건너뛴 재설치에서는 기존 Secret의 두 키를 `jsonpath`로 읽어 프리플라이트 DB명으로 쓴다(격리 DB 이름을 바꿔 둔 경우에도 [4/7]이 실제 대상 DB를 검사). 프리플라이트는 `default` 가 아니라 앱 계정(`CH_USER`/`CH_PASSWORD` — [2/7] 입력값 또는 기존 Secret)으로 접속한다: 6a `accounts.sql`의 `GRANT SELECT ON gpu_data.dim_token_service_dist TO mart`가 빠져 있으면 `default`로는 통과하고 CronJob 첫 실행에서야 실패하므로, 설치 시점에 GRANT 누락을 잡는다(`--password`는 파드 안 clickhouse-client 인자로만 전달 — 로그·이미지에 남지 않는다).
- 설계 해석: 라벨 `app: token-metrics-collector`를 CronJob·jobTemplate·pod template 3곳에 둔다 — T9 `rerun.py`·T10 `manual_load.py`가 `kubectl get jobs -l app=token-metrics-collector`로 실행 단위를 조회할 수 있게(기존 모듈은 CronJob 라벨만).
- 설계 해석: company-verify에서 `token-metrics-ca-bundle` ConfigMap은 `-verify` 접미 없이 공용(같은 CA를 두 CronJob이 마운트; base `volumes[1]`은 `optional: true`라 부재해도 파드 기동). Secret·endpoints ConfigMap·CronJob만 분리한다.
- 설계 해석: 프리플라이트 [4/7]의 chi-* 파드 탐색·`CH_HOST=${ch_pod%-*}.clickhouse.svc` 유도는 기존 모듈 install.sh의 방식 그대로(설계 §5.6 "동일 방식"). 실제 클러스터명은 커밋하지 않고 주석에 `chi-<cluster>-<cluster>-0-0-0` 플레이스홀더만 둔다.
- 설계 해석: 개발 머신에 `python` 바이너리가 없어 모든 실행 커맨드는 `python3 -m pytest`로 쓴다(CI T11도 `python3`). Dockerfile `CMD`의 `python`은 컨테이너(python:3.12-slim) 안의 바이너리라 그대로.
- 설계 해석: usage()는 헤더 2~14행을 `sed -n '2,14p'`로 출력 — 헤더를 늘리면 범위를 같이 바꿔야 하며 `test_install_sh_contract`가 `lines[1:14]`가 전부 주석인지 검사해 불일치를 잡는다.
- 이 태스크는 `tools/rerun.py`·`tools/manual_load.py`(T9/T10)·`endpoints-metrics.company.yaml`(gitignored, Plan 6a G)·`docs/operations/*`(T12)를 만들지 않는다. install.sh [7/7] 안내 문구의 CLI 형태는 T9/T10 Interfaces에 맞춰 두었고, T9/T10 구현 시 옵션 이름이 바뀌면 이 안내 문구 2줄을 같은 커밋에서 갱신한다.

---

### Task 9: tools/rerun.py — --chunk-days 7 청크 순차 Job · 실행 창(10:50 KST + 활성 mart Job 0) · --chain-mart 동일 범위 전파 · exit 0/1/2/3

**Files:**
- Create: `collectors/token-metrics/tools/rerun.py`
- Test: `collectors/token-metrics/tests/test_rerun.py`
- Modify: 없음 (기존 `collectors/token-usage/tools/rerun.py`는 zero-diff — 골격만 복제)

**Interfaces:**
- Consumes:
  - T6 CLI `python -m app.main --from D0 --to D1 [--service S] [--replace]`(rerun 모드 = 날짜별 배치; `--replace` 없으면 앵커 존재 날짜·서비스는 `SKIPPED reason=already_loaded`, 0 뮤테이션).
  - T8 `k8s/base/cronjob.yaml`: CronJob `token-metrics-collector`(`spec.jobTemplate.spec.activeDeadlineSeconds: 3000`, `backoffLimit: 0`, 컨테이너 `name: token-metrics-collector`) — 청크 1개 = Job 1개가 이 데드라인을 그대로 상속한다(override 없음).
  - 설계 §5.6(286행): CRONJOB 고정·command·`--chunk-days` 기본 7·`TIMEOUT_SINGLE_S = 3000+600`·`--chain-mart` 동일 날짜·실행 창 10:50 이후 + 활성 `token-mart-metrics` Job 0. §6.3(313행): mart-metrics rerun 수신 CLI `python3 mart/token-metrics/tools/rerun.py --context C --namespace N --from D0 --to D1 [--chunk-days N]`(Plan 6c 산출 — 6b는 **호출만**).
- Produces (T12 README · Plan 6c `mart/token-metrics/tools/rerun.py` 체이닝 계약이 소비):
  - 상수: `CRONJOB = "token-metrics-collector"`, `MART_CRONJOB = "token-mart-metrics"`, `MART_RERUN = "mart/token-metrics/tools/rerun.py"`, `REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]`, `POLL_S = 10`, `TIMEOUT_SINGLE_S = 3000 + 600`, `DEFAULT_CHUNK_DAYS = 7`, `WINDOW_OPEN_HHMM = (10, 50)`, `KST = dt.timezone(dt.timedelta(hours=9))`.
  - CLI: `python3 tools/rerun.py --context C [--namespace monitoring] [--cronjob token-metrics-collector] --from YYYY-MM-DD --to YYYY-MM-DD [--service S] [--replace] [--chunk-days 7] [--chain-mart] [--force-window]`. `--from/--to`는 **필수 쌍**(argparse `required=True` — 기존 모듈의 "인자 없이 어제 1일 수동 트리거" 모드는 제공하지 않는다: 정기 슬롯 8회가 그 역할이고, 수동 1회 트리거는 `kubectl create job --from=cronjob/token-metrics-collector …`을 T12 README가 안내). `--from > --to` → exit 2, 날짜 형식 오류 → exit 2, `--chunk-days`가 `1..CHUNK_DAYS_MAX(15)` 밖 → exit 2 (모두 `argparse.ArgumentParser.exit(2, msg)` = `SystemExit(2)`). 상한 15 = §4.0 실행당 뮤테이션 예산 `METRICS_MAX_MUTATIONS_PER_RUN` 45 ÷ 날짜당 3 — `--replace` 청크가 예산 초과(`FAILURE reason=mutation_budget`)로 실패하지 않게 하는 정적 가드(6c mart rerun 상한 16 이하이므로 체인 전파도 안전).
  - `def kubectl(context: str, args: list[str], *, capture: bool = False, input_data: str | None = None) -> subprocess.CompletedProcess` — 기존과 동일 시그니처(`kubectl --context=C --insecure-skip-tls-verify …`, `check=True`, `text=True`).
  - `def now_kst() -> dt.datetime` — `dt.datetime.now(KST)`. 테스트는 이 함수를 monkeypatch한다(C 타입 `datetime.datetime.now`는 monkeypatch 불가).
  - `def split_chunks(d0: dt.date, d1: dt.date, chunk_days: int) -> list[tuple[dt.date, dt.date]]` — inclusive 구간을 앞에서부터 `chunk_days`일씩 자른다(마지막 조각은 짧아도 됨). `(2026-09-01, 2026-09-20, 7) → [(09-01, 09-07), (09-08, 09-14), (09-15, 09-20)]`; `d0 > d1` 또는 `chunk_days < 1` → `ValueError`.
  - `def build_collect_command(from_d: str, to_d: str, service: str | None, replace: bool) -> list[str]` — `["python", "-m", "app.main", "--from", from_d, "--to", to_d]` + `["--service", service]`(있을 때) + `["--replace"]`(있을 때).
  - `def build_job_spec(cronjob_obj: dict, job_name: str, command: list[str]) -> dict` — `{"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": job_name, "labels": {"app": CRONJOB, "rerun": "1"}}, "spec": <jobTemplate.spec deepcopy + containers[0].command override>}`. `active_deadline_s` 인자 **없음**(서버 데드라인 3000 상속 — 청크 7일 × 서비스 수의 부하는 `LOAD_BUDGET_S`가 아니라 이 데드라인이 막고, 초과 시 Job Failed → 다음 청크 중단 + exit 1).
  - `def job_names(cronjob: str, epoch: int, n: int) -> list[str]` — `[f"{cronjob}-rerun-{epoch}-{i}" for i in range(n)]`.
  - `def check_window(now: dt.datetime, active_mart_jobs: int) -> str | None` — naive datetime → `ValueError`(aware면 `astimezone(KST)` 후 판정); `(now.hour, now.minute) < WINDOW_OPEN_HHMM` → `"window_closed"`; `active_mart_jobs > 0` → `"mart_job_active"`; 정상 → `None`. `--chain-mart` 여부와 무관하게 **항상** 적용(§6.3 — 수집기 rerun 자체가 mart-metrics 10:20 배치와 같은 fact 테이블을 두고 겹치지 않도록); `--force-window`는 검사 생략 + stdout `[WARN] 실행 창 검사 생략(--force-window)`.
  - `def count_active_mart_jobs(context: str, namespace: str, mart_cronjob: str) -> int` — `kubectl get jobs -n NS -o json` 1회 → `items` 중 (`metadata.ownerReferences[*].name == mart_cronjob` **또는** `metadata.name.startswith(mart_cronjob + "-")`) 이고 `status.active`(int, 기본 0) `> 0`인 항목 수.
  - `def build_mart_command(context: str, namespace: str, from_d: str, to_d: str, chunk_days: int, *, mart_cronjob: str = MART_CRONJOB, force: bool = False) -> list[str]` — `["python3", MART_RERUN, "--context", context, "--namespace", namespace]` + (`mart_cronjob != MART_CRONJOB`이면 `["--cronjob", mart_cronjob]`) + `["--from", from_d, "--to", to_d, "--chunk-days", str(chunk_days)]` + (`force`면 `["--force"]`). **청크 분할 전 전체 `--from/--to`**를 그대로 전파(수집기가 `already_loaded`로 스킵한 날짜 포함 — mart는 자기 판단으로 재계산). `def mart_cronjob_for(collector_cronjob: str) -> str` — `…-verify`로 끝나면 `token-mart-metrics-verify`, 아니면 `token-mart-metrics`(6c company-verify overlay `nameSuffix: -verify`). `main()`은 `mart_cronjob=mart_cronjob_for(args.cronjob)`, `force=args.force_window`로 호출하고 활성 Job 집계(`count_active_mart_jobs`)에도 같은 `mart_cronjob`을 쓴다.
  - `def wait_job(context: str, namespace: str, job_name: str, timeout_s: int) -> bool` — 기존 골격 그대로(Job json 폴링 `POLL_S` + 파드 `logs -f` 스트리밍; 성공 `[INFO] 전체 로그 재조회: …`, 실패 `[ERROR] job {job_name} failed — 전체 로그: …` stderr, 타임아웃 `[ERROR] job {job_name} timeout ({timeout_s}s)`).
  - `def build_arg_parser() -> argparse.ArgumentParser`, `def main(argv: list[str] | None = None) -> int`.
  - 종료코드: **0** 전 청크 성공(+ `--chain-mart`면 mart rerun의 반환값 그대로 — 6c는 활성 `token-mart-*` Job이 있으면 `--force`와 무관하게 exit 2 `RERUN REFUSED active_jobs=<n>`) / **1** Job 실패·타임아웃, `--chain-mart`에 `REPO_ROOT / MART_RERUN` 부재 / **2** 사용법 / **3** 실행 창 밖.
  - stdout/stderr 문구(README·운영자가 grep): `[INFO] 청크 {i}/{n}: {c0} .. {c1} → Job {name}`, `[ERROR] 실행 창 밖: {reason} — KST 10:50 이후·활성 token-mart-metrics Job 0일 때 재시도 (--force-window로 강제)`(stderr, exit 3), `[ERROR] 청크 {i}/{n} 실패 — 이후 청크 중단; 재시도: --from {c0} --to {to} (그 외 인자 동일)`(stderr, exit 1), `[NEXT] collectors rerun 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3):` + 다음 줄 `  ` + `shlex.join(build_mart_command(...))`, `[ERROR] --chain-mart: mart/token-metrics/tools/rerun.py 가 아직 없습니다 (Plan 6c 전) — mart-metrics 구현 후 위 명령을 실행하세요.`(stderr, exit 1).

> **설계 해석 (T9)**: (a) 실행 창 검사는 `--chain-mart` 유무와 무관하게 항상 수행한다 — §5.6 문장("실행 창 10:50 이후 + 활성 token-mart-metrics Job 0 확인")이 `rerun.py` 항목 전체에 걸려 있고, 수집기 rerun의 DELETE/INSERT가 mart-metrics 10:20 배치의 fact SELECT와 겹치면 안 되기 때문. (b) 청크 실패 시 `[ERROR]` 안내의 재시도 범위는 "실패 청크 시작일 ~ 원래 `--to`"(성공한 앞 청크는 `--replace` 없이도 `already_loaded`로 스킵되므로 재실행 안전). (c) Job 이름 `<cronjob>-rerun-<epoch>-<i>`는 `--cronjob token-metrics-collector-verify`(company-verify)에도 그대로 적용되고, Job 라벨 `app`은 상수 `CRONJOB` 값(pod 라벨은 jobTemplate이 이미 가짐). (d) 청크 Job은 `activeDeadlineSeconds: 3000`을 override 없이 상속 — 기존 모듈의 `range_deadline_s` 일수 비례 산식은 **복제하지 않는다**(§5.6 `TIMEOUT_SINGLE_S = 3000+600` 단일 값). (e) 활성 mart Job 게이트의 폭이 6b와 6c에서 다르다: 6b `count_active_mart_jobs`는 짝이 되는 mart CronJob(`token-mart-metrics` 또는 `-verify`) 소유 Job과 그 이름 접두사의 rerun Job만 세고(§5.6 문장 그대로 — "활성 token-mart-metrics Job 0"), 6c `active_mart_jobs`는 `token-mart-*` 전부(기존 token-usage mart 포함)를 세며 `--force`로도 우회하지 않는다(6c `ACTIVE_JOB_PREFIX = "token-mart-"`, `RERUN REFUSED active_jobs=<n>`). 따라서 6b 창 검사를 통과해도 `--chain-mart`의 6c 단계가 exit 2로 거부될 수 있고, 그 경우 수집은 이미 끝났으므로 운영자는 `[NEXT]`에 찍힌 명령을 다른 mart Job 종료 후 그대로 다시 실행한다 — 6b가 6c 규칙을 복제하지 않는 이유: 게이트의 진실은 6c 것이며 6b가 넓게 막으면 token-usage mart 실행 중 수집기 rerun까지 불필요하게 막힌다. (f) `--cronjob …-verify`면 mart 쪽도 `--cronjob token-mart-metrics-verify`, `--force-window`면 6c `--force`(10:50 창만 생략)를 `[NEXT]` 명령과 `--chain-mart` 호출 양쪽에 전파한다 — 전파하지 않으면 verify 환경의 체인이 운영 `token-mart-metrics`를 재실행하고, 창 밖 강제 실행이 6c에서 `RERUN REFUSED window`로 멈춘다.

- [ ] **Step 1: 실패하는 테스트 (1/2)** — `collectors/token-metrics/tests/test_rerun.py` (순수 함수·상수·Job 스펙)

기존 `collectors/token-usage/tests/test_rerun.py`의 importlib 로드 패턴을 그대로 쓴다(패키지화 없음). `cronjob_obj()`는 T8 `k8s/base/cronjob.yaml`을 `yaml.safe_load`한 dict에 서버 필드(`uid`/`resourceVersion`)를 덧붙인 것 — `build_job_spec`이 그 필드를 **버리는지**까지 실 매니페스트로 검증한다.

```python
"""tools/rerun.py 계약 테스트 (§5.6 rerun · §6.3 창/체인).

kubectl·시간·Job 대기는 전부 페이크 — 클러스터 없이 순서·이름·종료코드를 고정한다.
"""
import datetime as dt
import importlib.util
import json
import pathlib
import re
import subprocess
import sys

import pytest
import yaml

MODULE_ROOT = pathlib.Path(__file__).resolve().parents[1]
_RERUN_PATH = MODULE_ROOT / "tools" / "rerun.py"
spec = importlib.util.spec_from_file_location("rerun", _RERUN_PATH)
rerun = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rerun)

KST = dt.timezone(dt.timedelta(hours=9))
D = dt.date.fromisoformat


def cronjob_obj():
    """T8 실 매니페스트 + 서버 필드(uid/resourceVersion) — kubectl get -o json 모사."""
    with open(MODULE_ROOT / "k8s" / "base" / "cronjob.yaml", encoding="utf-8") as fh:
        obj = yaml.safe_load(fh)
    obj["metadata"].update({"namespace": "monitoring", "resourceVersion": "123", "uid": "x"})
    return obj


# ---- 상수 (§5.6) --------------------------------------------------------------

def test_cronjob_constant_is_metrics_collector():
    assert rerun.CRONJOB == "token-metrics-collector"
    assert rerun.MART_CRONJOB == "token-mart-metrics"
    assert rerun.MART_RERUN == "mart/token-metrics/tools/rerun.py"
    assert rerun.DEFAULT_CHUNK_DAYS == 7
    assert rerun.CHUNK_DAYS_MAX == 15                                      # §4.0 뮤테이션 예산 45 = 15일 × 3
    assert rerun.WINDOW_OPEN_HHMM == (10, 50)
    assert rerun.KST.utcoffset(None) == dt.timedelta(hours=9)
    # REPO_ROOT = tools/rerun.py 기준 3단계 위 = 레포 루트 (collectors/token-metrics/tools)
    assert (rerun.REPO_ROOT / "collectors" / "token-metrics" / "tools" / "rerun.py") == _RERUN_PATH


def test_timeout_single():
    # §5.6: TIMEOUT_SINGLE_S = 서버 activeDeadlineSeconds 3000 + 폴링 마진 600
    assert rerun.TIMEOUT_SINGLE_S == 3600
    assert rerun.TIMEOUT_SINGLE_S == \
        cronjob_obj()["spec"]["jobTemplate"]["spec"]["activeDeadlineSeconds"] + 600


# ---- split_chunks ------------------------------------------------------------

def test_split_chunks_7_days():
    assert rerun.split_chunks(D("2026-09-01"), D("2026-09-20"), 7) == [
        (D("2026-09-01"), D("2026-09-07")),
        (D("2026-09-08"), D("2026-09-14")),
        (D("2026-09-15"), D("2026-09-20")),
    ]
    assert rerun.split_chunks(D("2026-09-10"), D("2026-09-10"), 7) == \
        [(D("2026-09-10"), D("2026-09-10"))]
    assert rerun.split_chunks(D("2026-09-01"), D("2026-09-07"), 7) == \
        [(D("2026-09-01"), D("2026-09-07"))]
    assert rerun.split_chunks(D("2026-09-01"), D("2026-09-08"), 7)[-1] == \
        (D("2026-09-08"), D("2026-09-08"))
    assert rerun.split_chunks(D("2026-09-01"), D("2026-09-03"), 1) == [
        (D("2026-09-01"), D("2026-09-01")),
        (D("2026-09-02"), D("2026-09-02")),
        (D("2026-09-03"), D("2026-09-03")),
    ]


def test_split_chunks_rejects_bad_input():
    with pytest.raises(ValueError):
        rerun.split_chunks(D("2026-09-10"), D("2026-09-01"), 7)
    with pytest.raises(ValueError):
        rerun.split_chunks(D("2026-09-01"), D("2026-09-02"), 0)


# ---- Job 스펙·커맨드 -----------------------------------------------------------

def test_build_job_spec_command_override():
    obj = cronjob_obj()
    cmd = ["python", "-m", "app.main", "--from", "2026-09-01", "--to", "2026-09-07", "--replace"]
    job = rerun.build_job_spec(obj, "j", cmd)
    assert job["apiVersion"] == "batch/v1" and job["kind"] == "Job"
    # 서버 필드(uid/resourceVersion/namespace) 제거 — name + 라벨만
    assert job["metadata"] == {"name": "j",
                               "labels": {"app": "token-metrics-collector", "rerun": "1"}}
    assert job["spec"]["activeDeadlineSeconds"] == 3000       # override 없음 — CronJob 값 상속
    assert job["spec"]["backoffLimit"] == 0
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["name"] == "token-metrics-collector"
    assert container["command"] == cmd
    assert obj == cronjob_obj()                                 # deepcopy — 원본 불변


def test_job_names_suffix_index():
    assert rerun.job_names("token-metrics-collector", 1700000000, 3) == [
        "token-metrics-collector-rerun-1700000000-0",
        "token-metrics-collector-rerun-1700000000-1",
        "token-metrics-collector-rerun-1700000000-2",
    ]
    assert rerun.job_names("token-metrics-collector-verify", 1, 1) == \
        ["token-metrics-collector-verify-rerun-1-0"]
    assert all(len(n) <= 63 for n in rerun.job_names("token-metrics-collector-verify", 9999999999, 100))


def test_collect_command_variants():
    base = ["python", "-m", "app.main", "--from", "2026-09-01", "--to", "2026-09-07"]
    assert rerun.build_collect_command("2026-09-01", "2026-09-07", None, False) == base
    assert rerun.build_collect_command("2026-09-01", "2026-09-07", None, True) == base + ["--replace"]
    assert rerun.build_collect_command("2026-09-01", "2026-09-07", "Mock Service A", False) == \
        base + ["--service", "Mock Service A"]
    assert rerun.build_collect_command("2026-09-01", "2026-09-07", "Mock Service A", True) == \
        base + ["--service", "Mock Service A", "--replace"]


def test_chain_mart_command_same_range():
    # §6.3: 청크 분할 전 전체 --from/--to를 그대로 전파 (수집기 스킵 날짜 포함)
    assert rerun.build_mart_command("c", "monitoring", "2026-09-01", "2026-09-20", 7) == [
        "python3", "mart/token-metrics/tools/rerun.py",
        "--context", "c", "--namespace", "monitoring",
        "--from", "2026-09-01", "--to", "2026-09-20", "--chunk-days", "7",
    ]
    # company-verify → 6c --cronjob token-mart-metrics-verify, --force-window → 6c --force (창만 생략)
    assert rerun.build_mart_command("c", "monitoring", "2026-09-01", "2026-09-20", 3,
                                    mart_cronjob="token-mart-metrics-verify", force=True) == [
        "python3", "mart/token-metrics/tools/rerun.py",
        "--context", "c", "--namespace", "monitoring", "--cronjob", "token-mart-metrics-verify",
        "--from", "2026-09-01", "--to", "2026-09-20", "--chunk-days", "3", "--force",
    ]
    assert rerun.mart_cronjob_for("token-metrics-collector") == "token-mart-metrics"
    assert rerun.mart_cronjob_for("token-metrics-collector-verify") == "token-mart-metrics-verify"


def test_cronjob_default_and_override():
    args = rerun.build_arg_parser().parse_args(
        ["--context", "c", "--from", "2026-09-01", "--to", "2026-09-01"])
    assert args.cronjob == rerun.CRONJOB == "token-metrics-collector"
    assert args.namespace == "monitoring" and args.chunk_days == 7
    assert args.replace is False and args.chain_mart is False and args.force_window is False
    args = rerun.build_arg_parser().parse_args(
        ["--context", "c", "--from", "2026-09-01", "--to", "2026-09-01",
         "--cronjob", "token-metrics-collector-verify", "--chunk-days", "3", "--replace"])
    assert args.cronjob == "token-metrics-collector-verify"
    assert args.chunk_days == 3 and args.replace is True
```

- [ ] **Step 2: 실패하는 테스트 (2/2)** — 같은 파일에 이어서 붙인다 (실행 창 · 활성 mart Job 집계 · `main()` 순차/중단/종료코드 · `--chain-mart`)

```python
# ---- 실행 창 (§6.3: 10:50 KST 이후 + 활성 token-mart-metrics Job 0) --------------

def test_window_before_1050_rejected():
    assert rerun.check_window(dt.datetime(2026, 9, 4, 10, 49, tzinfo=KST), 0) == "window_closed"
    assert rerun.check_window(dt.datetime(2026, 9, 4, 10, 50, tzinfo=KST), 0) is None
    assert rerun.check_window(dt.datetime(2026, 9, 4, 11, 0, tzinfo=KST), 0) is None
    assert rerun.check_window(dt.datetime(2026, 9, 4, 0, 5, tzinfo=KST), 0) == "window_closed"


def test_window_active_mart_job_rejected():
    assert rerun.check_window(dt.datetime(2026, 9, 4, 11, 0, tzinfo=KST), 1) == "mart_job_active"
    # 두 조건 동시 위반이면 창이 먼저 (운영자가 먼저 고칠 수 있는 원인)
    assert rerun.check_window(dt.datetime(2026, 9, 4, 9, 0, tzinfo=KST), 1) == "window_closed"


def test_window_requires_aware_kst():
    with pytest.raises(ValueError):
        rerun.check_window(dt.datetime(2026, 9, 4, 11, 0), 0)          # naive 금지
    # UTC aware 입력은 KST로 환산해 판정 (01:55Z = 10:55 KST → 열림)
    assert rerun.check_window(dt.datetime(2026, 9, 4, 1, 55, tzinfo=dt.timezone.utc), 0) is None
    assert rerun.check_window(dt.datetime(2026, 9, 4, 1, 45, tzinfo=dt.timezone.utc), 0) == "window_closed"


def test_now_kst_is_aware_kst():
    now = rerun.now_kst()
    assert now.tzinfo is not None and now.utcoffset() == dt.timedelta(hours=9)


def test_count_active_mart_jobs_filters_by_owner_and_prefix(monkeypatch):
    items = [
        {"metadata": {"name": "token-mart-metrics-29300000",
                      "ownerReferences": [{"kind": "CronJob", "name": "token-mart-metrics"}]},
         "status": {"active": 1}},
        {"metadata": {"name": "token-mart-metrics-rerun-1700000000-0"},        # rerun Job (owner 없음)
         "status": {"active": 1}},
        {"metadata": {"name": "token-metrics-collector-29300000",             # 수집기 자신 — 제외
                      "ownerReferences": [{"kind": "CronJob", "name": "token-metrics-collector"}]},
         "status": {"active": 1}},
        {"metadata": {"name": "token-mart-metrics-29299999",                  # 완료 — active 키 없음
                      "ownerReferences": [{"kind": "CronJob", "name": "token-mart-metrics"}]},
         "status": {"succeeded": 1}},
    ]
    calls = []

    def fake(context, args, *, capture=False, input_data=None):
        calls.append((context, list(args), capture))
        return subprocess.CompletedProcess(args, 0, stdout=json.dumps({"items": items}), stderr="")

    monkeypatch.setattr(rerun, "kubectl", fake)
    assert rerun.count_active_mart_jobs("c", "monitoring", "token-mart-metrics") == 2
    assert calls == [("c", ["get", "jobs", "-n", "monitoring", "-o", "json"], True)]


# ---- main(): 순차 청크 · 중단 · 종료코드 ------------------------------------------

class FakeK8s:
    """rerun.kubectl 대체 — get cronjob은 fixture 반환, apply -f -는 본문(Job) 기록."""

    def __init__(self, cronjob, events):
        self.cronjob = cronjob
        self.events = events           # 공유 이벤트 로그: ("apply", name) / ("wait", name)
        self.applied = []
        self.get_cronjob_calls = 0

    def __call__(self, context, args, *, capture=False, input_data=None):
        args = list(args)
        if args[:2] == ["get", "cronjob"]:
            self.get_cronjob_calls += 1
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(self.cronjob), stderr="")
        if args[0] == "apply":
            assert args[1:] == ["-n", "monitoring", "-f", "-"]
            job = json.loads(input_data)
            self.applied.append(job)
            self.events.append(("apply", job["metadata"]["name"]))
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected kubectl {args}")


def _run_main(monkeypatch, argv, *, wait_results, now, active=0):
    events = []
    k8s = FakeK8s(cronjob_obj(), events)
    results = list(wait_results)
    waited = []

    def fake_wait(context, namespace, job_name, timeout_s):
        events.append(("wait", job_name))
        waited.append((job_name, timeout_s))
        return results.pop(0)

    monkeypatch.setattr(rerun, "kubectl", k8s)
    monkeypatch.setattr(rerun, "wait_job", fake_wait)
    monkeypatch.setattr(rerun, "count_active_mart_jobs", lambda c, n, m: active)
    monkeypatch.setattr(rerun, "now_kst", lambda: now)
    rc = rerun.main(argv)
    return rc, k8s, waited, events


RANGE = ["--from", "2026-09-01", "--to", "2026-09-20", "--chunk-days", "7"]
NOON = dt.datetime(2026, 9, 4, 12, 0, tzinfo=KST)


def test_main_sequential_chunks_and_stop_on_failure(monkeypatch, capsys):
    rc, k8s, waited, events = _run_main(
        monkeypatch, ["--context", "c"] + RANGE + ["--force-window"],
        wait_results=[True, True, True], now=dt.datetime(2026, 9, 4, 10, 0, tzinfo=KST))
    assert rc == 0
    names = [j["metadata"]["name"] for j in k8s.applied]
    assert len(names) == 3
    assert all(re.fullmatch(r"token-metrics-collector-rerun-\d+-[012]", n) for n in names)
    assert [n.rsplit("-", 1)[1] for n in names] == ["0", "1", "2"]
    assert len({n.rsplit("-", 1)[0] for n in names}) == 1              # 같은 epoch
    # apply → wait → apply → wait … (순차; 다음 Job은 앞 Job 완료 후 생성)
    assert events == [("apply", names[0]), ("wait", names[0]),
                      ("apply", names[1]), ("wait", names[1]),
                      ("apply", names[2]), ("wait", names[2])]
    assert [t for _, t in waited] == [3600, 3600, 3600]
    cmds = [j["spec"]["template"]["spec"]["containers"][0]["command"] for j in k8s.applied]
    assert cmds[0] == ["python", "-m", "app.main", "--from", "2026-09-01", "--to", "2026-09-07"]
    assert cmds[1] == ["python", "-m", "app.main", "--from", "2026-09-08", "--to", "2026-09-14"]
    assert cmds[2] == ["python", "-m", "app.main", "--from", "2026-09-15", "--to", "2026-09-20"]
    assert all(j["spec"]["activeDeadlineSeconds"] == 3000 for j in k8s.applied)
    assert k8s.get_cronjob_calls == 1                                   # CronJob 조회 1회
    out = capsys.readouterr().out
    assert "[WARN] 실행 창 검사 생략(--force-window)" in out              # 10:00인데 강제
    assert "[INFO] 청크 1/3: 2026-09-01 .. 2026-09-07 → Job " + names[0] in out
    assert "[NEXT] collectors rerun 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3):" in out
    assert ("  python3 mart/token-metrics/tools/rerun.py --context c --namespace monitoring "
            "--from 2026-09-01 --to 2026-09-20 --chunk-days 7 --force\n") in out   # --force-window → 6c --force

    # 2번째 청크 실패 → 3번째 apply 없음, exit 1, 재시도 범위 = 실패 청크 시작 ~ 원래 --to
    rc, k8s, waited, events = _run_main(
        monkeypatch, ["--context", "c"] + RANGE + ["--replace"], wait_results=[True, False], now=NOON)
    assert rc == 1
    assert len(k8s.applied) == 2 and len(waited) == 2
    assert k8s.applied[1]["spec"]["template"]["spec"]["containers"][0]["command"][-1] == "--replace"
    captured = capsys.readouterr()
    assert "[ERROR] 청크 2/3 실패 — 이후 청크 중단; 재시도: --from 2026-09-08 --to 2026-09-20" in captured.err
    assert "[NEXT]" not in captured.out                                 # 실패 시 mart 안내 없음


def test_main_service_and_replace_propagate_to_every_chunk(monkeypatch):
    rc, k8s, _, _ = _run_main(
        monkeypatch, ["--context", "c"] + RANGE + ["--service", "Mock Service A", "--replace"],
        wait_results=[True, True, True], now=NOON)
    assert rc == 0
    for job in k8s.applied:
        cmd = job["spec"]["template"]["spec"]["containers"][0]["command"]
        assert cmd[-3:] == ["--service", "Mock Service A", "--replace"]
        assert job["metadata"]["labels"] == {"app": "token-metrics-collector", "rerun": "1"}


def test_main_window_closed_exit_3(monkeypatch, capsys):
    rc, k8s, waited, _ = _run_main(
        monkeypatch, ["--context", "c"] + RANGE, wait_results=[],
        now=dt.datetime(2026, 9, 4, 10, 0, tzinfo=KST))
    assert rc == 3 and k8s.applied == [] and waited == [] and k8s.get_cronjob_calls == 0
    err = capsys.readouterr().err
    assert ("[ERROR] 실행 창 밖: window_closed — KST 10:50 이후·활성 token-mart-metrics Job 0일 때 "
            "재시도 (--force-window로 강제)") in err

    rc, k8s, _, _ = _run_main(monkeypatch, ["--context", "c"] + RANGE, wait_results=[],
                              now=NOON, active=1)
    assert rc == 3 and k8s.applied == []
    assert "[ERROR] 실행 창 밖: mart_job_active" in capsys.readouterr().err


def test_usage_errors():
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "c", "--to", "2026-09-01"])                       # --from 없음
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "c", "--from", "2026-09-01"])                     # --to 없음
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "c", "--from", "2026-09-01", "--to", "2026-09-02", "--chunk-days", "0"])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:                                          # 상한 15 초과 (뮤테이션 예산 45)
        rerun.main(["--context", "c", "--from", "2026-09-01", "--to", "2026-09-02", "--chunk-days", "16"])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "c", "--from", "2026-09-10", "--to", "2026-09-01"])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "c", "--from", "2026/09/01", "--to", "2026-09-02"])
    assert e.value.code == 2


# ---- --chain-mart ---------------------------------------------------------------

def test_chain_mart_missing_path_exit_1(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(rerun, "REPO_ROOT", tmp_path)                    # mart/token-metrics 부재
    rc, k8s, _, _ = _run_main(monkeypatch, ["--context", "c"] + RANGE + ["--chain-mart"],
                              wait_results=[True, True, True], now=NOON)
    assert rc == 1 and len(k8s.applied) == 3                              # 수집 3청크는 완료
    captured = capsys.readouterr()
    assert "[NEXT]" in captured.out                                        # 안내는 먼저 출력
    assert "[ERROR] --chain-mart: mart/token-metrics/tools/rerun.py 가 아직 없습니다 (Plan 6c 전)" in captured.err


def test_chain_mart_calls_mart_rerun_with_full_range(monkeypatch, tmp_path):
    mart_path = tmp_path / "mart" / "token-metrics" / "tools" / "rerun.py"
    mart_path.parent.mkdir(parents=True)
    mart_path.write_text("", encoding="utf-8")
    monkeypatch.setattr(rerun, "REPO_ROOT", tmp_path)
    called = []

    def fake_call(argv):
        called.append(list(argv))
        return 7                                                           # mart rerun 반환값 그대로

    monkeypatch.setattr(rerun.subprocess, "call", fake_call)
    rc, _, _, _ = _run_main(monkeypatch, ["--context", "c"] + RANGE + ["--chain-mart"],
                            wait_results=[True, True, True], now=NOON)
    assert rc == 7
    assert called == [[sys.executable, str(mart_path),
                       "--context", "c", "--namespace", "monitoring",
                       "--from", "2026-09-01", "--to", "2026-09-20", "--chunk-days", "7"]]


def test_chain_mart_propagates_verify_cronjob_and_force(monkeypatch, tmp_path, capsys):
    # --cronjob …-verify → mart --cronjob token-mart-metrics-verify; --force-window → mart --force
    mart_path = tmp_path / "mart" / "token-metrics" / "tools" / "rerun.py"
    mart_path.parent.mkdir(parents=True)
    mart_path.write_text("", encoding="utf-8")
    monkeypatch.setattr(rerun, "REPO_ROOT", tmp_path)
    called = []
    monkeypatch.setattr(rerun.subprocess, "call", lambda argv: called.append(list(argv)) or 0)
    rc, k8s, _, _ = _run_main(
        monkeypatch, ["--context", "c", "--cronjob", "token-metrics-collector-verify",
                      "--from", "2026-09-01", "--to", "2026-09-20", "--chunk-days", "15",
                      "--chain-mart", "--force-window"],
        wait_results=[True, True], now=dt.datetime(2026, 9, 4, 9, 0, tzinfo=KST))
    assert rc == 0
    assert len(k8s.applied) == 2                                           # 20일 / 15일 → 2청크
    assert called == [[sys.executable, str(mart_path),
                       "--context", "c", "--namespace", "monitoring", "--cronjob", "token-mart-metrics-verify",
                       "--from", "2026-09-01", "--to", "2026-09-20", "--chunk-days", "15", "--force"]]
    out = capsys.readouterr().out
    assert ("  python3 mart/token-metrics/tools/rerun.py --context c --namespace monitoring "
            "--cronjob token-mart-metrics-verify --from 2026-09-01 --to 2026-09-20 --chunk-days 15 --force\n") in out
```

- [ ] **Step 3: 실패 확인**

Run: `cd collectors/token-metrics && python3 -m pytest tests/test_rerun.py -q`
Expected: 수집 단계 오류 1건 — `ERROR tests/test_rerun.py - FileNotFoundError: [Errno 2] No such file or directory: '…/collectors/token-metrics/tools/rerun.py'` (importlib `exec_module`가 부재 파일에서 실패), 마지막 줄 `1 error`.

- [ ] **Step 4: 구현 (1/2)** — `collectors/token-metrics/tools/rerun.py` (docstring · 상수 · 순수 함수)

기존 `collectors/token-usage/tools/rerun.py`(203행)를 골격으로 하되 **수동 트리거 모드·`--push-vm`·`range_deadline_s`·`TIMEOUT_RANGE_S`를 제거**하고 청크·창·체인을 추가한다. 파일 전체(2/2와 이어 붙여 1개 파일):

```python
"""token-metrics collector 재수행 도구 (설계 §5.6 · §6.3).

날짜 범위(--from/--to, **inclusive**, KST — app.main 계약)를 --chunk-days(기본 7)일씩
잘라 CronJob `token-metrics-collector` 스펙에서 **청크당 Job 1개**를 순차 생성한다
(command override: `python -m app.main --from --to [--service] [--replace]`).
청크 Job은 CronJob의 activeDeadlineSeconds(3000, §5.2)를 그대로 상속한다 — 초과하면
k8s가 Job을 Failed로 만들고 이 도구는 다음 청크를 만들지 않는다(exit 1; 안내된 범위로
재시도. 앞선 성공 청크는 --replace 없이 재실행해도 already_loaded로 스킵되어 안전).

실행 창(§6.3): KST 10:50 이후 + 활성 `token-mart-metrics` Job 0 — 둘 중 하나라도
위반이면 Job을 만들지 않고 exit 3 (--force-window로 생략 가능, WARN 출력).
--chain-mart 여부와 무관하게 항상 검사한다(수집기 DELETE/INSERT가 mart-metrics
10:20 배치의 fact 읽기와 겹치지 않도록).

완료 시 동일 날짜 범위의 mart-metrics rerun 명령을 **항상 출력**(§6.3 의무 절차 —
수집기가 already_loaded로 스킵한 날짜도 포함해 청크 분할 전 전체 --from/--to 그대로),
--chain-mart 지정 시 직접 실행하고 그 반환값으로 종료한다.

사용법:
  python3 tools/rerun.py --context prod --namespace monitoring \
      --from 2026-09-01 --to 2026-09-20 --replace --chain-mart
  python3 tools/rerun.py --context prod --from 2026-09-10 --to 2026-09-10 \
      --service "Mock Service A" --replace

옵션:
  --context       kubectl context (필수)
  --namespace     기본 monitoring
  --cronjob       대상 CronJob 이름 (기본 token-metrics-collector — company-verify는
                  token-metrics-collector-verify)
  --from/--to     YYYY-MM-DD, KST, 둘 다 inclusive. 필수 쌍.
                  (수동 1회 트리거는 이 도구가 아니라 정기 슬롯 8회 또는
                   kubectl create job --from=cronjob/token-metrics-collector <name>)
  --service       단일 서비스만 (endpoints.yaml의 service 정본; 미존재 시 파드가 exit 2)
  --replace       앵커 존재 날짜도 교체 (§5.2 — 없으면 SKIPPED reason=already_loaded)
  --chunk-days    청크 길이(일), 기본 7, 1 이상
  --chain-mart    완료 후 mart/token-metrics/tools/rerun.py 를 같은 범위로 실행
  --force-window  실행 창 검사 생략 (긴급 시에만 — mart-metrics 배치와 겹칠 수 있음)

종료코드: 0 성공 / 1 Job 실패·타임아웃·mart rerun 부재 / 2 사용법 / 3 실행 창 밖
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import pathlib
import shlex
import subprocess
import sys
import time

CRONJOB = "token-metrics-collector"
MART_CRONJOB = "token-mart-metrics"
MART_RERUN = "mart/token-metrics/tools/rerun.py"   # Plan 6c 산출 경로 (부재 시 안내 후 exit 1)
REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]   # tools/ → token-metrics/ → collectors/ → 루트
POLL_S = 10
TIMEOUT_SINGLE_S = 3000 + 600     # 서버 activeDeadlineSeconds(§5.2) + 폴링 마진 600 — 청크 1개 = Job 1개
DEFAULT_CHUNK_DAYS = 7
CHUNK_DAYS_MAX = 15               # §4.0: 실행당 뮤테이션 예산 45 = 15일 × 3 (--replace 청크가 예산을 넘지 않게) — 6c 상한 16 이하
WINDOW_OPEN_HHMM = (10, 50)       # §6.3: mart-metrics 10:20 배치(activeDeadlineSeconds 1800) 종료 후
KST = dt.timezone(dt.timedelta(hours=9))


def kubectl(context, args, *, capture=False, input_data=None):
    cmd = ["kubectl", f"--context={context}", "--insecure-skip-tls-verify"] + list(args)
    return subprocess.run(cmd, check=True, text=True, input=input_data,
                          capture_output=capture)


def now_kst():
    """aware KST 현재 시각 — 테스트는 이 함수를 페이크로 바꾼다 (datetime.now 는 C 타입이라 불가)."""
    return dt.datetime.now(KST)


def split_chunks(d0, d1, chunk_days):
    """inclusive [d0, d1]을 chunk_days일씩 앞에서부터 자른다 (마지막 조각은 짧아도 됨)."""
    if chunk_days < 1:
        raise ValueError(f"chunk_days must be >= 1: {chunk_days}")
    if d0 > d1:
        raise ValueError(f"from({d0}) > to({d1})")
    chunks = []
    start = d0
    while start <= d1:
        end = min(start + dt.timedelta(days=chunk_days - 1), d1)
        chunks.append((start, end))
        start = end + dt.timedelta(days=1)
    return chunks


def build_collect_command(from_d, to_d, service, replace):
    cmd = ["python", "-m", "app.main", "--from", from_d, "--to", to_d]
    if service:
        cmd += ["--service", service]
    if replace:
        cmd += ["--replace"]
    return cmd


def build_job_spec(cronjob_obj, job_name, command):
    """CronJob 오브젝트에서 1회성 Job 스펙 생성 + containers[0].command override.

    metadata는 name + 라벨만 남긴다 (uid/resourceVersion/namespace 등 서버 필드 제거).
    activeDeadlineSeconds는 jobTemplate.spec 값(3000, §5.2)을 그대로 상속 — override 인자 없음
    (청크 7일 × 서비스 수의 부하 상한 = 이 서버 데드라인; 초과 시 Failed → 다음 청크 중단)."""
    spec = copy.deepcopy(cronjob_obj["spec"]["jobTemplate"]["spec"])
    spec["template"]["spec"]["containers"][0]["command"] = list(command)
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": job_name, "labels": {"app": CRONJOB, "rerun": "1"}},
            "spec": spec}


def job_names(cronjob, epoch, n):
    return [f"{cronjob}-rerun-{epoch}-{i}" for i in range(n)]


def check_window(now, active_mart_jobs):
    """§6.3 실행 창. 위반 사유 문자열 / 정상 None. naive datetime 은 거부 (KST 규율)."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("check_window: aware datetime required (KST)")
    local = now.astimezone(KST)
    if (local.hour, local.minute) < WINDOW_OPEN_HHMM:
        return "window_closed"
    if active_mart_jobs > 0:
        return "mart_job_active"
    return None


def count_active_mart_jobs(context, namespace, mart_cronjob):
    """활성(status.active > 0) mart-metrics Job 수 — CronJob 소유(ownerReferences) 또는
    이름 접두사 `<mart_cronjob>-…`(mart rerun Job, owner 없음) 둘 다 집계."""
    res = kubectl(context, ["get", "jobs", "-n", namespace, "-o", "json"], capture=True)
    n = 0
    for item in json.loads(res.stdout).get("items", []):
        meta = item.get("metadata", {})
        owners = {o.get("name") for o in meta.get("ownerReferences", [])}
        name = str(meta.get("name", ""))
        if mart_cronjob not in owners and not name.startswith(mart_cronjob + "-"):
            continue
        if int(item.get("status", {}).get("active", 0) or 0) > 0:
            n += 1
    return n


def build_mart_command(context, namespace, from_d, to_d, chunk_days, *,
                       mart_cronjob=MART_CRONJOB, force=False):
    # §6.3: 청크 분할 전 전체 --from/--to 를 동일 값 그대로 전파 (수집기 스킵 날짜 포함).
    # company-verify(--cronjob …-verify)면 mart 쪽도 token-mart-metrics-verify 로, --force-window 면 6c --force 로 전파
    # (6c --force 는 10:50 창만 생략 — 활성 token-mart-* Job 이 있으면 6c 가 여전히 exit 2 로 거부한다).
    cmd = ["python3", MART_RERUN, "--context", context, "--namespace", namespace]
    if mart_cronjob != MART_CRONJOB:
        cmd += ["--cronjob", mart_cronjob]
    cmd += ["--from", from_d, "--to", to_d, "--chunk-days", str(chunk_days)]
    if force:
        cmd.append("--force")
    return cmd


def mart_cronjob_for(collector_cronjob):
    """수집기 CronJob 이름 → 짝이 되는 mart CronJob 이름 (company-verify: `-verify` 접미사 동반)."""
    return MART_CRONJOB + "-verify" if collector_cronjob.endswith("-verify") else MART_CRONJOB
```

- [ ] **Step 5: 구현 (2/2)** — 같은 파일에 이어서 (`wait_job` · argparse · `main`)

```python
def wait_job(context, namespace, job_name, timeout_s):
    """Job 완료 폴링 + 파드 로그 스트리밍. 성공 True / 실패·타임아웃 False.

    backoffLimit=0(§5.2)이라 파드는 1개지만, 파드 집합 순회 골격은 기존 모듈과 동일하게 둔다 —
    마커 라인(SERVICE_RESULT/BATCH_RESULT)이 운영 기록이므로 가공 없이 그대로 출력."""
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
    p.add_argument("--namespace", default="monitoring")
    p.add_argument("--cronjob", default=CRONJOB,
                   help=f"대상 CronJob 이름 (기본 {CRONJOB}; company-verify는 {CRONJOB}-verify)")
    p.add_argument("--from", dest="from_d", required=True, help="YYYY-MM-DD (KST, inclusive)")
    p.add_argument("--to", dest="to_d", required=True, help="YYYY-MM-DD (KST, inclusive)")
    p.add_argument("--service", default=None)
    p.add_argument("--replace", action="store_true")
    p.add_argument("--chunk-days", dest="chunk_days", type=int, default=DEFAULT_CHUNK_DAYS,
                   help=f"청크 길이(일), 기본 {DEFAULT_CHUNK_DAYS}, 1..{CHUNK_DAYS_MAX} (뮤테이션 예산 45 = 15일×3)")
    p.add_argument("--chain-mart", dest="chain_mart", action="store_true")
    p.add_argument("--force-window", dest="force_window", action="store_true",
                   help="실행 창(10:50 KST 이후 + 활성 mart Job 0) 검사 생략")
    return p


def main(argv=None):
    p = build_arg_parser()
    args = p.parse_args(argv)
    try:
        d0, d1 = dt.date.fromisoformat(args.from_d), dt.date.fromisoformat(args.to_d)
    except ValueError:
        p.exit(2, "--from/--to는 YYYY-MM-DD 형식\n")
    if d0 > d1:
        p.exit(2, f"--from({d0}) > --to({d1})\n")
    if not 1 <= args.chunk_days <= CHUNK_DAYS_MAX:
        p.exit(2, f"--chunk-days는 1..{CHUNK_DAYS_MAX} (지정값 {args.chunk_days}; 뮤테이션 예산 45 = 15일×3, §4.0; "
                  f"6c mart rerun 상한 16 이하)\n")
    from_s, to_s = d0.isoformat(), d1.isoformat()
    mart_cronjob = mart_cronjob_for(args.cronjob)

    # §6.3 실행 창 — 체인 여부와 무관하게 항상 (수집기 DELETE/INSERT 가 mart-metrics 와 겹치지 않도록)
    if args.force_window:
        print("[WARN] 실행 창 검사 생략(--force-window)", flush=True)
    else:
        active = count_active_mart_jobs(args.context, args.namespace, mart_cronjob)
        reason = check_window(now_kst(), active)
        if reason:
            print(f"[ERROR] 실행 창 밖: {reason} — KST 10:50 이후·활성 {mart_cronjob} Job 0일 때 "
                  f"재시도 (--force-window로 강제)", file=sys.stderr)
            return 3

    res = kubectl(args.context, ["get", "cronjob", args.cronjob, "-n", args.namespace,
                                 "-o", "json"], capture=True)
    cronjob_obj = json.loads(res.stdout)
    chunks = split_chunks(d0, d1, args.chunk_days)
    names = job_names(args.cronjob, int(time.time()), len(chunks))
    n = len(chunks)
    for i, ((c0, c1), job_name) in enumerate(zip(chunks, names), start=1):
        print(f"[INFO] 청크 {i}/{n}: {c0} .. {c1} → Job {job_name}", flush=True)
        job = build_job_spec(cronjob_obj, job_name,
                             build_collect_command(c0.isoformat(), c1.isoformat(),
                                                   args.service, args.replace))
        kubectl(args.context, ["apply", "-n", args.namespace, "-f", "-"],
                input_data=json.dumps(job))
        if not wait_job(args.context, args.namespace, job_name, TIMEOUT_SINGLE_S):
            print(f"[ERROR] 청크 {i}/{n} 실패 — 이후 청크 중단; 재시도: --from {c0} --to {to_s} "
                  f"(그 외 인자 동일)", file=sys.stderr)
            return 1

    # §6.3: collectors rerun 후 동일 날짜 범위 mart-metrics rerun 은 의무 — 항상 안내.
    # --cronjob …-verify → mart 쪽 --cronjob token-mart-metrics-verify, --force-window → 6c --force (창만 생략).
    # 6c 는 활성 token-mart-* Job 이 있으면 --force 와 무관하게 exit 2 로 거부한다 — 그때는 위 명령을 다시 실행.
    mart_cmd = build_mart_command(args.context, args.namespace, from_s, to_s, args.chunk_days,
                                  mart_cronjob=mart_cronjob, force=args.force_window)
    print("")
    print("[NEXT] collectors rerun 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3):")
    print("  " + shlex.join(mart_cmd), flush=True)
    if args.chain_mart:
        mart_path = REPO_ROOT / MART_RERUN
        if not mart_path.exists():
            print(f"[ERROR] --chain-mart: {MART_RERUN} 가 아직 없습니다 (Plan 6c 전) — "
                  f"mart-metrics 구현 후 위 명령을 실행하세요.", file=sys.stderr)
            return 1
        # 절대경로 + 리스트 인자 (cwd 무관, 공백 인자 안전); mart rerun 의 반환값 그대로 종료
        return subprocess.call([sys.executable, str(mart_path)] + mart_cmd[2:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: 통과 확인** (전체 회귀 포함)

Run: `cd collectors/token-metrics && python3 -m pytest tests/test_rerun.py -v`
Expected: `21 passed` (test_rerun.py 단독 — 상수 2 · split 2 · Job/커맨드 5 · 창 4 · 집계 1 · main 4 · chain 3).

Run: `cd collectors/token-metrics && python3 -m pytest -q`
Expected: T2~T8 테스트 + 위 20건 전부 `passed`, 실패 0.

Run: `cd collectors/token-metrics && python3 tools/rerun.py --help | head -3 && python3 tools/rerun.py --context c --from 2026-09-10 --to 2026-09-01; echo "exit=$?"`
Expected: 첫 줄 `usage: rerun.py [-h] --context CONTEXT …`, 이어서 stderr `--from(2026-09-10) > --to(2026-09-01)`, 마지막 줄 `exit=2` (kubectl 호출 전 사용법 검증 — 클러스터 불필요).

Run: `git diff --stat -- collectors/token-usage mart/token-usage`
Expected: 출력 없음 (zero-diff — 기존 rerun.py는 읽기만 했다).

- [ ] **Step 7: Commit**

```bash
git add collectors/token-metrics/tools/rerun.py collectors/token-metrics/tests/test_rerun.py
git commit -m "feat(collectors-metrics): tools/rerun.py — --chunk-days 7 순차 Job·실행 창(10:50 KST·활성 mart Job 0)·--chain-mart 동일 범위 전파 (Plan 6b T9)" \
  -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>" \
  -m "Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

---

### Task 10: tools/manual_load.py — CSV → ConfigMap(token-metrics-manual-<ts>) → Job(/manual 볼륨 [2]) → 로그 스트리밍 → finally ConfigMap 삭제

**Files:**
- Create: `collectors/token-metrics/tools/manual_load.py`
- Test: `collectors/token-metrics/tests/test_manual_load.py`
- Modify: 없음 (기존 `collectors/token-usage/tools/rerun.py`는 zero-diff — `kubectl()`·`build_job_spec()`·`wait_job()` 골격만 **복제**, import 0. 운영자가 파일 1개만 복사해 쓰는 관례 — T9 `tools/rerun.py`와도 import 관계 없음)

**설계 근거:** §5.5(279행) "전달 경로(P0) = k8s Job: `tools/manual_load.py --from --to --gpu gpu.csv --serving serving.csv [--engine engine.csv] [--replace] [--context --namespace]` — CSV를 ConfigMap `token-metrics-manual-<ts>`로 생성 → CronJob 템플릿에서 Job 생성(`rerun.py`와 같은 골격, `/manual` 볼륨 마운트 + command 위 CLI) → 로그 스트리밍 → 완료 후 ConfigMap 삭제. 운영자 워크스테이션은 kubectl만 있으면 되고 ClickHouse 직접 접근·프록시·CA가 필요 없다 … CSV·엑셀은 gitignore" · §5.6(287행) `tools/manual_load.py`(§5.5) · §5.5(278행) 모듈 내부 CLI `python -m app.main --manual-gpu /manual/gpu.csv --manual-serving /manual/serving.csv [--manual-engine /manual/engine.csv] --from D0 --to D1 [--service S] [--replace] [--generated-at ISO]` · §6.3 "collectors rerun/manual 후 동일 날짜 mart-metrics rerun 의무"(안내만 — 체인 없음) · §7.2·Plan 6a G(실제 제출 파일은 `*manual_metrics*.csv` — gitignore; 템플릿 `token_metrics_manual_v0_*.csv`만 커밋).

**Interfaces:**
- Consumes:
  - T7 `python -m app.main` manual 모드 CLI: `--manual-gpu <path> --manual-serving <path> [--manual-engine <path>] --from D0 --to D1 [--service S] [--replace] [--generated-at ISO]`(dest `manual_gpu/manual_serving/manual_engine/generated_at`; 쌍 검증·`--from/--to` 필수는 파드 안에서 T7 `_manual_args_error`가 수행 → exit 2). 파드 stdout 마커 `MANUAL_INPUT module=token-metrics rows_gpu=… rows_serving=… rows_engine=… rows_outside_range=… rows_other_service=…` 1줄 + `SERVICE_RESULT … source_type=manual-v0` + `BATCH_RESULT` 1줄 — 이 도구는 가공 없이 스트리밍만 한다.
  - T8 `k8s/base/cronjob.yaml`(`kubectl get cronjob <name> -o json` 결과): `spec.jobTemplate.spec.activeDeadlineSeconds: 3000`, `backoffLimit: 0`, 컨테이너 `[0].name == "token-metrics-collector"`, **volumes 순서 계약 `[0] endpoints`·`[1] ca-bundle`**(`volumeMounts`도 같은 순서) — 이 도구는 `[2] manual`을 append한다(index 계약).
  - T9 `tools/rerun.py`의 `kubectl(context, args, *, capture=False, input_data=None)`·`wait_job(context, namespace, job_name, timeout_s)`·`now_kst()`·`POLL_S = 10` — 본문 동일하게 복제(import 없음).
  - Plan 6a F 템플릿 3파일 `docs/templates/token_metrics_manual_v0_{gpu,serving,engine}.csv`(헤더 `date,service,model,gpuType,category,gpuCount,gpuHours` / `date,service,model,metric,name,unit,p50,p90,p95,p99` / `service,engine_type,engine_version`; `#` 주석 줄) — 테스트 입력. 파서 규칙은 T7 몫 — 이 도구는 **바이트를 검사하지 않고** 그대로 실어 나른다(BOM 제거만).
- Produces (T11 e2e 문서 · T12 README manual 절 · Plan 6c 운영 문서가 소비):
  - CLI: `python3 tools/manual_load.py --context C [--namespace monitoring] [--cronjob token-metrics-collector] --from D0 --to D1 --gpu <gpu.csv> --serving <serving.csv> [--engine <engine.csv>] [--service S] [--replace] [--generated-at ISO] [--timeout-s 3600] [--keep-configmap]`. `--context/--from/--to/--gpu/--serving`는 argparse `required=True`. 사용법·파일·크기 오류는 전부 `argparse.ArgumentParser.exit(2, msg)` = `SystemExit(2)`(kubectl 호출 0회): 날짜 형식 오류 `--from/--to는 YYYY-MM-DD 형식`, `--from > --to` → `--from(D0) > --to(D1)`, 파일 부재 → `[ERROR] 파일 없음: <path>`, 합계 초과 → `[ERROR] CSV 합계 {n} bytes > 900000 — 날짜 범위를 나눠 제출`.
  - 상수: `CRONJOB = "token-metrics-collector"`, `CONFIGMAP_PREFIX = "token-metrics-manual-"`, `TS_FORMAT = "%Y%m%d%H%M%S"`, `MOUNT_PATH = "/manual"`, `VOLUME_NAME = "manual"`, `FILE_KEYS = ("gpu.csv", "serving.csv", "engine.csv")`, `LABELS = {"app": CRONJOB, "manual": "1"}`, `MAX_CONFIGMAP_BYTES = 900_000`(k8s ConfigMap 1MiB 한도 여유), `POLL_S = 10`, `TIMEOUT_S = 3000 + 600`, `KST = dt.timezone(dt.timedelta(hours=9))`, `MART_RERUN = "mart/token-metrics/tools/rerun.py"`.
  - `def kubectl(context: str, args: list[str], *, capture: bool = False, input_data: str | None = None) -> subprocess.CompletedProcess` — T9와 동일(`kubectl --context=C --insecure-skip-tls-verify …`, `check=True`, `text=True`).
  - `def now_kst() -> dt.datetime` — `dt.datetime.now(KST)`(테스트가 monkeypatch).
  - `def timestamp(now: dt.datetime) -> str` — aware datetime만(naive → `ValueError`), `now.astimezone(KST).strftime(TS_FORMAT)` = 14자리(예 `20260904113000`).
  - `def configmap_name(now_kst: dt.datetime) -> str` — `CONFIGMAP_PREFIX + timestamp(now_kst)`(예 `token-metrics-manual-20260904113000`; `^[a-z0-9-]+$` DNS-1123 준수).
  - `def job_name(cronjob: str, ts: str) -> str` — `f"{cronjob}-manual-{ts}"`(예 `token-metrics-collector-manual-20260904113000`; `-verify` 포함 52자 ≤ 63).
  - `def read_manual_files(gpu: pathlib.Path, serving: pathlib.Path, engine: pathlib.Path | None) -> dict[str, str]` — 키 순서 `gpu.csv`, `serving.csv`, (engine이 있을 때) `engine.csv`; 값 `Path.read_text(encoding="utf-8-sig")`(BOM 제거·universal newline으로 CRLF→LF); 부재 파일은 `FileNotFoundError` 그대로.
  - `def total_bytes(files: dict[str, str]) -> int` — `sum(len(v.encode("utf-8")))`(ConfigMap `data`에 실리는 바이트).
  - `def build_configmap(name: str, files: dict[str, str]) -> dict` — `{"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name, "labels": {"app": "token-metrics-collector", "manual": "1"}}, "data": dict(files)}`; `files` 키가 `FILE_KEYS` 밖이면 `ValueError`.
  - `def build_manual_command(from_d: str, to_d: str, *, engine: bool, service: str | None, replace: bool, generated_at: str | None) -> list[str]` — `["python", "-m", "app.main", "--manual-gpu", "/manual/gpu.csv", "--manual-serving", "/manual/serving.csv"]` + `["--manual-engine", "/manual/engine.csv"]`(engine=True) + `["--from", from_d, "--to", to_d]` + `["--service", service]`(있을 때) + `["--replace"]`(replace=True) + `["--generated-at", generated_at]`(있을 때) — 이 순서 고정.
  - `def build_job_spec(cronjob_obj: dict, job_name: str, command: list[str], configmap_name: str) -> dict` — `{"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": job_name, "labels": {"app": "token-metrics-collector", "manual": "1"}}, "spec": <jobTemplate.spec deepcopy>}`에 `containers[0].command = command`, `spec.template.spec.volumes.append({"name": "manual", "configMap": {"name": configmap_name}})`(→ index `[2]`), `containers[0].volumeMounts.append({"name": "manual", "mountPath": "/manual", "readOnly": True})`. `activeDeadlineSeconds`(3000) override 없음. 템플릿에 이미 `manual` 볼륨이 있으면 `ValueError`(이중 append 방지). 서버 필드(uid/resourceVersion/namespace) 제거.
  - `def wait_job(context: str, namespace: str, job_name: str, timeout_s: int) -> bool` — T9와 동일 본문(Job json 폴링 + 파드 `logs -f` 스트리밍; 성공 `[INFO] 전체 로그 재조회: …`, 실패 `[ERROR] job {job_name} failed — 전체 로그: …` stderr, 타임아웃 `[ERROR] job {job_name} timeout ({timeout_s}s)`).
  - `def delete_configmap(context: str, namespace: str, name: str) -> bool` — `kubectl delete configmap <name> -n NS --ignore-not-found`; `subprocess.CalledProcessError`/`OSError`는 삼키고 stderr `[WARN] ConfigMap 삭제 실패 — 수동 삭제: kubectl --context={C} delete configmap {name} -n {NS}` 출력 후 `False`(종료코드 불변).
  - `def build_arg_parser() -> argparse.ArgumentParser`, `def main(argv: list[str] | None = None) -> int`.
  - `main()` 호출 순서(테스트가 kubectl 페이크로 고정): 인자·날짜·파일·크기 검증(kubectl 0회) → `now = now_kst()` → `cm_name = configmap_name(now)`, `job = job_name(cronjob, timestamp(now))`(ConfigMap·Job이 **같은 ts**) → stdout `[INFO] configmap={cm_name} job={job} files=gpu.csv,serving.csv[,engine.csv] bytes={n}` → `kubectl create -n NS -f -`(ConfigMap JSON) → `try:` `kubectl get cronjob <cronjob> -n NS -o json` → `build_job_spec` → `kubectl apply -n NS -f -`(Job JSON) → `wait_job(C, NS, job, args.timeout_s)` → rc 0/1 `finally:` `--keep-configmap`이면 stdout `[INFO] ConfigMap 보존(--keep-configmap) — 정리: kubectl --context={C} delete configmap {cm_name} -n {NS}`, 아니면 `delete_configmap(...)`(예외·Ctrl-C 경로에서도 실행). rc 0일 때만 stdout `[NEXT] manual 적재 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3): python3 mart/token-metrics/tools/rerun.py --context C --namespace N --from D0 --to D1`(체인 실행 없음 — 실행 창 검사는 mart 측 책임) + `[INFO] Job 오브젝트는 남김(로그 재조회용) — 정리: kubectl --context={C} delete job {job} -n {NS}`.
  - 종료코드: **0** Job Complete / **1** Job Failed·타임아웃 / **2** 사용법·파일 부재·크기 초과(`SystemExit(2)`). `kubectl` 자체 실패(`CalledProcessError`)는 전파(traceback + 비0) — ConfigMap은 `finally`로 정리됨.

> **설계 해석 (T10)**: (a) ConfigMap은 `kubectl apply`가 아니라 **`kubectl create -f -`**로 만든다 — `apply`는 `kubectl.kubernetes.io/last-applied-configuration` 주석에 오브젝트 전체(= CSV 본문)를 한 번 더 저장하므로 900KB 본문이 etcd 요청 상한(1.5MiB)을 넘긴다. 이름이 타임스탬프로 유일하므로 create가 자연스럽다(Job은 T9와 같이 `apply`). (b) ConfigMap 값은 `utf-8-sig`로 읽어 BOM만 제거하고 내용은 검증하지 않는다 — 헤더 바이트 일치·주석·숫자 검증은 파드 안 T7 파서·T3 normalize 한 곳의 책임(§5.3 "행 단위 거부는 한 곳"). (c) 크기 기준은 BOM 제거 후 UTF-8 바이트 합계(ConfigMap `data`에 실리는 값 그대로). (d) Job 오브젝트는 지우지 않는다(CronJob history와 별개인 수동 Job — `kubectl logs job/…` 재조회 가능; 정리 명령을 마지막에 안내). (e) `[NEXT]`는 성공 시에만 출력하고 mart rerun을 체인하지 않는다 — 실행 창(10:50 KST·활성 Job 0) 검사는 mart rerun 자신의 책임이며 manual 적재는 낮 시간에 수행되는 일이 많아 즉시 체인이 창 검사에 걸리기 쉽다. manual_load.py 자체는 T9 `check_window`/`count_active_mart_jobs`를 복제하지 않는다: `--replace` 없는 첫 적재는 INSERT만이라 mart 10:20 배치와 겹쳐도 무해하고(§5.4 — 앵커 summary 가 마지막에 들어가므로 mart 는 그 날짜를 아직 보지 않는다), `--replace`(DELETE×3) 재적재의 실행 시각 규칙(10:50 KST 이후·활성 `token-mart-metrics` Job 0)은 README "수기 적재" 절이 운영 절차로 명시한다 — 코드 가드를 두 곳에 두면 기준(10:50·접두사)이 갈라질 때 한쪽만 고쳐지는 문제가 생긴다. (f) 사용법·파일·크기 오류는 T9와 같이 `ArgumentParser.exit(2, msg)`로 통일(`SystemExit(2)`; 테스트는 `pytest.raises(SystemExit)`로 코드 확인).

- [ ] **Step 0: 전제 확인 — T7 manual CLI · T8 cronjob.yaml 볼륨 순서 · T9 rerun.py · Plan 6a 템플릿 3파일**

```bash
cd /home/mini/github/token-data-pipeline
grep -n '"--manual-gpu"\|"--manual-serving"\|"--manual-engine"\|"--generated-at"' collectors/token-metrics/app/main.py
grep -n "^          volumes:\|            - name: endpoints\|            - name: ca-bundle\|activeDeadlineSeconds: 3000" collectors/token-metrics/k8s/base/cronjob.yaml
grep -n "^def kubectl\|^def wait_job\|^def now_kst\|^POLL_S" collectors/token-metrics/tools/rerun.py
ls docs/templates/token_metrics_manual_v0_gpu.csv docs/templates/token_metrics_manual_v0_serving.csv docs/templates/token_metrics_manual_v0_engine.csv
grep -n "manual_metrics" .gitignore
```

Expected: `main.py`에서 4개 인자(`--manual-gpu`, `--manual-serving`, `--manual-engine`, `--generated-at`) 각 1행; `cronjob.yaml`에서 `volumes:` 1행 + `- name: endpoints`·`- name: ca-bundle` (endpoints가 먼저; volumeMounts 쪽까지 각 2행) + `activeDeadlineSeconds: 3000` 1행; `rerun.py`에서 `POLL_S = 10`·`def kubectl`·`def now_kst`·`def wait_job` 4행; 템플릿 3파일 경로 3행; `.gitignore`에 `*manual_metrics*.csv` 1행. 하나라도 비면 중단하고 보고한다(선행 태스크·Plan 6a 산출물을 대신 만들지 않는다).

- [ ] **Step 1: 실패하는 테스트 (1/2)** — `collectors/token-metrics/tests/test_manual_load.py` (로드 패턴 · 상수 · 이름 · ConfigMap 본문)

T9 `tests/test_rerun.py`와 같은 importlib 로드 패턴(패키지화 없음). `cronjob_obj()`는 T8 실 매니페스트 + 서버 필드 — `build_job_spec`이 서버 필드를 버리고 **볼륨 `[2]`에 append**하는지 실 매니페스트로 검증한다.

```python
"""tools/manual_load.py 계약 테스트 (§5.5 전달 경로 P0 = k8s Job).

kubectl·시간·Job 대기는 전부 페이크 — 클러스터 없이 ConfigMap 본문·Job 스펙(/manual 볼륨 [2]·command)·
호출 순서(create → get cronjob → apply → wait → delete)·finally 삭제 보장·종료코드를 고정한다.
"""
import datetime as dt
import importlib.util
import json
import pathlib
import re
import subprocess

import pytest
import yaml

MODULE_ROOT = pathlib.Path(__file__).resolve().parents[1]
REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
TEMPLATES = REPO_ROOT / "docs" / "templates"
_ML_PATH = MODULE_ROOT / "tools" / "manual_load.py"
spec = importlib.util.spec_from_file_location("manual_load", _ML_PATH)
ml = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ml)

KST = dt.timezone(dt.timedelta(hours=9))
NOW = dt.datetime(2026, 9, 4, 11, 30, 0, tzinfo=KST)
TS = "20260904113000"
CM_NAME = "token-metrics-manual-" + TS
JOB_NAME = "token-metrics-collector-manual-" + TS

GPU_HEADER = "date,service,model,gpuType,category,gpuCount,gpuHours"
SERVING_HEADER = "date,service,model,metric,name,unit,p50,p90,p95,p99"
ENGINE_HEADER = "service,engine_type,engine_version"
GPU_ROW = "2026-08-26,Mock Service A,claude-sonnet-5,H100,serving,4,96.0"
SERVING_ROW = "2026-08-26,Mock Service A,claude-sonnet-5,outputTps,,,41.0,,,"
ENGINE_ROW = "Mock Service A,vllm,0.8.4"


def cronjob_obj():
    """T8 실 매니페스트 + 서버 필드(uid/resourceVersion/namespace) — kubectl get -o json 모사."""
    with open(MODULE_ROOT / "k8s" / "base" / "cronjob.yaml", encoding="utf-8") as fh:
        obj = yaml.safe_load(fh)
    obj["metadata"].update({"namespace": "monitoring", "resourceVersion": "123", "uid": "x"})
    return obj


def write_inputs(tmp_path, *, engine=False, gpu_bom=False):
    """최소 입력 3파일(헤더 + 예시 1행). gpu_bom=True 면 엑셀식 UTF-8 BOM 을 앞에 붙인다."""
    gpu = tmp_path / "gpu_manual_metrics.csv"
    serving = tmp_path / "serving_manual_metrics.csv"
    gpu_text = GPU_HEADER + "\n" + GPU_ROW + "\n"
    gpu.write_bytes((("\ufeff" if gpu_bom else "") + gpu_text).encode("utf-8"))
    serving.write_text(SERVING_HEADER + "\n" + SERVING_ROW + "\n", encoding="utf-8")
    paths = {"gpu": gpu, "serving": serving, "engine": None}
    if engine:
        eng = tmp_path / "engine_manual_metrics.csv"
        eng.write_text(ENGINE_HEADER + "\n" + ENGINE_ROW + "\n", encoding="utf-8")
        paths["engine"] = eng
    return paths


# ---- 상수 (§5.5 · §5.6) ---------------------------------------------------------

def test_constants():
    assert ml.CRONJOB == "token-metrics-collector"
    assert ml.CONFIGMAP_PREFIX == "token-metrics-manual-"
    assert ml.TS_FORMAT == "%Y%m%d%H%M%S"
    assert ml.MOUNT_PATH == "/manual" and ml.VOLUME_NAME == "manual"
    assert ml.FILE_KEYS == ("gpu.csv", "serving.csv", "engine.csv")
    assert ml.LABELS == {"app": "token-metrics-collector", "manual": "1"}
    assert ml.MAX_CONFIGMAP_BYTES == 900_000
    assert ml.POLL_S == 10
    assert ml.TIMEOUT_S == 3600 == \
        cronjob_obj()["spec"]["jobTemplate"]["spec"]["activeDeadlineSeconds"] + 600
    assert ml.KST.utcoffset(None) == dt.timedelta(hours=9)
    assert ml.MART_RERUN == "mart/token-metrics/tools/rerun.py"


# ---- 이름 (DNS-1123) ------------------------------------------------------------

def test_configmap_name_format():
    assert ml.timestamp(NOW) == TS
    assert ml.configmap_name(NOW) == "token-metrics-manual-20260904113000"
    assert re.fullmatch(r"^[a-z0-9-]+$", ml.configmap_name(NOW))
    # UTC aware 입력은 KST 로 환산 (02:30Z = 11:30 KST)
    assert ml.configmap_name(dt.datetime(2026, 9, 4, 2, 30, 0, tzinfo=dt.timezone.utc)) == CM_NAME
    with pytest.raises(ValueError):
        ml.timestamp(dt.datetime(2026, 9, 4, 11, 30, 0))                # naive 금지 (KST 규율)


def test_job_name_with_verify_suffix_fits_63():
    assert ml.job_name("token-metrics-collector", TS) == JOB_NAME
    name = ml.job_name("token-metrics-collector-verify", TS)
    assert name == "token-metrics-collector-verify-manual-20260904113000"
    assert len(name) <= 63 and re.fullmatch(r"^[a-z0-9-]+$", name)


def test_now_kst_is_aware_kst():
    now = ml.now_kst()
    assert now.tzinfo is not None and now.utcoffset() == dt.timedelta(hours=9)


# ---- ConfigMap 본문 --------------------------------------------------------------

def test_configmap_body_from_files(tmp_path):
    p = write_inputs(tmp_path, gpu_bom=True)
    files = ml.read_manual_files(p["gpu"], p["serving"], None)
    assert list(files) == ["gpu.csv", "serving.csv"]                    # engine 없음 · 키 순서
    assert files["gpu.csv"].startswith("date,service,")                  # BOM 제거
    assert "\ufeff" not in files["gpu.csv"]
    assert files["serving.csv"].splitlines()[1] == SERVING_ROW
    cm = ml.build_configmap(CM_NAME, files)
    assert cm["apiVersion"] == "v1" and cm["kind"] == "ConfigMap"
    assert cm["metadata"] == {"name": CM_NAME,
                              "labels": {"app": "token-metrics-collector", "manual": "1"}}
    assert set(cm["data"]) == {"gpu.csv", "serving.csv"}
    assert cm["metadata"]["labels"]["manual"] == "1"
    assert cm["data"]["gpu.csv"] == files["gpu.csv"]                     # 값 그대로 (검증·가공 없음)
    json.dumps(cm)                                                       # kubectl -f - 로 보낼 수 있는 JSON


def test_configmap_crlf_normalized_and_engine_key(tmp_path):
    p = write_inputs(tmp_path, engine=True)
    p["gpu"].write_bytes((GPU_HEADER + "\r\n" + GPU_ROW + "\r\n").encode("utf-8"))   # 엑셀 CRLF
    files = ml.read_manual_files(p["gpu"], p["serving"], p["engine"])
    assert list(files) == ["gpu.csv", "serving.csv", "engine.csv"]
    assert files["gpu.csv"] == GPU_HEADER + "\n" + GPU_ROW + "\n"        # universal newline → LF
    assert files["engine.csv"].splitlines()[0] == ENGINE_HEADER
    assert ml.total_bytes(files) == sum(len(v.encode("utf-8")) for v in files.values())


def test_read_manual_files_missing_raises(tmp_path):
    p = write_inputs(tmp_path)
    with pytest.raises(FileNotFoundError):
        ml.read_manual_files(tmp_path / "nope.csv", p["serving"], None)
    with pytest.raises(FileNotFoundError):
        ml.read_manual_files(p["gpu"], p["serving"], tmp_path / "nope_engine.csv")


def test_build_configmap_rejects_unknown_key():
    with pytest.raises(ValueError):
        ml.build_configmap(CM_NAME, {"gpu.csv": "x", "extra.csv": "y"})
```

- [ ] **Step 2: 실패하는 테스트 (2/2)** — 같은 파일에 이어서 붙인다 (Job 스펙 `/manual` 볼륨 `[2]` · command 순서 · `main()` 호출 순서/finally 삭제/종료코드 · 크기 가드 · 사용법 · 템플릿 왕복)

```python
# ---- Job 스펙 (/manual 볼륨 [2] · command override) ------------------------------

def test_job_spec_has_manual_volume_and_command():
    obj = cronjob_obj()
    cmd = ml.build_manual_command("2026-08-26", "2026-08-31", engine=False, service=None,
                                  replace=False, generated_at=None)
    job = ml.build_job_spec(obj, JOB_NAME, cmd, CM_NAME)
    assert job["apiVersion"] == "batch/v1" and job["kind"] == "Job"
    assert job["metadata"] == {"name": JOB_NAME,
                               "labels": {"app": "token-metrics-collector", "manual": "1"}}
    assert "uid" not in job["metadata"] and "resourceVersion" not in job["metadata"]
    assert job["spec"]["activeDeadlineSeconds"] == 3000                  # override 없음 — CronJob 값 상속
    assert job["spec"]["backoffLimit"] == 0
    pod = job["spec"]["template"]["spec"]
    assert pod["volumes"][0]["name"] == "endpoints"                      # 기존 볼륨 보존 (index 계약)
    assert pod["volumes"][1]["name"] == "ca-bundle"
    assert pod["volumes"][2] == {"name": "manual",
                                 "configMap": {"name": "token-metrics-manual-20260904113000"}}
    assert len(pod["volumes"]) == 3
    container = pod["containers"][0]
    assert container["name"] == "token-metrics-collector"
    assert container["volumeMounts"][0]["mountPath"] == "/etc/token-metrics"
    assert container["volumeMounts"][2] == {"name": "manual", "mountPath": "/manual",
                                            "readOnly": True}
    assert container["volumeMounts"][2]["readOnly"] is True
    assert container["command"][:7] == ["python", "-m", "app.main",
                                        "--manual-gpu", "/manual/gpu.csv",
                                        "--manual-serving", "/manual/serving.csv"]
    assert container["command"] == cmd
    assert obj == cronjob_obj()                                          # deepcopy — 원본 불변
    json.dumps(job)


def test_job_spec_rejects_double_manual_volume():
    obj = cronjob_obj()
    pod = obj["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    pod["volumes"].append({"name": "manual", "configMap": {"name": "stale"}})
    with pytest.raises(ValueError):
        ml.build_job_spec(obj, JOB_NAME, ["python", "-m", "app.main"], CM_NAME)


def test_engine_optional():
    base = ["python", "-m", "app.main", "--manual-gpu", "/manual/gpu.csv",
            "--manual-serving", "/manual/serving.csv"]
    rng = ["--from", "2026-08-26", "--to", "2026-08-31"]
    cmd = ml.build_manual_command("2026-08-26", "2026-08-31", engine=False, service=None,
                                  replace=False, generated_at=None)
    assert cmd == base + rng
    assert "--manual-engine" not in cmd
    cmd = ml.build_manual_command("2026-08-26", "2026-08-31", engine=True, service=None,
                                  replace=False, generated_at=None)
    assert cmd == base + ["--manual-engine", "/manual/engine.csv"] + rng
    assert cmd.index("--manual-engine") < cmd.index("--from")            # engine 이 --from 앞
    cmd = ml.build_manual_command("2026-08-26", "2026-08-31", engine=True, service="Mock Service A",
                                  replace=True, generated_at="2026-09-01T09:00:00+09:00")
    assert cmd == base + ["--manual-engine", "/manual/engine.csv"] + rng + \
        ["--service", "Mock Service A", "--replace", "--generated-at", "2026-09-01T09:00:00+09:00"]
    assert cmd[-2:] == ["--generated-at", "2026-09-01T09:00:00+09:00"]  # 마지막 2원소
    cmd = ml.build_manual_command("2026-08-26", "2026-08-26", engine=False, service=None,
                                  replace=True, generated_at=None)
    assert cmd[-1] == "--replace" and "--service" not in cmd and "--generated-at" not in cmd


# ---- main(): create → get cronjob → apply → wait → finally delete -----------------

class FakeK8s:
    """ml.kubectl 대체 — 호출 인자 목록을 기록. create/apply -f - 본문(JSON)은 kind 별로 보관."""

    def __init__(self, cronjob, *, delete_fails=False, job_apply_fails=False):
        self.cronjob = cronjob
        self.calls = []                # list[list[str]] — kubectl 인자 그대로
        self.created = []              # create -f - 본문 (ConfigMap)
        self.applied = []              # apply -f - 본문 (Job)
        self.delete_fails = delete_fails
        self.job_apply_fails = job_apply_fails

    def __call__(self, context, args, *, capture=False, input_data=None):
        args = list(args)
        assert context == "c"
        self.calls.append(args)
        if args[:2] == ["get", "cronjob"]:
            assert args == ["get", "cronjob", "token-metrics-collector", "-n", "monitoring",
                            "-o", "json"]
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(self.cronjob), stderr="")
        if args[0] == "create":
            assert args[1:] == ["-n", "monitoring", "-f", "-"]
            body = json.loads(input_data)
            assert body["kind"] == "ConfigMap"
            self.created.append(body)
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[0] == "apply":
            assert args[1:] == ["-n", "monitoring", "-f", "-"]
            body = json.loads(input_data)
            assert body["kind"] == "Job"
            if self.job_apply_fails:
                raise subprocess.CalledProcessError(1, ["kubectl"] + args)
            self.applied.append(body)
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:2] == ["delete", "configmap"]:
            if self.delete_fails:
                raise subprocess.CalledProcessError(1, ["kubectl"] + args)
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected kubectl {args}")


def _run_main(monkeypatch, argv, *, wait_result, **k8s_kw):
    k8s = FakeK8s(cronjob_obj(), **k8s_kw)
    waited = []

    def fake_wait(context, namespace, job_name, timeout_s):
        waited.append((context, namespace, job_name, timeout_s))
        k8s.calls.append(["<wait>", job_name])           # 순서 검증용 마커 (kubectl 호출 아님)
        return wait_result

    monkeypatch.setattr(ml, "kubectl", k8s)
    monkeypatch.setattr(ml, "wait_job", fake_wait)
    monkeypatch.setattr(ml, "now_kst", lambda: NOW)
    rc = ml.main(argv)
    return rc, k8s, waited


def _argv(p, *extra):
    argv = ["--context", "c", "--from", "2026-08-26", "--to", "2026-08-31",
            "--gpu", str(p["gpu"]), "--serving", str(p["serving"])]
    if p["engine"] is not None:
        argv += ["--engine", str(p["engine"])]
    return argv + list(extra)


DELETE_CALL = ["delete", "configmap", CM_NAME, "-n", "monitoring", "--ignore-not-found"]


def test_configmap_deleted_on_success(monkeypatch, tmp_path, capsys):
    p = write_inputs(tmp_path, engine=True)
    rc, k8s, waited = _run_main(monkeypatch, _argv(p), wait_result=True)
    assert rc == 0
    # 호출 순서: create(ConfigMap) → get cronjob → apply(Job) → wait → delete configmap (마지막)
    assert k8s.calls == [["create", "-n", "monitoring", "-f", "-"],
                         ["get", "cronjob", "token-metrics-collector", "-n", "monitoring", "-o", "json"],
                         ["apply", "-n", "monitoring", "-f", "-"],
                         ["<wait>", JOB_NAME],
                         DELETE_CALL]
    assert sum(1 for c in k8s.calls if c[:2] == ["delete", "configmap"]) == 1
    assert waited == [("c", "monitoring", JOB_NAME, 3600)]              # TIMEOUT_S 기본
    cm, job = k8s.created[0], k8s.applied[0]
    assert cm["metadata"]["name"] == CM_NAME
    assert list(cm["data"]) == ["gpu.csv", "serving.csv", "engine.csv"]
    assert job["metadata"]["name"] == JOB_NAME                          # ConfigMap 과 같은 ts
    assert job["spec"]["template"]["spec"]["volumes"][2]["configMap"]["name"] == cm["metadata"]["name"]
    assert job["spec"]["template"]["spec"]["containers"][0]["command"] == [
        "python", "-m", "app.main", "--manual-gpu", "/manual/gpu.csv",
        "--manual-serving", "/manual/serving.csv", "--manual-engine", "/manual/engine.csv",
        "--from", "2026-08-26", "--to", "2026-08-31"]
    out = capsys.readouterr().out
    n = ml.total_bytes(ml.read_manual_files(p["gpu"], p["serving"], p["engine"]))
    assert f"[INFO] configmap={CM_NAME} job={JOB_NAME} files=gpu.csv,serving.csv,engine.csv bytes={n}" in out
    assert ("[NEXT] manual 적재 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3): "
            "python3 mart/token-metrics/tools/rerun.py --context c --namespace monitoring "
            "--from 2026-08-26 --to 2026-08-31") in out
    assert f"[INFO] Job 오브젝트는 남김(로그 재조회용) — 정리: kubectl --context=c delete job {JOB_NAME} -n monitoring" in out


def test_keep_configmap_skips_delete(monkeypatch, tmp_path, capsys):
    p = write_inputs(tmp_path)
    rc, k8s, _ = _run_main(monkeypatch, _argv(p, "--keep-configmap"), wait_result=True)
    assert rc == 0
    assert not any(c[:2] == ["delete", "configmap"] for c in k8s.calls)   # delete 0회
    assert list(k8s.created[0]["data"]) == ["gpu.csv", "serving.csv"]     # engine 없음
    out = capsys.readouterr().out
    assert f"[INFO] configmap={CM_NAME} job={JOB_NAME} files=gpu.csv,serving.csv bytes=" in out
    assert f"[INFO] ConfigMap 보존(--keep-configmap) — 정리: kubectl --context=c delete configmap {CM_NAME} -n monitoring" in out


def test_configmap_deleted_on_failure(monkeypatch, tmp_path, capsys):
    p = write_inputs(tmp_path)
    rc, k8s, waited = _run_main(monkeypatch, _argv(p), wait_result=False)
    assert rc == 1
    assert k8s.calls[-1] == DELETE_CALL                                   # 실패해도 마지막은 삭제
    assert len(waited) == 1
    assert "[NEXT]" not in capsys.readouterr().out                        # 실패 시 mart 안내 없음
```

같은 파일에 이어서 (삭제 실패 WARN · 예외 경로 finally · 크기 가드 · 사용법 · `--timeout-s`/`--service`/`--replace`/`--generated-at` 전파 · 템플릿 왕복):

```python
def test_configmap_delete_failure_is_warn_only(monkeypatch, tmp_path, capsys):
    p = write_inputs(tmp_path)
    rc, k8s, _ = _run_main(monkeypatch, _argv(p), wait_result=True, delete_fails=True)
    assert rc == 0                                                        # 종료코드 불변
    assert k8s.calls[-1] == DELETE_CALL
    err = capsys.readouterr().err
    assert (f"[WARN] ConfigMap 삭제 실패 — 수동 삭제: kubectl --context=c delete configmap "
            f"{CM_NAME} -n monitoring") in err


def test_configmap_deleted_when_job_apply_raises(monkeypatch, tmp_path):
    p = write_inputs(tmp_path)
    with pytest.raises(subprocess.CalledProcessError):
        _run_main(monkeypatch, _argv(p), wait_result=True, job_apply_fails=True)
    # 예외가 전파돼도 finally 가 ConfigMap 을 지운다 — 페이크는 monkeypatch 된 ml.kubectl 에 남아 있다
    k8s = ml.kubectl
    assert k8s.calls[-1] == DELETE_CALL
    assert k8s.applied == [] and len(k8s.created) == 1


def test_timeout_and_passthrough_flags(monkeypatch, tmp_path):
    p = write_inputs(tmp_path)
    rc, k8s, waited = _run_main(
        monkeypatch, _argv(p, "--service", "Mock Service A", "--replace",
                           "--generated-at", "2026-09-01T09:00:00+09:00", "--timeout-s", "120"),
        wait_result=True)
    assert rc == 0
    assert waited == [("c", "monitoring", JOB_NAME, 120)]
    cmd = k8s.applied[0]["spec"]["template"]["spec"]["containers"][0]["command"]
    assert cmd[-5:] == ["--service", "Mock Service A", "--replace",
                        "--generated-at", "2026-09-01T09:00:00+09:00"]
    assert cmd == ["python", "-m", "app.main", "--manual-gpu", "/manual/gpu.csv",
                   "--manual-serving", "/manual/serving.csv",
                   "--from", "2026-08-26", "--to", "2026-08-31",
                   "--service", "Mock Service A", "--replace",
                   "--generated-at", "2026-09-01T09:00:00+09:00"]


def test_cronjob_override_changes_job_name_and_get(monkeypatch, tmp_path):
    p = write_inputs(tmp_path)
    k8s = FakeK8s(cronjob_obj())
    seen = []

    def relaxed(context, args, *, capture=False, input_data=None):
        seen.append(list(args))
        if args[:2] == ["get", "cronjob"]:
            assert args[2] == "token-metrics-collector-verify"
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(k8s.cronjob), stderr="")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(ml, "kubectl", relaxed)
    monkeypatch.setattr(ml, "wait_job", lambda c, n, j, t: True)
    monkeypatch.setattr(ml, "now_kst", lambda: NOW)
    rc = ml.main(_argv(p, "--cronjob", "token-metrics-collector-verify"))
    assert rc == 0
    assert seen[1][2] == "token-metrics-collector-verify"
    assert seen[-1] == DELETE_CALL                                        # ConfigMap 이름은 cronjob 무관


def test_size_guard(monkeypatch, tmp_path, capsys):
    p = write_inputs(tmp_path)
    big = GPU_HEADER + "\n" + ("2026-08-26,Mock Service A,m,H100,serving,1,1.0\n" * 21_000)  # 47 bytes x 21_000 = 987_000
    assert len(big.encode("utf-8")) >= 950_000
    p["gpu"].write_text(big, encoding="utf-8")
    k8s = FakeK8s(cronjob_obj())
    monkeypatch.setattr(ml, "kubectl", k8s)
    monkeypatch.setattr(ml, "now_kst", lambda: NOW)
    with pytest.raises(SystemExit) as e:
        ml.main(_argv(p))
    assert e.value.code == 2
    assert k8s.calls == []                                                # create/apply 0회
    n = ml.total_bytes(ml.read_manual_files(p["gpu"], p["serving"], None))
    assert f"[ERROR] CSV 합계 {n} bytes > 900000 — 날짜 범위를 나눠 제출" in capsys.readouterr().err


def test_usage_errors(monkeypatch, tmp_path, capsys):
    p = write_inputs(tmp_path)
    k8s = FakeK8s(cronjob_obj())
    monkeypatch.setattr(ml, "kubectl", k8s)
    monkeypatch.setattr(ml, "now_kst", lambda: NOW)
    with pytest.raises(SystemExit) as e:                                  # --gpu 부재 파일
        ml.main(["--context", "c", "--from", "2026-08-26", "--to", "2026-08-31",
                 "--gpu", str(tmp_path / "missing_manual_metrics.csv"), "--serving", str(p["serving"])])
    assert e.value.code == 2
    assert "[ERROR] 파일 없음: " in capsys.readouterr().err
    with pytest.raises(SystemExit) as e:                                  # --engine 부재 파일
        ml.main(_argv(p, "--engine", str(tmp_path / "missing_engine.csv")))
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:                                  # --from > --to
        ml.main(["--context", "c", "--from", "2026-09-10", "--to", "2026-09-01",
                 "--gpu", str(p["gpu"]), "--serving", str(p["serving"])])
    assert e.value.code == 2
    assert "--from(2026-09-10) > --to(2026-09-01)" in capsys.readouterr().err
    with pytest.raises(SystemExit) as e:                                  # 날짜 형식
        ml.main(["--context", "c", "--from", "2026/08/26", "--to", "2026-08-31",
                 "--gpu", str(p["gpu"]), "--serving", str(p["serving"])])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:                                  # --serving 없음 (required)
        ml.main(["--context", "c", "--from", "2026-08-26", "--to", "2026-08-31", "--gpu", str(p["gpu"])])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:                                  # --from 없음 (required)
        ml.main(["--context", "c", "--to", "2026-08-31", "--gpu", str(p["gpu"]), "--serving", str(p["serving"])])
    assert e.value.code == 2
    assert k8s.calls == []                                                # 전부 kubectl 호출 전


def test_arg_parser_defaults():
    args = ml.build_arg_parser().parse_args(
        ["--context", "c", "--from", "2026-08-26", "--to", "2026-08-31", "--gpu", "g", "--serving", "s"])
    assert args.namespace == "monitoring" and args.cronjob == "token-metrics-collector"
    assert args.engine is None and args.service is None and args.generated_at is None
    assert args.replace is False and args.keep_configmap is False
    assert args.timeout_s == 3600


# ---- 템플릿 3파일 왕복 (Plan 6a F) ------------------------------------------------

def test_templates_round_trip_to_command():
    gpu = TEMPLATES / "token_metrics_manual_v0_gpu.csv"
    serving = TEMPLATES / "token_metrics_manual_v0_serving.csv"
    engine = TEMPLATES / "token_metrics_manual_v0_engine.csv"
    files = ml.read_manual_files(gpu, serving, engine)
    cm = ml.build_configmap(CM_NAME, files)
    assert set(cm["data"]) == {"gpu.csv", "serving.csv", "engine.csv"}
    first_rows = {k: [ln for ln in v.splitlines() if ln and not ln.lstrip().startswith("#")][0]
                  for k, v in cm["data"].items()}
    assert first_rows["gpu.csv"] == GPU_HEADER
    assert first_rows["serving.csv"] == SERVING_HEADER
    assert first_rows["engine.csv"] == "service,engine_type,engine_version"
    assert ml.total_bytes(files) < ml.MAX_CONFIGMAP_BYTES
    job = ml.build_job_spec(cronjob_obj(), JOB_NAME,
                            ml.build_manual_command("2026-08-26", "2026-08-26", engine=True,
                                                    service=None, replace=False, generated_at=None),
                            CM_NAME)
    cmd = job["spec"]["template"]["spec"]["containers"][0]["command"]
    assert cmd == ["python", "-m", "app.main", "--manual-gpu", "/manual/gpu.csv",
                   "--manual-serving", "/manual/serving.csv", "--manual-engine", "/manual/engine.csv",
                   "--from", "2026-08-26", "--to", "2026-08-26"]
    # 파드 안 경로 = 마운트 경로 + ConfigMap 키 (T7 파서가 여는 파일)
    mount = job["spec"]["template"]["spec"]["containers"][0]["volumeMounts"][2]["mountPath"]
    assert {f"{mount}/{k}" for k in cm["data"]} == {"/manual/gpu.csv", "/manual/serving.csv", "/manual/engine.csv"}
```

- [ ] **Step 3: 실패 확인**

Run: `cd collectors/token-metrics && python3 -m pytest tests/test_manual_load.py -q`
Expected: 수집 단계 오류 1건 — `ERROR tests/test_manual_load.py - FileNotFoundError: [Errno 2] No such file or directory: '…/collectors/token-metrics/tools/manual_load.py'` (importlib `exec_module`가 부재 파일에서 실패), 마지막 줄 `1 error`.

- [ ] **Step 4: 구현 (1/2)** — `collectors/token-metrics/tools/manual_load.py` (docstring · 상수 · `kubectl` · 이름 · 파일 읽기 · ConfigMap/command/Job 빌더)

T9 `tools/rerun.py`에서 `kubectl()`·`now_kst()`·`wait_job()`을 **본문 동일하게 복제**한다(import 없음 — 운영자가 이 파일 1개만 워크스테이션에 복사해 쓴다). 파일 전체(2/2와 이어 붙여 1개 파일):

```python
"""token-metrics 수기(manual-v0) CSV 적재 도구 — 전달 경로 P0 = k8s Job (설계 §5.5 · §5.6).

워크스테이션의 CSV 3파일(gpu·serving·선택 engine)을 ConfigMap `token-metrics-manual-<ts>`로
올리고, CronJob `token-metrics-collector` 템플릿에서 1회성 Job을 만들어(`/manual` 볼륨 마운트 +
command `python -m app.main --manual-gpu /manual/gpu.csv --manual-serving /manual/serving.csv
[--manual-engine /manual/engine.csv] --from --to [--service] [--replace] [--generated-at]`)
로그를 스트리밍한 뒤 **완료·실패·예외 어느 경로에서든** ConfigMap을 삭제한다(--keep-configmap 제외).
운영자 워크스테이션에는 kubectl(대상 context)만 있으면 된다 — ClickHouse 직접 접근·프록시·CA 불필요.

CSV 내용은 검증하지 않는다(BOM 제거만) — 헤더 바이트 일치·주석·숫자·서비스 등록은 파드 안
app.manual(T7)·app.normalize(T3) 한 곳의 책임. 앵커가 있는 (date, service)는 --replace 없이는
SKIPPED reason=already_loaded (§5.5 안전 기본값).

CSV는 리포에 커밋하지 말 것 — 실제 제출 파일은 *manual_metrics*.csv 이름으로 저장(.gitignore, §7.2).
템플릿: docs/templates/token_metrics_manual_v0_{gpu,serving,engine}.csv

사용법:
  python3 tools/manual_load.py --context prod --from 2026-08-26 --to 2026-08-31 \
      --gpu ~/metrics/gpu.csv --serving ~/metrics/serving.csv --engine ~/metrics/engine.csv \
      --generated-at 2026-09-01T09:00:00+09:00
  python3 tools/manual_load.py --context prod --from 2026-08-26 --to 2026-08-26 \
      --gpu ~/metrics/gpu.csv --serving ~/metrics/serving.csv --service "Mock Service A" --replace

옵션:
  --context         kubectl context (필수)
  --namespace       기본 monitoring
  --cronjob         템플릿 CronJob 이름 (기본 token-metrics-collector — company-verify는
                    token-metrics-collector-verify)
  --from/--to       YYYY-MM-DD, KST, 둘 다 inclusive. 필수 쌍 (범위 밖 행은 파드가 rows_outside_range 로 셈)
  --gpu/--serving   CSV 경로 (필수). --engine 은 선택 (엔진 자기신고 — 없으면 engine_type '')
  --service         단일 서비스만 (endpoints.yaml의 service 정본)
  --replace         앵커 존재 (date, service) 도 교체 (§5.4 DELETE summary→gpu→serving 후 INSERT)
  --generated-at    ISO 8601 (+09:00 권장) — 없으면 파드가 적재 시각을 쓴다
  --timeout-s       Job 완료 대기 상한 (기본 3600 = activeDeadlineSeconds 3000 + 600)
  --keep-configmap  완료 후 ConfigMap 을 지우지 않는다 (디버그 — 정리 명령을 출력)

종료코드: 0 Job Complete / 1 Job Failed·타임아웃 / 2 사용법·파일 부재·CSV 합계 > 900000 bytes
완료 후 동일 날짜 범위의 mart-metrics rerun 은 의무(§6.3) — 명령을 출력만 하고 체인하지 않는다
(실행 창 10:50 KST·활성 mart Job 0 검사는 mart rerun 자신의 책임).
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import pathlib
import shlex
import subprocess
import sys
import time

CRONJOB = "token-metrics-collector"
CONFIGMAP_PREFIX = "token-metrics-manual-"
TS_FORMAT = "%Y%m%d%H%M%S"        # 14자리 KST — ConfigMap·Job 이름 공용 (DNS-1123: 소문자·숫자·하이픈)
MOUNT_PATH = "/manual"            # 파드 안 CSV 경로 = MOUNT_PATH + "/" + FILE_KEYS[i] (T7 CLI 계약)
VOLUME_NAME = "manual"            # cronjob.yaml volumes [0] endpoints · [1] ca-bundle 뒤에 [2] 로 append
FILE_KEYS = ("gpu.csv", "serving.csv", "engine.csv")
LABELS = {"app": CRONJOB, "manual": "1"}
MAX_CONFIGMAP_BYTES = 900_000     # k8s ConfigMap 1MiB 한도 여유 (create 사용 — apply 의 last-applied 주석 없음)
POLL_S = 10
TIMEOUT_S = 3000 + 600            # 서버 activeDeadlineSeconds(§5.2) + 폴링 마진 600
KST = dt.timezone(dt.timedelta(hours=9))
MART_RERUN = "mart/token-metrics/tools/rerun.py"   # Plan 6c 산출 경로 — 안내만 (체인 없음)


def kubectl(context, args, *, capture=False, input_data=None):
    cmd = ["kubectl", f"--context={context}", "--insecure-skip-tls-verify"] + list(args)
    return subprocess.run(cmd, check=True, text=True, input=input_data,
                          capture_output=capture)


def now_kst():
    """aware KST 현재 시각 — 테스트는 이 함수를 페이크로 바꾼다 (datetime.now 는 C 타입이라 불가)."""
    return dt.datetime.now(KST)


def timestamp(now):
    """aware datetime → KST 14자리. naive 는 거부 (KST 규율)."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("timestamp: aware datetime required (KST)")
    return now.astimezone(KST).strftime(TS_FORMAT)


def configmap_name(now_kst):
    return CONFIGMAP_PREFIX + timestamp(now_kst)


def job_name(cronjob, ts):
    # token-metrics-collector-verify-manual-YYYYmmddHHMMSS = 52자 ≤ 63 (DNS-1123 label)
    return f"{cronjob}-manual-{ts}"


def read_manual_files(gpu, serving, engine):
    """CSV → {ConfigMap 키: 텍스트}. utf-8-sig 로 BOM 제거, universal newline 으로 CRLF→LF.
    내용은 검증하지 않는다(파드 안 T7 파서 책임). 부재 파일은 FileNotFoundError 그대로."""
    files = {
        "gpu.csv": pathlib.Path(gpu).read_text(encoding="utf-8-sig"),
        "serving.csv": pathlib.Path(serving).read_text(encoding="utf-8-sig"),
    }
    if engine is not None:
        files["engine.csv"] = pathlib.Path(engine).read_text(encoding="utf-8-sig")
    return files


def total_bytes(files):
    """ConfigMap data 에 실리는 UTF-8 바이트 합계 (BOM 제거 후)."""
    return sum(len(v.encode("utf-8")) for v in files.values())


def build_configmap(name, files):
    unknown = set(files) - set(FILE_KEYS)
    if unknown:
        raise ValueError(f"build_configmap: unknown keys {sorted(unknown)} (allowed {FILE_KEYS})")
    return {"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": name, "labels": dict(LABELS)},
            "data": dict(files)}


def build_manual_command(from_d, to_d, *, engine, service, replace, generated_at):
    """T7 manual 모드 CLI (§5.5) — 인자 순서 고정: gpu·serving → [engine] → from/to → [service] → [replace] → [generated-at]."""
    cmd = ["python", "-m", "app.main",
           "--manual-gpu", f"{MOUNT_PATH}/gpu.csv",
           "--manual-serving", f"{MOUNT_PATH}/serving.csv"]
    if engine:
        cmd += ["--manual-engine", f"{MOUNT_PATH}/engine.csv"]
    cmd += ["--from", from_d, "--to", to_d]
    if service:
        cmd += ["--service", service]
    if replace:
        cmd += ["--replace"]
    if generated_at:
        cmd += ["--generated-at", generated_at]
    return cmd


def build_job_spec(cronjob_obj, job_name, command, configmap_name):
    """CronJob 오브젝트 → 1회성 Job 스펙: containers[0].command override + /manual 볼륨 append.

    metadata 는 name + 라벨만 (uid/resourceVersion/namespace 등 서버 필드 제거).
    activeDeadlineSeconds 는 jobTemplate.spec 값(3000, §5.2) 그대로 상속.
    volumes/volumeMounts 는 T8 계약 순서([0] endpoints, [1] ca-bundle) 뒤 [2] 에 append."""
    spec = copy.deepcopy(cronjob_obj["spec"]["jobTemplate"]["spec"])
    pod = spec["template"]["spec"]
    volumes = pod.setdefault("volumes", [])
    if any(v.get("name") == VOLUME_NAME for v in volumes):
        raise ValueError(f"build_job_spec: volume '{VOLUME_NAME}' already present in CronJob template")
    container = pod["containers"][0]
    container["command"] = list(command)
    volumes.append({"name": VOLUME_NAME, "configMap": {"name": configmap_name}})
    container.setdefault("volumeMounts", []).append(
        {"name": VOLUME_NAME, "mountPath": MOUNT_PATH, "readOnly": True})
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": job_name, "labels": dict(LABELS)},
            "spec": spec}
```

- [ ] **Step 5: 구현 (2/2)** — 같은 파일에 이어서 (`wait_job` 복제 · `delete_configmap` · argparse · `main`)

```python
def wait_job(context, namespace, job_name, timeout_s):
    """Job 완료 폴링 + 파드 로그 스트리밍. 성공 True / 실패·타임아웃 False. (T9 rerun.py 와 동일 본문)

    backoffLimit=0(§5.2)이라 파드는 1개지만, 파드 집합 순회 골격은 기존 모듈과 동일하게 둔다 —
    마커 라인(MANUAL_INPUT/SERVICE_RESULT/BATCH_RESULT)이 운영 기록이므로 가공 없이 그대로 출력."""
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


def delete_configmap(context, namespace, name):
    """finally 경로용 — 실패해도 예외를 내지 않고 WARN + 수동 삭제 명령만 안내 (종료코드 불변)."""
    try:
        kubectl(context, ["delete", "configmap", name, "-n", namespace, "--ignore-not-found"])
        return True
    except (subprocess.CalledProcessError, OSError):
        print(f"[WARN] ConfigMap 삭제 실패 — 수동 삭제: kubectl --context={context} "
              f"delete configmap {name} -n {namespace}", file=sys.stderr)
        return False


def build_arg_parser():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", default="monitoring")
    p.add_argument("--cronjob", default=CRONJOB,
                   help=f"템플릿 CronJob 이름 (기본 {CRONJOB}; company-verify는 {CRONJOB}-verify)")
    p.add_argument("--from", dest="from_d", required=True, help="YYYY-MM-DD (KST, inclusive)")
    p.add_argument("--to", dest="to_d", required=True, help="YYYY-MM-DD (KST, inclusive)")
    p.add_argument("--gpu", required=True, help="gpu CSV (템플릿 token_metrics_manual_v0_gpu.csv)")
    p.add_argument("--serving", required=True, help="serving CSV (템플릿 token_metrics_manual_v0_serving.csv)")
    p.add_argument("--engine", default=None, help="engine CSV (선택, 템플릿 token_metrics_manual_v0_engine.csv)")
    p.add_argument("--service", default=None)
    p.add_argument("--replace", action="store_true")
    p.add_argument("--generated-at", dest="generated_at", default=None,
                   help="ISO 8601 제출 시각 (+09:00 권장) — 없으면 파드가 적재 시각을 쓴다")
    p.add_argument("--timeout-s", dest="timeout_s", type=int, default=TIMEOUT_S,
                   help=f"Job 완료 대기 상한 초 (기본 {TIMEOUT_S})")
    p.add_argument("--keep-configmap", dest="keep_configmap", action="store_true",
                   help="완료 후 ConfigMap 을 지우지 않음 (디버그)")
    return p


def main(argv=None):
    p = build_arg_parser()
    args = p.parse_args(argv)
    try:
        d0, d1 = dt.date.fromisoformat(args.from_d), dt.date.fromisoformat(args.to_d)
    except ValueError:
        p.exit(2, "--from/--to는 YYYY-MM-DD 형식\n")
    if d0 > d1:
        p.exit(2, f"--from({d0}) > --to({d1})\n")
    from_s, to_s = d0.isoformat(), d1.isoformat()

    # 파일 존재 → 읽기(BOM 제거) → 크기 가드 — 전부 kubectl 호출 전 (실패 시 클러스터 무변경)
    paths = [pathlib.Path(args.gpu), pathlib.Path(args.serving)]
    engine_path = pathlib.Path(args.engine) if args.engine else None
    if engine_path is not None:
        paths.append(engine_path)
    for path in paths:
        if not path.is_file():
            p.exit(2, f"[ERROR] 파일 없음: {path}\n")
    files = read_manual_files(paths[0], paths[1], engine_path)
    n_bytes = total_bytes(files)
    if n_bytes > MAX_CONFIGMAP_BYTES:
        p.exit(2, f"[ERROR] CSV 합계 {n_bytes} bytes > {MAX_CONFIGMAP_BYTES} — 날짜 범위를 나눠 제출\n")

    ctx, ns = args.context, args.namespace
    now = now_kst()
    ts = timestamp(now)
    cm_name = configmap_name(now)
    job = job_name(args.cronjob, ts)                     # ConfigMap 과 같은 ts
    print(f"[INFO] configmap={cm_name} job={job} files={','.join(files)} bytes={n_bytes}", flush=True)

    # create: apply 는 last-applied 주석에 본문을 한 번 더 저장해 etcd 요청 상한을 넘길 수 있다 (설계 해석 a)
    kubectl(ctx, ["create", "-n", ns, "-f", "-"], input_data=json.dumps(build_configmap(cm_name, files)))
    rc = 1
    try:
        res = kubectl(ctx, ["get", "cronjob", args.cronjob, "-n", ns, "-o", "json"], capture=True)
        cronjob_obj = json.loads(res.stdout)
        command = build_manual_command(from_s, to_s, engine=engine_path is not None,
                                       service=args.service, replace=args.replace,
                                       generated_at=args.generated_at)
        kubectl(ctx, ["apply", "-n", ns, "-f", "-"],
                input_data=json.dumps(build_job_spec(cronjob_obj, job, command, cm_name)))
        rc = 0 if wait_job(ctx, ns, job, args.timeout_s) else 1
    finally:
        # 성공·실패·예외(Ctrl-C 포함) 어느 경로에서든 ConfigMap 정리 — Job 오브젝트는 남긴다 (로그 재조회용)
        if args.keep_configmap:
            print(f"[INFO] ConfigMap 보존(--keep-configmap) — 정리: kubectl --context={ctx} "
                  f"delete configmap {cm_name} -n {ns}", flush=True)
        else:
            delete_configmap(ctx, ns, cm_name)

    if rc == 0:
        # §6.3: manual 적재 후 동일 날짜 범위 mart-metrics rerun 은 의무 — 안내만 (창 검사는 mart 측 책임)
        mart_cmd = ["python3", MART_RERUN, "--context", ctx, "--namespace", ns,
                    "--from", from_s, "--to", to_s]
        print("[NEXT] manual 적재 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3): "
              + shlex.join(mart_cmd), flush=True)
    print(f"[INFO] Job 오브젝트는 남김(로그 재조회용) — 정리: kubectl --context={ctx} "
          f"delete job {job} -n {ns}", flush=True)
    return rc


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 6: 통과 확인** (전체 회귀 · 사용법 스모크 · zero-diff)

Run: `cd collectors/token-metrics && python3 -m pytest tests/test_manual_load.py -v`
Expected: `22 passed` (상수 1 · 이름/now_kst 3 · ConfigMap 본문 4 · Job 스펙/command 3 · main 성공/keep/실패 3 · WARN/예외 2 · 전파/cronjob 2 · 크기 1 · 사용법 1 · 파서 기본값 1 · 템플릿 왕복 1 = 22). 실패 0, skip 0.

Run: `cd collectors/token-metrics && python3 -m pytest -q`
Expected: T2~T9 테스트 + 위 22건 전부 `passed`, 실패 0 (T9 `test_rerun.py`와 이름이 겹치는 테스트 함수가 있어도 모듈이 다르므로 충돌 없음).

Run: `cd collectors/token-metrics && python3 tools/manual_load.py --help | head -3; python3 tools/manual_load.py --context c --from 2026-09-10 --to 2026-09-01 --gpu /nonexistent/gpu.csv --serving /nonexistent/serving.csv; echo "exit=$?"`
Expected: 첫 줄 `usage: manual_load.py [-h] --context CONTEXT …`, 이어서 stderr `--from(2026-09-10) > --to(2026-09-01)`, 마지막 줄 `exit=2` (날짜 검증이 파일 검증보다 먼저 — kubectl 호출 전, 클러스터 불필요).

Run: `cd collectors/token-metrics && python3 tools/manual_load.py --context c --from 2026-09-01 --to 2026-09-01 --gpu /nonexistent/gpu.csv --serving /nonexistent/serving.csv; echo "exit=$?"`
Expected: stderr `[ERROR] 파일 없음: /nonexistent/gpu.csv`, `exit=2`.

Run: `cd collectors/token-metrics && python3 -c "import ast,sys; ast.parse(open('tools/manual_load.py').read()); print('ok')" && grep -c "^import \|^from " tools/manual_load.py && grep -n "rerun" tools/manual_load.py | grep -i "import"; echo "imports-from-rerun=$?"`
Expected: `ok`, 표준 라이브러리 import 10줄(`__future__` 포함 — 서드파티 0: 워크스테이션에 pip 설치 불필요), `imports-from-rerun=1`(grep 무결과 = rerun.py에서 import하는 줄 0).

Run: `git status --porcelain -- collectors/token-metrics/tools collectors/token-metrics/tests/test_manual_load.py && git diff --stat -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml`
Expected: `?? collectors/token-metrics/tools/manual_load.py`·`?? collectors/token-metrics/tests/test_manual_load.py` 2행(또는 T9 파일이 이미 커밋돼 있으면 그 2행만), `git diff --stat` 출력 없음(zero-diff — 기존 rerun.py는 읽기만 했다).

- [ ] **Step 7: Commit**

```bash
git add collectors/token-metrics/tools/manual_load.py collectors/token-metrics/tests/test_manual_load.py
git commit -m "feat(collectors-metrics): tools/manual_load.py — CSV→ConfigMap→Job(/manual 볼륨)→로그→ConfigMap 삭제 (Plan 6b T10)" \
  -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>" \
  -m "Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

---

### Task 11: E2E(CH 24.8 + mock 18125/18001 — 2회 실행 already_loaded·mutations 0·--replace 3·시나리오 3종·manual 1회) · ci_expectations.py · verify SQL · test-collector-metrics.yml · release-images-metrics.yml · .gitignore +2행

**Files:**
- Create: `collectors/token-metrics/tests/e2e/run_e2e.sh`(실행 비트 `chmod +x`), `collectors/token-metrics/tests/e2e/ci_expectations.py`, `collectors/token-metrics/tests/e2e/verify_expected_results.sql`, `collectors/token-metrics/tests/e2e/ddl_test_dims.sql`, `collectors/token-metrics/tests/e2e/manual_e2e/e2e_manual_v0_gpu.csv`, `collectors/token-metrics/tests/e2e/manual_e2e/e2e_manual_v0_serving.csv`, `collectors/token-metrics/tests/e2e/manual_e2e/e2e_manual_v0_engine.csv`(고정 합성 데이터 — Plan 6a `.gitignore` 패턴 `*manual_metrics*.csv`에 걸리지 않는 이름이라 커밋 가능; 날짜는 `{DATE}` 플레이스홀더 — run_e2e.sh가 sed로 치환해 `tests/e2e/.tmp/`에 생성), `.github/workflows/test-collector-metrics.yml`, `.github/workflows/release-images-metrics.yml`
- Modify: `.gitignore`(+2줄 — 현재 12행 `collectors/token-usage/tests/e2e/endpoints.e2e.yaml` 바로 아래에 `collectors/token-metrics/tests/e2e/endpoints.e2e.yaml`, `collectors/token-metrics/tests/e2e/.tmp/`; 기존 12행은 그대로. Plan 6a G 블록이 이미 병합돼 그 아래에 더 있어도 삽입 위치는 동일)
- Test: `run_e2e.sh` 자체(도커 필요 — CI `e2e` job이 실행), `tests/e2e/ci_expectations.py`는 CI `unit` job에서 `python tests/e2e/ci_expectations.py 2026-09-10 e2e-seed-1 "claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5" | grep -q '^rows_gpu=5 rows_serving=9 gpu_hours_sum='` 스모크. 기존 `.github/workflows/{release-images,test-collector,test-mart}.yml`·`collectors/token-usage/**`는 zero-diff(읽기만 — 클론 원본).

**설계 근거:** §7.3(350-354행) "e2e(CH 24.8 + mock, 자기 DDL 2파일 + `gpu_data.dim_token_service` 최소 twin, 수집기 2회(2회차 already_loaded·DELETE 0), 기대치 `datagen.build_metrics`, 시나리오 grep, manual CSV 적재 1회) / image / manifests(contract-lock: `schedule "5 2-9"`, `timeZone`, `startingDeadlineSeconds: 540`, `activeDeadlineSeconds: 3000`, `backoffLimit: 0`, `-verify` 이름)" · §5.6(288행) `test-collector-metrics.yml`(paths `collectors/token-metrics/**`, `tools/mock-provider/**`)·`release-images-metrics.yml` 신규 — "기존 `release-images.yml`은 무수정, 기존 이미지 재빌드 유발 금지"(matrix는 6b에서 `collectors/token-metrics` 1항목, `mart/token-metrics`는 Plan 6c가 additive) · §4.0(119-128행) 뮤테이션 장부 — 정기 8슬롯 실행은 0, `--replace`는 날짜당 fact ≤3(gpu·serving·summary), 감사 append-only → `system.mutations`로 실측 · §5.2 표(앵커 존재 & `--replace` 없음 → `SKIPPED reason=already_loaded`; rerun 409 재차 → `FAILURE reason=not_ready`) · §5.5(manual-v0 — 동일 normalize+replace 경로, `--generated-at`, 기존 앵커면 `already_loaded`) · §7.2(공개 레포 — 사내 주소 0, 실데이터 gitignore: `endpoints.e2e.yaml`·`.tmp/`) · digest D9.1~D9.3(기존 e2e 3파일 관용구 — docker network·`CLICKHOUSE_SKIP_USER_SETUP=1`·헬스 60회·DDL 단일노드 변환 정규식 3종·`urllib` 문장별 실행·`--expect-empty` SQL)·D11.1/D11.2(워크플로 클론 원본).

**Interfaces:**
- Consumes:
  - T1 mock: `GET /v1/metrics?date=YYYY-MM-DD`(`build_metrics(cfg, date, scn)` — 기본 3모델 gpu 5행·serving 3레코드, `engine {"type":"vllm","version":"0.10.1"}`, `generatedAt = "<date+1>T02:05:00+09:00"`), `Config.metrics_retention_days`(env `MOCK_METRICS_RETENTION_DAYS`), `POST /__mock/scenario`(키별 setattr — int 플래그 `metrics_gpu_hours_over`·`metrics_empty_gpu` 등 6종, `not_ready_until_uptime_s: float ≥ 0`, `retry_after_s: int ≥ 1`), `_date_gate`(uptime < `not_ready_until_uptime_s` → 409 `data_not_ready` + `Retry-After`), `tools/mock-provider/Dockerfile`(python:3.12-slim, uvicorn `app.main:app` 8000).
  - T2 `load_config()` env: `CH_HOST CH_PORT CH_USER CH_PASSWORD CH_CLUSTER ENDPOINTS_FILE`; `load_endpoints()` 키 `serviceGroup service baseUrl enabled apiSince coverageSince`(`apiSince` 기본 `2026-09-09` — e2e는 과거로 명시); `requirements-dev.txt`.
  - T3 normalize: gpu `model="unknown"`은 `category="test"`에서 허용(5행 전부 통과), serving 레코드당 `ttft_ms`·`itl_ms`·`output_tps` 3행, 시나리오 OFF면 `flags` 빈 배열, `hours_over_count` WARN 코드.
  - T5 writer: 테이블 `fact.raw_token_metrics_{gpu,serving,summary}_1d_{local,dist}`, `fact.collect_audit_metrics_1d_{local,dist}`, `gpu_data.dim_token_metrics_service_{local,dist}`; summary 컬럼 `gpu_rows`·`serving_rows`·`engine_type`·`engine_version`·`source_type`; `--replace` = summary→gpu→serving `_local` ALTER DELETE 3건(`mutations_sync=2`) + 앵커 존재 시 감사 INSERT 1행; 정기 = 앵커 존재 시 DELETE 0; 레지스트리 diff-sync(현재 집합이 비면 DELETE 생략 → 최초 실행 뮤테이션 0, 동일 집합이면 0).
  - T6 CLI: `python3 -m app.main <batch_time_iso>`(정기 — target_date = batch_time − 1일, `slot=HH`, `final = hour ≥ 9`), `--from D0 --to D1 [--service S] [--replace]`(rerun); 마커 `SERVICE_RESULT status=<S> module=token-metrics service=<svc> source_type=<t> rows=<n> pages=1 warn=<n> rejected=<n>[ reason=<r>]`, `BATCH_RESULT status=<S> module=token-metrics services_ok=<n> services_failed=<n> services_skipped=<n> rows=<n> elapsed=<n>s slot=<HH> final=<0|1>[ reason=<r>]`, `CHECK WARN service=<svc> <code>=<count>`; 종료 코드 `1 if any FAILURE else 0`; rerun 409 → `min(max(Retry-After,1),300)`s 뒤 1회 재방문 → 재차 409 = `FAILURE reason=not_ready`.
  - T7 manual CLI: `--manual-gpu <p> --manual-serving <p> [--manual-engine <p>] --from D0 --to D1 [--service S] [--replace] [--generated-at ISO]`; 헤더 상수 `GPU_HEADER = "date,service,model,gpuType,category,gpuCount,gpuHours"`, `SERVING_HEADER = "date,service,model,metric,name,unit,p50,p90,p95,p99"`, `ENGINE_HEADER = "service,engine_type,engine_version"`; `#` 시작 줄은 주석, 첫 비주석 줄 = 헤더(바이트 동일); 마커 `MANUAL_INPUT module=token-metrics rows_gpu=<n> rows_serving=<n> rows_engine=<n> rows_outside_range=<n> rows_other_service=<n>`; `source_type=manual-v0`.
  - T8 매니페스트: `collectors/token-metrics/k8s/overlays/{stage,company,company-verify}`(kustomize), `Dockerfile`(`CMD ["python", "-m", "app.main"]`), 계약 문자열(`schedule: 5 2-9 * * *`, `timeZone: Asia/Seoul`, `startingDeadlineSeconds: 540`, `activeDeadlineSeconds: 3000`, `backoffLimit: 0`, `concurrencyPolicy: Forbid`, `memory: 1Gi`/`256Mi`, `registry-pull-secret`, `token-metrics-ch-secret[-verify]`, `token-metrics-endpoints[-verify]`, `token-metrics-ca-bundle`, `METRICS_MAX_MUTATIONS_PER_RUN`, stage 이미지 `ghcr.io/yoonsungnam/token-metrics-collector`, company-verify CronJob `token-metrics-collector-verify`).
  - Plan 6a DDL: `collectors/token-metrics/ddl/company/raw_token_metrics.sql`(fact 4테이블 `_local`/`_dist`, `ON CLUSTER 'gpu-monitoring'`, `ReplicatedMergeTree(...)`, `Distributed('gpu-monitoring', 'fact', '<t>_local', cityHash64(service))`), `collectors/token-metrics/ddl/company/dim_token_metrics_service.sql`(`gpu_data.dim_token_metrics_service_{local,dist}` 12컬럼, `ORDER BY (service)`, `Distributed('gpu-monitoring', 'gpu_data', 'dim_token_metrics_service_local', rand())`). 머리말 주석에 `;`가 포함된 줄이 있다(`-- 뮤테이션 … (앵커 존재→스킵, 미존재→INSERT만);`) — 변환 블록이 주석 줄을 먼저 제거한다.
  - 기존 레포(읽기만): `collectors/token-usage/tests/e2e/{run_e2e.sh,ci_expectations.py,verify_expected_results.sql}`, `.github/workflows/{test-collector,release-images}.yml`, `collectors/token-usage/ddl/company/dim_token_service.sql`(twin 컬럼 7종의 정본).
- Produces(T12 README·Plan 6c CI가 소비):
  - 포트·이름: CH `18125:8123`, mock `18001:8000`(기존 e2e 18123/18000/18124와 충돌 없음); 컨테이너 `ch-e2e-metrics`, `mock-e2e-metrics`; 네트워크 `tokenmetricse2e`; 이미지 `clickhouse/clickhouse-server:24.8`, `token-mock-provider:e2e`(CI가 `tools/mock-provider`에서 빌드). 스크립트 상수 `CH_URL="http://127.0.0.1:18125/"`, `MOCK_URL="http://127.0.0.1:18001"`, `SVC="Mock Service A"`.
  - `SEED="e2e-seed-1"`; mock env `MOCK_SERVICE_GROUP="Mock Group" MOCK_SERVICE="Mock Service A" MOCK_SEED="${SEED}" MOCK_METRICS_RETENTION_DAYS=14`; `DATE_ARG="${1:-$(date -d yesterday +%F)}"`(러너 로컬 TZ — 어제는 어느 TZ에서도 mock의 "과거" 조건 충족), `NEXT_DAY=$(date -d "${DATE_ARG} +1 day" +%F)`, `DATE2=$(date -d "${DATE_ARG} -1 day" +%F)`(시나리오 C·manual용 — 보존 14일 안); 정기 호출 `python3 -m app.main "${NEXT_DAY}T02:05:00+09:00"`(target_date = DATE_ARG, `slot=02 final=0`).
  - 수집기 env(export): `CH_HOST=127.0.0.1 CH_PORT=18125 CH_USER=default CH_PASSWORD= CH_CLUSTER="" ENDPOINTS_FILE=tests/e2e/endpoints.e2e.yaml`; 생성 파일 `tests/e2e/endpoints.e2e.yaml`(gitignore): `services: [{serviceGroup: "Mock Group", service: "Mock Service A", baseUrl: "http://127.0.0.1:18001", enabled: true, apiSince: "2026-01-01", coverageSince: "2026-01-01"}]`.
  - bash 헬퍼(run_e2e.sh 내부): `chq <sql>`(CH HTTP 스칼라 질의 — stdout), `expect_eq <what> <got> <want>`, `run_collector <args...>`(`OUT`/`RC` 설정 — `set +e` 구간), `need_line <what> <fixed-string>`(`$OUT`에 고정 문자열 부재 → `<what> missing` + 전체 출력 덤프 + exit 1), `scenario <json>`(`POST /__mock/scenario`).
  - `tests/e2e/ddl_test_dims.sql`(변환 없이 그대로 적용): `CREATE DATABASE IF NOT EXISTS gpu_data;` + `gpu_data.dim_token_service_local`(7컬럼 twin: `service_group LowCardinality(String), service LowCardinality(String), base_url String, enabled UInt8, source_type LowCardinality(String), note String DEFAULT '', updated_at DateTime('Asia/Seoul')`, `ENGINE = MergeTree ORDER BY (service)`) + `gpu_data.dim_token_service_dist`(`Distributed('default', 'gpu_data', 'dim_token_service_local', rand())`) + `INSERT INTO gpu_data.dim_token_service_dist VALUES ('Mock Group', 'Mock Service A', 'http://127.0.0.1:18001', 1, 'usage-api-v1', '', now());` — 프리플라이트 `SELECT count() FROM gpu_data.dim_token_service_dist` == `1`.
  - `tests/e2e/ci_expectations.py <date> <seed> <models_csv>` → 1줄 `rows_gpu=<n> rows_serving=<n> gpu_hours_sum=<x>`(`rows_gpu = len(p["gpu"])`, `rows_serving = 3 * len(p["serving"])`, `gpu_hours_sum = round(sum(r["gpuHours"] for r in p["gpu"]), 1)`; `p = build_metrics(MockConfig(service_group="Mock Group", service="Mock Service A", seed=seed, models=[...]), date)`); 기본 3모델 = `rows_gpu=5 rows_serving=9`.
  - `tests/e2e/verify_expected_results.sql` 플레이스홀더 `{DATE} {SERVICE} {EXP_GPU_ROWS} {EXP_SERVING_ROWS} {EXP_GPU_HOURS}`; 검사 11종(`check_name`, 0행 = 통과): `gpu_row_count`, `serving_row_count`, `gpu_hours_sum`, `summary_anchor_once`, `summary_source_type`, `summary_engine`, `summary_counts`, `audit_empty`, `registry_synced`, `collected_at_kst_sane`, `no_flags_on_clean_run`.
  - `tests/e2e/manual_e2e/*.csv`(헤더 = T7 상수와 바이트 동일): gpu 2행 `{DATE},Mock Service A,claude-opus-4-8,H100,serving,4,96` / `{DATE},Mock Service A,claude-opus-4-8,H100,standby,1,24`; serving 3행(`claude-opus-4-8`: `ttftMs,,,120,300,400,800` / `itlMs,,,20,40,50,90` / `outputTps,,,41,,,` — 표준 지표 행은 6a 템플릿 규칙대로 name/unit 빈칸); engine 1행 `Mock Service A,vllm,0.8.4`. run_e2e.sh가 `{DATE}`→`DATE2`로 치환해 `tests/e2e/.tmp/e2e_manual_v0_{gpu,serving,engine}.csv` 생성 → manual 기대 `MANUAL_INPUT module=token-metrics rows_gpu=2 rows_serving=3 rows_engine=1 rows_outside_range=0 rows_other_service=0`, `SERVICE_RESULT status=SUCCESS … source_type=manual-v0 rows=5 pages=1`.
  - run_e2e.sh 시퀀스(마커·수치 기대): (1) 컨테이너·헬스 → (2) DDL(`CREATE DATABASE IF NOT EXISTS fact;` + 6a DDL 2파일 변환 + `ddl_test_dims.sql`) + 프리플라이트 1 → (3) 정기 1회: `SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A source_type=metrics-api-v1 rows=14 pages=1 warn=0 rejected=0`, `BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 services_failed=0 services_skipped=0 rows=14 `, `slot=02 final=0`, RC 0 → (4) 정기 2회: `… source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=already_loaded`, `BATCH_RESULT status=SUCCESS … services_skipped=1`, `system.mutations WHERE database IN ('fact','gpu_data')` == 0 → (5) verify SQL 0행 → (6) `--from $DATE_ARG --to $DATE_ARG --replace`: `rows=14`, fact mutations == 3, 감사 == 1, 앵커 == 1 → (7) 시나리오 A `{"metrics_gpu_hours_over": 1}` + `--replace`: `CHECK WARN service=Mock Service A hours_over_count=1`, `warn=1`, SUCCESS; B `{"metrics_gpu_hours_over": 0, "metrics_empty_gpu": 1}` + `--replace`: `rows=9 `, summary `gpu_rows` == 0, gpu 행 0, fact mutations == 9; C `{"metrics_empty_gpu": 0, "not_ready_until_uptime_s": 100000, "retry_after_s": 1}` + `--from $DATE2 --to $DATE2`: `SERVICE_RESULT status=FAILURE … reason=not_ready`, `BATCH_RESULT status=FAILURE`, RC 1; reset `{"not_ready_until_uptime_s": 0}` → (8) manual(DATE2, `--generated-at "${NEXT_DAY}T09:00:00+09:00"`): MANUAL_INPUT 줄 + `source_type=manual-v0 rows=5 pages=1` + SQL manual-v0 앵커 1행·`engine_version` `0.8.4`·gpu 2·serving 3 + 재호출 `--from $DATE2 --to $DATE2` → `reason=already_loaded`, fact mutations 여전히 9 → (9) `E2E PASS (date=${DATE_ARG}, gpu=${EXP_GPU_ROWS}, serving=${EXP_SERVING_ROWS})`.
  - `.github/workflows/test-collector-metrics.yml`: `name: test-collector-metrics`; `on.push.branches [main]`·`on.pull_request` 모두 `paths: ["collectors/token-metrics/**", "tools/mock-provider/**", ".github/workflows/test-collector-metrics.yml"]`; jobs `unit`(working-directory `collectors/token-metrics`, python 3.12, pytest `--ignore=tests/e2e`, ci_expectations 스모크), `e2e`(mock 이미지 빌드 + `./collectors/token-metrics/tests/e2e/run_e2e.sh`), `image`(`docker build -t token-metrics-collector:ci .` + `--help`), `manifests`(3 overlay 렌더 → 계약 grep 13종 + `-verify` 3종 + stage 이미지 + `token-usage` 0건).
  - `.github/workflows/release-images-metrics.yml`: `name: release-images-metrics`; `on.push.branches [main]` `paths: ["collectors/token-metrics/**", ".github/workflows/release-images-metrics.yml"]` + `workflow_dispatch`; `permissions: {packages: write, contents: read}`; matrix include 1항목 `{context: collectors/token-metrics, image: token-metrics-collector}`; 태그 `ghcr.io/yoonsungnam/${{ matrix.image }}:latest` + `:${{ steps.sha7.outputs.sha7 }}`.
  - `.gitignore` +2행: `collectors/token-metrics/tests/e2e/endpoints.e2e.yaml`, `collectors/token-metrics/tests/e2e/.tmp/`.

- [ ] **Step 1: 전제 확인 — T1~T10 산출물·Plan 6a DDL·클론 원본 3종이 있는지(없으면 중단·보고)**

Run: `ls collectors/token-metrics/app/main.py collectors/token-metrics/app/manual.py collectors/token-metrics/app/writer.py collectors/token-metrics/requirements-dev.txt collectors/token-metrics/Dockerfile collectors/token-metrics/k8s/overlays/stage/kustomization.yaml collectors/token-metrics/k8s/overlays/company/kustomization.yaml collectors/token-metrics/k8s/overlays/company-verify/kustomization.yaml collectors/token-metrics/ddl/company/raw_token_metrics.sql collectors/token-metrics/ddl/company/dim_token_metrics_service.sql tools/mock-provider/Dockerfile collectors/token-usage/tests/e2e/run_e2e.sh .github/workflows/test-collector.yml .github/workflows/release-images.yml && grep -n "^def build_metrics" tools/mock-provider/app/datagen.py && grep -n "metrics_retention_days" tools/mock-provider/app/config.py | head -1 && grep -n "MANUAL_INPUT_PREFIX = \|^GPU_HEADER = \|^SERVING_HEADER = \|^ENGINE_HEADER = " collectors/token-metrics/app/manual.py && grep -c "ON CLUSTER 'gpu-monitoring'" collectors/token-metrics/ddl/company/raw_token_metrics.sql collectors/token-metrics/ddl/company/dim_token_metrics_service.sql && ls collectors/token-metrics/tests/e2e 2>&1 | head -1`
Expected: 파일 경로 14개, `tools/mock-provider/app/datagen.py:<n>:def build_metrics(cfg: Config, date: str, scn: ScenarioState | None = None) -> dict:`, `config.py`의 `metrics_retention_days` 1줄, `manual.py` 4줄(`MANUAL_INPUT_PREFIX = "MANUAL_INPUT module=token-metrics"`·헤더 상수 3개), `raw_token_metrics.sql:8`·`dim_token_metrics_service.sql:2`(ON CLUSTER 문 수 — 정확한 수는 6a 파일 기준이며 0이 아니면 된다), 마지막 줄 `ls: cannot access 'collectors/token-metrics/tests/e2e': No such file or directory`(이 태스크가 처음 만든다). 하나라도 없으면 T1~T10 또는 Plan 6a가 병합되지 않은 것 — 중단하고 보고한다(대신 만들지 않는다).

Run: `cd /home/mini/github/token-data-pipeline && sed -n 12p .gitignore && grep -c "token-metrics" .gitignore .github/workflows/test-collector.yml .github/workflows/release-images.yml`
Expected: `collectors/token-usage/tests/e2e/endpoints.e2e.yaml`(삽입 기준 행) 다음에 `.gitignore:0`·`test-collector.yml:0`·`release-images.yml:0`(6b e2e 항목·워크플로는 아직 없음; `.gitignore`에 Plan 6a G 블록이 있어도 `token-metrics` 경로 행은 0이어야 한다 — 0이 아니면 그 줄을 보고 중복 삽입을 피한다).

- [ ] **Step 2: `tests/e2e/ddl_test_dims.sql` — 토큰 레지스트리 최소 twin(단일노드용 — 변환 없이 적용) + 프리플라이트 대상**

`collectors/token-metrics/tests/e2e/ddl_test_dims.sql` 신규(전체 내용):

```sql
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
```

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && mkdir -p tests/e2e/manual_e2e && ls tests/e2e/ddl_test_dims.sql && grep -c "^CREATE \|^INSERT " tests/e2e/ddl_test_dims.sql && grep -v "^--" tests/e2e/ddl_test_dims.sql | grep -c ";" && grep -o "service_group\|service \|base_url\|enabled\|source_type\|note\|updated_at" tests/e2e/ddl_test_dims.sql | sort -u | wc -l && grep -c "ON CLUSTER\|Replicated" tests/e2e/ddl_test_dims.sql`
Expected: 경로 1줄, `4`(CREATE 3 + INSERT 1), `4`(문장 종결 `;` 4 — 주석 줄 제외; run_e2e.sh 의 DDL 루프는 주석 줄을 지운 뒤 따옴표 밖 `;` 로 분할하므로 이 파일의 문자열 리터럴에는 `;` 를 두지 않는다), `7`(twin 컬럼 7종), `0`(단일노드 전용 — 클러스터 문법 없음).

- [ ] **Step 3: `tests/e2e/manual_e2e/*.csv` 3파일 — 고정 합성 manual-v0 입력(`{DATE}` 플레이스홀더, 헤더 = T7 상수)**

`collectors/token-metrics/tests/e2e/manual_e2e/e2e_manual_v0_gpu.csv` 신규(전체 내용):

```csv
# E2E manual-v0 gpu — {DATE} 는 run_e2e.sh 가 DATE2 로 치환해 tests/e2e/.tmp/ 에 생성 (§5.5, 헤더 = app/manual.py GPU_HEADER)
date,service,model,gpuType,category,gpuCount,gpuHours
{DATE},Mock Service A,claude-opus-4-8,H100,serving,4,96
{DATE},Mock Service A,claude-opus-4-8,H100,standby,1,24
```

`collectors/token-metrics/tests/e2e/manual_e2e/e2e_manual_v0_serving.csv` 신규(전체 내용):

```csv
# E2E manual-v0 serving — metric 은 API 키(ttftMs|itlMs|outputTps); 표준 지표 행은 name/unit 빈칸(ms·tokens/s 자동), outputTps 는 p50 만 (헤더 = SERVING_HEADER)
date,service,model,metric,name,unit,p50,p90,p95,p99
{DATE},Mock Service A,claude-opus-4-8,ttftMs,,,120,300,400,800
{DATE},Mock Service A,claude-opus-4-8,itlMs,,,20,40,50,90
{DATE},Mock Service A,claude-opus-4-8,outputTps,,,41,,,
```

`collectors/token-metrics/tests/e2e/manual_e2e/e2e_manual_v0_engine.csv` 신규(전체 내용):

```csv
# E2E manual-v0 engine — 서비스당 1행 (헤더 = ENGINE_HEADER); 0.8.4 는 mock 의 0.10.1 과 구별되는 값
service,engine_type,engine_version
Mock Service A,vllm,0.8.4
```

Run:
```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 - <<'PY'
import app.manual as m
want = {"gpu": m.GPU_HEADER, "serving": m.SERVING_HEADER, "engine": m.ENGINE_HEADER}
for k, header in want.items():
    lines = open(f"tests/e2e/manual_e2e/e2e_manual_v0_{k}.csv", encoding="utf-8").read().split("\n")
    body = [l for l in lines if l and not l.startswith("#")]
    assert body[0] == header, (k, body[0], header)
    print(k, "header_ok rows=%d date_rows=%d" % (len(body) - 1, sum(l.startswith("{DATE}") for l in body[1:])))
PY
```
Expected: `gpu header_ok rows=2 date_rows=2`, `serving header_ok rows=3 date_rows=3`, `engine header_ok rows=1 date_rows=0`(헤더가 T7 상수와 바이트 동일 — 다르면 `AssertionError` 튜플로 어느 파일인지 보인다).

- [ ] **Step 4: `tests/e2e/ci_expectations.py` — mock `build_metrics` 결정성으로 기대 행수·gpu_hours 합 산출**

`collectors/token-metrics/tests/e2e/ci_expectations.py` 신규(전체 내용):

```python
"""mock-provider 의 결정성으로 CI 기대값을 산출한다 (Plan 6b T11 — §7.3 "기대치 datagen.build_metrics").

mock 저장소가 이 레포 안에 있으므로 tools/mock-provider/app 을 직접 import 해
같은 (seed, date) 의 /v1/metrics 페이로드를 재현하고, ClickHouse 적재 결과와 비교할 상수를 출력한다.
사용: python ci_expectations.py <date> <seed> <models(콤마)>
출력: "rows_gpu=<n> rows_serving=<n> gpu_hours_sum=<x>"
  rows_gpu     = gpu 행 수 (T3: model="unknown" 은 category="test" 에서 허용 → 기본 3모델 5행 전부 적재)
  rows_serving = serving 레코드 수 × 3 (레코드당 ttft_ms · itl_ms · output_tps long-form 1행씩)
  gpu_hours_sum = gpuHours 합 (소수 1자리 — verify SQL 은 abs 차 0.05 허용)
"""
from __future__ import annotations

import sys
from pathlib import Path

# parents[0]=e2e parents[1]=tests parents[2]=token-metrics parents[3]=collectors
# parents[4]=repo root — tools/mock-provider 는 repo root 기준
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "tools" / "mock-provider"))

from app.config import Config as MockConfig   # noqa: E402  (mock 의 app — 수집기 app 과 무관)
from app.datagen import build_metrics         # noqa: E402

SERVING_ROWS_PER_RECORD = 3   # ttft_ms · itl_ms · output_tps


def expectations(date: str, seed: str, models: list[str]) -> tuple[int, int, float]:
    cfg = MockConfig(service_group="Mock Group", service="Mock Service A", seed=seed, models=models)
    p = build_metrics(cfg, date)
    rows_gpu = len(p["gpu"])
    rows_serving = SERVING_ROWS_PER_RECORD * len(p["serving"])
    gpu_hours_sum = round(sum(r["gpuHours"] for r in p["gpu"]), 1)
    return rows_gpu, rows_serving, gpu_hours_sum


def main() -> None:
    if len(sys.argv) != 4:
        print("usage: ci_expectations.py <date> <seed> <models,comma,separated>", file=sys.stderr)
        raise SystemExit(2)
    date, seed, models_csv = sys.argv[1:4]
    models = [m for m in models_csv.split(",") if m]
    rows_gpu, rows_serving, gpu_hours_sum = expectations(date, seed, models)
    print(f"rows_gpu={rows_gpu} rows_serving={rows_serving} gpu_hours_sum={gpu_hours_sum}")


if __name__ == "__main__":
    main()
```

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && python3 tests/e2e/ci_expectations.py 2026-09-10 e2e-seed-1 "claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5" && python3 tests/e2e/ci_expectations.py 2026-09-10 e2e-seed-1 "claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5" | grep -q '^rows_gpu=5 rows_serving=9 gpu_hours_sum=' && echo SMOKE_OK && python3 tests/e2e/ci_expectations.py 2026-09-10 e2e-seed-1 "claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5" > /tmp/claude-1000/-home-mini-github-token-data-pipeline/8a0025e9-e64c-4caf-972c-788ea90abe0e/scratchpad/exp1.txt && python3 tests/e2e/ci_expectations.py 2026-09-10 e2e-seed-1 "claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5" | cmp - /tmp/claude-1000/-home-mini-github-token-data-pipeline/8a0025e9-e64c-4caf-972c-788ea90abe0e/scratchpad/exp1.txt && echo DETERMINISTIC; python3 tests/e2e/ci_expectations.py 2026-09-10; echo "exit=$?"`
Expected: `rows_gpu=5 rows_serving=9 gpu_hours_sum=331.0`(seed `e2e-seed-1`·2026-09-10 기준 — 다른 seed/날짜면 값이 달라진다; 3모델 serving 3행 + standby 1행 + unknown/test 1행 = 5; 3×3 = 9), `SMOKE_OK`, `DETERMINISTIC`(같은 인자 → 바이트 동일 출력), 인자 부족 시 stderr `usage: …` + `exit=2`.

- [ ] **Step 5: `tests/e2e/verify_expected_results.sql` — `--expect-empty` 방식 검사 11종(정기 2회 실행 직후 상태)**

`collectors/token-metrics/tests/e2e/verify_expected_results.sql` 신규(전체 내용):

```sql
-- --expect-empty 방식: 기대와 다른 행만 SELECT — 출력 없으면 통과 (기존 token-usage e2e 와 같은 형식)
-- 실행 전 치환: {DATE} {SERVICE} {EXP_GPU_ROWS} {EXP_SERVING_ROWS} {EXP_GPU_HOURS}
-- 실행 시점: 정기 2회(2회차 already_loaded) 직후 — 감사 0행·flags 빈 배열·앵커 1행이 전제 (§4.0 정기 = 뮤테이션 0)

SELECT 'gpu_row_count' AS check_name, count() AS actual, {EXP_GPU_ROWS} AS expected
FROM fact.raw_token_metrics_gpu_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != {EXP_GPU_ROWS}

UNION ALL

SELECT 'serving_row_count', count(), {EXP_SERVING_ROWS}
FROM fact.raw_token_metrics_serving_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != {EXP_SERVING_ROWS}

UNION ALL

SELECT 'gpu_hours_sum', sum(gpu_hours), {EXP_GPU_HOURS}
FROM fact.raw_token_metrics_gpu_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING abs(sum(gpu_hours) - {EXP_GPU_HOURS}) > 0.05

UNION ALL

SELECT 'summary_anchor_once', count(), 1
FROM fact.raw_token_metrics_summary_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != 1

UNION ALL

SELECT 'summary_source_type', count(), 0
FROM fact.raw_token_metrics_summary_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}' AND source_type != 'metrics-api-v1'
HAVING count() != 0

UNION ALL

SELECT 'summary_engine', count(), 0
FROM fact.raw_token_metrics_summary_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
  AND (engine_type != 'vllm' OR engine_version != '0.10.1')
HAVING count() != 0

UNION ALL

SELECT 'summary_counts', count(), 0
FROM fact.raw_token_metrics_summary_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
  AND (gpu_rows != {EXP_GPU_ROWS} OR serving_rows != {EXP_SERVING_ROWS})
HAVING count() != 0

UNION ALL

-- 2회 실행은 already_loaded 로 DELETE·감사 INSERT 가 없다 (§5.2 표 · §5.4 (2))
SELECT 'audit_empty', count(), 0
FROM fact.collect_audit_metrics_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != 0

UNION ALL

-- 정기 실행의 레지스트리 diff-sync 결과 (§4.3) — endpoints 1건 = 행 1건
SELECT 'registry_synced', count(), 1
FROM gpu_data.dim_token_metrics_service_dist
WHERE service = '{SERVICE}'
HAVING count() != 1

UNION ALL

SELECT 'collected_at_kst_sane', count(), 0
FROM fact.raw_token_metrics_gpu_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
  AND (toHour(collected_at) NOT BETWEEN 0 AND 23
       OR collected_at < now('Asia/Seoul') - INTERVAL 2 HOUR
       OR collected_at > now('Asia/Seoul') + INTERVAL 10 MINUTE)
HAVING count() != 0

UNION ALL

-- 시나리오 OFF 기본 데이터 — gpu·serving 모두 flags 빈 배열 (T3)
SELECT 'no_flags_on_clean_run', count(), 0
FROM (
    SELECT flags FROM fact.raw_token_metrics_gpu_1d_dist
    WHERE date = '{DATE}' AND service = '{SERVICE}'
    UNION ALL
    SELECT flags FROM fact.raw_token_metrics_serving_1d_dist
    WHERE date = '{DATE}' AND service = '{SERVICE}'
)
WHERE length(flags) != 0
HAVING count() != 0
```

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && grep -c "^SELECT '" tests/e2e/verify_expected_results.sql && grep -c "^UNION ALL" tests/e2e/verify_expected_results.sql && grep -o "{[A-Z_]*}" tests/e2e/verify_expected_results.sql | sort -u | tr '\n' ' '; echo; grep -o "^SELECT '[a-z_]*'" tests/e2e/verify_expected_results.sql | tr '\n' ' '; echo; sed -e "s/{DATE}/2026-09-10/g" -e "s/{SERVICE}/Mock Service A/g" -e "s/{EXP_GPU_ROWS}/5/g" -e "s/{EXP_SERVING_ROWS}/9/g" -e "s/{EXP_GPU_HOURS}/331.0/g" tests/e2e/verify_expected_results.sql | grep -c "{"`
Expected: `11`(최상위 SELECT 11), `10`(UNION ALL 10), `{DATE} {EXP_GPU_HOURS} {EXP_GPU_ROWS} {EXP_SERVING_ROWS} {SERVICE}`, 검사명 11개 `SELECT 'gpu_row_count' SELECT 'serving_row_count' SELECT 'gpu_hours_sum' SELECT 'summary_anchor_once' SELECT 'summary_source_type' SELECT 'summary_engine' SELECT 'summary_counts' SELECT 'audit_empty' SELECT 'registry_synced' SELECT 'collected_at_kst_sane' SELECT 'no_flags_on_clean_run'`, 마지막 `0`(치환 후 남는 `{` 없음 — 실제 실행은 Step 6 run_e2e.sh 의 (5)).

- [ ] **Step 6: `tests/e2e/run_e2e.sh` — 컨테이너·DDL 변환·정기 2회·verify·`--replace`·시나리오 A/B/C·manual (실행 비트)**

`collectors/token-metrics/tests/e2e/run_e2e.sh` 신규(전체 내용 — 기존 `collectors/token-usage/tests/e2e/run_e2e.sh` 관용구 복제 + 뮤테이션 장부·시나리오·manual 구간 추가):

```bash
#!/usr/bin/env bash
# CI/로컬(도커 필요) E2E — collectors/token-metrics (Plan 6b T11 · 설계 §7.3, §4.0 뮤테이션 장부 실측)
# CH 24.8 + mock-provider 컨테이너 → 6a DDL 2파일 단일노드 변환 + 토큰 레지스트리 최소 twin
# → 정기 2회(2회차 already_loaded · system.mutations 0) → verify --expect-empty
# → --replace 재수집(mutations 3 · 감사 1) → 시나리오 A(hours_over WARN)/B(gpu 빈 배열)/C(409 not_ready)
# → manual-v0 1회(MANUAL_INPUT · manual-v0 앵커) + 재호출 already_loaded
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."     # collectors/token-metrics

DATE_ARG="${1:-$(date -d "yesterday" +%F)}"    # 러너 로컬 TZ 의 어제 — 어느 TZ 에서도 mock 의 "과거" 조건 충족
NEXT_DAY=$(date -d "${DATE_ARG} +1 day" +%F)   # 정기 batch_time 의 날짜 (target_date = batch_time − 1일 = DATE_ARG)
DATE2=$(date -d "${DATE_ARG} -1 day" +%F)      # 시나리오 C · manual 용 두 번째 날짜 (MOCK_METRICS_RETENTION_DAYS=14 안)
SEED="e2e-seed-1"
SVC="Mock Service A"
CH_URL="http://127.0.0.1:18125/"
MOCK_URL="http://127.0.0.1:18001"
TMP=tests/e2e/.tmp                              # gitignore — sed 치환 CSV · verify 출력

chq() {            # chq <sql> — CH HTTP 스칼라 질의 (TSV 1값, 개행은 $(...) 가 제거)
  curl -sf "${CH_URL}" --data-binary "$1"
}
expect_eq() {      # expect_eq <what> <got> <want>
  if [ "$2" != "$3" ]; then echo "$1: expected $3, got $2"; exit 1; fi
}
run_collector() {  # run_collector <args...> — OUT / RC 설정 (set -e 아래에서 비정상 종료도 값으로 받는다)
  set +e
  OUT=$(python3 -m app.main "$@" 2>&1)
  RC=$?
  set -e
}
need_line() {      # need_line <what> <fixed-string> — $OUT 에 문자열이 없으면 실패 + 전체 출력 덤프
  grep -qF -- "$2" <<<"$OUT" || { echo "$1 missing"; echo "$OUT"; exit 1; }
}
scenario() {       # scenario '<json>' — mock 시나리오 키별 병합 (T1 set_scenario; 나머지 키는 유지)
  curl -sf -X POST "${MOCK_URL}/__mock/scenario" -H 'content-type: application/json' -d "$1" >/dev/null
}

# (1) 컨테이너 — 기존 token-usage e2e 와 포트·컨테이너·네트워크 이름이 다르다 (병렬 실행 충돌 없음)
docker network create tokenmetricse2e 2>/dev/null || true
trap 'docker rm -f ch-e2e-metrics mock-e2e-metrics >/dev/null 2>&1 || true; docker network rm tokenmetricse2e >/dev/null 2>&1 || true' EXIT

# CLICKHOUSE_SKIP_USER_SETUP=1: 공식 이미지는 비밀번호 미설정 시 default 유저의
# 네트워크 접근을 차단(localhost 전용) — published port 경유(브리지 IP) 쿼리가 403이 됨
docker run -d --rm --name ch-e2e-metrics --network tokenmetricse2e -p 18125:8123 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  clickhouse/clickhouse-server:24.8
docker run -d --rm --name mock-e2e-metrics --network tokenmetricse2e -p 18001:8000 \
  -e MOCK_SERVICE_GROUP="Mock Group" -e MOCK_SERVICE="${SVC}" \
  -e MOCK_SEED="${SEED}" -e MOCK_METRICS_RETENTION_DAYS=14 \
  token-mock-provider:e2e

for _ in $(seq 1 60); do
  curl -sf http://127.0.0.1:18125/ping >/dev/null && \
  curl -sf "${MOCK_URL}/healthz" >/dev/null && break
  sleep 1
done
curl -sf http://127.0.0.1:18125/ping >/dev/null || { echo "clickhouse not healthy after 60s"; exit 1; }
curl -sf "${MOCK_URL}/healthz" >/dev/null || { echo "mock-provider not healthy after 60s"; exit 1; }

# (2) DDL: 6a 초안(company)을 단일노드용으로 변환 — ON CLUSTER 제거, Replicated → MergeTree,
#     Distributed('gpu-monitoring', …) → Distributed('default', …, rand()); dist→local 뷰 없이.
#     tests/e2e/ddl_test_dims.sql(토큰 레지스트리 최소 twin)은 변환 없이 이어 붙인다.
python3 - <<'PY'
import pathlib, re, urllib.error, urllib.request

CH = "http://127.0.0.1:18125/"
# 단일노드 E2E 는 admin 수동 절차(accounts.sql)가 없으므로 이 스크립트가 fact DB 를 대신 생성
sql = "CREATE DATABASE IF NOT EXISTS fact;\n"
sql += pathlib.Path("ddl/company/raw_token_metrics.sql").read_text(encoding="utf-8")
sql += "\n" + pathlib.Path("ddl/company/dim_token_metrics_service.sql").read_text(encoding="utf-8")

# 6a DDL 머리말 주석에 ';' 가 있는 줄이 있어 문장 분할 전에 주석 줄('--' 로 시작)을 제거
sql = re.sub(r"^[ \t]*--.*$", "", sql, flags=re.M)
sql = re.sub(r"ON CLUSTER 'gpu-monitoring'", "", sql)
sql = re.sub(r"ENGINE = ReplicatedMergeTree\([^)]*\)", "ENGINE = MergeTree", sql, flags=re.S)
sql = re.sub(r"ENGINE = Distributed\('gpu-monitoring', '(\w+)', '(\w+)',[\s\S]*?\);",
             r"ENGINE = Distributed('default', '\1', '\2', rand());", sql)
if "gpu-monitoring" in sql or "Replicated" in sql:
    print("single-node transform incomplete (gpu-monitoring / Replicated still present)")
    raise SystemExit(1)

dims = pathlib.Path("tests/e2e/ddl_test_dims.sql").read_text(encoding="utf-8")
sql += "\n" + re.sub(r"^[ \t]*--.*$", "", dims, flags=re.M)

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

n = 0
for stmt in split_statements(sql):
    req = urllib.request.Request(CH, data=(stmt.strip() + ";").encode("utf-8"))
    try:
        urllib.request.urlopen(req).read()
    except urllib.error.HTTPError as e:
        print(f"DDL failed (HTTP {e.code}): {e.read().decode(errors='replace')}")
        print(f"statement: {stmt.strip()[:200]}")
        raise SystemExit(1)
    n += 1
print(f"DDL applied (single-node transformed, {n} statements)")
PY
expect_eq "preflight dim_token_service twin" "$(chq 'SELECT count() FROM gpu_data.dim_token_service_dist')" "1"
expect_eq "fact raw_token_metrics tables" \
  "$(chq "SELECT count() FROM system.tables WHERE database = 'fact' AND name LIKE 'raw_token_metrics_%'")" "6"
expect_eq "fact audit tables" \
  "$(chq "SELECT count() FROM system.tables WHERE database = 'fact' AND name LIKE 'collect_audit_metrics_1d_%'")" "2"
expect_eq "gpu_data metrics registry tables" \
  "$(chq "SELECT count() FROM system.tables WHERE database = 'gpu_data' AND name LIKE 'dim_token_metrics_service_%'")" "2"

export CH_HOST=127.0.0.1 CH_PORT=18125 CH_USER=default CH_PASSWORD= CH_CLUSTER=""
export ENDPOINTS_FILE=tests/e2e/endpoints.e2e.yaml
cat > tests/e2e/endpoints.e2e.yaml <<EOF
services:
  - serviceGroup: "Mock Group"
    service: "${SVC}"
    baseUrl: "${MOCK_URL}"
    enabled: true
    apiSince: "2026-01-01"        # 정기 게이트(before_since) 통과 — T2 기본값 2026-09-09 보다 과거
    coverageSince: "2026-01-01"
EOF
mkdir -p "${TMP}"

# 기대치 — mock datagen 결정성 (같은 seed·date·models)
read -r EXP <<<"$(python3 tests/e2e/ci_expectations.py "${DATE_ARG}" "${SEED}" \
  "claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5")"
EXP_GPU_ROWS=$(sed -E 's/.*rows_gpu=([0-9]+).*/\1/' <<<"$EXP")
EXP_SERVING_ROWS=$(sed -E 's/.*rows_serving=([0-9]+).*/\1/' <<<"$EXP")
EXP_GPU_HOURS=$(sed -E 's/.*gpu_hours_sum=([0-9.]+).*/\1/' <<<"$EXP")
EXP_ROWS=$((EXP_GPU_ROWS + EXP_SERVING_ROWS))
expect_eq "expected rows (5 gpu + 9 serving)" "${EXP_ROWS}" "14"

MUT_Q="SELECT count() FROM system.mutations WHERE database IN ('fact', 'gpu_data')"
MUT_FACT_Q="SELECT count() FROM system.mutations WHERE database = 'fact'"
WHERE_D1="WHERE date = '${DATE_ARG}' AND service = '${SVC}'"
WHERE_D2="WHERE date = '${DATE2}' AND service = '${SVC}'"

# (3) 정기 1회 — batch_time = DATE_ARG 다음날 02:05 KST → target_date = DATE_ARG, slot=02(비최종)
run_collector "${NEXT_DAY}T02:05:00+09:00"
expect_eq "regular run 1 exit code" "${RC}" "0"
need_line "regular run 1 SERVICE_RESULT" "SERVICE_RESULT status=SUCCESS module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=${EXP_ROWS} pages=1 warn=0 rejected=0"
need_line "regular run 1 BATCH_RESULT" "BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 services_failed=0 services_skipped=0 rows=${EXP_ROWS} "
need_line "regular run 1 slot/final" "slot=02 final=0"
expect_eq "BATCH_RESULT lines in run 1" "$(grep -c '^BATCH_RESULT ' <<<"$OUT")" "1"

# (4) 정기 2회 — 앵커 존재 → already_loaded (fetch 없음) · 뮤테이션 0 (§4.0 정기 = 0; 레지스트리 diff 도 변경 없음)
run_collector "${NEXT_DAY}T02:05:00+09:00"
expect_eq "regular run 2 exit code" "${RC}" "0"
need_line "regular run 2 already_loaded" "SERVICE_RESULT status=SKIPPED module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=already_loaded"
need_line "regular run 2 BATCH_RESULT" "BATCH_RESULT status=SUCCESS module=token-metrics services_ok=0 services_failed=0 services_skipped=1 rows=0 "
expect_eq "mutations after 2 regular runs (fact + gpu_data)" "$(chq "${MUT_Q}")" "0"

# (5) verify --expect-empty — 정기 2회 직후 상태 (행수·합·앵커·감사 0·레지스트리·flags 빈 배열)
sed -e "s/{DATE}/${DATE_ARG}/g" -e "s/{SERVICE}/${SVC}/g" \
    -e "s/{EXP_GPU_ROWS}/${EXP_GPU_ROWS}/g" -e "s/{EXP_SERVING_ROWS}/${EXP_SERVING_ROWS}/g" \
    -e "s/{EXP_GPU_HOURS}/${EXP_GPU_HOURS}/g" tests/e2e/verify_expected_results.sql \
  | curl -sf --data-binary @- "${CH_URL}?default_format=TSV" > "${TMP}/verify_out.tsv"
if [ -s "${TMP}/verify_out.tsv" ]; then
  echo "E2E VERIFY FAILED:"; cat "${TMP}/verify_out.tsv"; exit 1
fi

# (6) --replace 재수집 — 앵커 존재 → 감사 INSERT 1 + DELETE 3(summary·gpu·serving _local) + INSERT (§5.4 · §4.0 날짜당 ≤3)
run_collector --from "${DATE_ARG}" --to "${DATE_ARG}" --replace
expect_eq "replace run exit code" "${RC}" "0"
need_line "replace SERVICE_RESULT" "SERVICE_RESULT status=SUCCESS module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=${EXP_ROWS} pages=1 warn=0 rejected=0"
need_line "replace BATCH_RESULT" "BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 services_failed=0 services_skipped=0 rows=${EXP_ROWS} "
expect_eq "fact mutations after --replace" "$(chq "${MUT_FACT_Q}")" "3"
expect_eq "gpu_data mutations after --replace (rerun never syncs registry)" \
  "$(chq "SELECT count() FROM system.mutations WHERE database = 'gpu_data'")" "0"
expect_eq "audit rows after --replace" "$(chq "SELECT count() FROM fact.collect_audit_metrics_1d_dist ${WHERE_D1}")" "1"
expect_eq "audit prev_source_type" \
  "$(chq "SELECT prev_source_type FROM fact.collect_audit_metrics_1d_dist ${WHERE_D1}")" "metrics-api-v1"
expect_eq "audit prev_gpu_rows" "$(chq "SELECT prev_gpu_rows FROM fact.collect_audit_metrics_1d_dist ${WHERE_D1}")" "${EXP_GPU_ROWS}"
expect_eq "summary anchor still once after --replace" \
  "$(chq "SELECT count() FROM fact.raw_token_metrics_summary_1d_dist ${WHERE_D1}")" "1"
expect_eq "gpu rows after --replace" "$(chq "SELECT count() FROM fact.raw_token_metrics_gpu_1d_dist ${WHERE_D1}")" "${EXP_GPU_ROWS}"
expect_eq "serving rows after --replace" \
  "$(chq "SELECT count() FROM fact.raw_token_metrics_serving_1d_dist ${WHERE_D1}")" "${EXP_SERVING_ROWS}"

# (7) 시나리오 A — gpuHours > gpuCount×24 → CHECK WARN hours_over_count=1, 행은 flags 로 적재·SUCCESS (§5.3 계층 2)
scenario '{"metrics_gpu_hours_over": 1}'
run_collector --from "${DATE_ARG}" --to "${DATE_ARG}" --replace
expect_eq "scenario A exit code" "${RC}" "0"
need_line "scenario A CHECK WARN" "CHECK WARN service=${SVC} hours_over_count=1"
need_line "scenario A SERVICE_RESULT" "SERVICE_RESULT status=SUCCESS module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=${EXP_ROWS} pages=1 warn=1 rejected=0"
need_line "scenario A BATCH_RESULT" "BATCH_RESULT status=SUCCESS module=token-metrics"
expect_eq "scenario A flagged gpu rows" \
  "$(chq "SELECT count() FROM fact.raw_token_metrics_gpu_1d_dist ${WHERE_D1} AND has(flags, 'hours_over_count')")" "1"

# 시나리오 B — gpu 빈 배열 + serving 행 = 케이스 E → SUCCESS rows=serving 만, summary gpu_rows=0 (§5.2 표 200 행)
scenario '{"metrics_gpu_hours_over": 0, "metrics_empty_gpu": 1}'
run_collector --from "${DATE_ARG}" --to "${DATE_ARG}" --replace
expect_eq "scenario B exit code" "${RC}" "0"
need_line "scenario B SERVICE_RESULT" "SERVICE_RESULT status=SUCCESS module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=${EXP_SERVING_ROWS} "
need_line "scenario B BATCH_RESULT" "BATCH_RESULT status=SUCCESS module=token-metrics"
expect_eq "scenario B summary gpu_rows" "$(chq "SELECT gpu_rows FROM fact.raw_token_metrics_summary_1d_dist ${WHERE_D1}")" "0"
expect_eq "scenario B gpu rows" "$(chq "SELECT count() FROM fact.raw_token_metrics_gpu_1d_dist ${WHERE_D1}")" "0"
expect_eq "scenario B serving rows" \
  "$(chq "SELECT count() FROM fact.raw_token_metrics_serving_1d_dist ${WHERE_D1}")" "${EXP_SERVING_ROWS}"
expect_eq "fact mutations after 3 replaces (3 per date per replace)" "$(chq "${MUT_FACT_Q}")" "9"
expect_eq "audit rows after 3 replaces (append-only)" \
  "$(chq "SELECT count() FROM fact.collect_audit_metrics_1d_dist ${WHERE_D1}")" "3"

# 시나리오 C — 409 data_not_ready: 정기 호출은 앵커 존재로 already_loaded 가 먼저이므로 다른 날짜(DATE2) rerun.
#   rerun 모드 규칙(§5.2): Retry-After 뒤 1회 재방문 → 재차 409 = FAILURE reason=not_ready, exit 1
scenario '{"metrics_empty_gpu": 0, "not_ready_until_uptime_s": 100000, "retry_after_s": 1}'
run_collector --from "${DATE2}" --to "${DATE2}"
expect_eq "scenario C exit code" "${RC}" "1"
need_line "scenario C SERVICE_RESULT" "SERVICE_RESULT status=FAILURE module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=not_ready"
need_line "scenario C BATCH_RESULT" "BATCH_RESULT status=FAILURE module=token-metrics services_ok=0 services_failed=1 services_skipped=0 rows=0 "
expect_eq "scenario C loaded nothing" "$(chq "SELECT count() FROM fact.raw_token_metrics_summary_1d_dist ${WHERE_D2}")" "0"
scenario '{"not_ready_until_uptime_s": 0}'      # reset (retry_after_s 는 ≥1 이어야 하므로 그대로 둔다)

# (8) manual-v0 — {DATE} → DATE2 치환본을 .tmp/ 에 생성, 같은 normalize+replace 경로 (§5.5)
for f in gpu serving engine; do
  sed "s/{DATE}/${DATE2}/g" "tests/e2e/manual_e2e/e2e_manual_v0_${f}.csv" > "${TMP}/e2e_manual_v0_${f}.csv"
done
run_collector --manual-gpu "${TMP}/e2e_manual_v0_gpu.csv" --manual-serving "${TMP}/e2e_manual_v0_serving.csv" \
  --manual-engine "${TMP}/e2e_manual_v0_engine.csv" --from "${DATE2}" --to "${DATE2}" \
  --generated-at "${NEXT_DAY}T09:00:00+09:00"
expect_eq "manual run exit code" "${RC}" "0"
need_line "manual MANUAL_INPUT" "MANUAL_INPUT module=token-metrics rows_gpu=2 rows_serving=3 rows_engine=1 rows_outside_range=0 rows_other_service=0"
need_line "manual SERVICE_RESULT" "SERVICE_RESULT status=SUCCESS module=token-metrics service=${SVC} source_type=manual-v0 rows=5 pages=1"
need_line "manual BATCH_RESULT" "BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 services_failed=0 services_skipped=0 rows=5 "
expect_eq "manual anchor (manual-v0)" \
  "$(chq "SELECT count() FROM fact.raw_token_metrics_summary_1d_dist ${WHERE_D2} AND source_type = 'manual-v0'")" "1"
expect_eq "manual engine_version" \
  "$(chq "SELECT engine_version FROM fact.raw_token_metrics_summary_1d_dist ${WHERE_D2}")" "0.8.4"
expect_eq "manual gpu rows" "$(chq "SELECT count() FROM fact.raw_token_metrics_gpu_1d_dist ${WHERE_D2}")" "2"
expect_eq "manual serving rows" "$(chq "SELECT count() FROM fact.raw_token_metrics_serving_1d_dist ${WHERE_D2}")" "3"
expect_eq "manual source_type on gpu rows" \
  "$(chq "SELECT countIf(source_type != 'manual-v0') FROM fact.raw_token_metrics_gpu_1d_dist ${WHERE_D2}")" "0"
expect_eq "fact mutations after manual first load (no anchor → INSERT only)" "$(chq "${MUT_FACT_Q}")" "9"

# manual 앵커가 있는 날짜의 rerun(--replace 없음) → already_loaded, 뮤테이션 0
run_collector --from "${DATE2}" --to "${DATE2}"
expect_eq "rerun after manual exit code" "${RC}" "0"
need_line "rerun after manual already_loaded" "SERVICE_RESULT status=SKIPPED module=token-metrics service=${SVC} source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=already_loaded"
expect_eq "fact mutations unchanged after already_loaded" "$(chq "${MUT_FACT_Q}")" "9"

# (9)
echo "E2E PASS (date=${DATE_ARG}, gpu=${EXP_GPU_ROWS}, serving=${EXP_SERVING_ROWS})"
```

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && chmod +x tests/e2e/run_e2e.sh && bash -n tests/e2e/run_e2e.sh && echo SYNTAX_OK && test -x tests/e2e/run_e2e.sh && echo EXEC_OK && grep -c "set -euo pipefail" tests/e2e/run_e2e.sh && grep -c "need_line \|expect_eq " tests/e2e/run_e2e.sh && grep -o "18125\|18001\|ch-e2e-metrics\|mock-e2e-metrics\|tokenmetricse2e\|token-mock-provider:e2e\|clickhouse-server:24.8" tests/e2e/run_e2e.sh | sort -u | wc -l && grep -c "python3 -m app.main" tests/e2e/run_e2e.sh; grep -c "/tmp/" tests/e2e/run_e2e.sh; grep -n "18123\|18000\|tokene2e \|ch-e2e \|mock-e2e " tests/e2e/run_e2e.sh | wc -l`
Expected: `SYNTAX_OK`, `EXEC_OK`, `1`, `57`(검사 호출 수 — 정확한 값은 위 본문 기준이며 50 이상이면 된다), `7`(포트·이름 7종 전부 등장), `1`(수집기 호출은 `run_collector` 헬퍼 1곳으로 집중), `0`(임시 파일은 `tests/e2e/.tmp/` — `/tmp` 미사용), `0`(기존 e2e 의 포트·이름 미사용 → 병렬 실행 충돌 없음).

Run: `cd /home/mini/github/token-data-pipeline/collectors/token-metrics && awk '/^python3 - <<.PY.$/{f=1; next} /^PY$/{f=0} f' tests/e2e/run_e2e.sh > /tmp/claude-1000/-home-mini-github-token-data-pipeline/8a0025e9-e64c-4caf-972c-788ea90abe0e/scratchpad/e2e_ddl_block.py && python3 -c "import ast; ast.parse(open('/tmp/claude-1000/-home-mini-github-token-data-pipeline/8a0025e9-e64c-4caf-972c-788ea90abe0e/scratchpad/e2e_ddl_block.py').read()); print('ddl-block ast ok')" && python3 - <<'PY'
# DDL 변환 정규식을 6a 실제 파일에 드라이런 — CH 없이 변환 결과만 검사 (run_e2e.sh 의 python 블록과 같은 식)
import pathlib, re
sql = pathlib.Path("ddl/company/raw_token_metrics.sql").read_text(encoding="utf-8")
sql += "\n" + pathlib.Path("ddl/company/dim_token_metrics_service.sql").read_text(encoding="utf-8")
sql = re.sub(r"^[ \t]*--.*$", "", sql, flags=re.M)
sql = re.sub(r"ON CLUSTER 'gpu-monitoring'", "", sql)
sql = re.sub(r"ENGINE = ReplicatedMergeTree\([^)]*\)", "ENGINE = MergeTree", sql, flags=re.S)
sql = re.sub(r"ENGINE = Distributed\('gpu-monitoring', '(\w+)', '(\w+)',[\s\S]*?\);",
             r"ENGINE = Distributed('default', '\1', '\2', rand());", sql)
out, buf, in_str = [], [], False          # run_e2e.sh 의 split_statements 와 같은 규칙 (따옴표 안 ';' 무시)
for ch in sql:
    if ch == "'":
        in_str = not in_str
    if ch == ";" and not in_str:
        out.append("".join(buf)); buf = []
    else:
        buf.append(ch)
out.append("".join(buf))
stmts = [s.strip() for s in out if s.strip()]
print("statements=%d create_table=%d gpu-monitoring=%d Replicated=%d distributed_default=%d comment_only=%d odd_quotes=%d" % (
    len(stmts), sum(1 for s in stmts if s.startswith("CREATE TABLE")), sql.count("gpu-monitoring"), sql.count("Replicated"),
    sql.count("Distributed('default'"), sum(1 for s in stmts if s.startswith("--")),
    sum(1 for s in stmts if s.count("'") % 2)))
PY`
Expected: `ddl-block ast ok`, 그다음 `statements=10 create_table=10 gpu-monitoring=0 Replicated=0 distributed_default=5 comment_only=0 odd_quotes=0`(fact 4테이블×2 + dim 2 = CREATE TABLE 10; `_dist` 5개 전부 `default` 클러스터로; 주석만 남은 문장 0 — 6a 머리말 주석의 `;` 는 주석 줄 제거로, 컬럼 COMMENT 문자열 안의 `;`(`model` · `generated_at` 컬럼 2곳)는 따옴표 인식 분할로 문장을 깨지 않는다; 따옴표 홀수 문장 0). 6a DDL 에 CREATE DATABASE 문이 있으면 `statements` 만 그만큼 늘 수 있다 — 나머지 값은 반드시 위와 같아야 한다. `create_table` 이 10 이 아니거나 `odd_quotes` 가 0 이 아니면 분할이 깨진 것 — run_e2e.sh 로 넘어가지 말고 여기서 고친다.

- [ ] **Step 7: `.github/workflows/test-collector-metrics.yml` — unit / e2e / image / manifests 4 job (기존 `test-collector.yml` 클론, 이름·경로 전면 교체)**

기존 `test-collector.yml` 은 한 글자도 바꾸지 않는다(§5.6, §7.5 zero-diff). 새 워크플로는 `collectors/token-metrics/**` · `tools/mock-provider/**` · 자기 자신 경로에만 반응한다. manifests job 은 T8 이 고정한 계약 문자열을 렌더 결과에서 grep 한다(T8 `tests/test_manifests.py::test_kustomize_render_if_available` 와 같은 목록 — CI 는 kubectl 이 있으므로 항상 실행). `token-usage` 누출 검사는 `if grep -q …; then exit 1; fi` 형태로 쓴다 — `! grep -q` 는 bash `set -e` 가 반전 명령의 실패를 무시하므로 job 이 실패하지 않는다(GitHub Actions 의 기본 shell 은 `bash -e {0}`).

`.github/workflows/test-collector-metrics.yml` 신규(전체 내용):

```yaml
name: test-collector-metrics

# Plan 6b T11 — token-metrics 수집기 전용 CI (설계 §5.6). 기존 test-collector.yml 은 무수정(§7.5 zero-diff).
on:
  push:
    branches: [main]
    paths: ["collectors/token-metrics/**", "tools/mock-provider/**",
            ".github/workflows/test-collector-metrics.yml"]
  pull_request:
    paths: ["collectors/token-metrics/**", "tools/mock-provider/**",
            ".github/workflows/test-collector-metrics.yml"]

jobs:
  unit:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: collectors/token-metrics
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -r requirements-dev.txt
      - run: python -m pytest tests/ -v --ignore=tests/e2e
      - name: E2E expectations are deterministic (mock datagen, no docker)
        run: |
          python tests/e2e/ci_expectations.py 2026-09-10 e2e-seed-1 "claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5" \
            | grep -q '^rows_gpu=5 rows_serving=9 gpu_hours_sum='

  e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -r collectors/token-metrics/requirements-dev.txt
      - name: Build mock image
        run: docker build -t token-mock-provider:e2e tools/mock-provider
      - name: Run E2E
        run: ./collectors/token-metrics/tests/e2e/run_e2e.sh

  image:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: collectors/token-metrics
    steps:
      - uses: actions/checkout@v4
      - name: Build image
        run: docker build -t token-metrics-collector:ci .
      - name: Container smoke test
        run: |
          # CH 없이 argparse 도움말이 exit 0으로 떠야 함 (이미지 엔트리 검증 — T6 CLI)
          docker run --rm token-metrics-collector:ci python -m app.main --help

  manifests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Render overlays
        run: |
          for o in stage company company-verify; do
            kubectl kustomize "collectors/token-metrics/k8s/overlays/${o}" > "/tmp/metrics-${o}.yaml"
          done
      - name: Contract fields locked (§5.2/§7.2 — T8 계약 문자열)
        run: |
          for f in /tmp/metrics-stage.yaml /tmp/metrics-company.yaml /tmp/metrics-company-verify.yaml; do
            grep -q 'schedule: 5 2-9 \* \* \*' "$f"
            grep -q 'timeZone: Asia/Seoul' "$f"
            grep -q 'startingDeadlineSeconds: 540' "$f"
            grep -q 'activeDeadlineSeconds: 3000' "$f"
            grep -q 'backoffLimit: 0' "$f"
            grep -q 'concurrencyPolicy: Forbid' "$f"
            grep -q 'memory: 1Gi' "$f"
            grep -q 'memory: 256Mi' "$f"
            grep -q 'name: registry-pull-secret' "$f"
            grep -q 'name: token-metrics-ch-secret' "$f"
            grep -q 'name: token-metrics-endpoints' "$f"
            grep -q 'name: token-metrics-ca-bundle' "$f"
            grep -q 'METRICS_MAX_MUTATIONS_PER_RUN' "$f"
            # 기존 모듈 이름 누출 0 (§5.1 이름 전면 교체) — 부정 연산자(느낌표) 반전은 set -e 를 우회하므로 if/exit 로 쓴다
            if grep -q 'token-usage' "$f"; then echo "token-usage leaked in $f"; exit 1; fi
          done
          grep -q 'ghcr.io/yoonsungnam/token-metrics-collector' /tmp/metrics-stage.yaml
          grep -q 'name: token-metrics-collector-verify' /tmp/metrics-company-verify.yaml
          grep -q 'name: token-metrics-ch-secret-verify' /tmp/metrics-company-verify.yaml
          grep -q 'name: token-metrics-endpoints-verify' /tmp/metrics-company-verify.yaml
```

Run: `python3 - <<'PY'
import yaml
w = yaml.safe_load(open(".github/workflows/test-collector-metrics.yml", encoding="utf-8"))
print("name=%s jobs=%s" % (w["name"], ",".join(w["jobs"])))
print("push_paths=%d pr_paths=%d" % (len(w[True]["push"]["paths"]), len(w[True]["pull_request"]["paths"])))
steps = w["jobs"]["manifests"]["steps"][2]["run"]
print("grep_q=%d if_grep=%d bang_grep=%d" % (steps.count("grep -q"), steps.count("if grep -q 'token-usage'"), steps.count("! grep")))
print("unit_steps=%d e2e_run=%s" % (len(w["jobs"]["unit"]["steps"]), w["jobs"]["e2e"]["steps"][-1]["run"]))
PY
grep -c "token-usage" .github/workflows/test-collector-metrics.yml; git diff --stat -- .github/workflows/test-collector.yml | wc -l`
Expected: `name=test-collector-metrics jobs=unit,e2e,image,manifests`, `push_paths=3 pr_paths=3`, `grep_q=18 if_grep=1 bang_grep=0`(루프 안 13 + 누출 if 1 + 루프 밖 4 = 18; `! grep` 0), `unit_steps=5 e2e_run=./collectors/token-metrics/tests/e2e/run_e2e.sh`, 그다음 `1`(파일 안 `token-usage` 는 누출 검사 grep 패턴 1줄뿐 — 주석의 `test-collector.yml` 언급은 `token-usage` 문자열이 아니다), `0`(기존 워크플로 무수정). (`on` 키는 PyYAML 1.1 에서 불리언 `True` 로 읽히므로 `w[True]` 로 접근한다.)

- [ ] **Step 8: `.github/workflows/release-images-metrics.yml` — matrix `collectors/token-metrics` 1항목 (기존 `release-images.yml` 무수정 → 기존 이미지 재빌드 유발 0)**

기존 `release-images.yml` 의 paths 에 `collectors/token-metrics/**` 를 넣으면 push 마다 mock·usage·mart 이미지 3개가 함께 재빌드된다 — 설계 §5.6 은 그래서 별도 워크플로를 요구한다. matrix 는 **1항목만**(Plan 6c 가 `mart/token-metrics` 항목과 paths 를 additive 로 추가). steps 는 기존과 동일(buildx → sha7 → GHCR login → build-push; 태그 `latest` + sha7 — 사내 Harbor 반입은 sha7 태그 지정, §5.6).

`.github/workflows/release-images-metrics.yml` 신규(전체 내용):

```yaml
name: release-images-metrics

# Plan 6b T11 — token-metrics-collector 이미지 전용 릴리스 (설계 §5.6).
# 기존 release-images.yml 은 무수정: 이 경로 변경으로 mock/usage/mart 이미지가 재빌드되지 않게 분리한다.
# Plan 6c 가 mart/token-metrics 항목과 paths 를 additive 로 추가한다.
on:
  push:
    branches: [main]
    paths:
      - "collectors/token-metrics/**"
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

Run: `python3 - <<'PY'
import yaml
w = yaml.safe_load(open(".github/workflows/release-images-metrics.yml", encoding="utf-8"))
inc = w["jobs"]["release"]["strategy"]["matrix"]["include"]
print("name=%s matrix=%d ctx=%s image=%s" % (w["name"], len(inc), inc[0]["context"], inc[0]["image"]))
print("paths=%s dispatch=%s" % (w[True]["push"]["paths"], "workflow_dispatch" in w[True]))
print("steps=%d tags_lines=%d perms=%s" % (len(w["jobs"]["release"]["steps"]),
      len(w["jobs"]["release"]["steps"][-1]["with"]["tags"].strip().splitlines()), w["jobs"]["release"]["permissions"]))
PY
diff <(sed -n '/^    steps:/,$p' .github/workflows/release-images.yml) <(sed -n '/^    steps:/,$p' .github/workflows/release-images-metrics.yml) && echo STEPS_IDENTICAL; grep -c "token-usage\|mart/token-usage\|mock-provider" .github/workflows/release-images-metrics.yml; git diff --stat -- .github/workflows/release-images.yml | wc -l`
Expected: `name=release-images-metrics matrix=1 ctx=collectors/token-metrics image=token-metrics-collector`, `paths=['collectors/token-metrics/**', '.github/workflows/release-images-metrics.yml'] dispatch=True`, `steps=5 tags_lines=2 perms={'packages': 'write', 'contents': 'read'}`, `STEPS_IDENTICAL`(steps 블록은 기존 파일과 바이트 동일 — 클론 규칙), `0`(기존 이미지 3종 어느 것도 언급 없음 → 재빌드 유발 0), `0`(기존 워크플로 무수정).

- [ ] **Step 9: `.gitignore` +2행 — 새 e2e 임시 endpoints 와 `tests/e2e/.tmp/` (§7.2 환경 데이터 경계, 기존 행 무수정)**

기존 12행 뒤에 append 하는 것이 아니라, 같은 목적의 기존 행(`collectors/token-usage/tests/e2e/endpoints.e2e.yaml`) **바로 뒤**에 2행을 넣는다 — Plan 6a 가 이미 `.gitignore` 아래쪽에 G 블록(`*manual_metrics*.csv` 등)을 추가했더라도 그 블록은 건드리지 않는다(6a 블록 패턴은 `e2e_manual_v0_*.csv` 와 매치되지 않으므로 Step 3 의 합성 CSV 는 커밋 가능). 기존 행 12줄은 한 글자도 바뀌지 않는다.

`.gitignore` 변경(before → after):

```diff
 # E2E 실행 시 run_e2e.sh가 생성하는 임시 endpoints — 레포 반입 금지
 collectors/token-usage/tests/e2e/endpoints.e2e.yaml
+collectors/token-metrics/tests/e2e/endpoints.e2e.yaml
+collectors/token-metrics/tests/e2e/.tmp/
```

Run: `sed -i '/^collectors\/token-usage\/tests\/e2e\/endpoints.e2e.yaml$/a collectors/token-metrics/tests/e2e/endpoints.e2e.yaml\ncollectors/token-metrics/tests/e2e/.tmp/' .gitignore && git diff --stat -- .gitignore && git diff -- .gitignore | grep -c '^-[^-]' ; git check-ignore -v collectors/token-metrics/tests/e2e/endpoints.e2e.yaml collectors/token-metrics/tests/e2e/.tmp/verify_out.tsv collectors/token-metrics/tests/e2e/.tmp/e2e_manual_v0_gpu.csv; git check-ignore -q collectors/token-metrics/tests/e2e/manual_e2e/e2e_manual_v0_gpu.csv && echo "IGNORED(bad)" || echo "COMMITTABLE"; git check-ignore -q collectors/token-metrics/tests/e2e/run_e2e.sh && echo "IGNORED(bad)" || echo "COMMITTABLE"`
Expected: `.gitignore | 2 ++`(+2 −0), `0`(삭제된 행 0), `check-ignore -v` 3줄이 각각 `.gitignore:13:collectors/token-metrics/tests/e2e/endpoints.e2e.yaml` · `.gitignore:14:collectors/token-metrics/tests/e2e/.tmp/` ×2 를 가리킨다(6a G 블록이 이미 있으면 줄 번호가 다를 수 있으나 패턴은 같아야 한다), 그다음 `COMMITTABLE` 2회(합성 manual CSV · 스크립트는 무시되지 않는다).

- [ ] **Step 10: 로컬 검증 — 문법·YAML·zero-diff·공개 레포 경계·(도커 있으면) E2E 실주행**

Run: `bash -n collectors/token-metrics/tests/e2e/run_e2e.sh && echo SH_OK && python3 -c "import yaml; yaml.safe_load(open('.github/workflows/test-collector-metrics.yml')); yaml.safe_load(open('.github/workflows/release-images-metrics.yml')); print('YAML_OK')" && python3 -c "import ast; ast.parse(open('collectors/token-metrics/tests/e2e/ci_expectations.py').read()); print('PY_OK')" && git diff --stat -- .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring | wc -l && git status --porcelain -- collectors tools .github .gitignore | grep -v "^?? collectors/token-metrics/tests/e2e/\|^?? .github/workflows/test-collector-metrics.yml\|^?? .github/workflows/release-images-metrics.yml\|^ M .gitignore" | wc -l && git status --porcelain -- collectors tools .github .gitignore | wc -l`
Expected: `SH_OK`, `YAML_OK`, `PY_OK`, `0`(zero-diff 대상 전부 무변경), `0`(이 태스크 산출물 외 변경 없음 — T1~T10 을 이미 커밋한 상태가 전제), `4`(`?? collectors/token-metrics/tests/e2e/` · 워크플로 2 · ` M .gitignore`; git 은 새 디렉터리를 1행으로 접는다). 두 번째 `wc -l` 이 0 이 아니면 그 행을 먼저 처리한다(이 태스크가 만든 파일이 아니면 커밋에 넣지 않는다). `git status`를 이 태스크의 경로(`collectors tools .github .gitignore`)로 한정하는 이유: 경로 한정이 없으면 세션 시작 시부터 있던 `?? docs/superpowers/specs/…` 같은 무관한 미추적 파일이 섞여 0/4 가 어긋난다 — 그 파일들은 이 태스크와 무관하며 커밋에 넣지 않는다.

Run: `cd /home/mini/github/token-data-pipeline && grep -rn -E "[a-z0-9-]+\.(corp|internal|local|company|co\.kr)\b|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,}|harbor\.|\.svc\b|10\.[0-9]+\.[0-9]+\.[0-9]+|192\.168\." collectors/token-metrics/tests/e2e .github/workflows/test-collector-metrics.yml .github/workflows/release-images-metrics.yml | grep -v "127.0.0.1" | wc -l && grep -rn "127.0.0.1" collectors/token-metrics/tests/e2e/run_e2e.sh | wc -l`
Expected: `0`(사내 호스트명·주소·이메일·사설 IP 0 — 새 파일이 아는 주소는 루프백뿐), 그다음 `6`(`CH_URL` · `MOCK_URL` · ping 2곳 · python 블록 `CH` · `CH_HOST=127.0.0.1` — 스크립트의 루프백 6곳; `endpoints.e2e.yaml` 의 `baseUrl` 은 `${MOCK_URL}` 변수 참조라 여기 안 잡힌다).

Run(도커가 있을 때만 — `docker info >/dev/null 2>&1 || echo NO_DOCKER` 로 먼저 확인): `cd /home/mini/github/token-data-pipeline && docker build -q -t token-mock-provider:e2e tools/mock-provider && ./collectors/token-metrics/tests/e2e/run_e2e.sh 2>&1 | tail -n 3; echo "rc=${PIPESTATUS[0]}"; docker ps -a --format '{{.Names}}' | grep -c "e2e-metrics"; git status --porcelain collectors/token-metrics/tests/e2e/ | grep -c "endpoints.e2e.yaml\|\.tmp/"`
Expected: 마지막 3줄 안에 `E2E PASS (date=<어제>, gpu=5, serving=9)`, `rc=0`, `0`(trap 이 컨테이너 2개를 제거했다), `0`(생성된 `endpoints.e2e.yaml` · `.tmp/` 는 Step 9 의 .gitignore 로 가려져 status 에 안 뜬다). `NO_DOCKER` 면 이 Run 은 건너뛰고 Step 11 커밋 본문에 `로컬 도커 부재, CI e2e로 검증` 을 넣는다(outline 4). E2E 가 중간에 실패하면 실패 줄(`need_line` 의 `MISSING:` / `expect_eq` 의 `expected=… actual=…` / `E2E VERIFY FAILED:` 이하 TSV)이 원인이다 — 스크립트가 아니라 해당 태스크(T3 정규화·T5 writer·T6 main·T7 manual)의 코드를 먼저 의심하고, 기대값 자체가 틀렸다고 확인됐을 때만 Step 5/6 의 상수를 고친다.

- [ ] **Step 11: Commit — 신규 9파일 + `.gitignore` (기존 워크플로·모듈 무수정)**

Run:
```bash
cd /home/mini/github/token-data-pipeline
git add collectors/token-metrics/tests/e2e/run_e2e.sh \
        collectors/token-metrics/tests/e2e/ci_expectations.py \
        collectors/token-metrics/tests/e2e/verify_expected_results.sql \
        collectors/token-metrics/tests/e2e/ddl_test_dims.sql \
        collectors/token-metrics/tests/e2e/manual_e2e/e2e_manual_v0_gpu.csv \
        collectors/token-metrics/tests/e2e/manual_e2e/e2e_manual_v0_serving.csv \
        collectors/token-metrics/tests/e2e/manual_e2e/e2e_manual_v0_engine.csv \
        .github/workflows/test-collector-metrics.yml \
        .github/workflows/release-images-metrics.yml \
        .gitignore
git diff --cached --stat | tail -n 1     # 기대: "10 files changed, … insertions(+)" — 삭제(-) 0
git ls-files --cached --stage collectors/token-metrics/tests/e2e/run_e2e.sh | cut -c1-6   # 기대: 100755 (실행 비트)
if docker info >/dev/null 2>&1; then E2E_NOTE="로컬 E2E 실주행 PASS (CH 24.8 + mock, gpu=5 serving=9)"; else E2E_NOTE="로컬 도커 부재, CI e2e로 검증"; fi
git commit -m "ci(collectors-metrics): E2E(CH 24.8+mock — 2회 실행 already_loaded·mutations 0·--replace 3·시나리오·manual 1회)·test-collector-metrics·release-images-metrics 신규 (Plan 6b T11)" \
  -m "run_e2e.sh: 단일노드 DDL 변환(주석 줄 제거 + 따옴표 인식 ';' 분할 — 6a COMMENT 안 ';' 대응) → 정기 2회(2회차 SKIPPED already_loaded, system.mutations 0) → verify 11종 --expect-empty → --replace(fact mutations 3, audit prev_* 1) → 시나리오 A(hours_over_count WARN)/B(gpu 빈 배열 rows=9)/C(rerun 409 → FAILURE not_ready) → manual-v0 1회(rows=5, mutations 추가 0). ${E2E_NOTE}." \
  -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>" \
  -m "Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
git show --stat HEAD | tail -n 12
```
Expected: `10 files changed`(+ 만; `.gitignore` 2 ++ 포함), `100755`, 커밋 stat 에 9개 신규 파일 + `.gitignore` 만 나열되고 `collectors/token-usage/` · `.github/workflows/release-images.yml` · `.github/workflows/test-collector.yml` 은 없다.

**Task 11 Self-Review 메모 (설계 해석 — 설계·outline 이 정하지 않았거나 두 갈래인 곳을 이렇게 고정했다):**

- **DDL 적용 루프가 D9.1 클론과 다르다**: 6a `raw_token_metrics.sql` 의 컬럼 COMMENT 문자열 2곳(`model` — `(≤128; 정규화는 mart)`, `generated_at` — `now(KST)+WARN; 오프셋≠+09:00`)과 머리말 주석에 `;` 가 있어 기존 e2e 의 `sql.split(";")` 을 그대로 쓰면 문장이 12개로 깨진다. 그래서 (a) 줄머리 `--` 주석 줄을 먼저 제거하고 (b) 작은따옴표 안의 `;` 를 건너뛰는 `split_statements` 로 분할한다(Step 6 의 드라이런이 `create_table=10 odd_quotes=0` 으로 이를 검증). `ddl_test_dims.sql` 도 같은 루프를 타므로 문자열 리터럴에 `;` 를 두지 않는다.
- **summary 카운트 컬럼명**: outline 의 `n_gpu`/`n_serving` 은 T3 `NormalizeResult` 프로퍼티 이름이고, 6a `raw_token_metrics_summary_1d` 의 컬럼은 `gpu_rows`/`serving_rows` 다(T5 writer 가 그렇게 INSERT). verify SQL `summary_counts` 와 run_e2e.sh 의 시나리오 B 검사는 컬럼명을 쓴다.
- **`system.mutations` 실측**: 단일노드 CH 24.8 은 완료된 뮤테이션도 `system.mutations` 에 남기므로(기본 `finished_mutations_to_keep=100`) 정기 2회 후 0 · `--replace` 후 fact 3 · 3회 후 9 · manual 후 9(추가 0) 를 개수로 검증한다. 클러스터 환경 수치가 아니라 **이 스크립트가 만든 단일노드**의 장부라는 점을 주석에 적었다. `database IN ('fact','gpu_data')` 로 좁혀 CH 내부 뮤테이션을 배제한다.
- **시나리오 C 의 해제**: mock `POST /__mock/scenario` 는 키별 merge 이므로 `{"not_ready_until_uptime_s": 0}` 만 보내 해제한다(`retry_after_s` 는 ≥1 검증이 있어 0 으로 못 돌리고, 1 이면 T6 재방문 대기 `min(max(1,1),300)=1s` 라 그대로 둬도 무해). 대상 날짜는 `DATE2 = DATE_ARG − 1일`(retention 14일 안) — 정기 대상일 `DATE_ARG` 의 적재 상태를 건드리지 않기 위해서다.
- **manual CSV 의 형식**: `#` 주석 1줄 + 헤더 + 데이터 — T7 파서가 `#` 줄을 건너뛰고 헤더를 상수와 비교한다는 계약에 맞췄다. 표준 지표(`ttftMs`/`itlMs`/`outputTps`) 행은 `name`/`unit` 을 비워 둔다(6a 템플릿 규칙 — T7 이 표준 지표의 name/unit 을 무시). `--generated-at "${NEXT_DAY}T09:00:00+09:00"` 는 오프셋 WARN 을 내지 않는 값이다.
- **`.gitignore` 삽입 위치**: 파일 끝 append 가 아니라 같은 목적의 기존 행(`collectors/token-usage/tests/e2e/endpoints.e2e.yaml`) 바로 뒤 — 6a G 블록의 존재·위치와 무관하게 `sed /a` 가 같은 결과를 낸다. 6a 패턴 `*manual_metrics*.csv` 는 `e2e_manual_v0_*.csv` 와 매치되지 않으므로 합성 CSV 는 커밋 가능(Step 9 의 `check-ignore` 가 확인).
- **임시 파일 위치**: 기존 e2e 는 `/tmp/verify_out.tsv` 를 쓰지만 여기서는 `tests/e2e/.tmp/`(gitignored) — 병렬 실행·권한·정리 문제를 피하고 outline 의 지시(`.tmp/` 를 `.gitignore` 에 추가)를 따른다. Step 6 검증의 `grep -c "/tmp/"` = 0 이 이를 고정한다.
- **`! grep -q 'token-usage'`(outline) 의 구현**: bash `set -e` 는 `!` 로 반전된 명령의 실패를 무시하므로 그대로 쓰면 누출이 있어도 job 이 통과한다. 의미는 같고 실패가 보장되는 `if grep -q …; then …; exit 1; fi` 로 썼다.
- **manifests job 의 렌더 파일명**: 기존 워크플로의 `/tmp/stage.yaml` 과 겹치지 않게 `/tmp/metrics-<overlay>.yaml` — 같은 러너를 쓰지는 않지만 로컬에서 두 워크플로의 명령을 이어 돌려도 서로 덮어쓰지 않는다.
- **T6 마감 시계**: `--replace`·시나리오·manual 호출 모두 batch_time 인자를 `${NEXT_DAY}T02:05:00+09:00` 으로 주지만, T6 의 마감은 `clock()`(프로세스 기동 기준 단조 시계) 이라 이 인자는 `target_date`·`slot=02`·`final=0` 만 정한다 — 그래서 모든 BATCH_RESULT 기대 문자열이 `slot=02 final=0` 이다.
- **추가한 프리플라이트**: outline 에 없는 테이블 수 확인 4종(`dim_token_service_%` 1 · `raw_token_metrics_%` 6 · `collect_audit_metrics_1d_%` 2 · `dim_token_metrics_service_%` 2)을 DDL 직후에 넣었다 — 6a 파일이 바뀌어 테이블 수가 달라지면 수집기 오류가 아니라 DDL 단계에서 바로 멈추게 하기 위해서다.
- **기대치 상수의 출처**: `rows_gpu=5`(3모델 serving + standby 1 + `unknown`/test 1) · `rows_serving=9`(3레코드 × 3지표) 는 T1 datagen 의 고정 구조라 seed 와 무관하고, `gpu_hours_sum` 만 seed 의존이라 CI unit job 의 grep 은 앞 두 값만 고정한다. manual 의 `rows=5` 는 gpu 2 + serving 3(T7 `SERVICE_RESULT rows` = gpu+serving 행 합) 이다.

---

### Task 12: 모듈 README — 실행·모드×게이트 표·환경변수·배포·manual-v0 절차·rerun 창·부분 적재 복구·마커·검증·DDL/뮤테이션 장부 (10개 절)

**Files:**
- Create: `collectors/token-metrics/README.md`(`## ` 절 정확히 10개 — 순서 고정: 실행 / 모드와 게이트 / 환경변수 / 배포 / 수기 적재 / 재수행 / 부분 적재 복구 / 마커 / 검증 / DDL·뮤테이션 장부)
- Modify: 없음 (`collectors/token-usage/README.md`는 절 구성의 클론 원본 — 읽기만, zero-diff)
- Test: 없음(문서). 검증은 Step 12의 `grep` 계약 + `--help` 플래그 대조(T6/T7/T9/T10 argparse와 README 명령의 플래그 집합 일치).

**설계 근거:** §5.1(233-237행: 8슬롯·모드) · §5.2(239-258행: env 표·CronJob 값·모드×게이트 표) · §5.3(260-266행: 게이트 사유 어휘) · §5.4(268-274행: 적재 순서·부분 적재 복구·뮤테이션 가드 45) · §5.5(276-280행: manual-v0 P0 = k8s Job, "워크스테이션 직접 실행(`CH_*` env + port-forward)은 대안으로 README에만 기재") · §5.6(282-289행: 배포·rerun) · §6.3(312-314행: rerun 창 10:50 KST + 활성 `token-mart-metrics` Job 0, `--chunk-days 7`, `--chain-mart`, "collectors rerun/manual 후 동일 날짜 mart-metrics rerun 의무") · §4.0(119-128행: 뮤테이션 장부 — 정본은 `ddl/README.md`, README는 링크 + 실측 쿼리만) · §7.3(354행: 운영 문서 `docs/operations/token-metrics-deploy.md`는 Plan 6c 몫 — README는 그 경로를 "예정"으로 링크하지 않고 자기완결로 쓴다) · §7.2(공개 레포 — 사내 주소는 `harbor.example.internal`·`chi-<cluster>.<ns>.svc` 플레이스홀더, 코드명·담당자 메일 0).

**Interfaces:**
- Consumes:
  - T2 `app/config.py`: env `CH_HOST CH_PORT CH_USER CH_PASSWORD CH_CLUSTER ENDPOINTS_FILE SOFT_DEADLINE_MINUTES LOAD_BUDGET_S FINAL_HOUR_KST MAX_RESPONSE_BYTES METRICS_MAX_MUTATIONS_PER_RUN COLLECTOR_HTTPS_PROXY COLLECTOR_API_VERIFY COLLECTOR_API_CA_BUNDLE`(기본 `localhost 8123 default "" "" endpoints.yaml 40 1200 9 5000000 45 None true None`), 불변식 `SOFT_DEADLINE_MINUTES*60 > LOAD_BUDGET_S`, endpoints 키 `serviceGroup service baseUrl enabled apiSince(기본 2026-09-09) coverageSince(기본 2026-08-26) until expectGpu expectServing usageIncludesConsumers note`; `requirements-dev.txt`.
  - T5 `app/writer.py`: `DB_FACT = os.getenv("CH_DB_FACT", "fact")`, `DB_DIM = os.getenv("CH_DB_DIM", "gpu_data")`; 테이블 `raw_token_metrics_gpu_1d`·`raw_token_metrics_serving_1d`·`raw_token_metrics_summary_1d`·`collect_audit_metrics_1d`·`dim_token_metrics_service`; `replace_batch` = DELETE 3건(summary→gpu→serving)/(date, 배치) → `MutationBudgetExceeded`.
  - T6 `app/main.py`: `python -m app.main [batch_time_iso]`, `--from D0 --to D1 [--service S] [--replace]`; 마커 3형식 + `note=sigterm`; 사유 어휘 `disabled before_since after_until already_loaded not_ready not_ready_at_0900 retention mutation_budget load_budget deadline unknown_service invariant_broken unexpected:<Type>`; `CHECK WARN service=- registry_sync_failed=1`; 종료 코드 `0/1/2`.
  - T7 `app/manual.py`: `--manual-gpu --manual-serving [--manual-engine] --from --to [--service] [--replace] [--generated-at]`; 헤더 상수 3종; 마커 `MANUAL_INPUT module=token-metrics rows_gpu=<n> rows_serving=<n> rows_engine=<n> rows_outside_range=<n> rows_other_service=<n>`; 규칙(등록·enabled 서비스만, 날짜 제약 없음, `(model, metric)` 중복 오류, `custom`은 name/unit 필수, `#` 주석, BOM 허용).
  - T8 `build.sh [--registry R] [--tag T] <stage|company>`, `install.sh [--registry R] [--tag T] [--context C] [--namespace N] [--endpoints F] <stage|company|company-verify>` 단계 [1/7]~[7/7]; CronJob `token-metrics-collector`(`5 2-9 * * *`, `Asia/Seoul`, `Forbid`, 540/3000/0, 256Mi→1Gi); Secret `token-metrics-ch-secret[-verify]`, ConfigMap `token-metrics-endpoints[-verify]`(키 `endpoints.yaml`)·`token-metrics-ca-bundle`; 볼륨 `/etc/token-metrics`·`/etc/token-metrics-ca`.
  - T9 `tools/rerun.py`: CLI 전문, 상수 `WINDOW_OPEN_HHMM=(10,50)`·`MART_CRONJOB="token-mart-metrics"`·`MART_RERUN="mart/token-metrics/tools/rerun.py"`·`TIMEOUT_SINGLE_S=3600`, Job 이름 `token-metrics-collector-rerun-<epoch>-<i>`(라벨 `app=token-metrics-collector,rerun=1`), 메시지 `[ERROR] 실행 창 밖: …`(exit 3)·`[ERROR] 청크 {i}/{n} 실패 — 이후 청크 중단; 재시도: --from {chunk_from} --to {to}`(exit 1)·`[NEXT] collectors rerun 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3):`·`[ERROR] --chain-mart: mart/token-metrics/tools/rerun.py 가 아직 없습니다 (Plan 6c 전)`.
  - T10 `tools/manual_load.py`: CLI 전문, ConfigMap `token-metrics-manual-<YYYYMMDDHHMMSS>`(라벨 `app=token-metrics-collector,manual=1`), Job `token-metrics-collector-manual-<ts>`(볼륨 `/manual`), `MAX_CONFIGMAP_BYTES=900_000`, `[INFO] configmap=… job=… files=… bytes=…`, `[NEXT] manual 적재 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3): …`; 종료 코드 `0/1/2`.
  - T11 e2e: 포트 CH `18125`·mock `18001`, `docker build -t token-mock-provider:e2e tools/mock-provider`, `tests/e2e/run_e2e.sh`, `tests/test_manifests.py`(T8), 워크플로 `test-collector-metrics.yml`·`release-images-metrics.yml`(sha7 태그).
  - T1 mock: `POST /__mock/scenario`(`metrics_gpu_hours_over` 등 6종 int 플래그, `not_ready_until_uptime_s`, `retry_after_s`), `POST /__mock/reset`.
  - Plan 6a: `collectors/token-metrics/ddl/{company,stage,company-verify}/{raw_token_metrics.sql,dim_token_metrics_service.sql}`, `ddl/<env>/accounts.sql`, `ddl/README.md`(장부 정본), 템플릿 `docs/templates/token_metrics_manual_v0_{gpu,serving,engine}.csv`, `.gitignore` 패턴 `*manual_metrics*.csv`·`endpoints-metrics.company.yaml`.
- Produces(Plan 6c 운영 문서·대시보드·mart-metrics README가 링크하는 절 앵커 — 문자열 그대로):
  - `## 실행`
  - `## 모드와 게이트`
  - `## 환경변수 (§5.2)`
  - `## 배포 (§5.6)`
  - `## 수기(manual-v0) 적재 (§5.5)`
  - `## 재수행 (§6.3)`
  - `## 부분 적재 복구 (§5.4)`
  - `## 마커 (§5.6)`
  - `## 검증`
  - `## DDL·뮤테이션 장부 (§4.0)`

> **설계 해석(T12):** (a) 수기 CSV의 gitignore 패턴은 Plan 6a G·T10·T11이 확정한 `*manual_metrics*.csv`를 따른다(outline T12 5단계의 `*.manual.csv`는 6a 이전 표기 — README 예시 파일명은 `gpu_manual_metrics.csv`·`serving_manual_metrics.csv`·`engine_manual_metrics.csv`로 패턴에 걸리게 짓는다). (b) 뮤테이션 산식은 T5 `replace_batch` 계약(호출당 DELETE 3건 = 날짜·배치당 3)에 맞춰 두 줄로 적는다 — `--replace`/manual 배치 rerun은 "날짜당 ≤3 → 45/3 = 15일/실행(`--chunk-days 7`이면 21 ≤ 45)", 정기 실행의 부분 적재 복구는 "(date, service)쌍당 3 → 15쌍/실행". (c) 워크스테이션 대안의 port-forward 대상은 `svc/<chi-headless>`(클러스터별 이름이 달라 플레이스홀더) — install.sh [7/7]가 산출하는 `CH_HOST=<ch_pod 접두>.clickhouse.svc`와 같은 서비스다.

- [ ] **Step 1: 전제 확인 — T2~T11 산출물·Plan 6a DDL/템플릿이 있고 README는 아직 없는지(없으면 중단·보고)**

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
ls app/config.py app/main.py app/manual.py app/writer.py tools/rerun.py tools/manual_load.py \
   install.sh build.sh k8s/base/cronjob.yaml ddl/README.md tests/e2e/run_e2e.sh tests/test_manifests.py \
   ddl/company/raw_token_metrics.sql ddl/company/dim_token_metrics_service.sql ddl/company/accounts.sql
ls ../../docs/templates/token_metrics_manual_v0_gpu.csv ../../docs/templates/token_metrics_manual_v0_serving.csv \
   ../../docs/templates/token_metrics_manual_v0_engine.csv
grep -n "manual_metrics\|endpoints-metrics.company.yaml" ../../.gitignore
grep -c "^## " ddl/README.md
test ! -e README.md && echo "README absent (ok)"
python3 -m app.main --help | grep -c -- "--manual-gpu\|--manual-serving\|--manual-engine\|--generated-at\|--from\|--to\|--service\|--replace"
python3 tools/rerun.py --help | grep -c -- "--chunk-days\|--chain-mart\|--force-window"
python3 tools/manual_load.py --help | grep -c -- "--gpu\|--serving\|--engine\|--keep-configmap\|--timeout-s"
```

Expected: `ls` 두 번 모두 오류 없이 경로만 출력(15개 + 3개); `.gitignore`에 `*manual_metrics*.csv`·`endpoints-metrics.company.yaml` 각 1행; `ddl/README.md`의 `## ` 절 수 `6`(Plan 6a T3 — 파일 표/장부 표/확정된 결정/환경 방침/적용 순서/이 초안에 없는 것); `README absent (ok)`; `--help` grep 카운트가 각각 `8`, `3`, `5` 이상(argparse가 플래그를 여러 줄에 걸쳐 찍으면 더 커도 된다 — 0이면 중단). 하나라도 비면 중단하고 보고한다(선행 태스크·Plan 6a 산출물을 대신 만들지 않는다).

- [ ] **Step 2: README 생성 — 머리말 + `## 실행`(정기·batch_time 명시·rerun·manual 1줄·종료 코드)**

`collectors/token-metrics/README.md`를 아래 내용으로 **새로** 만든다(코드 블록은 README 안에서 4칸 들여쓰기 — 이 계획서의 펜스와 충돌하지 않게 한다). 명령의 플래그는 T6/T7 `app/main.py` argparse와 글자 단위로 같다.

```markdown
# collectors/token-metrics

`GET /v1/metrics?date=YYYY-MM-DD`(GPU 시간·서빙 성능 백분위·엔진 정보)를 서비스별·일별로 수집해 ClickHouse `fact.raw_token_metrics_{gpu,serving,summary}_1d`에 적재하는 수집기. 클론 원본은 `collectors/token-usage`(절 구성·배포 골격이 같다)이지만 별도 모듈이다 — 이미지 `token-metrics-collector`, CronJob `token-metrics-collector`, 마커 `module=token-metrics`, 레지스트리 `gpu_data.dim_token_metrics_service`. VM push 없음 — 메트릭 지표는 mart-metrics가 만든다.

절: 실행 / 모드와 게이트 / 환경변수 / 배포 / 수기(manual-v0) 적재 / 재수행 / 부분 적재 복구 / 마커 / 검증 / DDL·뮤테이션 장부.

## 실행

로컬 준비(단위 테스트·수동 실행 공통):

    cd collectors/token-metrics
    pip install -r requirements-dev.txt

정기(regular) 모드 — 인자 없이 실행하면 batch_time = 지금(KST), 대상 날짜 = batch_time − 1일, 슬롯 = batch_time의 KST 시각(`slot=HH`), 최종 슬롯 여부 = 시각 ≥ `FINAL_HOUR_KST`(9):

    CH_HOST=127.0.0.1 CH_USER=default CH_PASSWORD= ENDPOINTS_FILE=endpoints.yaml python -m app.main

batch_time을 명시(정기 슬롯 재현 — 2026-09-10 데이터를 02시 슬롯으로; naive 입력은 KST로 해석):

    python -m app.main 2026-09-11T02:05:00+09:00

rerun 모드 — 날짜 범위(`--from`·`--to` 둘 다 필수, D0 ≤ D1). 앵커(summary 행)가 있는 (날짜, 서비스)는 `--replace` 없이는 `SKIPPED reason=already_loaded`, 있으면 DELETE×3 후 재적재:

    python -m app.main --from 2026-09-01 --to 2026-09-07
    python -m app.main --from 2026-09-01 --to 2026-09-07 --service "Mock Service A" --replace

manual-v0 모드 — CSV(gpu·serving 필수, engine 선택)를 API 대신 입력으로 쓴다. 전문·규칙은 `수기(manual-v0) 적재` 절:

    python -m app.main --manual-gpu /manual/gpu.csv --manual-serving /manual/serving.csv --manual-engine /manual/engine.csv \
      --from 2026-08-26 --to 2026-08-31 --generated-at 2026-09-01T09:00:00+09:00

종료 코드: `0` = FAILURE 서비스 없음(SKIPPED·NODATA 포함) / `1` = 서비스 하나라도 FAILURE / `2` = 인자·설정 오류(`--from`·`--to` 짝 누락, D0 > D1, `--replace`를 범위 없이 사용, `unknown service: <S>`, manual 파일 짝 누락, `config error: <Type>: <msg>`). `2`는 BATCH_RESULT 없이 끝난다.
```

검증:

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
grep -c "^## " README.md
grep -c "VM push 없음 — 메트릭 지표는 mart-metrics가 만든다" README.md
grep -c "^    " README.md
```

Expected: `1` / `1` / `8`(들여쓴 코드 줄 8개 — `cd`, `pip`, 정기 1, batch_time 1, rerun 2, manual 2; manual의 둘째 줄은 6칸 들여쓰기라 4칸 시작 grep에도 걸린다).

- [ ] **Step 3: `## 모드와 게이트` 추가 — 모드×게이트 표 + 사유 어휘 14종 + `unexpected:<Type>`**

`collectors/token-metrics/README.md` 끝에 아래를 이어 붙인다(T6 `_gate`/`_outcome_from_error`/`_load_items` 계약을 표로 옮긴 것 — 사유 문자열은 `app/main.py` 상수와 글자 단위로 같다).

```markdown
## 모드와 게이트

모드는 CLI 인자로 정해진다 — 인자 없음/`batch_time` = 정기(regular), `--from/--to` = rerun, `--manual-*` = manual. 게이트(`api_since`·`until`)와 최종 슬롯 판정은 **정기 모드에만** 적용되고, 레지스트리 동기화(`gpu_data.dim_token_metrics_service` diff-sync)도 정기 모드에서만 한다. `enabled: false` 서비스는 모든 모드에서 `SKIPPED reason=disabled`.

| 모드 | 트리거 | 대상 날짜 | 앵커 존재 시 | 409 2회째 | 레지스트리 동기화 | 최종 슬롯 |
|---|---|---|---|---|---|---|
| 정기(regular) | CronJob `5 2-9 * * *`(KST 8슬롯) 또는 `python -m app.main [batch_time]` | batch_time − 1일 | `SKIPPED already_loaded`(뮤테이션 0; 앵커가 manual-v0면 `CHECK WARN manual_row_present=1`) | 비최종 슬롯 `SKIPPED not_ready` / 최종 슬롯(09시) `FAILURE not_ready_at_0900` | 함 | slot ≥ `FINAL_HOUR_KST`(09) → `final=1` |
| rerun | `tools/rerun.py` 또는 `--from D0 --to D1 [--service S] [--replace]` | 범위(D0..D1) | `--replace` 없으면 `SKIPPED already_loaded`, 있으면 DELETE×3(summary→gpu→serving) 후 재적재 | `FAILURE not_ready` | 안 함 | `final=0` |
| manual | `tools/manual_load.py` 또는 `--manual-gpu … --manual-serving … --from --to` | 범위(D0..D1) | rerun과 동일 | 해당 없음(API 호출 없음) | 안 함 | `final=0` |

404(보존 기간 밖)는 정기 모드 `FAILURE retention`, rerun `SKIPPED retention`. 409는 큐 끝에서 `min(max(Retry-After, 1), 300)`초 뒤 1회만 재방문한다. rerun·manual은 전 서비스 fetch/normalize를 마친 뒤 `replace_batch` 1회로 적재하고, 정기 모드는 서비스별 순차 적재다.

게이트·실패 사유(`SERVICE_RESULT … reason=<r>` 어휘 — 순서대로):

| reason | 상태 | 뜻 |
|---|---|---|
| `disabled` | SKIPPED | endpoints의 `enabled: false` — 모든 모드 |
| `before_since` | SKIPPED | 정기 모드에서 `target_date < apiSince`(`apiSince` 기본 `2026-09-09`) |
| `after_until` | SKIPPED | 정기 모드에서 `until < target_date`(`until`이 있는 서비스만) |
| `already_loaded` | SKIPPED | 앵커(summary 행) 존재 & `--replace` 없음 — 뮤테이션 0 |
| `not_ready` | SKIPPED(정기 비최종) / FAILURE(rerun) | 409 `data_not_ready` 2회째 |
| `not_ready_at_0900` | FAILURE | 정기 최종 슬롯(09시)에서 409 2회째 — 이 줄이 09:00 알림의 근거 |
| `retention` | FAILURE(정기) / SKIPPED(rerun) | 404 — 제공자 보존 기간 밖 |
| `retryable` | FAILURE | 429/5xx/네트워크 오류 — 내부 재시도 3회(지수 백오프) 소진 후에도 실패 |
| `permanent_error` | FAILURE | 400, 응답 본문 > `METRICS_MAX_RESPONSE_BYTES`(5MB), `date` 에코 불일치, non-JSON, 보고서 구조 위반(`gpu`/`serving`이 배열 아님 등) — 재시도 없이 즉시 |
| `mutation_budget` | FAILURE | 예정 DELETE 합산이 `METRICS_MAX_MUTATIONS_PER_RUN`(45) 초과 — 적재 착수 전 차단, BATCH_RESULT에도 `reason=mutation_budget` |
| `load_budget` | FAILURE | 적재 착수 시점에 남은 시간이 `LOAD_BUDGET_S`(1200) 미만 — writer 호출 없음 |
| `deadline` | FAILURE | 큐 처리 중 `SOFT_DEADLINE_MINUTES`(40) 도달 — 남은 서비스 전부 |
| `unknown_service` | (exit 2) | `--service`가 endpoints에 없음 — stderr `unknown service: <S>`, SERVICE_RESULT 없이 종료 |
| `invariant_broken` | FAILURE | 정규화 불변식 위반(제공자 응답 구조 오류 등) |
| `unexpected:<Type>` | FAILURE | fetch·normalize·writer의 예상 밖 예외 — `<Type>`은 파이썬 예외 클래스명 |

`rows == 0`이고 `rejected == 0`이면 `NODATA`, `rows == 0`이고 `rejected > 0`이면 `SUCCESS rows=0 rejected=<n>` + `CHECK WARN … all_rows_rejected=1`.
```

검증:

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
grep -c "^## " README.md
grep -o '^| `[a-z0-9_:<>A-Z]*` |' README.md | wc -l
grep -n "^| 정기(regular)\|^| rerun \|^| manual " README.md | wc -l
```

Expected: `2` / `15`(사유 14종 + `unexpected:<Type>`) / `3`.

- [ ] **Step 4: `## 환경변수 (§5.2)` 추가 — env 표 16행 + 불변식 + endpoints 키**

`collectors/token-metrics/README.md` 끝에 이어 붙인다(T2 `load_config()`·T5 `DB_FACT/DB_DIM`이 읽는 이름 전부 — 그 외 env는 앱이 읽지 않는다).

```markdown
## 환경변수 (§5.2)

앱(`app/config.py`·`app/writer.py`)이 읽는 env 전부. 값의 출처: CronJob 리터럴(`k8s/base/cronjob.yaml`) / Secret `token-metrics-ch-secret[-verify]`(install.sh [2/7]) / install.sh [7/7] `set env`.

| 변수 | 기본값 | 출처 | 뜻 |
|---|---|---|---|
| `CH_HOST` | `localhost` | install.sh [7/7] `set env`(`<ch_pod 접두>.clickhouse.svc`) | ClickHouse HTTP 호스트 |
| `CH_PORT` | `8123` | Secret | ClickHouse HTTP 포트 |
| `CH_USER` | `default` | Secret(프롬프트 기본 `mart`; company-verify `token_verify`) | 적재 계정 — GRANT는 `ddl/<env>/accounts.sql` |
| `CH_PASSWORD` | `""` | Secret | 적재 계정 비밀번호 |
| `CH_CLUSTER` | `""` | Secret(company·company-verify `gpu-monitoring`, stage 빈 값) | 비어 있지 않으면 `ALTER … DELETE`에 `ON CLUSTER` 부착 |
| `CH_DB_FACT` | `fact` | Secret — **company-verify 전용**(`token_verify_fact`) | fact 4테이블의 DB |
| `CH_DB_DIM` | `gpu_data` | Secret — **company-verify 전용**(`token_verify_dim`) | 레지스트리·프리플라이트 DB |
| `ENDPOINTS_FILE` | `endpoints.yaml` | CronJob 리터럴 `/etc/token-metrics/endpoints.yaml` | 서비스 목록(ConfigMap `token-metrics-endpoints[-verify]` 마운트) |
| `SOFT_DEADLINE_MINUTES` | `40` | CronJob 리터럴 | 잡 소프트 데드라인 — 초과 시 남은 서비스 `FAILURE deadline` |
| `LOAD_BUDGET_S` | `1200` | CronJob 리터럴 | 적재 착수에 필요한 잔여 시간 — 부족하면 `FAILURE load_budget`. 불변식 `SOFT_DEADLINE_MINUTES*60 > LOAD_BUDGET_S`(위반 시 `config error: ValueError: SOFT_DEADLINE_MINUTES*60 must exceed LOAD_BUDGET_S`, exit 2) |
| `FINAL_HOUR_KST` | `9` | CronJob 리터럴 | 정기 모드 최종 슬롯 시각 — batch_time KST 시각 ≥ 이 값이면 `final=1` |
| `MAX_RESPONSE_BYTES` | `5000000` | CronJob 리터럴 | `/v1/metrics` 응답 상한(초과 = `FAILURE`) |
| `METRICS_MAX_MUTATIONS_PER_RUN` | `45` | CronJob 리터럴 | 실행당 예정 DELETE 상한(§4.0 = 3 × 15) — 초과면 적재 전 `FAILURE mutation_budget` |
| `COLLECTOR_HTTPS_PROXY` | 미설정 | Secret(install.sh [2/7] 프롬프트: `none` → 빈 값, enter → 키 없음, 값) | 미설정 = 시스템 프록시 상속 / `""` = 직접 연결 / 값 = 전용 프록시(제공자 API 호출에만 적용, ClickHouse에는 미적용) |
| `COLLECTOR_API_VERIFY` | `true` | 수동(Secret에 직접 넣을 때만) | `false`면 제공자 TLS 검증 끔 — stage 자체서명 실험용, company 금지 |
| `COLLECTOR_API_CA_BUNDLE` | 미설정 | Secret(CA 파일 입력 시 `/etc/token-metrics-ca/ca-bundle.pem`) + ConfigMap `token-metrics-ca-bundle` | 사내 CA 번들 경로 — 있으면 `verify=<경로>` |

`VM_PUSH_URL`은 없다 — VM push 없음, 메트릭 지표는 mart-metrics가 만든다. `CH_DB_FACT`/`CH_DB_DIM`은 격리 검증(company-verify) 전용이며 stage/company Secret에는 키 자체가 없다(앱 기본값 사용).

endpoints 파일 키(`services:` 목록 원소): `serviceGroup`, `service`, `baseUrl`, `enabled`, `apiSince`(기본 `2026-09-09` — 이 날짜 전은 정기 모드 `before_since`), `coverageSince`(기본 `2026-08-26` — 수기 적재 시작일, 레지스트리 컬럼), `until`(선택 — 지나면 `after_until`), `expectGpu`, `expectServing`, `usageIncludesConsumers`, `note`. 정본: stage `endpoints.yaml`(커밋), company `endpoints-metrics.company.yaml`(gitignore).
```

검증:

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
grep -c "^## " README.md
grep -c '^| `[A-Z_]*` |' README.md
for v in $(grep -o 'os.getenv("[A-Z_]*"\|os.environ.get("[A-Z_]*"\|_int_env("[A-Z_]*"' app/config.py app/writer.py | grep -o '"[A-Z_]*"' | tr -d '"' | sort -u); do grep -q "\`$v\`" README.md || echo "missing in README: $v"; done
```

Expected: `3` / `16` / 출력 없음(앱이 읽는 env 이름이 표에 전부 있다).

- [ ] **Step 5: `## 배포 (§5.6)` 추가 — build.sh/install.sh 명령·[1/7]~[7/7] 요약·CronJob 값·endpoints 정본·apply -k 주의·sha7 태그**

`collectors/token-metrics/README.md` 끝에 이어 붙인다(T8 `build.sh`/`install.sh` 머리말 주석·`k8s/base/cronjob.yaml` 값과 동일).

```markdown
## 배포 (§5.6)

이미지 빌드+푸시(항상 둘 다; 태그 기본 = git short SHA; stage 레지스트리 기본 `ghcr.io/yoonsungnam`, company는 `--registry` 필수):

    ./collectors/token-metrics/build.sh stage
    ./collectors/token-metrics/build.sh --registry harbor.example.internal/gpu-monitoring --tag <sha7> company

설치(대화형 — Secret 값 프롬프트). company-verify는 별도 이미지가 없다(company 이미지를 그대로 사용):

    ./collectors/token-metrics/install.sh stage
    ./collectors/token-metrics/install.sh --registry harbor.example.internal/gpu-monitoring --tag <sha7> --context <ctx> company
    ./collectors/token-metrics/install.sh --registry harbor.example.internal/gpu-monitoring --tag <sha7> --context <ctx> company-verify

install.sh 7단계(§5.6):

| 단계 | 내용 |
|---|---|
| [1/7] | `registry-pull-secret` — **없을 때만** 생성. 있으면 `이미 존재합니다 — 네임스페이스 공유 Secret이므로 갱신하지 않습니다`(기존 수집기와 공유, 프롬프트 없음) |
| [2/7] | `token-metrics-ch-secret[-verify]` 멱등 생성 — `CH_USER`(기본 `mart` / verify `token_verify`), `CH_PASSWORD`, `CH_PORT=8123`, `CH_CLUSTER`(`gpu-monitoring` / stage 빈 값), company-verify만 `CH_DB_FACT`/`CH_DB_DIM`, `COLLECTOR_HTTPS_PROXY`(`none`/enter/값), CA 파일 입력 시 `COLLECTOR_API_CA_BUNDLE` + ConfigMap `token-metrics-ca-bundle` |
| [3/7] | ConfigMap `token-metrics-endpoints[-verify]`(키 `endpoints.yaml`) — 원본 stage `endpoints.yaml`, company `endpoints-metrics.company.yaml`(gitignore), `--endpoints F`로 대체 가능 |
| [4/7] | 프리플라이트 2항목 — **앱 계정(`CH_USER`/`CH_PASSWORD`)으로** 실행해 GRANT까지 검증: `SELECT name FROM system.databases WHERE name IN ('fact','gpu_data')`(verify는 `token_verify_*`)가 2행이 아니면 `[ERROR] 프리플라이트 실패: DB 부재 또는 GRANT 누락 — admin이 ddl/<env>/accounts.sql 실행 필요` exit 1; `SELECT count() FROM gpu_data.dim_token_service_dist`(토큰 레지스트리) 실패면 `[ERROR] 프리플라이트 실패: 토큰 레지스트리 SELECT 불가(GRANT 누락) — admin이 ddl/<env>/accounts.sql 실행 필요` exit 1. [2/7]을 건너뛴 재설치는 기존 Secret의 계정을 읽어 쓴다 |
| [5/7] | DDL 2파일 적용 — `ddl/<env>/raw_token_metrics.sql` + `ddl/<env>/dim_token_metrics_service.sql`. `ddl/<env>/accounts.sql`(GRANT)은 **admin이 수동 실행**(§4.0) |
| [6/7] | `kubectl apply -k k8s/overlays/<env> -n monitoring` |
| [7/7] | `kubectl set image cronjob/token-metrics-collector token-metrics-collector=<REGISTRY>/token-metrics-collector:<TAG>` + `kubectl set env cronjob/token-metrics-collector CH_HOST=<ch_pod 접두>.clickhouse.svc` + 수동 테스트 명령 안내 |

CronJob `token-metrics-collector`(company-verify는 `token-metrics-collector-verify`) 값 — `k8s/base/cronjob.yaml`:

| 항목 | 값 |
|---|---|
| schedule | `5 2-9 * * *`, `timeZone: Asia/Seoul`(02:05~09:05 KST 8슬롯 — 09시 슬롯이 최종 `final=1`) |
| concurrencyPolicy / startingDeadlineSeconds | `Forbid` / `540` |
| activeDeadlineSeconds / backoffLimit / restartPolicy | `3000` / `0` / `Never`(재시도 없음 — 슬롯 실패는 다음 슬롯이 받는다) |
| history | successful 3 / failed 3 |
| resources | requests `100m`/`256Mi`, limits `1`/`1Gi` |
| env | `envFrom` Secret + 리터럴 `ENDPOINTS_FILE=/etc/token-metrics/endpoints.yaml`, `SOFT_DEADLINE_MINUTES=40`, `LOAD_BUDGET_S=1200`, `FINAL_HOUR_KST=9`, `MAX_RESPONSE_BYTES=5000000`, `METRICS_MAX_MUTATIONS_PER_RUN=45` |
| volumes | `[0] endpoints` → `/etc/token-metrics`, `[1] ca-bundle`(optional) → `/etc/token-metrics-ca`; manual Job은 `[2] manual` → `/manual`을 추가한다 |
| label | `app: token-metrics-collector` |

주의: install.sh 밖에서 `kubectl apply -k`를 직접 재실행하면 이미지가 `latest`로 리셋된다 — 재적용은 항상 install.sh 경유(`[7/7]`가 `set image`로 다시 덮는다). 이미지 태그는 `.github/workflows/release-images-metrics.yml`이 만드는 sha7(`main` 푸시마다 `ghcr.io/yoonsungnam/token-metrics-collector:<sha7>`); company는 같은 sha7로 `build.sh --registry … company`가 사내 레지스트리에 푸시한다. `k8s/overlays/company`에는 사내 주소를 두지 않는다(§7.2).
```

검증:

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
grep -c "^## " README.md
grep -c "^| \[[1-7]/7\] |" README.md
for k in "5 2-9 \* \* \*" "startingDeadlineSeconds" "activeDeadlineSeconds" "backoffLimit" "concurrencyPolicy" "METRICS_MAX_MUTATIONS_PER_RUN" "token-metrics-ca-bundle"; do grep -q "$k" k8s/base/cronjob.yaml || echo "not in cronjob.yaml: $k"; done
grep -n "harbor\." README.md | grep -v "harbor\.example\.internal"
```

Expected: `4` / `7` / 출력 없음(README가 적은 계약 문자열이 실제 매니페스트에 있다) / 출력 없음(사내 주소 없음).

- [ ] **Step 6: `## 수기(manual-v0) 적재 (§5.5)` 추가 — 템플릿 3파일·헤더 원문·규칙·P0 k8s Job 절차·900KB·gitignore·워크스테이션 대안·mart rerun 의무·MANUAL_INPUT**

`collectors/token-metrics/README.md` 끝에 이어 붙인다(T7 파서 규칙·T10 `manual_load.py` 출력 문구·Plan 6a 템플릿 헤더와 동일).

```markdown
## 수기(manual-v0) 적재 (§5.5)

go-live(`apiSince`, 기본 2026-09-09) 이전 구간(`coverageSince`, 기본 2026-08-26 이후)은 서비스 담당자가 CSV로 제출하고 운영자가 적재한다. API 경로와 **같은 normalize·replace 경로**를 타며 `source_type=manual-v0`로 앵커가 남는다.

템플릿(주석·예시 행 포함 — 예시 행은 삭제 후 실값으로 교체):

    docs/templates/token_metrics_manual_v0_gpu.csv
    docs/templates/token_metrics_manual_v0_serving.csv
    docs/templates/token_metrics_manual_v0_engine.csv

헤더 3줄(바이트 동일 요구 — 첫 비주석 줄):

    date,service,model,gpuType,category,gpuCount,gpuHours
    date,service,model,metric,name,unit,p50,p90,p95,p99
    service,engine_type,engine_version

규칙(파서 `app/manual.py` — 위반은 `manual input error: <경로>:<줄>: <필드>`로 exit 2, 적재 없음):
- `service`는 endpoints에 등록되고 `enabled: true`인 서비스만(`apiSince`는 무시 — **날짜 제약 없음**). 미등록 `--service`는 `unknown service: <S>`.
- 같은 (date, service, model, metric)의 serving 행 중복은 오류(`(model, metric)` 중복 금지). `metric`은 API 키 그대로 `ttftMs | itlMs | outputTps | e2eMs | custom`(fact 표기 `ttft_ms` 등으로의 변환은 normalize가 한다).
- `custom`은 `name`·`unit` 필수, 표준 지표 행은 둘 다 빈 값. `outputTps`는 `p50`만, 나머지 표준 지표는 `p50..p99` 4개 모두.
- `#`로 시작하는 줄은 주석(안의 쉼표 무시), UTF-8 BOM 허용(`utf-8-sig`), 빈 셀 = 부재. 숫자 검증(형·범위·행 거부)과 플래그(`gpuHours > gpuCount × 24` → `hours_over_count`, `p50 ≤ p90 ≤ p95 ≤ p99` 역전 → `pct_non_monotone` — 적재하되 `flags`에 표기)는 normalize 한 곳에서만 — 파서는 형태만 만든다.
- `--from/--to` 밖의 날짜 행은 `rows_outside_range`, `--service` 지정 시 다른 서비스 행은 `rows_other_service`로 세고 버린다(오류 아님). 행이 하나도 없는 (date, service)는 **페이로드를 만들지 않는다** — 앵커가 남지 않으며(`NODATA` 앵커 아님) mart 불변식 `metrics_missing`이 그 날을 "수기 입력 없음"으로 본다; `--from/--to` 범위 안이라도 CSV에 행이 없으면 그 (date, service)에는 아무것도 적재되지 않는다(제출 누락과 실제 0행을 CSV로는 구분할 수 없으므로 완결 표시를 심지 않는다).
- 기존 앵커(API·manual 불문)가 있으면 `--replace` 없이는 `SKIPPED already_loaded`. 레지스트리 동기화는 하지 않는다.
- **`--replace` 실행 시각**: `tools/manual_load.py`는 실행 창(10:50 KST)·활성 mart Job을 검사하지 않는다(`tools/rerun.py`와 달리). `--replace`는 (date, service)마다 DELETE×3 + INSERT를 내므로 mart-metrics 10:20 배치(activeDeadlineSeconds 1800 → 늦어도 10:50 종료)와 겹치지 않게 **10:50 KST 이후·활성 `token-mart-metrics` Job 0일 때** 실행한다 — 운영자 확인: `kubectl get jobs -n monitoring | grep token-mart-`. 앵커 없는 첫 적재(`--replace` 없음)는 INSERT뿐이라 시각 제약이 없다.

P0 경로 — k8s Job(운영자 워크스테이션에는 kubectl만 있으면 된다; ClickHouse 접근·프록시·CA 불필요). 실제 제출 파일은 `*manual_metrics*.csv` 이름으로 저장한다(`.gitignore` — 레포 반입 금지):

    python3 collectors/token-metrics/tools/manual_load.py --context <ctx> --namespace monitoring \
      --from 2026-08-26 --to 2026-08-31 \
      --gpu gpu_manual_metrics.csv --serving serving_manual_metrics.csv --engine engine_manual_metrics.csv \
      --generated-at 2026-09-01T09:00:00+09:00 --replace

흐름: CSV 3파일 → ConfigMap `token-metrics-manual-<YYYYMMDDHHMMSS>`(`kubectl create`) → CronJob `token-metrics-collector` 템플릿에서 Job `token-metrics-collector-manual-<ts>` 생성(`/manual` 볼륨 마운트, command `python -m app.main --manual-gpu /manual/gpu.csv --manual-serving /manual/serving.csv [--manual-engine /manual/engine.csv] --from D0 --to D1 [--service S] [--replace] [--generated-at ISO]`) → 로그 스트리밍(`--timeout-s` 기본 3600) → 종료 시 ConfigMap 삭제(`--keep-configmap`이면 보존), Job 오브젝트는 로그 재조회용으로 남긴다. 시작 시 `[INFO] configmap=<name> job=<job> files=gpu.csv,serving.csv[,engine.csv] bytes=<n>`, 성공 시 `[NEXT] manual 적재 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3): python3 mart/token-metrics/tools/rerun.py --context <ctx> --namespace monitoring --from D0 --to D1`. CSV 합계는 900,000 bytes(ConfigMap 상한) 이하 — 초과면 `[ERROR] CSV 합계 <n> bytes > 900000 — 날짜 범위를 나눠 제출` exit 2. 종료 코드 `0`(적재 성공) / `1`(Job 실패·타임아웃) / `2`(인자·파일·크기 오류 — kubectl 호출 전).

Job 정리:

    kubectl --context <ctx> -n monitoring delete job -l app=token-metrics-collector,manual=1

워크스테이션 직접 실행(대안 — ClickHouse에 직접 붙는다; 제공자 API 호출은 없으므로 프록시·CA는 불필요):

    kubectl --context <ctx> -n clickhouse port-forward svc/<chi-headless> 8123:8123
    cd collectors/token-metrics
    CH_HOST=127.0.0.1 CH_PORT=8123 CH_USER=mart CH_PASSWORD=<비밀번호> CH_CLUSTER=gpu-monitoring \
      ENDPOINTS_FILE=endpoints-metrics.company.yaml \
      python -m app.main --manual-gpu gpu_manual_metrics.csv --manual-serving serving_manual_metrics.csv \
        --manual-engine engine_manual_metrics.csv --from 2026-08-26 --to 2026-08-31 \
        --generated-at 2026-09-01T09:00:00+09:00 --replace

`--generated-at`은 제공자 기준 산출 시각(KST `+09:00`; 다른 오프셋은 `CHECK WARN generated_at_offset_mismatch`, 파싱 실패는 `generated_at_parse_failed` — 적재는 계속). 생략하면 적재 시각.

적재 후 **같은 날짜 범위의 mart-metrics rerun은 의무**다(§6.3 — manual_load.py는 안내만 하고 체인하지 않는다; 실행 창 10:50 KST 검사는 mart rerun 자신이 한다). 로그의 `MANUAL_INPUT module=token-metrics rows_gpu=<n> rows_serving=<n> rows_engine=<n> rows_outside_range=<n> rows_other_service=<n>` 1줄(SERVICE_RESULT보다 앞)로 파서가 읽은 행 수를 확인한다 — 페이로드·행 원문은 로그에 남지 않는다.
```

검증:

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
grep -c "^## " README.md
for t in gpu serving engine; do h=$(grep -v '^#' ../../docs/templates/token_metrics_manual_v0_$t.csv | head -1); grep -q -F "    $h" README.md || echo "header mismatch: $t"; done
grep -o "[a-z]*_manual_metrics\.csv" README.md | wc -l
grep -n "\.manual\.csv" README.md
```

Expected: `5` / 출력 없음(헤더 3줄이 템플릿 첫 비주석 줄과 바이트 동일) / `6`(예시 파일명 — P0 3 + 워크스테이션 3, 전부 `*manual_metrics*.csv` 패턴) / 출력 없음(옛 패턴 없음).

- [ ] **Step 7: `## 재수행 (§6.3)` 추가 — rerun.py CLI·실행 창(exit 3)·청크=Job·실패/재시도·--chain-mart·수동 1회 트리거·뮤테이션 산식·정리**

`collectors/token-metrics/README.md` 끝에 이어 붙인다(T9 `tools/rerun.py` 상수·문구와 동일).

```markdown
## 재수행 (§6.3)

날짜 범위 재수집은 워크스테이션에서 `tools/rerun.py`로 한다(kubectl만 필요). CronJob 템플릿에서 Job을 만들어 파드 로그를 스트리밍한다:

    python3 collectors/token-metrics/tools/rerun.py --context <ctx> --namespace monitoring \
      --from 2026-09-01 --to 2026-09-20 [--service "Mock Service A"] [--replace] [--chunk-days 7] [--chain-mart] [--force-window] \
      [--cronjob token-metrics-collector-verify]

- `--from/--to`는 필수 쌍(인자 없이 "어제 1일" 모드는 없다 — 정기 8슬롯이 그 역할). `--from > --to`, 날짜 형식 오류, `--chunk-days`가 `1..15` 밖이면 exit 2(상한 15 = 뮤테이션 예산 45 ÷ 날짜당 3 — 아래 산식).
- **실행 창**: KST 10:50 이후이고 활성 `token-mart-metrics` Job(company-verify는 `token-mart-metrics-verify`)이 0일 때만 실행한다(`--chain-mart` 유무와 무관 — 수집기의 DELETE/INSERT가 mart-metrics 10:20 배치의 fact SELECT와 겹치지 않게). 밖이면 stderr `[ERROR] 실행 창 밖: <window_closed|mart_job_active> — KST 10:50 이후·활성 token-mart-metrics Job 0일 때 재시도 (--force-window로 강제)` exit 3. `--force-window`는 검사를 건너뛰고 `[WARN] 실행 창 검사 생략(--force-window)`을 찍는다. 이 검사는 짝이 되는 mart CronJob의 Job(소유 Job + `token-mart-metrics-rerun-*`)만 센다 — mart rerun 자신은 `token-mart-*` 전부(기존 token-usage mart 포함)를 세므로 아래 `--chain-mart` 단계에서 6c가 추가로 거부할 수 있다.
- **청크 = Job 1개**: 범위를 앞에서부터 `--chunk-days`(기본 7)일씩 잘라 청크마다 Job `token-metrics-collector-rerun-<epoch>-<i>`(라벨 `app=token-metrics-collector,rerun=1`, command `python -m app.main --from <c0> --to <c1> [--service S] [--replace]`)를 만든다. `activeDeadlineSeconds: 3000`은 CronJob 값을 그대로 상속하고 클라이언트 대기는 3600초. 진행 표시 `[INFO] 청크 <i>/<n>: <c0> .. <c1> → Job <name>`.
- 청크가 실패(Job Failed·타임아웃)하면 이후 청크를 만들지 않고 stderr `[ERROR] 청크 <i>/<n> 실패 — 이후 청크 중단; 재시도: --from <c0> --to <to> (그 외 인자 동일)` exit 1. 앞선 성공 청크는 `--replace` 없이 재실행해도 `already_loaded`로 스킵되므로 안내된 범위로 그대로 재시도한다.
- 앵커가 있는 (날짜, 서비스)는 `--replace` 없이는 `SKIPPED already_loaded`; `--replace`는 날짜마다 DELETE×3(summary→gpu→serving) 후 재적재하고 감사 행(`collect_audit_metrics_1d`)을 1행 남긴다. 409는 큐 끝 1회 재방문 뒤에도 409면 `FAILURE not_ready`, 404는 `SKIPPED retention`.
- **mart-metrics rerun 의무**: 전 청크 성공 시 `[NEXT] collectors rerun 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3):` + 다음 줄에 실행할 명령을 찍는다. `--chain-mart`를 주면 **청크 분할 전 전체 범위**를 `python3 mart/token-metrics/tools/rerun.py --context <ctx> --namespace monitoring [--cronjob token-mart-metrics-verify] --from <D0> --to <D1> --chunk-days <n> [--force]`에 그대로 전파해 실행한다(수집기가 스킵한 날짜 포함 — mart가 자기 판단으로 재계산; 종료 코드는 mart rerun 값). 전파 규칙: `--cronjob …-verify`(company-verify)면 mart 쪽 `--cronjob token-mart-metrics-verify`, `--force-window`면 mart 쪽 `--force`(6c는 10:50 창 검사만 생략). **6c는 활성 `token-mart-*` Job이 하나라도 있으면 `--force`와 무관하게 `RERUN REFUSED active_jobs=<n> (token-mart-* running)` exit 2로 거부한다** — 수집 청크는 이미 끝났으므로 다른 mart Job이 끝난 뒤 `[NEXT]`에 찍힌 명령을 그대로 다시 실행하면 된다(수집기를 다시 돌릴 필요 없음). 그 파일이 아직 없으면 stderr `[ERROR] --chain-mart: mart/token-metrics/tools/rerun.py 가 아직 없습니다 (Plan 6c 전) — mart-metrics 구현 후 위 명령을 실행하세요.` exit 1.
- 종료 코드: `0` 전 청크 성공(+ `--chain-mart`면 mart rerun 반환값 — 6c 거부는 `2`) / `1` Job 실패·타임아웃·mart rerun 파일 부재 / `2` 사용법 / `3` 실행 창 밖.

정기 슬롯 1회를 수동으로 재현(어제 날짜, 현재 시각을 슬롯으로 — 실행 창 검사 없음, 앵커가 있으면 `already_loaded`):

    kubectl --context <ctx> create job --from=cronjob/token-metrics-collector token-metrics-collector-manual-$(date +%s) -n monitoring

뮤테이션 산식(§4.0 — 가드 `METRICS_MAX_MUTATIONS_PER_RUN=45`는 적재 착수 전에 예정 DELETE 합산을 검사한다):
- `--replace` rerun·manual 배치: 날짜당 DELETE ≤3(서비스 수와 무관 — 한 날짜의 전 서비스를 `IN (...)`으로 한 번에 지운다) → 45/3 = **15일/실행**. `--chunk-days 7`이면 청크당 21 ≤ 45로 항상 통과하고, `--chunk-days 15`가 한 Job의 상한이다(`tools/rerun.py`가 `CHUNK_DAYS_MAX = 15`로 정적 거부 — 16 이상은 exit 2).
- 정기 실행의 부분 적재 복구(아래 절): (date, service)쌍당 3 → **15쌍/실행**.
- 초과하면 적재 없이 `SERVICE_RESULT … FAILURE reason=mutation_budget` + `BATCH_RESULT … reason=mutation_budget`(exit 1) → `--chunk-days`를 줄이거나 `--service`로 나눠 실행한다. 실측은 `system.mutations`(DDL·뮤테이션 장부 절).

rerun Job 정리(로그 재조회가 끝난 뒤):

    kubectl --context <ctx> -n monitoring delete job -l app=token-metrics-collector,rerun=1
```

검증:

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
grep -c "^## " README.md
for m in "실행 창 밖:" "이후 청크 중단; 재시도:" "collectors rerun 후 동일 날짜 mart-metrics rerun은 의무입니다" "가 아직 없습니다 (Plan 6c 전)" "실행 창 검사 생략(--force-window)"; do grep -q -F "$m" tools/rerun.py && grep -q -F "$m" README.md || echo "message mismatch: $m"; done
grep -c "kubectl --context <ctx> create job --from=cronjob/token-metrics-collector" README.md
```

Expected: `6` / 출력 없음(README의 5개 문구가 `tools/rerun.py`에 그대로 있다) / `1`.

- [ ] **Step 8: `## 부분 적재 복구 (§5.4)` 추가 — 적재 순서 + 복구 문단(outline 문장 그대로)**

`collectors/token-metrics/README.md` 끝에 이어 붙인다(§5.4 적재 순서·T5 `replace_batch` 존재확인 3종 계약).

```markdown
## 부분 적재 복구 (§5.4)

적재 순서(크래시 안전): 존재확인 3종(summary·gpu·serving) → 감사 행 → DELETE summary→gpu→serving → INSERT gpu→serving→summary. 앵커 = summary 행(INSERT 마지막·DELETE 첫 번째)이라 "앵커 있음 = 그 (date, service)는 완결"이 성립한다.

앵커 = summary 행(INSERT 마지막·DELETE 첫 번째). 앵커 없이 gpu/serving 행만 남은 (date, service)는 '부분 적재'(이전 실행이 INSERT 도중 중단)다. 다음 실행(정기·rerun·manual 불문)은 already_loaded 게이트를 통과하고, `replace_batch`가 존재확인 3종의 합집합으로 잔여 행을 감지해 DELETE×3(summary→gpu→serving) 후 재적재한다 — 정기 실행에서 뮤테이션(3)이 생기는 유일한 경우이며, 감사 행은 앵커가 있던 세대만 남으므로 이 경우 `collect_audit_metrics_1d`에는 행이 추가되지 않는다. 로그는 `SERVICE_RESULT status=SUCCESS`이고 별도 CHECK 코드는 없다(뮤테이션 실측은 `system.mutations` — 아래 장부 절). 운영자 개입 불필요; 다만 정기 실행이 `mutation_budget`으로 실패하면 부분 적재가 15쌍 이상 누적된 것이므로 `tools/rerun.py --replace`를 `--service`로 나눠 실행한다.

복구 주체: (i) date = 오늘−1이고 남은 슬롯이 있으면 다음 정기 슬롯이, (ii) 그 외(어제가 아닌 날짜, 09시 최종 슬롯 이후)는 운영자 `tools/rerun.py --from D --to D`가 복구한다 — 부분 적재는 앵커가 없으므로 **`--replace`가 필요 없다**(잔여 행 감지가 DELETE를 한다). (iii) 제공자 보존 기간 밖(404 → `retention`)이라 API로 다시 받을 수 없는 날짜와 manual-v0로 넣었던 날짜는 `tools/manual_load.py`로 같은 CSV를 재적재한다(앵커가 없으므로 `--replace` 불필요 — 잔여 행 감지가 DELETE×3 후 재적재; CSV는 제출자가 보관한 원본을 다시 쓴다).
```

검증:

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
grep -c "^## " README.md
grep -c "정기 실행에서 뮤테이션(3)이 생기는 유일한 경우" README.md
grep -c "로 같은 CSV를 재적재한다" README.md
```

Expected: `7` / `1` / `1`.

- [ ] **Step 9: `## 마커 (§5.6)` 추가 — 세 형식 원문·SIGTERM·MANUAL_INPUT·CHECK 코드 어휘·로깅 계약**

`collectors/token-metrics/README.md` 끝에 이어 붙인다(T6 `_service_line`/`_batch_line`/`_check_lines`·T7 `MANUAL_INPUT_PREFIX`와 글자 단위로 같다).

```markdown
## 마커 (§5.6)

파드 로그 1줄 = 1마커. 알림 규칙·대시보드는 이 줄만 grep한다.

    SERVICE_RESULT status=<SUCCESS|NODATA|SKIPPED|FAILURE> module=token-metrics service=<svc> source_type=<metrics-api-v1|manual-v0> rows=<n> pages=1 warn=<n> rejected=<n>[ reason=<r>]
    BATCH_RESULT status=<SUCCESS|NODATA|FAILURE> module=token-metrics services_ok=<n> services_failed=<n> services_skipped=<n> rows=<n> elapsed=<n>s slot=<HH> final=<0|1>[ reason=<r>]
    CHECK WARN service=<svc> <code>=<count>
    MANUAL_INPUT module=token-metrics rows_gpu=<n> rows_serving=<n> rows_engine=<n> rows_outside_range=<n> rows_other_service=<n>

- `SERVICE_RESULT`는 서비스마다 1줄(`CHECK WARN` 줄들이 그 앞), `BATCH_RESULT`는 잡당 1줄(마지막). `pages=1`은 고정(`/v1/metrics`는 페이지가 없다). `reason`은 모드와 게이트 절의 어휘.
- `slot=HH`는 batch_time의 KST 시각(정기 8슬롯 `02`..`09`; rerun·manual은 실행 시각), `final=1`은 정기 모드에서 시각 ≥ `FINAL_HOUR_KST`(09)일 때만. 09시 슬롯의 `BATCH_RESULT … final=1` 줄 부재 = 그날 수집 실패(§7.5 알림 근거). `BATCH_RESULT status`: FAILURE 1개라도 있으면 `FAILURE`, 전부 `NODATA`면 `NODATA`, 그 외(전부 SKIPPED 포함) `SUCCESS`; `services_ok` = SUCCESS+NODATA 수. `reason=mutation_budget`은 BATCH_RESULT에도 붙는다.
- SIGTERM(activeDeadlineSeconds 3000·노드 축출)을 받으면 마지막으로 계산된 `BATCH_RESULT` 줄에 ` note=sigterm`을 붙여 다시 찍고 종료한다 — 그 줄의 수치는 중단 시점까지의 누계.
- `CHECK WARN` 코드 어휘(행 플래그는 거부가 아니라 **적재하되 `flags` 컬럼에 표기** — mart가 판단): `hours_over_count`(gpuHours > gpuCount×24인 행 수), `unknown_violation`(serving/standby 용도의 `model=unknown` 행 수), `dup_merged`(gpu 동일 키 `(model, gpuType, category)`로 합산된 원행 수), `pct_non_monotone`(p50≤p90≤p95≤p99 역전 행 수), `dup_model_kept_first`·`dup_custom_kept_first`(serving 중복 레코드/custom name — 첫 것 유지, 버린 수), `identity_drift`(응답의 serviceGroup/service가 레지스트리와 다름), `generated_at_parse_failed`·`generated_at_offset_mismatch`(`generatedAt` 파싱 실패 / 오프셋 ≠ +09:00), `engine_malformed`, `extra_top_keys`(응답 최상위 미지 키 수 — 설계 §5.3은 "무시"; 적재는 그대로 하고 관측용으로만 센다), `all_rows_rejected`(rows=0 & rejected>0), `manual_row_present`(정기 모드에서 manual-v0 앵커 발견 — API로 덮으려면 `rerun.py --replace`), `registry_sync_failed`(`service=-` — 레지스트리 diff-sync 실패, 수집은 계속).
- `MANUAL_INPUT`은 manual 모드에서 실행당 1줄(SERVICE_RESULT보다 앞) — 파서가 읽은 행 수만.
- 로깅 계약(§3 전제 11): 페이로드·CSV 행 원문·모델명 목록을 로그에 쓰지 않는다 — 코드·카운트·서비스명만. 파서 오류도 `<경로>:<줄>: <필드>`까지만.
```

검증:

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
grep -c "^## " README.md
for c in hours_over_count unknown_violation dup_merged pct_non_monotone dup_model_kept_first dup_custom_kept_first identity_drift generated_at_parse_failed generated_at_offset_mismatch engine_malformed extra_top_keys all_rows_rejected manual_row_present registry_sync_failed; do grep -rq "$c" app/ || echo "code not in app/: $c"; grep -q "\`$c\`" README.md || echo "code not in README: $c"; done
grep -c "note=sigterm" README.md
```

Expected: `8` / 출력 없음(CHECK 코드 14종이 `app/`와 README 양쪽에 있다) / `1`.

- [ ] **Step 10: `## 검증` 추가 — 단위·매니페스트·E2E(18125/18001)·mock 시나리오·CI 워크플로**

`collectors/token-metrics/README.md` 끝에 이어 붙인다(T11 `run_e2e.sh` 포트·이미지 이름, T1 mock 시나리오 키와 동일).

```markdown
## 검증

단위 테스트(클러스터·도커 불필요 — `tests/e2e/`는 제외):

    cd collectors/token-metrics
    python -m pytest -q tests/ --ignore=tests/e2e

매니페스트 계약(`schedule: 5 2-9 * * *`, `timeZone`, `startingDeadlineSeconds: 540`, `activeDeadlineSeconds: 3000`, `backoffLimit: 0`, `-verify` 이름, 렌더 결과에 기존 수집기 모듈 이름·`VM_PUSH_URL` 0건 — kubectl이 있으면 `kubectl kustomize` 렌더까지):

    python -m pytest -q tests/test_manifests.py

E2E(도커 — ClickHouse 24.8 `18125:8123` + mock provider `18001:8000`; 기존 수집기 e2e의 18123/18000과 충돌 없음). 정기 2회(2회차 `already_loaded`·`system.mutations` 0) → 검증 SQL 11종 → `--replace`(fact 뮤테이션 정확히 3·감사 1행) → 시나리오 3종(`hours_over_count` WARN / `metrics_empty_gpu` → `rows=9` / 409 2회 → `FAILURE reason=not_ready`) → manual-v0 1회(`rows_gpu=2 rows_serving=3 rows_engine=1`) → `E2E PASS`:

    docker build -t token-mock-provider:e2e tools/mock-provider
    ./collectors/token-metrics/tests/e2e/run_e2e.sh            # 날짜 기본 = 어제; 인자로 YYYY-MM-DD 지정 가능

mock provider 시나리오(로컬 mock에 직접 — 플래그 6종 `metrics_gpu_hours_over`, `metrics_unknown_serving`, `metrics_pct_non_monotone`, `metrics_dup_gpu_rows`, `metrics_empty_gpu`, `metrics_engine_null`(0/1) + `not_ready_until_uptime_s`(초) + `retry_after_s`; 보존 기간 `MOCK_METRICS_RETENTION_DAYS`, 기본 14 — 그보다 오래된 날짜는 404):

    curl -X POST localhost:18001/__mock/scenario -H 'content-type: application/json' -d '{"metrics_gpu_hours_over": 1}'
    curl -X POST localhost:18001/__mock/reset

CI: `.github/workflows/test-collector-metrics.yml`(paths `collectors/token-metrics/**`, `tools/mock-provider/**` — jobs `unit`/`e2e`/`image`/`manifests`), `.github/workflows/release-images-metrics.yml`(`main` 푸시 → `ghcr.io/yoonsungnam/token-metrics-collector:<sha7>`·`:latest`). 기존 `test-collector.yml`·`release-images.yml`은 이 모듈을 보지 않는다.
```

검증:

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
grep -c "^## " README.md
grep -q "18125" tests/e2e/run_e2e.sh && grep -q "18001" tests/e2e/run_e2e.sh && echo PORTS_OK
for k in metrics_gpu_hours_over metrics_unknown_serving metrics_pct_non_monotone metrics_dup_gpu_rows metrics_empty_gpu metrics_engine_null; do grep -rq "$k" ../../tools/mock-provider/app/ || echo "scenario flag not in mock: $k"; done
ls ../../.github/workflows/test-collector-metrics.yml ../../.github/workflows/release-images-metrics.yml
```

Expected: `9` / `PORTS_OK` / 출력 없음(시나리오 플래그 6종이 mock에 있다) / 워크플로 2파일 경로.

- [ ] **Step 11: `## DDL·뮤테이션 장부 (§4.0)` 추가 — ddl 디렉터리·accounts.sql(admin)·장부 요약 + `ddl/README.md` 링크·실측 쿼리**

`collectors/token-metrics/README.md` 끝에 이어 붙인다(장부 정본은 Plan 6a `ddl/README.md` — 여기서는 수집기 관련 행만 요약하고 링크한다).

```markdown
## DDL·뮤테이션 장부 (§4.0)

    ddl/
    ├── README.md                       # 파일 표 · 뮤테이션 장부(설계 §4.0 표 그대로) · 확정된 결정 · 적용 순서
    ├── company/
    │   ├── raw_token_metrics.sql       # fact.raw_token_metrics_{gpu,serving,summary}_1d + fact.collect_audit_metrics_1d (_local/_dist ×4)
    │   ├── dim_token_metrics_service.sql   # gpu_data.dim_token_metrics_service (_local/_dist)
    │   └── accounts.sql                # GRANT TO mart (테이블 레벨; 감사 테이블은 SELECT·INSERT만) — admin 수동
    ├── stage/            (생성물 — tools/gen_stage_ddl.py; 직접 수정 금지)
    └── company-verify/   (생성물 — tools/gen_verify_ddl.py; token_verify_* DB·계정)

install.sh [5/7]가 `raw_token_metrics.sql`·`dim_token_metrics_service.sql`을 적용하고(`IF NOT EXISTS` — 재실행 안전), `accounts.sql`은 admin이 먼저 실행한다([4/7] 프리플라이트가 DB 존재만 확인). DB는 만들지 않는다(company `fact`·`gpu_data`는 기존 DB).

뮤테이션 장부(수집기 관련 행 — 전체 표·일 총량 상한은 `ddl/README.md`):

| 경로 | 뮤테이션 |
|---|---|
| 정기 시간별 실행(8슬롯) | **0** — 앵커 존재→스킵, 미존재→INSERT만; 레지스트리 동기화는 정기 실행에서만·diff-check |
| 레지스트리 변경(endpoints 편집·최초 배포) | 1(최초 배포는 현재 집합이 비면 DELETE 생략 → 0) |
| 크래시 잔여물 복구(부분 적재) | 서비스당 ≤3 — 정기 실행에서 뮤테이션이 생기는 유일한 경우 |
| 재수집 `--replace`·manual 재적재 | 날짜당 fact **≤3**(summary·gpu·serving; 감사는 append-only; 테이블별 `service IN (...)` 배칭) |
| 실행당 가드 | `METRICS_MAX_MUTATIONS_PER_RUN`(기본 **45** = 3×15) — 첫 DELETE 전 존재확인으로 합산, 초과 시 `FAILURE reason=mutation_budget`; 긴 범위는 `tools/rerun.py --chunk-days`(기본 7) |
| 피크(02:00~03:00) | 02:05 첫 슬롯은 INSERT만; 재수집은 **10:50 KST 이후**(rerun.py 실행 창) |

실측(최근 24시간 fact 뮤테이션 수 — 정기 슬롯만 돌았다면 0):

    SELECT count() FROM system.mutations WHERE database='fact' AND table LIKE 'raw_token_metrics_%' AND create_time > now() - INTERVAL 1 DAY

레지스트리·감사 쪽까지 보려면 `database IN ('fact','gpu_data')`, company-verify는 `database IN ('token_verify_fact','token_verify_dim')`. `is_done = 0`인 행이 남아 있으면 `mutations_sync=2`로 기다리던 실행이 중단된 것 — 다음 rerun 전에 `SELECT * FROM system.mutations WHERE is_done = 0`으로 확인한다.
```

검증:

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
grep -c "^## " README.md
grep -n "^## " README.md
grep -c "정기 시간별 실행(8슬롯)" ddl/README.md README.md
grep -c "system.mutations WHERE database='fact' AND table LIKE 'raw_token_metrics_%'" README.md
```

Expected: `10` / 절 앵커 10줄이 이 순서로 — `## 실행`, `## 모드와 게이트`, `## 환경변수 (§5.2)`, `## 배포 (§5.6)`, `## 수기(manual-v0) 적재 (§5.5)`, `## 재수행 (§6.3)`, `## 부분 적재 복구 (§5.4)`, `## 마커 (§5.6)`, `## 검증`, `## DDL·뮤테이션 장부 (§4.0)` / `ddl/README.md:1`·`README.md:1`(장부 첫 행 문구가 정본과 같다) / `1`.

- [ ] **Step 12: 문서-코드 대조 — 절 10개·클론 원본 언급·사내 주소/메일/코드명 0·Plan 6c 문서 링크 0·모든 명령의 플래그가 `--help`와 일치**

```bash
cd /home/mini/github/token-data-pipeline/collectors/token-metrics
echo "sections=$(grep -c '^## ' README.md)"
grep -n '^## ' README.md | cut -d: -f2 | tr '\n' '|'; echo
echo "token-usage mentions=$(grep -c 'token-usage' README.md)"
echo "--- internal host / mail / plan-6c link (expect empty) ---"
grep -n "harbor\." README.md | grep -v "harbor\.example\.internal"
grep -n -E "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}" README.md
grep -n "docs/operations/token-metrics-deploy" README.md
echo "--- flags vs --help ---"
python3 - <<'PY'
import re, subprocess, sys
text = open("README.md", encoding="utf-8").read()
# 백슬래시 이어짐을 한 줄로 합친 뒤, README의 4칸 들여쓴 명령 줄 중 이 모듈 CLI 3종을 부르는 줄만 본다
# (본문 표·문장 안의 플래그 언급은 제외 — 명령은 코드 줄이 정본).
joined = re.sub(r"\\\n\s*", " ", text)
targets = {   # 개발 머신에는 `python` 바이너리가 없다(python3 만) — README 명령 줄의 `python -m app.main` 은 컨테이너 안 것
    "app.main": ["python3", "-m", "app.main", "--help"],
    "tools/rerun.py": ["python3", "tools/rerun.py", "--help"],
    "tools/manual_load.py": ["python3", "tools/manual_load.py", "--help"],
}
helps = {k: subprocess.run(v, capture_output=True, text=True).stdout for k, v in targets.items()}
mismatches = 0
for line in joined.splitlines():
    if not line.startswith("    ") or "python" not in line:
        continue
    for key, help_text in helps.items():
        if key not in line:
            continue
        for flag in sorted(set(re.findall(r"--[a-z][a-z-]*", line))):
            if flag not in help_text:
                mismatches += 1
                print(f"MISMATCH {key}: {flag} :: {line.strip()[:100]}")
print(f"flag mismatches: {mismatches}")
sys.exit(1 if mismatches else 0)
PY
echo "rc=$?"
```

Expected:
- `sections=10`, 절 순서 `## 실행|## 모드와 게이트|## 환경변수 (§5.2)|## 배포 (§5.6)|## 수기(manual-v0) 적재 (§5.5)|## 재수행 (§6.3)|## 부분 적재 복구 (§5.4)|## 마커 (§5.6)|## 검증|## DDL·뮤테이션 장부 (§4.0)|`
- `token-usage mentions=1`(머리말의 "클론 원본" 1회 — 검증 절의 `test-collector.yml`·e2e 포트 설명은 `token-usage` 문자열을 쓰지 않는다; 2 이상이면 해당 문장을 "기존 수집기"로 바꾼다)
- 사내 주소·메일·Plan 6c 문서 링크 grep 전부 출력 없음
- `flag mismatches: 0`, `rc=0`. MISMATCH가 나오면 README 명령을 고친다(argparse가 정본 — `app/main.py`·`tools/*.py`는 이 태스크에서 수정하지 않는다).

- [ ] **Step 13: zero-diff 확인 + 커밋**

```bash
cd /home/mini/github/token-data-pipeline
git status --porcelain -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml
git status --porcelain -- collectors/token-metrics
```

Expected: 첫 명령 출력 없음(zero-diff 대상 무변경); 둘째 명령은 `?? collectors/token-metrics/README.md` 1줄만(다른 파일이 보이면 이 태스크 밖의 변경 — 커밋에 넣지 않는다).

```bash
cd /home/mini/github/token-data-pipeline
git add collectors/token-metrics/README.md
git commit -m "docs(collectors-metrics): 모듈 README — 모드×게이트·env·배포·manual-v0·rerun 창·부분 적재 복구·마커·장부 (Plan 6b T12)

- 절 10개(실행/모드와 게이트/환경변수/배포/수기 적재/재수행/부분 적재 복구/마커/검증/DDL·뮤테이션 장부) — Plan 6c 운영 문서가 앵커로 링크
- 모드×게이트 표 + 사유 어휘 14종 + unexpected:<Type>, env 16종(VM push 없음 명시), install.sh [1/7]~[7/7]·CronJob 값
- manual-v0: 템플릿 3파일·헤더 원문·규칙·k8s Job P0 경로(900KB·ConfigMap 정리)·워크스테이션 port-forward 대안·mart-metrics rerun 의무
- rerun: 실행 창 10:50 KST·활성 token-mart-metrics Job 0(exit 3)·청크=Job·--chain-mart 전체 범위 전파·뮤테이션 산식 45=3×15
- 부분 적재 복구 문단(정기 실행 뮤테이션 3의 유일한 경우), 마커 3형식+MANUAL_INPUT+CHECK 코드 14종, system.mutations 실측 쿼리
- 공개 레포: 사내 주소는 harbor.example.internal 플레이스홀더, 코드명·메일 0; docs/operations/token-metrics-deploy.md 미링크(자기완결)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
git log --oneline -1
```

Expected: `git log --oneline -1`이 `docs(collectors-metrics): 모듈 README — 모드×게이트·env·배포·manual-v0·rerun 창·부분 적재 복구·마커·장부 (Plan 6b T12)`로 시작하는 1줄.

---

## 완료 기준 (Plan 6b)

- [ ] T1~T12 커밋 **13개**(T7은 파서·main manual 모드 2커밋)가 `feat/token-metrics-design`(또는 그 위에 딴 `feat/token-metrics-collector`) 브랜치에 순서대로 존재하고, 각 커밋 메시지가 `type(scope): 한국어 설명 (Plan 6b Tn)` + 트레일러 2줄(`Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`, `Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54`)을 지킨다. type/scope는 T1 `feat(mock)`, T2~T10 `feat(collectors-metrics)`, T11 `ci(collectors-metrics)`, T12 `docs(collectors-metrics)`.
- [ ] zero-diff(기존 산출물): `git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org assets/model-catalog tools/verify/invariants.sql docs/operations/company-verify.md docs/operations/stage-runbook.md docs/operations/rerun.md docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml` 출력 0줄.
- [ ] zero-diff(Plan 6a 산출 `collectors/token-metrics/ddl/**`·`docs/templates/**` — 6b는 읽기만): 전제 = **6a PR이 `main`에 병합돼 있고 6b 브랜치가 그 `main` 위로 rebase 된 상태**(T1 Step 1이 확인하는 6a 산출 존재 조건과 같다). 그 상태에서 `git diff --stat main -- collectors/token-metrics/ddl docs/templates` 출력 0줄. 6a가 아직 병합 전이면 이 항목은 `git diff --stat <6a 브랜치> -- collectors/token-metrics/ddl docs/templates`로 대신 확인하고(6a 브랜치 기준 0줄), 6b PR 본문에 "6a 병합 후 rebase 예정"을 적는다 — `main` 기준으로는 6a 파일 전체가 추가로 보이므로 0줄이 될 수 없다.
- [ ] additive 범위 확인: `git diff --stat main -- tools/mock-provider .github/workflows/test-mock-provider.yml .gitignore`에 나온 파일이 `tools/mock-provider/app/{config,scenarios,datagen,main}.py`, `tools/mock-provider/tests/test_metrics_api.py`, `tools/mock-provider/contract/{token-metric-api.yaml,SOURCE.md}`, `tools/mock-provider/contract/tests/check_metrics_api.py`, `tools/mock-provider/run_conformance.sh`, `tools/mock-provider/README.md`, `.github/workflows/test-mock-provider.yml`, `.gitignore`뿐이다. `.gitignore` diff는 `+` 2줄(`collectors/token-metrics/tests/e2e/endpoints.e2e.yaml`, `collectors/token-metrics/tests/e2e/.tmp/`)만.
- [ ] mock: `cd tools/mock-provider && python3 -m pytest -q` 전부 통과(기존 테스트 수 + `tests/test_metrics_api.py` 신규 ≥ 12), `./run_conformance.sh` 출력 끝에 usage 단계 OK + `check_metrics_api.py` exit 0(FAIL 0), 벤더링 바이트 동일: `S=/tmp/claude-1000/-home-mini-github-token-data-pipeline/8a0025e9-e64c-4caf-972c-788ea90abe0e/scratchpad/token-metric-api-spec; git -C $S rev-parse --short HEAD` == `6a552d2`(아니면 `git -C $S checkout 6a552d2`; 클론이 없으면 `git clone https://github.com/YoonsungNam/token-metric-api-spec.git $S && git -C $S checkout 6a552d2`), `diff $S/token-metric-api.yaml contract/token-metric-api.yaml` 0줄, `diff $S/scripts/check_metrics_api.py contract/tests/check_metrics_api.py` 0줄, `curl -s "http://127.0.0.1:8000/v1/metrics?date=$(date -d yesterday +%F)" | python3 -c "import sys,json; p=json.load(sys.stdin); print(len(p['gpu']), len(p['serving']), sorted(p))"` → `5 3 ['date', 'engine', 'generatedAt', 'gpu', 'service', 'serviceGroup', 'serving']`; `?date=$(date -d '-30 day' +%F)` → HTTP 404(`retention_days=14`).
- [ ] 수집기 단위: `cd collectors/token-metrics && python3 -m pytest -q tests/ --ignore=tests/e2e` 전부 통과 — 파일별 최소 개수: test_config 8, test_events 2, test_normalize 22, test_api_client 12, test_writer 16, test_main 24, test_manual 14, test_manifests 8, test_rerun 12, test_manual_load 9 (합 ≥ 127). `python3 -m app.main --help` exit 0(CH 접속 없이).
- [ ] 자기완결: `grep -rn "token_usage\|token-usage" collectors/token-metrics/app collectors/token-metrics/tools` 출력 0줄; `grep -rn "^from app\.\|^import app" collectors/token-metrics/app | grep -v "from app\.\(config\|events\|normalize\|api_client\|writer\|manual\)"` 출력 0줄; `grep -rn "vm_push\|VM_PUSH_URL\|random" collectors/token-metrics/app` 0줄(`random` 모듈 미사용).
- [ ] Python 3.10 호환: `python3.10 -m py_compile collectors/token-metrics/app/*.py collectors/token-metrics/tools/*.py`(3.10이 없으면 `grep -rn "StrEnum\|match \|ExceptionGroup\|tomllib" collectors/token-metrics` 0줄로 대체).
- [ ] 마커 형식(unit 로그 캡처): `python3 -m pytest -q tests/test_main.py -k "batch_line or service_line or sigterm"` 통과 — T6 단위 fixture(gpu 2행 + serving 1레코드 = rows 5)의 정확 문자열 `SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A source_type=metrics-api-v1 rows=5 pages=1 warn=0 rejected=0`, `BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 services_failed=0 services_skipped=0 rows=5 elapsed=0s slot=02 final=0`, `CHECK WARN service=Mock Service A hours_over_count=1`, SIGTERM 재출력 ` note=sigterm`; mock 3모델 기준 `rows=14`는 E2E(T11) 단언에서 확인.
- [ ] 매니페스트: `kubectl kustomize collectors/token-metrics/k8s/overlays/company-verify | grep -c -- "-verify"` ≥ 3(`token-metrics-collector-verify`, `token-metrics-ch-secret-verify`, `token-metrics-endpoints-verify`); `kubectl kustomize collectors/token-metrics/k8s/overlays/stage | grep -q "ghcr.io/yoonsungnam/token-metrics-collector:latest"`; 렌더 3종에서 `grep -c "token-usage"` 0; `grep -q 'schedule: 5 2-9 \* \* \*'`, `startingDeadlineSeconds: 540`, `activeDeadlineSeconds: 3000`, `backoffLimit: 0`, `METRICS_MAX_MUTATIONS_PER_RUN` 모두 존재.
- [ ] `bash -n collectors/token-metrics/{build.sh,install.sh,tests/e2e/run_e2e.sh}` exit 0; `install.sh` 텍스트에 `apply_sql` 호출 2건(`raw_token_metrics.sql`, `dim_token_metrics_service.sql`)·`accounts.sql` apply 0건·`vminsert` 0건·`system.databases` 1건·`dim_token_service_dist` 1건.
- [ ] E2E(도커 있는 곳 또는 CI `test-collector-metrics` e2e job): `docker build -t token-mock-provider:e2e tools/mock-provider && ./collectors/token-metrics/tests/e2e/run_e2e.sh` 마지막 줄 `E2E PASS (date=<어제>, gpu=5, serving=9)`; 중간 단언 — 2회차 `reason=already_loaded`, `system.mutations`(fact) 0 → `--replace` 후 3, 감사 1행, 시나리오 `hours_over_count=1`·`rows=9`(empty gpu)·`reason=not_ready`, manual `MANUAL_INPUT module=token-metrics rows_gpu=2 rows_serving=3 rows_engine=1 rows_outside_range=0 rows_other_service=0` + `source_type=manual-v0 rows=5`.
- [ ] CI 파일: `python3 -c "import yaml,sys; [yaml.safe_load(open(f)) for f in sys.argv[1:]]" .github/workflows/test-collector-metrics.yml .github/workflows/release-images-metrics.yml .github/workflows/test-mock-provider.yml` exit 0; `release-images-metrics.yml`의 `matrix.include` 항목 수 1(`collectors/token-metrics` → `token-metrics-collector`); `git diff --stat main -- .github/workflows/release-images.yml` 0줄.
- [ ] 공개 레포 규칙: `git grep -n -i -E "harbor\.[a-z]+\.(co\.kr|com)|@[a-z]+\.(co\.kr|com)" -- collectors/token-metrics tools/mock-provider .github | grep -v "noreply@anthropic.com"` 출력 0줄; 사내 프로젝트 코드명 0건(`memory/public-repo-codename.md`의 치환 규칙대로 grep); 실 데이터 파일(`endpoints-metrics.company.yaml`, `*manual_metrics*.csv` — `.gitignore` 패턴·README 예시 파일명 `gpu_manual_metrics.csv` 등과 같은 규칙) `git status --porcelain --untracked-files=all`에 미등장.
- [ ] README: `grep -c "^## " collectors/token-metrics/README.md` == 10, 명령 플래그가 `--help` 출력과 일치(T12 11단계).
- [ ] draft PR: 제목 `feat: 메트릭 수집기 token-metrics-collector (Plan 6b) — mock /v1/metrics·수집기·배포·CI`, 본문에 (a) 13커밋 목록, (b) zero-diff 검사 명령·출력, (c) e2e 실행 증빙(CI run 링크 또는 로컬 `E2E PASS` 줄), (d) 6c에 넘기는 인터페이스 절 링크(아래 `## 인터페이스 요약`), (e) `release-images-metrics.yml` matrix가 collectors 1항목뿐이며 mart 항목은 6c가 additive로 추가함을 명시.

## 인터페이스 요약 (Plan 6c가 소비하는 것 — 6b가 고정)

### A. fact 적재 의미론
- 앵커 = `fact.raw_token_metrics_summary_1d` 행(`(date, service)`당 정확히 1행; NODATA도 1행 — summary 컬럼 `gpu_rows=0, serving_rows=0`; `NormalizeResult` 프로퍼티 `n_gpu/n_serving`은 코드 안 이름이고 컬럼명은 6a `gpu_rows/serving_rows`). 앵커 존재 = 그 (date, service)의 gpu/serving 적재 완료. 앵커 없이 gpu/serving 행이 있으면 부분 적재(다음 실행이 자동 복구 — 6c 불변식은 이를 `partial_load`로 **WARN**만).
- DELETE 순서 `summary → gpu → serving`, INSERT 순서 `gpu → serving → summary`; 정기 실행은 뮤테이션 0(앵커 존재 → 스킵, 미존재 → INSERT만; 부분 적재 복구 시에만 3). `--replace`·manual은 날짜당 `_delete_day_in`(`service IN (...)`) 3회 = (date, 서비스 집합)당 3. 예산 `METRICS_MAX_MUTATIONS_PER_RUN=45`(실행당 누계, 레지스트리 DELETE 포함) 초과 예정 시 적재 전 `FAILURE reason=mutation_budget`.
- `source_type ∈ {"metrics-api-v1", "manual-v0"}`(summary·gpu·serving 모두 같은 값). 같은 (date, service)에 두 source_type이 공존하지 않는다(교체 시 3테이블 전부 DELETE).
- `generated_at`: API `generatedAt` 파싱(오프셋 ≠ +09:00 → KST 변환 + WARN `generated_at_offset_mismatch`; 파싱 실패·naive → 수집 시각 + WARN `generated_at_parse_failed`; 빈 값(manual `--generated-at` 미지정) → 적재 시각, WARN 없음). `collected_at`: aware KST `now`. 두 컬럼 모두 `DateTime('Asia/Seoul')`.
- `engine_type/engine_version`: summary 컬럼(문자열, 부재 = `''`). API `engine` 부재·`null` → `''`/`''`(WARN 없음 — 계약상 선택 필드); dict이지만 `type` 비어있음·길이 초과 등 형식 위반 → `''`/`''` + WARN `engine_malformed`.
- `flags` 어휘(Array(String), 빈 배열 정상) — gpu: `hours_over_count`(gpuHours > gpuCount×24 + 1e-6), `unknown_violation`(`model='unknown'`이 `category≠'test'`), `dup_merged`(동일 키 행 합산); serving: `pct_non_monotone`(p50 ≤ p90 ≤ p95 ≤ p99 위반), `unknown_violation`(`model='unknown'`), `dup_model_kept_first`, `dup_custom_kept_first`. 6c 불변식·mart 품질 플래그는 이 문자열을 `has(flags, '<code>')`로 검사한다.
- serving long-form: `metric ∈ ttft_ms|itl_ms|e2e_ms|output_tps|custom`, `name`(custom만 비어있지 않음; 표준 지표 `''`), `unit`(`ms` / `tokens/s` / custom 단위 ≤32자), `p50/p90/p95/p99` Nullable(Float64)(`output_tps`는 p50만 non-null).
- gpu 행 도메인: `category ∈ serving|standby|test`, `gpu_count > 0`, `gpu_hours ≥ 0`, `model`≤128·`gpu_type`≤64.

### B. 마커(로그) 계약 — 6c 대시보드·불변식이 grep
- `SERVICE_RESULT status=SUCCESS|NODATA|SKIPPED|FAILURE module=token-metrics service=<svc> source_type=metrics-api-v1|manual-v0 rows=<n> pages=1 warn=<n> rejected=<n>[ reason=<code>]`
- `BATCH_RESULT status=SUCCESS|NODATA|FAILURE module=token-metrics services_ok=<n> services_failed=<n> services_skipped=<n> rows=<n> elapsed=<int>s slot=HH final=0|1[ reason=<code>]` — 실행당 1줄(rerun/manual 범위도 1줄); SIGTERM 시 마지막 줄 재출력 + ` note=sigterm`.
- `CHECK WARN service=<svc> <code>=<count>` — 코드: `hours_over_count, unknown_violation, dup_merged, pct_non_monotone, dup_model_kept_first, dup_custom_kept_first, identity_drift, generated_at_parse_failed, generated_at_offset_mismatch, engine_malformed, extra_top_keys, all_rows_rejected, manual_row_present, registry_sync_failed`(마지막은 `service=-`). 서비스별 `warn=<n>`은 그 서비스 CHECK 코드 카운트의 합.
- `MANUAL_INPUT module=token-metrics rows_gpu= rows_serving= rows_engine= rows_outside_range= rows_other_service=` — manual 모드 정보 1줄.
- reason 어휘: `disabled | before_since | after_until | already_loaded | not_ready | not_ready_at_0900 | retention | retryable | permanent_error | mutation_budget | load_budget | deadline | unknown_service | invariant_broken | unexpected:<ExceptionType>`(`retryable` = 429/5xx/네트워크 재시도 3회 소진, `permanent_error` = 400·본문 5MB 초과·date 에코 불일치·non-JSON·구조 위반 — 둘 다 FAILURE).
- 슬롯: `slot=HH`는 batch_time KST 시각(정기 `02..09`); `final=1`은 정기 모드 + `hour ≥ FINAL_HOUR_KST(9)`뿐 — 6c "09시 최종 미적재" 판정은 `final=1`인 BATCH_RESULT의 `services_failed`/`reason=not_ready_at_0900`을 본다.

### C. 레지스트리 `gpu_data.dim_token_metrics_service`
- 12컬럼 순서 = Plan 6a B. 동기화 = 정기 실행에서만, **diff-sync**(현재 11컬럼 튜플 집합 == endpoints 산출 집합이면 무변경; 다르면 `DELETE WHERE 1`(local, ON CLUSTER) 1회 + 전체 INSERT — `updated_at`은 비교 제외). 실패는 `CHECK WARN service=- registry_sync_failed=1`(수집은 계속). rerun·manual은 동기화하지 않는다.
- endpoints 파일 키(§4.3): `serviceGroup, service, baseUrl, enabled, apiSince(기본 2026-09-09), coverageSince(기본 2026-08-26), until(null), expectGpu(true), expectServing(true), usageIncludesConsumers(false), note('')`. 사내 정본 `collectors/token-metrics/endpoints-metrics.company.yaml`(gitignored) → ConfigMap `token-metrics-endpoints`.
- 토큰 레지스트리 `gpu_data.dim_token_service_dist`는 **읽기 전용 접점**(install.sh 프리플라이트 SELECT만) — 수집기 본체는 읽지 않는다. 6c M0 불변식(`service_group/service` 바이트 일치)은 두 dim을 6c가 조인한다.

### D. rerun 체인 계약(6c `mart/token-metrics/tools/rerun.py`가 받는 호출)
- `python3 mart/token-metrics/tools/rerun.py --context <C> --namespace <N> [--cronjob token-mart-metrics-verify] --from <D0> --to <D1> --chunk-days <n> [--force]` — 6b `tools/rerun.py --chain-mart`가 `[sys.executable, <repo>/mart/token-metrics/tools/rerun.py, ...]`로 호출. 옵션 전파 규칙: 6b `--cronjob`이 `-verify`로 끝나면(company-verify) `--cronjob token-mart-metrics-verify`를 붙이고(아니면 6c 기본값 `token-mart-metrics`에 맡겨 생략), 6b `--force-window`면 `--force`를 붙인다(6c `--force`는 10:50 창 검사만 생략). `--chunk-days`는 6b `CHUNK_DAYS_MAX = 15`(뮤테이션 예산 45 = 15×3) 이하 값만 오므로 6c 상한 16 안이다. D0/D1은 수집기 rerun의 전체 범위(청크 분할 전, 스킵 날짜 포함).
- 6b 창 검사는 `kubectl get jobs -n <N> -o json`에서 `ownerReferences[].name == "<mart cronjob>"`(`token-mart-metrics` 또는 `token-mart-metrics-verify`) 또는 이름 접두 `<mart cronjob>-`인 Job의 `status.active`를 본다 — 6c의 rerun Job 이름은 `token-mart-metrics[-verify]-rerun-…` 접두 규칙을 지켜야 한다. 6c 자신의 활성 Job 게이트는 더 넓다(`token-mart-*` 전부, `--force`로도 우회 불가, `RERUN REFUSED active_jobs=<n> (token-mart-* running)` exit 2) — 6b는 이를 복제하지 않으며, 체인 단계에서 거부되면 운영자가 `[NEXT]` 명령을 재실행한다(6b README 재수행 절).
- 6c가 `release-images-metrics.yml`에 추가할 것: `paths`에 `mart/token-metrics/**`, `matrix.include`에 `{context: mart/token-metrics, image: token-mart-metrics}`(additive; 6b가 만든 collectors 항목·paths 유지).

### E. 배포 이름(6c 운영 문서·company-verify 절차가 참조)
- 이미지 `token-metrics-collector`, CronJob `token-metrics-collector[-verify]`(라벨 `app: token-metrics-collector`), Secret `token-metrics-ch-secret[-verify]`, ConfigMap `token-metrics-endpoints[-verify]`, `token-metrics-ca-bundle`(optional, suffix 없음), `token-metrics-manual-<YYYYmmddHHMMSS>`(manual 임시), 공유 `registry-pull-secret`(없을 때만 생성).
- Job 이름: 정기 = CronJob 생성 이름, rerun `token-metrics-collector-rerun-<epoch>-<i>`, manual(P0) `token-metrics-collector-manual-<YYYYmmddHHMMSS>`, 수동 트리거 `token-metrics-collector-manual-<epoch>`.
- 컨테이너 env 리터럴 6개(`ENDPOINTS_FILE, SOFT_DEADLINE_MINUTES=40, LOAD_BUDGET_S=1200, FINAL_HOUR_KST=9, MAX_RESPONSE_BYTES=5000000, METRICS_MAX_MUTATIONS_PER_RUN=45`) + Secret envFrom(`CH_USER, CH_PASSWORD, CH_PORT, CH_CLUSTER[, CH_DB_FACT, CH_DB_DIM, COLLECTOR_HTTPS_PROXY, COLLECTOR_API_CA_BUNDLE]`) + `set env CH_HOST`.

### F. mock-provider(6c e2e·대시보드 데모가 재사용)
- `GET /v1/metrics?date=` — `datagen.build_metrics(cfg, date, scn)` 결정적(같은 seed·date·시나리오 → 바이트 동일), 3모델 기본 gpu 5행·serving 3레코드(ttftMs/itlMs/outputTps), `engine {"type":"vllm","version":"0.10.1"}`, retention 14일(`MOCK_METRICS_RETENTION_DAYS`). 시나리오 int 6종 `metrics_gpu_hours_over, metrics_unknown_serving, metrics_pct_non_monotone, metrics_dup_gpu_rows, metrics_empty_gpu, metrics_engine_null` + 기존 `not_ready_until_uptime_s, retry_after_s, rate_limit_every, error_503_every, name_drift`가 metrics에도 적용. e2e 기대치는 `tests/e2e/ci_expectations.py <date> <seed> <models>` → `rows_gpu=5 rows_serving=9 gpu_hours_sum=<x>`.

## Self-Review 노트

### 설계 절별 커버리지

| 설계 절(행) | 반영 Task |
|---|---|
| §3 전제(58-78): 사내 CH_HOST 헤드리스·프록시/CA·mart 계정 공유 | T2(Config env) · T8(install.sh Secret/CA ConfigMap·`set env CH_HOST`) · T12(워크스테이션 대안) |
| §4.0 뮤테이션 장부(119-128): 정기 0 / `--replace` ≤3 / 45 | T5(`mutations_done`·`MutationBudgetExceeded`·`_delete_day_in`) · T6(`reason=mutation_budget`) · T11(`system.mutations` 실측 0→3) · T12(장부 절) |
| §4.1 fact 4테이블 컬럼·앵커(Plan 6a A) | T5(GPU_COLS/SERVING_COLS/SUMMARY_COLS/AUDIT_COLS 명시 INSERT) · T3(long-form serving 행) |
| §4.3 레지스트리 12컬럼·endpoints 키·동기화 규칙(196-229) | T2(`ServiceEntry`·`load_endpoints` 기본값) · T5(`sync_registry` diff) · T6(정기만 호출·WARN) · T11(`registry_synced` 검사) |
| §5.1 슬롯 8회·모드(233-237) | T6(`RunContext.slot/final`, `_target_dates`) · T8(`schedule "5 2-9 * * *"`) |
| §5.2 CronJob 값·env 표·마커 형식(239-258) | T2(Config 기본값·불변식) · T6(`_service_line`/`_batch_line`/`_check_lines`/SIGTERM) · T8(cronjob.yaml·test_manifests) · T11(manifests grep) |
| §5.3 게이트·사유 어휘·normalize 3계층(260-266) | T3(normalize) · T4(HTTP→Event 번역표) · T6(`_gate`·`_outcome_from_error`·409 재방문) |
| §5.4 적재 시퀀스·부분 적재·배칭·가드(268-274) | T5 · T6(정기 1건/rerun 날짜당 1건 배칭) · T12(복구 절) |
| §5.5 manual-v0 CLI·템플릿·P0 전달 경로(276-280) | T7(`app/manual.py`·main 분기) · T10(`tools/manual_load.py`) · T11(manual 1회 e2e) · T12 |
| §5.6 클론·배포·rerun·manual_load·CI·release(282-289) | T8 · T9 · T10 · T11 · T12 |
| §6.3 rerun 체인·창 10:50·활성 Job 0(312-314) | T9(`check_window`·`count_active_mart_jobs`·`build_mart_command`) · T12 |
| §7.3 mock·단위/e2e 테스트 항목(350-354) | T1(mock 6 시나리오·계약 벤더링·conformance) · T2~T10 테스트 · T11 e2e |
| §7.5 zero-diff·additive 목록(361-370) | Global Constraints · 완료 기준 zero-diff/additive 검사 · T11(`release-images.yml` 무수정) |
| §6.1 mart 컬럼, §7.1 불변식, §7.3 `docs/operations/token-metrics-deploy.md`, §8 스펙 v1.14, `tools/data-admin/delete_data.py` 타깃 | Plan 6c 몫(6a가 §8 처리) — `## 인터페이스 요약` D·E에 경계 명시 |
| §10 일정(415-436) | 마감 9/9 API·9/14 보고 — 이 플랜은 T1→T12 순서가 곧 임계 경로(T1·T2·T3·T5·T6이 선행; T8~T11은 T6 이후 병렬 가능) |

### 설계 해석 (설계가 명시하지 않아 이 플랜이 확정한 것 — 6c·리뷰어가 이의 시 여기서 고친다)

1. **Task 순서 T3=normalize, T4=api_client**: `MetricsPayload`가 `normalize.py`에 살고 `api_client.fetch_metrics`가 그것을 반환하므로 normalize를 먼저 만든다(header File Structure 주석도 T3/T4로 표기).
2. **배칭 단위**: 정기 = 서비스별 `replace_batch(date, [item])` 순차(서비스 1개 실패가 다른 서비스 적재를 막지 않게), rerun/manual = 날짜당 `replace_batch(date, items)` 1회(§5.4 "날짜당 3회" 문장을 서비스 집합 `IN (...)`으로 구현). 두 경로 모두 같은 함수.
3. **DELETE×3 조건 = 존재확인 3종 합집합**: gpu·serving·summary 중 하나라도 대상 서비스 행이 있으면 3테이블 모두 DELETE(부분 적재 잔여 행 회수). 감사 행은 summary 앵커가 있던 서비스만(`fetch_prev_summary`가 None이면 감사 없음).
4. **뮤테이션 가드 = 실행당 누계** `MetricsWriter.mutations_done`(레지스트리 `DELETE WHERE 1` 포함) + 예정 3 > 45 → 적재 전 예외. 설계 표현 "실행당 ≤45"를 누계로 읽었다. 정기 실행에서 부분 적재 복구가 겹쳐 15쌍을 넘는 일은 사실상 없지만, 넘으면 `FAILURE reason=mutation_budget`로 멈추고 README가 `--service` 분할을 안내한다.
5. **409 재방문 1회는 큐 끝에서**(`min(max(Retry-After, 1), 300)`초 대기): 2회째 409의 결과는 정기 비최종 `SKIPPED not_ready` / 정기 최종(slot ≥ 09) `FAILURE not_ready_at_0900` / rerun·manual `FAILURE not_ready`. `final`은 정기 모드 + `batch_time.hour ≥ FINAL_HOUR_KST`뿐(rerun은 항상 0).
6. **레지스트리 동기화 = diff-sync**(§4.3 "동기화"를 "다를 때만 교체"로): 비교 키는 `updated_at` 제외 11컬럼 튜플 집합. 실패는 WARN(`registry_sync_failed=1`)이고 수집은 계속 — 수집 데이터가 레지스트리보다 우선.
7. **`generatedAt` 빈 값·manual 미지정은 WARN 없이 적재 시각**, `engine` 부재/null도 WARN 없음(계약상 선택), 형식 위반만 `engine_malformed`. 응답 최상위 미지 키는 `extra_top_keys` WARN(거부 아님) — 설계 §5.3 표는 "최상위 추가 키(무시)"라 WARN 없이 버리는 것이 원문이고, 이 플랜은 **의도적으로 벗어나** 적재는 그대로 하되(무시 = 거부 아님은 지킨다) 관측용으로 `CHECK WARN … extra_top_keys=<키 수>` 한 줄을 남긴다. 이유: 제공자가 계약 밖 키를 보내기 시작한 사실(스키마 드리프트)을 로그 없이 잃지 않기 위함이며, 6c 불변식·알림은 이 코드를 오류로 취급하지 않는다(`warn=` 합계에만 포함). 설계 원문대로 돌리려면 T3 `normalize_payload`의 `W_EXTRA_KEYS` 3줄만 지우면 된다.
8. **mock `_date_gate` 확장은 kwargs additive**(`retention_days=None, subject="usage"`) — 기존 usage 엔드포인트 응답 바이트 동일 유지. metrics 시나리오는 int 0/1 필드 6종(bool 거부 규칙은 기존 `_SCENARIO_RULES` 그대로).
9. **`ci_expectations.py`는 mock의 `datagen`을 import**(`sys.path`에 `tools/mock-provider` 추가) — 기대치 이중 구현 금지. 단위 CI(unit job)에서도 형식 스모크만 실행.
10. **rerun.py는 `--from/--to` 필수**(기존 모듈의 "인자 없이 어제 1일" 모드 없음 — 8슬롯 정기가 대체; 수동 1회는 `kubectl create job --from=cronjob/...`). 창 검사는 체인 여부와 무관하게 항상(`--force-window`로만 우회). 청크 Job은 `activeDeadlineSeconds: 3000`을 그대로 상속(일수 비례 재설정 없음 — 7일 × 서비스 수의 부하는 수집기 예산이 아니라 서버 데드라인이 막는다; 실패 시 이후 청크 중단·재시도 명령 출력).
11. **활성 mart Job 판정** = `ownerReferences[].name == "token-mart-metrics"` 또는 이름 접두 `token-mart-metrics-` + `status.active > 0`. 6c rerun Job 이름 규칙이 이를 따라야 함(인터페이스 D).
12. **manual_load.py는 rerun.py의 함수를 import하지 않고 복제**(운영자 파일 1개 복사 사용 관례). ConfigMap 900KB 상한, Job 오브젝트는 남기고 ConfigMap만 `finally` 삭제(`--keep-configmap`로 보존). mart-metrics rerun은 체인하지 않고 `[NEXT]` 안내만.
13. **install.sh 7단계**(기존 6단계 + 프리플라이트 분리): pull secret은 존재 시 프롬프트 없이 통과; 프리플라이트는 `system.databases` 2행 + `dim_token_service_dist` SELECT — 실패 시 DDL 적용 전에 exit 1. `CH_DB_FACT/CH_DB_DIM` 기본값을 company-verify 프롬프트 기본값(`token_verify_fact`/`token_verify_dim`)으로 제시.
14. **`release-images-metrics.yml` matrix = collectors 1항목**(설계 §5.6은 "collectors·mart 2개"라 했으나 mart 이미지는 Plan 6c 산출이므로 6c가 additive로 추가) — 인터페이스 D에 명시.
15. **e2e `not_ready` 검증은 다른 날짜(`DATE_ARG − 1`)의 rerun 모드로**: 같은 날짜는 앵커 존재 → `already_loaded`가 409보다 먼저 판정되기 때문. `retry_after_s=1`로 재방문 대기를 1초로 줄인다.
16. **e2e manual CSV는 `tests/e2e/manual_e2e/e2e_manual_v0_*.csv`**(Plan 6a gitignore 패턴 `*manual_metrics*.csv` 밖 — 합성 고정값이라 커밋 가능), `{DATE}` 치환본은 `tests/e2e/.tmp/`(gitignore 추가 1줄).
17. **`ddl_test_dims.sql`의 `gpu_data.dim_token_service` twin**은 기존 모듈 `DIM_COLS` 7컬럼(`service_group, service, base_url, enabled, source_type, note, updated_at`) MergeTree + Distributed('default') — 프리플라이트 SELECT와 6c M0 조인이 쓰는 컬럼만.
18. **README는 자기완결**(§7.3의 `docs/operations/token-metrics-deploy.md`는 6c 몫 — 6b README는 그 문서를 링크하지 않는다). `## ` 절 10개 고정(완료 기준 grep).
19. **커밋 단위 13개**: T1 mock 1커밋, T2~T6 수집기 앱 5커밋(각 모듈 + 테스트), T7 2커밋(`app/manual.py` 파서 → `app/main.py` manual 분기 — 파서가 독립 검증 가능한 단위라 분리), T8 배포, T9 rerun, T10 manual_load, T11 e2e+CI+gitignore, T12 README. 중간 커밋에서 CI red 없음(각 커밋이 자기 테스트를 통과; e2e는 T11 커밋부터 워크플로 존재).

### 기계 검사 결과 (플랜 작성 시점)

- `grep -n "^### Task" outline.md` → Task 1~12 제목 12개, 본문 순서 = header File Structure의 T 번호(T3 normalize / T4 api_client 재표기 완료).
- 설계 행 범위는 `grep -n "^### \|^## " docs/superpowers/specs/2026-08-31-token-metrics-ingest-design.md`(436행 파일)로 재확인: §4.3 196-229, §5.1 233-237, §5.2 239-258, §5.3 260-266, §5.4 268-274, §5.5 276-280, §5.6 282-289, §6.3 312-314, §7.3 350-354, §7.5 361-370.
- 금지어(미정 표시어·"나중에"·"적절히"·"Task N과 유사" 계열) outline/header/footer 본문 0건(이 문장 자체 제외). 실 호스트·코드명·이메일 0건(`harbor.example.internal`·`ghcr.io/yoonsungnam`만).
- 크로스 태스크 식별자 대조: T5 `replace_batch/anchor_exists/sync_registry/mutations_done` ↔ T6 호출; T3 `MetricsPayload/SOURCE_MANUAL/PCT_KEYS/LATENCY_KEYS` ↔ T4·T7; T7 `MANUAL_INPUT` ↔ T10·T11; T8 볼륨 순서 `[0] endpoints [1] ca-bundle` ↔ T10 `[2] manual`; T9 `MART_RERUN/MART_CRONJOB` ↔ 인터페이스 D; WARN/flag 코드 문자열은 T3 상수(`W_*`/`F_*`) 값에서 그대로 옮김.

### 조립 검사 결과 (ASSEMBLER — task-01~12 병합 후, 2026-09-05)

**설계 절별 커버리지(조립본 대조)** — 위 표의 각 행을 조립된 Task 본문에서 재확인한 결과:

| 설계 절 | 조립본에서 확인한 근거 |
|---|---|
| §3 전제 | T2 `load_config()` env 14종(`COLLECTOR_API_VERIFY` 포함) · T8 install.sh [2/7] Secret·CA ConfigMap·[7/7] `set env CH_HOST` · T12 README 배포 절 |
| §4.0 뮤테이션 장부 | T5 `MutationBudgetExceeded(planned, done, limit)`·`mutations_done`·`_delete_day_in` · T6 `REASON_MUTATION_BUDGET` 승격 · T11 `system.mutations` 단언 0→3→9 · T12 장부 절 |
| §4.1 fact 4테이블 | T5 `GPU_COLS(12)/SERVING_COLS(15)/SUMMARY_COLS(15)/AUDIT_COLS(9)`가 Plan 6a `_dist` DDL 컬럼 순서와 **바이트 일치**(스크립트 대조 MATCH 5/5) |
| §4.3 레지스트리 | T2 `ServiceEntry` 11필드 + `dim_key()`/`dim_row()` · T5 `DIM_COLS(12)` = Plan 6a DDL · T6 정기 모드만 `sync_registry` · T11 `registry_synced` |
| §5.1 슬롯·모드 | T6 `RunContext.slot/final`·`make_context`·`_target_dates` · T8 `schedule "5 2-9 * * *"` |
| §5.2 CronJob·env·마커 | T8 cronjob.yaml 리터럴 6개·Secret envFrom · T6 `_service_line/_batch_line/_check_lines`·`_sigterm_handler` · T11 manifests grep 13종 |
| §5.3 게이트·normalize | T3 상수·플래그·WARN 코드 · T4 번역표 · T6 `_gate`·`_outcome_from_error`·409 큐 끝 재방문 |
| §5.4 적재 시퀀스 | T5 `replace_batch` (1)존재확인→(2)감사·DELETE summary→gpu→serving→(3)INSERT gpu→serving→summary · T6 정기 1건/rerun·manual 날짜당 1건 |
| §5.5 manual-v0 | T7 `GPU_HEADER/SERVING_HEADER/ENGINE_HEADER`(T11 e2e CSV 헤더와 바이트 동일)·`COUNT_KEYS` 5종 = `MANUAL_INPUT` 마커 순서 · T10 ConfigMap→Job(/manual `[2]`) |
| §5.6 클론·배포·CI | T8 Dockerfile `CMD ["python", "-m", "app.main"]` · T9 `["python", "-m", "app.main", "--from", …]` · T11 워크플로 2개 |
| §6.3 rerun 체인 | T9 `check_window`·`count_active_mart_jobs`·`build_mart_command` · T10 `[NEXT]` 안내(체인 없음) |
| §7.3 테스트 | 조립본 `def test_` 수: T1 18 · T2 14 · T3 38 · T4 22 · T5 30 · T6 51 · T7 33(파서 25 + main manual 8) · T8 13 · T9 21 · T10 22 — 완료 기준의 파일별 최소 개수 전부 충족 |
| §7.5 zero-diff | 모든 Task `Modify:`가 Global Constraints 허용 목록 안(T1 mock 8파일·워크플로 1, T7 `app/main.py`·`tests/test_main.py`(둘 다 6b 신규), T11 `.gitignore` +2행) |

**플레이스홀더 스캔**: 미정 표시어 6종(영문 약어 2종·"나중에 구현"·"Task N과 유사"·"적절히"·"경계 케이스" 계열)의 대소문자 무시 grep → 0건(이 문장은 검색어 자체를 담지 않는다). 사내 코드명 0건, `noreply@anthropic.com` 외 이메일 0건, 실 호스트 0건(`harbor.example.internal`·`ghcr.io/yoonsungnam`·k8s 내부 DNS `*.clickhouse.svc`만).

**펜스 블록 파싱**: python 59 · yaml 8 · json 0 · (bash 68 · diff 11 · markdown 11 · text 10 · dockerfile 1 · sql 2 · csv 3) — `ast.parse`/`yaml.safe_load_all` 실패 0. python 2블록(T5 Step 8 `insert_service_day`/`replace_batch`, Step 12 `sync_registry`)은 `MetricsWriter` 클래스 본문에 붙여 넣는 **4칸 들여쓰기 메서드 조각**이라 단독 파싱은 IndentationError, `textwrap.dedent` 후 파싱 OK(의도된 형태 — 본문에 "클래스 안에 추가" 명시).

**커밋 메시지**: `git commit` 13건 전부 `type(scope): 한국어 설명 (Plan 6b Tn)` + 트레일러 2줄, Tn이 속한 Task 번호와 일치(T7 2건).

**타입·이름 정합성 대조(수정한 것)**:
1. **T7 Step 8 앵커 3건을 T6 최종 `main.py`에 맞춤** — (a) `from app.normalize import (…)`가 2줄 괄호 import라 "그 줄 바로 아래"가 괄호 안이 되므로 앵커를 `from app.writer import MetricsWriter, MutationBudgetExceeded`(단일 줄) 아래로 변경; (c) `def main(argv=None)` → T6 실제 시그니처 `def main(argv: list[str] | None = None) -> int:`(grep 앵커 `^def main(argv`); (e) `batch_time = _parse_batch_time(args.batch_time)`가 T6에서 `try:` 블록 안(8칸)이라 manual 분기는 그 **`try:` 줄 바로 위**(4칸)에 삽입하도록 변경. 앵커 확인 커맨드의 grep 5개도 같은 문자열로 교체.
2. **`python` → `python3` 통일(실행 커맨드 73줄)** — 개발 머신에 `python` 바이너리가 없다(T8 설계 해석·`which python` 부재). Run:/bash 블록의 pytest·`-c`·`- <<'PY'`·`py_compile`·`app.main --help` 호출을 `python3`로; 컨테이너 안(Dockerfile `CMD`, T9 `build_collect_command`, T10 `build_manual_command`), CI 워크플로(`setup-python` 러너), README 사용 예시는 `python` 유지. Global Constraints에 규칙 1문장 추가.
3. **header File Structure 주석** — `ServiceEntry` "10필드" → 11필드(dim 12컬럼 − `updated_at`); `test_main.py`에 "+T7 manual 모드 main() 8개 append", `test_manual.py`는 파서 전용으로 재표기; `datagen.py`에 `METRICS_GPU_TYPE` 추가.
4. **footer 완료 기준** — 커밋 "12개" → **13개**(T7 2커밋), type/scope 표기를 실제 커밋(`feat(mock)`·`feat(collectors-metrics)`·`ci(collectors-metrics)`·`docs(collectors-metrics)`)과 일치; draft PR 본문 "12커밋 목록" → 13; 마커 형식 검사 항목의 기대 문자열을 T6 단위 fixture(rows=5, `-k "batch_line or service_line or sigterm"`)에 맞춤(`rows=14`는 mock 3모델 기준 — T11 e2e에서 단언).
5. **설계 해석 16·19** — gitignore 패턴을 Plan 6a 실제 값 `*manual_metrics*.csv`로, 커밋 단위를 13개(T7 분리 사유 포함)로 정정.
6. **T8 Consumes env 목록**에 `COLLECTOR_API_VERIFY` 추가(T2 `load_config()`가 읽는 14번째 env — 매니페스트는 설정하지 않음).

**대조 후 수정 불필요로 확인한 것**: T6 `writer.anchor_exists/anchor_source_type/replace_batch(date, items)/sync_registry(entries)` ↔ T5 시그니처; T6 test `FakeWriter(anchors, anchor_types, raise_budget, raise_sync)`·`run_collection(..., writer=, register_dims=, emit_batch=, outcomes_sink=, started=)`·`_batch_line`·`_batch_reason`·`_batch_status` ↔ T7 `_run_manual`·`_manual_env`; `cfg.<field>` 사용 전부(T4·T5·T6) ↔ T2 `Config` 필드; `MetricsPayload` 9필드 ↔ T4·T7 생성 호출; `CollectError(Event.X, msg[, retry_after_s])` ↔ T4·T6; T9/T10 `build_job_spec` 인자 수(3/4) 정의 ↔ 호출; T10 볼륨 `[2] manual` ↔ T8 volumes `[0] endpoints [1] ca-bundle`; T11 `need_line` 고정 문자열 15건 ↔ T6 `_service_line/_batch_line` f-string 순서·T7 `COUNT_KEYS` 순서; T11 `ddl_test_dims.sql` 7컬럼 ↔ 기존 `collectors/token-usage/ddl/company/dim_token_service.sql`; T12 README `## ` 절 10개.

**리뷰 라운드 1: 28건 반영, 0건 기각**(지적 28건 중 중복 4쌍 포함 — 전부 반영). 반영 요지: (major) T7 manual-v0가 행 없는 `(date, service)`에 NODATA 앵커를 만들지 않도록 `build_payloads`·`_run_manual`·테스트·README Step 6 재작성 / T9 `--chain-mart`가 `--cronjob …-verify`·`--force`를 6c `rerun.py`로 전파하고 `--chunk-days` 1..`CHUNK_DAYS_MAX = 15` 검증 + 테스트 1개 추가(T9 21) / T12 `app.main --help` 실행 커맨드 `python3`; (minor) 사유 어휘 14종(`retryable`·`permanent_error` 추가)·`BATCH_RESULT status=<SUCCESS|NODATA|FAILURE>` / install.sh [4/7] 프리플라이트를 앱 계정 `--user/--password`로 실행([2/7] 생략 시 기존 Secret에서 읽음)·GRANT 누락 메시지 / README Step 8 복구 (iii) manual_load.py 재적재 / `--replace` 10:50 KST 창 문서화(T10 (e)·README Step 6) / `extra_top_keys` WARN은 §5.3 "무시"와의 의도적 편차로 기록(설계 해석 7·T3·README Step 9) / `-k manual` 9 passed·`--help` grep 앵커·install.sh 스모크 `rc=` 캡처(SIGPIPE 141 회피) / footer `*manual_metrics*.csv`·zero-diff 항목 분리(6a 병합 전제)·인터페이스 A `gpu_rows=0, serving_rows=0`·B 어휘·D 전파 규칙 / T7 memo 규칙명(`rejected`·`dup_custom_kept_first`) / T11 `gpu_hours_sum=331.0`·`git status --porcelain -- collectors tools .github .gitignore` / 6b·6c 활성 Job 게이트 폭 차이 명시(T9 (e)·README Step 7·인터페이스 D). 반영 후 재검사: 펜스 python 59·yaml 8·json 0 파싱 실패 0(T5 메서드 조각 2블록은 dedent 후 OK), 미정 표시어 grep 0건.
