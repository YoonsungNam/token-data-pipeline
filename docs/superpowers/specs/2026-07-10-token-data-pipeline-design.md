# token-data-pipeline 설계 문서

- 작성일: 2026-07-10 · **현재 버전 v1.13 (2026-07-14)** — 개정 이력은 §0
- 상태: 설계 확정 (사용자 승인) — 구현 진행 중 (Plan 1 mock-provider·Plan 2a collector core 머지)
- 참조: [gpu-data-pipeline 분석](../../gpu-data-pipeline-analysis.md), [token-usage-api-spec](https://github.com/YoonsungNam/token-usage-api-spec) (`token-usage-api.yaml` v1.1.0, 로컬 클론 `/home/mini/github/token-usage-api-spec`)

## 0. 개정 이력

| 버전 | 일자 | 내용 |
|---|---|---|
| v1.0 | 2026-07-10 | 최초 작성 |
| v1.1 | 2026-07-12 | 5렌즈 리뷰 확정 지적 22건 반영: 서비스 식별 정본 규칙(§5.0), 409/페이지네이션 원자성(§5.2·5.3), 부분 데이터 게이팅(§7.1 STEP 0), dim_user_org 이력화(§4.2), 물리 설계 표·분산 조인 표준(§4.0), BATCH_RESULT/SERVICE_RESULT 분리(§5.6), 소프트 데드라인(§5.2), 기준정보 확장(로스터·budget·anon 귀속, §6.1) 등 |
| v1.2 | 2026-07-12 | 수집 확장 모델 신설(§5.9 적재 계약): API 미제공 소스(object storage·스냅샷 API) 확장 경로 + 비용 파생 원칙(§4.3). 2-포크 반박 검증 반영 — 어댑터 프레임워크 기각(YAGNI), 소스 유형별 별도 모듈 + 문서 계약 방식 채택. summary에 is_derived, dim_service에 source_type 추가 |
| v1.3 | 2026-07-12 | 비용 2계층 확장 모델 신설(§4.4): Layer P(가격/차지백) / Layer C(GPU 타입·수행시간 기반 원가, PD분리 대응) 분리. 포크 검증 반영 — 동료 레포 실사(LLM 모델 개념 부재 확인), dim_model에 serving_type, 모델명 매핑 테이블 명시, 지표 이원화(총원가/실효원가), gpu_hours 할당 기준. 미결 12~15 추가 |
| v1.4 | 2026-07-12 | 최종 리뷰 라운드(통합 정합·보안/개인정보·용량·premortem·준비도) 확정 16건 반영: dim_service source_type 스코프 교체(§5.9-6), 이벤트 분류→정책 표(§5.2), 적재 완료 데드라인 계약 9조, 조회 계정·데이터 경계·로깅 계약(보안 4건), 파티션 재설계(상세=일 단위)·deadline 산식 교정, 정정 프로토콜(§8.4)·백업/DR(§8.5), DB 소유권·GRANT 테이블 레벨 한정, mart 시간 계약, stage 환경 전제(실사). 미결 16~20 추가 |
| v1.5 | 2026-07-13 | **조직 모델을 고정 3레벨(org_l1~l3)에서 가변 깊이 경로 배열(org_path Array)로 전환** — 사용자 확인: 실조직이 가변 깊이 위계. dim_user_org·mart 상세·org agg·미매핑 규칙·§9-1 협의 항목 일괄 개정. 서브트리 질의는 prefix 비교 표준 |
| v1.7 | 2026-07-13 | **§9-18 협의 확정** (사용자·소유자): ① fact DB **공유**(token_fact 폐기 — GRANT 테이블 레벨 유지), ② gpu_data 내 토큰 테이블은 **`*_token_*` 접두사 규칙**(dim_token_service 등 — 충돌 예방·소유 식별), ③ 정례 뮤테이션 예산 **제안: 일 150건/피크 창 80건**(동료 실측 일 ~155건 근거 — 소유자 최종 확인 대기). DDL(PR #3)·수집기 코드 반영 완료 |
| v1.6 | 2026-07-13 | **동료(클러스터 소유자) 리뷰 반영** (GitHub 이슈 #1 + 구두 확정): ① dim·view의 DB = **`gpu_data`**(동료 소유 공유 — token_data 폐지, §9-18 부분 확정), ② company 클러스터 **2샤드×2레플리카** 명시(§9-3 부분 해소), ③ §7.3 모니터링 방안 소유자 승인, ④ OOM 실경험 반영 — 페이지 배치 flush(MAX_BUFFER_ROWS)·Pod resources 명세(§5.1·§7.2), ⑤ §10에 DDL 초안 선리뷰 절차 추가 |
| v1.8 | 2026-07-13 | 정례 뮤테이션 예산 **확정 적용: 일 총량 150건 / 피크 창(02:00~03:00) 80건** (사용자 결정 — 소유자 사후 컨펌 진행 중, 이슈 #1). §4.0(c) 갱신 |
| v1.9 | 2026-07-14 | **Plan 3 mart DDL 초안 리뷰 반영**: §4.3에 summary 부재 시맨틱 신설(reported_\*/diff_\* Nullable — STEP 0 경고-후-진행 케이스의 거짓 대사 방지), created_by CHECK는 `_dist`에도 선언(비동기 INSERT 큐 정체 방지 — 24.8 실증), mart INSERT는 `_dist` 경유만(co-location 보장), org agg의 org_depth 물리 컬럼 제외(파생 가능 — YAGNI) |
| v1.10 | 2026-07-14 | **Plan 3 T8 체인 통합 검증 + 최종 스펙 동기화**: §4.3에 summary-only(NODATA) 서비스의 `agg_token_service_1d` 보강 행 규칙 추가(sums 0·reported 유지·diff=0−reported — detail-부재/summary-부재 쌍대 규칙, 커버리지 도메인과 agg 도메인 일치), §5.6에 mart BATCH_RESULT `missing_services` 쌍따옴표 규약(서비스명 공백 보호) + `coverage=N/M`·`rows_mart`·`rows_view`는 mart 전용 필드 명시, §7.1 멱등성 불릿에 INSERT `insert_deduplicate=0` 계약(서버측 보강은 accounts.sql) 명문화 |
| v1.11 | 2026-07-14 | **dim_token_user_org/dim_token_model 명명 확정 반영 (PR #8, Plan 4 T1)**: gpu_data의 사용자-조직·모델 단가 dim 2종을 `dim_token_*` 규칙으로 확정하고(§4.2 도입부의 §9-18 잔여협의 문구를 "적용 확정(PR #8)"로 해소), 이 이름을 스펙 전역(§3 아키텍처 다이어그램, §4.2 표·본문, §4.3, §4.4, §5.9, §6.1/§7.2 .gitignore 패턴 문구, §7.1 조인·검증 문구, §8.5 사본 등급 표, §9 미결사항)에 동기화. mart STEP 1(`app/steps.py`)의 실제 조인 대상도 `dim_token_user_org_dist`/`dim_token_model_dist`로 동일 개명 — 로직·컬럼은 불변, 테이블명 표기만 갱신 |
| v1.12 | 2026-07-14 | **① 계정 공유 결정 반영** (사용자·클러스터 소유자 합의, 이슈 #1): 전용 계정 3종(token_collector/token_mart/token_dashboard_reader)을 폐지하고 동료의 기존 운영계정 `mart`를 공유 — §7.2 계정·GRANT 경계 절 개정(accounts.sql 4파일에서 CREATE USER 제거·GRANT 대상을 `mart`로 전환, mart의 서버측 `ALTER USER ... SETTINGS insert_deduplicate=0`은 공유 계정 전역 영향이라 제거하고 클라이언트 설정으로만 유지). 잔여 리스크(대시보드=쓰기 권한 계정 사용, 실명 dim 접근)는 §9-1/§9-3 협의 항목에 추가. **② anon 핸들명 표기 결정**(사용자, 2026-07-14): anonymous 계정의 **비실명 핸들명**을
대시보드에 표기하기로 완화 — 이전 규칙("anon 행 `user_name` 강제 빈 문자열")을 폐지한다.
저장: `gpu_data.dim_token_user_org.user_name`에 anon 행도 비실명 핸들명 저장을 허용(실명
기입 금지는 사내 투입 리뷰에서 확인 — 도구는 실명 여부를 판별할 수 없음, §6.1 (2) 개정).
표기: `mart.token_usage_1d`/`gpu_data.view_token_usage_1d`에 `user_name` 컬럼 신설(user_type
다음 위치) — 값은 **anonymous 행만** dim의 date 기준 유효 핸들명, **identified/unclassified는
빈 문자열**(identified 실명 표기는 재식별·실명 노출 확대 우려로 별도 결정 — §9-1 보류 항목에
추가, §4.2/§4.3 개정) |
| v1.13 | 2026-07-14 | **company 검증 2단계 전략 실체화** (§7.2에 "company 검증 2단계 전략" 소절 신설): 1단계(격리) — `tools/gen_verify_ddl.py`가 `ddl/company/*.sql`에서 DB 한정자·ZK 복제 경로·Distributed 인자·GRANT 대상만 치환해 격리 DB 3종(기본안 `token_verify_fact`/`token_verify_dim`/`token_verify_mart`)·전용 계정(`token_verify`) 대상 `ddl/company-verify/*.sql`을 생성(커밋, `--check` CI 드리프트 가드) + k8s overlay `company-verify`(Secret/ConfigMap `-verify` 접미) + `install.sh <module> company-verify`(VM push 1단계 비활성) + `rerun.py --cronjob` 오버라이드. 절차 문서 `docs/operations/company-verify.md` 신설(리스크 표·설치 절차·성공 기준 체크리스트·카나리아 전환·철수). §9-19에 "격리 검증으로 해소 경로 확보" 갱신 |

## 1. 배경과 목적

사내 여러 AI 추론 서비스가 `token-usage-api` 계약(OpenAPI 3.1)을 구현하면, 중앙 수집기가 매일
각 서비스의 `GET /v1/usage`(사용자×모델 상세)와 `GET /v1/usage/summary`(서비스 합계)를 pull하여
ClickHouse에 적재하고, 기준정보와 조합해 대시보드용 테이블을 만든다.

구조·작업방식은 동료의 `gpu-data-pipeline`(분석 문서 참조)을 참조 모델로 하되,
토큰 데이터의 특성(이미 확정된 일별 집계를 pull)에 맞게 단순화한다.

## 2. 확정된 결정사항

| 항목 | 결정 | 비고 |
|---|---|---|
| 저장소 | 동료와 같은 ClickHouse 클러스터 | DDL/모니터링/운영 패턴 재사용 |
| 범위 | collectors + assets + mart 3계층 전부 | |
| 서비스 목록 | 레포 내 설정 파일(`endpoints.yaml`) → ConfigMap. **이 값이 서비스 식별의 정본(§5.0)** | 사내 URL은 레포에 커밋하지 않음 (§7.2) |
| 환경 | **stage(홈랩) + company 2단계** (dev/kind 없음) | 로컬 검증은 CI가 담당 |
| 수집 토폴로지 | **단일 수집기 CronJob이 전 서비스 순회** (usage-api-v1) | 서비스별 실패 격리. **API 미제공 소스는 적재 계약(§5.9)을 준수하는 별도 수집기 모듈로 확장** — 동료 assets/ 선례(소스별 독립 모듈) |
| 수집 경로 | **API→ClickHouse 직행**, 서비스 단위 합계만 VictoriaMetrics 게이지 push | per-user 데이터는 VM에 넣지 않음 |
| DB 배치 | **fact(공유 — §9-18 확정)** / **gpu_data(기준정보+view — 공유, 이슈 #1 확정)** / mart(집계 — 공유/전용 여부는 Plan 3 DDL 협의에서 확정) | fact 공유 확정(2026-07-13): token_fact 안 폐기. 안전장치 = **전 계정 테이블 레벨 GRANT**(§7.2) + `*_token_*` 네이밍. gpu_data 내 토큰 테이블은 **`*_token_*` 접두사 규칙**(dim_token_service, view_token_usage_* — 충돌 예방·소유 식별) |
| 대시보드 | 최종적으로 사내 대시보드가 `gpu_data`의 view table을 읽음. 사외(홈랩) 작업 중엔 Grafana 테스터 대시보드로 대체 | |

## 3. 아키텍처

```text
각 서비스 (token-usage-api 구현체)
  │  GET /v1/usage (cursor 페이지네이션) + GET /v1/usage/summary
  ▼  매일 02:00 KST, date=어제
collectors/token-usage ──► fact.raw_token_usage_1d          (사용자×모델 상세)
  │                    ──► fact.raw_token_usage_summary_1d  (서비스 보고 합계)
  │                    ──► gpu_data.dim_token_service           (endpoints.yaml — 자기 source_type 범위 교체)
  │                    ──► VictoriaMetrics                  (서비스 단위 일합계 게이지)
  │
assets/user-org      ──► gpu_data.dim_token_user_org  (전 직원 로스터, 이력형)
assets/model-catalog ──► gpu_data.dim_token_model     (model→provider·단가, 이력형)
  │
  ▼  매일 04:00 KST
mart/token-usage
  STEP 0: 서비스 커버리지 게이트 (enabled vs 당일 summary 존재)
  STEP 1: fact × gpu_data(dim) 조인 ──► mart.token_usage_1d + mart.agg_token_{service,org,model}_1d
  STEP 2: mart ──► gpu_data.view_token_usage_*   ◄── 대시보드가 읽는 최종 테이블
```

배치 간 의존성은 동료 방식대로 **cron 오프셋**으로 표현한다 (수집 02:00 → mart 04:00).
중앙 오케스트레이터는 사용하지 않는다. 수집 실패분의 사후 회수는 **rerun 체이닝**(§8.3)으로 잇는다:
collectors rerun 완료 후 동일 날짜의 mart rerun이 의무 절차이며, collectors `tools/rerun.py`가
완료 시 mart rerun 명령을 출력하고 `--chain-mart` 옵션으로 직접 트리거할 수 있다.

### 레포 구조 (모노레포, 모듈=배포단위)

```text
token-data-pipeline/
├── collectors/token-usage/     # main.py, api_client.py, clickhouse_client.py, vm_push.py,
│                               # config.py, endpoints.yaml(stage 예시), ddl/{stage,company}/,
│                               # k8s/(base+overlays), build.sh, install.sh, tools/rerun.py, tests/
│   # (향후) collectors/token-usage-snapshot/ · token-usage-storage/ —
│   #        적재 계약(§5.9) 준수하는 독립 모듈로 클론 생성, 소스 확정 시
├── assets/
│   ├── user-org/               # csv_to_dim_user_org_insert.py (SQL 생성), ddl/, (2단계: sync CronJob)
│   └── model-catalog/          # ddl/ + seed_dim_token_model.sql (멱등 시드)
├── mart/token-usage/           # batch.py, mart.py, ddl/, k8s/, build.sh, install.sh,
│                               # tests/, tools/rerun.py, warning_messages.md
├── tools/
│   ├── mock-provider/          # 스펙 구현 가짜 서비스 (FastAPI) + k8s + 시나리오 옵션
│   └── delete_data.py          # (date범위[, service]) fact 삭제 + user_id 축 파기 모드 (§8.3)
├── docs/
│   ├── monitoring/             # grafana_dashboard_token_usage.json + 가이드
│   ├── operations/             # rerun.md (체이닝·VM 정정 절차 포함)
│   └── superpowers/specs/      # 본 문서
└── .github/workflows/          # CI (모듈별 path 필터)
```

각 모듈은 자기 디렉터리에 실행 코드·DDL·배포 스크립트·재수행 도구·README를 자기완결적으로 보유한다.
공용 유틸은 구현 시 3회 이상 중복되면 공용 패키지로 승격한다.

## 4. 데이터 모델

### 4.0 물리 설계 공통 (전 테이블)

모든 테이블은 `<이름>_local`(ReplicatedMergeTree 계열) + `<이름>_dist`(Distributed) 쌍.
시간 컬럼은 `DateTime('Asia/Seoul')`, 문자열은 NOT NULL(빈 문자열 정규화),
저카디널리티 컬럼은 `LowCardinality(String)`. **파티션 단위는 테이블별로 확정**(아래 표) —
(date, service) 단위 delete-then-insert가 **정례** 경로이므로, 행이 많은 상세 테이블은 일 단위
파티션으로 뮤테이션 재작성 범위를 "월 파티션 병합 파트 전체 × 서비스 수"에서 "해당 일자 파트"로
축소한다 (25개월 ≈ 760 파티션, 허용 범위). 소행수 테이블은 월 단위 유지.

**테이블별 물리 설계 표** (리뷰 #20·#21 + v1.4 파티션 재설계 반영):

| 테이블 | PARTITION BY | ORDER BY | Distributed 샤딩키 | 비고 |
|---|---|---|---|---|
| `fact.raw_token_usage_1d` | **toYYYYMMDD(date)** | `(date, service, user_type, user_id, model)` | `cityHash64(service, user_id)` | service_group은 일반 컬럼 |
| `fact.raw_token_usage_summary_1d` | toYYYYMM(date) | `(date, service)` | `cityHash64(service)` | 소행수 |
| `gpu_data.dim_*` | (파티션 없음/단일) | 각 키 | `rand()` | 소용량 — 조인은 GLOBAL(아래) |
| `mart.token_usage_1d` | **toYYYYMMDD(date)** | `(date, service, user_type, user_id, model)` | `cityHash64(service, user_id)` | raw와 co-location |
| `mart.agg_token_*_1d` | toYYYYMM(date) | `(date, <grain 키>)` | `cityHash64(service)` 또는 grain 키 해시 | 소행수. org agg는 grain 키가 `org_path Array(String)` — ORDER BY에 Array 허용, 샤딩키는 `cityHash64(arrayStringConcat(org_path, '>'))` |
| `gpu_data.view_token_usage_1d` | **toYYYYMMDD(date)** | mart 상세 동일 | mart 상세 동일 | |
| `gpu_data.view_token_usage_*_1d` (agg 3종) | toYYYYMM(date) | mart agg 동일 | mart agg 동일 | |

**정례 뮤테이션 절감 규칙** (공유 클러스터 배려 — 동료가 part 폭증·뮤테이션으로 하드닝을 겪은 클러스터임):
(a) 정상 일일 경로는 DELETE 전에 해당 (date, service) 행 존재를 SELECT로 확인, 없으면 DELETE 스킵
(첫 수집의 no-op 뮤테이션 제거 — 단일 작성자 CronJob + `concurrencyPolicy: Forbid` 전제라 경합 없음).
(b) 다중 서비스 rerun은 `DELETE WHERE date=D AND service IN (...)` 배칭으로 뮤테이션 수를 O(서비스)→O(1)로.
(c) 정례 뮤테이션 예산 — **확정: 일 총량 150건 / 피크 창(02:00~03:00) 80건** (동료 파이프라인 실측: snap 시간당 6건 + mart 일 11건 ≈ 일 155건 기운영, 토큰 추가분 ≈ 일 68건 — §9-18. 2026-07-13 확정 적용, 소유자 사후 컨펌 진행 중: 이슈 #1).

**분산 조인 표준**: mart STEP 1의 `fact × dim` 조인은 **`GLOBAL LEFT JOIN`을 표준**으로 한다
(dim 3종은 소용량이라 브로드캐스트 비용 무시 가능 — 동료 mart/aip에서 검증된 패턴.
dim이 커지면 전 샤드 전량 적재 방식으로 전환을 검토). 이 표준은 ddl과 §7.1 mart SQL,
stage RUNBOOK의 Distributed 검증 항목에 일관 반영한다.

### 4.1 fact DB (수집 원본)

**`fact.raw_token_usage_1d`** — grain: `date × service × user_id × user_type × model`

| 컬럼 | 타입 | 비고 |
|---|---|---|
| date | Date | 사용량 발생 일자 (KST) |
| service_group | LowCardinality(String) | **정본 = endpoints.yaml 값** (§5.0) |
| service | LowCardinality(String) | **정본 = endpoints.yaml 값** (§5.0) |
| reported_service_group | LowCardinality(String) | 응답 원문 보존 (감사·계약 위반 추적) |
| reported_service | LowCardinality(String) | 응답 원문 보존 |
| user_id | String | **unclassified는 `''`로 정규화** (스펙의 null → CH ORDER BY 키 Nullable 회피) |
| user_type | LowCardinality(String) | identified / anonymous / unclassified |
| model | LowCardinality(String) | `unknown` 허용 (계약 표준 값) |
| input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, requests | UInt64 | 캐시 필드 생략 시 0 |
| generated_at | DateTime('Asia/Seoul') | **마지막 페이지의 generatedAt** (§5.3 페이지 불변성 검사 통과 값) |
| collected_at | DateTime('Asia/Seoul') | 수집기 적재 시각 |

TTL: `date + INTERVAL 25 MONTH` (미결 §9-7).

**`fact.raw_token_usage_summary_1d`** — grain: `date × service`

summary 응답 원본: 토큰 5필드 + `distinct_users` UInt32 + `distinct_identified_users` Nullable(UInt32)
(optional 필드) + `reported_service_group/reported_service` + **`is_derived` UInt8** + `generated_at`, `collected_at`.
detail 합과의 정합성 검증 기준이자 mart STEP 0 커버리지 게이트의 기준(**NODATA인 서비스도
summary 행은 반드시 적재** — §5.2).

`is_derived=1` = 소스가 summary를 제공하지 않아 **detail에서 합산 파생한 행**(§5.9 계약 3조).
파생 summary는 (a) Σdetail vs summary 검증을 스킵(자기 자신 비교 방지 — SERVICE_RESULT에
`verify=skipped_derived`), (b) `agg_token_service_1d`의 reported_* diff 컬럼을 0이 아닌 **NULL**로,
(c) §5.5 VM `reported_*` 게이지 push를 생략한다 ("보고값"이 아니므로).

### 4.2 gpu_data DB (기준정보 + view — 동료 소유 DB 공유, v1.6)

동료 확인(이슈 #1): 기준정보·대시보드용 정제 테이블은 기존 **`gpu_data`** DB에 둔다.
기존 테이블(dim_project_info/dim_unit_environment/dim_project_unit/dim_division_mapping 등)과
이름 충돌 없음을 확인함. 공유 DB이므로 우리 테이블 GRANT는 테이블 레벨 한정(§7.2)이며,
`dim_token_service`·`dim_token_user_org`·`dim_token_model` 등 범용 이름 충돌 방지를 위한
**`dim_token_*` 규칙 적용 확정(PR #8)** — §9-18 잔여 협의였던 접두사 필요 여부는 이 확정으로 해소.

| 테이블 | grain / 컬럼 요지 | 관리 주체 |
|---|---|---|
| `dim_token_service` | service — service_group, service, base_url, enabled UInt8, **source_type** LowCardinality(String) DEFAULT 'usage-api-v1', note, updated_at | **각 수집 모듈이 자기 source_type 범위만 원자 교체** (DELETE WHERE source_type='<자기 유형>' → INSERT, §5.9 계약 6조 — 전 테이블 교체 금지: 타 모듈 등록분 삭제 방지). 리뷰 #12의 "enabled=false 유지·항목 제거 금지"는 모듈별 범위 안에서 동일 적용 |
| `dim_token_user_org` | **(user_id, effective_from)** — user_name, **org_path Array(String)**(최상위→말단, 가변 깊이 — 예: `['DS부문','반도체연구소','공정연구팀','소자파트']`), org_depth UInt8, is_active UInt8, updated_at | assets/user-org |
| `dim_token_model` | (model, effective_from) — provider, **serving_type**(internal\|external), input/cache_read/cache_creation/output 단가(USD per MTok), currency, note | assets/model-catalog 시드 SQL. serving_type은 §4.4 Layer C 대상 판별("원가 NULL"이 미수집인지 대상외인지 구분) |
| `dim_budget` *(2단계, 선택)* | (scope_type: org\|service_group, scope, month) — budget_usd | 미결 §9-8 |
| `view_token_usage_1d` | mart.token_usage_1d와 동일 컬럼(`user_name` 포함, v1.12 — **anonymous 행만** 비실명 핸들명 표기) | mart STEP 2 |
| `view_token_usage_service_1d` / `_org_1d` / `_model_1d` | 각 agg와 동일 컬럼 | mart STEP 2 |

- **`dim_token_user_org`는 "사용자 매핑"이 아니라 전 직원 로스터**(사용 이력 없는 직원 포함)를 목표로 한다
  → 부서 정원(headcount) 파생 가능 → **도입률·1인당 사용량** 계산 가능 (리뷰·관점 분석 반영).
  `(user_id, effective_from)` 이력 키(dim_token_model과 동일 패턴, 리뷰 #17)로 조직 이동을 이력화하고,
  mart STEP 1은 **date 기준 유효 행**(`effective_from <= date`인 최신 행)을 조인한다 —
  rerun이 실행 시점과 무관하게 결정적(deterministic)이 된다.
  **anonymous 계정도 매핑이 제공되면 로스터에 포함**해 부서 귀속한다 (§6.1).
- **anon 비실명 핸들명 대시보드 표기 (v1.12, 사용자 결정 2026-07-14)**: 이전 "anon 행
  `user_name` 강제 빈 문자열" 규칙을 완화 — `dim_token_user_org.user_name`에 비실명
  핸들명 저장을 허용하고(§6.1 (2)), `view_token_usage_1d`(및 mart.token_usage_1d)에
  `user_name` 컬럼을 신설해 **anonymous 행만** 표기한다. identified/unclassified는
  빈 문자열 — identified 실명을 동일 경로로 노출할지는 §9-1 보류 항목.
- `dim_token_model`의 시드에는 **`model='unknown'` 행(전 단가 NULL, note='계약 표준 값 — 단가 산정 불가')을
  포함**한다 (리뷰 #15) — "dim_token_model 미등록 WARN"이 unknown으로 상시 발화해 경보가 무력화되는 것을 방지.
  이 WARN의 의미는 "단가 등록이 필요한 진짜 신규 모델"로 warning_messages.md에 명시.
- `enabled=0`인 서비스는 수집 대상에서 제외 — flag 게이트 패턴.
  **폐기된 서비스는 endpoints.yaml에서 제거하지 않고 `enabled: false`로 유지**한다 (리뷰 #12 —
  범위 교체 방식에서 dim_service 이력 유실과 잔존 데이터 조인 고아를 방지).
- view/mart 테이블의 `created_by` LowCardinality(String)는 **DEFAULT 없음** (리뷰 #22).
  공유 테이블 쓰기 계약: **모든 작성자는 INSERT 시 created_by를 명시 삽입**(본 파이프라인은
  'token-pipeline' 고정). DDL에 `CONSTRAINT check_created_by CHECK created_by != ''`를 두어
  컬럼 생략을 INSERT 에러로 조기 검출하고, `validation.sql`에 "created_by='' 또는 예상 외 값 검출" 쿼리 상비.
- view table의 최종 컬럼 계약은 사내 대시보드 협의로 확정 (미결 §9-1 — org 롤업 기본 표시 깊이,
  anonymous 버킷 표시, 불완전 데이터 마커 포함). 확정 전에는 mart와 동일 스키마로 운영.

### 4.3 mart DB (1차 집계)

| 테이블 | grain | 내용 |
|---|---|---|
| `mart.token_usage_1d` | date × service × user × model | raw + `user_name`(표기용, v1.12 — **anonymous 행만** dim의 date 기준 유효 핸들명, identified/unclassified는 빈 문자열) + 조직(**org_path Array** + 편의 파생 `org_top`=org_path[1], `org_leaf`=말단) + `total_input_tokens`(=input+cache_read+cache_creation) + `cost` Nullable(Float64) + created_by |
| `mart.agg_token_service_1d` | date × service_group × service | 토큰 합계, requests, distinct_users(detail에서 uniqExact, user_id≠''), reported_* 컬럼(=`fact.raw_token_usage_summary_1d`에서 조인한 서비스 보고값)과 차이 컬럼 |
| `mart.agg_token_org_1d` | **date × org_path (말단 경로 단위)** | 조직별 합계 + distinct_users + **headcount**(로스터에서 해당 경로 소속 정원) + **adoption_rate**. 상위 레벨 롤업은 쿼리 시 `arraySlice(org_path, 1, k)` GROUP BY — 조직 수 × 일 수준의 소행수라 사전 롤업 불요. **서브트리 질의 표준 = prefix 비교**: `arraySlice(org_path, 1, length(P)) = P` (조직명 전역 유니크에 의존하지 않음 — 부서장 "내 하위 전체" 뷰) |
| `mart.agg_token_model_1d` | date × model × provider | 모델별 합계 + 서비스 수 |

- **anon 비실명 핸들명 표기 (v1.12)**: `mart.token_usage_1d`의 `user_name`(user_type 다음 위치)은
  `if(user_type = 'anonymous', dim_token_user_org.user_name, '')` — **anonymous 행만** dim의
  date 기준 유효 행(`effective_from <= date` 최신, argMax) 핸들명을 표기하고, identified/
  unclassified는 항상 빈 문자열이다. identified 실명을 동일 경로로 노출할지는 재식별·실명
  노출 확대 우려로 **별도 결정 대상**(§9-1 보류 — 위 anonymous 표기 확정과 무관).
- `cost` = Σ(토큰별 단가 × 양) / 1e6. date 기준 유효 단가(`effective_from <= date` 최신 행) 사용.
  dim_token_model 미등록 모델은 cost NULL + 모델명 집합 CHECK WARN (`unknown`은 시드 포함으로 자연 제외).
- **비용은 파생 데이터 (확장 원칙)**: mart에 토큰 4종 수량, dim_token_model에 단가 4종+이력이 있으므로
  **유형별 비용 분해·캐시 절감액(cache_read × (input단가−cache_read단가))은 물리 컬럼 없이 쿼리로
  언제든 계산 가능**하다. 분해 물리 컬럼이 필요해지는 조건(대시보드 성능·§9-1 계약 확정)이 오면
  `migrate_add_*` + mart rerun으로 추가한다 — v1.1의 effective_from 이력 덕에 재계산이 결정적이라
  안전. 단가 소급 정정도 동일 경로(dim 이력 정정 → 기간 rerun). 통화/환율은 §9-5 미결 상속.
  **물리 컬럼 범위**: mart 상세·agg·view 모두 `cost`(합계, Nullable)만 보유 — 분해 컬럼은 보류.
  미등록 모델은 cost NULL (분해 도입 시에도 "미등록=전 비용 컬럼 NULL"로 단순화).
- agg의 소스는 `mart.token_usage_1d`로 통일해 조직 조인 결과가 어긋나는 것을 방지한다
  (예외: `agg_token_service_1d`의 reported_* 컬럼만 `fact.raw_token_usage_summary_1d`를 조인).
- **summary 부재 시맨틱 (v1.9 — Plan 3 DDL 리뷰)**: `agg_token_service_1d`의 reported_\*
  컬럼은 **Nullable** — STEP 0가 경고 후 진행하는 "summary 행 없는 서비스"(detail만 존재)는
  reported_\*·diff_\* 전부 NULL로 적재한다. 비-Nullable이면 LEFT JOIN 미스가 "보고값 0"으로
  위장되고 diff가 거짓 대사 불일치를 기록한다. (§4.1의 is_derived=1 diff NULL 규칙과 별개
  케이스 — is_derived는 summary 행이 있되 파생인 경우, 이 규칙은 행 자체가 없는 경우.)
- **summary-only(NODATA) 보강 (v1.10)**: summary만 있고 detail 0행인 서비스는
  `agg_token_service_1d`에 sums 0·reported 유지·diff=0−reported(is_derived=0일 때) 행으로
  노출 — 커버리지 도메인과 agg 도메인의 일치. detail-부재와 summary-부재의 쌍대 규칙.

### 4.4 비용 2계층 확장 모델 — Layer P(가격) / Layer C(원가) (확장 슬롯)

**배경**: 모델은 여러 GPU 타입에서 수행될 수 있다 (예: **PD분리** — prefill/decode를 서로 다른
GPU 타입 인스턴스에 분리 서빙). 사내 서빙 모델의 "원가"는 토큰 단가표가 아니라 **GPU 타입별
수행 시간**에서 나온다. 그러나 token-usage-api 계약에는 GPU 정보가 전혀 없다(소비 측 계약 —
누가 어떤 모델로 몇 토큰). 원가는 **공급 측(서빙 인프라) 도메인**이므로 별도 수집 경로가 필수다.
동료 레포 실사로 확인: `fact.raw_gpu_util_1m`의 태그는 host/gpu_index/**gpu_model(GPU 하드웨어
모델명)**뿐이고 레포 전체에 LLM 모델 개념이 없다 → **서빙 플랫폼 메타(model→GPU 할당 이력)가
반드시 필요**하다.

**Layer P — 가격/차지백** (v1.2 구현 범위, §4.3 그대로): 토큰 수량 × dim_token_model 단가.
소비 측 관점, 외부 API 모델은 실지불액. **차지백/청구는 이 계층만 사용한다.**

**Layer C — 원가 분석** (확장 슬롯, `dim_token_model.serving_type='internal'` 모델 전용):
**차지백에 사용하지 않는다** — 유휴 GPU 원가를 소비자에게 배분하는 논쟁을 원천 회피하고,
원가는 공급 효율(가동률·마진) 분석 용도로 한정한다. 테이블 스케치 (DDL은 케이스 확정 시):

| 스케치 | grain / 내용 |
|---|---|
| `fact.model_gpu_usage_1d` | date × model × **gpu_type [× phase(prefill\|decode)]** × gpu_hours. gpu_hours는 **할당(occupancy) 기준** — 전용 할당·MIG 슬라이스(gpu_type 세분) 지원, 시분할 다중 모델 공유는 범위 외(§9-15). 소스: 서빙 플랫폼 메타 + 동료 `fact.raw_gpu_util_1m`(읽기 전용, 보정/검증) |
| `gpu_data.dim_gpu_cost` | (gpu_type, effective_from) × cost_per_gpu_hour. gpu_type은 동료의 gpu_model(하드웨어) 체계와 매핑 — 동료 `dimension.gpu_model_quota_info`(GPU 모델별 quota 단가 선례)와의 관계 확인 §9-14 |
| `gpu_data.dim_model_serving_map` | token-usage-api의 `model` 문자열 ↔ 서빙 플랫폼 식별자(deployment명/모델 경로). **§5.0과 동형의 정본 문제** — 이 매핑 없이는 (date, model) 결합이 성립하지 않음 (§9-13) |
| `mart.model_cost_1d` | date × model — **지표 이원화**: ① `total_cost`(Σ gpu_hours×단가 — 토큰과 무관한 총원가) ② `effective_cost_per_mtok`(총원가 ÷ 그날 모델 토큰 처리량). 저사용일의 $/MTok 급등은 버그가 아니라 **가동률 신호** — 저활용 경고와 함께 표기 |

- **PD분리 안분**: phase별 원가를 prefill→input(+cache) 토큰, decode→output 토큰에 각각 귀속 —
  PD분리 배포에서는 자연스럽게 해결된다. 통합 배포의 input/output 안분 가중치는 자의적이므로 §9-15.
- **결합**: 두 Layer는 **(date, model)로만 결합**. 대시보드: 모델별 가격 vs 원가(마진), 실효 단가
  추이, GPU 타입 믹스, 가동률. 서비스/부서별 "원가 참고 배분"은 토큰 비중 안분으로 가능하되
  분석 용도로만(차지백 아님).
- **소유권**: 이 확장은 본 레포 소관 — 동료 레포(RESPONSIBILITIES 기준)에는 서빙 메타에 해당하는
  계층이 없고 소비자가 이쪽이다. 동료 테이블은 같은 ClickHouse에서 **읽기 전용 입력**으로만 조인.

## 5. collectors/token-usage

### 5.0 서비스 식별 정본(canonical) 규칙 (리뷰 #1 — HIGH)

계약상 응답의 serviceGroup/service는 자유 문자열이며 provider가 표시명을 바꿀 수 있다.
**endpoints.yaml의 service/serviceGroup이 유일한 정본**이고, 다음 전부에 일관 사용한다:
멱등 DELETE 술어, fact 적재 컬럼(service_group/service), agg의 reported_* 조인 키, VM 레이블,
ORDER BY·샤딩 키. 응답 원문은 `reported_service_group/reported_service` 컬럼에 보존만 한다.
**응답값 ≠ 설정값이면 CHECK WARN** (BATCH_RESULT에 카운트 노출), 페이지 간·detail↔summary 간
서비스 명칭 불일치도 같은 검증에 포함한다.

### 5.1 정상 흐름 (CronJob 매일 02:00 KST)

1. `target_date` = batch_time(기본 now) − 1일 (KST). batch_time은 ISO8601 위치 인자.
2. endpoints.yaml 로드 → `gpu_data.dim_token_service`의 **자기 source_type 범위 교체**
   (검증 후 `DELETE WHERE source_type='usage-api-v1'` → INSERT, mutations_sync=2).
3. `enabled` 서비스 순회 — **서비스별 try/except 격리**:
   1. `GET /v1/usage/summary?date=<target_date>`
   2. `GET /v1/usage?date=...&limit=1000` → `nextCursor` 루프. cursor 사용 중 date/limit 고정(스펙 의무).
      **페이지 상한 `MAX_PAGES`(env, 기본 200) 도달 = 부분 적재 금지, 해당 서비스 FAILURE** —
      delete-then-insert 이전에 중단하므로 기존 데이터 보존, BATCH_RESULT에 pages=와 사유 기록 (리뷰 #6).
      **페이지 간 불변성 검사**(§5.3) 수행.
   3. 행 정규화·검증 (§5.4)
   4. 정합성: Σdetail vs summary 비교 → 불일치 시 `CHECK WARN` (적재는 진행)
   5. `(date, service)` 단위 멱등 적재: 기존 행 존재 확인(SELECT) 후 있을 때만
      `ALTER TABLE <local> ON CLUSTER DELETE WHERE date=... AND service=<정본>`을
      **`mutations_sync=2` 설정으로 실행**(전 레플리카 완료까지 동기 대기 — 동료 collector 방식,
      폴링·추가 GRANT 불요, 리뷰 #8; no-op 뮤테이션 스킵은 §4.0 절감 규칙) → INSERT
      (detail·summary 모두, `insert_distributed_sync=1`). DELETE 직전 기존 세대의 요약을
      감사 이력에 보존(§8.4 정정 프로토콜). **NODATA(빈 records)여도 summary 행은 반드시 적재** —
      mart STEP 0 커버리지 게이트의 기준 (리뷰 #16).
      적재 시퀀스 시작 전 **잔여 시간 < 적재 시퀀스 예산(§5.2, 12분)** 이면 시작하지 않고
      FAILURE 처리 (DELETE 후 INSERT 전 kill로 인한 유실 방지 — 판단 기준을 수치로 고정).
   6. VictoriaMetrics push: 서비스 단위 합계 게이지 (§5.5)
   7. 서비스별 결과 로그: **`SERVICE_RESULT` 마커** (§5.6)
4. 전체 종료: **`BATCH_RESULT` 최종 1줄** (§5.6). 실패 서비스 ≥1 → `exit 1`
   (성공 서비스 적재는 유지 — 부분 실패 허용). **SIGTERM 수신 시에도 요약 BATCH_RESULT를 출력**하는
   핸들러를 둔다 (리뷰 #14 — deadline kill 시 마커 유실 방지).

**메모리 규칙 (v1.6 — 소유자의 OOM 실경험 반영, 이슈 #1)**: 서비스당 전체 페이지를 메모리에
전량 버퍼링하지 않는다. `MAX_BUFFER_ROWS`(env, 기본 20,000행 — 동료 #133과 동일 상한) 도달 시마다
INSERT flush한다: DELETE는 첫 flush 전 1회만 실행하고 이후 배치 INSERT를 반복 — 도중 실패 시
해당 (date, service)는 FAILURE → rerun이 전체 교체하므로 부분 상태가 영구화되지 않는다.
페이지 불변성 검사(§5.3)와 Σdetail 정합 검증(3-4)은 스트리밍 누적 집계로 수행(전량 버퍼 불요).
Pod resources는 §7.2에 명세.

**재수집 = 기본 동작**: 적재가 항상 delete-then-insert이므로 별도 `--purge` 플래그가 없다.
`main.py --from <d1> --to <d2> [--service <name>]`으로 과거 구간·특정 서비스만 재실행한다.
**collectors rerun 후에는 동일 날짜의 mart rerun이 의무** (§3, §8.3).

### 5.2 HTTP 에러 처리 매트릭스 (usage-api-v1의 이벤트 번역표)

수집 오케스트레이션 정책은 **공통 이벤트 분류 → 정책 표 1벌**로 정의한다 (모든 소스 모듈 공통 —
수치는 이 표에만 존재하고, 아래 HTTP 매트릭스와 §5.9 케이스 지침은 이 표를 참조하는 번역표다.
신규 소스 모듈은 자기 신호의 번역표만 정의하면 된다):

| 분류 | 수집기 동작 | 공통 예산 | SERVICE_RESULT status | exit 영향 |
|---|---|---|---|---|
| NOT_READY | 대기열 끝으로 후송, 대기 경과 후 **전체 재시작**으로 재방문 | 서비스당 누적 대기 30분 / 소프트 데드라인 | 초과 시 FAILURE | 실패 시 1 |
| RETRYABLE | 대기(캡 300s) 또는 지수 백오프(5s→25s→125s) 후 재시도 | 최대 3회 | 초과 시 FAILURE | 실패 시 1 |
| PERMANENT_ERROR | 재시도 없이 즉시 실패 (우리/소스 결함 신호) | — | FAILURE | 1 |
| RETENTION | 회수 불가 — **실행 컨텍스트 분기**: 일일 정기=계약 위반 신호, 명시적 재수집=정상 예상 | — | 정기: FAILURE / 재수집: SKIPPED | 정기: 1 / 재수집: 0 |
| EMPTY | 정상 처리 (0행) — summary 행은 적재 | — | NODATA | 0 |
| INVARIANT_BROKEN | 수집분 폐기 후 처음부터 재시작 | 최대 2회 | 초과 시 FAILURE | 실패 시 1 |

**전역 소프트 데드라인과 적재 시퀀스 예산** (리뷰 #14, v1.4 산식 교정): Job 경과 **50분**을
소프트 데드라인으로 두고, 모든 대기·백오프·새 서비스 착수 전에 체크한다. 초과 시 남은 서비스를
FAILURE로 마킹하고 **정상 경로로 종료**해 최종 BATCH_RESULT 출력을 보장한다.
`Retry-After`는 **`min(Retry-After, 300s)` 캡**(초과 값 수신 시 WARN).
**적재 시퀀스 예산 = 12분** (mutations_sync=2 클라이언트 타임아웃 300s × 2테이블 + INSERT/검증 여유 —
이 타임아웃을 명시적 상한으로 계약에 포함). `activeDeadlineSeconds: 4320` = 소프트 데드라인(50분) +
적재 시퀀스 예산(12분) + 종료 마진(10분) — 산식과 값을 일치시킴(구 3600은 예산이 0이 되는 자기모순).

| 응답 | 의미 | 수집기 동작 |
|---|---|---|
| 409 `data_not_ready` | 집계 미확정 | 해당 서비스를 **대기열 끝으로 미루고 다음 서비스 먼저 처리**, `Retry-After`(캡 300s) 경과 후 재방문. **재방문 = §5.1-3-1부터 전체 재시작** — 이전 방문의 summary·부분 페이지는 폐기, cursor 재개 금지 ((date,service)는 원자 스냅샷 단위, 리뷰 #2). 서비스당 누적 대기 30분 또는 소프트 데드라인 초과 시 FAILURE (다음날 rerun+mart 체이닝으로 회수) |
| 페이지네이션 도중 409 | 계약의 '페이지네이션 중 불변' 위반 신호 | WARN 카운트 후 해당 서비스 detail **전체 재시작**, 대기 시간은 서비스당 예산에 합산 (리뷰 #2) |
| 429 `rate_limited` | 호출 제한 | `Retry-After`(캡 300s) 대기 후 재시도, 최대 3회 |
| 500 / 503 / 네트워크 오류 | 일시 장애 | 지수 백오프(5s→25s→125s) 최대 3회, 초과 시 FAILURE |
| 400 | 잘못된 요청 | **재시도 없이 즉시 FAILURE** — 우리 쪽 결함 신호. 단 `invalid_cursor`는 cursor 없이 1회 처음부터 재시작 |
| 404 `data_not_retained` | 보존 기간 초과 | **실행 컨텍스트로 분기** (리뷰 #4): 일일 정기 실행(target=어제)에서는 계약 위반 신호이므로 **FAILURE**(즉시 조사); 명시적 과거 재수집(`--from/--to`)에서는 WARN + SKIP (보존 초과가 정상 예상되는 유일한 경로) |
| 200 + 빈 records | 사용량 실제 0 | 서비스 status=NODATA (성공) — **summary 행은 적재** |
| 페이지 상한 도달 | 규모 초과/무한 루프 | 부분 적재 금지, FAILURE (리뷰 #6) |

### 5.3 페이지네이션 불변성 검사 (리뷰 #3 — HIGH)

- 매 페이지의 `(serviceGroup, service, date, generatedAt)`이 **첫 페이지와 다르면** 해당 서비스의
  detail 수집분을 폐기하고 cursor 없이 처음부터 재시작 (최대 2회, 초과 시 FAILURE — §5.2에 준함).
- 저장하는 `generated_at`은 **마지막 페이지의 generatedAt**.
- §5.4의 논리 키 중복 SUM 병합은 **전 페이지 generatedAt이 동일해 불변성이 확인된 수집분에만** 적용 —
  provider가 논리 키를 쪼개 보낸 경우(SUM 정당)와 페이지네이션 불안정으로 같은 행이 재등장한 경우
  (SUM=이중집계)를 구분한다.

### 5.4 행 정규화·검증 규칙

- `userId: null` → `user_id=''` (user_type=unclassified와 함께). 스펙의 `userType`↔`userId` 제약 위반 행은
  **행 단위 거부 + WARN 카운트**.
- 캐시 토큰 필드 생략 → 0. 음수/타입 위반 → 행 거부 + WARN.
- 논리 키 `(user_id, user_type, model)` 중복 → 계약 위반 WARN 후 **SUM 병합** (§5.3의 불변성 확인 전제).
- 응답 serviceGroup/service ≠ 설정값 → CHECK WARN (§5.0).
- `generatedAt` KST 패턴 위반 → WARN만 (적재 진행).
- 거부 행 수는 SERVICE_RESULT `rejected=` 필드로 노출.

### 5.5 VictoriaMetrics push (합계만)

- 대상: 서비스 단위 일합계만 — `token_usage_daily_{input,cache_read,cache_creation,output}_tokens`,
  `token_usage_daily_requests`, **`token_usage_daily_reported_distinct_users`**, 레이블 `{service_group, service}`.
  **값의 소스는 summary 보고값으로 통일** (리뷰 #5) — 게이지 목록이 summary 응답 필드와 1:1 대응,
  의미는 "서비스가 보고한 값(검증 전)". mart의 detail 기반 `distinct_users`와 이름 수준에서 구분.
- **distinct_users 게이지는 비가산 지표** — 서비스 간/serviceGroup 단위 `sum()` 금지 (계약이 교차 집계를
  금지). 과제·조직 단위 고유 사용자 수는 mart에서만 계산. 이 주의를 §7.3 모니터링 가이드에 명시.
- 방식: `POST /api/v1/import/prometheus`, **타임스탬프 = target_date 23:59:59 KST**.
- push 실패는 WARN — ClickHouse 적재가 원천이므로 배치를 실패시키지 않는다.
- **rerun(`--from/--to`)에서는 VM push 기본 생략** (`--push-vm` 옵트인, 리뷰 #19) — VM은 동일 timestamp
  재push 시 하향 정정이 반영되지 않음(dedup이 큰 값 유지). VM 게이지는 "최초 수집 시점 기준 근사 추이"이고
  정정의 원천은 ClickHouse. 하향 정정을 VM에 반영해야 하면 delete_series API 후 재push (rerun.md 부록).

### 5.6 로그 마커 규약 (리뷰 #9·#18 — 기존 대시보드 '무수정 편입' 성립 조건)

기존 batch_result 대시보드의 LogsQL은 **1 실행 = 1 마커 라인**을 전제하므로:

- **`BATCH_RESULT`는 job당 최종 1줄만**:
  `BATCH_RESULT status=<S> module=token-usage services_ok=N services_failed=M services_skipped=K rows=... elapsed=...`
  status 집계 규칙: 실패 서비스 ≥1 → FAILURE, 전 서비스 NODATA → NODATA, 그 외 SUCCESS.
  SKIP(재수집 404)은 exit code에 영향 없음(0).
- **서비스별 결과는 별도 마커 `SERVICE_RESULT`**:
  `SERVICE_RESULT status=SUCCESS|NODATA|SKIPPED|FAILURE module=token-usage service=<정본> source_type=usage-api-v1 rows= pages= warn= rejected=`
  (`source_type=` 필드는 소스 유형별 지연·실패 특성을 모니터링에서 구분하기 위한 것 — 신규 모듈도 동일 필드 출력)
- §7.3: BATCH_RESULT는 기존 대시보드에 무수정 편입. 서비스별 드릴다운·**서비스 단위 연속 NODATA/SKIP 감시**는
  SERVICE_RESULT 기반 LogsQL 패널을 token-usage 대시보드에 별도 제공 ("전체 SUCCESS인데 한 서비스만 조용히
  빠지는" 경로 차단).
- **로깅 계약 (v1.4, 개인정보)**: 모든 로그(마커·WARN·에러·디버그)에 **레코드 페이로드와 user_id 원문을
  남기지 않는다** — 거부/중복/위반 사유는 카운트·페이지/행 인덱스·필드명·(필요 시) 솔트 고정 user_id
  해시로만 표현하고, HTTP 오류·파싱 실패 시 응답 본문 덤프도 크기·구조 요약만 남긴다. stdout은
  VictoriaLogs로 수집되어 ClickHouse 접근 통제를 우회하기 때문 — "per-user 데이터는 VM에 넣지 않음"(§2)
  원칙의 로그 경로 확장. warning_messages.md는 WARN별 허용 로그 필드를 명세하고, CI E2E에 "거부 행 발생 시
  로그에 user_id 원문 부재" 검증을 포함한다(§8.2). 신규 소스 모듈은 §5.9 계약 6조로 이 규칙을 상속.
- **mart BATCH_RESULT 필드 규약 (v1.10)**: mart BATCH_RESULT의 `missing_services` 값은
  쌍따옴표로 감싼다(서비스명 공백 보호) — `coverage=N/M`·`rows_mart`·`rows_view` 필드는
  mart 전용(§7.1, collectors BATCH_RESULT에는 없음).

### 5.7 파일 구성·환경변수

`main.py`(오케스트레이션·CLI·SIGTERM 핸들러) / `api_client.py`(HTTP·페이지네이션·재시도 — requests 세션 주입 가능) /
`clickhouse_client.py`(mutations_sync=2 DELETE→INSERT) / `vm_push.py` / `config.py` / `endpoints.yaml`.

환경변수 계약: `CH_*`(공통), `VM_PUSH_URL`, `ENDPOINTS_FILE`, `MAX_PAGES`(기본 200),
`SOFT_DEADLINE_MINUTES`(기본 50), `RETRY_*`, 그리고 **아웃바운드 HTTP 방침** (리뷰 #11 — 수집기는
아웃바운드 HTTP가 핵심 기능): `COLLECTOR_HTTPS_PROXY`(미설정=시스템 상속 / 빈 문자열=직접 연결 / 값=전용 프록시),
`COLLECTOR_API_VERIFY`/`COLLECTOR_API_CA_BUNDLE`(사내 CA) — 동료 mart/aip의 검증된 패턴.
전 서비스 공통 1개 방침으로 시작, endpoints.yaml 서비스별 override는 필요 시 후속.

`endpoints.yaml` 형식:

```yaml
services:
  - serviceGroup: "Mock Group"     # ← 정본 (§5.0)
    service: "Mock Service A"      # ← 정본
    baseUrl: "http://mock-provider.token-pipeline.svc:8000"
    enabled: true                  # 폐기 시 false로 유지, 항목 제거 금지 (§4.2)
    # type: usage-api-v1           # optional, 기본값. dim_service.source_type으로 전달.
    #                              # 다른 유형은 별도 모듈의 설정 파일에서 관리 (§5.9)
```

### 5.9 수집 확장 모델 — 적재 계약 (Sink Contract)

**배경**: API(`token-usage-api`)를 구현하지 못하는 서비스가 있을 수 있다 — 예: (케이스 1) 기준정보 A와
raw 메트릭이 object storage로 제공, (케이스 2) 스펙의 정보를 모두 담은 스냅샷 단건 API.
**확장 방식은 코드 프레임워크가 아니라 문서 계약**이다 (2-포크 반박 검증 결론: 구현 1개뿐인
어댑터 인터페이스는 추측성 추상화 — 동료 assets/의 소스별 독립 모듈 선례를 따름).
신규 소스는 **이 계약을 준수하는 별도 수집기 모듈**(`collectors/token-usage-<type>/`)로 클론 생성하며,
공용 코드는 §3의 승격 규칙(3회 중복 시 추출)을 따른다. **mart 이하 전 계층은 소스 유형을 모른다** —
이것이 이 계약이 보장하는 확장성이다.

**계약 조항** (모든 수집 모듈의 의무):

1. **정규 출력(NormalizedUsage)**: `fact.raw_token_usage_1d` 스키마 그대로 — 논리 키 5개(date,
   service, user_id, user_type, model) × 토큰 5필드, §5.4 정규화 규칙(null→'', 캐시 생략→0,
   위반 행 거부) 적용. 소스가 이벤트/요청 수준 데이터면 **집계 책임은 해당 모듈에 있다**
   (§5.4의 "중복 키 SUM 병합 WARN"은 사전 집계를 계약하는 usage-api-v1 전용 규칙).
2. **멱등성**: `(date, service)` 원자 교체 (delete-then-insert). 모듈 내부 스테이징 테이블을
   쓰는 경우 스테이징도 동일 키(또는 파일 키) 단위 멱등 — rerun 시 잔존물이 남지 않아야 한다.
   기존 행이 있을 때만 DELETE(§4.0 no-op 스킵)하고, DELETE 직전 기존 세대의 요약을 감사 이력에
   보존한다(§8.4). 모듈이 늘수록 delete-then-insert가 뮤테이션을 선형 증가시키므로 §4.0의
   **정례 뮤테이션 예산을 준수**한다.
3. **summary 행 필수**: 소스가 summary를 제공하지 않으면 detail 합산으로 파생 적재 + `is_derived=1`
   (§4.1의 파생 시맨틱: 검증 스킵·diff NULL·VM reported_* push 생략). NODATA여도 summary 행 적재 —
   mart STEP 0 커버리지 게이트의 전제.
4. **readiness/finality 판정 규칙을 명시적으로 정의**하고 공통 이벤트 분류로 번역할 것 —
   정책·예산·status 매핑은 §5.2의 분류→정책 표 1벌을 따른다(수치 재정의 금지, 소스 고유 값만
   자기 번역표에). 모듈은 자기 **적재 시퀀스 예산**(§5.2와 동형)을 수치로 선언한다.
   "확정 데이터만 적재" 원칙은 소스가 무엇이든 불변.
5. **서비스 식별 정본 = 해당 모듈의 설정 파일** (§5.0의 일반화). 소스 쪽 명칭은 reported_*에 보존.
6. **관측·등록**: 실행당 BATCH_RESULT 1줄 + 서비스별 SERVICE_RESULT(`source_type=` 포함),
   §5.6 로깅 계약(페이로드·user_id 원문 금지) 상속. `dim_token_service`에는 **자기 source_type 범위만
   원자 교체**로 등록(`DELETE WHERE source_type='<자기 유형>'` → INSERT — 전 테이블 교체 금지,
   타 모듈 등록분 보호) — coverage 게이트·기존 대시보드에 자동 편입.
7. **기준정보 결합 원칙**: 전사 기준정보(B — dim_token_user_org/dim_token_model)는 **mart 시점 결합**(불변).
   소스가 제공하는 기준정보(A)는 **`gpu_data`의 dim으로 승격해 effective_from 이력 append**가
   기본 — 수집 시점 결합은 rerun 결정성(§7.1)을 깨므로, 불가피하면 "해당 date 파티션의 A 스냅샷만
   사용"(최신본 금지)으로 결정성을 확보한다.
8. **테스트 대응물 의무**: 모듈은 CI용 mock 대응물(mock-provider 상당 — 예: mock-storage fixture)을
   함께 만든다. 3단 검증 체계(§8.2)가 새 소스에서도 성립해야 한다.
9. **적재 완료 데드라인 (v1.4)**: 모듈은 **T+1 03:30 KST**(= mart 04:00 − 마진)까지 대상
   (date, service)의 fact 적재를 완료해야 한다. 소스가 이를 구조적으로 보장할 수 없으면 온보딩
   시점에 (a) mart 스케줄 조정 또는 (b) 해당 서비스의 지연 허용 등록(mart STEP 0 게이트의
   expected-late 예외 목록)을 함께 확정한다 — 이 협의 없이는 §10의 "mart/view 무변경" 보장이
   성립하지 않는다. §9-10·11 협의 안건에 "데이터 확정 시각(readiness SLO)" 포함.

**케이스별 설계 지침** (구현은 소스 확정 시, §9-10·11):

- **object storage 소스** (`collectors/token-usage-storage/`):
  - **manifest 필수**: `<prefix>/<date>/_MANIFEST.json` (generatedAt, 파일 목록+체크섬, 행수).
    manifest 부재=NOT_READY(대기열, 예산 합산) / manifest-파일 불일치=PERMANENT_ERROR /
    설정된 보존창(retention_days) 밖 date=RETENTION. — 스토리지에는 409/404 구분이 없으므로
    이것이 readiness·retention 판정의 유일한 수단 (소스 제공팀과의 계약 필수, §9-10).
  - **§5.3의 등가 규칙**: 수집 시작 시 manifest를 스냅샷으로 고정, 그 목록만 다운로드 + 체크섬 대조.
    수집 도중 파일 추가/교체 감지 시 INVARIANT_BROKEN (정책·예산은 §5.2 분류 표를 따름).
  - 파일 → 스테이징(`fact.stg_*`, 멱등) → 변환·집계 SQL → 계약 1조의 정규 출력. 무거운 의존성
    (S3 SDK, parquet)은 이 모듈 이미지에만 존재.
  - 다운로드·집계는 대기(wait)가 아닌 처리(work) 시간 — 소스별 time_budget을 설정으로 분리해
    §5.2 소프트 데드라인 계상에서 구분.
- **스냅샷 API 소스** (`collectors/token-usage-snapshot/`):
  - 단건 GET(date 파라미터) → 전체 records(+summary). cursor 없음.
  - readiness/finality 신호(HTTP 409 상당? 응답 내 필드?)를 소스 계약으로 확정해야 함 (§9-11).
    매핑 불가 형식·코드는 PERMANENT_ERROR.
  - **응답 크기 상한 + 스트리밍 파싱**(MAX_PAGES의 등가물) — 단건 대용량 응답의 OOM 방지
    (동료 #133 교훈).

## 6. assets

### 6.1 user-org (전 직원 로스터 → 조직 dimension)

- **목표 데이터**: 사용 이력 여부와 무관한 **전 직원 로스터** + **조직 전체 경로(가변 깊이)** +
  `(user_id, effective_from)` 이력. CSV 형식: 경로는 구분자 `>` 단일 컬럼(예:
  `DS부문>반도체연구소>공정연구팀`) — 생성 도구가 `org_path` 배열로 분해(빈 세그먼트 거부).
  이것이 있어야 도입률(활성/정원)·1인당 사용량·미사용자 파악이 가능하다 (관점 분석 반영).
  **anonymous 계정 매핑이 제공되면 로스터에 포함**해 부서 귀속(미제공 시 unknown 버킷).
- **1단계**: `csv_to_dim_user_org_insert.py` — CSV → INSERT SQL **생성** 도구 (실행과 분리, 리뷰 가능).
  effective_from 컬럼 포함(TSV에 없으면 `--effective-from` 옵션, 기본 과거 기준일).
  적재는 이력 append + 사전 검증(키 중복·구간 검증) → count 검증. 갱신은 새 effective_from 행 추가
  (기존 행 불변 — dim_token_model과 동일 규약. 단 **파기·가명화는 예외**로 아래 보존 규칙에 따름).
- **2단계**: 사내 인사/조직 DB 소스 확정 시 sync CronJob 추가 (미결 §9-2 — 환경 데이터 경계 상속).
- **환경 데이터 경계 (v1.4)**: 도구 자체는 사외에서 **합성 fixture CSV**로만 개발·테스트한다.
  **실로스터 CSV와 생성 INSERT SQL은 레포·사외 환경 취급 금지** — 사내 반입 후 사내 절차로만
  투입·리뷰 (.gitignore 선제 패턴 등록, §7.2 환경 데이터 경계).
- **anonymous 매핑 취급 (v1.4, (2)는 v1.12 개정)**: (1) 수령 게이트 — anon id↔조직/실명 매핑의
  수령은 개인정보 처리 근거(정책 승인) 확인 후로 게이트(§9-2). (2) 저장 범위 — 승인되어도 anon
  행은 `user_name`에 **비실명 핸들명**만 저장 허용(실명 기입 금지 — 사내 투입 리뷰에서 확인,
  도구는 실명 여부를 판별할 수 없음, 사용자 결정 2026-07-14) + 조직 귀속 컬럼을 채움. 이전
  "빈 문자열 강제" 규칙은 폐지됐다 — 대시보드 표기 경로는 §4.2/§4.3(mart/view의 `user_name`
  컬럼, anonymous 행만) 참조. (3) 사용 범위 — anon 조직 귀속은 org 단위 집계에도 사용,
  per-user 상세(mart/view)의 조직 부착 여부는 §9-1 협의(소규모 부서 재식별 우려 기록) — 단
  **비실명 핸들명 표기 자체는 §9-1과 별개로 확정**(위 (2)).
- **보존 규칙 (v1.4)**: 퇴사(is_active=0) 후 N년 경과 행은 삭제 또는 user_name 가명화 —
  N·방식은 사내 개인정보 보존 정책과 함께 확정(§9-7). 파기 요청 처리는 §8.3의 user_id 축 삭제 경로.
- **미매핑 규칙** (리뷰 #13, v1.5 개정): 매핑 없는 user_id는 **`org_path = ['unknown']`, org_depth=1**로
  통일하며, 이 규칙은 §4.0의 빈 문자열 정규화보다 우선한다 (빈 배열과 'unknown'의 의미 구분 —
  어떤 깊이의 롤업에서도 'unknown' 버킷이 최상위 1개로 나타남).
  anonymous 사용량을 'unknown'에 합산할지 별도 `['anonymous']` 버킷으로 분리할지는 §9-1 협의 안건.

### 6.2 model-catalog (모델 단가)

- 시드 SQL 방식 — dim_holiday 3요소 패턴: (a) 출처·기준일 헤더 주석, (b) `NOT IN` 멱등 가드,
  (c) 말미 검증 SELECT. 단가 변경 시 새 `effective_from` 행 추가 (기존 행 불변).
- **`model='unknown'` 행(전 단가 NULL)을 시드에 포함** (§4.2, 리뷰 #15).

## 7. mart/token-usage · 배포 · 모니터링

### 7.1 mart 배치 (CronJob 매일 04:00 KST)

- **시간·CLI 계약 (v1.4)**: §5.1과 동일 — `target_date` = batch_time(기본 now) − 1일 (KST),
  batch_time은 ISO8601 위치 인자, 과거 재수행은 `--from/--to` 날짜범위. STEP 0 게이트·STEP 1의
  date 기준 유효 dim 선택·멱등 DELETE 술어·count 검증은 모두 target_date 기준이며, 날짜범위
  rerun은 **날짜별로 STEP 0→2 전체(게이트·검증 포함)를 독립 반복**한다.
- `batch.py`(I/O 오케스트레이션) + `mart.py`(순수 로직). 변환은 전부 **서버사이드
  `INSERT INTO ... SELECT`** (§4.0의 GLOBAL LEFT JOIN 표준, ClickHouse 파라미터 바인딩).
- **STEP 0 — 서비스 커버리지 게이트** (리뷰 #16 — HIGH): `dim_token_service`의 enabled 집합
  (**source_type과 무관하게 전체** — 게이트는 소스 유형을 모름) vs target_date의
  `fact.raw_token_usage_summary_1d` 행 존재를 비교 (NODATA도 summary는 적재되므로 FAILURE와 구분됨).
  §5.9 계약 9조의 expected-late 예외 목록에 등록된 서비스는 경고 대상에서 제외.
  정책: **적재는 진행하되 조용함 금지** — BATCH_RESULT에 `coverage=N/M missing_services=<목록>` 노출,
  누락 존재 시 LogsQL 경고 패널이 발화(§7.3). view의 불완전 마커 컬럼은 §9-1 대시보드 협의에 포함.
- STEP 1: fact × dim GLOBAL LEFT JOIN → `mart.token_usage_1d` → 그로부터 agg 3종.
  dim_token_user_org·dim_token_model 조인은 **date 기준 유효 행**(effective_from <= date 최신) 선택.
- STEP 2: mart → `gpu_data.view_token_usage_*` 적재 (created_by='token-pipeline' **명시 삽입**).
- 멱등성: `DELETE WHERE date=... [AND created_by='token-pipeline']` → **`wait_for_mutations`**
  (system.mutations 폴링 3s/300s. **CH_CLUSTER 설정 시 `clusterAllReplicas(cluster, system.mutations)`로
  전 레플리카 폴링** — 동료 mart/s2job 방식, 리뷰 #8) → INSERT. **INSERT는 `insert_deduplicate=0`
  (v1.10)** — 재삽입(rerun의 DELETE→동일 데이터 INSERT)이 ReplicatedMergeTree 블록 중복제거에
  걸려 조용히 폐기되는 것을 방지(클라이언트 설정은 `app/ch.py`의 `insert_select`). **서버측
  보강은 v1.12에서 제거** — 계정 공유 결정(§7.2)으로 `mart`가 동료와 공유하는 계정이 되어,
  `ALTER USER mart SETTINGS insert_deduplicate = 0` 같은 서버측 전역 SETTINGS 변경은 공유
  계정 전체에 영향을 미친다. 이 변경은 소유자 판단으로 남기고(이슈 #1), 이 파이프라인은
  클라이언트 설정만으로 계약을 유지한다.
- **INSERT 직후 count 검증 규칙** (리뷰 #10): Distributed 조회 재시도 10회/5초 간격(RETRY_* 조정 가능),
  `actual >= expected`면 통과(초과분은 중복 징후 CHECK WARN), 재시도 소진 후 미달이면 FAILURE.
  §6.1 dim 교체의 count 검증에도 동일 적용. (근거: 동료 verify_fact_rows — 레플리카 복제 lag)
- 인라인 검증: view 합계 == mart 합계 == raw 합계, detail vs summary 불일치 서비스 목록,
  dim_token_user_org 매핑 실패율(임계 기본 20% CHECK WARN), dim_token_model 미등록 모델 집합 WARN(unknown 제외).
- 종료: `BATCH_RESULT status=... module=mart-token coverage=N/M missing_services=... rows_mart=... rows_view=... warn=...`
- **dim 갱신의 소급 정책** (리뷰 #17): 조직 귀속은 `(user_id, effective_from)` 이력 조인으로
  **발생일 기준 고정** — rerun을 언제 돌려도 같은 결과. 과거 조직 정정이 필요하면 dim 이력 정정 후
  해당 기간 mart rerun (rerun.md 절차).

### 7.2 배포 (stage/company)

**환경 전제 — stage 홈랩 (v1.4, 2026-07-12 실사)**:
- stage ClickHouse 클러스터명 = **'gpu-monitoring'** (CHI 'gpu-monitoring', 현재 1샤드×1레플리카) —
  **동료 레포의 stage 관례('metrics')와 다름**: ddl/stage 작성 시 참조 모델의 ON CLUSTER 리터럴을
  복사하지 말 것. `CH_CLUSTER` 환경변수 값과 DDL의 ON CLUSTER 리터럴은 항상 일치.
- ClickHouse Operator 존재(chi-* 자동 탐색 성립), VictoriaMetrics 존재(§9-4 stage 검증 대상).
- **VictoriaLogs 부재** → §5.6/§7.3의 BATCH_RESULT "무수정 편입"·SERVICE_RESULT LogsQL 패널은
  stage에서 검증 불가 — 검증 시점을 company 반입 단계로 이관(또는 홈랩 VL 설치를 선행 작업으로).
- 레플리카 1이므로 전-레플리카 항목(mutations_sync=2의 다중 레플리카 대기, clusterAllReplicas 폴링,
  복제 lag count 재시도)의 실검증 여부는 §9-19 (레플리카 증설 vs company 단계 검증 이관).
  ZK 블록 해시 dedup 검증은 레플리카 1에서도 가능하므로 stage 잔류.

**환경 전제 — company (v1.6, 소유자 확인 이슈 #1)**: company 클러스터 'gpu-monitoring'은
**2샤드×2레플리카** — 분산 설계(mutations_sync=2·clusterAllReplicas·count 재시도·GLOBAL JOIN,
`insert_distributed_sync=1`)는 이 토폴로지를 전제로 이미 설계됨. stage(1샤드×1레플리카)와의
격차가 §9-19 결정의 근거.

**환경 데이터 경계 (v1.4)**: **stage(사외 홈랩)에는 실사용자 식별자·실명 로스터·사내 실사용량
데이터를 반입하지 않는다** — stage의 dim_token_user_org와 fact는 합성 데이터만 허용(mock-provider 합성
원칙의 assets 경로 확장). 실로스터 CSV·생성 SQL은 사내 저장소에서만 취급하며 .gitignore에 선제
패턴(`assets/user-org/data/`, `*roster*.csv`, `dim_user_org_insert*.sql`, `dim_token_user_org_insert*.sql`)을
등록한다 — endpoints.company.yaml과 동일한 분리 원칙의 확장.

**계정·GRANT 경계 (v1.12, 2026-07-14 계정 공유 결정 — 사용자·클러스터 소유자 합의, 이슈 #1)**:
- 전용 계정 3종(token_collector / token_mart / token_dashboard_reader)을 **폐지**하고,
  동료의 **기존 운영계정 `mart`를 공유**한다. 계정 생성·비밀번호 관리는 동료 소유 — 이
  레포의 accounts.sql은 더 이상 `CREATE USER`를 하지 않는다(과거 `CREATE USER IF NOT
  EXISTS`로 만들어진 token_* 계정이 남아있다면 소유자와 협의 후 `DROP USER`).
  구v1.4 근거였던 "이름 충돌 시 조용히 공유"(`CREATE USER IF NOT EXISTS`) 우려는 계정을
  아예 공유하기로 하면서 해소됐다.
- GRANT는 여전히 **우리 몫만 자기 테이블에 테이블 레벨**(_dist/_local 각각)로 명시 부여 —
  **이 레포 accounts.sql은 DB 레벨 GRANT를 요청하지 않는다**. 동료 계정 `mart`가 이미
  `mart.*` 같은 DB 레벨 광역 GRANT(DROP TABLE 포함)를 가지고 있을 수 있으나, 그것과
  무관하게 우리가 필요로 하는 최소 권한을 테이블 단위로 이 레포에 문서화해 둔다(향후
  GRANT가 좁혀지거나 계정이 다시 분리되어도 우리 요구사항이 남아있도록). 신규 테이블
  추가 시 accounts.sql GRANT 추가는 `migrate_add_*.sql` 절차의 일부.
- **읽기 전용 대시보드 계정 분리는 폐지**: 이전 안(전용 `token_dashboard_reader`로 권한
  모델을 사외에서 선검증)은 계정 공유 결정으로 대체됐다 — 대시보드도 공유 계정 `mart`를
  사용한다. **잔여 리스크**: `mart`는 쓰기 권한을 가진 계정이고 gpu_data의 실명
  dim(`dim_token_user_org`)에도 접근 가능하므로 "계정 분리에 의한 접근 통제"는 더 이상
  성립하지 않는다 — per-user 노출 grain·실명 dim 접근·ROW POLICY 필요 여부는 §9-1/§9-3
  협의로 이관(임시 방침 없음 — 확정 전 반입 게이트일 수 있음).
- **DDL 실행 주체 분리**: `CREATE DATABASE`/accounts.sql(GRANT)은 admin 수동 실행
  (company에서는 클러스터 소유자/DBA 협의 절차 포함). `CREATE USER`는 더 이상 이 레포의
  책임이 아니다(계정은 동료 소유). install.sh의 chi-* 자동 DDL 적용 대상은 테이블
  DDL·migrate로 한정. stage에서도 동일 경계 적용(습관 차이로 인한 반입 재작업 방지).

- 스크립트 규약: `./build.sh <stage|company>` / `./install.sh <stage|company>` +
  `--registry/--tag/--context/--namespace`. 태그 기본 git short SHA. stage=ghcr.io 기본,
  company=`--registry` 필수(Harbor) + `BASE_IMAGE` 프록시 치환. `python:3.12-slim`,
  requirements 선복사 캐시, 이미지 1개 + CronJob command 교체.
- k8s: kustomize base + overlays(stage/company). CronJob 공통: `concurrencyPolicy: Forbid`,
  `backoffLimit: 1`, `timeZone: Asia/Seoul`, historyLimit 3, envFrom secretRef,
  **`imagePullSecrets: registry-pull-secret`** (base 명시, 단일 이름으로 통일 — company install.sh가
  멱등 생성, 리뷰 #11). 수집기 `activeDeadlineSeconds: 4320`(산식 §5.2 — v1.4 교정), mart `1800`.
  **Pod resources (v1.6 — OOM 실경험)**: 수집기 requests 256Mi / limits **1Gi**
  (MAX_BUFFER_ROWS 20,000행 flush 전제 — §5.1 메모리 규칙), mart requests 256Mi / limits 1Gi
  (서버사이드 SQL 중심이라 경량). limits 없는 배포 금지.
- install.sh: Secret 멱등 생성(`<module>-ch-secret`, y/N 확인), `chi-*` 파드 자동 탐색 후 DDL 적용,
  CH_CLUSTER 주입, **kube API 서버 호스트 NO_PROXY 자동 추가** + 수집기 프록시 env를 Secret에 포함(§5.7).
- DDL: `ddl/{stage,company}/` 분리, 최소 권한 `accounts.sql` (리뷰 #8 반영, v1.12에서
  공유 계정 `mart` 기준으로 갱신 — 계정명은 전부 `mart` 하나이며, 아래는 "몫"별 GRANT 묶음):
  - 수집기 몫(구 token_collector): fact 자기 테이블 INSERT(dist/local) + local ALTER DELETE +
    gpu_data.dim_token_service 쓰기 — 전부 **테이블 레벨**. `mutations_sync=2` 방식이므로
    system.mutations 권한 불요.
  - mart 배치 몫(구 token_mart): fact·gpu_data 자기 테이블 조회 + mart·view 자기 테이블
    쓰기(INSERT/ALTER DELETE) + **GRANT SELECT ON system.mutations + GRANT CREATE
    TEMPORARY TABLE ON \*.\*** (clusterAllReplicas 폴링·GLOBAL JOIN에 필요).
  - 대시보드 몫(구 token_dashboard_reader): 별도 GRANT 불필요 — 위 mart 배치 몫의 SELECT가
    이미 `gpu_data.view_*`를 포함한다(계정 공유 결정). 잔여 리스크는 위 계정·GRANT 경계
    절 참조.
  - 전부 공유 계정 `mart`에 부여 — 이후 스키마 변경은 `migrate_add_*.sql` 관행 (GRANT
    추가 포함).
- **endpoints.yaml 분리 원칙**: 레포에는 stage(mock-provider)용만 커밋. 사내 서비스 URL 목록은
  `endpoints.company.yaml`(.gitignore)에서 install.sh가 ConfigMap 생성.

**company 검증 2단계 전략 (v1.13, 신설)**: company 반입은 **1단계(격리) → 2단계(정규)**
2단계로 진행한다 — stage(1s×1r·mock)로는 검증할 수 없는 company 토폴로지(2s×2r)·실 서비스
API 거동을, production DB(`fact`/`gpu_data`/`mart`)를 오염시키지 않고 먼저 검증하기 위함
(§9-19 해소 경로). 절차 전체는 `docs/operations/company-verify.md` 참조 — 이 절은 스펙
레벨의 골격만 요약한다.
- **1단계(격리)**: 동일 물리 클러스터·동일 실 서비스 API를 대상으로, 격리 DB 3종(기본안
  `token_verify_fact`/`token_verify_dim`/`token_verify_mart`)과 전용 계정(`token_verify`)만
  사용. DDL은 `tools/gen_verify_ddl.py`가 `ddl/company/*.sql`에서 구조 토큰(DB 한정자·ZK
  복제 경로·Distributed 인자·GRANT 대상)만 치환해 `ddl/company-verify/*.sql`로 생성(커밋
  대상, `--check`로 CI 드리프트 가드). k8s overlay `company-verify`(`nameSuffix: -verify` +
  Secret/ConfigMap 이름 패치)와 `install.sh <module> company-verify`로 배포하며, VM push는
  1단계 비활성.
- **공유 잔여 리스크**: 뮤테이션 예산이 파이프라인 기여분 기준 2배(~136건/일, 확정 예산
  일 150/피크 80에 근접)로 늘고, VictoriaLogs 마커는 `-verify` 파드명으로만 구분되며,
  **실 서비스 API에 대해 1단계와 2단계 CronJob을 병행 가동하지 않는다**(이중 폴링 금지 —
  교체 전환).
- **카나리아 전환**: 1단계 체크리스트(멱등 2-run 행수 보존·coverage 게이트·3계층 합계 일치·
  조직 귀속·cost·마커 — E2E 검증 항목의 실데이터판) 통과 후, 1단계 CronJob을 suspend하고
  공유 계정 `mart`로 production DB 대상 1일치 rerun → 동일 검증 SQL 재확인 → 정규 CronJob
  기동 순으로 전환한다.
- **철수**: 검증 실패 또는 전환 완료 후 `DROP DATABASE` 격리 DB 3종 + `DROP USER
  token_verify` + 1단계 CronJob 삭제 (`docs/operations/company-verify.md` 커맨드 포함).

### 7.3 모니터링

- **BATCH_RESULT(1줄/실행) → 기존 Grafana batch_result 대시보드 무수정 편입** (module 2종:
  `token-usage`, `mart-token`; 일배치 누락 평가창 25h). 기존 쿼리 중 라인 수 가정이 있는 패널(②③)과의
  충돌은 SERVICE_RESULT 분리(§5.6)로 해소됨을 확인. **대시보드 소유자 승인 완료** (이슈 #1 항목 3 — v1.6).
- **SERVICE_RESULT 기반 신규 패널** (token-usage 대시보드): 서비스별 상태 테이블, 서비스 단위 연속
  NODATA/SKIP/부재 감지, mart coverage 경고(missing_services 발화).
- VM 게이지: 서비스별 보고값 추이 (신뢰 수준 = 최초 수집 기준, §5.5). **distinct_users는 비가산 —
  serviceGroup 단위 sum() 금지** 주의를 Grafana 가이드에 명시.
- 홈랩 테스터 대시보드: `docs/monitoring/grafana_dashboard_token_usage.json` —
  `gpu_data.view_*` 조회 패널 + coverage/품질 패널.

## 8. mock-provider · 테스트 · 운영

### 8.1 tools/mock-provider

- 스펙 전체를 구현한 FastAPI 서비스. 합성 데이터는 (서비스 수, 사용자 수, 모델 목록, 시드) 파라미터로
  결정적 생성.
- 시나리오 옵션: 409 후 N초 뒤 200 전이, 429/503 확률 주입, invalid cursor, summary≠detail 불일치,
  unclassified/anonymous 행, **응답 서비스명 변경/공백 오타 주입**(§5.0 검증),
  **페이지네이션 도중 재집계(generatedAt 변경) 주입**(§5.3 검증), **페이지 N에서 409 전이**(§5.2 검증).
- 스펙 레포의 `tests/conformance_check.py`를 CI에서 mock-provider에 실행해 계약 준수 검증.
- 홈랩 배포 매니페스트 포함 → stage 전 구간 E2E.

### 8.2 테스트 (3단 분담)

1. **CI**: 단위(api_client cursor 루프·409/429·불변성 검사 분기 — Fake transport, 정규화, mart 검증 로직) +
   E2E(ClickHouse 컨테이너 + mock-provider → DDL → 수집기 → mart → `verify_expected_results.sql --expect-empty`) +
   conformance. **신규 소스 모듈은 자체 mock 대응물로 동일한 E2E를 구성** (§5.9 계약 8조).
2. **stage 런북**: 실클러스터 특성(Replicated ZK 해시, 비동기 mutation — mutations_sync=2와
   clusterAllReplicas 폴링 각각, 계정 권한, GLOBAL JOIN) 수동 검증.
3. **company 스팟체크** (`tests/company/inspect_*.sql`): view↔mart↔fact 합계 재계산, detail vs summary
   불일치, **fact와 mart/view의 (date, service) 커버리지 차이 검출**(rerun 체이닝 절차 누락 가시화, 리뷰 #7),
   created_by 이상 값, 품질 체크 UNION ALL("출력 없으면 정상").

### 8.3 운영 도구

- `tools/rerun.py` (모듈별): 날짜범위형 — CronJob 스펙에서 Job 생성 + command override + 로그 스트리밍 +
  완료 폴링. **collectors rerun은 완료 시 동일 날짜 mart rerun 명령을 출력하고 `--chain-mart` 옵션으로
  직접 트리거** (리뷰 #7). **체이닝 날짜 전달 계약 (v1.4)**: collectors rerun의 `--from/--to`가
  mart rerun에 **동일 값 그대로** 전파된다(--chain-mart가 이 인자로 mart Job command 구성) —
  두 모듈의 유일한 접점 인자. 다중 서비스 rerun의 DELETE는 `service IN (...)` 배칭(§4.0).
  절차는 `docs/operations/rerun.md`에 의무로 명시.
- **`tools/delete_data.py`** (리뷰 #12 + v1.4 확장): ① (date범위[, service]) 기준 fact 일괄 삭제
  (ON CLUSTER + mutation 대기) — 삭제 후 해당 날짜 mart rerun으로 재생성 (서비스 폐기·정정 경로).
  ② **user_id 축 삭제 모드** — 파기 요청·퇴사자 처리용: fact·mart·view 3계층 직접 ALTER DELETE
  (25개월치 mart rerun 우회는 비현실적), 절차는 rerun.md에 "파기 요청 처리"로 명시.
  기존 두 계정의 ALTER DELETE GRANT로 충족되는지, 전용 운영 계정을 둘지는 accounts.sql 설계에서 확정.
- `ddl/*/validation.sql`: 날짜 커버리지, dim 키·구간 중복, created_by 이상, raw 중복 등 상비 검증 쿼리.

### 8.4 정정(restatement) 프로토콜 (v1.4)

provider가 "확정" 보고한 값이 사후에 틀린 것으로 판명되는 시나리오(집계 버그 후 재확정)에 대한 3단 대응:

1. **감지**: 매일 정기 수집에 **D-2~D-7 summary 경량 재조회**(서비스당 +6회 단건 GET — §5.2 예산과
   §9-6 rate limit 협의에 계상)를 추가해, 저장된 (generated_at, 토큰 합계)와 비교. 차이 발견 시
   `RESTATEMENT` 마커(SERVICE_RESULT 수준) 발화 → 운영자가 해당 (date, service) rerun 판단.
   보조 수단으로 §9-6 협의 안건에 "확정 후 정정 시 통지 의무" 포함.
2. **감사**: 재수집이 기존 (date, service)를 덮어쓸 때 DELETE 직전 기존 세대의 요약
   (generated_at, collected_at, 토큰 5필드 합계, 행수)을 경량 감사 테이블
   `fact.collect_audit_1d`(append-only)에 보존 — §5.9 계약 2조로 전 소스 모듈에 일관 적용.
   delete-then-insert의 "1세대만 보존"이 차지백 정정 감사를 불가능하게 만드는 문제의 최소 비용 해소.
3. **절차**: 보존창 밖(재수집 404) 정정의 수동 경로 — fact 수동 정정 INSERT 규약(정정분 식별:
   collected_at 의미 규약)·mart rerun 연계·차지백 소비자 공지를 운영 문서로 정의 (§9-16).

### 8.5 백업/DR — 유일 사본 보전 (v1.4)

토큰 fact는 GPU 파이프라인과 달리 **원천이 소멸하는 데이터**다 (provider 보존창 경과 후 404 —
ClickHouse가 유일 사본, 25개월 보존·차지백 근거). 동료 레포에도 백업 선례가 없어 패턴 재사용으로
커버되지 않는다.

- **사본 등급 분류**: 유일 사본 = `fact.raw_token_usage_1d`·`raw_token_usage_summary_1d`·
  `fact.collect_audit_1d` + `dim_token_user_org` 이력(인사 소스의 이력 제공이 미확인인 동안 조건부) /
  재생성 가능 = mart·view 전부(rerun), dim_token_model·dim_token_service(시드/yaml). **백업 대상은 유일 사본만**.
- 백업 방식(ClickHouse BACKUP 또는 파티션 export)·주기·보관처·소유/비용은 클러스터 소유자(동료)와
  합의 — §9-17, 사내 반입 전 확정.
- **복구 런북 골격** (docs/operations/): 계정·DDL 재적용 → dim 시드/CSV 복원 → fact 백업 restore →
  원천 보존창 내 잔여 기간 재수집(--from/--to) → mart rerun 체이닝. delete_data.py 오조작 복구도 동일 경로.
- **최소 보존 하한**: §9-6 협의에 "서비스 최소 보존 하한(예: 30~35일 — 장애 감지~rerun 회수 창 + 여유)"
  추가. endpoints.yaml에 서비스별 `retentionDays`를 기록해 rerun 도구가 회수 불능 구간 요청 시 사전
  경고(§5.2 RETENTION 분기와 정합 — object storage 소스의 retention_days와 대칭).

## 9. 미결사항 (Open Questions)

| # | 항목 | 임시 방침 | 확정 방법 |
|---|---|---|---|
| 1 | 사내 대시보드 view table 컬럼 계약 — **org 롤업 기본 표시 깊이·서브트리 필터 UX(가변 깊이 전제), anonymous 버킷 표시·per-user 행 조직 부착 여부, 불완전 데이터 마커, 노출 grain(per-user vs agg만), 소규모 조직(headcount 1~2) 셀 억제 기준, identified 사용자 실명(user_name) 표기 여부(v1.12 — anon 비실명 핸들명 표기는 확정됐으나 identified 실명은 재식별·노출 확대 우려로 보류).** **잔여 리스크(v1.12 계정 공유 결정으로 추가)**: 대시보드가 쓰기 권한을 가진 공유 계정 `mart`로 접근하게 되어 계정 분리에 의한 접근 통제가 사라짐 — per-user 노출 grain 확정 시 이 리스크를 함께 다룰 것 | mart와 동일 스키마(anon 비실명 핸들명 `user_name`은 v1.12로 확정 적용) | 대시보드 담당과 협의 |
| 2 | dim_token_user_org 소스 시스템 (인사/조직 DB — **전 직원 로스터+이력+조직 전체 경로(가변 깊이) 제공 가능 여부**, **anon 매핑 제공의 정책 승인 여부**(별도 항목), §7.2 환경 데이터 경계 상속) | CSV 시드(경로 `>` 구분 컬럼) | 사내 확인 후 2단계 sync 설계 |
| 3 | company ClickHouse 네임스페이스·계정 정책 (+per-user 조회의 ROW POLICY/계정 분리 정책) — **클러스터 'gpu-monitoring' 2샤드×2레플리카는 확인됨(이슈 #1). 계정 정책은 v1.12에서 "공유 계정 `mart`로 확정"(전용 계정 3종 폐지) — 잔여는 네임스페이스 + 공유 계정 하에서의 실명 dim(dim_token_user_org) 접근 통제(ROW POLICY 등)** | 잔여: 네임스페이스·ROW POLICY | 사내 반입 시 확인 |
| 4 | VM push 엔드포인트(vminsert)와 사내 VM 정책 | stage 홈랩 VM으로 검증 | 사내 확인 |
| 5 | 모델 단가 통화(USD/KRW)·환율·실계약가 | USD 고정, cost는 참고 지표 | 비용 리포트 요구 확정 시 |
| 6 | 수집 시각(02:00 일괄)·서비스별 rate limit·**limit 상향(최대 5000)**·**최소 보존 하한(30~35일)**·**정정 통지 의무**·**D-2~D-7 재조회 예산** 협의 | 02:00 순차 + Retry-After 존중, limit 1000 | 서비스 구현팀들과 협의 |
| 7 | raw/mart/view TTL 보존 기간 + **dim_token_user_org 퇴사자 파기/가명화 기준(N년)** — 사내 **개인정보 보존 정책 정합** | 전 테이블 25개월 | 스토리지 + 개인정보 담당 확인 (company 반입 게이트일 수 있음) |
| 8 | **dim_budget 도입 여부와 예산 주체**(org 단위? 과제 단위?) — 예산 대비 소진율 요구 | 2단계 보류 | 경영/재무 요구 확인 |
| 9 | **서비스 개명 시 과거 데이터 이관 정책**(이관/삭제/별도 시리즈 보존) | enabled=0 유지 + 신규 이름으로 신규 시리즈 | 첫 개명 사례 전 결정 |
| 10 | **object storage 소스 계약** — manifest 스키마·경로 규약·파일 포맷·보존창·기준정보 A의 형식/이력 제공 여부·**데이터 확정 시각(readiness SLO, §5.9 계약 9조)** | 케이스 발생 시 모듈 신설 | 해당 소스 제공팀과 협의 |
| 11 | **스냅샷 API 소스 계약** — readiness/finality 신호, 응답 크기 상한, **데이터 확정 시각(readiness SLO)** (§5.9) | 케이스 발생 시 모듈 신설 | 해당 서비스팀과 협의 (표준 usage-api-v1 구현 유도가 1순위) |
| 12 | **서빙 플랫폼 메타 소스** — model→GPU 할당 이력(gpu_type, phase, 시간) 제공 가능 여부 (§4.4 Layer C 전제) | Layer C 보류 | 서빙 플랫폼(kserve/vLLM) 운영팀 협의 |
| 13 | **모델명 매핑 정본** — token-usage-api `model` ↔ 서빙 식별자 (dim_model_serving_map) | Layer C 보류 | §9-12와 함께 협의 |
| 14 | **GPU 시간당 원가 소스** — 산정 정책(감가·전력·상면), 동료 `dimension.gpu_model_quota_info`와의 관계 | Layer C 보류 | 재무/인프라 정책 확인 |
| 15 | **유휴·공유 GPU 정책** — 시분할 다중 모델 공유의 gpu_hours 안분, 통합 배포의 input/output 원가 가중치 | 범위 외 선언 | Layer C 설계 확정 시 |
| 16 | **보존창 밖 수동 정정 절차** — fact 수동 INSERT 규약·차지백 소비자 공지 (§8.4-3) | RESTATEMENT 감지·감사 이력은 설계 반영됨 | 운영 문서 작성 시 확정 |
| 17 | **백업 방식·주기·보관처·소유/비용** (§8.5 — 유일 사본 한정) | 백업 없이 시작 금지(사내 반입 전 확정) | 클러스터 소유자(동료)와 합의 |
| 18 | **DB 소유권 — 확정**: fact 공유(2026-07-13)·gpu_data `*_token_*` 접두사. 잔여: mart DB 공유/전용(Plan 3 DDL에서), 뮤테이션 예산 수치(일 150/피크 80 제안)의 소유자 최종 확인 | 반영 완료(PR #3, 수집기 코드) | mart는 Plan 3 DDL 초안 리뷰에서 |
| 19 | **stage 레플리카 증설 여부** — 전-레플리카 검증 항목(§7.2 환경 전제)을 stage에서 할지 company로 이관할지. **v1.13: company 격리 검증(company-verify)으로 해소 경로 확보** — 실제 company 2s×2r 클러스터 위에서 production DB를 건드리지 않고 mutations_sync=2 다중 레플리카 대기·clusterAllReplicas 폴링·복제 lag count 재시도를 실데이터로 검증할 수 있다(`docs/operations/company-verify.md`) | 레플리카 1 유지 + company 이관(격리 검증으로 이관 경로 구체화, v1.13) | stage 런북 작성 전 결정 — 격리 검증 통과를 회신 조건으로 사용 가능 |
| 20 | **홈랩 VictoriaLogs 설치 여부** — BATCH_RESULT/SERVICE_RESULT 패널의 stage 검증 가능성 | 미설치 — company 단계 검증 | stage 대시보드 작업 전 결정 |

## 10. 구현 순서 (권장)

1. mock-provider + conformance 통과 (시나리오 옵션 포함 — 테스트 기반 먼저)
2. collectors/token-usage — **DDL 초안을 먼저 작성해 동료 리뷰(이슈/PR)를 받은 뒤**(이슈 #1 항목 5
   요청 + §9-18 잔여 협의를 DDL로 구체화) 수집기 + CI E2E 진행
3. mart/token-usage STEP 0·1·2 + 검증 SQL
4. assets (model-catalog 시드 → user-org CSV 도구)
5. Grafana 테스터 대시보드 + stage 런북 + rerun/delete 도구
6. (사내 반입 후) company overlay·endpoints.company.yaml·대시보드 계약 반영

**신규 소스 온보딩 절차** (케이스 발생 시): 소스 계약 협의(§9-10·11) → 표준 usage-api-v1 구현
가능성 먼저 타진 → 불가하면 `collectors/token-usage-<type>/` 모듈 클론 → §5.9 적재 계약 준수 +
CI mock 대응물 → dim_service 등록으로 coverage·모니터링 자동 편입 → mart/view는 무변경.

작업 컨벤션은 동료 방식을 따른다: conventional commits(`type(scope): 설명`),
`type/kebab-case` 브랜치, 소형 PR(feat → 하드닝 fix → docs 분리), BATCH_RESULT/SERVICE_RESULT 마커는
첫 모듈부터.
