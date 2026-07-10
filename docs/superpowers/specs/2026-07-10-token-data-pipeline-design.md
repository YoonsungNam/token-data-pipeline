# token-data-pipeline 설계 문서

- 작성일: 2026-07-10
- 상태: 설계 확정 (사용자 승인), 구현 전
- 참조: [gpu-data-pipeline 분석](../../gpu-data-pipeline-analysis.md), [token-usage-api-spec](https://github.com/YoonsungNam/token-usage-api-spec) (`token-usage-api.yaml` v1.1.0, 로컬 클론 `/home/mini/github/token-usage-api-spec`)

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
| 서비스 목록 | 레포 내 설정 파일(`endpoints.yaml`) → ConfigMap | 사내 URL은 레포에 커밋하지 않음 (§7.2) |
| 환경 | **stage(홈랩) + company 2단계** (dev/kind 없음) | 로컬 검증은 CI가 담당 |
| 수집 토폴로지 | **단일 수집기 CronJob이 전 서비스 순회** | 서비스별 실패 격리 |
| 수집 경로 | **API→ClickHouse 직행**, 서비스 단위 합계만 VictoriaMetrics 게이지 push | per-user 데이터는 VM에 넣지 않음 |
| DB 배치 | fact(raw) / token_data(기준정보+view) / mart(집계) | 동료의 fact/gpu_data/mart 구조와 대칭 |
| 대시보드 | 최종적으로 사내 대시보드가 `token_data`의 view table을 읽음. 사외(홈랩) 작업 중엔 Grafana 테스터 대시보드로 대체 | |

## 3. 아키텍처

```text
각 서비스 (token-usage-api 구현체)
  │  GET /v1/usage (cursor 페이지네이션) + GET /v1/usage/summary
  ▼  매일 02:00 KST, date=어제
collectors/token-usage ──► fact.raw_token_usage_1d          (사용자×모델 상세)
  │                    ──► fact.raw_token_usage_summary_1d  (서비스 보고 합계)
  │                    ──► token_data.dim_service           (endpoints.yaml 전량 교체)
  │                    ──► VictoriaMetrics                  (서비스 단위 일합계 게이지)
  │
assets/user-org      ──► token_data.dim_user_org  (userId→부서/조직)
assets/model-catalog ──► token_data.dim_model     (model→provider·단가)
  │
  ▼  매일 04:00 KST
mart/token-usage
  STEP 1: fact × token_data(dim) 조인 ──► mart.token_usage_1d + mart.agg_token_{service,org,model}_1d
  STEP 2: mart ──► token_data.view_token_usage_*   ◄── 대시보드가 읽는 최종 테이블
```

배치 간 의존성은 동료 방식대로 **cron 오프셋**으로 표현한다 (수집 02:00 → mart 04:00).
중앙 오케스트레이터는 사용하지 않는다.

### 레포 구조 (모노레포, 모듈=배포단위)

```text
token-data-pipeline/
├── collectors/token-usage/     # main.py, api_client.py, clickhouse_client.py, vm_push.py,
│                               # config.py, endpoints.yaml(stage 예시), ddl/{stage,company}/,
│                               # k8s/(base+overlays), build.sh, install.sh, tools/rerun.py, tests/
├── assets/
│   ├── user-org/               # csv_to_dim_user_org_insert.py (SQL 생성), ddl/, (2단계: sync CronJob)
│   └── model-catalog/          # ddl/ + seed_dim_model.sql (멱등 시드)
├── mart/token-usage/           # batch.py, mart.py, ddl/, k8s/, build.sh, install.sh,
│                               # tests/, tools/rerun.py, warning_messages.md
├── tools/mock-provider/        # 스펙 구현 가짜 서비스 (FastAPI) + k8s + 시나리오 옵션
├── docs/
│   ├── monitoring/             # grafana_dashboard_token_usage.json + 가이드
│   ├── operations/             # rerun.md
│   └── superpowers/specs/      # 본 문서
└── .github/workflows/          # CI (모듈별 path 필터)
```

각 모듈은 자기 디렉터리에 실행 코드·DDL·배포 스크립트·재수행 도구·README를 자기완결적으로 보유한다.
동료 레포와 달리 `wait_for_mutations` 등 공용 유틸은 복붙하지 않고 모듈 내 `common/` 없이
**모듈별 단일 파일 내 함수로 두되, 구현 시 3회 이상 중복되면 공용 패키지로 승격**한다.

## 4. 데이터 모델

공통: 모든 테이블은 `<이름>_local`(ReplicatedMergeTree 계열) + `<이름>_dist`(Distributed) 쌍.
`PARTITION BY toYYYYMM(date)`, 시간 컬럼은 `DateTime('Asia/Seoul')`, 문자열은 NOT NULL(빈 문자열 정규화),
저카디널리티 컬럼은 `LowCardinality(String)`.

### 4.1 fact DB (수집 원본)

**`fact.raw_token_usage_1d`** — grain: `date × service × user_id × user_type × model`

| 컬럼 | 타입 | 비고 |
|---|---|---|
| date | Date | 사용량 발생 일자 (KST) |
| service_group | LowCardinality(String) | 과제명 (응답 필드) |
| service | LowCardinality(String) | 서비스 식별자 (응답 필드) |
| user_id | String | **unclassified는 `''`로 정규화** (스펙의 null → CH ORDER BY 키 Nullable 회피) |
| user_type | LowCardinality(String) | identified / anonymous / unclassified |
| model | LowCardinality(String) | `unknown` 허용 |
| input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, requests | UInt64 | 캐시 필드 생략 시 0 |
| generated_at | DateTime('Asia/Seoul') | 서비스가 보고한 집계 산출 시각 |
| collected_at | DateTime('Asia/Seoul') | 수집기 적재 시각 |

- ORDER BY `(service_group, service, date, user_type, user_id, model)`
- Distributed 샤딩키: `cityHash64(service, user_id)`
- TTL: `date + INTERVAL 25 MONTH` (전년 동월 비교 가능 — 미결 §9-7)

**`fact.raw_token_usage_summary_1d`** — grain: `date × service`

summary 응답 원본: 토큰 5필드 + `distinct_users` UInt32 + `distinct_identified_users` Nullable(UInt32)
(optional 필드) + `generated_at`, `collected_at`. ORDER BY `(service_group, service, date)`.
detail 합과의 정합성 검증 기준이자, 대시보드 서비스 합계의 대사(對査) 소스.

### 4.2 token_data DB (기준정보 + view)

| 테이블 | grain / 컬럼 요지 | 관리 주체 |
|---|---|---|
| `dim_service` | service — service_group, service, base_url, enabled UInt8, note, updated_at | 수집기가 시작 시 endpoints.yaml로 전량 교체 |
| `dim_user_org` | user_id — user_name, org_l1(사업부), org_l2(부서), org_l3(그룹), updated_at | assets/user-org (1단계 CSV 시드) |
| `dim_model` | (model, effective_from) — provider, input/cache_read/cache_creation/output 단가(USD per MTok), currency, note | assets/model-catalog 시드 SQL |
| `view_token_usage_1d` | mart.token_usage_1d와 동일 컬럼 | mart STEP 2 |
| `view_token_usage_service_1d` / `_org_1d` / `_model_1d` | 각 agg와 동일 컬럼 | mart STEP 2 |

- `dim_model`은 `effective_from`으로 단가 변경 이력을 남기고, mart가 date 기준 최신 유효 단가를 선택한다.
- `enabled=0`인 서비스는 수집 대상에서 제외 — 동료의 flag 게이트 패턴.
- view table 컬럼에는 `created_by` LowCardinality(String) DEFAULT 'token-pipeline' 포함 —
  추후 다른 파이프라인과 테이블을 공유해도 `DELETE WHERE ... AND created_by=`로 부분 멱등성 확보.
- **view table의 최종 컬럼 계약은 사내 대시보드 협의로 확정** (미결 §9-1). 확정 전에는 mart와 동일 스키마로 운영.

### 4.3 mart DB (1차 집계)

| 테이블 | grain | 내용 |
|---|---|---|
| `mart.token_usage_1d` | date × service × user × model | raw + 조직(org_l1~l3) + `total_input_tokens`(=input+cache_read+cache_creation) + `cost` Nullable(Float64) |
| `mart.agg_token_service_1d` | date × service_group × service | 토큰 합계, requests, distinct_users(detail에서 uniqExact, user_id≠''), reported_* 컬럼(=`fact.raw_token_usage_summary_1d`에서 조인한 서비스 보고값)과 차이 컬럼 |
| `mart.agg_token_org_1d` | date × org_l1 × org_l2 | 부서별 합계 + distinct_users |
| `mart.agg_token_model_1d` | date × model × provider | 모델별 합계 + 서비스 수 |

- `cost` = Σ(토큰별 단가 × 양) / 1e6. dim_model 미등록 모델은 cost NULL + 모델명 집합 CHECK WARN.
- agg의 소스는 `mart.token_usage_1d`로 통일해 조직 조인 결과가 어긋나는 것을 방지한다
  (예외: `agg_token_service_1d`의 reported_* 컬럼만 `fact.raw_token_usage_summary_1d`를 조인). 단, 상세 자체는 raw에서
  언제든 전체 재생성 가능하므로 동료의 "롤업 오류" 문제(가중 평균·중복 카운트)는 발생하지 않는다
  (토큰은 전부 가산적 합계이고 distinct_users만 상세에서 매번 재계산).

## 5. collectors/token-usage

### 5.1 정상 흐름 (CronJob 매일 02:00 KST)

1. `target_date` = batch_time(기본 now) − 1일 (KST). batch_time은 ISO8601 위치 인자.
2. endpoints.yaml 로드 → `token_data.dim_service` 전량 교체 (검증 후 DELETE→INSERT).
3. `enabled` 서비스 순회 — **서비스별 try/except 격리** (한 서비스 실패가 다른 서비스를 막지 않음):
   1. `GET /v1/usage/summary?date=<target_date>`
   2. `GET /v1/usage?date=...&limit=1000` → `nextCursor` 루프. cursor 사용 중 date/limit 고정(스펙 의무),
      페이지 수 상한(기본 200p = 20만 행)으로 무한 루프 방어.
   3. 행 정규화·검증 (§5.3)
   4. 정합성: Σdetail vs summary 비교 → 불일치 시 `CHECK WARN` (적재는 진행)
   5. `(date, service)` 단위 멱등 적재: `ALTER TABLE <local> ON CLUSTER DELETE WHERE date=... AND service=...`
      → `wait_for_mutations`(system.mutations 폴링, 3s/300s) → INSERT (detail·summary 모두, `insert_distributed_sync=1`)
   6. VictoriaMetrics push: 서비스 단위 합계 게이지 (§5.4)
   7. 서비스별 결과 로그: `BATCH_RESULT status=SUCCESS|NODATA|FAILURE module=token-usage service=<name> rows=... pages=... warn=...`
4. 전체 종료 라인: `BATCH_RESULT status=... module=token-usage services_ok=N services_failed=M`.
   실패 서비스 ≥1 → `exit 1` (성공 서비스 적재는 유지 — 부분 실패 허용, K8s Job 실패로 가시화).

**재수집 = 기본 동작**: 적재가 항상 delete-then-insert이므로 별도 `--purge` 플래그가 없다.
`main.py --from <d1> --to <d2> [--service <name>]`으로 과거 구간·특정 서비스만 재실행한다.
동료가 사후에 겪은 MergeTree 중복 문제를 기본값으로 차단하는 설계.

### 5.2 HTTP 에러 처리 매트릭스 (스펙 대응)

| 응답 | 의미 | 수집기 동작 |
|---|---|---|
| 409 `data_not_ready` | 집계 미확정 | 해당 서비스를 **대기열 끝으로 미루고 다음 서비스 먼저 처리**, `Retry-After` 경과 후 재방문. 서비스당 누적 대기 30분 또는 전체 Job 경과 55분 초과 시 해당 서비스 FAILURE (다음날 rerun으로 회수). 409 대기가 다른 서비스 수집을 지연시키지 않음 |
| 429 `rate_limited` | 호출 제한 | `Retry-After` 대기 후 재시도, 최대 3회 |
| 500 / 503 / 네트워크 오류 | 일시 장애 | 지수 백오프(5s→25s→125s) 최대 3회, 초과 시 FAILURE |
| 400 | 잘못된 요청 | **재시도 없이 즉시 FAILURE** — 당일/미래 date 계산 버그 등 우리 쪽 결함 신호. 단 `invalid_cursor`는 cursor 없이 1회 처음부터 재시작 |
| 404 `data_not_retained` | 보존 기간 초과 | 과거 재수집 시나리오 — WARN + 해당 서비스 SKIP (FAILURE 아님, 회수 불가능하므로) |
| 200 + 빈 records | 사용량 실제 0 | status=NODATA (성공, 경고 마커) |

### 5.3 행 정규화·검증 규칙

- `userId: null` → `user_id=''` (user_type=unclassified와 함께). 스펙의 `userType`↔`userId` 제약 위반 행
  (예: identified인데 userId null)은 **행 단위 거부 + WARN 카운트**.
- 캐시 토큰 필드 생략 → 0. 음수/타입 위반 → 행 거부 + WARN.
- 논리 키 `(user_id, user_type, model)` 중복 → 계약 위반 WARN 후 **SUM 병합** (유실 방지 우선).
- `generatedAt` KST 패턴 위반 → WARN만 (적재 진행) — 엄격 검증은 provider 셀프 점검(conformance_check) 책임.
- 거부 행 수는 BATCH_RESULT `rejected=` 필드로 노출.

### 5.4 VictoriaMetrics push (합계만)

- 대상: 서비스 단위 일합계만 — `token_usage_daily_{input,cache_read,cache_creation,output}_tokens`,
  `token_usage_daily_requests`, `token_usage_daily_distinct_users`, 레이블 `{service_group, service}`.
  **per-user/per-model 데이터는 VM에 넣지 않는다** (카디널리티 안티패턴 회피).
- 방식: `POST /api/v1/import/prometheus`에 **타임스탬프 = target_date 23:59:59 KST**를 명시해 적재
  (push 시각이 아닌 발생 일자에 정렬).
- push 실패는 WARN — ClickHouse 적재가 원천이므로 배치를 실패시키지 않는다.

### 5.5 파일 구성

`main.py`(오케스트레이션·CLI) / `api_client.py`(HTTP·페이지네이션·재시도 — 순수, requests 주입 가능) /
`clickhouse_client.py`(DELETE→대기→INSERT) / `vm_push.py` / `config.py`(환경변수: `CH_*`, `VM_PUSH_URL`,
`ENDPOINTS_FILE`, `RETRY_*`) / `endpoints.yaml`.

`endpoints.yaml` 형식:

```yaml
services:
  - serviceGroup: "Mock Group"
    service: "Mock Service A"
    baseUrl: "http://mock-provider.token-pipeline.svc:8000"
    enabled: true
```

## 6. assets

### 6.1 user-org (userId→부서/조직)

- **1단계 (본 설계 범위)**: `csv_to_dim_user_org_insert.py` — CSV를 받아 INSERT SQL을 **생성만** 하는 도구
  (동료의 `tsv_to_*` 패턴: 실행과 분리해 산출물이 리뷰 가능). 필수 컬럼 검증, 따옴표 이스케이프,
  기본값 규약 포함. 적재는 dim 전량 교체 프로토콜: 후보 검증(키 중복) → DELETE → mutation 대기 → INSERT → count 검증.
- **2단계 (후속)**: 사내 인사/조직 DB 소스 확정 시 sync CronJob 추가 (미결 §9-2).
- 매핑 없는 user_id는 mart에서 `org_l1='unknown'` 처리 — dim이 비어 있어도 파이프라인은 동작.

### 6.2 model-catalog (모델 단가)

- 시드 SQL 방식 — 동료의 `dim_holiday` 3요소 패턴: (a) 출처·기준일 헤더 주석,
  (b) `NOT IN` 멱등 가드, (c) 말미 검증 SELECT. 단가 변경 시 새 `effective_from` 행 추가 (기존 행 불변).

## 7. mart/token-usage · 배포 · 모니터링

### 7.1 mart 배치 (CronJob 매일 04:00 KST)

- `batch.py`(I/O 오케스트레이션) + `mart.py`(순수 로직 — 얇음: 검증 규칙·SQL 상수 위주).
  변환은 전부 **서버사이드 `INSERT INTO ... SELECT`** (ClickHouse 파라미터 바인딩, f-string 값 삽입 금지).
- STEP 1: fact × dim LEFT JOIN → `mart.token_usage_1d` → 그로부터 agg 3종.
- STEP 2: mart → `token_data.view_token_usage_*` 적재.
- 두 STEP 모두 날짜 단위 멱등: `DELETE WHERE date=... [AND created_by='token-pipeline']` → `wait_for_mutations` → INSERT.
- 인라인 검증: view 합계 == mart 합계 == raw 합계 (uniq 제외 가산 필드), detail vs summary 불일치 서비스 목록,
  dim_user_org 매핑 실패율(임계 기본 20% CHECK WARN), dim_model 미등록 모델 집합 WARN.
- 모든 WARN은 `warning_messages.md`에 메시지 키→의미→대응 표로 문서화.
- 종료: `BATCH_RESULT status=... module=mart-token rows_mart=... rows_view=... warn=...`.

### 7.2 배포 (stage/company)

- 스크립트 규약은 동료와 동일: `./build.sh <stage|company>` / `./install.sh <stage|company>` +
  `--registry/--tag/--context/--namespace`. 태그 기본 git short SHA. stage=ghcr.io 기본,
  company=`--registry` 필수(Harbor) + `BASE_IMAGE` 프록시 치환. `python:3.12-slim` 베이스,
  requirements 선복사 캐시, 이미지 1개 + CronJob command 교체.
- k8s: kustomize base + overlays(stage/company). CronJob 공통: `concurrencyPolicy: Forbid`,
  `backoffLimit: 1`, `timeZone: Asia/Seoul`, historyLimit 3, envFrom secretRef.
  수집기 `activeDeadlineSeconds: 3600`(409 대기 포함), mart `1800`.
- DDL: `ddl/{stage,company}/` 분리 (stage=cluster 'metrics', company=cluster 'gpu-monitoring' — 미결 §9-3),
  최소 권한 `accounts.sql`(수집기: fact INSERT + local ALTER DELETE + token_data.dim_service 쓰기 /
  mart: fact·token_data 조회 + mart·view 쓰기), 이후 변경은 `migrate_add_*.sql` 관행.
- **endpoints.yaml 분리 원칙**: 레포에는 stage(mock-provider)용만 커밋. **사내 서비스 URL 목록은
  사외 레포에 절대 커밋하지 않고**, company 배포 시 install.sh가 로컬 별도 파일(기본
  `endpoints.company.yaml`, .gitignore 등록)에서 ConfigMap을 생성한다.
- install.sh: Secret 멱등 생성(y/N 확인), `chi-*` 파드 자동 탐색 후 DDL 적용(kubectl cp + clickhouse-client),
  CH_CLUSTER 주입 — 동료 함수 재사용.

### 7.3 모니터링

- BATCH_RESULT 마커 → VictoriaLogs → **기존 Grafana batch_result 대시보드에 무수정 편입**
  (module 2종: `token-usage`, `mart-token`; 일배치 누락 평가창 25h).
- VM 게이지(§5.4)로 기존 VM 생태계에서 서비스별 토큰 추이 관찰.
- 홈랩 테스터 대시보드: `docs/monitoring/grafana_dashboard_token_usage.json` 커밋 —
  `token_data.view_*` 조회 패널(서비스별/부서별/모델별 추이, 사용자 상위 N, detail vs summary 불일치 스탯).
  사내 대시보드가 최종 소비자가 되기 전까지의 검증 수단.

## 8. mock-provider · 테스트 · 운영

### 8.1 tools/mock-provider

- 스펙 전체를 구현한 FastAPI 서비스. 합성 데이터는 (서비스 수, 사용자 수, 모델 목록, 시드) 파라미터로
  결정적 생성 — 같은 시드면 같은 데이터 (CI 기대값 고정 가능).
- 시나리오 옵션(환경변수/쿼리): 409 후 N초 뒤 200 전이, 429/503 확률 주입, invalid cursor,
  summary≠detail 불일치 주입, unclassified/anonymous 행 포함.
- **스펙 레포의 `tests/conformance_check.py`를 CI에서 mock-provider에 실행해 mock 자체의 계약 준수를 검증**
  — 테스트 대상(수집기)과 테스트 도구(mock)의 드리프트 차단.
- 홈랩 배포 매니페스트 포함 → stage에서 전 구간 E2E (수집→CH→mart→view→Grafana).

### 8.2 테스트 (3단 분담)

1. **CI (GitHub Actions, 모듈별 path 필터)**:
   - 단위: `api_client`(cursor 루프·409/429 분기 — Fake transport), 정규화 규칙, mart 검증 로직 (DB 불필요).
   - E2E: ClickHouse 컨테이너 + mock-provider 기동 → DDL(단일노드 단순화 스키마) → 수집기 실행 →
     mart 실행 → `verify_expected_results.sql --expect-empty` (기대값과 다른 행만 SELECT, 출력 없으면 통과).
   - conformance: mock-provider에 conformance_check.py.
2. **stage 런북** (`ddl/stage/RUNBOOK.md`): 실클러스터 특성(Replicated ZK 블록 해시, 비동기 mutation,
   계정 권한, Distributed) 수동 검증. 단계별 '의미' 설명 + 성공 로그 원문 + 에러→원인→조치 표.
3. **company 스팟체크** (`tests/company/inspect_*.sql`): view↔mart↔fact 합계 재계산 비교(`*_ok` 컬럼),
   detail vs summary 불일치 서비스, 품질 체크 UNION ALL("출력 없으면 정상") — 미매핑 조직 비율,
   음수/이상치, 날짜 누락.

### 8.3 운영 도구

- `tools/rerun.py` (모듈별): 날짜범위형 — CronJob 스펙에서 Job 생성 + command override(`--from/--to/--service`) +
  Pod 로그 스트리밍 + 완료 폴링. `docs/operations/rerun.md`에 절차.
- `ddl/*/validation.sql`: 상비 검증 쿼리 (raw 중복, watermark 아님—날짜 커버리지, dim 키 중복 등).

## 9. 미결사항 (Open Questions)

| # | 항목 | 임시 방침 | 확정 방법 |
|---|---|---|---|
| 1 | 사내 대시보드가 읽을 view table 컬럼 계약 | mart와 동일 스키마로 시작 | 대시보드 담당과 협의 |
| 2 | dim_user_org 소스 시스템 (인사/조직 DB 인터페이스) | CSV 시드로 시작 | 사내 확인 후 2단계 sync 설계 |
| 3 | company ClickHouse 클러스터명·네임스페이스·계정 정책 | 동료와 동일('gpu-monitoring') 가정 | 사내 반입 시 확인 |
| 4 | VM push 엔드포인트(vminsert)와 사내 VM 운영 정책 | stage 홈랩 VM으로 검증 | 사내 확인 |
| 5 | 모델 단가 통화(USD/KRW)·환율 처리 | USD 고정, cost는 참고 지표 | 비용 리포트 요구 확정 시 |
| 6 | 수집 시각(02:00 일괄)·서비스별 rate limit 협의 | 02:00 순차 호출 + Retry-After 존중 | 서비스 구현팀들과 협의 |
| 7 | raw/mart/view TTL 보존 기간 | 전 테이블 25개월 | 스토리지 검토 후 조정 |

## 10. 구현 순서 (권장)

1. mock-provider + conformance 통과 (테스트 기반 먼저)
2. collectors/token-usage (DDL + 수집기 + CI E2E)
3. mart/token-usage STEP 1·2 + 검증 SQL
4. assets (model-catalog 시드 → user-org CSV 도구)
5. Grafana 테스터 대시보드 + stage 런북 + rerun 도구
6. (사내 반입 후) company overlay·endpoints.company.yaml·대시보드 계약 반영

작업 컨벤션은 동료 방식을 따른다: conventional commits(`type(scope): 설명`),
`type/kebab-case` 브랜치, 소형 PR(feat → 하드닝 fix → docs 분리), BATCH_RESULT 마커는 첫 모듈부터.
