# token-data-pipeline 설계 문서

- 작성일: 2026-07-10 · **개정 v1.1: 2026-07-12** (멀티렌즈 리뷰 확정 지적 22건 + 기준정보 3건 반영)
- 상태: 설계 확정 (사용자 승인), 구현 전
- 참조: [gpu-data-pipeline 분석](../../gpu-data-pipeline-analysis.md), [token-usage-api-spec](https://github.com/YoonsungNam/token-usage-api-spec) (`token-usage-api.yaml` v1.1.0, 로컬 클론 `/home/mini/github/token-usage-api-spec`)

## 0. 개정 이력

| 버전 | 일자 | 내용 |
|---|---|---|
| v1.0 | 2026-07-10 | 최초 작성 |
| v1.1 | 2026-07-12 | 5렌즈 리뷰 확정 지적 22건 반영: 서비스 식별 정본 규칙(§5.0), 409/페이지네이션 원자성(§5.2·5.3), 부분 데이터 게이팅(§7.1 STEP 0), dim_user_org 이력화(§4.2), 물리 설계 표·분산 조인 표준(§4.0), BATCH_RESULT/SERVICE_RESULT 분리(§5.6), 소프트 데드라인(§5.2), 기준정보 확장(로스터·budget·anon 귀속, §6.1) 등 |
| v1.2 | 2026-07-12 | 수집 확장 모델 신설(§5.9 적재 계약): API 미제공 소스(object storage·스냅샷 API) 확장 경로 + 비용 파생 원칙(§4.3). 2-포크 반박 검증 반영 — 어댑터 프레임워크 기각(YAGNI), 소스 유형별 별도 모듈 + 문서 계약 방식 채택. summary에 is_derived, dim_service에 source_type 추가 |
| v1.3 | 2026-07-12 | 비용 2계층 확장 모델 신설(§4.4): Layer P(가격/차지백) / Layer C(GPU 타입·수행시간 기반 원가, PD분리 대응) 분리. 포크 검증 반영 — 동료 레포 실사(LLM 모델 개념 부재 확인), dim_model에 serving_type, 모델명 매핑 테이블 명시, 지표 이원화(총원가/실효원가), gpu_hours 할당 기준. 미결 12~15 추가 |

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
assets/user-org      ──► token_data.dim_user_org  (전 직원 로스터, 이력형)
assets/model-catalog ──► token_data.dim_model     (model→provider·단가, 이력형)
  │
  ▼  매일 04:00 KST
mart/token-usage
  STEP 0: 서비스 커버리지 게이트 (enabled vs 당일 summary 존재)
  STEP 1: fact × token_data(dim) 조인 ──► mart.token_usage_1d + mart.agg_token_{service,org,model}_1d
  STEP 2: mart ──► token_data.view_token_usage_*   ◄── 대시보드가 읽는 최종 테이블
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
│   └── model-catalog/          # ddl/ + seed_dim_model.sql (멱등 시드)
├── mart/token-usage/           # batch.py, mart.py, ddl/, k8s/, build.sh, install.sh,
│                               # tests/, tools/rerun.py, warning_messages.md
├── tools/
│   ├── mock-provider/          # 스펙 구현 가짜 서비스 (FastAPI) + k8s + 시나리오 옵션
│   └── delete_data.py          # (date범위[, service]) fact 일괄 삭제 + mutation 대기 (§8.3)
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
`PARTITION BY toYYYYMM(date)`, 시간 컬럼은 `DateTime('Asia/Seoul')`, 문자열은 NOT NULL(빈 문자열 정규화),
저카디널리티 컬럼은 `LowCardinality(String)`.

**테이블별 물리 설계 표** (리뷰 #20·#21 반영 — ORDER BY는 스펙 내 술어 패턴과 프리픽스 정합):

| 테이블 | ORDER BY | Distributed 샤딩키 | 비고 |
|---|---|---|---|
| `fact.raw_token_usage_1d` | `(date, service, user_type, user_id, model)` | `cityHash64(service, user_id)` | service_group은 일반 컬럼 (service의 종속 속성) |
| `fact.raw_token_usage_summary_1d` | `(date, service)` | `cityHash64(service)` | |
| `token_data.dim_*` 3종 | 각 키 | `rand()` | 소용량 — 조인은 GLOBAL(아래) |
| `mart.token_usage_1d` | `(date, service, user_type, user_id, model)` | `cityHash64(service, user_id)` | raw와 co-location |
| `mart.agg_token_*_1d` | `(date, <grain 키>)` | `cityHash64(service)` 또는 grain 키 해시 | |
| `token_data.view_token_usage_*` | mart 대응 동일 | mart 대응 동일 | |

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

### 4.2 token_data DB (기준정보 + view)

| 테이블 | grain / 컬럼 요지 | 관리 주체 |
|---|---|---|
| `dim_service` | service — service_group, service, base_url, enabled UInt8, **source_type** LowCardinality(String) DEFAULT 'usage-api-v1', note, updated_at | 수집기가 시작 시 endpoints.yaml로 전량 교체. **수집 경로와 무관한 서비스 레지스트리** — 신규 소스 모듈도 자기 서비스를 여기 등록(§5.9 계약 6조) |
| `dim_user_org` | **(user_id, effective_from)** — user_name, org_l1(사업부), org_l2(부서), org_l3(그룹), is_active UInt8, updated_at | assets/user-org |
| `dim_model` | (model, effective_from) — provider, **serving_type**(internal\|external), input/cache_read/cache_creation/output 단가(USD per MTok), currency, note | assets/model-catalog 시드 SQL. serving_type은 §4.4 Layer C 대상 판별("원가 NULL"이 미수집인지 대상외인지 구분) |
| `dim_budget` *(2단계, 선택)* | (scope_type: org\|service_group, scope, month) — budget_usd | 미결 §9-8 |
| `view_token_usage_1d` | mart.token_usage_1d와 동일 컬럼 | mart STEP 2 |
| `view_token_usage_service_1d` / `_org_1d` / `_model_1d` | 각 agg와 동일 컬럼 | mart STEP 2 |

- **`dim_user_org`는 "사용자 매핑"이 아니라 전 직원 로스터**(사용 이력 없는 직원 포함)를 목표로 한다
  → 부서 정원(headcount) 파생 가능 → **도입률·1인당 사용량** 계산 가능 (리뷰·관점 분석 반영).
  `(user_id, effective_from)` 이력 키(dim_model과 동일 패턴, 리뷰 #17)로 조직 이동을 이력화하고,
  mart STEP 1은 **date 기준 유효 행**(`effective_from <= date`인 최신 행)을 조인한다 —
  rerun이 실행 시점과 무관하게 결정적(deterministic)이 된다.
  **anonymous 계정도 매핑이 제공되면 로스터에 포함**해 부서 귀속한다 (§6.1).
- `dim_model`의 시드에는 **`model='unknown'` 행(전 단가 NULL, note='계약 표준 값 — 단가 산정 불가')을
  포함**한다 (리뷰 #15) — "dim_model 미등록 WARN"이 unknown으로 상시 발화해 경보가 무력화되는 것을 방지.
  이 WARN의 의미는 "단가 등록이 필요한 진짜 신규 모델"로 warning_messages.md에 명시.
- `enabled=0`인 서비스는 수집 대상에서 제외 — flag 게이트 패턴.
  **폐기된 서비스는 endpoints.yaml에서 제거하지 않고 `enabled: false`로 유지**한다 (리뷰 #12 —
  전량 교체 방식에서 dim_service 이력 유실과 잔존 데이터 조인 고아를 방지).
- view/mart 테이블의 `created_by` LowCardinality(String)는 **DEFAULT 없음** (리뷰 #22).
  공유 테이블 쓰기 계약: **모든 작성자는 INSERT 시 created_by를 명시 삽입**(본 파이프라인은
  'token-pipeline' 고정). DDL에 `CONSTRAINT check_created_by CHECK created_by != ''`를 두어
  컬럼 생략을 INSERT 에러로 조기 검출하고, `validation.sql`에 "created_by='' 또는 예상 외 값 검출" 쿼리 상비.
- view table의 최종 컬럼 계약은 사내 대시보드 협의로 확정 (미결 §9-1 — org 축 깊이(l2 vs l3),
  anonymous 버킷 표시, 불완전 데이터 마커 포함). 확정 전에는 mart와 동일 스키마로 운영.

### 4.3 mart DB (1차 집계)

| 테이블 | grain | 내용 |
|---|---|---|
| `mart.token_usage_1d` | date × service × user × model | raw + 조직(org_l1~l3) + `total_input_tokens`(=input+cache_read+cache_creation) + `cost` Nullable(Float64) + created_by |
| `mart.agg_token_service_1d` | date × service_group × service | 토큰 합계, requests, distinct_users(detail에서 uniqExact, user_id≠''), reported_* 컬럼(=`fact.raw_token_usage_summary_1d`에서 조인한 서비스 보고값)과 차이 컬럼 |
| `mart.agg_token_org_1d` | date × org_l1 × org_l2 | 부서별 합계 + distinct_users + **headcount**(로스터 기준 정원) + **adoption_rate**(=distinct_users/headcount) |
| `mart.agg_token_model_1d` | date × model × provider | 모델별 합계 + 서비스 수 |

- `cost` = Σ(토큰별 단가 × 양) / 1e6. date 기준 유효 단가(`effective_from <= date` 최신 행) 사용.
  dim_model 미등록 모델은 cost NULL + 모델명 집합 CHECK WARN (`unknown`은 시드 포함으로 자연 제외).
- **비용은 파생 데이터 (확장 원칙)**: mart에 토큰 4종 수량, dim_model에 단가 4종+이력이 있으므로
  **유형별 비용 분해·캐시 절감액(cache_read × (input단가−cache_read단가))은 물리 컬럼 없이 쿼리로
  언제든 계산 가능**하다. 분해 물리 컬럼이 필요해지는 조건(대시보드 성능·§9-1 계약 확정)이 오면
  `migrate_add_*` + mart rerun으로 추가한다 — v1.1의 effective_from 이력 덕에 재계산이 결정적이라
  안전. 단가 소급 정정도 동일 경로(dim 이력 정정 → 기간 rerun). 통화/환율은 §9-5 미결 상속.
  **물리 컬럼 범위**: mart 상세·agg·view 모두 `cost`(합계, Nullable)만 보유 — 분해 컬럼은 보류.
  미등록 모델은 cost NULL (분해 도입 시에도 "미등록=전 비용 컬럼 NULL"로 단순화).
- agg의 소스는 `mart.token_usage_1d`로 통일해 조직 조인 결과가 어긋나는 것을 방지한다
  (예외: `agg_token_service_1d`의 reported_* 컬럼만 `fact.raw_token_usage_summary_1d`를 조인).

### 4.4 비용 2계층 확장 모델 — Layer P(가격) / Layer C(원가) (확장 슬롯)

**배경**: 모델은 여러 GPU 타입에서 수행될 수 있다 (예: **PD분리** — prefill/decode를 서로 다른
GPU 타입 인스턴스에 분리 서빙). 사내 서빙 모델의 "원가"는 토큰 단가표가 아니라 **GPU 타입별
수행 시간**에서 나온다. 그러나 token-usage-api 계약에는 GPU 정보가 전혀 없다(소비 측 계약 —
누가 어떤 모델로 몇 토큰). 원가는 **공급 측(서빙 인프라) 도메인**이므로 별도 수집 경로가 필수다.
동료 레포 실사로 확인: `fact.raw_gpu_util_1m`의 태그는 host/gpu_index/**gpu_model(GPU 하드웨어
모델명)**뿐이고 레포 전체에 LLM 모델 개념이 없다 → **서빙 플랫폼 메타(model→GPU 할당 이력)가
반드시 필요**하다.

**Layer P — 가격/차지백** (v1.2 구현 범위, §4.3 그대로): 토큰 수량 × dim_model 단가.
소비 측 관점, 외부 API 모델은 실지불액. **차지백/청구는 이 계층만 사용한다.**

**Layer C — 원가 분석** (확장 슬롯, `dim_model.serving_type='internal'` 모델 전용):
**차지백에 사용하지 않는다** — 유휴 GPU 원가를 소비자에게 배분하는 논쟁을 원천 회피하고,
원가는 공급 효율(가동률·마진) 분석 용도로 한정한다. 테이블 스케치 (DDL은 케이스 확정 시):

| 스케치 | grain / 내용 |
|---|---|
| `fact.model_gpu_usage_1d` | date × model × **gpu_type [× phase(prefill\|decode)]** × gpu_hours. gpu_hours는 **할당(occupancy) 기준** — 전용 할당·MIG 슬라이스(gpu_type 세분) 지원, 시분할 다중 모델 공유는 범위 외(§9-15). 소스: 서빙 플랫폼 메타 + 동료 `fact.raw_gpu_util_1m`(읽기 전용, 보정/검증) |
| `token_data.dim_gpu_cost` | (gpu_type, effective_from) × cost_per_gpu_hour. gpu_type은 동료의 gpu_model(하드웨어) 체계와 매핑 — 동료 `dimension.gpu_model_quota_info`(GPU 모델별 quota 단가 선례)와의 관계 확인 §9-14 |
| `token_data.dim_model_serving_map` | token-usage-api의 `model` 문자열 ↔ 서빙 플랫폼 식별자(deployment명/모델 경로). **§5.0과 동형의 정본 문제** — 이 매핑 없이는 (date, model) 결합이 성립하지 않음 (§9-13) |
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
2. endpoints.yaml 로드 → `token_data.dim_service` 전량 교체 (검증 후 DELETE→INSERT).
3. `enabled` 서비스 순회 — **서비스별 try/except 격리**:
   1. `GET /v1/usage/summary?date=<target_date>`
   2. `GET /v1/usage?date=...&limit=1000` → `nextCursor` 루프. cursor 사용 중 date/limit 고정(스펙 의무).
      **페이지 상한 `MAX_PAGES`(env, 기본 200) 도달 = 부분 적재 금지, 해당 서비스 FAILURE** —
      delete-then-insert 이전에 중단하므로 기존 데이터 보존, BATCH_RESULT에 pages=와 사유 기록 (리뷰 #6).
      **페이지 간 불변성 검사**(§5.3) 수행.
   3. 행 정규화·검증 (§5.4)
   4. 정합성: Σdetail vs summary 비교 → 불일치 시 `CHECK WARN` (적재는 진행)
   5. `(date, service)` 단위 멱등 적재: `ALTER TABLE <local> ON CLUSTER DELETE WHERE date=... AND service=<정본>`
      을 **`mutations_sync=2` 설정으로 실행**(전 레플리카 완료까지 동기 대기 — 동료 collector 방식,
      폴링·추가 GRANT 불요, 리뷰 #8) → INSERT (detail·summary 모두, `insert_distributed_sync=1`).
      **NODATA(빈 records)여도 summary 행은 반드시 적재** — mart STEP 0 커버리지 게이트의 기준 (리뷰 #16).
      적재 시퀀스 시작 전 잔여 시간을 확인해(§5.2 소프트 데드라인), 완료 불가능하면 시작하지 않고
      FAILURE 처리 (DELETE 후 INSERT 전 kill로 인한 유실 방지).
   6. VictoriaMetrics push: 서비스 단위 합계 게이지 (§5.5)
   7. 서비스별 결과 로그: **`SERVICE_RESULT` 마커** (§5.6)
4. 전체 종료: **`BATCH_RESULT` 최종 1줄** (§5.6). 실패 서비스 ≥1 → `exit 1`
   (성공 서비스 적재는 유지 — 부분 실패 허용). **SIGTERM 수신 시에도 요약 BATCH_RESULT를 출력**하는
   핸들러를 둔다 (리뷰 #14 — deadline kill 시 마커 유실 방지).

**재수집 = 기본 동작**: 적재가 항상 delete-then-insert이므로 별도 `--purge` 플래그가 없다.
`main.py --from <d1> --to <d2> [--service <name>]`으로 과거 구간·특정 서비스만 재실행한다.
**collectors rerun 후에는 동일 날짜의 mart rerun이 의무** (§3, §8.3).

### 5.2 HTTP 에러 처리 매트릭스 (usage-api-v1의 이벤트 번역표)

수집 오케스트레이션 정책(대기열·소프트 데드라인·FAILURE 전이·SERVICE_RESULT status)은
**공통 이벤트 분류**(§5.9: NOT_READY / RETRYABLE / PERMANENT_ERROR / RETENTION / EMPTY /
INVARIANT_BROKEN) 기준으로 1벌만 존재하며, 아래 표는 usage-api-v1 소스의 HTTP 신호를
그 분류로 번역한 것이다. 신규 소스 모듈은 자기 신호의 번역표만 정의하면 된다.

**전역 소프트 데드라인** (리뷰 #14): Job 경과 **50분**을 소프트 데드라인으로 두고,
409 재방문 대기·429 Retry-After 대기·5xx 백오프·새 서비스 착수 전에 모두 체크한다.
초과 시 남은 서비스를 FAILURE로 마킹하고 **정상 경로로 종료**해 최종 BATCH_RESULT 출력을 보장한다.
`Retry-After`는 **`min(Retry-After, 300s)` 캡** 적용(초과 값 수신 시 WARN).
`activeDeadlineSeconds: 3600` = 소프트 데드라인(50분) + mutation 대기 상한 + 종료 마진 10분 (하드 안전망).

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
3. **summary 행 필수**: 소스가 summary를 제공하지 않으면 detail 합산으로 파생 적재 + `is_derived=1`
   (§4.1의 파생 시맨틱: 검증 스킵·diff NULL·VM reported_* push 생략). NODATA여도 summary 행 적재 —
   mart STEP 0 커버리지 게이트의 전제.
4. **readiness/finality 판정 규칙을 명시적으로 정의**하고 공통 이벤트 분류(NOT_READY / RETRYABLE /
   PERMANENT_ERROR / RETENTION / EMPTY / INVARIANT_BROKEN)로 번역할 것 (§5.2가 usage-api-v1의 번역표).
   "확정 데이터만 적재" 원칙은 소스가 무엇이든 불변.
5. **서비스 식별 정본 = 해당 모듈의 설정 파일** (§5.0의 일반화). 소스 쪽 명칭은 reported_*에 보존.
6. **관측**: 실행당 BATCH_RESULT 1줄 + 서비스별 SERVICE_RESULT(`source_type=` 포함), 자기 서비스를
   `dim_service`(source_type 포함)에 등록 — coverage 게이트·기존 대시보드에 자동 편입.
7. **기준정보 결합 원칙**: 전사 기준정보(B — dim_user_org/dim_model)는 **mart 시점 결합**(불변).
   소스가 제공하는 기준정보(A)는 **`token_data`의 dim으로 승격해 effective_from 이력 append**가
   기본 — 수집 시점 결합은 rerun 결정성(§7.1)을 깨므로, 불가피하면 "해당 date 파티션의 A 스냅샷만
   사용"(최신본 금지)으로 결정성을 확보한다.
8. **테스트 대응물 의무**: 모듈은 CI용 mock 대응물(mock-provider 상당 — 예: mock-storage fixture)을
   함께 만든다. 3단 검증 체계(§8.2)가 새 소스에서도 성립해야 한다.

**케이스별 설계 지침** (구현은 소스 확정 시, §9-10·11):

- **object storage 소스** (`collectors/token-usage-storage/`):
  - **manifest 필수**: `<prefix>/<date>/_MANIFEST.json` (generatedAt, 파일 목록+체크섬, 행수).
    manifest 부재=NOT_READY(대기열, 예산 합산) / manifest-파일 불일치=PERMANENT_ERROR /
    설정된 보존창(retention_days) 밖 date=RETENTION. — 스토리지에는 409/404 구분이 없으므로
    이것이 readiness·retention 판정의 유일한 수단 (소스 제공팀과의 계약 필수, §9-10).
  - **§5.3의 등가 규칙**: 수집 시작 시 manifest를 스냅샷으로 고정, 그 목록만 다운로드 + 체크섬 대조.
    수집 도중 파일 추가/교체 감지 시 INVARIANT_BROKEN(폐기 후 재시작 ≤2회).
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

- **목표 데이터**: 사용 이력 여부와 무관한 **전 직원 로스터** + 조직 3레벨 + `(user_id, effective_from)` 이력.
  이것이 있어야 도입률(활성/정원)·1인당 사용량·미사용자 파악이 가능하다 (관점 분석 반영).
  **anonymous 계정 매핑이 제공되면 로스터에 포함**해 부서 귀속(미제공 시 unknown 버킷).
- **1단계**: `csv_to_dim_user_org_insert.py` — CSV → INSERT SQL **생성** 도구 (실행과 분리, 리뷰 가능).
  effective_from 컬럼 포함(TSV에 없으면 `--effective-from` 옵션, 기본 과거 기준일).
  적재는 이력 append + 사전 검증(키 중복·구간 검증) → count 검증. 갱신은 새 effective_from 행 추가
  (기존 행 불변 — dim_model과 동일 규약).
- **2단계**: 사내 인사/조직 DB 소스 확정 시 sync CronJob 추가 (미결 §9-2).
- **미매핑 규칙** (리뷰 #13): 매핑 없는 user_id는 **org_l1/l2/l3 모두 'unknown'으로 통일**하며,
  이 규칙은 §4.0의 빈 문자열 정규화보다 우선한다 (''과 'unknown'의 의미 구분).
  anonymous 사용량을 'unknown'에 합산할지 별도 'anonymous' 버킷으로 분리할지는 §9-1 협의 안건.

### 6.2 model-catalog (모델 단가)

- 시드 SQL 방식 — dim_holiday 3요소 패턴: (a) 출처·기준일 헤더 주석, (b) `NOT IN` 멱등 가드,
  (c) 말미 검증 SELECT. 단가 변경 시 새 `effective_from` 행 추가 (기존 행 불변).
- **`model='unknown'` 행(전 단가 NULL)을 시드에 포함** (§4.2, 리뷰 #15).

## 7. mart/token-usage · 배포 · 모니터링

### 7.1 mart 배치 (CronJob 매일 04:00 KST)

- `batch.py`(I/O 오케스트레이션) + `mart.py`(순수 로직). 변환은 전부 **서버사이드
  `INSERT INTO ... SELECT`** (§4.0의 GLOBAL LEFT JOIN 표준, ClickHouse 파라미터 바인딩).
- **STEP 0 — 서비스 커버리지 게이트** (리뷰 #16 — HIGH): `dim_service`의 enabled 집합 vs
  당일 `fact.raw_token_usage_summary_1d` 행 존재를 비교 (NODATA도 summary는 적재되므로 FAILURE와 구분됨).
  정책: **적재는 진행하되 조용함 금지** — BATCH_RESULT에 `coverage=N/M missing_services=<목록>` 노출,
  누락 존재 시 LogsQL 경고 패널이 발화(§7.3). view의 불완전 마커 컬럼은 §9-1 대시보드 협의에 포함.
- STEP 1: fact × dim GLOBAL LEFT JOIN → `mart.token_usage_1d` → 그로부터 agg 3종.
  dim_user_org·dim_model 조인은 **date 기준 유효 행**(effective_from <= date 최신) 선택.
- STEP 2: mart → `token_data.view_token_usage_*` 적재 (created_by='token-pipeline' **명시 삽입**).
- 멱등성: `DELETE WHERE date=... [AND created_by='token-pipeline']` → **`wait_for_mutations`**
  (system.mutations 폴링 3s/300s. **CH_CLUSTER 설정 시 `clusterAllReplicas(cluster, system.mutations)`로
  전 레플리카 폴링** — 동료 mart/s2job 방식, 리뷰 #8) → INSERT.
- **INSERT 직후 count 검증 규칙** (리뷰 #10): Distributed 조회 재시도 10회/5초 간격(RETRY_* 조정 가능),
  `actual >= expected`면 통과(초과분은 중복 징후 CHECK WARN), 재시도 소진 후 미달이면 FAILURE.
  §6.1 dim 교체의 count 검증에도 동일 적용. (근거: 동료 verify_fact_rows — 레플리카 복제 lag)
- 인라인 검증: view 합계 == mart 합계 == raw 합계, detail vs summary 불일치 서비스 목록,
  dim_user_org 매핑 실패율(임계 기본 20% CHECK WARN), dim_model 미등록 모델 집합 WARN(unknown 제외).
- 종료: `BATCH_RESULT status=... module=mart-token coverage=N/M missing_services=... rows_mart=... rows_view=... warn=...`
- **dim 갱신의 소급 정책** (리뷰 #17): 조직 귀속은 `(user_id, effective_from)` 이력 조인으로
  **발생일 기준 고정** — rerun을 언제 돌려도 같은 결과. 과거 조직 정정이 필요하면 dim 이력 정정 후
  해당 기간 mart rerun (rerun.md 절차).

### 7.2 배포 (stage/company)

- 스크립트 규약: `./build.sh <stage|company>` / `./install.sh <stage|company>` +
  `--registry/--tag/--context/--namespace`. 태그 기본 git short SHA. stage=ghcr.io 기본,
  company=`--registry` 필수(Harbor) + `BASE_IMAGE` 프록시 치환. `python:3.12-slim`,
  requirements 선복사 캐시, 이미지 1개 + CronJob command 교체.
- k8s: kustomize base + overlays(stage/company). CronJob 공통: `concurrencyPolicy: Forbid`,
  `backoffLimit: 1`, `timeZone: Asia/Seoul`, historyLimit 3, envFrom secretRef,
  **`imagePullSecrets: registry-pull-secret`** (base 명시, 단일 이름으로 통일 — company install.sh가
  멱등 생성, 리뷰 #11). 수집기 `activeDeadlineSeconds: 3600`(산식 §5.2), mart `1800`.
- install.sh: Secret 멱등 생성(`<module>-ch-secret`, y/N 확인), `chi-*` 파드 자동 탐색 후 DDL 적용,
  CH_CLUSTER 주입, **kube API 서버 호스트 NO_PROXY 자동 추가** + 수집기 프록시 env를 Secret에 포함(§5.7).
- DDL: `ddl/{stage,company}/` 분리, 최소 권한 `accounts.sql` (리뷰 #8 반영):
  - 수집기 계정: fact INSERT(dist/local) + local ALTER DELETE + token_data.dim_service 쓰기.
    `mutations_sync=2` 방식이므로 system.mutations 권한 불요.
  - mart 계정: fact·token_data 조회 + mart·view 쓰기(INSERT/ALTER DELETE) +
    **GRANT SELECT ON system.mutations + GRANT CREATE TEMPORARY TABLE ON \*.\***
    (clusterAllReplicas 폴링·GLOBAL JOIN에 필요).
  - 이후 스키마 변경은 `migrate_add_*.sql` 관행.
- **endpoints.yaml 분리 원칙**: 레포에는 stage(mock-provider)용만 커밋. 사내 서비스 URL 목록은
  `endpoints.company.yaml`(.gitignore)에서 install.sh가 ConfigMap 생성.

### 7.3 모니터링

- **BATCH_RESULT(1줄/실행) → 기존 Grafana batch_result 대시보드 무수정 편입** (module 2종:
  `token-usage`, `mart-token`; 일배치 누락 평가창 25h). 기존 쿼리 중 라인 수 가정이 있는 패널(②③)과의
  충돌은 SERVICE_RESULT 분리(§5.6)로 해소됨을 확인.
- **SERVICE_RESULT 기반 신규 패널** (token-usage 대시보드): 서비스별 상태 테이블, 서비스 단위 연속
  NODATA/SKIP/부재 감지, mart coverage 경고(missing_services 발화).
- VM 게이지: 서비스별 보고값 추이 (신뢰 수준 = 최초 수집 기준, §5.5). **distinct_users는 비가산 —
  serviceGroup 단위 sum() 금지** 주의를 Grafana 가이드에 명시.
- 홈랩 테스터 대시보드: `docs/monitoring/grafana_dashboard_token_usage.json` —
  `token_data.view_*` 조회 패널 + coverage/품질 패널.

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
  직접 트리거** (리뷰 #7). 절차는 `docs/operations/rerun.md`에 의무로 명시.
- **`tools/delete_data.py`** (리뷰 #12): (date범위[, service]) 기준 fact 일괄 삭제(ON CLUSTER +
  mutation 대기). 삭제 후 해당 날짜 mart rerun으로 mart/view 재생성 — 서비스 폐기·정정 시 잔존 데이터 정리 경로.
- `ddl/*/validation.sql`: 날짜 커버리지, dim 키·구간 중복, created_by 이상, raw 중복 등 상비 검증 쿼리.

## 9. 미결사항 (Open Questions)

| # | 항목 | 임시 방침 | 확정 방법 |
|---|---|---|---|
| 1 | 사내 대시보드 view table 컬럼 계약 — **org 축 깊이(l2 vs l3), anonymous 버킷 표시, 불완전 데이터 마커 포함** | mart와 동일 스키마 | 대시보드 담당과 협의 |
| 2 | dim_user_org 소스 시스템 (인사/조직 DB — **전 직원 로스터+이력 제공 가능 여부**) | CSV 시드(로스터 형식) | 사내 확인 후 2단계 sync 설계 |
| 3 | company ClickHouse 클러스터명·네임스페이스·계정 정책 | 동료와 동일('gpu-monitoring') 가정 | 사내 반입 시 확인 |
| 4 | VM push 엔드포인트(vminsert)와 사내 VM 정책 | stage 홈랩 VM으로 검증 | 사내 확인 |
| 5 | 모델 단가 통화(USD/KRW)·환율·실계약가 | USD 고정, cost는 참고 지표 | 비용 리포트 요구 확정 시 |
| 6 | 수집 시각(02:00 일괄)·서비스별 rate limit·**limit 상향(최대 5000) 협의** | 02:00 순차 + Retry-After 존중, limit 1000 | 서비스 구현팀들과 협의 |
| 7 | raw/mart/view TTL 보존 기간 | 전 테이블 25개월 | 스토리지 검토 후 조정 |
| 8 | **dim_budget 도입 여부와 예산 주체**(org 단위? 과제 단위?) — 예산 대비 소진율 요구 | 2단계 보류 | 경영/재무 요구 확인 |
| 9 | **서비스 개명 시 과거 데이터 이관 정책**(이관/삭제/별도 시리즈 보존) | enabled=0 유지 + 신규 이름으로 신규 시리즈 | 첫 개명 사례 전 결정 |
| 10 | **object storage 소스 계약** — manifest 스키마·경로 규약·파일 포맷·보존창·기준정보 A의 형식/이력 제공 여부 (§5.9) | 케이스 발생 시 모듈 신설 | 해당 소스 제공팀과 협의 |
| 11 | **스냅샷 API 소스 계약** — readiness/finality 신호, 응답 크기 상한 (§5.9) | 케이스 발생 시 모듈 신설 | 해당 서비스팀과 협의 (표준 usage-api-v1 구현 유도가 1순위) |
| 12 | **서빙 플랫폼 메타 소스** — model→GPU 할당 이력(gpu_type, phase, 시간) 제공 가능 여부 (§4.4 Layer C 전제) | Layer C 보류 | 서빙 플랫폼(kserve/vLLM) 운영팀 협의 |
| 13 | **모델명 매핑 정본** — token-usage-api `model` ↔ 서빙 식별자 (dim_model_serving_map) | Layer C 보류 | §9-12와 함께 협의 |
| 14 | **GPU 시간당 원가 소스** — 산정 정책(감가·전력·상면), 동료 `dimension.gpu_model_quota_info`와의 관계 | Layer C 보류 | 재무/인프라 정책 확인 |
| 15 | **유휴·공유 GPU 정책** — 시분할 다중 모델 공유의 gpu_hours 안분, 통합 배포의 input/output 원가 가중치 | 범위 외 선언 | Layer C 설계 확정 시 |

## 10. 구현 순서 (권장)

1. mock-provider + conformance 통과 (시나리오 옵션 포함 — 테스트 기반 먼저)
2. collectors/token-usage (DDL + 수집기 + CI E2E)
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
