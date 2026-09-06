# token-data-pipeline — /v1/metrics(GPU Hour·성능 메트릭) 반입 설계 문서

- 작성일: 2026-08-31 · **현재 버전 v0.5.2 (2026-09-06 — 구현 계획 Plan 6a/6b/6c 작성 완료, 실행 단계)** — 개정 이력은 §0
- 상태: **승인됨(2026-09-04)** → 구현 계획 3종 작성 완료(2026-09-06): [Plan 6a 스키마·시드](../plans/2026-09-04-token-metrics-schema.md) · [Plan 6b 수집기](../plans/2026-09-04-token-metrics-collector.md) · [Plan 6c 마트](../plans/2026-09-04-token-metrics-mart.md). **일정의 정본은 Plan 6a §'일정 재기준 (2026-09-06 기준)'** — 본 문서 §10 표는 8/31 원안 보존
- 참조: [마스터 설계 v1.13](2026-07-10-token-data-pipeline-design.md), **[비용 모델 정의서 Draft v0.1](../../cost-model-spec.md) — §6.4의 계산 규칙 정본**, [token-metric-api-spec](https://github.com/YoonsungNam/token-metric-api-spec) (`token-metric-api.yaml` v0.1.0 @6a552d2, `docs/METRICS_COLLECTION_SPEC.md`, `docs/internal/DECISIONS.md` #1~#24, `docs/internal/COLLECTOR_DESIGN.md`, `docs/METADATA_SHEET_TEMPLATE.xlsx`), [token-usage-api-spec](https://github.com/YoonsungNam/token-usage-api-spec) v1.1.0 @6c32650, [gpu-data-pipeline 분석](../../gpu-data-pipeline-analysis.md)
- 관계: 마스터 설계의 **자매 스펙**. 마스터 §4.4(Layer C 확장 슬롯)·§5.9(적재 계약)·§9-12~15(미결)를 이 문서가 구체화한다. 마스터 스펙은 **v1.14**로 §0 개정 이력 1행 + 계약 개정 절(§8 목록)만 반영한다.
- 산출 근거: 코드 리더 7종 + 데이터 인벤토리 → 독립 설계 3안 → 심사 2인(코드 대조) → 합성(v0.1) → 반박 검증 14건 + 완결성 비평(v0.2) → 사용자 회신 반영(v0.3) → 리뷰 25건 반영(v0.4) → 비용 모델 정의서 정렬(v0.5) → PR 전 정합 검증 23건 반영(v0.5.1).

## 0. 개정 이력

| 버전 | 일자 | 내용 |
|---|---|---|
| v0.1 | 2026-08-31 | 최초 초안 — 승자안(마감 최소) + 접목 8건 + 심사 필수 수정 12건 |
| v0.2 | 2026-08-31 | 반박 검증·비평 반영: 소프트 데드라인 산식, DELETE ×3, join miss `''`, since 게이트 범위, 검증 계층 재정의, P0-core/stretch, GRANT 표, 뮤테이션 장부 |
| v0.3 | 2026-08-31 | **사용자 회신 반영**: 사내 운영본 분기 → 기존 모듈 zero-diff·신규 모듈 2개 독립 배포; 메트릭 전용 레지스트리; **비용 모델(§6.4)** TCO×GPU-h·토큰 비율 배부; 공유 계정 `mart`; 주말 수집; API backfill 부재 전제; 불변식 별도 파일 |
| v0.4 | 2026-08-31 | 리뷰 25건 반영: zero-diff 누락 3건(CA ConfigMap·`registry-pull-secret`·`release-images.yml`·batch_result 패널 → §7.5 명시) ② 뮤테이션 가드·`--chunk-days`·장부 재산정 ③ P0 DDL 매니페스트 표(§4.0) 단일화, view 파일 P1 제외 ④ 시드는 admin 실행, 사내 시드는 플레이스홀더만, stage 합성값은 생성기 밖 fixture ⑤ manual `reported_*`=레지스트리 값, `identity_drift`는 API만 ⑥ `since` → `api_since`/`coverage_since` 분리 ⑦ 슬롯 산식 `startingDeadlineSeconds 540` ⑧ 배부 모집단 = 토큰 레지스트리 전 서비스(소비 전용 포함), provider 판정·T=0·음수 share·다중 제공자 표현 확정, M2 `allocation_capacity_cost_krw` 개명 ⑨ 수기 적재는 k8s Job 경로(`tools/manual_load.py`) P0, 템플릿 3파일 ⑩ mart accounts에 `CREATE TEMPORARY TABLE`·`system.mutations` GRANT, 토큰 측 읽기 DB env 분리, CronJob `timeZone`/`startingDeadlineSeconds`, 읽기 계약 3테이블/13컬럼, P0-core 총비용 패널 fallback |
| v0.5 | 2026-09-03 | **[비용 모델 정의서 Draft v0.1](../../cost-model-spec.md) 반입·정렬**: ① 모델 비용 C = **(serving+standby)만** × TCO — test·유휴는 **서비스 그룹 귀속**(배분 안 함, 정의서 3.2~3.4) → M1 개정, M2를 그룹 grain `agg_token_gpu_group_1d`로 개편(+`test_cost_krw`·`idle_cost_krw`·I2 항등식) ② 배분 키 = **가중 토큰 W(uncached 1 / cached 0.1 / output 4)**, uncached=input+cacheCreation(정의서 3.5~3.6) — v0.4의 단순 토큰 비율 대체 ③ W=0·C>0 → 호스팅 그룹 전액 + `token_not_reported`(정의서 I8) ④ 토큰 단가 p=C/W는 **파생 표시 전용**(기준월·가동률 병기, 정의서 3.7·D8) ⑤ **사외 API 비용(③)** = 벤더 KRW 단가표 신설 `dim_token_vendor_price`(P0-stretch; 처리등급 tier) — M4에 `external_api` 모드로 통합 ⑥ 할당표를 **serviceGroup 단위**로 재키(정의서 2.3) ⑦ 불변식 I1~I8 매핑 ⑧ 표시 규칙(측정/배분/추정 라벨·요청당 원가) §6.2 반영 ⑨ 미결에 정의서 §10 항목(가중치 TCO 확인·workloadType·처리등급·reasoningTokens·PTU·벤더 대사·D1 합의) 추가 ⑩ §10 일정 재기준 필요 주석(오늘 9/3) |
| v0.5.1 | 2026-09-03 | **PR 전 정합 정정**(다중 렌즈 검증 23건): ① §4.0 `dim_token_gpu_allocation` 물리 키 `service`→`service_group`(v0.5 ⑥ 잔재) ② 미결 M4 '단순 비율·가중(1:4)' 문구 삭제(가중 W가 정본), M3 `over_prov_ratio`→`utilization`/`over_report`/`identity_gap_krw`, M6 gpu_data 5테이블 ③ §10 '12파일'→14파일·'dim DDL 3·시드 3'→4·4, §9 날짜에 재기준 주석 확장 ④ §1 (b) 사용자 확정 문구에 v0.5 대체 명시 ⑤ 정의서 정밀화: **③ 사외 API = input×p_in + cache_read×p_cache + cache_creation×p_write + output×p_out**(정의서 3.5 `uncached`=input+cacheCreation과 3.9 `uncached×p_in`의 명명 충돌 → cacheCreation 이중 계산 방지, 컬럼 `krw_per_mtok_input`으로 개명, 정의서 피드백 M21) · `provider(m)` = serving/standby 행 보유 서비스만(test 전용은 비호스팅) · `allocated_gpu_hours = allocated_gpu_count × 24` 명시 · 벤더 단가 부재 시 `external_api` 행 NULL + `vendor_price_missing` · W(m)=0 특례의 M4/§6.4 문구 통일 · p 파생식 표기 정정 ⑥ §2 #12 grain에 `provider_service`, §7.2 생성기 `\|vendor_price`, §9 M7·M13 결번 주석 |
| v0.5.2 | 2026-09-06 | **구현 계획 반입**: Plan 6a(스키마·시드 11 Task)·6b(수집기 12 Task)·6c(마트 11 Task) 작성 + 4렌즈 검증·정정 완료. §10 일정은 Plan 6a §'일정 재기준 (2026-09-06 기준)'으로 이관(HARD 게이트 ① 9/8 오전 DDL 사인오프 ② 9/9 admin 슬롯+Harbor+수기 수치+M15 ③ 9/11 프리즈). 본문 설계 내용 변경 없음 |

## 1. 배경과 목적

기존 파이프라인은 각 서비스의 `GET /v1/usage`(사용자×모델 토큰·requests)를 매일 pull하여 fact→mart→`gpu_data.view_token_usage_*`를 만든다(**사내에서 이미 운영 중 — 단, 사내 배포본은 GitHub 코드와 일부 분기됨**). 사내 스펙 `token-metric-api-spec`이 자매 API `GET /v1/metrics`를 정의했다 — **모델별 GPU Hour**(비용의 유일한 근거)와 **성능 메트릭**(TTFT·ITL·Output TPS·E2E)을 같은 `(serviceGroup, service, model)` 표기로 제공하여, 대시보드에서 **비용·효율·성능을 한 화면**에 보이는 것이 목적이다.

일정 제약: 서비스들은 **9/1부터 `/v1/metrics` 개발 착수, 기한 2026-09-09(수)**, **2026-09-14(월) 대표이사 보고**(스펙 DECISIONS #24). 실데이터는 9/9(또는 그 다음날)부터이고, **API로 backfill할 이력이 없을 수 있음을 전제**한다 — go-live 이전 구간(8/26~9/8)은 수기(manual-v0)가 주 경로, API backfill은 가능한 서비스에 한한 보너스다. 데이터 수집은 주말 포함 **매일** 돈다.

**사용자 확정 사항(2026-08-31 회신)**: (a) 기존 모듈은 사내 분기본이 운영 중이므로 **GitHub의 기존 모듈 코드를 수정·재배포하지 않는다(zero-diff)**; 신규 코드만 독립 모듈로 빌드·배포 (b) 비용 모델 = 제공자 `Σ GPU Type별 TCO × 매핑 GPU Hours`, 구독자 `모델 비용 × 토큰 비율` — **v0.5에서 비용 모델 정의서 규칙(test 제외·가중 W 1/0.1/4)으로 대체, §6.4** (c) 공유 계정 `mart` 사용 (d) 수집은 주말 포함 매일 (e) API backfill 부재 감안.

**P0-core (9/14 필수)**: gpu fact · serving fact(custom 포함, long form) · summary 앵커(engine) · 감사 · `dim_token_metrics_service` · `dim_token_model_alias` · `dim_token_gpu_tco` · `agg_token_model_cost_1d` · `token_metrics_check_1d` · 수기(manual-v0) 로더(k8s Job 경로) · mock `/v1/metrics` · `invariants_metrics.sql` 5종 · Grafana 패널(총비용은 "배부 미적용" fallback 포함).
**P0-stretch (같은 DDL PR에 포함; 구현 순서 마지막, 프리즈 전 미착지 시 P1 이월)**: `agg_token_model_share_1d`(서비스 비용 ①②③ — 배부·사외 API) · `dim_token_gpu_allocation`(그룹 할당표) · `dim_token_vendor_price`(벤더 KRW 단가) · `agg_token_gpu_group_1d`(그룹 총비용·실험·유휴) · 할당·expect 체크 · 불변식 I1/I2/I3(`idle_negative`·`group_identity_gap`·`share_sum_mismatch`).
**P0 코드 델타(객체 외)**: 신규 모듈 2개 스캐폴딩(클론), 공유 도구 additive 수정(`tools/gen_*` 등록, `run_invariants.py --sql`, 신규 워크플로 3개 `test-collector-metrics.yml`·`test-mart-metrics.yml`·`release-images-metrics.yml`), mock `/v1/metrics`, `docs/monitoring/grafana_dashboard_token_metrics.json`, `tools/manual_load.py`. **기존 모듈 파일 수정 0건**(§7.5의 예외 1건 = 사내 batch_result 패널 규칙, 소유자 작업).

## 2. 새 데이터 종류 인벤토리

| # | 데이터 요소 | 소스 | grain | 현재 저장 | phase | 반입 위치(§) |
|---|---|---|---|---|---|---|
| 1 | **gpu 블록** — 모델별 GPU Hour (`gpuCount`, `gpuHours`, `category`) | API `gpu[]` | date × service × model × gpuType × category | 없음 | **P0** | `fact.raw_token_metrics_gpu_1d` (§4.1) |
| 2 | **serving 블록** — TTFT/ITL/E2E p50~p99, outputTps p50 | API `serving[]` | date × service × model × metric | 없음 | **P0** | `fact.raw_token_metrics_serving_1d` (§4.1) |
| 3 | serving `custom[]` | API `serving[].custom[]` | date × service × model × name | 없음 | **P0** (long form; 서비스 간 비교 금지는 패널 규칙) | 동일 테이블 metric='custom' |
| 4 | **응답 헤더 + engine 자기신고** | API 최상위 | date × service | 없음 | **P0** | `fact.raw_token_metrics_summary_1d` (앵커) |
| 5 | 시트 [1] 서비스 — owner, metricsUrl, engineType | 엑셀 탭 `서비스` | service × effective_from | 부분 | P1 (engine_type만; owner·URL 미적재) | `gpu_data.dim_token_service_meta` |
| 6 | **시트 [2] 모델 — canonical/alias/family/paramsB** | 엑셀 탭 `모델` | alias × effective_from → canonical | 부분 (alias 개념 없음) | **P0** alias / P1 meta | `gpu_data.dim_token_model_alias` (§4.2) |
| 7 | 시트 [3] GPU 할당 매핑 | 엑셀 탭 `GPU할당매핑` | service × infra × workgroup × unit | 없음 | P1 | `gpu_data.dim_token_gpu_unit_map` |
| 8 | 시트 [4] 소비관계 `consumes` | 엑셀 탭 `소비관계` | service × model × effective_from | 부분 | P1 (다중 제공자 모델의 배부 귀속, §6.4) | `gpu_data.dim_token_model_consumes` |
| 9 | **GPU 기종 TCO** (원/GPU·hour) | 운영자 TCO표(정본 — 정의서 §1) | gpu_type × effective_from | 없음 | **P0** (NULL 플레이스홀더로 출발) | `gpu_data.dim_token_gpu_tco` (§4.2) |
| 10 | **할당(그룹 쿼터)·유휴·실험 비용·그룹 총비용** — 정의서 3.1/3.3/3.4 | 수기 할당표(serviceGroup 단위) / 동료 DSCloud(P1) | date × service_group × gpu_type | 없음 | P0-stretch / P1 | `gpu_data.dim_token_gpu_allocation` → `mart.agg_token_gpu_group_1d` |
| 11 | **모델별 비용 C** = (serving+standby)×TCO — test 제외(정의서 3.2) + 가중 토큰·p 파생 | 파생 | date × service × canonical | 부분 | **P0** | `mart.agg_token_model_cost_1d` (§6.1 M1, §6.4) |
| 12 | **서비스 비용 ①②③** — 공유 모델 배분(가중 W 1/0.1/4, 정의서 3.5~3.6) + **사외 API 비용(벤더 KRW 단가, 정의서 3.9)** | 파생 + 운영자 벤더 단가표 | date × model × service × provider_service | 없음 | **P0-stretch** | `mart.agg_token_model_share_1d`(§6.1 M4) + `gpu_data.dim_token_vendor_price` |
| 13 | **검증 결과·품질 플래그·알림** | 파생 | date × service × check × model × gpu_type | 부분 | **P0** 체크 테이블+마커+불변식 / P2 발송기 | `mart.token_metrics_check_1d` (§6.1 M3) |
| 14 | 엔진 `/metrics` Prometheus 스크랩 | 스크랩 | minute × service × stream × metric × le | 없음 | P2 | 별도 모듈 |
| 15 | **go-live 이전 수기 수치** | 수기 | #1·#2·#4와 동일 | 없음 | **P0 (주 경로)** | 같은 fact 테이블 `source_type='manual-v0'` (§5.5) |

**JOIN 키와 정규화 요건**:
- 토큰 ↔ 메트릭: `(date, service, canonical_model)`. mart-metrics 시점에 **양측** 정규화 `canon(x) = if(a.canonical = '', x, a.canonical)` — ClickHouse는 `join_use_nulls=0`이라 LEFT JOIN miss가 `''`; `coalesce`는 미등록 모델을 한 키로 뭉갬. 미등록은 `model_registered=0`.
- 서비스 식별: **정본 = 각 모듈의 설정 파일**(마스터 §5.9-5) — 신규 모듈은 자기 `endpoints.yaml`을 가지며, 문자열은 사내 토큰 레지스트리(`gpu_data.dim_token_service`, 읽기 전용)와 바이트 동일해야 한다(M0 `service_not_in_usage_registry` WARN + 시드 생성기 `--services` 대조). **토큰 측 모집단(배부·M1 토큰 집계)은 `dim_token_service enabled=1` 전체**(소비 전용 서비스 포함) — 메트릭 레지스트리 등록 여부와 무관.
- gpu_type → TCO 키: API `gpuType` 정확 일치. 동료 문자열 정규화 맵은 P1.
- 시트 `gpuDashboardUnits` → 동료 할당 매핑 미확인 → 9/14는 수기 시드.

## 3. 확정된 전제·제약

| # | 전제 | 근거 |
|---|---|---|
| 1 | **사내 운영본 ≠ GitHub**(사용자 회신). 기존 모듈 재빌드·재배포는 사내 변경을 덮어쓸 위험 → 기존 모듈 파일 무수정, 신규 모듈은 자체 이미지·리소스명으로 공존. 사내 테이블 스키마도 다를 수 있음 → 신규 mart는 **읽기 계약 3테이블/13컬럼**(§6.1)만 의존하고 설치 시 `DESCRIBE` 프리플라이트 | 사용자 회신 2026-08-31; `mart/token-usage/ddl/company/mart_tables.sql` |
| 2 | `dim_token_service enabled=1`을 토큰 커버리지 대상으로 읽는 소비자 5곳(source_type 무시) — 사내도 동일 가정 → 메트릭 서비스는 그 테이블에 등록하지 않음 | `mart/token-usage/app/batch.py:24`, `tools/verify/invariants.sql:184-185`, Grafana JSON:533,593, `stage-runbook.md:317`, `company-verify.md:213` |
| 3 | `/v1/usage`에 소비 서비스 식별 필드 없음 → 배부는 **구독 서비스 자신의 usage 보고 토큰**으로 계산(§6.4) | `tools/mock-provider/contract/token-usage-api.yaml` diff 0 |
| 4 | alias→canonical 개념 없음. `dim_token_model` 키는 raw 문자열(변경 불가) | grep 0건, `dim_token_model.sql:32` |
| 5 | 동료 할당 모델: DSCloud만 GPU 장수, Slurm은 quota 단위, ai-platform은 소스 없음 | gpu-data-pipeline `source_tables.sql:75-127`, `sync_gdash_snap.py:244-425` |
| 6 | 뮤테이션 예산 일 150/피크 80. 존재확인→DELETE 스킵으로 첫 적재·409 재시도는 0; `ALTER DELETE`는 0행 매치여도 뮤테이션 | 마스터 §4.0(a), `clickhouse_client.py:98-111` |
| 7 | 수집기 골격은 클론 가능. **적재 예산은 소프트 데드라인 안에 예약**(`main.py:110,158,177`) → SOFT×60 > LOAD_BUDGET 필수 | `collectors/token-usage/app/main.py` |
| 8 | DDL 생성기·CI grep·이미지 매트릭스는 명시 리스트; `release-images.yml`은 **path 필터가 매트릭스 전체를 재빌드**(기존 이미지 포함) → 신규 워크플로 분리 | `tools/gen_stage_ddl.py:26-37`, `tools/gen_verify_ddl.py:56-81`, `.github/workflows/release-images.yml:19-27` |
| 9 | `/v1/metrics` 계약: 단건 GET, 보존 14일, `number`→Float64, `unknown`은 test만, `gpu:[]`/`serving:[]` 정상 가능, custom name 유일성 제약 없음. **케이스 E**(스펙 §4) = 사외 AI 모델 API 전용 서비스: `gpu:[]` + serving 행 선택 | `token-metric-api.yaml`, SPEC.md §4 |
| 10 | 스펙 타이밍: 02:00 시작·1시간 간격 409 재시도·09:00 알림 | `token-metric-api.yaml:15-18` |
| 11 | 마커 계약: 잡당 BATCH_RESULT 1줄(`module=` 고유), SERVICE_RESULT `source_type=`, 로그에 페이로드 금지. 사내 batch_result 대시보드는 레포 밖(무수정 편입 전제) | 마스터 §5.6·§7.3, `grafana_dashboard_token_usage.json:623` |
| 12 | `tools/rerun.py`는 command를 무조건 덮어씀; rerun Job은 Forbid 대상 아님 → 신규 모듈은 자기 rerun.py | `collectors/token-usage/tools/rerun.py:52-70,157-168` |
| 13 | 기존 CronJob 매니페스트는 `imagePullSecrets: registry-pull-secret`(네임스페이스 공유 Secret, 기존 install.sh가 생성)·`ca-bundle` ConfigMap(`token-usage-ca-bundle`, optional)을 참조; install.sh는 CronJob별 `set image`(컨테이너 이름 매칭)·`set env CH_HOST`(클러스터 내 `chi-*` 헤드리스 서비스명) 주입 | `collectors/token-usage/k8s/base/cronjob.yaml`, `install.sh:225-233` |
| 14 | ClickHouse: LEFT JOIN miss = `''`; `sum()`은 NULL 건너뛰어 부분 합 | `mart/token-usage/app/ch.py:100-104`, `steps.py:68,102` |
| 15 | GRANT는 테이블 레벨·admin 수동·모듈별 accounts.sql; `mart`의 DB 레벨 GRANT는 가정하지 않음; 감사 테이블은 INSERT만; mart 배치는 `CREATE TEMPORARY TABLE ON *.*`·`SELECT ON system.mutations` 필요(기존 mart accounts.sql에만 존재) | `collectors/token-usage/ddl/company/accounts.sql`, `mart/token-usage/ddl/company/accounts.sql`, 마스터 §7.2 |
| 16 | `run_invariants.py`는 SQL 경로 고정 → `--sql` 옵션 additive | `tools/verify/run_invariants.py:38` |
| 17 | 사람 게이트: fact·gpu_data 신규 테이블 승인, GRANT admin 수동, TCO·할당 수치, 시트 제출, Harbor 반입(신규 이미지 2개), 사내 스키마·스케줄 확인 | `docs/operations/company-verify.md:87-106` |

## 4. 데이터 모델

### 4.0 물리 설계 + P0 DDL 매니페스트 (마스터 §4.0 상속)

`<이름>_local` + `<이름>_dist` 쌍, `DateTime('Asia/Seoul')`, 문자열 NOT NULL(''), `LowCardinality`, **숫자 부재는 Nullable**, index_granularity 8192, gpu_data는 `dim_token_*`/`view_token_*` 접두사, 생성기 정규식 형식 준수. **DDL은 전부 신규 파일**(기존 DDL 파일 무수정).

**P0 DDL 매니페스트 (정본 — §5.6·§7.5·§10은 이 표를 참조)**:

| 모듈 디렉터리 | 파일 | 내용 | 적용 주체 |
|---|---|---|---|
| `collectors/token-metrics/ddl/company/` | `raw_token_metrics.sql` | fact 4테이블 | install.sh |
| 〃 | `dim_token_metrics_service.sql` | 레지스트리 | install.sh |
| 〃 | `accounts.sql` | GRANT(§4.2 표) | admin |
| `mart/token-metrics/ddl/company/` | `mart_metrics_tables.sql` | mart 4테이블(core 2 + stretch 2) | install.sh |
| 〃 | `accounts.sql` | GRANT(§4.2 표) | admin |
| `assets/model-catalog/ddl/company/` (신규 파일만) | `dim_token_model_alias.sql`, `dim_token_gpu_tco.sql`, `dim_token_gpu_allocation.sql`, `dim_token_vendor_price.sql` | dim 4 | admin |
| 〃 | `seed_dim_token_model_alias.sql`, `seed_dim_token_gpu_tco.sql`, `seed_dim_token_gpu_allocation.sql`, `seed_dim_token_vendor_price.sql` | 시드(사내 = `unknown`·NULL 플레이스홀더만; 실값은 생성기 산출 gitignore SQL) | admin |
| 〃 | `accounts_metrics.sql` | dim 4종 SELECT → mart | admin |
| (P1) `mart/token-metrics/ddl/company/view_token_metrics.sql` | — | P1 — **생성기 목록에 넣지 않음**(없는 파일은 `--check` 실패) | — |

생성기 등록: `tools/gen_stage_ddl.py` SOURCES += 위 14파일; `tools/gen_verify_ddl.py` MODULES += `collectors/token-metrics`(files 3), `mart/token-metrics`(files 2); assets는 glob. stage 합성값(TCO·벤더 단가 예시)은 **생성기 밖** `assets/model-catalog/fixtures/stage_seed_*.sql`(stage 런북이 수동 적용) — 사내 시드에 합성값이 섞이지 않게.

| 테이블 | DB | PARTITION BY | ORDER BY | 샤딩키 | TTL | phase |
|---|---|---|---|---|---|---|
| `raw_token_metrics_gpu_1d` | fact | toYYYYMM(date) | (date, service, model, gpu_type, category) | cityHash64(service) | 25 MONTH | P0 |
| `raw_token_metrics_serving_1d` | fact | toYYYYMM(date) | (date, service, model, metric, name) | cityHash64(service) | 25 MONTH | P0 |
| `raw_token_metrics_summary_1d` | fact | toYYYYMM(date) | (date, service) | cityHash64(service) | 25 MONTH | P0 |
| `collect_audit_metrics_1d` | fact | toYYYYMM(date) | (date, service, replaced_at) | cityHash64(service) | 25 MONTH | P0 |
| `dim_token_metrics_service` | gpu_data | 없음 | (service) | rand() | 없음 | P0 |
| `dim_token_model_alias` | gpu_data | 없음 | (alias, effective_from) | cityHash64(alias) | 없음 | P0 |
| `dim_token_gpu_tco` | gpu_data | 없음 | (gpu_type, effective_from) | cityHash64(gpu_type) | 없음 | P0 |
| `dim_token_gpu_allocation` | gpu_data | 없음 | (service_group, gpu_type, effective_from) | cityHash64(service_group) | 없음 | P0-stretch |
| `agg_token_model_cost_1d` | mart | toYYYYMM(date) | (date, service, model) | cityHash64(service) | 25 MONTH | P0 |
| `token_metrics_check_1d` | mart | toYYYYMM(date) | (date, service, check_name, model, gpu_type) | cityHash64(service) | 25 MONTH | P0 |
| `agg_token_model_share_1d` | mart | toYYYYMM(date) | (date, model, service, provider_service) | cityHash64(model) | 25 MONTH | P0-stretch |
| `agg_token_gpu_group_1d` | mart | toYYYYMM(date) | (date, service_group, gpu_type) | cityHash64(service_group) | 25 MONTH | P0-stretch |
| `dim_token_vendor_price` | gpu_data | 없음 | (provider, model, tier, effective_from) | cityHash64(model) | 없음 | P0-stretch |
| `view_token_*` 4종 · `dim_token_model_meta` · `dim_token_service_meta` · `dim_token_gpu_unit_map` · `dim_token_model_consumes` | gpu_data | — | — | — | — | P1 |

**뮤테이션 장부** (`collectors/token-metrics/ddl/README.md`에 동일 표):

| 경로 | 뮤테이션 |
|---|---|
| 정기 시간별 실행(8슬롯) | **0** — 앵커 존재→스킵, 미존재→INSERT만; 레지스트리 동기화는 정기 실행에서만·diff-check |
| 레지스트리 변경(endpoints 편집·최초 배포) | 1(최초 배포는 현재 집합이 비면 DELETE 생략 → 0); `api_since`/`coverage_since`는 typed 컬럼이라 go-live에 뮤테이션 없음 |
| 크래시 잔여물 복구 | 서비스당 ≤3 |
| 재수집 `--replace`(수집기) | 날짜당 fact **≤3**(gpu·serving·summary; 감사는 append-only; 테이블별 `service IN (...)` 배칭) |
| mart-metrics rerun | 날짜당 ≤4(M1·M3·M4·M2) |
| 일 총량 | 평시 토큰 ≤68 + 메트릭 0; mart-only rerun(alias/TCO 정정) 68 + 4D ≤ 150 → **D ≤ 20**; fact+mart rerun 68 + 7D ≤ 150 → **D ≤ 11**; 격리 검증 병행 시 D ≤ 2 |
| 실행당 가드 | `METRICS_MAX_MUTATIONS_PER_RUN`(수집기, 기본 **45** = 3×15) / `MART_METRICS_MAX_MUTATIONS_PER_RUN`(mart, 기본 **64** = 4×16) — 첫 DELETE 전 존재확인 선조회로 합산, 초과 시 `FAILURE reason=mutation_budget`. 두 rerun.py 모두 **`--chunk-days`(기본 7)** 로 긴 범위를 순차 Job으로 분할 |
| 피크(02:00~03:00) | 02:05 첫 슬롯은 INSERT만; 재수집은 **10:50 KST 이후** |

### 4.1 fact (수집 원본 — 4테이블, 파일 1개)

**`fact.raw_token_metrics_gpu_1d`** — grain: date × service × model × gpu_type × category

| 컬럼 | 타입 | 비고 |
|---|---|---|
| date | Date | KST 집계일 |
| service_group, service | LowCardinality(String) | **정본 = `collectors/token-metrics/endpoints.yaml`** |
| model | LowCardinality(String) | API 문자열 그대로(≤128; 정규화는 mart) — `unknown`은 category=test만 정상 |
| gpu_type | LowCardinality(String) | TCO표 키(정확 일치, ≤64) |
| category | LowCardinality(String) | serving \| standby \| test |
| gpu_count | Float64 | 그날 최대 장수(분수 허용). **비용 미사용** |
| gpu_hours | Float64 | 장수×시간 적분 — 비용의 유일한 근거 |
| flags | Array(String) | `hours_over_count`(FAIL, 원행 기준), `unknown_violation`(FAIL), `dup_merged`(WARN — 비용 포함). 빈 배열이 정상 |
| source_type | LowCardinality(String) | `metrics-api-v1` \| `manual-v0` |
| generated_at, collected_at | DateTime('Asia/Seoul') | |

**`fact.raw_token_metrics_serving_1d`** — grain: date × service × model × metric × name (**long form**). 유일성은 정규화기가 (model)·(model, custom.name) 중복 제거 후 성립.

| 컬럼 | 타입 | 비고 |
|---|---|---|
| date, service_group, service, model | 상동 | |
| metric | LowCardinality(String) | ttft_ms \| itl_ms \| e2e_ms \| output_tps \| custom |
| name | String DEFAULT '' | 표준 지표 '' / custom 지표명(≤64) |
| unit | LowCardinality(String) | 'ms' / 'tokens/s' / custom 단위(≤32) |
| p50, p90, p95, p99 | Nullable(Float64) | 부재 = NULL. output_tps는 p50만 |
| flags | Array(String) | `pct_non_monotone`(FAIL), `unknown_violation`(FAIL), `dup_model_kept_first`, `dup_custom_kept_first` |
| source_type | LowCardinality(String) | 상동 |
| generated_at, collected_at | DateTime('Asia/Seoul') | |

**`fact.raw_token_metrics_summary_1d`** — grain: date × service. **응답당 정확히 1행(앵커)**, NODATA(rows==0)도 기록. **DELETE 첫 번째·INSERT 마지막**(앵커 존재 = 적재 완료). flags 없음(응답 단위 위반은 CHECK WARN 카운트).

| 컬럼 | 타입 | 비고 |
|---|---|---|
| date, service_group, service | | |
| reported_service_group, reported_service | String | API 응답 원문; **manual-v0는 레지스트리 값**을 그대로 기록. 불일치 검사(`identity_drift`)는 `source_type='metrics-api-v1'`에만 |
| engine_type | LowCardinality(String) DEFAULT '' | null·형태 불량이면 '' (+`engine_malformed` WARN) |
| engine_version | String DEFAULT '' | |
| gpu_rows, serving_rows, custom_rows, rejected_rows | UInt32 | |
| merged_dups | UInt16 | |
| source_type | LowCardinality(String) | `metrics-api-v1` \| `manual-v0` |
| generated_at, collected_at | DateTime('Asia/Seoul') | 파싱 실패 → now(KST)+WARN; 오프셋≠+09:00 → KST 변환 + `generated_at_offset_mismatch` WARN |

**`fact.collect_audit_metrics_1d`** — append-only(절대 DELETE 안 함, GRANT도 INSERT만): date, service, prev_generated_at, prev_collected_at, prev_source_type, prev_gpu_rows, prev_gpu_hours_sum, prev_serving_rows, replaced_at.

### 4.2 gpu_data 기준정보 (append-only, **시드·생성 SQL은 admin 실행** — `mart`는 SELECT만)

| 테이블 | 컬럼 | 시드 규칙 |
|---|---|---|
| `dim_token_model_alias` (P0) | alias String, effective_from Date, canonical String, defining_service LowCardinality DEFAULT '', source LowCardinality DEFAULT 'metadata-sheet', note DEFAULT '' | identity 행(canonical→canonical)·`unknown`→`unknown` 필수; alias 없는 canonical-only 행도 identity 생성(`defining_service=''`). 검증: dup_key, alias_maps_to_two_canonicals, alias_loop, empty_canonical, missing_identity_row, service_not_in_registry |
| `dim_token_gpu_tco` (P0) | gpu_type String, effective_from Date, **tco_krw_per_gpu_hour** Nullable(Float64), currency LowCardinality DEFAULT 'KRW', basis LowCardinality DEFAULT '' (depreciation\|lease\|power-inclusive\|tco), note DEFAULT '' | 사내 시드 = `unknown` 행 + {H100, A100, H200, L40S} NULL 플레이스홀더(effective_from 2026-01-01)만; 실값은 `csv_to_layer_c_dim_insert.py --table gpu_tco`가 만든 gitignore SQL을 admin 적용. stage 합성값은 fixture(§4.0) |
| `dim_token_gpu_allocation` (P0-stretch) | **service_group** LowCardinality, gpu_type String, effective_from Date, allocated_gpu_count Nullable(Float64), source LowCardinality DEFAULT 'manual', note DEFAULT '' | **할당표는 serviceGroup 단위**(정의서 2.3 — 쿼터 보유 단위). ORDER BY (service_group, gpu_type, effective_from). 단위는 장수(count) — M2에서 `allocated_gpu_hours = allocated_gpu_count × 24`(일 쿼터 전일 적용, `effective_from` 00:00 KST부터; 정의서 2.3의 'GPU·h/일'과 등가). 플레이스홀더는 `gpu_type='unknown'`(M2 무시, `no_allocation`으로만 노출). 철회는 0 행 append |
| `dim_token_vendor_price` (P0-stretch) | provider LowCardinality, model String(canonical), tier LowCardinality DEFAULT 'standard' (standard\|batch\|flex\|priority), effective_from Date, krw_per_mtok_input/cached/cache_creation/output Nullable(Float64), note DEFAULT '' | 사외 API 비용(정의서 3.9). **`input`은 캐시 생성·읽기를 제외한 순수 입력 토큰 단가, `cache_creation`은 벤더 공표 전체 write 단가(입력 대비 할증분이 아님)**(정의서 3.5의 `uncached`=input+cacheCreation과 다른 개념 — 3.9의 `uncached×p_in` 표기는 cacheCreation 이중 계산 소지, 미결 M21로 정의서에 피드백; 벤더 TTL별 write 단가 차이는 note 컬럼 + 최고 단가 적용, M21). 시드는 생성기 `--table vendor_price`(사내 CSV → gitignore SQL, admin). 처리등급은 시트 컬럼 확정 전까지 'standard'(미결 M18) |

**effective_from 규약**: 시드 append 시 `effective_from` = **소급 시작일**(보통 2026-08-26 또는 그 서비스의 첫 데이터 날짜). 통화 KRW 고정; Layer P(USD/MTok) 불변; `dim_token_model` 재키 없음.

**GRANT (admin 수동, 전부 신규 파일·전부 `TO mart`, `ON CLUSTER 'gpu-monitoring'` — 기존 accounts.sql 무수정; 이미 있는 권한은 no-op)**:

| 파일 | GRANT |
|---|---|
| `collectors/token-metrics/ddl/company/accounts.sql` | `raw_token_metrics_{gpu,serving,summary}_1d`: SELECT, INSERT on `_dist`·`_local` + ALTER DELETE on `_local`; `collect_audit_metrics_1d`: SELECT, INSERT만; `gpu_data.dim_token_metrics_service`: SELECT, INSERT on `_dist`·`_local` + ALTER DELETE on `_local`; SELECT on `gpu_data.dim_token_service_dist`(프리플라이트·M0용, 기존 권한이면 no-op) |
| `mart/token-metrics/ddl/company/accounts.sql` | mart 4테이블: SELECT, INSERT on `_dist` + ALTER DELETE on `_local`; SELECT on `gpu_data.dim_token_model_alias_dist`, `dim_token_gpu_tco_dist`, `dim_token_gpu_allocation_dist`, `dim_token_vendor_price_dist`, `dim_token_metrics_service_dist`, `fact.raw_token_metrics_*_dist`, `mart.token_usage_1d_dist`, `mart.agg_token_service_1d_dist`, `gpu_data.dim_token_service_dist`; **`CREATE TEMPORARY TABLE ON *.*`, `SELECT ON system.mutations`**(GLOBAL JOIN·뮤테이션 폴링 — 기존 mart accounts.sql과 동일, no-op if present) |
| `assets/model-catalog/ddl/company/accounts_metrics.sql` | dim 4종 SELECT의 사본(assets 단독 적용용) |

### 4.3 메트릭 레지스트리 — `gpu_data.dim_token_metrics_service` (신규, 기존 `dim_token_service` 무접촉)

기존 `dim_token_service`에 행을 넣으면 사내 배포본의 STEP 0·불변식·패널 5곳이 토큰 커버리지 대상으로 오인하고, 이를 고치려면 기존 모듈 재배포가 필요하다(전제 §3-1·2) → **신규 모듈 소유의 레지스트리 테이블**(마스터 §5.9-6의 문서화된 예외).

| 컬럼 | 타입 | 비고 |
|---|---|---|
| service_group, service | LowCardinality(String) | 정본 = `collectors/token-metrics/endpoints.yaml`; 토큰 레지스트리와 바이트 동일해야 함(M0 WARN) |
| base_url | String | 메트릭 base(기본 = usage와 동일 호스트) |
| enabled | UInt8 | 0이면 **모든 모드**에서 `SKIPPED reason=disabled` |
| api_since | Date | **정기 API 수집 게이트**: target_date < api_since면 호출 안 함(기본 2026-09-09 = go-live 첫 데이터 날짜) |
| coverage_since | Date | **M0 커버리지 기대 시작일**(기본 2026-08-26): 이 날짜 이후 앵커(API든 manual이든)가 없으면 `metrics_missing` |
| until | Nullable(Date) | 마지막 데이터 날짜(게이트·커버리지 공통) |
| expect_gpu, expect_serving | UInt8 DEFAULT 1 | `gpu:[]`/`serving:[]`가 정상인 서비스는 0 |
| usage_includes_consumers | UInt8 DEFAULT 0 | 플랫폼 제공자: 자기 `/v1/usage`가 소비자 호출분을 포함 보고하면 1(§6.4 분모) |
| note | String DEFAULT '' | |
| updated_at | DateTime('Asia/Seoul') | |

물리: ORDER BY (service), 파티션·TTL 없음, `rand()`. 동기화: **정기 실행에서만**(rerun·manual 모드는 레지스트리를 읽기만) endpoints를 원하는 집합으로 만들고 현재 행과 diff(비교 키 = updated_at 제외 전 컬럼) → 다를 때만 `ALTER DELETE`(전체) + INSERT(현재 집합이 비면 DELETE 생략). M0 기대 집합 = `enabled=1 AND coverage_since ≤ d AND (until IS NULL OR d ≤ until)`.

`collectors/token-metrics/endpoints.yaml`(ConfigMap `token-metrics-endpoints`; 사내는 `endpoints-metrics.company.yaml` gitignore):

```yaml
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

## 5. 수집기 — 신규 모듈 `collectors/token-metrics/` (독립 이미지·CronJob)

### 5.1 모듈 토폴로지 (D1)

`collectors/token-usage`를 **클론**(마스터 §5.9 "별도 수집기 모듈로 클론 생성"; 2번째 중복 허용)해 `collectors/token-metrics/`를 만든다: `app/{main,api_client,normalize,writer,config,events}.py`, `Dockerfile`(동일 패턴), `build.sh`(`IMAGE_NAME="token-metrics-collector"`), `install.sh`(§5.6), `k8s/base`+`overlays/{stage,company,company-verify}`, `tools/rerun.py`, `tools/manual_load.py`, `ddl/{company,stage,company-verify}`, `tests/`, `README.md`. **기존 모듈의 파일·Secret·ConfigMap·CronJob을 수정하지 않는다.** 클론 매니페스트에서 기존 이름을 참조하는 항목은 전부 교체: `imagePullSecrets: registry-pull-secret`(네임스페이스 공유 Secret — 신규 install.sh는 **없을 때만 생성, 있으면 갱신 안 함**), `ca-bundle` ConfigMap → `token-metrics-ca-bundle`(optional), Secret → `token-metrics-ch-secret`, ConfigMap → `token-metrics-endpoints`. 유일한 접점은 읽기 전용 테이블(`gpu_data.dim_token_service`).

근거: 사내 운영본 분기(전제 §3-1) → 기존 이미지 재빌드 금지; 독립 모듈은 "새 코드만 새로 배포"를 구조로 보장. 비용: 스캐폴딩 클론(~600 LOC) + Harbor 반입 이미지 2개. 기각: 기존 모듈 2번째 엔트리포인트(v0.2), 같은 Job 2차 패스.

### 5.2 스케줄·정책 (D8)

CronJob `token-metrics-collector`: `schedule: "5 2-9 * * *"`, `timeZone: Asia/Seoul`, `concurrencyPolicy: Forbid`, **`startingDeadlineSeconds: 540`**, **`backoffLimit: 0`**, `restartPolicy: Never`, **`activeDeadlineSeconds: 3000`** = 소프트 데드라인 `SOFT_DEADLINE_MINUTES=40`(2400s — 신규 착수·409 재방문 창 20분 + 그 안에 예약된 적재 예산 `LOAD_BUDGET_S=1200`; 클론한 `main.py:110/177` 의미) + 종료 마진 600s. 불변식 `SOFT×60 > LOAD_BUDGET`을 `test_config.py`로 고정. 슬롯 산식: 지연 시작 ≤540 + 3000 + grace 30 = **3570s < 3600s** → 다음 슬롯을 Forbid가 건너뛰지 않음. 최종 슬롯: 시작 ≤09:14 + 3000s = **≤10:04**(+grace ≤10:04:30) → mart-metrics **10:20**. 컨테이너 `name`/`image` = `token-metrics-collector`. env(자기 Secret): `CH_HOST/PORT/USER=mart/PASSWORD/CLUSTER/CH_DB_FACT/CH_DB_DIM`, `ENDPOINTS_FILE`, `SOFT_DEADLINE_MINUTES=40`, `LOAD_BUDGET_S=1200`, `FINAL_HOUR_KST=9`, `MAX_RESPONSE_BYTES=5000000`, `METRICS_MAX_MUTATIONS_PER_RUN=45`, 프록시/CA 3종. VM push 없음.

**실행 모드 × 게이트**: `api_since`/`until` 게이트와 "최종 슬롯" 판정(프로세스 batch_time KST hour ≥ `FINAL_HOUR_KST`)은 **정기 실행(target_date = KST 오늘−1)에만**; `--from/--to`·manual 모드는 무시(rerun의 409는 `FAILURE reason=not_ready`). `enabled=0`은 모든 모드에서 `SKIPPED reason=disabled`; endpoints에 없는 `--service`는 exit 2.

| 조건 | 동작 | SERVICE_RESULT |
|---|---|---|
| enabled=0 (모든 모드) | 없음 | `SKIPPED reason=disabled` |
| 정기 & target_date < api_since / > until | HTTP 호출·적재 없음 | `SKIPPED reason=before_since` / `after_until` |
| 앵커 존재(source_type 무관) & 정기 또는 `--from/--to`·manual **without** `--replace` | 스킵(0 뮤테이션). manual-v0 앵커면 정기 경로에서 `CHECK WARN manual_row_present` | `SKIPPED reason=already_loaded` |
| 앵커 존재 & `--replace`(rerun·manual) | §5.4 (2)~(3) 교체(감사 `prev_source_type`); rerun의 404는 `SKIPPED reason=retention`(기존 행 보존) | `SUCCESS`/`NODATA`/`SKIPPED` |
| 앵커 없음 & 자식 행 있음(부분 적재 잔여물) | 재수집 — 확장 존재확인이 DELETE×3 강제 | 정상 상태값 |
| 200 | normalize → replace | `SUCCESS`(rows>0) / `NODATA`(rows==0 AND rejected==0 — gpu:[] AND serving:[]; 케이스 E(gpu:[] + serving 행)는 SUCCESS) / rows==0 AND rejected>0 → `SUCCESS rows=0 rejected=<n>` + `CHECK WARN all_rows_rejected` |
| 409 | 큐 끝 1회 재방문(min(Retry-After,300)s). 재차 409: 비최종 슬롯 `SKIPPED reason=not_ready`, 최종 슬롯 `FAILURE reason=not_ready_at_0900` → exit 1 → BATCH_RESULT FAILURE = **스펙 09:00 알림**. 최종 슬롯 유실 시 `final=1` 줄 부재 → M3 `metrics_missing`이 담당; 패널 규칙 "final=1 부재 = FAILURE"(§7.5) | |
| 404 | RETENTION — 정기 FAILURE / rerun SKIPPED | |
| 429/5xx/네트워크 | RETRYABLE 3회(5/25/125s, 캡 300s) | |
| 400 / 본문 > 5MB / date 에코 불일치 / non-JSON / 필수키 누락 / 비배열 | PERMANENT_ERROR | `FAILURE` |

마커: `SERVICE_RESULT status=… module=token-metrics service=<정본> source_type=metrics-api-v1|manual-v0 rows=<gpu+serving+custom> pages=1 warn= rejected= [reason=]`; 실행당 1줄 `BATCH_RESULT status=… module=token-metrics services_ok= services_failed= services_skipped= rows= elapsed= slot=HH final=0|1`(정상일 8줄, 일 상태 = `final=1` 줄). SIGTERM 시 캐시 줄 재출력. `CHECK WARN service=<svc> <code>=<count>`.

### 5.3 정규화·검증 (D9) — 3계층

기준: **계층 1 = OpenAPI 스키마 형태 위반(타입·필수키·enum·additionalProperties·minimum·maxLength) → 거부**; **계층 2 = 형태는 맞으나 스펙 yaml:43-44 "운영자 검증" 규칙·단조성 위반 → 적재 + 플래그/WARN**(의도적 예외: gpu 행의 serving/standby `unknown`은 스키마 위반이지만 운영자 검증 항목 → 원문 보존 + `unknown_violation`); **계층 3 = 교차 행·교차 소스 → M3 + 불변식**. 숫자 판정은 bool 제외.

1. **구조 → 거부**(`rejected=` 카운트만). 응답 단위(PERMANENT_ERROR): 필수키 누락, date 에코 불일치, gpu/serving 비배열, non-JSON, 본문 > 5MB. 행 단위 — gpu: model/gpuType 빈값·길이 초과(128/64), category ∉ enum, gpuCount/gpuHours 비숫자·음수·gpuCount≤0; serving: model 빈값/>128, 허용 외 키, ttftMs/itlMs/e2eMs 블록의 p키 누락·비숫자·음수·허용 외 키, outputTps p50 누락·비숫자·음수·p50 외 키, custom name/unit 누락·길이 초과·p키 0개·p값 비숫자·허용 외 키, 지표 0개.
2. **의미 → 적재 + 행 플래그 또는 응답 WARN**: 행 — `hours_over_count`(gpuHours > gpuCount×24+1e-6, **병합 전 원행 기준**), `unknown_violation`, `pct_non_monotone`(EPS 1e-6), `dup_merged`(SUM hours/MAX count, WARN), `dup_model_kept_first`(long form 전개 전), `dup_custom_kept_first`. 응답(API만) — `identity_drift`, `generated_at_parse_failed`, `generated_at_offset_mismatch`, `engine_malformed`, 최상위 추가 키(무시). **mart 규칙(§6.1·§6.4)**: 모델 비용 C(M1)는 FAIL 플래그(`hours_over_count`, `unknown_violation`) 행 제외 + `flagged_gpu_hours` 노출, FAIL 행의 비용은 그룹 행(M2) `unattributed_cost_krw`로; 유휴 산출(I1)은 **플래그 포함 전체 보고 시간** 기준(idle = 할당 − Σ전체).
3. **교차 → M3 + 불변식**: `serving_missing_for_gpu_model`(requests>0), `serving_without_gpu_serving_row`(expect_gpu=0 면제), expect 대비 빈 블록, Σ할당 초과, 미등록 모델, 미TCO 기종, 부분 적재, 전량 거부, `service_not_in_usage_registry`, `provider_ambiguous`, `consumer_tokens_exceed_provider`, `vendor_price_missing`, `no_allocation`(§6.4). 서비스 전체 혼합 percentile 행은 일반 모델 행 취급 → `unregistered_model`.

### 5.4 적재 시퀀스 (§5.9 계약 2조, 크래시 안전)

(1) **존재 SELECT 3종(summary/gpu/serving)** — fetch·normalize·예산 가드 이후 DELETE 직전(사전 `already_loaded` 판정과 별개) → 셋 다 없으면 DELETE 생략. (2) 하나라도 있으면: 앵커가 있을 때만 감사 INSERT → **DELETE 순서 고정: summary(앵커) → gpu → serving**(`_local`, ON CLUSTER, `mutations_sync=2`; 3 뮤테이션). (3) **INSERT 순서: gpu → serving → summary 마지막**(`insert_distributed_sync=1`, `insert_deduplicate=0`). 적재 착수 전 `deadline − now < LOAD_BUDGET_S`면 미착수 FAILURE. 가드는 (2) 직전 — 모든 모드(정기·rerun·manual) 공통.

**부분 적재 복구**: 앵커 없이 자식 행만 남으면 (i) date=오늘−1이고 남은 슬롯이 있으면 다음 슬롯이, (ii) 그 외는 운영자 `--from/--to` rerun(`--replace` 불필요)이 복구. 보존 밖 manual-v0 날짜의 크래시는 manual 재적재로 복구. mart-metrics는 **앵커가 있는 (date, service)의 자식 행만** 읽고, 잔여물은 M3 `partial_load`.

**배칭**: `--replace`(rerun·manual) 2단계 — (A) 대상 날짜 전 서비스 fetch/CSV 파싱·normalize·가드 → (B) 테이블당 `_delete_day_in(table, date, services)` 앵커→gpu→serving → (C) 서비스별 INSERT. 정기 경로는 서비스별 순차. 긴 범위는 rerun.py `--chunk-days`(기본 7)로 순차 Job 분할.

### 5.5 수기(manual-v0) 로더 — go-live 이전 구간의 주 경로

CLI(모듈 내부): `python -m app.main --manual-gpu /manual/gpu.csv --manual-serving /manual/serving.csv [--manual-engine /manual/engine.csv] --from D0 --to D1 [--service S] [--replace] [--generated-at ISO]`. 담당자 템플릿 3파일 `docs/templates/token_metrics_manual_v0_{gpu,serving,engine}.csv` — gpu: `date,service,model,gpuType,category,gpuCount,gpuHours`; serving: `date,service,model,metric,name,unit,p50,p90,p95,p99`; engine: `service,engine_type,engine_version`(스펙 §3 규칙 주석 포함). (date, service)별 `MetricsPayload`로 묶어 **동일 normalize + replace 경로**(§5.4; 가드·배칭 포함), `source_type='manual-v0'`, `reported_*` = 레지스트리 값, `generated_at` = `--generated-at`(권장: 제출 시각) 또는 적재 시각. 서비스는 endpoints에 등록(`enabled=1`)돼 있어야 함(api_since 무시); 모델 문자열은 alias dim 대조(미등록은 mart 플래그). **날짜 제약 없음**; 기존 앵커(API·manual 불문)가 있으면 `--replace` 없이는 `already_loaded` 스킵(안전 기본값). 레지스트리 동기화는 하지 않음(정기 실행 전용).

**전달 경로(P0) = k8s Job**: `tools/manual_load.py --from --to --gpu gpu.csv --serving serving.csv [--engine engine.csv] [--replace] [--context --namespace]` — CSV를 ConfigMap `token-metrics-manual-<ts>`로 생성 → CronJob 템플릿에서 Job 생성(`rerun.py`와 같은 골격, `/manual` 볼륨 마운트 + command 위 CLI) → 로그 스트리밍 → 완료 후 ConfigMap 삭제. 운영자 워크스테이션은 kubectl만 있으면 되고 ClickHouse 직접 접근·프록시·CA가 필요 없다(사내 `CH_HOST`는 클러스터 내부 헤드리스 서비스 — 전제 §3-13). 워크스테이션 직접 실행(`CH_*` env + port-forward)은 대안으로 README에만 기재. CSV·엑셀은 gitignore. 기각: 별도 manual 테이블, 직접 INSERT SQL 생성기.

### 5.6 배포·재수집·CI (전부 신규 모듈 안)

- `k8s/base/cronjob.yaml`(§5.2 필드) + `overlays/{stage,company,company-verify}`(company-verify는 `nameSuffix: -verify` + 자기 secretRef/configMap 패치).
- `install.sh <stage|company|company-verify>`: `registry-pull-secret` 없을 때만 생성, Secret `token-metrics-ch-secret`(CH_USER 기본 `mart`), ConfigMap `token-metrics-endpoints`(← `endpoints-metrics.company.yaml`), 선택 ConfigMap `token-metrics-ca-bundle`, `apply_sql` §4.0 매니페스트의 collectors 2파일, `kubectl apply -k`, `set image`/`set env CH_HOST`. **프리플라이트**: `fact`/`gpu_data` DB 존재·`gpu_data.dim_token_service_dist` SELECT 가능 확인.
- `tools/rerun.py`: CRONJOB `token-metrics-collector` 고정, command `python -m app.main --from --to [--service] [--replace]`, `--chunk-days`(기본 7), `TIMEOUT_SINGLE_S = 3000+600`; `--chain-mart`는 `mart/token-metrics/tools/rerun.py`를 같은 날짜로 호출(수집기 스킵 날짜 포함). 실행 창 **10:50 이후** + 활성 `token-mart-metrics` Job 0 확인.
- `tools/manual_load.py`(§5.5).
- CI `.github/workflows/test-collector-metrics.yml`(paths: `collectors/token-metrics/**`, `tools/mock-provider/**`): unit / e2e(CH 24.8 + mock, 자기 DDL 2파일 + `gpu_data.dim_token_service` 최소 twin, 수집기 2회(2회차 already_loaded·DELETE 0), 기대치 `datagen.build_metrics`, 시나리오 grep, manual CSV 적재 1회) / image / manifests(contract-lock: `schedule "5 2-9"`, `timeZone`, `startingDeadlineSeconds: 540`, `activeDeadlineSeconds: 3000`, `backoffLimit: 0`, `-verify` 이름). **`release-images-metrics.yml`**(신규; paths·matrix = `collectors/token-metrics`, `mart/token-metrics` 2개만 — 기존 `release-images.yml`은 무수정, 기존 이미지 재빌드 유발 금지); Harbor 반입은 sha7 태그 지정.
- 공유 도구 등록(additive): §4.0 매니페스트대로 `tools/gen_stage_ddl.py`·`tools/gen_verify_ddl.py`, `test-assets.yml` paths·grep 디렉터리(+2), `tools/data-admin/delete_data.py` 타깃(P1).

## 6. mart-metrics — 신규 모듈 `mart/token-metrics/` (독립 이미지·CronJob 10:20)

### 6.1 배치 (D7)

`mart/token-usage`를 **클론**: CronJob `token-mart-metrics`(`schedule: "20 10 * * *"`, **`timeZone: Asia/Seoul`**, Forbid, **`startingDeadlineSeconds: 1800`**, `activeDeadlineSeconds: 1800`, 컨테이너 `name`/`image` = `token-mart-metrics`, Secret `token-mart-metrics-ch-secret`(CH_USER=mart, `CH_DB_FACT/CH_DB_DIM/CH_DB_MART`, **토큰 측 읽기 전용 `CH_DB_TOKEN_MART`/`CH_DB_TOKEN_DIM`(기본 = CH_DB_MART/CH_DB_DIM; company-verify 격리 시 운영 DB를 가리키게)**), env `MART_METRICS_MAX_MUTATIONS_PER_RUN=64`, `imagePullSecrets: registry-pull-secret`). `batch.py`: 첫 `_run_table` 전 날짜 전체 × 4테이블 `exists` 선조회 → 예정 DELETE 합산 → 초과 시 `FAILURE reason=mutation_budget`. `steps.py`: DB명은 `DB_FACT/DB_DIM/DB_MART/DB_TOKEN_MART/DB_TOKEN_DIM`만, `{{d:Date}}`, `'coalesce('` 부재 단언. `created_by='token-metrics-pipeline'`(기존 `token-pipeline`과 구분; 불변식 `created_by_wrong_metrics`가 이 값 검사).

**읽기 계약(사내 스키마 프리플라이트 — install.sh가 `DESCRIBE`로 확인, 불일치 시 설치 중단)**: `mart.token_usage_1d`(date, service_group, service, model, input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, requests — 9), `mart.agg_token_service_1d`(date, service — 2), `gpu_data.dim_token_service`(service, enabled — 2). **3테이블/13컬럼**, 그 외 컬럼·테이블 의존 없음.

**메트릭 측 소스는 앵커가 있는 (date, service)만**. 공통 CTE: `eff_alias`, `eff_tco`, `eff_alloc`(`HAVING gpu_type != 'unknown'`), `reg`(=`dim_token_metrics_service` 전체 행), `usage_svc`(=`dim_token_service enabled=1` — 토큰 측 모집단), `canon(x) = if(a.canonical = '', x, a.canonical)`, `fail_flag = hasAny(flags, ['hours_over_count','unknown_violation'])`.

- **M0 커버리지(WARN)**: 기대 = `reg WHERE enabled=1 AND coverage_since ≤ d AND (until IS NULL OR d ≤ until)`, 실제 = 앵커. `metrics_coverage=N/M missing_services="…"`. `reg.service ∉ usage_svc` → `CHECK WARN service_not_in_usage_registry`. **M0b**: `mart.agg_token_service_1d`에 D 행 없음 → `CHECK WARN token_mart_absent`(M1은 GPU-only, 토큰 의존 체크·M4 스킵).
- **M1 `agg_token_model_cost_1d`** (date × service × canon): `tok_agg` = `mart.token_usage_1d` **중 `usage_svc` 서비스 전부**(소비 전용 서비스 포함) GROUP BY (date, service, canon(model)) + `1 AS has_rows`; `gpu_agg` = gpu fact GROUP BY (date, service, canon(model)) + `1 AS has_rows`(FAIL 행 포함해 키 유지). **`keys` = 두 키 집합 UNION DISTINCT를 구동 테이블로 양쪽 GLOBAL LEFT JOIN**. 컬럼: date, service_group, service, model(canon), serving/standby/test_gpu_hours(`NOT fail_flag`), flagged_gpu_hours, equiv_gpu_count(=Σhours/24), scaled_intraday, **model_cost_krw**(§6.4 (1) — **serving+standby만, test 제외**; `if(countIf(g.category IN ('serving','standby') AND NOT fail_flag AND isNull(t.tco)) > 0, NULL, sumIf(g.gpu_hours * t.tco, g.category IN ('serving','standby') AND NOT fail_flag))`), input/cache_read/cache_creation/output_tokens·requests, **uncached_tokens**(=input+cache_creation)·**cached_tokens**(=cache_read)·total_tokens(=4합; 표시 3분류는 뷰 규칙 — 정의서 4.1/D5), **weighted_tokens**(= W(s,m) — §6.4 (3) 가중치), **tokens_per_gpu_hour**(= total_tokens / serving_gpu_hours; 0이면 NULL), gpu_type_mix Array(String), model_registered, tco_missing, has_token_rows, has_gpu_rows, **quality_flag**(우선순위 고정: `partial` > `no_tco` > `flagged` > `manual` > `no_metrics`(reg 등록·coverage 기대인데 앵커 없음) > `consumer_only`(reg 미등록 토큰 서비스) > `normal`), created_by. 토큰 단가 p(§6.4 (5))는 컬럼이 아니라 패널 파생(`Σ model_cost / Σ weighted_tokens`). EXPECTED = `uniqExact((date, service, canon))` over 동일 키 UNION ALL(canon 식 문자열 공유).
- **M3 `token_metrics_check_1d`** (컬럼: date, service_group, service, check_name, model DEFAULT '', gpu_type DEFAULT '', severity FAIL\|WARN\|INFO, observed/threshold Nullable(Float64), detail(수·이름만), source_type, created_by): P0-core — `metrics_missing`(FAIL), `partial_load`(FAIL), `rows_rejected`, `unregistered_model`, `hours_over_count`, `unknown_violation`, `pct_non_monotone`, `gpu_type_no_tco`, `serving_missing_for_gpu_model`(requests>0, M0b 통과 시), `serving_without_gpu_serving_row`(expect_gpu=0 면제), `identity_drift`(API만), `service_not_in_usage_registry`, `manual_source`(INFO); stretch — `provider_ambiguous`, `consumer_tokens_exceed_provider`, `vendor_price_missing`, `no_allocation`, `sum_hours_over_allocation`, `gpu_block_empty_unexpected`, `serving_block_empty_unexpected`. EXPECTED = 같은 UNION의 count(). **9/14 알림 표면** = 이 테이블 패널 + `module=token-metrics final=1 status=FAILURE` LogsQL + 수동 통보.
- **M4 `agg_token_model_share_1d`** (P0-stretch; date × model(canon) × service × provider_service): §6.4 (4)~(6) — 서비스 비용 ①②③을 한 테이블로. 컬럼: date, model, service, service_group, provider_service(사외는 벤더 표기), is_provider, denominator_mode(`all_services`\|`provider_reported`\|`token_not_reported`\|`no_provider`\|`provider_ambiguous`\|`external_api`), **service_wtokens**(= W(s,m) — 가중 1/0.1/4)·**model_total_wtokens**(= W(m)), share Nullable(Float64)(= W(s)/W(m)), model_cost_krw Nullable, **allocated_cost_krw** Nullable(내부: = model_cost × share — 정의서 3.6; `external_api`: = (input×krw_per_mtok_input + cache_read×krw_per_mtok_cached + cache_creation×krw_per_mtok_cache_creation + output×krw_per_mtok_output)/1e6 — 정의서 3.9, `dim_token_vendor_price` tier='standard' 기본; 단가 행 부재 시 NULL + `vendor_price_missing`), quality_flag, created_by. 특례: `token_not_reported`(W(m)=0·C>0) → **호스팅 그룹 귀속**의 구현 = 제공자 서비스 행 share=1·전액 + quality_flag(I8; 서비스 비용 패널에서는 '그룹 귀속' 라벨로 분리 표시, §6.2); 전용 모델은 share=1 단일 행(I4). 행 = (그날 그 모델에 토큰이 있는 `usage_svc` 서비스) ∪ (제공자 후보 전부 — 다중이면 후보별 행, share NULL) ∪ (사외 API 모델의 사용 서비스 — 벤더 단가 행 유무와 무관, 부재 시 NULL 행). EXPECTED = 동일 키 집합 uniqExact.
- **M2 `agg_token_gpu_group_1d`** (P0-stretch; **date × service_group × gpu_type** — 정의서 3.1/3.3/3.4의 그룹 귀속·유휴는 쿼터 보유 단위인 serviceGroup grain): allocated_gpu_hours Nullable(= 할당표 `dim_token_gpu_allocation`(service_group 키) date 유효 행의 `allocated_gpu_count × 24`), **group_total_cost_krw**(= allocated × TCO — 정의서 3.4), serving/standby/test_gpu_hours(그룹 합, 비FAIL), reported_gpu_hours_total(플래그 포함 전체 보고), flagged_gpu_hours, **model_cost_sum_krw**(= Σ 그룹 호스팅 모델 C), **test_cost_krw**(= Σ test × TCO — 그룹 귀속, 배분 안 함), **idle_gpu_hours**(= allocated − reported_total; 음수면 0 + `over_report` FAIL — I1), **idle_cost_krw**, **unattributed_cost_krw**(FAIL 플래그 행 × TCO), **identity_gap_krw**(= group_total − model_cost_sum − test_cost − idle_cost − unattributed — I2 검증, ±오차), utilization(= reported_total/allocated), over_report, equiv_gpu_count, tco_missing, allocation_source, quality_flag, created_by. 행 = 그룹에 gpu 행이 있거나 (`unknown` 아닌 할당 행 AND 그룹 내 서비스 앵커 ≥1) 쌍만. 대시보드 "그룹 행"(실험·유휴 별도 표시 — 정의서 §0)의 원천.
- 인라인 검증 `CHECK WARN …`; 메트릭 fact가 없는 날은 토큰-only 행 + NULL + WARN, **절대 FAILURE 아님**. 마커: 날짜당 1줄 `BATCH_RESULT status=… module=mart-metrics metrics_coverage=N/M missing_services="…" rows_mart= rows_check= rows_share= warn= elapsed=`.

### 6.2 대시보드 읽기 계약 (9/14)

공유 계정 **`mart`**로 `mart.agg_token_model_cost_1d_dist`, `mart.token_metrics_check_1d_dist`, (stretch) `mart.agg_token_model_share_1d_dist`·`mart.agg_token_gpu_group_1d_dist`, `fact.raw_token_metrics_serving_1d_dist`(성능 — service×model 단위만; 출처는 `source_type`)를 직접 읽는다. `gpu_data.view_token_*` 4종은 P1. 산출물 `docs/monitoring/grafana_dashboard_token_metrics.json`(Plan 6c, 신규 파일): 모델별 비용 C(serving+standby 분해), **서비스별 총비용 — P0-core 패널 = Σ M1 `model_cost_krw` by service("배부 미적용" 라벨); stretch 패널 = M4 합산(①②③ — §6.4 (6))**, **그룹 행 패널**(그룹 총비용 = ΣC + 실험 + 유휴 + 미귀속 — M2), 토큰 단가 p 파생 패널(**기준월·가동률 병기** — 정의서 3.7), 토큰/GPU-h·**요청당 원가**(1차 효율 지표 — 정의서 §7), TTFT/ITL 추이, 출처(manual-v0 vs API), 데이터 품질(check). **라벨 규칙(정의서 §7)**: 모든 비용 값에 `측정`(C·그룹 총비용) / `배분`(M4 share분) / `추정`(벤더 단가 기반 ③, 콘솔 대사 전) 라벨; 토큰 표시 3분류(Uncached/Cached/Output)는 뷰에서만(D5); 임원용 표현 규칙 준수. 사내 batch_result 패널의 `module=token-metrics` `final=1` 규칙은 §7.5의 예외 항목(소유자 작업).

### 6.3 rerun 체인

`mart/token-metrics/tools/rerun.py`(CRONJOB `token-mart-metrics`, command `app.batch`, `--chunk-days` 기본 7). 수집기 `rerun.py --chain-mart` → 동일 날짜 범위. 토큰 mart(사내 스케줄 — M15에서 확인; GitHub 기준 04:00)와 같은 구간을 backfill하면 **토큰 mart 재수행 후 mart-metrics**. 창: **10:50 이후** + 활성 Job 0 확인.

### 6.4 비용 모델 — **정본 = [비용 모델 정의서 Draft v0.1](../../cost-model-spec.md)** (이 절은 정의서 수식의 파이프라인 매핑; 충돌 시 정의서 우선, 정의서 개정 시 이 절 + mart rerun)

**(1) 모델 비용 C** (정의서 3.2): `model_cost_krw(d, s, m) = Σ_{gpu_type} (serving + standby) gpu_hours × tco_krw_per_gpu_hour(gpu_type, d)`
- **test는 C에 불포함**(그룹 귀속, 정의서 3.3/D2), standby는 포함(D1 — 팀 합의 대기 미결 M20). C는 토큰과 무관(그날 토큰 0이어도 동일).
- 파이프라인 보정: **FAIL 플래그 행(`hours_over_count`, `unknown_violation`)은 C에서 제외**하고 그 비용을 그룹 행 `unattributed_cost_krw`로 노출(정의서에 없는 데이터 품질 케이스 — 물리적으로 불가능하거나 모델 귀속 불가; 정정 시 rerun 복원). `gpu_count` 미사용. TCO는 date 유효 행; 기종 하나라도 NULL이면 C NULL(부분 합 금지).

**(2) 그룹 귀속 비용** (정의서 3.1/3.3/3.4, M2=`agg_token_gpu_group_1d`):
`idle(group, gpu_type) = 할당 − Σ(보고 gpuHours 전체 — 플래그 포함)` (I1: idle ≥ 0, 음수면 `over_report` FAIL + 0 클램프); `실험 비용 = Σ test × TCO`; `유휴 비용 = Σ idle × TCO`; **그룹 총비용 = 할당 × TCO** (I2 항등식: `그룹 총비용 = Σ C + 실험 + 유휴 + unattributed ± 오차` — 검증 포인트, `identity_gap_krw` 컬럼).

**(3) 가중 토큰 W — 배분 키** (정의서 3.5, 상수는 steps.py `W_UNC=1, W_CACHE=0.1, W_OUT=4` — TCO 팀 승인값 정본, 변경 시 상수 교체 + rerun):
`uncached = input_tokens + cache_creation_tokens`, `cached = cache_read_tokens`, `output = output_tokens`; `W(s, m, d) = 1·uncached + 0.1·cached + 4·output`; `W(m, d) = Σ_s W(s, m, d)` — 모집단 = `dim_token_service enabled=1` **전 서비스**(메트릭 미등록 소비 전용 포함; 토큰은 각 서비스 자신의 `/v1/usage` 보고 — 소비자 식별 필드 없이 성립, 전제 §3-3).

**(4) 공유 모델 배분** (정의서 3.6): `부담(s, m) = C(m) × W(s) / W(m)` — 전용 모델은 자동으로 전액 귀속(I4). I3: `Σ_s 부담 = C`(±1원, `share_sum_mismatch`). **W(m)=0 인데 C>0** → 호스팅 그룹 전액 + **`token_not_reported`** 플래그(I8) — 구현은 M4의 제공자 서비스 행(share=1) + quality_flag, 패널에서 '그룹 귀속' 라벨(§6.1 M4 특례와 동일).
- `provider(m)` = 그날 m에 FAIL 없는 **serving/standby** gpu 행(C>0 성립 행)을 가진 서비스 — test 전용 행만 가진 서비스는 비호스팅(정의서 3.2: test는 C를 형성하지 않음); 다중이면 `provider_ambiguous`(후보별 행·share NULL·배부 보류 — P1 `consumes`로 확정); 0개면 — 그날 m의 gpu 행이 **전혀 없으면** 사외 API 후보(아래 (6), `external_api`), gpu 행은 있으나 test뿐이면 `no_provider`(사내 실험 모델, C=0·배분 없음·`vendor_price_missing` 미발화).
- 분모 모드 보정(정의서 미정 케이스): 플랫폼 제공자의 usage가 소비자 호출분을 **포함** 보고하면(`usage_includes_consumers=1`) `W(m) = W(provider, m)`, 제공자 자기분 = `max(W(m) − Σ_{s≠p} W(s), 0)`(`consumer_tokens_exceed_provider` WARN) — 기본(=0)은 Σ 전 서비스. 미결 M4.

**(5) 토큰 단가 p — 파생 표시 전용** (정의서 3.7/D8): `p = C(m) / W(m)` (원/가중토큰), `p_uncached = p`, `p_cached = 0.1·p`, `p_output = 4·p` (원/토큰; 표시 시 ×1e6 → 원/1M, 정의서 5.2) — **비용 입력 아님**(순환), 패널에서 M1·M4 컬럼으로 파생 계산하고 **기준월·가동률 병기**(§6.2).

**(6) 서비스 비용** (정의서 3.8): `서비스 비용(s) = ① Σ C(전용 모델) + ② Σ 공유 모델 부담 + ③ 사외 API 비용` — M4가 ①②③을 한 테이블로 흡수(전용도 share=1 행). **③** = `input × p_in + cache_read × p_cache + cache_creation × p_write + output × p_out`(정의서 3.9; 여기서 `input`은 cacheCreation을 **제외**한 순수 입력 — 3.5의 `uncached`와 혼용 금지, 이중 계산 방지) — `gpu_data.dim_token_vendor_price`(provider × model × tier(처리등급, 기본 'standard') × effective_from → input/cached/cache_creation/output 원/1M) 조인, `denominator_mode='external_api'` 행으로 기록; 단가 행 부재 시 NULL + `vendor_price_missing`(GPU 경로의 `tco_missing`과 대칭). 처리등급 시트 컬럼은 미결 M18, 단가 값·PTU는 M21, 벤더 콘솔 월 대사는 M22. 기존 Layer P(USD, `dim_token_model`)는 무변경 병존(사내 mart 소유) — KRW 벤더 단가와 이중 표기 방지는 패널에서 ③만 사용.

**(7) 할당 기준 비용**(DECISIONS #13) = 그룹 총비용(할당×TCO) — (2)에서 자연 산출(M2 `group_total_cost_krw`). 헤드라인 = 서비스 비용(6) + 그룹 행(실험·유휴), 정의서 §7 표현 규칙 준수.

## 7. 기준정보·검증·운영·범위

### 7.1 불변식 — 별도 파일 `tools/verify/invariants_metrics.sql` + `run_invariants.py --sql <path>`(additive)

기존 `invariants.sql`은 무수정. 신규 5블록(파이프라인 무결성만): `metrics_anchor_missing`, `metrics_gpu_dup_key`, `metrics_serving_dup_key`, `metrics_cost_sum_mismatch`(mart C vs fact `sumIf(gpu_hours, category IN ('serving','standby') AND NOT fail_flag)`×TCO — M1과 동일 술어·NULL 규칙), `created_by_wrong_metrics`(mart 4테이블, `'token-metrics-pipeline'`). stretch(정의서 §8 매핑): `share_sum_mismatch`(I3 — `denominator_mode ∈ {all_services, provider_reported, token_not_reported}` AND C NOT NULL에서 `Σ allocated = C` ±1원; I4는 이의 전용 모델 특례), `group_identity_gap`(I2 — `abs(identity_gap_krw) > 1`), `idle_negative`(I1 — `over_report=1`). I5·I7은 기존 토큰 파이프라인 검증이 커버, I6(reasoningTokens)은 usage v1.2 이후(P2). 신규 테이블 설치 후 실행(GitHub 체크아웃의 `tools/verify`에서 — 사내 분기본에는 `--sql`이 없음).

### 7.2 메타데이터 시트 반입 — `assets/model-catalog/` 신규 파일(기존 파일 무수정)

- P0: 탭 `모델` → CSV → `sheet_to_dim_token_model_alias_insert.py --csv --effective-from D --services collectors/token-metrics/endpoints*.yaml --target-db gpu_data|token_verify_dim`(로스터 생성기 클론; NOT IN 가드·`검증` 앵커·§4.2 검증 6종·쉼표 alias·자동 교정 없음·canonical-only identity). `csv_to_layer_c_dim_insert.py --table gpu_tco|gpu_allocation|vendor_price`(vendor_price는 stretch). 산출 SQL은 **admin 적용**. `--xlsx` 직접 읽기는 P1.
- P1: 탭 `서비스`(engine_type만; owner·URL 미적재), `GPU할당매핑`(+unit 체크), `소비관계`(배부 귀속).
- `.gitignore` 추가: `*metadata*.xlsx`, `*metadata*.csv`, `*gpu_tco*.csv`, `*gpu_allocation*.csv`, `*vendor_price*.csv`, `*manual_metrics*.csv`, `endpoints-metrics.company.yaml`, `dim_token_model_alias_insert*.sql`, `dim_token_gpu_*_insert*.sql`, `dim_token_vendor_price_insert*.sql`, `alert_routing*.json`, `assets/model-catalog/data/`. 생성기 `csv_to_layer_c_dim_insert.py`는 `--table gpu_tco|gpu_allocation|vendor_price` 3종 지원.

### 7.3 mock·테스트·운영

- mock-provider: `GET /v1/metrics`(같은 앱; `_identity()`/`cfg.models` 동일 문자열), `datagen.build_metrics`(결정적·단조 percentile·test `unknown` 1행·engine 고정), `metrics_retention_days=14`, 시나리오 int 플래그 6종, `contract/token-metric-api.yaml` + `contract/tests/check_metrics_api.py` 벤더링(@6a552d2), `run_conformance.sh` 추가(additive — 기존 collector e2e 무영향).
- 단위·e2e: 수집기 `test_{api_client,normalize,writer,main,config}.py`(§5.3 전 규칙, 존재확인 3종, DELETE/INSERT 순서, 배칭, 가드, 레지스트리 diff, 모드×게이트 매트릭스 incl. disabled/unknown service/manual `--replace`), mart `test_{steps,batch}.py`(canon 식 동일·`coalesce` 부재·M4 분모 모드 6종(정의서 5.1 워크 예시를 fixture로 재현: Qwen3-32B 240,000원 배분·I3 합계 보존, 5.2 p 검산)·`share_sum`·가중치 상수 1/0.1/4·quality_flag 우선순위·프리플라이트 13컬럼), e2e `seed_metrics.py` + `ddl_test_dims.sql`(dim 4종 twin + 읽기 계약 3테이블 최소 컬럼 twin).
- 운영 문서: `docs/operations/token-metrics-deploy.md`(신설 — §7.5 절차, 프리플라이트, rerun 창 10:50, `--replace`·`--chunk-days`, 부분 적재 복구, `manual_load.py` 절차), 각 모듈 README, `docs/monitoring/README.md`에 신규 대시보드 절 추가(기존 절 무수정).

### 7.4 명시적 보류

- **P1**: `view_token_*` 4종; 토큰 mart canonical(사내 분기본 동기화 이후); `dim_token_model_meta`(체급); `dim_token_service_meta`+`engine_mismatch`; `dim_token_model_consumes`+`consumes_double_count`+다중 제공자 귀속; `dim_token_gpu_unit_map`+`unit_double_mapped`/`unmapped_unit`+동료 DSCloud 유도 할당; `--xlsx`; 워크스테이션 직접 실행 경로 정식화; `delete_data.py` 타깃; gpu_type 정규화 맵; 날짜 접미 alias 자동 매핑; P0-stretch 미착지분; 공용 패키지 추출(3번째 중복 시).
- **P2**: Prometheus 스크랩 모듈 + 교차검증 임계값; 알림 발송기; 환율/마진 뷰; 메트릭 VM 게이지; RESTATEMENT 감지; `/v1/usage` 소비 서비스 필드(배부 정밀화).

### 7.5 배포 전략 — "새 코드만 새로 배포"

- **기존 자산 zero-diff 목록**: `collectors/token-usage/**`, `mart/token-usage/**`, `assets/user-org/**`, `assets/model-catalog/`의 기존 파일, `tools/verify/invariants.sql`, `docs/operations/{company-verify,stage-runbook,rerun}.md`, `docs/monitoring/grafana_dashboard_token_usage.json`, `.github/workflows/{release-images,test-collector,test-mart}.yml`, 사내 리소스(`token-usage-ch-secret`, `token-usage-endpoints`, `token-usage-ca-bundle`, `token-usage-collector`, `token-mart-daily`, `token-mart-ch-secret`). CI로 강제: 신규 워크플로의 path 필터는 신규 디렉터리만; PR 체크리스트 "기존 모듈 diff 0".
- **유일한 기존 자산 변경 = 사내 batch_result Grafana 대시보드**(레포 밖, 모니터링 소유자 작업): `module=token-metrics`는 하루 8줄이며 일 상태 = `final=1` 줄, `final=1` 부재 = FAILURE; `module=mart-metrics` 1줄. M8 미확정 시 fallback = M3 `metrics_missing` 패널 + 임시 LogsQL `module=token-metrics final=1 status=FAILURE`.
- **공유 Secret 예외**: `registry-pull-secret`은 네임스페이스 공유 — 신규 install.sh는 **없을 때만 생성**, 있으면 손대지 않음.
- **신규 모듈 2개 = 독립 배포 단위**: `./collectors/token-metrics/build.sh company --registry <harbor> --tag <sha7>` → `token-metrics-collector`; `./mart/token-metrics/build.sh company …` → `token-mart-metrics`; 각 `install.sh company --context … --registry … --tag <sha7>`가 자기 리소스만 생성·갱신. Harbor 반입은 sha7 태그의 신규 이미지 2개만(ghcr에서 기존 이미지 태그 `latest` 재빌드가 일어나도 사내 무영향 — 사내는 지정 태그만 반입).
- **DDL/GRANT**: §4.0 매니페스트의 신규 파일만 admin/install 적용. 기존 테이블 ALTER 없음.
- **격리 검증(company-verify)은 선택**: 신규 모듈은 기존 테이블에 쓰지 않으므로 공유 계정 `mart`로 운영 DB에 직접 설치해도 기존 파이프라인 오염 위험이 없다. 권장 = stage(mock) → 운영 직접 설치 → `invariants_metrics.sql`. 격리 모드를 쓰면 `CH_DB_TOKEN_MART/CH_DB_TOKEN_DIM`을 운영 DB로 지정해 토큰 측 읽기를 유지(없으면 GPU-only 검증).
- **롤백**: CronJob 2개 suspend + 신규 테이블 DROP — 기존 파이프라인 영향 0.
- **사내 프리플라이트**: mart install.sh가 §6.1 읽기 계약 13컬럼을 `DESCRIBE`로 확인; 수집기 install.sh가 DB·`dim_token_service` 접근 확인.

## 8. 마스터 스펙 v1.14 개정 목록

| 절 | 개정 |
|---|---|
| §0 | v1.14 행: "메트릭 싱크 확장 — 자매 스펙 2026-08-31 참조" |
| §3 | 레포 트리에 `collectors/token-metrics/`, `mart/token-metrics/`(§5.9 클론 규칙 적용 사례) |
| §4.0 | 물리 표 신규 행 + `--replace` 2단계 IN 배칭 + 모듈별 예산·가드·`--chunk-days`·장부 |
| §4.2/§9-1 | P0 예외: 대시보드가 mart/fact `_dist` 직접 조회(공유 계정 `mart`), `view_token_*` 복사본은 P1 |
| §4.4 | Layer C 스케치 명 → 실제 테이블명, 조인 키 `(date, service, canonical)` via `dim_token_model_alias`, **비용 모델 = [비용 모델 정의서](../../cost-model-spec.md)**(C=(serving+standby)×TCO, test·유휴 그룹 귀속, 가중 W 1/0.1/4 배분, 사외 API 벤더 단가 — §6.4 매핑) |
| §5.2/§5.9-4 | 시간별 모듈 NOT_READY 번역, 소프트 데드라인 안 적재 예약 산식 |
| §5.6/§7.3 | 라벨 `module=token-metrics`(8줄/일, `slot=/final=`, `backoffLimit 0`) · `module=mart-metrics`; 사내 batch_result 패널 규칙 1건(§7.5) |
| §5.9 | 2′ 앵커 DELETE 첫·INSERT 마지막, 3′ 앵커 행, **6조 예외: 메트릭 싱크는 자기 레지스트리 `dim_token_metrics_service`(api_since/coverage_since)를 가지며 토큰 coverage 게이트에 편입되지 않음**, 9 싱크별 데드라인(metrics T+1 10:04 → mart-metrics 10:20) |
| §7.2 | 배포 원칙: 사내 분기본 존재 시 기존 모듈 zero-diff·신규 모듈 독립 배포·`release-images` 분리(§7.5) |
| §8.3 | rerun 체인 표·`--replace`·`--chunk-days`·`manual_load.py`·`run_invariants.py --sql` |
| §9 | #12·#13·#14 "확정(자매 스펙)"; 신규 #21 할당 소스, #22 시트 반입, #23 알림 채널, #24 환율/마진, #25 스크랩 임계값, #26 `/v1/usage` 소비자 필드, #27 사내 분기본 ↔ GitHub 동기화 |

## 9. 미결사항 (Open Questions)

> M7(공유 계정 `mart` 직접 읽기 — 사용자 확정 (c)로 해소)·M13(주말 작업 여부 — 설계와 무관, 삭제)은 **결번**. '확정 방법'의 날짜는 8/31 기준 원안 — §10 재기준 주석과 함께 9/3부터 재배치(M14·M15는 미회신 시 즉시 재요청).

| # | 항목 | 임시 방침 | 확정 방법 |
|---|---|---|---|
| M1 | GPU 기종 TCO(원/GPU·h) 값·산정 기준·이력 시작일; 플레이스홀더 기종 집합 | NULL 플레이스홀더, 값 없으면 cost NULL('n/a') | 재무/인프라 (9/7) |
| M2 | 토큰 단가 p 표시 기준(기준월·가동률 병기, 정의서 3.7) + 요청당 원가 패널 구성 ack | 정의서 §7 그대로 | 보고 담당 |
| M3 | 할당 수치 출처·동료 매핑·`utilization`/`over_report`/`identity_gap_krw` 허용 오차 | 수기 시드(stretch), 오차 ±1원 | GPU 대시보드 소유자 (P1) |
| M4 | 배부 정밀화: 플랫폼 제공자별 `usageIncludesConsumers`; 다중 제공자 모델 귀속(consumes) — 배분 키는 정의서 가중 W(1/0.1/4)로 확정, 가중치 값 자체는 M17 | 기본 0(Σ 전 서비스), 다중 제공자는 보류(`provider_ambiguous`) | 플랫폼 제공 팀 확인 (9/8) |
| M5 | 알림 채널·수신자 | 체크 테이블 패널 + 수동 통보 | 온보딩 안내 시 |
| M6 | fact 4·gpu_data 5·mart 4 테이블 승인 + GRANT/시드 admin 슬롯 | DDL 초안 PR 선리뷰(원안 9/1 → 9/4, 재기준 필요) | 클러스터 소유자·admin |
| M8 | 사내 batch_result 대시보드 `module=token-metrics` 8줄 + `final=1` 규칙 + "부재=FAILURE", `module=mart-metrics` ack | 마커 문법 확정; 미확정 시 §7.5 fallback | 모니터링 소유자 (9/10) |
| M9 | 체급 경계 | params 원시값(P1), view 미고정 | 대시보드 담당 |
| M10 | 스크랩 교차검증 임계값 | P2 | — |
| M11 | `/v1/usage` 보존 하한·RESTATEMENT 메트릭 확장 | 14일 계약 그대로 | 서비스팀 협의 |
| M12 | 시트·CSV 실파일 보관 경로, owner 회신 반영 절차 | 레포 밖 + gitignore | 운영 문서 작성 시 |
| M14 | 서비스별 `apiSince`와 이력 제공 여부 — **기본 = API backfill 없음**(apiSince=2026-09-09, coverageSince=2026-08-26, 8/26~9/8은 manual-v0); 이력 가능 서비스만 apiSince를 앞당겨 `--from/--to` | 기본값 적용 | 서비스 담당자 회신(9/2) |
| M15 | **사내 스키마·스케줄 확인**: `mart.token_usage_1d`·`agg_token_service_1d`·`dim_token_service`의 읽기 계약 13컬럼 존재, 사내 토큰 mart CronJob 스케줄(10:20 순서 전제), `registry-pull-secret`·CA ConfigMap 이름 | install 프리플라이트로 확인 | 사내 DESCRIBE·kubectl get (9/2까지) |
| M16 | 사내 분기본 ↔ GitHub 동기화 계획(P1 토큰 mart canonical 등 전제) | 이번 범위 밖 | 별도 협의 |
| M17 | `w_cached = 0.1` 등 가중치의 TCO 팀 승인(정의서 §10); 실측 교체(정의서 6.1)는 P2 | 1 / 0.1 / 4 상수 | TCO 팀 |
| M18 | 메타데이터 시트 컬럼 추가: `workloadType`(llm-text/embedding/…), 사외 API `처리등급`(tier); non-LLM custom 지표 이름표 | tier='standard' 고정, workloadType 미사용 | 시트 v2 협의 (정의서 §10) |
| M19 | usage API v1.2 `reasoningTokens` + 불변식 I6 — 기존 파이프라인·사내 분기본 영향이라 이번 범위 밖 | 미반영(P2) | usage 스펙 개정 시 |
| M20 | D1(standby를 C에 포함) 팀 합의 → 스펙 레포 DECISIONS.md 반영 | 포함(정의서 D1) | 팀 합의 |
| M21 | 벤더 KRW 단가표 값(provider×model×tier)·Azure PTU 서비스 존재 여부(있으면 예약 용량×시간 = ①류 처리); **정의서 피드백**: 3.9의 `uncached × p_in`은 3.5 정의(`uncached`=input+cacheCreation)와 결합 시 cacheCreation 이중 계산 → `input × p_in`으로 표기 정정 요청, `p_write`는 전체 write 단가(할증분 아님)로 명시 요청; Anthropic TTL별(5분/1시간) write 단가 표현 방식 | 플레이스홀더 NULL, PTU 없음 가정; 파이프라인은 `input×p_in`·전체 write 단가 해석, TTL은 최고 단가 | 운영자/재무·정의서 소유자 |
| M22 | 벤더 콘솔 월 1회 대사 절차(③ '추정' 라벨 제거 조건) | '추정' 라벨 유지 | 운영 문서 작성 시 (정의서 3.9) |

## 10. 구현 순서·일정 (2026-08-31 월 → 09-14 월)

> **재기준 완료(2026-09-06)**: 잔여 일정의 정본은 **[Plan 6a §'일정 재기준 (2026-09-06 기준)'](../plans/2026-09-04-token-metrics-schema.md#일정-재기준-2026-09-06-기준)**이다. 아래 표는 8/31 승인 가정의 원안(보존용). 8/31~9/2 칸(플랜 작성·DDL draft PR·사람 요청 발송·M15 확인)의 실제 진행 여부를 확인해 잔여 작업을 오늘(9/3)부터 재배치할 것 — 승인 시 writing-plans 단계에서 확정한다. 코드 총량(≈5~6 e-day)과 게이트 구조는 불변.

| 일자 | 작업 | 게이트 |
|---|---|---|
| **8/31 (월)** | 설계 승인 → Plan 6a(스키마·기준정보·GRANT·생성기 등록·수기 템플릿 3파일), 6b(mock + `collectors/token-metrics` 클론·로직·`manual_load.py`·CI·배포), 6c(`mart/token-metrics` 클론·M0~M4·프리플라이트·불변식·Grafana·`token-metrics-deploy.md`); 브랜치 `feat/token-metrics`; 스펙 벤더링; **사람 요청 발송**(TCO·할당·시트 `모델` CSV·apiSince/이력·수기 엑셀(템플릿 첨부)·DDL 리뷰 예고·admin/Harbor 슬롯(신규 이미지 2)·대시보드 라벨·KPI 분모·`usageIncludesConsumers`·**사내 DESCRIBE/스케줄/Secret 이름(M15)**) | — |
| 9/1 (화) | Plan 2렌즈 리뷰; **PR draft: DDL**(§4.0 매니페스트 14파일 + 생성기 등록 + 미러 재생성); PR mock `/v1/metrics`; 수집기 모듈 스캐폴딩 클론(기계적, 이름 교체 체크리스트) | fact/gpu_data 소유자 리뷰 요청(사인오프 9/4) |
| 9/2 (수)~9/3 (목) | 수집기 로직(config·레지스트리 diff·api·normalize(TDD)·writer·main·manual 모드), `rerun.py`(`--chunk-days`)·`manual_load.py`, k8s/install/overlay, 단위·e2e·`test-collector-metrics.yml`·`release-images-metrics.yml` | 시트 `모델` 1차 CSV, apiSince 회신, **M15 결과** |
| 9/4 (금) | DDL 사인오프 → 머지; 수집기 머지; stage: 이미지 ghcr push → install → mock 대상 실행(SUCCESS/NODATA/not_ready) + manual CSV 적재 1회 | **HARD: DDL 사인오프** |
| 9/5 (토)~9/6 (일) | 버퍼(개발 작업 없음 가정 — 설계 영향 없음; 수집 CronJob은 주말에도 평소대로 실행) | — |
| 9/7 (월) | mart 모듈: 클론 + M0/M0b/M1/M3 + 프리플라이트 + CronJob/install/rerun + e2e + `test-mart-metrics.yml`; assets 생성기·fixture·gitignore; `invariants_metrics.sql` + `--sql`; `token-metrics-deploy.md`; stage 체인 green; Grafana JSON 초안(core 패널); stretch(M4 배부·M2·할당) 착수; Harbor 반입 요청(sha7 이미지 2) | 시트 최종 CSV·TCO CSV·할당 CSV **9/7 EOD** |
| 9/8 (화) | 사내: admin — accounts 3파일·dim DDL 4·플레이스홀더 시드 4(vendor_price는 stretch)·생성 SQL(alias/TCO 실값); Harbor 반입; `install.sh company` ×2(프리플라이트 통과·`registry-pull-secret` 존재 확인); `endpoints-metrics.company.yaml`; **manual-v0 적재 8/26~9/7**(`manual_load.py`); `invariants_metrics` | **HARD: admin 슬롯·Harbor·수기 수치·M15** |
| 9/9 (수) | 서비스 go-live(첫 데이터 날짜 9/9 → 첫 API 수집은 9/10 02:05). **≤10:00 manual-v0 9/8 적재**; 10:20 mart-metrics(manual 데이터로 M1/M3 첫 산출); ≥10:50 이력 가능 서비스 backfill `rerun.py --from 2026-08-26 --to 2026-09-08 --chunk-days 7 --chain-mart`(manual 앵커는 `--replace` 없으면 SKIPPED) | 서비스 실구현(외부) |
| 9/10 (목) | 첫 API 수집(D=9/9) → 09:05 최종 슬롯 FAILURE=미확정 알림 → 10:20 mart-metrics; rejected/unregistered/identity_drift triage; alias v2·TCO 정정(effective_from 소급, admin) + **mart-metrics rerun만**(`--from 2026-08-26 --to 2026-09-09 --chunk-days 7` — 4×15=60 ≤ 가드 64, 일 총량 68+60=128); Grafana 패널 확정; stretch 마감 | 대시보드 라벨 ack(M8) |
| 9/11 (금) | 프리즈(DDL 동결; stretch 미착지분 P1 이월); 9/10까지 데이터로 dry-run(manual 8/26~9/8 + API 9/9~9/10); manual→API `--replace`는 이력 가능 서비스에 한해 **≤7일 청크**(7×7=49 → 68+49=117 ≤ 150); 문서 머지 | 보고 담당 dry-run 리뷰 |
| 9/12 (토)~9/13 (일) | 자동 수집(주말 포함)·mart-metrics; batch_result 모니터링 | — |
| **9/14 (월)** | 10:20 mart-metrics(D=9/13) → 10:50 패널 export(8/26~9/13; manual-v0 일자 표시; API 5일) | 보고 |

코드 총량 ≈5~6 engineer-day; 크리티컬 패스는 사람 게이트(TCO·할당·시트·DDL 사인오프·admin/Harbor 슬롯·사내 스키마 확인)라 **미발송 요청은 재기준 즉시(9/3) 발송**해야 한다.

작업 컨벤션: 마스터 §10 상속 — `type(scope): 한국어 설명 (Plan 6x Tn)`, scope는 `collectors-metrics`/`mart-metrics`, `feat/<kebab>` 브랜치, draft DDL PR → feat PR → fix → docs, 플랜은 2렌즈 리뷰 후 커밋 제목에 건수. **기존 모듈 디렉터리·§7.5 목록을 건드리는 diff는 PR에서 거부.**
