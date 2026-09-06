# Token Metrics Schema & Reference Data (Plan 6a/6) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 메트릭 싱크(`/v1/metrics`) 반입의 **스키마 계층**을 완성한다 — §4.0 매니페스트 14파일(fact 4·레지스트리 1·mart 4·dim 4·시드 4·accounts 3)의 DDL/GRANT/시드, 생성기(`gen_stage_ddl.py`/`gen_verify_ddl.py`) 등록과 stage·company-verify 미러 재생성, 메타데이터 시트·Layer C 반입 생성기 2종(+테스트), 수기 템플릿 CSV 3파일, `.gitignore` 경계, 마스터 스펙 v1.14 개정 — Plan 6b(수집기)·6c(mart)가 **이 플랜의 테이블·파일·CLI 이름만 참조**하면 되도록 인터페이스를 고정한다.

**Architecture:** 기존 모듈(`collectors/token-usage`, `mart/token-usage`, `assets/user-org`, `assets/model-catalog`의 기존 파일)은 **zero-diff**이고, 신규 DDL은 전부 신규 파일(`collectors/token-metrics/ddl/company/`, `mart/token-metrics/ddl/company/`, `assets/model-catalog/ddl/company/` 신규 파일)로만 추가된다. 모든 테이블은 `<이름>_local`(ReplicatedMergeTree) + `<이름>_dist`(Distributed 'gpu-monitoring') 쌍이며, 두 생성기의 정규식(`ENGINE = Distributed('gpu-monitoring', '<db>', '<table>_local',` / `ENGINE = ReplicatedMergeTree(...)` / `\nON CLUSTER 'gpu-monitoring'`)이 그대로 먹히는 형식으로 작성해 stage·company-verify 미러는 **생성기로만** 만든다. 기준정보 실값은 레포 밖 CSV → 생성기 → gitignore SQL → admin 적용이고, 레포에는 `unknown`·NULL 플레이스홀더 시드와 stage 합성 fixture만 남는다.

**Tech Stack:** ClickHouse 24.8(company 2s×2r / stage 1s×1r, 클러스터 `'gpu-monitoring'`), Python 3.10+(stdlib only — 생성기·테스트), pytest ≥ 8, GitHub Actions(`test-assets.yml` additive), git.

**Spec:**
- 설계 정본: `docs/superpowers/specs/2026-08-31-token-metrics-ingest-design.md` (§4.0–§4.3, §5.5, §6.1, §7.1, §7.2, §7.5, §8, §10)
- 비용 모델 정의서: `docs/cost-model-spec.md` (Draft v0.1 — C=(serving+standby)×TCO, 가중 W 1/0.1/4, 사외 API KRW/MTok)
- 마스터 스펙(v1.13 → v1.14 개정 대상): `docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md`

---

## Global Constraints

- **zero-diff 목록(설계 §7.5)** — 다음은 **한 바이트도 수정하지 않는다**: `collectors/token-usage/**`, `mart/token-usage/**`, `assets/user-org/**`, `assets/model-catalog/`의 기존 파일(`ddl/company/{dim_token_model,seed_dim_token_model,accounts}.sql`, `ddl/stage/*`, `ddl/company-verify/*`의 기존 3파일, `README.md`, `ddl/README.md`), `tools/verify/invariants.sql`, `docs/operations/{company-verify,stage-runbook,rerun}.md`, `docs/monitoring/grafana_dashboard_token_usage.json`, `.github/workflows/{release-images,test-collector,test-mart}.yml`. 각 Task의 커밋 전 `git diff --stat main -- <zero-diff 경로들>`이 빈 출력이어야 한다(각 커밋 Step에 포함).
- **허용된 additive 편집만**: `tools/gen_stage_ddl.py` SOURCES 리스트(항목 추가), `tools/gen_verify_ddl.py` MODULES 리스트(항목 추가), `.github/workflows/test-assets.yml`(paths·grep 디렉터리·job 추가), `.gitignore`(패턴 추가), 마스터 스펙(v1.14 개정 — §8 목록 그대로). 이 플랜은 `tools/verify/run_invariants.py --sql`, `tools/mock-provider/**`, `docs/monitoring/README.md`를 건드리지 않는다(각각 6c·6b·6c).
- **뮤테이션 예산(마스터 §4.0 (c))**: 일 150 / 피크(02:00~03:00) 80. 이 플랜의 산출물(DDL·시드·GRANT)은 뮤테이션 0 — 시드는 `NOT IN` 가드 INSERT만, ALTER 없음. 가드 상수는 6b/6c가 읽는 **문서 값**으로만 등장: `METRICS_MAX_MUTATIONS_PER_RUN=45`, `MART_METRICS_MAX_MUTATIONS_PER_RUN=64`, `--chunk-days 7`.
- **이름 규칙**: 테이블·컬럼·env·CLI·마커 이름은 설계 §4.0–§4.3·§5.5·§6.1·§7.1·§7.2와 **바이트 동일**. gpu_data 테이블은 `dim_token_*` 접두사. 설계가 타입·헤더를 명시하지 않은 곳은 이 플랜이 확정하고 `## Self-Review 노트`의 "설계 해석"에 기록한다.
- **DDL 컨벤션(설계 §4.0 / 마스터 §4.0)**: `CREATE TABLE IF NOT EXISTS <db>.<t>_local` 다음 줄에 단독 `ON CLUSTER 'gpu-monitoring'`; 컬럼 블록은 `(`…`)`가 각각 단독 줄; `ENGINE = ReplicatedMergeTree(\n    '/clickhouse/tables/{shard}/<db>/<t>_local',\n    '{replica}'\n)`; `_dist`는 **정확히** `ENGINE = Distributed('gpu-monitoring', '<db>', '<t>_local',` + 샤딩키 + `);` — 앞 세 인자는 단일 공백 구분으로 한 줄에, 샤딩키는 같은 줄(단일 공백) 또는 다음 들여쓴 줄(기존 `collectors/token-usage/ddl/company/raw_token_usage.sql` 스타일) 둘 다 허용(E2E 단일노드 변환 `[\s\S]*?\);`·`gen_verify_ddl.py` ③ `\s*` 정규식·T2 `\s*` 정규식 모두 개행 허용); `DateTime('Asia/Seoul')`; 문자열 NOT NULL(''); 저카디널리티 문자열은 `LowCardinality(String)`; 숫자 부재는 `Nullable`; `SETTINGS index_granularity = 8192`; fact/mart는 `PARTITION BY toYYYYMM(date)` + `TTL date + INTERVAL 25 MONTH`, dim은 파티션·TTL 없음; `COMMENT`는 `_local`에만, `_dist`는 동일 컬럼 목록에서 COMMENT·DEFAULT 제거; **COMMENT 문자열 안에 `;` 금지**(6b/6c e2e `run_e2e.sh`가 `--` 주석 줄만 걷어낸 뒤 `sql.split(";")`로 문장을 나눈다 — T2 lint가 `_local` 컬럼 블록의 COMMENT 문자열을 검사); mart 테이블은 `created_by LowCardinality(String)`(DEFAULT 없음) + `CONSTRAINT check_created_by CHECK created_by != ''`를 `_local`·`_dist` 양쪽에.
- **GRANT 컨벤션**: 테이블 레벨만(DB 레벨 금지), 전부 `TO mart`. collectors 파일은 `GRANT <priv> ON <db>.<t> TO mart ON CLUSTER 'gpu-monitoring';` 형식(기존 `collectors/token-usage/ddl/company/accounts.sql`과 동일), mart·assets 파일은 정규형 `GRANT ON CLUSTER 'gpu-monitoring' <priv> ON <db>.<t> TO mart;`. ALTER DELETE는 `_local`에만, 감사 테이블은 SELECT·INSERT만. 신규 accounts 파일은 **CREATE DATABASE·CREATE USER를 하지 않는다**(fact/gpu_data/mart는 존재 — 6b install.sh 프리플라이트가 확인).
- **시드 컨벤션(dim_holiday 3요소)**: (a) 출처·기준일 헤더, (b) `WHERE (<키>) NOT IN (SELECT <키> FROM <t>_dist)` 멱등 가드 + `SETTINGS insert_distributed_sync = 1;`, (c) 앵커 `-- 검증: 결과가 비어야 정상 ------------------------------------------------` 뒤 검증 SELECT(`UNION ALL`, 동일 4컬럼 형태 `check_name, key, effective_from, cnt`). 사내 시드는 **`unknown`·NULL 플레이스홀더만**(합성 수치 금지); stage 합성값은 `assets/model-catalog/fixtures/stage_seed_*.sql`(생성기 밖).
- **Python 3.10+ 호환**: `StrEnum`·`tomllib`·`match` 미사용, `from __future__ import annotations` 사용, `datetime.UTC` 미사용. 생성기는 stdlib only(PyYAML 의존 없음). `random` 미사용(결정적 출력 — 같은 입력 → 같은 SQL, 타임스탬프 미기록). 데이터 원문(모델명·서비스명·수치)은 stdout/stderr에 에코하지 않고 행 번호·필드명·건수만 출력.
- **KST 규율**: 모든 DateTime 컬럼은 `DateTime('Asia/Seoul')`; 시드·템플릿의 날짜는 KST 달력일(`Date`). 생성기는 날짜 문자열을 `YYYY-MM-DD`로만 받고 시각·타임존 변환을 하지 않는다.
- **공개 레포 규칙**: 실제 사내 호스트명·주소·프로젝트 코드명·담당자 이메일 금지. 예시는 `harbor.example.internal`, `chi-<cluster>.<ns>.svc`, `http://token-mock-provider-a.monitoring.svc:8000`만. 실데이터 파일(시트 CSV·TCO/할당/단가 CSV·수기 CSV/xlsx·생성 SQL·사내 endpoints)은 Task 1의 `.gitignore` 패턴으로 차단하고 커밋 전 `git status --porcelain`으로 확인.
- **커밋 규약**: `type(scope): 한국어 설명 (Plan 6a Tn)` — scope는 `ddl` / `assets` / `tools` / `ci` / `docs`. 커밋 트레일러 2줄 필수:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` / `Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54`. 브랜치 `feat/token-metrics-schema`(main에서 분기; 설계 브랜치 `feat/token-metrics-design`이 아직 미머지면 그 위에서 분기).
- **테스트 실행 규약**: 로컬은 `cd assets/model-catalog && python3 -m pytest -q`(이 환경에는 `python`이 없고 `python3`만 있다 — 플랜의 로컬 bash 블록은 전부 `python3`); CI `test-assets.yml`의 job만 러너가 제공하는 `python -m pytest`를 쓴다. 모듈 루트에 빈 `conftest.py` + `tests/__init__.py`(user-org 패턴) — `from sheet_to_dim_token_model_alias_insert import …` 성립.

## 일정 재기준 (2026-09-06 기준)

설계 §10의 8/31 원안 중 **8/31~9/5 칸은 미진행**(DDL PR 미생성·사람 요청 미발송·M15 미확인)이며, 플랜 3종은 9/4(금) 착수 → **9/6(일) 완성·커밋**됐다. 잔여 작업을 9/7(월)부터 아래로 재배치한다. 코드 총량(≈5~6 e-day)·게이트 구조는 불변이나 **여유일이 0**이므로 stretch(M4/M2)는 9/10 이후 착지분만 포함하고 나머지는 P1로 이월한다. 주말(9/12~13)은 개발 없음, 기존 수집 CronJob은 평소대로 실행.

| 일자 | 작업 | 게이트 |
|---|---|---|
| 9/6 (일) | Plan 6a/6b/6c 작성·4렌즈 리뷰·커밋(이 문서) | — |
| **9/7 (월)** | **오전: 사람 요청 발송(아래 체크리스트 전부)**; **Plan 6a 실행 = DDL draft PR**(매니페스트 14파일 + 생성기 등록 + 미러 재생성 + 생성기 2종 + 템플릿 3파일 + gitignore + v1.14); 오후: Plan 6b 착수(mock `/v1/metrics` + `collectors/token-metrics` config/normalize/api_client) | fact/gpu_data/mart 소유자 DDL 리뷰 요청(사인오프 목표 **9/8 오전**) |
| **9/8 (화)** | **HARD: DDL 사인오프 → 6a 머지**; Plan 6b 완료(writer/main/manual·`rerun.py`·`manual_load.py`·배포·CI·`release-images-metrics.yml`); stage: 이미지 push → install → mock 대상 실행(SUCCESS/NODATA/not_ready) + manual CSV 적재 1회; stage 시드 fixture 수동 적용; Plan 6c 착수(M0/M0b/M1/M3) | 시트 `모델` CSV·TCO CSV·할당 CSV **9/8 EOD**; apiSince 회신; **M15 결과** |
| **9/9 (수)** | 서비스 go-live(첫 데이터 날짜 9/9). Plan 6c 완료(프리플라이트·`invariants_metrics.sql`+`--sql`·CI·`token-metrics-deploy.md`·Grafana 초안); Harbor 반입 요청(sha7 이미지 2); **사내 admin 슬롯**: accounts 3파일·dim DDL 4·플레이스홀더 시드 4·생성 SQL(alias/TCO 실값); `install.sh company` ×2(프리플라이트·`registry-pull-secret` 존재 확인); `endpoints-metrics.company.yaml`; **저녁: manual-v0 적재 8/26~9/8** → `mart/token-metrics/tools/rerun.py --from 2026-08-26 --to 2026-09-08 --chunk-days 7`(≥10:50 창 충족) | **HARD: admin 슬롯·Harbor·수기 수치·M15** |
| 9/10 (목) | 첫 API 수집(D=9/9, 02:05~09:05 8슬롯) → 10:20 mart-metrics; rejected/unregistered/identity_drift triage; **≥10:50** 이력 가능 서비스 backfill `rerun.py --from 2026-08-26 --to 2026-09-09 --chunk-days 7 --chain-mart`(manual 앵커는 `--replace` 없으면 SKIPPED); alias v2·TCO 정정(effective_from 소급, admin) + mart-metrics rerun(4×15=60 ≤ 64, 일 총량 68+60=128 ≤ 150); Grafana 패널 확정; stretch(M4/M2) 착수 | 대시보드 라벨 ack(M8) |
| 9/11 (금) | **프리즈**(DDL 동결; stretch 미착지분 P1 이월); dry-run(manual 8/26~9/8 + API 9/9~9/10); manual→API `--replace`는 이력 가능 서비스에 한해 **≤7일 청크**(7×7=49 → 68+49=117 ≤ 150); 문서 머지 | 보고 담당 dry-run 리뷰 |
| 9/12 (토)~9/13 (일) | 자동 수집·mart-metrics(주말 포함); batch_result 모니터링 | — |
| **9/14 (월)** | 10:20 mart-metrics(D=9/13) → 10:50 패널 export(8/26~9/13; manual-v0 일자 표시; API 5일) | 보고 |

**HARD 게이트**: ① 9/8 오전 DDL 사인오프(없으면 6b/6c는 stage까지만 진행, 사내 설치 불가) ② 9/9 admin 슬롯 + Harbor 반입 + 수기 수치 + M15(없으면 9/10 첫 산출 불가) ③ 9/11 프리즈.

**사람 요청 발송 체크리스트(9/7 월 오전 발송 — 이 플랜의 Task 10 템플릿·Task 3 DDL 초안을 첨부)**:
- [ ] TCO(원/GPU·h) 값·산정 기준(basis)·이력 시작일 — 재무/인프라(M1); 회신 형식 = Task 9 `--table gpu_tco` CSV 헤더
- [ ] GPU 할당표(serviceGroup × gpu_type × 장수) 출처·수치 — GPU 대시보드 소유자(M3); 회신 형식 = `--table gpu_allocation` CSV 헤더
- [ ] 메타데이터 시트 `모델` 탭 CSV(canonical·aliases·defining_service) — 시트 소유자(M18); 회신 형식 = Task 8 CSV 헤더
- [ ] 서비스별 `apiSince`·이력(backfill) 제공 여부 — 서비스 담당자(M14; 기본 apiSince=2026-09-09·coverageSince=2026-08-26)
- [ ] 수기 manual-v0 엑셀(8/26~9/8) — 서비스 담당자; **Task 10 템플릿 3파일 첨부**
- [ ] DDL 리뷰 예고(이 PR 링크) — fact/gpu_data/mart 소유자(M6), 사인오프 9/8 오전
- [ ] admin 슬롯(9/9) — accounts 3파일·dim 4·시드 4·생성 SQL 적용 + Harbor 반입 슬롯(신규 이미지 2: `token-metrics-collector`, `token-mart-metrics`, sha7 태그)
- [ ] 대시보드 라벨 규칙(`module=token-metrics` 8줄/일·`final=1`·부재=FAILURE, `module=mart-metrics`) — 모니터링 소유자(M8)
- [ ] 플랫폼 제공자별 `usageIncludesConsumers` — 플랫폼 제공 팀(M4)
- [ ] **M15 사내 확인**: `DESCRIBE mart.token_usage_1d`(9컬럼)·`mart.agg_token_service_1d`(date, service)·`gpu_data.dim_token_service`(service, enabled), 토큰 mart CronJob 스케줄(10:20 순서 전제), `registry-pull-secret`·CA ConfigMap 이름
- [ ] 벤더 KRW 단가표(provider×model×tier)·PTU 존재 여부 — 운영자/재무(M21; stretch)

## File Structure

```text
.gitignore                                                      # Modify (additive): §7.2 패턴 12종
tools/gen_stage_ddl.py                                          # Modify (additive): SOURCES += 14
tools/gen_verify_ddl.py                                         # Modify (additive): MODULES += 2
.github/workflows/test-assets.yml                               # Modify (additive): paths +4, grep dirs +2, job unit-model-catalog
docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md # Modify: v1.14 개정(설계 §8 표 그대로)

collectors/token-metrics/ddl/
├── README.md                                                   # Create: 파일표·뮤테이션 장부(설계 §4.0 표 그대로)·적용 순서
├── company/
│   ├── raw_token_metrics.sql                                   # Create: fact 4테이블 (_local/_dist ×4)
│   ├── dim_token_metrics_service.sql                           # Create: gpu_data.dim_token_metrics_service
│   └── accounts.sql                                            # Create: GRANT (collectors 형식)
├── stage/{raw_token_metrics,dim_token_metrics_service,accounts}.sql          # Generated: gen_stage_ddl.py
└── company-verify/{raw_token_metrics,dim_token_metrics_service,accounts}.sql # Generated: gen_verify_ddl.py

mart/token-metrics/ddl/
├── README.md                                                   # Create
├── company/
│   ├── mart_metrics_tables.sql                                 # Create: mart 4테이블
│   └── accounts.sql                                            # Create: GRANT (정규형)
├── stage/{mart_metrics_tables,accounts}.sql                    # Generated
└── company-verify/{mart_metrics_tables,accounts}.sql           # Generated

assets/model-catalog/
├── conftest.py                                                 # Create (빈 파일 — import 경로)
├── sheet_to_dim_token_model_alias_insert.py                    # Create: 시트 `모델` CSV → alias INSERT SQL
├── csv_to_layer_c_dim_insert.py                                # Create: --table gpu_tco|gpu_allocation|vendor_price
├── ddl/company/
│   ├── dim_token_model_alias.sql                               # Create
│   ├── dim_token_gpu_tco.sql                                   # Create
│   ├── dim_token_gpu_allocation.sql                            # Create
│   ├── dim_token_vendor_price.sql                              # Create
│   ├── seed_dim_token_model_alias.sql                          # Create: unknown identity 플레이스홀더
│   ├── seed_dim_token_gpu_tco.sql                              # Create: unknown + H100/A100/H200/L40S NULL
│   ├── seed_dim_token_gpu_allocation.sql                       # Create: (unknown, unknown) NULL
│   ├── seed_dim_token_vendor_price.sql                         # Create: (unknown, unknown, standard) NULL
│   └── accounts_metrics.sql                                    # Create: dim 4종 SELECT → mart
├── ddl/stage/<위 9파일>                                        # Generated
├── ddl/company-verify/<위 9파일>                               # Generated (glob)
├── fixtures/
│   ├── stage_seed_dim_token_model_alias.sql                    # Create: stage 합성(mock 모델 3종)
│   ├── stage_seed_dim_token_gpu_tco.sql                        # Create: stage 합성 TCO
│   ├── stage_seed_dim_token_gpu_allocation.sql                 # Create: stage 합성 할당
│   ├── stage_seed_dim_token_vendor_price.sql                   # Create: stage 합성 벤더 단가
│   ├── synthetic_model_sheet.csv                               # Create: 생성기 테스트 입력
│   ├── synthetic_endpoints_metrics.yaml                        # Create: --services 테스트 입력
│   ├── synthetic_layer_c_tco.csv                               # Create
│   ├── synthetic_layer_c_allocation.csv                        # Create
│   └── synthetic_layer_c_price.csv                             # Create
└── tests/
    ├── __init__.py                                             # Create (빈 파일)
    ├── test_ddl_manifest.py                                    # Create: 14파일 컨벤션 lint (T2)
    ├── test_sheet_alias_tool.py                                # Create (T8)
    └── test_layer_c_tool.py                                    # Create (T9)

docs/templates/
├── token_metrics_manual_v0_gpu.csv                             # Create
├── token_metrics_manual_v0_serving.csv                         # Create
└── token_metrics_manual_v0_engine.csv                          # Create
```

Task 순서: T1 gitignore → T2 DDL 매니페스트 lint 테스트(RED) → T3 collectors DDL → T4 mart DDL → T5 assets dim DDL + accounts_metrics → T6 시드 4 + stage fixture 4(lint GREEN) → T7 생성기 등록·미러 재생성·CI → T8 alias 생성기 → T9 Layer C 생성기 → T10 수기 템플릿 → T11 마스터 스펙 v1.14.

---

### Task 1: `.gitignore` — 메타데이터 시트·Layer C·수기 실데이터 경계 (설계 §7.2)

**Files:**
- Modify (additive): `.gitignore` (현재 16행 — 말미에 블록 추가, 기존 행 무수정)
- Test: `git check-ignore -q` 양성/음성 검증 (Step 2)

**Interfaces:**
- Consumes: 설계 §7.2 gitignore 목록 12패턴(그대로).
- Produces: 다음 파일이 커밋 불가 — `*metadata*.xlsx`, `*metadata*.csv`, `*gpu_tco*.csv`, `*gpu_allocation*.csv`, `*vendor_price*.csv`, `*manual_metrics*.csv`, `endpoints-metrics.company.yaml`, `dim_token_model_alias_insert*.sql`, `dim_token_gpu_*_insert*.sql`, `dim_token_vendor_price_insert*.sql`, `alert_routing*.json`, `assets/model-catalog/data/`. 생성기 기본 `--out` 이름(`dim_token_model_alias_insert.sql`, `dim_token_gpu_tco_insert.sql`, `dim_token_gpu_allocation_insert.sql`, `dim_token_vendor_price_insert.sql`)은 전부 이 패턴에 걸린다. 레포 fixture·템플릿 이름은 이 패턴을 **피해서** 정한다(`synthetic_layer_c_*.csv`, `token_metrics_manual_v0_*.csv`, `stage_seed_dim_token_*.sql`).

- [ ] **Step 0: 전제 — 미추적 플랜 문서 3종 커밋 (clean tree 확보)**

시작 브랜치(`feat/token-metrics-design` @4035532)에는 Plan 6a/6b/6c 문서 3종이 미추적(`??`)으로 남아 있다. 이후 모든 Task의 `git status` 기대값(T7 Step 4 `28`·Step 6 `0`, T8~T11 "위 출력 없음", 완료 기준 "추적 외 파일 0")은 clean tree 전제이므로 분기 전에 먼저 커밋한다.

```bash
cd /home/mini/github/token-data-pipeline
git status --porcelain --untracked-files=all
# 기대: 아래 3줄만 — 그 외 줄이 있으면 먼저 정리한다(이미 커밋돼 출력이 비면 이 Step은 건너뛴다)
#   ?? docs/superpowers/plans/2026-09-04-token-metrics-collector.md
#   ?? docs/superpowers/plans/2026-09-04-token-metrics-mart.md
#   ?? docs/superpowers/plans/2026-09-04-token-metrics-schema.md
git add docs/superpowers/plans/2026-09-04-token-metrics-collector.md docs/superpowers/plans/2026-09-04-token-metrics-mart.md docs/superpowers/plans/2026-09-04-token-metrics-schema.md
git commit -m "docs: Plan 6a/6b/6c 구현 계획 3종 반입 (Plan 6a T1 전제)

writing-plans 산출물 — docs/superpowers/plans/2026-09-04-token-metrics-{schema,collector,mart}.md. 코드 변경 없음.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
git status --porcelain --untracked-files=all | wc -l
# 기대: 0 (clean tree — 이후 Task의 git status 기대값 전제)
```

- [ ] **Step 1: 브랜치 생성 + `.gitignore` 블록 추가**

```bash
cd /home/mini/github/token-data-pipeline
git checkout -b feat/token-metrics-schema
cat >> .gitignore <<'GI'

# 설계 2026-08-31 §7.2 (Plan 6a): 메타데이터 시트·Layer C 실값·수기 CSV·생성 SQL·사내 endpoints 반입 금지
*metadata*.xlsx
*metadata*.csv
*gpu_tco*.csv
*gpu_allocation*.csv
*vendor_price*.csv
*manual_metrics*.csv
endpoints-metrics.company.yaml
dim_token_model_alias_insert*.sql
dim_token_gpu_*_insert*.sql
dim_token_vendor_price_insert*.sql
alert_routing*.json
assets/model-catalog/data/
GI
tail -n 14 .gitignore
```

기대: 마지막 14줄이 위 블록(빈 줄 + 주석 + 12패턴).

- [ ] **Step 2: 양성·음성 검증 (`git check-ignore -q` — exit 0 = 무시, 1 = 무시 안 됨)**

```bash
cd /home/mini/github/token-data-pipeline
# 양성 — 전부 무시돼야 함 (13줄 = 패턴 12 + data/ 디렉터리 예시 1)
for p in \
  assets/model-catalog/data/metadata_2026-09-07.xlsx \
  metadata_models.csv \
  x/team_gpu_tco_2026.csv \
  x/gpu_allocation_v1.csv \
  x/vendor_price_krw.csv \
  x/svcA_manual_metrics_0826.csv \
  collectors/token-metrics/endpoints-metrics.company.yaml \
  dim_token_model_alias_insert.sql \
  dim_token_gpu_tco_insert.sql \
  dim_token_gpu_allocation_insert_v2.sql \
  dim_token_vendor_price_insert.sql \
  docs/alert_routing_company.json \
  assets/model-catalog/data/anything.txt; do
  git check-ignore -q "$p" && echo "IGNORED  $p" || { echo "NOT IGNORED (BUG) $p"; exit 1; }
done
# 음성 — 레포 산출물은 무시되면 안 됨 (0건 무시)
for p in \
  collectors/token-metrics/ddl/company/raw_token_metrics.sql \
  collectors/token-metrics/ddl/company/dim_token_metrics_service.sql \
  collectors/token-metrics/ddl/company/accounts.sql \
  mart/token-metrics/ddl/company/mart_metrics_tables.sql \
  mart/token-metrics/ddl/company/accounts.sql \
  assets/model-catalog/ddl/company/dim_token_model_alias.sql \
  assets/model-catalog/ddl/company/dim_token_gpu_tco.sql \
  assets/model-catalog/ddl/company/dim_token_gpu_allocation.sql \
  assets/model-catalog/ddl/company/dim_token_vendor_price.sql \
  assets/model-catalog/ddl/company/seed_dim_token_model_alias.sql \
  assets/model-catalog/ddl/company/seed_dim_token_gpu_tco.sql \
  assets/model-catalog/ddl/company/seed_dim_token_gpu_allocation.sql \
  assets/model-catalog/ddl/company/seed_dim_token_vendor_price.sql \
  assets/model-catalog/ddl/company/accounts_metrics.sql \
  assets/model-catalog/fixtures/stage_seed_dim_token_gpu_tco.sql \
  assets/model-catalog/fixtures/synthetic_model_sheet.csv \
  assets/model-catalog/fixtures/synthetic_endpoints_metrics.yaml \
  assets/model-catalog/fixtures/synthetic_layer_c_tco.csv \
  assets/model-catalog/fixtures/synthetic_layer_c_allocation.csv \
  assets/model-catalog/fixtures/synthetic_layer_c_price.csv \
  docs/templates/token_metrics_manual_v0_gpu.csv \
  docs/templates/token_metrics_manual_v0_serving.csv \
  docs/templates/token_metrics_manual_v0_engine.csv \
  collectors/token-metrics/endpoints.yaml; do
  git check-ignore -q "$p" && { echo "IGNORED (BUG) $p"; exit 1; } || echo "ok       $p"
done
echo "check-ignore OK"
```

기대: 양성 13줄 전부 `IGNORED`, 음성 24줄 전부 `ok`, 마지막 `check-ignore OK`. (파일이 아직 없어도 `check-ignore`는 경로 문자열만으로 판정한다.)

- [ ] **Step 3: 커밋**

```bash
cd /home/mini/github/token-data-pipeline
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml
# 기대: 출력 없음 (zero-diff)
git add .gitignore
git commit -m "chore(assets): 메타데이터 시트·Layer C·수기 실데이터 gitignore 경계 추가 (Plan 6a T1)

설계 2026-08-31 §7.2의 12패턴 그대로. 생성기 기본 --out 이름 4종·사내 endpoints-metrics.company.yaml·assets/model-catalog/data/ 차단.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

### Task 2: DDL 매니페스트 lint 테스트 (RED) — `assets/model-catalog/tests/test_ddl_manifest.py`

**Files:**
- Create: `assets/model-catalog/conftest.py` (0바이트 — `assets/user-org/conftest.py`와 동일 목적: 모듈 루트를 import 경로에)
- Create: `assets/model-catalog/tests/__init__.py` (0바이트)
- Create: `assets/model-catalog/tests/test_ddl_manifest.py`
- Test: 자기 자신 (T2 직후는 FAIL — 매니페스트 파일 부재; T3~T6이 만든 뒤 GREEN)

**Interfaces:**
- Consumes: 설계 §4.0 물리 표(테이블·DB·PARTITION·ORDER BY·샤딩키·TTL) + §4.2 GRANT 표 + 시드 규칙. 레포 루트 = `Path(__file__).resolve().parents[3]`.
- Produces: 14파일에 대한 기계 검증 — (1) 존재·탭 없음·개행 종료, (2) `_local`/`_dist` 쌍·ZK 경로·`ON CLUSTER` 단독 줄·PARTITION/ORDER BY/TTL/샤딩키·`index_granularity`, (3) `_local`↔`_dist` (컬럼명, 타입) 동일 + `_dist`에 COMMENT/DEFAULT 없음 + `_local` COMMENT 문자열에 `;` 없음(e2e `split(";")` 호환), (4) mart는 `CONSTRAINT check_created_by` 양쪽, (5) E2E 단일노드 변환 정규식 2종 적용 후 잔존 0, (6) accounts 3파일의 GRANT 집합 = §4.2 표(집합 동일), (7) 시드 4파일의 3요소 + 플레이스홀더 규칙, (8) stage fixture 4파일. 이 테스트가 GREEN이면 6b/6c의 e2e(`run_e2e.sh`)가 이 DDL을 단일노드로 변환해 적용할 수 있다.

- [ ] **Step 1: 빈 conftest·패키지 파일 생성**

```bash
cd /home/mini/github/token-data-pipeline/assets/model-catalog
mkdir -p tests fixtures
: > conftest.py
: > tests/__init__.py
wc -c conftest.py tests/__init__.py
```

기대: 두 파일 모두 `0`.

- [ ] **Step 2: lint 테스트 작성 (실패해야 함)**

`assets/model-catalog/tests/test_ddl_manifest.py`:

```python
"""설계 2026-08-31 §4.0 P0 DDL 매니페스트(14파일) 컨벤션 lint (Plan 6a T2).

목적: 두 생성기(tools/gen_stage_ddl.py, tools/gen_verify_ddl.py)와 e2e 단일노드 변환
정규식이 그대로 먹히는 형식인지, §4.0 물리 표·§4.2 GRANT 표·시드 3요소가 파일에
그대로 반영됐는지를 기계로 검증한다. ClickHouse 문법 검증은 6b/6c e2e가 담당.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
CLUSTER = "gpu-monitoring"

COLLECTORS_DIR = "collectors/token-metrics/ddl/company"
MART_DIR = "mart/token-metrics/ddl/company"
ASSETS_DIR = "assets/model-catalog/ddl/company"
FIXTURES_DIR = "assets/model-catalog/fixtures"

MANIFEST = [
    f"{COLLECTORS_DIR}/raw_token_metrics.sql",
    f"{COLLECTORS_DIR}/dim_token_metrics_service.sql",
    f"{COLLECTORS_DIR}/accounts.sql",
    f"{MART_DIR}/mart_metrics_tables.sql",
    f"{MART_DIR}/accounts.sql",
    f"{ASSETS_DIR}/dim_token_model_alias.sql",
    f"{ASSETS_DIR}/dim_token_gpu_tco.sql",
    f"{ASSETS_DIR}/dim_token_gpu_allocation.sql",
    f"{ASSETS_DIR}/dim_token_vendor_price.sql",
    f"{ASSETS_DIR}/seed_dim_token_model_alias.sql",
    f"{ASSETS_DIR}/seed_dim_token_gpu_tco.sql",
    f"{ASSETS_DIR}/seed_dim_token_gpu_allocation.sql",
    f"{ASSETS_DIR}/seed_dim_token_vendor_price.sql",
    f"{ASSETS_DIR}/accounts_metrics.sql",
]

# (id, rel_path, db, table, partition, order_by, sharding, kind)  kind: fact | dim | mart
TABLES = [
    ("collectors_gpu", f"{COLLECTORS_DIR}/raw_token_metrics.sql", "fact", "raw_token_metrics_gpu_1d",
     "toYYYYMM(date)", "(date, service, model, gpu_type, category)", "cityHash64(service)", "fact"),
    ("collectors_serving", f"{COLLECTORS_DIR}/raw_token_metrics.sql", "fact", "raw_token_metrics_serving_1d",
     "toYYYYMM(date)", "(date, service, model, metric, name)", "cityHash64(service)", "fact"),
    ("collectors_summary", f"{COLLECTORS_DIR}/raw_token_metrics.sql", "fact", "raw_token_metrics_summary_1d",
     "toYYYYMM(date)", "(date, service)", "cityHash64(service)", "fact"),
    ("collectors_audit", f"{COLLECTORS_DIR}/raw_token_metrics.sql", "fact", "collect_audit_metrics_1d",
     "toYYYYMM(date)", "(date, service, replaced_at)", "cityHash64(service)", "fact"),
    ("collectors_registry", f"{COLLECTORS_DIR}/dim_token_metrics_service.sql", "gpu_data", "dim_token_metrics_service",
     None, "(service)", "rand()", "dim"),
    ("mart_cost", f"{MART_DIR}/mart_metrics_tables.sql", "mart", "agg_token_model_cost_1d",
     "toYYYYMM(date)", "(date, service, model)", "cityHash64(service)", "mart"),
    ("mart_check", f"{MART_DIR}/mart_metrics_tables.sql", "mart", "token_metrics_check_1d",
     "toYYYYMM(date)", "(date, service, check_name, model, gpu_type)", "cityHash64(service)", "mart"),
    ("mart_share", f"{MART_DIR}/mart_metrics_tables.sql", "mart", "agg_token_model_share_1d",
     "toYYYYMM(date)", "(date, model, service, provider_service)", "cityHash64(model)", "mart"),
    ("mart_group", f"{MART_DIR}/mart_metrics_tables.sql", "mart", "agg_token_gpu_group_1d",
     "toYYYYMM(date)", "(date, service_group, gpu_type)", "cityHash64(service_group)", "mart"),
    ("assets_dim_alias", f"{ASSETS_DIR}/dim_token_model_alias.sql", "gpu_data", "dim_token_model_alias",
     None, "(alias, effective_from)", "cityHash64(alias)", "dim"),
    ("assets_dim_tco", f"{ASSETS_DIR}/dim_token_gpu_tco.sql", "gpu_data", "dim_token_gpu_tco",
     None, "(gpu_type, effective_from)", "cityHash64(gpu_type)", "dim"),
    ("assets_dim_allocation", f"{ASSETS_DIR}/dim_token_gpu_allocation.sql", "gpu_data", "dim_token_gpu_allocation",
     None, "(service_group, gpu_type, effective_from)", "cityHash64(service_group)", "dim"),
    ("assets_dim_vendor_price", f"{ASSETS_DIR}/dim_token_vendor_price.sql", "gpu_data", "dim_token_vendor_price",
     None, "(provider, model, tier, effective_from)", "cityHash64(model)", "dim"),
]

# 설계 §4.1/§4.2/§4.3/§6.1 컬럼 목록 (순서 포함) — _local 컬럼명은 이 목록과 정확히 같아야 한다
COLUMNS = {
    "raw_token_metrics_gpu_1d": [
        "date", "service_group", "service", "model", "gpu_type", "category", "gpu_count", "gpu_hours",
        "flags", "source_type", "generated_at", "collected_at"],
    "raw_token_metrics_serving_1d": [
        "date", "service_group", "service", "model", "metric", "name", "unit", "p50", "p90", "p95", "p99",
        "flags", "source_type", "generated_at", "collected_at"],
    "raw_token_metrics_summary_1d": [
        "date", "service_group", "service", "reported_service_group", "reported_service", "engine_type",
        "engine_version", "gpu_rows", "serving_rows", "custom_rows", "rejected_rows", "merged_dups",
        "source_type", "generated_at", "collected_at"],
    "collect_audit_metrics_1d": [
        "date", "service", "prev_generated_at", "prev_collected_at", "prev_source_type", "prev_gpu_rows",
        "prev_gpu_hours_sum", "prev_serving_rows", "replaced_at"],
    "dim_token_metrics_service": [
        "service_group", "service", "base_url", "enabled", "api_since", "coverage_since", "until",
        "expect_gpu", "expect_serving", "usage_includes_consumers", "note", "updated_at"],
    "agg_token_model_cost_1d": [
        "date", "service_group", "service", "model", "serving_gpu_hours", "standby_gpu_hours", "test_gpu_hours",
        "flagged_gpu_hours", "equiv_gpu_count", "scaled_intraday", "model_cost_krw", "input_tokens",
        "cache_read_tokens", "cache_creation_tokens", "output_tokens", "requests", "uncached_tokens",
        "cached_tokens", "total_tokens", "weighted_tokens", "tokens_per_gpu_hour", "gpu_type_mix",
        "model_registered", "tco_missing", "has_token_rows", "has_gpu_rows", "quality_flag", "created_by"],
    "token_metrics_check_1d": [
        "date", "service_group", "service", "check_name", "model", "gpu_type", "severity", "observed",
        "threshold", "detail", "source_type", "created_by"],
    "agg_token_model_share_1d": [
        "date", "model", "service", "service_group", "provider_service", "is_provider", "denominator_mode",
        "service_wtokens", "model_total_wtokens", "share", "model_cost_krw", "allocated_cost_krw",
        "quality_flag", "created_by"],
    "agg_token_gpu_group_1d": [
        "date", "service_group", "gpu_type", "allocated_gpu_hours", "group_total_cost_krw", "serving_gpu_hours",
        "standby_gpu_hours", "test_gpu_hours", "reported_gpu_hours_total", "flagged_gpu_hours",
        "model_cost_sum_krw", "test_cost_krw", "idle_gpu_hours", "idle_cost_krw", "unattributed_cost_krw",
        "identity_gap_krw", "utilization", "over_report", "equiv_gpu_count", "tco_missing",
        "allocation_source", "quality_flag", "created_by"],
    "dim_token_model_alias": ["alias", "effective_from", "canonical", "defining_service", "source", "note"],
    "dim_token_gpu_tco": ["gpu_type", "effective_from", "tco_krw_per_gpu_hour", "currency", "basis", "note"],
    "dim_token_gpu_allocation": ["service_group", "gpu_type", "effective_from", "allocated_gpu_count", "source", "note"],
    "dim_token_vendor_price": [
        "provider", "model", "tier", "effective_from", "krw_per_mtok_input", "krw_per_mtok_cached",
        "krw_per_mtok_cache_creation", "krw_per_mtok_output", "note"],
}

_CREATE_RE = re.compile(
    r"CREATE TABLE IF NOT EXISTS (?P<name>[a-z_]+\.[a-z0-9_]+)\n"
    r"ON CLUSTER 'gpu-monitoring'\n"
    r"\(\n(?P<cols>.*?)\n\)\n"
    r"(?P<engine>ENGINE = .*?;)",
    re.S,
)
# e2e 단일노드 변환 (collectors/mart/assets run_e2e.sh 공통) — 이 두 정규식이 전 ENGINE에 매치해야 한다
_E2E_REPL_RE = re.compile(r"ENGINE = ReplicatedMergeTree\([^)]*\)", re.S)
_E2E_DIST_RE = re.compile(r"ENGINE = Distributed\('gpu-monitoring', '(\w+)', '(\w+)',[\s\S]*?\);")

# GRANT 두 형식 (collectors 형식 / 정규형)
_GRANT_COLLECTORS_RE = re.compile(r"^GRANT (?P<priv>.+?) ON (?P<tbl>\S+)\s+TO mart ON CLUSTER 'gpu-monitoring';$")
_GRANT_CANONICAL_RE = re.compile(r"^GRANT ON CLUSTER 'gpu-monitoring' (?P<priv>.+?) ON (?P<tbl>\S+)\s+TO mart;$")


def _read(rel: str) -> str:
    path = REPO_ROOT / rel
    assert path.exists(), f"매니페스트 파일 부재: {rel}"
    return path.read_text(encoding="utf-8")


def _blocks(text: str) -> dict:
    return {m.group("name"): m for m in _CREATE_RE.finditer(text)}


def _columns(cols_text: str) -> list:
    """(name, type) 목록 — CONSTRAINT·주석·빈 줄 제외. 타입은 두 번째 토큰(콤마 제거)."""
    out = []
    for raw in cols_text.split("\n"):
        line = raw.strip()
        if not line or line.startswith("--") or line.startswith("CONSTRAINT"):
            continue
        tokens = line.split()
        out.append((tokens[0], tokens[1].rstrip(",")))
    return out


def _grants(text: str, form: str) -> set:
    pat = _GRANT_COLLECTORS_RE if form == "collectors" else _GRANT_CANONICAL_RE
    found = set()
    for raw in text.split("\n"):
        line = raw.strip()
        if not line or line.startswith("--"):
            continue
        m = pat.match(line)
        assert m, f"GRANT 형식 위반({form}): {line}"
        privs = frozenset(p.strip() for p in m.group("priv").split(","))
        found.add((privs, m.group("tbl")))
    return found


# ---------------------------------------------------------------- 파일 공통

def _code_lines(text: str) -> str:
    return "\n".join(l for l in text.split("\n") if not l.lstrip().startswith("--"))


@pytest.mark.parametrize("rel", MANIFEST, ids=[p.split("/")[-1] + "@" + p.split("/")[0] for p in MANIFEST])
def test_manifest_file_hygiene(rel):
    text = _read(rel)
    code = _code_lines(text)
    assert "\t" not in text, "탭 금지"
    assert text.endswith("\n"), "개행으로 종료"
    assert "CREATE DATABASE" not in code, "신규 파일은 DB를 만들지 않는다 (fact/gpu_data/mart 존재 전제)"
    assert "CREATE USER" not in code
    assert "harbor." not in text and "@" not in text, "공개 레포 — 사내 주소·이메일 금지"


# ---------------------------------------------------------------- 테이블 쌍

@pytest.mark.parametrize("case", TABLES, ids=[c[0] for c in TABLES])
def test_table_pair_conventions(case):
    _, rel, db, table, partition, order_by, sharding, kind = case
    text = _read(rel)
    blocks = _blocks(text)
    local_name = f"{db}.{table}_local"
    dist_name = f"{db}.{table}_dist"
    assert local_name in blocks, f"{local_name} CREATE 블록 부재 (ON CLUSTER 단독 줄·'(' ')' 단독 줄 형식 확인)"
    assert dist_name in blocks, f"{dist_name} CREATE 블록 부재"
    local = blocks[local_name]
    dist = blocks[dist_name]

    engine_local = local.group("engine")
    expected_repl = (
        "ENGINE = ReplicatedMergeTree(\n"
        f"    '/clickhouse/tables/{{shard}}/{db}/{table}_local',\n"
        "    '{replica}'\n"
        ")"
    )
    assert engine_local.startswith(expected_repl), f"{local_name}: ReplicatedMergeTree/ZK 경로 형식"
    assert f"ORDER BY {order_by}" in engine_local, f"{local_name}: ORDER BY {order_by}"
    assert "SETTINGS index_granularity = 8192;" in engine_local
    if kind == "dim":
        assert "PARTITION BY" not in engine_local, f"{local_name}: dim은 파티션 없음"
        assert "TTL" not in engine_local, f"{local_name}: dim은 TTL 없음"
    else:
        assert f"PARTITION BY {partition}" in engine_local
        assert "TTL date + INTERVAL 25 MONTH" in engine_local

    engine_dist = dist.group("engine")
    expected_dist = re.compile(
        r"^ENGINE = Distributed\('gpu-monitoring', '" + re.escape(db) + r"', '" + re.escape(table)
        + r"_local',\s*" + re.escape(sharding) + r"\);$"
    )
    assert expected_dist.match(engine_dist), f"{dist_name}: Distributed 인자 형식 — {engine_dist!r}"

    local_cols = _columns(local.group("cols"))
    dist_cols = _columns(dist.group("cols"))
    assert [c[0] for c in local_cols] == COLUMNS[table], f"{local_name}: 컬럼 목록·순서가 설계와 다름"
    assert local_cols == dist_cols, f"{table}: _local/_dist (컬럼, 타입) 불일치"
    assert "COMMENT" not in dist.group("cols"), f"{dist_name}: _dist에는 COMMENT 없음"
    assert "DEFAULT" not in dist.group("cols"), f"{dist_name}: _dist에는 DEFAULT 없음"
    for m in re.finditer(r"COMMENT '([^']*)'", local.group("cols")):
        assert ";" not in m.group(1), f"{local_name}: COMMENT 문자열에 ';' 금지 (e2e run_e2e.sh가 ';'로 문장을 분리)"

    constraint = "CONSTRAINT check_created_by CHECK created_by != ''"
    if kind == "mart":
        assert constraint in local.group("cols") and constraint in dist.group("cols")
        created_by_line = [l for l in local.group("cols").split("\n") if l.strip().startswith("created_by ")]
        assert created_by_line and "DEFAULT" not in created_by_line[0], "created_by는 DEFAULT 없음"
    else:
        assert "created_by" not in local.group("cols")

    for name, typ in local_cols:
        if name.endswith("_at"):
            assert typ == "DateTime('Asia/Seoul')", f"{table}.{name}: KST DateTime"
        assert not typ.startswith("Nullable(String"), f"{table}.{name}: 문자열은 NOT NULL('')"


@pytest.mark.parametrize("rel", [t for t in MANIFEST if "accounts" not in t and "seed_" not in t],
                         ids=lambda p: p.split("/")[-1])
def test_e2e_single_node_conversion_leaves_no_residual(rel):
    text = _read(rel)
    n_local = text.count("_local\nON CLUSTER")
    n_dist = text.count("_dist\nON CLUSTER")
    assert n_local == n_dist and n_local > 0
    converted = _E2E_REPL_RE.sub("ENGINE = MergeTree", text)
    converted = _E2E_DIST_RE.sub(r"ENGINE = Distributed('default', '\1', '\2', rand());", converted)
    code = _code_lines(converted)
    assert "ReplicatedMergeTree" not in code
    assert "Distributed('gpu-monitoring'" not in code
    assert converted.count("ENGINE = MergeTree") == n_local
    assert converted.count("Distributed('default'") == n_dist


# ---------------------------------------------------------------- GRANT (설계 §4.2 표)

def _rw(db, t):
    return {(frozenset({"SELECT", "INSERT"}), f"{db}.{t}_dist"),
            (frozenset({"SELECT", "INSERT"}), f"{db}.{t}_local"),
            (frozenset({"ALTER DELETE"}), f"{db}.{t}_local")}


def test_collectors_accounts_grants():
    found = _grants(_read(f"{COLLECTORS_DIR}/accounts.sql"), "collectors")
    expected = set()
    for t in ("raw_token_metrics_gpu_1d", "raw_token_metrics_serving_1d", "raw_token_metrics_summary_1d"):
        expected |= _rw("fact", t)
    expected |= {(frozenset({"SELECT", "INSERT"}), "fact.collect_audit_metrics_1d_dist"),
                 (frozenset({"SELECT", "INSERT"}), "fact.collect_audit_metrics_1d_local")}
    expected |= _rw("gpu_data", "dim_token_metrics_service")
    expected.add((frozenset({"SELECT"}), "gpu_data.dim_token_service_dist"))
    assert found == expected
    assert not any("collect_audit_metrics_1d" in tbl and "ALTER DELETE" in privs for privs, tbl in found)


def test_mart_accounts_grants():
    found = _grants(_read(f"{MART_DIR}/accounts.sql"), "canonical")
    expected = set()
    for t in ("agg_token_model_cost_1d", "token_metrics_check_1d", "agg_token_model_share_1d", "agg_token_gpu_group_1d"):
        expected.add((frozenset({"SELECT", "INSERT"}), f"mart.{t}_dist"))
        expected.add((frozenset({"ALTER DELETE"}), f"mart.{t}_local"))
    for t in ("dim_token_model_alias", "dim_token_gpu_tco", "dim_token_gpu_allocation", "dim_token_vendor_price",
              "dim_token_metrics_service", "dim_token_service"):
        expected.add((frozenset({"SELECT"}), f"gpu_data.{t}_dist"))
    for t in ("raw_token_metrics_gpu_1d", "raw_token_metrics_serving_1d", "raw_token_metrics_summary_1d"):
        expected.add((frozenset({"SELECT"}), f"fact.{t}_dist"))
    expected.add((frozenset({"SELECT"}), "mart.token_usage_1d_dist"))
    expected.add((frozenset({"SELECT"}), "mart.agg_token_service_1d_dist"))
    expected.add((frozenset({"CREATE TEMPORARY TABLE"}), "*.*"))
    expected.add((frozenset({"SELECT"}), "system.mutations"))
    assert found == expected


def test_assets_accounts_metrics_grants():
    found = _grants(_read(f"{ASSETS_DIR}/accounts_metrics.sql"), "canonical")
    expected = {(frozenset({"SELECT"}), f"gpu_data.{t}_dist") for t in (
        "dim_token_model_alias", "dim_token_gpu_tco", "dim_token_gpu_allocation", "dim_token_vendor_price")}
    assert found == expected


# ---------------------------------------------------------------- 시드 (dim_holiday 3요소 + 플레이스홀더 규칙)

SEEDS = {
    "seed_dim_token_model_alias.sql": ("dim_token_model_alias", "(alias, effective_from)",
                                       ["dup_key", "alias_maps_to_two_canonicals", "alias_loop", "empty_canonical",
                                        "missing_identity_row", "service_not_in_registry"]),
    "seed_dim_token_gpu_tco.sql": ("dim_token_gpu_tco", "(gpu_type, effective_from)",
                                   ["dup_key", "unknown_row_state", "basis_domain"]),
    "seed_dim_token_gpu_allocation.sql": ("dim_token_gpu_allocation", "(service_group, gpu_type, effective_from)",
                                          ["dup_key", "unknown_row_state"]),
    "seed_dim_token_vendor_price.sql": ("dim_token_vendor_price", "(provider, model, tier, effective_from)",
                                        ["dup_key", "unknown_row_state", "tier_domain"]),
}
ANCHOR = "-- 검증: 결과가 비어야 정상 ------------------------------------------------"


@pytest.mark.parametrize("fname", sorted(SEEDS), ids=lambda f: "seed_" + f[len("seed_dim_token_"):-4])
def test_seed_three_elements_and_placeholders(fname):
    table, key, checks = SEEDS[fname]
    text = _read(f"{ASSETS_DIR}/{fname}")
    assert f"INSERT INTO gpu_data.{table}_dist" in text
    assert f"WHERE {key} NOT IN (" in text, "NOT IN 멱등 가드 (키 튜플 형식 그대로)"
    assert "SETTINGS insert_distributed_sync = 1;" in text
    assert ANCHOR in text
    for c in checks:
        assert f"'{c}' AS check_name" in text or f"SELECT '{c}'," in text, f"검증 {c} 부재"
    assert "'unknown'" in text, "unknown 플레이스홀더 행 필수"
    code = _code_lines(text)
    assert "합성" not in code and "toNullable(" not in code, "사내 시드는 NULL 플레이스홀더만 (합성 수치 금지 — 코드 라인 기준)"
    if fname == "seed_dim_token_gpu_tco.sql":
        for g in ("'H100'", "'A100'", "'H200'", "'L40S'"):
            assert g in text
        assert "'KRW'" in text
    if fname == "seed_dim_token_vendor_price.sql":
        assert "'standard'" in text


@pytest.mark.parametrize("fname", sorted(SEEDS), ids=lambda f: "stage_fixture_" + f[len("seed_dim_token_"):-4])
def test_stage_fixture_exists_and_is_synthetic(fname):
    table, key, _ = SEEDS[fname]
    text = _read(f"{FIXTURES_DIR}/stage_{fname}")
    assert "합성" in text.split("\n")[1], "둘째 줄 헤더에 '합성' 표기 (사내 적용 금지 경고)"
    assert f"INSERT INTO gpu_data.{table}_dist" in text
    assert f"WHERE {key} NOT IN (" in text
    assert "SETTINGS insert_distributed_sync = 1;" in text
    assert ANCHOR in text
    for raw in _code_lines(text).split("\n"):
        if "toDate('2026-01-01')" in raw:
            assert "'unknown'" in raw, (
                "fixture 실값 행은 플레이스홀더 키 날짜(2026-01-01)를 재사용하지 않는다 — "
                "시드가 먼저 적용되면 NOT IN 가드가 실값 행을 무음 skip (Self-Review #4)")
```

- [ ] **Step 3: 실행 → 실패 확인**

```bash
cd /home/mini/github/token-data-pipeline/assets/model-catalog
python3 -m pytest -q tests/test_ddl_manifest.py 2>&1 | tail -n 5
```

기대: `45 failed in 0.3s`(hygiene 14 + 테이블 쌍 13 + e2e 변환 7 + GRANT 3 + 시드 4 + stage fixture 4), 실패 메시지는 `AssertionError: 매니페스트 파일 부재: collectors/token-metrics/ddl/company/raw_token_metrics.sql` 류. (수집 오류 0 — 테스트 모듈 자체는 import 돼야 한다: `python3 -c "import ast,sys; ast.parse(open('tests/test_ddl_manifest.py').read())"` 통과.)

- [ ] **Step 4: 커밋 (RED 상태 커밋 — T6까지 CI에 물리지 않음: `test-assets.yml`의 model-catalog job은 T7에서 추가)**

```bash
cd /home/mini/github/token-data-pipeline
git add assets/model-catalog/conftest.py assets/model-catalog/tests/__init__.py assets/model-catalog/tests/test_ddl_manifest.py
git commit -m "test(assets): §4.0 DDL 매니페스트 14파일 컨벤션 lint 테스트 (Plan 6a T2)

_local/_dist 쌍·ZK 경로·PARTITION/ORDER BY/TTL/샤딩키·컬럼 parity·created_by CHECK·e2e 변환 정규식·GRANT 집합(§4.2)·시드 3요소를 기계 검증. T3~T6에서 GREEN.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

### Task 3: collectors DDL — `collectors/token-metrics/ddl/company/{raw_token_metrics,dim_token_metrics_service,accounts}.sql` + `ddl/README.md`

**Files:**
- Create: `collectors/token-metrics/ddl/company/raw_token_metrics.sql` (fact 4테이블 = 8 CREATE)
- Create: `collectors/token-metrics/ddl/company/dim_token_metrics_service.sql`
- Create: `collectors/token-metrics/ddl/company/accounts.sql`
- Create: `collectors/token-metrics/ddl/README.md` (뮤테이션 장부 = 설계 §4.0 표 그대로)
- Test: `assets/model-catalog/tests/test_ddl_manifest.py -k "collectors or raw_token_metrics or dim_token_metrics_service"` (T2)

**Interfaces:**
- Consumes: 설계 §4.0 물리 표(fact 4 + 레지스트리), §4.1 컬럼, §4.2 GRANT 표 1행, §4.3 레지스트리 컬럼.
- Produces (6b가 소비): `fact.raw_token_metrics_gpu_1d_{local,dist}`, `fact.raw_token_metrics_serving_1d_{local,dist}`, `fact.raw_token_metrics_summary_1d_{local,dist}`, `fact.collect_audit_metrics_1d_{local,dist}`, `gpu_data.dim_token_metrics_service_{local,dist}`; 6b `install.sh`의 `apply_sql` 대상 = `ddl/<env>/raw_token_metrics.sql`, `ddl/<env>/dim_token_metrics_service.sql`(accounts.sql은 admin). INSERT는 `_dist` 경유, 존재확인 SELECT는 `_dist`, 멱등 DELETE는 `_local`(`ON CLUSTER`).

- [ ] **Step 1: `raw_token_metrics.sql` 작성**

```bash
mkdir -p /home/mini/github/token-data-pipeline/collectors/token-metrics/ddl/company
```

`collectors/token-metrics/ddl/company/raw_token_metrics.sql`:

```sql
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
```

- [ ] **Step 2: `dim_token_metrics_service.sql` 작성**

`collectors/token-metrics/ddl/company/dim_token_metrics_service.sql`:

```sql
-- =============================================================
-- Company/Stage ClickHouse DDL — gpu_data.dim_token_metrics_service (설계 2026-08-31 §4.3)
-- Target cluster: gpu-monitoring
-- Writer: mart (공유 계정 — collectors/token-metrics 정기 실행에서만 동기화; rerun·manual 모드는 읽기만)
-- 주의: gpu_data는 기존(동료 소유) DB — DB 생성문 없음. dim_token_* 접두사 규칙 적용.
-- 기존 gpu_data.dim_token_service(토큰 레지스트리)와 무접촉 — 메트릭 싱크 전용 레지스트리
--   (마스터 §5.9-6의 문서화된 예외: 토큰 coverage 게이트에 편입되지 않음).
-- 동기화 (§4.3): endpoints.yaml을 원하는 집합으로 만들고 현재 행과 diff(비교 키 = updated_at 제외 전 컬럼)
--   → 다를 때만 ALTER DELETE(전체) + INSERT (현재 집합이 비면 DELETE 생략 → 최초 배포 뮤테이션 0).
-- M0 기대 집합 = enabled=1 AND coverage_since <= d AND (until IS NULL OR d <= until).
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_metrics_service_local
ON CLUSTER 'gpu-monitoring'
(
    service_group            LowCardinality(String)  COMMENT '정본 = collectors/token-metrics/endpoints.yaml — 토큰 레지스트리와 바이트 동일 (M0 WARN)',
    service                  LowCardinality(String)  COMMENT '정본 = collectors/token-metrics/endpoints.yaml',
    base_url                 String                  COMMENT '메트릭 base (기본 = usage와 동일 호스트)',
    enabled                  UInt8                   COMMENT '0이면 모든 모드에서 SKIPPED reason=disabled',
    api_since                Date                    COMMENT '정기 API 수집 게이트: target_date < api_since면 호출 안 함 (기본 2026-09-09)',
    coverage_since           Date                    COMMENT 'M0 커버리지 기대 시작일 (기본 2026-08-26): 이후 앵커 없으면 metrics_missing',
    until                    Nullable(Date)          COMMENT '마지막 데이터 날짜 (게이트·커버리지 공통) — NULL = 진행 중',
    expect_gpu               UInt8 DEFAULT 1         COMMENT 'gpu:[]가 정상인 서비스는 0',
    expect_serving           UInt8 DEFAULT 1         COMMENT 'serving:[]가 정상인 서비스는 0',
    usage_includes_consumers UInt8 DEFAULT 0         COMMENT '플랫폼 제공자: 자기 /v1/usage가 소비자 호출분을 포함 보고하면 1 (§6.4 분모)',
    note                     String DEFAULT ''       COMMENT '운영 메모 (자유 텍스트)',
    updated_at               DateTime('Asia/Seoul')  COMMENT '동기화 시각 (diff 비교 키에서 제외)'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/gpu_data/dim_token_metrics_service_local',
    '{replica}'
)
ORDER BY (service)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_metrics_service_dist
ON CLUSTER 'gpu-monitoring'
(
    service_group            LowCardinality(String),
    service                  LowCardinality(String),
    base_url                 String,
    enabled                  UInt8,
    api_since                Date,
    coverage_since           Date,
    until                    Nullable(Date),
    expect_gpu               UInt8,
    expect_serving           UInt8,
    usage_includes_consumers UInt8,
    note                     String,
    updated_at               DateTime('Asia/Seoul')
)
ENGINE = Distributed('gpu-monitoring', 'gpu_data', 'dim_token_metrics_service_local', rand());
```

- [ ] **Step 3: `accounts.sql` 작성 (collectors 형식, admin 수동)**

`collectors/token-metrics/ddl/company/accounts.sql`:

```sql
-- =============================================================
-- collectors/token-metrics GRANT 추가분 (설계 2026-08-31 §4.2 GRANT 표 · 마스터 v1.12 §7.2 계정·GRANT 경계)
-- 실행 주체: admin 수동 (install.sh 자동 적용 대상 아님 — §7.2)
-- 계정: 공유 운영계정 mart (계정 생성·비밀번호는 동료 소유 — 이 파일은 사용자를 만들지 않는다)
-- 원칙: 자기 테이블에 테이블 레벨 GRANT만(DB 레벨 금지). 이미 있는 권한은 no-op.
--   fact/gpu_data DB는 기존 DB — 이 파일은 DB를 만들지 않는다 (6b install.sh 프리플라이트가 존재 확인).
-- 기존 collectors/token-usage/ddl/company/accounts.sql 무수정 — 신규 테이블 몫만 여기에.
-- =============================================================

-- 1) 수집기 몫 --------------------------------------------------
-- 수집 원본 3테이블: 존재 확인 SELECT(앵커) + INSERT(_dist 경유) + 멱등 DELETE(_local만, --replace·크래시 복구)
GRANT SELECT, INSERT ON fact.raw_token_metrics_gpu_1d_dist      TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON fact.raw_token_metrics_gpu_1d_local     TO mart ON CLUSTER 'gpu-monitoring';
GRANT ALTER DELETE   ON fact.raw_token_metrics_gpu_1d_local     TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON fact.raw_token_metrics_serving_1d_dist  TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON fact.raw_token_metrics_serving_1d_local TO mart ON CLUSTER 'gpu-monitoring';
GRANT ALTER DELETE   ON fact.raw_token_metrics_serving_1d_local TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON fact.raw_token_metrics_summary_1d_dist  TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON fact.raw_token_metrics_summary_1d_local TO mart ON CLUSTER 'gpu-monitoring';
GRANT ALTER DELETE   ON fact.raw_token_metrics_summary_1d_local TO mart ON CLUSTER 'gpu-monitoring';
-- 교체 감사(append-only): INSERT만 — DELETE 권한 없음 (감사 불변성)
GRANT SELECT, INSERT ON fact.collect_audit_metrics_1d_dist      TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON fact.collect_audit_metrics_1d_local     TO mart ON CLUSTER 'gpu-monitoring';
-- 메트릭 레지스트리: 정기 실행 diff 동기화 (ALTER DELETE 전체 + INSERT)
GRANT SELECT, INSERT ON gpu_data.dim_token_metrics_service_dist  TO mart ON CLUSTER 'gpu-monitoring';
GRANT SELECT, INSERT ON gpu_data.dim_token_metrics_service_local TO mart ON CLUSTER 'gpu-monitoring';
GRANT ALTER DELETE   ON gpu_data.dim_token_metrics_service_local TO mart ON CLUSTER 'gpu-monitoring';
-- 프리플라이트·M0 (토큰 레지스트리 읽기 전용 — 기존 권한이면 no-op)
GRANT SELECT ON gpu_data.dim_token_service_dist TO mart ON CLUSTER 'gpu-monitoring';
```

- [ ] **Step 4: `ddl/README.md` 작성 (뮤테이션 장부 = 설계 §4.0 표)**

`collectors/token-metrics/ddl/README.md`:

```markdown
# collectors/token-metrics DDL

> 상태: Plan 6a 초안 — 설계 `docs/superpowers/specs/2026-08-31-token-metrics-ingest-design.md` §4.0–§4.3.
> 기존 `collectors/token-usage/ddl/**` 무수정 — 이 디렉터리의 파일만 신규 모듈이 적용한다.

## 파일

| 파일 | 내용 | 적용 주체 |
|---|---|---|
| `company/raw_token_metrics.sql` | fact 4테이블 — `raw_token_metrics_gpu_1d`, `raw_token_metrics_serving_1d`, `raw_token_metrics_summary_1d`(앵커), `collect_audit_metrics_1d`(append-only) | `install.sh company` |
| `company/dim_token_metrics_service.sql` | 메트릭 레지스트리 `gpu_data.dim_token_metrics_service` (기존 `dim_token_service` 무접촉) | `install.sh company` |
| `company/accounts.sql` | 공유 계정 `mart` GRANT (설계 §4.2 표) | admin 수동 |
| `stage/*.sql` | `tools/gen_stage_ddl.py` 생성물 (ON CLUSTER 제거·MergeTree) — 직접 수정 금지 | `install.sh stage` |
| `company-verify/*.sql` | `tools/gen_verify_ddl.py` 생성물 (`token_verify_*` DB·계정) — 직접 수정 금지 | `install.sh company-verify` |

## 뮤테이션 장부 (설계 §4.0 — 동일 표)

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

## 확정된 결정

- 앵커 = `raw_token_metrics_summary_1d` (date, service) 1행. NODATA(rows==0)도 앵커 기록. 적재 순서: 앵커 DELETE 첫 번째 → gpu/serving DELETE·INSERT → 앵커 INSERT 마지막.
- `collect_audit_metrics_1d`는 append-only — GRANT에 ALTER DELETE 없음.
- 레지스트리는 정기 실행에서만 동기화(비교 키 = `updated_at` 제외 전 컬럼). rerun·manual 모드는 읽기만.
- `flags Array(String)`: 빈 배열이 정상. FAIL 플래그(`hours_over_count`, `unknown_violation`, `pct_non_monotone`) 행도 fact에는 남기고 mart가 제외한다(M1 `fail_flag`).
- 문자열은 NOT NULL(''), 숫자 부재는 Nullable(p50~p99), DateTime은 전부 `Asia/Seoul`.

## 환경 방침

- company: `fact`·`gpu_data`는 기존 DB — 이 디렉터리는 DB를 만들지 않는다. install.sh 프리플라이트가 두 DB 존재와 `gpu_data.dim_token_service_dist` SELECT 가능을 확인한다.
- stage: `stage/*.sql`은 생성물. 시드 합성값은 `assets/model-catalog/fixtures/stage_seed_*.sql`을 stage 런북 절차로 수동 적용(기존 `docs/operations/stage-runbook.md` 무수정 — 절차는 `docs/operations/token-metrics-deploy.md`(6c)에).
- company-verify(선택): `company-verify/*.sql`은 `token_verify_fact`/`token_verify_dim`/`token_verify_mart` + 계정 `token_verify` 대상 생성물. 신규 모듈은 기존 테이블에 쓰지 않으므로 운영 DB 직접 설치가 권장 경로(설계 §7.5).

## 적용 순서 (설계 §7.5 — DDL/GRANT는 신규 파일만)

1. admin: `company/accounts.sql`(GRANT) — 테이블 생성 전이어도 GRANT는 이름 기반이라 선적용 가능.
2. `./install.sh company --context … --registry … --tag <sha7>` → `apply_sql` = `raw_token_metrics.sql`, `dim_token_metrics_service.sql`(IF NOT EXISTS, 재실행 안전).
3. `mart/token-metrics/ddl/company/accounts.sql`(admin) → `mart/token-metrics` install.sh(6c).
4. `assets/model-catalog/ddl/company/` dim 4·시드 4·`accounts_metrics.sql`(admin) — mart-metrics 첫 실행 전.

## 이 초안에 없는 것

- `view_token_*` 4종·`dim_token_model_meta`·`dim_token_service_meta`·`dim_token_gpu_unit_map`·`dim_token_model_consumes`(P1 — 생성기 목록에 넣지 않음).
- `tools/data-admin/delete_data.py` 타깃 등록(P1).
```

- [ ] **Step 5: lint 실행 → collectors 항목 GREEN**

```bash
cd /home/mini/github/token-data-pipeline/assets/model-catalog
python3 -m pytest -q tests/test_ddl_manifest.py -k "collectors or raw_token_metrics or dim_token_metrics_service" 2>&1 | tail -n 3
python3 -m pytest -q tests/test_ddl_manifest.py 2>&1 | tail -n 1
```

기대: 첫 명령 `11 passed, 34 deselected`; 둘째 명령 `34 failed, 11 passed`(mart·assets 미작성분만 실패).

- [ ] **Step 6: 커밋**

```bash
cd /home/mini/github/token-data-pipeline
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org
# 기대: 출력 없음
# README 검증(비코드 산출물 — lint MANIFEST 밖): 테이블 5종 전부 언급 + 공개 레포 위생(test_ddl_manifest.py hygiene와 동일 패턴)
for t in raw_token_metrics_gpu_1d raw_token_metrics_serving_1d raw_token_metrics_summary_1d collect_audit_metrics_1d dim_token_metrics_service; do
  grep -q "$t" collectors/token-metrics/ddl/README.md && echo "ok $t" || { echo "MISSING $t"; exit 1; }
done
! grep -nE 'harbor\.|@|svc\.cluster' collectors/token-metrics/ddl/README.md && echo "README hygiene OK"
# 기대: ok 5줄 + README hygiene OK
git add collectors/token-metrics/ddl
git commit -m "feat(ddl): collectors/token-metrics fact 4테이블·메트릭 레지스트리·GRANT 초안 (Plan 6a T3)

설계 §4.1 fact(raw_token_metrics_gpu/serving/summary_1d, collect_audit_metrics_1d append-only), §4.3 gpu_data.dim_token_metrics_service(api_since/coverage_since/until 게이트), §4.2 GRANT(collectors 형식, TO mart). README에 §4.0 뮤테이션 장부 동일 표.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

### Task 4: mart DDL — `mart/token-metrics/ddl/company/{mart_metrics_tables,accounts}.sql` + `ddl/README.md`

**Files:**
- Create: `mart/token-metrics/ddl/company/mart_metrics_tables.sql` (M1·M3·M4·M2 = 4테이블 8 CREATE)
- Create: `mart/token-metrics/ddl/company/accounts.sql` (정규형 GRANT)
- Create: `mart/token-metrics/ddl/README.md`
- Test: `assets/model-catalog/tests/test_ddl_manifest.py -k "mart"` (T2)

**Interfaces:**
- Consumes: 설계 §6.1 M1/M3/M4/M2 컬럼 목록, §4.2 GRANT 표 2행, §6.2 읽기 계약.
- Produces (6c가 소비): `mart.agg_token_model_cost_1d_{local,dist}`, `mart.token_metrics_check_1d_{local,dist}`, `mart.agg_token_model_share_1d_{local,dist}`, `mart.agg_token_gpu_group_1d_{local,dist}`. `created_by`는 DEFAULT 없음 — 6c `steps.py`가 `'token-metrics-pipeline'`을 명시 INSERT. 6c `install.sh`의 `apply_sql` 대상 = `ddl/<env>/mart_metrics_tables.sql`. Grafana(6c)는 `_dist`만 읽는다.
- 타입 규약(설계 해석 — Self-Review 노트 참조): 시간·비용 = Float64(설계가 Nullable이라 명시한 것만 Nullable), 토큰 카운트 = UInt64, 플래그 = UInt8, 분류 문자열 = LowCardinality(String), 비율 = Nullable(Float64).

- [ ] **Step 1: `mart_metrics_tables.sql` 작성**

```bash
mkdir -p /home/mini/github/token-data-pipeline/mart/token-metrics/ddl/company
```

`mart/token-metrics/ddl/company/mart_metrics_tables.sql`:

```sql
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
    model_cost_sum_krw       Nullable(Float64)       COMMENT 'Σ 그룹 호스팅 모델 C (M1 합)',
    test_cost_krw            Nullable(Float64)       COMMENT '= Σ test × TCO — 그룹 귀속, 배분 안 함',
    idle_gpu_hours           Nullable(Float64)       COMMENT '= allocated − reported_total (음수면 0)',
    idle_cost_krw            Nullable(Float64)       COMMENT '= idle_gpu_hours × TCO',
    unattributed_cost_krw    Nullable(Float64)       COMMENT '= flagged_gpu_hours × TCO',
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
```

- [ ] **Step 2: `accounts.sql` 작성 (정규형, admin 수동)**

`mart/token-metrics/ddl/company/accounts.sql`:

```sql
-- =============================================================
-- mart/token-metrics GRANT 추가분 (설계 2026-08-31 §4.2 GRANT 표 · 마스터 v1.12 §7.2 계정·GRANT 경계)
-- 실행 주체: admin 수동 (install.sh 자동 적용 대상 아님 — §7.2)
-- 계정: 공유 운영계정 mart (계정 생성·비밀번호는 동료 소유 — 이 파일은 사용자를 만들지 않는다)
-- 원칙: 자기 테이블에 테이블 레벨 GRANT만(DB 레벨 금지); INSERT는 _dist 경유만, _local에는 멱등 DELETE용 ALTER DELETE만.
--   정규형 `GRANT ON CLUSTER ... <priv> ON <table> TO <user>`. 이미 있는 권한은 no-op.
-- 기존 mart/token-usage/ddl/company/accounts.sql 무수정 — 신규 테이블 몫 + 읽기 계약 몫만 여기에.
-- =============================================================

-- 1) mart-metrics 자기 테이블 (M1·M3·M4·M2) ----------------------------------

GRANT ON CLUSTER 'gpu-monitoring' SELECT, INSERT ON mart.agg_token_model_cost_1d_dist   TO mart;
GRANT ON CLUSTER 'gpu-monitoring' ALTER DELETE   ON mart.agg_token_model_cost_1d_local  TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT, INSERT ON mart.token_metrics_check_1d_dist    TO mart;
GRANT ON CLUSTER 'gpu-monitoring' ALTER DELETE   ON mart.token_metrics_check_1d_local   TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT, INSERT ON mart.agg_token_model_share_1d_dist  TO mart;
GRANT ON CLUSTER 'gpu-monitoring' ALTER DELETE   ON mart.agg_token_model_share_1d_local TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT, INSERT ON mart.agg_token_gpu_group_1d_dist    TO mart;
GRANT ON CLUSTER 'gpu-monitoring' ALTER DELETE   ON mart.agg_token_gpu_group_1d_local   TO mart;

-- 2) 기준정보 읽기 (Layer C 3 + alias + 메트릭 레지스트리 + 토큰 레지스트리) ------
--    dim 4종 SELECT는 assets/model-catalog/ddl/company/accounts_metrics.sql과 중복 — 이미 있으면 no-op

GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.dim_token_model_alias_dist     TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.dim_token_gpu_tco_dist         TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.dim_token_gpu_allocation_dist  TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.dim_token_vendor_price_dist    TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.dim_token_metrics_service_dist TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.dim_token_service_dist         TO mart;

-- 3) 소스 읽기 — 메트릭 fact 3 + 토큰 mart 읽기 계약 2 (§6.1: 3테이블/13컬럼) -----

GRANT ON CLUSTER 'gpu-monitoring' SELECT ON fact.raw_token_metrics_gpu_1d_dist     TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON fact.raw_token_metrics_serving_1d_dist TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON fact.raw_token_metrics_summary_1d_dist TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON mart.token_usage_1d_dist               TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON mart.agg_token_service_1d_dist         TO mart;

-- 4) 배치 실행 권한 — GLOBAL JOIN 임시 테이블·뮤테이션 폴링 (기존 collectors/token-usage/ddl/company/accounts.sql 49-50행과 동일 권한 — no-op if present)

GRANT ON CLUSTER 'gpu-monitoring' CREATE TEMPORARY TABLE ON *.* TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON system.mutations TO mart;
```

- [ ] **Step 3: `ddl/README.md` 작성**

`mart/token-metrics/ddl/README.md`:

```markdown
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
```

- [ ] **Step 4: lint 실행 → mart 항목 GREEN**

```bash
cd /home/mini/github/token-data-pipeline/assets/model-catalog
python3 -m pytest -q tests/test_ddl_manifest.py -k "mart" 2>&1 | tail -n 3
python3 -m pytest -q tests/test_ddl_manifest.py 2>&1 | tail -n 1
```

기대: 첫 명령 `8 passed, 37 deselected`(hygiene 2 + 쌍 4 + e2e 1 + GRANT 1); 둘째 명령 `26 failed, 19 passed`.

- [ ] **Step 5: 커밋**

```bash
cd /home/mini/github/token-data-pipeline
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org
# 기대: 출력 없음
# README 검증(비코드 산출물 — lint MANIFEST 밖): mart 4테이블 전부 언급 + 공개 레포 위생
for t in agg_token_model_cost_1d token_metrics_check_1d agg_token_model_share_1d agg_token_gpu_group_1d; do
  grep -q "$t" mart/token-metrics/ddl/README.md && echo "ok $t" || { echo "MISSING $t"; exit 1; }
done
! grep -nE 'harbor\.|@|svc\.cluster' mart/token-metrics/ddl/README.md && echo "README hygiene OK"
# 기대: ok 4줄 + README hygiene OK
git add mart/token-metrics/ddl
git commit -m "feat(ddl): mart/token-metrics M1·M3·M4·M2 4테이블·GRANT 초안 (Plan 6a T4)

설계 §6.1 컬럼 목록 그대로(agg_token_model_cost_1d 28·token_metrics_check_1d 12·agg_token_model_share_1d 14·agg_token_gpu_group_1d 23), created_by DEFAULT 없음 + CHECK, §4.2 GRANT(정규형: 자기 4테이블 + dim 6·fact 3·토큰 mart 2 SELECT + CREATE TEMPORARY TABLE·system.mutations).

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

### Task 5: assets dim DDL 4 + `accounts_metrics.sql` — `assets/model-catalog/ddl/company/`

**Files:**
- Create: `assets/model-catalog/ddl/company/dim_token_model_alias.sql`
- Create: `assets/model-catalog/ddl/company/dim_token_gpu_tco.sql`
- Create: `assets/model-catalog/ddl/company/dim_token_gpu_allocation.sql`
- Create: `assets/model-catalog/ddl/company/dim_token_vendor_price.sql`
- Create: `assets/model-catalog/ddl/company/accounts_metrics.sql`
- Test: `assets/model-catalog/tests/test_ddl_manifest.py -k "assets_dim or accounts_metrics"` (T2)
- 무수정: `assets/model-catalog/ddl/company/{dim_token_model,seed_dim_token_model,accounts}.sql`, `assets/model-catalog/README.md`

**Interfaces:**
- Consumes: 설계 §4.2 컬럼 표(4 dim) + effective_from 규약(소급 시작일, KRW 고정).
- Produces (6c M1~M4·T8/T9 생성기가 소비): `gpu_data.dim_token_model_alias_{local,dist}`(alias, effective_from 키), `gpu_data.dim_token_gpu_tco_{local,dist}`(gpu_type, effective_from), `gpu_data.dim_token_gpu_allocation_{local,dist}`(service_group, gpu_type, effective_from), `gpu_data.dim_token_vendor_price_{local,dist}`(provider, model, tier, effective_from). 이력 규약: date 유효 행 = `effective_from <= d`인 최신 행(`argMax`/`ORDER BY effective_from DESC LIMIT 1 BY 키` — 6c 공통 CTE `eff_alias`/`eff_tco`/`eff_alloc`).

- [ ] **Step 1: `dim_token_model_alias.sql`**

`assets/model-catalog/ddl/company/dim_token_model_alias.sql`:

```sql
-- =============================================================
-- Company/Stage ClickHouse DDL — gpu_data.dim_token_model_alias (설계 2026-08-31 §4.2, P0)
-- Target cluster: gpu-monitoring
-- Writer: admin 수동 (seed_dim_token_model_alias.sql + sheet_to_dim_token_model_alias_insert.py 생성 SQL) /
--         Reader: mart (공유 계정 — mart-metrics M1 canon(x) = if(a.canonical = '', x, a.canonical))
-- 주의: gpu_data는 기존(동료 소유) DB — DB 생성문 없음. dim_token_* 접두사 규칙 적용.
-- 이력 규약 (§4.2): (alias, effective_from) — 재매핑은 새 effective_from 행 append (기존 행 불변).
--   identity 행(canonical→canonical)·unknown→unknown 필수. alias 없는 canonical-only 행도 identity 생성.
-- model_registered 판정: canonical이 dim_token_model_alias에 identity 행(생성기 자동 생성)으로 존재하면 1 — gpu_data.dim_token_model 조회 없음(6c 읽기 계약 3테이블 고정).
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_model_alias_local
ON CLUSTER 'gpu-monitoring'
(
    alias            String                                    COMMENT 'API/수기 원문 모델 문자열 (≤128) — identity 행은 canonical과 동일',
    effective_from   Date                                      COMMENT '매핑 적용 시작일 (이력 키)',
    canonical        String                                    COMMENT '정규화 대상 모델명 — 빈 문자열 금지(empty_canonical)',
    defining_service LowCardinality(String) DEFAULT ''         COMMENT '이 alias를 정의한(보고한) 서비스 — 레지스트리 service와 일치해야 함(service_not_in_registry)',
    source           LowCardinality(String) DEFAULT 'metadata-sheet' COMMENT 'metadata-sheet | manual | seed',
    note             String DEFAULT ''                         COMMENT '시트 비고 원문 (자유 텍스트)'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/gpu_data/dim_token_model_alias_local',
    '{replica}'
)
ORDER BY (alias, effective_from)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_model_alias_dist
ON CLUSTER 'gpu-monitoring'
(
    alias            String,
    effective_from   Date,
    canonical        String,
    defining_service LowCardinality(String),
    source           LowCardinality(String),
    note             String
)
ENGINE = Distributed('gpu-monitoring', 'gpu_data', 'dim_token_model_alias_local', cityHash64(alias));
```

- [ ] **Step 2: `dim_token_gpu_tco.sql`**

`assets/model-catalog/ddl/company/dim_token_gpu_tco.sql`:

```sql
-- =============================================================
-- Company/Stage ClickHouse DDL — gpu_data.dim_token_gpu_tco (설계 2026-08-31 §4.2, P0 — Layer C)
-- Target cluster: gpu-monitoring
-- Writer: admin 수동 (seed_dim_token_gpu_tco.sql 플레이스홀더 + csv_to_layer_c_dim_insert.py --table gpu_tco 생성 SQL) /
--         Reader: mart (공유 계정 — mart-metrics M1 model_cost_krw = Σ gpu_hours × tco_krw_per_gpu_hour)
-- 주의: gpu_data는 기존(동료 소유) DB — DB 생성문 없음.
-- 이력 규약 (§4.2): (gpu_type, effective_from) — 단가 변경은 새 effective_from 행 append. 통화 KRW 고정.
--   TCO는 Nullable — 플레이스홀더·미등록 기종은 NULL → 비용 NULL 전파(부분 합 금지, 0원 위장 금지).
-- gpu_type은 fact.raw_token_metrics_gpu_1d.gpu_type과 정확 일치(대소문자 포함) — TCO표 키.
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_gpu_tco_local
ON CLUSTER 'gpu-monitoring'
(
    gpu_type             String                            COMMENT 'TCO표 키 — fact gpu_type과 정확 일치 (≤64)',
    effective_from       Date                              COMMENT '단가 적용 시작일 (이력 키)',
    tco_krw_per_gpu_hour Nullable(Float64)                 COMMENT 'KRW per GPU-hour — 플레이스홀더는 NULL',
    currency             LowCardinality(String) DEFAULT 'KRW' COMMENT 'KRW 고정',
    basis                LowCardinality(String) DEFAULT '' COMMENT 'depreciation | lease | power-inclusive | tco | 빈 문자열(플레이스홀더) — basis_domain 검증',
    note                 String DEFAULT ''                 COMMENT '출처·기준월 (예: TCO팀 2026-08 확정)'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/gpu_data/dim_token_gpu_tco_local',
    '{replica}'
)
ORDER BY (gpu_type, effective_from)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_gpu_tco_dist
ON CLUSTER 'gpu-monitoring'
(
    gpu_type             String,
    effective_from       Date,
    tco_krw_per_gpu_hour Nullable(Float64),
    currency             LowCardinality(String),
    basis                LowCardinality(String),
    note                 String
)
ENGINE = Distributed('gpu-monitoring', 'gpu_data', 'dim_token_gpu_tco_local', cityHash64(gpu_type));
```

- [ ] **Step 3: `dim_token_gpu_allocation.sql`**

`assets/model-catalog/ddl/company/dim_token_gpu_allocation.sql`:

```sql
-- =============================================================
-- Company/Stage ClickHouse DDL — gpu_data.dim_token_gpu_allocation (설계 2026-08-31 §4.2, P0-stretch — Layer C)
-- Target cluster: gpu-monitoring
-- Writer: admin 수동 (seed_dim_token_gpu_allocation.sql 플레이스홀더 + csv_to_layer_c_dim_insert.py --table gpu_allocation) /
--         Reader: mart (공유 계정 — mart-metrics M2 allocated_gpu_hours = allocated_gpu_count × 24)
-- 주의: gpu_data는 기존(동료 소유) DB — DB 생성문 없음.
-- 할당표는 serviceGroup 단위 (정의서 2.3 — 쿼터 보유 단위). 단위는 장수(count).
-- 이력 규약 (§4.2): (service_group, gpu_type, effective_from) — 변경은 새 effective_from 행 append; 철회는 0 행 append.
-- 플레이스홀더는 gpu_type='unknown' (M2 무시 — HAVING gpu_type != 'unknown', no_allocation으로만 노출).
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_gpu_allocation_local
ON CLUSTER 'gpu-monitoring'
(
    service_group       LowCardinality(String)                COMMENT '정본 = endpoints.yaml serviceGroup (바이트 동일)',
    gpu_type            String                                COMMENT 'TCO표 키 — 플레이스홀더 unknown',
    effective_from      Date                                  COMMENT '할당 적용 시작일 00:00 KST (이력 키)',
    allocated_gpu_count Nullable(Float64)                     COMMENT '할당 장수(분수 허용) — 플레이스홀더는 NULL, 철회는 0',
    source              LowCardinality(String) DEFAULT 'manual' COMMENT 'manual | quota-sheet | seed',
    note                String DEFAULT ''                     COMMENT '출처·기준 (자유 텍스트)'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/gpu_data/dim_token_gpu_allocation_local',
    '{replica}'
)
ORDER BY (service_group, gpu_type, effective_from)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_gpu_allocation_dist
ON CLUSTER 'gpu-monitoring'
(
    service_group       LowCardinality(String),
    gpu_type            String,
    effective_from      Date,
    allocated_gpu_count Nullable(Float64),
    source              LowCardinality(String),
    note                String
)
ENGINE = Distributed('gpu-monitoring', 'gpu_data', 'dim_token_gpu_allocation_local', cityHash64(service_group));
```

- [ ] **Step 4: `dim_token_vendor_price.sql`**

`assets/model-catalog/ddl/company/dim_token_vendor_price.sql`:

```sql
-- =============================================================
-- Company/Stage ClickHouse DDL — gpu_data.dim_token_vendor_price (설계 2026-08-31 §4.2, P0-stretch — 사외 API 비용, 정의서 3.9)
-- Target cluster: gpu-monitoring
-- Writer: admin 수동 (seed_dim_token_vendor_price.sql 플레이스홀더 + csv_to_layer_c_dim_insert.py --table vendor_price) /
--         Reader: mart (공유 계정 — mart-metrics M4 external_api: allocated_cost_krw = Σ tokens × krw_per_mtok / 1e6)
-- 주의: gpu_data는 기존(동료 소유) DB — DB 생성문 없음.
-- 단가 의미 (§4.2): input = 캐시 생성·읽기를 제외한 순수 입력 단가, cache_creation = 벤더 공표 전체 write 단가
--   (입력 대비 할증분 아님; TTL별 차이는 note + 최고 단가 — 미결 M21). tier는 'standard' 기본(미결 M18).
-- 이력 규약: (provider, model, tier, effective_from) — 변경은 새 effective_from 행 append. KRW 고정.
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_vendor_price_local
ON CLUSTER 'gpu-monitoring'
(
    provider                    LowCardinality(String)                  COMMENT '벤더 (anthropic 등) — M4 provider_service 표기',
    model                       String                                  COMMENT 'canonical 모델명',
    tier                        LowCardinality(String) DEFAULT 'standard' COMMENT 'standard | batch | flex | priority (tier_domain)',
    effective_from              Date                                    COMMENT '단가 적용 시작일 (이력 키)',
    krw_per_mtok_input          Nullable(Float64)                       COMMENT '순수 입력 단가 (KRW per MTok)',
    krw_per_mtok_cached         Nullable(Float64)                       COMMENT '캐시 읽기 단가',
    krw_per_mtok_cache_creation Nullable(Float64)                       COMMENT '캐시 생성(write) 전체 단가',
    krw_per_mtok_output         Nullable(Float64)                       COMMENT '출력 단가',
    note                        String DEFAULT ''                       COMMENT '출처·환율 기준일 (예: 공표가 × 환율 2026-08)'
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/gpu_data/dim_token_vendor_price_local',
    '{replica}'
)
ORDER BY (provider, model, tier, effective_from)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS gpu_data.dim_token_vendor_price_dist
ON CLUSTER 'gpu-monitoring'
(
    provider                    LowCardinality(String),
    model                       String,
    tier                        LowCardinality(String),
    effective_from              Date,
    krw_per_mtok_input          Nullable(Float64),
    krw_per_mtok_cached         Nullable(Float64),
    krw_per_mtok_cache_creation Nullable(Float64),
    krw_per_mtok_output         Nullable(Float64),
    note                        String
)
ENGINE = Distributed('gpu-monitoring', 'gpu_data', 'dim_token_vendor_price_local', cityHash64(model));
```

- [ ] **Step 5: `accounts_metrics.sql` (dim 4종 SELECT 사본 — assets 단독 적용용)**

`assets/model-catalog/ddl/company/accounts_metrics.sql`:

```sql
-- =============================================================
-- model-catalog 메트릭 기준정보 GRANT 추가분 (설계 2026-08-31 §4.2 GRANT 표 3행)
-- 실행 주체: admin 수동. 계정은 공유 운영계정 mart — 이 파일은 사용자를 만들지 않는다.
-- 쓰기 주체: 시드·생성 SQL은 admin 수동 (mart는 SELECT만).
-- 기존 accounts.sql(dim_token_model SELECT) 무수정 — 파일명이 accounts.sql이 아니므로
--   gen_verify_ddl.py의 격리 DB/계정 프리펜드는 붙지 않는다(격리 검증에서는 collectors accounts 미러가 먼저 실행돼 DB·계정이 존재).
-- mart/token-metrics/ddl/company/accounts.sql의 dim 4종 SELECT와 중복 — 어느 쪽을 먼저 적용해도 no-op.
-- =============================================================

-- mart(공유 계정) — mart-metrics M1~M4 조인 읽기 (_dist만 — _local 우회 차단)
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.dim_token_model_alias_dist    TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.dim_token_gpu_tco_dist        TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.dim_token_gpu_allocation_dist TO mart;
GRANT ON CLUSTER 'gpu-monitoring' SELECT ON gpu_data.dim_token_vendor_price_dist   TO mart;
```

- [ ] **Step 6: lint 실행 → assets dim·accounts_metrics GREEN**

```bash
cd /home/mini/github/token-data-pipeline/assets/model-catalog
python3 -m pytest -q tests/test_ddl_manifest.py -k "assets_dim or accounts_metrics" 2>&1 | tail -n 3
python3 -m pytest -q tests/test_ddl_manifest.py 2>&1 | tail -n 1
```

기대: 첫 명령 `6 passed, 39 deselected`(쌍 4 + hygiene accounts_metrics 1 + GRANT 1); 둘째 명령 `12 failed, 33 passed` — 남은 실패 id는 전부 `seed_`·`stage_fixture_`·`seed_dim_token_*.sql@assets`(T6).

- [ ] **Step 7: 커밋**

```bash
cd /home/mini/github/token-data-pipeline
git status --short assets/model-catalog/ddl/company
# 기대: ?? 5개 (dim 4 + accounts_metrics.sql) — 기존 3파일은 변경 없음
git add assets/model-catalog/ddl/company/dim_token_model_alias.sql assets/model-catalog/ddl/company/dim_token_gpu_tco.sql assets/model-catalog/ddl/company/dim_token_gpu_allocation.sql assets/model-catalog/ddl/company/dim_token_vendor_price.sql assets/model-catalog/ddl/company/accounts_metrics.sql
git commit -m "feat(assets): gpu_data 메트릭 기준정보 dim 4종(alias·gpu_tco·gpu_allocation·vendor_price) DDL + SELECT GRANT 사본 (Plan 6a T5)

설계 §4.2 컬럼·이력 키 그대로(effective_from append-only, KRW 고정, TCO·단가 Nullable). 기존 dim_token_model·accounts.sql 무수정.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

### Task 6: 시드 4(사내 플레이스홀더만) + stage 합성 fixture 4 — lint GREEN

**Files:**
- Create: `assets/model-catalog/ddl/company/seed_dim_token_model_alias.sql`
- Create: `assets/model-catalog/ddl/company/seed_dim_token_gpu_tco.sql`
- Create: `assets/model-catalog/ddl/company/seed_dim_token_gpu_allocation.sql`
- Create: `assets/model-catalog/ddl/company/seed_dim_token_vendor_price.sql`
- Create: `assets/model-catalog/fixtures/stage_seed_dim_token_model_alias.sql`
- Create: `assets/model-catalog/fixtures/stage_seed_dim_token_gpu_tco.sql`
- Create: `assets/model-catalog/fixtures/stage_seed_dim_token_gpu_allocation.sql`
- Create: `assets/model-catalog/fixtures/stage_seed_dim_token_vendor_price.sql`
- Test: `assets/model-catalog/tests/test_ddl_manifest.py` 전체 (T2) → 45 passed

**Interfaces:**
- Consumes: 설계 §4.0 규칙(사내 시드 = 플레이스홀더만, stage 합성값은 생성기 밖 fixture), §4.2 시드 규칙 열, 기존 `seed_dim_token_model.sql`의 dim_holiday 3요소 패턴.
- Produces: admin이 적용하는 시드 4(플레이스홀더 키 = `effective_from 2026-01-01`), stage 런북(6c `token-metrics-deploy.md`)이 시드 **이후** `clickhouse-client --multiquery < fixtures/stage_seed_*.sql`로 적용하는 합성 시드 4 — fixture의 실값 행은 `effective_from 2026-08-01`(시드 키 `2026-01-01`과 비충돌; `unknown` 행만 2026-01-01). 플레이스홀더 키 날짜 `2026-01-01`은 T9 생성기의 `placeholder_effective_from` 검증 상수와 동일.
- 검증 SELECT 출력 계약(공통 4열): `check_name String, key String, effective_from Date, cnt UInt64` — 결과가 비어야 정상. 같은 `_dist`를 서브쿼리로 다시 읽는 검증은 `GLOBAL IN`/`GLOBAL NOT IN`(각 샤드 전역 조회 — 설계 §4.0).

- [ ] **Step 1: `seed_dim_token_model_alias.sql`**

`assets/model-catalog/ddl/company/seed_dim_token_model_alias.sql`:

```sql
-- =============================================================
-- gpu_data.dim_token_model_alias 시드 (설계 2026-08-31 §4.2 — dim_holiday 3요소 패턴)
-- (a) 출처·기준일: 플레이스홀더만 — unknown→unknown identity 행(2026-01-01). 실제 매핑은
--     메타데이터 시트 '모델' CSV → assets/model-catalog/sheet_to_dim_token_model_alias_insert.py
--     생성 SQL(gitignore: dim_token_model_alias_insert*.sql)을 admin이 적용.
-- (b) NOT IN 멱등 가드 — 재실행 안전.
-- (c) 말미 검증 SELECT — 결과가 비어야 정상.
-- 실행 주체: admin 수동. mart는 SELECT만.
-- 재매핑 절차: 기존 행 수정 금지 — 새 effective_from 행 append 후 해당 기간 mart-metrics rerun.
-- 검증 6종(설계 §4.2): dup_key, alias_maps_to_two_canonicals, alias_loop, empty_canonical, missing_identity_row,
--   service_not_in_registry — 마지막은 gpu_data.dim_token_metrics_service_dist 대조. 레지스트리 테이블은
--   collectors/token-metrics/ddl/company/dim_token_metrics_service.sql(README 적용 순서 2단계)이 만들고 이 시드는
--   4단계에 적용되므로 항상 먼저 존재한다.
-- =============================================================

INSERT INTO gpu_data.dim_token_model_alias_dist
    (alias, effective_from, canonical, defining_service, source, note)
SELECT *
FROM (
    -- 계약 표준값 unknown: identity — 정규화 불가 모델이 canon()에서 unknown으로 유지되게
    SELECT 'unknown' AS alias, toDate('2026-01-01') AS effective_from, 'unknown' AS canonical,
           '' AS defining_service, 'seed' AS source, '계약 표준 값 — identity (정규화 불가 모델)' AS note
)
WHERE (alias, effective_from) NOT IN (
    SELECT alias, effective_from FROM gpu_data.dim_token_model_alias_dist
)
-- 동기 분산 삽입 — 같은 파일의 멱등 가드·말미 검증이 미플러시 행을 놓치지 않게
SETTINGS insert_distributed_sync = 1;

-- 검증: 결과가 비어야 정상 ------------------------------------------------
-- 1) (alias, effective_from) 키 중복 없음
SELECT 'dup_key' AS check_name, alias AS key, effective_from, count() AS cnt
FROM gpu_data.dim_token_model_alias_dist
GROUP BY alias, effective_from
HAVING count() > 1

UNION ALL

-- 2) 같은 (alias, effective_from)이 서로 다른 canonical로 — 모순 매핑
SELECT 'alias_maps_to_two_canonicals', alias, effective_from, uniqExact(canonical)
FROM gpu_data.dim_token_model_alias_dist
GROUP BY alias, effective_from
HAVING uniqExact(canonical) > 1

UNION ALL

-- 3) alias_loop: 비-identity 행의 canonical이 다시 다른 비-identity 행의 alias — 체인·순환 금지(1단계 매핑만)
SELECT 'alias_loop', alias, effective_from, toUInt64(1)
FROM gpu_data.dim_token_model_alias_dist
WHERE alias != canonical
  AND canonical GLOBAL IN (
      SELECT alias FROM gpu_data.dim_token_model_alias_dist WHERE alias != canonical
  )

UNION ALL

-- 4) empty_canonical: canonical 빈 문자열 금지
SELECT 'empty_canonical', alias, effective_from, toUInt64(1)
FROM gpu_data.dim_token_model_alias_dist
WHERE canonical = ''

UNION ALL

-- 5) missing_identity_row: 모든 canonical은 identity 행(alias = canonical)을 가져야 함
SELECT 'missing_identity_row', canonical, min(effective_from), count()
FROM gpu_data.dim_token_model_alias_dist
WHERE canonical != ''
  AND canonical GLOBAL NOT IN (
      SELECT alias FROM gpu_data.dim_token_model_alias_dist WHERE alias = canonical
  )
GROUP BY canonical

UNION ALL

-- 6) service_not_in_registry: alias 행의 defining_service는 메트릭 레지스트리 서비스와 바이트 동일해야 함
SELECT 'service_not_in_registry', alias, effective_from, toUInt64(1)
FROM gpu_data.dim_token_model_alias_dist
WHERE defining_service != ''
  AND defining_service GLOBAL NOT IN (
      SELECT service FROM gpu_data.dim_token_metrics_service_dist
  );
```

- [ ] **Step 2: `seed_dim_token_gpu_tco.sql`**

`assets/model-catalog/ddl/company/seed_dim_token_gpu_tco.sql`:

```sql
-- =============================================================
-- gpu_data.dim_token_gpu_tco 시드 (설계 2026-08-31 §4.2 — dim_holiday 3요소 패턴)
-- (a) 출처·기준일: 플레이스홀더만 — unknown + {H100, A100, H200, L40S} 전부 NULL(2026-01-01).
--     실값(TCO팀 확정, KRW/GPU-h)은 assets/model-catalog/csv_to_layer_c_dim_insert.py --table gpu_tco
--     생성 SQL(gitignore: dim_token_gpu_*_insert*.sql)을 admin이 append 적용 — effective_from = 소급 시작일(> 2026-01-01).
-- (b) NOT IN 멱등 가드 — 재실행 안전.
-- (c) 말미 검증 SELECT — 결과가 비어야 정상.
-- 실행 주체: admin 수동. mart는 SELECT만. stage 합성값은 fixtures/stage_seed_dim_token_gpu_tco.sql.
-- =============================================================

INSERT INTO gpu_data.dim_token_gpu_tco_dist
    (gpu_type, effective_from, tco_krw_per_gpu_hour, currency, basis, note)
SELECT *
FROM (
    -- 계약 표준값 unknown: TCO NULL — 미등록 기종 비용이 0원으로 위장되지 않게
    SELECT 'unknown' AS gpu_type, toDate('2026-01-01') AS effective_from,
           CAST(NULL AS Nullable(Float64)) AS tco_krw_per_gpu_hour,
           'KRW' AS currency, '' AS basis, '계약 표준 값 — TCO 산정 불가' AS note
    UNION ALL
    SELECT 'H100', toDate('2026-01-01'), CAST(NULL AS Nullable(Float64)), 'KRW', '',
           '플레이스홀더 — 실값은 Layer C 생성 SQL로 append (gpu_type_no_tco 발화 대상)'
    UNION ALL
    SELECT 'A100', toDate('2026-01-01'), CAST(NULL AS Nullable(Float64)), 'KRW', '',
           '플레이스홀더 — 실값은 Layer C 생성 SQL로 append'
    UNION ALL
    SELECT 'H200', toDate('2026-01-01'), CAST(NULL AS Nullable(Float64)), 'KRW', '',
           '플레이스홀더 — 실값은 Layer C 생성 SQL로 append'
    UNION ALL
    SELECT 'L40S', toDate('2026-01-01'), CAST(NULL AS Nullable(Float64)), 'KRW', '',
           '플레이스홀더 — 실값은 Layer C 생성 SQL로 append'
)
WHERE (gpu_type, effective_from) NOT IN (
    SELECT gpu_type, effective_from FROM gpu_data.dim_token_gpu_tco_dist
)
SETTINGS insert_distributed_sync = 1;

-- 검증: 결과가 비어야 정상 ------------------------------------------------
-- 1) (gpu_type, effective_from) 키 중복 없음
SELECT 'dup_key' AS check_name, gpu_type AS key, effective_from, count() AS cnt
FROM gpu_data.dim_token_gpu_tco_dist
GROUP BY gpu_type, effective_from
HAVING count() > 1

UNION ALL

-- 2) unknown 행 존재 + TCO 전부 NULL (행 부재 또는 값 오염 시 1행 노출)
SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'), countIf(tco_krw_per_gpu_hour IS NOT NULL)
FROM gpu_data.dim_token_gpu_tco_dist
WHERE gpu_type = 'unknown'
HAVING count() = 0 OR countIf(tco_krw_per_gpu_hour IS NOT NULL) > 0

UNION ALL

-- 3) basis 도메인
SELECT 'basis_domain', gpu_type, effective_from, toUInt64(1)
FROM gpu_data.dim_token_gpu_tco_dist
WHERE basis NOT IN ('', 'depreciation', 'lease', 'power-inclusive', 'tco')

UNION ALL

-- 4) 통화 KRW 고정
SELECT 'currency_krw', gpu_type, effective_from, toUInt64(1)
FROM gpu_data.dim_token_gpu_tco_dist
WHERE currency != 'KRW';
```

- [ ] **Step 3: `seed_dim_token_gpu_allocation.sql`**

`assets/model-catalog/ddl/company/seed_dim_token_gpu_allocation.sql`:

```sql
-- =============================================================
-- gpu_data.dim_token_gpu_allocation 시드 (설계 2026-08-31 §4.2 — dim_holiday 3요소 패턴)
-- (a) 출처·기준일: 플레이스홀더만 — (service_group='unknown', gpu_type='unknown', NULL, 2026-01-01).
--     실값(그룹별 GPU 할당 장수)은 assets/model-catalog/csv_to_layer_c_dim_insert.py --table gpu_allocation
--     생성 SQL(gitignore)을 admin이 append 적용. M2는 gpu_type='unknown' 행을 무시(no_allocation으로만 노출).
-- (b) NOT IN 멱등 가드 — 재실행 안전.
-- (c) 말미 검증 SELECT — 결과가 비어야 정상.
-- 실행 주체: admin 수동. mart는 SELECT만. stage 합성값은 fixtures/stage_seed_dim_token_gpu_allocation.sql.
-- =============================================================

INSERT INTO gpu_data.dim_token_gpu_allocation_dist
    (service_group, gpu_type, effective_from, allocated_gpu_count, source, note)
SELECT *
FROM (
    SELECT 'unknown' AS service_group, 'unknown' AS gpu_type, toDate('2026-01-01') AS effective_from,
           CAST(NULL AS Nullable(Float64)) AS allocated_gpu_count,
           'seed' AS source, '계약 표준 값 — 할당표 미확정 플레이스홀더' AS note
)
WHERE (service_group, gpu_type, effective_from) NOT IN (
    SELECT service_group, gpu_type, effective_from FROM gpu_data.dim_token_gpu_allocation_dist
)
SETTINGS insert_distributed_sync = 1;

-- 검증: 결과가 비어야 정상 ------------------------------------------------
-- 1) (service_group, gpu_type, effective_from) 키 중복 없음
SELECT 'dup_key' AS check_name, concat(service_group, '/', gpu_type) AS key, effective_from, count() AS cnt
FROM gpu_data.dim_token_gpu_allocation_dist
GROUP BY service_group, gpu_type, effective_from
HAVING count() > 1

UNION ALL

-- 2) unknown 플레이스홀더 행 존재 + gpu_type='unknown' 행은 전부 NULL (값이 들어가면 M2 무시 대상에 값이 숨는 것)
SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'), countIf(allocated_gpu_count IS NOT NULL)
FROM gpu_data.dim_token_gpu_allocation_dist
WHERE gpu_type = 'unknown'
HAVING count() = 0 OR countIf(allocated_gpu_count IS NOT NULL) > 0

UNION ALL

-- 3) 음수 할당 금지 (철회는 0 행)
SELECT 'negative_count', concat(service_group, '/', gpu_type), effective_from, toUInt64(1)
FROM gpu_data.dim_token_gpu_allocation_dist
WHERE allocated_gpu_count < 0;
```

- [ ] **Step 4: `seed_dim_token_vendor_price.sql`**

`assets/model-catalog/ddl/company/seed_dim_token_vendor_price.sql`:

```sql
-- =============================================================
-- gpu_data.dim_token_vendor_price 시드 (설계 2026-08-31 §4.2 — dim_holiday 3요소 패턴)
-- (a) 출처·기준일: 플레이스홀더만 — (provider='unknown', model='unknown', tier='standard', 단가 NULL, 2026-01-01).
--     실값(벤더 공표가 × 환율, KRW/MTok)은 assets/model-catalog/csv_to_layer_c_dim_insert.py --table vendor_price
--     생성 SQL(gitignore: dim_token_vendor_price_insert*.sql)을 admin이 append 적용.
-- (b) NOT IN 멱등 가드 — 재실행 안전.
-- (c) 말미 검증 SELECT — 결과가 비어야 정상.
-- 실행 주체: admin 수동. mart는 SELECT만. stage 합성값은 fixtures/stage_seed_dim_token_vendor_price.sql.
-- =============================================================

INSERT INTO gpu_data.dim_token_vendor_price_dist
    (provider, model, tier, effective_from, krw_per_mtok_input, krw_per_mtok_cached,
     krw_per_mtok_cache_creation, krw_per_mtok_output, note)
SELECT *
FROM (
    SELECT 'unknown' AS provider, 'unknown' AS model, 'standard' AS tier, toDate('2026-01-01') AS effective_from,
           CAST(NULL AS Nullable(Float64)) AS krw_per_mtok_input,
           CAST(NULL AS Nullable(Float64)) AS krw_per_mtok_cached,
           CAST(NULL AS Nullable(Float64)) AS krw_per_mtok_cache_creation,
           CAST(NULL AS Nullable(Float64)) AS krw_per_mtok_output,
           '계약 표준 값 — 벤더 단가 미확정 플레이스홀더' AS note
)
WHERE (provider, model, tier, effective_from) NOT IN (
    SELECT provider, model, tier, effective_from FROM gpu_data.dim_token_vendor_price_dist
)
SETTINGS insert_distributed_sync = 1;

-- 검증: 결과가 비어야 정상 ------------------------------------------------
-- 1) (provider, model, tier, effective_from) 키 중복 없음
SELECT 'dup_key' AS check_name, concat(provider, '/', model, '/', tier) AS key, effective_from, count() AS cnt
FROM gpu_data.dim_token_vendor_price_dist
GROUP BY provider, model, tier, effective_from
HAVING count() > 1

UNION ALL

-- 2) unknown 행 존재 + 단가 전부 NULL
SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'),
       countIf(krw_per_mtok_input IS NOT NULL OR krw_per_mtok_cached IS NOT NULL
               OR krw_per_mtok_cache_creation IS NOT NULL OR krw_per_mtok_output IS NOT NULL)
FROM gpu_data.dim_token_vendor_price_dist
WHERE provider = 'unknown' AND model = 'unknown'
HAVING count() = 0
    OR countIf(krw_per_mtok_input IS NOT NULL OR krw_per_mtok_cached IS NOT NULL
               OR krw_per_mtok_cache_creation IS NOT NULL OR krw_per_mtok_output IS NOT NULL) > 0

UNION ALL

-- 3) tier 도메인
SELECT 'tier_domain', concat(provider, '/', model, '/', tier), effective_from, toUInt64(1)
FROM gpu_data.dim_token_vendor_price_dist
WHERE tier NOT IN ('standard', 'batch', 'flex', 'priority');
```

- [ ] **Step 5: stage 합성 fixture 4 (`assets/model-catalog/fixtures/`)**

```bash
mkdir -p /home/mini/github/token-data-pipeline/assets/model-catalog/fixtures
```

`assets/model-catalog/fixtures/stage_seed_dim_token_model_alias.sql`:

```sql
-- =============================================================
-- [stage 전용] gpu_data.dim_token_model_alias 합성 시드 — 사내 적용 금지 (설계 §4.0: stage 합성값은 생성기 밖 fixture)
-- 합성 데이터: tools/mock-provider 모델 3종(claude-opus-4-8 / claude-sonnet-5 / claude-haiku-4-5)의 identity + 별칭 2건.
-- 적용: clickhouse-client --multiquery < assets/model-catalog/fixtures/stage_seed_dim_token_model_alias.sql
--   (stage 런북 절차 — docs/operations/token-metrics-deploy.md, Plan 6c). 3요소(NOT IN 가드·동기 삽입·말미 검증) 동일.
-- 적용 순서: ddl/stage/seed_dim_token_*.sql(플레이스홀더 행, effective_from 2026-01-01) **이후**에 적용한다. 실값 행은
--   effective_from 2026-08-01 — 시드 키(2026-01-01)를 재사용하지 않는다(NOT IN 가드가 시드 행과 충돌해 무음 skip 되는 사고 방지).
-- =============================================================

INSERT INTO gpu_data.dim_token_model_alias_dist
    (alias, effective_from, canonical, defining_service, source, note)
SELECT *
FROM (
    SELECT 'unknown' AS alias, toDate('2026-01-01') AS effective_from, 'unknown' AS canonical,
           '' AS defining_service, 'seed' AS source, '합성 — identity' AS note
    UNION ALL
    SELECT 'claude-opus-4-8', toDate('2026-08-01'), 'claude-opus-4-8', '', 'seed', '합성 — identity'
    UNION ALL
    SELECT 'claude-sonnet-5', toDate('2026-08-01'), 'claude-sonnet-5', '', 'seed', '합성 — identity'
    UNION ALL
    SELECT 'claude-haiku-4-5', toDate('2026-08-01'), 'claude-haiku-4-5', '', 'seed', '합성 — identity'
    UNION ALL
    SELECT 'claude-sonnet-5-20260101', toDate('2026-08-01'), 'claude-sonnet-5', 'Mock Service A', 'seed', '합성 — 날짜 접미 별칭'
    UNION ALL
    SELECT 'opus-4.8', toDate('2026-08-01'), 'claude-opus-4-8', 'Mock Service B', 'seed', '합성 — 축약 별칭'
)
WHERE (alias, effective_from) NOT IN (
    SELECT alias, effective_from FROM gpu_data.dim_token_model_alias_dist
)
SETTINGS insert_distributed_sync = 1;

-- 검증: 결과가 비어야 정상 ------------------------------------------------
SELECT 'dup_key' AS check_name, alias AS key, effective_from, count() AS cnt
FROM gpu_data.dim_token_model_alias_dist
GROUP BY alias, effective_from
HAVING count() > 1

UNION ALL

SELECT 'alias_maps_to_two_canonicals', alias, effective_from, uniqExact(canonical)
FROM gpu_data.dim_token_model_alias_dist
GROUP BY alias, effective_from
HAVING uniqExact(canonical) > 1

UNION ALL

SELECT 'alias_loop', alias, effective_from, toUInt64(1)
FROM gpu_data.dim_token_model_alias_dist
WHERE alias != canonical
  AND canonical GLOBAL IN (
      SELECT alias FROM gpu_data.dim_token_model_alias_dist WHERE alias != canonical
  )

UNION ALL

SELECT 'empty_canonical', alias, effective_from, toUInt64(1)
FROM gpu_data.dim_token_model_alias_dist
WHERE canonical = ''

UNION ALL

SELECT 'missing_identity_row', canonical, min(effective_from), count()
FROM gpu_data.dim_token_model_alias_dist
WHERE canonical != ''
  AND canonical GLOBAL NOT IN (
      SELECT alias FROM gpu_data.dim_token_model_alias_dist WHERE alias = canonical
  )
GROUP BY canonical

UNION ALL

SELECT 'service_not_in_registry', alias, effective_from, toUInt64(1)
FROM gpu_data.dim_token_model_alias_dist
WHERE defining_service != ''
  AND defining_service GLOBAL NOT IN (
      SELECT service FROM gpu_data.dim_token_metrics_service_dist
  );
```

`assets/model-catalog/fixtures/stage_seed_dim_token_gpu_tco.sql`:

```sql
-- =============================================================
-- [stage 전용] gpu_data.dim_token_gpu_tco 합성 시드 — 사내 적용 금지 (설계 §4.0)
-- 합성 데이터: KRW/GPU-h 임의값(실제 TCO 아님) — mart-metrics stage e2e에서 비용 산식 경로만 검증.
-- 적용: clickhouse-client --multiquery < assets/model-catalog/fixtures/stage_seed_dim_token_gpu_tco.sql
-- 적용 순서: ddl/stage/seed_dim_token_*.sql(플레이스홀더 행, effective_from 2026-01-01) **이후**에 적용한다. 실값 행은
--   effective_from 2026-08-01 — 시드 키(2026-01-01)를 재사용하지 않는다(NOT IN 가드가 시드 행과 충돌해 무음 skip 되는 사고 방지).
-- =============================================================

INSERT INTO gpu_data.dim_token_gpu_tco_dist
    (gpu_type, effective_from, tco_krw_per_gpu_hour, currency, basis, note)
SELECT *
FROM (
    SELECT 'unknown' AS gpu_type, toDate('2026-01-01') AS effective_from,
           CAST(NULL AS Nullable(Float64)) AS tco_krw_per_gpu_hour,
           'KRW' AS currency, '' AS basis, '합성 — TCO 산정 불가' AS note
    UNION ALL
    -- 실값 행은 2026-08-01: 시드(ddl/stage/seed_dim_token_gpu_tco.sql)의 H100/A100/H200/L40S NULL 행 키(2026-01-01)와 겹치면
    -- NOT IN 가드가 이 행을 무음 skip 한다(시드가 먼저 적용됨) — 그래서 시드 키를 재사용하지 않는다
    SELECT 'H100', toDate('2026-08-01'), toNullable(4200.0), 'KRW', 'tco', '합성값 — stage 전용'
    UNION ALL
    SELECT 'A100', toDate('2026-08-01'), toNullable(2100.0), 'KRW', 'tco', '합성값 — stage 전용'
    UNION ALL
    SELECT 'H200', toDate('2026-08-01'), toNullable(5300.0), 'KRW', 'tco', '합성값 — stage 전용'
    UNION ALL
    SELECT 'L40S', toDate('2026-08-01'), toNullable(1300.0), 'KRW', 'tco', '합성값 — stage 전용'
    UNION ALL
    -- TCO 미등록 기종 경로(gpu_type_no_tco·tco_missing=1) 검증용: 행 자체를 두지 않는다 — B200은 의도적 부재
    SELECT 'H100', toDate('2026-08-26'), toNullable(4300.0), 'KRW', 'tco', '합성값 — 이력 2행째(effective_from 갱신 경로)'
)
WHERE (gpu_type, effective_from) NOT IN (
    SELECT gpu_type, effective_from FROM gpu_data.dim_token_gpu_tco_dist
)
SETTINGS insert_distributed_sync = 1;

-- 검증: 결과가 비어야 정상 ------------------------------------------------
SELECT 'dup_key' AS check_name, gpu_type AS key, effective_from, count() AS cnt
FROM gpu_data.dim_token_gpu_tco_dist
GROUP BY gpu_type, effective_from
HAVING count() > 1

UNION ALL

SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'), countIf(tco_krw_per_gpu_hour IS NOT NULL)
FROM gpu_data.dim_token_gpu_tco_dist
WHERE gpu_type = 'unknown'
HAVING count() = 0 OR countIf(tco_krw_per_gpu_hour IS NOT NULL) > 0

UNION ALL

SELECT 'basis_domain', gpu_type, effective_from, toUInt64(1)
FROM gpu_data.dim_token_gpu_tco_dist
WHERE basis NOT IN ('', 'depreciation', 'lease', 'power-inclusive', 'tco')

UNION ALL

SELECT 'currency_krw', gpu_type, effective_from, toUInt64(1)
FROM gpu_data.dim_token_gpu_tco_dist
WHERE currency != 'KRW';
```

`assets/model-catalog/fixtures/stage_seed_dim_token_gpu_allocation.sql`:

```sql
-- =============================================================
-- [stage 전용] gpu_data.dim_token_gpu_allocation 합성 시드 — 사내 적용 금지 (설계 §4.0)
-- 합성 데이터: Mock Group(tools/mock-provider·endpoints.yaml serviceGroup) H100 8장·A100 4장 — M2 그룹 행 경로 검증.
-- 적용: clickhouse-client --multiquery < assets/model-catalog/fixtures/stage_seed_dim_token_gpu_allocation.sql
-- 적용 순서: ddl/stage/seed_dim_token_*.sql(플레이스홀더 행, effective_from 2026-01-01) **이후**에 적용한다. 실값 행은
--   effective_from 2026-08-01 — 시드 키(2026-01-01)를 재사용하지 않는다(NOT IN 가드가 시드 행과 충돌해 무음 skip 되는 사고 방지).
-- =============================================================

INSERT INTO gpu_data.dim_token_gpu_allocation_dist
    (service_group, gpu_type, effective_from, allocated_gpu_count, source, note)
SELECT *
FROM (
    SELECT 'unknown' AS service_group, 'unknown' AS gpu_type, toDate('2026-01-01') AS effective_from,
           CAST(NULL AS Nullable(Float64)) AS allocated_gpu_count,
           'seed' AS source, '합성 — 플레이스홀더' AS note
    UNION ALL
    SELECT 'Mock Group', 'H100', toDate('2026-08-01'), toNullable(8.0), 'seed', '합성값 — stage 전용'
    UNION ALL
    SELECT 'Mock Group', 'A100', toDate('2026-08-01'), toNullable(4.0), 'seed', '합성값 — stage 전용'
)
WHERE (service_group, gpu_type, effective_from) NOT IN (
    SELECT service_group, gpu_type, effective_from FROM gpu_data.dim_token_gpu_allocation_dist
)
SETTINGS insert_distributed_sync = 1;

-- 검증: 결과가 비어야 정상 ------------------------------------------------
SELECT 'dup_key' AS check_name, concat(service_group, '/', gpu_type) AS key, effective_from, count() AS cnt
FROM gpu_data.dim_token_gpu_allocation_dist
GROUP BY service_group, gpu_type, effective_from
HAVING count() > 1

UNION ALL

SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'), countIf(allocated_gpu_count IS NOT NULL)
FROM gpu_data.dim_token_gpu_allocation_dist
WHERE gpu_type = 'unknown'
HAVING count() = 0 OR countIf(allocated_gpu_count IS NOT NULL) > 0

UNION ALL

SELECT 'negative_count', concat(service_group, '/', gpu_type), effective_from, toUInt64(1)
FROM gpu_data.dim_token_gpu_allocation_dist
WHERE allocated_gpu_count < 0;
```

`assets/model-catalog/fixtures/stage_seed_dim_token_vendor_price.sql`:

```sql
-- =============================================================
-- [stage 전용] gpu_data.dim_token_vendor_price 합성 시드 — 사내 적용 금지 (설계 §4.0)
-- 합성 데이터: seed_dim_token_model.sql 공표 USD 단가 × 합성 환율 1350 (실제 환율·계약가 아님) — M4 external_api 경로 검증.
-- 적용: clickhouse-client --multiquery < assets/model-catalog/fixtures/stage_seed_dim_token_vendor_price.sql
-- 적용 순서: ddl/stage/seed_dim_token_*.sql(플레이스홀더 행, effective_from 2026-01-01) **이후**에 적용한다. 실값 행은
--   effective_from 2026-08-01 — 시드 키(2026-01-01)를 재사용하지 않는다(NOT IN 가드가 시드 행과 충돌해 무음 skip 되는 사고 방지).
-- =============================================================

INSERT INTO gpu_data.dim_token_vendor_price_dist
    (provider, model, tier, effective_from, krw_per_mtok_input, krw_per_mtok_cached,
     krw_per_mtok_cache_creation, krw_per_mtok_output, note)
SELECT *
FROM (
    SELECT 'unknown' AS provider, 'unknown' AS model, 'standard' AS tier, toDate('2026-01-01') AS effective_from,
           CAST(NULL AS Nullable(Float64)) AS krw_per_mtok_input,
           CAST(NULL AS Nullable(Float64)) AS krw_per_mtok_cached,
           CAST(NULL AS Nullable(Float64)) AS krw_per_mtok_cache_creation,
           CAST(NULL AS Nullable(Float64)) AS krw_per_mtok_output,
           '합성 — 플레이스홀더' AS note
    UNION ALL
    SELECT 'anthropic', 'claude-opus-4-8', 'standard', toDate('2026-08-01'),
           toNullable(6750.0), toNullable(675.0), toNullable(8437.5), toNullable(33750.0), '합성값 — USD×1350'
    UNION ALL
    SELECT 'anthropic', 'claude-sonnet-5', 'standard', toDate('2026-08-01'),
           toNullable(4050.0), toNullable(405.0), toNullable(5062.5), toNullable(20250.0), '합성값 — USD×1350'
    UNION ALL
    SELECT 'anthropic', 'claude-haiku-4-5', 'standard', toDate('2026-08-01'),
           toNullable(1350.0), toNullable(135.0), toNullable(1687.5), toNullable(6750.0), '합성값 — USD×1350'
)
WHERE (provider, model, tier, effective_from) NOT IN (
    SELECT provider, model, tier, effective_from FROM gpu_data.dim_token_vendor_price_dist
)
SETTINGS insert_distributed_sync = 1;

-- 검증: 결과가 비어야 정상 ------------------------------------------------
SELECT 'dup_key' AS check_name, concat(provider, '/', model, '/', tier) AS key, effective_from, count() AS cnt
FROM gpu_data.dim_token_vendor_price_dist
GROUP BY provider, model, tier, effective_from
HAVING count() > 1

UNION ALL

SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'),
       countIf(krw_per_mtok_input IS NOT NULL OR krw_per_mtok_cached IS NOT NULL
               OR krw_per_mtok_cache_creation IS NOT NULL OR krw_per_mtok_output IS NOT NULL)
FROM gpu_data.dim_token_vendor_price_dist
WHERE provider = 'unknown' AND model = 'unknown'
HAVING count() = 0
    OR countIf(krw_per_mtok_input IS NOT NULL OR krw_per_mtok_cached IS NOT NULL
               OR krw_per_mtok_cache_creation IS NOT NULL OR krw_per_mtok_output IS NOT NULL) > 0

UNION ALL

SELECT 'tier_domain', concat(provider, '/', model, '/', tier), effective_from, toUInt64(1)
FROM gpu_data.dim_token_vendor_price_dist
WHERE tier NOT IN ('standard', 'batch', 'flex', 'priority');
```

- [ ] **Step 6: lint 전체 GREEN + gitignore 음성 재확인**

```bash
cd /home/mini/github/token-data-pipeline/assets/model-catalog
python3 -m pytest -q tests/test_ddl_manifest.py 2>&1 | tail -n 1
cd /home/mini/github/token-data-pipeline
git check-ignore -v assets/model-catalog/ddl/company/seed_dim_token_gpu_tco.sql assets/model-catalog/fixtures/stage_seed_dim_token_gpu_tco.sql; echo "exit=$?"
```

기대: `45 passed`; `check-ignore` 출력 없음 + `exit=1`(무시 대상 아님).

- [ ] **Step 7: 커밋**

```bash
cd /home/mini/github/token-data-pipeline
git add assets/model-catalog/ddl/company/seed_dim_token_model_alias.sql assets/model-catalog/ddl/company/seed_dim_token_gpu_tco.sql assets/model-catalog/ddl/company/seed_dim_token_gpu_allocation.sql assets/model-catalog/ddl/company/seed_dim_token_vendor_price.sql assets/model-catalog/fixtures/stage_seed_dim_token_model_alias.sql assets/model-catalog/fixtures/stage_seed_dim_token_gpu_tco.sql assets/model-catalog/fixtures/stage_seed_dim_token_gpu_allocation.sql assets/model-catalog/fixtures/stage_seed_dim_token_vendor_price.sql
git commit -m "feat(assets): 메트릭 기준정보 시드 4(사내 플레이스홀더만) + stage 합성 fixture 4 (Plan 6a T6)

dim_holiday 3요소(NOT IN 가드·insert_distributed_sync·말미 검증) 그대로. 사내 시드는 unknown·{H100,A100,H200,L40S} NULL 플레이스홀더(effective_from 2026-01-01)만, 합성 수치는 fixtures/stage_seed_*.sql에만(설계 §4.0) — 실값 행은 effective_from 2026-08-01로 시드 키와 비충돌(시드 후 적용, NOT IN 무음 skip 방지). alias 시드·fixture는 설계 §4.2 검증 6종(service_not_in_registry 포함). 검증 4열 계약(check_name,key,effective_from,cnt)·GLOBAL IN.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

---

### Task 7: 생성기 등록(`gen_stage_ddl.py` SOURCES +14 · `gen_verify_ddl.py` MODULES +2) · 미러 28파일 재생성 · `test-assets.yml` additive

**Files:**
- Modify (additive): `tools/gen_stage_ddl.py` — `SOURCES` 리스트 말미에 14항목 추가
- Modify (additive): `tools/gen_verify_ddl.py` — `MODULES` 리스트에 모듈 2개 추가(assets 항목은 주석만 보강)
- Generated (14): `collectors/token-metrics/ddl/stage/{raw_token_metrics,dim_token_metrics_service,accounts}.sql`, `mart/token-metrics/ddl/stage/{mart_metrics_tables,accounts}.sql`, `assets/model-catalog/ddl/stage/{dim_token_model_alias,dim_token_gpu_tco,dim_token_gpu_allocation,dim_token_vendor_price,seed_dim_token_model_alias,seed_dim_token_gpu_tco,seed_dim_token_gpu_allocation,seed_dim_token_vendor_price,accounts_metrics}.sql`
- Generated (14): 위와 동일 파일명의 `ddl/company-verify/` 미러
- Modify (additive): `.github/workflows/test-assets.yml` — paths +4(push·pull_request 동일), 잔존 grep 디렉터리 +2, job `unit-model-catalog` 추가

**Interfaces:**
- `python3 tools/gen_stage_ddl.py` / `--check` — 출력 경로 = 원본 rel의 `/company/` → `/stage/` 치환. 변환: `GRANT ON CLUSTER 'gpu-monitoring' ` 제거, `\nON CLUSTER 'gpu-monitoring'` 제거, ` ON CLUSTER 'gpu-monitoring'` 제거, `ReplicatedMergeTree(...)` → `MergeTree`(괄호 없음 — `tools/gen_stage_ddl.py` 66행 치환 문자열·Step 5 grep과 동일). `STAGE_DB_PREPEND`는 token-usage accounts 2파일에만 붙는다(신규 파일 무관).
- `python3 tools/gen_verify_ddl.py` / `--check` — `\b(fact|gpu_data|mart)\.`·ZK 경로·Distributed 2번째 인자·`CREATE DATABASE IF NOT EXISTS <db>`·`\bTO mart\b` 치환. **파일명이 정확히 `accounts.sql`일 때만** 격리 DB 3종 + 계정 생성 프리펜드 → `collectors/token-metrics/…/accounts.sql`·`mart/token-metrics/…/accounts.sql`에는 붙고, `assets/model-catalog/…/accounts_metrics.sql`에는 GENERATED 헤더만 붙는다(설계 해석 — Self-Review 노트에 기록).
- 등록 후 두 생성기 모두 **25개 파일**(기존 11 + 신규 14).

**선행 상태 주의**: T5·T6 커밋 시점에는 `assets/model-catalog/ddl/company-verify/`가 glob(`"files": None`) 대상이라 `gen_verify_ddl.py --check`가 `[MISSING] …` 9건으로 **exit 1**(CI `verify-ddl` red)이다. 이 Task가 끝나야 green — PR head 기준으로 판정하므로 중간 커밋의 red는 허용한다(PR 본문에 명기).

- [ ] **Step 1: 등록 전 기준선 확인 (RED)**

Run:
```bash
cd /home/mini/github/token-data-pipeline
python3 tools/gen_stage_ddl.py --check; echo "stage exit=$?"
python3 tools/gen_verify_ddl.py --check 2>&1 | tail -3; echo "verify exit=${PIPESTATUS[0]}"
```
Expected:
```text
--check OK: 11개 파일 전부 커밋본과 일치
stage exit=0
[MISSING] assets/model-catalog/ddl/company-verify/seed_dim_token_model_alias.sql
[MISSING] assets/model-catalog/ddl/company-verify/seed_dim_token_vendor_price.sql
[ERROR] --check: 9개 파일이 커밋본과 불일치 — python3 tools/gen_verify_ddl.py 재실행 후 커밋하세요.
verify exit=1
```
(stage는 SOURCES 미등록이라 신규 파일을 모르므로 11개 OK; verify는 glob이 신규 9파일을 잡아 MISSING.)

- [ ] **Step 2: `tools/gen_stage_ddl.py` SOURCES에 14항목 추가 (앵커 치환 — 기존 11항목·변환 로직 무수정)**

Run (레포 루트에서):
```bash
cd /home/mini/github/token-data-pipeline
python3 - <<'PY'
import pathlib
p = pathlib.Path("tools/gen_stage_ddl.py")
s = p.read_text(encoding="utf-8")
old = '''    "assets/model-catalog/ddl/company/accounts.sql",
]
'''
new = '''    "assets/model-catalog/ddl/company/accounts.sql",
    # Plan 6a (설계 2026-08-31 §4.0 매니페스트) — collectors/mart token-metrics + 메트릭 기준정보 dim 4·시드 4·GRANT 사본
    "collectors/token-metrics/ddl/company/raw_token_metrics.sql",
    "collectors/token-metrics/ddl/company/dim_token_metrics_service.sql",
    "collectors/token-metrics/ddl/company/accounts.sql",
    "mart/token-metrics/ddl/company/mart_metrics_tables.sql",
    "mart/token-metrics/ddl/company/accounts.sql",
    "assets/model-catalog/ddl/company/dim_token_model_alias.sql",
    "assets/model-catalog/ddl/company/dim_token_gpu_tco.sql",
    "assets/model-catalog/ddl/company/dim_token_gpu_allocation.sql",
    "assets/model-catalog/ddl/company/dim_token_vendor_price.sql",
    "assets/model-catalog/ddl/company/seed_dim_token_model_alias.sql",
    "assets/model-catalog/ddl/company/seed_dim_token_gpu_tco.sql",
    "assets/model-catalog/ddl/company/seed_dim_token_gpu_allocation.sql",
    "assets/model-catalog/ddl/company/seed_dim_token_vendor_price.sql",
    "assets/model-catalog/ddl/company/accounts_metrics.sql",
]
'''
assert s.count(old) == 1, "SOURCES 말미 앵커가 정확히 1회 있어야 함"
p.write_text(s.replace(old, new), encoding="utf-8")
print("gen_stage_ddl.py SOURCES: +14")
PY
python3 -c "import ast,sys; ast.parse(open('tools/gen_stage_ddl.py').read()); print('ast OK')"
python3 -c "
import importlib.util
spec = importlib.util.spec_from_file_location('g', 'tools/gen_stage_ddl.py'); m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
print('SOURCES =', len(m.SOURCES))"
git diff --numstat -- tools/gen_stage_ddl.py
```
Expected:
```text
gen_stage_ddl.py SOURCES: +14
ast OK
SOURCES = 25
15	0	tools/gen_stage_ddl.py
```
(추가 15줄 = 주석 1 + 항목 14, 삭제 0.)

- [ ] **Step 3: `tools/gen_verify_ddl.py` MODULES에 모듈 2개 추가 (assets 항목은 주석만 보강)**

Run (레포 루트에서):
```bash
cd /home/mini/github/token-data-pipeline
python3 - <<'PY'
import pathlib
p = pathlib.Path("tools/gen_verify_ddl.py")
s = p.read_text(encoding="utf-8")
old = '''        "files": None,  # glob *.sql
    },
]
'''
new = '''        "files": None,  # glob *.sql  (Plan 6a: dim_token_{model_alias,gpu_tco,gpu_allocation,vendor_price}·seed 4·accounts_metrics.sql 포함)
    },
    # Plan 6a (설계 2026-08-31 §4.0) — 신규 모듈 2종. accounts.sql은 이름 규칙(⑤)대로 격리 DB·계정 프리펜드가 붙는다.
    {
        "label": "collectors/token-metrics",
        "src_dir": REPO_ROOT / "collectors/token-metrics/ddl/company",
        "out_dir": REPO_ROOT / "collectors/token-metrics/ddl/company-verify",
        "files": ["raw_token_metrics.sql", "dim_token_metrics_service.sql", "accounts.sql"],
    },
    {
        "label": "mart/token-metrics",
        "src_dir": REPO_ROOT / "mart/token-metrics/ddl/company",
        "out_dir": REPO_ROOT / "mart/token-metrics/ddl/company-verify",
        "files": ["mart_metrics_tables.sql", "accounts.sql"],
    },
]
'''
assert s.count(old) == 1, "MODULES 말미(assets glob 항목) 앵커가 정확히 1회 있어야 함"
p.write_text(s.replace(old, new), encoding="utf-8")
print("gen_verify_ddl.py MODULES: +2")
PY
python3 -c "import ast; ast.parse(open('tools/gen_verify_ddl.py').read()); print('ast OK')"
python3 -c "
import importlib.util
spec = importlib.util.spec_from_file_location('g', 'tools/gen_verify_ddl.py'); m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
print('MODULES =', len(m.MODULES), [x['label'] for x in m.MODULES])"
git diff --numstat -- tools/gen_verify_ddl.py
```
Expected:
```text
gen_verify_ddl.py MODULES: +2
ast OK
MODULES = 6 ['collectors/token-usage', 'mart/token-usage', 'assets/user-org', 'assets/model-catalog', 'collectors/token-metrics', 'mart/token-metrics']
14	1	tools/gen_verify_ddl.py
```
(삭제 1줄은 assets 항목의 `# glob *.sql` 주석 줄이 주석 보강으로 바뀐 것 — 코드 토큰 변화 없음. 기존 4항목의 label/src_dir/out_dir/files는 그대로.)

- [ ] **Step 4: 미러 28파일 생성 + `--check` 양쪽 GREEN**

Run:
```bash
cd /home/mini/github/token-data-pipeline
python3 tools/gen_verify_ddl.py 2>&1 | tail -1
python3 tools/gen_stage_ddl.py 2>&1 | tail -1
python3 tools/gen_verify_ddl.py --check; echo "verify exit=$?"
python3 tools/gen_stage_ddl.py --check; echo "stage exit=$?"
git status --porcelain --untracked-files=all | grep -c '^??'
git status --porcelain --untracked-files=all | grep '^??' | sed 's/^?? //' | sort
```
Expected:
```text
[OK] 25개 파일 생성 완료 (fact=token_verify_fact dim=token_verify_dim mart=token_verify_mart account=token_verify)
wrote assets/model-catalog/ddl/stage/accounts_metrics.sql
[OK] --check: 25개 파일 전부 커밋본과 일치
verify exit=0
--check OK: 25개 파일 전부 커밋본과 일치
stage exit=0
28
assets/model-catalog/ddl/company-verify/accounts_metrics.sql
assets/model-catalog/ddl/company-verify/dim_token_gpu_allocation.sql
assets/model-catalog/ddl/company-verify/dim_token_gpu_tco.sql
assets/model-catalog/ddl/company-verify/dim_token_model_alias.sql
assets/model-catalog/ddl/company-verify/dim_token_vendor_price.sql
assets/model-catalog/ddl/company-verify/seed_dim_token_gpu_allocation.sql
assets/model-catalog/ddl/company-verify/seed_dim_token_gpu_tco.sql
assets/model-catalog/ddl/company-verify/seed_dim_token_model_alias.sql
assets/model-catalog/ddl/company-verify/seed_dim_token_vendor_price.sql
assets/model-catalog/ddl/stage/accounts_metrics.sql
assets/model-catalog/ddl/stage/dim_token_gpu_allocation.sql
assets/model-catalog/ddl/stage/dim_token_gpu_tco.sql
assets/model-catalog/ddl/stage/dim_token_model_alias.sql
assets/model-catalog/ddl/stage/dim_token_vendor_price.sql
assets/model-catalog/ddl/stage/seed_dim_token_gpu_allocation.sql
assets/model-catalog/ddl/stage/seed_dim_token_gpu_tco.sql
assets/model-catalog/ddl/stage/seed_dim_token_model_alias.sql
assets/model-catalog/ddl/stage/seed_dim_token_vendor_price.sql
collectors/token-metrics/ddl/company-verify/accounts.sql
collectors/token-metrics/ddl/company-verify/dim_token_metrics_service.sql
collectors/token-metrics/ddl/company-verify/raw_token_metrics.sql
collectors/token-metrics/ddl/stage/accounts.sql
collectors/token-metrics/ddl/stage/dim_token_metrics_service.sql
collectors/token-metrics/ddl/stage/raw_token_metrics.sql
mart/token-metrics/ddl/company-verify/accounts.sql
mart/token-metrics/ddl/company-verify/mart_metrics_tables.sql
mart/token-metrics/ddl/stage/accounts.sql
mart/token-metrics/ddl/stage/mart_metrics_tables.sql
```
(`--untracked-files=all`은 통째로 새 디렉터리(`collectors/token-metrics/ddl/stage/` 등 4개)를 파일 단위로 펼치기 위함 — 기본 모드면 디렉터리 4줄로 접혀 22줄이 된다. `??`는 신규 28파일뿐이어야 하고, ` M`은 `tools/gen_stage_ddl.py`·`tools/gen_verify_ddl.py` 2건뿐이어야 한다 — 기존 미러 22파일이 `M`으로 뜨면 생성기 변환 로직을 건드린 것이므로 되돌린다.)

- [ ] **Step 5: 생성 결과 구조 검증 — 잔존 0 · 프리펜드 규칙 · stage 변환 · 기존 미러 zero-diff**

Run:
```bash
cd /home/mini/github/token-data-pipeline
# (a) company-verify 6디렉터리 원 DB 구조 토큰 잔존 0 (CI verify-ddl과 동일 정규식)
! grep -rnE "\b(fact|gpu_data|mart)\.|/clickhouse/tables/\{shard\}/(fact|gpu_data|mart)/|Distributed\('gpu-monitoring', ?'(fact|gpu_data|mart)',|CREATE DATABASE IF NOT EXISTS (fact|gpu_data|mart)\b|\bTO mart\b" \
  collectors/token-usage/ddl/company-verify mart/token-usage/ddl/company-verify \
  assets/user-org/ddl/company-verify assets/model-catalog/ddl/company-verify \
  collectors/token-metrics/ddl/company-verify mart/token-metrics/ddl/company-verify && echo "residual grep OK"
# (b) 프리펜드 규칙: accounts.sql 2파일에는 격리 DB·계정 생성이 붙고, accounts_metrics.sql에는 안 붙는다
grep -c "CREATE DATABASE IF NOT EXISTS token_verify_\|CREATE USER IF NOT EXISTS token_verify" \
  collectors/token-metrics/ddl/company-verify/accounts.sql mart/token-metrics/ddl/company-verify/accounts.sql assets/model-catalog/ddl/company-verify/accounts_metrics.sql
# (c) stage 변환: 코드 라인(주석 `--` 제외)에 ON CLUSTER·Replicated 잔존 0, GRANT 문 수는 company와 동일
grep -rn "ON CLUSTER\|ReplicatedMergeTree" collectors/token-metrics/ddl/stage mart/token-metrics/ddl/stage assets/model-catalog/ddl/stage | grep -vcE '^[^:]+:[0-9]+:\s*--'
grep -c "^GRANT " collectors/token-metrics/ddl/stage/accounts.sql mart/token-metrics/ddl/stage/accounts.sql assets/model-catalog/ddl/stage/accounts_metrics.sql
grep -c "^GRANT " collectors/token-metrics/ddl/company/accounts.sql mart/token-metrics/ddl/company/accounts.sql assets/model-catalog/ddl/company/accounts_metrics.sql
# (d) 격리 미러 치환 확인 — ZK 경로·Distributed 인자·GRANT 대상
grep -c "/clickhouse/tables/{shard}/token_verify_fact/" collectors/token-metrics/ddl/company-verify/raw_token_metrics.sql
grep -c "Distributed('gpu-monitoring', 'token_verify_mart', " mart/token-metrics/ddl/company-verify/mart_metrics_tables.sql
grep -c "TO token_verify;" assets/model-catalog/ddl/company-verify/accounts_metrics.sql
# (e) 기존 미러·zero-diff 목록 무변경
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org \
  assets/model-catalog/ddl/company/dim_token_model.sql assets/model-catalog/ddl/company/seed_dim_token_model.sql assets/model-catalog/ddl/company/accounts.sql \
  assets/model-catalog/ddl/stage/dim_token_model.sql assets/model-catalog/ddl/stage/seed_dim_token_model.sql assets/model-catalog/ddl/stage/accounts.sql \
  assets/model-catalog/ddl/company-verify/dim_token_model.sql assets/model-catalog/ddl/company-verify/seed_dim_token_model.sql assets/model-catalog/ddl/company-verify/accounts.sql \
  assets/model-catalog/README.md assets/model-catalog/ddl/README.md tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json \
  .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml | wc -l
# (f) lint 여전히 GREEN (매니페스트 14파일은 무변경)
cd assets/model-catalog && python3 -m pytest -q 2>&1 | tail -1
```
Expected:
```text
residual grep OK
collectors/token-metrics/ddl/company-verify/accounts.sql:4
mart/token-metrics/ddl/company-verify/accounts.sql:4
assets/model-catalog/ddl/company-verify/accounts_metrics.sql:0
0
collectors/token-metrics/ddl/stage/accounts.sql:15
mart/token-metrics/ddl/stage/accounts.sql:21
assets/model-catalog/ddl/stage/accounts_metrics.sql:4
collectors/token-metrics/ddl/company/accounts.sql:15
mart/token-metrics/ddl/company/accounts.sql:21
assets/model-catalog/ddl/company/accounts_metrics.sql:4
4
4
4
0
45 passed in 0.XXs
```
(`(c)` 첫 줄 `0` = 코드 라인 잔존 0 — GENERATED 헤더 주석의 "ON CLUSTER 제거" 문구는 제외. GRANT 문 수 15/21/4는 T3·T4·T5 파일과 동일(stage는 `GRANT ON CLUSTER 'gpu-monitoring' ` 접두만 벗겨진다). grep -c의 `4`/`4`/`4`: fact 4테이블 ZK 경로 4, mart 4테이블 Distributed 4, dim 4종 GRANT 4. `(e)`의 `0`은 zero-diff 목록에 변경이 없다는 뜻.)

- [ ] **Step 6: 커밋 — 생성기 등록 + 미러 28파일**

```bash
cd /home/mini/github/token-data-pipeline
git add tools/gen_stage_ddl.py tools/gen_verify_ddl.py \
  collectors/token-metrics/ddl/stage collectors/token-metrics/ddl/company-verify \
  mart/token-metrics/ddl/stage mart/token-metrics/ddl/company-verify \
  assets/model-catalog/ddl/stage assets/model-catalog/ddl/company-verify
git status --porcelain | grep -vE '^(A|M)  ' | wc -l   # 기대: 0 (스테이징 안 된 변경 없음 — `A ` 신규 28, `M ` 생성기 2뿐)
git commit -m "chore(tools): gen_stage_ddl SOURCES +14·gen_verify_ddl MODULES +2 등록 + stage/company-verify 미러 28파일 재생성 (Plan 6a T7)

- gen_stage_ddl.py SOURCES 25항목(기존 11 무수정): collectors/token-metrics 3·mart/token-metrics 2·model-catalog 신규 9
- gen_verify_ddl.py MODULES 6항목(기존 4 무수정): collectors/token-metrics(files 3)·mart/token-metrics(files 2) 추가
- 양쪽 --check 25/25 일치, company-verify 6디렉터리 원 DB 구조 토큰 잔존 0
- accounts.sql(2)은 격리 DB·계정 프리펜드, accounts_metrics.sql은 GENERATED 헤더만(이름 규칙 ⑤)
- 기존 미러 22파일·zero-diff 목록 무변경

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

- [ ] **Step 7: `.github/workflows/test-assets.yml` additive 편집 — paths +4 · 잔존 grep 디렉터리 +2 · job `unit-model-catalog`**

파일 전체를 아래로 교체한다(기존 job `verify-ddl`/`unit`/`e2e`의 step은 한 줄도 바뀌지 않는다 — 변경은 리스트 항목 추가와 job 1개 추가뿐이며 Step 8의 numstat로 확인).

`.github/workflows/test-assets.yml`:
```yaml
name: test-assets

on:
  push:
    branches: [main]
    paths: ["assets/**", ".github/workflows/test-assets.yml",
            "mart/token-usage/tests/e2e/ddl_test_dims.sql",
            "tools/gen_verify_ddl.py",
            "collectors/token-usage/ddl/company/**", "collectors/token-usage/ddl/company-verify/**",
            "**/ddl/stage/**", "tools/gen_stage_ddl.py",
            "mart/token-usage/ddl/company/**", "mart/token-usage/ddl/company-verify/**",
            "assets/user-org/ddl/company-verify/**", "assets/model-catalog/ddl/company-verify/**",
            "collectors/token-usage/k8s/overlays/company-verify/**",
            "mart/token-usage/k8s/overlays/company-verify/**",
            "collectors/token-metrics/ddl/company/**", "collectors/token-metrics/ddl/company-verify/**",
            "mart/token-metrics/ddl/company/**", "mart/token-metrics/ddl/company-verify/**"]
  pull_request:
    paths: ["assets/**", ".github/workflows/test-assets.yml",
            "mart/token-usage/tests/e2e/ddl_test_dims.sql",
            "tools/gen_verify_ddl.py",
            "collectors/token-usage/ddl/company/**", "collectors/token-usage/ddl/company-verify/**",
            "**/ddl/stage/**", "tools/gen_stage_ddl.py",
            "mart/token-usage/ddl/company/**", "mart/token-usage/ddl/company-verify/**",
            "assets/user-org/ddl/company-verify/**", "assets/model-catalog/ddl/company-verify/**",
            "collectors/token-usage/k8s/overlays/company-verify/**",
            "mart/token-usage/k8s/overlays/company-verify/**",
            "collectors/token-metrics/ddl/company/**", "collectors/token-metrics/ddl/company-verify/**",
            "mart/token-metrics/ddl/company/**", "mart/token-metrics/ddl/company-verify/**"]

jobs:
  verify-ddl:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: gen_verify_ddl.py --check (드리프트 가드)
        run: python3 tools/gen_verify_ddl.py --check
      - name: stage DDL drift check
        run: python3 tools/gen_stage_ddl.py --check
      - name: 원 DB명 구조 토큰 잔존 0 (격리 실패 시 최악 케이스 — ZK 경로 등)
        run: |
          ! grep -rnE "\b(fact|gpu_data|mart)\.|/clickhouse/tables/\{shard\}/(fact|gpu_data|mart)/|Distributed\('gpu-monitoring', ?'(fact|gpu_data|mart)',|CREATE DATABASE IF NOT EXISTS (fact|gpu_data|mart)\b|\bTO mart\b" \
            collectors/token-usage/ddl/company-verify \
            mart/token-usage/ddl/company-verify \
            assets/user-org/ddl/company-verify \
            assets/model-catalog/ddl/company-verify \
            collectors/token-metrics/ddl/company-verify \
            mart/token-metrics/ddl/company-verify
      - name: Render company-verify overlays
        run: |
          kubectl kustomize collectors/token-usage/k8s/overlays/company-verify > /tmp/collectors-company-verify.yaml
          kubectl kustomize mart/token-usage/k8s/overlays/company-verify > /tmp/mart-company-verify.yaml
          grep -q 'name: token-usage-collector-verify' /tmp/collectors-company-verify.yaml
          grep -q 'name: token-usage-ch-secret-verify' /tmp/collectors-company-verify.yaml
          grep -q 'name: token-usage-endpoints-verify' /tmp/collectors-company-verify.yaml
          grep -q 'name: token-mart-daily-verify' /tmp/mart-company-verify.yaml
          grep -q 'name: token-mart-ch-secret-verify' /tmp/mart-company-verify.yaml

  unit:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: assets/user-org
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install "pytest>=8"
      - run: python -m pytest tests/ -v --ignore=tests/e2e

  # Plan 6a: model-catalog — DDL 매니페스트 lint(T2) + alias 생성기(T8) + Layer C 생성기(T9)
  unit-model-catalog:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: assets/model-catalog
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install "pytest>=8"
      - run: python -m pytest tests/ -v --ignore=tests/e2e

  e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Run E2E
        run: ./assets/user-org/tests/e2e/run_e2e.sh
```

- [ ] **Step 8: 워크플로 검증 — YAML 파싱 · additive numstat · CI와 동일 커맨드 로컬 재현**

Run:
```bash
cd /home/mini/github/token-data-pipeline
python3 - <<'PY'
import yaml
d = yaml.safe_load(open(".github/workflows/test-assets.yml", encoding="utf-8"))
on = d.get("on", d.get(True))            # PyYAML 1.1은 'on'을 True로 읽는다
assert on["push"]["paths"] == on["pull_request"]["paths"], "push/pull_request paths 불일치"
assert len(on["push"]["paths"]) == 18, len(on["push"]["paths"])
assert list(d["jobs"]) == ["verify-ddl", "unit", "unit-model-catalog", "e2e"], list(d["jobs"])
assert d["jobs"]["unit-model-catalog"]["defaults"]["run"]["working-directory"] == "assets/model-catalog"
assert d["jobs"]["unit"]["defaults"]["run"]["working-directory"] == "assets/user-org"
grep_step = [s for s in d["jobs"]["verify-ddl"]["steps"] if s.get("name", "").startswith("원 DB명")][0]
for dd in ("collectors/token-metrics/ddl/company-verify", "mart/token-metrics/ddl/company-verify"):
    assert dd in grep_step["run"], dd
print("yaml OK: paths=18 jobs=4 grep dirs=6")
PY
git diff --numstat -- .github/workflows/test-assets.yml
wc -l .github/workflows/test-assets.yml
# CI 재현: verify-ddl 3단계 + unit-model-catalog
python3 tools/gen_verify_ddl.py --check | tail -1 && python3 tools/gen_stage_ddl.py --check
( cd assets/model-catalog && python3 -m pytest tests/ -v --ignore=tests/e2e 2>&1 | tail -1 )   # CI는 러너의 python, 로컬은 python3
```
Expected:
```text
yaml OK: paths=18 jobs=4 grep dirs=6
23	3	.github/workflows/test-assets.yml
96 .github/workflows/test-assets.yml
[OK] --check: 25개 파일 전부 커밋본과 일치
--check OK: 25개 파일 전부 커밋본과 일치
============================== 45 passed in 0.XXs ==============================
```
(numstat `23 3`: 삭제 3줄은 리스트 종결 `]` 2줄과 grep 마지막 디렉터리 줄에 계속 표시(`,`/`\`)가 붙은 것 — 항목 삭제 없음. PyYAML이 없으면 `pip install pyyaml` 후 재실행 — 워크플로 검증에만 쓰고 생성기는 stdlib only.)

- [ ] **Step 9: 커밋 — CI**

```bash
cd /home/mini/github/token-data-pipeline
git add .github/workflows/test-assets.yml
git commit -m "ci(ci): test-assets.yml — token-metrics ddl paths +4·잔존 grep 디렉터리 +2·job unit-model-catalog (Plan 6a T7)

- push/pull_request paths 동일 18항목(기존 14 무수정)
- verify-ddl 잔존 grep: collectors/token-metrics·mart/token-metrics company-verify 추가(6디렉터리)
- unit-model-catalog: assets/model-catalog에서 pytest tests/ --ignore=tests/e2e (T2 lint + T8/T9 생성기 테스트)
- 기존 job verify-ddl/unit/e2e step 무변경 (numstat 23/3 — 삭제 3줄은 리스트 종결 기호 이동)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

---

### Task 8: 메타데이터 시트 `모델` 탭 → alias INSERT 생성기 — `assets/model-catalog/sheet_to_dim_token_model_alias_insert.py` (+ 테스트·fixture)

**Files:**
- Create: `assets/model-catalog/fixtures/synthetic_model_sheet.csv` (합성 시트 3행 — mock 모델 3종)
- Create: `assets/model-catalog/fixtures/synthetic_endpoints_metrics.yaml` (설계 §4.3 endpoints-metrics 형식, 서비스 2)
- Create: `assets/model-catalog/tests/test_sheet_alias_tool.py` (RED → GREEN)
- Create: `assets/model-catalog/sheet_to_dim_token_model_alias_insert.py`

**Interfaces:**
- CLI(설계 §7.2 그대로): `python3 assets/model-catalog/sheet_to_dim_token_model_alias_insert.py --csv <시트 CSV> --effective-from YYYY-MM-DD --services <endpoints*.yaml> [--services …] [--out dim_token_model_alias_insert.sql] [--chunk-size 500] [--target-db gpu_data|token_verify_dim]`. exit 0 성공 / 1 검증 실패(`SheetError`) / 2 인자 오류.
- CSV 헤더: `canonical,aliases,defining_service,effective_from,note` (`canonical`,`aliases` 필수 헤더). `aliases`는 쉼표 구분(strip만 — 자동 교정 없음), 빈 값 = canonical-only(identity 행만). 빈 `effective_from` → `--effective-from`(둘 다 없으면 오류). **`2026-01-01` 금지**(사내 시드 플레이스홀더 키 — NOT IN 가드 충돌).
- 순수 함수: `load_services(paths) -> set[str]`(stdlib 줄 정규식 `service:` — PyYAML 없음, 0건이면 `SheetError`), `parse_sheet(rows, default_effective_from|None, services) -> list[AliasRow]`(출력 순서: 입력 행마다 identity 행 → alias 행들; identity 행은 `defining_service=''`), `render_sql(rows, chunk_size, source_name, default_effective_from, target_db="gpu_data") -> str`(결정적).
- 파일 내 검증(순서): `empty_canonical` → `empty_alias_segment` → effective_from 형식·`effective_from_is_placeholder_date` → `service_not_in_registry`(alias 행은 defining_service 필수 + `--services` 집합에 존재) → 전역 `alias_loop` → `alias_maps_to_two_canonicals` → `dup_key`. `missing_identity_row`는 구조적으로 불가(항상 identity 생성) — SQL 검증만.
- 출력 SQL: NOT IN 가드 INSERT(chunk) + `SETTINGS insert_distributed_sync = 1;` + 앵커 `-- 검증: 결과가 비어야 정상` + 검증 6종(`dup_key`, `alias_maps_to_two_canonicals`, `alias_loop`, `empty_canonical`, `missing_identity_row`, `service_not_in_registry` — 마지막은 `<target_db>.dim_token_metrics_service_dist` 대조). 4열 `check_name, key, effective_from, cnt`, 분산 서브쿼리는 `GLOBAL IN`/`GLOBAL NOT IN`(T6 시드와 동일).
- 상수: `PLACEHOLDER_EFFECTIVE_FROM = "2026-01-01"`, `SOURCE_SHEET = "metadata-sheet"`, `DEFAULT_OUT_FILENAME = "dim_token_model_alias_insert.sql"`(T1 gitignore 커버), `CHECK_NAMES` 튜플.

- [ ] **Step 1: 합성 fixture 2파일**

`assets/model-catalog/fixtures/synthetic_model_sheet.csv`:
```csv
canonical,aliases,defining_service,effective_from,note
claude-sonnet-5,"claude-sonnet-5-20260101, sonnet-5",Mock Service A,2026-08-26,합성 — 날짜 접미 alias 2종
claude-opus-4-8,opus-4.8,Mock Service B,2026-08-26,합성 — 점 표기 alias
claude-haiku-4-5,,,,합성 — canonical-only (identity만; effective_from은 --effective-from)
```

`assets/model-catalog/fixtures/synthetic_endpoints_metrics.yaml`:
```yaml
# 합성 — sheet_to_dim_token_model_alias_insert.py --services 테스트 입력 (설계 2026-08-31 §4.3 endpoints-metrics 형식)
# 실제 사내 파일은 collectors/token-metrics/endpoints-metrics.company.yaml (gitignore)
services:
  - serviceGroup: "Mock Group"          # 토큰 레지스트리와 바이트 동일
    service: "Mock Service A"
    baseUrl: "http://token-mock-provider-a.monitoring.svc:8000"
    enabled: true
    apiSince: "2026-09-09"
    coverageSince: "2026-08-26"
    until: null
    expectGpu: true
    expectServing: true
    usageIncludesConsumers: false
  - serviceGroup: "Mock Group"
    service: 'Mock Service B'           # 작은따옴표도 허용
    baseUrl: "http://token-mock-provider-b.monitoring.svc:8000"
    enabled: true
    apiSince: "2026-09-09"
    coverageSince: "2026-08-26"
    until: null
    expectGpu: true
    expectServing: false
    usageIncludesConsumers: false
```

Run:
```bash
cd /home/mini/github/token-data-pipeline
python3 -c "
import csv; rows = list(csv.DictReader(open('assets/model-catalog/fixtures/synthetic_model_sheet.csv', encoding='utf-8')))
assert [r['canonical'] for r in rows] == ['claude-sonnet-5', 'claude-opus-4-8', 'claude-haiku-4-5']; print('csv OK', len(rows))"
grep -c "^    service: " assets/model-catalog/fixtures/synthetic_endpoints_metrics.yaml
git check-ignore -v assets/model-catalog/fixtures/synthetic_model_sheet.csv assets/model-catalog/fixtures/synthetic_endpoints_metrics.yaml; echo "check-ignore exit=$? (1 = 무시 안 됨 = 정상)"
```
Expected:
```text
csv OK 3
2
check-ignore exit=1 (1 = 무시 안 됨 = 정상)
```

- [ ] **Step 2: 실패하는 테스트 작성 (RED)**

`assets/model-catalog/tests/test_sheet_alias_tool.py`:
```python
"""Tests for sheet_to_dim_token_model_alias_insert.py — 순수 로직(parse_sheet/render_sql/load_services) + CLI.

TDD (Plan 6a T8): 이 파일을 먼저 작성 → FAIL 확인 → 구현 → 통과.
검증 실패 경로도 데이터 행(모델명·서비스명 원문)을 절대 에코하지 않는다 — 행 번호·필드명·검증명만.
"""
import subprocess
import sys
from pathlib import Path

import pytest

from sheet_to_dim_token_model_alias_insert import (
    CHECK_NAMES,
    PLACEHOLDER_EFFECTIVE_FROM,
    AliasRow,
    SheetError,
    load_services,
    parse_sheet,
    render_sql,
)

MODULE_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = MODULE_ROOT / "sheet_to_dim_token_model_alias_insert.py"
FIXTURE_CSV = MODULE_ROOT / "fixtures" / "synthetic_model_sheet.csv"
FIXTURE_YAML = MODULE_ROOT / "fixtures" / "synthetic_endpoints_metrics.yaml"

SERVICES = {"Mock Service A", "Mock Service B"}
EF = "2026-08-26"


def _row(**overrides):
    base = {
        "canonical": "claude-sonnet-5",
        "aliases": "claude-sonnet-5-20260101",
        "defining_service": "Mock Service A",
        "effective_from": EF,
        "note": "합성",
    }
    base.update(overrides)
    return base


def _keys(rows):
    return [(r.alias, r.effective_from, r.canonical, r.defining_service, r.source) for r in rows]


# ---------------------------------------------------------------- parse_sheet

def test_identity_row_first_then_alias_rows():
    rows = parse_sheet([_row(aliases="claude-sonnet-5-20260101, sonnet-5")], None, SERVICES)
    assert _keys(rows) == [
        ("claude-sonnet-5", EF, "claude-sonnet-5", "", "metadata-sheet"),
        ("claude-sonnet-5-20260101", EF, "claude-sonnet-5", "Mock Service A", "metadata-sheet"),
        ("sonnet-5", EF, "claude-sonnet-5", "Mock Service A", "metadata-sheet"),
    ]
    assert all(isinstance(r, AliasRow) for r in rows)


def test_canonical_only_row_emits_identity_with_empty_service():
    rows = parse_sheet([_row(aliases="", defining_service="")], None, SERVICES)
    assert _keys(rows) == [("claude-sonnet-5", EF, "claude-sonnet-5", "", "metadata-sheet")]


def test_no_auto_correction_only_strip():
    rows = parse_sheet([_row(aliases="  Claude-Sonnet-5.v2 ")], None, SERVICES)
    assert rows[1].alias == "Claude-Sonnet-5.v2"


@pytest.mark.parametrize("bad", ["a,,b", "a,", ",a", " , a"])
def test_empty_alias_segment_rejected(bad):
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(aliases=bad)], None, SERVICES)
    assert "empty_alias_segment" in str(excinfo.value)


def test_empty_canonical_rejected():
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(canonical="  ")], None, SERVICES)
    assert "empty_canonical" in str(excinfo.value)


def test_alias_maps_to_two_canonicals_rejected():
    rows_in = [
        _row(canonical="claude-sonnet-5", aliases="sonnet"),
        _row(canonical="claude-opus-4-8", aliases="sonnet", defining_service="Mock Service B"),
    ]
    with pytest.raises(SheetError) as excinfo:
        parse_sheet(rows_in, None, SERVICES)
    msg = str(excinfo.value)
    assert "alias_maps_to_two_canonicals" in msg
    assert "2번째" in msg and "1번째" in msg
    assert "sonnet" not in msg


def test_remap_across_effective_from_allowed():
    rows_in = [
        _row(canonical="claude-sonnet-5", aliases="sonnet", effective_from="2026-08-26"),
        _row(canonical="claude-opus-4-8", aliases="sonnet", defining_service="Mock Service B",
             effective_from="2026-09-01"),
    ]
    rows = parse_sheet(rows_in, None, SERVICES)
    assert len(rows) == 4


def test_alias_loop_rejected():
    rows_in = [
        _row(canonical="claude-sonnet-5", aliases="sonnet-5"),
        _row(canonical="sonnet-5", aliases="sonnet-5-legacy", defining_service="Mock Service B"),
    ]
    with pytest.raises(SheetError) as excinfo:
        parse_sheet(rows_in, None, SERVICES)
    msg = str(excinfo.value)
    assert "alias_loop" in msg
    assert "sonnet" not in msg


def test_dup_key_identity_rejected():
    rows_in = [_row(aliases=""), _row(aliases="")]
    with pytest.raises(SheetError) as excinfo:
        parse_sheet(rows_in, None, SERVICES)
    assert "dup_key" in str(excinfo.value)


def test_dup_key_alias_equals_own_canonical_rejected():
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(aliases="claude-sonnet-5, sonnet-5")], None, SERVICES)
    assert "dup_key" in str(excinfo.value)


def test_service_not_in_registry_rejected():
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(defining_service="Mock Service Z")], None, SERVICES)
    msg = str(excinfo.value)
    assert "service_not_in_registry" in msg
    assert "Mock Service Z" not in msg


def test_alias_row_without_service_rejected():
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(defining_service="")], None, SERVICES)
    assert "service_not_in_registry" in str(excinfo.value)


def test_blank_effective_from_uses_default():
    rows = parse_sheet([_row(effective_from="")], "2026-09-01", SERVICES)
    assert {r.effective_from for r in rows} == {"2026-09-01"}


def test_blank_effective_from_without_default_rejected():
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(effective_from="")], None, SERVICES)
    assert "--effective-from" in str(excinfo.value)


def test_bad_date_rejected():
    with pytest.raises(SheetError):
        parse_sheet([_row(effective_from="2026/08/26")], None, SERVICES)


@pytest.mark.parametrize("kw", [{"effective_from": PLACEHOLDER_EFFECTIVE_FROM}, {"effective_from": ""}])
def test_placeholder_effective_from_rejected(kw):
    default = PLACEHOLDER_EFFECTIVE_FROM if kw["effective_from"] == "" else None
    with pytest.raises(SheetError) as excinfo:
        parse_sheet([_row(**kw)], default, SERVICES)
    assert "effective_from_is_placeholder_date" in str(excinfo.value)


# ---------------------------------------------------------------- load_services

def test_load_services_from_fixture_yaml():
    assert load_services([FIXTURE_YAML]) == SERVICES


def test_load_services_multiple_files_union(tmp_path):
    other = tmp_path / "endpoints-extra.yaml"
    other.write_text("services:\n  - service: \"Mock Service C\"   # 주석\n    enabled: true\n", encoding="utf-8")
    assert load_services([FIXTURE_YAML, other]) == SERVICES | {"Mock Service C"}


def test_load_services_ignores_comments_and_rejects_empty(tmp_path):
    empty = tmp_path / "endpoints-empty.yaml"
    empty.write_text("# service: \"Commented Out\"\nservices: []\n", encoding="utf-8")
    with pytest.raises(SheetError):
        load_services([empty])


# ---------------------------------------------------------------- render_sql

def test_render_sql_three_elements_and_six_checks():
    rows = parse_sheet([_row()], None, SERVICES)
    sql = render_sql(rows, 500, "synthetic_model_sheet.csv", None)
    assert "INSERT INTO gpu_data.dim_token_model_alias_dist" in sql
    assert "(alias, effective_from, canonical, defining_service, source, note)" in sql
    assert "WHERE (alias, effective_from) NOT IN (" in sql
    assert "SETTINGS insert_distributed_sync = 1;" in sql
    assert "-- 검증: 결과가 비어야 정상" in sql
    assert "synthetic_model_sheet.csv" in sql
    tail = sql.split("-- 검증: 결과가 비어야 정상", 1)[1]
    positions = [tail.index(f"'{name}'") for name in CHECK_NAMES]
    assert positions == sorted(positions), "검증 6종 순서 = CHECK_NAMES (service_not_in_registry 마지막)"
    assert "SELECT service FROM gpu_data.dim_token_metrics_service_dist" in tail
    assert "GLOBAL IN" in tail and "GLOBAL NOT IN" in tail
    assert tail.count("UNION ALL") == 5


def test_render_sql_deterministic():
    rows = parse_sheet([_row(), _row(canonical="claude-opus-4-8", aliases="opus-4.8",
                                     defining_service="Mock Service B")], None, SERVICES)
    assert render_sql(rows, 500, "s.csv", None) == render_sql(rows, 500, "s.csv", None)


def test_render_sql_quote_and_backslash_escape():
    rows = parse_sheet([_row(aliases="o'brien\\v2")], None, SERVICES)
    sql = render_sql(rows, 500, "s.csv", None)
    assert "o\\'brien\\\\v2" in sql


def test_render_sql_chunking():
    rows_in = [_row(canonical=f"model-{i}", aliases=f"m{i}-a, m{i}-b") for i in range(5)]
    rows = parse_sheet(rows_in, None, SERVICES)
    assert len(rows) == 15
    sql = render_sql(rows, 4, "s.csv", None)
    assert sql.count("INSERT INTO gpu_data.dim_token_model_alias_dist") == 4
    assert sql.count("'dup_key' AS check_name") == 1


def test_render_sql_target_db_override():
    rows = parse_sheet([_row()], None, SERVICES)
    sql = render_sql(rows, 500, "s.csv", None, "token_verify_dim")
    assert "token_verify_dim.dim_token_model_alias_dist" in sql
    assert "token_verify_dim.dim_token_metrics_service_dist" in sql
    assert "gpu_data." not in sql


# ---------------------------------------------------------------- CLI

def _run(*extra, csv_path=FIXTURE_CSV, out_path):
    return subprocess.run(
        [sys.executable, str(TOOL_PATH), "--csv", str(csv_path), "--out", str(out_path),
         "--services", str(FIXTURE_YAML), *extra],
        capture_output=True, text=True,
    )


def test_cli_roundtrip_fixture(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run("--effective-from", EF, out_path=out_path)
    assert result.returncode == 0, result.stderr
    body = out_path.read_text(encoding="utf-8")
    assert body.count("INSERT INTO gpu_data.dim_token_model_alias_dist") == 1
    # 3 canonical(identity 3) + alias 3 = 6행
    assert body.count(" AS alias, ") == 6
    assert "'claude-haiku-4-5' AS alias, toDate('2026-08-26') AS effective_from, 'claude-haiku-4-5' AS canonical, '' AS defining_service" in body
    assert "출력 행수: 6 (identity 3, alias 3)" in result.stdout
    assert "레지스트리 서비스 수: 2" in result.stdout


def test_cli_target_db_option(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run("--effective-from", EF, "--target-db", "token_verify_dim", out_path=out_path)
    assert result.returncode == 0, result.stderr
    body = out_path.read_text(encoding="utf-8")
    assert "INSERT INTO token_verify_dim.dim_token_model_alias_dist" in body
    assert "gpu_data." not in body


def test_cli_target_db_invalid_is_usage_error(tmp_path):
    result = _run("--effective-from", EF, "--target-db", "mart", out_path=tmp_path / "o.sql")
    assert result.returncode == 2


def test_cli_missing_default_effective_from_fails_on_blank_row(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run(out_path=out_path)
    assert result.returncode == 1
    assert "--effective-from" in result.stderr
    assert not out_path.exists()


def test_cli_stdout_summary_only(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run("--effective-from", EF, out_path=out_path)
    assert result.returncode == 0
    assert "INSERT INTO" not in result.stdout
    assert "claude-" not in result.stdout
    assert "Mock Service" not in result.stdout
    assert result.stderr == ""


def test_cli_error_output_has_no_data_rows(tmp_path):
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(
        "canonical,aliases,defining_service,effective_from,note\n"
        "secret-model-name,secret-alias,Mock Service Q,2026-08-26,\n",
        encoding="utf-8",
    )
    out_path = tmp_path / "out.sql"
    result = _run(csv_path=bad_csv, out_path=out_path)
    assert result.returncode == 1
    assert "service_not_in_registry" in result.stderr
    for secret in ("secret-model-name", "secret-alias", "Mock Service Q"):
        assert secret not in result.stderr and secret not in result.stdout
    assert not out_path.exists()


def test_cli_missing_required_header(tmp_path):
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text("model,alias\nx,y\n", encoding="utf-8")
    result = _run(csv_path=bad_csv, out_path=tmp_path / "o.sql")
    assert result.returncode == 1
    assert "필수 컬럼 없음" in result.stderr
```

Run:
```bash
cd /home/mini/github/token-data-pipeline/assets/model-catalog && python3 -m pytest -q tests/test_sheet_alias_tool.py 2>&1 | tail -4
```
Expected (수집 단계 ImportError — 모듈 부재):
```text
=========================== short test summary info ============================
ERROR tests/test_sheet_alias_tool.py
!!!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
1 error in 0.3s
```

- [ ] **Step 3: 생성기 구현**

`assets/model-catalog/sheet_to_dim_token_model_alias_insert.py`:
```python
#!/usr/bin/env python3
"""메타데이터 시트 `모델` 탭 CSV → gpu_data.dim_token_model_alias_dist INSERT SQL 생성기 (Plan 6a T8).

`assets/user-org/csv_to_dim_user_org_insert.py`(로스터 생성기) 클론 — stdlib만 사용(csv/argparse/
dataclasses/re). Python 3.10+. PyYAML 의존 없음(--services는 줄 정규식으로 `service:` 키만 읽는다).

CSV 계약(설계 2026-08-31 §4.2·§7.2): 헤더 `canonical,aliases,defining_service,effective_from,note`.
  - canonical: 필수. 정규화 대상 모델명(≤128). 빈 값 금지(empty_canonical).
  - aliases: 쉼표 구분(공백은 strip만 — 자동 교정 없음, 대소문자·하이픈 보존). 빈 값 = canonical-only
    행(identity 행만 생성, defining_service=''). 빈 세그먼트(`a,,b`, `a,`, `,a`) 금지.
  - defining_service: alias 행에 필수 — 레지스트리(endpoints*.yaml의 `service:`)와 바이트 동일해야 함
    (service_not_in_registry). identity 행은 항상 ''.
  - effective_from: 선택(YYYY-MM-DD) — 빈 값이면 --effective-from(둘 다 없으면 오류).
    2026-01-01(사내 시드 플레이스홀더 키)은 금지 — NOT IN 가드가 플레이스홀더 행과 충돌해 무음 skip 됨.
  - note: 선택.

출력: 멱등(NOT IN 가드) INSERT SQL + 말미 `-- 검증: 결과가 비어야 정상` 앵커 뒤 검증 SELECT 6종
  (dup_key, alias_maps_to_two_canonicals, alias_loop, empty_canonical, missing_identity_row,
   service_not_in_registry — 마지막 1종은 gpu_data.dim_token_metrics_service_dist 대조).
파일 내 검증(SQL 이전): empty_canonical, empty_alias_segment, effective_from 형식·플레이스홀더,
  service_not_in_registry, alias_loop, alias_maps_to_two_canonicals, dup_key.
  missing_identity_row는 구조적으로 불가(모든 canonical에 identity 행을 항상 생성) — SQL 검증만.

데이터 경계(§7.2): 실시트 CSV·생성 SQL은 레포 반입 금지 — .gitignore가 `*metadata*.csv`,
  `dim_token_model_alias_insert*.sql` 패턴으로 선제 차단. stdout은 요약(건수)만, 데이터 원문
  (모델명·서비스명)은 성공/실패 경로 모두에서 에코하지 않는다(행 번호·필드명·건수만).

exit code: 0 성공 / 1 검증 실패(SheetError) / 2 인자·입력 오류(argparse).
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

DEFAULT_OUT_FILENAME = "dim_token_model_alias_insert.sql"
DEFAULT_CHUNK_SIZE = 500

TABLE_NAME = "dim_token_model_alias_dist"
REGISTRY_TABLE_NAME = "dim_token_metrics_service_dist"
DEFAULT_TARGET_DB = "gpu_data"
TARGET_DB_CHOICES = ("gpu_data", "token_verify_dim")

# 사내 시드(seed_dim_token_model_alias.sql)의 플레이스홀더 키 날짜 — 생성 SQL은 이 날짜를 쓰면
# unknown 행과 NOT IN 가드 충돌 소지가 있어 금지(설계 §4.2 effective_from 규약: 소급 시작일).
PLACEHOLDER_EFFECTIVE_FROM = "2026-01-01"

SOURCE_SHEET = "metadata-sheet"
REQUIRED_HEADERS = ("canonical", "aliases")
CHECK_NAMES = (
    "dup_key",
    "alias_maps_to_two_canonicals",
    "alias_loop",
    "empty_canonical",
    "missing_identity_row",
    "service_not_in_registry",
)

_DATE_FMT = "%Y-%m-%d"
_SERVICE_LINE_RE = re.compile(r"^\s*-?\s*service\s*:\s*(.+?)\s*$")


class SheetError(Exception):
    """CSV 검증 실패. 메시지는 행 번호·필드명·검증명만 포함 — 데이터 값 에코 금지."""


@dataclass
class AliasRow:
    alias: str
    effective_from: str
    canonical: str
    defining_service: str
    source: str
    note: str
    row_no: int  # 원본 CSV 데이터 행 번호(1부터) — 오류 메시지용, SQL에는 미기록


def _parse_date(value: str, field_label: str, row_no: int) -> str:
    try:
        dt = datetime.strptime(value, _DATE_FMT)
    except ValueError as exc:
        where = f"{row_no}번째 데이터 행: {field_label}" if row_no > 0 else field_label
        raise SheetError(f"{where} 날짜 형식 오류 (YYYY-MM-DD 필요)") from exc
    return dt.strftime(_DATE_FMT)


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def load_services(paths) -> set:
    """endpoints*.yaml 여러 파일에서 `service:` 값 집합을 읽는다 (stdlib 줄 정규식 — PyYAML 없음).

    주석 줄(`#`)은 무시, 값의 따옴표는 벗긴다. 결과가 비면 SheetError(레지스트리 없이 검증 불가).
    """
    services: set = set()
    for p in paths:
        text = Path(p).read_text(encoding="utf-8")
        for line in text.splitlines():
            if line.lstrip().startswith("#"):
                continue
            m = _SERVICE_LINE_RE.match(line.split(" #", 1)[0])
            if m:
                value = _strip_quotes(m.group(1).strip())
                if value:
                    services.add(value)
    if not services:
        raise SheetError("--services: service 항목이 하나도 없음 (endpoints*.yaml 확인)")
    return services


def _split_aliases(raw: str, row_no: int) -> list:
    raw = raw.strip()
    if raw == "":
        return []
    segments = [seg.strip() for seg in raw.split(",")]
    if any(seg == "" for seg in segments):
        raise SheetError(
            f"{row_no}번째 데이터 행: aliases 필드에 빈 세그먼트가 있음 "
            "(선행/후행 ',' 또는 연속 ',,' 금지 — empty_alias_segment)"
        )
    return segments


def parse_sheet(rows, default_effective_from, services) -> list:
    """CSV DictReader 행 목록 → 검증된 AliasRow 목록 (순수 함수, 결정적 순서).

    출력 순서: 입력 행 순서대로 [identity 행, alias 행들...]. 자동 교정 없음(strip만).
    """
    if default_effective_from is not None:
        default_effective_from = _parse_date(default_effective_from, "--effective-from", 0)
        if default_effective_from == PLACEHOLDER_EFFECTIVE_FROM:
            raise SheetError(
                f"--effective-from: {PLACEHOLDER_EFFECTIVE_FROM}은 사내 시드 플레이스홀더 키 날짜 — "
                "금지 (effective_from_is_placeholder_date)"
            )
    services = set(services)

    records: list = []  # (row_no, canonical, aliases, defining_service, effective_from, note)
    for idx, raw in enumerate(rows, start=1):
        canonical = (raw.get("canonical") or "").strip()
        if not canonical:
            raise SheetError(f"{idx}번째 데이터 행: canonical 필드가 비어 있음 (empty_canonical)")

        aliases = _split_aliases(raw.get("aliases") or "", idx)

        effective_from_raw = (raw.get("effective_from") or "").strip()
        if effective_from_raw:
            effective_from = _parse_date(effective_from_raw, "effective_from", idx)
        elif default_effective_from is not None:
            effective_from = default_effective_from
        else:
            raise SheetError(
                f"{idx}번째 데이터 행: effective_from 비어 있음 + --effective-from 미지정"
            )
        if effective_from == PLACEHOLDER_EFFECTIVE_FROM:
            raise SheetError(
                f"{idx}번째 데이터 행: effective_from이 사내 시드 플레이스홀더 키 날짜"
                f"({PLACEHOLDER_EFFECTIVE_FROM}) — 금지 (effective_from_is_placeholder_date)"
            )

        defining_service = (raw.get("defining_service") or "").strip()
        if aliases:
            if not defining_service:
                raise SheetError(
                    f"{idx}번째 데이터 행: alias 행인데 defining_service 비어 있음 "
                    "(service_not_in_registry)"
                )
            if defining_service not in services:
                raise SheetError(
                    f"{idx}번째 데이터 행: defining_service가 --services 레지스트리에 없음 "
                    "(service_not_in_registry)"
                )

        note = (raw.get("note") or "").strip()
        records.append((idx, canonical, aliases, defining_service, effective_from, note))

    # 전역 검증 1) alias_loop: 어떤 행의 canonical이 다른 행의 alias(비-identity)로 등장 — 체인 금지
    alias_owner: dict = {}
    for row_no, _canonical, aliases, _svc, _ef, _note in records:
        for a in aliases:
            alias_owner.setdefault(a, row_no)
    for row_no, canonical, _aliases, _svc, _ef, _note in records:
        # 같은 행의 aliases에 자기 canonical이 있는 경우는 identity 행과의 dup_key로 잡는다(아래 3)
        if canonical in alias_owner and alias_owner[canonical] != row_no:
            raise SheetError(
                f"{row_no}번째 데이터 행: canonical이 {alias_owner[canonical]}번째 데이터 행의 "
                "alias로도 등장 — 1단계 매핑만 허용 (alias_loop)"
            )

    # 전역 검증 2) alias_maps_to_two_canonicals / 3) dup_key — (alias, effective_from) 키 기준
    out: list = []
    key_to: dict = {}  # (alias, effective_from) -> (canonical, row_no)
    for row_no, canonical, aliases, defining_service, effective_from, note in records:
        emitted = [(canonical, "")] + [(a, defining_service) for a in aliases]
        for alias, svc in emitted:
            key = (alias, effective_from)
            if key in key_to:
                prev_canonical, prev_row = key_to[key]
                if prev_canonical != canonical:
                    raise SheetError(
                        f"{row_no}번째 데이터 행: 같은 (alias, effective_from)이 {prev_row}번째 "
                        "데이터 행과 다른 canonical로 매핑됨 (alias_maps_to_two_canonicals)"
                    )
                raise SheetError(
                    f"{row_no}번째 데이터 행: (alias, effective_from) 키 중복 "
                    f"(최초 발생: {prev_row}번째 데이터 행) (dup_key)"
                )
            key_to[key] = (canonical, row_no)
            out.append(
                AliasRow(
                    alias=alias,
                    effective_from=effective_from,
                    canonical=canonical,
                    defining_service=svc,
                    source=SOURCE_SHEET,
                    note=note,
                    row_no=row_no,
                )
            )
    return out


def _escape_sql_string(value: str) -> str:
    """이스케이프 순서: '\\' -> '\\\\' 먼저, 그다음 "'" -> "\\'" (순서 바뀌면 SQL 파손)."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _full_row_literal(row: AliasRow) -> str:
    return (
        f"SELECT '{_escape_sql_string(row.alias)}' AS alias, "
        f"toDate('{row.effective_from}') AS effective_from, "
        f"'{_escape_sql_string(row.canonical)}' AS canonical, "
        f"'{_escape_sql_string(row.defining_service)}' AS defining_service, "
        f"'{_escape_sql_string(row.source)}' AS source, "
        f"'{_escape_sql_string(row.note)}' AS note"
    )


def _chunks(rows: list, chunk_size: int):
    for i in range(0, len(rows), chunk_size):
        yield rows[i : i + chunk_size]


def _union_all(literals) -> str:
    return "\n    UNION ALL\n    ".join(literals)


def render_sql(rows, chunk_size: int, source_name: str, default_effective_from,
               target_db: str = DEFAULT_TARGET_DB) -> str:
    """검증된 AliasRow 목록 → 결정적(byte-identical) INSERT SQL 문자열 (순수 함수).

    (a) 헤더 주석(basename·행수·기본 effective_from — 생성 시각 없음)
    (b) chunk당 NOT IN 멱등 가드 INSERT + SETTINGS insert_distributed_sync = 1
    (c) '-- 검증: 결과가 비어야 정상' 앵커 뒤 검증 SELECT 6종 (전역, 시드 파일과 동일 4열
        check_name, key, effective_from, cnt) — service_not_in_registry가 마지막(레지스트리 대조).
    """
    target_table = f"{target_db}.{TABLE_NAME}"
    registry_table = f"{target_db}.{REGISTRY_TABLE_NAME}"
    chunks = list(_chunks(rows, chunk_size))
    identity_count = sum(1 for r in rows if r.alias == r.canonical)

    lines = []
    lines.append("-- =============================================================")
    lines.append(f"-- {target_table} 메타데이터 시트 `모델` 탭 INSERT")
    lines.append("-- 생성: sheet_to_dim_token_model_alias_insert.py (Plan 6a T8)")
    lines.append(f"-- 소스 파일: {source_name}")
    lines.append(f"-- 행수: {len(rows)} (identity {identity_count}, alias {len(rows) - identity_count})")
    lines.append(f"-- 기본 effective_from: {default_effective_from or '(행별 값만)'}")
    lines.append("-- 경고: 실시트 산출물(이 파일)은 레포·사외 환경 반입 금지 (§7.2, .gitignore 커버)")
    lines.append("-- 실행 주체: admin 수동 — 사내 절차 리뷰 후 실행. 재매핑은 새 effective_from 행 append (기존 행 불변)")
    lines.append("-- =============================================================")
    lines.append("")

    for chunk in chunks:
        lines.append(f"INSERT INTO {target_table}")
        lines.append("    (alias, effective_from, canonical, defining_service, source, note)")
        lines.append("SELECT *")
        lines.append("FROM (")
        lines.append("    " + _union_all(_full_row_literal(r) for r in chunk))
        lines.append(")")
        lines.append("WHERE (alias, effective_from) NOT IN (")
        lines.append(f"    SELECT alias, effective_from FROM {target_table}")
        lines.append(")")
        lines.append("SETTINGS insert_distributed_sync = 1;")
        lines.append("")

    lines.append("-- 검증: 결과가 비어야 정상 ------------------------------------------------")
    lines.append("-- 1) dup_key: (alias, effective_from) 전역 중복 없음")
    lines.append("SELECT 'dup_key' AS check_name, alias AS key, effective_from, count() AS cnt")
    lines.append(f"FROM {target_table}")
    lines.append("GROUP BY alias, effective_from")
    lines.append("HAVING count() > 1")
    lines.append("")
    lines.append("UNION ALL")
    lines.append("")
    lines.append("-- 2) alias_maps_to_two_canonicals: 같은 (alias, effective_from)이 서로 다른 canonical로")
    lines.append("SELECT 'alias_maps_to_two_canonicals', alias, effective_from, uniqExact(canonical)")
    lines.append(f"FROM {target_table}")
    lines.append("GROUP BY alias, effective_from")
    lines.append("HAVING uniqExact(canonical) > 1")
    lines.append("")
    lines.append("UNION ALL")
    lines.append("")
    lines.append("-- 3) alias_loop: 비-identity 행의 canonical이 다시 다른 비-identity 행의 alias (1단계 매핑만)")
    lines.append("SELECT 'alias_loop', alias, effective_from, toUInt64(1)")
    lines.append(f"FROM {target_table}")
    lines.append("WHERE alias != canonical")
    lines.append("  AND canonical GLOBAL IN (")
    lines.append(f"      SELECT alias FROM {target_table} WHERE alias != canonical")
    lines.append("  )")
    lines.append("")
    lines.append("UNION ALL")
    lines.append("")
    lines.append("-- 4) empty_canonical: canonical 빈 문자열 금지")
    lines.append("SELECT 'empty_canonical', alias, effective_from, toUInt64(1)")
    lines.append(f"FROM {target_table}")
    lines.append("WHERE canonical = ''")
    lines.append("")
    lines.append("UNION ALL")
    lines.append("")
    lines.append("-- 5) missing_identity_row: 모든 canonical은 identity 행(alias = canonical)을 가져야 함")
    lines.append("SELECT 'missing_identity_row', canonical, min(effective_from), count()")
    lines.append(f"FROM {target_table}")
    lines.append("WHERE canonical != ''")
    lines.append("  AND canonical GLOBAL NOT IN (")
    lines.append(f"      SELECT alias FROM {target_table} WHERE alias = canonical")
    lines.append("  )")
    lines.append("GROUP BY canonical")
    lines.append("")
    lines.append("UNION ALL")
    lines.append("")
    lines.append("-- 6) service_not_in_registry: alias 행의 defining_service가 메트릭 레지스트리에 없음")
    lines.append(f"--    (레지스트리 {registry_table}는 collectors/token-metrics 정기 실행이 동기화 — 설계 §4.3)")
    lines.append("SELECT 'service_not_in_registry', alias, effective_from, toUInt64(1)")
    lines.append(f"FROM {target_table}")
    lines.append("WHERE defining_service != ''")
    lines.append("  AND defining_service GLOBAL NOT IN (")
    lines.append(f"      SELECT service FROM {registry_table}")
    lines.append("  );")
    lines.append("")
    return "\n".join(lines) + "\n"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="메타데이터 시트 `모델` 탭 CSV -> gpu_data.dim_token_model_alias_dist INSERT SQL 생성기"
    )
    parser.add_argument("--csv", required=True, help="입력 CSV 경로 (헤더 canonical,aliases,defining_service,effective_from,note)")
    parser.add_argument(
        "--effective-from",
        default=None,
        help="행의 effective_from이 빈 경우의 기본값 (YYYY-MM-DD, 소급 시작일 — 2026-01-01 금지)",
    )
    parser.add_argument(
        "--services",
        action="append",
        required=True,
        help="endpoints*.yaml 경로 (반복 가능) — defining_service 대조용 레지스트리",
    )
    parser.add_argument("--out", default=DEFAULT_OUT_FILENAME, help=f"출력 SQL 경로 (기본: {DEFAULT_OUT_FILENAME})")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help=f"INSERT chunk 크기 (기본 {DEFAULT_CHUNK_SIZE})")
    parser.add_argument(
        "--target-db",
        default=DEFAULT_TARGET_DB,
        choices=TARGET_DB_CHOICES,
        help="INSERT 대상 dim DB명 (기본: gpu_data — company-verify는 token_verify_dim)",
    )
    return parser


def main(argv=None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.chunk_size <= 0:
        parser.error("--chunk-size는 1 이상이어야 함")

    csv_path = Path(args.csv)
    try:
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames or []
            raw_rows = list(reader)
    except OSError:
        parser.error(f"--csv 파일을 열 수 없음: {csv_path.name}")
        return 2  # pragma: no cover — parser.error already exits

    missing = [h for h in REQUIRED_HEADERS if h not in header]
    if missing:
        print(f"검증 실패: CSV 헤더에 필수 컬럼 없음: {', '.join(missing)}", file=sys.stderr)
        return 1

    try:
        services = load_services(args.services)
        rows = parse_sheet(raw_rows, args.effective_from, services)
    except OSError as exc:
        print(f"검증 실패: --services 파일을 열 수 없음: {Path(str(exc.filename or '')).name}", file=sys.stderr)
        return 1
    except SheetError as exc:
        print(f"검증 실패: {exc}", file=sys.stderr)
        return 1

    sql_text = render_sql(rows, args.chunk_size, csv_path.name, args.effective_from, args.target_db)
    out_path = Path(args.out)
    out_path.write_text(sql_text, encoding="utf-8")

    identity_count = sum(1 for r in rows if r.alias == r.canonical)
    num_chunks = (len(rows) + args.chunk_size - 1) // args.chunk_size if rows else 0
    print(f"생성 완료: {out_path.name}")   # 경로 미출력 — tmp 경로 문자열이 stdout 위생 검사('claude-' 등)에 걸리지 않게
    print(f"입력 데이터 행수: {len(raw_rows)} → 출력 행수: {len(rows)} (identity {identity_count}, alias {len(rows) - identity_count})")
    print(f"레지스트리 서비스 수: {len(services)} (--services {len(args.services)}개 파일)")
    print(f"chunk 크기: {args.chunk_size} (chunk 수: {num_chunks})")
    print(
        "검증: 출력 SQL 말미 \"-- 검증: 결과가 비어야 정상\" 섹션 실행 후 결과가 비어 있어야 정상 "
        "(admin 리뷰 절차; service_not_in_registry는 레지스트리 동기화 이후에만 유의미)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

Run:
```bash
cd /home/mini/github/token-data-pipeline/assets/model-catalog && python3 -m pytest -q tests/test_sheet_alias_tool.py 2>&1 | tail -1 && python3 -m pytest -q 2>&1 | tail -1
```
Expected:
```text
35 passed in 1.0s
80 passed in 1.6s
```

- [ ] **Step 4: CLI 스모크 — fixture 왕복 + 기본 `--out` 이름 gitignore 확인 + 데이터 원문 미에코**

Run:
```bash
cd /home/mini/github/token-data-pipeline
python3 assets/model-catalog/sheet_to_dim_token_model_alias_insert.py \
  --csv assets/model-catalog/fixtures/synthetic_model_sheet.csv \
  --services assets/model-catalog/fixtures/synthetic_endpoints_metrics.yaml \
  --effective-from 2026-08-26 --out dim_token_model_alias_insert.sql; echo "exit=$?"
git check-ignore -v dim_token_model_alias_insert.sql
grep -c " AS alias, " dim_token_model_alias_insert.sql
grep -o "^-- [0-9]) [a-z_]*" dim_token_model_alias_insert.sql | tr '\n' ' '; echo
git status --porcelain | grep -c "dim_token_model_alias_insert.sql"
rm -f dim_token_model_alias_insert.sql
```
Expected:
```text
생성 완료: dim_token_model_alias_insert.sql
입력 데이터 행수: 3 → 출력 행수: 6 (identity 3, alias 3)
레지스트리 서비스 수: 2 (--services 1개 파일)
chunk 크기: 500 (chunk 수: 1)
검증: 출력 SQL 말미 "-- 검증: 결과가 비어야 정상" 섹션 실행 후 결과가 비어 있어야 정상 (admin 리뷰 절차; service_not_in_registry는 레지스트리 동기화 이후에만 유의미)
exit=0
.gitignore:26:dim_token_model_alias_insert*.sql	dim_token_model_alias_insert.sql
6
-- 1) dup_key -- 2) alias_maps_to_two_canonicals -- 3) alias_loop -- 4) empty_canonical -- 5) missing_identity_row -- 6) service_not_in_registry 
0
```
(`26` = T1이 추가한 패턴의 `.gitignore` 줄 번호(원본 16행 + 빈 줄 + 주석 + 8번째 패턴). 마지막 `0` = 생성 SQL은 `git status`에 나타나지 않는다(§7.2 경계). 검증 6종 순서 1)~6), `service_not_in_registry`가 6).)

- [ ] **Step 5: 커밋**

```bash
cd /home/mini/github/token-data-pipeline
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml
# 기대: 출력 없음
git status --porcelain | grep -v "^?? assets/model-catalog/" ; echo "(위 출력 없음 = 신규 파일만)"
git add assets/model-catalog/sheet_to_dim_token_model_alias_insert.py assets/model-catalog/tests/test_sheet_alias_tool.py \
  assets/model-catalog/fixtures/synthetic_model_sheet.csv assets/model-catalog/fixtures/synthetic_endpoints_metrics.yaml
git commit -m "feat(assets): 메타데이터 시트 모델 탭 → dim_token_model_alias INSERT 생성기 + 테스트 35 + 합성 fixture 2 (Plan 6a T8)

- 로스터 생성기 클론(stdlib only): --csv --effective-from --services(반복) --out --chunk-size --target-db gpu_data|token_verify_dim
- 파일 내 검증 7종(empty_canonical, empty_alias_segment, 날짜·플레이스홀더 2026-01-01 금지, service_not_in_registry, alias_loop, alias_maps_to_two_canonicals, dup_key) — 자동 교정 없음(strip만)
- 출력 SQL: NOT IN 가드 + insert_distributed_sync + 검증 앵커 + §4.2 검증 6종(service_not_in_registry 마지막, 레지스트리 dim_token_metrics_service_dist 대조)
- canonical-only 행은 identity만(defining_service=''), source='metadata-sheet'; stdout/stderr에 모델·서비스 원문 미출력

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

---

### Task 9: Layer C 기준정보 CSV → `dim_token_{gpu_tco,gpu_allocation,vendor_price}` INSERT 생성기 — `assets/model-catalog/csv_to_layer_c_dim_insert.py` (+ 테스트·fixture)

**Files:**
- Create: `assets/model-catalog/fixtures/synthetic_layer_c_tco.csv` (합성 3행 — H100/A100 수치 + H200 NULL 플레이스홀더)
- Create: `assets/model-catalog/fixtures/synthetic_layer_c_allocation.csv` (합성 3행 — source 기본값·0 철회 행 포함)
- Create: `assets/model-catalog/fixtures/synthetic_layer_c_price.csv` (합성 2행 — tier 기본값·cached NULL 포함)
- Create: `assets/model-catalog/tests/test_layer_c_tool.py` (RED → GREEN)
- Create: `assets/model-catalog/csv_to_layer_c_dim_insert.py`

**Interfaces:**
- CLI(설계 §7.2 "Layer C 기준정보 CSV → INSERT 생성기" 1개, `--table`로 3테이블 분기): `python3 assets/model-catalog/csv_to_layer_c_dim_insert.py --table gpu_tco|gpu_allocation|vendor_price --csv <CSV> [--effective-from YYYY-MM-DD] [--out <경로>] [--chunk-size 500] [--target-db gpu_data|token_verify_dim]`. `--out` 기본값은 테이블별 `dim_token_gpu_tco_insert.sql` / `dim_token_gpu_allocation_insert.sql` / `dim_token_vendor_price_insert.sql`(T1 gitignore 패턴 `dim_token_gpu_*_insert*.sql`·`dim_token_vendor_price_insert*.sql` 커버). exit 0 성공 / 1 검증 실패(`LayerCError`) 또는 필수 헤더 누락 / 2 인자 오류(argparse — 잘못된 `--table`·`--target-db`).
- CSV 계약(헤더 = 설계 §4.2 컬럼명 그대로, 통화 KRW 고정이므로 `currency` 컬럼은 선택 — 있으면 ''/`KRW`만 허용):
  - `gpu_tco`: `gpu_type,effective_from,tco_krw_per_gpu_hour,basis,note` — 필수 헤더 `gpu_type`,`tco_krw_per_gpu_hour`. `basis` ∈ {'', depreciation, lease, power-inclusive, tco}.
  - `gpu_allocation`: `service_group,gpu_type,effective_from,allocated_gpu_count,source,note` — 필수 헤더 `service_group`,`gpu_type`,`allocated_gpu_count`. `source` 빈 값 → `manual`. 철회는 0 행 append(음수 금지).
  - `vendor_price`: `provider,model,tier,effective_from,krw_per_mtok_input,krw_per_mtok_cached,krw_per_mtok_cache_creation,krw_per_mtok_output,note` — 필수 헤더 `provider`,`model`,`krw_per_mtok_input`,`krw_per_mtok_output`. `tier` 빈 값 → `standard`(∈ standard|batch|flex|priority). `cached`/`cache_creation` 빈 값 → NULL.
  - 공통: 숫자 셀 빈 값 → NULL(플레이스홀더 — 비용 NULL 전파), 천 단위 쉼표 허용, `nan`/`inf`/음수 금지. 문자열은 strip만(자동 교정 없음). 키 값 `unknown`은 시드 예약어 — 금지. 빈 `effective_from` → `--effective-from`(둘 다 없으면 오류), **`2026-01-01` 금지**(시드 플레이스홀더 키 날짜 — NOT IN 가드 충돌). 키 튜플(`effective_from` 포함) 중복 금지.
- 순수 함수: `TABLE_SPECS[name] -> TableSpec`(frozen dataclass: `name, table, key_columns, string_columns, numeric_columns, trailing_columns, default_out, check_names`), `required_headers(spec)`, `insert_columns(spec)`(= DDL 컬럼 순서), `parse_rows(spec, rows, default_effective_from|None) -> list[DimRow]`, `render_sql(spec, rows, chunk_size, source_name, default_effective_from, target_db="gpu_data") -> str`(결정적).
- 파일 내 검증명(오류 메시지 괄호 안): `empty_key`, `unknown_reserved`, `bad_number`, `negative_value`, `currency_krw`, `basis_domain`, `tier_domain`, `effective_from_is_placeholder_date`, `dup_key`(최초 행 번호 포함). 메시지에 데이터 값 원문 없음.
- 출력 SQL: NOT IN 가드 INSERT(chunk, 숫자는 `CAST(x AS Nullable(Float64))`/`CAST(NULL AS Nullable(Float64))`, 날짜는 `toDate('…')`, 별칭은 chunk 첫 행만) + `SETTINGS insert_distributed_sync = 1;` + 앵커 `-- 검증: 결과가 비어야 정상` + **T6 시드와 동일한 검증 항목·순서**(gpu_tco: `dup_key`,`unknown_row_state`,`basis_domain`,`currency_krw` / gpu_allocation: `dup_key`,`unknown_row_state`,`negative_count` / vendor_price: `dup_key`,`unknown_row_state`,`tier_domain`), 4열 `check_name, key, effective_from, cnt`. `unknown_row_state`는 시드 적용 이후에만 비어 있으므로 stdout 안내문에 "시드 적용 이후에 실행"을 명시.
- 상수: `PLACEHOLDER_EFFECTIVE_FROM = "2026-01-01"`, `RESERVED_KEY_VALUE = "unknown"`, `CURRENCY = "KRW"`, `BASIS_DOMAIN`, `TIER_DOMAIN`, `TARGET_DB_CHOICES = ("gpu_data", "token_verify_dim")`.

- [ ] **Step 1: 합성 fixture 3파일**

`assets/model-catalog/fixtures/synthetic_layer_c_tco.csv`:
```csv
gpu_type,effective_from,tco_krw_per_gpu_hour,basis,note
H100,2026-08-26,4300,tco,합성 — 실제 TCO 아님 (KRW/GPU-h 임의값)
A100,2026-08-26,2100,tco,합성
H200,,,,합성 — 단가 미확정(NULL → 비용 NULL 전파 경로); effective_from은 --effective-from
```

`assets/model-catalog/fixtures/synthetic_layer_c_allocation.csv`:
```csv
service_group,gpu_type,effective_from,allocated_gpu_count,source,note
Mock Group,H100,2026-08-26,8,quota-sheet,합성 — 장수(count)
Mock Group,A100,2026-08-26,4,,합성 — source 빈 값 → manual
Mock Group,H100,2026-09-01,0,quota-sheet,합성 — 철회는 0 행 append
```

`assets/model-catalog/fixtures/synthetic_layer_c_price.csv`:
```csv
provider,model,tier,effective_from,krw_per_mtok_input,krw_per_mtok_cached,krw_per_mtok_cache_creation,krw_per_mtok_output,note
anthropic,claude-sonnet-5,,2026-08-26,4050,405,5062.5,20250,합성 — seed_dim_token_model USD 3/0.3/3.75/15 × 1350
anthropic,claude-haiku-4-5,batch,2026-08-26,675,,,3375,합성 — cached/cache_creation 미확정(NULL)
```

Run:
```bash
cd /home/mini/github/token-data-pipeline
python3 -c "
import csv
for name, n in (('tco', 3), ('allocation', 3), ('price', 2)):
    rows = list(csv.DictReader(open(f'assets/model-catalog/fixtures/synthetic_layer_c_{name}.csv', encoding='utf-8')))
    assert len(rows) == n, (name, len(rows))
print('csv OK 3/3/2')"
git check-ignore -v assets/model-catalog/fixtures/synthetic_layer_c_tco.csv assets/model-catalog/fixtures/synthetic_layer_c_allocation.csv assets/model-catalog/fixtures/synthetic_layer_c_price.csv; echo "check-ignore exit=$? (1 = 무시 안 됨 = 정상 — 파일명이 *gpu_tco*/*gpu_allocation*/*vendor_price* 패턴을 피함)"
```
Expected:
```text
csv OK 3/3/2
check-ignore exit=1 (1 = 무시 안 됨 = 정상 — 파일명이 *gpu_tco*/*gpu_allocation*/*vendor_price* 패턴을 피함)
```

- [ ] **Step 2: 실패하는 테스트 작성 (RED)**

`assets/model-catalog/tests/test_layer_c_tool.py`:
```python
"""Tests for csv_to_layer_c_dim_insert.py — 순수 로직(parse_rows/render_sql) + CLI, 3테이블 공통.

TDD (Plan 6a T9): 이 파일을 먼저 작성 → FAIL 확인 → 구현 → 통과.
검증 실패 경로도 데이터 행(기종·그룹·모델·단가 원문)을 절대 에코하지 않는다 — 행 번호·필드명·검증명만.
"""
import subprocess
import sys
from pathlib import Path

import pytest

from csv_to_layer_c_dim_insert import (
    PLACEHOLDER_EFFECTIVE_FROM,
    TABLE_SPECS,
    DimRow,
    LayerCError,
    insert_columns,
    parse_rows,
    render_sql,
    required_headers,
)

MODULE_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = MODULE_ROOT / "csv_to_layer_c_dim_insert.py"
FIXTURES = {
    "gpu_tco": MODULE_ROOT / "fixtures" / "synthetic_layer_c_tco.csv",
    "gpu_allocation": MODULE_ROOT / "fixtures" / "synthetic_layer_c_allocation.csv",
    "vendor_price": MODULE_ROOT / "fixtures" / "synthetic_layer_c_price.csv",
}
EF = "2026-08-26"

TCO = TABLE_SPECS["gpu_tco"]
ALLOC = TABLE_SPECS["gpu_allocation"]
PRICE = TABLE_SPECS["vendor_price"]


def _tco(**o):
    base = {"gpu_type": "H100", "effective_from": EF, "tco_krw_per_gpu_hour": "4300", "basis": "tco", "note": "합성"}
    base.update(o)
    return base


def _alloc(**o):
    base = {"service_group": "Mock Group", "gpu_type": "H100", "effective_from": EF,
            "allocated_gpu_count": "8", "source": "", "note": "합성"}
    base.update(o)
    return base


def _price(**o):
    base = {"provider": "anthropic", "model": "claude-sonnet-5", "tier": "", "effective_from": EF,
            "krw_per_mtok_input": "4050", "krw_per_mtok_cached": "405",
            "krw_per_mtok_cache_creation": "5062.5", "krw_per_mtok_output": "20250", "note": "합성"}
    base.update(o)
    return base


# ---------------------------------------------------------------- 스펙 계약

def test_table_specs_match_ddl_column_order():
    assert insert_columns(TCO) == ("gpu_type", "effective_from", "tco_krw_per_gpu_hour", "currency", "basis", "note")
    assert insert_columns(ALLOC) == ("service_group", "gpu_type", "effective_from", "allocated_gpu_count", "source", "note")
    assert insert_columns(PRICE) == (
        "provider", "model", "tier", "effective_from", "krw_per_mtok_input", "krw_per_mtok_cached",
        "krw_per_mtok_cache_creation", "krw_per_mtok_output", "note",
    )


def test_required_headers_and_default_out_names_are_gitignored_patterns():
    assert required_headers(TCO) == ("gpu_type", "tco_krw_per_gpu_hour")
    assert required_headers(ALLOC) == ("service_group", "gpu_type", "allocated_gpu_count")
    assert required_headers(PRICE) == ("provider", "model", "krw_per_mtok_input", "krw_per_mtok_output")
    assert TCO.default_out == "dim_token_gpu_tco_insert.sql"
    assert ALLOC.default_out == "dim_token_gpu_allocation_insert.sql"
    assert PRICE.default_out == "dim_token_vendor_price_insert.sql"


# ---------------------------------------------------------------- parse_rows (공통 규칙)

def test_tco_row_parsed_with_krw_and_float():
    rows = parse_rows(TCO, [_tco()], None)
    assert isinstance(rows[0], DimRow)
    assert rows[0].values == {"gpu_type": "H100", "effective_from": EF, "tco_krw_per_gpu_hour": 4300.0,
                              "currency": "KRW", "basis": "tco", "note": "합성"}


def test_alloc_defaults_source_manual_and_allows_zero():
    rows = parse_rows(ALLOC, [_alloc(allocated_gpu_count="0")], None)
    assert rows[0].values["source"] == "manual"
    assert rows[0].values["allocated_gpu_count"] == 0.0


def test_price_defaults_tier_standard_and_optional_null():
    rows = parse_rows(PRICE, [_price(krw_per_mtok_cached="", krw_per_mtok_cache_creation="")], None)
    assert rows[0].values["tier"] == "standard"
    assert rows[0].values["krw_per_mtok_cached"] is None
    assert rows[0].values["krw_per_mtok_cache_creation"] is None
    assert rows[0].values["krw_per_mtok_input"] == 4050.0


def test_empty_numeric_becomes_null_placeholder():
    rows = parse_rows(TCO, [_tco(tco_krw_per_gpu_hour="")], None)
    assert rows[0].values["tco_krw_per_gpu_hour"] is None


def test_no_auto_correction_only_strip():
    rows = parse_rows(TCO, [_tco(gpu_type="  h100-sxm ")], None)
    assert rows[0].values["gpu_type"] == "h100-sxm"


def test_thousands_separator_accepted():
    rows = parse_rows(PRICE, [_price(krw_per_mtok_output="20,250")], None)
    assert rows[0].values["krw_per_mtok_output"] == 20250.0


@pytest.mark.parametrize("spec,row", [(TCO, _tco(gpu_type=" ")), (ALLOC, _alloc(service_group="")),
                                      (PRICE, _price(model=""))])
def test_empty_key_rejected(spec, row):
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(spec, [row], None)
    assert "empty_key" in str(excinfo.value)


@pytest.mark.parametrize("spec,row", [(TCO, _tco(gpu_type="unknown")), (ALLOC, _alloc(gpu_type="unknown")),
                                      (PRICE, _price(provider="unknown"))])
def test_unknown_reserved_rejected(spec, row):
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(spec, [row], None)
    assert "unknown_reserved" in str(excinfo.value)


@pytest.mark.parametrize("bad", ["abc", "nan", "inf", "4300원"])
def test_bad_number_rejected(bad):
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(TCO, [_tco(tco_krw_per_gpu_hour=bad)], None)
    assert "bad_number" in str(excinfo.value)


@pytest.mark.parametrize("spec,row", [(TCO, _tco(tco_krw_per_gpu_hour="-1")), (ALLOC, _alloc(allocated_gpu_count="-8")),
                                      (PRICE, _price(krw_per_mtok_cached="-0.5"))])
def test_negative_value_rejected(spec, row):
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(spec, [row], None)
    assert "negative_value" in str(excinfo.value)


def test_basis_domain_rejected():
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(TCO, [_tco(basis="rental")], None)
    assert "basis_domain" in str(excinfo.value)
    assert "rental" not in str(excinfo.value)


def test_tier_domain_rejected():
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(PRICE, [_price(tier="premium")], None)
    assert "tier_domain" in str(excinfo.value)
    assert "premium" not in str(excinfo.value)


def test_currency_column_must_be_krw_if_present():
    rows = parse_rows(TCO, [_tco(currency="KRW")], None)
    assert rows[0].values["currency"] == "KRW"
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(TCO, [_tco(currency="USD")], None)
    assert "currency_krw" in str(excinfo.value)


def test_dup_key_rejected_with_row_numbers():
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(ALLOC, [_alloc(), _alloc(allocated_gpu_count="4")], None)
    msg = str(excinfo.value)
    assert "dup_key" in msg and "2번째" in msg and "1번째" in msg
    assert "Mock Group" not in msg


def test_same_key_different_effective_from_allowed():
    rows = parse_rows(ALLOC, [_alloc(), _alloc(effective_from="2026-09-01", allocated_gpu_count="0")], None)
    assert len(rows) == 2


def test_blank_effective_from_uses_default():
    rows = parse_rows(TCO, [_tco(effective_from="")], "2026-09-01")
    assert rows[0].values["effective_from"] == "2026-09-01"


def test_blank_effective_from_without_default_rejected():
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(TCO, [_tco(effective_from="")], None)
    assert "--effective-from" in str(excinfo.value)


def test_bad_date_rejected():
    with pytest.raises(LayerCError):
        parse_rows(TCO, [_tco(effective_from="26/08/2026")], None)


@pytest.mark.parametrize("kw", [{"effective_from": PLACEHOLDER_EFFECTIVE_FROM}, {"effective_from": ""}])
def test_placeholder_effective_from_rejected(kw):
    default = PLACEHOLDER_EFFECTIVE_FROM if kw["effective_from"] == "" else None
    with pytest.raises(LayerCError) as excinfo:
        parse_rows(TCO, [_tco(**kw)], default)
    assert "effective_from_is_placeholder_date" in str(excinfo.value)


# ---------------------------------------------------------------- render_sql

def test_render_sql_tco_three_elements_and_checks():
    rows = parse_rows(TCO, [_tco(), _tco(gpu_type="H200", tco_krw_per_gpu_hour="")], None)
    sql = render_sql(TCO, rows, 500, "synthetic_layer_c_tco.csv", None)
    assert "INSERT INTO gpu_data.dim_token_gpu_tco_dist" in sql
    assert "(gpu_type, effective_from, tco_krw_per_gpu_hour, currency, basis, note)" in sql
    assert "SELECT 'H100' AS gpu_type, toDate('2026-08-26') AS effective_from, CAST(4300.0 AS Nullable(Float64)) AS tco_krw_per_gpu_hour, 'KRW' AS currency, 'tco' AS basis, '합성' AS note" in sql
    assert "SELECT 'H200', toDate('2026-08-26'), CAST(NULL AS Nullable(Float64)), 'KRW', 'tco', '합성'" in sql
    assert "WHERE (gpu_type, effective_from) NOT IN (" in sql
    assert "SETTINGS insert_distributed_sync = 1;" in sql
    assert "-- 검증: 결과가 비어야 정상" in sql
    assert "synthetic_layer_c_tco.csv" in sql
    tail = sql.split("-- 검증: 결과가 비어야 정상", 1)[1]
    positions = [tail.index(f"'{name}'") for name in TCO.check_names]
    assert positions == sorted(positions)
    assert tail.count("UNION ALL") == 3


def test_render_sql_alloc_checks_and_concat_key():
    rows = parse_rows(ALLOC, [_alloc()], None)
    sql = render_sql(ALLOC, rows, 500, "a.csv", None)
    assert "WHERE (service_group, gpu_type, effective_from) NOT IN (" in sql
    tail = sql.split("-- 검증: 결과가 비어야 정상", 1)[1]
    assert "concat(service_group, '/', gpu_type) AS key" in tail
    assert all(f"'{name}'" in tail for name in ALLOC.check_names)
    assert tail.count("UNION ALL") == 2


def test_render_sql_price_checks_and_concat_key():
    rows = parse_rows(PRICE, [_price()], None)
    sql = render_sql(PRICE, rows, 500, "p.csv", None)
    assert "WHERE (provider, model, tier, effective_from) NOT IN (" in sql
    assert "'standard' AS tier" in sql
    tail = sql.split("-- 검증: 결과가 비어야 정상", 1)[1]
    assert "concat(provider, '/', model, '/', tier) AS key" in tail
    assert all(f"'{name}'" in tail for name in PRICE.check_names)
    assert tail.count("UNION ALL") == 2


def test_render_sql_deterministic():
    rows = parse_rows(TCO, [_tco(), _tco(gpu_type="A100", tco_krw_per_gpu_hour="2100")], None)
    assert render_sql(TCO, rows, 500, "t.csv", None) == render_sql(TCO, rows, 500, "t.csv", None)


def test_render_sql_quote_and_backslash_escape():
    rows = parse_rows(ALLOC, [_alloc(service_group="O'Brien\\Group")], None)
    sql = render_sql(ALLOC, rows, 500, "a.csv", None)
    assert "O\\'Brien\\\\Group" in sql


def test_render_sql_chunking():
    rows = parse_rows(TCO, [_tco(gpu_type=f"G{i}") for i in range(5)], None)
    sql = render_sql(TCO, rows, 2, "t.csv", None)
    assert sql.count("INSERT INTO gpu_data.dim_token_gpu_tco_dist") == 3
    assert sql.count("'dup_key' AS check_name") == 1


def test_render_sql_target_db_override():
    rows = parse_rows(PRICE, [_price()], None)
    sql = render_sql(PRICE, rows, 500, "p.csv", None, "token_verify_dim")
    assert "token_verify_dim.dim_token_vendor_price_dist" in sql
    assert "gpu_data." not in sql


# ---------------------------------------------------------------- CLI

def _run(table, *extra, csv_path=None, out_path):
    return subprocess.run(
        [sys.executable, str(TOOL_PATH), "--table", table, "--csv", str(csv_path or FIXTURES[table]),
         "--out", str(out_path), *extra],
        capture_output=True, text=True,
    )


@pytest.mark.parametrize("table,expected_rows,expected_nulls", [
    ("gpu_tco", 3, 1), ("gpu_allocation", 3, 0), ("vendor_price", 2, 2),
])
def test_cli_roundtrip_fixtures(tmp_path, table, expected_rows, expected_nulls):
    out_path = tmp_path / "out.sql"
    result = _run(table, "--effective-from", EF, out_path=out_path)
    assert result.returncode == 0, result.stderr
    body = out_path.read_text(encoding="utf-8")
    assert body.count(f"INSERT INTO gpu_data.{TABLE_SPECS[table].table}_dist") == 1
    assert body.count("CAST(NULL AS Nullable(Float64))") == expected_nulls
    assert f"출력 행수: {expected_rows} (NULL 숫자 셀 {expected_nulls})" in result.stdout
    assert result.stderr == ""


def test_cli_alloc_fixture_source_default_and_zero_row(tmp_path):
    out_path = tmp_path / "out.sql"
    assert _run("gpu_allocation", out_path=out_path).returncode == 0
    body = out_path.read_text(encoding="utf-8")
    assert "'A100', toDate('2026-08-26'), CAST(4.0 AS Nullable(Float64)), 'manual', " in body
    assert "'H100', toDate('2026-09-01'), CAST(0.0 AS Nullable(Float64)), 'quota-sheet', " in body


def test_cli_target_db_option(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run("gpu_tco", "--effective-from", EF, "--target-db", "token_verify_dim", out_path=out_path)
    assert result.returncode == 0, result.stderr
    body = out_path.read_text(encoding="utf-8")
    assert "INSERT INTO token_verify_dim.dim_token_gpu_tco_dist" in body
    assert "gpu_data." not in body


def test_cli_invalid_table_and_target_db_are_usage_errors(tmp_path):
    bad_table = subprocess.run(
        [sys.executable, str(TOOL_PATH), "--table", "gpu_cost", "--csv", str(FIXTURES["gpu_tco"]),
         "--out", str(tmp_path / "o.sql")], capture_output=True, text=True,
    )
    assert bad_table.returncode == 2
    assert _run("gpu_tco", "--target-db", "mart", out_path=tmp_path / "o.sql").returncode == 2


def test_cli_missing_default_effective_from_fails_on_blank_row(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run("gpu_tco", out_path=out_path)
    assert result.returncode == 1
    assert "--effective-from" in result.stderr
    assert not out_path.exists()


def test_cli_stdout_summary_only(tmp_path):
    out_path = tmp_path / "out.sql"
    result = _run("vendor_price", out_path=out_path)
    assert result.returncode == 0
    assert "INSERT INTO" not in result.stdout
    assert "claude-" not in result.stdout and "anthropic" not in result.stdout
    assert "4050" not in result.stdout


def test_cli_error_output_has_no_data_rows(tmp_path):
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(
        "gpu_type,effective_from,tco_krw_per_gpu_hour,basis,note\n"
        "SECRET-GPU,2026-08-26,99999,secret-basis,\n",
        encoding="utf-8",
    )
    out_path = tmp_path / "out.sql"
    result = _run("gpu_tco", csv_path=bad_csv, out_path=out_path)
    assert result.returncode == 1
    assert "basis_domain" in result.stderr
    for secret in ("SECRET-GPU", "99999", "secret-basis"):
        assert secret not in result.stderr and secret not in result.stdout
    assert not out_path.exists()


def test_cli_missing_required_header(tmp_path):
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text("gpu,cost\nH100,1\n", encoding="utf-8")
    result = _run("gpu_tco", csv_path=bad_csv, out_path=tmp_path / "o.sql")
    assert result.returncode == 1
    assert "필수 컬럼 없음" in result.stderr
```

Run:
```bash
cd /home/mini/github/token-data-pipeline/assets/model-catalog && python3 -m pytest -q tests/test_layer_c_tool.py 2>&1 | tail -4
```
Expected (수집 단계 ImportError — 모듈 부재):
```text
=========================== short test summary info ============================
ERROR tests/test_layer_c_tool.py
!!!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
1 error in 0.5s
```

- [ ] **Step 3: 생성기 구현**

`assets/model-catalog/csv_to_layer_c_dim_insert.py`:
```python
#!/usr/bin/env python3
"""Layer C 기준정보 CSV → gpu_data.dim_token_{gpu_tco,gpu_allocation,vendor_price}_dist INSERT SQL 생성기 (Plan 6a T9).

`sheet_to_dim_token_model_alias_insert.py`(T8)와 같은 골격 — stdlib만 사용(csv/argparse/dataclasses).
Python 3.10+. `--table` 1개로 3테이블을 다루며, 테이블별 계약은 TABLE_SPECS에만 있다.

CSV 계약(설계 2026-08-31 §4.2 — 통화 KRW 고정, effective_from = 소급 시작일):
  gpu_tco        헤더 gpu_type,effective_from,tco_krw_per_gpu_hour,basis,note
                 필수 gpu_type,tco_krw_per_gpu_hour. basis ∈ {'', depreciation, lease, power-inclusive, tco}.
  gpu_allocation 헤더 service_group,gpu_type,effective_from,allocated_gpu_count,source,note
                 필수 service_group,gpu_type,allocated_gpu_count. source 빈 값 → 'manual'. 철회는 0.
  vendor_price   헤더 provider,model,tier,effective_from,krw_per_mtok_input,krw_per_mtok_cached,
                 krw_per_mtok_cache_creation,krw_per_mtok_output,note
                 필수 provider,model,krw_per_mtok_input,krw_per_mtok_output. tier 빈 값 → 'standard'
                 (∈ standard|batch|flex|priority). cached/cache_creation 빈 값 → NULL.
  공통: 선택 컬럼 currency는 있으면 ''/'KRW'만 허용(currency_krw). 숫자 컬럼은 빈 값 → NULL(플레이스홀더),
        음수 금지(negative_value). 키 값 'unknown'은 시드 플레이스홀더 예약어 — 금지(unknown_reserved).
        effective_from 빈 값 → --effective-from(둘 다 없으면 오류); 2026-01-01(시드 플레이스홀더 키) 금지.
        자동 교정 없음(strip만). 키 튜플 중복 금지(dup_key).

출력: 멱등(NOT IN 가드) INSERT SQL + `SETTINGS insert_distributed_sync = 1;` + 말미
  `-- 검증: 결과가 비어야 정상` 앵커 뒤 검증 SELECT — 시드 파일(seed_dim_token_*.sql)과 동일 항목·4열
  (check_name, key, effective_from, cnt).

데이터 경계(§7.2): 실 CSV·생성 SQL은 레포 반입 금지 — .gitignore가 `*gpu_tco*.csv`, `*gpu_allocation*.csv`,
  `*vendor_price*.csv`, `dim_token_gpu_*_insert*.sql`, `dim_token_vendor_price_insert*.sql`로 차단.
  stdout은 요약(건수)만, 데이터 원문(기종·그룹·모델·단가)은 성공/실패 경로 모두에서 에코하지 않는다.

exit code: 0 성공 / 1 검증 실패(LayerCError) / 2 인자·입력 오류(argparse).
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

DEFAULT_CHUNK_SIZE = 500
DEFAULT_TARGET_DB = "gpu_data"
TARGET_DB_CHOICES = ("gpu_data", "token_verify_dim")
PLACEHOLDER_EFFECTIVE_FROM = "2026-01-01"
RESERVED_KEY_VALUE = "unknown"
CURRENCY = "KRW"
BASIS_DOMAIN = ("", "depreciation", "lease", "power-inclusive", "tco")
TIER_DOMAIN = ("standard", "batch", "flex", "priority")

_DATE_FMT = "%Y-%m-%d"


class LayerCError(Exception):
    """CSV 검증 실패. 메시지는 행 번호·필드명·검증명만 포함 — 데이터 값 에코 금지."""


@dataclass(frozen=True)
class TableSpec:
    name: str                 # --table 값
    table: str                # <db>.<table>_dist 의 테이블 부분
    key_columns: tuple        # NOT IN 가드 키 (effective_from 포함)
    string_columns: tuple     # (컬럼, 필수 여부, 기본값)
    numeric_columns: tuple    # (컬럼, 필수 여부)
    trailing_columns: tuple   # 문자열 후행 컬럼 (기본값 '')
    default_out: str
    check_names: tuple


TABLE_SPECS = {
    "gpu_tco": TableSpec(
        name="gpu_tco",
        table="dim_token_gpu_tco",
        key_columns=("gpu_type", "effective_from"),
        string_columns=(("gpu_type", True, ""),),
        numeric_columns=(("tco_krw_per_gpu_hour", True),),
        trailing_columns=("currency", "basis", "note"),
        default_out="dim_token_gpu_tco_insert.sql",
        check_names=("dup_key", "unknown_row_state", "basis_domain", "currency_krw"),
    ),
    "gpu_allocation": TableSpec(
        name="gpu_allocation",
        table="dim_token_gpu_allocation",
        key_columns=("service_group", "gpu_type", "effective_from"),
        string_columns=(("service_group", True, ""), ("gpu_type", True, "")),
        numeric_columns=(("allocated_gpu_count", True),),
        trailing_columns=("source", "note"),
        default_out="dim_token_gpu_allocation_insert.sql",
        check_names=("dup_key", "unknown_row_state", "negative_count"),
    ),
    "vendor_price": TableSpec(
        name="vendor_price",
        table="dim_token_vendor_price",
        key_columns=("provider", "model", "tier", "effective_from"),
        string_columns=(("provider", True, ""), ("model", True, ""), ("tier", False, "standard")),
        numeric_columns=(
            ("krw_per_mtok_input", True),
            ("krw_per_mtok_cached", False),
            ("krw_per_mtok_cache_creation", False),
            ("krw_per_mtok_output", True),
        ),
        trailing_columns=("note",),
        default_out="dim_token_vendor_price_insert.sql",
        check_names=("dup_key", "unknown_row_state", "tier_domain"),
    ),
}
TABLE_CHOICES = tuple(TABLE_SPECS)


def required_headers(spec: TableSpec) -> tuple:
    return tuple(c for c, req, _d in spec.string_columns if req) + tuple(c for c, req in spec.numeric_columns if req)


def insert_columns(spec: TableSpec) -> tuple:
    """INSERT 컬럼 순서 = DDL 컬럼 순서: 문자열 키 → effective_from → 숫자 → 후행."""
    return (
        tuple(c for c, _r, _d in spec.string_columns)
        + ("effective_from",)
        + tuple(c for c, _r in spec.numeric_columns)
        + spec.trailing_columns
    )


@dataclass
class DimRow:
    values: dict     # 컬럼명 → str | float | None  (effective_from은 'YYYY-MM-DD' 문자열)
    row_no: int      # 원본 CSV 데이터 행 번호(1부터) — 오류 메시지용, SQL 미기록

    def key(self, spec: TableSpec) -> tuple:
        return tuple(self.values[c] for c in spec.key_columns)


def _parse_date(value: str, field_label: str, row_no: int) -> str:
    try:
        dt = datetime.strptime(value, _DATE_FMT)
    except ValueError as exc:
        where = f"{row_no}번째 데이터 행: {field_label}" if row_no > 0 else field_label
        raise LayerCError(f"{where} 날짜 형식 오류 (YYYY-MM-DD 필요)") from exc
    return dt.strftime(_DATE_FMT)


def _parse_number(raw: str, column: str, row_no: int):
    raw = raw.strip()
    if raw == "":
        return None
    try:
        value = float(raw.replace(",", ""))
    except ValueError as exc:
        raise LayerCError(f"{row_no}번째 데이터 행: {column} 숫자 형식 오류 (bad_number)") from exc
    if math.isnan(value) or math.isinf(value):
        raise LayerCError(f"{row_no}번째 데이터 행: {column} 숫자 형식 오류 (bad_number)")
    if value < 0:
        raise LayerCError(f"{row_no}번째 데이터 행: {column} 음수 금지 — 철회는 0 (negative_value)")
    return value


def parse_rows(spec: TableSpec, rows, default_effective_from) -> list:
    """CSV DictReader 행 목록 → 검증된 DimRow 목록 (순수 함수, 입력 순서 유지, 자동 교정 없음)."""
    if default_effective_from is not None:
        default_effective_from = _parse_date(default_effective_from, "--effective-from", 0)
        if default_effective_from == PLACEHOLDER_EFFECTIVE_FROM:
            raise LayerCError(
                f"--effective-from: {PLACEHOLDER_EFFECTIVE_FROM}은 사내 시드 플레이스홀더 키 날짜 — "
                "금지 (effective_from_is_placeholder_date)"
            )

    out: list = []
    seen: dict = {}
    for idx, raw in enumerate(rows, start=1):
        values: dict = {}
        for column, required, default in spec.string_columns:
            v = (raw.get(column) or "").strip()
            if v == "" and default:
                v = default
            if required and v == "":
                raise LayerCError(f"{idx}번째 데이터 행: {column} 필드가 비어 있음 (empty_key)")
            if v == RESERVED_KEY_VALUE:
                raise LayerCError(
                    f"{idx}번째 데이터 행: {column}='unknown'은 시드 플레이스홀더 예약어 (unknown_reserved)"
                )
            values[column] = v

        ef_raw = (raw.get("effective_from") or "").strip()
        if ef_raw:
            effective_from = _parse_date(ef_raw, "effective_from", idx)
        elif default_effective_from is not None:
            effective_from = default_effective_from
        else:
            raise LayerCError(f"{idx}번째 데이터 행: effective_from 비어 있음 + --effective-from 미지정")
        if effective_from == PLACEHOLDER_EFFECTIVE_FROM:
            raise LayerCError(
                f"{idx}번째 데이터 행: effective_from이 사내 시드 플레이스홀더 키 날짜"
                f"({PLACEHOLDER_EFFECTIVE_FROM}) — 금지 (effective_from_is_placeholder_date)"
            )
        values["effective_from"] = effective_from

        for column, required in spec.numeric_columns:
            if required and column not in raw:
                raise LayerCError(f"CSV 헤더에 필수 컬럼 없음: {column}")
            values[column] = _parse_number(raw.get(column) or "", column, idx)

        currency = (raw.get("currency") or "").strip()
        if currency not in ("", CURRENCY):
            raise LayerCError(f"{idx}번째 데이터 행: currency는 KRW 고정 (currency_krw)")
        for column in spec.trailing_columns:
            if column == "currency":
                values[column] = CURRENCY
            elif column == "source":
                values[column] = (raw.get(column) or "").strip() or "manual"
            else:
                values[column] = (raw.get(column) or "").strip()

        if spec.name == "gpu_tco" and values["basis"] not in BASIS_DOMAIN:
            raise LayerCError(
                f"{idx}번째 데이터 행: basis 도메인 위반 — 허용: depreciation|lease|power-inclusive|tco|빈 값 (basis_domain)"
            )
        if spec.name == "vendor_price" and values["tier"] not in TIER_DOMAIN:
            raise LayerCError(
                f"{idx}번째 데이터 행: tier 도메인 위반 — 허용: standard|batch|flex|priority (tier_domain)"
            )

        row = DimRow(values=values, row_no=idx)
        key = row.key(spec)
        if key in seen:
            raise LayerCError(
                f"{idx}번째 데이터 행: 키 {spec.key_columns} 중복 (최초 발생: {seen[key]}번째 데이터 행) (dup_key)"
            )
        seen[key] = idx
        out.append(row)
    return out


def _escape_sql_string(value: str) -> str:
    """이스케이프 순서: '\\' -> '\\\\' 먼저, 그다음 "'" -> "\\'" (순서 바뀌면 SQL 파손)."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _literal(column: str, value, with_alias: bool) -> str:
    if column == "effective_from":
        expr = f"toDate('{value}')"
    elif value is None:
        expr = "CAST(NULL AS Nullable(Float64))"
    elif isinstance(value, float):
        expr = f"CAST({value!r} AS Nullable(Float64))"
    else:
        expr = f"'{_escape_sql_string(value)}'"
    return f"{expr} AS {column}" if with_alias else expr


def _row_select(spec: TableSpec, row: DimRow, with_alias: bool) -> str:
    return "SELECT " + ", ".join(_literal(c, row.values[c], with_alias) for c in insert_columns(spec))


def _chunks(rows: list, chunk_size: int):
    for i in range(0, len(rows), chunk_size):
        yield rows[i : i + chunk_size]


def _key_expr(spec: TableSpec) -> str:
    string_keys = [c for c in spec.key_columns if c != "effective_from"]
    if len(string_keys) == 1:
        return string_keys[0]
    return "concat(" + ", '/', ".join(string_keys) + ")"


def _verification(spec: TableSpec, target_table: str) -> list:
    key_expr = _key_expr(spec)
    key_list = ", ".join(spec.key_columns)
    lines = ["-- 검증: 결과가 비어야 정상 ------------------------------------------------"]
    lines += [
        f"-- 1) dup_key: ({key_list}) 키 중복 없음",
        f"SELECT 'dup_key' AS check_name, {key_expr} AS key, effective_from, count() AS cnt",
        f"FROM {target_table}",
        f"GROUP BY {key_list}",
        "HAVING count() > 1",
        "",
        "UNION ALL",
        "",
    ]
    if spec.name == "gpu_tco":
        lines += [
            "-- 2) unknown_row_state: unknown 행 존재 + TCO 전부 NULL",
            "SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'), countIf(tco_krw_per_gpu_hour IS NOT NULL)",
            f"FROM {target_table}",
            "WHERE gpu_type = 'unknown'",
            "HAVING count() = 0 OR countIf(tco_krw_per_gpu_hour IS NOT NULL) > 0",
            "",
            "UNION ALL",
            "",
            "-- 3) basis_domain",
            "SELECT 'basis_domain', gpu_type, effective_from, toUInt64(1)",
            f"FROM {target_table}",
            "WHERE basis NOT IN ('', 'depreciation', 'lease', 'power-inclusive', 'tco')",
            "",
            "UNION ALL",
            "",
            "-- 4) currency_krw: 통화 KRW 고정",
            "SELECT 'currency_krw', gpu_type, effective_from, toUInt64(1)",
            f"FROM {target_table}",
            "WHERE currency != 'KRW';",
        ]
    elif spec.name == "gpu_allocation":
        lines += [
            "-- 2) unknown_row_state: 플레이스홀더 행 존재 + gpu_type='unknown' 행은 전부 NULL",
            "SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'), countIf(allocated_gpu_count IS NOT NULL)",
            f"FROM {target_table}",
            "WHERE gpu_type = 'unknown'",
            "HAVING count() = 0 OR countIf(allocated_gpu_count IS NOT NULL) > 0",
            "",
            "UNION ALL",
            "",
            "-- 3) negative_count: 음수 할당 금지 (철회는 0 행)",
            f"SELECT 'negative_count', {key_expr}, effective_from, toUInt64(1)",
            f"FROM {target_table}",
            "WHERE allocated_gpu_count < 0;",
        ]
    else:
        lines += [
            "-- 2) unknown_row_state: unknown 행 존재 + 단가 전부 NULL",
            "SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'),",
            "       countIf(krw_per_mtok_input IS NOT NULL OR krw_per_mtok_cached IS NOT NULL",
            "               OR krw_per_mtok_cache_creation IS NOT NULL OR krw_per_mtok_output IS NOT NULL)",
            f"FROM {target_table}",
            "WHERE provider = 'unknown' AND model = 'unknown'",
            "HAVING count() = 0",
            "    OR countIf(krw_per_mtok_input IS NOT NULL OR krw_per_mtok_cached IS NOT NULL",
            "               OR krw_per_mtok_cache_creation IS NOT NULL OR krw_per_mtok_output IS NOT NULL) > 0",
            "",
            "UNION ALL",
            "",
            "-- 3) tier_domain",
            f"SELECT 'tier_domain', {key_expr}, effective_from, toUInt64(1)",
            f"FROM {target_table}",
            "WHERE tier NOT IN ('standard', 'batch', 'flex', 'priority');",
        ]
    lines.append("")
    return lines


def render_sql(spec: TableSpec, rows: list, chunk_size: int, source_name: str, default_effective_from,
               target_db: str = DEFAULT_TARGET_DB) -> str:
    """검증된 DimRow 목록 → 결정적(byte-identical) INSERT SQL 문자열 (순수 함수)."""
    target_table = f"{target_db}.{spec.table}_dist"
    columns = insert_columns(spec)
    null_count = sum(1 for r in rows for c, _req in spec.numeric_columns if r.values[c] is None)

    lines = []
    lines.append("-- =============================================================")
    lines.append(f"-- {target_table} Layer C 기준정보 INSERT (--table {spec.name})")
    lines.append("-- 생성: csv_to_layer_c_dim_insert.py (Plan 6a T9)")
    lines.append(f"-- 소스 파일: {source_name}")
    lines.append(f"-- 행수: {len(rows)} (NULL 숫자 셀 {null_count})")
    lines.append(f"-- 기본 effective_from: {default_effective_from or '(행별 값만)'}")
    lines.append("-- 통화: KRW 고정 (설계 2026-08-31 §4.2)")
    lines.append("-- 경고: 실값 산출물(이 파일)은 레포·사외 환경 반입 금지 (§7.2, .gitignore 커버)")
    lines.append("-- 실행 주체: admin 수동 — 변경은 새 effective_from 행 append (기존 행 불변)")
    lines.append("-- =============================================================")
    lines.append("")
    for chunk in _chunks(rows, chunk_size):
        lines.append(f"INSERT INTO {target_table}")
        lines.append(f"    ({', '.join(columns)})")
        lines.append("SELECT *")
        lines.append("FROM (")
        lines.append("    " + "\n    UNION ALL\n    ".join(_row_select(spec, r, i == 0) for i, r in enumerate(chunk)))
        lines.append(")")
        lines.append(f"WHERE ({', '.join(spec.key_columns)}) NOT IN (")
        lines.append(f"    SELECT {', '.join(spec.key_columns)} FROM {target_table}")
        lines.append(")")
        lines.append("SETTINGS insert_distributed_sync = 1;")
        lines.append("")
    lines += _verification(spec, target_table)
    return "\n".join(lines) + "\n"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Layer C 기준정보 CSV -> gpu_data.dim_token_{gpu_tco,gpu_allocation,vendor_price}_dist INSERT SQL 생성기"
    )
    parser.add_argument("--table", required=True, choices=TABLE_CHOICES, help="대상 dim (CSV 계약은 모듈 docstring)")
    parser.add_argument("--csv", required=True, help="입력 CSV 경로")
    parser.add_argument(
        "--effective-from",
        default=None,
        help="행의 effective_from이 빈 경우의 기본값 (YYYY-MM-DD, 소급 시작일 — 2026-01-01 금지)",
    )
    parser.add_argument("--out", default=None, help="출력 SQL 경로 (기본: dim_token_<table>_insert.sql — gitignore 대상)")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help=f"INSERT chunk 크기 (기본 {DEFAULT_CHUNK_SIZE})")
    parser.add_argument(
        "--target-db",
        default=DEFAULT_TARGET_DB,
        choices=TARGET_DB_CHOICES,
        help="INSERT 대상 dim DB명 (기본: gpu_data — company-verify는 token_verify_dim)",
    )
    return parser


def main(argv=None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    spec = TABLE_SPECS[args.table]

    if args.chunk_size <= 0:
        parser.error("--chunk-size는 1 이상이어야 함")

    csv_path = Path(args.csv)
    try:
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames or []
            raw_rows = list(reader)
    except OSError:
        parser.error(f"--csv 파일을 열 수 없음: {csv_path.name}")
        return 2  # pragma: no cover — parser.error already exits

    missing = [h for h in required_headers(spec) if h not in header]
    if missing:
        print(f"검증 실패: CSV 헤더에 필수 컬럼 없음: {', '.join(missing)}", file=sys.stderr)
        return 1

    try:
        rows = parse_rows(spec, raw_rows, args.effective_from)
    except LayerCError as exc:
        print(f"검증 실패: {exc}", file=sys.stderr)
        return 1

    sql_text = render_sql(spec, rows, args.chunk_size, csv_path.name, args.effective_from, args.target_db)
    out_path = Path(args.out or spec.default_out)
    out_path.write_text(sql_text, encoding="utf-8")

    null_count = sum(1 for r in rows for c, _req in spec.numeric_columns if r.values[c] is None)
    num_chunks = (len(rows) + args.chunk_size - 1) // args.chunk_size if rows else 0
    print(f"생성 완료: {out_path.name} (--table {spec.name})")   # 경로 미출력 — tmp 경로 문자열이 stdout 위생 검사에 걸리지 않게
    print(f"입력 데이터 행수: {len(raw_rows)} → 출력 행수: {len(rows)} (NULL 숫자 셀 {null_count})")
    print(f"chunk 크기: {args.chunk_size} (chunk 수: {num_chunks})")
    print(
        "검증: 출력 SQL 말미 \"-- 검증: 결과가 비어야 정상\" 섹션 실행 후 결과가 비어 있어야 정상 "
        "(admin 리뷰 절차 — 시드 seed_dim_token_*.sql 적용 이후에 실행)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

Run:
```bash
cd /home/mini/github/token-data-pipeline/assets/model-catalog && python3 -m pytest -q tests/test_layer_c_tool.py 2>&1 | tail -1 && python3 -m pytest -q 2>&1 | tail -1
```
Expected:
```text
48 passed in 2.3s
128 passed in 4.8s
```
(128 = T2 매니페스트 45 + T8 alias 35 + T9 Layer C 48.)

- [ ] **Step 4: CLI 스모크 — 3테이블 fixture 왕복 + 기본 `--out` 이름 gitignore 확인 + 검증 항목 순서**

Run:
```bash
cd /home/mini/github/token-data-pipeline
python3 assets/model-catalog/csv_to_layer_c_dim_insert.py --table gpu_tco \
  --csv assets/model-catalog/fixtures/synthetic_layer_c_tco.csv --effective-from 2026-08-26; echo "exit=$?"
python3 assets/model-catalog/csv_to_layer_c_dim_insert.py --table gpu_allocation \
  --csv assets/model-catalog/fixtures/synthetic_layer_c_allocation.csv | head -2
python3 assets/model-catalog/csv_to_layer_c_dim_insert.py --table vendor_price \
  --csv assets/model-catalog/fixtures/synthetic_layer_c_price.csv | head -2
git check-ignore -v dim_token_gpu_tco_insert.sql dim_token_gpu_allocation_insert.sql dim_token_vendor_price_insert.sql
for f in dim_token_gpu_tco_insert.sql dim_token_gpu_allocation_insert.sql dim_token_vendor_price_insert.sql; do
  echo "$f: INSERT $(grep -c '^INSERT INTO gpu_data\.' "$f") / NULL $(grep -o 'CAST(NULL AS Nullable(Float64))' "$f" | wc -l) / $(grep -o '^-- [0-9]) [a-z_]*' "$f" | tr '\n' ' ')"
done
git status --porcelain | grep -c "_insert.sql"
rm -f dim_token_gpu_tco_insert.sql dim_token_gpu_allocation_insert.sql dim_token_vendor_price_insert.sql
```
Expected:
```text
생성 완료: dim_token_gpu_tco_insert.sql (--table gpu_tco)
입력 데이터 행수: 3 → 출력 행수: 3 (NULL 숫자 셀 1)
chunk 크기: 500 (chunk 수: 1)
검증: 출력 SQL 말미 "-- 검증: 결과가 비어야 정상" 섹션 실행 후 결과가 비어 있어야 정상 (admin 리뷰 절차 — 시드 seed_dim_token_*.sql 적용 이후에 실행)
exit=0
생성 완료: dim_token_gpu_allocation_insert.sql (--table gpu_allocation)
입력 데이터 행수: 3 → 출력 행수: 3 (NULL 숫자 셀 0)
생성 완료: dim_token_vendor_price_insert.sql (--table vendor_price)
입력 데이터 행수: 2 → 출력 행수: 2 (NULL 숫자 셀 2)
.gitignore:27:dim_token_gpu_*_insert*.sql	dim_token_gpu_tco_insert.sql
.gitignore:27:dim_token_gpu_*_insert*.sql	dim_token_gpu_allocation_insert.sql
.gitignore:28:dim_token_vendor_price_insert*.sql	dim_token_vendor_price_insert.sql
dim_token_gpu_tco_insert.sql: INSERT 1 / NULL 1 / -- 1) dup_key -- 2) unknown_row_state -- 3) basis_domain -- 4) currency_krw 
dim_token_gpu_allocation_insert.sql: INSERT 1 / NULL 0 / -- 1) dup_key -- 2) unknown_row_state -- 3) negative_count 
dim_token_vendor_price_insert.sql: INSERT 1 / NULL 2 / -- 1) dup_key -- 2) unknown_row_state -- 3) tier_domain 
0
```
(`23`/`24` = T1이 추가한 `.gitignore` 패턴 줄 번호(alias 패턴 22 다음 두 줄). allocation·price fixture는 모든 행에 `effective_from`이 있어 `--effective-from` 생략 가능, tco fixture의 H200 행은 빈 값이라 `--effective-from` 필수. 마지막 `0` = 생성 SQL 3개가 `git status`에 나타나지 않는다(§7.2 경계).)

- [ ] **Step 5: 커밋**

```bash
cd /home/mini/github/token-data-pipeline
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml
# 기대: 출력 없음
git status --porcelain | grep -v "^?? assets/model-catalog/" ; echo "(위 출력 없음 = 신규 파일만)"
git add assets/model-catalog/csv_to_layer_c_dim_insert.py assets/model-catalog/tests/test_layer_c_tool.py \
  assets/model-catalog/fixtures/synthetic_layer_c_tco.csv assets/model-catalog/fixtures/synthetic_layer_c_allocation.csv \
  assets/model-catalog/fixtures/synthetic_layer_c_price.csv
git commit -m "feat(assets): Layer C 기준정보 CSV → dim_token_{gpu_tco,gpu_allocation,vendor_price} INSERT 생성기 + 테스트 48 + 합성 fixture 3 (Plan 6a T9)

- 생성기 1개 --table gpu_tco|gpu_allocation|vendor_price (stdlib only): --csv --effective-from --out --chunk-size --target-db gpu_data|token_verify_dim
- CSV 헤더 = 설계 §4.2 컬럼명, 통화 KRW 고정(currency 컬럼 선택·KRW만), 숫자 빈 값 → NULL 플레이스홀더, 음수·nan·inf 금지, 'unknown' 예약어 금지, 2026-01-01 금지, dup_key
- 출력 SQL: NOT IN 가드 + insert_distributed_sync + 검증 앵커 + T6 시드와 동일한 검증 항목(unknown_row_state/basis_domain/currency_krw/negative_count/tier_domain)
- stdout/stderr에 기종·그룹·모델·단가 원문 미출력(§7.2); 기본 --out 이름은 .gitignore 커버

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

---

### Task 10: 수기(manual-v0) 제출 템플릿 3파일 — `docs/templates/token_metrics_manual_v0_{gpu,serving,engine}.csv`

**Files:**
- Create: `docs/templates/token_metrics_manual_v0_gpu.csv`
- Create: `docs/templates/token_metrics_manual_v0_serving.csv`
- Create: `docs/templates/token_metrics_manual_v0_engine.csv`

**Interfaces:**
- 헤더(설계 §5.5 그대로, 변경 금지): gpu `date,service,model,gpuType,category,gpuCount,gpuHours` / serving `date,service,model,metric,name,unit,p50,p90,p95,p99` / engine `service,engine_type,engine_version`.
- 주석 규칙(6b `collectors/token-metrics` manual CSV 파서 계약): **`#`로 시작하는 줄은 건너뛴다**(주석 안의 쉼표 무시). 첫 비주석 줄이 헤더. 빈 셀 = 부재(serving의 `name`/`unit`은 `metric=custom`일 때만, `outputTps`는 `p50`만). 예시 행은 합성값(Mock Service A/B, mock 모델 `claude-sonnet-5`/`claude-haiku-4-5`)이며 담당자가 삭제 후 실값으로 교체한다.
- serving `metric` 값은 스펙 §3 키 그대로(`ttftMs|itlMs|outputTps|e2eMs|custom`) — fact `metric`(`ttft_ms|itl_ms|output_tps|e2e_ms|custom`) 변환은 6b normalize(API 경로와 동일). 설계 해석: 헤더가 이미 API camelCase(`gpuType`,`gpuCount`,`gpuHours`)이므로 metric도 API 키를 쓰고 변환은 한 곳(normalize)에서만 한다.
- 실제 제출 파일명은 `*manual_metrics*.csv`(T1 gitignore) — 템플릿 이름 `token_metrics_manual_v0_*`는 패턴에 걸리지 않아 커밋된다(T1 Step 2 음성 목록에 포함).

- [ ] **Step 1: 템플릿 3파일 작성**

`docs/templates/token_metrics_manual_v0_gpu.csv`:
```csv
# token_metrics_manual_v0_gpu — go-live 이전 구간 수기(manual-v0) GPU Hour 제출 템플릿 (설계 2026-08-31 §5.5 · /v1/metrics 스펙 §3 gpu 블록)
# '#'로 시작하는 줄은 주석 — 로더(collectors/token-metrics manual 모드)가 건너뜀. 첫 비주석 줄이 헤더(변경 금지). 1행 = 1일 × 1모델 × 1기종 × 1용도
# date = YYYY-MM-DD (제출 대상일, KST). service = 메타데이터 시트·토큰 API와 바이트 동일한 공식 표기 (endpoints 레지스트리 등록·enabled 필수)
# model = canonical 표기 (≤128). gpuType = 단순 기종 표기 (≤64, 운영자 단가표 dim_token_gpu_tco 의 키 — 예 H100, A100, H200, L40S)
# category = serving | standby | test. model "unknown" 은 category=test 에서만 허용 (serving/standby 의 "unknown" 은 거부)
# gpuCount = 그날 최대 장수 (>0, 표시·검증용 — 비용 계산에 쓰지 않음). gpuHours = 장수 × 매핑·할당 시간의 적분 (GPU·hour, ≥0) — 비용의 유일한 근거
# 검증: 행별 gpuHours ≤ gpuCount × 24. 하루 중 장수 증감은 gpuHours 에 반영, 기종 변경·시간 분할(주간/야간 모델)은 행을 나눔. 유휴는 제출하지 않음 (할당 − Σ제공 으로 산출)
# 사내 플랫폼 사용분은 행을 쓰지 않음 (GPU 는 플랫폼이 제공 — 이중 계상 방지). 사용·가동이 실제 0인 날은 그 날의 행이 없음 (빈 gpu 배열과 동일)
# 아래 예시 행은 합성값 — 제출 전 삭제하고 실제 값으로 교체. 실제 파일은 *manual_metrics*.csv 로 저장 (레포 반입 금지 — .gitignore)
date,service,model,gpuType,category,gpuCount,gpuHours
2026-08-26,Mock Service A,claude-sonnet-5,H100,serving,4,96.0
2026-08-26,Mock Service A,claude-sonnet-5,H100,standby,1,24.0
2026-08-26,Mock Service B,claude-haiku-4-5,A100,serving,1,24.0
2026-08-26,Mock Service B,unknown,A100,test,1,2.0
```

`docs/templates/token_metrics_manual_v0_serving.csv`:
```csv
# token_metrics_manual_v0_serving — 수기(manual-v0) 성능 메트릭 제출 템플릿 (설계 2026-08-31 §5.5 · 스펙 §3 serving 블록). long form: 1행 = 1일 × 1모델 × 1지표
# '#'로 시작하는 줄은 주석 — 로더가 건너뜀. 첫 비주석 줄이 헤더(변경 금지)
# date, service = gpu 템플릿과 동일 규칙. model = canonical (≤128) — gpu 템플릿에 category=serving 행이 있는 모델만 (사내 플랫폼 소비 모델·standby/test 전용 모델·그날 요청 0건 모델은 행 의무 없음)
# metric = ttftMs | itlMs | outputTps | e2eMs | custom (스펙 §3 키 그대로 — 로더가 fact metric ttft_ms / itl_ms / output_tps / e2e_ms / custom 으로 변환)
# name = metric=custom 일 때만 지표명 (≤64, 예 queueWaitMs) — 표준 지표 행은 빈 값. unit = custom 일 때만 필수 (≤32, 예 ms, requests) — 표준 지표 행은 빈 값 (ms 또는 tokens/s 자동)
# p50,p90,p95,p99: ttftMs / itlMs / e2eMs 는 4개 모두 필수 (ms, ≥0). outputTps 는 p50 만 (tokens/s) — p90/p95/p99 빈 값. custom 은 4개 중 최소 1개
# 스트리밍(케이스 A~D): ttftMs·itlMs·outputTps. 비스트리밍(케이스 F): e2eMs 필수 + custom 선택. 사외 AI API 전용(케이스 E)·플랫폼 소비 전용 서비스는 행 없음
# 모델 단위가 정본 — 그 모델로 처리된 요청만의 분포 (모델 간 표본 혼합 금지). 레플리카가 여러 대면 로그·버킷을 모아 한 번에 percentile (레플리카별 p99 의 평균 ≠ 전체 p99)
# 표본 단위: TTFT·outputTps = 요청당 1표본, ITL = 토큰 간격당 1표본. p50 ≤ p90 ≤ p95 ≤ p99 (역전 시 pct_non_monotone FAIL)
# 아래 예시 행은 합성값 — 제출 전 삭제하고 실제 값으로 교체. 실제 파일은 *manual_metrics*.csv 로 저장 (레포 반입 금지 — .gitignore)
date,service,model,metric,name,unit,p50,p90,p95,p99
2026-08-26,Mock Service A,claude-sonnet-5,ttftMs,,,280,560,720,1200
2026-08-26,Mock Service A,claude-sonnet-5,itlMs,,,24,38,47,80
2026-08-26,Mock Service A,claude-sonnet-5,outputTps,,,41.0,,,
2026-08-26,Mock Service B,claude-haiku-4-5,e2eMs,,,1400,2600,3300,5200
2026-08-26,Mock Service B,claude-haiku-4-5,custom,queueWaitMs,ms,120,,,900
```

`docs/templates/token_metrics_manual_v0_engine.csv`:
```csv
# token_metrics_manual_v0_engine — 수기(manual-v0) 추론 엔진 자기신고 템플릿 (선택 파일 — 설계 2026-08-31 §5.5 · 스펙 §3 Engine)
# '#'로 시작하는 줄은 주석 — 로더가 건너뜀. 첫 비주석 줄이 헤더(변경 금지). 1행 = 1서비스 (제출 범위 --from/--to 의 모든 날짜에 동일 적용)
# service = gpu 템플릿과 동일 규칙 (endpoints 레지스트리 등록 필수). engine_type = 엔진 종류 (필수, ≤64, 예 vllm, sglang, custom). engine_version = 버전 원문 (선택, ≤64 — 없으면 빈 값 → '')
# 메타데이터 시트 `서비스` 탭의 engineType 과 불일치하면 운영자가 담당자에게 알림 (스펙 §3). 이 파일을 제출하지 않으면 engine_type = '' 로 적재
# 아래 예시 행은 합성값 — 제출 전 삭제하고 실제 값으로 교체. 실제 파일은 *manual_metrics*.csv 로 저장 (레포 반입 금지 — .gitignore)
service,engine_type,engine_version
Mock Service A,vllm,0.8.4
Mock Service B,custom,
```

- [ ] **Step 2: 헤더·주석 규칙·gitignore 경계 검증**

Run:
```bash
cd /home/mini/github/token-data-pipeline
python3 - <<'PY'
import csv, io
EXPECTED = {
    "gpu": ("date,service,model,gpuType,category,gpuCount,gpuHours", 4),
    "serving": ("date,service,model,metric,name,unit,p50,p90,p95,p99", 5),
    "engine": ("service,engine_type,engine_version", 2),
}
for name, (header, n_rows) in EXPECTED.items():
    path = f"docs/templates/token_metrics_manual_v0_{name}.csv"
    lines = open(path, encoding="utf-8").read().splitlines()
    comments = [l for l in lines if l.startswith("#")]
    body = [l for l in lines if not l.startswith("#")]
    assert body[0] == header, (name, body[0])
    rows = list(csv.DictReader(io.StringIO("\n".join(body))))
    assert len(rows) == n_rows, (name, len(rows))
    assert all(len(r) == len(header.split(",")) for r in rows), name
    assert all(not l.startswith(" ") for l in lines), name
    print(f"{name}: 주석 {len(comments)}줄, 헤더 OK, 예시 행 {len(rows)}")
PY
git check-ignore -v docs/templates/token_metrics_manual_v0_gpu.csv docs/templates/token_metrics_manual_v0_serving.csv docs/templates/token_metrics_manual_v0_engine.csv; echo "check-ignore exit=$? (1 = 무시 안 됨 = 정상)"
git check-ignore -q "docs/templates/mock_service_a_manual_metrics_2026-08.csv"; echo "실파일명 예시 check-ignore exit=$? (0 = 무시됨 = 정상)"
grep -c "unknown" docs/templates/token_metrics_manual_v0_gpu.csv
```
Expected:
```text
gpu: 주석 9줄, 헤더 OK, 예시 행 4
serving: 주석 10줄, 헤더 OK, 예시 행 5
engine: 주석 5줄, 헤더 OK, 예시 행 2
check-ignore exit=1 (1 = 무시 안 됨 = 정상)
실파일명 예시 check-ignore exit=0 (0 = 무시됨 = 정상)
2
```
(`2` = gpu 템플릿의 "unknown" 등장 2회 — 주석 1줄(허용 규칙) + `category=test` 예시 행 1줄. 실파일명 예시는 존재하지 않아도 `check-ignore`가 경로 문자열만으로 판정.)

- [ ] **Step 3: 커밋**

```bash
cd /home/mini/github/token-data-pipeline
git status --porcelain | grep -v "^?? docs/templates/" ; echo "(위 출력 없음 = 신규 파일만)"
git add docs/templates/token_metrics_manual_v0_gpu.csv docs/templates/token_metrics_manual_v0_serving.csv docs/templates/token_metrics_manual_v0_engine.csv
git commit -m "docs: 수기(manual-v0) 제출 템플릿 3파일 — gpu/serving/engine 헤더 + 스펙 §3 규칙 주석 (Plan 6a T10)

- 헤더 = 설계 §5.5: gpu date,service,model,gpuType,category,gpuCount,gpuHours / serving date,service,model,metric,name,unit,p50,p90,p95,p99 / engine service,engine_type,engine_version
- '#' 시작 줄은 주석(6b manual CSV 파서가 건너뜀), 예시 행은 합성값(Mock Service A/B)
- 실제 제출 파일은 *manual_metrics*.csv (T1 gitignore) — 템플릿 이름은 패턴 밖

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

---

### Task 11: 마스터 스펙 v1.14 개정 — `docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md` (설계 §8 개정 목록)

**Files:**
- Modify: `docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md` (허용된 additive 편집 — 설계 §8 표의 11행을 그대로 반영, 기존 문장은 §9 #12·#13·#14의 "Layer C 보류" 3셀과 3행 머리말 버전 문자열만 치환)

**Interfaces:**
- 개정 방식: **앵커 치환 스크립트 1회 실행**. 모든 앵커는 파일에 정확히 1회 존재해야 하며(`assert count == 1`), 이미 개정된 파일(`v1.14` 문자열 존재)에는 재실행이 거부된다(AssertionError, exit 1). 스크립트는 레포에 남기지 않는다(표준입력 실행) — 산출물은 마스터 스펙 diff 1파일뿐.
- 설계 §8 표 11행 ↔ 삽입 위치(모든 삽입은 해당 절의 **끝**, 다음 제목 바로 앞 — 기존 문단·표·제목은 이동하지 않음):

| 설계 §8 행 | 마스터 스펙 편집 | 형태 |
|---|---|---|
| §0 | 3행 `**현재 버전 v1.13 (2026-07-14)**` → `**현재 버전 v1.14 (2026-09-04)**`; §0 표 마지막에 `\| v1.14 \| 2026-09-04 \| **메트릭 싱크 확장 — 자매 스펙 … 참조** …\|` 1행(`## 1. 배경과 목적` 직전) | 치환 1 + 표 행 1 |
| §3 | 레포 트리에 `├── collectors/token-metrics/`(`collectors/token-usage` 블록 끝 주석 줄 뒤) · `├── mart/token-metrics/`(`mart/token-usage` 블록 끝 줄 뒤) 각 2줄 | 코드블록 내 4줄 |
| §4.0 | `### 4.1 fact DB (수집 원본)` 직전: 물리 표 신규 13행(fact 4·gpu_data 5·mart 4) + `--replace` 2단계 IN 배칭 + 예산·가드(45/64)·`--chunk-days` + 장부 위치 | 표 + 항목 3 |
| §4.2/§9-1 | `### 4.3 mart DB (1차 집계)` 직전: P0 예외(대시보드가 mart/fact `_dist` 직접 조회, `view_token_metrics_*` 4종은 P1) | 문단 1 |
| §4.4 | `## 5. collectors/token-usage` 직전: Layer C 스케치 명 → 실제 테이블명 4쌍, 조인 키 `(date, service, canonical)`, 비용 모델 = 정의서 링크 `../../cost-model-spec.md` | 문단 1 |
| §5.2/§5.9-4 | `### 5.3 페이지네이션 불변성 검사 (리뷰 #3 — HIGH)` 직전: 8슬롯 NOT_READY 번역(`SKIPPED reason=not_ready` / `FAILURE reason=not_ready_at_0900`), `SOFT_DEADLINE_MINUTES=40`·`LOAD_BUDGET_S=1200` 예약 산식, 슬롯 산식 3570s | 문단 1 |
| §5.6/§7.3 | `### 5.7 파일 구성·환경변수` 직전: 라벨 `module=token-metrics`(8줄/일, `slot=HH final=0\|1`, `backoffLimit: 0`)·`module=mart-metrics` 항목; `## 8. mock-provider · 테스트 · 운영` 직전: batch_result 패널 규칙 1건(§7.3 끝) | 목록 항목 2 |
| §5.9 | `## 6. assets` 직전: 2′ 순서(DELETE summary→gpu→serving / INSERT gpu→serving→summary), 3′ 앵커 = `raw_token_metrics_summary_1d`, **6조 예외**(자기 레지스트리 `dim_token_metrics_service`, coverage 게이트 미편입), 9조 데드라인(metrics T+1 10:04 → mart-metrics 10:20) | 항목 4 |
| §7.2 | `### 7.3 모니터링` 직전: 배포 원칙(기존 모듈 zero-diff 목록·신규 모듈 독립 배포·`release-images-metrics.yml` 분리·롤백) | 문단 1 |
| §8.3 | `### 8.4 정정(restatement) 프로토콜 (v1.4)` 직전: rerun 체인 표 4행(`rerun.py` ×2 · `manual_load.py` · `run_invariants.py --sql`) + `--replace` 안전 기본값 | 표 |
| §9 | #12·#13·#14 "Layer C 보류" 셀 → `**확정(자매 스펙)** — …` + 출처 `자매 스펙 §… (v1.14)`; `## 10. 구현 순서 (권장)` 직전에 #21~#27 7행 | 치환 3 + 표 행 7 |

- 설계 해석(기록): (1) 설계 §8 "§4.2/§9-1"은 §4.2(view table) 절 끝에 두고 §9 #1은 무수정 — #1은 이미 "view table 도입 완료"라 P0 예외를 §4.2 본문에 적는 편이 독자 경로에 맞음. (2) "§5.6/§7.3"은 두 곳(마커 라벨은 §5.6, 패널 규칙은 §7.3)에 나눠 적음. (3) §9 #26 "`/v1/usage` 소비자 필드"는 자매 스펙 §6.4·§7.4의 P2 항목(소비 서비스 귀속 필드 + `usageIncludesConsumers` 기본 0)으로 해석. (4) 제목(`^## `/`^### `)은 추가하지 않는다 — 앵커 링크·목차 무변경(제목 수 36 유지).
- 검증 계약(Step 3): `git diff --numstat` = `68 4`(추가 68 / 삭제 4: 3행 버전 문자열 + §9 3행), `v1.14` 24회, `Layer C 보류` 0회, `확정(자매 스펙)` 3회, #21~#27 7행, 제목 수 36, 트리 줄 91·98.

- [ ] **Step 1: 앵커 사전 점검 (개정 전 상태 확인)**

```bash
cd /home/mini/github/token-data-pipeline
SPEC=docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md
wc -l "$SPEC" | grep -o "^[0-9]*"
sed -n 3p "$SPEC" | grep -o "현재 버전 v1.13 (2026-07-14)"
grep -c "v1.14" "$SPEC"
grep -c "Layer C 보류" "$SPEC"
grep -c "^## \|^### " "$SPEC"
grep -n "^| 20 |" "$SPEC" | grep -o "^[0-9]*"
grep -n "^## 10. 구현 순서 (권장)$" "$SPEC" | grep -o "^[0-9]*"
for a in "### 4.1 fact DB (수집 원본)" "### 4.3 mart DB (1차 집계)" "## 5. collectors/token-usage" \
         "### 5.3 페이지네이션 불변성 검사 (리뷰 #3 — HIGH)" "### 5.7 파일 구성·환경변수" "## 6. assets" \
         "### 7.3 모니터링" "## 8. mock-provider · 테스트 · 운영" "### 8.4 정정(restatement) 프로토콜 (v1.4)"; do
  printf '%s ' "$(grep -c -F -x "$a" "$SPEC")"
done; echo
git status --porcelain -- "$SPEC" ; echo "(위 출력 없음 = 스펙 미변경 상태)"
```
Expected:
```
814
현재 버전 v1.13 (2026-07-14)
0
3
36
796
798
1 1 1 1 1 1 1 1 1 
(위 출력 없음 = 스펙 미변경 상태)
```
(`grep -c "v1.14"`는 일치 0이면 exit 1을 내지만 출력은 `0` — 그대로 진행. 앵커 9개가 각 1회, §9 마지막 행이 #20, `## 10` 제목이 798행.)

- [ ] **Step 2: 개정 스크립트 실행 (표준입력 히어닥 — 레포에 스크립트 파일을 남기지 않음)**

아래 블록을 **그대로** 실행한다. 스크립트 본문은 `replace_once`(앵커 1회 검증 후 치환)와 `insert_before_heading`(빈 줄 + 제목 앞에 블록 삽입, 블록 앞뒤 빈 줄 1개씩)만 사용하며, 치환·삽입 문자열이 곧 v1.14 개정 본문이다(설계 §8 표의 12행과 1:1).

```bash
cd /home/mini/github/token-data-pipeline
python3 - <<'PY'
"""마스터 스펙 v1.14 개정 — 자매 스펙(2026-08-31) §8 개정 목록을 앵커 치환으로 적용 (Plan 6a T11).

모든 앵커는 파일 내 정확히 1회 존재해야 하며(assert), 이미 적용된 파일(v1.14 문자열 존재)에는 재적용하지 않는다.
"""
from pathlib import Path

SPEC = Path("docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md")
SISTER = "[/v1/metrics 반입 설계 2026-08-31](2026-08-31-token-metrics-ingest-design.md)"

text = SPEC.read_text(encoding="utf-8")
assert "v1.14" not in text, "이미 v1.14가 적용된 파일 — 재적용 금지"


def replace_once(old: str, new: str) -> None:
    global text
    assert text.count(old) == 1, f"앵커 {text.count(old)}회: {old[:70]!r}"
    text = text.replace(old, new)


def insert_before_heading(heading: str, block: str) -> None:
    """빈 줄 + 제목 앞에 블록을 끼워 넣는다(블록 앞뒤 빈 줄 1개씩)."""
    replace_once("\n\n" + heading + "\n", "\n\n" + block + "\n\n" + heading + "\n")


# ---------------------------------------------------------------- 머리말 · §0
replace_once(
    "**현재 버전 v1.13 (2026-07-14)**",
    "**현재 버전 v1.14 (2026-09-04)**",
)
replace_once(
    "\n\n## 1. 배경과 목적\n",
    "\n| v1.14 | 2026-09-04 | **메트릭 싱크 확장 — 자매 스펙 " + SISTER + " 참조**: "
    "`/v1/metrics`(GPU Hour·성능 메트릭) 반입을 §5.9 클론 규칙의 첫 적용 사례로 추가 — "
    "신규 모듈 2개(`collectors/token-metrics/`, `mart/token-metrics/`)·fact 4·gpu_data 5·mart 4 테이블(§4.0 표 확장), "
    "Layer C 스케치를 실제 테이블명·비용 모델 정의서로 확정(§4.4), 시간별 모듈 NOT_READY 번역·소프트 데드라인 예약 산식(§5.2), "
    "마커 라벨 `module=token-metrics`(8줄/일, `slot=/final=`)·`module=mart-metrics`(§5.6/§7.3), 계약 2′·3′·6조 예외·9조 싱크별 데드라인(§5.9), "
    "배포 원칙 zero-diff·독립 배포·`release-images` 분리(§7.2), 운영 도구 rerun 체인 표(§8.3), §9 #12·#13·#14 확정 + #21~#27 신규. "
    "**기존 모듈·테이블·마커는 무변경** — 본 개정은 전부 additive |"
    "\n\n## 1. 배경과 목적\n",
)

# ---------------------------------------------------------------- §3 레포 트리
replace_once(
    "│   #        적재 계약(§5.9) 준수하는 독립 모듈로 클론 생성, 소스 확정 시\n",
    "│   #        적재 계약(§5.9) 준수하는 독립 모듈로 클론 생성, 소스 확정 시\n"
    "├── collectors/token-metrics/   # (v1.14) /v1/metrics 수집기 — §5.9 클론 규칙 적용 사례: app/, ddl/{stage,company}/,\n"
    "│                               # k8s/, build.sh, install.sh, tools/{rerun,manual_load}.py, tests/ (자매 스펙 §5)\n",
)
replace_once(
    "│                               # tests/, tools/rerun.py, warning_messages.md\n",
    "│                               # tests/, tools/rerun.py, warning_messages.md\n"
    "├── mart/token-metrics/         # (v1.14) mart-metrics — §5.9 클론 규칙 적용 사례: app/{batch,mart}.py, ddl/, k8s/,\n"
    "│                               # build.sh, install.sh, tools/rerun.py, tests/ (자매 스펙 §6)\n",
)

# ---------------------------------------------------------------- §4.0 물리 표 + 배칭·예산·장부
insert_before_heading("### 4.1 fact DB (수집 원본)", """**v1.14 메트릭 싱크 확장** (자매 스펙 §4.0 — DDL 매니페스트·P1 항목은 자매 스펙) — 물리 표 신규 행:

| 테이블 | PARTITION BY | ORDER BY | Distributed 샤딩키 | 비고 |
|---|---|---|---|---|
| `fact.raw_token_metrics_gpu_1d` | toYYYYMM(date) | `(date, service, model, gpu_type, category)` | `cityHash64(service)` | P0, TTL 25 MONTH |
| `fact.raw_token_metrics_serving_1d` | toYYYYMM(date) | `(date, service, model, metric, name)` | `cityHash64(service)` | P0, long form |
| `fact.raw_token_metrics_summary_1d` | toYYYYMM(date) | `(date, service)` | `cityHash64(service)` | P0, 앵커(응답당 1행) |
| `fact.collect_audit_metrics_1d` | toYYYYMM(date) | `(date, service, replaced_at)` | `cityHash64(service)` | P0, append-only |
| `gpu_data.dim_token_metrics_service` | (파티션 없음) | `(service)` | `rand()` | P0, 메트릭 레지스트리 |
| `gpu_data.dim_token_model_alias` | (파티션 없음) | `(alias, effective_from)` | `cityHash64(alias)` | P0 |
| `gpu_data.dim_token_gpu_tco` | (파티션 없음) | `(gpu_type, effective_from)` | `cityHash64(gpu_type)` | P0 |
| `gpu_data.dim_token_gpu_allocation` | (파티션 없음) | `(service_group, gpu_type, effective_from)` | `cityHash64(service_group)` | P0-stretch |
| `gpu_data.dim_token_vendor_price` | (파티션 없음) | `(provider, model, tier, effective_from)` | `cityHash64(model)` | P0-stretch |
| `mart.agg_token_model_cost_1d` | toYYYYMM(date) | `(date, service, model)` | `cityHash64(service)` | P0 |
| `mart.token_metrics_check_1d` | toYYYYMM(date) | `(date, service, check_name, model, gpu_type)` | `cityHash64(service)` | P0 |
| `mart.agg_token_model_share_1d` | toYYYYMM(date) | `(date, model, service, provider_service)` | `cityHash64(model)` | P0-stretch |
| `mart.agg_token_gpu_group_1d` | toYYYYMM(date) | `(date, service_group, gpu_type)` | `cityHash64(service_group)` | P0-stretch |

- **`--replace` 2단계 IN 배칭**(rerun·manual): (A) 대상 날짜 전 서비스 fetch/CSV 파싱·normalize·가드 → (B) 테이블당 `_delete_day_in(table, date, services)` 앵커(summary) → gpu → serving → (C) 서비스별 INSERT(gpu → serving → summary 마지막). 정기 경로는 서비스별 순차·INSERT만(뮤테이션 0).
- **모듈별 예산·가드**: 정기 시간별 실행(8슬롯) 뮤테이션 0 / `--replace` 날짜당 fact ≤3 / mart-metrics rerun 날짜당 ≤4(M1·M3·M4·M2). 일 총량 150 안에서 mart-only rerun D ≤ 20일, fact+mart rerun D ≤ 11일(격리 검증 병행 시 D ≤ 2). 실행당 가드 `METRICS_MAX_MUTATIONS_PER_RUN`(수집기, 기본 45 = 3×15)·`MART_METRICS_MAX_MUTATIONS_PER_RUN`(mart, 기본 64 = 4×16) — 첫 DELETE 전 존재확인 선조회로 합산, 초과 시 `FAILURE reason=mutation_budget`. 두 `rerun.py` 모두 **`--chunk-days`(기본 7)** 로 긴 범위를 순차 Job으로 분할. 재수집 창은 10:50 KST 이후(피크 02:00~03:00 회피).
- **뮤테이션 장부**: 경로별 뮤테이션 수 표는 `collectors/token-metrics/ddl/README.md`(자매 스펙 §4.0 표와 동일)에 두고 경로·예산 변경 시 함께 갱신한다.""")

# ---------------------------------------------------------------- §4.2 P0 예외 (대시보드 직접 조회)
insert_before_heading("### 4.3 mart DB (1차 집계)", """**v1.14 P0 예외 — 메트릭 대시보드의 직접 조회** (자매 스펙 §6.2·§8): 메트릭 싱크의 대시보드는 `view_token_*` 복사본 없이 **mart/fact `_dist`를 공유 계정 `mart`로 직접 조회**한다(`mart.agg_token_model_cost_1d`, `mart.token_metrics_check_1d`, `fact.raw_token_metrics_*_1d`). `gpu_data.view_token_metrics_*` 복사본 4종은 P1(§9-1 잔여 항목). 토큰 측 view table 계약과 `created_by` 규칙은 무변경.""")

# ---------------------------------------------------------------- §4.4 Layer C 확정
insert_before_heading("## 5. collectors/token-usage", """**v1.14 Layer C 확정** (자매 스펙 §4·§6.4 — **비용 모델 = [비용 모델 정의서](../../cost-model-spec.md)**, 충돌 시 정의서 우선): 위 스케치 명은 실제 테이블명으로 대체된다 — `fact.model_gpu_usage_1d` → `fact.raw_token_metrics_gpu_1d`(date × service × model × gpu_type × category(serving|standby|test); `gpu_hours`가 비용의 유일한 근거, phase 없음), `gpu_data.dim_gpu_cost` → `gpu_data.dim_token_gpu_tco`(gpu_type × effective_from, `tco_krw_per_gpu_hour` KRW), `gpu_data.dim_model_serving_map` → `gpu_data.dim_token_model_alias`(alias → canonical, effective_from 이력), `mart.model_cost_1d` → `mart.agg_token_model_cost_1d`. **조인 키 = `(date, service, canonical)`** — 토큰 `model`과 메트릭 `model`을 모두 `dim_token_model_alias`로 canonical 정규화한 뒤 결합한다((date, model)만의 결합은 폐기). 비용 모델: C = (serving + standby) × TCO, test·유휴는 서비스 그룹 귀속, 사내 플랫폼 제공 모델의 원가는 소비 서비스에 가중 W(input 1 / cached 0.1 / output 4) 토큰 배분, 사외 API 모델은 벤더 KRW 단가(`dim_token_vendor_price`) — 매핑 세부는 자매 스펙 §6.4. "차지백에 사용하지 않는다" 원칙은 유지.""")

# ---------------------------------------------------------------- §5.2 시간별 모듈 번역
insert_before_heading("### 5.3 페이지네이션 불변성 검사 (리뷰 #3 — HIGH)", """**v1.14 시간별 모듈의 번역** (자매 스펙 §5.2 — §5.9 계약 4조 적용 사례): `collectors/token-metrics`는 하루 8슬롯(`schedule "5 2-9"`, 02:05~09:05 KST)으로 돌며 409를 **비최종 슬롯 `SKIPPED reason=not_ready`(다음 슬롯 재시도) / 최종 슬롯(batch_time KST hour ≥ `FINAL_HOUR_KST`=9) `FAILURE reason=not_ready_at_0900`**으로 번역한다 — 위 표의 "서비스당 누적 대기" 대신 슬롯 재방문이 대기 역할(큐 끝 1회 재방문 min(Retry-After, 300s)은 유지). 소프트 데드라인 안 적재 예약 산식: `SOFT_DEADLINE_MINUTES=40`(2400s = 신규 착수·409 재방문 창 20분 + 예약 적재 예산 `LOAD_BUDGET_S=1200`), 적재 착수 전 `deadline − now < LOAD_BUDGET_S`면 미착수 FAILURE; 불변식 `SOFT×60 > LOAD_BUDGET`은 `test_config.py`로 고정. 슬롯 산식 `startingDeadlineSeconds 540 + activeDeadlineSeconds 3000 + grace 30 = 3570s < 3600s`(`concurrencyPolicy: Forbid`가 다음 슬롯을 건너뛰지 않음). `api_since`/`until` 게이트와 최종 슬롯 판정은 정기 실행에만 적용, `--from/--to`·manual 모드의 409는 `FAILURE reason=not_ready`.""")

# ---------------------------------------------------------------- §5.6 마커 라벨
insert_before_heading("### 5.7 파일 구성·환경변수", """- **v1.14 마커 라벨 추가** (자매 스펙 §5.2·§7.5): `module=token-metrics` — BATCH_RESULT는 **하루 8줄**(슬롯당 1줄, 필드 `slot=HH final=0|1` 추가; 일 상태 = `final=1` 줄, `final=1` 부재 = FAILURE; CronJob `backoffLimit: 0`이라 Job당 시도 1회 = 줄 1개), SERVICE_RESULT에 `source_type=metrics-api-v1|manual-v0`. `module=mart-metrics` — BATCH_RESULT 1줄/실행. 기존 `token-usage`·`mart-token` 마커 문법·필드는 무변경(§7.3 패널 규칙 참조).""")

# ---------------------------------------------------------------- §5.9 계약 적용
insert_before_heading("## 6. assets", """**v1.14 메트릭 싱크의 계약 적용** (자매 스펙 §4.3·§5.2·§5.4 — 클론 규칙의 첫 적용 사례):

- **2′ 멱등성**: 교체 시 **DELETE 순서 summary(앵커) → gpu → serving, INSERT 순서 gpu → serving → summary 마지막**(`insert_distributed_sync=1`, `insert_deduplicate=0`) — 앵커 존재 = 적재 완료. 앵커 없이 자식 행만 남은 부분 적재는 다음 슬롯(date=오늘−1) 또는 운영자 `--from/--to` rerun이 복구하고, mart-metrics는 **앵커가 있는 (date, service)의 자식 행만** 읽는다(잔여물은 M3 `partial_load`).
- **3′ 앵커 행**: `fact.raw_token_metrics_summary_1d`가 summary 행의 역할 — 응답당 정확히 1행, NODATA(rows==0)도 기록, `is_derived` 없음(소스가 항상 응답 단위 확정 데이터).
- **6조 예외**: 메트릭 싱크는 `dim_token_service`에 등록하지 않는다 — **자기 레지스트리 `gpu_data.dim_token_metrics_service`**(`api_since`/`coverage_since`/`until`, 자기 source_type 범위 원자 교체)를 가지며 **토큰 coverage 게이트(mart STEP 0)에 편입되지 않는다**. 메트릭 커버리지 판정은 mart-metrics M3 `metrics_missing`(FAIL).
- **9조 싱크별 데드라인**: 토큰 싱크 T+1 03:30(불변) / 메트릭 싱크 **T+1 10:04**(최종 슬롯 시작 ≤09:14 + `activeDeadlineSeconds` 3000s) → **mart-metrics 10:20**. 토큰 mart(04:00)와 mart-metrics는 독립 스케줄이며 같은 구간 backfill은 토큰 mart 재수행 후 mart-metrics.""")

# ---------------------------------------------------------------- §7.2 배포 원칙
insert_before_heading("### 7.3 모니터링", """**v1.14 배포 원칙 — "새 코드만 새로 배포"** (자매 스펙 §7.5): 사내 분기본이 존재하는 상태에서 신규 모듈을 반입할 때 (1) **기존 모듈 zero-diff** — `collectors/token-usage/**`, `mart/token-usage/**`, `assets/user-org/**`, `assets/model-catalog/`의 기존 파일, `tools/verify/invariants.sql`, `docs/operations/{company-verify,stage-runbook,rerun}.md`, `docs/monitoring/grafana_dashboard_token_usage.json`, `.github/workflows/{release-images,test-collector,test-mart}.yml`, 사내 리소스(`token-usage-ch-secret`, `token-usage-endpoints`, `token-usage-ca-bundle`, `token-usage-collector`, `token-mart-daily`, `token-mart-ch-secret`); (2) **신규 모듈 독립 배포** — 이미지 `token-metrics-collector`·`token-mart-metrics` 2개, 각자 `build.sh company --registry <harbor> --tag <sha7>`/`install.sh company`가 자기 리소스만 생성·갱신, Harbor 반입은 sha7 태그 신규 이미지 2개만, 공유 `registry-pull-secret`은 없을 때만 생성; (3) **`release-images` 분리** — `release-images-metrics.yml` 신규(paths·matrix = 신규 모듈 2개), 기존 `release-images.yml` 무수정(기존 이미지 재빌드 유발 금지). 공유 도구는 additive 등록만(`tools/gen_stage_ddl.py` SOURCES·`tools/gen_verify_ddl.py` MODULES·`run_invariants.py --sql`·`test-assets.yml` paths). 격리 검증(company-verify)은 선택 — 신규 모듈은 기존 테이블에 쓰지 않는다. 롤백 = CronJob 2개 suspend + 신규 테이블 DROP(기존 파이프라인 영향 0).""")

# ---------------------------------------------------------------- §7.3 batch_result 패널 규칙
insert_before_heading("## 8. mock-provider · 테스트 · 운영", """- **v1.14 batch_result 패널 규칙 1건** (자매 스펙 §7.5): `module=token-metrics`는 하루 8줄이며 **일 상태 = `final=1` 줄, `final=1` 부재 = FAILURE**; `module=mart-metrics`는 1줄(일배치 누락 평가창 25h 동일). 사내 batch_result 대시보드의 유일한 변경(레포 밖, 모니터링 소유자 작업 — §9-23); 미확정 시 fallback = mart-metrics M3 `metrics_missing` 패널 + 임시 LogsQL `module=token-metrics final=1 status=FAILURE`. 메트릭 대시보드 안내는 `docs/monitoring/README.md` 신규 절(기존 절 무수정), 대시보드 JSON은 `mart/token-metrics/` 안.""")

# ---------------------------------------------------------------- §8.3 운영 도구
insert_before_heading("### 8.4 정정(restatement) 프로토콜 (v1.4)", """- **v1.14 메트릭 싱크 운영 도구** (자매 스펙 §5.5·§5.6·§6.3·§7.1) — rerun 체인 표:

| 도구 | CronJob | command | 옵션 | 창 |
|---|---|---|---|---|
| `collectors/token-metrics/tools/rerun.py` | `token-metrics-collector` | `python -m app.main --from --to [--service] [--replace]` | `--chunk-days`(기본 7) · `--chain-mart`(→ 아래 mart-metrics rerun, 동일 날짜 그대로 전파) | 10:50 KST 이후 + 활성 `token-mart-metrics` Job 0 |
| `mart/token-metrics/tools/rerun.py` | `token-mart-metrics` | `app.batch` | `--chunk-days`(기본 7) | 10:50 이후; 토큰 mart와 같은 구간 backfill은 토큰 mart 재수행 후 |
| `collectors/token-metrics/tools/manual_load.py` | `token-metrics-collector`(Job 1회) | `python -m app.main --manual-gpu … --manual-serving … [--manual-engine …] --from --to [--replace] [--generated-at]` | `--gpu --serving [--engine] [--replace] [--context --namespace]` — CSV → ConfigMap `token-metrics-manual-<ts>` → Job → 완료 후 ConfigMap 삭제; 템플릿 `docs/templates/token_metrics_manual_v0_{gpu,serving,engine}.csv` | go-live 이전 구간(날짜 제약 없음) |
| `tools/verify/run_invariants.py --sql tools/verify/invariants_metrics.sql` | — | — | additive `--sql`(기본값 = 기존 `invariants.sql`, 무변경) | 배포·rerun 후 |

`--replace`는 rerun·manual 공통 — 기존 앵커(API·manual 불문)가 있으면 `--replace` 없이는 `already_loaded` 스킵(안전 기본값). 뮤테이션 예산·`--chunk-days` 분할은 §4.0 v1.14 항목.""")

# ---------------------------------------------------------------- §9 #12·#13·#14 확정 + #21~#27
replace_once(
    "| Layer C 보류 | 서빙 플랫폼(kserve/vLLM) 운영팀 협의 |",
    "| **확정(자매 스펙)** — 서비스 자기신고 `/v1/metrics` gpu 블록(model × gpuType × category, gpuHours; phase 없음), go-live 이전 구간은 manual-v0 | 자매 스펙 §4.1·§5.5 (v1.14) |",
)
replace_once(
    "| Layer C 보류 | §9-12와 함께 협의 |",
    "| **확정(자매 스펙)** — 메타데이터 시트 `모델` 탭 → `gpu_data.dim_token_model_alias`(alias → canonical, effective_from 이력; 생성기 `assets/model-catalog/sheet_to_dim_token_model_alias_insert.py`) | 자매 스펙 §4.2·§7.2 (v1.14) |",
)
replace_once(
    "| Layer C 보류 | 재무/인프라 정책 확인 |",
    "| **확정(자매 스펙)** — `gpu_data.dim_token_gpu_tco`(gpu_type × effective_from, KRW/GPU·h, basis ∈ depreciation\\|lease\\|power-inclusive\\|tco; 값은 NULL 플레이스홀더 → 비용 NULL 전파, 실값은 admin 시드·생성기 `csv_to_layer_c_dim_insert.py`) | 자매 스펙 §4.2·M1 (v1.14) |",
)
replace_once(
    "\n\n## 10. 구현 순서 (권장)\n",
    "\n| 21 | **(v1.14) GPU 할당 수치 출처** — `dim_token_gpu_allocation`(service_group × gpu_type × effective_from)의 소스·동료 매핑·`utilization`/`over_report` 허용 오차 | 수기 시드(P0-stretch), 오차 ±1원 | GPU 대시보드 소유자 (자매 스펙 M3) |"
    "\n| 22 | **(v1.14) 메타데이터 시트 반입** — 시트·CSV 실파일 보관 경로(레포 밖 + gitignore), owner 회신 반영 절차, 시트 v2 컬럼(`workloadType`·사외 API tier) | 생성기 2종(alias·Layer C)으로 admin INSERT, tier='standard' 고정 | 운영 문서 작성 시 (자매 스펙 M12·M18) |"
    "\n| 23 | **(v1.14) 알림 채널·수신자** — 메트릭 체크(`mart.token_metrics_check_1d`) 위반·`final=1` 부재의 통보 경로 + 사내 batch_result 패널 규칙 ack | 체크 테이블 패널 + 수동 통보; 패널 미확정 시 §7.3 fallback | 모니터링 소유자·온보딩 안내 시 (자매 스펙 M5·M8) |"
    "\n| 24 | **(v1.14) 환율/마진** — 벤더 KRW 단가표 값(provider × model × tier)·환율 기준·토큰 단가 p 표시 기준(기준월·가동률 병기)·가중 W(1/0.1/4)의 TCO 팀 승인 | 플레이스홀더 NULL, 상수 1/0.1/4, '추정' 라벨 유지 | 운영자/재무·정의서 소유자·TCO 팀 (자매 스펙 M2·M17·M21·M22) |"
    "\n| 25 | **(v1.14) 스크랩 교차검증 임계값** — 엔진 `/metrics` 스크랩 값과 자기신고 serving 값의 허용 편차 | P2(미구현) | 케이스 A~C 팀과 협의 (자매 스펙 M10) |"
    "\n| 26 | **(v1.14) `/v1/usage` 소비자 필드** — 플랫폼 제공자 usage 응답에 소비 서비스 귀속 필드를 추가해 배부 정밀화(자매 스펙 §6.4·§7.4 P2); 현재는 레지스트리 `usage_includes_consumers` 플래그 + 가중 W 배분 | P2 — `usageIncludesConsumers` 기본 0(Σ 전 서비스), 다중 제공자는 보류(`provider_ambiguous`) | 플랫폼 제공 팀 확인 + usage 스펙 개정 시 (자매 스펙 M4) |"
    "\n| 27 | **(v1.14) 사내 분기본 ↔ GitHub 동기화** — P1 토큰 mart canonical 등 기존 모듈 변경분의 사내 반영 계획 | 이번 범위 밖(§7.2 v1.14 zero-diff 원칙) | 별도 협의 (자매 스펙 M16) |"
    "\n\n## 10. 구현 순서 (권장)\n",
)

SPEC.write_text(text, encoding="utf-8")
print("v1.14 적용 완료:", SPEC)
PY
```
Expected:
```
v1.14 적용 완료: docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md
```
(앵커가 1회가 아니면 `AssertionError: 앵커 N회: '…'`로 exit 1 — 파일은 쓰지 않는다(치환은 메모리, `write_text`는 마지막). 그 경우 Step 1을 다시 확인하고 스펙 원문이 v1.13 상태인지 `git diff --stat -- docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md`로 확인한다.)

- [ ] **Step 3: 개정 결과 검증 (설계 §8 11행 전부 반영 + 제목·앵커 무변경)**

```bash
cd /home/mini/github/token-data-pipeline
SPEC=docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md
git diff --numstat -- "$SPEC"
sed -n 3p "$SPEC" | grep -o "현재 버전 v1.14 (2026-09-04)"
grep -n "^| v1.14 | 2026-09-04 |" "$SPEC" | grep -o "^[0-9]*"
grep -c "v1.14" "$SPEC"
grep -c "Layer C 보류" "$SPEC"; grep -c "확정(자매 스펙)" "$SPEC"
grep -o "^| 2[1-7] | \*\*(v1.14)" "$SPEC" | tr '\n' ' '; echo
grep -c "^## \|^### " "$SPEC"
grep -n "^├── collectors/token-metrics/\|^├── mart/token-metrics/" "$SPEC" | grep -o "^[0-9]*:├── [a-z/-]*"
grep -c "^\*\*v1.14 \|^- \*\*v1.14 " "$SPEC"
```
Expected:
```
68	4	docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md
현재 버전 v1.14 (2026-09-04)
32
24
0
3
| 21 | **(v1.14) | 22 | **(v1.14) | 23 | **(v1.14) | 24 | **(v1.14) | 25 | **(v1.14) | 26 | **(v1.14) | 27 | **(v1.14) 
36
91:├── collectors/token-metrics/
98:├── mart/token-metrics/
9
```
읽기: `68 4` = 추가 68줄/삭제 4줄(3행 버전 + §9 #12·#13·#14; `grep -c "Layer C 보류"`는 0이라 exit 1이지만 출력 `0`이 정상). `24` = v1.14 언급 수(머리말 1 + §0 행 1 + 트리 2 + 절 블록 9 + §9 확정 3 + 신규 행 7 + §8.3 표 아래 문단의 "§4.0 v1.14 항목" 참조 1). `36` = 제목 수 불변(제목 추가 0). `9` = 절 삽입 블록 수(§4.0·§4.2·§4.4·§5.2·§5.6·§5.9·§7.2·§7.3·§8.3).

추가로 개정문 자체를 눈으로 확인한다(공개 레포 규칙 — 사내 호스트·코드명·이메일 0):
```bash
cd /home/mini/github/token-data-pipeline
git diff -U0 -- docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md | grep "^+" | grep -c -i "harbor\.\|\.svc\|@\|\.co\.kr\|\.com"
```
Expected:
```
0
```
(개정문에는 레지스트리 주소·클러스터 주소·이메일이 없다 — `<harbor>`·`<sha7>` 플레이스홀더만.)

- [ ] **Step 4: 커밋 (docs 1커밋 — 마스터 스펙만)**

```bash
cd /home/mini/github/token-data-pipeline
git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml
# 기대: 출력 없음
git status --porcelain | grep -v "^ M docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md$" ; echo "(위 출력 없음 = 마스터 스펙만 변경)"
git add docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md
git commit -m "docs: 마스터 스펙 v1.14 — 메트릭 싱크 확장 개정 (Plan 6a T11)

- 자매 스펙 2026-08-31 §8 개정 목록 11행 반영: §0 v1.14 행, §3 트리(collectors/mart token-metrics), §4.0 물리 표 13행 + --replace 배칭·예산 45/64·--chunk-days·장부, §4.2 P0 대시보드 직접 조회 예외, §4.4 Layer C 확정(실제 테이블명·조인 키 (date, service, canonical)·비용 모델 정의서), §5.2 8슬롯 NOT_READY 번역·소프트 데드라인 산식, §5.6/§7.3 마커 라벨·패널 규칙, §5.9 2′·3′·6조 예외·9조 데드라인, §7.2 배포 원칙(zero-diff·독립 배포·release-images 분리), §8.3 rerun 체인 표, §9 #12·#13·#14 확정 + #21~#27
- 전부 additive(제목 추가 0, 기존 문단 이동 0) — 삭제 4줄은 버전 문자열·§9 보류 셀 3개 치환분
- 공개 레포 규칙: 사내 주소·코드명·이메일 0 (<harbor>·<sha7> 플레이스홀더)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01EfFw32XY5K7iztKUnyQa54"
```

---

## 6b/6c가 소비하는 인터페이스

Plan 6b(`collectors/token-metrics`)·6c(`mart/token-metrics`)는 아래 이름만 참조한다. 컬럼 순서는 DDL의 선언 순서이며 INSERT는 **컬럼 목록 명시**를 전제로 한다(위치 의존 금지). `_dist`는 `_local`과 동일 컬럼(COMMENT·DEFAULT 없음, CONSTRAINT는 mart만 양쪽).

### A. fact (collectors/token-metrics — 6b가 쓰고 6c가 읽음)

DDL: `collectors/token-metrics/ddl/company/raw_token_metrics.sql`(4테이블 × `_local`/`_dist`), GRANT: `collectors/token-metrics/ddl/company/accounts.sql`. stage 미러 `collectors/token-metrics/ddl/stage/`, 격리 미러 `collectors/token-metrics/ddl/company-verify/`(생성기 전용).

| 테이블 | 컬럼(타입) | ORDER BY / 샤딩키 |
|---|---|---|
| `fact.raw_token_metrics_gpu_1d` | `date Date`, `service_group LowCardinality(String)`, `service LowCardinality(String)`, `model LowCardinality(String)`, `gpu_type LowCardinality(String)`, `category LowCardinality(String)`, `gpu_count Float64`, `gpu_hours Float64`, `flags Array(String)`, `source_type LowCardinality(String)`, `generated_at DateTime('Asia/Seoul')`, `collected_at DateTime('Asia/Seoul')` | `(date, service, model, gpu_type, category)` / `cityHash64(service)` |
| `fact.raw_token_metrics_serving_1d` | `date Date`, `service_group LowCardinality(String)`, `service LowCardinality(String)`, `model LowCardinality(String)`, `metric LowCardinality(String)`, `name String DEFAULT ''`, `unit LowCardinality(String)`, `p50 Nullable(Float64)`, `p90 Nullable(Float64)`, `p95 Nullable(Float64)`, `p99 Nullable(Float64)`, `flags Array(String)`, `source_type LowCardinality(String)`, `generated_at DateTime('Asia/Seoul')`, `collected_at DateTime('Asia/Seoul')` | `(date, service, model, metric, name)` / `cityHash64(service)` |
| `fact.raw_token_metrics_summary_1d` (앵커, 응답당 1행) | `date Date`, `service_group LowCardinality(String)`, `service LowCardinality(String)`, `reported_service_group String`, `reported_service String`, `engine_type LowCardinality(String) DEFAULT ''`, `engine_version String DEFAULT ''`, `gpu_rows UInt32`, `serving_rows UInt32`, `custom_rows UInt32`, `rejected_rows UInt32`, `merged_dups UInt16`, `source_type LowCardinality(String)`, `generated_at DateTime('Asia/Seoul')`, `collected_at DateTime('Asia/Seoul')` | `(date, service)` / `cityHash64(service)` |
| `fact.collect_audit_metrics_1d` (append-only, `--replace` 감사) | `date Date`, `service LowCardinality(String)`, `prev_generated_at DateTime('Asia/Seoul')`, `prev_collected_at DateTime('Asia/Seoul')`, `prev_source_type LowCardinality(String)`, `prev_gpu_rows UInt32`, `prev_gpu_hours_sum Float64`, `prev_serving_rows UInt32`, `replaced_at DateTime('Asia/Seoul')` | `(date, service, replaced_at)` / `cityHash64(service)` |

공통: `PARTITION BY toYYYYMM(date)`, `TTL date + INTERVAL 25 MONTH`. 도메인(6b normalize가 보장): `category ∈ serving|standby|test`, `metric ∈ ttft_ms|itl_ms|output_tps|e2e_ms|custom`(API 키 `ttftMs|itlMs|outputTps|e2eMs|custom`에서 변환), `unit`은 `ms|tokens/s|<custom 단위 ≤32>`, `source_type ∈ metrics-api-v1|manual-v0`, `flags`는 문자열 배열(gpu: `hours_over_count|unknown_violation|dup_merged`, serving: `pct_non_monotone|unknown_violation|dup_model_kept_first|dup_custom_kept_first`; 빈 배열이 정상). GRANT(`accounts.sql`, `TO mart`): fact 4테이블 `_dist`·`_local` SELECT, INSERT + `ALTER DELETE`는 gpu/serving/summary `_local` 3개만(audit는 SELECT·INSERT); `gpu_data.dim_token_metrics_service` `_dist`/`_local` SELECT, INSERT + `_local` ALTER DELETE; `gpu_data.dim_token_service_dist` SELECT(6b 프리플라이트의 토큰 레지스트리 대조). 뮤테이션 장부: `collectors/token-metrics/ddl/README.md`(정기 0 / `--replace` 날짜·서비스당 ≤3 / `METRICS_MAX_MUTATIONS_PER_RUN=45`).

### B. 메트릭 레지스트리 (gpu_data — 6b가 원자 교체, 6c가 읽음)

DDL: `collectors/token-metrics/ddl/company/dim_token_metrics_service.sql`. 시드 없음(6b `install.sh company`가 `endpoints-metrics.company.yaml`에서 적재 — 파일은 gitignore 25행).

| 테이블 | 컬럼(타입) | ORDER BY / 샤딩키 |
|---|---|---|
| `gpu_data.dim_token_metrics_service` | `service_group LowCardinality(String)`, `service LowCardinality(String)`, `base_url String`, `enabled UInt8`, `api_since Date`, `coverage_since Date`, `until Nullable(Date)`, `expect_gpu UInt8 DEFAULT 1`, `expect_serving UInt8 DEFAULT 1`, `usage_includes_consumers UInt8 DEFAULT 0`, `note String DEFAULT ''`, `updated_at DateTime('Asia/Seoul')` | `(service)` / `rand()` |

endpoints 파일 키(YAML, camelCase → 컬럼 snake_case): `serviceGroup, service, baseUrl, enabled, apiSince, coverageSince, until, expectGpu, expectServing, usageIncludesConsumers` — 합성 예시 `assets/model-catalog/fixtures/synthetic_endpoints_metrics.yaml`(T8 `--services` 입력, 6b 테스트 fixture로 재사용 가능).

DEFAULT는 `_local` 전용 — INSERT의 유일 허용 경로인 `_dist`에는 DEFAULT가 없으므로(GRANT INSERT는 `_dist`만) 컬럼을 빠뜨리면 `expect_gpu`/`expect_serving`이 1이 아니라 0, `note`는 ''가 된다. 6b registry sync는 12컬럼 전부(`expect_gpu, expect_serving, usage_includes_consumers, note` 포함)를 명시 INSERT 한다(계약).

### C. mart (mart/token-metrics — 6c가 쓰고 대시보드가 읽음)

DDL: `mart/token-metrics/ddl/company/mart_metrics_tables.sql`(4테이블), GRANT: `mart/token-metrics/ddl/company/accounts.sql`. 전 테이블 `created_by LowCardinality(String)` + `CONSTRAINT check_created_by CHECK created_by != ''`(`_local`·`_dist` 양쪽), `PARTITION BY toYYYYMM(date)`, `TTL date + INTERVAL 25 MONTH`.

| 테이블 | 컬럼(타입) | ORDER BY / 샤딩키 |
|---|---|---|
| `mart.agg_token_model_cost_1d` (M1) | `date Date`, `service_group LowCardinality(String)`, `service LowCardinality(String)`, `model LowCardinality(String)`, `serving_gpu_hours Float64`, `standby_gpu_hours Float64`, `test_gpu_hours Float64`, `flagged_gpu_hours Float64`, `equiv_gpu_count Float64`, `scaled_intraday UInt8 DEFAULT 0`, `model_cost_krw Nullable(Float64)`, `input_tokens UInt64`, `cache_read_tokens UInt64`, `cache_creation_tokens UInt64`, `output_tokens UInt64`, `requests UInt64`, `uncached_tokens UInt64`, `cached_tokens UInt64`, `total_tokens UInt64`, `weighted_tokens Float64`, `tokens_per_gpu_hour Nullable(Float64)`, `gpu_type_mix Array(String)`, `model_registered UInt8`, `tco_missing UInt8`, `has_token_rows UInt8`, `has_gpu_rows UInt8`, `quality_flag LowCardinality(String)`, `created_by LowCardinality(String)` | `(date, service, model)` / `cityHash64(service)` |
| `mart.token_metrics_check_1d` (M3) | `date Date`, `service_group LowCardinality(String)`, `service LowCardinality(String)`, `check_name LowCardinality(String)`, `model LowCardinality(String) DEFAULT ''`, `gpu_type LowCardinality(String) DEFAULT ''`, `severity LowCardinality(String)`, `observed Nullable(Float64)`, `threshold Nullable(Float64)`, `detail String DEFAULT ''`, `source_type LowCardinality(String) DEFAULT ''`, `created_by LowCardinality(String)` | `(date, service, check_name, model, gpu_type)` / `cityHash64(service)` |
| `mart.agg_token_model_share_1d` (M4, stretch) | `date Date`, `model LowCardinality(String)`, `service LowCardinality(String)`, `service_group LowCardinality(String)`, `provider_service LowCardinality(String)`, `is_provider UInt8`, `denominator_mode LowCardinality(String)`, `service_wtokens Float64`, `model_total_wtokens Float64`, `share Nullable(Float64)`, `model_cost_krw Nullable(Float64)`, `allocated_cost_krw Nullable(Float64)`, `quality_flag LowCardinality(String)`, `created_by LowCardinality(String)` | `(date, model, service, provider_service)` / `cityHash64(model)` |
| `mart.agg_token_gpu_group_1d` (M2, stretch) | `date Date`, `service_group LowCardinality(String)`, `gpu_type LowCardinality(String)`, `allocated_gpu_hours Nullable(Float64)`, `group_total_cost_krw Nullable(Float64)`, `serving_gpu_hours Float64`, `standby_gpu_hours Float64`, `test_gpu_hours Float64`, `reported_gpu_hours_total Float64`, `flagged_gpu_hours Float64`, `model_cost_sum_krw Nullable(Float64)`, `test_cost_krw Nullable(Float64)`, `idle_gpu_hours Nullable(Float64)`, `idle_cost_krw Nullable(Float64)`, `unattributed_cost_krw Nullable(Float64)`, `identity_gap_krw Nullable(Float64)`, `utilization Nullable(Float64)`, `over_report UInt8`, `equiv_gpu_count Float64`, `tco_missing UInt8`, `allocation_source LowCardinality(String)`, `quality_flag LowCardinality(String)`, `created_by LowCardinality(String)` | `(date, service_group, gpu_type)` / `cityHash64(service_group)` |

GRANT(`mart/token-metrics/ddl/company/accounts.sql`, `TO mart`): mart 4테이블 `_dist` SELECT, INSERT + `_local` ALTER DELETE; 읽기 SELECT — `gpu_data.dim_token_{model_alias,gpu_tco,gpu_allocation,vendor_price,metrics_service,service}_dist`, `fact.raw_token_metrics_{gpu,serving,summary}_1d_dist`, `mart.token_usage_1d_dist`, `mart.agg_token_service_1d_dist`; `CREATE TEMPORARY TABLE ON *.*`; `system.mutations` SELECT. `created_by='token-metrics-pipeline'` 고정(설계 §6.1) — 6c `steps.py`가 모든 INSERT에 명시(DEFAULT 없음, `CHECK created_by != ''`; 불변식 `created_by_wrong_metrics`가 이 값을 검사). DDL 컬럼 COMMENT에 `;`가 없으므로 6c e2e의 `sql.split(";")`(`--` 주석 줄 제거 후)로 이 파일과 `raw_token_metrics.sql`을 그대로 나눌 수 있다(T2 lint 보장). 뮤테이션: 정기·rerun 날짜당 ≤4(M1·M3·M4·M2), `MART_METRICS_MAX_MUTATIONS_PER_RUN=64`, `--chunk-days 7`.

### D. 기준정보 dim (assets/model-catalog — admin 적용, 6c가 읽음)

DDL: `assets/model-catalog/ddl/company/dim_token_{model_alias,gpu_tco,gpu_allocation,vendor_price}.sql`, GRANT: `assets/model-catalog/ddl/company/accounts_metrics.sql`(4테이블 `_dist` SELECT `TO mart` — 기존 `accounts.sql` 무수정), 사내 시드: `assets/model-catalog/ddl/company/seed_dim_token_{model_alias,gpu_tco,gpu_allocation,vendor_price}.sql`, stage 합성 시드: `assets/model-catalog/fixtures/stage_seed_dim_token_{model_alias,gpu_tco,gpu_allocation,vendor_price}.sql`(생성기 밖, 수동 적용). 파티션·TTL 없음.

| 테이블 | 컬럼(타입) | ORDER BY / 샤딩키 | 사내 시드 플레이스홀더 행 |
|---|---|---|---|
| `gpu_data.dim_token_model_alias` | `alias String`, `effective_from Date`, `canonical String`, `defining_service LowCardinality(String) DEFAULT ''`, `source LowCardinality(String) DEFAULT 'metadata-sheet'`, `note String DEFAULT ''` | `(alias, effective_from)` / `cityHash64(alias)` | `('unknown', 2026-01-01, 'unknown', '', 'seed', …)` 1행 — identity(정규화 불가 모델을 `unknown`으로 유지) |
| `gpu_data.dim_token_gpu_tco` | `gpu_type String`, `effective_from Date`, `tco_krw_per_gpu_hour Nullable(Float64)`, `currency LowCardinality(String) DEFAULT 'KRW'`, `basis LowCardinality(String) DEFAULT ''`, `note String DEFAULT ''` | `(gpu_type, effective_from)` / `cityHash64(gpu_type)` | `('unknown', 2026-01-01, NULL, 'KRW', '', …)` + `H100/A100/H200/L40S`(2026-01-01, NULL, 'KRW', '') 4행 = 5행 — 값 전부 NULL(비용 NULL 전파, `tco_missing=1`) |
| `gpu_data.dim_token_gpu_allocation` | `service_group LowCardinality(String)`, `gpu_type String`, `effective_from Date`, `allocated_gpu_count Nullable(Float64)`, `source LowCardinality(String) DEFAULT 'manual'`, `note String DEFAULT ''` | `(service_group, gpu_type, effective_from)` / `cityHash64(service_group)` | `('unknown', 'unknown', 2026-01-01, NULL, 'seed', …)` 1행 |
| `gpu_data.dim_token_vendor_price` | `provider LowCardinality(String)`, `model String`, `tier LowCardinality(String) DEFAULT 'standard'`, `effective_from Date`, `krw_per_mtok_input Nullable(Float64)`, `krw_per_mtok_cached Nullable(Float64)`, `krw_per_mtok_cache_creation Nullable(Float64)`, `krw_per_mtok_output Nullable(Float64)`, `note String DEFAULT ''` | `(provider, model, tier, effective_from)` / `cityHash64(model)` | `('unknown', 'unknown', 'standard', 2026-01-01, NULL×4, …)` 1행 |

DEFAULT(`defining_service ''`, `source 'metadata-sheet'`, `currency 'KRW'`, `basis ''`, `source 'manual'`, `tier 'standard'`, `note ''`)는 `_local` 전용 — 시드·생성기·admin 수동 INSERT는 전부 `_dist` 경유이므로 모든 컬럼을 명시한다(T6 시드·T8/T9 생성기 출력의 컬럼 목록은 이미 전 컬럼; 6c는 읽기만). 시드 검증은 설계 §4.2의 6종 전부(`service_not_in_registry` 포함 — `gpu_data.dim_token_metrics_service_dist` 대조, collectors DDL 선적용 전제 = README 적용 순서 2단계 → 시드 4단계). stage 합성 fixture(`fixtures/stage_seed_dim_token_*.sql`)는 시드 **이후** 적용하며 실값 행은 `effective_from 2026-08-01`(`unknown` 행만 2026-01-01 — 시드 키와 비충돌).

6c 조회 규약(설계 §6.1 — 이 플랜이 고정한 키): 이력 조회는 `effective_from <= date`인 행 중 `max(effective_from)`(`argMax`) — `2026-01-01` 플레이스홀더 행은 항상 가장 오래된 이력이라 실값 행이 자동으로 우선한다. canonical 정규화 `canon(model, date)` = `dim_token_model_alias`에서 `alias = model AND effective_from <= date`의 최신 `canonical`, 없으면 `model` 그대로(등록 여부는 `model_registered`). `unknown`·NULL 플레이스홀더는 6c가 "미확정"으로 취급 — `eff_alloc`은 `gpu_type != 'unknown'`만, TCO NULL → `tco_missing=1`·`model_cost_krw` NULL, 벤더 단가 NULL → 비용 NULL(설계 §6.1 공통 CTE `eff_alias/eff_tco/eff_alloc`, `canon(x) = if(a.canonical = '', x, a.canonical)`). 시드·생성 SQL 검증 SELECT의 4열 계약 `check_name, key, effective_from, cnt`(`-- 검증: 결과가 비어야 정상` 앵커 뒤) — 6c `invariants_metrics.sql`은 같은 형식으로 dim 무결성 검사를 추가할 수 있다.

### E. 생성기 CLI (레포 밖 실값 → gitignore SQL → admin)

| 생성기 | 시그니처 | 입력 CSV 헤더 | 출력·검증 |
|---|---|---|---|
| `assets/model-catalog/sheet_to_dim_token_model_alias_insert.py` | `python3 sheet_to_dim_token_model_alias_insert.py --csv <모델탭.csv> --services <endpoints*.yaml> [--services …] [--effective-from YYYY-MM-DD] [--out dim_token_model_alias_insert.sql] [--chunk-size 500] [--target-db gpu_data\|token_verify_dim]` | `canonical,aliases,defining_service,effective_from,note`(`aliases`는 쉼표 구분, 빈 값 = identity 행만) | canonical마다 identity 행(alias=canonical, defining_service='') + alias 행; 검증 6종 `dup_key, alias_maps_to_two_canonicals, alias_loop, empty_canonical, missing_identity_row, service_not_in_registry`(마지막은 `gpu_data.dim_token_metrics_service_dist` 대조); exit 0/1(SheetError)/2(argparse) |
| `assets/model-catalog/csv_to_layer_c_dim_insert.py` | `python3 csv_to_layer_c_dim_insert.py --table gpu_tco\|gpu_allocation\|vendor_price --csv <파일> [--effective-from YYYY-MM-DD] [--out dim_token_<table>_insert.sql] [--chunk-size 500] [--target-db gpu_data\|token_verify_dim]` | gpu_tco `gpu_type,effective_from,tco_krw_per_gpu_hour,basis,note` / gpu_allocation `service_group,gpu_type,effective_from,allocated_gpu_count,source,note` / vendor_price `provider,model,tier,effective_from,krw_per_mtok_input,krw_per_mtok_cached,krw_per_mtok_cache_creation,krw_per_mtok_output,note`(선택 컬럼 `currency`는 ''/'KRW'만) | 파일 내 검증 `empty_key, unknown_reserved, bad_number, negative_value, currency_krw, basis_domain, tier_domain, effective_from_is_placeholder_date, dup_key`; SQL 검증 = 시드와 동일 항목(gpu_tco `dup_key, unknown_row_state, basis_domain, currency_krw` / gpu_allocation `dup_key, unknown_row_state, negative_count` / vendor_price `dup_key, unknown_row_state, tier_domain`); exit 0/1(LayerCError)/2 |

공통 규칙: `effective_from` 빈 값 → `--effective-from`(둘 다 없으면 오류), `2026-01-01`(시드 플레이스홀더 키) 금지, 키 값 `unknown` 금지, 자동 교정 없음(strip만), 출력은 `NOT IN` 가드 + `SETTINGS insert_distributed_sync = 1;` + 검증 앵커, stdout/stderr에 데이터 원문 미출력(행 번호·필드명·건수만), 결정적 출력(타임스탬프 없음). 기본 `--out` 파일명은 전부 gitignore 26~28행에 걸린다. 테스트: `cd assets/model-catalog && python3 -m pytest -q` → 134 passed(T2 매니페스트 45 + T8 alias 35+3 + T9 Layer C 48+3 — 최종 리뷰 fix 커밋의 인코딩·--out 테스트 6).

### F. 수기(manual-v0) 템플릿 (6b `manual_load.py`·`--manual-*` 파서 계약)

경로: `docs/templates/token_metrics_manual_v0_{gpu,serving,engine}.csv`. 파서 규칙: **`#`로 시작하는 줄은 건너뛴다**(주석 안의 쉼표 무시), 첫 비주석 줄이 헤더(바이트 동일 요구), 빈 셀 = 부재, 인코딩 UTF-8, 날짜 `YYYY-MM-DD`(KST).

| 파일 | 헤더 | 규칙(스펙 §3 — 6b normalize가 검증) |
|---|---|---|
| `token_metrics_manual_v0_gpu.csv` | `date,service,model,gpuType,category,gpuCount,gpuHours` | `category ∈ serving\|standby\|test`, `model=unknown`은 `category=test`만, `gpuHours ≤ gpuCount × 24`, `gpuCount > 0`, `gpuHours ≥ 0` |
| `token_metrics_manual_v0_serving.csv` | `date,service,model,metric,name,unit,p50,p90,p95,p99` | `metric`은 **API 키** `ttftMs\|itlMs\|outputTps\|e2eMs\|custom`(fact `metric` 변환은 normalize); `ttftMs/itlMs/e2eMs`는 p50·p90·p95·p99 전부 필수, `outputTps`는 p50만, `custom`은 `name`(≤64)·`unit`(≤32) 필수 + p-키 ≥1 |
| `token_metrics_manual_v0_engine.csv` | `service,engine_type,engine_version` | 선택 파일(`--manual-engine`); `engine_type ≤64`, `engine_version ≤64`(빈 값 허용) → summary `engine_type/engine_version` |

실제 제출 파일은 `*manual_metrics*.csv`로 저장(gitignore 24행) — 템플릿 파일명은 패턴 밖이라 커밋된다. 수기 적재의 `source_type='manual-v0'`, `generated_at`은 `--generated-at`(없으면 적재 시각).

### G. `.gitignore` 패턴 (T1 — 18~30행)

```
# 설계 2026-08-31 §7.2 (Plan 6a): 메타데이터 시트·Layer C 실값·수기 CSV·생성 SQL·사내 endpoints 반입 금지
*metadata*.xlsx
*metadata*.csv
*gpu_tco*.csv
*gpu_allocation*.csv
*vendor_price*.csv
*manual_metrics*.csv
endpoints-metrics.company.yaml
dim_token_model_alias_insert*.sql
dim_token_gpu_*_insert*.sql
dim_token_vendor_price_insert*.sql
alert_routing*.json
assets/model-catalog/data/
```
커밋되는 합성 파일은 패턴을 피한 이름을 쓴다: `synthetic_model_sheet.csv`, `synthetic_layer_c_{tco,allocation,price}.csv`, `synthetic_endpoints_metrics.yaml`, `token_metrics_manual_v0_*.csv`. 6b의 사내 endpoints는 `collectors/token-metrics/endpoints-metrics.company.yaml`(25행), 6c의 알림 라우팅 실파일은 `alert_routing*.json`(29행) 이름을 쓴다.

### H. 공유 도구 등록 상태 (T7)

- `tools/gen_stage_ddl.py` SOURCES: 신규 14항목(collectors 3·mart 2·assets 9) → `ddl/stage/` 미러 14파일. `python3 tools/gen_stage_ddl.py --check` = 25파일 OK.
- `tools/gen_verify_ddl.py` MODULES: `collectors/token-metrics`·`mart/token-metrics` 추가(assets는 glob) → `ddl/company-verify/` 미러 14파일. 격리 DB명: `token_verify_fact`/`token_verify_dim`/`token_verify_mart`(기존 규칙), 프리펜드는 파일명이 정확히 `accounts.sql`일 때만(`accounts_metrics.sql`은 GRANT 치환만).
- `.github/workflows/test-assets.yml`: push·pull_request paths 각 +4(동일 18항목), 잔존 grep 디렉터리 +2, job `unit-model-catalog`(`defaults.run.working-directory: assets/model-catalog`, `python -m pytest tests/ -v --ignore=tests/e2e` — 러너의 `python`; 로컬 재현은 `python3`). 6b·6c는 자기 워크플로(`test-collector-metrics.yml`, `test-mart-metrics.yml`, `release-images-metrics.yml`)를 새로 만든다 — 기존 3개 무수정.
- 6c 몫으로 남긴 additive: `tools/verify/run_invariants.py --sql`(기본값 = 기존 `invariants.sql`), `tools/verify/invariants_metrics.sql` 신규, `docs/monitoring/README.md` 신규 절. 6b 몫: `tools/mock-provider/**`(`/v1/metrics` 추가).

## 완료 기준 (Plan 6a)

- [ ] T1~T11 커밋 12개(T7 = tools + ci 2커밋)가 `feat/token-metrics-schema` 브랜치에 순서대로 존재하고, 각 커밋 메시지가 `type(scope): … (Plan 6a Tn)` + 트레일러 2줄 규약을 지킨다. 그 앞에 T1 Step 0의 전제 커밋 1(플랜 문서 3종 반입 — 분기 전 설계 브랜치)이 있다.
- [ ] `git diff --stat main -- collectors/token-usage mart/token-usage assets/user-org tools/verify/invariants.sql docs/operations docs/monitoring/grafana_dashboard_token_usage.json .github/workflows/release-images.yml .github/workflows/test-collector.yml .github/workflows/test-mart.yml` 출력 없음(zero-diff). `assets/model-catalog/`의 기존 파일(`ddl/company/{dim_token_model,seed_dim_token_model,accounts}.sql`, `ddl/stage/*` 기존 4, `ddl/company-verify/*` 기존 3, `README.md`, `ddl/README.md`)도 `git diff --stat main --` 출력 없음.
- [ ] 신규 DDL 매니페스트 14파일 존재: `collectors/token-metrics/ddl/company/{raw_token_metrics,dim_token_metrics_service,accounts}.sql`, `mart/token-metrics/ddl/company/{mart_metrics_tables,accounts}.sql`, `assets/model-catalog/ddl/company/{dim_token_model_alias,dim_token_gpu_tco,dim_token_gpu_allocation,dim_token_vendor_price,seed_dim_token_model_alias,seed_dim_token_gpu_tco,seed_dim_token_gpu_allocation,seed_dim_token_vendor_price,accounts_metrics}.sql` + README 2(`collectors/token-metrics/ddl/README.md`, `mart/token-metrics/ddl/README.md`).
- [ ] `python3 tools/gen_stage_ddl.py --check`·`python3 tools/gen_verify_ddl.py --check` 모두 exit 0(25파일), 미러 28파일이 생성기 출력과 바이트 동일.
- [ ] `cd assets/model-catalog && python3 -m pytest -q` → `134 passed`(매니페스트 lint 45 + alias 생성기 38 + Layer C 생성기 51 — 최종 리뷰 fix 커밋 +6). `python3 -m py_compile` 대상 2파일 OK. CI `test-assets.yml`의 `unit-model-catalog`·`verify-ddl` green.
- [ ] 시드 4파일·stage fixture 4파일 모두 3요소(헤더·`NOT IN` 가드·`-- 검증: 결과가 비어야 정상` 앵커) + 4열 검증 계약; 사내 시드에 `unknown`·NULL 외 수치 0건(`grep -c "toNullable(" assets/model-catalog/ddl/company/seed_dim_token_model_alias.sql assets/model-catalog/ddl/company/seed_dim_token_gpu_tco.sql assets/model-catalog/ddl/company/seed_dim_token_gpu_allocation.sql assets/model-catalog/ddl/company/seed_dim_token_vendor_price.sql` 각 0 — 기존 `seed_dim_token_model.sql`의 4건은 대상 밖).
- [ ] `.gitignore` 18~30행 존재, `git check-ignore -q` 양성 목록(실파일명 예시 13줄) exit 0·음성 목록(합성 fixture·템플릿) exit 1. `git status --porcelain --untracked-files=all | grep -vE '^(A|M)  '` 출력 없음(추적 외 파일 0).
- [ ] 템플릿 3파일 헤더가 설계 §5.5와 바이트 동일, 예시 행은 합성값(Mock Service A/B)만.
- [ ] 마스터 스펙 v1.14: Step 3 검증 출력과 동일(`68 4`, v1.14 24회, `Layer C 보류` 0, 제목 수 36, #21~#27).
- [ ] 공개 레포 규칙: `git grep -n -i -E "harbor\.[a-z]+\.(co\.kr|com)|@[a-z]+\.(co\.kr|com)" | grep -v "noreply@anthropic.com"` 출력 0줄(커밋 트레일러의 `noreply@anthropic.com`만 제외)(`harbor.example.internal`·`chi-<cluster>.<ns>.svc` 플레이스홀더만); 사내 프로젝트 코드명 0건.
- [ ] draft PR 생성(제목 `feat: 메트릭 싱크 스키마·기준정보 (Plan 6a) — DDL 리뷰 요청`), 본문에 (a) 매니페스트 14파일 목록, (b) T5·T6 중간 커밋의 `verify-ddl` red 허용 사유(T7에서 green), (c) `## 6b/6c가 소비하는 인터페이스` 링크, (d) fact/gpu_data/mart 소유자 리뷰 요청·사인오프 목표 9/8 오전.

## Self-Review 노트

### 설계 해석 (설계가 명시하지 않아 이 플랜이 확정한 것 — 6b/6c·리뷰어가 이의 시 여기서 고친다)

1. **타입 규약**(T3·T4·T5): gpu 시간 합 `Float64`, 비용·비율 `Nullable(Float64)`, 토큰·요청 카운트 `UInt64`, 가중 토큰 `Float64`, 0/1 플래그 `UInt8`, 분류 문자열 `LowCardinality(String)`, 자유 문자열 `String DEFAULT ''`, summary 행 카운트 `UInt32`·`merged_dups UInt16`, dim 키 문자열은 `String`(alias·gpu_type·model — 카디널리티 상한 불명), 분류성 dim 컬럼(`service_group`·`provider`·`tier`·`basis`·`currency`·`source`)은 `LowCardinality(String)`.
2. **`accounts_metrics.sql`**(T5): 기존 `assets/model-catalog/ddl/company/accounts.sql`은 zero-diff라 dim 4테이블 GRANT를 별도 파일로 둠. `gen_verify_ddl.py`의 격리 DB·계정 프리펜드는 파일명이 정확히 `accounts.sql`일 때만 붙으므로 `accounts_metrics.sql` 미러는 GRANT 치환만 — 격리 검증 순서는 `assets/…/company-verify/accounts.sql`(기존, 프리펜드 포함) → `accounts_metrics.sql`.
3. **검증 SELECT 4열 계약** `check_name, key, effective_from, cnt`(T6·T8·T9): dim_holiday 시드의 3요소 패턴을 따르되 복합 키는 `concat(a, '/', b)`로 `key` 1열에 합친다 — `UNION ALL`이 컬럼 수·타입 일치를 요구하기 때문.
4. **플레이스홀더 키 날짜 `2026-01-01`·예약어 `unknown`**: 시드는 `effective_from=2026-01-01`·키 `unknown`만 쓰고, 두 생성기는 그 날짜·그 값을 **금지**한다(`effective_from_is_placeholder_date`, `unknown_reserved`) — `NOT IN` 가드가 플레이스홀더 행과 충돌해 실값이 무음 skip 되는 사고 방지. `gpu_tco` 시드의 `H100/A100/H200/L40S` NULL 행도 같은 날짜라 실값 CSV는 반드시 소급 시작일(예: 2026-08-26)을 쓴다. stage 합성 fixture(T6)도 같은 이유로 실값 행을 `2026-08-01`에 두고(`unknown` 행만 2026-01-01) 시드 **이후** 적용한다 — `test_stage_fixture_exists_and_is_synthetic`가 코드 라인의 `toDate('2026-01-01')`은 `'unknown'` 행에만 있음을 단언.
5. **alias 생성기의 identity 행 자동 생성**(T8): 시트 `모델` 탭은 canonical 행에 alias만 적으므로 생성기가 canonical마다 identity 행(alias=canonical, defining_service='')을 항상 만든다 — 6c `canon()`이 등록 모델을 `model_registered=1`로 판정하는 근거. `defining_service`는 alias 행에서만 필수이며 레지스트리(endpoints `service:` 키)와 바이트 동일해야 함.
6. **Layer C CSV 헤더 = 컬럼명 그대로**(T9): 설계 §4.2가 컬럼만 정의하므로 CSV 헤더는 컬럼명과 동일하게 하고 `currency`는 선택 컬럼(있으면 ''/'KRW'만) — 통화 KRW 고정 원칙을 파일 형식으로도 강제. 숫자 리터럴은 `CAST(x AS Nullable(Float64))`로 출력해 NULL 행과 UNION ALL 타입이 일치하게 함. 할당 철회는 삭제가 아니라 `allocated_gpu_count=0` 신규 이력 행.
7. **serving 템플릿 `metric` = API 키**(T10): 헤더가 이미 API camelCase(`gpuType,gpuCount,gpuHours`)이므로 `metric`도 `ttftMs|itlMs|outputTps|e2eMs|custom`을 쓰고 fact 이름(`ttft_ms` 등) 변환은 6b normalize 한 곳에서만 한다. engine 템플릿은 선택 파일.
8. **stage 합성 시드는 fixtures/에, 생성기 밖**(T6): `gen_stage_ddl.py`는 company 시드를 그대로 stage로 미러하므로(플레이스홀더만) 합성 수치가 필요한 stage 시드는 `assets/model-catalog/fixtures/stage_seed_*.sql`로 두고 stage runbook 절차에서 수동 적용(6b/6c의 stage 검증 전제).
9. **T7 중간 커밋의 CI red 허용**: T5·T6 시점에는 `assets/model-catalog/ddl/company-verify/` 미러가 없어 `gen_verify_ddl.py --check`가 exit 1 — T7까지 한 PR로 묶어 PR head 기준 green을 판정한다.
10. **마스터 스펙 v1.14 삽입 위치**(T11): 설계 §8 "§4.2/§9-1"은 §4.2 절 끝에만 적고 §9 #1은 무수정; "§5.6/§7.3"은 두 곳 분리; §9 #26 `/v1/usage` 소비자 필드 = 자매 스펙 §6.4·§7.4의 P2(`usageIncludesConsumers` 기본 0); 제목 추가 0(앵커·목차 무변경).
11. **테스트 실행 명령**: 로컬 bash 블록은 전부 `python3 -m pytest` / `python3 -c`(이 플랜 작성·실행 환경: `python` 부재, Python 3.10.12, pytest 9.0.2 — `which python`이 비어 `python -m pytest`는 그대로 실행되지 않는다). CI `test-assets.yml`의 job(`unit`·`unit-model-catalog`)만 러너(`setup-python`)가 제공하는 `python -m pytest`를 쓴다 — 출력은 동일.
12. **`git check-ignore`는 `-q`만**(T1·T9·T10): git 2.34.1에서 `-v -q` 조합은 fatal — 양성/음성 판정은 exit code로만, 패턴 출처는 별도 `-v` 호출로 본다. 추적 외 파일 판정은 `git status --porcelain --untracked-files=all`(디렉터리 접기 방지).
13. **커밋 단위**: DDL(T3·T4·T5)은 모듈별 `ddl` 커밋, 시드+fixture(T6)·생성기(T8·T9)·템플릿(T10)·스펙(T11)은 각 1커밋, T7은 생성기 등록+미러(`chore(tools)`)와 CI(`ci(ci)`) 2커밋 — 총 12. 그 앞에 T1 Step 0의 전제 커밋(미추적 플랜 문서 3종 반입, `docs`) 1개가 분기 전 설계 브랜치에 있어 이후 모든 `git status` 기대값이 clean tree 전제로 성립한다.

### 설계 절별 커버리지

| 설계 절 | 반영 Task |
|---|---|
| §4.0 매니페스트 14파일·뮤테이션 장부·컨벤션 | T2(lint) · T3(fact 4 + 레지스트리 + accounts + README 장부) · T4(mart 4 + accounts + README) · T5(dim 4 + accounts_metrics) · T6(시드 4) |
| §4.1 fact 컬럼·ORDER BY·앵커 | T3 |
| §4.2 dim 컬럼·effective_from 규약·KRW 고정·시드 플레이스홀더 | T5 · T6 · T8 · T9 |
| §4.3 레지스트리 `dim_token_metrics_service`·endpoints-metrics 형식 | T3 · T8 fixture(`synthetic_endpoints_metrics.yaml`) · T1(gitignore 25행) |
| §5.2 (마커 라벨·슬롯) | T11(v1.14 §5.6) — 코드는 6b |
| §5.5 수기 템플릿 헤더 | T10 |
| §5.6 (rerun·manual 도구) | T11(v1.14 §8.3) — 코드는 6b |
| §6.1 mart 컬럼·created_by | T4 |
| §6.3 (`--sql`·invariants_metrics) | 6c 몫 — H절에 경계 명시 |
| §7.1 미러 생성기 등록 | T7 |
| §7.2 데이터 경계(gitignore)·시트→alias 생성기·Layer C 생성기 | T1 · T8 · T9 |
| §7.5 zero-diff·additive 목록 | Global Constraints · 각 커밋 Step의 `git diff --stat main` 검사 · 완료 기준 |
| §8 마스터 스펙 v1.14 11행 | T11 |
| §10 일정 | `## 일정 재기준 (2026-09-06 기준)` |

### 리뷰 반영

리뷰 라운드 1: 15건 반영, 1건 기각(사유: #12 "시드 검증 5종(service_not_in_registry 제외)" 문구 추가는 #2(시드·fixture에 6번째 검증 SELECT 추가)와 상충 — #2를 적용해 시드가 6종을 전부 내므로 "5종 제외" 문장은 거짓이 됨; 대신 인터페이스 D·T6 시드 헤더에 6종 + 레지스트리 적용 순서 의존을 명시). 반영 목록: model_registered 정의(alias dim identity 히트, dim_token_model 미조회), alias 시드·fixture 6번째 검증 + SEEDS 6종, 로컬 `python3`, 커밋 12 + T1 Step 0 전제 커밋, `MergeTree`(괄호 없음), README 검증 라인(T3·T4), COMMENT `;` 제거 3곳 + Global Constraint + lint 단언, `created_by='token-metrics-pipeline'` 고정, 인터페이스 H CI 문구, `_dist` 샤딩키 개행 허용 문구, GRANT 주석 출처(collectors accounts.sql 49-50행), fixture 실값 행 2026-08-01 + 헤더 + lint 단언, 생성기 `생성 완료: {out_path.name}`, 인터페이스 B/D DEFAULT `_local` 전용 주의.

### 기계 검사 결과 (플랜 작성 시점)

- `wc -l` / `grep -n "^### Task"`: Task 1~11 제목 11개, 본문 순서 = File Structure의 Task 순서.
- ```` ```python ```` 블록 전부 `ast.parse()` OK, ```` ```yaml ```` 블록 전부 `yaml.safe_load()` OK(작성 환경 PyYAML 6.0.3). T11의 개정 스크립트는 bash 히어닥(`python3 - <<'PY'`) 안에 있어 별도로 추출해 `ast.parse()` OK.
- 금지어 grep(오케스트레이터 지정 6패턴 — 미정 표시어·"나중에"·"적절히"·"경계 사례"·"Task N과 유사" 계열, 대소문자 무시): 본문 0건. 이 노트 자체가 패턴 문자열을 담지 않도록 서술형으로 적었다.
- 플랜의 코드 블록을 작성 환경의 레포 사본에 추출해 실행: T2 45 → T8 80 → T9 128 passed, 생성기 `--check` 25파일 OK, `.gitignore` 양성 12/음성 목록 exit 확인, T11 스크립트 dry-run `68 4`·제목 36·v1.14 24회 재현.
- 타입 일관성: 인터페이스 절(A~D)의 컬럼·타입은 T3·T4·T5 DDL 본문과 동일 문자열에서 추출(수기 재타이핑 없음).
