# gpu-data-pipeline 분석 보고서

> 작성일: 2026-07-10
> 대상: `yamatoeru/gpu-data-pipeline` (로컬 클론: `/home/mini/github/gpu-data-pipeline`)
> 목적: 회사 token data 수집 파이프라인 설계를 위해 동료의 파이프라인 구조·작업방식을 파악

---

## 1. 한눈에 보기

GPU Analytics 플랫폼의 **데이터 파이프라인 모노레포**. ClickHouse가 중앙 저장소이고,
모든 배치는 **Docker 이미지 + K8s CronJob**으로 실행된다. 중앙 오케스트레이터(Airflow 등) 없이
**cron 스케줄 오프셋으로 배치 간 의존성을 표현**하는 단순한 구조.

```text
A. Fact/Raw 수집:  telegraf/dcgm-exporter/node_exporter → VictoriaMetrics
                     └─► collectors/*  ──► ClickHouse fact.raw_*_1m (매 1분)
B. Dimension 동기화: PostgreSQL/내부API/CSV
                     └─► assets/*      ──► ClickHouse fact.if_*_ddz(raw) → dim_* (매시간)
C. Mart 생성:       raw_* + dim_*
                     └─► mart/*        ──► ClickHouse mart_*/fact_table_* (매일 새벽)
```

| 계층 | 역할 | 주기 | 대표 출력 |
|---|---|---|---|
| `collectors/` | 시계열 fact 수집 (VictoriaMetrics → CH) | 매 1분 | `fact.raw_gpu_util_1m` |
| `assets/` | 기준정보 동기화 (PG/API → CH) | 매시간 | `fact.*_snapshot_history`, `gpu_data.dim_*` |
| `mart/` | fact+dim 조합 분석 배치 | 매일 02:00~06:00 KST | `mart.fact_job_gpu_usage_1m`, `agg_*_1m/_1d` |

- **모듈 = 배포 단위**: 각 모듈이 자기 디렉터리에 `main.py/batch.py + Dockerfile + requirements.txt + build.sh + install.sh + ddl/ + k8s/ + tools/rerun.py + README.md`를 자기완결적으로 보유. 공유 모듈 없이 clone-and-modify.
- **환경 3단계**: `dev`(kind 로컬) / `stage`(홈랩, ghcr.io) / `company`(사내 Harbor + K8s). DDL·kustomize overlay·스크립트 기본값 모두 환경별 분리.
- 개발 기간 약 3개월(2026-04-05~07-10), 399커밋, 사실상 1인 개발 + Claude Code 페어(커밋 76%에 Co-Authored-By: Claude).

## 2. 계층별 상세

### 2.1 collectors/ — 분 단위 fact 수집

- 구성: `gpu-util`, `system-util` 두 수집기. 각각 4파일 고정 구성 —
  `main.py`(인터벌 계산·오케스트레이션) / `victoria.py`(PromQL 조회·파싱) / `clickhouse_client.py`(INSERT/DELETE) / `config.py`(환경변수).
- **스테이트리스 인터벌 격자 멱등성**: 워터마크 테이블 없이 매분 CronJob이 떠서 "직전 완료된 1분 구간"만 수집. ts를 인터벌 경계로 floor해 재수집 시 정확히 같은 ts를 갖게 함.
- **스케줄 시각 역산**: K8s Job 이름 suffix(스케줄시각/60 인코딩, Downward API)로 Pod 기동이 늦어도 올바른 period 계산 (gpu-util만).
- **재수집**: `main.py --from/--to [--purge]`. `--purge`는 "실제 데이터를 얻은 period만" `ALTER TABLE <local> DELETE` 후 INSERT — 원본 없는 구간은 건드리지 않아 유실 방지. `mutations_sync=2`로 삭제 완료 대기 후 INSERT.
- **OOM/part 폭증 방지**: 60인터벌 또는 2만 행마다 flush (Pod limit 1Gi 기준).
- **유실 방지 우선**: purge 삭제 실패해도 적재는 진행(중복 감수), 인터벌 하나 실패가 전체를 중단시키지 않고 오류 카운트 후 exit 1.
- 값 검증: NaN/0~100 범위 밖 제거 + PromQL `< 101` 필터 이중 방어, 중복 시리즈 평균 dedup.
- CronJob 정책: `* * * * *` + `concurrencyPolicy: Replace` + `backoffLimit: 0` + `startingDeadlineSeconds: 50` (밀리면 다음 주기가 대체).

### 2.2 assets/ — 시간 단위 dimension 동기화

- 구성: `crms`(PG→PG 인벤토리 전체 교체), `gdash`/`pcms`(PG→CH). **sync/snap 2단 분리**가 핵심:
  - **sync** = 소스 → CH raw(`fact.if_*_ddz`) **증분 append** (대상 테이블 `max(if_seq)`를 워터마크로 사용)
  - **snap** = raw → 스냅샷 이력(`fact.*_snapshot_history`, ReplacingMergeTree + argMax dedup) → `dim_*` 갱신
- 스케줄 체이닝: `:00`(crms, rm_gpu sync) → `:05`(gdash sync) → `:30`(rm_gpu snap) → `:35`(gdash snap).
- **전용 워터마크 테이블** `fact.batch_watermark`(ORDER BY batch_name): 스냅샷+dim 갱신 **전체 성공 시에만 갱신** → 실패 시 다음 실행이 같은 구간 재처리.
- **dim 전량 교체 프로토콜**: 후보 SELECT 사전 검증(키 중복/기간 중첩) → `ALTER DELETE WHERE environment='DSCloud'`(범위 한정) + mutation 폴링 → INSERT SELECT → count 검증 → 실패 시 역순 재적재. `environment` 컬럼으로 멀티 소스가 한 dim 테이블 공유.
- **SCD2 이력 생성**: `cityHash64` state_signature + `lagInFrame` 변경 감지 + 누적합 구간화 + `leadInFrame` end_time + expired_reason 분류 (`sync_gdash_snap.py:244-425`).
- **소실 엔터티 감지**: 현재 상태만 주는 소스에서 사라진 VM에 `del_yn='y'` 합성 행 삽입해 종료 시점 이력화.
- CronJob 정책: `Forbid` + `backoffLimit: 2` + `activeDeadlineSeconds: 1800`.

### 2.3 mart/ — 일 단위 분석 배치

- 구성: `s2job`(Slurm job × GPU metric), `ds`(VM 스냅샷 × GPU metric), `aip`(외부 REST API). 세 배치가 공용 `gpu_data.fact_table_unit/project`에 `created_by` 태그로 공존.
- **2파일 골격**: `mart.py` = DB I/O 없는 순수 변환(dataclass + 순수 함수, 단위테스트 대상) / `batch.py` = ClickHouse I/O·오케스트레이션. 일 집계는 `batch_1d.py` 별도(같은 이미지, CronJob command override).
- **멱등성 표준 시퀀스**: `ALTER TABLE <t>_local ON CLUSTER DELETE WHERE toDate(ts)=target_date [AND created_by='<소스>']` → `system.mutations` 폴링(`wait_for_mutations`, 3초 간격/300초 타임아웃) → INSERT. DELETE가 비동기라 대기 없이 INSERT하면 중복 발생.
- **시간 이원화**: `batch_time`(실행 시각, 스냅샷 신뢰 상한) vs `target_date`(=batch_time−1일, 조회·DELETE 범위).
- **1d 집계는 1m agg 롤업 금지** — fact에서 직접 재계산 (중복 카운트/가중치/quota 스케일 오류 회피).
- 메모리: 하루치를 1시간 청크로 나눠 조회→변환→즉시 INSERT.
- 분산 CH 방어 3종: `CH_CLUSTER` 빈 값 경고, INSERT 후 count 재시도 검증, 레플리카 race 시 Python-side join.
- SQL 관리: 배치 SQL은 Python 모듈 상수 + ClickHouse 네이티브 파라미터 바인딩(`{d:Date}`), 독립 SQL(DDL/seed/검증)은 `.sql` 파일. 템플릿 엔진 없음.

## 3. 공통 규약 (전 계층)

- **테이블**: `<이름>_local`(Replicated*MergeTree) + `<이름>`/`_dist`(Distributed, cityHash64 샤딩) 쌍. INSERT는 _dist로(`insert_distributed_sync=1`), DELETE는 _local + ON CLUSTER로. `PARTITION BY toYYYYMM(ts)` + TTL(raw 3개월/스냅샷 12개월/fact 6개월/agg 13개월). 네이밍: `raw_*`/`if_*_ddz`/`*_snapshot_history`/`dim_*`/`fact_*`/`agg_*_{1m,1d}`.
- **타임존 규율**: 전 코드 KST-aware datetime(naive 금지), CH 컬럼 `DateTime('Asia/Seoul')`, cron 주석에 KST 환산 병기.
- **BATCH_RESULT 로그 마커**: 모든 배치가 종료 시 `BATCH_RESULT status=SUCCESS|NODATA|FAILURE module=<이름> elapsed=.. rows=..` 한 줄을 stdout에 출력 (SUCCESS=exit 0, NODATA=WARN/exit 0, FAILURE=exit 1) → VictoriaLogs 수집 → Grafana LogsQL로 실패 알림(5m)/NODATA 경고/누락 감지(주기별 평가창 5m/70m/25h). 배치 결과 테이블 없음 — 로그가 관측 계층.
- **설정 = 환경변수 only**: `CH_HOST/CH_PORT/CH_USER/CH_PASSWORD/CH_DATABASE/CH_CLUSTER` 공통 규약, envFrom secretRef. `CH_CLUSTER` 빈 값이면 ON CLUSTER 생략(dev 단일노드).
- **DDL도 코드와 함께**: `ddl/{dev,stage,company}/` 환경별 3벌 + 계정/GRANT(`accounts.sql`) + `migrate_add_<컬럼>.sql`(이슈번호·local→dist 순서 주석, `ADD COLUMN IF NOT EXISTS`로 멱등) + `validation.sql`(상비 검증 쿼리 런북).
- **created_by/flag 컬럼**: 공용 테이블에서 소스별 부분 멱등성(`DELETE WHERE created_by='x'`), dim 행 단위 파이프라인 참여 스위치(flag=0 제외).

## 4. 배포/운영/검증

- **배포 = 로컬 수동 2단계** (CI/CD 자동 배포 없음): `./build.sh <env>` → `./install.sh <env>`.
  - build.sh: dev=`docker build`+kind load(태그 `dev`), stage/company=`buildx --platform linux/amd64`+push(태그 git short SHA), 사내는 Harbor 프록시 `BASE_IMAGE` 치환.
  - install.sh: 대화형 Secret 생성(멱등 apply + y/N 확인) → CH 파드 자동 탐색(`chi-*`) 후 `kubectl cp`+`clickhouse-client`로 DDL 적용 → kustomize overlay apply → `kubectl set image/env`로 이미지·VM_URL·CH_HOST·CH_CLUSTER 주입.
  - 인자 규약 통일: positional `<dev|stage|company>` + `--registry/--tag/--context/--namespace`. company는 필수값 강제(실수 방지 가드).
- **Dockerfile 단일 패턴**(9개 전부 동일): `ARG BASE_IMAGE=python:3.12-slim` → requirements 선복사 캐시 → `CMD ["python","<entry>.py"]`. 스크립트 여러 개면 이미지 1개 + CronJob command 교체.
- **재수행 표준화**: 모듈마다 `tools/rerun.py` — `kubectl create job --from=cronjob --dry-run -o json`으로 스펙 뜨고 command override → Pod 로그 스트리밍 → 완료 폴링. 날짜범위형(mart)과 1회 트리거형(watermark 기반 assets) 두 유형.
- **검증 3단 분담**:
  - **CI**(GitHub Actions, s2job만): ClickHouse 24.8 실컨테이너 → 단순화 스키마 → Python seed 생성기 → `batch.py` 실제 실행 → `verify_expected_results.sql --expect-empty`(불일치 행 나오면 실패). path 필터로 해당 모듈 변경 시만 트리거.
  - **stage 런북**: 실클러스터 특성(Replicated ZK 블록 해시, 비동기 뮤테이션, 권한) 수동 검증. 단계마다 '의미' 설명 + 성공 로그 원문 + 에러→원인→조치.
  - **company 스팟체크**: `inspect_*.sql`(agg↔fact 재계산 비교, 품질 체크 UNION ALL "출력 없으면 정상").
- 단위테스트: 순수 로직은 경계값 unittest, I/O는 수제 Fake client 주입, SQL은 clickhouse-local 통합테스트(`@skipUnless`). `warning_messages.md`로 모든 WARNING의 의미·대응 문서화.

## 5. 작업 방식 (git 히스토리 분석)

- **개발 순서**: collectors(4월 초) → 모노레포 3계층 재구조화+s2job+CI(04-12) → mart 고도화 → collector 하드닝 스프린트(이슈 하나에 fix 8연타) → mart/aip → assets 정식화(설계문서→코드→README 같은 날) → 운영 하드닝 → 모니터링+PR 전환(06-28~). **수집부터 만들고 데이터를 쌓으면서 mart를 뒤에 붙이는 순서.**
- **커밋 컨벤션**: conventional commits 81% — `type(모듈scope): 설명 (#이슈번호)`. fix 43% > feat 19% > docs 10% — "일단 배포하고 운영에서 발견된 문제를 잘게 fix"하는 스타일. 본문은 문제→원인→변경 구조로 상세히.
- **feat → fix(하드닝) → docs 3연속 소형 PR 사이클**: 예) 재수집 기능 #129 구현 → 코드리뷰 후속 하드닝 #131 → README #132 → OOM fix #133 → --purge #134 (하루 안에). 리뷰 지적을 feat PR에 욱여넣지 않고 별도 fix PR로 분리.
- **워크플로 진화**: 초기 탐색기는 main 직접 push + GitHub 이슈 추적 → 안정화 후(#115~) `type/kebab-case` 브랜치 + 1~3파일 소형 PR.
- 테스트는 기능과 동시가 아니라 하드닝 국면에 사후 추가. adversarial code review로 하드닝하는 커밋 존재. revert도 해시 명시로 3회.
- Claude Code 페어 개발(커밋 76%), 설계 문서(`docs/design-*.md`: 개요→DDL 전문→소스 스키마 표→로직 의사코드→환경변수 표→미결사항 표) 선행 사례 있음.

## 6. 토큰 파이프라인에 가져갈 것 (우선순위)

1. **3계층 책임 분리 + 모듈=배포단위 구조** 그대로: `collectors/`(토큰 사용량 수집) / `assets/`(모델·팀·API키·요금표 dim) / `mart/`(비용·사용량 집계).
2. **BATCH_RESULT 로그 마커를 첫 모듈부터**: 마커 규약만 지키면 기존 Grafana batch_result 대시보드·알림에 무수정 편입 (`docs/monitoring/batch_result_grafana.md`).
3. **멱등성을 처음부터 설계**: 이 레포는 사후에 고통스럽게 추가했음(#60 fix 8연타, #129~#134). 수집=인터벌 격자+purge-then-insert, 동기화=워터마크(전체 성공 시만 갱신), mart=DELETE(created_by 한정)→wait_for_mutations→INSERT.
4. **재수행(rerun) UX를 스펙에 포함**: `--from/--to/--purge` CLI + `tools/rerun.py` + `docs/operations/rerun.md`.
5. **템플릿으로 복사할 파일**: `mart/s2job/build.sh`(가장 깔끔, 99줄), `mart/ds/batch.py`+`mart.py`(소규모 골격), `wait_for_mutations()`, `fact.batch_watermark` DDL, `tools/rerun.py`, CronJob 스펙 결정표(분=Replace/backoff0, 시간·일=Forbid+deadline), `.github/workflows/test-mart-s2job.yml`, `warning_messages.md`·RUNBOOK 양식.
6. **ClickHouse 분산 노하우**: `_local`/`_dist` 쌍, `insert_distributed_sync=1`, `mutations_sync=2`, ZK 블록 해시 함정(TRUNCATE vs ALTER DELETE), INSERT 후 count 재시도.
7. **시간 규율**: KST-aware 통일 + `DateTime('Asia/Seoul')` + batch_time/target_date 이원화.

### 반면교사 (피할 것)

- **공유 모듈 없는 코드 복제**: `wait_for_mutations` 등이 6곳+에 복붙 — 토큰 파이프라인은 처음부터 공용 유틸 모듈화 검토.
- **README 선언 vs 구현 불일치**: MongoDB·dept-files는 README에만 존재. "구현됨/수동 도구/계획"을 구분 표기할 것.
- **테스트-코드 불일치 방치**: rm_gpu_snap 테스트가 리팩터링된 함수를 mock하는 사례.
- `csv-to-ch`(전 컬럼 Nullable(String)·멱등성 없음·대화형)는 일회성 탐색 전용으로 경계 문서화.
- install.sh 대화형이라 무인 배포 불가 — 사내망 수동 배포 전제. 토큰 파이프라인에서 자동화 필요하면 개선 지점.

## 7. 필독 파일 (읽는 순서 추천)

1. `README.md`, `docs/RESPONSIBILITIES.md` — 계층 철학
2. `collectors/metric/gpu-util/main.py` + `README.md` — 수집기 골격, 시간 처리
3. `assets/pcms/rm_gpu_snap/sync_rm_gpu_snap.py` + `ddl/tables.sql` + `ddl/validation.sql` — sync/snap·워터마크·dim 교체
4. `mart/s2job/batch.py` + `mart.py` + `README.md` — mart 골격, wait_for_mutations
5. `mart/s2job/build.sh` + `install.sh` — 배포 스크립트 원본
6. `docs/monitoring/batch_result_grafana.md`, `docs/operations/rerun.md` — 관측·재수행
7. `mart/s2job/ddl/stage/RUNBOOK.md`, `.github/workflows/test-mart-s2job.yml` — 검증 체계
8. `docs/design-rm-gpu-ds.md` — 설계 문서 양식 (토큰 파이프라인 설계서 템플릿)

---

*상세 원본 분석(계층별 6개 리포트)은 세션 스크래치패드에서 생성되어 본 문서로 통합됨.*
