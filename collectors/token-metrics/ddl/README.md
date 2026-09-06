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
