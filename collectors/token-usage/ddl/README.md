# collectors/token-usage DDL — 초안 (동료 리뷰용)

> **상태: 협의용 초안** — 이슈 #1 항목 5(DDL 사전 공유 요청)에 대한 응답.
> 스펙 v1.6 §4.0~4.2·§7.2·§8.4 기준. 아직 어느 환경에도 적용하지 않음.

## 파일

| 파일 | 내용 |
|---|---|
| `company/raw_token_usage.sql` | `fact` DB + 수집 원본 3테이블 (raw 상세 / summary / 교체 감사) — local+dist 쌍 |
| `company/dim_token_service.sql` | `gpu_data.dim_token_service` — 서비스 레지스트리 (이슈 #1 확정: gpu_data 공유) |
| `company/accounts.sql` | 공유 계정 `mart`(동료 소유, 계정 공유 합의 2026-07-14 — 이슈 #1) 앞 **테이블 레벨** GRANT (CREATE USER 없음) |

## 확정된 결정 (2026-07-13, 소유자 협의)

1. **fact DB 공유 확정** — 전용 token_fact 안을 폐기하고 기존 `fact` DB에 `raw_token_usage_*`·
   `collect_audit_1d`를 둔다. 우리 계정 GRANT는 테이블 레벨 한정 유지.
2. **`gpu_data.dim_token_service`로 확정** — `dim_token_*` 접두사 규칙 채택
   (사유는 dim_token_service.sql 헤더 주석 참조 — 공유 DB 내 충돌 예방 + 소유 식별).
   gpu_data에 만드는 토큰 파이프라인 테이블 전부(view_token_usage_* 포함)가 이 규칙을 따른다.
3. **정례 뮤테이션 예산 (확정)** — 동료 파이프라인 실측: 기존 정례 뮤테이션은
   시간당 snap 배치 6건(dim 3테이블 × 2배치) + 일배치 mart 11건 ≈ **일 ~155건**이 이미 운영 중.
   토큰 파이프라인 추가분은 일 ~68건(수집 30서비스×2테이블 + mart 8테이블)으로 기존의 절반 이하.
   확정 예산: **일 총량 150건 / 피크 창(02:00~03:00) 80건** — 상세 테이블이 일 단위 파티션이라
   뮤테이션당 재작성 범위가 동료의 월 파티션 케이스보다 작고, mutations_sync=2 직렬 실행이라
   큐 적체 없음. 초과 시 대응: rerun IN 배칭(설계 반영됨)·no-op 스킵 확대.
   → 2026-07-13 확정 적용 (스펙 §4.0 반영) — 소유자 사후 컨펌 진행 중 (이슈 #1).

## 환경 방침

stage(홈랩)와 company의 CHI/클러스터명이 동일(`gpu-monitoring`)하므로 **단일 DDL 세트로 시작**
(동료 레포의 stage='metrics'와 다른 지점 — 스펙 §7.2 환경 전제). 토폴로지 차이(1s1r vs 2s2r)는
ReplicatedMergeTree의 `{shard}/{replica}` 매크로가 흡수. 환경별 차이가 실제로 생기면 그때
`ddl/stage/`를 분리한다.

## 적용 순서 (스펙 §7.2 — DDL 실행 주체 분리)

1. `accounts.sql`의 `CREATE DATABASE`(fact DB 생성 포함)·`CREATE USER`·GRANT는 **admin 수동 실행**
   (company에서는 클러스터 소유자 협의 후). `CHANGE_ME_*` 비밀번호는 실행 전 치환.
2. 테이블 DDL(`raw_token_usage.sql`, `dim_token_service.sql`)은 install.sh 자동 적용 대상.
3. 이후 스키마 변경은 `migrate_add_*.sql` 관행 (GRANT 추가 포함).

## 이 초안에 없는 것 (후속 Plan에서)

- mart·view 테이블 DDL과 해당 GRANT → Plan 3 (mart)
- `gpu_data.dim_token_user_org`·`dim_token_model` → Plan 4 (assets)
- 대시보드용 별도 GRANT는 불필요 — 공유 계정 `mart`가 이미 view 테이블 read 권한을
  가짐(계정 공유 결정, 2026-07-14). 잔여 리스크는 스펙 §7.2/§9-1/§9-3 참조
