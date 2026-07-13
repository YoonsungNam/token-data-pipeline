# collectors/token-usage DDL — 초안 (동료 리뷰용)

> **상태: 협의용 초안** — 이슈 #1 항목 5(DDL 사전 공유 요청)에 대한 응답.
> 스펙 v1.6 §4.0~4.2·§7.2·§8.4 기준. 아직 어느 환경에도 적용하지 않음.

## 파일

| 파일 | 내용 |
|---|---|
| `company/raw_token_usage.sql` | `token_fact` DB + 수집 원본 3테이블 (raw 상세 / summary / 교체 감사) — local+dist 쌍 |
| `company/dim_service.sql` | `gpu_data.dim_service` — 서비스 레지스트리 (이슈 #1 확정: gpu_data 공유) |
| `company/accounts.sql` | 계정 3종(`token_collector`/`token_mart`/`token_dashboard_reader`) + **테이블 레벨** GRANT |

## 협의가 필요한 결정 지점 (§9-18 — 이 초안의 기본값)

1. **fact DB**: 초안은 **전용 DB `token_fact`** 사용. 이유: 기존 `mart` 계정이 `mart.*`에
   DB 레벨 광역 권한(DROP TABLE 포함)을 갖고 있어, 같은 패턴이 `fact`에도 있다면 상호 정리
   스크립트의 사정권을 분리하는 게 안전하다고 판단. **기존 `fact` DB 공유가 더 낫다면 이
   파일에서 DB명만 치환하면 됨** — 그 경우 기존 계정들의 `fact.*` 권한 범위 확인 필요.
2. **테이블 이름 접두사**: `gpu_data.dim_service`는 범용 이름 — gpu_data에 추가하실 미래
   테이블과 충돌 우려가 있으면 `dim_token_service` 등 접두사로 변경 가능.
3. **정례 뮤테이션**: 수집이 (date, service) 단위 delete-then-insert라 서비스 30개 기준
   일 ~60건 (기존 행 없으면 스킵, rerun은 IN 배칭). 허용 수준인지 확인 요청.

## 환경 방침

stage(홈랩)와 company의 CHI/클러스터명이 동일(`gpu-monitoring`)하므로 **단일 DDL 세트로 시작**
(동료 레포의 stage='metrics'와 다른 지점 — 스펙 §7.2 환경 전제). 토폴로지 차이(1s1r vs 2s2r)는
ReplicatedMergeTree의 `{shard}/{replica}` 매크로가 흡수. 환경별 차이가 실제로 생기면 그때
`ddl/stage/`를 분리한다.

## 적용 순서 (스펙 §7.2 — DDL 실행 주체 분리)

1. `accounts.sql`의 `CREATE DATABASE`·`CREATE USER`·GRANT는 **admin 수동 실행** (company에서는
   클러스터 소유자 협의 후). `CHANGE_ME_*` 비밀번호는 실행 전 치환.
2. 테이블 DDL(`raw_token_usage.sql`, `dim_service.sql`)은 install.sh 자동 적용 대상.
3. 이후 스키마 변경은 `migrate_add_*.sql` 관행 (GRANT 추가 포함).

## 이 초안에 없는 것 (후속 Plan에서)

- mart·view 테이블 DDL과 해당 GRANT → Plan 3 (mart)
- `gpu_data.dim_user_org`·`dim_model` → Plan 4 (assets)
- `token_dashboard_reader`의 실효 GRANT는 view 테이블 생성 시(Plan 3) 부여 — 계정만 선정의
