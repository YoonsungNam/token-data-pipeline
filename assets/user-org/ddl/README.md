# assets/user-org DDL 초안 (Plan 4)

스펙 v1.10 §4.2/§6.1 기준. `gpu_data.dim_token_user_org` (`_local` ReplicatedMergeTree +
`_dist` Distributed) + 공유 계정 `mart` 읽기 GRANT.

## 협의 지점 (소유자 리뷰 요청)

1. **네이밍**: `dim_token_user_org` — dim_token_service 때 확정한 `dim_token_*` 규칙의
   적용입니다(스펙 §4.2의 무접두사 표기는 v1.11에서 정리 완료).
   특히 `dim_model`류 범용 이름은 충돌 위험이 커서 접두사가 안전합니다. 이견 있으시면
   말씀 주세요.
2. **gpu_data 신규 테이블 1종** + 공유 계정 `mart` SELECT GRANT (dim_token_service 때와 동일 절차).
3. **쓰기 주체**: 전용 계정 없이 **admin 수동**(이력 append는 사내 절차 투입·리뷰,
   §6.1 1단계 — 빈도 낮음 + 개인정보 테이블 최소 권한). 2단계 sync CronJob(§9-2) 도입 시
   전용 계정 신설.

## 데이터 경계 (§7.2 — 절대 규칙)

- **실로스터 CSV·생성 INSERT SQL은 레포·사외 환경 취급 금지** — .gitignore 선제 패턴
  (`assets/user-org/data/`, `*roster*.csv`, `dim_user_org_insert*.sql`, `dim_token_user_org_insert*.sql`).
- stage(사외 홈랩)에는 **합성 로스터만** (fixtures/ — mock user-#### 체계).
- anonymous 매핑 행은 `user_name`에 **비실명 핸들명** 저장을 허용 (2026-07-14 개정 — 이전
  "빈 문자열 강제" 규칙 완화). 실명 기입 금지는 사내 투입 리뷰에서 확인(도구는 판별 불가,
  §6.1). 대시보드 표기 경로는 mart/view의 `user_name` 컬럼(anonymous 행만 — identified/
  unclassified는 빈 문자열, §9-1 보류, §4.2/§4.3).

## 적용 순서

0. **배포 순서 주의**: mart dim 참조 개명(Plan 4 T1 — 이 브랜치에 포함)과 이 DDL은 함께
   배포한다 — 개명 이전 mart 이미지가 남아 있으면 STEP 1이 UNKNOWN_TABLE로 즉사한다.
1. 테이블 DDL(`dim_token_user_org.sql`) + `accounts.sql` GRANT — admin 수동.
2. 로스터 투입: `csv_to_dim_user_org_insert.py`(Plan 4 도구)로 CSV → INSERT SQL 생성
   → 사내 리뷰 → admin 실행 (생성 SQL에 사전 검증·멱등 가드·말미 count 검증 포함).
3. 갱신 = 새 effective_from 행 append (기존 행 불변). 이력 정정 시 해당 기간
   **mart rerun 의무** (docs/operations/rerun.md).
