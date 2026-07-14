# assets/user-org

조직별 사용자 로스터 관리 모듈 (스펙 §6.1). `gpu_data.dim_token_user_org` 이력 테이블에
사내 조직 구조 변화(부서 개편, 팀 이동, 입퇴사)를 반영한다.

## 역할 분담

- **Stage 1 (수동 투입)**: `csv_to_dim_user_org_insert.py` 도구로 CSV 로스터 → INSERT SQL 변환 후
  사내 리뷰·admin 실행.
- **Stage 2 (자동 동기화)**: CronJob 기반 자동 sync — §9-2 미결(프로토타입 논의 중).

## CSV 계약 (§6.1)

| 열 | 필수 | 형식 | 의미 |
|---|---|---|---|
| `user_id` | ✓ | 문자열 | 시스템 사용자 ID (고유 식별자) |
| `user_name` | ✓ | 문자열 | 표시명 (실명) — `anon-*` user_id는 공백 강제 |
| `org` | ✓ | `A>B>C` | 조직 경로 (계층 분리자 `>`). 빈 세그먼트 금지 |
| `is_active` | ✗ | 0 or 1 | 활성 상태 (기본 1) |
| `effective_from` | ✗ | YYYY-MM-DD | 발효 시점 (기본 §6.1 정책 기준일) |

## 사내 투입 절차

1. **CSV 로스터 작성** — 상기 계약 준수하여 `data/` 또는 별도 위치에서 준비.
   - 모든 현직 사용자 포함.
   - 이전 투입 로스터의 차분 또는 전체 재구성 (절차는 조직 운영팀 결정).

2. **INSERT SQL 생성** — `csv_to_dim_user_org_insert.py` 실행:
   ```bash
   python3 assets/user-org/csv_to_dim_user_org_insert.py \
       --csv data/org_members_2026-07.csv \
       --effective-from 2026-07-15 \
       --out data/insert_20260715.sql
   ```
   - `--csv`: 입력 로스터 경로.
   - `--effective-from`: 발효 기준일 (YYYY-MM-DD). CSV에 없으면 적용.
   - `--out`: 출력 SQL 파일 (기본: `dim_token_user_org_insert.sql`).
   - `--chunk-size`: INSERT 청크 크기 (기본 500 행).

3. **리뷰** — 생성된 SQL 파일을 조직 운영팀 담당자가 검토:
   - 헤더 주석: 로스터 파일명, 행수, 기본 effective_from.
   - INSERT문: NOT IN 멱등 가드가 있으므로 재실행 안전.
   - 말미 검증 SELECT: 실행 후 "결과가 비어야 정상" — 중복/누락/충돌 확인.

4. **실행** — admin이 검증된 SQL을 수동 실행:
   ```sql
   -- SQL 파일 전체 실행 (INSERT + 말미 검증)
   ```
   - 말미 검증 결과가 비어야만 정상 완료.
   - 실패 시 §8.4 정정 절차 참조.

## 데이터 경계 (§7.2 — 절대 규칙)

| 규칙 | 근거 |
|---|---|
| **실로스터 CSV·생성 INSERT SQL은 레포·사외 환경 반입 금지** | 개인정보(실명·조직명) 포함. `.gitignore`가 `assets/user-org/data/`, `*roster*.csv`, `dim_token_user_org_insert*.sql` 패턴으로 선제 차단. |
| **stage(사외 홈랩) 환경에는 합성 로스터만** | `fixtures/synthetic_org_members.csv` — mock user ID 체계(user-####). fixtures는 공개 데이터지만, 실 로스터는 절대 반입 금지. |
| **anonymous 매핑 행은 `user_name` 빈 문자열 강제** | `user_id` 접두사가 `anon-`이면 `user_name`을 공백으로 강제(도구 자동 처리). 실명 결합 금지(§6.1). |

## 도구 사용 예

### 합성 로스터로 테스트

```bash
# stage 환경에서 합성 로스터 사용
python3 assets/user-org/csv_to_dim_user_org_insert.py \
    --csv assets/user-org/fixtures/synthetic_org_members.csv \
    --effective-from 2026-07-01 \
    --out /tmp/test_insert.sql

# 출력 예
# 생성 완료: /tmp/test_insert.sql
# 입력 행수: 30
# chunk 크기: 500 (chunk 수: 1)
# anon-* user_name 강제 치환: 1건
# 검증: 출력 SQL 말미 "-- 검증: 결과가 비어야 정상" 섹션 실행 후 결과가 비어 있어야 정상 (admin 리뷰 절차)
```

### --out 기본값과 .gitignore 이중 차단

- 도구의 `--out` 기본값은 `dim_token_user_org_insert.sql` (현재 작업 디렉터리).
- `.gitignore` 패턴이 명시적으로 차단:
  - `dim_token_user_org_insert*.sql` — 생성 SQL 전체.
  - `*roster*.csv` — 로스터 파일.
  - `assets/user-org/data/` — 로스터 저장 디렉터리.
- 운영자가 `--out` 옵션으로 명시적으로 사외 위치를 지정해도 git 저장소 내에
  저장하지 않도록 조직 절차를 구성.

## 갱신 규칙

기존 행 수정 금지 — **새 effective_from 행을 append** 후 재투입.

### 예시: 조직 개편으로 팀 이동

| effective_from | user_id | org |
|---|---|---|
| 2026-01-01 | user-0005 | A부문>X팀 |
| 2026-06-01 | user-0005 | B부문>Z팀 | ← 새 행 추가(기존 행 불변) |

### 이력 정정 시 mart rerun 의무

조직 데이터(dim_token_user_org) 이력을 정정하면 조직 차원 집계(mart.token_usage_1d 등)도
해당 기간을 **다시 계산해야 한다**. `docs/operations/rerun.md`의 "dim 정정 시 mart rerun"
섹션 참조.

## DDL 및 배포

- `ddl/README.md` — 적용 순서, 협의 지점, 선행 조건(mart 개명 먼저) 참조.
- `ddl/company/dim_token_user_org.sql` — 테이블 생성 DDL.
- `ddl/company/accounts.sql` — 권한 설정 (admin 수동 실행).

## 검증

```bash
python -m pytest assets/user-org/tests/ --ignore=assets/user-org/tests/e2e
```
