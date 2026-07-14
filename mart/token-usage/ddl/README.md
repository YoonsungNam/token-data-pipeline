# mart/token-usage DDL 초안 (Plan 3)

스펙 v1.8 §4.0/§4.3/§4.2 기준. mart 1차 집계 4테이블 + `gpu_data.view_token_usage_*` 4테이블
(전부 `_local` ReplicatedMergeTree + `_dist` Distributed 쌍) + 계정 GRANT 추가분.

| 파일 | 내용 | 실행 주체 |
|---|---|---|
| `company/mart_tables.sql` | mart.token_usage_1d + agg 3종 (service/org/model) | install.sh 자동 |
| `company/view_token_usage.sql` | gpu_data.view_token_usage_* 4종 (대시보드 최종 테이블) | install.sh 자동 |
| `company/accounts.sql` | token_mart 쓰기 GRANT + token_dashboard_reader 읽기 GRANT | **admin 수동** |

## 협의 지점 (소유자 리뷰 요청 — §9-18 잔여)

1. **mart DB 공유/전용 (§9-18의 마지막 잔여 항목)** — 이 초안은 **공유 mart DB**(동료
   기존 `mart` DB에 동거)를 기본안으로 작성했다. 근거:
   - fact 공유 결정(2026-07-13)과 동일한 논리 — DB 수를 늘리지 않고, GRANT는 어차피
     테이블 레벨이라 공유해도 파괴 반경이 생기지 않음
   - 테이블명에 token이 이미 포함(`token_usage_1d`, `agg_token_*_1d`)되어 기존
     `fact_job_gpu_usage_*` 등과 충돌·혼동 없음 — gpu_data의 `*_token_*` 규칙과 같은 효과
   - 전용 DB(`mart_token` 등)로 가야 한다면 파일 내 DB명만 치환하면 됨 (구조 동일)
   → **공유로 진행해도 되는지 확인 부탁드립니다.**
2. **gpu_data 신규 테이블 4종** — `view_token_usage_{1d,service_1d,org_1d,model_1d}`.
   `*_token_*` 접두사 규칙 준수, 물리 테이블(CREATE VIEW 아님 — 대시보드가 mart 재계산과
   무관하게 안정적으로 읽는 최종 테이블). dim_token_service 때와 동일하게 gpu_data에
   추가 GRANT(위 accounts.sql)가 필요합니다.
3. **뮤테이션 예산 내 mart 기여** — 확정 예산(일 150/피크 80) 산정에 이미 "mart 8테이블"이
   포함돼 있음(collectors/token-usage/ddl/README.md). 일일 정상 경로: 최대 8건
   (mart 4 + view 4, 첫 적재일은 존재 확인 no-op 스킵으로 0건), 04:00 창이라 피크 창
   (02:00~03:00)과 겹치지 않음.

## 임시 방침 상속 (미결 — 스펙 §9)

- TTL 전 테이블 25개월 (§9-7 — 보존 정책 확정 시 일괄 조정)
- view 스키마 = mart 동일 (§9-1 대시보드 컬럼 계약 확정 전 — org 롤업 표시 깊이,
  anonymous 버킷, per-user 행 조직 부착, 불완전 마커 등은 협의 후 `migrate_add_*`)
- cost는 USD 고정 참고 지표 (§9-5)

## 설계 결정 요약

- **co-location**: `mart.token_usage_1d`는 fact 상세와 동일 파티션(일)·ORDER BY·샤딩키
  (`cityHash64(service, user_id)`) — 조인·비교 쿼리의 셔플 최소화. 이 보장은 **INSERT가
  `_dist` 경유일 때만 자동 성립**하므로 GRANT도 `_dist` INSERT만 부여 (`_local`은 멱등
  DELETE용 ALTER DELETE만)
- **org agg grain = 말단 org_path** (가변 깊이 — §4.3): 상위 롤업은 쿼리 시
  `arraySlice(org_path, 1, k)` GROUP BY, 서브트리는 prefix 비교. 샤딩키
  `cityHash64(arrayStringConcat(org_path, '>'))`. org_depth는 `length(org_path)`로
  파생 가능하므로 물리 컬럼 없음 (YAGNI)
- **created_by 공유 쓰기 계약** (§4.2 리뷰 #22): DEFAULT 없음 + `CONSTRAINT
  check_created_by CHECK created_by != ''`를 **`_local`과 `_dist` 양쪽에** 선언 —
  `_dist`에도 있어야 비동기 Distributed INSERT에서도 initiator 시점에 즉시 거부된다
  (local에만 있으면 위반이 백그라운드 전송 큐에 박혀 후속 배치를 막는다 — 24.8 실증)
- **reported_\*/diff_\* Nullable** (§4.1 + STEP 0 정합): ① `is_derived=1`(detail 합산
  파생 summary)이면 diff_\*는 자기 자신 비교이므로 NULL. ② **summary 행 자체가 없는
  서비스**(STEP 0 경고 대상이나 적재는 진행)는 reported_\*·diff_\* 전부 NULL —
  비-Nullable이면 LEFT JOIN 미스가 "보고값 0"으로 위장되고 거짓 대사 불일치가 기록된다
- **reader 계정은 view `_dist`만 GRANT**: fact·mart·dim 직접 조회 차단 + `_local` 우회
  차단 (§7.2). per-user 상세의 노출 grain·조직 부착은 §9-1/§9-3 미결 — 이 GRANT가
  per-user 접근 통제를 대신하지 않음

## 적용 순서 (스펙 §7.2 — DDL 실행 주체 분리)

1. `accounts.sql`의 GRANT는 **admin 수동 실행** (계정 자체는 collectors accounts.sql에서
   기생성 — 없으면 그것부터). mart DB가 전용으로 확정되면 CREATE DATABASE도 admin.
2. 테이블 DDL(`mart_tables.sql`, `view_token_usage.sql`)은 mart install.sh 자동 적용
   대상 (Plan 3 후속 태스크).
3. **선행 의존**: STEP 1이 조인하는 `gpu_data.dim_token_user_org`/`dim_token_model`의 DDL과
   token_mart SELECT GRANT는 assets(Plan 4) 소관 — **mart 가동 전 Plan 4 적용 전제**
   (E2E/stage 검증에서는 테스트 DDL로 대체).
4. 이후 스키마 변경은 `migrate_add_*.sql` 관행 (GRANT 추가 포함).
