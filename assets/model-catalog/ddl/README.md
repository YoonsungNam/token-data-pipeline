# assets/model-catalog DDL 초안 (Plan 4)

스펙 v1.10 §4.2/§6.2 기준. `gpu_data.dim_token_model` (`_local`+`_dist`) + 시드 SQL +
공유 계정 `mart` 읽기 GRANT.

## 협의 지점

- **네이밍**: `dim_token_model` — `dim_model`은 범용 이름이라 공유 gpu_data에서 충돌
  위험이 가장 큰 케이스입니다 (dim_token_* 규칙 적용, user-org README 참조).

## 시드 규약 (§6.2 — dim_holiday 3요소 패턴)

`seed_dim_token_model.sql`: (a) 출처·기준일 헤더, (b) `NOT IN` 멱등 가드, (c) 말미 검증
SELECT(결과 비어야 정상). `model='unknown'` 행은 전 단가 NULL로 시드 — "미등록 WARN"
경보 무력화 방지 (리뷰 #15).

## 단가 갱신 절차

기존 행 수정 금지 — **새 effective_from 행 append** 후 시드 SQL 재실행(멱등 가드가 기존
행 스킵). 소급 정정 = 이력 정정 후 해당 기간 mart rerun (§4.3). 통화는 USD 고정
(§9-5 미결 상속 — cost는 참고 지표).
