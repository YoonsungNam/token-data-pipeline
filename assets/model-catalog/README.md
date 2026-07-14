# assets/model-catalog

AI 모델 카탈로그 및 단가 관리 모듈 (스펙 §6.2). `gpu_data.dim_token_model` 이력 테이블에
모델별 사용 단가(input, cache read/creation, output per MTok)를 기록한다.

## 개요

모델 카탈로그는 반기/분기 단위 공표 단가(Anthropic 또는 사내 계약가)를 이력으로 관리한다.
기본 단위는 매우 낮은 빈도(계약 변경 시만)로 단가 갱신은 누적 기록 패턴을 따른다.

## 단가 갱신 절차

### 1단계: 새 단가 행을 파일에 추가

`ddl/company/seed_dim_token_model.sql` 파일에서:

1. 상황: 새로운 모델 추가 또는 기존 모델 단가 변경.
2. 작업: UNION ALL 섹션에 새로운 ROW를 append:
   ```sql
   UNION ALL
   SELECT 'claude-new-model', toDate('2026-08-01'), 'anthropic', 'external',
          toNullable(4.0), toNullable(0.4), toNullable(5.0), toNullable(20.0),
          'USD', '공표 단가 2026-08'
   ```
   - 날짜: 이전 effective_from보다 **나중** (이력은 시간 순).
   - 기존 행은 **절대 수정 금지** — 새 행으로만 갱신.

### 2단계: 시드 SQL 재실행

```bash
# admin이 수동 실행 (stage/company 환경 모두)
CH_HOST=... CH_USER=admin CH_PASSWORD=... \
    clickhouse-client --format=JSONCompact \
    < assets/model-catalog/ddl/company/seed_dim_token_model.sql
```

- NOT IN 멱등 가드가 이전 행을 자동 스킵.
- 말미 검증 SELECT 결과가 비어야 정상:
  - `dup_key`: (model, effective_from) 중복 없음.
  - `unknown_row_state`: `model='unknown'` 행이 존재하고 모든 단가가 NULL.

### 3단계: 소급 정정 시 mart rerun

기존 행을 정정할 일은 드물지만(계약 오류), 발생 시:

```bash
# 1. 이력 정정 (§8.4 절차)
# 2. 해당 기간 mart rerun (mars.token_usage_1d 재계산)
python3 mart/token-usage/tools/rerun.py --context company \
    --from 2026-08-01 --to 2026-08-31

# 자세한 내용은 docs/operations/rerun.md 참조
```

## unknown 행의 의미

모든 `effective_from`에 대해 `model='unknown'` 행을 유지한다:

| 컬럼 | 값 | 의미 |
|---|---|---|
| `model` | `unknown` | "미등록 모델" 표준 대푯값 |
| 모든 단가 | NULL | "단가 산정 불가" 명시 |
| `effective_from` | `2026-01-01` | 항상 기본 시드 기준일 |
| `note` | `계약 표준 값 — 단가 산정 불가` | 설명 |

### 용도

- Mart STEP 1 집계에서 미등록 모델이 나오면 mart.token_usage_1d에서 금액 열이 NULL이 됨.
- "미등록 모델" 경보(`CHECK WARN "unregistered_models"`)를 정상적으로 발화하게 함.
- unknown 행이 없으면 dim 조인 실패 → `UNKNOWN_TABLE` 에러 → 운영자는 카탈로그 누락을
  깨닫지 못함.

## 환경 및 설정

- 통화: 고정 USD (§9-5 미결 상속).
- 단가 단위: per 1,000,000 토큰(MTok) — §4.2 정의.
- provider/serving_type은 향후 확장 고려(현재는 Anthropic external만).

## DDL 및 권한

- `ddl/README.md` — 협의 지점(네이밍), 시드 규약 상세.
- `ddl/company/dim_token_model.sql` — 테이블 생성 DDL.
- `ddl/company/accounts.sql` — token_mart 읽기 권한 (admin 수동).

## 참고

- mart의 4가지 집계 테이블(token_usage_1d, agg_token_service_1d, agg_token_org_1d,
  agg_token_model_1d)이 모두 dim_token_model에 LEFT JOIN.
- 단가 NULL이면 agg_token_model_1d의 금액 컬럼도 NULL.
- 모델 변경 이력이 비즈니스에 직접 영향(비용 집계) — 정확성과 감시 필수.
