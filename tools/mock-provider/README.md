# mock-provider

`token-usage-api` 계약(v1.1.0, `contract/` vendored @6c32650)을 구현한 결정적 mock 서비스.
수집기·mart의 CI E2E와 stage(홈랩) 통합 테스트의 데이터 소스 역할 (스펙 §8.1).
자매 계약 `token-metric-api`(`contract/token-metric-api.yaml` vendored @6a552d2)의 `GET /v1/metrics`도
같은 앱에서 제공한다 — 아래 "/v1/metrics" 절.

## 실행

    pip install -r requirements-dev.txt
    uvicorn app.main:app --port 8000
    curl "http://127.0.0.1:8000/v1/usage?date=$(date -d yesterday +%F)&limit=100"
    curl "http://127.0.0.1:8000/v1/metrics?date=$(date -d yesterday +%F)"

## 설정 (환경변수)

| 변수 | 기본값 | 의미 |
|---|---|---|
| MOCK_SERVICE_GROUP / MOCK_SERVICE | Mock Group / Mock Service A | 응답 정체성 |
| MOCK_SEED | token-mock-1 | 결정적 데이터 시드 — 같은 seed+date = 같은 데이터 |
| MOCK_USERS / MOCK_ANON_USERS | 50 / 10 | identified/anonymous 사용자 수 |
| MOCK_MODELS | claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5 | 모델 목록 |
| MOCK_RETENTION_DAYS | 90 | 이보다 오래된 date 요청 → 404 (`/v1/usage*`) |
| MOCK_METRICS_RETENTION_DAYS | 14 | `/v1/metrics`: 이보다 오래된 date 요청 → 404 (계약 보존 14일) |

## 시나리오 주입 (계약 밖, 테스트 전용)

    curl -X POST localhost:8000/__mock/scenario -H 'content-type: application/json' \
      -d '{"not_ready_at_page": 2}'
    curl -X POST localhost:8000/__mock/reset

필드: not_ready_until_uptime_s · retry_after_s · rate_limit_every · error_503_every ·
summary_extra_pct · name_drift · generated_at_change_at_page · not_ready_at_page
(전부 OFF = 완전한 계약 준수 — CI conformance가 이 불변식을 검증)

`/v1/metrics` 전용 int 플래그 6종(0=OFF, 1=ON; `_shared_gate`의 429/503·`not_ready_until_uptime_s`·
`retry_after_s`·`name_drift`는 두 계약이 공유):

| 플래그 | 1이면 | 수집기 §5.3 검증 항목 |
|---|---|---|
| metrics_gpu_hours_over | 첫 gpu 행 `gpuHours = gpuCount*24 + 10` | `hours_over_count` |
| metrics_unknown_serving | `model="unknown", category="serving"` 행 1개 추가 | `unknown_violation` |
| metrics_pct_non_monotone | 첫 serving 행 `ttftMs.p90 = p50 - 1` | `pct_non_monotone` |
| metrics_dup_gpu_rows | 첫 gpu 행 복제본을 인덱스 1에 삽입(인접 중복) | `dup_merged` |
| metrics_empty_gpu | `gpu: []` (serving만 있는 응답) | 케이스 E — `NODATA` 아님 |
| metrics_engine_null | `engine: null` | engine 부재 허용 |

## /v1/metrics (token-metric-api @6a552d2)

    curl "http://127.0.0.1:8000/v1/metrics?date=$(date -d yesterday +%F)"

- 단건 응답 `{date, serviceGroup, service, generatedAt, engine, gpu, serving}` — `app/datagen.py::build_metrics`가
  같은 (seed, date, 시나리오)에서 항상 같은 본문을 만든다 (계약 C4 멱등성 · 수집기 E2E 기대치 산출의 근거 — Plan 6b T11
  `collectors/token-metrics/tests/e2e/ci_expectations.py`가 이 함수를 import한다).
- 기본 데이터(모델 3종): `gpu` 5행 = 모델당 `serving` 1행(H100, `gpuCount` 1..8, `gpuHours ≤ gpuCount×24`) +
  첫 모델 `standby` 1행(1장·24.0h) + `model="unknown"` `test` 1행; `serving` 3행 = 모델당 `ttftMs`·`itlMs`(p50≤p90≤p95≤p99)·
  `outputTps{p50}`; `engine` 고정 `{"type": "vllm", "version": "0.10.1"}`; `generatedAt` = 다음날 `T02:05:00+09:00`.
- 응답 코드는 usage와 같은 `_date_gate` 규칙: 당일/미래/형식 오류 400 `invalid_date`, `date` 누락 400,
  `MOCK_METRICS_RETENTION_DAYS`(14) 초과 404 `data_not_retained`, `not_ready_until_uptime_s` 안이면 409 `data_not_ready` + `Retry-After`,
  429/503은 usage와 **같은 요청 카운터**를 공유한다.

## 검증

    python -m pytest tests/ -v      # 단위/계약 시맨틱
    ./run_conformance.sh            # usage conformance_check 통과 + metrics check_metrics_api "FAIL 0"

이미지 빌드·컨테이너 스모크는 CI의 image job에서 검증 (로컬 개발 머신에는 docker 없음).

`./run_conformance.sh <date>`에 `MOCK_METRICS_RETENTION_DAYS`(기본 14)보다 오래된 날짜를 주면 `/v1/metrics`가
404 `data_not_retained`를 돌려주므로 `check_metrics_api.py`는 그 날짜에 대해 B1~B10 gpu/serving 구조 검증을
건너뛰고 WARN(`retention-404`)만 남긴 채 `FAIL 0`(= 통과)로 끝난다 — 그 날짜의 응답 구조가 실제로 검증됐다는
뜻이 아니다. 구조 검증까지 받으려면 보존 기간 안의 날짜(기본 최근 14일)를 지정한다.

## stage 배포

**전제:**
- `registry-pull-secret` Secret이 `monitoring` 네임스페이스에 존재해야 함 (stage-runbook 참조)
- 이미지는 `release-images` CI 워크플로에서 공급됨 (`ghcr.io/yoonsungnam/token-mock-provider:latest`)

**배포:**

    kubectl apply -n monitoring -f tools/mock-provider/k8s.yaml

4개 리소스가 생성됨:
- `token-mock-provider-a` Service/Deployment (Mock Service A)
- `token-mock-provider-b` Service/Deployment (Mock Service B)

각 서비스는 포트 8000에서 동작하며, `collectors/token-usage/endpoints.yaml`의 baseUrl을 통해 수집기에서 접근됨.

**env 커스터마이즈:** `k8s.yaml`의 각 Deployment `env` 블록을 직접 편집해 위 "설정
(환경변수)" 표의 값을 덮어쓸 수 있다 — 데이터 볼륨·결정적 시드를 바꾸고 싶을 때 사용.

```yaml
# 예: Mock Service A의 사용자 수를 늘리고 시드를 바꾸는 경우
env:
  - name: MOCK_SERVICE_GROUP
    value: "Mock Group"
  - name: MOCK_SERVICE
    value: "Mock Service A"
  - name: MOCK_SEED
    value: "stage-seed-a-v2"      # 시드를 바꾸면 같은 date라도 다른 데이터셋이 생성됨
  - name: MOCK_USERS
    value: "200"                  # identified 사용자 수 확장(기본 50)
  - name: MOCK_ANON_USERS
    value: "10"
```

편집 후에는 재적용이 필요하다:

```bash
kubectl apply -n monitoring -f tools/mock-provider/k8s.yaml
kubectl -n monitoring rollout restart deployment/token-mock-provider-a
```

`MOCK_SEED`를 바꾸면 이전 시드로 이미 적재된 fact/mart 데이터와 정합이 깨지므로(§8.1
결정적 재현성 전제), 배포 중간에 바꿀 경우 해당 날짜 이후 구간은 collectors/mart
rerun이 필요하다(`docs/operations/rerun.md`).
