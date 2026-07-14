# mock-provider

`token-usage-api` 계약(v1.1.0, `contract/` vendored @6c32650)을 구현한 결정적 mock 서비스.
수집기·mart의 CI E2E와 stage(홈랩) 통합 테스트의 데이터 소스 역할 (스펙 §8.1).

## 실행

    pip install -r requirements-dev.txt
    uvicorn app.main:app --port 8000
    curl "http://127.0.0.1:8000/v1/usage?date=$(date -d yesterday +%F)&limit=100"

## 설정 (환경변수)

| 변수 | 기본값 | 의미 |
|---|---|---|
| MOCK_SERVICE_GROUP / MOCK_SERVICE | Mock Group / Mock Service A | 응답 정체성 |
| MOCK_SEED | token-mock-1 | 결정적 데이터 시드 — 같은 seed+date = 같은 데이터 |
| MOCK_USERS / MOCK_ANON_USERS | 50 / 10 | identified/anonymous 사용자 수 |
| MOCK_MODELS | claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5 | 모델 목록 |
| MOCK_RETENTION_DAYS | 90 | 이보다 오래된 date 요청 → 404 |

## 시나리오 주입 (계약 밖, 테스트 전용)

    curl -X POST localhost:8000/__mock/scenario -H 'content-type: application/json' \
      -d '{"not_ready_at_page": 2}'
    curl -X POST localhost:8000/__mock/reset

필드: not_ready_until_uptime_s · retry_after_s · rate_limit_every · error_503_every ·
summary_extra_pct · name_drift · generated_at_change_at_page · not_ready_at_page
(전부 OFF = 완전한 계약 준수 — CI conformance가 이 불변식을 검증)

## 검증

    python -m pytest tests/ -v      # 단위/계약 시맨틱
    ./run_conformance.sh            # 스펙 레포의 conformance_check 통과

이미지 빌드·컨테이너 스모크는 CI의 image job에서 검증 (로컬 개발 머신에는 docker 없음).

## stage 배포

**전제:**
- `registry-pull-secret` ConfigMap/Secret이 `monitoring` 네임스페이스에 존재해야 함 (stage-runbook 참조)
- 이미지는 `release-images` CI 워크플로에서 공급됨 (`ghcr.io/yoonsungnam/token-mock-provider:latest`)

**배포:**

    kubectl apply -n monitoring -f tools/mock-provider/k8s.yaml

4개 리소스가 생성됨:
- `token-mock-provider-a` Service/Deployment (Mock Service A)
- `token-mock-provider-b` Service/Deployment (Mock Service B)

각 서비스는 포트 8000에서 동작하며, `collectors/token-usage/endpoints.yaml`의 baseUrl을 통해 수집기에서 접근됨.
