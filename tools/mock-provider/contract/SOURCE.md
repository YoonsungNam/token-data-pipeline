# Vendored contract files

- 출처: https://github.com/YoonsungNam/token-usage-api-spec @ commit `6c32650` (2026-06-17)
- 파일: `token-usage-api.yaml` (공유용 최종 스펙 v1.1.0), `tests/conformance_check.py`
- 이유: CI 자립성 — 사설 레포 접근 토큰 없이 conformance를 실행하기 위해 고정 복사.
- 갱신 절차: 원본 레포 갱신 시 이 디렉터리를 다시 복사하고 본 파일의 커밋 해시를 갱신한다.
  (원본과의 드리프트는 이 해시로 추적)
- 주의: `conformance_check.py`는 반드시 `contract/tests/` 하위에 유지할 것 — 스크립트가
  `HERE/../token-usage-api.yaml` 상대 경로로 스펙을 찾으므로 flat 복사 시 즉시 깨진다.

## token-metric-api (`GET /v1/metrics`) — Plan 6b T1 추가

- 출처: https://github.com/YoonsungNam/token-metric-api-spec @ commit `6a552d2` (2026-08-31, spec `version: 0.1.0`)
- 파일: `token-metric-api.yaml` (490행) ← 원본 루트 `token-metric-api.yaml`,
  `tests/check_metrics_api.py` (569행, 실행 비트) ← 원본 `scripts/check_metrics_api.py`
- sha256 (바이트 동일 복사 — `sha256sum contract/token-metric-api.yaml contract/tests/check_metrics_api.py`로 재확인):
  - `a7961c71370ba5bcc7cefe60bf71249090aca8a9e20ed60d8d1f27c9a8d4dc27  token-metric-api.yaml`
  - `7173ca982c1bcbc0255e02c81a7a35486837a597d0ac5ad90df7885099525a0e  tests/check_metrics_api.py`
- 이유: 메트릭 계약(케이스 A~F) 준수 검증 — `run_conformance.sh`가 usage 단계 뒤에 같은 uvicorn
  프로세스로 `check_metrics_api.py`를 실행한다 (FAIL 있으면 exit 1, WARN만이면 exit 0, `--date` 오류 exit 2).
- 갱신 절차: 원본 레포의 새 커밋을 스크래치에 clone → 두 파일을 `cp`로 바이트 복사(`chmod +x` 유지) →
  `sha256sum`으로 본 절의 해시·커밋·날짜 갱신 → `./run_conformance.sh`로 재검증. 스크립트 수정 금지(드리프트는 해시로 추적).
- 주의: `check_metrics_api.py`는 표준 라이브러리만 쓰고 스펙 yaml을 읽지 않는다(상대 경로 의존 없음) —
  `contract/tests/` 배치는 usage `conformance_check.py`와의 일관성 목적. 동작 검사 C3는 "30일 전 → 404"를
  기대하므로 `MOCK_METRICS_RETENTION_DAYS`를 30 이상으로 올리면 WARN(비치명)이 난다.
