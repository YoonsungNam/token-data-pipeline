# Vendored contract files

- 출처: https://github.com/YoonsungNam/token-usage-api-spec @ commit `6c32650` (2026-06-17)
- 파일: `token-usage-api.yaml` (공유용 최종 스펙 v1.1.0), `tests/conformance_check.py`
- 이유: CI 자립성 — 사설 레포 접근 토큰 없이 conformance를 실행하기 위해 고정 복사.
- 갱신 절차: 원본 레포 갱신 시 이 디렉터리를 다시 복사하고 본 파일의 커밋 해시를 갱신한다.
  (원본과의 드리프트는 이 해시로 추적)
- 주의: `conformance_check.py`는 반드시 `contract/tests/` 하위에 유지할 것 — 스크립트가
  `HERE/../token-usage-api.yaml` 상대 경로로 스펙을 찾으므로 flat 복사 시 즉시 깨진다.
