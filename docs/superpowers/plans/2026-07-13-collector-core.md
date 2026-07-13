# Collector Core Implementation Plan (Plan 2a/5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `collectors/token-usage/` 수집기 코어 — endpoints.yaml의 서비스들을 순회해 token-usage-api를 pull하고, 정규화·검증을 거쳐 ClickHouse에 (date, service) 원자 교체로 적재하며, SERVICE_RESULT/BATCH_RESULT 마커를 남긴다. CI에서 mock-provider + ClickHouse 컨테이너로 E2E 검증.

**Architecture:** 5개 파일 책임 분리 — `config.py`(env), `api_client.py`(HTTP·페이지네이션·이벤트 분류 — requests 세션 주입 가능한 순수 클라이언트), `normalize.py`(행 정규화·검증·집계 — DB/HTTP 무접촉 순수 함수), `clickhouse_client.py`(존재확인→DELETE(mutations_sync=2)→배치 INSERT→감사 append), `main.py`(서비스 큐 오케스트레이션·소프트 데드라인·마커·CLI). 배포(k8s/build/install/rerun)는 Plan 2b — DDL 협의(§9-18) 확정 후.

**Tech Stack:** Python 3.12, requests, clickhouse-connect, PyYAML, pytest. CI: mock-provider 이미지(PR #2) + clickhouse-server 컨테이너.

## Global Constraints

- 스펙 v1.6 §5 전체가 요구사항 원문 (`docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md`).
- **서비스 식별 정본 = endpoints.yaml** (§5.0): DELETE 술어·적재 컬럼·조인 키 전부 설정값. 응답 원문은 `reported_service_group/reported_service` 보존, 불일치 시 CHECK WARN.
- **공통 이벤트 분류→정책 표 1벌** (§5.2): NOT_READY(대기열 후송·svc 30분/소프트 데드라인)·RETRYABLE(≤3회, Retry-After 캡 300s)·PERMANENT_ERROR(즉시 FAILURE)·RETENTION(정기=FAILURE/재수집=SKIPPED)·EMPTY(NODATA, summary는 적재)·INVARIANT_BROKEN(폐기 후 재시작 ≤2회).
- **(date, service) = 원자 스냅샷** (§5.2·5.3): 409 재방문은 summary부터 전체 재시작(cursor 재개 금지). 매 페이지 `(serviceGroup, service, date, generatedAt)` 첫 페이지 대비 불변성 검사. 저장 generated_at = 마지막 페이지 값.
- **멱등 적재** (§5.1-3-5): 기존 행 존재 SELECT 확인 → 있을 때만 `ALTER TABLE <local> ON CLUSTER DELETE`(`mutations_sync=2`) → INSERT(`insert_distributed_sync=1`). DELETE 직전 기존 세대 요약을 `collect_audit_1d`에 append. **NODATA여도 summary 행 적재**.
- **메모리 규칙** (§5.1 v1.6): 전량 버퍼 금지 — `MAX_BUFFER_ROWS`(기본 20,000) 도달 시마다 INSERT flush (DELETE는 첫 flush 전 1회). 검증(Σdetail, 불변성)은 스트리밍 누적.
- **정규화** (§5.4): `userId null→''`, 캐시 생략→0, userType↔userId 위반·음수·타입 위반 행 거부+카운트, 논리 키 중복은 불변성 확인 시에만 SUM 병합.
- **소프트 데드라인** (§5.2): Job 경과 `SOFT_DEADLINE_MINUTES`(기본 50분) — 모든 대기·신규 서비스 착수 전 체크. **적재 시퀀스 예산 12분**: 잔여 시간 < 예산이면 시작하지 않고 FAILURE. `Retry-After` 캡 `min(RA, 300s)`.
- **마커** (§5.6): `BATCH_RESULT`는 실행당 최종 1줄(status 집계: 실패≥1→FAILURE, 전부 NODATA→NODATA, 그 외 SUCCESS; `services_ok/failed/skipped`). 서비스별은 `SERVICE_RESULT status=SUCCESS|NODATA|SKIPPED|FAILURE module=token-usage service=<정본> source_type=usage-api-v1 rows= pages= warn= rejected=`. **SIGTERM 시에도 요약 BATCH_RESULT 출력**.
- **로깅 계약** (§5.6): 로그에 레코드 페이로드·user_id 원문 금지 — 카운트·인덱스·필드명만.
- **404 분기** (§5.2): 일일 정기(target=어제)=FAILURE / 명시적 재수집(--from/--to)=SKIPPED+WARN.
- CLI (§5.1·§7.1과 동일 계약): `main.py [batch_time]`(ISO8601, 기본 now, target_date=−1일 KST), `--from/--to`(재수집 — VM push 기본 생략), `--service <name>`.
- 테이블·DB명은 DDL 초안(PR #3) 기준: `token_fact.raw_token_usage_1d(_local/_dist)`, `raw_token_usage_summary_1d`, `collect_audit_1d`, `gpu_data.dim_service`. §9-18 협의로 DB명 변경 시 `config.py`의 상수 2개만 수정.
- 환경변수 계약 (§5.7): `CH_HOST/CH_PORT/CH_USER/CH_PASSWORD/CH_CLUSTER`(빈 값=ON CLUSTER 생략), `VM_PUSH_URL`(빈 값=push 생략), `ENDPOINTS_FILE`, `MAX_PAGES`(200), `SOFT_DEADLINE_MINUTES`(50), `MAX_BUFFER_ROWS`(20000), `NOT_READY_BUDGET_MINUTES`(30), `COLLECTOR_HTTPS_PROXY`(미설정=상속/빈 문자열=직접), `COLLECTOR_API_VERIFY`/`COLLECTOR_API_CA_BUNDLE`.
- **Python 3.10+ 호환 문법** (개발 머신 3.10 / CI·컨테이너 3.12 — `StrEnum` 등 3.11+ 전용 금지), `random` 금지(테스트 포함), KST 규율(naive datetime 금지), 커밋 `type(collectors): 설명`, 테스트는 `cd collectors/token-usage && python -m pytest`.

## File Structure

```text
collectors/token-usage/
├── app/
│   ├── __init__.py
│   ├── config.py            # env → Config + endpoints.yaml 로더(정본 레지스트리)
│   ├── events.py            # 공통 이벤트 분류(Enum) + CollectError(분류 캐리어)
│   ├── api_client.py        # summary/usage pull, 페이지네이션+불변성 검사, HTTP→이벤트 번역
│   ├── normalize.py         # 행 정규화·검증·SUM 병합·집계 (순수)
│   ├── clickhouse_client.py # 멱등 적재(존재확인→DELETE→배치INSERT→감사), dim_service 범위 교체
│   ├── vm_push.py           # 서비스 합계 게이지 (reported_* — summary 보고값)
│   └── main.py              # 오케스트레이터: 서비스 큐·데드라인·마커·SIGTERM·CLI
├── tests/
│   ├── test_config.py
│   ├── test_api_client.py   # Fake transport — 페이지네이션·409·429·불변성·상한
│   ├── test_normalize.py
│   ├── test_clickhouse_client.py  # Fake CH client — 시퀀스·flush·감사
│   └── test_main.py         # Fake 전부 주입 — 큐·데드라인·마커·exit code
├── ddl/                     # (PR #3에서 작성됨 — 이 플랜은 수정하지 않음)
├── endpoints.yaml           # stage(mock-provider 2인스턴스) 예시
├── conftest.py              # (빈 파일)
├── requirements.txt / requirements-dev.txt
└── tests/e2e/               # CI 전용
    ├── docker-compose 없이 — CI가 컨테이너 기동 (워크플로 참조)
    ├── seed_expectations.py # mock 결정성 기반 기대값 산출
    └── verify_expected_results.sql   # --expect-empty 방식
.github/workflows/test-collector.yml  # 단위 + E2E(mock+CH 컨테이너)
```

---

### Task 1: 스캐폴딩 + Config + endpoints 로더

**Files:**
- Create: `collectors/token-usage/app/__init__.py`(빈), `collectors/token-usage/conftest.py`(빈), `collectors/token-usage/app/config.py`
- Create: `collectors/token-usage/requirements.txt`, `collectors/token-usage/requirements-dev.txt`, `collectors/token-usage/endpoints.yaml`
- Test: `collectors/token-usage/tests/test_config.py`

**Interfaces:**
- Produces:
  - `Config` dataclass — 필드: `ch_host: str`, `ch_port: int`, `ch_user: str`, `ch_password: str`, `ch_cluster: str`, `vm_push_url: str`, `endpoints_file: str`, `max_pages: int`, `soft_deadline_minutes: int`, `max_buffer_rows: int`, `not_ready_budget_minutes: int`, `https_proxy: str | None`, `api_verify: bool | str`
  - `load_config() -> Config`
  - `ServiceEntry` (frozen dataclass): `service_group: str`, `service: str`, `base_url: str`, `enabled: bool`, `source_type: str` (기본 'usage-api-v1')
  - `load_endpoints(path: str) -> list[ServiceEntry]` — yaml 파싱, 필수 키 누락/빈 문자열/중복 service는 `ValueError`

- [ ] **Step 1: 의존성·설정 파일**

`collectors/token-usage/requirements.txt`:

```text
requests>=2.32,<3
clickhouse-connect>=0.7,<1
PyYAML>=6,<7
```

`collectors/token-usage/requirements-dev.txt`:

```text
-r requirements.txt
pytest>=8
```

`collectors/token-usage/endpoints.yaml` (stage: mock-provider 2인스턴스 예시):

```yaml
# 서비스 식별의 정본 (스펙 §5.0) — 폐기 서비스는 enabled: false로 유지, 항목 제거 금지
services:
  - serviceGroup: "Mock Group"
    service: "Mock Service A"
    baseUrl: "http://mock-provider-a.token-pipeline.svc:8000"
    enabled: true
  - serviceGroup: "Mock Group"
    service: "Mock Service B"
    baseUrl: "http://mock-provider-b.token-pipeline.svc:8000"
    enabled: true
```

설치: `cd collectors/token-usage && pip install -r requirements-dev.txt`

- [ ] **Step 2: 실패하는 테스트** — `collectors/token-usage/tests/test_config.py`

```python
import pytest

from app.config import load_config, load_endpoints


def test_config_defaults(monkeypatch):
    for k in ("CH_HOST", "CH_PORT", "CH_CLUSTER", "VM_PUSH_URL", "MAX_PAGES",
              "SOFT_DEADLINE_MINUTES", "MAX_BUFFER_ROWS", "NOT_READY_BUDGET_MINUTES",
              "COLLECTOR_HTTPS_PROXY", "COLLECTOR_API_VERIFY", "COLLECTOR_API_CA_BUNDLE"):
        monkeypatch.delenv(k, raising=False)
    cfg = load_config()
    assert cfg.ch_host == "localhost" and cfg.ch_port == 8123
    assert cfg.ch_cluster == ""          # 빈 값 = ON CLUSTER 생략
    assert cfg.vm_push_url == ""         # 빈 값 = push 생략
    assert cfg.max_pages == 200
    assert cfg.soft_deadline_minutes == 50
    assert cfg.max_buffer_rows == 20_000
    assert cfg.not_ready_budget_minutes == 30
    assert cfg.https_proxy is None       # 미설정 = 시스템 상속
    assert cfg.api_verify is True


def test_proxy_and_verify_semantics(monkeypatch):
    monkeypatch.setenv("COLLECTOR_HTTPS_PROXY", "")      # 빈 문자열 = 프록시 무시(직접)
    monkeypatch.setenv("COLLECTOR_API_CA_BUNDLE", "/etc/ca.pem")
    cfg = load_config()
    assert cfg.https_proxy == ""
    assert cfg.api_verify == "/etc/ca.pem"
    monkeypatch.setenv("COLLECTOR_API_VERIFY", "false")
    assert load_config().api_verify is False


def test_load_endpoints_ok(tmp_path):
    p = tmp_path / "eps.yaml"
    p.write_text(
        "services:\n"
        "  - serviceGroup: G1\n    service: S1\n    baseUrl: http://a:8000\n    enabled: true\n"
        "  - serviceGroup: G1\n    service: S2\n    baseUrl: http://b:8000\n    enabled: false\n"
    )
    eps = load_endpoints(str(p))
    assert [e.service for e in eps] == ["S1", "S2"]
    assert eps[0].enabled and not eps[1].enabled
    assert eps[0].source_type == "usage-api-v1"


def test_load_endpoints_rejects_bad(tmp_path):
    dup = tmp_path / "dup.yaml"
    dup.write_text(
        "services:\n"
        "  - {serviceGroup: G, service: S, baseUrl: 'http://a', enabled: true}\n"
        "  - {serviceGroup: G, service: S, baseUrl: 'http://b', enabled: true}\n"
    )
    with pytest.raises(ValueError):
        load_endpoints(str(dup))
    missing = tmp_path / "missing.yaml"
    missing.write_text("services:\n  - {serviceGroup: G, baseUrl: 'http://a', enabled: true}\n")
    with pytest.raises(ValueError):
        load_endpoints(str(missing))
    empty = tmp_path / "empty.yaml"
    empty.write_text("services:\n  - {serviceGroup: '', service: S, baseUrl: 'http://a', enabled: true}\n")
    with pytest.raises(ValueError):
        load_endpoints(str(empty))
```

- [ ] **Step 3: 실패 확인**

Run: `cd collectors/token-usage && python -m pytest tests/test_config.py -v`
Expected: FAIL — `No module named 'app'`

- [ ] **Step 4: 구현** — `collectors/token-usage/app/config.py` (+빈 `app/__init__.py`, `conftest.py`)

```python
import os
from dataclasses import dataclass

import yaml


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    return int(raw) if raw.strip() else default


@dataclass
class Config:
    ch_host: str = "localhost"
    ch_port: int = 8123
    ch_user: str = "default"
    ch_password: str = ""
    ch_cluster: str = ""            # 빈 값 = 단일노드, ON CLUSTER 생략 (§4.0)
    vm_push_url: str = ""           # 빈 값 = VM push 생략 (§5.5)
    endpoints_file: str = "endpoints.yaml"
    max_pages: int = 200            # 도달 = 부분 적재 금지, FAILURE (§5.2)
    soft_deadline_minutes: int = 50
    max_buffer_rows: int = 20_000   # §5.1 메모리 규칙
    not_ready_budget_minutes: int = 30
    https_proxy: str | None = None  # None=상속, ''=직접 연결, 값=전용 프록시 (§5.7)
    api_verify: bool | str = True   # False | True | CA bundle 경로


def load_config() -> Config:
    verify_raw = os.getenv("COLLECTOR_API_VERIFY", "")
    ca_bundle = os.getenv("COLLECTOR_API_CA_BUNDLE", "")
    api_verify: bool | str = True
    if verify_raw.strip().lower() == "false":
        api_verify = False
    elif ca_bundle.strip():
        api_verify = ca_bundle.strip()
    return Config(
        ch_host=os.getenv("CH_HOST", "localhost"),
        ch_port=_int_env("CH_PORT", 8123),
        ch_user=os.getenv("CH_USER", "default"),
        ch_password=os.getenv("CH_PASSWORD", ""),
        ch_cluster=os.getenv("CH_CLUSTER", ""),
        vm_push_url=os.getenv("VM_PUSH_URL", ""),
        endpoints_file=os.getenv("ENDPOINTS_FILE", "endpoints.yaml"),
        max_pages=_int_env("MAX_PAGES", 200),
        soft_deadline_minutes=_int_env("SOFT_DEADLINE_MINUTES", 50),
        max_buffer_rows=_int_env("MAX_BUFFER_ROWS", 20_000),
        not_ready_budget_minutes=_int_env("NOT_READY_BUDGET_MINUTES", 30),
        https_proxy=os.environ.get("COLLECTOR_HTTPS_PROXY"),
        api_verify=api_verify,
    )


@dataclass(frozen=True)
class ServiceEntry:
    service_group: str
    service: str
    base_url: str
    enabled: bool
    source_type: str = "usage-api-v1"


def load_endpoints(path: str) -> list[ServiceEntry]:
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    entries: list[ServiceEntry] = []
    seen: set[str] = set()
    for i, item in enumerate(doc.get("services") or []):
        missing = [k for k in ("serviceGroup", "service", "baseUrl", "enabled") if k not in item]
        if missing:
            raise ValueError(f"services[{i}]: missing keys {missing}")
        group = str(item["serviceGroup"]).strip()
        service = str(item["service"]).strip()
        base_url = str(item["baseUrl"]).strip()
        if not group or not service or not base_url:
            raise ValueError(f"services[{i}]: empty serviceGroup/service/baseUrl")
        if service in seen:
            raise ValueError(f"services[{i}]: duplicate service '{service}'")
        seen.add(service)
        entries.append(ServiceEntry(
            service_group=group, service=service, base_url=base_url.rstrip("/"),
            enabled=bool(item["enabled"]),
            source_type=str(item.get("type", "usage-api-v1")),
        ))
    if not entries:
        raise ValueError("endpoints file has no services")
    return entries
```

- [ ] **Step 5: 통과 확인**

Run: `cd collectors/token-usage && python -m pytest tests/test_config.py -v`
Expected: 4 passed

- [ ] **Step 6: Commit**

```bash
git add collectors/token-usage
git commit -m "feat(collectors): scaffold collector with config and endpoints registry"
```

---

### Task 2: 이벤트 분류 + api_client (페이지네이션·불변성·HTTP 번역)

**Files:**
- Create: `collectors/token-usage/app/events.py`, `collectors/token-usage/app/api_client.py`
- Test: `collectors/token-usage/tests/test_api_client.py`

**Interfaces:**
- Consumes: `ServiceEntry`, `Config` (Task 1)
- Produces:
  - `events.Event` (str-mixin Enum, 3.10 호환): `NOT_READY, RETRYABLE, PERMANENT_ERROR, RETENTION, EMPTY, INVARIANT_BROKEN`
  - `events.CollectError(Exception)` — 필드: `event: Event`, `message: str`, `retry_after_s: int`(기본 0)
  - `api_client.UsagePayload` (dataclass): `records: list[dict]`(원시 JSON dict), `summary: dict | None`, `generated_at: str`(마지막 페이지), `reported_service_group: str`, `reported_service: str`, `pages: int`
  - `api_client.fetch_service(entry, date: str, cfg, session) -> UsagePayload` — summary→detail 전체 수집. 실패는 전부 `CollectError`로 번역. **호출자가 재시도하지 않도록 RETRYABLE(429/5xx/네트워크)은 내부에서 최대 3회 소진 후 던짐**. 409는 즉시 `CollectError(NOT_READY, retry_after_s=캡 적용값)` — 재방문(전체 재시작)은 main의 큐가 담당. INVARIANT_BROKEN(페이지 간 메타 변화)도 즉시 던짐 — 재시작 ≤2회는 main 담당.
- HTTP→이벤트 번역표 (§5.2): 409→NOT_READY / 429·500·503·ConnectionError·Timeout→RETRYABLE / 400(invalid_cursor 제외)·404 외 4xx·형식 오류→PERMANENT_ERROR / 404→RETENTION / 200 빈 전체→EMPTY 아님(빈 records는 정상 — EMPTY 판정은 normalize 후 main에서) / invalid_cursor 400→1회 처음부터 재시작 후 재발 시 PERMANENT_ERROR

- [ ] **Step 1: 실패하는 테스트** — `collectors/token-usage/tests/test_api_client.py`

```python
import pytest

from app.api_client import fetch_service
from app.config import Config, ServiceEntry
from app.events import CollectError, Event

ENTRY = ServiceEntry(service_group="G", service="S", base_url="http://svc", enabled=True)
CFG = Config(max_pages=5)
DATE = "2026-06-15"


class FakeResponse:
    def __init__(self, status_code, body=None, headers=None):
        self.status_code = status_code
        self._body = body or {}
        self.headers = headers or {}

    def json(self):
        return self._body


class FakeSession:
    """스크립트된 응답 시퀀스를 돌려주는 requests.Session 대역."""

    def __init__(self, script):
        self.script = list(script)   # (url_substr, response) — 순서 검증
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        expect_substr, resp = self.script.pop(0)
        assert expect_substr in url, f"unexpected call {url}, expected {expect_substr}"
        return resp() if callable(resp) else resp


def page(records, next_cursor=None, gen="2026-06-16T02:05:00+09:00", group="G", service="S"):
    body = {"serviceGroup": group, "service": service, "date": DATE,
            "generatedAt": gen, "records": records}
    if next_cursor:
        body["nextCursor"] = next_cursor
    return FakeResponse(200, body)


SUMMARY = FakeResponse(200, {"serviceGroup": "G", "service": "S", "date": DATE,
                             "generatedAt": "2026-06-16T02:05:00+09:00",
                             "inputTokens": 10, "outputTokens": 2, "requests": 1,
                             "distinctUsers": 1})


def test_happy_path_two_pages():
    s = FakeSession([
        ("/v1/usage/summary", SUMMARY),
        ("/v1/usage", page([{"userId": "u1"}], next_cursor="c1")),
        ("/v1/usage", page([{"userId": "u2"}])),
    ])
    out = fetch_service(ENTRY, DATE, CFG, s)
    assert [r["userId"] for r in out.records] == ["u1", "u2"]
    assert out.pages == 2 and out.summary["inputTokens"] == 10
    assert out.generated_at == "2026-06-16T02:05:00+09:00"
    # cursor 전달 확인: 2번째 usage 호출에 cursor=c1
    assert s.calls[2][1].get("cursor") == "c1"


def test_409_raises_not_ready_with_capped_retry_after():
    s = FakeSession([("/v1/usage/summary",
                      FakeResponse(409, {"code": "data_not_ready", "message": "x"},
                                   headers={"Retry-After": "9999"}))])
    with pytest.raises(CollectError) as ei:
        fetch_service(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.NOT_READY
    assert ei.value.retry_after_s == 300          # min(RA, 300) 캡 (§5.2)


def test_retryable_exhausts_three_attempts_then_raises():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        return FakeResponse(503, {"code": "service_unavailable", "message": "x"},
                            headers={"Retry-After": "0"})

    s = FakeSession([("/v1/usage/summary", flaky)] * 3)
    with pytest.raises(CollectError) as ei:
        fetch_service(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.RETRYABLE and calls["n"] == 3


def test_404_maps_to_retention():
    s = FakeSession([("/v1/usage/summary",
                      FakeResponse(404, {"code": "data_not_retained", "message": "x"}))])
    with pytest.raises(CollectError) as ei:
        fetch_service(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.RETENTION


def test_page_invariance_violation_raises():
    s = FakeSession([
        ("/v1/usage/summary", SUMMARY),
        ("/v1/usage", page([], next_cursor="c1")),
        ("/v1/usage", page([], gen="2026-06-16T02:35:00+09:00")),   # generatedAt 변화
    ])
    with pytest.raises(CollectError) as ei:
        fetch_service(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.INVARIANT_BROKEN


def test_max_pages_exceeded_is_permanent_error():
    script = [("/v1/usage/summary", SUMMARY)]
    for i in range(CFG.max_pages):
        script.append(("/v1/usage", page([], next_cursor=f"c{i}")))
    s = FakeSession(script)
    with pytest.raises(CollectError) as ei:
        fetch_service(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert "MAX_PAGES" in ei.value.message


def test_mid_pagination_409_is_not_ready():
    s = FakeSession([
        ("/v1/usage/summary", SUMMARY),
        ("/v1/usage", page([], next_cursor="c1")),
        ("/v1/usage", FakeResponse(409, {"code": "data_not_ready", "message": "x"},
                                   headers={"Retry-After": "5"})),
    ])
    with pytest.raises(CollectError) as ei:
        fetch_service(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.NOT_READY   # 재방문 시 전체 재시작 — main 큐 담당


def test_summary_absent_is_tolerated_as_none():
    # summary 엔드포인트가 404 아닌 500 소진 등으로 실패하면 CollectError지만,
    # 스냅샷 원자성 원칙상 detail만 있는 부분 결과는 반환하지 않는다 — 전체 실패.
    # (파생 summary는 §5.9 계약상 '소스가 summary를 제공하지 않는 유형' 전용 —
    #  usage-api-v1은 summary 필수이므로 실패는 실패다)
    s = FakeSession([("/v1/usage/summary",
                      FakeResponse(400, {"code": "invalid_date", "message": "x"}))])
    with pytest.raises(CollectError) as ei:
        fetch_service(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
```

- [ ] **Step 2: 실패 확인**

Run: `cd collectors/token-usage && python -m pytest tests/test_api_client.py -v`
Expected: FAIL — `No module named 'app.events'`

- [ ] **Step 3: 구현 (1/2)** — `collectors/token-usage/app/events.py`

```python
"""공통 이벤트 분류 (스펙 §5.2 분류→정책 표의 분류 축).

정책(대기열·예산·status 매핑·exit 영향)은 main.py 오케스트레이터에 1벌만 존재한다.
api_client는 HTTP 신호를 이 분류로 번역만 한다 — 신규 소스 모듈(§5.9)도 동일 분류 사용.
"""
from enum import Enum


class Event(str, Enum):  # StrEnum은 3.11+ — 3.10 호환 형태 사용
    NOT_READY = "NOT_READY"                # 대기열 후송, 재방문=전체 재시작
    RETRYABLE = "RETRYABLE"                # 내부 재시도 소진 후 FAILURE
    PERMANENT_ERROR = "PERMANENT_ERROR"    # 즉시 FAILURE
    RETENTION = "RETENTION"                # 정기=FAILURE / 재수집=SKIPPED
    EMPTY = "EMPTY"                        # 사용량 0 — NODATA (summary는 적재)
    INVARIANT_BROKEN = "INVARIANT_BROKEN"  # 폐기 후 재시작 ≤2회


class CollectError(Exception):
    def __init__(self, event: Event, message: str, retry_after_s: int = 0):
        super().__init__(f"{event.value}: {message}")
        self.event = event
        self.message = message
        self.retry_after_s = retry_after_s
```

- [ ] **Step 4: 구현 (2/2)** — `collectors/token-usage/app/api_client.py`

```python
"""usage-api-v1 클라이언트 — HTTP 신호를 공통 이벤트 분류로 번역 (§5.2 번역표).

세션은 주입받는다(테스트: Fake, 운영: main이 프록시/CA 설정한 requests.Session).
RETRYABLE(429/5xx/네트워크)은 이 계층에서 최대 3회 소진한다. NOT_READY/INVARIANT_BROKEN의
재방문·재시작 정책은 main의 큐가 담당한다 — 여기서는 즉시 던진다.
"""
import time
from dataclasses import dataclass, field

import requests

from app.config import Config, ServiceEntry
from app.events import CollectError, Event

RETRY_AFTER_CAP_S = 300          # min(Retry-After, 300s) (§5.2)
RETRYABLE_ATTEMPTS = 3
BACKOFF_S = (5, 25, 125)         # 지수 백오프 (§5.2)
HTTP_TIMEOUT_S = 60
PAGE_LIMIT = 1000                # 계약 기본값 — 상향은 §9-6 협의


@dataclass
class UsagePayload:
    records: list[dict] = field(default_factory=list)
    summary: dict | None = None
    generated_at: str = ""
    reported_service_group: str = ""
    reported_service: str = ""
    pages: int = 0


def _capped_retry_after(resp) -> int:
    try:
        return min(int(resp.headers.get("Retry-After", "5")), RETRY_AFTER_CAP_S)
    except ValueError:
        return 5


def _error_code(resp) -> str:
    try:
        return str(resp.json().get("code", ""))
    except Exception:
        return ""


def _translate_error(resp) -> CollectError:
    """비-200 응답 → CollectError (§5.2 usage-api-v1 번역표)."""
    sc = resp.status_code
    code = _error_code(resp)
    if sc == 409:
        return CollectError(Event.NOT_READY, f"data_not_ready ({code})",
                            retry_after_s=_capped_retry_after(resp))
    if sc == 404:
        return CollectError(Event.RETENTION, f"data_not_retained ({code})")
    if sc in (429, 500, 503) or sc >= 500:
        return CollectError(Event.RETRYABLE, f"http {sc} ({code})",
                            retry_after_s=_capped_retry_after(resp))
    if sc == 400 and code == "invalid_cursor":
        # §5.2: cursor 없이 처음부터 재시작 — INVARIANT_BROKEN 경로(재시작 ≤2회)로 위임
        return CollectError(Event.INVARIANT_BROKEN, "invalid_cursor — restart pagination")
    return CollectError(Event.PERMANENT_ERROR, f"http {sc} ({code})")


def _get_with_retry(session, url: str, params: dict) -> dict:
    """GET 1회 의미 단위 — RETRYABLE만 내부 소진(≤3회), 그 외 즉시 번역해 던짐."""
    last: CollectError | None = None
    for attempt in range(RETRYABLE_ATTEMPTS):
        try:
            resp = session.get(url, params=params, timeout=HTTP_TIMEOUT_S)
        except requests.RequestException as exc:
            last = CollectError(Event.RETRYABLE, f"network: {type(exc).__name__}")
            if attempt < RETRYABLE_ATTEMPTS - 1:
                time.sleep(BACKOFF_S[attempt])
            continue
        if resp.status_code == 200:
            return resp.json()
        err = _translate_error(resp)
        if err.event is not Event.RETRYABLE:
            raise err
        last = err
        if attempt < RETRYABLE_ATTEMPTS - 1:
            time.sleep(min(err.retry_after_s or BACKOFF_S[attempt], RETRY_AFTER_CAP_S))
    raise last  # type: ignore[misc]


def fetch_service(entry: ServiceEntry, date: str, cfg: Config, session) -> UsagePayload:
    """(date, service) 원자 스냅샷 수집: summary → detail 전 페이지.

    페이지 불변성(§5.3): 매 페이지 (serviceGroup, service, date, generatedAt)이
    첫 페이지와 다르면 INVARIANT_BROKEN. 저장 generated_at = 마지막 페이지 값
    (불변성 검사를 통과했으므로 첫 페이지 값과 동일).
    """
    summary = _get_with_retry(session, f"{entry.base_url}/v1/usage/summary", {"date": date})

    out = UsagePayload(summary=summary)
    cursor: str | None = None
    anchor: tuple | None = None
    while True:
        if out.pages >= cfg.max_pages:
            raise CollectError(Event.PERMANENT_ERROR,
                               f"MAX_PAGES({cfg.max_pages}) exceeded — 부분 적재 금지 (§5.2)")
        params: dict = {"date": date, "limit": PAGE_LIMIT}
        if cursor is not None:
            params["cursor"] = cursor
        body = _get_with_retry(session, f"{entry.base_url}/v1/usage", params)
        out.pages += 1
        meta = (body.get("serviceGroup"), body.get("service"),
                body.get("date"), body.get("generatedAt"))
        if anchor is None:
            anchor = meta
            out.reported_service_group = str(body.get("serviceGroup", ""))
            out.reported_service = str(body.get("service", ""))
        elif meta != anchor:
            raise CollectError(Event.INVARIANT_BROKEN,
                               f"page meta changed at page {out.pages}: {anchor} -> {meta}")
        out.records.extend(body.get("records") or [])
        out.generated_at = str(body.get("generatedAt", ""))
        cursor = body.get("nextCursor")
        if cursor is None:
            return out
```

- [ ] **Step 5: 통과 확인** (전체 회귀 포함)

Run: `cd collectors/token-usage && python -m pytest tests/ -v`
Expected: 12 passed (config 4 + api_client 8)

- [ ] **Step 6: Commit**

```bash
git add collectors/token-usage/app/events.py collectors/token-usage/app/api_client.py collectors/token-usage/tests/test_api_client.py
git commit -m "feat(collectors): api client with event taxonomy and page invariance"
```

---

### Task 3: normalize — 행 정규화·검증·집계 (순수 함수)

**Files:**
- Create: `collectors/token-usage/app/normalize.py`
- Test: `collectors/token-usage/tests/test_normalize.py`

**Interfaces:**
- Consumes: `UsagePayload`(원시 dict 목록), `ServiceEntry`
- Produces:
  - `NormalizedRow` (frozen dataclass): `user_id: str`, `user_type: str`, `model: str`, `input_tokens: int`, `cache_read_tokens: int`, `cache_creation_tokens: int`, `output_tokens: int`, `requests: int`
  - `NormalizeResult` (dataclass): `rows: list[NormalizedRow]`, `rejected: int`, `merged_dups: int`, `warns: list[str]`(user_id 원문 미포함 — §5.6 로깅 계약), `totals: dict`(input/cache_read/cache_creation/output/requests 합)
  - `normalize_records(raw: list[dict]) -> NormalizeResult` — §5.4 규칙 전부
  - `check_identity(entry, payload) -> list[str]` — 응답 serviceGroup/service ≠ 설정값 WARN (§5.0)
  - `check_summary(totals: dict, summary: dict) -> list[str]` — Σdetail vs summary 비교 WARN (§5.1-3-4)

- [ ] **Step 1: 실패하는 테스트** — `collectors/token-usage/tests/test_normalize.py`

```python
from app.config import ServiceEntry
from app.normalize import check_identity, check_summary, normalize_records

ENTRY = ServiceEntry(service_group="G", service="S", base_url="http://x", enabled=True)


def R(**kw):
    base = {"userId": "u1", "userType": "identified", "model": "m",
            "inputTokens": 10, "outputTokens": 2, "requests": 1}
    base.update(kw)
    return base


def test_null_user_id_normalized_to_empty():
    out = normalize_records([R(userId=None, userType="unclassified")])
    assert out.rows[0].user_id == "" and out.rows[0].user_type == "unclassified"
    assert out.rejected == 0


def test_missing_cache_fields_default_zero():
    out = normalize_records([R()])
    assert out.rows[0].cache_read_tokens == 0 and out.rows[0].cache_creation_tokens == 0


def test_usertype_userid_contract_violations_rejected():
    bad = [
        R(userId=None, userType="identified"),      # identified인데 null
        R(userId="u2", userType="unclassified"),    # unclassified인데 문자열
        R(userType="alien"),                        # 미지 userType
        R(inputTokens=-1),                          # 음수
        R(inputTokens="ten"),                       # 타입 위반
        R(model=""),                                # 빈 model
    ]
    out = normalize_records(bad)
    assert out.rejected == 6 and out.rows == []
    assert len(out.warns) >= 1
    assert all("u2" not in w for w in out.warns)    # 로깅 계약: user_id 원문 금지


def test_duplicate_logical_key_sum_merged():
    out = normalize_records([R(inputTokens=10, requests=1), R(inputTokens=5, requests=2)])
    assert len(out.rows) == 1
    assert out.rows[0].input_tokens == 15 and out.rows[0].requests == 3
    assert out.merged_dups == 1


def test_totals_accumulate():
    out = normalize_records([R(), R(userId="u2", cacheReadTokens=7)])
    assert out.totals["input_tokens"] == 20
    assert out.totals["cache_read_tokens"] == 7
    assert out.totals["requests"] == 2


def test_check_identity_warns_on_drift():
    class P:
        reported_service_group = "G"
        reported_service = "S "          # 공백 드리프트
    warns = check_identity(ENTRY, P())
    assert len(warns) == 1 and "S " in warns[0]
    P.reported_service = "S"
    assert check_identity(ENTRY, P()) == []


def test_check_summary_warns_on_mismatch_and_skips_derived():
    totals = {"input_tokens": 10, "cache_read_tokens": 0, "cache_creation_tokens": 0,
              "output_tokens": 2, "requests": 1}
    ok = {"inputTokens": 10, "outputTokens": 2, "requests": 1}
    assert check_summary(totals, ok) == []
    bad = {"inputTokens": 11, "outputTokens": 2, "requests": 1}
    warns = check_summary(totals, bad)
    assert len(warns) == 1 and "inputTokens" in warns[0]
```

- [ ] **Step 2: 실패 확인**

Run: `cd collectors/token-usage && python -m pytest tests/test_normalize.py -v`
Expected: FAIL — `No module named 'app.normalize'`

- [ ] **Step 3: 구현** — `collectors/token-usage/app/normalize.py`

```python
"""행 정규화·검증·집계 (§5.4) — DB/HTTP 무접촉 순수 함수.

로깅 계약(§5.6): warns 문자열에 user_id 원문·레코드 페이로드를 넣지 않는다.
(서비스명 드리프트 경고의 서비스명은 개인정보가 아니므로 허용)
"""
from dataclasses import dataclass, field

USER_TYPES = ("identified", "anonymous", "unclassified")
TOKEN_FIELDS = (("inputTokens", "input_tokens", True),
                ("cacheReadTokens", "cache_read_tokens", False),
                ("cacheCreationTokens", "cache_creation_tokens", False),
                ("outputTokens", "output_tokens", True),
                ("requests", "requests", True))


@dataclass(frozen=True)
class NormalizedRow:
    user_id: str
    user_type: str
    model: str
    input_tokens: int
    cache_read_tokens: int
    cache_creation_tokens: int
    output_tokens: int
    requests: int


@dataclass
class NormalizeResult:
    rows: list[NormalizedRow] = field(default_factory=list)
    rejected: int = 0
    merged_dups: int = 0
    warns: list[str] = field(default_factory=list)
    totals: dict = field(default_factory=lambda: {
        "input_tokens": 0, "cache_read_tokens": 0, "cache_creation_tokens": 0,
        "output_tokens": 0, "requests": 0})


def _validate(raw: dict) -> NormalizedRow | None:
    user_type = raw.get("userType")
    if user_type not in USER_TYPES:
        return None
    user_id = raw.get("userId")
    if user_type == "unclassified":
        if user_id is not None:
            return None
        user_id = ""                         # null → '' 정규화 (§5.4)
    else:
        if not isinstance(user_id, str) or not user_id:
            return None
    model = raw.get("model")
    if not isinstance(model, str) or not model:
        return None
    values: dict[str, int] = {}
    for api_key, col, required in TOKEN_FIELDS:
        v = raw.get(api_key, None if required else 0)
        if v is None and not required:
            v = 0
        if type(v) is not int or v < 0:      # bool 거부 포함
            return None
        values[col] = v
    return NormalizedRow(user_id=user_id, user_type=user_type, model=model, **values)


def normalize_records(raw_records: list[dict]) -> NormalizeResult:
    out = NormalizeResult()
    merged: dict[tuple, NormalizedRow] = {}
    for i, raw in enumerate(raw_records):
        row = _validate(raw)
        if row is None:
            out.rejected += 1
            continue
        key = (row.user_id, row.user_type, row.model)
        if key in merged:
            prev = merged[key]
            merged[key] = NormalizedRow(
                user_id=row.user_id, user_type=row.user_type, model=row.model,
                input_tokens=prev.input_tokens + row.input_tokens,
                cache_read_tokens=prev.cache_read_tokens + row.cache_read_tokens,
                cache_creation_tokens=prev.cache_creation_tokens + row.cache_creation_tokens,
                output_tokens=prev.output_tokens + row.output_tokens,
                requests=prev.requests + row.requests)
            out.merged_dups += 1
        else:
            merged[key] = row
        for _, col, _req in TOKEN_FIELDS:
            out.totals[col] += getattr(row, col)
    out.rows = list(merged.values())
    if out.rejected:
        out.warns.append(f"rejected_rows={out.rejected} (계약 위반 — 인덱스/사유는 debug 로그 금지 대상 제외)")
    if out.merged_dups:
        out.warns.append(f"merged_duplicate_keys={out.merged_dups} (계약 위반 — SUM 병합, §5.4)")
    return out


def check_identity(entry, payload) -> list[str]:
    warns = []
    if payload.reported_service_group != entry.service_group or \
       payload.reported_service != entry.service:
        warns.append(
            f"identity_drift: reported=({payload.reported_service_group!r}, "
            f"{payload.reported_service!r}) != configured=({entry.service_group!r}, "
            f"{entry.service!r}) (§5.0 — 정본은 설정값)")
    return warns


def check_summary(totals: dict, summary: dict) -> list[str]:
    """Σdetail vs summary (§5.1-3-4). is_derived 소스는 호출하지 않는 것이 계약."""
    warns = []
    pairs = (("inputTokens", "input_tokens"), ("cacheReadTokens", "cache_read_tokens"),
             ("cacheCreationTokens", "cache_creation_tokens"),
             ("outputTokens", "output_tokens"), ("requests", "requests"))
    for api_key, col in pairs:
        reported = summary.get(api_key, 0) or 0
        if reported != totals[col]:
            warns.append(f"summary_mismatch: {api_key} reported={reported} detail_sum={totals[col]}")
    return warns
```

- [ ] **Step 4: 통과 확인** (전체 회귀)

Run: `cd collectors/token-usage && python -m pytest tests/ -v`
Expected: 19 passed

- [ ] **Step 5: Commit**

```bash
git add collectors/token-usage/app/normalize.py collectors/token-usage/tests/test_normalize.py
git commit -m "feat(collectors): pure normalization with contract validation and dedup"
```

---

### Task 4: clickhouse_client — 멱등 적재·감사·dim 교체

**Files:**
- Create: `collectors/token-usage/app/clickhouse_client.py`
- Test: `collectors/token-usage/tests/test_clickhouse_client.py`

**Interfaces:**
- Consumes: `Config`, `NormalizedRow`, `ServiceEntry`
- Produces:
  - `CHWriter(cfg, client=None)` — client는 clickhouse-connect 클라이언트 또는 테스트 Fake (`command(sql, parameters=...)`, `query(sql, parameters=...)→obj.result_rows`, `insert(table, data, column_names=...)` 인터페이스)
  - `CHWriter.replace_service_day(entry, date, rows_iter, summary_row, audit_prev) -> int` — 시퀀스: (1) 기존 행 존재 SELECT (2) 있으면 audit append 후 detail+summary DELETE(`mutations_sync=2`, `_local` [+ON CLUSTER]) (3) rows_iter를 `max_buffer_rows` 단위 배치 INSERT (4) summary 1행 INSERT. 반환: 적재 행수
  - `CHWriter.fetch_prev_summary(service, date) -> dict | None` — audit용 기존 세대 요약 (detail 합계+행수, summary의 generated_at/collected_at)
  - `CHWriter.replace_dim_services(entries, source_type='usage-api-v1')` — 자기 source_type 범위 교체 (§5.9 계약 6조)
  - 상수: `DB_FACT = "token_fact"`, `DB_DIM = "gpu_data"` (§9-18 협의 변경 지점 — 주석 명시)

- [ ] **Step 1: 실패하는 테스트** — `collectors/token-usage/tests/test_clickhouse_client.py`

```python
from app.clickhouse_client import CHWriter, DB_FACT
from app.config import Config, ServiceEntry
from app.normalize import NormalizedRow

ENTRY = ServiceEntry(service_group="G", service="S", base_url="http://x", enabled=True)
DATE = "2026-06-15"


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeCH:
    def __init__(self, existing_count=0):
        self.commands = []      # (sql, parameters)
        self.inserts = []       # (table, row_count, column_names)
        self.existing_count = existing_count

    def command(self, sql, parameters=None, settings=None):
        self.commands.append((" ".join(sql.split()), parameters, settings))

    def query(self, sql, parameters=None):
        if "count()" in sql:
            return FakeResult([[self.existing_count]])
        return FakeResult([])

    def insert(self, table, data, column_names=None):
        self.inserts.append((table, len(data), tuple(column_names or ())))


def rows(n):
    return (NormalizedRow(user_id=f"u{i}", user_type="identified", model="m",
                          input_tokens=1, cache_read_tokens=0, cache_creation_tokens=0,
                          output_tokens=1, requests=1) for i in range(n))


def summary_row():
    return {"input_tokens": 3, "cache_read_tokens": 0, "cache_creation_tokens": 0,
            "output_tokens": 3, "requests": 3, "distinct_users": 3,
            "distinct_identified_users": None, "is_derived": 0,
            "generated_at": "2026-06-16 02:05:00", "reported_service_group": "G",
            "reported_service": "S"}


def test_first_load_skips_delete_and_audit():
    ch = FakeCH(existing_count=0)
    w = CHWriter(Config(max_buffer_rows=10), client=ch)
    n = w.replace_service_day(ENTRY, DATE, rows(3), summary_row(), audit_prev=None)
    assert n == 3
    assert not any("DELETE" in c[0] for c in ch.commands)          # no-op 스킵 (§4.0)
    detail = [i for i in ch.inserts if i[0].endswith("raw_token_usage_1d_dist")]
    assert sum(i[1] for i in detail) == 3
    assert any(i[0].endswith("raw_token_usage_summary_1d_dist") for i in ch.inserts)
    assert not any(i[0].endswith("collect_audit_1d_dist") for i in ch.inserts)


def test_reload_deletes_with_mutations_sync_and_audits():
    ch = FakeCH(existing_count=5)
    w = CHWriter(Config(ch_cluster="gpu-monitoring", max_buffer_rows=10), client=ch)
    prev = {"prev_row_count": 5, "prev_input_tokens": 9, "prev_cache_read_tokens": 0,
            "prev_cache_creation_tokens": 0, "prev_output_tokens": 1, "prev_requests": 5,
            "prev_generated_at": "2026-06-16 02:05:00",
            "prev_collected_at": "2026-06-16 02:10:00"}
    w.replace_service_day(ENTRY, DATE, rows(2), summary_row(), audit_prev=prev)
    deletes = [c for c in ch.commands if "DELETE" in c[0]]
    assert len(deletes) == 2                                       # detail + summary
    assert all("_local" in c[0] for c in deletes)
    assert all("ON CLUSTER" in c[0] for c in deletes)
    assert all(c[2] and c[2].get("mutations_sync") == 2 for c in deletes)
    assert any(i[0].endswith("collect_audit_1d_dist") for i in ch.inserts)


def test_no_on_cluster_when_cluster_empty():
    ch = FakeCH(existing_count=1)
    w = CHWriter(Config(ch_cluster="", max_buffer_rows=10), client=ch)
    w.replace_service_day(ENTRY, DATE, rows(1), summary_row(), audit_prev=None)
    deletes = [c for c in ch.commands if "DELETE" in c[0]]
    assert deletes and all("ON CLUSTER" not in c[0] for c in deletes)


def test_buffer_flush_batches():
    ch = FakeCH(existing_count=0)
    w = CHWriter(Config(max_buffer_rows=4), client=ch)
    n = w.replace_service_day(ENTRY, DATE, rows(10), summary_row(), audit_prev=None)
    assert n == 10
    detail = [i for i in ch.inserts if i[0].endswith("raw_token_usage_1d_dist")]
    assert [i[1] for i in detail] == [4, 4, 2]                     # MAX_BUFFER_ROWS flush


def test_dim_replace_scopes_to_source_type():
    ch = FakeCH()
    w = CHWriter(Config(ch_cluster="gpu-monitoring"), client=ch)
    w.replace_dim_services([ENTRY])
    deletes = [c for c in ch.commands if "DELETE" in c[0]]
    assert len(deletes) == 1
    assert "source_type" in deletes[0][0] and "dim_service_local" in deletes[0][0]
    assert deletes[0][1] == {"stype": "usage-api-v1"}
    assert any(i[0].endswith("dim_service_dist") for i in ch.inserts)
```

- [ ] **Step 2: 실패 확인**

Run: `cd collectors/token-usage && python -m pytest tests/test_clickhouse_client.py -v`
Expected: FAIL — `No module named 'app.clickhouse_client'`

- [ ] **Step 3: 구현** — `collectors/token-usage/app/clickhouse_client.py`

```python
"""ClickHouse 멱등 적재 (§5.1-3-5).

시퀀스: 존재 SELECT → (있으면) 감사 append + DELETE(mutations_sync=2, _local[+ON CLUSTER])
→ 배치 INSERT(insert_distributed_sync=1). DB명은 §9-18 협의 변경 지점 — 아래 상수 2개만 수정.
"""
from datetime import datetime, timedelta, timezone

import clickhouse_connect

from app.config import Config, ServiceEntry

DB_FACT = "token_fact"   # §9-18: 공유 fact DB 확정 시 "fact"로 변경
DB_DIM = "gpu_data"      # 이슈 #1 확정

KST = timezone(timedelta(hours=9))

DETAIL_COLS = ("date", "service_group", "service", "reported_service_group",
               "reported_service", "user_id", "user_type", "model", "input_tokens",
               "cache_read_tokens", "cache_creation_tokens", "output_tokens",
               "requests", "generated_at", "collected_at")
SUMMARY_COLS = ("date", "service_group", "service", "reported_service_group",
                "reported_service", "input_tokens", "cache_read_tokens",
                "cache_creation_tokens", "output_tokens", "requests", "distinct_users",
                "distinct_identified_users", "is_derived", "generated_at", "collected_at")
AUDIT_COLS = ("date", "service", "prev_generated_at", "prev_collected_at",
              "prev_input_tokens", "prev_cache_read_tokens", "prev_cache_creation_tokens",
              "prev_output_tokens", "prev_requests", "prev_row_count", "replaced_at")
DIM_COLS = ("service_group", "service", "base_url", "enabled", "source_type",
            "note", "updated_at")


def now_kst_naive() -> datetime:
    """CH DateTime('Asia/Seoul') 컬럼용 — KST 벽시계, tzinfo 제거."""
    return datetime.now(KST).replace(tzinfo=None)


class CHWriter:
    def __init__(self, cfg: Config, client=None):
        self.cfg = cfg
        self.client = client or clickhouse_connect.get_client(
            host=cfg.ch_host, port=cfg.ch_port, username=cfg.ch_user,
            password=cfg.ch_password, settings={"insert_distributed_sync": 1})

    def _on_cluster(self) -> str:
        return f" ON CLUSTER '{self.cfg.ch_cluster}'" if self.cfg.ch_cluster else ""

    def _exists(self, table_dist: str, date: str, service: str) -> bool:
        r = self.client.query(
            f"SELECT count() FROM {table_dist} WHERE date = %(d)s AND service = %(s)s",
            parameters={"d": date, "s": service})
        return bool(r.result_rows and r.result_rows[0][0])

    def _delete_day(self, table_local: str, date: str, service: str) -> None:
        self.client.command(
            f"ALTER TABLE {table_local}{self._on_cluster()} "
            f"DELETE WHERE date = %(d)s AND service = %(s)s",
            parameters={"d": date, "s": service},
            settings={"mutations_sync": 2})

    def fetch_prev_summary(self, service: str, date: str) -> dict | None:
        """교체 전 세대 요약 — 감사(§8.4)용. summary 행 존재를 앵커로 사용
        (NODATA 세대는 detail 0행 + summary 1행 — 이 경우도 감사 대상)."""
        s = self.client.query(
            f"SELECT generated_at, collected_at "
            f"FROM {DB_FACT}.raw_token_usage_summary_1d_dist "
            f"WHERE date = %(d)s AND service = %(s)s "
            f"ORDER BY collected_at DESC LIMIT 1",
            parameters={"d": date, "s": service})
        if not s.result_rows:
            return None
        gen, col = s.result_rows[0]
        d = self.client.query(
            f"SELECT count(), sum(input_tokens), sum(cache_read_tokens), "
            f"sum(cache_creation_tokens), sum(output_tokens), sum(requests) "
            f"FROM {DB_FACT}.raw_token_usage_1d_dist "
            f"WHERE date = %(d)s AND service = %(s)s",
            parameters={"d": date, "s": service})
        c, i, cr, cc, o, q = (d.result_rows[0] if d.result_rows else (0, 0, 0, 0, 0, 0))
        return {"prev_row_count": c, "prev_input_tokens": i or 0,
                "prev_cache_read_tokens": cr or 0, "prev_cache_creation_tokens": cc or 0,
                "prev_output_tokens": o or 0, "prev_requests": q or 0,
                "prev_generated_at": gen, "prev_collected_at": col}

    def replace_service_day(self, entry: ServiceEntry, date: str, rows_iter,
                            summary_row: dict, audit_prev: dict | None) -> int:
        detail_dist = f"{DB_FACT}.raw_token_usage_1d_dist"
        detail_local = f"{DB_FACT}.raw_token_usage_1d_local"
        summary_dist = f"{DB_FACT}.raw_token_usage_summary_1d_dist"
        summary_local = f"{DB_FACT}.raw_token_usage_summary_1d_local"
        if self._exists(detail_dist, date, entry.service) or \
           self._exists(summary_dist, date, entry.service):
            if audit_prev:
                self.client.insert(
                    f"{DB_FACT}.collect_audit_1d_dist",
                    [[date, entry.service, audit_prev["prev_generated_at"],
                      audit_prev["prev_collected_at"], audit_prev["prev_input_tokens"],
                      audit_prev["prev_cache_read_tokens"],
                      audit_prev["prev_cache_creation_tokens"],
                      audit_prev["prev_output_tokens"], audit_prev["prev_requests"],
                      audit_prev["prev_row_count"], now_kst_naive()]],
                    column_names=AUDIT_COLS)
            self._delete_day(detail_local, date, entry.service)
            self._delete_day(summary_local, date, entry.service)

        collected_at = now_kst_naive()
        total = 0
        buf: list[list] = []

        def flush():
            nonlocal buf
            if buf:
                self.client.insert(detail_dist, buf, column_names=DETAIL_COLS)
                buf = []

        for row in rows_iter:
            buf.append([date, entry.service_group, entry.service,
                        summary_row["reported_service_group"],
                        summary_row["reported_service"], row.user_id, row.user_type,
                        row.model, row.input_tokens, row.cache_read_tokens,
                        row.cache_creation_tokens, row.output_tokens, row.requests,
                        summary_row["generated_at"], collected_at])
            total += 1
            if len(buf) >= self.cfg.max_buffer_rows:   # §5.1 메모리 규칙
                flush()
        flush()

        self.client.insert(
            summary_dist,
            [[date, entry.service_group, entry.service,
              summary_row["reported_service_group"], summary_row["reported_service"],
              summary_row["input_tokens"], summary_row["cache_read_tokens"],
              summary_row["cache_creation_tokens"], summary_row["output_tokens"],
              summary_row["requests"], summary_row["distinct_users"],
              summary_row["distinct_identified_users"], summary_row["is_derived"],
              summary_row["generated_at"], collected_at]],
            column_names=SUMMARY_COLS)
        return total

    def replace_dim_services(self, entries: list[ServiceEntry],
                             source_type: str = "usage-api-v1") -> None:
        """자기 source_type 범위만 원자 교체 (§5.9 계약 6조 — 타 모듈 등록분 보호)."""
        self.client.command(
            f"ALTER TABLE {DB_DIM}.dim_service_local{self._on_cluster()} "
            f"DELETE WHERE source_type = %(stype)s",
            parameters={"stype": source_type},
            settings={"mutations_sync": 2})
        now = now_kst_naive()
        self.client.insert(
            f"{DB_DIM}.dim_service_dist",
            [[e.service_group, e.service, e.base_url, 1 if e.enabled else 0,
              e.source_type, "", now] for e in entries],
            column_names=DIM_COLS)
```

- [ ] **Step 4: 통과 확인** (전체 회귀)

Run: `cd collectors/token-usage && python -m pytest tests/ -v`
Expected: 26 passed (24+2)

- [ ] **Step 5: Commit**

```bash
git add collectors/token-usage/app/clickhouse_client.py collectors/token-usage/tests/test_clickhouse_client.py
git commit -m "feat(collectors): idempotent CH writer with audit and scoped dim replace"
```

---

### Task 5: vm_push — 서비스 합계 게이지

**Files:**
- Create: `collectors/token-usage/app/vm_push.py`
- Test: `collectors/token-usage/tests/test_vm_push.py` (신규)

**Interfaces:**
- Consumes: `Config`, summary dict(API 원문), `ServiceEntry`
- Produces: `push_service_summary(cfg, entry, date: str, summary: dict, session) -> list[str]`
  - `cfg.vm_push_url` 빈 값 또는 summary가 파생(is_derived)이면 **push 생략** (§5.5)
  - `POST {vm_push_url}/api/v1/import/prometheus`, 타임스탬프 = `date 23:59:59 KST`의 epoch ms
  - 게이지 6종: `token_usage_daily_{input,cache_read,cache_creation,output}_tokens`, `token_usage_daily_requests`, `token_usage_daily_reported_distinct_users` — 레이블 `{service_group, service}`
  - 실패는 WARN 문자열 반환 (배치 실패 아님 — §5.5)

- [ ] **Step 1: 실패하는 테스트** — `collectors/token-usage/tests/test_vm_push.py`

```python
from app.config import Config, ServiceEntry
from app.vm_push import push_service_summary

ENTRY = ServiceEntry(service_group="G", service="S", base_url="http://x", enabled=True)
SUMMARY = {"inputTokens": 10, "cacheReadTokens": 1, "cacheCreationTokens": 2,
           "outputTokens": 3, "requests": 4, "distinctUsers": 5}


class FakeSession:
    def __init__(self, status=204):
        self.posts = []
        self.status = status

    def post(self, url, data=None, timeout=None):
        self.posts.append((url, data))
        class R:
            status_code = self.status
        return R()


def test_push_skipped_when_url_empty():
    s = FakeSession()
    warns = push_service_summary(Config(vm_push_url=""), ENTRY, "2026-06-15", SUMMARY, s)
    assert warns == [] and s.posts == []


def test_push_lines_and_timestamp():
    s = FakeSession()
    warns = push_service_summary(Config(vm_push_url="http://vm:8480"), ENTRY,
                                 "2026-06-15", SUMMARY, s)
    assert warns == []
    url, data = s.posts[0]
    assert url == "http://vm:8480/api/v1/import/prometheus"
    lines = data.strip().split("\n")
    assert len(lines) == 6
    assert 'token_usage_daily_input_tokens{service_group="G",service="S"} 10' in lines[0]
    # 타임스탬프 = 2026-06-15 23:59:59 KST = 2026-06-15T14:59:59Z → epoch ms
    assert lines[0].endswith(" 1781535599000")
    assert any("reported_distinct_users" in ln and " 5 " in ln for ln in lines)


def test_push_failure_is_warn_not_raise():
    s = FakeSession(status=500)
    warns = push_service_summary(Config(vm_push_url="http://vm:8480"), ENTRY,
                                 "2026-06-15", SUMMARY, s)
    assert len(warns) == 1 and "vm_push_failed" in warns[0]
```

- [ ] **Step 2: 실패 확인**

Run: `cd collectors/token-usage && python -m pytest tests/test_vm_push.py -v`
Expected: FAIL — `No module named 'app.vm_push'`

- [ ] **Step 3: 구현** — `collectors/token-usage/app/vm_push.py`

```python
"""VictoriaMetrics 게이지 push (§5.5) — 서비스 단위 summary 보고값만.

distinct_users는 비가산(교차 sum 금지) — 게이지명에 reported_ 접두로 의미 고정.
push 실패는 WARN (CH가 원천). rerun 경로는 main이 이 함수를 호출하지 않는다(기본 생략).
"""
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))

GAUGES = (("token_usage_daily_input_tokens", "inputTokens"),
          ("token_usage_daily_cache_read_tokens", "cacheReadTokens"),
          ("token_usage_daily_cache_creation_tokens", "cacheCreationTokens"),
          ("token_usage_daily_output_tokens", "outputTokens"),
          ("token_usage_daily_requests", "requests"),
          ("token_usage_daily_reported_distinct_users", "distinctUsers"))


def _label_escape(v: str) -> str:
    return v.replace("\\", "\\\\").replace('"', '\\"')


def push_service_summary(cfg, entry, date: str, summary: dict, session) -> list[str]:
    if not cfg.vm_push_url:
        return []
    if summary.get("is_derived"):
        return []          # 파생 summary는 '보고값'이 아님 — push 생략 (§4.1)
    ts_ms = int(datetime.fromisoformat(f"{date}T23:59:59+09:00").timestamp() * 1000)
    labels = (f'service_group="{_label_escape(entry.service_group)}",'
              f'service="{_label_escape(entry.service)}"')
    lines = [f"{name}{{{labels}}} {int(summary.get(key, 0) or 0)} {ts_ms}"
             for name, key in GAUGES]
    try:
        resp = session.post(f"{cfg.vm_push_url}/api/v1/import/prometheus",
                            data="\n".join(lines) + "\n", timeout=30)
        if resp.status_code >= 300:
            return [f"vm_push_failed: http {resp.status_code} (service={entry.service})"]
    except Exception as exc:
        return [f"vm_push_failed: {type(exc).__name__} (service={entry.service})"]
    return []
```

- [ ] **Step 4: 통과 확인** (전체 회귀)

Run: `cd collectors/token-usage && python -m pytest tests/ -v`
Expected: 27 passed

- [ ] **Step 5: Commit**

```bash
git add collectors/token-usage/app/vm_push.py collectors/token-usage/tests/test_vm_push.py
git commit -m "feat(collectors): VM gauge push for reported service summaries"
```

---

### Task 6: main — 오케스트레이터 (큐·데드라인·마커·CLI)

**Files:**
- Create: `collectors/token-usage/app/main.py`
- Test: `collectors/token-usage/tests/test_main.py`

**Interfaces:**
- Consumes: Task 1~5 전부 (전부 주입 가능 — 테스트는 Fake)
- Produces:
  - `run_collection(cfg, entries, target_date, *, is_rerun, clock, sleeper, fetcher, writer, pusher) -> int` — exit code (0/1). 의존성 전부 파라미터 (기본값: 실제 구현)
  - `ServiceOutcome` (dataclass): `service, status('SUCCESS|NODATA|SKIPPED|FAILURE'), rows, pages, warns, rejected, reason`
  - `main(argv) -> int` — CLI: `[batch_time]`, `--from/--to`, `--service`; SIGTERM 핸들러
  - 마커 형식(§5.6): 서비스별 `SERVICE_RESULT status=%s module=token-usage service=%s source_type=usage-api-v1 rows=%d pages=%d warn=%d rejected=%d` / 최종 `BATCH_RESULT status=%s module=token-usage services_ok=%d services_failed=%d services_skipped=%d rows=%d elapsed=%ds`
- 정책 구현 (§5.2 분류→정책 표 — 정책은 여기 1벌):
  - NOT_READY → `(entry, resume_at)` 큐 끝 재삽입, 서비스별 누적 대기 추적(`not_ready_budget_minutes` 초과 시 FAILURE), 재방문 = fetch 전체 재시작
  - INVARIANT_BROKEN → 재시작 카운터 ≤2회, 초과 FAILURE
  - RETENTION → is_rerun ? SKIPPED : FAILURE
  - RETRYABLE 소진·PERMANENT_ERROR → FAILURE
  - 빈 records → NODATA (summary는 적재)
  - 소프트 데드라인: 모든 대기·착수 전 체크. **적재 시퀀스 예산 12분** — 잔여 < 예산이면 착수 안 함
  - 큐가 전부 NOT_READY 대기 상태면 가장 이른 resume_at까지 sleep (sleeper 주입)

- [ ] **Step 1: 실패하는 테스트** — `collectors/token-usage/tests/test_main.py`

```python
from app.api_client import UsagePayload
from app.config import Config, ServiceEntry
from app.events import CollectError, Event
from app.main import run_collection

E1 = ServiceEntry(service_group="G", service="S1", base_url="http://a", enabled=True)
E2 = ServiceEntry(service_group="G", service="S2", base_url="http://b", enabled=True)
DATE = "2026-06-15"


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def payload(n_records=2, group="G", service=None, entry=None):
    recs = [{"userId": f"u{i}", "userType": "identified", "model": "m",
             "inputTokens": 1, "outputTokens": 1, "requests": 1} for i in range(n_records)]
    return UsagePayload(records=recs,
                        summary={"inputTokens": n_records, "outputTokens": n_records,
                                 "requests": n_records, "distinctUsers": n_records},
                        generated_at="2026-06-16T02:05:00+09:00",
                        reported_service_group=group,
                        reported_service=service or (entry.service if entry else "S1"),
                        pages=1)


class FakeWriter:
    def __init__(self):
        self.loaded = []

    def fetch_prev_summary(self, service, date):
        return None

    def replace_service_day(self, entry, date, rows_iter, summary_row, audit_prev):
        rows = list(rows_iter)
        self.loaded.append((entry.service, len(rows)))
        return len(rows)

    def replace_dim_services(self, entries, source_type="usage-api-v1"):
        self.dim = [e.service for e in entries]


def run(entries, fetcher, *, is_rerun=False, clock=None, cfg=None, sleeps=None):
    w = FakeWriter()
    code = run_collection(
        cfg or Config(), entries, DATE, is_rerun=is_rerun,
        clock=clock or Clock(), sleeper=(sleeps.append if sleeps is not None else lambda s: None),
        fetcher=fetcher, writer=w, pusher=lambda *a, **k: [])
    return code, w


def test_all_success_exit_zero(capsys):
    code, w = run([E1, E2], lambda e, d, c, s: payload(entry=e))
    assert code == 0 and [x[0] for x in w.loaded] == ["S1", "S2"]
    out = capsys.readouterr().out
    assert out.count("SERVICE_RESULT status=SUCCESS") == 2
    assert "BATCH_RESULT status=SUCCESS module=token-usage services_ok=2 services_failed=0" in out


def test_empty_records_is_nodata_but_summary_loaded(capsys):
    code, w = run([E1], lambda e, d, c, s: payload(0, entry=e))
    assert code == 0
    assert w.loaded == [("S1", 0)]              # summary 행 적재 경로는 replace 호출로 표현
    assert "SERVICE_RESULT status=NODATA" in capsys.readouterr().out


def test_permanent_error_isolated_other_service_succeeds(capsys):
    def fetcher(e, d, c, s):
        if e.service == "S1":
            raise CollectError(Event.PERMANENT_ERROR, "http 400")
        return payload(entry=e)
    code, w = run([E1, E2], fetcher)
    assert code == 1                            # 부분 실패 → exit 1, 성공분 유지
    assert w.loaded == [("S2", 2)]
    out = capsys.readouterr().out
    assert "SERVICE_RESULT status=FAILURE" in out and "SERVICE_RESULT status=SUCCESS" in out
    assert "BATCH_RESULT status=FAILURE" in out


def test_retention_split_by_run_mode(capsys):
    def fetcher(e, d, c, s):
        raise CollectError(Event.RETENTION, "404")
    code, _ = run([E1], fetcher, is_rerun=True)
    assert code == 0                            # 재수집: SKIPPED, exit 영향 없음
    assert "SERVICE_RESULT status=SKIPPED" in capsys.readouterr().out
    code, _ = run([E1], fetcher, is_rerun=False)
    assert code == 1                            # 일일 정기: FAILURE (§5.2)
    assert "SERVICE_RESULT status=FAILURE" in capsys.readouterr().out


def test_not_ready_requeues_then_succeeds(capsys):
    clock = Clock()
    calls = {"n": 0}
    sleeps = []

    def fetcher(e, d, c, s):
        calls["n"] += 1
        if calls["n"] == 1:
            raise CollectError(Event.NOT_READY, "not ready", retry_after_s=60)
        return payload(entry=e)

    def sleeper(s):                             # sleep이 시계를 전진시켜야 재방문 도달
        sleeps.append(s)
        clock.t += s

    w = FakeWriter()
    code = run_collection(Config(), [E1], DATE, is_rerun=False, clock=clock,
                          sleeper=sleeper, fetcher=fetcher, writer=w,
                          pusher=lambda *a, **k: [])
    assert code == 0 and calls["n"] == 2        # 재방문 = 전체 재시작(fetch 재호출)
    assert sleeps and sleeps[0] >= 60           # resume_at까지 sleep


def test_not_ready_budget_exhausted_fails(capsys):
    def fetcher(e, d, c, s):
        # 단일 방문의 대기 요구가 예산(30분=1800s)을 즉시 초과 — main은 캡하지 않음(캡은 api 계층)
        raise CollectError(Event.NOT_READY, "not ready", retry_after_s=1900)

    code, _ = run([E1], fetcher, cfg=Config(not_ready_budget_minutes=30))
    assert code == 1
    out = capsys.readouterr().out
    assert "SERVICE_RESULT status=FAILURE" in out and "not_ready_budget" in out


def test_invariant_broken_restarts_twice_then_fails(capsys):
    calls = {"n": 0}

    def fetcher(e, d, c, s):
        calls["n"] += 1
        raise CollectError(Event.INVARIANT_BROKEN, "meta changed")

    code, _ = run([E1], fetcher)
    assert code == 1 and calls["n"] == 3        # 최초 + 재시작 2회 (§5.3)


def test_soft_deadline_marks_remaining_failed(capsys):
    clock = Clock()

    def fetcher(e, d, c, s):
        clock.t += 51 * 60                      # 첫 서비스가 51분 소모
        return payload(entry=e)

    code, w = run([E1, E2], fetcher, clock=clock, cfg=Config(soft_deadline_minutes=50))
    assert code == 1
    assert w.loaded == [("S1", 2)]              # S2는 착수 안 함
    out = capsys.readouterr().out
    assert "deadline" in out and "BATCH_RESULT" in out   # 정상 종료 + 마커 보장


def test_identity_drift_counted_as_warn(capsys):
    code, _ = run([E1], lambda e, d, c, s: payload(entry=e, group="G-DRIFT"))
    assert code == 0
    assert "warn=1" in capsys.readouterr().out  # §5.0 CHECK WARN
```

- [ ] **Step 2: 실패 확인**

Run: `cd collectors/token-usage && python -m pytest tests/test_main.py -v`
Expected: FAIL — `No module named 'app.main'`

- [ ] **Step 3: 구현** — `collectors/token-usage/app/main.py`

```python
"""수집 오케스트레이터 — 정책(§5.2 분류→정책 표)은 이 파일에 1벌만 존재한다.

로깅 계약(§5.6): 어떤 로그에도 레코드 페이로드·user_id 원문을 남기지 않는다.
"""
import argparse
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import date as date_cls, datetime, timedelta, timezone

import requests

from app import api_client, vm_push
from app.clickhouse_client import CHWriter
from app.config import Config, ServiceEntry, load_config, load_endpoints
from app.events import CollectError, Event
from app.normalize import check_identity, check_summary, normalize_records

KST = timezone(timedelta(hours=9))
LOAD_BUDGET_S = 12 * 60          # 적재 시퀀스 예산 (§5.2 v1.4)
INVARIANT_RESTARTS = 2

_batch_status = {"line": "BATCH_RESULT status=FAILURE module=token-usage "
                         "services_ok=0 services_failed=0 services_skipped=0 rows=0 elapsed=0s"}


def _sigterm_handler(signum, frame):
    print(_batch_status["line"] + " note=sigterm", flush=True)     # §5.1-4 마커 보장
    sys.exit(1)


@dataclass
class ServiceOutcome:
    service: str
    status: str = "FAILURE"
    rows: int = 0
    pages: int = 0
    warns: int = 0
    rejected: int = 0
    reason: str = ""


@dataclass
class _QueueItem:
    entry: ServiceEntry
    resume_at: float = 0.0
    waited_s: float = 0.0
    restarts: int = 0


def _service_line(o: ServiceOutcome) -> str:
    return (f"SERVICE_RESULT status={o.status} module=token-usage service={o.service} "
            f"source_type=usage-api-v1 rows={o.rows} pages={o.pages} "
            f"warn={o.warns} rejected={o.rejected}"
            + (f" reason={o.reason}" if o.reason else ""))


def _collect_one(cfg: Config, entry: ServiceEntry, target_date: str,
                 fetcher, writer, pusher, is_rerun: bool) -> ServiceOutcome:
    o = ServiceOutcome(service=entry.service)
    payload = fetcher(entry, target_date, cfg, _session(cfg))
    o.pages = payload.pages
    norm = normalize_records(payload.records)
    o.rejected = norm.rejected
    warns = list(norm.warns)
    warns += check_identity(entry, payload)
    warns += check_summary(norm.totals, payload.summary or {})
    o.warns = len(warns)
    for w in warns:
        print(f"CHECK WARN service={entry.service} {w}", flush=True)

    s = payload.summary or {}
    summary_row = {
        "reported_service_group": payload.reported_service_group,
        "reported_service": payload.reported_service,
        "input_tokens": int(s.get("inputTokens", 0) or 0),
        "cache_read_tokens": int(s.get("cacheReadTokens", 0) or 0),
        "cache_creation_tokens": int(s.get("cacheCreationTokens", 0) or 0),
        "output_tokens": int(s.get("outputTokens", 0) or 0),
        "requests": int(s.get("requests", 0) or 0),
        "distinct_users": int(s.get("distinctUsers", 0) or 0),
        "distinct_identified_users": s.get("distinctIdentifiedUsers"),
        "is_derived": 0,
        "generated_at": _kst_naive(payload.generated_at),
    }
    audit_prev = writer.fetch_prev_summary(entry.service, target_date)
    o.rows = writer.replace_service_day(entry, target_date, iter(norm.rows),
                                        summary_row, audit_prev)
    if not is_rerun:
        for w in pusher(cfg, entry, target_date, {**s, "is_derived": 0}, _session(cfg)):
            o.warns += 1
            print(f"CHECK WARN service={entry.service} {w}", flush=True)
    o.status = "NODATA" if o.rows == 0 else "SUCCESS"   # EMPTY → NODATA (§5.2)
    return o


def _kst_naive(iso_str: str) -> datetime:
    try:
        return datetime.fromisoformat(iso_str).astimezone(KST).replace(tzinfo=None)
    except ValueError:
        return datetime.now(KST).replace(tzinfo=None)


_sessions: dict = {}


def _session(cfg: Config):
    key = id(cfg)
    if key not in _sessions:
        sess = requests.Session()
        if cfg.https_proxy is not None:                  # ''=직접, 값=전용 (§5.7)
            sess.proxies = {"http": cfg.https_proxy or None,
                            "https": cfg.https_proxy or None}
            sess.trust_env = bool(cfg.https_proxy)
        sess.verify = cfg.api_verify
        _sessions[key] = sess
    return _sessions[key]


def run_collection(cfg: Config, entries: list[ServiceEntry], target_date: str, *,
                   is_rerun: bool = False, clock=time.monotonic, sleeper=time.sleep,
                   fetcher=api_client.fetch_service, writer=None,
                   pusher=vm_push.push_service_summary) -> int:
    started = clock()
    deadline = started + cfg.soft_deadline_minutes * 60
    writer = writer or CHWriter(cfg)
    writer.replace_dim_services([e for e in entries])    # 레지스트리 반영 (§5.1-2)

    queue = [_QueueItem(entry=e) for e in entries if e.enabled]
    outcomes: list[ServiceOutcome] = []

    while queue:
        now = clock()
        if now >= deadline or deadline - now < LOAD_BUDGET_S:
            for item in queue:                            # 잔여 전부 FAILURE, 정상 종료 (§5.2)
                outcomes.append(ServiceOutcome(service=item.entry.service,
                                               reason="deadline"))
            queue.clear()
            break
        ready = [q for q in queue if q.resume_at <= now]
        if not ready:
            wake = min(q.resume_at for q in queue)
            sleeper(min(wake - now, deadline - now))
            continue
        item = ready[0]
        queue.remove(item)
        try:
            outcomes.append(_collect_one(cfg, item.entry, target_date,
                                         fetcher, writer, pusher, is_rerun))
        except CollectError as err:
            if err.event is Event.NOT_READY:
                item.waited_s += max(err.retry_after_s, 1)
                if item.waited_s > cfg.not_ready_budget_minutes * 60:
                    outcomes.append(ServiceOutcome(service=item.entry.service,
                                                   reason="not_ready_budget"))
                else:
                    item.resume_at = clock() + max(err.retry_after_s, 1)
                    queue.append(item)                    # 큐 끝 재삽입 — 전체 재시작 (§5.2)
                    continue
            elif err.event is Event.INVARIANT_BROKEN and item.restarts < INVARIANT_RESTARTS:
                item.restarts += 1
                queue.append(item)                        # 폐기 후 재시작 ≤2회 (§5.3)
                continue
            elif err.event is Event.RETENTION and is_rerun:
                outcomes.append(ServiceOutcome(service=item.entry.service,
                                               status="SKIPPED", reason="retention"))
            else:
                outcomes.append(ServiceOutcome(service=item.entry.service,
                                               reason=err.event.value))
        except Exception as exc:                          # 예상 밖 — 서비스 격리 유지
            outcomes.append(ServiceOutcome(service=item.entry.service,
                                           reason=f"unexpected:{type(exc).__name__}"))

    for o in outcomes:
        print(_service_line(o), flush=True)

    ok = sum(1 for o in outcomes if o.status in ("SUCCESS", "NODATA"))
    failed = sum(1 for o in outcomes if o.status == "FAILURE")
    skipped = sum(1 for o in outcomes if o.status == "SKIPPED")
    total_rows = sum(o.rows for o in outcomes)
    if failed:
        status = "FAILURE"
    elif outcomes and all(o.status == "NODATA" for o in outcomes):
        status = "NODATA"
    else:
        status = "SUCCESS"
    line = (f"BATCH_RESULT status={status} module=token-usage services_ok={ok} "
            f"services_failed={failed} services_skipped={skipped} rows={total_rows} "
            f"elapsed={int(clock() - started)}s")
    _batch_status["line"] = line
    print(line, flush=True)
    return 1 if failed else 0


def _target_dates(args) -> tuple[list[str], bool]:
    if args.from_date or args.to_date:
        if not (args.from_date and args.to_date):
            print("--from/--to는 쌍으로 지정 (KST, YYYY-MM-DD)", file=sys.stderr)
            sys.exit(2)
        d0 = date_cls.fromisoformat(args.from_date)
        d1 = date_cls.fromisoformat(args.to_date)
        return [str(d0 + timedelta(days=i)) for i in range((d1 - d0).days + 1)], True
    batch_time = (datetime.fromisoformat(args.batch_time).astimezone(KST)
                  if args.batch_time else datetime.now(KST))
    if batch_time.tzinfo is None:
        batch_time = batch_time.replace(tzinfo=KST)
    return [str(batch_time.date() - timedelta(days=1))], False


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_time", nargs="?", default=None,
                        help="ISO8601 (기본 now, KST) — target_date = batch_time - 1일")
    parser.add_argument("--from", dest="from_date", default=None)
    parser.add_argument("--to", dest="to_date", default=None)
    parser.add_argument("--service", default=None, help="단일 서비스만 (재수집용)")
    args = parser.parse_args(argv)

    signal.signal(signal.SIGTERM, _sigterm_handler)
    cfg = load_config()
    entries = load_endpoints(cfg.endpoints_file)
    if args.service:
        entries = [e for e in entries if e.service == args.service]
        if not entries:
            print(f"unknown service: {args.service}", file=sys.stderr)
            return 2
    dates, is_rerun = _target_dates(args)
    worst = 0
    for d in dates:
        worst = max(worst, run_collection(cfg, entries, d, is_rerun=is_rerun))
    return worst


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: 통과 확인** (전체 회귀)

Run: `cd collectors/token-usage && python -m pytest tests/ -v`
Expected: 36 passed

- [ ] **Step 5: Commit**

```bash
git add collectors/token-usage/app/main.py collectors/token-usage/tests/test_main.py
git commit -m "feat(collectors): orchestrator with policy table, deadline, and markers"
```

---

### Task 7: CI E2E — mock-provider + ClickHouse 컨테이너

**Files:**
- Create: `collectors/token-usage/tests/e2e/ci_expectations.py`
- Create: `collectors/token-usage/tests/e2e/verify_expected_results.sql`
- Create: `collectors/token-usage/tests/e2e/run_e2e.sh`
- Create: `.github/workflows/test-collector.yml`

**Interfaces:**
- Consumes: mock-provider(PR #2 — 결정적: 같은 seed+date=같은 데이터), DDL 초안(PR #3 파일 — CI는 단일노드이므로 `sed`로 Replicated→MergeTree·ON CLUSTER 제거 변환해 적용)
- Produces: `run_e2e.sh` — 로컬(도커 있는 환경)과 CI 공용 E2E 스크립트

- [ ] **Step 1: 기대값 산출기** — `collectors/token-usage/tests/e2e/ci_expectations.py`

```python
"""mock-provider의 결정성으로 CI 기대값을 산출한다.

mock의 datagen 로직(sha256 기반)을 그대로 재현 — mock 저장소가 이 레포 안에 있으므로
tools/mock-provider/app을 직접 import해 기대 행수·합계를 계산하고, ClickHouse 적재
결과와 비교할 SQL 상수를 출력한다.
사용: python ci_expectations.py <date> <seed> <users> <anon> <models(콤마)>
출력: "rows=<n> input_sum=<n> requests_sum=<n>"
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tools" / "mock-provider"))

from app.config import Config as MockConfig            # noqa: E402
from app.datagen import build_records, build_summary   # noqa: E402


def main():
    date, seed, users, anon, models = sys.argv[1:6]
    cfg = MockConfig(users=int(users), anon_users=int(anon),
                     models=[m for m in models.split(",") if m], seed=seed)
    records = build_records(cfg, date)
    summary = build_summary(records)
    print(f"rows={len(records)} input_sum={summary['inputTokens']} "
          f"requests_sum={summary['requests']}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 검증 SQL** — `collectors/token-usage/tests/e2e/verify_expected_results.sql`

```sql
-- --expect-empty 방식: 기대와 다른 행만 SELECT — 출력 없으면 통과 (동료 s2job 패턴)
-- 실행 전 치환: {DATE} {SERVICE} {EXP_ROWS} {EXP_INPUT} {EXP_REQ}

SELECT 'detail_row_count_mismatch' AS check_name, count() AS actual, {EXP_ROWS} AS expected
FROM token_fact.raw_token_usage_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != {EXP_ROWS}

UNION ALL

SELECT 'detail_input_sum_mismatch', sum(input_tokens), {EXP_INPUT}
FROM token_fact.raw_token_usage_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING sum(input_tokens) != {EXP_INPUT}

UNION ALL

SELECT 'summary_row_missing', count(), 1
FROM token_fact.raw_token_usage_summary_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != 1

UNION ALL

SELECT 'summary_matches_detail', s.requests, {EXP_REQ}
FROM token_fact.raw_token_usage_summary_1d_dist AS s
WHERE s.date = '{DATE}' AND s.service = '{SERVICE}' AND s.requests != {EXP_REQ}

UNION ALL

SELECT 'dim_service_registered', count(), 1
FROM gpu_data.dim_service_dist
WHERE service = '{SERVICE}' AND source_type = 'usage-api-v1'
HAVING count() != 1

UNION ALL

-- 재수집 멱등성: 2회 실행 후에도 행수 동일 (E2E 스크립트가 2회 실행)
SELECT 'no_duplicate_after_rerun', count(), {EXP_ROWS}
FROM token_fact.raw_token_usage_1d_dist
WHERE date = '{DATE}' AND service = '{SERVICE}'
HAVING count() != {EXP_ROWS}
```

- [ ] **Step 3: E2E 스크립트** — `collectors/token-usage/tests/e2e/run_e2e.sh`

```bash
#!/usr/bin/env bash
# CI/로컬(도커 필요) E2E: CH 컨테이너 + mock-provider 컨테이너 → DDL(단일노드 변환)
# → 수집 2회(멱등성) → verify --expect-empty → 시나리오 케이스(identity drift WARN)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."     # collectors/token-usage

DATE_ARG="${1:-$(date -d "yesterday" +%F)}"
SEED="e2e-seed-1"

docker network create tokene2e 2>/dev/null || true
trap 'docker rm -f ch-e2e mock-e2e >/dev/null 2>&1 || true' EXIT

docker run -d --rm --name ch-e2e --network tokene2e -p 18123:8123 \
  clickhouse/clickhouse-server:24.8
docker run -d --rm --name mock-e2e --network tokene2e -p 18000:8000 \
  -e MOCK_SERVICE_GROUP="Mock Group" -e MOCK_SERVICE="Mock Service A" \
  -e MOCK_SEED="${SEED}" -e MOCK_USERS=30 -e MOCK_ANON_USERS=5 \
  token-mock-provider:e2e

for _ in $(seq 1 60); do
  curl -sf http://127.0.0.1:18123/ping >/dev/null && \
  curl -sf http://127.0.0.1:18000/healthz >/dev/null && break
  sleep 1
done

# DDL: 초안(company)을 단일노드용으로 변환 — Replicated 제거, ON CLUSTER 제거, dist→local 뷰 없이
python3 - <<'PY'
import re, pathlib, urllib.request

sql = pathlib.Path("ddl/company/raw_token_usage.sql").read_text()
sql += "\nCREATE DATABASE IF NOT EXISTS gpu_data;\n"
sql += pathlib.Path("ddl/company/dim_service.sql").read_text()
sql = re.sub(r"ON CLUSTER 'gpu-monitoring'", "", sql)
sql = re.sub(r"ENGINE = ReplicatedMergeTree\([^)]*\)", "ENGINE = MergeTree", sql, flags=re.S)
sql = re.sub(r"ENGINE = Distributed\('gpu-monitoring', '(\w+)', '(\w+)',[^)]*\)",
             r"ENGINE = Distributed('default', '\1', '\2', rand())", sql)
for stmt in sql.split(";"):
    if stmt.strip():
        req = urllib.request.Request("http://127.0.0.1:18123/", data=(stmt + ";").encode())
        urllib.request.urlopen(req).read()
print("DDL applied (single-node transformed)")
PY

export CH_HOST=127.0.0.1 CH_PORT=18123 CH_CLUSTER="" VM_PUSH_URL=""
export ENDPOINTS_FILE=tests/e2e/endpoints.e2e.yaml
cat > tests/e2e/endpoints.e2e.yaml <<EOF
services:
  - serviceGroup: "Mock Group"
    service: "Mock Service A"
    baseUrl: "http://127.0.0.1:18000"
    enabled: true
EOF

# 수집 2회 — 멱등성(delete-then-insert) 검증
# batch_time = target_date 다음날 02:00 (target_date = batch_time − 1일, §5.1)
NEXT_DAY=$(date -d "${DATE_ARG} +1 day" +%F)
python3 -m app.main "${NEXT_DAY}T02:00:00+09:00"
python3 -m app.main "${NEXT_DAY}T02:00:00+09:00"

read -r EXP <<<"$(python3 tests/e2e/ci_expectations.py "${DATE_ARG}" "${SEED}" 30 5 \
  "claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5")"
EXP_ROWS=$(sed -E 's/.*rows=([0-9]+).*/\1/' <<<"$EXP")
EXP_INPUT=$(sed -E 's/.*input_sum=([0-9]+).*/\1/' <<<"$EXP")
EXP_REQ=$(sed -E 's/.*requests_sum=([0-9]+).*/\1/' <<<"$EXP")

sed -e "s/{DATE}/${DATE_ARG}/g" -e "s/{SERVICE}/Mock Service A/g" \
    -e "s/{EXP_ROWS}/${EXP_ROWS}/g" -e "s/{EXP_INPUT}/${EXP_INPUT}/g" \
    -e "s/{EXP_REQ}/${EXP_REQ}/g" tests/e2e/verify_expected_results.sql \
  | curl -sf --data-binary @- "http://127.0.0.1:18123/?default_format=TSV" > /tmp/verify_out.tsv
if [ -s /tmp/verify_out.tsv ]; then
  echo "E2E VERIFY FAILED:"; cat /tmp/verify_out.tsv; exit 1
fi

# 시나리오: 서비스명 드리프트 → CHECK WARN + 적재는 진행 (§5.0)
curl -sf -X POST http://127.0.0.1:18000/__mock/scenario \
  -H 'content-type: application/json' -d '{"name_drift": " "}' >/dev/null
OUT=$(python3 -m app.main "${NEXT_DAY}T02:00:00+09:00" 2>&1) || true
grep -q "identity_drift" <<<"$OUT" || { echo "identity drift WARN missing"; exit 1; }
grep -q "BATCH_RESULT status=SUCCESS" <<<"$OUT" || { echo "drift must not fail batch"; exit 1; }

echo "E2E PASS (date=${DATE_ARG}, rows=${EXP_ROWS})"
```

```bash
chmod +x collectors/token-usage/tests/e2e/run_e2e.sh
```

- [ ] **Step 4: CI 워크플로** — `.github/workflows/test-collector.yml`

```yaml
name: test-collector

on:
  push:
    branches: [main]
    paths: ["collectors/token-usage/**", "tools/mock-provider/**",
            ".github/workflows/test-collector.yml"]
  pull_request:
    paths: ["collectors/token-usage/**", "tools/mock-provider/**",
            ".github/workflows/test-collector.yml"]

jobs:
  unit:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: collectors/token-usage
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -r requirements-dev.txt
      - run: python -m pytest tests/ -v --ignore=tests/e2e

  e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -r collectors/token-usage/requirements-dev.txt
      - name: Build mock image
        run: docker build -t token-mock-provider:e2e tools/mock-provider
      - name: Run E2E
        run: ./collectors/token-usage/tests/e2e/run_e2e.sh
```

- [ ] **Step 5: 로컬/CI 검증**

로컬에 docker가 없으므로(§7.2 환경 전제) **이 태스크의 실행 검증은 CI가 담당**: 커밋·푸시 후 PR
체크에서 `unit`·`e2e` 둘 다 green 확인. 로컬에서는 `python -m pytest tests/ --ignore=tests/e2e`
(36 passed) + `bash -n tests/e2e/run_e2e.sh`(문법) + workflow yaml 파싱(`python -c "import yaml,...`)만 수행.

- [ ] **Step 6: Commit**

```bash
git add collectors/token-usage/tests/e2e .github/workflows/test-collector.yml
git commit -m "ci(collectors): E2E with mock-provider and single-node ClickHouse"
```

---

### Task 8: 모듈 README

**Files:**
- Create: `collectors/token-usage/README.md`

- [ ] **Step 1: README 작성** — `collectors/token-usage/README.md`

```markdown
# collectors/token-usage

token-usage-api(v1.1.0)를 구현한 사내 서비스들을 매일 pull하여 ClickHouse에 적재하는
수집기 (스펙 §5). 서비스 목록의 정본은 `endpoints.yaml`(§5.0).

## 실행

    pip install -r requirements-dev.txt
    CH_HOST=... python -m app.main                      # target_date = 어제 (KST)
    python -m app.main 2026-06-16T02:00:00+09:00        # batch_time 명시
    python -m app.main --from 2026-06-10 --to 2026-06-12 --service "Mock Service A"
    # 재수집 = 기본 동작(delete-then-insert), --purge 없음. rerun 후 mart rerun 의무(§8.3)

## 환경변수 (§5.7)

| 변수 | 기본값 | 의미 |
|---|---|---|
| CH_HOST/CH_PORT/CH_USER/CH_PASSWORD | localhost/8123/default/'' | ClickHouse 접속 |
| CH_CLUSTER | '' | 빈 값 = ON CLUSTER 생략 (단일노드/CI) |
| VM_PUSH_URL | '' | 빈 값 = VM push 생략. rerun 경로는 항상 생략 |
| ENDPOINTS_FILE | endpoints.yaml | 서비스 레지스트리 (정본) |
| MAX_PAGES / MAX_BUFFER_ROWS | 200 / 20000 | 페이지 상한(초과=FAILURE) / flush 단위 |
| SOFT_DEADLINE_MINUTES / NOT_READY_BUDGET_MINUTES | 50 / 30 | §5.2 예산 |
| COLLECTOR_HTTPS_PROXY / COLLECTOR_API_VERIFY / COLLECTOR_API_CA_BUNDLE | 상속/true/'' | 아웃바운드 HTTP 방침 |

## 마커 (§5.6)

- 실행당 1줄: `BATCH_RESULT status=... module=token-usage services_ok=... rows=... elapsed=...`
- 서비스별: `SERVICE_RESULT status=SUCCESS|NODATA|SKIPPED|FAILURE service=... rows= pages= warn= rejected=`
- 로그에 user_id 원문·레코드 페이로드 금지 (§5.6 로깅 계약)

## 검증

    python -m pytest tests/ --ignore=tests/e2e     # 단위 (DB/네트워크 불요)
    ./tests/e2e/run_e2e.sh                          # E2E (docker 필요 — CI에서 실행)

## DDL

`ddl/` 참조 (PR #3 초안 — §9-18 협의로 DB명 변경 시 `app/clickhouse_client.py`의
`DB_FACT`/`DB_DIM` 상수만 수정).
```

- [ ] **Step 2: Commit**

```bash
git add collectors/token-usage/README.md
git commit -m "docs(collectors): module README with env and marker contracts"
```

---

## 완료 기준 (Plan 2a)

- [ ] 단위 테스트 36개 통과 (config 4 / api_client 8 / normalize 7 / clickhouse 5 / vm_push 3 / main 9)
- [ ] CI `unit` + `e2e` job green — E2E는 멱등성(2회 실행)·기대값 일치(--expect-empty)·identity drift WARN 케이스 포함
- [ ] 마커·로깅 계약 준수 (user_id 원문 없음 — main 테스트가 검증)
- [ ] Plan 2b(배포: k8s/build/install/rerun --chain-mart)는 §9-18 DDL 협의 확정 후 별도 플랜

## Self-Review 노트

- Task 6 `run_collection`의 정책 수치(NOT_READY 30분·재시작 2회·데드라인 50분·적재 예산 12분)는 §5.2 표와 1:1 — 구현자는 수치를 바꾸지 말 것.
- Task 7 E2E의 DDL 단일노드 변환(sed/regex)은 CI 전용 — 운영 DDL은 PR #3 원본만 사용.
- mock의 `MOCK_USERS=30`은 E2E 속도용 (conformance의 600과 다름 — 목적이 다름).
