# Mock Provider Implementation Plan (Plan 1/5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `token-usage-api` 계약(v1.1.0)을 완전 구현한 결정적(deterministic) mock 서비스를 만들어, 스펙 레포의 `conformance_check.py`를 통과시키고 CI에 편입한다 — 이후 수집기(Plan 2)·mart(Plan 3)의 모든 E2E 테스트 기반.

**Architecture:** FastAPI 단일 앱. 합성 데이터는 난수 상태 없이 `sha256(seed|date|user|model|…)` 해시로 생성 — 같은 (seed, date)는 언제나 같은 데이터셋(계약의 "페이지네이션 중 불변" + CI 기대값 고정을 동시에 만족). 오류·일탈 시나리오는 계약 밖 네임스페이스(`/__mock/*`)의 런타임 제어 엔드포인트로 주입한다(시나리오 OFF 상태에서 conformance 통과가 불변식).

**Tech Stack:** Python 3.12, FastAPI, uvicorn, pytest + FastAPI TestClient(httpx), jsonschema+pyyaml(conformance).

## Global Constraints

- 스펙 v1.4(`docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md`) §8.1이 이 모듈의 요구사항 원문.
- 계약 원문: `token-usage-api.yaml` v1.1.0 (원본 레포 `YoonsungNam/token-usage-api-spec` @ `6c32650`) — 본 플랜에서 레포 내로 vendor.
- 결정성: `random` 모듈 사용 금지 — 모든 합성 값은 해시 기반. 같은 `MOCK_SEED`+date → 바이트 단위 동일 응답(시나리오 OFF 시).
- 합성 식별자만 사용: `user-NNNN` / `anon-NNNN` 형식 (실명 유사 문자열 금지 — 스펙 §7.2 환경 데이터 경계).
- 모든 일자/시각은 KST(+09:00). `generatedAt`은 계약 패턴 `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?\+09:00$` 준수.
- Dockerfile은 동료 패턴: `ARG BASE_IMAGE=python:3.12-slim`, requirements 선복사 캐시, `CMD ["uvicorn", ...]`.
- 커밋은 conventional commits: `type(mock-provider): 설명`.
- 에러 응답 본문은 항상 `{"code": ..., "message": ...}`.
- Python 3.12 문법(타입 힌트 `str | None` 등) 사용. 테스트 실행은 모듈 디렉터리에서: `cd tools/mock-provider && python -m pytest`.

## File Structure

```text
tools/mock-provider/
├── app/
│   ├── __init__.py          # (빈 파일)
│   ├── config.py            # 환경변수 → Config (서비스 정체성·규모·시드·보존창)
│   ├── datagen.py           # 결정적 레코드/summary 생성 + API 직렬화 + generatedAt
│   ├── cursors.py           # keyset cursor encode/decode (date/limit 바인딩)
│   ├── scenarios.py         # 런타임 시나리오 상태 (409 전이·429·503·불일치·드리프트)
│   └── main.py              # FastAPI 앱: /v1/usage, /v1/usage/summary, /__mock/*
├── tests/
│   ├── test_config.py
│   ├── test_datagen.py
│   ├── test_cursors.py
│   ├── test_api.py          # 계약 정상 경로 + 오류 시맨틱
│   └── test_scenarios.py
├── contract/                # vendored 계약 (출처 고정)
│   ├── SOURCE.md
│   ├── token-usage-api.yaml
│   └── conformance_check.py
├── conftest.py              # (빈 파일) pytest가 모듈 루트를 sys.path에 등록하게 함
├── requirements.txt         # 런타임 의존성
├── requirements-dev.txt     # 테스트·conformance 의존성
├── run_conformance.sh       # uvicorn 기동 → conformance 실행 → 종료
├── Dockerfile
└── README.md
.github/workflows/test-mock-provider.yml   # pytest + conformance (path filter)
```

---

### Task 1: 스캐폴딩 + Config

**Files:**
- Create: `tools/mock-provider/app/__init__.py`, `tools/mock-provider/app/config.py`
- Create: `tools/mock-provider/conftest.py` (빈 파일 — pytest sys.path 등록용)
- Create: `tools/mock-provider/requirements.txt`, `tools/mock-provider/requirements-dev.txt`
- Test: `tools/mock-provider/tests/test_config.py`

**Interfaces:**
- Produces: `config.load_config() -> Config` — 필드: `service_group: str`, `service: str`, `seed: str`, `users: int`, `anon_users: int`, `models: list[str]`, `retention_days: int`. 이후 모든 태스크가 `Config`를 주입받는다.

- [ ] **Step 1: 의존성 파일 생성**

`tools/mock-provider/requirements.txt`:

```text
fastapi>=0.115,<1
uvicorn[standard]>=0.30,<1
```

`tools/mock-provider/requirements-dev.txt`:

```text
-r requirements.txt
pytest>=8
httpx>=0.27
jsonschema>=4.21
pyyaml>=6
```

설치: `cd tools/mock-provider && pip install -r requirements-dev.txt`

- [ ] **Step 2: 실패하는 테스트 작성** — `tools/mock-provider/tests/test_config.py`

```python
from app.config import load_config


def test_defaults(monkeypatch):
    for k in ("MOCK_SERVICE_GROUP", "MOCK_SERVICE", "MOCK_SEED", "MOCK_USERS",
              "MOCK_ANON_USERS", "MOCK_MODELS", "MOCK_RETENTION_DAYS"):
        monkeypatch.delenv(k, raising=False)
    cfg = load_config()
    assert cfg.service_group == "Mock Group"
    assert cfg.service == "Mock Service A"
    assert cfg.seed == "token-mock-1"
    assert cfg.users == 50 and cfg.anon_users == 10
    assert cfg.models == ["claude-opus-4-8", "claude-sonnet-5", "claude-haiku-4-5"]
    assert cfg.retention_days == 90


def test_env_override(monkeypatch):
    monkeypatch.setenv("MOCK_USERS", "3")
    monkeypatch.setenv("MOCK_MODELS", " m1 , m2 ")
    cfg = load_config()
    assert cfg.users == 3
    assert cfg.models == ["m1", "m2"]
```

- [ ] **Step 3: 실패 확인**

Run: `cd tools/mock-provider && python -m pytest tests/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app'` 또는 `load_config` 미정의

- [ ] **Step 4: 구현** — `tools/mock-provider/app/__init__.py` (빈 파일) + `tools/mock-provider/conftest.py` (빈 파일) + `tools/mock-provider/app/config.py`

```python
import os
from dataclasses import dataclass, field


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    return int(raw) if raw.strip() else default


@dataclass
class Config:
    service_group: str = "Mock Group"
    service: str = "Mock Service A"
    seed: str = "token-mock-1"
    users: int = 50
    anon_users: int = 10
    models: list[str] = field(
        default_factory=lambda: ["claude-opus-4-8", "claude-sonnet-5", "claude-haiku-4-5"]
    )
    retention_days: int = 90


def load_config() -> Config:
    models_raw = os.getenv("MOCK_MODELS", "")
    models = [m.strip() for m in models_raw.split(",") if m.strip()]
    return Config(
        service_group=os.getenv("MOCK_SERVICE_GROUP", "Mock Group"),
        service=os.getenv("MOCK_SERVICE", "Mock Service A"),
        seed=os.getenv("MOCK_SEED", "token-mock-1"),
        users=_int_env("MOCK_USERS", 50),
        anon_users=_int_env("MOCK_ANON_USERS", 10),
        models=models or ["claude-opus-4-8", "claude-sonnet-5", "claude-haiku-4-5"],
        retention_days=_int_env("MOCK_RETENTION_DAYS", 90),
    )
```

- [ ] **Step 5: 통과 확인**

Run: `cd tools/mock-provider && python -m pytest tests/test_config.py -v`
Expected: 2 passed

- [ ] **Step 6: Commit**

```bash
git add tools/mock-provider/app tools/mock-provider/tests tools/mock-provider/conftest.py tools/mock-provider/requirements*.txt
git commit -m "feat(mock-provider): scaffold module with env-driven config"
```

---

### Task 2: 결정적 데이터 생성 (datagen)

**Files:**
- Create: `tools/mock-provider/app/datagen.py`
- Test: `tools/mock-provider/tests/test_datagen.py`

**Interfaces:**
- Consumes: `Config` (Task 1)
- Produces:
  - `UsageRecord` (frozen dataclass): `user_id: str | None`, `user_type: str`, `model: str`, `input_tokens: int`, `cache_read_tokens: int`, `cache_creation_tokens: int`, `output_tokens: int`, `requests: int`
  - `build_records(cfg: Config, date: str) -> list[UsageRecord]` — (seed, date) 결정적, 항상 동일 순서
  - `build_summary(records: list[UsageRecord]) -> dict` — 키: `inputTokens, cacheReadTokens, cacheCreationTokens, outputTokens, requests, distinctUsers, distinctIdentifiedUsers`
  - `to_api_dict(r: UsageRecord) -> dict` — camelCase, 캐시 필드는 0이면 **생략**(수집기의 "생략→0" 정규화 경로 검증용)
  - `generated_at(date: str) -> str` — `"<date+1일>T02:05:00+09:00"`

- [ ] **Step 1: 실패하는 테스트 작성** — `tools/mock-provider/tests/test_datagen.py`

```python
from app.config import Config
from app.datagen import build_records, build_summary, generated_at, to_api_dict

CFG = Config(users=5, anon_users=2, models=["m-a", "m-b"], seed="t")
DATE = "2026-06-15"


def test_deterministic_and_stable_order():
    a, b = build_records(CFG, DATE), build_records(CFG, DATE)
    assert a == b and len(a) > 0


def test_different_date_differs():
    assert build_records(CFG, DATE) != build_records(CFG, "2026-06-16")


def test_user_type_rules():
    recs = build_records(CFG, DATE)
    for r in recs:
        if r.user_type in ("identified", "anonymous"):
            assert isinstance(r.user_id, str) and r.user_id
        else:
            assert r.user_type == "unclassified" and r.user_id is None
    # unclassified에 model='unknown' 행이 정확히 1개 존재 (계약 예시 형태)
    unknown = [r for r in recs if r.user_type == "unclassified" and r.model == "unknown"]
    assert len(unknown) == 1
    # 논리 키 (user_id, user_type, model) 중복 없음 — 계약의 사전 집계 전제
    keys = [(r.user_id, r.user_type, r.model) for r in recs]
    assert len(keys) == len(set(keys))


def test_non_negative_and_summary_matches_detail():
    recs = build_records(CFG, DATE)
    s = build_summary(recs)
    assert s["inputTokens"] == sum(r.input_tokens for r in recs)
    assert s["cacheReadTokens"] == sum(r.cache_read_tokens for r in recs)
    assert s["outputTokens"] == sum(r.output_tokens for r in recs)
    assert s["requests"] == sum(r.requests for r in recs)
    ids = {r.user_id for r in recs if r.user_id is not None}
    assert s["distinctUsers"] == len(ids)
    assert s["distinctIdentifiedUsers"] == len(
        {r.user_id for r in recs if r.user_type == "identified"}
    )
    assert all(
        min(r.input_tokens, r.cache_read_tokens, r.cache_creation_tokens,
            r.output_tokens, r.requests) >= 0
        for r in recs
    )


def test_api_dict_omits_zero_cache_fields():
    recs = build_records(CFG, DATE)
    zero_cache = next(r for r in recs if r.cache_read_tokens == 0)
    d = to_api_dict(zero_cache)
    assert "cacheReadTokens" not in d
    assert d["userId"] == zero_cache.user_id and d["userType"] == zero_cache.user_type
    nonzero = next(r for r in recs if r.cache_read_tokens > 0)
    assert to_api_dict(nonzero)["cacheReadTokens"] == nonzero.cache_read_tokens


def test_generated_at_kst_format():
    assert generated_at("2026-06-15") == "2026-06-16T02:05:00+09:00"
```

- [ ] **Step 2: 실패 확인**

Run: `cd tools/mock-provider && python -m pytest tests/test_datagen.py -v`
Expected: FAIL — `No module named 'app.datagen'`

- [ ] **Step 3: 구현** — `tools/mock-provider/app/datagen.py`

```python
"""결정적 합성 사용량 생성 — 난수 상태 없이 sha256 해시만 사용.

같은 (seed, date)는 항상 같은 데이터셋을 반환한다: 계약의 '페이지네이션 도중
데이터셋 불변'과 CI 기대값 고정이 이 성질 하나로 보장된다.
"""
import hashlib
from dataclasses import dataclass
from datetime import date as date_cls, timedelta

from app.config import Config


@dataclass(frozen=True)
class UsageRecord:
    user_id: str | None
    user_type: str
    model: str
    input_tokens: int
    cache_read_tokens: int
    cache_creation_tokens: int
    output_tokens: int
    requests: int


def _det_int(seed: str, *parts: str, lo: int, hi: int) -> int:
    """[lo, hi] 범위의 결정적 정수 (부분 문자열들의 해시)."""
    key = "|".join((seed,) + parts)
    digest = hashlib.sha256(key.encode()).digest()
    return lo + int.from_bytes(digest[:8], "big") % (hi - lo + 1)


def _record(cfg: Config, date: str, user_id: str | None, user_type: str, model: str) -> UsageRecord:
    k = user_id or "unclassified"
    base = _det_int(cfg.seed, date, k, model, "in", lo=1_000, hi=200_000)
    omit_cache = _det_int(cfg.seed, date, k, model, "omitc", lo=0, hi=2) == 0
    return UsageRecord(
        user_id=user_id,
        user_type=user_type,
        model=model,
        input_tokens=base,
        cache_read_tokens=0 if omit_cache else _det_int(cfg.seed, date, k, model, "cr", lo=0, hi=base),
        cache_creation_tokens=0 if omit_cache else _det_int(cfg.seed, date, k, model, "cc", lo=0, hi=base // 4),
        output_tokens=_det_int(cfg.seed, date, k, model, "out", lo=100, hi=base // 2 + 100),
        requests=_det_int(cfg.seed, date, k, model, "req", lo=1, hi=500),
    )


def build_records(cfg: Config, date: str) -> list[UsageRecord]:
    records: list[UsageRecord] = []
    for i in range(cfg.users):
        uid = f"user-{i:04d}"
        for model in cfg.models:
            # 사용자·모델 조합의 약 1/3은 그날 미사용 — 조합 밀도를 결정적으로 낮춤
            if _det_int(cfg.seed, date, uid, model, "use", lo=0, hi=2) == 0:
                continue
            records.append(_record(cfg, date, uid, "identified", model))
    for i in range(cfg.anon_users):
        uid = f"anon-{i:04d}"
        model = cfg.models[_det_int(cfg.seed, date, uid, "pick", lo=0, hi=len(cfg.models) - 1)]
        records.append(_record(cfg, date, uid, "anonymous", model))
    # unclassified: userId null + 모델 단위 합산 행 (첫 모델 1행 + 'unknown' 1행 — 중복 키 방지)
    unclassified_models = list(dict.fromkeys([cfg.models[0], "unknown"]))
    for model in unclassified_models:
        records.append(_record(cfg, date, None, "unclassified", model))
    return records


def build_summary(records: list[UsageRecord]) -> dict:
    return {
        "inputTokens": sum(r.input_tokens for r in records),
        "cacheReadTokens": sum(r.cache_read_tokens for r in records),
        "cacheCreationTokens": sum(r.cache_creation_tokens for r in records),
        "outputTokens": sum(r.output_tokens for r in records),
        "requests": sum(r.requests for r in records),
        "distinctUsers": len({r.user_id for r in records if r.user_id is not None}),
        "distinctIdentifiedUsers": len(
            {r.user_id for r in records if r.user_type == "identified"}
        ),
    }


def to_api_dict(r: UsageRecord) -> dict:
    d = {
        "userId": r.user_id,
        "userType": r.user_type,
        "model": r.model,
        "inputTokens": r.input_tokens,
        "outputTokens": r.output_tokens,
        "requests": r.requests,
    }
    if r.cache_read_tokens > 0:
        d["cacheReadTokens"] = r.cache_read_tokens
    if r.cache_creation_tokens > 0:
        d["cacheCreationTokens"] = r.cache_creation_tokens
    return d


def generated_at(date: str) -> str:
    next_day = date_cls.fromisoformat(date) + timedelta(days=1)
    return f"{next_day.isoformat()}T02:05:00+09:00"
```

- [ ] **Step 4: 통과 확인**

Run: `cd tools/mock-provider && python -m pytest tests/test_datagen.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add tools/mock-provider/app/datagen.py tools/mock-provider/tests/test_datagen.py
git commit -m "feat(mock-provider): deterministic hash-based usage data generation"
```

---

### Task 3: Keyset cursor (date/limit 바인딩)

**Files:**
- Create: `tools/mock-provider/app/cursors.py`
- Test: `tools/mock-provider/tests/test_cursors.py`

**Interfaces:**
- Produces:
  - `CursorError(ValueError)`
  - `encode_cursor(offset: int, date: str, limit: int) -> str` (urlsafe base64 JSON)
  - `decode_cursor(cursor: str, date: str, limit: int) -> int` — 손상/형식 오류/`date`·`limit` 불일치 시 `CursorError` (계약: "cursor 사용 시 date/limit은 최초 호출과 동일해야 한다")

- [ ] **Step 1: 실패하는 테스트 작성** — `tools/mock-provider/tests/test_cursors.py`

```python
import pytest

from app.cursors import CursorError, decode_cursor, encode_cursor


def test_roundtrip():
    c = encode_cursor(1000, "2026-06-15", 500)
    assert decode_cursor(c, "2026-06-15", 500) == 1000


def test_malformed_cursor_rejected():
    for bad in ("not-base64!!!", "aGVsbG8=", ""):
        with pytest.raises(CursorError):
            decode_cursor(bad, "2026-06-15", 500)


def test_date_or_limit_mismatch_rejected():
    c = encode_cursor(1000, "2026-06-15", 500)
    with pytest.raises(CursorError):
        decode_cursor(c, "2026-06-16", 500)
    with pytest.raises(CursorError):
        decode_cursor(c, "2026-06-15", 1000)


def test_negative_offset_rejected():
    c = encode_cursor(-1, "2026-06-15", 500)
    with pytest.raises(CursorError):
        decode_cursor(c, "2026-06-15", 500)
```

- [ ] **Step 2: 실패 확인**

Run: `cd tools/mock-provider && python -m pytest tests/test_cursors.py -v`
Expected: FAIL — `No module named 'app.cursors'`

- [ ] **Step 3: 구현** — `tools/mock-provider/app/cursors.py`

```python
import base64
import binascii
import json


class CursorError(ValueError):
    pass


def encode_cursor(offset: int, date: str, limit: int) -> str:
    payload = json.dumps({"o": offset, "d": date, "l": limit}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode()).decode()


def decode_cursor(cursor: str, date: str, limit: int) -> int:
    try:
        data = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        offset, c_date, c_limit = data["o"], data["d"], data["l"]
    except (binascii.Error, UnicodeDecodeError, ValueError, KeyError, TypeError) as exc:
        raise CursorError("cursor is malformed; restart pagination without cursor") from exc
    if type(offset) is not int or offset < 0:
        raise CursorError("cursor is malformed; restart pagination without cursor")
    if c_date != date or c_limit != limit:
        raise CursorError("date/limit must match the first call of this pagination")
    return offset
```

- [ ] **Step 4: 통과 확인**

Run: `cd tools/mock-provider && python -m pytest tests/test_cursors.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add tools/mock-provider/app/cursors.py tools/mock-provider/tests/test_cursors.py
git commit -m "feat(mock-provider): keyset cursor with date/limit binding"
```

---

### Task 4: FastAPI 엔드포인트 — 계약 정상 경로

**Files:**
- Create: `tools/mock-provider/app/scenarios.py` (상태 홀더만 — 동작은 Task 6)
- Create: `tools/mock-provider/app/main.py`
- Test: `tools/mock-provider/tests/test_api.py`

**Interfaces:**
- Consumes: Task 1~3 전부
- Produces:
  - `scenarios.ScenarioState` (dataclass): `not_ready_until_uptime_s: float = 0.0`, `retry_after_s: int = 5`, `rate_limit_every: int = 0`, `error_503_every: int = 0`, `summary_extra_pct: int = 0`, `name_drift: str = ""`, `generated_at_change_at_page: int = 0`, `not_ready_at_page: int = 0`, `request_count: int = 0`
  - FastAPI `app.main:app` — `GET /v1/usage`, `GET /v1/usage/summary`, `GET /healthz`
  - 테스트 픽스처 규약: `app.main.CFG`, `app.main.SCN`은 모듈 전역 — 테스트가 교체/리셋 가능

- [ ] **Step 1: 실패하는 테스트 작성** — `tools/mock-provider/tests/test_api.py`

```python
from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient

import app.main as main
from app.config import Config
from app.datagen import build_records
from app.scenarios import ScenarioState


@pytest.fixture()
def client(monkeypatch):
    cfg = Config(users=8, anon_users=2, models=["m-a", "m-b"], seed="api-t")
    monkeypatch.setattr(main, "CFG", cfg)
    monkeypatch.setattr(main, "SCN", ScenarioState())
    return TestClient(main.app)


def yesterday() -> str:
    return (main.now_kst().date() - timedelta(days=1)).isoformat()


def collect_all_pages(client, d, limit):
    rows, cursor, pages = [], None, 0
    while True:
        params = {"date": d, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        resp = client.get("/v1/usage", params=params)
        assert resp.status_code == 200
        body = resp.json()
        assert body["serviceGroup"] == main.CFG.service_group
        assert body["service"] == main.CFG.service
        assert body["date"] == d
        rows.extend(body["records"])
        pages += 1
        cursor = body.get("nextCursor")
        if cursor is None:
            return rows, pages, body


def test_pagination_covers_full_dataset_without_dup(client):
    d = yesterday()
    expected = build_records(main.CFG, d)
    rows, pages, last = collect_all_pages(client, d, limit=7)
    assert len(rows) == len(expected)
    assert pages == -(-len(expected) // 7)  # ceil
    keys = [(r["userId"], r["userType"], r["model"]) for r in rows]
    assert len(keys) == len(set(keys))
    assert last["generatedAt"].endswith("+09:00")


def test_page_size_respects_limit(client):
    d = yesterday()
    resp = client.get("/v1/usage", params={"date": d, "limit": 3})
    body = resp.json()
    assert len(body["records"]) == 3 and "nextCursor" in body


def test_summary_equals_detail_sums(client):
    d = yesterday()
    rows, _, _ = collect_all_pages(client, d, limit=100)
    s = client.get("/v1/usage/summary", params={"date": d}).json()
    assert s["inputTokens"] == sum(r["inputTokens"] for r in rows)
    assert s["outputTokens"] == sum(r["outputTokens"] for r in rows)
    assert s["requests"] == sum(r["requests"] for r in rows)
    assert s["cacheReadTokens"] == sum(r.get("cacheReadTokens", 0) for r in rows)
    ids = {r["userId"] for r in rows if r["userId"] is not None}
    assert s["distinctUsers"] == len(ids)
    assert s["serviceGroup"] == main.CFG.service_group


def test_dataset_immutable_across_pagination(client):
    d = yesterday()
    a, _, _ = collect_all_pages(client, d, limit=5)
    b, _, _ = collect_all_pages(client, d, limit=5)
    assert a == b


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}
```

- [ ] **Step 2: 실패 확인**

Run: `cd tools/mock-provider && python -m pytest tests/test_api.py -v`
Expected: FAIL — `No module named 'app.scenarios'` (이후 `app.main`)

- [ ] **Step 3: 구현 (1/2)** — `tools/mock-provider/app/scenarios.py`

```python
from dataclasses import dataclass


@dataclass
class ScenarioState:
    """계약 밖 일탈 주입 상태 — 전부 기본값(OFF)이면 완전한 계약 준수 동작."""
    not_ready_until_uptime_s: float = 0.0   # 앱 가동 N초 전까지 모든 date 409
    retry_after_s: int = 5                  # 409/429 응답의 Retry-After 값
    rate_limit_every: int = 0               # N번째 요청마다 429 (0=off)
    error_503_every: int = 0                # N번째 요청마다 503 (0=off)
    summary_extra_pct: int = 0              # summary inputTokens를 +N% 왜곡 (§5.1-3-4 검증용)
    name_drift: str = ""                    # 응답 serviceGroup/service 뒤에 붙일 문자열 (§5.0 검증용)
    generated_at_change_at_page: int = 0    # N페이지부터 generatedAt 변경 (§5.3 검증용)
    not_ready_at_page: int = 0              # N페이지에서 409 (§5.2 검증용)
    request_count: int = 0                  # 429/503 주기 판정용 카운터
```

- [ ] **Step 4: 구현 (2/2)** — `tools/mock-provider/app/main.py`

```python
import time
from datetime import date as date_cls, datetime, timedelta, timezone

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from app.config import load_config
from app.cursors import CursorError, decode_cursor, encode_cursor
from app.datagen import build_records, build_summary, generated_at, to_api_dict
from app.scenarios import ScenarioState

KST = timezone(timedelta(hours=9))

app = FastAPI(title="token-usage-api mock provider")
CFG = load_config()
SCN = ScenarioState()
STARTED_AT = time.monotonic()


def now_kst() -> datetime:
    return datetime.now(KST)


def _err(status: int, code: str, message: str, retry_after: int | None = None) -> JSONResponse:
    headers = {"Retry-After": str(retry_after)} if retry_after is not None else None
    return JSONResponse({"code": code, "message": message}, status_code=status, headers=headers)


def _shared_gate() -> JSONResponse | None:
    """요청 공통 게이트: 429/503 주기 시나리오 (OFF면 통과)."""
    SCN.request_count += 1
    n = SCN.request_count
    if SCN.rate_limit_every and n % SCN.rate_limit_every == 0:
        return _err(429, "rate_limited", "too many requests; retry after the indicated delay",
                    retry_after=SCN.retry_after_s)
    if SCN.error_503_every and n % SCN.error_503_every == 0:
        return _err(503, "service_unavailable", "service temporarily unavailable; retry with backoff",
                    retry_after=SCN.retry_after_s)
    return None


def _date_gate(raw_date: str) -> tuple[date_cls | None, JSONResponse | None]:
    """계약의 date 규칙: 당일/미래 400, 보존 초과 404, 미확정 409."""
    try:
        d = date_cls.fromisoformat(raw_date)
    except ValueError:
        return None, _err(400, "invalid_date", "date must be YYYY-MM-DD")
    today = now_kst().date()
    if d >= today:
        return None, _err(400, "invalid_date", "date must be a past day (KST)")
    if d < today - timedelta(days=CFG.retention_days):
        return None, _err(404, "data_not_retained",
                          "usage data for the requested date is past the retention window")
    if time.monotonic() - STARTED_AT < SCN.not_ready_until_uptime_s:
        return None, _err(409, "data_not_ready",
                          "usage for the requested date is not finalized yet; retry later",
                          retry_after=SCN.retry_after_s)
    return d, None


def _identity() -> tuple[str, str]:
    return CFG.service_group + SCN.name_drift, CFG.service + SCN.name_drift


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get("/v1/usage")
def get_usage(date: str = Query(...), cursor: str | None = Query(None),
              limit: int = Query(1000)):
    if (gate := _shared_gate()) is not None:
        return gate
    if not 1 <= limit <= 5000:
        return _err(400, "invalid_limit", "limit must be within 1..5000")
    _, date_err = _date_gate(date)
    if date_err is not None:
        return date_err
    offset = 0
    if cursor is not None:
        try:
            offset = decode_cursor(cursor, date, limit)
        except CursorError as exc:
            return _err(400, "invalid_cursor", str(exc))
    page_no = offset // limit + 1
    if SCN.not_ready_at_page and page_no >= SCN.not_ready_at_page:
        return _err(409, "data_not_ready",
                    "usage for the requested date is not finalized yet; retry later",
                    retry_after=SCN.retry_after_s)
    records = build_records(CFG, date)
    page = records[offset:offset + limit]
    gen = generated_at(date)
    if SCN.generated_at_change_at_page and page_no >= SCN.generated_at_change_at_page:
        gen = gen.replace("T02:05:00", "T02:35:00")
    group, service = _identity()
    body: dict = {
        "serviceGroup": group,
        "service": service,
        "date": date,
        "generatedAt": gen,
        "records": [to_api_dict(r) for r in page],
    }
    if offset + limit < len(records):
        body["nextCursor"] = encode_cursor(offset + limit, date, limit)
    return body


@app.get("/v1/usage/summary")
def get_usage_summary(date: str = Query(...)):
    if (gate := _shared_gate()) is not None:
        return gate
    _, date_err = _date_gate(date)
    if date_err is not None:
        return date_err
    summary = build_summary(build_records(CFG, date))
    if SCN.summary_extra_pct:
        summary["inputTokens"] = summary["inputTokens"] * (100 + SCN.summary_extra_pct) // 100
    group, service = _identity()
    return {"serviceGroup": group, "service": service, "date": date,
            "generatedAt": generated_at(date), **summary}
```

- [ ] **Step 5: 통과 확인**

Run: `cd tools/mock-provider && python -m pytest tests/test_api.py -v`
Expected: 5 passed

- [ ] **Step 6: Commit**

```bash
git add tools/mock-provider/app/main.py tools/mock-provider/app/scenarios.py tools/mock-provider/tests/test_api.py
git commit -m "feat(mock-provider): contract endpoints with pagination and summary"
```

---

### Task 5: 계약 오류 시맨틱 (400/404/409 + 에러 본문)

**Files:**
- Modify: `tools/mock-provider/tests/test_api.py` (테스트 추가 — 구현은 Task 4에 이미 존재, 검증 태스크)

**Interfaces:**
- Consumes: Task 4의 `main.app`, `_date_gate` 동작

- [ ] **Step 1: 오류 시맨틱 테스트 추가** — `tools/mock-provider/tests/test_api.py` 하단에 append

```python
def test_bad_date_and_limit_are_400(client):
    assert client.get("/v1/usage", params={"date": "2026/06/15"}).status_code == 400
    today = main.now_kst().date().isoformat()
    for d in (today, (main.now_kst().date() + timedelta(days=1)).isoformat()):
        r = client.get("/v1/usage", params={"date": d})
        assert r.status_code == 400 and r.json()["code"] == "invalid_date"
    for bad_limit in (0, 5001):
        r = client.get("/v1/usage", params={"date": yesterday(), "limit": bad_limit})
        assert r.status_code == 400 and r.json()["code"] == "invalid_limit"


def test_retention_exceeded_is_404(client):
    old = (main.now_kst().date() - timedelta(days=main.CFG.retention_days + 1)).isoformat()
    for path in ("/v1/usage", "/v1/usage/summary"):
        r = client.get(path, params={"date": old})
        assert r.status_code == 404 and r.json()["code"] == "data_not_retained"


def test_invalid_cursor_is_400(client):
    r = client.get("/v1/usage", params={"date": yesterday(), "cursor": "garbage!!"})
    assert r.status_code == 400 and r.json()["code"] == "invalid_cursor"


def test_cursor_with_changed_limit_is_400(client):
    d = yesterday()
    first = client.get("/v1/usage", params={"date": d, "limit": 3}).json()
    r = client.get("/v1/usage", params={"date": d, "limit": 4, "cursor": first["nextCursor"]})
    assert r.status_code == 400 and r.json()["code"] == "invalid_cursor"


def test_not_ready_409_with_retry_after(client):
    main.SCN.not_ready_until_uptime_s = 10 ** 9  # 사실상 항상 미확정
    for path in ("/v1/usage", "/v1/usage/summary"):
        r = client.get(path, params={"date": yesterday()})
        assert r.status_code == 409 and r.json()["code"] == "data_not_ready"
        assert int(r.headers["Retry-After"]) >= 1
```

- [ ] **Step 2: 통과 확인 (전체 회귀 포함)**

Run: `cd tools/mock-provider && python -m pytest tests/ -v`
Expected: 전부 PASS (기존 17 + 신규 5 = 22). 실패 시 Task 4 구현을 수정하되 테스트를 약화하지 말 것.

- [ ] **Step 3: Commit**

```bash
git add tools/mock-provider/tests/test_api.py
git commit -m "test(mock-provider): contract error semantics (400/404/409, error body)"
```

---

### Task 6: 시나리오 제어 엔드포인트와 일탈 동작

**Files:**
- Modify: `tools/mock-provider/app/main.py` (`/__mock/*` 2개 추가)
- Test: `tools/mock-provider/tests/test_scenarios.py`

**Interfaces:**
- Produces:
  - `POST /__mock/scenario` — body: ScenarioState 필드의 부분 집합(JSON), 알 수 없는 키는 400. 응답: 현재 상태 dict
  - `POST /__mock/reset` — 상태 초기화(request_count 포함). 응답: `{"status": "reset"}`
  - 계약 불변식: **시나리오 전부 OFF일 때 `/__mock` 외 동작은 완전한 계약 준수** (Task 7 conformance가 이를 증명)

- [ ] **Step 1: 실패하는 테스트 작성** — `tools/mock-provider/tests/test_scenarios.py`

```python
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

import app.main as main
from app.config import Config
from app.scenarios import ScenarioState


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(main, "CFG", Config(users=8, anon_users=2, models=["m-a"], seed="scn"))
    monkeypatch.setattr(main, "SCN", ScenarioState())
    return TestClient(main.app)


def yday() -> str:
    return (main.now_kst().date() - timedelta(days=1)).isoformat()


def test_scenario_set_and_reset(client):
    r = client.post("/__mock/scenario", json={"rate_limit_every": 2})
    assert r.status_code == 200 and r.json()["rate_limit_every"] == 2
    assert client.post("/__mock/scenario", json={"nope": 1}).status_code == 400
    client.post("/__mock/reset")
    assert main.SCN.rate_limit_every == 0


def test_rate_limit_every_2nd_request(client):
    client.post("/__mock/scenario", json={"rate_limit_every": 2})
    codes = [client.get("/v1/usage", params={"date": yday()}).status_code for _ in range(4)]
    assert codes == [200, 429, 200, 429]


def test_503_injection(client):
    client.post("/__mock/scenario", json={"error_503_every": 2})
    codes = [client.get("/v1/usage", params={"date": yday()}).status_code for _ in range(2)]
    assert codes == [200, 503]


def test_summary_mismatch_scenario(client):
    base = client.get("/v1/usage/summary", params={"date": yday()}).json()
    client.post("/__mock/scenario", json={"summary_extra_pct": 10})
    skewed = client.get("/v1/usage/summary", params={"date": yday()}).json()
    assert skewed["inputTokens"] == base["inputTokens"] * 110 // 100
    assert skewed["outputTokens"] == base["outputTokens"]


def test_name_drift_scenario(client):
    client.post("/__mock/scenario", json={"name_drift": " "})
    body = client.get("/v1/usage", params={"date": yday()}).json()
    assert body["service"] == main.CFG.service + " "


def test_generated_at_changes_at_page(client):
    client.post("/__mock/scenario", json={"generated_at_change_at_page": 2})
    d = yday()
    p1 = client.get("/v1/usage", params={"date": d, "limit": 3}).json()
    p2 = client.get("/v1/usage", params={"date": d, "limit": 3, "cursor": p1["nextCursor"]}).json()
    assert p1["generatedAt"] != p2["generatedAt"]


def test_409_at_page(client):
    client.post("/__mock/scenario", json={"not_ready_at_page": 2})
    d = yday()
    p1 = client.get("/v1/usage", params={"date": d, "limit": 3})
    assert p1.status_code == 200
    p2 = client.get("/v1/usage", params={"date": d, "limit": 3,
                                         "cursor": p1.json()["nextCursor"]})
    assert p2.status_code == 409
```

- [ ] **Step 2: 실패 확인**

Run: `cd tools/mock-provider && python -m pytest tests/test_scenarios.py -v`
Expected: FAIL — `/__mock/scenario` 404

- [ ] **Step 3: 구현** — `tools/mock-provider/app/main.py`에 추가 (파일 하단)

```python
from dataclasses import fields as dc_fields  # 파일 상단 import 절에 추가


@app.post("/__mock/scenario")
def set_scenario(payload: dict):
    allowed = {f.name for f in dc_fields(ScenarioState)}
    unknown = set(payload) - allowed
    if unknown:
        return _err(400, "invalid_scenario", f"unknown scenario fields: {sorted(unknown)}")
    for key, value in payload.items():
        setattr(SCN, key, value)
    return {f.name: getattr(SCN, f.name) for f in dc_fields(ScenarioState)}


@app.post("/__mock/reset")
def reset_scenario():
    global SCN
    SCN = ScenarioState()
    return {"status": "reset"}
```

주의: `reset`이 `SCN`을 재할당하므로 `_shared_gate`/`_date_gate`/핸들러의 `SCN` 참조는 항상
모듈 전역을 읽어야 한다(현 구현이 이미 그렇게 되어 있음 — 지역 변수로 캡처하지 말 것).

- [ ] **Step 4: 통과 확인 (전체 회귀)**

Run: `cd tools/mock-provider && python -m pytest tests/ -v`
Expected: 전부 PASS

- [ ] **Step 5: Commit**

```bash
git add tools/mock-provider/app/main.py tools/mock-provider/tests/test_scenarios.py
git commit -m "feat(mock-provider): runtime scenario injection via /__mock endpoints"
```

---

### Task 7: 계약 vendor + conformance 통과

**Files:**
- Create: `tools/mock-provider/contract/SOURCE.md`
- Create: `tools/mock-provider/contract/token-usage-api.yaml` (복사)
- Create: `tools/mock-provider/contract/conformance_check.py` (복사)
- Create: `tools/mock-provider/run_conformance.sh`

**Interfaces:**
- Consumes: 로컬 클론 `/home/mini/github/token-usage-api-spec` (@ `6c32650`)
- Produces: `./run_conformance.sh` — exit 0 = 계약 준수. Plan 2(수집기)의 CI가 재사용.

- [ ] **Step 1: 계약 파일 vendor**

```bash
mkdir -p tools/mock-provider/contract
cp /home/mini/github/token-usage-api-spec/token-usage-api.yaml tools/mock-provider/contract/
cp /home/mini/github/token-usage-api-spec/tests/conformance_check.py tools/mock-provider/contract/
```

`tools/mock-provider/contract/SOURCE.md`:

```markdown
# Vendored contract files

- 출처: https://github.com/YoonsungNam/token-usage-api-spec @ commit `6c32650` (2026-06-17)
- 파일: `token-usage-api.yaml` (공유용 최종 스펙 v1.1.0), `tests/conformance_check.py`
- 이유: CI 자립성 — 사설 레포 접근 토큰 없이 conformance를 실행하기 위해 고정 복사.
- 갱신 절차: 원본 레포 갱신 시 이 디렉터리를 다시 복사하고 본 파일의 커밋 해시를 갱신한다.
  (원본과의 드리프트는 이 해시로 추적)
```

- [ ] **Step 2: 실행 스크립트 작성** — `tools/mock-provider/run_conformance.sh`

```bash
#!/usr/bin/env bash
# mock-provider를 기동하고 vendored conformance_check로 계약 준수를 검증한다.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PORT="${PORT:-8000}"
DATE_ARG="${1:-$(date -d "yesterday" +%F)}"

python -m uvicorn app.main:app --host 127.0.0.1 --port "${PORT}" &
UVICORN_PID=$!
trap 'kill "${UVICORN_PID}" 2>/dev/null || true' EXIT

for _ in $(seq 1 50); do
  if curl -sf "http://127.0.0.1:${PORT}/healthz" > /dev/null; then break; fi
  sleep 0.2
done

python contract/conformance_check.py --base-url "http://127.0.0.1:${PORT}" --date "${DATE_ARG}"
echo "CONFORMANCE PASS (date=${DATE_ARG})"
```

```bash
chmod +x tools/mock-provider/run_conformance.sh
```

- [ ] **Step 3: 실행 및 통과 확인**

Run: `cd tools/mock-provider && ./run_conformance.sh`
Expected: conformance_check의 전 항목 통과 후 마지막 줄 `CONFORMANCE PASS (date=...)`, exit 0.
실패 시: **mock 구현을 계약에 맞게 수정**한다 (conformance_check·yaml은 수정 금지 — 계약이 정본).
conformance_check.py의 옵션·검증 항목은 `python contract/conformance_check.py --help`와 파일
상단 docstring으로 확인 (스키마 적합성 + summary=detail 합 + userType↔userId + generatedAt KST +
페이지네이션 종료 + 미래날짜 400 + 잘못된 cursor 400 검사).

- [ ] **Step 4: Commit**

```bash
git add tools/mock-provider/contract tools/mock-provider/run_conformance.sh
git commit -m "feat(mock-provider): vendor contract @6c32650 and pass conformance"
```

---

### Task 8: Dockerfile + 스모크 테스트

**Files:**
- Create: `tools/mock-provider/Dockerfile`, `tools/mock-provider/.dockerignore`

**Interfaces:**
- Produces: 이미지 `token-mock-provider` — 8000 포트, 환경변수(Task 1의 `MOCK_*` + 시나리오는 런타임 `/__mock`)로 구성. Plan 2의 CI E2E와 Plan 5의 stage 배포가 이 이미지를 사용.

- [ ] **Step 1: Dockerfile 작성** — 동료 패턴 준수 (스펙 §7.2)

`tools/mock-provider/Dockerfile`:

```dockerfile
ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

`tools/mock-provider/.dockerignore`:

```text
tests/
contract/
__pycache__/
*.pyc
```

- [ ] **Step 2: 빌드 및 스모크 테스트**

```bash
cd tools/mock-provider
docker build -t token-mock-provider:dev .
docker run -d --rm --name mock-smoke -p 18000:8000 -e MOCK_SERVICE="Smoke Svc" token-mock-provider:dev
sleep 2
curl -sf http://127.0.0.1:18000/healthz
curl -sf "http://127.0.0.1:18000/v1/usage/summary?date=$(date -d yesterday +%F)" | head -c 200
docker stop mock-smoke
```

Expected: `{"status":"ok"}` + summary JSON에 `"service": "Smoke Svc"` 포함, 컨테이너 정상 종료.

- [ ] **Step 3: Commit**

```bash
git add tools/mock-provider/Dockerfile tools/mock-provider/.dockerignore
git commit -m "feat(mock-provider): containerize with colleague Dockerfile pattern"
```

---

### Task 9: CI 워크플로 + 모듈 README

**Files:**
- Create: `.github/workflows/test-mock-provider.yml`
- Create: `tools/mock-provider/README.md`

**Interfaces:**
- Consumes: Task 1~8 전부 (pytest 스위트, run_conformance.sh)
- Produces: `tools/mock-provider/**` 변경 시 자동 검증 게이트 — Plan 2가 이 워크플로 패턴(path filter + 단계 구성)을 복제.

- [ ] **Step 1: CI 워크플로 작성** — `.github/workflows/test-mock-provider.yml`

```yaml
name: test-mock-provider

on:
  push:
    branches: [main]
    paths: ["tools/mock-provider/**", ".github/workflows/test-mock-provider.yml"]
  pull_request:
    paths: ["tools/mock-provider/**", ".github/workflows/test-mock-provider.yml"]

jobs:
  test:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: tools/mock-provider
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install deps
        run: pip install -r requirements-dev.txt
      - name: Unit tests
        run: python -m pytest tests/ -v
      - name: Conformance (계약 준수 — 시나리오 OFF 불변식)
        run: ./run_conformance.sh
```

- [ ] **Step 2: README 작성** — `tools/mock-provider/README.md`

```markdown
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
```

- [ ] **Step 3: 로컬 최종 회귀**

Run: `cd tools/mock-provider && python -m pytest tests/ -v && ./run_conformance.sh`
Expected: 전부 PASS + `CONFORMANCE PASS`

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/test-mock-provider.yml tools/mock-provider/README.md
git commit -m "ci(mock-provider): pytest + conformance workflow, module README"
```

---

## 완료 기준 (Plan 1)

- [ ] `python -m pytest tests/` 전부 통과 (config/datagen/cursors/api/scenarios)
- [ ] `./run_conformance.sh` exit 0 — 시나리오 OFF 상태에서 계약 완전 준수
- [ ] `docker build` + 스모크 통과
- [ ] CI 워크플로가 push/PR에서 두 검증을 자동 실행
- [ ] 스펙 §8.1의 시나리오 옵션 7종(409 전이·429·503·summary 불일치·서비스명 드리프트·generatedAt 변경·페이지 중 409) 전부 주입 가능

다음: Plan 2 (collectors/token-usage) — 이 mock을 CI E2E의 소스로 사용.
