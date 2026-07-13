import pytest
from unittest.mock import patch

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
    with patch("app.api_client.time.sleep"):  # Monkeypatch to avoid actual sleep
        with pytest.raises(CollectError) as ei:
            fetch_service(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.RETRYABLE and calls["n"] == 3


def test_5xx_without_retry_after_uses_backoff_schedule(monkeypatch):
    sleeps = []
    monkeypatch.setattr("app.api_client.time.sleep", sleeps.append)
    s = FakeSession([("/v1/usage/summary",
                      FakeResponse(503, {"code": "service_unavailable", "message": "x"}))] * 3)
    with pytest.raises(CollectError):
        fetch_service(ENTRY, DATE, CFG, s)
    assert sleeps == [5, 25]          # 지수 백오프 스케줄 복원 (§5.2)


def test_200_non_json_body_is_permanent_error():
    class BadJsonResponse(FakeResponse):
        def json(self):
            raise ValueError("not json")

    s = FakeSession([("/v1/usage/summary", BadJsonResponse(200, {}))])
    with pytest.raises(CollectError) as ei:
        fetch_service(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR


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
