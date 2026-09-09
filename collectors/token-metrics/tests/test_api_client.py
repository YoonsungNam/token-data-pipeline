"""api_client 테스트 — FakeSession 스크립트 패턴(기존 모듈 test_api_client.py 관용구 복제).

모든 테스트는 `time.sleep`을 패치한다(autouse fixture `sl`) — 재시도 대기 스케줄은 호출 인자로 검증.
공통 fixture 상수는 Plan 6b 전 태스크 공통.
"""
import json
from datetime import date
from unittest.mock import call, patch

import pytest
import requests

from app.api_client import (BACKOFF_S, HTTP_TIMEOUT_S, METRICS_PATH, RETRY_AFTER_CAP_S,
                            RETRYABLE_ATTEMPTS, fetch_metrics)
from app.config import Config, ServiceEntry
from app.events import CollectError, Event
from app.normalize import SOURCE_API, MetricsPayload

SERVICE_GROUP = "Mock Group"
SERVICE = "Mock Service A"
DATE = "2026-09-10"
GPU_TYPE = "H100"
ENGINE = {"type": "vllm", "version": "0.10.1"}
GENERATED_AT = "2026-09-11T02:05:00+09:00"

ENTRY = ServiceEntry(SERVICE_GROUP, SERVICE, "http://svc", True,
                     date(2026, 9, 9), date(2026, 8, 26), None)
CFG = Config()
URL = "http://svc/v1/metrics"

REPORT = {
    "date": DATE, "serviceGroup": SERVICE_GROUP, "service": SERVICE,
    "generatedAt": GENERATED_AT, "engine": ENGINE,
    "gpu": [{"model": "claude-opus-4-8", "gpuType": GPU_TYPE, "category": "serving",
             "gpuCount": 8, "gpuHours": 192}],
    "serving": [{"model": "claude-opus-4-8",
                 "ttftMs": {"p50": 100, "p90": 200, "p95": 250, "p99": 400},
                 "outputTps": {"p50": 50}}],
}


class FakeResponse:
    """requests.Response 대역 — status_code / headers / content(bytes) / json()."""

    def __init__(self, status_code, body=None, headers=None, content=None):
        self.status_code = status_code
        self._body = {} if body is None else body          # `[]`도 그대로 보존(not_object 검증용)
        self.headers = headers or {}
        self.content = json.dumps(self._body).encode() if content is None else content

    def json(self):
        return self._body


class BadJsonResponse(FakeResponse):
    def json(self):
        raise ValueError("not json")


class FakeSession:
    """스크립트된 응답 시퀀스를 돌려주는 requests.Session 대역 — 호출 순서·params·timeout 기록."""

    def __init__(self, script):
        self.script = list(script)   # (url_substr, response | callable) — 순서 검증
        self.calls = []
        self.timeouts = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        self.timeouts.append(timeout)
        assert self.script, f"unexpected extra call {url}"
        expect_substr, resp = self.script.pop(0)
        assert expect_substr in url, f"unexpected call {url}, expected {expect_substr}"
        return resp() if callable(resp) else resp


def _raise_conn_error():
    raise requests.ConnectionError("boom")


@pytest.fixture(autouse=True)
def sl():
    """time.sleep 대역 — 실제 대기 없이 호출 인자만 기록."""
    with patch("app.api_client.time.sleep") as m:
        yield m


# ---------- 상수 (§5.2 "재시도 3회(5/25/125s, 캡 300s)") ----------

def test_constants_match_design():
    assert RETRY_AFTER_CAP_S == 300
    assert RETRYABLE_ATTEMPTS == 3
    assert BACKOFF_S == (5, 25, 125)
    assert HTTP_TIMEOUT_S == 60
    assert METRICS_PATH == "/v1/metrics"


# ---------- 번역표: 409 / 404 / 400 / 그 외 4xx ----------

def test_409_not_ready_capped_retry_after(sl):
    s = FakeSession([(METRICS_PATH, FakeResponse(409, {"code": "data_not_ready", "message": "x"},
                                                 headers={"Retry-After": "900"}))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.NOT_READY
    assert ei.value.retry_after_s == 300           # min(Retry-After, 300) 캡 (§5.2)
    assert "data_not_ready" in ei.value.message
    assert sl.call_count == 0                       # NOT_READY는 즉시 던진다 — 재방문은 main 큐 담당
    assert len(s.calls) == 1


def test_409_without_retry_after_defaults_to_5():
    s = FakeSession([(METRICS_PATH, FakeResponse(409, {"code": "data_not_ready"}))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.NOT_READY and ei.value.retry_after_s == 5


def test_404_retention():
    s = FakeSession([(METRICS_PATH, FakeResponse(404, {"code": "data_not_retained", "message": "x"}))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.RETENTION
    assert ei.value.retry_after_s == 0


def test_400_permanent():
    s = FakeSession([(METRICS_PATH, FakeResponse(400, {"code": "invalid_date", "message": "x"}))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "http 400 (invalid_date)"


def test_418_permanent_no_retry(sl):
    # 429·5xx 외의 비-200은 전부 PERMANENT_ERROR — 재시도·대기 없음
    s = FakeSession([(METRICS_PATH, FakeResponse(418, {"code": "teapot"}))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert sl.call_count == 0 and len(s.calls) == 1


# ---------- RETRYABLE: 429 / 5xx / 네트워크 — 이 계층에서 3회 소진 ----------

def test_429_then_200_retries_with_retry_after(sl):
    s = FakeSession([
        (METRICS_PATH, FakeResponse(429, {"code": "rate_limited"}, headers={"Retry-After": "7"})),
        (METRICS_PATH, FakeResponse(200, REPORT)),
    ])
    payload = fetch_metrics(ENTRY, DATE, CFG, s)
    assert isinstance(payload, MetricsPayload)
    assert sl.call_args_list == [call(7)]          # Retry-After 우선, 백오프 대신
    assert len(s.calls) == 2


def test_429_retry_after_capped_at_300(sl):
    s = FakeSession([
        (METRICS_PATH, FakeResponse(429, {"code": "rate_limited"}, headers={"Retry-After": "9999"})),
        (METRICS_PATH, FakeResponse(200, REPORT)),
    ])
    fetch_metrics(ENTRY, DATE, CFG, s)
    assert sl.call_args_list == [call(RETRY_AFTER_CAP_S)]


def test_503_three_times_exhausts(sl):
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        return FakeResponse(503, {"code": "service_unavailable", "message": "x"})

    s = FakeSession([(METRICS_PATH, flaky)] * 3)
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.RETRYABLE
    assert ei.value.message == "http 503 (service_unavailable)"
    assert calls["n"] == RETRYABLE_ATTEMPTS
    assert sl.call_args_list == [call(5), call(25)]   # 마지막 시도 뒤에는 대기 없음 (§5.2 5/25/125)


def test_5xx_retry_after_zero_falls_back_to_backoff(sl):
    # Retry-After: 0 은 "대기값 없음"과 같다 — 백오프 스케줄 사용
    s = FakeSession([(METRICS_PATH, FakeResponse(502, {"code": "bad_gateway"},
                                                 headers={"Retry-After": "0"}))] * 3)
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.RETRYABLE
    assert sl.call_args_list == [call(5), call(25)]


def test_network_error_retryable(sl):
    s = FakeSession([(METRICS_PATH, _raise_conn_error)] * 3)
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.RETRYABLE
    assert ei.value.message == "network: ConnectionError"
    assert len(s.calls) == 3 and sl.call_args_list == [call(5), call(25)]


def test_network_error_then_200_recovers(sl):
    s = FakeSession([(METRICS_PATH, _raise_conn_error), (METRICS_PATH, FakeResponse(200, REPORT))])
    payload = fetch_metrics(ENTRY, DATE, CFG, s)
    assert payload.reported_service == SERVICE and sl.call_args_list == [call(5)]


# ---------- 200 본문 가드: MAX_RESPONSE_BYTES / non-JSON (§5.2 PERMANENT_ERROR 행) ----------

def test_body_over_max_bytes_permanent(sl):
    small_cfg = Config(max_response_bytes=10)
    s = FakeSession([(METRICS_PATH, FakeResponse(200, REPORT, content=b"x" * 11))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, small_cfg, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "body too large: 11 > 10"
    assert sl.call_count == 0                       # 재시도 대상 아님


def test_body_at_max_bytes_is_accepted():
    exact = json.dumps(REPORT).encode()
    s = FakeSession([(METRICS_PATH, FakeResponse(200, REPORT, content=exact))])
    payload = fetch_metrics(ENTRY, DATE, Config(max_response_bytes=len(exact)), s)
    assert payload.reported_service == SERVICE      # 경계값(== max)은 통과 — 초과(>)만 거부


class ContentLengthOnlyResponse:
    """C: Content-Length 선검사 테스트 전용 — `.content` 접근 시 AssertionError(선검사가 막았어야 한다)."""

    def __init__(self, status_code, headers):
        self.status_code = status_code
        self.headers = headers

    @property
    def content(self):
        raise AssertionError(".content 접근됨 — Content-Length 선검사가 막았어야 한다")

    def json(self):
        raise AssertionError("json() 접근됨 — Content-Length 선검사가 막았어야 한다")


def test_content_length_precheck_rejects_without_touching_body(sl):
    small_cfg = Config(max_response_bytes=10)
    s = FakeSession([(METRICS_PATH, ContentLengthOnlyResponse(200, headers={"Content-Length": "11"}))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, small_cfg, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "body too large: 11 > 10"          # 사후 검사와 같은 사유 문구
    assert sl.call_count == 0


def test_content_length_non_numeric_falls_through_to_post_hoc_check():
    s = FakeSession([(METRICS_PATH, FakeResponse(200, REPORT, headers={"Content-Length": "abc"}))])
    payload = fetch_metrics(ENTRY, DATE, CFG, s)
    assert payload.reported_service == SERVICE                    # 비숫자 헤더는 무시 — 본문 검사로 통과


def test_malformed_json_permanent(sl):
    s = FakeSession([(METRICS_PATH, BadJsonResponse(200, {}))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "malformed json body (http 200)"
    assert sl.call_count == 0


# ---------- 구조 위반 → PERMANENT_ERROR "report structure: <코드>" (§5.3-1 응답 단위) ----------

def test_date_echo_mismatch_permanent():
    s = FakeSession([(METRICS_PATH, FakeResponse(200, dict(REPORT, date="2026-09-09")))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "report structure: date_mismatch"


def test_missing_required_key_permanent():
    body = {k: v for k, v in REPORT.items() if k != "gpu"}
    s = FakeSession([(METRICS_PATH, FakeResponse(200, body))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "report structure: missing_keys:gpu"


def test_non_array_permanent():
    s = FakeSession([(METRICS_PATH, FakeResponse(200, dict(REPORT, serving={})))])
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "report structure: serving_not_array"


def test_not_object_permanent():
    s = FakeSession([(METRICS_PATH, FakeResponse(200, []))])   # JSON 배열 최상위
    with pytest.raises(CollectError) as ei:
        fetch_metrics(ENTRY, DATE, CFG, s)
    assert ei.value.event is Event.PERMANENT_ERROR
    assert ei.value.message == "report structure: not_object"


# ---------- 정상 경로·단건 호출 계약 ----------

def test_happy_path_single_get(sl):
    s = FakeSession([(METRICS_PATH, FakeResponse(200, REPORT))])
    payload = fetch_metrics(ENTRY, DATE, CFG, s)
    assert isinstance(payload, MetricsPayload)
    assert payload.source_type == SOURCE_API == "metrics-api-v1"
    assert payload.date == DATE
    assert payload.reported_service_group == SERVICE_GROUP
    assert payload.reported_service == SERVICE
    assert payload.generated_at_raw == GENERATED_AT
    assert payload.engine == ENGINE
    assert len(payload.gpu) == 1 and len(payload.serving) == 1
    assert payload.extra_top_keys == []
    assert s.calls == [(URL, {"date": DATE})]       # 호출 1회, params는 date만 (limit·cursor 없음)
    assert s.timeouts == [HTTP_TIMEOUT_S]
    assert sl.call_count == 0


def test_no_summary_or_pagination_calls():
    s = FakeSession([(METRICS_PATH, FakeResponse(200, REPORT))])
    fetch_metrics(ENTRY, DATE, CFG, s)
    assert s.script == [] and len(s.calls) == 1     # summary·다음 페이지 호출 없음
    assert all("/v1/usage" not in url for url, _ in s.calls)


def test_base_url_without_trailing_slash_joins_path():
    entry = ServiceEntry(SERVICE_GROUP, SERVICE, "http://svc:8000/root", True,
                         date(2026, 9, 9), date(2026, 8, 26), None)
    s = FakeSession([("http://svc:8000/root/v1/metrics", FakeResponse(200, REPORT))])
    fetch_metrics(entry, DATE, CFG, s)
    assert s.calls[0][0] == "http://svc:8000/root/v1/metrics"
