"""GET /v1/metrics (token-metric-api @6a552d2) — config·시나리오·datagen·엔드포인트 테스트.

기존 tests/test_api.py의 fixture 패턴(monkeypatch CFG/SCN + TestClient)을 그대로 복제한다.
"""
import json
from dataclasses import fields as dc_fields
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

import app.main as main
from app.config import Config, load_config
from app.datagen import (METRICS_ENGINE, build_metrics, build_records, build_summary,
                         generated_at)
from app.scenarios import ScenarioState

METRICS_FLAGS = ("metrics_gpu_hours_over", "metrics_unknown_serving", "metrics_pct_non_monotone",
                 "metrics_dup_gpu_rows", "metrics_empty_gpu", "metrics_engine_null")
DATE = "2026-09-10"
CFG3 = Config(users=8, anon_users=2, seed="metrics-t")   # models 기본 3종 → gpu 5행 / serving 3행


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(main, "CFG", Config(users=8, anon_users=2, seed="metrics-api-t"))
    monkeypatch.setattr(main, "SCN", ScenarioState())
    return TestClient(main.app)


def yday() -> str:
    return (main.now_kst().date() - timedelta(days=1)).isoformat()


def days_ago(n: int) -> str:
    return (main.now_kst().date() - timedelta(days=n)).isoformat()


# ---------------------------------------------------------------- Step 1: config
def test_metrics_retention_default_14(monkeypatch):
    for k in ("MOCK_RETENTION_DAYS", "MOCK_METRICS_RETENTION_DAYS"):
        monkeypatch.delenv(k, raising=False)
    cfg = load_config()
    assert cfg.metrics_retention_days == 14
    assert cfg.retention_days == 90            # 기존 usage 보존 기본값 불변


def test_metrics_retention_rejects_zero(monkeypatch):
    monkeypatch.setenv("MOCK_METRICS_RETENTION_DAYS", "0")
    with pytest.raises(ValueError, match="MOCK_METRICS_RETENTION_DAYS must be >= 1"):
        load_config()
    monkeypatch.setenv("MOCK_METRICS_RETENTION_DAYS", "30")
    assert load_config().metrics_retention_days == 30


# ---------------------------------------------------------------- Step 2: 시나리오 플래그 6종
def test_scenario_metrics_flags_reject_bool(client):
    r = client.post("/__mock/scenario", json={"metrics_empty_gpu": True})
    assert r.status_code == 400
    assert r.json() == {"code": "invalid_scenario", "message": "metrics_empty_gpu must be int"}


def test_scenario_metrics_flags_reject_negative(client):
    r = client.post("/__mock/scenario", json={"metrics_dup_gpu_rows": -1})
    assert r.status_code == 400
    assert r.json() == {"code": "invalid_scenario", "message": "metrics_dup_gpu_rows must be >= 0"}


def test_scenario_reset_clears_metrics_flags(client):
    r = client.post("/__mock/scenario", json={f: 1 for f in METRICS_FLAGS})
    assert r.status_code == 200
    assert all(r.json()[f] == 1 for f in METRICS_FLAGS)
    assert all(getattr(main.SCN, f) == 1 for f in METRICS_FLAGS)
    client.post("/__mock/reset")
    assert all(getattr(main.SCN, f) == 0 for f in METRICS_FLAGS)
    # 기존 9필드 순서 불변, 신규 6필드는 dataclass 끝에 append
    names = [f.name for f in dc_fields(ScenarioState)]
    assert names[:9] == ["not_ready_until_uptime_s", "retry_after_s", "rate_limit_every",
                         "error_503_every", "summary_extra_pct", "name_drift",
                         "generated_at_change_at_page", "not_ready_at_page", "request_count"]
    assert names[9:] == list(METRICS_FLAGS)


# ---------------------------------------------------------------- Step 3: datagen.build_metrics
def test_build_metrics_deterministic():
    a, b = build_metrics(CFG3, DATE), build_metrics(CFG3, DATE)
    assert a == b
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
    assert build_metrics(CFG3, "2026-09-11") != a                      # 날짜가 다르면 데이터가 다름
    assert build_metrics(CFG3, DATE, ScenarioState()) == a              # 전 플래그 OFF == scn 생략
    assert a["engine"] == METRICS_ENGINE and a["engine"] is not METRICS_ENGINE   # 복사본(호출자 mutate 안전)


def test_build_metrics_shape():
    p = build_metrics(CFG3, DATE)
    assert set(p) == {"date", "serviceGroup", "service", "generatedAt", "engine", "gpu", "serving"}
    assert list(p) == ["date", "serviceGroup", "service", "generatedAt", "engine", "gpu", "serving"]
    assert p["date"] == DATE and p["generatedAt"] == generated_at(DATE) == "2026-09-11T02:05:00+09:00"
    assert p["serviceGroup"] == CFG3.service_group and p["service"] == CFG3.service
    assert p["engine"] == {"type": "vllm", "version": "0.10.1"}
    gpu, serving = p["gpu"], p["serving"]
    assert len(gpu) == 5 and len(serving) == 3
    assert {r["category"] for r in gpu} == {"serving", "standby", "test"}
    assert all(set(r) == {"model", "gpuType", "category", "gpuCount", "gpuHours"} for r in gpu)
    assert all(r["gpuType"] == "H100" for r in gpu)
    assert [r["model"] for r in gpu if r["category"] == "serving"] == CFG3.models
    assert [r["model"] for r in gpu if r["category"] == "standby"] == [CFG3.models[0]]
    assert [r["category"] for r in gpu if r["model"] == "unknown"] == ["test"]
    assert all(isinstance(r["gpuCount"], int) and r["gpuCount"] >= 1 for r in gpu)
    assert all(isinstance(r["gpuHours"], float) and 0 < r["gpuHours"] <= r["gpuCount"] * 24 for r in gpu)
    keys = [(r["model"], r["gpuType"], r["category"]) for r in gpu]
    assert len(keys) == len(set(keys))                                  # 기본 응답은 중복 행 없음
    assert [r["model"] for r in serving] == CFG3.models
    for r in serving:
        assert set(r) == {"model", "ttftMs", "itlMs", "outputTps"}
        assert set(r["outputTps"]) == {"p50"} and isinstance(r["outputTps"]["p50"], float)
        for block in ("ttftMs", "itlMs"):
            pc = r[block]
            assert list(pc) == ["p50", "p90", "p95", "p99"]
            assert all(isinstance(pc[k], float) and pc[k] >= 0 for k in pc)
            assert pc["p50"] <= pc["p90"] <= pc["p95"] <= pc["p99"]


def test_build_metrics_scenarios():
    base = build_metrics(CFG3, DATE)

    def with_flag(name: str) -> dict:
        scn = ScenarioState()
        setattr(scn, name, 1)
        return build_metrics(CFG3, DATE, scn)

    dup = with_flag("metrics_dup_gpu_rows")
    assert len(dup["gpu"]) == 6 and dup["gpu"][0] == dup["gpu"][1] == base["gpu"][0]
    assert dup["gpu"][2:] == base["gpu"][1:]
    over = with_flag("metrics_gpu_hours_over")
    assert over["gpu"][0]["gpuHours"] == over["gpu"][0]["gpuCount"] * 24 + 10
    assert over["gpu"][1:] == base["gpu"][1:]
    unk = with_flag("metrics_unknown_serving")
    assert len(unk["gpu"]) == 6
    assert unk["gpu"][-1] == {"model": "unknown", "gpuType": "H100", "category": "serving",
                              "gpuCount": 1, "gpuHours": 24.0}
    mono = with_flag("metrics_pct_non_monotone")
    assert mono["serving"][0]["ttftMs"]["p90"] == mono["serving"][0]["ttftMs"]["p50"] - 1
    assert mono["serving"][0]["itlMs"] == base["serving"][0]["itlMs"]
    assert mono["serving"][1:] == base["serving"][1:]
    empty = with_flag("metrics_empty_gpu")
    assert empty["gpu"] == [] and empty["serving"] == base["serving"]
    null_engine = with_flag("metrics_engine_null")
    assert null_engine["engine"] is None and null_engine["gpu"] == base["gpu"]
    assert base == build_metrics(CFG3, DATE)                            # 시나리오가 기본 결과를 오염시키지 않음


# ---------------------------------------------------------------- Step 4: GET /v1/metrics
def test_metrics_ok_shape(client):
    d = yday()
    resp = client.get("/v1/metrics", params={"date": d})
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")
    body = resp.json()
    assert list(body) == ["date", "serviceGroup", "service", "generatedAt", "engine", "gpu", "serving"]
    assert body["date"] == d
    assert body["serviceGroup"] == "Mock Group" and body["service"] == "Mock Service A"
    assert body["generatedAt"] == f"{main.now_kst().date().isoformat()}T02:05:00+09:00"
    assert body["engine"] == METRICS_ENGINE
    assert len(body["gpu"]) == 5 and len(body["serving"]) == 3
    assert body == build_metrics(main.CFG, d)                           # 시나리오 OFF == 순수 생성기 출력


def test_metrics_today_400(client):
    today = main.now_kst().date()
    for d in (today.isoformat(), (today + timedelta(days=7)).isoformat(), "2026-13-99", "2026/09/10"):
        r = client.get("/v1/metrics", params={"date": d})
        assert r.status_code == 400 and r.json()["code"] == "invalid_date"


def test_metrics_missing_date_400(client):
    r = client.get("/v1/metrics")
    assert r.status_code == 400
    assert r.json() == {"code": "invalid_date", "message": "date query parameter is required"}


def test_metrics_retention_404_at_15_days(client):
    r = client.get("/v1/metrics", params={"date": days_ago(15)})
    assert r.status_code == 404
    assert r.json() == {"code": "data_not_retained",
                        "message": "metrics data for the requested date is past the retention window"}
    assert client.get("/v1/metrics", params={"date": days_ago(14)}).status_code == 200
    # usage 보존(90일)과 독립 — 같은 15일 전 date가 usage에서는 200
    assert client.get("/v1/usage/summary", params={"date": days_ago(15)}).status_code == 200


def test_metrics_not_ready_409_with_retry_after(client):
    main.SCN.not_ready_until_uptime_s = 10 ** 9   # 사실상 항상 미확정
    r = client.get("/v1/metrics", params={"date": yday()})
    assert r.status_code == 409 and r.headers["Retry-After"] == "5"
    assert r.json() == {"code": "data_not_ready",
                        "message": "metrics for the requested date is not finalized yet; retry later"}
    # 당일/미래 400은 409보다 우선
    assert client.get("/v1/metrics", params={"date": main.now_kst().date().isoformat()}).status_code == 400


def test_metrics_shares_request_counter_with_usage(client):
    main.SCN.rate_limit_every = 2
    assert client.get("/v1/usage/summary", params={"date": yday()}).status_code == 200
    r = client.get("/v1/metrics", params={"date": yday()})
    assert r.status_code == 429 and r.headers["Retry-After"] == "5"
    assert client.get("/v1/metrics", params={"date": yday()}).status_code == 200


def test_metrics_identity_drift(client):
    main.SCN.name_drift = " "
    body = client.get("/v1/metrics", params={"date": yday()}).json()
    assert body["service"] == "Mock Service A " and body["serviceGroup"] == "Mock Group "


def test_metrics_same_date_same_body(client):
    a = client.get("/v1/metrics", params={"date": yday()})
    b = client.get("/v1/metrics", params={"date": yday()})
    assert a.status_code == b.status_code == 200 and a.content == b.content


def test_metrics_scenario_flags_via_endpoint(client):
    assert client.post("/__mock/scenario", json={"metrics_empty_gpu": 1, "metrics_engine_null": 1}).status_code == 200
    body = client.get("/v1/metrics", params={"date": yday()}).json()
    assert body["gpu"] == [] and body["engine"] is None and len(body["serving"]) == 3
    client.post("/__mock/reset")
    body = client.get("/v1/metrics", params={"date": yday()}).json()
    assert len(body["gpu"]) == 5 and body["engine"] == METRICS_ENGINE


def test_usage_endpoints_unchanged_bytes(client):
    d = yday()
    expected = {"serviceGroup": main.CFG.service_group, "service": main.CFG.service, "date": d,
                "generatedAt": generated_at(d), **build_summary(build_records(main.CFG, d))}
    r = client.get("/v1/usage/summary", params={"date": d})
    assert r.status_code == 200
    assert r.content == json.dumps(expected, ensure_ascii=False, separators=(",", ":")).encode()
    # _date_gate 기본 인자 경로: usage 메시지 문자열 불변
    old = days_ago(main.CFG.retention_days + 1)
    assert client.get("/v1/usage/summary", params={"date": old}).json() == {
        "code": "data_not_retained",
        "message": "usage data for the requested date is past the retention window"}
    main.SCN.not_ready_until_uptime_s = 10 ** 9
    assert client.get("/v1/usage", params={"date": d}).json() == {
        "code": "data_not_ready",
        "message": "usage for the requested date is not finalized yet; retry later"}
