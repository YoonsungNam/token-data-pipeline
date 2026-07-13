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
