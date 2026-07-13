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


def test_missing_date_is_400_with_contract_body(client):
    for path in ("/v1/usage", "/v1/usage/summary"):
        r = client.get(path)
        assert r.status_code == 400
        body = r.json()
        assert body["code"] == "invalid_date" and "message" in body


def test_non_numeric_limit_is_400(client):
    r = client.get("/v1/usage", params={"date": yesterday(), "limit": "abc"})
    assert r.status_code == 400 and r.json()["code"] == "invalid_limit"


def test_bad_date_and_limit_are_400(client):
    for bad in ("2026/06/15", "20260615", "2026-W25-1"):
        r = client.get("/v1/usage", params={"date": bad})
        assert r.status_code == 400 and r.json()["code"] == "invalid_date"
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
