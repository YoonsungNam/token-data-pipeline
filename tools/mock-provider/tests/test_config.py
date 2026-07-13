import pytest

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


def test_models_deduped(monkeypatch):
    monkeypatch.setenv("MOCK_MODELS", "m1,m2,m1")
    assert load_config().models == ["m1", "m2"]


def test_negative_env_rejected(monkeypatch):
    monkeypatch.setenv("MOCK_RETENTION_DAYS", "-1")
    with pytest.raises(ValueError):
        load_config()
    monkeypatch.delenv("MOCK_RETENTION_DAYS")
    monkeypatch.setenv("MOCK_USERS", "-5")
    with pytest.raises(ValueError):
        load_config()
