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
