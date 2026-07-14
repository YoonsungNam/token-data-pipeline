from app.config import load_config

ENV_KEYS = (
    "CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_CLUSTER",
    "EXPECTED_LATE_SERVICES", "ORG_MAP_WARN_THRESHOLD",
    "RETRY_COUNT", "RETRY_INTERVAL_S", "MUTATION_POLL_S", "MUTATION_TIMEOUT_S",
    "INSERT_QUORUM",
)


def _clear_env(monkeypatch):
    for k in ENV_KEYS:
        monkeypatch.delenv(k, raising=False)


def test_config_defaults(monkeypatch):
    _clear_env(monkeypatch)
    cfg = load_config()
    assert cfg.ch_host == "localhost"
    assert cfg.ch_port == 8123
    assert cfg.ch_user == "default"
    assert cfg.ch_password == ""
    assert cfg.ch_cluster == ""                  # 빈 값 = 단일노드, ON CLUSTER 생략
    assert cfg.expected_late_services == []
    assert cfg.org_map_warn_threshold == 0.2
    assert cfg.retry_count == 10
    assert cfg.retry_interval_s == 5
    assert cfg.mutation_poll_s == 3
    assert cfg.mutation_timeout_s == 300
    assert cfg.insert_quorum == ""                # 빈 값 = 미적용


def test_env_parsing(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("CH_HOST", "ch.internal")
    monkeypatch.setenv("CH_PORT", "9000")
    monkeypatch.setenv("CH_USER", "mart")
    monkeypatch.setenv("CH_PASSWORD", "secret")
    monkeypatch.setenv("CH_CLUSTER", "gpu-monitoring")
    monkeypatch.setenv("ORG_MAP_WARN_THRESHOLD", "0.35")
    monkeypatch.setenv("RETRY_COUNT", "3")
    monkeypatch.setenv("RETRY_INTERVAL_S", "2")
    monkeypatch.setenv("MUTATION_POLL_S", "1")
    monkeypatch.setenv("MUTATION_TIMEOUT_S", "60")
    monkeypatch.setenv("INSERT_QUORUM", "auto")
    cfg = load_config()
    assert cfg.ch_host == "ch.internal"
    assert cfg.ch_port == 9000
    assert cfg.ch_user == "mart"
    assert cfg.ch_password == "secret"
    assert cfg.ch_cluster == "gpu-monitoring"
    assert cfg.org_map_warn_threshold == 0.35
    assert cfg.retry_count == 3
    assert cfg.retry_interval_s == 2
    assert cfg.mutation_poll_s == 1
    assert cfg.mutation_timeout_s == 60
    assert cfg.insert_quorum == "auto"


def test_expected_late_services_comma_split_and_empty_removed(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("EXPECTED_LATE_SERVICES", "svc-a, svc-b,,  svc-c ,")
    cfg = load_config()
    assert cfg.expected_late_services == ["svc-a", "svc-b", "svc-c"]


def test_expected_late_services_default_empty(monkeypatch):
    _clear_env(monkeypatch)
    cfg = load_config()
    assert cfg.expected_late_services == []
