from app.config import Config, load_config

ENV_KEYS = (
    "CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_CLUSTER",
    "RETRY_COUNT", "RETRY_INTERVAL_S", "MUTATION_POLL_S", "MUTATION_TIMEOUT_S",
    "INSERT_QUORUM", "MART_METRICS_MAX_MUTATIONS_PER_RUN",
    # 토큰 mart(mart/token-usage) 전용 — 6c에는 없어야 한다 (설계 §6.1: late 목록은 레지스트리
    # coverage_since/until로 대체). 잔존 env가 있어도 무시되는지 확인용으로 함께 지운다.
    "EXPECTED_LATE_SERVICES", "ORG_MAP_WARN_THRESHOLD",
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
    assert cfg.retry_count == 10
    assert cfg.retry_interval_s == 5
    assert cfg.mutation_poll_s == 3
    assert cfg.mutation_timeout_s == 300
    assert cfg.insert_quorum == ""                # 빈 값 = 미적용
    assert cfg.max_mutations_per_run == 64        # 설계 §4.0 — 4테이블 × 16일


def test_env_parsing(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("CH_HOST", "ch.internal")
    monkeypatch.setenv("CH_PORT", "9000")
    monkeypatch.setenv("CH_USER", "mart")
    monkeypatch.setenv("CH_PASSWORD", "secret")
    monkeypatch.setenv("CH_CLUSTER", "gpu-monitoring")
    monkeypatch.setenv("RETRY_COUNT", "3")
    monkeypatch.setenv("RETRY_INTERVAL_S", "2")
    monkeypatch.setenv("MUTATION_POLL_S", "1")
    monkeypatch.setenv("MUTATION_TIMEOUT_S", "60")
    monkeypatch.setenv("INSERT_QUORUM", "auto")
    monkeypatch.setenv("MART_METRICS_MAX_MUTATIONS_PER_RUN", "12")
    cfg = load_config()
    assert cfg.ch_host == "ch.internal"
    assert cfg.ch_port == 9000
    assert cfg.ch_user == "mart"
    assert cfg.ch_password == "secret"
    assert cfg.ch_cluster == "gpu-monitoring"
    assert cfg.retry_count == 3
    assert cfg.retry_interval_s == 2
    assert cfg.mutation_poll_s == 1
    assert cfg.mutation_timeout_s == 60
    assert cfg.insert_quorum == "auto"
    assert cfg.max_mutations_per_run == 12


def test_defaults_budget_64_and_no_expected_late(monkeypatch):
    """6c 델타: 예산 필드는 기본 64, 토큰 mart 전용 필드 2개는 존재하지 않는다(잔존 env가
    주입돼도 무시 — company Secret에 EXPECTED_LATE_SERVICES가 남아 있어도 무해)."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("EXPECTED_LATE_SERVICES", "svc-a,svc-b")
    monkeypatch.setenv("ORG_MAP_WARN_THRESHOLD", "0.5")
    cfg = Config()
    assert cfg.max_mutations_per_run == 64
    assert hasattr(cfg, "expected_late_services") is False
    assert hasattr(cfg, "org_map_warn_threshold") is False
    loaded = load_config()
    assert loaded == Config()                     # 잔존 env 무시 → 전부 기본값
    assert hasattr(loaded, "expected_late_services") is False


def test_env_override_budget(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("MART_METRICS_MAX_MUTATIONS_PER_RUN", "8")
    assert load_config().max_mutations_per_run == 8
    monkeypatch.setenv("MART_METRICS_MAX_MUTATIONS_PER_RUN", "  ")   # 공백 = 미설정 → 기본값
    assert load_config().max_mutations_per_run == 64
