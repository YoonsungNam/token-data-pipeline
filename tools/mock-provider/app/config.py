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
    metrics_retention_days: int = 14        # /v1/metrics 보존 일수 (계약: 14일 초과 → 404)


def load_config() -> Config:
    models_raw = os.getenv("MOCK_MODELS", "")
    models = [m.strip() for m in models_raw.split(",") if m.strip()]
    models = list(dict.fromkeys(models))
    cfg = Config(
        service_group=os.getenv("MOCK_SERVICE_GROUP", "Mock Group"),
        service=os.getenv("MOCK_SERVICE", "Mock Service A"),
        seed=os.getenv("MOCK_SEED", "token-mock-1"),
        users=_int_env("MOCK_USERS", 50),
        anon_users=_int_env("MOCK_ANON_USERS", 10),
        models=models or ["claude-opus-4-8", "claude-sonnet-5", "claude-haiku-4-5"],
        retention_days=_int_env("MOCK_RETENTION_DAYS", 90),
        metrics_retention_days=_int_env("MOCK_METRICS_RETENTION_DAYS", 14),
    )
    if cfg.users < 0 or cfg.anon_users < 0:
        raise ValueError("MOCK_USERS/MOCK_ANON_USERS must be >= 0")
    if cfg.retention_days < 1:
        raise ValueError("MOCK_RETENTION_DAYS must be >= 1")
    if cfg.metrics_retention_days < 1:
        raise ValueError("MOCK_METRICS_RETENTION_DAYS must be >= 1")
    return cfg
