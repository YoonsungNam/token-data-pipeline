import os
from dataclasses import dataclass

import yaml


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    return int(raw) if raw.strip() else default


@dataclass
class Config:
    ch_host: str = "localhost"
    ch_port: int = 8123
    ch_user: str = "default"
    ch_password: str = ""
    ch_cluster: str = ""            # 빈 값 = 단일노드, ON CLUSTER 생략 (§4.0)
    vm_push_url: str = ""           # 빈 값 = VM push 생략 (§5.5)
    endpoints_file: str = "endpoints.yaml"
    max_pages: int = 200            # 도달 = 부분 적재 금지, FAILURE (§5.2)
    soft_deadline_minutes: int = 50
    max_buffer_rows: int = 20_000   # §5.1 메모리 규칙
    not_ready_budget_minutes: int = 30
    https_proxy: str | None = None  # None=상속, ''=직접 연결, 값=전용 프록시 (§5.7)
    api_verify: bool | str = True   # False | True | CA bundle 경로


def load_config() -> Config:
    verify_raw = os.getenv("COLLECTOR_API_VERIFY", "")
    ca_bundle = os.getenv("COLLECTOR_API_CA_BUNDLE", "")
    api_verify: bool | str = True
    if verify_raw.strip().lower() == "false":
        api_verify = False
    elif ca_bundle.strip():
        api_verify = ca_bundle.strip()
    return Config(
        ch_host=os.getenv("CH_HOST", "localhost"),
        ch_port=_int_env("CH_PORT", 8123),
        ch_user=os.getenv("CH_USER", "default"),
        ch_password=os.getenv("CH_PASSWORD", ""),
        ch_cluster=os.getenv("CH_CLUSTER", ""),
        vm_push_url=os.getenv("VM_PUSH_URL", ""),
        endpoints_file=os.getenv("ENDPOINTS_FILE", "endpoints.yaml"),
        max_pages=_int_env("MAX_PAGES", 200),
        soft_deadline_minutes=_int_env("SOFT_DEADLINE_MINUTES", 50),
        max_buffer_rows=_int_env("MAX_BUFFER_ROWS", 20_000),
        not_ready_budget_minutes=_int_env("NOT_READY_BUDGET_MINUTES", 30),
        https_proxy=os.environ.get("COLLECTOR_HTTPS_PROXY"),
        api_verify=api_verify,
    )


@dataclass(frozen=True)
class ServiceEntry:
    service_group: str
    service: str
    base_url: str
    enabled: bool
    source_type: str = "usage-api-v1"


def load_endpoints(path: str) -> list[ServiceEntry]:
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    entries: list[ServiceEntry] = []
    seen: set[str] = set()
    for i, item in enumerate(doc.get("services") or []):
        missing = [k for k in ("serviceGroup", "service", "baseUrl", "enabled") if k not in item]
        if missing:
            raise ValueError(f"services[{i}]: missing keys {missing}")
        group = str(item["serviceGroup"]).strip()
        service = str(item["service"]).strip()
        base_url = str(item["baseUrl"]).strip()
        if not group or not service or not base_url:
            raise ValueError(f"services[{i}]: empty serviceGroup/service/baseUrl")
        if service in seen:
            raise ValueError(f"services[{i}]: duplicate service '{service}'")
        seen.add(service)
        entries.append(ServiceEntry(
            service_group=group, service=service, base_url=base_url.rstrip("/"),
            enabled=bool(item["enabled"]),
            source_type=str(item.get("type", "usage-api-v1")),
        ))
    if not entries:
        raise ValueError("endpoints file has no services")
    return entries
