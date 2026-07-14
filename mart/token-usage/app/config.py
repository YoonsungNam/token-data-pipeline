import os
from dataclasses import dataclass, field


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    return int(raw) if raw.strip() else default


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "")
    return float(raw) if raw.strip() else default


def _csv_env(name: str) -> list[str]:
    raw = os.getenv(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass
class Config:
    ch_host: str = "localhost"
    ch_port: int = 8123
    ch_user: str = "default"
    ch_password: str = ""              # 운영 계정 mart(공유)는 Secret 주입
    ch_cluster: str = ""               # 빈 값 = 단일노드 (ON CLUSTER·clusterAllReplicas 생략, §4.0)
    expected_late_services: list[str] = field(default_factory=list)  # STEP 0 경고 제외 (§5.9-9)
    org_map_warn_threshold: float = 0.2   # dim_token_user_org 매핑 실패율 CHECK WARN 임계
    retry_count: int = 10               # count 검증 재시도 횟수 (§7.1 리뷰 #10)
    retry_interval_s: int = 5
    mutation_poll_s: int = 3            # wait_for_mutations 폴링 주기
    mutation_timeout_s: int = 300
    insert_quorum: str = ""             # 빈 값 = 미적용, company는 install.sh가 'auto' 주입 (§9-19)


def load_config() -> Config:
    return Config(
        ch_host=os.getenv("CH_HOST", "localhost"),
        ch_port=_int_env("CH_PORT", 8123),
        ch_user=os.getenv("CH_USER", "default"),
        ch_password=os.getenv("CH_PASSWORD", ""),
        ch_cluster=os.getenv("CH_CLUSTER", ""),
        expected_late_services=_csv_env("EXPECTED_LATE_SERVICES"),
        org_map_warn_threshold=_float_env("ORG_MAP_WARN_THRESHOLD", 0.2),
        retry_count=_int_env("RETRY_COUNT", 10),
        retry_interval_s=_int_env("RETRY_INTERVAL_S", 5),
        mutation_poll_s=_int_env("MUTATION_POLL_S", 3),
        mutation_timeout_s=_int_env("MUTATION_TIMEOUT_S", 300),
        insert_quorum=os.getenv("INSERT_QUORUM", ""),
    )
