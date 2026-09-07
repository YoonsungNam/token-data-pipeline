"""mart-metrics 배치 환경변수 (설계 §6.1) — mart/token-usage/app/config.py 클론 + 델타.

델타: EXPECTED_LATE_SERVICES·ORG_MAP_WARN_THRESHOLD 제거(토큰 mart 전용 — 6c의 M0 커버리지
기대 집합은 레지스트리 dim_token_metrics_service의 coverage_since/until로 계산하므로 late
목록이 없다), 뮤테이션 예산 MART_METRICS_MAX_MUTATIONS_PER_RUN 추가(설계 §4.0 장부 —
기본 64 = 4테이블 × 16일; batch.py가 첫 DELETE 전 exists 선조회 합산과 비교).
CH_DB_* 는 config가 아니라 app.ch 모듈 상수(로드 시 1회 평가)로 읽는다.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    return int(raw) if raw.strip() else default


@dataclass
class Config:
    ch_host: str = "localhost"
    ch_port: int = 8123
    ch_user: str = "default"
    ch_password: str = ""              # 운영 계정 mart(공유)는 Secret 주입
    ch_cluster: str = ""               # 빈 값 = 단일노드 (ON CLUSTER·clusterAllReplicas 생략, §4.0)
    retry_count: int = 10               # count 검증 재시도 횟수
    retry_interval_s: int = 5
    mutation_poll_s: int = 3            # wait_for_mutations 폴링 주기
    mutation_timeout_s: int = 300
    insert_quorum: str = ""             # 빈 값 = 미적용, company는 install.sh가 'auto' 주입
    max_mutations_per_run: int = 64     # 설계 §4.0 — 초과 시 FAILURE reason=mutation_budget (T5)


def load_config() -> Config:
    return Config(
        ch_host=os.getenv("CH_HOST", "localhost"),
        ch_port=_int_env("CH_PORT", 8123),
        ch_user=os.getenv("CH_USER", "default"),
        ch_password=os.getenv("CH_PASSWORD", ""),
        ch_cluster=os.getenv("CH_CLUSTER", ""),
        retry_count=_int_env("RETRY_COUNT", 10),
        retry_interval_s=_int_env("RETRY_INTERVAL_S", 5),
        mutation_poll_s=_int_env("MUTATION_POLL_S", 3),
        mutation_timeout_s=_int_env("MUTATION_TIMEOUT_S", 300),
        insert_quorum=os.getenv("INSERT_QUORUM", ""),
        max_mutations_per_run=_int_env("MART_METRICS_MAX_MUTATIONS_PER_RUN", 64),
    )
