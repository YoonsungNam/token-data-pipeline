"""env + endpoints.yaml 로더 — collectors/token-usage/app/config.py 의 클론 (설계 2026-08-31 §5.1).

Config      : §5.2 env 목록. VM push·페이지네이션·버퍼·NOT_READY 예산 항목은 없고
              LOAD_BUDGET_S / FINAL_HOUR_KST / MAX_RESPONSE_BYTES / METRICS_MAX_MUTATIONS_PER_RUN 이 추가.
              불변식 SOFT_DEADLINE_MINUTES*60 > LOAD_BUDGET_S 를 load_config 가 강제한다.
              DB 명(CH_DB_FACT / CH_DB_DIM)은 여기 없다 — app/writer.py 모듈 상수 DB_FACT / DB_DIM 이 읽는다.
ServiceEntry: 레지스트리 gpu_data.dim_token_metrics_service 의 updated_at 제외 11컬럼 (§4.3).
              dim_key() = diff-sync 비교 키, dim_row(updated_at) = INSERT 값 행 12개 (DDL 컬럼 순서).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime

import yaml

DEFAULT_API_SINCE = "2026-09-09"        # §4.3 — go-live 첫 데이터 날짜 (정기 API 수집 게이트)
DEFAULT_COVERAGE_SINCE = "2026-08-26"   # §4.3 — M0 커버리지 기대 시작일 (manual 포함)
_REQUIRED_KEYS = ("serviceGroup", "service", "baseUrl", "enabled")


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    return int(raw) if raw.strip() else default


@dataclass
class Config:
    ch_host: str = "localhost"
    ch_port: int = 8123
    ch_user: str = "default"
    ch_password: str = ""
    ch_cluster: str = ""                 # 빈 값 = 단일노드, ON CLUSTER 생략 (§4.0)
    endpoints_file: str = "endpoints.yaml"
    soft_deadline_minutes: int = 40      # §5.2 — 2400s = 신규 착수·409 재방문 창 + 예약된 적재 예산
    load_budget_s: int = 1200            # §5.2 — 데드라인 앞에 예약된 적재 예산 (SOFT*60 > LOAD 불변식)
    final_hour_kst: int = 9              # §5.2 — batch_time KST hour >= 값 이면 최종 슬롯 (final=1)
    max_response_bytes: int = 5_000_000  # §5.2 — 본문 > 5MB 는 PERMANENT_ERROR
    max_mutations_per_run: int = 45      # §4.0 뮤테이션 장부 — 예정 DELETE 수 초과 시 reason=mutation_budget
    https_proxy: str | None = None       # None=상속, ''=직접 연결, 값=전용 프록시
    api_verify: bool | str = True        # False | True | CA bundle 경로


def load_config() -> Config:
    verify_raw = os.getenv("COLLECTOR_API_VERIFY", "")
    ca_bundle = os.getenv("COLLECTOR_API_CA_BUNDLE", "")
    api_verify: bool | str = True
    if verify_raw.strip().lower() == "false":
        api_verify = False
    elif ca_bundle.strip():
        api_verify = ca_bundle.strip()
    cfg = Config(
        ch_host=os.getenv("CH_HOST", "localhost"),
        ch_port=_int_env("CH_PORT", 8123),
        ch_user=os.getenv("CH_USER", "default"),
        ch_password=os.getenv("CH_PASSWORD", ""),
        ch_cluster=os.getenv("CH_CLUSTER", ""),
        endpoints_file=os.getenv("ENDPOINTS_FILE", "endpoints.yaml"),
        soft_deadline_minutes=_int_env("SOFT_DEADLINE_MINUTES", 40),
        load_budget_s=_int_env("LOAD_BUDGET_S", 1200),
        final_hour_kst=_int_env("FINAL_HOUR_KST", 9),
        max_response_bytes=_int_env("MAX_RESPONSE_BYTES", 5_000_000),
        max_mutations_per_run=_int_env("METRICS_MAX_MUTATIONS_PER_RUN", 45),
        https_proxy=os.environ.get("COLLECTOR_HTTPS_PROXY"),
        api_verify=api_verify,
    )
    # §5.2 불변식: 소프트 데드라인(신규 착수·409 재방문 창)이 적재 예산보다 커야 예산 예약이 성립한다.
    if cfg.soft_deadline_minutes * 60 <= cfg.load_budget_s:
        raise ValueError("SOFT_DEADLINE_MINUTES*60 must exceed LOAD_BUDGET_S")
    return cfg


@dataclass(frozen=True)
class ServiceEntry:
    service_group: str
    service: str
    base_url: str
    enabled: bool
    api_since: date
    coverage_since: date
    until: date | None
    expect_gpu: bool = True
    expect_serving: bool = True
    usage_includes_consumers: bool = False
    note: str = ""

    def dim_key(self) -> tuple:
        """레지스트리 diff 비교 키 = updated_at 제외 11컬럼 (§4.3) — DDL 컬럼 순서·타입(UInt8→int, Date→date)."""
        return (
            self.service_group.strip(), self.service.strip(), self.base_url.strip(), int(self.enabled),
            self.api_since, self.coverage_since, self.until,
            int(self.expect_gpu), int(self.expect_serving), int(self.usage_includes_consumers),
            self.note.strip(),
        )

    def dim_row(self, updated_at: datetime) -> list:
        """INSERT 값 행 12개 (DDL 컬럼 순서) — updated_at 은 aware KST datetime (writer 의 now_kst())."""
        return list(self.dim_key()) + [updated_at]


def _date_field(item: dict, key: str, default: str | None, i: int) -> date | None:
    """YYYY-MM-DD 문자열 또는 YAML date → date. 부재·null·빈 문자열은 default (None 이면 None)."""
    raw = item.get(key)
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        raw = default
    if raw is None:
        return None
    try:
        return date.fromisoformat(str(raw).strip())
    except ValueError as exc:
        raise ValueError(f"services[{i}]: bad date {key}") from exc


def load_endpoints(path: str) -> list[ServiceEntry]:
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    entries: list[ServiceEntry] = []
    seen: set[str] = set()
    for i, item in enumerate((doc or {}).get("services") or []):
        if not isinstance(item, dict):
            raise ValueError(f"services[{i}]: not a mapping")
        missing = [k for k in _REQUIRED_KEYS if k not in item]
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
        api_since = _date_field(item, "apiSince", DEFAULT_API_SINCE, i)
        coverage_since = _date_field(item, "coverageSince", DEFAULT_COVERAGE_SINCE, i)
        until = _date_field(item, "until", None, i)
        if until is not None and until < coverage_since:
            raise ValueError(f"services[{i}]: until before coverageSince")
        # coverage_since > api_since 는 허용 (검증하지 않음 — §4.3 두 날짜는 독립 게이트)
        entries.append(ServiceEntry(
            service_group=group, service=service, base_url=base_url.rstrip("/"),
            enabled=bool(item["enabled"]),
            api_since=api_since, coverage_since=coverage_since, until=until,
            expect_gpu=bool(item.get("expectGpu", True)),
            expect_serving=bool(item.get("expectServing", True)),
            usage_includes_consumers=bool(item.get("usageIncludesConsumers", False)),
            note=str(item.get("note") or "").strip(),
        ))   # 알 수 없는 키(type 등)는 무시
    if not entries:
        raise ValueError("endpoints file has no services")
    return entries
