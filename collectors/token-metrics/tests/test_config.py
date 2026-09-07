from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.config import (
    DEFAULT_API_SINCE, DEFAULT_COVERAGE_SINCE, Config, ServiceEntry, load_config, load_endpoints,
)

KST = timezone(timedelta(hours=9))
REPO_ROOT = Path(__file__).resolve().parents[3]           # tests/ → token-metrics/ → collectors/ → 레포 루트
FIXTURE = REPO_ROOT / "assets" / "model-catalog" / "fixtures" / "synthetic_endpoints_metrics.yaml"
ENV_KEYS = ("CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_CLUSTER", "ENDPOINTS_FILE",
            "SOFT_DEADLINE_MINUTES", "LOAD_BUDGET_S", "FINAL_HOUR_KST", "MAX_RESPONSE_BYTES",
            "METRICS_MAX_MUTATIONS_PER_RUN",
            "COLLECTOR_HTTPS_PROXY", "COLLECTOR_API_VERIFY", "COLLECTOR_API_CA_BUNDLE")
MINIMAL = (
    "services:\n"
    "  - serviceGroup: \"Mock Group\"\n    service: \"Mock Service A\"\n"
    "    baseUrl: \"http://mock\"\n    enabled: true\n"
)


def _clear_env(monkeypatch):
    for k in ENV_KEYS:
        monkeypatch.delenv(k, raising=False)


def test_config_defaults(monkeypatch):
    _clear_env(monkeypatch)
    cfg = load_config()
    assert cfg.ch_host == "localhost" and cfg.ch_port == 8123 and cfg.ch_user == "default"
    assert cfg.ch_password == ""
    assert cfg.ch_cluster == ""              # 빈 값 = ON CLUSTER 생략
    assert cfg.endpoints_file == "endpoints.yaml"
    assert cfg.soft_deadline_minutes == 40   # §5.2
    assert cfg.load_budget_s == 1200         # §5.2
    assert cfg.final_hour_kst == 9           # §5.2 최종 슬롯
    assert cfg.max_response_bytes == 5_000_000
    assert cfg.max_mutations_per_run == 45   # §4.0 뮤테이션 장부
    assert cfg.https_proxy is None           # 미설정 = 시스템 상속
    assert cfg.api_verify is True
    # 클론에서 제거된 필드 — VM push·페이지네이션·버퍼·NOT_READY 예산 없음 (§5.1·§5.2)
    for gone in ("vm_push_url", "max_pages", "max_buffer_rows", "not_ready_budget_minutes"):
        assert not hasattr(cfg, gone), gone


def test_soft_deadline_exceeds_load_budget(monkeypatch):
    # §5.2 불변식 SOFT×60 > LOAD_BUDGET — 기본값(2400 > 1200)과 dataclass 기본값 양쪽에서 고정
    _clear_env(monkeypatch)
    cfg = load_config()
    assert cfg.soft_deadline_minutes * 60 > cfg.load_budget_s
    assert cfg.soft_deadline_minutes * 60 == 2400 and cfg.load_budget_s == 1200
    assert Config().soft_deadline_minutes * 60 > Config().load_budget_s


def test_load_config_rejects_budget_over_deadline(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("SOFT_DEADLINE_MINUTES", "10")
    monkeypatch.setenv("LOAD_BUDGET_S", "1200")
    with pytest.raises(ValueError, match="SOFT_DEADLINE_MINUTES\\*60 must exceed LOAD_BUDGET_S"):
        load_config()
    monkeypatch.setenv("SOFT_DEADLINE_MINUTES", "20")     # 1200 == 1200 도 거부 (<=)
    with pytest.raises(ValueError):
        load_config()


def test_env_override(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("METRICS_MAX_MUTATIONS_PER_RUN", "3")
    monkeypatch.setenv("FINAL_HOUR_KST", "8")
    monkeypatch.setenv("MAX_RESPONSE_BYTES", "100")
    monkeypatch.setenv("CH_HOST", "chi-gpu-monitoring.clickhouse.svc")
    monkeypatch.setenv("CH_PORT", "8124")
    monkeypatch.setenv("CH_USER", "mart")
    monkeypatch.setenv("CH_CLUSTER", "gpu-monitoring")
    monkeypatch.setenv("ENDPOINTS_FILE", "/etc/token-metrics/endpoints.yaml")
    monkeypatch.setenv("SOFT_DEADLINE_MINUTES", "")         # 빈 문자열 = 기본값 (_int_env)
    cfg = load_config()
    assert cfg.max_mutations_per_run == 3
    assert cfg.final_hour_kst == 8
    assert cfg.max_response_bytes == 100
    assert cfg.ch_host == "chi-gpu-monitoring.clickhouse.svc" and cfg.ch_port == 8124
    assert cfg.ch_user == "mart" and cfg.ch_cluster == "gpu-monitoring"
    assert cfg.endpoints_file == "/etc/token-metrics/endpoints.yaml"
    assert cfg.soft_deadline_minutes == 40


def test_proxy_and_verify_semantics(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("COLLECTOR_HTTPS_PROXY", "")      # 빈 문자열 = 프록시 무시(직접)
    monkeypatch.setenv("COLLECTOR_API_CA_BUNDLE", "/etc/ca.pem")
    cfg = load_config()
    assert cfg.https_proxy == ""
    assert cfg.api_verify == "/etc/ca.pem"
    monkeypatch.setenv("COLLECTOR_API_VERIFY", "false")
    assert load_config().api_verify is False
    monkeypatch.setenv("COLLECTOR_HTTPS_PROXY", "http://proxy.example.internal:3128")
    assert load_config().https_proxy == "http://proxy.example.internal:3128"


def test_load_endpoints_defaults(tmp_path):
    p = tmp_path / "eps.yaml"
    p.write_text(MINIMAL, encoding="utf-8")
    eps = load_endpoints(str(p))
    assert len(eps) == 1
    e = eps[0]
    assert e.service_group == "Mock Group" and e.service == "Mock Service A"
    assert e.base_url == "http://mock" and e.enabled is True
    assert e.api_since == date(2026, 9, 9) == date.fromisoformat(DEFAULT_API_SINCE)          # §4.3 기본
    assert e.coverage_since == date(2026, 8, 26) == date.fromisoformat(DEFAULT_COVERAGE_SINCE)
    assert e.until is None
    assert e.expect_gpu and e.expect_serving
    assert not e.usage_includes_consumers
    assert e.note == ""
    assert isinstance(e, ServiceEntry)


def test_load_endpoints_full_fields(tmp_path):
    p = tmp_path / "eps.yaml"
    p.write_text(
        "services:\n"
        "  - serviceGroup: \"Mock Group\"          # 토큰 레지스트리와 바이트 동일\n"
        "    service: \"Mock Service A\"\n"
        "    baseUrl: \"http://token-mock-provider-a.monitoring.svc:8000/\"\n"
        "    enabled: true\n"
        "    apiSince: 2026-09-09\n"                 # 따옴표 없는 YAML 날짜도 허용
        "    coverageSince: \"2026-08-26\"\n"
        "    until: \"2026-12-31\"\n"
        "    expectGpu: false\n"
        "    expectServing: true\n"
        "    usageIncludesConsumers: true\n"
        "    note: \"platform provider\"\n",
        encoding="utf-8",
    )
    e = load_endpoints(str(p))[0]
    assert e.base_url == "http://token-mock-provider-a.monitoring.svc:8000"   # trailing '/' 제거
    assert e.api_since == date(2026, 9, 9) and e.coverage_since == date(2026, 8, 26)
    assert e.until == date(2026, 12, 31)
    assert e.expect_gpu is False and e.expect_serving is True
    assert e.usage_includes_consumers is True
    assert e.note == "platform provider"


def test_load_endpoints_synthetic_fixture():
    # Plan 6a T8 산출물 — 6b 로더가 그대로 읽어야 한다 (설계 §4.3 형식의 정본 예시)
    assert FIXTURE.exists(), f"Plan 6a T8 fixture 부재: {FIXTURE}"
    eps = load_endpoints(str(FIXTURE))
    assert len(eps) >= 1
    services = [e.service for e in eps]
    assert len(services) == len(set(services))
    assert all(e.service_group == "Mock Group" for e in eps)
    assert all(e.api_since == date(2026, 9, 9) and e.until is None for e in eps)


def test_load_endpoints_rejects_bad(tmp_path):
    def load(text: str):
        p = tmp_path / "bad.yaml"
        p.write_text(text, encoding="utf-8")
        return load_endpoints(str(p))

    with pytest.raises(ValueError, match=r"services\[0\]: missing keys \['service'\]"):
        load("services:\n  - {serviceGroup: G, baseUrl: 'http://a', enabled: true}\n")
    with pytest.raises(ValueError, match="duplicate service 'S'"):
        load("services:\n"
             "  - {serviceGroup: G, service: S, baseUrl: 'http://a', enabled: true}\n"
             "  - {serviceGroup: G, service: S, baseUrl: 'http://b', enabled: true}\n")
    with pytest.raises(ValueError, match="empty serviceGroup/service/baseUrl"):
        load("services:\n  - {serviceGroup: '', service: S, baseUrl: 'http://a', enabled: true}\n")
    with pytest.raises(ValueError, match=r"services\[0\]: bad date apiSince"):
        load("services:\n  - {serviceGroup: G, service: S, baseUrl: 'http://a', enabled: true, apiSince: '2026-13-01'}\n")
    with pytest.raises(ValueError, match=r"services\[0\]: bad date until"):
        load("services:\n  - {serviceGroup: G, service: S, baseUrl: 'http://a', enabled: true, until: 'soon'}\n")
    with pytest.raises(ValueError, match=r"services\[0\]: until before coverageSince"):
        load("services:\n  - {serviceGroup: G, service: S, baseUrl: 'http://a', enabled: true,"
             " coverageSince: '2026-08-26', until: '2026-08-25'}\n")
    with pytest.raises(ValueError, match=r"services\[1\]: not a mapping"):
        load("services:\n  - {serviceGroup: G, service: S, baseUrl: 'http://a', enabled: true}\n  - just-a-string\n")
    with pytest.raises(ValueError, match="endpoints file has no services"):
        load("services: []\n")
    with pytest.raises(ValueError, match="endpoints file has no services"):
        load("# 빈 문서\n")


def test_load_endpoints_unknown_keys_ignored_and_coverage_after_since_allowed(tmp_path):
    p = tmp_path / "eps.yaml"
    p.write_text(
        "services:\n"
        "  - {serviceGroup: G, service: S, baseUrl: 'http://a', enabled: true,"
        " type: usage-api-v1, foo: 1, apiSince: '2026-09-01', coverageSince: '2026-09-15'}\n",
        encoding="utf-8",
    )
    e = load_endpoints(str(p))[0]
    assert not hasattr(e, "source_type")             # 기존 모듈의 type→source_type 은 클론에서 제거
    assert e.coverage_since > e.api_since             # 허용 — 검증 항목 아님


def test_dim_row_and_key_shapes():
    entry = ServiceEntry(
        service_group=" Mock Group ", service="Mock Service A", base_url="http://mock", enabled=True,
        api_since=date(2026, 9, 9), coverage_since=date(2026, 8, 26), until=None,
        expect_gpu=True, expect_serving=False, usage_includes_consumers=False, note=" ops ",
    )
    now = datetime(2026, 9, 11, 2, 5, tzinfo=KST)
    row = entry.dim_row(now)
    assert len(row) == 12                                  # DDL 12컬럼 순서
    assert row[0] == "Mock Group" and row[1] == "Mock Service A" and row[2] == "http://mock"
    assert row[3] == 1                                     # enabled → UInt8
    assert row[4] == date(2026, 9, 9) and row[5] == date(2026, 8, 26) and row[6] is None
    assert row[7] == 1 and row[8] == 0 and row[9] == 0     # expect_gpu / expect_serving / usage_includes_consumers
    assert row[10] == "ops"
    assert row[11] is now                                  # updated_at 마지막
    key = entry.dim_key()
    assert len(key) == 11
    assert all(not isinstance(v, datetime) for v in key)   # updated_at 미포함
    assert key == tuple(row[:11])
    assert isinstance(key, tuple)
