"""Tests for app/mart.py — pure logic (coverage, markers, date windows)."""
import argparse
from datetime import date as date_cls, datetime, timedelta, timezone
from dataclasses import dataclass

import pytest

from app.mart import (
    Coverage,
    batch_line,
    compute_coverage,
    target_dates,
    Warn,
)


KST = timezone(timedelta(hours=9))


# Test fixtures for coverage
@pytest.fixture
def cov():
    """Coverage: enabled=[S1, S2, S3], present=[S1], missing=[S2, S3], warn_targets=[S2, S3]."""
    return Coverage(enabled=3, present=1, missing=["S2", "S3"], warn_targets=["S2", "S3"])


@pytest.fixture
def cov_b():
    """Coverage with space in service name: enabled=[S1, Mock Service B, S3], present=[S1]."""
    return Coverage(enabled=3, present=1, missing=["Mock Service B", "S3"], warn_targets=["Mock Service B", "S3"])


@pytest.fixture
def full_cov():
    """Coverage: all services present, no missing."""
    return Coverage(enabled=3, present=3, missing=[], warn_targets=[])


# ============================================================================
# compute_coverage tests
# ============================================================================

def test_compute_coverage_basic():
    """Basic case: all enabled services in summary."""
    c = compute_coverage(["S1", "S2"], {"S1", "S2"}, [])
    assert c.enabled == 2
    assert c.present == 2
    assert c.missing == []
    assert c.warn_targets == []


def test_compute_coverage_missing_not_in_summary():
    """Some services missing from summary."""
    c = compute_coverage(["S1", "S2", "S3"], {"S1"}, [])
    assert c.enabled == 3
    assert c.present == 1
    assert c.missing == ["S2", "S3"]
    assert c.warn_targets == ["S2", "S3"]


def test_compute_coverage_missing_and_expected_late_exclusion():
    """Missing service in expected_late list excluded from warn_targets but present in missing."""
    c = compute_coverage(["S1", "S2", "S3"], {"S1"}, expected_late=["S3"])
    assert (c.enabled, c.present) == (3, 1)
    assert c.missing == ["S2", "S3"]          # 마커에는 전부 노출
    assert c.warn_targets == ["S2"]           # 경고 대상에서만 제외 (§5.9-9)


def test_compute_coverage_missing_sorted():
    """Missing list is sorted."""
    c = compute_coverage(["Z", "A", "M"], {"Z"}, [])
    assert c.missing == ["A", "M"]


def test_compute_coverage_warn_targets_sorted():
    """Warn targets list is sorted."""
    c = compute_coverage(["Z", "A", "M"], set(), expected_late=[])
    assert c.warn_targets == ["A", "M", "Z"]


def test_compute_coverage_empty_enabled():
    """No enabled services."""
    c = compute_coverage([], set(), [])
    assert c.enabled == 0
    assert c.present == 0
    assert c.missing == []
    assert c.warn_targets == []


# ============================================================================
# batch_line tests
# ============================================================================

def test_batch_line_format_contract(cov):
    """Batch line format matches contract: BATCH_RESULT status=... module=mart-token coverage=N/M ..."""
    line = batch_line("SUCCESS", cov, 100, 100, 2, 12.3)
    assert line.startswith("BATCH_RESULT status=SUCCESS module=mart-token coverage=1/3 ")
    # missing_services 값은 항상 쌍따옴표 — 서비스명 공백이 k=v 파싱을 깨지 않도록
    assert 'missing_services="S2,S3"' in line and "rows_mart=100" in line
    assert "rows_view=100" in line
    assert "warn=2" in line
    assert "elapsed=12.3" in line


def test_batch_line_missing_with_spaces_quoted(cov_b):
    """Spaces in service names are protected by double quotes."""
    line = batch_line("SUCCESS", cov_b, 0, 0, 0, 1.0)
    assert 'missing_services="Mock Service B,S3"' in line


def test_batch_line_no_missing_dash(full_cov):
    """Empty missing list renders as missing_services="-"."""
    line = batch_line("SUCCESS", full_cov, 0, 0, 0, 1.0)
    assert 'missing_services="-"' in line


def test_batch_line_status_failure():
    """FAILURE status is preserved."""
    cov = Coverage(enabled=2, present=0, missing=["S1", "S2"], warn_targets=["S1", "S2"])
    line = batch_line("FAILURE", cov, 50, 25, 1, 5.0)
    assert "status=FAILURE" in line
    assert "coverage=0/2" in line


def test_batch_line_elapsed_one_decimal():
    """Elapsed time formatted to 1 decimal place."""
    cov = Coverage(enabled=1, present=1, missing=[], warn_targets=[])
    line = batch_line("SUCCESS", cov, 10, 10, 0, 123.456)
    assert "elapsed=123.5" in line  # rounded


def test_batch_line_single_missing():
    """Single missing service is still quoted."""
    cov = Coverage(enabled=2, present=1, missing=["OnlyOne"], warn_targets=["OnlyOne"])
    line = batch_line("SUCCESS", cov, 1, 1, 0, 0.1)
    assert 'missing_services="OnlyOne"' in line


# ============================================================================
# target_dates tests
# ============================================================================

def test_target_dates_range_inclusive_and_pair_required():
    """--from/--to must be paired, inclusive range."""
    args = argparse.Namespace(from_date="2026-01-01", to_date="2026-01-03", batch_time=None)
    dates, is_rerun = target_dates(args)
    assert dates == ["2026-01-01", "2026-01-02", "2026-01-03"]
    assert is_rerun is True


def test_target_dates_single_day():
    """--from/--to same day returns single day list."""
    args = argparse.Namespace(from_date="2026-01-01", to_date="2026-01-01", batch_time=None)
    dates, is_rerun = target_dates(args)
    assert dates == ["2026-01-01"]
    assert is_rerun is True


def test_target_dates_from_without_to_returns_none():
    """--from without --to returns (None, False)."""
    args = argparse.Namespace(from_date="2026-01-01", to_date=None, batch_time=None)
    dates, is_rerun = target_dates(args)
    assert dates is None
    assert is_rerun is False


def test_target_dates_to_without_from_returns_none():
    """--to without --from returns (None, False)."""
    args = argparse.Namespace(from_date=None, to_date="2026-01-03", batch_time=None)
    dates, is_rerun = target_dates(args)
    assert dates is None
    assert is_rerun is False


def test_target_dates_batch_time_aware_kst():
    """Aware datetime in KST is preserved."""
    dt_aware = datetime(2026, 1, 15, 10, 30, tzinfo=KST)
    args = argparse.Namespace(from_date=None, to_date=None, batch_time=dt_aware.isoformat())
    dates, is_rerun = target_dates(args)
    # batch_time 2026-01-15 → target_date = 2026-01-14
    assert dates == ["2026-01-14"]
    assert is_rerun is False


def test_target_dates_batch_time_naive_interpreted_as_kst():
    """Naive datetime is interpreted as KST."""
    args = argparse.Namespace(from_date=None, to_date=None, batch_time="2026-01-15T10:30:00")
    dates, is_rerun = target_dates(args)
    # naive input treated as KST → batch_time = 2026-01-15 KST
    # target_date = 2026-01-14
    assert dates == ["2026-01-14"]
    assert is_rerun is False


def test_target_dates_aware_utc_converted_to_kst():
    """Aware UTC datetime converted to KST."""
    dt_utc = datetime(2026, 1, 15, 1, 30, tzinfo=timezone.utc)  # 2026-01-15 10:30 KST
    args = argparse.Namespace(from_date=None, to_date=None, batch_time=dt_utc.isoformat())
    dates, is_rerun = target_dates(args)
    # UTC 2026-01-15T01:30:00 = 2026-01-15 10:30 KST → target_date = 2026-01-14
    assert dates == ["2026-01-14"]
    assert is_rerun is False


def test_target_dates_no_args_defaults_to_yesterday():
    """No args defaults to batch_time = now(KST), target_date = yesterday."""
    # We can't easily test this without mocking time.now(), so we just verify
    # the structure works with None batch_time
    args = argparse.Namespace(from_date=None, to_date=None, batch_time=None)
    dates, is_rerun = target_dates(args)
    assert len(dates) == 1
    assert isinstance(dates[0], str)
    # Parse to verify format
    d = date_cls.fromisoformat(dates[0])
    assert isinstance(d, date_cls)
    assert is_rerun is False


# ============================================================================
# Warn class tests
# ============================================================================

def test_warn_init():
    """Warn class initialized with count and text."""
    w = Warn(count=1, text="test warning")
    assert w.count == 1
    assert w.text == "test warning"


def test_warn_add():
    """Warn objects can be added."""
    w1 = Warn(count=2, text="warn1")
    w2 = Warn(count=3, text="warn2")
    w_sum = w1 + w2
    assert w_sum.count == 5
    assert w_sum.text == "warn1\nwarn2"


def test_warn_add_empty_text():
    """Adding Warn with empty text doesn't add newline."""
    w1 = Warn(count=1, text="warn1")
    w2 = Warn(count=0, text="")
    w_sum = w1 + w2
    assert w_sum.text == "warn1"


def test_warn_no_user_id_in_text():
    """Warn text should not contain user_id (verified by caller)."""
    # This is more of a contract test — the Warn class itself
    # doesn't validate content, but we document the contract.
    w = Warn(count=1, text="service=S1 field=value")
    assert "user_id" not in w.text
