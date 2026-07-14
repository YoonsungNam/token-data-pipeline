"""순수 로직 — coverage 게이트, 마커, 날짜 윈도우.

I/O 금지 (클라이언트, 네트워크, 시계 부작용 없음). §5.6 로깅 계약: user_id 원문 미포함.
"""
import sys
from dataclasses import dataclass
from datetime import date as date_cls, datetime, timedelta, timezone


KST = timezone(timedelta(hours=9))


@dataclass
class Coverage:
    """Coverage state: enabled count, present count, missing services, warn targets."""
    enabled: int          # 활성화된 서비스 수
    present: int          # summary에 있는 enabled 서비스 수
    missing: list[str]    # enabled 중 summary에 없는 서비스 (정렬)
    warn_targets: list[str]  # missing 중 expected_late에 없는 것 (정렬)


@dataclass
class Warn:
    """Warning aggregator: count + text (no user_id in text per §5.6)."""
    count: int
    text: str

    def __add__(self, other: "Warn") -> "Warn":
        """Combine two Warn objects."""
        if not self.text:
            combined_text = other.text
        elif not other.text:
            combined_text = self.text
        else:
            combined_text = f"{self.text}\n{other.text}"
        return Warn(count=self.count + other.count, text=combined_text)


def compute_coverage(
    enabled_services: list[str],
    summary_services: set[str],
    expected_late: list[str],
) -> Coverage:
    """
    Compute coverage state.

    missing = enabled - summary (sorted)
    warn_targets = missing - expected_late (sorted)
    """
    enabled_set = set(enabled_services)
    missing_set = enabled_set - summary_services
    missing = sorted(missing_set)
    expected_late_set = set(expected_late)
    warn_targets = sorted(missing_set - expected_late_set)

    return Coverage(
        enabled=len(enabled_set),
        present=len(enabled_set & summary_services),
        missing=missing,
        warn_targets=warn_targets,
    )


def batch_line(
    status: str,
    coverage: Coverage,
    rows_mart: int,
    rows_view: int,
    warn_count: int,
    elapsed_s: float,
) -> str:
    """
    Format batch result marker line.

    Format (§5.6): BATCH_RESULT status=<S> module=mart-token coverage=N/M
    missing_services="..." rows_mart=<n> rows_view=<n> warn=<n> elapsed=<sec, 1 decimal>

    missing_services value is always double-quoted (to protect spaces in service names).
    Empty missing list renders as "-".
    """
    if coverage.missing:
        missing_str = ",".join(coverage.missing)
    else:
        missing_str = "-"

    # coverage=N/M where N = present (count in summary), M = enabled (total count)
    coverage_display = f"{coverage.present}/{coverage.enabled}"

    # Format elapsed to 1 decimal place
    elapsed_display = f"{elapsed_s:.1f}"

    return (
        f"BATCH_RESULT status={status} module=mart-token coverage={coverage_display} "
        f'missing_services="{missing_str}" rows_mart={rows_mart} rows_view={rows_view} '
        f"warn={warn_count} elapsed={elapsed_display}"
    )


def target_dates(args) -> tuple[list[str] | None, bool]:
    """
    Parse CLI args for target date(s).

    Returns (dates, is_rerun) where:
    - dates: list of YYYY-MM-DD strings (inclusive range), or None if args invalid
    - is_rerun: True if multi-date range (--from/--to), False otherwise

    Contract matches collectors' _target_dates:
    - --from/--to must be paired, YYYY-MM-DD, inclusive
    - naive datetime interpreted as KST
    - aware datetime converted to KST
    - default: batch_time = now(KST), target_date = yesterday
    """
    if args.from_date or args.to_date:
        # --from/--to must be paired
        if not (args.from_date and args.to_date):
            print("--from/--to는 쌍으로 지정 (KST, YYYY-MM-DD)", file=sys.stderr)
            return None, False

        d0 = date_cls.fromisoformat(args.from_date)
        d1 = date_cls.fromisoformat(args.to_date)
        # Inclusive range: (d1 - d0).days + 1
        dates = [str(d0 + timedelta(days=i)) for i in range((d1 - d0).days + 1)]
        return dates, True

    # Parse batch_time (default to now(KST))
    if args.batch_time:
        parsed = datetime.fromisoformat(args.batch_time)
        if parsed.tzinfo is None:
            # naive input is interpreted as KST (§5.1)
            parsed = parsed.replace(tzinfo=KST)
        batch_time = parsed.astimezone(KST)
    else:
        batch_time = datetime.now(KST)

    # target_date = batch_time - 1 day
    target_date = batch_time.date() - timedelta(days=1)
    return [str(target_date)], False
