"""수집 오케스트레이터 — 정책(§5.2 분류→정책 표)은 이 파일에 1벌만 존재한다.

로깅 계약(§5.6): 어떤 로그에도 레코드 페이로드·user_id 원문을 남기지 않는다.
"""
import argparse
import signal
import sys
import time
from dataclasses import dataclass
from datetime import date as date_cls, datetime, timedelta, timezone

import requests

from app import api_client, vm_push
from app.clickhouse_client import CHWriter
from app.config import Config, ServiceEntry, load_config, load_endpoints
from app.events import CollectError, Event
from app.normalize import check_identity, check_summary, normalize_records

KST = timezone(timedelta(hours=9))
LOAD_BUDGET_S = 12 * 60          # 적재 시퀀스 예산 (§5.2 v1.4)
INVARIANT_RESTARTS = 2

_batch_status = {"line": "BATCH_RESULT status=FAILURE module=token-usage "
                         "services_ok=0 services_failed=0 services_skipped=0 rows=0 elapsed=0s"}


def _sigterm_handler(signum, frame):
    print(_batch_status["line"] + " note=sigterm", flush=True)     # §5.1-4 마커 보장
    sys.exit(1)


@dataclass
class ServiceOutcome:
    service: str
    status: str = "FAILURE"
    rows: int = 0
    pages: int = 0
    warns: int = 0
    rejected: int = 0
    reason: str = ""


@dataclass
class _QueueItem:
    entry: ServiceEntry
    resume_at: float = 0.0
    waited_s: float = 0.0
    restarts: int = 0


def _service_line(o: ServiceOutcome) -> str:
    return (f"SERVICE_RESULT status={o.status} module=token-usage service={o.service} "
            f"source_type=usage-api-v1 rows={o.rows} pages={o.pages} "
            f"warn={o.warns} rejected={o.rejected}"
            + (f" reason={o.reason}" if o.reason else ""))


def _batch_line(outcomes: list[ServiceOutcome], started: float, clock) -> str:
    ok = sum(1 for o in outcomes if o.status in ("SUCCESS", "NODATA"))
    failed = sum(1 for o in outcomes if o.status == "FAILURE")
    skipped = sum(1 for o in outcomes if o.status == "SKIPPED")
    total_rows = sum(o.rows for o in outcomes)
    if failed:
        status = "FAILURE"
    elif outcomes and all(o.status == "NODATA" for o in outcomes):
        status = "NODATA"
    else:
        status = "SUCCESS"
    return (f"BATCH_RESULT status={status} module=token-usage services_ok={ok} "
            f"services_failed={failed} services_skipped={skipped} rows={total_rows} "
            f"elapsed={int(clock() - started)}s")


def _collect_one(cfg: Config, entry: ServiceEntry, target_date: str,
                 fetcher, writer, pusher, is_rerun: bool) -> ServiceOutcome:
    o = ServiceOutcome(service=entry.service)
    payload = fetcher(entry, target_date, cfg, _session(cfg))
    o.pages = payload.pages
    norm = normalize_records(payload.records)
    o.rejected = norm.rejected
    warns = list(norm.warns)
    warns += check_identity(entry, payload)
    warns += check_summary(norm.totals, payload.summary or {})
    o.warns = len(warns)
    for w in warns:
        print(f"CHECK WARN service={entry.service} {w}", flush=True)

    generated_at, gen_warn = _kst_naive(payload.generated_at)
    if gen_warn:
        o.warns += 1
        print(f"CHECK WARN service={entry.service} {gen_warn}", flush=True)

    s = payload.summary or {}
    summary_row = {
        "reported_service_group": payload.reported_service_group,
        "reported_service": payload.reported_service,
        "input_tokens": int(s.get("inputTokens", 0) or 0),
        "cache_read_tokens": int(s.get("cacheReadTokens", 0) or 0),
        "cache_creation_tokens": int(s.get("cacheCreationTokens", 0) or 0),
        "output_tokens": int(s.get("outputTokens", 0) or 0),
        "requests": int(s.get("requests", 0) or 0),
        "distinct_users": int(s.get("distinctUsers", 0) or 0),
        "distinct_identified_users": s.get("distinctIdentifiedUsers"),
        "is_derived": 0,
        "generated_at": generated_at,
    }
    audit_prev = writer.fetch_prev_summary(entry.service, target_date)
    o.rows = writer.replace_service_day(entry, target_date, iter(norm.rows),
                                        summary_row, audit_prev)
    if not is_rerun:
        for w in pusher(cfg, entry, target_date, {**s, "is_derived": 0}, _session(cfg)):
            o.warns += 1
            print(f"CHECK WARN service={entry.service} {w}", flush=True)
    o.status = "NODATA" if o.rows == 0 else "SUCCESS"   # EMPTY → NODATA (§5.2)
    return o


def _kst_naive(iso_str: str) -> tuple[datetime, str | None]:
    try:
        return datetime.fromisoformat(iso_str).astimezone(KST).replace(tzinfo=None), None
    except ValueError:
        kind = type(iso_str).__name__ if not isinstance(iso_str, str) else "unparseable_str"
        return (datetime.now(KST).replace(tzinfo=None),
                f"generated_at_parse_failed: {kind}")   # 원문 미포함 (§5.6 로깅 계약)


_sessions: dict = {}


def _session(cfg: Config):
    key = (cfg.https_proxy, str(cfg.api_verify))
    if key not in _sessions:
        sess = requests.Session()
        if cfg.https_proxy is not None:                  # ''=직접, 값=전용 (§5.7)
            sess.proxies = {"http": cfg.https_proxy or None,
                            "https": cfg.https_proxy or None}
            sess.trust_env = bool(cfg.https_proxy)
        sess.verify = cfg.api_verify
        _sessions[key] = sess
    return _sessions[key]


def run_collection(cfg: Config, entries: list[ServiceEntry], target_date: str, *,
                   is_rerun: bool = False, clock=time.monotonic, sleeper=time.sleep,
                   fetcher=api_client.fetch_service, writer=None,
                   pusher=vm_push.push_service_summary,
                   register_dims: bool = True) -> int:
    started = clock()
    deadline = started + cfg.soft_deadline_minutes * 60
    writer = writer or CHWriter(cfg)
    if register_dims:
        writer.replace_dim_services([e for e in entries])   # 레지스트리 반영 (§5.1-2) — CLI 호출당 1회

    queue = [_QueueItem(entry=e) for e in entries if e.enabled]
    outcomes: list[ServiceOutcome] = []

    def _record(outcomes: list[ServiceOutcome], o: ServiceOutcome) -> None:
        outcomes.append(o)
        print(_service_line(o), flush=True)                 # 완료 즉시 출력 (SIGTERM 마커 신선도)
        _batch_status["line"] = _batch_line(outcomes, started, clock)

    while queue:
        now = clock()
        if now >= deadline or deadline - now < LOAD_BUDGET_S:
            for item in queue:                            # 잔여 전부 FAILURE, 정상 종료 (§5.2)
                _record(outcomes, ServiceOutcome(service=item.entry.service,
                                                 reason="deadline"))
            queue.clear()
            break
        ready = [q for q in queue if q.resume_at <= now]
        if not ready:
            wake = min(q.resume_at for q in queue)
            sleeper(min(wake - now, deadline - now))
            continue
        item = ready[0]
        queue.remove(item)
        try:
            _record(outcomes, _collect_one(cfg, item.entry, target_date,
                                           fetcher, writer, pusher, is_rerun))
        except CollectError as err:
            if err.event is Event.NOT_READY:
                item.waited_s += max(err.retry_after_s, 1)
                if item.waited_s > cfg.not_ready_budget_minutes * 60:
                    _record(outcomes, ServiceOutcome(service=item.entry.service,
                                                     reason="not_ready_budget"))
                else:
                    item.resume_at = clock() + max(err.retry_after_s, 1)
                    queue.append(item)                    # 큐 끝 재삽입 — 전체 재시작 (§5.2)
                    continue
            elif err.event is Event.INVARIANT_BROKEN and item.restarts < INVARIANT_RESTARTS:
                item.restarts += 1
                queue.append(item)                        # 폐기 후 재시작 ≤2회 (§5.3)
                continue
            elif err.event is Event.RETENTION and is_rerun:
                _record(outcomes, ServiceOutcome(service=item.entry.service,
                                                 status="SKIPPED", reason="retention"))
            else:
                _record(outcomes, ServiceOutcome(service=item.entry.service,
                                                 reason=err.event.value))
        except Exception as exc:                          # 예상 밖 — 서비스 격리 유지
            _record(outcomes, ServiceOutcome(service=item.entry.service,
                                             reason=f"unexpected:{type(exc).__name__}"))

    line = _batch_line(outcomes, started, clock)
    _batch_status["line"] = line
    print(line, flush=True)
    failed = sum(1 for o in outcomes if o.status == "FAILURE")
    return 1 if failed else 0


def _target_dates(args) -> tuple[list[str] | None, bool]:
    if args.from_date or args.to_date:
        if not (args.from_date and args.to_date):
            print("--from/--to는 쌍으로 지정 (KST, YYYY-MM-DD)", file=sys.stderr)
            return None, False
        d0 = date_cls.fromisoformat(args.from_date)
        d1 = date_cls.fromisoformat(args.to_date)
        return [str(d0 + timedelta(days=i)) for i in range((d1 - d0).days + 1)], True
    if args.batch_time:
        parsed = datetime.fromisoformat(args.batch_time)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=KST)      # naive 입력은 KST로 해석 (§5.1)
        batch_time = parsed.astimezone(KST)
    else:
        batch_time = datetime.now(KST)
    return [str(batch_time.date() - timedelta(days=1))], False


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_time", nargs="?", default=None,
                        help="ISO8601 (기본 now, KST) — target_date = batch_time - 1일")
    parser.add_argument("--from", dest="from_date", default=None)
    parser.add_argument("--to", dest="to_date", default=None)
    parser.add_argument("--service", default=None, help="단일 서비스만 (재수집용)")
    args = parser.parse_args(argv)

    signal.signal(signal.SIGTERM, _sigterm_handler)
    cfg = load_config()
    entries = load_endpoints(cfg.endpoints_file)
    if args.service:
        entries = [e for e in entries if e.service == args.service]
        if not entries:
            print(f"unknown service: {args.service}", file=sys.stderr)
            return 2
    dates, is_rerun = _target_dates(args)
    if dates is None:
        return 2
    worst = 0
    for i, d in enumerate(dates):
        worst = max(worst, run_collection(cfg, entries, d, is_rerun=is_rerun,
                                          register_dims=(i == 0)))
    return worst


if __name__ == "__main__":
    sys.exit(main())
