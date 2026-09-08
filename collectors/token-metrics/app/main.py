"""수집 오케스트레이터 — collectors/token-usage/app/main.py 의 클론 (설계 2026-08-31 §5.1).

정책(§5.2 모드×게이트 표 · 409 큐 끝 재방문 1회 · 최종 슬롯 판정 · §5.4 적재 예산 가드 · 마커)은
이 파일에 1벌만 존재한다. api_client 는 HTTP→Event 번역, normalize 는 순수 함수, writer 는 적재 시퀀스만.

모드(RunContext.mode):
  regular — target_date = KST 오늘−1. api_since/until 게이트·최종 슬롯 판정(batch_time.hour >= FINAL_HOUR_KST)
            ·레지스트리 동기화·manual_row_present WARN 은 이 모드에만. 앵커 존재 = SKIPPED already_loaded(뮤테이션 0).
  rerun   — --from/--to. 게이트·final 무시(409 재차 = FAILURE not_ready, 404 = SKIPPED retention).
            --replace 없으면 앵커 존재 = already_loaded. 날짜당 replace_batch 1회(§5.4 배칭 (A)→(B)(C)).
  manual  — T7 이 추가(CSV → MetricsPayload, source_type manual-v0). rerun 과 같은 정책, 동기화 없음.

로깅 계약(§3 전제 11·마스터 §5.6): 어떤 로그에도 페이로드·행 원문을 남기지 않는다 — 카운트·서비스명·코드만.
마커: SERVICE_RESULT(서비스당 1줄) / BATCH_RESULT(실행당 1줄, slot=HH final=0|1) / CHECK WARN(코드=카운트).
"""
from __future__ import annotations

import argparse
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import date as date_cls, datetime, timedelta, timezone

import requests

from app import api_client
from app.config import Config, ServiceEntry, load_config, load_endpoints
from app.events import CollectError, Event
from app.normalize import (SOURCE_API, SOURCE_MANUAL, MetricsPayload, NormalizeResult, PayloadError,
                           normalize_payload)
from app.writer import MetricsWriter, MutationBudgetExceeded
from app.manual import COUNT_KEYS, ManualCsvError, date_range, load_manual_csvs

KST = timezone(timedelta(hours=9))
MODULE = "token-metrics"
MODE_REGULAR = "regular"
MODE_RERUN = "rerun"
MODE_MANUAL = "manual"
MANUAL_INPUT_PREFIX = "MANUAL_INPUT module=token-metrics"   # §5.5 수기 입력 정보 마커(실행당 1줄, 카운트만)
NOT_READY_REVISIT_CAP_S = 300            # §5.2 409: 큐 끝 1회 재방문, 대기 = min(Retry-After, 300)s
REASON_DEADLINE = "deadline"
REASON_LOAD_BUDGET = "load_budget"
REASON_MUTATION_BUDGET = "mutation_budget"

_batch_status = {"line": f"BATCH_RESULT status=FAILURE module={MODULE} services_ok=0 services_failed=0 "
                         "services_skipped=0 rows=0 elapsed=0s slot=-- final=0"}


def _sigterm_handler(signum, frame):
    print(_batch_status["line"] + " note=sigterm", flush=True)     # 마커 보장 (§5.2 SIGTERM 캐시 줄 재출력)
    sys.exit(1)


@dataclass
class RunContext:
    mode: str                              # MODE_REGULAR | MODE_RERUN | MODE_MANUAL
    replace: bool                          # --replace (rerun·manual) — 앵커 존재 시 교체 허용
    batch_time: datetime                   # aware KST
    slot: str = ""                         # batch_time.strftime("%H") — BATCH_RESULT slot=HH
    final: bool = False                    # 정기 & batch_time.hour >= FINAL_HOUR_KST (make_context 가 계산)
    source_type: str = SOURCE_API          # 모든 ServiceOutcome.source_type 에 복사 (manual 은 SOURCE_MANUAL)

    def __post_init__(self) -> None:
        if not self.slot:
            self.slot = self.batch_time.strftime("%H")


def make_context(cfg: Config, mode: str, batch_time: datetime, replace: bool = False,
                 source_type: str = SOURCE_API) -> RunContext:
    """최종 슬롯 판정은 정기 실행에만 (§5.2 '실행 모드 × 게이트') — rerun·manual 은 항상 final=0."""
    final = mode == MODE_REGULAR and batch_time.hour >= cfg.final_hour_kst
    return RunContext(mode=mode, replace=replace, batch_time=batch_time, final=final, source_type=source_type)


@dataclass
class ServiceOutcome:
    service: str
    status: str = "FAILURE"                # SUCCESS | NODATA | SKIPPED | FAILURE
    source_type: str = SOURCE_API
    rows: int = 0                          # gpu + serving + custom (normalize 통과 행)
    warn: int = 0                          # NormalizeResult.warn_total (행 플래그 + 응답 WARN)
    rejected: int = 0
    reason: str = ""
    checks: dict[str, int] = field(default_factory=dict)   # CHECK WARN <code>=<count> (SERVICE_RESULT 직전 출력)


@dataclass
class _QueueItem:
    entry: ServiceEntry
    resume_at: float = 0.0
    revisited: bool = False                # 409 큐 끝 재방문은 1회 — 재차 409 는 최종 판정


def _service_line(o: ServiceOutcome) -> str:
    return (f"SERVICE_RESULT status={o.status} module={MODULE} service={o.service} "
            f"source_type={o.source_type} rows={o.rows} pages=1 warn={o.warn} rejected={o.rejected}"
            + (f" reason={o.reason}" if o.reason else ""))


def _check_lines(service: str, checks: dict[str, int]) -> list[str]:
    """인라인 검증 마커 — 코드·카운트만 (페이로드 없음). 0 인 코드는 출력하지 않는다."""
    return [f"CHECK WARN service={service} {code}={n}" for code, n in sorted(checks.items()) if n]


def _batch_reason(outcomes: list[ServiceOutcome]) -> str:
    """§4.0 뮤테이션 가드 — 어느 서비스든 mutation_budget 이면 배치 reason 으로 승격 (exit 1 과 함께 알림 근거)."""
    return REASON_MUTATION_BUDGET if any(o.reason == REASON_MUTATION_BUDGET for o in outcomes) else ""


def _batch_line(outcomes: list[ServiceOutcome], started: float, clock, ctx: RunContext,
                reason: str = "") -> str:
    ok = sum(1 for o in outcomes if o.status in ("SUCCESS", "NODATA"))
    failed = sum(1 for o in outcomes if o.status == "FAILURE")
    skipped = sum(1 for o in outcomes if o.status == "SKIPPED")
    total_rows = sum(o.rows for o in outcomes)
    if failed:
        status = "FAILURE"
    elif outcomes and all(o.status == "NODATA" for o in outcomes):
        status = "NODATA"
    else:
        status = "SUCCESS"                 # 전부 SKIPPED(게이트·already_loaded)도 SUCCESS — 뮤테이션 0 정상 종료
    return (f"BATCH_RESULT status={status} module={MODULE} services_ok={ok} "
            f"services_failed={failed} services_skipped={skipped} rows={total_rows} "
            f"elapsed={int(clock() - started)}s slot={ctx.slot} final={int(ctx.final)}"
            + (f" reason={reason}" if reason else ""))


def _gate(entry: ServiceEntry, target_date: str, ctx: RunContext) -> str | None:
    """§5.2 게이트 — disabled 는 모든 모드, api_since/until 은 정기 실행에만. 반환 = SKIPPED reason 또는 None."""
    if not entry.enabled:
        return "disabled"
    if ctx.mode != MODE_REGULAR:
        return None
    target = date_cls.fromisoformat(target_date)
    if target < entry.api_since:
        return "before_since"
    if entry.until is not None and target > entry.until:
        return "after_until"
    return None


def _outcome_from_error(entry: ServiceEntry, err: CollectError, ctx: RunContext,
                        revisited: bool) -> ServiceOutcome | None:
    """CollectError → ServiceOutcome. None = 409 첫 방문: 호출자가 큐 끝에 재삽입한다 (§5.2 409 행)."""
    st = ctx.source_type
    if err.event is Event.NOT_READY:
        if not revisited:
            return None
        if ctx.mode == MODE_REGULAR:
            if ctx.final:                  # 최종 슬롯 재차 409 → exit 1 → BATCH FAILURE = 스펙 09:00 알림
                return ServiceOutcome(service=entry.service, source_type=st, reason="not_ready_at_0900")
            return ServiceOutcome(service=entry.service, status="SKIPPED", source_type=st, reason="not_ready")
        return ServiceOutcome(service=entry.service, source_type=st, reason="not_ready")
    if err.event is Event.RETENTION:
        if ctx.mode == MODE_REGULAR:
            return ServiceOutcome(service=entry.service, source_type=st, reason="retention")
        return ServiceOutcome(service=entry.service, status="SKIPPED", source_type=st, reason="retention")
    return ServiceOutcome(service=entry.service, source_type=st, reason=err.event.value)


_sessions: dict = {}


def _session(cfg: Config):
    """프록시/CA 의미는 기존 모듈과 동일 (§5.2 프록시/CA 3종): None=상속, ''=직접 연결, 값=전용 프록시."""
    key = (cfg.https_proxy, str(cfg.api_verify))
    if key not in _sessions:
        sess = requests.Session()
        if cfg.https_proxy is not None:
            sess.proxies = {"http": cfg.https_proxy or None,
                            "https": cfg.https_proxy or None}
            sess.trust_env = bool(cfg.https_proxy)
        sess.verify = cfg.api_verify
        _sessions[key] = sess
    return _sessions[key]


def _parse_batch_time(raw: str | None) -> datetime:
    """naive 입력은 KST 로 해석(호스트 TZ 무관), aware 는 KST 로 변환 — 슬롯(HH)·final 판정의 기준."""
    if not raw:
        return datetime.now(KST)
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=KST)
    return parsed.astimezone(KST)


def _target_dates(args, batch_time: datetime) -> tuple[list[str], str]:
    """--from/--to → ([D0..D1], rerun) / 없으면 ([batch_time.date() − 1], regular). 위반은 ValueError (main 이 exit 2)."""
    if args.from_date or args.to_date:
        if not (args.from_date and args.to_date):
            raise ValueError("--from/--to must be given together (KST, YYYY-MM-DD)")
        d0 = date_cls.fromisoformat(args.from_date)
        d1 = date_cls.fromisoformat(args.to_date)
        if d0 > d1:
            raise ValueError("--from must not be after --to")
        return [str(d0 + timedelta(days=i)) for i in range((d1 - d0).days + 1)], MODE_RERUN
    return [str(batch_time.date() - timedelta(days=1))], MODE_REGULAR


def _prepare_one(cfg: Config, entry: ServiceEntry, target_date: str, ctx: RunContext, writer, fetcher, session):
    """게이트 → 앵커(already_loaded·manual_row_present) → fetch → normalize. 반환 = ServiceOutcome(SKIPPED) 또는
    적재 대기 튜플 (entry, payload, result). CollectError 는 호출자(run_collection)가 §5.2 표로 판정."""
    gate = _gate(entry, target_date, ctx)
    if gate is not None:
        return ServiceOutcome(service=entry.service, status="SKIPPED", source_type=ctx.source_type, reason=gate)
    if not ctx.replace and writer.anchor_exists(target_date, entry.service):
        o = ServiceOutcome(service=entry.service, status="SKIPPED", source_type=ctx.source_type,
                           reason="already_loaded")
        if ctx.mode == MODE_REGULAR and writer.anchor_source_type(target_date, entry.service) == SOURCE_MANUAL:
            o.checks["manual_row_present"] = 1      # 정기 실행이 수동 앵커를 만남 — 덮어쓰지 않고 WARN 만 (§5.2)
        return o
    payload: MetricsPayload = fetcher(entry, target_date, cfg, session)
    try:
        result: NormalizeResult = normalize_payload(payload, entry)
    except PayloadError as exc:                     # 구조 위반 = 4xx 와 같은 급 (재시도 무의미)
        raise CollectError(Event.PERMANENT_ERROR, f"report structure: {exc}") from exc
    return entry, payload, result


def _load_items(cfg: Config, target_date: str, items: list, writer, clock, deadline: float,
                ctx: RunContext) -> list[ServiceOutcome]:
    """§5.2 마지막 행 + §5.4: 적재 착수 전 잔여 시간 < LOAD_BUDGET_S 면 착수하지 않고 FAILURE load_budget.
    replace_batch 1회(정기 = item 1개, rerun/manual = 날짜당 N개) → SUCCESS/NODATA. MutationBudgetExceeded → mutation_budget."""
    st = ctx.source_type
    if deadline - clock() < cfg.load_budget_s:
        return [ServiceOutcome(service=e.service, source_type=st, reason=REASON_LOAD_BUDGET) for e, _, _ in items]
    try:
        writer.replace_batch(target_date, items)
    except MutationBudgetExceeded:
        return [ServiceOutcome(service=e.service, source_type=st, reason=REASON_MUTATION_BUDGET)
                for e, _, _ in items]
    outs: list[ServiceOutcome] = []
    for e, _, r in items:
        o = ServiceOutcome(service=e.service, status="NODATA" if r.is_nodata else "SUCCESS", source_type=st,
                           rows=r.rows, warn=r.warn_total, rejected=r.rejected, checks=dict(r.warns))
        if o.rows == 0 and o.rejected > 0:
            o.checks["all_rows_rejected"] = 1       # 앵커는 적재됐지만 행이 전부 거부됨 — 마트 단위 WARN
        outs.append(o)
    return outs


def run_collection(cfg: Config, entries: list[ServiceEntry], target_date: str, ctx: RunContext, *,
                   clock=time.monotonic, sleeper=time.sleep, fetcher=api_client.fetch_metrics,
                   writer=None, session=None, register_dims: bool = True, dim_entries=None,
                   emit_batch: bool = True, outcomes_sink=None, started: float | None = None) -> int:
    """날짜 1개 수집. 반환 = exit code(0 | 1 = FAILURE 1개 이상).
    큐 루프: 데드라인/적재 예산 검사 → 준비된 항목(resume_at ≤ now) → _prepare_one → 409 첫 방문은 큐 끝 재삽입.
    정기 = 서비스별 즉시 적재, rerun/manual = 큐 소진 후 날짜당 replace_batch 1회 (§5.4 (A)→(B)(C)).
    outcomes_sink 가 주어지면 그 누적 목록으로 SIGTERM 캐시 줄을 갱신하고(_run_dates 집계), emit_batch=False 면
    BATCH_RESULT 를 출력하지 않는다(집계 줄은 _run_dates 가 1회 출력)."""
    started = clock() if started is None else started
    deadline = started + cfg.soft_deadline_minutes * 60
    writer = writer if writer is not None else MetricsWriter(cfg)
    session = session if session is not None else _session(cfg)
    if register_dims and ctx.mode == MODE_REGULAR:
        try:
            writer.sync_registry(dim_entries if dim_entries is not None else entries)
        except Exception as exc:                    # 레지스트리 동기화 실패는 수집을 막지 않는다 — WARN 마커만
            print("CHECK WARN service=- registry_sync_failed=1", flush=True)
            print(f"registry sync failed: {type(exc).__name__}", file=sys.stderr)

    queue = [_QueueItem(entry=e) for e in entries]
    outcomes: list[ServiceOutcome] = []
    pending: list = []                              # rerun/manual: 적재 대기 (entry, payload, result)
    scope = outcomes_sink if outcomes_sink is not None else outcomes

    def _record(o: ServiceOutcome) -> None:
        outcomes.append(o)
        if outcomes_sink is not None:
            outcomes_sink.append(o)
        for line in _check_lines(o.service, o.checks):
            print(line, flush=True)
        print(_service_line(o), flush=True)
        _batch_status["line"] = _batch_line(scope, started, clock, ctx, reason=_batch_reason(scope))

    def _load(items: list) -> None:
        try:
            for o in _load_items(cfg, target_date, items, writer, clock, deadline, ctx):
                _record(o)
        except Exception as exc:                    # CH 연결 오류 등 — 항목 단위 격리, 마커 보장
            for e, _, _ in items:
                _record(ServiceOutcome(service=e.service, source_type=ctx.source_type,
                                       reason=f"unexpected:{type(exc).__name__}"))

    while queue:
        now = clock()
        if deadline - now < cfg.load_budget_s:      # 신규 착수 창 종료 — 남은 큐는 fetch 없이 FAILURE deadline
            for item in queue:
                _record(ServiceOutcome(service=item.entry.service, source_type=ctx.source_type,
                                       reason=REASON_DEADLINE))
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
            prepared = _prepare_one(cfg, item.entry, target_date, ctx, writer, fetcher, session)
        except CollectError as err:
            o = _outcome_from_error(item.entry, err, ctx, item.revisited)
            if o is None:                           # 409 첫 방문 → 큐 끝 재삽입 1회, 대기 = min(max(Retry-After,1),300)
                item.resume_at = clock() + min(max(err.retry_after_s, 1), NOT_READY_REVISIT_CAP_S)
                item.revisited = True
                queue.append(item)
            else:
                _record(o)
            continue
        except Exception as exc:                    # 예상 밖 예외 — 서비스 단위 격리
            _record(ServiceOutcome(service=item.entry.service, source_type=ctx.source_type,
                                   reason=f"unexpected:{type(exc).__name__}"))
            continue
        if isinstance(prepared, ServiceOutcome):
            _record(prepared)
        elif ctx.mode == MODE_REGULAR:
            _load([prepared])
        else:
            pending.append(prepared)

    if pending:
        _load(pending)

    line = _batch_line(scope, started, clock, ctx, reason=_batch_reason(scope))
    _batch_status["line"] = line                    # SIGTERM 신선도 — emit_batch와 무관하게 갱신
    if emit_batch:
        print(line, flush=True)
    return 1 if any(o.status == "FAILURE" for o in outcomes) else 0


def _run_dates(cfg: Config, entries: list[ServiceEntry], dim_entries: list[ServiceEntry], dates: list[str],
               ctx: RunContext, fetcher, *, writer=None, clock=time.monotonic, sleeper=time.sleep,
               started: float | None = None, register_dims: bool = True) -> int:
    """날짜 N개(정기 = 1개) → BATCH_RESULT 1줄. writer 1개 공유(뮤테이션 장부 누적), started 1개(소프트 데드라인은
    실행 전체), 레지스트리 동기화는 첫 날짜에서만. 반환 = 날짜별 exit code 의 최댓값."""
    started = clock() if started is None else started
    writer = writer if writer is not None else MetricsWriter(cfg)
    all_outcomes: list[ServiceOutcome] = []
    worst = 0
    for i, d in enumerate(dates):
        code = run_collection(cfg, entries, d, ctx, clock=clock, sleeper=sleeper, fetcher=fetcher,
                              writer=writer, register_dims=(register_dims and i == 0), dim_entries=dim_entries,
                              emit_batch=False, outcomes_sink=all_outcomes, started=started)
        worst = max(worst, code)
    line = _batch_line(all_outcomes, started, clock, ctx, reason=_batch_reason(all_outcomes))
    _batch_status["line"] = line
    print(line, flush=True)
    return worst


def _add_manual_args(parser: argparse.ArgumentParser) -> None:
    """manual-v0 (§5.5) 전용 인자 4개 — 정기·rerun 인자(batch_time/--from/--to/--service/--replace)는 T6 그대로."""
    parser.add_argument("--manual-gpu", dest="manual_gpu", default=None,
                        help="manual-v0 gpu CSV (§5.5) — --manual-serving 과 쌍, --from/--to 필수")
    parser.add_argument("--manual-serving", dest="manual_serving", default=None,
                        help="manual-v0 serving CSV")
    parser.add_argument("--manual-engine", dest="manual_engine", default=None,
                        help="manual-v0 engine CSV (선택)")
    parser.add_argument("--generated-at", dest="generated_at", default=None,
                        help="manual-v0 generated_at ISO8601 (권장 +09:00; 없으면 적재 시각)")


def _manual_args_error(args: argparse.Namespace) -> str:
    """manual 인자 조합 검증 — 오류 메시지 또는 빈 문자열. 설정 로드·DB 접근 전에 호출된다."""
    if bool(args.manual_gpu) != bool(args.manual_serving):
        return "--manual-gpu/--manual-serving must be given together"
    if args.manual_gpu and not (args.from_date and args.to_date):
        return "manual mode requires --from/--to (KST, YYYY-MM-DD)"
    if (args.manual_engine or args.generated_at) and not args.manual_gpu:
        return "--manual-engine/--generated-at require --manual-gpu/--manual-serving"
    return ""


def _run_manual(cfg: Config, args, entries: list[ServiceEntry], all_entries: list[ServiceEntry],
                started: float, clock=time.monotonic) -> int:
    """manual-v0 (§5.5): CSV 3파일 → (date, service) MetricsPayload → API 와 동일한 normalize/replace 경로.

    - 레지스트리 동기화 없음(register_dims=False — 정기 실행 전용 §4.3), api_since/until 게이트 없음(MODE_MANUAL),
      enabled=0 은 SKIPPED disabled(모든 모드), 앵커 있으면 --replace 없이는 SKIPPED already_loaded(_prepare_one).
    - 페이로드가 있는 (date, service) 만 대상 — 행 없는 (date, service) 는 fetch 하지 않고 앵커도 남기지 않는다
      (6c metrics_missing). 날짜마다 대상 서비스 집합이 달라 _run_dates(고정 entries) 대신 날짜별 run_collection 을
      직접 돌리되 writer·started·outcomes 를 공유해 뮤테이션 장부·소프트 데드라인·BATCH_RESULT 1줄은 _run_dates 와 같다.
    - 뮤테이션 예산 가드·날짜당 replace_batch 1회 배칭·mutation_budget 승격은 run_collection/writer 공통.
    - 파일 계약 위반(ManualCsvError)·날짜 인자 오류·파일 없음은 적재 없이 stderr + exit 2.
    """
    try:
        dates = date_range(args.from_date, args.to_date)
        payloads, counts = load_manual_csvs(
            args.manual_gpu, args.manual_serving, args.manual_engine,
            args.from_date, args.to_date, all_entries, args.service, args.generated_at or "")
    except (ManualCsvError, ValueError, OSError) as exc:
        print(f"manual input error: {exc}", file=sys.stderr)
        return 2
    print(f"{MANUAL_INPUT_PREFIX} " + " ".join(f"{k}={counts[k]}" for k in COUNT_KEYS), flush=True)
    ctx = make_context(cfg, MODE_MANUAL, datetime.now(KST), replace=args.replace, source_type=SOURCE_MANUAL)

    def fetcher(entry: ServiceEntry, target_date: str, _cfg: Config, _session) -> MetricsPayload:
        return payloads[(target_date, entry.service)]                # 대상은 키가 있는 (date, service) 로만 좁힌다

    writer = MetricsWriter(cfg)                                      # 날짜 전체가 1개 writer 공유(뮤테이션 장부 누적)
    all_outcomes: list[ServiceOutcome] = []
    worst = 0
    for d in dates:
        targets = [e for e in entries if (d, e.service) in payloads]  # --service 필터 후 entries 기준(disabled 포함 → gate)
        if not targets:                                              # 그날 행 없음 → fetch·적재·앵커 없음
            continue
        code = run_collection(cfg, targets, d, ctx, clock=clock, fetcher=fetcher, writer=writer,
                              register_dims=False, emit_batch=False, outcomes_sink=all_outcomes, started=started)
        worst = max(worst, code)
    line = _batch_line(all_outcomes, started, clock, ctx, reason=_batch_reason(all_outcomes))
    _batch_status["line"] = line
    print(line, flush=True)
    return worst


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="token-metrics-collector")
    parser.add_argument("batch_time", nargs="?", default=None,
                        help="ISO8601 (기본 now, KST) — 정기 target_date = batch_time − 1일, slot=HH·final 판정 기준")
    parser.add_argument("--from", dest="from_date", default=None, help="rerun 시작일 (KST, YYYY-MM-DD) — --to 와 쌍")
    parser.add_argument("--to", dest="to_date", default=None, help="rerun 종료일 (포함)")
    parser.add_argument("--service", default=None, help="단일 서비스만 (레지스트리 동기화는 전체 기준)")
    parser.add_argument("--replace", action="store_true",
                        help="앵커가 있어도 교체 (rerun 전용 — 정기 실행은 뮤테이션 0 보장)")
    _add_manual_args(parser)
    args = parser.parse_args(argv)
    manual_err = _manual_args_error(args)
    if manual_err:
        print(manual_err, file=sys.stderr)
        return 2

    signal.signal(signal.SIGTERM, _sigterm_handler)
    try:
        cfg = load_config()
        all_entries = load_endpoints(cfg.endpoints_file)
    except Exception as exc:                        # env 불변식·파일 부재·YAML/스키마 오류 — 적재 전 종료
        print(f"config error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    entries = all_entries
    if args.service:
        entries = [e for e in all_entries if e.service == args.service]
        if not entries:
            print(f"unknown service: {args.service}", file=sys.stderr)
            return 2
    if args.manual_gpu:
        return _run_manual(cfg, args, entries, all_entries, started=time.monotonic())
    try:
        batch_time = _parse_batch_time(args.batch_time)
        dates, mode = _target_dates(args, batch_time)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.replace and mode != MODE_RERUN:
        print("--replace requires --from/--to (regular run keeps mutations at 0)", file=sys.stderr)
        return 2
    ctx = make_context(cfg, mode, batch_time, replace=args.replace)
    return _run_dates(cfg, entries, all_entries, dates, ctx, api_client.fetch_metrics)


if __name__ == "__main__":
    sys.exit(main())
