"""main 오케스트레이터(§5.2 모드×게이트 · §5.4 예산 가드 · 마커 · SIGTERM) 테스트 — Fake writer/fetcher, DB·HTTP 없음.
공통 fixture 상수는 Plan 6b 전 태스크 공통(Mock Group / Mock Service A / 2026-09-10 / 2026-09-11T02:05+09:00)."""
import argparse
from datetime import date, datetime

import pytest

from app.config import Config, ServiceEntry
from app.events import CollectError, Event
from app.main import (KST, MODE_MANUAL, MODE_REGULAR, MODE_RERUN, MODULE, NOT_READY_REVISIT_CAP_S,
                      RunContext, ServiceOutcome, _batch_line, _batch_reason, _batch_status,
                      _check_lines, _gate, _outcome_from_error, _parse_batch_time, _service_line,
                      _sigterm_handler, _target_dates, make_context)

SERVICE_GROUP = "Mock Group"
SERVICE = "Mock Service A"
SERVICE_B = "Mock Service B"
SERVICE_C = "Mock Service C"
BASE_URL = "http://mock"
DATE = "2026-09-10"
GPU_TYPE = "H100"
ENGINE = {"type": "vllm", "version": "0.10.1"}
GENERATED_AT = "2026-09-11T02:05:00+09:00"
MODELS = ["claude-opus-4-8", "claude-sonnet-5", "claude-haiku-4-5"]


def entry(service=SERVICE, enabled=True, api_since="2026-09-09", until=None, base_url=BASE_URL) -> ServiceEntry:
    return ServiceEntry(service_group=SERVICE_GROUP, service=service, base_url=base_url, enabled=enabled,
                        api_since=date.fromisoformat(api_since), coverage_since=date(2026, 8, 26),
                        until=None if until is None else date.fromisoformat(until))


ENTRY = entry()
ENTRY_B = entry(SERVICE_B, base_url="http://mock-b")
ENTRY_C = entry(SERVICE_C, base_url="http://mock-c")


class Clock:
    def __init__(self, t=0.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s


def ctx(mode=MODE_REGULAR, hour=2, replace=False, cfg=None) -> RunContext:
    """batch_time = 2026-09-11 HH:05 KST — 정기 target_date 는 DATE(2026-09-10)."""
    return make_context(cfg or Config(), mode, datetime(2026, 9, 11, hour, 5, tzinfo=KST), replace=replace)


# ---------- 상수·RunContext ----------

def test_module_constants_and_initial_batch_line():
    assert MODULE == "token-metrics"
    assert (MODE_REGULAR, MODE_RERUN, MODE_MANUAL) == ("regular", "rerun", "manual")
    assert NOT_READY_REVISIT_CAP_S == 300
    assert _batch_status["line"].startswith("BATCH_RESULT status=FAILURE module=token-metrics ")
    assert _batch_status["line"].endswith(" rows=0 elapsed=0s slot=-- final=0")


def test_make_context_slot_and_final():
    c = ctx(hour=2)
    assert (c.mode, c.replace, c.slot, c.final, c.source_type) == ("regular", False, "02", False, "metrics-api-v1")
    assert ctx(hour=9).final is True                                   # FINAL_HOUR_KST 기본 9
    assert ctx(hour=8).final is False
    assert ctx(hour=8, cfg=Config(final_hour_kst=8)).final is True     # env 조정 반영
    assert ctx(mode=MODE_RERUN, hour=9).final is False                 # 최종 판정은 정기 전용
    assert ctx(mode=MODE_MANUAL, hour=9).final is False
    manual = RunContext(mode=MODE_MANUAL, replace=True, batch_time=datetime(2026, 9, 11, 14, 0, tzinfo=KST),
                        source_type="manual-v0")                       # T7 조립 형태
    assert (manual.slot, manual.final, manual.source_type) == ("14", False, "manual-v0")


# ---------- 마커 문자열 ----------

def test_service_line_format_exact():
    o = ServiceOutcome(service=SERVICE, status="SUCCESS", rows=5, warn=2, rejected=1)
    assert _service_line(o) == ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A "
                                "source_type=metrics-api-v1 rows=5 pages=1 warn=2 rejected=1")
    o = ServiceOutcome(service=SERVICE, status="SKIPPED", source_type="manual-v0", reason="already_loaded")
    assert _service_line(o) == ("SERVICE_RESULT status=SKIPPED module=token-metrics service=Mock Service A "
                                "source_type=manual-v0 rows=0 pages=1 warn=0 rejected=0 reason=already_loaded")
    assert ServiceOutcome(service=SERVICE).status == "FAILURE"          # 기본 = FAILURE


def test_check_lines_sorted_nonzero_only_no_payload():
    lines = _check_lines(SERVICE, {"identity_drift": 1, "hours_over_count": 2, "engine_malformed": 0})
    assert lines == ["CHECK WARN service=Mock Service A hours_over_count=2",
                     "CHECK WARN service=Mock Service A identity_drift=1"]
    assert _check_lines(SERVICE, {}) == []


def test_batch_line_status_rules_slot_final_reason():
    clock = Clock(7.9)
    ok = ServiceOutcome(service=SERVICE, status="SUCCESS", rows=5)
    nodata = ServiceOutcome(service=SERVICE_B, status="NODATA")
    skipped = ServiceOutcome(service=SERVICE_C, status="SKIPPED", reason="disabled")
    failed = ServiceOutcome(service=SERVICE_C, reason="retention")
    assert _batch_line([ok, nodata, skipped], 0.0, clock, ctx(hour=2)) == (
        "BATCH_RESULT status=SUCCESS module=token-metrics services_ok=2 services_failed=0 "
        "services_skipped=1 rows=5 elapsed=7s slot=02 final=0")
    assert _batch_line([ok, failed], 0.0, clock, ctx(hour=9)).startswith(
        "BATCH_RESULT status=FAILURE module=token-metrics services_ok=1 services_failed=1 services_skipped=0 ")
    assert _batch_line([ok, failed], 0.0, clock, ctx(hour=9)).endswith(" slot=09 final=1")
    assert "status=NODATA " in _batch_line([nodata, nodata], 0.0, clock, ctx())      # 전부 NODATA
    assert "status=SUCCESS " in _batch_line([skipped], 0.0, clock, ctx())           # 전부 SKIPPED = SUCCESS
    assert "status=SUCCESS " in _batch_line([], 0.0, clock, ctx())
    assert _batch_line([failed], 0.0, clock, ctx(), reason="mutation_budget").endswith(
        " slot=02 final=0 reason=mutation_budget")
    assert _batch_reason([ok, ServiceOutcome(service=SERVICE_B, reason="mutation_budget")]) == "mutation_budget"
    assert _batch_reason([ok, failed]) == ""


def test_sigterm_reemits_cached_line(capsys):
    cached = ("BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 services_failed=0 "
              "services_skipped=0 rows=5 elapsed=3s slot=02 final=0")
    _batch_status["line"] = cached
    with pytest.raises(SystemExit) as ei:
        _sigterm_handler(15, None)
    assert ei.value.code == 1
    assert capsys.readouterr().out.rstrip("\n").splitlines()[-1] == cached + " note=sigterm"


# ---------- 게이트 매트릭스 (§5.2 모드×게이트) ----------

def test_gate_disabled_all_modes():
    e = entry(enabled=False)
    for mode in (MODE_REGULAR, MODE_RERUN, MODE_MANUAL):
        assert _gate(e, DATE, ctx(mode=mode)) == "disabled"


def test_gate_before_since_after_until_regular_only():
    e = entry(api_since="2026-09-09", until="2026-09-30")
    assert _gate(e, "2026-09-08", ctx()) == "before_since"
    assert _gate(e, "2026-09-09", ctx()) is None                        # 경계 = 포함
    assert _gate(e, "2026-09-30", ctx()) is None
    assert _gate(e, "2026-10-01", ctx()) == "after_until"
    assert _gate(entry(until=None), "2099-01-01", ctx()) is None       # until 없음 = 열림
    for mode in (MODE_RERUN, MODE_MANUAL):
        assert _gate(e, "2026-09-08", ctx(mode=mode)) is None
        assert _gate(e, "2026-10-01", ctx(mode=mode)) is None


# ---------- 오류 → outcome 매트릭스 ----------

def test_outcome_from_error_not_ready_matrix():
    err = CollectError(Event.NOT_READY, "409", retry_after_s=900)
    assert _outcome_from_error(ENTRY, err, ctx(), revisited=False) is None          # 큐 끝 재방문
    o = _outcome_from_error(ENTRY, err, ctx(hour=2), revisited=True)
    assert (o.status, o.reason) == ("SKIPPED", "not_ready")
    o = _outcome_from_error(ENTRY, err, ctx(hour=9), revisited=True)
    assert (o.status, o.reason) == ("FAILURE", "not_ready_at_0900")
    o = _outcome_from_error(ENTRY, err, ctx(mode=MODE_RERUN, hour=9), revisited=True)
    assert (o.status, o.reason) == ("FAILURE", "not_ready")
    assert _outcome_from_error(ENTRY, err, ctx(mode=MODE_RERUN), revisited=False) is None


def test_outcome_from_error_retention_and_others():
    ret = CollectError(Event.RETENTION, "404")
    assert (_outcome_from_error(ENTRY, ret, ctx(), False).status,
            _outcome_from_error(ENTRY, ret, ctx(), False).reason) == ("FAILURE", "retention")
    o = _outcome_from_error(ENTRY, ret, ctx(mode=MODE_RERUN), False)
    assert (o.status, o.reason) == ("SKIPPED", "retention")
    for ev in (Event.PERMANENT_ERROR, Event.RETRYABLE, Event.INVARIANT_BROKEN):
        o = _outcome_from_error(ENTRY, CollectError(ev, "x"), ctx(), True)
        assert (o.status, o.reason, o.service) == ("FAILURE", ev.value, SERVICE)
    manual = RunContext(mode=MODE_MANUAL, replace=False, batch_time=datetime(2026, 9, 11, 14, 0, tzinfo=KST),
                        source_type="manual-v0")
    o = _outcome_from_error(ENTRY, CollectError(Event.PERMANENT_ERROR, "x"), manual, False)
    assert o.source_type == "manual-v0"                                 # ctx.source_type 복사


# ---------- 날짜 산출·batch_time 해석 ----------

def _args(**kw) -> argparse.Namespace:
    base = {"batch_time": None, "from_date": None, "to_date": None, "service": None, "replace": False}
    base.update(kw)
    return argparse.Namespace(**base)


def test_parse_batch_time_naive_is_kst_and_aware_converted():
    bt = _parse_batch_time("2026-09-11T02:05:00")
    assert bt == datetime(2026, 9, 11, 2, 5, tzinfo=KST) and bt.tzinfo is not None
    assert _parse_batch_time("2026-09-10T17:05:00+00:00") == datetime(2026, 9, 11, 2, 5, tzinfo=KST)
    assert _parse_batch_time(None).tzinfo is not None                  # now(KST)


def test_target_dates_regular_is_yesterday_rerun_is_range():
    bt = _parse_batch_time("2026-09-11T02:05:00+09:00")
    assert _target_dates(_args(), bt) == (["2026-09-10"], MODE_REGULAR)
    assert _target_dates(_args(from_date="2026-09-01", to_date="2026-09-03"), bt) == (
        ["2026-09-01", "2026-09-02", "2026-09-03"], MODE_RERUN)
    assert _target_dates(_args(from_date="2026-09-03", to_date="2026-09-03"), bt)[0] == ["2026-09-03"]


def test_target_dates_rejects_half_pair_and_reversed():
    bt = _parse_batch_time(None)
    with pytest.raises(ValueError, match="--from/--to"):
        _target_dates(_args(from_date="2026-09-01"), bt)
    with pytest.raises(ValueError, match="--from/--to"):
        _target_dates(_args(to_date="2026-09-01"), bt)
    with pytest.raises(ValueError, match="after"):
        _target_dates(_args(from_date="2026-09-03", to_date="2026-09-01"), bt)
    with pytest.raises(ValueError):
        _target_dates(_args(from_date="2026-13-01", to_date="2026-09-01"), bt)


# ---------- run_collection / main (Step 6 추가) ----------
from app.main import _run_dates, main, run_collection   # noqa: E402
from app.normalize import check_report_structure         # noqa: E402
from app.writer import MutationBudgetExceeded            # noqa: E402


def G(**kw) -> dict:
    base = {"model": MODELS[1], "gpuType": GPU_TYPE, "category": "serving", "gpuCount": 4, "gpuHours": 96.0}
    base.update(kw)
    return base


TTFT = {"p50": 280, "p90": 560, "p95": 720, "p99": 1200}
ITL = {"p50": 24, "p90": 38, "p95": 47, "p99": 80}


def S(**kw) -> dict:
    base = {"model": MODELS[1], "ttftMs": dict(TTFT), "itlMs": dict(ITL), "outputTps": {"p50": 41.0}}
    base.update(kw)
    return base


def report(d=DATE, service=SERVICE, gpu=None, serving=None, **top) -> dict:
    """기본 = gpu 2행(opus·sonnet) + serving 1레코드(ttftMs·itlMs·outputTps → long form 3행) → rows 5."""
    body = {"date": d, "serviceGroup": SERVICE_GROUP, "service": service, "generatedAt": GENERATED_AT,
            "engine": ENGINE,
            "gpu": [G(model=MODELS[0]), G(model=MODELS[1])] if gpu is None else gpu,
            "serving": [S()] if serving is None else serving}
    body.update(top)
    return body


class FakeWriter:
    """T5 MetricsWriter 대역 — main 이 부르는 4메서드만. batches = replace_batch 호출 기록 [(date, [services])]."""

    def __init__(self, anchors=(), anchor_types=None, raise_budget=False, raise_sync=False):
        self.anchors = set(anchors)                      # {(date, service)} — anchor_exists
        self.anchor_types = dict(anchor_types or {})     # {(date, service): source_type} — anchor_source_type
        self.batches = []
        self.sync_calls = 0
        self.sync_entries = None
        self.raise_budget = raise_budget
        self.raise_sync = raise_sync

    def anchor_exists(self, date, service):
        return (date, service) in self.anchors

    def anchor_source_type(self, date, service):
        return self.anchor_types.get((date, service))

    def replace_batch(self, date, items):
        if self.raise_budget:
            raise MutationBudgetExceeded(3, 0, 2)
        self.batches.append((date, [e.service for e, _, _ in items]))
        return {e.service: r.rows for e, _, r in items}

    def sync_registry(self, entries):
        self.sync_calls += 1
        self.sync_entries = [e.service for e in entries]
        if self.raise_sync:
            raise RuntimeError("ch down")
        return True


def fetcher_ok(**overrides):
    """항상 200 — report(**overrides) 를 서비스명·날짜만 바꿔 MetricsPayload 로 반환. f.calls = [(service, date)]."""
    calls = []

    def f(entry, d, cfg, session):
        calls.append((entry.service, d))
        return check_report_structure(report(d=d, service=entry.service, **overrides), d)
    f.calls = calls
    return f


def fetcher_script(script):
    """서비스별 순서 스크립트 {service: [CollectError | None, ...]} — CollectError 는 raise, None 은 기본 200.
    소진 후 마지막 항목 반복. 스크립트에 없는 서비스는 항상 200. f.calls = [service, ...] (호출 순서)."""
    calls = []
    cursor = {}

    def f(entry, d, cfg, session):
        calls.append(entry.service)
        seq = script.get(entry.service, [None])
        i = cursor.get(entry.service, 0)
        cursor[entry.service] = i + 1
        step = seq[min(i, len(seq) - 1)]
        if isinstance(step, BaseException):
            raise step
        return check_report_structure(report(d=d, service=entry.service), d)
    f.calls = calls
    return f


def nr(retry=900) -> CollectError:
    return CollectError(Event.NOT_READY, "409", retry_after_s=retry)


def run(capsys, entries, fetcher, *, c=None, cfg=None, writer=None, clock=None, sleeps=None,
        dim_entries=None, register_dims=True, target=DATE):
    """run_collection 1회 → (exit code, writer, stdout 줄 목록). sleeper 는 시계를 전진시킨다(재방문 도달)."""
    clock = clock or Clock()
    w = writer if writer is not None else FakeWriter()

    def sleeper(s):
        if sleeps is not None:
            sleeps.append(s)
        clock.advance(s)

    code = run_collection(cfg or Config(), entries, target, c or ctx(), clock=clock, sleeper=sleeper,
                          fetcher=fetcher, writer=w, session=object(), register_dims=register_dims,
                          dim_entries=dim_entries)
    return code, w, capsys.readouterr().out.rstrip("\n").splitlines()


def _first(lines, prefix):
    return next(i for i, l in enumerate(lines) if l.startswith(prefix))


# ---------- 게이트·앵커 (§5.2 표 1~4행) ----------

def test_gate_disabled_all_modes_no_fetch(capsys):
    f = fetcher_ok()
    for mode in (MODE_REGULAR, MODE_RERUN):
        code, w, out = run(capsys, [entry(enabled=False)], f, c=ctx(mode=mode))
        assert code == 0 and f.calls == [] and w.batches == []
        assert ("SERVICE_RESULT status=SKIPPED module=token-metrics service=Mock Service A "
                "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=disabled") in out


def test_gate_before_since_after_until_regular_only_in_run(capsys):
    e = entry(api_since="2026-09-09", until="2026-09-30")
    f = fetcher_ok()
    _, _, out = run(capsys, [e], f, target="2026-09-08")
    assert any(l.endswith("reason=before_since") for l in out) and f.calls == []
    _, _, out = run(capsys, [e], f, target="2026-10-01")
    assert any(l.endswith("reason=after_until") for l in out) and f.calls == []
    code, w, out = run(capsys, [e], f, c=ctx(mode=MODE_RERUN), target="2026-09-08")
    assert code == 0 and f.calls == [(SERVICE, "2026-09-08")] and w.batches == [("2026-09-08", [SERVICE])]


def test_already_loaded_skips_without_fetch(capsys):
    f = fetcher_ok()
    code, w, out = run(capsys, [ENTRY], f, writer=FakeWriter(anchors={(DATE, SERVICE)}))
    assert code == 0 and f.calls == [] and w.batches == []
    assert ("SERVICE_RESULT status=SKIPPED module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=already_loaded") in out
    code, w, out = run(capsys, [ENTRY], f, c=ctx(mode=MODE_RERUN), writer=FakeWriter(anchors={(DATE, SERVICE)}))
    assert code == 0 and f.calls == [] and any(l.endswith("reason=already_loaded") for l in out)   # rerun w/o --replace


def test_manual_row_present_warn_regular_only(capsys):
    mk = lambda: FakeWriter(anchors={(DATE, SERVICE)}, anchor_types={(DATE, SERVICE): "manual-v0"})  # noqa: E731
    _, _, out = run(capsys, [ENTRY], fetcher_ok(), writer=mk())
    assert out.index("CHECK WARN service=Mock Service A manual_row_present=1") < _first(out, "SERVICE_RESULT")
    assert any(l.endswith("reason=already_loaded") for l in out)
    _, _, out = run(capsys, [ENTRY], fetcher_ok(), c=ctx(mode=MODE_RERUN), writer=mk())
    assert not any(l.startswith("CHECK WARN") for l in out)
    assert any(l.endswith("reason=already_loaded") for l in out)


def test_replace_bypasses_anchor(capsys):
    f = fetcher_ok()
    code, w, out = run(capsys, [ENTRY], f, c=ctx(mode=MODE_RERUN, replace=True),
                       writer=FakeWriter(anchors={(DATE, SERVICE)}))
    assert code == 0 and len(f.calls) == 1 and w.batches == [(DATE, [SERVICE])]
    assert any(l.startswith("SERVICE_RESULT status=SUCCESS") for l in out)


# ---------- 200 → 상태값 (§5.2 표 "200" 행) ----------

def test_success_nodata_and_case_e(capsys):
    _, w, out = run(capsys, [ENTRY], fetcher_ok())
    assert ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=5 pages=1 warn=0 rejected=0") in out
    assert w.batches == [(DATE, [SERVICE])]
    code, w, out = run(capsys, [ENTRY], fetcher_ok(gpu=[], serving=[]))
    assert code == 0 and w.batches == [(DATE, [SERVICE])]               # NODATA 도 앵커(summary) 적재
    assert ("SERVICE_RESULT status=NODATA module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0") in out
    assert out[-1].startswith("BATCH_RESULT status=NODATA ")
    _, _, out = run(capsys, [ENTRY], fetcher_ok(gpu=[]))                # 케이스 E: gpu:[] + serving 행 = SUCCESS
    assert ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=3 pages=1 warn=0 rejected=0") in out


def test_all_rows_rejected_warn(capsys):
    _, _, out = run(capsys, [ENTRY], fetcher_ok(gpu=[G(category="prod")], serving=[]))
    assert "CHECK WARN service=Mock Service A all_rows_rejected=1" in out
    assert ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=1") in out
    assert out[-1].startswith("BATCH_RESULT status=SUCCESS ")


def test_warn_and_check_lines_from_flags(capsys):
    _, _, out = run(capsys, [ENTRY], fetcher_ok(gpu=[G(gpuCount=2, gpuHours=49)], serving=[],
                                                 serviceGroup="Drift Group"))
    assert [l for l in out if l.startswith("CHECK WARN")] == [
        "CHECK WARN service=Mock Service A hours_over_count=1",
        "CHECK WARN service=Mock Service A identity_drift=1"]         # 코드 정렬, 카운트만
    assert out.index("CHECK WARN service=Mock Service A identity_drift=1") < _first(out, "SERVICE_RESULT")
    assert ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=1 pages=1 warn=2 rejected=0") in out


def test_no_payload_in_logs(capsys):
    secret = "secret-model-xyz"
    _, _, out = run(capsys, [ENTRY], fetcher_ok(gpu=[G(model=secret, gpuCount=1, gpuHours=99)],
                                                 serving=[S(model=secret)]))
    assert secret not in "\n".join(out)                                 # 로그 페이로드 금지 (§3 전제 11)
    assert "CHECK WARN service=Mock Service A hours_over_count=1" in out


# ---------- 409 not_ready: 큐 끝 재방문 1회 · 최종 슬롯 · rerun (§5.2) ----------

def test_409_revisit_once_then_skip_non_final(capsys):
    f = fetcher_script({SERVICE: [nr(900), nr(900)]})
    sleeps = []
    code, w, out = run(capsys, [ENTRY], f, sleeps=sleeps)
    assert code == 0 and f.calls == [SERVICE, SERVICE] and w.batches == []
    assert sleeps == [300]                                              # min(max(900,1), 300)
    assert ("SERVICE_RESULT status=SKIPPED module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=not_ready") in out
    assert out[-1].startswith("BATCH_RESULT status=SUCCESS module=token-metrics services_ok=0 "
                              "services_failed=0 services_skipped=1 rows=0 ")
    assert out[-1].endswith(" slot=02 final=0")


def test_409_twice_final_slot_failure_exit1(capsys):
    f = fetcher_script({SERVICE: [nr(60), nr(60)]})
    code, w, out = run(capsys, [ENTRY], f, c=ctx(hour=9))
    assert code == 1 and f.calls == [SERVICE, SERVICE] and w.batches == []
    assert any(l.startswith("SERVICE_RESULT status=FAILURE") and l.endswith("reason=not_ready_at_0900") for l in out)
    assert out[-1].startswith("BATCH_RESULT status=FAILURE module=token-metrics services_ok=0 "
                              "services_failed=1 services_skipped=0 ")
    assert out[-1].endswith(" slot=09 final=1")


def test_409_revisit_at_queue_end(capsys):
    f = fetcher_script({SERVICE: [nr(60), None], SERVICE_B: [None]})
    sleeps = []
    code, w, _ = run(capsys, [ENTRY, ENTRY_B], f, sleeps=sleeps)
    assert code == 0 and f.calls == [SERVICE, SERVICE_B, SERVICE]       # A 는 큐 끝으로
    assert sleeps == [60]                                               # B 처리 후 A 의 resume 까지 대기
    assert w.batches == [(DATE, [SERVICE_B]), (DATE, [SERVICE])]        # 정기: 서비스별 순차 적재


def test_409_retry_after_zero_waits_at_least_1s(capsys):
    sleeps = []
    code, _, _ = run(capsys, [ENTRY], fetcher_script({SERVICE: [nr(0), None]}), sleeps=sleeps)
    assert code == 0 and sleeps == [1]


def test_409_in_rerun_is_failure_not_ready(capsys):
    code, _, out = run(capsys, [ENTRY], fetcher_script({SERVICE: [nr(60), nr(60)]}),
                       c=ctx(mode=MODE_RERUN, hour=9))
    assert code == 1
    assert any(l.startswith("SERVICE_RESULT status=FAILURE") and l.endswith("reason=not_ready") for l in out)
    assert out[-1].endswith(" slot=09 final=0")                         # rerun: final 판정 없음


# ---------- 404·4xx·503·구조 위반·예상 밖 예외 ----------

def test_retention_regular_failure_rerun_skipped(capsys):
    ret = {SERVICE: [CollectError(Event.RETENTION, "404")]}
    code, _, out = run(capsys, [ENTRY], fetcher_script(ret))
    assert code == 1 and any(l.startswith("SERVICE_RESULT status=FAILURE") and l.endswith("reason=retention")
                             for l in out)
    code, _, out = run(capsys, [ENTRY], fetcher_script(ret), c=ctx(mode=MODE_RERUN, replace=True))
    assert code == 0 and any(l.startswith("SERVICE_RESULT status=SKIPPED") and l.endswith("reason=retention")
                             for l in out)


def test_permanent_error_failure_isolated(capsys):
    f = fetcher_script({SERVICE: [CollectError(Event.PERMANENT_ERROR, "http 400")], SERVICE_B: [None]})
    code, w, out = run(capsys, [ENTRY, ENTRY_B], f)
    assert code == 1 and w.batches == [(DATE, [SERVICE_B])]
    assert ("SERVICE_RESULT status=FAILURE module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=permanent_error") in out
    assert any(l.startswith("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service B") for l in out)
    assert out[-1].startswith("BATCH_RESULT status=FAILURE ") and out[-1].endswith(" slot=02 final=0")


def test_retryable_exhausted_reason(capsys):
    code, _, out = run(capsys, [ENTRY], fetcher_script({SERVICE: [CollectError(Event.RETRYABLE, "503")]}))
    assert code == 1 and any(l.endswith("reason=retryable") for l in out)


def test_normalize_payload_error_is_permanent_error(capsys):
    def f(entry, d, cfg, session):
        p = check_report_structure(report(d=d, service=entry.service), d)
        p.gpu = {"not": "a list"}                                       # normalize 단계 구조 위반 유발
        return p
    code, w, out = run(capsys, [ENTRY], f)
    assert code == 1 and w.batches == [] and any(l.endswith("reason=permanent_error") for l in out)


def test_unexpected_exception_isolated(capsys):
    def f(entry, d, cfg, session):
        raise KeyError("boom")
    code, w, out = run(capsys, [ENTRY, ENTRY_B], f)
    assert code == 1 and w.batches == []
    assert sum(1 for l in out if l.endswith("reason=unexpected:KeyError")) == 2
    assert out[-1].startswith("BATCH_RESULT status=FAILURE ")


def test_writer_exception_isolated_as_unexpected(capsys):
    class Boom(FakeWriter):
        def replace_batch(self, date, items):
            raise RuntimeError("ch down")
    code, _, out = run(capsys, [ENTRY], fetcher_ok(), writer=Boom())
    assert code == 1 and any(l.endswith("reason=unexpected:RuntimeError") for l in out)


# ---------- 소프트 데드라인 · LOAD_BUDGET · 뮤테이션 예산 (§5.2 마지막 행, §5.4) ----------

def test_load_budget_reservation(capsys):
    clock = Clock()

    def f(entry, d, cfg, session):
        clock.advance(2400 - 1199)                                      # 잔여 1199s < LOAD_BUDGET_S 1200
        return check_report_structure(report(d=d, service=entry.service), d)
    code, w, out = run(capsys, [ENTRY], f, clock=clock, cfg=Config(soft_deadline_minutes=40, load_budget_s=1200))
    assert code == 1 and w.batches == []
    assert ("SERVICE_RESULT status=FAILURE module=token-metrics service=Mock Service A "
            "source_type=metrics-api-v1 rows=0 pages=1 warn=0 rejected=0 reason=load_budget") in out


def test_load_budget_boundary_exactly_budget_loads(capsys):
    clock = Clock()

    def f(entry, d, cfg, session):
        clock.advance(2400 - 1200)                                      # 잔여 == 1200 → 착수
        return check_report_structure(report(d=d, service=entry.service), d)
    code, w, _ = run(capsys, [ENTRY], f, clock=clock, cfg=Config(soft_deadline_minutes=40, load_budget_s=1200))
    assert code == 0 and w.batches == [(DATE, [SERVICE])]


def test_deadline_remaining_queue_failure(capsys):
    clock = Clock()

    def f(entry, d, cfg, session):
        clock.advance(2500)                                             # 첫 fetch 중 2400s 경과
        return check_report_structure(report(d=d, service=entry.service), d)
    code, w, out = run(capsys, [ENTRY, ENTRY_B, ENTRY_C], f, clock=clock)
    assert code == 1 and w.batches == []
    assert any(l.startswith("SERVICE_RESULT status=FAILURE module=token-metrics service=Mock Service A")
               and l.endswith("reason=load_budget") for l in out)
    assert sum(1 for l in out if l.endswith("reason=deadline")) == 2   # B·C 는 fetch 없이 deadline
    assert out[-1].startswith("BATCH_RESULT status=FAILURE ")           # 정상 종료 + 마커 보장


def test_deadline_before_start_marks_all_failed_no_fetch(capsys):
    def f(entry, d, cfg, session):
        raise AssertionError("데드라인 소진 후에는 fetch 가 호출되면 안 됨")
    code, w, out = run(capsys, [ENTRY, ENTRY_B], f, cfg=Config(soft_deadline_minutes=10, load_budget_s=1200))
    assert code == 1 and w.batches == [] and sum(1 for l in out if l.endswith("reason=deadline")) == 2


def test_mutation_budget_failure_reason_promoted(capsys):
    code, w, out = run(capsys, [ENTRY, ENTRY_B], fetcher_ok(), c=ctx(mode=MODE_RERUN, replace=True),
                       writer=FakeWriter(raise_budget=True))
    assert code == 1 and w.batches == []
    assert sum(1 for l in out if l.startswith("SERVICE_RESULT status=FAILURE")
               and l.endswith("reason=mutation_budget")) == 2
    assert out[-1].endswith(" slot=02 final=0 reason=mutation_budget")


# ---------- 배칭 (§5.4 (A)(B)(C)) · 레지스트리 동기화 ----------

def test_rerun_batches_per_date_single_replace_batch(capsys):
    three = [ENTRY, ENTRY_B, ENTRY_C]
    _, w, out = run(capsys, three, fetcher_ok(), c=ctx(mode=MODE_RERUN, replace=True))
    assert w.batches == [(DATE, [SERVICE, SERVICE_B, SERVICE_C])]      # (A) 전부 fetch → (B)(C) 1회
    assert sum(1 for l in out if l.startswith("SERVICE_RESULT status=SUCCESS")) == 3
    _, w, _ = run(capsys, three, fetcher_ok())
    assert w.batches == [(DATE, [SERVICE]), (DATE, [SERVICE_B]), (DATE, [SERVICE_C])]   # 정기: 서비스별


def test_rerun_batch_excludes_skipped_and_failed(capsys):
    f = fetcher_script({SERVICE: [CollectError(Event.PERMANENT_ERROR, "400")]})
    code, w, _ = run(capsys, [ENTRY, ENTRY_B, ENTRY_C], f, c=ctx(mode=MODE_RERUN),
                     writer=FakeWriter(anchors={(DATE, SERVICE_C)}))
    assert code == 1 and w.batches == [(DATE, [SERVICE_B])]            # A=FAILURE, C=already_loaded(--replace 없음)


def test_registry_sync_regular_only_with_full_dim_entries(capsys):
    _, w, _ = run(capsys, [ENTRY], fetcher_ok(), dim_entries=[ENTRY, ENTRY_B])
    assert w.sync_calls == 1 and w.sync_entries == [SERVICE, SERVICE_B]   # --service 필터 전 전체
    _, w, _ = run(capsys, [ENTRY], fetcher_ok())
    assert w.sync_calls == 1 and w.sync_entries == [SERVICE]               # dim_entries 미지정 → entries
    _, w, _ = run(capsys, [ENTRY], fetcher_ok(), c=ctx(mode=MODE_RERUN, replace=True))
    assert w.sync_calls == 0
    _, w, _ = run(capsys, [ENTRY], fetcher_ok(), register_dims=False)
    assert w.sync_calls == 0


def test_registry_sync_failure_is_warn_not_fatal(capsys):
    code, w, out = run(capsys, [ENTRY], fetcher_ok(), writer=FakeWriter(raise_sync=True))
    assert code == 0 and w.batches == [(DATE, [SERVICE])]
    assert out[0] == "CHECK WARN service=- registry_sync_failed=1"
    assert any(l.startswith("SERVICE_RESULT status=SUCCESS") for l in out)


def test_batch_line_format_and_final_in_run(capsys):
    _, _, out = run(capsys, [ENTRY], fetcher_ok())
    assert out[-1] == ("BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 services_failed=0 "
                       "services_skipped=0 rows=5 elapsed=0s slot=02 final=0")
    assert _batch_status["line"] == out[-1]                             # SIGTERM 캐시 = 마지막 줄
    _, _, out = run(capsys, [ENTRY], fetcher_ok(), c=ctx(hour=9))
    assert out[-1].endswith(" rows=5 elapsed=0s slot=09 final=1")
    cfg8 = Config(final_hour_kst=8)
    _, _, out = run(capsys, [ENTRY], fetcher_ok(), c=ctx(hour=8, cfg=cfg8), cfg=cfg8)
    assert out[-1].endswith(" slot=08 final=1") and sum(1 for l in out if l.startswith("BATCH_RESULT")) == 1


# ---------- main(): CLI · 대상일 · 단일 BATCH_RESULT ----------

class _NoSignal:
    SIGTERM = 15

    @staticmethod
    def signal(signum, handler):
        return None


def _patch_main(monkeypatch, entries, writer, fetcher, cfg=None):
    monkeypatch.setattr("app.main.load_config", lambda: cfg or Config())
    monkeypatch.setattr("app.main.load_endpoints", lambda p: entries)
    monkeypatch.setattr("app.main.MetricsWriter", lambda c: writer)
    monkeypatch.setattr("app.main.api_client", type("M", (), {"fetch_metrics": staticmethod(fetcher)}))
    monkeypatch.setattr("app.main.signal", _NoSignal)


def test_main_unknown_service_exit_2(monkeypatch, capsys):
    _patch_main(monkeypatch, [ENTRY], FakeWriter(), fetcher_ok())
    assert main(["--service", "nope"]) == 2
    assert "unknown service: nope" in capsys.readouterr().err


def test_main_from_to_pair_required_and_ordered(monkeypatch, capsys):
    f = fetcher_ok()
    _patch_main(monkeypatch, [ENTRY], FakeWriter(), f)
    assert main(["--from", "2026-09-01"]) == 2
    assert main(["--to", "2026-09-01"]) == 2
    assert main(["--from", "2026-09-03", "--to", "2026-09-01"]) == 2
    err = capsys.readouterr().err
    assert "--from/--to must be given together" in err and "--from must not be after --to" in err
    assert f.calls == []


def test_main_replace_requires_range(monkeypatch, capsys):
    f = fetcher_ok()
    _patch_main(monkeypatch, [ENTRY], FakeWriter(), f)
    assert main(["--replace"]) == 2
    assert "--replace requires --from/--to" in capsys.readouterr().err and f.calls == []


def test_main_config_error_exit_2(monkeypatch, capsys):
    def bad(p):
        raise ValueError("endpoints file has no services")
    monkeypatch.setattr("app.main.load_config", lambda: Config())
    monkeypatch.setattr("app.main.load_endpoints", bad)
    monkeypatch.setattr("app.main.signal", _NoSignal)
    assert main([]) == 2
    assert "config error: ValueError: endpoints file has no services" in capsys.readouterr().err


def test_main_regular_target_is_yesterday(monkeypatch, capsys):
    f = fetcher_ok()
    w = FakeWriter()
    _patch_main(monkeypatch, [ENTRY, ENTRY_B], w, f)
    code = main(["2026-09-11T02:05:00+09:00", "--service", SERVICE])
    out = capsys.readouterr().out.rstrip("\n").splitlines()
    assert code == 0 and f.calls == [(SERVICE, "2026-09-10")]
    assert w.sync_entries == [SERVICE, SERVICE_B]                       # --service 필터 전 전체로 동기화
    assert w.batches == [("2026-09-10", [SERVICE])]
    assert out[-1].startswith("BATCH_RESULT status=SUCCESS module=token-metrics services_ok=1 "
                              "services_failed=0 services_skipped=0 rows=5 elapsed=")
    assert out[-1].endswith("s slot=02 final=0")


def test_main_final_slot_from_batch_time(monkeypatch, capsys):
    _patch_main(monkeypatch, [ENTRY], FakeWriter(), fetcher_ok())
    assert main(["2026-09-11T09:05:00+09:00"]) == 0
    assert capsys.readouterr().out.rstrip("\n").splitlines()[-1].endswith(" slot=09 final=1")


def test_main_emits_single_batch_line_for_range(monkeypatch, capsys):
    f = fetcher_ok()
    w = FakeWriter()
    _patch_main(monkeypatch, [ENTRY], w, f)
    code = main(["--from", "2026-09-01", "--to", "2026-09-03"])
    out = capsys.readouterr().out.rstrip("\n").splitlines()
    assert code == 0 and w.sync_calls == 0                              # rerun: 동기화 없음, api_since 무시
    assert [d for _, d in f.calls] == ["2026-09-01", "2026-09-02", "2026-09-03"]
    assert sum(1 for l in out if l.startswith("BATCH_RESULT")) == 1
    assert sum(1 for l in out if l.startswith("SERVICE_RESULT status=SUCCESS")) == 3
    assert out[-1].startswith("BATCH_RESULT status=SUCCESS module=token-metrics services_ok=3 "
                              "services_failed=0 services_skipped=0 rows=15 ")
    assert out[-1].endswith(" final=0")
    assert w.batches == [("2026-09-01", [SERVICE]), ("2026-09-02", [SERVICE]), ("2026-09-03", [SERVICE])]


def test_main_range_mutation_budget_reason_in_aggregate_line(monkeypatch, capsys):
    _patch_main(monkeypatch, [ENTRY], FakeWriter(raise_budget=True), fetcher_ok())
    code = main(["--from", "2026-09-01", "--to", "2026-09-02", "--replace"])
    out = capsys.readouterr().out.rstrip("\n").splitlines()
    assert code == 1 and out[-1].startswith("BATCH_RESULT status=FAILURE module=token-metrics services_ok=0 "
                                            "services_failed=2 services_skipped=0 rows=0 ")
    assert out[-1].endswith(" final=0 reason=mutation_budget")
    assert _batch_status["line"] == out[-1]


# ---- manual 모드 (T7 — 설계 §5.5 · §5.2 표 manual 행) -------------------------------------
from pathlib import Path

from app.main import MANUAL_INPUT_PREFIX

TEMPLATES = Path(__file__).resolve().parents[3] / "docs" / "templates"
MANUAL_ARGS = ["--manual-gpu", str(TEMPLATES / "token_metrics_manual_v0_gpu.csv"),
               "--manual-serving", str(TEMPLATES / "token_metrics_manual_v0_serving.csv"),
               "--manual-engine", str(TEMPLATES / "token_metrics_manual_v0_engine.csv")]
MANUAL_DATE = "2026-08-26"                      # 템플릿 예시 행의 날짜 (api_since 2026-09-09 보다 앞 — manual 은 게이트 없음)
RANGE = ["--from", MANUAL_DATE, "--to", MANUAL_DATE]
M_A = ServiceEntry(service_group="Mock Group", service="Mock Service A", base_url="http://mock",
                   enabled=True, api_since=date(2026, 9, 9), coverage_since=date(2026, 8, 26), until=None)
M_B = ServiceEntry(service_group="Mock Group", service="Mock Service B", base_url="http://mock-b",
                   enabled=True, api_since=date(2026, 9, 9), coverage_since=date(2026, 8, 26), until=None)
LINE_A = ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service A "
          "source_type=manual-v0 rows=5 pages=1 warn=0 rejected=0")
LINE_B = ("SERVICE_RESULT status=SUCCESS module=token-metrics service=Mock Service B "
          "source_type=manual-v0 rows=4 pages=1 warn=0 rejected=0")


def _manual_env(monkeypatch, writer, entries=None):
    monkeypatch.setattr("app.main.load_config", lambda: Config())
    monkeypatch.setattr("app.main.load_endpoints", lambda p: entries if entries is not None else [M_A, M_B])
    monkeypatch.setattr("app.main.MetricsWriter", lambda cfg: writer)


def test_manual_mode_markers_and_no_registry_sync(capsys, monkeypatch):
    w = FakeWriter()
    _manual_env(monkeypatch, w)
    code = main(MANUAL_ARGS + RANGE)
    out = capsys.readouterr().out
    lines = out.splitlines()
    assert code == 0
    assert lines[0] == (f"{MANUAL_INPUT_PREFIX} rows_gpu=4 rows_serving=5 rows_engine=2 "
                        "rows_outside_range=0 rows_other_service=0")
    assert LINE_A in lines and LINE_B in lines                     # api_since 이전 날짜여도 before_since 없음
    assert out.count("MANUAL_INPUT") == 1 and out.count("BATCH_RESULT") == 1
    batch = lines[-1]
    assert batch.startswith("BATCH_RESULT status=SUCCESS module=token-metrics services_ok=2 "
                            "services_failed=0 services_skipped=0 rows=9 elapsed=")
    assert " slot=" in batch and batch.endswith(" final=0")         # manual 은 최종 슬롯 판정 없음
    assert w.sync_calls == 0                                        # §5.5 레지스트리 동기화 없음
    assert len(w.batches) == 1 and sorted(w.batches[0][1]) == ["Mock Service A", "Mock Service B"]  # 날짜당 replace_batch 1회
    assert "CHECK WARN" not in out
    assert "claude-sonnet-5" not in out and "queueWaitMs" not in out  # 페이로드·행 원문 금지 (§3 전제 11)


def test_manual_mode_requires_pair_and_range(capsys, monkeypatch):
    w = FakeWriter()
    _manual_env(monkeypatch, w)
    assert main(MANUAL_ARGS[:2] + RANGE) == 2                       # --manual-gpu 만
    assert "--manual-gpu/--manual-serving must be given together" in capsys.readouterr().err
    assert main(MANUAL_ARGS[2:4] + RANGE) == 2                      # --manual-serving 만
    assert "must be given together" in capsys.readouterr().err
    assert main(MANUAL_ARGS) == 2                                   # --from/--to 없음
    assert "manual mode requires --from/--to" in capsys.readouterr().err
    assert main(["--generated-at", "2026-08-27T09:00:00+09:00"] + RANGE) == 2
    assert "require --manual-gpu/--manual-serving" in capsys.readouterr().err
    assert main(MANUAL_ARGS[4:] + RANGE) == 2                       # --manual-engine 단독
    assert "require --manual-gpu/--manual-serving" in capsys.readouterr().err
    assert w.batches == [] and w.sync_calls == 0


def test_manual_input_error_exits_2_without_load(capsys, monkeypatch, tmp_path):
    w = FakeWriter()
    _manual_env(monkeypatch, w)
    bad = tmp_path / "gpu.csv"
    bad.write_text("date,service,model\n", encoding="utf-8")
    code = main(["--manual-gpu", str(bad), "--manual-serving", MANUAL_ARGS[3]] + RANGE)
    captured = capsys.readouterr()
    assert code == 2
    assert "manual input error:" in captured.err and ":1: header mismatch" in captured.err
    assert "MANUAL_INPUT" not in captured.out and "BATCH_RESULT" not in captured.out
    assert w.batches == []
    assert main(MANUAL_ARGS + ["--from", "2026-08-27", "--to", "2026-08-26"]) == 2     # 역순 범위
    assert "--from must not be after --to" in capsys.readouterr().err
    missing = str(tmp_path / "absent.csv")                                              # 파일 없음(OSError) → 2
    assert main(["--manual-gpu", missing, "--manual-serving", MANUAL_ARGS[3]] + RANGE) == 2
    assert "manual input error:" in capsys.readouterr().err
    assert w.batches == []


def test_manual_already_loaded_without_replace(capsys, monkeypatch):
    w = FakeWriter()
    w.anchors.add((MANUAL_DATE, "Mock Service A"))
    _manual_env(monkeypatch, w)
    code = main(MANUAL_ARGS + RANGE)
    out = capsys.readouterr().out
    assert code == 0
    assert ("SERVICE_RESULT status=SKIPPED module=token-metrics service=Mock Service A "
            "source_type=manual-v0 rows=0 pages=1 warn=0 rejected=0 reason=already_loaded") in out
    assert LINE_B in out
    assert len(w.batches) == 1 and w.batches[0][1] == ["Mock Service B"]
    assert "services_ok=1 services_failed=0 services_skipped=1 rows=4" in out

    w2 = FakeWriter()
    w2.anchors.add((MANUAL_DATE, "Mock Service A"))
    _manual_env(monkeypatch, w2)
    code = main(MANUAL_ARGS + RANGE + ["--replace"])
    out = capsys.readouterr().out
    assert code == 0 and "reason=already_loaded" not in out and LINE_A in out
    assert len(w2.batches) == 1 and sorted(w2.batches[0][1]) == ["Mock Service A", "Mock Service B"]
    assert w2.sync_calls == 0


def test_manual_disabled_gate_and_service_filter(capsys, monkeypatch):
    disabled_b = ServiceEntry(service_group="Mock Group", service="Mock Service B", base_url="http://mock-b",
                              enabled=False, api_since=date(2026, 9, 9), coverage_since=date(2026, 8, 26),
                              until=None)
    w = FakeWriter()
    _manual_env(monkeypatch, w, entries=[M_A, disabled_b])
    assert main(MANUAL_ARGS + RANGE) == 0
    out = capsys.readouterr().out
    assert ("SERVICE_RESULT status=SKIPPED module=token-metrics service=Mock Service B "
            "source_type=manual-v0 rows=0 pages=1 warn=0 rejected=0 reason=disabled") in out   # 모든 모드 공통
    assert w.batches[0][1] == ["Mock Service A"]

    w2 = FakeWriter()
    _manual_env(monkeypatch, w2)
    assert main(MANUAL_ARGS + RANGE + ["--service", "Mock Service A"]) == 0
    out = capsys.readouterr().out
    assert (f"{MANUAL_INPUT_PREFIX} rows_gpu=2 rows_serving=3 rows_engine=2 "
            "rows_outside_range=0 rows_other_service=4") in out         # B 행 gpu 2 + serving 2 는 무시·카운트
    assert LINE_A in out and "service=Mock Service B" not in out
    assert w2.batches[0][1] == ["Mock Service A"]

    w3 = FakeWriter()
    _manual_env(monkeypatch, w3)
    assert main(MANUAL_ARGS + RANGE + ["--service", "nope"]) == 2       # T6 필터: unknown service → exit 2
    assert "unknown service: nope" in capsys.readouterr().err
    assert w3.batches == []


def test_manual_multi_day_no_rows_no_anchor_and_single_batch_line(capsys, monkeypatch):
    w = FakeWriter()
    _manual_env(monkeypatch, w)
    code = main(MANUAL_ARGS + ["--from", MANUAL_DATE, "--to", "2026-08-27",
                               "--generated-at", "2026-08-28T09:00:00+09:00"])
    out = capsys.readouterr().out
    assert code == 0
    assert out.count("BATCH_RESULT") == 1 and out.count("MANUAL_INPUT") == 1
    assert out.count("SERVICE_RESULT status=SUCCESS") == 2                 # 08-26 서비스 2개만
    assert "status=NODATA" not in out                                      # 08-27 행 없음 → 페이로드·앵커 없음(NODATA 아님)
    assert "services_ok=2 services_failed=0 services_skipped=0 rows=9" in out
    assert [d for d, _ in w.batches] == [MANUAL_DATE]                      # 행 있는 날짜만 replace_batch, 08-27 은 호출 없음
    assert sorted(w.batches[0][1]) == ["Mock Service A", "Mock Service B"]


def test_manual_generated_at_offset_mismatch_is_warn(capsys, monkeypatch):
    w = FakeWriter()
    _manual_env(monkeypatch, w)
    assert main(MANUAL_ARGS + RANGE + ["--generated-at", "2026-08-27T00:00:00+00:00"]) == 0
    out = capsys.readouterr().out
    assert "CHECK WARN service=Mock Service A generated_at_offset_mismatch=1" in out
    assert "service=Mock Service A source_type=manual-v0 rows=5 pages=1 warn=1 rejected=0" in out


def test_manual_mutation_budget_promoted_to_batch_reason(capsys, monkeypatch):
    w = FakeWriter(raise_budget=True)
    _manual_env(monkeypatch, w)
    code = main(MANUAL_ARGS + RANGE + ["--replace"])
    out = capsys.readouterr().out
    assert code == 1
    assert out.count("reason=mutation_budget") == 3                    # 서비스 2줄 + BATCH 1줄
    assert out.splitlines()[-1].startswith("BATCH_RESULT status=FAILURE module=token-metrics services_ok=0 services_failed=2")
    assert out.splitlines()[-1].endswith(" final=0 reason=mutation_budget")
