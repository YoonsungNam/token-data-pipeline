from app.api_client import UsagePayload
from app.config import Config, ServiceEntry
from app.events import CollectError, Event
from app.main import main, run_collection

E1 = ServiceEntry(service_group="G", service="S1", base_url="http://a", enabled=True)
E2 = ServiceEntry(service_group="G", service="S2", base_url="http://b", enabled=True)
DATE = "2026-06-15"


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def payload(n_records=2, group="G", service=None, entry=None):
    recs = [{"userId": f"u{i}", "userType": "identified", "model": "m",
             "inputTokens": 1, "outputTokens": 1, "requests": 1} for i in range(n_records)]
    return UsagePayload(records=recs,
                        summary={"inputTokens": n_records, "outputTokens": n_records,
                                 "requests": n_records, "distinctUsers": n_records},
                        generated_at="2026-06-16T02:05:00+09:00",
                        reported_service_group=group,
                        reported_service=service or (entry.service if entry else "S1"),
                        pages=1)


class FakeWriter:
    def __init__(self):
        self.loaded = []

    def fetch_prev_summary(self, service, date):
        return None

    def replace_service_day(self, entry, date, rows_iter, summary_row, audit_prev):
        rows = list(rows_iter)
        self.loaded.append((entry.service, len(rows)))
        return len(rows)

    def replace_dim_services(self, entries, source_type="usage-api-v1"):
        self.dim = [e.service for e in entries]


def run(entries, fetcher, *, is_rerun=False, clock=None, cfg=None, sleeps=None):
    w = FakeWriter()
    code = run_collection(
        cfg or Config(), entries, DATE, is_rerun=is_rerun,
        clock=clock or Clock(), sleeper=(sleeps.append if sleeps is not None else lambda s: None),
        fetcher=fetcher, writer=w, pusher=lambda *a, **k: [])
    return code, w


def test_all_success_exit_zero(capsys):
    code, w = run([E1, E2], lambda e, d, c, s: payload(entry=e))
    assert code == 0 and [x[0] for x in w.loaded] == ["S1", "S2"]
    out = capsys.readouterr().out
    assert out.count("SERVICE_RESULT status=SUCCESS") == 2
    assert "BATCH_RESULT status=SUCCESS module=token-usage services_ok=2 services_failed=0" in out


def test_service_filter_keeps_full_dim_registry():
    w = FakeWriter()
    code = run_collection(Config(), [E1], DATE, is_rerun=True, clock=Clock(),
                          sleeper=lambda s: None, fetcher=lambda e, d, c, s: payload(entry=e),
                          writer=w, pusher=lambda *a, **k: [], dim_entries=[E1, E2])
    assert code == 0 and w.dim == ["S1", "S2"]    # 필터 실행에도 레지스트리는 전체


def test_empty_records_is_nodata_but_summary_loaded(capsys):
    code, w = run([E1], lambda e, d, c, s: payload(0, entry=e))
    assert code == 0
    assert w.loaded == [("S1", 0)]              # summary 행 적재 경로는 replace 호출로 표현
    assert "SERVICE_RESULT status=NODATA" in capsys.readouterr().out


def test_permanent_error_isolated_other_service_succeeds(capsys):
    def fetcher(e, d, c, s):
        if e.service == "S1":
            raise CollectError(Event.PERMANENT_ERROR, "http 400")
        return payload(entry=e)
    code, w = run([E1, E2], fetcher)
    assert code == 1                            # 부분 실패 → exit 1, 성공분 유지
    assert w.loaded == [("S2", 2)]
    out = capsys.readouterr().out
    assert "SERVICE_RESULT status=FAILURE" in out and "SERVICE_RESULT status=SUCCESS" in out
    assert "BATCH_RESULT status=FAILURE" in out


def test_retention_split_by_run_mode(capsys):
    def fetcher(e, d, c, s):
        raise CollectError(Event.RETENTION, "404")
    code, _ = run([E1], fetcher, is_rerun=True)
    assert code == 0                            # 재수집: SKIPPED, exit 영향 없음
    assert "SERVICE_RESULT status=SKIPPED" in capsys.readouterr().out
    code, _ = run([E1], fetcher, is_rerun=False)
    assert code == 1                            # 일일 정기: FAILURE (§5.2)
    assert "SERVICE_RESULT status=FAILURE" in capsys.readouterr().out


def test_not_ready_requeues_then_succeeds(capsys):
    clock = Clock()
    calls = {"n": 0}
    sleeps = []

    def fetcher(e, d, c, s):
        calls["n"] += 1
        if calls["n"] == 1:
            raise CollectError(Event.NOT_READY, "not ready", retry_after_s=60)
        return payload(entry=e)

    def sleeper(s):                             # sleep이 시계를 전진시켜야 재방문 도달
        sleeps.append(s)
        clock.t += s

    w = FakeWriter()
    code = run_collection(Config(), [E1], DATE, is_rerun=False, clock=clock,
                          sleeper=sleeper, fetcher=fetcher, writer=w,
                          pusher=lambda *a, **k: [])
    assert code == 0 and calls["n"] == 2        # 재방문 = 전체 재시작(fetch 재호출)
    assert sleeps and sleeps[0] >= 60           # resume_at까지 sleep


def test_not_ready_budget_exhausted_fails(capsys):
    def fetcher(e, d, c, s):
        # 단일 방문의 대기 요구가 예산(30분=1800s)을 즉시 초과 — main은 캡하지 않음(캡은 api 계층)
        raise CollectError(Event.NOT_READY, "not ready", retry_after_s=1900)

    code, _ = run([E1], fetcher, cfg=Config(not_ready_budget_minutes=30))
    assert code == 1
    out = capsys.readouterr().out
    assert "SERVICE_RESULT status=FAILURE" in out and "not_ready_budget" in out


def test_invariant_broken_restarts_twice_then_fails(capsys):
    calls = {"n": 0}

    def fetcher(e, d, c, s):
        calls["n"] += 1
        raise CollectError(Event.INVARIANT_BROKEN, "meta changed")

    code, _ = run([E1], fetcher)
    assert code == 1 and calls["n"] == 3        # 최초 + 재시작 2회 (§5.3)


def test_soft_deadline_marks_remaining_failed(capsys):
    # soft_deadline_minutes(5분) < LOAD_BUDGET_S(12분) — 잡 시작부터 잔여 예산이 이미
    # 적재 시퀀스 예산 미만이므로 어떤 서비스도 착수하지 않고 전부 deadline으로
    # FAILURE 처리 후 정상 종료해야 한다 (§5.2).
    def fetcher(e, d, c, s):
        raise AssertionError("데드라인 소진 후에는 fetch가 호출되면 안 됨")

    code, w = run([E1, E2], fetcher, clock=Clock(), cfg=Config(soft_deadline_minutes=5))
    assert code == 1
    assert w.loaded == []                       # 아무 것도 착수 안 함
    out = capsys.readouterr().out
    assert out.count("reason=deadline") == 2
    assert "BATCH_RESULT" in out                # 정상 종료 + 마커 보장


def test_load_budget_recheck_blocks_load_before_write(capsys):
    # C1/I4 회귀: fetch 자체가 오래 걸려 남은 예산이 적재 시퀀스 예산(12분) 밑으로
    # 떨어지면, 페이지네이션까지 성공했더라도 writer.replace_service_day 착수 전에 차단한다.
    clock = Clock()

    def fetcher(e, d, c, s):
        clock.t += 45 * 60                      # 남은 예산 5분 < LOAD_BUDGET_S(12분)
        return payload(entry=e)

    code, w = run([E1], fetcher, clock=clock, cfg=Config(soft_deadline_minutes=50))
    assert code == 1
    assert w.loaded == []                       # 적재 미착수 (§5.1-3-5)
    out = capsys.readouterr().out
    assert "SERVICE_RESULT status=FAILURE" in out and "reason=PERMANENT_ERROR" in out


def test_identity_drift_counted_as_warn(capsys):
    code, _ = run([E1], lambda e, d, c, s: payload(entry=e, group="G-DRIFT"))
    assert code == 0
    assert "warn=1" in capsys.readouterr().out  # §5.0 CHECK WARN


def test_naive_batch_time_interpreted_as_kst(monkeypatch):
    import argparse
    from app.main import _target_dates
    args = argparse.Namespace(batch_time="2026-07-11T23:30:00", from_date=None,
                              to_date=None, service=None)
    dates, is_rerun = _target_dates(args)
    assert dates == ["2026-07-10"] and is_rerun is False   # KST 해석 — 호스트 TZ 무관
    args.batch_time = "2026-07-11T23:30:00+09:00"
    assert _target_dates(args)[0] == ["2026-07-10"]


def test_multi_date_rerun_emits_single_batch_line(capsys, monkeypatch):
    monkeypatch.setattr("app.main.load_config", lambda: Config())
    monkeypatch.setattr("app.main.load_endpoints", lambda p: [E1])
    monkeypatch.setattr("app.main.CHWriter", lambda cfg: FakeWriter())
    monkeypatch.setattr("app.main.api_client",
                        type("M", (), {"fetch_service": staticmethod(lambda e, d, c, s: payload(entry=e))}))
    code = main(["--from", "2026-06-14", "--to", "2026-06-15"])
    out = capsys.readouterr().out
    assert out.count("BATCH_RESULT") == 1 and code == 0
