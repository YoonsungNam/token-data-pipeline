import datetime as dt
import importlib.util
import json
import pathlib
import subprocess
import types

import pytest

_RERUN_PATH = pathlib.Path(__file__).resolve().parents[1] / "tools" / "rerun.py"
spec = importlib.util.spec_from_file_location("rerun_metrics", _RERUN_PATH)
rerun = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rerun)

D = dt.date


def cronjob_obj():
    return {
        "metadata": {"name": "token-mart-metrics", "namespace": "monitoring",
                     "resourceVersion": "123", "uid": "x"},
        "spec": {"jobTemplate": {"spec": {
            "backoffLimit": 1,
            "activeDeadlineSeconds": 1800,
            "template": {"spec": {"restartPolicy": "Never", "containers": [
                {"name": "token-mart-metrics", "image": "img:tag"}]}},
        }}},
    }


def jobs_json(*items):
    return {"items": [{"metadata": {"name": n}, "status": s} for n, s in items]}


def kst(hh, mm):
    return dt.datetime(2026, 9, 5, hh, mm, tzinfo=rerun.KST)


# ── 상수·심볼 ────────────────────────────────────────────────────────────────────────────

def test_cronjob_and_window_constants():
    # 델타 누락 시 token-usage CronJob(token-mart-daily)을 오트리거하는 치명 오류가 된다
    assert rerun.CRONJOB == "token-mart-metrics"
    assert rerun.WINDOW_HHMM == (10, 50)
    assert rerun.NAMESPACE_DEFAULT == "monitoring"
    assert rerun.CHUNK_DAYS_DEFAULT == 7
    assert rerun.CHUNK_DAYS_MAX == 16                                     # 64 = 16일 × 4 변이
    assert rerun.ACTIVE_JOB_PREFIX == "token-mart-"
    assert rerun.KST.utcoffset(None) == dt.timedelta(hours=9)


def test_no_chain_or_downstream_symbols():
    # 체인의 종단 — 하류 심볼(MART_RERUN/build_mart_command)·원형 command 빌더 없음
    for sym in ("MART_RERUN", "build_mart_command", "build_collect_command"):
        assert not hasattr(rerun, sym), sym


def test_no_chain_flag():
    parser = rerun.build_arg_parser()
    for flag in (["--chain"], ["--chain-mart"], ["--service", "S"], ["--push-vm"],
                 ["--replace"], ["--target-db", "x"]):
        with pytest.raises(SystemExit):
            parser.parse_args(["--context", "c"] + flag)


def test_cli_defaults_and_overrides():
    args = rerun.build_arg_parser().parse_args(["--context", "homelab"])
    assert args.namespace == "monitoring" and args.cronjob == "token-mart-metrics"
    assert args.chunk_days == 7 and args.force is False
    args = rerun.build_arg_parser().parse_args(
        ["--context", "c", "-n", "ns", "--cronjob", "token-mart-metrics-verify",
         "--from", "2026-08-01", "--to", "2026-08-17", "--chunk-days", "3", "--force"])
    assert (args.namespace, args.cronjob, args.chunk_days, args.force) == \
        ("ns", "token-mart-metrics-verify", 3, True)


# ── 순수 함수 ────────────────────────────────────────────────────────────────────────────

def test_chunk_ranges_seven_day_split():
    assert rerun.chunk_ranges(D(2026, 8, 1), D(2026, 8, 17), 7) == \
        [(D(2026, 8, 1), D(2026, 8, 7)), (D(2026, 8, 8), D(2026, 8, 14)), (D(2026, 8, 15), D(2026, 8, 17))]


def test_chunk_ranges_single_day():
    assert rerun.chunk_ranges(D(2026, 8, 1), D(2026, 8, 1), 7) == [(D(2026, 8, 1), D(2026, 8, 1))]
    assert rerun.chunk_ranges(D(2026, 8, 1), D(2026, 8, 7), 7) == [(D(2026, 8, 1), D(2026, 8, 7))]
    with pytest.raises(ValueError):
        rerun.chunk_ranges(D(2026, 8, 1), D(2026, 8, 7), 0)


def test_window_ok_boundary():
    assert rerun.window_ok(kst(10, 49)) is False
    assert rerun.window_ok(kst(10, 50)) is True
    assert rerun.window_ok(kst(23, 59)) is True
    assert rerun.window_ok(kst(0, 0)) is False                            # 자정 직후도 창 밖
    assert rerun.window_ok(kst(10, 49), force=True) is True


def test_active_mart_jobs_counts_only_active_token_mart_prefix():
    fixture = jobs_json(("token-mart-metrics-x", {"active": 1}),
                        ("token-mart-daily-y", {"active": 1}),
                        ("token-usage-collector-z", {"active": 1}),
                        ("token-mart-metrics-old", {"succeeded": 1}))
    assert rerun.active_mart_jobs(fixture) == 2
    assert rerun.active_mart_jobs({"items": []}) == 0
    assert rerun.active_mart_jobs({}) == 0


def test_build_batch_command_args():
    # ENTRYPOINT(python -m app.batch) 뒤 args만 — "python"/"-m"이 들어가면 인자가 중복된다
    assert rerun.build_batch_command(D(2026, 8, 1), D(2026, 8, 7)) == \
        ["--from", "2026-08-01", "--to", "2026-08-07"]


def test_range_deadline_seven_days_1800():
    assert rerun.range_deadline_s(1) == 1800
    assert rerun.range_deadline_s(7) == 1800
    assert rerun.range_deadline_s(8) == 3600
    assert rerun.range_deadline_s(16) == 5400
    assert rerun.range_deadline_s(100) == rerun.TIMEOUT_RANGE_S == 7200


def test_job_name_format_and_length():
    name = rerun.job_name("token-mart-metrics-verify", D(2026, 8, 1), D(2026, 8, 7), 1756000000)
    assert name == "token-mart-metrics-verify-rerun-20260801-20260807-1756000000"
    assert len(name) <= 63


def test_build_job_spec_overrides_args_not_command_and_strips_cron_metadata():
    job = rerun.build_job_spec(cronjob_obj(), "token-mart-metrics-rerun-1",
                               ["--from", "2026-08-01", "--to", "2026-08-07"])
    assert job["kind"] == "Job"
    # uid/resourceVersion 제거 — name + 라벨만 남는다 (D4: build_job_spec 라벨 계약,
    # collectors/token-metrics/tools/rerun.py build_job_spec와 동일 관례)
    assert job["metadata"] == {"name": "token-mart-metrics-rerun-1",
                               "labels": {"app": "token-mart-metrics", "rerun": "1"}}
    c0 = job["spec"]["template"]["spec"]["containers"][0]
    assert c0["args"] == ["--from", "2026-08-01", "--to", "2026-08-07"]
    assert "command" not in c0                                             # ENTRYPOINT 유지
    assert job["spec"]["activeDeadlineSeconds"] == 1800                    # 기본: CronJob 값 상속
    assert job["spec"]["ttlSecondsAfterFinished"] == 86400                 # 완료 후 GC 기본값(없을 때만)
    job = rerun.build_job_spec(cronjob_obj(), "j", ["--from", "a", "--to", "b"], active_deadline_s=3600)
    assert job["spec"]["activeDeadlineSeconds"] == 3600


def test_build_job_spec_keeps_existing_ttl():
    # setdefault 의미 — CronJob 템플릿이 이미 값을 가지면 덮어쓰지 않는다
    obj = cronjob_obj()
    obj["spec"]["jobTemplate"]["spec"]["ttlSecondsAfterFinished"] = 3600
    job = rerun.build_job_spec(obj, "j", ["--from", "a", "--to", "b"])
    assert job["spec"]["ttlSecondsAfterFinished"] == 3600


# ── main: 인자 오류(exit 2) ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("argv", [
    ["--context", "homelab", "--from", "2026-08-05", "--to", "2026-08-01"],
    ["--context", "homelab", "--from", "2026/08/01", "--to", "2026-08-02"],
    ["--context", "homelab", "--from", "2026-08-01"],
    ["--context", "homelab", "--chunk-days", "17"],
    ["--context", "homelab", "--chunk-days", "0"],
    [],
])
def test_usage_errors_exit_2(argv):
    with pytest.raises(SystemExit) as e:
        rerun.main(argv)
    assert e.value.code == 2


# ── main: 게이트·순차 청크 실행 (kubectl/wait_job/_now_kst 대체) ──────────────────────────

class FakeKubectl:
    """kubectl(context, args, capture=…, input_data=…) 대체 — 호출 기록 + 고정 응답."""

    def __init__(self, jobs=None, cronjob=None):
        self.calls = []
        self.jobs = jobs if jobs is not None else {"items": []}
        self.cronjob = cronjob if cronjob is not None else cronjob_obj()

    def __call__(self, context, args, capture=False, input_data=None):
        self.calls.append((list(args), input_data))
        if args[:2] == ["get", "jobs"]:
            return types.SimpleNamespace(stdout=json.dumps(self.jobs))
        if args[:2] == ["get", "cronjob"]:
            return types.SimpleNamespace(stdout=json.dumps(self.cronjob))
        return types.SimpleNamespace(stdout="")

    def applied(self):
        return [json.loads(inp) for a, inp in self.calls if a[:1] == ["apply"]]


def test_window_refused_exit_2_without_kubectl(monkeypatch, capsys):
    fake = FakeKubectl()
    monkeypatch.setattr(rerun, "kubectl", fake)
    monkeypatch.setattr(rerun, "_now_kst", lambda: kst(10, 49))
    assert rerun.main(["--context", "c"]) == 2
    assert rerun.main(["--context", "c", "--from", "2026-08-01", "--to", "2026-08-02"]) == 2
    assert fake.calls == []                                                # 창 밖이면 kubectl 미호출
    # T11 런타임이 이 문구를 그대로 인용한다 — 고정 (SHOULD-3(b)(8))
    assert "RERUN REFUSED window" in capsys.readouterr().err


def test_active_jobs_refused_exit_2_even_with_force(monkeypatch, capsys):
    fake = FakeKubectl(jobs=jobs_json(("token-mart-daily-abc", {"active": 1})))
    monkeypatch.setattr(rerun, "kubectl", fake)
    monkeypatch.setattr(rerun, "_now_kst", lambda: kst(10, 49))
    assert rerun.main(["--context", "c", "--force"]) == 2
    assert [a[:2] for a, _ in fake.calls] == [["get", "jobs"]]            # 활성 Job 조회 후 중단
    # T11 런타임이 이 문구를 그대로 인용한다 — 고정 (SHOULD-3(b)(8))
    assert "RERUN REFUSED active_jobs=" in capsys.readouterr().err


def test_manual_mode_creates_job_from_cronjob(monkeypatch):
    fake = FakeKubectl()
    waited = []
    monkeypatch.setattr(rerun, "kubectl", fake)
    monkeypatch.setattr(rerun, "_now_kst", lambda: kst(11, 0))
    monkeypatch.setattr(rerun, "wait_job", lambda ctx, ns, name, t: waited.append((name, t)) or True)
    assert rerun.main(["--context", "c", "-n", "ns", "--cronjob", "token-mart-metrics-verify"]) == 0
    create = [a for a, _ in fake.calls if a[:2] == ["create", "job"]]
    assert len(create) == 1 and create[0][2] == "--from=cronjob/token-mart-metrics-verify"
    assert create[0][3].startswith("token-mart-metrics-verify-manual-")
    assert waited[0][1] == rerun.TIMEOUT_SINGLE_S == 2400


def test_range_mode_runs_chunks_sequentially(monkeypatch):
    fake = FakeKubectl()
    waited = []
    monkeypatch.setattr(rerun, "kubectl", fake)
    monkeypatch.setattr(rerun, "_now_kst", lambda: kst(10, 50))
    monkeypatch.setattr(rerun, "wait_job", lambda ctx, ns, name, t: waited.append((name, t)) or True)
    assert rerun.main(["--context", "c", "--from", "2026-08-01", "--to", "2026-08-17"]) == 0
    jobs = fake.applied()
    assert [j["spec"]["template"]["spec"]["containers"][0]["args"] for j in jobs] == [
        ["--from", "2026-08-01", "--to", "2026-08-07"],
        ["--from", "2026-08-08", "--to", "2026-08-14"],
        ["--from", "2026-08-15", "--to", "2026-08-17"],
    ]
    assert [j["spec"]["activeDeadlineSeconds"] for j in jobs] == [1800, 1800, 1800]
    assert [j["metadata"]["name"][:42] for j in jobs] == [
        "token-mart-metrics-rerun-20260801-20260807",
        "token-mart-metrics-rerun-20260808-20260814",
        "token-mart-metrics-rerun-20260815-20260817",
    ]
    # apply → wait → apply → wait … (순차: 다음 apply 전에 wait가 끝난다)
    order = [a[0] for a, _ in fake.calls]
    assert order == ["get", "get", "apply", "apply", "apply"]
    assert [n[:42] for n, _ in waited] == [j["metadata"]["name"][:42] for j in jobs]
    assert all(t == 1800 + 600 for _, t in waited)


def test_range_mode_stops_at_failed_chunk(monkeypatch, capsys):
    fake = FakeKubectl()
    results = iter([True, False])
    monkeypatch.setattr(rerun, "kubectl", fake)
    monkeypatch.setattr(rerun, "_now_kst", lambda: kst(12, 0))
    monkeypatch.setattr(rerun, "wait_job", lambda ctx, ns, name, t: next(results))
    assert rerun.main(["--context", "c", "--from", "2026-08-01", "--to", "2026-08-17"]) == 1
    assert len(fake.applied()) == 2                                        # 3번째 청크 미실행
    err = capsys.readouterr().err
    assert "--from 2026-08-15 --to 2026-08-17" in err                      # 남은 범위 재실행 안내


def test_range_mode_chunk_days_16_deadline_5400(monkeypatch):
    fake = FakeKubectl()
    monkeypatch.setattr(rerun, "kubectl", fake)
    monkeypatch.setattr(rerun, "_now_kst", lambda: kst(12, 0))
    monkeypatch.setattr(rerun, "wait_job", lambda ctx, ns, name, t: True)
    assert rerun.main(["--context", "c", "--from", "2026-08-01", "--to", "2026-08-16",
                       "--chunk-days", "16"]) == 0
    jobs = fake.applied()
    assert len(jobs) == 1 and jobs[0]["spec"]["activeDeadlineSeconds"] == 5400


def test_main_kubectl_failure_is_clean_error_not_traceback(monkeypatch, capsys):
    # kubectl 실패(인증 만료 등)는 트레이스백이 아니라 exit 1 + 정리된 stderr 메시지여야 한다
    # (collectors/token-metrics/tools/rerun.py fix1과 동일 관례)
    def fake_kubectl(context, args, *, capture=False, input_data=None):
        if args[:2] == ["get", "jobs"]:
            raise subprocess.CalledProcessError(1, ["kubectl", "get", "jobs"],
                                                stderr="error: You must be logged in to the server")
        raise AssertionError(f"unexpected kubectl {args}")

    monkeypatch.setattr(rerun, "kubectl", fake_kubectl)
    monkeypatch.setattr(rerun, "_now_kst", lambda: kst(11, 0))
    rc = rerun.main(["--context", "c", "--force"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "[ERROR] kubectl 실패 (rc=1): kubectl get jobs" in err
    assert "You must be logged in to the server" in err


# ── kubectl()/wait_job() 계약 (SHOULD-3(b)) ───────────────────────────────────────────────

def test_kubectl_builds_expected_argv_and_calls_subprocess_run(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return types.SimpleNamespace(stdout="{}")

    monkeypatch.setattr(rerun.subprocess, "run", fake_run)
    rerun.kubectl("homelab", ["get", "jobs", "-n", "monitoring", "-o", "json"],
                  capture=True, input_data=None)
    assert len(calls) == 1
    cmd, kwargs = calls[0]
    assert cmd == ["kubectl", "--context=homelab", "--insecure-skip-tls-verify",
                   "get", "jobs", "-n", "monitoring", "-o", "json"]
    assert kwargs["check"] is True
    assert kwargs["text"] is True
    assert kwargs["capture_output"] is True
    assert kwargs["input"] is None


def test_wait_job_returns_true_on_complete_condition(monkeypatch):
    calls = {"get_job": 0}

    def fake_kubectl(context, args, *, capture=False, input_data=None):
        if args[:2] == ["get", "job"]:
            calls["get_job"] += 1
            conditions = [] if calls["get_job"] == 1 else [{"type": "Complete", "status": "True"}]
            return types.SimpleNamespace(stdout=json.dumps({"status": {"conditions": conditions}}))
        if args[:2] == ["get", "pods"]:
            return types.SimpleNamespace(stdout="")                        # 파드 없음 — 로그 스트리밍 스킵
        raise AssertionError(f"unexpected kubectl call: {args}")

    monkeypatch.setattr(rerun, "kubectl", fake_kubectl)
    monkeypatch.setattr(rerun.time, "sleep", lambda s: None)
    assert rerun.wait_job("c", "ns", "job-1", 3600) is True
    assert calls["get_job"] == 2                                           # 2번째 폴링에서 Complete


def test_wait_job_returns_false_on_failed_condition(monkeypatch):
    def fake_kubectl(context, args, *, capture=False, input_data=None):
        if args[:2] == ["get", "job"]:
            return types.SimpleNamespace(stdout=json.dumps(
                {"status": {"conditions": [{"type": "Failed", "status": "True"}]}}))
        if args[:2] == ["get", "pods"]:
            return types.SimpleNamespace(stdout="")
        raise AssertionError(f"unexpected kubectl call: {args}")

    monkeypatch.setattr(rerun, "kubectl", fake_kubectl)
    monkeypatch.setattr(rerun.time, "sleep", lambda s: None)
    assert rerun.wait_job("c", "ns", "job-1", 3600) is False
