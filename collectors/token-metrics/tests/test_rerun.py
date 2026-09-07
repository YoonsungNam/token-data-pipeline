"""tools/rerun.py 계약 테스트 (§5.6 rerun · §6.3 창/체인).

kubectl·시간·Job 대기는 전부 페이크 — 클러스터 없이 순서·이름·종료코드를 고정한다.
"""
import datetime as dt
import importlib.util
import json
import pathlib
import re
import subprocess
import sys

import pytest
import yaml

MODULE_ROOT = pathlib.Path(__file__).resolve().parents[1]
_RERUN_PATH = MODULE_ROOT / "tools" / "rerun.py"
spec = importlib.util.spec_from_file_location("rerun", _RERUN_PATH)
rerun = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rerun)

KST = dt.timezone(dt.timedelta(hours=9))
D = dt.date.fromisoformat


def cronjob_obj():
    """T8 실 매니페스트 + 서버 필드(uid/resourceVersion) — kubectl get -o json 모사."""
    with open(MODULE_ROOT / "k8s" / "base" / "cronjob.yaml", encoding="utf-8") as fh:
        obj = yaml.safe_load(fh)
    obj["metadata"].update({"namespace": "monitoring", "resourceVersion": "123", "uid": "x"})
    return obj


# ---- 상수 (§5.6) --------------------------------------------------------------

def test_cronjob_constant_is_metrics_collector():
    assert rerun.CRONJOB == "token-metrics-collector"
    assert rerun.MART_CRONJOB == "token-mart-metrics"
    assert rerun.MART_RERUN == "mart/token-metrics/tools/rerun.py"
    assert rerun.DEFAULT_CHUNK_DAYS == 7
    assert rerun.CHUNK_DAYS_MAX == 15                                      # §4.0 뮤테이션 예산 45 = 15일 × 3
    assert rerun.WINDOW_OPEN_HHMM == (10, 50)
    assert rerun.KST.utcoffset(None) == dt.timedelta(hours=9)
    # REPO_ROOT = tools/rerun.py 기준 3단계 위 = 레포 루트 (collectors/token-metrics/tools)
    assert (rerun.REPO_ROOT / "collectors" / "token-metrics" / "tools" / "rerun.py") == _RERUN_PATH


def test_timeout_single():
    # §5.6: TIMEOUT_SINGLE_S = 서버 activeDeadlineSeconds 3000 + 폴링 마진 600
    assert rerun.TIMEOUT_SINGLE_S == 3600
    assert rerun.TIMEOUT_SINGLE_S == \
        cronjob_obj()["spec"]["jobTemplate"]["spec"]["activeDeadlineSeconds"] + 600


# ---- split_chunks ------------------------------------------------------------

def test_split_chunks_7_days():
    assert rerun.split_chunks(D("2026-09-01"), D("2026-09-20"), 7) == [
        (D("2026-09-01"), D("2026-09-07")),
        (D("2026-09-08"), D("2026-09-14")),
        (D("2026-09-15"), D("2026-09-20")),
    ]
    assert rerun.split_chunks(D("2026-09-10"), D("2026-09-10"), 7) == \
        [(D("2026-09-10"), D("2026-09-10"))]
    assert rerun.split_chunks(D("2026-09-01"), D("2026-09-07"), 7) == \
        [(D("2026-09-01"), D("2026-09-07"))]
    assert rerun.split_chunks(D("2026-09-01"), D("2026-09-08"), 7)[-1] == \
        (D("2026-09-08"), D("2026-09-08"))
    assert rerun.split_chunks(D("2026-09-01"), D("2026-09-03"), 1) == [
        (D("2026-09-01"), D("2026-09-01")),
        (D("2026-09-02"), D("2026-09-02")),
        (D("2026-09-03"), D("2026-09-03")),
    ]


def test_split_chunks_rejects_bad_input():
    with pytest.raises(ValueError):
        rerun.split_chunks(D("2026-09-10"), D("2026-09-01"), 7)
    with pytest.raises(ValueError):
        rerun.split_chunks(D("2026-09-01"), D("2026-09-02"), 0)


# ---- Job 스펙·커맨드 -----------------------------------------------------------

def test_build_job_spec_command_override():
    obj = cronjob_obj()
    cmd = ["python", "-m", "app.main", "--from", "2026-09-01", "--to", "2026-09-07", "--replace"]
    job = rerun.build_job_spec(obj, "j", cmd)
    assert job["apiVersion"] == "batch/v1" and job["kind"] == "Job"
    # 서버 필드(uid/resourceVersion/namespace) 제거 — name + 라벨만
    assert job["metadata"] == {"name": "j",
                               "labels": {"app": "token-metrics-collector", "rerun": "1"}}
    assert job["spec"]["activeDeadlineSeconds"] == 3000       # override 없음 — CronJob 값 상속
    assert job["spec"]["backoffLimit"] == 0
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["name"] == "token-metrics-collector"
    assert container["command"] == cmd
    assert obj == cronjob_obj()                                 # deepcopy — 원본 불변


def test_job_names_suffix_index():
    assert rerun.job_names("token-metrics-collector", 1700000000, 3) == [
        "token-metrics-collector-rerun-1700000000-0",
        "token-metrics-collector-rerun-1700000000-1",
        "token-metrics-collector-rerun-1700000000-2",
    ]
    assert rerun.job_names("token-metrics-collector-verify", 1, 1) == \
        ["token-metrics-collector-verify-rerun-1-0"]
    assert all(len(n) <= 63 for n in rerun.job_names("token-metrics-collector-verify", 9999999999, 100))


def test_collect_command_variants():
    base = ["python", "-m", "app.main", "--from", "2026-09-01", "--to", "2026-09-07"]
    assert rerun.build_collect_command("2026-09-01", "2026-09-07", None, False) == base
    assert rerun.build_collect_command("2026-09-01", "2026-09-07", None, True) == base + ["--replace"]
    assert rerun.build_collect_command("2026-09-01", "2026-09-07", "Mock Service A", False) == \
        base + ["--service", "Mock Service A"]
    assert rerun.build_collect_command("2026-09-01", "2026-09-07", "Mock Service A", True) == \
        base + ["--service", "Mock Service A", "--replace"]


def test_chain_mart_command_same_range():
    # §6.3: 청크 분할 전 전체 --from/--to를 그대로 전파 (수집기 스킵 날짜 포함)
    assert rerun.build_mart_command("c", "monitoring", "2026-09-01", "2026-09-20", 7) == [
        "python3", "mart/token-metrics/tools/rerun.py",
        "--context", "c", "--namespace", "monitoring",
        "--from", "2026-09-01", "--to", "2026-09-20", "--chunk-days", "7",
    ]
    # company-verify → 6c --cronjob token-mart-metrics-verify, --force-window → 6c --force (창만 생략)
    assert rerun.build_mart_command("c", "monitoring", "2026-09-01", "2026-09-20", 3,
                                    mart_cronjob="token-mart-metrics-verify", force=True) == [
        "python3", "mart/token-metrics/tools/rerun.py",
        "--context", "c", "--namespace", "monitoring", "--cronjob", "token-mart-metrics-verify",
        "--from", "2026-09-01", "--to", "2026-09-20", "--chunk-days", "3", "--force",
    ]
    assert rerun.mart_cronjob_for("token-metrics-collector") == "token-mart-metrics"
    assert rerun.mart_cronjob_for("token-metrics-collector-verify") == "token-mart-metrics-verify"


def test_cronjob_default_and_override():
    args = rerun.build_arg_parser().parse_args(
        ["--context", "c", "--from", "2026-09-01", "--to", "2026-09-01"])
    assert args.cronjob == rerun.CRONJOB == "token-metrics-collector"
    assert args.namespace == "monitoring" and args.chunk_days == 7
    assert args.replace is False and args.chain_mart is False and args.force_window is False
    args = rerun.build_arg_parser().parse_args(
        ["--context", "c", "--from", "2026-09-01", "--to", "2026-09-01",
         "--cronjob", "token-metrics-collector-verify", "--chunk-days", "3", "--replace"])
    assert args.cronjob == "token-metrics-collector-verify"
    assert args.chunk_days == 3 and args.replace is True


# ---- 실행 창 (§6.3: 10:50 KST 이후 + 활성 token-mart-metrics Job 0) --------------

def test_window_before_1050_rejected():
    assert rerun.check_window(dt.datetime(2026, 9, 4, 10, 49, tzinfo=KST), 0) == "window_closed"
    assert rerun.check_window(dt.datetime(2026, 9, 4, 10, 50, tzinfo=KST), 0) is None
    assert rerun.check_window(dt.datetime(2026, 9, 4, 11, 0, tzinfo=KST), 0) is None
    assert rerun.check_window(dt.datetime(2026, 9, 4, 0, 5, tzinfo=KST), 0) == "window_closed"


def test_window_active_mart_job_rejected():
    assert rerun.check_window(dt.datetime(2026, 9, 4, 11, 0, tzinfo=KST), 1) == "mart_job_active"
    # 두 조건 동시 위반이면 창이 먼저 (운영자가 먼저 고칠 수 있는 원인)
    assert rerun.check_window(dt.datetime(2026, 9, 4, 9, 0, tzinfo=KST), 1) == "window_closed"


def test_window_requires_aware_kst():
    with pytest.raises(ValueError):
        rerun.check_window(dt.datetime(2026, 9, 4, 11, 0), 0)          # naive 금지
    # UTC aware 입력은 KST로 환산해 판정 (01:55Z = 10:55 KST → 열림)
    assert rerun.check_window(dt.datetime(2026, 9, 4, 1, 55, tzinfo=dt.timezone.utc), 0) is None
    assert rerun.check_window(dt.datetime(2026, 9, 4, 1, 45, tzinfo=dt.timezone.utc), 0) == "window_closed"


def test_now_kst_is_aware_kst():
    now = rerun.now_kst()
    assert now.tzinfo is not None and now.utcoffset() == dt.timedelta(hours=9)


def test_count_active_mart_jobs_filters_by_owner_and_prefix(monkeypatch):
    items = [
        {"metadata": {"name": "token-mart-metrics-29300000",
                      "ownerReferences": [{"kind": "CronJob", "name": "token-mart-metrics"}]},
         "status": {"active": 1}},
        {"metadata": {"name": "token-mart-metrics-rerun-1700000000-0"},        # rerun Job (owner 없음)
         "status": {"active": 1}},
        {"metadata": {"name": "token-metrics-collector-29300000",             # 수집기 자신 — 제외
                      "ownerReferences": [{"kind": "CronJob", "name": "token-metrics-collector"}]},
         "status": {"active": 1}},
        {"metadata": {"name": "token-mart-metrics-29299999",                  # 완료 — active 키 없음
                      "ownerReferences": [{"kind": "CronJob", "name": "token-mart-metrics"}]},
         "status": {"succeeded": 1}},
    ]
    calls = []

    def fake(context, args, *, capture=False, input_data=None):
        calls.append((context, list(args), capture))
        return subprocess.CompletedProcess(args, 0, stdout=json.dumps({"items": items}), stderr="")

    monkeypatch.setattr(rerun, "kubectl", fake)
    assert rerun.count_active_mart_jobs("c", "monitoring", "token-mart-metrics") == 2
    assert calls == [("c", ["get", "jobs", "-n", "monitoring", "-o", "json"], True)]


# ---- main(): 순차 청크 · 중단 · 종료코드 ------------------------------------------

class FakeK8s:
    """rerun.kubectl 대체 — get cronjob은 fixture 반환, apply -f -는 본문(Job) 기록."""

    def __init__(self, cronjob, events):
        self.cronjob = cronjob
        self.events = events           # 공유 이벤트 로그: ("apply", name) / ("wait", name)
        self.applied = []
        self.get_cronjob_calls = 0

    def __call__(self, context, args, *, capture=False, input_data=None):
        args = list(args)
        if args[:2] == ["get", "cronjob"]:
            self.get_cronjob_calls += 1
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(self.cronjob), stderr="")
        if args[0] == "apply":
            assert args[1:] == ["-n", "monitoring", "-f", "-"]
            job = json.loads(input_data)
            self.applied.append(job)
            self.events.append(("apply", job["metadata"]["name"]))
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected kubectl {args}")


def _run_main(monkeypatch, argv, *, wait_results, now, active=0):
    events = []
    k8s = FakeK8s(cronjob_obj(), events)
    results = list(wait_results)
    waited = []

    def fake_wait(context, namespace, job_name, timeout_s):
        events.append(("wait", job_name))
        waited.append((job_name, timeout_s))
        return results.pop(0)

    monkeypatch.setattr(rerun, "kubectl", k8s)
    monkeypatch.setattr(rerun, "wait_job", fake_wait)
    monkeypatch.setattr(rerun, "count_active_mart_jobs", lambda c, n, m: active)
    monkeypatch.setattr(rerun, "now_kst", lambda: now)
    rc = rerun.main(argv)
    return rc, k8s, waited, events


RANGE = ["--from", "2026-09-01", "--to", "2026-09-20", "--chunk-days", "7"]
NOON = dt.datetime(2026, 9, 4, 12, 0, tzinfo=KST)


def test_main_sequential_chunks_and_stop_on_failure(monkeypatch, capsys):
    rc, k8s, waited, events = _run_main(
        monkeypatch, ["--context", "c"] + RANGE + ["--force-window"],
        wait_results=[True, True, True], now=dt.datetime(2026, 9, 4, 10, 0, tzinfo=KST))
    assert rc == 0
    names = [j["metadata"]["name"] for j in k8s.applied]
    assert len(names) == 3
    assert all(re.fullmatch(r"token-metrics-collector-rerun-\d+-[012]", n) for n in names)
    assert [n.rsplit("-", 1)[1] for n in names] == ["0", "1", "2"]
    assert len({n.rsplit("-", 1)[0] for n in names}) == 1              # 같은 epoch
    # apply → wait → apply → wait … (순차; 다음 Job은 앞 Job 완료 후 생성)
    assert events == [("apply", names[0]), ("wait", names[0]),
                      ("apply", names[1]), ("wait", names[1]),
                      ("apply", names[2]), ("wait", names[2])]
    assert [t for _, t in waited] == [3600, 3600, 3600]
    cmds = [j["spec"]["template"]["spec"]["containers"][0]["command"] for j in k8s.applied]
    assert cmds[0] == ["python", "-m", "app.main", "--from", "2026-09-01", "--to", "2026-09-07"]
    assert cmds[1] == ["python", "-m", "app.main", "--from", "2026-09-08", "--to", "2026-09-14"]
    assert cmds[2] == ["python", "-m", "app.main", "--from", "2026-09-15", "--to", "2026-09-20"]
    assert all(j["spec"]["activeDeadlineSeconds"] == 3000 for j in k8s.applied)
    assert k8s.get_cronjob_calls == 1                                   # CronJob 조회 1회
    out = capsys.readouterr().out
    assert "[WARN] 실행 창 검사 생략(--force-window)" in out              # 10:00인데 강제
    assert "[INFO] 청크 1/3: 2026-09-01 .. 2026-09-07 → Job " + names[0] in out
    assert "[NEXT] collectors rerun 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3):" in out
    assert ("  python3 mart/token-metrics/tools/rerun.py --context c --namespace monitoring "
            "--from 2026-09-01 --to 2026-09-20 --chunk-days 7 --force\n") in out   # --force-window → 6c --force

    # 2번째 청크 실패 → 3번째 apply 없음, exit 1, 재시도 범위 = 실패 청크 시작 ~ 원래 --to
    rc, k8s, waited, events = _run_main(
        monkeypatch, ["--context", "c"] + RANGE + ["--replace"], wait_results=[True, False], now=NOON)
    assert rc == 1
    assert len(k8s.applied) == 2 and len(waited) == 2
    assert k8s.applied[1]["spec"]["template"]["spec"]["containers"][0]["command"][-1] == "--replace"
    captured = capsys.readouterr()
    assert "[ERROR] 청크 2/3 실패 — 이후 청크 중단; 재시도: --from 2026-09-08 --to 2026-09-20" in captured.err
    assert "[NEXT]" not in captured.out                                 # 실패 시 mart 안내 없음


def test_main_service_and_replace_propagate_to_every_chunk(monkeypatch):
    rc, k8s, _, _ = _run_main(
        monkeypatch, ["--context", "c"] + RANGE + ["--service", "Mock Service A", "--replace"],
        wait_results=[True, True, True], now=NOON)
    assert rc == 0
    for job in k8s.applied:
        cmd = job["spec"]["template"]["spec"]["containers"][0]["command"]
        assert cmd[-3:] == ["--service", "Mock Service A", "--replace"]
        assert job["metadata"]["labels"] == {"app": "token-metrics-collector", "rerun": "1"}


def test_main_window_closed_exit_3(monkeypatch, capsys):
    rc, k8s, waited, _ = _run_main(
        monkeypatch, ["--context", "c"] + RANGE, wait_results=[],
        now=dt.datetime(2026, 9, 4, 10, 0, tzinfo=KST))
    assert rc == 3 and k8s.applied == [] and waited == [] and k8s.get_cronjob_calls == 0
    err = capsys.readouterr().err
    assert ("[ERROR] 실행 창 밖: window_closed — KST 10:50 이후·활성 token-mart-metrics Job 0일 때 "
            "재시도 (--force-window로 강제)") in err

    rc, k8s, _, _ = _run_main(monkeypatch, ["--context", "c"] + RANGE, wait_results=[],
                              now=NOON, active=1)
    assert rc == 3 and k8s.applied == []
    assert "[ERROR] 실행 창 밖: mart_job_active" in capsys.readouterr().err


def test_usage_errors():
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "c", "--to", "2026-09-01"])                       # --from 없음
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "c", "--from", "2026-09-01"])                     # --to 없음
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "c", "--from", "2026-09-01", "--to", "2026-09-02", "--chunk-days", "0"])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:                                          # 상한 15 초과 (뮤테이션 예산 45)
        rerun.main(["--context", "c", "--from", "2026-09-01", "--to", "2026-09-02", "--chunk-days", "16"])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "c", "--from", "2026-09-10", "--to", "2026-09-01"])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "c", "--from", "2026/09/01", "--to", "2026-09-02"])
    assert e.value.code == 2


# ---- --chain-mart ---------------------------------------------------------------

def test_chain_mart_missing_path_exit_1(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(rerun, "REPO_ROOT", tmp_path)                    # mart/token-metrics 부재
    rc, k8s, _, _ = _run_main(monkeypatch, ["--context", "c"] + RANGE + ["--chain-mart"],
                              wait_results=[True, True, True], now=NOON)
    assert rc == 1 and len(k8s.applied) == 3                              # 수집 3청크는 완료
    captured = capsys.readouterr()
    assert "[NEXT]" in captured.out                                        # 안내는 먼저 출력
    assert "[ERROR] --chain-mart: mart/token-metrics/tools/rerun.py 가 아직 없습니다 (Plan 6c 전)" in captured.err


def test_chain_mart_calls_mart_rerun_with_full_range(monkeypatch, tmp_path):
    mart_path = tmp_path / "mart" / "token-metrics" / "tools" / "rerun.py"
    mart_path.parent.mkdir(parents=True)
    mart_path.write_text("", encoding="utf-8")
    monkeypatch.setattr(rerun, "REPO_ROOT", tmp_path)
    called = []

    def fake_call(argv):
        called.append(list(argv))
        return 7                                                           # mart rerun 반환값 그대로

    monkeypatch.setattr(rerun.subprocess, "call", fake_call)
    rc, _, _, _ = _run_main(monkeypatch, ["--context", "c"] + RANGE + ["--chain-mart"],
                            wait_results=[True, True, True], now=NOON)
    assert rc == 7
    assert called == [[sys.executable, str(mart_path),
                       "--context", "c", "--namespace", "monitoring",
                       "--from", "2026-09-01", "--to", "2026-09-20", "--chunk-days", "7"]]


def test_chain_mart_propagates_verify_cronjob_and_force(monkeypatch, tmp_path, capsys):
    # --cronjob …-verify → mart --cronjob token-mart-metrics-verify; --force-window → mart --force
    mart_path = tmp_path / "mart" / "token-metrics" / "tools" / "rerun.py"
    mart_path.parent.mkdir(parents=True)
    mart_path.write_text("", encoding="utf-8")
    monkeypatch.setattr(rerun, "REPO_ROOT", tmp_path)
    called = []
    monkeypatch.setattr(rerun.subprocess, "call", lambda argv: called.append(list(argv)) or 0)
    rc, k8s, _, _ = _run_main(
        monkeypatch, ["--context", "c", "--cronjob", "token-metrics-collector-verify",
                      "--from", "2026-09-01", "--to", "2026-09-20", "--chunk-days", "15",
                      "--chain-mart", "--force-window"],
        wait_results=[True, True], now=dt.datetime(2026, 9, 4, 9, 0, tzinfo=KST))
    assert rc == 0
    assert len(k8s.applied) == 2                                           # 20일 / 15일 → 2청크
    assert called == [[sys.executable, str(mart_path),
                       "--context", "c", "--namespace", "monitoring", "--cronjob", "token-mart-metrics-verify",
                       "--from", "2026-09-01", "--to", "2026-09-20", "--chunk-days", "15", "--force"]]
    out = capsys.readouterr().out
    assert ("  python3 mart/token-metrics/tools/rerun.py --context c --namespace monitoring "
            "--cronjob token-mart-metrics-verify --from 2026-09-01 --to 2026-09-20 --chunk-days 15 --force\n") in out
