"""tools/manual_load.py 계약 테스트 (§5.5 전달 경로 P0 = k8s Job).

kubectl·시간·Job 대기는 전부 페이크 — 클러스터 없이 ConfigMap 본문·Job 스펙(/manual 볼륨 [2]·command)·
호출 순서(create → get cronjob → apply → wait → delete)·finally 삭제 보장·종료코드를 고정한다.
"""
import datetime as dt
import importlib.util
import json
import pathlib
import re
import subprocess

import pytest
import yaml

MODULE_ROOT = pathlib.Path(__file__).resolve().parents[1]
REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
TEMPLATES = REPO_ROOT / "docs" / "templates"
_ML_PATH = MODULE_ROOT / "tools" / "manual_load.py"
spec = importlib.util.spec_from_file_location("manual_load", _ML_PATH)
ml = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ml)

KST = dt.timezone(dt.timedelta(hours=9))
NOW = dt.datetime(2026, 9, 4, 11, 30, 0, tzinfo=KST)
TS = "20260904113000"
CM_NAME = "token-metrics-manual-" + TS
JOB_NAME = "token-metrics-collector-manual-" + TS

GPU_HEADER = "date,service,model,gpuType,category,gpuCount,gpuHours"
SERVING_HEADER = "date,service,model,metric,name,unit,p50,p90,p95,p99"
ENGINE_HEADER = "service,engine_type,engine_version"
GPU_ROW = "2026-08-26,Mock Service A,claude-sonnet-5,H100,serving,4,96.0"
SERVING_ROW = "2026-08-26,Mock Service A,claude-sonnet-5,outputTps,,,41.0,,,"
ENGINE_ROW = "Mock Service A,vllm,0.8.4"


def cronjob_obj():
    """T8 실 매니페스트 + 서버 필드(uid/resourceVersion/namespace) — kubectl get -o json 모사."""
    with open(MODULE_ROOT / "k8s" / "base" / "cronjob.yaml", encoding="utf-8") as fh:
        obj = yaml.safe_load(fh)
    obj["metadata"].update({"namespace": "monitoring", "resourceVersion": "123", "uid": "x"})
    return obj


def write_inputs(tmp_path, *, engine=False, gpu_bom=False):
    """최소 입력 3파일(헤더 + 예시 1행). gpu_bom=True 면 엑셀식 UTF-8 BOM 을 앞에 붙인다."""
    gpu = tmp_path / "gpu_manual_metrics.csv"
    serving = tmp_path / "serving_manual_metrics.csv"
    gpu_text = GPU_HEADER + "\n" + GPU_ROW + "\n"
    gpu.write_bytes((("﻿" if gpu_bom else "") + gpu_text).encode("utf-8"))
    serving.write_text(SERVING_HEADER + "\n" + SERVING_ROW + "\n", encoding="utf-8")
    paths = {"gpu": gpu, "serving": serving, "engine": None}
    if engine:
        eng = tmp_path / "engine_manual_metrics.csv"
        eng.write_text(ENGINE_HEADER + "\n" + ENGINE_ROW + "\n", encoding="utf-8")
        paths["engine"] = eng
    return paths


# ---- 상수 (§5.5 · §5.6) ---------------------------------------------------------

def test_constants():
    assert ml.CRONJOB == "token-metrics-collector"
    assert ml.CONFIGMAP_PREFIX == "token-metrics-manual-"
    assert ml.TS_FORMAT == "%Y%m%d%H%M%S"
    assert ml.MOUNT_PATH == "/manual" and ml.VOLUME_NAME == "manual"
    assert ml.FILE_KEYS == ("gpu.csv", "serving.csv", "engine.csv")
    assert ml.LABELS == {"app": "token-metrics-collector", "manual": "1"}
    assert ml.MAX_CONFIGMAP_BYTES == 900_000
    assert ml.POLL_S == 10
    assert ml.TIMEOUT_S == 3600 == \
        cronjob_obj()["spec"]["jobTemplate"]["spec"]["activeDeadlineSeconds"] + 600
    assert ml.KST.utcoffset(None) == dt.timedelta(hours=9)
    assert ml.MART_RERUN == "mart/token-metrics/tools/rerun.py"


# ---- 이름 (DNS-1123) ------------------------------------------------------------

def test_configmap_name_format():
    assert ml.timestamp(NOW) == TS
    assert ml.configmap_name(NOW) == "token-metrics-manual-20260904113000"
    assert re.fullmatch(r"^[a-z0-9-]+$", ml.configmap_name(NOW))
    # UTC aware 입력은 KST 로 환산 (02:30Z = 11:30 KST)
    assert ml.configmap_name(dt.datetime(2026, 9, 4, 2, 30, 0, tzinfo=dt.timezone.utc)) == CM_NAME
    with pytest.raises(ValueError):
        ml.timestamp(dt.datetime(2026, 9, 4, 11, 30, 0))                # naive 금지 (KST 규율)


def test_job_name_with_verify_suffix_fits_63():
    assert ml.job_name("token-metrics-collector", TS) == JOB_NAME
    name = ml.job_name("token-metrics-collector-verify", TS)
    assert name == "token-metrics-collector-verify-manual-20260904113000"
    assert len(name) <= 63 and re.fullmatch(r"^[a-z0-9-]+$", name)


def test_now_kst_is_aware_kst():
    now = ml.now_kst()
    assert now.tzinfo is not None and now.utcoffset() == dt.timedelta(hours=9)


# ---- ConfigMap 본문 --------------------------------------------------------------

def test_configmap_body_from_files(tmp_path):
    p = write_inputs(tmp_path, gpu_bom=True)
    files = ml.read_manual_files(p["gpu"], p["serving"], None)
    assert list(files) == ["gpu.csv", "serving.csv"]                    # engine 없음 · 키 순서
    assert files["gpu.csv"].startswith("date,service,")                  # BOM 제거
    assert "﻿" not in files["gpu.csv"]
    assert files["serving.csv"].splitlines()[1] == SERVING_ROW
    cm = ml.build_configmap(CM_NAME, files)
    assert cm["apiVersion"] == "v1" and cm["kind"] == "ConfigMap"
    assert cm["metadata"] == {"name": CM_NAME,
                              "labels": {"app": "token-metrics-collector", "manual": "1"}}
    assert set(cm["data"]) == {"gpu.csv", "serving.csv"}
    assert cm["metadata"]["labels"]["manual"] == "1"
    assert cm["data"]["gpu.csv"] == files["gpu.csv"]                     # 값 그대로 (검증·가공 없음)
    json.dumps(cm)                                                       # kubectl -f - 로 보낼 수 있는 JSON


def test_configmap_crlf_normalized_and_engine_key(tmp_path):
    p = write_inputs(tmp_path, engine=True)
    p["gpu"].write_bytes((GPU_HEADER + "\r\n" + GPU_ROW + "\r\n").encode("utf-8"))   # 엑셀 CRLF
    files = ml.read_manual_files(p["gpu"], p["serving"], p["engine"])
    assert list(files) == ["gpu.csv", "serving.csv", "engine.csv"]
    assert files["gpu.csv"] == GPU_HEADER + "\n" + GPU_ROW + "\n"        # universal newline → LF
    assert files["engine.csv"].splitlines()[0] == ENGINE_HEADER
    assert ml.total_bytes(files) == sum(len(v.encode("utf-8")) for v in files.values())


def test_read_manual_files_missing_raises(tmp_path):
    p = write_inputs(tmp_path)
    with pytest.raises(FileNotFoundError):
        ml.read_manual_files(tmp_path / "nope.csv", p["serving"], None)
    with pytest.raises(FileNotFoundError):
        ml.read_manual_files(p["gpu"], p["serving"], tmp_path / "nope_engine.csv")


def test_build_configmap_rejects_unknown_key():
    with pytest.raises(ValueError):
        ml.build_configmap(CM_NAME, {"gpu.csv": "x", "extra.csv": "y"})


# ---- Job 스펙 (/manual 볼륨 [2] · command override) ------------------------------

def test_job_spec_has_manual_volume_and_command():
    obj = cronjob_obj()
    cmd = ml.build_manual_command("2026-08-26", "2026-08-31", engine=False, service=None,
                                  replace=False, generated_at=None)
    job = ml.build_job_spec(obj, JOB_NAME, cmd, CM_NAME)
    assert job["apiVersion"] == "batch/v1" and job["kind"] == "Job"
    assert job["metadata"] == {"name": JOB_NAME,
                               "labels": {"app": "token-metrics-collector", "manual": "1"}}
    assert "uid" not in job["metadata"] and "resourceVersion" not in job["metadata"]
    assert job["spec"]["activeDeadlineSeconds"] == 3000                  # override 없음 — CronJob 값 상속
    assert job["spec"]["backoffLimit"] == 0
    pod = job["spec"]["template"]["spec"]
    assert pod["volumes"][0]["name"] == "endpoints"                      # 기존 볼륨 보존 (index 계약)
    assert pod["volumes"][1]["name"] == "ca-bundle"
    assert pod["volumes"][2] == {"name": "manual",
                                 "configMap": {"name": "token-metrics-manual-20260904113000"}}
    assert len(pod["volumes"]) == 3
    container = pod["containers"][0]
    assert container["name"] == "token-metrics-collector"
    assert container["volumeMounts"][0]["mountPath"] == "/etc/token-metrics"
    assert container["volumeMounts"][2] == {"name": "manual", "mountPath": "/manual",
                                            "readOnly": True}
    assert container["volumeMounts"][2]["readOnly"] is True
    assert container["command"][:7] == ["python", "-m", "app.main",
                                        "--manual-gpu", "/manual/gpu.csv",
                                        "--manual-serving", "/manual/serving.csv"]
    assert container["command"] == cmd
    assert obj == cronjob_obj()                                          # deepcopy — 원본 불변
    json.dumps(job)


def test_job_spec_rejects_double_manual_volume():
    obj = cronjob_obj()
    pod = obj["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    pod["volumes"].append({"name": "manual", "configMap": {"name": "stale"}})
    with pytest.raises(ValueError):
        ml.build_job_spec(obj, JOB_NAME, ["python", "-m", "app.main"], CM_NAME)


def test_engine_optional():
    base = ["python", "-m", "app.main", "--manual-gpu", "/manual/gpu.csv",
            "--manual-serving", "/manual/serving.csv"]
    rng = ["--from", "2026-08-26", "--to", "2026-08-31"]
    cmd = ml.build_manual_command("2026-08-26", "2026-08-31", engine=False, service=None,
                                  replace=False, generated_at=None)
    assert cmd == base + rng
    assert "--manual-engine" not in cmd
    cmd = ml.build_manual_command("2026-08-26", "2026-08-31", engine=True, service=None,
                                  replace=False, generated_at=None)
    assert cmd == base + ["--manual-engine", "/manual/engine.csv"] + rng
    assert cmd.index("--manual-engine") < cmd.index("--from")            # engine 이 --from 앞
    cmd = ml.build_manual_command("2026-08-26", "2026-08-31", engine=True, service="Mock Service A",
                                  replace=True, generated_at="2026-09-01T09:00:00+09:00")
    assert cmd == base + ["--manual-engine", "/manual/engine.csv"] + rng + \
        ["--service", "Mock Service A", "--replace", "--generated-at", "2026-09-01T09:00:00+09:00"]
    assert cmd[-2:] == ["--generated-at", "2026-09-01T09:00:00+09:00"]  # 마지막 2원소
    cmd = ml.build_manual_command("2026-08-26", "2026-08-26", engine=False, service=None,
                                  replace=True, generated_at=None)
    assert cmd[-1] == "--replace" and "--service" not in cmd and "--generated-at" not in cmd


# ---- main(): create → get cronjob → apply → wait → finally delete -----------------

class FakeK8s:
    """ml.kubectl 대체 — 호출 인자 목록을 기록. create/apply -f - 본문(JSON)은 kind 별로 보관."""

    def __init__(self, cronjob, *, delete_fails=False, job_apply_fails=False):
        self.cronjob = cronjob
        self.calls = []                # list[list[str]] — kubectl 인자 그대로
        self.created = []              # create -f - 본문 (ConfigMap)
        self.applied = []              # apply -f - 본문 (Job)
        self.delete_fails = delete_fails
        self.job_apply_fails = job_apply_fails

    def __call__(self, context, args, *, capture=False, input_data=None):
        args = list(args)
        assert context == "c"
        self.calls.append(args)
        if args[:2] == ["get", "cronjob"]:
            assert args == ["get", "cronjob", "token-metrics-collector", "-n", "monitoring",
                            "-o", "json"]
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(self.cronjob), stderr="")
        if args[0] == "create":
            assert args[1:] == ["-n", "monitoring", "-f", "-"]
            body = json.loads(input_data)
            assert body["kind"] == "ConfigMap"
            self.created.append(body)
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[0] == "apply":
            assert args[1:] == ["-n", "monitoring", "-f", "-"]
            body = json.loads(input_data)
            assert body["kind"] == "Job"
            if self.job_apply_fails:
                raise subprocess.CalledProcessError(1, ["kubectl"] + args)
            self.applied.append(body)
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[:2] == ["delete", "configmap"]:
            if self.delete_fails:
                raise subprocess.CalledProcessError(1, ["kubectl"] + args)
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected kubectl {args}")


def _run_main(monkeypatch, argv, *, wait_result, **k8s_kw):
    k8s = FakeK8s(cronjob_obj(), **k8s_kw)
    waited = []

    def fake_wait(context, namespace, job_name, timeout_s):
        waited.append((context, namespace, job_name, timeout_s))
        k8s.calls.append(["<wait>", job_name])           # 순서 검증용 마커 (kubectl 호출 아님)
        return wait_result

    monkeypatch.setattr(ml, "kubectl", k8s)
    monkeypatch.setattr(ml, "wait_job", fake_wait)
    monkeypatch.setattr(ml, "now_kst", lambda: NOW)
    rc = ml.main(argv)
    return rc, k8s, waited


def _argv(p, *extra):
    argv = ["--context", "c", "--from", "2026-08-26", "--to", "2026-08-31",
            "--gpu", str(p["gpu"]), "--serving", str(p["serving"])]
    if p["engine"] is not None:
        argv += ["--engine", str(p["engine"])]
    return argv + list(extra)


DELETE_CALL = ["delete", "configmap", CM_NAME, "-n", "monitoring", "--ignore-not-found"]


def test_configmap_deleted_on_success(monkeypatch, tmp_path, capsys):
    p = write_inputs(tmp_path, engine=True)
    rc, k8s, waited = _run_main(monkeypatch, _argv(p), wait_result=True)
    assert rc == 0
    # 호출 순서: create(ConfigMap) → get cronjob → apply(Job) → wait → delete configmap (마지막)
    assert k8s.calls == [["create", "-n", "monitoring", "-f", "-"],
                         ["get", "cronjob", "token-metrics-collector", "-n", "monitoring", "-o", "json"],
                         ["apply", "-n", "monitoring", "-f", "-"],
                         ["<wait>", JOB_NAME],
                         DELETE_CALL]
    assert sum(1 for c in k8s.calls if c[:2] == ["delete", "configmap"]) == 1
    assert waited == [("c", "monitoring", JOB_NAME, 3600)]              # TIMEOUT_S 기본
    cm, job = k8s.created[0], k8s.applied[0]
    assert cm["metadata"]["name"] == CM_NAME
    assert list(cm["data"]) == ["gpu.csv", "serving.csv", "engine.csv"]
    assert job["metadata"]["name"] == JOB_NAME                          # ConfigMap 과 같은 ts
    assert job["spec"]["template"]["spec"]["volumes"][2]["configMap"]["name"] == cm["metadata"]["name"]
    assert job["spec"]["template"]["spec"]["containers"][0]["command"] == [
        "python", "-m", "app.main", "--manual-gpu", "/manual/gpu.csv",
        "--manual-serving", "/manual/serving.csv", "--manual-engine", "/manual/engine.csv",
        "--from", "2026-08-26", "--to", "2026-08-31"]
    out = capsys.readouterr().out
    n = ml.total_bytes(ml.read_manual_files(p["gpu"], p["serving"], p["engine"]))
    assert f"[INFO] configmap={CM_NAME} job={JOB_NAME} files=gpu.csv,serving.csv,engine.csv bytes={n}" in out
    assert ("[NEXT] manual 적재 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3): "
            "python3 mart/token-metrics/tools/rerun.py --context c --namespace monitoring "
            "--from 2026-08-26 --to 2026-08-31") in out
    assert f"[INFO] Job 오브젝트는 남김(로그 재조회용) — 정리: kubectl --context=c delete job {JOB_NAME} -n monitoring" in out


def test_keep_configmap_skips_delete(monkeypatch, tmp_path, capsys):
    p = write_inputs(tmp_path)
    rc, k8s, _ = _run_main(monkeypatch, _argv(p, "--keep-configmap"), wait_result=True)
    assert rc == 0
    assert not any(c[:2] == ["delete", "configmap"] for c in k8s.calls)   # delete 0회
    assert list(k8s.created[0]["data"]) == ["gpu.csv", "serving.csv"]     # engine 없음
    out = capsys.readouterr().out
    assert f"[INFO] configmap={CM_NAME} job={JOB_NAME} files=gpu.csv,serving.csv bytes=" in out
    assert f"[INFO] ConfigMap 보존(--keep-configmap) — 정리: kubectl --context=c delete configmap {CM_NAME} -n monitoring" in out


def test_configmap_deleted_on_failure(monkeypatch, tmp_path, capsys):
    p = write_inputs(tmp_path)
    rc, k8s, waited = _run_main(monkeypatch, _argv(p), wait_result=False)
    assert rc == 1
    assert k8s.calls[-1] == DELETE_CALL                                   # 실패해도 마지막은 삭제
    assert len(waited) == 1
    assert "[NEXT]" not in capsys.readouterr().out                        # 실패 시 mart 안내 없음


def test_configmap_delete_failure_is_warn_only(monkeypatch, tmp_path, capsys):
    p = write_inputs(tmp_path)
    rc, k8s, _ = _run_main(monkeypatch, _argv(p), wait_result=True, delete_fails=True)
    assert rc == 0                                                        # 종료코드 불변
    assert k8s.calls[-1] == DELETE_CALL
    err = capsys.readouterr().err
    assert (f"[WARN] ConfigMap 삭제 실패 — 수동 삭제: kubectl --context=c delete configmap "
            f"{CM_NAME} -n monitoring") in err


def test_configmap_deleted_when_job_apply_raises(monkeypatch, tmp_path):
    p = write_inputs(tmp_path)
    with pytest.raises(subprocess.CalledProcessError):
        _run_main(monkeypatch, _argv(p), wait_result=True, job_apply_fails=True)
    # 예외가 전파돼도 finally 가 ConfigMap 을 지운다 — 페이크는 monkeypatch 된 ml.kubectl 에 남아 있다
    k8s = ml.kubectl
    assert k8s.calls[-1] == DELETE_CALL
    assert k8s.applied == [] and len(k8s.created) == 1


def test_timeout_and_passthrough_flags(monkeypatch, tmp_path):
    p = write_inputs(tmp_path)
    rc, k8s, waited = _run_main(
        monkeypatch, _argv(p, "--service", "Mock Service A", "--replace",
                           "--generated-at", "2026-09-01T09:00:00+09:00", "--timeout-s", "120"),
        wait_result=True)
    assert rc == 0
    assert waited == [("c", "monitoring", JOB_NAME, 120)]
    cmd = k8s.applied[0]["spec"]["template"]["spec"]["containers"][0]["command"]
    assert cmd[-5:] == ["--service", "Mock Service A", "--replace",
                        "--generated-at", "2026-09-01T09:00:00+09:00"]
    assert cmd == ["python", "-m", "app.main", "--manual-gpu", "/manual/gpu.csv",
                   "--manual-serving", "/manual/serving.csv",
                   "--from", "2026-08-26", "--to", "2026-08-31",
                   "--service", "Mock Service A", "--replace",
                   "--generated-at", "2026-09-01T09:00:00+09:00"]


def test_cronjob_override_changes_job_name_and_get(monkeypatch, tmp_path):
    p = write_inputs(tmp_path)
    k8s = FakeK8s(cronjob_obj())
    seen = []

    def relaxed(context, args, *, capture=False, input_data=None):
        seen.append(list(args))
        if args[:2] == ["get", "cronjob"]:
            assert args[2] == "token-metrics-collector-verify"
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(k8s.cronjob), stderr="")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(ml, "kubectl", relaxed)
    monkeypatch.setattr(ml, "wait_job", lambda c, n, j, t: True)
    monkeypatch.setattr(ml, "now_kst", lambda: NOW)
    rc = ml.main(_argv(p, "--cronjob", "token-metrics-collector-verify"))
    assert rc == 0
    assert seen[1][2] == "token-metrics-collector-verify"
    assert seen[-1] == DELETE_CALL                                        # ConfigMap 이름은 cronjob 무관


def test_size_guard(monkeypatch, tmp_path, capsys):
    p = write_inputs(tmp_path)
    big = GPU_HEADER + "\n" + ("2026-08-26,Mock Service A,m,H100,serving,1,1.0\n" * 21_000)  # 47 bytes x 21_000 = 987_000
    assert len(big.encode("utf-8")) >= 950_000
    p["gpu"].write_text(big, encoding="utf-8")
    k8s = FakeK8s(cronjob_obj())
    monkeypatch.setattr(ml, "kubectl", k8s)
    monkeypatch.setattr(ml, "now_kst", lambda: NOW)
    with pytest.raises(SystemExit) as e:
        ml.main(_argv(p))
    assert e.value.code == 2
    assert k8s.calls == []                                                # create/apply 0회
    n = ml.total_bytes(ml.read_manual_files(p["gpu"], p["serving"], None))
    assert f"[ERROR] CSV 합계 {n} bytes > 900000 — 날짜 범위를 나눠 제출" in capsys.readouterr().err


def test_usage_errors(monkeypatch, tmp_path, capsys):
    p = write_inputs(tmp_path)
    k8s = FakeK8s(cronjob_obj())
    monkeypatch.setattr(ml, "kubectl", k8s)
    monkeypatch.setattr(ml, "now_kst", lambda: NOW)
    with pytest.raises(SystemExit) as e:                                  # --gpu 부재 파일
        ml.main(["--context", "c", "--from", "2026-08-26", "--to", "2026-08-31",
                 "--gpu", str(tmp_path / "missing_manual_metrics.csv"), "--serving", str(p["serving"])])
    assert e.value.code == 2
    assert "[ERROR] 파일 없음: " in capsys.readouterr().err
    with pytest.raises(SystemExit) as e:                                  # --engine 부재 파일
        ml.main(_argv(p, "--engine", str(tmp_path / "missing_engine.csv")))
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:                                  # --from > --to
        ml.main(["--context", "c", "--from", "2026-09-10", "--to", "2026-09-01",
                 "--gpu", str(p["gpu"]), "--serving", str(p["serving"])])
    assert e.value.code == 2
    assert "--from(2026-09-10) > --to(2026-09-01)" in capsys.readouterr().err
    with pytest.raises(SystemExit) as e:                                  # 날짜 형식
        ml.main(["--context", "c", "--from", "2026/08/26", "--to", "2026-08-31",
                 "--gpu", str(p["gpu"]), "--serving", str(p["serving"])])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:                                  # --serving 없음 (required)
        ml.main(["--context", "c", "--from", "2026-08-26", "--to", "2026-08-31", "--gpu", str(p["gpu"])])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:                                  # --from 없음 (required)
        ml.main(["--context", "c", "--to", "2026-08-31", "--gpu", str(p["gpu"]), "--serving", str(p["serving"])])
    assert e.value.code == 2
    assert k8s.calls == []                                                # 전부 kubectl 호출 전


def test_arg_parser_defaults():
    args = ml.build_arg_parser().parse_args(
        ["--context", "c", "--from", "2026-08-26", "--to", "2026-08-31", "--gpu", "g", "--serving", "s"])
    assert args.namespace == "monitoring" and args.cronjob == "token-metrics-collector"
    assert args.engine is None and args.service is None and args.generated_at is None
    assert args.replace is False and args.keep_configmap is False
    assert args.timeout_s == 3600


# ---- 템플릿 3파일 왕복 (Plan 6a F) ------------------------------------------------

def test_templates_round_trip_to_command():
    gpu = TEMPLATES / "token_metrics_manual_v0_gpu.csv"
    serving = TEMPLATES / "token_metrics_manual_v0_serving.csv"
    engine = TEMPLATES / "token_metrics_manual_v0_engine.csv"
    files = ml.read_manual_files(gpu, serving, engine)
    cm = ml.build_configmap(CM_NAME, files)
    assert set(cm["data"]) == {"gpu.csv", "serving.csv", "engine.csv"}
    first_rows = {k: [ln for ln in v.splitlines() if ln and not ln.lstrip().startswith("#")][0]
                  for k, v in cm["data"].items()}
    assert first_rows["gpu.csv"] == GPU_HEADER
    assert first_rows["serving.csv"] == SERVING_HEADER
    assert first_rows["engine.csv"] == "service,engine_type,engine_version"
    assert ml.total_bytes(files) < ml.MAX_CONFIGMAP_BYTES
    job = ml.build_job_spec(cronjob_obj(), JOB_NAME,
                            ml.build_manual_command("2026-08-26", "2026-08-26", engine=True,
                                                    service=None, replace=False, generated_at=None),
                            CM_NAME)
    cmd = job["spec"]["template"]["spec"]["containers"][0]["command"]
    assert cmd == ["python", "-m", "app.main", "--manual-gpu", "/manual/gpu.csv",
                   "--manual-serving", "/manual/serving.csv", "--manual-engine", "/manual/engine.csv",
                   "--from", "2026-08-26", "--to", "2026-08-26"]
    # 파드 안 경로 = 마운트 경로 + ConfigMap 키 (T7 파서가 여는 파일)
    mount = job["spec"]["template"]["spec"]["containers"][0]["volumeMounts"][2]["mountPath"]
    assert {f"{mount}/{k}" for k in cm["data"]} == {"/manual/gpu.csv", "/manual/serving.csv", "/manual/engine.csv"}
