import importlib.util
import pathlib

import pytest

_RERUN_PATH = pathlib.Path(__file__).resolve().parents[1] / "tools" / "rerun.py"
spec = importlib.util.spec_from_file_location("rerun", _RERUN_PATH)
rerun = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rerun)


def cronjob_obj():
    return {
        "metadata": {"name": "token-mart-daily", "namespace": "monitoring",
                     "resourceVersion": "123", "uid": "x"},
        "spec": {"jobTemplate": {"spec": {
            "backoffLimit": 1,
            "activeDeadlineSeconds": 1800,
            "template": {"spec": {"restartPolicy": "Never", "containers": [
                {"name": "token-mart", "image": "img:tag"}]}},
        }}},
    }


def test_cronjob_constant_is_mart():
    # 델타 누락 시 collectors CronJob(token-usage-collector)을 오트리거하는 치명 오류가 된다
    assert rerun.CRONJOB == "token-mart-daily"


def test_no_mart_rerun_downstream_symbols():
    # 이 모듈이 체이닝의 수신 측 — 하류(MART_RERUN/build_mart_command/--chain-mart)가 없다
    assert not hasattr(rerun, "MART_RERUN")
    assert not hasattr(rerun, "build_mart_command")


def test_build_job_spec_overrides_command_and_strips_cron_metadata():
    job = rerun.build_job_spec(cronjob_obj(), "token-mart-daily-rerun-1",
                               ["python", "-m", "app.batch", "--from", "2026-07-01", "--to", "2026-07-02"])
    assert job["kind"] == "Job"
    assert job["metadata"] == {"name": "token-mart-daily-rerun-1"}   # uid/resourceVersion 제거
    tpl = job["spec"]["template"]["spec"]["containers"][0]
    assert tpl["command"] == ["python", "-m", "app.batch", "--from", "2026-07-01", "--to", "2026-07-02"]
    assert job["spec"]["activeDeadlineSeconds"] == 1800                # 기본: CronJob 값 상속


def test_build_job_spec_deadline_override_for_range():
    # §7.2의 1800s는 '1일치' 산식 — 다일 range rerun은 일수 비례로 재설정하지 않으면
    # k8s가 30분에 강제 종료해 기간 회수(§8.3)가 불능이 된다
    job = rerun.build_job_spec(cronjob_obj(), "j", ["c"], active_deadline_s=5400)
    assert job["spec"]["activeDeadlineSeconds"] == 5400


def test_range_deadline_base_1800():
    assert rerun.range_deadline_s(1) == 1800


def test_range_deadline_scales_with_days_and_caps():
    assert rerun.range_deadline_s(1) == 1800
    assert rerun.range_deadline_s(3) == 3 * 1800
    assert rerun.range_deadline_s(100) == rerun.TIMEOUT_RANGE_S          # 상한 캡


def test_collect_command_variants():
    assert rerun.build_collect_command("2026-07-01", "2026-07-02") == \
        ["python", "-m", "app.batch", "--from", "2026-07-01", "--to", "2026-07-02"]
    assert rerun.build_collect_command("2026-07-01", "2026-07-01") == \
        ["python", "-m", "app.batch", "--from", "2026-07-01", "--to", "2026-07-01"]


def test_cli_accepts_collectors_chain_interface():
    # §8.3 체이닝 계약: collectors build_mart_command가 만드는
    # "--context c --namespace n --from D --to D" 형태가 그대로 파싱돼야 한다.
    argv = ["--context", "c", "--namespace", "n", "--from", "2026-07-01", "--to", "2026-07-02"]
    args = rerun.build_arg_parser().parse_args(argv)
    assert args.context == "c"
    assert args.namespace == "n"
    assert rerun.build_collect_command(args.from_d, args.to_d) == \
        ["python", "-m", "app.batch", "--from", "2026-07-01", "--to", "2026-07-02"]


def test_no_service_or_push_vm_or_chain_mart_flags():
    # mart rerun에는 --service/--push-vm/--chain-mart 플래그가 없다 (수신 측 — 하류 없음)
    parser = rerun.build_arg_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--context", "c", "--service", "S"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--context", "c", "--push-vm"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--context", "c", "--chain-mart"])


def test_from_after_to_is_usage_error():
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "homelab", "--from", "2026-07-05", "--to", "2026-07-01"])
    assert e.value.code == 2


def test_malformed_date_is_usage_error():
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "homelab", "--from", "2026/07/01", "--to", "2026-07-02"])
    assert e.value.code == 2


def test_from_without_to_is_usage_error():
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "homelab", "--from", "2026-07-01"])
    assert e.value.code == 2


def test_context_required():
    with pytest.raises(SystemExit) as e:
        rerun.main([])
    assert e.value.code == 2


def test_cronjob_default_and_override():
    # company-verify 등 -verify 접미 CronJob 재수행용 오버라이드 (docs/operations/company-verify.md)
    args = rerun.build_arg_parser().parse_args(["--context", "homelab"])
    assert args.cronjob == rerun.CRONJOB == "token-mart-daily"

    args = rerun.build_arg_parser().parse_args(
        ["--context", "homelab", "--cronjob", "token-mart-daily-verify"])
    assert args.cronjob == "token-mart-daily-verify"
