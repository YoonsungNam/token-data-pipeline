import importlib.util
import json
import pathlib
import shlex

import pytest

_RERUN_PATH = pathlib.Path(__file__).resolve().parents[1] / "tools" / "rerun.py"
spec = importlib.util.spec_from_file_location("rerun", _RERUN_PATH)
rerun = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rerun)


def cronjob_obj():
    return {
        "metadata": {"name": "token-usage-collector", "namespace": "monitoring",
                     "resourceVersion": "123", "uid": "x"},
        "spec": {"jobTemplate": {"spec": {
            "backoffLimit": 1,
            "activeDeadlineSeconds": 4320,
            "template": {"spec": {"restartPolicy": "Never", "containers": [
                {"name": "token-usage-collector", "image": "img:tag"}]}},
        }}},
    }


def test_build_job_spec_overrides_command_and_strips_cron_metadata():
    job = rerun.build_job_spec(cronjob_obj(), "token-usage-collector-rerun-1",
                               ["python", "-m", "app.main", "--from", "2026-07-01", "--to", "2026-07-02"])
    assert job["kind"] == "Job"
    assert job["metadata"] == {"name": "token-usage-collector-rerun-1"}   # uid/resourceVersion 제거
    tpl = job["spec"]["template"]["spec"]["containers"][0]
    assert tpl["command"] == ["python", "-m", "app.main", "--from", "2026-07-01", "--to", "2026-07-02"]
    assert job["spec"]["activeDeadlineSeconds"] == 4320                   # 기본: CronJob 값 상속


def test_build_job_spec_deadline_override_for_range():
    # §5.2의 4320s는 '1일치' 산식 — 다일 range rerun은 일수 비례로 재설정하지 않으면
    # k8s가 72분에 강제 종료해 기간 회수(§8.3)가 불능이 된다
    job = rerun.build_job_spec(cronjob_obj(), "j", ["c"], active_deadline_s=12960)
    assert job["spec"]["activeDeadlineSeconds"] == 12960


def test_range_deadline_scales_with_days_and_caps():
    assert rerun.range_deadline_s(1) == 4320
    assert rerun.range_deadline_s(3) == 3 * 4320
    assert rerun.range_deadline_s(100) == rerun.TIMEOUT_RANGE_S           # 상한 캡


def test_collect_command_variants():
    assert rerun.build_collect_command("2026-07-01", "2026-07-02", None, False) == \
        ["python", "-m", "app.main", "--from", "2026-07-01", "--to", "2026-07-02"]
    assert rerun.build_collect_command("2026-07-01", "2026-07-01", "Mock Service A", True) == \
        ["python", "-m", "app.main", "--from", "2026-07-01", "--to", "2026-07-01",
         "--service", "Mock Service A", "--push-vm"]


def test_mart_command_propagates_dates_verbatim():
    # §8.3 v1.4 체이닝 날짜 전달 계약: --from/--to 동일 값 그대로
    cmd = rerun.build_mart_command("homelab", "monitoring", "2026-07-01", "2026-07-03")
    assert "--from 2026-07-01" in cmd and "--to 2026-07-03" in cmd and "--context homelab" in cmd


def test_chain_command_parses_in_mart_rerun():
    # Plan 3 T8 Step 1(a): 클러스터 없이 체이닝 정합을 실증 — collectors
    # build_mart_command의 출력 문자열을 shlex.split 후 mart rerun.py의
    # argparse(build_arg_parser)에 그대로 투입해 성공 + 동일 날짜를 확인한다.
    # 양쪽 rerun.py를 importlib로 파일 기준 상대 경로로 로드한다 (패키지화 없음).
    mart_rerun_path = (pathlib.Path(__file__).resolve().parents[3]
                       / "mart" / "token-usage" / "tools" / "rerun.py")
    assert mart_rerun_path.exists(), f"mart rerun.py 미존재: {mart_rerun_path}"
    mart_spec = importlib.util.spec_from_file_location("mart_rerun", mart_rerun_path)
    mart_rerun = importlib.util.module_from_spec(mart_spec)
    mart_spec.loader.exec_module(mart_rerun)

    cmd = rerun.build_mart_command("homelab", "monitoring", "2026-07-01", "2026-07-03")
    tokens = shlex.split(cmd)
    assert tokens[0] == "python3"
    assert tokens[1] == rerun.MART_RERUN            # collectors가 참조하는 상대경로 그대로
    mart_args = mart_rerun.build_arg_parser().parse_args(tokens[2:])
    assert mart_args.context == "homelab"
    assert mart_args.namespace == "monitoring"
    assert mart_args.from_d == "2026-07-01"          # collectors --from과 동일 날짜
    assert mart_args.to_d == "2026-07-03"            # collectors --to와 동일 날짜


def test_from_after_to_is_usage_error():
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "homelab", "--from", "2026-07-05", "--to", "2026-07-01"])
    assert e.value.code == 2


def test_malformed_date_is_usage_error():
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "homelab", "--from", "2026/07/01", "--to", "2026-07-02"])
    assert e.value.code == 2


def test_service_without_range_is_usage_error():
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "homelab", "--service", "S"])
    assert e.value.code == 2
