"""mart/token-usage 재수행 도구 (§8.3) — 체이닝의 **수신 측**.

collectors rerun 완료 후 동일 날짜의 mart rerun은 의무다(§3/§8.3). collectors
rerun.py의 build_mart_command가 만드는 커맨드가 정확히 이 CLI를 호출한다:

    python3 mart/token-usage/tools/rerun.py --context <ctx> --namespace <ns> \
        --from <d> --to <d>

이 모듈은 체인의 수신 측이라 하류가 없다 — MART_RERUN 상수·build_mart_command·
[NEXT] 에필로그·체인 실행부는 이 모듈에 존재하지 않는다(collectors와의 결정적 차이).

두 가지 모드:
  1) 1회 수동 트리거(기본) — CronJob에서 Job 생성 (실행 시점 기준 어제 KST 집계,
     app.batch의 기본 target_date 계약과 동일)
  2) 날짜 범위 재수행(--from/--to, **inclusive** — app.batch 계약과 동일) — CronJob
     스펙에서 Job을 만들되 command를 override

사용법:
  python3 mart/token-usage/tools/rerun.py --context homelab
  python3 mart/token-usage/tools/rerun.py --context homelab \
      --from 2026-07-01 --to 2026-07-03

옵션:
  --context     kubectl context (필수)
  --namespace   기본 monitoring
  --from/--to   YYYY-MM-DD, KST, 둘 다 inclusive. 반드시 쌍으로.

--service/--push-vm/--chain-mart 플래그는 없다 — mart는 서비스 단위 재수집 개념이
없고(fact 전체 재집계), VM push도 하지 않으며, 이 모듈 자체가 체인의 종단이다.
"""
import argparse
import copy
import datetime as dt
import json
import subprocess
import sys
import time

CRONJOB = "token-mart-daily"
POLL_S = 10
TIMEOUT_SINGLE_S = 1800 + 600     # 서버 activeDeadlineSeconds + 폴링 마진 600 (range 모드와 동일 산식)
TIMEOUT_RANGE_S = 6 * 3600        # 동료 관례 (기간 재수행)


def kubectl(context, args, *, capture=False, input_data=None):
    cmd = ["kubectl", f"--context={context}", "--insecure-skip-tls-verify"] + args
    return subprocess.run(cmd, check=True, text=True, input=input_data,
                          capture_output=capture)


def build_collect_command(from_d, to_d):
    return ["python", "-m", "app.batch", "--from", from_d, "--to", to_d]


def range_deadline_s(n_days):
    """§7.2의 activeDeadlineSeconds 1800s는 mart(서버사이드 SQL 경량)의 '1일치' 산식 —
    다일 range rerun은 일수 비례로 재설정한다 (상한 = 폴링 타임아웃). 그대로 상속하면
    30분에 k8s가 강제 종료해 기간 회수(§8.3)가 불능."""
    return min(1800 * n_days, TIMEOUT_RANGE_S)


def build_job_spec(cronjob_obj, job_name, command, active_deadline_s=None):
    """CronJob 오브젝트에서 1회성 Job 스펙 생성 + containers[0].command override.

    metadata는 name만 남긴다 (uid/resourceVersion 등 서버 필드 제거).
    active_deadline_s=None이면 jobTemplate.spec 값(일일 계약 1800) 상속,
    값이 있으면 override (range rerun의 일수 비례 재설정)."""
    spec = copy.deepcopy(cronjob_obj["spec"]["jobTemplate"]["spec"])
    spec["template"]["spec"]["containers"][0]["command"] = list(command)
    if active_deadline_s is not None:
        spec["activeDeadlineSeconds"] = active_deadline_s
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": job_name}, "spec": spec}


def wait_job(context, namespace, job_name, timeout_s):
    """Job 완료 폴링 + 파드별 로그 스트리밍. 성공 True / 실패 False.

    backoffLimit=1 재시도 파드까지 각각 스트리밍한다 — 마커 라인(§5.6/§7.1)이 운영
    기록이므로 가공 없이 그대로 출력. 날짜범위 rerun은 날짜별 BATCH_RESULT가 독립
    출력되므로(§7.1) 한 Job 로그 안에 여러 줄이 순서대로 나타날 수 있다."""
    deadline = time.monotonic() + timeout_s
    seen_pods = set()
    while time.monotonic() < deadline:
        res = kubectl(context, ["get", "job", job_name, "-n", namespace, "-o", "json"],
                      capture=True)
        status = json.loads(res.stdout).get("status", {})
        conds = {c["type"]: c["status"] for c in status.get("conditions", [])}
        pods = kubectl(context, ["get", "pods", "-l", f"job-name={job_name}", "-n", namespace,
                                 "-o", "jsonpath={.items[*].metadata.name}"],
                       capture=True).stdout.split()
        for pod in pods:
            if pod not in seen_pods:
                seen_pods.add(pod)
                subprocess.run(["kubectl", f"--context={context}", "--insecure-skip-tls-verify",
                                "logs", "-f", f"pod/{pod}", "-n", namespace,
                                "--pod-running-timeout=5m"], check=False)
        if conds.get("Complete") == "True":
            print(f"[INFO] 전체 로그 재조회: kubectl --context={context} logs job/{job_name} "
                  f"-n {namespace} --prefix --tail=-1")
            return True
        if conds.get("Failed") == "True":
            print(f"[ERROR] job {job_name} failed — 전체 로그: kubectl --context={context} "
                  f"logs job/{job_name} -n {namespace} --prefix --tail=-1", file=sys.stderr)
            return False
        time.sleep(POLL_S)
    print(f"[ERROR] job {job_name} timeout ({timeout_s}s)", file=sys.stderr)
    return False


def build_arg_parser():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", default="monitoring")
    p.add_argument("--from", dest="from_d", default=None)
    p.add_argument("--to", dest="to_d", default=None)
    return p


def main(argv=None):
    p = build_arg_parser()
    args = p.parse_args(argv)

    if bool(args.from_d) != bool(args.to_d):
        p.exit(2, "--from/--to는 쌍으로 지정 (YYYY-MM-DD, KST, inclusive)\n")
    n_days = 1
    if args.from_d:
        try:
            d0, d1 = dt.date.fromisoformat(args.from_d), dt.date.fromisoformat(args.to_d)
        except ValueError:
            p.exit(2, "--from/--to는 YYYY-MM-DD 형식\n")
        if d0 > d1:
            p.exit(2, f"--from({d0}) > --to({d1})\n")
        n_days = (d1 - d0).days + 1

    epoch = int(time.time())
    if args.from_d:
        job_name = f"{CRONJOB}-rerun-{epoch}"
        res = kubectl(args.context, ["get", "cronjob", CRONJOB, "-n", args.namespace,
                                     "-o", "json"], capture=True)
        job = build_job_spec(json.loads(res.stdout), job_name,
                             build_collect_command(args.from_d, args.to_d),
                             active_deadline_s=range_deadline_s(n_days))
        kubectl(args.context, ["apply", "-n", args.namespace, "-f", "-"],
                input_data=json.dumps(job))
        timeout = range_deadline_s(n_days) + 600      # 서버 데드라인 + 폴링 마진
    else:
        job_name = f"{CRONJOB}-manual-{epoch}"
        # command override 없음 — 컨테이너 기본 CMD(python -m app.batch, 인자 없음)가
        # target_date = 실행 시점 기준 어제(KST)를 산정 (app.batch 계약)
        kubectl(args.context, ["create", "job", f"--from=cronjob/{CRONJOB}",
                               job_name, "-n", args.namespace])
        timeout = TIMEOUT_SINGLE_S

    ok = wait_job(args.context, args.namespace, job_name, timeout)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
