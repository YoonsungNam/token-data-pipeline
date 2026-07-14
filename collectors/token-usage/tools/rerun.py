"""token-usage collector 재수행 도구 (§8.3).

두 가지 모드:
  1) 1회 수동 트리거(기본) — CronJob에서 Job 생성 (실행 시점 기준 어제 KST 수집)
  2) 날짜 범위 재수집(--from/--to, **inclusive** — main.py 계약. 동료 metric rerun의
     to-제외와 다름) — CronJob 스펙에서 Job을 만들되 command를 override

완료 시 동일 날짜 mart rerun 명령을 **항상 출력**(§8.3 의무 절차 — collectors rerun 후
mart rerun 의무), --chain-mart 지정 시 직접 트리거한다.

사용법:
  python3 collectors/token-usage/tools/rerun.py --context homelab
  python3 collectors/token-usage/tools/rerun.py --context homelab \
      --from 2026-07-01 --to 2026-07-03 [--service "Mock Service A"] [--push-vm] [--chain-mart]

옵션:
  --context     kubectl context (필수)
  --namespace   기본 monitoring
  --cronjob     대상 CronJob 이름 (기본 token-usage-collector — company-verify 등
                -verify 접미 CronJob을 재수행할 때 지정, docs/operations/company-verify.md)
  --from/--to   YYYY-MM-DD, KST, 둘 다 inclusive. 반드시 쌍으로.
  --service     단일 서비스만 재수집 (--from/--to 필요)
  --push-vm     rerun에서도 VM push (§5.5 옵트인 — 기본 생략)
  --chain-mart  완료 후 mart rerun 직접 트리거 (§8.3)
"""
import argparse
import copy
import datetime as dt
import json
import pathlib
import subprocess
import sys
import time

CRONJOB = "token-usage-collector"
MART_RERUN = "mart/token-usage/tools/rerun.py"   # Plan 3에서 확정되는 경로 (부재 시 안내 실패)
POLL_S = 10
TIMEOUT_SINGLE_S = 4320 + 600     # 서버 activeDeadlineSeconds + 폴링 마진 600 (range 모드와 동일 산식)
TIMEOUT_RANGE_S = 6 * 3600        # 동료 관례 (기간 재수집)


def kubectl(context, args, *, capture=False, input_data=None):
    cmd = ["kubectl", f"--context={context}", "--insecure-skip-tls-verify"] + args
    return subprocess.run(cmd, check=True, text=True, input=input_data,
                          capture_output=capture)


def build_collect_command(from_d, to_d, service, push_vm):
    cmd = ["python", "-m", "app.main", "--from", from_d, "--to", to_d]
    if service:
        cmd += ["--service", service]
    if push_vm:
        cmd += ["--push-vm"]
    return cmd


def range_deadline_s(n_days):
    """§5.2의 activeDeadlineSeconds 4320s는 '1일치' 산식 — 다일 range rerun은
    일수 비례로 재설정한다 (상한 = 폴링 타임아웃). 그대로 상속하면 72분에
    k8s가 강제 종료해 기간 회수(§8.3)가 불능."""
    return min(4320 * n_days, TIMEOUT_RANGE_S)


def build_job_spec(cronjob_obj, job_name, command, active_deadline_s=None):
    """CronJob 오브젝트에서 1회성 Job 스펙 생성 + containers[0].command override.

    metadata는 name만 남긴다 (uid/resourceVersion 등 서버 필드 제거).
    active_deadline_s=None이면 jobTemplate.spec 값(일일 계약 4320) 상속,
    값이 있으면 override (range rerun의 일수 비례 재설정)."""
    spec = copy.deepcopy(cronjob_obj["spec"]["jobTemplate"]["spec"])
    spec["template"]["spec"]["containers"][0]["command"] = list(command)
    if active_deadline_s is not None:
        spec["activeDeadlineSeconds"] = active_deadline_s
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": job_name}, "spec": spec}


def build_mart_command(context, namespace, from_d, to_d):
    # §8.3 v1.4: collectors rerun의 --from/--to를 동일 값 그대로 전파 (유일한 접점 인자)
    return (f"python3 {MART_RERUN} --context {context} --namespace {namespace} "
            f"--from {from_d} --to {to_d}")


def wait_job(context, namespace, job_name, timeout_s):
    """Job 완료 폴링 + 파드별 로그 스트리밍. 성공 True / 실패 False.

    backoffLimit=1 재시도 파드까지 각각 스트리밍한다 — 마커 라인(§5.6)이 운영
    기록이므로 가공 없이 그대로 출력."""
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
    p.add_argument("--cronjob", default=CRONJOB,
                    help=f"대상 CronJob 이름 (기본 {CRONJOB})")
    p.add_argument("--from", dest="from_d", default=None)
    p.add_argument("--to", dest="to_d", default=None)
    p.add_argument("--service", default=None)
    p.add_argument("--push-vm", action="store_true")
    p.add_argument("--chain-mart", action="store_true")
    return p


def main(argv=None):
    p = build_arg_parser()
    args = p.parse_args(argv)

    if bool(args.from_d) != bool(args.to_d):
        p.exit(2, "--from/--to는 쌍으로 지정 (YYYY-MM-DD, KST, inclusive)\n")
    if args.service and not args.from_d:
        p.exit(2, "--service는 --from/--to와 함께만 (재수집 용도, §5.1)\n")
    if args.push_vm and not args.from_d:
        p.exit(2, "--push-vm은 --from/--to와 함께만 (§5.5)\n")
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
        job_name = f"{args.cronjob}-rerun-{epoch}"
        res = kubectl(args.context, ["get", "cronjob", args.cronjob, "-n", args.namespace,
                                     "-o", "json"], capture=True)
        job = build_job_spec(json.loads(res.stdout), job_name,
                             build_collect_command(args.from_d, args.to_d,
                                                   args.service, args.push_vm),
                             active_deadline_s=range_deadline_s(n_days))
        kubectl(args.context, ["apply", "-n", args.namespace, "-f", "-"],
                input_data=json.dumps(job))
        timeout = range_deadline_s(n_days) + 600      # 서버 데드라인 + 폴링 마진
    else:
        job_name = f"{args.cronjob}-manual-{epoch}"
        # 파드의 target_date와 일치시키기 위해 트리거 시점 기준으로 고정 (§8.3 자정 크로스 방지)
        kst_now = dt.datetime.now(dt.timezone(dt.timedelta(hours=9)))
        manual_target_date = (kst_now.date() - dt.timedelta(days=1)).isoformat()
        kubectl(args.context, ["create", "job", f"--from=cronjob/{args.cronjob}",
                               job_name, "-n", args.namespace])
        timeout = TIMEOUT_SINGLE_S

    ok = wait_job(args.context, args.namespace, job_name, timeout)
    if not ok:
        return 1

    # §3/§8.3: collectors rerun 후 동일 날짜 mart rerun은 의무 — 모드 무관 항상 안내.
    # 수동 트리거의 대상 날짜 = 실행 시점 기준 어제 (KST, main.py 계약)
    if args.from_d:
        mart_from, mart_to = args.from_d, args.to_d
    else:
        mart_from = mart_to = manual_target_date
    mart_cmd = build_mart_command(args.context, args.namespace, mart_from, mart_to)
    print("")
    print("[NEXT] collectors rerun 후 동일 날짜 mart rerun은 의무입니다 (§3/§8.3):")
    print(f"  {mart_cmd}")
    if args.chain_mart:
        repo_root = pathlib.Path(__file__).resolve().parents[3]
        mart_path = repo_root / MART_RERUN
        if not mart_path.exists():
            print(f"[ERROR] --chain-mart: {MART_RERUN} 가 아직 없습니다 (Plan 3 전) — "
                  f"mart 구현 후 위 명령을 실행하세요.", file=sys.stderr)
            return 1
        # 절대경로 + 리스트 인자 (cwd 무관, 공백 인자 안전)
        return subprocess.call([sys.executable, str(mart_path),
                                "--context", args.context, "--namespace", args.namespace,
                                "--from", mart_from, "--to", mart_to])
    return 0


if __name__ == "__main__":
    sys.exit(main())
