"""token-metrics collector 재수행 도구 (설계 §5.6 · §6.3).

날짜 범위(--from/--to, **inclusive**, KST — app.main 계약)를 --chunk-days(기본 7)일씩
잘라 CronJob `token-metrics-collector` 스펙에서 **청크당 Job 1개**를 순차 생성한다
(command override: `python -m app.main --from --to [--service] [--replace]`).
청크 Job은 CronJob의 activeDeadlineSeconds(3000, §5.2)를 그대로 상속한다 — 초과하면
k8s가 Job을 Failed로 만들고 이 도구는 다음 청크를 만들지 않는다(exit 1; 안내된 범위로
재시도. 앞선 성공 청크는 --replace 없이 재실행해도 already_loaded로 스킵되어 안전).

실행 창(§6.3): KST 10:50 이후 + 활성 `token-mart-metrics` Job 0 — 둘 중 하나라도
위반이면 Job을 만들지 않고 exit 3 (--force-window로 생략 가능, WARN 출력).
--chain-mart 여부와 무관하게 항상 검사한다(수집기 DELETE/INSERT가 mart-metrics
10:20 배치의 fact 읽기와 겹치지 않도록).

완료 시 동일 날짜 범위의 mart-metrics rerun 명령을 **항상 출력**(§6.3 의무 절차 —
수집기가 already_loaded로 스킵한 날짜도 포함해 청크 분할 전 전체 --from/--to 그대로),
--chain-mart 지정 시 직접 실행하고 그 반환값으로 종료한다.

사용법:
  python3 tools/rerun.py --context prod --namespace monitoring \
      --from 2026-09-01 --to 2026-09-20 --replace --chain-mart
  python3 tools/rerun.py --context prod --from 2026-09-10 --to 2026-09-10 \
      --service "Mock Service A" --replace

옵션:
  --context       kubectl context (필수)
  --namespace     기본 monitoring
  --cronjob       대상 CronJob 이름 (기본 token-metrics-collector — company-verify는
                  token-metrics-collector-verify)
  --from/--to     YYYY-MM-DD, KST, 둘 다 inclusive. 필수 쌍.
                  (수동 1회 트리거는 이 도구가 아니라 정기 슬롯 8회 또는
                   kubectl create job --from=cronjob/token-metrics-collector <name>)
  --service       단일 서비스만 (endpoints.yaml의 service 정본; 미존재 시 파드가 exit 2)
  --replace       앵커 존재 날짜도 교체 (§5.2 — 없으면 SKIPPED reason=already_loaded)
  --chunk-days    청크 길이(일), 기본 7, 1 이상
  --chain-mart    완료 후 mart/token-metrics/tools/rerun.py 를 같은 범위로 실행
  --force-window  실행 창 검사 생략 (긴급 시에만 — mart-metrics 배치와 겹칠 수 있음)

종료코드: 0 성공 / 1 Job 실패·타임아웃·mart rerun 부재 / 2 사용법 / 3 실행 창 밖
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import pathlib
import shlex
import subprocess
import sys
import time

CRONJOB = "token-metrics-collector"
MART_CRONJOB = "token-mart-metrics"
MART_RERUN = "mart/token-metrics/tools/rerun.py"   # Plan 6c 산출 경로 (부재 시 안내 후 exit 1)
REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]   # tools/ → token-metrics/ → collectors/ → 루트
POLL_S = 10
TIMEOUT_SINGLE_S = 3000 + 600     # 서버 activeDeadlineSeconds(§5.2) + 폴링 마진 600 — 청크 1개 = Job 1개
DEFAULT_CHUNK_DAYS = 7
CHUNK_DAYS_MAX = 15               # §4.0: 실행당 뮤테이션 예산 45 = 15일 × 3 (--replace 청크가 예산을 넘지 않게) — 6c 상한 16 이하
WINDOW_OPEN_HHMM = (10, 50)       # §6.3: mart-metrics 10:20 배치(activeDeadlineSeconds 1800) 종료 후
KST = dt.timezone(dt.timedelta(hours=9))


def kubectl(context, args, *, capture=False, input_data=None):
    cmd = ["kubectl", f"--context={context}", "--insecure-skip-tls-verify"] + list(args)
    return subprocess.run(cmd, check=True, text=True, input=input_data,
                          capture_output=capture)


def now_kst():
    """aware KST 현재 시각 — 테스트는 이 함수를 페이크로 바꾼다 (datetime.now 는 C 타입이라 불가)."""
    return dt.datetime.now(KST)


def split_chunks(d0, d1, chunk_days):
    """inclusive [d0, d1]을 chunk_days일씩 앞에서부터 자른다 (마지막 조각은 짧아도 됨)."""
    if chunk_days < 1:
        raise ValueError(f"chunk_days must be >= 1: {chunk_days}")
    if d0 > d1:
        raise ValueError(f"from({d0}) > to({d1})")
    chunks = []
    start = d0
    while start <= d1:
        end = min(start + dt.timedelta(days=chunk_days - 1), d1)
        chunks.append((start, end))
        start = end + dt.timedelta(days=1)
    return chunks


def build_collect_command(from_d, to_d, service, replace):
    cmd = ["python", "-m", "app.main", "--from", from_d, "--to", to_d]
    if service:
        cmd += ["--service", service]
    if replace:
        cmd += ["--replace"]
    return cmd


def build_job_spec(cronjob_obj, job_name, command):
    """CronJob 오브젝트에서 1회성 Job 스펙 생성 + containers[0].command override.

    metadata는 name + 라벨만 남긴다 (uid/resourceVersion/namespace 등 서버 필드 제거).
    activeDeadlineSeconds는 jobTemplate.spec 값(3000, §5.2)을 그대로 상속 — override 인자 없음
    (청크 7일 × 서비스 수의 부하 상한 = 이 서버 데드라인; 초과 시 Failed → 다음 청크 중단).
    ttlSecondsAfterFinished는 없을 때만 86400(1일)으로 채운다(fix1 nit) — CronJob 소유가
    아닌 이 1회성 Job은 successfulJobsHistoryLimit의 GC 대상이 아니라 방치되면 영구 잔존한다."""
    spec = copy.deepcopy(cronjob_obj["spec"]["jobTemplate"]["spec"])
    spec["template"]["spec"]["containers"][0]["command"] = list(command)
    spec.setdefault("ttlSecondsAfterFinished", 86400)
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": job_name, "labels": {"app": CRONJOB, "rerun": "1"}},
            "spec": spec}


def job_names(cronjob, epoch, n):
    return [f"{cronjob}-rerun-{epoch}-{i}" for i in range(n)]


def check_window(now, active_mart_jobs):
    """§6.3 실행 창. 위반 사유 문자열 / 정상 None. naive datetime 은 거부 (KST 규율)."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("check_window: aware datetime required (KST)")
    local = now.astimezone(KST)
    if (local.hour, local.minute) < WINDOW_OPEN_HHMM:
        return "window_closed"
    if active_mart_jobs > 0:
        return "mart_job_active"
    return None


def count_active_mart_jobs(context, namespace, mart_cronjob):
    """활성(status.active > 0) mart-metrics Job 수 — CronJob 소유(ownerReferences) 또는
    이름 접두사 `<mart_cronjob>-…`(mart rerun Job, owner 없음) 둘 다 집계.

    fix1: mart_cronjob이 prod(`-verify`로 끝나지 않음)일 때는 verify 오버레이의 rerun Job
    (`<mart_cronjob>-verify-…`)을 접두사 매치에서 제외한다 — 같은 네임스페이스에 공존하는
    verify mart Job이 prod 수집기 rerun의 실행 창을 오버-블록하면 안 된다(설계 해석 e).
    CronJob 소유(owner) 매치는 이름이 아니라 ownerReferences 값이므로 영향받지 않는다."""
    res = kubectl(context, ["get", "jobs", "-n", namespace, "-o", "json"], capture=True)
    n = 0
    for item in json.loads(res.stdout).get("items", []):
        meta = item.get("metadata", {})
        owners = {o.get("name") for o in meta.get("ownerReferences", [])}
        name = str(meta.get("name", ""))
        if not mart_cronjob.endswith("-verify") and name.startswith(mart_cronjob + "-verify-"):
            continue
        if mart_cronjob not in owners and not name.startswith(mart_cronjob + "-"):
            continue
        if int(item.get("status", {}).get("active", 0) or 0) > 0:
            n += 1
    return n


def build_mart_command(context, namespace, from_d, to_d, chunk_days, *,
                       mart_cronjob=MART_CRONJOB, force=False):
    # §6.3: 청크 분할 전 전체 --from/--to 를 동일 값 그대로 전파 (수집기 스킵 날짜 포함).
    # company-verify(--cronjob …-verify)면 mart 쪽도 token-mart-metrics-verify 로, --force-window 면 6c --force 로 전파
    # (6c --force 는 10:50 창만 생략 — 활성 token-mart-* Job 이 있으면 6c 가 여전히 exit 2 로 거부한다).
    cmd = ["python3", MART_RERUN, "--context", context, "--namespace", namespace]
    if mart_cronjob != MART_CRONJOB:
        cmd += ["--cronjob", mart_cronjob]
    cmd += ["--from", from_d, "--to", to_d, "--chunk-days", str(chunk_days)]
    if force:
        cmd.append("--force")
    return cmd


def mart_cronjob_for(collector_cronjob):
    """수집기 CronJob 이름 → 짝이 되는 mart CronJob 이름 (company-verify: `-verify` 접미사 동반)."""
    return MART_CRONJOB + "-verify" if collector_cronjob.endswith("-verify") else MART_CRONJOB


def wait_job(context, namespace, job_name, timeout_s):
    """Job 완료 폴링 + 파드 로그 스트리밍. 성공 True / 실패·타임아웃 False.

    backoffLimit=0(§5.2)이라 파드는 1개지만, 파드 집합 순회 골격은 기존 모듈과 동일하게 둔다 —
    마커 라인(SERVICE_RESULT/BATCH_RESULT)이 운영 기록이므로 가공 없이 그대로 출력."""
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
                   help=f"대상 CronJob 이름 (기본 {CRONJOB}; company-verify는 {CRONJOB}-verify)")
    p.add_argument("--from", dest="from_d", required=True, help="YYYY-MM-DD (KST, inclusive)")
    p.add_argument("--to", dest="to_d", required=True, help="YYYY-MM-DD (KST, inclusive)")
    p.add_argument("--service", default=None)
    p.add_argument("--replace", action="store_true")
    p.add_argument("--chunk-days", dest="chunk_days", type=int, default=DEFAULT_CHUNK_DAYS,
                   help=f"청크 길이(일), 기본 {DEFAULT_CHUNK_DAYS}, 1..{CHUNK_DAYS_MAX} (뮤테이션 예산 45 = 15일×3)")
    p.add_argument("--chain-mart", dest="chain_mart", action="store_true")
    p.add_argument("--force-window", dest="force_window", action="store_true",
                   help="실행 창(10:50 KST 이후 + 활성 mart Job 0) 검사 생략")
    return p


def main(argv=None):
    p = build_arg_parser()
    args = p.parse_args(argv)
    try:
        d0, d1 = dt.date.fromisoformat(args.from_d), dt.date.fromisoformat(args.to_d)
    except ValueError:
        p.exit(2, "--from/--to는 YYYY-MM-DD 형식\n")
    if d0 > d1:
        p.exit(2, f"--from({d0}) > --to({d1})\n")
    if not 1 <= args.chunk_days <= CHUNK_DAYS_MAX:
        p.exit(2, f"--chunk-days는 1..{CHUNK_DAYS_MAX} (지정값 {args.chunk_days}; 뮤테이션 예산 45 = 15일×3, §4.0; "
                  f"6c mart rerun 상한 16 이하)\n")
    from_s, to_s = d0.isoformat(), d1.isoformat()
    mart_cronjob = mart_cronjob_for(args.cronjob)

    try:
        # §6.3 실행 창 — 체인 여부와 무관하게 항상 (수집기 DELETE/INSERT 가 mart-metrics 와 겹치지 않도록)
        if args.force_window:
            print("[WARN] 실행 창 검사 생략(--force-window)", flush=True)
        else:
            active = count_active_mart_jobs(args.context, args.namespace, mart_cronjob)
            reason = check_window(now_kst(), active)
            if reason:
                print(f"[ERROR] 실행 창 밖: {reason} — KST 10:50 이후·활성 {mart_cronjob} Job 0일 때 "
                      f"재시도 (--force-window로 강제)", file=sys.stderr)
                return 3

        res = kubectl(args.context, ["get", "cronjob", args.cronjob, "-n", args.namespace,
                                     "-o", "json"], capture=True)
        cronjob_obj = json.loads(res.stdout)
        chunks = split_chunks(d0, d1, args.chunk_days)
        names = job_names(args.cronjob, int(time.time()), len(chunks))
        n = len(chunks)
        for i, ((c0, c1), job_name) in enumerate(zip(chunks, names), start=1):
            print(f"[INFO] 청크 {i}/{n}: {c0} .. {c1} → Job {job_name}", flush=True)
            job = build_job_spec(cronjob_obj, job_name,
                                 build_collect_command(c0.isoformat(), c1.isoformat(),
                                                       args.service, args.replace))
            kubectl(args.context, ["apply", "-n", args.namespace, "-f", "-"],
                    input_data=json.dumps(job))
            if not wait_job(args.context, args.namespace, job_name, TIMEOUT_SINGLE_S):
                print(f"[ERROR] 청크 {i}/{n} 실패 — 이후 청크 중단; 재시도: --from {c0} --to {to_s} "
                      f"(그 외 인자 동일)", file=sys.stderr)
                return 1

        # §6.3: collectors rerun 후 동일 날짜 범위 mart-metrics rerun 은 의무 — 항상 안내.
        # --cronjob …-verify → mart 쪽 --cronjob token-mart-metrics-verify, --force-window → 6c --force (창만 생략).
        # 6c 는 활성 token-mart-* Job 이 있으면 --force 와 무관하게 exit 2 로 거부한다 — 그때는 위 명령을 다시 실행.
        mart_cmd = build_mart_command(args.context, args.namespace, from_s, to_s, args.chunk_days,
                                      mart_cronjob=mart_cronjob, force=args.force_window)
        print("")
        print("[NEXT] collectors rerun 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3):")
        print("  " + shlex.join(mart_cmd), flush=True)
        if args.chain_mart:
            mart_path = REPO_ROOT / MART_RERUN
            if not mart_path.exists():
                print(f"[ERROR] --chain-mart: {MART_RERUN} 가 아직 없습니다 (Plan 6c 전) — "
                      f"mart-metrics 구현 후 위 명령을 실행하세요.", file=sys.stderr)
                return 1
            # 절대경로 + 리스트 인자 (cwd 무관, 공백 인자 안전); mart rerun 의 반환값 그대로 종료
            return subprocess.call([sys.executable, str(mart_path)] + mart_cmd[2:])
        return 0
    except subprocess.CalledProcessError as e:
        # fix1: kubectl 실패(인증 만료·컨텍스트 오류·API 다운)를 트레이스백 대신 정리된 exit 1로 —
        # 종료코드 공간은 0/1/2/3 그대로, e.stdout(Job/CronJob JSON 본문)은 찍지 않는다.
        print(f"[ERROR] kubectl 실패 (rc={e.returncode}): {shlex.join(e.cmd)}", file=sys.stderr)
        if e.stderr:
            print(e.stderr.rstrip(), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
