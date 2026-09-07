"""mart/token-metrics 재수행 도구 (설계 §7.5 / §4.0 뮤테이션 장부) — 체인의 **종단**(하류 없음).

두 가지 모드:
  1) 1회 수동 트리거(기본) — CronJob token-mart-metrics에서 Job 생성 (실행 시점 기준
     어제 KST 집계, app.batch의 기본 target_date 계약과 동일)
  2) 날짜 범위 재수행(--from/--to, **inclusive** — app.batch 계약과 동일) — 범위를
     --chunk-days(기본 7)일 단위 청크로 나눠 청크마다 CronJob 스펙에서 Job을 만들되
     containers[0].args만 override(이미지 ENTRYPOINT = python -m app.batch), **순차** 실행

두 모드 공통 게이트(설계 §7.5 재실행 절차):
  - 창: 현재 KST가 10:50 이후여야 한다 (일일 CronJob 10:20 + activeDeadlineSeconds 1800
    = 10:50 — 일일 실행과의 겹침 차단). --force로 무시 가능.
  - 활성 Job: 네임스페이스에 실행 중(status.active>0)인 token-mart-* Job이 0이어야 한다
    (token-mart-daily/token-mart-metrics[-verify] 모두 — 동일 mart DB 변이 직렬화). --force로도
    무시할 수 없다.

변이 예산(설계 §4.0 뮤테이션 장부): 배치 1회 변이 = 날짜당 4(mart 4테이블 delete_day) → 청크 7일 = 28
≤ MART_METRICS_MAX_MUTATIONS_PER_RUN 기본 64(= 16일×4). 따라서 --chunk-days 상한 16.

kubectl 실패(인증 만료·컨텍스트 오류·API 다운)는 트레이스백 대신 정리된 exit 1 + stderr
메시지로 처리한다(collectors/token-metrics/tools/rerun.py fix1과 동일 관례).

사용법:
  python3 mart/token-metrics/tools/rerun.py --context homelab
  python3 mart/token-metrics/tools/rerun.py --context homelab \
      --from 2026-08-01 --to 2026-08-17 [--chunk-days 7] [--force]

옵션:
  --context       kubectl context (필수)
  -n/--namespace  기본 monitoring
  --cronjob       대상 CronJob 이름 (기본 token-mart-metrics — company-verify는
                  token-mart-metrics-verify 지정)
  --from/--to     YYYY-MM-DD, KST, 둘 다 inclusive. 반드시 쌍으로.
  --chunk-days    청크 일수 (기본 7, 1..16)
  --force         창(10:50 KST) 게이트 무시 (활성 Job 게이트는 무시 불가)

--service/--push-vm/--chain-mart/--chain/--replace/--target-db 플래그는 없다 — mart-metrics는
서비스 단위 재수집 개념이 없고(mart 재집계), 이 모듈 자체가 체인의 종단이며, 격리 검증은
Secret의 CH_DB_*로만 분기한다.

종료코드: 0 성공 / 1 Job 실패 또는 kubectl 실패 / 2 사용법·창·활성 Job 거부
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import shlex
import subprocess
import sys
import time

CRONJOB = "token-mart-metrics"
NAMESPACE_DEFAULT = "monitoring"
WINDOW_HHMM = (10, 50)            # 설계 §7.5: 10:20 CronJob + activeDeadlineSeconds 1800
CHUNK_DAYS_DEFAULT = 7            # 설계 §7.5 --chunk-days 7 (7×4 = 28 변이 ≤ 64)
CHUNK_DAYS_MAX = 16               # 예산 64 = 16일 × 4 변이/일 (설계 §4.0)
ACTIVE_JOB_PREFIX = "token-mart-"
DEADLINE_PER_CHUNK_S = 1800       # CronJob activeDeadlineSeconds — 청크 7일당 30분
TIMEOUT_RANGE_S = 7200            # 청크 Job activeDeadlineSeconds 상한
POLL_S = 10
TIMEOUT_SINGLE_S = DEADLINE_PER_CHUNK_S + 600     # 서버 데드라인 + 폴링 마진
KST = dt.timezone(dt.timedelta(hours=9), "KST")


def _now_kst() -> dt.datetime:
    """테스트에서 monkeypatch 하는 현재 시각(aware KST)."""
    return dt.datetime.now(KST)


def kubectl(context, args, *, capture=False, input_data=None):
    cmd = ["kubectl", f"--context={context}", "--insecure-skip-tls-verify"] + args
    return subprocess.run(cmd, check=True, text=True, input=input_data,
                          capture_output=capture)


def chunk_ranges(from_d, to_d, chunk_days):
    """[from_d, to_d] inclusive를 chunk_days일 단위 (start, end) inclusive 목록으로 분할."""
    if chunk_days < 1:
        raise ValueError(f"chunk_days must be >= 1: {chunk_days}")
    out = []
    start = from_d
    while start <= to_d:
        end = min(start + dt.timedelta(days=chunk_days - 1), to_d)
        out.append((start, end))
        start = end + dt.timedelta(days=1)
    return out


def window_ok(now, force=False):
    """now(aware KST)가 WINDOW_HHMM(10:50) 이후면 True. force=True면 항상 True."""
    if force:
        return True
    hh, mm = WINDOW_HHMM
    return now.hour * 60 + now.minute >= hh * 60 + mm


def active_mart_jobs(kubectl_json):
    """`kubectl get jobs -o json` 결과에서 이름이 token-mart-* 이고 status.active > 0 인 Job 수."""
    n = 0
    for item in kubectl_json.get("items", []):
        name = item.get("metadata", {}).get("name", "")
        active = item.get("status", {}).get("active", 0) or 0
        if name.startswith(ACTIVE_JOB_PREFIX) and active > 0:
            n += 1
    return n


def build_batch_command(from_d, to_d):
    """컨테이너 args override — ENTRYPOINT(python -m app.batch) 뒤에 붙는 인자만."""
    return ["--from", from_d.isoformat(), "--to", to_d.isoformat()]


def range_deadline_s(n_days):
    """청크 Job activeDeadlineSeconds — 7일당 1800s(설계 해석: 일일 계약 1800s는 '7일 이하 청크'
    산식), 상한 TIMEOUT_RANGE_S. range_deadline_s(7)=1800, (8)=3600, (100)=7200."""
    return min(DEADLINE_PER_CHUNK_S * ((n_days + 6) // 7), TIMEOUT_RANGE_S)


def job_name(cronjob, from_d, to_d, epoch):
    """<cronjob>-rerun-YYYYMMDD-YYYYMMDD-<epoch> (token-mart-metrics-verify 포함 63자 이내)."""
    return f"{cronjob}-rerun-{from_d:%Y%m%d}-{to_d:%Y%m%d}-{epoch}"


def build_job_spec(cronjob_obj, name, args, active_deadline_s=None):
    """CronJob 오브젝트에서 1회성 Job 스펙 생성 + containers[0].args override.

    command는 건드리지 않는다(이미지 ENTRYPOINT python -m app.batch 유지). metadata는 name +
    라벨만 남긴다(uid/resourceVersion 등 서버 필드 제거 — collectors/token-metrics/tools/rerun.py와
    동일 관례: 라벨로 rerun Job을 식별). active_deadline_s=None이면 jobTemplate.spec 값
    (일일 계약 1800) 상속, 값이 있으면 override. ttlSecondsAfterFinished는 없을 때만 86400(1일)으로
    채운다 — CronJob 소유가 아닌 이 1회성 Job은 successfulJobsHistoryLimit의 GC 대상이 아니라
    방치되면 영구 잔존한다(collectors 6b fix1과 동일 관례)."""
    spec = copy.deepcopy(cronjob_obj["spec"]["jobTemplate"]["spec"])
    spec["template"]["spec"]["containers"][0]["args"] = list(args)
    if active_deadline_s is not None:
        spec["activeDeadlineSeconds"] = active_deadline_s
    spec.setdefault("ttlSecondsAfterFinished", 86400)
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": name, "labels": {"app": "token-mart-metrics", "rerun": "1"}},
            "spec": spec}


def wait_job(context, namespace, job_name, timeout_s):
    """Job 완료 폴링 + 파드별 로그 스트리밍. 성공 True / 실패 False.

    backoffLimit=1 재시도 파드까지 각각 스트리밍한다 — 마커 라인(BATCH_RESULT …, 설계 §6.3)이
    운영 기록이므로 가공 없이 그대로 출력. 청크 Job은 날짜별 BATCH_RESULT가 독립 출력되므로
    한 Job 로그 안에 여러 줄이 순서대로 나타난다."""
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
    p.add_argument("-n", "--namespace", default=NAMESPACE_DEFAULT)
    p.add_argument("--cronjob", default=CRONJOB,
                   help=f"대상 CronJob 이름 (기본 {CRONJOB})")
    p.add_argument("--from", dest="from_d", default=None)
    p.add_argument("--to", dest="to_d", default=None)
    p.add_argument("--chunk-days", dest="chunk_days", type=int, default=CHUNK_DAYS_DEFAULT,
                   help=f"청크 일수 (기본 {CHUNK_DAYS_DEFAULT}, 1..{CHUNK_DAYS_MAX})")
    p.add_argument("--force", action="store_true",
                   help="창(10:50 KST) 게이트 무시 — 활성 Job 게이트는 무시 불가")
    return p


def main(argv=None):
    p = build_arg_parser()
    args = p.parse_args(argv)

    if not 1 <= args.chunk_days <= CHUNK_DAYS_MAX:
        p.exit(2, f"--chunk-days는 1..{CHUNK_DAYS_MAX} (변이 예산 64 = 16일×4, 설계 §4.0)\n")
    if bool(args.from_d) != bool(args.to_d):
        p.exit(2, "--from/--to는 쌍으로 지정 (YYYY-MM-DD, KST, inclusive)\n")
    d0 = d1 = None
    if args.from_d:
        try:
            d0, d1 = dt.date.fromisoformat(args.from_d), dt.date.fromisoformat(args.to_d)
        except ValueError:
            p.exit(2, "--from/--to는 YYYY-MM-DD 형식\n")
        if d0 > d1:
            p.exit(2, f"--from({d0}) > --to({d1})\n")

    try:
        now = _now_kst()
        if not window_ok(now, args.force):
            print(f"RERUN REFUSED window (>=10:50 KST) — use --force (now={now:%H:%M} KST)",
                  file=sys.stderr)
            return 2
        res = kubectl(args.context, ["get", "jobs", "-n", args.namespace, "-o", "json"], capture=True)
        n_active = active_mart_jobs(json.loads(res.stdout))
        if n_active > 0:
            print(f"RERUN REFUSED active_jobs={n_active} ({ACTIVE_JOB_PREFIX}* running)", file=sys.stderr)
            return 2

        epoch = int(time.time())
        if d0 is None:
            name = f"{args.cronjob}-manual-{epoch}"
            # args override 없음 — 컨테이너 ENTRYPOINT(python -m app.batch, 인자 없음)가
            # target_date = 실행 시점 기준 어제(KST)를 산정 (app.batch 계약)
            kubectl(args.context, ["create", "job", f"--from=cronjob/{args.cronjob}",
                                   name, "-n", args.namespace])
            return 0 if wait_job(args.context, args.namespace, name, TIMEOUT_SINGLE_S) else 1

        chunks = chunk_ranges(d0, d1, args.chunk_days)
        print(f"[INFO] range {d0}..{d1} → {len(chunks)} chunk(s) × ≤{args.chunk_days}d "
              f"(≤{args.chunk_days * 4} mutations/chunk) — sequential")
        res = kubectl(args.context, ["get", "cronjob", args.cronjob, "-n", args.namespace,
                                     "-o", "json"], capture=True)
        cronjob_obj = json.loads(res.stdout)
        for i, (c0, c1) in enumerate(chunks, 1):
            n_days = (c1 - c0).days + 1
            name = job_name(args.cronjob, c0, c1, epoch)
            deadline = range_deadline_s(n_days)
            job = build_job_spec(cronjob_obj, name, build_batch_command(c0, c1),
                                 active_deadline_s=deadline)
            print(f"[INFO] chunk {i}/{len(chunks)} {c0}..{c1} job={name} deadline={deadline}s")
            kubectl(args.context, ["apply", "-n", args.namespace, "-f", "-"],
                    input_data=json.dumps(job))
            if not wait_job(args.context, args.namespace, name, deadline + 600):
                remaining = chunks[i:]
                if remaining:
                    print(f"[ERROR] chunk {i} failed — 남은 청크 {len(remaining)}개 미실행: "
                          f"{remaining[0][0]}..{remaining[-1][1]} (재실행: --from {remaining[0][0]} "
                          f"--to {remaining[-1][1]})", file=sys.stderr)
                return 1
        return 0
    except subprocess.CalledProcessError as e:
        # kubectl 실패(인증 만료·컨텍스트 오류·API 다운)를 트레이스백 대신 정리된 exit 1로 —
        # collectors/token-metrics/tools/rerun.py fix1과 동일 관례.
        print(f"[ERROR] kubectl 실패 (rc={e.returncode}): {shlex.join(e.cmd)}", file=sys.stderr)
        if e.stderr:
            print(e.stderr.rstrip(), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
