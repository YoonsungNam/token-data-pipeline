"""token-metrics 수기(manual-v0) CSV 적재 도구 — 전달 경로 P0 = k8s Job (설계 §5.5 · §5.6).

워크스테이션의 CSV 3파일(gpu·serving·선택 engine)을 ConfigMap `token-metrics-manual-<ts>`로
올리고, CronJob `token-metrics-collector` 템플릿에서 1회성 Job을 만들어(`/manual` 볼륨 마운트 +
command `python -m app.main --manual-gpu /manual/gpu.csv --manual-serving /manual/serving.csv
[--manual-engine /manual/engine.csv] --from --to [--service] [--replace] [--generated-at]`)
로그를 스트리밍한 뒤 **완료·실패·예외 어느 경로에서든** ConfigMap을 삭제한다(--keep-configmap 제외).
운영자 워크스테이션에는 kubectl(대상 context)만 있으면 된다 — ClickHouse 직접 접근·프록시·CA 불필요.

CSV 내용은 검증하지 않는다(BOM 제거만) — 헤더 바이트 일치·주석·숫자·서비스 등록은 파드 안
app.manual(T7)·app.normalize(T3) 한 곳의 책임. 앵커가 있는 (date, service)는 --replace 없이는
SKIPPED reason=already_loaded (§5.5 안전 기본값).

CSV는 리포에 커밋하지 말 것 — 실제 제출 파일은 *manual_metrics*.csv 이름으로 저장(.gitignore, §7.2).
템플릿: docs/templates/token_metrics_manual_v0_{gpu,serving,engine}.csv

사용법:
  python3 tools/manual_load.py --context prod --from 2026-08-26 --to 2026-08-31 \
      --gpu ~/metrics/gpu.csv --serving ~/metrics/serving.csv --engine ~/metrics/engine.csv \
      --generated-at 2026-09-01T09:00:00+09:00
  python3 tools/manual_load.py --context prod --from 2026-08-26 --to 2026-08-26 \
      --gpu ~/metrics/gpu.csv --serving ~/metrics/serving.csv --service "Mock Service A" --replace

옵션:
  --context         kubectl context (필수)
  --namespace       기본 monitoring
  --cronjob         템플릿 CronJob 이름 (기본 token-metrics-collector — company-verify는
                    token-metrics-collector-verify)
  --from/--to       YYYY-MM-DD, KST, 둘 다 inclusive. 필수 쌍 (범위 밖 행은 파드가 rows_outside_range 로 셈)
  --gpu/--serving   CSV 경로 (필수). --engine 은 선택 (엔진 자기신고 — 없으면 engine_type '')
  --service         단일 서비스만 (endpoints.yaml의 service 정본)
  --replace         앵커 존재 (date, service) 도 교체 (§5.4 DELETE summary→gpu→serving 후 INSERT)
  --generated-at    ISO 8601 (+09:00 권장) — 없으면 파드가 적재 시각을 쓴다
  --timeout-s       Job 완료 대기 상한 (기본 3600 = activeDeadlineSeconds 3000 + 600)
  --keep-configmap  완료 후 ConfigMap 을 지우지 않는다 (디버그 — 정리 명령을 출력)

종료코드: 0 Job Complete / 1 Job Failed·타임아웃 / 2 사용법·파일 부재·CSV 합계 > 900000 bytes
완료 후 동일 날짜 범위의 mart-metrics rerun 은 의무(§6.3) — 명령을 출력만 하고 체인하지 않는다
(실행 창 10:50 KST·활성 mart Job 0 검사는 mart rerun 자신의 책임).
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
CONFIGMAP_PREFIX = "token-metrics-manual-"
TS_FORMAT = "%Y%m%d%H%M%S"        # 14자리 KST — ConfigMap·Job 이름 공용 (DNS-1123: 소문자·숫자·하이픈)
MOUNT_PATH = "/manual"            # 파드 안 CSV 경로 = MOUNT_PATH + "/" + FILE_KEYS[i] (T7 CLI 계약)
VOLUME_NAME = "manual"            # cronjob.yaml volumes [0] endpoints · [1] ca-bundle 뒤에 [2] 로 append
FILE_KEYS = ("gpu.csv", "serving.csv", "engine.csv")
LABELS = {"app": CRONJOB, "manual": "1"}
MAX_CONFIGMAP_BYTES = 900_000     # k8s ConfigMap 1MiB 한도 여유 (create 사용 — apply 의 last-applied 주석 없음)
REPLACE_DAYS_MAX = 15             # MetricsWriter 변이 예산 45 = 15일 × 3(summary·gpu·serving) — --replace 범위가
                                   # 예산을 넘지 않게 (tools/rerun.py CHUNK_DAYS_MAX 와 같은 산식이지만 파일은 독립 사본)
POLL_S = 10
TIMEOUT_S = 3000 + 600            # 서버 activeDeadlineSeconds(§5.2) + 폴링 마진 600
KST = dt.timezone(dt.timedelta(hours=9))
MART_RERUN = "mart/token-metrics/tools/rerun.py"   # Plan 6c 산출 경로 — 안내만 (체인 없음)


def kubectl(context, args, *, capture=False, input_data=None):
    cmd = ["kubectl", f"--context={context}", "--insecure-skip-tls-verify"] + list(args)
    return subprocess.run(cmd, check=True, text=True, input=input_data,
                          capture_output=capture)


def now_kst():
    """aware KST 현재 시각 — 테스트는 이 함수를 페이크로 바꾼다 (datetime.now 는 C 타입이라 불가)."""
    return dt.datetime.now(KST)


def timestamp(now):
    """aware datetime → KST 14자리. naive 는 거부 (KST 규율)."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("timestamp: aware datetime required (KST)")
    return now.astimezone(KST).strftime(TS_FORMAT)


def configmap_name(now_kst):
    return CONFIGMAP_PREFIX + timestamp(now_kst)


def job_name(cronjob, ts):
    # token-metrics-collector-verify-manual-YYYYmmddHHMMSS = 52자 ≤ 63 (DNS-1123 label)
    return f"{cronjob}-manual-{ts}"


def read_manual_files(gpu, serving, engine):
    """CSV → {ConfigMap 키: 텍스트}. utf-8-sig 로 BOM 제거, universal newline 으로 CRLF→LF.
    내용은 검증하지 않는다(파드 안 T7 파서 책임). 부재 파일은 FileNotFoundError 그대로."""
    files = {
        "gpu.csv": pathlib.Path(gpu).read_text(encoding="utf-8-sig"),
        "serving.csv": pathlib.Path(serving).read_text(encoding="utf-8-sig"),
    }
    if engine is not None:
        files["engine.csv"] = pathlib.Path(engine).read_text(encoding="utf-8-sig")
    return files


def total_bytes(files):
    """ConfigMap data 에 실리는 UTF-8 바이트 합계 (BOM 제거 후)."""
    return sum(len(v.encode("utf-8")) for v in files.values())


def build_configmap(name, files):
    unknown = set(files) - set(FILE_KEYS)
    if unknown:
        raise ValueError(f"build_configmap: unknown keys {sorted(unknown)} (allowed {FILE_KEYS})")
    return {"apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": name, "labels": dict(LABELS)},
            "data": dict(files)}


def build_manual_command(from_d, to_d, *, engine, service, replace, generated_at):
    """T7 manual 모드 CLI (§5.5) — 인자 순서 고정: gpu·serving → [engine] → from/to → [service] → [replace] → [generated-at]."""
    cmd = ["python", "-m", "app.main",
           "--manual-gpu", f"{MOUNT_PATH}/gpu.csv",
           "--manual-serving", f"{MOUNT_PATH}/serving.csv"]
    if engine:
        cmd += ["--manual-engine", f"{MOUNT_PATH}/engine.csv"]
    cmd += ["--from", from_d, "--to", to_d]
    if service:
        cmd += ["--service", service]
    if replace:
        cmd += ["--replace"]
    if generated_at:
        cmd += ["--generated-at", generated_at]
    return cmd


def build_job_spec(cronjob_obj, job_name, command, configmap_name):
    """CronJob 오브젝트 → 1회성 Job 스펙: containers[0].command override + /manual 볼륨 append.

    metadata 는 name + 라벨만 (uid/resourceVersion/namespace 등 서버 필드 제거).
    activeDeadlineSeconds 는 jobTemplate.spec 값(3000, §5.2) 그대로 상속.
    volumes/volumeMounts 는 T8 계약 순서([0] endpoints, [1] ca-bundle) 뒤 [2] 에 append."""
    spec = copy.deepcopy(cronjob_obj["spec"]["jobTemplate"]["spec"])
    pod = spec["template"]["spec"]
    volumes = pod.setdefault("volumes", [])
    if any(v.get("name") == VOLUME_NAME for v in volumes):
        raise ValueError(f"build_job_spec: volume '{VOLUME_NAME}' already present in CronJob template")
    container = pod["containers"][0]
    container["command"] = list(command)
    volumes.append({"name": VOLUME_NAME, "configMap": {"name": configmap_name}})
    container.setdefault("volumeMounts", []).append(
        {"name": VOLUME_NAME, "mountPath": MOUNT_PATH, "readOnly": True})
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": job_name, "labels": dict(LABELS)},
            "spec": spec}


def wait_job(context, namespace, job_name, timeout_s):
    """Job 완료 폴링 + 파드 로그 스트리밍. 성공 True / 실패·타임아웃 False. (T9 rerun.py 와 동일 본문)

    backoffLimit=0(§5.2)이라 파드는 1개지만, 파드 집합 순회 골격은 기존 모듈과 동일하게 둔다 —
    마커 라인(MANUAL_INPUT/SERVICE_RESULT/BATCH_RESULT)이 운영 기록이므로 가공 없이 그대로 출력."""
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


def delete_configmap(context, namespace, name):
    """finally 경로용 — 실패해도 예외를 내지 않고 WARN + 수동 삭제 명령만 안내 (종료코드 불변)."""
    try:
        kubectl(context, ["delete", "configmap", name, "-n", namespace, "--ignore-not-found"])
        return True
    except (subprocess.CalledProcessError, OSError):
        print(f"[WARN] ConfigMap 삭제 실패 — 수동 삭제: kubectl --context={context} "
              f"delete configmap {name} -n {namespace}", file=sys.stderr)
        return False


def build_arg_parser():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", default="monitoring")
    p.add_argument("--cronjob", default=CRONJOB,
                   help=f"템플릿 CronJob 이름 (기본 {CRONJOB}; company-verify는 {CRONJOB}-verify)")
    p.add_argument("--from", dest="from_d", required=True, help="YYYY-MM-DD (KST, inclusive)")
    p.add_argument("--to", dest="to_d", required=True, help="YYYY-MM-DD (KST, inclusive)")
    p.add_argument("--gpu", required=True, help="gpu CSV (템플릿 token_metrics_manual_v0_gpu.csv)")
    p.add_argument("--serving", required=True, help="serving CSV (템플릿 token_metrics_manual_v0_serving.csv)")
    p.add_argument("--engine", default=None, help="engine CSV (선택, 템플릿 token_metrics_manual_v0_engine.csv)")
    p.add_argument("--service", default=None)
    p.add_argument("--replace", action="store_true")
    p.add_argument("--generated-at", dest="generated_at", default=None,
                   help="ISO 8601 제출 시각 (+09:00 권장) — 없으면 파드가 적재 시각을 쓴다")
    p.add_argument("--timeout-s", dest="timeout_s", type=int, default=TIMEOUT_S,
                   help=f"Job 완료 대기 상한 초 (기본 {TIMEOUT_S})")
    p.add_argument("--keep-configmap", dest="keep_configmap", action="store_true",
                   help="완료 후 ConfigMap 을 지우지 않음 (디버그)")
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
    if args.replace and (d1 - d0).days + 1 > REPLACE_DAYS_MAX:
        p.exit(2, f"[ERROR] --replace는 한 번에 {REPLACE_DAYS_MAX}일 이하만 가능합니다(변이 예산 45/3) — "
                  f"--from/--to 범위를 나눠 제출\n")
    from_s, to_s = d0.isoformat(), d1.isoformat()

    # 파일 존재 → 읽기(BOM 제거) → 크기 가드 — 전부 kubectl 호출 전 (실패 시 클러스터 무변경)
    paths = [pathlib.Path(args.gpu), pathlib.Path(args.serving)]
    engine_path = pathlib.Path(args.engine) if args.engine else None
    if engine_path is not None:
        paths.append(engine_path)
    for path in paths:
        if not path.is_file():
            p.exit(2, f"[ERROR] 파일 없음: {path}\n")
    try:
        files = read_manual_files(paths[0], paths[1], engine_path)
    except UnicodeDecodeError:
        p.exit(2, "[ERROR] UTF-8 아님 — 엑셀에서는 'CSV UTF-8'로 저장 후 다시 제출\n")
    n_bytes = total_bytes(files)
    if n_bytes > MAX_CONFIGMAP_BYTES:
        p.exit(2, f"[ERROR] CSV 합계 {n_bytes} bytes > {MAX_CONFIGMAP_BYTES} — 날짜 범위를 나눠 제출\n")

    ctx, ns = args.context, args.namespace
    now = now_kst()
    ts = timestamp(now)
    cm_name = configmap_name(now)
    job = job_name(args.cronjob, ts)                     # ConfigMap 과 같은 ts
    print(f"[INFO] configmap={cm_name} job={job} files={','.join(files)} bytes={n_bytes}", flush=True)

    rc = 1
    try:
        # create: apply 는 last-applied 주석에 본문을 한 번 더 저장해 etcd 요청 상한을 넘길 수 있다 (설계 해석 a)
        kubectl(ctx, ["create", "-n", ns, "-f", "-"], input_data=json.dumps(build_configmap(cm_name, files)))
        try:
            res = kubectl(ctx, ["get", "cronjob", args.cronjob, "-n", ns, "-o", "json"], capture=True)
            cronjob_obj = json.loads(res.stdout)
            command = build_manual_command(from_s, to_s, engine=engine_path is not None,
                                           service=args.service, replace=args.replace,
                                           generated_at=args.generated_at)
            kubectl(ctx, ["apply", "-n", ns, "-f", "-"],
                    input_data=json.dumps(build_job_spec(cronjob_obj, job, command, cm_name)))
            rc = 0 if wait_job(ctx, ns, job, args.timeout_s) else 1
        finally:
            # 성공·실패·예외(Ctrl-C 포함) 어느 경로에서든 ConfigMap 정리 — Job 오브젝트는 남긴다 (로그 재조회용)
            if args.keep_configmap:
                print(f"[INFO] ConfigMap 보존(--keep-configmap) — 정리: kubectl --context={ctx} "
                      f"delete configmap {cm_name} -n {ns}", flush=True)
            else:
                if rc != 0:
                    print(f"[WARN] Job이 아직 실행 중이면 입력 ConfigMap 삭제로 실패합니다 — 상태: "
                          f"kubectl --context={ctx} get job {job} -n {ns}", file=sys.stderr, flush=True)
                delete_configmap(ctx, ns, cm_name)
    except subprocess.CalledProcessError as e:
        # kubectl 실패(인증 만료·컨텍스트 오류·API 다운)를 트레이스백 대신 정리된 exit 1로 —
        # ConfigMap create 자체가 실패했으면 위 finally 는 실행되지 않는다(아직 아무것도 만들지 않았다).
        print(f"[ERROR] kubectl 실패 (rc={e.returncode}): {shlex.join(e.cmd)}", file=sys.stderr)
        if e.stderr:
            print(e.stderr.rstrip(), file=sys.stderr)
        return 1

    if rc == 0:
        # §6.3: manual 적재 후 동일 날짜 범위 mart-metrics rerun 은 의무 — 안내만 (창 검사는 mart 측 책임)
        mart_cmd = ["python3", MART_RERUN, "--context", ctx, "--namespace", ns,
                    "--from", from_s, "--to", to_s]
        print("[NEXT] manual 적재 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3): "
              + shlex.join(mart_cmd), flush=True)
    print(f"[INFO] Job 오브젝트는 남김(로그 재조회용) — 정리: kubectl --context={ctx} "
          f"delete job {job} -n {ns}", flush=True)
    return rc


if __name__ == "__main__":
    sys.exit(main())
