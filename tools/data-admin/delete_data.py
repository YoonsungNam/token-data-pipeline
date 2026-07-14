"""tools/data-admin/delete_data.py — 되돌릴 수 없는 삭제 도구 (§8.3 ②, Plan 5 T3).

fact/mart/view 테이블에서 데이터를 영구 삭제한다. 두 모드:

  --mode date : (date범위[, service]) 기준 fact 일괄 삭제 — 서비스 폐기·오적재 회수
                (§8.4 정정(restatement) 프로토콜의 수동 정정 경로). 대상 =
                fact.raw_token_usage_1d, fact.raw_token_usage_summary_1d 2테이블만.
                **fact.collect_audit_1d는 대상이 아니다** — §8.4의 감사 이력은
                append-only 불변이며, 이 도구가 지우는 원본과 별개로 항상 보존된다
                (삭제 금지). 완료 후 동일 기간 mart rerun이 의무다(§8.3) — 이 도구가
                안내 커맨드를 항상 출력한다.
  --mode user : user_id 축 파기(퇴사자·개인정보 파기 요청) — fact·mart·view 3계층
                상세 테이블에서 직접 ALTER DELETE (25개월치를 mart rerun으로 우회
                재생성하는 것은 비현실적, §8.3 ②). agg_token_*/
                raw_token_usage_summary_1d 등 집계 테이블은 user_id 컬럼이 없어
                (개인 식별 불가 집계) 대상이 아니다. gpu_data.dim_token_user_org
                이력의 파기/가명화는 이 도구의 범위 밖 — 별도 admin 경로(§6.1 보존
                규칙)로 처리한다.

되돌릴 수 없는 삭제이므로 안전장치가 본질이다:
  - dry-run이 기본이다. --yes 없이 실행하면 대상 건수만 조회·출력하고 종료한다
    (exit 0) — client.command()는 절대 호출되지 않는다(SELECT count()만, _dist 경유).
  - --yes를 지정해도 실행 직전 대상 요약을 다시 조회·출력한다(재확인) — 그 다음에만
    ALTER TABLE ... DELETE를 실행한다.
  - 모든 DELETE는 ON CLUSTER(CH_CLUSTER 설정 시)로 나가며, 실행 후
    wait_for_mutations(3s 폴링/300s 타임아웃, CH_CLUSTER 시 clusterAllReplicas)로
    전 레플리카 완료를 기다린다 — mart/token-usage/app/ch.py CHGate의
    delete_day/wait_for_mutations 로직을 이식했다(의존 없이 이 모듈에 자체 포함).
  - user_id/date/service 등 사용자 입력값은 전부 ClickHouse 서버사이드 바인딩
    ({d1:Date} 등 named 파라미터)으로 전달한다 — f-string으로 값을 SQL에 직접
    삽입하지 않는다.
  - 로그는 카운트·테이블명 중심이다 — 요약/완료 메시지 본문에 user_id 원문을
    반복 노출하지 않는다(인자로 받은 값 자체의 존재는 불가피).

DB명은 CH_DB_FACT/CH_DB_DIM/CH_DB_MART 환경변수(mart/token-usage/app/ch.py와 동일
계약, 기본 fact/gpu_data/mart), 접속은 CH_HOST/CH_PORT/CH_USER/CH_PASSWORD/
CH_CLUSTER 환경변수(collectors 계약과 동일)로 결정한다.

사용법:
  # dry-run(기본) — 대상 건수만 출력, 삭제 없음
  python3 delete_data.py --mode date --from 2026-07-01 --to 2026-07-03 [--service S]
  python3 delete_data.py --mode user --user-id U

  # 실제 삭제
  python3 delete_data.py --mode date --from 2026-07-01 --to 2026-07-03 --yes \
      [--context homelab --namespace monitoring]
  python3 delete_data.py --mode user --user-id U --yes

exit: 0 정상(dry-run 포함) / 1 실행 오류(뮤테이션 타임아웃 등) / 2 인자 오류.
"""
import argparse
import datetime as dt
import os
import sys
import time

import clickhouse_connect

DB_FACT = os.getenv("CH_DB_FACT", "fact")          # mart/token-usage/app/ch.py와 동일 계약
DB_DIM = os.getenv("CH_DB_DIM", "gpu_data")         # §9-18 협의 변경 지점
DB_MART = os.getenv("CH_DB_MART", "mart")

MART_RERUN_CMD = (
    "python3 mart/token-usage/tools/rerun.py --context {ctx} --namespace {ns} "
    "--from {d1} --to {d2}"
)


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    return int(raw) if raw.strip() else default


def load_conn_config() -> dict:
    """CH 접속 env — collectors 계약과 동일(CH_HOST/PORT/USER/PASSWORD/CH_CLUSTER).

    호출 시점마다 새로 읽는다(장수명 프로세스 전제인 mart/ch.py의 import-시점
    고정과 달리, 이 도구는 1회성 CLI라 매 invocation이 곧 1회 env 평가다)."""
    return {
        "host": os.getenv("CH_HOST", "localhost"),
        "port": _int_env("CH_PORT", 8123),
        "user": os.getenv("CH_USER", "default"),
        "password": os.getenv("CH_PASSWORD", ""),
        "cluster": os.getenv("CH_CLUSTER", ""),
    }


class Target:
    """삭제 대상 테이블 1개 — local(ALTER DELETE 실행)·dist(dry-run count),
    술어(server-side bind 자리표시자)·파라미터를 동봉한다."""

    def __init__(self, local: str, dist: str, predicate: str, params: dict):
        self.local = local
        self.dist = dist
        self.predicate = predicate
        self.params = params


def date_targets(d1: str, d2: str, service: str | None = None) -> list[Target]:
    """모드 date 대상 — fact 상세+summary 2테이블만.

    fact.collect_audit_1d는 여기 포함하지 않는다 — §8.4 감사 이력은 append-only
    불변이라 삭제 대상이 될 수 없다(이 함수가 의도적으로 다루지 않는 테이블)."""
    predicate = "date BETWEEN {d1:Date} AND {d2:Date}"
    params = {"d1": dt.date.fromisoformat(d1), "d2": dt.date.fromisoformat(d2)}
    if service:
        predicate += " AND service = {s:String}"
        params["s"] = service
    return [
        Target(f"{DB_FACT}.raw_token_usage_1d_local",
               f"{DB_FACT}.raw_token_usage_1d_dist", predicate, dict(params)),
        Target(f"{DB_FACT}.raw_token_usage_summary_1d_local",
               f"{DB_FACT}.raw_token_usage_summary_1d_dist", predicate, dict(params)),
    ]


def user_targets(user_id: str) -> list[Target]:
    """모드 user 대상 — fact 상세 + mart 상세 + view 상세 3테이블.

    agg_token_*/raw_token_usage_summary_1d는 user_id 컬럼이 없는 집계 테이블이라
    (개인 식별 불가) 대상이 아니다. gpu_data.dim_token_user_org 이력도 별도
    admin 경로(§6.1)라 여기 포함하지 않는다."""
    predicate = "user_id = {u:String}"
    params = {"u": user_id}
    return [
        Target(f"{DB_FACT}.raw_token_usage_1d_local",
               f"{DB_FACT}.raw_token_usage_1d_dist", predicate, dict(params)),
        Target(f"{DB_MART}.token_usage_1d_local",
               f"{DB_MART}.token_usage_1d_dist", predicate, dict(params)),
        Target(f"{DB_DIM}.view_token_usage_1d_local",
               f"{DB_DIM}.view_token_usage_1d_dist", predicate, dict(params)),
    ]


class DeleteGate:
    """ON CLUSTER ALTER DELETE + wait_for_mutations.

    mart/token-usage/app/ch.py CHGate.delete_day/wait_for_mutations의 로직을
    이식했다 — 그 모듈에 대한 import 의존 없이 이 파일에 자체 포함(§8.3 계약:
    이 도구는 다른 모듈 배포 이미지와 무관하게 단독 실행되어야 한다)."""

    def __init__(self, client, cluster: str = "", clock=time.monotonic,
                 sleeper=time.sleep, poll_s: int = 3, timeout_s: int = 300):
        self.client = client
        self.cluster = cluster
        self.clock = clock
        self.sleeper = sleeper
        self.poll_s = poll_s
        self.timeout_s = timeout_s

    def _on_cluster(self) -> str:
        return f" ON CLUSTER '{self.cluster}'" if self.cluster else ""

    def _mutation_scope(self) -> str:
        if self.cluster:
            return f"clusterAllReplicas('{self.cluster}', system.mutations)"
        return "system.mutations"

    def count(self, target: Target) -> int:
        """dry-run/재확인용 — 항상 _dist 경유 SELECT count()만 호출(command 미사용)."""
        r = self.client.query(
            f"SELECT count() FROM {target.dist} WHERE {target.predicate}",
            parameters=target.params)
        return int(r.result_rows[0][0]) if r.result_rows else 0

    def delete(self, target: Target) -> None:
        """local 테이블에 ON CLUSTER DELETE 후 전 레플리카 뮤테이션 완료까지 대기."""
        self.client.command(
            f"ALTER TABLE {target.local}{self._on_cluster()} DELETE WHERE {target.predicate}",
            parameters=target.params)
        self.wait_for_mutations(target.local)

    def wait_for_mutations(self, table_local: str) -> None:
        """CH_CLUSTER 설정 시 clusterAllReplicas(cluster, system.mutations)로 전
        레플리카를 폴링(poll_s), timeout_s 초과 시 TimeoutError.
        table_local은 "database.table" 형식."""
        db, tbl = table_local.split(".", 1)
        scope = self._mutation_scope()
        start = self.clock()
        while True:
            r = self.client.query(
                f"SELECT count() FROM {scope} "
                f"WHERE database = %(db)s AND table = %(tbl)s AND is_done = 0",
                parameters={"db": db, "tbl": tbl})
            pending = int(r.result_rows[0][0]) if r.result_rows else 0
            if not pending:
                return
            if self.clock() - start >= self.timeout_s:
                raise TimeoutError(
                    f"wait_for_mutations timeout ({self.timeout_s}s): "
                    f"{table_local} pending={pending}")
            self.sleeper(self.poll_s)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["date", "user"], required=True,
                    help="date = 날짜범위[+service] fact 삭제 / user = user_id 축 파기")
    p.add_argument("--from", dest="from_d", default=None,
                    help="YYYY-MM-DD (mode date 필수, inclusive)")
    p.add_argument("--to", dest="to_d", default=None,
                    help="YYYY-MM-DD (mode date 필수, inclusive)")
    p.add_argument("--service", default=None, help="정본 서비스명 (mode date, 선택)")
    p.add_argument("--user-id", dest="user_id", default=None,
                    help="사내 id (mode user 필수, 공백 불가)")
    p.add_argument("--yes", action="store_true",
                    help="실제 삭제 실행 — 지정하지 않으면 dry-run(대상 건수만 출력)")
    p.add_argument("--context", default="<context>",
                    help="완료 안내에 쓰일 mart rerun 커맨드의 kubectl context 치환값 "
                         "(mode date, 안내 문구 치환용 — 기본 플레이스홀더 <context>)")
    p.add_argument("--namespace", default="monitoring",
                    help="완료 안내에 쓰일 mart rerun 커맨드의 namespace 치환값 "
                         "(mode date, 기본 monitoring)")
    return p


def _print_target_summary(gate: DeleteGate, targets: list[Target], label: str) -> int:
    print(f"[{label}]")
    total = 0
    for t in targets:
        n = gate.count(t)
        total += n
        print(f"  {t.dist}: {n}건")
    print(f"  합계: {total}건")
    return total


def main(argv=None, client=None, clock=time.monotonic, sleeper=time.sleep) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.mode == "date":
        if not args.from_d or not args.to_d:
            parser.exit(2, "--mode date는 --from/--to 필수(YYYY-MM-DD, inclusive)\n")
        try:
            d0, d1 = dt.date.fromisoformat(args.from_d), dt.date.fromisoformat(args.to_d)
        except ValueError:
            parser.exit(2, "--from/--to는 YYYY-MM-DD 형식이어야 합니다\n")
        if d0 > d1:
            parser.exit(2, f"--from({d0}) > --to({d1})\n")
        targets = date_targets(args.from_d, args.to_d, args.service)
    else:
        if not args.user_id or not args.user_id.strip():
            parser.exit(2, "--mode user는 --user-id 필수(공백 불가)\n")
        targets = user_targets(args.user_id)

    conn = load_conn_config()
    ch_client = client if client is not None else clickhouse_connect.get_client(
        host=conn["host"], port=conn["port"], username=conn["user"],
        password=conn["password"])
    gate = DeleteGate(ch_client, cluster=conn["cluster"], clock=clock, sleeper=sleeper)

    _print_target_summary(gate, targets, "대상 요약 (dry-run)")

    if not args.yes:
        print("[DRY-RUN] --yes 없이 종료 — 삭제는 수행되지 않았습니다 (되돌릴 수 없는 작업).")
        return 0

    print()
    _print_target_summary(gate, targets, "대상 요약 재확인 — 아래를 실제 삭제합니다")
    print("[실행] 되돌릴 수 없는 삭제를 시작합니다 (ON CLUSTER ALTER DELETE + 뮤테이션 대기).")
    try:
        for t in targets:
            gate.delete(t)
    except Exception as exc:
        print(f"[ERROR] 삭제 실행 실패: {exc}", file=sys.stderr)
        return 1

    print("[완료] 삭제를 마쳤습니다.")
    if args.mode == "date":
        print("[필수 후속조치] 동일 기간 mart rerun 실행이 의무입니다(§8.3):")
        print("  " + MART_RERUN_CMD.format(ctx=args.context, ns=args.namespace,
                                            d1=args.from_d, d2=args.to_d))
    else:
        print(f"[안내] {DB_DIM}.dim_token_user_org 행의 파기/가명화는 이 도구의 범위 밖입니다 "
              "— 별도 admin 경로로 처리하세요(§6.1 보존 규칙).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
