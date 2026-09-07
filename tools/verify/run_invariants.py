"""tools/verify/run_invariants.py — company 검증 성공 기준 러너
(docs/operations/company-verify.md "성공 기준 체크리스트"의 실행형).

tools/verify/invariants.sql을 읽어 {FACT}/{DIM}/{MART}/{DATE} 토큰을 치환한 뒤
ClickHouse에 실행한다. 위반 행이 하나라도 있으면 출력 후 exit 1, 없으면
"ALL INVARIANTS PASS"를 출력하고 exit 0(mart/token-usage/tests/e2e/
verify_expected_results.sql과 동일한 "빈 결과 = 통과" 패턴).

DB명은 CH_DB_FACT/CH_DB_DIM/CH_DB_MART 환경변수(mart/token-usage/app/ch.py와
동일 계약, 기본 fact/gpu_data/mart)로 결정한다:
  - 1단계(격리) 실행: CH_DB_FACT=token_verify_fact CH_DB_DIM=token_verify_dim
    CH_DB_MART=token_verify_mart python3 tools/verify/run_invariants.py --date ...
  - 2단계(정규)/stage 실행: 기본값(env 미설정) 그대로 실행.

CH 접속은 CH_HOST/CH_PORT/CH_USER/CH_PASSWORD/CH_CLUSTER 환경변수(collectors
계약과 동일)로 결정한다. invariants.sql은 읽기 전용 SELECT만 실행하므로
CH_CLUSTER는 접속 정보로만 취급되고 ON CLUSTER 등 뮤테이션 절차에는 쓰이지 않는다.

사용법:
  python3 tools/verify/run_invariants.py --date 2026-07-14
  python3 tools/verify/run_invariants.py            # --date 생략 시 어제(KST)
  python3 tools/verify/run_invariants.py --sql tools/verify/invariants_metrics.sql --date 2026-09-03
                                                    # --sql: 다른 불변식 파일(기본 invariants.sql)

exit: 0 = 전 불변식 통과(위반 0건) / 1 = 위반 존재 / 2 = 인자 오류.
"""
import argparse
import datetime as dt
import os
import pathlib
import sys
from datetime import datetime, timedelta, timezone

import clickhouse_connect

DB_FACT_DEFAULT = "fact"          # mart/token-usage/app/ch.py와 동일 계약
DB_DIM_DEFAULT = "gpu_data"       # §9-18 협의 변경 지점
DB_MART_DEFAULT = "mart"

SQL_PATH = pathlib.Path(__file__).resolve().parent / "invariants.sql"

KST = timezone(timedelta(hours=9))


def now_kst() -> datetime:
    """CH DateTime('Asia/Seoul') 컬럼용과 동일 계약 — aware KST
    (mart/token-usage/app/ch.py now_kst()와 동일 관례, naive datetime 금지)."""
    return datetime.now(KST)


def default_date_kst(now: datetime) -> str:
    """--date 미지정 시 기본값 — now(aware) 기준 어제(KST) 날짜 문자열."""
    return (now.astimezone(KST) - timedelta(days=1)).date().isoformat()


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    return int(raw) if raw.strip() else default


def load_conn_config() -> dict:
    """CH 접속 env — collectors 계약과 동일(CH_HOST/PORT/USER/PASSWORD/CH_CLUSTER).
    호출 시점마다 새로 읽는다(1회성 CLI — delete_data.py와 동일 관례)."""
    return {
        "host": os.getenv("CH_HOST", "localhost"),
        "port": _int_env("CH_PORT", 8123),
        "user": os.getenv("CH_USER", "default"),
        "password": os.getenv("CH_PASSWORD", ""),
        "cluster": os.getenv("CH_CLUSTER", ""),
    }


def load_db_config() -> dict:
    """DB명 env — mart/token-usage/app/ch.py와 동일 계약(CH_DB_FACT/DIM/MART)."""
    return {
        "fact": os.getenv("CH_DB_FACT", DB_FACT_DEFAULT),
        "dim": os.getenv("CH_DB_DIM", DB_DIM_DEFAULT),
        "mart": os.getenv("CH_DB_MART", DB_MART_DEFAULT),
    }


def render(sql: str, fact: str, dim: str, mart: str, date: str) -> str:
    """순수 치환 함수 — {FACT}/{DIM}/{MART}/{DATE} 토큰을 실제 값으로 바꾼다.
    부수효과 없음(테스트 대상) — 토큰 외 SQL 본문은 그대로 통과."""
    return (
        sql.replace("{FACT}", fact)
           .replace("{DIM}", dim)
           .replace("{MART}", mart)
           .replace("{DATE}", date)
    )


def load_sql(path: pathlib.Path = SQL_PATH) -> str:
    return path.read_text(encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", default=None,
                    help="YYYY-MM-DD (기본: 어제, KST). 검증 대상 단일일.")
    p.add_argument("--sql", default=None, metavar="PATH",
                    help="불변식 SQL 파일 경로 (기본: tools/verify/invariants.sql — "
                         "메트릭 파이프라인은 tools/verify/invariants_metrics.sql)")
    return p


def _print_violations(rows: list[tuple], date_str: str, dbs: dict,
                      sql_name: str = "invariants.sql") -> None:
    # detail은 invariants.sql의 각 검사 SQL이 만든 문자열을 그대로(verbatim) 찍는다.
    # §5.6/v1.4 로깅 계약("모든 로그에 레코드 페이로드와 user_id 원문을 남기지
    # 않는다")은 SQL 쪽 책임이다 — 각 검사 SQL은 detail에 user_id/user_name
    # 원문을 절대 넣지 않는다(특히 identified_name_leak — 위반 건수만 집계해
    # 노출, PII 원문 0). 여기서 다시 필터링하는 건 과설계이므로 하지 않는다.
    print(f"[FAIL] {len(rows)}건의 불변식 위반 발견 "
          f"(date={date_str}, DBs={dbs['fact']}/{dbs['dim']}/{dbs['mart']}, sql={sql_name})")
    header = f"{'check_name':<28} {'bad_count':>10}  detail"
    print(header)
    print("-" * len(header))
    for row in rows:
        check_name, detail, bad_count = row[0], row[1], row[2]
        print(f"{str(check_name):<28} {str(bad_count):>10}  {detail}")


def main(argv=None, client=None, now_fn=now_kst) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.date is not None:
        try:
            dt.date.fromisoformat(args.date)
        except ValueError:
            parser.exit(2, "--date는 YYYY-MM-DD 형식이어야 합니다\n")
        date_str = args.date
    else:
        date_str = default_date_kst(now_fn())

    # --sql: 다른 불변식 파일(메트릭 파이프라인 invariants_metrics.sql). 미지정 시 SQL_PATH —
    # 기존 호출 경로·출력은 그대로(additive, 설계 §7.3). 상대 경로는 cwd 기준.
    sql_path = pathlib.Path(args.sql) if args.sql else SQL_PATH
    if not sql_path.is_file():
        parser.exit(2, f"--sql 파일을 찾을 수 없습니다: {sql_path}\n")

    dbs = load_db_config()
    sql = render(load_sql(sql_path), fact=dbs["fact"], dim=dbs["dim"], mart=dbs["mart"],
                 date=date_str)

    conn = load_conn_config()
    ch_client = client if client is not None else clickhouse_connect.get_client(
        host=conn["host"], port=conn["port"], username=conn["user"],
        password=conn["password"])

    # distributed_product_mode='global': 불변식 SQL도 _dist 대상 서브쿼리/조인을 각
    # 샤드에서 전역 조회하도록 강제 — insert_select와 동일 취지(로컬 샤드만 보고 부분
    # 집계하는 사고 방지, §4.0 분산 조인 표준).
    # B5(M-6, additive) — --sql(invariants_metrics.sql)만 join_use_nulls=0을 추가로 고정한다
    # (그 파일 헤더 :33이 이 의존을 명시 — LEFT JOIN 미스는 ''/0). 기본 invariants.sql 경로는
    # 이 설정이 없어도 동일하게 동작하므로(둘 다 zero-diff 대상) 손대지 않는다 — 동작·출력 불변.
    settings = {"distributed_product_mode": "global"}
    if args.sql:
        settings["join_use_nulls"] = 0
    result = ch_client.query(sql, settings=settings)
    rows = list(result.result_rows or [])

    if rows:
        _print_violations(rows, date_str, dbs, sql_path.name)
        return 1

    print(f"ALL INVARIANTS PASS (date={date_str}, DBs={dbs['fact']}/{dbs['dim']}/{dbs['mart']}, "
          f"sql={sql_path.name})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
