"""ClickHouse 멱등 시퀀스 프리미티브 — CHGate (mart/token-usage/app/ch.py 클론, 설계 §6.1).

호출자(steps.py/batch.py — T3~T7)의 시퀀스: exists(존재 확인 — 없으면 delete 스킵,
§4.0 뮤테이션 절감) → delete_day(ALTER TABLE ... DELETE, ON CLUSTER + wait_for_mutations 내장)
→ insert_select(항상 _dist 경유, insert_distributed_sync=1 + insert_deduplicate=0 — 재삽입
중복제거 차단 + distributed_product_mode=global — §4.0 분산 조인 표준)
→ verify_count(재시도 RETRY_COUNT×RETRY_INTERVAL_S).

DB명은 아래 상수 **5종만** 쓴다(테이블명 하드코딩 금지, f"{DB_MART}.agg_token_model_cost_1d_dist"
형식). 토큰 측 읽기 계약 3테이블(token_usage_1d·agg_token_service_1d → DB_TOKEN_MART,
dim_token_service → DB_TOKEN_DIM)은 CH_DB_TOKEN_* 로 분리돼, company-verify 격리 DB
(token_verify_*)에서 검증할 때 운영 DB를 가리키게 할 수 있다(설계 §6.1·§7.5 — 미설정이면
CH_DB_MART/CH_DB_DIM을 따른다).

상수는 모듈 import 시점에 1회 평가된다. steps.py의 SQL 상수는 이 모듈을 import하는
시점에 f-string으로 DB명이 보간되어 문자열로 고정된다 — env는 프로세스 시작 시 1회
읽히므로(CronJob env 주입 전제) 런타임 중 재평가되지 않는다. 이는 의도된 동작이다.

`SESSION_SETTINGS = {"join_use_nulls": 0}`을 세션 전체(클라이언트 기본값 + 각 SELECT/INSERT
호출)에 명시적으로 고정한다: EXPECTED_SQL/verify_count가 실행하는 카운트 쿼리와 INSERT...SELECT가
LEFT JOIN 미스를 **동일하게** 키잉해야 한다 — 양쪽 모두 canon()의 `a.canonical = ''`/
`if(a.canonical = '', ...)` 가드와 `ga.has_rows = 0`류 기본값이 join_use_nulls=0(미스=빈
문자열/0)을 전제하기 때문이다. 서버/프로파일 기본값에 기대지 않고 클라이언트 생성 시
(`get_client(settings=...)`)와 문장별(_count/query/insert_select) 양쪽에서 명시한다 —
문장별 지정은 호출자가 클라이언트를 주입(테스트·company-verify)해도 동일하게 적용되도록 하기
위함이다.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

import clickhouse_connect

from app.config import Config

DB_FACT = os.getenv("CH_DB_FACT", "fact")                 # raw_token_metrics_*_1d (Plan 6a A)
DB_DIM = os.getenv("CH_DB_DIM", "gpu_data")               # dim_token_* 4종 + dim_token_metrics_service (6a B/D)
DB_MART = os.getenv("CH_DB_MART", "mart")                 # 6c가 쓰는 mart 4테이블 (6a C)
DB_TOKEN_MART = os.getenv("CH_DB_TOKEN_MART", DB_MART)    # 읽기 계약: token_usage_1d·agg_token_service_1d
DB_TOKEN_DIM = os.getenv("CH_DB_TOKEN_DIM", DB_DIM)       # 읽기 계약: dim_token_service

# join_use_nulls=0 세션 고정 — canon()/`= ''` 가드와 LEFT JOIN 미스 기본값이 이를 전제(모듈 docstring).
SESSION_SETTINGS = {"join_use_nulls": 0}

KST = timezone(timedelta(hours=9))


def now_kst() -> datetime:
    """CH DateTime('Asia/Seoul') 컬럼용 — aware KST. naive datetime을 clickhouse-connect가
    호스트 TZ로 해석하면 KST 벽시계와 어긋난다 (항상 tzinfo 유지)."""
    return datetime.now(KST)


class CHGate:
    def __init__(self, cfg: Config, client=None, clock=time.monotonic, sleeper=time.sleep):
        self.cfg = cfg
        self.client = client or clickhouse_connect.get_client(
            host=cfg.ch_host, port=cfg.ch_port, username=cfg.ch_user,
            password=cfg.ch_password, settings=SESSION_SETTINGS)
        self.clock = clock
        self.sleeper = sleeper

    def _on_cluster(self) -> str:
        return f" ON CLUSTER '{self.cfg.ch_cluster}'" if self.cfg.ch_cluster else ""

    def _mutation_scope(self) -> str:
        if self.cfg.ch_cluster:
            return f"clusterAllReplicas('{self.cfg.ch_cluster}', system.mutations)"
        return "system.mutations"

    def _count(self, table_dist: str, date: str) -> int:
        r = self.client.query(
            f"SELECT count() FROM {table_dist} WHERE date = %(d)s",
            parameters={"d": date}, settings=SESSION_SETTINGS)
        return int(r.result_rows[0][0]) if r.result_rows else 0

    def exists(self, table_dist: str, date: str) -> bool:
        """존재 확인 SELECT — False면 호출자가 delete_day를 스킵한다 (§4.0 뮤테이션 절감).
        batch.plan_mutations(T5)가 날짜 전체 × 4테이블에 대해 이 값을 합산해 예산과 비교한다."""
        return self._count(table_dist, date) > 0

    def delete_day(self, table_local: str, date: str, extra_pred: str = "") -> None:
        """local 테이블에 ON CLUSTER DELETE 후 전 레플리카 뮤테이션 완료까지 대기.
        extra_pred: created_by 등 추가 조건 — 'AND ...' 형태로 전달."""
        pred = "date = %(d)s" + (f" {extra_pred}" if extra_pred else "")
        self.client.command(
            f"ALTER TABLE {table_local}{self._on_cluster()} DELETE WHERE {pred}",
            parameters={"d": date})
        self.wait_for_mutations(table_local)

    def wait_for_mutations(self, table_local: str) -> None:
        """CH_CLUSTER 설정 시 clusterAllReplicas(cluster, system.mutations)로 전 레플리카를
        폴링(3s), 300s 초과 시 TimeoutError. table_local은 "database.table" 형식."""
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
            if self.clock() - start >= self.cfg.mutation_timeout_s:
                raise TimeoutError(
                    f"wait_for_mutations timeout ({self.cfg.mutation_timeout_s}s): "
                    f"{table_local} pending={pending}")
            self.sleeper(self.cfg.mutation_poll_s)

    def insert_select(self, sql: str, params: dict | None = None) -> int:
        """INSERT INTO ... SELECT 실행 — 항상 _dist 경유(co-location).
        insert_deduplicate=0 필수(재삽입 중복제거 차단 — Global Constraints).
        cfg.insert_quorum이 설정된 경우만 insert_quorum 설정을 포함한다.
        distributed_product_mode='global': GLOBAL LEFT JOIN이 각 샤드에서 dim을
        전역 조회하도록 강제(로컬 샤드만 보고 부분 조인하는 사고 방지 — §4.0 분산 조인 표준).
        SESSION_SETTINGS(join_use_nulls=0)를 병합 — LEFT JOIN 미스가 EXPECTED_SQL/verify_count와
        동일하게 키잉되도록(모듈 docstring)."""
        settings = {**SESSION_SETTINGS, "insert_distributed_sync": 1, "insert_deduplicate": 0,
                    "distributed_product_mode": "global"}
        if self.cfg.insert_quorum:
            settings["insert_quorum"] = self.cfg.insert_quorum
        result = self.client.command(sql, parameters=params, settings=settings)
        written = getattr(result, "written_rows", None)
        if written is None:
            raise RuntimeError(
                "insert_select: written_rows 미획득 — 드라이버 반환형 확인 필요 "
                "(재실행 폴백은 이중 적재 위험으로 금지)")
        return int(written)

    def verify_count(self, table_dist: str, date: str, expected: int) -> tuple[bool, int]:
        """RETRY_COUNT회(간격 RETRY_INTERVAL_S)까지 재시도. actual>=expected면 통과
        (초과 시에도 통과 — 중복 적재 징후 WARN 판단은 호출자 책임)."""
        actual = self._count(table_dist, date)
        attempt = 1
        while actual < expected and attempt < self.cfg.retry_count:
            self.sleeper(self.cfg.retry_interval_s)
            actual = self._count(table_dist, date)
            attempt += 1
        return actual >= expected, actual

    def query(self, sql: str, params: dict | None = None) -> list[tuple]:
        """M0 커버리지·예산 선조회·인라인 검증용 범용 SELECT."""
        r = self.client.query(sql, parameters=params, settings=SESSION_SETTINGS)
        return [tuple(row) for row in (r.result_rows or [])]

    def describe(self, table_dist: str) -> list[str]:
        """읽기 계약 프리플라이트용(설계 §6.1·§7.5) — DESCRIBE TABLE의 컬럼명을 선언 순으로.
        테이블 부재는 [] (EXISTS TABLE 선조회 — 드라이버 예외 메시지 파싱 없이 부재를 구분;
        preflight.missing_columns가 `<table>.*`로 보고). SELECT 권한은 SHOW TABLES/COLUMNS를
        함의하므로 GRANT(Plan 6a accounts.sql)로 충분하다."""
        found = self.query(f"EXISTS TABLE {table_dist}")
        if not found or not int(found[0][0]):
            return []
        return [str(row[0]) for row in self.query(f"DESCRIBE TABLE {table_dist}")]
