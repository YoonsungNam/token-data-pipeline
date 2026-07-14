"""ClickHouse 멱등 시퀀스 프리미티브 (§7.1) — CHGate.

호출자(mart.py/steps.py/batch.py — T2~T4)의 시퀀스: exists(존재 확인 — 없으면 delete 스킵,
§4.0 뮤테이션 절감) → delete_day(ALTER TABLE ... DELETE, ON CLUSTER + wait_for_mutations 내장)
→ insert_select(항상 _dist 경유, insert_distributed_sync=1 + insert_deduplicate=0 — 재삽입
중복제거 차단, Global Constraints) → verify_count(재시도 RETRY_COUNT×RETRY_INTERVAL_S).

DB명은 §9-18 협의 변경 지점 — 아래 상수 3개만 수정 (테이블명 하드코딩 금지,
f"{DB_MART}.token_usage_1d_dist" 형식으로 참조).
"""
import time
from datetime import datetime, timedelta, timezone

import clickhouse_connect

from app.config import Config

DB_FACT = "fact"        # §9-18 협의 변경 지점 (PR #6)
DB_DIM = "gpu_data"     # §9-18 협의 변경 지점 (PR #6)
DB_MART = "mart"        # §9-18 협의 변경 지점 (PR #6) — mart DB 공유/전용 잔여 협의 중

KST = timezone(timedelta(hours=9))


def now_kst() -> datetime:
    """CH DateTime('Asia/Seoul') 컬럼용 — aware KST. naive datetime을 clickhouse-connect가
    호스트 TZ로 해석하면 KST 벽시계와 어긋난다 (수집기 C2 회귀 방지 — 항상 tzinfo 유지)."""
    return datetime.now(KST)


class CHGate:
    def __init__(self, cfg: Config, client=None, clock=time.monotonic, sleeper=time.sleep):
        self.cfg = cfg
        self.client = client or clickhouse_connect.get_client(
            host=cfg.ch_host, port=cfg.ch_port, username=cfg.ch_user,
            password=cfg.ch_password)
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
            parameters={"d": date})
        return int(r.result_rows[0][0]) if r.result_rows else 0

    def exists(self, table_dist: str, date: str) -> bool:
        """존재 확인 SELECT — False면 호출자가 delete_day를 스킵한다 (§4.0 뮤테이션 절감)."""
        return self._count(table_dist, date) > 0

    def delete_day(self, table_local: str, date: str, extra_pred: str = "") -> None:
        """local 테이블에 ON CLUSTER DELETE 후 전 레플리카 뮤테이션 완료까지 대기.
        extra_pred: view 테이블 등 created_by 등 추가 조건 (§7.1) — 'AND ...' 형태로 전달."""
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
        cfg.insert_quorum이 설정된 경우만 insert_quorum 설정을 포함한다."""
        settings = {"insert_distributed_sync": 1, "insert_deduplicate": 0}
        if self.cfg.insert_quorum:
            settings["insert_quorum"] = self.cfg.insert_quorum
        result = self.client.command(sql, parameters=params, settings=settings)
        written = getattr(result, "written_rows", None)
        if written is None:
            # clickhouse-connect의 command() 반환이 QuerySummary가 아닌 구현체 대비 폴백
            fallback = self.client.query(sql, parameters=params)
            written = len(fallback.result_rows) if fallback.result_rows else 0
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
        """STEP 0 커버리지·인라인 검증용 범용 SELECT."""
        r = self.client.query(sql, parameters=params)
        return [tuple(row) for row in (r.result_rows or [])]
