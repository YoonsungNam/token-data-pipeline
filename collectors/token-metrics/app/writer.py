"""ClickHouse 멱등 적재 — 설계 2026-08-31 §5.4 적재 시퀀스 · §4.0 뮤테이션 장부 · §4.3 레지스트리 diff-sync.

시퀀스(§5.4, 크래시 안전):
  (1) 존재 SELECT 3종 (summary/gpu/serving `_dist`, `WHERE date = … AND service IN (…)`) — fetch·normalize 이후, DELETE 직전
  (2) 하나라도 있으면: 앵커(summary)가 있는 서비스만 감사 INSERT(append-only)
      → DELETE 순서 고정 summary(앵커) → gpu → serving (`_local`[+ON CLUSTER], mutations_sync=2; 테이블당 1회 = 날짜당 ≤3)
  (3) INSERT 순서 gpu → serving → summary 마지막 (insert_distributed_sync=1, insert_deduplicate=0)
      — 앵커(summary) 존재 = 적재 완료. 자식 행만 남은 잔여물은 다음 실행의 (1)이 잡아 DELETE×3 을 강제한다.
뮤테이션 가드(§4.0): 예정 DELETE 수 + 실행 누적(mutations_done) > METRICS_MAX_MUTATIONS_PER_RUN 이면
  DELETE·INSERT 전에 MutationBudgetExceeded — 호출자(main)가 FAILURE reason=mutation_budget 으로 번역.
DB명은 모듈 상수 2종만 (company-verify 격리 DB는 env CH_DB_FACT/CH_DB_DIM — 모듈 로드 시 1회 결정).
기존 collectors/token-usage/app/clickhouse_client.py 의 관용구(ON CLUSTER·mutations_sync=2·aware KST)를 복제 — import 없음.
"""
from __future__ import annotations

import os
from datetime import date as date_t, datetime, timedelta, timezone

import clickhouse_connect

from app.config import Config, ServiceEntry
from app.normalize import MetricsPayload, NormalizeResult

# company-verify 격리 DB 검증용 — 모듈 로드 시 1회 결정(CronJob env 주입 전제). 미설정 = 공유 DB.
DB_FACT = os.getenv("CH_DB_FACT", "fact")
DB_DIM = os.getenv("CH_DB_DIM", "gpu_data")

KST = timezone(timedelta(hours=9))
CLIENT_SETTINGS = {"insert_distributed_sync": 1, "insert_deduplicate": 0}   # §5.4 (3)
MUTATIONS_SYNC = {"mutations_sync": 2}                                       # §5.4 (2)

T_GPU = "raw_token_metrics_gpu_1d"
T_SERVING = "raw_token_metrics_serving_1d"
T_SUMMARY = "raw_token_metrics_summary_1d"       # 앵커 — 응답당 1행, NODATA 포함
T_AUDIT = "collect_audit_metrics_1d"             # append-only (GRANT도 INSERT만)
T_DIM = "dim_token_metrics_service"              # gpu_data 레지스트리 (정기 실행 diff-sync)
DELETE_ORDER = (T_SUMMARY, T_GPU, T_SERVING)     # 앵커 먼저
INSERT_ORDER = (T_GPU, T_SERVING, T_SUMMARY)     # 앵커 마지막

# 컬럼 튜플 = Plan 6a DDL `_dist` 선언 순서 (tests/test_writer.py 가 DDL을 파싱해 대조). INSERT는 항상 column_names 명시.
GPU_COLS = ("date", "service_group", "service", "model", "gpu_type", "category",
            "gpu_count", "gpu_hours", "flags", "source_type", "generated_at", "collected_at")
SERVING_COLS = ("date", "service_group", "service", "model", "metric", "name", "unit",
                "p50", "p90", "p95", "p99", "flags", "source_type", "generated_at", "collected_at")
SUMMARY_COLS = ("date", "service_group", "service", "reported_service_group", "reported_service",
                "engine_type", "engine_version", "gpu_rows", "serving_rows", "custom_rows",
                "rejected_rows", "merged_dups", "source_type", "generated_at", "collected_at")
AUDIT_COLS = ("date", "service", "prev_generated_at", "prev_collected_at", "prev_source_type",
              "prev_gpu_rows", "prev_gpu_hours_sum", "prev_serving_rows", "replaced_at")
DIM_COLS = ("service_group", "service", "base_url", "enabled", "api_since", "coverage_since", "until",
            "expect_gpu", "expect_serving", "usage_includes_consumers", "note", "updated_at")


def now_kst() -> datetime:
    """CH DateTime('Asia/Seoul') 컬럼용 — aware KST. naive datetime을 드라이버가 int(x.timestamp())로 다루면
    호스트 TZ로 해석되어 KST 벽시계와 어긋난다 — 항상 tzinfo를 유지한 채 넘긴다(기존 모듈 C2 회귀 방지)."""
    return datetime.now(KST)


class MutationBudgetExceeded(Exception):
    """예정 DELETE 수 + 실행 누적이 METRICS_MAX_MUTATIONS_PER_RUN 을 넘음 — 적재 없이 FAILURE reason=mutation_budget."""

    def __init__(self, planned: int, done: int, limit: int):
        super().__init__(f"planned={planned} done={done} limit={limit}")
        self.planned = planned
        self.done = done
        self.limit = limit


def _dim_sort_key(row: tuple) -> tuple[str, ...]:
    """레지스트리 diff 정렬 키 — `until` 이 None/date 로 섞여도 비교 가능하도록 전 원소를 문자열화(동치 판정은 원 튜플로)."""
    return tuple("" if v is None else str(v) for v in row)


class MetricsWriter:
    def __init__(self, cfg: Config, client=None):
        self.cfg = cfg
        self.client = client or clickhouse_connect.get_client(
            host=cfg.ch_host, port=cfg.ch_port, username=cfg.ch_user,
            password=cfg.ch_password, settings=CLIENT_SETTINGS)
        self.mutations_done = 0     # 실행 누적 — fact DELETE·레지스트리 DELETE 마다 +1 (§4.0 가드 합산 기준)

    # ---- 이름 조합 -------------------------------------------------------------------

    def _on_cluster(self) -> str:
        return f" ON CLUSTER '{self.cfg.ch_cluster}'" if self.cfg.ch_cluster else ""

    def _dist(self, name: str) -> str:
        return f"{DB_FACT}.{name}_dist"

    def _local(self, name: str) -> str:
        return f"{DB_FACT}.{name}_local"

    # ---- 읽기 (뮤테이션 0) --------------------------------------------------------------

    def anchor_exists(self, date: str, service: str) -> bool:
        """사전 already_loaded 판정(§5.2 표) — 앵커(summary)만 본다."""
        r = self.client.query(
            f"SELECT count() FROM {self._dist(T_SUMMARY)} WHERE date = %(d)s AND service = %(s)s",
            parameters={"d": date, "s": service})
        return bool(r.result_rows and r.result_rows[0][0])

    def anchor_source_type(self, date: str, service: str) -> str | None:
        """앵커의 source_type — 정기 경로에서 manual-v0 앵커면 CHECK WARN manual_row_present (§5.2 표)."""
        r = self.client.query(
            f"SELECT source_type FROM {self._dist(T_SUMMARY)} "
            f"WHERE date = %(d)s AND service = %(s)s ORDER BY collected_at DESC LIMIT 1",
            parameters={"d": date, "s": service})
        return r.result_rows[0][0] if r.result_rows else None

    def existing_services(self, date: str, services: list[str]) -> dict[str, set[str]]:
        """§5.4 (1) 존재 SELECT 3종 — 테이블별로 '이 날짜에 행이 있는 서비스' 집합.
        clickhouse-connect 는 list 파라미터를 배열 리터럴로 직렬화하고 CH 는 `service IN [...]` 를 허용한다."""
        ss = sorted(set(services))
        if not ss:
            return {T_SUMMARY: set(), T_GPU: set(), T_SERVING: set()}
        out: dict[str, set[str]] = {}
        for t in (T_SUMMARY, T_GPU, T_SERVING):
            r = self.client.query(
                f"SELECT DISTINCT service FROM {self._dist(t)} "
                f"WHERE date = %(d)s AND service IN %(ss)s",
                parameters={"d": date, "ss": ss})
            out[t] = {row[0] for row in r.result_rows}
        return out

    def fetch_prev_summary(self, date: str, service: str) -> dict | None:
        """교체 전 세대 요약 — 감사(append-only)용. 앵커(summary)가 없으면 None(NODATA 세대는 앵커가 있으므로 감사 대상)."""
        s = self.client.query(
            f"SELECT generated_at, collected_at, source_type FROM {self._dist(T_SUMMARY)} "
            f"WHERE date = %(d)s AND service = %(s)s ORDER BY collected_at DESC LIMIT 1",
            parameters={"d": date, "s": service})
        if not s.result_rows:
            return None
        gen, col, stype = s.result_rows[0]
        g = self.client.query(
            f"SELECT count(), sum(gpu_hours) FROM {self._dist(T_GPU)} "
            f"WHERE date = %(d)s AND service = %(s)s",
            parameters={"d": date, "s": service})
        gpu_rows, gpu_hours = (g.result_rows[0] if g.result_rows else (0, None))
        v = self.client.query(
            f"SELECT count() FROM {self._dist(T_SERVING)} WHERE date = %(d)s AND service = %(s)s",
            parameters={"d": date, "s": service})
        serving_rows = v.result_rows[0][0] if v.result_rows else 0
        return {"prev_generated_at": gen, "prev_collected_at": col, "prev_source_type": stype,
                "prev_gpu_rows": int(gpu_rows or 0), "prev_gpu_hours_sum": float(gpu_hours or 0.0),
                "prev_serving_rows": int(serving_rows or 0)}

    # ---- 뮤테이션 (각 호출 = 1 뮤테이션) -----------------------------------------------

    def _delete_day_in(self, table_local: str, date: str, services: list[str]) -> None:
        """§5.4 배칭 (B) — 테이블당 1회 `service IN (...)` DELETE (`_local`[+ON CLUSTER], mutations_sync=2)."""
        self.client.command(
            f"ALTER TABLE {table_local}{self._on_cluster()} "
            f"DELETE WHERE date = %(d)s AND service IN %(ss)s",
            parameters={"d": date, "ss": sorted(services)},
            settings=MUTATIONS_SYNC)
        self.mutations_done += 1

    # ---- 적재 (§5.4 (3) INSERT 순서 gpu → serving → summary 마지막) ----------------------

    def insert_service_day(self, entry: ServiceEntry, date: str, payload: MetricsPayload,
                           result: NormalizeResult, collected_at: datetime) -> int:
        """서비스 1개·날짜 1개 INSERT — 0행 자식 테이블은 INSERT 생략, summary(앵커)는 항상 1행(NODATA 포함).
        manual-v0 는 호출자(T7)가 payload.reported_* 에 레지스트리 값을 넣어 온다. 반환 = result.rows."""
        date_v = date_t.fromisoformat(date)   # 네이티브 INSERT 의 Date 직렬화는 date 객체 필요
        stype = payload.source_type
        gen = result.generated_at
        gpu_rows = [[date_v, entry.service_group, entry.service, r.model, r.gpu_type, r.category,
                     r.gpu_count, r.gpu_hours, list(r.flags), stype, gen, collected_at]
                    for r in result.gpu_rows]
        if gpu_rows:
            self.client.insert(self._dist(T_GPU), gpu_rows, column_names=GPU_COLS)
        serving_rows = [[date_v, entry.service_group, entry.service, r.model, r.metric, r.name, r.unit,
                         r.p50, r.p90, r.p95, r.p99, list(r.flags), stype, gen, collected_at]
                        for r in result.serving_rows]
        if serving_rows:
            self.client.insert(self._dist(T_SERVING), serving_rows, column_names=SERVING_COLS)
        self.client.insert(
            self._dist(T_SUMMARY),
            [[date_v, entry.service_group, entry.service,
              payload.reported_service_group, payload.reported_service,
              result.engine_type, result.engine_version,
              result.n_gpu, result.n_serving, result.n_custom, result.rejected, result.merged_dups,
              stype, gen, collected_at]],
            column_names=SUMMARY_COLS)
        return result.rows

    def replace_batch(self, date: str,
                      items: list[tuple[ServiceEntry, MetricsPayload, NormalizeResult]]) -> dict[str, int]:
        """§5.4 (1)~(3) + 배칭 (B): 날짜 1개·서비스 N개.
        (1) 존재 SELECT 3종 → affected = 합집합 → (2) 가드 → 앵커 있는 서비스 감사 INSERT → DELETE summary→gpu→serving
        (테이블당 1회, 서비스 IN 배칭 = 날짜당 ≤3 뮤테이션) → (3) 서비스별 INSERT gpu→serving→summary.
        정기 경로는 item 1개로 호출(서비스별 순차), --replace/manual 은 날짜당 1회 호출. 반환 {service: rows}."""
        if not items:
            return {}
        existing = self.existing_services(date, [e.service for e, _, _ in items])
        affected = sorted(set().union(*existing.values()))
        planned = 3 if affected else 0
        limit = self.cfg.max_mutations_per_run
        if self.mutations_done + planned > limit:               # §4.0 가드 — DELETE·INSERT·감사 전
            raise MutationBudgetExceeded(planned, self.mutations_done, limit)
        date_v = date_t.fromisoformat(date)
        for svc in sorted(existing[T_SUMMARY]):                 # 앵커가 있는 서비스만 감사(append-only)
            prev = self.fetch_prev_summary(date, svc)
            if prev:
                self.client.insert(
                    self._dist(T_AUDIT),
                    [[date_v, svc, prev["prev_generated_at"], prev["prev_collected_at"],
                      prev["prev_source_type"], prev["prev_gpu_rows"], prev["prev_gpu_hours_sum"],
                      prev["prev_serving_rows"], now_kst()]],
                    column_names=AUDIT_COLS)
        if affected:                                            # 자식 행만 남은 잔여물도 3회 강제(§5.2 표)
            for t in DELETE_ORDER:
                self._delete_day_in(self._local(t), date, affected)
        collected_at = now_kst()                                # 배치 1회 — 같은 실행의 서비스는 같은 적재 시각
        out: dict[str, int] = {}
        for entry, payload, result in items:
            out[entry.service] = self.insert_service_day(entry, date, payload, result, collected_at)
        return out

    # ---- 레지스트리 diff-sync (§4.3 — 정기 실행에서만 호출; rerun·manual 은 호출하지 않는다) --------

    def sync_registry(self, entries: list[ServiceEntry]) -> bool:
        """endpoints 집합(원하는 상태) vs gpu_data.dim_token_metrics_service 현재 행 — updated_at 제외 11컬럼 비교.
        같으면 False(SELECT 1회, 뮤테이션 0). 다르면 현재 집합이 비어있지 않을 때만 ALTER DELETE(전체, 1 뮤테이션)
        → 전 행 INSERT(컬럼 명시) → True. 실패는 호출자가 CHECK WARN registry_sync_failed 로 다룬다."""
        desired = sorted((e.dim_key() for e in entries), key=_dim_sort_key)
        cols = ", ".join(DIM_COLS[:-1])                          # updated_at 제외 — 비교 키만 읽는다
        r = self.client.query(f"SELECT {cols} FROM {DB_DIM}.{T_DIM}_dist")
        current = sorted((tuple(row) for row in r.result_rows), key=_dim_sort_key)
        if desired == current:
            return False
        if current:                                              # 최초 배포(빈 테이블)는 DELETE 생략 → 뮤테이션 0
            self.client.command(
                f"ALTER TABLE {DB_DIM}.{T_DIM}_local{self._on_cluster()} DELETE WHERE 1",
                settings=MUTATIONS_SYNC)
            self.mutations_done += 1
        now = now_kst()
        self.client.insert(f"{DB_DIM}.{T_DIM}_dist",
                           [e.dim_row(now) for e in entries], column_names=DIM_COLS)
        return True
