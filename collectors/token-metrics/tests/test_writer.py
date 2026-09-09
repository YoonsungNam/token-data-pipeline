"""writer(§5.4 적재 시퀀스 · §4.0 뮤테이션 장부 · §4.3 레지스트리 diff-sync) 테스트 — FakeCH, 실제 CH 없음.
공통 fixture 상수는 Plan 6b 전 태스크 공통(Mock Group / Mock Service A / 2026-09-10)."""
import os
import subprocess
import sys
from datetime import date as date_t, datetime
from pathlib import Path

import pytest

from app.config import Config, ServiceEntry
from app.normalize import KST, SOURCE_API, GpuRow, MetricsPayload, NormalizeResult, ServingRow
from app.writer import (AUDIT_COLS, CLIENT_SETTINGS, DB_DIM, DB_FACT, DELETE_ORDER, DIM_COLS,
                        GPU_COLS, INSERT_ORDER, MUTATIONS_SYNC, SERVING_COLS, SUMMARY_COLS,
                        T_AUDIT, T_DIM, T_GPU, T_SERVING, T_SUMMARY, MetricsWriter,
                        MutationBudgetExceeded, now_kst)

MODULE_ROOT = Path(__file__).resolve().parent.parent
DDL_FACT = MODULE_ROOT / "ddl" / "company" / "raw_token_metrics.sql"
DDL_DIM = MODULE_ROOT / "ddl" / "company" / "dim_token_metrics_service.sql"

SERVICE_GROUP = "Mock Group"
SERVICE = "Mock Service A"
BASE_URL = "http://mock"
DATE = "2026-09-10"
DATE_V = date_t(2026, 9, 10)
GENERATED_AT = datetime(2026, 9, 11, 2, 5, tzinfo=KST)
PREV_GEN = datetime(2026, 8, 27, 9, 0, tzinfo=KST)
PREV_COL = datetime(2026, 8, 27, 9, 10, tzinfo=KST)
ALL_TABLES = (T_SUMMARY, T_GPU, T_SERVING)


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeCH:
    """존재확인 3종 · prev summary · gpu/serving 집계 · 레지스트리 SELECT를 테이블명 부분문자열로 라우팅.
    existing: {T_SUMMARY: {svc,…}, T_GPU: {…}, T_SERVING: {…}} (없는 키 = 빈 집합).
    prev_summary: (generated_at, collected_at, source_type) | None. gpu_agg: (count, sum(gpu_hours)).
    dim_rows: 레지스트리 현재 행(updated_at 제외 11컬럼 튜플) 목록."""

    def __init__(self, existing=None, prev_summary=None, gpu_agg=(0, None), serving_count=0,
                 dim_rows=None, insert_fails=False):
        existing = existing or {}
        self.existing = {t: set(existing.get(t, ())) for t in ALL_TABLES}
        self.prev_summary = prev_summary
        self.gpu_agg = gpu_agg
        self.serving_count = serving_count
        self.dim_rows = [tuple(r) for r in (dim_rows or [])]
        self.insert_fails = insert_fails    # registry_sync_failed(B) 테스트용 — INSERT 만 실패시킨다
        self.events = []        # ("command", 정규화 sql) | ("insert", table) — 호출 순서
        self.queries = []       # (정규화 sql, parameters)
        self.commands = []      # (정규화 sql, parameters, settings)
        self.inserts = []       # (table, row_count, column_names)
        self.insert_rows = []   # (table, data)

    def query(self, sql, parameters=None):
        norm = " ".join(sql.split())
        self.queries.append((norm, parameters))
        p = parameters or {}
        if "DISTINCT service" in norm:
            for t in ALL_TABLES:
                if t + "_dist" in norm:
                    hit = self.existing[t] & set(p.get("ss", ()))
                    return FakeResult([[s] for s in sorted(hit)])
            return FakeResult([])
        if T_DIM + "_dist" in norm:
            return FakeResult([list(r) for r in self.dim_rows])
        if T_SUMMARY + "_dist" in norm and "generated_at" in norm:
            return FakeResult([list(self.prev_summary)] if self.prev_summary else [])
        if T_SUMMARY + "_dist" in norm and "source_type" in norm:
            return FakeResult([[self.prev_summary[2]]] if self.prev_summary else [])
        if T_SUMMARY + "_dist" in norm and "count()" in norm:
            return FakeResult([[1 if p.get("s") in self.existing[T_SUMMARY] else 0]])
        if T_GPU + "_dist" in norm and "sum(gpu_hours)" in norm:
            return FakeResult([list(self.gpu_agg)])
        if T_SERVING + "_dist" in norm and "count()" in norm:
            return FakeResult([[self.serving_count]])
        return FakeResult([])

    def command(self, sql, parameters=None, settings=None):
        norm = " ".join(sql.split())
        self.commands.append((norm, parameters, settings))
        self.events.append(("command", norm))

    def insert(self, table, data, column_names=None):
        if self.insert_fails:
            raise RuntimeError("insert boom")
        self.inserts.append((table, len(data), tuple(column_names or ())))
        self.insert_rows.append((table, data))
        self.events.append(("insert", table))


def entry(service=SERVICE, **kw) -> ServiceEntry:
    base = dict(service_group=SERVICE_GROUP, service=service, base_url=BASE_URL, enabled=True,
                api_since=date_t(2026, 9, 9), coverage_since=date_t(2026, 8, 26), until=None)
    base.update(kw)
    return ServiceEntry(**base)


def payload(service=SERVICE) -> MetricsPayload:
    return MetricsPayload(date=DATE, reported_service_group=SERVICE_GROUP, reported_service=service,
                          generated_at_raw="2026-09-11T02:05:00+09:00",
                          engine={"type": "vllm", "version": "0.10.1"},
                          gpu=[], serving=[], source_type=SOURCE_API)


_STD = (("ttft_ms", "ms", 280.0, 560.0, 720.0, 1200.0),
        ("itl_ms", "ms", 24.0, 38.0, 47.0, 80.0),
        ("output_tps", "tokens/s", 41.0, None, None, None))   # output_tps는 p50만 (p90..p99 None)


def result(n_gpu=2, n_serving=3, n_custom=0) -> NormalizeResult:
    """T3 dataclass 직접 조립 (n_serving <= 3: ttft_ms, itl_ms, output_tps 순)."""
    gpu = [GpuRow(model=f"m{i}", gpu_type="H100", category="serving", gpu_count=2.0,
                  gpu_hours=48.0, flags=[]) for i in range(n_gpu)]
    serving = [ServingRow(model="m0", metric=m, name="", unit=u, p50=a, p90=b, p95=c, p99=d, flags=[])
               for m, u, a, b, c, d in _STD[:n_serving]]
    serving += [ServingRow(model="m0", metric="custom", name=f"c{i}", unit="ms", p50=1.0, p90=None,
                           p95=None, p99=None, flags=[]) for i in range(n_custom)]
    return NormalizeResult(generated_at=GENERATED_AT, gpu_rows=gpu, serving_rows=serving,
                           engine_type="vllm", engine_version="0.10.1")


def writer(ch: FakeCH, **cfg_kw) -> MetricsWriter:
    return MetricsWriter(Config(**cfg_kw), client=ch)


def _ddl_columns(path: Path, table: str) -> list[str]:
    """`CREATE TABLE IF NOT EXISTS <table>` 의 컬럼 목록(첫 토큰)을 선언 순서로 — `_dist` 전용(COMMENT·DEFAULT 없음)."""
    text = path.read_text(encoding="utf-8")
    body = text[text.index(f"CREATE TABLE IF NOT EXISTS {table}"):]
    body = body[body.index("(") + 1:]
    body = body[:body.index("\n)")]
    return [line.strip().split()[0] for line in body.splitlines()
            if line.strip() and not line.strip().startswith("--")]


# ---- 상수·컬럼 튜플 -----------------------------------------------------------

def test_client_settings_constant():
    assert CLIENT_SETTINGS == {"insert_distributed_sync": 1, "insert_deduplicate": 0}   # §5.4 (3)
    assert MUTATIONS_SYNC == {"mutations_sync": 2}                                       # §5.4 (2)


def test_table_names_and_orders():
    assert (T_GPU, T_SERVING, T_SUMMARY, T_AUDIT, T_DIM) == (
        "raw_token_metrics_gpu_1d", "raw_token_metrics_serving_1d", "raw_token_metrics_summary_1d",
        "collect_audit_metrics_1d", "dim_token_metrics_service")
    assert DELETE_ORDER == (T_SUMMARY, T_GPU, T_SERVING)     # 앵커 먼저 지운다
    assert INSERT_ORDER == (T_GPU, T_SERVING, T_SUMMARY)     # 앵커 마지막에 넣는다
    assert T_AUDIT not in DELETE_ORDER                       # 감사는 append-only


def test_column_tuples_match_dist_ddl():
    """INSERT는 컬럼 목록 명시(Plan 6a) — 튜플이 DDL `_dist` 선언 순서와 바이트 단위로 같아야 한다."""
    assert list(GPU_COLS) == _ddl_columns(DDL_FACT, f"fact.{T_GPU}_dist")
    assert list(SERVING_COLS) == _ddl_columns(DDL_FACT, f"fact.{T_SERVING}_dist")
    assert list(SUMMARY_COLS) == _ddl_columns(DDL_FACT, f"fact.{T_SUMMARY}_dist")
    assert list(AUDIT_COLS) == _ddl_columns(DDL_FACT, f"fact.{T_AUDIT}_dist")
    assert list(DIM_COLS) == _ddl_columns(DDL_DIM, f"gpu_data.{T_DIM}_dist")
    assert (len(GPU_COLS), len(SERVING_COLS), len(SUMMARY_COLS), len(AUDIT_COLS), len(DIM_COLS)) == (12, 15, 15, 9, 12)


def test_dim_cols_prefix_is_service_entry_key():
    assert DIM_COLS[-1] == "updated_at"
    assert len(entry().dim_key()) == len(DIM_COLS) - 1 == 11           # diff 비교 키 = updated_at 제외


def test_now_kst_is_aware():
    assert now_kst().tzinfo is not None
    assert now_kst().utcoffset().total_seconds() == 9 * 3600


# ---- 존재확인 3종 · prev summary · _delete_day_in · 가드 예외 ------------------

def test_anchor_exists_and_source_type():
    ch = FakeCH(existing={T_SUMMARY: {SERVICE}}, prev_summary=(PREV_GEN, PREV_COL, "manual-v0"))
    w = writer(ch)
    assert w.anchor_exists(DATE, SERVICE) is True
    assert w.anchor_exists(DATE, "Mock Service B") is False
    sql, params = ch.queries[0]
    assert sql == (f"SELECT count() FROM {DB_FACT}.{T_SUMMARY}_dist "
                   f"WHERE date = %(d)s AND service = %(s)s")
    assert params == {"d": DATE, "s": SERVICE}
    assert w.anchor_source_type(DATE, SERVICE) == "manual-v0"
    sql, params = ch.queries[-1]
    assert sql == (f"SELECT source_type FROM {DB_FACT}.{T_SUMMARY}_dist "
                   f"WHERE date = %(d)s AND service = %(s)s ORDER BY collected_at DESC LIMIT 1")
    assert params == {"d": DATE, "s": SERVICE}
    assert writer(FakeCH()).anchor_source_type(DATE, SERVICE) is None
    assert ch.commands == [] and ch.inserts == []                       # 읽기 전용 — 뮤테이션 0


def test_existing_services_three_tables_in_clause():
    ch = FakeCH(existing={T_SUMMARY: {"A", "C"}, T_SERVING: {"B"}, T_GPU: {"Z"}})
    got = writer(ch).existing_services(DATE, ["C", "A", "B", "A"])
    assert got == {T_SUMMARY: {"A", "C"}, T_GPU: set(), T_SERVING: {"B"}}   # Z는 요청 밖 → 제외
    assert len(ch.queries) == 3
    assert [q[0] for q in ch.queries] == [
        f"SELECT DISTINCT service FROM {DB_FACT}.{t}_dist WHERE date = %(d)s AND service IN %(ss)s"
        for t in (T_SUMMARY, T_GPU, T_SERVING)]
    assert all(q[1] == {"d": DATE, "ss": ["A", "B", "C"]} for q in ch.queries)   # 중복 제거·정렬
    assert writer(FakeCH()).existing_services(DATE, []) == {T_SUMMARY: set(), T_GPU: set(), T_SERVING: set()}


def test_fetch_prev_summary_values_and_none():
    ch = FakeCH(prev_summary=(PREV_GEN, PREV_COL, "manual-v0"), gpu_agg=(5, 120.5), serving_count=7)
    prev = writer(ch).fetch_prev_summary(DATE, SERVICE)
    assert prev == {"prev_generated_at": PREV_GEN, "prev_collected_at": PREV_COL,
                    "prev_source_type": "manual-v0", "prev_gpu_rows": 5,
                    "prev_gpu_hours_sum": 120.5, "prev_serving_rows": 7}
    assert len(ch.queries) == 3 and all(q[1] == {"d": DATE, "s": SERVICE} for q in ch.queries)
    ch2 = FakeCH(prev_summary=(PREV_GEN, PREV_COL, "metrics-api-v1"), gpu_agg=(0, None), serving_count=0)
    prev2 = writer(ch2).fetch_prev_summary(DATE, SERVICE)
    assert prev2["prev_gpu_rows"] == 0 and prev2["prev_gpu_hours_sum"] == 0.0     # NODATA 세대도 감사 대상
    assert prev2["prev_serving_rows"] == 0
    ch3 = FakeCH(prev_summary=None, gpu_agg=(3, 10.0))
    assert writer(ch3).fetch_prev_summary(DATE, SERVICE) is None                 # 앵커 없음 → 감사 없음
    assert len(ch3.queries) == 1                                                 # summary만 조회하고 중단


def test_delete_day_in_sql_settings_and_counter():
    ch = FakeCH()
    w = writer(ch, ch_cluster="gpu-monitoring")
    w._delete_day_in(w._local(T_SUMMARY), DATE, ["C", "A"])
    assert w.mutations_done == 1
    sql, params, settings = ch.commands[0]
    assert sql == (f"ALTER TABLE {DB_FACT}.{T_SUMMARY}_local ON CLUSTER 'gpu-monitoring' "
                   f"DELETE WHERE date = %(d)s AND service IN %(ss)s")
    assert params == {"d": DATE, "ss": ["A", "C"]}
    assert settings == {"mutations_sync": 2}
    w._delete_day_in(w._local(T_GPU), DATE, ["A"])
    assert w.mutations_done == 2


def test_mutation_budget_exceeded_attrs():
    e = MutationBudgetExceeded(3, 6, 8)
    assert (e.planned, e.done, e.limit) == (3, 6, 8)
    assert str(e) == "planned=3 done=6 limit=8"
    assert isinstance(e, Exception)


def test_writer_starts_with_zero_mutations():
    w = writer(FakeCH(), ch_cluster="")
    assert w.mutations_done == 0
    assert w._on_cluster() == ""
    assert w._dist(T_GPU) == f"{DB_FACT}.raw_token_metrics_gpu_1d_dist"
    assert w._local(T_GPU) == f"{DB_FACT}.raw_token_metrics_gpu_1d_local"
    assert writer(FakeCH(), ch_cluster="gpu-monitoring")._on_cluster() == " ON CLUSTER 'gpu-monitoring'"


# ---- DB명 상수 (company-verify 격리 DB) ------------------------------------------

def test_db_names_default():
    """CH_DB_FACT/CH_DB_DIM 미설정 시 기본값 — 기존 배포·E2E 무변경."""
    assert DB_FACT == "fact"
    assert DB_DIM == "gpu_data"


def test_db_names_env_override():
    """모듈 로드 시 1회 결정(CronJob env 주입 전제) — 이미 import된 프로세스에서 os.environ을 바꿔도
    재평가되지 않으므로 자식 프로세스를 띄워 import 시점 반영을 검증한다(기존 모듈 D6.2 관용구)."""
    env = {"PATH": os.environ.get("PATH", ""),
           "CH_DB_FACT": "token_verify_fact", "CH_DB_DIM": "token_verify_dim"}
    res = subprocess.run(
        [sys.executable, "-c", "from app.writer import DB_FACT, DB_DIM; print(DB_FACT); print(DB_DIM)"],
        cwd=str(MODULE_ROOT), env=env, capture_output=True, text=True, check=True)
    assert res.stdout.strip().splitlines() == ["token_verify_fact", "token_verify_dim"]


# ---- 적재 시퀀스 (§5.4) ------------------------------------------------------------

def _tables(ch: FakeCH, kind: str) -> list[str]:
    return [t for k, t in ch.events if k == kind]


def test_first_load_no_delete_no_audit():
    ch = FakeCH()                                              # 존재 3종 전부 빈 집합
    got = writer(ch).replace_batch(DATE, [(entry(), payload(), result())])
    assert got == {SERVICE: 5}                                 # gpu 2 + serving 3
    assert ch.commands == []                                   # 뮤테이션 0 (§4.0 정기 = INSERT만)
    assert [t.rsplit(".", 1)[1] for t, _, _ in ch.inserts] == [f"{T_GPU}_dist", f"{T_SERVING}_dist", f"{T_SUMMARY}_dist"]
    assert not any(t.endswith(f"{T_AUDIT}_dist") for t, _, _ in ch.inserts)
    assert [n for _, n, _ in ch.inserts] == [2, 3, 1]
    assert [c for _, _, c in ch.inserts] == [GPU_COLS, SERVING_COLS, SUMMARY_COLS]   # 컬럼 목록 명시
    assert len(ch.queries) == 3                                # 존재 SELECT 3종만


def test_insert_order_summary_last():
    ch = FakeCH()
    got = writer(ch).replace_batch(DATE, [(entry(), payload(), result(n_gpu=0, n_serving=1))])
    assert got == {SERVICE: 1}
    tables = [t for t, _, _ in ch.inserts]
    assert tables[-1].endswith(f"{T_SUMMARY}_dist")            # 앵커 마지막
    assert not any(t.endswith(f"{T_GPU}_dist") for t in tables)   # 0행 gpu INSERT 생략
    assert [t.rsplit(".", 1)[1] for t in tables] == [f"{T_SERVING}_dist", f"{T_SUMMARY}_dist"]
    ch2 = FakeCH()
    assert writer(ch2).replace_batch(DATE, [(entry(), payload(), result(n_gpu=0, n_serving=0))]) == {SERVICE: 0}
    assert [t.rsplit(".", 1)[1] for t, _, _ in ch2.inserts] == [f"{T_SUMMARY}_dist"]   # NODATA 앵커 1행


def test_reload_audit_then_delete_order_then_insert():
    ch = FakeCH(existing={T_SUMMARY: {SERVICE}, T_GPU: {SERVICE}, T_SERVING: {SERVICE}},
                prev_summary=(PREV_GEN, PREV_COL, "manual-v0"), gpu_agg=(5, 120.5), serving_count=7)
    w = writer(ch, ch_cluster="gpu-monitoring")
    got = w.replace_batch(DATE, [(entry(), payload(), result())])
    assert got == {SERVICE: 5}
    kinds = [k for k, _ in ch.events]
    assert kinds == ["insert", "command", "command", "command", "insert", "insert", "insert"]
    assert ch.events[0][1].endswith(f"{T_AUDIT}_dist")                     # 감사 먼저
    deletes = [c for c in ch.commands if "DELETE" in c[0]]
    assert len(deletes) == 3
    assert [c[0] for c in deletes] == [
        f"ALTER TABLE {DB_FACT}.{t}_local ON CLUSTER 'gpu-monitoring' "
        f"DELETE WHERE date = %(d)s AND service IN %(ss)s" for t in (T_SUMMARY, T_GPU, T_SERVING)]
    assert all(c[1] == {"d": DATE, "ss": [SERVICE]} for c in deletes)
    assert all(c[2] == {"mutations_sync": 2} for c in deletes)
    assert [t.rsplit(".", 1)[1] for t in _tables(ch, "insert")[1:]] == [f"{T_GPU}_dist", f"{T_SERVING}_dist", f"{T_SUMMARY}_dist"]
    audit_table, audit_rows = ch.insert_rows[0]
    assert audit_table == f"{DB_FACT}.{T_AUDIT}_dist" and ch.inserts[0][2] == AUDIT_COLS
    assert len(audit_rows) == 1 and len(audit_rows[0]) == 9
    assert audit_rows[0][:8] == [DATE_V, SERVICE, PREV_GEN, PREV_COL, "manual-v0", 5, 120.5, 7]
    assert audit_rows[0][8].tzinfo is not None                              # replaced_at aware KST
    assert w.mutations_done == 3


def test_children_only_forces_three_deletes_without_audit():
    """앵커 없이 gpu 행만 남은 부분 적재 잔여물(§5.2 표) — 확장 존재확인이 DELETE×3 강제, 감사는 없음."""
    ch = FakeCH(existing={T_GPU: {SERVICE}})
    w = writer(ch)
    w.replace_batch(DATE, [(entry(), payload(), result())])
    assert len([c for c in ch.commands if "DELETE" in c[0]]) == 3
    assert not any(t.endswith(f"{T_AUDIT}_dist") for t, _, _ in ch.inserts)
    assert len(ch.queries) == 3                                # prev summary 조회 없음(앵커 집합이 빔)
    assert w.mutations_done == 3


def test_no_on_cluster_when_cluster_empty():
    ch = FakeCH(existing={T_SUMMARY: {SERVICE}}, prev_summary=(PREV_GEN, PREV_COL, "metrics-api-v1"))
    writer(ch, ch_cluster="").replace_batch(DATE, [(entry(), payload(), result())])
    deletes = [c for c in ch.commands if "DELETE" in c[0]]
    assert len(deletes) == 3 and all("ON CLUSTER" not in c[0] for c in deletes)
    assert all("_local" in c[0] for c in deletes)


def test_batch_in_clause_and_single_delete_set():
    """--replace 배칭(§5.4 B): 서비스 3개, 테이블마다 다른 존재 집합 → 합집합 1개로 DELETE 3회(6·9 아님)."""
    ch = FakeCH(existing={T_SUMMARY: {"A", "C"}, T_SERVING: {"B"}},
                prev_summary=(PREV_GEN, PREV_COL, "metrics-api-v1"), gpu_agg=(1, 24.0), serving_count=1)
    items = [(entry(s), payload(s), result()) for s in ("C", "A", "B")]
    got = writer(ch).replace_batch(DATE, items)
    assert got == {"C": 5, "A": 5, "B": 5}
    deletes = [c for c in ch.commands if "DELETE" in c[0]]
    assert len(deletes) == 3
    assert all(c[1] == {"d": DATE, "ss": ["A", "B", "C"]} for c in deletes)
    audits = [(t, n) for t, n, _ in ch.inserts if t.endswith(f"{T_AUDIT}_dist")]
    assert audits == [(f"{DB_FACT}.{T_AUDIT}_dist", 1)] * 2                # 앵커 있는 A·C 만 감사
    assert [r[0][1] for t, r in ch.insert_rows if t.endswith(f"{T_AUDIT}_dist")] == ["A", "C"]   # 정렬 순
    fact = [t.rsplit(".", 1)[1] for t, _, _ in ch.inserts if not t.endswith(f"{T_AUDIT}_dist")]
    assert fact == [f"{T_GPU}_dist", f"{T_SERVING}_dist", f"{T_SUMMARY}_dist"] * 3   # 서비스별 gpu→serving→summary
    assert [r[0][2] for t, r in ch.insert_rows if t.endswith(f"{T_SUMMARY}_dist")] == ["C", "A", "B"]   # items 순서
    # 감사 → DELETE 3 → INSERT 순서 (전 서비스 DELETE 뒤에야 첫 INSERT)
    kinds = [k for k, _ in ch.events]
    assert kinds[:5] == ["insert", "insert", "command", "command", "command"] and set(kinds[5:]) == {"insert"}


def test_mutation_budget_guard_before_any_write():
    ch = FakeCH(existing={T_GPU: {SERVICE}}, prev_summary=(PREV_GEN, PREV_COL, "metrics-api-v1"))
    w = writer(ch, max_mutations_per_run=2)
    with pytest.raises(MutationBudgetExceeded) as ei:
        w.replace_batch(DATE, [(entry(), payload(), result())])
    assert (ei.value.planned, ei.value.done, ei.value.limit) == (3, 0, 2)
    assert ch.commands == [] and ch.inserts == []              # DELETE·INSERT·감사 전부 없음
    assert len(ch.queries) == 3                                # 존재 선조회는 수행됨(가드 합산 근거)
    assert w.mutations_done == 0


def test_budget_zero_allows_first_load():
    """정기 경로(존재 0) 는 예정 DELETE 0 → 예산 0 이어도 통과 (뮤테이션 장부 '정기 0')."""
    ch = FakeCH()
    assert writer(ch, max_mutations_per_run=0).replace_batch(DATE, [(entry(), payload(), result())]) == {SERVICE: 5}
    assert ch.commands == []


def test_mutations_done_accumulates_across_batches():
    ch = FakeCH(existing={T_SUMMARY: {SERVICE}}, prev_summary=(PREV_GEN, PREV_COL, "metrics-api-v1"))
    w = writer(ch, max_mutations_per_run=8)
    w.replace_batch(DATE, [(entry(), payload(), result())])
    assert w.mutations_done == 3
    w.replace_batch("2026-09-11", [(entry(), payload(), result())])
    assert w.mutations_done == 6
    with pytest.raises(MutationBudgetExceeded) as ei:
        w.replace_batch("2026-09-12", [(entry(), payload(), result())])
    assert (ei.value.planned, ei.value.done, ei.value.limit) == (3, 6, 8)
    assert w.mutations_done == 6                               # 예외 후 누적 불변
    assert len([c for c in ch.commands if "DELETE" in c[0]]) == 6


def test_row_shapes():
    ch = FakeCH()
    w = writer(ch)
    collected = datetime(2026, 9, 11, 2, 7, tzinfo=KST)
    n = w.insert_service_day(entry(), DATE, payload(), result(n_gpu=2, n_serving=3, n_custom=1), collected)
    assert n == 6
    rows = {t.rsplit(".", 1)[1]: data for t, data in ch.insert_rows}
    gpu = rows[f"{T_GPU}_dist"]
    assert len(gpu) == 2 and all(len(r) == len(GPU_COLS) == 12 for r in gpu)
    assert type(gpu[0][0]) is date_t and gpu[0][0] == DATE_V            # 드라이버 Date 직렬화 요건 (str 불가)
    assert gpu[0][1:6] == [SERVICE_GROUP, SERVICE, "m0", "H100", "serving"]
    assert gpu[0][6:8] == [2.0, 48.0]
    assert isinstance(gpu[0][8], list) and gpu[0][8] == []               # flags Array(String)
    assert gpu[0][9] == "metrics-api-v1"
    assert gpu[0][10] == GENERATED_AT and gpu[0][10].tzinfo is not None
    assert gpu[0][11] is collected
    serving = rows[f"{T_SERVING}_dist"]
    assert len(serving) == 4 and all(len(r) == len(SERVING_COLS) == 15 for r in serving)
    assert serving[0][3:7] == ["m0", "ttft_ms", "", "ms"] and serving[0][7:11] == [280.0, 560.0, 720.0, 1200.0]
    assert serving[2][4:7] == ["output_tps", "", "tokens/s"] and serving[2][7] == 41.0
    assert serving[2][8] is None and serving[2][9] is None and serving[2][10] is None   # p90..p99 None 유지
    assert serving[3][4:7] == ["custom", "c0", "ms"]
    assert all(r[12] == "metrics-api-v1" and r[13] == GENERATED_AT and r[14] is collected for r in serving)
    summary = rows[f"{T_SUMMARY}_dist"]
    assert len(summary) == 1 and len(summary[0]) == len(SUMMARY_COLS) == 15
    assert summary[0][:7] == [DATE_V, SERVICE_GROUP, SERVICE, SERVICE_GROUP, SERVICE, "vllm", "0.10.1"]
    assert summary[0][7:12] == [2, 3, 1, 0, 0]                           # gpu_rows serving_rows custom_rows rejected merged_dups
    assert summary[0][12:] == ["metrics-api-v1", GENERATED_AT, collected]


def test_summary_carries_reported_identity_and_rejected_counts():
    """summary 의 reported_* 는 payload 원문(identity drift 추적), rejected/merged_dups 는 result 값."""
    ch = FakeCH()
    p = payload()
    p.reported_service_group, p.reported_service = "Mock Group ", "mock service a"
    r = result(n_gpu=1, n_serving=1)
    r.rejected, r.merged_dups = 4, 2
    writer(ch).insert_service_day(entry(), DATE, p, r, now_kst())
    summary = [d for t, d in ch.insert_rows if t.endswith(f"{T_SUMMARY}_dist")][0][0]
    assert summary[3:5] == ["Mock Group ", "mock service a"]
    assert summary[7:12] == [1, 1, 0, 4, 2]


def test_replace_batch_empty_items_is_noop():
    ch = FakeCH(existing={T_SUMMARY: {SERVICE}})
    assert writer(ch).replace_batch(DATE, []) == {}
    assert ch.queries == [] and ch.commands == [] and ch.inserts == []


# ---- 레지스트리 diff-sync (§4.3) ----------------------------------------------------

ENTRIES = [
    entry("Mock Service A"),
    entry("Mock Service B", base_url="http://mock-b/", expect_gpu=False, until=date_t(2026, 12, 31),
          note="b"),
    entry("Mock Service C", enabled=False, usage_includes_consumers=True),
]
DIM_SELECT = (f"SELECT service_group, service, base_url, enabled, api_since, coverage_since, until, "
              f"expect_gpu, expect_serving, usage_includes_consumers, note FROM {DB_DIM}.{T_DIM}_dist")


def test_sync_registry_noop_when_equal():
    ch = FakeCH(dim_rows=[e.dim_key() for e in ENTRIES])      # 현재 행 = 원하는 집합 (updated_at 은 비교 밖)
    w = writer(ch, ch_cluster="gpu-monitoring")
    assert w.sync_registry(ENTRIES) is False
    assert ch.commands == [] and ch.inserts == []              # 쿼리 1회, 뮤테이션 0 (§4.0 장부 '정기 0')
    assert len(ch.queries) == 1 and ch.queries[0] == (DIM_SELECT, None)
    assert "updated_at" not in ch.queries[0][0]
    assert w.mutations_done == 0


def test_sync_registry_order_independent_and_none_until_safe():
    """현재 행 순서가 달라도 같은 집합이면 no-op; until 이 None/date 로 섞인 정렬이 TypeError 없이 동작."""
    rows = [ENTRIES[2].dim_key(), ENTRIES[0].dim_key(), ENTRIES[1].dim_key()]
    assert writer(FakeCH(dim_rows=rows)).sync_registry(ENTRIES) is False
    twins = [entry("Same", until=None), entry("Same", until=date_t(2026, 12, 31))]   # service 동일·until 만 다름
    ch = FakeCH(dim_rows=[twins[1].dim_key(), twins[0].dim_key()])
    assert writer(ch).sync_registry(twins) is False


def test_sync_registry_replaces_when_diff():
    changed = [ENTRIES[0], entry("Mock Service B", base_url="http://mock-b/", expect_gpu=True), ENTRIES[2]]
    ch = FakeCH(dim_rows=[e.dim_key() for e in changed])      # B 의 expect_gpu 가 DB 와 다름
    w = writer(ch, ch_cluster="gpu-monitoring")
    assert w.sync_registry(ENTRIES) is True
    assert len(ch.commands) == 1
    sql, params, settings = ch.commands[0]
    assert sql == f"ALTER TABLE {DB_DIM}.{T_DIM}_local ON CLUSTER 'gpu-monitoring' DELETE WHERE 1"
    assert params is None and settings == {"mutations_sync": 2}
    assert ch.inserts == [(f"{DB_DIM}.{T_DIM}_dist", len(ENTRIES), DIM_COLS)]
    assert [k for k, _ in ch.events] == ["command", "insert"]            # DELETE 뒤 INSERT
    data = ch.insert_rows[0][1]
    assert all(len(r) == 12 for r in data)
    assert [r[1] for r in data] == ["Mock Service A", "Mock Service B", "Mock Service C"]
    assert data[1][:11] == list(ENTRIES[1].dim_key()) and data[1][2] == "http://mock-b/"
    assert data[1][6] == date_t(2026, 12, 31) and data[0][6] is None      # until: Nullable(Date)
    assert data[2][3] == 0 and data[2][9] == 1                            # enabled / usage_includes_consumers UInt8
    assert len({r[11] for r in data}) == 1 and data[0][11].tzinfo is not None   # updated_at 동일·aware
    assert w.mutations_done == 1                                          # 레지스트리 DELETE 도 장부 합산


def test_sync_registry_skips_delete_when_current_empty():
    """최초 배포(현재 집합 빔) — DELETE 생략 → 뮤테이션 0 (§4.0 장부)."""
    ch = FakeCH(dim_rows=[])
    w = writer(ch, ch_cluster="gpu-monitoring")
    assert w.sync_registry(ENTRIES) is True
    assert ch.commands == []
    assert ch.inserts == [(f"{DB_DIM}.{T_DIM}_dist", 3, DIM_COLS)]
    assert w.mutations_done == 0


def test_sync_registry_removed_service_triggers_replace():
    ch = FakeCH(dim_rows=[e.dim_key() for e in ENTRIES])
    w = writer(ch)
    assert w.sync_registry(ENTRIES[:2]) is True                          # endpoints 에서 C 제거
    assert len(ch.commands) == 1 and "ON CLUSTER" not in ch.commands[0][0]
    assert ch.inserts[0][1] == 2
    assert w.mutations_done == 1


def test_sync_registry_budget_guard_before_delete():
    """B(1): sync_registry 도 per-date 경로와 같은 뮤테이션 가드를 DELETE 전에 적용 — DELETE·INSERT 모두 안 나간다."""
    changed = [ENTRIES[0], entry("Mock Service B", base_url="http://mock-b/", expect_gpu=True), ENTRIES[2]]
    ch = FakeCH(dim_rows=[e.dim_key() for e in changed])           # 현재 ≠ desired → DELETE 대상
    w = writer(ch, max_mutations_per_run=0)
    with pytest.raises(MutationBudgetExceeded) as ei:
        w.sync_registry(ENTRIES)
    assert (ei.value.planned, ei.value.done, ei.value.limit) == (1, 0, 0)
    assert ch.commands == [] and ch.inserts == []                  # 가드가 DELETE 전에 막는다 — 뮤테이션 0
    assert w.mutations_done == 0


def test_sync_registry_insert_failure_warns_before_reraise(capsys):
    """B(2): DELETE 뒤 INSERT 가 실패하면 다음 정규 슬롯까지 레지스트리가 빌 수 있음을 stderr WARN 으로 남기고 재던진다."""
    changed = [ENTRIES[0], entry("Mock Service B", base_url="http://mock-b/", expect_gpu=True), ENTRIES[2]]
    ch = FakeCH(dim_rows=[e.dim_key() for e in changed], insert_fails=True)
    w = writer(ch, ch_cluster="gpu-monitoring")
    with pytest.raises(RuntimeError):
        w.sync_registry(ENTRIES)
    assert len(ch.commands) == 1                                   # DELETE 는 이미 나갔다
    assert w.mutations_done == 1
    err = capsys.readouterr().err
    assert ("[WARN] registry_sync: DELETE 후 INSERT 실패 — 다음 정규 슬롯까지 "
            "dim_token_metrics_service가 비어 있을 수 있음") in err
