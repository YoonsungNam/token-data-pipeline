import subprocess
import sys
from pathlib import Path

import pytest

from app.ch import CHGate, DB_DIM, DB_FACT, DB_MART, now_kst
from app.config import Config

MODULE_ROOT = Path(__file__).resolve().parent.parent

DATE = "2026-07-10"


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeSummary:
    """clickhouse-connect QuerySummary 흉내 — written_rows만 필요."""

    def __init__(self, written_rows):
        self.written_rows = written_rows


class FakeCH:
    """collectors/token-usage FakeCH 스타일 확장 — command/query 호출 이력을 전부 기록한다.

    - commands: [(sql, parameters, settings), ...]  (client.command 호출)
    - queries:  [(sql, parameters), ...]             (client.query 호출)
    - mutations_left: system.mutations/clusterAllReplicas 폴링 응답용 카운트다운.
      None이면 항상 pending(count=1) — 타임아웃 테스트 전용. 0이면 즉시 완료(no-op 대기).
    - count_sequence: exists/verify_count의 count() 응답을 순서대로 흉내(마지막 값은 유지).
    - rows: query()의 일반 SELECT 응답(list[list]) — count()/mutations 분기보다 우선.
    """

    def __init__(self, existing_count=0, mutations_left=0, count_sequence=None,
                 insert_written_rows=0, rows=None):
        self.commands = []
        self.queries = []
        self.existing_count = existing_count
        self.mutations_left = mutations_left
        self.count_sequence = list(count_sequence) if count_sequence is not None else None
        self.insert_written_rows = insert_written_rows
        self.rows = rows

    def command(self, sql, parameters=None, settings=None):
        self.commands.append((" ".join(sql.split()), parameters, settings))
        return FakeSummary(self.insert_written_rows)

    def query(self, sql, parameters=None):
        sql_n = " ".join(sql.split())
        self.queries.append((sql_n, parameters))
        if "system.mutations" in sql_n:
            if self.mutations_left is None:
                return FakeResult([[1]])                      # 항상 pending
            if self.mutations_left > 0:
                self.mutations_left -= 1
                return FakeResult([[self.mutations_left + 1]])
            return FakeResult([[0]])
        if self.rows is not None:
            return FakeResult(self.rows)
        if self.count_sequence:
            val = self.count_sequence[0]
            if len(self.count_sequence) > 1:
                self.count_sequence.pop(0)
            return FakeResult([[val]])
        return FakeResult([[self.existing_count]])


class FakeClock:
    """sleeper가 clock을 전진시키는 결정론적 시계 (2a 교훈 — 실 sleep 유입 차단)."""

    def __init__(self):
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_exists_skips_delete_when_absent():      # §4.0 no-op 뮤테이션 스킵
    ch = FakeCH(existing_count=0)
    g = CHGate(Config(), client=ch)
    assert g.exists("mart.token_usage_1d_dist", DATE) is False


def test_exists_true_when_present():
    ch = FakeCH(existing_count=5)
    g = CHGate(Config(), client=ch)
    assert g.exists("mart.token_usage_1d_dist", DATE) is True


def test_delete_day_on_cluster_and_waits():
    ch = FakeCH(existing_count=5)
    g = CHGate(Config(ch_cluster="gpu-monitoring"), client=ch)
    g.delete_day("mart.token_usage_1d_local", DATE)
    cmd = ch.commands[0][0]
    assert "ON CLUSTER 'gpu-monitoring'" in cmd and "DELETE WHERE date =" in cmd
    assert any("clusterAllReplicas" in q for q, _ in ch.queries)   # 전 레플리카 폴링


def test_delete_day_no_on_cluster_when_cluster_empty():
    ch = FakeCH(existing_count=5)
    g = CHGate(Config(ch_cluster=""), client=ch)
    g.delete_day("mart.token_usage_1d_local", DATE)
    cmd = ch.commands[0][0]
    assert "ON CLUSTER" not in cmd
    assert all("clusterAllReplicas" not in q for q, _ in ch.queries)
    assert any("system.mutations" in q for q, _ in ch.queries)


def test_delete_day_extra_pred_created_by():     # view 테이블용 (§7.1)
    ch = FakeCH(existing_count=5)
    CHGate(Config(), client=ch).delete_day(
        "gpu_data.view_token_usage_1d_local", DATE,
        extra_pred="AND created_by = 'token-pipeline'")
    assert "AND created_by = 'token-pipeline'" in ch.commands[0][0]


def test_wait_for_mutations_timeout_raises():    # sleeper가 clock을 전진 (2a 교훈)
    ch = FakeCH(mutations_left=None)              # 항상 pending — 타임아웃 강제
    fc = FakeClock()
    g = CHGate(Config(mutation_timeout_s=9, mutation_poll_s=3), client=ch,
               clock=fc.time, sleeper=fc.sleep)
    with pytest.raises(TimeoutError):
        g.wait_for_mutations("mart.token_usage_1d_local")


def test_wait_for_mutations_returns_when_pending_reaches_zero():
    ch = FakeCH(mutations_left=2)                 # 2회 pending 후 완료
    fc = FakeClock()
    g = CHGate(Config(), client=ch, clock=fc.time, sleeper=fc.sleep)
    g.wait_for_mutations("mart.token_usage_1d_local")
    assert fc.now == 6.0                          # 2 * mutation_poll_s(3) 만큼 전진


def test_verify_count_retries_then_passes():     # 재시도 중 카운트 도달
    ch = FakeCH(count_sequence=[3, 3, 7])
    g = CHGate(Config(retry_count=5, retry_interval_s=0), client=ch)
    ok, actual = g.verify_count("mart.token_usage_1d_dist", DATE, expected=7)
    assert ok is True and actual == 7


def test_verify_count_actual_over_expected_passes_with_flag():  # 초과=통과(중복 징후는 호출자가 WARN)
    ch = FakeCH(existing_count=10)
    g = CHGate(Config(), client=ch)
    ok, actual = g.verify_count("mart.token_usage_1d_dist", DATE, expected=7)
    assert ok is True and actual == 10


def test_verify_count_exhausted_fails():
    ch = FakeCH(existing_count=3)
    g = CHGate(Config(retry_count=3, retry_interval_s=0), client=ch)
    ok, actual = g.verify_count("mart.token_usage_1d_dist", DATE, expected=7)
    assert ok is False and actual == 3


def test_insert_select_returns_written_rows_and_sync_setting():
    # FakeCH.command가 summary(written_rows) 흉내 + settings 단정:
    # insert_distributed_sync=1 AND insert_deduplicate=0 (재삽입 폐기 차단 — Global Constraints)
    ch = FakeCH(insert_written_rows=42)
    g = CHGate(Config(), client=ch)
    n = g.insert_select("INSERT INTO mart.token_usage_1d_dist SELECT ...", {"d": DATE})
    assert n == 42
    _sql, _params, settings = ch.commands[0]
    assert settings["insert_distributed_sync"] == 1
    assert settings["insert_deduplicate"] == 0
    assert settings["distributed_product_mode"] == "global"   # §4.0 분산 조인 — 각 샤드 전역 조회
    assert "insert_quorum" not in settings        # 기본 미설정


def test_insert_select_quorum_only_when_configured():
    # cfg.insert_quorum='' → settings에 insert_quorum 없음; 'auto' → insert_quorum='auto'
    ch1 = FakeCH(insert_written_rows=5)
    CHGate(Config(insert_quorum=""), client=ch1).insert_select("INSERT ... SELECT ...")
    assert "insert_quorum" not in ch1.commands[0][2]

    ch2 = FakeCH(insert_written_rows=5)
    CHGate(Config(insert_quorum="auto"), client=ch2).insert_select("INSERT ... SELECT ...")
    assert ch2.commands[0][2]["insert_quorum"] == "auto"


def test_insert_select_without_written_rows_raises():
    # 재실행 폴백 금지 — 이중 적재 위험 (T1 리뷰)
    class FakeCHNoWrittenRows:
        def command(self, sql, parameters=None, settings=None):
            # written_rows 없는 요약 반환
            return FakeSummary(None)

    ch = FakeCHNoWrittenRows()
    g = CHGate(Config(), client=ch)
    with pytest.raises(RuntimeError, match="insert_select: written_rows 미획득"):
        g.insert_select("INSERT INTO mart.token_usage_1d_dist SELECT ...")


def test_query_returns_rows():
    # 범용 SELECT 프리미티브 — STEP 0 커버리지·인라인 검증이 사용
    ch = FakeCH(rows=[["svc-a", 3], ["svc-b", 1]])
    g = CHGate(Config(), client=ch)
    result = g.query("SELECT service, count() FROM mart.token_usage_1d_dist GROUP BY service")
    assert result == [("svc-a", 3), ("svc-b", 1)]


def test_now_kst_is_aware():
    assert now_kst().tzinfo is not None


def test_db_names_default():
    """company 2단계 검증(CH_DB_FACT/CH_DB_DIM/CH_DB_MART) — 미설정 시 기존 배포·E2E
    무변경 기본값."""
    assert DB_FACT == "fact"
    assert DB_DIM == "gpu_data"
    assert DB_MART == "mart"


def test_db_names_env_override():
    """CH_DB_FACT/CH_DB_DIM/CH_DB_MART는 모듈 로드 시점에 1회 결정된다(CronJob env 주입
    전제) — 이미 import된 프로세스 내에서 os.environ만 바꿔서는 재평가되지 않으므로,
    자식 프로세스를 새로 띄워 import 시점에 env가 반영되는지 검증한다. steps.py의 SQL
    상수는 app.ch import 시점에 f-string으로 DB명이 이미 보간되므로, 치환된 DB명이
    SQL 문자열에 실제로 박히는지까지 함께 확인한다."""
    env = {"PATH": subprocess.os.environ.get("PATH", ""),
           "CH_DB_FACT": "token_verify_fact", "CH_DB_DIM": "token_verify_gpu_data",
           "CH_DB_MART": "token_verify_mart"}
    result = subprocess.run(
        [sys.executable, "-c",
         "from app.ch import DB_FACT, DB_DIM, DB_MART; "
         "print(DB_FACT); print(DB_DIM); print(DB_MART)\n"
         "from app import steps; print(steps.SQL_DETAIL[:200])"],
        cwd=str(MODULE_ROOT), env=env, capture_output=True, text=True, check=True)
    lines = result.stdout.splitlines()
    assert lines[0] == "token_verify_fact"
    assert lines[1] == "token_verify_gpu_data"
    assert lines[2] == "token_verify_mart"
    sql_snippet = "\n".join(lines[3:])
    assert "token_verify_mart.token_usage_1d_dist" in sql_snippet
