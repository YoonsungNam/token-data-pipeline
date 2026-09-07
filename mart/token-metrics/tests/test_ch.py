import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest

from app.ch import (CHGate, DB_DIM, DB_FACT, DB_MART, DB_TOKEN_DIM, DB_TOKEN_MART, KST,
                    SESSION_SETTINGS, now_kst)
from app.config import Config

MODULE_ROOT = Path(__file__).resolve().parent.parent

DATE = "2026-09-01"


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeSummary:
    """clickhouse-connect QuerySummary 흉내 — written_rows만 필요."""

    def __init__(self, written_rows):
        self.written_rows = written_rows


class FakeCH:
    """mart/token-usage FakeCH 클론 — command/query 호출 이력을 전부 기록한다.

    - commands: [(sql, parameters, settings), ...]  (client.command 호출)
    - queries:  [(sql, parameters, settings), ...]   (client.query 호출)
    - mutations_left: system.mutations/clusterAllReplicas 폴링 응답용 카운트다운.
      None이면 항상 pending(count=1) — 타임아웃 테스트 전용. 0이면 즉시 완료(no-op 대기).
    - count_sequence: exists/verify_count의 count() 응답을 순서대로 흉내(마지막 값은 유지).
    - rows: query()의 일반 SELECT/DESCRIBE 응답(list[list]) — count()/mutations 분기보다 우선.
    - table_exists: `EXISTS TABLE …` 응답(1/0) — describe()의 선조회 (6c 델타).
    """

    def __init__(self, existing_count=0, mutations_left=0, count_sequence=None,
                 insert_written_rows=0, rows=None, table_exists=1):
        self.commands = []
        self.queries = []
        self.existing_count = existing_count
        self.mutations_left = mutations_left
        self.count_sequence = list(count_sequence) if count_sequence is not None else None
        self.insert_written_rows = insert_written_rows
        self.rows = rows
        self.table_exists = table_exists

    def command(self, sql, parameters=None, settings=None):
        self.commands.append((" ".join(sql.split()), parameters, settings))
        return FakeSummary(self.insert_written_rows)

    def query(self, sql, parameters=None, settings=None):
        sql_n = " ".join(sql.split())
        self.queries.append((sql_n, parameters, settings))
        if sql_n.startswith("EXISTS TABLE"):
            return FakeResult([[self.table_exists]])
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
    """sleeper가 clock을 전진시키는 결정론적 시계 (실 sleep 유입 차단)."""

    def __init__(self):
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_exists_skips_delete_when_absent():      # §4.0 no-op 뮤테이션 스킵
    ch = FakeCH(existing_count=0)
    g = CHGate(Config(), client=ch)
    assert g.exists("mart.agg_token_model_cost_1d_dist", DATE) is False


def test_exists_true_when_present():
    ch = FakeCH(existing_count=5)
    g = CHGate(Config(), client=ch)
    assert g.exists("mart.agg_token_model_cost_1d_dist", DATE) is True


def test_delete_day_on_cluster_and_waits():
    ch = FakeCH(existing_count=5)
    g = CHGate(Config(ch_cluster="gpu-monitoring"), client=ch)
    g.delete_day("mart.agg_token_model_cost_1d_local", DATE)
    cmd = ch.commands[0][0]
    assert "ON CLUSTER 'gpu-monitoring'" in cmd and "DELETE WHERE date =" in cmd
    assert any("clusterAllReplicas" in q for q, _, _s in ch.queries)   # 전 레플리카 폴링


def test_delete_day_no_on_cluster_when_cluster_empty():
    ch = FakeCH(existing_count=5)
    g = CHGate(Config(ch_cluster=""), client=ch)
    g.delete_day("mart.agg_token_model_cost_1d_local", DATE)
    cmd = ch.commands[0][0]
    assert "ON CLUSTER" not in cmd
    assert all("clusterAllReplicas" not in q for q, _, _s in ch.queries)
    assert any("system.mutations" in q for q, _, _s in ch.queries)


def test_delete_day_extra_pred_created_by():     # 추가 술어 형식 'AND …' (§7.1)
    ch = FakeCH(existing_count=5)
    CHGate(Config(), client=ch).delete_day(
        "mart.token_metrics_check_1d_local", DATE,
        extra_pred="AND created_by = 'token-metrics-pipeline'")
    assert "AND created_by = 'token-metrics-pipeline'" in ch.commands[0][0]
    assert ch.commands[0][1] == {"d": DATE}


def test_wait_for_mutations_timeout_raises():    # sleeper가 clock을 전진
    ch = FakeCH(mutations_left=None)              # 항상 pending — 타임아웃 강제
    fc = FakeClock()
    g = CHGate(Config(mutation_timeout_s=9, mutation_poll_s=3), client=ch,
               clock=fc.time, sleeper=fc.sleep)
    with pytest.raises(TimeoutError):
        g.wait_for_mutations("mart.agg_token_model_cost_1d_local")


def test_wait_for_mutations_returns_when_pending_reaches_zero():
    ch = FakeCH(mutations_left=2)                 # 2회 pending 후 완료
    fc = FakeClock()
    g = CHGate(Config(), client=ch, clock=fc.time, sleeper=fc.sleep)
    g.wait_for_mutations("mart.agg_token_model_cost_1d_local")
    assert fc.now == 6.0                          # 2 * mutation_poll_s(3) 만큼 전진


def test_verify_count_retries_then_passes():     # 재시도 중 카운트 도달
    ch = FakeCH(count_sequence=[3, 3, 7])
    g = CHGate(Config(retry_count=5, retry_interval_s=0), client=ch)
    ok, actual = g.verify_count("mart.agg_token_model_cost_1d_dist", DATE, expected=7)
    assert ok is True and actual == 7


def test_verify_count_actual_over_expected_passes_with_flag():  # 초과=통과(중복 징후는 호출자가 WARN)
    ch = FakeCH(existing_count=10)
    g = CHGate(Config(), client=ch)
    ok, actual = g.verify_count("mart.agg_token_model_cost_1d_dist", DATE, expected=7)
    assert ok is True and actual == 10


def test_verify_count_exhausted_fails():
    ch = FakeCH(existing_count=3)
    g = CHGate(Config(retry_count=3, retry_interval_s=0), client=ch)
    ok, actual = g.verify_count("mart.agg_token_model_cost_1d_dist", DATE, expected=7)
    assert ok is False and actual == 3


def test_insert_select_settings_contract():
    # settings **정확 일치**: insert_distributed_sync=1 AND insert_deduplicate=0(재삽입 폐기 차단)
    # AND distributed_product_mode='global'(§4.0 분산 조인 — 각 샤드 전역 조회) AND
    # join_use_nulls=0(SESSION_SETTINGS — LEFT JOIN 미스를 EXPECTED_SQL과 동일하게 키잉, I-1).
    # 그 외 키 없음.
    ch = FakeCH(insert_written_rows=42)
    g = CHGate(Config(), client=ch)
    n = g.insert_select("INSERT INTO mart.agg_token_model_cost_1d_dist SELECT ...", {"d": DATE})
    assert n == 42
    sql, params, settings = ch.commands[0]
    assert sql.startswith("INSERT INTO mart.agg_token_model_cost_1d_dist")
    assert params == {"d": DATE}
    assert settings == {"join_use_nulls": 0, "insert_distributed_sync": 1, "insert_deduplicate": 0,
                        "distributed_product_mode": "global"}


def test_insert_select_quorum_only_when_configured():
    # cfg.insert_quorum='' → settings에 insert_quorum 없음; 'auto' → insert_quorum='auto' 추가
    ch1 = FakeCH(insert_written_rows=5)
    CHGate(Config(insert_quorum=""), client=ch1).insert_select("INSERT ... SELECT ...")
    assert "insert_quorum" not in ch1.commands[0][2]

    ch2 = FakeCH(insert_written_rows=5)
    CHGate(Config(insert_quorum="auto"), client=ch2).insert_select("INSERT ... SELECT ...")
    assert ch2.commands[0][2] == {"join_use_nulls": 0, "insert_distributed_sync": 1,
                                  "insert_deduplicate": 0, "distributed_product_mode": "global",
                                  "insert_quorum": "auto"}


def test_insert_select_without_written_rows_raises():
    # 재실행 폴백 금지 — 이중 적재 위험
    class FakeCHNoWrittenRows:
        def command(self, sql, parameters=None, settings=None):
            return FakeSummary(None)               # written_rows 없는 요약 반환

    ch = FakeCHNoWrittenRows()
    g = CHGate(Config(), client=ch)
    with pytest.raises(RuntimeError, match="insert_select: written_rows 미획득"):
        g.insert_select("INSERT INTO mart.agg_token_model_cost_1d_dist SELECT ...")


def test_query_returns_rows():
    # 범용 SELECT 프리미티브 — M0 커버리지·예산 선조회·인라인 검증이 사용
    ch = FakeCH(rows=[["svc-a", 3], ["svc-b", 1]])
    g = CHGate(Config(), client=ch)
    result = g.query("SELECT service, count() FROM mart.agg_token_model_cost_1d_dist GROUP BY service")
    assert result == [("svc-a", 3), ("svc-b", 1)]


def test_session_settings_join_use_nulls_sent_on_all_client_calls():
    # B1(I-1) — join_use_nulls=0을 세션 전체에 명시: exists/verify_count/query/insert_select
    # 각각이 client에 보내는 settings에 join_use_nulls=0이 포함돼야 EXPECTED_SQL(카운트 쿼리)과
    # INSERT...SELECT가 LEFT JOIN 미스를 동일하게 키잉한다(canon()의 `= ''` 가드 전제).
    ch = FakeCH(existing_count=5, insert_written_rows=1)
    g = CHGate(Config(), client=ch)

    g.exists("mart.agg_token_model_cost_1d_dist", DATE)
    g.verify_count("mart.agg_token_model_cost_1d_dist", DATE, expected=1)
    g.query("SELECT 1")
    g.insert_select("INSERT INTO mart.agg_token_model_cost_1d_dist SELECT ...")

    assert ch.queries, "query() 호출이 기록되지 않음"
    for _, _, settings in ch.queries:
        assert settings is not None and settings.get("join_use_nulls") == 0
    assert ch.commands, "command() 호출이 기록되지 않음"
    for _, _, settings in ch.commands:
        assert settings.get("join_use_nulls") == 0


def test_get_client_receives_session_settings_when_no_client_injected(monkeypatch):
    # 클라이언트를 주입하지 않는 실배포 경로 — get_client(settings=SESSION_SETTINGS)로 세션
    # 기본값을 고정(문장별 지정과는 별개 방어선, B1(I-1)).
    captured = {}

    def fake_get_client(**kwargs):
        captured.update(kwargs)
        return FakeCH()

    monkeypatch.setattr("app.ch.clickhouse_connect.get_client", fake_get_client)
    CHGate(Config())
    assert captured["settings"] == SESSION_SETTINGS
    assert captured["settings"]["join_use_nulls"] == 0


def test_now_kst_is_aware():
    assert now_kst().tzinfo is not None
    assert now_kst().utcoffset() == timedelta(hours=9)
    assert KST.utcoffset(None) == timedelta(hours=9)


def test_db_names_default_five():
    """설계 §6.1 DB 상수 5종 — 미설정 시 기존 배포·E2E 무변경 기본값. 토큰 측 2종은
    CH_DB_TOKEN_* 미설정이면 CH_DB_MART/CH_DB_DIM(여기서는 기본값)을 따른다."""
    assert DB_FACT == "fact"
    assert DB_DIM == "gpu_data"
    assert DB_MART == "mart"
    assert DB_TOKEN_MART == "mart"
    assert DB_TOKEN_DIM == "gpu_data"


def _child_db_constants(env: dict) -> list[str]:
    """자식 프로세스에서 app.ch를 import — 상수는 모듈 로드 시 1회 결정되므로(CronJob env
    주입 전제) 이미 import된 프로세스에서 os.environ만 바꿔서는 재평가되지 않는다."""
    result = subprocess.run(
        [sys.executable, "-c",
         "from app.ch import DB_FACT, DB_DIM, DB_MART, DB_TOKEN_MART, DB_TOKEN_DIM; "
         "print('\\n'.join([DB_FACT, DB_DIM, DB_MART, DB_TOKEN_MART, DB_TOKEN_DIM]))"],
        cwd=str(MODULE_ROOT), env=env, capture_output=True, text=True, check=True)
    return result.stdout.splitlines()


def test_db_constants_five_with_token_fallback():
    """company-verify 격리(설계 §6.1·§7.5): CH_DB_TOKEN_MART/CH_DB_TOKEN_DIM 미설정 →
    CH_DB_MART/CH_DB_DIM 값을 그대로 따른다(fallback); 설정 시 그 값(운영 DB로 토큰 측
    읽기 유지)."""
    base = {"PATH": os.environ.get("PATH", ""),
            "CH_DB_FACT": "token_verify_fact", "CH_DB_DIM": "token_verify_dim",
            "CH_DB_MART": "token_verify_mart"}
    assert _child_db_constants(base) == [
        "token_verify_fact", "token_verify_dim", "token_verify_mart",
        "token_verify_mart", "token_verify_dim"]                 # fallback = CH_DB_MART/CH_DB_DIM
    assert _child_db_constants({**base, "CH_DB_TOKEN_MART": "mart",
                                "CH_DB_TOKEN_DIM": "gpu_data"}) == [
        "token_verify_fact", "token_verify_dim", "token_verify_mart", "mart", "gpu_data"]


def test_describe_returns_column_names():
    # DESCRIBE TABLE의 첫 컬럼(name)만 선언 순으로 — 프리플라이트(app.preflight)가 대조
    ch = FakeCH(rows=[["date", "Date", "", "", "", "", ""],
                      ["service", "LowCardinality(String)", "", "", "", "", ""]])
    g = CHGate(Config(), client=ch)
    assert g.describe("mart.token_usage_1d_dist") == ["date", "service"]
    assert [q for q, _, _s in ch.queries] == ["EXISTS TABLE mart.token_usage_1d_dist",
                                             "DESCRIBE TABLE mart.token_usage_1d_dist"]


def test_describe_absent_table_returns_empty_without_describe():
    # 테이블 부재 = [] (EXISTS TABLE 선조회 — 드라이버 예외 파싱 없이 부재 판정; DESCRIBE 미실행)
    ch = FakeCH(rows=[["date", "Date"]], table_exists=0)
    g = CHGate(Config(), client=ch)
    assert g.describe("mart.token_usage_1d_dist") == []
    assert [q for q, _, _s in ch.queries] == ["EXISTS TABLE mart.token_usage_1d_dist"]
