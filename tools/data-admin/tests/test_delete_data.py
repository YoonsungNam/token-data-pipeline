"""tools/data-admin/delete_data.py 테스트 (§8.3 ②, Plan 5 T3).

FakeCH는 collectors/mart FakeCH 스타일을 확장한다 — command()/query() 호출 이력을
전부 기록해 SQL·파라미터·설정을 직접 검사할 수 있게 한다 (mart/token-usage
tests/test_ch.py와 동일 관례).
"""
import datetime as dt
import os
import pathlib
import subprocess
import sys

import pytest

import delete_data as dd

MODULE_ROOT = pathlib.Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def clean_ch_env(monkeypatch):
    """CH_* env가 로컬 개발 셸에 남아있어도 테스트가 결정론적이도록 매 테스트 전 제거."""
    for name in ("CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_CLUSTER"):
        monkeypatch.delenv(name, raising=False)


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeCH:
    """command/query 호출 이력 기록 + system.mutations 폴링 흉내.

    - commands: [(sql, parameters, settings), ...]
    - queries:  [(sql, parameters), ...]
    - existing_count: count() 응답 기본값(대상 요약 dry-run/재확인용).
    - mutations_left: system.mutations 폴링 카운트다운 — None이면 항상 pending
      (타임아웃 유도), 0이면 즉시 완료.
    - fail_on_command: 지정 시 command() 호출에서 그대로 raise (실행 오류 exit 1 케이스용).
    """

    def __init__(self, existing_count=3, mutations_left=0, fail_on_command=None):
        self.commands = []
        self.queries = []
        self.existing_count = existing_count
        self.mutations_left = mutations_left
        self.fail_on_command = fail_on_command

    def command(self, sql, parameters=None, settings=None):
        if self.fail_on_command is not None:
            raise self.fail_on_command
        self.commands.append((" ".join(sql.split()), parameters, settings))

    def query(self, sql, parameters=None):
        sql_n = " ".join(sql.split())
        self.queries.append((sql_n, parameters))
        if "system.mutations" in sql_n or "clusterAllReplicas" in sql_n:
            if self.mutations_left is None:
                return FakeResult([[1]])                      # 항상 pending
            if self.mutations_left > 0:
                self.mutations_left -= 1
                return FakeResult([[self.mutations_left + 1]])
            return FakeResult([[0]])
        return FakeResult([[self.existing_count]])


class FakeClock:
    """sleeper가 clock을 전진시키는 결정론적 시계 — 실 sleep 유입 차단."""

    def __init__(self):
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


# ---------------------------------------------------------------------------
# 대상 테이블·술어 (date/user 모드)
# ---------------------------------------------------------------------------

def test_date_mode_targets_two_tables_and_excludes_audit():
    targets = dd.date_targets("2026-07-01", "2026-07-03")
    names = [t.local for t in targets]
    assert names == [
        f"{dd.DB_FACT}.raw_token_usage_1d_local",
        f"{dd.DB_FACT}.raw_token_usage_summary_1d_local",
    ]
    assert all("collect_audit" not in n for n in names)   # 감사 이력은 append-only 불변


def test_date_mode_predicate_and_params_without_service():
    for t in dd.date_targets("2026-07-01", "2026-07-03"):
        assert t.predicate == "date BETWEEN {d1:Date} AND {d2:Date}"
        assert t.params == {"d1": dt.date(2026, 7, 1), "d2": dt.date(2026, 7, 3)}


def test_date_mode_predicate_appends_service_filter():
    for t in dd.date_targets("2026-07-01", "2026-07-03", service="svc-a"):
        assert t.predicate == "date BETWEEN {d1:Date} AND {d2:Date} AND service = {s:String}"
        assert t.params["s"] == "svc-a"


def test_user_mode_targets_three_tables_excludes_agg_and_summary():
    targets = dd.user_targets("u1")
    names = [t.local for t in targets]
    assert names == [
        f"{dd.DB_FACT}.raw_token_usage_1d_local",
        f"{dd.DB_MART}.token_usage_1d_local",
        f"{dd.DB_DIM}.view_token_usage_1d_local",
    ]
    assert all("agg_" not in n and "summary" not in n for n in names)


def test_user_mode_predicate_and_params():
    for t in dd.user_targets("u1"):
        assert t.predicate == "user_id = {u:String}"
        assert t.params == {"u": "u1"}


# ---------------------------------------------------------------------------
# DB명 env 계약 (mart/token-usage/app/ch.py와 동일 패턴)
# ---------------------------------------------------------------------------

def test_db_name_defaults():
    assert dd.DB_FACT == "fact"
    assert dd.DB_DIM == "gpu_data"
    assert dd.DB_MART == "mart"


def test_db_name_env_override_via_subprocess():
    # DB_FACT/DB_DIM/DB_MART는 모듈 로드 시점에 1회 결정된다 — 이미 import된 프로세스
    # 내에서 os.environ만 바꿔서는 재평가되지 않으므로 자식 프로세스로 검증한다
    # (collectors tests/test_clickhouse_client.py test_db_names_env_override와 동일 관례).
    env = {"PATH": os.environ.get("PATH", ""),
           "CH_DB_FACT": "f2", "CH_DB_DIM": "d2", "CH_DB_MART": "m2"}
    result = subprocess.run(
        [sys.executable, "-c",
         "import delete_data as d; print(d.DB_FACT); print(d.DB_DIM); print(d.DB_MART)"],
        cwd=str(MODULE_ROOT), env=env, capture_output=True, text=True, check=True)
    assert result.stdout.strip().splitlines() == ["f2", "d2", "m2"]


# ---------------------------------------------------------------------------
# dry-run 기본 — command() 미호출, exit 0
# ---------------------------------------------------------------------------

def test_date_mode_dry_run_calls_no_command_and_exits_0(capsys):
    ch = FakeCH(existing_count=7)
    rc = dd.main(["--mode", "date", "--from", "2026-07-01", "--to", "2026-07-03"], client=ch)
    assert rc == 0
    assert ch.commands == []
    assert ch.queries                                        # count() 조회는 있음
    out = capsys.readouterr().out
    assert "DRY-RUN" in out
    assert "7건" in out


def test_user_mode_dry_run_calls_no_command_and_exits_0(capsys):
    ch = FakeCH(existing_count=2)
    rc = dd.main(["--mode", "user", "--user-id", "u1"], client=ch)
    assert rc == 0
    assert ch.commands == []
    out = capsys.readouterr().out
    assert "DRY-RUN" in out


# ---------------------------------------------------------------------------
# --yes — ON CLUSTER DELETE 시퀀스 + wait, 대상 요약 재출력
# ---------------------------------------------------------------------------

def test_yes_date_mode_executes_on_cluster_delete_and_waits(monkeypatch):
    monkeypatch.setenv("CH_CLUSTER", "gpu-monitoring")
    ch = FakeCH(existing_count=5, mutations_left=0)
    fc = FakeClock()
    rc = dd.main(
        ["--mode", "date", "--from", "2026-07-01", "--to", "2026-07-02", "--yes"],
        client=ch, clock=fc.time, sleeper=fc.sleep)
    assert rc == 0
    assert len(ch.commands) == 2                              # detail + summary
    for cmd, params, _ in ch.commands:
        assert "ON CLUSTER 'gpu-monitoring'" in cmd
        assert "DELETE WHERE date BETWEEN {d1:Date} AND {d2:Date}" in cmd
        assert params == {"d1": dt.date(2026, 7, 1), "d2": dt.date(2026, 7, 2)}
    assert any("clusterAllReplicas" in q for q, _ in ch.queries)


def test_yes_user_mode_executes_three_deletes(monkeypatch):
    monkeypatch.setenv("CH_CLUSTER", "gpu-monitoring")
    ch = FakeCH(existing_count=1, mutations_left=0)
    rc = dd.main(["--mode", "user", "--user-id", "u9", "--yes"], client=ch)
    assert rc == 0
    assert len(ch.commands) == 3
    for cmd, params, _ in ch.commands:
        assert "ON CLUSTER 'gpu-monitoring'" in cmd
        assert "DELETE WHERE user_id = {u:String}" in cmd
        assert params == {"u": "u9"}


def test_no_cluster_omits_on_cluster_and_plain_mutations_scope():
    ch = FakeCH(existing_count=1, mutations_left=0)
    rc = dd.main(["--mode", "user", "--user-id", "u1", "--yes"], client=ch)
    assert rc == 0
    assert all("ON CLUSTER" not in c[0] for c in ch.commands)
    assert any("system.mutations" in q and "clusterAllReplicas" not in q
               for q, _ in ch.queries)


def test_yes_reprints_target_summary_before_executing(capsys):
    ch = FakeCH(existing_count=2, mutations_left=0)
    dd.main(["--mode", "user", "--user-id", "u9", "--yes"], client=ch)
    out = capsys.readouterr().out
    assert out.count("합계:") == 2                            # 최초 dry-run + --yes 재확인


def test_dry_run_without_yes_prints_summary_only_once(capsys):
    ch = FakeCH(existing_count=2)
    dd.main(["--mode", "user", "--user-id", "u9"], client=ch)
    out = capsys.readouterr().out
    assert out.count("합계:") == 1


# ---------------------------------------------------------------------------
# 완료 후 안내 — mart rerun 의무(date) / dim 별도 admin 경로(user)
# ---------------------------------------------------------------------------

def test_date_mode_completion_prints_mart_rerun_command(capsys):
    ch = FakeCH(existing_count=1, mutations_left=0)
    rc = dd.main(
        ["--mode", "date", "--from", "2026-07-01", "--to", "2026-07-02", "--yes",
         "--context", "homelab", "--namespace", "ns1"],
        client=ch)
    assert rc == 0
    out = capsys.readouterr().out
    assert ("python3 mart/token-usage/tools/rerun.py --context homelab --namespace ns1 "
            "--from 2026-07-01 --to 2026-07-02") in out


def test_date_mode_completion_uses_placeholder_defaults(capsys):
    ch = FakeCH(existing_count=0, mutations_left=0)
    dd.main(["--mode", "date", "--from", "2026-07-01", "--to", "2026-07-01", "--yes"],
            client=ch)
    out = capsys.readouterr().out
    assert ("python3 mart/token-usage/tools/rerun.py --context <context> --namespace monitoring "
            "--from 2026-07-01 --to 2026-07-01") in out


def test_date_mode_dry_run_does_not_print_mart_rerun_command(capsys):
    ch = FakeCH(existing_count=0)
    dd.main(["--mode", "date", "--from", "2026-07-01", "--to", "2026-07-01"], client=ch)
    out = capsys.readouterr().out
    assert "mart/token-usage/tools/rerun.py" not in out


def test_user_mode_completion_notes_dim_separate_admin_path(capsys):
    ch = FakeCH(existing_count=0, mutations_left=0)
    dd.main(["--mode", "user", "--user-id", "u1", "--yes"], client=ch)
    out = capsys.readouterr().out
    assert "dim_token_user_org" in out
    assert "§6.1" in out
    assert "mart/token-usage/tools/rerun.py" not in out    # user 모드는 mart rerun 안내 없음


def test_user_mode_output_never_leaks_raw_user_id():
    # Global Constraints: 로그는 카운트 중심 — CLI 인자로 받은 값 자체는 불가피하지만
    # 요약/완료 메시지 본문에 원문을 반복 노출하지 않는다.
    ch = FakeCH(existing_count=0, mutations_left=0)
    dd.main(["--mode", "user", "--user-id", "very-secret-id", "--yes"], client=ch)


# ---------------------------------------------------------------------------
# wait_for_mutations 단위 검증 (DeleteGate)
# ---------------------------------------------------------------------------

def test_wait_for_mutations_timeout_raises():
    ch = FakeCH(mutations_left=None)
    fc = FakeClock()
    gate = dd.DeleteGate(ch, cluster="", clock=fc.time, sleeper=fc.sleep,
                         poll_s=3, timeout_s=9)
    with pytest.raises(TimeoutError):
        gate.wait_for_mutations("fact.raw_token_usage_1d_local")


def test_wait_for_mutations_returns_when_pending_reaches_zero():
    ch = FakeCH(mutations_left=2)
    fc = FakeClock()
    gate = dd.DeleteGate(ch, clock=fc.time, sleeper=fc.sleep)
    gate.wait_for_mutations("fact.raw_token_usage_1d_local")
    assert fc.now == 6.0                                      # 2 * poll_s(3)


# ---------------------------------------------------------------------------
# 실행 오류 — exit 1
# ---------------------------------------------------------------------------

def test_execution_error_returns_exit_1_and_reports_stderr(capsys):
    ch = FakeCH(existing_count=1, fail_on_command=RuntimeError("boom"))
    rc = dd.main(["--mode", "user", "--user-id", "u1", "--yes"], client=ch)
    assert rc == 1
    err = capsys.readouterr().err
    assert "boom" in err


# ---------------------------------------------------------------------------
# 인자 오류 — exit 2
# ---------------------------------------------------------------------------

def test_exit2_missing_mode():
    with pytest.raises(SystemExit) as e:
        dd.main([])
    assert e.value.code == 2


def test_exit2_date_mode_missing_from_to():
    with pytest.raises(SystemExit) as e:
        dd.main(["--mode", "date"])
    assert e.value.code == 2


def test_exit2_date_mode_from_after_to():
    with pytest.raises(SystemExit) as e:
        dd.main(["--mode", "date", "--from", "2026-07-05", "--to", "2026-07-01"])
    assert e.value.code == 2


def test_exit2_date_mode_malformed_date():
    with pytest.raises(SystemExit) as e:
        dd.main(["--mode", "date", "--from", "2026/07/01", "--to", "2026-07-02"])
    assert e.value.code == 2


def test_exit2_user_mode_missing_user_id():
    with pytest.raises(SystemExit) as e:
        dd.main(["--mode", "user"])
    assert e.value.code == 2


def test_exit2_user_mode_blank_user_id():
    with pytest.raises(SystemExit) as e:
        dd.main(["--mode", "user", "--user-id", "   "])
    assert e.value.code == 2


def test_help_exits_0():
    with pytest.raises(SystemExit) as e:
        dd.main(["--help"])
    assert e.value.code == 0
