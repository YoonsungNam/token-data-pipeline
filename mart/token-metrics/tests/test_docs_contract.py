"""Plan 6c T11 문서 계약 테스트 — 대시보드 JSON·README·배포 문서가 코드(app/, tools/, DDL)와 일치하는지 검사.

ClickHouse 없이 파일만 읽는다. 레포 루트 = mart/token-metrics/tests/ 의 세 단계 위.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
DASH = REPO / "docs" / "monitoring" / "grafana_dashboard_token_metrics.json"
MON_README = REPO / "docs" / "monitoring" / "README.md"
DEPLOY_DOC = REPO / "docs" / "operations" / "token-metrics-deploy.md"
MOD_README = REPO / "mart" / "token-metrics" / "README.md"
DDL = REPO / "mart" / "token-metrics" / "ddl" / "company" / "mart_metrics_tables.sql"
INSTALL_SH = REPO / "mart" / "token-metrics" / "install.sh"
RERUN_PY = REPO / "mart" / "token-metrics" / "tools" / "rerun.py"
RUN_INV = REPO / "tools" / "verify" / "run_invariants.py"
CONFIG_PY = REPO / "mart" / "token-metrics" / "app" / "config.py"
CH_PY = REPO / "mart" / "token-metrics" / "app" / "ch.py"

TIME_MACRO = "date BETWEEN toDate($__fromTime) AND toDate($__toTime)"
DS_CH = {"type": "grafana-clickhouse-datasource", "uid": "${DS_CLICKHOUSE}"}
DS_GRAFANA = {"type": "datasource", "uid": "grafana"}

# 대시보드가 읽어도 되는 테이블 — mart 계정 GRANT(Plan 6a mart accounts.sql) 안의 _dist 만
ALLOWED_FROM = {
    "mart.agg_token_model_cost_1d_dist",
    "mart.token_metrics_check_1d_dist",
    "mart.agg_token_model_share_1d_dist",
    "mart.agg_token_gpu_group_1d_dist",
    "fact.raw_token_metrics_summary_1d_dist",
    "fact.raw_token_metrics_serving_1d_dist",   # 설계 §6.2 성능(TTFT/ITL) — service×model 단위, source_type 병기
    "gpu_data.dim_token_metrics_service_dist",
}

# (id, title, type, 주 FROM, rawSql·DDL 양쪽에 있어야 하는 컬럼) — 설계 §6.2 내용 전부(아웃라인 11패널 + 설계 §6.2가 P0 산출물로 적은 5패널)
PANEL_SPEC = [
    (1, "1. 모델별 일별 model_cost_krw (serving/standby 분해)", "timeseries", "mart.agg_token_model_cost_1d_dist",
     ["model", "model_cost_krw", "serving_gpu_hours", "standby_gpu_hours"]),
    (2, "2. 서비스별 총비용 (측정, 배부 미적용)", "timeseries", "mart.agg_token_model_cost_1d_dist",
     ["service", "model_cost_krw"]),
    (3, "3. 서비스×모델 GPU 시간·비용 (당일)", "table", "mart.agg_token_model_cost_1d_dist",
     ["service", "model", "serving_gpu_hours", "standby_gpu_hours", "test_gpu_hours",
      "flagged_gpu_hours", "model_cost_krw", "tokens_per_gpu_hour", "quality_flag"]),
    (4, "4. 서비스별 tokens_per_gpu_hour 추이", "timeseries", "mart.agg_token_model_cost_1d_dist",
     ["service", "total_tokens", "serving_gpu_hours"]),
    (5, "5. 토큰 단가 p (파생 — 기준월·가동률 병기)", "table", "mart.agg_token_model_cost_1d_dist",
     ["service_group", "model", "model_cost_krw", "weighted_tokens"]),
    (6, "6. quality_flag 분포", "table", "mart.agg_token_model_cost_1d_dist",
     ["quality_flag"]),
    (7, "7. 검사 결과 (FAIL/WARN)", "table", "mart.token_metrics_check_1d_dist",
     ["date", "service", "check_name", "severity", "model", "gpu_type", "observed", "threshold",
      "detail", "source_type"]),
    (8, "8. 일별 FAIL/WARN 건수", "timeseries", "mart.token_metrics_check_1d_dist",
     ["severity"]),
    (9, "9. 모델 비용 배분 (share)", "table", "mart.agg_token_model_share_1d_dist",
     ["model", "service", "provider_service", "denominator_mode", "share", "allocated_cost_krw",
      "quality_flag"]),
    (10, "10. 서비스별 배분 총비용 (M4 합산, stretch)", "table", "mart.agg_token_model_share_1d_dist",
     ["service", "denominator_mode", "allocated_cost_krw"]),
    (11, "11. 그룹 GPU 정체성 (I2)", "table", "mart.agg_token_gpu_group_1d_dist",
     ["service_group", "gpu_type", "allocated_gpu_hours", "reported_gpu_hours_total",
      "model_cost_sum_krw", "test_cost_krw", "idle_gpu_hours", "idle_cost_krw", "unattributed_cost_krw",
      "utilization", "identity_gap_krw", "over_report", "quality_flag"]),
    (12, "12. 그룹 utilization 추이", "timeseries", "mart.agg_token_gpu_group_1d_dist",
     ["service_group", "gpu_type", "utilization"]),
    (13, "13. TTFT/ITL 추이 (p50/p95)", "timeseries", "fact.raw_token_metrics_serving_1d_dist",
     ["service", "model", "metric", "p50", "p95", "source_type"]),
    (14, "14. 출처 (manual-v0 vs API)", "timeseries", "fact.raw_token_metrics_summary_1d_dist",
     ["service", "source_type"]),
    (15, "15. 일별 메트릭 커버리지", "table", "fact.raw_token_metrics_summary_1d_dist",
     ["service", "rejected_rows"]),
]
GRIDPOS = {
    1: (0, 0, 12, 8), 2: (12, 0, 12, 8), 3: (0, 8, 12, 8), 4: (12, 8, 12, 8), 5: (0, 16, 12, 8),
    6: (12, 16, 12, 8), 7: (0, 24, 16, 8), 8: (16, 24, 8, 8), 9: (0, 32, 12, 8), 10: (12, 32, 12, 8),
    11: (0, 40, 12, 8), 12: (12, 40, 12, 8), 13: (0, 48, 12, 8), 14: (12, 48, 6, 8), 15: (18, 48, 6, 8),
    16: (0, 56, 24, 4),
}
# 템플릿 변수 사용 패널 — service_group: 커버리지(15) 제외 전부; service: p 파생(5, 모델 단위 C÷W 라 서비스 필터 무의미)·M2(11·12, service 컬럼 없음)·커버리지(15) 제외
GROUP_FILTER_PANELS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}
SERVICE_FILTER_PANELS = {1, 2, 3, 4, 6, 7, 8, 9, 10, 13, 14}
# 모듈 README 환경변수 표 — app/config.py 또는 app/ch.py 에 문자열로 존재해야 한다 (EXPECTED_LATE_SERVICES 없음)
ENV_VARS = [
    "CH_HOST", "CH_PORT", "CH_USER", "CH_PASSWORD", "CH_CLUSTER",
    "RETRY_COUNT", "RETRY_INTERVAL_S", "MUTATION_POLL_S", "MUTATION_TIMEOUT_S", "INSERT_QUORUM",
    "MART_METRICS_MAX_MUTATIONS_PER_RUN",
]
DB_ENV_VARS = ["CH_DB_FACT", "CH_DB_DIM", "CH_DB_MART", "CH_DB_TOKEN_MART", "CH_DB_TOKEN_DIM"]
MARKER_FIELDS = ["status=", "module=mart-metrics", "metrics_coverage=", "missing_services=",
                 "rows_mart=", "rows_check=", "rows_share=", "warn=", "elapsed="]


def load_dash() -> dict:
    return json.loads(DASH.read_text(encoding="utf-8"))


def data_panels(d: dict) -> list[dict]:
    return [p for p in d["panels"] if p["type"] != "text"]


def from_tables(sql: str) -> set[str]:
    """rawSql 안의 FROM/JOIN 뒤 `db.table` 식별자 집합 (서브쿼리 포함)."""
    return set(re.findall(r"\b(?:FROM|JOIN)\s+([a-z_]+\.[a-z_0-9]+)", sql))


def ddl_columns(table_local: str) -> set[str]:
    """mart_metrics_tables.sql 에서 `CREATE TABLE … <table_local>` 블록의 컬럼명 집합."""
    text = DDL.read_text(encoding="utf-8")
    m = re.search(
        r"CREATE TABLE IF NOT EXISTS " + re.escape(table_local) + r"\s*\n.*?\n\((.*?)\n\)\s*\nENGINE",
        text, re.S,
    )
    assert m is not None, f"DDL block not found: {table_local}"
    cols = set()
    for line in m.group(1).splitlines():
        mm = re.match(r"^\s{4}([a-z_0-9]+)\s+[A-Z]", line)
        if mm:
            cols.add(mm.group(1))
    assert cols, f"no columns parsed for {table_local}"
    return cols


def argparse_flags(path: Path) -> set[str]:
    """스크립트 원문에서 `--flag`/`-n` 형태 옵션 정의 문자열을 모은다 (bash·python 공통)."""
    text = path.read_text(encoding="utf-8")
    return set(re.findall(r"(?<![\w-])(--?[a-z][a-z0-9-]*)\b", text))


def cli_flags_in_doc(text: str, script: str) -> set[str]:
    """문서의 펜스 코드 블록 안에서 `script` 를 호출하는 줄에 쓰인 옵션 플래그 집합 (산문 줄은 제외)."""
    flags = set()
    in_fence = False
    for line in text.splitlines():
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence and script in line and not line.lstrip().startswith("#"):
            flags.update(re.findall(r"(?<![\w-])(--?[a-z][a-z0-9-]*)\b", line.split(script, 1)[1]))
    return flags


# ---------------------------------------------------------------- 대시보드 JSON

def test_dashboard_identity():
    d = load_dash()
    assert d["uid"] == "token-metrics-stage"
    assert d["title"] == "Token Metrics — Stage Tester"
    assert d["tags"] == ["token-metrics", "stage"]
    assert d["schemaVersion"] == 41
    assert d["timezone"] == "Asia/Seoul"
    assert d["time"] == {"from": "now-30d", "to": "now"}
    assert d["__inputs"][0]["name"] == "DS_CLICKHOUSE"
    assert d["__inputs"][0]["pluginId"] == "grafana-clickhouse-datasource"
    assert len(d["panels"]) == 16
    assert [p["id"] for p in d["panels"]] == list(range(1, 17))
    # 직렬화 규약: 기존 token_usage JSON 과 동일 (indent=2, ensure_ascii=False, 개행 종료)
    raw = DASH.read_text(encoding="utf-8")
    assert raw == json.dumps(d, indent=2, ensure_ascii=False) + "\n"


def test_panel_ids_titles_types_from():
    by_id = {p["id"]: p for p in load_dash()["panels"]}
    for pid, title, ptype, main_from, _cols in PANEL_SPEC:
        p = by_id[pid]
        assert p["title"] == title, (pid, p["title"])
        assert p["type"] == ptype, (pid, p["type"])
        assert p["pluginVersion"] == "11.6.0"
        assert main_from in from_tables(p["targets"][0]["rawSql"]), (pid, main_from)
    assert by_id[16]["type"] == "text"
    assert by_id[16]["title"] == "참고: BATCH_RESULT 마커 패널"


def test_panel_columns_exist_in_rawsql_and_ddl():
    by_id = {p["id"]: p for p in load_dash()["panels"]}
    ddl_local = {
        "mart.agg_token_model_cost_1d_dist": "mart.agg_token_model_cost_1d_local",
        "mart.token_metrics_check_1d_dist": "mart.token_metrics_check_1d_local",
        "mart.agg_token_model_share_1d_dist": "mart.agg_token_model_share_1d_local",
        "mart.agg_token_gpu_group_1d_dist": "mart.agg_token_gpu_group_1d_local",
    }
    for pid, _title, _ptype, main_from, cols in PANEL_SPEC:
        sql = by_id[pid]["targets"][0]["rawSql"]
        for c in cols:
            assert re.search(r"\b" + re.escape(c) + r"\b", sql), (pid, c)
        if main_from in ddl_local:
            ddl_cols = ddl_columns(ddl_local[main_from])
            missing = [c for c in cols if c not in ddl_cols]
            assert not missing, (pid, main_from, missing)


def test_time_macro_and_datasource():
    d = load_dash()
    for p in data_panels(d):
        assert p["datasource"] == DS_CH, p["id"]
        t = p["targets"][0]
        assert t["editorType"] == "sql" and t["queryType"] == "sql" and t["format"] == 1
        assert t["pluginVersion"] == "4.19.0" and t["refId"] == "A"
        assert t["datasource"] == DS_CH
        assert TIME_MACRO in t["rawSql"], p["id"]
        assert "$__timeFilter" not in t["rawSql"], p["id"]
    text = d["panels"][15]
    assert text["datasource"] == DS_GRAFANA
    assert text["options"]["mode"] == "markdown"


def test_from_tables_are_dist_and_allowed():
    for p in data_panels(load_dash()):
        tables = from_tables(p["targets"][0]["rawSql"])
        assert tables, p["id"]
        for t in tables:
            assert t.endswith("_dist"), (p["id"], t)
            assert t in ALLOWED_FROM, (p["id"], t)


def test_no_user_identifiers():
    raw = DASH.read_text(encoding="utf-8")
    assert "user_id" not in raw
    assert "user_name" not in raw
    assert "user_email" not in raw


def test_gridpos_fixed_and_non_overlapping():
    panels = load_dash()["panels"]
    rects = []
    for p in panels:
        g = p["gridPos"]
        assert (g["x"], g["y"], g["w"], g["h"]) == GRIDPOS[p["id"]], p["id"]
        assert g["x"] + g["w"] <= 24
        rects.append((p["id"], g["x"], g["y"], g["x"] + g["w"], g["y"] + g["h"]))
    for i, (ia, ax1, ay1, ax2, ay2) in enumerate(rects):
        for ib, bx1, by1, bx2, by2 in rects[i + 1:]:
            overlap = ax1 < bx2 and bx1 < ax2 and ay1 < by2 and by1 < ay2
            assert not overlap, (ia, ib)


def test_templating_and_requires():
    d = load_dash()
    names = [v["name"] for v in d["templating"]["list"]]
    assert names == ["service_group", "service"]
    for v in d["templating"]["list"]:
        assert v["type"] == "query" and v["multi"] is True and v["includeAll"] is True
        assert v["datasource"] == DS_CH
        assert "mart.agg_token_model_cost_1d_dist" in v["query"]
    assert "${service_group:singlequote}" in d["templating"]["list"][1]["query"]
    req = {(r["type"], r["id"]): r["version"] for r in d["__requires"]}
    assert req[("grafana", "grafana")] == "11.6.0"
    assert req[("datasource", "grafana-clickhouse-datasource")] == "4.19.0"
    assert {("panel", "timeseries"), ("panel", "table"), ("panel", "text")} <= set(req)
    # 변수 사용 패널: GROUP_FILTER_PANELS 는 service_group 필터, SERVICE_FILTER_PANELS 는 service 필터 — 나머지는 그 변수를 쓰지 않는다
    by_id = {p["id"]: p for p in d["panels"]}
    for pid, _t, _ty, _f, _c in PANEL_SPEC:
        sql = by_id[pid]["targets"][0]["rawSql"]
        assert ("${service_group:singlequote}" in sql) == (pid in GROUP_FILTER_PANELS), pid
        assert ("${service:singlequote}" in sql) == (pid in SERVICE_FILTER_PANELS), pid


def test_text_panel_marker_note():
    content = load_dash()["panels"][15]["options"]["content"]
    for f in MARKER_FIELDS:
        assert f in content, f
    assert "측정" in content and "배분" in content and "추정" in content


def test_design_required_panels():
    """설계 §6.2 가 grafana_dashboard_token_metrics.json 내용으로 명시한 항목이 실제 rawSql 에 있는지 (정의서 §7 라벨 포함)."""
    by_id = {p["id"]: p for p in load_dash()["panels"]}
    sql = {pid: by_id[pid]["targets"][0]["rawSql"] for pid, *_ in PANEL_SPEC}
    # 1) 모델별 C 의 serving+standby 분해 — C × serving/(serving+standby) 비례 분해
    assert "AS serving_cost_krw" in sql[1] and "AS standby_cost_krw" in sql[1]
    assert "nullIf(serving_gpu_hours + standby_gpu_hours, 0)" in sql[1]
    # 2) 서비스별 총비용 P0-core = Σ M1 model_cost_krw by service, '배부 미적용' 라벨
    assert "'측정 (배부 미적용)' AS cost_label" in sql[2]
    assert "GROUP BY time, service" in sql[2] and "sum(model_cost_krw)" in sql[2]
    # 5) 토큰 단가 p = Σ C / Σ W (정의서 3.7) — 기준월(toStartOfMonth)·가동률(M2 Σ reported / Σ allocated) 병기, 라벨 '파생'
    assert "toStartOfMonth(date) AS base_month" in sql[5]
    assert "AS p_krw_per_m_wtoken" in sql[5] and "AS utilization_pct" in sql[5]
    assert "mart.agg_token_gpu_group_1d_dist" in from_tables(sql[5])
    assert "'파생' AS cost_label" in sql[5]
    # 10) stretch = M4 합산 by service — 라벨은 denominator_mode 에서 파생(배분/추정/그룹 귀속)
    assert "multiIf(denominator_mode = 'external_api', '추정'" in sql[10]
    assert "sum(allocated_cost_krw)" in sql[10] and "GROUP BY date, service, cost_label" in sql[10]
    # 11) 그룹 행 = ΣC + 실험 + 유휴 + 미귀속 — 네 항 모두 표시
    for col in ("model_cost_sum_krw", "test_cost_krw", "idle_cost_krw", "unattributed_cost_krw"):
        assert col in sql[11], col
    # 13) TTFT/ITL — 표준 지표 2종만, source_type 병기
    assert "metric IN ('ttft_ms', 'itl_ms')" in sql[13] and "source_type" in sql[13]
    # 14) 출처 — source_type 별 서비스 수
    assert "GROUP BY time, source_type" in sql[14]
    # 15) 커버리지 분모 = 마커 metrics_coverage 분모와 같은 술어(T5 M0: enabled=1 AND coverage_since <= d AND (until IS NULL OR d <= until))
    assert "r.enabled = 1 AND r.coverage_since <= d.date AND (isNull(r.until) OR d.date <= r.until)" in sql[15]
    assert "AS expected_services" in sql[15] and "registered_services" not in sql[15]
    # 비용 라벨 컬럼이 있는 패널은 정의서 §7 의 네 라벨(+ 파생) 밖의 값을 쓰지 않는다
    for pid in (2, 3, 5, 9, 10, 11):
        assert "AS cost_label" in sql[pid], pid


# ---------------------------------------------------------------- README / 배포 문서

def test_monitoring_readme_section_7():
    text = MON_README.read_text(encoding="utf-8")
    heads = [ln for ln in text.splitlines() if ln.startswith("## ")]
    assert len(heads) == 7, heads
    assert heads[6] == "## 7. token-metrics 대시보드"
    sec7 = text.split("## 7. token-metrics 대시보드", 1)[1]
    assert "docs/monitoring/grafana_dashboard_token_metrics.json" in sec7
    assert "token-metrics-stage" in sec7
    assert TIME_MACRO in sec7
    # 16 패널 표: `| 1 |` … `| 16 |` 행
    for n in range(1, 17):
        assert re.search(r"^\| " + str(n) + r" \|", sec7, re.M), n
    assert "len(d['panels'])==16" in sec7
    # 기존 1~6절은 손대지 않는다 (git diff 로도 확인 — Step 6)
    assert heads[0].startswith("## 1. 전제") and heads[5].startswith("## 6. JSON 검증")


def test_deploy_doc_sections_and_placeholders():
    text = DEPLOY_DOC.read_text(encoding="utf-8")
    expected = [
        "## 0. 전제", "## 1. 기준정보 dim 4종", "## 2. collectors-metrics(6b)",
        "## 3. mart-metrics install.sh", "## 4. 첫 배치·마커", "## 5. invariants_metrics",
        "## 6. 대시보드", "## 7. 재실행(rerun --chunk-days 7)", "## 8. company-verify 격리(선택)",
        "## 9. 트러블슈팅",
    ]
    heads = [ln for ln in text.splitlines() if ln.startswith("## ")]
    assert heads == expected, heads
    # 공개 레포: 사내 호스트 0 — harbor 는 플레이스홀더만, chi 서비스 주소는 <cluster>.<ns> 형태만
    for host in re.findall(r"harbor\.[A-Za-z0-9.\-]+", text):
        assert host == "harbor.example.internal", host
    for svc in re.findall(r"chi-[A-Za-z0-9<>.\-]+\.svc", text):
        assert svc == "chi-<cluster>.<ns>.svc", svc
    assert not re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text), "email in public doc"
    # 핵심 계약 문자열
    for needle in [
        "BATCH_RESULT status=SUCCESS module=mart-metrics",
        "PREFLIGHT FAIL read_contract missing=",
        "ALL INVARIANTS PASS",
        "RERUN REFUSED window (>=10:50 KST)",
        "token-mart-metrics-ch-secret-verify",
        "MART_METRICS_MAX_MUTATIONS_PER_RUN=64",
        "reason=read_contract", "reason=mutation_budget", "token_mart_absent", "metrics_missing", "no_tco",
        "stage_seed_dim_token_",
        "manual_load.py",
    ]:
        assert needle in text, needle


def test_deploy_doc_cli_flags_exist():
    text = DEPLOY_DOC.read_text(encoding="utf-8")
    for script, path in [
        ("mart/token-metrics/install.sh", INSTALL_SH),
        ("mart/token-metrics/tools/rerun.py", RERUN_PY),
        ("tools/verify/run_invariants.py", RUN_INV),
    ]:
        used = cli_flags_in_doc(text, script)
        assert used, script
        defined = argparse_flags(path)
        missing = sorted(used - defined)
        assert not missing, (script, missing)


def test_module_readme_env_and_marker():
    text = MOD_README.read_text(encoding="utf-8")
    code = CONFIG_PY.read_text(encoding="utf-8") + CH_PY.read_text(encoding="utf-8")
    rows = [ln for ln in text.splitlines() if re.match(r"^\| `[A-Z_]+` \|", ln)]
    names = [re.match(r"^\| `([A-Z_]+)` \|", ln).group(1) for ln in rows]
    assert names == ENV_VARS + DB_ENV_VARS, names
    for n in names:
        assert f'"{n}"' in code, n
    assert "| `EXPECTED_LATE_SERVICES` |" not in text
    assert "EXPECTED_LATE_SERVICES" in text  # "없음" 을 명시하는 문장
    for f in MARKER_FIELDS:
        assert f in text, f
    for code_name in ["metrics_coverage missing=", "service_not_in_usage_registry", "token_mart_absent",
                      "dup_suspect:"]:
        assert code_name in text, code_name
    assert "M0 → M0b → M1 → M3 → M4 → M2" in text
    assert "docs/cost-model-spec.md" in text
    assert "docs/operations/token-metrics-deploy.md" in text
    assert "bash tests/e2e/run_e2e.sh" in text
