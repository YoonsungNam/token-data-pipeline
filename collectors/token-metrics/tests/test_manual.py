"""manual-v0 CSV 파서 (설계 §5.5 · Plan 6a F) — 템플릿 3파일을 fixture로 그대로 사용."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest

from app.config import ServiceEntry
from app.manual import (
    ENGINE_HEADER, GPU_HEADER, SERVING_HEADER, ManualCsvError, _num, date_range, load_manual_csvs,
    read_csv_rows,
)
from app.normalize import KST, normalize_payload

TEMPLATES = Path(__file__).resolve().parents[3] / "docs" / "templates"
T_GPU = str(TEMPLATES / "token_metrics_manual_v0_gpu.csv")
T_SERVING = str(TEMPLATES / "token_metrics_manual_v0_serving.csv")
T_ENGINE = str(TEMPLATES / "token_metrics_manual_v0_engine.csv")
TDATE = "2026-08-26"                       # 템플릿 예시 행의 날짜

ENTRY_A = ServiceEntry(service_group="Mock Group", service="Mock Service A", base_url="http://mock",
                       enabled=True, api_since=date(2026, 9, 9), coverage_since=date(2026, 8, 26), until=None)
ENTRY_B = ServiceEntry(service_group="Mock Group", service="Mock Service B", base_url="http://mock-b",
                       enabled=True, api_since=date(2026, 9, 9), coverage_since=date(2026, 8, 26), until=None)
ENTRIES = [ENTRY_A, ENTRY_B]


def write(tmp_path: Path, name: str, text: str, encoding: str = "utf-8") -> str:
    p = tmp_path / name
    p.write_text(text, encoding=encoding)
    return str(p)


# ---- read_csv_rows ------------------------------------------------------------------

def test_templates_headers_and_row_counts():
    assert [r for _, r in read_csv_rows(T_GPU, GPU_HEADER)] and len(read_csv_rows(T_GPU, GPU_HEADER)) == 4
    assert len(read_csv_rows(T_SERVING, SERVING_HEADER)) == 5
    assert len(read_csv_rows(T_ENGINE, ENGINE_HEADER)) == 2
    lineno, first = read_csv_rows(T_GPU, GPU_HEADER)[0]
    assert lineno == 11                                   # 주석 9줄 + 헤더 1줄 → 첫 데이터 행은 물리 11행
    assert first == {"date": TDATE, "service": "Mock Service A", "model": "claude-sonnet-5",
                     "gpuType": "H100", "category": "serving", "gpuCount": "4", "gpuHours": "96.0"}


def test_comment_and_blank_lines_skipped(tmp_path):
    path = write(tmp_path, "gpu.csv",
                 "# 주석, 안의, 쉼표는, 무시\n"
                 "\n"
                 "   # 좌측 공백 뒤 주석도 무시\n"
                 f"{GPU_HEADER}\n"
                 "\n"
                 f"{TDATE},Mock Service A,claude-sonnet-5,H100,serving,4,96.0\n"
                 "# 끝 주석\n")
    rows = read_csv_rows(path, GPU_HEADER)
    assert [ln for ln, _ in rows] == [6]
    assert rows[0][1]["gpuCount"] == "4"


def test_header_mismatch_error_reports_real_line(tmp_path):
    path = write(tmp_path, "gpu.csv", "# c1\n# c2\ndate,service,model\n")
    with pytest.raises(ManualCsvError) as ei:
        read_csv_rows(path, GPU_HEADER)
    msg = str(ei.value)
    assert msg.endswith(":3: header mismatch") and ":1:" not in msg


def test_header_missing_error(tmp_path):
    path = write(tmp_path, "gpu.csv", "# only comments\n\n")
    with pytest.raises(ManualCsvError, match=r":0: header missing"):
        read_csv_rows(path, GPU_HEADER)


def test_bom_tolerated(tmp_path):
    path = write(tmp_path, "gpu.csv", f"{GPU_HEADER}\n{TDATE},Mock Service A,m,H100,serving,1,2\n",
                 encoding="utf-8-sig")
    assert read_csv_rows(path, GPU_HEADER)[0][1]["date"] == TDATE


def test_crlf_and_cell_strip(tmp_path):
    path = write(tmp_path, "gpu.csv", f"{GPU_HEADER}\r\n{TDATE}, Mock Service A ,m,H100,serving,1,2\r\n")
    assert read_csv_rows(path, GPU_HEADER)[0][1]["service"] == "Mock Service A"


def test_column_count_error(tmp_path):
    path = write(tmp_path, "gpu.csv", f"{GPU_HEADER}\n{TDATE},Mock Service A,m,H100,serving,1\n")
    with pytest.raises(ManualCsvError, match=r":2: column count"):
        read_csv_rows(path, GPU_HEADER)


def test_quoted_cell_with_comma(tmp_path):
    path = write(tmp_path, "gpu.csv", f"{GPU_HEADER}\n{TDATE},\"Mock Service A\",\"m,v2\",H100,serving,1,2\n")
    assert read_csv_rows(path, GPU_HEADER)[0][1]["model"] == "m,v2"


# ---- _num / date_range --------------------------------------------------------------

def test_num_conversion():
    assert _num("") is None
    assert _num("4") == 4.0 and isinstance(_num("4"), float)
    assert _num("96.0") == 96.0
    assert _num("abc") == "abc"                           # 비숫자는 원문 유지 → normalize 가 거부


def test_date_range_inclusive_and_errors():
    assert date_range("2026-08-26", "2026-08-28") == ["2026-08-26", "2026-08-27", "2026-08-28"]
    assert date_range("2026-08-26", "2026-08-26") == ["2026-08-26"]
    with pytest.raises(ValueError, match="--from must not be after --to"):
        date_range("2026-08-27", "2026-08-26")
    with pytest.raises(ValueError):
        date_range("2026-13-01", "2026-13-02")


# ---- load_manual_csvs ---------------------------------------------------------------

def gpu_csv(tmp_path: Path, *rows: str) -> str:
    return write(tmp_path, "gpu.csv", "\n".join(("# gpu", GPU_HEADER) + rows) + "\n")


def serving_csv(tmp_path: Path, *rows: str) -> str:
    return write(tmp_path, "serving.csv", "\n".join(("# serving", SERVING_HEADER) + rows) + "\n")


def engine_csv(tmp_path: Path, *rows: str) -> str:
    return write(tmp_path, "engine.csv", "\n".join(("# engine", ENGINE_HEADER) + rows) + "\n")


def load(gpu: str, serving: str, engine: str | None = None, *, frm: str = TDATE, to: str = TDATE,
         entries=ENTRIES, only: str | None = None, gen: str = ""):
    return load_manual_csvs(gpu, serving, engine, frm, to, entries, only, gen)


def test_templates_parse_as_is():
    payloads, counts = load(T_GPU, T_SERVING, T_ENGINE)
    assert set(payloads) == {(TDATE, "Mock Service A"), (TDATE, "Mock Service B")}
    a = payloads[(TDATE, "Mock Service A")]
    assert a.source_type == "manual-v0" and a.generated_at_raw == ""
    assert (a.reported_service_group, a.reported_service) == ("Mock Group", "Mock Service A")
    assert a.gpu == [
        {"model": "claude-sonnet-5", "gpuType": "H100", "category": "serving", "gpuCount": 4.0, "gpuHours": 96.0},
        {"model": "claude-sonnet-5", "gpuType": "H100", "category": "standby", "gpuCount": 1.0, "gpuHours": 24.0},
    ]
    assert len(a.serving) == 1 and set(a.serving[0]) == {"model", "ttftMs", "itlMs", "outputTps"}
    assert a.serving[0]["ttftMs"] == {"p50": 280.0, "p90": 560.0, "p95": 720.0, "p99": 1200.0}
    assert a.serving[0]["outputTps"] == {"p50": 41.0}
    assert a.engine == {"type": "vllm", "version": "0.8.4"}
    b = payloads[(TDATE, "Mock Service B")]
    assert [r["model"] for r in b.gpu] == ["claude-haiku-4-5", "unknown"]
    assert len(b.serving) == 1 and set(b.serving[0]) == {"model", "e2eMs", "custom"}
    assert set(b.serving[0]["e2eMs"]) == {"p50", "p90", "p95", "p99"}
    assert b.serving[0]["custom"] == [{"name": "queueWaitMs", "unit": "ms", "p50": 120.0, "p99": 900.0}]
    assert b.engine == {"type": "custom", "version": ""}
    assert counts == {"rows_gpu": 4, "rows_serving": 5, "rows_engine": 2,
                      "rows_outside_range": 0, "rows_other_service": 0}


def test_templates_then_normalize_clean():
    payloads, _ = load(T_GPU, T_SERVING, T_ENGINE)
    ra = normalize_payload(payloads[(TDATE, "Mock Service A")], ENTRY_A)
    assert (ra.rows, ra.rejected, ra.warns) == (5, 0, {})               # gpu 2 + ttft/itl/outputTps 3
    assert (ra.engine_type, ra.engine_version) == ("vllm", "0.8.4")
    rb = normalize_payload(payloads[(TDATE, "Mock Service B")], ENTRY_B)
    assert (rb.rows, rb.rejected, rb.warns) == (4, 0, {})               # gpu 2 + e2e 1 + custom 1
    assert (rb.engine_type, rb.engine_version) == ("custom", "")


def test_unknown_service_error_has_no_row_content(tmp_path):
    g = gpu_csv(tmp_path, f"{TDATE},secret-svc-x,secret-model-xyz,H100,serving,1,2")
    s = serving_csv(tmp_path)
    with pytest.raises(ManualCsvError) as ei:
        load(g, s)
    msg = str(ei.value)
    assert msg.endswith(":3: unknown service (not in endpoints)")
    assert "secret-svc-x" not in msg and "secret-model-xyz" not in msg


def test_validation_applies_to_ignored_rows(tmp_path):
    g = gpu_csv(tmp_path, "2026-01-01,secret-svc-x,m,H100,serving,1,2")     # 범위 밖이어도 미등록은 오류
    with pytest.raises(ManualCsvError, match="unknown service"):
        load(g, serving_csv(tmp_path))
    s = serving_csv(tmp_path, f"{TDATE},Mock Service B,m,ttft_ms,,,1,2,3,4")  # --service A 필터 밖이어도 metric 오류
    with pytest.raises(ManualCsvError, match="bad metric"):
        load(gpu_csv(tmp_path), s, only="Mock Service A")


def test_bad_date_error(tmp_path):
    for bad in ("2026/08/26", "26-08-26", "2026-8-26", "20260826"):
        g = gpu_csv(tmp_path, f"{bad},Mock Service A,m,H100,serving,1,2")
        with pytest.raises(ManualCsvError, match=r":3: bad date"):
            load(g, serving_csv(tmp_path))


def test_bad_metric_error(tmp_path):
    s = serving_csv(tmp_path, f"{TDATE},Mock Service A,m,ttft_ms,,,1,2,3,4")
    with pytest.raises(ManualCsvError, match=r":3: bad metric"):
        load(gpu_csv(tmp_path), s)


def test_duplicate_model_metric_error(tmp_path):
    s = serving_csv(tmp_path,
                    f"{TDATE},Mock Service A,m,ttftMs,,,1,2,3,4",
                    f"{TDATE},Mock Service A,m,ttftMs,,,5,6,7,8")
    with pytest.raises(ManualCsvError, match=r":4: duplicate \(model, metric\)"):
        load(gpu_csv(tmp_path), s)
    # 같은 model 의 서로 다른 지표는 한 레코드로 합쳐진다(long form → API 레코드)
    s2 = serving_csv(tmp_path,
                     f"{TDATE},Mock Service A,m,ttftMs,,,1,2,3,4",
                     f"{TDATE},Mock Service A,m,outputTps,,,9,,,")
    payloads, _ = load(gpu_csv(tmp_path), s2)
    rec = payloads[(TDATE, "Mock Service A")].serving
    assert len(rec) == 1 and set(rec[0]) == {"model", "ttftMs", "outputTps"}
    # custom name 중복은 파서 오류가 아니다 — normalize 가 dup_custom_kept_first 로 처리
    s3 = serving_csv(tmp_path,
                     f"{TDATE},Mock Service A,m,custom,q,ms,1,,,",
                     f"{TDATE},Mock Service A,m,custom,q,ms,2,,,")
    payloads, _ = load(gpu_csv(tmp_path), s3)
    assert len(payloads[(TDATE, "Mock Service A")].serving[0]["custom"]) == 2


def test_engine_errors_and_optional(tmp_path):
    e_dup = engine_csv(tmp_path, "Mock Service A,vllm,0.8.4", "Mock Service A,sglang,")
    with pytest.raises(ManualCsvError, match=r":4: duplicate service"):
        load(gpu_csv(tmp_path), serving_csv(tmp_path), e_dup, only="Mock Service A")
    e_unknown = engine_csv(tmp_path, "secret-svc-x,vllm,")
    with pytest.raises(ManualCsvError, match=r":3: unknown service"):
        load(gpu_csv(tmp_path), serving_csv(tmp_path), e_unknown, only="Mock Service A")
    # 컨트롤러 판정: payloads[(TDATE, "Mock Service A")]를 인덱싱하는 아래 두 load() 는 Mock Service A gpu 행을
    # 주어야 그 (date, service) 키에 페이로드가 생긴다("채택 행 ≥ 1건" 규칙은 그대로 — 이 두 곳만 gpu 행 추가).
    g = gpu_csv(tmp_path, f"{TDATE},Mock Service A,m,H100,serving,1,2")
    payloads, counts = load(g, serving_csv(tmp_path), None, only="Mock Service A")
    assert payloads[(TDATE, "Mock Service A")].engine is None and counts["rows_engine"] == 0
    e_ok = engine_csv(tmp_path, "Mock Service B,custom,")          # --service A 필터와 무관하게 파일 전체를 센다
    payloads, counts = load(g, serving_csv(tmp_path), e_ok, only="Mock Service A")
    assert payloads[(TDATE, "Mock Service A")].engine is None and counts["rows_engine"] == 1


def test_outside_range_and_other_service_counted(tmp_path):
    g = gpu_csv(tmp_path,
                f"{TDATE},Mock Service A,m,H100,serving,1,2",
                "2026-08-27,Mock Service A,m,H100,serving,1,2",          # 범위 밖
                f"{TDATE},Mock Service B,m,H100,serving,1,2")             # 다른 서비스
    payloads, counts = load(g, serving_csv(tmp_path), only="Mock Service A")
    assert set(payloads) == {(TDATE, "Mock Service A")}
    assert len(payloads[(TDATE, "Mock Service A")].gpu) == 1
    assert counts == {"rows_gpu": 1, "rows_serving": 0, "rows_engine": 0,
                      "rows_outside_range": 1, "rows_other_service": 1}


def test_empty_service_day_yields_no_payload(tmp_path):
    g = gpu_csv(tmp_path, f"{TDATE},Mock Service A,m,H100,serving,1,2")
    payloads, counts = load(g, serving_csv(tmp_path), to="2026-08-27")
    assert set(payloads) == {(TDATE, "Mock Service A")}                  # 08-27 은 행 없음 → 키 없음(앵커 없음)
    assert ("2026-08-27", "Mock Service A") not in payloads
    assert counts["rows_gpu"] == 1 and counts["rows_outside_range"] == 0  # 범위 안(08-26..08-27)이므로 outside 아님
    assert normalize_payload(payloads[(TDATE, "Mock Service A")], ENTRY_A).is_nodata is False


def test_only_service_without_rows_yields_no_payload(tmp_path):
    g = gpu_csv(tmp_path, f"{TDATE},Mock Service B,m,H100,serving,1,2")
    payloads, counts = load(g, serving_csv(tmp_path), only="Mock Service A")
    assert payloads == {} and counts["rows_other_service"] == 1           # 행 0건 → 페이로드 0개(NODATA 앵커 아님)
    with pytest.raises(ValueError, match="unknown service: nope"):
        load(g, serving_csv(tmp_path), only="nope")


def test_service_only_in_serving_gets_payload(tmp_path):
    s = serving_csv(tmp_path, f"{TDATE},Mock Service B,m,ttftMs,,,1,2,3,4",
                    f"{TDATE},Mock Service B,m,itlMs,,,1,2,3,4",
                    f"{TDATE},Mock Service B,m,outputTps,,,9,,,")
    payloads, _ = load(gpu_csv(tmp_path), s)
    assert set(payloads) == {(TDATE, "Mock Service B")}
    r = normalize_payload(payloads[(TDATE, "Mock Service B")], ENTRY_B)
    assert r.rows == 3 and r.is_nodata is False                          # 케이스 E: gpu:[] + serving 행 → SUCCESS


def test_bad_number_kept_for_normalize(tmp_path):
    g = gpu_csv(tmp_path, f"{TDATE},Mock Service A,m,H100,serving,abc,2")
    payloads, _ = load(g, serving_csv(tmp_path))
    assert payloads[(TDATE, "Mock Service A")].gpu[0]["gpuCount"] == "abc"
    r = normalize_payload(payloads[(TDATE, "Mock Service A")], ENTRY_A)
    assert (r.rows, r.rejected) == (0, 1)


def test_blank_pct_omitted_and_blank_custom_name_kept(tmp_path):
    s = serving_csv(tmp_path,
                    f"{TDATE},Mock Service A,m,ttftMs,,,1,2,3,",           # p99 부재 → 키 없음 → normalize 거부
                    f"{TDATE},Mock Service A,m2,custom,,ms,1,,,")          # name 빈값 → "" → normalize 거부
    payloads, _ = load(gpu_csv(tmp_path), s)
    recs = payloads[(TDATE, "Mock Service A")].serving
    assert recs[0]["ttftMs"] == {"p50": 1.0, "p90": 2.0, "p95": 3.0}
    assert recs[1]["custom"] == [{"name": "", "unit": "ms", "p50": 1.0}]
    r = normalize_payload(payloads[(TDATE, "Mock Service A")], ENTRY_A)
    assert (r.rows, r.rejected) == (0, 2)


def test_generated_at_passthrough(tmp_path):
    g = gpu_csv(tmp_path, f"{TDATE},Mock Service A,m,H100,serving,1,2")
    payloads, _ = load(g, serving_csv(tmp_path), gen="2026-08-27T09:00:00+09:00")
    p = payloads[(TDATE, "Mock Service A")]
    assert p.generated_at_raw == "2026-08-27T09:00:00+09:00"
    r = normalize_payload(p, ENTRY_A)
    assert r.generated_at == datetime(2026, 8, 27, 9, 0, tzinfo=KST) and r.warns == {}
    payloads, _ = load(g, serving_csv(tmp_path), gen="")
    assert payloads[(TDATE, "Mock Service A")].generated_at_raw == ""
