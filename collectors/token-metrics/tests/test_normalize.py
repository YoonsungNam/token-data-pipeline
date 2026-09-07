"""normalize 3계층(§5.3) 테스트 — 순수 함수만, DB/HTTP 없음. 공통 fixture 상수는 Plan 6b 전 태스크 공통."""
from datetime import date, datetime

import pytest

from app.config import ServiceEntry
from app.normalize import (KST, SOURCE_API, W_GEN_OFFSET, W_GEN_PARSE, MetricsPayload,
                           PayloadError, check_report_structure, parse_engine,
                           parse_generated_at)

SERVICE_GROUP = "Mock Group"
SERVICE = "Mock Service A"
BASE_URL = "http://mock"
DATE = "2026-09-10"
GPU_TYPE = "H100"
ENGINE = {"type": "vllm", "version": "0.10.1"}
GENERATED_AT = "2026-09-11T02:05:00+09:00"
NOW = datetime(2026, 9, 11, 3, 0, tzinfo=KST)

ENTRY = ServiceEntry(service_group=SERVICE_GROUP, service=SERVICE, base_url=BASE_URL,
                     enabled=True, api_since=date(2026, 9, 9),
                     coverage_since=date(2026, 8, 26), until=None)


def report(**kw) -> dict:
    base = {"date": DATE, "serviceGroup": SERVICE_GROUP, "service": SERVICE,
            "generatedAt": GENERATED_AT, "engine": ENGINE, "gpu": [], "serving": []}
    base.update(kw)
    return base


# ---------- check_report_structure (응답 단위 → PayloadError) ----------

def test_check_report_missing_keys():
    with pytest.raises(PayloadError) as ei:
        check_report_structure({"date": DATE}, DATE)
    assert str(ei.value).startswith("missing_keys:")
    assert "serviceGroup" in str(ei.value) and "serving" in str(ei.value)


def test_check_report_not_object():
    with pytest.raises(PayloadError) as ei:
        check_report_structure([report()], DATE)
    assert str(ei.value) == "not_object"


def test_check_report_date_mismatch():
    with pytest.raises(PayloadError) as ei:
        check_report_structure(report(date="2026-09-09"), DATE)
    assert str(ei.value) == "date_mismatch"


def test_check_report_non_array():
    with pytest.raises(PayloadError) as ei:
        check_report_structure(report(gpu={}), DATE)
    assert str(ei.value) == "gpu_not_array"
    with pytest.raises(PayloadError) as ei:
        check_report_structure(report(serving="x"), DATE)
    assert str(ei.value) == "serving_not_array"


def test_check_report_ok_builds_api_payload():
    p = check_report_structure(report(gpu=[{"model": "m"}], engine=None), DATE)
    assert isinstance(p, MetricsPayload)
    assert p.source_type == SOURCE_API
    assert (p.date, p.reported_service_group, p.reported_service) == (DATE, SERVICE_GROUP, SERVICE)
    assert p.generated_at_raw == GENERATED_AT and p.engine is None
    assert p.gpu == [{"model": "m"}] and p.serving == [] and p.extra_top_keys == []


def test_check_report_extra_keys_recorded():
    p = check_report_structure(report(foo=1, bar=2), DATE)
    assert p.extra_top_keys == ["bar", "foo"]        # 정렬·무시(적재 안 함)


# ---------- parse_generated_at / parse_engine ----------

def test_generated_at_kst_ok():
    dt, warn = parse_generated_at(GENERATED_AT, NOW)
    assert dt == datetime(2026, 9, 11, 2, 5, tzinfo=KST) and warn is None
    assert dt.tzinfo is not None and dt.utcoffset().total_seconds() == 9 * 3600


def test_generated_at_offset_mismatch():
    dt, warn = parse_generated_at("2026-09-10T17:05:00+00:00", NOW)
    assert warn == W_GEN_OFFSET
    assert dt == datetime(2026, 9, 11, 2, 5, tzinfo=KST)       # KST 변환 (다음날 02:05)
    assert dt.utcoffset().total_seconds() == 9 * 3600


def test_generated_at_z_suffix():
    dt, warn = parse_generated_at("2026-09-10T17:05:00Z", NOW)
    assert warn == W_GEN_OFFSET                                # 파싱 성공 + 오프셋 불일치
    assert dt == datetime(2026, 9, 11, 2, 5, tzinfo=KST)


def test_generated_at_parse_failed_uses_now():
    assert parse_generated_at("nope", NOW) == (NOW, W_GEN_PARSE)
    assert parse_generated_at("2026-09-11T02:05:00", NOW) == (NOW, W_GEN_PARSE)   # naive
    assert parse_generated_at("None", NOW) == (NOW, W_GEN_PARSE)                  # str(None)


def test_generated_at_empty_is_now_without_warn():
    assert parse_generated_at("", NOW) == (NOW, None)
    assert parse_generated_at("   ", NOW) == (NOW, None)


def test_engine_variants():
    assert parse_engine(None) == ("", "", False)
    assert parse_engine(ENGINE) == ("vllm", "0.10.1", False)
    assert parse_engine({"type": "sglang"}) == ("sglang", "", False)          # version 부재
    assert parse_engine({"type": "custom", "version": None}) == ("custom", "", False)
    for bad in ({"type": ""}, {"version": "x"}, "vllm", {"type": "a" * 65},
                {"type": "vllm", "version": "v" * 65}, {"type": 7}, ["vllm"]):
        assert parse_engine(bad) == ("", "", True), bad


# ---------- normalize_gpu ----------

from app.normalize import F_DUP_MERGED, F_HOURS_OVER, F_UNKNOWN, GpuRow, normalize_gpu  # noqa: E402


def G(**kw) -> dict:
    base = {"model": "claude-sonnet-5", "gpuType": GPU_TYPE, "category": "serving",
            "gpuCount": 4, "gpuHours": 96.0}
    base.update(kw)
    return base


def test_gpu_reject_rules():
    bad = [
        "not-a-dict",                    # 비dict
        G(model=""),                     # model 빈값
        G(model="   "),                  # model 공백만
        G(model="m" * 129),              # model 129자
        G(gpuType="g" * 65),             # gpuType 65자
        G(category="prod"),              # category ∉ enum
        G(gpuCount=True),                # bool은 숫자 아님
        G(gpuCount=0),                   # gpuCount ≤ 0
        G(gpuHours=-1),                  # gpuHours 음수
        G(gpuHours="24"),                # 문자열 숫자
        G(gpuHours=float("nan")),        # 비유한
    ]
    for raw in bad:
        rows, rejected, merged, counts = normalize_gpu([raw])
        assert (rows, rejected, merged, counts) == ([], 1, 0, {}), raw
    missing = dict(G()); del missing["gpuHours"]
    assert normalize_gpu([missing])[1] == 1


def test_gpu_ok_row_shape():
    rows, rejected, merged, counts = normalize_gpu([G(gpuCount=2, gpuHours=48)])
    assert rejected == 0 and merged == 0 and counts == {}
    assert rows == [GpuRow(model="claude-sonnet-5", gpu_type=GPU_TYPE, category="serving",
                           gpu_count=2.0, gpu_hours=48.0, flags=[])]
    assert isinstance(rows[0].gpu_count, float) and isinstance(rows[0].gpu_hours, float)


def test_gpu_hours_over_count_on_original_rows():
    rows, _, _, counts = normalize_gpu([G(gpuCount=2, gpuHours=49)])
    assert rows[0].flags == [F_HOURS_OVER] and counts == {F_HOURS_OVER: 1}
    rows, _, _, counts = normalize_gpu([G(gpuCount=2, gpuHours=48.0000001)])   # EPS 안
    assert rows[0].flags == [] and counts == {}


def test_gpu_unknown_violation():
    rows, _, _, counts = normalize_gpu([G(model="unknown", category="serving"),
                                        G(model="unknown", category="standby"),
                                        G(model="unknown", category="test")])
    assert [r.flags for r in rows] == [[F_UNKNOWN], [F_UNKNOWN], []]
    assert counts == {F_UNKNOWN: 2}


def test_gpu_dup_merged_sum_hours_max_count():
    rows, rejected, merged, counts = normalize_gpu([G(gpuCount=2, gpuHours=10), G(gpuCount=4, gpuHours=20)])
    assert rejected == 0 and merged == 1
    assert rows == [GpuRow(model="claude-sonnet-5", gpu_type=GPU_TYPE, category="serving",
                           gpu_count=4.0, gpu_hours=30.0, flags=[F_DUP_MERGED])]
    assert counts == {F_DUP_MERGED: 1}
    # 원행 중 하나가 over → 병합행에 hours_over_count도 (병합 전 원행 기준)
    rows, _, merged, counts = normalize_gpu([G(gpuCount=1, gpuHours=30), G(gpuCount=4, gpuHours=20)])
    assert rows[0].flags == [F_HOURS_OVER, F_DUP_MERGED] and rows[0].gpu_hours == 50.0
    assert counts == {F_HOURS_OVER: 1, F_DUP_MERGED: 1}
    # 3행 같은 키 → merged 2, 순서는 첫 등장
    rows, _, merged, _ = normalize_gpu([G(category="test"), G(), G(), G()])
    assert merged == 2 and [r.category for r in rows] == ["test", "serving"]


def test_gpu_extra_keys_ignored():
    rows, rejected, _, _ = normalize_gpu([G(note="x", replicas=3)])
    assert rejected == 0 and len(rows) == 1


# ---------- normalize_serving ----------

from app.normalize import F_DUP_CUSTOM, F_DUP_MODEL, F_PCT, ServingRow, normalize_serving  # noqa: E402

TTFT = {"p50": 280, "p90": 560, "p95": 720, "p99": 1200}
ITL = {"p50": 24, "p90": 38, "p95": 47, "p99": 80}
E2E = {"p50": 1400, "p90": 2600, "p95": 3300, "p99": 5200}


def S(**kw) -> dict:
    base = {"model": "claude-sonnet-5", "ttftMs": dict(TTFT), "itlMs": dict(ITL), "outputTps": {"p50": 41.0}}
    base.update(kw)
    return base


def test_serving_reject_rules():
    bad = [
        "not-a-dict",                                        # 비dict
        S(model=""),                                         # model 빈값
        S(model="m" * 129),                                  # model 129자
        {"model": "m", "foo": 1},                            # 허용 외 키
        {"model": "m"},                                      # 지표 0개
        S(ttftMs={"p50": 1, "p90": 2, "p95": 3}),            # p99 누락
        S(ttftMs={**TTFT, "p999": 9}),                       # 추가 키
        S(ttftMs={**TTFT, "p50": -1}),                       # 음수
        S(ttftMs={**TTFT, "p50": "280"}),                    # 비숫자
        S(ttftMs={**TTFT, "p50": True}),                     # bool
        S(ttftMs=[280, 560, 720, 1200]),                     # dict 아님
        S(outputTps={"p50": 1, "p90": 2}),                   # p50 외 키
        S(outputTps={}),                                     # p50 누락
        S(outputTps={"p50": -0.5}),                          # 음수
        S(custom=[{"unit": "ms", "p50": 1}]),                # custom name 누락
        S(custom=[{"name": "q", "unit": "u" * 33, "p50": 1}]),   # unit 33자
        S(custom=[{"name": "n" * 65, "unit": "ms", "p50": 1}]),  # name 65자
        S(custom=[{"name": "q", "unit": "ms"}]),             # p키 0개
        S(custom=[{"name": "q", "unit": "ms", "p50": "x"}]), # p값 비숫자
        S(custom=[{"name": "q", "unit": "ms", "p50": 1, "avg": 2}]),   # 허용 외 키
        S(custom={"name": "q", "unit": "ms", "p50": 1}),     # custom이 list 아님
        S(custom=["q"]),                                     # 원소 dict 아님
    ]
    for rec in bad:
        rows, rejected, counts = normalize_serving([rec])
        assert (rows, rejected, counts) == ([], 1, {}), rec


def test_serving_long_form_case_a():
    rows, rejected, counts = normalize_serving([S()])
    assert rejected == 0 and counts == {}
    assert [(r.metric, r.name, r.unit) for r in rows] == [("ttft_ms", "", "ms"), ("itl_ms", "", "ms"),
                                                          ("output_tps", "", "tokens/s")]
    assert rows[0] == ServingRow(model="claude-sonnet-5", metric="ttft_ms", name="", unit="ms",
                                 p50=280.0, p90=560.0, p95=720.0, p99=1200.0, flags=[])
    assert (rows[2].p50, rows[2].p90, rows[2].p95, rows[2].p99) == (41.0, None, None, None)


def test_serving_case_f_e2e_and_custom():
    rec = {"model": "claude-haiku-4-5", "e2eMs": dict(E2E),
           "custom": [{"name": "queueWaitMs", "unit": "ms", "p50": 120, "p90": 300},
                      {"name": "batchSize", "unit": "requests", "p50": 8}]}
    rows, rejected, counts = normalize_serving([rec])
    assert rejected == 0 and counts == {} and len(rows) == 3
    assert (rows[0].metric, rows[0].unit, rows[0].p99) == ("e2e_ms", "ms", 5200.0)
    assert (rows[1].metric, rows[1].name, rows[1].unit, rows[1].p50, rows[1].p90, rows[1].p95, rows[1].p99) == \
        ("custom", "queueWaitMs", "ms", 120.0, 300.0, None, None)
    assert (rows[2].metric, rows[2].name, rows[2].unit, rows[2].p50, rows[2].p95) == \
        ("custom", "batchSize", "requests", 8.0, None)


def test_serving_row_order_across_records():
    rows, _, _ = normalize_serving([{"model": "b", "outputTps": {"p50": 1}, "ttftMs": dict(TTFT)},
                                    {"model": "a", "e2eMs": dict(E2E)}])
    assert [(r.model, r.metric) for r in rows] == [("b", "ttft_ms"), ("b", "output_tps"), ("a", "e2e_ms")]


def test_serving_empty_custom_list_is_valid_zero_rows():
    rows, rejected, counts = normalize_serving([{"model": "m", "custom": []}])
    assert (rows, rejected, counts) == ([], 0, {})


def test_serving_pct_non_monotone():
    rows, _, counts = normalize_serving([S(ttftMs={**TTFT, "p90": 100})])          # p90 < p50
    assert rows[0].flags == [F_PCT] and rows[1].flags == [] and rows[2].flags == []
    assert counts == {F_PCT: 1}
    rows, _, counts = normalize_serving([S(ttftMs={**TTFT, "p90": 280 - 1e-7})])   # EPS 안
    assert rows[0].flags == [] and counts == {}
    rows, _, counts = normalize_serving([S(custom=[{"name": "q", "unit": "ms", "p50": 5, "p90": 4}])])
    assert rows[3].metric == "custom" and rows[3].flags == [F_PCT] and counts == {F_PCT: 1}
    rows, _, counts = normalize_serving([S(custom=[{"name": "q", "unit": "ms", "p50": 5, "p99": 4}])])
    assert rows[3].flags == [F_PCT]                                                  # 부재 p는 건너뛰고 비교


def test_serving_dup_model_kept_first():
    rows, rejected, counts = normalize_serving([S(), S(ttftMs={**TTFT, "p50": 1})])
    assert rejected == 0 and len(rows) == 3
    assert rows[0].p50 == 280.0                                # 첫 레코드 값 유지
    assert all(r.flags == [F_DUP_MODEL] for r in rows)
    assert counts == {F_DUP_MODEL: 1}
    rows, rejected, counts = normalize_serving([S(), S(), {"model": "other", "e2eMs": dict(E2E)}])
    assert len(rows) == 4 and counts == {F_DUP_MODEL: 1} and rows[3].flags == []


def test_serving_dup_model_after_rejected_record_is_reject_not_dup():
    rows, rejected, counts = normalize_serving([S(), S(ttftMs={"p50": 1})])       # 2번째는 형태 위반
    assert rejected == 1 and counts == {} and all(r.flags == [] for r in rows)


def test_serving_dup_custom_kept_first():
    rec = S(custom=[{"name": "q", "unit": "ms", "p50": 1}, {"name": "q", "unit": "ms", "p50": 2},
                    {"name": "r", "unit": "ms", "p50": 3}])
    rows, rejected, counts = normalize_serving([rec])
    customs = [r for r in rows if r.metric == "custom"]
    assert rejected == 0 and [(c.name, c.p50) for c in customs] == [("q", 1.0), ("r", 3.0)]
    assert customs[0].flags == [F_DUP_CUSTOM] and customs[1].flags == []
    assert counts == {F_DUP_CUSTOM: 1}


def test_serving_unknown_violation():
    rows, _, counts = normalize_serving([S(model="unknown", custom=[{"name": "q", "unit": "ms", "p50": 1}])])
    assert len(rows) == 4 and all(r.flags == [F_UNKNOWN] for r in rows)
    assert counts == {F_UNKNOWN: 4}
    rows, _, _ = normalize_serving([S(model="unknown", ttftMs={**TTFT, "p90": 1})])
    assert rows[0].flags == [F_PCT, F_UNKNOWN]                 # 고정 순서


def test_serving_custom_negative_allowed():
    rows, rejected, _ = normalize_serving([{"model": "m", "custom": [{"name": "delta", "unit": "ms", "p50": -1}]}])
    assert rejected == 0 and rows[0].p50 == -1.0


# ---------- normalize_payload ----------

from app.normalize import (SOURCE_MANUAL, W_ENGINE, W_EXTRA_KEYS, W_IDENTITY,  # noqa: E402
                           NormalizeResult, normalize_payload)


def payload(**kw) -> MetricsPayload:
    base = dict(date=DATE, reported_service_group=SERVICE_GROUP, reported_service=SERVICE,
                generated_at_raw=GENERATED_AT, engine=dict(ENGINE), gpu=[], serving=[],
                source_type=SOURCE_API)
    base.update(kw)
    return MetricsPayload(**base)


def test_payload_identity_drift_api_only():
    r = normalize_payload(payload(reported_service="Mock Service A "), ENTRY, now=NOW)
    assert r.warns == {W_IDENTITY: 1} and r.warn_total == 1
    r = normalize_payload(payload(reported_service_group="Other"), ENTRY, now=NOW)
    assert r.warns == {W_IDENTITY: 1}
    r = normalize_payload(payload(reported_service="Mock Service A ", source_type=SOURCE_MANUAL), ENTRY, now=NOW)
    assert r.warns == {}


def test_payload_counts_and_nodata():
    r = normalize_payload(payload(gpu=[G(), G(category="standby", gpuCount=1, gpuHours=24)],
                                  serving=[S(custom=[{"name": "q", "unit": "ms", "p50": 1}])]), ENTRY, now=NOW)
    assert (r.n_gpu, r.n_serving, r.n_custom, r.rows) == (2, 3, 1, 6)
    assert r.rejected == 0 and r.merged_dups == 0 and r.warns == {} and not r.is_nodata
    assert (r.engine_type, r.engine_version) == ("vllm", "0.10.1")
    assert r.generated_at == datetime(2026, 9, 11, 2, 5, tzinfo=KST)
    assert normalize_payload(payload(), ENTRY, now=NOW).is_nodata                        # gpu:[] serving:[]
    r = normalize_payload(payload(serving=[S()]), ENTRY, now=NOW)                          # 케이스 E
    assert not r.is_nodata and r.rows == 3
    r = normalize_payload(payload(gpu=[G(category="prod")]), ENTRY, now=NOW)               # 전량 거부
    assert (r.rows, r.rejected, r.is_nodata) == (0, 1, False)


def test_payload_rejected_and_merged_sum_both_blocks():
    r = normalize_payload(payload(gpu=[G(), G(), G(gpuCount=0)], serving=[S(), {"model": "m"}]), ENTRY, now=NOW)
    assert (r.n_gpu, r.merged_dups, r.rejected) == (1, 1, 2)
    assert r.warns == {F_DUP_MERGED: 1}


def test_payload_warn_total_sums_flags_and_response_warns():
    r = normalize_payload(payload(gpu=[G(gpuCount=2, gpuHours=49)], reported_service="X"), ENTRY, now=NOW)
    assert r.warns == {F_HOURS_OVER: 1, W_IDENTITY: 1} and r.warn_total == 2
    assert r.gpu_rows[0].flags == [F_HOURS_OVER]


def test_payload_response_warns_generated_at_engine_extra_keys():
    r = normalize_payload(payload(generated_at_raw="nope", engine="vllm", extra_top_keys=["a", "b"]), ENTRY, now=NOW)
    assert r.warns == {W_GEN_PARSE: 1, W_ENGINE: 1, W_EXTRA_KEYS: 2}
    assert r.generated_at == NOW and (r.engine_type, r.engine_version) == ("", "")
    r = normalize_payload(payload(generated_at_raw="2026-09-10T17:05:00Z"), ENTRY, now=NOW)
    assert r.warns == {W_GEN_OFFSET: 1} and r.generated_at == datetime(2026, 9, 11, 2, 5, tzinfo=KST)


def test_payload_generated_at_now_injected():
    r = normalize_payload(payload(generated_at_raw="", source_type=SOURCE_MANUAL), ENTRY, now=NOW)
    assert r.generated_at is NOW and r.warns == {}
    r = normalize_payload(payload(generated_at_raw=""), ENTRY)                              # now 미주입 → aware KST
    assert r.generated_at.tzinfo is not None and r.generated_at.utcoffset().total_seconds() == 9 * 3600


def test_payload_engine_null_no_warn():
    r = normalize_payload(payload(engine=None), ENTRY, now=NOW)
    assert r.warns == {} and (r.engine_type, r.engine_version) == ("", "")


def test_payload_non_array_raises():
    with pytest.raises(PayloadError) as ei:
        normalize_payload(payload(gpu={}), ENTRY, now=NOW)
    assert str(ei.value) == "gpu_not_array"
    with pytest.raises(PayloadError) as ei:
        normalize_payload(payload(serving=None), ENTRY, now=NOW)
    assert str(ei.value) == "serving_not_array"


def test_normalize_result_direct_construction_defaults():
    r = NormalizeResult(generated_at=NOW)
    assert (r.rows, r.rejected, r.merged_dups, r.warns, r.engine_type, r.engine_version) == (0, 0, 0, {}, "", "")
    assert r.is_nodata and r.warn_total == 0
