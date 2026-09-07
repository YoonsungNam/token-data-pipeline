#!/usr/bin/env python3
"""E2E 시드 - Plan 6c T10 시나리오(Mock Service A/B/C/D)를 단일노드 CH에 적재한다.

적재 대상(모두 <db>.<table>_dist - run_e2e.sh가 단일노드 MergeTree로 만든 대역):
  gpu_data.dim_token_service_dist          토큰 측 레지스트리(A/B/C/D enabled=1)
  gpu_data.dim_token_metrics_service_dist  메트릭 레지스트리(A/B/C - C는 앵커 없음 -> metrics_missing FAIL, coverage 2/3)
  fact.raw_token_metrics_gpu_1d_dist       gpu 6행(A: Qwen3-32B/H100 serving·standby·test, B: claude-sonnet-5 3행 + FAIL 1)
  fact.raw_token_metrics_serving_1d_dist   serving 2행(A: ttft_ms·output_tps)
  fact.raw_token_metrics_summary_1d_dist   앵커 2행(A metrics-api-v1 rejected 1, B manual-v0)
  mart.token_usage_1d_dist                 토큰 6행(A/B/D x 합성 사용자 2 - Qwen3-32B)
  mart.agg_token_service_1d_dist           서비스 집계 3행(A/B/D)

결정성: 수치는 전부 모듈 상수, 합성 user_id는 sha256(f"{service}|{date}|{k}") 앞 12자 - random 미사용.
정본 이원화: TCO_KRW/ALLOCATION/ALIASES/VENDOR_PRICE는 tests/e2e/ddl_test_dims.sql 시드의 파이썬 재현이며
tests/test_e2e_seed.py가 두 파일을 교차 대조한다(값을 고치면 둘 다 고친다).

사용법: CH_HOST=127.0.0.1 CH_PORT=18124 python3 tests/e2e/seed_metrics.py 2026-09-03
"""
import hashlib
import os
import sys
from datetime import date as date_cls
from datetime import datetime, time, timedelta, timezone

KST = timezone(timedelta(hours=9))

DB_FACT = os.getenv("CH_DB_FACT", "fact")
DB_DIM = os.getenv("CH_DB_DIM", "gpu_data")
DB_MART = os.getenv("CH_DB_MART", "mart")

SERVICE_GROUP = "Mock Group"
SVC_A = "Mock Service A"
SVC_B = "Mock Service B"
SVC_C = "Mock Service C"
SVC_D = "Mock Service D"
MODEL_QWEN = "Qwen3-32B"
MODEL_SONNET = "claude-sonnet-5"
SOURCE_API = "metrics-api-v1"
SOURCE_MANUAL = "manual-v0"
CREATED_BY_TOKEN_MART = "token-pipeline"       # mart.token_usage_1d CHECK created_by != '' (token-usage 배치의 값)
REGISTRY_SINCE = date_cls(2026, 8, 26)          # api_since = coverage_since (digest §20 fixture와 동일)

# ---- ddl_test_dims.sql 시드의 파이썬 재현(교차 대조 대상) ----
TCO_KRW = {"unknown": None, "H100": 4200.0, "A100": 2100.0, "H200": 5300.0, "L40S": 1300.0}
ALLOCATION = {("unknown", "unknown"): None, ("Mock Group", "H100"): 8.0, ("Mock Group", "A100"): 4.0}
ALIASES = {
    "unknown": "unknown",
    "claude-opus-4-8": "claude-opus-4-8",
    "claude-sonnet-5": "claude-sonnet-5",
    "claude-haiku-4-5": "claude-haiku-4-5",
    "claude-sonnet-5-20260101": "claude-sonnet-5",
    "opus-4.8": "claude-opus-4-8",
    "Qwen3-32B": "Qwen3-32B",
}
VENDOR_PRICE = {
    ("unknown", "unknown"): (None, None, None, None),
    ("anthropic", "claude-opus-4-8"): (6750.0, 675.0, 8437.5, 33750.0),
    ("anthropic", "claude-sonnet-5"): (4050.0, 405.0, 5062.5, 20250.0),
    ("anthropic", "claude-haiku-4-5"): (1350.0, 135.0, 1687.5, 6750.0),
}

# ---- 시나리오 상수 ----
USAGE_REGISTRY = (SVC_A, SVC_B, SVC_C, SVC_D)
# (service, expect_gpu, expect_serving, usage_includes_consumers)
METRICS_REGISTRY = ((SVC_A, 1, 1, 0), (SVC_B, 1, 0, 0), (SVC_C, 1, 1, 0))
# (service, model, gpu_type, category, gpu_count, gpu_hours, flags, source_type)
GPU_ROWS = (
    (SVC_A, MODEL_QWEN, "H100", "serving", 2.0, 40.0, (), SOURCE_API),
    (SVC_A, MODEL_QWEN, "H100", "standby", 1.0, 8.0, (), SOURCE_API),
    (SVC_A, MODEL_QWEN, "H100", "test", 1.0, 2.0, (), SOURCE_API),
    (SVC_B, MODEL_SONNET, "H100", "serving", 1.0, 20.0, (), SOURCE_MANUAL),
    (SVC_B, MODEL_SONNET, "B200", "serving", 1.0, 4.0, (), SOURCE_MANUAL),
    (SVC_B, MODEL_SONNET, "H100", "standby", 2.0, 50.0, ("hours_over_count",), SOURCE_MANUAL),
)
# (service, model, metric, name, unit, p50, p90, p95, p99, flags, source_type)
SERVING_ROWS = (
    (SVC_A, MODEL_QWEN, "ttft_ms", "", "ms", 120.0, 240.0, 300.0, 450.0, (), SOURCE_API),
    (SVC_A, MODEL_QWEN, "output_tps", "", "tokens/s", 80.0, 60.0, 55.0, 40.0, (), SOURCE_API),
)
# (service, gpu_rows, serving_rows, custom_rows, rejected_rows, merged_dups, source_type)
SUMMARY_ROWS = ((SVC_A, 3, 2, 0, 1, 0, SOURCE_API), (SVC_B, 3, 0, 0, 0, 0, SOURCE_MANUAL))
# (service, input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, requests) - 모델은 전부 Qwen3-32B
TOKEN_SCENARIO = (
    (SVC_A, 2_000_000, 5_000_000, 0, 250_000, 100),
    (SVC_B, 4_000_000, 10_000_000, 0, 500_000, 200),
    (SVC_D, 500_000, 0, 0, 0, 10),
)

# ---- 컬럼 순서(DDL 정본과 1:1) ----
DIM_TOKEN_SERVICE_COLS = ("service_group", "service", "base_url", "enabled", "source_type", "note", "updated_at")
DIM_METRICS_SERVICE_COLS = ("service_group", "service", "base_url", "enabled", "api_since", "coverage_since", "until",
                            "expect_gpu", "expect_serving", "usage_includes_consumers", "note", "updated_at")
GPU_COLS = ("date", "service_group", "service", "model", "gpu_type", "category", "gpu_count", "gpu_hours", "flags",
            "source_type", "generated_at", "collected_at")
SERVING_COLS = ("date", "service_group", "service", "model", "metric", "name", "unit", "p50", "p90", "p95", "p99",
                "flags", "source_type", "generated_at", "collected_at")
SUMMARY_COLS = ("date", "service_group", "service", "reported_service_group", "reported_service", "engine_type",
                "engine_version", "gpu_rows", "serving_rows", "custom_rows", "rejected_rows", "merged_dups",
                "source_type", "generated_at", "collected_at")
TOKEN_USAGE_COLS = ("date", "service_group", "service", "user_id", "user_type", "user_name", "model", "org_path",
                    "org_top", "org_leaf", "input_tokens", "cache_read_tokens", "cache_creation_tokens",
                    "output_tokens", "total_input_tokens", "requests", "cost", "created_by")
AGG_SERVICE_COLS = ("date", "service_group", "service", "input_tokens", "cache_read_tokens", "cache_creation_tokens",
                    "output_tokens", "total_input_tokens", "requests", "distinct_users", "cost", "is_derived",
                    "created_by")

SEED_TABLES = {
    "dim_token_service": (f"{DB_DIM}.dim_token_service_dist", DIM_TOKEN_SERVICE_COLS),
    "dim_metrics_service": (f"{DB_DIM}.dim_token_metrics_service_dist", DIM_METRICS_SERVICE_COLS),
    "gpu": (f"{DB_FACT}.raw_token_metrics_gpu_1d_dist", GPU_COLS),
    "serving": (f"{DB_FACT}.raw_token_metrics_serving_1d_dist", SERVING_COLS),
    "summary": (f"{DB_FACT}.raw_token_metrics_summary_1d_dist", SUMMARY_COLS),
    "token_usage": (f"{DB_MART}.token_usage_1d_dist", TOKEN_USAGE_COLS),
    "agg_service": (f"{DB_MART}.agg_token_service_1d_dist", AGG_SERVICE_COLS),
}


def synthetic_user_id(service: str, date: str, k: int) -> str:
    """합성 user_id - 결정적(sha256), 실제 사번/이메일 형태 아님."""
    return "u-" + hashlib.sha256(f"{service}|{date}|{k}".encode("utf-8")).hexdigest()[:12]


def _split_two(value: int) -> tuple:
    """서비스 합계를 합성 사용자 2명으로 나눈다(합이 보존되도록 floor/나머지)."""
    return (value // 2, value - value // 2)


def _base_url(service: str) -> str:
    return f"http://mock-{service[-1].lower()}.invalid"


def build_seed(date: str) -> dict:
    """date(YYYY-MM-DD) 하루치 시드 - SEED_TABLES 키 순서의 {key: [tuple, ...]} (컬럼 순서 = SEED_TABLES[key][1])."""
    d = date_cls.fromisoformat(date)
    next_day = d + timedelta(days=1)
    generated_at = datetime.combine(next_day, time(2, 5), tzinfo=KST)   # 수집기 계약: D+1 02:05 KST 생성
    collected_at = datetime.combine(next_day, time(4, 5), tzinfo=KST)   # D+1 04:05 KST 수집
    registry_updated_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=KST)

    seed = {}
    seed["dim_token_service"] = [
        (SERVICE_GROUP, svc, _base_url(svc), 1, "usage-api-v1", "e2e", registry_updated_at)
        for svc in USAGE_REGISTRY]
    seed["dim_metrics_service"] = [
        (SERVICE_GROUP, svc, _base_url(svc), 1, REGISTRY_SINCE, REGISTRY_SINCE, None,
         expect_gpu, expect_serving, uic, "e2e", registry_updated_at)
        for svc, expect_gpu, expect_serving, uic in METRICS_REGISTRY]
    seed["gpu"] = [
        (d, SERVICE_GROUP, svc, model, gpu_type, category, gpu_count, gpu_hours, list(flags), source_type,
         generated_at, collected_at)
        for svc, model, gpu_type, category, gpu_count, gpu_hours, flags, source_type in GPU_ROWS]
    seed["serving"] = [
        (d, SERVICE_GROUP, svc, model, metric, name, unit, p50, p90, p95, p99, list(flags), source_type,
         generated_at, collected_at)
        for svc, model, metric, name, unit, p50, p90, p95, p99, flags, source_type in SERVING_ROWS]
    seed["summary"] = [
        (d, SERVICE_GROUP, svc, SERVICE_GROUP, svc, "", "", gpu_rows, serving_rows, custom_rows, rejected_rows,
         merged_dups, source_type, generated_at, collected_at)
        for svc, gpu_rows, serving_rows, custom_rows, rejected_rows, merged_dups, source_type in SUMMARY_ROWS]

    token_rows = []
    agg_rows = []
    for svc, input_tokens, cache_read, cache_creation, output_tokens, requests in TOKEN_SCENARIO:
        for k in (0, 1):
            i, cr, cc, o, r = (_split_two(v)[k] for v in (input_tokens, cache_read, cache_creation,
                                                          output_tokens, requests))
            token_rows.append((d, SERVICE_GROUP, svc, synthetic_user_id(svc, date, k), "identified", "",
                               MODEL_QWEN, ["unknown"], "unknown", "unknown",
                               i, cr, cc, o, i + cr + cc, r, None, CREATED_BY_TOKEN_MART))
        agg_rows.append((d, SERVICE_GROUP, svc, input_tokens, cache_read, cache_creation, output_tokens,
                         input_tokens + cache_read + cache_creation, requests, 2, None, 0, CREATED_BY_TOKEN_MART))
    seed["token_usage"] = token_rows
    seed["agg_service"] = agg_rows
    return seed


def seed_all(client, date: str) -> dict:
    """build_seed(date)를 SEED_TABLES 순서로 INSERT - {key: rows}. 멱등 아님(run_e2e.sh가 새 컨테이너에 1회 호출)."""
    counts = {}
    for key, rows in build_seed(date).items():
        table, cols = SEED_TABLES[key]
        client.insert(table, rows, column_names=list(cols))
        counts[key] = len(rows)
    return counts


def _client():
    """clickhouse_connect는 여기서만 import - 단위 테스트(tests/test_e2e_seed.py)는 드라이버 없이 build_seed만 쓴다."""
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", "127.0.0.1"),
        port=int(os.getenv("CH_PORT", "8123")),
        username=os.getenv("CH_USER", "default"),
        password=os.getenv("CH_PASSWORD", ""),
    )


def main(argv=None) -> int:
    args = sys.argv[1:] if argv is None else list(argv)
    if len(args) != 1:
        print("usage: seed_metrics.py <YYYY-MM-DD>", file=sys.stderr)
        return 2
    try:
        date_cls.fromisoformat(args[0])
    except ValueError:
        print("date must be YYYY-MM-DD", file=sys.stderr)
        return 2
    counts = seed_all(_client(), args[0])
    for key, n in counts.items():
        print(f"seeded {SEED_TABLES[key][0]} rows={n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
