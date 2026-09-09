from app.ch import DB_TOKEN_DIM, DB_TOKEN_MART
from app.preflight import READ_CONTRACT, contract_tables, missing_columns


def _full() -> dict[str, list[str]]:
    """계약 그대로의 DESCRIBE 결과 흉내(사내 여분 컬럼 없음)."""
    return {table: list(cols) for table, cols in READ_CONTRACT.items()}


def test_contract_is_three_tables_thirteen_columns():
    assert len(READ_CONTRACT) == 3
    assert sum(len(v) for v in READ_CONTRACT.values()) == 13
    assert READ_CONTRACT["mart.token_usage_1d"] == (
        "date", "service_group", "service", "model",
        "input_tokens", "cache_read_tokens", "cache_creation_tokens", "output_tokens",
        "requests")
    assert READ_CONTRACT["mart.agg_token_service_1d"] == ("date", "service")
    assert READ_CONTRACT["gpu_data.dim_token_service"] == ("service", "enabled")
    # §5.6 로깅 계약과 무관하게, 계약에 user_id/user_type 등 개인 식별 컬럼은 없어야 한다
    assert not any(c.startswith("user_") for cols in READ_CONTRACT.values() for c in cols)


def test_contract_tables_use_token_db_constants_without_dist_suffix():
    tables = contract_tables()
    assert tables == [f"{DB_TOKEN_MART}.token_usage_1d",
                      f"{DB_TOKEN_MART}.agg_token_service_1d",
                      f"{DB_TOKEN_DIM}.dim_token_service"]
    assert all(t.count(".") == 1 for t in tables)                       # 'db.table'
    assert not any(t.endswith("_dist") or t.endswith("_local") for t in tables)
    assert tables == list(READ_CONTRACT)                                # 선언 순서 유지


def test_missing_columns_empty_when_superset():
    described = _full()
    described["mart.token_usage_1d"] += ["user_id", "user_type", "batch_time"]   # 사내 여분 컬럼
    described["gpu_data.dim_token_service"] = ["service_group", "service", "base_url",
                                               "enabled", "note"]               # 순서 무관
    described["mart.some_other_table"] = ["x"]                                   # 계약 밖 테이블 무시
    assert missing_columns(described) == []
    assert missing_columns(_full()) == []


def test_missing_columns_reports_table_and_column():
    described = _full()
    described["mart.token_usage_1d"].remove("requests")
    assert missing_columns(described) == ["mart.token_usage_1d.requests"]

    described = _full()
    del described["gpu_data.dim_token_service"]                # 테이블 키 부재
    assert missing_columns(described) == ["gpu_data.dim_token_service.*"]

    described = _full()
    described["gpu_data.dim_token_service"] = []               # CHGate.describe()의 부재 응답 []
    described["mart.agg_token_service_1d"].remove("service")
    described["mart.token_usage_1d"].remove("cache_read_tokens")
    assert missing_columns(described) == [                     # 정렬(테이블 → 컬럼)
        "gpu_data.dim_token_service.*",
        "mart.agg_token_service_1d.service",
        "mart.token_usage_1d.cache_read_tokens",
    ]
    assert missing_columns({}) == [f"{t}.*" for t in sorted(READ_CONTRACT)]
