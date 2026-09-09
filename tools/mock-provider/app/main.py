import re
import time
from dataclasses import fields as dc_fields
from datetime import date as date_cls, datetime, timedelta, timezone

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.config import load_config
from app.cursors import CursorError, decode_cursor, encode_cursor
from app.datagen import build_metrics, build_records, build_summary, generated_at, to_api_dict
from app.scenarios import ScenarioState

KST = timezone(timedelta(hours=9))

app = FastAPI(title="token-usage-api mock provider")
CFG = load_config()
SCN = ScenarioState()
STARTED_AT = time.monotonic()


def now_kst() -> datetime:
    return datetime.now(KST)


def _err(status: int, code: str, message: str, retry_after: int | None = None) -> JSONResponse:
    headers = {"Retry-After": str(retry_after)} if retry_after is not None else None
    return JSONResponse({"code": code, "message": message}, status_code=status, headers=headers)


@app.exception_handler(RequestValidationError)
async def _validation_error_handler(request: Request, exc: RequestValidationError):
    return _err(400, "invalid_request", "malformed or missing query parameters")


@app.exception_handler(Exception)
async def _internal_error_handler(request: Request, exc: Exception):
    return _err(500, "internal_error", "unexpected server error")


def _shared_gate() -> JSONResponse | None:
    """요청 공통 게이트: 429/503 주기 시나리오 (OFF면 통과)."""
    SCN.request_count += 1
    n = SCN.request_count
    if SCN.rate_limit_every and n % SCN.rate_limit_every == 0:
        return _err(429, "rate_limited", "too many requests; retry after the indicated delay",
                    retry_after=SCN.retry_after_s)
    if SCN.error_503_every and n % SCN.error_503_every == 0:
        return _err(503, "service_unavailable", "service temporarily unavailable; retry with backoff",
                    retry_after=SCN.retry_after_s)
    return None


def _date_gate(raw_date: str, retention_days: int | None = None,
               subject: str = "usage") -> tuple[date_cls | None, JSONResponse | None]:
    """계약의 date 규칙: 당일/미래 400, 보존 초과 404, 미확정 409.

    retention_days/subject는 /v1/metrics용 additive 인자 — 기본값이면 기존 usage 동작·메시지와 바이트 동일.
    """
    if retention_days is None:
        retention_days = CFG.retention_days
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_date):
        return None, _err(400, "invalid_date", "date must be YYYY-MM-DD")
    try:
        d = date_cls.fromisoformat(raw_date)
    except ValueError:
        return None, _err(400, "invalid_date", "date must be YYYY-MM-DD")
    today = now_kst().date()
    if d >= today:
        return None, _err(400, "invalid_date", "date must be a past day (KST)")
    if time.monotonic() - STARTED_AT < SCN.not_ready_until_uptime_s:
        return None, _err(409, "data_not_ready",
                          f"{subject} for the requested date is not finalized yet; retry later",
                          retry_after=SCN.retry_after_s)
    if d < today - timedelta(days=retention_days):
        return None, _err(404, "data_not_retained",
                          f"{subject} data for the requested date is past the retention window")
    return d, None


def _identity() -> tuple[str, str]:
    return CFG.service_group + SCN.name_drift, CFG.service + SCN.name_drift


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get("/v1/usage")
def get_usage(date: str | None = Query(None), cursor: str | None = Query(None),
              limit: str = Query("1000")):
    if (gate := _shared_gate()) is not None:
        return gate
    if date is None:
        return _err(400, "invalid_date", "date query parameter is required")
    try:
        limit_val = int(limit)
    except ValueError:
        return _err(400, "invalid_limit", "limit must be an integer")
    if not 1 <= limit_val <= 5000:
        return _err(400, "invalid_limit", "limit must be within 1..5000")
    _, date_err = _date_gate(date)
    if date_err is not None:
        return date_err
    offset = 0
    if cursor is not None:
        try:
            offset = decode_cursor(cursor, date, limit_val)
        except CursorError as exc:
            return _err(400, "invalid_cursor", str(exc))
    page_no = offset // limit_val + 1
    if SCN.not_ready_at_page and page_no >= SCN.not_ready_at_page:
        return _err(409, "data_not_ready",
                    "usage for the requested date is not finalized yet; retry later",
                    retry_after=SCN.retry_after_s)
    records = build_records(CFG, date)
    page = records[offset:offset + limit_val]
    gen = generated_at(date)
    if SCN.generated_at_change_at_page and page_no >= SCN.generated_at_change_at_page:
        gen = gen.replace("T02:05:00", "T02:35:00")
    group, service = _identity()
    body: dict = {
        "serviceGroup": group,
        "service": service,
        "date": date,
        "generatedAt": gen,
        "records": [to_api_dict(r) for r in page],
    }
    if offset + limit_val < len(records):
        body["nextCursor"] = encode_cursor(offset + limit_val, date, limit_val)
    return body


@app.get("/v1/usage/summary")
def get_usage_summary(date: str | None = Query(None)):
    if (gate := _shared_gate()) is not None:
        return gate
    if date is None:
        return _err(400, "invalid_date", "date query parameter is required")
    _, date_err = _date_gate(date)
    if date_err is not None:
        return date_err
    summary = build_summary(build_records(CFG, date))
    if SCN.summary_extra_pct:
        summary["inputTokens"] = summary["inputTokens"] * (100 + SCN.summary_extra_pct) // 100
    group, service = _identity()
    return {"serviceGroup": group, "service": service, "date": date,
            "generatedAt": generated_at(date), **summary}


@app.get("/v1/metrics")
def get_metrics(date: str | None = Query(None)):
    """token-metric-api @6a552d2 GET /v1/metrics — 단건, 보존 CFG.metrics_retention_days(기본 14)."""
    if (gate := _shared_gate()) is not None:
        return gate
    if date is None:
        return _err(400, "invalid_date", "date query parameter is required")
    _, date_err = _date_gate(date, retention_days=CFG.metrics_retention_days, subject="metrics")
    if date_err is not None:
        return date_err
    payload = build_metrics(CFG, date, SCN)
    payload["serviceGroup"], payload["service"] = _identity()
    return payload


_SCENARIO_RULES: dict[str, tuple[type, int | float]] = {
    # field: (required type, minimum)
    "not_ready_until_uptime_s": (float, 0),
    "retry_after_s": (int, 1),            # 계약 Retry-After minimum: 1
    "rate_limit_every": (int, 0),
    "error_503_every": (int, 0),
    "summary_extra_pct": (int, -99),      # -100 이하면 음수 토큰 → 계약 minimum: 0 위반
    "name_drift": (str, 0),
    "generated_at_change_at_page": (int, 0),
    "not_ready_at_page": (int, 0),
    # /v1/metrics 전용 int 플래그 6종 (0=OFF, 1=ON; 최대값 검사 없음 — 0/1만 의미)
    "metrics_gpu_hours_over": (int, 0),
    "metrics_unknown_serving": (int, 0),
    "metrics_pct_non_monotone": (int, 0),
    "metrics_dup_gpu_rows": (int, 0),
    "metrics_empty_gpu": (int, 0),
    "metrics_engine_null": (int, 0),
}


@app.post("/__mock/scenario")
def set_scenario(payload: dict):
    unknown = set(payload) - set(_SCENARIO_RULES)
    if unknown:
        return _err(400, "invalid_scenario", f"unknown scenario fields: {sorted(unknown)}")
    for key, value in payload.items():
        want, minimum = _SCENARIO_RULES[key]
        if want is float and type(value) is int:
            value = float(value)                      # uptime 초는 int 입력 허용
        if type(value) is not want:                   # bool 거부 포함 (type is 비교)
            return _err(400, "invalid_scenario", f"{key} must be {want.__name__}")
        if want is not str and value < minimum:
            return _err(400, "invalid_scenario", f"{key} must be >= {minimum}")
        payload[key] = value
    for key, value in payload.items():
        setattr(SCN, key, value)
    return {f.name: getattr(SCN, f.name) for f in dc_fields(ScenarioState)}


@app.post("/__mock/reset")
def reset_scenario():
    global SCN
    SCN = ScenarioState()
    return {"status": "reset"}
