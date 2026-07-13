import time
from datetime import date as date_cls, datetime, timedelta, timezone

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from app.config import load_config
from app.cursors import CursorError, decode_cursor, encode_cursor
from app.datagen import build_records, build_summary, generated_at, to_api_dict
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


def _date_gate(raw_date: str) -> tuple[date_cls | None, JSONResponse | None]:
    """계약의 date 규칙: 당일/미래 400, 보존 초과 404, 미확정 409."""
    try:
        d = date_cls.fromisoformat(raw_date)
    except ValueError:
        return None, _err(400, "invalid_date", "date must be YYYY-MM-DD")
    today = now_kst().date()
    if d >= today:
        return None, _err(400, "invalid_date", "date must be a past day (KST)")
    if d < today - timedelta(days=CFG.retention_days):
        return None, _err(404, "data_not_retained",
                          "usage data for the requested date is past the retention window")
    if time.monotonic() - STARTED_AT < SCN.not_ready_until_uptime_s:
        return None, _err(409, "data_not_ready",
                          "usage for the requested date is not finalized yet; retry later",
                          retry_after=SCN.retry_after_s)
    return d, None


def _identity() -> tuple[str, str]:
    return CFG.service_group + SCN.name_drift, CFG.service + SCN.name_drift


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get("/v1/usage")
def get_usage(date: str = Query(...), cursor: str | None = Query(None),
              limit: int = Query(1000)):
    if (gate := _shared_gate()) is not None:
        return gate
    if not 1 <= limit <= 5000:
        return _err(400, "invalid_limit", "limit must be within 1..5000")
    _, date_err = _date_gate(date)
    if date_err is not None:
        return date_err
    offset = 0
    if cursor is not None:
        try:
            offset = decode_cursor(cursor, date, limit)
        except CursorError as exc:
            return _err(400, "invalid_cursor", str(exc))
    page_no = offset // limit + 1
    if SCN.not_ready_at_page and page_no >= SCN.not_ready_at_page:
        return _err(409, "data_not_ready",
                    "usage for the requested date is not finalized yet; retry later",
                    retry_after=SCN.retry_after_s)
    records = build_records(CFG, date)
    page = records[offset:offset + limit]
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
    if offset + limit < len(records):
        body["nextCursor"] = encode_cursor(offset + limit, date, limit)
    return body


@app.get("/v1/usage/summary")
def get_usage_summary(date: str = Query(...)):
    if (gate := _shared_gate()) is not None:
        return gate
    _, date_err = _date_gate(date)
    if date_err is not None:
        return date_err
    summary = build_summary(build_records(CFG, date))
    if SCN.summary_extra_pct:
        summary["inputTokens"] = summary["inputTokens"] * (100 + SCN.summary_extra_pct) // 100
    group, service = _identity()
    return {"serviceGroup": group, "service": service, "date": date,
            "generatedAt": generated_at(date), **summary}
