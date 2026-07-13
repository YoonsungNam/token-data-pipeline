"""usage-api-v1 클라이언트 — HTTP 신호를 공통 이벤트 분류로 번역 (§5.2 번역표).

세션은 주입받는다(테스트: Fake, 운영: main이 프록시/CA 설정한 requests.Session).
RETRYABLE(429/5xx/네트워크)은 이 계층에서 최대 3회 소진한다. NOT_READY/INVARIANT_BROKEN의
재방문·재시작 정책은 main의 큐가 담당한다 — 여기서는 즉시 던진다.
"""
import time
from dataclasses import dataclass, field

import requests

from app.config import Config, ServiceEntry
from app.events import CollectError, Event

RETRY_AFTER_CAP_S = 300          # min(Retry-After, 300s) (§5.2)
RETRYABLE_ATTEMPTS = 3
BACKOFF_S = (5, 25, 125)         # 지수 백오프 (§5.2)
HTTP_TIMEOUT_S = 60
PAGE_LIMIT = 1000                # 계약 기본값 — 상향은 §9-6 협의


@dataclass
class UsagePayload:
    records: list[dict] = field(default_factory=list)
    summary: dict | None = None
    generated_at: str = ""
    reported_service_group: str = ""
    reported_service: str = ""
    pages: int = 0


def _capped_retry_after(resp) -> int:
    try:
        return min(int(resp.headers.get("Retry-After", "5")), RETRY_AFTER_CAP_S)
    except ValueError:
        return 5


def _error_code(resp) -> str:
    try:
        return str(resp.json().get("code", ""))
    except Exception:
        return ""


def _translate_error(resp) -> CollectError:
    """비-200 응답 → CollectError (§5.2 usage-api-v1 번역표)."""
    sc = resp.status_code
    code = _error_code(resp)
    if sc == 409:
        return CollectError(Event.NOT_READY, f"data_not_ready ({code})",
                            retry_after_s=_capped_retry_after(resp))
    if sc == 404:
        return CollectError(Event.RETENTION, f"data_not_retained ({code})")
    if sc in (429, 500, 503) or sc >= 500:
        return CollectError(Event.RETRYABLE, f"http {sc} ({code})",
                            retry_after_s=_capped_retry_after(resp))
    if sc == 400 and code == "invalid_cursor":
        # §5.2: cursor 없이 처음부터 재시작 — INVARIANT_BROKEN 경로(재시작 ≤2회)로 위임
        return CollectError(Event.INVARIANT_BROKEN, "invalid_cursor — restart pagination")
    return CollectError(Event.PERMANENT_ERROR, f"http {sc} ({code})")


def _get_with_retry(session, url: str, params: dict) -> dict:
    """GET 1회 의미 단위 — RETRYABLE만 내부 소진(≤3회), 그 외 즉시 번역해 던짐."""
    last: CollectError | None = None
    for attempt in range(RETRYABLE_ATTEMPTS):
        try:
            resp = session.get(url, params=params, timeout=HTTP_TIMEOUT_S)
        except requests.RequestException as exc:
            last = CollectError(Event.RETRYABLE, f"network: {type(exc).__name__}")
            if attempt < RETRYABLE_ATTEMPTS - 1:
                time.sleep(BACKOFF_S[attempt])
            continue
        if resp.status_code == 200:
            return resp.json()
        err = _translate_error(resp)
        if err.event is not Event.RETRYABLE:
            raise err
        last = err
        if attempt < RETRYABLE_ATTEMPTS - 1:
            time.sleep(min(err.retry_after_s or BACKOFF_S[attempt], RETRY_AFTER_CAP_S))
    raise last  # type: ignore[misc]


def fetch_service(entry: ServiceEntry, date: str, cfg: Config, session) -> UsagePayload:
    """(date, service) 원자 스냅샷 수집: summary → detail 전 페이지.

    페이지 불변성(§5.3): 매 페이지 (serviceGroup, service, date, generatedAt)이
    첫 페이지와 다르면 INVARIANT_BROKEN. 저장 generated_at = 마지막 페이지 값
    (불변성 검사를 통과했으므로 첫 페이지 값과 동일).
    """
    summary = _get_with_retry(session, f"{entry.base_url}/v1/usage/summary", {"date": date})

    out = UsagePayload(summary=summary)
    cursor: str | None = None
    anchor: tuple | None = None
    while True:
        if out.pages >= cfg.max_pages:
            raise CollectError(Event.PERMANENT_ERROR,
                               f"MAX_PAGES({cfg.max_pages}) exceeded — 부분 적재 금지 (§5.2)")
        params: dict = {"date": date, "limit": PAGE_LIMIT}
        if cursor is not None:
            params["cursor"] = cursor
        body = _get_with_retry(session, f"{entry.base_url}/v1/usage", params)
        out.pages += 1
        meta = (body.get("serviceGroup"), body.get("service"),
                body.get("date"), body.get("generatedAt"))
        if anchor is None:
            anchor = meta
            out.reported_service_group = str(body.get("serviceGroup", ""))
            out.reported_service = str(body.get("service", ""))
        elif meta != anchor:
            raise CollectError(Event.INVARIANT_BROKEN,
                               f"page meta changed at page {out.pages}: {anchor} -> {meta}")
        out.records.extend(body.get("records") or [])
        out.generated_at = str(body.get("generatedAt", ""))
        cursor = body.get("nextCursor")
        if cursor is None:
            return out
