"""metrics-api-v1 클라이언트 — HTTP 신호를 공통 이벤트 분류로 번역 (설계 2026-08-31 §5.2 응답 행).

기존 usage-api-v1 수집기 api_client 의 클론(§5.1 — 원본 모듈은 zero-diff, import 없음) — summary 호출·
페이지네이션·`invalid_cursor` 분기를 제거하고 `GET /v1/metrics?date=` **단건 1회**로 축소했다.

번역표 (§5.2):
    409                       → NOT_READY  (retry_after_s = min(Retry-After, 300); 큐 끝 1회 재방문은 main 담당)
    404                       → RETENTION  (정기 FAILURE / rerun SKIPPED 은 main 담당)
    429 / 5xx / 네트워크 예외 → RETRYABLE  (이 계층에서 3회 소진: 5/25/125s, Retry-After 우선, 캡 300s)
    400 / 그 외 비-200        → PERMANENT_ERROR
    200 이지만 본문 > MAX_RESPONSE_BYTES / non-JSON → PERMANENT_ERROR
    200 이지만 필수키 누락 / date 에코 불일치 / gpu·serving 비배열 → PERMANENT_ERROR (normalize.check_report_structure)

세션은 주입받는다(테스트: FakeSession, 운영: main 이 프록시/CA 를 설정한 requests.Session).
로그 출력 없음 — 페이로드·행 원문은 어디에도 남기지 않는다(마커는 main 이 카운트·코드만 출력).
"""
from __future__ import annotations

import time

import requests

from app.config import Config, ServiceEntry
from app.events import CollectError, Event
from app.normalize import MetricsPayload, PayloadError, check_report_structure

RETRY_AFTER_CAP_S = 300          # min(Retry-After, 300s) (§5.2)
RETRYABLE_ATTEMPTS = 3
BACKOFF_S = (5, 25, 125)         # 지수 백오프 (§5.2) — 마지막 시도 뒤에는 대기하지 않으므로 125 는 예비값
HTTP_TIMEOUT_S = 60
METRICS_PATH = "/v1/metrics"     # 계약 @6a552d2 — 단건, 커서 없음


def _capped_retry_after(resp) -> int:
    try:
        return min(int(resp.headers.get("Retry-After", "5")), RETRY_AFTER_CAP_S)
    except (ValueError, TypeError):
        return 5


def _error_code(resp) -> str:
    try:
        return str(resp.json().get("code", ""))
    except Exception:
        return ""


def _translate_error(resp) -> CollectError:
    """비-200 응답 → CollectError (§5.2 metrics-api-v1 번역표). 페이지 재시작 분기 없음(응답 1건)."""
    sc = resp.status_code
    code = _error_code(resp)
    if sc == 409:
        return CollectError(Event.NOT_READY, f"data_not_ready ({code})",
                            retry_after_s=_capped_retry_after(resp))
    if sc == 404:
        return CollectError(Event.RETENTION, f"data_not_retained ({code})")
    if sc == 429 or sc >= 500:
        return CollectError(Event.RETRYABLE, f"http {sc} ({code})",
                            retry_after_s=_capped_retry_after(resp)
                            if "Retry-After" in resp.headers else 0)
    return CollectError(Event.PERMANENT_ERROR, f"http {sc} ({code})")   # 400 포함


def _get_with_retry(session, url: str, params: dict, max_bytes: int) -> object:
    """GET 1회 의미 단위 — RETRYABLE 만 내부 소진(≤3회), 그 외 즉시 번역해 던짐.

    200 은 본문 크기 → JSON 파싱 순으로 검사한다(둘 다 PERMANENT_ERROR, 재시도 없음).
    반환은 파싱된 JSON 값(dict 가 아닐 수 있음 — 구조 판정은 check_report_structure).
    """
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
            cl_header = resp.headers.get("Content-Length")           # 있으면 .content 를 건드리기 전에 먼저 판단
            if cl_header is not None:
                try:
                    cl = int(cl_header)
                except ValueError:
                    cl = None                                        # 비숫자 — 사후 검사로 폴백
                if cl is not None and cl > max_bytes:
                    raise CollectError(Event.PERMANENT_ERROR, f"body too large: {cl} > {max_bytes}")
            n = len(resp.content)
            if n > max_bytes:
                raise CollectError(Event.PERMANENT_ERROR, f"body too large: {n} > {max_bytes}")
            try:
                return resp.json()
            except Exception:
                raise CollectError(Event.PERMANENT_ERROR, "malformed json body (http 200)")
        err = _translate_error(resp)
        if err.event is not Event.RETRYABLE:
            raise err
        last = err
        if attempt < RETRYABLE_ATTEMPTS - 1:
            time.sleep(min(err.retry_after_s or BACKOFF_S[attempt], RETRY_AFTER_CAP_S))
    raise last  # type: ignore[misc]


def fetch_metrics(entry: ServiceEntry, date: str, cfg: Config, session) -> MetricsPayload:
    """(date, service) 스냅샷 1건: GET {base_url}/v1/metrics?date=<date> → 응답 단위 구조 검사.

    페이지 불변성 검사는 없다(응답 1건). 구조 위반(PayloadError)은 PERMANENT_ERROR 로 번역한다 —
    메시지 `report structure: <코드>` 의 코드는 normalize 의 어휘 그대로(not_object / missing_keys:… /
    date_mismatch / gpu_not_array / serving_not_array).
    """
    body = _get_with_retry(session, f"{entry.base_url}{METRICS_PATH}", {"date": date},
                           cfg.max_response_bytes)
    try:
        return check_report_structure(body, date)
    except PayloadError as e:
        raise CollectError(Event.PERMANENT_ERROR, f"report structure: {e}") from e
