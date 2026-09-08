"""공통 이벤트 분류 (설계 2026-08-31 §5.2 모드×게이트 표의 분류 축 — token-metrics 클론).

정책(대기열·재방문·final 판정·status 매핑·exit 영향)은 main.py 오케스트레이터에 1벌만 존재한다.
api_client 는 HTTP 신호를 이 분류로 번역만 한다. 값은 소문자 — main 이 `FAILURE reason=<value>` 로
마커 reason 어휘(not_ready · retention · permanent_error …)에 그대로 쓴다.
"""
from enum import Enum


class Event(str, Enum):  # StrEnum 은 3.11+ — 3.10 호환 형태 사용
    NOT_READY = "not_ready"                # 409: 큐 끝 1회 재방문, 재차 409 → 비최종 SKIPPED / 최종 FAILURE
    RETRYABLE = "retryable"                # 429/5xx/네트워크: 내부 재시도 3회 소진 후 FAILURE
    PERMANENT_ERROR = "permanent_error"    # 400 / >5MB / date 에코 불일치 / non-JSON / 구조 위반: 즉시 FAILURE
    RETENTION = "retention"                # 404: 정기 FAILURE / rerun SKIPPED
    EMPTY = "empty"                        # gpu:[] AND serving:[] — NODATA (summary 앵커는 적재)
    INVARIANT_BROKEN = "invariant_broken"  # 적재 중 불변식 위반 — 폐기 후 재시작


class CollectError(Exception):
    def __init__(self, event: Event, message: str = "", retry_after_s: int = 0):
        super().__init__(f"{event.value}: {message}")
        self.event = event
        self.message = message
        self.retry_after_s = retry_after_s
