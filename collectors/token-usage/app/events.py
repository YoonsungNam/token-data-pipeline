"""공통 이벤트 분류 (스펙 §5.2 분류→정책 표의 분류 축).

정책(대기열·예산·status 매핑·exit 영향)은 main.py 오케스트레이터에 1벌만 존재한다.
api_client는 HTTP 신호를 이 분류로 번역만 한다 — 신규 소스 모듈(§5.9)도 동일 분류 사용.
"""
from enum import Enum


class Event(str, Enum):  # StrEnum은 3.11+ — 3.10 호환 형태 사용
    NOT_READY = "NOT_READY"                # 대기열 후송, 재방문=전체 재시작
    RETRYABLE = "RETRYABLE"                # 내부 재시도 소진 후 FAILURE
    PERMANENT_ERROR = "PERMANENT_ERROR"    # 즉시 FAILURE
    RETENTION = "RETENTION"                # 정기=FAILURE / 재수집=SKIPPED
    EMPTY = "EMPTY"                        # 사용량 0 — NODATA (summary는 적재)
    INVARIANT_BROKEN = "INVARIANT_BROKEN"  # 폐기 후 재시작 ≤2회


class CollectError(Exception):
    def __init__(self, event: Event, message: str, retry_after_s: int = 0):
        super().__init__(f"{event.value}: {message}")
        self.event = event
        self.message = message
        self.retry_after_s = retry_after_s
