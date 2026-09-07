import pytest

from app.events import CollectError, Event


def test_event_values():
    # T6 이 FAILURE reason=<event.value> 로 마커에 그대로 쓴다 — 소문자 어휘 고정
    assert Event.NOT_READY.value == "not_ready"
    assert Event.RETRYABLE.value == "retryable"
    assert Event.PERMANENT_ERROR.value == "permanent_error"
    assert Event.RETENTION.value == "retention"
    assert Event.EMPTY.value == "empty"
    assert Event.INVARIANT_BROKEN.value == "invariant_broken"
    assert len(Event) == 6
    assert isinstance(Event.NOT_READY, str)          # str 혼합 Enum (StrEnum 미사용 — 3.10 호환)
    assert Event("retention") is Event.RETENTION


def test_collect_error_defaults():
    err = CollectError(Event.RETRYABLE)
    assert err.event is Event.RETRYABLE
    assert err.message == ""
    assert err.retry_after_s == 0
    assert "retryable" in str(err)
    assert isinstance(err, Exception)


def test_collect_error_carries_message_and_retry_after():
    err = CollectError(Event.NOT_READY, "data_not_ready", retry_after_s=900)
    assert err.retry_after_s == 900
    assert str(err) == "not_ready: data_not_ready"
    with pytest.raises(CollectError) as ei:
        raise err
    assert ei.value.event is Event.NOT_READY
