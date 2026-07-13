import pytest

from app.cursors import CursorError, decode_cursor, encode_cursor


def test_roundtrip():
    c = encode_cursor(1000, "2026-06-15", 500)
    assert decode_cursor(c, "2026-06-15", 500) == 1000


def test_malformed_cursor_rejected():
    for bad in ("not-base64!!!", "aGVsbG8=", ""):
        with pytest.raises(CursorError):
            decode_cursor(bad, "2026-06-15", 500)


def test_date_or_limit_mismatch_rejected():
    c = encode_cursor(1000, "2026-06-15", 500)
    with pytest.raises(CursorError):
        decode_cursor(c, "2026-06-16", 500)
    with pytest.raises(CursorError):
        decode_cursor(c, "2026-06-15", 1000)


def test_negative_offset_rejected():
    c = encode_cursor(-1, "2026-06-15", 500)
    with pytest.raises(CursorError):
        decode_cursor(c, "2026-06-15", 500)


def test_non_int_offset_rejected():
    import base64
    import json

    for bad_offset in (True, False, "10", 10.5, None):
        payload = json.dumps({"o": bad_offset, "d": "2026-06-15", "l": 500})
        cursor = base64.urlsafe_b64encode(payload.encode()).decode()
        with pytest.raises(CursorError):
            decode_cursor(cursor, "2026-06-15", 500)


def test_missing_key_rejected():
    import base64
    import json

    payload = json.dumps({"o": 10, "d": "2026-06-15"})  # "l" 누락
    cursor = base64.urlsafe_b64encode(payload.encode()).decode()
    with pytest.raises(CursorError):
        decode_cursor(cursor, "2026-06-15", 500)


def test_zero_offset_roundtrip():
    c = encode_cursor(0, "2026-06-15", 500)
    assert decode_cursor(c, "2026-06-15", 500) == 0
