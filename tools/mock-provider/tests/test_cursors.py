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
