import base64
import binascii
import json


class CursorError(ValueError):
    pass


def encode_cursor(offset: int, date: str, limit: int) -> str:
    payload = json.dumps({"o": offset, "d": date, "l": limit}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode()).decode()


def decode_cursor(cursor: str, date: str, limit: int) -> int:
    try:
        data = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        offset, c_date, c_limit = data["o"], data["d"], data["l"]
    except (binascii.Error, UnicodeDecodeError, ValueError, KeyError, TypeError) as exc:
        raise CursorError("cursor is malformed; restart pagination without cursor") from exc
    if type(offset) is not int or offset < 0:
        raise CursorError("cursor is malformed; restart pagination without cursor")
    if c_date != date or c_limit != limit:
        raise CursorError("date/limit must match the first call of this pagination")
    return offset
