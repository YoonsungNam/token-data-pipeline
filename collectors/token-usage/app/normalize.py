"""行 정규화·검증·집계 (§5.4) — DB/HTTP 무접촉 순수 함수.

로깅 계약(§5.6): warns 문자열에 user_id 원문·레코드 페이로드를 넣지 않는다.
(서비스명 드리프트 경고의 서비스명은 개인정보가 아니므로 허용)
"""
from dataclasses import dataclass, field

USER_TYPES = ("identified", "anonymous", "unclassified")
TOKEN_FIELDS = (("inputTokens", "input_tokens", True),
                ("cacheReadTokens", "cache_read_tokens", False),
                ("cacheCreationTokens", "cache_creation_tokens", False),
                ("outputTokens", "output_tokens", True),
                ("requests", "requests", True))


@dataclass(frozen=True)
class NormalizedRow:
    user_id: str
    user_type: str
    model: str
    input_tokens: int
    cache_read_tokens: int
    cache_creation_tokens: int
    output_tokens: int
    requests: int


@dataclass
class NormalizeResult:
    rows: list[NormalizedRow] = field(default_factory=list)
    rejected: int = 0
    merged_dups: int = 0
    warns: list[str] = field(default_factory=list)
    totals: dict = field(default_factory=lambda: {
        "input_tokens": 0, "cache_read_tokens": 0, "cache_creation_tokens": 0,
        "output_tokens": 0, "requests": 0})


def _validate(raw: dict) -> NormalizedRow | None:
    user_type = raw.get("userType")
    if user_type not in USER_TYPES:
        return None
    user_id = raw.get("userId")
    if user_type == "unclassified":
        if user_id is not None:
            return None
        user_id = ""                         # null → '' 정규화 (§5.4)
    else:
        if not isinstance(user_id, str) or not user_id:
            return None
    model = raw.get("model")
    if not isinstance(model, str) or not model:
        return None
    values: dict[str, int] = {}
    for api_key, col, required in TOKEN_FIELDS:
        v = raw.get(api_key, None if required else 0)
        if v is None and not required:
            v = 0
        if type(v) is not int or v < 0:      # bool 거부 포함
            return None
        values[col] = v
    return NormalizedRow(user_id=user_id, user_type=user_type, model=model, **values)


def normalize_records(raw_records: list[dict]) -> NormalizeResult:
    out = NormalizeResult()
    merged: dict[tuple, NormalizedRow] = {}
    for i, raw in enumerate(raw_records):
        row = _validate(raw)
        if row is None:
            out.rejected += 1
            continue
        key = (row.user_id, row.user_type, row.model)
        if key in merged:
            prev = merged[key]
            merged[key] = NormalizedRow(
                user_id=row.user_id, user_type=row.user_type, model=row.model,
                input_tokens=prev.input_tokens + row.input_tokens,
                cache_read_tokens=prev.cache_read_tokens + row.cache_read_tokens,
                cache_creation_tokens=prev.cache_creation_tokens + row.cache_creation_tokens,
                output_tokens=prev.output_tokens + row.output_tokens,
                requests=prev.requests + row.requests)
            out.merged_dups += 1
        else:
            merged[key] = row
        for _, col, _req in TOKEN_FIELDS:
            out.totals[col] += getattr(row, col)
    out.rows = list(merged.values())
    if out.rejected:
        out.warns.append(f"rejected_rows={out.rejected} (계약 위반 — 인덱스/사유는 debug 로그 금지 대상 제외)")
    if out.merged_dups:
        out.warns.append(f"merged_duplicate_keys={out.merged_dups} (계약 위반 — SUM 병합, §5.4)")
    return out


def check_identity(entry, payload) -> list[str]:
    warns = []
    if payload.reported_service_group != entry.service_group or \
       payload.reported_service != entry.service:
        warns.append(
            f"identity_drift: reported=({payload.reported_service_group!r}, "
            f"{payload.reported_service!r}) != configured=({entry.service_group!r}, "
            f"{entry.service!r}) (§5.0 — 정본은 설정값)")
    return warns


def check_summary(totals: dict, summary: dict) -> list[str]:
    """Σdetail vs summary (§5.1-3-4). is_derived 소스는 호출하지 않는 것이 계약."""
    warns = []
    pairs = (("inputTokens", "input_tokens"), ("cacheReadTokens", "cache_read_tokens"),
             ("cacheCreationTokens", "cache_creation_tokens"),
             ("outputTokens", "output_tokens"), ("requests", "requests"))
    for api_key, col in pairs:
        reported = summary.get(api_key, 0) or 0
        if reported != totals[col]:
            warns.append(f"summary_mismatch: {api_key} reported={reported} detail_sum={totals[col]}")
    return warns
