"""결정적 합성 사용량 생성 — 난수 상태 없이 sha256 해시만 사용.

같은 (seed, date)는 항상 같은 데이터셋을 반환한다: 계약의 '페이지네이션 도중
데이터셋 불변'과 CI 기대값 고정이 이 성질 하나로 보장된다.
"""
import hashlib
from dataclasses import dataclass
from datetime import date as date_cls, timedelta

from app.config import Config


@dataclass(frozen=True)
class UsageRecord:
    user_id: str | None
    user_type: str
    model: str
    input_tokens: int
    cache_read_tokens: int
    cache_creation_tokens: int
    output_tokens: int
    requests: int


def _det_int(seed: str, *parts: str, lo: int, hi: int) -> int:
    """[lo, hi] 범위의 결정적 정수 (부분 문자열들의 해시)."""
    key = "|".join((seed,) + parts)
    digest = hashlib.sha256(key.encode()).digest()
    return lo + int.from_bytes(digest[:8], "big") % (hi - lo + 1)


def _record(cfg: Config, date: str, user_id: str | None, user_type: str, model: str) -> UsageRecord:
    k = user_id or "unclassified"
    base = _det_int(cfg.seed, date, k, model, "in", lo=1_000, hi=200_000)
    omit_cache = _det_int(cfg.seed, date, k, model, "omitc", lo=0, hi=2) == 0
    return UsageRecord(
        user_id=user_id,
        user_type=user_type,
        model=model,
        input_tokens=base,
        cache_read_tokens=0 if omit_cache else _det_int(cfg.seed, date, k, model, "cr", lo=0, hi=base),
        cache_creation_tokens=0 if omit_cache else _det_int(cfg.seed, date, k, model, "cc", lo=0, hi=base // 4),
        output_tokens=_det_int(cfg.seed, date, k, model, "out", lo=100, hi=base // 2 + 100),
        requests=_det_int(cfg.seed, date, k, model, "req", lo=1, hi=500),
    )


def build_records(cfg: Config, date: str) -> list[UsageRecord]:
    records: list[UsageRecord] = []
    for i in range(cfg.users):
        uid = f"user-{i:04d}"
        for model in cfg.models:
            # 사용자·모델 조합의 약 1/3은 그날 미사용 — 조합 밀도를 결정적으로 낮춤
            if _det_int(cfg.seed, date, uid, model, "use", lo=0, hi=2) == 0:
                continue
            records.append(_record(cfg, date, uid, "identified", model))
    for i in range(cfg.anon_users):
        uid = f"anon-{i:04d}"
        model = cfg.models[_det_int(cfg.seed, date, uid, "pick", lo=0, hi=len(cfg.models) - 1)]
        records.append(_record(cfg, date, uid, "anonymous", model))
    # unclassified: userId null + 모델 단위 합산 행 (첫 모델 1행 + 'unknown' 1행 — 중복 키 방지)
    unclassified_models = list(dict.fromkeys([cfg.models[0], "unknown"]))
    for model in unclassified_models:
        records.append(_record(cfg, date, None, "unclassified", model))
    return records


def build_summary(records: list[UsageRecord]) -> dict:
    return {
        "inputTokens": sum(r.input_tokens for r in records),
        "cacheReadTokens": sum(r.cache_read_tokens for r in records),
        "cacheCreationTokens": sum(r.cache_creation_tokens for r in records),
        "outputTokens": sum(r.output_tokens for r in records),
        "requests": sum(r.requests for r in records),
        "distinctUsers": len({r.user_id for r in records if r.user_id is not None}),
        "distinctIdentifiedUsers": len(
            {r.user_id for r in records if r.user_type == "identified"}
        ),
    }


def to_api_dict(r: UsageRecord) -> dict:
    d = {
        "userId": r.user_id,
        "userType": r.user_type,
        "model": r.model,
        "inputTokens": r.input_tokens,
        "outputTokens": r.output_tokens,
        "requests": r.requests,
    }
    if r.cache_read_tokens > 0:
        d["cacheReadTokens"] = r.cache_read_tokens
    if r.cache_creation_tokens > 0:
        d["cacheCreationTokens"] = r.cache_creation_tokens
    return d


def generated_at(date: str) -> str:
    next_day = date_cls.fromisoformat(date) + timedelta(days=1)
    return f"{next_day.isoformat()}T02:05:00+09:00"
