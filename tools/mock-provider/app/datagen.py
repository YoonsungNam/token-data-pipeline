"""결정적 합성 사용량 생성 — 난수 상태 없이 sha256 해시만 사용.

같은 (seed, date)는 항상 같은 데이터셋을 반환한다: 계약의 '페이지네이션 도중
데이터셋 불변'과 CI 기대값 고정이 이 성질 하나로 보장된다.
"""
import hashlib
from dataclasses import dataclass
from datetime import date as date_cls, timedelta

from app.config import Config
from app.scenarios import ScenarioState


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


# ---------------------------------------------------------------------------
# /v1/metrics (token-metric-api @6a552d2) — 결정적 GPU Hour·성능 메트릭 생성
# ---------------------------------------------------------------------------
METRICS_ENGINE: dict = {"type": "vllm", "version": "0.10.1"}   # 고정 자기신고 (계약 Engine)
METRICS_GPU_TYPE = "H100"


def _pct(seed: str, date: str, model: str, key: str) -> dict:
    """LatencyPercentiles — p50≤p90≤p95≤p99 단조를 누적합으로 보장 (전부 float)."""
    p50 = _det_int(seed, date, model, key, "p50", lo=50, hi=500)
    p90 = p50 + _det_int(seed, date, model, key, "d90", lo=1, hi=200)
    p95 = p90 + _det_int(seed, date, model, key, "d95", lo=1, hi=100)
    p99 = p95 + _det_int(seed, date, model, key, "d99", lo=1, hi=300)
    return {"p50": float(p50), "p90": float(p90), "p95": float(p95), "p99": float(p99)}


def build_metrics(cfg: Config, date: str, scn: ScenarioState | None = None) -> dict:
    """같은 (seed, date, scn)이면 항상 같은 dict (키 순서 포함) — C4 멱등성·CI 기대치의 근거.

    gpu = 모델당 serving 1행 + 첫 모델 standby 1행 + model="unknown" test 1행 (기본 3모델 → 5행),
    serving = 모델당 1행(ttftMs·itlMs·outputTps{p50}). serviceGroup/service는 cfg 값이며
    호출자(main.get_metrics)가 _identity()로 덮어쓴다. 시나리오 적용 순서:
    dup → hours_over → unknown_serving → pct_non_monotone → empty_gpu → engine_null.
    """
    seed = cfg.seed
    gpu: list[dict] = []
    for model in cfg.models:
        gpu_count = _det_int(seed, date, model, "gc", lo=1, hi=8)
        hours_per_gpu = _det_int(seed, date, model, "gh", lo=6, hi=24)
        gpu.append({"model": model, "gpuType": METRICS_GPU_TYPE, "category": "serving",
                    "gpuCount": gpu_count, "gpuHours": round(gpu_count * hours_per_gpu * 1.0, 1)})
    if cfg.models:
        gpu.append({"model": cfg.models[0], "gpuType": METRICS_GPU_TYPE, "category": "standby",
                    "gpuCount": 1, "gpuHours": 24.0})
    gpu.append({"model": "unknown", "gpuType": METRICS_GPU_TYPE, "category": "test",
                "gpuCount": 1, "gpuHours": float(_det_int(seed, date, "unk", "th", lo=1, hi=12))})
    serving: list[dict] = []
    for model in cfg.models:
        serving.append({
            "model": model,
            "ttftMs": _pct(seed, date, model, "ttft"),
            "itlMs": _pct(seed, date, model, "itl"),
            "outputTps": {"p50": float(_det_int(seed, date, model, "tps", lo=5, hi=200))},
        })
    engine: dict | None = dict(METRICS_ENGINE)

    if scn is not None:
        if scn.metrics_dup_gpu_rows and gpu:
            gpu.insert(1, dict(gpu[0]))                       # 첫 행 복제 — 인접 중복 (dup_merged)
        if scn.metrics_gpu_hours_over and gpu:
            gpu[0]["gpuHours"] = float(gpu[0]["gpuCount"] * 24 + 10)   # hours_over_count
        if scn.metrics_unknown_serving:
            gpu.append({"model": "unknown", "gpuType": METRICS_GPU_TYPE, "category": "serving",
                        "gpuCount": 1, "gpuHours": 24.0})     # unknown_violation
        if scn.metrics_pct_non_monotone and serving:
            serving[0]["ttftMs"]["p90"] = serving[0]["ttftMs"]["p50"] - 1   # pct_non_monotone
        if scn.metrics_empty_gpu:
            gpu = []                                          # 케이스 E
        if scn.metrics_engine_null:
            engine = None

    return {
        "date": date,
        "serviceGroup": cfg.service_group,
        "service": cfg.service,
        "generatedAt": generated_at(date),
        "engine": engine,
        "gpu": gpu,
        "serving": serving,
    }
