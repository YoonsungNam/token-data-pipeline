"""3계층 정규화·검증 (설계 §5.3) — DB/HTTP 무접촉 순수 함수. API 응답·manual-v0 CSV 공통 경로 (§5.5).

계층 1 = 스키마 형태 위반 → 거부(rejected 카운트만) / 응답 단위 위반 → PayloadError(호출자가 PERMANENT_ERROR로 번역)
계층 2 = 형태는 맞으나 운영자 검증·단조성 위반 → 적재 + 행 플래그(flags) 또는 응답 WARN(warns)
계층 3 = 교차 행·교차 소스 → mart-metrics(M3)·불변식 — 이 모듈 밖.
숫자 판정은 bool 제외·유한값만. 로깅 계약: 예외 메시지·warns 키에 행 원문을 넣지 않는다(코드·카운트만).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:                       # 순수 모듈 유지 — 런타임 import 없음(pyyaml 불필요)
    from app.config import ServiceEntry

EPS = 1e-6
KST = timezone(timedelta(hours=9))
CATEGORIES = ("serving", "standby", "test")
PCT_KEYS = ("p50", "p90", "p95", "p99")
LATENCY_KEYS = {"ttftMs": "ttft_ms", "itlMs": "itl_ms", "e2eMs": "e2e_ms"}
SERVING_ALLOWED_KEYS = {"model", "ttftMs", "itlMs", "e2eMs", "outputTps", "custom"}
CUSTOM_ALLOWED_KEYS = {"name", "unit"} | set(PCT_KEYS)
REPORT_REQUIRED_KEYS = ("date", "serviceGroup", "service", "generatedAt", "gpu", "serving")
REPORT_KNOWN_KEYS = set(REPORT_REQUIRED_KEYS) | {"engine"}
SOURCE_API = "metrics-api-v1"
SOURCE_MANUAL = "manual-v0"

MAX_MODEL_LEN = 128          # GpuRecord.model / ServingRecord.model maxLength
MAX_GPU_TYPE_LEN = 64        # GpuRecord.gpuType maxLength
MAX_CUSTOM_NAME_LEN = 64     # CustomMetric.name maxLength
MAX_CUSTOM_UNIT_LEN = 32     # CustomMetric.unit maxLength
MAX_ENGINE_LEN = 64          # Engine.type / Engine.version maxLength

# 행 플래그 (fact.flags 어휘 — Plan 6a A)
F_HOURS_OVER = "hours_over_count"
F_UNKNOWN = "unknown_violation"
F_PCT = "pct_non_monotone"
F_DUP_MERGED = "dup_merged"
F_DUP_MODEL = "dup_model_kept_first"
F_DUP_CUSTOM = "dup_custom_kept_first"
GPU_FLAG_ORDER = (F_HOURS_OVER, F_UNKNOWN, F_DUP_MERGED)
SERVING_FLAG_ORDER = (F_PCT, F_UNKNOWN, F_DUP_MODEL, F_DUP_CUSTOM)

# 응답 WARN 코드 (CHECK WARN service=<svc> <code>=<count>)
W_IDENTITY = "identity_drift"
W_GEN_PARSE = "generated_at_parse_failed"
W_GEN_OFFSET = "generated_at_offset_mismatch"
W_ENGINE = "engine_malformed"
W_EXTRA_KEYS = "extra_top_keys"


class PayloadError(ValueError):
    """응답 단위 구조 위반 코드: not_object | missing_keys:<k,..> | date_mismatch | gpu_not_array | serving_not_array."""


@dataclass
class MetricsPayload:
    date: str
    reported_service_group: str
    reported_service: str
    generated_at_raw: str            # ISO 문자열 원문; "" = 적재 시각 사용(manual 기본, WARN 없음)
    engine: object                   # API 원문 (dict | None | 기타)
    gpu: list                        # API 형태 dict 목록 (비배열이면 normalize_payload가 PayloadError)
    serving: list
    source_type: str                 # SOURCE_API | SOURCE_MANUAL
    extra_top_keys: list[str] = field(default_factory=list)


@dataclass
class GpuRow:
    model: str
    gpu_type: str
    category: str
    gpu_count: float
    gpu_hours: float
    flags: list[str]


@dataclass
class ServingRow:
    model: str
    metric: str                      # ttft_ms | itl_ms | e2e_ms | output_tps | custom
    name: str                        # 표준 지표 '' / custom 지표명
    unit: str                        # 'ms' / 'tokens/s' / custom 단위
    p50: float | None
    p90: float | None
    p95: float | None
    p99: float | None
    flags: list[str]


@dataclass
class NormalizeResult:
    generated_at: datetime           # aware KST (필수 — 앞자리: 나머지 필드는 기본값)
    gpu_rows: list[GpuRow] = field(default_factory=list)
    serving_rows: list[ServingRow] = field(default_factory=list)   # 표준 + custom 모두 (long form)
    rejected: int = 0
    merged_dups: int = 0
    warns: dict[str, int] = field(default_factory=dict)            # 행 플래그 카운트 + 응답 WARN (0인 코드는 키 없음)
    engine_type: str = ""
    engine_version: str = ""

    @property
    def n_gpu(self) -> int:
        return len(self.gpu_rows)

    @property
    def n_serving(self) -> int:
        return sum(1 for r in self.serving_rows if r.metric != "custom")

    @property
    def n_custom(self) -> int:
        return sum(1 for r in self.serving_rows if r.metric == "custom")

    @property
    def rows(self) -> int:
        return self.n_gpu + self.n_serving + self.n_custom

    @property
    def warn_total(self) -> int:
        return sum(self.warns.values())

    @property
    def is_nodata(self) -> bool:
        return self.rows == 0 and self.rejected == 0


def _is_num(v: object) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def check_report_structure(body: object, expected_date: str) -> MetricsPayload:
    """응답 단위 구조 검사(§5.3-1) — 위반 시 PayloadError(코드). 통과 시 API 페이로드(source_type=SOURCE_API)."""
    if not isinstance(body, dict):
        raise PayloadError("not_object")
    missing = [k for k in REPORT_REQUIRED_KEYS if k not in body]
    if missing:
        raise PayloadError("missing_keys:" + ",".join(missing))
    if body["date"] != expected_date:
        raise PayloadError("date_mismatch")
    if not isinstance(body["gpu"], list):
        raise PayloadError("gpu_not_array")
    if not isinstance(body["serving"], list):
        raise PayloadError("serving_not_array")
    return MetricsPayload(
        date=expected_date,
        reported_service_group=str(body["serviceGroup"]),
        reported_service=str(body["service"]),
        generated_at_raw=str(body["generatedAt"]),
        engine=body.get("engine"),
        gpu=body["gpu"],
        serving=body["serving"],
        source_type=SOURCE_API,
        extra_top_keys=sorted(set(body) - REPORT_KNOWN_KEYS),
    )


def parse_generated_at(raw: str, now: datetime) -> tuple[datetime, str | None]:
    """generatedAt → aware KST. ''→(now, None) / 파싱 실패·naive→(now, W_GEN_PARSE) / 오프셋≠+09:00→(KST 변환, W_GEN_OFFSET)."""
    s = raw.strip() if isinstance(raw, str) else str(raw).strip()
    if s == "":
        return now, None
    if s.endswith("Z"):                 # 3.10 fromisoformat은 'Z' 미지원
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return now, W_GEN_PARSE
    if dt.tzinfo is None or dt.utcoffset() is None:
        return now, W_GEN_PARSE
    if dt.utcoffset() != timedelta(hours=9):
        return dt.astimezone(KST), W_GEN_OFFSET
    return dt.astimezone(KST), None


def parse_engine(engine: object) -> tuple[str, str, bool]:
    """Engine 블록 → (engine_type, engine_version, malformed). None은 정상 부재('' , '', False)."""
    if engine is None:
        return "", "", False
    if not isinstance(engine, dict):
        return "", "", True
    etype = engine.get("type")
    if not isinstance(etype, str) or not etype.strip() or len(etype) > MAX_ENGINE_LEN:
        return "", "", True
    version = engine.get("version")
    if version is None:
        version = ""
    if not isinstance(version, str) or len(version) > MAX_ENGINE_LEN:
        return "", "", True
    return etype, version, False


def _str_field(obj: dict, key: str, max_len: int) -> str | None:
    """문자열 필드 검사 — 부재·비str·strip 후 빈값·길이 초과면 None. 통과 값은 원문 그대로(정규화는 mart)."""
    v = obj.get(key)
    if not isinstance(v, str) or not v.strip() or len(v) > max_len:
        return None
    return v


def _ordered_flags(flags: set[str], order: tuple[str, ...]) -> list[str]:
    return [f for f in order if f in flags]


def _count_flags(rows: list, counts: dict[str, int], skip: str | None = None) -> None:
    for r in rows:
        for f in r.flags:
            if f == skip:
                continue
            counts[f] = counts.get(f, 0) + 1


def _validate_gpu(raw: object) -> GpuRow | None:
    """계층 1(gpu 행): 형태 위반 → None. 추가 키는 무시(GpuRecord에 additionalProperties 없음)."""
    if not isinstance(raw, dict):
        return None
    model = _str_field(raw, "model", MAX_MODEL_LEN)
    gpu_type = _str_field(raw, "gpuType", MAX_GPU_TYPE_LEN)
    if model is None or gpu_type is None:
        return None
    category = raw.get("category")
    if category not in CATEGORIES:
        return None
    count, hours = raw.get("gpuCount"), raw.get("gpuHours")
    if not _is_num(count) or not _is_num(hours) or count <= 0 or hours < 0:
        return None
    return GpuRow(model=model, gpu_type=gpu_type, category=category,
                  gpu_count=float(count), gpu_hours=float(hours), flags=[])


def normalize_gpu(rows: list) -> tuple[list[GpuRow], int, int, dict[str, int]]:
    """gpu 배열 → (병합 행, rejected, merged_dups, flag_counts).

    계층 2 플래그는 병합 전 원행 기준(hours_over_count·unknown_violation) → 키 (model, gpu_type, category)로
    병합: gpu_hours=SUM, gpu_count=MAX, flags=합집합 + dup_merged. 출력 순서 = 첫 등장 순서.
    flag_counts: hours_over_count·unknown_violation = 플래그가 붙은 출력 행 수, dup_merged = 병합된 원행 수(= merged_dups).
    """
    merged: dict[tuple[str, str, str], GpuRow] = {}
    rejected = 0
    merged_dups = 0
    for raw in rows:
        row = _validate_gpu(raw)
        if row is None:
            rejected += 1
            continue
        flags: set[str] = set()
        if row.gpu_hours > row.gpu_count * 24 + EPS:
            flags.add(F_HOURS_OVER)
        if row.model == "unknown" and row.category in ("serving", "standby"):
            flags.add(F_UNKNOWN)
        key = (row.model, row.gpu_type, row.category)
        prev = merged.get(key)
        if prev is None:
            row.flags = _ordered_flags(flags, GPU_FLAG_ORDER)
            merged[key] = row
            continue
        prev.gpu_hours += row.gpu_hours
        prev.gpu_count = max(prev.gpu_count, row.gpu_count)
        prev.flags = _ordered_flags(set(prev.flags) | flags | {F_DUP_MERGED}, GPU_FLAG_ORDER)
        merged_dups += 1
    out = list(merged.values())
    counts: dict[str, int] = {}
    _count_flags(out, counts, skip=F_DUP_MERGED)
    if merged_dups:
        counts[F_DUP_MERGED] = merged_dups
    return out, rejected, merged_dups, counts


def _pct_block(block: object, keys: tuple[str, ...]) -> dict[str, float] | None:
    """ttftMs/itlMs/e2eMs(키 집합 == p50..p99) · outputTps(키 집합 == p50) — 값은 숫자·≥0. 위반 → None."""
    if not isinstance(block, dict) or set(block) != set(keys):
        return None
    if not all(_is_num(block[k]) and block[k] >= 0 for k in keys):
        return None
    return {k: float(block[k]) for k in keys}


def _custom_item(item: object) -> tuple[str, str, dict[str, float]] | None:
    """CustomMetric: name(≤64)·unit(≤32) 필수, 허용 키 {name, unit, p50..p99}, p키 ≥1, p값 숫자(음수 허용). 위반 → None."""
    if not isinstance(item, dict) or set(item) - CUSTOM_ALLOWED_KEYS:
        return None
    name = _str_field(item, "name", MAX_CUSTOM_NAME_LEN)
    unit = _str_field(item, "unit", MAX_CUSTOM_UNIT_LEN)
    if name is None or unit is None:
        return None
    present = [k for k in PCT_KEYS if k in item]
    if not present or not all(_is_num(item[k]) for k in present):
        return None
    return name, unit, {k: float(item[k]) for k in present}


def _is_non_monotone(p: dict[str, float]) -> bool:
    """존재하는 p값을 p50→p90→p95→p99 순으로 비교, next < prev - EPS 이면 True."""
    prev: float | None = None
    for k in PCT_KEYS:
        if k not in p:
            continue
        if prev is not None and p[k] < prev - EPS:
            return True
        prev = p[k]
    return False


def _expand_record(record: dict) -> tuple[list[ServingRow], int] | None:
    """계층 1(serving 레코드) 검사 + long-form 전개. 위반 → None(레코드 1개 = rejected 1).
    반환 (rows, dup_custom_discarded). 행 순서: ttftMs, itlMs, e2eMs, outputTps, custom.
    """
    if not isinstance(record, dict) or set(record) - SERVING_ALLOWED_KEYS:
        return None
    model = _str_field(record, "model", MAX_MODEL_LEN)
    if model is None:
        return None
    metric_keys = [k for k in ("ttftMs", "itlMs", "e2eMs", "outputTps", "custom") if k in record]
    if not metric_keys:                                  # 지표 0개 (minProperties 2)
        return None
    unknown = model == "unknown"
    rows: list[ServingRow] = []
    dup_custom = 0

    def _row(metric: str, name: str, unit: str, p: dict[str, float]) -> ServingRow:
        flags: set[str] = set()
        if _is_non_monotone(p):
            flags.add(F_PCT)
        if unknown:
            flags.add(F_UNKNOWN)
        return ServingRow(model=model, metric=metric, name=name, unit=unit,
                          p50=p.get("p50"), p90=p.get("p90"), p95=p.get("p95"), p99=p.get("p99"),
                          flags=_ordered_flags(flags, SERVING_FLAG_ORDER))

    for api_key, metric in LATENCY_KEYS.items():
        if api_key in record:
            p = _pct_block(record[api_key], PCT_KEYS)
            if p is None:
                return None
            rows.append(_row(metric, "", "ms", p))
    if "outputTps" in record:
        p = _pct_block(record["outputTps"], ("p50",))
        if p is None:
            return None
        rows.append(_row("output_tps", "", "tokens/s", p))
    if "custom" in record:
        customs = record["custom"]
        if not isinstance(customs, list):
            return None
        seen: dict[str, ServingRow] = {}
        for item in customs:
            parsed = _custom_item(item)
            if parsed is None:
                return None
            name, unit, p = parsed
            first = seen.get(name)
            if first is not None:                        # 같은 name 중복 → 첫 것 유지 + 플래그
                first.flags = _ordered_flags(set(first.flags) | {F_DUP_CUSTOM}, SERVING_FLAG_ORDER)
                dup_custom += 1
                continue
            seen[name] = _row("custom", name, unit, p)
        rows.extend(seen.values())
    return rows, dup_custom


def normalize_serving(records: list) -> tuple[list[ServingRow], int, dict[str, int]]:
    """serving 배열 → (long-form 행, rejected, flag_counts).

    레코드 단위 거부(계층 1)가 중복 판정보다 먼저. 중복 model(2번째 이후 레코드)은 버리고 첫 레코드의 모든 행에
    dup_model_kept_first. flag_counts: pct_non_monotone·unknown_violation = 플래그가 붙은 출력 행 수,
    dup_model_kept_first = 버린 레코드 수, dup_custom_kept_first = 버린 custom 항목 수.
    """
    out: list[ServingRow] = []
    by_model: dict[str, list[ServingRow]] = {}
    rejected = 0
    dup_model = 0
    dup_custom = 0
    for record in records:
        expanded = _expand_record(record)
        if expanded is None:
            rejected += 1
            continue
        rows, n_dup_custom = expanded
        model = record["model"]
        first = by_model.get(model)
        if first is not None:
            for r in first:
                r.flags = _ordered_flags(set(r.flags) | {F_DUP_MODEL}, SERVING_FLAG_ORDER)
            dup_model += 1
            continue
        by_model[model] = rows
        out.extend(rows)
        dup_custom += n_dup_custom
    counts: dict[str, int] = {}
    for r in out:
        for f in r.flags:
            if f in (F_PCT, F_UNKNOWN):
                counts[f] = counts.get(f, 0) + 1
    if dup_model:
        counts[F_DUP_MODEL] = dup_model
    if dup_custom:
        counts[F_DUP_CUSTOM] = dup_custom
    return out, rejected, counts


def normalize_payload(payload: MetricsPayload, entry: ServiceEntry,
                      now: datetime | None = None) -> NormalizeResult:
    """페이로드(API·manual 공통) → NormalizeResult. gpu/serving이 list가 아니면 PayloadError.

    warns = gpu·serving flag_counts + identity_drift(API만: reported_* ≠ 레지스트리 정본)
            + generated_at WARN + engine_malformed + extra_top_keys(개수, >0일 때만).
    """
    if now is None:
        now = datetime.now(KST)
    if not isinstance(payload.gpu, list):
        raise PayloadError("gpu_not_array")
    if not isinstance(payload.serving, list):
        raise PayloadError("serving_not_array")
    gpu_rows, gpu_rejected, merged_dups, gpu_counts = normalize_gpu(payload.gpu)
    serving_rows, serving_rejected, serving_counts = normalize_serving(payload.serving)
    warns: dict[str, int] = {}
    for counts in (gpu_counts, serving_counts):
        for code, n in counts.items():
            warns[code] = warns.get(code, 0) + n
    if payload.source_type == SOURCE_API and \
       (payload.reported_service_group, payload.reported_service) != (entry.service_group, entry.service):
        warns[W_IDENTITY] = 1
    generated_at, gen_warn = parse_generated_at(payload.generated_at_raw, now)
    if gen_warn is not None:
        warns[gen_warn] = 1
    engine_type, engine_version, malformed = parse_engine(payload.engine)
    if malformed:
        warns[W_ENGINE] = 1
    if payload.extra_top_keys:
        warns[W_EXTRA_KEYS] = len(payload.extra_top_keys)
    return NormalizeResult(
        generated_at=generated_at,
        gpu_rows=gpu_rows,
        serving_rows=serving_rows,
        rejected=gpu_rejected + serving_rejected,
        merged_dups=merged_dups,
        warns=warns,
        engine_type=engine_type,
        engine_version=engine_version,
    )
