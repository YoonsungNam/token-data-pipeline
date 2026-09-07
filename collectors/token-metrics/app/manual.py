"""manual-v0 CSV 로더 (설계 §5.5) — 템플릿 3파일(docs/templates/token_metrics_manual_v0_{gpu,serving,engine}.csv)을
(date, service)별 MetricsPayload로 묶는다. 값 검증(형태 거부·의미 플래그)은 하지 않는다 — API 경로와 동일하게
normalize_payload가 한 곳에서 한다(§5.3). 이 모듈은 '파일이 계약대로 생겼는가'(헤더·컬럼 수·등록 서비스·날짜·metric 키·중복)만 본다.

파서 규칙(Plan 6a F): '#'로 시작하는 줄은 주석(안의 쉼표 무시), 빈 줄 무시, 첫 비주석 줄이 헤더(바이트 동일), 빈 셀 = 부재,
UTF-8(BOM 허용), 날짜 YYYY-MM-DD(KST). 오류 메시지는 '경로:줄번호: 무엇' — 행 원문·서비스명·모델명은 넣지 않는다(§3 전제 11).
"""
from __future__ import annotations

import csv
from datetime import date, timedelta

from app.config import ServiceEntry
from app.normalize import PCT_KEYS, SOURCE_MANUAL, MetricsPayload

GPU_HEADER = "date,service,model,gpuType,category,gpuCount,gpuHours"
SERVING_HEADER = "date,service,model,metric,name,unit,p50,p90,p95,p99"
ENGINE_HEADER = "service,engine_type,engine_version"
STANDARD_METRICS = ("ttftMs", "itlMs", "e2eMs", "outputTps")      # API 키 그대로 — fact metric 변환은 normalize
SERVING_METRICS = STANDARD_METRICS + ("custom",)
COUNT_KEYS = ("rows_gpu", "rows_serving", "rows_engine", "rows_outside_range", "rows_other_service")


class ManualCsvError(ValueError):
    """파일 계약 위반 — 적재 전 전체 거부(main이 stderr + exit 2). 메시지에 행 원문을 담지 않는다."""

    def __init__(self, path: str, lineno: int, what: str):
        super().__init__(f"{path}:{lineno}: {what}")
        self.path = path
        self.lineno = lineno
        self.what = what


def date_range(from_date: str, to_date: str) -> list[str]:
    """--from/--to 포함 범위 (YYYY-MM-DD, KST). 형식 오류는 date.fromisoformat의 ValueError."""
    d0 = date.fromisoformat(from_date)
    d1 = date.fromisoformat(to_date)
    if d1 < d0:
        raise ValueError("--from must not be after --to")
    return [str(d0 + timedelta(days=i)) for i in range((d1 - d0).days + 1)]


def read_csv_rows(path: str, expected_header: str) -> list[tuple[int, dict[str, str]]]:
    """주석·빈 줄을 건너뛰고 헤더(바이트 동일)를 확인한 뒤 (물리 줄 번호, {컬럼: strip 된 셀}) 목록을 돌려준다.
    셀 안 개행은 지원하지 않는다(줄 단위 파싱 — 템플릿 계약 밖)."""
    columns = expected_header.split(",")
    rows: list[tuple[int, dict[str, str]]] = []
    header_seen = False
    with open(path, encoding="utf-8-sig", newline="") as f:
        for lineno, raw in enumerate(f, start=1):
            line = raw.rstrip("\r\n")
            if line.strip() == "" or line.lstrip().startswith("#"):
                continue
            if not header_seen:
                if line != expected_header:
                    raise ManualCsvError(path, lineno, "header mismatch")
                header_seen = True
                continue
            cells = next(csv.reader([line]))
            if len(cells) != len(columns):
                raise ManualCsvError(path, lineno, f"column count {len(cells)} != {len(columns)}")
            rows.append((lineno, {col: cell.strip() for col, cell in zip(columns, cells)}))
    if not header_seen:
        raise ManualCsvError(path, 0, "header missing")
    return rows


def _num(cell: str) -> object:
    """빈 셀 → None(부재), 숫자 → float, 그 외 → 원문 str(normalize _is_num 이 비숫자로 거부)."""
    if cell == "":
        return None
    try:
        return float(cell)
    except ValueError:
        return cell


def _check_row(path: str, lineno: int, row: dict[str, str], registry: dict[str, ServiceEntry]) -> str:
    """등록·날짜 형식 검증(필터와 무관하게 모든 행) — 통과 시 정규 날짜 문자열 반환."""
    if row["service"] not in registry:
        raise ManualCsvError(path, lineno, "unknown service (not in endpoints)")
    raw_date = row["date"]
    try:
        parsed = date.fromisoformat(raw_date)
    except ValueError:
        raise ManualCsvError(path, lineno, "bad date") from None
    if str(parsed) != raw_date:                      # YYYY-MM-DD 만 (3.11+ 의 YYYYMMDD 완화 형식 거부)
        raise ManualCsvError(path, lineno, "bad date")
    return raw_date


def load_manual_csvs(gpu_path: str, serving_path: str, engine_path: str | None,
                     from_date: str, to_date: str, entries: list[ServiceEntry],
                     only_service: str | None, generated_at_raw: str,
                     ) -> tuple[dict[tuple[str, str], MetricsPayload], dict[str, int]]:
    """템플릿 3파일 → {(date, service): MetricsPayload} + 카운트(COUNT_KEYS).

    - 파일 계약 위반(헤더·컬럼 수·미등록 서비스·날짜 형식·metric 키·(model, metric) 중복·engine 서비스 중복)은
      ManualCsvError — 아무것도 적재하지 않는다. 값 검증은 normalize_payload 몫(빈 셀·비숫자는 그대로 전달).
    - 필터: --service 밖 행 → rows_other_service, --from/--to 밖 행 → rows_outside_range (둘 다면 앞 것만).
    - payload 는 채택된 gpu∪serving 행이 1건 이상인 (date, service) 에만 만든다 — 행 없는 (date, service) 는
      키 없음(페이로드·앵커 없음 → 6c metrics_missing 이 '수기 입력 없음'으로 본다).
    """
    registry = {e.service: e for e in entries}
    if only_service is not None and only_service not in registry:
        raise ValueError(f"unknown service: {only_service}")
    dates = date_range(from_date, to_date)
    counts = {k: 0 for k in COUNT_KEYS}
    gpu_by_key: dict[tuple[str, str], list[dict]] = {}
    serving_by_key: dict[tuple[str, str], dict[str, dict]] = {}     # (date, service) → {model: record}

    def _target(row: dict[str, str], day: str) -> bool:
        if only_service is not None and row["service"] != only_service:
            counts["rows_other_service"] += 1
            return False
        if not (dates[0] <= day <= dates[-1]):
            counts["rows_outside_range"] += 1
            return False
        return True

    for lineno, row in read_csv_rows(gpu_path, GPU_HEADER):
        day = _check_row(gpu_path, lineno, row, registry)
        if not _target(row, day):
            continue
        counts["rows_gpu"] += 1
        gpu_by_key.setdefault((day, row["service"]), []).append({
            "model": row["model"], "gpuType": row["gpuType"], "category": row["category"],
            "gpuCount": _num(row["gpuCount"]), "gpuHours": _num(row["gpuHours"]),
        })

    for lineno, row in read_csv_rows(serving_path, SERVING_HEADER):
        day = _check_row(serving_path, lineno, row, registry)
        metric = row["metric"]
        if metric not in SERVING_METRICS:
            raise ManualCsvError(serving_path, lineno, "bad metric")
        if not _target(row, day):
            continue
        counts["rows_serving"] += 1
        records = serving_by_key.setdefault((day, row["service"]), {})
        record = records.setdefault(row["model"], {"model": row["model"]})
        pcts = {p: _num(row[p]) for p in PCT_KEYS if row[p] != ""}     # 빈 p = 키 부재(normalize 가 필수키 판정)
        if metric == "custom":
            record.setdefault("custom", []).append({"name": row["name"], "unit": row["unit"], **pcts})
        else:
            if metric in record:
                raise ManualCsvError(serving_path, lineno, "duplicate (model, metric)")
            record[metric] = pcts                                       # 표준 지표 행의 name/unit 셀은 무시

    engine_map: dict[str, dict] = {}
    if engine_path is not None:
        for lineno, row in read_csv_rows(engine_path, ENGINE_HEADER):
            service = row["service"]
            if service not in registry:
                raise ManualCsvError(engine_path, lineno, "unknown service (not in endpoints)")
            if service in engine_map:
                raise ManualCsvError(engine_path, lineno, "duplicate service")
            engine_map[service] = {"type": row["engine_type"], "version": row["engine_version"]}
            counts["rows_engine"] += 1

    payloads: dict[tuple[str, str], MetricsPayload] = {}
    for day, service in sorted(set(gpu_by_key) | set(serving_by_key)):   # 행이 1건 이상인 (date, service) 만
        entry = registry[service]
        payloads[(day, service)] = MetricsPayload(
            date=day,
            reported_service_group=entry.service_group,      # §5.5 reported_* = 레지스트리 값
            reported_service=entry.service,
            generated_at_raw=generated_at_raw,               # "" = 적재 시각 (normalize, WARN 없음)
            engine=engine_map.get(service),                  # 파일 없음/행 없음 → None → engine_type ''
            gpu=gpu_by_key.get((day, service), []),
            serving=list(serving_by_key.get((day, service), {}).values()),
            source_type=SOURCE_MANUAL,
        )
    return payloads, counts
