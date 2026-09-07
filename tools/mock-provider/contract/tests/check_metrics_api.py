#!/usr/bin/env python3
"""
check_metrics_api.py — GET /v1/metrics 구현 자가 검사 스크립트 (서비스 담당자용)

사용법:
    python3 check_metrics_api.py --base-url http://my-service.internal:8080
    python3 check_metrics_api.py --base-url http://... --date 2026-08-18
    python3 check_metrics_api.py --base-url http://... --skip-behavior

의존성: 파이썬 3.9+ 표준 라이브러리만 사용 (추가 설치 불필요).
스펙: docs/METRICS_COLLECTION_SPEC.md · token-metric-api.yaml
"""

import argparse
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
PCT_KEYS = ["p50", "p90", "p95", "p99"]
CATEGORIES = {"serving", "standby", "test"}
EPS = 1e-6

results = []       # (level, check_id, message)
fixes_needed = {}  # fix_key -> "FAIL" | "WARN" (FAIL 우선, 삽입 순서 유지)

# ── 고치는 방법 상세 가이드 ─────────────────────────────────────────────
FIX_GUIDES = {
    "conn": """▍연결 실패
  문제: 서비스에 HTTP 요청이 도달하지 못했다 (DNS·방화벽·포트·타임아웃).
  고치는 법:
    1. --base-url 의 호스트·포트가 맞는지, 서비스가 떠 있는지 확인
    2. 같은 망에서 curl "{base}/v1/metrics?date=..." 로 재현
    3. 운영자측 수집기 대역까지 방화벽이 열려 있는지 확인""",

    "json": """▍응답이 JSON 객체가 아님
  문제: 200 본문이 JSON 객체로 파싱되지 않는다 (HTML 오류 페이지·문자열 등).
  고치는 법: JSON 객체를 직렬화해 반환하고 Content-Type: application/json 설정.
  올바른 형태: {"date": "...", "serviceGroup": "...", "service": "...", "generatedAt": "...", "gpu": [...], "serving": []}""",

    "required-fields": """▍최상위 필수 필드 (B1)
  규칙: date · serviceGroup · service · generatedAt · gpu · serving 6개는 **항상** 포함한다.
    - serving 은 항상 포함 — GPU 서빙 팀(케이스 A~D·F)은 모델별 행을 채우고, 사외 API 전용·플랫폼 소비 전용만 빈 배열 [] (키 생략·null 불가)
    - serviceGroup·service 는 메타데이터 시트 공식 표기와 **같은 문자열** (기존 토큰 API 와도 동일 — JOIN 조건)
  올바른 예: {"date":"2026-08-18","serviceGroup":"ds-assistant","service":"ds-assistant-portal",
             "generatedAt":"2026-08-19T01:30:00+09:00","gpu":[...],"serving":[]}""",

    "date-echo": """▍date 에코 (B2)
  규칙: 요청 쿼리의 date 값을 본문 date 에 그대로 반환한다.
  확인: 서버의 "오늘" 이 아니라 **요청받은 일자**의 집계를 반환하는지, date 파싱 후 재포맷 과정에서
        시간대가 끼어 하루가 밀리지 않는지 (모든 일자는 KST 기준).""",

    "generated-at": """▍generatedAt 형식 (B3)
  규칙: 집계 산출 시각을 ISO 8601 + KST 오프셋(+09:00)으로. UTC 'Z' 표기·오프셋 생략 금지.
  올바른 예: "2026-08-19T01:30:00+09:00"
  파이썬: datetime.now(timezone(timedelta(hours=9))).isoformat(timespec="seconds")""",

    "gpu-structure": """▍gpu 행 구조 (B4)
  규칙: gpu 는 행 배열, 각 행은 model × gpuType × category 단위 객체로 5개 필드 필수.
  올바른 예: {"model":"llama3.3-70b","gpuType":"H100","gpuCount":4,"gpuHours":96.0,"category":"serving"}
    - model: canonical 표기(문자열) / gpuType: 단순 기종 표기(H100, A100)
    - gpuCount·gpuHours: 숫자 (문자열 "4" 불가)""",

    "category": """▍category 값 (B5)
  규칙: "serving"(프로덕션) / "standby"(HA 대기) / "test"(실험) 3개 문자열만 허용.
  유휴는 보내지 않는다 — 운영자가 [할당(고정 쿼터) − Σ제공]으로 산출한다.""",

    "unknown": """▍serving/standby 의 model "unknown" 금지 (B6)
  규칙: 프로덕션(serving·standby) 행의 model 은 canonical 표기여야 한다.
  고치는 법:
    - 모델을 알지만 표기가 없다 → 메타데이터 시트에 canonical + alias 등록 후 그 표기 사용
    - 실험용 GPU 라 모델 귀속이 무의미하다 → category 를 "test" 로 (test 만 unknown 허용)""",

    "count-hours": """▍gpuCount·gpuHours 규칙 (B7)
  규칙: gpuCount = 그날 그 행에 매핑된 **최대 장수** (> 0)
        gpuHours = 장수 × 매핑·할당 시간의 적분 (≥ 0), 행별 gpuHours ≤ gpuCount × 24
  자주 하는 실수:
    - gpuCount 를 평균 장수로 기입 (최대 기준이어야 함)
    - gpuHours 에 분·초 단위 값이나 다른 날짜·기종 분이 섞임
  올바른 예: 2장으로 12h + 증설 후 4장으로 12h → gpuCount 4, gpuHours 72.0 (= 2×12 + 4×12)""",

    "dup-row": """▍gpu 행 중복 (B8)
  규칙: 같은 (model, gpuType, category)는 **한 행으로 합산**한다.
  레플리카별·시간대별로 행을 쪼개지 말 것 — 행이 나뉘는 경우는 기종 변경·모델 교체(시간 분할)뿐.""",

    "no-serving-row": """▍serving 행 부재 (B8)
  standby/test 행만 보고 중이다. 프로덕션 서빙 GPU 가 있다면 serving 행이 누락된 것 —
  효율 지표(원/1M토큰)는 serving 행으로만 계산되므로 누락 시 서비스가 비효율로 보인다.""",

    "percentiles": """▍percentile 객체 규칙 (B9)
  규칙: ttftMs / itlMs / e2eMs 는 {p50, p90, p95, p99} 4키 완비, 각각 0 이상의 숫자,
        p50 ≤ p90 ≤ p95 ≤ p99 (비감소).
  비감소 위반의 전형적 원인: **레플리카별 percentile 을 평균·재조합** — percentile 은 합성이 안 된다.
  고치는 법: 전 레플리카의 요청 로그를 모아 **한 번에** percentile 을 계산한다 (레플리카별 p99 의 평균 ≠ 전체 p99).""",

    "tps": """▍outputTps 규칙 (B9)
  규칙: outputTps 는 {"p50": 값} 만 — avg·p90+ 금지.
    - avg 금지: 요청별 비율(TPS)의 평균은 통계적으로 왜곡. 총 처리량이 필요하면 토큰 API 에서 파생 (이중 소스 금지)
    - 상위 percentile 금지: TPS 의 나쁜 꼬리는 낮은 쪽이며 ITL p99 가 같은 정보를 담는다
  올바른 예: "outputTps": {"p50": 35.2}""",

    "custom": """▍custom 지표 규칙 (B9)
  규칙: 각 항목은 {"name": 지표명, "unit": 단위, p50/p90/p95/p99 중 최소 1개}.
    - name·unit 필수 (unit 없는 숫자는 단위 착오를 유발)
    - 값 키는 p50/p90/p95/p99 만 허용 — 단일값 지표는 p50 하나로 기입
    - 표준 지표(TTFT/ITL/TPS/E2E)로 표현 가능한 값은 custom 에 넣지 않는다
  올바른 예: {"name": "queueWaitMs", "unit": "ms", "p50": 120, "p99": 900}""",

    "serving-structure": """▍serving 블록 구조 (B9)
  규칙: serving 은 배열이다 — 자체 GPU 서빙 팀은 모델별 행을 채우고, 사외 API 전용·플랫폼 소비 전용만 빈 배열 []. null·키 생략 불가.
  각 행은 모델당 1행: {"model": canonical, ttftMs/itlMs/outputTps/e2eMs/custom 중 해당 지표}.
    - model 은 canonical 표기 필수 ("unknown" 불가 — 성능은 모델 단위가 정본)
    - 각 행의 지표는 **그 모델로 처리된 요청만**의 분포로 계산 (모델 간 표본 혼합 금지)""",

    "no-metrics": """▍serving 행의 지표 구성 (B10)
  규칙: 행마다 표준 지표 최소 1개 —
    - 스트리밍(케이스 A~D): ttftMs + itlMs (쌍으로) + outputTps — A~C는 엔진 히스토그램에서 산출 가능
    - 비스트리밍(케이스 F): e2eMs 필수, custom 은 보조 (custom 만으로는 부족)
  측정법(케이스 D): 요청 수신·첫 청크·마지막 청크 시각 로깅 → TTFT = 첫 청크 − 수신,
    ITL = 청크 간 간격(간격당 1표본), TPS = 출력 토큰 ÷ (마지막 − 첫 청크). 상세: 스펙 §4 작성 가이드.""",

    "engine": """▍engine 자기신고 (B11)
  권장 형식: "engine": {"type": "vllm", "version": "0.8.4"}
  넣으면 좋은 이유: 버전을 API 로 자기신고하면 메타데이터 시트 수기 갱신이 사라지고,
  엔진 업그레이드로 인한 성능 변화(예: "지난주부터 ITL 상승")를 운영자가 추적할 수 있다.""",

    "extra-fields": """▍스펙에 없는 최상위 필드 (B12)
  스펙 외 필드는 제거한다. 특히 **토큰량·requests 는 보내지 않는다** —
  그건 기존 토큰 API(/v1/usage) 담당이며, 두 곳에서 보내면 이중 계상된다 (이중 소스 금지).""",

    "error-body": """▍오류 응답 본문 형식
  4xx 응답 본문은 {"code": "...", "message": "..."} JSON 권장 (token-metric-api.yaml Error 스키마).
  예: 400 → {"code": "invalid_date", "message": "date must be a past date in YYYY-MM-DD (KST)"}""",

    "content-type": """▍Content-Type 헤더
  200 응답에 Content-Type: application/json 을 설정한다.""",

    "status-code": """▍상태 코드 규칙 (§2 응답 규칙)
  /v1/metrics 가 쓰는 코드는 4개뿐:
    200 확정 데이터 (실제 0 이면 200 + gpu: []) · 409 집계 전 미확정 · 400 당일/미래/형식 오류 · 404 보존(14일) 초과
  과거 유효 날짜에 400 이 나오면: date 파서가 KST(+09:00) 기준인지, 타임존 차이로 하루 밀리지 않는지 확인.""",

    "today-future-400": """▍당일·미래 date → 400 (C1/C2)
  규칙: 요청 date ≥ 오늘(KST) 이면 400 을 반환한다.
  이유: 아직 확정 전인 날을 200(빈 값)으로 주면 "실제 사용량 0"과 구분이 안 돼 대시보드에 가짜 0 이 적재된다.
  구현: if request_date >= today_kst: return 400 {"code": "invalid_date", ...}
        오늘 판정은 반드시 KST(+09:00) 기준 — 서버가 UTC 면 하루 밀린다.""",

    "retention-404": """▍보존 기간 초과 → 404 (C3)
  규칙: 최근 14일 밖의 date 는 404 {"code": "data_not_retained", ...}.
  14일 이내는 재수집(backfill) 대상이므로 조회 가능해야 한다 — 14일보다 짧게 지우면 정정 재수집이 실패한다.""",

    "idempotency": """▍같은 date 재호출 = 같은 응답 (C4)
  규칙: 확정된 일자는 언제 다시 호출해도 같은 본문을 반환한다 (운영자 재수집 안전의 전제).
  위반의 전형적 원인: 호출 때마다 재집계 / 응답에 현재 시각·난수가 섞임 / 확정 전 데이터를 200 으로 응답.
  구현: 일자 확정 시점에 결과를 저장(테이블·스냅샷)하고 이후엔 저장본만 반환.
        정정이 필요하면 재집계 후 저장본을 교체하고 운영자에게 재수집을 요청한다.""",

    "serving-empty": """▍GPU 서빙 중인데 성능 행 없음 (B10)
  규칙: gpu 블록에 serving 행이 있으면 그 모델들의 성능 행을 serving 배열에 채운다 —
  vLLM/SGLang(케이스 A~C)도 자체 집계 대상이다 (운영자는 개방된 엔진 /metrics 로 교차 검증만 한다).
  산출 힌트(A~C): 엔진 히스토그램의 그날 증가분 버킷에서 percentile 계산 —
    histogram_quantile(0.99, sum by (le, model_name) (increase(vllm:time_to_first_token_seconds_bucket[1d])))
  사내 플랫폼에서 소비만 하는 모델은 행을 넣지 않는다 (성능은 플랫폼이 제공).
  예외: 그날 요청이 0건이었던 모델은 성능 행을 생략할 수 있다 — 전 모델 0건이면 이 FAIL 은 무시해도 된다.""",

    "date-format-400": """▍date 형식 검증 → 400 (C5)
  규칙: date 파라미터를 YYYY-MM-DD 로 파싱 검증하고 실패 시 400 (2026-13-99 같은 값 거부).
  구현: datetime.strptime(date, "%Y-%m-%d") 수준의 검증이면 충분.""",
}


def report(level, check_id, message, fix=None):
    results.append((level, check_id, message))
    if fix and level in ("FAIL", "WARN") and fixes_needed.get(fix) != "FAIL":
        fixes_needed[fix] = level
    mark = {"PASS": "\033[32m[PASS]\033[0m", "WARN": "\033[33m[WARN]\033[0m",
            "FAIL": "\033[31m[FAIL]\033[0m", "INFO": "[INFO]"}[level]
    print(f"{mark} {check_id:<6} {message}")


def http_get(base_url, date, timeout):
    """(status_code, headers, body_text) 반환. 연결 실패 시 (None, None, 오류문자열)."""
    url = f"{base_url.rstrip('/')}/v1/metrics?{urllib.parse.urlencode({'date': date})}"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            headers = {k.lower(): v for k, v in resp.headers.items()}
            return resp.status, headers, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        headers = {k.lower(): v for k, v in e.headers.items()}
        return e.code, headers, e.read().decode("utf-8", "replace")
    except Exception as e:  # 연결 자체가 실패 (DNS·거부·타임아웃)
        return None, None, f"{type(e).__name__}: {e}"


def check_error_body(check_id, body):
    """4xx 응답 본문이 {code, message} 형식인지 (token-metric-api.yaml Error 스키마)."""
    try:
        doc = json.loads(body)
        if isinstance(doc, dict) and isinstance(doc.get("code"), str) and isinstance(doc.get("message"), str):
            report("PASS", check_id, f"오류 본문 형식 OK (code={doc['code']})")
        else:
            report("WARN", check_id, "오류 본문에 code/message(string)가 없음 — Error 스키마 권장", fix="error-body")
    except (json.JSONDecodeError, ValueError):
        report("WARN", check_id, "오류 본문이 JSON이 아님 — Error 스키마 {code, message} 권장", fix="error-body")


def check_percentiles(path, obj, monotonic=True):
    """LatencyPercentiles: p50/p90/p95/p99 4키 완비·숫자·비감소, 그 외 키 금지."""
    ok = True
    if not isinstance(obj, dict):
        report("FAIL", "B9", f"{path} 가 객체가 아님", fix="percentiles")
        return False
    extra = set(obj) - set(PCT_KEYS)
    if extra:
        report("FAIL", "B9", f"{path} 에 허용되지 않은 키 {sorted(extra)} — p50/p90/p95/p99만 허용", fix="percentiles")
        ok = False
    missing = [k for k in PCT_KEYS if k not in obj]
    if missing:
        report("FAIL", "B9", f"{path} 에 {missing} 누락 — 4개 percentile 모두 필수", fix="percentiles")
        return False
    vals = []
    for k in PCT_KEYS:
        v = obj[k]
        if not isinstance(v, (int, float)) or isinstance(v, bool) or v < 0:
            report("FAIL", "B9", f"{path}.{k} 는 0 이상의 숫자여야 함 (현재: {v!r})", fix="percentiles")
            ok = False
        else:
            vals.append(v)
    if monotonic and len(vals) == 4 and any(vals[i] > vals[i + 1] + EPS for i in range(3)):
        report("FAIL", "B9", f"{path} percentile 이 비감소가 아님 (p50≤p90≤p95≤p99 여야 함): {vals}", fix="percentiles")
        ok = False
    return ok


def check_gpu_block(gpu):
    if not isinstance(gpu, list):
        report("FAIL", "B4", "gpu 는 배열이어야 함", fix="gpu-structure")
        return
    report("PASS", "B4", f"gpu 배열 확인 ({len(gpu)}행)")
    seen = {}
    for i, row in enumerate(gpu):
        p = f"gpu[{i}]"
        if not isinstance(row, dict):
            report("FAIL", "B4", f"{p} 가 객체가 아님", fix="gpu-structure")
            continue
        missing = [k for k in ("model", "gpuType", "gpuCount", "gpuHours", "category") if k not in row]
        if missing:
            report("FAIL", "B4", f"{p} 필수 필드 누락: {missing}", fix="gpu-structure")
            continue
        model, gtype, cat = row["model"], row["gpuType"], row["category"]
        cnt, hrs = row["gpuCount"], row["gpuHours"]
        if not isinstance(model, str) or not model:
            report("FAIL", "B4", f"{p}.model 은 비어있지 않은 문자열이어야 함", fix="gpu-structure")
        if not isinstance(gtype, str) or not gtype:
            report("FAIL", "B4", f"{p}.gpuType 은 비어있지 않은 문자열이어야 함", fix="gpu-structure")
        if not isinstance(cat, str) or cat not in CATEGORIES:
            report("FAIL", "B5", f"{p}.category={cat!r} — serving|standby|test 만 허용", fix="category")
        elif cat in ("serving", "standby") and model == "unknown":
            report("FAIL", "B6", f"{p}: category={cat} 에서 model \"unknown\" 금지 (test 만 허용)", fix="unknown")
        if not isinstance(cnt, (int, float)) or isinstance(cnt, bool) or cnt <= 0:
            report("FAIL", "B7", f"{p}.gpuCount 는 0보다 큰 숫자여야 함 (현재: {cnt!r})", fix="count-hours")
        if not isinstance(hrs, (int, float)) or isinstance(hrs, bool) or hrs < 0:
            report("FAIL", "B7", f"{p}.gpuHours 는 0 이상의 숫자여야 함 (현재: {hrs!r})", fix="count-hours")
        elif isinstance(cnt, (int, float)) and not isinstance(cnt, bool) and cnt > 0 and hrs > cnt * 24 + EPS:
            report("FAIL", "B7", f"{p}: gpuHours({hrs}) > gpuCount×24({cnt * 24}) — 검증 규칙 위반", fix="count-hours")
        key = (str(model), str(gtype), str(cat))
        if key in seen:
            report("WARN", "B8", f"{p}: 동일 (model, gpuType, category) 행 중복 — gpu[{seen[key]}] 와 합쳐서 한 행으로 제공 권장", fix="dup-row")
        seen[key] = i
    if not any(isinstance(r, dict) and r.get("category") == "serving" for r in gpu) and gpu:
        report("WARN", "B8", "serving 행이 없음 — standby/test 만 제공 중인지 확인", fix="no-serving-row")


def check_custom(path, arr):
    if not isinstance(arr, list):
        report("FAIL", "B9", f"{path} 는 배열이어야 함", fix="custom")
        return
    for j, m in enumerate(arr):
        p = f"{path}[{j}]"
        if not isinstance(m, dict):
            report("FAIL", "B9", f"{p} 가 객체가 아님", fix="custom")
            continue
        if not isinstance(m.get("name"), str) or not m.get("name"):
            report("FAIL", "B9", f"{p}.name 필수 (문자열)", fix="custom")
        if not isinstance(m.get("unit"), str) or not m.get("unit"):
            report("FAIL", "B9", f"{p}.unit 필수 (문자열, 단위 표기)", fix="custom")
        extra = set(m) - {"name", "unit", *PCT_KEYS}
        if extra:
            report("FAIL", "B9", f"{p} 에 허용되지 않은 키 {sorted(extra)} — name/unit/p50/p90/p95/p99만 허용", fix="custom")
        vals = [k for k in PCT_KEYS if k in m]
        if not vals:
            report("FAIL", "B9", f"{p}: 값 키(p50/p90/p95/p99) 최소 1개 필수 — 단일값 지표는 p50 하나로", fix="custom")
        for k in vals:
            if not isinstance(m[k], (int, float)) or isinstance(m[k], bool):
                report("FAIL", "B9", f"{p}.{k} 는 숫자여야 함", fix="custom")


def check_serving_block(serving, present, gpu_serving_models):
    if serving is None:
        if present:
            report("FAIL", "B9", "serving 이 null — 배열이어야 함 (자체 서빙 없으면 빈 배열 [])", fix="serving-structure")
        # 키 자체가 없으면 B1 에서 이미 FAIL 처리됨
        return
    if not isinstance(serving, list):
        report("FAIL", "B9", "serving 은 배열이어야 함", fix="serving-structure")
        return
    if not serving:
        if gpu_serving_models:
            report("FAIL", "B10", f"gpu 블록에 serving 행이 있는데 성능 행이 없음 (모델: {sorted(gpu_serving_models)}) — GPU 서빙 팀(케이스 A~D·F)은 serving 작성 필수 (그날 전 모델 요청 0건이었다면 무시 가능)", fix="serving-empty")
        else:
            report("PASS", "B9", "serving: [] (사외 API 전용·플랫폼 소비 전용이면 정상)")
        return
    report("PASS", "B9", f"serving 배열 확인 ({len(serving)}행) — 모델별 성능")
    allowed = {"model", "ttftMs", "itlMs", "outputTps", "e2eMs", "custom"}
    seen_models = set()
    for i, row in enumerate(serving):
        p = f"serving[{i}]"
        if not isinstance(row, dict):
            report("FAIL", "B9", f"{p} 가 객체가 아님", fix="serving-structure")
            continue
        if not isinstance(row.get("model"), str) or not row.get("model"):
            report("FAIL", "B9", f"{p}.model 필수 (canonical 표기)", fix="serving-structure")
        elif row["model"] == "unknown":
            report("FAIL", "B9", f"{p}.model 에 \"unknown\" 불가 — 성능은 canonical 모델 단위", fix="serving-structure")
        extra = set(row) - allowed
        if extra:
            report("FAIL", "B9", f"{p} 에 허용되지 않은 키 {sorted(extra)}", fix="serving-structure")
        m = row.get("model")
        if isinstance(m, str) and m:
            if m in seen_models:
                report("WARN", "B10", f"{p}: 동일 model 행 중복 — 모델당 1행으로 합칠 것", fix="serving-structure")
            seen_models.add(m)
        standard = [k for k in ("ttftMs", "itlMs", "outputTps", "e2eMs") if k in row]
        if not standard and "custom" not in row:
            report("FAIL", "B10", f"{p}: 지표가 하나도 없음 — 스트리밍은 ttftMs·itlMs·outputTps, 비스트리밍은 e2eMs", fix="no-metrics")
        elif not standard:
            report("WARN", "B10", f"{p}: 표준 지표 없이 custom 만 있음 — 비스트리밍(케이스 F)은 e2eMs 필수, custom 은 보조", fix="no-metrics")
        if "ttftMs" in row:
            check_percentiles(f"{p}.ttftMs", row["ttftMs"])
        if "itlMs" in row:
            check_percentiles(f"{p}.itlMs", row["itlMs"])
        if "e2eMs" in row:
            check_percentiles(f"{p}.e2eMs", row["e2eMs"])
        if ("ttftMs" in row) != ("itlMs" in row):
            report("WARN", "B10", f"{p}: ttftMs/itlMs 중 하나만 있음 — 스트리밍 모델은 쌍으로 제공", fix="no-metrics")
        if "outputTps" in row:
            tps = row["outputTps"]
            if not isinstance(tps, dict):
                report("FAIL", "B9", f"{p}.outputTps 는 객체여야 함", fix="tps")
            else:
                if "p50" not in tps or not isinstance(tps.get("p50"), (int, float)) or isinstance(tps.get("p50"), bool) or tps["p50"] < 0:
                    report("FAIL", "B9", f"{p}.outputTps.p50 필수 (0 이상의 숫자)", fix="tps")
                extra_t = set(tps) - {"p50"}
                if extra_t:
                    report("FAIL", "B9", f"{p}.outputTps 에 허용되지 않은 키 {sorted(extra_t)} — p50만 허용 (avg·상위 percentile 없음)", fix="tps")
        if "custom" in row:
            check_custom(f"{p}.custom", row["custom"])
    missing_models = gpu_serving_models - seen_models
    if missing_models:
        report("WARN", "B10", f"gpu serving 모델 중 성능 행이 없는 모델: {sorted(missing_models)} (그날 요청 0건인 모델이면 정상)", fix="serving-empty")


def check_body(body, req_date):
    try:
        doc = json.loads(body)
    except (json.JSONDecodeError, ValueError) as e:
        report("FAIL", "A3", f"응답이 JSON 이 아님: {e}", fix="json")
        return None
    report("PASS", "A3", "JSON 파싱 OK")
    if not isinstance(doc, dict):
        report("FAIL", "B1", "응답 본문은 객체여야 함", fix="json")
        return None

    missing = [k for k in ("date", "serviceGroup", "service", "generatedAt", "gpu", "serving") if k not in doc]
    if missing:
        report("FAIL", "B1", f"최상위 필수 필드 누락: {missing} (serving 은 자체 서빙이 없어도 빈 배열 [] 로 포함)", fix="required-fields")
    else:
        report("PASS", "B1", "최상위 필수 필드 존재 (date, serviceGroup, service, generatedAt, gpu, serving)")

    if doc.get("date") != req_date:
        report("FAIL", "B2", f"date 에코 불일치: 요청={req_date}, 응답={doc.get('date')!r}", fix="date-echo")
    else:
        report("PASS", "B2", "date 에코 일치")

    for k in ("serviceGroup", "service"):
        v = doc.get(k)
        if not isinstance(v, str) or not v:
            report("FAIL", "B1", f"{k} 는 비어있지 않은 문자열이어야 함 (메타데이터 시트 공식 표기)", fix="required-fields")

    gen = doc.get("generatedAt")
    if isinstance(gen, str) and re.search(r"\+09:00$", gen):
        try:
            datetime.fromisoformat(gen)
            report("PASS", "B3", f"generatedAt OK ({gen})")
        except ValueError:
            report("FAIL", "B3", f"generatedAt 이 ISO 8601 이 아님: {gen!r}", fix="generated-at")
    else:
        report("FAIL", "B3", f"generatedAt 은 ISO 8601 + '+09:00' 오프셋이어야 함 (현재: {gen!r})", fix="generated-at")

    gpu_serving_models = set()
    if isinstance(doc.get("gpu"), list):
        for r in doc["gpu"]:
            if isinstance(r, dict) and r.get("category") == "serving" and isinstance(r.get("model"), str) and r["model"] not in ("", "unknown"):
                gpu_serving_models.add(r["model"])
    if "gpu" in doc:
        check_gpu_block(doc["gpu"])
    check_serving_block(doc.get("serving"), "serving" in doc, gpu_serving_models)

    if "engine" in doc and doc["engine"] is not None:
        eng = doc["engine"]
        if not isinstance(eng, dict) or not isinstance(eng.get("type"), str) or not eng.get("type"):
            report("FAIL", "B11", f"engine 은 {{type, version?}} 객체여야 함 (현재: {eng!r})", fix="engine")
        else:
            report("PASS", "B11", f"engine 자기신고 OK (type={eng['type']})")
    else:
        report("WARN", "B11", "engine 자기신고 없음 — 권장 (버전 수기 갱신을 없애줌)", fix="engine")

    known = {"date", "serviceGroup", "service", "generatedAt", "engine", "gpu", "serving"}
    extra = set(doc) - known
    if extra:
        report("WARN", "B12", f"스펙에 없는 최상위 필드: {sorted(extra)} (토큰량·requests 를 보내지 말 것 — 이중 소스 금지)", fix="extra-fields")
    return doc


def behavior_checks(base_url, timeout):
    today = datetime.now(KST).date()

    status, headers, body = http_get(base_url, today.isoformat(), timeout)
    if status == 400:
        report("PASS", "C1", f"당일 date({today}) → 400")
        check_error_body("C1", body)
    elif status is None:
        report("FAIL", "C1", f"당일 date 호출 실패: {body}", fix="conn")
    else:
        report("FAIL", "C1", f"당일 date 는 400 이어야 함 (현재: {status}) — 미확정 데이터 구분 규칙", fix="today-future-400")

    future = (today + timedelta(days=7)).isoformat()
    status, headers, body = http_get(base_url, future, timeout)
    if status == 400:
        report("PASS", "C2", f"미래 date({future}) → 400")
    elif status is None:
        report("FAIL", "C2", f"미래 date 호출 실패: {body}", fix="conn")
    else:
        report("FAIL", "C2", f"미래 date 는 400 이어야 함 (현재: {status})", fix="today-future-400")

    old = (today - timedelta(days=30)).isoformat()
    status, headers, body = http_get(base_url, old, timeout)
    if status == 404:
        report("PASS", "C3", f"보존 기간 초과 date({old}) → 404")
    elif status == 200:
        report("WARN", "C3", f"30일 전 date 가 200 — 보존을 14일보다 길게 제공 중 (스펙상 초과분은 404)", fix="retention-404")
    elif status is None:
        report("FAIL", "C3", f"과거 date 호출 실패: {body}", fix="conn")
    else:
        report("FAIL", "C3", f"보존 기간 초과 date 는 404 여야 함 (현재: {status})", fix="retention-404")

    bad = "2026-13-99"
    status, headers, body = http_get(base_url, bad, timeout)
    if status == 400:
        report("PASS", "C5", f"형식 오류 date({bad}) → 400")
    elif status is None:
        report("FAIL", "C5", f"형식 오류 date 호출 실패: {body}", fix="conn")
    else:
        report("FAIL", "C5", f"형식 오류 date 는 400 이어야 함 (현재: {status})", fix="date-format-400")


def print_fix_guides():
    fail_keys = [k for k, v in fixes_needed.items() if v == "FAIL"]
    warn_keys = [k for k, v in fixes_needed.items() if v == "WARN"]
    if not fail_keys and not warn_keys:
        return
    print("\n" + "─" * 30 + " 고치는 방법 " + "─" * 30)
    for k in fail_keys:
        print("\n" + FIX_GUIDES[k])
    if warn_keys:
        print("\n" + "· " * 3 + "아래는 경고(WARN) 항목 — 계약 위반은 아니지만 확인 권장" + " ·" * 3)
        for k in warn_keys:
            print("\n" + FIX_GUIDES[k])
    print("\n" + "─" * 73)
    print("규칙 전문: docs/METRICS_COLLECTION_SPEC.md · 스키마: token-metric-api.yaml")


def main():
    ap = argparse.ArgumentParser(description="GET /v1/metrics 구현 자가 검사")
    ap.add_argument("--base-url", required=True, help="서비스 베이스 URL (예: http://my-svc.internal:8080)")
    ap.add_argument("--date", help="검사 대상 일자 YYYY-MM-DD (기본: 어제, KST)")
    ap.add_argument("--skip-behavior", action="store_true",
                    help="응답 규칙 동작 검사(C1~C5: 당일·미래·보존초과·형식오류·멱등) 생략")
    ap.add_argument("--no-fix-guide", action="store_true", help="실패 시 '고치는 방법' 상세 출력 생략")
    ap.add_argument("--timeout", type=float, default=10.0, help="HTTP 타임아웃 초 (기본 10)")
    args = ap.parse_args()

    date = args.date or (datetime.now(KST).date() - timedelta(days=1)).isoformat()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        print(f"--date 형식 오류: {date} (YYYY-MM-DD)")
        sys.exit(2)
    try:
        d = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        print(f"--date 가 존재하지 않는 날짜: {date}")
        sys.exit(2)
    if d >= datetime.now(KST).date():
        print(f"--date 는 과거 날짜여야 함 (KST 기준 오늘: {datetime.now(KST).date().isoformat()}) — 스펙상 당일/미래는 400 대상")
        sys.exit(2)

    print(f"# check_metrics_api — base={args.base_url} date={date}\n")

    status, headers, body = http_get(args.base_url, date, args.timeout)
    if status is None:
        report("FAIL", "A1", f"연결 실패: {body}", fix="conn")
    else:
        report("PASS", "A1", f"호출 성공 (HTTP {status})")
        if status == 200:
            ct = (headers.get("content-type") or "").lower()
            if "application/json" in ct:
                report("PASS", "A2", f"Content-Type OK ({ct})")
            else:
                report("WARN", "A2", f"Content-Type 이 application/json 이 아님 ({ct or '없음'})", fix="content-type")
            doc = check_body(body, date)
            # C4 멱등성: 같은 date 재호출 → 동일 본문
            if doc is not None and not args.skip_behavior:
                status2, _, body2 = http_get(args.base_url, date, args.timeout)
                try:
                    same = status2 == 200 and json.loads(body2) == doc
                except (json.JSONDecodeError, ValueError):
                    same = False
                if same:
                    report("PASS", "C4", "같은 date 재호출 시 동일 결과 (멱등성)")
                else:
                    report("FAIL", "C4", f"같은 date 재호출 결과가 다름 (2차: HTTP {status2}) — 재수집 안전성 위반", fix="idempotency")
        elif status == 409:
            report("PASS", "A2", "409 (집계 전 미확정) — 유효한 응답. 확정 후 재실행하거나 --date 로 확정된 날짜 지정")
            if headers and headers.get("retry-after"):
                report("INFO", "A2", f"Retry-After: {headers['retry-after']}초")
            check_error_body("A2", body)
        elif status == 404:
            report("WARN", "A2", "404 (보존 기간 초과) — 최근 날짜로 --date 를 지정해 재실행", fix="retention-404")
            check_error_body("A2", body)
        elif status == 400:
            report("FAIL", "A2", f"과거 유효 날짜({date})에 400 — date 파싱을 확인", fix="status-code")
            check_error_body("A2", body)
        else:
            report("FAIL", "A2", f"예상 밖 상태 코드 {status} — 200/409(/404) 이어야 함", fix="status-code")

    if not args.skip_behavior and status is not None:
        print()
        behavior_checks(args.base_url, args.timeout)

    n = {"PASS": 0, "WARN": 0, "FAIL": 0, "INFO": 0}
    for level, _, _ in results:
        n[level] += 1
    print(f"\n결과: PASS {n['PASS']} · WARN {n['WARN']} · FAIL {n['FAIL']}")
    if not args.no_fix_guide:
        print_fix_guides()
    if n["FAIL"]:
        print("\n→ 위 '고치는 방법'대로 수정한 뒤 재실행하세요.")
        sys.exit(1)
    print("→ 통과! 운영자측 검증(할당 대비 Σ검증, 모델명 정규화 등)은 수집 개시 후 자동으로 수행됩니다.")
    sys.exit(0)


if __name__ == "__main__":
    main()
