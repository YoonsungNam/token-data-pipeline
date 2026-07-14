#!/usr/bin/env python3
"""mart E2E용 fact 시드 — mock 서버 없이 mock datagen을 직접 import해 fact 2테이블에
INSERT한다 (Plan 3 T5). mart는 HTTP를 전혀 호출하지 않으므로(§collectors만 API 수집),
mock-provider 컨테이너 기동 자체가 불필요 — datagen(seed, date) 결정성만 재사용한다.

수집기(collectors/token-usage/app/{normalize,clickhouse_client}.py) 정규화·적재 규칙 재현:
- userId None → '' 로 정규화(user_id 컬럼이 Nullable이 아님, §5.4) + user_type은 그대로 보존
- cache_read/cache_creation 생략 → 0 (datagen이 이미 0으로 반환 — 별도 처리 불필요)
- date 컬럼은 datetime.date 객체로 삽입(str 삽입 시 수집기 E2E 회귀 재현 위험 — 반드시
  date 객체, Plan 2a E2E 교훈)
- generated_at/collected_at은 aware KST datetime(naive 삽입 시 clickhouse-connect가 호스트
  TZ로 해석해 벽시계가 어긋난다 — 수집기 C2 회귀 방지 교훈)

메인 날짜(첫 인자) 서비스 4개 시드 시나리오 (브리프 Step 2):
- Mock Service A: records + summary(is_derived=0) — 정상 케이스
- Mock Service B: records만, summary 미적재 — STEP0 missing 대상(coverage 마커 검증)
- Mock Service C: records + summary(is_derived=1) — 파생 시맨틱(diff_* NULL, reported_* 유지)
- Mock Service D: summary만(전 필드 0, is_derived=0), records 없음 — NODATA(agg summary-only 보강)

A/B/C는 "동일 seed"로 같은 (date, cfg) 조합의 records를 그대로 재사용한다 — datagen이
서비스명과 무관하게 결정적이므로, 세 서비스가 동일 사용자 분포를 공유한다(=
mart_expectations.py가 num_services=3으로 스케일하는 근거). D는 summary 값만 전부 0으로
직접 구성한다(records 미적재이므로 build_summary 재사용 불가).

두 번째 이후 인자(5월 고정 날짜 등)는 Service A(records+summary, is_derived=0)만 추가
시드한다 — B/C/D는 그 날짜에 아예 존재하지 않는다(브리프 §Step2 "Service A를 동일
seed로 추가 시드"; mart_expectations.py가 이 날짜를 num_services=1로 스케일하는 전제와
반드시 일치해야 한다 — build_seed_rows(..., full=False) 참조).

사용: python3 seed_fact.py <main_date> [<service_a_only_date> ...]   (CH_HOST/CH_PORT env,
mart/app/config.py와 동일 기본값 localhost:8123)
"""
import os
import sys
from datetime import date as date_cls, datetime, timedelta, timezone
from pathlib import Path

# e2e -> tests -> token-usage -> mart -> repo root = parents[4] (mart_expectations.py와
# 동일 근거로 실측 확인 — 맹복사 아님).
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "tools" / "mock-provider"))

from app.config import Config as MockConfig            # noqa: E402
from app.datagen import build_records, build_summary   # noqa: E402

KST = timezone(timedelta(hours=9))

SERVICE_GROUP = "Mock Group"

# collectors/token-usage/app/clickhouse_client.py의 DETAIL_COLS/SUMMARY_COLS 그대로 재사용
# (컬럼 순서 정본은 fact DDL — 위치 기반 삽입 금지, 항상 column_names 명시).
DETAIL_COLS = ("date", "service_group", "service", "reported_service_group",
               "reported_service", "user_id", "user_type", "model", "input_tokens",
               "cache_read_tokens", "cache_creation_tokens", "output_tokens",
               "requests", "generated_at", "collected_at")
SUMMARY_COLS = ("date", "service_group", "service", "reported_service_group",
                "reported_service", "input_tokens", "cache_read_tokens",
                "cache_creation_tokens", "output_tokens", "requests", "distinct_users",
                "distinct_identified_users", "is_derived", "generated_at", "collected_at")

_ZERO_SUMMARY = {"inputTokens": 0, "cacheReadTokens": 0, "cacheCreationTokens": 0,
                  "outputTokens": 0, "requests": 0, "distinctUsers": 0,
                  "distinctIdentifiedUsers": 0}


def _client():
    return __import__("clickhouse_connect").get_client(
        host=os.environ.get("CH_HOST", "localhost"),
        port=int(os.environ.get("CH_PORT", "8123")))


def _detail_rows(date_v, service: str, records, generated_at, collected_at) -> list[list]:
    """records(mock datagen UsageRecord) -> DETAIL_COLS 순서의 삽입 행 리스트.

    순수 함수(CH 미접촉) — CH 없이도 계산 부분만 단독 실행/검증 가능(브리프 Step 6).
    """
    rows = []
    for r in records:
        uid = r.user_id if r.user_id is not None else ""   # userId None -> '' (§5.4)
        rows.append([date_v, SERVICE_GROUP, service, SERVICE_GROUP, service,
                     uid, r.user_type, r.model, r.input_tokens, r.cache_read_tokens,
                     r.cache_creation_tokens, r.output_tokens, r.requests,
                     generated_at, collected_at])
    return rows


def _summary_row(date_v, service: str, summary: dict, is_derived: int,
                  generated_at, collected_at) -> list:
    """summary dict(build_summary 반환 형식) -> SUMMARY_COLS 순서의 삽입 행. 순수 함수."""
    return [date_v, SERVICE_GROUP, service, SERVICE_GROUP, service,
            summary["inputTokens"], summary["cacheReadTokens"],
            summary["cacheCreationTokens"], summary["outputTokens"],
            summary["requests"], summary["distinctUsers"],
            summary["distinctIdentifiedUsers"], is_derived,
            generated_at, collected_at]


def build_seed_rows(cfg: MockConfig, date: str, full: bool = True) -> tuple[list[list], list[list]]:
    """(date, cfg)에 대한 시드의 (detail_rows, summary_rows) — CH 미접촉 순수 함수.

    full=True(메인 날짜): 4서비스(A/B/C/D) 전부 시드.
    full=False(5월 고정 날짜, 브리프 §Step2): Service A(records+summary, is_derived=0)만
    추가 시드 — B/C/D는 그 날짜에 아예 존재하지 않는다(mart_expectations.py가
    num_services=1로 스케일하는 전제와 일치시켜야 한다).
    """
    date_v = date_cls.fromisoformat(date)
    next_day = date_v + timedelta(days=1)
    generated_at = datetime(next_day.year, next_day.month, next_day.day, 2, 5, 0, tzinfo=KST)
    collected_at = datetime(next_day.year, next_day.month, next_day.day, 4, 5, 0, tzinfo=KST)

    records = build_records(cfg, date)
    summary = build_summary(records)

    detail_rows: list[list] = []
    summary_rows: list[list] = []

    # Mock Service A — records + summary(is_derived=0) 정상 (모든 날짜 공통)
    detail_rows += _detail_rows(date_v, "Mock Service A", records, generated_at, collected_at)
    summary_rows.append(_summary_row(date_v, "Mock Service A", summary, 0,
                                      generated_at, collected_at))

    if full:
        # Mock Service B — records만, summary 미적재(STEP0 missing 대상)
        detail_rows += _detail_rows(date_v, "Mock Service B", records, generated_at, collected_at)

        # Mock Service C — records + summary(is_derived=1, 파생 시맨틱: reported_*=합산값
        # 유지, diff_*는 STEP1이 자기 자신 비교로 보고 NULL 처리)
        detail_rows += _detail_rows(date_v, "Mock Service C", records, generated_at, collected_at)
        summary_rows.append(_summary_row(date_v, "Mock Service C", summary, 1,
                                          generated_at, collected_at))

        # Mock Service D — summary만(전 필드 0, is_derived=0), records 없음(NODATA)
        summary_rows.append(_summary_row(date_v, "Mock Service D", _ZERO_SUMMARY, 0,
                                          generated_at, collected_at))

    return detail_rows, summary_rows


def seed_date(client, cfg: MockConfig, date: str, full: bool = True) -> int:
    detail_rows, summary_rows = build_seed_rows(cfg, date, full=full)
    if detail_rows:
        client.insert("fact.raw_token_usage_1d_dist", detail_rows, column_names=DETAIL_COLS)
    if summary_rows:
        client.insert("fact.raw_token_usage_summary_1d_dist", summary_rows,
                       column_names=SUMMARY_COLS)
    return len(detail_rows)


def main(argv=None) -> int:
    """인자: <메인_날짜> [<A-only_추가_날짜> ...] — 첫 인자만 4서비스(A/B/C/D) 전부 시드,
    이후 인자(예: 5월 고정 날짜)는 Service A만 추가 시드한다(브리프 §Step2)."""
    argv = argv if argv is not None else sys.argv[1:]
    if not argv:
        print("usage: seed_fact.py <main_date> [<service_a_only_date> ...]", file=sys.stderr)
        return 2
    # mart_expectations.py 기본값과 동일(seed=token-mock-1, users=50, anon=10,
    # models=[opus,sonnet,haiku]) — 두 스크립트가 같은 결정적 데이터셋을 보도록 고정.
    cfg = MockConfig()
    client = _client()

    main_date, *extra_dates = argv
    n = seed_date(client, cfg, main_date, full=True)
    print(f"seeded date={main_date} (A/B/C/D) detail_rows={n}")
    for date in extra_dates:
        n = seed_date(client, cfg, date, full=False)
        print(f"seeded date={date} (Service A only) detail_rows={n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
