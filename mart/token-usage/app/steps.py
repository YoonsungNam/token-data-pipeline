"""STEP 1(fact×dim 조인·집계 4테이블)/STEP 2(view 적재) 서버사이드 SQL + 실행 함수 (Plan 3 T3).

전부 서버사이드 INSERT INTO ... SELECT — GLOBAL LEFT JOIN 표준(§4.0), 날짜는
ClickHouse 서버 바인딩(`{d:Date}`)만 사용한다(SQL 인젝션·타입 사고 방지, §7.1).

이스케이프 규칙: 아래 SQL 상수는 모듈 로드 시 f-string으로 DB명(DB_FACT/DB_DIM/DB_MART)만
보간하고, 서버 바인딩 자리는 소스에서 `{{d:Date}}`로 이중 중괄호 이스케이프해 최종 문자열에는
`{d:Date}`가 그대로 남는다.

컬럼 정본: mart/token-usage/ddl/company/{mart_tables.sql,view_token_usage.sql}(PR #6) —
아래 INSERT 컬럼 목록·SELECT 순서는 DDL 컬럼 순서를 그대로 옮긴 것. 위치 기반 INSERT 금지
(DDL 컬럼 추가 시 조용한 어긋남 방지) — 모든 INSERT가 대상 컬럼을 명시한다.
"""
from app.ch import DB_DIM, DB_FACT, DB_MART


class StepError(Exception):
    """verify_count 실패(재시도 소진 후 actual < expected) 시 발생 — 호출자(batch.py)가
    BATCH_RESULT status=FAILURE로 전파한다."""


# =============================================================================
# STEP 1 — SQL 상수
# =============================================================================

SQL_DETAIL = f"""
INSERT INTO {DB_MART}.token_usage_1d_dist
    (date, service_group, service, user_id, user_type, model,
     org_path, org_top, org_leaf,
     input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens,
     total_input_tokens, requests, cost, created_by)
WITH
    eff_org AS (
        SELECT user_id, argMax(org_path, effective_from) AS org_path
        FROM {DB_DIM}.dim_user_org_dist
        WHERE effective_from <= {{d:Date}}
        GROUP BY user_id
    ),
    eff_model AS (
        -- 단가는 Nullable — 미등록 모델(join miss)·unknown(전 단가 NULL 시드) 모두
        -- cost 산식의 NULL 전파로 자연히 cost NULL이 된다 (§4.3·§6.2 리뷰 #15)
        SELECT model,
               argMax(provider, effective_from)                 AS provider,
               argMax(input_usd_per_mtok, effective_from)       AS p_in,
               argMax(cache_read_usd_per_mtok, effective_from)  AS p_cr,
               argMax(cache_creation_usd_per_mtok, effective_from) AS p_cc,
               argMax(output_usd_per_mtok, effective_from)      AS p_out
        FROM {DB_DIM}.dim_model_dist
        WHERE effective_from <= {{d:Date}}
        GROUP BY model
    )
SELECT
    r.date,
    r.service_group,
    r.service,
    r.user_id,
    r.user_type,
    r.model,
    if(length(o.org_path) = 0, ['unknown'], o.org_path)              AS org_path_v,
    org_path_v[1]                                                     AS org_top,
    org_path_v[length(org_path_v)]                                    AS org_leaf,
    r.input_tokens,
    r.cache_read_tokens,
    r.cache_creation_tokens,
    r.output_tokens,
    r.input_tokens + r.cache_read_tokens + r.cache_creation_tokens    AS total_input_tokens,
    r.requests,
    (r.input_tokens * m.p_in + r.cache_read_tokens * m.p_cr
     + r.cache_creation_tokens * m.p_cc + r.output_tokens * m.p_out) / 1e6
                                                                      AS cost,
    'token-pipeline'                                                  AS created_by
FROM {DB_FACT}.raw_token_usage_1d_dist AS r
GLOBAL LEFT JOIN eff_org   AS o ON o.user_id = r.user_id
GLOBAL LEFT JOIN eff_model AS m ON m.model  = r.model
WHERE r.date = {{d:Date}}
"""

SQL_AGG_SERVICE = f"""
INSERT INTO {DB_MART}.agg_token_service_1d_dist
    (date, service_group, service,
     input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens,
     total_input_tokens, requests, distinct_users, cost, is_derived,
     reported_input_tokens, reported_cache_read_tokens, reported_cache_creation_tokens,
     reported_output_tokens, reported_requests, reported_distinct_users,
     reported_distinct_identified_users,
     diff_input_tokens, diff_cache_read_tokens, diff_cache_creation_tokens,
     diff_output_tokens, diff_requests, created_by)
SELECT
    m.date, m.service_group, m.service,
    sum(m.input_tokens), sum(m.cache_read_tokens), sum(m.cache_creation_tokens),
    sum(m.output_tokens), sum(m.total_input_tokens), sum(m.requests),
    uniqExactIf(m.user_id, m.user_id != '')                            AS distinct_users,
    sum(m.cost)                                                        AS cost,
    coalesce(s.is_derived, 0)                                          AS is_derived,
    if(s.has_summary = 1, s.input_tokens, NULL)                        AS reported_input_tokens,
    if(s.has_summary = 1, s.cache_read_tokens, NULL),
    if(s.has_summary = 1, s.cache_creation_tokens, NULL),
    if(s.has_summary = 1, s.output_tokens, NULL),
    if(s.has_summary = 1, s.requests, NULL),
    if(s.has_summary = 1, s.distinct_users, NULL),
    s.distinct_identified_users,
    if(s.has_summary = 1 AND s.is_derived = 0,
       toInt64(sum(m.input_tokens)) - toInt64(s.input_tokens), NULL)   AS diff_input_tokens,
    if(s.has_summary = 1 AND s.is_derived = 0,
       toInt64(sum(m.cache_read_tokens)) - toInt64(s.cache_read_tokens), NULL),
    if(s.has_summary = 1 AND s.is_derived = 0,
       toInt64(sum(m.cache_creation_tokens)) - toInt64(s.cache_creation_tokens), NULL),
    if(s.has_summary = 1 AND s.is_derived = 0,
       toInt64(sum(m.output_tokens)) - toInt64(s.output_tokens), NULL),
    if(s.has_summary = 1 AND s.is_derived = 0,
       toInt64(sum(m.requests)) - toInt64(s.requests), NULL),
    'token-pipeline'
FROM {DB_MART}.token_usage_1d_dist AS m
GLOBAL LEFT JOIN (
    SELECT date, service, input_tokens, cache_read_tokens, cache_creation_tokens,
           output_tokens, requests, distinct_users, distinct_identified_users,
           is_derived, 1 AS has_summary
    FROM {DB_FACT}.raw_token_usage_summary_1d_dist
    WHERE date = {{d:Date}}
) AS s ON s.service = m.service
WHERE m.date = {{d:Date}}
GROUP BY m.date, m.service_group, m.service,
         s.has_summary, s.is_derived, s.input_tokens, s.cache_read_tokens,
         s.cache_creation_tokens, s.output_tokens, s.requests,
         s.distinct_users, s.distinct_identified_users

UNION ALL

-- summary만 있고 detail 0행(NODATA)인 서비스 보강 — detail 소스만으로는 행째
-- 탈락해 reported_* 관측이 사라진다. sums 0 + reported 유지 + diff = 0−reported
-- (summary가 0이면 diff 0 = 정상 NODATA, 비0이면 음수 = 대사 불일치 신호)
-- 이 보강 규칙은 스펙 §4.3에 없던 결정 — T8에서 스펙 v1.10으로 동기화한다.
SELECT
    s.date, s.service_group, s.service,
    0, 0, 0, 0, 0, 0,
    0                                           AS distinct_users,
    NULL                                        AS cost,
    s.is_derived,
    s.input_tokens, s.cache_read_tokens, s.cache_creation_tokens,
    s.output_tokens, s.requests, s.distinct_users, s.distinct_identified_users,
    if(s.is_derived = 0, 0 - toInt64(s.input_tokens), NULL),
    if(s.is_derived = 0, 0 - toInt64(s.cache_read_tokens), NULL),
    if(s.is_derived = 0, 0 - toInt64(s.cache_creation_tokens), NULL),
    if(s.is_derived = 0, 0 - toInt64(s.output_tokens), NULL),
    if(s.is_derived = 0, 0 - toInt64(s.requests), NULL),
    'token-pipeline'
FROM {DB_FACT}.raw_token_usage_summary_1d_dist AS s
WHERE s.date = {{d:Date}}
  AND s.service NOT IN (
      SELECT DISTINCT service FROM {DB_MART}.token_usage_1d_dist WHERE date = {{d:Date}})
"""

SQL_AGG_ORG = f"""
INSERT INTO {DB_MART}.agg_token_org_1d_dist
    (date, org_path, input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens,
     total_input_tokens, requests, distinct_users, headcount, adoption_rate, cost, created_by)
SELECT
    m.date, m.org_path,
    sum(m.input_tokens), sum(m.cache_read_tokens), sum(m.cache_creation_tokens),
    sum(m.output_tokens), sum(m.total_input_tokens), sum(m.requests),
    uniqExactIf(m.user_id, m.user_id != '')                        AS distinct_users,
    coalesce(any(h.headcount), 0)                                  AS headcount,
    if(coalesce(any(h.headcount), 0) > 0,
       uniqExactIf(m.user_id, m.user_id != '') / any(h.headcount),
       NULL)                                                       AS adoption_rate,
    sum(m.cost),
    'token-pipeline'
FROM {DB_MART}.token_usage_1d_dist AS m
GLOBAL LEFT JOIN (
    SELECT org_path, countIf(is_active = 1) AS headcount
    FROM (
        SELECT user_id,
               argMax(org_path, effective_from)  AS org_path,
               argMax(is_active, effective_from) AS is_active
        FROM {DB_DIM}.dim_user_org_dist
        WHERE effective_from <= {{d:Date}}
        GROUP BY user_id
    )
    GROUP BY org_path
) AS h ON h.org_path = m.org_path
WHERE m.date = {{d:Date}}
GROUP BY m.date, m.org_path
"""

SQL_AGG_MODEL = f"""
INSERT INTO {DB_MART}.agg_token_model_1d_dist
    (date, model, provider, input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens,
     total_input_tokens, requests, distinct_services, cost, created_by)
SELECT
    m.date, m.model,
    coalesce(any(em.provider), '')                                 AS provider,
    sum(m.input_tokens), sum(m.cache_read_tokens), sum(m.cache_creation_tokens),
    sum(m.output_tokens), sum(m.total_input_tokens), sum(m.requests),
    uniqExact(m.service)                                           AS distinct_services,
    sum(m.cost),
    'token-pipeline'
FROM {DB_MART}.token_usage_1d_dist AS m
GLOBAL LEFT JOIN (
    SELECT model, argMax(provider, effective_from) AS provider
    FROM {DB_DIM}.dim_model_dist
    WHERE effective_from <= {{d:Date}}
    GROUP BY model
) AS em ON em.model = m.model
WHERE m.date = {{d:Date}}
GROUP BY m.date, m.model
"""

# =============================================================================
# STEP 2 — view 4종 SQL 상수 (mart에서 그대로 복사, SELECT * 금지 — 컬럼 드리프트 조기 검출)
# =============================================================================

SQL_VIEW_DETAIL = f"""
INSERT INTO {DB_DIM}.view_token_usage_1d_dist
    (date, service_group, service, user_id, user_type, model,
     org_path, org_top, org_leaf,
     input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens,
     total_input_tokens, requests, cost, created_by)
SELECT date, service_group, service, user_id, user_type, model,
       org_path, org_top, org_leaf,
       input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens,
       total_input_tokens, requests, cost, created_by
FROM {DB_MART}.token_usage_1d_dist
WHERE date = {{d:Date}}
"""

SQL_VIEW_SERVICE = f"""
INSERT INTO {DB_DIM}.view_token_usage_service_1d_dist
    (date, service_group, service,
     input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens,
     total_input_tokens, requests, distinct_users, cost, is_derived,
     reported_input_tokens, reported_cache_read_tokens, reported_cache_creation_tokens,
     reported_output_tokens, reported_requests, reported_distinct_users,
     reported_distinct_identified_users,
     diff_input_tokens, diff_cache_read_tokens, diff_cache_creation_tokens,
     diff_output_tokens, diff_requests, created_by)
SELECT date, service_group, service,
       input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens,
       total_input_tokens, requests, distinct_users, cost, is_derived,
       reported_input_tokens, reported_cache_read_tokens, reported_cache_creation_tokens,
       reported_output_tokens, reported_requests, reported_distinct_users,
       reported_distinct_identified_users,
       diff_input_tokens, diff_cache_read_tokens, diff_cache_creation_tokens,
       diff_output_tokens, diff_requests, created_by
FROM {DB_MART}.agg_token_service_1d_dist
WHERE date = {{d:Date}}
"""

SQL_VIEW_ORG = f"""
INSERT INTO {DB_DIM}.view_token_usage_org_1d_dist
    (date, org_path, input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens,
     total_input_tokens, requests, distinct_users, headcount, adoption_rate, cost, created_by)
SELECT date, org_path, input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens,
       total_input_tokens, requests, distinct_users, headcount, adoption_rate, cost, created_by
FROM {DB_MART}.agg_token_org_1d_dist
WHERE date = {{d:Date}}
"""

SQL_VIEW_MODEL = f"""
INSERT INTO {DB_DIM}.view_token_usage_model_1d_dist
    (date, model, provider, input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens,
     total_input_tokens, requests, distinct_services, cost, created_by)
SELECT date, model, provider, input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens,
       total_input_tokens, requests, distinct_services, cost, created_by
FROM {DB_MART}.agg_token_model_1d_dist
WHERE date = {{d:Date}}
"""

SQL_VIEWS = {
    "detail": SQL_VIEW_DETAIL,
    "service": SQL_VIEW_SERVICE,
    "org": SQL_VIEW_ORG,
    "model": SQL_VIEW_MODEL,
}


# =============================================================================
# 실행 함수 — 멱등 시퀀스(§7.1): exists(없으면 delete 스킵, §4.0) → delete_day(+wait 내장)
# → insert_select(_dist 경유) → verify_count(재시도 RETRY_COUNT×RETRY_INTERVAL_S)
# =============================================================================

# (result_key, short_name, dist_table, local_table, sql) — agg는 detail 완료 후 실행
# (소스 통일 §4.3) 순서 고정: detail → agg_service → agg_org → agg_model.
_STEP1_TABLES = [
    ("rows_detail", f"{DB_MART}.token_usage_1d_dist", f"{DB_MART}.token_usage_1d_local", SQL_DETAIL),
    ("rows_svc", f"{DB_MART}.agg_token_service_1d_dist", f"{DB_MART}.agg_token_service_1d_local", SQL_AGG_SERVICE),
    ("rows_org", f"{DB_MART}.agg_token_org_1d_dist", f"{DB_MART}.agg_token_org_1d_local", SQL_AGG_ORG),
    ("rows_model", f"{DB_MART}.agg_token_model_1d_dist", f"{DB_MART}.agg_token_model_1d_local", SQL_AGG_MODEL),
]

# view DELETE 술어는 created_by 추가 — 공유 테이블(§7.1). 순서는 STEP 1과 동일하게 유지.
_VIEW_DELETE_PRED = "AND created_by = 'token-pipeline'"

_STEP2_TABLES = [
    ("view_detail", f"{DB_DIM}.view_token_usage_1d_dist", f"{DB_DIM}.view_token_usage_1d_local", SQL_VIEWS["detail"]),
    ("view_service", f"{DB_DIM}.view_token_usage_service_1d_dist", f"{DB_DIM}.view_token_usage_service_1d_local", SQL_VIEWS["service"]),
    ("view_org", f"{DB_DIM}.view_token_usage_org_1d_dist", f"{DB_DIM}.view_token_usage_org_1d_local", SQL_VIEWS["org"]),
    ("view_model", f"{DB_DIM}.view_token_usage_model_1d_dist", f"{DB_DIM}.view_token_usage_model_1d_local", SQL_VIEWS["model"]),
]


def _run_table(gate, date: str, dist: str, local: str, sql: str, warns: list, extra_pred: str = "") -> int:
    """공용 시퀀스: exists → (delete_day) → insert_select → verify_count.

    verify_count 실패는 StepError(FAILURE 전파). 초과분(actual > written)은
    "dup_suspect:<table>" 경고를 warns에 추가한다."""
    if gate.exists(dist, date):
        gate.delete_day(local, date, extra_pred=extra_pred)
    written = gate.insert_select(sql, {"d": date})
    ok, actual = gate.verify_count(dist, date, written)
    if not ok:
        raise StepError(
            f"verify_count failed: {dist} date={date} expected={written} actual={actual}")
    if actual > written:
        warns.append(f"dup_suspect:{dist}")
    return written


def run_step1(gate, date: str) -> dict:
    """STEP 1 — fact×dim 조인·집계 4테이블. 실행 순서 고정: detail → agg_service →
    agg_org → agg_model (agg는 detail 완료 후 — 소스 통일 §4.3)."""
    warns: list = []
    result: dict = {}
    for result_key, dist, local, sql in _STEP1_TABLES:
        result[result_key] = _run_table(gate, date, dist, local, sql, warns)
    result["warns"] = warns
    return result


def run_step2(gate, date: str) -> dict:
    """STEP 2 — gpu_data.view_token_usage_* 4테이블(mart에서 그대로 복사).
    view DELETE 술어는 created_by 추가(공유 테이블, §7.1). 마커의 rows_view는
    rows_view_detail로 통일(T4와 동일 문구)."""
    warns: list = []
    written: dict = {}
    for short, dist, local, sql in _STEP2_TABLES:
        written[short] = _run_table(gate, date, dist, local, sql, warns, extra_pred=_VIEW_DELETE_PRED)
    return {
        "rows_view_detail": written["view_detail"],
        "rows_view_total": sum(written.values()),
        "warns": warns,
    }
