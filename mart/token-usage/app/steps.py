"""STEP 1(fact×dim 조인·집계 4테이블)/STEP 2(view 적재) 서버사이드 SQL + 실행 함수 (Plan 3 T3).

전부 서버사이드 INSERT INTO ... SELECT — GLOBAL LEFT JOIN 표준(§4.0), 날짜는
ClickHouse 서버 바인딩(`{d:Date}`)만 사용한다(SQL 인젝션·타입 사고 방지, §7.1).

이스케이프 규칙: 아래 SQL 상수는 모듈 로드 시 f-string으로 DB명(DB_FACT/DB_DIM/DB_MART)만
보간하고, 서버 바인딩 자리는 소스에서 `{{d:Date}}`로 이중 중괄호 이스케이프해 최종 문자열에는
`{d:Date}`가 그대로 남는다.

컬럼 정본: mart/token-usage/ddl/company/{mart_tables.sql,view_token_usage.sql}(PR #6) —
아래 INSERT 컬럼 목록·SELECT 순서는 DDL 컬럼 순서를 그대로 옮긴 것. 위치 기반 INSERT 금지
(DDL 컬럼 추가 시 조용한 어긋남 방지) — 모든 INSERT가 대상 컬럼을 명시한다.

verify expected(CI 회귀 수정): insert_select의 written_rows는 verify_count의 expected로
쓰지 않는다 — Distributed(insert_distributed_sync=1) 경로에서 written_rows가 이중 계상되어
영원히 통과 불가능한 expected를 만든 CI 실패(rows_mart=0, elapsed=45.1s = 재시도 소진)가
있었다. 대신 테이블별 EXPECTED_SQL 소스 카운트 쿼리를 쓴다 — 근거·EXPECTED_SQL 정의는
아래 "EXPECTED_SQL" 섹션 docstring 참조.
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
        FROM {DB_DIM}.dim_token_user_org_dist
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
        FROM {DB_DIM}.dim_token_model_dist
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
        FROM {DB_DIM}.dim_token_user_org_dist
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
    FROM {DB_DIM}.dim_token_model_dist
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
# EXPECTED_SQL — verify_count의 expected는 insert_select의 written_rows가 아니라
# 여기 정의된 "소스 카운트" 쿼리 결과를 쓴다.
#
# 진단(CI 재현, expected=678 vs actual=339 영원 미달 → verify_count 재시도 10회 소진):
# clickhouse-connect의 QuerySummary.written_rows는 HTTP X-ClickHouse-Summary 헤더
# 기반이며, ClickHouse 공식 문서(system.query_log.written_rows)는 이를 "쿼리가 쓴
# 행수 — 파이프라인 내부에서 트리거되는 다운스트림 INSERT(예: 부착된 materialized
# view)가 쓴 행 포함"이라 명시한다. `INSERT INTO ... Distributed SELECT`를
# insert_distributed_sync=1로 실행하면 초기자(coordinator)가 로컬 샤드로의 포워딩을
# 같은 파이프라인 내부의 다운스트림 쓰기로 동기 수행하므로, 위 규칙에 따라
# written_rows에 "초기자 dist 쓰기 + local 쓰기"가 이중 계상된다(단일 샤드 E2E에서
# 실측: expected(=written_rows)=678=2×339, actual(진짜 적재 행수)=339 — verify_count가
# 영원히 통과할 수 없는 값을 스스로 만들어낸 것). written_rows 자체가 잘못 계산된
# 것이 아니라 "쓰기 요청 수"를 세는 것이지 "고유하게 적재된 행 수"를 세는 것이
# 아니므로, 애초에 멱등성 검증(verify)에 쓰기 부적합한 지표다.
#
# 따라서 verify는 written_rows에 의존하지 않고 항상 독립적인 소스 카운트로 수행한다.
# 실행 순서(§ 실행 함수): INSERT 후 expected 쿼리(gate.query) → verify_count(table,
# date, expected) — expected 쿼리는 직전 테이블 verify 통과 후 실행되므로 가시성
# 전제가 성립한다. written_rows는 텔레메트리로 강등 — StepError 메시지와 성공 로그에
# written_rows=W expected=E actual=A로 남겨 다음 실패 시 진단 가능하게 한다.
# =============================================================================

EXPECTED_SQL_DETAIL = f"""
SELECT count() FROM {DB_FACT}.raw_token_usage_1d_dist WHERE date = {{d:Date}}
"""
# ↑ STEP 1 detail의 GLOBAL LEFT JOIN 우측(eff_org/eff_model)은 각각 GROUP BY
# user_id / model + argMax로 키가 유니크 — fan-out이 없어 좌측(raw_token_usage_1d)
# 행수가 1:1로 보존된다. 따라서 raw 원본 행수가 곧 detail 기대 행수.

EXPECTED_SQL_AGG_SERVICE = f"""
SELECT uniqExact(service) FROM (
    SELECT service FROM {DB_MART}.token_usage_1d_dist
    WHERE date = {{d:Date}}
    UNION ALL
    SELECT service FROM {DB_FACT}.raw_token_usage_summary_1d_dist
    WHERE date = {{d:Date}}
)
"""
# ↑ agg_service 행 그레인은 date×service — detail에 존재하는 서비스 도메인과
# summary-only(NODATA 보강, UNION ALL 분기) 서비스 도메인의 합집합. uniqExact로
# 두 소스를 합쳐 distinct service 수를 셈.

EXPECTED_SQL_AGG_ORG = f"""
SELECT uniqExact(org_path) FROM {DB_MART}.token_usage_1d_dist WHERE date = {{d:Date}}
"""
# ↑ org_path는 detail 삽입 시 이미 물질화되어 있다(미매핑은 ['unknown']) —
# agg_org의 행 그레인은 date×org_path이므로 detail의 distinct org_path 수가 기대값.

EXPECTED_SQL_AGG_MODEL = f"""
SELECT uniqExact(model) FROM {DB_MART}.token_usage_1d_dist WHERE date = {{d:Date}}
"""
# ↑ agg_model의 행 그레인은 date×model — provider는 argMax로 model당 유일하게
# 부착되므로 fan-out 없이 detail의 distinct model 수가 기대값.

EXPECTED_SQL_VIEW_DETAIL = f"""
SELECT count() FROM {DB_MART}.token_usage_1d_dist WHERE date = {{d:Date}}
"""

EXPECTED_SQL_VIEW_SERVICE = f"""
SELECT count() FROM {DB_MART}.agg_token_service_1d_dist WHERE date = {{d:Date}}
"""

EXPECTED_SQL_VIEW_ORG = f"""
SELECT count() FROM {DB_MART}.agg_token_org_1d_dist WHERE date = {{d:Date}}
"""

EXPECTED_SQL_VIEW_MODEL = f"""
SELECT count() FROM {DB_MART}.agg_token_model_1d_dist WHERE date = {{d:Date}}
"""
# ↑ view 4종은 대응 mart 테이블을 SELECT * 없이 그대로 복사(STEP 2) — 대응 mart
# 테이블의 해당 날짜 행수가 곧 기대값.


# =============================================================================
# 실행 함수 — 멱등 시퀀스(§7.1): exists(없으면 delete 스킵, §4.0) → delete_day(+wait 내장)
# → insert_select(_dist 경유) → expected 소스 카운트 쿼리 → verify_count(재시도
# RETRY_COUNT×RETRY_INTERVAL_S)
# =============================================================================

# (result_key, dist_table, local_table, sql, expected_sql) — agg는 detail 완료 후 실행
# (소스 통일 §4.3) 순서 고정: detail → agg_service → agg_org → agg_model.
_STEP1_TABLES = [
    ("rows_detail", f"{DB_MART}.token_usage_1d_dist", f"{DB_MART}.token_usage_1d_local",
     SQL_DETAIL, EXPECTED_SQL_DETAIL),
    ("rows_svc", f"{DB_MART}.agg_token_service_1d_dist", f"{DB_MART}.agg_token_service_1d_local",
     SQL_AGG_SERVICE, EXPECTED_SQL_AGG_SERVICE),
    ("rows_org", f"{DB_MART}.agg_token_org_1d_dist", f"{DB_MART}.agg_token_org_1d_local",
     SQL_AGG_ORG, EXPECTED_SQL_AGG_ORG),
    ("rows_model", f"{DB_MART}.agg_token_model_1d_dist", f"{DB_MART}.agg_token_model_1d_local",
     SQL_AGG_MODEL, EXPECTED_SQL_AGG_MODEL),
]

# view DELETE 술어는 created_by 추가 — 공유 테이블(§7.1). 순서는 STEP 1과 동일하게 유지.
_VIEW_DELETE_PRED = "AND created_by = 'token-pipeline'"

_STEP2_TABLES = [
    ("view_detail", f"{DB_DIM}.view_token_usage_1d_dist", f"{DB_DIM}.view_token_usage_1d_local",
     SQL_VIEWS["detail"], EXPECTED_SQL_VIEW_DETAIL),
    ("view_service", f"{DB_DIM}.view_token_usage_service_1d_dist", f"{DB_DIM}.view_token_usage_service_1d_local",
     SQL_VIEWS["service"], EXPECTED_SQL_VIEW_SERVICE),
    ("view_org", f"{DB_DIM}.view_token_usage_org_1d_dist", f"{DB_DIM}.view_token_usage_org_1d_local",
     SQL_VIEWS["org"], EXPECTED_SQL_VIEW_ORG),
    ("view_model", f"{DB_DIM}.view_token_usage_model_1d_dist", f"{DB_DIM}.view_token_usage_model_1d_local",
     SQL_VIEWS["model"], EXPECTED_SQL_VIEW_MODEL),
]


def _run_table(gate, date: str, dist: str, local: str, sql: str, expected_sql: str,
                warns: list, extra_pred: str = "") -> int:
    """공용 시퀀스: exists → (delete_day) → insert_select → expected 소스 카운트
    조회(gate.query) → verify_count.

    verify_count의 expected는 insert_select의 written_rows가 아니라 expected_sql의
    소스 카운트 결과를 쓴다(Distributed 이중 계상 회피 — 모듈 상단 EXPECTED_SQL
    docstring 참조). written_rows는 텔레메트리로만 로그에 남긴다.

    verify_count 실패는 StepError(FAILURE 전파). 초과분(actual > expected)은
    "dup_suspect:<table>" 경고를 warns에 추가한다."""
    if gate.exists(dist, date):
        gate.delete_day(local, date, extra_pred=extra_pred)
    written = gate.insert_select(sql, {"d": date})
    expected_rows = gate.query(expected_sql, {"d": date})
    expected = int(expected_rows[0][0]) if expected_rows else 0
    ok, actual = gate.verify_count(dist, date, expected)
    if not ok:
        raise StepError(
            f"verify_count failed: {dist} date={date} "
            f"written_rows={written} expected={expected} actual={actual}")
    if actual > expected:
        warns.append(f"dup_suspect:{dist}")
    print(
        f"STEP table ok: {dist} date={date} written_rows={written} "
        f"expected={expected} actual={actual}",
        flush=True)
    return written


def run_step1(gate, date: str) -> dict:
    """STEP 1 — fact×dim 조인·집계 4테이블. 실행 순서 고정: detail → agg_service →
    agg_org → agg_model (agg는 detail 완료 후 — 소스 통일 §4.3)."""
    warns: list = []
    result: dict = {}
    for result_key, dist, local, sql, expected_sql in _STEP1_TABLES:
        result[result_key] = _run_table(gate, date, dist, local, sql, expected_sql, warns)
    result["warns"] = warns
    return result


def run_step2(gate, date: str) -> dict:
    """STEP 2 — gpu_data.view_token_usage_* 4테이블(mart에서 그대로 복사).
    view DELETE 술어는 created_by 추가(공유 테이블, §7.1). 마커의 rows_view는
    rows_view_detail로 통일(T4와 동일 문구)."""
    warns: list = []
    written: dict = {}
    for short, dist, local, sql, expected_sql in _STEP2_TABLES:
        written[short] = _run_table(gate, date, dist, local, sql, expected_sql, warns,
                                     extra_pred=_VIEW_DELETE_PRED)
    return {
        "rows_view_detail": written["view_detail"],
        "rows_view_total": sum(written.values()),
        "warns": warns,
    }
