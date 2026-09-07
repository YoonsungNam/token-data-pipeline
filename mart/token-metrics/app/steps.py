"""mart-metrics M1/M3/M4/M2 서버사이드 SQL + 실행 함수 (Plan 6c T3~T7; 이 파일은 T3 시점 = 공통 조각 + M1).

전부 서버사이드 INSERT INTO ... SELECT — GLOBAL LEFT JOIN 표준(설계 §4.0,
distributed_product_mode=global은 CHGate.insert_select settings), 날짜는 ClickHouse 서버
바인딩(`{d:Date}`)만 사용한다(SQL 인젝션·타입 사고 방지, 마스터 §7.1).

이스케이프 규칙: 아래 SQL 상수는 모듈 로드 시 f-string으로 DB명(DB_FACT/DB_DIM/DB_MART/
DB_TOKEN_MART/DB_TOKEN_DIM)과 가중치 상수(W_*)만 보간하고, 서버 바인딩 자리는 소스에서
`{{d:Date}}`로 이중 중괄호 이스케이프해 최종 문자열에는 `{d:Date}`가 그대로 남는다.

DB명 규약(설계 §6.1): 메트릭 fact는 DB_FACT, 메트릭 dim·레지스트리는 DB_DIM, mart 4테이블은
DB_MART, **토큰 측 읽기(token_usage_1d·agg_token_service_1d → DB_TOKEN_MART, dim_token_service
→ DB_TOKEN_DIM)는 읽기 계약 13컬럼만**(app/preflight.py READ_CONTRACT) — company-verify 격리
검증 시 토큰 측만 운영 DB를 가리킨다.

공통 서브쿼리(설계 §6.1 "공통 CTE" eff_alias/eff_tco/eff_alloc/eff_price/reg/usage_svc/anchor):
CTE 대신 **괄호 포함 서브쿼리 문자열 상수 SUB_***로 두고 호출측이 `AS a/t/al/p/r/u/an`을
붙인다 — INSERT…SELECT와 EXPECTED_SQL이 같은 문자열 조각을 공유해 파생 오차를 0으로 만들기
위함(설계 해석 3). 이력 조회는 `effective_from <= {d:Date}` 행 중 argMax(최신) — 최신 행의
NULL은 NULL로 전파(`nullIf(argMax(ifNull(x, -1), effective_from), -1)`; argMax가 NULL arg를
건너뛰어 과거 실값이 되살아나는 문제 회피, 설계 해석 2).

컬럼 정본: mart/token-metrics/ddl/company/mart_metrics_tables.sql(Plan 6a T4) — 아래 INSERT
컬럼 목록·SELECT 순서는 DDL 컬럼 순서를 그대로 옮긴 것. 위치 기반 INSERT 금지 — 모든 INSERT가
대상 컬럼을 명시한다(tests/test_steps.py가 DDL 파일을 파싱해 동일 리스트를 단언).

verify expected: insert_select의 written_rows는 verify_count의 expected로 쓰지 않는다 —
Distributed(insert_distributed_sync=1) 경로에서 written_rows가 이중 계상되어 영원히 통과
불가능한 expected를 만든 토큰 mart CI 실패 전례(mart/token-usage/app/steps.py docstring).
대신 테이블별 EXPECTED_SQL 소스 카운트(같은 키 서브쿼리 문자열의 UNION ALL + uniqExact)를 쓴다.

비용 모델(설계 §6.4 = docs/cost-model-spec.md): C = Σ(serving+standby, 비FAIL) gpu_hours × TCO
(기종 하나라도 TCO NULL → NULL, test 제외), FAIL 플래그 = FAIL_PRED, W = W_UNC·(input +
cache_creation) + W_CACHE·cache_read + W_OUT·output(app/mart.py 상수 정본). 참조 구현은
app/mart.py(model_cost/weighted_tokens …) — SQL과 동일 규칙, e2e가 대조한다.
"""
from __future__ import annotations

from app.ch import DB_DIM, DB_FACT, DB_MART, DB_TOKEN_DIM, DB_TOKEN_MART
from app.mart import FAIL_FLAGS, W_CACHE, W_OUT, W_UNC


class StepError(Exception):
    """verify_count 실패(재시도 소진 후 actual < expected) 시 발생 — 호출자(batch.py)가
    BATCH_RESULT status=FAILURE로 전파한다."""


# 공유 쓰기 계약(Plan 6a C) — 기존 토큰 mart의 'token-pipeline'과 구분, 불변식
# created_by_wrong_metrics가 이 값을 검사한다.
CREATED_BY = "token-metrics-pipeline"

# mart 4테이블(DB_MART 접두 없이 — 호출측이 f"{DB_MART}.{T_M1}_dist"/_local로 조립).
# MART_TABLES 순서 = 배치 실행 순서 M1 → M3 → M4 → M2(설계 §6.1) = 뮤테이션 예산 선검사 순회 순서.
T_M1 = "agg_token_model_cost_1d"
T_M3 = "token_metrics_check_1d"
T_M4 = "agg_token_model_share_1d"
T_M2 = "agg_token_gpu_group_1d"
MART_TABLES = (T_M1, T_M3, T_M4, T_M2)


# =============================================================================
# 공통 조각 — canon()·FAIL_PRED·SUB_* (설계 §6.1 299)
# =============================================================================

def canon(x: str) -> str:
    """canonical 정규화 식 — `dim_token_model_alias` 히트(a.canonical != '')면 canonical, 아니면
    원문 그대로(LEFT JOIN 미스는 join_use_nulls=0 규약으로 ''). INSERT와 EXPECTED가 **같은
    문자열**을 쓰도록 반드시 이 함수로 만든다(테스트가 동일성 단언). 호출측은 alias 서브쿼리를
    `AS a`로 조인해 두어야 한다."""
    return f"if(a.canonical = '', {x}, a.canonical)"


# FAIL 플래그 술어(설계 §6.4 (1) 파이프라인 보정) — app.mart.FAIL_FLAGS에서 생성해 참조 구현과
# 문자열 정본을 공유한다. gpu fact alias는 항상 `g`.
FAIL_PRED = "hasAny(g.flags, [" + ",".join(f"'{f}'" for f in FAIL_FLAGS) + "])"

# 가중 토큰 식 조각(설계 §6.4 (3), 정의서 3.5) — M1(outer SELECT)·M4(wt 서브쿼리)가 공유.
# 피연산자는 **같은 SELECT 안의 alias**(input_tokens/cache_read_tokens/cache_creation_tokens/
# output_tokens)를 가리키므로, 사용측은 이 조각 앞에서 4컬럼을 그 이름으로 alias 해 둔다.
_WTOK_EXPR = (
    f"{W_UNC} * (input_tokens + cache_creation_tokens)"
    f" + {W_CACHE} * cache_read_tokens + {W_OUT} * output_tokens"
)

# eff_alias — alias별 date 유효 최신 canonical (String, NULL 없음 → 그냥 argMax)
SUB_EFF_ALIAS = f"""(SELECT alias, argMax(canonical, effective_from) AS canonical
        FROM {DB_DIM}.dim_token_model_alias_dist
        WHERE effective_from <= {{d:Date}}
        GROUP BY alias)"""

# eff_tco — gpu_type별 date 유효 최신 TCO(원/GPU·h). 최신 이력 행이 NULL이면 NULL(설계 해석 2).
SUB_EFF_TCO = f"""(SELECT gpu_type,
               nullIf(argMax(ifNull(tco_krw_per_gpu_hour, -1), effective_from), -1) AS tco
        FROM {DB_DIM}.dim_token_gpu_tco_dist
        WHERE effective_from <= {{d:Date}}
        GROUP BY gpu_type)"""

# eff_alloc — (service_group, gpu_type)별 date 유효 최신 할당 GPU 수. `unknown` 기종 제외(설계 §6.1).
SUB_EFF_ALLOC = f"""(SELECT service_group, gpu_type,
               nullIf(argMax(ifNull(allocated_gpu_count, -1), effective_from), -1) AS allocated_gpu_count,
               argMax(source, effective_from) AS source
        FROM {DB_DIM}.dim_token_gpu_allocation_dist
        WHERE effective_from <= {{d:Date}}
        GROUP BY service_group, gpu_type
        HAVING gpu_type != 'unknown')"""

# eff_price — (provider, model)별 date 유효 최신 벤더 단가 4종(원/1M 토큰), 처리등급 standard 고정
# (설계 §6.4 (6)). 단가 NULL은 NULL 그대로(비용 NULL 전파 + M3 vendor_price_missing).
SUB_EFF_PRICE = f"""(SELECT provider, model,
               nullIf(argMax(ifNull(krw_per_mtok_input, -1), effective_from), -1)          AS p_in,
               nullIf(argMax(ifNull(krw_per_mtok_cached, -1), effective_from), -1)         AS p_cached,
               nullIf(argMax(ifNull(krw_per_mtok_cache_creation, -1), effective_from), -1) AS p_cc,
               nullIf(argMax(ifNull(krw_per_mtok_output, -1), effective_from), -1)         AS p_out
        FROM {DB_DIM}.dim_token_vendor_price_dist
        WHERE tier = 'standard' AND effective_from <= {{d:Date}}
        GROUP BY provider, model)"""

# reg — 메트릭 레지스트리 전체 행(설계 §4.3; 6b가 원자 교체). until은 Nullable(Date).
SUB_REG = f"""(SELECT service, service_group, enabled, coverage_since, until,
               expect_gpu, expect_serving, usage_includes_consumers
        FROM {DB_DIM}.dim_token_metrics_service_dist)"""

# usage_svc — 토큰 측 모집단(dim_token_service enabled=1; 읽기 계약 2컬럼 service/enabled).
SUB_USAGE_SVC = f"""(SELECT service FROM {DB_TOKEN_DIM}.dim_token_service_dist WHERE enabled = 1)"""

# anchor — 그날 앵커(summary, 응답당 1행). 메트릭 측 소스는 앵커가 있는 (date, service)만(설계 §6.1).
SUB_ANCHOR = f"""(SELECT service, service_group, reported_service_group, reported_service, source_type,
               gpu_rows, serving_rows, rejected_rows
        FROM {DB_FACT}.raw_token_metrics_summary_1d_dist
        WHERE date = {{d:Date}})"""

# 자식 행 실측 수(서비스 단위) — 앵커 gpu_rows/serving_rows와 대조(partial 판정, 설계 해석 4).
# 앵커 serving_rows = 표준 지표 행 수(metric != 'custom', Plan 6b NormalizeResult.n_serving)이고 custom 행은
# custom_rows로 따로 기록되므로(설계 §4.1 long form) serving 실측은 metric != 'custom' 행만 센다.
SUB_GPU_CNT = f"""(SELECT service, count() AS n
        FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist
        WHERE date = {{d:Date}}
        GROUP BY service)"""
SUB_SERVING_CNT = f"""(SELECT service, countIf(metric != 'custom') AS n
        FROM {DB_FACT}.raw_token_metrics_serving_1d_dist
        WHERE date = {{d:Date}}
        GROUP BY service)"""


# =============================================================================
# M1 — mart.agg_token_model_cost_1d (설계 §6.1 302, §6.4 (1)(3))
#   grain: date × service × model(canon). keys = tok 키 ∪ gpu 키(UNION DISTINCT)를 구동 테이블로
#   tok_agg/gpu_agg/reg/anchor/자식 카운트를 GLOBAL LEFT JOIN. INSERT와 EXPECTED가 같은 키 조각
#   (_TOK_KEYS/_GPU_KEYS)을 공유한다.
# =============================================================================

# 토큰 측 소스 = token_usage_1d(읽기 계약 9컬럼) 중 usage_svc 서비스 전부(소비 전용 포함).
_TOK_SRC = f"""FROM {DB_TOKEN_MART}.token_usage_1d_dist AS u
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = u.model"""
_TOK_TAIL = f"""WHERE u.date = {{d:Date}} AND u.service GLOBAL IN {SUB_USAGE_SVC}
    GROUP BY u.service, {canon('u.model')}"""

# 메트릭 측 소스 = gpu fact 중 앵커가 있는 서비스(FAIL 행 포함해 키 유지 — 시간은 NOT FAIL_PRED로 분리).
_GPU_SRC = f"""FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = g.model"""
_GPU_TAIL = f"""WHERE g.date = {{d:Date}} AND g.service GLOBAL IN (SELECT service FROM {SUB_ANCHOR})
    GROUP BY g.service, {canon('g.model')}"""

# 키 서브쿼리 조각 — SQL_M1의 keys(UNION DISTINCT)와 EXPECTED_SQL_M1(UNION ALL + uniqExact) 공유.
_TOK_KEYS = f"""SELECT u.service AS service, {canon('u.model')} AS model
    {_TOK_SRC}
    {_TOK_TAIL}"""
_GPU_KEYS = f"""SELECT g.service AS service, {canon('g.model')} AS model
    {_GPU_SRC}
    {_GPU_TAIL}"""

SQL_M1 = f"""
INSERT INTO {DB_MART}.{T_M1}_dist
    (date, service_group, service, model,
     serving_gpu_hours, standby_gpu_hours, test_gpu_hours, flagged_gpu_hours,
     equiv_gpu_count, scaled_intraday, model_cost_krw,
     input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, requests,
     uncached_tokens, cached_tokens, total_tokens, weighted_tokens, tokens_per_gpu_hour,
     gpu_type_mix, model_registered, tco_missing, has_token_rows, has_gpu_rows,
     quality_flag, created_by)
WITH
    tok_agg AS (
        SELECT u.service                     AS service,
               any(u.service_group)          AS service_group,
               {canon('u.model')}            AS model,
               sum(u.input_tokens)           AS input_tokens,
               sum(u.cache_read_tokens)      AS cache_read_tokens,
               sum(u.cache_creation_tokens)  AS cache_creation_tokens,
               sum(u.output_tokens)          AS output_tokens,
               sum(u.requests)               AS requests,
               max(a.canonical != '')        AS registered,
               1                             AS has_rows
        {_TOK_SRC}
        {_TOK_TAIL}
    ),
    gpu_agg AS (
        -- 시간 4분류: serving/standby/test는 비FAIL 행만, flagged는 FAIL 행 전체(카테고리 무관).
        -- C = Σ(serving+standby, 비FAIL) gpu_hours × TCO — 그 행 중 TCO NULL 기종이 하나라도 있으면
        -- (tco_null_cnt > 0) outer에서 NULL(부분 합 금지, 설계 §6.4 (1)). test 시간은 C 불포함.
        SELECT g.service                     AS service,
               any(g.service_group)          AS service_group,
               {canon('g.model')}            AS model,
               sumIf(g.gpu_hours, g.category = 'serving' AND NOT {FAIL_PRED})  AS serving_gpu_hours,
               sumIf(g.gpu_hours, g.category = 'standby' AND NOT {FAIL_PRED})  AS standby_gpu_hours,
               sumIf(g.gpu_hours, g.category = 'test' AND NOT {FAIL_PRED})     AS test_gpu_hours,
               sumIf(g.gpu_hours, {FAIL_PRED})                                 AS flagged_gpu_hours,
               countIf(g.category IN ('serving','standby') AND NOT {FAIL_PRED} AND isNull(t.tco))
                                                                               AS tco_null_cnt,
               sumIf(g.gpu_hours * t.tco,
                     g.category IN ('serving','standby') AND NOT {FAIL_PRED} AND isNotNull(t.tco))
                                                                               AS cost_sum,
               arraySort(groupUniqArray(g.gpu_type))                           AS gpu_type_mix,
               max(isNull(t.tco))                                              AS tco_missing,
               max(a.canonical != '')                                          AS registered,
               1                                                               AS has_rows
        {_GPU_SRC}
        GLOBAL LEFT JOIN {SUB_EFF_TCO} AS t ON t.gpu_type = g.gpu_type
        {_GPU_TAIL}
    ),
    keys AS (
        {_TOK_KEYS}
        UNION DISTINCT
        {_GPU_KEYS}
    )
SELECT
    {{d:Date}}                                                        AS date,
    multiIf(r.service_group != '', r.service_group,
            ga.service_group != '', ga.service_group,
            tk.service_group)                                         AS service_group,
    k.service                                                         AS service,
    k.model                                                           AS model,
    ga.serving_gpu_hours                                              AS serving_gpu_hours,
    ga.standby_gpu_hours                                              AS standby_gpu_hours,
    ga.test_gpu_hours                                                 AS test_gpu_hours,
    ga.flagged_gpu_hours                                              AS flagged_gpu_hours,
    (ga.serving_gpu_hours + ga.standby_gpu_hours + ga.test_gpu_hours) / 24
                                                                      AS equiv_gpu_count,
    0                                                                 AS scaled_intraday,
    if(ga.has_rows = 0 OR ga.tco_null_cnt > 0, NULL, ifNull(ga.cost_sum, 0))
                                                                      AS model_cost_krw,
    tk.input_tokens                                                   AS input_tokens,
    tk.cache_read_tokens                                              AS cache_read_tokens,
    tk.cache_creation_tokens                                          AS cache_creation_tokens,
    tk.output_tokens                                                  AS output_tokens,
    tk.requests                                                       AS requests,
    input_tokens + cache_creation_tokens                              AS uncached_tokens,
    cache_read_tokens                                                 AS cached_tokens,
    input_tokens + cache_read_tokens + cache_creation_tokens + output_tokens
                                                                      AS total_tokens,
    {_WTOK_EXPR}                                                      AS weighted_tokens,
    if(ga.serving_gpu_hours > 0, total_tokens / ga.serving_gpu_hours, NULL)
                                                                      AS tokens_per_gpu_hour,
    ga.gpu_type_mix                                                   AS gpu_type_mix,
    greatest(tk.registered, ga.registered)                            AS model_registered,
    ga.tco_missing                                                    AS tco_missing,
    tk.has_rows                                                       AS has_token_rows,
    ga.has_rows                                                       AS has_gpu_rows,
    -- 우선순위 고정(설계 §6.1): partial > no_tco > flagged > manual > no_metrics > consumer_only > normal
    multiIf(
        an.service != '' AND (an.gpu_rows != gc.n OR an.serving_rows != sc.n),          'partial',
        ga.has_rows = 1 AND ga.tco_null_cnt > 0,                                        'no_tco',
        ga.flagged_gpu_hours > 0,                                                       'flagged',
        an.source_type = 'manual-v0',                                                   'manual',
        r.service != '' AND r.enabled = 1 AND r.coverage_since <= {{d:Date}}
            AND (isNull(r.until) OR {{d:Date}} <= r.until) AND an.service = '',         'no_metrics',
        r.service = '',                                                                 'consumer_only',
        'normal')                                                     AS quality_flag,
    '{CREATED_BY}'                                                    AS created_by
FROM keys AS k
GLOBAL LEFT JOIN tok_agg AS tk ON tk.service = k.service AND tk.model = k.model
GLOBAL LEFT JOIN gpu_agg AS ga ON ga.service = k.service AND ga.model = k.model
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = k.service
GLOBAL LEFT JOIN {SUB_ANCHOR} AS an ON an.service = k.service
GLOBAL LEFT JOIN {SUB_GPU_CNT} AS gc ON gc.service = k.service
GLOBAL LEFT JOIN {SUB_SERVING_CNT} AS sc ON sc.service = k.service
"""

EXPECTED_SQL_M1 = f"""
SELECT uniqExact((service, model)) FROM (
    {_TOK_KEYS}
    UNION ALL
    {_GPU_KEYS}
)
"""
# ↑ M1 행 그레인은 date×service×model(canon) — keys(UNION DISTINCT)의 distinct 키 수와 같다.
# 좌측 keys에 붙는 tok_agg/gpu_agg(GROUP BY 키 유니크)·reg(ORDER BY service 유일)·anchor
# (date×service 1행)·자식 카운트(GROUP BY service)는 전부 키 유니크라 fan-out이 없다.


# =============================================================================
# 실행 함수 — 공용 시퀀스 _run_table + run_m1 (run_m3/run_m4/run_m2는 T4/T6/T7)
# =============================================================================

def _run_table(gate, date: str, dist: str, local: str, sql: str, expected_sql: str,
                warns: list, extra_pred: str = "") -> int:
    """공용 시퀀스: exists → (delete_day) → insert_select → expected 소스 카운트
    조회(gate.query) → verify_count.

    verify_count의 expected는 insert_select의 written_rows가 아니라 expected_sql의
    소스 카운트 결과를 쓴다(Distributed 이중 계상 회피 — 모듈 상단 docstring 참조).
    written_rows는 텔레메트리로만 로그에 남긴다.

    verify_count 실패는 StepError(FAILURE 전파). 초과분(actual > expected)은
    "dup_suspect:<table>" 경고를 warns에 추가한다.

    반환은 **verify_count의 actual**(실제 적재 행수 — 소스 카운트 기반)이다.
    written_rows는 Distributed 경로에서 신뢰 불가(단일노드에서 0, 다샤드에서 이중
    계상)라 마커 행수로 쓸 수 없다 — 텔레메트리로 로그에만 남긴다."""
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
    return actual


def run_m1(gate, date: str) -> dict:
    """M1 — mart.agg_token_model_cost_1d 1테이블. 반환 {"rows_mart": actual, "warns": [...]}
    (마커 rows_mart의 소스). 메트릭 fact가 없는 날도 토큰-only 행이 적재되므로 실행을 건너뛰지
    않는다(설계 §6.1 — 절대 FAILURE 아님)."""
    warns: list = []
    rows = _run_table(gate, date, f"{DB_MART}.{T_M1}_dist", f"{DB_MART}.{T_M1}_local",
                      SQL_M1, EXPECTED_SQL_M1, warns)
    return {"rows_mart": rows, "warns": warns}
