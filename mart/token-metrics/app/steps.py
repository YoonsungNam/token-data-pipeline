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
        FROM {DB_DIM}.dim_token_metrics_service_dist
        LIMIT 1 BY service)"""

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


# ============================================================================
# M3 token_metrics_check_1d — 데이터 품질 검사 (설계 §6.1 M3, §4.3, §5.3-3, §5.4; Plan 6c T4)
#
# 각 검사 = 독립 SELECT 블록 (check_name, select_sql). build_m3_sql()이 블록을
# "\nUNION ALL\n"으로 이어 INSERT를 만들고, build_m3_expected()가 **같은 블록 문자열**의
# count()를 EXPECTED로 쓴다(파생 오차 0 — tools/verify/invariants.sql의 블록 리스트 방식).
# 12컬럼 순서는 DDL 선언 순서(Plan 6a mart_metrics_tables.sql token_metrics_check_1d_local)로
# 고정 — _m3_select()만이 헤더를 만든다. 블록 내부 UNION ALL은 반드시 들여쓰기(4칸 이상)해서
# 최상위 조립 토큰 "\nUNION ALL\n"과 구분한다.
#
# detail 규약(마스터 §5.6 로그·검사 표 비노출): 수·이름(model/gpu_type/카운트)만 —
# 응답 원문(reported_*)·user_id·페이로드는 싣지 않는다.
# 메트릭 측 소스는 앵커가 있는 (date, service)만 읽는다(§6.1) — partial_load만 예외
# (앵커 없는 잔여물 자체가 검출 대상).
# ============================================================================

M3_COLUMNS = ("date", "service_group", "service", "check_name", "model", "gpu_type",
              "severity", "observed", "threshold", "detail", "source_type", "created_by")

# 앵커가 있는 서비스 집합 — 팩트 블록의 GLOBAL IN 우변
_M3_ANCHORED = f"(SELECT service FROM {SUB_ANCHOR})"

# 앵커 vs 자식 행수(partial_load) — serving은 표준 지표 행(metric != 'custom')만
# (Plan 6b NormalizeResult.n_serving과 동일 정의; custom_rows는 비교하지 않는다 — 설계 해석)
_M3_CHILD_COUNTS = f"""(
    SELECT service, any(service_group) AS service_group,
           sum(gpu_n) AS actual_gpu, sum(serving_n) AS actual_serving
    FROM
    (
        SELECT service, any(service_group) AS service_group, count() AS gpu_n, 0 AS serving_n
        FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist
        WHERE date = {{d:Date}}
        GROUP BY service
        UNION ALL
        SELECT service, any(service_group) AS service_group, 0 AS gpu_n,
               countIf(metric != 'custom') AS serving_n
        FROM {DB_FACT}.raw_token_metrics_serving_1d_dist
        WHERE date = {{d:Date}}
        GROUP BY service
    )
    GROUP BY service
)"""


def _m3_select(check_name: str, severity: str, *, service_group: str, service: str,
               observed: str, threshold: str, detail: str, body: str,
               model: str = "''", gpu_type: str = "''", source_type: str = "''") -> str:
    """12컬럼(DDL 순서) SELECT 헤더 + FROM 본문. 값 인자는 SQL 식 문자열이다."""
    if "'" in check_name or "'" in severity:
        raise ValueError(
            f"M3 check_name/severity must not contain a single quote: {check_name!r}/{severity!r}")
    if severity not in ("FAIL", "WARN", "INFO"):
        raise ValueError(f"M3 severity must be FAIL|WARN|INFO: {check_name}={severity}")
    return (
        "SELECT\n"
        "    {d:Date} AS date,\n"
        f"    {service_group} AS service_group,\n"
        f"    {service} AS service,\n"
        f"    '{check_name}' AS check_name,\n"
        f"    {model} AS model,\n"
        f"    {gpu_type} AS gpu_type,\n"
        f"    '{severity}' AS severity,\n"
        f"    toNullable(toFloat64({observed})) AS observed,\n"
        f"    toNullable(toFloat64({threshold})) AS threshold,\n"
        f"    {detail} AS detail,\n"
        f"    {source_type} AS source_type,\n"
        f"    '{CREATED_BY}' AS created_by\n"
        f"{body}"
    )


# --- 1) metrics_missing FAIL — reg 기대(enabled·coverage 유효)인데 앵커 부재 (§4.3 M0 기대 집합)
_M3_METRICS_MISSING = _m3_select(
    "metrics_missing", "FAIL",
    service_group="r.service_group", service="r.service",
    observed="0", threshold="1", detail="'no summary row'",
    body=f"""FROM {SUB_REG} AS r
GLOBAL LEFT JOIN {SUB_ANCHOR} AS an ON an.service = r.service
WHERE r.enabled = 1
  AND r.coverage_since <= {{d:Date}}
  AND (isNull(r.until) OR {{d:Date}} <= r.until)
  AND an.service = ''""")

# --- 2) partial_load FAIL — (a) 자식 행은 있으나 앵커 부재, (b) 앵커 카운트 ≠ 실제 자식 행수 (§5.4)
_M3_PARTIAL_LOAD = _m3_select(
    "partial_load", "FAIL",
    service_group="if(an.service != '', an.service_group, c.service_group)", service="k.service",
    observed="c.actual_gpu + c.actual_serving", threshold="an.gpu_rows + an.serving_rows",
    detail=("concat('gpu=', toString(c.actual_gpu), '/', toString(an.gpu_rows), "
            "' serving=', toString(c.actual_serving), '/', toString(an.serving_rows))"),
    source_type="an.source_type",
    body=f"""FROM
(
    SELECT service FROM {_M3_CHILD_COUNTS} AS cc
    UNION DISTINCT
    SELECT service FROM {SUB_ANCHOR} AS aa
) AS k
GLOBAL LEFT JOIN {_M3_CHILD_COUNTS} AS c ON c.service = k.service
GLOBAL LEFT JOIN {SUB_ANCHOR} AS an ON an.service = k.service
WHERE an.service = ''
   OR an.gpu_rows != c.actual_gpu
   OR an.serving_rows != c.actual_serving""")

# --- 3) rows_rejected WARN — 앵커 rejected_rows > 0 (§5.3-1 구조 거부 카운트)
_M3_ROWS_REJECTED = _m3_select(
    "rows_rejected", "WARN",
    service_group="an.service_group", service="an.service",
    observed="an.rejected_rows", threshold="0", detail="'rejected rows in summary'",
    source_type="an.source_type",
    body=f"""FROM {SUB_ANCHOR} AS an
WHERE an.rejected_rows > 0""")

# --- 4) unregistered_model WARN — gpu 팩트 모델이 alias 표에 없음(canon = 원문) (§4.2 alias 시드 규칙)
_M3_UNREGISTERED_MODEL = _m3_select(
    "unregistered_model", "WARN",
    service_group="x.service_group", service="x.service", model="x.canon_model",
    observed="x.n", threshold="0", detail="''", source_type="x.source_type",
    body=f"""FROM
(
    SELECT g.service, any(g.service_group) AS service_group,
           {canon('g.model')} AS canon_model, count() AS n, any(g.source_type) AS source_type
    FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = g.model
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN {_M3_ANCHORED} AND a.canonical = ''
    GROUP BY g.service, {canon('g.model')}
) AS x""")

# --- 5) hours_over_count FAIL — 행 플래그(§5.3-2, gpuHours > gpuCount×24) 집계 (service, canon, gpu_type)
_M3_HOURS_OVER_COUNT = _m3_select(
    "hours_over_count", "FAIL",
    service_group="x.service_group", service="x.service", model="x.canon_model", gpu_type="x.gpu_type",
    observed="x.hours", threshold="x.hours_cap",
    detail="concat('model=', x.canon_model, ' gpu_type=', x.gpu_type)", source_type="x.source_type",
    body=f"""FROM
(
    SELECT g.service, any(g.service_group) AS service_group,
           {canon('g.model')} AS canon_model, g.gpu_type,
           sum(g.gpu_hours) AS hours, sum(g.gpu_count) * 24 AS hours_cap,
           any(g.source_type) AS source_type
    FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = g.model
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN {_M3_ANCHORED}
      AND hasAny(g.flags, ['hours_over_count'])
    GROUP BY g.service, {canon('g.model')}, g.gpu_type
) AS x""")

# --- 6) unknown_violation FAIL — 정규화가 플래그한 미지 항목(§5.3-2): gpu·serving 팩트 양쪽, 모델 원문 그대로
_M3_UNKNOWN_VIOLATION = _m3_select(
    "unknown_violation", "FAIL",
    service_group="x.service_group", service="x.service", model="x.model", gpu_type="x.gpu_type",
    observed="x.n", threshold="0", detail="concat('part=', x.part)", source_type="x.source_type",
    body=f"""FROM
(
    SELECT service, any(service_group) AS service_group, model, gpu_type, part,
           count() AS n, any(source_type) AS source_type
    FROM
    (
        SELECT g.service, g.service_group, g.model, g.gpu_type, 'gpu' AS part, g.source_type
        FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
        WHERE g.date = {{d:Date}} AND g.service GLOBAL IN {_M3_ANCHORED}
          AND hasAny(g.flags, ['unknown_violation'])
        UNION ALL
        SELECT s.service, s.service_group, s.model, '' AS gpu_type, 'serving' AS part, s.source_type
        FROM {DB_FACT}.raw_token_metrics_serving_1d_dist AS s
        WHERE s.date = {{d:Date}} AND s.service GLOBAL IN {_M3_ANCHORED}
          AND hasAny(s.flags, ['unknown_violation'])
    )
    GROUP BY service, model, gpu_type, part
) AS x""")

# --- 7) pct_non_monotone FAIL — serving 행 플래그(§4.1 158 FAIL 플래그, p50>p90>... §5.3-2) 집계 (service, canon)
_M3_PCT_NON_MONOTONE = _m3_select(
    "pct_non_monotone", "FAIL",
    service_group="x.service_group", service="x.service", model="x.canon_model",
    observed="x.n", threshold="0", detail="concat('metrics=', x.metrics)", source_type="x.source_type",
    body=f"""FROM
(
    SELECT s.service, any(s.service_group) AS service_group,
           {canon('s.model')} AS canon_model, count() AS n,
           arrayStringConcat(arraySort(groupUniqArray(toString(s.metric))), ',') AS metrics,
           any(s.source_type) AS source_type
    FROM {DB_FACT}.raw_token_metrics_serving_1d_dist AS s
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = s.model
    WHERE s.date = {{d:Date}} AND s.service GLOBAL IN {_M3_ANCHORED}
      AND hasAny(s.flags, ['pct_non_monotone'])
    GROUP BY s.service, {canon('s.model')}
) AS x""")

# --- 8) gpu_type_no_tco WARN — 비용 계산 대상(serving/standby, FAIL 제외) gpu_type에 유효 TCO 없음 (§4.2 M1 cost NULL 사유)
_M3_GPU_TYPE_NO_TCO = _m3_select(
    "gpu_type_no_tco", "WARN",
    service_group="x.service_group", service="x.service", gpu_type="x.gpu_type",
    observed="x.hours", threshold="0", detail="'no effective tco'", source_type="x.source_type",
    body=f"""FROM
(
    SELECT g.service, any(g.service_group) AS service_group, g.gpu_type,
           sum(g.gpu_hours) AS hours, any(g.source_type) AS source_type
    FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    GLOBAL LEFT JOIN {SUB_EFF_TCO} AS t ON t.gpu_type = g.gpu_type
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN {_M3_ANCHORED}
      AND g.category IN ('serving', 'standby') AND NOT {FAIL_PRED} AND isNull(t.tco)
    GROUP BY g.service, g.gpu_type
) AS x""")

# --- 9) serving_missing_for_gpu_model WARN — gpu serving 행이 있는 (service, canon)에 serving 지표 행이 없고
#        token_usage_1d 요청은 있음 (§6.1 M4 share 분모 결손 사전 경고)
_M3_SERVING_MISSING_FOR_GPU_MODEL = _m3_select(
    "serving_missing_for_gpu_model", "WARN",
    service_group="gk.service_group", service="gk.service", model="gk.canon_model",
    observed="tk.requests", threshold="0", detail="'gpu serving row without serving metrics'",
    source_type="gk.source_type",
    body=f"""FROM
(
    SELECT g.service, any(g.service_group) AS service_group, {canon('g.model')} AS canon_model,
           any(g.source_type) AS source_type
    FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = g.model
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN {_M3_ANCHORED} AND g.category = 'serving'
    GROUP BY g.service, {canon('g.model')}
) AS gk
GLOBAL LEFT JOIN
(
    SELECT s.service, {canon('s.model')} AS canon_model, 1 AS has_rows
    FROM {DB_FACT}.raw_token_metrics_serving_1d_dist AS s
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = s.model
    WHERE s.date = {{d:Date}} AND s.metric != 'custom'
    GROUP BY s.service, {canon('s.model')}
) AS sk ON sk.service = gk.service AND sk.canon_model = gk.canon_model
GLOBAL LEFT JOIN
(
    SELECT u.service, {canon('u.model')} AS canon_model, sum(u.requests) AS requests
    {_TOK_SRC}
    {_TOK_TAIL}
) AS tk ON tk.service = gk.service AND tk.canon_model = gk.canon_model
WHERE sk.has_rows = 0 AND tk.requests > 0""")

# --- 10) serving_without_gpu_serving_row WARN — serving 지표 행은 있으나 gpu serving 행 없음 (reg expect_gpu=1인 서비스만)
_M3_SERVING_WITHOUT_GPU_SERVING_ROW = _m3_select(
    "serving_without_gpu_serving_row", "WARN",
    service_group="sk.service_group", service="sk.service", model="sk.canon_model",
    observed="1", threshold="0", detail="'serving metrics without gpu serving row'",
    source_type="sk.source_type",
    body=f"""FROM
(
    SELECT s.service, any(s.service_group) AS service_group, {canon('s.model')} AS canon_model,
           any(s.source_type) AS source_type
    FROM {DB_FACT}.raw_token_metrics_serving_1d_dist AS s
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = s.model
    WHERE s.date = {{d:Date}} AND s.service GLOBAL IN {_M3_ANCHORED} AND s.metric != 'custom'
    GROUP BY s.service, {canon('s.model')}
) AS sk
GLOBAL LEFT JOIN
(
    SELECT g.service, {canon('g.model')} AS canon_model, 1 AS has_rows
    FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    GLOBAL LEFT JOIN {SUB_EFF_ALIAS} AS a ON a.alias = g.model
    WHERE g.date = {{d:Date}} AND g.category = 'serving'
    GROUP BY g.service, {canon('g.model')}
) AS gk ON gk.service = sk.service AND gk.canon_model = sk.canon_model
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = sk.service
WHERE gk.has_rows = 0 AND r.expect_gpu = 1""")

# --- 11) identity_drift WARN — API 응답 자기신고(reported_*)가 헤더/레지스트리와 다름 (§5.3-3)
#         detail은 불일치 여부(0/1)만 — reported_* 원문은 싣지 않는다 (마스터 §5.6)
_M3_IDENTITY_DRIFT = _m3_select(
    "identity_drift", "WARN",
    service_group="r.service_group", service="an.service",
    observed="toUInt8(an.reported_service != an.service) + toUInt8(an.reported_service_group != r.service_group)",
    threshold="0",
    detail=("concat('svc_diff=', toString(toUInt8(an.reported_service != an.service)), "
            "' group_diff=', toString(toUInt8(an.reported_service_group != r.service_group)))"),
    source_type="an.source_type",
    body=f"""FROM {SUB_ANCHOR} AS an
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = an.service
WHERE an.source_type = 'metrics-api-v1'
  AND r.service != ''
  AND (an.reported_service != an.service OR an.reported_service_group != r.service_group)""")

# --- 12) service_not_in_usage_registry WARN — 메트릭 레지스트리 서비스가 token_usage 레지스트리에 없음 (§4.3 조인 키 전제)
_M3_SERVICE_NOT_IN_USAGE_REGISTRY = _m3_select(
    "service_not_in_usage_registry", "WARN",
    service_group="r.service_group", service="r.service",
    observed="1", threshold="0", detail="'not in dim_token_service'",
    body=f"""FROM {SUB_REG} AS r
WHERE r.enabled = 1
  AND r.coverage_since <= {{d:Date}}
  AND (isNull(r.until) OR {{d:Date}} <= r.until)
  AND r.service GLOBAL NOT IN {SUB_USAGE_SVC}""")

# --- 13) manual_source INFO — 앵커 source_type = 'manual-v0' (§5.2 수동 반입 표기, 정보성)
_M3_MANUAL_SOURCE = _m3_select(
    "manual_source", "INFO",
    service_group="an.service_group", service="an.service",
    observed="1", threshold="0", detail="'manual-v0'", source_type="an.source_type",
    body=f"""FROM {SUB_ANCHOR} AS an
WHERE an.source_type = 'manual-v0'""")

# 핵심 13블록 — 순서는 설계 §6.1 M3 표 순서 (T5 batch·문서가 이 순서를 인용)
M3_BLOCKS_CORE: list[tuple[str, str]] = [
    ("metrics_missing", _M3_METRICS_MISSING),
    ("partial_load", _M3_PARTIAL_LOAD),
    ("rows_rejected", _M3_ROWS_REJECTED),
    ("unregistered_model", _M3_UNREGISTERED_MODEL),
    ("hours_over_count", _M3_HOURS_OVER_COUNT),
    ("unknown_violation", _M3_UNKNOWN_VIOLATION),
    ("pct_non_monotone", _M3_PCT_NON_MONOTONE),
    ("gpu_type_no_tco", _M3_GPU_TYPE_NO_TCO),
    ("serving_missing_for_gpu_model", _M3_SERVING_MISSING_FOR_GPU_MODEL),
    ("serving_without_gpu_serving_row", _M3_SERVING_WITHOUT_GPU_SERVING_ROW),
    ("identity_drift", _M3_IDENTITY_DRIFT),
    ("service_not_in_usage_registry", _M3_SERVICE_NOT_IN_USAGE_REGISTRY),
    ("manual_source", _M3_MANUAL_SOURCE),
]

# 확장 블록 — T4 시점 비어 있음. T6(share 경고)·T7(gpu 그룹 경고)이 append한다.
M3_BLOCKS_STRETCH: list[tuple[str, str]] = []

M3_INSERT_COLUMNS = ", ".join(M3_COLUMNS)


def _m3_union(blocks: list[tuple[str, str]]) -> str:
    if not blocks:
        raise ValueError("build_m3: blocks must not be empty")
    return "\nUNION ALL\n".join(sql for _, sql in blocks)


def build_m3_sql(blocks: list[tuple[str, str]]) -> str:
    """블록 리스트 → INSERT INTO {DB_MART}.token_metrics_check_1d_dist (12컬럼) + UNION ALL 본문."""
    return f"INSERT INTO {DB_MART}.{T_M3}_dist ({M3_INSERT_COLUMNS})\n" + _m3_union(blocks)


def build_m3_expected(blocks: list[tuple[str, str]]) -> str:
    """같은 블록 문자열의 count() — INSERT 본문과 문자 단위로 동일한 UNION을 감싼다."""
    return "SELECT count() FROM (\n" + _m3_union(blocks) + "\n)"


SQL_M3_SUMMARY = f"""SELECT check_name, severity, count() AS n
FROM {DB_MART}.{T_M3}_dist
WHERE date = {{d:Date}}
GROUP BY check_name, severity
ORDER BY check_name, severity"""


def run_m3(gate, date: str, blocks: list[tuple[str, str]] | None = None) -> dict:
    """M3: 검사 블록 UNION ALL 적재 → 검사별 건수를 'CHECK WARN|INFO <check_name> severity=<sev> count=<n>'
    로 출력·warns에 추가. 검사 행이 있어도 STEP은 성공이다(FAIL은 severity 값일 뿐 — 실패 처리는
    _run_table의 verify 불일치·예외만). T5 batch는 'CHECK WARN ' 접두 라인 수를 warn= 마커에 센다."""
    if blocks is None:
        blocks = M3_BLOCKS_CORE + M3_BLOCKS_STRETCH
    warns: list[str] = []
    rows = _run_table(gate, date, f"{DB_MART}.{T_M3}_dist", f"{DB_MART}.{T_M3}_local",
                      build_m3_sql(blocks), build_m3_expected(blocks), warns)
    for check_name, severity, n in gate.query(SQL_M3_SUMMARY, {"d": date}):
        level = "INFO" if severity == "INFO" else "WARN"
        line = f"CHECK {level} {check_name} severity={severity} count={int(n)}"
        print(line, flush=True)
        warns.append(line)
    return {"rows_check": rows, "warns": warns}


# ============================================================================
# M4 agg_token_model_share_1d — 공유 모델 비용 배분 (설계 §6.1 M4, §6.4 (3)~(6); Plan 6c T6)
#
# grain: date × model(canon) × service × provider_service. 행 = (그날 그 모델에 토큰이 있는
# usage_svc 서비스: wt) ∪ (제공자 후보 전부: prov_rows — 다중이면 후보별 행, share NULL).
# 모델 단위 판정(mode CTE)은 설계 §6.4 (4) 순서로 고정:
#   n_prov >= 2                       → provider_ambiguous (후보 행 is_provider=1, share·allocated NULL)
#   n_prov = 0 AND gpu 행 있음(test뿐) → no_provider        (C=0, allocated 0, share는 정보용)
#   n_prov = 0 AND gpu 행 전혀 없음    → external_api       (벤더 단가 ③ / 1e6, tier='standard')
#   W(m) = 0 AND C > 0                → token_not_reported (제공자 행 share=1 전액, I8)
#   usage_includes_consumers = 1      → provider_reported  (C4: D = max(W(p), Σ_{s≠p}W(s));
#                                        소비자 share = W(s)/D, 제공자 자기분 share =
#                                        max(W(p)−Σ_{s≠p}W(s), 0)/D → Σ share = 1, Σ 배분 = C —
#                                        이 모드에서 예외 없이 I3 성립)
#   기본                               → all_services       (W(m)=Σ_s W(s,m), 정의서 3.6)
# W(p)=0 이고 소비자 토큰 > 0이면 mode는 provider_reported로 남고(w_m = Σ 소비자 > 0) 소비자가 C
# 전액을 자기 비중대로 가져간다 — 브리프 규칙 4의 'provider_reported·W(p)=0 특례(소비자 share NULL)'는
# C4로 대체(설계 해석; M3 consumer_tokens_exceed_provider가 그 날을 표시).
# C(m)은 같은 배치에서 선행 적재된 M1 제공자 행(has_gpu_rows=1)의 model_cost_krw를 읽는다
# (설계 해석 — TCO 재계산 없음: 실행 순서 M1 → M3 → M4는 batch.RUNNERS가 보장).
# quality_flag 우선순위: partial > no_tco > provider_ambiguous > vendor_price_missing
#                       > token_not_reported > normal (partial/no_tco = M1 제공자 행 값 상속).
# ============================================================================

# wt — (service, model) 가중 토큰 + 토큰 4성분(외부 API 단가식용). 모집단 = usage_svc(_TOK_TAIL).
_M4_WT = f"""SELECT u.service                     AS service,
           any(u.service_group)          AS service_group,
           {canon('u.model')}            AS model,
           sum(u.input_tokens)           AS input_tokens,
           sum(u.cache_read_tokens)      AS cache_read_tokens,
           sum(u.cache_creation_tokens)  AS cache_creation_tokens,
           sum(u.output_tokens)          AS output_tokens,
           {_WTOK_EXPR}                  AS wtok
    {_TOK_SRC}
    {_TOK_TAIL}"""

# wt_total — 모델별 Σ_s W(s,m) (all_services 분모)
_M4_WT_TOTAL = f"""SELECT model, sum(wtok) AS w_all
    FROM
    (
        {_M4_WT}
    )
    GROUP BY model"""

# prov_rows — 제공자 후보 (model, service): 앵커 서비스의 FAIL 없는 serving/standby gpu 행 (C>0 성립 행)
_M4_PROV_ROWS = f"""SELECT {canon('g.model')} AS model, g.service AS service
    {_GPU_SRC}
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN (SELECT service FROM {SUB_ANCHOR})
      AND g.category IN ('serving', 'standby') AND NOT {FAIL_PRED}
    GROUP BY g.service, {canon('g.model')}"""

# prov — 모델별 후보 배열·수·단일 제공자(다중/0이면 '')
_M4_PROV = f"""SELECT model,
           arraySort(groupUniqArray(service))  AS providers,
           length(providers)                   AS n_prov,
           if(n_prov = 1, providers[1], '')    AS provider
    FROM
    (
        {_M4_PROV_ROWS}
    )
    GROUP BY model"""

# gpu_any — 그날 gpu 행이 하나라도 있는 모델(카테고리·FAIL 무관): no_provider vs external_api 판별
_M4_GPU_ANY = f"""SELECT {canon('g.model')} AS model, 1 AS has_gpu
    {_GPU_SRC}
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN (SELECT service FROM {SUB_ANCHOR})
    GROUP BY {canon('g.model')}"""

# vendor — 모델별 벤더 단가 1행(provider 최소값으로 고정 — (provider, model) 다중 등록 시 fan-out 방지).
# 단가 NULL은 -1 sentinel로 argMin을 통과시켜 NULL 그대로 돌려준다(SUB_EFF_* 규약과 동일).
# 별칭은 `vendor`(≠ 소스 컬럼 provider) — `AS provider`로 두면 같은 SELECT의 argMin(…, provider)가
# 컬럼 대신 집계 별칭을 가리켜(prefer_column_name_to_alias=0) 중첩 집계 오류가 난다.
_M4_VENDOR = f"""(SELECT model,
               min(provider)                                        AS vendor,
               nullIf(argMin(ifNull(p_in, -1), provider), -1)       AS p_in,
               nullIf(argMin(ifNull(p_cached, -1), provider), -1)   AS p_cached,
               nullIf(argMin(ifNull(p_cc, -1), provider), -1)       AS p_cc,
               nullIf(argMin(ifNull(p_out, -1), provider), -1)      AS p_out,
               1                                                    AS has_price
        FROM {SUB_EFF_PRICE} AS ep
        GROUP BY model)"""

# m1c — 같은 배치 M1 제공자 행: C(m)·품질 플래그 (has_gpu_rows=1 행만)
_M4_M1C = f"""SELECT model, service, model_cost_krw, quality_flag, 1 AS has_m1
    FROM {DB_MART}.{T_M1}_dist
    WHERE date = {{d:Date}} AND has_gpu_rows = 1"""

# 공통 CTE 블록 — SQL_M4와 EXPECTED_SQL_M4가 문자 단위로 공유(파생 오차 0). keys는 INSERT만 붙인다.
_M4_CTES = f"""WITH
    wt AS (
        {_M4_WT}
    ),
    wt_total AS (
        {_M4_WT_TOTAL}
    ),
    prov AS (
        {_M4_PROV}
    ),
    gpu_any AS (
        {_M4_GPU_ANY}
    ),
    m1c AS (
        {_M4_M1C}
    ),
    models AS (
        SELECT model FROM wt
        UNION DISTINCT
        SELECT model FROM prov
    ),
    mode AS (
        -- 모델 단위 판정(모듈 상단 주석 순서). 미스 값: n_prov 0, has_gpu 0, w_all 0, uic 0, C NULL.
        SELECT m.model                                   AS model,
               p.n_prov                                  AS n_prov,
               p.provider                                AS provider,
               ga.has_gpu                                AS has_gpu,
               mt.w_all                                  AS w_all,
               wp.wtok                                   AS w_prov,
               r.usage_includes_consumers                AS uic,
               if(uic = 1, greatest(w_prov, w_all - w_prov), w_all)  AS w_m,
               -- C4: 비용 보존 분모 = max(W(p), Σ소비자)
               mc.model_cost_krw                         AS model_cost_krw,
               v.vendor                                  AS vendor,
               v.p_in                                    AS p_in,
               v.p_cached                                AS p_cached,
               v.p_cc                                    AS p_cc,
               v.p_out                                   AS p_out,
               multiIf(n_prov >= 2,                                 'provider_ambiguous',
                       n_prov = 0 AND has_gpu = 1,                  'no_provider',
                       n_prov = 0,                                  'external_api',
                       w_m = 0 AND ifNull(model_cost_krw, 0) > 0,   'token_not_reported',
                       uic = 1,                                     'provider_reported',
                       'all_services')                   AS denominator_mode
        FROM models AS m
        GLOBAL LEFT JOIN prov AS p ON p.model = m.model
        GLOBAL LEFT JOIN gpu_any AS ga ON ga.model = m.model
        GLOBAL LEFT JOIN wt_total AS mt ON mt.model = m.model
        GLOBAL LEFT JOIN wt AS wp ON wp.model = p.model AND wp.service = p.provider
        GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = p.provider
        GLOBAL LEFT JOIN m1c AS mc ON mc.model = p.model AND mc.service = p.provider
        GLOBAL LEFT JOIN {_M4_VENDOR} AS v ON v.model = m.model
    )"""

# 키 조각 — SQL_M4 keys(UNION DISTINCT)와 EXPECTED_SQL_M4(UNION ALL + uniqExact) 공유.
# wt 행의 provider_service: 단일 제공자면 p, external_api면 벤더 표기(없으면 ''), 그 외 ''.
_M4_WT_KEYS = """SELECT w.model AS model, w.service AS service,
           multiIf(md.n_prov = 1, md.provider,
                   md.denominator_mode = 'external_api', md.vendor,
                   '')                                          AS provider_service,
           toUInt8(md.n_prov = 1 AND w.service = md.provider)   AS is_provider
    FROM wt AS w
    GLOBAL LEFT JOIN mode AS md ON md.model = w.model"""
_M4_PROV_KEYS = f"""SELECT model, service, service AS provider_service, toUInt8(1) AS is_provider
    FROM
    (
        {_M4_PROV_ROWS}
    )"""

SQL_M4 = f"""
INSERT INTO {DB_MART}.{T_M4}_dist
    (date, model, service, service_group, provider_service, is_provider, denominator_mode,
     service_wtokens, model_total_wtokens, share, model_cost_krw, allocated_cost_krw,
     quality_flag, created_by)
{_M4_CTES},
    keys AS (
        {_M4_WT_KEYS}
        UNION DISTINCT
        {_M4_PROV_KEYS}
    )
SELECT
    {{d:Date}}                                                        AS date,
    k.model                                                           AS model,
    k.service                                                         AS service,
    multiIf(r.service_group != '', r.service_group,
            an.service_group != '', an.service_group,
            w.service_group)                                          AS service_group,
    k.provider_service                                                AS provider_service,
    k.is_provider                                                     AS is_provider,
    md.denominator_mode                                               AS denominator_mode,
    -- provider_reported 제공자 자기분 = max(W(p) − Σ 소비자 W(s), 0) = max(2·W(p) − W_all, 0)
    if(k.is_provider = 1 AND md.denominator_mode = 'provider_reported',
       greatest(w.wtok - (md.w_all - w.wtok), 0.0), w.wtok)           AS service_wtokens,
    md.w_m                                                            AS model_total_wtokens,
    multiIf(md.denominator_mode = 'provider_ambiguous', NULL,
            md.denominator_mode = 'token_not_reported', if(k.is_provider = 1, 1.0, NULL),
            md.w_m = 0, NULL,
            service_wtokens / md.w_m)                                 AS share,
    multiIf(md.denominator_mode = 'no_provider', toNullable(0.0),
            mc.has_m1 = 1, mc.model_cost_krw,
            NULL)                                                     AS model_cost_krw,
    multiIf(md.denominator_mode = 'external_api',
                (w.input_tokens * md.p_in + w.cache_read_tokens * md.p_cached
                 + w.cache_creation_tokens * md.p_cc + w.output_tokens * md.p_out) / 1e6,
            md.denominator_mode = 'provider_ambiguous', NULL,
            md.denominator_mode = 'no_provider', toNullable(0.0),
            md.denominator_mode = 'token_not_reported', if(k.is_provider = 1, model_cost_krw, NULL),
            model_cost_krw * share)                                   AS allocated_cost_krw,
    -- 우선순위 고정(설계 §6.1 M4): partial > no_tco > provider_ambiguous > vendor_price_missing
    --                             > token_not_reported > normal
    multiIf(
        mc.quality_flag = 'partial',                                              'partial',
        mc.quality_flag = 'no_tco',                                               'no_tco',
        md.denominator_mode = 'provider_ambiguous',                               'provider_ambiguous',
        md.denominator_mode = 'external_api' AND isNull(allocated_cost_krw),      'vendor_price_missing',
        md.denominator_mode = 'token_not_reported',                               'token_not_reported',
        'normal')                                                     AS quality_flag,
    '{CREATED_BY}'                                                    AS created_by
FROM keys AS k
GLOBAL LEFT JOIN mode AS md ON md.model = k.model
GLOBAL LEFT JOIN wt AS w ON w.model = k.model AND w.service = k.service
GLOBAL LEFT JOIN m1c AS mc ON mc.model = k.model AND mc.service = k.provider_service
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = k.service
GLOBAL LEFT JOIN {SUB_ANCHOR} AS an ON an.service = k.service
"""

EXPECTED_SQL_M4 = f"""
{_M4_CTES}
SELECT uniqExact((model, service, provider_service)) FROM (
    {_M4_WT_KEYS}
    UNION ALL
    {_M4_PROV_KEYS}
)
"""
# ↑ M4 행 그레인은 date×model×service×provider_service — keys(UNION DISTINCT)의 distinct 키 수.
# 좌측 keys에 붙는 mode(GROUP BY model 유니크)·wt(GROUP BY service, model)·m1c(M1 그레인
# date×service×model)·reg·anchor(service 유니크)는 전부 키 유니크라 fan-out이 없다.


def run_m4(gate, date: str) -> dict:
    """M4 — mart.agg_token_model_share_1d 1테이블. 반환 {"rows_share": actual, "warns": [...]}
    (마커 rows_share의 소스). 토큰 mart 부재일(M0b token_mart_absent)은 batch가 이 러너를
    건너뛰고 rows_share=0을 기록한다 — 여기서는 판단하지 않는다(설계 §6.1 M0b)."""
    warns: list = []
    rows = _run_table(gate, date, f"{DB_MART}.{T_M4}_dist", f"{DB_MART}.{T_M4}_local",
                      SQL_M4, EXPECTED_SQL_M4, warns)
    return {"rows_share": rows, "warns": warns}


# ============================================================================
# M3 stretch — share 경고 3블록 (설계 §6.1 M3 stretch, §6.4 (4)(6); Plan 6c T6)
#   M4와 같은 조각(_M4_PROV/_M4_WT/_M4_WT_TOTAL/_M4_GPU_ANY/_M4_VENDOR)을 쓴다 — M4 판정과
#   검사 검출이 문자 단위로 같은 집합을 본다. 3블록 모두 모델 단위(model 컬럼 채움, gpu_type '').
# ============================================================================

# --- 14) provider_ambiguous WARN — 제공자 후보 다중 모델(M4 후보별 행·share NULL·배부 보류)
_M3_PROVIDER_AMBIGUOUS = _m3_select(
    "provider_ambiguous", "WARN",
    service_group="''", service="''", model="p.model",
    observed="p.n_prov", threshold="1",
    detail="concat('model=', p.model, ' providers=', toString(p.n_prov))",
    body=f"""FROM
(
    {_M4_PROV}
) AS p
WHERE p.n_prov >= 2""")

# --- 15) vendor_price_missing WARN — external_api 모델(gpu 행 전무·토큰 사용 있음) 중 유효 단가 부재/NULL
#         no_provider(test 전용 gpu 행) 모델은 gpu_any에 잡혀 발화하지 않는다(설계 §6.4 (4)).
_M3_VENDOR_PRICE_MISSING = _m3_select(
    "vendor_price_missing", "WARN",
    service_group="''", service="''", model="x.model",
    observed="1", threshold="0", detail="concat('model=', x.model)",
    body=f"""FROM
(
    SELECT {canon('u.model')} AS model
    {_TOK_SRC}
    WHERE u.date = {{d:Date}} AND u.service GLOBAL IN {SUB_USAGE_SVC}
    GROUP BY {canon('u.model')}
) AS x
GLOBAL LEFT JOIN
(
    {_M4_GPU_ANY}
) AS ga ON ga.model = x.model
GLOBAL LEFT JOIN {_M4_VENDOR} AS v ON v.model = x.model
WHERE ga.has_gpu = 0
  AND (v.has_price = 0 OR isNull(v.p_in) OR isNull(v.p_cached) OR isNull(v.p_cc) OR isNull(v.p_out))""")

# --- 16) consumer_tokens_exceed_provider WARN — provider_reported(usage_includes_consumers=1) 모델에서
#         Σ_{s≠p} W(s,m) > W(p,m) (제공자 자기분 0 클램프 발생 — 설계 §6.4 (4) 분모 모드 보정)
_M3_CONSUMER_TOKENS_EXCEED_PROVIDER = _m3_select(
    "consumer_tokens_exceed_provider", "WARN",
    service_group="r.service_group", service="p.provider", model="p.model",
    observed="t.w_all - wp.wtok", threshold="wp.wtok",
    detail="concat('model=', p.model)",
    body=f"""FROM
(
    {_M4_PROV}
) AS p
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = p.provider
GLOBAL LEFT JOIN
(
    {_M4_WT_TOTAL}
) AS t ON t.model = p.model
GLOBAL LEFT JOIN
(
    {_M4_WT}
) AS wp ON wp.model = p.model AND wp.service = p.provider
WHERE p.n_prov = 1 AND r.usage_includes_consumers = 1 AND (t.w_all - wp.wtok) > wp.wtok""")

M3_BLOCKS_STRETCH.extend([
    ("provider_ambiguous", _M3_PROVIDER_AMBIGUOUS),
    ("vendor_price_missing", _M3_VENDOR_PRICE_MISSING),
    ("consumer_tokens_exceed_provider", _M3_CONSUMER_TOKENS_EXCEED_PROVIDER),
])


# ============================================================================
# M2 agg_token_gpu_group_1d — 그룹 귀속·유휴 (설계 §6.1 M2, §6.4 (2)(7); 정의서 3.1/3.3/3.4, I1/I2; Plan 6c T7)
#
# grain: date × service_group × gpu_type (쿼터 보유 단위). 행 = grp 키(앵커 서비스의 gpu 행이 있는
# (그룹, 기종)) ∪ alloc 키(`unknown` 아닌 date 유효 할당 행 AND 그 그룹에 앵커 서비스 ≥ 1) — UNION DISTINCT.
# 비용은 M1을 읽지 않고 fact 시간 × 그 기종 TCO를 outer에서 직접 곱한다(그레인이 기종 단위라
# Σ_model (serving+standby)×TCO = 그룹 (serving+standby)×TCO — M1 합과 같되 M1의 "기종 하나라도 NULL이면
# 모델 C NULL" 규칙과 무관하게 기종별로 닫힌다; 설계 해석 T7-1). TCO NULL이면 Nullable 산술로
# 비용 6컬럼(group_total/model_cost_sum/test_cost/idle_cost/unattributed/identity_gap)이 전부 NULL.
#   allocated_gpu_hours = allocated_gpu_count × 24 (할당 행 없음 → NULL)
#   idle_gpu_hours      = greatest(allocated − reported_total, 0)  (I1 클램프; over_report = reported > allocated)
#   identity_gap_krw    = group_total − model_cost_sum − test_cost − idle_cost − unattributed  (I2, 적재 컬럼끼리 계산)
#   unattributed_cost_krw = (flagged + other) × TCO — other = 비FAIL 행 중 category ∉ {serving,standby,test} (설계 해석 D3-2: I2 항등 유지)
# 참조 구현 app/mart.py group_overhead()와 같은 규칙 — tests/test_mart.py가 대조한다.
# ============================================================================

# gpu fact(앵커 서비스만) → (그룹, 기종) 집계 — grp CTE와 키 조각이 같은 꼬리를 공유
_M2_GPU_TAIL = f"""FROM {DB_FACT}.raw_token_metrics_gpu_1d_dist AS g
    WHERE g.date = {{d:Date}} AND g.service GLOBAL IN {_M3_ANCHORED}
    GROUP BY g.service_group, g.gpu_type"""

# 할당 행(unknown 제외, date 유효 최신) 중 앵커 서비스가 1개 이상인 그룹만
_M2_ALLOC_TAIL = f"""FROM {SUB_EFF_ALLOC} AS al
    WHERE al.service_group GLOBAL IN (SELECT service_group FROM {SUB_ANCHOR})"""

# 키 조각 — SQL_M2의 keys(UNION DISTINCT)와 EXPECTED_SQL_M2(UNION ALL + uniqExact) 공유
_M2_GPU_KEYS = f"""SELECT g.service_group AS service_group, g.gpu_type AS gpu_type
    {_M2_GPU_TAIL}"""
_M2_ALLOC_KEYS = f"""SELECT al.service_group AS service_group, al.gpu_type AS gpu_type
    {_M2_ALLOC_TAIL}"""

# grp — 시간 5분류(그룹 합): serving/standby/test는 비FAIL 행만, reported_total은 플래그 포함 전체,
# flagged는 FAIL 행 전체(카테고리 무관), other는 비FAIL 행 중 category가 serving/standby/test 어디에도
# 속하지 않는 나머지(R1: unattributed_cost_krw = (flagged + other) × TCO — I2 항등 유지).
# M3 no_allocation/sum_hours_over_allocation 블록도 같은 조각을 쓴다.
_M2_GRP = f"""SELECT g.service_group                                                  AS service_group,
           g.gpu_type                                                       AS gpu_type,
           sumIf(g.gpu_hours, g.category = 'serving' AND NOT {FAIL_PRED})   AS serving_gpu_hours,
           sumIf(g.gpu_hours, g.category = 'standby' AND NOT {FAIL_PRED})   AS standby_gpu_hours,
           sumIf(g.gpu_hours, g.category = 'test' AND NOT {FAIL_PRED})      AS test_gpu_hours,
           sum(g.gpu_hours)                                                 AS reported_gpu_hours_total,
           sumIf(g.gpu_hours, {FAIL_PRED})                                  AS flagged_gpu_hours,
           sumIf(g.gpu_hours, g.category NOT IN ('serving', 'standby', 'test') AND NOT {FAIL_PRED})
                                                                             AS other_gpu_hours,
           count()                                                          AS gpu_rows
    {_M2_GPU_TAIL}"""

SQL_M2 = f"""
INSERT INTO {DB_MART}.{T_M2}_dist
    (date, service_group, gpu_type,
     allocated_gpu_hours, group_total_cost_krw,
     serving_gpu_hours, standby_gpu_hours, test_gpu_hours, reported_gpu_hours_total, flagged_gpu_hours,
     model_cost_sum_krw, test_cost_krw, idle_gpu_hours, idle_cost_krw, unattributed_cost_krw,
     identity_gap_krw, utilization, over_report, equiv_gpu_count, tco_missing,
     allocation_source, quality_flag, created_by)
WITH
    grp AS (
        {_M2_GRP}
    ),
    keys AS (
        {_M2_GPU_KEYS}
        UNION DISTINCT
        {_M2_ALLOC_KEYS}
    )
SELECT
    {{d:Date}}                                                        AS date,
    k.service_group                                                   AS service_group,
    k.gpu_type                                                        AS gpu_type,
    al.allocated_gpu_count * 24                                       AS allocated_gpu_hours,
    allocated_gpu_hours * t.tco                                       AS group_total_cost_krw,
    gp.serving_gpu_hours                                              AS serving_gpu_hours,
    gp.standby_gpu_hours                                              AS standby_gpu_hours,
    gp.test_gpu_hours                                                 AS test_gpu_hours,
    gp.reported_gpu_hours_total                                       AS reported_gpu_hours_total,
    gp.flagged_gpu_hours                                              AS flagged_gpu_hours,
    (serving_gpu_hours + standby_gpu_hours) * t.tco                   AS model_cost_sum_krw,
    test_gpu_hours * t.tco                                            AS test_cost_krw,
    if(isNull(allocated_gpu_hours), NULL,
       greatest(allocated_gpu_hours - reported_gpu_hours_total, 0))   AS idle_gpu_hours,
    idle_gpu_hours * t.tco                                            AS idle_cost_krw,
    (flagged_gpu_hours + gp.other_gpu_hours) * t.tco                  AS unattributed_cost_krw,
    group_total_cost_krw - model_cost_sum_krw - test_cost_krw - idle_cost_krw - unattributed_cost_krw
                                                                      AS identity_gap_krw,
    if(isNull(allocated_gpu_hours) OR allocated_gpu_hours = 0, NULL,
       reported_gpu_hours_total / allocated_gpu_hours)                AS utilization,
    toUInt8(ifNull(reported_gpu_hours_total > allocated_gpu_hours, 0)) AS over_report,
    reported_gpu_hours_total / 24                                     AS equiv_gpu_count,
    toUInt8(isNull(t.tco))                                            AS tco_missing,
    al.source                                                         AS allocation_source,
    -- 우선순위 고정(설계 해석 T7-2): over_report > no_tco > no_allocation > flagged > normal
    multiIf(
        over_report = 1,                 'over_report',
        tco_missing = 1,                 'no_tco',
        isNull(allocated_gpu_hours),     'no_allocation',
        flagged_gpu_hours > 0,           'flagged',
        'normal')                                                     AS quality_flag,
    '{CREATED_BY}'                                                    AS created_by
FROM keys AS k
GLOBAL LEFT JOIN grp AS gp ON gp.service_group = k.service_group AND gp.gpu_type = k.gpu_type
GLOBAL LEFT JOIN {SUB_EFF_ALLOC} AS al ON al.service_group = k.service_group AND al.gpu_type = k.gpu_type
GLOBAL LEFT JOIN {SUB_EFF_TCO} AS t ON t.gpu_type = k.gpu_type
"""

EXPECTED_SQL_M2 = f"""
SELECT uniqExact((service_group, gpu_type)) FROM (
    {_M2_GPU_KEYS}
    UNION ALL
    {_M2_ALLOC_KEYS}
)
"""
# ↑ grp(GROUP BY 키 유니크)·eff_alloc(GROUP BY service_group, gpu_type)·eff_tco(GROUP BY gpu_type)는
# 전부 키 유니크라 keys 좌측에 fan-out이 없다 — 적재 행수 = keys의 distinct 키 수.


def run_m2(gate, date: str) -> dict:
    """M2 — mart.agg_token_gpu_group_1d 1테이블. 반환 {"rows_group": actual, "warns": [...]}.
    rows_group는 마커 필드가 아니다(Plan 6a H 고정) — batch.py가 로그 `M2 rows_group=<n>`만 남긴다.
    gpu 행도 할당 행도 없는 날은 0행 적재(expected 0 = actual 0)로 성공 — 절대 FAILURE 아님."""
    warns: list = []
    rows = _run_table(gate, date, f"{DB_MART}.{T_M2}_dist", f"{DB_MART}.{T_M2}_local",
                      SQL_M2, EXPECTED_SQL_M2, warns)
    return {"rows_group": rows, "warns": warns}


# ============================================================================
# M3 stretch — 그룹·앵커 4블록 (설계 §6.1 M3 stretch, §6.4 (2) I1; Plan 6c T7)
#   17·18은 M2와 같은 조각(_M2_GRP·SUB_EFF_ALLOC)을 써서 M2 quality_flag 판정과 문자 단위로 같은 집합을
#   본다(그룹 단위 — service '', gpu_type 채움). 19·20은 앵커(summary) × 레지스트리 기대(expect_*)
#   — 서비스 단위(service 채움, gpu_type ''). 4블록 모두 model ''. detail은 이름·수만(§5.6).
# ============================================================================

# --- 17) no_allocation WARN — gpu 행이 있는 (그룹, 기종)에 date 유효 할당(unknown 제외)이 없거나 NULL
#         (= M2 allocated_gpu_hours NULL·quality_flag no_allocation과 같은 술어 isNull(al.allocated_gpu_count))
_M3_NO_ALLOCATION = _m3_select(
    "no_allocation", "WARN",
    service_group="x.service_group", service="''", gpu_type="x.gpu_type",
    observed="x.reported_gpu_hours_total", threshold="0",
    detail="concat('gpu_type=', x.gpu_type)",
    body=f"""FROM
(
    {_M2_GRP}
) AS x
GLOBAL LEFT JOIN {SUB_EFF_ALLOC} AS al ON al.service_group = x.service_group AND al.gpu_type = x.gpu_type
WHERE isNull(al.allocated_gpu_count)""")

# --- 18) sum_hours_over_allocation FAIL — 보고 합(플래그 포함) > 할당 × 24 (I1 idle < 0 → M2 over_report=1·idle 0 클램프)
_M3_SUM_HOURS_OVER_ALLOCATION = _m3_select(
    "sum_hours_over_allocation", "FAIL",
    service_group="x.service_group", service="''", gpu_type="x.gpu_type",
    observed="x.reported_gpu_hours_total", threshold="al.allocated_gpu_count * 24",
    detail="concat('gpu_type=', x.gpu_type)",
    body=f"""FROM
(
    {_M2_GRP}
) AS x
GLOBAL LEFT JOIN {SUB_EFF_ALLOC} AS al ON al.service_group = x.service_group AND al.gpu_type = x.gpu_type
WHERE x.reported_gpu_hours_total > al.allocated_gpu_count * 24""")

# --- 19) gpu_block_empty_unexpected WARN — 앵커는 있는데 gpu 블록 0행이고 레지스트리가 expect_gpu=1
_M3_GPU_BLOCK_EMPTY_UNEXPECTED = _m3_select(
    "gpu_block_empty_unexpected", "WARN",
    service_group="an.service_group", service="an.service",
    observed="an.gpu_rows", threshold="1", detail="'expect_gpu=1'", source_type="an.source_type",
    body=f"""FROM {SUB_ANCHOR} AS an
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = an.service
WHERE r.expect_gpu = 1 AND an.gpu_rows = 0""")

# --- 20) serving_block_empty_unexpected WARN — 앵커는 있는데 serving 블록 0행이고 레지스트리가 expect_serving=1
_M3_SERVING_BLOCK_EMPTY_UNEXPECTED = _m3_select(
    "serving_block_empty_unexpected", "WARN",
    service_group="an.service_group", service="an.service",
    observed="an.serving_rows", threshold="1", detail="'expect_serving=1'", source_type="an.source_type",
    body=f"""FROM {SUB_ANCHOR} AS an
GLOBAL LEFT JOIN {SUB_REG} AS r ON r.service = an.service
WHERE r.expect_serving = 1 AND an.serving_rows = 0""")

M3_BLOCKS_STRETCH.extend([
    ("no_allocation", _M3_NO_ALLOCATION),
    ("sum_hours_over_allocation", _M3_SUM_HOURS_OVER_ALLOCATION),
    ("gpu_block_empty_unexpected", _M3_GPU_BLOCK_EMPTY_UNEXPECTED),
    ("serving_block_empty_unexpected", _M3_SERVING_BLOCK_EMPTY_UNEXPECTED),
])
# ↑ 20블록 완성: core 13 + stretch 7(T6 3 + T7 4). run_m3 기본 = M3_BLOCKS_CORE + M3_BLOCKS_STRETCH.
