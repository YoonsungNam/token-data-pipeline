-- =============================================================
-- 메트릭 파이프라인(/v1/metrics 반입, Plan 6a~6c) 검증 불변식 — 설계 §7.3
-- tools/verify/invariants.sql(토큰 파이프라인)의 메트릭판. 기존 파일은 손대지 않고
-- run_invariants.py --sql 로 이 파일을 지정해 실행한다.
--
-- 실데이터 검증이므로 고정 기대값이 아니라 불변식으로 판정한다 —
-- tools/verify/invariants.sql·mart/token-usage/tests/e2e/verify_expected_results.sql과
-- 동일 패턴: **빈 출력 = 전건 통과.** 어떤 SELECT든 1행이라도 나오면 그 행이 위반이다.
--
-- 치환 토큰 (run_invariants.py가 render()로 치환, 잔존 시 실행 실패) — 기존 4종만 쓴다:
--   {FACT}  — 메트릭 fact DB명 (기본 fact,     1단계 격리 시 token_verify_fact)
--             raw_token_metrics_gpu_1d / raw_token_metrics_serving_1d / raw_token_metrics_summary_1d
--   {DIM}   — 메트릭 dim  DB명 (기본 gpu_data, 1단계 격리 시 token_verify_dim)
--             dim_token_model_alias / dim_token_gpu_tco
--   {MART}  — 메트릭 mart DB명 (기본 mart,     1단계 격리 시 token_verify_mart)
--             agg_token_model_cost_1d / token_metrics_check_1d /
--             agg_token_model_share_1d / agg_token_gpu_group_1d
--   {DATE}  — 대상일 'YYYY-MM-DD' (단일 날짜, 리터럴 문자열로 치환)
--   토큰 측 DB 토큰은 없다 — 이 파일은 메트릭 파이프라인의 신규 테이블만 읽는다.
--
-- 사용법 (GitHub 체크아웃 기준):
--   python3 tools/verify/run_invariants.py --sql tools/verify/invariants_metrics.sql --date 2026-09-03
--   사내 분기본 run_invariants.py에는 --sql 이 없다 — 이 파일은 GitHub본 러너로만 실행한다.
--   (또는 치환 후 clickhouse-client --multiquery로 직접 실행 — 읽기 전용 SELECT만)
--
-- 컬럼 계약: 전 SELECT가 3컬럼(check_name String, detail String, bad_count UInt64)
-- 로 통일 — bad_count는 전부 toUInt64(...), detail은 전부 String(리터럴 또는
-- concat/toString). UNION ALL 체인에서 타입이 갈라지면 NO_COMMON_TYPE으로 실패한다.
-- 단일 집계 블록은 HAVING count() > 0 으로 위반 0건일 때 행 자체를 없앤다.
--
-- 분산 규약(설계 §4.0): _dist 대상 서브쿼리/조인은 GLOBAL IN / GLOBAL NOT IN /
-- GLOBAL LEFT JOIN 을 명시한다(러너의 distributed_product_mode=global 과 이중 안전).
-- join_use_nulls=0 규약: LEFT JOIN 미스는 ''/0, Nullable 컬럼은 NULL.
-- NULL 처리는 ifNull / isNull / isNotNull / nullIf 만 쓴다(Plan 6c 공통 규칙).
-- detail에는 서비스·모델·기종·그룹명과 집계값만 싣는다 — 사용자 식별자·페이로드 원문 0
-- (마스터 §5.6; 메트릭 테이블에는 사용자 식별 컬럼 자체가 없다).
--
-- I2(group_identity_gap)·I1(idle_negative)에는 over_report 면제가 없다 — over_report=1
-- 행은 idle 클램프로 항등식 gap이 함께 생기므로 두 불변식이 동시에 잡는 것이 의도된
-- 동작이다(over-report 발생일은 그 자체로 데이터 품질 결함).
--
-- 블록(순서 고정, UNION ALL 로 이은 11 SELECT = 8 이름; created_by_wrong_metrics 는 테이블별 4 SELECT):
--   P0      1) metrics_anchor_missing   2) metrics_gpu_dup_key   3) metrics_serving_dup_key
--           4) metrics_cost_sum_mismatch   5) created_by_wrong_metrics x4
--   stretch 6) share_sum_mismatch(정의서 §8 I3·I4)   7) group_identity_gap(I2)   8) idle_negative(I1)
--
-- 이 파일은 검증(SELECT)만 수행한다 — 쓰기 구문 없음.
-- =============================================================

-- 1) metrics_anchor_missing — 자식 팩트(gpu ∪ serving)에 (date, service)가 있는데 앵커
--    {FACT}.raw_token_metrics_summary_1d_dist 에 같은 (date, service) 행이 없다.
--    Plan 6a 쓰기 계약: 앵커는 서비스당 정확히 1행이고 자식 행은 앵커 없이 존재할 수 없다 —
--    M1/M3 의 앵커 필터가 이런 서비스를 조용히 제외하므로 여기서 잡는다.
--    서비스명은 사용자 식별자가 아니므로 detail 에 정렬 나열한다.
SELECT
    'metrics_anchor_missing' AS check_name,
    concat('services=', arrayStringConcat(arraySort(groupUniqArray(service)), ',')) AS detail,
    toUInt64(uniqExact(service)) AS bad_count
FROM
(
    SELECT service FROM {FACT}.raw_token_metrics_gpu_1d_dist WHERE date = '{DATE}'
    UNION DISTINCT
    SELECT service FROM {FACT}.raw_token_metrics_serving_1d_dist WHERE date = '{DATE}'
) AS c
WHERE service GLOBAL NOT IN (
    SELECT service FROM {FACT}.raw_token_metrics_summary_1d_dist WHERE date = '{DATE}'
)
HAVING count() > 0

UNION ALL

-- 2) metrics_gpu_dup_key — gpu 팩트 ORDER BY 키 (date, service, model, gpu_type, category)
--    중복. Plan 6a 재적재 계약(삭제 → 적재)이 깨지면 같은 키가 2행 이상 남는다.
--    안쪽에서 키별 행 수 n > 1 을 세고 바깥에서 1행으로 접는다
--    (dup_keys = 중복 키 수, extra_rows = 키당 초과 행 수의 합).
SELECT
    'metrics_gpu_dup_key' AS check_name,
    concat('dup_keys=', toString(count()), ' extra_rows=', toString(sum(n - 1))) AS detail,
    toUInt64(count()) AS bad_count
FROM
(
    SELECT service, model, gpu_type, category, count() AS n
    FROM {FACT}.raw_token_metrics_gpu_1d_dist
    WHERE date = '{DATE}'
    GROUP BY service, model, gpu_type, category
    HAVING n > 1
) AS d
HAVING count() > 0

UNION ALL

-- 3) metrics_serving_dup_key — serving 팩트 ORDER BY 키 (date, service, model, metric, name)
--    중복. 한 (service, model)에 여러 metric/name 행이 있는 것은 정상이므로 키 전체로 본다.
SELECT
    'metrics_serving_dup_key' AS check_name,
    concat('dup_keys=', toString(count()), ' extra_rows=', toString(sum(n - 1))) AS detail,
    toUInt64(count()) AS bad_count
FROM
(
    SELECT service, model, metric, name, count() AS n
    FROM {FACT}.raw_token_metrics_serving_1d_dist
    WHERE date = '{DATE}'
    GROUP BY service, model, metric, name
    HAVING n > 1
) AS d
HAVING count() > 0

UNION ALL

-- 4) metrics_cost_sum_mismatch — M1 {MART}.agg_token_model_cost_1d_dist 의 model_cost_krw
--    (has_gpu_rows = 1 행)와 gpu 팩트 재계산 C 의 대사. 재계산 술어는 mart/token-metrics/app/
--    steps.py SQL_M1 gpu_agg 와 문자 그대로 같다(도구 독립성 — import 대신 문자열 복제):
--      canon = if(a.canonical = '', g.model, a.canonical),
--      eff_alias / eff_tco = effective_from <= date 최신 행 argMax(TCO 최신 행 NULL → NULL),
--      비용 행 = category IN ('serving','standby') AND NOT hasAny(g.flags, FAIL 2종),
--      앵커 있는 서비스만, test 시간 불포함.
--    NULL 규칙(설계 §6.4 (1) 부분 합 금지)까지 대칭으로 대사한다:
--      null_mismatch  = mart 의 NULL 여부 != (재계산 tco_null_cnt > 0)
--      value_mismatch = 둘 다 값이 있는데 abs 차이 > 1원
--    mart 주도 GLOBAL LEFT JOIN(키 누락은 M1 자체 verify_count 가 담당). 위반 pair 수를 1행으로.
SELECT
    'metrics_cost_sum_mismatch' AS check_name,
    concat('pairs=', toString(count()),
           ' null_rule=', toString(countIf(null_mismatch = 1)),
           ' value=', toString(countIf(value_mismatch = 1))) AS detail,
    toUInt64(count()) AS bad_count
FROM
(
    SELECT
        toUInt8(isNull(m.model_cost_krw) != (f.tco_null_cnt > 0)) AS null_mismatch,
        toUInt8(isNotNull(m.model_cost_krw) AND f.tco_null_cnt = 0
                AND abs(ifNull(m.model_cost_krw, 0) - ifNull(f.fact_cost, 0)) > 1) AS value_mismatch
    FROM
    (
        SELECT service, model, model_cost_krw
        FROM {MART}.agg_token_model_cost_1d_dist
        WHERE date = '{DATE}' AND has_gpu_rows = 1
    ) AS m
    GLOBAL LEFT JOIN
    (
        SELECT
            g.service AS service,
            if(a.canonical = '', g.model, a.canonical) AS canon_model,
            countIf(g.category IN ('serving','standby') AND NOT hasAny(g.flags, ['hours_over_count','unknown_violation']) AND isNull(t.tco)) AS tco_null_cnt,
            sumIf(g.gpu_hours * t.tco, g.category IN ('serving','standby') AND NOT hasAny(g.flags, ['hours_over_count','unknown_violation']) AND isNotNull(t.tco)) AS fact_cost
        FROM {FACT}.raw_token_metrics_gpu_1d_dist AS g
        GLOBAL LEFT JOIN
        (
            SELECT alias, argMax(canonical, effective_from) AS canonical
            FROM {DIM}.dim_token_model_alias_dist
            WHERE effective_from <= '{DATE}'
            GROUP BY alias
        ) AS a ON a.alias = g.model
        GLOBAL LEFT JOIN
        (
            SELECT gpu_type,
                   nullIf(argMax(ifNull(tco_krw_per_gpu_hour, -1), effective_from), -1) AS tco
            FROM {DIM}.dim_token_gpu_tco_dist
            WHERE effective_from <= '{DATE}'
            GROUP BY gpu_type
        ) AS t ON t.gpu_type = g.gpu_type
        WHERE g.date = '{DATE}'
          AND g.service GLOBAL IN (
              SELECT service FROM {FACT}.raw_token_metrics_summary_1d_dist WHERE date = '{DATE}'
          )
        GROUP BY g.service, if(a.canonical = '', g.model, a.canonical)
    ) AS f ON f.service = m.service AND f.canon_model = m.model
) AS x
WHERE null_mismatch = 1 OR value_mismatch = 1
HAVING count() > 0

UNION ALL

-- 5) created_by_wrong_metrics — mart 4테이블에서 created_by != 'token-metrics-pipeline'
--    (Plan 6a 쓰기 계약: DEFAULT 없음 + CHECK created_by != '' — 값 자체는 이 불변식이 검사;
--    설계 §7.1). 테이블별 SELECT 4개, created_by 값별 GROUP BY 로 위반 값당 1행.
SELECT
    'created_by_wrong_metrics' AS check_name,
    concat('table=agg_token_model_cost_1d created_by=', created_by) AS detail,
    toUInt64(count()) AS bad_count
FROM {MART}.agg_token_model_cost_1d_dist
WHERE date = '{DATE}' AND created_by != 'token-metrics-pipeline'
GROUP BY created_by

UNION ALL

SELECT
    'created_by_wrong_metrics' AS check_name,
    concat('table=token_metrics_check_1d created_by=', created_by) AS detail,
    toUInt64(count()) AS bad_count
FROM {MART}.token_metrics_check_1d_dist
WHERE date = '{DATE}' AND created_by != 'token-metrics-pipeline'
GROUP BY created_by

UNION ALL

SELECT
    'created_by_wrong_metrics' AS check_name,
    concat('table=agg_token_model_share_1d created_by=', created_by) AS detail,
    toUInt64(count()) AS bad_count
FROM {MART}.agg_token_model_share_1d_dist
WHERE date = '{DATE}' AND created_by != 'token-metrics-pipeline'
GROUP BY created_by

UNION ALL

SELECT
    'created_by_wrong_metrics' AS check_name,
    concat('table=agg_token_gpu_group_1d created_by=', created_by) AS detail,
    toUInt64(count()) AS bad_count
FROM {MART}.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}' AND created_by != 'token-metrics-pipeline'
GROUP BY created_by

UNION ALL

-- 6) share_sum_mismatch (stretch, 정의서 §8 I3: 모델 m 의 Σ_s allocated_cost(s, m) = C(m),
--    서비스 미보고분 보정 없음; I4 token_not_reported 는 제공자 행 share=1·allocated=C 인 특수형).
--    {MART}.agg_token_model_share_1d_dist 는 (date, model, service) grain 이고 denominator_mode
--    는 모델당 하나이므로 model 로 GROUP BY 하면 그 모델의 서비스 행이 모두 접힌다.
--    대상: mode 3종(all_services / provider_reported / token_not_reported) × model_cost_krw
--    NOT NULL(C NULL 모델은 배분값도 NULL 로 정의상 합이 없다). provider_ambiguous 는 배분
--    NULL, no_provider 는 C=0·배분 0, external_api 는 벤더 단가식이라 I3 대상이 아니다.
--    provider_reported 는 T6 설계(분모 = greatest(w_p, w_all − w_p))로 Σ share = 1 이 모든
--    입력에서 성립하도록 정규화되어 있으므로 별도 면제가 필요 없다 — 세 모드 모두 동일 기준.
SELECT
    'share_sum_mismatch' AS check_name,
    concat('model=', model,
           ' mode=', any(denominator_mode),
           ' sum_allocated=', toString(round(ifNull(sum(allocated_cost_krw), 0), 2)),
           ' model_cost=', toString(round(ifNull(any(model_cost_krw), 0), 2))) AS detail,
    toUInt64(count()) AS bad_count
FROM {MART}.agg_token_model_share_1d_dist
WHERE date = '{DATE}'
  AND denominator_mode IN ('all_services','provider_reported','token_not_reported')
  AND isNotNull(model_cost_krw)
GROUP BY model
HAVING abs(ifNull(sum(allocated_cost_krw), 0) - ifNull(any(model_cost_krw), 0)) > 1

UNION ALL

-- 7) group_identity_gap (stretch, 정의서 §8 I2: 그룹 총비용 = Σ 모델비용 + 테스트 + 유휴 +
--    미귀속). {MART}.agg_token_gpu_group_1d_dist 의 identity_gap_krw(M2 가 계산한 좌우변 차)
--    가 ±1원을 넘으면 위반 — 설계 §7.1 그대로, over_report 면제 없음. over_report = 1 행은
--    idle 클램프(정의서 I1)로 gap 이 (할당 − 보고) × TCO 만큼 생기므로 여기와 8) idle_negative
--    에 함께 잡힌다(둘 다 위반이 맞다 — 항등식과 idle ≥ 0 이 동시에 깨진 상태). tco_missing = 1
--    행은 항등식의 항 자체가 NULL 이라 판정 불가(M2 quality_flag no_tco)이므로 제외.
SELECT
    'group_identity_gap' AS check_name,
    concat(service_group, '/', gpu_type,
           ' gap=', toString(round(ifNull(identity_gap_krw, 0), 2))) AS detail,
    toUInt64(1) AS bad_count
FROM {MART}.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}'
  AND tco_missing = 0
  AND isNotNull(identity_gap_krw)
  AND abs(identity_gap_krw) > 1

UNION ALL

-- 8) idle_negative (stretch, 정의서 §8 I1: idle_gpu_hours ≥ 0). M2 는 보고 시간이 할당 시간을
--    넘으면 idle 을 0 으로 클램프하고 over_report = 1 을 세운다 — 그 행이 곧 I1 위반 후보
--    (할당 dim 이 실제보다 작거나 팩트 시간이 과다 보고). 행당 1건, 보고/할당 시간을 노출.
SELECT
    'idle_negative' AS check_name,
    concat(service_group, '/', gpu_type,
           ' reported=', toString(round(reported_gpu_hours_total, 2)),
           ' allocated=', toString(round(ifNull(allocated_gpu_hours, 0), 2))) AS detail,
    toUInt64(1) AS bad_count
FROM {MART}.agg_token_gpu_group_1d_dist
WHERE date = '{DATE}'
  AND over_report = 1
