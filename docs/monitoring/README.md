# Grafana 테스터 대시보드 — token-usage (stage)

`grafana_dashboard_token_usage.json`은 홈랩(stage) 실배포 검증용 대시보드다(Plan 5 T4, §7.3).
`gpu_data.view_token_usage_*_dist` 4테이블 + `mart.agg_token_service_1d_dist`(대사 품질)를
조회한다. company 단계의 `batch_result` 대시보드(BATCH_RESULT 마커 기반, VictoriaLogs)와는
별개이며 무수정 편입 대상이 아니다(§9-20 — 마커 패널은 company 단계).

## 1. 전제 — grafana-clickhouse-datasource 플러그인

- 플러그인 ID: `grafana-clickhouse-datasource` (Grafana Labs 공식 ClickHouse 플러그인).
- 이 대시보드는 **v4 계열**(확인 시점 최신 4.19.0, Grafana ≥ 11.6.0 요구)의 시간 매크로
  문법을 기준으로 작성됐다 — `$__fromTime`/`$__toTime`은 인자 없이 `toDateTime(<unix>)`로
  치환되는 매크로다(구버전 `$__timeFilter(col)` 계열과 인자 유무가 다르니 혼동 주의).
- 미설치 시: Grafana 관리자 → Administration → Plugins → "ClickHouse" 검색 설치, 또는
  `grafana-cli plugins install grafana-clickhouse-datasource` (Grafana 파드/이미지에 반영
  필요 — 스테이지 런북 T5의 설치 항목).
- 버전 확인: Administration → Plugins → ClickHouse → 버전 배지. 4.x 미만이면 매크로 치환
  결과가 달라질 수 있으므로 업그레이드 후 사용.
- **Grafana 코어 버전 확인** (최소 11.6.0, `__requires` 참조): Grafana UI 우측 하단 버전 표시 또는
  `kubectl get deploy grafana -n monitoring -o jsonpath='{.spec.template.spec.containers[0].image}'` 
  로 확인.

## 2. ClickHouse 데이터소스 설정

계정 공유 결정(2026-07-14, 이슈 #1): 전용 `token_dashboard_reader` 계정은 폐지하고 동료의
기존 운영 계정 **`mart`**를 대시보드에도 그대로 사용한다(`mart/token-usage/ddl/company/accounts.sql`
참조). 이 계정은 `gpu_data.view_token_usage_*_dist` 4테이블 + `mart.agg_token_service_1d_dist`에
대한 SELECT 권한을 이미 갖고 있다(mart STEP 1/2 GRANT에 포함).

Grafana → Connections → Data sources → Add data source → ClickHouse:

| 필드 | 값 |
|---|---|
| Name | 임의(예: `ClickHouse (mart)`) — 임포트 시 이 데이터소스를 `DS_CLICKHOUSE` 입력에 매핑 |
| Server address | `chi-gpu-monitoring-gpu-monitoring-0-0.clickhouse.svc` |
| Server port | `8123` (HTTP) |
| Protocol | HTTP |
| Default database | **비워둠** — 이 대시보드의 SQL은 전부 `db.table` 완전 수식(FROM에 DB명 포함)이므로 기본 DB를 지정할 필요가 없다 |
| Username | `mart` |
| Password | 동료(클러스터 소유자)에게 요청 — 이 레포는 `mart` 계정의 비밀번호를 생성·관리하지 않는다(§7.2) |
| TLS/Secure | 클러스터 내부 통신이므로 비활성(사내 정책과 다르면 조정) |

저장 후 "Save & test"로 연결 확인.

## 3. 대시보드 임포트 절차

1. Grafana 좌측 메뉴 → Dashboards → New → Import.
2. `docs/monitoring/grafana_dashboard_token_usage.json` 파일 업로드(또는 JSON 내용 붙여넣기).
3. Import 화면에 **"ClickHouse (mart)"** 입력 필드가 나타난다 — 이는 대시보드 최상위
   `__inputs` 블록의 `DS_CLICKHOUSE` 선언 때문이다. 이 필드에 위 2절에서 만든 ClickHouse
   데이터소스를 선택한다. **이 매핑 프롬프트가 뜨지 않으면 `__inputs` 선언이 깨진 것 —
   모든 패널이 "datasource not found"로 실패하니 import를 중단하고 JSON을 확인한다.**
4. Import 클릭. uid `token-usage-stage`로 고정 생성/갱신된다(같은 uid의 기존 대시보드가
   있으면 덮어쓰기 여부를 묻는다).
5. 상단 시간 범위(기본 `now-30d` ~ `now`)와 템플릿 변수 `org_depth`(기본 `1`, 1~4 선택)를
   확인한다.

## 4. 패널 구성

`gpu_data.view_token_usage_*_dist`(4테이블)와 `mart.agg_token_service_1d_dist`를 조회하는
7개 데이터 패널 + 참고용 텍스트 패널 1개, 총 8개 패널이다.

| # | 패널 | FROM (물리 `_dist` 테이블) | 목적 |
|---|---|---|---|
| 1 | 서비스별 일별 total_input_tokens 추이 | `gpu_data.view_token_usage_service_1d_dist` | 시계열 — 서비스별 일별 토큰 추이 |
| 2 | org 롤업 (`$org_depth`) | `gpu_data.view_token_usage_org_1d_dist` | `arraySlice(org_path,1,$org_depth)` 롤업 |
| 3 | 모델별 토큰·cost | `gpu_data.view_token_usage_model_1d_dist` | 모델·provider별 집계 |
| 4 | 서비스 대사 품질 | `mart.agg_token_service_1d_dist` | `diff_*` 비0 또는 summary 부재(NULL) 서비스만 표시, `is_derived` 구분 |
| 5 | unknown 버킷 비율 | `gpu_data.view_token_usage_org_1d_dist` | `org_path=['unknown']`(미매핑) 비중 |
| 6 | anon 핸들명 사용 상위 | `gpu_data.view_token_usage_1d_dist` | `user_type='anonymous'` GROUP BY `user_name` — 비실명 핸들명 표기 검증 |
| 7 | 일별 수집 커버리지 | `gpu_data.view_token_usage_service_1d_dist` | 일자별 보고 서비스 수(`reporting_services`) — collectors `endpoints.yaml`의 enabled 서비스 수와 비교 |
| — | (텍스트) 참고: BATCH_RESULT 마커 패널 | 없음(쿼리 없음) | BATCH_RESULT/VictoriaLogs 안내 — company 단계 |

패널 4(대사 품질)만 **mart DB**를 직접 조회한다(같은 `mart` 계정으로 접속되므로 추가 설정
불필요) — 나머지는 전부 `gpu_data`(view) 조회다.

패널 7은 `reporting_services`(당일 summary 적재 서비스 수)와 `enabled_services`
(`gpu_data.dim_token_service_dist WHERE enabled = 1` — collector가 매 실행 원자 교체하는
레지스트리)를 **한 쿼리에서 함께 반환**해 커버리지 결손을 자동 비교한다(§7.1 coverage
계약의 대시보드 반영). 두 값이 다르면 어느 서비스가 빠졌는지 STEP 0 마커
(`missing_services=`)에서 확인한다.

## 5. 시간 필터 매크로 규칙

모든 데이터 패널은 아래 형태로 시간 범위를 건다(전 패널 일관 적용):

```sql
date BETWEEN toDate($__fromTime) AND toDate($__toTime)
```

- `$__fromTime`/`$__toTime`은 grafana-clickhouse-datasource가 인자 없이 제공하는 매크로로,
  각각 `toDateTime(<범위 시작/끝의 unix timestamp>)`로 치환된다.
- 모든 대상 테이블의 `date` 컬럼은 ClickHouse `Date` 타입이므로 `toDate(...)`로 감싸
  `Date BETWEEN Date`로 비교한다(브리프의 "매크로 또는 date 컬럼 BETWEEN" 두 방식을 결합).

## 6. JSON 검증(개발자용 — 재작성 시 재실행)

이 JSON을 다시 생성/수정하면 아래를 재확인한다(커밋 전 실행, 리포에는 포함하지 않는
1회성 검증 스크립트 — `.superpowers/sdd/task-4-report.md`에 실행 로그 보존):

1. `json.load` + 필수 키(`__inputs`/`panels`/`templating`/`uid`) 확인.
2. 모든 패널 `rawSql`의 `FROM <db>.<table>`을 추출해 `ddl/**/*.sql`의
   `CREATE TABLE IF NOT EXISTS` 이름과 대조 — 전부 `_dist` 접미 + DDL에 실재해야 함.
3. 모든 패널 `datasource`가 객체형(`{"type":..., "uid":...}`)인지 확인 — 텍스트 패널은
   내장 `{"type":"datasource","uid":"grafana"}`, 나머지는 전부
   `{"type":"grafana-clickhouse-datasource","uid":"${DS_CLICKHOUSE}"}`.
4. 모든 `rawSql`을 clickhouse-format으로 문법 검증 —매크로(`$__fromTime`/`$__toTime`)는
   ClickHouse 문법이 아니므로 검증 시에만 `now() - INTERVAL 30 DAY` / `now()`로,
   `$org_depth`는 `1`로 치환한 사본을 사용한다(치환은 검증 전용 — 실제 JSON에는 반영하지
   않는다):

   ```bash
   kubectl exec -i -n clickhouse chi-gpu-monitoring-gpu-monitoring-0-0-0 \
       -- clickhouse-format --multiquery < <(치환된 쿼리 파일)
   ```

   읽기 전용 커맨드(`clickhouse-format`)이며 클러스터에 아무 것도 쓰지 않는다.

## 7. token-metrics 대시보드

`grafana_dashboard_token_metrics.json`(uid `token-metrics-stage`, title `Token Metrics — Stage Tester`,
tags `token-metrics`/`stage`)은 Plan 6c(mart/token-metrics)의 stage 검증용 대시보드다(설계
2026-08-31 §6.2). 기존 `grafana_dashboard_token_usage.json`(uid `token-usage-stage`)과는 **별개
파일·별개 uid**이며 기존 JSON은 무수정이다. 전제(§1 플러그인 v4·§2 데이터소스·§3 임포트 절차)는
그대로 — 같은 `mart` 계정 데이터소스를 쓰고, 6c 계정 GRANT(`mart/token-metrics/ddl/company/accounts.sql`)가
mart 4테이블·fact 앵커·레지스트리 SELECT를 포함하므로 추가 설정은 없다.

조회 대상은 mart-metrics 4테이블(`mart.agg_token_model_cost_1d_dist` M1, `mart.token_metrics_check_1d_dist` M3,
`mart.agg_token_model_share_1d_dist` M4, `mart.agg_token_gpu_group_1d_dist` M2) + 앵커 fact
`fact.raw_token_metrics_summary_1d_dist` + 성능 fact `fact.raw_token_metrics_serving_1d_dist`(service×model 단위만)
+ 레지스트리 `gpu_data.dim_token_metrics_service_dist`로, 데이터 패널 15개 + 텍스트 패널 1개 = 16개다(설계 §6.2가
나열한 내용 전부). `user_id`/`user_name` 컬럼은 어떤 패널에도 없다(§5.6).

| # | 패널 | FROM (물리 `_dist` 테이블) | 목적 |
|---|---|---|---|
| 1 | 모델별 일별 model_cost_krw (serving/standby 분해) | `mart.agg_token_model_cost_1d_dist` | 시계열 — 모델별 C(측정) + `serving_cost_krw`/`standby_cost_krw`(C × 시간 비례 분해). TCO 부재 행이 있으면 NULL(0 아님) |
| 2 | 서비스별 총비용 (측정, 배부 미적용) | `mart.agg_token_model_cost_1d_dist` | 시계열 — 설계 §6.2 P0-core: 서비스별 Σ M1 `model_cost_krw`, `cost_label` = `측정 (배부 미적용)` |
| 3 | 서비스×모델 GPU 시간·비용 (당일) | `mart.agg_token_model_cost_1d_dist` | 범위 내 최신 집계일 한 날의 service×model — serving/standby/test/flagged 시간, C, `krw_per_request`(요청당 원가), `tokens_per_gpu_hour`, `quality_flag` |
| 4 | 서비스별 tokens_per_gpu_hour 추이 | `mart.agg_token_model_cost_1d_dist` | 시계열 — Σ total_tokens / Σ serving_gpu_hours |
| 5 | 토큰 단가 p (파생 — 기준월·가동률 병기) | `mart.agg_token_model_cost_1d_dist` (+ `mart.agg_token_gpu_group_1d_dist` 조인) | 정의서 3.7 — 기준월(`base_month`)·그룹·모델별 p = Σ C / Σ W(원/1M 가중토큰), p_cached = 0.1p, p_output = 4p, `utilization_pct`(M2 월 가동률) 병기, `cost_label` = `파생` |
| 6 | quality_flag 분포 | `mart.agg_token_model_cost_1d_dist` | 플래그별 행수 + 비용 NULL·GPU 무·토큰 무 행수 |
| 7 | 검사 결과 (FAIL/WARN) | `mart.token_metrics_check_1d_dist` | M3 severity FAIL/WARN 행 — `check_name`·`observed`·`threshold`·`detail`·`source_type` |
| 8 | 일별 FAIL/WARN 건수 | `mart.token_metrics_check_1d_dist` | 시계열 — severity 별 건수 |
| 9 | 모델 비용 배분 (share) | `mart.agg_token_model_share_1d_dist` | M4 — `denominator_mode`, `share`, `allocated_cost_krw`; `cost_label` = 배분/추정(external_api)/그룹 귀속(token_not_reported) |
| 10 | 서비스별 배분 총비용 (M4 합산, stretch) | `mart.agg_token_model_share_1d_dist` | 설계 §6.2 stretch — 서비스별 Σ `allocated_cost_krw` 를 `cost_label` 별로 합산(§6.4 (6) ①②③); 패널 2와 대비 |
| 11 | 그룹 GPU 정체성 (I2) | `mart.agg_token_gpu_group_1d_dist` | M2 그룹 행 — 그룹 총비용 = `model_cost_sum_krw`(ΣC) + `test_cost_krw`(실험) + `idle_cost_krw`(유휴) + `unattributed_cost_krw`(미귀속), `identity_gap_krw`(≈0 정상), `over_report` |
| 12 | 그룹 utilization 추이 | `mart.agg_token_gpu_group_1d_dist` | 시계열 — service_group/gpu_type 별 utilization |
| 13 | TTFT/ITL 추이 (p50/p95) | `fact.raw_token_metrics_serving_1d_dist` | 시계열 — service×model 별 `ttft_ms`/`itl_ms` p50·p95(ms), `source_type` 병기 |
| 14 | 출처 (manual-v0 vs API) | `fact.raw_token_metrics_summary_1d_dist` | 시계열 — 날짜별 `source_type` 별 보고 서비스 수 |
| 15 | 일별 메트릭 커버리지 | `fact.raw_token_metrics_summary_1d_dist` (+ `gpu_data.dim_token_metrics_service_dist` 조인) | 보고 서비스 수(`reported_services`) vs 기대 서비스 수(`expected_services` — 마커 `metrics_coverage` 분모와 같은 술어를 날짜별로 계산), `rejected_rows`, `manual_services` |
| 16 | (텍스트) 참고: BATCH_RESULT 마커 패널 | 없음(쿼리 없음) | `BATCH_RESULT … module=mart-metrics` 마커 형식·VictoriaLogs 안내 + 라벨 규칙(측정/배분/추정/그룹 귀속/파생) |

템플릿 변수 2개: `service_group`(`SELECT DISTINCT service_group FROM mart.agg_token_model_cost_1d_dist ORDER BY 1`,
multi/All)과 `service`(같은 테이블, `WHERE service_group IN (${service_group:singlequote})`). 패널 1~14는
`service_group`, 패널 1~4·6~10·13·14는 `service` 변수로 필터한다(패널 5는 모델 단위 C÷W라 서비스 필터가 무의미,
M2 패널 11·12는 service 컬럼이 없고, 커버리지 패널 15는 필터 없음).

시간 필터는 §5 규칙 그대로 — 모든 데이터 패널이 `date BETWEEN toDate($__fromTime) AND toDate($__toTime)`
를 건다. 비용 표시는 `docs/cost-model-spec.md` §7 라벨 규칙을 따른다: `cost_label` 컬럼(측정 = GPU 시간×TCO,
배분 = 가중 토큰 비율 1/0.1/4, 추정 = 사외 API 벤더 단가, 파생 = 토큰 단가 p — 기준월·가동률 병기), 비용 NULL은
"측정 불가"(TCO/단가 부재)이지 0이 아니다. 패널 13(TTFT/ITL)은 `custom` 지표와 `e2e_ms`/`output_tps`를 보여주지
않는다 — 성능 패널은 설계 §6.2대로 service×model 단위의 표준 지연 지표 2종만.

JSON 검증(재작성 시 재실행 — §6 절차 + 아래 한 줄; 계약 테스트는
`cd mart/token-metrics && python -m pytest -q tests/test_docs_contract.py`):

```bash
python3 -c "import json;d=json.load(open('docs/monitoring/grafana_dashboard_token_metrics.json'));assert d['uid']=='token-metrics-stage';assert len(d['panels'])==16;print('ok')"
```
