# token-metrics 배포 런북 (Plan 6a/6b/6c — 설계 2026-08-31 §7.1·§7.3·§7.5)

`/v1/metrics` 반입 파이프라인(collectors/token-metrics = 6b, mart/token-metrics = 6c)과 기준정보
dim 4종(assets/model-catalog = 6a)을 **기존 토큰 파이프라인(collectors/token-usage·mart/token-usage) 무수정**으로
얹는 절차다. 순서는 설계 §4.0 매니페스트·§7.5 그대로 — ① 기준정보 dim → ② collectors-metrics → ③ mart-metrics `install.sh`
(읽기 계약 프리플라이트) → ④ 첫 배치·마커 → ⑤ `invariants_metrics` → ⑥ 대시보드. 재실행(§7)·격리 검증(§8)·
트러블슈팅(§9)·롤백(§9 끝)은 뒤에 있다. stage 공통 환경(홈랩 컨텍스트·CH 파드 탐색·apply_sql)은
`docs/operations/stage-runbook.md` §2, 사내 2단계 검증 전략은 `docs/operations/company-verify.md`, 기존 모듈의
재실행 규칙은 `docs/operations/rerun.md` 를 따른다.

## 0. 전제

- Plan 6a 산출물이 머지돼 있다: `assets/model-catalog/ddl/{company,stage}/dim_token_{model_alias,gpu_tco,gpu_allocation,vendor_price}.sql`
  + `seed_dim_token_*.sql` + `accounts_metrics.sql`, `collectors/token-metrics/ddl/`, `mart/token-metrics/ddl/`.
- 기존 토큰 파이프라인이 설치·가동 중이다: `mart.token_usage_1d_dist`·`mart.agg_token_service_1d_dist`·
  `gpu_data.dim_token_service_dist` 가 존재해야 6c `install.sh` 의 읽기 계약 프리플라이트(3테이블/13컬럼)를 통과한다.
  없으면 §3 이 `PREFLIGHT FAIL read_contract missing=<db.table.column,…>` 로 `exit 3` 한다(GPU-only 검증은 §8).
- admin 권한으로 `clickhouse-client` 를 실행할 수 있는 kube 컨텍스트가 있다(DDL·GRANT 는 admin 수동, 설계 §7.1).
  사내 클러스터 서비스 주소는 문서상 `chi-<cluster>.<ns>.svc` 로만 적는다(실값은 사내 문서).
- 컨테이너 레지스트리: stage 는 `ghcr.io/yoonsungnam/token-mart-metrics`(`.github/workflows/release-images-metrics.yml`
  이 push), company 는 `harbor.example.internal/<project>/token-mart-metrics:<sha7>` (Harbor 빌드 절차는 `company-verify.md` §0).
- 아래 셸 변수를 세션마다 잡는다(`stage-runbook.md` §2 와 같은 규칙 — 파드 이름은 `chi-` 접두로 탐색):

```bash
export KUBE_CONTEXT=<ctx>              # stage: homelab / company: 사내 컨텍스트 이름
export CH_NS=clickhouse
export CH_POD="$(kubectl --context "$KUBE_CONTEXT" -n "$CH_NS" get pods -o name | grep '^pod/chi-' | head -1 | cut -d/ -f2)"
apply_sql() { kubectl --context "$KUBE_CONTEXT" -n "$CH_NS" exec -i "$CH_POD" -- clickhouse-client --multiquery < "$1"; }
echo "$CH_POD"                          # 예: chi-<cluster>-<cluster>-0-0-0
```

## 1. 기준정보 dim 4종

`gpu_data.dim_token_model_alias`(별칭→canonical), `dim_token_gpu_tco`(기종별 TCO 원/GPU시간), `dim_token_gpu_allocation`
(그룹×기종 배정 GPU 수), `dim_token_vendor_price`(사외 API 단가 원/1M 토큰). 이력 조회 키는 `effective_from <= date` 의
최신 행이며 시드의 `2026-01-01` 플레이스홀더 행(값 NULL·`unknown`)은 항상 실값 행에 밀린다(Plan 6a D).

stage(홈랩) — DDL 미러 `ddl/stage/` + 플레이스홀더 시드 + **합성 실값 fixture**(`assets/model-catalog/fixtures/`):

```bash
for t in dim_token_model_alias dim_token_gpu_tco dim_token_gpu_allocation dim_token_vendor_price; do
  apply_sql "assets/model-catalog/ddl/stage/$t.sql"
done
for t in model_alias gpu_tco gpu_allocation vendor_price; do
  apply_sql "assets/model-catalog/ddl/stage/seed_dim_token_$t.sql"           # 플레이스홀더 행 (NULL·unknown)
  apply_sql "assets/model-catalog/fixtures/stage_seed_dim_token_$t.sql"      # 합성 실값
done
apply_sql assets/model-catalog/ddl/stage/accounts_metrics.sql                # mart 계정 4테이블 _dist SELECT (기존 accounts.sql 무수정)
```

company — DDL `ddl/company/` + 플레이스홀더 시드 + **실값은 생성기 출력(gitignore)** 을 admin 이 적용한다:

```bash
for t in dim_token_model_alias dim_token_gpu_tco dim_token_gpu_allocation dim_token_vendor_price; do
  apply_sql "assets/model-catalog/ddl/company/$t.sql"
done
for t in model_alias gpu_tco gpu_allocation vendor_price; do
  apply_sql "assets/model-catalog/ddl/company/seed_dim_token_$t.sql"
done
apply_sql assets/model-catalog/ddl/company/accounts_metrics.sql
# 실값 (레포 밖 CSV → gitignore 된 *_insert.sql — 커밋 금지, 설계 §7.2)
python3 assets/model-catalog/sheet_to_dim_token_model_alias_insert.py \
  --csv <모델탭.csv> --services collectors/token-metrics/endpoints-metrics.company.yaml \
  --effective-from <YYYY-MM-DD> --out dim_token_model_alias_insert.sql
for t in gpu_tco gpu_allocation vendor_price; do
  python3 assets/model-catalog/csv_to_layer_c_dim_insert.py --table "$t" --csv "<${t}.csv>" \
    --effective-from <YYYY-MM-DD> --out "dim_token_${t}_insert.sql"
done
for f in dim_token_model_alias_insert.sql dim_token_gpu_tco_insert.sql dim_token_gpu_allocation_insert.sql dim_token_vendor_price_insert.sql; do
  apply_sql "$f"        # 각 파일 끝 "-- 검증: 결과가 비어야 정상" 이후 SELECT 가 0행이어야 한다 (check_name, key, effective_from, cnt)
done
```

company-verify(격리) 로 dim 을 반입할 때는 위 생성기에 `--target-db token_verify_dim` 을 붙인다(§8).

확인(4테이블 행수 — stage 는 fixture 행수, company 는 시드 + 실값):

```bash
kubectl --context "$KUBE_CONTEXT" -n "$CH_NS" exec "$CH_POD" -- clickhouse-client -q "
SELECT 'model_alias', count() FROM gpu_data.dim_token_model_alias_dist
UNION ALL SELECT 'gpu_tco', count() FROM gpu_data.dim_token_gpu_tco_dist
UNION ALL SELECT 'gpu_allocation', count() FROM gpu_data.dim_token_gpu_allocation_dist
UNION ALL SELECT 'vendor_price', count() FROM gpu_data.dim_token_vendor_price_dist"
```

TCO 가 NULL 인 기종이 남아 있으면 그 기종을 쓰는 (service, model) 의 `model_cost_krw` 는 NULL(`quality_flag=no_tco`)로
적재된다 — 0 이 아니라 "측정 불가" 다(`docs/cost-model-spec.md` §7).

## 2. collectors-metrics(6b)

수집기(CronJob `token-metrics-collector`, `5 2-9 * * *` KST — 02:05~09:05 매시, mart 는 10:20)는 Plan 6b 산출물의
`collectors/token-metrics/install.sh` 로 설치한다 — 절차·Secret·`endpoints-metrics.company.yaml`(gitignore) 은
`collectors/token-metrics/README.md` 와 `collectors/token-metrics/ddl/README.md` 를 따른다(이 문서는 링크만).
6c 가 읽는 것은 fact 3테이블(`fact.raw_token_metrics_{gpu,serving,summary}_1d_dist`)과 레지스트리
`gpu_data.dim_token_metrics_service_dist` 다. API 가 없는 서비스의 수기 제출(manual-v0)은
`collectors/token-metrics/tools/manual_load.py --context <ctx> --from <A> --to <B> --gpu <gpu.csv> --serving <serving.csv> [--engine <engine.csv>] [--replace]`
(템플릿 `docs/templates/token_metrics_manual_v0_*.csv`) 로 적재하며 `source_type='manual-v0'` 로 구분된다(§9 의 `manual` 플래그).

수집기가 아직 없어도 6c 는 설치·실행된다(fact 0행 → 토큰-only 행 + `CHECK WARN metrics_coverage missing=<n>`,
`status=SUCCESS`) — 다만 GPU 시간·비용이 전부 0/NULL 이라 검증은 무의미하다.

## 3. mart-metrics install.sh

이미지 빌드 후 `mart/token-metrics/install.sh` 를 실행한다. 단계는 6개이며 **[3/6] 프리플라이트가 DDL 적용([4/6]) 앞**에 있다:

| 단계 | 내용 |
|---|---|
| `[1/6]` | `registry-pull-secret` — **없을 때만** 생성(있으면 그대로 사용, 대화형) |
| `[2/6]` | Secret `token-mart-metrics-ch-secret`(격리 overlay 는 `-verify` 접미) — 키 11개 `CH_HOST CH_PORT CH_USER CH_PASSWORD CH_CLUSTER CH_DB_FACT CH_DB_DIM CH_DB_MART CH_DB_TOKEN_MART CH_DB_TOKEN_DIM MART_METRICS_MAX_MUTATIONS_PER_RUN`(company/-verify 는 `INSERT_QUORUM=auto` 추가) |
| `[3/6]` | 읽기 계약 DESCRIBE 프리플라이트 — `${CH_DB_TOKEN_MART}.token_usage_1d_dist` 9컬럼, `${CH_DB_TOKEN_MART}.agg_token_service_1d_dist` 2컬럼, `${CH_DB_TOKEN_DIM}.dim_token_service_dist` 2컬럼(=13). **앱 계정(`CH_USER`/`CH_PASSWORD`)으로 here-string 실행**해 GRANT 누락도 함께 잡는다(admin 계정이 아님 — 비밀번호는 argv 에 오르지 않는다). 누락 시 `PREFLIGHT FAIL read_contract missing=<db.table.column,…>` 출력 후 `exit 3` — 테이블은 만들지 않는다 |
| `[4/6]` | `mart/token-metrics/ddl/<overlay>/mart_metrics_tables.sql` 적용(4테이블 `_local`/`_dist`; `accounts.sql` 은 admin 수동) |
| `[5/6]` | `kubectl apply -k mart/token-metrics/k8s/overlays/<overlay>` — CronJob `token-mart-metrics`(`20 10 * * *` KST, `activeDeadlineSeconds 1800`) |
| `[6/6]` | 이미지 주소 주입(`kubectl set image cronjob/token-mart-metrics token-mart-metrics=<registry>/token-mart-metrics:<tag>`) + 수동 실행 커맨드 안내 — `CH_HOST` 는 [2/6] Secret 의 키(envFrom)이지 이 단계가 넣는 정적 env 가 아니다 |

```bash
# admin — GRANT (mart 계정: mart 4테이블 INSERT/SELECT + _local ALTER DELETE, 읽기 dim 6·fact 3·토큰 mart 2)
apply_sql mart/token-metrics/ddl/company/accounts.sql          # stage 는 ddl/stage/accounts.sql

# stage
./mart/token-metrics/build.sh --tag <sha7> stage
./mart/token-metrics/install.sh --overlay stage --context homelab --registry ghcr.io/yoonsungnam --tag <sha7> -n monitoring

# company
./mart/token-metrics/build.sh --registry harbor.example.internal/<project> --tag <sha7> company
./mart/token-metrics/install.sh --overlay company --context "$KUBE_CONTEXT" --registry harbor.example.internal/<project> --tag <sha7> -n monitoring
```

Secret 값: `CH_HOST` 는 클러스터 내부 서비스 주소(사내: `chi-<cluster>.<ns>.svc`, stage: `stage-runbook.md` §2 의 값),
`CH_DB_TOKEN_MART=mart`·`CH_DB_TOKEN_DIM=gpu_data`(기존 토큰 파이프라인 DB — 격리 검증은 §8),
`MART_METRICS_MAX_MUTATIONS_PER_RUN=64`(= 16일 × 4테이블; 정기 실행은 날짜당 ≤4).

설치 확인:

```bash
kubectl --context "$KUBE_CONTEXT" -n monitoring get cronjob token-mart-metrics -o jsonpath='{.spec.schedule}{"\n"}'   # 20 10 * * *
kubectl --context "$KUBE_CONTEXT" -n monitoring get secret token-mart-metrics-ch-secret -o jsonpath='{.data}' | python3 -c "import json,sys;print(sorted(json.load(sys.stdin)))"   # 키 11개
kubectl --context "$KUBE_CONTEXT" -n "$CH_NS" exec "$CH_POD" -- clickhouse-client -q "SHOW TABLES FROM mart LIKE '%token_%'"   # agg_token_model_cost_1d_*, token_metrics_check_1d_*, agg_token_model_share_1d_*, agg_token_gpu_group_1d_* (+ 기존 token_usage_1d_*)
```

## 4. 첫 배치·마커

CronJob 을 기다리지 않고 수동 Job 으로 1회 실행한다(인자 없음 = 어제(KST) 1일 — 특정 날짜·범위는 §7 `rerun.py`):

```bash
JOB="token-mart-metrics-manual-$(TZ=Asia/Seoul date +%Y%m%d)"
kubectl --context "$KUBE_CONTEXT" -n monitoring create job --from=cronjob/token-mart-metrics "$JOB"
kubectl --context "$KUBE_CONTEXT" -n monitoring wait --for=condition=complete --timeout=1800s "job/$JOB"
kubectl --context "$KUBE_CONTEXT" -n monitoring logs "job/$JOB" | grep -E "PREFLIGHT|CHECK WARN|BATCH_RESULT"
```

성공 마커(한 줄, 값은 예시):

```
BATCH_RESULT status=SUCCESS module=mart-metrics metrics_coverage=3/3 missing_services="-" rows_mart=42 rows_check=7 rows_share=39 warn=0 elapsed=12.4
```

| 필드 | 의미 |
|---|---|
| `status` | `SUCCESS` / `FAILURE`(`reason=` 동반) — 메트릭이 없는 날도 `SUCCESS`(rows 0, `metrics_coverage` WARN)이며 별도 NODATA 상태는 없다(설계 §6.1) |
| `module=mart-metrics` | 고정(기존 `token-usage`·`mart-token` 과 구분 — VictoriaLogs 대시보드 필터 키) |
| `metrics_coverage=<present>/<enabled>` | 레지스트리 `enabled=1` 서비스 중 그날 앵커(`raw_token_metrics_summary_1d`)가 있는 수 |
| `missing_services="a,b"` | 앵커 없는 enabled 서비스 + 사용량 레지스트리에 없는 메트릭 레지스트리 서비스(합집합, 없으면 `"-"`) — `user_id`·payload 는 절대 마커에 싣지 않는다(마스터 §5.6) |
| `rows_mart` / `rows_check` / `rows_share` | M1 / M3 / M4 적재 행수(M2 는 `rows_mart` 에 포함되지 않음 — 로그 `M2 rows_group=<n>` 줄) |
| `warn` | `CHECK WARN` 건수 — `metrics_coverage missing=<n>`, `service_not_in_usage_registry service=<s>`, `token_mart_absent date=<d>`, `dup_suspect:<table>` |
| `reason` | `read_contract` / `mutation_budget` / `verify_count` / `sigterm` / `exception` (§9) |

`status=SUCCESS warn>0` 은 정상 종료다(적재됨) — WARN 코드를 §9 표로 해석한다. 첫 실행이 `FAILURE reason=read_contract` 면
§3 프리플라이트가 통과했더라도 런타임에 토큰 mart 컬럼이 바뀐 것이므로 `mart/token-metrics/app/preflight.py` 의
`READ_CONTRACT` 와 실제 `DESCRIBE` 를 대조한다.

## 5. invariants_metrics

GitHub 체크아웃의 `tools/verify/run_invariants.py` 는 `--sql` 옵션(Plan 6c T9 additive)으로 `invariants_metrics.sql`
을 실행한다 — **사내 분기본의 `run_invariants.py` 에는 `--sql` 이 없으므로** 반드시 이 체크아웃에서 실행한다.
8블록: `metrics_anchor_missing, metrics_gpu_dup_key, metrics_serving_dup_key, metrics_cost_sum_mismatch,
created_by_wrong_metrics, share_sum_mismatch, group_identity_gap, idle_negative`.

```bash
kubectl --context "$KUBE_CONTEXT" -n "$CH_NS" port-forward "$CH_POD" 18123:8123 >/dev/null 2>&1 &
PF=$!
CH_HOST=127.0.0.1 CH_PORT=18123 CH_USER=mart CH_PASSWORD=<mart-password> \
  python3 tools/verify/run_invariants.py --sql tools/verify/invariants_metrics.sql --date <YYYY-MM-DD>
kill $PF
```

기대 출력: `ALL INVARIANTS PASS (date=<YYYY-MM-DD>, DBs=fact/gpu_data/mart, sql=invariants_metrics.sql)` (exit 0 —
`sql=` 접미는 T9 가 `--sql` 과 함께 추가한 출력). 위반이 있으면 `[FAIL] n건` 과 `check_name / bad_count / detail` 표가
출력되고 exit 1 — `metrics_cost_sum_mismatch` 는 M1 C 와 fact 재계산 불일치(T3 술어 변경 여부), `share_sum_mismatch` 는
Σ allocated ≠ C ±1원(I3), `group_identity_gap` 은 `abs(identity_gap_krw) > 1`(I2), `idle_negative` 는 `over_report=1`
(I1 — 보고 > 배정, `dim_token_gpu_allocation` 갱신 대상).

## 6. 대시보드

`docs/monitoring/grafana_dashboard_token_metrics.json`(uid `token-metrics-stage`, 16패널)을 `docs/monitoring/README.md`
§3 절차로 임포트한다(데이터소스는 기존 `mart` 계정 ClickHouse 데이터소스 그대로 — §7 참조). 첫 배치 후 확인 순서:
패널 15(커버리지 `reported_services` = `expected_services` — 마커 `metrics_coverage` 와 같은 분모) → 패널 7(FAIL/WARN 0 또는 §9 해석)
→ 패널 1·2·3(비용 NULL 은 `no_tco` — §1 TCO 갱신) → 패널 11(`identity_gap_krw` ≈ 0) → 패널 13(TTFT/ITL 값이 있으면 6b serving
블록 적재 정상). `BATCH_RESULT` 마커 패널은 VictoriaLogs 가 있는 company 단계에서
기존 `batch_result` 대시보드에 module `mart-metrics` 로 편입한다(패널 16 텍스트).

## 7. 재실행(rerun --chunk-days 7)

`mart/token-metrics/tools/rerun.py` 는 CronJob `token-mart-metrics` 로부터 수동 Job 을 만들어 날짜 범위를 **7일 청크**로
순차 실행한다(청크당 `activeDeadlineSeconds` = `1800 × ceil(청크일수/7)`, 상한 7200초). 규칙(설계 §7.5):

- **창**: 현재 KST 가 10:50 이전이면 `RERUN REFUSED window (>=10:50 KST) — use --force (now=<HH:MM> KST)` 로 exit 2 —
  정기 실행(10:20)과 겹치지 않게 한다. 단일 날짜 즉시 재실행 등 의도된 경우만 `--force`.
- **활성 Job 0**: `token-mart-*` 접두 Job(정기 `token-mart-metrics-*`·기존 `token-mart-daily-*`) 이 실행 중이면
  `RERUN REFUSED active_jobs=<n> (token-mart-* running)` 로 exit 2 — `--force` 로도 우회할 수 없다(같은 mart DB
  파티션에 동시 뮤테이션을 넣지 않기 위함). kubectl/Job 자체 실패는 exit 1, 성공은 exit 0(exit 3 은 없다).
- **예산**: 날짜당 4 뮤테이션(M1·M3·M4·M2 DELETE) × 16일 = `MART_METRICS_MAX_MUTATIONS_PER_RUN=64` → `--chunk-days` 상한 16.
  청크 7일 = 28 변이 ≤ 64.
- 재실행은 날짜별 `DELETE WHERE date = …` 후 재적재(멱등) — 부분 적재(예: M1·M3 만 들어가고 M4 에서 실패) 도 같은 날짜를 다시 돌리면 4테이블 모두 정합된다.
- **토큰 mart 와 같은 구간을 backfill 할 때는 순서가 있다(설계 §6.3)**: 토큰 mart(`token-mart-daily`, 사내 스케줄은 stage-runbook 에서 확인 — GitHub 기준 04:00)
  재수행이 **끝난 뒤** mart-metrics `rerun.py` 를 실행한다. M1 의 토큰 컬럼(`input_tokens … weighted_tokens`)과 M4 전체가
  `mart.token_usage_1d_dist`/`mart.agg_token_service_1d_dist` 를 읽으므로, 토큰 mart 가 아직 옛 값이면 mart-metrics 결과도 옛 값으로 굳는다.

```bash
# 범위 재실행 (기본 청크 7일 — 예: 16일 = 7+7+2 청크 3개)
python3 mart/token-metrics/tools/rerun.py --from 2026-09-01 --to 2026-09-16 --context "$KUBE_CONTEXT" -n monitoring --chunk-days 7
# 특정 하루 즉시 재실행 (창 무시, 활성 Job 게이트는 그대로)
python3 mart/token-metrics/tools/rerun.py --from 2026-09-04 --to 2026-09-04 --context "$KUBE_CONTEXT" --force
# company-verify 격리 CronJob 대상
python3 mart/token-metrics/tools/rerun.py --from 2026-09-04 --to 2026-09-04 --context "$KUBE_CONTEXT" --cronjob token-mart-metrics-verify
# 진행 확인 (CronJob/jobTemplate/pod 3곳 라벨이 app=token-mart-metrics; rerun Job은 rerun=1 도 붙는다)
kubectl --context "$KUBE_CONTEXT" -n monitoring get jobs -l app=token-mart-metrics
```

수집기(6b) 쪽을 다시 받은 뒤 mart 까지 이어 돌릴 때는 `collectors/token-metrics/tools/rerun.py --from <A> --to <B>`
가 완료 후 `MART_RERUN`(= `mart/token-metrics/tools/rerun.py`) 커맨드를 `build_mart_command()` 로 조립해 항상 안내하며,
`--chain-mart` 플래그를 주면 그 커맨드를 직접 실행(반환값 그대로 종료)한다 — 6c 쪽은 `--chain` 옵션이 없는 체인의 종단이다.
수기 CSV 를 `manual_load.py --replace` 로 갈아끼운 날짜도 반드시 mart 를 재실행한다(fact 만 바뀌고 mart 는 그대로이므로).

## 8. company-verify 격리(선택)

`docs/operations/company-verify.md` 1단계(격리 DB `token_verify_fact/token_verify_dim/token_verify_mart`) 에 6c 를 얹는다:

```bash
./mart/token-metrics/install.sh --overlay company-verify --context "$KUBE_CONTEXT" --registry harbor.example.internal/<project> --tag <sha7> -n monitoring
```

- Secret 이름 `token-mart-metrics-ch-secret-verify`, CronJob `token-mart-metrics-verify`, DDL 은 `mart/token-metrics/ddl/company-verify/`
  (`tools/gen_verify_ddl.py` 출력 — DB 3종 치환).
- Secret 의 `CH_DB_FACT=token_verify_fact CH_DB_DIM=token_verify_dim CH_DB_MART=token_verify_mart`. 토큰 mart 참조
  `CH_DB_TOKEN_MART`/`CH_DB_TOKEN_DIM` 은 **운영 DB(`mart`/`gpu_data`) 로 지정**해 실제 토큰 집계와 결합한다(읽기 전용 —
  6c 는 토큰 mart 에 쓰지 않는다). 운영 토큰 mart 가 아직 없으면 격리 토큰 mart(`token_verify_mart`/`token_verify_dim` —
  company-verify 1단계가 만든 빈 테이블)를 가리켜 프리플라이트를 통과시키고 **GPU-only 검증** 으로 진행한다: 매 날짜
  `CHECK WARN token_mart_absent date=<d>` + M4 스킵(`rows_share=0`)이 정상이다.
- 격리 dim 4종은 §1 의 생성기에 `--target-db token_verify_dim` 을 붙여 만든다. 불변식은 §5 명령에
  `CH_DB_FACT=token_verify_fact CH_DB_DIM=token_verify_dim CH_DB_MART=token_verify_mart` 를 앞세워 실행한다.
- 2단계(정규) 전환 = §3 `--overlay company` 재설치 + 격리 CronJob `suspend`(§9 롤백과 같은 명령).

## 9. 트러블슈팅

| 증상 | 원인 | 조치 |
|---|---|---|
| `PREFLIGHT FAIL read_contract missing=…` (install `exit 3`) 또는 `BATCH_RESULT status=FAILURE … reason=read_contract` | 토큰 mart 미설치 / `CH_DB_TOKEN_MART`·`CH_DB_TOKEN_DIM` 오기 / 토큰 mart 컬럼 변경 | 기존 파이프라인 설치 확인, Secret DB명 확인, `app/preflight.py` `READ_CONTRACT` vs `DESCRIBE` 대조 — 계약 변경이면 6c 코드 수정(기존 모듈 무수정) |
| `reason=mutation_budget` | 한 Job 에 16일 초과 범위(> 64 변이) | 범위 축소 — §7 `rerun.py --chunk-days 7` 로 청크 실행 |
| `reason=verify_count` | 적재 후 재조회 행수 ≠ 기대(EXPECTED) — 동시 쓰기·복제 지연 | 활성 Job 0 확인 후 해당 날짜 재실행; 반복되면 `dup_suspect` WARN·`invariants_metrics` 확인 |
| `reason=sigterm` (`note=sigterm`) | `activeDeadlineSeconds`(1800) 초과·노드 축출 | 부분 적재 상태 — 같은 날짜 재실행(§7 멱등). 반복되면 범위 축소 |
| `CHECK WARN token_mart_absent date=<d>` | 그 날짜 토큰 mart 행 0(토큰 배치 미완·GPU-only 격리) | 정상 — M4 스킵. 토큰 배치(`token-mart-daily`) 완료 후 §7 로 재실행하면 M4 채워짐 |
| `CHECK WARN metrics_coverage missing=<n>` / M3 `metrics_missing` FAIL | enabled 서비스의 앵커(summary) 없음 — 6b 수집 실패·API 미응답·수기 미제출 | 6b 수집 로그(`token-metrics-collector` Job) 확인 → 수집 재실행(`--chain-mart`) 또는 `manual_load.py` 수기 적재 후 §7 |
| `quality_flag=no_tco` / 비용 NULL | `dim_token_gpu_tco` 에 그 기종·날짜 유효 TCO 없음 | §1 생성기로 TCO dim 갱신(`--effective-from` 은 실제 적용일) 후 해당 범위 §7 재실행 |
| `quality_flag=flagged` / `flagged_gpu_hours>0` | 6b 정규화 FAIL 플래그(`hours_over_count`·`unknown_violation`) 행 — C 에서 제외, 그룹 `unattributed` 로 | 서비스 제공 데이터 교정 요청 → `manual_load.py --replace` 또는 재수집 후 §7 |
| `CHECK WARN service_not_in_usage_registry service=<s>` | 메트릭 레지스트리에만 있고 토큰 레지스트리(`dim_token_service`)에 없는 서비스 | 토큰 파이프라인 endpoints 등록 여부 확인(정상일 수 있음 — GPU 만 보고하는 서비스) |
| 패널 11 `over_report=1` / 불변식 `idle_negative` | 보고 GPU 시간 > 배정 × 24 | `dim_token_gpu_allocation` 갱신 또는 서비스 보고값 교정 후 §7 |
| 대시보드 변수 `service_group` 비어 있음 | M1 0행(첫 배치 전) 또는 데이터소스 계정 GRANT 누락 | §4 첫 배치, `accounts.sql` 적용 확인 |

**롤백(설계 §7.5)** — CronJob 2개 `suspend` + 신규 테이블 DROP. 기존 토큰 파이프라인·`gpu_data.dim_token_model` 은 건드리지 않는다:

```bash
kubectl --context "$KUBE_CONTEXT" -n monitoring patch cronjob token-mart-metrics -p '{"spec":{"suspend":true}}'
kubectl --context "$KUBE_CONTEXT" -n monitoring patch cronjob token-metrics-collector -p '{"spec":{"suspend":true}}'
# 필요 시 테이블 제거 (admin — mart 4 + fact 4 + dim 5; ON CLUSTER 는 DDL 파일의 클러스터명과 동일)
kubectl --context "$KUBE_CONTEXT" -n "$CH_NS" exec "$CH_POD" -- clickhouse-client --multiquery -q "
DROP TABLE IF EXISTS mart.agg_token_model_cost_1d_dist ON CLUSTER 'gpu-monitoring';  DROP TABLE IF EXISTS mart.agg_token_model_cost_1d_local ON CLUSTER 'gpu-monitoring';
DROP TABLE IF EXISTS mart.token_metrics_check_1d_dist ON CLUSTER 'gpu-monitoring';   DROP TABLE IF EXISTS mart.token_metrics_check_1d_local ON CLUSTER 'gpu-monitoring';
DROP TABLE IF EXISTS mart.agg_token_model_share_1d_dist ON CLUSTER 'gpu-monitoring'; DROP TABLE IF EXISTS mart.agg_token_model_share_1d_local ON CLUSTER 'gpu-monitoring';
DROP TABLE IF EXISTS mart.agg_token_gpu_group_1d_dist ON CLUSTER 'gpu-monitoring';   DROP TABLE IF EXISTS mart.agg_token_gpu_group_1d_local ON CLUSTER 'gpu-monitoring';"
```

fact 4테이블(`fact.raw_token_metrics_*`, `fact.collect_audit_metrics_1d`)과 dim 5테이블(`gpu_data.dim_token_metrics_service`,
`dim_token_{model_alias,gpu_tco,gpu_allocation,vendor_price}`)의 DROP 은 각 모듈 `ddl/README.md` 의 목록대로 같은 형식으로 실행한다.
재설치는 §1 부터.
