# collectors/token-metrics

`GET /v1/metrics?date=YYYY-MM-DD`(GPU 시간·서빙 성능 백분위·엔진 정보)를 서비스별·일별로 수집해 ClickHouse `fact.raw_token_metrics_{gpu,serving,summary}_1d`에 적재하는 수집기. 클론 원본은 `collectors/token-usage`(절 구성·배포 골격이 같다)이지만 별도 모듈이다 — 이미지 `token-metrics-collector`, CronJob `token-metrics-collector`, 마커 `module=token-metrics`, 레지스트리 `gpu_data.dim_token_metrics_service`. VM push 없음 — 메트릭 지표는 mart-metrics가 만든다.

절: 실행 / 모드와 게이트 / 환경변수 / 배포 / 수기(manual-v0) 적재 / 재수행 / 부분 적재 복구 / 마커 / 검증 / DDL·뮤테이션 장부.

## 실행

로컬 준비(단위 테스트·수동 실행 공통):

    cd collectors/token-metrics
    pip install -r requirements-dev.txt

정기(regular) 모드 — 인자 없이 실행하면 batch_time = 지금(KST), 대상 날짜 = batch_time − 1일, 슬롯 = batch_time의 KST 시각(`slot=HH`), 최종 슬롯 여부 = 시각 ≥ `FINAL_HOUR_KST`(9):

    CH_HOST=127.0.0.1 CH_USER=default CH_PASSWORD= ENDPOINTS_FILE=endpoints.yaml python -m app.main

batch_time을 명시(정기 슬롯 재현 — 2026-09-10 데이터를 02시 슬롯으로; naive 입력은 KST로 해석):

    python -m app.main 2026-09-11T02:05:00+09:00

rerun 모드 — 날짜 범위(`--from`·`--to` 둘 다 필수, D0 ≤ D1). 앵커(summary 행)가 있는 (날짜, 서비스)는 `--replace` 없이는 `SKIPPED reason=already_loaded`, 있으면 DELETE×3 후 재적재:

    python -m app.main --from 2026-09-01 --to 2026-09-07
    python -m app.main --from 2026-09-01 --to 2026-09-07 --service "Mock Service A" --replace

manual-v0 모드 — CSV(gpu·serving 필수, engine 선택)를 API 대신 입력으로 쓴다. 전문·규칙은 `수기(manual-v0) 적재` 절:

    python -m app.main --manual-gpu /manual/gpu.csv --manual-serving /manual/serving.csv --manual-engine /manual/engine.csv \
      --from 2026-08-26 --to 2026-08-31 --generated-at 2026-09-01T09:00:00+09:00

종료 코드: `0` = FAILURE 서비스 없음(SKIPPED·NODATA 포함) / `1` = 서비스 하나라도 FAILURE / `2` = 인자·설정 오류(`--from`·`--to` 짝 누락, D0 > D1, `--replace`를 범위 없이 사용, `unknown service: <S>`, manual 파일 짝 누락, `config error: <Type>: <msg>`). `2`는 BATCH_RESULT 없이 끝난다.

## 모드와 게이트

모드는 CLI 인자로 정해진다 — 인자 없음/`batch_time` = 정기(regular), `--from/--to` = rerun, `--manual-*` = manual. 게이트(`api_since`·`until`)와 최종 슬롯 판정은 **정기 모드에만** 적용되고, 레지스트리 동기화(`gpu_data.dim_token_metrics_service` diff-sync)도 정기 모드에서만 한다. `enabled: false` 서비스는 모든 모드에서 `SKIPPED reason=disabled`.

| 모드 | 트리거 | 대상 날짜 | 앵커 존재 시 | 409 2회째 | 레지스트리 동기화 | 최종 슬롯 |
|---|---|---|---|---|---|---|
| 정기(regular) | CronJob `5 2-9 * * *`(KST 8슬롯) 또는 `python -m app.main [batch_time]` | batch_time − 1일 | `SKIPPED already_loaded`(뮤테이션 0; 앵커가 manual-v0면 `CHECK WARN manual_row_present=1`) | 비최종 슬롯 `SKIPPED not_ready` / 최종 슬롯(09시) `FAILURE not_ready_at_0900` | 함 | slot ≥ `FINAL_HOUR_KST`(09) → `final=1` |
| rerun | `tools/rerun.py` 또는 `--from D0 --to D1 [--service S] [--replace]` | 범위(D0..D1) | `--replace` 없으면 `SKIPPED already_loaded`, 있으면 DELETE×3(summary→gpu→serving) 후 재적재 | `FAILURE not_ready` | 안 함 | `final=0` |
| manual | `tools/manual_load.py` 또는 `--manual-gpu … --manual-serving … --from --to` | 범위(D0..D1) | rerun과 동일 | 해당 없음(API 호출 없음) | 안 함 | `final=0` |

404(보존 기간 밖)는 정기 모드 `FAILURE retention`, rerun `SKIPPED retention`. 409는 큐 끝에서 `min(max(Retry-After, 1), 300)`초 뒤 1회만 재방문한다. rerun·manual은 전 서비스 fetch/normalize를 마친 뒤 `replace_batch` 1회로 적재하고, 정기 모드는 서비스별 순차 적재다.

게이트·실패 사유(`SERVICE_RESULT … reason=<r>` 어휘 — 순서대로):

| reason | 상태 | 뜻 |
|---|---|---|
| `disabled` | SKIPPED | endpoints의 `enabled: false` — 모든 모드 |
| `before_since` | SKIPPED | 정기 모드에서 `target_date < apiSince`(`apiSince` 기본 `2026-09-09`) |
| `after_until` | SKIPPED | 정기 모드에서 `until < target_date`(`until`이 있는 서비스만) |
| `already_loaded` | SKIPPED | 앵커(summary 행) 존재 & `--replace` 없음 — 뮤테이션 0 |
| `not_ready` | SKIPPED(정기 비최종) / FAILURE(rerun) | 409 `data_not_ready` 2회째 |
| `not_ready_at_0900` | FAILURE | 정기 최종 슬롯(09시)에서 409 2회째 — 이 줄이 09:00 알림의 근거 |
| `retention` | FAILURE(정기) / SKIPPED(rerun) | 404 — 제공자 보존 기간 밖 |
| `retryable` | FAILURE | 429/5xx/네트워크 오류 — 내부 재시도 3회(지수 백오프) 소진 후에도 실패 |
| `permanent_error` | FAILURE | 400, 응답 본문 > `MAX_RESPONSE_BYTES`(5MB), `date` 에코 불일치, non-JSON, 보고서 구조 위반(`gpu`/`serving`이 배열 아님 등) — 재시도 없이 즉시 |
| `mutation_budget` | FAILURE | 예정 DELETE 합산이 `METRICS_MAX_MUTATIONS_PER_RUN`(45) 초과 — 적재 착수 전 차단, BATCH_RESULT에도 `reason=mutation_budget` |
| `load_budget` | FAILURE | 적재 착수 시점에 남은 시간이 `LOAD_BUDGET_S`(1200) 미만 — writer 호출 없음 |
| `deadline` | FAILURE | 큐 처리 중 남은 시간이 `LOAD_BUDGET_S`(1200) 미만이 되면 컷 — 즉 `SOFT_DEADLINE_MINUTES − LOAD_BUDGET_S`(기본 40분−20분=20분) 경과 시점에 남은 서비스 전부를 신규 fetch 없이 즉시 종료(`SOFT_DEADLINE_MINUTES`(40분) 자체는 적재 착수의 하드 스톱 — `load_budget` 행 참고) |
| `unknown_service` | (exit 2) | `--service`가 endpoints에 없음 — stderr `unknown service: <S>`, SERVICE_RESULT 없이 종료 |
| `invariant_broken` (및 `empty`) | FAILURE | 예약 — 현재 코드 경로 없음(둘 다 발생하지 않음). `Event.INVARIANT_BROKEN`·`Event.EMPTY`는 열거형에만 존재하고 `app/`의 어디에서도 raise되지 않는다(NODATA 판정은 `rows==0 and rejected==0`을 직접 보며 `Event.EMPTY`를 거치지 않는다) |
| `unexpected:<Type>` | FAILURE | fetch·normalize·writer의 예상 밖 예외 — `<Type>`은 파이썬 예외 클래스명 |

`rows == 0`이고 `rejected == 0`이면 `NODATA`, `rows == 0`이고 `rejected > 0`이면 `SUCCESS rows=0 rejected=<n>` + `CHECK WARN … all_rows_rejected=1`.

## 환경변수 (§5.2)

앱(`app/config.py`·`app/writer.py`)이 읽는 env 전부. 값의 출처: CronJob 리터럴(`k8s/base/cronjob.yaml`) / Secret `token-metrics-ch-secret[-verify]`(install.sh [2/7]) / install.sh [7/7] `set env`.

| 변수 | 기본값 | 출처 | 뜻 |
|---|---|---|---|
| `CH_HOST` | `localhost` | install.sh [7/7] `set env`(`<ch_pod 접두>.clickhouse.svc`) | ClickHouse HTTP 호스트 |
| `CH_PORT` | `8123` | Secret | ClickHouse HTTP 포트 |
| `CH_USER` | `default` | Secret(프롬프트 기본 `mart`; company-verify `token_verify`) | 적재 계정 — GRANT는 `ddl/<env>/accounts.sql` |
| `CH_PASSWORD` | `""` | Secret | 적재 계정 비밀번호 |
| `CH_CLUSTER` | `""` | Secret(company·company-verify `gpu-monitoring`, stage 빈 값) | 비어 있지 않으면 `ALTER … DELETE`에 `ON CLUSTER` 부착 |
| `CH_DB_FACT` | `fact` | Secret — **company-verify 전용**(`token_verify_fact`) | fact 4테이블의 DB |
| `CH_DB_DIM` | `gpu_data` | Secret — **company-verify 전용**(`token_verify_dim`) | 레지스트리·프리플라이트 DB |
| `ENDPOINTS_FILE` | `endpoints.yaml` | CronJob 리터럴 `/etc/token-metrics/endpoints.yaml` | 서비스 목록(ConfigMap `token-metrics-endpoints[-verify]` 마운트) |
| `SOFT_DEADLINE_MINUTES` | `40` | CronJob 리터럴 | 잡 소프트 데드라인 — 마지막 `LOAD_BUDGET_S`초는 적재 전용으로 예약되므로, 남은 서비스는 이 값 도달 전인 `SOFT_DEADLINE_MINUTES − LOAD_BUDGET_S`(기본 40−20=20분) 경과 시점에 이미 `FAILURE deadline`으로 컷된다 — `SOFT_DEADLINE_MINUTES`(40분) 자체는 적재 착수의 하드 스톱(`FAILURE load_budget`) |
| `LOAD_BUDGET_S` | `1200` | CronJob 리터럴 | 적재 착수에 필요한 잔여 시간 — 부족하면 `FAILURE load_budget`. 불변식 `SOFT_DEADLINE_MINUTES*60 > LOAD_BUDGET_S`(위반 시 `config error: ValueError: SOFT_DEADLINE_MINUTES*60 must exceed LOAD_BUDGET_S`, exit 2) |
| `FINAL_HOUR_KST` | `9` | CronJob 리터럴 | 정기 모드 최종 슬롯 시각 — batch_time KST 시각 ≥ 이 값이면 `final=1` |
| `MAX_RESPONSE_BYTES` | `5000000` | CronJob 리터럴 | `/v1/metrics` 응답 상한(초과 = `FAILURE`) |
| `METRICS_MAX_MUTATIONS_PER_RUN` | `45` | CronJob 리터럴 | 실행당 예정 DELETE 상한(§4.0 = 3 × 15) — 초과면 적재 전 `FAILURE mutation_budget` |
| `COLLECTOR_HTTPS_PROXY` | 미설정 | Secret(install.sh [2/7] 프롬프트: `none` → 빈 값, enter → 키 없음, 값) | 미설정 = 시스템 프록시 상속 / `""` = 직접 연결 / 값 = 전용 프록시(제공자 API 호출에만 적용, ClickHouse에는 미적용) |
| `COLLECTOR_API_VERIFY` | `true` | 수동(Secret에 직접 넣을 때만) | `false`면 제공자 TLS 검증 끔 — stage 자체서명 실험용, company 금지 |
| `COLLECTOR_API_CA_BUNDLE` | 미설정 | Secret(CA 파일 입력 시 `/etc/token-metrics-ca/ca-bundle.pem`) + ConfigMap `token-metrics-ca-bundle` | 사내 CA 번들 경로 — 있으면 `verify=<경로>` |

`VM_PUSH_URL`은 없다 — VM push 없음, 메트릭 지표는 mart-metrics가 만든다. `CH_DB_FACT`/`CH_DB_DIM`은 격리 검증(company-verify) 전용이며 stage/company Secret에는 키 자체가 없다(앱 기본값 사용).

endpoints 파일 키(`services:` 목록 원소): `serviceGroup`, `service`, `baseUrl`, `enabled`, `apiSince`(기본 `2026-09-09` — 이 날짜 전은 정기 모드 `before_since`), `coverageSince`(기본 `2026-08-26` — 수기 적재 시작일, 레지스트리 컬럼), `until`(선택 — 지나면 `after_until`), `expectGpu`, `expectServing`, `usageIncludesConsumers`, `note`. 정본: stage `endpoints.yaml`(커밋), company `endpoints-metrics.company.yaml`(gitignore).

## 배포 (§5.6)

이미지 빌드+푸시(항상 둘 다; 태그 기본 = git short SHA; stage 레지스트리 기본 `ghcr.io/yoonsungnam`, company는 `--registry` 필수):

    ./collectors/token-metrics/build.sh stage
    ./collectors/token-metrics/build.sh --registry harbor.example.internal/gpu-monitoring --tag <sha7> company

설치(대화형 — Secret 값 프롬프트). company-verify는 별도 이미지가 없다(company 이미지를 그대로 사용):

    ./collectors/token-metrics/install.sh stage
    ./collectors/token-metrics/install.sh --registry harbor.example.internal/gpu-monitoring --tag <sha7> --context <ctx> company
    ./collectors/token-metrics/install.sh --registry harbor.example.internal/gpu-monitoring --tag <sha7> --context <ctx> company-verify

install.sh 7단계(§5.6):

| 단계 | 내용 |
|---|---|
| [1/7] | `registry-pull-secret` — **없을 때만** 생성. 있으면 `이미 존재합니다 — 네임스페이스 공유 Secret이므로 갱신하지 않습니다`(기존 수집기와 공유, 프롬프트 없음) |
| [2/7] | `token-metrics-ch-secret[-verify]` 멱등 생성 — `CH_USER`(기본 `mart` / verify `token_verify`), `CH_PASSWORD`, `CH_PORT=8123`, `CH_CLUSTER`(`gpu-monitoring` / stage 빈 값), company-verify만 `CH_DB_FACT`/`CH_DB_DIM`, `COLLECTOR_HTTPS_PROXY`(`none`/enter/값), CA 파일 입력 시 `COLLECTOR_API_CA_BUNDLE` + ConfigMap `token-metrics-ca-bundle` |
| [3/7] | ConfigMap `token-metrics-endpoints[-verify]`(키 `endpoints.yaml`) — 원본 stage `endpoints.yaml`, company `endpoints-metrics.company.yaml`(gitignore), `--endpoints F`로 대체 가능 |
| [4/7] | 프리플라이트 — **앱 계정(`CH_USER`/`CH_PASSWORD`)으로** `clickhouse-client` 실행해 GRANT까지 검증한다. 비밀번호는 `kubectl exec` argv에 넣지 않고(API 서버 감사 이벤트에 남는다) here-string으로 파드 stdin에 실어 `clickhouse-client`가 표준입력에서 읽게 한다. 접속 자체가 안 되면 `[ERROR] 프리플라이트 실패: ClickHouse 접속 불가 (계정 <CH_USER>)` exit 1; 접속은 되지만 `SELECT count() FROM system.databases WHERE name IN ('fact','gpu_data')`(verify는 `token_verify_*`)의 반환값이 기대 개수(1개 또는 2개)와 다르면 `[ERROR] 프리플라이트 실패: DB 부재 또는 GRANT 누락 — admin이 ddl/<env>/accounts.sql 실행 필요` exit 1; `SELECT count() FROM gpu_data.dim_token_service_dist`(토큰 레지스트리)가 실패하면 `[ERROR] 프리플라이트 실패: 토큰 레지스트리 SELECT 불가(GRANT 누락) — admin이 ddl/<env>/accounts.sql 실행 필요` exit 1. [2/7]을 건너뛴 재설치는 기존 Secret의 계정을 읽어 쓴다 |
| [5/7] | DDL 2파일 적용 — `ddl/<env>/raw_token_metrics.sql` + `ddl/<env>/dim_token_metrics_service.sql`. `ddl/<env>/accounts.sql`(GRANT)은 **admin이 수동 실행**(§4.0) |
| [6/7] | `kubectl apply -k k8s/overlays/<env> -n monitoring` |
| [7/7] | `kubectl set image cronjob/token-metrics-collector token-metrics-collector=<REGISTRY>/token-metrics-collector:<TAG>` + `kubectl set env cronjob/token-metrics-collector CH_HOST=<ch_pod 접두>.clickhouse.svc` + 수동 테스트 명령 안내 |

CronJob `token-metrics-collector`(company-verify는 `token-metrics-collector-verify`) 값 — `k8s/base/cronjob.yaml`:

| 항목 | 값 |
|---|---|
| schedule | `5 2-9 * * *`, `timeZone: Asia/Seoul`(02:05~09:05 KST 8슬롯 — 09시 슬롯이 최종 `final=1`) |
| concurrencyPolicy / startingDeadlineSeconds | `Forbid` / `540` |
| activeDeadlineSeconds / backoffLimit / restartPolicy | `3000` / `0` / `Never`(재시도 없음 — 슬롯 실패는 다음 슬롯이 받는다) |
| history | successful 3 / failed 3 |
| resources | requests `100m`/`256Mi`, limits `1`/`1Gi` |
| env | `envFrom` Secret + 리터럴 `ENDPOINTS_FILE=/etc/token-metrics/endpoints.yaml`, `SOFT_DEADLINE_MINUTES=40`, `LOAD_BUDGET_S=1200`, `FINAL_HOUR_KST=9`, `MAX_RESPONSE_BYTES=5000000`, `METRICS_MAX_MUTATIONS_PER_RUN=45` |
| volumes | `[0] endpoints` → `/etc/token-metrics`, `[1] ca-bundle`(optional) → `/etc/token-metrics-ca`; manual Job은 `[2] manual` → `/manual`을 추가한다 |
| label | `app: token-metrics-collector` |

주의: install.sh 밖에서 `kubectl apply -k`를 직접 재실행하면 이미지가 `latest`로 리셋된다 — 재적용은 항상 install.sh 경유(`[7/7]`가 `set image`로 다시 덮는다). 이미지 태그는 `.github/workflows/release-images-metrics.yml`이 만드는 sha7(`main` 푸시마다 `ghcr.io/yoonsungnam/token-metrics-collector:<sha7>`); company는 같은 sha7로 `build.sh --registry … company`가 사내 레지스트리에 푸시한다. `k8s/overlays/company`에는 사내 주소를 두지 않는다(§7.2).

## 수기(manual-v0) 적재 (§5.5)

go-live(`apiSince`, 기본 2026-09-09) 이전 구간(`coverageSince`, 기본 2026-08-26 이후)은 서비스 담당자가 CSV로 제출하고 운영자가 적재한다. API 경로와 **같은 normalize·replace 경로**를 타며 `source_type=manual-v0`로 앵커가 남는다.

템플릿(주석·예시 행 포함 — 예시 행은 삭제 후 실값으로 교체):

    docs/templates/token_metrics_manual_v0_gpu.csv
    docs/templates/token_metrics_manual_v0_serving.csv
    docs/templates/token_metrics_manual_v0_engine.csv

헤더 3줄(바이트 동일 요구 — 첫 비주석 줄):

    date,service,model,gpuType,category,gpuCount,gpuHours
    date,service,model,metric,name,unit,p50,p90,p95,p99
    service,engine_type,engine_version

규칙(파서 `app/manual.py` — 위반은 `manual input error: <경로>:<줄번호>: <what>`로 exit 2, 적재 없음; `<what>`은 고정 문구다 — `header mismatch`, `header missing`, `unparsable line`(csv 모듈이 줄을 못 읽음, 예: NUL 바이트), `unknown service (not in endpoints)`, `bad date`, `bad metric`, `duplicate (model, metric)`, `duplicate service`; 컬럼 수 불일치만 정수를 채운 `column count <n> != <m>`):
- `service`는 endpoints에 등록된 서비스만(`apiSince`는 무시 — **날짜 제약 없음**). 미등록 `--service`는 `unknown service: <S>`. `enabled: false` 서비스도 파싱은 통과한다 — 거부되지 않고 다른 모드와 동일하게 `SKIPPED reason=disabled`(위 모드와 게이트 절 참고).
- 같은 (date, service, model, metric)의 serving 행 중복은 오류(`(model, metric)` 중복 금지). `metric`은 API 키 그대로 `ttftMs | itlMs | outputTps | e2eMs | custom`(fact 표기 `ttft_ms` 등으로의 변환은 normalize가 한다).
- `custom`은 `name`·`unit` 필수. 표준 지표 행의 `name`·`unit` 셀은 값이 있어도 무시한다(거부하지 않음 — 파서가 조용히 버린다). `outputTps`는 `p50`만, 나머지 표준 지표는 `p50..p99` 4개 모두.
- `#`로 시작하는 줄은 주석(안의 쉼표 무시), UTF-8 BOM 허용(`utf-8-sig`), 빈 셀 = 부재. 숫자 검증(형·범위·행 거부)과 플래그(`gpuHours > gpuCount × 24` → `hours_over_count`, `p50 ≤ p90 ≤ p95 ≤ p99` 역전 → `pct_non_monotone` — 적재하되 `flags`에 표기)는 normalize 한 곳에서만 — 파서는 형태만 만든다.
- `--from/--to` 밖의 날짜 행은 `rows_outside_range`, `--service` 지정 시 다른 서비스 행은 `rows_other_service`로 세고 버린다(오류 아님) — **둘 다 해당하면 `rows_other_service`만 센다**(파서가 `--service` 필터를 날짜 범위 필터보다 먼저 본다). 행이 하나도 없는 (date, service)는 **페이로드를 만들지 않는다** — 앵커가 남지 않으며(`NODATA` 앵커 아님) mart 불변식 `metrics_missing`이 그 날을 "수기 입력 없음"으로 본다; `--from/--to` 범위 안이라도 CSV에 행이 없으면 그 (date, service)에는 아무것도 적재되지 않는다(제출 누락과 실제 0행을 CSV로는 구분할 수 없으므로 완결 표시를 심지 않는다).
- 기존 앵커(API·manual 불문)가 있으면 `--replace` 없이는 `SKIPPED already_loaded`. 레지스트리 동기화는 하지 않는다.
- **`--replace` 실행 시각**: `tools/manual_load.py`는 실행 창(10:50 KST)·활성 mart Job을 검사하지 않는다(`tools/rerun.py`와 달리). `--replace`는 (date, service)마다 DELETE×3 + INSERT를 내므로 mart-metrics 10:20 배치(activeDeadlineSeconds 1800 → 늦어도 10:50 종료)와 겹치지 않게 **10:50 KST 이후·활성 `token-mart-metrics` Job 0일 때** 실행한다 — 운영자 확인: `kubectl get jobs -n monitoring | grep token-mart-`. 앵커 없는 첫 적재(`--replace` 없음)는 INSERT뿐이라 시각 제약이 없다.

P0 경로 — k8s Job(운영자 워크스테이션에는 kubectl만 있으면 된다; ClickHouse 접근·프록시·CA 불필요). 실제 제출 파일은 `*manual_metrics*.csv` 이름으로 저장한다(`.gitignore` — 레포 반입 금지):

    python3 collectors/token-metrics/tools/manual_load.py --context <ctx> --namespace monitoring \
      [--cronjob token-metrics-collector-verify] \
      --from 2026-08-26 --to 2026-08-31 \
      --gpu gpu_manual_metrics.csv --serving serving_manual_metrics.csv --engine engine_manual_metrics.csv \
      --generated-at 2026-09-01T09:00:00+09:00 --replace

흐름: CSV 3파일 → ConfigMap `token-metrics-manual-<YYYYMMDDHHMMSS>`(`kubectl create`) → `--cronjob`(기본 `token-metrics-collector`; company-verify는 `token-metrics-collector-verify`) 템플릿에서 Job `token-metrics-collector-manual-<ts>` 생성(`/manual` 볼륨 마운트, command `python -m app.main --manual-gpu /manual/gpu.csv --manual-serving /manual/serving.csv [--manual-engine /manual/engine.csv] --from D0 --to D1 [--service S] [--replace] [--generated-at ISO]`) → 로그 스트리밍(`--timeout-s` 기본 3600) → 종료 시 ConfigMap 삭제(`--keep-configmap`이면 보존; Job이 성공하지 못했으면 — rc≠0 또는 중단 — 삭제 직전 stderr `[WARN] Job이 아직 실행 중이면 입력 ConfigMap 삭제로 실패합니다 — 상태: kubectl --context=<ctx> get job <job> -n <ns>`를 찍지만 `--keep-configmap`이 아닌 한 ConfigMap은 그대로 삭제한다), Job 오브젝트는 로그 재조회용으로 남긴다. 시작 시 `[INFO] configmap=<name> job=<job> files=gpu.csv,serving.csv[,engine.csv] bytes=<n>`, 성공 시 `[NEXT] manual 적재 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3): python3 mart/token-metrics/tools/rerun.py --context <ctx> --namespace monitoring --from D0 --to D1`. CSV가 UTF-8이 아니면(예: 엑셀 CP949 저장) `[ERROR] UTF-8 아님 — 엑셀에서는 'CSV UTF-8'로 저장 후 다시 제출` exit 2. CSV 합계는 900,000 bytes(ConfigMap 상한) 이하 — 초과면 `[ERROR] CSV 합계 <n> bytes > 900000 — 날짜 범위를 나눠 제출` exit 2. `--replace` 범위가 15일을 넘으면(변이 예산 45/3, 뮤테이션 산식 절) `[ERROR] --replace는 한 번에 15일 이하만 가능합니다(변이 예산 45/3) — --from/--to 범위를 나눠 제출` exit 2 — 전부 kubectl 호출 전(클러스터 무변경). kubectl 호출 자체가 실패하면(인증 만료·잘못된 context·API 서버 다운 등) `tools/rerun.py`와 같은 형식으로 트레이스백 대신 stderr `[ERROR] kubectl 실패 (rc=<n>): <argv>` + kubectl의 stderr를 그대로 찍고 exit 1(ConfigMap이 이미 만들어졌으면 finally가 그대로 정리한다). 종료 코드 `0`(적재 성공) / `1`(Job 실패·타임아웃·kubectl 실패) / `2`(인자·파일·인코딩·크기·`--replace` 범위 오류 — kubectl 호출 전).

Job 정리:

    kubectl --context <ctx> -n monitoring delete job -l app=token-metrics-collector,manual=1

워크스테이션 직접 실행(대안 — ClickHouse에 직접 붙는다; 제공자 API 호출은 없으므로 프록시·CA는 불필요):

    kubectl --context <ctx> -n clickhouse port-forward svc/<chi-headless> 8123:8123
    cd collectors/token-metrics
    CH_HOST=127.0.0.1 CH_PORT=8123 CH_USER=mart CH_PASSWORD=<비밀번호> CH_CLUSTER=gpu-monitoring \
      ENDPOINTS_FILE=endpoints-metrics.company.yaml \
      python -m app.main --manual-gpu gpu_manual_metrics.csv --manual-serving serving_manual_metrics.csv \
        --manual-engine engine_manual_metrics.csv --from 2026-08-26 --to 2026-08-31 \
        --generated-at 2026-09-01T09:00:00+09:00 --replace

`svc/<chi-headless>`는 install.sh [7/7]가 산출하는 `CH_HOST=<ch_pod 접두>.clickhouse.svc`와 같은 서비스다(클러스터마다 이름이 달라 플레이스홀더로 표기).

`--generated-at`은 제공자 기준 산출 시각(KST `+09:00`; 다른 오프셋은 `CHECK WARN generated_at_offset_mismatch`, 파싱 실패는 `generated_at_parse_failed` — 적재는 계속). 생략하면 적재 시각.

적재 후 **같은 날짜 범위의 mart-metrics rerun은 의무**다(§6.3 — manual_load.py는 안내만 하고 체인하지 않는다; 실행 창 10:50 KST 검사는 mart rerun 자신이 한다). 로그의 `MANUAL_INPUT module=token-metrics rows_gpu=<n> rows_serving=<n> rows_engine=<n> rows_outside_range=<n> rows_other_service=<n>` 1줄(SERVICE_RESULT보다 앞)로 파서가 읽은 행 수를 확인한다 — 페이로드·행 원문은 로그에 남지 않는다.

## 재수행 (§6.3)

날짜 범위 재수집은 워크스테이션에서 `tools/rerun.py`로 한다(kubectl만 필요). CronJob 템플릿에서 Job을 만들어 파드 로그를 스트리밍한다:

    python3 collectors/token-metrics/tools/rerun.py --context <ctx> --namespace monitoring \
      --from 2026-09-01 --to 2026-09-20 [--service "Mock Service A"] [--replace] [--chunk-days 7] [--chain-mart] [--force-window] \
      [--cronjob token-metrics-collector-verify]

- `--from/--to`는 필수 쌍(인자 없이 "어제 1일" 모드는 없다 — 정기 8슬롯이 그 역할). `--from > --to`, 날짜 형식 오류, `--chunk-days`가 `1..15` 밖이면 exit 2(상한 15 = 뮤테이션 예산 45 ÷ 날짜당 3 — 아래 산식).
- **실행 창**: KST 10:50 이후이고 활성 `token-mart-metrics` Job(company-verify는 `token-mart-metrics-verify`)이 0일 때만 실행한다(`--chain-mart` 유무와 무관 — 수집기의 DELETE/INSERT가 mart-metrics 10:20 배치의 fact SELECT와 겹치지 않게). 밖이면 stderr `[ERROR] 실행 창 밖: <window_closed|mart_job_active> — KST 10:50 이후·활성 token-mart-metrics Job 0일 때 재시도 (--force-window로 강제)` exit 3. `--force-window`는 검사를 건너뛰고 `[WARN] 실행 창 검사 생략(--force-window)`을 찍는다. 이 검사는 짝이 되는 mart CronJob의 Job(소유 Job + `token-mart-metrics-rerun-*`)만 센다 — 대상이 prod(`--cronjob`이 `-verify`로 끝나지 않을 때)이면 이름이 `token-mart-metrics-verify-`로 시작하는 Job(같은 네임스페이스의 company-verify 오버레이 rerun)은 이 카운트에서 제외한다(소유 Job 매치는 이름이 아니라 ownerReferences 값이라 영향받지 않는다). mart rerun 자신은 `token-mart-*` 전부(기존 수집기 mart 포함)를 세므로 아래 `--chain-mart` 단계에서 6c가 추가로 거부할 수 있다.
- **청크 = Job 1개**: 범위를 앞에서부터 `--chunk-days`(기본 7)일씩 잘라 청크마다 Job `token-metrics-collector-rerun-<epoch>-<i>`(라벨 `app=token-metrics-collector,rerun=1`, command `python -m app.main --from <c0> --to <c1> [--service S] [--replace]`)를 만든다. `activeDeadlineSeconds: 3000`은 CronJob 값을 그대로 상속하고 클라이언트 대기는 3600초. 진행 표시 `[INFO] 청크 <i>/<n>: <c0> .. <c1> → Job <name>`. 청크 Job은 `ttlSecondsAfterFinished: 86400`(CronJob 템플릿에 이미 값이 있으면 그 값을 유지 — `setdefault`)로 생성되어 완료 24시간 뒤 k8s가 자동 삭제한다(CronJob 소유가 아닌 1회성 Job이라 `successfulJobsHistoryLimit`의 GC 대상이 아니므로 별도 TTL이 필요하다).
- 청크가 실패(Job Failed·타임아웃)하면 이후 청크를 만들지 않고 stderr `[ERROR] 청크 <i>/<n> 실패 — 이후 청크 중단; 재시도: --from <c0> --to <to> (그 외 인자 동일)` exit 1. 앞선 성공 청크는 `--replace` 없이 재실행해도 `already_loaded`로 스킵되므로 안내된 범위로 그대로 재시도한다.
- 앵커가 있는 (날짜, 서비스)는 `--replace` 없이는 `SKIPPED already_loaded`; `--replace`는 날짜마다 DELETE×3(summary→gpu→serving) 후 재적재하고 감사 행(`collect_audit_metrics_1d`)을 1행 남긴다. 409는 큐 끝 1회 재방문 뒤에도 409면 `FAILURE not_ready`, 404는 `SKIPPED retention`.
- **mart-metrics rerun 의무**: 전 청크 성공 시 `[NEXT] collectors rerun 후 동일 날짜 mart-metrics rerun은 의무입니다 (§6.3):` + 다음 줄에 실행할 명령을 찍는다. `--chain-mart`를 주면 **청크 분할 전 전체 범위**를 `python3 mart/token-metrics/tools/rerun.py --context <ctx> --namespace monitoring [--cronjob token-mart-metrics-verify] --from <D0> --to <D1> --chunk-days <n> [--force]`에 그대로 전파해 실행한다(수집기가 스킵한 날짜 포함 — mart가 자기 판단으로 재계산; 종료 코드는 mart rerun 값). 전파 규칙: `--cronjob …-verify`(company-verify)면 mart 쪽 `--cronjob token-mart-metrics-verify`, `--force-window`면 mart 쪽 `--force`(6c는 10:50 창 검사만 생략). **6c는 활성 `token-mart-*` Job이 하나라도 있으면 `--force`와 무관하게 `RERUN REFUSED active_jobs=<n>` exit 2로 거부한다** — 수집 청크는 이미 끝났으므로 다른 mart Job이 끝난 뒤 `[NEXT]`에 찍힌 명령을 그대로 다시 실행하면 된다(수집기를 다시 돌릴 필요 없음). 그 파일이 아직 없으면 stderr `[ERROR] --chain-mart: mart/token-metrics/tools/rerun.py 가 아직 없습니다 (Plan 6c 전) — mart-metrics 구현 후 위 명령을 실행하세요.` exit 1.
- kubectl 호출 자체가 실패하면(인증 만료·잘못된 context·API 서버 다운 등) 트레이스백 대신 stderr `[ERROR] kubectl 실패 (rc=<n>): <argv>` + kubectl의 stderr를 그대로 찍고 exit 1(청크 실패와 같은 종료 코드 공간).
- **주의(CronJob 격리와 무관)**: rerun Job은 CronJob이 소유한 Job이 아니라 독립 Job이라 CronJob의 `concurrencyPolicy: Forbid`에 걸리지 않는다 — 심야에 여러 청크를 도는 rerun이 02:05~09:05 KST 정기 슬롯과 겹칠 수 있다. 실행 창(기본 10:50 이후) 안에서 돌려 정기 슬롯과 자연히 겹치지 않게 하거나, 겹침을 의식적으로 감수하고 `--force-window`로 강행한다.
- 종료 코드: `0` 전 청크 성공(+ `--chain-mart`면 mart rerun 반환값 — 6c 거부는 `2`) / `1` Job 실패·타임아웃·mart rerun 파일 부재·kubectl 실패 / `2` 사용법 / `3` 실행 창 밖.

정기 슬롯 1회를 수동으로 재현(어제 날짜, 현재 시각을 슬롯으로 — 실행 창 검사 없음, 앵커가 있으면 `already_loaded`):

    kubectl --context <ctx> create job --from=cronjob/token-metrics-collector token-metrics-collector-manual-$(date +%s) -n monitoring

뮤테이션 산식(§4.0 — 가드 `METRICS_MAX_MUTATIONS_PER_RUN=45`는 적재 착수 전에 예정 DELETE 합산을 검사한다):
- `--replace` rerun·manual 배치: 날짜당 DELETE ≤3(서비스 수와 무관 — 한 날짜의 전 서비스를 `IN (...)`으로 한 번에 지운다) → 45/3 = **15일/실행**. `--chunk-days 7`이면 청크당 21 ≤ 45로 항상 통과하고, `--chunk-days 15`가 한 Job의 상한이다(`tools/rerun.py`가 `CHUNK_DAYS_MAX = 15`로 정적 거부 — 16 이상은 exit 2). `tools/manual_load.py`에는 `--chunk-days`가 없다 — 대신 `--replace` 범위 자체를 15일로 정적 거부한다(`REPLACE_DAYS_MAX = 15` — 16일 이상은 `[ERROR] --replace는 한 번에 15일 이하만 가능합니다(변이 예산 45/3) — --from/--to 범위를 나눠 제출` exit 2, kubectl 호출 전).
- 정기 실행의 부분 적재 복구(아래 절): (date, service)쌍당 3 → **15쌍/실행**.
- 초과하면 적재 없이 `SERVICE_RESULT … FAILURE reason=mutation_budget` + `BATCH_RESULT … reason=mutation_budget`(exit 1)이 되는 것은 `tools/rerun.py` 청크 안에서다 — 그때는 `--chunk-days`를 줄이거나 `--service`로 나눠 재시도한다. `tools/manual_load.py --replace`는 위 15일 상한이 범위 자체를 미리 막으므로 이 경로를 타지 않는다. `--replace` 없이 여러 날짜를 한 Job에 넣다가 부분 적재 잔여물 복구(날짜당 ≤3)로 예산을 넘으면, 예산 검사는 날짜마다 이뤄지므로 **그 시점까지의 날짜는 이미 적재됐고 이후 날짜들만** `FAILURE reason=mutation_budget`로 끝난다(부분 적재) — `--from/--to` 범위를 나눠 여러 번 제출한다. 실측은 `system.mutations`(DDL·뮤테이션 장부 절).

rerun Job 정리(로그 재조회가 끝난 뒤):

    kubectl --context <ctx> -n monitoring delete job -l app=token-metrics-collector,rerun=1

## 부분 적재 복구 (§5.4)

적재 순서(크래시 안전): 존재확인 3종(summary·gpu·serving) → 감사 행 → DELETE summary→gpu→serving → INSERT gpu→serving→summary. 앵커 = summary 행(INSERT 마지막·DELETE 첫 번째)이라 "앵커 있음 = 그 (date, service)는 완결"이 성립한다.

앵커 없이 gpu/serving 행만 남은 (date, service)는 '부분 적재'(이전 실행이 INSERT 도중 중단)다. 다음 실행(정기·rerun·manual 불문)은 already_loaded 게이트를 통과하고, `replace_batch`가 존재확인 3종의 합집합으로 잔여 행을 감지해 DELETE×3(summary→gpu→serving) 후 재적재한다 — 정기 실행에서 뮤테이션(3)이 생기는 유일한 경우이며, 감사 행은 앵커가 있던 세대만 남으므로 이 경우 `collect_audit_metrics_1d`에는 행이 추가되지 않는다. 로그는 `SERVICE_RESULT status=SUCCESS`이고 별도 CHECK 코드는 없다(뮤테이션 실측은 `system.mutations` — 아래 장부 절). 운영자 개입 불필요; 다만 정기 실행이 `mutation_budget`으로 실패하면 부분 적재가 15쌍 이상 누적된 것이므로 `tools/rerun.py --replace`를 `--service`로 나눠 실행한다.

복구 주체: (i) date = 오늘−1이고 남은 슬롯이 있으면 다음 정기 슬롯이, (ii) 그 외(어제가 아닌 날짜, 09시 최종 슬롯 이후)는 운영자 `tools/rerun.py --from D --to D`가 복구한다 — 부분 적재는 앵커가 없으므로 **`--replace`가 필요 없다**(잔여 행 감지가 DELETE를 한다). (iii) 제공자 보존 기간 밖(404 → `retention`)이라 API로 다시 받을 수 없는 날짜와 manual-v0로 넣었던 날짜는 `tools/manual_load.py`로 같은 CSV를 재적재한다(앵커가 없으므로 `--replace` 불필요 — 잔여 행 감지가 DELETE×3 후 재적재; CSV는 제출자가 보관한 원본을 다시 쓴다).

## 마커 (§5.6)

파드 로그 1줄 = 1마커. 알림 규칙·대시보드는 이 줄만 grep한다.

    SERVICE_RESULT status=<SUCCESS|NODATA|SKIPPED|FAILURE> module=token-metrics service=<svc> source_type=<metrics-api-v1|manual-v0> rows=<n> pages=1 warn=<n> rejected=<n>[ reason=<r>]
    BATCH_RESULT status=<SUCCESS|NODATA|FAILURE> module=token-metrics services_ok=<n> services_failed=<n> services_skipped=<n> rows=<n> elapsed=<n>s slot=<HH> final=<0|1>[ reason=<r>]
    CHECK WARN service=<svc> <code>=<count>
    MANUAL_INPUT module=token-metrics rows_gpu=<n> rows_serving=<n> rows_engine=<n> rows_outside_range=<n> rows_other_service=<n>

`<svc>`는 서비스 정본 표기 그대로 찍힌다(공백 포함 가능 — 예 `service=Mock Service A`). 값 자체에는 공백이 없다는 규약은 각 필드(status·module·rows 등)에만 적용된다.

- `SERVICE_RESULT`는 서비스마다 1줄(`CHECK WARN` 줄들이 그 앞), `BATCH_RESULT`는 잡당 1줄(마지막). `pages=1`은 고정(`/v1/metrics`는 페이지가 없다). `reason`은 모드와 게이트 절의 어휘.
- `slot=HH`는 batch_time의 KST 시각(정기 8슬롯 `02`..`09`; rerun·manual은 실행 시각), `final=1`은 정기 모드에서 시각 ≥ `FINAL_HOUR_KST`(09)일 때만. 09시 슬롯의 `BATCH_RESULT … final=1` 줄 부재 = 그날 수집 실패(§7.5 알림 근거). `BATCH_RESULT status`: FAILURE 1개라도 있으면 `FAILURE`, 전부 `NODATA`면 `NODATA`, 그 외(전부 SKIPPED 포함) `SUCCESS`; `services_ok` = SUCCESS+NODATA 수. `reason=mutation_budget`은 BATCH_RESULT에도 붙는다.
- SIGTERM(activeDeadlineSeconds 3000·노드 축출)을 받으면 마지막으로 계산된 `BATCH_RESULT` 줄에 ` note=sigterm`을 붙여 다시 찍고 종료한다 — 그 줄의 수치는 중단 시점까지의 누계.
- `CHECK WARN` 코드 어휘(행 플래그는 거부가 아니라 **적재하되 `flags` 컬럼에 표기** — mart가 판단): `hours_over_count`(gpuHours > gpuCount×24인 행 수), `unknown_violation`(serving/standby 용도의 `model=unknown` 행 수), `dup_merged`(gpu 동일 키 `(model, gpuType, category)`로 합산된 원행 수), `pct_non_monotone`(p50≤p90≤p95≤p99 역전 행 수), `dup_model_kept_first`·`dup_custom_kept_first`(serving 중복 레코드/custom name — 첫 것 유지, 버린 수), `identity_drift`(응답의 serviceGroup/service가 레지스트리와 다름), `generated_at_parse_failed`·`generated_at_offset_mismatch`(`generatedAt` 파싱 실패 / 오프셋 ≠ +09:00), `engine_malformed`, `extra_top_keys`(응답 최상위 미지 키 수 — 설계 §5.3은 "무시"라고 쓰지만 구현은 **WARN으로 관측**한다: 적재는 그대로 하되 카운트로 남긴다, §5.3 표기와의 의도적 편차), `all_rows_rejected`(rows=0 & rejected>0), `manual_row_present`(정기 모드에서 manual-v0 앵커 발견 — API로 덮으려면 `rerun.py --replace`), `registry_sync_failed`(`service=-` — 레지스트리 diff-sync 실패, 수집은 계속; DELETE 뒤 INSERT가 실패한 경우에는 stderr에 `[WARN] registry_sync: DELETE 후 INSERT 실패 — 다음 정규 슬롯까지 dim_token_metrics_service가 비어 있을 수 있음`도 찍힌다 — 다음 정규 슬롯의 sync가 다시 채운다).
- `MANUAL_INPUT`은 manual 모드에서 실행당 1줄(SERVICE_RESULT보다 앞) — 파서가 읽은 행 수만.
- 로깅 계약(§3 전제 11): 페이로드·CSV 행 원문·모델명 목록을 로그에 쓰지 않는다 — 코드·카운트·서비스명만. 파서 오류도 `<경로>:<줄>: <필드>`까지만.

## 검증

단위 테스트(클러스터·도커 불필요 — `tests/e2e/`는 제외):

    cd collectors/token-metrics
    python -m pytest -q tests/ --ignore=tests/e2e

매니페스트 계약(`schedule: 5 2-9 * * *`, `timeZone`, `startingDeadlineSeconds: 540`, `activeDeadlineSeconds: 3000`, `backoffLimit: 0`, `-verify` 이름, 렌더 결과에 기존 수집기 모듈 이름·`VM_PUSH_URL` 0건 — kubectl이 있으면 `kubectl kustomize` 렌더까지):

    python -m pytest -q tests/test_manifests.py

E2E(도커 — ClickHouse 24.8 `18125:8123` + mock provider `18001:8000`; 기존 수집기 e2e의 18123/18000과 충돌 없음). 정기 2회(2회차 `already_loaded`·`system.mutations` 0) → 검증 SQL 11종 → `--replace`(fact 뮤테이션 정확히 3·감사 1행) → 시나리오 3종(`hours_over_count` WARN / `metrics_empty_gpu` → `rows=9` / 409 2회 → `FAILURE reason=not_ready`) → manual-v0 1회(`rows_gpu=2 rows_serving=3 rows_engine=1`) → `E2E PASS`:

    docker build -t token-mock-provider:e2e tools/mock-provider
    ./collectors/token-metrics/tests/e2e/run_e2e.sh            # 날짜 기본 = KST 어제(TZ=Asia/Seoul 고정, 러너 로컬 TZ 무관); 인자로 YYYY-MM-DD 지정 가능

mock provider 시나리오(로컬 mock에 직접 — 플래그 6종 `metrics_gpu_hours_over`, `metrics_unknown_serving`, `metrics_pct_non_monotone`, `metrics_dup_gpu_rows`, `metrics_empty_gpu`, `metrics_engine_null`(0/1) + `not_ready_until_uptime_s`(초) + `retry_after_s`; 보존 기간 `MOCK_METRICS_RETENTION_DAYS`, 기본 14 — 그보다 오래된 날짜는 404):

    curl -X POST localhost:18001/__mock/scenario -H 'content-type: application/json' -d '{"metrics_gpu_hours_over": 1}'
    curl -X POST localhost:18001/__mock/reset

CI: `.github/workflows/test-collector-metrics.yml`(paths `collectors/token-metrics/**`, `tools/mock-provider/**` — jobs `unit`/`e2e`/`image`/`manifests`), `.github/workflows/release-images-metrics.yml`(`main` 푸시 → `ghcr.io/yoonsungnam/token-metrics-collector:<sha7>`·`:latest`). 기존 `test-collector.yml`·`release-images.yml`은 이 모듈을 보지 않는다.

## DDL·뮤테이션 장부 (§4.0)

    ddl/
    ├── README.md                       # 파일 표 · 뮤테이션 장부(설계 §4.0 표 그대로) · 확정된 결정 · 적용 순서
    ├── company/
    │   ├── raw_token_metrics.sql       # fact.raw_token_metrics_{gpu,serving,summary}_1d + fact.collect_audit_metrics_1d (_local/_dist ×4)
    │   ├── dim_token_metrics_service.sql   # gpu_data.dim_token_metrics_service (_local/_dist)
    │   └── accounts.sql                # GRANT TO mart (테이블 레벨; 감사 테이블은 SELECT·INSERT만) — admin 수동
    ├── stage/            (생성물 — tools/gen_stage_ddl.py; 직접 수정 금지)
    └── company-verify/   (생성물 — tools/gen_verify_ddl.py; token_verify_* DB·계정)

install.sh [5/7]가 `raw_token_metrics.sql`·`dim_token_metrics_service.sql`을 적용하고(`IF NOT EXISTS` — 재실행 안전), `accounts.sql`은 admin이 먼저 실행한다([4/7] 프리플라이트가 DB 존재만 확인). DB는 만들지 않는다(company `fact`·`gpu_data`는 기존 DB).

뮤테이션 장부(수집기 관련 행 — 전체 표·일 총량 상한은 `ddl/README.md`):

| 경로 | 뮤테이션 |
|---|---|
| 정기 시간별 실행(8슬롯) | **0** — 앵커 존재→스킵, 미존재→INSERT만; 레지스트리 동기화는 정기 실행에서만·diff-check |
| 레지스트리 변경(endpoints 편집·최초 배포) | 1(최초 배포는 현재 집합이 비면 DELETE 생략 → 0) |
| 크래시 잔여물 복구(부분 적재) | 서비스당 ≤3 — 정기 실행에서 뮤테이션이 생기는 유일한 경우 |
| 재수집 `--replace`·manual 재적재 | 날짜당 fact **≤3**(summary·gpu·serving; 감사는 append-only; 테이블별 `service IN (...)` 배칭) |
| 실행당 가드 | `METRICS_MAX_MUTATIONS_PER_RUN`(기본 **45** = 3×15) — 첫 DELETE 전 존재확인으로 합산, 초과 시 `FAILURE reason=mutation_budget`; 긴 범위는 `tools/rerun.py --chunk-days`(기본 7) |
| 피크(02:00~03:00) | 02:05 첫 슬롯은 INSERT만; 재수집은 **10:50 KST 이후**(rerun.py 실행 창) |

실측(최근 24시간 fact 뮤테이션 수 — 정기 슬롯만 돌았다면 0):

    SELECT count() FROM system.mutations WHERE database='fact' AND table LIKE 'raw_token_metrics_%' AND create_time > now() - INTERVAL 1 DAY

레지스트리·감사 쪽까지 보려면 `database IN ('fact','gpu_data')`, company-verify는 `database IN ('token_verify_fact','token_verify_dim')`. `is_done = 0`인 행이 남아 있으면 `mutations_sync=2`로 기다리던 실행이 중단된 것 — 다음 rerun 전에 `SELECT * FROM system.mutations WHERE is_done = 0`으로 확인한다.
