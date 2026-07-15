# stage 런북 (홈랩 실배포)

이 문서는 홈랩(stage) 실배포 세션의 **단일 정본**이다 — 모든 커맨드는 그대로 복붙
실행 가능해야 하고, 실물 인터페이스(install.sh 옵션·rerun.py 플래그·매니페스트
리소스명·DDL 경로)와 1:1이어야 한다. 설치→검증→(필요 시)철수 순서를 이 문서
하나로 따라갈 수 있다.

**범위 경계** (Plan 5 확정):
- §9-19(전-레플리카/복제 지연 검증)는 stage(1샤드×1레플리카)에서 재현 불가 — 이 런북의
  범위가 아니며, `docs/operations/company-verify.md` 1단계(2샤드×2레플리카 격리 검증)로
  이관된다.
- §9-20(VictoriaLogs 기반 BATCH_RESULT 마커 대시보드 패널)은 홈랩에 VictoriaLogs가 없어
  이 런북에서 검증할 수 없다 — company 단계에서 확보된다. 이 런북의 마커 확인은
  `kubectl logs` 직접 조회로 대체한다(§8 참조).

전제: kubectl 컨텍스트 `homelab`(0.(c) 참조), GitHub PAT(`read:packages` 스코프,
0.(b) 참조). 네임스페이스는 전 구간 `monitoring`, ClickHouse 네임스페이스는 `clickhouse`.

---

## 0. 사전 준비

### (a) release-images 워크플로 1회 실행 확인 (ghcr 3이미지 — latest 태그)

`.github/workflows/release-images.yml`은 main push(3모듈 경로) 또는 수동 트리거로
`ghcr.io/yoonsungnam/{token-mock-provider,token-usage-collector,token-mart}`에
`latest`+`<sha7>` 2태그를 push한다. 아직 한 번도 돌지 않았다면 수동 트리거한다:

```bash
gh workflow run release-images.yml -R YoonsungNam/token-data-pipeline
gh run list -R YoonsungNam/token-data-pipeline --workflow=release-images.yml --limit 5
```

`latest` 태그가 실제로 ghcr에 push됐는지 확인(0.(b)에서 준비할 PAT과 GitHub id 필요,
`read:packages`로 조회 가능):

```bash
GH_ID=<github-id>
for img in token-mock-provider token-usage-collector token-mart; do
  echo "== ${img} =="
  GH_TOKEN=<PAT> gh api "users/${GH_ID}/packages/container/${img}/versions" \
    --jq '.[0].metadata.container.tags'
done
```

3개 이미지 전부 출력에 `"latest"`가 포함돼야 한다 — 하나라도 비어 있으면 install.sh의
`--tag latest`가 ImagePullBackOff로 실패한다(install.sh 기본 태그는 로컬 git short SHA이며
ghcr에는 그 태그가 없다 — 그래서 `--tag latest`가 3·5단계에 필수다).

### (b) GitHub PAT 준비

`read:packages` 스코프의 Personal Access Token 1개를 미리 발급해 둔다 — 1단계
`registry-pull-secret` 생성과 위 (a)의 패키지 조회 양쪽에 사용한다.

### (c) kubectl 컨텍스트 정렬 (정본: rename)

이 머신의 현재 컨텍스트명은 `kubernetes-admin@kubernetes`이지만 collectors/mart
install.sh의 stage 기본값은 `homelab`이다. 매 커맨드에 실제 컨텍스트명을 명시하는
대신, 이 컨텍스트를 `homelab`으로 rename하는 것을 정본 절차로 채택한다(아래 전
커맨드가 이 이름을 전제):

```bash
kubectl config get-contexts
kubectl config rename-context kubernetes-admin@kubernetes homelab
kubectl config current-context   # homelab 확인
```

이미 `homelab`이라는 이름의 다른 컨텍스트가 존재하면 `rename-context`가 실패한다 —
그 경우 기존 컨텍스트명을 그대로 쓰고 아래 모든 `--context homelab`을 실제 이름으로
치환한다.

---

## 1. mock-provider 배포 (+ registry-pull-secret)

collectors install.sh보다 mock 배포가 먼저이므로, install.sh [1/6]이 만들어 줄
`registry-pull-secret`을 여기서 먼저 수동 생성한다:

```bash
kubectl --context homelab create secret docker-registry registry-pull-secret \
  --docker-server=ghcr.io \
  --docker-username=<github-id> \
  --docker-password=<PAT> \
  -n monitoring
```

(3단계·5단계에서 collectors/mart install.sh가 [1/6]·[1/5]에서 이 Secret이 이미
있음을 감지하고 "이미 존재합니다. 갱신하시겠습니까? [y/N]"을 물으면 `N`으로 스킵한다 —
이미 올바르게 생성돼 있으므로 재입력 불필요.)

mock-provider 2벌(Mock Service A/B) 배포:

```bash
kubectl --context homelab apply -n monitoring -f tools/mock-provider/k8s.yaml
```

4개 리소스(Service/Deployment × 2)가 생성된다. healthz 확인:

```bash
# readinessProbe(GET /healthz:8000)가 통과해야 Available
kubectl --context homelab -n monitoring wait --for=condition=available \
  deployment/token-mock-provider-a deployment/token-mock-provider-b --timeout=120s

# 애플리케이션 레벨 직접 확인 (포트포워딩 + curl, A/B 각각)
kubectl --context homelab -n monitoring port-forward svc/token-mock-provider-a 18000:8000 &
sleep 2 && curl -s http://127.0.0.1:18000/healthz; echo
kill %1

kubectl --context homelab -n monitoring port-forward svc/token-mock-provider-b 18001:8000 &
sleep 2 && curl -s http://127.0.0.1:18001/healthz; echo
kill %1
```

각각 `{"status":"ok"}`가 나오면 정상. (`tools/mock-provider/README.md`의 "## stage
배포" 절 — env 커스터마이즈 방법 포함 — 참조.)

---

## 2. admin DDL — 계정 공유 GRANT + fact DB 생성

계정 공유 결정(2026-07-14)으로 `mart` 계정은 **동료 소유의 기존 운영계정**이다 — 이
**stage 실사(2026-07-15)**: 홈랩 CHI에는 ZooKeeper/Keeper가 없어 `ON CLUSTER`·
`ReplicatedMergeTree`가 불가하다 — 그래서 이 런북의 DDL은 전부 `ddl/stage/`
(tools/gen_stage_ddl.py 생성 변형 — MergeTree·클러스터 절 없음)를 쓴다. 또한 이
클러스터에는 **`mart` 계정이 존재하지 않으므로 stage에서는 직접 생성**한다
(비밀번호는 stage 검증용으로 직접 결정 — company 반입 시에만 동료의 실제 비밀번호 사용):

    read -rsp 'stage mart 비밀번호: ' PW && kubectl --context homelab exec -i \
      -n clickhouse chi-gpu-monitoring-gpu-monitoring-0-0-0 -- \
      clickhouse-client -q "CREATE USER IF NOT EXISTS mart IDENTIFIED WITH sha256_password BY '$PW'" \
      && unset PW && echo " -> created"

런북의 어떤 `accounts.sql`도 `CREATE USER`를 하지 않으며(4개 파일 전부 확인됨,
`CHANGE_ME` 류 치환 대상 없음), GRANT만 수행한다. **company에서 `mart` 계정의 실제 비밀번호는
이 단계에서 다루지 않는다** — 3·5단계 install.sh의 `CH_PASSWORD` 프롬프트에 입력할
값이며, 동료(클러스터 소유자)에게 사전에 확인해 둔다(Secret 입력값과 반드시 동일해야
접속이 성공한다).

ClickHouse 파드 탐색(install.sh와 동일 로직) + 파일 적용 헬퍼:

```bash
CH_NS=clickhouse
CH_POD="$(kubectl --context homelab get pods -n "${CH_NS}" -o name \
  | sed 's#^pod/##' | grep '^chi-' | head -1)"
echo "CH_POD=${CH_POD}"   # 예상: chi-gpu-monitoring-gpu-monitoring-0-0-0

apply_sql() {
  local f="$1" base
  base="$(basename "$1")"
  kubectl --context homelab cp "$f" "${CH_NS}/${CH_POD}:/tmp/${base}"
  kubectl --context homelab exec -n "${CH_NS}" "${CH_POD}" -- \
    sh -c "clickhouse-client --multiquery < /tmp/${base}"
  kubectl --context homelab exec -n "${CH_NS}" "${CH_POD}" -- rm -f "/tmp/${base}"
}
```

4개 accounts.sql을 순서대로 적용(company-verify.md와 동일 순서 — collectors 선행,
assets는 mart보다 선행):

```bash
apply_sql collectors/token-usage/ddl/stage/accounts.sql
apply_sql assets/user-org/ddl/stage/accounts.sql
apply_sql assets/model-catalog/ddl/stage/accounts.sql
apply_sql mart/token-usage/ddl/stage/accounts.sql
```

- `collectors/.../accounts.sql`: `CREATE DATABASE IF NOT EXISTS fact ON CLUSTER
  'gpu-monitoring'` + `mart` 계정 앞 fact/gpu_data 테이블 레벨 GRANT.
- `assets/user-org`·`assets/model-catalog`: `gpu_data.dim_token_user_org_dist`/
  `dim_token_model_dist` SELECT GRANT(테이블은 4단계에서 생성되지만 ClickHouse는
  아직 없는 테이블에도 GRANT를 걸 수 있다 — 대상 테이블 생성 시 즉시 유효해진다).
- `mart/.../accounts.sql`: `mart.*`/`gpu_data.view_token_usage_*` GRANT.
- `gpu_data` DB 자체는 이 레포가 만들지 않는다(동료 소유) — 위 명령이
  `UNKNOWN_DATABASE` 류로 실패하면 클러스터 소유자와 먼저 협의한다.

---

## 3. collectors 설치

```bash
./collectors/token-usage/install.sh --tag latest stage
```

**`--tag latest` 필수** — 생략하면 로컬 git short SHA 태그를 쓰는데 ghcr에는 그
태그가 없어 ImagePullBackOff가 난다(0.(a) 참조). `build.sh`는 실행하지 않는다 —
로컬에 docker가 없고, CI(release-images)가 이미지 공급을 대체한다.

대화형 프롬프트 입력값:

| 단계 | 프롬프트 | 입력값 | 비고 |
|---|---|---|---|
| [1/6] | `registry-pull-secret` 갱신? `[y/N]` | `N` | 1단계에서 이미 생성 완료 |
| [2/6] | `CH_USER [mart]` | (enter → `mart`) | 계정 공유 결정 |
| [2/6] | `CH_PASSWORD` | *2단계에서 직접 정한 `mart` 비밀번호* | stage는 직접 생성한 계정 — company에서만 동료 제공 값 |
| [2/6] | `COLLECTOR_HTTPS_PROXY` | `none` | mock은 http, 홈랩 직접 연결 전제(사내 프록시 경유 환경이면 프록시 URL 입력) |
| [2/6] | 사내 CA 번들 파일 경로 | (enter, 빈값) | mock은 http라 CA 불요 |
| [2/6] | `CH_DB_FACT` | (enter, 빈값) | company-verify 전용 — stage는 기본값 `fact` |
| [2/6] | `CH_DB_DIM` | (enter, 빈값) | company-verify 전용 — stage는 기본값 `gpu_data` |

이후 [3/6] endpoints ConfigMap(`collectors/token-usage/endpoints.yaml` — mock
Service A/B baseUrl이 이미 `token-mock-provider-{a,b}.monitoring.svc:8000`으로
갱신돼 있음), [4/6] 테이블 DDL(`raw_token_usage.sql`+`dim_token_service.sql`, admin
DDL은 이미 2단계에서 끝났다는 안내 echo만 나옴), [5/6] CronJob apply(overlay
`stage`), [6/6] 이미지·`CH_HOST`·`VM_PUSH_URL`(vminsert 자동 탐색) 주입까지 자동
진행된다.

---

## 4. assets 시드 — dim 2종 + 시드 + 합성 로스터

assets 모듈은 install.sh를 갖지 않는다 — dim 테이블 DDL은 전부 admin(chi 파드)
수동 적용이다. 2단계에서 정의한 `apply_sql`/`CH_POD`를 그대로 재사용한다(같은 셸
세션이 아니라면 2단계 앞부분을 다시 실행).

```bash
apply_sql assets/user-org/ddl/stage/dim_token_user_org.sql
apply_sql assets/model-catalog/ddl/stage/dim_token_model.sql
apply_sql assets/model-catalog/ddl/stage/seed_dim_token_model.sql
```

`seed_dim_token_model.sql` 적용 후 콘솔에 말미 검증 SELECT 결과(`dup_key`,
`unknown_row_state`)가 출력된다 — **결과가 비어 있어야 정상**.

합성 로스터(`fixtures/synthetic_org_members.csv`, 실 로스터 아님 — mock user-####
체계 32행) → INSERT SQL 생성. **산출물은 레포 반입 금지 대상**이므로(`.gitignore`의
`dim_token_user_org_insert*.sql` 패턴 — 기본 `--out` 파일명이 정확히 이 패턴에
걸린다) `/tmp`에 생성한다:

```bash
python3 assets/user-org/csv_to_dim_user_org_insert.py \
  --csv assets/user-org/fixtures/synthetic_org_members.csv \
  --out /tmp/stage_org_insert.sql
```

생성된 SQL을 리뷰(합성 데이터라도 실 로스터 투입과 동일 관례 — 헤더 주석의 행수·
기본 effective_from 확인)한 뒤 적용:

```bash
cat /tmp/stage_org_insert.sql   # 리뷰
apply_sql /tmp/stage_org_insert.sql
```

콘솔에 말미 검증 SELECT(`dup_key`/`missing_key`/`key_conflict`)가 chunk별로 출력된다
— **전부 비어 있어야 정상**.

---

## 5. mart 설치

```bash
./mart/token-usage/install.sh --tag latest stage
```

**`--tag latest` 필수**(3단계와 동일 사유). 대화형 프롬프트 입력값:

| 단계 | 프롬프트 | 입력값 | 비고 |
|---|---|---|---|
| [1/5] | `registry-pull-secret` 갱신? `[y/N]` | `N` | 1단계에서 이미 생성 완료 |
| [2/5] | `CH_USER [mart]` | (enter → `mart`) | |
| [2/5] | `CH_PASSWORD` | *2단계에서 직접 정한 값* | 3단계와 동일 값 |
| [2/5] | `EXPECTED_LATE_SERVICES` | (enter, 빈값) | mock 2서비스는 지연 시나리오 없음(기본 설정) |
| [2/5] | `CH_DB_FACT` | (enter, 빈값) | company-verify 전용 |
| [2/5] | `CH_DB_DIM` | (enter, 빈값) | company-verify 전용 |
| [2/5] | `CH_DB_MART` | (enter, 빈값) | company-verify 전용 |

`INSERT_QUORUM` 프롬프트는 뜨지 않는다 — company/company-verify에서만 install.sh가
`auto`를 자동 주입한다(stage는 1샤드×1레플리카라 미설정, §9-19). 이후 [3/5] 테이블
DDL(`mart_tables.sql`+`view_token_usage.sql`, assets가 4단계에서 이미 준비돼 있어야
STEP 1 조인이 성공), [4/5] CronJob apply(overlay `stage`), [5/5] 이미지·`CH_HOST`
주입까지 자동 진행된다.

---

## 6. 수동 체인 실행

```bash
python3 collectors/token-usage/tools/rerun.py --context homelab --chain-mart
```

`--from/--to` 생략 = 1회 수동 트리거(실행 시점 기준 KST **어제** target_date). 완료
후 `--chain-mart`가 동일 날짜로 `mart/token-usage/tools/rerun.py`를 직접 트리거한다.
두 Job 모두 `wait_job()`이 파드 로그를 실시간 스트리밍하고, 완료 시 재조회 커맨드를
자동으로 출력한다(`[INFO] 전체 로그 재조회: kubectl --context=homelab logs job/<name>
-n monitoring --prefix --tail=-1`) — 그 줄을 그대로 복사해 재확인할 수 있다.

마커를 다시 조회하려면:

```bash
# 최근 생성된 Job 목록에서 이름 확인
kubectl --context homelab -n monitoring get jobs --sort-by=.metadata.creationTimestamp | tail -6

# collectors 잡 (이름 예: token-usage-collector-manual-<epoch>)
kubectl --context homelab -n monitoring logs job/<collectors-job-name> --prefix --tail=-1 \
  | grep -E "BATCH_RESULT|SERVICE_RESULT"

# mart 잡 (이름 예: token-mart-daily-rerun-<epoch> — --chain-mart는 --from/--to를 채워 호출하므로 -rerun- 접미)
kubectl --context homelab -n monitoring logs job/<mart-job-name> --prefix --tail=-1 \
  | grep "BATCH_RESULT"
```

---

## 7. 성공 기준 체크리스트

이후 SQL은 전부 2단계에서 정의한 `CH_POD`(chi 파드) 경유로 실행한다 — 새 셸
세션이면 먼저 2단계의 `CH_POD` 탐색 커맨드를 다시 실행한다. 편의 헬퍼:

```bash
ch() { kubectl --context homelab exec -n clickhouse "${CH_POD}" -- clickhouse-client --query "$1"; }
D=$(TZ=Asia/Seoul date -d yesterday +%F)   # 6단계 수동 트리거의 target_date와 동일
echo "D=${D}"
```

### 1) coverage 마커 정확

```bash
ch "SELECT count() FROM gpu_data.dim_token_service_dist WHERE enabled = 1"
```

판정: 이 값(활성 서비스 수 — 현재 Mock Service A/B 2개)이 6단계 mart 잡 로그의
`BATCH_RESULT ... coverage=N/M`의 M과 같아야 한다. mock은 기본 설정(시나리오 주입
없음)에서 항상 응답하므로 N도 M과 같아 `coverage=2/2`, `missing_services="-"`가
정상.

### 2) 3계층 합계 일치

```bash
ch "SELECT sum(input_tokens + cache_read_tokens + cache_creation_tokens) FROM fact.raw_token_usage_1d_dist WHERE date = '${D}'"
ch "SELECT sum(total_input_tokens) FROM mart.token_usage_1d_dist WHERE date = '${D}'"
ch "SELECT sum(total_input_tokens) FROM gpu_data.view_token_usage_1d_dist WHERE date = '${D}'"
```

판정: 세 값이 완전히 동일(raw = mart detail = view detail). 서비스 단위 대사도 확인:

```bash
ch "SELECT service, is_derived, diff_input_tokens, diff_cache_read_tokens, diff_cache_creation_tokens, diff_output_tokens, diff_requests FROM mart.agg_token_service_1d_dist WHERE date = '${D}'"
```

판정: mock의 summary는 detail과 별도 경로로 자체 계산되므로(§4.1) `diff_*` 전부 0이
정상(대사 불일치 없음).

### 3) 조직 귀속 (합성 로스터 기준)

```bash
ch "SELECT arraySlice(org_path,1,1) AS org1, sum(total_input_tokens) AS tokens, uniqExact(user_id) AS users FROM mart.token_usage_1d_dist WHERE date = '${D}' GROUP BY org1 ORDER BY org1"
ch "SELECT org_path, distinct_users, headcount, adoption_rate FROM mart.agg_token_org_1d_dist WHERE date = '${D}' ORDER BY org_path"
```

판정: 로스터 상위 조직(A~F부문) 버킷 + `['unknown']` 버킷이 함께 나타나야 한다 —
`MOCK_USERS=50`(A/B 각각)인데 합성 로스터는 identified 사용자를 `user-0000`~`user-0029`
대까지만 커버하므로(fixtures 참조), 그 밖의 identified 사용자와 로스터 미등록
`anon-0001`~`anon-0009`가 `unknown` 버킷으로 잡히는 것이 정상. 2번째 쿼리의
`headcount`는 dim_token_user_org의 date 기준 유효 로스터 인원과 일치해야 하고,
`adoption_rate = distinct_users/headcount`(headcount=0인 버킷은 NULL)이어야 한다.

### 4) cost 계산

```bash
ch "SELECT model, count(), countIf(cost IS NULL) AS null_cost, countIf(cost IS NOT NULL) AS priced FROM mart.token_usage_1d_dist WHERE date = '${D}' GROUP BY model ORDER BY model"
```

판정: mock 기본 `MOCK_MODELS`(claude-opus-4-8/claude-sonnet-5/claude-haiku-4-5)
3종 전부 `dim_token_model` 시드에 등록돼 있으므로 `null_cost=0`(전부 cost NOT NULL)이
정상 — 등록 모델에 $0 위장 없이 실제 단가가 곱해졌는지의 최소 판정 기준이다. 만약
시나리오 주입 등으로 미등록 모델이 섞이면 그 모델 행만 `cost IS NULL`이어야 한다.

### 5) anon 핸들명 view 노출

```bash
ch "SELECT user_id, user_name, count() FROM gpu_data.view_token_usage_1d_dist WHERE date = '${D}' AND user_type = 'anonymous' GROUP BY user_id, user_name ORDER BY user_id"
ch "SELECT countIf(user_type != 'anonymous' AND user_name != '') AS leak FROM gpu_data.view_token_usage_1d_dist WHERE date = '${D}'"
```

판정: 합성 로스터에 등록된 `anon-0000`만 `user_name='합성-9000'`으로 노출되고,
로스터 미등록인 `anon-0001`~`anon-0009`는 `user_name=''`이어야 한다. 2번째 쿼리
(`leak`)는 identified/unclassified 유형에 user_name이 새는 사례가 없어야 하므로
0이 정상(§9-1 보류 원칙 — anonymous만 dim 핸들명 노출).

### 6) 멱등 2-run 행수 보존 (insert_deduplicate — stage 1레플리카 ZK dedup 실검증)

```bash
BEFORE_FACT=$(ch "SELECT count() FROM fact.raw_token_usage_1d_dist WHERE date = '${D}'")
BEFORE_MART=$(ch "SELECT count() FROM mart.token_usage_1d_dist WHERE date = '${D}'")

python3 collectors/token-usage/tools/rerun.py --context homelab --from "${D}" --to "${D}" --chain-mart

AFTER_FACT=$(ch "SELECT count() FROM fact.raw_token_usage_1d_dist WHERE date = '${D}'")
AFTER_MART=$(ch "SELECT count() FROM mart.token_usage_1d_dist WHERE date = '${D}'")
echo "fact: before=${BEFORE_FACT} after=${AFTER_FACT}"
echo "mart: before=${BEFORE_MART} after=${AFTER_MART}"
```

판정: `BEFORE_FACT == AFTER_FACT` 이고 `BEFORE_MART == AFTER_MART`. delete-then-insert
+ 클라이언트 `insert_deduplicate=0`으로 같은 (date, service)를 재적재해도 행 증식
없이 정확히 교체돼야 한다. stage는 1샤드×1레플리카라 ZK 복제 지연 게이트
(`INSERT_QUORUM`, company 전용)가 관여하지 않는 순수 클라이언트 설정 검증 환경이다.

### 7) rerun --chain-mart exit 전파

```bash
python3 collectors/token-usage/tools/rerun.py --context homelab --chain-mart
echo "exit=$?"
```

판정: 정상 시 `exit=0`. collectors Job 실패 시 `--chain-mart` 실행 없이 즉시 1,
mart 단계 실패 시 mart rerun의 리턴값이 그대로 전파돼 비0이어야 한다(rerun.py의
`subprocess.call` 리턴값 그대로 반환).

### 8) 대시보드 임포트 확인 + Grafana 패널 표시 (mart 계정)

`docs/monitoring/README.md` 절차를 그대로 따른다:
1. §1 — `grafana-clickhouse-datasource` 플러그인 설치 확인(미설치 시 안내된
   `grafana-cli plugins install grafana-clickhouse-datasource` 또는 Administration UI 경로).
2. §2 — ClickHouse 데이터소스 등록(계정 `mart`, 비밀번호는 2단계에서 직접 정한 값(3·5단계와 동일)).
3. §3 — `docs/monitoring/grafana_dashboard_token_usage.json` 임포트, `DS_CLICKHOUSE`
   입력 필드에 방금 만든 데이터소스 매핑(이 프롬프트가 안 뜨면 `__inputs` 선언
   문제이므로 중단하고 JSON을 재확인).

판정: 조회 에러가 있는 패널이 없어야 하며, 패널 1(서비스별 추이)·2(org 롤업)·3(모델별)·
5(unknown 버킷)·6(anon 핸들명 상위)·7(커버리지)에 방금 적재한 `${D}` 데이터가 보인다
(기본 시간범위 `now-30d`~`now`에 포함되므로 별도 조정 불필요 — 안 보이면 시간범위를
넓혀 재확인). **예외 — 패널 4(대사 품질)는 위반 행만 표시하는 패널이라 정상 배포에서는
"No data"가 곧 정상이다** (mock 데이터는 diff_* 전부 0 — §7-2 판정과 동일 근거).
패널 4에 행이 보인다면 그것이 오히려 대사 불일치 신호다.

---

## 8. 정례화 확인

CronJob이 suspend 상태가 아니고(02:00/04:00 KST 자동 스케줄) 있는지 확인:

```bash
kubectl --context homelab -n monitoring get cronjob token-usage-collector token-mart-daily \
  -o custom-columns=NAME:.metadata.name,SCHEDULE:.spec.schedule,SUSPEND:.spec.suspend,LAST_SCHEDULE:.status.lastScheduleTime
```

판정: `SUSPEND` 열이 `<none>`(base 매니페스트에 `suspend` 필드 자체가 없음 = 활성) —
`token-usage-collector`는 `0 2 * * *`, `token-mart-daily`는 `0 4 * * *`(둘 다
Asia/Seoul).

다음날, 자동 스케줄이 실행된 뒤 마커를 확인한다:

```bash
kubectl --context homelab -n monitoring get jobs --sort-by=.metadata.creationTimestamp \
  | grep -E "^token-usage-collector-|^token-mart-daily-" | tail -4
# 위에서 확인한 최신 Job 이름으로 치환
kubectl --context homelab -n monitoring logs job/<최신-collectors-job> --prefix --tail=-1 | grep BATCH_RESULT
kubectl --context homelab -n monitoring logs job/<최신-mart-job> --prefix --tail=-1 | grep BATCH_RESULT
```

(CronJob이 만든 Job은 수동 트리거와 이름 패턴이 다르다 — k8s가 자동 생성한 해시
접미 이름을 위 `get jobs` 목록에서 그대로 복사해 쓴다.)

---

## 9. 철수 (필요 시)

CronJob·Secret·mock을 제거한다. **DB(fact/gpu_data/mart의 테이블·데이터)는 유지한다**
— stage 데이터를 삭제할 의무가 없고, `tools/data-admin/delete_data.py`는 개인정보
파기·오적재 회수 전용 도구(§8.3 ②)라 이 철수 목적에는 사용하지 않는다.

```bash
kubectl --context homelab -n monitoring delete cronjob token-usage-collector token-mart-daily
kubectl --context homelab -n monitoring delete secret token-usage-ch-secret token-mart-ch-secret registry-pull-secret
kubectl --context homelab -n monitoring delete configmap token-usage-endpoints
kubectl --context homelab -n monitoring delete -f tools/mock-provider/k8s.yaml
```

재배포 시 install.sh의 테이블 DDL은 `CREATE TABLE IF NOT EXISTS`이므로 기존 테이블에
안전하게 재적용된다 — 데이터 유실 없이 CronJob/Secret/mock만 다시 만들면 된다.

---

## 부록 — 이 런북이 다루지 않는 것

- **§9-19 전-레플리카 검증**(다중 샤드/레플리카 뮤테이션 대기, clusterAllReplicas
  폴링, INSERT_QUORUM 게이트 실검증)은 stage(1s×1r)에서 재현 불가 —
  `docs/operations/company-verify.md` 1단계로 이관.
- **§9-20 VictoriaLogs 마커 대시보드 패널**(BATCH_RESULT/SERVICE_RESULT를 Grafana에서
  LogsQL로 조회)은 홈랩에 VictoriaLogs가 없어 이 런북에서 구성하지 않는다 — company
  단계에서 확보된다. 이 런북 6·8단계의 마커 확인은 `kubectl logs` 직접 조회로
  대체한다.
- **개인정보 파기(user_id 축 삭제)**는 이 런북의 범위가 아니다 —
  `docs/operations/rerun.md`의 "파기 요청 처리" 절 참조.
