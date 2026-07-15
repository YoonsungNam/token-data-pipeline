# company 2단계 검증 전략 — 1단계 격리 검증 (company-verify)

company 반입은 2단계로 진행한다: **1단계(격리)**는 동일 물리 클러스터(`gpu-monitoring`,
company 2샤드×2레플리카) + 동일 실 서비스 API를 대상으로 하되, 격리 DB 3종
(`token_verify_fact`/`token_verify_dim`/`token_verify_mart`, 기본안)과 전용 계정
(`token_verify`)만 써서 production DB(`fact`/`gpu_data`/`mart`)를 전혀 건드리지 않고
파이프라인 전체(collectors→mart→view)를 실데이터로 검증한다. **2단계(정규)**는 1단계
통과 후 production DB로 전환하는 카나리아 절차다.

이름·계정은 CLI로 변경 가능하지만(`tools/gen_verify_ddl.py --fact/--dim/--mart/--account`),
이 문서는 기본안 이름을 기준으로 서술한다.

## 목적

- stage(홈랩, 1샤드×1레플리카, mock-provider 합성 데이터)는 검증할 수 없는 것들을 확인한다:
  company 토폴로지(2s×2r — mutations_sync=2 다중 레플리카 대기·clusterAllReplicas 폴링·복제
  lag count 재시도, §9-19)와 실 서비스 API 응답(스펙 §5.0/§5.9 계약 위반 여부, 실제
  service_group/service 명명 정합, 실 볼륨에서의 페이지네이션·재시도 동작).
- production DB(`fact`/`gpu_data`/`mart`)에 검증용 데이터가 섞이는 사고를 원천 차단한다 —
  잘못된 rerun·버그가 있는 collectors/mart 이미지를 실데이터로 시험할 때 실제 대시보드·
  차지백 데이터를 오염시키지 않는다.
- VictoriaLogs/BATCH_RESULT 등 stage에서 검증 불가능했던 항목(§7.2 환경 전제)의 검증
  경로를 company 반입 이전에 확보한다.

## 0. 사내 반입 (레포 · 이미지 · 컨텍스트)

사내망은 GitHub private repo·ghcr.io에 접근할 수 없으므로, 검증 작업 머신에 **소스와
이미지를 먼저 반입**한다. 아래 `<...>`는 사내에서 실값으로 치환한다 — **이 문서에 사내
호스트명·주소를 커밋하지 않는다**(레포가 반입 중 공개 상태가 될 수 있으므로 — 아래 (a)).

### (a) 레포 반입 — 임시 공개 전환

private→**임시 public**→다운로드→**즉시 private 복귀**. 공개 창(window) 동안 **전체 git
히스토리가 외부에 노출**되므로, 전환 전 아래를 확인한다(이미 통과 상태 — 2026-07-15 스캔):

- 실 비밀번호·토큰: accounts.sql은 `CHANGE_ME_*` 플레이스홀더만, 커밋된 실 비밀번호 0
- 실데이터: `endpoints.company.yaml`·실로스터 CSV·`*_insert*.sql`·`.env`는 .gitignore로 **커밋 이력 0**, fixture는 합성(`합성-NNNN`)만
- 사내 인프라 식별자: harbor 호스트·프로젝트명은 **미커밋**(build.sh가 런타임 파생) — 이 문서도 플레이스홀더 유지로 노출 안 함

```bash
# 전환 전 재확인 (0 이어야 안전)
git log --all --diff-filter=A --name-only --pretty=format: \
  | grep -iE "roster.*\.csv$|_insert.*\.sql$|endpoints\.company\.yaml$|\.env$" | sort -u

# GitHub: Settings → Danger Zone → Change visibility → Public (사내 다운로드 직후 즉시 Private 복귀)
# 사내 머신에서:
git clone https://github.com/YoonsungNam/token-data-pipeline.git
# 복귀 확인 후 작업 — 공개 창은 최소화 (다운로드 완료 즉시 Private)
```

> 대안(공개 노출 0): mini 머신에서 `git bundle create token-pipeline.bundle --all` 후 번들
> 파일만 승인된 채널로 반입 → 사내에서 `git clone token-pipeline.bundle`. 공개 전환이
> 부담되면 이 방식을 권장.

### (b) 이미지 반입 — 사내 Harbor 빌드·푸시

company는 **실 서비스 API**를 대상으로 하므로 mock-provider 이미지는 불요 — collectors·mart
2개만 사내 Harbor에 올린다(build.sh company 경로가 Harbor proxy 베이스 이미지·`linux/amd64`를
처리):

```bash
# <harbor> = 사내 레지스트리 호스트, <project> = Harbor 프로젝트  (실값은 사내에서만)
./collectors/token-usage/build.sh --registry <harbor>/<project> company
./mart/token-usage/build.sh        --registry <harbor>/<project> company
# → <harbor>/<project>/token-usage-collector:<sha>, <harbor>/<project>/token-mart:<sha> 푸시
```

### (c) kubectl 컨텍스트

사내 클러스터 컨텍스트를 확인·지정한다(`--context <company-context>`). 이 문서의 이후
커맨드는 `<company-context>`·`-n <ns>`를 사내 실값으로 치환해 사용한다(네임스페이스는
§9-3 미결 — 착수 체크리스트 참조).

## 격리 성립 vs 공유 잔여 (리스크 표)

| 구분 | 항목 | 상태 |
|---|---|---|
| **격리 성립** | 데이터 | production `fact`/`gpu_data`/`mart`와 완전 분리 — `token_verify_*` 3종 격리 DB만 사용, 오염 없음 |
| **격리 성립** | ZK 복제 경로 | `/clickhouse/tables/{shard}/token_verify_*/...` — 기존 Replicated 테이블 경로와 겹치지 않음. **누락 시 기존 Replicated 테이블과 복제 상태가 꼬이는 최악 케이스**이므로 `tools/gen_verify_ddl.py`가 생성물 전체에 원 DB명 구조 토큰 잔존 0을 자체 단정한다 |
| **격리 성립** | 검증 대표성 | 동일 물리 클러스터(`gpu-monitoring`, company 2s×2r)·동일 스키마·동일 실 서비스 API 대상 — stage(1s×1r, mock)보다 신뢰도 높음(§9-19 해소 경로) |
| **공유 잔여** | 뮤테이션 예산 | 클러스터 자체는 공유 — 토큰 파이프라인 기여분이 평시 ~68건/일(README 실측)에서 **~136건/일(2배)**로 증가, 확정 예산(일 150건/피크 창 80건, §4.0(c))에 근접·잠식 — **1단계 착수 전 클러스터 소유자에게 사전 통지 필수** |
| **공유 잔여** | VM push | **1단계 비활성** — `install.sh company-verify`가 VM_PUSH_URL 주입을 자동 스킵(명시 echo). VictoriaMetrics 게이지 대시보드는 1단계 검증 대상이 아니다 |
| **공유 잔여** | VictoriaLogs 마커 | BATCH_RESULT/SERVICE_RESULT는 기존 대시보드와 동일 module 레이블(`token-usage`/`mart-token`)을 공유 — **`-verify` 파드명으로만 구분**된다. 파드명 필터 없이 module 단위로 집계하는 패널은 1단계 실행분이 이중으로 잡힌 것처럼 보일 수 있음(LogsQL 쿼리에 `pod=~".*-verify-.*"` 제외/포함 필터 권장) |
| **공유 잔여** | 실 서비스 API | **이중 수집 금지 — 병행 금지, 교체 전환.** company-verify CronJob과 2단계 정규(company) CronJob은 **동시에 활성화하지 않는다** — 같은 실 서비스 API를 두 파이프라인이 동시에 폴링하면 provider 측 rate-limit 예산을 이중 소모하고, 이상 수집 패턴으로 오탐될 수 있다. 검증이 끝나면 1단계를 suspend하고 정규 파이프라인으로 **교체**한다(아래 "2단계 전환" 절차) |
| **공유 잔여** | 디스크 | 격리 DB 3종도 동일 클러스터의 물리 디스크를 사용한다 — 검증 기간(서비스 수 × 검증 일수)의 예상 데이터량을 클러스터 소유자에게 사전 통지 |

## 착수 전 소유자·운영자 확정 체크리스트 (blocking)

1단계는 아래가 전부 확보돼야 착수한다 — 코드는 준비 완료 상태이고, 남은 것은 소유자
승인·운영 산출물이다:

- [ ] **뮤테이션 예산 (소유자)** — 2배 증가(~136건/일)가 확정 예산(150/일)에 근접하므로,
  근원 예산 미확정(이슈 #1 사후 컨펌 진행 중)과 함께 **소유자 통지 + 무이의** 확보. §0 사전 통지.
- [ ] **격리 DB·계정 생성 승인 (소유자/DBA)** — 공유 클러스터에 `CREATE DATABASE token_verify_*` ×3
  + `CREATE USER token_verify`(스펙 §7.2의 "레포는 CREATE USER 안 함" 원칙의 **명시적 예외** —
  격리 검증 전용, 소유자 승인 필수) + 테이블 레벨 GRANT를 admin이 수동 실행. `CHANGE_ME_VERIFY`
  비밀번호를 실행 전 치환하고, **동일 값**을 install.sh `[2/6] CH_PASSWORD` 프롬프트에 입력.
- [ ] **endpoints.company.yaml (운영자)** — 실 서비스 URL 목록(gitignored). collectors install [3/6]가 필요.
- [ ] **이미지 (company harbor)** — token-usage-collector·token-mart를 사내 harbor에 push,
  install.sh `--registry <harbor>`로 배포.
- [ ] **네임스페이스** — 이 문서·install.sh는 `monitoring`을 기본 가정(§9-3 미결). company k8s ns가
  다르면 모든 커맨드의 `-n monitoring`·install.sh `--namespace`를 실제 ns로 교체.

> insert_deduplicate 서버측 여부(이슈 #1)는 1단계에 무관하다 — 전용 `token_verify` 계정이라
> "공유 계정 전역 영향" 문제가 성립하지 않고, 멱등성은 클라이언트 `insert_deduplicate=0`으로 충족.
> 이 협의는 2단계(공유 mart 계정) 전환에서만 유효.

## 1단계 설치 절차

### 0) 사전 통지

클러스터 소유자에게 뮤테이션 예산 2배 증가(~136건/일)와 디스크 사용 증가를 사전 통지한다
(위 리스크 표 참조). 이 저장소는 예산 초과 여부를 자동 감시하지 않는다.

### 1) DDL 생성물 확인 (커밋된 상태 — 보통 재생성 불필요)

`ddl/company-verify/*.sql`은 `tools/gen_verify_ddl.py`가 `ddl/company/*.sql`로부터 생성해
커밋해 둔 결과물이다. 원본 DDL이 바뀐 뒤 미갱신됐는지는 CI(`verify-ddl` 잡)가 상시 확인하지만,
로컬에서 직접 확인하려면:

```bash
python3 tools/gen_verify_ddl.py --check
```

기본안과 다른 이름으로 격리하려면(드문 경우) override 후 재생성·커밋한다:

```bash
python3 tools/gen_verify_ddl.py --fact my_verify_fact --dim my_verify_dim \
    --mart my_verify_mart --account my_verify_user
```

### 2) admin: DB 3종 + 계정 생성 + GRANT (accounts류)

**company에서는 클러스터 소유자 협의 후** 수동 실행. 각 모듈의 `ddl/company-verify/accounts.sql`
머리에 이미 다음이 포함돼 있다(생성기가 프리펜드) — `CHANGE_ME_VERIFY` 비밀번호를 실행 전
실제 값으로 치환할 것:

```sql
CREATE DATABASE IF NOT EXISTS token_verify_fact ON CLUSTER 'gpu-monitoring';
CREATE DATABASE IF NOT EXISTS token_verify_dim ON CLUSTER 'gpu-monitoring';
CREATE DATABASE IF NOT EXISTS token_verify_mart ON CLUSTER 'gpu-monitoring';
CREATE USER IF NOT EXISTS token_verify ON CLUSTER 'gpu-monitoring'
    IDENTIFIED WITH sha256_password BY 'CHANGE_ME_VERIFY';
```

적용 순서(§7.2 DDL 실행 주체 분리와 동일 원칙 — collectors 선행, assets는 mart보다 선행):

```bash
# admin 세션에서, CHANGE_ME_VERIFY 치환 후
clickhouse-client --multiquery < collectors/token-usage/ddl/company-verify/accounts.sql
clickhouse-client --multiquery < assets/user-org/ddl/company-verify/accounts.sql
clickhouse-client --multiquery < assets/model-catalog/ddl/company-verify/accounts.sql
clickhouse-client --multiquery < mart/token-usage/ddl/company-verify/accounts.sql
```

### 3) admin: assets 테이블 DDL + 시드 (assets는 install.sh를 갖지 않음 — §6.1/§6.2 그대로 상시 admin 수동)

```bash
clickhouse-client --multiquery < assets/user-org/ddl/company-verify/dim_token_user_org.sql
clickhouse-client --multiquery < assets/model-catalog/ddl/company-verify/dim_token_model.sql
clickhouse-client --multiquery < assets/model-catalog/ddl/company-verify/seed_dim_token_model.sql
```

로스터 투입(§6.1)은 `csv_to_dim_user_org_insert.py`가 생성한 INSERT SQL을 사내 리뷰 후
`token_verify_dim.dim_token_user_org_dist`에 실행 — 정규 절차와 동일, 대상 DB만 격리명.

### 4) install.sh company-verify (collectors/mart 테이블 DDL + CronJob 배포)

```bash
./collectors/token-usage/install.sh --context <company-context> --registry <harbor> company-verify
./mart/token-usage/install.sh --context <company-context> --registry <harbor> company-verify
```

- `--context`/`--registry`는 company와 동일하게 필수.
- Secret/ConfigMap/CronJob 이름은 전부 `-verify` 접미(`token-usage-ch-secret-verify`,
  `token-usage-endpoints-verify`, `token-usage-collector-verify`, `token-mart-ch-secret-verify`,
  `token-mart-daily-verify`).
- Secret에 `CH_DB_FACT`/`CH_DB_DIM`(collectors)·`CH_DB_FACT`/`CH_DB_DIM`/`CH_DB_MART`(mart)와
  `CH_USER=token_verify`가 **자동 포함**된다(프롬프트 없음 — 기본안 값 그대로).
- DDL 적용 대상은 `ddl/company-verify/`(테이블 DDL만 — accounts는 위 2)단계에서 admin이
  이미 처리했어야 한다).
- `VM_PUSH_URL` 주입은 자동 스킵(명시 echo) — 1단계 VM 오염 방지.
- k8s overlay `company-verify`(`nameSuffix: -verify` + Secret/ConfigMap 이름 패치)가 적용된다.

### 5) endpoints (collectors)

`endpoints.company.yaml`(gitignored, 사내 실 서비스 URL 목록)을 그대로 사용한다 —
1단계도 실 서비스 API를 대상으로 한다(합성 데이터가 아님). `--endpoints`로 다른 파일을
지정하지 않는 한 `install.sh`가 이 파일을 기본값으로 찾는다.

## 성공 기준 체크리스트 (E2E 검증 항목의 실데이터판)

stage E2E(`mart/token-usage/tests/e2e/run_e2e.sh` 등)가 합성 fixture의 **알려진 기대값**과
비교하는 것과 달리, 1단계는 실데이터라 절대값을 미리 알 수 없다 — 대신 **내부 정합성
불변식**을 검증한다. 아래 전부가 성립해야 2단계로 넘어간다.

1. **멱등성(2-run 행수 보존)** — 같은 날짜에 collectors→mart rerun을 2회 연속 실행한 뒤
   `token_verify_fact.raw_token_usage_1d_dist`/`token_verify_mart.token_usage_1d_dist`/
   `token_verify_dim.view_token_usage_1d_dist`의 `count(*)`가 1회차와 2회차에 동일해야 한다
   (delete-then-insert + 클라이언트 `insert_deduplicate=0`이 재삽입 시 행 증식 없이 정확히
   교체됐는지 — §7.1).
2. **coverage 게이트** — mart `BATCH_RESULT`의 `coverage=N/M`이 `dim_token_service`의
   enabled 집합과 일치하고, `missing_services`가 비어 있거나 §5.9 계약 9조의 expected-late
   목록에 등록된 서비스만 남아야 한다.
3. **3계층 합계 일치** — `token_verify_fact.raw_token_usage_1d_dist`(raw 합계) ==
   `token_verify_mart.token_usage_1d_dist`(mart detail 합계) ==
   `token_verify_dim.view_token_usage_1d_dist`(view 합계), 서비스 단위로는
   `token_verify_mart.agg_token_service_1d_dist`의 `diff_*` 컬럼이 `is_derived=0`인 서비스에서
   전부 0이어야 한다(§4.1 대사 불일치 없음).
4. **조직 귀속** — `token_verify_mart.agg_token_org_1d_dist`의 `distinct_users`/`headcount`가
   `token_verify_dim.dim_token_user_org_dist`의 date 기준 유효 로스터와 정합하고, 미매핑
   버킷(`org_path=['unknown']`)의 존재 여부가 실제 미매핑 사용자 유무와 일치해야 한다.
5. **cost** — `token_verify_dim.dim_token_model_dist`에 등록된 모델은 `cost IS NOT NULL`,
   미등록/unknown 모델은 `cost IS NULL`로 전파돼야 한다($0 위장 없음, §4.2 리뷰 #15).
6. **마커** — collectors/mart 양쪽의 `BATCH_RESULT status=SUCCESS`가 `-verify` 파드에서
   발화하고, VictoriaLogs에서 해당 파드명으로 조회 가능해야 한다(SERVICE_RESULT 포함).

위 3~5의 SQL 불변식은 **`tools/verify/invariants.sql` + `tools/verify/run_invariants.py`**로
실행형으로 제공된다(고정 기대값 대신 위반 행 노출 — 빈 출력이면 통과). 1단계는 격리 DB로 실행:

```bash
CH_DB_FACT=token_verify_fact CH_DB_DIM=token_verify_dim CH_DB_MART=token_verify_mart \
CH_HOST=<chi-host> CH_USER=token_verify CH_PASSWORD=<...> \
    python3 tools/verify/run_invariants.py --date <D>
```

1(멱등)·2(coverage)·6(마커)는 SQL 불변식이 아니라 실행 행위·마커 확인이므로 위 커맨드와
별개로 수동 확인한다(1은 rerun 2회 후 count 비교, 2·6은 BATCH_RESULT 로그).

## 2단계 전환 (카나리아)

1단계 체크리스트가 전부 통과하면:

1. **1단계 CronJob suspend** — 실 서비스 API 이중 수집을 막기 위해 정규 파이프라인을
   켜기 **전에** 먼저 1단계를 멈춘다:

   ```bash
   kubectl --context <company-context> patch cronjob token-usage-collector-verify \
       -n monitoring -p '{"spec":{"suspend":true}}'
   kubectl --context <company-context> patch cronjob token-mart-daily-verify \
       -n monitoring -p '{"spec":{"suspend":true}}'
   ```

2. **카나리아: mart(공유 계정)로 1일치 rerun** — 정규 DDL(`ddl/company/`)·GRANT가 이미
   적용돼 있다는 전제로(§7.2), production DB를 대상으로 단 하루만 수동 실행한다:

   ```bash
   python3 collectors/token-usage/tools/rerun.py --context <company-context> \
       --from <D> --to <D> --chain-mart
   ```

3. **검증 SQL** — 동일 불변식을 이번엔 production DB명(기본값)으로 재확인:

   ```bash
   CH_HOST=<chi-host> CH_USER=mart CH_PASSWORD=<...> \
       python3 tools/verify/run_invariants.py --date <D>
   ```
   (CH_DB_* override 없이 기본 `fact`/`gpu_data`/`mart` 사용.)

4. **정상 확인 후 정규 CronJob 기동** — `install.sh company`가 아직 적용되지 않았다면
   지금 적용한다(overlay `company`, DDL 대상 `ddl/company/`). 이미 배포돼 있었다면 별도
   조치 불요 — 정규 스케줄(collectors `0 2 * * *`, mart `0 4 * * *`, 둘 다 Asia/Seoul)이
   다음 실행부터 그대로 정상 가동된다.

## 철수 (1단계 종료 — 검증 실패 또는 2단계 전환 완료 후 정리)

```bash
# CronJob 삭제
kubectl --context <company-context> delete cronjob token-usage-collector-verify -n monitoring
kubectl --context <company-context> delete cronjob token-mart-daily-verify -n monitoring

# (선택) Secret/ConfigMap 정리
kubectl --context <company-context> delete secret token-usage-ch-secret-verify -n monitoring
kubectl --context <company-context> delete configmap token-usage-endpoints-verify -n monitoring
kubectl --context <company-context> delete secret token-mart-ch-secret-verify -n monitoring
```

```sql
-- admin 수동 (클러스터 소유자 협의 후)
DROP DATABASE IF EXISTS token_verify_fact ON CLUSTER 'gpu-monitoring';
DROP DATABASE IF EXISTS token_verify_dim ON CLUSTER 'gpu-monitoring';
DROP DATABASE IF EXISTS token_verify_mart ON CLUSTER 'gpu-monitoring';
DROP USER IF EXISTS token_verify ON CLUSTER 'gpu-monitoring';
```

DROP DATABASE는 해당 DB의 모든 테이블(격리 데이터 전부)을 함께 삭제한다 — 되돌릴 수 없다.
2단계 전환 완료 후라면 격리 데이터는 이미 검증 목적을 다했으므로 보존할 필요가 없다(검증
실패로 재작업이 필요한 경우가 아니라면 즉시 철수해도 무방).
