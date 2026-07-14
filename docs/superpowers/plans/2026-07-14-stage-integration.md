# stage 통합 (Plan 5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 홈랩(stage) 실배포에 필요한 마지막 산출물 — 이미지 릴리스 워크플로, mock 배포 매니페스트, delete_data.py, Grafana 테스터 대시보드, 스테이지 런북. (실배포 자체는 런북 따라 별도 세션 — 사용자 참여.)

**Architecture:** 코드는 전부 기존 관례 연장. 이미지는 GitHub Actions→ghcr.io(홈랩이 pull), 대시보드는 CH 데이터소스(계정 공유 결정 — mart 계정), 런북이 설치→검증→철수의 단일 정본.

**Tech Stack:** GitHub Actions(ghcr push), Python 3.10+(delete_data — clickhouse-connect), Grafana 대시보드 JSON, bash.

## Global Constraints

- **Python 3.10 호환**, **로컬 docker 없음**, **홈랩 실사 결과 반영**: VictoriaMetrics는 클러스터판 — vminsert 서비스명이 `victoriametrics-victoria-metrics-cluster-vminsert`(:8480)라 **접두 매칭(`^vminsert`)은 실패** — 포함 매칭으로 보정. Grafana는 monitoring ns(:80). VictoriaLogs 없음(§9-20 — 마커 패널은 company 단계).
- **이미지 공급 경로**: 홈랩은 `ghcr.io/yoonsungnam/*`에서 pull — 릴리스 워크플로가 main push 시 3이미지(token-mock-provider/token-usage-collector/token-mart)를 `latest`+`<git short SHA>` 태그로 push. private 패키지이므로 클러스터에는 `registry-pull-secret`(install.sh가 대화형 생성 — GitHub PAT read:packages 필요, **런북에 명시**).
- **계정 공유 결정 반영**: 대시보드·검증 쿼리는 `mart` 계정 사용.
- **delete_data 계약(§8.3 ②)**: 모드 A(날짜범위[+service]) = fact 상세·summary 2테이블 삭제(**collect_audit는 append-only 감사 — 삭제 금지**) 후 "동일 기간 mart rerun 의무" 안내 출력; 모드 B(user_id 축, 파기·퇴사자) = fact 상세 + mart 상세 + view 상세 3테이블 ALTER DELETE. 공통: **dry-run이 기본**(대상 행수 카운트만 출력), `--yes`로 실행, ON CLUSTER + wait_for_mutations, DB명은 CH_DB_* env 반영, 로그에 user_id 원문 노출 최소화(카운트 중심 — 인자로 받은 값 자체는 불가피).
- **런북 = 정본**: 설치 순서·성공 기준·철수를 실행 가능한 커맨드로. §9-19(전-레플리카 항목은 company-verify 1단계로 이관)·§9-20(VictoriaLogs 부재) 결정 명시.
- 사내 URL·비밀번호 커밋 금지. 커밋 관례, 태스크당 1커밋+.

## 파일 구조 (신규/변경)

```
.github/workflows/release-images.yml       # T1
collectors/token-usage/install.sh          # T1 — vminsert 포함 매칭 보정
tools/mock-provider/k8s.yaml               # T2 — Deployment+Service (stage)
tools/mock-provider/README.md              # T2 — 배포 절 추가
tools/data-admin/{delete_data.py,requirements-dev.txt,conftest.py,tests/}  # T3
.github/workflows/test-tools.yml           # T3
docs/monitoring/grafana_dashboard_token_usage.json  # T4
docs/operations/stage-runbook.md           # T5
```

---

### Task 1: 이미지 릴리스 워크플로 + VM 탐색 보정

**Files:**
- Create: `.github/workflows/release-images.yml`
- Modify: `collectors/token-usage/install.sh` (vminsert 탐색), `docs/operations/company-verify.md`(이미지 공급 언급 시 정합 확인)

**Interfaces:**
- Produces: `ghcr.io/yoonsungnam/{token-mock-provider,token-usage-collector,token-mart}:{latest,<sha7>}`. **주의: install.sh 기본 태그는 로컬 HEAD의 git short SHA — CI가 push한 태그와 다를 수 있으므로 stage 런북 커맨드는 반드시 `--tag latest`(또는 확인된 sha7)를 명시**한다 (T5에 반영).

- [ ] **Step 1: release-images.yml** — 트리거 `push: branches [main]`(paths: 각 모듈 소스) + `workflow_dispatch`. 잡 1개, matrix 3항목(context/이미지명): tools/mock-provider→token-mock-provider, collectors/token-usage→token-usage-collector, mart/token-usage→token-mart. 스텝: checkout → `docker/login-action@v3`(registry ghcr.io, username `${{ github.actor }}`, password `${{ secrets.GITHUB_TOKEN }}`) → build+push 2태그(`latest`, `${GITHUB_SHA::7}`). `permissions: packages: write, contents: read`. (buildx 불요 — 러너 amd64 = 클러스터 amd64.)
- [ ] **Step 2: install.sh vminsert 탐색 보정** — 홈랩 실사: 서비스명 `victoriametrics-victoria-metrics-cluster-vminsert`. `grep '^vminsert'` → `grep -E '(^|-)vminsert($|-)'`? 실측 이름에는 `-vminsert`(말미)이므로 **포함 매칭 `grep 'vminsert'`** + head -1 (vmselect/vmstorage 오매칭 없음 — 'vminsert' 부분열은 유일). vmsingle 폴백도 `grep 'vmsingle'`. 주석에 실측 서비스명 기록.
- [ ] **Step 3: 검증** — yaml 파싱, `bash -n`, 기존 CI grep 영향 없음 확인. 로컬에서 탐색 로직만 재현: `kubectl get svc -A -o jsonpath=...` 실행(읽기 전용) 후 새 grep이 vminsert 1건을 잡는지 확인.
- [ ] **Step 4: Commit** — `feat(ci): ghcr 이미지 릴리스 워크플로 + vminsert 탐색 보정 (Plan 5 T1)`

---

### Task 2: mock-provider stage 배포 매니페스트

**Files:**
- Create: `tools/mock-provider/k8s.yaml`
- Modify: `tools/mock-provider/README.md` (## stage 배포 절)

**Interfaces:**
- Produces: monitoring ns의 Deployment/Service **2쌍** `token-mock-provider-a`/`-b`(:8000) — **endpoints.yaml 실물이 이미 2개 서비스**(Mock Service A/B)를 전제하므로 2벌 배포가 정본. endpoints.yaml의 baseUrl을 `http://token-mock-provider-a.monitoring.svc:8000` / `-b`로 **무조건 갱신**(현행 `mock-provider-a/-b.token-pipeline.svc` — 존재하지 않는 ns/이름). coverage 기대 = enabled 2. 런북(T5)이 적용 순서 소유.

- [ ] **Step 1: k8s.yaml** — Deployment+Service 2쌍(`token-mock-provider-a`: MOCK_SERVICE="Mock Service A"/MOCK_SEED="stage-seed-a", `-b`: MOCK_SERVICE="Mock Service B"/MOCK_SEED="stage-seed-b" — MOCK_SERVICE_GROUP="Mock Group" 공통, MOCK_USERS="50"/MOCK_ANON_USERS="10", replicas 1, image `ghcr.io/yoonsungnam/token-mock-provider:latest`, imagePullPolicy Always, imagePullSecrets registry-pull-secret, readinessProbe httpGet /healthz:8000, resources requests 64Mi/limits 256Mi). namespace 미기재(-n 주입) — 동료 flat 매니페스트 관례.
- [ ] **Step 2: collectors endpoints.yaml 갱신(무조건)** — 2개 서비스의 baseUrl을 `http://token-mock-provider-a.monitoring.svc:8000` / `http://token-mock-provider-b.monitoring.svc:8000`으로. 서비스명·그룹은 기존 값 유지. (collector E2E는 endpoints.e2e.yaml을 자체 생성하므로 무영향 — 확인.)
- [ ] **Step 3: README 배포 절** — apply 커맨드, env 커스터마이즈, 이미지 공급(release-images).
- [ ] **Step 4: 검증** — `kubectl apply --dry-run=client -f`(클러스터 무변형 — client dry-run은 로컬 검증), yaml 파싱, 기존 mock 테스트 회귀(40).
- [ ] **Step 5: Commit** — `feat(mock): stage 배포 매니페스트 (Plan 5 T2)`

---

### Task 3: tools/data-admin/delete_data.py + 테스트 + CI

**Files:**
- Create: `tools/data-admin/{delete_data.py,requirements-dev.txt,conftest.py,tests/__init__.py,tests/test_delete_data.py}`
- Create: `.github/workflows/test-tools.yml`
- Modify: `docs/operations/rerun.md` ("파기 요청 처리" 절 — §8.3 의무)

**Interfaces:**
- Produces: `python3 tools/data-admin/delete_data.py --mode date --from D1 --to D2 [--service S] [--yes]` / `--mode user --user-id U [--yes]`. CH 접속 env(CH_HOST/PORT/USER/PASSWORD/CH_CLUSTER — collectors 계약) + DB명 env(CH_DB_FACT/CH_DB_DIM/CH_DB_MART — **mart/token-usage/app/ch.py 계약**과 동일: 기본 fact/gpu_data/mart). dry-run 기본.

**동작 계약(§8.3 ② — Global Constraints 상세)**:
- 모드 date: 대상 = `{DB_FACT}.raw_token_usage_1d_local`, `raw_token_usage_summary_1d_local` (audit 제외 — 주석으로 감사 불변 명시). 술어 `date BETWEEN {d1} AND {d2} [AND service = {s}]`. dry-run: `_dist`에서 count 출력. 실행: ON CLUSTER ALTER DELETE → wait_for_mutations(3s/300s, clusterAllReplicas — CH_CLUSTER 시) → 완료 후 **동일 기간 mart rerun 의무 안내 항상 출력** — mart rerun은 `--context` 필수이므로 delete_data가 `--context <ctx> --namespace <ns>`(안내 치환용 옵션, 기본 `<context>`/`monitoring` 플레이스홀더)를 받아 `python3 mart/token-usage/tools/rerun.py --context <ctx> --namespace <ns> --from D1 --to D2` 형식으로 출력(테스트로 고정).
- 모드 user: 대상 3테이블 = fact 상세, `{DB_MART}.token_usage_1d_local`, `{DB_DIM}.view_token_usage_1d_local`. 술어 `user_id = {u}`. agg/summary는 대상 아님(개인 식별 불가 집계 — 주석). dry-run 동일. 완료 후 "dim_token_user_org 행 파기/가명화는 별도 admin 경로(§6.1 보존 규칙)" 안내.
- 삭제는 되돌릴 수 없음 — `--yes` 없으면 dry-run 결과만 내고 종료(exit 0). 진행 시 대상 요약 재출력 후 실행. exit: 0/1(실행 오류)/2(인자).

- [ ] **Step 1: 실패 테스트** — FakeCH 주입(collectors FakeCH 스타일): 모드별 SQL 생성(테이블·술어·ON CLUSTER), audit 미포함, dry-run이 command 미호출(query만), --yes 시 mutation 시퀀스+wait, mart rerun 안내 출력, user 모드 3테이블, exit 2 케이스(모드 누락·from>to·user-id 공백).
- [ ] **Step 2: FAIL** → **Step 3: 구현**(clickhouse-connect, mart/token-usage/app/ch.py의 wait_for_mutations 로직 이식 — 의존 없이 자체 포함) → **Step 4: 통과** — `cd tools/data-admin && python3 -m pytest tests/ -q`
- [ ] **Step 5: test-tools.yml** — unit 잡(paths: tools/data-admin/**, 워크플로 자신).
- [ ] **Step 6: rerun.md "파기 요청 처리" 절** — delete_data.py user 모드 + dim 별도 경로 + 절차(§8.3 의무 명시).
- [ ] **Step 7: Commit** — `feat(tools): delete_data.py — 날짜 정정·user_id 파기 (§8.3, Plan 5 T3)`

---

### Task 4: Grafana 테스터 대시보드

**Files:**
- Create: `docs/monitoring/grafana_dashboard_token_usage.json`
- Create: `docs/monitoring/README.md` (임포트 절차·데이터소스 요건)

**Interfaces:**
- Consumes: `gpu_data.view_token_usage_*` 4테이블(+ coverage 품질은 mart.agg_token_service_1d의 diff/is_derived — mart 계정 접속).
- Produces: Grafana 임포트용 JSON — 데이터소스는 **변수 `${DS_CLICKHOUSE}`**(grafana-clickhouse-datasource 플러그인 전제 — 미설치면 런북의 설치 항목).

- [ ] **Step 1: 대시보드 JSON** — 패널 구성(§7.3 + §6 관점, 전부 view/agg 조회 SQL 포함):
  1. 서비스별 일별 total_input_tokens 추이 (view_token_usage_service_1d, 시계열)
  2. org 롤업 (view_token_usage_org_1d — `arraySlice(org_path,1,$org_depth)` GROUP BY, 변수 $org_depth 기본 1)
  3. 모델별 토큰·cost (view_token_usage_model_1d)
  4. 서비스 대사 품질 — diff_* 비0 서비스 테이블(agg 조회, is_derived/summary 부재 NULL 구분)
  5. unknown 버킷 비율 (org 매핑 실패 관측)
  6. anon 핸들명 사용 상위 (view_token_usage_1d WHERE user_type='anonymous' GROUP BY user_name — 핸들명 표기 결정 검증)
  7. 일별 수집 커버리지 — agg_service 행수/enabled 수(참고 텍스트 패널: BATCH_RESULT 마커 패널은 VictoriaLogs 필요 — company 단계, §9-20)
  - **모든 FROM 대상은 `_dist` 접미 테이블**(물리 테이블명 — `_dist` 누락 시 전 패널 실패). 시간 필터는 `$__fromTime/$__toTime`(CH 플러그인 매크로) 또는 date 컬럼 BETWEEN — 플러그인 문법 확인해 일관 적용. schemaVersion 최신, uid 고정 `token-usage-stage`.
  - **임포트 호환**: 최상위 `__inputs` 블록에 datasource 입력(`DS_CLICKHOUSE`, type `grafana-clickhouse-datasource`) 선언 + 각 패널 datasource는 객체형 `{"type":"grafana-clickhouse-datasource","uid":"${DS_CLICKHOUSE}"}` — 이 선언이 없으면 임포트 시 매핑 프롬프트가 뜨지 않아 전 패널 datasource not found.
- [ ] **Step 2: JSON 유효성** — python 스크립트로: json.load + 필수 키(panels/templating/uid/**__inputs**) 단정 + **JSON 내 모든 FROM 테이블명을 추출해 ddl/**/*.sql의 CREATE TABLE 이름과 대조 일치**(전부 `_dist`) + 패널 datasource 객체형 확인. SQL들은 clickhouse-format 검증.
- [ ] **Step 3: README** — 임포트 절차, CH 데이터소스 설정(mart 계정 — 계정 공유 결정, URL http://chi-....clickhouse.svc:8123), 플러그인 요건.
- [ ] **Step 4: Commit** — `feat(monitoring): Grafana 테스터 대시보드 (Plan 5 T4)`

---

### Task 5: 스테이지 런북 + 최종 정리

**Files:**
- Create: `docs/operations/stage-runbook.md`
- Modify: 각 모듈 README의 스테이지 관련 링크(필요 시)

- [ ] **Step 1: stage-runbook.md** — 실행 가능한 커맨드로 전 절차(전제: kubectl context homelab, GitHub PAT read:packages):
  0. 사전: (a) release-images 워크플로 1회 실행 확인(ghcr 3이미지 — latest 태그), (b) GitHub PAT(read:packages) 준비, (c) **kubectl 컨텍스트 정렬** — 이 머신의 현행 컨텍스트명은 `kubernetes-admin@kubernetes`인데 install.sh 기본값은 `homelab`: `kubectl config rename-context kubernetes-admin@kubernetes homelab`을 정본 절차로 실행(또는 전 커맨드에 실제 컨텍스트명 명시 — 런북은 rename을 정본으로 채택)
  1. mock 배포: `kubectl apply -n monitoring -f tools/mock-provider/k8s.yaml` (+registry-pull-secret 선행 — collectors install.sh가 만들거나 수동 생성 커맨드 병기) → healthz 확인
  2. admin DDL: accounts.sql(계정 공유 — mart 계정에 GRANT 추가 + fact DB 생성)·assets accounts — chi 파드 clickhouse-client 실행 커맨드 병기
  3. collectors 설치: `./collectors/token-usage/install.sh --tag latest stage` — **`--tag latest` 필수**(생략 시 로컬 SHA 태그 → ghcr에 없어 ImagePullBackOff). Secret 입력값 표(mart 계정 등). (build.sh는 CI release-images가 대체 — 로컬 docker 없음)
  4. assets 시드: dim DDL은 install.sh(mart) 대상 아님 — 런북이 chi 파드 적용 커맨드 병기(dim 2종 + seed + 합성 로스터: `csv_to_dim_user_org_insert.py --csv assets/user-org/fixtures/synthetic_org_members.csv` 생성 SQL 적용)
  5. mart 설치: `./mart/token-usage/install.sh --tag latest stage` (**--tag latest 필수** — 위와 동일 사유)
  6. 수동 체인 실행: collectors rerun(어제) → `--chain-mart` → 마커 확인 커맨드(kubectl logs)
  7. **성공 기준 체크리스트**(각 항목 SQL/커맨드 병기): coverage 마커 정확, 3계층 합계 일치, 조직 귀속(합성 로스터 기준), cost 계산, anon 핸들명 view 노출, **멱등 2-run 행수 보존(insert_deduplicate — stage 1레플리카 ZK dedup 실검증)**, rerun --chain-mart exit 전파, Grafana 대시보드 패널 표시(mart 계정)
  8. 정례화: CronJob 활성 상태 확인(02:00/04:00 자동 — suspend 아님), 다음날 마커 확인
  9. 철수(필요 시): CronJob·Secret·mock 삭제, DB는 유지(stage 데이터)
  - §9-19: 전-레플리카 검증 항목은 company-verify 1단계로 이관(런북 범위 아님 명시), §9-20: VictoriaLogs 마커 패널은 company 단계.
- [ ] **Step 2: 문서-코드 대조**(커맨드·이름 전수) → 3모듈+tools 단위 테스트 전체.
- [ ] **Step 3: Commit** — `docs(ops): stage 런북 (Plan 5 T5)`
