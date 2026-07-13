# collector 배포 (Plan 2b) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** collectors/token-usage를 k8s CronJob으로 배포 가능하게 만든다 — Dockerfile/build.sh/install.sh/kustomize 매니페스트/tools/rerun.py(--chain-mart)/rerun.md.

**Architecture:** 동료 gpu-data-pipeline의 모듈=배포단위 관례를 따르되 스펙 §7.2 계약이 우선한다(환경 2단 stage|company, envFrom Secret, kustomize base+overlays, DDL 실행 주체 분리). 오케스트레이터 없이 cron 오프셋(수집 02:00 → mart 04:00 KST)으로 의존성을 표현한다.

**Tech Stack:** bash, kustomize(kubectl -k), Python 3.10+(rerun.py, 서드파티 의존성 금지 — stdlib만), GitHub Actions.

## Global Constraints

- **Python 3.10 호환** (dev 머신 3.10): `X | None` OK(3.10+), `StrEnum`·`tomllib` 금지. 이미지/CI는 3.12.
- **로컬에 docker 없음**: 컨테이너/매니페스트 검증은 CI가 수행. 로컬 검증은 `bash -n`, `python3 -m pytest`, yaml 파싱까지.
- **로컬에서 공유 클러스터(kubectl) 변형 금지**: install.sh/rerun.py 실행 검증은 이 플랜 범위 밖(Plan 5 stage 통합에서 사용자 참여하에). 이 플랜은 코드·테스트·CI 검증까지.
- **계약 수치(§5.2/§7.2 — 임의 변경 금지)**: schedule `0 2 * * *` + `timeZone: Asia/Seoul`, `concurrencyPolicy: Forbid`, `backoffLimit: 1`, `activeDeadlineSeconds: 4320`, historyLimit 3/3, resources requests 256Mi / limits 1Gi (**limits 없는 배포 금지**), imagePullSecrets 이름 `registry-pull-secret`, Secret 이름 `token-usage-ch-secret`(envFrom), ConfigMap 이름 `token-usage-endpoints`, CH 계정 `token_collector`, CH_CLUSTER=`gpu-monitoring`(stage·company 동일 — 동료 stage 관례 'metrics' 복사 금지).
- **보안 경계(§7.2)**: 사내 서비스 URL·비밀번호·실사용자 데이터를 레포에 커밋하지 않는다. `endpoints.company.yaml`은 gitignored — install.sh가 ConfigMap으로 주입. Secret은 install.sh 대화형 생성(무에코), 매니페스트에 자격증명 없음.
- **DDL 실행 주체 분리(§7.2)**: install.sh는 테이블 DDL(raw_token_usage.sql, dim_token_service.sql)만 자동 적용. `CREATE DATABASE`/`CREATE USER`/GRANT(accounts.sql)는 admin 수동 — install.sh는 안내만 출력.
- **rerun 체이닝 계약(§8.3 v1.4)**: collectors rerun의 `--from/--to`가 mart rerun에 **동일 값 그대로** 전파. 완료 시 mart rerun 명령을 **항상 출력**, `--chain-mart`는 직접 트리거. `--from/--to`는 **inclusive** (main.py 계약 — 동료 metric rerun의 to-제외와 다름, 문서에 명시).
- **마커 계약(§5.6)**: rerun Job도 동일 BATCH_RESULT/SERVICE_RESULT 마커를 출력하며 rerun.py는 이를 스트리밍만 할 뿐 가공하지 않는다. 로그에 user_id 원문·페이로드 금지 규약은 rerun.py 출력에도 적용.
- **VM_PUSH_URL은 경로 없는 베이스 URL**: `vm_push.py:31`이 `/api/v1/import/prometheus`를 스스로 부착한다 (tests/test_vm_push.py가 계약 고정). vmsingle → `http://<svc>.<ns>.svc:<port>`, vminsert(클러스터판) → `http://<svc>.<ns>.svc:<port>/insert/0/prometheus`. 경로를 포함해 주입하면 전 push가 조용히 404(WARN)로 죽는다.
- 커밋 메시지는 기존 관례(`feat(collectors): ...`, `docs: ...`)를 따르고 태스크당 1커밋 이상.

## 참고 파일 (구현자가 열어볼 것)

- 스펙 §5.1/§5.2/§5.6/§7.2/§8.3/§8.4: `docs/superpowers/specs/2026-07-10-token-data-pipeline-design.md`
- 기존 모듈: `collectors/token-usage/` (app/config.py의 env 계약, app/main.py의 CLI 계약)
- 동료 관례 원본: `/home/mini/github/gpu-data-pipeline/collectors/metric/{build.sh,install.sh,k8s/,tools/rerun.py}` (참고용 — 스펙과 충돌 시 스펙 우선)
- 패키징 선례: `tools/mock-provider/Dockerfile`, `.github/workflows/test-mock-provider.yml`의 image 잡

---

### Task 1: Dockerfile + build.sh + CI image 잡

**Files:**
- Create: `collectors/token-usage/Dockerfile`
- Create: `collectors/token-usage/build.sh` (chmod +x)
- Modify: `.github/workflows/test-collector.yml` (image 잡 추가)

**Interfaces:**
- Produces: 이미지 이름 `token-usage-collector`, 태그 기본 git short SHA. `./collectors/token-usage/build.sh [--registry R] [--tag T] <stage|company>`. Task 3의 install.sh가 동일 `--registry/--tag` 규약을 소비.

- [ ] **Step 1: Dockerfile 작성**

```dockerfile
# 스펙 §7.2: python:3.12-slim, requirements 선복사 캐시, 이미지 1개 + CronJob command 교체.
# BASE_IMAGE는 company 빌드에서 Harbor proxy로 치환된다 (build.sh --registry 경로).
ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
# endpoints.yaml은 이미지에 굽지 않는다 — ConfigMap 마운트 + ENDPOINTS_FILE env가 정본 (§7.2)
CMD ["python", "-m", "app.main"]
```

- [ ] **Step 2: build.sh 작성**

```bash
#!/usr/bin/env bash
# token-usage collector 이미지 빌드/푸시 (스펙 §7.2 스크립트 규약)
#
# 사용법:
#   ./collectors/token-usage/build.sh [--registry <registry>] [--tag <tag>] <stage|company>
#
#   stage:   REGISTRY 기본 ghcr.io/yoonsungnam
#   company: --registry 필수 (사내 Harbor) — BASE_IMAGE를 Harbor proxy로 치환
#   태그 기본: git short SHA (git 밖이면 latest)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

IMAGE_NAME="token-usage-collector"
REGISTRY=""
TAG=""
ENV=""

usage() {
  grep '^#' "$0" | head -8; exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry) REGISTRY="$2"; shift 2 ;;
    --tag)      TAG="$2"; shift 2 ;;
    stage|company) ENV="$1"; shift ;;
    *) echo "[ERROR] unknown arg: $1"; usage ;;
  esac
done
[[ -n "${ENV}" ]] || usage

if [[ -z "${REGISTRY}" ]]; then
  case "${ENV}" in
    stage)   REGISTRY="ghcr.io/yoonsungnam" ;;
    company) echo "[ERROR] company 환경에서는 --registry 옵션이 필수입니다."; usage ;;
  esac
fi

if [[ -z "${TAG}" ]]; then
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    TAG="$(git rev-parse --short HEAD)"
  else
    TAG="latest"
  fi
fi

BUILD_ARGS=()
if [[ "${ENV}" == "company" ]]; then
  # 사내망은 docker hub 직접 pull 불가 — Harbor pull-through proxy 경유 (동료 레포 관례)
  BUILD_ARGS+=(--build-arg "BASE_IMAGE=${REGISTRY%%/*}/proxy-docker-registry-1.docker.io/python:3.12-slim")
fi

docker buildx build --platform linux/amd64 "${BUILD_ARGS[@]}" \
  -t "${IMAGE_NAME}:${TAG}" . --load

FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}:${TAG}"
docker tag "${IMAGE_NAME}:${TAG}" "${FULL_IMAGE}"
docker push "${FULL_IMAGE}"

echo ""
echo "[OK] pushed ${FULL_IMAGE}"
echo "다음 단계:"
echo "  ./collectors/token-usage/install.sh --registry ${REGISTRY} --tag ${TAG} ${ENV}"
```

- [ ] **Step 3: `bash -n collectors/token-usage/build.sh` 통과 확인, `chmod +x`**

- [ ] **Step 4: CI image 잡 추가** — `.github/workflows/test-collector.yml`의 jobs에 추가 (mock-provider image 잡 패턴):

```yaml
  image:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: collectors/token-usage
    steps:
      - uses: actions/checkout@v4
      - name: Build image
        run: docker build -t token-usage-collector:ci .
      - name: Container smoke test
        run: |
          # CH 없이 argparse 도움말이 exit 0으로 떠야 함 (이미지 엔트리 검증)
          docker run --rm token-usage-collector:ci python -m app.main --help
```

- [ ] **Step 5: 로컬 검증** — docker 없으므로: `bash -n build.sh`, `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/test-collector.yml'))"`, 기존 단위 테스트 회귀 없음(`python3 -m pytest tests/ --ignore=tests/e2e -q`).

- [ ] **Step 6: Commit** — `feat(collectors): Dockerfile + build.sh + CI image smoke (Plan 2b T1)`

---

### Task 2: k8s kustomize base + overlays + CI manifests 잡

**Files:**
- Create: `collectors/token-usage/k8s/base/kustomization.yaml`
- Create: `collectors/token-usage/k8s/base/cronjob.yaml`
- Create: `collectors/token-usage/k8s/overlays/stage/kustomization.yaml`
- Create: `collectors/token-usage/k8s/overlays/company/kustomization.yaml`
- Modify: `.github/workflows/test-collector.yml` (manifests 잡 추가)

**Interfaces:**
- Produces: CronJob 이름 `token-usage-collector`(Task 3 install.sh의 `set image/set env` 대상, Task 5 rerun.py의 `--from=cronjob/` 대상), Secret 이름 `token-usage-ch-secret`, ConfigMap 이름 `token-usage-endpoints`, env `ENDPOINTS_FILE=/etc/token-usage/endpoints.yaml`.

- [ ] **Step 1: base/cronjob.yaml 작성**

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: token-usage-collector
spec:
  # 매일 02:00 KST 실행 (§5.1) — mart 배치(04:00 KST)가 이 잡의 완료를 전제 (§3 cron 오프셋 의존)
  schedule: "0 2 * * *"
  timeZone: Asia/Seoul
  # §4.0 no-op DELETE 스킵 규칙의 전제: 단일 작성자 (경합 금지)
  concurrencyPolicy: Forbid
  # 컨트롤러 일시 중단 등으로 02:00을 놓쳐도 1시간 내에는 따라잡는다 (스펙 미명시 — 선택값)
  startingDeadlineSeconds: 3600
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      backoffLimit: 1
      # §5.2 산식 계약: soft deadline 50분 + 적재 시퀀스 예산 12분 + 종료 마진 10분 = 4320s
      # 산식과 연동된 값 — 단독 수정 금지
      activeDeadlineSeconds: 4320
      template:
        spec:
          # Never: 실패 시 새 파드로 1회 재시도(backoffLimit) — 파드 로그가 실행 단위와 1:1이 되어
          # BATCH_RESULT '1 실행 = 1 마커 라인'(§5.6) 소비가 깔끔 (mart/s2job 관례)
          restartPolicy: Never
          imagePullSecrets:
            - name: registry-pull-secret
          containers:
            - name: token-usage-collector
              image: token-usage-collector:latest
              imagePullPolicy: Always
              envFrom:
                - secretRef:
                    name: token-usage-ch-secret
              env:
                - name: ENDPOINTS_FILE
                  value: /etc/token-usage/endpoints.yaml
              volumeMounts:
                - name: endpoints
                  mountPath: /etc/token-usage
                  readOnly: true
                - name: ca-bundle
                  mountPath: /etc/token-usage-ca
                  readOnly: true
              resources:
                requests:
                  cpu: 100m
                  memory: 256Mi
                limits:
                  cpu: "1"
                  # §7.2 (v1.6 OOM 실경험): limits 없는 배포 금지.
                  # MAX_BUFFER_ROWS 20,000행 flush 전제의 1Gi
                  memory: 1Gi
              # CA 번들은 install.sh가 ConfigMap으로 주입(선택) — 있으면
              # COLLECTOR_API_CA_BUNDLE=/etc/token-usage-ca/ca-bundle.pem (Secret 키)
          volumes:
            - name: endpoints
              configMap:
                name: token-usage-endpoints
            - name: ca-bundle
              configMap:
                name: token-usage-ca-bundle
                optional: true
```

- [ ] **Step 2: base/kustomization.yaml**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - cronjob.yaml
```

- [ ] **Step 3: overlays/stage/kustomization.yaml**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
# namespace는 매니페스트에 고정하지 않는다 — install.sh의 -n ${NAMESPACE}(기본 monitoring)에 일원화
# (고정 시 --namespace 옵션과 apply -k가 충돌)
resources:
  - ../../base
images:
  - name: token-usage-collector
    newName: ghcr.io/yoonsungnam/token-usage-collector
    # 실제 태그는 install.sh가 kubectl set image로 덮는다 (build.sh 태그와 일치)
    newTag: latest
```

- [ ] **Step 4: overlays/company/kustomization.yaml**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
# namespace 고정 없음 — install.sh -n 일원화 (stage overlay와 동일 사유)
resources:
  - ../../base
# 이미지 주소는 install.sh가 --registry/--tag로 kubectl set image 주입 (사내 Harbor 주소 커밋 금지)
```

- [ ] **Step 5: CI manifests 잡 추가** — `.github/workflows/test-collector.yml` jobs에:

```yaml
  manifests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Render overlays
        run: |
          kubectl kustomize collectors/token-usage/k8s/overlays/stage   > /tmp/stage.yaml
          kubectl kustomize collectors/token-usage/k8s/overlays/company > /tmp/company.yaml
      - name: Contract fields locked (§5.2/§7.2)
        run: |
          for f in /tmp/stage.yaml /tmp/company.yaml; do
            grep -q 'activeDeadlineSeconds: 4320' "$f"
            grep -q 'concurrencyPolicy: Forbid' "$f"
            grep -q 'timeZone: Asia/Seoul' "$f"
            grep -q 'schedule: 0 2 \* \* \*' "$f"
            grep -q 'memory: 1Gi' "$f"
            grep -q 'name: registry-pull-secret' "$f"
            grep -q 'name: token-usage-ch-secret' "$f"
            grep -q 'name: token-usage-endpoints' "$f"
          done
          grep -q 'ghcr.io/yoonsungnam/token-usage-collector' /tmp/stage.yaml
```

(주의: kustomize 렌더 출력에서 schedule 값은 따옴표가 벗겨질 수 있음 — 잡이 실패하면 실제 렌더 출력을 보고 grep 패턴을 렌더 결과에 맞출 것. 계약값 자체를 바꾸는 방향은 금지.)

- [ ] **Step 6: 로컬 검증** — kubectl은 로컬에 있음(클러스터 접근 불필요): `kubectl kustomize collectors/token-usage/k8s/overlays/stage`와 `.../company` 렌더 성공 + Step 5의 grep들을 로컬에서 그대로 실행해 통과 확인.

- [ ] **Step 7: Commit** — `feat(collectors): k8s kustomize base/overlays + CI manifest contract checks (Plan 2b T2)`

---

### Task 3: install.sh

**Files:**
- Create: `collectors/token-usage/install.sh` (chmod +x)
- Modify: `collectors/token-usage/ddl/company/raw_token_usage.sql` (CREATE DATABASE 이동)
- Modify: `collectors/token-usage/ddl/company/accounts.sql` (CREATE DATABASE 수용)
- Modify: `collectors/token-usage/tests/e2e/run_e2e.sh` (E2E DDL 변환 보정)
- Modify: `collectors/token-usage/ddl/README.md` (적용 순서 문구)

**Interfaces:**
- Consumes: Task 1의 `--registry/--tag` 규약, Task 2의 CronJob/Secret/ConfigMap 이름 + ca-bundle optional 볼륨.
- Produces: `./collectors/token-usage/install.sh [--registry R] [--tag T] [--context C] [--namespace N] [--endpoints F] <stage|company>` — README(Task 6)가 이 인터페이스를 문서화.

- [ ] **Step 0: DDL 실행 주체 경계 정리 (§7.2 위반 해소)** — `raw_token_usage.sql`의 `CREATE DATABASE IF NOT EXISTS fact ON CLUSTER 'gpu-monitoring';` 문을 삭제하고 `accounts.sql` 상단(admin 수동 실행 파일)으로 이동한다(주석: "fact DB 생성은 admin — §7.2 DDL 실행 주체 분리"). install.sh가 자동 적용하는 파일에 DB 생성이 남아 있으면 공유 클러스터 경계 위반.
  - E2E 보정: `tests/e2e/run_e2e.sh`의 DDL 변환 파이썬에서 `sql += "\nCREATE DATABASE IF NOT EXISTS gpu_data;\n"` 옆에 `sql = "CREATE DATABASE IF NOT EXISTS fact;\n" + sql`을 추가(단일노드 E2E는 admin 절차가 없으므로 스크립트가 대신 생성).
  - `ddl/README.md` 적용 순서 1항에 "fact DB 생성 포함(accounts.sql)" 문구 반영.
  - 검증: `python3 -m pytest tests/ --ignore=tests/e2e -q` 회귀 없음 + `bash -n tests/e2e/run_e2e.sh` (E2E 자체는 CI에서 확인).

- [ ] **Step 1: install.sh 작성**

```bash
#!/usr/bin/env bash
# token-usage collector 설치 (스펙 §7.2)
#
# 사용법:
#   ./collectors/token-usage/install.sh [--registry <registry>] [--tag <tag>] \
#     [--context <kube-context>] [--namespace <ns>] [--endpoints <file>] <stage|company>
#
#   stage:   context 기본 homelab, endpoints 기본 endpoints.yaml (mock)
#   company: --context/--registry 필수, endpoints 기본 endpoints.company.yaml (gitignored)
#
# 수행 순서:
#   [1/6] registry-pull-secret 멱등 생성 (대화형)
#   [2/6] token-usage-ch-secret 멱등 생성 (대화형 — envFrom으로 컨테이너 env가 됨)
#   [3/6] token-usage-endpoints ConfigMap 생성/갱신
#   [4/6] 테이블 DDL 적용 (chi-* 파드 자동 탐색 — accounts.sql은 admin 수동, §7.2)
#   [5/6] CronJob 배포 (kustomize overlay)
#   [6/6] 이미지 주소/CH_HOST/VM_PUSH_URL 주입 + 수동 테스트 커맨드 안내
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

IMAGE_NAME="token-usage-collector"
CRONJOB_NAME="token-usage-collector"
SECRET_NAME="token-usage-ch-secret"
PULL_SECRET_NAME="registry-pull-secret"
CONFIGMAP_NAME="token-usage-endpoints"
CH_NAMESPACE="clickhouse"

REGISTRY=""; TAG=""; KUBE_CONTEXT=""; NAMESPACE="monitoring"; ENDPOINTS_SRC=""; ENV=""

usage() { sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry)  REGISTRY="$2"; shift 2 ;;
    --tag)       TAG="$2"; shift 2 ;;
    --context)   KUBE_CONTEXT="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --endpoints) ENDPOINTS_SRC="$2"; shift 2 ;;
    stage|company) ENV="$1"; shift ;;
    *) echo "[ERROR] unknown arg: $1"; usage ;;
  esac
done
[[ -n "${ENV}" ]] || usage

case "${ENV}" in
  stage)
    KUBE_CONTEXT="${KUBE_CONTEXT:-homelab}"
    REGISTRY="${REGISTRY:-ghcr.io/yoonsungnam}"
    ENDPOINTS_SRC="${ENDPOINTS_SRC:-${HERE}/endpoints.yaml}"
    ;;
  company)
    [[ -n "${KUBE_CONTEXT}" ]] || { echo "[ERROR] company 환경에서는 --context 옵션이 필수입니다."; usage; }
    [[ -n "${REGISTRY}" ]]     || { echo "[ERROR] company 환경에서는 --registry 옵션이 필수입니다."; usage; }
    ENDPOINTS_SRC="${ENDPOINTS_SRC:-${HERE}/endpoints.company.yaml}"
    ;;
esac

if [[ -z "${TAG}" ]]; then
  if git -C "${HERE}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    TAG="$(git -C "${HERE}" rev-parse --short HEAD)"
  else
    TAG="latest"
  fi
fi

KUBECTL="kubectl --context=${KUBE_CONTEXT} --insecure-skip-tls-verify"

# kube API 서버 호스트를 NO_PROXY에 자동 추가 (§7.2 — 사내 프록시 환경에서 kubectl 통신 보존)
api_server="$(${KUBECTL} config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || true)"
if [[ -n "${api_server}" ]]; then
  api_host="$(printf '%s' "${api_server}" | sed -E 's#^https?://##; s#:[0-9]+$##')"
  export NO_PROXY="${NO_PROXY:+${NO_PROXY},}${api_host}"
  export no_proxy="${no_proxy:+${no_proxy},}${api_host}"
fi

echo "=== token-usage collector install (${ENV}) ==="
echo "context=${KUBE_CONTEXT} namespace=${NAMESPACE} image=${REGISTRY}/${IMAGE_NAME}:${TAG}"
echo "endpoints=${ENDPOINTS_SRC}"

# ── [1/6] registry pull secret ────────────────────────────────────────────────
echo ""
echo "[1/6] image pull secret '${PULL_SECRET_NAME}'"
if ${KUBECTL} get secret "${PULL_SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  read -r -p "  이미 존재합니다. 갱신하시겠습니까? [y/N] " ans
else
  ans="y"
fi
if [[ "${ans}" == "y" || "${ans}" == "Y" ]]; then
  read -r -p "  registry server [${REGISTRY%%/*}]: " reg_server
  reg_server="${reg_server:-${REGISTRY%%/*}}"
  read -r -p "  registry username: " reg_user
  read -r -s -p "  registry password/token: " reg_pass; echo ""
  ${KUBECTL} create secret docker-registry "${PULL_SECRET_NAME}" \
    --docker-server="${reg_server}" --docker-username="${reg_user}" \
    --docker-password="${reg_pass}" -n "${NAMESPACE}" \
    --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -
fi

# ── [2/6] app secret (envFrom — 키 이름이 곧 컨테이너 env 이름, §5.7) ─────────
echo ""
echo "[2/6] app secret '${SECRET_NAME}'"
if ${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  read -r -p "  이미 존재합니다. 갱신하시겠습니까? [y/N] " ans
else
  ans="y"
fi
if [[ "${ans}" == "y" || "${ans}" == "Y" ]]; then
  read -r -p "  CH_USER [token_collector]: " ch_user
  ch_user="${ch_user:-token_collector}"
  read -r -s -p "  CH_PASSWORD: " ch_pass; echo ""
  read -r -p "  COLLECTOR_HTTPS_PROXY ('none'=직접 연결, enter=시스템 상속, 값=프록시 URL): " http_proxy_v
  read -r -p "  사내 CA 번들 파일 경로 (없으면 enter): " ca_bundle_v
  args=(--from-literal="CH_USER=${ch_user}"
        --from-literal="CH_PASSWORD=${ch_pass}"
        --from-literal="CH_PORT=8123"
        --from-literal="CH_CLUSTER=gpu-monitoring")
  # §5.7 3분기: 키 미설정=시스템 상속 / 빈 값=직접 연결 / 값=전용 프록시.
  # read로는 미입력과 빈 값이 구분되지 않으므로 'none' 센티널로 빈 값을 받는다.
  case "${http_proxy_v}" in
    "")   ;;                                                         # enter → 키 미설정 = 상속
    none) args+=(--from-literal="COLLECTOR_HTTPS_PROXY=") ;;         # 빈 값 = 직접 연결
    *)    args+=(--from-literal="COLLECTOR_HTTPS_PROXY=${http_proxy_v}") ;;
  esac
  if [[ -n "${ca_bundle_v}" ]]; then
    # CA '파일'을 파드에 전달해야 한다 — 경로 문자열만 Secret에 넣으면 컨테이너에서 열 수 없음.
    # base cronjob.yaml의 optional ConfigMap 볼륨(/etc/token-usage-ca)에 실어 보낸다.
    [[ -f "${ca_bundle_v}" ]] || { echo "[ERROR] CA 파일이 없습니다: ${ca_bundle_v}"; exit 1; }
    ${KUBECTL} create configmap token-usage-ca-bundle \
      --from-file="ca-bundle.pem=${ca_bundle_v}" -n "${NAMESPACE}" \
      --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -
    args+=(--from-literal="COLLECTOR_API_CA_BUNDLE=/etc/token-usage-ca/ca-bundle.pem")
  fi
  ${KUBECTL} create secret generic "${SECRET_NAME}" "${args[@]}" -n "${NAMESPACE}" \
    --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -
fi

# ── [3/6] endpoints ConfigMap (§7.2 endpoints.yaml 분리 원칙) ─────────────────
echo ""
echo "[3/6] endpoints ConfigMap '${CONFIGMAP_NAME}'"
if [[ ! -f "${ENDPOINTS_SRC}" ]]; then
  echo "[ERROR] endpoints 파일이 없습니다: ${ENDPOINTS_SRC}"
  echo "        company는 사내 URL 목록을 endpoints.company.yaml(gitignored)로 준비하세요."
  exit 1
fi
${KUBECTL} create configmap "${CONFIGMAP_NAME}" \
  --from-file="endpoints.yaml=${ENDPOINTS_SRC}" -n "${NAMESPACE}" \
  --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -

# ── [4/6] 테이블 DDL (chi-* 자동 탐색, kubectl cp + clickhouse-client) ────────
echo ""
echo "[4/6] table DDL"
ch_pod="$(${KUBECTL} get pods -n "${CH_NAMESPACE}" -o name 2>/dev/null \
  | sed 's#^pod/##' | grep '^chi-' | head -1 || true)"
if [[ -z "${ch_pod}" ]]; then
  echo "[ERROR] ${CH_NAMESPACE} 네임스페이스에서 chi-* ClickHouse 파드를 찾지 못했습니다."
  exit 1
fi
echo "  ClickHouse pod: ${ch_pod}"
apply_sql() {
  local sql_file="$1" base tmp_pod_path
  base="$(basename "${sql_file}")"
  tmp_pod_path="/tmp/${base}"
  ${KUBECTL} cp "${sql_file}" "${CH_NAMESPACE}/${ch_pod}:${tmp_pod_path}"
  ${KUBECTL} exec -n "${CH_NAMESPACE}" "${ch_pod}" -- \
    sh -c "clickhouse-client --multiquery < ${tmp_pod_path}"
  ${KUBECTL} exec -n "${CH_NAMESPACE}" "${ch_pod}" -- rm -f "${tmp_pod_path}"
  echo "  applied: ${base}"
}
apply_sql "${HERE}/ddl/company/raw_token_usage.sql"
apply_sql "${HERE}/ddl/company/dim_token_service.sql"
echo "  (accounts.sql은 적용하지 않았습니다 — CREATE DATABASE/CREATE USER/GRANT는 admin 수동 실행, §7.2)"
echo "  (fact DB가 아직 없으면 admin이 accounts.sql을 먼저 실행해야 테이블 DDL이 성공합니다)"
echo "  (company: 클러스터 소유자와 협의 후 ddl/company/accounts.sql의 CHANGE_ME_* 치환 실행)"

# ── [5/6] CronJob 배포 ────────────────────────────────────────────────────────
echo ""
echo "[5/6] CronJob apply (overlay: ${ENV})"
${KUBECTL} apply -k "${HERE}/k8s/overlays/${ENV}" -n "${NAMESPACE}"

# ── [6/6] 이미지/서비스 주소 주입 ────────────────────────────────────────────
echo ""
echo "[6/6] set image / set env"
${KUBECTL} set image "cronjob/${CRONJOB_NAME}" \
  "${IMAGE_NAME}=${REGISTRY}/${IMAGE_NAME}:${TAG}" -n "${NAMESPACE}"

# CH_HOST: [4/6]에서 찾은 chi-* 파드명에서 말미 ordinal을 잘라 헤드리스 서비스명 유도
# (예: chi-gpu-monitoring-gpu-monitoring-0-0-0 → chi-gpu-monitoring-gpu-monitoring-0-0)
ch_host="${ch_pod%-*}.${CH_NAMESPACE}.svc"
${KUBECTL} set env "cronjob/${CRONJOB_NAME}" "CH_HOST=${ch_host}" -n "${NAMESPACE}"
echo "  CH_HOST=${ch_host}"

# VictoriaMetrics 자동 탐색 (§5.5) — vminsert(클러스터판) 우선, 없으면 vmsingle 폴백.
# 주의: VM_PUSH_URL은 '경로 없는 베이스' — vm_push.py가 /api/v1/import/prometheus를 부착한다.
#   vmsingle → http://<svc>.<ns>.svc:<port>
#   vminsert → http://<svc>.<ns>.svc:<port>/insert/0/prometheus  (테넌트 프리픽스만)
vm_pairs="$(${KUBECTL} get svc -A \
  -o jsonpath='{range .items[*]}{.metadata.name} {.metadata.namespace} {.spec.ports[0].port}{"\n"}{end}' 2>/dev/null || true)"
vm_line="$(printf '%s\n' "${vm_pairs}" | grep '^vminsert' | head -1 || true)"
vm_suffix="/insert/0/prometheus"
if [[ -z "${vm_line}" ]]; then
  vm_line="$(printf '%s\n' "${vm_pairs}" | grep -E '^vmsingle' | head -1 || true)"
  vm_suffix=""
fi
if [[ -n "${vm_line}" ]]; then
  read -r vm_svc vm_ns vm_port <<<"${vm_line}"
  vm_url="http://${vm_svc}.${vm_ns}.svc:${vm_port}${vm_suffix}"
  ${KUBECTL} set env "cronjob/${CRONJOB_NAME}" "VM_PUSH_URL=${vm_url}" -n "${NAMESPACE}"
  echo "  VM_PUSH_URL=${vm_url}"
else
  echo "  [WARN] VictoriaMetrics 서비스(vminsert/vmsingle)를 찾지 못했습니다 — VM_PUSH_URL 미주입(push 생략, §5.5)"
fi

echo ""
echo "[OK] 설치 완료. 수동 테스트:"
echo "  ${KUBECTL} create job --from=cronjob/${CRONJOB_NAME} ${CRONJOB_NAME}-manual-\$(date +%s) -n ${NAMESPACE}"
echo "  (또는: python3 ${HERE}/tools/rerun.py --context ${KUBE_CONTEXT} --namespace ${NAMESPACE})"
```

- [ ] **Step 2: §5.7 3분기 확인** — proxy 입력이 미설정(상속)/빈 값(직접 연결, 'none' 센티널)/값(전용 프록시)의 3분기를 정확히 만드는지 코드로 재확인. envFrom이므로 Secret 키의 존재/부재가 곧 env의 존재/부재다.

- [ ] **Step 3: `bash -n collectors/token-usage/install.sh` 통과, `chmod +x`**

- [ ] **Step 4: 셸 단위 검증(클러스터 없이)** — `KUBECTL=echo` 같은 주입은 구조상 불가하므로, 최소한: `./install.sh` 인자 없이 실행 → usage 출력 + exit 1; `./install.sh company` → --context 필수 에러; `./install.sh --endpoints /nonexistent stage`는 대화형 프롬프트 전에 죽지 않는지 확인 불가(kubectl 필요) — 여기까지는 수동 확인 없이 코드 리뷰로 갈음하고 실행 검증은 Plan 5로 명시.

- [ ] **Step 5: Commit** — `feat(collectors): install.sh — secrets/endpoints ConfigMap/table DDL/CronJob (Plan 2b T3)`

---

### Task 4: main.py `--push-vm` 옵트인 (§5.5)

**Files:**
- Modify: `collectors/token-usage/app/main.py`
- Test: `collectors/token-usage/tests/test_main.py`

**Interfaces:**
- Consumes: 기존 `run_collection(...)`과 rerun 경로의 VM push 생략 로직 (main.py — **구현 전에 반드시 현재 코드를 읽고** 생략이 구현된 정확한 지점을 확인할 것: `is_rerun`이 pusher 호출을 막는 위치).
- Produces: CLI 플래그 `--push-vm` — rerun(`--from/--to`) 경로에서도 VM push 수행. Task 5 rerun.py가 command override에 이 플래그를 전파.

**요구사항 (스펙 §5.5 원문)**: "rerun(`--from/--to`)에서는 VM push 기본 생략 (`--push-vm` 옵트인) — VM은 동일 timestamp 재push 시 하향 정정이 반영되지 않음(dedup이 큰 값 유지)."

- [ ] **Step 1: 실패 테스트 작성** — tests/test_main.py에 추가 (기존 E1/payload/FakeWriter/Clock 헬퍼 재사용):

```python
def test_rerun_vm_push_default_skip_and_push_vm_opt_in():
    calls = []

    def spy_pusher(cfg, entry, date, summary, session):
        calls.append(entry.service)
        return []

    code = run_collection(Config(), [E1], DATE, is_rerun=True, clock=Clock(),
                          sleeper=lambda s: None, fetcher=lambda e, d, c, s: payload(entry=e),
                          writer=FakeWriter(), pusher=spy_pusher)
    assert code == 0 and calls == []                     # rerun 기본: push 생략 (§5.5)

    code = run_collection(Config(), [E1], DATE, is_rerun=True, clock=Clock(),
                          sleeper=lambda s: None, fetcher=lambda e, d, c, s: payload(entry=e),
                          writer=FakeWriter(), pusher=spy_pusher, push_vm=True)
    assert code == 0 and calls == ["S1"]                 # --push-vm 옵트인
```

- [ ] **Step 2: 테스트 실패 확인** — `python3 -m pytest tests/test_main.py -q` → 신규 테스트 FAIL (`push_vm` 인자 부재 TypeError)

- [ ] **Step 3: 구현** — 현재 생략 지점은 `main.py:115` `_collect_one` 내 `if not is_rerun:` (pusher 호출 가드).
  - `_collect_one`에 `push_vm: bool = False` 파라미터 추가, 조건을 `if not is_rerun or push_vm:`으로.
  - `run_collection`에 `push_vm: bool = False` 추가, `_collect_one`으로 전달.
  - argparse에 `parser.add_argument("--push-vm", dest="push_vm", action="store_true", help="rerun 경로에서도 VM push (§5.5 옵트인)")`; `main()`의 두 `run_collection` 호출부에 `push_vm=args.push_vm` 전달. 정기 경로(batch_time, is_rerun=False)는 동작 불변.

- [ ] **Step 4: 전체 단위 테스트 통과** — `python3 -m pytest tests/ --ignore=tests/e2e -q` (기존 47 + 신규)

- [ ] **Step 5: README 환경변수/실행 섹션에 --push-vm 한 줄 추가, Commit** — `feat(collectors): --push-vm opt-in for rerun VM push (Plan 2b T4)`

---

### Task 5: tools/rerun.py + 단위 테스트

**Files:**
- Create: `collectors/token-usage/tools/rerun.py`
- Test: `collectors/token-usage/tests/test_rerun.py`

**Interfaces:**
- Consumes: CronJob `token-usage-collector`(Task 2), main.py CLI(`--from/--to/--service/--push-vm`, inclusive 날짜).
- Produces: `python3 collectors/token-usage/tools/rerun.py --context C [--namespace monitoring] [--from D1 --to D2] [--service S] [--push-vm] [--chain-mart]`. 순수 함수 `build_job_spec(cronjob_obj, job_name, command)`·`build_mart_command(ctx, ns, d1, d2)`(테스트 대상). mart 체이닝 대상 경로 상수 `MART_RERUN = "mart/token-usage/tools/rerun.py"`(Plan 3에서 확정 — 부재 시 안내 실패).

- [ ] **Step 1: 실패 테스트 작성** — `tests/test_rerun.py`. tools/는 패키지가 아니므로 파일 경로 로드:

```python
import importlib.util
import json
import pathlib

import pytest

_RERUN_PATH = pathlib.Path(__file__).resolve().parents[1] / "tools" / "rerun.py"
spec = importlib.util.spec_from_file_location("rerun", _RERUN_PATH)
rerun = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rerun)


def cronjob_obj():
    return {
        "metadata": {"name": "token-usage-collector", "namespace": "monitoring",
                     "resourceVersion": "123", "uid": "x"},
        "spec": {"jobTemplate": {"spec": {
            "backoffLimit": 1,
            "activeDeadlineSeconds": 4320,
            "template": {"spec": {"restartPolicy": "Never", "containers": [
                {"name": "token-usage-collector", "image": "img:tag"}]}},
        }}},
    }


def test_build_job_spec_overrides_command_and_strips_cron_metadata():
    job = rerun.build_job_spec(cronjob_obj(), "token-usage-collector-rerun-1",
                               ["python", "-m", "app.main", "--from", "2026-07-01", "--to", "2026-07-02"])
    assert job["kind"] == "Job"
    assert job["metadata"] == {"name": "token-usage-collector-rerun-1"}   # uid/resourceVersion 제거
    tpl = job["spec"]["template"]["spec"]["containers"][0]
    assert tpl["command"] == ["python", "-m", "app.main", "--from", "2026-07-01", "--to", "2026-07-02"]
    assert job["spec"]["activeDeadlineSeconds"] == 4320                   # 기본: CronJob 값 상속


def test_build_job_spec_deadline_override_for_range():
    # §5.2의 4320s는 '1일치' 산식 — 다일 range rerun은 일수 비례로 재설정하지 않으면
    # k8s가 72분에 강제 종료해 기간 회수(§8.3)가 불능이 된다
    job = rerun.build_job_spec(cronjob_obj(), "j", ["c"], active_deadline_s=12960)
    assert job["spec"]["activeDeadlineSeconds"] == 12960


def test_range_deadline_scales_with_days_and_caps():
    assert rerun.range_deadline_s(1) == 4320
    assert rerun.range_deadline_s(3) == 3 * 4320
    assert rerun.range_deadline_s(100) == rerun.TIMEOUT_RANGE_S           # 상한 캡


def test_collect_command_variants():
    assert rerun.build_collect_command("2026-07-01", "2026-07-02", None, False) == \
        ["python", "-m", "app.main", "--from", "2026-07-01", "--to", "2026-07-02"]
    assert rerun.build_collect_command("2026-07-01", "2026-07-01", "Mock Service A", True) == \
        ["python", "-m", "app.main", "--from", "2026-07-01", "--to", "2026-07-01",
         "--service", "Mock Service A", "--push-vm"]


def test_mart_command_propagates_dates_verbatim():
    # §8.3 v1.4 체이닝 날짜 전달 계약: --from/--to 동일 값 그대로
    cmd = rerun.build_mart_command("homelab", "monitoring", "2026-07-01", "2026-07-03")
    assert "--from 2026-07-01" in cmd and "--to 2026-07-03" in cmd and "--context homelab" in cmd


def test_from_after_to_is_usage_error():
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "homelab", "--from", "2026-07-05", "--to", "2026-07-01"])
    assert e.value.code == 2


def test_malformed_date_is_usage_error():
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "homelab", "--from", "2026/07/01", "--to", "2026-07-02"])
    assert e.value.code == 2


def test_service_without_range_is_usage_error():
    with pytest.raises(SystemExit) as e:
        rerun.main(["--context", "homelab", "--service", "S"])
    assert e.value.code == 2
```

- [ ] **Step 2: 테스트 실패 확인** — `python3 -m pytest tests/test_rerun.py -q` → 모듈/함수 부재로 FAIL

- [ ] **Step 3: rerun.py 구현**

```python
"""token-usage collector 재수행 도구 (§8.3).

두 가지 모드:
  1) 1회 수동 트리거(기본) — CronJob에서 Job 생성 (실행 시점 기준 어제 KST 수집)
  2) 날짜 범위 재수집(--from/--to, **inclusive** — main.py 계약. 동료 metric rerun의
     to-제외와 다름) — CronJob 스펙에서 Job을 만들되 command를 override

완료 시 동일 날짜 mart rerun 명령을 **항상 출력**(§8.3 의무 절차 — collectors rerun 후
mart rerun 의무), --chain-mart 지정 시 직접 트리거한다.

사용법:
  python3 collectors/token-usage/tools/rerun.py --context homelab
  python3 collectors/token-usage/tools/rerun.py --context homelab \
      --from 2026-07-01 --to 2026-07-03 [--service "Mock Service A"] [--push-vm] [--chain-mart]

옵션:
  --context     kubectl context (필수)
  --namespace   기본 monitoring
  --from/--to   YYYY-MM-DD, KST, 둘 다 inclusive. 반드시 쌍으로.
  --service     단일 서비스만 재수집 (--from/--to 필요)
  --push-vm     rerun에서도 VM push (§5.5 옵트인 — 기본 생략)
  --chain-mart  완료 후 mart rerun 직접 트리거 (§8.3)
"""
import argparse
import copy
import datetime as dt
import json
import pathlib
import subprocess
import sys
import time

CRONJOB = "token-usage-collector"
MART_RERUN = "mart/token-usage/tools/rerun.py"   # Plan 3에서 확정되는 경로 (부재 시 안내 실패)
POLL_S = 10
TIMEOUT_SINGLE_S = 80 * 60        # activeDeadlineSeconds 4320s + 재시도 1회 + 마진
TIMEOUT_RANGE_S = 6 * 3600        # 동료 관례 (기간 재수집)


def kubectl(context, args, *, capture=False, input_data=None):
    cmd = ["kubectl", f"--context={context}", "--insecure-skip-tls-verify"] + args
    return subprocess.run(cmd, check=True, text=True, input=input_data,
                          capture_output=capture)


def build_collect_command(from_d, to_d, service, push_vm):
    cmd = ["python", "-m", "app.main", "--from", from_d, "--to", to_d]
    if service:
        cmd += ["--service", service]
    if push_vm:
        cmd += ["--push-vm"]
    return cmd


def range_deadline_s(n_days):
    """§5.2의 activeDeadlineSeconds 4320s는 '1일치' 산식 — 다일 range rerun은
    일수 비례로 재설정한다 (상한 = 폴링 타임아웃). 그대로 상속하면 72분에
    k8s가 강제 종료해 기간 회수(§8.3)가 불능."""
    return min(4320 * n_days, TIMEOUT_RANGE_S)


def build_job_spec(cronjob_obj, job_name, command, active_deadline_s=None):
    """CronJob 오브젝트에서 1회성 Job 스펙 생성 + containers[0].command override.

    metadata는 name만 남긴다 (uid/resourceVersion 등 서버 필드 제거).
    active_deadline_s=None이면 jobTemplate.spec 값(일일 계약 4320) 상속,
    값이 있으면 override (range rerun의 일수 비례 재설정)."""
    spec = copy.deepcopy(cronjob_obj["spec"]["jobTemplate"]["spec"])
    spec["template"]["spec"]["containers"][0]["command"] = list(command)
    if active_deadline_s is not None:
        spec["activeDeadlineSeconds"] = active_deadline_s
    return {"apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": job_name}, "spec": spec}


def build_mart_command(context, namespace, from_d, to_d):
    # §8.3 v1.4: collectors rerun의 --from/--to를 동일 값 그대로 전파 (유일한 접점 인자)
    return (f"python3 {MART_RERUN} --context {context} --namespace {namespace} "
            f"--from {from_d} --to {to_d}")


def wait_job(context, namespace, job_name, timeout_s):
    """Job 완료 폴링 + 파드별 로그 스트리밍. 성공 True / 실패 False.

    backoffLimit=1 재시도 파드까지 각각 스트리밍한다 — 마커 라인(§5.6)이 운영
    기록이므로 가공 없이 그대로 출력."""
    deadline = time.monotonic() + timeout_s
    seen_pods = set()
    while time.monotonic() < deadline:
        res = kubectl(context, ["get", "job", job_name, "-n", namespace, "-o", "json"],
                      capture=True)
        status = json.loads(res.stdout).get("status", {})
        conds = {c["type"]: c["status"] for c in status.get("conditions", [])}
        pods = kubectl(context, ["get", "pods", "-l", f"job-name={job_name}", "-n", namespace,
                                 "-o", "jsonpath={.items[*].metadata.name}"],
                       capture=True).stdout.split()
        for pod in pods:
            if pod not in seen_pods:
                seen_pods.add(pod)
                subprocess.run(["kubectl", f"--context={context}", "--insecure-skip-tls-verify",
                                "logs", "-f", f"pod/{pod}", "-n", namespace,
                                "--pod-running-timeout=5m"], check=False)
        if conds.get("Complete") == "True":
            return True
        if conds.get("Failed") == "True":
            print(f"[ERROR] job {job_name} failed — 전체 로그: kubectl --context={context} "
                  f"logs job/{job_name} -n {namespace} --prefix --tail=-1", file=sys.stderr)
            return False
        time.sleep(POLL_S)
    print(f"[ERROR] job {job_name} timeout ({timeout_s}s)", file=sys.stderr)
    return False


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", default="monitoring")
    p.add_argument("--from", dest="from_d", default=None)
    p.add_argument("--to", dest="to_d", default=None)
    p.add_argument("--service", default=None)
    p.add_argument("--push-vm", action="store_true")
    p.add_argument("--chain-mart", action="store_true")
    args = p.parse_args(argv)

    if bool(args.from_d) != bool(args.to_d):
        p.exit(2, "--from/--to는 쌍으로 지정 (YYYY-MM-DD, KST, inclusive)\n")
    if args.service and not args.from_d:
        p.exit(2, "--service는 --from/--to와 함께만 (재수집 용도, §5.1)\n")
    if args.push_vm and not args.from_d:
        p.exit(2, "--push-vm은 --from/--to와 함께만 (§5.5)\n")
    n_days = 1
    if args.from_d:
        try:
            d0, d1 = dt.date.fromisoformat(args.from_d), dt.date.fromisoformat(args.to_d)
        except ValueError:
            p.exit(2, "--from/--to는 YYYY-MM-DD 형식\n")
        if d0 > d1:
            p.exit(2, f"--from({d0}) > --to({d1})\n")
        n_days = (d1 - d0).days + 1

    epoch = int(time.time())
    if args.from_d:
        job_name = f"{CRONJOB}-rerun-{epoch}"
        res = kubectl(args.context, ["get", "cronjob", CRONJOB, "-n", args.namespace,
                                     "-o", "json"], capture=True)
        job = build_job_spec(json.loads(res.stdout), job_name,
                             build_collect_command(args.from_d, args.to_d,
                                                   args.service, args.push_vm),
                             active_deadline_s=range_deadline_s(n_days))
        kubectl(args.context, ["apply", "-n", args.namespace, "-f", "-"],
                input_data=json.dumps(job))
        timeout = range_deadline_s(n_days) + 600      # 서버 데드라인 + 폴링 마진
    else:
        job_name = f"{CRONJOB}-manual-{epoch}"
        kubectl(args.context, ["create", "job", f"--from=cronjob/{CRONJOB}",
                               job_name, "-n", args.namespace])
        timeout = TIMEOUT_SINGLE_S

    ok = wait_job(args.context, args.namespace, job_name, timeout)
    if not ok:
        return 1

    # §3/§8.3: collectors rerun 후 동일 날짜 mart rerun은 의무 — 모드 무관 항상 안내.
    # 수동 트리거의 대상 날짜 = 실행 시점 기준 어제 (KST, main.py 계약)
    if args.from_d:
        mart_from, mart_to = args.from_d, args.to_d
    else:
        kst_now = dt.datetime.now(dt.timezone(dt.timedelta(hours=9)))
        mart_from = mart_to = (kst_now.date() - dt.timedelta(days=1)).isoformat()
    mart_cmd = build_mart_command(args.context, args.namespace, mart_from, mart_to)
    print("")
    print("[NEXT] collectors rerun 후 동일 날짜 mart rerun은 의무입니다 (§3/§8.3):")
    print(f"  {mart_cmd}")
    if args.chain_mart:
        repo_root = pathlib.Path(__file__).resolve().parents[3]
        mart_path = repo_root / MART_RERUN
        if not mart_path.exists():
            print(f"[ERROR] --chain-mart: {MART_RERUN} 가 아직 없습니다 (Plan 3 전) — "
                  f"mart 구현 후 위 명령을 실행하세요.", file=sys.stderr)
            return 1
        # 절대경로 + 리스트 인자 (cwd 무관, 공백 인자 안전)
        return subprocess.call([sys.executable, str(mart_path),
                                "--context", args.context, "--namespace", args.namespace,
                                "--from", mart_from, "--to", mart_to])
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: 테스트 통과 확인** — `python3 -m pytest tests/test_rerun.py -q` 전부 PASS. 이어서 전체: `python3 -m pytest tests/ --ignore=tests/e2e -q`

- [ ] **Step 5: 계약 재확인 체크리스트** (구현자 셀프 체크 — 코드와 대조)
  - [ ] `--from/--to` inclusive가 main.py에 그대로 전달되는가 (변환 없음)
  - [ ] mart 명령의 날짜가 **동일 값 그대로**인가 (§8.3 v1.4) — 수동 모드는 어제(KST)
  - [ ] mart rerun 명령을 성공 시 **항상 출력**하는가 (모드·--chain-mart 여부 무관)
  - [ ] activeDeadlineSeconds: 일일 CronJob은 4320 유지, range rerun Job은 일수 비례(캡 TIMEOUT_RANGE_S)로 override되는가
  - [ ] 재시도 파드(backoffLimit=1)의 로그도 스트리밍되는가
  - [ ] rerun.py가 마커 라인을 가공하지 않고 그대로 스트리밍하는가 (§5.6)

- [ ] **Step 6: Commit** — `feat(collectors): tools/rerun.py — manual/range rerun + --chain-mart (Plan 2b T5)`

---

### Task 6: docs/operations/rerun.md + 모듈 README 배포 섹션

**Files:**
- Create: `docs/operations/rerun.md`
- Modify: `collectors/token-usage/README.md` (## 배포, ## 재수행 섹션 추가)

**Interfaces:**
- Consumes: Task 1~5의 인터페이스 전부 (커맨드 라인 문자열은 각 스크립트의 usage와 일치해야 함 — 복사 후 대조).

- [ ] **Step 1: docs/operations/rerun.md 작성** (동료 레포 docs/operations/rerun.md 관례 — 표 + 모듈별 절차)

```markdown
# 재수행 (rerun) 절차

배치 실패·정정(§8.4)·과거 구간 회수의 표준 절차. 모듈별 tools/rerun.py를 사용한다.

| 모듈 | CronJob | namespace | 모드 |
|---|---|---|---|
| collectors/token-usage | token-usage-collector | monitoring | 1회 수동 트리거 / 날짜 범위(--from/--to, inclusive) |
| mart/token-usage (Plan 3) | (미정) | (미정) | 날짜 범위 |

## collectors/token-usage

    # 1회 수동 트리거 (실행 시점 기준 어제 KST)
    python3 collectors/token-usage/tools/rerun.py --context homelab

    # 날짜 범위 재수집 — 둘 다 inclusive (동료 metric rerun의 to-제외와 다름에 주의)
    python3 collectors/token-usage/tools/rerun.py --context homelab \
        --from 2026-07-01 --to 2026-07-03 [--service "<정본 서비스명>"] [--chain-mart]

- 적재는 항상 delete-then-insert(§5.1)이므로 별도 --purge가 없다. 같은 (date, service)의
  재실행은 전체 교체이며, 교체 직전 세대는 fact.collect_audit_1d에 보존된다(§8.4-2).
- **collectors rerun 후 동일 날짜의 mart rerun은 의무다(§3/§8.3).** rerun.py가 완료 시
  mart rerun 명령을 출력하며, --chain-mart로 직접 트리거할 수 있다. 날짜(--from/--to)는
  동일 값 그대로 전파된다(v1.4 체이닝 계약).
- RESTATEMENT 마커(§8.4-1, D-2~D-7 summary 재조회에서 발화 — **재조회 자체는 후속 백로그,
  아직 미구현**)를 보면 운영자가 해당 (date, service)의 rerun 여부를 판단한다 —
  이 문서의 날짜 범위 재수집 + --service 조합을 사용.

## VM push와 rerun (§5.5)

rerun 경로는 VM push를 기본 생략한다 — VictoriaMetrics는 동일 timestamp 재push 시
dedup이 큰 값을 유지해 **하향 정정이 반영되지 않는다**. 필요 시:

1. 상향 정정만 확실하면 `--push-vm` 옵트인으로 재push.
2. 하향 정정을 VM에 반영해야 하면 해당 시계열을 delete_series API로 삭제 후 재push:

       curl -s "http://<vm>/api/v1/admin/tsdb/delete_series?match[]={__name__=~'token_usage_.*',service='<서비스>'}&start=<D1>&end=<D2>"
       python3 collectors/token-usage/tools/rerun.py ... --push-vm

   (delete_series는 관리 API — 사용 전 VM 운영자와 협의. 삭제는 되돌릴 수 없다.)
```

- [ ] **Step 2: 모듈 README에 배포 섹션 추가** — `collectors/token-usage/README.md` (동료 관례: 이미지 빌드&푸시 → Secret+CronJob 배포 → 수동 실행):

```markdown
## 배포 (§7.2)

    # 1. 이미지 빌드 & 푸시 (태그 기본 = git short SHA)
    ./collectors/token-usage/build.sh stage
    ./collectors/token-usage/build.sh --registry <harbor> company

    # 2. Secret + endpoints ConfigMap + 테이블 DDL + CronJob (대화형)
    ./collectors/token-usage/install.sh stage
    ./collectors/token-usage/install.sh --registry <harbor> --context <ctx> company
    # accounts.sql(CREATE USER/GRANT)은 admin 수동 실행 — install.sh는 안내만 출력

    # 3. 수동 실행 (테스트)
    python3 collectors/token-usage/tools/rerun.py --context homelab

- CronJob: 매일 02:00 KST (mart 04:00이 완료를 전제 — §3 cron 오프셋), Forbid,
  activeDeadlineSeconds 4320(§5.2 산식), resources 256Mi/1Gi(§7.2).
- endpoints: 레포에는 stage(mock)용만. 사내 목록은 endpoints.company.yaml(gitignored)을
  install.sh가 ConfigMap `token-usage-endpoints`로 주입.

## 재수행

`docs/operations/rerun.md` 참조 — collectors rerun 후 동일 날짜 mart rerun 의무(§8.3).
```

- [ ] **Step 3: 문서-코드 대조** — README/rerun.md의 모든 커맨드 문자열을 실제 스크립트 usage와 대조(옵션명·기본값). 불일치는 문서가 아니라 이 단계에서 잡는다.

- [ ] **Step 4: Commit** — `docs: rerun 운영 절차 + collector 배포 섹션 (Plan 2b T6)`
