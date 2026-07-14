#!/usr/bin/env bash
# token-usage collector 설치 (스펙 §7.2)
#
# 사용법:
#   ./collectors/token-usage/install.sh [--registry <registry>] [--tag <tag>] \
#     [--context <kube-context>] [--namespace <ns>] [--endpoints <file>] <stage|company|company-verify>
#
#   stage:           context 기본 homelab, endpoints 기본 endpoints.yaml (mock)
#   company:         --context/--registry 필수, endpoints 기본 endpoints.company.yaml (gitignored)
#   company-verify:  company 2단계 검증 전략의 1단계(격리) — --context/--registry 필수(company와
#                     동일 요건). Secret/ConfigMap/CronJob 이름 -verify 접미, DDL 적용 대상은
#                     ddl/company-verify/(생성기 tools/gen_verify_ddl.py 출력 — 격리 DB 3종),
#                     VM_PUSH_URL 주입 생략(1단계 VM 오염 방지). 절차: docs/operations/company-verify.md
#
# 수행 순서:
#   [1/6] registry-pull-secret 멱등 생성 (대화형)
#   [2/6] token-usage-ch-secret[-verify] 멱등 생성 (대화형 — envFrom으로 컨테이너 env가 됨)
#   [3/6] token-usage-endpoints[-verify] ConfigMap 생성/갱신
#   [4/6] 테이블 DDL 적용 (chi-* 파드 자동 탐색 — accounts.sql은 admin 수동, §7.2)
#   [5/6] CronJob 배포 (kustomize overlay)
#   [6/6] 이미지 주소/CH_HOST/VM_PUSH_URL(company-verify는 생략) 주입 + 수동 테스트 커맨드 안내
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

IMAGE_NAME="token-usage-collector"
CRONJOB_NAME="token-usage-collector"
SECRET_NAME="token-usage-ch-secret"
PULL_SECRET_NAME="registry-pull-secret"
CONFIGMAP_NAME="token-usage-endpoints"
CH_NAMESPACE="clickhouse"

REGISTRY=""; TAG=""; KUBE_CONTEXT=""; NAMESPACE="monitoring"; ENDPOINTS_SRC=""; ENV=""

usage() { sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry)  REGISTRY="$2"; shift 2 ;;
    --tag)       TAG="$2"; shift 2 ;;
    --context)   KUBE_CONTEXT="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --endpoints) ENDPOINTS_SRC="$2"; shift 2 ;;
    stage|company|company-verify) ENV="$1"; shift ;;
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
  company-verify)
    # 1단계 격리 검증 — company와 동일 요건(--context/--registry 필수). 수집 대상은 동일한
    # 실 서비스 API(endpoints.company.yaml) — 병행 금지·교체 전환 원칙(company-verify.md).
    [[ -n "${KUBE_CONTEXT}" ]] || { echo "[ERROR] company-verify 환경에서는 --context 옵션이 필수입니다."; usage; }
    [[ -n "${REGISTRY}" ]]     || { echo "[ERROR] company-verify 환경에서는 --registry 옵션이 필수입니다."; usage; }
    ENDPOINTS_SRC="${ENDPOINTS_SRC:-${HERE}/endpoints.company.yaml}"
    SECRET_NAME="${SECRET_NAME}-verify"
    CONFIGMAP_NAME="${CONFIGMAP_NAME}-verify"
    CRONJOB_NAME="${CRONJOB_NAME}-verify"
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
  ch_user_default="mart"
  [[ "${ENV}" == "company-verify" ]] && ch_user_default="token_verify"
  read -r -p "  CH_USER [${ch_user_default}]: " ch_user
  ch_user="${ch_user:-${ch_user_default}}"
  read -r -s -p "  CH_PASSWORD: " ch_pass; echo ""
  read -r -p "  COLLECTOR_HTTPS_PROXY ('none'=직접 연결, enter=시스템 상속, 값=프록시 URL): " http_proxy_v
  read -r -p "  사내 CA 번들 파일 경로 (없으면 enter): " ca_bundle_v
  # CH_DB_FACT/CH_DB_DIM — 격리 검증(company-verify) 전용 (§company 2단계 검증 전략,
  # docs/operations/company-verify.md). company-verify는 tools/gen_verify_ddl.py 기본안
  # 값을 자동 포함(프롬프트 없음) — stage/company는 평시 enter(=앱 기본값 fact/gpu_data).
  if [[ "${ENV}" == "company-verify" ]]; then
    ch_db_fact_v="token_verify_fact"
    ch_db_dim_v="token_verify_dim"
  else
    read -r -p "  CH_DB_FACT (격리 검증(company-verify) 전용 — 평시 enter): " ch_db_fact_v
    read -r -p "  CH_DB_DIM (격리 검증(company-verify) 전용 — 평시 enter): " ch_db_dim_v
  fi
  args=(--from-literal="CH_USER=${ch_user}"
        --from-literal="CH_PASSWORD=${ch_pass}"
        --from-literal="CH_PORT=8123"
        --from-literal="CH_CLUSTER=gpu-monitoring")
  [[ -n "${ch_db_fact_v}" ]] && args+=(--from-literal="CH_DB_FACT=${ch_db_fact_v}")
  [[ -n "${ch_db_dim_v}" ]] && args+=(--from-literal="CH_DB_DIM=${ch_db_dim_v}")
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
DDL_DIR="ddl/company"
[[ "${ENV}" == "company-verify" ]] && DDL_DIR="ddl/company-verify"
if [[ "${ENV}" == "company-verify" ]]; then
  echo "  (격리 검증(1단계) — ${DDL_DIR}/의 테이블 DDL만 적용. DB 3종·전용 계정은 admin이"
  echo "   ${DDL_DIR}/accounts.sql로 먼저 생성해야 아래 테이블 DDL이 성공합니다: python3"
  echo "   tools/gen_verify_ddl.py로 재생성 가능, docs/operations/company-verify.md 참조)"
else
  echo "  (fact DB가 아직 없으면 admin이 accounts.sql을 먼저 실행해야 테이블 DDL이 성공합니다)"
  echo "  (gpu_data DB는 동료 소유 — 부재 시 소유자와 협의)"
fi
apply_sql "${HERE}/${DDL_DIR}/raw_token_usage.sql"
apply_sql "${HERE}/${DDL_DIR}/dim_token_service.sql"
echo "  (accounts.sql은 적용하지 않았습니다 — CREATE DATABASE/CREATE USER/GRANT는 admin 수동 실행, §7.2)"
echo "  (${ENV}: 클러스터 소유자와 협의 후 ${DDL_DIR}/accounts.sql의 CHANGE_ME_* 치환 실행)"

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

if [[ "${ENV}" == "company-verify" ]]; then
  echo "  [SKIP] VM_PUSH_URL 주입 생략 — 1단계 격리 검증은 VictoriaMetrics를 오염시키지 않는다"
  echo "         (docs/operations/company-verify.md — VM push 1단계 비활성)"
  echo ""
  echo "[OK] 설치 완료. 수동 테스트:"
  echo "  ${KUBECTL} create job --from=cronjob/${CRONJOB_NAME} ${CRONJOB_NAME}-manual-\$(date +%s) -n ${NAMESPACE}"
  echo "  (또는: python3 ${HERE}/tools/rerun.py --context ${KUBE_CONTEXT} --namespace ${NAMESPACE} --cronjob ${CRONJOB_NAME})"
  exit 0
fi

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
