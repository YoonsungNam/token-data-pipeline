#!/usr/bin/env bash
# token-mart 배치 설치 (스펙 §7.2 — collectors install.sh 델타)
#
# 사용법:
#   ./mart/token-usage/install.sh [--registry <registry>] [--tag <tag>] \
#     [--context <kube-context>] [--namespace <ns>] <stage|company>
#
#   stage:   context 기본 homelab
#   company: --context/--registry 필수
#
# 수행 순서 (mart는 endpoints ConfigMap 불요 — dim_token_service가 게이트 기준, §7.2):
#   [1/5] registry-pull-secret 멱등 생성 (대화형)
#   [2/5] token-mart-ch-secret 멱등 생성 (대화형 — envFrom으로 컨테이너 env가 됨)
#   [3/5] 테이블 DDL 적용 (chi-* 파드 자동 탐색 — accounts.sql은 admin 수동, §7.2)
#   [4/5] CronJob 배포 (kustomize overlay)
#   [5/5] 이미지 주소/CH_HOST 주입 + 수동 테스트 커맨드 안내
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

IMAGE_NAME="token-mart"
CRONJOB_NAME="token-mart-daily"
SECRET_NAME="token-mart-ch-secret"
PULL_SECRET_NAME="registry-pull-secret"
CH_NAMESPACE="clickhouse"

REGISTRY=""; TAG=""; KUBE_CONTEXT=""; NAMESPACE="monitoring"; ENV=""

usage() { sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry)  REGISTRY="$2"; shift 2 ;;
    --tag)       TAG="$2"; shift 2 ;;
    --context)   KUBE_CONTEXT="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    stage|company) ENV="$1"; shift ;;
    *) echo "[ERROR] unknown arg: $1"; usage ;;
  esac
done
[[ -n "${ENV}" ]] || usage

case "${ENV}" in
  stage)
    KUBE_CONTEXT="${KUBE_CONTEXT:-homelab}"
    REGISTRY="${REGISTRY:-ghcr.io/yoonsungnam}"
    ;;
  company)
    [[ -n "${KUBE_CONTEXT}" ]] || { echo "[ERROR] company 환경에서는 --context 옵션이 필수입니다."; usage; }
    [[ -n "${REGISTRY}" ]]     || { echo "[ERROR] company 환경에서는 --registry 옵션이 필수입니다."; usage; }
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

echo "=== token-mart install (${ENV}) ==="
echo "context=${KUBE_CONTEXT} namespace=${NAMESPACE} image=${REGISTRY}/${IMAGE_NAME}:${TAG}"

# ── [1/5] registry pull secret ────────────────────────────────────────────────
echo ""
echo "[1/5] image pull secret '${PULL_SECRET_NAME}'"
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

# ── [2/5] app secret (envFrom — 키 이름이 곧 컨테이너 env 이름, §5.7/§7.2) ────
# collectors와 델타: CH_USER 기본값은 계정 공유 결정(2026-07-14)으로 동일(mart) —
# proxy/CA 프롬프트 없음(mart는 아웃바운드 HTTP 없음 — YAGNI), EXPECTED_LATE_SERVICES
# 선택 입력(enter=스킵 — 키 자체를 Secret에 넣지 않아 컨테이너 env 미설정 → app 기본값
# '' 사용). INSERT_QUORUM은 company에서만 'auto' 자동 포함 — Global Constraints
# 레플리카 지연 게이트(§9-19), 대화형 프롬프트 아님.
echo ""
echo "[2/5] app secret '${SECRET_NAME}'"
if ${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  read -r -p "  이미 존재합니다. 갱신하시겠습니까? [y/N] " ans
else
  ans="y"
fi
if [[ "${ans}" == "y" || "${ans}" == "Y" ]]; then
  read -r -p "  CH_USER [mart]: " ch_user
  ch_user="${ch_user:-mart}"
  read -r -s -p "  CH_PASSWORD: " ch_pass; echo ""
  read -r -p "  EXPECTED_LATE_SERVICES (콤마 구분 서비스명, 없으면 enter): " expected_late_v
  args=(--from-literal="CH_USER=${ch_user}"
        --from-literal="CH_PASSWORD=${ch_pass}"
        --from-literal="CH_PORT=8123"
        --from-literal="CH_CLUSTER=gpu-monitoring")
  if [[ -n "${expected_late_v}" ]]; then
    args+=(--from-literal="EXPECTED_LATE_SERVICES=${expected_late_v}")
  fi
  if [[ "${ENV}" == "company" ]]; then
    # company 2s×2r 전제 — 레플리카 지연 게이트(§9-19). stage(1s×1r)는 미포함.
    args+=(--from-literal="INSERT_QUORUM=auto")
  fi
  ${KUBECTL} create secret generic "${SECRET_NAME}" "${args[@]}" -n "${NAMESPACE}" \
    --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -
fi

# ── [3/5] 테이블 DDL (chi-* 자동 탐색, kubectl cp + clickhouse-client) ────────
echo ""
echo "[3/5] table DDL"
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
echo "  mart/gpu_data DB·GRANT 설정은 admin: ddl/company/accounts.sql (계정은 공유 mart —"
echo "  계정 공유 합의 2026-07-14, CREATE USER는 이 레포 소관 아님. insert_deduplicate=0은"
echo "  app/ch.py 클라이언트 설정으로만 적용)"
echo "  (위 GRANT가 없으면 아래 테이블 DDL은 만들어져도 mart(공유 계정)의 INSERT/DELETE가 실패합니다)"
apply_sql "${HERE}/ddl/company/mart_tables.sql"
apply_sql "${HERE}/ddl/company/view_token_usage.sql"
echo "  (accounts.sql은 적용하지 않았습니다 — GRANT는 admin 수동 실행, §7.2. 계정 생성·비밀번호는"
echo "  동료 소유이므로 이 레포에서 관리하지 않습니다)"

# ── [4/5] CronJob 배포 ────────────────────────────────────────────────────────
echo ""
echo "[4/5] CronJob apply (overlay: ${ENV})"
${KUBECTL} apply -k "${HERE}/k8s/overlays/${ENV}" -n "${NAMESPACE}"

# ── [5/5] 이미지/CH_HOST 주입 (mart는 VM push 없음 — VM_PUSH_URL 주입 대상 아님) ─
echo ""
echo "[5/5] set image / set env"
${KUBECTL} set image "cronjob/${CRONJOB_NAME}" \
  "${IMAGE_NAME}=${REGISTRY}/${IMAGE_NAME}:${TAG}" -n "${NAMESPACE}"

# CH_HOST: [3/5]에서 찾은 chi-* 파드명에서 말미 ordinal을 잘라 헤드리스 서비스명 유도
# (예: chi-gpu-monitoring-gpu-monitoring-0-0-0 → chi-gpu-monitoring-gpu-monitoring-0-0)
ch_host="${ch_pod%-*}.${CH_NAMESPACE}.svc"
${KUBECTL} set env "cronjob/${CRONJOB_NAME}" "CH_HOST=${ch_host}" -n "${NAMESPACE}"
echo "  CH_HOST=${ch_host}"

echo ""
echo "[OK] 설치 완료. 수동 테스트:"
echo "  ${KUBECTL} create job --from=cronjob/${CRONJOB_NAME} ${CRONJOB_NAME}-manual-\$(date +%s) -n ${NAMESPACE}"
echo "  (또는: python3 ${HERE}/tools/rerun.py --context ${KUBE_CONTEXT} --namespace ${NAMESPACE})"
