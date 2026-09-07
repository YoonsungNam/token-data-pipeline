#!/usr/bin/env bash
# token-metrics collector 설치 (설계 §5.6 배포 · §7.5 "새 코드만 새로 배포")
#
# 사용법:
#   ./collectors/token-metrics/install.sh [--registry <registry>] [--tag <tag>] \
#     [--context <kube-context>] [--namespace <ns>] [--endpoints <file>] <stage|company|company-verify>
#
#   stage:           context 기본 homelab, registry 기본 ghcr.io/yoonsungnam, endpoints 기본 endpoints.yaml (mock)
#   company:         --context/--registry 필수, endpoints 기본 endpoints-metrics.company.yaml (gitignored)
#   company-verify:  격리 검증(선택 — 설계 §7.5). --context/--registry 필수(company와 동일 요건).
#                     Secret/ConfigMap/CronJob 이름 -verify 접미, DDL은 ddl/company-verify/, CH_USER 기본
#                     token_verify, CH_DB_FACT/CH_DB_DIM 프롬프트(기본 token_verify_fact/token_verify_dim)
#   예: ./collectors/token-metrics/install.sh company --context <ctx> --registry harbor.example.internal/gpu-monitoring --tag <sha7>
#   기존 토큰 수집기 모듈의 Secret/ConfigMap/CronJob은 건드리지 않는다 — registry-pull-secret만 공유(없을 때만 생성)
#
# 수행 순서:
#   [1/7] registry-pull-secret — 없을 때만 생성 (네임스페이스 공유 Secret; 있으면 갱신하지 않음, §7.5)
#   [2/7] token-metrics-ch-secret[-verify] 멱등 생성 (대화형 — envFrom으로 컨테이너 env가 됨)
#   [3/7] token-metrics-endpoints[-verify] ConfigMap 생성/갱신
#   [4/7] 프리플라이트: chi-* 파드 탐색 → 앱 계정(CH_USER)으로 fact/gpu_data DB 존재 + 토큰 레지스트리 dim_token_service_dist SELECT (GRANT 검증, §5.6)
#   [5/7] 테이블 DDL 적용: raw_token_metrics.sql + dim_token_metrics_service.sql (accounts.sql은 admin 수동 — §4.0)
#   [6/7] CronJob 배포 (kustomize overlay)
#   [7/7] 이미지 주소/CH_HOST 주입 + 수동 테스트 커맨드 안내 (VM push 없음 — §5.2)
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

IMAGE_NAME="token-metrics-collector"
CRONJOB_NAME="token-metrics-collector"
SECRET_NAME="token-metrics-ch-secret"
PULL_SECRET_NAME="registry-pull-secret"
CONFIGMAP_NAME="token-metrics-endpoints"
CA_CONFIGMAP_NAME="token-metrics-ca-bundle"
CH_NAMESPACE="clickhouse"

REGISTRY=""; TAG=""; KUBE_CONTEXT=""; NAMESPACE="monitoring"; ENDPOINTS_SRC=""; ENV=""
# 프리플라이트·DDL 안내가 쓰는 DB명 — 앱 기본값(fact/gpu_data). company-verify는 token_verify_*,
# Secret에 CH_DB_FACT/CH_DB_DIM이 있으면 [2/7]에서 그 값으로 덮어쓴다.
DB_FACT="fact"; DB_DIM="gpu_data"
DDL_DIR="ddl/company"

usage() { sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }

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
    # stage 홈랩 CHI는 ZK 없음 — Replicated/ON CLUSTER 불가, 생성 변형 사용 (tools/gen_stage_ddl.py)
    DDL_DIR="ddl/stage"
    ;;
  company)
    [[ -n "${KUBE_CONTEXT}" ]] || { echo "[ERROR] company 환경에서는 --context 옵션이 필수입니다."; usage; }
    [[ -n "${REGISTRY}" ]]     || { echo "[ERROR] company 환경에서는 --registry 옵션이 필수입니다."; usage; }
    ENDPOINTS_SRC="${ENDPOINTS_SRC:-${HERE}/endpoints-metrics.company.yaml}"
    ;;
  company-verify)
    # 격리 검증 — company와 동일 요건(--context/--registry 필수). 수집 대상은 동일한 실 서비스 API
    # (endpoints-metrics.company.yaml). DB는 token_verify_fact/token_verify_dim (tools/gen_verify_ddl.py 규칙).
    [[ -n "${KUBE_CONTEXT}" ]] || { echo "[ERROR] company-verify 환경에서는 --context 옵션이 필수입니다."; usage; }
    [[ -n "${REGISTRY}" ]]     || { echo "[ERROR] company-verify 환경에서는 --registry 옵션이 필수입니다."; usage; }
    ENDPOINTS_SRC="${ENDPOINTS_SRC:-${HERE}/endpoints-metrics.company.yaml}"
    SECRET_NAME="${SECRET_NAME}-verify"
    CONFIGMAP_NAME="${CONFIGMAP_NAME}-verify"
    CRONJOB_NAME="${CRONJOB_NAME}-verify"
    DB_FACT="token_verify_fact"; DB_DIM="token_verify_dim"
    DDL_DIR="ddl/company-verify"
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

# kube API 서버 호스트를 NO_PROXY에 자동 추가 (사내 프록시 환경에서 kubectl 통신 보존 — 기존 모듈과 동일)
api_server="$(${KUBECTL} config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || true)"
if [[ -n "${api_server}" ]]; then
  api_host="$(printf '%s' "${api_server}" | sed -E 's#^https?://##; s#:[0-9]+$##')"
  export NO_PROXY="${NO_PROXY:+${NO_PROXY},}${api_host}"
  export no_proxy="${no_proxy:+${no_proxy},}${api_host}"
fi

echo "=== token-metrics collector install (${ENV}) ==="
echo "context=${KUBE_CONTEXT} namespace=${NAMESPACE} image=${REGISTRY}/${IMAGE_NAME}:${TAG}"
echo "endpoints=${ENDPOINTS_SRC} ddl=${DDL_DIR} db=${DB_FACT}/${DB_DIM}"

# ── [1/7] registry pull secret (네임스페이스 공유 — 없을 때만 생성, §7.5) ───────
echo ""
echo "[1/7] image pull secret '${PULL_SECRET_NAME}'"
if ${KUBECTL} get secret "${PULL_SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  echo "  이미 존재합니다 — 네임스페이스 공유 Secret이므로 갱신하지 않습니다"
else
  reg_server=""
  read -r -p "  registry server [${REGISTRY%%/*}]: " reg_server
  reg_server="${reg_server:-${REGISTRY%%/*}}"
  reg_user=""
  read -r -p "  registry username: " reg_user
  reg_pass=""
  read -r -s -p "  registry password/token: " reg_pass; echo ""
  ${KUBECTL} create secret docker-registry "${PULL_SECRET_NAME}" \
    --docker-server="${reg_server}" --docker-username="${reg_user}" \
    --docker-password="${reg_pass}" -n "${NAMESPACE}" \
    --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -
fi

# ── [2/7] app secret (envFrom — 키 이름이 곧 컨테이너 env 이름) ─────────────────
echo ""
echo "[2/7] app secret '${SECRET_NAME}'"
ans=""
if ${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  read -r -p "  이미 존재합니다. 갱신하시겠습니까? [y/N] " ans
else
  ans="y"
fi
if [[ "${ans}" == "y" || "${ans}" == "Y" ]]; then
  ch_user_default="mart"
  [[ "${ENV}" == "company-verify" ]] && ch_user_default="token_verify"
  ch_user=""
  read -r -p "  CH_USER [${ch_user_default}]: " ch_user
  ch_user="${ch_user:-${ch_user_default}}"
  ch_pass=""
  read -r -s -p "  CH_PASSWORD: " ch_pass; echo ""
  http_proxy_v=""
  read -r -p "  COLLECTOR_HTTPS_PROXY ('none'=직접 연결, enter=시스템 상속, 값=프록시 URL): " http_proxy_v
  ca_bundle_v=""
  read -r -p "  사내 CA 번들 파일 경로 (없으면 enter): " ca_bundle_v
  # CH_DB_FACT/CH_DB_DIM — 격리 검증(company-verify) 전용 프롬프트(기본 token_verify_fact/token_verify_dim).
  # stage/company는 키를 만들지 않는다(앱 기본값 fact/gpu_data — app/writer.py DB_FACT/DB_DIM).
  ch_db_fact_v=""; ch_db_dim_v=""
  if [[ "${ENV}" == "company-verify" ]]; then
    read -r -p "  CH_DB_FACT [${DB_FACT}]: " ch_db_fact_v
    read -r -p "  CH_DB_DIM [${DB_DIM}]: " ch_db_dim_v
    ch_db_fact_v="${ch_db_fact_v:-${DB_FACT}}"
    ch_db_dim_v="${ch_db_dim_v:-${DB_DIM}}"
    DB_FACT="${ch_db_fact_v}"; DB_DIM="${ch_db_dim_v}"
  fi
  # stage 홈랩 CHI는 ZK 없음 — ON CLUSTER 불가하므로 단일노드 모드(빈 값).
  # company/company-verify는 클러스터명 주입 (CH_CLUSTER와 DDL의 ON CLUSTER 리터럴 일치 전제)
  CH_CLUSTER_VALUE="gpu-monitoring"
  [[ "${ENV}" == "stage" ]] && CH_CLUSTER_VALUE=""
  args=(--from-literal="CH_USER=${ch_user}"
        --from-literal="CH_PASSWORD=${ch_pass}"
        --from-literal="CH_PORT=8123"
        --from-literal="CH_CLUSTER=${CH_CLUSTER_VALUE}")
  [[ -n "${ch_db_fact_v}" ]] && args+=(--from-literal="CH_DB_FACT=${ch_db_fact_v}")
  [[ -n "${ch_db_dim_v}" ]] && args+=(--from-literal="CH_DB_DIM=${ch_db_dim_v}")
  # 프록시 3분기: 키 미설정=시스템 상속 / 빈 값=직접 연결 / 값=전용 프록시 (app/config.py load_config).
  # read로는 미입력과 빈 값이 구분되지 않으므로 'none' 센티널로 빈 값을 받는다.
  case "${http_proxy_v}" in
    "")   ;;                                                         # enter → 키 미설정 = 상속
    none) args+=(--from-literal="COLLECTOR_HTTPS_PROXY=") ;;         # 빈 값 = 직접 연결
    *)    args+=(--from-literal="COLLECTOR_HTTPS_PROXY=${http_proxy_v}") ;;
  esac
  if [[ -n "${ca_bundle_v}" ]]; then
    # CA '파일'을 파드에 전달해야 한다 — 경로 문자열만 Secret에 넣으면 컨테이너에서 열 수 없음.
    # base cronjob.yaml의 optional ConfigMap 볼륨(/etc/token-metrics-ca)에 실어 보낸다.
    [[ -f "${ca_bundle_v}" ]] || { echo "[ERROR] CA 파일이 없습니다: ${ca_bundle_v}"; exit 1; }
    ${KUBECTL} create configmap "${CA_CONFIGMAP_NAME}" \
      --from-file="ca-bundle.pem=${ca_bundle_v}" -n "${NAMESPACE}" \
      --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -
    args+=(--from-literal="COLLECTOR_API_CA_BUNDLE=/etc/token-metrics-ca/ca-bundle.pem")
  fi
  ${KUBECTL} create secret generic "${SECRET_NAME}" "${args[@]}" -n "${NAMESPACE}" \
    --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -
else
  # 갱신하지 않으면 기존 Secret의 CH_DB_FACT/CH_DB_DIM(있을 때만)을 프리플라이트 대상 DB로 쓰고,
  # CH_USER/CH_PASSWORD 는 프리플라이트가 앱 계정으로 접속(GRANT 검증)하는 데 쓴다
  existing_fact="$(${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.data.CH_DB_FACT}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  existing_dim="$(${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.data.CH_DB_DIM}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  [[ -n "${existing_fact}" ]] && DB_FACT="${existing_fact}"
  [[ -n "${existing_dim}" ]] && DB_DIM="${existing_dim}"
  ch_user="$(${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.data.CH_USER}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  ch_pass="$(${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.data.CH_PASSWORD}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  if [[ -z "${ch_user}" ]]; then
    echo "[ERROR] 기존 Secret '${SECRET_NAME}'에 CH_USER가 없습니다 — 갱신(y)으로 다시 만드세요."
    exit 1
  fi
  if [[ -z "${ch_pass}" ]]; then
    echo "[ERROR] 기존 Secret '${SECRET_NAME}'에 CH_PASSWORD가 없습니다 — 갱신(y)으로 다시 만드세요."
    exit 1
  fi
fi

# ── [3/7] endpoints ConfigMap (endpoints.yaml 분리 원칙 — 이미지에 굽지 않음) ────
echo ""
echo "[3/7] endpoints ConfigMap '${CONFIGMAP_NAME}'"
if [[ ! -f "${ENDPOINTS_SRC}" ]]; then
  echo "[ERROR] endpoints 파일이 없습니다: ${ENDPOINTS_SRC}"
  echo "        company는 사내 URL 목록을 endpoints-metrics.company.yaml(gitignored)로 준비하세요 (설계 §4.3 키)."
  exit 1
fi
${KUBECTL} create configmap "${CONFIGMAP_NAME}" \
  --from-file="endpoints.yaml=${ENDPOINTS_SRC}" -n "${NAMESPACE}" \
  --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -

# ── [4/7] 프리플라이트 (chi-* 자동 탐색 → DB 존재 + 토큰 레지스트리 SELECT, §5.6·§7.5) ─
echo ""
echo "[4/7] preflight (db=${DB_FACT}/${DB_DIM}, registry=${DB_DIM}.dim_token_service_dist)"
ch_pod="$(${KUBECTL} get pods -n "${CH_NAMESPACE}" -o name 2>/dev/null \
  | sed 's#^pod/##' | grep '^chi-' | head -1 || true)"
if [[ -z "${ch_pod}" ]]; then
  echo "[ERROR] ${CH_NAMESPACE} 네임스페이스에서 chi-* ClickHouse 파드를 찾지 못했습니다."
  exit 1
fi
echo "  ClickHouse pod: ${ch_pod}"
ch_query() {
  # 파드 안의 clickhouse-client(로컬 접속)로 단일 쿼리 — 접속 계정은 컨테이너가 쓸 앱 계정(CH_USER/CH_PASSWORD).
  # default 계정으로 돌리면 GRANT 누락을 잡지 못하므로(accounts.sql의 GRANT SELECT ON dim_token_service_dist TO mart)
  # 앱 계정으로 같은 SELECT 를 실행해 "DB 존재 + 앱 계정이 실제로 읽을 수 있음"을 함께 확인한다 (§5.6).
  # (system.databases 는 계정에 권한이 있는 DB만 보여 준다 — 행 수가 기대치 미만이면 DB 부재이거나 GRANT 누락)
  # 비밀번호는 --password argv로 넘기지 않는다 — kubectl exec의 argv는 API 서버 감사 이벤트 커맨드에 그대로
  # 남으므로, here-string으로 파드 stdin에 실어 clickhouse-client가 표준입력에서 읽게 한다
  # (버전 무관 형태; -i가 있어야 here-string이 파드 stdin까지 전달된다).
  ${KUBECTL} exec -i -n "${CH_NAMESPACE}" "${ch_pod}" -- \
    sh -c 'clickhouse-client --user "$0" --password "$(cat)" --query "$1"' "${ch_user}" "$1" <<<"${ch_pass}"
}
# 기대 DB 개수 — company-verify 프롬프트에서 CH_DB_FACT==CH_DB_DIM으로 입력하면 대상 집합이 1개로 줄어든다.
expected_db_count=2
[[ "${DB_FACT}" == "${DB_DIM}" ]] && expected_db_count=1
if ! db_rows="$(ch_query "SELECT count() FROM system.databases WHERE name IN ('${DB_FACT}','${DB_DIM}')")"; then
  echo "[ERROR] 프리플라이트 실패: ClickHouse 접속 불가 (계정 ${ch_user})" >&2
  exit 1
fi
if [[ "${db_rows}" != "${expected_db_count}" ]]; then
  echo "[ERROR] 프리플라이트 실패: DB 부재 또는 GRANT 누락 — admin이 ${HERE}/${DDL_DIR}/accounts.sql 실행 필요"
  echo "        계정 ${ch_user} 기준 필요: ${DB_FACT}, ${DB_DIM} (기대 ${expected_db_count}개) / 발견 ${db_rows}개"
  exit 1
fi
echo "  DB OK (as ${ch_user}): ${DB_FACT}, ${DB_DIM}"
if ! registry_rows="$(ch_query "SELECT count() FROM ${DB_DIM}.dim_token_service_dist")"; then
  echo "[ERROR] 프리플라이트 실패: 토큰 레지스트리 SELECT 불가(GRANT 누락) — admin이 ${HERE}/${DDL_DIR}/accounts.sql 실행 필요"
  echo "        대상: ${DB_DIM}.dim_token_service_dist (계정 ${ch_user})"
  echo "        기존 토큰 수집기 모듈이 같은 클러스터에 설치돼 있어야 합니다 (설계 §5.1 — 유일한 접점, 읽기 전용)"
  exit 1
fi
echo "  registry OK (as ${ch_user}): ${DB_DIM}.dim_token_service_dist rows=${registry_rows}"

# ── [5/7] 테이블 DDL (kubectl cp + clickhouse-client — §4.0 매니페스트의 collectors 2파일) ─
echo ""
echo "[5/7] table DDL (${DDL_DIR})"
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
if [[ "${ENV}" == "company-verify" ]]; then
  echo "  (격리 검증 — ${DDL_DIR}/의 테이블 DDL만 적용. DB·전용 계정은 admin이 ${DDL_DIR}/accounts.sql로"
  echo "   먼저 생성(프리플라이트가 DB 존재를 확인함): python3 tools/gen_verify_ddl.py로 재생성 가능)"
else
  echo "  (DB 존재·앱 계정 GRANT는 프리플라이트가 확인함 — 계정/GRANT 자체는 admin이 ${DDL_DIR}/accounts.sql로 적용, 설계 §4.0)"
fi
apply_sql "${HERE}/${DDL_DIR}/raw_token_metrics.sql"
apply_sql "${HERE}/${DDL_DIR}/dim_token_metrics_service.sql"
echo "  (accounts.sql은 적용하지 않았습니다 — CREATE DATABASE/CREATE USER/GRANT는 admin 수동 실행, 설계 §4.0)"

# ── [6/7] CronJob 배포 ────────────────────────────────────────────────────────
echo ""
echo "[6/7] CronJob apply (overlay: ${ENV})"
${KUBECTL} apply -k "${HERE}/k8s/overlays/${ENV}" -n "${NAMESPACE}"

# ── [7/7] 이미지/CH_HOST 주입 ────────────────────────────────────────────────
echo ""
echo "[7/7] set image / set env"
${KUBECTL} set image "cronjob/${CRONJOB_NAME}" \
  "${IMAGE_NAME}=${REGISTRY}/${IMAGE_NAME}:${TAG}" -n "${NAMESPACE}"

# CH_HOST: [4/7]에서 찾은 chi-* 파드명에서 말미 ordinal을 잘라 헤드리스 서비스명 유도
# (예: chi-<cluster>-<cluster>-0-0-0 → chi-<cluster>-<cluster>-0-0.clickhouse.svc)
ch_host="${ch_pod%-*}.${CH_NAMESPACE}.svc"
${KUBECTL} set env "cronjob/${CRONJOB_NAME}" "CH_HOST=${ch_host}" -n "${NAMESPACE}"
echo "  CH_HOST=${ch_host}"

echo ""
echo "[OK] 설치 완료. 정기 실행은 02:05~09:05 KST 8슬롯 (BATCH_RESULT ... slot=HH final=0|1). 수동 테스트:"
echo "  ${KUBECTL} create job --from=cronjob/${CRONJOB_NAME} ${CRONJOB_NAME}-manual-\$(date +%s) -n ${NAMESPACE}"
echo "  (날짜 범위 재수집: python3 ${HERE}/tools/rerun.py --context ${KUBE_CONTEXT} --namespace ${NAMESPACE} --cronjob ${CRONJOB_NAME} --from <D0> --to <D1>)"
echo "  (수기 CSV 적재: python3 ${HERE}/tools/manual_load.py --context ${KUBE_CONTEXT} --namespace ${NAMESPACE} --from <D0> --to <D1> --gpu <gpu.csv> --serving <serving.csv>)"
