#!/usr/bin/env bash
# token-mart-metrics 배치 설치 (설계 §7.5 "새 코드만 새로 배포" — mart/token-usage/install.sh 클론)
#
# 사용법:
#   ./mart/token-metrics/install.sh [--registry <registry>] [--tag <tag>] \
#     [--context <kube-context>] [-n|--namespace <ns>] <stage|company|company-verify>
#   (환경은 위치 인자 또는 --overlay <stage|company|company-verify> — 배포 문서(T11) 표기와 동일)
#
#   stage:           context 기본 homelab, registry 기본 ghcr.io/yoonsungnam
#   company:         --context/--registry 필수 (사내 Harbor 주소는 인자로만 — 커밋 금지)
#   company-verify:  격리 검증(선택 — 설계 §7.5). Secret/CronJob 이름 -verify 접미, DDL은 ddl/company-verify/,
#                    CH_DB_FACT/DIM/MART = token_verify_*; CH_DB_TOKEN_MART/CH_DB_TOKEN_DIM은 운영 DB(mart/gpu_data)
#                    유지(토큰 측 읽기 — 운영 GRANT 의존, Plan 6a ddl/README).
#
# 수행 순서:
#   [1/6] registry-pull-secret — 없을 때만 생성 (네임스페이스 공유 Secret, 있으면 손대지 않음 — 설계 §7.5)
#   [2/6] token-mart-metrics-ch-secret[-verify] 멱등 생성 (envFrom — 키 11개, CH_HOST 포함)
#   [3/6] 읽기 계약 프리플라이트 — DESCRIBE 3테이블/13컬럼 (설계 §6.1; 불일치 시 exit 3, DDL 적용 전 중단)
#   [4/6] 테이블 DDL 적용 (mart_metrics_tables.sql — accounts.sql은 admin 수동)
#   [5/6] CronJob 배포 (kustomize overlay)
#   [6/6] 이미지 주소 주입 + 수동 테스트 커맨드 안내
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

IMAGE_NAME="token-mart-metrics"
CRONJOB_NAME="token-mart-metrics"
SECRET_NAME="token-mart-metrics-ch-secret"
PULL_SECRET_NAME="registry-pull-secret"
CH_NAMESPACE="clickhouse"

REGISTRY=""; TAG=""; KUBE_CONTEXT=""; NAMESPACE="monitoring"; ENV=""

usage() { sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry)      REGISTRY="$2"; shift 2 ;;
    --tag)           TAG="$2"; shift 2 ;;
    --context)       KUBE_CONTEXT="$2"; shift 2 ;;
    -n|--namespace)  NAMESPACE="$2"; shift 2 ;;
    --overlay)       ENV="$2"; shift 2 ;;
    stage|company|company-verify) ENV="$1"; shift ;;
    *) echo "[ERROR] unknown arg: $1"; usage ;;
  esac
done
case "${ENV}" in stage|company|company-verify) ;; *) echo "[ERROR] env must be stage|company|company-verify: '${ENV}'"; usage ;; esac

# DB명 5종 (설계 §6.1 Secret 키 CH_DB_* — [2/6] Secret 값이자 [3/6] 프리플라이트의 DESCRIBE 대상 접두).
# 토큰 측 읽기 전용 CH_DB_TOKEN_MART/CH_DB_TOKEN_DIM 기본 = CH_DB_MART/CH_DB_DIM (app/ch.py fallback과 동일).
CH_DB_FACT="fact"; CH_DB_DIM="gpu_data"; CH_DB_MART="mart"
CH_DB_TOKEN_MART="mart"; CH_DB_TOKEN_DIM="gpu_data"
MAX_MUTATIONS="64"

case "${ENV}" in
  stage)
    KUBE_CONTEXT="${KUBE_CONTEXT:-homelab}"
    REGISTRY="${REGISTRY:-ghcr.io/yoonsungnam}"
    ;;
  company)
    [[ -n "${KUBE_CONTEXT}" ]] || { echo "[ERROR] company 환경에서는 --context 옵션이 필수입니다."; usage; }
    [[ -n "${REGISTRY}" ]]     || { echo "[ERROR] company 환경에서는 --registry 옵션이 필수입니다."; usage; }
    ;;
  company-verify)
    [[ -n "${KUBE_CONTEXT}" ]] || { echo "[ERROR] company-verify 환경에서는 --context 옵션이 필수입니다."; usage; }
    [[ -n "${REGISTRY}" ]]     || { echo "[ERROR] company-verify 환경에서는 --registry 옵션이 필수입니다."; usage; }
    SECRET_NAME="${SECRET_NAME}-verify"
    CRONJOB_NAME="${CRONJOB_NAME}-verify"
    # 격리 DB 3종(tools/gen_verify_ddl.py 기본안) — 토큰 측 읽기(CH_DB_TOKEN_*)는 운영 DB 유지
    CH_DB_FACT="token_verify_fact"; CH_DB_DIM="token_verify_dim"; CH_DB_MART="token_verify_mart"
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

# kube API 서버 호스트를 NO_PROXY에 자동 추가 (사내 프록시 환경에서 kubectl 통신 보존 — 원형과 동일)
api_server="$(${KUBECTL} config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || true)"
if [[ -n "${api_server}" ]]; then
  api_host="$(printf '%s' "${api_server}" | sed -E 's#^https?://##; s#:[0-9]+$##')"
  export NO_PROXY="${NO_PROXY:+${NO_PROXY},}${api_host}"
  export no_proxy="${no_proxy:+${no_proxy},}${api_host}"
fi

echo "=== token-mart-metrics install (${ENV}) ==="
echo "context=${KUBE_CONTEXT} namespace=${NAMESPACE} image=${REGISTRY}/${IMAGE_NAME}:${TAG}"

# ── ClickHouse 파드 탐색 (chi-*; [2/6] CH_HOST 값·[3/6] DESCRIBE·[4/6] DDL이 공유) ──────────
ch_pod="$(${KUBECTL} get pods -n "${CH_NAMESPACE}" -o name 2>/dev/null \
  | sed 's#^pod/##' | grep '^chi-' | head -1 || true)"
if [[ -z "${ch_pod}" ]]; then
  echo "[ERROR] ${CH_NAMESPACE} 네임스페이스에서 chi-* ClickHouse 파드를 찾지 못했습니다."
  exit 1
fi
# 파드명 말미 ordinal을 잘라 헤드리스 서비스명 유도 (예: chi-<cluster>-<cluster>-0-0-0 → chi-<cluster>-<cluster>-0-0)
ch_host="${ch_pod%-*}.${CH_NAMESPACE}.svc"
echo "  ClickHouse pod: ${ch_pod} (CH_HOST=${ch_host})"

# ── [1/6] registry pull secret — 없을 때만 생성 (설계 §7.5 공유 Secret 예외) ───────────────
echo ""
echo "[1/6] image pull secret '${PULL_SECRET_NAME}'"
if ${KUBECTL} get secret "${PULL_SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  echo "  이미 존재합니다 — 네임스페이스 공유 Secret이므로 손대지 않습니다 (기존 token-usage 배포 소유)."
else
  read -r -p "  registry server [${REGISTRY%%/*}]: " reg_server
  reg_server="${reg_server:-${REGISTRY%%/*}}"
  read -r -p "  registry username: " reg_user
  read -r -s -p "  registry password/token: " reg_pass; echo ""
  ${KUBECTL} create secret docker-registry "${PULL_SECRET_NAME}" \
    --docker-server="${reg_server}" --docker-username="${reg_user}" \
    --docker-password="${reg_pass}" -n "${NAMESPACE}"
fi

# ── [2/6] app secret (envFrom — 키 이름이 곧 컨테이너 env 이름, 설계 §6.1) ─────────────────
# 원형과 델타: 지연 서비스 허용 목록 env 없음(M0 커버리지는 레지스트리 coverage_since/until이 결정),
# CH_DB_* 5종 + MART_METRICS_MAX_MUTATIONS_PER_RUN 항상 포함, CH_HOST도 Secret 키(정적 env 주입 금지).
# INSERT_QUORUM=auto는 company·company-verify 자동 포함(2s×2r 물리 클러스터 — 레플리카 지연 게이트).
echo ""
echo "[2/6] app secret '${SECRET_NAME}'"
if ${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  read -r -p "  이미 존재합니다. 갱신하시겠습니까? [y/N] " ans || ans=""
else
  ans="y"
fi
if [[ "${ans}" == "y" || "${ans}" == "Y" ]]; then
  ch_user_default="mart"
  [[ "${ENV}" == "company-verify" ]] && ch_user_default="token_verify"
  read -r -p "  CH_USER [${ch_user_default}]: " ch_user
  ch_user="${ch_user:-${ch_user_default}}"
  read -r -s -p "  CH_PASSWORD: " ch_pass; echo ""
  read -r -p "  CH_DB_FACT [${CH_DB_FACT}]: " v;        CH_DB_FACT="${v:-${CH_DB_FACT}}"
  read -r -p "  CH_DB_DIM [${CH_DB_DIM}]: " v;          CH_DB_DIM="${v:-${CH_DB_DIM}}"
  read -r -p "  CH_DB_MART [${CH_DB_MART}]: " v;        CH_DB_MART="${v:-${CH_DB_MART}}"
  read -r -p "  CH_DB_TOKEN_MART (토큰 mart 읽기 — 격리 검증 시 운영 DB) [${CH_DB_TOKEN_MART}]: " v
  CH_DB_TOKEN_MART="${v:-${CH_DB_TOKEN_MART}}"
  read -r -p "  CH_DB_TOKEN_DIM (dim_token_service 읽기 — 격리 검증 시 운영 DB) [${CH_DB_TOKEN_DIM}]: " v
  CH_DB_TOKEN_DIM="${v:-${CH_DB_TOKEN_DIM}}"
  read -r -p "  MART_METRICS_MAX_MUTATIONS_PER_RUN [${MAX_MUTATIONS}]: " v; MAX_MUTATIONS="${v:-${MAX_MUTATIONS}}"
  # stage 홈랩 CHI는 ZK 없음 — ON CLUSTER 불가하므로 단일노드 모드(빈 값). company/-verify는 클러스터명 주입
  # (CH_CLUSTER와 DDL의 ON CLUSTER 리터럴 일치 전제)
  CH_CLUSTER_VALUE="gpu-monitoring"
  [[ "${ENV}" == "stage" ]] && CH_CLUSTER_VALUE=""
  args=(--from-literal="CH_HOST=${ch_host}"
        --from-literal="CH_PORT=8123"
        --from-literal="CH_USER=${ch_user}"
        --from-literal="CH_PASSWORD=${ch_pass}"
        --from-literal="CH_CLUSTER=${CH_CLUSTER_VALUE}"
        --from-literal="CH_DB_FACT=${CH_DB_FACT}"
        --from-literal="CH_DB_DIM=${CH_DB_DIM}"
        --from-literal="CH_DB_MART=${CH_DB_MART}"
        --from-literal="CH_DB_TOKEN_MART=${CH_DB_TOKEN_MART}"
        --from-literal="CH_DB_TOKEN_DIM=${CH_DB_TOKEN_DIM}"
        --from-literal="MART_METRICS_MAX_MUTATIONS_PER_RUN=${MAX_MUTATIONS}")
  if [[ "${ENV}" == "company" || "${ENV}" == "company-verify" ]]; then
    args+=(--from-literal="INSERT_QUORUM=auto")
  fi
  ${KUBECTL} create secret generic "${SECRET_NAME}" "${args[@]}" -n "${NAMESPACE}" \
    --dry-run=client -o yaml | ${KUBECTL} apply -n "${NAMESPACE}" -f -
else
  # 갱신하지 않으면(N/EOF) 기존 Secret의 CH_DB_TOKEN_MART/CH_DB_TOKEN_DIM(있을 때만)을 [3/6] 프리플라이트
  # DESCRIBE 대상 DB로 쓴다 — READ_CONTRACT 배열의 db 접두가 이 값으로 보간된다. CH_USER/CH_PASSWORD도
  # 함께 읽어 둔다 — 아래 ch_query()가 [3/6] DESCRIBE를 이 값들로(앱 계정) 실행한다(둘 중 하나라도
  # 없으면 갱신(y)을 유도하고 중단).
  existing_token_mart="$(${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.data.CH_DB_TOKEN_MART}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  existing_token_dim="$(${KUBECTL} get secret "${SECRET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.data.CH_DB_TOKEN_DIM}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  [[ -n "${existing_token_mart}" ]] && CH_DB_TOKEN_MART="${existing_token_mart}"
  [[ -n "${existing_token_dim}" ]] && CH_DB_TOKEN_DIM="${existing_token_dim}"
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

# 파드 안의 clickhouse-client(로컬 접속)로 단일 쿼리 — 접속 계정은 컨테이너가 쓸 앱 계정(CH_USER/CH_PASSWORD).
# default 계정으로 돌리면 GRANT 누락을 잡지 못하므로(accounts.sql의 GRANT SELECT ... TO mart) 앱 계정으로
# 같은 쿼리를 실행해 "테이블 존재 + 앱 계정이 DESCRIBE/SELECT 가능"을 함께 확인한다(설계 §6.1·§7.5;
# collectors/token-metrics/install.sh ch_query()와 동일 관례).
# 비밀번호는 --password argv로 넘기지 않는다 — kubectl exec의 argv는 API 서버 감사 이벤트 커맨드에 그대로
# 남으므로, here-string으로 파드 stdin에 실어 clickhouse-client가 표준입력에서 읽게 한다
# (버전 무관 형태; -i가 있어야 here-string이 파드 stdin까지 전달된다).
ch_query() {
  ${KUBECTL} exec -i -n "${CH_NAMESPACE}" "${ch_pod}" -- \
    sh -c 'clickhouse-client --user "$0" --password "$(cat)" --query "$1"' "${ch_user}" "$1" <<<"${ch_pass}"
}

# ── [3/6] 읽기 계약 프리플라이트 (설계 §6.1 3테이블/13컬럼 — DESCRIBE 대조, 불일치 시 설치 중단) ──
# 항목 형식 "<db>.<table>_dist:<column>" — tests/test_install_contract.py가 app/preflight.py READ_CONTRACT와
# 동일함을 단언한다(정본 2곳의 드리프트 차단). 여분 컬럼은 허용(계약 = 부분집합).
READ_CONTRACT=(
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:date"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:service_group"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:service"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:model"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:input_tokens"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:cache_read_tokens"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:cache_creation_tokens"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:output_tokens"
  "${CH_DB_TOKEN_MART}.token_usage_1d_dist:requests"
  "${CH_DB_TOKEN_MART}.agg_token_service_1d_dist:date"
  "${CH_DB_TOKEN_MART}.agg_token_service_1d_dist:service"
  "${CH_DB_TOKEN_DIM}.dim_token_service_dist:service"
  "${CH_DB_TOKEN_DIM}.dim_token_service_dist:enabled"
)
echo ""
echo "[3/6] read-contract preflight (DESCRIBE — ${#READ_CONTRACT[@]} columns)"
missing=()
prev_table=""; cols=""
for entry in "${READ_CONTRACT[@]}"; do
  table="${entry%%:*}"; col="${entry##*:}"
  if [[ "${table}" != "${prev_table}" ]]; then
    prev_table="${table}"
    cols="$(ch_query "DESCRIBE TABLE ${table}" 2>/dev/null | cut -f1 || true)"
    if [[ -z "${cols}" ]]; then
      missing+=("${table%_dist}.*")
      echo "  ${table}: 테이블 부재(또는 DESCRIBE 권한 없음)"
    else
      echo "  ${table}: $(printf '%s\n' "${cols}" | wc -l) columns"
    fi
  fi
  if [[ -n "${cols}" ]] && ! grep -qx "${col}" <<<"${cols}"; then
    missing+=("${table%_dist}.${col}")
  fi
done
if [[ ${#missing[@]} -gt 0 ]]; then
  echo "PREFLIGHT FAIL read_contract missing=$(IFS=,; echo "${missing[*]}")"
  echo "  (설계 §6.1 읽기 계약 불일치 — DDL/CronJob 적용 전 중단. 사내 스키마·GRANT 확인 후 재실행)"
  exit 3
fi
echo "  PREFLIGHT OK read_contract tables=$(printf '%s\n' "${READ_CONTRACT[@]%%:*}" | sort -u | wc -l) columns=${#READ_CONTRACT[@]}"

# ── [4/6] 테이블 DDL (kubectl cp + clickhouse-client — 원형 apply_sql 그대로) ────────────────
echo ""
echo "[4/6] table DDL"
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
# stage 홈랩 CHI는 ZK 없음 — Replicated/ON CLUSTER 불가, 생성 변형 사용 (tools/gen_stage_ddl.py)
[[ "${ENV}" == "stage" ]] && DDL_DIR="ddl/stage"
echo "  (GRANT는 admin 수동: ${DDL_DIR}/accounts.sql — Plan 6a ddl/README 적용 순서 2. 읽기 대상 DDL"
echo "   collectors/token-metrics/ddl·assets/model-catalog/ddl은 6b install.sh/admin이 먼저 적용해야 한다)"
apply_sql "${HERE}/${DDL_DIR}/mart_metrics_tables.sql"
echo "  (accounts.sql은 적용하지 않았습니다 — GRANT/계정은 admin 수동, 설계 §7.5 DDL/GRANT)"

# ── [5/6] CronJob 배포 ────────────────────────────────────────────────────────────────────
echo ""
echo "[5/6] CronJob apply (overlay: ${ENV})"
${KUBECTL} apply -k "${HERE}/k8s/overlays/${ENV}" -n "${NAMESPACE}"

# ── [6/6] 이미지 주소 주입 (env는 전부 Secret — 정적 env 주입 없음) ──────────────────────────
echo ""
echo "[6/6] set image"
${KUBECTL} set image "cronjob/${CRONJOB_NAME}" \
  "${IMAGE_NAME}=${REGISTRY}/${IMAGE_NAME}:${TAG}" -n "${NAMESPACE}"

rerun_hint="python3 ${HERE}/tools/rerun.py --context ${KUBE_CONTEXT} --namespace ${NAMESPACE}"
[[ "${ENV}" == "company-verify" ]] && rerun_hint="${rerun_hint} --cronjob ${CRONJOB_NAME}"
echo ""
echo "[OK] 설치 완료. 수동 테스트(창 10:50 KST 이후 — 설계 §6.3):"
echo "  ${rerun_hint}"
echo "  (범위 재수행: ${rerun_hint} --from YYYY-MM-DD --to YYYY-MM-DD [--chunk-days 7])"
echo "  (또는 kubectl 직접: ${KUBECTL} create job --from=cronjob/${CRONJOB_NAME} ${CRONJOB_NAME}-manual-\$(date +%s) -n ${NAMESPACE})"
