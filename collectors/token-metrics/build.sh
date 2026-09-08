#!/usr/bin/env bash
# token-metrics collector 이미지 빌드/푸시 (설계 §7.5 — 신규 이미지 token-metrics-collector만; 기존 이미지 재빌드 없음)
#
# 사용법:
#   ./collectors/token-metrics/build.sh [--registry <registry>] [--tag <tag>] <stage|company>
#
#   stage:   REGISTRY 기본 ghcr.io/yoonsungnam
#   company: --registry 필수 (사내 Harbor, 예: harbor.example.internal/gpu-monitoring) — BASE_IMAGE를 Harbor proxy로 치환
#   태그 기본: git short SHA (git 밖이면 latest). company-verify는 company 이미지를 그대로 쓴다.
set -euo pipefail
SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
cd "$(dirname "${SCRIPT_PATH}")"

IMAGE_NAME="token-metrics-collector"
REGISTRY=""
TAG=""
ENV=""

usage() {
  grep '^# ' "${SCRIPT_PATH}" | head -8; exit 1
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
  # 사내망은 docker hub 직접 pull 불가 — Harbor pull-through proxy 경유 (기존 모듈과 동일 관례)
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
echo "  ./collectors/token-metrics/install.sh --registry ${REGISTRY} --tag ${TAG} ${ENV}"
