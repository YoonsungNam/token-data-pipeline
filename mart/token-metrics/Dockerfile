# 설계 §7.5 독립 이미지 token-mart-metrics — mart/token-usage/Dockerfile 클론 (python:3.12-slim,
# requirements 선복사 캐시). BASE_IMAGE는 company 빌드에서 Harbor proxy로 치환된다 (build.sh --registry 경로).
ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
# ENTRYPOINT(원형은 CMD): rerun.py가 containers[0].args = ["--from", D, "--to", D]만 덮는다 (설계 §6.3).
# 스모크는 `docker run --rm token-mart-metrics:ci --help` — 뒤에 `python -m app.batch`를 다시 붙이지 말 것.
# mart는 endpoints ConfigMap 불요 — dim_token_metrics_service/앵커가 게이트 기준 (설계 §6.1 M0)
ENTRYPOINT ["python", "-m", "app.batch"]
