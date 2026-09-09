# token-metrics-collector 이미지 (설계 §5.1 클론 규칙 — 기존 수집기 Dockerfile과 동일 패턴).
# python:3.12-slim, requirements 선복사 캐시, 이미지 1개 + CronJob command 교체(rerun/manual Job).
# BASE_IMAGE는 company 빌드에서 Harbor proxy로 치환된다 (build.sh --registry 경로).
ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
# endpoints.yaml은 이미지에 굽지 않는다 — ConfigMap 마운트 + ENDPOINTS_FILE env가 정본 (§5.6)
CMD ["python", "-m", "app.main"]
