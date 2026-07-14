# 스펙 §7.2: python:3.12-slim, requirements 선복사 캐시, 이미지 1개 + CronJob command 교체.
# BASE_IMAGE는 company 빌드에서 Harbor proxy로 치환된다 (build.sh --registry 경로).
ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
# mart는 endpoints 불요 — dim_token_service 게이트 기준
CMD ["python", "-m", "app.batch"]
