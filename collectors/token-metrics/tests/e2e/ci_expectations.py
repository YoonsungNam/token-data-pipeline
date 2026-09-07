"""mock-provider 의 결정성으로 CI 기대값을 산출한다 (Plan 6b T11 — §7.3 "기대치 datagen.build_metrics").

mock 저장소가 이 레포 안에 있으므로 tools/mock-provider/app 을 직접 import 해
같은 (seed, date) 의 /v1/metrics 페이로드를 재현하고, ClickHouse 적재 결과와 비교할 상수를 출력한다.
사용: python ci_expectations.py <date> <seed> <models(콤마)>
출력: "rows_gpu=<n> rows_serving=<n> gpu_hours_sum=<x>"
  rows_gpu     = gpu 행 수 (T3: model="unknown" 은 category="test" 에서 허용 → 기본 3모델 5행 전부 적재)
  rows_serving = serving 레코드 수 × 3 (레코드당 ttft_ms · itl_ms · output_tps long-form 1행씩)
  gpu_hours_sum = gpuHours 합 (소수 1자리 — verify SQL 은 abs 차 0.05 허용)
"""
from __future__ import annotations

import sys
from pathlib import Path

# parents[0]=e2e parents[1]=tests parents[2]=token-metrics parents[3]=collectors
# parents[4]=repo root — tools/mock-provider 는 repo root 기준
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "tools" / "mock-provider"))

from app.config import Config as MockConfig   # noqa: E402  (mock 의 app — 수집기 app 과 무관)
from app.datagen import build_metrics         # noqa: E402

SERVING_ROWS_PER_RECORD = 3   # ttft_ms · itl_ms · output_tps


def expectations(date: str, seed: str, models: list[str]) -> tuple[int, int, float]:
    cfg = MockConfig(service_group="Mock Group", service="Mock Service A", seed=seed, models=models)
    p = build_metrics(cfg, date)
    rows_gpu = len(p["gpu"])
    rows_serving = SERVING_ROWS_PER_RECORD * len(p["serving"])
    gpu_hours_sum = round(sum(r["gpuHours"] for r in p["gpu"]), 1)
    return rows_gpu, rows_serving, gpu_hours_sum


def main() -> None:
    if len(sys.argv) != 4:
        print("usage: ci_expectations.py <date> <seed> <models,comma,separated>", file=sys.stderr)
        raise SystemExit(2)
    date, seed, models_csv = sys.argv[1:4]
    models = [m for m in models_csv.split(",") if m]
    rows_gpu, rows_serving, gpu_hours_sum = expectations(date, seed, models)
    print(f"rows_gpu={rows_gpu} rows_serving={rows_serving} gpu_hours_sum={gpu_hours_sum}")


if __name__ == "__main__":
    main()
