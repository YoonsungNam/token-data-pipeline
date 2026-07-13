"""mock-provider의 결정성으로 CI 기대값을 산출한다.

mock의 datagen 로직(sha256 기반)을 그대로 재현 — mock 저장소가 이 레포 안에 있으므로
tools/mock-provider/app을 직접 import해 기대 행수·합계를 계산하고, ClickHouse 적재
결과와 비교할 SQL 상수를 출력한다.
사용: python ci_expectations.py <date> <seed> <users> <anon> <models(콤마)>
출력: "rows=<n> input_sum=<n> requests_sum=<n>"
"""
import sys
from pathlib import Path

# parents[0]=e2e parents[1]=tests parents[2]=token-usage parents[3]=collectors
# parents[4]=repo root — tools/mock-provider는 repo root 기준
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "tools" / "mock-provider"))

from app.config import Config as MockConfig            # noqa: E402
from app.datagen import build_records, build_summary   # noqa: E402


def main():
    date, seed, users, anon, models = sys.argv[1:6]
    cfg = MockConfig(users=int(users), anon_users=int(anon),
                     models=[m for m in models.split(",") if m], seed=seed)
    records = build_records(cfg, date)
    summary = build_summary(records)
    print(f"rows={len(records)} input_sum={summary['inputTokens']} "
          f"requests_sum={summary['requests']}")


if __name__ == "__main__":
    main()
