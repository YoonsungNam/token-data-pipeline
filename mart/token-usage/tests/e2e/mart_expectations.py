#!/usr/bin/env python3
"""mart E2E 기대값 산출 — mock datagen 재현으로 결정적 기대값을 계산해
`key=value` 줄로 출력한다(줄마다 1개). run_e2e.sh가 줄 단위로 읽어 셸
연관배열(EXP[...])에 담고, verify_expected_results.sql의 {EXP_*} 토큰을
sed로 치환한다 (Plan 3 T5).

인자 서명은 collectors/token-usage/tests/e2e/ci_expectations.py와 동일
(date seed users anon models) — 브리프 Step 6의 로컬 검증 커맨드가 이
5-인자 형태를 그대로 사용한다:
    python3 tests/e2e/mart_expectations.py 2026-07-10 token-mock-1 50 10 \
        "claude-opus-4-8,claude-sonnet-5,claude-haiku-4-5"
6번째 인자 num_services(옵션, 기본 3)는 mart 전용 확장이다 — datagen은
서비스명과 무관하게 (seed, date)만으로 결정되므로, 시드 서비스 A/B/C가
"동일 seed"를 그대로 재사용하는 성질(seed_fact.py 참조)을 이용해 detail
총합·org 버킷 합계·cost 합 등을 num_services배로 스케일한다. 5월 고정
시드는 Service A 1개만 추가되므로(§Step2) run_e2e.sh가 num_services=1로
재호출해 user-0005 행수 기대값(발생일 기준 조직 귀속 검증용)을 따로 얻는다.

주의(정본 이원화 리스크): 아래 PRICES/resolve_org는 tests/e2e/ddl_test_dims.sql의
시드값을 그대로 재현한 것이다 — 시드 파일을 고치면 이 파일도 함께 갱신해야
값이 어긋나지 않는다.
"""
import sys
from pathlib import Path

# e2e -> tests -> token-usage -> mart -> repo root = parents[4].
# collectors/token-usage/tests/e2e/ci_expectations.py와 "우연히" 같은 인덱스
# (mart/collectors 둘 다 repo root 바로 아래 1단 디렉터리라 경로 깊이가 동일) —
# 브리프 지시대로 맹복사하지 않고 실측(Path(...).parents[i] 출력)으로 확인했다.
sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "tools" / "mock-provider"))

from app.config import Config as MockConfig    # noqa: E402
from app.datagen import build_records          # noqa: E402

# dim_token_model 시드(ddl_test_dims.sql)와 동일 단가(USD per MTok) — 정본은 SQL 파일,
# 이 표는 cost 기대값 계산을 위한 재현.
PRICES = {
    "claude-opus-4-8": (15.0, 1.5, 18.75, 75.0),
    "claude-sonnet-5": (3.0, 0.3, 3.75, 15.0),
}

# dim_token_user_org 시드(ddl_test_dims.sql)와 동일 매핑 — user-0000~0019만 등록
# (X팀 0~6 / Y팀 7~13 / Z팀 14~19), user-0005는 2026-06-01부로 X팀→Z팀 이관.
# user-0020 이후(및 anon-*, unclassified '')는 의도적 미등록 → unknown 버킷.
ORG_X = ("A부문", "X팀")
ORG_Y = ("A부문", "Y팀")
ORG_Z = ("B부문", "Z팀")
ORG_UNKNOWN = ("unknown",)


def resolve_org(user_id: str, date: str) -> tuple:
    """dim_token_user_org 시드(ddl_test_dims.sql)와 1:1 대응하는 (user_id, date) → org_path.

    mart STEP1의 조인은 user_id 문자열 키만 사용하므로(user_type 무관) 여기도
    동일하게 user_id 패턴만으로 판정한다.
    """
    if user_id and user_id.startswith("user-"):
        idx = int(user_id[5:])
        if idx == 5:
            return ORG_Z if date >= "2026-06-01" else ORG_X
        if 0 <= idx <= 6:
            return ORG_X
        if 7 <= idx <= 13:
            return ORG_Y
        if 14 <= idx <= 19:
            return ORG_Z
    return ORG_UNKNOWN


def compute(date: str, seed: str, users: int, anon: int, models: list[str],
            num_services: int = 3) -> dict:
    cfg = MockConfig(users=users, anon_users=anon, models=models, seed=seed)
    records = build_records(cfg, date)

    total_input = 0
    org_totals = {ORG_X: 0, ORG_Y: 0, ORG_Z: 0}
    unknown_rows = 0
    haiku_rows = 0
    unknown_model_rows = 0
    cost_sum = 0.0
    user5_rows = 0

    for r in records:
        uid = r.user_id or ""                      # userId None -> '' (§5.4 정규화 재현)
        ti = r.input_tokens + r.cache_read_tokens + r.cache_creation_tokens
        total_input += ti

        org = resolve_org(uid, date)
        if org == ORG_UNKNOWN:
            unknown_rows += 1
        else:
            org_totals[org] += ti

        if r.model == "claude-haiku-4-5":
            haiku_rows += 1
        if r.model == "unknown":
            unknown_model_rows += 1
        if r.model in PRICES:
            p_in, p_cr, p_cc, p_out = PRICES[r.model]
            cost_sum += (r.input_tokens * p_in + r.cache_read_tokens * p_cr
                         + r.cache_creation_tokens * p_cc + r.output_tokens * p_out) / 1e6
        if uid == "user-0005":
            user5_rows += 1

    n = num_services
    return {
        "rows": len(records),
        "detail_rows": n * len(records),
        "detail_total_input": n * total_input,
        "org_x_total": n * org_totals[ORG_X],
        "org_y_total": n * org_totals[ORG_Y],
        "org_z_total": n * org_totals[ORG_Z],
        "unknown_rows": n * unknown_rows,
        "haiku_null_rows": n * haiku_rows,
        "unknown_model_rows": n * unknown_model_rows,
        "cost_sum": f"{n * cost_sum:.10f}",
        "user5_rows": n * user5_rows,
    }


def main() -> None:
    if len(sys.argv) not in (6, 7):
        print("usage: mart_expectations.py <date> <seed> <users> <anon> <models(콤마)> "
              "[num_services=3]", file=sys.stderr)
        raise SystemExit(2)
    date, seed, users, anon, models = sys.argv[1:6]
    num_services = int(sys.argv[6]) if len(sys.argv) == 7 else 3
    result = compute(date, seed, int(users), int(anon),
                      [m for m in models.split(",") if m], num_services)
    for k, v in result.items():
        print(f"{k}={v}")


if __name__ == "__main__":
    main()
