"""읽기 계약 프리플라이트 (설계 §6.1·§7.5) — 순수 함수(I/O 없음).

토큰 측 3테이블/13컬럼만 의존한다(그 외 기존 테이블·컬럼 의존 없음). 키는 `db.table`
(`_dist` 접미 없음) — 호출자(batch.preflight_or_fail(T5) / install.sh [3/6](T8))가
`f"{table}_dist"`로 DESCRIBE 한 결과를 이 키로 넘긴다. DB명은 app.ch의
DB_TOKEN_MART/DB_TOKEN_DIM(모듈 로드 시 1회 평가 — company-verify 격리 시 운영 DB).
install.sh의 bash 배열 READ_CONTRACT(13항목 `db.table_dist:column`)는
tests/test_install_contract.py(T8)가 이 dict와 동일함을 단언한다.
"""
from __future__ import annotations

from app.ch import DB_TOKEN_DIM, DB_TOKEN_MART

READ_CONTRACT: dict[str, tuple[str, ...]] = {
    f"{DB_TOKEN_MART}.token_usage_1d": (
        "date", "service_group", "service", "model",
        "input_tokens", "cache_read_tokens", "cache_creation_tokens", "output_tokens",
        "requests"),                                          # 9
    f"{DB_TOKEN_MART}.agg_token_service_1d": ("date", "service"),   # 2 — M0b 토큰 mart 존재 확인
    f"{DB_TOKEN_DIM}.dim_token_service": ("service", "enabled"),    # 2 — usage_svc 모집단
}


def contract_tables() -> list[str]:
    """계약 테이블 3개 — `db.table`(`_dist` 없음), READ_CONTRACT 선언 순."""
    return list(READ_CONTRACT)


def missing_columns(described: dict[str, list[str]]) -> list[str]:
    """DESCRIBE 결과 대조 → 누락 목록(정렬). 여분 컬럼·계약 밖 테이블 키는 무시한다.
    described[table]이 없거나 빈 리스트(테이블 부재 — CHGate.describe()가 []를 반환)면
    `<table>.*` 1항목으로 보고한다. 비어 있지 않은 반환 = 계약 위반 → 호출자가
    `PREFLIGHT FAIL read_contract missing=<a,b,...>` 로그 후 중단(설치 exit 3 / 배치 FAILURE)."""
    missing: list[str] = []
    for table, cols in READ_CONTRACT.items():
        have = described.get(table)
        if not have:
            missing.append(f"{table}.*")
            continue
        have_set = set(have)
        missing.extend(f"{table}.{col}" for col in cols if col not in have_set)
    return sorted(missing)
