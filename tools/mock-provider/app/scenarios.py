from dataclasses import dataclass


@dataclass
class ScenarioState:
    """계약 밖 일탈 주입 상태 — 전부 기본값(OFF)이면 완전한 계약 준수 동작."""
    not_ready_until_uptime_s: float = 0.0   # 앱 가동 N초 전까지, 과거 유효 date 요청에 대해 409 (당일/미래 400은 그대로)
    retry_after_s: int = 5                  # 409/429 응답의 Retry-After 값
    rate_limit_every: int = 0               # N번째 요청마다 429 (0=off)
    error_503_every: int = 0                # N번째 요청마다 503 (0=off)
    summary_extra_pct: int = 0              # summary inputTokens를 +N% 왜곡 (§5.1-3-4 검증용)
    name_drift: str = ""                    # 응답 serviceGroup/service 뒤에 붙일 문자열 (§5.0 검증용)
    generated_at_change_at_page: int = 0    # N페이지부터 generatedAt 변경 (§5.3 검증용)
    not_ready_at_page: int = 0              # N페이지에서 409 (§5.2 검증용)
    request_count: int = 0                  # 429/503 주기 판정용 카운터
