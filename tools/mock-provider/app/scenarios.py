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
    # --- /v1/metrics 전용 (int 0/1 — 0=OFF; 수집기 §5.3 계층 2 플래그·케이스 E·engine null 검증용) ---
    metrics_gpu_hours_over: int = 0         # 1이면 첫 gpu 행 gpuHours = gpuCount*24 + 10 (hours_over_count)
    metrics_unknown_serving: int = 0        # 1이면 model="unknown", category="serving" 행 1개 추가 (unknown_violation)
    metrics_pct_non_monotone: int = 0       # 1이면 첫 serving 행 ttftMs p90 = p50 - 1 (pct_non_monotone)
    metrics_dup_gpu_rows: int = 0           # 1이면 첫 gpu 행 복제본을 인덱스 1에 삽입 — 인접 중복 (dup_merged)
    metrics_empty_gpu: int = 0              # 1이면 gpu: [] (케이스 E — serving만 있는 응답)
    metrics_engine_null: int = 0            # 1이면 engine: null (engine 부재 허용 검증)
