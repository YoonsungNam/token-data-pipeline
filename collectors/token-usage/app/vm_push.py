"""VictoriaMetrics 게이지 push (§5.5) — 서비스 단위 summary 보고값만.

distinct_users는 비가산(교차 sum 금지) — 게이지명에 reported_ 접두로 의미 고정.
push 실패는 WARN (CH가 원천). rerun 경로는 main이 이 함수를 호출하지 않는다(기본 생략).
"""
from datetime import datetime

GAUGES = (("token_usage_daily_input_tokens", "inputTokens"),
          ("token_usage_daily_cache_read_tokens", "cacheReadTokens"),
          ("token_usage_daily_cache_creation_tokens", "cacheCreationTokens"),
          ("token_usage_daily_output_tokens", "outputTokens"),
          ("token_usage_daily_requests", "requests"),
          ("token_usage_daily_reported_distinct_users", "distinctUsers"))


def _label_escape(v: str) -> str:
    return v.replace("\\", "\\\\").replace('"', '\\"')


def push_service_summary(cfg, entry, date: str, summary: dict, session) -> list[str]:
    if not cfg.vm_push_url:
        return []
    if summary.get("is_derived"):
        return []          # 파생 summary는 '보고값'이 아님 — push 생략 (§4.1)
    ts_ms = int(datetime.fromisoformat(f"{date}T23:59:59+09:00").timestamp() * 1000)
    labels = (f'service_group="{_label_escape(entry.service_group)}",'
              f'service="{_label_escape(entry.service)}"')
    lines = [f"{name}{{{labels}}} {int(summary.get(key, 0) or 0)} {ts_ms}"
             for name, key in GAUGES]
    try:
        resp = session.post(f"{cfg.vm_push_url}/api/v1/import/prometheus",
                            data="\n".join(lines) + "\n", timeout=30)
        if resp.status_code >= 300:
            return [f"vm_push_failed: http {resp.status_code} (service={entry.service})"]
    except Exception as exc:
        return [f"vm_push_failed: {type(exc).__name__} (service={entry.service})"]
    return []
