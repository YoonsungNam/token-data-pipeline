"""ClickHouse 멱등 적재 (§5.1-3-5).

시퀀스: 존재 SELECT → (있으면) 감사 append + DELETE(mutations_sync=2, _local[+ON CLUSTER])
→ 배치 INSERT(insert_distributed_sync=1). DB명은 §9-18 협의 변경 지점 — 아래 상수 2개만 수정.
"""
from datetime import datetime, timedelta, timezone

import clickhouse_connect

from app.config import Config, ServiceEntry

DB_FACT = "token_fact"   # §9-18: 공유 fact DB 확정 시 "fact"로 변경
DB_DIM = "gpu_data"      # 이슈 #1 확정 — 접두사 여부는 §9-18 잔여 협의

KST = timezone(timedelta(hours=9))

DETAIL_COLS = ("date", "service_group", "service", "reported_service_group",
               "reported_service", "user_id", "user_type", "model", "input_tokens",
               "cache_read_tokens", "cache_creation_tokens", "output_tokens",
               "requests", "generated_at", "collected_at")
SUMMARY_COLS = ("date", "service_group", "service", "reported_service_group",
                "reported_service", "input_tokens", "cache_read_tokens",
                "cache_creation_tokens", "output_tokens", "requests", "distinct_users",
                "distinct_identified_users", "is_derived", "generated_at", "collected_at")
AUDIT_COLS = ("date", "service", "prev_generated_at", "prev_collected_at",
              "prev_input_tokens", "prev_cache_read_tokens", "prev_cache_creation_tokens",
              "prev_output_tokens", "prev_requests", "prev_row_count", "replaced_at")
DIM_COLS = ("service_group", "service", "base_url", "enabled", "source_type",
            "note", "updated_at")


def now_kst_naive() -> datetime:
    """CH DateTime('Asia/Seoul') 컬럼용 — KST 벽시계, tzinfo 제거."""
    return datetime.now(KST).replace(tzinfo=None)


class CHWriter:
    def __init__(self, cfg: Config, client=None):
        self.cfg = cfg
        self.client = client or clickhouse_connect.get_client(
            host=cfg.ch_host, port=cfg.ch_port, username=cfg.ch_user,
            password=cfg.ch_password, settings={"insert_distributed_sync": 1})

    def _on_cluster(self) -> str:
        return f" ON CLUSTER '{self.cfg.ch_cluster}'" if self.cfg.ch_cluster else ""

    def _exists(self, table_dist: str, date: str, service: str) -> bool:
        r = self.client.query(
            f"SELECT count() FROM {table_dist} WHERE date = %(d)s AND service = %(s)s",
            parameters={"d": date, "s": service})
        return bool(r.result_rows and r.result_rows[0][0])

    def _delete_day(self, table_local: str, date: str, service: str) -> None:
        self.client.command(
            f"ALTER TABLE {table_local}{self._on_cluster()} "
            f"DELETE WHERE date = %(d)s AND service = %(s)s",
            parameters={"d": date, "s": service},
            settings={"mutations_sync": 2})

    def fetch_prev_summary(self, service: str, date: str) -> dict | None:
        """교체 전 세대 요약 — 감사(§8.4)용. summary 행 존재를 앵커로 사용
        (NODATA 세대는 detail 0행 + summary 1행 — 이 경우도 감사 대상)."""
        s = self.client.query(
            f"SELECT generated_at, collected_at "
            f"FROM {DB_FACT}.raw_token_usage_summary_1d_dist "
            f"WHERE date = %(d)s AND service = %(s)s "
            f"ORDER BY collected_at DESC LIMIT 1",
            parameters={"d": date, "s": service})
        if not s.result_rows:
            return None
        gen, col = s.result_rows[0]
        d = self.client.query(
            f"SELECT count(), sum(input_tokens), sum(cache_read_tokens), "
            f"sum(cache_creation_tokens), sum(output_tokens), sum(requests) "
            f"FROM {DB_FACT}.raw_token_usage_1d_dist "
            f"WHERE date = %(d)s AND service = %(s)s",
            parameters={"d": date, "s": service})
        c, i, cr, cc, o, q = (d.result_rows[0] if d.result_rows else (0, 0, 0, 0, 0, 0))
        return {"prev_row_count": c, "prev_input_tokens": i or 0,
                "prev_cache_read_tokens": cr or 0, "prev_cache_creation_tokens": cc or 0,
                "prev_output_tokens": o or 0, "prev_requests": q or 0,
                "prev_generated_at": gen, "prev_collected_at": col}

    def replace_service_day(self, entry: ServiceEntry, date: str, rows_iter,
                            summary_row: dict, audit_prev: dict | None) -> int:
        detail_dist = f"{DB_FACT}.raw_token_usage_1d_dist"
        detail_local = f"{DB_FACT}.raw_token_usage_1d_local"
        summary_dist = f"{DB_FACT}.raw_token_usage_summary_1d_dist"
        summary_local = f"{DB_FACT}.raw_token_usage_summary_1d_local"
        if self._exists(detail_dist, date, entry.service) or \
           self._exists(summary_dist, date, entry.service):
            if audit_prev:
                self.client.insert(
                    f"{DB_FACT}.collect_audit_1d_dist",
                    [[date, entry.service, audit_prev["prev_generated_at"],
                      audit_prev["prev_collected_at"], audit_prev["prev_input_tokens"],
                      audit_prev["prev_cache_read_tokens"],
                      audit_prev["prev_cache_creation_tokens"],
                      audit_prev["prev_output_tokens"], audit_prev["prev_requests"],
                      audit_prev["prev_row_count"], now_kst_naive()]],
                    column_names=AUDIT_COLS)
            self._delete_day(detail_local, date, entry.service)
            self._delete_day(summary_local, date, entry.service)

        collected_at = now_kst_naive()
        total = 0
        buf: list[list] = []

        def flush():
            nonlocal buf
            if buf:
                self.client.insert(detail_dist, buf, column_names=DETAIL_COLS)
                buf = []

        for row in rows_iter:
            buf.append([date, entry.service_group, entry.service,
                        summary_row["reported_service_group"],
                        summary_row["reported_service"], row.user_id, row.user_type,
                        row.model, row.input_tokens, row.cache_read_tokens,
                        row.cache_creation_tokens, row.output_tokens, row.requests,
                        summary_row["generated_at"], collected_at])
            total += 1
            if len(buf) >= self.cfg.max_buffer_rows:   # §5.1 메모리 규칙
                flush()
        flush()

        self.client.insert(
            summary_dist,
            [[date, entry.service_group, entry.service,
              summary_row["reported_service_group"], summary_row["reported_service"],
              summary_row["input_tokens"], summary_row["cache_read_tokens"],
              summary_row["cache_creation_tokens"], summary_row["output_tokens"],
              summary_row["requests"], summary_row["distinct_users"],
              summary_row["distinct_identified_users"], summary_row["is_derived"],
              summary_row["generated_at"], collected_at]],
            column_names=SUMMARY_COLS)
        return total

    def replace_dim_services(self, entries: list[ServiceEntry],
                             source_type: str = "usage-api-v1") -> None:
        """자기 source_type 범위만 원자 교체 (§5.9 계약 6조 — 타 모듈 등록분 보호)."""
        self.client.command(
            f"ALTER TABLE {DB_DIM}.dim_service_local{self._on_cluster()} "
            f"DELETE WHERE source_type = %(stype)s",
            parameters={"stype": source_type},
            settings={"mutations_sync": 2})
        now = now_kst_naive()
        self.client.insert(
            f"{DB_DIM}.dim_service_dist",
            [[e.service_group, e.service, e.base_url, 1 if e.enabled else 0,
              e.source_type, "", now] for e in entries],
            column_names=DIM_COLS)
