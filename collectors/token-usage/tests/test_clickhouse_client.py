from app.clickhouse_client import CHWriter, DB_FACT
from app.config import Config, ServiceEntry
from app.normalize import NormalizedRow

ENTRY = ServiceEntry(service_group="G", service="S", base_url="http://x", enabled=True)
DATE = "2026-06-15"


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeCH:
    def __init__(self, existing_count=0):
        self.commands = []      # (sql, parameters)
        self.inserts = []       # (table, row_count, column_names)
        self.insert_rows = []   # (table, data) for detailed inspection
        self.existing_count = existing_count

    def command(self, sql, parameters=None, settings=None):
        self.commands.append((" ".join(sql.split()), parameters, settings))

    def query(self, sql, parameters=None):
        if "count()" in sql:
            return FakeResult([[self.existing_count]])
        return FakeResult([])

    def insert(self, table, data, column_names=None):
        self.inserts.append((table, len(data), tuple(column_names or ())))
        self.insert_rows.append((table, data))


class TableAwareFakeCH(FakeCH):
    """summary 존재/부재와 detail 합계를 테이블별로 분리 응답."""

    def __init__(self, summary_row=None, detail_agg=(0, 0, 0, 0, 0, 0)):
        super().__init__()
        self.summary_row = summary_row
        self.detail_agg = detail_agg

    def query(self, sql, parameters=None):
        if "raw_token_usage_summary_1d_dist" in sql and "generated_at" in sql:
            return FakeResult([list(self.summary_row)] if self.summary_row else [])
        if "count()" in sql:
            return FakeResult([list(self.detail_agg)])
        return FakeResult([])


def rows(n):
    return (NormalizedRow(user_id=f"u{i}", user_type="identified", model="m",
                          input_tokens=1, cache_read_tokens=0, cache_creation_tokens=0,
                          output_tokens=1, requests=1) for i in range(n))


def summary_row():
    return {"input_tokens": 3, "cache_read_tokens": 0, "cache_creation_tokens": 0,
            "output_tokens": 3, "requests": 3, "distinct_users": 3,
            "distinct_identified_users": None, "is_derived": 0,
            "generated_at": "2026-06-16 02:05:00", "reported_service_group": "G",
            "reported_service": "S"}


def test_first_load_skips_delete_and_audit():
    ch = FakeCH(existing_count=0)
    w = CHWriter(Config(max_buffer_rows=10), client=ch)
    n = w.replace_service_day(ENTRY, DATE, rows(3), summary_row(), audit_prev=None)
    assert n == 3
    assert not any("DELETE" in c[0] for c in ch.commands)          # no-op 스킵 (§4.0)
    detail = [i for i in ch.inserts if i[0].endswith("raw_token_usage_1d_dist")]
    assert sum(i[1] for i in detail) == 3
    assert any(i[0].endswith("raw_token_usage_summary_1d_dist") for i in ch.inserts)
    assert not any(i[0].endswith("collect_audit_1d_dist") for i in ch.inserts)


def test_reload_deletes_with_mutations_sync_and_audits():
    ch = FakeCH(existing_count=5)
    w = CHWriter(Config(ch_cluster="gpu-monitoring", max_buffer_rows=10), client=ch)
    prev = {"prev_row_count": 5, "prev_input_tokens": 9, "prev_cache_read_tokens": 0,
            "prev_cache_creation_tokens": 0, "prev_output_tokens": 1, "prev_requests": 5,
            "prev_generated_at": "2026-06-16 02:05:00",
            "prev_collected_at": "2026-06-16 02:10:00"}
    w.replace_service_day(ENTRY, DATE, rows(2), summary_row(), audit_prev=prev)
    deletes = [c for c in ch.commands if "DELETE" in c[0]]
    assert len(deletes) == 2                                       # detail + summary
    assert all("_local" in c[0] for c in deletes)
    assert all("ON CLUSTER" in c[0] for c in deletes)
    assert all(c[2] and c[2].get("mutations_sync") == 2 for c in deletes)
    assert any(i[0].endswith("collect_audit_1d_dist") for i in ch.inserts)


def test_no_on_cluster_when_cluster_empty():
    ch = FakeCH(existing_count=1)
    w = CHWriter(Config(ch_cluster="", max_buffer_rows=10), client=ch)
    w.replace_service_day(ENTRY, DATE, rows(1), summary_row(), audit_prev=None)
    deletes = [c for c in ch.commands if "DELETE" in c[0]]
    assert deletes and all("ON CLUSTER" not in c[0] for c in deletes)


def test_buffer_flush_batches():
    ch = FakeCH(existing_count=0)
    w = CHWriter(Config(max_buffer_rows=4), client=ch)
    n = w.replace_service_day(ENTRY, DATE, rows(10), summary_row(), audit_prev=None)
    assert n == 10
    detail = [i for i in ch.inserts if i[0].endswith("raw_token_usage_1d_dist")]
    assert [i[1] for i in detail] == [4, 4, 2]                     # MAX_BUFFER_ROWS flush


def test_dim_replace_scopes_to_source_type():
    ch = FakeCH()
    w = CHWriter(Config(ch_cluster="gpu-monitoring"), client=ch)
    w.replace_dim_services([ENTRY])
    deletes = [c for c in ch.commands if "DELETE" in c[0]]
    assert len(deletes) == 1
    assert "source_type" in deletes[0][0] and "dim_token_service_local" in deletes[0][0]
    assert deletes[0][1] == {"stype": "usage-api-v1"}
    assert any(i[0].endswith("dim_token_service_dist") for i in ch.inserts)


def test_fetch_prev_summary_covers_nodata_generation():
    ch = TableAwareFakeCH(summary_row=("2026-06-16 02:05:00", "2026-06-16 02:10:00"),
                          detail_agg=(0, 0, 0, 0, 0, 0))
    w = CHWriter(Config(), client=ch)
    prev = w.fetch_prev_summary("S", DATE)
    assert prev is not None and prev["prev_row_count"] == 0
    assert prev["prev_generated_at"] == "2026-06-16 02:05:00"
    ch2 = TableAwareFakeCH(summary_row=None)
    assert CHWriter(Config(), client=ch2).fetch_prev_summary("S", DATE) is None


def test_dim_rows_carry_entry_source_type():
    ch = FakeCH()
    w = CHWriter(Config(), client=ch)
    entries = [ENTRY,
               ServiceEntry(service_group="G", service="S3", base_url="http://c",
                            enabled=True, source_type="snapshot-api")]
    w.replace_dim_services(entries)
    table, n, cols = ch.inserts[-1]
    assert table.endswith("dim_token_service_dist") and n == 2
    dim_data = ch.insert_rows[-1][1]
    assert dim_data[0][4] == "usage-api-v1"   # first entry source_type
    assert dim_data[1][4] == "snapshot-api"   # second entry source_type
