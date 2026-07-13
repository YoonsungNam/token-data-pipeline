from app.config import Config, ServiceEntry
from app.vm_push import push_service_summary

ENTRY = ServiceEntry(service_group="G", service="S", base_url="http://x", enabled=True)
SUMMARY = {"inputTokens": 10, "cacheReadTokens": 1, "cacheCreationTokens": 2,
           "outputTokens": 3, "requests": 4, "distinctUsers": 5}


class FakeSession:
    def __init__(self, status=204):
        self.posts = []
        self.status = status

    def post(self, url, data=None, timeout=None):
        self.posts.append((url, data))
        class R:
            status_code = self.status
        return R()


def test_push_skipped_when_url_empty():
    s = FakeSession()
    warns = push_service_summary(Config(vm_push_url=""), ENTRY, "2026-06-15", SUMMARY, s)
    assert warns == [] and s.posts == []


def test_push_lines_and_timestamp():
    s = FakeSession()
    warns = push_service_summary(Config(vm_push_url="http://vm:8480"), ENTRY,
                                 "2026-06-15", SUMMARY, s)
    assert warns == []
    url, data = s.posts[0]
    assert url == "http://vm:8480/api/v1/import/prometheus"
    lines = data.strip().split("\n")
    assert len(lines) == 6
    assert 'token_usage_daily_input_tokens{service_group="G",service="S"} 10' in lines[0]
    # 타임스탬프 = 2026-06-15 23:59:59 KST = 2026-06-15T14:59:59Z → epoch ms
    assert lines[0].endswith(" 1781535599000")
    assert any("reported_distinct_users" in ln and " 5 " in ln for ln in lines)


def test_push_failure_is_warn_not_raise():
    s = FakeSession(status=500)
    warns = push_service_summary(Config(vm_push_url="http://vm:8480"), ENTRY,
                                 "2026-06-15", SUMMARY, s)
    assert len(warns) == 1 and "vm_push_failed" in warns[0]


def test_push_skipped_when_summary_is_derived():
    s = FakeSession()
    warns = push_service_summary(Config(vm_push_url="http://vm:8480"), ENTRY,
                                 "2026-06-15", {**SUMMARY, "is_derived": 1}, s)
    assert warns == [] and s.posts == []
