from app.config import ServiceEntry
from app.normalize import check_identity, check_summary, normalize_records

ENTRY = ServiceEntry(service_group="G", service="S", base_url="http://x", enabled=True)


def R(**kw):
    base = {"userId": "u1", "userType": "identified", "model": "m",
            "inputTokens": 10, "outputTokens": 2, "requests": 1}
    base.update(kw)
    return base


def test_null_user_id_normalized_to_empty():
    out = normalize_records([R(userId=None, userType="unclassified")])
    assert out.rows[0].user_id == "" and out.rows[0].user_type == "unclassified"
    assert out.rejected == 0


def test_missing_cache_fields_default_zero():
    out = normalize_records([R()])
    assert out.rows[0].cache_read_tokens == 0 and out.rows[0].cache_creation_tokens == 0


def test_usertype_userid_contract_violations_rejected():
    bad = [
        R(userId=None, userType="identified"),      # identified인데 null
        R(userId="u2", userType="unclassified"),    # unclassified인데 문자열
        R(userType="alien"),                        # 미지 userType
        R(inputTokens=-1),                          # 음수
        R(inputTokens="ten"),                       # 타입 위반
        R(model=""),                                # 빈 model
    ]
    out = normalize_records(bad)
    assert out.rejected == 6 and out.rows == []
    assert len(out.warns) >= 1
    assert all("u2" not in w for w in out.warns)    # 로깅 계약: user_id 원문 금지


def test_duplicate_logical_key_sum_merged():
    out = normalize_records([R(inputTokens=10, requests=1), R(inputTokens=5, requests=2)])
    assert len(out.rows) == 1
    assert out.rows[0].input_tokens == 15 and out.rows[0].requests == 3
    assert out.merged_dups == 1


def test_totals_accumulate():
    out = normalize_records([R(), R(userId="u2", cacheReadTokens=7)])
    assert out.totals["input_tokens"] == 20
    assert out.totals["cache_read_tokens"] == 7
    assert out.totals["requests"] == 2


def test_check_identity_warns_on_drift():
    class P:
        reported_service_group = "G"
        reported_service = "S "          # 공백 드리프트
    warns = check_identity(ENTRY, P())
    assert len(warns) == 1 and "S " in warns[0]
    P.reported_service = "S"
    assert check_identity(ENTRY, P()) == []


def test_check_summary_warns_on_mismatch_and_skips_derived():
    totals = {"input_tokens": 10, "cache_read_tokens": 0, "cache_creation_tokens": 0,
              "output_tokens": 2, "requests": 1}
    ok = {"inputTokens": 10, "outputTokens": 2, "requests": 1}
    assert check_summary(totals, ok) == []
    bad = {"inputTokens": 11, "outputTokens": 2, "requests": 1}
    warns = check_summary(totals, bad)
    assert len(warns) == 1 and "inputTokens" in warns[0]
