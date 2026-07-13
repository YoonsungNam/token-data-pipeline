from app.config import Config
from app.datagen import build_records, build_summary, generated_at, to_api_dict

CFG = Config(users=5, anon_users=2, models=["m-a", "m-b"], seed="t")
DATE = "2026-06-15"


def test_deterministic_and_stable_order():
    a, b = build_records(CFG, DATE), build_records(CFG, DATE)
    assert a == b and len(a) > 0


def test_different_date_differs():
    assert build_records(CFG, DATE) != build_records(CFG, "2026-06-16")


def test_user_type_rules():
    recs = build_records(CFG, DATE)
    for r in recs:
        if r.user_type in ("identified", "anonymous"):
            assert isinstance(r.user_id, str) and r.user_id
        else:
            assert r.user_type == "unclassified" and r.user_id is None
    # unclassified에 model='unknown' 행이 정확히 1개 존재 (계약 예시 형태)
    unknown = [r for r in recs if r.user_type == "unclassified" and r.model == "unknown"]
    assert len(unknown) == 1
    # 논리 키 (user_id, user_type, model) 중복 없음 — 계약의 사전 집계 전제
    keys = [(r.user_id, r.user_type, r.model) for r in recs]
    assert len(keys) == len(set(keys))


def test_non_negative_and_summary_matches_detail():
    recs = build_records(CFG, DATE)
    s = build_summary(recs)
    assert s["inputTokens"] == sum(r.input_tokens for r in recs)
    assert s["cacheReadTokens"] == sum(r.cache_read_tokens for r in recs)
    assert s["outputTokens"] == sum(r.output_tokens for r in recs)
    assert s["requests"] == sum(r.requests for r in recs)
    ids = {r.user_id for r in recs if r.user_id is not None}
    assert s["distinctUsers"] == len(ids)
    assert s["distinctIdentifiedUsers"] == len(
        {r.user_id for r in recs if r.user_type == "identified"}
    )
    assert all(
        min(r.input_tokens, r.cache_read_tokens, r.cache_creation_tokens,
            r.output_tokens, r.requests) >= 0
        for r in recs
    )


def test_api_dict_omits_zero_cache_fields():
    recs = build_records(CFG, DATE)
    zero_cache = next(r for r in recs if r.cache_read_tokens == 0)
    d = to_api_dict(zero_cache)
    assert "cacheReadTokens" not in d
    assert d["userId"] == zero_cache.user_id and d["userType"] == zero_cache.user_type
    nonzero = next(r for r in recs if r.cache_read_tokens > 0)
    assert to_api_dict(nonzero)["cacheReadTokens"] == nonzero.cache_read_tokens


def test_generated_at_kst_format():
    assert generated_at("2026-06-15") == "2026-06-16T02:05:00+09:00"


def test_unknown_as_first_model_does_not_duplicate_keys():
    cfg = Config(users=1, anon_users=0, models=["unknown", "m-b"], seed="t")
    recs = build_records(cfg, DATE)
    keys = [(r.user_id, r.user_type, r.model) for r in recs]
    assert len(keys) == len(set(keys))
    unknown = [r for r in recs if r.user_type == "unclassified" and r.model == "unknown"]
    assert len(unknown) == 1
