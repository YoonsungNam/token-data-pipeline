#!/usr/bin/env python3
"""E2E 기대값 산출 — seed_metrics.build_seed(date)를 app.mart(T2)의 비용 함수와 M3/M4/M2 규칙(T4/T6/T7)으로
재계산해 `key=value` 9줄을 출력한다(줄마다 1개). run_e2e.sh가 셸 연관배열 EXP[...]에 담아
verify_expected_results.sql의 {EXP_*} 토큰을 sed로 치환한다 (Plan 6c T10).

    python3 tests/e2e/mart_expectations.py 2026-09-03
    EXP_M1_ROWS=4
    EXP_M1_QWEN_COST=201600.0000
    ...
    EXP_COVERAGE=2/3

정본 이원화 주의: 여기의 판정 규칙은 app/steps.py SQL의 파이썬 재현이다 — SQL 술어를 고치면 여기도 고친다
(어긋나면 verify_expected_results.sql expect-empty가 실패한다 — 그것이 이 파일의 존재 이유).

R1(controller ruling, scan-C N-5 / scan-B C4): provider_reported/all_services의 share·배분 계산은
app.mart.allocate_shared / provider_self_weight 를 그대로 호출한다(공식을 인라인으로 재구현하지 않는다).
C4(app/steps.py 헤더 ~:709, tests/test_mart.py::test_c4_*가 고정): provider_reported 모드의 분모
D = max(W(provider), Sigma_{s != provider} W(s)) — 제공자 자기분 = provider_self_weight(w_provider, others),
wt = {..consumers.., provider: 자기분} 로 만든 뒤 D = sum(wt.values()), allocate_shared(cost, wt)를 호출하면
Sigma share = 1, Sigma allocated = C 가 그대로 성립한다(I3).
R2(scan-B D3-secondary, T7에 반영): m2_rows는 SQL_M2의 unattributed = (flagged + other) × TCO를 그대로
따른다 — "other" = 비FAIL 행 중 category NOT IN (serving, standby, test).

## 알려진 SQL과의 차이(현재 시드에서는 0행이라 드러나지 않음) — fix1 Minor 8, 이 파일이 SQL의 참조
구현이 아니라 근사 미러임을 명시한다. 아래 네 항목은 이 시드로는 값이 갈리지 않아 드러나지 않지만,
시드를 확장하기 전에 먼저 SQL에 맞춰 정렬해야 한다(SQL이 정본, 이 파일이 틀렸으면 이 파일을 고친다):
  1. `m4_rows`의 파이썬 키는 `(model, service)`인데 SQL(`agg_token_model_share_1d`)의 grain은
     `(model, service, provider_service)`다 — `provider_ambiguous`처럼 같은 (model, service)에
     provider_service가 다른 행이 여러 개 나올 수 있는 경우 파이썬 dict가 마지막 행으로 덮어써 행을
     잃는다(이 시드는 provider_ambiguous가 0건이라 드러나지 않는다).
  2. `identity_drift`: 파이썬은 앵커 자신의 `service_group` 필드 + `source_type == 'metrics-api-v1'`
     필터로 비교하는데, SQL(app/steps.py:601-614, `_M3_IDENTITY_DRIFT`)은 앵커를 레지스트리(`SUB_REG`)에
     조인한 `r.service_group`과 비교한다 — 이 시드는 앵커/레지스트리 service_group이 항상 같은 값이라
     드러나지 않는다.
  3. `service_not_in_usage_registry`: 파이썬은 `reg_enabled - usage_svc` 집합 차만 보고, SQL은 그 위에
     `coverage_since <= d AND (until IS NULL OR d <= until)` 창을 추가로 건다 — 창 밖 서비스가 있으면
     파이썬이 과다 계상할 수 있다(이 시드는 전 레지스트리 행이 창 안이라 드러나지 않는다).
  4. `vendor_price_missing`: 파이썬은 이미 계산된 M4 `quality_flag == 'vendor_price_missing'`에서
     역산하는데, SQL은 `external_api` 모드에서 토큰-모델 조인으로 벤더 단가 부재를 직접 판정한다 —
     이 시드는 `external_api` 모드 자체가 0행이라 드러나지 않는다.
"""
import pathlib
import sys
from types import SimpleNamespace

HERE = pathlib.Path(__file__).resolve().parent            # mart/token-metrics/tests/e2e
sys.path.insert(0, str(HERE.parents[1]))                   # mart/token-metrics  → `from app import mart`
sys.path.insert(0, str(HERE))                              # tests/e2e           → `import seed_metrics`

from app import mart              # noqa: E402
import seed_metrics as sm         # noqa: E402

EXP_KEYS = ("EXP_M1_ROWS", "EXP_M1_QWEN_COST", "EXP_M3_FAIL_ROWS", "EXP_M3_WARN_ROWS", "EXP_M4_ROWS",
            "EXP_M4_QWEN_SUM", "EXP_M2_ROWS", "EXP_M2_IDLE_H100", "EXP_COVERAGE")
TOKEN_FIELDS = ("input_tokens", "cache_read_tokens", "cache_creation_tokens", "output_tokens", "requests")
# T4 core 13 + T6 stretch 3 + T7 stretch 4 = 20블록, severity는 steps.M3_BLOCKS_CORE + M3_BLOCKS_STRETCH와 동일
# (R6 — tests/test_e2e_seed.py::test_m3_severity_matches_steps_module이 app.steps 대조로 이 표를 고정한다)
M3_SEVERITY = {
    "metrics_missing": "FAIL", "partial_load": "FAIL", "rows_rejected": "WARN", "unregistered_model": "WARN",
    "hours_over_count": "FAIL", "unknown_violation": "FAIL", "pct_non_monotone": "FAIL",
    "gpu_type_no_tco": "WARN", "serving_missing_for_gpu_model": "WARN",
    "serving_without_gpu_serving_row": "WARN", "identity_drift": "WARN",
    "service_not_in_usage_registry": "WARN", "manual_source": "INFO",
    "provider_ambiguous": "WARN", "consumer_tokens_exceed_provider": "WARN", "vendor_price_missing": "WARN",
    "no_allocation": "WARN", "sum_hours_over_allocation": "FAIL",
    "gpu_block_empty_unexpected": "WARN", "serving_block_empty_unexpected": "WARN",
}
M2_HOURS_PER_DAY = 24.0


def _rows(seed, key):
    cols = sm.SEED_TABLES[key][1]
    return [dict(zip(cols, r)) for r in seed[key]]


def _canon(model: str) -> str:
    """T3 canon(x) = if(alias.canonical = \'\', x, alias.canonical)."""
    return sm.ALIASES.get(model, model)


def _ctx(seed) -> SimpleNamespace:
    """시드에서 파생 구조를 한 번만 만든다(T3 SUB_* 서브쿼리의 파이썬 대응)."""
    dated = [r for key in ("summary", "gpu", "serving", "token_usage") for r in _rows(seed, key)]
    d = dated[0]["date"]
    reg = {r["service"]: r for r in _rows(seed, "dim_metrics_service")}
    reg_enabled = {s for s, r in reg.items() if r["enabled"] == 1}
    expected = {s for s in reg_enabled
                if reg[s]["coverage_since"] <= d and (reg[s]["until"] is None or d <= reg[s]["until"])}
    usage_svc = {r["service"] for r in _rows(seed, "dim_token_service") if r["enabled"] == 1}
    anchors = {r["service"]: r for r in _rows(seed, "summary") if r["date"] == d}
    gpu_raw = [r for r in _rows(seed, "gpu") if r["date"] == d]
    serving_raw = [r for r in _rows(seed, "serving") if r["date"] == d]
    gpu = {}                                                   # (service, canon) -> [(category, gpu_type, hours, flags)]
    for r in gpu_raw:
        if r["service"] in anchors:
            gpu.setdefault((r["service"], _canon(r["model"])), []).append(
                (r["category"], r["gpu_type"], r["gpu_hours"], list(r["flags"])))
    serving = {}                                               # (service, canon) -> [row]
    for r in serving_raw:
        if r["service"] in anchors:
            serving.setdefault((r["service"], _canon(r["model"])), []).append(r)
    tokens = {}                                                # (service, canon) -> [input, cache_read, cache_creation, output, requests]
    for r in _rows(seed, "token_usage"):
        if r["date"] == d and r["service"] in usage_svc:
            acc = tokens.setdefault((r["service"], _canon(r["model"])), [0, 0, 0, 0, 0])
            for i, f in enumerate(TOKEN_FIELDS):
                acc[i] += r[f]
    tco = {k: v for k, v in sm.TCO_KRW.items()}                # 전 행 effective_from 2026-01-01 <= d
    alloc = {k: v for k, v in sm.ALLOCATION.items() if k[1] != "unknown"}   # SUB_EFF_ALLOC HAVING gpu_type != \'unknown\'
    group_of = {r["service"]: r["service_group"] for r in _rows(seed, "dim_token_service")}
    group_of.update({s: r["service_group"] for s, r in anchors.items()})
    return SimpleNamespace(d=d, reg=reg, reg_enabled=reg_enabled, expected=expected, usage_svc=usage_svc,
                           anchors=anchors, gpu_raw=gpu_raw, serving_raw=serving_raw, gpu=gpu, serving=serving,
                           tokens=tokens, tco=tco, alloc=alloc, group_of=group_of)


def _partial(c, svc: str) -> bool:
    """partial_load: 앵커 gpu_rows/serving_rows가 실제 fact 행수와 다르면 True(serving은 metric != \'custom\')."""
    a = c.anchors[svc]
    n_gpu = sum(1 for r in c.gpu_raw if r["service"] == svc)
    n_serving = sum(1 for r in c.serving_raw if r["service"] == svc and r["metric"] != "custom")
    return a["gpu_rows"] != n_gpu or a["serving_rows"] != n_serving


def m1_rows(seed) -> dict:
    """M1 agg_token_model_cost_1d — (service, canon) → {model_cost_krw, weighted_tokens, requests, has_*, quality_flag}."""
    c = _ctx(seed)
    out = {}
    for key in sorted(set(c.gpu) | set(c.tokens)):
        svc, _model = key
        gpu_rows = c.gpu.get(key, [])
        tok = c.tokens.get(key)
        cost = mart.model_cost(gpu_rows, c.tco)
        wt = mart.weighted_tokens(tok[0], tok[1], tok[2], tok[3]) if tok else 0.0
        anchor = c.anchors.get(svc)
        partial = anchor is not None and _partial(c, svc)
        no_tco = bool(gpu_rows) and cost is None
        flagged = any(mart.is_fail(flags) for _, _, _, flags in gpu_rows)
        manual = anchor is not None and anchor["source_type"] == sm.SOURCE_MANUAL
        no_metrics = svc in c.expected and anchor is None
        consumer_only = tok is not None and not gpu_rows
        out[key] = {
            "model_cost_krw": cost, "weighted_tokens": wt, "requests": tok[4] if tok else 0,
            "has_gpu_rows": int(bool(gpu_rows)), "has_token_rows": int(tok is not None),
            "quality_flag": mart.quality_flag_m1(partial, no_tco, flagged, manual, no_metrics, consumer_only),
        }
    return out


def _providers(c, model: str) -> list:
    """§6.4 (4) provider(m) = FAIL 없는 serving/standby gpu 행이 있는 (앵커) 서비스 — 정렬."""
    return sorted({svc for (svc, m), rows in c.gpu.items() if m == model
                   and any(cat in ("serving", "standby") and not mart.is_fail(flags) for cat, _, _, flags in rows)})


def _vendor_price(model: str):
    for (_provider, m), price in sm.VENDOR_PRICE.items():
        if m == model:
            return _provider, price
    return "", (None, None, None, None)


def m4_rows(seed) -> dict:
    """M4 agg_token_model_share_1d — (model, service) → {provider_service, is_provider, denominator_mode, share,
    allocated_cost_krw, quality_flag, model_cost_krw}. 모드 판정 순서 = SQL_M4 mode CTE(T6).

    R1: provider_reported/all_services의 share·배분은 app.mart.allocate_shared / provider_self_weight를
    그대로 호출한다(인라인 공식 재구현 금지). provider_reported 분모 D = max(W(provider), 나머지 합) -
    provider_self_weight(w_provider, others)로 제공자 자기분을 만든 wt에 allocate_shared(cost, wt)를
    호출하면 Sigma share = 1 / Sigma allocated = C가 그대로 성립한다(I3, C4).
    """
    c = _ctx(seed)
    m1 = m1_rows(seed)
    out = {}
    for model in sorted({m for _, m in m1}):
        providers = _providers(c, model)
        has_gpu = any(m == model for _, m in c.gpu)
        wtokens = {svc: m1[(svc, m)]["weighted_tokens"] for (svc, m) in m1 if m == model and m1[(svc, m)]["has_token_rows"]}
        w_all = sum(wtokens.values())
        provider = providers[0] if len(providers) == 1 else ""
        prov_m1 = m1.get((provider, model)) if provider else None
        cost = prov_m1["model_cost_krw"] if prov_m1 else None
        prov_quality = prov_m1["quality_flag"] if prov_m1 else ("no_tco" if provider else "")
        uic = bool(provider) and c.reg.get(provider, {}).get("usage_includes_consumers", 0) == 1
        w_prov = wtokens.get(provider, 0.0) if provider else 0.0
        others = sum(w for s, w in wtokens.items() if s != provider)

        # R1: provider_reported 전용 wt/D/배분 — provider_self_weight + allocate_shared(cost, wt)로만 계산.
        wt_pr: dict = {}
        d_pr = 0.0
        alloc_pr: dict = {}
        if uic:
            wt_pr = dict(wtokens)
            wt_pr[provider] = mart.provider_self_weight(w_prov, others)
            d_pr = sum(wt_pr.values())
            alloc_pr = mart.allocate_shared(cost, wt_pr) if cost is not None else {}

        # 모드 판정의 w_m: provider_reported 후보(uic)이면 D(= max(w_prov, others)), 아니면 w_all.
        w_m = d_pr if uic else w_all
        if len(providers) >= 2:
            mode = "provider_ambiguous"
        elif not providers and has_gpu:
            mode = "no_provider"
        elif not providers:
            mode = "external_api"
        elif w_m == 0 and (cost or 0.0) > 0:
            mode = "token_not_reported"
        elif uic:
            mode = "provider_reported"
        else:
            mode = "all_services"

        # all_services 전용 배분 — allocate_shared(cost, wtokens) 그대로(§6.4 (4)).
        alloc_all = mart.allocate_shared(cost, wtokens) if (mode == "all_services" and cost is not None) else {}

        vendor, price = _vendor_price(model) if mode == "external_api" else ("", (None, None, None, None))

        for svc in sorted(set(wtokens) | set(providers)):
            is_provider = int(svc in providers) if mode == "provider_ambiguous" else int(svc == provider)
            share = None
            allocated = None
            if mode == "provider_reported":
                w_s = wt_pr.get(svc, wtokens.get(svc, 0.0))
                share = (w_s / d_pr) if d_pr > 0 else None
                allocated = alloc_pr.get(svc)
            elif mode == "all_services":
                w_s = wtokens.get(svc, 0.0)
                share = (w_s / w_all) if w_all > 0 else None
                allocated = alloc_all.get(svc)
            else:
                w_s = wtokens.get(svc, 0.0)
                share = (w_s / w_m) if w_m > 0 else None

            if mode == "provider_ambiguous":
                share = None
            elif mode == "no_provider":
                allocated = 0.0
            elif mode == "external_api":
                tok = c.tokens.get((svc, model), [0, 0, 0, 0, 0])
                allocated = mart.external_api_cost(tok[0], tok[1], tok[2], tok[3], price)
            elif mode == "token_not_reported":
                share = 1.0 if is_provider else None
                allocated = cost if is_provider else None

            quality = ("partial" if prov_quality == "partial" else "no_tco" if prov_quality == "no_tco"
                       else "provider_ambiguous" if mode == "provider_ambiguous"
                       else "vendor_price_missing" if mode == "external_api" and allocated is None
                       else "token_not_reported" if mode == "token_not_reported" else "normal")
            provider_service = provider if provider else (vendor if mode == "external_api" else "")
            if mode == "provider_ambiguous" and is_provider:
                provider_service = svc
            out[(model, svc)] = {"provider_service": provider_service, "is_provider": is_provider,
                                 "denominator_mode": mode, "share": share, "allocated_cost_krw": allocated,
                                 "quality_flag": quality, "model_cost_krw": cost if mode != "no_provider" else 0.0}
    return out


def m2_rows(seed) -> dict:
    """M2 agg_token_gpu_group_1d — (service_group, gpu_type) → group_overhead(...) + reported/allocated/quality.
    행 집합 = 앵커 서비스의 gpu 행이 있는 그룹 UNION (unknown 아닌 할당 행 AND 그룹 내 앵커 서비스 >= 1).

    R2(scan-B D3-secondary, T7에 반영): SQL_M2는 non-FAIL 행 중 category가 {serving,standby,test}에
    속하지 않는 행("other")을 flagged 행과 합쳐 unattributed_cost_krw = (flagged + other) × TCO로 만든다
    (steps.SQL_M2의 other_gpu_hours 필드 + "(flagged_gpu_hours + gp.other_gpu_hours) * t.tco"). 여기서도
    "other" 버킷을 명시로 분리해 group_overhead()의 6번째 인자(flagged)에 flagged+other 합을 넘긴다 —
    row["flagged_gpu_hours"]는 FAIL 전용(적재되는 값)으로 그대로 둔다.
    """
    c = _ctx(seed)
    sums = {}
    for (svc, _m), rows in c.gpu.items():
        group = c.group_of[svc]
        for category, gpu_type, hours, flags in rows:
            acc = sums.setdefault((group, gpu_type),
                                  {"serving": 0.0, "standby": 0.0, "test": 0.0, "flagged": 0.0, "other": 0.0})
            if mart.is_fail(flags):
                acc["flagged"] += float(hours)
            elif category in ("serving", "standby", "test"):
                acc[category] += float(hours)
            else:
                acc["other"] += float(hours)
    anchor_groups = {c.group_of[s] for s in c.anchors}
    keys = set(sums) | {k for k, cnt in c.alloc.items() if cnt is not None and k[0] in anchor_groups}
    out = {}
    for key in sorted(keys):
        acc = sums.get(key, {"serving": 0.0, "standby": 0.0, "test": 0.0, "flagged": 0.0, "other": 0.0})
        count = c.alloc.get(key)
        allocated_hours = count * M2_HOURS_PER_DAY if count is not None else None
        reported = acc["serving"] + acc["standby"] + acc["test"] + acc["flagged"] + acc["other"]
        tco = c.tco.get(key[1])
        row = mart.group_overhead(allocated_hours, reported, acc["serving"], acc["standby"], acc["test"],
                                  acc["flagged"] + acc["other"], tco)
        row["allocated_gpu_hours"] = allocated_hours
        row["reported_gpu_hours_total"] = reported
        row["flagged_gpu_hours"] = acc["flagged"]
        row["tco_missing"] = int(tco is None)
        # T7-2 우선순위: over_report > no_tco > no_allocation > flagged > normal
        row["quality_flag"] = ("over_report" if row["over_report"] == 1 else "no_tco" if tco is None
                               else "no_allocation" if allocated_hours is None
                               else "flagged" if acc["flagged"] > 0 else "normal")
        out[key] = row
    return out


def m3_counts(seed) -> dict:
    """M3 token_metrics_check_1d — 20블록 이름 → 기대 행수(T4/T6/T7 술어의 파이썬 재현).

    R3(D-1): 테이블명은 token_metrics_check_1d(check_token_metrics_1d 아님) — 이 docstring이 정본.
    """
    c = _ctx(seed)
    n = {name: 0 for name in M3_SEVERITY}
    fact_svcs = {r["service"] for r in c.gpu_raw} | {r["service"] for r in c.serving_raw}
    n["metrics_missing"] = len([s for s in c.expected if s not in c.anchors])
    n["partial_load"] = (sum(1 for s in c.anchors if _partial(c, s)) + len(fact_svcs - set(c.anchors)))
    n["rows_rejected"] = sum(1 for a in c.anchors.values() if a["rejected_rows"] > 0)
    # B3(M-2) — SQL(_M3_UNREGISTERED_MODEL, app/steps.py)은 raw_token_metrics_gpu_1d_dist(gpu
    # 팩트)만, 앵커 있는 서비스로 스캔한다. serving/token_usage까지 넓게 보던 이전 미러는 그
    # 둘에만 있는 미등록 모델을 SQL이 세지 않는데도 세는 다섯 번째(문서화 안 된) 발산이었다 —
    # 현재 시드는 모든 모델이 ALIASES에 있어 양쪽 다 0이라 드러나지 않았다. SQL 스코프로 좁힌다.
    models_seen = {(r["service"], r["model"]) for r in c.gpu_raw if r["service"] in c.anchors}
    n["unregistered_model"] = len({k for k in models_seen if k[1] not in sm.ALIASES})
    for flag in ("hours_over_count", "unknown_violation"):
        n[flag] = len({(r["service"], _canon(r["model"]), r["gpu_type"]) for r in c.gpu_raw
                       if r["service"] in c.anchors and flag in r["flags"]})
    n["pct_non_monotone"] = sum(1 for r in c.serving_raw if r["service"] in c.anchors and "pct_non_monotone" in r["flags"])
    n["gpu_type_no_tco"] = len({(svc, gpu_type) for (svc, _m), rows in c.gpu.items()
                                for cat, gpu_type, _h, flags in rows
                                if cat in ("serving", "standby") and not mart.is_fail(flags)
                                and c.tco.get(gpu_type) is None})
    for key, rows in c.gpu.items():
        has_serving_gpu = any(cat == "serving" for cat, _g, _h, _f in rows)
        requests = c.tokens.get(key, [0, 0, 0, 0, 0])[4]
        if has_serving_gpu and key not in c.serving and requests > 0:
            n["serving_missing_for_gpu_model"] += 1
    for key in c.serving:
        has_serving_gpu = any(cat == "serving" for cat, _g, _h, _f in c.gpu.get(key, []))
        if not has_serving_gpu and c.reg.get(key[0], {}).get("expect_gpu", 0) == 1:
            n["serving_without_gpu_serving_row"] += 1
    n["identity_drift"] = sum(1 for a in c.anchors.values() if a["source_type"] == sm.SOURCE_API
                              and (a["reported_service"] != a["service"]
                                   or a["reported_service_group"] != a["service_group"]))
    n["service_not_in_usage_registry"] = len(c.reg_enabled - c.usage_svc)
    n["manual_source"] = sum(1 for a in c.anchors.values() if a["source_type"] == sm.SOURCE_MANUAL)
    m4 = m4_rows(seed)
    models = {m for m, _s in m4}
    n["provider_ambiguous"] = sum(1 for m in models if len(_providers(c, m)) >= 2)
    for m in models:
        rows = [r for (mm, _s), r in m4.items() if mm == m]
        if rows and rows[0]["denominator_mode"] == "provider_reported":
            provider = rows[0]["provider_service"]
            m1 = m1_rows(seed)
            w_prov = m1.get((provider, m), {"weighted_tokens": 0.0})["weighted_tokens"]
            others = sum(m1[(s, mm)]["weighted_tokens"] for (s, mm) in m1 if mm == m and s != provider)
            n["consumer_tokens_exceed_provider"] += int(others > w_prov)
    n["vendor_price_missing"] = sum(1 for r in m4.values() if r["quality_flag"] == "vendor_price_missing")
    m2 = m2_rows(seed)
    for key, row in m2.items():
        has_gpu_rows = any(c.group_of[svc] == key[0] and gpu_type == key[1]
                           for (svc, _m), rows in c.gpu.items() for _c, gpu_type, _h, _f in rows)
        if has_gpu_rows and row["allocated_gpu_hours"] is None:
            n["no_allocation"] += 1
        if row["allocated_gpu_hours"] is not None and row["reported_gpu_hours_total"] > row["allocated_gpu_hours"]:
            n["sum_hours_over_allocation"] += 1
    for svc, a in c.anchors.items():
        reg = c.reg.get(svc, {})
        n["gpu_block_empty_unexpected"] += int(a["gpu_rows"] == 0 and reg.get("expect_gpu", 0) == 1)
        n["serving_block_empty_unexpected"] += int(a["serving_rows"] == 0 and reg.get("expect_serving", 0) == 1)
    return n


def expect(date: str) -> dict:
    """run_e2e.sh 계약 — EXP_KEYS 순서의 9키."""
    seed = sm.build_seed(date)
    c = _ctx(seed)
    m1, m3, m4, m2 = m1_rows(seed), m3_counts(seed), m4_rows(seed), m2_rows(seed)
    coverage = mart.compute_coverage(c.expected, set(c.anchors), [])
    return {
        "EXP_M1_ROWS": len(m1),
        "EXP_M1_QWEN_COST": m1[(sm.SVC_A, sm.MODEL_QWEN)]["model_cost_krw"],
        "EXP_M3_FAIL_ROWS": sum(v for k, v in m3.items() if M3_SEVERITY[k] == "FAIL"),
        "EXP_M3_WARN_ROWS": sum(v for k, v in m3.items() if M3_SEVERITY[k] == "WARN"),
        "EXP_M4_ROWS": len(m4),
        "EXP_M4_QWEN_SUM": sum(r["allocated_cost_krw"] for (m, _s), r in m4.items()
                               if m == sm.MODEL_QWEN and r["allocated_cost_krw"] is not None),
        "EXP_M2_ROWS": len(m2),
        "EXP_M2_IDLE_H100": m2[(sm.SERVICE_GROUP, "H100")]["idle_gpu_hours"],
        "EXP_COVERAGE": f"{coverage.present}/{coverage.enabled}",
    }


def _fmt(value) -> str:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"unexpected expectation value: {value!r}")
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def main(argv=None) -> int:
    args = sys.argv[1:] if argv is None else list(argv)
    if len(args) != 1:
        print("usage: mart_expectations.py <YYYY-MM-DD>", file=sys.stderr)
        return 2
    for key, value in expect(args[0]).items():
        print(f"{key}={_fmt(value)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
