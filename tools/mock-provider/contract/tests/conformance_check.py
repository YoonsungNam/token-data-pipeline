#!/usr/bin/env python3
"""
셀프 점검 스크립트 — 추론 서비스(프로바이더)가 **자기 구현 서버에 직접 돌려** 계약 준수를
확인한다. 만든 뒤 검사용이자, 만드는 중 통과 기준(자기 테스트)으로도 쓴다.

스키마 적합성(OpenAPI + if/then)뿐 아니라 스키마로 표현 못 하는 불변식까지 검사한다:
  - 모든 행에 model 존재(비어있지 않음)
  - userType↔userId 규칙 (identified/anonymous=문자열, unclassified=null)
  - (userId, userType, model) 는 한 날짜 안에서 유일
  - 페이지네이션: nextCursor 를 끝까지 따라가며 종료
  - generatedAt 은 KST(+09:00)  (스키마 pattern 으로 검증)
  - summary 토큰 합 == detail 전체 행 합
  - distinctUsers == detail 의 고유 userId 수 (identified+anonymous, unclassified 제외)
  - distinctIdentifiedUsers(옵션) == detail 의 identified 고유 userId 수 (제공 시에만)
  - 미래 date → 400, 잘못된 cursor → 400

사용법:
  # 자기 서버 셀프 점검
  python tests/conformance_check.py --base-url https://my-svc.internal --date 2026-06-15
  # (환경변수도 가능: BASE_URL, DATE)

  # 서버 없이 스크립트 자체 동작 확인(스펙 예시 사용)
  python tests/conformance_check.py --demo

요구: pip install jsonschema pyyaml
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

import yaml

try:
    from jsonschema import Draft202012Validator as Validator
except Exception:
    from jsonschema import Draft7Validator as Validator

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = os.path.join(HERE, "..", "token-usage-api.yaml")

results = []


def record(case_id, ok, detail=""):
    results.append((case_id, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {case_id}" + (f" — {detail}" if detail else ""))


def load_validators():
    schemas = yaml.safe_load(open(SPEC, encoding="utf-8"))["components"]["schemas"]

    def build(name):
        s = dict(schemas[name])
        s["$defs"] = schemas
        return json.loads(json.dumps(s).replace("#/components/schemas/", "#/$defs/"))

    return schemas, {
        "page": Validator(build("UsagePage")),
        "summary": Validator(build("UsageSummary")),
    }


def schema_errors(validator, obj):
    out = []
    for e in validator.iter_errors(obj):
        loc = "/".join(str(p) for p in e.absolute_path) or "(root)"
        out.append(f"{loc}: {e.message}")
    return out


# ---- 불변식 검사 -----------------------------------------------------------

def _brief(r):
    return json.dumps({k: r.get(k) for k in ("userId", "userType", "model")},
                      ensure_ascii=False)


def check_records(records, label):
    seen, bad_model, bad_uid, dup = {}, [], [], None
    for i, r in enumerate(records):
        m = r.get("model")
        if not isinstance(m, str) or not m:
            bad_model.append((i, r))
        ut, uid = r.get("userType"), r.get("userId")
        if ut in ("identified", "anonymous"):
            if not isinstance(uid, str):
                bad_uid.append((i, r))
        elif ut == "unclassified":
            if uid is not None:
                bad_uid.append((i, r))
        key = (uid, ut, m)
        if key in seen:
            dup = dup or (key, seen[key], i)
        else:
            seen[key] = i

    def ex(rows):
        i, r = rows[0]
        return f"{len(rows)} row(s); e.g. row[{i}] {_brief(r)}"

    record(f"C2 model present/non-empty on every row [{label}]",
           not bad_model, ex(bad_model) if bad_model else "")
    record(f"C3 userType<->userId rule [{label}]",
           not bad_uid, ex(bad_uid) if bad_uid else "")
    record(f"C4 (userId,userType,model) unique [{label}]",
           dup is None, f"dup key {dup[0]} at rows {dup[1]} & {dup[2]}" if dup else "")


def _sum(records, field):
    return sum(int(r.get(field, 0) or 0) for r in records)


def check_summary_consistency(records, summary):
    ok, detail = True, []
    for f in ("inputTokens", "outputTokens", "cacheReadTokens",
              "cacheCreationTokens", "requests"):
        s_val, d_val = int(summary.get(f, 0) or 0), _sum(records, f)
        if s_val != d_val:
            ok = False
            detail.append(f"{f}: summary={s_val} detail={d_val}")
    record("C8 summary tokens == sum(detail rows)", ok, "; ".join(detail))

    distinct = len({r["userId"] for r in records
                    if r.get("userType") in ("identified", "anonymous")
                    and isinstance(r.get("userId"), str)})
    s_distinct = summary.get("distinctUsers")
    record("C9 distinctUsers == distinct userIds (identified+anonymous)",
           distinct == s_distinct, f"summary={s_distinct} detail={distinct}")

    # C9b: distinctIdentifiedUsers (optional) — 제공된 경우에만 검사
    if "distinctIdentifiedUsers" in summary:
        distinct_id = len({r["userId"] for r in records
                           if r.get("userType") == "identified" and isinstance(r.get("userId"), str)})
        s_id = summary.get("distinctIdentifiedUsers")
        record("C9b distinctIdentifiedUsers == distinct identified userIds (optional)",
               distinct_id == s_id, f"summary={s_id} detail={distinct_id}")


# ---- HTTP ------------------------------------------------------------------

def http_get(url):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8"))
        except Exception:
            return e.code, None
    except Exception as e:  # noqa: BLE001
        return None, {"_error": str(e)}


def fetch_all_pages(base_url, date, v):
    records, cursor, pages, ok = [], None, 0, True
    while True:
        pages += 1
        if pages > 10000:
            record("C5 pagination terminates", False, "exceeded 10000 pages")
            return None
        url = f"{base_url}/v1/usage?date={urllib.parse.quote(date)}"
        if cursor:
            url += f"&cursor={urllib.parse.quote(cursor)}"
        status, body = http_get(url)
        if status != 200 or not isinstance(body, dict):
            record("C1 GET /v1/usage 200 + page schema valid", False, f"status={status}")
            return None
        errs = schema_errors(v["page"], body)
        if errs:
            record("C1 GET /v1/usage 200 + page schema valid", False, "; ".join(errs[:3]))
            return None
        records.extend(body.get("records", []))
        cursor = body.get("nextCursor")
        if not cursor:
            break
    record("C1 GET /v1/usage 200 + page schema valid (incl. generatedAt KST)", ok,
           f"{pages} page(s)")
    record("C5 pagination terminates (nextCursor 끝까지)", True, f"{len(records)} rows")
    return records


def run_live(base_url, date):
    _, v = load_validators()
    base_url = base_url.rstrip("/")

    records = fetch_all_pages(base_url, date, v)
    if records is not None:
        check_records(records, "live")

    status, summary = http_get(f"{base_url}/v1/usage/summary?date={urllib.parse.quote(date)}")
    if status == 200 and isinstance(summary, dict):
        errs = schema_errors(v["summary"], summary)
        record("C7 GET /v1/usage/summary 200 + schema valid", not errs, "; ".join(errs[:3]))
        if not errs and records is not None:
            check_summary_consistency(records, summary)
    else:
        record("C7 GET /v1/usage/summary 200 + schema valid", False, f"status={status}")

    status, _ = http_get(f"{base_url}/v1/usage?date=2999-12-31")
    record("C10 future date -> 400", status == 400, f"status={status}")

    status, body = http_get(
        f"{base_url}/v1/usage?date={urllib.parse.quote(date)}&cursor=__not_a_valid_cursor__")
    record("C11 invalid cursor -> 400", status == 400,
           f"status={status} code={(body or {}).get('code')}")

    print("\n[manual] C12: 미확정 일자 → 409 / 사용량 0 일자 → 200 빈 records "
          "(자동 강제 불가, 운영 시 수동 확인)")


def run_demo():
    schemas, v = load_validators()
    page = schemas["UsagePage"]["examples"][0]
    summary = schemas["UsageSummary"]["examples"][0]
    pe = schema_errors(v["page"], page)
    record("demo: UsagePage example schema valid", not pe, "; ".join(pe[:3]))
    se = schema_errors(v["summary"], summary)
    record("demo: UsageSummary example schema valid", not se, "; ".join(se[:3]))
    check_records(page["records"], "demo")
    check_summary_consistency(page["records"], summary)


def main():
    ap = argparse.ArgumentParser(description="token-usage-api 셀프 점검 스크립트")
    ap.add_argument("--base-url", default=os.environ.get("BASE_URL"))
    ap.add_argument("--date", default=os.environ.get("DATE"))
    ap.add_argument("--demo", action="store_true",
                    help="서버 없이 스펙 예시로 스크립트 동작 확인")
    args = ap.parse_args()

    if args.demo:
        run_demo()
    elif args.base_url and args.date:
        run_live(args.base_url, args.date)
    else:
        ap.error("--base-url 와 --date (또는 BASE_URL/DATE) 필요. 서버 없이 보려면 --demo")

    passed = sum(1 for _, ok in results if ok)
    failed = len(results) - passed
    print(f"\n{'='*48}\n{passed}/{len(results)} PASS" + (f", {failed} FAIL" if failed else ""))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
