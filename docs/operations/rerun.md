# 재수행 (rerun) 절차

배치 실패·정정(§8.4)·과거 구간 회수의 표준 절차. 모듈별 tools/rerun.py를 사용한다.

| 모듈 | CronJob | namespace | 모드 |
|---|---|---|---|
| collectors/token-usage | token-usage-collector | monitoring | 1회 수동 트리거 / 날짜 범위(--from/--to, inclusive) |
| mart/token-usage (Plan 3) | (미정) | (미정) | 날짜 범위 |

## collectors/token-usage

    # 1회 수동 트리거 (실행 시점 기준 어제 KST)
    python3 collectors/token-usage/tools/rerun.py --context homelab

    # 날짜 범위 재수집 — 둘 다 inclusive (동료 metric rerun의 to-제외와 다름에 주의)
    python3 collectors/token-usage/tools/rerun.py --context homelab \
        --from 2026-07-01 --to 2026-07-03 [--service "<정본 서비스명>"] [--chain-mart]

- 적재는 항상 delete-then-insert(§5.1)이므로 별도 --purge가 없다. 같은 (date, service)의
  재실행은 전체 교체이며, 교체 직전 세대는 fact.collect_audit_1d에 보존된다(§8.4-2).
- **collectors rerun 후 동일 날짜의 mart rerun은 의무다(§3/§8.3).** rerun.py가 완료 시
  mart rerun 명령을 출력하며, --chain-mart로 직접 트리거할 수 있다. 날짜(--from/--to)는
  동일 값 그대로 전파된다(v1.4 체이닝 계약).
- RESTATEMENT 마커(§8.4-1, D-2~D-7 summary 재조회에서 발화 — **재조회 자체는 후속 백로그,
  아직 미구현**)를 보면 운영자가 해당 (date, service)의 rerun 여부를 판단한다 —
  이 문서의 날짜 범위 재수집 + --service 조합을 사용.
- 6시간 캡(TIMEOUT_RANGE_S) 기준 안전 범위는 약 5일 — 긴 구간은 분할 실행.

## VM push와 rerun (§5.5)

rerun 경로는 VM push를 기본 생략한다 — VictoriaMetrics는 동일 timestamp 재push 시
dedup이 큰 값을 유지해 **하향 정정이 반영되지 않는다**. 필요 시:

1. 상향 정정만 확실하면 `--push-vm` 옵트인으로 재push.
2. 하향 정정을 VM에 반영해야 하면 해당 시계열을 delete_series API로 삭제 후 재push:

       curl -s "http://<vm>/api/v1/admin/tsdb/delete_series?match[]={__name__=~'token_usage_.*',service='<서비스>'}&start=<D1>&end=<D2>"
       python3 collectors/token-usage/tools/rerun.py ... --push-vm

   (delete_series는 관리 API — 사용 전 VM 운영자와 협의. 삭제는 되돌릴 수 없다.)
