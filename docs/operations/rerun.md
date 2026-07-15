# 재수행 (rerun) 절차

배치 실패·정정(§8.4)·과거 구간 회수의 표준 절차. 모듈별 tools/rerun.py를 사용한다.

| 모듈 | CronJob | namespace | 모드 |
|---|---|---|---|
| collectors/token-usage | token-usage-collector | monitoring | 1회 수동 트리거 / 날짜 범위(--from/--to, inclusive) |
| mart/token-usage | token-mart-daily | monitoring | 1회 수동 트리거 / 날짜 범위(--from/--to, inclusive) |

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

## mart/token-usage

    # 1회 수동 트리거 (실행 시점 기준 어제 KST — app.batch 기본 target_date 계약)
    python3 mart/token-usage/tools/rerun.py --context homelab

    # 날짜 범위 재수행 — 둘 다 inclusive (collectors와 동일 계약)
    python3 mart/token-usage/tools/rerun.py --context homelab \
        --from 2026-07-01 --to 2026-07-03

- **collectors rerun 후 동일 날짜의 mart rerun은 의무다(§3/§8.3).** collectors
  `tools/rerun.py --chain-mart`가 이 모듈을 직접 트리거한다(위 collectors 섹션 참조) —
  이 모듈 자체는 체이닝의 **수신 측**이라 `--service`/`--push-vm`/`--chain-mart` 플래그가
  없다(하류 없음). collectors rerun 완료 시 출력되는 mart rerun 명령을 그대로 복사해
  실행해도 되고, `--chain-mart`로 자동 트리거해도 된다.
- STEP 0→2 전체가 날짜별 독립 반복이라(§7.1) **날짜 범위 rerun은 날짜마다
  `BATCH_RESULT` 마커가 별도 줄로 출력된다** — collectors rerun이 실행 전체를 하나의
  요약 라인으로 내는 것과 다른 계약이니 로그 확인 시 유의(마커 상세는
  `mart/token-usage/README.md` 참조).
- mart는 VM push를 하지 않는다 — 아래 "VM push와 rerun" 절차는 collectors 전용.
- 6시간 캡(TIMEOUT_RANGE_S) 기준 안전 범위는 약 12일(1800s×n_days 산식 — collectors의
  4320s×n_days보다 산식 기저가 작아 캡까지 여유가 더 크다) — 긴 구간은 분할 실행.

## dim 정정 시 mart rerun

**dim_token_user_org 또는 dim_token_model 이력을 정정하면 해당 기간의 mart rerun이 의무다**
(§4.3 발생일 기준 고정).

### 상황

1. **dim_token_user_org 이력 정정**: 조직 변경 이력 오류(팀 이동 날짜 재지정, 조직명 오류 등)를
   §8.4 절차로 정정.
2. **dim_token_model 단가 소급 정정**: 기존 단가 행에 오류가 있어 수정 (§6.2 원칙 상 새
   effective_from 행이 아닌 기존 행 수정).

### 왜 mart rerun인가?

- mart는 **사실(fact) → dim 조인 → 집계** 파이프라인 (§4.3 단계별 독립).
- dim만 수정해도 mart의 읽은 조인 결과(이미 테이블에 저장됨)는 옛날 값을 유지.
- 따라서 영향 기간을 다시 계산(STEP 0→2 재실행)해야 한다.

### 실행 절차

정정 후 영향을 받은 기간(effective_from 범위)에 대해:

```bash
# 예: dim_token_user_org를 2026-07-15 기준일로 정정한 경우
python3 mart/token-usage/tools/rerun.py --context company \
    --from 2026-07-15 --to 2026-07-31

# 예: dim_token_model의 2026-08-01 단가를 수정한 경우
python3 mart/token-usage/tools/rerun.py --context company \
    --from 2026-08-01 --to 2026-08-31
```

- `--from/--to` 범위는 **inclusive** (§4.3 발생일이 포함된 구간 전체).
- 긴 기간은 분할 실행 가능 (안전 범위 약 12일 참조).
- 성공 조건: BATCH_RESULT 마커가 `status=SUCCESS` (§5.6/§7.1).

## 파기 요청 처리 (§8.3 user_id 축 삭제)

퇴사자 처리·개인정보 파기 요청 등 **user_id 단위 영구 삭제**는
`tools/data-admin/delete_data.py --mode user`를 사용한다. fact·mart·view 25개월치를
mart rerun으로 우회 재생성하는 것은 비현실적이므로, 이 모드는 3계층(fact 상세·mart
상세·view 상세)에 직접 ALTER DELETE를 실행한다(§8.3 ②) — **삭제는 되돌릴 수 없다.**

    # 1) dry-run(기본) — 대상 건수만 확인, 삭제는 수행되지 않음
    python3 tools/data-admin/delete_data.py --mode user --user-id <user_id>

    # 2) 대상 건수를 확인한 뒤 실제 삭제
    python3 tools/data-admin/delete_data.py --mode user --user-id <user_id> --yes

- **대상**: `fact.raw_token_usage_1d`, `mart.token_usage_1d`, `gpu_data.view_token_usage_1d`
  (상세 3테이블). `agg_token_*`·`raw_token_usage_summary_1d` 등 집계 테이블은 user_id
  컬럼이 없어(개인 식별 불가 집계) 대상이 아니다.
- `--yes` 없이 실행하면 대상 건수만 출력하고 종료한다(exit 0) — 실제 삭제는 `--yes`를
  붙여 재실행해야 한다. `--yes` 시에도 실행 직전 대상 요약을 다시 출력한 뒤에만 삭제한다.
- ON CLUSTER + wait_for_mutations(3s 폴링/300s 타임아웃, CH_CLUSTER 설정 시
  clusterAllReplicas)로 전 레플리카 완료까지 대기한 뒤 종료한다.
- **`gpu_data.dim_token_user_org` 행의 파기/가명화는 이 도구의 범위 밖이다** — 별도
  admin 경로로 처리한다(§6.1 보존 규칙: 퇴사 후 N년 경과 행 삭제/가명화와 동일 트랙).
  사용 이력(fact/mart/view)과 기준정보(dim)는 소유·절차가 분리되어 있으므로, 파기
  요청 처리 시 두 경로를 모두 확인해야 한다.
- 날짜 범위 기준 fact 정정(서비스 폐기·오적재 회수 등)은 `--mode date`를 사용한다 —
  이는 §8.4 정정(restatement) 프로토콜의 수동 정정 경로에 해당하며, 완료 후 동일
  기간 mart rerun이 의무다(위 "mart/token-usage" 절 참조 — 완료 시 도구가 실행할
  rerun 커맨드를 직접 안내한다).

## VM push와 rerun (§5.5)

rerun 경로는 VM push를 기본 생략한다 — VictoriaMetrics는 동일 timestamp 재push 시
dedup이 큰 값을 유지해 **하향 정정이 반영되지 않는다**. 필요 시:

1. 상향 정정만 확실하면 `--push-vm` 옵트인으로 재push.
2. 하향 정정을 VM에 반영해야 하면 해당 시계열을 delete_series API로 삭제 후 재push:

       curl -s "http://<vm>/api/v1/admin/tsdb/delete_series?match[]={__name__=~'token_usage_.*',service='<서비스>'}&start=<D1>&end=<D2>"
       python3 collectors/token-usage/tools/rerun.py ... --push-vm

   (delete_series는 관리 API — 사용 전 VM 운영자와 협의. 삭제는 되돌릴 수 없다.)
