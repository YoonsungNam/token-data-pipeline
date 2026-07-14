# mart/token-usage

collectors가 적재한 `fact.raw_token_usage_*`를 조직/모델 차원으로 집계해
`mart.token_usage_1d` + `agg_token_{service,org,model}_1d` 4테이블과 대시보드용
`gpu_data.view_token_usage_*` 4테이블로 내리는 일배치 (스펙 §4). collectors가
매일 02:00 KST에 적재를 끝낸다는 전제 위에서 04:00 KST에 실행된다(§5.9-9 cron
오프셋).

## 실행

    pip install -r requirements-dev.txt
    CH_HOST=... python -m app.batch                              # target_date = 어제 (KST)
    python -m app.batch 2026-07-11T04:00:00+09:00                # batch_time 명시
    python -m app.batch --from 2026-07-01 --to 2026-07-03         # 날짜 범위 재수행(inclusive)
    # 날짜 범위 재수행도 날짜별로 STEP 0→2 전체를 독립 반복한다(§7.1) — collectors의
    # 단일 집계 라인과 달리 날짜당 BATCH_RESULT 1줄이 각각 출력된다.

## 환경변수 (§7.1 Global Constraints)

| 변수 | 기본값 | 의미 |
|---|---|---|
| CH_HOST/CH_PORT/CH_USER/CH_PASSWORD | localhost/8123/default/'' | ClickHouse 접속 (운영 계정 token_mart는 Secret 주입) |
| CH_CLUSTER | '' | 빈 값 = 단일노드 (ON CLUSTER·clusterAllReplicas 생략, CI/stage) |
| EXPECTED_LATE_SERVICES | '' | STEP 0 coverage 경고 제외 목록(콤마 구분, 공백/빈 항목 제거 — Secret 경유 envFrom 주입, §5.9-9) |
| ORG_MAP_WARN_THRESHOLD | 0.2 | dim_user_org 매핑 실패율 CHECK WARN 임계 |
| RETRY_COUNT / RETRY_INTERVAL_S | 10 / 5 | INSERT 후 count 검증 재시도 횟수/간격 |
| MUTATION_POLL_S / MUTATION_TIMEOUT_S | 3 / 300 | wait_for_mutations 폴링 주기/타임아웃 |
| INSERT_QUORUM | '' | 빈 값 = 미적용. company(2s×2r)는 install.sh가 `auto` 주입 — detail 적재 직후 agg가 `_dist`로 읽을 때 지연 레플리카 라우팅에 의한 무음 과소집계를 막는 게이트(§9-19). stage(1s×1r)/CI 단일노드는 미설정 |

## 마커 (§5.6/§7.1)

- 실행(날짜)당 1줄:

      BATCH_RESULT status=<SUCCESS|FAILURE> module=mart-token coverage=<N>/<M> \
        missing_services="<a,b,...|->" rows_mart=<n> rows_view=<n> warn=<n> elapsed=<sec, 1자리>

  - `coverage=N/M`: M=활성화된 서비스 수, N=해당 날짜 summary에 존재하는 활성 서비스 수.
  - `missing_services`: coverage 미달 서비스 목록 — **항상 쌍따옴표로 감싼다**(서비스명
    공백 보호). 없으면 `-`.
  - **날짜 범위 rerun은 날짜별로 이 줄이 독립 출력된다** — collectors rerun이 실행
    전체를 하나의 요약 라인으로 내는 것과 다른 계약이니 로그 파싱 시 유의.
- STEP 0 coverage 미달·검증 4종(totals_mismatch/diff_mismatch/org_map_fail_rate/
  unregistered_models) 각각 `CHECK WARN ...` 1줄씩 출력(§7.1 "조용함 금지").
- 로그에 user_id 원문·레코드 페이로드 금지(§5.6).

## 배포 (§7.2)

    # 1. 이미지 빌드 & 푸시 (태그 기본 = git short SHA)
    ./mart/token-usage/build.sh stage
    ./mart/token-usage/build.sh --registry <harbor> company

    # 2. Secret + 테이블 DDL + CronJob (대화형) — endpoints ConfigMap 단계 없음
    #    (mart는 endpoints 불요 — gpu_data.dim_token_service가 게이트 기준)
    ./mart/token-usage/install.sh stage
    ./mart/token-usage/install.sh --registry <harbor> --context <ctx> company
    # accounts.sql(CREATE USER/GRANT/insert_deduplicate 서버 설정)은 admin 수동 실행
    # — install.sh는 mart_tables.sql/view_token_usage.sql만 자동 적용하고 안내만 출력

    # 3. 수동 실행 (테스트)
    python3 mart/token-usage/tools/rerun.py --context homelab

- CronJob `token-mart-daily`: 매일 04:00 KST(수집 02:00 완료+적재 데드라인 03:30
  이후 — §5.9-9), Forbid, activeDeadlineSeconds 1800(§7.2 — 서버사이드 SQL 경량),
  resources 256Mi/1Gi.
- VM push 없음 — VM_PUSH_URL 주입 대상이 아니다(mart는 VictoriaMetrics를 건드리지 않는다).
- install.sh 밖에서 `kubectl apply -k`를 직접 재실행하면 이미지가 latest로 리셋된다 —
  재적용은 항상 install.sh 경유.

## 재수행

`docs/operations/rerun.md` 참조.

**collectors rerun 후 동일 날짜의 mart rerun은 의무다(§3/§8.3).** collectors
`tools/rerun.py --chain-mart`가 이 모듈(`mart/token-usage/tools/rerun.py`)을
직접 트리거한다 — 이 모듈은 체이닝의 **수신 측**이라 하류 체이닝 플래그
(`--service`/`--push-vm`/`--chain-mart`)가 없다. `--from/--to`(inclusive, 쌍)
날짜 범위 또는 무인자 1회 트리거만 지원한다.

## 검증

    python -m pytest tests/ --ignore=tests/e2e     # 단위 (DB/네트워크 불요)
    ./tests/e2e/run_e2e.sh                          # E2E (docker 필요 — CI에서 실행)

## DDL

`ddl/README.md` 참조 — `ddl/company/{mart_tables.sql,view_token_usage.sql}`은
install.sh 자동 적용 대상, `ddl/company/accounts.sql`(GRANT·insert_deduplicate
서버 설정)은 admin 수동 실행.
