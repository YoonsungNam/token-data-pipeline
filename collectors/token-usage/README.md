# collectors/token-usage

token-usage-api(v1.1.0)를 구현한 사내 서비스들을 매일 pull하여 ClickHouse에 적재하는
수집기 (스펙 §5). 서비스 목록의 정본은 `endpoints.yaml`(§5.0).

## 실행

    pip install -r requirements-dev.txt
    CH_HOST=... python -m app.main                      # target_date = 어제 (KST)
    python -m app.main 2026-06-16T02:00:00+09:00        # batch_time 명시
    python -m app.main --from 2026-06-10 --to 2026-06-12 --service "Mock Service A"
    python -m app.main --from 2026-06-10 --to 2026-06-12 --push-vm   # rerun 경로도 VM push (§5.5 옵트인)
    # 재수집 = 기본 동작(delete-then-insert), --purge 없음. rerun 후 mart rerun 의무(§8.3)

## 환경변수 (§5.7)

| 변수 | 기본값 | 의미 |
|---|---|---|
| CH_HOST/CH_PORT/CH_USER/CH_PASSWORD | localhost/8123/default/'' | ClickHouse 접속 |
| CH_CLUSTER | '' | 빈 값 = ON CLUSTER 생략 (단일노드/CI) |
| VM_PUSH_URL | '' | 빈 값 = VM push 생략. rerun 경로는 기본 생략 (--push-vm 옵트인, §5.5) |
| ENDPOINTS_FILE | endpoints.yaml | 서비스 레지스트리 (정본) |
| MAX_PAGES / MAX_BUFFER_ROWS | 200 / 20000 | 페이지 상한(초과=FAILURE) / flush 단위 |
| SOFT_DEADLINE_MINUTES / NOT_READY_BUDGET_MINUTES | 50 / 30 | §5.2 예산 |
| COLLECTOR_HTTPS_PROXY / COLLECTOR_API_VERIFY / COLLECTOR_API_CA_BUNDLE | 상속/true/'' | 아웃바운드 HTTP 방침 |
| CH_DB_FACT / CH_DB_DIM | fact / gpu_data | company 2단계 검증(격리 DB) 전용 — `docs/operations/company-verify.md`. **격리 검증 외 변경 금지** |

## 배포 (§7.2)

    # 1. 이미지 빌드 & 푸시 (태그 기본 = git short SHA)
    ./collectors/token-usage/build.sh stage
    ./collectors/token-usage/build.sh --registry <harbor> company

    # 2. Secret + endpoints ConfigMap + 테이블 DDL + CronJob (대화형)
    ./collectors/token-usage/install.sh stage
    ./collectors/token-usage/install.sh --registry <harbor> --context <ctx> company
    # accounts.sql(CREATE USER/GRANT)은 admin 수동 실행 — install.sh는 안내만 출력

    # 3. 수동 실행 (테스트)
    python3 collectors/token-usage/tools/rerun.py --context homelab

- CronJob: 매일 02:00 KST (mart 04:00이 완료를 전제 — §3 cron 오프셋), Forbid,
  activeDeadlineSeconds 4320(§5.2 산식), resources 256Mi/1Gi(§7.2).
- endpoints: 레포에는 stage(mock)용만. 사내 목록은 endpoints.company.yaml(gitignored)을
  install.sh가 ConfigMap `token-usage-endpoints`로 주입.
- install.sh 밖에서 `kubectl apply -k`를 직접 재실행하면 이미지가 latest로 리셋된다 — 재적용은 항상 install.sh 경유.

## 재수행

`docs/operations/rerun.md` 참조 — collectors rerun 후 동일 날짜 mart rerun 의무(§8.3).

## 마커 (§5.6)

- 실행당 1줄: `BATCH_RESULT status=... module=token-usage services_ok=... rows=... elapsed=...`
- 서비스별: `SERVICE_RESULT status=SUCCESS|NODATA|SKIPPED|FAILURE service=... rows= pages= warn= rejected=`
- 로그에 user_id 원문·레코드 페이로드 금지 (§5.6 로깅 계약)

## 검증

    python -m pytest tests/ --ignore=tests/e2e     # 단위 (DB/네트워크 불요)
    ./tests/e2e/run_e2e.sh                          # E2E (docker 필요 — CI에서 실행)

## DDL

**§9-18 확정 반영됨**: fact DB 공유, `gpu_data.dim_token_service`(dim_token_* 접두사). DDL은 `ddl/` 참조
