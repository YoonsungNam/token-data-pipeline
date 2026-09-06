# /v1/metrics 반입 — 미결사항 장부 (2026-09-06 재기준)

- 원천: [설계 2026-08-31 §9 미결사항](2026-08-31-token-metrics-ingest-design.md#9-미결사항-open-questions)(M1~M22, M7·M13 결번). 이 문서는 §9를 **대체하지 않고** 각 항목에 재기준 날짜·담당(역할)·기본값·차단 관계·요청/회신 형식·미회신 시 조치·상태를 붙인 **실행용 장부**다.
- 일정 정본: [Plan 6a §일정 재기준 (2026-09-06 기준)](../plans/2026-09-04-token-metrics-schema.md#일정-재기준-2026-09-06-기준). 아래 날짜는 전부 재기준 날짜(§9 표의 8/31 원안 날짜가 아님).
- 갱신 규칙: 해소되면 **상태** 칸만 `resolved(날짜 — 근거)`로 바꾸고 행은 지우지 않는다. 새 항목은 M24부터(M23은 아래에서 사용). 담당은 역할명만 적는다(이름·이메일·사내 주소 금지 — 공개 레포).
- 상태 범례: `open` 회신 대기 · `default` 기본값으로 진행 중(회신 오면 정정) · `resolved` 해소 · `deferred` 이번 범위 밖(P1/P2 이월).

## 0. HARD 게이트와 미충족 시 결과

| 게이트 | 일시 | 충족 조건 | 미충족 시 |
|---|---|---|---|
| ① DDL 사인오프 | 9/8(화) 오전 | fact·gpu_data·mart 소유자가 Plan 6a draft PR의 DDL 14파일을 승인(M6 전반) | 6b/6c는 stage까지만 진행, 사내 설치 불가 → 9/14 보고에 사내 데이터 없음(stage mock + stage에 적재한 수기 1회분만) |
| ② go-live 전제 | 9/9(수) | admin 슬롯(M6 후반) + Harbor 반입 + 수기 수치(manual-v0) + M15 | 9/10 첫 산출 불가 → 보고 구간이 9/10 이후로 밀림 |
| ③ 프리즈 | 9/11(금) | DDL 동결, stretch 미착지분 P1 이월 | — |

## 1. A군 — 게이트를 막는 항목 (9/7 오전 발송, 회신 기한 명시)

| ID | 질문 | 담당(역할) | 기한 | 기본값(미회신) | 차단 | 상태 |
|---|---|---|---|---|---|---|
| M6-a | DDL 14파일 승인(collectors 3 · mart 2 · assets 9 = 테이블 fact 4·gpu_data 5·mart 4) + `ddl/README.md` 2종 | fact/gpu_data/mart 소유자 | 9/8 오전 | 없음 — HARD ① | ① | open |
| M6-b | 9/9 admin 슬롯: accounts 3파일 GRANT·dim DDL 4·플레이스홀더 시드 4·실값 생성 SQL 적용 | 클러스터 admin | 9/9 | 없음 — HARD ② | ② | open |
| M15 | 사내 스키마·스케줄·이름 확인(§1 'M15 사내 확인' 참조) | 사내 운영자 | 9/8 | 9/9 슬롯 전반에 admin이 직접 실행(13컬럼·레지스트리는 6b/6c 프리플라이트가 재검사, 스케줄·CA 이름은 자동 검사 없음) | ② | open |
| M1 | GPU 기종 TCO(원/GPU·h)·basis·이력 시작일 | 재무/인프라 | 9/8 EOD | NULL → 비용 `n/a`(0원 위장 금지) | 9/14 보고의 비용 값 | open |
| M14 | 서비스별 `apiSince`·이력 제공 여부 | 서비스 담당자 | 9/8 EOD | `apiSince=2026-09-09`, `coverageSince=2026-08-26`, API backfill 없음 | 9/10 backfill 범위 | default |
| M14-수기 | manual-v0 수기 수치 8/26~9/8 (템플릿 3파일) | 서비스 담당자 | **9/9 12:00** | 없음 — HARD ②(해당 서비스 구간 `metrics_missing`) | ② | open |
| M23 | 메타데이터 시트 `모델` 탭 CSV(canonical·aliases·defining_service) — 신규 ID(§9에 없음; Plan 6a 체크리스트 L54가 이를 "(M18)"로 오라벨 — 6b 착수 시 같이 정정) | 시트 소유자 | 9/8 EOD | identity-only alias(플레이스홀더 시드) — 모델명이 원문 그대로 표시 | alias 실값 | default |
| M3 | GPU 할당표(serviceGroup × gpu_type × 장수)·출처·허용 오차 | GPU 대시보드 소유자 | 9/8 EOD (stretch) | 수기 시드 없음 → M2 패널 제외, 오차 ±1원 | 9/14 M2 패널 | default |
| Harbor | 이미지 2종 반입 슬롯(`token-metrics-collector`, `token-mart-metrics`, sha7) | 사내 admin | 9/9 | 없음 — HARD ② | ② | open |
| M8 | batch_result 대시보드 라벨(`module=token-metrics` 8줄/일·`final=1`·부재=FAILURE, `module=mart-metrics`) | 모니터링 소유자 | 9/10 | §7.5 fallback: `metrics_missing` 패널 + 임시 LogsQL | 없음(이월 가능) | default |
| M4 | 플랫폼 제공자별 `usageIncludesConsumers` | 플랫폼 제공 팀 | 9/8 | 0(Σ 전 서비스), 다중 제공자 `provider_ambiguous` 보류 | stretch M4 share만 | default |

### A군 요청·회신 형식·미회신 시 조치

**M6-a DDL 사인오프**
- 요청: Plan 6a draft PR 링크 + "§7 예상 질문·답" 절을 첨부해 9/7 오전 발송. 리뷰 범위는 PR의 DDL 14파일과 `ddl/README.md`(적용 순서·뮤테이션 장부)로 한정.
- 회신 형식: PR 승인(Approve) 또는 파일·행 단위 코멘트. 구두 승인은 PR 코멘트로 옮겨 기록.
- 미회신 시: 9/8 오후까지 1회 재요청. 그래도 없으면 게이트 ① 미충족 경로(stage까지만)로 진행하고 9/9 admin 슬롯은 취소하지 않고 **유지**(사인오프가 9/9 오전에 와도 슬롯을 쓸 수 있게).
- 사인오프 뒤 변경: PR 수정으로만(재사인오프). 9/9 적용 뒤 변경은 §4-D4의 경로.

**M6-b admin 슬롯 (9/9)** — 단일 순서는 §6. README 순서 그대로면 **admin 3턴(순서 1·3·4) + 운영자 2턴(순서 2·3)** 인터리브다. admin 턴을 2회로 줄이려면 순서 3의 mart `accounts.sql`(이름 기반 GRANT — 테이블 전 선적용 가능)을 순서 1에 붙여 admin(1+3의 GRANT) → 운영자(2, 3의 install) → admin(4)로 접는다 — README 순서와 동치. 예약은 이 접은 형태(admin 2턴)로 한다. 필요 입력: PR 머지 커밋 SHA(9/8), Harbor 이미지 sha7(9/9 오전), 실값 CSV 4종(레포 밖).

**M15 사내 확인** — 아래를 복붙 실행해 결과 그대로 회신(값이 아니라 존재·컬럼명·스케줄만 필요).
```sql
DESCRIBE mart.token_usage_1d_dist;        -- 읽기 계약 9컬럼: date, service_group, service, model, input_tokens, cache_read_tokens, cache_creation_tokens, output_tokens, requests
DESCRIBE mart.agg_token_service_1d_dist;  -- date, service
DESCRIBE gpu_data.dim_token_service_dist; -- service, enabled
-- 사내 DDL은 `_local`/`_dist` 쌍만 만든다(접미사 없는 이름은 계약 표기) — 6c install.sh [3/6]도 `_dist`를 DESCRIBE
```
```bash
kubectl -n <ns> get cronjob token-mart-daily -o jsonpath='{.spec.schedule}{"\n"}'   # 10:20 순서 전제 확인
kubectl -n <ns> get secret registry-pull-secret -o name; kubectl -n <ns> get configmap | grep -i ca      # 이름 확인
```
- 미회신 시: 9/9 슬롯 전반에 admin이 위 5줄을 실행. 자동 재검사 범위는 부분적이다 — 레지스트리 SELECT·DB 존재는 6b `install.sh` [4/7], 읽기 계약 13컬럼은 6c `install.sh` [3/6](exit 3)이 잡지만, **`token-mart-daily` 스케줄(10:20 순서 전제)과 CA ConfigMap 이름은 어느 프리플라이트도 검사하지 않는다**(CA는 선택 항목) → 이 2줄은 슬롯 전반에 admin이 반드시 직접 확인.

**M1 TCO** — 회신 형식 = `csv_to_layer_c_dim_insert.py --table gpu_tco` 헤더:
```text
gpu_type,effective_from,tco_krw_per_gpu_hour,basis,note
H100,2026-08-26,<원/GPU·h>,tco,<출처·기준월>
```
- `gpu_type`은 fact의 `gpu_type`과 대소문자까지 동일. `basis` ∈ {depreciation, lease, power-inclusive, tco}(빈 값도 허용되나 회신에는 채워 달라고 요청). `currency` 컬럼은 선택(있으면 `KRW`만 — 생성기 `currency_krw` 검사). `effective_from`은 소급 시작일(2026-01-01 플레이스홀더보다 뒤).
- 확정 불가 시 제안: **잠정값**을 `basis='tco'`, `note='잠정(재무 9/8 구두) — 확정 시 정정'`으로 받는다. 보고에서 잠정임을 표시. 정정은 §4-D3 절차.
- 미회신 시: NULL 유지 → 9/14 보고 비용 열 전부 `n/a`. 보고 담당에 9/8 EOD 시점에 통보.

**M14 apiSince / M14-수기** — 요청 문안: "귀 서비스의 `/v1/metrics`가 과거 날짜(8/26~9/8)를 응답할 수 있으면 가능한 첫 날짜(`apiSince`)를, 없으면 첨부 템플릿 3파일에 수기로 8/26~9/8 값을 채워 회신".
- 템플릿: `docs/templates/token_metrics_manual_v0_gpu.csv`(`date,service,model,gpuType,category,gpuCount,gpuHours`), `..._serving.csv`(`date,service,model,metric,name,unit,p50,p90,p95,p99`), `..._engine.csv`(선택, `service,engine_type,engine_version`). 엑셀 저장은 **CSV UTF-8**.
- 적재: 9/9 저녁 `manual_load.py`(6b) → `rerun.py --from 2026-08-26 --to 2026-09-08 --chunk-days 7`(6c, ≥10:50 창). 이력 가능 서비스는 9/10 ≥10:50 `collectors/token-metrics/tools/rerun.py --context <ctx> --from <apiSince> --to 2026-09-09 --chunk-days 7 --chain-mart --replace`(`--context/--from/--to` 필수 쌍, 없으면 exit 2; manual 앵커가 있으므로 `--replace` 필수 — 없으면 `SKIPPED reason=already_loaded`).
- 미회신 시: 해당 서비스는 8/26~9/8 `metrics_missing`으로 보고. 9/9 12:00 이후 도착분은 9/10 저녁 적재.

**M23 시트 모델 탭** — 회신 형식 = `sheet_to_dim_token_model_alias_insert.py` 헤더 `canonical,aliases,defining_service,effective_from,note`. **aliases는 `,`(쉼표) 구분** — 생성기가 `split(",")`으로 자르므로 `;`로 받으면 `a;b` 전체가 alias 1개로 조용히 적재된다(exit 0, 검증 6종 어느 것도 못 잡음). 엑셀에서 셀 안에 쉼표를 그대로 두고 CSV UTF-8로 저장하면 필드가 자동으로 큰따옴표로 감싸진다. `defining_service`는 `endpoints-metrics.company.yaml`의 `service:`와 바이트 동일. 예시 행:
```text
canonical,aliases,defining_service,effective_from,note
mock-model-a,"Mock-Model-A,mock_model_a",Mock Service A,2026-08-26,예시
``` 미회신 시 identity-only로 9/14 보고, 도착 시 `--effective-from 2026-08-26` 소급 append + 해당 구간 rerun(9/10 예산 4×15=60 ≤ 64).

**M3 할당표** — 회신 형식 = `--table gpu_allocation` 헤더 `service_group,gpu_type,effective_from,allocated_gpu_count,source,note`. 9/8 EOD 미회신 시 9/14 보고에 "M2(할당 대비) 패널 제외" 한 줄 명시.

**Harbor** — 9/7에 슬롯만 예약(sha 미정 명시), 9/9 오전 CI `release-images-metrics.yml` 산출 sha7 2개 전달. 절차는 `docs/operations/company-verify.md` §0.

**M8 / M4** — 기본값으로 진행하며 회신은 이월 가능. M8 미확정이면 6c Grafana 초안에 §7.5 fallback 패널을 넣고 라벨은 P1.

## 2. B군 — 보고·정의 (9/14 전 결정, 게이트 아님)

| ID | 질문 | 담당(역할) | 기본값 | 제안 | 상태 |
|---|---|---|---|---|---|
| M2 | 토큰 단가 p 표시 기준(기준월·가동률 병기) + 요청당 원가 패널 ack | 보고 담당 | 정의서 §7 그대로 | 9/11 dry-run 리뷰에서 함께 확인 | default |
| M9 | 체급 경계 | 대시보드 담당 | params 원시값(P1), view 미고정 | 9/14 보고에 체급 미적용 명시 | deferred(P1) |
| M17 | 가중치 `1 / 0.1 / 4`의 TCO 팀 승인 | TCO 팀 | 상수 유지 | 9/7 요청에 "확인만" 항목으로 포함 | default |
| M18 | 메타데이터 시트 컬럼 추가: `workloadType`(llm-text/embedding/…), 사외 API 처리등급(`tier`), non-LLM custom 지표 이름표(§9 원문) | 시트 소유자 | `tier='standard'` 고정(vendor_price 생성기 tier 빈 값 → standard), `workloadType` 미사용 | 시트 v2 협의 — 9/14 범위 밖(M23 모델 탭 CSV와 별개) | deferred |
| M20 | D1(standby를 C에 포함) 팀 합의 → 스펙 레포 DECISIONS.md | 팀 | 포함(정의서 D1) | 9/11 dry-run 리뷰 안건 | default |
| M21 | 벤더 KRW 단가표(provider×model×tier)·PTU 존재; 정의서 3.9 `uncached×p_in` 이중 계산 피드백; TTL별 write 단가 표현 | 운영자/재무 · 정의서 소유자 | NULL·PTU 없음; 파이프라인은 `input×p_in`·전체 write 단가·TTL 최고 단가 | 단가표는 stretch(`--table vendor_price` 헤더 `provider,model,tier,effective_from,krw_per_mtok_input,krw_per_mtok_cached,krw_per_mtok_cache_creation,krw_per_mtok_output,note`); **정의서 피드백은 9/7 요청에 동봉**(3.9를 `input×p_in`으로 표기 정정, `p_write`는 전체 write 단가로 명시) | open |
| M22 | 벤더 콘솔 월 1회 대사 절차('추정' 라벨 제거 조건) | 운영 문서 | '추정' 유지 | 6c 런북 §9에 절차 자리만 예약 | deferred |

## 3. C군 — 이번 범위 밖 (P1/P2 이월)

| ID | 질문 | 처리 |
|---|---|---|
| M5 | 알림 채널·수신자 | 온보딩 안내 시 결정. 그때까지 체크 테이블 패널 + 수동 통보 |
| M10 | 스크랩 교차검증 임계값 | P2 |
| M11 | `/v1/usage` 보존 하한·RESTATEMENT 메트릭 확장 | 14일 계약 유지, 서비스팀 협의 P1 |
| M12 | 시트·CSV 실파일 보관 경로, owner 회신 반영 절차 | 레포 밖 + gitignore(§7.2 패턴). 6c 런북 §1에 "보관 경로는 사내 문서" 한 줄 |
| M16 | 사내 분기본 ↔ GitHub 동기화 | 별도 협의 |
| M19 | usage v1.2 `reasoningTokens` + 불변식 I6 | P2 (기존 파이프라인 영향) |

## 4. D군 — 실행 잔여 (코드 변경 없이 6b/6c/런북으로 이관)

| # | 항목 | 이관 위치 | 제안 | 상태 |
|---|---|---|---|---|
| D1 | 생성기 2종: 헤더만 있는 CSV → INSERT 0행 SQL을 exit 0으로 생성(F2) | 6c T11 런북 §1 apply 직전 | "생성 SQL의 INSERT 행수 0이면 apply 하지 않는다" 1줄 + 적용 후 4테이블 `count()` 확인 | open |
| D2 | `collectors/token-metrics/ddl/README.md`와 `mart/token-metrics/ddl/README.md`의 적용 순서 불일치 | 6a 브랜치 | collectors README 1~4를 정본으로 통일, alias 시드 검증 6의 레지스트리 선행 조건 명시 | resolved(2026-09-06 — 커밋 `59ba164`) |
| D3 | dim 실값 **정정** 절차 없음 — 같은 `(key, effective_from)` 재실행은 NOT IN 가드로 무시, 더 이른 날짜의 오류 행은 append로 못 덮음 | 6c T11 런북 §9 | admin: `ALTER TABLE gpu_data.<dim>_local ON CLUSTER 'gpu-monitoring' DELETE WHERE <key> AND effective_from = <d>` → 정정 행 append → 해당 구간 rerun. 뮤테이션 장부에 "dim 정정 = admin 1(예산 외)" 행 추가 | open |
| D4 | 9/9 적용 후 ~9/11 프리즈 사이 스키마 변경 경로 없음(ZK 경로 고정 `CREATE TABLE IF NOT EXISTS`) | 6c T11 런북 §0 | 사인오프 후~적용 전 = PR 수정; 적용 후 = admin `ALTER … ON CLUSTER`를 `migrate_add_<변경>.sql`로(`collectors/token-usage/ddl/README.md`·`mart/token-usage/ddl/README.md`의 명명 관행 — 레포에 실제 migrate 파일은 아직 없음), DROP/재생성은 금지 | open |
| D5 | 테스트 강도(F3)·BOM 테스트 단언(F4)·문구(Minor 5) | P1 follow-up | 6a PR 머지 후 이슈 1건으로 묶음 | deferred |
| D6 | `assets/user-org` 생성기도 BOM CSV 미허용(같은 한계) | P1 | 기존 모듈 무수정 원칙 — 별도 PR | deferred |
| D7 | `service_not_in_registry`(alias 시드 검증 6)는 레지스트리 첫 동기화 뒤에만 유의미 | §6 순서 | 순서 2(운영자 install.sh) 이후에만 시드 적용 — D2로 문서화 | resolved(D2) |
| D8 | 생성기 2종 사용법의 발견성 — `assets/model-catalog/README.md`는 zero-diff 대상이라 생성기·zero-diff 절이 없다(시드 갱신 절차만) | 6c T11 런북 §1 | 런북 §1에서 Plan 6a의 생성기 표('생성기 \| 시그니처 \| 입력 CSV 헤더')로 링크. README에 생성기 절 추가는 P1(zero-diff 해제 후) | open |
| D9 | 6b 플랜 완료 기준(L10460)·T1 Step 1(L138 `git branch --show-current` 기대값)의 작업 브랜치명이 `feat/token-metrics-design`(원격은 #12 스쿼시로 삭제, 로컬 브랜치는 잔존) | 6b 착수 시 Ruling | 플랜이 L10460에서 이미 허용한 대안 채택: 6b 브랜치 = `feat/token-metrics-collector`(6a 브랜치에서 분기), PR base = 6a 브랜치 → 6a 머지 후 main. L138 기대값도 같은 Ruling에 포함 | open |

## 5. E군 — 레포·프로세스 결정 기록

| # | 항목 | 결정(2026-09-06) | 상태 |
|---|---|---|---|
| E1 | 설계 PR #12 · 플랜 PR #13 머지 방식 | **스쿼시** — main `33a20ce`(#12), `517a175`(#13). 코드명이 들어 있던 커밋(스쿼시 전 원 커밋)은 main 조상에서 제외 — `refs/pull/12/head`·`refs/pull/13/head`로는 여전히 도달 가능(잔존 수용). rebase 전 로컬 6a 브랜치도 그 커밋을 조상으로 가지므로 E3의 `--onto`가 필수 | resolved |
| E2 | `delete_branch_on_merge` | 활성화 — base 삭제 시 스택 PR이 main으로 자동 전환 | resolved |
| E3 | Plan 6a 브랜치 반입 | `feat/token-metrics-schema`를 main(`517a175`, 트리 = 구 `d686592`) 위로 **`git rebase --onto origin/main d686592 feat/token-metrics-schema`**(스쿼시 전 원 커밋 8개를 잘라내고 a4f51aa 이후 태스크 커밋만 재적용 — 평범한 `git rebase main`은 `docs/cost-model-spec.md` add/add 충돌 + 코드명 커밋 재유입 위험) → **push 전 게이트**: `git log origin/main..HEAD -i -S'<코드명>' --oneline` 0줄·`git grep -i <코드명>` 0건·`git diff --stat origin/main -- <zero-diff 목록>` 빈 출력 → draft PR base=main → 9/8 사인오프 후 **merge commit**(태스크 커밋 보존, 6b/6c 스택 rebase 불필요) | open(rebase 대기) |
| E4 | 6b/6c 착수 시점 | 6b: 9/7 오후, 6a 브랜치 위 스택(6b 플랜 L10462 "6a 병합 후 rebase 예정" 경로). 6c: 6a 머지 후 main에서(T1·T2만 6a 파일 불필요) — 사인오프 지연 시 6a 브랜치 위 스택 | resolved |
| E5 | main 브랜치 보호 | 9/14까지 미보호 유지(운영자 단독). 이후 PR 필수·리뷰 1로 검토 | deferred |
| E6 | PR #12 제목의 "(승인 대기)" | "(2026-09-04 승인)"으로 정정 | resolved |

## 6. 9/9 admin 슬롯 — 단일 적용 순서 (정본: `collectors/token-metrics/ddl/README.md` §적용 순서)

| 순서 | 주체 | 작업 | 근거 |
|---|---|---|---|
| 1 | admin | `collectors/token-metrics/ddl/company/accounts.sql` (GRANT — 이름 기반, 테이블 전 선적용 가능) | README 1 |
| 2 | 운영자 | `collectors/token-metrics/install.sh company --context … --registry … --tag <sha7>` → fact 4 + `gpu_data.dim_token_metrics_service` + `endpoints-metrics.company.yaml` ConfigMap | README 2 |
| 3 | admin | `mart/token-metrics/ddl/company/accounts.sql` → 운영자 `mart/token-metrics/install.sh company …`(프리플라이트 = 기존 토큰 mart 13컬럼만) | README 3 |
| 4 | admin | `assets/model-catalog/ddl/company/` dim 4 → 플레이스홀더 시드 4 → `accounts_metrics.sql` → 실값 생성 SQL(alias·TCO·할당·단가; INSERT 0행이면 skip — D1) → 4테이블 `count()` | README 4 — alias 시드 검증 6이 순서 2의 레지스트리를 읽음 |
| 5 | 운영자 | 저녁 manual-v0 적재 → `rerun.py --from 2026-08-26 --to 2026-09-08 --chunk-days 7`(≥10:50 창) | Plan 6a 일정 9/9 |

admin 턴 접기: GRANT는 이름 기반이라 순서 3의 mart `accounts.sql`을 순서 1에 선적용해도 동치(M6-b 예약은 이 접은 형태 — admin 2턴). 순서 5의 `rerun.py`는 6c `mart/token-metrics/tools/rerun.py`(`--context` 포함 전체 인자는 6c 런북 §1).

## 7. DDL 리뷰 예상 질문·준비된 답 (draft PR 본문에 동봉)

| # | 예상 질문(대상) | 준비된 답 | 결정 필요 |
|---|---|---|---|
| Q1 | fact가 `toYYYYMM` 파티션인데 날짜 단위 DELETE로 재수집(--replace)하는 이유 (fact) | 설계 §4.0 명시(소행수·25개월 TTL). 뮤테이션은 날짜당 ≤3, 일 총량 150 예산 안. 월 파티션이라 DROP PARTITION은 부적합 | 리뷰어가 `toYYYYMMDD`를 요구하면 사인오프 전 PR 수정(테스트·미러 재생성 30분) |
| Q2 | 레지스트리가 ReplacingMergeTree가 아니라 ALTER DELETE + INSERT diff 동기화인 이유 (gpu_data) | 정기 실행에서만, 집합이 다를 때만 1회; `mutations_sync=2` 동기라 DELETE 미완료 상태에서 INSERT되는 일은 없고, DELETE~INSERT 사이의 짧은 빈 창은 수집 슬롯 02:05~09:05와 mart 10:20이 겹치지 않아 관측되지 않음; 최초 배포는 DELETE 생략(뮤테이션 0) | 없음 |
| Q3 | dim 실값 오류 정정 경로 (gpu_data·admin) | D3 절차(admin DELETE 1 + append + rerun) | 런북에 채택 |
| Q4 | admin 슬롯이 운영자 단계와 인터리브 (admin) | §6 단일 순서 + D2 | 없음 |
| Q5 | 적용 후 스키마 변경 경로 (admin) | D4 | 런북에 채택 |
| Q6 | fact `model LowCardinality(String)` vs dim `String` (fact·mart) | API 계약 ≤128자·서비스 수 소규모라 part별 사전 크기 상한 안; mart canon() 조인은 암묵 캐스트 | 상한 우려 시 fact만 `String`으로 — 사인오프 전 결정 |
| Q7 | collectors `accounts.sql`에 `system.mutations`·`CREATE TEMPORARY TABLE` GRANT 없음 (admin) | 수집기는 `mutations_sync=2`라 불요; mart `accounts.sql` 41~42행이 명시 부여(no-op if present, 기존 mart 계정과 동일 범위) | 없음 |

## 8. 변경 이력

| 날짜 | 내용 |
|---|---|
| 2026-09-06 | 최초 작성 — §9 20항목 재기준, D1~D9·E1~E6 추가, 9/9 단일 순서·리뷰 예상 질문 7건 |
| 2026-09-06 | 검증(3렌즈) 정정 — alias 구분자 `;`→`,`(생성기 계약), M15 DESCRIBE `_dist`·프리플라이트 범위 분리, 모델 탭 CSV를 M23으로 분리하고 M18 §9 원문 복원, M6-a 14파일 구성, M14 rerun 필수 인자, TCO 헤더(currency 선택), admin 턴 수, D4 명명·D8·D9 줄 번호, E1 잔존 범위·E3 `--onto` 명령·push 전 게이트, Q2 빈 창 표현, §0 ① 결과 문구 |
