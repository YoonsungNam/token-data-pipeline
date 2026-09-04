# 토큰 비용 모델 정의서 (Cost Model Spec)

- 상태: Draft v0.1 (2026-09-04)
- 목적: GPU·토큰 수집 데이터로부터 **모델 비용 → 서비스 비용**을 산출하는 규칙을 한 곳에 고정한다.
- 입력 소스: `token-metric-api-spec` (GPU·성능), `token-usage-api-spec` (토큰)
- 원칙: 수집 스키마는 바꾸지 않는다. 이 문서는 **계산·표시 규칙**만 정의한다.

---

## 0. 한 문단 요약

모델 비용은 GPU 시간 × 단가로 확정되며 토큰은 여기에 관여하지 않는다.
토큰은 **한 모델을 여러 서비스가 공유할 때 그 비용을 나누는 배분 키**로만 쓰이며,
이때 유형별 가중치(uncached 1 / cached 0.1 / output 4)를 적용한다.
서비스 비용 = 자체 GPU 모델 비용(전액) + 공유 모델 배분분 + 사외 API 비용.
유휴 GPU는 비용이며, 어느 모델에도 귀속되지 않으므로 서비스 그룹 행에 별도 표시한다.

---

## 1. 용어

| 용어 | 정의 |
|---|---|
| 서비스 그룹 (`serviceGroup`) | GPU 쿼터(할당)를 보유하는 조직 단위 (과제). 비용 귀속의 최상위 단위 |
| 서비스 (`service`) | 모델을 호출해 사용하는 애플리케이션 단위. usage API 보고 단위 |
| 모델 (`model`) | GPU에 올라간 하나의 배포 단위. gpu 블록의 행 단위 |
| 전용 모델 | 한 서비스만 사용하는 모델 (호스팅 팀 = 사용 팀) |
| 공유 모델 | 둘 이상의 서비스가 API로 호출하는 모델 (사내 플랫폼 모델 등). 메타데이터 시트의 `consumes` 관계로 식별 |
| 사외 API | Claude, OpenAI 등 외부 벤더 모델. GPU 없음, 벤더 청구서가 비용 |
| `category` | gpu 블록의 용도 구분: `serving` / `standby` / `test`. `idle`은 보고하지 않고 운영자가 산출 |
| 단가 | GPU 종류별 TCO 기반 원/GPU·h. TCO 팀 제공, 정본 |
| 할당 (쿼터) | 서비스 그룹에 고정 배정된 GPU 수 × 시간. 운영자 보유 데이터 |

---

## 2. 입력 데이터

### 2.1 GPU (metric API `gpu` 블록)
- 행 단위: `serviceGroup × service × model × gpuType × category × date`
- 값: `gpuHours`
- `category ∈ {serving, standby, test}`. `idle`은 행으로 오지 않는다.

### 2.2 토큰 (usage API)
- 행 단위: `serviceGroup × service × model × userId × date`
- 값: `inputTokens`, `cacheReadTokens`, `cacheCreationTokens`, `outputTokens`, `requests`
- (추가 예정) `reasoningTokens` — `outputTokens`의 부분집합, 옵션
- 모든 값은 provider가 응답으로 보고한 usage 기준. 자체 토크나이저 추정 금지.

### 2.3 운영자 보유 데이터
- 단가표: `gpuType → 원/GPU·h`
- 할당표: `serviceGroup → gpuType → 할당 GPU·h/일`
- 벤더 단가표: `provider × model × 토큰유형 × 처리등급 → 원/1M 토큰`
- 가중치: `w_uncached = 1`, `w_cached = 0.1`, `w_output = 4`

---

## 3. 수식

### 3.1 GPU 시간 분해

```
할당 GPU·h  =  serving + standby + test + idle
idle        =  할당 − Σ(보고된 gpuHours)         ← 운영자 산출, gpuType별
```

제약: `idle ≥ 0`. 음수면 보고 오류(할당 초과 보고)로 플래그.

### 3.2 모델 비용 C

```
C(model, date) = Σ_gpuType [ (serving + standby) gpuHours × 단가(gpuType) ]
```

- `test`는 포함하지 않는다 (3.3).
- gpuType이 섞여 있으면 종류별로 곱한 뒤 합산한다.
- **C는 토큰과 무관하다.** 그날 토큰이 0이어도 C는 동일하다.

### 3.3 그룹 귀속 비용 (서비스로 배분하지 않음)

```
실험 비용(group, date) = Σ test gpuHours × 단가
유휴 비용(group, date) = Σ idle gpuHours × 단가
```

### 3.4 서비스 그룹 총비용

```
그룹 총비용 = 할당 GPU·h × 단가
           = Σ C(그룹이 호스팅하는 모델) + 실험 비용 + 유휴 비용
```

항등식: 위 두 줄은 항상 일치해야 한다 (검증 포인트).

### 3.5 가중 토큰 W (배분 키)

```
uncached(s) = inputTokens + cacheCreationTokens
cached(s)   = cacheReadTokens
output(s)   = outputTokens                       (reasoning 포함)

W(s, model, date) = 1·uncached(s) + 0.1·cached(s) + 4·output(s)
W(model, date)    = Σ_s W(s, model, date)        (그 모델을 쓴 모든 서비스 합)
```

### 3.6 공유 모델 배분

```
서비스 s의 부담 = C(model) × W(s) ÷ W(model)
```

- 전용 모델이면 `W(s) ÷ W(model) = 1` → 전액 귀속. 가중치 무관.
- 항등식: `Σ_s 부담 = C(model)`. 가중치를 바꿔도 총액은 변하지 않는다.
- W(model) = 0 인데 C > 0 이면 (토큰 미보고): 호스팅 그룹에 전액 귀속하고 "토큰 미보고" 플래그.

### 3.7 토큰 단가 (파생 지표, 정보용)

```
p = C(model) ÷ W(model)              (원/가중토큰)
p_uncached = p,  p_cached = 0.1·p,  p_output = 4·p
```

- 이 값은 **배분의 결과**이며 비용 입력이 아니다. `p × 토큰`을 다시 더하면 C가 나올 뿐이다 (순환).
- 가동률에 따라 매월 변한다. 표시 시 반드시 "기준월, 가동률" 병기.
- 유형 간 비율(1 : 0.1 : 4)은 가정이 그대로 나오는 것이다. 실측되는 것은 수준(원/1M)뿐이다.

### 3.8 서비스 비용

```
서비스 비용(s) = ① + ② + ③

① 자체 GPU 모델 비용 = Σ C(s가 호스팅하고 전용으로 쓰는 모델)
② 공유 모델 배분분   = Σ_model [ C(model) × W(s) ÷ W(model) ]
③ 사외 API 비용      = Σ (토큰유형별 토큰 × 벤더 단가)
```

- 자체 서빙 모델을 타 팀에도 열어준 경우 → 그 모델은 공유 모델로 취급, ②의 계산 적용.
- ③은 사내에서 "토큰 가격 × 토큰량 = 비용"이 성립하는 유일한 경우.

### 3.9 사외 API 비용 상세

```
③ = uncached × p_in + cached × p_cache + cacheCreation × p_write + output × p_out
```

- 벤더 단가는 모델·처리등급(standard / batch / flex / priority)별로 다르다. 메타데이터 시트에 처리등급 컬럼 필요.
- OpenAI credit은 선불 잔액(결제 방식)이지 계산 단위가 아니다. 사용 기준으로 산출하고, 충전액과의 차이는 재무 대사 항목.
- 월 1회 벤더 콘솔(사용량/비용) 값과 대사하면 "추정" 라벨을 제거할 수 있다.

---

## 4. 토큰 유형 매핑

### 4.1 수집 4(+1) → 표시 3 → 가중치

| 대시보드 표시 | usage API 필드 | 가중치 |
|---|---|---|
| Input (Uncached) | `inputTokens + cacheCreationTokens` | 1 |
| Input (Cached) | `cacheReadTokens` | 0.1 |
| Output | `outputTokens` (reasoning 포함) | 4 |

- 합치는 것은 **뷰 레이어**에서만. API는 4개 필드를 유지한다 (벤더 대사에 cacheCreation 단가가 별도로 필요, 되돌릴 수 없음).
- `reasoningTokens`가 추가되면 Output 내부의 부분집합으로 표시(선택). 가중치는 output과 동일(4).

### 4.2 provider 응답 → usage API 필드

| usage API | Anthropic | OpenAI (Chat) | OpenAI (Responses) | vLLM / SGLang |
|---|---|---|---|---|
| `inputTokens` | `input_tokens` (캐시 제외값 그대로) | `prompt_tokens − prompt_tokens_details.cached_tokens` | `input_tokens − input_tokens_details.cached_tokens` | `prompt_tokens − prompt_tokens_details.cached_tokens` |
| `cacheReadTokens` | `cache_read_input_tokens` | `prompt_tokens_details.cached_tokens` | `input_tokens_details.cached_tokens` | `prompt_tokens_details.cached_tokens` |
| `cacheCreationTokens` | `cache_creation_input_tokens` | 0 | 0 | 0 |
| `outputTokens` | `output_tokens` (thinking 포함) | `completion_tokens` | `output_tokens` | `completion_tokens` |
| `reasoningTokens` | 별도 필드 없음 (thinking 블록 직접 산정 또는 0) | `completion_tokens_details.reasoning_tokens` | `output_tokens_details.reasoning_tokens` | `reasoning_content` 토크나이즈 (`--reasoning-parser` 필요) |

주의: Anthropic의 `input_tokens`는 캐시분을 **제외**한 값, OpenAI·vLLM의 `prompt_tokens`는 캐시분을 **포함**한 값이다. 후자는 빼야 한다. 빼지 않으면 캐시가 이중 계상된다.

스트리밍 시 OpenAI 호환 API는 `stream_options: {"include_usage": true}`를 켜야 마지막 청크에 usage가 온다.

### 4.3 엔진 메트릭(Prometheus)과의 관계

- vLLM `/metrics`는 `prompt_tokens_total`, `generation_tokens_total`, `prefix_cache_queries`, `prefix_cache_hits`를 제공한다. 모델 단위 총량이며 서비스·사용자 구분은 없다.
- cache read는 엔진 메트릭에서도 분해 가능(`prefix_cache_hits`). cache write는 vLLM에 개념이 없다.
- reasoning/answer 분해는 엔진 메트릭에 없고 응답 레벨(`reasoning_content`)에만 존재한다.
- 2단계 커버리지: (a) `/metrics` 스크래핑으로 모델 단위 행(`userType: unclassified`)을 운영자가 자동 생성 (서비스 부담 0), (b) 사용자 단위가 필요한 서비스만 usage API 구현.
- 두 경로가 겹치면 `Σ outputTokens vs generation_tokens_total(increase, 1d)`로 교차 검증.

---

## 5. 워크 예시

### 5.1 공유 모델 배분

Qwen3-32B, A100 × 2, 하루 48 GPU·h (serving 44 + standby 4), 단가 5,000원 → **C = 240,000원**

| 서비스 | uncached | output | W = 1·un + 4·out | W ÷ ΣW | 배분 |
|---|---|---|---|---|---|
| HR 챗봇 | 10M | 1M | 14M | 14/44 | 76,364원 |
| 문서 요약 | 20M | 2M | 28M | 28/44 | 152,727원 |
| 코딩 도우미 | 1M | 0.25M | 2M | 2/44 | 10,909원 |
| 합계 | | | 44M | 1 | 240,000원 |

가중치를 1:1로 바꾸면 HR 챗봇 배분은 240,000 × 11/34.25 ≈ 77,080원. 총액은 불변.

### 5.2 토큰 단가 파생

Llama-70B, H100 × 4, 96 GPU·h, 단가 5,000원 → C = 480,000원
토큰: uncached 50M, cached 30M, output 10M → W = 50 + 3 + 40 = 93M

```
p = 480,000 ÷ 93M ≈ 0.00516원/가중토큰
p_uncached ≈ 5,160원/1M,  p_cached ≈ 516원/1M,  p_output ≈ 20,600원/1M
검산: 50M×5,160 + 30M×516 + 10M×20,600 ≈ 480,000 ✓
```

### 5.3 서비스 그룹 총비용

그룹 할당 H100 120 GPU·h/일. 보고: serving 96, standby 24 (llama-70b) + test 0.
→ idle = 120 − 120 = 0. 그룹 총비용 = 120 × 단가 = C(llama-70b) + 0 + 0.
다음 날 serving 80, standby 24 → idle = 16 → 유휴 비용 = 16 × 단가, 그룹 행에 별도 표시.

---

## 6. 설계 판단 (DECISIONS 후보)

| # | 판단 | 근거 | 대안 |
|---|---|---|---|
| D1 | `standby`를 C에 포함 | HA 대기는 그 모델을 쓰는 서비스들이 함께 누리는 가용성 비용 | 운영 팀 선택으로 보고 그룹에 남김 |
| D2 | `test`·`idle`은 그룹 귀속, 배분 안 함 | 어느 서비스의 산출물에도 대응하지 않음. 조치 주체가 쿼터 보유 그룹 | — |
| D3 | 유휴는 비용에 **포함**, 행만 분리 | GPU는 할당 시점부터 TCO 발생 | — |
| D4 | `cacheCreation` → 표시상 Uncached에 합산 | 처리 시점엔 캐시에 없던 토큰, prefill 전량 수행. vLLM은 0 | 별도 표시 (Claude 대사용) |
| D5 | API는 4개 필드 유지, 3개 표시는 뷰에서만 | 벤더 대사 필요, 합치면 되돌릴 수 없음 | — |
| D6 | 가중치 1 / 0.1 / 4, TCO 팀 승인값이 정본 | prefill(병렬) vs decode(순차)의 물리적 차이. 시장 가격 비율 3~5×와 정합 | 실측값으로 교체 (6.1) |
| D7 | Artificial Analysis는 분류 체계·가중치 구조의 참조로만 사용 | AA 수치는 벤더 판매 가격, 사내는 TCO 원가. 가동률·토크나이저·워크로드가 다름 | make-vs-buy 지수로만 비교 (가동률 병기) |
| D8 | 토큰 단가는 파생 지표, 비용 입력 아님 | 순환 (p × 토큰 = C) | — |

### 6.1 가중치 실측 방법 (선택)

vLLM V1 `/metrics`의 `vllm:request_prefill_time_seconds`, `vllm:request_decode_time_seconds` 히스토그램으로:

```
w_output(실측) = (Σ decode시간 ÷ Σ output토큰) ÷ (Σ prefill시간 ÷ Σ input토큰)
```

배치로 인해 토큰 단위 귀속은 근사치이나 비율로는 충분. 모델 티어별 분기 1회 재측정 후 3.5의 계수만 교체.

---

## 7. 표시·라벨 규칙

- 직접 측정값과 배분·추정값을 구분해 라벨링한다: `측정` / `배분` / `추정`.
- 토큰 단가(3.7)는 항상 `기준월`, `가동률(%)`과 함께 표시한다.
- 모델 간 비교 뷰에서는 토크나이저 차이를 보정한다 (사내 한국어 샘플로 모델별 `native ÷ o200k` 비율을 오프라인 측정 → 보정계수). 수집 규칙(자체 토크나이저 금지)과 충돌하지 않음.
- 토큰당이 아니라 **요청당·작업당 원가**를 1차 효율 지표로 둔다 (reasoning 모델의 verbosity가 여기서만 보임).
- 임원용 자료에서는 vLLM, Prometheus 등 기술 용어를 제거하고 "어느 서비스가 GPU 비용을 쓰는가 / 얼마나 놀고 있는가 / 모델을 바꾸면 무엇이 달라지는가"로 표현한다.

---

## 8. 구현 시 불변식 (테스트 포인트)

```
I1  idle ≥ 0                                        (gpuType별, 일별)
I2  그룹 총비용 == 할당 × 단가 == ΣC + 실험 + 유휴
I3  Σ_s (C × W_s ÷ W) == C                          (부동소수 허용오차 내)
I4  전용 모델: 배분 결과 == C, 서비스 1개
I5  총 input == uncached + cached  (== inputTokens + cacheRead + cacheCreation)
I6  reasoningTokens ≤ outputTokens                  (필드 존재 시)
I7  summary == Σ detail                             (usage API 기존 불변식)
I8  W(model) == 0 이고 C > 0 → "토큰 미보고" 플래그, 호스팅 그룹 전액 귀속
```

---

## 9. 계산 의사코드

```python
def model_cost(rows_gpu, rate):
    # rows_gpu: [(model, gpuType, category, gpuHours)]
    C = defaultdict(float)
    for m, g, cat, h in rows_gpu:
        if cat in ("serving", "standby"):
            C[m] += h * rate[g]
    return C

def group_overhead(rows_gpu, alloc, rate):
    # alloc: {gpuType: allocated gpu hours}
    reported = defaultdict(float); test = 0.0
    for m, g, cat, h in rows_gpu:
        reported[g] += h
        if cat == "test": test += h * rate[g]
    idle = {g: alloc[g] - reported[g] for g in alloc}
    assert all(v >= 0 for v in idle.values()), "over-report"
    idle_cost = sum(idle[g] * rate[g] for g in alloc)
    return test, idle_cost

W_UNC, W_CACHE, W_OUT = 1.0, 0.1, 4.0

def weighted_tokens(u):
    unc = u.inputTokens + u.cacheCreationTokens
    return W_UNC*unc + W_CACHE*u.cacheReadTokens + W_OUT*u.outputTokens

def allocate_shared(C_model, usage_rows):
    # usage_rows: rows for one model, all services
    W = {s: weighted_tokens(u) for s, u in usage_rows}
    total = sum(W.values())
    if total == 0:
        return None  # flag: token not reported -> host group keeps C
    return {s: C_model * w / total for s, w in W.items()}
```

---

## 10. 열린 항목

- [ ] usage API v1.2: `reasoningTokens` 옵션 필드 추가 + 불변식 I6 + provider 매핑 표(4.2) 반영
- [ ] `w_cached = 0.1` TCO 팀 확인
- [ ] 메타데이터 시트: `workloadType` (llm-text / embedding / speech / vision / image-gen / other), 사외 API `처리등급` 컬럼
- [ ] non-LLM 서비스: workloadType별 표준 custom 메트릭 이름표 (`itemsPerSec`, `rtf`, `secPerImage` 등). "같은 workloadType 안에서는 비교 가능"으로 규칙 수정
- [ ] Azure OpenAI PTU 사용 서비스 여부 확인 (있으면 ①과 유사 처리: 예약 용량 × 시간이 비용, 토큰은 배분 키)
- [ ] 벤더 콘솔 대사 절차 (월 1회) 문서화
- [ ] D1(standby 포함 여부) 팀 합의 후 DECISIONS.md 반영
