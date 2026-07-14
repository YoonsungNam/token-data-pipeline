-- =============================================================
-- E2E 전용 dim_user_org / dim_model 단일노드 정본 (Plan 3 T5)
-- Global Constraints(스펙 §4.2 표) 컬럼 그대로 — Plan 4(assets)가 상속할 dim
-- 인터페이스 정본. 이 파일은 "단일노드 정본"이므로 처음부터 MergeTree 단일
-- 테이블로 작성한다(Replicated/Distributed 쌍 아님) — 단 STEP1(app/steps.py)이
-- `{DB_DIM}.dim_user_org_dist`/`dim_model_dist`를 하드코딩 참조하므로 테이블명은
-- `_dist` 접미사를 그대로 쓴다. Plan 4의 실제 company DDL은 mart_tables.sql과
-- 동일하게 ReplicatedMergeTree+Distributed 쌍으로 작성될 예정 — 이 파일은 그
-- 전까지 E2E가 참조하는 대역(정본 컬럼 계약은 동일, 물리 엔진만 단순화).
-- run_e2e.sh의 단일노드 변환 파이썬 블록에도 포함되지만(Replicated/ON CLUSTER/
-- Distributed 패턴이 없어) 정규식이 매칭할 대상이 없어 그대로 통과한다.
-- =============================================================

CREATE TABLE IF NOT EXISTS gpu_data.dim_user_org_dist
(
    user_id        String                 COMMENT '사내 id',
    effective_from Date                   COMMENT '이력 키 — 조직 이동/퇴사 시 신규 행 추가(기존 행 불변)',
    user_name      String,
    org_path       Array(String)          COMMENT '최상위→말단 가변 깊이 — 미매핑 조회는 조인측(mart)이 [''unknown'']으로 대체',
    org_depth      UInt8,
    is_active      UInt8                  COMMENT '0 = 퇴사/비활성 — headcount 산정 제외(§4.3 agg_token_org)',
    updated_at     DateTime('Asia/Seoul')
)
ENGINE = MergeTree
ORDER BY (user_id, effective_from);

CREATE TABLE IF NOT EXISTS gpu_data.dim_model_dist
(
    model                       String,
    effective_from              Date,
    provider                    String,
    serving_type                String                 COMMENT 'internal | external',
    input_usd_per_mtok          Nullable(Float64)      COMMENT '미등록 모델·unknown은 NULL — cost 산식 자연 전파(§4.3, §6.2 리뷰 #15 — $0 위장 금지)',
    cache_read_usd_per_mtok     Nullable(Float64),
    cache_creation_usd_per_mtok Nullable(Float64),
    output_usd_per_mtok         Nullable(Float64),
    currency                    String,
    note                        String
)
ENGINE = MergeTree
ORDER BY (model, effective_from);

-- -------------------------------------------------------------
-- 시드 데이터 (결정적 — mart_expectations.py의 resolve_org()/PRICES와 1:1 대응.
-- 이 시드를 고치면 mart_expectations.py도 함께 갱신해야 한다.)
-- -------------------------------------------------------------

-- dim_user_org: user-0000~0019 20명, org_path 3종 (X팀 7명 / Y팀 7명 / Z팀 6명),
-- 전원 is_active=1, effective_from='2026-01-01'.
INSERT INTO gpu_data.dim_user_org_dist
    (user_id, effective_from, user_name, org_path, org_depth, is_active, updated_at) VALUES
('user-0000', '2026-01-01', 'user-0000', ['A부문','X팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0001', '2026-01-01', 'user-0001', ['A부문','X팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0002', '2026-01-01', 'user-0002', ['A부문','X팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0003', '2026-01-01', 'user-0003', ['A부문','X팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0004', '2026-01-01', 'user-0004', ['A부문','X팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0005', '2026-01-01', 'user-0005', ['A부문','X팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0006', '2026-01-01', 'user-0006', ['A부문','X팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0007', '2026-01-01', 'user-0007', ['A부문','Y팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0008', '2026-01-01', 'user-0008', ['A부문','Y팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0009', '2026-01-01', 'user-0009', ['A부문','Y팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0010', '2026-01-01', 'user-0010', ['A부문','Y팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0011', '2026-01-01', 'user-0011', ['A부문','Y팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0012', '2026-01-01', 'user-0012', ['A부문','Y팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0013', '2026-01-01', 'user-0013', ['A부문','Y팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0014', '2026-01-01', 'user-0014', ['B부문','Z팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0015', '2026-01-01', 'user-0015', ['B부문','Z팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0016', '2026-01-01', 'user-0016', ['B부문','Z팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0017', '2026-01-01', 'user-0017', ['B부문','Z팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0018', '2026-01-01', 'user-0018', ['B부문','Z팀'], 2, 1, '2026-01-01 00:00:00'),
('user-0019', '2026-01-01', 'user-0019', ['B부문','Z팀'], 2, 1, '2026-01-01 00:00:00');

-- user-0005 조직 이동: 2026-06-01부로 X팀 -> Z팀 (이력 조인 검증 — 발생일 기준 귀속:
-- 5월 데이터는 이관 전 X팀 귀속, 6월 이후는 Z팀 귀속이어야 한다).
INSERT INTO gpu_data.dim_user_org_dist
    (user_id, effective_from, user_name, org_path, org_depth, is_active, updated_at) VALUES
('user-0005', '2026-06-01', 'user-0005', ['B부문','Z팀'], 2, 1, '2026-06-01 00:00:00');

-- user-0018/0019 퇴사(is_active=0): 2026-06-15부 — headcount 산정 검증(agg_token_org).
INSERT INTO gpu_data.dim_user_org_dist
    (user_id, effective_from, user_name, org_path, org_depth, is_active, updated_at) VALUES
('user-0018', '2026-06-15', 'user-0018', ['B부문','Z팀'], 2, 0, '2026-06-15 00:00:00'),
('user-0019', '2026-06-15', 'user-0019', ['B부문','Z팀'], 2, 0, '2026-06-15 00:00:00');

-- user-0020 이후(mock 기본 users=50까지) 및 anon-*/unclassified('')는 의도적으로
-- 미등록 → mart STEP1 조인 미스 → org_path=['unknown'] 자연 귀속 + org 매핑
-- 실패율 CHECK WARN 유발(§4.3, org_map_warn_threshold 기본 0.2) — 시드 행 불필요.

-- dim_model: opus/sonnet만 단가 4종 등록. claude-haiku-4-5는 의도적 미등록
-- (cost NULL 자연 전파 + unregistered_models CHECK WARN). unknown은 시드는
-- 포함하되 전 단가 NULL(§4.3, §6.2 리뷰 #15 — $0 위장 금지. WARN 목록에서는
-- model != 'unknown' 조건으로 자연 제외 — app/batch.py SQL_VALIDATE_UNREGISTERED_MODELS 참조).
INSERT INTO gpu_data.dim_model_dist
    (model, effective_from, provider, serving_type, input_usd_per_mtok,
     cache_read_usd_per_mtok, cache_creation_usd_per_mtok, output_usd_per_mtok,
     currency, note) VALUES
('claude-opus-4-8', '2026-01-01', 'anthropic', 'external', 15, 1.5, 18.75, 75, 'USD', ''),
('claude-sonnet-5', '2026-01-01', 'anthropic', 'external', 3, 0.3, 3.75, 15, 'USD', ''),
('unknown', '2026-01-01', '', 'external', NULL, NULL, NULL, NULL, 'USD', '계약 표준 값 — 단가 산정 불가');
