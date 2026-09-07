#!/usr/bin/env python3
"""메타데이터 시트 `모델` 탭 CSV → gpu_data.dim_token_model_alias_dist INSERT SQL 생성기 (Plan 6a T8).

`assets/user-org/csv_to_dim_user_org_insert.py`(로스터 생성기) 클론 — stdlib만 사용(csv/argparse/
dataclasses/re). Python 3.10+. PyYAML 의존 없음(--services는 줄 정규식으로 `service:` 키만 읽는다).

CSV 계약(설계 2026-08-31 §4.2·§7.2): 헤더 `canonical,aliases,defining_service,effective_from,note`.
  - canonical: 필수. 정규화 대상 모델명(≤128). 빈 값 금지(empty_canonical).
  - aliases: 쉼표 구분(공백은 strip만 — 자동 교정 없음, 대소문자·하이픈 보존). 빈 값 = canonical-only
    행(identity 행만 생성, defining_service=''). 빈 세그먼트(`a,,b`, `a,`, `,a`) 금지.
  - defining_service: alias 행에 필수 — 레지스트리(endpoints*.yaml의 `service:`)와 바이트 동일해야 함
    (service_not_in_registry). identity 행은 항상 ''.
  - effective_from: 선택(YYYY-MM-DD) — 빈 값이면 --effective-from(둘 다 없으면 오류).
    2026-01-01(사내 시드 플레이스홀더 키)은 금지 — NOT IN 가드가 플레이스홀더 행과 충돌해 무음 skip 됨.
  - note: 선택.

출력: 멱등(NOT IN 가드) INSERT SQL + 말미 `-- 검증: 결과가 비어야 정상` 앵커 뒤 검증 SELECT 6종
  (dup_key, alias_maps_to_two_canonicals, alias_loop, empty_canonical, missing_identity_row,
   service_not_in_registry — 마지막 1종은 gpu_data.dim_token_metrics_service_dist 대조).
파일 내 검증(SQL 이전): empty_canonical, empty_alias_segment, effective_from 형식·플레이스홀더,
  service_not_in_registry, alias_loop, alias_maps_to_two_canonicals, dup_key.
  missing_identity_row는 구조적으로 불가(모든 canonical에 identity 행을 항상 생성) — SQL 검증만.

데이터 경계(§7.2): 실시트 CSV·생성 SQL은 레포 반입 금지 — .gitignore가 `*metadata*.csv`,
  `dim_token_model_alias_insert*.sql` 패턴으로 선제 차단. stdout은 요약(건수)만, 데이터 원문
  (모델명·서비스명)은 성공/실패 경로 모두에서 에코하지 않는다(행 번호·필드명·건수만).

exit code: 0 성공 / 1 검증 실패(SheetError) / 2 인자·입력 오류(argparse).
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

DEFAULT_OUT_FILENAME = "dim_token_model_alias_insert.sql"
DEFAULT_CHUNK_SIZE = 500

TABLE_NAME = "dim_token_model_alias_dist"
REGISTRY_TABLE_NAME = "dim_token_metrics_service_dist"
DEFAULT_TARGET_DB = "gpu_data"
TARGET_DB_CHOICES = ("gpu_data", "token_verify_dim")

# 사내 시드(seed_dim_token_model_alias.sql)의 플레이스홀더 키 날짜 — 생성 SQL은 이 날짜를 쓰면
# unknown 행과 NOT IN 가드 충돌 소지가 있어 금지(설계 §4.2 effective_from 규약: 소급 시작일).
PLACEHOLDER_EFFECTIVE_FROM = "2026-01-01"

SOURCE_SHEET = "metadata-sheet"
REQUIRED_HEADERS = ("canonical", "aliases")
CHECK_NAMES = (
    "dup_key",
    "alias_maps_to_two_canonicals",
    "alias_loop",
    "empty_canonical",
    "missing_identity_row",
    "service_not_in_registry",
)

_DATE_FMT = "%Y-%m-%d"
_SERVICE_LINE_RE = re.compile(r"^\s*-?\s*service\s*:\s*(.+?)\s*$")


class SheetError(Exception):
    """CSV 검증 실패. 메시지는 행 번호·필드명·검증명만 포함 — 데이터 값 에코 금지."""


@dataclass
class AliasRow:
    alias: str
    effective_from: str
    canonical: str
    defining_service: str
    source: str
    note: str
    row_no: int  # 원본 CSV 데이터 행 번호(1부터) — 오류 메시지용, SQL에는 미기록


def _parse_date(value: str, field_label: str, row_no: int) -> str:
    try:
        dt = datetime.strptime(value, _DATE_FMT)
    except ValueError as exc:
        where = f"{row_no}번째 데이터 행: {field_label}" if row_no > 0 else field_label
        raise SheetError(f"{where} 날짜 형식 오류 (YYYY-MM-DD 필요)") from exc
    return dt.strftime(_DATE_FMT)


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def load_services(paths) -> set:
    """endpoints*.yaml 여러 파일에서 `service:` 값 집합을 읽는다 (stdlib 줄 정규식 — PyYAML 없음).

    주석 줄(`#`)은 무시, 값의 따옴표는 벗긴다. 결과가 비면 SheetError(레지스트리 없이 검증 불가).
    """
    services: set = set()
    for p in paths:
        text = Path(p).read_text(encoding="utf-8")
        for line in text.splitlines():
            if line.lstrip().startswith("#"):
                continue
            m = _SERVICE_LINE_RE.match(line.split(" #", 1)[0])
            if m:
                value = _strip_quotes(m.group(1).strip())
                if value:
                    services.add(value)
    if not services:
        raise SheetError("--services: service 항목이 하나도 없음 (endpoints*.yaml 확인)")
    return services


def _split_aliases(raw: str, row_no: int) -> list:
    raw = raw.strip()
    if raw == "":
        return []
    segments = [seg.strip() for seg in raw.split(",")]
    if any(seg == "" for seg in segments):
        raise SheetError(
            f"{row_no}번째 데이터 행: aliases 필드에 빈 세그먼트가 있음 "
            "(선행/후행 ',' 또는 연속 ',,' 금지 — empty_alias_segment)"
        )
    return segments


def parse_sheet(rows, default_effective_from, services) -> list:
    """CSV DictReader 행 목록 → 검증된 AliasRow 목록 (순수 함수, 결정적 순서).

    출력 순서: 입력 행 순서대로 [identity 행, alias 행들...]. 자동 교정 없음(strip만).
    """
    if default_effective_from is not None:
        default_effective_from = _parse_date(default_effective_from, "--effective-from", 0)
        if default_effective_from == PLACEHOLDER_EFFECTIVE_FROM:
            raise SheetError(
                f"--effective-from: {PLACEHOLDER_EFFECTIVE_FROM}은 사내 시드 플레이스홀더 키 날짜 — "
                "금지 (effective_from_is_placeholder_date)"
            )
    services = set(services)

    records: list = []  # (row_no, canonical, aliases, defining_service, effective_from, note)
    for idx, raw in enumerate(rows, start=1):
        canonical = (raw.get("canonical") or "").strip()
        if not canonical:
            raise SheetError(f"{idx}번째 데이터 행: canonical 필드가 비어 있음 (empty_canonical)")

        aliases = _split_aliases(raw.get("aliases") or "", idx)

        effective_from_raw = (raw.get("effective_from") or "").strip()
        if effective_from_raw:
            effective_from = _parse_date(effective_from_raw, "effective_from", idx)
        elif default_effective_from is not None:
            effective_from = default_effective_from
        else:
            raise SheetError(
                f"{idx}번째 데이터 행: effective_from 비어 있음 + --effective-from 미지정"
            )
        if effective_from == PLACEHOLDER_EFFECTIVE_FROM:
            raise SheetError(
                f"{idx}번째 데이터 행: effective_from이 사내 시드 플레이스홀더 키 날짜"
                f"({PLACEHOLDER_EFFECTIVE_FROM}) — 금지 (effective_from_is_placeholder_date)"
            )

        defining_service = (raw.get("defining_service") or "").strip()
        if aliases:
            if not defining_service:
                raise SheetError(
                    f"{idx}번째 데이터 행: alias 행인데 defining_service 비어 있음 "
                    "(service_not_in_registry)"
                )
            if defining_service not in services:
                raise SheetError(
                    f"{idx}번째 데이터 행: defining_service가 --services 레지스트리에 없음 "
                    "(service_not_in_registry)"
                )

        note = (raw.get("note") or "").strip()
        records.append((idx, canonical, aliases, defining_service, effective_from, note))

    # 전역 검증 1) alias_loop: 어떤 행의 canonical이 다른 행의 alias(비-identity)로 등장 — 체인 금지
    alias_owner: dict = {}
    for row_no, _canonical, aliases, _svc, _ef, _note in records:
        for a in aliases:
            alias_owner.setdefault(a, row_no)
    for row_no, canonical, _aliases, _svc, _ef, _note in records:
        # 같은 행의 aliases에 자기 canonical이 있는 경우는 identity 행과의 dup_key로 잡는다(아래 3)
        if canonical in alias_owner and alias_owner[canonical] != row_no:
            raise SheetError(
                f"{row_no}번째 데이터 행: canonical이 {alias_owner[canonical]}번째 데이터 행의 "
                "alias로도 등장 — 1단계 매핑만 허용 (alias_loop)"
            )

    # 전역 검증 2) alias_maps_to_two_canonicals / 3) dup_key — (alias, effective_from) 키 기준
    out: list = []
    key_to: dict = {}  # (alias, effective_from) -> (canonical, row_no)
    for row_no, canonical, aliases, defining_service, effective_from, note in records:
        emitted = [(canonical, "")] + [(a, defining_service) for a in aliases]
        for alias, svc in emitted:
            key = (alias, effective_from)
            if key in key_to:
                prev_canonical, prev_row = key_to[key]
                if prev_canonical != canonical:
                    raise SheetError(
                        f"{row_no}번째 데이터 행: 같은 (alias, effective_from)이 {prev_row}번째 "
                        "데이터 행과 다른 canonical로 매핑됨 (alias_maps_to_two_canonicals)"
                    )
                raise SheetError(
                    f"{row_no}번째 데이터 행: (alias, effective_from) 키 중복 "
                    f"(최초 발생: {prev_row}번째 데이터 행) (dup_key)"
                )
            key_to[key] = (canonical, row_no)
            out.append(
                AliasRow(
                    alias=alias,
                    effective_from=effective_from,
                    canonical=canonical,
                    defining_service=svc,
                    source=SOURCE_SHEET,
                    note=note,
                    row_no=row_no,
                )
            )
    return out


def _escape_sql_string(value: str) -> str:
    """이스케이프 순서: '\\' -> '\\\\' 먼저, 그다음 "'" -> "\\'" (순서 바뀌면 SQL 파손)."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _full_row_literal(row: AliasRow) -> str:
    return (
        f"SELECT '{_escape_sql_string(row.alias)}' AS alias, "
        f"toDate('{row.effective_from}') AS effective_from, "
        f"'{_escape_sql_string(row.canonical)}' AS canonical, "
        f"'{_escape_sql_string(row.defining_service)}' AS defining_service, "
        f"'{_escape_sql_string(row.source)}' AS source, "
        f"'{_escape_sql_string(row.note)}' AS note"
    )


def _chunks(rows: list, chunk_size: int):
    for i in range(0, len(rows), chunk_size):
        yield rows[i : i + chunk_size]


def _union_all(literals) -> str:
    return "\n    UNION ALL\n    ".join(literals)


def render_sql(rows, chunk_size: int, source_name: str, default_effective_from,
               target_db: str = DEFAULT_TARGET_DB) -> str:
    """검증된 AliasRow 목록 → 결정적(byte-identical) INSERT SQL 문자열 (순수 함수).

    (a) 헤더 주석(basename·행수·기본 effective_from — 생성 시각 없음)
    (b) chunk당 NOT IN 멱등 가드 INSERT + SETTINGS insert_distributed_sync = 1
    (c) '-- 검증: 결과가 비어야 정상' 앵커 뒤 검증 SELECT 6종 (전역, 시드 파일과 동일 4열
        check_name, key, effective_from, cnt) — service_not_in_registry가 마지막(레지스트리 대조).
    """
    target_table = f"{target_db}.{TABLE_NAME}"
    registry_table = f"{target_db}.{REGISTRY_TABLE_NAME}"
    chunks = list(_chunks(rows, chunk_size))
    identity_count = sum(1 for r in rows if r.alias == r.canonical)

    lines = []
    lines.append("-- =============================================================")
    lines.append(f"-- {target_table} 메타데이터 시트 `모델` 탭 INSERT")
    lines.append("-- 생성: sheet_to_dim_token_model_alias_insert.py (Plan 6a T8)")
    lines.append(f"-- 소스 파일: {source_name}")
    lines.append(f"-- 행수: {len(rows)} (identity {identity_count}, alias {len(rows) - identity_count})")
    lines.append(f"-- 기본 effective_from: {default_effective_from or '(행별 값만)'}")
    lines.append("-- 경고: 실시트 산출물(이 파일)은 레포·사외 환경 반입 금지 (§7.2, .gitignore 커버)")
    lines.append("-- 실행 주체: admin 수동 — 사내 절차 리뷰 후 실행. 재매핑은 새 effective_from 행 append (기존 행 불변)")
    lines.append("-- =============================================================")
    lines.append("")

    for chunk in chunks:
        lines.append(f"INSERT INTO {target_table}")
        lines.append("    (alias, effective_from, canonical, defining_service, source, note)")
        lines.append("SELECT *")
        lines.append("FROM (")
        lines.append("    " + _union_all(_full_row_literal(r) for r in chunk))
        lines.append(")")
        lines.append("WHERE (alias, effective_from) NOT IN (")
        lines.append(f"    SELECT alias, effective_from FROM {target_table}")
        lines.append(")")
        lines.append("SETTINGS insert_distributed_sync = 1;")
        lines.append("")

    lines.append("-- 검증: 결과가 비어야 정상 ------------------------------------------------")
    lines.append("-- 1) dup_key: (alias, effective_from) 전역 중복 없음")
    lines.append("SELECT 'dup_key' AS check_name, alias AS key, effective_from, count() AS cnt")
    lines.append(f"FROM {target_table}")
    lines.append("GROUP BY alias, effective_from")
    lines.append("HAVING count() > 1")
    lines.append("")
    lines.append("UNION ALL")
    lines.append("")
    lines.append("-- 2) alias_maps_to_two_canonicals: 같은 (alias, effective_from)이 서로 다른 canonical로")
    lines.append("SELECT 'alias_maps_to_two_canonicals', alias, effective_from, uniqExact(canonical)")
    lines.append(f"FROM {target_table}")
    lines.append("GROUP BY alias, effective_from")
    lines.append("HAVING uniqExact(canonical) > 1")
    lines.append("")
    lines.append("UNION ALL")
    lines.append("")
    lines.append("-- 3) alias_loop: 비-identity 행의 canonical이 다시 다른 비-identity 행의 alias (1단계 매핑만)")
    lines.append("SELECT 'alias_loop', alias, effective_from, toUInt64(1)")
    lines.append(f"FROM {target_table}")
    lines.append("WHERE alias != canonical")
    lines.append("  AND canonical GLOBAL IN (")
    lines.append(f"      SELECT alias FROM {target_table} WHERE alias != canonical")
    lines.append("  )")
    lines.append("")
    lines.append("UNION ALL")
    lines.append("")
    lines.append("-- 4) empty_canonical: canonical 빈 문자열 금지")
    lines.append("SELECT 'empty_canonical', alias, effective_from, toUInt64(1)")
    lines.append(f"FROM {target_table}")
    lines.append("WHERE canonical = ''")
    lines.append("")
    lines.append("UNION ALL")
    lines.append("")
    lines.append("-- 5) missing_identity_row: 모든 canonical은 identity 행(alias = canonical)을 가져야 함")
    lines.append("SELECT 'missing_identity_row', canonical, min(effective_from), count()")
    lines.append(f"FROM {target_table}")
    lines.append("WHERE canonical != ''")
    lines.append("  AND canonical GLOBAL NOT IN (")
    lines.append(f"      SELECT alias FROM {target_table} WHERE alias = canonical")
    lines.append("  )")
    lines.append("GROUP BY canonical")
    lines.append("")
    lines.append("UNION ALL")
    lines.append("")
    lines.append("-- 6) service_not_in_registry: alias 행의 defining_service가 메트릭 레지스트리에 없음")
    lines.append(f"--    (레지스트리 {registry_table}는 collectors/token-metrics 정기 실행이 동기화 — 설계 §4.3)")
    lines.append("SELECT 'service_not_in_registry', alias, effective_from, toUInt64(1)")
    lines.append(f"FROM {target_table}")
    lines.append("WHERE defining_service != ''")
    lines.append("  AND defining_service GLOBAL NOT IN (")
    lines.append(f"      SELECT service FROM {registry_table}")
    lines.append("  );")
    lines.append("")
    return "\n".join(lines) + "\n"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="메타데이터 시트 `모델` 탭 CSV -> gpu_data.dim_token_model_alias_dist INSERT SQL 생성기"
    )
    parser.add_argument("--csv", required=True, help="입력 CSV 경로 (헤더 canonical,aliases,defining_service,effective_from,note)")
    parser.add_argument(
        "--effective-from",
        default=None,
        help="행의 effective_from이 빈 경우의 기본값 (YYYY-MM-DD, 소급 시작일 — 2026-01-01 금지)",
    )
    parser.add_argument(
        "--services",
        action="append",
        required=True,
        help="endpoints*.yaml 경로 (반복 가능) — defining_service 대조용 레지스트리",
    )
    parser.add_argument("--out", default=DEFAULT_OUT_FILENAME, help=f"출력 SQL 경로 (기본: {DEFAULT_OUT_FILENAME})")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help=f"INSERT chunk 크기 (기본 {DEFAULT_CHUNK_SIZE})")
    parser.add_argument(
        "--target-db",
        default=DEFAULT_TARGET_DB,
        choices=TARGET_DB_CHOICES,
        help="INSERT 대상 dim DB명 (기본: gpu_data — company-verify는 token_verify_dim)",
    )
    return parser


def main(argv=None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.chunk_size <= 0:
        parser.error("--chunk-size는 1 이상이어야 함")

    csv_path = Path(args.csv)
    try:
        with csv_path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames or []
            raw_rows = list(reader)
    except OSError:
        parser.error(f"--csv 파일을 열 수 없음: {csv_path.name}")
        return 2  # pragma: no cover — parser.error already exits
    except UnicodeDecodeError:
        print(
            f"검증 실패: --csv 파일이 UTF-8이 아님(Excel은 'CSV UTF-8'로 저장): {csv_path.name}",
            file=sys.stderr,
        )
        return 1

    missing = [h for h in REQUIRED_HEADERS if h not in header]
    if missing:
        print(f"검증 실패: CSV 헤더에 필수 컬럼 없음: {', '.join(missing)}", file=sys.stderr)
        return 1

    try:
        services = load_services(args.services)
        rows = parse_sheet(raw_rows, args.effective_from, services)
    except OSError as exc:
        print(f"검증 실패: --services 파일을 열 수 없음: {Path(str(exc.filename or '')).name}", file=sys.stderr)
        return 1
    except SheetError as exc:
        print(f"검증 실패: {exc}", file=sys.stderr)
        return 1

    sql_text = render_sql(rows, args.chunk_size, csv_path.name, args.effective_from, args.target_db)
    out_path = Path(args.out)
    try:
        out_path.write_text(sql_text, encoding="utf-8")
    except OSError:
        print(f"검증 실패: --out 파일을 쓸 수 없음: {out_path.name}", file=sys.stderr)
        return 1

    identity_count = sum(1 for r in rows if r.alias == r.canonical)
    num_chunks = (len(rows) + args.chunk_size - 1) // args.chunk_size if rows else 0
    print(f"생성 완료: {out_path.name}")   # 경로 미출력 — tmp 경로 문자열이 stdout 위생 검사('claude-' 등)에 걸리지 않게
    print(f"입력 데이터 행수: {len(raw_rows)} → 출력 행수: {len(rows)} (identity {identity_count}, alias {len(rows) - identity_count})")
    print(f"레지스트리 서비스 수: {len(services)} (--services {len(args.services)}개 파일)")
    print(f"chunk 크기: {args.chunk_size} (chunk 수: {num_chunks})")
    print(
        "검증: 출력 SQL 말미 \"-- 검증: 결과가 비어야 정상\" 섹션 실행 후 결과가 비어 있어야 정상 "
        "(admin 리뷰 절차; service_not_in_registry는 레지스트리 동기화 이후에만 유의미)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
