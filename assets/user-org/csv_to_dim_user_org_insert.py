#!/usr/bin/env python3
"""CSV 로스터 → gpu_data.dim_token_user_org_dist INSERT SQL 생성기 (Plan 4 T2).

stdlib만 사용(csv/argparse/dataclasses). Python 3.10.

CSV 계약(§6.1): 헤더 `user_id,user_name,org,is_active,effective_from`.
  - org: '>' 구분 단일 컬럼 (예: 'A부문>X팀').
  - is_active: 선택, 기본 1.
  - effective_from: 선택, 없으면 --effective-from, 그것도 없으면 DEFAULT_EFFECTIVE_FROM.

출력: gpu_data.dim_token_user_org_dist에 대한 멱등(NOT IN 가드) INSERT SQL +
  말미 검증 SELECT(dup_key 전역 / missing_key·key_conflict chunk별).

데이터 경계(§7.2): 실로스터 CSV·생성 SQL은 레포 반입 금지 — .gitignore가
  `*roster*.csv`, `dim_token_user_org_insert*.sql` 패턴으로 선제 차단한다.
  이 스크립트의 stdout은 요약(행수·카운트)만 출력하고, 데이터 행(실명·조직명)은
  성공/실패 경로 모두에서 절대 에코하지 않는다.

exit code: 0 성공 / 1 검증 실패(RosterError) / 2 인자·입력 오류(argparse).
"""
from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# effective_from 미지정 시 기본 과거 기준일 — "이력 시작 이전(레코드 부재 구간)
# 전부 이 값으로 유효하다"는 의미의 상수. 실제 최초 투입일이 아니라 관례적 하한선.
DEFAULT_EFFECTIVE_FROM = "2020-01-01"

DEFAULT_OUT_FILENAME = "dim_token_user_org_insert.sql"
DEFAULT_CHUNK_SIZE = 500

ANON_PREFIX = "anon-"

TARGET_TABLE = "gpu_data.dim_token_user_org_dist"

_DATE_FMT = "%Y-%m-%d"


class RosterError(Exception):
    """CSV 검증 실패. 메시지는 행 번호·필드명만 포함 — 데이터 값 에코 금지."""


@dataclass
class Row:
    user_id: str
    user_name: str
    org_path: list
    org_depth: int
    is_active: int
    effective_from: str


def _parse_date(value: str, field_label: str, row_no: int) -> str:
    """YYYY-MM-DD 형식 검증 + 정규화된 문자열 반환. 실패 시 RosterError(행 번호·필드명만)."""
    try:
        dt = datetime.strptime(value, _DATE_FMT)
    except ValueError as exc:
        raise RosterError(
            f"{row_no}번째 데이터 행: {field_label} 날짜 형식 오류 (YYYY-MM-DD 필요)"
        ) from exc
    return dt.strftime(_DATE_FMT)


def parse_roster(rows, default_effective_from: str) -> list:
    """CSV DictReader 행 목록 → 검증된 Row 목록 (순수 함수, 입력 행 순서 보존).

    위반 시 RosterError(행 번호·필드명만 — 데이터 값 에코 금지).
    """
    # default_effective_from 자체도 형식 검증 (row_no=0 — CLI 인자/디폴트 값 문맥)
    default_effective_from = _parse_date(default_effective_from, "default_effective_from", 0)

    parsed: list = []
    seen_keys: dict = {}

    for idx, raw in enumerate(rows, start=1):
        user_id = (raw.get("user_id") or "").strip()
        if not user_id:
            raise RosterError(f"{idx}번째 데이터 행: user_id 필드가 비어 있음")

        org_raw = (raw.get("org") or "").strip()
        if not org_raw:
            raise RosterError(f"{idx}번째 데이터 행: org 필드가 비어 있음")

        segments = org_raw.split(">")
        if any(seg.strip() == "" for seg in segments):
            raise RosterError(
                f"{idx}번째 데이터 행: org 필드에 빈 세그먼트가 있음 "
                "(선행/후행 '>' 또는 연속 '>>' 금지)"
            )
        org_path = [seg.strip() for seg in segments]
        org_depth = len(org_path)

        effective_from_raw = (raw.get("effective_from") or "").strip()
        effective_from = effective_from_raw or default_effective_from
        effective_from = _parse_date(effective_from, "effective_from", idx)

        is_active_raw = (raw.get("is_active") or "").strip()
        if is_active_raw == "":
            is_active = 1
        elif is_active_raw in ("0", "1"):
            is_active = int(is_active_raw)
        else:
            raise RosterError(f"{idx}번째 데이터 행: is_active 필드 값이 0/1이 아님")

        key = (user_id, effective_from)
        if key in seen_keys:
            first_row_no = seen_keys[key]
            raise RosterError(
                f"{idx}번째 데이터 행: (user_id, effective_from) 키 중복 "
                f"(최초 발생: {first_row_no}번째 데이터 행)"
            )
        seen_keys[key] = idx

        # anon-* user_name 강제 빈 문자열 로직 폐지 (2026-07-14 결정) — 비실명 핸들명은
        # 로스터에 그대로 보존한다. 실명 기입 금지는 이 도구가 판별할 수 없으므로
        # 사내 투입 리뷰에서 확인한다 — main()의 stderr 안내가 건수로 리마인드한다.
        user_name = (raw.get("user_name") or "").strip()

        parsed.append(
            Row(
                user_id=user_id,
                user_name=user_name,
                org_path=org_path,
                org_depth=org_depth,
                is_active=is_active,
                effective_from=effective_from,
            )
        )

    return parsed


def _escape_sql_string(value: str) -> str:
    """이스케이프 순서: '\\' -> '\\\\' 먼저, 그다음 "'" -> "\\'" (순서 바뀌면 SQL 파손)."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _array_literal(org_path) -> str:
    return "[" + ",".join(f"'{_escape_sql_string(seg)}'" for seg in org_path) + "]"


def _full_row_literal(row: Row) -> str:
    return (
        f"SELECT '{_escape_sql_string(row.user_id)}' AS user_id, "
        f"toDate('{row.effective_from}') AS effective_from, "
        f"'{_escape_sql_string(row.user_name)}' AS user_name, "
        f"{_array_literal(row.org_path)} AS org_path, "
        f"{row.org_depth} AS org_depth, "
        f"{row.is_active} AS is_active, "
        f"now('Asia/Seoul') AS updated_at"
    )


def _key_literal(row: Row) -> str:
    return (
        f"SELECT '{_escape_sql_string(row.user_id)}' AS user_id, "
        f"toDate('{row.effective_from}') AS effective_from"
    )


def _conflict_row_literal(row: Row) -> str:
    return (
        f"SELECT '{_escape_sql_string(row.user_id)}' AS user_id, "
        f"toDate('{row.effective_from}') AS effective_from, "
        f"'{_escape_sql_string(row.user_name)}' AS user_name, "
        f"{_array_literal(row.org_path)} AS org_path, "
        f"{row.is_active} AS is_active"
    )


def _chunks(rows: list, chunk_size: int):
    for i in range(0, len(rows), chunk_size):
        yield rows[i : i + chunk_size]


def _union_all(literals) -> str:
    return "\n    UNION ALL\n    ".join(literals)


def render_sql(rows, chunk_size: int, source_name: str, default_effective_from: str) -> str:
    """검증된 Row 목록 → 결정적(byte-identical) INSERT SQL 문자열 (순수 함수).

    (a) 헤더 주석(basename·행수·기본 effective_from, 생성 시각 없음 — 결정성)
    (b) chunk당 NOT IN 멱등 가드 INSERT (SETTINGS insert_distributed_sync = 1은 INSERT에만)
    (c) 말미 검증 SELECT — 섹션 시작 리터럴은 seed 파일과 동일:
        '-- 검증: 결과가 비어야 정상' (T3 sed 앵커)
        - dup_key: 전역(비chunk) 1문
        - missing_key / key_conflict: INSERT와 동일 chunk_size로 분할된 각 chunk별 위반 행 노출문
          (max_query_size 256KiB 한계 회피 — count 비교가 아니라 위반 행 직접 노출)
    """
    chunks = list(_chunks(rows, chunk_size))

    lines = []
    lines.append("-- =============================================================")
    lines.append("-- gpu_data.dim_token_user_org 로스터 INSERT")
    lines.append("-- 생성: csv_to_dim_user_org_insert.py (Plan 4 T2)")
    lines.append(f"-- 소스 파일: {source_name}")
    lines.append(f"-- 행수: {len(rows)}")
    lines.append(f"-- 기본 effective_from: {default_effective_from}")
    lines.append("-- 경고: 실로스터 산출물(이 파일)은 레포·사외 환경 반입 금지 (§7.2, .gitignore 커버)")
    lines.append("-- 실행 주체: admin 수동 — 사내 절차 리뷰 후 실행 (§6.1)")
    lines.append("-- =============================================================")
    lines.append("")

    for chunk in chunks:
        lines.append(f"INSERT INTO {TARGET_TABLE}")
        lines.append(
            "    (user_id, effective_from, user_name, org_path, org_depth, is_active, updated_at)"
        )
        lines.append("SELECT *")
        lines.append("FROM (")
        lines.append("    " + _union_all(_full_row_literal(r) for r in chunk))
        lines.append(")")
        lines.append("WHERE (user_id, effective_from) NOT IN (")
        lines.append(f"    SELECT user_id, effective_from FROM {TARGET_TABLE}")
        lines.append(")")
        lines.append("SETTINGS insert_distributed_sync = 1;")
        lines.append("")

    lines.append("-- 검증: 결과가 비어야 정상 ------------------------------------------------")
    lines.append("-- 1) dup_key: (user_id, effective_from) 전역 중복 없음")
    lines.append("SELECT 'dup_key' AS check_name, user_id, effective_from, count() AS cnt")
    lines.append(f"FROM {TARGET_TABLE}")
    lines.append("GROUP BY user_id, effective_from")
    lines.append("HAVING count() > 1;")
    lines.append("")

    total_chunks = len(chunks)
    for chunk_idx, chunk in enumerate(chunks, start=1):
        lines.append(
            f"-- 2-{chunk_idx}) missing_key: 파일 키가 dim에 없는 경우 노출 "
            f"(chunk {chunk_idx}/{total_chunks})"
        )
        lines.append(
            "SELECT 'missing_key' AS check_name, "
            "chunk_keys.user_id AS user_id, chunk_keys.effective_from AS effective_from"
        )
        lines.append("FROM (")
        lines.append("    " + _union_all(_key_literal(r) for r in chunk))
        lines.append(") AS chunk_keys")
        lines.append("WHERE (chunk_keys.user_id, chunk_keys.effective_from) NOT IN (")
        lines.append(f"    SELECT user_id, effective_from FROM {TARGET_TABLE}")
        lines.append(");")
        lines.append("")

        lines.append(
            f"-- 3-{chunk_idx}) key_conflict: 동일 키의 파일<->dim 내용 불일치 노출 "
            f"(chunk {chunk_idx}/{total_chunks})"
        )
        lines.append("-- 동일 키 정정은 금지 — 새 effective_from 행 또는 §8.4 정정 절차")
        lines.append(
            "SELECT 'key_conflict' AS check_name, "
            "file_kv.user_id AS user_id, file_kv.effective_from AS effective_from"
        )
        lines.append("FROM (")
        lines.append("    " + _union_all(_conflict_row_literal(r) for r in chunk))
        lines.append(") AS file_kv")
        lines.append(f"INNER JOIN {TARGET_TABLE} AS db")
        lines.append("    ON file_kv.user_id = db.user_id AND file_kv.effective_from = db.effective_from")
        lines.append("WHERE file_kv.user_name != db.user_name")
        lines.append("   OR file_kv.org_path != db.org_path")
        lines.append("   OR file_kv.is_active != db.is_active;")
        lines.append("")

    return "\n".join(lines) + "\n"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="CSV 로스터 -> gpu_data.dim_token_user_org_dist INSERT SQL 생성기"
    )
    parser.add_argument("--csv", required=True, help="입력 CSV 경로")
    parser.add_argument(
        "--effective-from",
        default=None,
        help=f"기본 effective_from (YYYY-MM-DD). 미지정 시 {DEFAULT_EFFECTIVE_FROM}",
    )
    parser.add_argument(
        "--out",
        default=DEFAULT_OUT_FILENAME,
        help=f"출력 SQL 경로 (기본: {DEFAULT_OUT_FILENAME})",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"INSERT/검증 chunk 크기 (기본 {DEFAULT_CHUNK_SIZE})",
    )
    return parser


def main(argv=None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.chunk_size <= 0:
        parser.error("--chunk-size는 1 이상이어야 함")

    default_effective_from = args.effective_from or DEFAULT_EFFECTIVE_FROM
    try:
        default_effective_from = _parse_date(default_effective_from, "--effective-from", 0)
    except RosterError:
        parser.error("--effective-from 형식 오류 (YYYY-MM-DD 필요)")

    csv_path = Path(args.csv)
    try:
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            raw_rows = list(reader)
    except OSError:
        parser.error(f"--csv 파일을 열 수 없음: {csv_path.name}")
        return 2  # pragma: no cover — parser.error already exits

    try:
        rows = parse_roster(raw_rows, default_effective_from)
    except RosterError as exc:
        print(f"검증 실패: {exc}", file=sys.stderr)
        return 1

    anon_count = sum(1 for r in rows if r.user_id.startswith(ANON_PREFIX))

    sql_text = render_sql(rows, args.chunk_size, csv_path.name, default_effective_from)

    out_path = Path(args.out)
    out_path.write_text(sql_text, encoding="utf-8")

    num_chunks = (len(rows) + args.chunk_size - 1) // args.chunk_size if rows else 0

    print(f"생성 완료: {out_path}")
    print(f"입력 행수: {len(rows)}")
    print(f"chunk 크기: {args.chunk_size} (chunk 수: {num_chunks})")
    # 카운트만(데이터 원문 미출력, §7.2) — anon user_name은 이제 강제 치환하지 않고
    # 값을 보존하므로, 실명 기입 금지 확인 책임이 도구가 아니라 투입 리뷰로 이동했음을
    # 매 실행마다 상기시킨다.
    print(
        f"anon {anon_count}행: user_name은 비실명 핸들명이어야 함"
        "(실명 기입 금지 — 투입 리뷰 확인)",
        file=sys.stderr,
    )
    print(
        "검증: 출력 SQL 말미 \"-- 검증: 결과가 비어야 정상\" 섹션 실행 후 "
        "결과가 비어 있어야 정상 (admin 리뷰 절차)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
