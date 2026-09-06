#!/usr/bin/env python3
"""Layer C 기준정보 CSV → gpu_data.dim_token_{gpu_tco,gpu_allocation,vendor_price}_dist INSERT SQL 생성기 (Plan 6a T9).

`sheet_to_dim_token_model_alias_insert.py`(T8)와 같은 골격 — stdlib만 사용(csv/argparse/dataclasses).
Python 3.10+. `--table` 1개로 3테이블을 다루며, 테이블별 계약은 TABLE_SPECS에만 있다.

CSV 계약(설계 2026-08-31 §4.2 — 통화 KRW 고정, effective_from = 소급 시작일):
  gpu_tco        헤더 gpu_type,effective_from,tco_krw_per_gpu_hour,basis,note
                 필수 gpu_type,tco_krw_per_gpu_hour. basis ∈ {'', depreciation, lease, power-inclusive, tco}.
  gpu_allocation 헤더 service_group,gpu_type,effective_from,allocated_gpu_count,source,note
                 필수 service_group,gpu_type,allocated_gpu_count. source 빈 값 → 'manual'. 철회는 0.
  vendor_price   헤더 provider,model,tier,effective_from,krw_per_mtok_input,krw_per_mtok_cached,
                 krw_per_mtok_cache_creation,krw_per_mtok_output,note
                 필수 provider,model,krw_per_mtok_input,krw_per_mtok_output. tier 빈 값 → 'standard'
                 (∈ standard|batch|flex|priority). cached/cache_creation 빈 값 → NULL.
  공통: 선택 컬럼 currency는 있으면 ''/'KRW'만 허용(currency_krw). 숫자 컬럼은 빈 값 → NULL(플레이스홀더),
        음수 금지(negative_value). 키 값 'unknown'은 시드 플레이스홀더 예약어 — 금지(unknown_reserved).
        effective_from 빈 값 → --effective-from(둘 다 없으면 오류); 2026-01-01(시드 플레이스홀더 키) 금지.
        자동 교정 없음(strip만). 키 튜플 중복 금지(dup_key).

출력: 멱등(NOT IN 가드) INSERT SQL + `SETTINGS insert_distributed_sync = 1;` + 말미
  `-- 검증: 결과가 비어야 정상` 앵커 뒤 검증 SELECT — 시드 파일(seed_dim_token_*.sql)과 동일 항목·4열
  (check_name, key, effective_from, cnt).

데이터 경계(§7.2): 실 CSV·생성 SQL은 레포 반입 금지 — .gitignore가 `*gpu_tco*.csv`, `*gpu_allocation*.csv`,
  `*vendor_price*.csv`, `dim_token_gpu_*_insert*.sql`, `dim_token_vendor_price_insert*.sql`로 차단.
  stdout은 요약(건수)만, 데이터 원문(기종·그룹·모델·단가)은 성공/실패 경로 모두에서 에코하지 않는다.

exit code: 0 성공 / 1 검증 실패(LayerCError) / 2 인자·입력 오류(argparse).
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

DEFAULT_CHUNK_SIZE = 500
DEFAULT_TARGET_DB = "gpu_data"
TARGET_DB_CHOICES = ("gpu_data", "token_verify_dim")
PLACEHOLDER_EFFECTIVE_FROM = "2026-01-01"
RESERVED_KEY_VALUE = "unknown"
CURRENCY = "KRW"
BASIS_DOMAIN = ("", "depreciation", "lease", "power-inclusive", "tco")
TIER_DOMAIN = ("standard", "batch", "flex", "priority")

_DATE_FMT = "%Y-%m-%d"


class LayerCError(Exception):
    """CSV 검증 실패. 메시지는 행 번호·필드명·검증명만 포함 — 데이터 값 에코 금지."""


@dataclass(frozen=True)
class TableSpec:
    name: str                 # --table 값
    table: str                # <db>.<table>_dist 의 테이블 부분
    key_columns: tuple        # NOT IN 가드 키 (effective_from 포함)
    string_columns: tuple     # (컬럼, 필수 여부, 기본값)
    numeric_columns: tuple    # (컬럼, 필수 여부)
    trailing_columns: tuple   # 문자열 후행 컬럼 (기본값 '')
    default_out: str
    check_names: tuple


TABLE_SPECS = {
    "gpu_tco": TableSpec(
        name="gpu_tco",
        table="dim_token_gpu_tco",
        key_columns=("gpu_type", "effective_from"),
        string_columns=(("gpu_type", True, ""),),
        numeric_columns=(("tco_krw_per_gpu_hour", True),),
        trailing_columns=("currency", "basis", "note"),
        default_out="dim_token_gpu_tco_insert.sql",
        check_names=("dup_key", "unknown_row_state", "basis_domain", "currency_krw"),
    ),
    "gpu_allocation": TableSpec(
        name="gpu_allocation",
        table="dim_token_gpu_allocation",
        key_columns=("service_group", "gpu_type", "effective_from"),
        string_columns=(("service_group", True, ""), ("gpu_type", True, "")),
        numeric_columns=(("allocated_gpu_count", True),),
        trailing_columns=("source", "note"),
        default_out="dim_token_gpu_allocation_insert.sql",
        check_names=("dup_key", "unknown_row_state", "negative_count"),
    ),
    "vendor_price": TableSpec(
        name="vendor_price",
        table="dim_token_vendor_price",
        key_columns=("provider", "model", "tier", "effective_from"),
        string_columns=(("provider", True, ""), ("model", True, ""), ("tier", False, "standard")),
        numeric_columns=(
            ("krw_per_mtok_input", True),
            ("krw_per_mtok_cached", False),
            ("krw_per_mtok_cache_creation", False),
            ("krw_per_mtok_output", True),
        ),
        trailing_columns=("note",),
        default_out="dim_token_vendor_price_insert.sql",
        check_names=("dup_key", "unknown_row_state", "tier_domain"),
    ),
}
TABLE_CHOICES = tuple(TABLE_SPECS)


def required_headers(spec: TableSpec) -> tuple:
    return tuple(c for c, req, _d in spec.string_columns if req) + tuple(c for c, req in spec.numeric_columns if req)


def insert_columns(spec: TableSpec) -> tuple:
    """INSERT 컬럼 순서 = DDL 컬럼 순서: 문자열 키 → effective_from → 숫자 → 후행."""
    return (
        tuple(c for c, _r, _d in spec.string_columns)
        + ("effective_from",)
        + tuple(c for c, _r in spec.numeric_columns)
        + spec.trailing_columns
    )


@dataclass
class DimRow:
    values: dict     # 컬럼명 → str | float | None  (effective_from은 'YYYY-MM-DD' 문자열)
    row_no: int      # 원본 CSV 데이터 행 번호(1부터) — 오류 메시지용, SQL 미기록

    def key(self, spec: TableSpec) -> tuple:
        return tuple(self.values[c] for c in spec.key_columns)


def _parse_date(value: str, field_label: str, row_no: int) -> str:
    try:
        dt = datetime.strptime(value, _DATE_FMT)
    except ValueError as exc:
        where = f"{row_no}번째 데이터 행: {field_label}" if row_no > 0 else field_label
        raise LayerCError(f"{where} 날짜 형식 오류 (YYYY-MM-DD 필요)") from exc
    return dt.strftime(_DATE_FMT)


def _parse_number(raw: str, column: str, row_no: int):
    raw = raw.strip()
    if raw == "":
        return None
    try:
        value = float(raw.replace(",", ""))
    except ValueError as exc:
        raise LayerCError(f"{row_no}번째 데이터 행: {column} 숫자 형식 오류 (bad_number)") from exc
    if math.isnan(value) or math.isinf(value):
        raise LayerCError(f"{row_no}번째 데이터 행: {column} 숫자 형식 오류 (bad_number)")
    if value < 0:
        raise LayerCError(f"{row_no}번째 데이터 행: {column} 음수 금지 — 철회는 0 (negative_value)")
    return value


def parse_rows(spec: TableSpec, rows, default_effective_from) -> list:
    """CSV DictReader 행 목록 → 검증된 DimRow 목록 (순수 함수, 입력 순서 유지, 자동 교정 없음)."""
    if default_effective_from is not None:
        default_effective_from = _parse_date(default_effective_from, "--effective-from", 0)
        if default_effective_from == PLACEHOLDER_EFFECTIVE_FROM:
            raise LayerCError(
                f"--effective-from: {PLACEHOLDER_EFFECTIVE_FROM}은 사내 시드 플레이스홀더 키 날짜 — "
                "금지 (effective_from_is_placeholder_date)"
            )

    out: list = []
    seen: dict = {}
    for idx, raw in enumerate(rows, start=1):
        values: dict = {}
        for column, required, default in spec.string_columns:
            v = (raw.get(column) or "").strip()
            if v == "" and default:
                v = default
            if required and v == "":
                raise LayerCError(f"{idx}번째 데이터 행: {column} 필드가 비어 있음 (empty_key)")
            if v == RESERVED_KEY_VALUE:
                raise LayerCError(
                    f"{idx}번째 데이터 행: {column}='unknown'은 시드 플레이스홀더 예약어 (unknown_reserved)"
                )
            values[column] = v

        ef_raw = (raw.get("effective_from") or "").strip()
        if ef_raw:
            effective_from = _parse_date(ef_raw, "effective_from", idx)
        elif default_effective_from is not None:
            effective_from = default_effective_from
        else:
            raise LayerCError(f"{idx}번째 데이터 행: effective_from 비어 있음 + --effective-from 미지정")
        if effective_from == PLACEHOLDER_EFFECTIVE_FROM:
            raise LayerCError(
                f"{idx}번째 데이터 행: effective_from이 사내 시드 플레이스홀더 키 날짜"
                f"({PLACEHOLDER_EFFECTIVE_FROM}) — 금지 (effective_from_is_placeholder_date)"
            )
        values["effective_from"] = effective_from

        for column, required in spec.numeric_columns:
            if required and column not in raw:
                raise LayerCError(f"CSV 헤더에 필수 컬럼 없음: {column}")
            values[column] = _parse_number(raw.get(column) or "", column, idx)

        currency = (raw.get("currency") or "").strip()
        if currency not in ("", CURRENCY):
            raise LayerCError(f"{idx}번째 데이터 행: currency는 KRW 고정 (currency_krw)")
        for column in spec.trailing_columns:
            if column == "currency":
                values[column] = CURRENCY
            elif column == "source":
                values[column] = (raw.get(column) or "").strip() or "manual"
            else:
                values[column] = (raw.get(column) or "").strip()

        if spec.name == "gpu_tco" and values["basis"] not in BASIS_DOMAIN:
            raise LayerCError(
                f"{idx}번째 데이터 행: basis 도메인 위반 — 허용: depreciation|lease|power-inclusive|tco|빈 값 (basis_domain)"
            )
        if spec.name == "vendor_price" and values["tier"] not in TIER_DOMAIN:
            raise LayerCError(
                f"{idx}번째 데이터 행: tier 도메인 위반 — 허용: standard|batch|flex|priority (tier_domain)"
            )

        row = DimRow(values=values, row_no=idx)
        key = row.key(spec)
        if key in seen:
            raise LayerCError(
                f"{idx}번째 데이터 행: 키 {spec.key_columns} 중복 (최초 발생: {seen[key]}번째 데이터 행) (dup_key)"
            )
        seen[key] = idx
        out.append(row)
    return out


def _escape_sql_string(value: str) -> str:
    """이스케이프 순서: '\\' -> '\\\\' 먼저, 그다음 "'" -> "\\'" (순서 바뀌면 SQL 파손)."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _literal(column: str, value, with_alias: bool) -> str:
    if column == "effective_from":
        expr = f"toDate('{value}')"
    elif value is None:
        expr = "CAST(NULL AS Nullable(Float64))"
    elif isinstance(value, float):
        expr = f"CAST({value!r} AS Nullable(Float64))"
    else:
        expr = f"'{_escape_sql_string(value)}'"
    return f"{expr} AS {column}" if with_alias else expr


def _row_select(spec: TableSpec, row: DimRow, with_alias: bool) -> str:
    return "SELECT " + ", ".join(_literal(c, row.values[c], with_alias) for c in insert_columns(spec))


def _chunks(rows: list, chunk_size: int):
    for i in range(0, len(rows), chunk_size):
        yield rows[i : i + chunk_size]


def _key_expr(spec: TableSpec) -> str:
    string_keys = [c for c in spec.key_columns if c != "effective_from"]
    if len(string_keys) == 1:
        return string_keys[0]
    return "concat(" + ", '/', ".join(string_keys) + ")"


def _verification(spec: TableSpec, target_table: str) -> list:
    key_expr = _key_expr(spec)
    key_list = ", ".join(spec.key_columns)
    lines = ["-- 검증: 결과가 비어야 정상 ------------------------------------------------"]
    lines += [
        f"-- 1) dup_key: ({key_list}) 키 중복 없음",
        f"SELECT 'dup_key' AS check_name, {key_expr} AS key, effective_from, count() AS cnt",
        f"FROM {target_table}",
        f"GROUP BY {key_list}",
        "HAVING count() > 1",
        "",
        "UNION ALL",
        "",
    ]
    if spec.name == "gpu_tco":
        lines += [
            "-- 2) unknown_row_state: unknown 행 존재 + TCO 전부 NULL",
            "SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'), countIf(tco_krw_per_gpu_hour IS NOT NULL)",
            f"FROM {target_table}",
            "WHERE gpu_type = 'unknown'",
            "HAVING count() = 0 OR countIf(tco_krw_per_gpu_hour IS NOT NULL) > 0",
            "",
            "UNION ALL",
            "",
            "-- 3) basis_domain",
            "SELECT 'basis_domain', gpu_type, effective_from, toUInt64(1)",
            f"FROM {target_table}",
            "WHERE basis NOT IN ('', 'depreciation', 'lease', 'power-inclusive', 'tco')",
            "",
            "UNION ALL",
            "",
            "-- 4) currency_krw: 통화 KRW 고정",
            "SELECT 'currency_krw', gpu_type, effective_from, toUInt64(1)",
            f"FROM {target_table}",
            "WHERE currency != 'KRW';",
        ]
    elif spec.name == "gpu_allocation":
        lines += [
            "-- 2) unknown_row_state: 플레이스홀더 행 존재 + gpu_type='unknown' 행은 전부 NULL",
            "SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'), countIf(allocated_gpu_count IS NOT NULL)",
            f"FROM {target_table}",
            "WHERE gpu_type = 'unknown'",
            "HAVING count() = 0 OR countIf(allocated_gpu_count IS NOT NULL) > 0",
            "",
            "UNION ALL",
            "",
            "-- 3) negative_count: 음수 할당 금지 (철회는 0 행)",
            f"SELECT 'negative_count', {key_expr}, effective_from, toUInt64(1)",
            f"FROM {target_table}",
            "WHERE allocated_gpu_count < 0;",
        ]
    else:
        lines += [
            "-- 2) unknown_row_state: unknown 행 존재 + 단가 전부 NULL",
            "SELECT 'unknown_row_state', 'unknown', toDate('2026-01-01'),",
            "       countIf(krw_per_mtok_input IS NOT NULL OR krw_per_mtok_cached IS NOT NULL",
            "               OR krw_per_mtok_cache_creation IS NOT NULL OR krw_per_mtok_output IS NOT NULL)",
            f"FROM {target_table}",
            "WHERE provider = 'unknown' AND model = 'unknown'",
            "HAVING count() = 0",
            "    OR countIf(krw_per_mtok_input IS NOT NULL OR krw_per_mtok_cached IS NOT NULL",
            "               OR krw_per_mtok_cache_creation IS NOT NULL OR krw_per_mtok_output IS NOT NULL) > 0",
            "",
            "UNION ALL",
            "",
            "-- 3) tier_domain",
            f"SELECT 'tier_domain', {key_expr}, effective_from, toUInt64(1)",
            f"FROM {target_table}",
            "WHERE tier NOT IN ('standard', 'batch', 'flex', 'priority');",
        ]
    lines.append("")
    return lines


def render_sql(spec: TableSpec, rows: list, chunk_size: int, source_name: str, default_effective_from,
               target_db: str = DEFAULT_TARGET_DB) -> str:
    """검증된 DimRow 목록 → 결정적(byte-identical) INSERT SQL 문자열 (순수 함수)."""
    target_table = f"{target_db}.{spec.table}_dist"
    columns = insert_columns(spec)
    null_count = sum(1 for r in rows for c, _req in spec.numeric_columns if r.values[c] is None)

    lines = []
    lines.append("-- =============================================================")
    lines.append(f"-- {target_table} Layer C 기준정보 INSERT (--table {spec.name})")
    lines.append("-- 생성: csv_to_layer_c_dim_insert.py (Plan 6a T9)")
    lines.append(f"-- 소스 파일: {source_name}")
    lines.append(f"-- 행수: {len(rows)} (NULL 숫자 셀 {null_count})")
    lines.append(f"-- 기본 effective_from: {default_effective_from or '(행별 값만)'}")
    lines.append("-- 통화: KRW 고정 (설계 2026-08-31 §4.2)")
    lines.append("-- 경고: 실값 산출물(이 파일)은 레포·사외 환경 반입 금지 (§7.2, .gitignore 커버)")
    lines.append("-- 실행 주체: admin 수동 — 변경은 새 effective_from 행 append (기존 행 불변)")
    lines.append("-- =============================================================")
    lines.append("")
    for chunk in _chunks(rows, chunk_size):
        lines.append(f"INSERT INTO {target_table}")
        lines.append(f"    ({', '.join(columns)})")
        lines.append("SELECT *")
        lines.append("FROM (")
        lines.append("    " + "\n    UNION ALL\n    ".join(_row_select(spec, r, i == 0) for i, r in enumerate(chunk)))
        lines.append(")")
        lines.append(f"WHERE ({', '.join(spec.key_columns)}) NOT IN (")
        lines.append(f"    SELECT {', '.join(spec.key_columns)} FROM {target_table}")
        lines.append(")")
        lines.append("SETTINGS insert_distributed_sync = 1;")
        lines.append("")
    lines += _verification(spec, target_table)
    return "\n".join(lines) + "\n"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Layer C 기준정보 CSV -> gpu_data.dim_token_{gpu_tco,gpu_allocation,vendor_price}_dist INSERT SQL 생성기"
    )
    parser.add_argument("--table", required=True, choices=TABLE_CHOICES, help="대상 dim (CSV 계약은 모듈 docstring)")
    parser.add_argument("--csv", required=True, help="입력 CSV 경로")
    parser.add_argument(
        "--effective-from",
        default=None,
        help="행의 effective_from이 빈 경우의 기본값 (YYYY-MM-DD, 소급 시작일 — 2026-01-01 금지)",
    )
    parser.add_argument("--out", default=None, help="출력 SQL 경로 (기본: dim_token_<table>_insert.sql — gitignore 대상)")
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
    spec = TABLE_SPECS[args.table]

    if args.chunk_size <= 0:
        parser.error("--chunk-size는 1 이상이어야 함")

    csv_path = Path(args.csv)
    try:
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames or []
            raw_rows = list(reader)
    except OSError:
        parser.error(f"--csv 파일을 열 수 없음: {csv_path.name}")
        return 2  # pragma: no cover — parser.error already exits

    missing = [h for h in required_headers(spec) if h not in header]
    if missing:
        print(f"검증 실패: CSV 헤더에 필수 컬럼 없음: {', '.join(missing)}", file=sys.stderr)
        return 1

    try:
        rows = parse_rows(spec, raw_rows, args.effective_from)
    except LayerCError as exc:
        print(f"검증 실패: {exc}", file=sys.stderr)
        return 1

    sql_text = render_sql(spec, rows, args.chunk_size, csv_path.name, args.effective_from, args.target_db)
    out_path = Path(args.out or spec.default_out)
    out_path.write_text(sql_text, encoding="utf-8")

    null_count = sum(1 for r in rows for c, _req in spec.numeric_columns if r.values[c] is None)
    num_chunks = (len(rows) + args.chunk_size - 1) // args.chunk_size if rows else 0
    print(f"생성 완료: {out_path.name} (--table {spec.name})")   # 경로 미출력 — tmp 경로 문자열이 stdout 위생 검사에 걸리지 않게
    print(f"입력 데이터 행수: {len(raw_rows)} → 출력 행수: {len(rows)} (NULL 숫자 셀 {null_count})")
    print(f"chunk 크기: {args.chunk_size} (chunk 수: {num_chunks})")
    print(
        "검증: 출력 SQL 말미 \"-- 검증: 결과가 비어야 정상\" 섹션 실행 후 결과가 비어 있어야 정상 "
        "(admin 리뷰 절차 — 시드 seed_dim_token_*.sql 적용 이후에 실행)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
