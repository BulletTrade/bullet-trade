#!/usr/bin/env python3
"""DuckDB 数据质量检查脚本（自包含，无项目内部依赖）。

对指定 DuckDB 表执行标准质检并输出结构化报告。

依赖：pip install duckdb pandas loguru

用法::

    python check_quality.py \\
        --duckdb-path ./ashare.duckdb \\
        --table stk_daily \\
        --pk ts_code,trade_date \\
        --date-col trade_date

    # 输出 JSON 格式
    python check_quality.py \\
        --duckdb-path ./ashare.duckdb \\
        --table stk_info \\
        --pk ts_code \\
        --format json
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
from loguru import logger


def _fq(table: str) -> str:
    parts = table.split(".")
    if len(parts) == 2:
        return f'"{parts[0]}"."{parts[1]}"'
    return f'"{table}"'


def _parse_schema_table(table: str):
    """Return (schema, table_name) for information_schema queries."""
    parts = table.split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, parts[0]


def _col_info_where(schema, table_name):
    """Return (WHERE clause, params) for information_schema.columns."""
    if schema:
        return "table_schema = ? AND table_name = ?", [schema, table_name]
    return "table_schema = current_schema() AND table_name = ?", [table_name]


def check_quality(
    duckdb_path: str,
    table: str,
    pk_cols: List[str],
    date_col: Optional[str] = None,
) -> Dict:
    """Run quality checks on a DuckDB table. Returns structured report dict."""
    fq = _fq(table)
    report = {
        "table": table,
        "check_time": datetime.now().isoformat(),
        "checks": {},
        "passed": True,
    }

    with duckdb.connect(duckdb_path, read_only=True) as con:
        # 1. Row count
        row_count = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
        report["checks"]["row_count"] = {"value": row_count, "pass": row_count > 0}

        # 2. PK uniqueness
        pk_expr = ", ".join(f'"{c}"' for c in pk_cols)
        dup_rows = con.execute(
            f"SELECT {pk_expr}, COUNT(*) AS cnt FROM {fq} GROUP BY {pk_expr} HAVING cnt > 1 LIMIT 5"
        ).fetchall()
        dup_count = len(dup_rows)
        report["checks"]["pk_unique"] = {
            "value": dup_count,
            "pass": dup_count == 0,
            "sample": [str(r) for r in dup_rows] if dup_rows else [],
        }

        # 3. PK null check (per column)
        pk_nulls = {}
        for col in pk_cols:
            null_count = con.execute(f'SELECT COUNT(*) FROM {fq} WHERE "{col}" IS NULL').fetchone()[0]
            pk_nulls[col] = null_count
        all_pk_clean = all(v == 0 for v in pk_nulls.values())
        report["checks"]["pk_no_null"] = {"value": pk_nulls, "pass": all_pk_clean}

        # 4. Date range (if date_col provided)
        if date_col:
            date_range = con.execute(
                f'SELECT MIN("{date_col}")::VARCHAR, MAX("{date_col}")::VARCHAR FROM {fq}'
            ).fetchone()
            report["checks"]["date_range"] = {
                "min": date_range[0],
                "max": date_range[1],
                "pass": True,  # Informational - agent validates against latest trade date
            }

        # 5. Distinct ts_code count (if column exists)
        schema, tbl = _parse_schema_table(table)
        where, params = _col_info_where(schema, tbl)
        cols_info = con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE {where} ORDER BY ordinal_position",
            params,
        ).fetchall()
        col_names = [r[0] for r in cols_info]

        if "ts_code" in col_names:
            ts_count = con.execute(f'SELECT COUNT(DISTINCT "ts_code") FROM {fq}').fetchone()[0]
            report["checks"]["distinct_ts_code"] = {"value": ts_count}

        if date_col and date_col in col_names:
            date_count = con.execute(f'SELECT COUNT(DISTINCT "{date_col}") FROM {fq}').fetchone()[0]
            report["checks"]["distinct_dates"] = {"value": date_count}

        # 6. NaN string pollution check
        nan_cols = []
        for col in col_names:
            col_type = con.execute(
                f"SELECT data_type FROM information_schema.columns "
                f"WHERE {where} AND column_name = ?",
                params + [col],
            ).fetchone()
            if col_type and col_type[0] == "VARCHAR":
                cnt = con.execute(
                    f"""SELECT COUNT(*) FROM {fq} WHERE "{col}" IN ('nan', 'NaN', 'NAN', 'None')"""
                ).fetchone()[0]
                if cnt > 0:
                    nan_cols.append({"column": col, "count": cnt})
        report["checks"]["nan_pollution"] = {
            "value": nan_cols,
            "pass": len(nan_cols) == 0,
        }

        # 7. Measure column null ratio (DOUBLE/FLOAT columns with > 50% null)
        high_null_cols = []
        if row_count > 0:
            for col in col_names:
                col_type = con.execute(
                    f"SELECT data_type FROM information_schema.columns "
                    f"WHERE {where} AND column_name = ?",
                    params + [col],
                ).fetchone()
                if col_type and col_type[0] in ("DOUBLE", "FLOAT", "DECIMAL", "BIGINT", "INTEGER"):
                    null_cnt = con.execute(
                        f'SELECT COUNT(*) FROM {fq} WHERE "{col}" IS NULL'
                    ).fetchone()[0]
                    ratio = null_cnt / row_count
                    if ratio > 0.5:
                        high_null_cols.append({"column": col, "null_ratio": round(ratio, 4)})
        report["checks"]["high_null_measures"] = {
            "value": high_null_cols,
            "pass": len(high_null_cols) == 0,
        }

    # Overall pass
    for check in report["checks"].values():
        if "pass" in check and not check["pass"]:
            report["passed"] = False
            break

    return report


def format_markdown(report: Dict) -> str:
    """Format quality report as markdown table for embedding in metadata docs."""
    checks = report["checks"]
    t = report["check_time"]
    lines = [
        "| 指标 | 值 | 检查时间 |",
        "|---|---|---|",
        f"| 总行数 | {checks['row_count']['value']} | {t} |",
    ]
    if "date_range" in checks:
        lines.append(f"| 数据起始 | {checks['date_range']['min']} | {t} |")
        lines.append(f"| 数据截止 | {checks['date_range']['max']} | {t} |")
    if "distinct_ts_code" in checks:
        lines.append(f"| 股票数 (DISTINCT ts_code) | {checks['distinct_ts_code']['value']} | {t} |")
    if "distinct_dates" in checks:
        lines.append(f"| 交易日/报告期数 | {checks['distinct_dates']['value']} | {t} |")
    lines.append(f"| PK 重复数 | {checks['pk_unique']['value']} | {t} |")
    pk_null_str = ", ".join(f"{k}={v}" for k, v in checks["pk_no_null"]["value"].items())
    lines.append(f"| PK 列 NULL 数 | {pk_null_str} | {t} |")
    nan_val = checks["nan_pollution"]["value"]
    nan_str = "无" if not nan_val else ", ".join(f"{x['column']}({x['count']})" for x in nan_val)
    lines.append(f"| NaN 字符串污染列 | {nan_str} | {t} |")
    high_null = checks["high_null_measures"]["value"]
    hn_str = "无" if not high_null else ", ".join(f"{x['column']}({x['null_ratio']:.1%})" for x in high_null)
    lines.append(f"| 度量列全 NULL 率 > 50% | {hn_str} | {t} |")
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description="DuckDB table quality check")
    p.add_argument("--duckdb-path", required=True, help="Path to DuckDB file")
    p.add_argument("--table", required=True, help="Table name to check")
    p.add_argument("--pk", required=True, help="Comma-separated PK column names")
    p.add_argument("--date-col", default=None, help="Date column name for range check")
    p.add_argument("--format", default="text", choices=["text", "json", "markdown"])
    args = p.parse_args()

    pk_cols = [c.strip() for c in args.pk.split(",")]
    report = check_quality(args.duckdb_path, args.table, pk_cols, args.date_col)

    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    elif args.format == "markdown":
        print(format_markdown(report))
    else:
        status = "✅ PASSED" if report["passed"] else "❌ FAILED"
        print(f"\nQuality Report: {report['table']} — {status}")
        print(f"Check Time: {report['check_time']}")
        for name, check in report["checks"].items():
            flag = "✓" if check.get("pass", True) else "✗"
            print(f"  {flag} {name}: {check['value']}")


if __name__ == "__main__":
    main()
