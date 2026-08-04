#!/usr/bin/env python3
"""Tushare → DuckDB 单表同步脚本（自包含，无项目内部依赖）。

支持三种维度：none（全量覆盖）、trade_date（按交易日增量）、period（按报告期增量）。
同步状态记录在 DuckDB 内部的 table_sync_state 表中，支持断点续传。

对 trade_date 维度，脚本默认采用 Asia/Shanghai 18:00 的安全截止规则：
- 未显式传入 --end-date 时，18:00 前默认只同步到上一个开放交易日
- 增量维度返回空 payload 时，默认记失败而不是成功，避免误标记
- 只有显式传入 --allow-empty-result 时，才允许 0 行结果记成功

依赖：pip install tushare duckdb pandas loguru

用法示例::

    # 全量覆盖（stock_basic 等无维度表）
    TUSHARE_TOKEN=xxx python sync_table.py \\
        --endpoint stock_basic \\
        --duckdb-path ./ashare.duckdb \\
        --target-table stk_info \\
        --mode overwrite \\
        --dimension-type none

    # 增量同步（daily 等交易日维度表）
    TUSHARE_TOKEN=xxx python sync_table.py \\
        --endpoint daily \\
        --duckdb-path ./ashare.duckdb \\
        --target-table stk_daily \\
        --mode append \\
        --dimension-type trade_date \\
        --start-date 20240101 \\
        --sync-all

    # 按任务文件批量同步
    TUSHARE_TOKEN=xxx python sync_table.py \\
        --tasks-file tasks.json \\
        --duckdb-path ./ashare.duckdb
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd
from loguru import logger

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9 fallback
    ZoneInfo = None

# ---------------------------------------------------------------------------
# Tushare client (cached singleton)
# ---------------------------------------------------------------------------

_tushare_client = None


def _get_tushare_client():
    global _tushare_client
    if _tushare_client is None:
        import tushare as ts

        token = os.environ.get("TUSHARE_TOKEN")
        if not token:
            raise RuntimeError(
                "TUSHARE_TOKEN environment variable is required. "
                "Get your token at https://tushare.pro"
            )
        _tushare_client = ts.pro_api(token=token)
    return _tushare_client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_PUBLISH_CUTOFF_HOUR = 18


class SyncError(Exception):
    """Structured sync error with context dict."""

    def __init__(self, message: str, context: Optional[Dict[str, object]] = None):
        super().__init__(message)
        self.context = context or {}


def _log_event(event: str, payload: Dict[str, object]) -> None:
    logger.info(json.dumps({"event": event, **payload}, ensure_ascii=False, default=str))


def _parse_table_name(table_name: str) -> Tuple[Optional[str], str]:
    parts = table_name.split(".")
    if len(parts) == 1:
        schema, table = None, parts[0]
    elif len(parts) == 2:
        schema, table = parts[0], parts[1]
    else:
        raise SyncError("Invalid table name. Use 'table' or 'schema.table'", {"table": table_name})
    for piece in (schema, table):
        if piece is not None and not VALID_IDENTIFIER.match(piece):
            raise SyncError("Table identifier contains invalid characters", {"table": table_name})
    return schema, table


def _fq(schema: Optional[str], table: str) -> str:
    return f'"{schema}"."{table}"' if schema else f'"{table}"'


def _normalize_date(text: str) -> str:
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y%m%d")
        except ValueError:
            continue
    raise SyncError("Invalid date format. Use YYYYMMDD or YYYY-MM-DD", {"date": text})


def _now_shanghai() -> datetime:
    if ZoneInfo is None:
        return datetime.now()
    return datetime.now(ZoneInfo("Asia/Shanghai"))


def _parse_params(params: Any) -> Dict[str, object]:
    if params is None or params == "":
        return {}
    if isinstance(params, dict):
        return dict(params)
    if isinstance(params, str):
        parsed = json.loads(params)
        if not isinstance(parsed, dict):
            raise SyncError("--params must be a JSON object", {"params": params})
        return parsed
    raise SyncError("--params must be a JSON object", {"params_type": type(params).__name__})


def _allow_empty_result(args) -> bool:
    return bool(getattr(args, "allow_empty_result", False))


# ---------------------------------------------------------------------------
# Sync state management
# ---------------------------------------------------------------------------

def _ensure_sync_state_table(con: duckdb.DuckDBPyConnection) -> str:
    """Ensure table_sync_state exists and return its qualified name."""
    # Check common candidates
    for candidate in ("table_sync_state", "meta.table_sync_status"):
        s, t = _parse_table_name(candidate)
        check_sql = (
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?"
            if s
            else "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = ?"
        )
        params = [s, t] if s else [t]
        row = con.execute(check_sql, params).fetchone()
        if row and int(row[0]) > 0:
            return _fq(s, t)

    # Create it
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS "table_sync_state" (
            source_table VARCHAR,
            dimension_type VARCHAR,
            dimension_value VARCHAR,
            is_sync INTEGER,
            error_message VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    return '"table_sync_state"'


def _write_sync_status(
    con: duckdb.DuckDBPyConnection,
    fq_state: str,
    source_table: str,
    dim_type: str,
    dim_value: str,
    is_sync: int,
    error_msg: str = "",
) -> None:
    con.execute(
        f"DELETE FROM {fq_state} WHERE source_table = ? AND dimension_type = ? AND dimension_value = ?",
        [source_table, dim_type, dim_value],
    )
    con.execute(
        f"INSERT INTO {fq_state} (source_table, dimension_type, dimension_value, is_sync, error_message, updated_at) "
        f"VALUES (?, ?, ?, ?, ?, ?)",
        [source_table, dim_type, dim_value, is_sync, error_msg, datetime.now()],
    )


def _list_synced(con: duckdb.DuckDBPyConnection, fq_state: str, source_table: str, dim_type: str) -> set:
    rows = con.execute(
        f"SELECT dimension_value FROM {fq_state} WHERE source_table = ? AND dimension_type = ? AND is_sync = 1",
        [source_table, dim_type],
    ).fetchall()
    return {str(r[0]) for r in rows}


# ---------------------------------------------------------------------------
# Dimension resolution
# ---------------------------------------------------------------------------

def _get_trade_dates(client, start: str, end: str) -> List[str]:
    cal = client.query("trade_cal", start_date=start, end_date=end)
    if cal is None or cal.empty:
        return []
    return sorted(cal[cal["is_open"] == 1]["cal_date"].astype(str).tolist())


def _get_report_periods(start: str, end: str) -> List[str]:
    s = pd.to_datetime(start, format="%Y%m%d")
    e = pd.to_datetime(end, format="%Y%m%d")
    return sorted(pd.date_range(start=s, end=e, freq="QE-DEC").strftime("%Y%m%d").tolist())


def _resolve_trade_date_end(client, args) -> str:
    if args.end_date:
        return _normalize_date(args.end_date)

    now_sh = _now_shanghai()
    today = now_sh.strftime("%Y%m%d")
    if bool(getattr(args, "disable_safe_trade_date", False)):
        return today

    cutoff_hour = int(getattr(args, "publish_cutoff_hour", DEFAULT_PUBLISH_CUTOFF_HOUR))
    if now_sh.hour >= cutoff_hour:
        return today

    lookback_start = (now_sh - timedelta(days=14)).strftime("%Y%m%d")
    open_days = _get_trade_dates(client, lookback_start, today)
    prior_open_days = [day for day in open_days if day < today]
    if prior_open_days:
        safe_end = prior_open_days[-1]
        _log_event(
            "safe_trade_date_applied",
            {
                "today": today,
                "effective_end_date": safe_end,
                "publish_cutoff_hour": cutoff_hour,
                "timezone": "Asia/Shanghai",
            },
        )
        return safe_end
    return today


def _resolve_dimensions(con, fq_state, client, source_table, args) -> List[str]:
    if args.dimension_type == "none":
        return [""]
    start = _normalize_date(args.start_date or ("20100101" if args.dimension_type == "trade_date" else "20100331"))
    if args.dimension_type == "trade_date":
        end = _resolve_trade_date_end(client, args)
        if start > end:
            raise SyncError(
                "No safe trade_date window is available yet. Retry after the publish cutoff or pass an explicit --end-date.",
                {"start_date": start, "effective_end_date": end},
            )
        values = _get_trade_dates(client, start, end)
    elif args.dimension_type == "period":
        end = _normalize_date(args.end_date or datetime.now().strftime("%Y%m%d"))
        values = _get_report_periods(start, end)
    else:
        raise SyncError("Unsupported dimension_type", {"dimension_type": args.dimension_type})
    if not values:
        return []
    if args.sync_all:
        synced = _list_synced(con, fq_state, source_table, args.dimension_type)
        return [v for v in values if v not in synced]
    return values


# ---------------------------------------------------------------------------
# Tushare data fetch
# ---------------------------------------------------------------------------

def _fetch(client, args, dim_value: str) -> pd.DataFrame:
    kwargs: Dict[str, object] = _parse_params(args.params)
    if args.dimension_type != "none":
        field = args.dimension_field or args.dimension_type
        kwargs[field] = dim_value

    last_exc = None
    for attempt in range(1, args.max_retries + 1):
        try:
            method_name = args.method or "query"
            if method_name == "query":
                df = client.query(args.endpoint, **kwargs)
            else:
                if not hasattr(client, method_name):
                    raise SyncError("Unsupported Tushare method", {"method": method_name})
                df = getattr(client, method_name)(**kwargs)
            if not isinstance(df, pd.DataFrame):
                raise SyncError("Tushare did not return a DataFrame")
            return df.copy()
        except Exception as exc:
            last_exc = exc
            if attempt < args.max_retries:
                wait = args.base_sleep * attempt
                logger.warning(f"Fetch attempt {attempt}/{args.max_retries} failed: {exc}, retry in {wait:.1f}s")
                time.sleep(wait)
    raise SyncError(f"Fetch failed after {args.max_retries} retries: {last_exc}")


def _ensure_expected_rows(df: pd.DataFrame, source: str, target: str, dim_value: str, args) -> None:
    if args.dimension_type == "none" or _allow_empty_result(args) or not df.empty:
        return

    raise SyncError(
        (
            "Empty payload returned from Tushare for incremental sync; "
            "not marking as successful. Retry after the publish cutoff or use --allow-empty-result if zero rows are expected."
        ),
        {
            "source_table": source,
            "target_table": target,
            "dimension_type": args.dimension_type,
            "dimension_value": dim_value,
        },
    )


# ---------------------------------------------------------------------------
# DuckDB write
# ---------------------------------------------------------------------------

def _table_exists(con, schema, table) -> bool:
    if schema:
        row = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()
    else:
        row = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?",
            [table],
        ).fetchone()
    return bool(row and int(row[0]) > 0)


def _get_columns(con, schema, table) -> List[str]:
    if schema:
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema, table],
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = ? ORDER BY ordinal_position",
            [table],
        ).fetchall()
    return [str(r[0]) for r in rows]


def _get_date_cols(con, schema, table) -> set:
    if schema:
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? AND data_type = 'DATE'",
            [schema, table],
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = ? AND data_type = 'DATE'",
            [table],
        ).fetchall()
    return {str(r[0]) for r in rows}


def _coerce_dates(df: pd.DataFrame, date_cols: set) -> pd.DataFrame:
    """Convert YYYYMMDD string columns to datetime64 for DuckDB DATE columns."""
    for col in date_cols:
        if col not in df.columns:
            continue
        s = df[col]
        if s.dropna().empty:
            continue
        converted = pd.to_datetime(s, format="%Y%m%d", errors="coerce")
        if converted.notna().sum() == 0:
            converted = pd.to_datetime(s, errors="coerce")
        df[col] = converted
    return df


def _write(con, df: pd.DataFrame, target: str, mode: str, first_batch: bool) -> Tuple[int, int]:
    """Write DataFrame to DuckDB. Returns (total_rows, loaded_rows)."""
    if df.empty:
        s, t = _parse_table_name(target)
        fq = _fq(s, t)
        if not _table_exists(con, s, t):
            return 0, 0
        row = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()
        return (int(row[0]) if row else 0), 0

    # Normalize NaN → None
    normalized = df.where(pd.notna(df), None)
    loaded = len(normalized)
    s, t = _parse_table_name(target)
    fq = _fq(s, t)
    if s:
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{s}"')
    if mode == "overwrite" and first_batch:
        con.execute(f"DROP TABLE IF EXISTS {fq}")

    if not _table_exists(con, s, t):
        con.register("_df", normalized)
        con.execute(f"CREATE TABLE {fq} AS SELECT * FROM _df")
        con.unregister("_df")
    else:
        cols = _get_columns(con, s, t)
        extra = [c for c in normalized.columns if c not in cols]
        if extra:
            logger.warning(f"Dropping extra columns not in target: {extra}")
        aligned = normalized.reindex(columns=cols)
        dcols = _get_date_cols(con, s, t)
        if dcols:
            aligned = _coerce_dates(aligned.copy(), dcols)
        con.register("_df", aligned)
        con.execute(f"INSERT INTO {fq} SELECT * FROM _df")
        con.unregister("_df")

    row = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()
    total = int(row[0]) if row else loaded
    return total, loaded


# ---------------------------------------------------------------------------
# Main sync logic
# ---------------------------------------------------------------------------

def sync_one_table(args) -> Dict[str, object]:
    """Sync a single Tushare endpoint to DuckDB. Returns result summary dict."""
    source = args.source_table or args.endpoint
    target = args.target_table or source
    db_path = Path(args.duckdb_path).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    client = _get_tushare_client()
    with duckdb.connect(str(db_path)) as con:
        fq_state = _ensure_sync_state_table(con)
        dims = _resolve_dimensions(con, fq_state, client, source, args)

        if not dims:
            result = {
                "source_table": source, "target_table": target,
                "dimension_type": args.dimension_type,
                "processed": 0, "loaded_rows": 0, "total_rows": 0,
            }
            _log_event("sync_skipped", result)
            return result

        _log_event("sync_started", {
            "source_table": source, "target_table": target,
            "dimension_type": args.dimension_type,
            "dimensions": len(dims), "mode": args.mode,
        })

        processed = 0
        total_loaded = 0
        total_rows = 0
        first_batch = True

        for dim in dims:
            try:
                df = _fetch(client, args, dim)
                _ensure_expected_rows(df, source, target, dim, args)
                total_rows, loaded = _write(con, df, target, args.mode, first_batch)
                first_batch = False
                processed += 1
                total_loaded += loaded

                if args.dimension_type != "none":
                    _write_sync_status(con, fq_state, source, args.dimension_type, dim, 1)

                _log_event("dimension_done", {
                    "source_table": source, "dimension": dim,
                    "loaded": loaded, "total": total_rows,
                })

                if args.sleep > 0:
                    time.sleep(args.sleep)

            except Exception as exc:
                if args.dimension_type != "none":
                    _write_sync_status(con, fq_state, source, args.dimension_type, dim, 0, str(exc))
                logger.error(f"Failed: source={source} dim={dim}: {exc}")
                if args.sync_all:
                    continue
                raise

    result = {
        "source_table": source, "target_table": target,
        "dimension_type": args.dimension_type, "mode": args.mode,
        "processed": processed, "loaded_rows": total_loaded, "total_rows": total_rows,
    }
    _log_event("sync_completed", result)
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Tushare → DuckDB single-table sync",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--endpoint", help="Tushare endpoint name (e.g. stock_basic, daily)")
    p.add_argument("--method", default="query", help="Tushare client method (default: query)")
    p.add_argument("--source-table", default=None, help="Logical name for sync state tracking")
    p.add_argument("--duckdb-path", required=True, help="Path to DuckDB file")
    p.add_argument("--target-table", default=None, help="DuckDB target table name")
    p.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    p.add_argument("--dimension-type", default="none", choices=["none", "trade_date", "period"])
    p.add_argument("--dimension-field", default=None, help="Override dimension param name in API call")
    p.add_argument("--start-date", default=None, help="Start date YYYYMMDD")
    p.add_argument("--end-date", default=None, help="End date YYYYMMDD")
    p.add_argument("--sync-all", action="store_true", help="Skip synced dims, continue on error")
    p.add_argument("--params", default=None, help="Extra Tushare kwargs as JSON string")
    p.add_argument("--max-retries", type=int, default=3, help="Max fetch retries (default: 3)")
    p.add_argument("--base-sleep", type=float, default=2.0, help="Retry backoff base seconds")
    p.add_argument("--sleep", type=float, default=0.3, help="Sleep between successful calls")
    p.add_argument(
        "--allow-empty-result",
        action="store_true",
        help="Treat empty payload as success. Use only when zero rows are semantically valid.",
    )
    p.add_argument(
        "--disable-safe-trade-date",
        action="store_true",
        help="Disable the default Asia/Shanghai publish-cutoff guard for trade_date sync when --end-date is omitted.",
    )
    p.add_argument(
        "--publish-cutoff-hour",
        type=int,
        default=DEFAULT_PUBLISH_CUTOFF_HOUR,
        help="Asia/Shanghai hour after which today's trade_date is treated as safe when --end-date is omitted.",
    )
    p.add_argument("--tasks-file", default=None, help="JSON file with array of task objects (batch mode)")
    return p


def _merge_task(base_args, task: dict):
    """Create a new namespace by overlaying task dict onto base args."""
    import copy
    merged = copy.deepcopy(base_args)
    field_map = {
        "endpoint": "endpoint", "method": "method",
        "source_table": "source_table", "target_table": "target_table",
        "mode": "mode", "dimension_type": "dimension_type",
        "dimension_field": "dimension_field",
        "start_date": "start_date", "end_date": "end_date",
        "sync_all": "sync_all", "params": "params", "extra_params": "params",
        "max_retries": "max_retries", "base_sleep": "base_sleep",
        "sleep": "sleep", "sleep_seconds": "sleep",
        "allow_empty_result": "allow_empty_result",
        "disable_safe_trade_date": "disable_safe_trade_date",
        "publish_cutoff_hour": "publish_cutoff_hour",
    }
    for json_key, attr in field_map.items():
        if json_key in task:
            setattr(merged, attr, task[json_key])
    return merged


def main():
    parser = _build_parser()
    args = parser.parse_args()

    if args.tasks_file:
        tasks_path = Path(args.tasks_file)
        if not tasks_path.exists():
            raise FileNotFoundError(f"Tasks file not found: {tasks_path}")
        with open(tasks_path) as f:
            tasks = json.load(f)
        ok, fail = 0, 0
        for i, task in enumerate(tasks):
            task_args = _merge_task(args, task)
            name = task.get("endpoint", f"task_{i}")
            try:
                result = sync_one_table(task_args)
                logger.info(f"✅ {name}: loaded {result['loaded_rows']} rows, total {result['total_rows']}")
                ok += 1
            except Exception as exc:
                logger.error(f"❌ {name}: {exc}")
                fail += 1
        logger.info(f"Batch done: {ok} succeeded, {fail} failed out of {len(tasks)} tasks")
    else:
        if not args.endpoint:
            parser.error("--endpoint is required (unless using --tasks-file)")
        sync_one_table(args)


if __name__ == "__main__":
    main()
