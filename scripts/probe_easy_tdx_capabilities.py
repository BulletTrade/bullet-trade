#!/usr/bin/env python
"""
探测 easy_tdx 免费数据能力边界

文件职责：在本机用 EasyTdxProvider 直连通达信在线行情服务器，输出可复现的能力摘要。
主要输入：样例证券、频率、请求条数、可选 host/port。
主要输出：JSON 格式探测报告，可重定向保存后补充到发布文档。
上下游关系：依赖 bullet_trade.data.providers.easy_tdx；不参与默认单元测试和回测流程。
关键约定：真实模式不使用 stub，连接失败会写入 error 字段而不是伪造行情。
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
from datetime import datetime
from importlib import metadata
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from bullet_trade.data.providers.easy_tdx import EasyTdxProvider


def _package_version(name: str) -> Optional[str]:
    """读取已安装包版本；缺失时返回 None。"""
    try:
        return metadata.version(name)
    except Exception:
        return None


def _frame_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """把行情 DataFrame 摘要为 JSON 友好结构。"""
    if df is None or df.empty:
        return {"rows": 0, "columns": []}
    if isinstance(df.columns, pd.MultiIndex):
        columns = ["|".join(map(str, item)) for item in df.columns.tolist()]
    else:
        columns = [str(item) for item in df.columns.tolist()]
    index = (
        df.index
        if isinstance(df.index, pd.DatetimeIndex)
        else pd.to_datetime(df.index, errors="coerce")
    )
    return {
        "rows": int(len(df)),
        "columns": columns,
        "first_time": str(index.min()) if len(index) else None,
        "last_time": str(index.max()) if len(index) else None,
        "sample": json.loads(df.tail(1).to_json(orient="records", force_ascii=False)),
    }


def _probe_price(
    provider: EasyTdxProvider,
    *,
    security: str,
    frequency: str,
    count: int,
    start_date: Optional[str],
    end_date: Optional[str],
) -> Dict[str, Any]:
    """探测单个证券单个频率的 K 线能力。"""
    try:
        df = provider.get_price(
            security,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            fields=["open", "high", "low", "close", "volume", "money"],
            count=count,
            fq="none",
            panel=True,
        )
        summary = _frame_summary(df)
        summary["ok"] = bool(summary["rows"])
        return summary
    except Exception as exc:
        return {"ok": False, "error": repr(exc)}


def _probe_quote(provider: EasyTdxProvider, security: str) -> Dict[str, Any]:
    """探测实时快照能力。"""
    try:
        tick = provider.get_current_tick(security)
        if not tick:
            return {"ok": False, "error": "empty_tick"}
        keys = sorted(str(key) for key in tick.keys())
        return {
            "ok": True,
            "keys": keys,
            "last_price": tick.get("last_price"),
            "high_limit": tick.get("limit_up"),
            "low_limit": tick.get("limit_down"),
        }
    except Exception as exc:
        return {"ok": False, "error": repr(exc)}


def _probe_catalog(provider: EasyTdxProvider) -> Dict[str, Any]:
    """探测证券列表接口能力。"""
    result: Dict[str, Any] = {}
    for item in ("stock", "fund", "index"):
        try:
            df = provider.get_all_securities(types=item)
            result[item] = {
                "ok": not df.empty,
                "rows": int(len(df)),
                "sample": df.head(3).index.tolist() if not df.empty else [],
            }
        except Exception as exc:
            result[item] = {"ok": False, "error": repr(exc)}
    return result


def run_probe(args: argparse.Namespace) -> Dict[str, Any]:
    """执行完整 easy_tdx 能力探测并返回报告字典。"""
    provider = EasyTdxProvider(
        {
            "host": args.host,
            "port": args.port,
            "timeout": args.timeout,
            "use_stub": False,
        }
    )
    report: Dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "easy_tdx_version": _package_version("easy-tdx"),
        "bullet_trade_provider": provider.name,
        "host": args.host or "auto",
        "port": args.port,
        "timeout": args.timeout,
        "symbols": args.symbols,
        "frequencies": args.frequencies,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "prices": {},
        "quotes": {},
        "catalog": {},
        "auth": {"ok": False},
    }
    try:
        provider.auth()
        report["auth"] = {"ok": True}
    except Exception as exc:
        report["auth"] = {"ok": False, "error": repr(exc)}
        return report

    for symbol in args.symbols:
        report["prices"][symbol] = {}
        for frequency in args.frequencies:
            report["prices"][symbol][frequency] = _probe_price(
                provider,
                security=symbol,
                frequency=frequency,
                count=args.count,
                start_date=args.start_date,
                end_date=args.end_date,
            )
        report["quotes"][symbol] = _probe_quote(provider, symbol)
    report["catalog"] = _probe_catalog(provider)
    return report


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="探测 easy_tdx 免费行情能力边界")
    parser.add_argument(
        "--symbols",
        default="000001.XSHE,600519.XSHG,510050.XSHG,000001.XSHG",
        help="逗号分隔的聚宽风格证券代码",
    )
    parser.add_argument(
        "--frequencies",
        default="daily,1m,5m,15m,30m,60m",
        help="逗号分隔的 K 线频率",
    )
    parser.add_argument("--count", type=int, default=50000, help="每个频率请求的 bar 数")
    parser.add_argument("--start-date", default=None, help="可选 K 线起始时间，例如 2015-12-01 09:30:00")
    parser.add_argument("--end-date", default=None, help="可选 K 线结束时间，例如 2015-12-31 15:00:00")
    parser.add_argument("--host", default=None, help="可选通达信行情服务器地址")
    parser.add_argument("--port", type=int, default=7709, help="通达信行情服务器端口")
    parser.add_argument("--timeout", type=float, default=10.0, help="连接超时秒数")
    parser.add_argument("--json-output", default=None, help="可选 JSON 报告输出路径")
    args = parser.parse_args(argv)
    args.symbols = [item.strip() for item in str(args.symbols).split(",") if item.strip()]
    args.frequencies = [item.strip() for item in str(args.frequencies).split(",") if item.strip()]
    return args


def main(argv: Optional[List[str]] = None) -> int:
    """脚本入口，输出 JSON 报告。"""
    args = parse_args(argv)
    report = run_probe(args)
    text = json.dumps(report, ensure_ascii=False, indent=2)
    print(text)
    if args.json_output:
        path = Path(args.json_output).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    return 0 if report.get("auth", {}).get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
