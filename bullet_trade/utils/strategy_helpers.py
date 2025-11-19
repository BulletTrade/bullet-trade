from __future__ import annotations

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, List, Optional

try:
    from tabulate import tabulate  # type: ignore
except Exception:
    tabulate = None  # fallback 为 DataFrame.to_string

from ..data import api as data_api


def _fen(x: float) -> float:
    return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def prettytable_print_df(df, headers: str = "keys", show_index: bool = False, max_rows: int = 50) -> None:
    if df is None:
        print("<empty>")
        return
    try:
        dfx = df.head(max_rows)
        if show_index:
            dfx = dfx.reset_index()
        if headers == "keys":
            header_list = [str(col) for col in dfx.columns]
        elif isinstance(headers, (list, tuple)):
            header_list = [str(h) for h in headers]
        else:
            header_list = [str(headers)]
        rows = [
            ["" if val is None else str(val) for val in row]
            for row in dfx.to_numpy().tolist()
        ]
        print(_format_table(header_list, rows))
    except Exception as exc:
        print(f"prettytable_print_df 失败: {exc}")


def _positions_df(context) -> "pd.DataFrame":  # type: ignore
    """保留旧接口，供外部可能的调用。"""
    import pandas as pd

    rows = []
    try:
        pos_map = getattr(context.portfolio, "positions", {}) or {}
        for code, pos in pos_map.items():
            amount = int(getattr(pos, "total_amount", 0) or 0)
            avg_cost = float(getattr(pos, "avg_cost", 0.0) or 0.0)
            price = float(getattr(pos, "price", 0.0) or 0.0)
            value = float(getattr(pos, "value", amount * price) or 0.0)
            pnl = (price - avg_cost) * amount
            rows.append(
                {
                    "code": code,
                    "amount": amount,
                    "avg_cost": _fen(avg_cost),
                    "price": _fen(price),
                    "value": _fen(value),
                    "pnl": _fen(pnl),
                }
            )
    except Exception:
        pass
    df = (
        pd.DataFrame(rows, columns=["code", "amount", "avg_cost", "price", "value", "pnl"])
        if rows
        else pd.DataFrame(columns=["code", "amount", "avg_cost", "price", "value", "pnl"])
    )
    return df


def _format_percent(value: float) -> str:
    return f"{value * 100:.2f}%"


def _format_table(headers: List[str], rows: List[List[Any]]) -> str:
    if tabulate is not None:
        return tabulate(rows, headers=headers, tablefmt="psql")

    widths = [len(h) for h in headers]
    str_rows: List[List[str]] = []
    for row in rows:
        str_row = [str(cell) for cell in row]
        str_rows.append(str_row)
        for idx, cell in enumerate(str_row):
            widths[idx] = max(widths[idx], len(cell))

    def _border(char: str = "-") -> str:
        return "+" + "+".join(char * (w + 2) for w in widths) + "+"

    def _format_row(row_vals: List[str]) -> str:
        cells = [f" {val:<{widths[i]}} " for i, val in enumerate(row_vals)]
        return "|" + "|".join(cells) + "|"

    lines = [_border("-"), _format_row(headers), _border("=")]
    for row in str_rows:
        lines.append(_format_row(row))
        lines.append(_border("-"))
    return "\n".join(lines)


def _position_rows(context, total_value: float, top_n: Optional[int]) -> List[List[Any]]:
    positions = getattr(context.portfolio, "positions", {}) or {}
    entries: List[List[Any]] = []
    for code, pos in positions.items():
        amount = int(getattr(pos, "total_amount", 0) or 0)
        if amount <= 0:
            continue
        closeable = int(getattr(pos, "closeable_amount", amount) or 0)
        avg_cost = float(getattr(pos, "avg_cost", 0.0) or 0.0)
        price = float(getattr(pos, "price", 0.0) or 0.0)
        value = float(getattr(pos, "value", amount * price) or 0.0)
        pnl_value = value - avg_cost * amount
        pnl_rate = (price / avg_cost - 1.0) if avg_cost > 0 else 0.0
        weight = (value / total_value) if total_value > 0 else 0.0
        try:
            info = data_api.get_security_info(code) or {}
        except Exception:
            info = {}
        display_name = info.get("display_name") or info.get("name") or ""
        buy_time = getattr(pos, "buy_time", None) or getattr(pos, "last_buy_time", None)
        if isinstance(buy_time, datetime):
            buy_str = buy_time.strftime("%Y-%m-%d")
        else:
            buy_str = "--"
        entries.append(
            [
                code,
                display_name,
                buy_str,
                amount,
                closeable,
                f"{avg_cost:.3f}",
                f"{price:.3f}",
                f"{value:,.2f}",
                f"{pnl_value:,.2f}",
                _format_percent(pnl_rate),
                _format_percent(weight),
            ]
        )

    if not entries:
        return []

    entries.sort(key=lambda row: float(row[7].replace(",", "")), reverse=True)
    if top_n is not None and top_n > 0:
        entries = entries[:top_n]

    formatted_rows = []
    for idx, row in enumerate(entries):
        formatted_rows.append([idx, *row])
    return formatted_rows


def _refresh_position_prices(context) -> None:
    portfolio = getattr(context, "portfolio", None)
    if not portfolio or not getattr(portfolio, "positions", None):
        return
    try:
        current_data = data_api.get_current_data()
    except Exception:
        return
    if not current_data:
        return
    for code, pos in portfolio.positions.items():
        try:
            snapshot = current_data[code]
        except Exception:
            continue
        last_price = float(getattr(snapshot, "last_price", 0.0) or 0.0)
        if last_price > 0 and hasattr(pos, "update_price"):
            try:
                pos.update_price(last_price)
            except Exception:
                continue
    try:
        portfolio.update_value()
    except Exception:
        pass


def print_portfolio_info(context, top_n: Optional[int] = None, sort_by: str = "value") -> None:
    """
    打印账户概要信息与主要持仓，样式对齐聚宽 CLI。
    Args:
        context: 回测或实盘上下文
        top_n: 仅显示前 N 个持仓
        sort_by: 兼容旧参数（已忽略，默认按市值排序）
    """
    _refresh_position_prices(context)
    total_value = float(getattr(context.portfolio, "total_value", 0.0) or 0.0)
    cash = float(getattr(context.portfolio, "available_cash", 0.0) or 0.0)
    starting_cash = float(getattr(context.portfolio, "starting_cash", 0.0) or 0.0)
    pnl_pct = ((total_value - starting_cash) / starting_cash * 100.0) if starting_cash > 0 else 0.0

    run_params = getattr(context, "run_params", None) or {}
    run_type = str(run_params.get("run_type") or "").upper()
    is_live = bool(run_params.get("is_live")) or run_type == "LIVE"

    if not is_live:
        print(f"系统 ==>当前收益：{pnl_pct:.2f}%，当前剩余金额{_fen(cash):,.2f}, 总价值:{_fen(total_value):,.2f}")
    print("当前持仓:")

    rows = _position_rows(context, total_value, top_n)
    if not rows:
        print("(无持仓)")
        return

    headers = [
        "idx",
        "股票代码",
        "名称",
        "买入时间",
        "持仓",
        "可用",
        "成本价",
        "当前价",
        "当前市值",
        "盈亏额",
        "盈亏率",
        "持仓比",
    ]
    print(_format_table(headers, rows))


__all__ = [
    "print_portfolio_info",
    "prettytable_print_df",
]
