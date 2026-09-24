"""现金证券成交的公司行动归属与毛净损益证据。"""

import math
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

from .futures_account import is_futures_security


def annotate_equity_trade_pnls(
    trades: Iterable[Any],
    events: Iterable[Dict[str, Any]],
    initial_positions: Optional[Iterable[Any]] = None,
) -> None:
    """按持仓平均成本分配分派和入场费用，不改变资金或成交。

    毛损益包含实际到账分派，净损益再扣入场与离场交易费用。
    每次从原始成交重新计算，不依赖此前注释。初始持仓缺少入场费用证据，
    即使提供平均成本也不生成完整净损益。只有已知数量全部归零后，后续
    重新建仓才恢复完整证据。期货成交不作任何修改。
    """
    def get(item, key, default=None):
        return item.get(key, default) if isinstance(item, dict) else getattr(item, key, default)

    def put(item, key, value):
        if isinstance(item, dict):
            item[key] = value
        else:
            setattr(item, key, value)

    def number(value):
        result = float(value or 0)
        if not math.isfinite(result):
            raise ValueError("现金证券损益证据包含非有限数值")
        return result

    def time_key(value):
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value).replace(" ", "T")

    def empty_state():
        return dict(qty=0.0, basis=0.0, fees=0.0, cash=0.0, known=True, qty_known=True)

    states = {}
    for position in initial_positions or []:
        code = get(position, "security")
        if not code or is_futures_security(code):
            continue
        quantity = number(get(position, "amount", get(position, "total_amount", 0)))
        if quantity < 0:
            raise ValueError("现金证券初始数量不能为负")
        if quantity:
            state = states.setdefault(code, empty_state())
            state["qty"] += quantity
            state["known"] = False

    timeline = []
    for index, event in enumerate(events or []):
        timestamp = event.get("strategy_time") or event.get("event_date")
        timeline.append((time_key(timestamp), 0, index, event))
    for index, trade in enumerate(trades or []):
        timeline.append((time_key(get(trade, "time")), 1, index, trade))
    for _, kind, _, item in sorted(timeline, key=lambda row: row[:3]):
        code = get(item, "code" if kind == 0 else "security")
        if not code or is_futures_security(code):
            continue
        state = states.setdefault(code, empty_state())
        if kind == 0:
            new_amount = get(item, "new_amount")
            if new_amount is not None:
                old_amount = get(item, "old_amount")
                if old_amount is None or abs(state["qty"] - number(old_amount)) > 1e-8:
                    state["known"] = False
                state["qty"] = number(new_amount)
                if state["qty"] < 0:
                    raise ValueError("公司行动后的现金证券数量不能为负")
                state["qty_known"] = True
            cash_in = number(get(item, "cash_in"))
            if cash_in and state["qty"] <= 0:
                state["known"] = False
                state["qty_known"] = False
            state["cash"] += cash_in
            continue
        for field in (
            "realized_pnl_gross", "realized_pnl_net", "allocated_entry_fees",
            "allocated_distributions", "pnl_basis_status",
        ):
            put(item, field, None)
        amount = number(get(item, "amount"))
        if amount == 0:
            put(item, "pnl_basis_status", "no_execution")
            continue
        price = number(get(item, "price"))
        if price <= 0:
            raise ValueError("现金证券成交价必须为正")
        fees = number(get(item, "commission")) + number(get(item, "tax"))
        if amount > 0:
            state["qty"] += amount
            state["basis"] += amount * price
            state["fees"] += fees
            put(item, "pnl_basis_status", "open")
            continue
        quantity = -amount
        if quantity > state["qty"] + 1e-8:
            # 未知初始持仓的部分卖出不能被误认为已经清仓。
            state["qty_known"] = False
            state["known"] = False
        if not state["known"] or state["qty"] <= 0:
            put(item, "pnl_basis_status", "unknown_opening_basis")
            state["known"] = False
            state["qty"] = max(0.0, state["qty"] - quantity)
        else:
            fraction = min(1.0, quantity / state["qty"])
            basis = state["basis"] * fraction
            entry_fees = state["fees"] * fraction
            distributions = state["cash"] * fraction
            gross = quantity * price - basis + distributions
            put(item, "realized_pnl_gross", gross)
            put(item, "realized_pnl_net", gross - entry_fees - fees)
            put(item, "allocated_entry_fees", entry_fees)
            put(item, "allocated_distributions", distributions)
            put(item, "pnl_basis_status", "corporate_actions_and_fees")
            state["qty"] -= quantity
            state["basis"] -= basis
            state["fees"] -= entry_fees
            state["cash"] -= distributions
        if state["qty_known"] and state["qty"] <= 1e-8:
            state.update(empty_state())
