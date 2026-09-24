"""
实盘价格辅助工具：最小价差、价格笼子、保护价计算等。
"""

from __future__ import annotations

import json
import os
from typing import Optional, Tuple, Any, Dict

from .futures_account import is_futures_security


def _split_security(security: str) -> Tuple[str, str]:
    parts = security.split(".")
    if len(parts) == 2:
        return parts[0], parts[1].upper()
    return security, ""


_LOT_RULES_LOADED = False
_LOT_RULES: Dict[str, Any] = {}


def _config_base_dir() -> str:
    return os.path.dirname(os.path.dirname(__file__))


def _lot_rules_path() -> str:
    return os.path.join(_config_base_dir(), "config", "security_overrides.json")


def _load_lot_rules_if_needed() -> None:
    global _LOT_RULES_LOADED, _LOT_RULES
    if _LOT_RULES_LOADED:
        return
    path = _lot_rules_path()
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    _LOT_RULES = data.get("lot_rules") or {}
    except Exception:
        _LOT_RULES = {}
    finally:
        _LOT_RULES_LOADED = True


def _normalize_market(market: str) -> str:
    upper = (market or "").upper()
    if upper in ("XSHG", "SH"):
        return "SH"
    if upper in ("XSHE", "SZ"):
        return "SZ"
    if upper in ("BJ", "BSE"):
        return "BJ"
    return upper


def _candidate_codes(code: str, market: str, raw: str) -> Tuple[str, ...]:
    candidates = []
    if raw:
        candidates.append(raw)
    if not code or not market:
        return tuple(candidates)
    if market == "SH":
        candidates.extend([f"{code}.XSHG", f"{code}.SH"])
    elif market == "SZ":
        candidates.extend([f"{code}.XSHE", f"{code}.SZ"])
    elif market == "BJ":
        candidates.extend([f"{code}.BJ", f"{code}.BSE"])
    return tuple(dict.fromkeys(candidates))


def _pick_lot_rule(security: str) -> Dict[str, Any]:
    _load_lot_rules_if_needed()
    rules = _LOT_RULES if isinstance(_LOT_RULES, dict) else {}
    if not rules:
        return {"min_lot": 100, "step": 100}
    by_code = rules.get("by_code") or {}
    by_prefix = rules.get("by_prefix") or {}
    by_market = rules.get("by_market") or {}
    default = rules.get("default") or {"min_lot": 100, "step": 100}

    code, market_raw = _split_security(security)
    market = _normalize_market(market_raw)
    if isinstance(by_code, dict):
        for candidate in _candidate_codes(code, market, security):
            item = by_code.get(candidate)
            if isinstance(item, dict):
                return item
    if isinstance(by_prefix, dict):
        for prefix, item in by_prefix.items():
            if code.startswith(prefix) and isinstance(item, dict):
                return item
    if isinstance(by_market, dict):
        item = by_market.get(market)
        if isinstance(item, dict):
            return item
    return default if isinstance(default, dict) else {"min_lot": 100, "step": 100}


def infer_lot_rule(security: str) -> Tuple[int, int]:
    rule = _pick_lot_rule(security)
    min_lot = int(rule.get("min_lot") or 0)
    step = int(rule.get("step") or 0)
    if min_lot <= 0:
        min_lot = 1
    if step <= 0:
        step = min_lot
    return min_lot, step


def adjust_order_amount(
    security: str,
    amount: int,
    is_buy: bool,
    closeable: Optional[int] = None,
) -> int:
    raw = int(amount or 0)
    if raw <= 0:
        return 0
    min_lot, step = infer_lot_rule(security)
    if is_buy:
        adjusted = (raw // step) * step
        return adjusted if adjusted >= min_lot else 0
    if closeable is not None:
        closeable = int(closeable)
        if closeable < min_lot:
            return min(raw, closeable)
    adjusted = (raw // step) * step
    if adjusted < min_lot:
        return 0
    if closeable is not None:
        return min(adjusted, closeable)
    return adjusted


def is_etf(security: str) -> bool:
    code, market = _split_security(security)
    return (market in ("XSHG", "SH") and code.startswith("5")) or (
        market in ("XSHE", "SZ") and code.startswith("1")
    )


def infer_lot_size(security: str) -> int:
    min_lot, _ = infer_lot_rule(security)
    return min_lot


def get_min_price_step(security: str, price: float) -> float:
    """
    根据标的和当前价格推断最小价差（tick size）。
    规则参考交易所公开信息，覆盖主板/创业板/ETF/北交所等常见场景。

    返回 0.0 表示档位未知，调用方应跳过按档位归档而不是退回某个默认档位。
    """
    if is_futures_security(security):
        # 期货档位随品种和上市时间变化，只能由合约规格表按合约解析；
        # 这里给 0.01 会把国债期货 0.005 这类细档位压平成非法报价
        return 0.0

    code, market = _split_security(security)
    price = float(price) if price and price > 0 else 1.0

    if is_etf(security):
        return 0.001

    # B 股
    if (market in ("XSHG", "SH") and code.startswith("9")) or (
        market in ("XSHE", "SZ") and code.startswith("2")
    ):
        return 0.001

    # 其余沪深 A 股
    if price < 1:
        return 0.001
    return 0.01


def _infer_price_rule(security: str) -> str:
    code, market = _split_security(security)
    if market in ("BJ", "BSE"):
        return "beijing"
    if market in ("XSHG", "SH"):
        if code.startswith("68"):
            return "sci"
        return "main"
    if market in ("XSHE", "SZ"):
        # 创业板（30开头）和主板同一规则（102%/98% + 十档）
        return "main"
    return "other"


def compute_price_bounds(
    security: str, base_price: float, tick_size: float
) -> Tuple[Optional[float], Optional[float]]:
    """
    返回 (买入上限, 卖出下限)，用于价格笼子裁剪。
    base_price 来自交易所“基准价”的近似值（通常取 last_price）。
    """
    rule = _infer_price_rule(security)
    tick = tick_size if tick_size > 0 else 0.01
    if rule == "beijing":
        return (
            max(base_price * 1.05, base_price + 0.1),
            min(base_price * 0.95, base_price - 0.1),
        )
    if rule == "sci":
        return base_price * 1.02, base_price * 0.98
    if rule == "main":
        extra = 10 * tick
        return max(base_price * 1.02, base_price + extra), min(
            base_price * 0.98, base_price - extra
        )
    # 其他市场不强制（返回 None）
    return None, None


def _clamp(value: float, lower: Optional[float], upper: Optional[float]) -> float:
    if lower is not None:
        value = max(value, lower)
    if upper is not None:
        value = min(value, upper)
    return value


def _to_positive_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _merge_lower(*values: Optional[float]) -> Optional[float]:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _merge_upper(*values: Optional[float]) -> Optional[float]:
    present = [value for value in values if value is not None]
    return min(present) if present else None


def _round_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return price
    rounded = round(price / tick_size) * tick_size
    # 避免浮点尾差
    return float(f"{rounded:.6f}")


def compute_trade_price_bounds(
    security: str,
    last_price: float,
    high_limit: Optional[float],
    low_limit: Optional[float],
    is_buy: bool,
) -> Tuple[Optional[float], Optional[float]]:
    """
    计算委托价/保护价/模拟成交价共同适用的边界。

    Args:
        security: 标的代码。
        last_price: 价格笼子的参考价，通常为当前最新价或回测当前 bar 价格。
        high_limit: 当日涨停价；缺失或非正数时不参与上界约束。
        low_limit: 当日跌停价；缺失或非正数时不参与下界约束。
        is_buy: True 表示买入，False 表示卖出。

    Returns:
        (lower, upper)：买入时 upper 为 min(涨停价, 买入笼子上沿)，
        卖出时 lower 为 max(跌停价, 卖出笼子下沿)。缺失的边界返回 None。
    """
    current_high = _to_positive_float(high_limit)
    current_low = _to_positive_float(low_limit)

    base_price = _to_positive_float(last_price)
    cage_buy: Optional[float] = None
    cage_sell: Optional[float] = None
    if base_price is not None:
        tick = get_min_price_step(security, base_price)
        cage_buy, cage_sell = compute_price_bounds(security, base_price, tick)

    if is_buy:
        return current_low, _merge_upper(current_high, cage_buy)
    return _merge_lower(current_low, cage_sell), current_high


def clamp_price_to_trade_bounds(
    security: str,
    price: float,
    last_price: float,
    high_limit: Optional[float],
    low_limit: Optional[float],
    is_buy: bool,
) -> float:
    """
    将委托价、保护价或模拟成交价裁剪到涨跌停与价格笼子的共同边界内。

    Args:
        security: 标的代码。
        price: 待裁剪价格。
        last_price: 价格笼子的参考价，通常为当前最新价或回测当前 bar 价格。
        high_limit: 当日涨停价。
        low_limit: 当日跌停价。
        is_buy: True 表示买入，False 表示卖出。

    Returns:
        已按 tick、涨跌停和价格笼子裁剪后的价格。
    """
    base_price = _to_positive_float(last_price) or _to_positive_float(price) or 1.0
    tick = get_min_price_step(security, base_price)
    rounded = _round_to_tick(float(price), tick)
    lower, upper = compute_trade_price_bounds(security, base_price, high_limit, low_limit, is_buy)
    clamped = _clamp(rounded, lower, upper)
    return _round_to_tick(clamped, tick)


def compute_market_protect_price(
    security: str,
    last_price: float,
    high_limit: Optional[float],
    low_limit: Optional[float],
    percent: float,
    is_buy: bool,
) -> float:
    """
    计算市价保护价：last_price*(1+percent) 经价格笼子/涨跌停/最小价差裁剪。
    """
    base_price = float(last_price)
    if base_price <= 0:
        # 尝试使用涨跌停作为基准
        fallback = high_limit if high_limit and high_limit > 0 else low_limit
        if not fallback:
            raise ValueError(f"{security} 缺少可用价格，无法计算保护价")
        base_price = float(fallback)

    protect_price = base_price * (1.0 + percent)
    rounded = clamp_price_to_trade_bounds(
        security,
        protect_price,
        base_price,
        high_limit,
        low_limit,
        is_buy,
    )

    if rounded <= 0:
        raise ValueError(f"{security} 保护价无效: {rounded}")
    return rounded


def resolve_market_percent(
    style: Any, is_buy: bool, default_buy: float, default_sell: float
) -> float:
    """
    计算市价单使用的比例：策略 style > 配置 > 默认。
    style 只要带有 buy_price_percent/sell_price_percent 属性即可（鸭子类型）。
    """
    if style and hasattr(style, "buy_price_percent") and hasattr(style, "sell_price_percent"):
        percent = style.buy_price_percent if is_buy else style.sell_price_percent
        if percent is not None:
            return float(percent)
    return float(default_buy if is_buy else default_sell)
