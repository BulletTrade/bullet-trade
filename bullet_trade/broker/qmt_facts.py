"""作者：BruceLee。

职责：直接读取xtquant四类原始结果并构造严格事实，不调用旧的宽松快照链。
输入：已连接QmtBroker及其SDK句柄；输出：版本化完整事实或脱密异常。
上下游：QmtBroker.get_account_facts及Server只读转发；仅复用证券/状态/方向映射。
约定：SDK异常、None和坏行拒绝整包；没有可卖、成本、价格、费用时保留None，不补零。
"""

from .facts import account_facts_from_reads

__all__ = ["read_qmt_facts"]


def _pick(row, *names):
    """输入SDK行与字段别名，返回首个明确值或None；保留真实零，不猜缺省值。"""
    for name in names:
        try:
            value = row.get(name) if isinstance(row, dict) else getattr(row, name, None)
        except Exception:
            raise ValueError("QMT严格事实字段读取失败") from None
        if value is not None and value != "":
            return value
    return None


def _required(row, *names):
    """输入SDK行与必需字段，返回实际值；缺失或布尔伪数值抛ValueError，不过滤坏行。"""
    value = _pick(row, *names)
    if value is None or isinstance(value, bool):
        raise ValueError("QMT严格事实缺少必要字段")
    return value


def _read(sdk, account, names, *, collection=True):
    """输入SDK、账户和兼容方法名，返回一次原始读取；None/异常/缺能力明确失败，不换源兜底。"""
    method = next(
        (getattr(sdk, name) for name in names if callable(getattr(sdk, name, None))), None
    )
    if method is None:
        raise NotImplementedError("QMT未提供所需的严格只读查询")
    try:
        value = method(account)
    except Exception:
        raise ValueError("QMT严格事实查询失败") from None
    if value is None:
        raise ValueError("QMT严格事实查询未返回结果")
    if not collection:
        return value
    if isinstance(value, dict) and value:
        return [value]
    if not isinstance(value, (list, tuple)):
        raise ValueError("QMT严格明细查询结果无效")
    return list(value)


def _status(broker, raw):
    """输入状态与原映射器，返回已知状态或unknown；未知值不使用旧映射的open兜底。"""
    known = {
        "new",
        "open",
        "submitted",
        "filling",
        "filled",
        "cancelled",
        "canceled",
        "rejected",
        "partly_canceled",
        "partial_filled",
        "part_succ",
        "canceling",
        "unreported",
        "wait_report",
        "wait_reporting",
        "alltraded",
        "all_traded",
        "completed",
        "withdraw",
        "failed",
        "invalid",
        "partcanceled",
        "partial_canceled",
        "canceling_in_progress",
        "已成",
        "已撤",
        "拒绝",
        "部成部撤",
        "部分成交",
        "撤单中",
    }
    if raw is None or str(raw).strip().lower() == "未知":
        return "unknown"
    recognized = str(raw).strip().lower() in known
    if isinstance(raw, int):
        try:
            from xtquant import xtconstant

            names = (
                "ORDER_UNREPORTED",
                "ORDER_WAIT_REPORT",
                "ORDER_WAIT_REPORTING",
                "ORDER_REPORTED",
                "ORDER_QUEUEING",
                "ORDER_PART_SUCC",
                "ORDER_PARTTRADED",
                "ORDER_PARTTRADED_QUEUEING",
                "ORDER_ALLTRADED",
                "ORDER_SUCCEEDED",
                "ORDER_PART_CANCEL",
                "ORDER_PARTSUCC_CANCEL",
                "ORDER_CANCELED",
                "ORDER_PARTTRADED_NOT_QUEUEING",
                "ORDER_WITHDRAW",
                "ORDER_WITHDRAWN",
                "ORDER_REPORTED_CANCEL",
                "ORDER_REJECTED",
                "ORDER_FAILED",
                "ORDER_INVALID",
                "ORDER_JUNK",
            )
            recognized = any(getattr(xtconstant, name, None) == raw for name in names)
        except ImportError:
            recognized = False
    if not recognized:
        return "unknown"
    mapped = broker._map_order_status(raw)
    return getattr(mapped, "value", mapped)


def read_qmt_facts(broker):
    """输入已连接QMT Broker，返回严格四类事实；只查询SDK，不订阅、下单、撤单或修改旧缓存。"""
    broker._ensure_connected()
    sdk, account = broker._xt_trader, broker._xt_account
    if sdk is None or account is None:
        raise ValueError("QMT查询句柄尚未就绪")
    asset = _read(sdk, account, ("query_stock_asset",), collection=False)
    raw_positions = _read(sdk, account, ("query_stock_positions",))
    raw_orders = _read(sdk, account, ("query_stock_orders", "get_stock_orders", "query_orders"))
    raw_trades = _read(sdk, account, ("query_stock_trades", "get_stock_trades", "query_trades"))
    assets = {
        "available_cash": _required(
            asset,
            "cash",
            "available",
            "available_cash",
            "enable_balance",
            "avail_fund",
            "fund_avail",
            "enable_money",
            "can_use_money",
            "cash_balance",
        ),
        "total_value": _required(
            asset, "total_asset", "total_assets", "asset_total", "static_asset"
        ),
        "cash": _pick(asset, "cash_balance"),
        "frozen_cash": _pick(asset, "frozen_cash", "frozen_money", "frozen_cash_balance"),
    }
    positions, orders, trades = [], [], []
    for row in raw_positions:
        positions.append(
            {
                "security": broker._map_to_jq_symbol(_required(row, "stock_code", "code")),
                "name": _pick(row, "stock_name", "name"),
                "amount": _required(row, "volume", "current_amount"),
                "closeable_amount": _pick(
                    row,
                    "available_volume",
                    "enable_amount",
                    "can_use_volume",
                    "can_sell_volume",
                    "sellable_volume",
                    "m_nCanUseVolume",
                ),
                "avg_cost": _pick(row, "cost_price", "avg_price", "average_price", "open_price"),
                "current_price": _pick(row, "last_price", "price", "current_price", "last"),
                "market_value": _pick(row, "market_value"),
            }
        )
    for row in raw_orders:
        order_type = _pick(row, "order_type", "orderType", "type")
        raw_state = _pick(row, "order_status", "status")
        price_type = _pick(row, "price_type", "priceType")
        orders.append(
            {
                "order_id": str(_required(row, "order_id", "entrust_id")),
                "security": broker._map_to_jq_symbol(
                    _required(row, "stock_code", "code", "security")
                ),
                "amount": _required(row, "order_volume", "volume", "amount"),
                "filled": _pick(
                    row, "traded_volume", "deal_volume", "filled_volume", "volume_traded", "filled"
                ),
                "order_price": None
                if broker._is_market_price_type(price_type)
                else _pick(row, "order_price", "price"),
                "status": _status(broker, raw_state),
                "raw_status": raw_state,
                "is_buy": broker._map_order_side(order_type),
                "order_type": order_type,
                "order_time": _pick(row, "order_time", "time"),
            }
        )
    for row in raw_trades:
        trades.append(
            {
                "trade_id": str(_required(row, "trade_id", "deal_no", "trade_no")),
                "order_id": str(_pick(row, "order_id", "entrust_id") or ""),
                "security": broker._map_to_jq_symbol(
                    _required(row, "stock_code", "code", "security")
                ),
                "amount": _required(row, "trade_volume", "traded_volume", "volume", "amount"),
                "price": _required(
                    row, "traded_price", "trade_price", "avg_price", "avg_cost", "price"
                ),
                "time": _pick(row, "trade_time", "traded_time", "time"),
                "commission": _pick(row, "commission_fee", "commission", "comm"),
                "tax": _pick(row, "tax", "stamp_tax"),
            }
        )
    return account_facts_from_reads(assets, positions, orders, trades)
