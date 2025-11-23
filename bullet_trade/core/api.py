"""
BulletTrade 核心 API 模块

提供聚宽兼容的API接口，用于策略编写
此模块会被 jqdata 兼容层导入，确保策略中的 from jqdata import * 可以正常工作
"""

# 导入全局对象
from .globals import g, log
from .notifications import send_msg, set_message_handler
from .runtime import get_current_engine

# 导入设置函数
from .settings import (
    set_benchmark, set_order_cost, set_slippage, set_option,
    OrderCost, FixedSlippage
)

# 导入订单函数
from .orders import (
    order, order_value, order_target, order_target_value, MarketOrderStyle, LimitOrderStyle
)

# 导入调度函数
from .scheduler import (
    run_daily, run_weekly, run_monthly, unschedule_all
)

# 导入数据模型
from .models import (
    Context, Portfolio, SubPortfolio, Position,
    Trade, Order, OrderStatus, OrderStyle, SecurityUnitData
)

# 导入数据API
from ..data.api import (
    get_price, attribute_history,
    get_current_data,
    get_trade_days, get_all_securities,
    get_index_stocks,
    get_split_dividend,
    set_data_provider, get_data_provider
)

# ---- Tick 订阅占位（后续与 QMT 对接完善） ----
from typing import Union, Sequence, Any, Optional, Set, Callable, Dict
import threading
# 订阅注册表（占位）：记录策略声明过的订阅标的
_tick_subscribed: Set[str] = set()
_tick_lock = threading.Lock()
_tick_handler: Optional[Callable[[Any, Dict[str, Any]], None]] = None
_xt_markets: Set[str] = set()  # 'SH'/'SZ'
_remote_tick_ctx = type("RemoteContext", (), {"live_trade": True})()


def _remote_provider():
    try:
        provider = get_data_provider()
    except Exception:
        return None
    if provider and getattr(provider, "name", "").lower() == "qmt-remote":
        return provider
    return None


def _current_live_engine():
    """
    如果当前处于 LiveEngine 环境，则返回引擎实例。
    """
    try:
        engine = get_current_engine()
    except Exception:
        return None
    if engine and getattr(engine, 'is_live', False):
        return engine
    return None


def subscribe(security: Union[str, Sequence[str]], frequency: str) -> None:
    """
    订阅标的的 tick 事件。

    - LiveEngine 运行时：直接委托引擎记录订阅并由券商/xtdata 推送 handle_tick；
    - 非 live 环境：回退到本地 xtdata 订阅，占位提供最小能力。
    """
    if frequency != 'tick':
        raise ValueError("subscribe 仅支持 frequency='tick'")
    symbols = [security] if isinstance(security, str) else list(security)
    markets: Set[str] = set()
    norm_syms: Set[str] = set()
    for s in symbols:
        s = str(s).strip()
        if s in ("SH", "SZ"):
            markets.add(s)
        else:
            norm_syms.add(s)

    # 类型与数量限制
    _validate_subscriptions(norm_syms)

    engine = _current_live_engine()
    if engine:
        engine.register_tick_subscription(norm_syms, markets)
        return

    provider = _remote_provider()
    if provider:
        _auto_bind_handle_tick()
        provider.set_tick_callback(_forward_remote_tick, _remote_tick_ctx)
        provider.subscribe_ticks(list(norm_syms))
        return

    with _tick_lock:
        _tick_subscribed.update(norm_syms)
        _xt_markets.update(markets)

    # 自动绑定策略的 handle_tick（若未显式注册）
    _auto_bind_handle_tick()

    # 尝试接入 xtdata 订阅（若环境可用）；失败不抛错
    try:
        from xtquant import xtdata  # type: ignore
        # 订阅市场全量
        if markets:
            try:
                xtdata.subscribe_whole_quote(list(markets), callback=_on_xt_tick)  # type: ignore
                log.info(f"订阅全市场: {list(markets)}")
            except Exception:
                pass
        # 订阅指定标的（QMT 代码需要映射）
        mapped = [_to_qmt_code(s) for s in norm_syms]
        if mapped:
            try:
                if hasattr(xtdata, 'subscribe_quote'):
                    xtdata.subscribe_quote(mapped, callback=_on_xt_tick)  # type: ignore
                else:
                    xtdata.subscribe_whole_quote(mapped, callback=_on_xt_tick)  # type: ignore
                log.info(f"订阅标的: {mapped}")
            except Exception:
                pass
    except Exception:
        pass
    return None


def unsubscribe(security: Union[str, Sequence[str]], frequency: str) -> None:
    """
    取消订阅标的 tick 事件（占位）。
    """
    if frequency != 'tick':
        raise ValueError("unsubscribe 仅支持 frequency='tick'")
    symbols = [security] if isinstance(security, str) else list(security)
    markets: Set[str] = set()
    norm_syms: Set[str] = set()
    for s in symbols:
        s = str(s).strip()
        if s in ("SH", "SZ"):
            markets.add(s)
        else:
            norm_syms.add(s)

    engine = _current_live_engine()
    if engine:
        engine.unregister_tick_subscription(norm_syms, markets)
        return

    with _tick_lock:
        for s in norm_syms:
            _tick_subscribed.discard(str(s))
        for m in markets:
            _xt_markets.discard(m)

    try:
        from xtquant import xtdata  # type: ignore
        # 市场退订
        if markets:
            try:
                if hasattr(xtdata, 'unsubscribe_whole_quote'):
                    xtdata.unsubscribe_whole_quote(list(markets))  # type: ignore
                    log.info(f"退订全市场: {list(markets)}")
            except Exception:
                pass
        # 标的退订
        mapped = [_to_qmt_code(s) for s in norm_syms]
        if mapped:
            try:
                if hasattr(xtdata, 'unsubscribe_quote'):
                    xtdata.unsubscribe_quote(mapped)  # type: ignore
                    log.info(f"退订标的: {mapped}")
            except Exception:
                pass
    except Exception:
        pass
    return None


def unsubscribe_all() -> None:
    """取消所有 tick 订阅。"""
    engine = _current_live_engine()
    if engine:
        engine.unsubscribe_all_ticks()
        return

    with _tick_lock:
        _tick_subscribed.clear()
        _xt_markets.clear()
    try:
        from xtquant import xtdata  # type: ignore
        if hasattr(xtdata, 'unsubscribe_all'):
            xtdata.unsubscribe_all()  # type: ignore
        else:
            # 退订市场
            if hasattr(xtdata, 'unsubscribe_whole_quote'):
                xtdata.unsubscribe_whole_quote(["SH", "SZ"])  # type: ignore
            # 退订标的（无法列举已订标的则忽略）
        log.info("退订全部 tick 订阅")
    except Exception:
        pass
    return None


def get_current_tick(security: str) -> Optional[dict]:
    """
    获取最新 tick 快照（最小实现）。
    在存在 xtquant/xtdata 环境时，调用 xtdata.get_full_tick([code]) 返回最新快照；
    否则返回 None（不报错）。
    """

    engine = _current_live_engine()

    if engine:

        return engine.get_current_tick_snapshot(security)


    try:
        # 延迟导入，避免无环境时报错
        from xtquant import xtdata  # type: ignore
    except Exception:
        return None

    try:
        mapped = _to_qmt_code(security)
        data = xtdata.get_full_tick([mapped])  # type: ignore
        tick = data.get(mapped) if isinstance(data, dict) else None
        if not tick:
            return None
        # 统一最小字段
        result = {
            'sid': security,
            'last_price': tick.get('lastPrice'),
            'dt': tick.get('time') or tick.get('datetime') or None,
        }
        return result
    except Exception:
        return None


def set_tick_handler(handler: Callable[[Any, Dict[str, Any]], None]) -> None:
    """
    注册 tick 事件回调处理函数。
    handler(context, tick): tick 包含 sid/last_price/dt 等字段；context 暂提供最小对象。
    """
    global _tick_handler
    _tick_handler = handler


def _to_qmt_code(code: str) -> str:
    if code.endswith('.XSHE'):
        return code.replace('.XSHE', '.SZ')
    if code.endswith('.XSHG'):
        return code.replace('.XSHG', '.SH')
    return code


def _to_jq_code(code: str) -> str:
    if code.endswith('.SZ') and len(code) >= 3:
        return code[:-3] + '.XSHE'
    if code.endswith('.SH') and len(code) >= 3:
        return code[:-3] + '.XSHG'
    return code


def _on_xt_tick(data: Any) -> None:
    """
    xtdata 订阅的回调兼容层：将原始 tick 转为聚宽风格并分发给注册的 handler。
    """
    handler = _tick_handler
    if handler is None:
        return
    # xtdata 可能传入 dict（code->tick 或 code->list[tick]）或对象列表
    try:
        items = []
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, (list, tuple)) and v:
                    for entry in v:
                        items.append((k, entry))
                else:
                    items.append((k, v))
        elif isinstance(data, (list, tuple)):
            for v in data:
                code = getattr(v, 'stock_code', None) or getattr(v, 'code', None) or getattr(v, 'InstrumentID', None)
                items.append((code, v))
    except Exception:
        items = []

    def _pick(src: Any, keys) -> Any:
        for key in keys:
            if isinstance(src, dict) and key in src:
                val = src.get(key)
            else:
                val = getattr(src, key, None) if hasattr(src, key) else None
            if val not in (None, ""):
                return val
        return None

    for code, t in items:
        try:
            jq_code = _to_jq_code(str(code)) if code else None
            last_price = _pick(t, ['lastPrice', 'last_price', 'price', 'latestPrice', 'newPrice', 'last'])
            ts = _pick(t, ['time', 'datetime', 'data_time', 'update_time'])
            tick = {
                'sid': jq_code,
                'symbol': code,
                'last_price': last_price,
                'dt': ts,
            }
            # 盘口/行情补充字段（尽量兼容 xtdata）
            mappings = {
                'bid1': ['bidPrice1', 'bidprice1', 'bid1', 'buyPrice1', 'buyprice1'],
                'ask1': ['askPrice1', 'askprice1', 'ask1', 'sellPrice1', 'sellprice1'],
                'bid1_volume': ['bidVolume1', 'bidvolume1', 'bid1_volume', 'buyVolume1', 'buyvolume1'],
                'ask1_volume': ['askVolume1', 'askvolume1', 'ask1_volume', 'sellVolume1', 'sellvolume1'],
                'last_close': ['preClose', 'lastClose', 'pre_close'],
                'open': ['open', 'openPrice', 'open_price'],
                'high': ['high', 'highPrice', 'high_price'],
                'low': ['low', 'lowPrice', 'low_price'],
                'volume': ['volume', 'vol'],
                'amount': ['amount', 'money', 'turnover', 'value'],
                'limit_up': ['limitUp', 'highLimit', 'limit_up'],
                'limit_down': ['limitDown', 'lowLimit', 'limit_down'],
            }
            for field, keys in mappings.items():
                val = _pick(t, keys)
                if val is not None:
                    tick[field] = val
            # 构造最小 context（后续接入真实 live context）
            class _Ctx:
                live_trade = True
            handler(_Ctx(), tick)  # type: ignore
        except Exception:
            continue


def _is_sim_mode() -> bool:
    """粗略判断是否为模拟交易模式（用于订阅数量限制）。
    - 环境 DEFAULT_BROKER=simulator 视为模拟；
    - live(g.live_trade=True) 视为非模拟；
    - 回测（存在 _current_context）不限制。
    """
    try:
        from bullet_trade.utils.env_loader import get_broker_config  # type: ignore
        from bullet_trade.data.api import _current_context  # type: ignore
        if _current_context is not None:
            return False
        # 尝试读取全局 g.live_trade
        try:
            from bullet_trade.core.globals import g  # type: ignore
            if bool(getattr(g, 'live_trade', False)):
                return False
        except Exception:
            pass
        default_broker = (get_broker_config() or {}).get('default', 'simulator')
        return str(default_broker).lower() == 'simulator'
    except Exception:
        return False


def _validate_subscriptions(symbols: Set[str]) -> None:
    """订阅前校验：类型与数量限制。
    - 禁止订阅主力合约/期货指数合约（如 RB9999.XSGE、IF00.CFFEX 等）
    - 模拟交易模式下单策略最多 100 个订阅（回测不限）
    """
    # 主力/连续/指数期货检测（启发式）
    def _is_forbidden(sym: str) -> bool:
        try:
            if '.' not in sym:
                return False
            code, exch = sym.split('.', 1)
            exch = exch.upper()
            # 期货交易所后缀
            is_fut_exch = any(exch.endswith(suf) for suf in ('XSGE', 'CFFEX', 'CCFX', 'DCE', 'CZCE', 'XINE', 'XSHF'))
            if not is_fut_exch:
                return False
            # 主力/连续合约：尾数 8888/9999/88/99
            digits = ''.join(ch for ch in code if ch.isdigit())
            if digits in ('8888', '9999', '88', '99', '0000', '00'):
                return True
            # 期指指数合约：例如 IF00/IC00/IH00（00 或 0000）
            letters = ''.join(ch for ch in code if ch.isalpha()).upper()
            if letters in ('IF', 'IH', 'IC') and digits in ('00', '0000'):
                return True
        except Exception:
            return False
        return False

    forb = [s for s in symbols if _is_forbidden(s)]
    if forb:
        raise ValueError(f"禁止订阅主力/期指指数合约: {', '.join(forb)}")

    # 模拟交易订阅上限
    if _is_sim_mode():
        with _tick_lock:
            total_after = len(_tick_subscribed.union(symbols))
        limit = 100
        if total_after > limit:
            raise ValueError(f"模拟交易单策略最多订阅 {limit} 个标的，当前将达到 {total_after}")


def _auto_bind_handle_tick() -> None:
    """尝试自动绑定策略中的 handle_tick(context, tick)。"""
    global _tick_handler
    if _tick_handler is not None:
        return
    try:
        import sys
        mod = sys.modules.get('__main__')
        func = getattr(mod, 'handle_tick', None)
        if callable(func):
            set_tick_handler(func)  # type: ignore[arg-type]
    except Exception:
        return

# 导出所有API
__all__ = [
    # 全局对象
    'g', 'log', 'send_msg', 'set_message_handler',
    
    # 设置函数
    'set_benchmark', 'set_order_cost', 'set_slippage', 'set_option',
    'OrderCost', 'FixedSlippage',
    
    # 订单函数
    'order', 'order_value', 'order_target', 'order_target_value', 'MarketOrderStyle', 'LimitOrderStyle',
    
    # 调度函数
    'run_daily', 'run_weekly', 'run_monthly', 'unschedule_all',
    
    # 数据模型
    'Context', 'Portfolio', 'SubPortfolio', 'Position',
    'Trade', 'Order', 'OrderStatus', 'OrderStyle', 'SecurityUnitData',
    
    # 数据API
    'get_price', 'attribute_history',
    'get_current_data',
    'get_trade_days', 'get_all_securities',
    'get_index_stocks',
    'get_split_dividend',
    'set_data_provider', 'get_data_provider',
    # Tick 订阅占位
    'subscribe', 'unsubscribe', 'unsubscribe_all', 'get_current_tick',
]
