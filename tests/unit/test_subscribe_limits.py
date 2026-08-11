import pytest


from bullet_trade.core import api as core_api
from bullet_trade.core.api import get_current_tick, subscribe, unsubscribe, unsubscribe_all
from bullet_trade.core.runtime import set_current_engine


class _StubLiveEngine:
    is_live = True

    def __init__(self):
        self.registered = []
        self.unregistered = []
        self.cleared = False

    def register_tick_subscription(self, symbols, markets):
        self.registered.append((tuple(symbols), tuple(markets)))

    def unregister_tick_subscription(self, symbols, markets):
        self.unregistered.append((tuple(symbols), tuple(markets)))

    def unsubscribe_all_ticks(self):
        self.cleared = True


def test_forbid_main_contract(monkeypatch):
    # 主力/指数合约应被禁止
    with pytest.raises(ValueError):
        subscribe("RB9999.XSGE", "tick")


def test_sim_limit_100(monkeypatch):
    # 模拟交易：订阅上限 100
    monkeypatch.setattr(
        "bullet_trade.utils.env_loader.get_broker_config",
        lambda: {"default": "simulator"},
    )
    # 保证是非 live/非回测
    try:
        from bullet_trade.core.globals import g
        g.live_trade = False
    except Exception:
        pass

    symbols = [f"{i:06d}.XSHE" for i in range(1, 102)]
    # 前 100 正常
    subscribe(symbols[:100], "tick")
    # 第 101 个应报错
    with pytest.raises(ValueError):
        subscribe(symbols[100:], "tick")


def test_frequency_reject():
    with pytest.raises(ValueError):
        subscribe("000001.XSHE", "1m")


def test_subscribe_routes_to_live_engine(monkeypatch):
    engine = _StubLiveEngine()
    set_current_engine(engine)
    try:
        subscribe(["000001.XSHE", "SH"], "tick")
        assert engine.registered == [(("000001.XSHE",), ("SH",))]
    finally:
        set_current_engine(None)


def test_unsubscribe_routes_to_live_engine(monkeypatch):
    engine = _StubLiveEngine()
    set_current_engine(engine)
    try:
        unsubscribe(["000001.XSHE", "SZ"], "tick")
        assert engine.unregistered == [(("000001.XSHE",), ("SZ",))]
    finally:
        set_current_engine(None)


def test_unsubscribe_all_routes_to_live_engine(monkeypatch):
    engine = _StubLiveEngine()
    set_current_engine(engine)
    try:
        unsubscribe_all()
        assert engine.cleared is True
    finally:
        set_current_engine(None)


def test_get_current_tick_routes_to_live_engine():
    engine = _StubLiveEngine()

    def _snapshot(symbol):
        return {"sid": symbol, "last_price": 1.23}

    engine.get_current_tick_snapshot = _snapshot  # type: ignore[attr-defined]
    set_current_engine(engine)
    try:
        snap = get_current_tick("000001.XSHE")
        assert snap["last_price"] == 1.23
    finally:
        set_current_engine(None)


def test_forward_remote_tick_preserves_context_and_payload(monkeypatch):
    """验证 qmt-remote tick 转发不改写上下文和载荷。"""

    received = []
    context = object()
    tick = {"sid": "000001.XSHE", "last_price": 12.34}
    monkeypatch.setattr(
        core_api,
        "_tick_handler",
        lambda ctx, data: received.append((ctx, data)),
    )

    core_api._forward_remote_tick(context, tick)

    assert received == [(context, tick)]


def test_forward_remote_tick_without_handler_is_noop(monkeypatch):
    """验证未注册策略 handler 时远程 tick 可安全丢弃。"""

    monkeypatch.setattr(core_api, "_tick_handler", None)

    result = core_api._forward_remote_tick(object(), {"sid": "000001.XSHE"})

    assert result is None
