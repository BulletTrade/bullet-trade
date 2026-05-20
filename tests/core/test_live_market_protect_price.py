import asyncio

import pytest

from bullet_trade.broker.base import BrokerBase
from bullet_trade.core import pricing
from bullet_trade.core.async_scheduler import AsyncScheduler
from bullet_trade.core.event_bus import EventBus
from bullet_trade.core.live_engine import LiveEngine
from bullet_trade.core.models import Position
from bullet_trade.core.orders import (
    clear_order_queue,
    order,
    order_target,
    order_target_value,
    order_value,
)
from bullet_trade.core.runtime import set_current_engine


class _RecordingBroker(BrokerBase):
    def __init__(self):
        super().__init__("dummy")
        self.orders = []

    def connect(self) -> bool:
        self._connected = True
        return True

    def disconnect(self) -> bool:
        self._connected = False
        return True

    def get_account_info(self):
        return {"available_cash": 1_000_000, "total_value": 1_000_000, "positions": []}

    def get_positions(self):
        return []

    async def buy(self, security, amount, price=None, wait_timeout=None, remark=None, *, market=False):
        self.orders.append((security, amount, price, "buy", market))
        return f"buy-{len(self.orders)}"

    async def sell(self, security, amount, price=None, wait_timeout=None, remark=None, *, market=False):
        self.orders.append((security, amount, price, "sell", market))
        return f"sell-{len(self.orders)}"

    async def cancel_order(self, order_id: str) -> bool:
        return True

    async def get_order_status(self, order_id: str):
        return {}


def _write_strategy(tmp_path):
    path = tmp_path / "strategy.py"
    path.write_text(
        "def initialize(context):\n"
        "    pass\n",
        encoding="utf-8",
    )
    return path


@pytest.mark.asyncio
async def test_local_live_market_helpers_compute_directional_protect_prices(monkeypatch, tmp_path):
    engine = LiveEngine(
        strategy_file=_write_strategy(tmp_path),
        broker_factory=_RecordingBroker,
        live_config={
            "runtime_dir": str(tmp_path / "runtime"),
            "g_autosave_enabled": False,
            "account_sync_enabled": False,
            "order_sync_enabled": False,
            "tick_sync_enabled": False,
            "risk_check_enabled": False,
            "broker_heartbeat_interval": 0,
        },
    )
    loop = asyncio.get_running_loop()
    engine._loop = loop
    engine._order_lock = asyncio.Lock()
    engine._stop_event = asyncio.Event()
    engine.event_bus = EventBus(loop)
    engine.async_scheduler = AsyncScheduler()
    engine.broker = _RecordingBroker()
    engine.context.portfolio.available_cash = 1_000_000
    engine.context.portfolio.total_value = 1_000_000
    engine.context.portfolio.positions["000001.XSHE"] = Position(
        security="000001.XSHE",
        total_amount=600,
        closeable_amount=600,
        avg_cost=9.5,
        price=10.0,
        value=6000.0,
    )
    engine._risk = None

    class Snap:
        paused = False
        last_price = 10.0
        high_limit = 10.8
        low_limit = 9.2

    monkeypatch.setattr(
        "bullet_trade.core.live_engine.get_current_data",
        lambda: {"000001.XSHE": Snap()},
    )

    clear_order_queue()
    set_current_engine(engine)
    try:
        order("000001.XSHE", 100)
        order("000001.XSHE", -100)
        order_value("000001.XSHE", 1000)
        order_value("000001.XSHE", -1000)
        order_target("000001.XSHE", 800)
        order_target("000001.XSHE", 400)
        order_target_value("000001.XSHE", 8000)
        order_target_value("000001.XSHE", 0)
        await engine._process_orders(engine.context.current_dt)
    finally:
        set_current_engine(None)
        clear_order_queue()

    buy_price = pricing.compute_market_protect_price(
        "000001.XSHE", 10.0, 10.8, 9.2, 0.015, True
    )
    sell_price = pricing.compute_market_protect_price(
        "000001.XSHE", 10.0, 10.8, 9.2, -0.015, False
    )
    assert len(engine.broker.orders) == 8
    for _, _, price, side, market in engine.broker.orders:
        assert market is True
        assert price == pytest.approx(buy_price if side == "buy" else sell_price)
    assert [row[3] for row in engine.broker.orders] == [
        "buy",
        "sell",
        "buy",
        "sell",
        "buy",
        "sell",
        "buy",
        "sell",
    ]
