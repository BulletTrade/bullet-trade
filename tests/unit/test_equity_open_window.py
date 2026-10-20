"""作者：BruceLee。

职责：日频回测开盘时刻（09:30，尚无已完成分钟线）的市价单撮合依据。
输入：内存行情、真实订单 API；输出：pytest 断言。不连网。
约定：价格取当日首根 09:31 分钟开盘价；首分钟零量（盘中临停）市价单取消，不按日线越过停牌。
"""

from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from bullet_trade.core.models import OrderStatus
from bullet_trade.core.orders import LimitOrderStyle, order
from tests.unit.test_equity_pending_matching import CODE, market  # noqa: F401

DAY = datetime(2026, 9, 16)


def _install(monkeypatch, engine, *, first_open, first_volume, daily_volume=10_000_000):
    """09:30 没有已完成分钟线；内部 provider 提供首根 09:31 分钟与当日日量。"""
    calls = []

    def visible(**kwargs):
        assert kwargs["end_date"] <= engine.context.current_dt
        return pd.DataFrame()

    def provider(**kwargs):
        calls.append(kwargs)
        if kwargs["frequency"] == "minute":
            first = DAY.replace(hour=9, minute=31)
            assert kwargs["start_date"] == first and kwargs["end_date"] == first
            return pd.DataFrame({"open": [first_open], "high": [first_open], "low": [first_open],
                                 "volume": [first_volume]}, index=[pd.Timestamp(first)])
        return pd.DataFrame({"volume": [daily_volume]}, index=[pd.Timestamp(kwargs["end_date"]).normalize()])

    monkeypatch.setattr("bullet_trade.core.engine.api_get_price", visible)
    monkeypatch.setattr("bullet_trade.core.engine.get_data_provider", lambda: SimpleNamespace(get_price=provider))
    return calls


def _open(engine):
    engine.frequency = "day"
    engine.context.current_dt = DAY.replace(hour=9, minute=30)
    engine.context.portfolio.available_cash = 1_000_000


def test_daily_open_market_order_fills_at_first_minute_open(market, monkeypatch):
    engine, quote, _ = market
    _open(engine)
    quote.last_price = 2.25
    _install(monkeypatch, engine, first_open=2.2, first_volume=500_000)
    ticket = order(CODE, 1000)
    engine._process_orders(engine.context.current_dt)
    assert ticket.filled == 1000
    assert engine.trades[0].price == pytest.approx(2.2)
    assert engine.trades[0].time == engine.context.current_dt


def test_daily_open_halted_first_minute_cancels_market_and_keeps_limit(market, monkeypatch):
    engine, quote, _ = market
    _open(engine)
    _install(monkeypatch, engine, first_open=2.304, first_volume=0)
    ticket = order(CODE, 1000)
    pending = order(CODE, 1000, style=LimitOrderStyle(2.30))
    engine._process_orders(engine.context.current_dt)
    assert not engine.trades
    assert ticket.status == OrderStatus.canceled
    assert pending.status == OrderStatus.open


def test_minute_frequency_open_time_keeps_original_rule(market, monkeypatch):
    engine, _, _ = market
    _open(engine)
    engine.frequency = "minute"
    calls = _install(monkeypatch, engine, first_open=2.2, first_volume=500_000)
    ticket = order(CODE, 1000)
    engine._process_orders(engine.context.current_dt)
    assert not engine.trades and ticket.status == OrderStatus.canceled
    assert not any(call.get("frequency") == "minute" for call in calls)
