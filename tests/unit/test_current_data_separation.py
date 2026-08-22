from datetime import datetime
from types import SimpleNamespace


from bullet_trade.core.settings import reset_settings
from bullet_trade.core.globals import g
from bullet_trade.data.api import get_current_data, set_current_context


def test_current_data_backtest_default():
    reset_settings()
    cd = get_current_data()
    # 默认无 xtdata 环境，非显式设置，应走 BacktestCurrentData
    assert type(cd).__name__ in ("BacktestCurrentData", "EmptyCurrentData")


def test_current_data_live_selected_without_xtdata(monkeypatch):
    reset_settings()
    g.live_trade = True

    class _StubProvider:
        requires_live_data = False

        def get_live_current(self, security):
            return {}

    ctx = SimpleNamespace(current_dt=datetime(2025, 1, 2, 9, 30))
    set_current_context(ctx)
    monkeypatch.setattr("bullet_trade.data.api._provider", _StubProvider(), raising=False)
    cd = get_current_data()
    # 实盘模式下返回 LiveCurrentData；访问时如 provider 无 live 快照则回退
    assert type(cd).__name__ == "LiveCurrentData"
    # 访问一个标的不应报错
    _ = cd["000001.XSHE"]
    set_current_context(None)
    g.live_trade = False


def test_live_current_data_prefers_provider_tick(monkeypatch):
    reset_settings()
    g.live_trade = True

    class DummyProvider:
        requires_live_data = False

        def get_live_current(self, security):
            return {
                "last_price": 12.34,
                "high_limit": 13.0,
                "low_limit": 11.0,
                "paused": False,
            }

    ctx = SimpleNamespace(current_dt=datetime(2025, 1, 2, 10, 0))
    set_current_context(ctx)
    monkeypatch.setattr("bullet_trade.data.api._provider", DummyProvider(), raising=False)
    cd = get_current_data()
    snap = cd["000001.XSHE"]
    assert snap.last_price == 12.34
    assert snap.high_limit == 13.0
    assert snap.low_limit == 11.0
    set_current_context(None)
    g.live_trade = False


def test_live_current_data_preserves_source_audit_fields(monkeypatch):
    """验证策略 get_current_data 可读取实时行情源时间和年龄证明。"""
    reset_settings()
    g.live_trade = True
    source_time = datetime(2025, 1, 2, 10, 0)
    received_time = datetime(2025, 1, 2, 10, 0, 1)

    class DummyProvider:
        """返回带行情审计字段的 provider 替身。"""

        requires_live_data = True

        def get_live_current(self, security):
            """返回固定实时快照。"""
            return {
                "last_price": 12.34,
                "high_limit": 13.0,
                "low_limit": 11.0,
                "paused": False,
                "source_time": source_time,
                "received_time": received_time,
                "age_seconds": 1.0,
                "source": "windows_miniqmt_xtdata",
                "bid_price1": 12.33,
                "ask_price1": 12.35,
                "bid_volume1": 1200,
                "ask_volume1": 900,
            }

    ctx = SimpleNamespace(current_dt=received_time)
    set_current_context(ctx)
    monkeypatch.setattr("bullet_trade.data.api._provider", DummyProvider(), raising=False)
    snap = get_current_data()["000001.XSHE"]
    assert snap.source_time == source_time
    assert snap.received_time == received_time
    assert snap.age_seconds == 1.0
    assert snap.source == "windows_miniqmt_xtdata"
    assert snap.bid_price1 == 12.33
    assert snap.ask_price1 == 12.35
    assert snap.bid_volume1 == 1200
    assert snap.ask_volume1 == 900
    set_current_context(None)
    g.live_trade = False
