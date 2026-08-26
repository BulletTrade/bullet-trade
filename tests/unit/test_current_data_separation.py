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
    _ = cd['000001.XSHE']
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
    assert snap.source_time is None
    assert snap.received_time is None
    assert snap.age_seconds is None
    assert snap.source is None
    set_current_context(None)
    g.live_trade = False


def test_live_current_data_preserves_freshness_metadata(monkeypatch):
    """验证实盘当前行情容器无损保留 provider 的时效审计字段。

    Args:
        monkeypatch: pytest 提供的属性替换工具。

    Returns:
        None: 断言源时间、接收时间、年龄和来源标识均原样进入 SecurityUnitData。

    Side Effects:
        临时切换全局实盘标志和当前上下文；测试结束后恢复为空上下文和非实盘。
    """

    reset_settings()
    g.live_trade = True
    source_time = datetime(2025, 1, 2, 10, 0, 1)
    received_time = datetime(2025, 1, 2, 10, 0, 2)

    class MetadataProvider:
        """返回带完整时效审计字段的实盘行情替身。"""

        requires_live_data = True

        def get_live_current(self, security):
            """返回固定实盘快照。

            Args:
                security: 待读取的证券代码。

            Returns:
                dict: 带价格和完整时效审计字段的快照。
            """

            return {
                "last_price": 12.34,
                "high_limit": 13.0,
                "low_limit": 11.0,
                "paused": False,
                "source_time": source_time,
                "received_time": received_time,
                "age_seconds": 1.0,
                "source": "test_live_feed",
            }

    ctx = SimpleNamespace(current_dt=datetime(2025, 1, 2, 10, 0, 2))
    set_current_context(ctx)
    monkeypatch.setattr(
        "bullet_trade.data.api._provider", MetadataProvider(), raising=False
    )
    try:
        snap = get_current_data()["000001.XSHE"]
        assert snap.source_time == source_time
        assert snap.received_time == received_time
        assert snap.age_seconds == 1.0
        assert snap.source == "test_live_feed"
    finally:
        set_current_context(None)
        g.live_trade = False
