"""
作者: BruceLee
文件职责: 验证回测数据会话的生命周期、内存预算、MiniQMT 下载去重和实盘隔离。
主要输入: fake provider、fake xtdata、合成 K 线和显式 session 配置。
主要输出: 单元测试断言，确保回测优化默认关闭且不会污染实盘路径。
上下游关系: 覆盖 bullet_trade.data.backtest_session、BacktestEngine 和 MiniQMTProvider 的协作边界。
关键约定: 不连接真实 QMT，不写真实行情缓存，不包含私有策略或内部环境信息。
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Dict, Tuple

import pandas as pd
import pytest

from bullet_trade.core.engine import BacktestEngine
from bullet_trade.core.exceptions import FutureDataError
from bullet_trade.core.globals import g, reset_globals
from bullet_trade.core.models import SecurityUnitData
from bullet_trade.core.orders import clear_order_queue
from bullet_trade.core.settings import reset_settings, set_option
from bullet_trade.data import api as data_api
from bullet_trade.data.backtest_session import (
    BacktestDataSession,
    BacktestDataSessionConfig,
    create_backtest_data_session,
    get_current_backtest_data_session,
    reset_current_backtest_data_session,
    set_current_backtest_data_session,
)
from bullet_trade.data.providers import miniqmt
from bullet_trade.data.providers.miniqmt import MiniQMTProvider

SECURITY_QMT = "000001.SZ"
SECURITY_QMT_B = "000002.SZ"
SECURITY_JQ = "000001.XSHE"
SECURITY_JQ_B = "000002.XSHE"


class FakeXtData:
    """
    MiniQMTProvider 测试用的 xtdata 替身。

    Attributes:
        frames: 按证券、周期和复权类型存储的合成 K 线。
        download_calls: 已触发的下载调用记录。
    """

    def __init__(self, frames: Dict[Tuple[str, str, str], pd.DataFrame]) -> None:
        """
        初始化 fake xtdata。

        Args:
            frames: 合成 K 线字典，键为 (security, period, dividend_type)。
        """
        self.frames = frames
        self.download_calls = []
        self.local_calls = []

    def download_history_data(self, stock_code: str, period: str, **kwargs) -> None:
        """
        记录历史数据下载调用。

        Args:
            stock_code: QMT 格式证券代码。
            period: QMT 周期。
            **kwargs: 兼容 xtquant 新版本的 start_time/end_time 参数。
        """
        self.download_calls.append((stock_code, period, kwargs))

    def get_local_data(self, stock_list, count, period, start_time, end_time, dividend_type):
        """
        返回指定证券、周期和复权类型的合成本地数据。

        Args:
            stock_list: QMT 证券列表。
            count: 请求条数。
            period: QMT 周期。
            start_time: 请求起始时间。
            end_time: 请求结束时间。
            dividend_type: QMT 复权类型。

        Returns:
            dict: 与 xtdata.get_local_data 兼容的返回结构。
        """
        _ = count, start_time, end_time
        security = stock_list[0]
        self.local_calls.append((security, period, dividend_type, start_time, end_time, count))
        df = self.frames.get((security, period, dividend_type), pd.DataFrame())
        return {security: df.copy()}

    def get_divid_factors(self, stock_code: str, start_time: str = "", end_time: str = ""):
        """
        返回空分红因子，避免测试进入事件复权路径。

        Args:
            stock_code: QMT 格式证券代码。
            start_time: 起始时间。
            end_time: 结束时间。

        Returns:
            pd.DataFrame: 空分红因子表。
        """
        _ = stock_code, start_time, end_time
        return pd.DataFrame()

    def get_trading_dates(
        self, market: str, start_time: str = "", end_time: str = "", count: int = -1
    ):
        """
        返回空交易日列表，避免停牌填充影响下载去重断言。

        Args:
            market: 市场代码。
            start_time: 起始时间。
            end_time: 结束时间。
            count: 请求条数。

        Returns:
            list: 空列表。
        """
        _ = market, start_time, end_time, count
        return []


class FakePriceProvider:
    """
    data.api 行情块缓存测试用 provider。

    Attributes:
        name: provider 名称。
        calls: get_price 调用参数记录。
        frame: 合成行情数据。
    """

    name = "fake"
    requires_live_data = False
    supports_dynamic_pre_price_basis = True

    def __init__(self, frame: pd.DataFrame) -> None:
        """
        初始化 fake price provider。

        Args:
            frame: 合成行情数据。
        """
        self.frame = frame
        self.calls = []

    def auth(self) -> None:
        """
        模拟 provider 认证。
        """
        return None

    def get_price(self, **kwargs) -> pd.DataFrame:
        """
        返回按 start_date/end_date 过滤后的合成行情。

        Args:
            **kwargs: get_price 参数。

        Returns:
            pd.DataFrame: 过滤后的行情数据。
        """
        self.calls.append(kwargs)
        df = self.frame.copy()
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        if start_date is not None:
            df = df[df.index >= pd.Timestamp(start_date)]
        if end_date is not None:
            end_ts = pd.Timestamp(end_date)
            if end_ts.time() == dt.datetime.min.time():
                end_ts = end_ts.normalize()
            df = df[df.index <= end_ts]
        fields = kwargs.get("fields")
        if fields:
            df = df[list(fields)]
        return df


class SyntheticBacktestProvider(FakePriceProvider):
    """
    合成回测等价性测试用 provider。

    Attributes:
        name: provider 名称，参与回测数据会话缓存 key。
        frame: 合成行情数据。
    """

    name = "synthetic"

    def auth(self) -> None:
        """
        模拟 provider 认证。
        """
        return None

    def get_trade_days(self, start_date=None, end_date=None, count=None):
        """
        返回合成行情索引中的交易日。

        Args:
            start_date: 起始日期。
            end_date: 结束日期。
            count: 返回数量。

        Returns:
            list[pd.Timestamp]: 过滤后的交易日列表。
        """
        days = sorted({pd.Timestamp(idx).normalize() for idx in self.frame.index})
        if start_date is not None:
            start_ts = pd.Timestamp(start_date).normalize()
            days = [day for day in days if day >= start_ts]
        if end_date is not None:
            end_ts = pd.Timestamp(end_date).normalize()
            days = [day for day in days if day <= end_ts]
        if count is not None:
            return days[-int(count) :]
        return days

    def get_security_info(self, security: str, date=None):
        """
        返回最小证券信息。

        Args:
            security: 证券代码。
            date: 查询日期。

        Returns:
            dict: 标的类型信息。
        """
        _ = date
        return {
            "code": security,
            "display_name": "合成证券",
            "type": "stock",
            "start_date": "2000-01-01",
            "end_date": "2099-12-31",
        }

    def get_all_securities(self, types=None, date=None):
        """
        返回包含合成标的的证券列表。

        Args:
            types: 证券类型过滤。
            date: 查询日期。

        Returns:
            pd.DataFrame: 证券元数据。
        """
        _ = types, date
        return pd.DataFrame(
            {
                "display_name": ["合成证券"],
                "type": ["stock"],
                "start_date": ["2000-01-01"],
                "end_date": ["2099-12-31"],
            },
            index=[SECURITY_JQ],
        )

    def get_index_stocks(self, index_symbol: str, date=None):
        """
        返回合成指数成分。

        Args:
            index_symbol: 指数代码。
            date: 查询日期。

        Returns:
            list[str]: 成分证券列表。
        """
        _ = index_symbol, date
        return [SECURITY_JQ]

    def get_split_dividend(self, security: str, start_date=None, end_date=None):
        """
        返回空权益事件。

        Args:
            security: 证券代码。
            start_date: 起始日期。
            end_date: 结束日期。

        Returns:
            list: 空列表。
        """
        _ = security, start_date, end_date
        return []


def _build_qmt_frame(dates: list[str]) -> pd.DataFrame:
    """
    构造 QMT 原始 K 线 DataFrame。

    Args:
        dates: 日期或分钟时间字符串列表。

    Returns:
        pd.DataFrame: 包含 time/open/high/low/close/volume/amount 的合成数据。
    """
    timestamps = pd.to_datetime(dates)
    prices = [10.0 + idx for idx in range(len(timestamps))]
    return pd.DataFrame(
        {
            "time": [int(ts.value // 10**6) for ts in timestamps],
            "open": prices,
            "high": [price + 0.1 for price in prices],
            "low": [price - 0.1 for price in prices],
            "close": prices,
            "volume": [1000] * len(timestamps),
            "amount": [price * 1000 for price in prices],
        }
    )


def _build_price_frame(dates: list[str], base: float = 10.0) -> pd.DataFrame:
    """
    构造 data.api 测试用标准行情 DataFrame。

    Args:
        dates: 日期或分钟时间字符串列表。
        base: 首条收盘价。

    Returns:
        pd.DataFrame: 包含 open/close/涨跌停/停牌/成交量字段的行情表。
    """
    index = pd.to_datetime(dates)
    closes = [base + idx for idx in range(len(index))]
    return pd.DataFrame(
        {
            "open": [price - 0.1 for price in closes],
            "close": closes,
            "high": [price + 0.2 for price in closes],
            "low": [price - 0.2 for price in closes],
            "volume": [10000] * len(index),
            "money": [price * 10000 for price in closes],
            "high_limit": [price * 1.1 for price in closes],
            "low_limit": [price * 0.9 for price in closes],
            "paused": [False] * len(index),
        },
        index=index,
    )


def _activate_session(session: BacktestDataSession):
    """
    激活回测数据会话并返回 token。

    Args:
        session: 待激活的回测数据会话。

    Returns:
        contextvars.Token: 用于恢复旧会话的 token。
    """
    return set_current_backtest_data_session(session)


def _make_provider(monkeypatch, fake_xt: FakeXtData, **config) -> MiniQMTProvider:
    """
    构造绑定 fake xtdata 的 MiniQMTProvider。

    Args:
        monkeypatch: pytest monkeypatch fixture。
        fake_xt: fake xtdata 对象。
        **config: provider 配置。

    Returns:
        MiniQMTProvider: 已绑定 fake xtdata 的 provider。
    """
    monkeypatch.setattr(
        miniqmt.MiniQMTProvider,
        "_ensure_xtdata",
        staticmethod(lambda: fake_xt),
    )
    monkeypatch.delenv("DATA_CACHE_DIR", raising=False)
    provider = MiniQMTProvider({"cache_dir": None, **config})
    return provider


@pytest.mark.unit
def test_backtest_data_session_defaults_to_disabled(monkeypatch):
    """
    验证回测数据会话默认关闭。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    monkeypatch.delenv("BT_BACKTEST_DATA_SESSION", raising=False)

    session = create_backtest_data_session(start_date="2025-01-01", end_date="2025-01-05")

    assert session.config.enabled is False
    assert session.active is False


@pytest.mark.unit
def test_backtest_engine_cleans_session_after_exception(monkeypatch):
    """
    验证 BacktestEngine.run 异常退出后也会清理回测数据会话。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """

    def initialize(context):
        """
        在 initialize 中断言会话已存在，然后主动抛错。

        Args:
            context: 回测上下文。
        """
        _ = context
        assert get_current_backtest_data_session() is not None
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "bullet_trade.core.engine.get_data_provider",
        lambda: SimpleNamespace(name="fake"),
    )
    engine = BacktestEngine(
        start_date="2025-01-01",
        end_date="2025-01-05",
        initialize=initialize,
        data_session_config={"enabled": True},
    )

    with pytest.raises(RuntimeError, match="boom"):
        engine.run()

    assert get_current_backtest_data_session() is None


@pytest.mark.unit
def test_backtest_session_enforces_memory_budget():
    """
    验证行情块缓存不会突破 max_cache_bytes 硬上限。
    """
    session = BacktestDataSession(
        BacktestDataSessionConfig(
            enabled=True,
            price_block_cache_enabled=True,
            max_cache_bytes=256,
        )
    )
    df = pd.DataFrame({"close": list(range(200))})

    cached = session.set_price_block(("too_large",), df)

    assert cached is False
    assert session.stats.degradations == 1
    assert session.stats.cache_bytes == 0


@pytest.mark.unit
def test_backtest_session_evicts_lru_blocks_under_memory_pressure():
    """
    验证多个行情块累计超过预算时会按 LRU 驱逐冷块。
    """
    block = pd.DataFrame({"close": list(range(8))})
    block_bytes = int(block.memory_usage(deep=True).sum())
    session = BacktestDataSession(
        BacktestDataSessionConfig(
            enabled=True,
            price_block_cache_enabled=True,
            max_cache_bytes=block_bytes * 2 + 1,
        )
    )

    assert session.set_price_block(("block", "a"), block) is True
    assert session.set_price_block(("block", "b"), block) is True
    assert session.get_price_block(("block", "a")) is not None
    assert session.set_price_block(("block", "c"), block) is True

    assert session.stats.evictions == 1
    assert session.stats.cache_bytes <= session.config.max_cache_bytes
    assert session.get_price_block(("block", "a")) is not None
    assert session.get_price_block(("block", "b")) is None
    assert session.get_price_block(("block", "c")) is not None


@pytest.mark.unit
def test_qmt_minute_window_normalizes_date_end_to_market_close():
    """
    验证分钟线回测结束日按收盘时间做覆盖校验，而不是要求覆盖到午夜。
    """
    session = BacktestDataSession(
        BacktestDataSessionConfig(
            enabled=True,
            start_date=dt.datetime(2025, 1, 1),
            end_date=dt.datetime(2025, 1, 3),
        )
    )

    _start, end, strict_start = session.resolve_qmt_window(
        period="1m",
        start_date="2025-01-01 14:50:00",
        end_date="2025-01-02 14:50:00",
        count=1,
    )

    assert end == dt.datetime(2025, 1, 3, 15, 0)
    assert strict_start is False


@pytest.mark.unit
def test_data_api_price_block_cache_reuses_single_security_count_window(monkeypatch):
    """
    验证 data.api 在启用行情块缓存时会复用单标的 count 历史窗口。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    frame = pd.DataFrame(
        {"close": [1.0, 2.0, 3.0, 4.0, 5.0]},
        index=pd.to_datetime(
            ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"]
        ),
    )
    provider = FakePriceProvider(frame)
    monkeypatch.setattr(data_api, "_provider", provider)
    monkeypatch.setattr(data_api, "_auth_attempted", False)
    context = SimpleNamespace(current_dt=dt.datetime(2025, 1, 3, 15, 0), run_params={})
    data_api.set_current_context(context)
    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-05",
        provider_name="fake",
    )
    token = _activate_session(session)
    try:
        first = data_api.get_price(
            SECURITY_JQ,
            end_date=context.current_dt,
            count=2,
            fields=["close"],
            fq="none",
        )
        context.current_dt = dt.datetime(2025, 1, 4, 15, 0)
        second = data_api.get_price(
            SECURITY_JQ,
            end_date=context.current_dt,
            count=2,
            fields=["close"],
            fq="none",
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)
        data_api.set_current_context(None)

    assert provider.calls == [
        {
            "security": SECURITY_JQ,
            "start_date": pd.Timestamp("2024-12-02 00:00:00").to_pydatetime(),
            "end_date": pd.Timestamp("2025-01-05 00:00:00").to_pydatetime(),
            "frequency": "daily",
            "fields": ["close"],
            "skip_paused": False,
            "fq": "none",
            "count": None,
            "panel": True,
            "fill_paused": True,
            "force_no_engine": False,
        }
    ]
    assert first["close"].tolist() == [2.0, 3.0]
    assert second["close"].tolist() == [3.0, 4.0]
    assert session.stats.cache_writes == 1
    assert session.stats.cache_hits == 1


@pytest.mark.unit
def test_data_api_price_block_cache_prefetches_backtest_warmup(monkeypatch):
    """
    验证 count 行情块会预取回测开始日前的数据，避免首段回测历史窗口变短。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    frame = pd.DataFrame(
        {"close": [90.0, 91.0, 92.0, 93.0, 94.0]},
        index=pd.to_datetime(
            ["2024-12-30", "2024-12-31", "2025-01-01", "2025-01-02", "2025-01-03"]
        ),
    )
    provider = FakePriceProvider(frame)
    monkeypatch.setattr(data_api, "_provider", provider)
    monkeypatch.setattr(data_api, "_auth_attempted", False)
    context = SimpleNamespace(current_dt=dt.datetime(2025, 1, 3, 14, 50), run_params={})
    data_api.set_current_context(context)
    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        start_date="2025-01-03",
        end_date="2025-01-05",
        provider_name="fake",
    )
    token = _activate_session(session)
    try:
        result = data_api.get_price(
            SECURITY_JQ,
            end_date=dt.datetime(2025, 1, 2, 15, 0),
            count=3,
            fields=["close"],
            fq="none",
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)
        data_api.set_current_context(None)

    assert provider.calls[0]["start_date"] < dt.datetime(2025, 1, 3)
    assert result["close"].tolist() == [91.0, 92.0, 93.0]


@pytest.mark.unit
def test_data_api_price_block_cache_extends_minute_end_date_to_close(monkeypatch):
    """
    验证分钟线行情块把日期型回测结束日覆盖到收盘，避免末日分钟价回退到前一日。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    frame = pd.DataFrame(
        {"close": [1.0, 2.0]},
        index=pd.to_datetime(["2025-01-02 15:00:00", "2025-01-03 14:50:00"]),
    )
    provider = FakePriceProvider(frame)
    monkeypatch.setattr(data_api, "_provider", provider)
    monkeypatch.setattr(data_api, "_auth_attempted", False)
    context = SimpleNamespace(current_dt=dt.datetime(2025, 1, 3, 14, 50), run_params={})
    data_api.set_current_context(context)
    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-03",
        provider_name="fake",
    )
    token = _activate_session(session)
    try:
        result = data_api.get_price(
            SECURITY_JQ,
            end_date=context.current_dt,
            count=1,
            frequency="1m",
            fields=["close"],
            fq="none",
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)
        data_api.set_current_context(None)

    assert provider.calls[0]["end_date"] == dt.datetime(2025, 1, 3, 15, 0)
    assert result["close"].tolist() == [2.0]


@pytest.mark.unit
def test_data_api_dynamic_pre_factor_block_reanchors_each_bar(monkeypatch):
    """
    验证通用 data API 缓存 factor 基础块后，会按当前回测日动态重锚前复权价格。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    reset_settings()
    frame = pd.DataFrame(
        {
            "close": [10.0, 20.0, 30.0, 40.0, 50.0],
            "factor": [1.0, 2.0, 3.0, 4.0, 5.0],
        },
        index=pd.to_datetime(
            ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"]
        ),
    )
    provider = FakePriceProvider(frame)
    monkeypatch.setattr(data_api, "_provider", provider)
    monkeypatch.setattr(data_api, "_auth_attempted", False)
    context = SimpleNamespace(current_dt=dt.datetime(2025, 1, 3, 14, 50), run_params={})
    data_api.set_current_context(context)
    set_option("use_real_price", True)
    set_option("avoid_future_data", True)
    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-05",
        provider_name="fake",
    )
    token = _activate_session(session)
    try:
        first = data_api.get_price(
            SECURITY_JQ,
            end_date=dt.datetime(2025, 1, 2, 15, 0),
            count=2,
            frequency="daily",
            fields=["close"],
            fq="pre",
        )
        context.current_dt = dt.datetime(2025, 1, 4, 14, 50)
        session.advance_bar(context.current_dt)
        second = data_api.get_price(
            SECURITY_JQ,
            end_date=dt.datetime(2025, 1, 3, 15, 0),
            count=2,
            frequency="daily",
            fields=["close"],
            fq="pre",
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)
        data_api.set_current_context(None)
        reset_settings()

    assert first["close"].tolist() == pytest.approx([10.0 / 3.0, 40.0 / 3.0])
    assert second["close"].tolist() == pytest.approx([10.0, 22.5])
    assert len(provider.calls) == 2
    assert provider.calls[0]["fields"] == ["close"]
    assert provider.calls[0]["fq"] == "none"
    assert provider.calls[1]["fields"] == ["factor"]
    assert provider.calls[1]["fq"] == "pre"
    assert session.stats.cache_writes == 1
    assert session.stats.cache_hits == 1
    assert session.stats.degradations == 0


@pytest.mark.unit
def test_data_api_dynamic_pre_factor_block_uses_same_day_minute_ref(monkeypatch):
    """
    验证分钟线动态前复权按参考日期包含同日 factor，而不是误取上一交易日。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    reset_settings()
    frame = pd.DataFrame(
        {
            "close": [100.0, 200.0, 300.0],
            "factor": [1.0, 2.0, 2.0],
        },
        index=pd.to_datetime(
            [
                "2025-01-02 14:50:00",
                "2025-01-03 14:49:00",
                "2025-01-03 14:50:00",
            ]
        ),
    )
    provider = FakePriceProvider(frame)
    monkeypatch.setattr(data_api, "_provider", provider)
    monkeypatch.setattr(data_api, "_auth_attempted", False)
    context = SimpleNamespace(current_dt=dt.datetime(2025, 1, 3, 14, 50), run_params={})
    data_api.set_current_context(context)
    set_option("use_real_price", True)
    set_option("avoid_future_data", True)
    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-05",
        provider_name="fake",
    )
    token = _activate_session(session)
    try:
        result = data_api.get_price(
            SECURITY_JQ,
            end_date=dt.datetime(2025, 1, 3, 14, 50),
            count=1,
            frequency="1m",
            fields=["close"],
            fq="pre",
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)
        data_api.set_current_context(None)
        reset_settings()

    assert result["close"].tolist() == pytest.approx([300.0])
    assert session.stats.degradations == 0


@pytest.mark.unit
def test_data_api_dynamic_pre_factor_block_degrades_when_factor_requested(monkeypatch):
    """
    验证用户显式请求 factor 字段时降级原路径，避免改变 provider 的 factor 语义。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    reset_settings()
    frame = pd.DataFrame(
        {"close": [10.0], "factor": [2.0]},
        index=pd.to_datetime(["2025-01-03 14:50:00"]),
    )
    provider = FakePriceProvider(frame)
    monkeypatch.setattr(data_api, "_provider", provider)
    monkeypatch.setattr(data_api, "_auth_attempted", False)
    context = SimpleNamespace(current_dt=dt.datetime(2025, 1, 3, 14, 50), run_params={})
    data_api.set_current_context(context)
    set_option("use_real_price", True)
    set_option("avoid_future_data", True)
    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-05",
        provider_name="fake",
    )
    token = _activate_session(session)
    try:
        result = data_api.get_price(
            SECURITY_JQ,
            end_date=dt.datetime(2025, 1, 3, 14, 50),
            count=1,
            frequency="1m",
            fields=["close", "factor"],
            fq="pre",
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)
        data_api.set_current_context(None)
        reset_settings()

    assert result["factor"].tolist() == [2.0]
    assert session.stats.cache_writes == 0
    assert session.stats.degradations == 1


@pytest.mark.unit
def test_data_api_dynamic_pre_factor_block_skips_miniqmt_provider(monkeypatch):
    """
    验证 MiniQMT 不走通用 factor 字段动态复权块，避免额外请求干扰 provider 内缓存。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    reset_settings()
    frame = pd.DataFrame(
        {"close": [10.0], "factor": [2.0]},
        index=pd.to_datetime(["2025-01-03"]),
    )
    provider = FakePriceProvider(frame)
    provider.name = "miniqmt"
    monkeypatch.setattr(data_api, "_provider", provider)
    monkeypatch.setattr(data_api, "_auth_attempted", False)
    context = SimpleNamespace(current_dt=dt.datetime(2025, 1, 4, 14, 50), run_params={})
    data_api.set_current_context(context)
    set_option("use_real_price", True)
    set_option("avoid_future_data", True)
    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-05",
        provider_name="miniqmt",
    )
    token = _activate_session(session)
    try:
        result = data_api.get_price(
            SECURITY_QMT,
            end_date=dt.datetime(2025, 1, 3, 15, 0),
            count=1,
            frequency="daily",
            fields=["close"],
            fq="pre",
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)
        data_api.set_current_context(None)
        reset_settings()

    assert result["close"].tolist() == [10.0]
    assert len(provider.calls) == 1
    assert provider.calls[0]["fields"] == ["close"]
    assert session.stats.cache_writes == 0


@pytest.mark.unit
def test_data_api_dynamic_pre_factor_block_preserves_missing_prices(monkeypatch):
    """
    验证动态前复权缓存不会把新上市前的缺失价格误填成 0。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    reset_settings()
    frame = pd.DataFrame(
        {"close": [None, 10.0], "factor": [1.0, 2.0]},
        index=pd.to_datetime(["2025-01-02", "2025-01-03"]),
    )
    provider = FakePriceProvider(frame)
    monkeypatch.setattr(data_api, "_provider", provider)
    monkeypatch.setattr(data_api, "_auth_attempted", False)
    context = SimpleNamespace(current_dt=dt.datetime(2025, 1, 3, 14, 50), run_params={})
    data_api.set_current_context(context)
    set_option("use_real_price", True)
    set_option("avoid_future_data", True)
    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-05",
        provider_name="fake",
    )
    token = _activate_session(session)
    try:
        result = data_api.get_price(
            SECURITY_JQ,
            end_date=dt.datetime(2025, 1, 2, 15, 0),
            count=1,
            frequency="daily",
            fields=["close"],
            fq="pre",
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)
        data_api.set_current_context(None)
        reset_settings()

    assert pd.isna(result["close"].iloc[0])
    assert session.stats.degradations == 0


@pytest.mark.unit
def test_miniqmt_backtest_session_dedupes_same_security_period(monkeypatch):
    """
    验证回测 session 内同一证券同一周期只触发一次 QMT 下载。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    frames = {
        (SECURITY_QMT, "1d", "none"): _build_qmt_frame(
            ["2024-12-20", "2025-01-01", "2025-01-02", "2025-01-03", "2025-01-05"]
        )
    }
    fake_xt = FakeXtData(frames)
    provider = _make_provider(monkeypatch, fake_xt, mode="backtest", auto_download=True)
    session = create_backtest_data_session(
        overrides={"enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-05",
        provider_name="miniqmt",
    )
    token = _activate_session(session)
    try:
        provider.get_price(SECURITY_JQ, end_date="2025-01-02", count=1, fq="none")
        provider.get_price(SECURITY_JQ, end_date="2025-01-03", count=1, fq="none")
    finally:
        session.close()
        reset_current_backtest_data_session(token)

    assert len(fake_xt.download_calls) == 1
    assert fake_xt.download_calls[0][0:2] == (SECURITY_QMT, "1d")
    assert session.stats.downloads == 1
    assert session.stats.skipped_downloads == 1
    assert session.qmt_manifest[-1]["skipped"] is True


@pytest.mark.unit
def test_miniqmt_count_window_with_start_date_uses_non_strict_coverage(monkeypatch):
    """
    验证带 count 的估算 start_date 不会导致 QMT 覆盖校验反复失败。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    frames = {(SECURITY_QMT, "1d", "none"): _build_qmt_frame(["2025-01-02", "2025-01-03"])}
    fake_xt = FakeXtData(frames)
    provider = _make_provider(monkeypatch, fake_xt, mode="backtest", auto_download=True)
    session = create_backtest_data_session(
        overrides={"enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-03",
        provider_name="miniqmt",
    )
    token = _activate_session(session)
    try:
        provider.get_price(
            SECURITY_JQ,
            start_date="2025-01-01",
            end_date="2025-01-03",
            count=1,
            fq="none",
        )
        provider.get_price(
            SECURITY_JQ,
            start_date="2025-01-02",
            end_date="2025-01-03",
            count=1,
            fq="none",
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)

    assert len(fake_xt.download_calls) == 1
    assert session.stats.coverage_failures == 0
    assert session.stats.skipped_downloads == 1


@pytest.mark.unit
def test_miniqmt_backtest_session_keeps_periods_independent(monkeypatch):
    """
    验证同一证券日线和分钟线使用独立 QMT 下载 key。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    frames = {
        (SECURITY_QMT, "1d", "none"): _build_qmt_frame(["2025-01-01", "2025-01-02"]),
        (SECURITY_QMT, "1m", "none"): _build_qmt_frame(
            ["2025-01-01 09:31:00", "2025-01-02 09:31:00"]
        ),
    }
    fake_xt = FakeXtData(frames)
    provider = _make_provider(monkeypatch, fake_xt, mode="backtest", auto_download=True)
    session = create_backtest_data_session(
        overrides={"enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-02",
        provider_name="miniqmt",
    )
    token = _activate_session(session)
    try:
        provider.get_price(SECURITY_JQ, end_date="2025-01-02", count=1, fq="none")
        provider.get_price(
            SECURITY_JQ, end_date="2025-01-02 09:31:00", count=1, frequency="1m", fq="none"
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)

    assert [(call[0], call[1]) for call in fake_xt.download_calls] == [
        (SECURITY_QMT, "1d"),
        (SECURITY_QMT, "1m"),
    ]


@pytest.mark.unit
def test_miniqmt_local_block_cache_reuses_dynamic_pre_factor_inputs(monkeypatch):
    """
    验证 MiniQMT 本地数据块缓存复用 raw/front_ratio 输入，动态复权仍逐日锚定。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    raw = _build_qmt_frame(
        [
            "2025-01-01",
            "2025-01-02",
            "2025-01-03",
            "2025-01-04",
            "2025-01-05",
            "2025-01-06",
        ]
    )
    front_ratio = raw.copy()
    for column in ("open", "high", "low", "close"):
        front_ratio[column] = front_ratio[column] * 10
    frames = {
        (SECURITY_QMT, "1d", "none"): raw,
        (SECURITY_QMT, "1d", "front_ratio"): front_ratio,
    }
    fake_xt = FakeXtData(frames)
    provider = _make_provider(monkeypatch, fake_xt, mode="backtest", auto_download=False)
    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-06",
        provider_name="miniqmt",
    )
    token = _activate_session(session)
    try:
        first = provider.get_price(
            SECURITY_JQ,
            start_date="2025-01-01",
            end_date="2025-01-03",
            count=2,
            fields=["close"],
            fq="pre",
            pre_factor_ref_date=dt.date(2025, 1, 3),
        )
        second = provider.get_price(
            SECURITY_JQ,
            start_date="2025-01-02",
            end_date="2025-01-04",
            count=2,
            fields=["close"],
            fq="pre",
            pre_factor_ref_date=dt.date(2025, 1, 4),
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)

    assert [call[:3] for call in fake_xt.local_calls] == [
        (SECURITY_QMT, "1d", "none"),
        (SECURITY_QMT, "1d", "front_ratio"),
    ]
    assert first["close"].tolist() == [11.0, 12.0]
    assert second["close"].tolist() == [12.0, 13.0]
    assert session.stats.cache_writes == 2
    assert session.stats.cache_hits == 2


@pytest.mark.unit
def test_miniqmt_live_mode_ignores_backtest_session(monkeypatch):
    """
    验证 live provider 不读取回测 session 的 downloaded 状态。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    frames = {(SECURITY_QMT, "1d", "none"): _build_qmt_frame(["2025-01-01", "2025-01-02"])}
    fake_xt = FakeXtData(frames)
    provider = _make_provider(monkeypatch, fake_xt, mode="live", auto_download=True)
    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-02",
        provider_name="miniqmt",
    )
    token = _activate_session(session)
    try:
        provider.get_price(SECURITY_JQ, end_date="2025-01-01", count=1, fq="none")
        provider.get_price(SECURITY_JQ, end_date="2025-01-02", count=1, fq="none")
    finally:
        session.close()
        reset_current_backtest_data_session(token)

    assert len(fake_xt.download_calls) == 2
    assert len(fake_xt.local_calls) == 2
    assert session.stats.downloads == 0
    assert session.stats.skipped_downloads == 0


@pytest.mark.unit
def test_miniqmt_coverage_failure_is_not_registered_as_success(monkeypatch):
    """
    验证覆盖不足时不会把 QMT 下载记录登记为已覆盖成功。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    frames = {(SECURITY_QMT, "1d", "none"): _build_qmt_frame(["2025-01-01"])}
    fake_xt = FakeXtData(frames)
    provider = _make_provider(monkeypatch, fake_xt, mode="backtest", auto_download=True)
    session = create_backtest_data_session(
        overrides={"enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-05",
        provider_name="miniqmt",
    )
    token = _activate_session(session)
    try:
        provider.get_price(SECURITY_JQ, end_date="2025-01-02", count=1, fq="none")
        provider.get_price(SECURITY_JQ, end_date="2025-01-03", count=1, fq="none")
    finally:
        session.close()
        reset_current_backtest_data_session(token)

    assert len(fake_xt.download_calls) == 2
    assert session.stats.coverage_failures == 2
    assert all(item["coverage_ok"] is False for item in session.qmt_manifest)


@pytest.mark.unit
def test_data_api_price_block_keeps_provider_independent(monkeypatch):
    """
    验证行情块缓存按 provider 隔离，不会跨数据源静默复用。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    frame_a = _build_price_frame(
        ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"],
        base=10.0,
    )
    frame_b = _build_price_frame(
        ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"],
        base=100.0,
    )
    provider_a = FakePriceProvider(frame_a)
    provider_a.name = "fake_a"
    provider_b = FakePriceProvider(frame_b)
    provider_b.name = "fake_b"
    context = SimpleNamespace(current_dt=dt.datetime(2025, 1, 3, 15, 0), run_params={})
    data_api.set_current_context(context)
    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-05",
        provider_name="fake_a",
    )
    token = _activate_session(session)
    try:
        monkeypatch.setattr(data_api, "_provider", provider_a)
        monkeypatch.setattr(data_api, "_auth_attempted", False)
        first = data_api.get_price(
            SECURITY_JQ,
            end_date=context.current_dt,
            count=2,
            fields=["close"],
            fq="none",
        )

        monkeypatch.setattr(data_api, "_provider", provider_b)
        monkeypatch.setattr(data_api, "_auth_attempted", False)
        second = data_api.get_price(
            SECURITY_JQ,
            end_date=context.current_dt,
            count=2,
            fields=["close"],
            fq="none",
        )

        monkeypatch.setattr(data_api, "_provider", provider_a)
        monkeypatch.setattr(data_api, "_auth_attempted", False)
        third = data_api.get_price(
            SECURITY_JQ,
            end_date=context.current_dt,
            count=2,
            fields=["close"],
            fq="none",
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)
        data_api.set_current_context(None)

    assert first["close"].tolist() == [11.0, 12.0]
    assert second["close"].tolist() == [101.0, 102.0]
    assert third["close"].tolist() == [11.0, 12.0]
    assert len(provider_a.calls) == 1
    assert len(provider_b.calls) == 1
    assert session.stats.cache_writes == 2
    assert session.stats.cache_hits == 1


@pytest.mark.unit
def test_data_api_cached_daily_block_still_enforces_intraday_visibility(monkeypatch):
    """
    验证已缓存日线块不会绕过盘前、盘中和收盘后的字段可见性规则。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    reset_settings()
    frame = _build_price_frame(
        ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"],
        base=10.0,
    )
    provider = FakePriceProvider(frame)
    monkeypatch.setattr(data_api, "_provider", provider)
    monkeypatch.setattr(data_api, "_auth_attempted", False)
    context = SimpleNamespace(current_dt=dt.datetime(2025, 1, 3, 15, 0), run_params={})
    data_api.set_current_context(context)
    set_option("avoid_future_data", True)
    session = create_backtest_data_session(
        overrides={"enabled": True, "price_block_cache_enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-05",
        provider_name="fake",
    )
    token = _activate_session(session)
    try:
        close_df = data_api.get_price(
            SECURITY_JQ,
            end_date=context.current_dt,
            count=1,
            frequency="daily",
            fields=["close"],
            fq="none",
        )
        assert close_df["close"].tolist() == [12.0]
        assert session.stats.cache_writes == 1

        context.current_dt = dt.datetime(2025, 1, 4, 9, 0)
        session.advance_bar(context.current_dt)
        with pytest.raises(FutureDataError):
            data_api.get_price(
                SECURITY_JQ,
                end_date=context.current_dt,
                count=1,
                frequency="daily",
                fields=["open"],
                fq="none",
            )

        context.current_dt = dt.datetime(2025, 1, 4, 10, 0)
        session.advance_bar(context.current_dt)
        with pytest.raises(FutureDataError):
            data_api.get_price(
                SECURITY_JQ,
                end_date=context.current_dt,
                count=1,
                frequency="daily",
                fields=["close"],
                fq="none",
            )

        context.current_dt = dt.datetime(2025, 1, 4, 15, 0)
        session.advance_bar(context.current_dt)
        after_close = data_api.get_price(
            SECURITY_JQ,
            end_date=context.current_dt,
            count=1,
            frequency="daily",
            fields=["close"],
            fq="none",
        )
    finally:
        session.close()
        reset_current_backtest_data_session(token)
        data_api.set_current_context(None)
        reset_settings()

    assert after_close["close"].tolist() == [13.0]
    assert len(provider.calls) == 1
    assert session.stats.cache_hits == 1


@pytest.mark.unit
def test_miniqmt_backtest_session_downloads_dynamic_new_security(monkeypatch):
    """
    验证动态标的池里首次出现的新标的会按需下载，并不会假设启动时已知全集。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    frames = {
        (SECURITY_QMT, "1d", "none"): _build_qmt_frame(["2025-01-01", "2025-01-02"]),
        (SECURITY_QMT_B, "1d", "none"): _build_qmt_frame(["2025-01-01", "2025-01-02"]),
    }
    fake_xt = FakeXtData(frames)
    provider = _make_provider(monkeypatch, fake_xt, mode="backtest", auto_download=True)
    session = create_backtest_data_session(
        overrides={"enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-02",
        provider_name="miniqmt",
    )
    token = _activate_session(session)
    try:
        provider.get_price(SECURITY_JQ, end_date="2025-01-01", count=1, fq="none")
        provider.get_price(SECURITY_JQ_B, end_date="2025-01-02", count=1, fq="none")
        provider.get_price(SECURITY_JQ, end_date="2025-01-02", count=1, fq="none")
    finally:
        session.close()
        reset_current_backtest_data_session(token)

    assert [(call[0], call[1]) for call in fake_xt.download_calls] == [
        (SECURITY_QMT, "1d"),
        (SECURITY_QMT_B, "1d"),
    ]
    assert session.stats.downloads == 2
    assert session.stats.skipped_downloads == 1


@pytest.mark.unit
def test_live_context_prefers_live_current_data_over_backtest_session(monkeypatch):
    """
    验证实盘上下文不会使用回测 current bar 快照。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    reset_globals()
    reset_settings()

    class LiveProvider:
        """
        实盘 current data 测试用 provider。
        """

        name = "live"
        requires_live_data = False

        def get_live_current(self, security: str):
            """
            返回实时行情快照。

            Args:
                security: 证券代码。

            Returns:
                dict: 实时行情字段。
            """
            return {
                "last_price": 12.34,
                "high_limit": 13.0,
                "low_limit": 11.0,
                "paused": False,
            }

    current_dt = dt.datetime(2025, 1, 2, 10, 0)
    context = SimpleNamespace(current_dt=current_dt, run_params={"is_live": True})
    session = create_backtest_data_session(
        overrides={"enabled": True, "current_bar_cache_enabled": True},
        start_date="2025-01-01",
        end_date="2025-01-02",
        provider_name="fake",
    )
    session.set_current_bar_value(
        ("current_data", SECURITY_JQ, current_dt),
        SecurityUnitData(security=SECURITY_JQ, last_price=99.0),
    )
    token = _activate_session(session)
    try:
        g.live_trade = True
        data_api.set_current_context(context)
        monkeypatch.setattr(data_api, "_provider", LiveProvider())
        snap = data_api.get_current_data()[SECURITY_JQ]
    finally:
        g.live_trade = False
        data_api.set_current_context(None)
        session.close()
        reset_current_backtest_data_session(token)
        reset_globals()
        reset_settings()

    assert snap.last_price == 12.34
    assert session.stats.current_bar_hits == 0


def _run_synthetic_equivalence_backtest(monkeypatch, enabled: bool):
    """
    运行合成策略回测并返回用于等价对比的关键结果。

    Args:
        monkeypatch: pytest monkeypatch fixture。
        enabled: 是否启用回测数据会话。

    Returns:
        tuple: 回测结果、引擎实例和 provider。
    """
    reset_settings()
    reset_globals()
    clear_order_queue()
    frame = _build_price_frame(
        ["2025-01-01", "2025-01-02", "2025-01-03"],
        base=10.0,
    )
    provider = SyntheticBacktestProvider(frame)
    monkeypatch.setattr(data_api, "_provider", provider)
    monkeypatch.setattr(data_api, "_auth_attempted", False)

    def initialize(context):
        """
        初始化合成策略。

        Args:
            context: 回测上下文。
        """
        _ = context
        set_option("use_real_price", False)
        set_option("avoid_future_data", True)

    def handle_data(context, data):
        """
        首个交易日买入一手，用于验证订单、成交、持仓和资金等价。

        Args:
            context: 回测上下文。
            data: 当前行情容器。
        """
        _ = data
        from bullet_trade.core.api import order

        if context.current_dt.date() == dt.date(2025, 1, 1):
            order(SECURITY_JQ, 100)

    engine = BacktestEngine(
        start_date="2025-01-01",
        end_date="2025-01-03",
        initial_cash=100000,
        initialize=initialize,
        handle_data=handle_data,
        data_session_config={"enabled": enabled, "price_block_cache_enabled": enabled},
    )
    results = engine.run()
    clear_order_queue()
    return results, engine, provider


def _stable_trade_rows(trades):
    """
    转换成交记录为稳定可比结构。

    Args:
        trades: Trade 对象列表。

    Returns:
        list[tuple]: 去除随机订单号后的成交信息。
    """
    return [
        (
            trade.security,
            trade.amount,
            round(float(trade.price), 6),
            trade.time,
            round(float(trade.commission), 2),
            round(float(trade.tax), 2),
        )
        for trade in trades
    ]


def _stable_order_rows(engine: BacktestEngine):
    """
    转换订单记录为稳定可比结构。

    Args:
        engine: 回测引擎。

    Returns:
        list[tuple]: 去除随机订单号后的订单信息。
    """
    rows = []
    for order in engine.orders.values():
        rows.append(
            (
                order.security,
                order.amount,
                order.status.value if hasattr(order.status, "value") else str(order.status),
                round(float(order.price), 6),
                int(getattr(order, "filled", 0) or 0),
            )
        )
    return rows


@pytest.mark.unit
def test_synthetic_strategy_results_are_equivalent_with_data_session(monkeypatch):
    """
    验证合成策略在优化前后交易日、订单、成交、持仓、现金和收益保持一致。

    Args:
        monkeypatch: pytest monkeypatch fixture。
    """
    baseline, baseline_engine, _provider_a = _run_synthetic_equivalence_backtest(
        monkeypatch, enabled=False
    )
    optimized, optimized_engine, provider_b = _run_synthetic_equivalence_backtest(
        monkeypatch, enabled=True
    )

    pd.testing.assert_frame_equal(
        baseline["daily_records"][["total_value", "cash", "positions_value"]],
        optimized["daily_records"][["total_value", "cash", "positions_value"]],
    )
    pd.testing.assert_frame_equal(
        baseline["daily_positions"].reset_index(drop=True),
        optimized["daily_positions"].reset_index(drop=True),
    )
    assert _stable_trade_rows(baseline["trades"]) == _stable_trade_rows(optimized["trades"])
    assert _stable_order_rows(baseline_engine) == _stable_order_rows(optimized_engine)
    assert baseline["summary"]["策略收益"] == optimized["summary"]["策略收益"]
    assert baseline["summary"]["最大回撤"] == optimized["summary"]["最大回撤"]
    assert optimized["backtest_data_session"]["stats"]["current_bar_hits"] >= 1
    assert provider_b.calls
