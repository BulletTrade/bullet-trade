"""EasyTdxProvider 离线单元测试。"""

from __future__ import annotations

import pandas as pd
import pytest

from bullet_trade.data.providers.easy_tdx import EasyTdxProvider


class FakeMacClient:
    """模拟 easy_tdx.MacClient，用于离线验证 online 模式。"""

    def __init__(self, *args, **kwargs) -> None:
        """记录初始化参数。"""
        self.args = args
        self.kwargs = kwargs
        self.connected = False

    @classmethod
    def from_best_host(cls, *args, **kwargs):
        """模拟自动选择行情服务器。"""
        return cls(*args, **kwargs)

    def connect(self) -> None:
        """标记连接成功。"""
        self.connected = True

    def get_stock_kline(self, **kwargs) -> pd.DataFrame:
        """返回两行 K 线数据。"""
        _ = kwargs
        return pd.DataFrame(
            [
                {
                    "datetime": "2024-01-02 09:31:00",
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "vol": 12.0,
                    "amount": 1212.0,
                },
                {
                    "datetime": "2024-01-02 09:32:00",
                    "open": 10.1,
                    "high": 10.3,
                    "low": 10.0,
                    "close": 10.2,
                    "vol": 13.0,
                    "amount": 1326.0,
                },
            ]
        )

    def get_stock_quotes(self, symbols) -> pd.DataFrame:
        """返回实时行情快照。"""
        _ = symbols
        return pd.DataFrame(
            [
                {
                    "price": 10.25,
                    "open": 10.0,
                    "high": 10.4,
                    "low": 9.9,
                    "vol": 100.0,
                    "amount": 102500.0,
                    "pre_close": 10.0,
                    "limit_up": 11.0,
                    "limit_down": 9.0,
                    "trading_status": 0,
                }
            ]
        )

    def get_stock_quotes_list(self, category, count=5000) -> pd.DataFrame:
        """返回证券列表。"""
        _ = category, count
        return pd.DataFrame([{"market": 0, "code": "000001", "name": "平安银行"}])


class RecordingMacClient(FakeMacClient):
    """记录 K 线请求参数，用于验证长区间请求不会被 8000 条截断。"""

    calls = []

    def get_stock_kline(self, **kwargs) -> pd.DataFrame:
        """记录请求并返回空数据，避免单测构造大量 K 线。"""
        self.__class__.calls.append(kwargs)
        return pd.DataFrame()


class FailingMacClient(FakeMacClient):
    """模拟连接失败的 MacClient。"""

    def connect(self) -> None:
        """抛出连接失败异常。"""
        raise OSError("connect failed")


class FactorMacClient(FakeMacClient):
    """返回跨除权日的未复权日线，用于验证 factor 构造。"""

    def get_stock_kline(self, **kwargs) -> pd.DataFrame:
        """返回两天日线，除权日前收盘价为 10 元。"""
        _ = kwargs
        return pd.DataFrame(
            [
                {
                    "datetime": "2024-01-02",
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "vol": 1000.0,
                    "amount": 10000.0,
                },
                {
                    "datetime": "2024-01-03",
                    "open": 9.0,
                    "high": 9.0,
                    "low": 9.0,
                    "close": 9.0,
                    "vol": 1000.0,
                    "amount": 9000.0,
                },
            ]
        )


class FactorTdxClient:
    """模拟 TdxClient 除权除息接口。"""

    def __init__(self, *args, **kwargs) -> None:
        """记录构造参数；测试中不建立真实连接。"""
        self.args = args
        self.kwargs = kwargs

    def connect(self) -> None:
        """模拟连接成功。"""

    def close(self) -> None:
        """模拟关闭连接。"""

    def get_xdxr_info(self, market, code) -> pd.DataFrame:
        """返回 2024-01-03 每股派息 1 元的除息事件。"""
        _ = market, code
        return pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2024-01-03"),
                    "category": 1,
                    "fenhong": 1.0,
                    "songzhuangu": 0.0,
                    "peigu": 0.0,
                    "peigujia": 0.0,
                }
            ]
        )


def _stub_provider() -> EasyTdxProvider:
    """创建显式 stub 模式 provider。"""
    return EasyTdxProvider({"use_stub": True})


def _fake_provider() -> EasyTdxProvider:
    """创建注入 fake client 的 provider。"""
    return EasyTdxProvider({"mac_client_cls": FakeMacClient, "timeout": 1.0})


@pytest.mark.unit
def test_easy_tdx_stub_mode_is_explicit() -> None:
    """stub 只能显式 use_stub=True 开启。"""
    provider = _stub_provider()

    provider.auth()
    df = provider.get_price("000001.XSHE", count=2, fields=["close"])

    assert provider.requires_live_data is False
    assert not df.empty
    assert list(df.columns) == ["close"]


@pytest.mark.unit
def test_easy_tdx_multi_panel_shape_is_field_code() -> None:
    """多证券 panel=True 应返回 (field, code) MultiIndex。"""
    provider = _stub_provider()

    df = provider.get_price(
        ["000001.XSHE", "600519.XSHG"],
        count=2,
        fields=["open", "close"],
        panel=True,
    )

    assert isinstance(df.columns, pd.MultiIndex)
    assert df.columns.names == ["field", "code"]
    assert ("close", "600519.XSHG") in df.columns


@pytest.mark.unit
def test_easy_tdx_panel_false_returns_time_code_long_table() -> None:
    """panel=False 应返回 time/code 长表。"""
    provider = _stub_provider()

    df = provider.get_price(
        ["000001.XSHE", "600519.XSHG"],
        count=1,
        fields=["close"],
        panel=False,
    )

    assert list(df.columns) == ["time", "code", "close"]
    assert set(df["code"]) == {"000001.XSHE", "600519.XSHG"}


@pytest.mark.unit
def test_easy_tdx_fake_online_price_normalizes_volume_and_shape() -> None:
    """fake online 模式应保持 K 线 volume=股，并返回单证券字段列。"""
    provider = _fake_provider()

    df = provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02 09:31:00",
        end_date="2024-01-02 09:32:00",
        frequency="1m",
        fields=["close", "volume"],
        fq="none",
    )

    assert list(df.columns) == ["close", "volume"]
    assert df.iloc[0]["volume"] == 12.0
    assert df.index[0] == pd.Timestamp("2024-01-02 09:31:00")


@pytest.mark.unit
def test_easy_tdx_long_range_request_not_capped_at_8000() -> None:
    """长分钟区间请求应允许超过 8000 条，避免只返回最新窗口。"""
    RecordingMacClient.calls = []
    provider = EasyTdxProvider({"mac_client_cls": RecordingMacClient, "timeout": 1.0})

    provider.get_price(
        "000001.XSHE",
        start_date="2026-01-19 09:31:00",
        end_date="2026-06-16 15:00:00",
        frequency="1m",
        fields=["close"],
        fq="none",
    )

    assert RecordingMacClient.calls
    assert RecordingMacClient.calls[-1]["count"] > 8000


@pytest.mark.unit
def test_easy_tdx_old_short_daily_range_fetches_enough_history() -> None:
    """旧日期短日线窗口也应按距今跨度估算请求条数。"""
    RecordingMacClient.calls = []
    provider = EasyTdxProvider({"mac_client_cls": RecordingMacClient, "timeout": 1.0})

    provider.get_price(
        "000001.XSHE",
        start_date="2024-01-01",
        end_date="2024-01-08",
        frequency="daily",
        fields=["close"],
        fq="none",
    )

    assert RecordingMacClient.calls
    assert RecordingMacClient.calls[-1]["count"] > 500


@pytest.mark.unit
def test_easy_tdx_get_bars_wraps_get_price_without_dynamic_anchor() -> None:
    """get_bars 应复用 get_price，默认 fq_ref_date 不应触发动态锚定限制。"""
    provider = _fake_provider()

    df = provider.get_bars(
        "000001.XSHE",
        count=1,
        unit="1m",
        fields=["date", "close"],
        end_dt="2024-01-02 09:32:00",
        df=True,
    )

    assert list(df.columns) == ["close"]
    assert df.iloc[-1]["close"] == 10.2


@pytest.mark.unit
def test_easy_tdx_real_mode_connection_failure_does_not_use_stub() -> None:
    """真实模式连接失败应抛异常，不能静默返回 stub 假行情。"""
    provider = EasyTdxProvider({"mac_client_cls": FailingMacClient, "timeout": 1.0})

    with pytest.raises(RuntimeError, match="不会自动返回假行情"):
        provider.get_price("000001.XSHE", count=1, fields=["close"])


@pytest.mark.unit
def test_easy_tdx_constructs_factor_from_xdxr_events() -> None:
    """请求 factor 时应由 TDX 除权除息事件构造累计复权因子。"""
    provider = EasyTdxProvider(
        {
            "mac_client_cls": FactorMacClient,
            "tdx_client_cls": FactorTdxClient,
            "timeout": 1.0,
        }
    )

    df = provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02",
        end_date="2024-01-03",
        frequency="daily",
        fields=["factor"],
        fq="pre",
    )

    assert list(df.columns) == ["factor"]
    assert df.iloc[0]["factor"] == pytest.approx(0.9)
    assert df.iloc[1]["factor"] == pytest.approx(1.0)


@pytest.mark.unit
def test_easy_tdx_pre_factor_ref_date_uses_constructed_factor() -> None:
    """pre_factor_ref_date 应按 raw * factor / factor_ref 动态前复权。"""
    provider = EasyTdxProvider(
        {
            "mac_client_cls": FactorMacClient,
            "tdx_client_cls": FactorTdxClient,
            "timeout": 1.0,
        }
    )

    df = provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02",
        end_date="2024-01-03",
        frequency="daily",
        fields=["close", "factor"],
        fq="pre",
        pre_factor_ref_date="2024-01-03",
    )

    assert df.iloc[0]["close"] == pytest.approx(9.0)
    assert df.iloc[1]["close"] == pytest.approx(9.0)
    assert df.iloc[0]["factor"] == pytest.approx(0.9)


@pytest.mark.unit
def test_easy_tdx_live_current_uses_quote_fields() -> None:
    """get_live_current 应直接使用 quote 字段，不需要历史 K 线猜测。"""
    provider = _fake_provider()

    snap = provider.get_live_current("000001.XSHE")

    assert snap == {
        "last_price": 10.25,
        "high_limit": 11.0,
        "low_limit": 9.0,
        "paused": False,
    }


@pytest.mark.unit
def test_easy_tdx_quote_accepts_real_easy_tdx_field_names() -> None:
    """真实 easy_tdx quote 的 close/buy_price_limit/sell_price_limit 应能映射。"""

    class QuoteFieldClient(FakeMacClient):
        def get_stock_quotes(self, symbols) -> pd.DataFrame:
            _ = symbols
            return pd.DataFrame(
                [
                    {
                        "close": 10.26,
                        "open": 10.0,
                        "high": 10.4,
                        "low": 9.9,
                        "vol": 100.0,
                        "amount": 102600.0,
                        "pre_close": 10.0,
                        "buy_price_limit": 11.0,
                        "sell_price_limit": 9.0,
                        "trading_status": 0,
                    }
                ]
            )

    provider = EasyTdxProvider({"mac_client_cls": QuoteFieldClient, "timeout": 1.0})

    snap = provider.get_live_current("000001.XSHE")

    assert snap == {
        "last_price": 10.26,
        "high_limit": 11.0,
        "low_limit": 9.0,
        "paused": False,
    }


@pytest.mark.unit
def test_easy_tdx_get_all_securities_from_fake_quotes_list() -> None:
    """get_all_securities 应能把通达信 market/code 转成聚宽代码。"""
    provider = _fake_provider()

    df = provider.get_all_securities(types="stock")

    assert "000001.XSHE" in df.index
    assert df.loc["000001.XSHE", "display_name"] == "平安银行"


@pytest.mark.unit
def test_easy_tdx_security_type_distinguishes_sz_stock_from_sh_index() -> None:
    """000001.XSHE 是深市股票，000001.XSHG 才按指数兜底。"""
    provider = _stub_provider()

    assert provider.get_security_info("000001.XSHE")["type"] == "stock"
    assert provider.get_security_info("000001.XSHG")["type"] == "index"


@pytest.mark.unit
def test_easy_tdx_split_dividend_without_tdx_client_returns_empty() -> None:
    """SDK 不提供 TdxClient 时，除权除息接口应降级为空列表。"""
    provider = _fake_provider()

    assert provider.get_split_dividend("000001.XSHE") == []
