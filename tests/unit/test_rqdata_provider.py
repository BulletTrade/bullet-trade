"""RQDataProvider 离线单元测试。"""

from __future__ import annotations

from typing import Tuple

import pandas as pd
import pytest

from bullet_trade.data.providers.rqdata import RQDataProvider


class DummyRQDataModule:
    """模拟 rqdatac SDK，用于验证 provider 合同而不访问真实账号。"""

    def __init__(self) -> None:
        """初始化调用记录。"""
        self.init_calls = []
        self.get_price_calls = []
        self.get_previous_trading_date_calls = []
        self.get_trading_dates_calls = []
        self.get_ex_factor_calls = []
        self.is_suspended_calls = []

    def init(self, *args, **kwargs) -> None:
        """记录认证调用参数。"""
        self.init_calls.append({"args": args, "kwargs": kwargs})

    def get_price(self, **kwargs) -> pd.DataFrame:
        """返回可预测的日线或分钟线 MultiIndex 行情。"""
        self.get_price_calls.append(kwargs)
        order_book_ids = kwargs.get("order_book_ids", [])
        if isinstance(order_book_ids, str):
            order_book_ids = [order_book_ids]
        start = pd.Timestamp(kwargs.get("start_date") or "2024-01-02")
        end = pd.Timestamp(kwargs.get("end_date") or start)
        freq = str(kwargs.get("frequency") or "1d")
        if freq.endswith("m"):
            dates = pd.date_range(start, end, freq="1min")
            if len(dates) == 0:
                dates = pd.DatetimeIndex([end])
        else:
            dates = pd.bdate_range(start.normalize(), end.normalize())
            if len(dates) == 0:
                dates = pd.DatetimeIndex([start.normalize()])
        fields = list(kwargs.get("fields") or ["open", "close"])
        rows = []
        for code in order_book_ids:
            for idx, ts in enumerate(dates):
                row = {"order_book_id": code, "datetime": ts}
                for field in fields:
                    if field == "total_turnover":
                        row[field] = 10500.0 + idx
                    elif field == "volume":
                        row[field] = 1000.0
                    elif field == "limit_up":
                        row[field] = 11.0
                    elif field == "limit_down":
                        row[field] = 9.0
                    elif field == "prev_close":
                        row[field] = 10.0
                    else:
                        row[field] = 10.0 + idx
                rows.append(row)
        df = pd.DataFrame(rows)
        return df.set_index(["order_book_id", "datetime"])

    def get_previous_trading_date(self, date, n=1, market="cn"):
        """记录 count 窗口推导并返回前推日期。"""
        self.get_previous_trading_date_calls.append({"date": date, "n": n, "market": market})
        return pd.Timestamp(date) - pd.Timedelta(days=max(int(n), 1))

    def get_trading_dates(self, start_date, end_date, market="cn"):
        """返回工作日交易日序列。"""
        self.get_trading_dates_calls.append(
            {"start_date": start_date, "end_date": end_date, "market": market}
        )
        return pd.bdate_range(pd.Timestamp(start_date), pd.Timestamp(end_date)).to_pydatetime()

    def get_ex_factor(self, order_book_ids, start_date=None, end_date=None, market="cn"):
        """返回两段复权因子，便于测试动态锚定。"""
        self.get_ex_factor_calls.append(
            {
                "order_book_ids": order_book_ids,
                "start_date": start_date,
                "end_date": end_date,
                "market": market,
            }
        )
        codes = [order_book_ids] if isinstance(order_book_ids, str) else list(order_book_ids)
        rows = []
        for code in codes:
            rows.append(
                {"order_book_id": code, "ex_date": pd.Timestamp("2024-01-01"), "ex_cum_factor": 1.0}
            )
            rows.append(
                {"order_book_id": code, "ex_date": pd.Timestamp("2024-01-04"), "ex_cum_factor": 2.0}
            )
        return pd.DataFrame(rows)

    def is_suspended(self, order_book_ids, start_date=None, end_date=None, market="cn"):
        """返回全 False 停牌表。"""
        self.is_suspended_calls.append(
            {
                "order_book_ids": order_book_ids,
                "start_date": start_date,
                "end_date": end_date,
                "market": market,
            }
        )
        dates = pd.bdate_range(pd.Timestamp(start_date), pd.Timestamp(end_date))
        return pd.DataFrame(False, index=list(order_book_ids), columns=dates)


def _provider() -> Tuple[RQDataProvider, DummyRQDataModule]:
    """创建注入 fake SDK 的 RQDataProvider。"""
    dummy = DummyRQDataModule()
    provider = RQDataProvider({"rqdatac": dummy, "authenticated": True})
    return provider, dummy


@pytest.mark.unit
def test_rqdata_auth_prefers_license() -> None:
    """license 认证应通过 rqdatac.init(license=...) 调用。"""
    dummy = DummyRQDataModule()
    provider = RQDataProvider({"rqdatac": dummy, "license": "lic"})

    provider.auth()

    assert dummy.init_calls == [{"args": (), "kwargs": {"license": "lic"}}]


@pytest.mark.unit
def test_rqdata_auth_requires_credentials() -> None:
    """缺少 license 和账号时应给出清晰错误。"""
    dummy = DummyRQDataModule()
    provider = RQDataProvider({"rqdatac": dummy})

    with pytest.raises(RuntimeError, match="RQData 账号未配置"):
        provider.auth()


@pytest.mark.unit
def test_rqdata_get_price_single_daily_maps_fields() -> None:
    """单证券日线应返回普通字段列，并把 money 映射到 total_turnover。"""
    provider, dummy = _provider()

    df = provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02",
        end_date="2024-01-03",
        fields=["close", "money"],
        fq="none",
    )

    assert list(df.columns) == ["close", "money"]
    assert dummy.get_price_calls[-1]["fields"] == ["close", "total_turnover"]
    assert df.index[0] == pd.Timestamp("2024-01-02")


@pytest.mark.unit
def test_rqdata_multi_panel_shape_is_field_code() -> None:
    """多证券 panel=True 应返回 (field, code) MultiIndex 列。"""
    provider, _ = _provider()

    df = provider.get_price(
        ["000001.XSHE", "600000.XSHG"],
        start_date="2024-01-02",
        end_date="2024-01-03",
        fields=["open", "close"],
        panel=True,
    )

    assert isinstance(df.columns, pd.MultiIndex)
    assert df.columns.names == ["field", "code"]
    assert ("close", "000001.XSHE") in df.columns
    assert ("open", "600000.XSHG") in df.columns


@pytest.mark.unit
def test_rqdata_panel_false_returns_time_code_long_table() -> None:
    """panel=False 应返回 time/code 长表，便于外层兼容层统一处理。"""
    provider, _ = _provider()

    df = provider.get_price(
        ["000001.XSHE", "600000.XSHG"],
        start_date="2024-01-02",
        end_date="2024-01-02",
        fields=["close"],
        panel=False,
    )

    assert list(df.columns) == ["time", "code", "close"]
    assert set(df["code"]) == {"000001.XSHE", "600000.XSHG"}


@pytest.mark.unit
def test_rqdata_minute_count_uses_nonzero_trading_day_window() -> None:
    """分钟线 count 推导不能出现 0 天窗口。"""
    provider, dummy = _provider()

    provider.get_price(
        "000001.XSHE",
        end_date="2024-01-03 09:31:00",
        frequency="1m",
        fields=["close"],
        count=1,
    )

    assert dummy.get_previous_trading_date_calls
    assert dummy.get_previous_trading_date_calls[-1]["n"] >= 1


@pytest.mark.unit
def test_rqdata_extra_fields_and_minute_limits_are_postprocessed() -> None:
    """factor、paused、avg 和分钟涨跌停应由 provider 后处理补齐。"""
    provider, dummy = _provider()

    df = provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02 09:31:00",
        end_date="2024-01-02 09:32:00",
        frequency="1m",
        fields=["close", "volume", "money", "avg", "factor", "paused", "high_limit", "low_limit"],
        fq="none",
    )

    for field in ("avg", "factor", "paused", "high_limit", "low_limit"):
        assert field in df.columns
    assert dummy.is_suspended_calls
    assert any(call["frequency"] == "1d" for call in dummy.get_price_calls)


@pytest.mark.unit
def test_rqdata_pre_factor_ref_date_uses_factor_anchor() -> None:
    """pre_factor_ref_date 应用 factor/factor_ref 锚定价格。"""
    provider, _ = _provider()

    df = provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02",
        end_date="2024-01-03",
        fields=["close"],
        fq="pre",
        pre_factor_ref_date="2024-01-04",
    )

    assert df.iloc[0]["close"] == 5.0


@pytest.mark.unit
def test_rqdata_security_type_distinguishes_sz_stock_from_sh_index() -> None:
    """000001.XSHE 是深市股票，000001.XSHG 才按指数兜底。"""
    provider, _ = _provider()

    assert provider.get_security_info("000001.XSHE")["type"] == "stock"
    assert provider.get_security_info("000001.XSHG")["type"] == "index"
    assert provider.get_security_info("399001.XSHE")["type"] == "index"
