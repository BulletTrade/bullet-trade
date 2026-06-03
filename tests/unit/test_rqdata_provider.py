"""
RQData 数据源基础接口测试。
"""

import datetime as dt

import pandas as pd
import pytest

from bullet_trade.data.providers.rqdata import RQDataProvider


class DummyRQDataModule:
    """模拟 rqdatac 模块，用于单元测试。"""

    def __init__(self):
        self.get_price_calls = []
        self.get_trading_dates_calls = []
        self.get_previous_trading_date_calls = []
        self.get_ex_factor_calls = []
        self.is_suspended_calls = []

    def init(self, username, password):
        pass

    def get_price(self, **kwargs):
        self.get_price_calls.append(kwargs)
        # 返回模拟数据
        if kwargs.get("expect_df"):
            order_book_ids = kwargs.get("order_book_ids", [])
            if isinstance(order_book_ids, str):
                order_book_ids = [order_book_ids]
            start_date = kwargs.get("start_date", "2024-01-02")
            end_date = kwargs.get("end_date", "2024-01-02")

            # 生成多个日期的数据
            dates = pd.bdate_range(start=start_date, end=end_date)
            if len(dates) == 0:
                dates = [pd.Timestamp(start_date)]

            rows = []
            for oid in order_book_ids:
                for d in dates:
                    rows.append((oid, d))

            index = pd.MultiIndex.from_tuples(rows, names=["order_book_id", "date"])
            n = len(rows)
            return pd.DataFrame(
                {
                    "open": [10.0] * n,
                    "close": [10.5] * n,
                    "high": [11.0] * n,
                    "low": [9.5] * n,
                    "volume": [1000.0] * n,
                    "total_turnover": [10500.0] * n,
                },
                index=index,
            )
        return None

    def get_trading_dates(self, start_date, end_date, market="cn"):
        self.get_trading_dates_calls.append(
            {"start_date": start_date, "end_date": end_date, "market": market}
        )
        # 返回 start_date 到 end_date 之间的交易日
        dates = pd.bdate_range(start=start_date, end=end_date)
        return dates.to_pydatetime().tolist()

    def get_previous_trading_date(self, date, n=1, market="cn"):
        self.get_previous_trading_date_calls.append(
            {"date": date, "n": n, "market": market}
        )
        # 简单返回 date - n 个工作日
        return pd.Timestamp(date) - pd.Timedelta(days=n)

    def get_next_trading_date(self, date, n=1, market="cn"):
        self.get_next_trading_date_calls = getattr(self, "get_next_trading_date_calls", [])
        self.get_next_trading_date_calls.append(
            {"date": date, "n": n, "market": market}
        )
        return pd.Timestamp(date) + pd.Timedelta(days=n)

    def get_ex_factor(self, order_book_ids, start_date=None, end_date=None, market="cn"):
        self.get_ex_factor_calls.append(
            {
                "order_book_ids": order_book_ids,
                "start_date": start_date,
                "end_date": end_date,
                "market": market,
            }
        )
        # 返回与 order_book_ids 匹配的数据
        # 注意：RQDataProvider.get_ex_factor 调用 rq.get_ex_factor() 获取原始数据
        # 模拟 rqdatac 返回的格式（带索引的 DataFrame）
        if isinstance(order_book_ids, str):
            order_book_ids = [order_book_ids]
        rows = []
        for oid in order_book_ids:
            rows.append({
                "ex_date": pd.Timestamp("2024-01-01"),
                "announcement_date": pd.Timestamp("2024-01-01"),
                "ex_cum_factor": 1.0,
                "ex_factor": 1.0,
                "ex_end_date": pd.Timestamp("2024-12-31"),
            })
        return pd.DataFrame(rows, index=pd.Index(order_book_ids, name="order_book_id"))

    def is_suspended(self, order_book_ids, start_date=None, end_date=None, market="cn"):
        self.is_suspended_calls.append(
            {
                "order_book_ids": order_book_ids,
                "start_date": start_date,
                "end_date": end_date,
                "market": market,
            }
        )
        # 返回与 order_book_ids 匹配的数据
        dates = pd.bdate_range(start=start_date, end=end_date)
        data = {d: {oid: False for oid in order_book_ids} for d in dates}
        return pd.DataFrame(data)


def _provider_with_dummy_rq(monkeypatch):
    """创建带有模拟 rqdatac 的 provider。"""
    provider = RQDataProvider({"cache_dir": None})
    dummy_rq = DummyRQDataModule()
    provider._rq = dummy_rq
    return provider, dummy_rq


@pytest.mark.unit
class TestRQDataProviderFormatDate:
    """测试 _format_date 方法。"""

    def test_format_date_none(self):
        provider = RQDataProvider()
        assert provider._format_date(None) is None

    def test_format_date_string_iso(self):
        provider = RQDataProvider()
        result = provider._format_date("2024-01-02")
        assert result == pd.Timestamp("2024-01-02")

    def test_format_date_string_compact(self):
        provider = RQDataProvider()
        result = provider._format_date("20240102")
        assert result == pd.Timestamp("2024-01-02")

    def test_format_date_date_object(self):
        provider = RQDataProvider()
        result = provider._format_date(dt.date(2024, 1, 2))
        assert result == pd.Timestamp("2024-01-02")

    def test_format_date_datetime_object(self):
        provider = RQDataProvider()
        result = provider._format_date(dt.datetime(2024, 1, 2, 10, 30))
        assert result == pd.Timestamp("2024-01-02 10:30:00")

    def test_format_date_timestamp(self):
        provider = RQDataProvider()
        ts = pd.Timestamp("2024-01-02 10:30:00")
        result = provider._format_date(ts)
        assert result == ts


@pytest.mark.unit
class TestRQDataProviderNormalizeFrequency:
    """测试 _normalize_frequency 方法。"""

    def test_daily_to_1d(self):
        provider = RQDataProvider()
        assert provider._normalize_frequency("daily") == "1d"
        assert provider._normalize_frequency("1d") == "1d"
        assert provider._normalize_frequency("d") == "1d"

    def test_minute_to_1m(self):
        provider = RQDataProvider()
        assert provider._normalize_frequency("minute") == "1m"
        assert provider._normalize_frequency("1m") == "1m"
        assert provider._normalize_frequency("m1") == "1m"

    def test_other_frequencies(self):
        provider = RQDataProvider()
        assert provider._normalize_frequency("5m") == "5m"
        assert provider._normalize_frequency("30m") == "30m"
        assert provider._normalize_frequency("1w") == "1w"


@pytest.mark.unit
class TestRQDataProviderTranslateFields:
    """测试 _translate_fields_to_rq 方法。"""

    def test_translate_money_to_total_turnover(self):
        result = RQDataProvider._translate_fields_to_rq(["money", "close"])
        assert "total_turnover" in result
        assert "close" in result

    def test_translate_limit_fields(self):
        result = RQDataProvider._translate_fields_to_rq(["high_limit", "low_limit"])
        assert "limit_up" in result
        assert "limit_down" in result

    def test_translate_pre_close(self):
        result = RQDataProvider._translate_fields_to_rq(["pre_close"])
        assert "prev_close" in result

    def test_extra_fields_not_translated(self):
        result = RQDataProvider._translate_fields_to_rq(["factor", "avg", "paused", "close"])
        assert result == ["close"]

    def test_none_fields_returns_none(self):
        assert RQDataProvider._translate_fields_to_rq(None) is None

    def test_empty_extra_fields_returns_none(self):
        assert RQDataProvider._translate_fields_to_rq(["factor", "avg"]) is None


@pytest.mark.unit
class TestRQDataProviderIsStock:
    """测试 _is_stock 方法。"""

    def test_sz_stock(self):
        assert RQDataProvider._is_stock("000001.XSHE") is True

    def test_sh_stock(self):
        assert RQDataProvider._is_stock("600000.XSHG") is True

    def test_gem_stock(self):
        assert RQDataProvider._is_stock("300001.XSHE") is True

    def test_etf(self):
        assert RQDataProvider._is_stock("159001.XSHE") is False

    def test_sh_index(self):
        # 000001.XSHG 是上证指数，以 0 开头但后缀是 XSHG
        # _is_stock 只检查前缀，所以这个被认为是股票（这是已知行为）
        assert RQDataProvider._is_stock("000001.XSHG") is True

    def test_sz_index(self):
        # 399001.XSHE 是深证成指，以 3 开头
        assert RQDataProvider._is_stock("399001.XSHE") is True


@pytest.mark.unit
class TestRQDataProviderGetPrice:
    """测试 get_price 方法。"""

    def test_get_price_single_security(self, monkeypatch):
        provider, dummy_rq = _provider_with_dummy_rq(monkeypatch)

        df = provider.get_price(
            "000001.XSHE",
            start_date="2024-01-01",
            end_date="2024-01-02",
            fields=["open", "close"],
        )

        assert not df.empty
        assert "open" in df.columns
        assert "close" in df.columns

    def test_get_price_multiple_securities(self, monkeypatch):
        provider, dummy_rq = _provider_with_dummy_rq(monkeypatch)

        df = provider.get_price(
            ["000001.XSHE", "600000.XSHG"],
            start_date="2024-01-01",
            end_date="2024-01-02",
            fields=["close"],
        )

        # 多证券返回平铺格式
        assert not df.empty

    def test_get_price_with_count_daily(self, monkeypatch):
        provider, dummy_rq = _provider_with_dummy_rq(monkeypatch)

        df = provider.get_price(
            "000001.XSHE",
            end_date="2024-01-10",
            count=5,
            fields=["close"],
        )

        # 验证调用了 get_trading_dates
        assert len(dummy_rq.get_trading_dates_calls) > 0

    def test_get_price_with_count_minute(self, monkeypatch):
        provider, dummy_rq = _provider_with_dummy_rq(monkeypatch)

        df = provider.get_price(
            "000001.XSHE",
            end_date="2024-01-10",
            count=100,
            frequency="1m",
            fields=["close"],
        )

        # 验证调用了 get_previous_trading_date（用于计算 start_date）
        assert len(dummy_rq.get_previous_trading_date_calls) > 0
        # 验证 n 参数正确（100 分钟 / 240 = 1 天，向上取整）
        call = dummy_rq.get_previous_trading_date_calls[-1]
        assert call["n"] == 1

    def test_get_price_with_count_5m(self, monkeypatch):
        provider, dummy_rq = _provider_with_dummy_rq(monkeypatch)

        df = provider.get_price(
            "000001.XSHE",
            end_date="2024-01-10",
            count=100,
            frequency="5m",
            fields=["close"],
        )

        # 验证调用了 get_previous_trading_date（用于计算 start_date）
        assert len(dummy_rq.get_previous_trading_date_calls) > 0
        # 100 * 5 = 500 分钟 / 240 = 3 天（向上取整）
        call = dummy_rq.get_previous_trading_date_calls[-1]
        assert call["n"] == 3

    def test_get_price_raises_if_both_start_date_and_count(self, monkeypatch):
        provider, dummy_rq = _provider_with_dummy_rq(monkeypatch)

        with pytest.raises(ValueError, match="不能同时指定 start_date 和 count"):
            provider.get_price(
                "000001.XSHE",
                start_date="2024-01-01",
                end_date="2024-01-10",
                count=5,
            )

    def test_get_price_money_field_mapped(self, monkeypatch):
        provider, dummy_rq = _provider_with_dummy_rq(monkeypatch)

        df = provider.get_price(
            "000001.XSHE",
            start_date="2024-01-01",
            end_date="2024-01-02",
            fields=["money"],
        )

        # 验证 money 被映射为 total_turnover
        call = dummy_rq.get_price_calls[-1]
        assert "total_turnover" in call.get("fields", [])

    def test_get_price_extra_fields_not_sent_to_rq(self, monkeypatch):
        provider, dummy_rq = _provider_with_dummy_rq(monkeypatch)

        # 使用 _translate_fields_to_rq 验证额外字段不传给 rqdatac
        result = RQDataProvider._translate_fields_to_rq(["close", "factor", "avg", "paused"])
        assert "factor" not in result
        assert "avg" not in result
        assert "paused" not in result
        assert "close" in result


@pytest.mark.unit
class TestRQDataProviderGetTradeDays:
    """测试 get_trade_days 方法。"""

    def test_get_trade_days_with_start_and_end(self, monkeypatch):
        provider, dummy_rq = _provider_with_dummy_rq(monkeypatch)

        days = provider.get_trade_days(
            start_date="2024-01-01",
            end_date="2024-01-10",
        )

        assert len(days) > 0
        assert all(isinstance(d, dt.datetime) for d in days)

    def test_get_trade_days_with_count(self, monkeypatch):
        provider, dummy_rq = _provider_with_dummy_rq(monkeypatch)

        days = provider.get_trade_days(
            end_date="2024-01-10",
            count=5,
        )

        # get_previous_trading_date 返回 date - n 天，然后 get_trading_dates 返回该范围内的交易日
        # 由于模拟实现简单，实际返回的数量可能不等于 count
        assert len(days) > 0

    def test_get_trade_days_empty_if_no_dates(self, monkeypatch):
        provider, dummy_rq = _provider_with_dummy_rq(monkeypatch)

        days = provider.get_trade_days()
        assert days == []


@pytest.mark.unit
class TestRQDataProviderAuth:
    """测试认证相关。"""

    def test_auth_with_license(self, monkeypatch):
        provider = RQDataProvider({"license": "test_license"})
        monkeypatch.setenv("RQDATA_LICENSE", "test_license")

        # 不应该抛出异常
        # provider.auth()  # 实际调用需要 rqdatac 模块

    def test_auth_raises_if_no_credentials(self, monkeypatch):
        provider = RQDataProvider()
        # 清除所有可能的环境变量
        for key in ["RQDATA_USERNAME", "RQDATA_USER", "RQDATA_PASSWORD", "RQDATA_PWD", "RQDATA_LICENSE"]:
            monkeypatch.delenv(key, raising=False)

        # 清除 provider 中的配置
        provider._username = ""
        provider._password = ""
        provider._license = ""

        with pytest.raises(RuntimeError, match="RQData 账号未配置"):
            provider.auth()


@pytest.mark.unit
class TestRQDataProviderPostprocessExtraFields:
    """测试 _postprocess_extra_fields 方法。"""

    def test_returns_original_df_if_no_extra_fields(self, monkeypatch):
        provider, dummy_rq = _provider_with_dummy_rq(monkeypatch)

        df = pd.DataFrame({"close": [10.0]})
        result = provider._postprocess_extra_fields(
            df, [], ["000001.XSHE"], "2024-01-01", "2024-01-02", "daily", False, "pre"
        )

        assert result is df  # 应该返回原始 df

    def test_returns_copy_with_extra_fields(self, monkeypatch):
        provider, dummy_rq = _provider_with_dummy_rq(monkeypatch)

        # _postprocess_extra_fields 内部 merge 使用 on=["code", "time"]
        df = pd.DataFrame(
            {"close": [10.0]},
            index=pd.MultiIndex.from_tuples(
                [("000001.XSHE", pd.Timestamp("2024-01-02"))],
                names=["code", "time"],
            ),
        )

        result = provider._postprocess_extra_fields(
            df, ["factor"], ["000001.XSHE"], "2024-01-01", "2024-01-02", "daily", False, "pre"
        )

        assert result is not df  # 应该返回新 df
        assert "factor" in result.columns
