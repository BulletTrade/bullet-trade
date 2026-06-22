import datetime as dt

import pandas as pd
import pytest

import bullet_trade.data.providers.jqdata as jqdata
from bullet_trade.data.providers.jqdata import JQDataProvider


@pytest.mark.unit
class TestJQDataProviderAuth:
    def test_auth_prefers_new_env_names(self, monkeypatch):
        provider = JQDataProvider()
        recorded = {}

        def fake_auth(user, pwd, host=None, port=None):
            payload = {"user": user, "pwd": pwd}
            if host is not None:
                payload["host"] = host
            if port is not None:
                payload["port"] = port
            recorded.update(payload)

        monkeypatch.setattr(jqdata.jq, "auth", fake_auth)

        for legacy_key in ("JQDATA_USER", "JQDATA_PWD"):
            monkeypatch.delenv(legacy_key, raising=False)

        monkeypatch.setenv("JQDATA_USERNAME", "new_user")
        monkeypatch.setenv("JQDATA_PASSWORD", "new_pwd")
        monkeypatch.setenv("JQDATA_SERVER", "srv.test")
        monkeypatch.setenv("JQDATA_PORT", "1234")

        provider.auth()

        assert recorded == {
            "user": "new_user",
            "pwd": "new_pwd",
            "host": "srv.test",
            "port": 1234,
        }

    def test_auth_falls_back_to_legacy_env_names(self, monkeypatch):
        provider = JQDataProvider()
        recorded = {}

        def fake_auth(user, pwd, host=None, port=None):
            payload = {"user": user, "pwd": pwd}
            if host is not None:
                payload["host"] = host
            if port is not None:
                payload["port"] = port
            recorded.update(payload)

        monkeypatch.setattr(jqdata.jq, "auth", fake_auth)

        for new_key in ("JQDATA_USERNAME", "JQDATA_PASSWORD"):
            monkeypatch.delenv(new_key, raising=False)

        monkeypatch.setenv("JQDATA_USER", "legacy_user")
        monkeypatch.setenv("JQDATA_PWD", "legacy_pwd")
        monkeypatch.setenv("JQDATA_SERVER", "")
        monkeypatch.setenv("JQDATA_PORT", "")

        provider.auth()

        assert recorded == {
            "user": "legacy_user",
            "pwd": "legacy_pwd",
        }

    def test_auth_strips_inline_comments_from_port(self, monkeypatch):
        provider = JQDataProvider()
        recorded = {}

        def fake_auth(user, pwd, host=None, port=None):
            payload = {"user": user, "pwd": pwd}
            if host is not None:
                payload["host"] = host
            if port is not None:
                payload["port"] = port
            recorded.update(payload)

        monkeypatch.setattr(jqdata.jq, "auth", fake_auth)

        monkeypatch.setenv("JQDATA_USERNAME", "comment_user")
        monkeypatch.setenv("JQDATA_PASSWORD", "comment_pwd")
        monkeypatch.setenv("JQDATA_SERVER", "srv.test")
        monkeypatch.setenv("JQDATA_PORT", "8087                   # 可选：端口")

        provider.auth()

        assert recorded == {
            "user": "comment_user",
            "pwd": "comment_pwd",
            "host": "srv.test",
            "port": 8087,
        }

    def test_auth_strips_inline_comments_from_host(self, monkeypatch):
        provider = JQDataProvider()
        recorded = {}

        def fake_auth(user, pwd, host=None, port=None):
            payload = {"user": user, "pwd": pwd}
            if host is not None:
                payload["host"] = host
            if port is not None:
                payload["port"] = port
            recorded.update(payload)

        monkeypatch.setattr(jqdata.jq, "auth", fake_auth)

        monkeypatch.setenv("JQDATA_USERNAME", "host_user")
        monkeypatch.setenv("JQDATA_PASSWORD", "host_pwd")
        monkeypatch.setenv("JQDATA_SERVER", "")  # 清空，或设置带注释的测试值
        monkeypatch.setenv("JQDATA_PORT", "")  # 清空

        provider.auth()

        assert recorded == {
            "user": "host_user",
            "pwd": "host_pwd",
        }


class _DummyQuery:
    def filter(self, *args, **kwargs):
        return self


class _DummyFinance:
    """JQData 分红单测专用 finance 假对象，避免访问真实 jqdatasdk finance 模块。"""

    STK_XR_XD = jqdata._FINANCE_TABLE_STUB
    FUND_DIVIDEND = jqdata._FINANCE_TABLE_STUB
    FUND_MF_DAILY_PROFIT = jqdata._FINANCE_TABLE_STUB

    def __init__(self, df):
        """
        初始化假 finance 对象。

        Args:
            df: run_query 需要返回的 DataFrame。

        Returns:
            None。仅保存内存 DataFrame。
        """
        self._df = df

    def run_query(self, q):
        """
        返回预置查询结果。

        Args:
            q: 兼容 finance.run_query 的查询对象，测试中不使用。

        Returns:
            pd.DataFrame: 初始化时传入的分红测试数据。
        """
        _ = q
        return self._df


@pytest.mark.unit
def test_get_split_dividend_prefers_ratio_fields(monkeypatch):
    provider = JQDataProvider({"cache_dir": None})
    monkeypatch.setattr(
        JQDataProvider,
        "_resolve_security_identity",
        lambda self, security, ref_date: (security, "stock"),
    )
    monkeypatch.setattr(jqdata, "query", lambda *args, **kwargs: _DummyQuery())

    df = pd.DataFrame(
        [
            {
                "a_xr_date": dt.date(2015, 7, 27),
                "bonus_ratio_rmb": 5.0,
                "dividend_ratio": 2.0,
                "transfer_ratio": 1.0,
            }
        ]
    )
    monkeypatch.setattr(jqdata, "finance", _DummyFinance(df))

    events = provider.get_split_dividend(
        "601318.XSHG", start_date="2015-07-27", end_date="2015-07-27"
    )

    assert len(events) == 1
    event = events[0]
    assert event["bonus_pre_tax"] == pytest.approx(5.0)
    assert event["scale_factor"] == pytest.approx(1.3)
    assert event["per_base"] == 10


@pytest.mark.unit
def test_get_split_dividend_falls_back_to_share_base(monkeypatch):
    provider = JQDataProvider({"cache_dir": None})
    monkeypatch.setattr(
        JQDataProvider,
        "_resolve_security_identity",
        lambda self, security, ref_date: (security, "stock"),
    )
    monkeypatch.setattr(jqdata, "query", lambda *args, **kwargs: _DummyQuery())

    df = pd.DataFrame(
        [
            {
                "a_xr_date": dt.date(2015, 7, 27),
                "bonus_ratio_rmb": 4.0,
                "dividend_number": 30000.0,
                "transfer_number": 10000.0,
                "distributed_share_base_implement": 100000.0,
            }
        ]
    )
    monkeypatch.setattr(jqdata, "finance", _DummyFinance(df))

    events = provider.get_split_dividend(
        "601318.XSHG", start_date="2015-07-27", end_date="2015-07-27"
    )

    assert len(events) == 1
    event = events[0]
    assert event["bonus_pre_tax"] == pytest.approx(4.0)
    assert event["scale_factor"] == pytest.approx(1.4)


@pytest.mark.unit
def test_get_split_dividend_resolves_sz_etf_suffix(monkeypatch):
    """验证 JQData provider 用 .SZ 查询 ETF 时仍能读取基金拆分事件。"""
    provider = JQDataProvider({"cache_dir": None})

    def fake_get_all_securities(types="stock", date=None):
        """
        返回测试用证券清单，只在 etf 类型下暴露聚宽后缀代码。

        Args:
            types: 查询的证券类型。
            date: 查询参考日期，测试中不使用。

        Returns:
            pd.DataFrame: 指定类型的证券索引。
        """
        _ = date
        if types == "etf":
            return pd.DataFrame(index=["159967.XSHE"])
        return pd.DataFrame()

    monkeypatch.setattr(provider, "get_all_securities", fake_get_all_securities)
    monkeypatch.setattr(jqdata, "query", lambda *args, **kwargs: _DummyQuery())

    df = pd.DataFrame(
        [
            {
                "ex_date": dt.date(2020, 11, 6),
                "record_date": dt.date(2020, 11, 5),
                "proportion": 0.0,
                "split_ratio": 3.63839094,
            }
        ]
    )
    monkeypatch.setattr(jqdata, "finance", _DummyFinance(df))

    events = provider.get_split_dividend(
        "159967.SZ",
        start_date="2020-11-06",
        end_date="2020-11-06",
    )

    assert len(events) == 1
    event = events[0]
    assert event["security"] == "159967.SZ"
    assert event["security_type"] == "etf"
    assert event["scale_factor"] == pytest.approx(3.63839094)
    assert event["per_base"] == 1
