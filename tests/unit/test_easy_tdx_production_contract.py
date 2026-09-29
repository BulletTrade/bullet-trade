"""easy_tdx 生产观察前的独立预期合同测试。

作者: BruceLee
日期: 2026-09-29
职责: 以固定公司行为与日期验证数据源，不把适配器自身输出当期望值。
输入: SDK 已归一到每股的事件、固定原始 K 线和可控网络故障。
输出: pytest 断言；失败表示不能据此验收生产 Message 观察。
上下游: EasyTdxProvider 的公开价格/日历接口；无外部行情、账户或数据库连接。
环境: 注入假客户端，禁止 stub；现金与送配比例均按 easy-tdx 1.14.4 语义。
"""

from datetime import datetime
import json
from pathlib import Path

import pandas as pd
import pytest

from bullet_trade.data.providers.easy_tdx import EasyTdxProvider

pytestmark = pytest.mark.unit


class FixedBarsClient:
    """模拟 SDK 从最新数据向前取 count 条的语义；维护固定日线及请求记录。"""

    def __init__(self):
        """建立三日原始行情；无参数，无返回，不访问网络。"""
        self.calls = []
        self.bars = pd.DataFrame(
            {
                "datetime": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
                "open": [10.0, 9.0, 9.5],
                "high": [10.0, 9.0, 9.5],
                "low": [10.0, 9.0, 9.5],
                "close": [10.0, 9.0, 9.5],
                "vol": [1000.0, 1000.0, 1000.0],
                "amount": [10000.0, 9000.0, 9500.0],
            }
        )

    def get_stock_kline(self, **kwargs):
        """按输入 count 返回最新原始行；返回 DataFrame，并记录请求参数。"""
        self.calls.append(kwargs)
        assert kwargs["adjust"] == 0, "此 fixture 只提供独立未复权价格"
        return self.bars.tail(kwargs["count"]).copy()


class CashEventClient:
    """提供已归一为每股派息 1 元的固定事件；无连接状态或外部依赖。"""

    def __init__(self, **kwargs):
        """接受并忽略连接参数；无返回，无网络副作用。"""

    def connect(self):
        """模拟连接成功；无参数，无返回，无网络副作用。"""

    def close(self):
        """模拟关闭；无参数，无返回，无网络副作用。"""

    def get_xdxr_info(self, market, code):
        """接收市场与代码，返回固定每股现金事件 DataFrame，无副作用。"""
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


class FailingEventsClient(CashEventClient):
    """模拟除权服务器失联，供测试区分无公司行为与读取失败。"""

    def connect(self):
        """无参数；固定抛连接错误，无返回，无真实网络副作用。"""
        raise ConnectionError("fixture: xdxr unavailable")


@pytest.fixture
def provider(monkeypatch):
    """接收 monkeypatch 清除 stub 覆盖，返回注入固定客户端的 provider。"""
    monkeypatch.delenv("EASY_TDX_USE_STUB", raising=False)
    return EasyTdxProvider({"client": FixedBarsClient(), "tdx_client_cls": CashEventClient})


@pytest.mark.parametrize(
    "security,expected",
    [
        ("000300.XSHG", 100000),
        ("000300.SH", 100000),
        ("399001.XSHE", 100000),
        ("399001.SZ", 100000),
        ("000001.XSHE", 1000),
        ("510300.XSHG", 1000),
    ],
)
def test_kline_volume_units(provider, security, expected):
    """输入固定行情、证券及独立单位预期，断言指数换算不影响股票和基金；无网络。"""
    result = provider.get_price(
        security, count=1, end_date="2024-01-04", fq=None, fields=["volume"]
    )
    assert result.iloc[-1]["volume"] == expected


def test_events_follow_selected_quote_host(provider):
    """输入已优选的行情地址，断言除权接口复用它且不进入SDK默认IP与无限重连。"""
    captured = {}

    class Events(CashEventClient):
        """只记录连接参数并提供固定除权事件，无网络。"""

        def __init__(self, **kwargs):
            """输入连接参数，保存供断言，无返回或网络副作用。"""
            captured.update(kwargs)

    provider._client._host = "fixture-selected-host"
    provider._tdx_client_cls = Events
    assert len(provider._fetch_xdxr_events("159915.SZ")) == 1
    assert captured["host"] == "fixture-selected-host"
    assert captured["auto_reconnect"] is False


def test_event_protocol_selects_own_hosts_and_retries_once(provider, monkeypatch):
    """输入PC协议候选和一次连接失败，验证独立择优、排除坏地址及成功地址复用。"""
    from easy_tdx import config
    from easy_tdx.exceptions import TdxConnectionError

    monkeypatch.setattr(config, "get_known_hosts", lambda: ["bad-pc", "good-pc"])
    selections = []

    class Events(CashEventClient):
        """模拟PC协议主机与除权结果，保存所选地址。"""

        def __init__(self, host=None, **kwargs):
            """输入地址和连接选项，保存地址；无返回、无网络。"""
            self._host = host

        @classmethod
        def from_best_host(cls, **kwargs):
            """输入候选列表，记录后返回第一候选连接，无网络。"""
            selections.append(kwargs["hosts"])
            return cls(host=kwargs["hosts"][0])

        def connect(self):
            """无输入，坏候选抛SDK连接错误，其他成功；无网络。"""
            if self._host == "bad-pc":
                raise TdxConnectionError("fixture")

    provider._client._host = "mac-only"
    provider._tdx_client_cls = Events
    assert len(provider._fetch_xdxr_events("159915.SZ")) == 1
    assert len(provider._fetch_xdxr_events("159915.SZ")) == 1
    assert selections == [["bad-pc", "good-pc"], ["good-pc"]]
    assert provider._xdxr_host == "good-pc"


@pytest.mark.parametrize(
    "cash,bonus,rights,rights_price,expected",
    [
        (1.0, 0.0, 0.0, 0.0, 0.9),
        (0.0, 0.5, 0.0, 0.0, 2.0 / 3.0),
        (0.0, 0.0, 0.3, 5.0, 11.5 / 13.0),
        (1.0, 0.5, 0.3, 5.0, 10.5 / 18.0),
    ],
    ids=["cash_per_share", "bonus_per_share", "rights_per_share", "mixed_per_share"],
)
def test_company_action_units(provider, cash, bonus, rights, rights_price, expected):
    """接收每股事件与独立理论乘数，断言除权公式；无返回、外部副作用。"""
    raw = pd.DataFrame({"close": [10.0]}, index=pd.to_datetime(["2024-01-02"]))
    event = pd.Series(
        {
            "date": pd.Timestamp("2024-01-03"),
            "fenhong": cash,
            "songzhuangu": bonus,
            "peigu": rights,
            "peigujia": rights_price,
        }
    )
    assert provider._event_adjust_factor(event, raw) == pytest.approx(expected)


def test_reference_after_query_window_applies_dividend(provider):
    """输入查询末日之后的参考日，期望历史 10 元锚定为 9 元；无外部副作用。"""
    result = provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02",
        end_date="2024-01-02",
        fields=["close"],
        fq="pre",
        pre_factor_ref_date="2024-01-03",
    )
    assert result["close"].tolist() == pytest.approx([9.0])


def test_raw_price_is_unchanged_when_factor_requested(provider):
    """输入未复权与 factor 组合，期望 close 仍为原始 10/9 元；无外部副作用。"""
    result = provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02",
        end_date="2024-01-03",
        fields=["close", "factor"],
        fq=None,
    )
    assert result["close"].tolist() == pytest.approx([10.0, 9.0])
    assert result["factor"].tolist() == [1.0, 1.0]


def test_event_failure_is_not_no_dividend(provider):
    """输入事件查询失败，期望显式异常；不能伪装无分红，测试无网络。"""
    provider._tdx_client_cls = FailingEventsClient
    with pytest.raises((ConnectionError, RuntimeError), match="xdxr|除权|复权"):
        provider.get_price(
            "000001.XSHE",
            start_date="2024-01-02",
            end_date="2024-01-03",
            fields=["close", "factor"],
            fq="pre",
        )


def test_history_count_is_relative_to_end_date(provider):
    """输入旧截止日与 count=1，期望返回该日前最后一行；无外部副作用。"""
    result = provider.get_price(
        "000001.XSHE",
        end_date="2024-01-02",
        count=1,
        fields=["close"],
        fq=None,
    )
    assert list(result.index) == [pd.Timestamp("2024-01-02")]
    assert result["close"].tolist() == pytest.approx([10.0])


def test_trade_day_count_is_relative_to_end_date(provider):
    """输入旧截止日与交易日 count=1，期望准确一日；无返回、外部副作用。"""
    result = provider.get_trade_days(end_date="2024-01-02", count=1)
    assert result == [datetime(2024, 1, 2)]


def test_reference_inside_query_window(provider):
    """参考日包含于查询区间时现金前复权保持既有正确行为；无外部副作用。"""
    result = provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02",
        end_date="2024-01-03",
        fields=["close"],
        fq="pre",
        pre_factor_ref_date="2024-01-03",
    )
    assert result["close"].tolist() == pytest.approx([9.0, 9.0])


@pytest.mark.parametrize("security", ["000001.XSHE", "510500.XSHG"])
def test_reuse_saved_qmt_dividend_prices(provider, monkeypatch, security):
    """复用大QMT已有408日真实原价、分红与聚宽金标；输入固定证券，无外部副作用。"""
    fixture = json.loads(
        (Path(__file__).parents[1] / "fixtures/qmt_adjustment_20260908.json").read_text()
    )
    case = next(x for x in fixture["cases"] if x["security"] == security)
    columns = ["datetime", "open", "high", "low", "close"]
    provider._client.bars = pd.DataFrame(case["raw_rows"], columns=columns)
    provider._client.bars["datetime"] = pd.to_datetime(provider._client.bars["datetime"])
    events = pd.DataFrame(
        [
            {
                "date": pd.Timestamp(x["date"]),
                "category": 1,
                "fenhong": float(x["cash_per_share"]),
                "songzhuangu": 0.0,
                "peigu": 0.0,
                "peigujia": 0.0,
            }
            for x in case["events"]
        ]
    )
    monkeypatch.setattr(provider, "_fetch_xdxr_events", lambda security: events.copy())
    actual = provider.get_price(
        security,
        start_date="2025-01-02",
        end_date="2026-09-07",
        fields=columns[1:],
        fq="pre",
        pre_factor_ref_date=fixture["reference_date"],
    )
    expected = pd.DataFrame(case["jq_pre_rows"], columns=columns)
    expected["datetime"] = pd.to_datetime(expected["datetime"])
    expected = expected.set_index("datetime").astype(float)
    pd.testing.assert_index_equal(actual.index, expected.index, check_names=False)
    # ETF 金标原有 9 格 0.001 残差，沿用一跳上限，股票仍逐格精确验收。
    tolerance = 0.001000000001 if security == "510500.XSHG" else 1e-12
    assert len(actual) == 408
    assert (actual - expected).abs().to_numpy().max() <= tolerance


def test_real_quote_time_is_preserved(provider, monkeypatch):
    """输入带服务器时间的报价，断言源时间来自SDK而非本机；无外部副作用。"""

    def quote(*args, **kwargs):
        """接收SDK参数返回固定真实格式报价；不访问网络。"""
        return pd.DataFrame(
            [
                {
                    "close": 10.0,
                    "server_update_date": 20260929,
                    "server_update_time": 103005,
                }
            ]
        )

    monkeypatch.setattr(provider._client, "get_stock_quotes", quote, raising=False)
    result = provider.get_live_current("000001.XSHE")
    assert result["source_time"] == "2026-09-29T10:30:05"
    assert "received_time" in result and "age_seconds" in result


def test_missing_server_time_is_not_fabricated(provider, monkeypatch):
    """输入没有源时间的报价，期望不生成 source_time；返回断言，无网络。"""

    def quote(*args, **kwargs):
        """忽略SDK参数返回缺时间报价；无副作用。"""
        return pd.DataFrame([{"close": 10.0}])

    monkeypatch.setattr(provider._client, "get_stock_quotes", quote, raising=False)
    assert "source_time" not in provider.get_live_current("000001.XSHE")


def test_daily_host_reselection_and_bounded_failover(monkeypatch):
    """用假主机模拟跨日和断线；验证每次最多重选一次，不依赖网络。"""

    class Client:
        """维护选主机计数与可控失败标志的假客户端。"""

        selected = 0

        @classmethod
        def from_best_host(cls, **kwargs):
            """记录每次选择并返回新客户端；输入连接参数，无外部副作用。"""
            cls.selected += 1
            return cls()

        def connect(self):
            """无输入无返回，模拟连接成功。"""

        def close(self):
            """无输入无返回，模拟连接关闭。"""

        def query(self):
            """无输入；第二次选中的连接报网络错，其他返回常量。"""
            if self.selected == 2:
                raise ConnectionError("fixture disconnect")
            return 42

    monkeypatch.setattr(EasyTdxProvider, "_ensure_config_dir", staticmethod(lambda: None))
    p = EasyTdxProvider({"mac_client_cls": Client})
    p.auth()
    p._selected_day = datetime(2000, 1, 1).date()
    assert p._call("query") == 42
    assert Client.selected == 3


@pytest.mark.parametrize(
    "security,value,expected",
    [
        ("513100.XSHG", 2.3340001106262207, 2.334),
        ("588000.SH", 1.6430000066757202, 1.643),
        ("561300.XSHG", 1.1230000257492065, 1.123),
        ("000001.XSHE", 10.100000381469727, 10.10),
    ],
)
def test_raw_prices_respect_quote_precision(provider, security, value, expected):
    """输入SDK单精度价格与独立报价精度预期，验证原始价消除尾差；无外部副作用。"""
    provider._client.bars["close"] = value
    result = provider.get_price(
        security,
        start_date="2024-01-02",
        end_date="2024-01-04",
        fields=["close"],
        fq=None,
    )
    assert result.close.tolist() == [expected] * 3


def test_factor_matches_applied_reference_ratio(provider):
    """输入参考日位于事件前的查询，factor应为实际价格乘数，不能返回未锚定累计值。"""
    result = provider.get_price(
        "000001.XSHE",
        start_date="2024-01-02",
        end_date="2024-01-03",
        fields=["close", "factor"],
        fq="pre",
        pre_factor_ref_date="2024-01-02",
    )
    assert result.factor.tolist() == pytest.approx([1.0, 1.0 / 0.9])


def test_empty_factor_does_not_switch_algorithms(provider, monkeypatch):
    """输入复权因子缺失，期望明确失败且不调用SDK复权；无外部副作用。"""

    def no_factor(*args, **kwargs):
        """接收因子请求参数，返回空序列以模拟不完整数据；无副作用。"""
        return pd.Series(dtype=float)

    monkeypatch.setattr(provider, "_build_factor_for_index", no_factor)
    with pytest.raises(RuntimeError, match="不能静默切换"):
        provider.get_price("000001.XSHE", start_date="2024-01-02", end_date="2024-01-03")
    assert all(call["adjust"] == 0 for call in provider._client.calls)
