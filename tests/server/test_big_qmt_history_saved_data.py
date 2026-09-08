"""真实 408 日 OHLC 经大 QMT 适配器的离线存档重放回归。

作者：BruceLee
职责：将已保存原始价和现金事件送入真实 BigQmtDataAdapter，检查完整时间轴、
不复权、前复权及重叠窗口；保留股票零差和 ETF 九格残差，不改变验收容差。
输入：big_qmt_history_facts_20260908.json 的真实418日全字段、元数据和35条事件，
以及 qmt_adjustment_20260908.json 的408日OHLC金标；不合成量额、停牌或上市日期。
重要边界：日历使用存档行情日期重放，不声称远端日历接口实测；本文件只验真实 OHLC，
不作量额、停牌、昨收的跨源相等或后复权全历史验收。
输出：pytest 断言；ETF 残差快照通过仅代表兼容性限制未变，不代表与聚宽逐格一致。
上下游：真实 BigQmtDataAdapter → 白名单内存假 Gateway；复用实际 wire 编解码。
环境：仅本地 JSON、pandas、pytest，不连接客户端，不导入策略，不写外部状态。
"""

from __future__ import annotations

import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from bullet_trade.data.providers.remote_qmt import _dataframe_from_payload
from bullet_trade.server.adapters.big_qmt import BigQmtDataAdapter, BigQmtGatewayConfig
from bullet_trade.server.adapters.qmt import dataframe_to_payload

pytestmark = pytest.mark.unit

_FIXTURE_PATH = Path(__file__).parents[1] / "fixtures" / "qmt_adjustment_20260908.json"
_FACTS_PATH = Path(__file__).parents[1] / "fixtures" / "big_qmt_history_facts_20260908.json"
_SECURITIES = ("000001.XSHE", "510500.XSHG")
_PRICE_FIELDS = ["open", "high", "low", "close"]


@pytest.fixture(scope="module")
def saved_prices():
    """读取既有真实价格金标；无输入，返回独立字典，仅读取同仓固定文件。"""
    return json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def saved_facts():
    """读取真实 QMT 全字段存档；无输入，返回字典，仅读取固定文件，不补造任何字段。"""
    return json.loads(_FACTS_PATH.read_text(encoding="utf-8"))


def _case(saved_prices, security):
    """选取证券金标；输入完整样本与代码，返回唯一案例，缺失或重复以断言失败。"""
    matches = [item for item in saved_prices["cases"] if item["security"] == security]
    assert len(matches) == 1
    return matches[0]


def _prices(case, key):
    """还原真实 OHLC；输入案例和行集名称，返回 time 索引新表，不补造价格或日期。"""
    frame = pd.DataFrame(case[key], columns=["time", *_PRICE_FIELDS])
    frame["time"] = pd.to_datetime(frame["time"])
    return frame.set_index("time").astype(float)


def _window(payload):
    """解析假网关窗口；输入载荷，返回左右边界，纯日期结束覆盖全天，无副作用。"""
    start = payload.get("start", payload.get("start_date"))
    end = payload.get("end", payload.get("end_date"))
    lower = pd.Timestamp(start) if start else None
    upper = pd.Timestamp(end) if end else None
    if upper is not None and len(str(end)) in (8, 10):
        upper += pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    return lower, upper


class _SavedPriceGateway:
    """重放真实行情、元数据及事件；仅持有内存存档和调用记录，禁止非白名单动作。"""

    def __init__(self, saved_facts):
        """装入固定样本；输入全字段存档，返回 None，仅初始化内存表及调用列表。"""
        self.config = BigQmtGatewayConfig()
        self.calls = []
        self.cases = {security: _case(saved_facts, security) for security in _SECURITIES}
        self.frames = {}
        for security, case in self.cases.items():
            source = case["raw"]
            frame = pd.DataFrame(source["records"], columns=source["columns"]).set_index("stime")
            frame.index = pd.to_datetime(frame.index, format="%Y%m%d").rename("time")
            assert len(frame) == 418 and frame.index.is_unique
            self.frames[security] = frame

    async def post(self, path, payload=None, *, timeout_seconds=None):
        """回应隔离事实请求；输入路径、载荷和超时，返回新 wire，非法动作断言失败。"""
        assert path in {
            "/data/history",
            "/data/split_dividend",
            "/data/trade_days",
            "/data/security_info",
        }, f"本回归禁止调用非行情动作: {path}"
        payload = payload or {}
        self.calls.append((path, deepcopy(payload)))
        aliases = {"000001.SZ": "000001.XSHE", "510500.SH": "510500.XSHG"}
        security = aliases.get(payload.get("security"), payload.get("security"))
        lower, upper = _window(payload)
        if path == "/data/trade_days":
            # 只重放存档交易日期，不用工作日日历补造交易日，也不声称远端日历已验证。
            days = self.frames[_SECURITIES[0]].index
            if lower is not None:
                days = days[days >= lower.normalize()]
            if upper is not None:
                days = days[days <= upper]
            count = payload.get("count")
            if count is not None and count > 0:
                days = days[-count:]
            return {"dtype": "list", "values": days.strftime("%Y-%m-%d").tolist()}
        assert security in self.cases
        case = self.cases[security]
        if path == "/data/security_info":
            return deepcopy(case["info"])
        if path == "/data/split_dividend":
            response = deepcopy(case["events"])
            events = response["events"]
            if lower is not None:
                events = [
                    event for event in events if pd.Timestamp(event["date"]) >= lower.normalize()
                ]
            if upper is not None:
                events = [event for event in events if pd.Timestamp(event["date"]) <= upper]
            response["events"] = events
            return response
        assert payload.get("frequency") == "1d", "真实金标只有日线，不得假装分钟实测"
        assert payload.get("fq") == "none", "自算只能读取原价，不能使用 JQ 或 QMT 原生复权价"
        assert payload.get("subscribe") is False
        assert payload.get("fill_data") is False
        assert lower is not None and upper is not None
        frame = self.frames[security]
        frame = frame.loc[(frame.index >= lower) & (frame.index <= upper)].copy()
        count = payload.get("count")
        if count is not None and count > 0:
            frame = frame.tail(count)
        frame = frame.loc[:, payload.get("fields") or frame.columns.tolist()]
        frame.index = frame.index.strftime("%Y%m%d")
        frame.index.name = "stime"
        return dataframe_to_payload(frame)


async def _request(adapter, saved_prices, security, *, fq, **overrides):
    """通过真实适配器读取价格；输入对象、金标、证券及窗口，返回解码表，副作用仅假调用。"""
    payload = {
        "security": security,
        "start": "2025-01-01",
        "end": saved_prices["reference_date"],
        "frequency": "1d",
        "fields": _PRICE_FIELDS,
        "fq": fq,
        "panel": False,
        "skip_paused": False,
        "fill_paused": True,
        "pre_factor_ref_date": saved_prices["reference_date"] if fq == "pre" else None,
    }
    payload.update(overrides)
    wire = await adapter.get_history(payload)
    return _dataframe_from_payload(wire)


def _differences(actual, expected):
    """对齐后列出逐格差异；输入完整真实价格表，返回差异列表，不用交集掩盖缺行。"""
    pd.testing.assert_index_equal(actual.index, expected.index)
    pd.testing.assert_index_equal(actual.columns, expected.columns)
    differences = []
    for stamp in actual.index:
        for field in _PRICE_FIELDS:
            candidate = Decimal(str(actual.at[stamp, field]))
            golden = Decimal(str(expected.at[stamp, field]))
            if candidate != golden:
                differences.append(
                    {
                        "date": stamp.strftime("%Y-%m-%d"),
                        "field": field,
                        "candidate": candidate,
                        "jq": golden,
                        "delta": candidate - golden,
                    }
                )
    return differences


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
async def test_adapter_saved_none_preserves_all_408_days(saved_prices, saved_facts, security):
    """真实原价经适配器不变；输入金标与证券，断言408日1632格OHLC，无真实辅助字段验收。"""
    gateway = _SavedPriceGateway(saved_facts)
    original = gateway.frames[security].copy(deep=True)
    actual = await _request(BigQmtDataAdapter(gateway), saved_prices, security, fq="none")
    expected = _prices(_case(saved_prices, security), "raw_rows")
    assert actual.shape == (408, 4)
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    pd.testing.assert_frame_equal(gateway.frames[security], original, check_exact=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
async def test_adapter_saved_pre_retains_stock_exact_and_etf_residual(
    saved_prices, saved_facts, security
):
    """真实前复权价格闭环；输入金标与证券，股票零差/ETF九差须保持，不将残差当通过标准。"""
    gateway = _SavedPriceGateway(saved_facts)
    case = _case(saved_prices, security)
    actual = await _request(BigQmtDataAdapter(gateway), saved_prices, security, fq="pre")
    expected = _prices(case, "jq_pre_rows")
    assert actual.shape == expected.shape == (408, 4)
    differences = _differences(actual, expected)
    expected_differences = [
        {**entry, **{key: Decimal(entry[key]) for key in ("candidate", "jq", "delta")}}
        for entry in case["diagnostic_expected_differences"]
    ]
    assert differences == expected_differences
    if security == "000001.XSHE":
        assert not differences
    else:
        assert len(differences) == 9
        assert sum(entry["field"] == "close" for entry in differences) == 2
        assert max(abs(entry["delta"]) for entry in differences) == Decimal("0.001")
    assert any(path == "/data/split_dividend" for path, _ in gateway.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
@pytest.mark.parametrize("fq", ["none", "pre"])
async def test_adapter_saved_overlap_is_independent_of_query_window(
    saved_prices, saved_facts, security, fq
):
    """同一基准下真实重叠窗口不漂移；输入金标、证券、模式，完整索引比较start与count结果。"""
    gateway = _SavedPriceGateway(saved_facts)
    adapter = BigQmtDataAdapter(gateway)
    full = await _request(adapter, saved_prices, security, fq=fq)
    window = await _request(
        adapter, saved_prices, security, fq=fq, start="2025-07-01", end="2025-08-29"
    )
    expected = full.loc["2025-07-01":"2025-08-29"]
    pd.testing.assert_frame_equal(window, expected, check_exact=True)
    counted = await _request(adapter, saved_prices, security, fq=fq, start=None, count=40)
    pd.testing.assert_frame_equal(counted, full.tail(40), check_exact=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
@pytest.mark.parametrize(
    "frequency,end,period_start,period_end",
    [
        ("1mon", "2026-05-03", "2026-04-01", "2026-04-30"),
        ("1w", "2026-05-04", "2026-04-27", "2026-04-30"),
    ],
)
async def test_adapter_saved_calendar_count_after_holiday_uses_whole_previous_period(
    saved_prices, saved_facts, security, frequency, end, period_start, period_end
):
    """新周月未有交易日时回取完整旧组；输入真实存档和周期边界，断言OHLC/量额，无外部调用。"""
    gateway = _SavedPriceGateway(saved_facts)
    source = gateway.frames[security]
    # 仅使用真实返回的交易日期，确认窗口处于新周/月开始后的休市段。
    assert source.loc[pd.Timestamp(period_end) + pd.Timedelta(days=1) : end].empty
    period = source.loc[period_start:period_end]
    assert len(period) > 1
    fields = [*_PRICE_FIELDS, "volume", "money"]
    expected = pd.DataFrame(
        {
            "open": [float(period["open"].iloc[0])],
            "high": [float(period["high"].max())],
            "low": [float(period["low"].min())],
            "close": [float(period["close"].iloc[-1])],
            # helper日线量为手，对外单位为股/份；money保持真实元值相加。
            "volume": [float((period["volume"] * 100.0).sum())],
            "money": [float(period["money"].sum())],
        },
        index=pd.DatetimeIndex([period.index[-1]], name="time"),
    )
    actual = await _request(
        BigQmtDataAdapter(gateway),
        saved_prices,
        security,
        fq="none",
        start=None,
        end=end,
        frequency=frequency,
        count=1,
        fields=fields,
    )
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
