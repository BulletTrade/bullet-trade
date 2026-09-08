"""真实股票/ETF一分钟输入的大QMT适配器离线回放。

作者：BruceLee
职责：核对真实基础分钟的完整轴、竞价、量单位、现金事件前复权和窗口不变量。
输入：2026-09-08保存的两证券各482根真实1m与各1根真实前驱分钟，旧原生多分钟none对照，及418日日线、
上市资料和完整事件；不使用聚宽因子，不合成停牌标记、公司事件或历史前收盘。
输出：pytest逐格断言；原生多分钟money差异固定记录，不为对齐修改原始金额。
上下游：真实BigQmtDataAdapter与wire编解码，四接口白名单内存Gateway；不启动服务或策略。
边界：5个早期日历窗口有真实trade_days，其余重放日线存档日期；前复权手算不是JQ数值验收。
两证券共35事件前依赖齐备后，可验QMT已表达权益的固定原点链，股改true不添加额外收益；
仍不证明事件外权益、源无漏报或与聚宽绝对因子一致；缺依赖和非法标识必须继续失败。
环境：仅本地JSON/pandas/pytest，无账号、地址、认证配置或网络副作用。
"""

from __future__ import annotations

import json
from copy import deepcopy
from decimal import ROUND_HALF_UP, Decimal, localcontext
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from bullet_trade.data.providers.remote_qmt import _dataframe_from_payload
from bullet_trade.data.qmt_adjustment import AdjustmentError
from bullet_trade.server.adapters.big_qmt import BigQmtDataAdapter, BigQmtGatewayConfig
from bullet_trade.server.adapters.qmt import dataframe_to_payload

pytestmark = pytest.mark.unit

_FIXTURES = Path(__file__).parents[1] / "fixtures"
_SECURITIES = ("000001.XSHE", "510500.XSHG")
_FIELDS = ["open", "high", "low", "close", "volume", "money"]
_FREQUENCIES = ("1m", "5m", "15m", "30m", "60m")
_REFERENCE = "2026-09-07"
_CASH_FACTS = {
    "000001.XSHE": ("2026-06-12", "11.3", "0.36", 2),
    "510500.XSHG": ("2026-07-15", "8.413", "0.149", 3),
}
_POST_FACTOR_SNAPSHOT = {
    "000001.XSHE": (
        "104.1949589411335515358066492679568431981",
        "107.6236778825236866868935225528256241443",
    ),
    "510500.XSHG": (
        "0.3340855449043369310047026073118897688125",
        "0.3401091105130913117791097574195218568513",
    ),
}
_NATIVE_MONEY_DIAGNOSTIC = {
    "000001.XSHE": {
        "5m": (35, 2, -35),
        "15m": (24, 3, -35),
        "30m": (15, 6, -35),
        "60m": (8, 8, -35),
    },
    "510500.XSHG": {
        "5m": (30, 3, -32),
        "15m": (21, 3, -32),
        "30m": (13, 5, -32),
        "60m": (7, 8, -32),
    },
}


@pytest.fixture(scope="module")
def minute_facts():
    """读取脱敏真实分钟样本；无输入，返回字典，只读取纳管固定JSON，不补字段。"""
    return json.loads((_FIXTURES / "big_qmt_real_minutes_20260908.json").read_text("utf-8"))


@pytest.fixture(scope="module")
def daily_facts():
    """读取已有真实日线和事件；无输入，返回字典，不用日线标记冒充分钟标记。"""
    return json.loads((_FIXTURES / "big_qmt_history_facts_20260908.json").read_text("utf-8"))


@pytest.fixture(scope="module")
def post_dependencies():
    """读取35事件前实际行情依赖；无输入，返回原始请求响应，不把填充值当真实交易收盘。"""
    return json.loads((_FIXTURES / "big_qmt_post_dependencies_20260908.json").read_text("utf-8"))


def _case(facts, security):
    """选择唯一证券事实；输入存档和代码，返回原案例，缺失或重复以断言失败。"""
    matches = [case for case in facts["cases"] if case["security"] == security]
    assert len(matches) == 1
    return matches[0]


def _frame(source, *, daily=False):
    """还原真实行情；输入columns/records及日线标记，返回新时间索引表，不填充缺值。"""
    frame = pd.DataFrame(source["records"], columns=source["columns"]).set_index("stime")
    frame.index = pd.to_datetime(frame.index, format="%Y%m%d" if daily else "%Y%m%d%H%M%S")
    return frame.rename_axis("time")


def _bounds(payload):
    """解析假网关窗口；输入请求，返回时间上下界，纯日期结束含全天，不修改状态。"""
    start, end = payload.get("start"), payload.get("end")
    lower = pd.Timestamp(start) if start is not None else None
    upper = pd.Timestamp(end) if end is not None else None
    if upper is not None and len(str(end)) == 10:
        upper += pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    return lower, upper


class _MinuteReplayGateway:
    """提供两证券真实基础行情和事件的内存网关，核心状态只有存档副本与调用记录。"""

    def __init__(self, minute_facts, daily_facts, dependencies=None):
        """装入真实样本；输入分钟/日线及可选历史依赖，返回None，仅创建独立内存表，不外取。"""
        self.config = BigQmtGatewayConfig()
        self.calls = []
        self.minutes = {s: _frame(_case(minute_facts, s)["raw"]) for s in _SECURITIES}
        self.minute_predecessors = {
            s: _frame(_case(minute_facts, s)["predecessor"]["response"]) for s in _SECURITIES
        }
        self.daily = {s: _frame(_case(daily_facts, s)["raw"], daily=True) for s in _SECURITIES}
        self.facts = {s: deepcopy(_case(daily_facts, s)) for s in _SECURITIES}
        self.dependencies = {}
        self.calendars = {}
        for security in _SECURITIES:
            pieces = []
            if dependencies is not None:
                pieces = [
                    _frame(case["response"], daily=True)
                    for case in dependencies["cases"]
                    if case["request"]["security"] == security
                ]
            self.dependencies[security] = (
                pd.concat(pieces).sort_index()
                if pieces
                else pd.DataFrame(index=pd.DatetimeIndex([], name="time"))
            )
            assert self.dependencies[security].index.is_unique
            self.calendars[security] = (
                self.daily[security].index.union(self.dependencies[security].index).sort_values()
            )
            if dependencies is not None:
                for case in dependencies.get("calendars", {}).get("cases", []):
                    request = case["request"]
                    if request["security"] != security:
                        continue
                    left, right = pd.Timestamp(request["start"]), pd.Timestamp(request["end"])
                    recorded = pd.DatetimeIndex(
                        pd.to_datetime(case["response"]["values"], format="%Y%m%d"), name="time"
                    )
                    current = self.calendars[security]
                    # 在明确采集窗内只信真实trade_days；窗外不补造从上市起的全日历。
                    current = current[(current < left) | (current > right)]
                    self.calendars[security] = current.union(recorded).sort_values()

    async def post(self, path, payload=None, *, timeout_seconds=None):
        """回应四种行情请求；输入路径/载荷/超时，返回新wire，越权调用断言失败，不联网。"""
        assert path in {
            "/data/history",
            "/data/security_info",
            "/data/trade_days",
            "/data/split_dividend",
        }, f"离线回放禁止非行情动作: {path}"
        payload = payload or {}
        self.calls.append((path, deepcopy(payload)))
        security = {"000001.SZ": "000001.XSHE", "510500.SH": "510500.XSHG"}.get(
            payload.get("security"), payload.get("security")
        )
        assert security in self.facts
        lower, upper = _bounds(payload)
        if path == "/data/security_info":
            return deepcopy(self.facts[security]["info"])
        if path == "/data/trade_days":
            # 5个早期窗口用真实trade_days，其余仅用已存日线日期；不补造缺失工作日。
            days = self.calendars[security]
            if lower is not None:
                days = days[days >= lower.normalize()]
            if upper is not None:
                days = days[days <= upper]
            if payload.get("count", -1) > 0:
                days = days[-payload["count"] :]
            return {"dtype": "list", "values": days.strftime("%Y-%m-%d").tolist()}
        if path == "/data/split_dividend":
            response = deepcopy(self.facts[security]["events"])
            response["events"] = [
                event
                for event in response["events"]
                if (lower is None or pd.Timestamp(event["date"]) >= lower.normalize())
                and (upper is None or pd.Timestamp(event["date"]) <= upper)
            ]
            return response
        assert payload.get("frequency") in {"1m", "1d"}, "不得拿原生多分钟充当基础行情"
        assert payload.get("fq") == "none", "不得拿QMT/JQ原生复权价格或因子作输入"
        assert payload.get("subscribe") is False
        assert lower is not None and upper is not None
        if payload.get("fill_data"):
            # 缺行反例只允许查询真实日线标记，返回原事实，绝不制造停牌或填充价。
            assert payload["frequency"] == "1d"
            assert set(payload["fields"]).issubset({"close", "preClose", "suspendFlag"})
        else:
            assert payload.get("fill_data") is False
        daily = payload["frequency"] == "1d"
        frame = (self.daily if daily else self.minutes)[security]
        if not daily:
            prior = self.minute_predecessors[security].loc[:, frame.columns]
            frame = pd.concat([prior, frame]).sort_index()
            assert frame.index.is_unique
        if daily and set(payload["fields"]).issubset({"close", "preClose", "suspendFlag"}):
            dependency = self.dependencies[security]
            if not dependency.empty:
                # 当前完整日线优先；旧依赖的原始浮点精度保留，不人为改写close或preClose。
                extra = dependency.loc[~dependency.index.isin(frame.index)]
                frame = pd.concat([frame[["close", "preClose", "suspendFlag"]], extra]).sort_index()
            if not payload["fill_data"]:
                frame = frame.loc[frame["suspendFlag"] == 0]
        frame = frame.loc[(frame.index >= lower) & (frame.index <= upper)].copy()
        # 缺字段反例必须让真实adapter检测，不在假网关抛KeyError掩盖真实校验。
        frame = frame.loc[:, [field for field in payload["fields"] if field in frame]]
        frame.index = frame.index.strftime("%Y%m%d" if daily else "%Y%m%d%H%M%S")
        return dataframe_to_payload(frame.rename_axis("stime"))


def _expected_basic(gateway, security, fq, reference=_REFERENCE):
    """独立合并竞价并手算复权；输入网关/证券/模式/基准，返回新表，不调用被测算法。"""
    source = gateway.minutes[security]
    frame = source[_FIELDS].astype(float).copy()
    for day in frame.index.normalize().unique():
        auction = day + pd.Timedelta(hours=9, minutes=30)
        first = auction + pd.Timedelta(minutes=1)
        frame.at[first, "open"] = frame.at[auction, "open"]
        frame.at[first, "high"] = max(frame.at[auction, "high"], frame.at[first, "high"])
        frame.at[first, "low"] = min(frame.at[auction, "low"], frame.at[first, "low"])
        for field in ("volume", "money"):
            frame.at[first, field] += frame.at[auction, field]
        frame = frame.drop(auction)
    factors = np.ones(len(frame))
    if fq == "post":
        factors = _expected_post_factors(gateway, security, frame.index)
        decimals = _CASH_FACTS[security][3]
        frame.loc[:, _FIELDS[:4]] = np.round(
            frame[_FIELDS[:4]].to_numpy() * factors[:, None], decimals
        )
        frame["volume"] = np.round(frame["volume"].to_numpy() / factors)
    elif fq == "pre":
        event_date, close, cash, decimals = _CASH_FACTS[security]
        events = [
            e
            for e in gateway.facts[security]["events"]["events"]
            if frame.index[0].date()
            < pd.Timestamp(e["date"]).date()
            <= max(frame.index[-1], pd.Timestamp(reference)).date()
        ]
        assert len(events) == 1 and events[0]["date"] == event_date
        event = events[0]
        assert Decimal(str(event["cash_per_share"])) == Decimal(cash)
        assert [event[k] for k in ("gift", "transfer", "rights", "rights_price")] == [0] * 4
        assert event["share_reform"] is False
        previous = gateway.daily[security].loc[: pd.Timestamp(event_date) - pd.Timedelta(days=1)]
        assert previous.iloc[-1]["suspendFlag"] == 0
        assert Decimal(str(previous.iloc[-1]["close"])) == Decimal(close)
        with localcontext() as context:
            context.prec = 40
            ex_price = (Decimal(close) - Decimal(cash)).quantize(
                Decimal(1).scaleb(-decimals), rounding=ROUND_HALF_UP
            )
            ratio = ex_price / Decimal(close)
            before = frame.index.normalize() < pd.Timestamp(event_date)
            if pd.Timestamp(reference) >= pd.Timestamp(event_date):
                factors[before] = float(ratio)
            else:
                factors[~before] = float(Decimal(1) / ratio)
        frame.loc[:, _FIELDS[:4]] = np.round(
            frame[_FIELDS[:4]].to_numpy() * factors[:, None], decimals
        )
        frame["volume"] = np.round(frame["volume"].to_numpy() / factors)
    else:
        assert fq in {None, "none"}, "仅支持明确复权方式，不构造外部因子种子"
    frame["factor"] = factors
    frame["paused"] = source.loc[frame.index, "suspendFlag"].astype(float)
    return frame


def _expected_post_factors(gateway, security, index):
    """以真实前收盘独立累计post链；输入网关/证券/完整输出轴，返回因子数组，不读取dr或JQ因子。"""
    facts = gateway.facts[security]
    events = sorted(facts["events"]["events"], key=lambda event: event["date"])
    origin = pd.Timestamp(facts["info"]["start_date"])
    assert origin <= index[0]
    dependencies = gateway.dependencies[security]
    assert not dependencies.empty, "固定post原点要求真实历史前收盘，不得用请求首行重置"
    decimals = _CASH_FACTS[security][3]
    factors = []
    with localcontext() as context:
        context.prec = 40
        ratios = []
        for event in events:
            day = pd.Timestamp(event["date"])
            if not origin < day <= index[-1]:
                continue
            assert isinstance(event["share_reform"], bool), "股改标记必须明确，不隐式强转真假"
            before = dependencies.loc[
                (dependencies.index < day) & dependencies["suspendFlag"].eq(0)
            ]
            assert not before.empty
            close = Decimal(str(before.iloc[-1]["close"]))
            cash = Decimal(str(event["cash_per_share"]))
            rights = Decimal(str(event["rights"]))
            scale = Decimal(1) + sum(Decimal(str(event[k])) for k in ("gift", "transfer", "rights"))
            ex_price = (
                (close - cash + rights * Decimal(str(event["rights_price"]))) / scale
            ).quantize(Decimal(1).scaleb(-decimals), rounding=ROUND_HALF_UP)
            assert close > 0 and ex_price > 0
            ratios.append((day, ex_price / close))
        for stamp in index:
            factor = Decimal(1)
            for day, ratio in ratios:
                if day <= stamp.normalize():
                    factor /= ratio
            factors.append(float(factor))
    return np.asarray(factors)


def _expected_post_daily(gateway, security):
    """从真实日线构造独立post期望；输入网关/证券，返回全字段新表，量仅从手转股一次。"""
    source = gateway.daily[security]
    factors = _expected_post_factors(gateway, security, source.index)
    decimals = _CASH_FACTS[security][3]
    result = source[_FIELDS].astype(float).copy()
    result.loc[:, _FIELDS[:4]] = np.round(
        result[_FIELDS[:4]].to_numpy() * factors[:, None], decimals
    )
    result["volume"] = np.round(result["volume"].to_numpy() * 100 / factors)
    result["factor"] = factors
    result["pre_close"] = np.round(source["preClose"].to_numpy() * factors, decimals)
    result["paused"] = source["suspendFlag"].astype(float)
    return result


def _aggregate(frame, size):
    """独立按已复权基础行合成；输入表和条数，返回组末索引OHLC量额，不吞尾组或重新复权。"""
    rows, stamps = [], []
    for offset in range(0, len(frame), size):
        block = frame.iloc[offset : offset + size]
        rows.append(
            [
                block["open"].iloc[0],
                block["high"].max(),
                block["low"].min(),
                block["close"].iloc[-1],
                block["volume"].sum(),
                block["money"].sum(),
            ]
        )
        stamps.append(block.index[-1])
    return pd.DataFrame(rows, index=pd.DatetimeIndex(stamps, name="time"), columns=_FIELDS)


async def _read(gateway, minute_facts, security, *, fq="none", frequency="1m", **overrides):
    """经真实adapter读取明确窗口；输入网关/存档/证券及覆盖参数，返回解码表，副作用仅内存调用。"""
    case = _case(minute_facts, security)
    payload = {
        "security": security,
        "start": case["start"],
        "end": case["end"],
        "frequency": frequency,
        "fields": _FIELDS,
        "fq": fq,
        "panel": False,
        "skip_paused": False,
        "fill_paused": True,
        "pre_factor_ref_date": _REFERENCE if fq == "pre" else None,
    }
    payload.update(overrides)
    return _dataframe_from_payload(await BigQmtDataAdapter(gateway).get_history(payload))


@pytest.mark.parametrize("security", _SECURITIES)
def test_saved_raw_minutes_have_exact_complete_real_axis_and_fields(minute_facts, security):
    """核对真实482根全轴字段；输入存档和证券，无返回，检查两日竞价/连续时段、真实flag和有限值。"""
    frame = _frame(_case(minute_facts, security)["raw"])
    days = frame.index.normalize().unique()
    assert len(days) == 2 and len(frame) == 482 and frame.index.is_unique
    expected = pd.DatetimeIndex(
        [
            stamp
            for day in days
            for stamp in list(
                pd.date_range(day + pd.Timedelta(hours=9, minutes=30), periods=121, freq="min")
            )
            + list(pd.date_range(day + pd.Timedelta(hours=13, minutes=1), periods=120, freq="min"))
        ],
        name="time",
    )
    pd.testing.assert_index_equal(frame.index, expected)
    assert frame.columns.tolist() == [*_FIELDS, "preClose", "suspendFlag"]
    assert np.isfinite(frame.to_numpy(dtype=float)).all()
    assert frame["suspendFlag"].eq(0).all()


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
@pytest.mark.parametrize("fq", [None, "none", "pre"])
@pytest.mark.parametrize("frequency", _FREQUENCIES)
async def test_real_minutes_full_axis_matches_independent_cash_and_auction_arithmetic(
    minute_facts, daily_facts, security, fq, frequency
):
    """真实分钟主矩阵逐格对照；输入样本/证券/方式/周期，无返回，不以交集掩盖缺行或量额差。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts)
    original = gateway.minutes[security].copy(deep=True)
    actual = await _read(gateway, minute_facts, security, fq=fq, frequency=frequency)
    expected = _aggregate(_expected_basic(gateway, security, fq), int(frequency[:-1]))
    assert len(actual) == 480 // int(frequency[:-1])
    pd.testing.assert_frame_equal(actual, expected, check_exact=True, check_dtype=False)
    pd.testing.assert_frame_equal(gateway.minutes[security], original, check_exact=True)
    assert all(
        p["frequency"] in {"1m", "1d"} for path, p in gateway.calls if path == "/data/history"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
@pytest.mark.parametrize("frequency", _FREQUENCIES[1:])
async def test_native_money_granularity_difference_is_diagnostic_not_acceptance(
    minute_facts, daily_facts, security, frequency
):
    """记录原生金额粒度差而非容差验收；输入真实存档/证券/周期，无返回，OHLC及一次量单位换算须全同。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts)
    actual = await _read(gateway, minute_facts, security, frequency=frequency)
    native = _frame(_case(minute_facts, security)["native_none"][frequency])
    native["volume"] *= 100
    pd.testing.assert_frame_equal(
        actual[_FIELDS[:5]], native[_FIELDS[:5]], check_exact=True, check_dtype=False
    )
    delta = actual["money"] - native["money"]
    assert (int(delta.ne(0).sum()), delta.abs().max(), delta.sum()) == (
        _NATIVE_MONEY_DIAGNOSTIC[security][frequency]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
@pytest.mark.parametrize("fq", ["none", "pre"])
@pytest.mark.parametrize("frequency", ["1m", "5m", "60m"])
async def test_real_minutes_count_and_start_overlap_keep_same_reference(
    minute_facts, daily_facts, security, fq, frequency
):
    """固定基准下尾窗不漂移；输入真实样本及方式周期，无返回，逐格比较count和显式start完整两组。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts)
    full = await _read(gateway, minute_facts, security, fq=fq, frequency=frequency)
    counted = await _read(
        gateway, minute_facts, security, fq=fq, frequency=frequency, start=None, count=2
    )
    size = int(frequency[:-1])
    start = gateway.minutes[security].index[-size * 2]
    explicit = await _read(
        gateway, minute_facts, security, fq=fq, frequency=frequency, start=str(start)
    )
    pd.testing.assert_frame_equal(counted, full.tail(2), check_exact=True)
    pd.testing.assert_frame_equal(explicit, counted, check_exact=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
@pytest.mark.parametrize("fq", ["none", "pre", "post"])
async def test_real_five_minute_count_crosses_cash_event_after_basic_adjustment(
    minute_facts, daily_facts, post_dependencies, security, fq
):
    """跨除权交易日的逆向五分钟按基础bar复权；输入真实存档/证券/方式，无返回，不将跨日组判错。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts, post_dependencies)
    end = _CASH_FACTS[security][0] + " 09:32:00"
    actual = await _read(
        gateway, minute_facts, security, fq=fq, frequency="5m", start=None, count=1, end=end
    )
    bases = _expected_basic(gateway, security, fq).loc[:end].tail(5)
    assert len(bases.index.normalize().unique()) == 2
    pd.testing.assert_frame_equal(actual, _aggregate(bases, 5), check_exact=True, check_dtype=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
@pytest.mark.parametrize("fq", ["none", "pre"])
async def test_real_minute_previous_close_uses_adjusted_previous_bar_and_fields(
    minute_facts, daily_facts, security, fq
):
    """分钟昨收取前一已复权基础close；输入真实存档/证券/方式，无返回，验证字段顺序和完整240行。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts)
    start = _CASH_FACTS[security][0] + " 09:31:00"
    fields = ["pre_close", "factor", "paused", "close", "volume", "money"]
    actual = await _read(gateway, minute_facts, security, fq=fq, start=start, fields=fields)
    expected = _expected_basic(gateway, security, fq)
    expected["pre_close"] = expected["close"].shift(1)
    expected = expected.loc[start:, fields]
    assert len(actual) == 240
    pd.testing.assert_frame_equal(actual, expected, check_exact=True, check_dtype=False)
    # 除权日0931的pre_close是上日收盘复权后值，不是当日原生preClose（0930价）。
    assert actual.iloc[0]["pre_close"] != gateway.minutes[security].loc[start, "preClose"]


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
async def test_real_minutes_reference_before_event_reanchors_both_days(
    minute_facts, daily_facts, security
):
    """前复权显式早基准同时锚定两日；输入真实样本/证券，无返回，事件后因子大于1不被默认end覆盖。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts)
    reference = gateway.minutes[security].index[0].strftime("%Y-%m-%d")
    actual = await _read(
        gateway,
        minute_facts,
        security,
        fq="pre",
        pre_factor_ref_date=reference,
        fields=[*_FIELDS, "factor"],
    )
    expected = _expected_basic(gateway, security, "pre", reference).loc[:, [*_FIELDS, "factor"]]
    assert actual.iloc[0]["factor"] == 1 and actual.iloc[-1]["factor"] > 1
    pd.testing.assert_frame_equal(actual, expected, check_exact=True, check_dtype=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
async def test_real_minutes_missing_suspend_field_is_not_filled_from_daily(
    minute_facts, daily_facts, security
):
    """构造缺字段反例不得补零；输入真实样本/证券，无返回，删除分钟flag必须明确失败。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts)
    gateway.minutes[security] = gateway.minutes[security].drop(columns="suspendFlag")
    with pytest.raises(AdjustmentError, match="缺少字段.*suspendFlag"):
        await _read(gateway, minute_facts, security)


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
async def test_real_minutes_missing_required_bar_is_not_intersection_or_pause(
    minute_facts, daily_facts, security
):
    """构造缺bar反例保留数据不足；输入真实样本/证券，无返回，日线flag0不得伪装停牌补齐。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts)
    missing = _CASH_FACTS[security][0] + " 14:59:00"
    gateway.minutes[security] = gateway.minutes[security].drop(pd.Timestamp(missing))
    with pytest.raises(AdjustmentError, match="没有明确停牌事实"):
        await _read(gateway, minute_facts, security, frequency="5m", start=None, count=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
async def test_real_post_missing_dependencies_do_not_change_fixed_listing_origin(
    minute_facts, daily_facts, security
):
    """缺真实post依赖必须失败；输入真实样本/证券，无返回，缺早期C不可换原点或原生兜底。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts)
    with pytest.raises(AdjustmentError, match="无法取得除权前实际交易日收盘"):
        await _read(gateway, minute_facts, security, fq="post", start=None, count=2)
    requests = [p for path, p in gateway.calls if path == "/data/split_dividend"]
    assert len(requests) == 1
    expected_listing = pd.Timestamp(gateway.facts[security]["info"]["start_date"]).strftime(
        "%Y-%m-%d"
    )
    assert requests[0]["start"] == expected_listing
    assert requests[0]["start"] < _case(minute_facts, security)["start"][:10]
    assert all(p["fq"] == "none" for path, p in gateway.calls if path == "/data/history")


@pytest.mark.parametrize("security,expected_count", [("000001.XSHE", 29), ("510500.XSHG", 6)])
def test_real_post_dependencies_preserve_complete_event_windows_and_actual_closes(
    daily_facts, post_dependencies, security, expected_count
):
    """核对35事件前窗口事实；输入存档/证券/数量，无返回，不把停牌填充close当事件前真实C。"""
    events = _case(daily_facts, security)["events"]["events"]
    cases = [case for case in post_dependencies["cases"] if case["request"]["security"] == security]
    assert len(cases) == len(events) == expected_count
    assert sorted(case["request"]["end"] for case in cases) == sorted(
        event["date"] for event in events
    )
    for case in cases:
        request, response = case["request"], case["response"]
        assert request["fq"] == "none" and request["fill_data"] is True
        assert request["subscribe"] is False and request["frequency"] == "1d"
        assert response["index_columns"] == ["stime"]
        frame = _frame(response, daily=True)
        assert frame.index.is_unique and frame.index.is_monotonic_increasing
        assert frame.columns.tolist() == ["close", "preClose", "suspendFlag"]
        assert np.isfinite(frame.to_numpy(dtype=float)).all()
        assert frame["suspendFlag"].isin([0, 1]).all()
        end = pd.Timestamp(request["end"])
        assert frame.index.min() >= pd.Timestamp(request["start"])
        assert frame.index.max() == end
        actual = frame.loc[(frame.index < end) & frame["suspendFlag"].eq(0)]
        assert not actual.empty and actual.iloc[-1]["close"] > 0
    if security == "510500.XSHG":
        case = next(case for case in cases if case["request"]["end"] == "2015-04-15")
        first_event = _frame(case["response"], daily=True)
        assert first_event.loc["2015-04-13":"2015-04-14", "suspendFlag"].tolist() == [1, 1]
        assert first_event.loc["2015-04-10", ["close", "suspendFlag"]].tolist() == [2.243, 0]


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
@pytest.mark.parametrize("frequency", _FREQUENCIES)
async def test_real_post_minutes_use_all_returned_events_and_fixed_listing_origin(
    minute_facts, daily_facts, post_dependencies, security, frequency
):
    """真实后复权五周期全轴对照；输入样本/证券/周期，无返回，以QMT完整返回事件而非JQ种子验收。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts, post_dependencies)
    actual = await _read(gateway, minute_facts, security, fq="post", frequency=frequency)
    bases = _expected_basic(gateway, security, "post")
    pd.testing.assert_frame_equal(
        actual, _aggregate(bases, int(frequency[:-1])), check_exact=True, check_dtype=False
    )
    # 仅从本证券上市日F0=1累计，快照来自独立Decimal40计算，不采用保存的JQ绝对因子。
    assert bases.iloc[0]["factor"] == float(_POST_FACTOR_SNAPSHOT[security][0])
    assert bases.iloc[-1]["factor"] == float(_POST_FACTOR_SNAPSHOT[security][1])
    requests = [p for path, p in gateway.calls if path == "/data/split_dividend"]
    listing = pd.Timestamp(gateway.facts[security]["info"]["start_date"]).strftime("%Y-%m-%d")
    assert len(requests) == 1 and requests[0]["start"] == listing
    # 只能透过明确flag寻找股票2007-05-31或ETF2015-04-10实际C，不读取其后的停牌填充价。
    pause_requests = [p for path, p in gateway.calls if path == "/data/history" and p["fill_data"]]
    expected_pause_days = (
        {
            "2007-06-01",
            "2007-06-04",
            "2007-06-05",
            "2007-06-06",
            "2007-06-07",
            "2007-06-08",
            "2007-06-11",
            "2007-06-12",
            "2007-06-13",
            "2007-06-14",
            "2007-06-15",
            "2007-06-18",
            "2007-06-19",
        }
        if security == "000001.XSHE"
        else {"2015-04-13", "2015-04-14"}
    )
    assert {p["start"] for p in pause_requests} == expected_pause_days


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
@pytest.mark.parametrize("frequency", _FREQUENCIES)
async def test_real_post_minute_overlap_never_restarts_factor_at_query_beginning(
    minute_facts, daily_facts, post_dependencies, security, frequency
):
    """真实后复权窗口保持同一原点；输入样本/证券/周期，无返回，比较全窗/count/显式start两组。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts, post_dependencies)
    full = await _read(gateway, minute_facts, security, fq="post", frequency=frequency)
    counted = await _read(
        gateway, minute_facts, security, fq="post", frequency=frequency, start=None, count=2
    )
    start = gateway.minutes[security].index[-int(frequency[:-1]) * 2]
    explicit = await _read(
        gateway, minute_facts, security, fq="post", frequency=frequency, start=str(start)
    )
    pd.testing.assert_frame_equal(counted, full.tail(2), check_exact=True)
    pd.testing.assert_frame_equal(explicit, counted, check_exact=True)
    listing = pd.Timestamp(gateway.facts[security]["info"]["start_date"]).strftime("%Y-%m-%d")
    assert {p["start"] for path, p in gateway.calls if path == "/data/split_dividend"} == {listing}


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
async def test_real_post_daily_matches_independent_408_day_prices_fields_and_overlap(
    minute_facts, daily_facts, post_dependencies, security
):
    """真实408日后复权字段和窗口逐格对照；输入存档及证券，无返回，量单位与日线preClose分别处理。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts, post_dependencies)
    fields = [*_FIELDS, "factor", "pre_close", "paused"]
    actual = await _read(
        gateway,
        minute_facts,
        security,
        fq="post",
        frequency="1d",
        start="2025-01-01",
        end=_REFERENCE,
        fields=fields,
    )
    expected = _expected_post_daily(gateway, security).loc["2025-01-01":, fields]
    assert len(actual) == len(expected) == 408
    pd.testing.assert_frame_equal(actual, expected, check_exact=True, check_dtype=False)
    counted = await _read(
        gateway,
        minute_facts,
        security,
        fq="post",
        frequency="1d",
        start=None,
        count=40,
        end=_REFERENCE,
        fields=fields,
    )
    pd.testing.assert_frame_equal(counted, actual.tail(40), check_exact=True)
    first_day = _case(minute_facts, security)["start"][:10]
    last_day = _case(minute_facts, security)["end"][:10]
    window = await _read(
        gateway,
        minute_facts,
        security,
        fq="post",
        frequency="1d",
        start=first_day,
        end=last_day,
        fields=fields,
    )
    pd.testing.assert_frame_equal(window, actual.loc[first_day:last_day], check_exact=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
async def test_real_post_daily_and_minute_share_factor_and_close_without_native_dr(
    minute_facts, daily_facts, post_dependencies, security
):
    """日分钟共用固定post因子且不依赖dr；输入样本及证券，无返回，同因子/收盘并验证三模式关系。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts, post_dependencies)
    post = await _read(gateway, minute_facts, security, fq="post", fields=["close", "factor"])
    daily = await _read(
        gateway,
        minute_facts,
        security,
        fq="post",
        frequency="1d",
        start=_case(minute_facts, security)["start"][:10],
        end=_case(minute_facts, security)["end"][:10],
        fields=["close", "factor"],
    )
    for day in daily.index:
        minutes = post.loc[str(day.date())]
        assert minutes["factor"].eq(daily.loc[day, "factor"]).all()
        assert minutes.iloc[-1]["close"] == daily.loc[day, "close"]
    pre = await _read(gateway, minute_facts, security, fq="pre", fields=["factor"])
    np.testing.assert_allclose(
        post["factor"] / post["factor"].iloc[-1], pre["factor"], rtol=0, atol=np.finfo(float).eps
    )
    none = await _read(
        gateway, minute_facts, security, fq="none", fields=["close", "factor", "money"]
    )
    assert none["factor"].eq(1).all()
    decimals = _CASH_FACTS[security][3]
    np.testing.assert_array_equal(post["close"], np.round(none["close"] * post["factor"], decimals))
    pre_values = await _read(gateway, minute_facts, security, fq="pre", fields=["close", "money"])
    np.testing.assert_array_equal(
        pre_values["close"], np.round(none["close"] * pre["factor"], decimals)
    )
    post_money = await _read(gateway, minute_facts, security, fq="post", fields=["money"])
    pd.testing.assert_series_equal(none["money"], pre_values["money"], check_exact=True)
    pd.testing.assert_series_equal(none["money"], post_money["money"], check_exact=True)
    # 这是显式构造的敏感性反例；只改不应参与候选数学的原生dr，实际事件事实和C保持不动。
    for event in gateway.facts[security]["events"]["events"]:
        event["qmt_dr"] = event["qmt_raw"]["dr"] = 123.0
    unchanged = await _read(gateway, minute_facts, security, fq="post", fields=["close", "factor"])
    pd.testing.assert_frame_equal(unchanged, post, check_exact=True)


def test_real_early_calendar_windows_match_saved_dependency_axes(
    minute_facts, daily_facts, post_dependencies
):
    """核对五个真实早期日历窗口；输入三存档，无返回，精确比对全部日期而非取交集。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts, post_dependencies)
    cases = post_dependencies["calendars"]["cases"]
    assert len(cases) == 5
    for case in cases:
        request, response = case["request"], case["response"]
        assert request["period"] == "1d" and request["count"] == -1
        expected = pd.DatetimeIndex(
            pd.to_datetime(response["values"], format="%Y%m%d"), name="time"
        )
        assert expected.is_unique and expected.is_monotonic_increasing
        left, right = pd.Timestamp(request["start"]), pd.Timestamp(request["end"])
        days = gateway.calendars[request["security"]]
        pd.testing.assert_index_equal(days[(days >= left) & (days <= right)], expected)
        frame = gateway.dependencies[request["security"]]
        pd.testing.assert_index_equal(frame.loc[left:right].index, expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_flag", [None, 0, 1, "true", float("nan")])
async def test_real_stock_post_rejects_nonboolean_share_reform_with_all_dependencies(
    minute_facts, daily_facts, post_dependencies, invalid_flag
):
    """真实股改标记只接受bool；输入样本及非法标记，无返回，不能因放行true而隐式转换其他值。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts, post_dependencies)
    event = next(e for e in gateway.facts["000001.XSHE"]["events"]["events"] if e["share_reform"])
    event["share_reform"] = invalid_flag
    with pytest.raises(AdjustmentError, match="股改标识不是布尔值"):
        await _read(gateway, minute_facts, "000001.XSHE", fq="post", start=None, count=2)
    assert all(p["fq"] == "none" for path, p in gateway.calls if path == "/data/history")


@pytest.mark.asyncio
async def test_real_stock_post_missing_actual_close_cannot_use_suspended_fill_price(
    minute_facts, daily_facts, post_dependencies
):
    """删除股改前实际C必须失败；输入真实样本，无返回，不能将连续停牌填充价格当实际收盘。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts, post_dependencies)
    gateway.dependencies["000001.XSHE"] = gateway.dependencies["000001.XSHE"].drop(
        pd.Timestamp("2007-05-31")
    )
    with pytest.raises(AdjustmentError, match="没有明确停牌事实.*2007-05-31"):
        await _read(gateway, minute_facts, "000001.XSHE", fq="post", start=None, count=2)
    assert all(p["fq"] == "none" for path, p in gateway.calls if path == "/data/history")


@pytest.mark.asyncio
@pytest.mark.parametrize("security", _SECURITIES)
@pytest.mark.parametrize("fq", ["none", "pre", "post"])
async def test_real_first_minute_previous_close_uses_saved_actual_predecessor(
    minute_facts, daily_facts, post_dependencies, security, fq
):
    """完整480分钟首行昨收由真实前驱计算；输入存档/证券/模式，无返回，不以日线价冒充前一bar。"""
    gateway = _MinuteReplayGateway(minute_facts, daily_facts, post_dependencies)
    recorded = _case(minute_facts, security)["predecessor"]
    request = recorded["request"]
    assert request["frequency"] == "1m" and request["fq"] == "none"
    assert request["fill_data"] is False and request["subscribe"] is False
    assert request["start"] == request["end"]
    predecessor = gateway.minute_predecessors[security]
    assert len(predecessor) == 1 and predecessor.index[0] == pd.Timestamp(request["start"])
    assert predecessor.iloc[0]["suspendFlag"] == 0
    fields = [*_FIELDS, "factor", "pre_close", "paused"]
    actual = await _read(gateway, minute_facts, security, fq=fq, fields=fields)
    expected = _expected_basic(gateway, security, fq)
    expected["pre_close"] = expected["close"].shift(1)
    # 两份真实前驱均在该窗口的最新现金事件前，其因子与首输出分钟完全相同。
    previous_close = np.round(
        predecessor.iloc[0]["close"] * expected.iloc[0]["factor"], _CASH_FACTS[security][3]
    )
    expected.loc[expected.index[0], "pre_close"] = previous_close
    assert len(actual) == 480
    pd.testing.assert_frame_equal(actual, expected[fields], check_exact=True, check_dtype=False)
