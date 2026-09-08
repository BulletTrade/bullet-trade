"""大 QMT 历史行情标准化的隔离端到端契约测试。

作者：BruceLee
职责：从公开价格入口及远程编解码进入真实数据适配器，验证唯一一次复权和聚合。
输入：内存中可精确手算的股票、ETF 原价、完整事件和交易日历，不读取真实账号。
输出：完整时间轴、价格量额、字段形状及错误断言；不生成文件或修改外部状态。
上下游：data.api / RemoteQmtProvider → BigQmtDataAdapter → 严格白名单假 Gateway。
环境约定：本地 pytest/pandas，不实例化真实连接，不启动服务、策略、订单或原生 SDK。
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import date, datetime
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from bullet_trade.core.settings import set_option
from bullet_trade.data import api as data_api
from bullet_trade.data.providers.remote_qmt import RemoteQmtProvider
from bullet_trade.remote.connection import RemoteServerError
from bullet_trade.server.adapters import big_qmt as big_qmt_module
from bullet_trade.server.adapters.big_qmt import (
    BigQmtDataAdapter,
    BigQmtGatewayConfig,
    BigQmtGatewayError,
)
from bullet_trade.server.adapters.qmt import dataframe_to_payload

pytestmark = pytest.mark.unit

_SECURITIES = ("000001.XSHE", "510500.XSHG")
_FIELDS = ["open", "high", "low", "close", "volume", "money"]
_EVENT_DATE = "2026-09-03"


def _instrument_values(security):
    """提供固定合成证券属性；输入证券代码，返回原价、现金、精度，无外部依赖。"""
    return (10.0, 2.0, 2) if security.startswith("000001") else (2.0, 0.4, 3)


def _canonical_security(security):
    """规范测试证券别名；输入单个代码，返回固定聚宽后缀，不访问证券服务。"""
    aliases = {
        "000001.SZ": "000001.XSHE",
        "510500.SH": "510500.XSHG",
        "000001": "000001.XSHE",
        "510500": "510500.XSHG",
    }
    return aliases.get(security, security)


def _event(security):
    """构造七项齐全的现金事件；输入证券，返回新字典，原生 dr 故意不用作计算种子。"""
    _, cash, _ = _instrument_values(security)
    return {
        "security": security,
        "date": _EVENT_DATE,
        "security_type": "stock" if security.startswith("000001") else "fund",
        "scale_factor": 1.0,
        "bonus_pre_tax": cash * (10 if security.startswith("000001") else 1),
        "per_base": 10 if security.startswith("000001") else 1,
        "cash_per_share": cash,
        "gift": 0.0,
        "transfer": 0.0,
        "rights": 0.0,
        "rights_price": 0.0,
        "share_reform": False,
        "qmt_dr": 1.234567,
        "qmt_raw": {
            "interest": cash,
            "stockBonus": 0.0,
            "stockGift": 0.0,
            "allotNum": 0.0,
            "allotPrice": 0.0,
            "gugai": 0.0,
            "dr": 1.234567,
        },
        "source_timestamp": "1788364800000",
    }


def _share_reform_event(event_date):
    """建立字段完整且映射一致的股改事实；输入日期，返回新字典，不补未提供的权益。"""
    event = _event(_SECURITIES[0])
    event.update(
        date=event_date,
        cash_per_share=0.009,
        gift=0.1,
        share_reform=True,
        scale_factor=1.1,
        bonus_pre_tax=0.09,
        source_timestamp=str(int(pd.Timestamp(event_date, tz="Asia/Shanghai").timestamp() * 1000)),
    )
    event["qmt_raw"].update(interest=0.009, stockBonus=0.1, gugai=1.0)
    return event


def _bars(security, frequency):
    """生成 helper 单位的基础原价；输入证券和1m/1d，返回新行情表，不复权或补缺。"""
    close, _, decimals = _instrument_values(security)
    days = pd.bdate_range("2026-08-31", "2026-09-08")
    if frequency == "1d":
        index = days
    else:
        stamps = []
        for day in days:
            stamps.extend(
                pd.date_range(day + pd.Timedelta(hours=9, minutes=30), periods=121, freq="min")
            )
            stamps.extend(
                pd.date_range(day + pd.Timedelta(hours=13, minutes=1), periods=120, freq="min")
            )
        index = pd.DatetimeIndex(stamps)
    prices = np.where(index.normalize() < pd.Timestamp(_EVENT_DATE), close, close * 0.8)
    return pd.DataFrame(
        {
            "open": np.round(prices, decimals),
            "high": np.round(prices + 0.1, decimals),
            "low": np.round(prices - 0.1, decimals),
            "close": np.round(prices, decimals),
            "volume": 8.0 if frequency == "1d" else 800.0,
            "money": 8000.0,
            "preClose": prices,
            "suspendFlag": 0,
        },
        index=index.rename("stime"),
    )


def _window(payload):
    """解析假 Gateway 查询边界；输入请求，返回含整日末尾的时间戳边界，不修改请求。"""
    start = payload.get("start", payload.get("start_date"))
    end = payload.get("end", payload.get("end_date"))
    lower = pd.Timestamp(start) if start else None
    upper = pd.Timestamp(end) if end else None
    if upper is not None and len(str(end)) in (8, 10):
        upper += pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    return lower, upper


class _FakeGateway:
    """提供四种只读事实接口；保存内存行情、事件和调用记录，禁止任何其他外部动作。"""

    def __init__(self):
        """建立完整合成样本；无输入和返回，仅初始化假行情、事件及可配置错误状态。"""
        self.config = BigQmtGatewayConfig()
        self.calls = []
        self.frames = {
            (security, frequency): _bars(security, frequency)
            for security in _SECURITIES
            for frequency in ("1m", "1d")
        }
        self.events = {security: [_event(security)] for security in _SECURITIES}
        self.overrides = {}
        self.start_date = "2026-08-31"
        self.suspension_facts = {}

    async def post(self, path, payload=None, *, timeout_seconds=None):
        """处理白名单查询；输入路径载荷及超时，返回新事实载荷，非法动作或配置错误抛错。"""
        allowed = {
            "/data/history",
            "/data/split_dividend",
            "/data/security_info",
            "/data/trade_days",
        }
        assert path in allowed, f"测试禁止调用 {path}"
        payload = payload or {}
        self.calls.append((path, deepcopy(payload)))
        if path in self.overrides:
            result = self.overrides[path]
            if isinstance(result, BaseException):
                raise result
            return deepcopy(result)
        security = _canonical_security(payload.get("security"))
        if path == "/data/security_info":
            _, _, decimals = _instrument_values(security)
            return {
                "start_date": self.start_date,
                "OpenDate": self.start_date.replace("-", ""),
                "type": "stock" if decimals == 2 else "etf",
                "PriceTick": 10**-decimals,
            }
        lower, upper = _window(payload)
        if path == "/data/trade_days":
            days = pd.bdate_range("2026-08-20", "2026-09-10")
            if lower is not None:
                days = days[days >= lower.normalize()]
            if upper is not None:
                days = days[days <= upper]
            count = payload.get("count")
            if count is not None and count > 0:
                days = days[-count:]
            return {"dtype": "list", "values": days.strftime("%Y-%m-%d").tolist()}
        if path == "/data/split_dividend":
            events = deepcopy(self.events[security])
            if lower is not None:
                events = [row for row in events if pd.Timestamp(row["date"]) >= lower.normalize()]
            if upper is not None:
                events = [row for row in events if pd.Timestamp(row["date"]) <= upper]
            return {
                "schema": "big-qmt-dividend-events/v1",
                "source": "ContextInfo.get_divid_factors",
                "gateway_build_id": "20260908_dividend_facts_v1",
                "events": events,
                "raw_event_count": len(events),
                "event_fields_complete": bool(events),
                "history_completeness_verified": False,
            }
        assert isinstance(security, str) and security in _SECURITIES
        assert payload.get("fq") == "none", "不得读取原生复权作为自算输入或兜底"
        assert payload.get("subscribe") is False
        assert lower is not None and upper is not None, "基础行情必须明确依赖窗口"
        frequency = payload["frequency"]
        assert frequency in {"1m", "1d"}, "多周期必须从基础行情合成"
        frame = self.frames[(security, frequency)]
        frame = frame.loc[(frame.index >= lower) & (frame.index <= upper)].copy()
        if payload.get("fill_data") is True:
            # 只为缺轴查询提供明确的日线停牌事实，填充值不能变成真实前收盘。
            assert frequency == "1d"
            assert set(payload.get("fields") or []) <= {"close", "preClose", "suspendFlag"}
            days = pd.bdate_range(max(lower.normalize(), pd.Timestamp(self.start_date)), upper)
            existing = set(frame.index)
            frame = frame.reindex(days)
            for day in days:
                if day not in existing:
                    frame.loc[day, "suspendFlag"] = self.suspension_facts.get((security, day), 0)
                    frame.loc[day, ["close", "preClose"]] = 99.0
        else:
            assert payload.get("fill_data") is False
        if payload.get("count") is not None and payload["count"] > 0:
            frame = frame.tail(payload["count"])
        requested = payload.get("fields") or frame.columns.tolist()
        frame = frame.loc[:, requested]
        # 保留 helper 的紧凑字符串索引形状，不能用已规范化的 DatetimeIndex 掩盖解析错误。
        frame.index = frame.index.strftime("%Y%m%d" if frequency == "1d" else "%Y%m%d%H%M%S")
        frame.index.name = "stime"
        return dataframe_to_payload(frame)


def _client(gateway):
    """创建真实适配器及无网络远程客户端；输入假Gateway，返回客户端及最外层请求记录。"""
    adapter = BigQmtDataAdapter(gateway)
    calls = []

    def request(action, payload):
        """执行真实适配并模拟错误wire；输入动作载荷，返回结果或远端异常，仅记录内存调用。"""
        assert action == "data.history"
        calls.append(deepcopy(payload))
        try:
            return asyncio.run(adapter.get_history(payload))
        except AssertionError:
            # 假Gateway白名单等测试安全断言不能被当作预期业务错误吞掉。
            raise
        except Exception as exc:
            # 对齐ServerSession编码及RemoteQmtConnection重建，不能保留本地异常类型。
            raise RemoteServerError(getattr(exc, "code", "REQUEST_FAILED"), str(exc)) from exc

    provider = object.__new__(RemoteQmtProvider)
    provider._connection = SimpleNamespace(request=request)
    return provider, calls


def _price_request(provider, security=_SECURITIES[0], **kwargs):
    """发出明确默认范围的价格请求；输入客户端及覆盖参数，返回结果，不直接处理算法。"""
    options = {
        "end_date": "2026-09-04 15:00:00",
        "count": 2,
        "fields": _FIELDS,
        "frequency": "1d",
        "fq": "pre",
        "pre_factor_ref_date": "2026-09-04",
    }
    options.update(kwargs)
    if options["fq"] != "pre":
        options["pre_factor_ref_date"] = None
    return provider.get_price(security, **options)


@pytest.mark.parametrize("security", _SECURITIES)
@pytest.mark.parametrize("fq", [None, "pre", "post"])
@pytest.mark.parametrize("frequency", ["1m", "5m", "15m", "30m", "60m", "1d"])
@pytest.mark.parametrize("after_event", [False, True])
def test_price_matrix_uses_raw_once_and_matches_exact_arithmetic(
    security, fq, frequency, after_event
):
    """验证完整复权周期矩阵；输入证券、方式、周期和事件侧，无返回，逐格检查独立手算值。"""
    gateway = _FakeGateway()
    originals = {key: value.copy(deep=True) for key, value in gateway.frames.items()}
    original_events = deepcopy(gateway.events)
    provider, _ = _client(gateway)
    day = "2026-09-03" if after_event else "2026-09-02"
    end = day + (" 15:00:00" if frequency == "1d" else " 11:30:00")

    result = _price_request(provider, security, frequency=frequency, fq=fq, end_date=end, count=1)

    close, _, decimals = _instrument_values(security)
    raw_close = close * (0.8 if after_event else 1.0)
    factor = (
        (1.0 if after_event else 0.8)
        if fq == "pre"
        else (1.25 if after_event else 1.0) if fq == "post" else 1.0
    )
    size = 1 if frequency == "1d" else int(frequency[:-1])
    stamp = pd.Timestamp(day if frequency == "1d" else end)
    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.tolist() == [stamp]
    assert not result.index.has_duplicates
    assert list(result.columns) == _FIELDS
    assert result.iloc[0].tolist() == [
        float(np.round(raw_close * factor, decimals)),
        float(np.round((raw_close + 0.1) * factor, decimals)),
        float(np.round((raw_close - 0.1) * factor, decimals)),
        float(np.round(raw_close * factor, decimals)),
        float(np.round(800.0 / factor) * size),
        8000.0 * size,
    ]
    assert gateway.events == original_events
    for key, original in originals.items():
        pd.testing.assert_frame_equal(gateway.frames[key], original)


def test_open_auction_merges_all_ohlc_and_units_once():
    """验证0930合入0931而不重复乘量；无输入，无返回，精确断言开高低收及量额。"""
    gateway = _FakeGateway()
    frame = gateway.frames[(_SECURITIES[1], "1m")]
    frame.loc["2026-09-03 09:30:00", _FIELDS] = [1.55, 1.75, 1.50, 1.60, 300.0, 480.0]
    frame.loc["2026-09-03 09:31:00", _FIELDS] = [1.61, 1.70, 1.58, 1.65, 800.0, 1300.0]
    provider, _ = _client(gateway)

    result = _price_request(
        provider, _SECURITIES[1], fq=None, frequency="1m", end_date="2026-09-03 09:31:00", count=1
    )

    assert result.index.tolist() == [pd.Timestamp("2026-09-03 09:31:00")]
    assert result.iloc[0].tolist() == [1.55, 1.75, 1.50, 1.65, 1100.0, 1780.0]


@pytest.mark.parametrize("fq", ["pre", "post"])
def test_overlapping_count_windows_keep_fixed_factor_and_exact_columns(fq):
    """验证改变count不改变重叠值及基准；输入复权方式，无返回，断言完整尾部和字段顺序。"""
    provider, _ = _client(_FakeGateway())
    narrow = _price_request(provider, count=2, fq=fq, fields=["factor", "close", "money"])
    broad = _price_request(provider, count=4, fq=fq, fields=["factor", "close", "money"])

    assert list(narrow.columns) == ["factor", "close", "money"]
    assert len(narrow) == 2 and len(broad) == 4
    pd.testing.assert_frame_equal(narrow, broad.tail(2))
    assert broad["factor"].tolist() == (
        [0.8, 0.8, 1.0, 1.0] if fq == "pre" else [1.0, 1.0, 1.25, 1.25]
    )


def test_reference_before_return_window_reanchors_backwards():
    """验证参考日早于行情的反向锚定；无输入，无返回，断言后续原价被乘1.25而非漏算。"""
    provider, _ = _client(_FakeGateway())
    result = _price_request(
        provider, count=2, fields=["close", "factor"], pre_factor_ref_date="2026-09-02"
    )
    assert result["close"].tolist() == [10.0, 10.0]
    assert result["factor"].tolist() == [1.25, 1.25]


def test_daily_pre_close_uses_same_day_source_reference_not_shifted_close():
    """日线昨收按当日因子转换而非shift；无输入，无返回，使用可区分两算法的源字段断言。"""
    gateway = _FakeGateway()
    gateway.frames[(_SECURITIES[0], "1d")].loc["2026-09-02", "preClose"] = 9.8
    provider, _ = _client(gateway)
    result = _price_request(provider, end_date="2026-09-02", count=1, fields=["pre_close", "close"])

    assert list(result.columns) == ["pre_close", "close"]
    assert result.iloc[0].tolist() == [7.84, 8.0]


def test_minute_pre_close_uses_adjusted_previous_bar_outside_return_count():
    """分钟昨收来自窗口外前一已复权bar；无输入，无返回，断言不能直接套用QMT日昨收。"""
    gateway = _FakeGateway()
    frame = gateway.frames[(_SECURITIES[1], "1m")]
    frame.loc["2026-09-02 10:29:00", "close"] = 1.99
    frame.loc["2026-09-02 10:30:00", "preClose"] = 9.99
    provider, _ = _client(gateway)
    result = _price_request(
        provider,
        _SECURITIES[1],
        frequency="1m",
        end_date="2026-09-02 10:30:00",
        count=1,
        fields=["pre_close", "close"],
    )

    assert result.index.tolist() == [pd.Timestamp("2026-09-02 10:30:00")]
    assert result.iloc[0].tolist() == [1.592, 1.6]


@pytest.mark.parametrize("skip_paused", [False, True])
def test_paused_uses_explicit_source_flag_instead_of_zero_volume(skip_paused):
    """停牌只依据明确标记；输入跳过开关，无返回，零量正常日不得误删，非零量停牌日可跳过。"""
    gateway = _FakeGateway()
    frame = gateway.frames[(_SECURITIES[0], "1d")]
    frame.loc["2026-09-03", "suspendFlag"] = 1
    frame.loc["2026-09-04", "volume"] = 0
    provider, _ = _client(gateway)
    result = _price_request(
        provider,
        fq=None,
        start_date="2026-09-02",
        count=None,
        fields=["paused"],
        skip_paused=skip_paused,
    )

    expected = (
        ["2026-09-02", "2026-09-04"] if skip_paused else ["2026-09-02", "2026-09-03", "2026-09-04"]
    )
    assert result.index.tolist() == pd.to_datetime(expected).tolist()
    assert result["paused"].tolist() == ([0, 0] if skip_paused else [0, 1, 0])


@pytest.mark.parametrize("fq", [None, "pre", "post"])
def test_fill_paused_false_preserves_row_and_masks_all_paused_fields(fq):
    """按真镜像保留停牌行并置空；输入复权方式，无返回，不复权factor为1，其余字段明确NaN。"""
    gateway = _FakeGateway()
    gateway.frames[(_SECURITIES[0], "1d")].loc["2026-09-03", "suspendFlag"] = 1
    provider, _ = _client(gateway)
    result = _price_request(
        provider,
        fq=fq,
        start_date="2026-09-02",
        count=None,
        fields=["open", "high", "low", "close", "pre_close", "volume", "money", "paused", "factor"],
        fill_paused=False,
    )

    assert (
        result.index.tolist() == pd.to_datetime(["2026-09-02", "2026-09-03", "2026-09-04"]).tolist()
    )
    nullable_fields = ["open", "high", "low", "close", "pre_close", "volume", "money", "paused"]
    assert result.loc["2026-09-03", nullable_fields].isna().all()
    if fq is None:
        assert result.loc["2026-09-03", "factor"] == 1.0
    else:
        assert pd.isna(result.loc["2026-09-03", "factor"])


@pytest.mark.parametrize("fq", [None, "pre", "post"])
def test_event_during_suspension_fills_price_and_factor_from_same_previous_bar(fq):
    """停牌遇除权时价格和因子同源填充；输入复权方式，无返回，禁止填价格却用事件后新因子。"""
    gateway = _FakeGateway()
    gateway.frames[(_SECURITIES[0], "1d")].loc["2026-09-03", "suspendFlag"] = 1
    provider, _ = _client(gateway)
    result = _price_request(
        provider,
        fq=fq,
        start_date="2026-09-02",
        count=None,
        fields=["open", "high", "low", "close", "pre_close", "volume", "money", "paused", "factor"],
        fill_paused=True,
    )

    expected_price = 8.0 if fq == "pre" else 10.0
    expected_factor = 0.8 if fq == "pre" else 1.0
    assert (
        result.loc["2026-09-03", ["open", "high", "low", "close", "pre_close"]].tolist()
        == [expected_price] * 5
    )
    assert result.loc["2026-09-03", ["volume", "money", "paused", "factor"]].tolist() == [
        0.0,
        0.0,
        1.0,
        expected_factor,
    ]
    assert result.loc["2026-09-04", "factor"] == (1.25 if fq == "post" else 1.0)


@pytest.mark.parametrize("fq", [None, "pre", "post"])
def test_multiday_group_with_unfilled_suspension_preserves_exact_nan_semantics(fq):
    """含未填停牌的多日组传播空值；输入复权方式，无返回，开高低保首有效值，末收及量额为空。"""
    gateway = _FakeGateway()
    gateway.frames[(_SECURITIES[0], "1d")].loc["2026-09-03", "suspendFlag"] = 1
    provider, _ = _client(gateway)
    result = _price_request(
        provider, fq=fq, frequency="2d", start_date="2026-09-02", count=None, fill_paused=False
    )
    first = [8.0, 8.08, 7.92] if fq == "pre" else [10.0, 10.1, 9.9]
    final = (
        [10.0, 10.12, 9.88, 10.0, 640.0, 8000.0]
        if fq == "post"
        else [8.0, 8.1, 7.9, 8.0, 800.0, 8000.0]
    )
    expected = pd.DataFrame(
        [[*first, np.nan, np.nan, np.nan], final],
        index=pd.DatetimeIndex(["2026-09-03", "2026-09-04"], name="time"),
        columns=_FIELDS,
    )
    pd.testing.assert_frame_equal(result, expected, check_exact=True)


def test_count_cross_event_group_adjusts_each_basic_bar_before_aggregation():
    """跨除权五分钟先复权再聚合；无输入，无返回，以两日前后原价不同但复权价相同作断言。"""
    provider, _ = _client(_FakeGateway())
    result = _price_request(provider, frequency="5m", end_date="2026-09-03 09:32:00", count=1)

    assert result.index.tolist() == [pd.Timestamp("2026-09-03 09:32:00")]
    # 上日最后3根每根800股/.8，当日0931含竞价共1600股、0932再800股。
    assert result.iloc[0].tolist() == [8.0, 8.1, 7.9, 8.0, 5400.0, 48000.0]


def test_start_group_and_count_group_have_different_explicit_row_anchors():
    """start正分组和count逆取不同；无输入，无返回，检查完整末时间而不把不足组丢弃。"""
    provider, _ = _client(_FakeGateway())
    forward = _price_request(
        provider,
        fq=None,
        frequency="5m",
        start_date="2026-09-03 10:01:00",
        end_date="2026-09-03 10:07:00",
        count=None,
        fields=["close", "volume"],
    )
    backward = _price_request(
        provider,
        fq=None,
        frequency="5m",
        end_date="2026-09-03 10:07:00",
        count=1,
        fields=["close", "volume"],
    )

    assert (
        forward.index.tolist()
        == pd.to_datetime(["2026-09-03 10:05:00", "2026-09-03 10:07:00"]).tolist()
    )
    assert forward["volume"].tolist() == [4000.0, 1600.0]
    assert backward.index.tolist() == [pd.Timestamp("2026-09-03 10:07:00")]
    assert backward["volume"].tolist() == [4000.0]


@pytest.mark.parametrize("frequency", ["1m", "5m"])
def test_earlier_intraday_gap_outside_count_window_does_not_block_complete_result(frequency):
    """足额count窗口不受当日更早无关缺行影响；输入周期，无返回，保留末时点与准确量额。"""
    gateway = _FakeGateway()
    key = (_SECURITIES[0], "1m")
    gateway.frames[key] = gateway.frames[key].drop(pd.Timestamp("2026-09-03 09:45:00"))
    provider, _ = _client(gateway)
    result = _price_request(
        provider, fq=None, frequency=frequency, end_date="2026-09-03 11:30:00", count=1
    )
    size = int(frequency[:-1])
    assert result.index.tolist() == [pd.Timestamp("2026-09-03 11:30:00")]
    assert result.iloc[0].tolist() == [8.0, 8.1, 7.9, 8.0, 800.0 * size, 8000.0 * size]


@pytest.mark.parametrize("frequency,fields", [("5m", _FIELDS), ("1m", ["close", "pre_close"])])
def test_gap_in_required_aggregation_or_previous_bar_remains_failure(frequency, fields):
    """聚合或分钟昨收真正依赖的缺行仍失败；输入周期字段，无返回，不能以收紧窗口掩盖缺数。"""
    gateway = _FakeGateway()
    key = (_SECURITIES[0], "1m")
    gateway.frames[key] = gateway.frames[key].drop(pd.Timestamp("2026-09-03 11:29:00"))
    provider, _ = _client(gateway)
    with pytest.raises(RemoteServerError):
        _price_request(
            provider,
            fq=None,
            frequency=frequency,
            end_date="2026-09-03 11:30:00",
            count=1,
            fields=fields,
        )


@pytest.mark.parametrize("frequency", ["1m", "5m"])
def test_missing_first_minute_outside_complete_tail_window_does_not_block_auction_normalization(
    frequency,
):
    """窗口外0931缺失不阻断尾窗；输入周期，无返回，0930仍在也不能在无关竞价阶段误失败。"""
    gateway = _FakeGateway()
    key = (_SECURITIES[0], "1m")
    gateway.frames[key] = gateway.frames[key].drop(pd.Timestamp("2026-09-03 09:31:00"))
    provider, _ = _client(gateway)
    result = _price_request(
        provider, fq=None, frequency=frequency, end_date="2026-09-03 11:30:00", count=1
    )

    size = int(frequency[:-1])
    assert result.index.tolist() == [pd.Timestamp("2026-09-03 11:30:00")]
    assert result.iloc[0].tolist() == [8.0, 8.1, 7.9, 8.0, 800.0 * size, 8000.0 * size]


@pytest.mark.parametrize(
    "end,fields",
    [
        ("2026-09-03 09:31:00", ["close"]),
        ("2026-09-03 09:32:00", ["close", "pre_close"]),
    ],
)
def test_missing_first_minute_required_by_result_or_pre_close_still_fails(end, fields):
    """返回bar或昨收需要0931时缺失仍失败；输入终点字段，无返回，不能用0930竞价冒充正常分钟。"""
    gateway = _FakeGateway()
    key = (_SECURITIES[0], "1m")
    gateway.frames[key] = gateway.frames[key].drop(pd.Timestamp("2026-09-03 09:31:00"))
    provider, _ = _client(gateway)
    with pytest.raises(RemoteServerError):
        _price_request(provider, fq=None, frequency="1m", end_date=end, count=1, fields=fields)


@pytest.mark.parametrize(
    "frequency,dates,volumes,money",
    [
        ("1w", ["2026-09-04", "2026-09-08"], [4600.0, 1600.0], [40000.0, 16000.0]),
        ("1mon", ["2026-08-31", "2026-09-08"], [1000.0, 5200.0], [8000.0, 48000.0]),
        ("1M", ["2026-08-31", "2026-09-08"], [1000.0, 5200.0], [8000.0, 48000.0]),
    ],
)
def test_natural_calendar_periods_use_daily_basis_and_calendar_boundaries(
    frequency, dates, volumes, money
):
    """自然周月不能冒充固定行数；输入周期与精确金标，无返回，检查完整分组末日及量额。"""
    provider, _ = _client(_FakeGateway())
    result = _price_request(
        provider,
        frequency=frequency,
        start_date="2026-08-31",
        end_date="2026-09-08",
        count=None,
        fields=["close", "volume", "money"],
    )
    assert result.index.tolist() == pd.to_datetime(dates).tolist()
    assert result["close"].tolist() == [8.0, 8.0]
    assert result["volume"].tolist() == volumes
    assert result["money"].tolist() == money


@pytest.mark.parametrize("panel", [False, True])
def test_multi_security_shape_keeps_all_requested_rows_and_fields(panel):
    """验证双证券形状及无丢行；输入panel开关，无返回，检查日期、代码和字段的完整集合。"""
    provider, _ = _client(_FakeGateway())
    result = _price_request(provider, list(_SECURITIES), panel=panel, fields=["close", "volume"])
    if panel:
        assert isinstance(result.columns, pd.MultiIndex)
        assert set(result.columns.tolist()) == {
            (field, security) for field in ["close", "volume"] for security in _SECURITIES
        }
        assert len(result) == 2
    else:
        assert list(result.columns) == ["time", "code", "close", "volume"]
        assert len(result) == 4
        assert set(result["code"]) == set(_SECURITIES)
        assert not result.duplicated(["time", "code"]).any()


def test_multi_security_default_reference_is_captured_once_and_index_payload_is_unchanged(
    monkeypatch,
):
    """同轮多证券默认基准只读一次时钟；输入替换器，无返回，模拟跨午夜且指数原payload不被加基准。"""
    clock_calls = []

    def today():
        """模拟午夜日期切换；无输入，返回两候选日期之一，仅记录测试内调用次数。"""
        value = date(2026, 9, 2) if not clock_calls else date(2026, 9, 4)
        clock_calls.append(value)
        return value

    monkeypatch.setattr(big_qmt_module, "_history_today", today)
    gateway = _FakeGateway()
    provider, requests = _client(gateway)
    result = provider.get_price(
        list(_SECURITIES),
        end_date="2026-09-02",
        count=1,
        fields=["close"],
        frequency="1d",
        fq="pre",
        panel=True,
    )

    assert clock_calls == [date(2026, 9, 2)]
    assert result.index.tolist() == [pd.Timestamp("2026-09-02")]
    assert result[("close", _SECURITIES[0])].tolist() == [10.0]
    assert result[("close", _SECURITIES[1])].tolist() == [2.0]

    native = pd.DataFrame(
        {"close": [3210.1234]}, index=pd.DatetimeIndex(["2026-09-02"], name="time")
    )
    gateway.overrides["/data/security_info"] = {"type": "index", "start_date": "2000-01-01"}
    gateway.overrides["/data/history"] = dataframe_to_payload(native)
    index_result = provider.get_price(
        "000016.XSHG", end_date="2026-09-02", count=1, fields=["close"], frequency="1d", fq="pre"
    )
    pd.testing.assert_frame_equal(index_result, native)
    assert gateway.calls[-1] == ("/data/history", requests[-1])
    assert requests[-1]["pre_factor_ref_date"] is None


def test_normal_trading_day_missing_from_one_security_is_explicit_failure():
    """正常交易日缺源bar不能innerjoin隐藏；无输入，无返回，双证券查询必须报告数据不足。"""
    gateway = _FakeGateway()
    key = (_SECURITIES[1], "1d")
    gateway.frames[key] = gateway.frames[key].drop(pd.Timestamp("2026-09-03"))
    provider, _ = _client(gateway)
    with pytest.raises((ValueError, RuntimeError)):
        _price_request(provider, list(_SECURITIES), fq=None, count=3)


@pytest.mark.parametrize("security", ["000016.XSHG", "000905.XSHG", "399673.XSHE"])
@pytest.mark.parametrize("fq", [None, "pre", "post"])
def test_index_keeps_original_payload_and_values_without_stock_etf_adjustment(security, fq):
    """指数保留原生旧路径；输入指数和复权方式，无返回，断言原请求及值不变且不取公司事件。"""
    gateway = _FakeGateway()
    native = pd.DataFrame(
        {"close": [3210.1234], "volume": [7.0], "money": [890.12]},
        index=pd.DatetimeIndex(["2026-09-04"], name="time"),
    )
    gateway.overrides["/data/security_info"] = {"type": "index", "start_date": "2000-01-01"}
    gateway.overrides["/data/history"] = dataframe_to_payload(native)
    provider, requests = _client(gateway)
    result = _price_request(provider, security, fq=fq, count=1, fields=["close", "volume", "money"])

    pd.testing.assert_frame_equal(result, native)
    history_calls = [payload for path, payload in gateway.calls if path == "/data/history"]
    assert history_calls == requests
    assert not any(path == "/data/split_dividend" for path, _ in gateway.calls)


@pytest.mark.parametrize(
    "security,kind",
    [
        ("00700.HK", "stock"),
        ("900901.SH", "stock"),
        ("200002.SZ", "stock"),
        ("430047.BJ", "stock"),
        ("160101.SZ", "fund"),
    ],
)
def test_non_a_share_or_etf_scope_keeps_old_payload_and_values(security, kind):
    """港股B股北京和普通基金不套A股语义；输入证券类型，无返回，原payload量价不变且不取事件。"""
    gateway = _FakeGateway()
    native = pd.DataFrame(
        {"close": [9.87654], "volume": [7.0]}, index=pd.DatetimeIndex(["2026-09-04"], name="time")
    )
    gateway.overrides["/data/security_info"] = {
        "type": kind,
        "qmt_security": security,
        "start_date": "2000-01-01",
    }
    gateway.overrides["/data/history"] = dataframe_to_payload(native)
    provider, requests = _client(gateway)
    result = _price_request(provider, security, fields=["close", "volume"], count=1)
    pd.testing.assert_frame_equal(result, native)
    assert [payload for path, payload in gateway.calls if path == "/data/history"] == requests
    assert all(path in {"/data/security_info", "/data/history"} for path, _ in gateway.calls)


@pytest.mark.parametrize("metadata_field", ["qmt_security", "qmt_code"])
def test_bare_symbol_uses_explicit_normalized_qmt_metadata_to_enter_standard_scope(metadata_field):
    """裸证券按QMT规范元数据判范围；输入元数据别名，无返回，ETF正确复权而非原生透传。"""
    gateway = _FakeGateway()
    gateway.overrides["/data/security_info"] = {
        "type": "etf",
        metadata_field: "510500.SH",
        "start_date": "2026-08-31",
        "PriceTick": 0.001,
    }
    provider, _ = _client(gateway)
    result = _price_request(
        provider, "510500", end_date="2026-09-02", count=1, fields=["close", "volume"]
    )
    assert result.iloc[0].tolist() == [1.6, 1000.0]
    assert any(path == "/data/split_dividend" for path, _ in gateway.calls)


@pytest.mark.parametrize(
    "replacement", [None, {}, {"events": []}, {"schema": "wrong", "events": []}]
)
def test_invalid_event_response_is_not_treated_as_no_events(replacement):
    """拒绝缺失或旧压缩事件响应；输入畸形响应，无返回，必须报错且不得调用原生复权。"""
    gateway = _FakeGateway()
    gateway.overrides["/data/split_dividend"] = replacement
    provider, _ = _client(gateway)
    with pytest.raises((ValueError, RuntimeError)):
        _price_request(provider, count=4)


def test_irrelevant_historical_event_is_not_parsed_as_current_adjustment_dependency():
    """窗口外历史事件缺前收盘不能阻断当前前复权；无输入，无返回，相关完整事件仍必须参与。"""
    gateway = _FakeGateway()
    old = {"date": "2015-04-15", "gift": -0.719675}
    gateway.overrides["/data/split_dividend"] = {
        "schema": "big-qmt-dividend-events/v1",
        "source": "ContextInfo.get_divid_factors",
        "events": [old, _event(_SECURITIES[0])],
        "raw_event_count": 2,
        "event_fields_complete": True,
        "history_completeness_verified": False,
    }
    provider, _ = _client(gateway)
    result = _price_request(provider, count=4, fields=["close"])
    assert result["close"].tolist() == [8.0, 8.0, 8.0, 8.0]


def test_irrelevant_2007_share_reform_does_not_block_recent_pre_adjustment():
    """近年前复权不受窗口外真实类型股改影响；无输入，无返回，2007字段只作反例不作算法输入。"""
    gateway = _FakeGateway()
    old = _share_reform_event("2007-06-20")
    gateway.overrides["/data/split_dividend"] = {
        "schema": "big-qmt-dividend-events/v1",
        "source": "ContextInfo.get_divid_factors",
        "events": [old, _event(_SECURITIES[0])],
        "raw_event_count": 2,
        "event_fields_complete": True,
        "history_completeness_verified": False,
    }
    provider, _ = _client(gateway)
    result = _price_request(provider, count=4, fields=["close"])
    assert result["close"].tolist() == [8.0, 8.0, 8.0, 8.0]


@pytest.mark.parametrize("selection", ["start", "count"])
def test_prefetched_previous_day_does_not_make_first_return_day_reform_relevant(selection):
    """预取前日不应扩大OHLC复权区间；输入窗口方式，无返回，首返回日股改在同日基准下无关。"""
    gateway = _FakeGateway()
    gateway.events[_SECURITIES[0]][0] = _share_reform_event(_EVENT_DATE)
    provider, _ = _client(gateway)
    options = {
        "fields": ["open", "high", "low", "close"],
        "pre_factor_ref_date": _EVENT_DATE,
        "end_date": _EVENT_DATE,
    }
    if selection == "start":
        options.update(start_date=_EVENT_DATE, count=None)
    else:
        options.update(count=1)

    result = _price_request(provider, **options)

    assert result.index.tolist() == [pd.Timestamp(_EVENT_DATE)]
    assert result.iloc[0].tolist() == [8.0, 8.1, 7.9, 8.0]


@pytest.mark.parametrize("fq", ["pre", "post"])
def test_relevant_share_reform_uses_only_expressed_cash_and_shares(fq):
    """股改仅按已表达现金份额复权；输入方式，无返回，断言手算结果且不读取原生因子。"""
    gateway = _FakeGateway()
    gateway.events[_SECURITIES[0]][0] = _share_reform_event(_EVENT_DATE)
    provider, _ = _client(gateway)
    result = _price_request(provider, count=4, fq=fq, fields=["close", "factor"])
    # 本合成窗口 C=10；(10-.009)/1.1 按两位报价得到 E=9.08。
    expected = [9.08, 9.08, 8.0, 8.0] if fq == "pre" else [10.0, 10.0, 8.81, 8.81]
    factors = [0.908, 0.908, 1.0, 1.0] if fq == "pre" else [1.0, 1.0, 10 / 9.08, 10 / 9.08]
    assert result["close"].tolist() == expected
    np.testing.assert_allclose(result["factor"], factors, rtol=1e-14)


@pytest.mark.parametrize("flag", [None, 0, 1, "true", "false"])
def test_share_reform_non_boolean_flag_still_fails(flag):
    """拒绝非布尔股改标识；输入错误值，无返回，不能因支持已表达权益而放松事件校验。"""
    gateway = _FakeGateway()
    gateway.events[_SECURITIES[0]][0]["share_reform"] = flag
    provider, _ = _client(gateway)
    with pytest.raises(RemoteServerError, match="股改标识不是布尔值"):
        _price_request(provider, count=4, fq="post")


def test_stock_post_with_2007_share_reform_still_requires_actual_previous_close():
    """股改post仍须真实前收盘；无输入，无返回，缺历史依赖明确失败而非重设原点。"""
    gateway = _FakeGateway()
    gateway.start_date = "1991-04-03"
    old = _share_reform_event("2007-06-20")
    gateway.events[_SECURITIES[0]].insert(0, old)
    provider, _ = _client(gateway)
    with pytest.raises((ValueError, RuntimeError), match="无法取得除权前实际交易日收盘"):
        _price_request(provider, count=4, fq="post")


def test_event_previous_close_skips_three_confirmed_suspended_days():
    """事件前连续三日停牌仍取真实收盘；无输入，无返回，辅助填充值99不得参与复权计算。"""
    gateway = _FakeGateway()
    security = _SECURITIES[0]
    event = _event(security)
    event["date"] = "2026-09-07"
    event["source_timestamp"] = str(
        int(pd.Timestamp(event["date"], tz="Asia/Shanghai").timestamp() * 1000)
    )
    gateway.events[security] = [event]
    paused_days = pd.to_datetime(["2026-09-02", "2026-09-03", "2026-09-04"])
    gateway.frames[(security, "1d")] = gateway.frames[(security, "1d")].drop(paused_days)
    gateway.suspension_facts = {(security, day): 1 for day in paused_days}
    provider, _ = _client(gateway)

    result = _price_request(
        provider,
        end_date="2026-09-01",
        count=1,
        fields=["close", "factor"],
        pre_factor_ref_date="2026-09-08",
    )

    assert result.index.tolist() == [pd.Timestamp("2026-09-01")]
    assert result.iloc[0].tolist() == [8.0, 0.8]
    assert any(
        path == "/data/history" and payload.get("fill_data") is True
        for path, payload in gateway.calls
    )


def test_successful_empty_event_window_is_usable_without_manual_completeness_gate():
    """合法空事件允许取数但不伪称全历史完整；无输入，无返回，验证原价及只读响应事实。"""
    gateway = _FakeGateway()
    gateway.events = {security: [] for security in _SECURITIES}
    provider, _ = _client(gateway)
    result = _price_request(provider, count=4, fields=["close", "factor"])
    assert result["factor"].tolist() == [1.0] * 4
    assert result["close"].tolist() == [10.0, 10.0, 8.0, 8.0]


@pytest.mark.parametrize(
    "field", ["cash_per_share", "gift", "transfer", "rights", "rights_price", "share_reform"]
)
def test_relevant_incomplete_event_fails_without_zero_filling(field):
    """相关事件缺字段必须失败；输入被删除字段，无返回，防止补零伪造合法事件。"""
    gateway = _FakeGateway()
    del gateway.events[_SECURITIES[0]][0][field]
    provider, _ = _client(gateway)
    with pytest.raises((ValueError, RuntimeError)):
        _price_request(provider, count=4)


@pytest.mark.parametrize(
    "path", ["/data/history", "/data/split_dividend", "/data/security_info", "/data/trade_days"]
)
def test_required_source_failure_propagates_without_alternative_source(path):
    """必需只读接口失败不能换源；输入故障路径，无返回，检查原始异常且无额外外部动作。"""
    gateway = _FakeGateway()
    gateway.overrides[path] = BigQmtGatewayError("测试事实不可用", code="SOURCE_FAILED")
    provider, _ = _client(gateway)
    with pytest.raises(RuntimeError, match="测试事实不可用"):
        _price_request(provider, fq="post", count=4)
    history_calls = [payload for action, payload in gateway.calls if action == "/data/history"]
    assert all(payload.get("fq") == "none" for payload in history_calls)
    if path == "/data/history":
        assert len(history_calls) == 1


@pytest.mark.parametrize("listing", ["1970-01-01", "0000-00-00", "0"])
def test_post_adjustment_rejects_missing_fixed_listing_origin(listing):
    """后复权拒绝无效上市原点；输入占位上市日，无返回，不允许以查询首行重新定基准。"""
    gateway = _FakeGateway()
    gateway.start_date = listing
    provider, _ = _client(gateway)
    with pytest.raises((ValueError, RuntimeError)):
        _price_request(provider, fq="post", count=4)


@pytest.mark.parametrize("defect", ["nan", "duplicate", "missing_previous_close"])
def test_invalid_raw_facts_are_not_silently_repaired(defect):
    """原始事实不合法不能静默修补；输入缺陷类型，无返回，相关计算必须明确失败。"""
    gateway = _FakeGateway()
    key = (_SECURITIES[0], "1d")
    frame = gateway.frames[key]
    if defect == "nan":
        frame.loc["2026-09-02", "close"] = np.nan
    elif defect == "duplicate":
        gateway.frames[key] = pd.concat([frame.loc[:"2026-09-02"], frame.loc["2026-09-02":]])
    else:
        gateway.frames[key] = frame.loc[frame.index >= pd.Timestamp(_EVENT_DATE)]
    provider, _ = _client(gateway)
    with pytest.raises((ValueError, RuntimeError)):
        _price_request(provider, count=4)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"frequency": "wrong"},
        {"count": 0},
        {"count": -1},
        {"count": True},
        {"fq": "follow"},
        {"fq": "front_ratio"},
        {"fields": ["not_a_price_field"]},
        {"start_date": "2026-09-01", "count": 2},
    ],
)
def test_invalid_public_request_fails_before_reading_price_or_event_facts(kwargs):
    """无效请求在行情事件取数前失败；输入错误参数，无返回，允许只读证券类型决定适用范围。"""
    gateway = _FakeGateway()
    provider, _ = _client(gateway)
    with pytest.raises((ValueError, RuntimeError)):
        _price_request(provider, **kwargs)
    assert all(path == "/data/security_info" for path, _ in gateway.calls)


@pytest.mark.parametrize("boundary", ["start_date", "end_date"])
@pytest.mark.parametrize("value", [False, 0])
def test_explicit_false_or_zero_date_is_not_replaced_with_default_window(boundary, value):
    """显式布尔或零日期不能当缺省；输入边界和非法值，无返回，禁止改为全历史或当天查询。"""
    gateway = _FakeGateway()
    provider, _ = _client(gateway)
    options = {boundary: value}
    if boundary == "start_date":
        options["count"] = None

    with pytest.raises((ValueError, RuntimeError)):
        _price_request(provider, **options)

    assert all(path == "/data/security_info" for path, _ in gateway.calls)


@pytest.mark.parametrize("frequency", ["", "   "])
def test_explicit_empty_frequency_is_not_replaced_with_daily(frequency):
    """显式空周期不是daily缺省别名；输入空值，无返回，禁止默默降为日线后正常取数。"""
    gateway = _FakeGateway()
    provider, _ = _client(gateway)

    with pytest.raises((ValueError, RuntimeError)):
        _price_request(provider, frequency=frequency)

    assert all(path == "/data/security_info" for path, _ in gateway.calls)


@pytest.mark.parametrize("context", [False, True])
def test_public_get_price_reaches_real_adapter_without_double_adjustment(monkeypatch, context):
    """公开入口进入真实适配器只处理一次；输入替换器与上下文开关，无返回，不启动任何服务。"""
    provider, requests = _client(_FakeGateway())
    monkeypatch.setattr(data_api, "_ensure_auth", lambda: provider)
    monkeypatch.setattr(data_api, "_get_default_provider", lambda: provider)
    monkeypatch.setattr(
        data_api,
        "_current_context",
        SimpleNamespace(current_dt=datetime(2026, 9, 4, 15)) if context else None,
    )
    set_option("use_real_price", True)
    set_option("avoid_future_data", False)
    monkeypatch.setattr(big_qmt_module, "_history_today", lambda: date(2026, 9, 4))

    result = data_api.get_price(
        _SECURITIES[0], end_date=datetime(2026, 9, 2, 15), count=1, fields=["close"], fq="pre"
    )

    assert len(requests) == 1
    assert result["close"].tolist() == [8.0]
    if context:
        assert requests[0]["pre_factor_ref_date"] == date(2026, 9, 4)


def test_public_failure_remains_empty_without_changing_reference_or_using_native(monkeypatch):
    """公开错误仍为空表但不能改变基准；输入替换器，无返回，断言所有重试同参数且无替代行情。"""
    gateway = _FakeGateway()
    gateway.overrides["/data/split_dividend"] = BigQmtGatewayError(
        "缺失除权事实", code="SOURCE_FAILED"
    )
    provider, requests = _client(gateway)
    monkeypatch.setattr(data_api, "_ensure_auth", lambda: provider)
    monkeypatch.setattr(data_api, "_get_default_provider", lambda: provider)
    monkeypatch.setattr(
        data_api, "_current_context", SimpleNamespace(current_dt=datetime(2026, 9, 4, 15))
    )
    set_option("use_real_price", True)
    set_option("avoid_future_data", False)

    result = data_api.get_price(
        _SECURITIES[0], end_date=datetime(2026, 9, 2, 15), count=1, fields=["close"]
    )

    assert result.empty
    assert len(requests) == 2
    assert all(
        row["fq"] == "pre" and row["pre_factor_ref_date"] == date(2026, 9, 4) for row in requests
    )


def test_public_invalid_share_reform_uses_real_remote_error_semantics_without_native_fallback(
    monkeypatch,
):
    """非法股改标识按远端wire返回空表；输入替换器，无返回，保留同fq/ref重试且无原生成功。"""
    gateway = _FakeGateway()
    gateway.events[_SECURITIES[0]][0] = _share_reform_event(_EVENT_DATE)
    gateway.events[_SECURITIES[0]][0]["share_reform"] = "true"
    provider, requests = _client(gateway)
    monkeypatch.setattr(data_api, "_ensure_auth", lambda: provider)
    monkeypatch.setattr(data_api, "_get_default_provider", lambda: provider)
    monkeypatch.setattr(
        data_api, "_current_context", SimpleNamespace(current_dt=datetime(2026, 9, 4, 15))
    )
    set_option("use_real_price", True)
    set_option("avoid_future_data", False)

    result = data_api.get_price(
        _SECURITIES[0], end_date=datetime(2026, 9, 2, 15), count=1, fields=["close"]
    )

    assert result.empty
    assert list(result.columns) == ["close"]
    assert len(requests) == 2
    assert all(
        row["fq"] == "pre" and row["pre_factor_ref_date"] == date(2026, 9, 4) for row in requests
    )
    assert all(
        payload.get("fq") == "none" for path, payload in gateway.calls if path == "/data/history"
    )
