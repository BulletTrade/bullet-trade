"""大 QMT 分钟缓存边界的隔离回归测试。

作者：BruceLee
职责：验证冷暖缓存、完成分钟边界和现场原始样本；不修改复权或聚合算法。
输入：既有合成网关、脱敏原始分钟、固定上海时钟；输出：时间轴与逐格断言。
上下游：真实 RemoteQmtProvider 和 BigQmtDataAdapter，内存 helper 替身。
环境：本地 pytest/pandas，不启动 QMT、网络、策略、交易或缓存下载。
"""

import asyncio
import json
from pathlib import Path

import pandas as pd
import pytest
from test_big_qmt_history_standardization import _client, _FakeGateway, _price_request

from bullet_trade.data.providers.remote_qmt import _dataframe_from_payload
from bullet_trade.data.qmt_adjustment import AdjustmentError
from bullet_trade.server.adapters import big_qmt as module
from bullet_trade.server.adapters.qmt import dataframe_to_payload

pytestmark = pytest.mark.unit
_FIELDS = ["open", "high", "low", "close", "volume", "money"]
_SECURITY = "000001.XSHE"


@pytest.fixture(autouse=True)
def fixed_clock(monkeypatch):
    """固定测试默认上海时钟；输入夹具，返回 None，避免样本日期使回归依赖运行当天。"""

    def frozen_now():
        """读取样本之后的固定时间；无输入，返回时间戳，无外部副作用。"""
        return pd.Timestamp("2026-09-10 18:00:00")

    monkeypatch.setattr(module, "_history_now", frozen_now, raising=False)


class _ColdGateway(_FakeGateway):
    """模拟整分下载漏边界的 helper；持有内存行情与已暖窗口，不调用任何外部接口。"""

    def __init__(self):
        """初始化冷缓存；无输入，返回 None，仅增加内存集合。"""
        super().__init__()
        self.warmed = set()

    async def post(self, path, payload=None, *, timeout_seconds=None):
        """重放缓存边界；输入请求，返回 wire，末秒窗口仅使本地模拟缓存变暖。"""
        result = await super().post(path, payload, timeout_seconds=timeout_seconds)
        if path == "/data/history" and payload.get("frequency") == "1m":
            end = pd.Timestamp(payload["end"])
            key = (payload["security"], end.floor("min"))
            if end.second == 0 and key not in self.warmed:
                frame = _dataframe_from_payload(result)
                result = dataframe_to_payload(
                    frame.loc[frame.index != end.strftime("%Y%m%d%H%M%S")]
                )
            elif end.second == 59:
                self.warmed.add(key)
        return result


@pytest.mark.parametrize("security", [_SECURITY, "510500.XSHG"])
@pytest.mark.parametrize("frequency", ["1m", "5m", "60m"])
@pytest.mark.parametrize("fq", [None, "pre", "post"])
@pytest.mark.parametrize("window", ["start", "count"])
@pytest.mark.parametrize("end", ["09:31:00", "10:10:00", "11:30:00", "13:01:00", "15:00:00"])
def test_cold_and_warm_windows_match(security, frequency, fq, window, end):
    """核对冷暖窗口；输入证券周期复权及边界，无返回，逐格比较且保证缺行修复不靠额外接口。"""
    cold = _ColdGateway()
    provider, _ = _client(cold)
    warm, _ = _client(_FakeGateway())
    options = dict(frequency=frequency, fq=fq, end_date="2026-09-04 " + end)
    (
        options.update(count=2)
        if window == "count"
        else options.update(start_date="2026-09-04 09:30:00", count=None)
    )
    expected = _price_request(warm, security, **options)
    for _ in range(2):
        actual = _price_request(provider, security, **options)
        pd.testing.assert_frame_equal(actual, expected)
        assert not actual.empty
    assert all(
        request["end"].endswith(":59")
        for path, request in cold.calls
        if path == "/data/history" and request["frequency"] == "1m"
    )


@pytest.mark.parametrize("now", ["10:10:00", "10:10:30", "10:10:59"])
@pytest.mark.parametrize("end", [None, "2026-09-04", "2026-09-04 10:11:00"])
def test_only_completed_minutes_at_request_clock(monkeypatch, now, end):
    """固定上海时钟验证当前与未来截止；输入时钟和截止，无返回，未完成10:11不得进入结果。"""

    def frozen_now():
        """读取本例固定上海时间；无输入，返回时间戳，不读取系统时钟。"""
        return pd.Timestamp("2026-09-04 " + now)

    monkeypatch.setattr(module, "_history_now", frozen_now, raising=False)
    provider, _ = _client(_FakeGateway())
    actual = _price_request(provider, frequency="1m", fq=None, end_date=end, count=2)
    assert actual.index.tolist() == list(pd.to_datetime(["2026-09-04 10:09", "2026-09-04 10:10"]))


@pytest.mark.parametrize("seconds", ["00", "01", "30", "59"])
def test_seconds_do_not_change_public_completed_bar(seconds):
    """保持含秒请求语义；输入截止秒数，无返回，最后一根始终是10:10而非10:11。"""
    provider, _ = _client(_ColdGateway())
    result = _price_request(
        provider, frequency="1m", fq=None, end_date="2026-09-04 10:10:" + seconds, count=1
    )
    assert result.index.tolist() == [pd.Timestamp("2026-09-04 10:10")]


def test_missing_active_minute_still_fails():
    """检查真实缺行不误判停牌；无输入返回，去掉所需分钟后必须明确失败。"""
    gateway = _ColdGateway()
    gateway.frames[(_SECURITY, "1m")] = gateway.frames[(_SECURITY, "1m")].drop(
        pd.Timestamp("2026-09-04 10:09")
    )
    provider, _ = _client(gateway)
    with pytest.raises(RuntimeError, match="没有明确停牌事实"):
        _price_request(provider, frequency="1m", fq=None, end_date="2026-09-04 10:10", count=2)


@pytest.mark.parametrize("offset,raises", [(30, False), (60, True)])
def test_fetch_window_validation_precedes_public_clip(offset, raises):
    """验证扩大窗口先验后裁剪；输入附加行偏移和预期，无返回，真正越界不能被静默掩盖。"""
    gateway = _FakeGateway()
    start, end = pd.Timestamp("2026-09-04 10:09"), pd.Timestamp("2026-09-04 10:10")
    raw = gateway.frames[(_SECURITY, "1m")].loc[start:end, _FIELDS].copy()
    raw.loc[end + pd.Timedelta(seconds=offset)] = raw.iloc[-1]
    gateway.overrides["/data/history"] = dataframe_to_payload(raw)
    adapter = module.BigQmtDataAdapter(gateway)
    if raises:
        with pytest.raises(AdjustmentError, match="依赖窗口之外"):
            asyncio.run(adapter._history_raw(_SECURITY, "1m", start, end, _FIELDS))
    else:
        result = asyncio.run(adapter._history_raw(_SECURITY, "1m", start, end, _FIELDS))
        pd.testing.assert_frame_equal(result, raw.loc[start:end], check_names=False)


@pytest.mark.parametrize("frequency", ["1m", "5m", "60m"])
@pytest.mark.parametrize("security", [_SECURITY, "510500.XSHG"])
def test_saved_intraday_raw_matches_independent_arithmetic(frequency, security):
    """独立手算现场竞价与分钟聚合；输入周期证券，无返回，不能把公开接口输出当作正确性依据。"""
    path = Path(__file__).parents[1] / "fixtures" / "big_qmt_intraday_boundary_20260909.json"
    case = next(
        case
        for case in json.loads(path.read_text("utf-8"))["cases"]
        if case["security"] == security
    )
    raw = pd.DataFrame(case["result"]["records"], columns=case["result"]["columns"]).set_index(
        "stime"
    )
    raw.index = pd.to_datetime(raw.index, format="%Y%m%d%H%M%S")
    gateway = _ColdGateway()
    gateway.frames[(security, "1m")] = raw
    provider, _ = _client(gateway)
    actual = _price_request(
        provider,
        security,
        frequency=frequency,
        fq=None,
        count=None,
        start_date="2026-09-09 09:30",
        end_date="2026-09-09 10:10",
    )
    expected = raw[_FIELDS].copy()
    auction, first = expected.iloc[0], expected.iloc[1].copy()
    first["open"] = auction["open"]
    first["high"] = max(first["high"], auction["high"])
    first["low"] = min(first["low"], auction["low"])
    first[["volume", "money"]] += auction[["volume", "money"]]
    expected.iloc[1] = first
    expected = expected.iloc[1:]
    size = int(frequency[:-1])
    rows, labels = [], []
    for start in range(0, len(expected), size):
        group = expected.iloc[start : start + size]
        rows.append(
            [
                group.open.iloc[0],
                group.high.max(),
                group.low.min(),
                group.close.iloc[-1],
                group.volume.sum(),
                group.money.sum(),
            ]
        )
        labels.append(group.index[-1])
    expected = pd.DataFrame(rows, columns=_FIELDS, index=pd.DatetimeIndex(labels))
    pd.testing.assert_frame_equal(
        actual, expected, check_names=False, check_dtype=False, check_exact=True
    )


@pytest.mark.parametrize("include_now", [False, True])
def test_get_bars_remains_explicitly_unsupported(include_now):
    """固定已有能力边界；输入include_now，无返回，只触发未实现异常，不生成伪造bars。"""
    provider, _ = _client(_FakeGateway())
    with pytest.raises(NotImplementedError):
        provider.get_bars(_SECURITY, 2, unit="5m", include_now=include_now)


@pytest.mark.parametrize(
    "now,last",
    [
        ("2026-09-04 09:29:59", "2026-09-03 15:00"),
        ("2026-09-04 09:30:30", "2026-09-03 15:00"),
        ("2026-09-04 12:15:00", "2026-09-04 11:30"),
        ("2026-09-04 13:00:59", "2026-09-04 11:30"),
        ("2026-09-04 15:30:00", "2026-09-04 15:00"),
        ("2026-09-06 12:00:00", "2026-09-04 15:00"),
    ],
)
@pytest.mark.parametrize("frequency", ["1m", "5m", "60m"])
def test_session_breaks_and_weekend_keep_count_semantics(monkeypatch, now, last, frequency):
    """检查非交易时段和跨日count；输入时钟末标签周期，无返回，不制造午休竞价或周末bar。"""

    def frozen_now():
        """读取本例时钟；无输入，返回时间戳，不访问外部时钟。"""
        return pd.Timestamp(now)

    monkeypatch.setattr(module, "_history_now", frozen_now)
    provider, _ = _client(_ColdGateway())
    result = _price_request(provider, frequency=frequency, fq=None, end_date=None, count=2)
    assert len(result) == 2
    assert result.index[-1] == pd.Timestamp(last)


def test_future_start_returns_empty_without_history_read(monkeypatch):
    """拒绝未完成区间产生行情；输入夹具，无返回，合法未来范围返回空表且不下载基础行情。"""

    def frozen_now():
        """读取固定盘中时钟；无输入，返回时间戳，无外部副作用。"""
        return pd.Timestamp("2026-09-04 10:10:30")

    monkeypatch.setattr(module, "_history_now", frozen_now)
    gateway = _FakeGateway()
    provider, _ = _client(gateway)
    result = _price_request(
        provider,
        frequency="1m",
        fq=None,
        count=None,
        start_date="2026-09-04 10:11",
        end_date="2026-09-04 11:00",
    )
    assert result.empty
    assert not any(path == "/data/history" for path, _ in gateway.calls)


def test_daily_dependency_window_is_not_expanded():
    """保留日线取数合同；无输入返回，断言日线仍传日期且未增加末秒补偿。"""
    gateway = _FakeGateway()
    provider, _ = _client(gateway)
    result = _price_request(provider, frequency="1d", fq=None, end_date="2026-09-04", count=1)
    assert result.index.tolist() == [pd.Timestamp("2026-09-04")]
    requests = [request for path, request in gateway.calls if path == "/data/history"]
    assert len(requests) == 1
    assert requests[0]["end"] == "2026-09-04"
