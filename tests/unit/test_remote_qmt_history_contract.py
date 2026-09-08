"""远程 QMT 历史请求的编码及共享 MiniQMT 适配契约测试。

作者：BruceLee
输入：固定行情、日期/周期/停牌/形状参数及本地假连接。
输出：逐值请求断言和 DataFrame 无损回传断言。
上下游：RemoteQmtProvider → data.history → QmtDataAdapter → 假 MiniQMT provider。
环境约定：不初始化原生 SDK、不连接网络、不启动服务；不验证或修改复权算法。
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from itertools import product
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from bullet_trade.data.providers.remote_qmt import RemoteQmtProvider
from bullet_trade.server.adapters.qmt import QmtDataAdapter, dataframe_to_payload

pytestmark = pytest.mark.unit


def _sample_frame() -> pd.DataFrame:
    """生成两根固定行情；无输入，返回新表，不读取行情或改变全局状态。"""
    return pd.DataFrame(
        {
            "open": [8.271, 8.285],
            "close": [8.279, 8.291],
            "volume": [123400.0, 234500.0],
            "money": [1020942.3, 1943922.7],
        },
        index=pd.DatetimeIndex(["2026-07-15 10:30:00", "2026-07-15 11:30:00"], name="time"),
    )


def _make_provider():
    """建立不联网客户端；无输入，返回客户端及请求 Mock，仅设置测试对象状态。"""
    provider = object.__new__(RemoteQmtProvider)
    request = Mock(return_value=dataframe_to_payload(_sample_frame()))
    provider._connection = SimpleNamespace(request=request)
    return provider, request


def test_history_default_payload_is_explicit_and_response_is_unchanged():
    """验证默认完整请求；无输入，无返回，仅对假连接调用及原样行情作断言。"""
    provider, request = _make_provider()

    result = provider.get_price("510500.XSHG")

    request.assert_called_once_with(
        "data.history",
        {
            "security": "510500.XSHG",
            "start": None,
            "end": None,
            "frequency": "daily",
            "fields": None,
            "skip_paused": False,
            "fq": "pre",
            "count": None,
            "panel": True,
            "fill_paused": True,
            "pre_factor_ref_date": None,
        },
    )
    pd.testing.assert_frame_equal(result, _sample_frame())


@pytest.mark.parametrize("security", ["510500.XSHG", ["510500.XSHG", "000001.XSHE"]])
@pytest.mark.parametrize("skip_paused,panel,fill_paused", list(product([False, True], repeat=3)))
def test_history_boolean_options_are_forwarded_without_mutation(
    security, skip_paused, panel, fill_paused
):
    """验证单双证券和布尔组合；输入参数化选项，无返回，不改调用方列表或行情。"""
    provider, request = _make_provider()
    fields = ["close", "volume", "money"]
    original_security = security[:] if isinstance(security, list) else security

    result = provider.get_price(
        security,
        end_date="2026-07-15 11:30:00",
        frequency="60m",
        fields=fields,
        skip_paused=skip_paused,
        panel=panel,
        fill_paused=fill_paused,
        count=2,
    )

    request.assert_called_once()
    action, payload = request.call_args.args
    assert action == "data.history"
    assert payload["security"] == original_security
    assert payload["fields"] == ["close", "volume", "money"]
    assert payload["skip_paused"] is skip_paused
    assert payload["panel"] is panel
    assert payload["fill_paused"] is fill_paused
    assert payload["count"] == 2
    assert security == original_security
    assert fields == ["close", "volume", "money"]
    pd.testing.assert_frame_equal(result, _sample_frame())


@pytest.mark.parametrize(
    "frequency,keep_time",
    [
        ("1h", True),
        (" 1H ", True),
        ("minute", True),
        ("min", True),
        ("1m", True),
        ("5m", True),
        ("15m", True),
        ("30m", True),
        ("60m", True),
        ("15minutes", True),
        ("daily", False),
        ("1d", False),
        ("1w", False),
        ("1mon", False),
        ("monthly", False),
        ("hour", False),
        ("hourly", False),
        ("2h", False),
    ],
)
def test_history_datetime_encoding_preserves_existing_frequency_behavior(frequency, keep_time):
    """验证仅补原生 1h 保时；输入周期及预期，无返回，不改写或承诺后端周期支持。"""
    provider, request = _make_provider()
    start = datetime(2026, 7, 14, 10, 31, 22)
    end = datetime(2026, 7, 15, 11, 32, 23)
    reference = datetime(2026, 9, 7, 14, 50, 24)

    provider.get_price(
        "510500.XSHG",
        start_date=start,
        end_date=end,
        pre_factor_ref_date=reference,
        frequency=frequency,
    )

    payload = request.call_args.args[1]
    date_format = "%Y-%m-%d %H:%M:%S" if keep_time else "%Y-%m-%d"
    assert payload["start"] == start.strftime(date_format)
    assert payload["end"] == end.strftime(date_format)
    assert payload["pre_factor_ref_date"] == reference.strftime(date_format)
    assert payload["frequency"] == frequency
    request.assert_called_once()


@pytest.mark.parametrize("frequency", ["1h", "60m", "daily"])
@pytest.mark.parametrize("value", [None, "2026-07-15 11:32:23", date(2026, 7, 15)])
def test_history_non_datetime_inputs_keep_the_existing_protocol(frequency, value):
    """验证字符串、date、空值不被客户端重写；输入周期及值，无返回，无外部副作用。"""
    provider, request = _make_provider()

    provider.get_price(
        "510500.XSHG",
        start_date=value,
        end_date=value,
        pre_factor_ref_date=value,
        frequency=frequency,
    )

    payload = request.call_args.args[1]
    assert payload["start"] == value
    assert payload["end"] == value
    assert payload["pre_factor_ref_date"] == value


@pytest.mark.parametrize("fq", [None, "none", "pre", "post"])
def test_history_adjustment_mode_is_not_normalized_or_applied_in_client(fq):
    """验证复权方式原样透传且不重复计算；输入 fq，无返回，仅使用假行情。"""
    provider, request = _make_provider()

    result = provider.get_price("510500.XSHG", fq=fq)

    assert request.call_args.args[1]["fq"] == fq
    pd.testing.assert_frame_equal(result, _sample_frame())


def test_history_remote_failure_is_not_retried_with_different_arguments():
    """验证失败原样向上传播；无输入，无返回，假连接只抛错一次、不调用其他接口。"""
    provider, request = _make_provider()
    request.side_effect = RuntimeError("history unavailable")

    with pytest.raises(RuntimeError, match="history unavailable"):
        provider.get_price("510500.XSHG", fq="pre", pre_factor_ref_date="2026-09-07")

    request.assert_called_once()
    assert request.call_args.args[1]["pre_factor_ref_date"] == "2026-09-07"


@pytest.mark.parametrize("frequency", ["1m", "60m", "1d"])
@pytest.mark.parametrize("fq", [None, "none", "pre", "post"])
@pytest.mark.parametrize("panel", [False, True])
def test_shared_miniqmt_adapter_receives_options_and_returns_prices_once(frequency, fq, panel):
    """验证共用 MiniQMT 参数链；输入已有周期/fq/形状，无返回，不初始化 SDK 或线程池。"""
    expected = _sample_frame()
    if not panel:
        expected = expected.reset_index().assign(code="510500.XSHG")
    else:
        expected.columns = pd.MultiIndex.from_tuples(
            [(field, "510500.XSHG") for field in expected.columns], names=["field", "code"]
        )
    original = expected.copy(deep=True)
    expected_wire = expected.copy(deep=True)
    if not panel:
        # 既有协议仅恢复索引类型，普通 datetime 数据列保留 ISO 字符串。
        expected_wire["time"] = expected_wire["time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    local_get_price = Mock(return_value=expected)
    adapter = object.__new__(QmtDataAdapter)
    adapter.provider = SimpleNamespace(get_price=local_get_price)

    async def _run_now(call):
        """同步执行注入的假行情函数；输入回调，返回其结果，不启线程或访问 QMT。"""
        return call()

    def _request(action, payload):
        """模拟传输给真实适配方法；输入动作和字典，返回 wire 结果，不启动网络服务。"""
        assert action == "data.history"
        return asyncio.run(adapter.get_history(payload))

    adapter._run_guarded_qmt_call = _run_now
    provider, request = _make_provider()
    request.side_effect = _request
    end = datetime(2026, 7, 15, 11, 30, 0)
    reference = datetime(2026, 9, 7, 14, 50, 0)
    fields = ["open", "close", "volume", "money"]

    result = provider.get_price(
        ["510500.XSHG"],
        end_date=end,
        frequency=frequency,
        fields=fields,
        fq=fq,
        count=2,
        skip_paused=True,
        panel=panel,
        fill_paused=False,
        pre_factor_ref_date=reference,
    )

    date_format = "%Y-%m-%d" if frequency == "1d" else "%Y-%m-%d %H:%M:%S"
    local_get_price.assert_called_once_with(
        ["510500.XSHG"],
        count=2,
        start_date=None,
        end_date=end.strftime(date_format),
        frequency=frequency,
        fq=fq,
        fields=fields,
        skip_paused=True,
        panel=panel,
        fill_paused=False,
        pre_factor_ref_date=reference.strftime(date_format),
    )
    request.assert_called_once()
    pd.testing.assert_frame_equal(result, expected_wire)
    pd.testing.assert_frame_equal(expected, original)
