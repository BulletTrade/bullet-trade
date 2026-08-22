"""
作者: BruceLee
文件职责: 用纯 Python fake SDK 验证华鑫 XMD JSONL sidecar 的线程、协议和行情字段合同。
主要输入: 内存 JSONL 流、fake xmdapi、同步模拟回调及白名单命令。
主要输出: pytest 断言，覆盖空域登录、沪深订阅、tick、队列溢出和主线程 Release。
上游关系: 华鑫 XMD sidecar 的定向单元测试，不通过 __init__ 暴露新入口。
下游关系: 仅调用 xmd_sidecar.py；不加载厂商 SDK、不联网、不访问凭据或交易接口。
关键环境或配置: fake SDK 不创建文件；测试同时以 Python 3.7 grammar 解析正式 sidecar。
"""

import ast
import io
import json
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

from bullet_trade.integrations.huaxin.xmd_sidecar import (
    XmdJsonlSidecar,
    XmdSidecarError,
    _normalise_security,
    load_xmdapi,
)

_TEST_XMD_FRONT = "tcp://127.0.0.1:9402"


class _FakeSpiBase:
    """模拟 SWIG 生成的 CTORATstpXMdSpi 基类。"""

    def __init__(self) -> None:
        """建立无状态 fake SPI 基类。

        返回:
            无。
        """


class _FakeLoginRequest:
    """表示必须保持空域的 fake 登录请求。"""


class _FakeResponse:
    """提供 SDK 回调所需的 ErrorID 标量。"""

    def __init__(self, error_id: int = 0) -> None:
        """保存响应错误码。

        参数:
            error_id: 零表示成功。
        返回:
            无。
        """

        self.ErrorID = int(error_id)


class _FakeSecurity:
    """提供订阅响应所需的证券和交易所标量。"""

    def __init__(self, code: bytes, exchange: str) -> None:
        """保存 fake 证券代码和交易所。

        参数:
            code: 六位证券代码 bytes。
            exchange: fake ExchangeID。
        返回:
            无。
        """

        self.SecurityID = code
        self.ExchangeID = exchange


class _FakeTick:
    """提供一条覆盖全部公开字段的 fake L1 行情。"""

    def __init__(
        self,
        code: bytes = b"511880",
        exchange: str = "1",
        volume: int = 99_267_162,
    ) -> None:
        """初始化稳定的华鑫 L1 快照字段。

        参数:
            code: 六位证券代码。
            exchange: fake ExchangeID。
            volume: 累计成交量。
        返回:
            无。
        """

        self.SecurityID = code
        self.ExchangeID = exchange
        self.TradingDay = b"20260817"
        self.UpdateTime = b"14:33:51"
        self.UpdateMillisec = 779
        self.LastPrice = 100.705
        self.BidPrice1 = 100.704
        self.AskPrice1 = 100.705
        self.BidVolume1 = 4_087_900
        self.AskVolume1 = 3_270_700
        self.UpperLimitPrice = 110.770
        self.LowerLimitPrice = 90.630
        self.Volume = int(volume)
        self.Turnover = 9_995_123_456.78


class _FakeApi:
    """同步触发回调并记录所有 SDK 控制调用的 fake XMD API。"""

    def __init__(self) -> None:
        """初始化调用记录和 SPI 引用。

        返回:
            无。
        """

        self.spi = None
        self.front = ""
        self.login_requests = []  # type: List[Any]
        self.subscribe_calls = []  # type: List[Tuple[Tuple[bytes, ...], str, int]]
        self.unsubscribe_calls = []  # type: List[Tuple[Tuple[bytes, ...], str, int]]
        self.release_count = 0
        self.release_thread: Optional[int] = None
        self.emit_subscription_response = True
        self.emit_unsubscription_response = True

    def RegisterSpi(self, spi: Any) -> None:
        """保存 sidecar 构造的 fake SPI。

        参数:
            spi: 回调对象。
        返回:
            无。
        """

        self.spi = spi

    def RegisterFront(self, front: str) -> None:
        """记录固定 TCP 行情前置。

        参数:
            front: 行情前置地址。
        返回:
            无。
        """

        self.front = str(front)

    def Init(self) -> None:
        """同步模拟 native 连接完成回调。

        返回:
            无。
        """

        self.spi.OnFrontConnected()

    def ReqUserLogin(self, request: Any, request_id: int) -> int:
        """记录空域请求并同步返回登录成功。

        参数:
            request: fake 登录请求对象。
            request_id: 请求号。
        返回:
            SDK 成功码零。
        """

        self.login_requests.append(request)
        self.spi.OnRspUserLogin(None, _FakeResponse(0), request_id)
        return 0

    def SubscribeMarketData(self, securities: List[bytes], exchange: str) -> int:
        """记录主线程订阅并同步返回成功回调。

        参数:
            securities: 单次证券代码集合。
            exchange: fake ExchangeID。
        返回:
            SDK 成功码零。
        """

        self.subscribe_calls.append((tuple(securities), exchange, threading.get_ident()))
        if self.emit_subscription_response:
            self.spi.OnRspSubMarketData(_FakeSecurity(securities[0], exchange), _FakeResponse(0))
        return 0

    def UnSubscribeMarketData(self, securities: List[bytes], exchange: str) -> int:
        """记录主线程解除订阅并同步返回成功回调。

        参数:
            securities: 单次证券代码集合。
            exchange: fake ExchangeID。
        返回:
            SDK 成功码零。
        """

        self.unsubscribe_calls.append((tuple(securities), exchange, threading.get_ident()))
        if self.emit_unsubscription_response:
            self.spi.OnRspUnSubMarketData(_FakeSecurity(securities[0], exchange), _FakeResponse(0))
        return 0

    def Release(self) -> None:
        """记录唯一一次 Release 及其线程。

        返回:
            无。
        """

        self.release_count += 1
        self.release_thread = threading.get_ident()

    def emit_tick(self, tick: _FakeTick) -> None:
        """模拟 native 行情线程提交一条 tick 回调。

        参数:
            tick: fake L1 行情对象。
        返回:
            无。
        """

        self.spi.OnRtnMarketData(tick)


class _FakeXmdApi:
    """提供 sidecar 所需最小 SWIG 模块表面的 fake xmdapi。"""

    TORA_TSTP_EXD_SSE = "1"
    TORA_TSTP_EXD_SZSE = "2"
    CTORATstpXMdSpi = _FakeSpiBase
    CTORATstpReqUserLoginField = _FakeLoginRequest

    def __init__(self) -> None:
        """创建可由测试直接检查的 fake API 实例。

        返回:
            无。
        """

        self.api = _FakeApi()

    def CTORATstpXMdApi_GetApiVersion(self) -> str:
        """返回与现场探针一致的 fake 版本字符串。

        返回:
            fake API 版本。
        """

        return "1.0.5_20230210.14:00:00"

    def CTORATstpXMdApi_CreateTstpXMdApi(self) -> _FakeApi:
        """返回唯一 fake API，不创建文件或网络连接。

        返回:
            fake API 实例。
        """

        return self.api


def _json_lines(stream: io.StringIO) -> List[Dict[str, Any]]:
    """把内存 JSONL 输出解析为对象列表。

    参数:
        stream: sidecar 输出的内存文本流。
    返回:
        逐行解析后的 JSON 对象。
    """

    return [json.loads(line) for line in stream.getvalue().splitlines() if line]


def _started_sidecar(
    queue_capacity: int = 1024,
) -> Tuple[XmdJsonlSidecar, _FakeXmdApi, io.StringIO]:
    """创建并启动一个完全内存化的 fake sidecar。

    参数:
        queue_capacity: 回调队列容量。
    返回:
        sidecar、fake 模块和输出流三元组。
    """

    module = _FakeXmdApi()
    output = io.StringIO()
    sidecar = XmdJsonlSidecar(
        sdk_dir="/explicit/fake-sdk",
        front=_TEST_XMD_FRONT,
        output_stream=output,
        xmdapi_module=module,
        queue_capacity=queue_capacity,
    )
    sidecar.start()
    return sidecar, module, output


@pytest.mark.unit
def test_start_uses_explicit_front_empty_login_and_main_thread_release() -> None:
    """验证显式前置、空域登录以及主线程唯一 Release。

    返回:
        无；调用与 JSONL 状态符合合同即通过。
    """

    owner_thread = threading.get_ident()
    sidecar, module, output = _started_sidecar()

    assert module.api.front == _TEST_XMD_FRONT
    assert len(module.api.login_requests) == 1
    assert vars(module.api.login_requests[0]) == {}
    events = _json_lines(output)
    assert events[0]["type"] == "ready"
    assert events[0]["api_version"] == "1.0.5_20230210.14:00:00"
    assert events[0]["connected"] is True
    assert events[0]["logged_in"] is True

    sidecar.stop()
    sidecar.stop()

    assert module.api.release_count == 1
    assert module.api.release_thread == owner_thread
    assert {event["type"] for event in _json_lines(output)} == {"ready"}


@pytest.mark.unit
def test_subscribe_sse_szse_and_emit_complete_tick_only_from_main_thread() -> None:
    """验证沪深常量分离、回调不直接输出以及完整 L1 tick 合同。

    返回:
        无；订阅调用和 tick JSON 字段符合预期即通过。
    """

    owner_thread = threading.get_ident()
    sidecar, module, output = _started_sidecar()
    sidecar.handle_command(
        {"op": "subscribe", "request_id": "sub-sh", "security": "511880", "exchange": "SSE"}
    )
    sidecar.handle_command(
        {"op": "subscribe", "request_id": "sub-sz", "security": "000001", "exchange": "SZSE"}
    )
    sidecar.drain_events()

    assert module.api.subscribe_calls == [
        ((b"511880",), "1", owner_thread),
        ((b"000001",), "2", owner_thread),
    ]
    before_callback = output.getvalue()
    module.api.emit_tick(_FakeTick())
    assert output.getvalue() == before_callback

    assert sidecar.drain_events() == 1
    tick = [event for event in _json_lines(output) if event["type"] == "tick"][-1]
    assert tick == {
        "type": "tick",
        "security": "511880",
        "exchange": "SSE",
        "TradingDay": "20260817",
        "UpdateTime": "14:33:51",
        "Millisec": 779,
        "Last": 100.705,
        "Bid1": 100.704,
        "Ask1": 100.705,
        "BidVolume1": 4_087_900,
        "AskVolume1": 3_270_700,
        "UpperLimit": 110.770,
        "LowerLimit": 90.630,
        "Volume": 99_267_162,
        "Turnover": 9_995_123_456.78,
        "receive_ns": tick["receive_ns"],
    }
    assert isinstance(tick["receive_ns"], int)
    assert tick["receive_ns"] > 0

    sidecar.handle_command(
        {
            "op": "unsubscribe",
            "request_id": "unsub-sh",
            "security": "511880",
            "exchange": "SSE",
        }
    )
    sidecar.drain_events()
    assert module.api.unsubscribe_calls[-1] == ((b"511880",), "1", owner_thread)
    response = [
        event
        for event in _json_lines(output)
        if event["type"] == "response"
        and event.get("op") == "unsubscribe"
        and event.get("request_id") == "unsub-sh"
    ][-1]
    assert response["ok"] is True
    assert response["active"] is False
    sidecar.stop()


@pytest.mark.unit
def test_bounded_callback_queue_reports_drop_without_blocking() -> None:
    """验证有界回调队列满时丢弃并由主线程输出稳定错误。

    返回:
        无；只保留一条 tick 且 health 报告一次丢弃即通过。
    """

    sidecar, module, output = _started_sidecar(queue_capacity=1)
    sidecar.handle_command(
        {"op": "subscribe", "request_id": "sub-one", "security": "511880", "exchange": "SSE"}
    )
    sidecar.drain_events()
    module.api.emit_tick(_FakeTick(volume=100))
    module.api.emit_tick(_FakeTick(volume=200))

    sidecar.drain_events()
    sidecar.handle_command({"op": "health", "request_id": "health-one"})
    events = _json_lines(output)
    ticks = [event for event in events if event["type"] == "tick"]
    errors = [event for event in events if event["type"] == "error"]
    health = [
        event for event in events if event.get("type") == "response" and event.get("op") == "health"
    ][-1]

    assert len(ticks) == 1
    assert ticks[0]["Volume"] == 100
    assert errors[-1]["code"] == "callback_queue_overflow"
    assert health["dropped_events"] == 1
    sidecar.stop()


@pytest.mark.unit
def test_first_matching_tick_completes_pending_subscribe_without_vendor_response() -> None:
    """验证 v1.0.5 缺订阅回调时，首个匹配 tick 可完成同 request_id 回执。

    返回:
        无；response 必须先于 tick 输出，且迟到厂商回调不得重复响应。
    """

    sidecar, module, output = _started_sidecar()
    module.api.emit_subscription_response = False
    sidecar.handle_command(
        {
            "op": "subscribe",
            "request_id": "sub-from-tick",
            "security": "511880",
            "exchange": "SSE",
        }
    )
    sidecar.drain_events()
    assert not [
        event
        for event in _json_lines(output)
        if event.get("type") == "response" and event.get("request_id") == "sub-from-tick"
    ]

    module.api.emit_tick(_FakeTick())
    sidecar.drain_events()
    events = _json_lines(output)
    response_index = next(
        index
        for index, event in enumerate(events)
        if event.get("type") == "response" and event.get("request_id") == "sub-from-tick"
    )
    tick_index = next(index for index, event in enumerate(events) if event.get("type") == "tick")
    response = events[response_index]

    assert response == {
        "type": "response",
        "request_id": "sub-from-tick",
        "op": "subscribe",
        "ok": True,
        "security": "511880",
        "exchange": "SSE",
        "active": True,
    }
    assert response_index < tick_index

    module.api.spi.OnRspSubMarketData(_FakeSecurity(b"511880", "1"), _FakeResponse(0))
    sidecar.drain_events()
    assert (
        len(
            [
                event
                for event in _json_lines(output)
                if event.get("type") == "response" and event.get("request_id") == "sub-from-tick"
            ]
        )
        == 1
    )
    sidecar.stop()


@pytest.mark.unit
def test_unsubscribe_return_zero_completes_once_without_vendor_response() -> None:
    """验证解除订阅缺厂商回调时以同步成功码完成一次 inactive 回执。

    返回:
        无；同 request_id 只能输出一次 response，迟到厂商回调不得重复响应。
    """

    sidecar, module, output = _started_sidecar()
    sidecar.handle_command(
        {
            "op": "subscribe",
            "request_id": "sub-before-unsub",
            "security": "511880",
            "exchange": "SSE",
        }
    )
    sidecar.drain_events()
    module.api.emit_unsubscription_response = False

    sidecar.handle_command(
        {
            "op": "unsubscribe",
            "request_id": "unsub-without-callback",
            "security": "511880",
            "exchange": "SSE",
        }
    )
    responses = [
        event
        for event in _json_lines(output)
        if event.get("type") == "response" and event.get("request_id") == "unsub-without-callback"
    ]
    assert responses == [
        {
            "type": "response",
            "request_id": "unsub-without-callback",
            "op": "unsubscribe",
            "ok": True,
            "security": "511880",
            "exchange": "SSE",
            "active": False,
        }
    ]

    module.api.spi.OnRspUnSubMarketData(_FakeSecurity(b"511880", "1"), _FakeResponse(0))
    sidecar.drain_events()
    matching = [
        event
        for event in _json_lines(output)
        if event.get("type") == "response" and event.get("request_id") == "unsub-without-callback"
    ]
    assert len(matching) == 1
    sidecar.stop()


@pytest.mark.unit
def test_json_protocol_rejects_unknown_commands_and_invalid_security() -> None:
    """验证 stdin 只接受四类命令且证券代码必须带明确市场后缀。

    返回:
        无；错误只以稳定 error JSON 输出即通过。
    """

    sidecar, _, output = _started_sidecar()
    sidecar.handle_json_line("not-json")
    sidecar.handle_json_line("[]")
    sidecar.handle_json_line('{"op":"place_order","request_id":"bad-op"}')
    sidecar.handle_json_line(
        '{"op":"subscribe","request_id":"bad-security",' '"security":"511880","exchange":"OTHER"}'
    )
    sidecar.handle_json_line('{"op":"stop","request_id":"stop-one"}')

    codes = [event["code"] for event in _json_lines(output) if event["type"] == "error"]
    assert codes == ["json_invalid", "json_invalid", "command_invalid", "security_invalid"]
    assert sidecar.stop_requested is True
    assert module_api_names(_FakeXmdApi()) == {
        "CTORATstpXMdApi_CreateTstpXMdApi",
        "CTORATstpXMdApi_GetApiVersion",
    }
    sidecar.stop()
    stop_response = _json_lines(output)[-1]
    assert stop_response == {
        "type": "response",
        "request_id": "stop-one",
        "op": "stop",
        "ok": True,
        "released": True,
    }
    assert {event["type"] for event in _json_lines(output)} <= {
        "ready",
        "response",
        "tick",
        "error",
    }


def module_api_names(module: Any) -> set:
    """提取 fake 模块中可调用的 XMdApi 工厂/版本函数名。

    参数:
        module: fake xmdapi 模块。
    返回:
        显式 XMdApi 函数名集合，用于证明不存在 Trader 写接口。
    """

    return {
        name
        for name in dir(module)
        if name.startswith("CTORATstpXMdApi_") and callable(getattr(module, name))
    }


@pytest.mark.unit
def test_security_normalisation_and_explicit_sdk_directory_gate() -> None:
    """验证六位代码与显式沪深交易所归一化及 SDK 目录门禁。

    返回:
        无；映射和稳定错误码符合预期即通过。
    """

    assert _normalise_security("511880", "SSE") == ("511880.XSHG", "511880", "SSE")
    assert _normalise_security("000001", "SZSE") == ("000001.XSHE", "000001", "SZSE")
    with pytest.raises(XmdSidecarError) as exc_info:
        load_xmdapi("relative-sdk")
    assert exc_info.value.code == "sdk_dir_invalid"


@pytest.mark.unit
def test_sidecar_source_parses_with_python37_grammar() -> None:
    """验证正式 sidecar 不使用 Python 3.8 以后才引入的语法。

    返回:
        无；Python 3.7 grammar 能完整解析源码即通过。
    """

    source_path = (
        Path(__file__).resolve().parents[4]
        / "bullet_trade"
        / "integrations"
        / "huaxin"
        / "xmd_sidecar.py"
    )
    source = source_path.read_text(encoding="utf-8")
    ast.parse(source, filename=str(source_path), feature_version=(3, 7))
