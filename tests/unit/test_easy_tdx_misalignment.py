"""EasyTdxProvider 串包（响应错位一格）回归测试。

背景：2026-10-16 生产中，名称查询 get_symbol_info 超时被吞，socket 里残留迟到响应，
之后每个请求都拿到上一个请求的结果（518880 拿到上证指数 3707.48，交易日历只有 1 行）。
本文件用假 socket 模拟通达信 MAC 服务器，复现"第 N 次请求超时、响应迟到"，
断言后续请求不会拿到错位数据：要么连接已重建拿到正确数据，要么明确报错。
"""

from __future__ import annotations

import socket
import struct
from datetime import datetime

import pandas as pd
import pytest

pytest.importorskip("easy_tdx")

from easy_tdx import MacClient  # noqa: E402
from easy_tdx.exceptions import TdxConnectionError  # noqa: E402
from easy_tdx.mac.commands import SymbolBarCmd, SymbolInfoCmd, SymbolQuotesCmd  # noqa: E402
from easy_tdx.transport import sync as tdx_sync  # noqa: E402

from bullet_trade.data.providers import easy_tdx as easy_tdx_module  # noqa: E402
from bullet_trade.data.providers.easy_tdx import (  # noqa: E402
    EasyTdxProvider,
    TdxResponseMismatchError,
    _guarded_connection_cls,
)

_MAGIC = 7654321
# (market, code) -> [(yyyymmdd, close)]
_BARS = {
    (1, "999999"): [(20261015, 3700.11), (20261016, 3707.48)],
    (1, "518880"): [(20261015, 8.43), (20261016, 8.472)],
    (1, "513100"): [(20261015, 1.61), (20261016, 1.633)],
}
_NAMES = {(1, "518880"): "黄金ETF华安", (1, "513100"): "纳指ETF"}


def _frame(
    request: bytes, body: bytes, *, seq: bytes | None = None, method: bytes | None = None
) -> bytes:
    """按 MAC 响应格式拼 16 字节帧头 + body；默认回显请求序号与命令号。"""
    seq = request[1:5] if seq is None else seq
    method = request[10:12] if method is None else method
    header = struct.pack("<IB4sB2sHH", _MAGIC, 0x0C, seq, 0, method, len(body), len(body))
    return header + body


def _bar_body(identity: bytes, bars) -> bytes:
    """构造 SymbolBarCmd 响应体：market+code 回显、33 字节头、每根 36 字节。"""
    body = bytearray(identity)
    body += struct.pack("<HBHI", 4, 0, len(bars), 0)
    for ymd, close in bars:
        body += struct.pack("<II7f", ymd, 0, close, close, close, close, 1e6, 1e4, 0.0)
    return bytes(body)


def _info_body(market: int, code: str, name: str) -> bytes:
    """构造 SymbolInfoCmd 响应体（194 字节），只关心 market/code/name。"""
    body = bytearray(8)
    body += struct.pack("<H22s44s", market, code.encode("gbk"), name.encode("gbk"))
    body += bytes(96 - len(body))
    body += struct.pack(
        "<III5ffIfII", 20261016, 103300, 0, 8.4, 8.4, 8.5, 8.3, 8.47, 0.0, 0, 0.0, 0, 0
    )
    body += struct.pack("<HIf20xI3f", 3, 0, 0.0, 0, 0.0, 0.0, 0.0)
    return bytes(body)


def _quotes_body(rows) -> bytes:
    """构造不带字段位图的 SymbolQuotesCmd 响应体，只回 market/code/name。"""
    body = bytearray(20)
    body += struct.pack("<IH", len(rows), len(rows))
    for market, code in rows:
        body += struct.pack("<H22s44s", market, code.encode("gbk"), b"")
    return bytes(body)


class FakeTdxServer:
    """假通达信 MAC 服务器：按请求内容生成正确响应，可让指定序号的请求超时后迟到。"""

    def __init__(self, timeout_requests=(), setup_timeout: bool = False) -> None:
        self.timeout_requests = set(timeout_requests)
        self.setup_timeout = setup_timeout
        self.request_count = 0
        self.sockets: list[FakeSocket] = []
        self.override = None  # 可选：callable(request) -> 响应字节，模拟服务器回错标的

    def socket_factory(self, *args, **kwargs) -> "FakeSocket":
        """替换 socket.socket，每次建连都新建独立缓冲区。"""
        sock = FakeSocket(self)
        self.sockets.append(sock)
        return sock

    def respond(self, request: bytes) -> bytes:
        """按 msg_id 构造与请求对应的正确响应。"""
        if self.override is not None:
            forged = self.override(request)
            if forged is not None:
                return forged
        msg_id = struct.unpack_from("<H", request, 10)[0]
        if msg_id == 0x122E:
            market, code_raw = struct.unpack_from("<H22s", request, 12)
            code = code_raw.rstrip(b"\x00").decode()
            return _frame(request, _bar_body(request[12:36], _BARS[(market, code)]))
        if msg_id == 0x122A:
            market, code_raw = struct.unpack_from("<H22s", request, 12)
            code = code_raw.rstrip(b"\x00").decode()
            return _frame(request, _info_body(market, code, _NAMES.get((market, code), code)))
        if msg_id == 0x122B:
            offset = 12 + 20
            (count,) = struct.unpack_from("<H", request, offset)
            rows = []
            for idx in range(count):
                market, code_raw = struct.unpack_from("<H22s", request, offset + 2 + idx * 24)
                rows.append((market, code_raw.rstrip(b"\x00").decode()))
            return _frame(request, _quotes_body(rows))
        raise AssertionError(f"未模拟的命令 {msg_id:#x}")


class FakeSocket:
    """假 socket：sendall 把响应放入缓冲区；超时请求的响应在 recv 超时之后才到达。"""

    def __init__(self, server: FakeTdxServer) -> None:
        self.server = server
        self.buffer = bytearray()
        self.closed = False
        self.late: bytes | None = None

    def settimeout(self, value) -> None:
        """接受超时设置，无副作用。"""

    def connect(self, address) -> None:
        """模拟建连成功。"""

    def close(self) -> None:
        """标记关闭，之后的读写抛 OSError。"""
        self.closed = True

    def sendall(self, data: bytes) -> None:
        """接收请求：握手回空包，行情请求按计划正常回或迟到回。"""
        if self.closed:
            raise OSError("socket closed")
        if data[0] == 0x0C:
            if not self.server.setup_timeout:
                self.buffer += struct.pack("<IIIHH", _MAGIC, 0, 0, 0, 0)
            return
        self.server.request_count += 1
        response = self.server.respond(bytes(data))
        if self.server.request_count in self.server.timeout_requests:
            self.late = response
        else:
            self.buffer += response

    def recv(self, n: int) -> bytes:
        """读取缓冲区；有迟到响应时先超时，再让迟到响应进入缓冲区。"""
        if self.closed:
            raise OSError("socket closed")
        if self.late is not None:
            self.buffer += self.late
            self.late = None
            raise socket.timeout("timed out")
        if not self.buffer:
            raise socket.timeout("timed out")
        chunk = bytes(self.buffer[:n])
        del self.buffer[:n]
        return chunk


@pytest.fixture
def fake_server(monkeypatch):
    """返回替换了 socket.socket 的假服务器工厂。"""

    def build(**kwargs) -> FakeTdxServer:
        server = FakeTdxServer(**kwargs)
        monkeypatch.setattr(tdx_sync.socket, "socket", server.socket_factory)
        return server

    monkeypatch.setattr(EasyTdxProvider, "_ensure_config_dir", staticmethod(lambda: None))
    monkeypatch.delenv("EASY_TDX_USE_STUB", raising=False)
    return build


def _guarded_conn(server: FakeTdxServer):
    """建立一条带防护的连接。"""
    conn = _guarded_connection_cls()("fake-host", 7709, 1.0)
    conn.connect()
    return conn


def _closes(bars) -> list[float]:
    """把 MacBar 列表转为 3 位小数收盘价。"""
    return [round(bar.close, 3) for bar in bars]


@pytest.mark.unit
def test_raw_sdk_reproduces_misalignment(fake_server) -> None:
    """对照组：未加防护的 SDK 连接在一次超时后会把上一个请求的响应交给下一个请求。"""
    server = fake_server(timeout_requests={1})
    conn = tdx_sync.TdxConnection("fake-host", 7709, 1.0)
    conn.connect()
    with pytest.raises(TdxConnectionError):
        conn.execute(SymbolBarCmd(1, "999999", count=2))
    # SDK 未关闭 socket，下一请求读到的是迟到的上证指数 K 线。
    assert _closes(conn.execute(SymbolBarCmd(1, "518880", count=2))) == [3700.11, 3707.48]
    assert len(server.sockets) == 1


@pytest.mark.unit
def test_guard_closes_socket_after_timeout(fake_server) -> None:
    """防护连接超时后立即关闭 socket，同一连接上的后续请求报错而不是拿到错位数据。"""
    server = fake_server(timeout_requests={1})
    conn = _guarded_conn(server)
    with pytest.raises(TdxConnectionError):
        conn.execute(SymbolBarCmd(1, "999999", count=2))
    assert server.sockets[0].closed
    with pytest.raises(TdxConnectionError, match="未连接"):
        conn.execute(SymbolBarCmd(1, "518880", count=2))


@pytest.mark.unit
def test_guard_rejects_stale_response_of_same_command(fake_server) -> None:
    """缓冲区残留同类命令（K 线）的旧响应时，靠请求序号识别串包并断开。"""
    server = fake_server()
    conn = _guarded_conn(server)
    stale_request = bytearray(SymbolBarCmd(1, "999999", count=2).build_request())
    stale_request[1:5] = struct.pack("<I", 0xFFFF0001)
    server.sockets[0].buffer += server.respond(bytes(stale_request))
    with pytest.raises(TdxResponseMismatchError, match="序号"):
        conn.execute(SymbolBarCmd(1, "518880", count=2))
    assert server.sockets[0].closed


@pytest.mark.unit
def test_guard_rejects_response_of_other_command(fake_server) -> None:
    """名称查询的响应被 K 线请求读到时（10/16 交易日历只剩 1 行），靠命令号识别。"""
    server = fake_server()
    conn = _guarded_conn(server)
    server.override = lambda req: _frame(
        req, _info_body(1, "518880", "黄金ETF华安"), method=struct.pack("<H", 0x122A)
    )
    with pytest.raises(TdxResponseMismatchError, match="命令号"):
        conn.execute(SymbolBarCmd(1, "999999", count=320))
    assert server.sockets[0].closed


@pytest.mark.unit
@pytest.mark.parametrize(
    "cmd,forged",
    [
        (
            SymbolBarCmd(1, "518880", count=2),
            lambda req: _frame(
                req, _bar_body(struct.pack("<H22s", 1, b"999999"), _BARS[(1, "999999")])
            ),
        ),
        (
            SymbolInfoCmd(1, "518880"),
            lambda req: _frame(req, _info_body(1, "513100", "纳指ETF")),
        ),
        (
            SymbolQuotesCmd([(1, "518880")]),
            lambda req: _frame(req, _quotes_body([(1, "000001")])),
        ),
    ],
)
def test_guard_rejects_wrong_security_in_body(fake_server, cmd, forged) -> None:
    """帧头对得上但响应体标的不是请求标的时，同样判定串包并断开。"""
    server = fake_server()
    conn = _guarded_conn(server)
    server.override = forged
    with pytest.raises(TdxResponseMismatchError, match="标的"):
        conn.execute(cmd)
    assert server.sockets[0].closed


@pytest.mark.unit
def test_guard_handshake_timeout_drops_connection(fake_server) -> None:
    """握手无响应不再忽略：直接放弃该连接，避免迟到的握手响应污染后续请求。"""
    server = fake_server(setup_timeout=True)
    conn = _guarded_connection_cls()("fake-host", 7709, 1.0)
    with pytest.raises(TdxConnectionError, match="握手"):
        conn.connect()
    assert server.sockets[0].closed
    assert conn._sock is None


@pytest.mark.unit
def test_provider_replays_1016_sequence_without_misalignment(fake_server) -> None:
    """复现 10/16：日历→名称查询超时→逐个取 ETF 历史；之后每个标的都拿到自己的数据。"""
    server = fake_server(timeout_requests={2})
    provider = EasyTdxProvider({"host": "fake-host", "mac_client_cls": MacClient, "timeout": 1.0})
    try:
        days = provider.get_trade_days(count=2)
        assert [d.date() for d in days] == [
            datetime(2026, 10, 15).date(),
            datetime(2026, 10, 16).date(),
        ]
        assert isinstance(provider._client._conn, _guarded_connection_cls())

        info = provider.get_security_info("518880.XSHG")
        assert info["display_name"] == "518880.XSHG"  # 超时降级为代码
        assert server.sockets[0].closed  # 而且连接已经重置

        for security, expected in (("518880.XSHG", 8.472), ("513100.XSHG", 1.633)):
            df = provider.get_price(
                security, count=1, fields=["close"], fq=None, end_date="2026-10-16"
            )
            assert df["close"].tolist() == [expected]
        days = provider.get_trade_days(count=2)
        assert len(days) == 2
        assert len(server.sockets) == 2
        # 迟到响应只留在已关闭的旧 socket 里，新连接没有残留。
        assert server.sockets[0].closed and server.sockets[0].buffer
        assert not server.sockets[-1].buffer
    finally:
        provider.close()


@pytest.mark.unit
def test_provider_name_lookup_uses_call_and_cache(fake_server) -> None:
    """名称查询经 _call、正确解析 DataFrame 名称，并在当日缓存不再请求。"""
    server = fake_server()
    provider = EasyTdxProvider({"host": "fake-host", "mac_client_cls": MacClient, "timeout": 1.0})
    try:
        assert provider.get_security_info("518880.XSHG")["display_name"] == "黄金ETF华安"
        before = server.request_count
        for _ in range(20):
            assert provider.get_security_info("518880.XSHG")["display_name"] == "黄金ETF华安"
        assert server.request_count == before
        provider._name_cache_day = datetime(2000, 1, 1).date()
        provider.get_security_info("518880.XSHG")
        assert server.request_count == before + 1
    finally:
        provider.close()


class _ScriptedClient:
    """可编排异常的假 MacClient，记录建连与关闭次数。"""

    created: list["_ScriptedClient"] = []
    plan: list = []

    def __init__(self, *args, **kwargs) -> None:
        self._host = f"host-{len(self.created)}"
        self.closed = False
        self.created.append(self)

    @classmethod
    def from_best_host(cls, **kwargs):
        return cls()

    def connect(self) -> None:
        """模拟连接成功。"""

    def close(self) -> None:
        self.closed = True

    def _next(self, default):
        action = self.plan.pop(0) if self.plan else default
        if isinstance(action, BaseException):
            raise action
        return action

    def get_symbol_info(self, market, code):
        return self._next(pd.DataFrame([{"market": market, "code": code, "name": "名称"}]))

    def get_stock_quotes_list(self, category, count=5000):
        return self._next(pd.DataFrame([{"market": 0, "code": "000001", "name": "平安银行"}]))

    def get_stock_quotes(self, stocks, fields=None):
        return self._next(None)

    def get_stock_kline(self, **kwargs):
        return self._next(pd.DataFrame())


@pytest.fixture
def scripted(monkeypatch):
    """返回使用 _ScriptedClient 的自动选址 provider。"""
    monkeypatch.setattr(EasyTdxProvider, "_ensure_config_dir", staticmethod(lambda: None))
    monkeypatch.delenv("EASY_TDX_USE_STUB", raising=False)
    from easy_tdx import config

    monkeypatch.setattr(config, "get_mac_hosts", lambda: ["host-0", "host-1", "host-2"])
    _ScriptedClient.created = []
    _ScriptedClient.plan = []
    return EasyTdxProvider({"mac_client_cls": _ScriptedClient})


@pytest.mark.unit
def test_call_closes_connection_on_non_network_error(scripted) -> None:
    """非网络异常（如解析错误）也必须先关闭连接再抛出，不在同一连接上继续使用。"""
    _ScriptedClient.plan = [ValueError("decode failed")]
    with pytest.raises(ValueError):
        scripted.get_all_securities("stock")
    assert _ScriptedClient.created[0].closed
    assert not scripted._connected


@pytest.mark.unit
def test_call_fails_over_on_mismatch(scripted) -> None:
    """串包按网络故障处理：关闭旧连接、换主机重试一次，拿到正确结果。"""
    _ScriptedClient.plan = [TdxResponseMismatchError("串包")]
    df = scripted.get_all_securities("stock")
    assert "000001.XSHE" in df.index
    assert _ScriptedClient.created[0].closed
    assert len(_ScriptedClient.created) == 2


@pytest.mark.unit
def test_name_failure_resets_connection_and_degrades(scripted) -> None:
    """名称查询失败：连接被重置、返回代码；失败结果短暂缓存，不会每分钟再打服务器。"""
    _ScriptedClient.plan = [ValueError("bad")]
    assert scripted.get_security_info("518880.XSHG")["display_name"] == "518880.XSHG"
    assert _ScriptedClient.created[0].closed
    _ScriptedClient.plan = [AssertionError("不应再次请求")]
    assert scripted.get_security_info("518880.XSHG")["display_name"] == "518880.XSHG"


@pytest.mark.unit
def test_name_lookup_rejects_other_security(scripted) -> None:
    """名称查询返回其他标的时视为串包：重置连接并降级为代码。"""
    _ScriptedClient.plan = [pd.DataFrame([{"market": 1, "code": "999999", "name": "上证指数"}])]
    assert scripted.get_security_info("518880.XSHG")["display_name"] == "518880.XSHG"
    assert _ScriptedClient.created[0].closed


def _quote(**overrides) -> pd.DataFrame:
    row = {
        "market": 1,
        "code": "518880",
        "close": 8.472,
        "open": 8.43,
        "high": 8.5,
        "low": 8.4,
        "pre_close": 8.43,
        "buy_price_limit": 9.273,
        "sell_price_limit": 7.587,
        "server_update_date": 20261016,
        "server_update_time": 103300,
    }
    row.update(overrides)
    return pd.DataFrame([row])


@pytest.mark.unit
@pytest.mark.parametrize(
    "overrides,match",
    [
        ({"code": "999999"}, "其他标的"),
        ({"close": 3707.48}, "涨跌停"),
        (
            {
                "close": 3707.48,
                "open": 3701.0,
                "high": 3710.0,
                "low": 3699.0,
                "pre_close": 3700.0,
                "buy_price_limit": 0.0,
                "sell_price_limit": 0.0,
            },
            None,
        ),
    ],
)
def test_quote_content_guard(scripted, overrides, match) -> None:
    """快照标的不符、价格超出涨跌停时按串包处理并重置连接。"""
    _ScriptedClient.plan = [_quote(**overrides)]
    if match is None:
        # 整行自洽（指数口径）但标的代码正确时不误报。
        assert scripted.get_live_current("518880.XSHG")["last_price"] == 3707.48
        return
    with pytest.raises(TdxResponseMismatchError, match=match):
        scripted.get_live_current("518880.XSHG")
    assert not scripted._connected


@pytest.mark.unit
def test_quote_magnitude_guard_without_limits(scripted) -> None:
    """无涨跌停（指数）时按昨收数量级判断。"""
    _ScriptedClient.plan = [
        _quote(code="000001", close=8.472, pre_close=3700.0, buy_price_limit=0, sell_price_limit=0)
    ]
    with pytest.raises(TdxResponseMismatchError, match="数量级"):
        scripted.get_live_current("000001.XSHG")


@pytest.mark.unit
def test_same_day_pre_close_jump_is_rejected(scripted) -> None:
    """同一行情日内昨收不应变化数量级；整行错位（别的标的的自洽行情）也能识别。"""
    _ScriptedClient.plan = [
        _quote(),
        _quote(
            close=1.633,
            open=1.62,
            high=1.64,
            low=1.61,
            pre_close=1.61,
            buy_price_limit=1.771,
            sell_price_limit=1.449,
        ),
    ]
    scripted.get_live_current("518880.XSHG")
    with pytest.raises(TdxResponseMismatchError, match="昨收突变"):
        scripted.get_live_current("518880.XSHG")


@pytest.mark.unit
def test_kline_content_guard_uses_quote_anchor(scripted) -> None:
    """有当日报价锚点后，K 线价格数量级与昨收/涨跌停不符即按串包处理。"""
    _ScriptedClient.plan = [_quote()]
    scripted.get_live_current("518880.XSHG")
    bars = pd.DataFrame(
        [
            {
                "datetime": "2026-10-15",
                "open": 3700.0,
                "high": 3700.0,
                "low": 3700.0,
                "close": 3700.11,
            },
            {
                "datetime": "2026-10-16",
                "open": 3707.0,
                "high": 3707.0,
                "low": 3707.0,
                "close": 3707.48,
            },
        ]
    )
    _ScriptedClient.plan = [bars]
    with pytest.raises(TdxResponseMismatchError, match="K线"):
        scripted.get_price("518880.XSHG", count=2, fields=["close"], fq=None, end_date="2026-10-16")
    assert not scripted._connected

    good = pd.DataFrame(
        [
            {"datetime": "2026-10-15", "open": 8.4, "high": 8.45, "low": 8.38, "close": 8.43},
            {"datetime": "2026-10-16", "open": 8.43, "high": 8.5, "low": 8.4, "close": 8.472},
        ]
    )
    _ScriptedClient.plan = [good]
    df = scripted.get_price(
        "518880.XSHG", count=2, fields=["close"], fq=None, end_date="2026-10-16"
    )
    assert df["close"].tolist() == [8.43, 8.472]


@pytest.mark.unit
def test_install_guard_ignores_non_sdk_clients() -> None:
    """测试替身等非 SDK 客户端不被替换内部连接。"""
    fake = type("Fake", (), {"_conn": object()})()
    original = fake._conn
    easy_tdx_module._install_connection_guard(fake)
    assert fake._conn is original
