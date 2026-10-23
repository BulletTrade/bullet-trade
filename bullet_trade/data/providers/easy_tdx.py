"""
easy_tdx 通达信在线数据源适配器

作者: BruceLee
文件职责：通过 easy_tdx SDK 直连通达信在线行情服务器，提供聚宽兼容行情接口。
主要输入：聚宽或通达信风格证券代码、日期范围、频率、字段、复权参数。
主要输出：K 线 DataFrame、交易日、证券列表、实时快照和有限的除权除息事件。
上下游关系：由 bullet_trade.data.api 的 provider 工厂按 easy_tdx/tdx 名称创建。
关键约定：真实模式连接失败不得自动返回假行情；stub 仅在 use_stub=True 时用于测试或演示。
"""

from __future__ import annotations

import importlib
import itertools
import logging
import math
import os
import struct
import time
from datetime import date as Date
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import numpy as np

from ..qmt_adjustment import AdjustmentEvent

from ..adjustment_cache import AdjustmentCache
from .base import DataProvider

__all__ = ["EasyTdxProvider"]

_MARKET_SZ = 0
_MARKET_SH = 1
_MARKET_BJ = 2
_SUFFIX_TO_MARKET: Dict[str, int] = {
    "XSHE": _MARKET_SZ,
    "SZ": _MARKET_SZ,
    "XSHG": _MARKET_SH,
    "SH": _MARKET_SH,
    "XBJG": _MARKET_BJ,
    "BJ": _MARKET_BJ,
    "BSE": _MARKET_BJ,
}
_MARKET_TO_SUFFIX: Dict[int, str] = {
    _MARKET_SZ: "XSHE",
    _MARKET_SH: "XSHG",
    _MARKET_BJ: "XBJG",
}
_FREQUENCY_TO_PERIOD: Dict[str, int] = {
    "5m": 0,
    "15m": 1,
    "30m": 2,
    "60m": 3,
    "1h": 3,
    "daily": 4,
    "1d": 4,
    "d": 4,
    "weekly": 5,
    "1w": 5,
    "monthly": 6,
    "1M": 6,
    "1m": 7,
    "minute": 7,
    "m1": 7,
}
_ADJUST_MAP: Dict[Optional[str], int] = {
    None: 0,
    "": 0,
    "none": 0,
    "pre": 1,
    "qfq": 1,
    "post": 2,
    "hfq": 2,
}
_DEFAULT_PRICE_FIELDS = ["open", "close", "high", "low", "volume", "money"]
_PRICE_FIELDS = {
    "open",
    "close",
    "high",
    "low",
    "avg",
    "price",
    "high_limit",
    "low_limit",
}
logger = logging.getLogger(__name__)
# Mac 标签位经 2026-09-30 临停/复牌及两只全日停牌证券的 QMT 快照交叉验证。
_MAC_PAUSED_TAG = 0x1000
_TDX_PAUSED_STATUS = 0x20
_STUB_NAMES: Dict[str, str] = {
    "000001.XSHE": "平安银行",
    "600519.XSHG": "贵州茅台",
    "601318.XSHG": "中国平安",
    "510050.XSHG": "上证50ETF",
    "000001.XSHG": "上证指数",
}

# ---------------------------------------------------------------------------
# 传输层串包防护
#
# easy-tdx 1.14.4 的 TdxConnection.execute 在 recv 超时后不关闭 socket，也不核对
# 响应帧头与请求的对应关系；握手超时直接忽略。迟到的响应会留在缓冲区，之后每个
# 请求都读到上一个请求的结果（2026-10-16 实证）。这里不改 site-packages，而是在
# provider 侧用子类替换 MacClient 内部连接：
#   1. MAC 请求帧 bytes 1-4（customize，SDK 恒为 0）写入单调递增序号，服务器在响应
#      帧头 bytes 5-8 原样回显；bytes 10-11 回显请求 bytes 10-11 的 msg_id；
#   2. 帧头 magic/序号/msg_id 任一不符即判定串包；
#   3. K 线与证券信息响应体回显 market+code，报价按返回代码核对标的；
#   4. 任何异常（含解析错误、串包）都立即关闭 socket，绝不在同一连接上继续读写。
# ---------------------------------------------------------------------------
_TDX_MAGIC = 7654321
_MAC_HEAD_FLAG = 0x1C
_MSG_SYMBOL_INFO = 0x122A
_MSG_SYMBOL_QUOTES = 0x122B
_MSG_SYMBOL_BAR = 0x122E
_SEQ_COUNTER = itertools.count(1)


class TdxResponseMismatchError(ConnectionError):
    """响应与请求不对应（串包）；继承 ConnectionError，便于按网络故障换连接。"""


_GUARDED_CONNECTION_CLS: Optional[type] = None


def _guarded_connection_cls() -> type:
    """无输入，返回带串包防护的 TdxConnection 子类；首次调用时导入 SDK 并构造。"""
    global _GUARDED_CONNECTION_CLS
    if _GUARDED_CONNECTION_CLS is not None:
        return _GUARDED_CONNECTION_CLS
    from easy_tdx.codec.frame import HEADER_SIZE, decompress_body, parse_header
    from easy_tdx.commands.setup import SETUP_COMMANDS
    from easy_tdx.exceptions import TdxConnectionError
    from easy_tdx.transport.sync import TdxConnection

    class GuardedTdxConnection(TdxConnection):
        """校验响应对应关系、异常即断开的同步连接。"""

        def _drop_socket(self) -> None:
            """关闭 socket 并清空引用；调用方已持有 _lock，故不在此 join 心跳线程。"""
            sock, self._sock = self._sock, None
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

        def _send_setup(self) -> None:
            """严格握手：任何握手响应缺失都视为连接不可用，避免残留迟到响应。"""
            assert self._sock is not None
            for cmd_bytes in SETUP_COMMANDS:
                self._sock.sendall(cmd_bytes)
                try:
                    header_buf = self._recv_exact(HEADER_SIZE)
                    header = parse_header(header_buf)
                    if header.magic != _TDX_MAGIC:
                        raise TdxResponseMismatchError("easy_tdx 握手响应 magic 不符")
                    if header.zipsize > 0:
                        self._recv_exact(header.zipsize)
                except OSError as exc:
                    raise TdxConnectionError(f"握手无响应，放弃该连接: {exc}") from exc

        def execute(self, cmd: Any) -> Any:
            """发送请求并核对响应帧头与标的；任何异常都关闭 socket 后再抛出。"""
            with self._lock:
                self._last_active = time.monotonic()
                self._consecutive_heartbeats = 0
                if self._sock is None:
                    raise TdxConnectionError("未连接，请先调用 connect()")
                request = bytearray(cmd.build_request())
                if len(request) >= 12 and request[0] == _MAC_HEAD_FLAG:
                    seq = next(_SEQ_COUNTER) % 0xFFFFFFFF or 1
                    request[1:5] = struct.pack("<I", seq)
                try:
                    self._sock.sendall(bytes(request))
                    header_buf = self._recv_exact(HEADER_SIZE)
                    header = parse_header(header_buf)
                    _check_frame_echo(request, header_buf, header.magic)
                    raw_body = self._recv_exact(header.zipsize)
                    body = decompress_body(header, raw_body)
                    _check_body_identity(request, body)
                    result = cmd.parse_response(body)
                    _check_quote_identity(request, cmd, result)
                    return result
                except BaseException as exc:
                    self._drop_socket()
                    if isinstance(exc, OSError) and not isinstance(exc, TdxResponseMismatchError):
                        raise TdxConnectionError(f"通信错误: {exc}") from exc
                    raise

    _GUARDED_CONNECTION_CLS = GuardedTdxConnection
    return GuardedTdxConnection


def _check_frame_echo(request: bytes, header_buf: bytes, magic: int) -> None:
    """输入请求与 16 字节响应帧头；magic、序号或 msg_id 回显不符时抛 TdxResponseMismatchError。"""
    if magic != _TDX_MAGIC:
        raise TdxResponseMismatchError(f"easy_tdx 响应 magic 异常: {magic}")
    if bytes(header_buf[5:9]) != bytes(request[1:5]):
        raise TdxResponseMismatchError(
            "easy_tdx 响应序号与请求不符（串包）: "
            f"request={bytes(request[1:5]).hex()} response={bytes(header_buf[5:9]).hex()}"
        )
    if bytes(header_buf[10:12]) != bytes(request[10:12]):
        raise TdxResponseMismatchError(
            "easy_tdx 响应命令号与请求不符（串包）: "
            f"request={bytes(request[10:12]).hex()} response={bytes(header_buf[10:12]).hex()}"
        )


def _check_body_identity(request: bytes, body: bytes) -> None:
    """输入请求与响应体；单标的命令响应回显的 market+code 与请求不符时抛错。"""
    if len(request) < 36:
        return
    msg_id = struct.unpack_from("<H", request, 10)[0]
    offset = {_MSG_SYMBOL_BAR: 0, _MSG_SYMBOL_INFO: 8}.get(msg_id)
    if offset is None:
        return
    expected = bytes(request[12:36])
    actual = bytes(body[offset : offset + 24])
    if actual != expected:
        raise TdxResponseMismatchError(
            f"easy_tdx 响应标的与请求不符（串包）: request={expected.hex()} response={actual.hex()}"
        )


def _check_quote_identity(request: bytes, cmd: Any, result: Any) -> None:
    """输入报价请求与解析结果；返回了未请求的标的时抛错。"""
    if len(request) < 12 or struct.unpack_from("<H", request, 10)[0] != _MSG_SYMBOL_QUOTES:
        return
    stocks = getattr(cmd, "_stocks", None)
    if not stocks or not isinstance(result, list):
        return
    requested = {(int(market), str(code).zfill(6)) for market, code in stocks}
    for item in result:
        market, code = getattr(item, "market", None), getattr(item, "code", None)
        if market is None or code is None:
            continue
        if (int(market), str(code).zfill(6)) not in requested:
            raise TdxResponseMismatchError(
                f"easy_tdx 报价返回未请求的标的（串包）: {market}/{code}"
            )


def _install_connection_guard(client: Any) -> None:
    """输入未连接的 MacClient，把内部 TdxConnection 换成防护子类；非真实 SDK 客户端不处理。"""
    conn = getattr(client, "_conn", None)
    if conn is None:
        return
    try:
        from easy_tdx.transport.sync import TdxConnection
    except ImportError:  # pragma: no cover - 只在缺依赖时触发
        return
    if not isinstance(conn, TdxConnection):
        return
    guarded_cls = _guarded_connection_cls()
    if isinstance(conn, guarded_cls):
        return
    client._conn = guarded_cls(conn.host, conn.port, conn.timeout)


class EasyTdxProvider(DataProvider):
    """通达信在线行情数据源，负责把 easy_tdx 输出转换为聚宽兼容格式。"""

    name: str = "easy_tdx"
    requires_live_data: bool = True
    _MAX_DAILY_FETCH = 50000
    _MAX_INTRADAY_FETCH = 50000

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        """初始化 provider，不在构造阶段连接通达信服务器。"""
        self.config = config or {}
        self._adjustment_cache = AdjustmentCache()
        self._host = self.config.get("host") or os.getenv("EASY_TDX_HOST")
        self._port = int(self.config.get("port") or os.getenv("EASY_TDX_PORT") or 7709)
        self._timeout = float(self.config.get("timeout") or os.getenv("EASY_TDX_TIMEOUT") or 10.0)
        self._use_stub = self._parse_bool(
            self.config.get("use_stub") or os.getenv("EASY_TDX_USE_STUB")
        )
        self.requires_live_data = not self._use_stub
        self._client: Optional[Any] = self.config.get("client")
        self._connected = bool(self._client is not None)
        self._easy_tdx: Optional[Any] = self.config.get("easy_tdx")
        self._mac_client_cls: Optional[Any] = self.config.get("mac_client_cls")
        self._tdx_client_cls: Optional[Any] = self.config.get("tdx_client_cls")
        self._selected_day = datetime.now().date()
        self._failed_host = None
        self._xdxr_host = None
        self._xdxr_selected_day = None
        # 证券名称进程内缓存，按自然日失效；失败结果只短暂缓存，避免故障期反复打服务器。
        self._name_cache: Dict[str, Tuple[str, float]] = {}
        self._name_cache_day: Optional[Date] = None
        # 当日报价锚点：security -> (日期, 昨收, 涨停, 跌停)，用于识别错位行情。
        self._price_refs: Dict[str, Tuple[Date, float, float, float]] = {}

    @staticmethod
    def _parse_bool(value: Any) -> bool:
        """把环境变量或配置值解析为布尔值。"""
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _as_list(security: Union[str, Sequence[str]]) -> List[str]:
        """把单证券或证券序列统一为列表。"""
        if isinstance(security, str):
            return [security]
        if security is None:
            return []
        return [str(item) for item in security]

    @staticmethod
    def tdx_to_jq(market: int, code: str) -> str:
        """把通达信 market/code 转换为聚宽代码。"""
        suffix = _MARKET_TO_SUFFIX.get(int(market), "XSHE")
        return f"{str(code).zfill(6)}.{suffix}"

    @staticmethod
    def _infer_jq_security_type(security: str) -> str:
        """按聚宽后缀和常见代码段推断证券类型。"""
        code = str(security).strip().upper()
        code_part = code.split(".", 1)[0]
        if (code.endswith((".XSHG", ".SH")) and code_part.startswith("000")) or (
            code.endswith((".XSHE", ".SZ")) and code_part.startswith("399")
        ):
            return "index"
        if code_part.startswith(("15", "16", "18", "50", "51", "56", "58")):
            return "fund"
        return "stock"

    @staticmethod
    def jq_to_tdx(security: str) -> Tuple[int, str]:
        """把聚宽或简写代码转换为通达信 market/code。"""
        sec = str(security).strip()
        if "." in sec:
            code, suffix = sec.rsplit(".", 1)
            return _SUFFIX_TO_MARKET.get(suffix.upper(), _MARKET_SZ), code
        code = sec
        if code.startswith(("6", "5", "9")):
            return _MARKET_SH, code
        if code.startswith(("8", "4")):
            return _MARKET_BJ, code
        return _MARKET_SZ, code

    @staticmethod
    def _normalize_frequency(frequency: str) -> str:
        """把聚宽频率标准化为内部字符串。"""
        freq = str(frequency or "daily").strip()
        lower = freq.lower()
        if lower in {"daily", "1d", "d", "day"}:
            return "daily"
        if lower in {"minute", "1m", "m1", "1min"}:
            return "1m"
        return freq

    @classmethod
    def _resolve_period(cls, frequency: str) -> int:
        """把标准化频率转换为 easy_tdx Period 整数。"""
        freq = str(frequency or "daily").strip()
        return _FREQUENCY_TO_PERIOD.get(freq, _FREQUENCY_TO_PERIOD.get(freq.lower(), 4))

    @staticmethod
    def _resolve_adjust(fq: Optional[str]) -> int:
        """把聚宽复权参数转换为 easy_tdx adjust 整数。"""
        if fq is None:
            return 0
        return _ADJUST_MAP.get(str(fq).lower().strip(), 0)

    @classmethod
    def _bars_per_day(cls, frequency: str) -> int:
        """返回分钟频率对应单日 bar 数。"""
        mapping = {"1m": 240, "5m": 48, "15m": 16, "30m": 8, "60m": 4}
        return mapping.get(str(frequency).lower(), 1)

    @staticmethod
    def _format_timestamp(
        value: Optional[Union[str, datetime, Date]],
    ) -> Optional[pd.Timestamp]:
        """把日期参数转换为 Timestamp。"""
        if value is None:
            return None
        parsed = pd.to_datetime(value)
        if pd.isna(parsed):
            return None
        return pd.Timestamp(parsed)

    def _ensure_easy_tdx(self) -> Any:
        """延迟导入 easy_tdx 模块；缺失时抛出安装指引。"""
        if self._easy_tdx is not None:
            return self._easy_tdx
        try:
            self._easy_tdx = importlib.import_module("easy_tdx")
        except ImportError as exc:  # pragma: no cover - 只在真实缺依赖时触发
            raise ImportError(
                "未安装 easy-tdx，请在 Python 3.10+ 环境执行 `pip install bullet-trade[tdx]` 或 `pip install easy-tdx`"
            ) from exc
        return self._easy_tdx

    def _resolve_mac_client_cls(self) -> Any:
        """解析 MacClient 类，兼容测试注入和真实 SDK。"""
        if self._mac_client_cls is not None:
            return self._mac_client_cls
        module = self._ensure_easy_tdx()
        self._mac_client_cls = getattr(module, "MacClient")
        return self._mac_client_cls

    def _resolve_tdx_client_cls(self) -> Any:
        """解析 TdxClient 类，缺失时抛出 NotImplementedError。"""
        if self._tdx_client_cls is not None:
            return self._tdx_client_cls
        module = self._ensure_easy_tdx()
        cls = getattr(module, "TdxClient", None)
        if cls is None:
            raise NotImplementedError("easy_tdx 未提供 TdxClient，无法读取除权除息数据")
        self._tdx_client_cls = cls
        return cls

    @staticmethod
    def _ensure_config_dir() -> None:
        """无输入，创建 SDK 实际配置目录；返回 None，仅产生目录创建副作用。"""
        Path(os.getenv("EASY_TDX_CONFIG_DIR") or Path.home() / ".easy_tdx").mkdir(
            parents=True, exist_ok=True
        )

    def auth(
        self,
        user: Optional[str] = None,
        pwd: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        """连接通达信行情服务器；真实模式失败时抛出异常，stub 模式跳过连接。"""
        _ = user, pwd
        if self._connected:
            return
        if self._use_stub:
            self._connected = True
            return
        injected = self.config.get("client")
        if injected is not None:
            # 注入客户端被重置后原样恢复；带 connect 的真实客户端需重新握手。
            self._client = injected
            connect = getattr(injected, "connect", None)
            if callable(connect) and getattr(injected, "_conn", None) is not None:
                _install_connection_guard(injected)
                connect()
            self._connected = True
            return
        target_host = host or self._host
        target_port = int(port or self._port)
        client_cls = self._resolve_mac_client_cls()
        try:
            self._ensure_config_dir()
            if target_host:
                self._client = client_cls(
                    host=target_host,
                    port=target_port,
                    timeout=self._timeout,
                    auto_reconnect=False,
                )
            elif hasattr(client_cls, "from_best_host"):
                options = {
                    "port": target_port,
                    "timeout": self._timeout,
                    "auto_reconnect": False,
                }
                if self._failed_host:
                    from easy_tdx.config import get_mac_hosts

                    options["hosts"] = [h for h in get_mac_hosts() if h != self._failed_host]
                    if not options["hosts"]:
                        raise RuntimeError("easy_tdx 没有其他候选行情服务器")
                self._client = client_cls.from_best_host(**options)
            else:
                self._client = client_cls(port=target_port, timeout=self._timeout)
            # 必须在 connect 之前替换内部连接，握手也走严格模式。
            _install_connection_guard(self._client)
            connect = getattr(self._client, "connect", None)
            if callable(connect):
                connect()
            self._connected = True
            self._selected_day = datetime.now().date()
            logger.info(
                "easy_tdx selected_host=%s reason=%s",
                getattr(self._client, "_host", target_host),
                "failover" if self._failed_host else "startup_or_new_day",
            )
            self._failed_host = None
        except Exception as exc:
            self._client = None
            self._connected = False
            message = (
                "EasyTdxProvider 连接通达信行情服务器失败；真实模式不会自动返回假行情，"
                "如仅做离线测试请显式传入 use_stub=True"
            )
            raise RuntimeError(message) from exc

    def _ensure_client(self) -> Optional[Any]:
        """返回已连接客户端；stub 模式返回 None。"""
        if self._use_stub:
            self._connected = True
            return None
        if (
            not self._host
            and not self.config.get("client")
            and self._connected
            and self._selected_day != datetime.now().date()
        ):
            self.close()
        if not self._connected:
            self.auth()
        return self._client

    def close(self) -> None:
        """无输入，关闭本 provider 的连接并清除连接状态；返回 None，不影响其他进程。"""
        client, self._client = self._client, None
        self._connected = False
        close = getattr(client, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:  # 关闭失败不影响后续重建连接
                logger.warning("easy_tdx close_failed error=%s: %s", type(exc).__name__, exc)

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """调用只读行情方法；输入方法与参数，返回 SDK 结果，失联最多换址一次后抛错。

        请求一旦可能已发出，任何异常（不只网络异常）都先关闭连接：socket 里可能残留
        迟到的响应，继续复用会让后续请求读到上一个请求的结果（串包）。
        自动模式下网络错误与串包从原候选名单排除失败 IP 后重选并重试一次；
        固定 host 或注入客户端只重置不换址；参数、数据解析等其他错误重置后原样抛出。
        """
        client = self._ensure_client()
        try:
            return getattr(client, method)(*args, **kwargs)
        except Exception as exc:
            failed_host = getattr(client, "_host", None)
            self.close()
            logger.warning(
                "easy_tdx reset_connection method=%s host=%s error=%s: %s",
                method,
                failed_host,
                type(exc).__name__,
                exc,
            )
            network_error = isinstance(exc, (ConnectionError, TimeoutError, OSError))
            network_error = network_error or type(exc).__name__ == "TdxConnectionError"
            if not network_error or self._host or self.config.get("client"):
                raise
            self._failed_host = failed_host
            self.auth()
            return getattr(self._client, method)(*args, **kwargs)

    def _resolve_fetch_count(
        self,
        *,
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
        frequency: str,
        count: Optional[int],
    ) -> int:
        """根据日期范围和 count 估算通达信请求条数。"""
        bars = self._bars_per_day(frequency)
        end_ts = self._format_timestamp(end_date)
        now = pd.Timestamp.now()
        offset_days = (
            max((now.normalize() - end_ts.normalize()).days, 0) if end_ts is not None else 0
        )
        cap = self._MAX_INTRADAY_FETCH if bars > 1 else self._MAX_DAILY_FETCH
        if count is not None:
            # SDK count 从最新向过去取，必须覆盖当前至历史截止日之间的数据。
            return min(
                max(int(count), 1) + offset_days * bars + (5 * bars if offset_days else 0),
                cap,
            )
        start_ts = self._format_timestamp(start_date)
        end_ts = max(end_ts or now, now)
        if start_ts is None:
            return 800 if frequency != "daily" else 3000
        fetch_end = max(end_ts, pd.Timestamp.now())
        days = max((fetch_end - start_ts).days + 1, 1)
        estimated = max(int(math.ceil(days * 0.75)) * bars + bars * 5, bars)
        cap = self._MAX_INTRADAY_FETCH if bars > 1 else self._MAX_DAILY_FETCH
        return min(max(estimated, 100 if bars == 1 else bars * 10), cap)

    def _fetch_single_kline(
        self,
        client: Any,
        security: str,
        *,
        period: int,
        fetch_count: int,
        adjust: int,
        fields: List[str],
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
        count: Optional[int],
    ) -> pd.DataFrame:
        """调用 easy_tdx 获取单证券 K 线并标准化。"""
        market, code = self.jq_to_tdx(security)
        fetcher = getattr(client, "get_stock_kline", None)
        if not callable(fetcher):
            raise RuntimeError("easy_tdx MacClient 未提供 get_stock_kline 方法")
        raw = self._call(
            "get_stock_kline",
            market=market,
            code=code,
            period=period,
            start=0,
            count=fetch_count,
            times=1,
            adjust=adjust,
        )
        df = self._normalize_kline_frame(raw, fields)
        self._check_kline_consistency(security, df)
        # Mac K 线的指数成交量单位与股票/ETF 不同；统一到聚宽的股数口径。
        if self._infer_jq_security_type(security) == "index" and "volume" in df:
            df["volume"] = df["volume"] * 100.0
        # SDK 用 float32 解码价格，按证券报价精度消除二进制尾差，不改变行情含义。
        decimals = 3 if self._infer_jq_security_type(security) == "fund" else 2
        for field in _PRICE_FIELDS.intersection(df.columns):
            df[field] = pd.to_numeric(df[field], errors="coerce").round(decimals)
        df = self._filter_by_date(df, start_date=start_date, end_date=end_date)
        if count is not None and not df.empty:
            df = df.tail(int(count))
        return df.reindex(columns=fields)

    def _fetch_daily_raw_for_factor(self, client: Any, security: str) -> pd.DataFrame:
        """读取未复权日线，用于按除权除息事件构造累计复权因子。"""
        market, code = self.jq_to_tdx(security)
        fetcher = getattr(client, "get_stock_kline", None)
        if not callable(fetcher):
            raise RuntimeError("easy_tdx MacClient 未提供 get_stock_kline 方法")
        raw = self._call(
            "get_stock_kline",
            market=market,
            code=code,
            period=self._resolve_period("daily"),
            start=0,
            count=self._MAX_DAILY_FETCH,
            times=1,
            adjust=0,
        )
        df = self._normalize_kline_frame(raw, ["close"])
        if df.empty:
            return df
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[df.index.notna()].sort_index()
        return df

    @staticmethod
    def _normalize_kline_frame(raw: Any, fields: List[str]) -> pd.DataFrame:
        """把 easy_tdx K 线返回值转换为 index=time 的聚宽字段表。"""
        df = pd.DataFrame(raw).copy()
        if df.empty:
            return pd.DataFrame()
        df.rename(columns={"vol": "volume", "amount": "money"}, inplace=True)
        time_col = None
        for candidate in ("datetime", "date", "time"):
            if candidate in df.columns:
                time_col = candidate
                break
        if time_col is None:
            return pd.DataFrame()
        df["time"] = pd.to_datetime(df[time_col], errors="coerce")
        df.dropna(subset=["time"], inplace=True)
        df.set_index("time", inplace=True)
        df.sort_index(inplace=True)
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
        if "money" in df.columns:
            df["money"] = pd.to_numeric(df["money"], errors="coerce").fillna(0.0)
        for field in _DEFAULT_PRICE_FIELDS:
            if field not in df.columns:
                df[field] = 0.0
        if "high_limit" in fields and "high_limit" not in df.columns:
            df["high_limit"] = 0.0
        if "low_limit" in fields and "low_limit" not in df.columns:
            df["low_limit"] = 0.0
        if "paused" in fields and "paused" not in df.columns:
            df["paused"] = False
        keep = [field for field in fields if field in df.columns]
        return df[keep]

    @staticmethod
    def _filter_by_date(
        df: pd.DataFrame,
        *,
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
    ) -> pd.DataFrame:
        """按 start_date/end_date 裁剪 K 线结果。"""
        if df.empty:
            return df
        result = df.copy()
        start_ts = EasyTdxProvider._format_timestamp(start_date)
        end_ts = EasyTdxProvider._format_timestamp(end_date)
        if start_ts is not None:
            result = result[result.index >= start_ts]
        if end_ts is not None:
            result = result[result.index <= end_ts]
        return result

    @staticmethod
    def _as_float(value: Any, default: float = 0.0) -> float:
        """把通达信事件字段转换为浮点数；空值或异常时返回默认值。"""
        try:
            if value is None or pd.isna(value):
                return default
            return float(value)
        except Exception:
            return default

    def _fetch_xdxr_events(self, security: str) -> pd.DataFrame:
        """输入证券，返回事件表；读取失败抛错；成功表当天复用，未命中产生只读网络请求。"""
        key = self.jq_to_tdx(security)
        hit, cached = self._adjustment_cache.get(key)
        if hit:
            return cached
        try:
            client_cls = self._resolve_tdx_client_cls()
        except Exception as exc:
            raise RuntimeError("easy_tdx 除权接口不可用") from exc
        market, code = self.jq_to_tdx(security)
        failed_host = None
        for attempt in range(2):
            client = None
            event_host = None
            try:
                if self._xdxr_selected_day != datetime.now().date():
                    self._xdxr_host = None
                event_host = self._host or self._xdxr_host
                if not event_host and callable(getattr(client_cls, "from_best_host", None)):
                    from easy_tdx.config import get_known_hosts

                    hosts = [host for host in get_known_hosts() if host != failed_host]
                    if not hosts:
                        raise RuntimeError("easy_tdx 没有可用除权候选主机")
                    client = client_cls.from_best_host(
                        hosts=hosts,
                        port=self._port,
                        timeout=self._timeout,
                        auto_reconnect=False,
                        ping_timeout=1.0,
                    )
                else:
                    event_host = event_host or getattr(self._client, "_host", None)
                    client = client_cls(
                        host=event_host,
                        port=self._port,
                        timeout=self._timeout,
                        auto_reconnect=False,
                    )
                connect = getattr(client, "connect", None)
                if callable(connect):
                    connect()
                records = client.get_xdxr_info(market, code)
                selected = getattr(client, "_host", event_host)
                if self._xdxr_host != selected:
                    logger.info(
                        "easy_tdx xdxr_host=%s reason=%s",
                        selected,
                        "failover" if attempt else "startup_or_new_day",
                    )
                self._xdxr_host = selected
                self._xdxr_selected_day = datetime.now().date()
                break
            except Exception as exc:
                failed_host = getattr(client, "_host", event_host)
                self._xdxr_host = None
                network_error = (
                    isinstance(exc, (OSError, ConnectionError, TimeoutError))
                    or type(exc).__name__ == "TdxConnectionError"
                )
                # 记下每次失败的主机与原因：此前只留下“读取失败”，无法判断是超时、断连还是解析错误。
                logger.warning(
                    "easy_tdx xdxr_failed attempt=%s host=%s security=%s error=%s: %s",
                    attempt,
                    failed_host,
                    security,
                    type(exc).__name__,
                    exc,
                )
                if self._host or attempt or not network_error:
                    raise RuntimeError(
                        f"easy_tdx 除权事件读取失败: {type(exc).__name__}: {exc}"
                    ) from exc
            finally:
                if client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass
        df = pd.DataFrame(records).copy()
        if df.empty:
            df = pd.DataFrame()
            if records is not None:
                self._adjustment_cache.put(key, df)
            return df
        if "date" not in df.columns:
            raise RuntimeError("easy_tdx 除权事件缺少日期")
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        valid_dates = df["date"].notna().all()
        df.dropna(subset=["date"], inplace=True)
        if "category" in df.columns:
            df = df[df["category"] == 1]
        df = df.sort_values("date").reset_index(drop=True)
        if valid_dates:
            self._adjustment_cache.put(key, df)
        return df

    def _event_adjust_factor(self, event: pd.Series, daily_raw: pd.DataFrame) -> float:
        """按单条除权除息事件计算前复权乘数。"""
        event_day = pd.Timestamp(event["date"]).normalize()
        before = daily_raw[daily_raw.index < event_day]
        if before.empty or "close" not in before.columns:
            return 1.0
        preclose = self._as_float(before["close"].iloc[-1], 0.0)
        if preclose <= 0:
            return 1.0
        cash = self._as_float(event.get("fenhong"), 0.0)
        bonus_ratio = self._as_float(event.get("songzhuangu"), 0.0)
        rights_ratio = self._as_float(event.get("peigu"), 0.0)
        rights_price = self._as_float(event.get("peigujia"), 0.0)
        denominator = 1.0 + bonus_ratio + rights_ratio
        if denominator <= 0:
            return 1.0
        factor = (preclose - cash + rights_price * rights_ratio) / (preclose * denominator)
        return factor if factor > 0 else 1.0

    def _build_factor_for_index(
        self,
        client: Any,
        security: str,
        target_index: pd.DatetimeIndex,
        *,
        ref_date: Optional[Union[str, datetime, Date]] = None,
    ) -> pd.Series:
        """按 TDX 除权除息事件为目标时间索引构造累计前复权 factor。"""
        if target_index.empty:
            return pd.Series(dtype="float64")
        daily_raw = self._fetch_daily_raw_for_factor(client, security)
        if daily_raw.empty:
            raise RuntimeError("easy_tdx 构造复权因子缺少原始日线")
        events = self._fetch_xdxr_events(security)

        target_ts = pd.DatetimeIndex(pd.to_datetime(target_index, errors="coerce")).dropna()
        if target_ts.empty:
            return pd.Series(dtype="float64")
        horizon = max(target_ts.max().normalize(), daily_raw.index.max().normalize())
        ref_ts = self._format_timestamp(ref_date)
        if ref_ts is not None:
            horizon = max(horizon, ref_ts.normalize())

        daily = daily_raw[daily_raw.index <= horizon].copy()
        if daily.empty:
            return pd.Series(dtype="float64")
        daily_days = pd.DatetimeIndex(daily.index.normalize()).drop_duplicates().sort_values()
        factor_by_day = pd.Series(1.0, index=daily_days, dtype="float64")
        origin_complete = True
        for _, event in events.iterrows():
            event_day = pd.Timestamp(event["date"]).normalize()
            if event_day > horizon:
                continue
            before = daily_raw[daily_raw.index < event_day]
            if before.empty:
                # 早于已知行情起点的事件不影响区间内前复权比值，但绝对后复权不可验证。
                origin_complete = False
                continue
            decimals = 3 if self._infer_jq_security_type(security) == "fund" else 2
            previous = before.index[-1]
            event_factor = float(
                AdjustmentEvent(
                    date=event_day.date(),
                    cash_per_share=str(event.get("fenhong", 0.0)),
                    gift=str(event.get("songzhuangu", 0.0)),
                    transfer="0",
                    rights=str(event.get("peigu", 0.0)),
                    rights_price=str(event.get("peigujia", 0.0)),
                    previous_close=str(round(float(before["close"].iloc[-1]), decimals)),
                    previous_close_date=previous.date(),
                ).multiplier(decimals)
            )
            if event_factor == 1.0:
                continue
            factor_by_day.loc[factor_by_day.index < event_day] *= event_factor

        target_days = pd.DatetimeIndex(pd.to_datetime(target_index, errors="coerce")).normalize()
        aligned = factor_by_day.reindex(target_days, method="ffill")
        aligned = aligned.bfill().fillna(1.0)
        result = pd.Series(aligned.to_numpy(dtype="float64"), index=target_index, dtype="float64")
        reference = ref_ts.normalize() if ref_ts is not None else daily_days[-1]
        visible = factor_by_day.loc[factor_by_day.index <= reference]
        if visible.empty:
            raise RuntimeError("easy_tdx 复权参考日早于已知历史")
        result.attrs["reference_factor"] = float(visible.iloc[-1])
        result.attrs["origin_factor"] = float(factor_by_day.iloc[0])
        result.attrs["origin_complete"] = origin_complete
        return result

    @staticmethod
    def _factor_ref_value(
        factor: pd.Series,
        *,
        ref_date: Optional[Union[str, datetime, Date]],
    ) -> float:
        """从 factor 序列中提取动态前复权参考值。"""
        if factor.empty:
            return 1.0
        if "reference_factor" in factor.attrs:
            return float(factor.attrs["reference_factor"])
        if ref_date is None:
            value = factor.iloc[-1]
            return float(value) if pd.notna(value) and value else 1.0
        ref_ts = EasyTdxProvider._format_timestamp(ref_date)
        if ref_ts is None:
            value = factor.iloc[-1]
            return float(value) if pd.notna(value) and value else 1.0
        sub = factor[factor.index <= ref_ts]
        value = sub.iloc[-1] if not sub.empty else factor.iloc[-1]
        return float(value) if pd.notna(value) and value else 1.0

    def _apply_constructed_pre_adjustment(
        self,
        frame: pd.DataFrame,
        *,
        factor: pd.Series,
        ref_date: Optional[Union[str, datetime, Date]],
        mode: str = "pre",
        price_decimals: Optional[int] = None,
    ) -> pd.DataFrame:
        """使用构造出的 factor 对未复权 K 线执行动态前复权。"""
        if frame.empty or factor.empty:
            return frame
        result = frame.copy()
        aligned = factor.reindex(result.index).ffill().bfill().fillna(1.0)
        factor_ref = self._factor_ref_value(factor, ref_date=ref_date)
        if mode == "post":
            if not factor.attrs.get("origin_complete", True):
                raise RuntimeError("easy_tdx 后复权原点之前的事件缺少历史价格，无法验证")
            factor_ref = float(factor.attrs["origin_factor"])
        ratio = (
            aligned / factor_ref if mode in {"pre", "post"} else pd.Series(1.0, index=result.index)
        )
        for field in _PRICE_FIELDS:
            if field in result.columns:
                result[field] = pd.to_numeric(result[field], errors="coerce") * ratio
                if price_decimals is not None:
                    result[field] = result[field].round(price_decimals)
        if mode in {"pre", "post"} and "volume" in result:
            result["volume"] = np.round(pd.to_numeric(result["volume"]) / ratio)
        result["factor"] = ratio
        return result

    def _assemble_price_result(
        self,
        frames: Dict[str, pd.DataFrame],
        *,
        fields: List[str],
        securities: List[str],
        panel: bool,
    ) -> pd.DataFrame:
        """按聚宽兼容 shape 组装单证券、多证券、panel 和长表。"""
        if len(securities) == 1:
            return frames.get(securities[0], pd.DataFrame(columns=fields))
        if not frames:
            return pd.DataFrame(columns=fields)
        if panel:
            parts = []
            for field in fields:
                field_frames = []
                for security in securities:
                    df = frames.get(security)
                    if df is None or df.empty:
                        continue
                    series = (
                        df[field]
                        if field in df.columns
                        else pd.Series(index=df.index, dtype="float64")
                    )
                    field_frames.append(series.rename(security))
                if field_frames:
                    parts.append(pd.concat(field_frames, axis=1))
                else:
                    parts.append(pd.DataFrame(columns=securities))
            wide = pd.concat(parts, axis=1, keys=fields)
            wide.columns.names = ["field", "code"]
            wide.sort_index(inplace=True)
            return wide
        rows = []
        for security in securities:
            df = frames.get(security)
            if df is None or df.empty:
                continue
            part = df.copy().reset_index().rename(columns={"index": "time"})
            if "time" not in part.columns:
                part.rename(columns={part.columns[0]: "time"}, inplace=True)
            part["code"] = security
            for field in fields:
                if field not in part.columns:
                    part[field] = False if field == "paused" else 0.0
            rows.append(part[["time", "code"] + fields])
        if not rows:
            return pd.DataFrame()
        return (
            pd.concat(rows, ignore_index=True).sort_values(["time", "code"]).reset_index(drop=True)
        )

    def get_price(
        self,
        security: Union[str, List[str]],
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        frequency: str = "daily",
        fields: Optional[List[str]] = None,
        skip_paused: bool = False,
        fq: str = "pre",
        count: Optional[int] = None,
        panel: bool = True,
        fill_paused: bool = True,
        pre_factor_ref_date: Optional[Union[str, datetime]] = None,
        prefer_engine: bool = False,
        force_no_engine: bool = False,
    ) -> pd.DataFrame:
        """获取通达信 K 线；前复权优先使用除权除息事件构造动态 factor。"""
        _ = skip_paused, fill_paused, prefer_engine, force_no_engine
        securities = self._as_list(security)
        if not securities:
            return pd.DataFrame()
        requested_fields = list(fields or _DEFAULT_PRICE_FIELDS)
        needs_factor = "factor" in requested_fields
        frequency_norm = self._normalize_frequency(frequency)
        if self._use_stub:
            return self._stub_get_price(
                securities,
                fields=requested_fields,
                frequency=frequency_norm,
                start_date=start_date,
                end_date=end_date,
                count=count,
                panel=panel,
            )
        client = self._ensure_client()
        if client is None:
            return pd.DataFrame()
        fetch_count = self._resolve_fetch_count(
            start_date=start_date,
            end_date=end_date,
            frequency=frequency_norm,
            count=count,
        )
        period = self._resolve_period(frequency_norm)
        mode = {"qfq": "pre", "hfq": "post"}.get(str(fq).lower(), str(fq or "none").lower())
        use_constructed_pre = mode in {"pre", "post"} or needs_factor
        adjust = 0
        frames: Dict[str, pd.DataFrame] = {}
        for sec in securities:
            fetch_fields = list(requested_fields)
            if use_constructed_pre and all(field == "factor" for field in fetch_fields):
                fetch_fields = ["close"]
            frame = self._fetch_single_kline(
                client,
                sec,
                period=period,
                fetch_count=fetch_count,
                adjust=adjust,
                fields=fetch_fields,
                start_date=start_date,
                end_date=end_date,
                count=count,
            )
            if use_constructed_pre and not frame.empty:
                factor = self._build_factor_for_index(
                    client,
                    sec,
                    pd.DatetimeIndex(frame.index),
                    ref_date=pre_factor_ref_date,
                )
                if factor.empty:
                    raise RuntimeError("easy_tdx 无法构造复权因子，不能静默切换到其他复权算法")
                frame = self._apply_constructed_pre_adjustment(
                    frame,
                    factor=factor,
                    ref_date=pre_factor_ref_date,
                    mode=mode,
                    price_decimals=(3 if self._infer_jq_security_type(sec) == "fund" else 2),
                )
            for field in requested_fields:
                if field not in frame.columns:
                    frame[field] = 0.0
            frame = frame[[field for field in requested_fields if field in frame.columns]]
            if not frame.empty:
                frames[sec] = frame
        return self._assemble_price_result(
            frames,
            fields=requested_fields,
            securities=securities,
            panel=panel,
        )

    def get_bars(
        self,
        security: Union[str, List[str]],
        count: int,
        unit: str = "1d",
        fields: Optional[List[str]] = None,
        include_now: bool = False,
        end_dt: Optional[Union[str, datetime]] = None,
        fq_ref_date: Union[int, datetime] = 1,
        df: bool = False,
    ) -> Any:
        """用 get_price 提供聚宽 get_bars 兼容入口。"""
        _ = include_now, fq_ref_date
        request_fields = [field for field in (fields or _DEFAULT_PRICE_FIELDS) if field != "date"]
        frequency = "daily" if str(unit).lower() in {"1d", "d", "day", "daily"} else unit
        result = self.get_price(
            security=security,
            end_date=end_dt,
            frequency=frequency,
            fields=request_fields,
            count=count,
            fq="pre",
            panel=df,
        )
        if df or not isinstance(result, pd.DataFrame):
            return result
        return result.to_dict()

    def get_trade_days(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        count: Optional[int] = None,
    ) -> List[datetime]:
        """用上证指数日线日期近似通达信交易日历。"""
        if self._use_stub:
            days = self._stub_trade_days(start_date=start_date, end_date=end_date, count=count)
            return days
        client = self._ensure_client()
        if client is None:
            return []
        fetch_count = self._resolve_fetch_count(
            start_date=start_date,
            end_date=end_date,
            frequency="daily",
            count=count or 3000,
        )
        raw = self._call(
            "get_stock_kline",
            market=_MARKET_SH,
            code="999999",
            period=4,
            start=0,
            count=min(fetch_count, self._MAX_DAILY_FETCH),
            times=1,
            adjust=0,
        )
        df = self._normalize_kline_frame(raw, ["close"])
        df = self._filter_by_date(df, start_date=start_date, end_date=end_date)
        days = [pd.Timestamp(idx).to_pydatetime() for idx in df.index]
        if count:
            days = days[-int(count) :]
        return days

    def get_all_securities(
        self,
        types: Union[str, List[str]] = "stock",
        date: Optional[Union[str, datetime]] = None,
    ) -> pd.DataFrame:
        """获取证券列表；online 模式依赖 easy_tdx quotes-list 能力。"""
        _ = date
        if self._use_stub:
            return self._stub_all_securities(types)
        client = self._ensure_client()
        if client is None:
            return pd.DataFrame()
        getter = getattr(client, "get_stock_quotes_list", None)
        if not callable(getter):
            return pd.DataFrame()
        type_list = [types] if isinstance(types, str) else list(types or [])
        rows = []
        categories = self._resolve_categories(type_list)
        for category, label in categories:
            # 经 _call 统一处理：失败先重置连接；不再吞异常返回残缺列表。
            try:
                raw = self._call("get_stock_quotes_list", category, count=5000)
            except TypeError:
                raw = self._call("get_stock_quotes_list", category)
            df = pd.DataFrame(raw)
            if df.empty:
                continue
            for _, row in df.iterrows():
                code = str(row.get("code", "")).strip()
                if not code:
                    continue
                market = int(row.get("market", _MARKET_SZ) or _MARKET_SZ)
                jq_code = self.tdx_to_jq(market, code)
                rows.append(
                    {
                        "code": jq_code,
                        "display_name": row.get("name") or jq_code,
                        "name": row.get("name") or code,
                        "start_date": Date(1990, 12, 19),
                        "end_date": Date(2200, 1, 1),
                        "type": label,
                    }
                )
        if not rows:
            return pd.DataFrame(columns=["display_name", "name", "start_date", "end_date", "type"])
        return pd.DataFrame(rows).drop_duplicates(subset=["code"]).set_index("code")

    def _resolve_categories(self, types: List[str]) -> List[Tuple[Any, str]]:
        """根据请求类型解析 easy_tdx 分类枚举或保守整数。"""
        try:
            category_module = importlib.import_module("easy_tdx.mac.enums")
            category = getattr(category_module, "Category")
            stock_cat = getattr(category, "A", 0)
            index_cat = getattr(category, "ZS", 1)
            fund_cat = getattr(category, "ETF", 2)
        except Exception:
            stock_cat, index_cat, fund_cat = 0, 1, 2
        requested = {str(item).lower() for item in types} or {"stock"}
        result = []
        if "all" in requested or "stock" in requested:
            result.append((stock_cat, "stock"))
        if "all" in requested or "index" in requested:
            result.append((index_cat, "index"))
        if "all" in requested or {"fund", "etf"} & requested:
            result.append((fund_cat, "fund"))
        return result

    def get_index_stocks(
        self,
        index_symbol: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> List[str]:
        """获取指数成分股；online 无法稳定取得时返回空列表。"""
        _ = date
        if self._use_stub:
            return ["000001.XSHE", "600519.XSHG"] if index_symbol else []
        client = self._ensure_client()
        if client is None:
            return []
        getter = getattr(client, "get_board_members", None)
        if not callable(getter):
            return []
        market, code = self.jq_to_tdx(index_symbol)
        board_code = f"{market}{code}"
        try:
            raw = self._call("get_board_members", board_code)
        except Exception as exc:
            # _call 已重置连接，成分股查询失败维持旧约定返回空列表。
            logger.warning("easy_tdx get_index_stocks_failed %s: %s", index_symbol, exc)
            return []
        df = pd.DataFrame(raw)
        if df.empty:
            return []
        result = []
        for _, row in df.iterrows():
            item_code = str(row.get("code", "")).strip()
            if item_code:
                result.append(self.tdx_to_jq(int(row.get("market", _MARKET_SZ)), item_code))
        return result

    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> List[Dict[str, Any]]:
        """读取通达信除权除息记录；不可用时返回空列表。"""
        if self._use_stub:
            return []
        try:
            client_cls = self._resolve_tdx_client_cls()
        except Exception:
            return []
        market, code = self.jq_to_tdx(security)
        try:
            client = client_cls(host=self._host, port=self._port, timeout=self._timeout)
            connect = getattr(client, "connect", None)
            if callable(connect):
                connect()
            records = client.get_xdxr_info(market, code)
        except Exception:
            return []
        finally:
            try:
                client.close()
            except Exception:
                pass
        start_ts = self._format_timestamp(start_date)
        end_ts = self._format_timestamp(end_date)
        records_df = pd.DataFrame(records).copy()
        if records_df.empty:
            return []
        if "category" in records_df.columns:
            records_df = records_df[records_df["category"] == 1]
        result = []
        for _, rec in records_df.iterrows():
            event_date = self._record_date(rec)
            if event_date is None:
                continue
            if start_ts is not None and pd.Timestamp(event_date) < start_ts.normalize():
                continue
            if end_ts is not None and pd.Timestamp(event_date) > end_ts.normalize():
                continue
            split = self._as_float(rec.get("songzhuangu", 0.0), 0.0) + self._as_float(
                rec.get("peigu", 0.0), 0.0
            )
            result.append(
                {
                    "security": security,
                    "date": event_date,
                    "scale_factor": 1.0 + split,
                    "bonus_pre_tax": self._as_float(rec.get("fenhong", 0.0), 0.0),
                    "per_base": 1.0,
                }
            )
        return result

    @staticmethod
    def _record_date(record: Any) -> Optional[Date]:
        """从 easy_tdx 除权记录对象中提取日期。"""
        if hasattr(record, "get"):
            raw_date = record.get("date")
            if raw_date is not None:
                try:
                    return pd.Timestamp(raw_date).date()
                except Exception:
                    return None
        try:
            return Date(int(record.year), int(record.month), int(record.day))
        except Exception:
            raw_date = getattr(record, "date", None)
            if raw_date is None:
                return None
            try:
                return pd.Timestamp(raw_date).date()
            except Exception:
                return None

    def get_security_info(
        self,
        security: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        """返回证券基础信息，online 模式尽量从 quote/info 中补名称。"""
        _ = date
        code = str(security)
        display_name = _STUB_NAMES.get(code, code)
        if not self._use_stub:
            display_name = self._lookup_display_name(code, display_name)
        kind = self._infer_jq_security_type(code)
        return {
            "code": security,
            "display_name": display_name,
            "name": display_name,
            "start_date": Date(1990, 12, 19),
            "end_date": Date(2200, 1, 1),
            "type": kind,
        }

    _NAME_FAILURE_TTL = 300.0

    def _lookup_display_name(self, code: str, fallback: str) -> str:
        """输入证券代码与兜底名，返回缓存或在线名称；失败时已重置连接并返回兜底名。"""
        today = datetime.now().date()
        if self._name_cache_day != today:
            self._name_cache.clear()
            self._name_cache_day = today
        cached = self._name_cache.get(code)
        if cached is not None and (cached[1] == 0.0 or time.monotonic() < cached[1]):
            return cached[0]
        try:
            client = self._ensure_client()
            if client is None or not callable(getattr(client, "get_symbol_info", None)):
                return fallback
            market, raw_code = self.jq_to_tdx(code)
            raw_info = self._call("get_symbol_info", market, raw_code)
            name = self._extract_symbol_name(raw_info, market, raw_code)
        except Exception as exc:
            # 名称只用于展示，降级为代码并短暂缓存失败结果；但连接必须先重置
            # （_call 内的异常已关闭，这里兜住标的核对等 _call 之外的失败）。
            self.close()
            logger.warning("easy_tdx display_name_failed %s: %s", code, exc)
            self._name_cache[code] = (fallback, time.monotonic() + self._NAME_FAILURE_TTL)
            return fallback
        if not name:
            return fallback
        self._name_cache[code] = (name, 0.0)
        return name

    @staticmethod
    def _extract_symbol_name(raw_info: Any, market: int, code: str) -> Optional[str]:
        """输入 get_symbol_info 结果（DataFrame 或对象），返回名称；标的不符时抛串包错误。"""
        if isinstance(raw_info, pd.DataFrame):
            if raw_info.empty:
                return None
            row = raw_info.iloc[0]
            got_market, got_code, name = row.get("market"), row.get("code"), row.get("name")
        else:
            got_market = getattr(raw_info, "market", None)
            got_code = getattr(raw_info, "code", None)
            name = getattr(raw_info, "name", None)
        if got_code is not None and pd.notna(got_code):
            if str(got_code).zfill(6) != str(code).zfill(6) or (
                got_market is not None and pd.notna(got_market) and int(got_market) != int(market)
            ):
                raise TdxResponseMismatchError(
                    f"easy_tdx 名称查询返回了其他标的: request={market}/{code} "
                    f"response={got_market}/{got_code}"
                )
        if name is None or (not isinstance(name, str) and pd.isna(name)):
            return None
        return str(name).strip() or None

    def get_current_tick(
        self,
        security: str,
        dt: Optional[Union[str, datetime]] = None,
        df: bool = False,
    ) -> Optional[Any]:
        """输入证券、可选逻辑时间及 df 开关，返回实时字典或 DataFrame。

        真实模式依赖 SDK 报价；仅明确停牌且零价时用有效昨收补齐最后价，保留源时间。
        空报价返回 None 或空表；连接异常沿既有换址流程传播，stub 仅返回测试数据。
        """
        if self._use_stub:
            tick = self._stub_current_tick(security)
            return pd.DataFrame([tick]) if df else tick
        client = self._ensure_client()
        if client is None:
            return pd.DataFrame() if df else None
        getter = getattr(client, "get_stock_quotes", None)
        if not callable(getter):
            return pd.DataFrame() if df else None
        market, code = self.jq_to_tdx(security)
        try:
            from easy_tdx.codec.bitmap import FieldBit, PresetField

            fields = list(PresetField.COMMON.value) + [
                FieldBit.SERVER_UPDATE_DATE,
                FieldBit.SERVER_UPDATE_TIME,
            ]
        except ImportError:
            fields = None
        try:
            if fields is None:
                quotes = self._call("get_stock_quotes", [(market, code)])
            else:
                quotes = self._call("get_stock_quotes", [(market, code)], fields=fields)
        except TypeError as exc:
            if "fields" not in str(exc):
                raise
            quotes = self._call("get_stock_quotes", [(market, code)])
        received = datetime.now()
        quote_df = pd.DataFrame(quotes)
        if quote_df.empty:
            return pd.DataFrame() if df else None
        row = quote_df.iloc[0]
        got_code = row.get("code")
        if got_code is not None and pd.notna(got_code):
            got_market = row.get("market")
            if str(got_code).zfill(6) != str(code).zfill(6) or (
                got_market is not None and pd.notna(got_market) and int(got_market) != int(market)
            ):
                self._reject_misaligned(
                    f"easy_tdx 报价返回了其他标的: request={security} response={got_market}/{got_code}"
                )
        raw_last_price = float(
            row.get("price", row.get("last_price", row.get("close", 0.0))) or 0.0
        )
        status = int(row.get("trading_status", 0) or 0)
        tag_value = row.get("stock_tag_flags")
        tag_flags = int(tag_value) if pd.notna(tag_value) else 0
        paused = bool(status & _TDX_PAUSED_STATUS or tag_flags & _MAC_PAUSED_TAG)
        previous_close = float(row.get("pre_close", 0.0) or 0.0)
        last_price = raw_last_price
        price_source = "quote"
        # 停牌的零价不是成交价；只有明确停牌且昨收有效，才提供可审计的估值价格。
        if paused and raw_last_price == 0 and math.isfinite(previous_close) and previous_close > 0:
            last_price = previous_close
            price_source = "pre_close"
        limit_up = row.get("limit_up", row.get("buy_price_limit", 0.0))
        limit_down = row.get("limit_down", row.get("sell_price_limit", 0.0))
        self._check_quote_consistency(
            security,
            row,
            last_price=raw_last_price,
            previous_close=previous_close,
            limit_up=self._as_float(limit_up, 0.0),
            limit_down=self._as_float(limit_down, 0.0),
        )
        tick = {
            "sid": security,
            "symbol": security,
            "last_price": last_price,
            "paused": paused,
            "open": float(row.get("open", 0.0) or 0.0),
            "high": float(row.get("high", 0.0) or 0.0),
            "low": float(row.get("low", 0.0) or 0.0),
            "volume": float(row.get("vol", row.get("volume", 0.0)) or 0.0) * 100.0,
            "amount": float(row.get("amount", 0.0) or 0.0),
            "pre_close": previous_close,
            "limit_up": float(limit_up or 0.0),
            "limit_down": float(limit_down or 0.0),
            "trading_status": status,
            "dt": str(dt or datetime.now()),
            "provider": self.name,
        }
        if pd.notna(tag_value) or price_source == "pre_close":
            tick.update(
                stock_tag_flags=tag_flags,
                raw_last_price=raw_last_price,
                last_price_source=price_source,
            )
        # 仅使用服务器实际回报的日期/时间；接收时刻不能冒充行情源时间。
        day, clock = row.get("server_update_date"), row.get("server_update_time")
        if pd.notna(day) and pd.notna(clock):
            try:
                source = datetime.strptime(f"{int(day):08d}{int(clock):06d}", "%Y%m%d%H%M%S")
                tick.update(
                    source_time=source.isoformat(),
                    received_time=received.isoformat(),
                    query_completed_time=received.isoformat(),
                    age_seconds=max((received - source).total_seconds(), 0.0),
                )
            except (ValueError, TypeError, OverflowError):
                pass
        return pd.DataFrame([tick]) if df else tick

    # 内容防线阈值：涨跌停外留 1% 浮点余量；无涨跌停时按数量级（0.5~2 倍）判断。
    _LIMIT_TOLERANCE = 0.01
    _MAGNITUDE_LOW = 0.5
    _MAGNITUDE_HIGH = 2.0
    # 历史日线相对昨收的容忍更宽，兼顾 10 送 10 等除权缺口。
    _HISTORY_MAGNITUDE_LOW = 1.0 / 3.0
    _HISTORY_MAGNITUDE_HIGH = 3.0

    def _reject_misaligned(self, message: str) -> None:
        """输入原因，关闭连接并抛出 TdxResponseMismatchError；用于内容防线判定的串包。"""
        self.close()
        logger.error("easy_tdx misaligned_response %s", message)
        raise TdxResponseMismatchError(message)

    @staticmethod
    def _quote_day(row: Any) -> Optional[Date]:
        """输入报价行，返回服务器行情日期；缺失或非法时返回 None。"""
        day = row.get("server_update_date") if hasattr(row, "get") else None
        if day is None or pd.isna(day):
            return None
        try:
            return datetime.strptime(f"{int(day):08d}", "%Y%m%d").date()
        except (ValueError, TypeError, OverflowError):
            return None

    def _check_quote_consistency(
        self,
        security: str,
        row: Any,
        *,
        last_price: float,
        previous_close: float,
        limit_up: float,
        limit_down: float,
    ) -> None:
        """输入报价核心字段，价格与昨收/涨跌停数量级不一致时按串包处理；一致则记录当日锚点。"""
        prices = [last_price] + [
            self._as_float(row.get(name), 0.0) for name in ("open", "high", "low")
        ]
        prices = [value for value in prices if math.isfinite(value) and value > 0]
        has_limits = limit_up > 0 and limit_down > 0 and limit_up >= limit_down
        if has_limits:
            low_bound = limit_down * (1 - self._LIMIT_TOLERANCE)
            high_bound = limit_up * (1 + self._LIMIT_TOLERANCE)
            outside = [value for value in prices if not low_bound <= value <= high_bound]
            if previous_close > 0 and not low_bound <= previous_close <= high_bound:
                outside.append(previous_close)
            if outside:
                self._reject_misaligned(
                    f"easy_tdx 报价超出涨跌停区间: {security} prices={outside} "
                    f"limit=[{limit_down}, {limit_up}]"
                )
        elif previous_close > 0 and self._infer_jq_security_type(security) != "stock":
            # 个股无涨跌停多为新股上市初期，涨幅可达数倍，不做数量级判断。
            outside = [
                value
                for value in prices
                if not self._MAGNITUDE_LOW <= value / previous_close <= self._MAGNITUDE_HIGH
            ]
            if outside:
                self._reject_misaligned(
                    f"easy_tdx 报价与昨收数量级不符: {security} prices={outside} "
                    f"pre_close={previous_close}"
                )
        day = self._quote_day(row)
        if day is None or not (math.isfinite(previous_close) and previous_close > 0):
            return
        cached = self._price_refs.get(security)
        if cached is not None and cached[0] == day:
            ratio = previous_close / cached[1]
            if not self._MAGNITUDE_LOW <= ratio <= self._MAGNITUDE_HIGH:
                self._reject_misaligned(
                    f"easy_tdx 同日昨收突变: {security} cached={cached[1]} now={previous_close}"
                )
        self._price_refs[security] = (
            day,
            previous_close,
            limit_up if has_limits else 0.0,
            limit_down if has_limits else 0.0,
        )

    def _check_kline_consistency(self, security: str, df: pd.DataFrame) -> None:
        """输入未复权 K 线，若与当日报价锚点数量级不一致则按串包处理；无锚点时跳过。"""
        ref = self._price_refs.get(security)
        if ref is None or df.empty:
            return
        day, previous_close, limit_up, limit_down = ref
        index_days = pd.DatetimeIndex(df.index).normalize()
        today_rows = df[index_days == pd.Timestamp(day)]
        price_cols = [name for name in ("open", "high", "low", "close") if name in df.columns]
        if not price_cols:
            return
        if not today_rows.empty:
            values = pd.to_numeric(today_rows[price_cols].stack(), errors="coerce")
            values = values[values > 0]
            if limit_up > 0 and limit_down > 0:
                low_bound = limit_down * (1 - self._LIMIT_TOLERANCE)
                high_bound = limit_up * (1 + self._LIMIT_TOLERANCE)
            elif self._infer_jq_security_type(security) == "stock":
                low_bound, high_bound = 0.0, math.inf
            else:
                low_bound = previous_close * self._MAGNITUDE_LOW
                high_bound = previous_close * self._MAGNITUDE_HIGH
            bad = values[(values < low_bound) | (values > high_bound)]
            if not bad.empty:
                self._reject_misaligned(
                    f"easy_tdx K线与当日报价不一致: {security} value={float(bad.iloc[0])} "
                    f"range=[{low_bound:.4f}, {high_bound:.4f}]"
                )
        before = df[index_days < pd.Timestamp(day)]
        if not before.empty and "close" in before.columns:
            last_close = self._as_float(before["close"].iloc[-1], 0.0)
            if last_close > 0:
                ratio = last_close / previous_close
                if not self._HISTORY_MAGNITUDE_LOW <= ratio <= self._HISTORY_MAGNITUDE_HIGH:
                    self._reject_misaligned(
                        f"easy_tdx 历史K线与昨收数量级不符: {security} last_close={last_close} "
                        f"pre_close={previous_close}"
                    )

    def get_live_current(self, security: str) -> Dict[str, Any]:
        """输入证券代码，返回实时价格、停牌状态及源时间审计字典。

        依赖 get_current_tick 的 SDK 查询；无报价返回空字典，网络异常传播。
        Mac 快照附原始价、标签和价格来源。非停牌行情不声明频道健康，过期即由下游拒绝；
        仅当本次查询成功且证券明确停牌、价格取自昨收时，附 feed_health=healthy（basis=paused_reference），
        与 MiniQMT 停牌快照语义一致，使下游接受停牌期间不再推进的源时间。
        """
        tick = self.get_current_tick(security)
        if not isinstance(tick, dict):
            return {}
        status = int(tick.get("trading_status", 0) or 0)
        result = {
            "last_price": float(tick.get("last_price", 0.0) or 0.0),
            "high_limit": float(tick.get("limit_up", 0.0) or 0.0),
            "low_limit": float(tick.get("limit_down", 0.0) or 0.0),
            "paused": bool(tick.get("paused", status & _TDX_PAUSED_STATUS)),
        }
        for name in (
            "source_time",
            "received_time",
            "query_completed_time",
            "age_seconds",
            "stock_tag_flags",
            "raw_last_price",
            "last_price_source",
        ):
            if name in tick:
                result[name] = tick[name]
        if result["paused"] and result["last_price"] > 0 and result.get("last_price_source") == "pre_close":
            result["feed_health"] = {
                "status": "healthy",
                "query_succeeded": True,
                "transport": "easy_tdx.quotes",
                "basis": "paused_reference",
            }
        return result

    def _stub_get_price(
        self,
        securities: List[str],
        *,
        fields: List[str],
        frequency: str,
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
        count: Optional[int],
        panel: bool,
    ) -> pd.DataFrame:
        """生成显式 stub 模式行情，仅用于单元测试和演示。"""
        end_ts = self._format_timestamp(end_date) or pd.Timestamp("2026-04-01")
        if start_date is not None:
            start_ts = self._format_timestamp(start_date) or (end_ts - pd.Timedelta(days=10))
        elif count is not None:
            start_ts = end_ts - pd.Timedelta(days=max(int(count) * 2, 1))
        else:
            start_ts = end_ts - pd.Timedelta(days=10)
        dates = []
        if frequency == "1m":
            current = start_ts.normalize() + pd.Timedelta(hours=9, minutes=30)
            while current <= end_ts:
                if current.weekday() < 5:
                    dates.append(current)
                current += pd.Timedelta(minutes=1)
        else:
            for day in pd.bdate_range(start_ts.normalize(), end_ts.normalize()):
                dates.append(pd.Timestamp(day))
        if count:
            dates = dates[-int(count) :]
        frames = {}
        for idx, sec in enumerate(securities):
            base = 10.0 + idx * 3.0
            rows = []
            for offset, ts in enumerate(dates):
                close = base + offset * 0.01
                rows.append(
                    {
                        "time": ts,
                        "open": round(close - 0.02, 3),
                        "close": round(close, 3),
                        "high": round(close + 0.03, 3),
                        "low": round(close - 0.04, 3),
                        "volume": 100000.0 + offset * 100.0,
                        "money": (100000.0 + offset * 100.0) * close,
                        "high_limit": round(close * 1.1, 3),
                        "low_limit": round(close * 0.9, 3),
                        "paused": False,
                    }
                )
            frame = pd.DataFrame(rows).set_index("time") if rows else pd.DataFrame()
            for field in fields:
                if field not in frame.columns:
                    frame[field] = False if field == "paused" else 0.0
            frames[sec] = frame[fields] if not frame.empty else frame
        return self._assemble_price_result(
            frames, fields=fields, securities=securities, panel=panel
        )

    @staticmethod
    def _stub_trade_days(
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
        count: Optional[int],
    ) -> List[datetime]:
        """生成显式 stub 模式交易日。"""
        end_ts = EasyTdxProvider._format_timestamp(end_date) or pd.Timestamp("2026-04-01")
        start_ts = EasyTdxProvider._format_timestamp(start_date) or (end_ts - pd.Timedelta(days=30))
        days = [pd.Timestamp(day).to_pydatetime() for day in pd.bdate_range(start_ts, end_ts)]
        if count:
            days = days[-int(count) :]
        return days

    @staticmethod
    def _stub_all_securities(types: Union[str, List[str]]) -> pd.DataFrame:
        """生成显式 stub 模式证券列表。"""
        requested = {types} if isinstance(types, str) else set(types or [])
        rows = []
        for code, name in _STUB_NAMES.items():
            kind = EasyTdxProvider._infer_jq_security_type(code)
            if requested and "all" not in requested and kind not in requested:
                continue
            rows.append(
                {
                    "code": code,
                    "display_name": name,
                    "name": name,
                    "start_date": Date(1990, 12, 19),
                    "end_date": Date(2200, 1, 1),
                    "type": kind,
                }
            )
        return pd.DataFrame(rows).set_index("code") if rows else pd.DataFrame()

    @staticmethod
    def _stub_current_tick(security: str) -> Dict[str, Any]:
        """生成显式 stub 模式实时快照。"""
        return {
            "sid": security,
            "symbol": security,
            "last_price": 10.0,
            "open": 9.9,
            "high": 10.1,
            "low": 9.8,
            "volume": 100000.0,
            "amount": 1000000.0,
            "pre_close": 9.95,
            "limit_up": 10.95,
            "limit_down": 8.96,
            "trading_status": 0,
            "provider": "easy_tdx_stub",
        }
