from __future__ import annotations

import asyncio
import errno
import hmac
import os
import secrets
import time
from typing import Any, Dict, Optional

from bullet_trade.core.globals import log

from .protocol import ProtocolError, read_message, write_message

PROTOCOL_VERSION = 1


class ClientSession:
    """
    维护单个 TCP 连接的状态：握手、请求处理、事件推送等。
    """

    # 请求超时时间（秒），超过此时间未完成的请求会被取消
    REQUEST_TIMEOUT = 60.0
    HISTORY_REQUEST_TIMEOUT = 150.0
    PLACE_ORDER_TIMEOUT_MARGIN = 30.0
    REQUEST_TIMEOUT_ENV = ("QMT_SERVER_REQUEST_TIMEOUT_SECONDS", "QMT_SERVER_REQUEST_TIMEOUT")
    PLACE_ORDER_TIMEOUT_MARGIN_ENV = (
        "QMT_SERVER_PLACE_ORDER_TIMEOUT_MARGIN_SECONDS",
        "QMT_SERVER_PLACE_ORDER_TIMEOUT_MARGIN",
    )

    def __init__(self, app: "ServerApplication", reader: asyncio.StreamReader, writer: asyncio.StreamWriter, peername: str):
        self.app = app
        self.reader = reader
        self.writer = writer
        self.peername = peername
        self.session_id = f"s{secrets.token_hex(6)}"
        self.account_key: Optional[str] = None
        self.sub_account_id: Optional[str] = None
        self.features = {}
        self._active = False
        self._send_lock = asyncio.Lock()
        self._last_ping = time.time()
        self._current_request: Optional[str] = None  # 当前正在处理的请求 action
        self._current_broker_called: Optional[bool] = None

    async def run(self) -> None:
        try:
            await self._handshake()
            await self._loop()
        except asyncio.IncompleteReadError as exc:
            self._log_expected_disconnect(exc)
        except ProtocolError as exc:
            log.warning(f"session {self.session_id} 协议错误: {exc}")
        except Exception as exc:
            if self._is_expected_disconnect(exc):
                self._log_expected_disconnect(exc)
                return
            log.error(f"session {self.session_id} 异常: {exc}")
        finally:
            await self.close()

    def _log_expected_disconnect(self, exc: BaseException) -> None:
        message = f"session {self.session_id} 客户端断开: {exc}"
        if self._current_request:
            log.warning(f"{message}, current_request={self._current_request}")
        else:
            log.debug(message)

    @staticmethod
    def _is_expected_disconnect(exc: BaseException) -> bool:
        if isinstance(exc, (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError, ConnectionAbortedError)):
            return True
        if not isinstance(exc, OSError):
            return False
        if getattr(exc, "winerror", None) in (64, 10053, 10054):
            return True
        return exc.errno in (errno.ECONNRESET, errno.EPIPE, errno.ECONNABORTED)

    async def _handshake(self) -> None:
        log.debug(f"[SESSION] {self.session_id} 等待握手...")
        message = await read_message(self.reader)
        if message.get("type") != "handshake":
            raise ProtocolError("首包必须为 handshake")
        token = str(message.get("token") or "").encode("utf-8")
        expected_token = str(self.app.config.token or "").encode("utf-8")
        if not hmac.compare_digest(token, expected_token):
            await self._send_error(message.get("id"), "AUTH_FAILED", "token 不匹配")
            raise ProtocolError("token 不匹配")
        client_features = message.get("features") or []
        self.features["client"] = client_features
        self.account_key = message.get("account_key")
        self.sub_account_id = message.get("sub_account_id")
        ack = {
            "type": "handshake_ack",
            "session_id": self.session_id,
            "keepalive": 20,
            "protocol": PROTOCOL_VERSION,
            "features": self.app.active_features(),
        }
        try:
            self.app.register_session(self)
        except Exception as exc:
            await self._send_error(message.get("id"), "SERVER_BUSY", str(exc))
            raise
        await write_message(self.writer, ack)
        self._active = True
        log.info(f"[SESSION] {self.session_id} 握手成功, peer={self.peername}, account={self.account_key or '-'}")

    async def _loop(self) -> None:
        while self._active:
            message = await read_message(self.reader)
            msg_type = message.get("type", "request")
            if msg_type == "ping":
                await self._send_pong(message)
                continue
            if msg_type != "request":
                await self._send_error(message.get("id"), "UNSUPPORTED", f"不支持的消息类型 {msg_type}")
                continue
            request_id = message.get("id")
            action = message.get("action")
            payload = message.get("payload") or {}
            self._current_request = action
            self._current_broker_called = (
                False
                if str(action or "") in {"broker.place_order", "broker.cancel_order"}
                else None
            )
            start = time.time()
            try:
                request_timeout = self._request_timeout_for(action, payload)
                # 使用 asyncio.wait_for 添加超时控制
                result = await asyncio.wait_for(
                    self.app.handle_request(self, action, payload),
                    timeout=request_timeout,
                )
            except asyncio.TimeoutError:
                elapsed = time.time() - start
                error_msg = f"请求超时（>{request_timeout}s）"
                log.warning(f"[SESSION] {self.session_id} 请求 {action} 超时, 耗时={elapsed:.1f}s")
                self.app.log_access(self, action, payload, "timeout", elapsed, error_msg, request_id=request_id)
                await self._send_error(
                    request_id,
                    "REQUEST_TIMEOUT",
                    error_msg,
                    # 同步券商调用可能已进入 executor 队列且无法由 asyncio
                    # 取消；超时发生时，尚未观察到 marker 不能证明未来不会
                    # 进入 native writer。只有已经观察到 True 才回传事实，
                    # 否则保持未知并交给订单对账，禁止误释放后产生双单。
                    broker_called=(
                        True if self._current_broker_called is True else None
                    ),
                )
            except Exception as exc:
                elapsed = time.time() - start
                self.app.log_access(self, action, payload, "error", elapsed, str(exc), request_id=request_id)
                broker_called = getattr(exc, "broker_called", None)
                if broker_called is None:
                    broker_called = self._current_broker_called
                await self._send_error(
                    request_id,
                    getattr(exc, "code", "REQUEST_FAILED"),
                    str(exc),
                    broker_called=broker_called,
                )
            else:
                elapsed = time.time() - start
                self.app.log_access(self, action, payload, "ok", elapsed, request_id=request_id)
                await self._send_response(request_id, result)
            finally:
                self._current_request = None
                self._current_broker_called = None

    async def _send_response(self, request_id: Optional[str], payload: Any) -> None:
        if request_id is None:
            return
        await self._safe_send({"type": "response", "id": request_id, "payload": payload})

    async def _send_error(
        self,
        request_id: Optional[str],
        code: str,
        message: str,
        *,
        broker_called: Optional[bool] = None,
    ) -> None:
        """发送结构化错误，并携带本次写请求是否进入券商调用的事实。

        Args:
            request_id: 客户端请求 ID；握手错误时可以为空。
            code: 稳定错误码。
            message: 面向调用方的错误说明。
            broker_called: 写请求是否已经进入 native 券商写接口；非交易请求为空。

        Returns:
            None: 错误帧写入当前 TCP 会话。

        Side Effects:
            向客户端发送一条协议错误消息。
        """

        body = {"type": "error", "code": code, "message": message}
        if broker_called is not None:
            body["broker_called"] = bool(broker_called)
        if request_id:
            body["id"] = request_id
        await self._safe_send(body)

    def mark_broker_called(self) -> None:
        """记录当前交易写请求已经进入 native 券商写接口。

        Returns:
            None: 仅更新当前顺序处理请求的会话内事实。

        Side Effects:
            后续错误响应会携带 ``broker_called=true``。
        """

        self._current_broker_called = True

    async def _send_pong(self, ping: Dict[str, Any]) -> None:
        self._last_ping = time.time()
        await self._safe_send({"type": "pong", "id": ping.get("id")})

    def _request_timeout_for(self, action: Optional[str], payload: Dict[str, Any]) -> float:
        """计算单次请求的 session 层超时。

        Args:
            action: 客户端请求 action，例如 `broker.place_order`。
            payload: 客户端请求载荷。

        Returns:
            float: session 外层 `asyncio.wait_for` 使用的超时秒数。

        业务原因:
            `data.history` 在大 QMT 中会先下载再读取，默认需要比普通查询更长
            的等待窗口。`broker.place_order` 可能显式传入较长 `wait_timeout`
            等待订单终态；session 外层超时必须不小于该业务等待窗口加安全
            余量，否则会在 adapter 返回前先产生 REQUEST_TIMEOUT。
        """

        timeout = self._positive_env_float(self.REQUEST_TIMEOUT_ENV, self.REQUEST_TIMEOUT)
        if str(action or "") == "data.history":
            return max(timeout, self.HISTORY_REQUEST_TIMEOUT)
        if str(action or "") != "broker.place_order":
            return timeout
        try:
            wait_timeout = float(payload.get("wait_timeout") or 0.0)
        except (TypeError, ValueError):
            wait_timeout = 0.0
        if wait_timeout <= 0:
            return timeout
        margin = self._positive_env_float(
            self.PLACE_ORDER_TIMEOUT_MARGIN_ENV,
            self.PLACE_ORDER_TIMEOUT_MARGIN,
        )
        return max(timeout, wait_timeout + margin)

    @staticmethod
    def _positive_env_float(names: tuple[str, ...], default: float) -> float:
        for name in names:
            raw = os.getenv(name)
            if raw in (None, ""):
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if value > 0:
                return value
        return float(default)

    async def send_event(self, event: str, payload: Dict[str, Any]) -> None:
        if not self._active:
            return
        await self._safe_send({"type": "event", "event": event, "payload": payload})

    async def _safe_send(self, message: Dict[str, Any]) -> None:
        if self.writer.is_closing():
            return
        async with self._send_lock:
            await write_message(self.writer, message)

    async def close(self) -> None:
        if not self._active:
            if not self.writer.is_closing():
                self.writer.close()
                try:
                    await self.writer.wait_closed()
                except Exception:
                    pass
            return
        current_req = self._current_request
        if current_req:
            log.warning(f"[SESSION] {self.session_id} 关闭时仍有请求在处理: {current_req}")
        self._active = False
        await self.app.unregister_session(self)
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass
        log.debug(f"[SESSION] {self.session_id} 已关闭")


# 避免循环导入
class ServerApplication:  # pragma: no cover
    config: Any

    def register_session(self, session: ClientSession) -> None: ...

    async def unregister_session(self, session: ClientSession) -> None: ...

    async def handle_request(self, session: ClientSession, action: str, payload: Dict[str, Any]) -> Dict[str, Any]: ...
