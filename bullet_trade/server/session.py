from __future__ import annotations

import asyncio
import secrets
import time
from typing import Any, Dict, Optional

from bullet_trade.core.globals import log

from .protocol import ProtocolError, read_message, write_message


class ClientSession:
    """
    维护单个 TCP 连接的状态：握手、请求处理、事件推送等。
    """

    # 请求超时时间（秒），超过此时间未完成的请求会被取消
    REQUEST_TIMEOUT = 60.0

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

    async def run(self) -> None:
        try:
            await self._handshake()
            await self._loop()
        except asyncio.IncompleteReadError:
            pass
        except ProtocolError as exc:
            log.warning(f"session {self.session_id} 协议错误: {exc}")
        except Exception as exc:
            log.error(f"session {self.session_id} 异常: {exc}")
        finally:
            await self.close()

    async def _handshake(self) -> None:
        log.debug(f"[SESSION] {self.session_id} 等待握手...")
        message = await read_message(self.reader)
        if message.get("type") != "handshake":
            raise ProtocolError("首包必须为 handshake")
        token = message.get("token")
        if token != self.app.config.token:
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
            start = time.time()
            try:
                # 使用 asyncio.wait_for 添加超时控制
                result = await asyncio.wait_for(
                    self.app.handle_request(self, action, payload),
                    timeout=self.REQUEST_TIMEOUT,
                )
            except asyncio.TimeoutError:
                elapsed = time.time() - start
                error_msg = f"请求超时（>{self.REQUEST_TIMEOUT}s）"
                log.warning(f"[SESSION] {self.session_id} 请求 {action} 超时, 耗时={elapsed:.1f}s")
                self.app.log_access(self, action, payload, "timeout", elapsed, error_msg, request_id=request_id)
                await self._send_error(request_id, "REQUEST_TIMEOUT", error_msg)
            except Exception as exc:
                elapsed = time.time() - start
                self.app.log_access(self, action, payload, "error", elapsed, str(exc), request_id=request_id)
                await self._send_error(request_id, "REQUEST_FAILED", str(exc))
            else:
                elapsed = time.time() - start
                self.app.log_access(self, action, payload, "ok", elapsed, request_id=request_id)
                await self._send_response(request_id, result)
            finally:
                self._current_request = None

    async def _send_response(self, request_id: Optional[str], payload: Any) -> None:
        if request_id is None:
            return
        await self._safe_send({"type": "response", "id": request_id, "payload": payload})

    async def _send_error(self, request_id: Optional[str], code: str, message: str) -> None:
        body = {"type": "error", "code": code, "message": message}
        if request_id:
            body["id"] = request_id
        await self._safe_send(body)

    async def _send_pong(self, ping: Dict[str, Any]) -> None:
        self._last_ping = time.time()
        await self._safe_send({"type": "pong", "id": ping.get("id")})

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
