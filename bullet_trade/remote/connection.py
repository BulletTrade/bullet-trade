"""
作者: BruceLee
文件说明:
    通用远程券商协议的客户端长连接。
    主要输入为远程 action、请求载荷和连接配置，主要输出为 RPC 响应与事件回调。
    该层在 Broker/Provider 与 bullet-trade server 之间管理握手、心跳、重连和请求语义。
    交易写请求在任何可能已写出的传输错误后均不得自动重发。
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import ssl
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

from bullet_trade.core.globals import log
from bullet_trade.server.protocol import ProtocolError, encode_message, read_message

_REQUEST_TIMEOUT_DEFAULT = object()
_AMBIGUOUS_WRITE_ACTIONS = frozenset({"broker.place_order", "broker.cancel_order"})
_IDEMPOTENT_TRANSITION_ACTIONS = frozenset(
    {"data.subscribe", "data.unsubscribe", "data.unsubscribe_all"}
)
_AMBIGUOUS_SERVER_ERROR_CODES = frozenset({"REQUEST_FAILED", "REQUEST_TIMEOUT"})


class RemoteServerError(RuntimeError):
    """表示远程服务器已明确返回的错误。"""

    def __init__(self, code: str, message: str) -> None:
        """初始化服务器错误。

        Args:
            code: 服务器错误码。
            message: 服务器错误文本。

        Returns:
            None。
        """

        super().__init__(message)
        self.code = str(code or "REQUEST_FAILED")


class RemoteSubmissionUnknownError(RuntimeError, TimeoutError):
    """表示写请求可能已送达，但响应无法确认。"""

    code = "SUBMIT_UNKNOWN"

    def __init__(
        self,
        action: str,
        idempotency_key: str,
        request_payload: Dict[str, Any],
        *,
        message: Optional[str] = None,
        resolution: Optional[Dict[str, Any]] = None,
    ) -> None:
        """初始化提交未知异常。

        Args:
            action: 原始写 action。
            idempotency_key: 原始幂等键。
            request_payload: 原始请求载荷快照。
            message: 可选错误说明。
            resolution: 可选的只读解析结果。

        Returns:
            None。

        Side Effects:
            无；调用方必须先查询对账，不得重发写 action。
        """

        self.action = str(action)
        self.idempotency_key = str(idempotency_key)
        self.request_payload = dict(request_payload)
        self.resolution = dict(resolution or {})
        detail = message or "transport error after write may have been dispatched"
        super().__init__(
            f"远程写请求状态=submit_unknown: action={self.action} "
            f"idempotency_key={self.idempotency_key} detail={detail}"
        )


def classify_remote_action(action: str) -> str:
    """按具体远程 action 返回副作用分类。

    Args:
        action: 远程 RPC action 名称。

    Returns:
        str: `ambiguous_write`、`idempotent_transition` 或 `none`。
    """

    normalized = str(action or "").strip()
    if normalized in _AMBIGUOUS_WRITE_ACTIONS:
        return "ambiguous_write"
    if normalized in _IDEMPOTENT_TRANSITION_ACTIONS:
        return "idempotent_transition"
    return "none"


class RemoteQmtConnection:
    """
    负责管理到 bullet-trade server 的长连接（握手、心跳、请求/响应以及事件推送）。

    该类在后台线程中运行 asyncio 事件循环，对外暴露同步 request/subscribe API。
    """

    def __init__(
        self,
        host: str,
        port: int,
        token: str,
        *,
        tls_cert: Optional[str] = None,
        tls_enabled: bool = False,
        request_timeout: float = 60.0,
    ) -> None:
        """创建尚未联网的远程连接。

        Args:
            host: 服务端主机或 IP。
            port: 服务端 TCP 端口。
            token: 握手固定 token。
            tls_cert: 用于验证服务端的 CA/证书路径。
            tls_enabled: 是否强制使用 TLS。
            request_timeout: 默认 RPC 等待秒数。

        Returns:
            None。

        Raises:
            ValueError: 请求 TLS 却未提供验证证书时抛出，禁止静默降级明文。

        Side Effects:
            仅保存配置并创建本地同步原语；不发起网络连接。
        """

        if tls_enabled and not str(tls_cert or "").strip():
            raise ValueError("启用远程 TLS 必须提供 tls_cert，禁止回退明文连接")
        self.host = host
        self.port = port
        self.token = token
        self.tls_cert = tls_cert
        self.tls_enabled = bool(tls_enabled)
        self.request_timeout = max(5.0, float(request_timeout))
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._connected = threading.Event()
        self._stop = threading.Event()
        self._pending: Dict[str, asyncio.Future] = {}
        self._event_handlers: Dict[str, List[Callable[[Dict[str, Any]], None]]] = {}
        self._subscriptions: Dict[str, set] = {}
        self._session_id: Optional[str] = None
        self._keepalive: float = 20.0

    def start(self) -> None:
        if self._thread:
            return
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        if not self._connected.wait(timeout=10):
            raise RuntimeError("连接 qmt server 超时")

    def close(self) -> None:
        self._stop.set()
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=5)
        self._thread = None

    def add_event_listener(self, event: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        self._event_handlers.setdefault(event, []).append(handler)

    def request(
        self,
        action: str,
        payload: Optional[Dict[str, Any]] = None,
        timeout: Any = _REQUEST_TIMEOUT_DEFAULT,
    ) -> Dict:
        """同步发送远程请求并等待响应。

        Args:
            action: 远程 action 名称，例如 `broker.place_order`。
            payload: 请求 payload；为空时使用空字典。
            timeout: 本次请求超时秒数。省略参数时使用连接默认 `request_timeout`；
                显式传入 `None` 时保留旧版无限等待语义。

        Returns:
            Dict: 远程服务返回的响应字典。

        Raises:
            RuntimeError: 连接尚未启动时抛出。
            TimeoutError: 请求超过有效 timeout 时抛出，并取消后台 pending future。
        """

        if not self._loop:
            raise RuntimeError("remote connection 尚未启动")
        prepared_payload = self._prepare_request_payload(action, payload or {})
        coro = self._request_async(action, prepared_payload)
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        effective_timeout = self.request_timeout if timeout is _REQUEST_TIMEOUT_DEFAULT else timeout
        try:
            return future.result(timeout=effective_timeout)
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            if classify_remote_action(action) == "ambiguous_write":
                raise RemoteSubmissionUnknownError(
                    action,
                    prepared_payload["idempotency_key"],
                    prepared_payload,
                    message="client response timeout",
                ) from exc
            raise TimeoutError(f"request timed out: action={action}") from exc

    def resolve_submission(
        self,
        idempotency_key: str,
        *,
        write_action: str,
        request_payload: Optional[Dict[str, Any]] = None,
        order_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        timeout: Any = _REQUEST_TIMEOUT_DEFAULT,
    ) -> Dict[str, Any]:
        """使用原幂等键发起只读的写结果解析。

        Args:
            idempotency_key: 原始写请求的幂等键。
            write_action: 原始写 action。
            request_payload: 原始写请求载荷，用于服务端冲突校验。
            order_id: 已知的精确订单号。
            context: 账户和虚拟子账户等路由上下文。
            timeout: 只读解析请求的超时秒数。

        Returns:
            Dict[str, Any]: 服务端返回的 accepted/rejected/unknown 解析结果。

        Raises:
            ValueError: 原 action 不是支持的模糊写请求时抛出。
        """

        if classify_remote_action(write_action) != "ambiguous_write":
            raise ValueError(f"不支持解析非写 action: {write_action}")
        payload = dict(context or {})
        payload.update(
            {
                "idempotency_key": str(idempotency_key or "").strip(),
                "write_action": str(write_action),
                "request_payload": dict(request_payload or {}),
            }
        )
        if order_id:
            payload["order_id"] = str(order_id)
        return self.request("broker.resolve_submission", payload, timeout=timeout)

    def subscribe(self, key: str, symbols: List[str]) -> Dict:
        current = self._subscriptions.setdefault(key, set())
        new = {s.strip().upper() for s in symbols if s} - current
        if not new:
            return {"count": len(current)}
        payload = {"securities": list(new)}
        resp = self.request("data.subscribe", payload)
        current.update(new)
        return resp

    def unsubscribe(self, key: str, symbols: Optional[List[str]] = None) -> Dict:
        current = self._subscriptions.get(key)
        if not current:
            return {"count": 0}
        if not symbols:
            to_remove = list(current)
            current.clear()
            self._subscriptions.pop(key, None)
        else:
            to_remove = [s.strip().upper() for s in symbols if s]
            for s in to_remove:
                current.discard(s)
            if not current:
                self._subscriptions.pop(key, None)
        if not to_remove:
            return {"count": len(current)}
        payload = {"securities": to_remove}
        return self.request("data.unsubscribe", payload)

    def _run_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.create_task(self._connect_loop())
        try:
            self._loop.run_forever()
        finally:
            tasks = [t for t in asyncio.all_tasks(self._loop) if not t.done()]
            for task in tasks:
                task.cancel()
            try:
                self._loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            except Exception:
                pass
            self._loop.close()

    async def _connect_loop(self) -> None:
        backoff = 1
        while not self._stop.is_set():
            try:
                await self._connect_once()
                backoff = 1
                await self._reader_task
            except Exception as exc:
                log.warning(f"remote client 连接失败: {exc}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _connect_once(self) -> None:
        ssl_context = None
        if self.tls_enabled:
            ssl_context = ssl.create_default_context(cafile=self.tls_cert)
        reader, writer = await asyncio.open_connection(self.host, self.port, ssl=ssl_context)
        self._reader = reader
        self._writer = writer
        await self._send(
            {
                "type": "handshake",
                "token": self.token,
                "protocol": 1,
                "features": ["tick", "order_stream"],
            }
        )
        ack = await read_message(reader)
        if ack.get("type") != "handshake_ack":
            raise ProtocolError("握手失败")
        self._session_id = ack.get("session_id")
        self._keepalive = ack.get("keepalive", 20)
        self._connected.set()
        log.info(f"已连接远程 server，session={self._session_id}")
        self._reader_task = asyncio.create_task(self._reader_loop(), name="remote-reader")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="remote-heartbeat")
        await self._resubscribe_all()

    async def _reader_loop(self) -> None:
        assert self._reader
        try:
            while not self._stop.is_set():
                msg = await read_message(self._reader)
                msg_type = msg.get("type")
                if msg_type == "response":
                    req_id = msg.get("id")
                    if req_id in self._pending:
                        self._pending.pop(req_id).set_result(msg.get("payload"))
                elif msg_type == "error":
                    req_id = msg.get("id")
                    err = RemoteServerError(
                        str(msg.get("code") or "REQUEST_FAILED"),
                        str(msg.get("message") or "server error"),
                    )
                    if req_id and req_id in self._pending:
                        self._pending.pop(req_id).set_exception(err)
                    else:
                        log.error(f"server error: {msg}")
                elif msg_type == "event":
                    await self._dispatch_event(msg.get("event"), msg.get("payload"))
                elif msg_type == "pong":
                    continue
        except asyncio.IncompleteReadError:
            log.warning("远程 server 关闭连接")
        except Exception as exc:
            log.error(f"read loop error: {exc}")
        finally:
            self._connected.clear()
            await self._cleanup_transport()

    async def _heartbeat_loop(self) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(max(self._keepalive / 2, 5))
                await self._send({"type": "ping", "id": str(uuid.uuid4())})
        except Exception:
            pass

    async def _cleanup_transport(self) -> None:
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        current_task = asyncio.current_task()
        if self._reader_task and self._reader_task is not current_task:
            self._reader_task.cancel()
        self._reader_task = None
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(RuntimeError("连接已断开"))
        self._pending.clear()
        self._reader = None
        self._writer = None

    async def _wait_until_connected(self, timeout: Optional[float] = None) -> None:
        start = time.monotonic()
        while not self._connected.is_set():
            if self._stop.is_set():
                raise RuntimeError("连接已停止")
            if timeout is not None and time.monotonic() - start > timeout:
                raise RuntimeError("等待远程连接就绪超时")
            await asyncio.sleep(0.2)

    async def _request_async(self, action: str, payload: Dict) -> Dict:
        """在后台事件循环执行一次 RPC 及其受控重试。

        Args:
            action: 远程 RPC action。
            payload: 请求载荷。

        Returns:
            Dict: 远程响应。

        Raises:
            RemoteSubmissionUnknownError: 模糊写可能已送达且传输失败时抛出。
            Exception: 服务端明确错误或只读请求重试仍失败时原样抛出。
        """

        prepared_payload = self._prepare_request_payload(action, payload)
        side_effect = classify_remote_action(action)
        last_error: Optional[Exception] = None
        retry_count = 0
        while not self._stop.is_set():
            await self._wait_until_connected()
            req_id = str(uuid.uuid4())
            loop = asyncio.get_running_loop()
            future: asyncio.Future = loop.create_future()
            self._pending[req_id] = future
            try:
                await self._send(
                    {"type": "request", "id": req_id, "action": action, "payload": prepared_payload}
                )
                return await future
            except asyncio.CancelledError:
                self._pending.pop(req_id, None)
                raise
            except RemoteServerError as exc:
                self._pending.pop(req_id, None)
                if side_effect == "ambiguous_write" and exc.code in _AMBIGUOUS_SERVER_ERROR_CODES:
                    raise RemoteSubmissionUnknownError(
                        action,
                        prepared_payload["idempotency_key"],
                        prepared_payload,
                        message=f"server error code={exc.code}: {exc}",
                    ) from exc
                raise
            except Exception as exc:
                self._pending.pop(req_id, None)
                if side_effect == "ambiguous_write":
                    raise RemoteSubmissionUnknownError(
                        action,
                        prepared_payload["idempotency_key"],
                        prepared_payload,
                        message=str(exc),
                    ) from exc
                if not self._should_retry(exc):
                    raise
                last_error = exc
                retry_count += 1
                if retry_count > 1:
                    raise last_error
                await asyncio.sleep(0.2)
        raise last_error or RuntimeError("连接已停止")

    def _prepare_request_payload(self, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """复制请求载荷，并为模糊写分配稳定幂等键。

        Args:
            action: 远程 RPC action。
            payload: 调用方提供的原始载荷。

        Returns:
            Dict[str, Any]: 不修改调用方对象的载荷副本。
        """

        prepared = dict(payload or {})
        if classify_remote_action(action) != "ambiguous_write":
            return prepared
        key = str(prepared.get("idempotency_key") or prepared.get("request_id") or "").strip()
        if not key:
            key = f"bt-{action.rsplit('.', 1)[-1]}-{uuid.uuid4().hex}"
        prepared["idempotency_key"] = key
        return prepared

    async def _send(self, message: Dict[str, Any]) -> None:
        if not self._writer:
            raise RuntimeError("writer 未初始化")
        frame = encode_message(message)
        self._writer.write(frame)
        await self._writer.drain()

    async def _dispatch_event(self, event: Optional[str], payload: Dict[str, Any]) -> None:
        if not event:
            return
        handlers = self._event_handlers.get(event, [])
        for handler in handlers:
            try:
                handler(payload)
            except Exception as exc:
                log.error(f"事件处理异常 {event}: {exc}")

    async def _resubscribe_all(self) -> None:
        if not self._subscriptions:
            return
        for symbols in self._subscriptions.values():
            if not symbols:
                continue
            payload = {"securities": list(symbols)}
            try:
                await self._request_async("data.subscribe", payload)
            except Exception as exc:
                log.error(f"重建订阅失败: {exc}")

    def _should_retry(self, exc: Exception) -> bool:
        if self._stop.is_set():
            return False
        transient_markers = ("连接已断开", "writer 未初始化", "连接尚未就绪")
        if isinstance(exc, RuntimeError):
            message = str(exc)
            if any(marker in message for marker in transient_markers):
                return True
        return isinstance(exc, (ConnectionError, OSError))
