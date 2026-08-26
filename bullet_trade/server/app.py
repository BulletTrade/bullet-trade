"""
作者: BruceLee
日期: 2026-03-20
文件说明:
    bullet-trade 服务端核心调度入口。
    本文件负责会话管理、broker/data action 分发、下单幂等、服务端风控、
    tick 订阅转发等能力。
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import re
import ssl
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from bullet_trade.core.globals import log
from bullet_trade.core.risk_control import RiskController
from bullet_trade.utils.portfolio_printer import render_account_overview

from .adapters.base import (
    AccountContext,
    AccountRouter,
    AdapterBundle,
    SubAccountConfig,
    VirtualAccountManager,
)
from .config import ServerConfig
from .idempotency_journal import (
    IdempotencyJournalConflictError,
    IdempotencyJournalError,
    PersistentIdempotencyJournal,
)
from .session import ClientSession
from .tick import TickSubscriptionManager


@dataclass
class _IdempotencyEntry:
    """记录一个已占用的写请求幂等键。"""

    action: str
    fingerprint: str
    result: Dict[str, Any]
    expires_at: float
    finalized: bool = False


class IdempotencyConflictError(ValueError):
    """表示同一幂等键被用于不同的规范化写请求。"""

    code = "IDEMPOTENCY_CONFLICT"


_IDEMPOTENCY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/+\-]{0,254}$")


def _build_tls_context(config: ServerConfig) -> Optional[ssl.SSLContext]:
    """按服务配置创建仅用于服务端监听的 TLS 上下文。

    Args:
        config: 包含证书、私钥和启用状态的服务配置。

    Returns:
        Optional[ssl.SSLContext]: 未启用 TLS 时返回 None；启用时返回已加载证书链的上下文。

    Raises:
        ValueError: TLS 已启用但证书或私钥路径缺失时抛出。
        OSError: 证书链不可读或格式无效时由标准库抛出。

    Side Effects:
        从本地磁盘读取证书链；不创建监听、不发起网络连接。
    """

    tls = config.tls
    if not tls.enabled:
        return None
    if not tls.cert_path or not tls.key_path:
        raise ValueError("TLS 配置不完整：证书和私钥必须同时提供")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    if hasattr(ssl, "TLSVersion"):
        context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(certfile=tls.cert_path, keyfile=tls.key_path)
    return context


class ServerApplication:
    """bullet-trade 远程服务应用。

    说明:
        1. 对外暴露统一的 broker/data TCP 协议。
        2. 支持多账户路由、子账户限额、下单幂等。
        3. 可选启用服务端风控，拦截异常下单和频繁撤单。
    """

    def __init__(self, config: ServerConfig, router: AccountRouter, adapters: AdapterBundle):
        self.config = config
        self.router = router
        self.adapters = adapters
        self.virtual_accounts = VirtualAccountManager(config.sub_accounts)
        self.tick_manager: Optional[TickSubscriptionManager] = None
        if adapters.data_adapter:
            self.tick_manager = TickSubscriptionManager(
                adapters.data_adapter,
                interval=1.0,
                max_subscriptions=config.max_subscriptions,
            )
        self._server: Optional[asyncio.AbstractServer] = None
        self._sessions: Set[ClientSession] = set()
        self._created_at = time.time()
        self._ip_allowlist = self._prepare_allowlist(config.allowlist)
        self._shutdown: Optional[asyncio.Event] = None
        self._started: Optional[asyncio.Event] = None
        self._idempotency_cache: Dict[Tuple[str, str, str], _IdempotencyEntry] = {}
        self._idempotency_lock: Optional[asyncio.Lock] = None
        self._idempotency_journal: Optional[PersistentIdempotencyJournal] = None
        self._idempotency_journal_error: Optional[str] = None
        self._idempotency_journal_required = bool(
            adapters.broker_adapter
            and adapters.broker_writes_require_persistent_idempotency is not False
        )
        self._idempotency_key_required = bool(
            adapters.broker_adapter and adapters.broker_writes_require_idempotency_key is not False
        )
        if self._idempotency_journal_required and self.config.idempotency_journal_path:
            try:
                self._idempotency_journal = PersistentIdempotencyJournal(
                    self.config.idempotency_journal_path,
                    self.config.idempotency_journal_max_entries,
                )
            except IdempotencyJournalError as exc:
                self._idempotency_journal_error = str(exc)
                log.error("幂等持久日志不可用，交易写将 fail-closed: %s", exc)
        elif self._idempotency_journal_required:
            self._idempotency_journal_error = "生产交易 server 未配置 QMT_SERVER_IDEMPOTENCY_JOURNAL_PATH"
        self._risk_by_account: Dict[str, RiskController] = {}
        self._risk_locks: Dict[str, asyncio.Lock] = {}
        if self.config.order_risk_enabled:
            for ctx in self.router.list_accounts():
                account_key = ctx.config.key or "default"
                self._risk_by_account[account_key] = RiskController()
                self._risk_locks[account_key] = asyncio.Lock()

    @property
    def _idempotency_lock_guard(self) -> asyncio.Lock:
        if self._idempotency_lock is None:
            self._idempotency_lock = asyncio.Lock()
        return self._idempotency_lock

    async def start(self) -> None:
        self._ensure_runtime_events()
        try:
            await self._start_components()
            ssl_context = _build_tls_context(self.config)
            self._server = await asyncio.start_server(
                self._handle_client,
                self.config.listen,
                self.config.port,
                ssl=ssl_context,
            )
        except Exception:
            await self.shutdown()
            raise
        host = (
            self._server.sockets[0].getsockname()
            if self._server.sockets
            else (self.config.listen, self.config.port)
        )
        log.info(f"QMT server listening on {host}")
        assert self._started is not None
        self._started.set()
        try:
            await self._server.serve_forever()
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        """幂等关闭监听、会话、tick manager、数据 adapter 与 broker adapter。

        Returns:
            None。

        Side Effects:
            只停止本应用持有的组件；华鑫 XMD 与 Trader 分别按自己的生命周期关闭。
        """

        self._ensure_runtime_events()
        assert self._shutdown is not None
        if self._shutdown.is_set():
            return
        self._shutdown.set()
        if self.tick_manager:
            await self.tick_manager.stop()
        for session in list(self._sessions):
            await session.close()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        data_adapter = self.adapters.data_adapter
        broker_adapter = self.adapters.broker_adapter
        if data_adapter and data_adapter is not broker_adapter:
            stop_fn = getattr(data_adapter, "stop", None)
            if callable(stop_fn):
                try:
                    await stop_fn()
                except Exception:
                    pass
        if broker_adapter:
            try:
                await broker_adapter.stop()
            except Exception:
                pass

    def active_features(self) -> List[str]:
        """返回当前配置启用的功能列表。

        Args:
            None。

        Returns:
            List[str]: 配置启用的功能名称。
        """

        features = []
        if self.adapters.data_adapter:
            features.append("data")
        if self.adapters.broker_adapter:
            features.append("broker")
        return features

    def _qmt_status_snapshot(self) -> Optional[Dict[str, Any]]:
        """读取 QMT adapter 暴露的 readiness 快照。

        Args:
            None。

        Returns:
            Optional[Dict[str, Any]]: QMT guard 快照；非 QMT server 返回 None。
        """

        for adapter in (self.adapters.broker_adapter, self.adapters.data_adapter):
            status_fn = getattr(adapter, "qmt_status", None)
            if callable(status_fn):
                return status_fn()
        return None

    def _backend_status_snapshot(self) -> Optional[Dict[str, Any]]:
        """读取当前 adapter 的通用后端 readiness 快照。

        Args:
            None。

        Returns:
            Optional[Dict[str, Any]]: 华鑫等通用后端优先返回 ``backend_status``；
            既有 QMT adapter 继续兼容 ``qmt_status``。
        """

        snapshots: List[Dict[str, Any]] = []
        seen: Set[int] = set()
        for adapter in (self.adapters.broker_adapter, self.adapters.data_adapter):
            if adapter is None or id(adapter) in seen:
                continue
            seen.add(id(adapter))
            status_fn = getattr(adapter, "backend_status", None)
            if callable(status_fn):
                value = status_fn()
                if isinstance(value, dict):
                    snapshots.append(value)
        if snapshots:
            if len(snapshots) > 1 and all(
                item.get("backend_type") == "huaxin" for item in snapshots
            ):
                return self._merge_huaxin_backend_statuses(snapshots)
            return snapshots[0]
        return self._qmt_status_snapshot()

    @staticmethod
    def _merge_huaxin_backend_statuses(
        snapshots: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """合并 Huaxin Trader/XMD health，同时保留逐模块 readiness。

        Args:
            snapshots: broker/data adapter 各自返回的华鑫 backend_status。

        Returns:
            Dict[str, Any]: action 合并、modules 分离且整体状态可判定的 health。

        Notes:
            ``modules.trader`` 与 ``modules.xmd_l1`` 各自保留 ready/state/reason；
            整体 ready 只表示所有已启用模块均 ready，不能反推任一具体写能力。
        """

        by_component = {
            str(item.get("component") or f"component_{index}"): dict(item)
            for index, item in enumerate(snapshots)
        }
        base = dict(by_component.get("trader") or snapshots[0])
        actions: Dict[str, Any] = {}
        modules: Dict[str, Any] = {}
        for component, item in by_component.items():
            actions.update(dict(item.get("actions") or {}))
            modules[component] = {
                "ready": bool(item.get("ready")),
                "state": str(item.get("state") or "unavailable"),
                "reason": item.get("reason"),
                "source": item.get("source"),
            }
            if component == "xmd_l1" and isinstance(item.get("xmd_l1"), dict):
                base["xmd_l1"] = dict(item["xmd_l1"])
        ready_values = [bool(item.get("ready")) for item in by_component.values()]
        overall_ready = bool(ready_values) and all(ready_values)
        any_ready = any(ready_values)
        base.update(
            {
                "backend_type": "huaxin",
                "component": "composite",
                "ready": overall_ready,
                "state": "ready" if overall_ready else ("degraded" if any_ready else "unavailable"),
                "reason": None if overall_ready else "one_or_more_modules_not_ready",
                "modules": modules,
                "actions": actions,
            }
        )
        return base

    async def wait_started(self) -> None:
        self._ensure_runtime_events()
        assert self._started is not None
        await self._started.wait()

    def _ensure_runtime_events(self) -> None:
        if self._shutdown is None:
            self._shutdown = asyncio.Event()
        if self._started is None:
            self._started = asyncio.Event()

    def register_session(self, session: ClientSession) -> None:
        if len(self._sessions) >= self.config.max_connections:
            raise RuntimeError("连接数达到上限")
        self._sessions.add(session)

    async def unregister_session(self, session: ClientSession) -> None:
        if session in self._sessions:
            self._sessions.remove(session)
        if self.tick_manager:
            await self.tick_manager.remove_session(session)

    def log_access(
        self,
        session: ClientSession,
        action: Optional[str],
        payload: Optional[Dict[str, Any]],
        status: str,
        duration: float,
        error: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> None:
        if not getattr(self.config, "access_log_enabled", True):
            return
        data = payload if isinstance(payload, dict) else {}
        account = data.get("account_key") or session.account_key or "-"
        sub_account = data.get("sub_account_id") or session.sub_account_id or "-"
        base = (
            f"[ACCESS] peer={session.peername} session={session.session_id} "
            f"id={request_id or '-'} action={action or '-'} account={account} sub={sub_account} "
            f"status={status} cost={duration * 1000:.1f}ms"
        )
        if error:
            log.warning(f"{base} error={error}")
        else:
            log.info(base)

    async def handle_request(
        self, session: ClientSession, action: Optional[str], payload: Dict
    ) -> Dict:
        if not action:
            raise ValueError("缺少 action 字段")
        if action == "data.subscribe":
            if not self.tick_manager:
                raise RuntimeError("数据服务未启用")
            symbols = payload.get("securities") or payload.get("symbols") or []
            return await self.tick_manager.subscribe(session, symbols)
        if action == "data.unsubscribe":
            if not self.tick_manager:
                return {"count": 0}
            return await self.tick_manager.unsubscribe(session, payload.get("securities"))
        if action == "data.unsubscribe_all":
            if not self.tick_manager:
                return {"count": 0}
            return await self.tick_manager.unsubscribe(session, None)
        if action == "admin.health":
            return self._health_snapshot()
        if action == "admin.print_account":
            return await self._admin_print_account(session, payload)
        if action.startswith("data."):
            return await self._dispatch_data(action.split(".", 1)[1], payload)
        if action.startswith("broker."):
            return await self._dispatch_broker(session, action.split(".", 1)[1], payload)
        raise ValueError(f"未知 action: {action}")

    async def _dispatch_data(self, method: str, payload: Dict) -> Dict:
        if not self.adapters.data_adapter:
            raise RuntimeError("数据服务未启用")
        if method == "current_tick":
            fn = getattr(self.adapters.data_adapter, "current_tick", None)
            if fn:
                return await fn(payload)
            snapshot_fn = getattr(self.adapters.data_adapter, "get_snapshot", None)
            if snapshot_fn:
                return await snapshot_fn(payload)
            tick_fn = getattr(self.adapters.data_adapter, "get_current_tick", None)
            if tick_fn:
                security = (
                    payload.get("security") or payload.get("stock") or payload.get("stockcode")
                )
                return await tick_fn(security)
            raise ValueError("数据接口 current_tick 未实现")
        fn = getattr(self.adapters.data_adapter, method, None)
        if fn is None:
            fn = getattr(self.adapters.data_adapter, f"get_{method}", None)
        if not fn:
            raise ValueError(f"数据接口 {method} 未实现")
        return await fn(payload)

    async def _dispatch_broker(self, session: ClientSession, method: str, payload: Dict) -> Dict:
        if not self.adapters.broker_adapter:
            raise RuntimeError("券商服务未启用")
        account_key = payload.get("account_key") or session.account_key
        sub_account_id = payload.get("sub_account_id") or session.sub_account_id
        resolved_key, sub_cfg = self.virtual_accounts.resolve(account_key, sub_account_id)
        if sub_account_id and "sub_account_id" not in payload:
            payload["sub_account_id"] = (
                sub_cfg.sub_account_id if sub_cfg else str(sub_account_id).split("@", 1)[0]
            )
        ctx = self.router.get(resolved_key)
        if method == "resolve_submission":
            return await self._resolve_submission(resolved_key, sub_cfg, ctx, payload)
        write_action = f"broker.{method}" if method in {"place_order", "cancel_order"} else None
        if write_action is not None and self._idempotency_key_required:
            _validate_required_idempotency_key(payload)
        write_fingerprint = (
            _build_write_fingerprint(write_action, payload) if write_action is not None else None
        )
        if method == "place_order":
            await self._maybe_reject_when_paused(payload)
            await self._maybe_fill_price(payload)
            await self.virtual_accounts.ensure_within_limit(sub_cfg, _estimate_order_value(payload))
        impl = method
        fn = getattr(self.adapters.broker_adapter, impl, None)
        if method == "cancel_order":
            request_fn = getattr(self.adapters.broker_adapter, "cancel_order_request", None)
            if callable(request_fn):
                impl = "cancel_order_request"
                fn = request_fn
        if fn is None:
            aliases = {
                "account": "get_account_info",
                "positions": "get_positions",
                "orders": "list_orders",
                "trades": "list_trades",
                "order_status": "get_order_status",
                "place_order": "place_order",
                "cancel_order": "cancel_order",
            }
            alias = aliases.get(method)
            if alias:
                impl = alias
                fn = getattr(self.adapters.broker_adapter, impl, None)
        if not fn:
            raise ValueError(f"券商接口 {method} 未实现")
        args = self._build_broker_args(impl, ctx, payload)
        if write_action is not None:
            cached_result = await self._claim_idempotent_write(
                resolved_key,
                sub_cfg,
                write_action,
                payload,
                fingerprint=write_fingerprint,
            )
            if cached_result is not None:
                if sub_cfg:
                    cached_result = _attach_sub_account_id(
                        cached_result,
                        sub_cfg.sub_account_id,
                    )
                return cached_result
        if method == "place_order" and resolved_key in self._risk_by_account:
            result = await self._place_order_with_server_risk(
                resolved_key=resolved_key,
                ctx=ctx,
                payload=payload,
                fn=fn,
                args=args,
            )
        elif method == "cancel_order" and resolved_key in self._risk_by_account:
            result = await self._cancel_order_with_server_risk(
                resolved_key=resolved_key,
                payload=payload,
                fn=fn,
                args=args,
            )
        else:
            result = await fn(*args)
        if method == "cancel_order":
            result = _enforce_cancel_result_semantics(payload, result)
        paused_msg = (payload.get("meta") or {}).get("paused_warning")
        if paused_msg:
            log.warning(paused_msg + "（已透传给客户端）")
            try:
                if isinstance(result, dict):
                    result.setdefault("warning", paused_msg)
            except Exception:
                pass
        if write_action is not None:
            await self._finalize_idempotent_write(
                resolved_key,
                sub_cfg,
                write_action,
                payload,
                result,
                fingerprint=write_fingerprint,
            )
        if sub_cfg:
            result = _attach_sub_account_id(result, sub_cfg.sub_account_id)
        return result

    async def _place_order_with_server_risk(
        self,
        *,
        resolved_key: str,
        ctx: AccountContext,
        payload: Dict,
        fn,
        args: Tuple,
    ) -> Dict:
        """在服务端风控保护下执行下单。

        Args:
            resolved_key: 解析后的真实父账户 key。
            ctx: 当前账户上下文。
            payload: 原始请求载荷。
            fn: 实际下单函数。
            args: 实际下单参数。

        Returns:
            Dict: 下单结果。
        """
        risk = self._risk_by_account.get(resolved_key)
        if risk is None:
            return await fn(*args)
        lock = self._risk_locks.setdefault(resolved_key, asyncio.Lock())
        async with lock:
            order_value = _estimate_order_value(payload)
            if order_value and order_value > 0:
                positions = await self.adapters.broker_adapter.get_positions(ctx)
                account_info = await self.adapters.broker_adapter.get_account_info(ctx)
                positions_count = _count_open_positions(positions)
                total_value = _extract_total_value(account_info)
                side = str(payload.get("side") or "BUY").upper()
                action = "buy" if side == "BUY" else "sell"
                risk.check_order(
                    order_value=order_value,
                    current_positions_count=positions_count,
                    security=str(payload.get("security") or ""),
                    total_value=total_value,
                    action=action,
                )
                result = await fn(*args)
                risk.record_trade(order_value=order_value, action=action)
                return result
            return await fn(*args)

    async def _cancel_order_with_server_risk(
        self,
        *,
        resolved_key: str,
        payload: Dict,
        fn,
        args: Tuple,
    ) -> Dict:
        """在服务端风控保护下执行撤单。

        Args:
            resolved_key: 解析后的真实父账户 key。
            payload: 原始请求载荷。
            fn: 实际撤单函数。
            args: 实际撤单参数。

        Returns:
            Dict: 撤单结果。
        """
        risk = self._risk_by_account.get(resolved_key)
        if risk is None:
            return await fn(*args)
        lock = self._risk_locks.setdefault(resolved_key, asyncio.Lock())
        order_id = str(payload.get("order_id") or "")
        async with lock:
            risk.check_cancel(order_id=order_id)
            result = await fn(*args)
            ok = False
            if isinstance(result, dict):
                value = result.get("value")
                if isinstance(value, bool):
                    ok = value
                elif value is None:
                    ok = bool(result.get("success", True))
                else:
                    ok = bool(value)
            else:
                ok = bool(result)
            if ok:
                risk.record_cancel(order_id=order_id)
            return result

    async def _claim_idempotent_write(
        self,
        resolved_key: str,
        sub_cfg: Optional[SubAccountConfig],
        action: str,
        payload: Dict[str, Any],
        *,
        fingerprint: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """在调用 adapter 前原子占用写请求的幂等键。

        Args:
            resolved_key: 解析后的真实父账户 key。
            sub_cfg: 可选虚拟子账户配置。
            action: 原始写 action。
            payload: 原始写请求载荷。
            fingerprint: 发送前已计算的规范化指纹。

        Returns:
            Optional[Dict[str, Any]]: 新 key 返回 None 表示可执行；
            旧 key 返回已有结果或 fail-closed 未知态。

        Raises:
            IdempotencyConflictError: 同 key 的 action 或载荷指纹不一致时抛出。

        Side Effects:
            新 key 会在进程内幂等缓存中占位，防止并发双写。
        """

        if self._idempotency_journal_required and self._idempotency_journal is None:
            raise RuntimeError("拒绝交易写：持久幂等日志不可用（%s）" % (self._idempotency_journal_error or "未配置"))
        key = str(payload.get("idempotency_key") or "").strip()
        if not key:
            return None
        cache_key = self._idempotency_cache_key(resolved_key, sub_cfg, key)
        normalized = fingerprint or _build_write_fingerprint(action, payload)
        now = time.monotonic()
        async with self._idempotency_lock_guard:
            pending_result = _unknown_submission_result(
                action,
                key,
                order_id=(
                    str(payload.get("order_id") or "") if action == "broker.cancel_order" else None
                ),
            )
            if self._idempotency_journal is not None:
                try:
                    journal_result = self._idempotency_journal.claim(
                        cache_key, action, normalized, pending_result
                    )
                except IdempotencyJournalConflictError as exc:
                    raise IdempotencyConflictError(f"idempotency_key 冲突: {key}") from exc
                except IdempotencyJournalError as exc:
                    raise RuntimeError(f"拒绝交易写：幂等持久日志失败: {exc}") from exc
                if journal_result is not None:
                    return journal_result
            self._purge_expired_idempotency_entries(now)
            entry = self._idempotency_cache.get(cache_key)
            if entry is not None:
                self._ensure_idempotency_match(entry, action, normalized, key)
                return dict(entry.result)
            self._idempotency_cache[cache_key] = _IdempotencyEntry(
                action=action,
                fingerprint=normalized,
                result=pending_result,
                expires_at=now + max(1, int(self.config.idempotency_ttl_seconds or 300)),
                finalized=False,
            )
        return None

    async def _finalize_idempotent_write(
        self,
        resolved_key: str,
        sub_cfg: Optional[SubAccountConfig],
        action: str,
        payload: Dict[str, Any],
        result: Any,
        *,
        fingerprint: Optional[str] = None,
    ) -> None:
        """用 adapter 的明确响应收口幂等占位。

        Args:
            resolved_key: 解析后的真实父账户 key。
            sub_cfg: 可选虚拟子账户配置。
            action: 原始写 action。
            payload: 原始写请求载荷。
            result: adapter 返回的明确结果。
            fingerprint: 发送前已计算的规范化指纹。

        Returns:
            None。

        Raises:
            IdempotencyConflictError: 幂等占位与当前请求不一致时抛出。

        Side Effects:
            将进程内幂等占位更新为最终响应。
        """

        key = str(payload.get("idempotency_key") or "").strip()
        if not key:
            return
        cache_key = self._idempotency_cache_key(resolved_key, sub_cfg, key)
        normalized = fingerprint or _build_write_fingerprint(action, payload)
        normalized_result = dict(result) if isinstance(result, dict) else {"value": result}
        normalized_result.setdefault("idempotency_key", key)
        finalized = _is_final_idempotency_result(normalized_result)
        now = time.monotonic()
        async with self._idempotency_lock_guard:
            if self._idempotency_journal is not None:
                try:
                    self._idempotency_journal.finalize(
                        cache_key,
                        action,
                        normalized,
                        normalized_result,
                        finalized=finalized,
                    )
                except IdempotencyJournalError as exc:
                    raise RuntimeError("交易写已返回但无法持久收口，保持 submit_unknown 并停止重试: %s" % exc) from exc
            self._purge_expired_idempotency_entries(now)
            entry = self._idempotency_cache.get(cache_key)
            if entry is not None:
                self._ensure_idempotency_match(entry, action, normalized, key)
            self._idempotency_cache[cache_key] = _IdempotencyEntry(
                action=action,
                fingerprint=normalized,
                result=normalized_result,
                expires_at=now + max(1, int(self.config.idempotency_ttl_seconds or 300)),
                finalized=finalized,
            )

    async def _resolve_submission(
        self,
        resolved_key: str,
        sub_cfg: Optional[SubAccountConfig],
        ctx: AccountContext,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """以幂等缓存、adapter 或精确订单事实解析模糊写。

        Args:
            resolved_key: 解析后的真实父账户 key。
            sub_cfg: 可选虚拟子账户配置。
            ctx: 父账户运行上下文。
            payload: 包含原 action、原载荷和幂等键的解析请求。

        Returns:
            Dict[str, Any]: accepted、rejected、submit_unknown 或 reconciling 结果。

        Raises:
            ValueError: 幂等键或原 action 缺失时抛出。
            IdempotencyConflictError: 原载荷与已占用指纹不一致时抛出。
        """

        key = str(payload.get("idempotency_key") or "").strip()
        if not key:
            raise ValueError("缺少 idempotency_key")
        action = str(payload.get("write_action") or payload.get("action") or "").strip()
        if action not in {"broker.place_order", "broker.cancel_order"}:
            raise ValueError(f"不支持的写 action: {action or '-'}")
        original_payload = payload.get("request_payload") or {}
        if not isinstance(original_payload, dict):
            raise ValueError("request_payload 必须是对象")
        requested_fingerprint = (
            _build_write_fingerprint(action, original_payload) if original_payload else None
        )
        cache_key = self._idempotency_cache_key(resolved_key, sub_cfg, key)
        pending_result: Optional[Dict[str, Any]] = None
        async with self._idempotency_lock_guard:
            if self._idempotency_journal is not None:
                try:
                    journal_entry = self._idempotency_journal.get(
                        cache_key, action, requested_fingerprint
                    )
                except IdempotencyJournalConflictError as exc:
                    raise IdempotencyConflictError(f"idempotency_key 冲突: {key}") from exc
                except IdempotencyJournalError as exc:
                    raise IdempotencyConflictError(f"幂等持久日志冲突: {key}") from exc
                if journal_entry is not None:
                    journal_result = dict(journal_entry["result"])
                    if journal_entry.get("finalized"):
                        return _format_resolved_submission(
                            action, key, journal_result, source="idempotency_journal"
                        )
                    pending_result = journal_result
            self._purge_expired_idempotency_entries(time.monotonic())
            entry = self._idempotency_cache.get(cache_key)
            if entry is not None:
                if requested_fingerprint is not None:
                    self._ensure_idempotency_match(
                        entry,
                        action,
                        requested_fingerprint,
                        key,
                    )
                elif entry.action != action:
                    raise IdempotencyConflictError(f"idempotency_key 冲突: {key}")
                if entry.finalized:
                    return _format_resolved_submission(
                        entry.action,
                        key,
                        entry.result,
                        source="idempotency_cache",
                    )
                pending_result = dict(entry.result)

        strong_keys = {key}
        order_id = str(payload.get("order_id") or original_payload.get("order_id") or "").strip()
        matching_orders = await self._query_submission_orders(ctx, order_id, strong_keys)
        matching_orders = [
            row
            for row in matching_orders
            if _order_matches_submission_contract(action, row, original_payload, key, order_id)
        ]
        if len(matching_orders) == 1:
            return _format_order_evidence(action, key, matching_orders[0])
        if len(matching_orders) > 1:
            return _unknown_submission_result(
                action,
                key,
                status="reconciling",
                reason="multiple_strong_order_matches",
                order_id=order_id or None,
            )
        if pending_result is not None:
            pending_result.setdefault("evidence", {"source": "idempotency_pending"})
            return pending_result
        return _unknown_submission_result(
            action,
            key,
            reason="no_strong_order_evidence",
            order_id=order_id or None,
        )

    async def _resolve_with_adapter(
        self,
        ctx: AccountContext,
        payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """调用最靠近真实写入的 adapter 解析能力。

        Args:
            ctx: 父账户运行上下文。
            payload: 解析请求载荷。

        Returns:
            Optional[Dict[str, Any]]: adapter 提供的结果；未实现或无结果时返回 None。

        Side Effects:
            只允许调用 adapter 的只读解析入口。
        """

        resolver = getattr(self.adapters.broker_adapter, "resolve_submission", None)
        if not callable(resolver):
            return None
        try:
            result = await resolver(ctx, payload)
        except Exception as exc:
            log.warning(f"adapter 解析写结果失败，保持 fail-closed: {exc}")
            return None
        return dict(result) if isinstance(result, dict) and result else None

    async def _query_submission_orders(
        self,
        ctx: AccountContext,
        order_id: str,
        strong_keys: Set[str],
    ) -> List[Dict[str, Any]]:
        """通过精确订单号或强关联键查询已有委托。

        Args:
            ctx: 父账户运行上下文。
            order_id: 可选精确订单号。
            strong_keys: 幂等键、客户订单号等精确匹配值。

        Returns:
            List[Dict[str, Any]]: 通过强身份证据匹配的订单快照。

        Side Effects:
            仅调用券商 adapter 的订单查询接口。
        """

        adapter = self.adapters.broker_adapter
        if adapter is None:
            return []
        if order_id:
            status_fn = getattr(adapter, "get_order_status", None)
            if callable(status_fn):
                try:
                    row = await status_fn(ctx, order_id)
                except Exception:
                    row = None
                if isinstance(row, dict) and row:
                    return [dict(row)]
        orders_fn = getattr(adapter, "list_orders", None)
        if not callable(orders_fn):
            return []
        try:
            rows = await orders_fn(ctx, {"idempotency_key": sorted(strong_keys)})
        except Exception:
            return []
        matched: List[Dict[str, Any]] = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            if _order_matches_strong_key(row, strong_keys):
                matched.append(dict(row))
        return matched

    @staticmethod
    def _ensure_idempotency_match(
        entry: _IdempotencyEntry,
        action: str,
        fingerprint: str,
        key: str,
    ) -> None:
        """校验已占用 key 与当前写请求完全一致。

        Args:
            entry: 已有幂等条目。
            action: 当前写 action。
            fingerprint: 当前规范化指纹。
            key: 当前幂等键，仅用于错误信息。

        Returns:
            None。

        Raises:
            IdempotencyConflictError: action 或指纹不一致时抛出。
        """

        if entry.action != action or entry.fingerprint != fingerprint:
            raise IdempotencyConflictError(f"idempotency_key 冲突: {key}")

    @staticmethod
    def _idempotency_cache_key(
        resolved_key: str,
        sub_cfg: Optional[SubAccountConfig],
        key: str,
    ) -> Tuple[str, str, str]:
        """构造隔离父账户和虚拟子账户的缓存键。

        Args:
            resolved_key: 真实父账户 key。
            sub_cfg: 可选虚拟子账户配置。
            key: 写请求幂等键。

        Returns:
            Tuple[str, str, str]: 进程内幂等缓存键。
        """

        return (resolved_key, sub_cfg.sub_account_id if sub_cfg else "", key)

    def _purge_expired_idempotency_entries(self, now: float) -> None:
        expired = [key for key, value in self._idempotency_cache.items() if value.expires_at <= now]
        for key in expired:
            self._idempotency_cache.pop(key, None)

    async def _maybe_fill_price(self, payload: Dict) -> None:
        """
        若下单缺少 price，尝试用数据服务补充最新成交价。

        市价单不能把该价格写回 protect_price；保护价应由 broker adapter 按买卖方向和默认偏移计算。
        """
        style_value = payload.get("style") or {}
        style = style_value if isinstance(style_value, dict) else {}
        explicit_price = (
            style.get("price")
            or style.get("protect_price")
            or style.get("limit_price")
            or payload.get("price")
        )
        strict_execution_price = any(
            bool(getattr(adapter, "requires_explicit_execution_price", False))
            for adapter in (self.adapters.broker_adapter, self.adapters.data_adapter)
            if adapter is not None
        )
        if strict_execution_price:
            if explicit_price is None:
                raise ValueError(
                    "HUAXIN_EXECUTION_PRICE_REQUIRED: 华鑫订单必须由上游基于新鲜 XMD " "快照显式提供限价或保护价"
                )
            return
        try:
            price = style.get("price")
            protect_price = style.get("protect_price")
            if price is not None or protect_price is not None:
                return
            security = payload.get("security")
            if not security or not self.adapters.data_adapter:
                return
            data_adapter = self.adapters.data_adapter
            snapshot = None
            snap_fn = getattr(data_adapter, "get_snapshot", None)
            if callable(snap_fn):
                snapshot = await snap_fn({"security": security})
            if not snapshot and hasattr(data_adapter, "get_current_tick"):
                try:
                    tick_fn = getattr(data_adapter, "get_current_tick")
                    snapshot = await tick_fn(security) if callable(tick_fn) else None
                except Exception:
                    snapshot = None
            price = None
            if isinstance(snapshot, dict):
                price = (
                    snapshot.get("last_price") or snapshot.get("lastPrice") or snapshot.get("price")
                )
            if price is None and callable(getattr(data_adapter, "get_history", None)):
                hist = await data_adapter.get_history(
                    {"security": security, "count": 1, "frequency": "1m"}
                )
                records = hist.get("records") if isinstance(hist, dict) else None
                if records:
                    last = records[-1]
                    if isinstance(last, (list, tuple)) and last:
                        price = last[-1] if isinstance(last[-1], (int, float)) else None
            if price is not None:
                if style.get("type", "").lower() == "market":
                    payload["_estimated_price"] = float(price)
                else:
                    style["price"] = float(price)
                    payload["style"] = style
                    payload.setdefault("price", float(price))
        except Exception:
            # 补价失败不终止下单，交由后续逻辑处理
            pass

    async def _maybe_reject_when_paused(self, payload: Dict) -> None:
        """
        下单前检查停牌，避免静默被券商拒绝；仅在数据服务可用时生效。
        """
        data_adapter = self.adapters.data_adapter
        if not data_adapter:
            return
        authoritative_realtime = bool(getattr(data_adapter, "authoritative_realtime_only", False))
        security = payload.get("security")
        if not security:
            return

        snapshot = None
        last_error: Optional[Exception] = None
        for fn_name in ("get_live_current", "get_snapshot", "get_current_tick"):
            fn = getattr(data_adapter, fn_name, None)
            if not callable(fn):
                continue
            try:
                if fn_name == "get_current_tick":
                    snapshot = await fn(security)  # type: ignore[misc,arg-type]
                else:
                    snapshot = await fn({"security": security})  # type: ignore[arg-type]
                break
            except Exception as exc:
                last_error = exc
                continue

        if not isinstance(snapshot, dict):
            if authoritative_realtime:
                raise ValueError("HUAXIN_XMD_SNAPSHOT_REQUIRED: 华鑫下单前无法取得新鲜权威行情") from last_error
            return
        if authoritative_realtime and str(snapshot.get("source") or "") != "huaxin_xmd_l1":
            raise ValueError("HUAXIN_XMD_SOURCE_INVALID: 华鑫下单前行情来源不是 huaxin_xmd_l1")

        paused_flag = snapshot.get("paused")
        if paused_flag is None:
            status = str(snapshot.get("status") or "").lower()
            paused_flag = status in {"paused", "halt", "停牌"}

        if paused_flag:
            msg = f"{security} 停牌，拒绝远程委托"
            log.warning(msg + "（仅警告，不阻塞委托）")
            payload.setdefault("meta", {})["paused_warning"] = msg

    def _build_broker_args(
        self, method: str, ctx: AccountContext, payload: Optional[Dict]
    ) -> Tuple:
        payload = payload or {}
        if method in ("get_account_info", "get_positions", "positions"):
            return (ctx,)
        if method == "list_orders":
            filters = payload.get("filters")
            return (ctx, filters or payload)
        if method == "list_trades":
            filters = payload.get("filters")
            return (ctx, filters or payload)
        if method == "get_order_status":
            order_id = payload.get("order_id")
            if not order_id:
                raise ValueError("缺少 order_id")
            return (ctx, order_id)
        if method == "place_order":
            return (ctx, payload)
        if method == "cancel_order":
            order_id = payload.get("order_id")
            if not order_id:
                raise ValueError("缺少 order_id")
            return (ctx, order_id)
        if method == "cancel_order_request":
            if not payload.get("order_id"):
                raise ValueError("缺少 order_id")
            return (ctx, payload)
        return (ctx, payload)

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        address = peer[0] if isinstance(peer, (list, tuple)) else str(peer)
        log.info(f"[CONN] 新连接: {address}, 当前活跃会话数: {len(self._sessions)}")
        if not self._is_ip_allowed(address):
            log.warning(f"拒绝未授权 IP: {address}")
            writer.close()
            await writer.wait_closed()
            return
        session = ClientSession(self, reader, writer, address)
        try:
            await session.run()
        except Exception as exc:
            log.error(f"[CONN] 会话 {session.session_id} 运行异常: {exc}")
        finally:
            log.info(f"[CONN] 连接关闭: {address}, session={session.session_id}")
            await session.close()

    async def _start_components(self) -> None:
        """依次启动 broker、独立 data adapter 与 tick manager。

        Returns:
            None。

        Raises:
            Exception: 任一显式启用模块启动失败时原样抛出，由 start 统一回滚。

        Side Effects:
            华鑫场景分别创建 Trader 与 XMD 生命周期，不以一个模块就绪替代另一个。
        """

        broker_adapter = self.adapters.broker_adapter
        data_adapter = self.adapters.data_adapter
        if broker_adapter:
            await broker_adapter.start()
        if data_adapter and data_adapter is not broker_adapter:
            start_fn = getattr(data_adapter, "start", None)
            if callable(start_fn):
                await start_fn()
        if self.tick_manager:
            await self.tick_manager.start()

    def _health_snapshot(self) -> Dict:
        journal_ready = self._idempotency_journal is not None
        value = {
            "process_alive": True,
            "uptime_seconds": max(0.0, time.time() - self._created_at),
            "sessions": len(self._sessions),
            "accounts": [ctx.config.key for ctx in self.router.list_accounts()],
            "features": self.active_features(),
            "idempotency_journal": {
                "required": self._idempotency_journal_required,
                "ready": journal_ready,
                "state": "ready"
                if journal_ready or not self._idempotency_journal_required
                else "unavailable",
                "reason": self._idempotency_journal_error,
            },
        }
        backend_status = self._backend_status_snapshot()
        if backend_status is not None:
            if self._idempotency_journal_required and not journal_ready:
                backend_status = dict(backend_status)
                actions = dict(backend_status.get("actions") or {})
                for action in ("broker.place_order", "broker.cancel_order"):
                    actions[action] = {
                        "status": "unavailable",
                        "reason": self._idempotency_journal_error or "持久幂等日志不可用",
                    }
                backend_status["actions"] = actions
            backend_type = (
                backend_status.get("backend_type") if isinstance(backend_status, dict) else None
            )
            status_key = "huaxin" if backend_type == "huaxin" else "qmt"
            value[status_key] = backend_status
            if backend_type:
                value["backend_type"] = backend_type
            big_qmt_gateway = (
                backend_status.get("big_qmt_gateway") if isinstance(backend_status, dict) else None
            )
            if big_qmt_gateway is not None:
                value["big_qmt_gateway"] = big_qmt_gateway
        return {
            "dtype": "dict",
            "value": value,
        }

    def _prepare_allowlist(self, allowlist: List[str]):
        """把已配置白名单编译为严格 IP 网络对象。

        Args:
            allowlist: IPv4/IPv6 单地址或 CIDR 列表。

        Returns:
            list: 可用于成员判断的 ipaddress 网络对象。

        Raises:
            ValueError: 任一条目非法，避免解析为空后意外放行全部来源。
        """

        networks = []
        for entry in allowlist:
            try:
                if "/" in entry:
                    networks.append(ipaddress.ip_network(entry, strict=False))
                else:
                    address = ipaddress.ip_address(entry)
                    networks.append(
                        ipaddress.ip_network(
                            f"{address}/{address.max_prefixlen}",
                            strict=False,
                        )
                    )
            except ValueError as exc:
                raise ValueError(f"server allowlist 含非法 IP/CIDR: {entry}") from exc
        return networks

    def _is_ip_allowed(self, ip: Optional[str]) -> bool:
        if not self._ip_allowlist:
            return True
        if not ip:
            return False
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            return False
        return any(addr in net for net in self._ip_allowlist)

    async def _admin_print_account(self, session: ClientSession, payload: Dict) -> Dict:
        if not self.adapters.broker_adapter:
            raise RuntimeError("券商服务未启用")
        account_key = payload.get("account_key") or session.account_key
        sub_account_id = payload.get("sub_account_id") or session.sub_account_id
        resolved_key, sub_cfg = self.virtual_accounts.resolve(account_key, sub_account_id)
        ctx = self.router.get(resolved_key)
        try:
            info = await self.adapters.broker_adapter.get_account_info(ctx)
            positions = await self.adapters.broker_adapter.get_positions(ctx)
        except Exception as exc:
            raise RuntimeError(f"获取账户信息失败: {exc}")

        # 适配 {"dtype":"dict","value":{...}} 或直接 dict
        if isinstance(info, dict) and info.get("dtype") == "dict" and "value" in info:
            info_dict = dict(info.get("value") or {})
        else:
            info_dict = dict(info or {})

        snapshot = {
            "available_cash": info_dict.get("available_cash"),
            "total_value": info_dict.get("total_value"),
            "positions": positions or [],
        }
        limit = int(payload.get("limit", 20) or 20)
        text = render_account_overview(snapshot, limit=limit)
        if self.config.log_account_snapshot:
            log.info("\n%s", text)
        result = {"dtype": "text", "value": text, "account_key": resolved_key}
        if sub_cfg:
            result["sub_account_id"] = sub_cfg.sub_account_id
        return result


def _estimate_order_value(payload: Dict) -> Optional[float]:
    try:
        amount = abs(float(payload.get("amount") or payload.get("volume") or 0))
    except (TypeError, ValueError):
        amount = 0.0
    style = payload.get("style") or {}
    price = (
        style.get("price")
        or style.get("protect_price")
        or payload.get("price")
        or payload.get("_estimated_price")
    )
    try:
        price = float(price) if price is not None else None
    except (TypeError, ValueError):
        price = None
    if amount and price:
        return amount * price
    return None


def _attach_sub_account_id(result: Any, sub_account_id: str) -> Any:
    """给 broker 返回结果追加子账户标识。

    Args:
        result: broker action 的返回值，通常是 dict 或 list[dict]。
        sub_account_id: 已解析出的虚拟子账户 ID。

    Returns:
        Any: 保持原返回结构的结果；dict/list[dict] 会追加 `sub_account_id`。

    Side Effects:
        对 dict/list[dict] 做原地补充，避免旧调用方的返回形态发生变化。
    """

    if isinstance(result, dict):
        result.setdefault("sub_account_id", sub_account_id)
        return result
    if isinstance(result, list):
        for item in result:
            if isinstance(item, dict):
                item.setdefault("sub_account_id", sub_account_id)
        return result
    return result


def _build_place_order_fingerprint(payload: Dict) -> str:
    """构造兼容旧调用方的下单幂等指纹。

    Args:
        payload: 下单请求载荷。

    Returns:
        str: 规范化 JSON 指纹。
    """

    return _build_write_fingerprint("broker.place_order", payload)


def _validate_required_idempotency_key(payload: Dict[str, Any]) -> None:
    """校验真实交易写请求必须携带可持久化的稳定幂等键。

    Args:
        payload: 下单或撤单请求载荷。

    Returns:
        None。

    Raises:
        ValueError: 幂等键为空、超过 255 字符或包含不安全字符时抛出。
    """

    raw_key = str(payload.get("idempotency_key") or "")
    key = raw_key.strip()
    if not key:
        raise ValueError("交易写请求缺少 idempotency_key")
    if raw_key != key or not _IDEMPOTENCY_KEY_PATTERN.fullmatch(key):
        raise ValueError("idempotency_key 格式非法：仅允许 1-255 位 ASCII 字母、数字及 ._:@/+-")
    payload["idempotency_key"] = key


def _build_write_fingerprint(action: str, payload: Dict[str, Any]) -> str:
    """按具体写 action 构造规范化幂等指纹。

    Args:
        action: `broker.place_order` 或 `broker.cancel_order`。
        payload: 写请求载荷。

    Returns:
        str: 可稳定比较的 JSON 指纹。

    Raises:
        ValueError: action 不是受支持的模糊写时抛出。
    """

    if action not in {"broker.place_order", "broker.cancel_order"}:
        raise ValueError(f"不支持的幂等写 action: {action}")
    style = payload.get("style") or {}
    normalized = {
        "action": action,
        "account_key": str(payload.get("account_key") or ""),
        "sub_account_id": str(payload.get("sub_account_id") or ""),
    }
    if action == "broker.place_order":
        normalized.update(
            {
                "security": str(payload.get("security") or ""),
                "side": str(payload.get("side") or "").upper(),
                "amount": _safe_int(payload.get("amount") or payload.get("volume") or 0),
                "style": {
                    "type": str(style.get("type") or "limit").lower(),
                    "market_type": str(
                        style.get("market_type") or payload.get("market_type") or ""
                    ).lower(),
                    "price": style.get("price"),
                    "protect_price": style.get("protect_price"),
                },
                "execution_binding": {
                    "execution_claim_token": str(payload.get("execution_claim_token") or ""),
                    "execution_claim_generation": _safe_int(
                        payload.get("execution_claim_generation") or 0
                    ),
                    "gateway_id_snapshot": _safe_int(payload.get("gateway_id_snapshot") or 0),
                    "sub_account_binding_id_snapshot": _safe_int(
                        payload.get("sub_account_binding_id_snapshot") or 0
                    ),
                    "backend_provider": str(payload.get("backend_provider") or ""),
                    "binding_version": str(payload.get("binding_version") or ""),
                },
            }
        )
    else:
        provider_extension = payload.get("provider_extension") or {}
        if isinstance(provider_extension, dict):
            huaxin_identity = provider_extension.get("huaxin_tora") or provider_extension
        else:
            huaxin_identity = {}
        if not isinstance(huaxin_identity, dict):
            huaxin_identity = {}
        normalized.update(
            {
                "order_id": str(payload.get("order_id") or ""),
                "provider_order_identity": {
                    "exchange": str(
                        payload.get("exchange") or huaxin_identity.get("exchange") or ""
                    ),
                    "order_sys_id": str(
                        payload.get("order_sys_id") or huaxin_identity.get("order_sys_id") or ""
                    ),
                    "front_id": _safe_int(
                        payload.get("front_id") or huaxin_identity.get("front_id") or 0
                    ),
                    "session_id": _safe_int(
                        payload.get("session_id") or huaxin_identity.get("session_id") or 0
                    ),
                    "order_ref": _safe_int(
                        payload.get("order_ref") or huaxin_identity.get("order_ref") or 0
                    ),
                },
            }
        )
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, default=str)


def _safe_int(value: Any) -> int:
    """将幂等指纹中的数量安全规范为整数。

    Args:
        value: 原始数量值。

    Returns:
        int: 可解析时的整数；无法解析时返回 0。
    """

    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _stable_local_submission_id(idempotency_key: str) -> str:
    """由幂等键派生不泄漏原键的稳定本地提交 ID。

    Args:
        idempotency_key: 原始幂等键。

    Returns:
        str: `submit_unknown:` 前缀的稳定本地 ID。
    """

    digest = hashlib.sha256(str(idempotency_key).encode("utf-8")).hexdigest()[:24]
    return f"submit_unknown:{digest}"


def _unknown_submission_result(
    action: str,
    idempotency_key: str,
    *,
    status: str = "submit_unknown",
    reason: str = "write_result_not_confirmed",
    order_id: Optional[str] = None,
) -> Dict[str, Any]:
    """构造不会被误当成失败后补单的未知态响应。

    Args:
        action: 原始写 action。
        idempotency_key: 原始幂等键。
        status: `submit_unknown` 或 `reconciling`。
        reason: 未能确认的原因。
        order_id: 可选的已知精确订单号。

    Returns:
        Dict[str, Any]: fail-closed 提交未知响应。
    """

    stable_id = str(order_id or "").strip() or _stable_local_submission_id(idempotency_key)
    return {
        "status": status,
        "submission_state": status,
        "write_action": action,
        "idempotency_key": idempotency_key,
        "order_id": stable_id,
        "stable_local_order_id": stable_id,
        "reason": reason,
    }


def _format_resolved_submission(
    action: str,
    idempotency_key: str,
    result: Dict[str, Any],
    *,
    source: str,
) -> Dict[str, Any]:
    """将已缓存或 adapter 结果规范为提交解析响应。

    Args:
        action: 原始写 action。
        idempotency_key: 原始幂等键。
        result: 原始写结果。
        source: 证据来源。

    Returns:
        Dict[str, Any]: 带稳定订单身份的解析响应。
    """

    raw = dict(result or {})
    raw_state = str(raw.get("submission_state") or raw.get("status") or "").lower()
    order_id = str(
        raw.get("order_id")
        or raw.get("stable_local_order_id")
        or (raw.get("last_snapshot") or {}).get("order_id")
        or ""
    ).strip()
    if raw_state in {"submit_unknown", "reconciling"}:
        resolved_state = raw_state
    elif raw_state in {"rejected", "failed", "error"}:
        resolved_state = "rejected"
    elif action == "broker.cancel_order" and raw.get("value") is False:
        resolved_state = "rejected"
    elif action == "broker.cancel_order" and raw_state not in {
        "canceled",
        "cancelled",
        "partly_canceled",
        "partly_cancelled",
    }:
        resolved_state = "submit_unknown"
    elif action == "broker.place_order" and not order_id:
        resolved_state = "submit_unknown"
    else:
        resolved_state = "accepted"
    stable_id = order_id or _stable_local_submission_id(idempotency_key)
    return {
        "status": resolved_state,
        "submission_state": resolved_state,
        "write_action": action,
        "idempotency_key": idempotency_key,
        "order_id": stable_id,
        "stable_local_order_id": stable_id,
        "resolved_result": raw,
        "evidence": {"source": source},
    }


def _format_order_evidence(
    action: str,
    idempotency_key: str,
    order: Dict[str, Any],
) -> Dict[str, Any]:
    """将精确订单查询事实规范为写结果解析。

    Args:
        action: 原始写 action。
        idempotency_key: 原始幂等键。
        order: 券商 adapter 返回的精确订单快照。

    Returns:
        Dict[str, Any]: accepted、rejected 或 reconciling 解析结果。
    """

    row = dict(order)
    order_status = str(row.get("status") or row.get("order_status") or "").lower()
    order_id = str(row.get("order_id") or "").strip()
    if action == "broker.cancel_order":
        if order_status in {"canceled", "cancelled", "partly_canceled", "partly_cancelled"}:
            state = "accepted"
        elif order_status in {"filled", "rejected", "failed", "error"}:
            state = "rejected"
        else:
            state = "reconciling"
    elif order_status in {"rejected", "failed", "error"}:
        state = "rejected"
    elif order_id:
        state = "accepted"
    else:
        state = "submit_unknown"
    stable_id = order_id or _stable_local_submission_id(idempotency_key)
    result = {
        "status": state,
        "submission_state": state,
        "write_action": action,
        "idempotency_key": idempotency_key,
        "order_id": stable_id,
        "stable_local_order_id": stable_id,
        "resolved_result": row,
        "evidence": {"source": "broker_order_query"},
    }
    if action == "broker.cancel_order" and state == "rejected":
        result["cancel_outcome"] = "not_canceled_already_terminal"
    return result


def _order_matches_strong_key(order: Dict[str, Any], strong_keys: Set[str]) -> bool:
    """判断订单是否精确携带任一强关联键。

    Args:
        order: 订单快照。
        strong_keys: 允许精确匹配的幂等键或客户请求号。

    Returns:
        bool: 顶层或受控扩展字段精确匹配时返回 True。
    """

    if not strong_keys:
        return False
    fields = ("idempotency_key", "client_order_id", "request_id")
    containers = [order]
    for name in ("extra", "meta", "provider_extension"):
        value = order.get(name)
        if isinstance(value, dict):
            containers.append(value)
    for container in containers:
        for field in fields:
            value = str(container.get(field) or "").strip()
            if value and value in strong_keys:
                return True
    return False


def _order_matches_submission_contract(
    action: str,
    order: Dict[str, Any],
    request_payload: Dict[str, Any],
    idempotency_key: str,
    expected_order_id: str,
) -> bool:
    """确认订单事实与原写请求完全一致，避免错认反向单或相邻同价单。

    Args:
        action: 原始写 action。
        order: 只读查询到的订单事实。
        request_payload: 原始写请求载荷。
        idempotency_key: 原始幂等键。
        expected_order_id: 撤单请求必须携带的精确订单号；下单时可为空。

    Returns:
        bool: action、原键和所有可声明订单身份均一致时返回 True。
    """

    row = dict(order or {})
    actual_order_id = str(row.get("order_id") or "").strip()
    if not actual_order_id:
        return False
    if action == "broker.cancel_order":
        return bool(expected_order_id) and actual_order_id == expected_order_id
    if action != "broker.place_order" or not _order_matches_strong_key(row, {idempotency_key}):
        return False
    security = str(request_payload.get("security") or "").strip()
    side = str(request_payload.get("side") or "").strip().upper()
    try:
        amount = int(request_payload.get("amount") or request_payload.get("volume") or 0)
        row_amount = int(row.get("amount") or row.get("volume") or 0)
    except (TypeError, ValueError):
        return False
    row_side = str(row.get("side") or row.get("direction") or "").strip().upper()
    return bool(security and side and amount > 0) and (
        str(row.get("security") or "").strip() == security
        and row_side == side
        and row_amount == amount
    )


def _is_final_idempotency_result(result: Dict[str, Any]) -> bool:
    """判断写响应能否在重启后直接重放，而无需继续对账。

    Args:
        result: adapter 返回的写响应。

    Returns:
        bool: 明确接受或明确拒绝返回 True；未知或对账中返回 False。
    """

    state = str(result.get("submission_state") or result.get("status") or "").strip().lower()
    return state not in {"", "submit_unknown", "reconciling", "pending", "unknown"}


def _enforce_cancel_result_semantics(payload: Dict[str, Any], result: Any) -> Dict[str, Any]:
    """把撤单结果收紧为“精确订单已撤”才算成功的公共合同。

    Args:
        payload: 原撤单请求，必须包含精确 `order_id`。
        result: broker adapter 返回的原始结果。

    Returns:
        Dict[str, Any]: `value=True` 仅代表目标订单已取消/部分取消；已成交或已拒绝
        返回 `value=False` 和显式 `cancel_outcome`，证据不足返回 `submit_unknown`。
    """

    expected_order_id = str(payload.get("order_id") or "").strip()
    raw = dict(result) if isinstance(result, dict) else {"value": result}
    if raw.get("cancel_outcome") == "rejected" and raw.get("value") is False:
        raw["success"] = False
        raw["status"] = "rejected"
        raw["submission_state"] = "rejected"
        raw["order_id"] = str(raw.get("order_id") or expected_order_id)
        return raw
    snapshot = raw.get("last_snapshot")
    row = dict(snapshot) if isinstance(snapshot, dict) else raw
    actual_order_id = str(row.get("order_id") or raw.get("order_id") or "").strip()
    status = str(row.get("status") or row.get("order_status") or raw.get("status") or "").lower()
    if expected_order_id and actual_order_id == expected_order_id:
        if status in {"canceled", "cancelled", "partly_canceled", "partly_cancelled"}:
            raw["value"] = True
            raw["success"] = True
            raw["status"] = status
            raw["submission_state"] = status
            raw["cancel_outcome"] = "cancelled"
            raw["order_id"] = actual_order_id
            return raw
        if status in {"filled", "rejected", "failed", "error"}:
            raw["value"] = False
            raw["success"] = False
            raw["status"] = status
            raw["submission_state"] = status
            raw["cancel_outcome"] = "not_canceled_already_terminal"
            raw["order_id"] = actual_order_id
            return raw
    return _unknown_submission_result(
        "broker.cancel_order",
        str(payload.get("idempotency_key") or ""),
        reason="cancel_not_confirmed_by_exact_order_id_and_status",
        order_id=expected_order_id or None,
    )


def _count_open_positions(rows: Any) -> int:
    if not isinstance(rows, list):
        return 0
    count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            amount = int(row.get("amount") or row.get("volume") or 0)
        except (TypeError, ValueError):
            amount = 0
        if amount > 0:
            count += 1
    return count


def _extract_total_value(payload: Any) -> float:
    if isinstance(payload, dict) and payload.get("dtype") == "dict" and "value" in payload:
        payload = payload.get("value") or {}
    if not isinstance(payload, dict):
        return 0.0
    candidates = (
        payload.get("total_value"),
        payload.get("total_asset"),
        payload.get("portfolio_value"),
    )
    for value in candidates:
        try:
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0
