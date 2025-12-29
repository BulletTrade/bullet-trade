from __future__ import annotations

import asyncio
import ipaddress
from typing import Any, Dict, List, Optional, Set, Tuple

from bullet_trade.core.globals import log
from bullet_trade.utils.portfolio_printer import render_account_overview

from .adapters.base import AccountRouter, AdapterBundle, AccountContext, SubAccountConfig, VirtualAccountManager
from .config import ServerConfig
from .session import ClientSession
from .tick import TickSubscriptionManager


class ServerApplication:
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
        self._ip_allowlist = self._prepare_allowlist(config.allowlist)
        self._shutdown = asyncio.Event()
        self._started = asyncio.Event()

    async def start(self) -> None:
        await self._start_components()
        self._server = await asyncio.start_server(self._handle_client, self.config.listen, self.config.port)
        host = self._server.sockets[0].getsockname() if self._server.sockets else (self.config.listen, self.config.port)
        log.info(f"QMT server listening on {host}")
        self._started.set()
        try:
            await self._server.serve_forever()
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
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
        if self.adapters.broker_adapter:
            try:
                await self.adapters.broker_adapter.stop()
            except Exception:
                pass

    def active_features(self) -> List[str]:
        features = []
        if self.adapters.data_adapter:
            features.append("data")
        if self.adapters.broker_adapter:
            features.append("broker")
        return features

    async def wait_started(self) -> None:
        await self._started.wait()

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

    async def handle_request(self, session: ClientSession, action: Optional[str], payload: Dict) -> Dict:
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
        ctx = self.router.get(resolved_key)
        if method == "place_order":
            await self._maybe_reject_when_paused(payload)
            await self._maybe_fill_price(payload)
            await self.virtual_accounts.ensure_within_limit(sub_cfg, _estimate_order_value(payload))
        impl = method
        fn = getattr(self.adapters.broker_adapter, impl, None)
        if fn is None:
            aliases = {
                "account": "get_account_info",
                "positions": "get_positions",
                "orders": "list_orders",
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
        result = await fn(*args)
        paused_msg = (payload.get("meta") or {}).get("paused_warning")
        if paused_msg:
            log.warning(paused_msg + "（已透传给客户端）")
            try:
                if isinstance(result, dict):
                    result.setdefault("warning", paused_msg)
            except Exception:
                pass
        if sub_cfg:
            result["sub_account_id"] = sub_cfg.sub_account_id
        return result

    async def _maybe_fill_price(self, payload: Dict) -> None:
        """
        若下单缺少 price，尝试用数据服务补充最新成交价。
        """
        try:
            style = payload.get("style") or {}
            price = style.get("price")
            protect_price = style.get("protect_price")
            if price is not None:
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
                price = snapshot.get("last_price") or snapshot.get("lastPrice") or snapshot.get("price")
            if price is None and callable(getattr(data_adapter, "get_history", None)):
                hist = await data_adapter.get_history({"security": security, "count": 1, "frequency": "1m"})
                records = hist.get("records") if isinstance(hist, dict) else None
                if records:
                    last = records[-1]
                    if isinstance(last, (list, tuple)) and last:
                        price = last[-1] if isinstance(last[-1], (int, float)) else None
            if price is not None:
                if style.get("type", "").lower() == "market":
                    style["protect_price"] = float(price)
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
        security = payload.get("security")
        if not security:
            return

        snapshot = None
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
            except Exception:
                continue

        if not isinstance(snapshot, dict):
            return

        paused_flag = snapshot.get("paused")
        if paused_flag is None:
            status = str(snapshot.get("status") or "").lower()
            paused_flag = status in {"paused", "halt", "停牌"}

        if paused_flag:
            msg = f"{security} 停牌，拒绝远程委托"
            log.warning(msg + "（仅警告，不阻塞委托）")
            payload.setdefault("meta", {})["paused_warning"] = msg

    def _build_broker_args(self, method: str, ctx: AccountContext, payload: Optional[Dict]) -> Tuple:
        payload = payload or {}
        if method in ("get_account_info", "get_positions"):
            return (ctx,)
        if method == "list_orders":
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
        return (ctx, payload)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
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
        if self.adapters.broker_adapter:
            await self.adapters.broker_adapter.start()
        if self.tick_manager:
            await self.tick_manager.start()

    def _health_snapshot(self) -> Dict:
        return {
            "dtype": "dict",
            "value": {
                "sessions": len(self._sessions),
                "accounts": [ctx.config.key for ctx in self.router.list_accounts()],
                "features": self.active_features(),
            },
        }

    def _prepare_allowlist(self, allowlist: List[str]):
        networks = []
        for entry in allowlist:
            try:
                if "/" in entry:
                    networks.append(ipaddress.ip_network(entry, strict=False))
                else:
                    networks.append(ipaddress.ip_network(entry + "/32"))
            except ValueError:
                continue
        return networks

    def _is_ip_allowed(self, ip: Optional[str]) -> bool:
        if not self._ip_allowlist or not ip:
            return True
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
    price = style.get("price") or payload.get("price")
    try:
        price = float(price) if price is not None else None
    except (TypeError, ValueError):
        price = None
    if amount and price:
        return amount * price
    return None
