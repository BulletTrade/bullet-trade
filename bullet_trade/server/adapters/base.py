from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Protocol, Tuple

from ..config import AccountConfig, ServerConfig, SubAccountConfig


@dataclass
class AccountContext:
    """Runtime holder for一个资金账号"""

    config: AccountConfig
    broker_handle: Optional[object] = None


class RemoteBrokerAdapter(Protocol):
    account_router: "AccountRouter"

    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...

    async def get_account_info(self, account: AccountContext) -> Dict:
        ...

    async def get_positions(self, account: AccountContext) -> List[Dict]:
        ...

    async def list_orders(
        self, account: AccountContext, filters: Optional[Dict] = None
    ) -> List[Dict]:
        ...

    async def list_trades(
        self, account: AccountContext, filters: Optional[Dict] = None
    ) -> List[Dict]:
        ...

    async def get_order_status(self, account: AccountContext, order_id: str) -> Dict:
        ...

    async def place_order(self, account: AccountContext, payload: Dict) -> Dict:
        ...

    async def cancel_order(self, account: AccountContext, order_id: str) -> Dict:
        ...


class RemoteDataAdapter(Protocol):
    async def get_history(self, payload: Dict) -> Dict:
        ...

    async def get_snapshot(self, payload: Dict) -> Dict:
        ...

    async def get_trade_days(self, payload: Dict) -> Dict:
        ...

    async def get_security_info(self, payload: Dict) -> Dict:
        ...

    async def ensure_cache(self, payload: Dict) -> Dict:
        ...

    async def get_current_tick(self, symbol: str) -> Optional[Dict]:
        ...


@dataclass
class AdapterBundle:
    """聚合行情与券商 adapter，并声明交易写入安全能力。

    真实 broker 默认要求每个写请求携带稳定幂等键。Server 在单进程内原子占位，
    持久订单与成交事实由上游账本和柜台各自的权威 owner 管理。
    """

    data_adapter: Optional[RemoteDataAdapter]
    broker_adapter: Optional[RemoteBrokerAdapter]
    broker_writes_require_idempotency_key: bool = True


class AccountRouter:
    """
    在 server 内管理真实账号的连接与上下文。
    """

    def __init__(self, configs: List[AccountConfig]) -> None:
        self._accounts: Dict[str, AccountContext] = {
            cfg.key or "default": AccountContext(cfg) for cfg in configs
        }
        if "default" not in self._accounts and configs:
            self._accounts["default"] = AccountContext(configs[0])
        self._lock: Optional[asyncio.Lock] = None

    @property
    def lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def list_accounts(self) -> List[AccountContext]:
        """枚举去重后的真实账户上下文。

        Returns:
            List[AccountContext]: 按配置顺序返回的唯一上下文；默认回退别名不会重复出现。
        """

        accounts: List[AccountContext] = []
        seen = set()
        for context in self._accounts.values():
            identity = context.config.key or "default"
            if identity in seen:
                continue
            seen.add(identity)
            accounts.append(context)
        return accounts

    def get(self, key: Optional[str]) -> AccountContext:
        if key and key in self._accounts:
            return self._accounts[key]
        if "default" in self._accounts:
            return self._accounts["default"]
        raise KeyError("未配置有效的 account_key")

    async def attach_handle(self, key: str, handle: object) -> None:
        async with self.lock:
            ctx = self.get(key)
            ctx.broker_handle = handle


@dataclass
class SubAccountState:
    config: SubAccountConfig
    used_value: float = 0.0


class VirtualAccountManager:
    """
    负责管理 sub_account_id -> account_key 的映射与额度校验。
    """

    def __init__(self, configs: List[SubAccountConfig]):
        self._configs: Dict[str, SubAccountState] = {
            cfg.sub_account_id: SubAccountState(cfg) for cfg in configs
        }
        self._lock: Optional[asyncio.Lock] = None

    @property
    def lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def resolve(
        self, account_key: Optional[str], sub_account_id: Optional[str]
    ) -> Tuple[str, Optional[SubAccountConfig]]:
        if not sub_account_id:
            return account_key or "default", None
        actual_sub = sub_account_id
        mapped_parent: Optional[str] = None
        if "@" in actual_sub:
            actual_sub, _, parent = actual_sub.partition("@")
            if parent:
                mapped_parent = parent
        state = self._configs.get(actual_sub)
        if state:
            mapped_parent = state.config.account_key or mapped_parent
        parent_key = mapped_parent or account_key or "default"
        cfg = state.config if state else None
        return parent_key, cfg

    async def ensure_within_limit(
        self, sub_cfg: Optional[SubAccountConfig], order_value: Optional[float]
    ) -> None:
        if sub_cfg is None or sub_cfg.order_limit is None:
            return
        if order_value is None:
            return
        if order_value <= sub_cfg.order_limit:
            return
        raise PermissionError(f"子账户 {sub_cfg.sub_account_id} 超过额度限制 {sub_cfg.order_limit}")


class AdapterBuilder(Protocol):
    def __call__(self, config: ServerConfig, account_router: AccountRouter) -> AdapterBundle:
        ...
