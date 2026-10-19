"""作者：BruceLee。

职责：合并远程服务CLI及环境配置，保持行情、交易和账户身份配置边界。
输入：原CLI对象及环境变量；输出：Server/Account配置，不启动SDK或网络。
上下游：原服务入口、账户路由及适配器；券商声明仅用于独立只读身份核验。
约定：券商可选且逐父账户明确配置，不从QMT类型、IP或客户端选择推断。
"""

from __future__ import annotations

import ipaddress
import secrets
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from bullet_trade.utils.env_loader import (
    get_env,
    get_env_bool,
    get_env_int,
    get_env_optional_bool,
)


@dataclass
class TLSConfig:
    enabled: bool = False
    cert_path: Optional[str] = None
    key_path: Optional[str] = None


@dataclass
class AccountConfig:
    """保存父账户路由和可选服务端券商声明；账号配置本身不是柜台查询证明。"""
    key: str
    account_id: str
    account_type: str = "stock"
    data_path: Optional[str] = None
    session_id: Optional[int] = None
    auto_subscribe: Optional[bool] = None
    broker_id: Optional[str] = None

    def __post_init__(self):
        """无输入返回；规范显式券商声明，不填保持旧行为，非法声明抛ValueError，不连接后端。"""
        if self.broker_id is not None:
            from bullet_trade.broker.identity import normalize_broker_id

            self.broker_id = normalize_broker_id(self.broker_id)


@dataclass
class SubAccountConfig:
    sub_account_id: str
    account_key: str
    order_limit: Optional[float] = None


@dataclass
class ServerConfig:
    """保存通用远程服务与 QMT 账户配置。"""

    server_type: str = "qmt"
    listen: str = "0.0.0.0"
    port: int = 58620
    token: str = field(default_factory=lambda: secrets.token_hex(16))
    enable_data: bool = True
    enable_broker: bool = True
    tls: TLSConfig = field(default_factory=TLSConfig)
    allowlist: List[str] = field(default_factory=list)
    max_connections: int = 64
    heartbeat_enabled: bool = True
    max_subscriptions: int = 200
    allow_full_market: bool = False
    accounts: List[AccountConfig] = field(default_factory=list)
    sub_accounts: List[SubAccountConfig] = field(default_factory=list)
    generated_token: bool = False
    log_file: Optional[str] = None
    log_account_snapshot: bool = False
    access_log_enabled: bool = True
    order_risk_enabled: bool = False
    idempotency_max_entries: int = 50000


def _split_items(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.replace(";", ",").split(",") if item.strip()]


def _parse_accounts(raw: Optional[str]) -> Dict[str, AccountConfig]:
    accounts: Dict[str, AccountConfig] = {}
    if not raw:
        return accounts
    for item in _split_items(raw):
        if "=" not in item:
            continue
        alias, rest = item.split("=", 1)
        alias = alias.strip()
        if not alias or not rest:
            continue
        segments = rest.split(":")
        account_id = segments[0].strip()
        account_type = segments[1].strip() if len(segments) > 1 and segments[1].strip() else "stock"
        data_path = segments[2].strip() if len(segments) > 2 and segments[2].strip() else None
        accounts[alias] = AccountConfig(
            key=alias,
            account_id=account_id,
            account_type=account_type,
            data_path=data_path,
        )
    return accounts


def _parse_sub_accounts(raw: Optional[str]) -> List[SubAccountConfig]:
    items: List[SubAccountConfig] = []
    if not raw:
        return items
    for token in _split_items(raw):
        limit = None
        entry = token
        if ":" in token:
            entry, _, extra = token.partition(":")
            extra = extra.strip()
            if extra.startswith("limit="):
                try:
                    limit = float(extra.split("=", 1)[1])
                except ValueError:
                    limit = None
        if "@" in entry:
            sub_id, _, parent = entry.partition("@")
        else:
            sub_id = entry
            parent = ""
        sub_id = sub_id.strip()
        parent = parent.strip()
        if not sub_id:
            continue
        if not parent:
            parent = "default"
        items.append(SubAccountConfig(sub_account_id=sub_id, account_key=parent, order_limit=limit))
    return items


def _parse_account_brokers(raw: Optional[str], accounts: Dict[str, AccountConfig]) -> None:
    """输入显式别名券商映射和原账户，无返回；只修改匹配声明，重复/未知/坏格式拒绝，无I/O。"""
    from bullet_trade.broker.identity import normalize_broker_id

    declared = {}
    for item in _split_items(raw):
        alias, separator, broker = item.partition("=")
        alias = alias.strip()
        if not separator or alias not in accounts or alias in declared:
            raise ValueError("服务端券商声明须对应唯一的已配置账户别名")
        declared[alias] = normalize_broker_id(broker)
    for alias, broker in declared.items():
        accounts[alias].broker_id = broker


def _parse_allowlist(raw: Optional[str], *, strict: bool = False) -> List[str]:
    """解析并规范化来源 IP/CIDR 白名单。

    Args:
        raw: 逗号或分号分隔的 IP/CIDR。
        strict: 遇到任一非法项时是否立即失败。

    Returns:
        List[str]: 规范化为显式前缀的网络列表；IPv6 单地址使用 /128。

    Raises:
        ValueError: strict=True 且存在非法条目。
    """

    result = []
    invalid = []
    for item in _split_items(raw):
        try:
            if "/" in item:
                network = ipaddress.ip_network(item, strict=False)
            else:
                address = ipaddress.ip_address(item)
                network = ipaddress.ip_network(
                    f"{address}/{address.max_prefixlen}",
                    strict=False,
                )
            result.append(network.with_prefixlen)
        except ValueError:
            invalid.append(item)
    if strict and invalid:
        raise ValueError("Server allowlist 含非法 IP/CIDR: " + ", ".join(invalid))
    return result


def build_server_config(args) -> ServerConfig:
    """合并 CLI 与环境变量，构造不触发任何后端连接的服务配置。

    Args:
        args: argparse namespace 或提供同名属性的测试对象。

    Returns:
        ServerConfig: 已完成基础类型转换的服务配置。

    Raises:
        ValueError: TLS 配置不完整或严格白名单包含非法条目时抛出。

    Side Effects:
        仅读取当前进程环境；不读取 secret 文件内容、不创建网络或 SDK 会话。
    """

    server_type = getattr(args, "server_type", None) or get_env("QMT_SERVER_TYPE", "qmt")
    listen = getattr(args, "listen", None) or get_env("QMT_SERVER_LISTEN", "0.0.0.0")
    port = getattr(args, "port", None)
    if port is None:
        port = get_env_int("QMT_SERVER_PORT", 58620)
    raw_token = getattr(args, "token", None) or get_env("QMT_SERVER_TOKEN")
    generated_token = False
    if not raw_token:
        raw_token = secrets.token_hex(16)
        generated_token = True
    enable_data = (
        getattr(args, "enable_data", None)
        if getattr(args, "enable_data", None) is not None
        else get_env_optional_bool("QMT_SERVER_ENABLE_DATA")
    )
    enable_broker = (
        getattr(args, "enable_broker", None)
        if getattr(args, "enable_broker", None) is not None
        else get_env_optional_bool("QMT_SERVER_ENABLE_BROKER")
    )
    tls_cert = getattr(args, "tls_cert", None) or get_env("QMT_SERVER_TLS_CERT")
    tls_key = getattr(args, "tls_key", None) or get_env("QMT_SERVER_TLS_KEY")
    if bool(tls_cert) != bool(tls_key):
        raise ValueError("TLS 配置不完整：证书和私钥必须同时提供")
    tls_enabled = bool(tls_cert and tls_key)
    tls = TLSConfig(enabled=tls_enabled, cert_path=tls_cert, key_path=tls_key)
    allowlist_raw = getattr(args, "allowlist", None) or get_env("QMT_SERVER_ALLOWLIST")
    allowlist = _parse_allowlist(
        allowlist_raw,
        strict=False,
    )
    max_connections = getattr(args, "max_connections", None)
    if max_connections is None:
        max_connections = get_env_int("QMT_SERVER_MAX_CONNECTIONS", 64)
    max_subscriptions = getattr(args, "max_subscriptions", None)
    if max_subscriptions is None:
        max_subscriptions = get_env_int("QMT_SERVER_MAX_SUBSCRIPTIONS", 200)
    allow_full_market = get_env_bool("QMT_SERVER_ALLOW_FULL_MARKET", False)
    log_file = getattr(args, "log_file", None) or get_env("QMT_SERVER_LOG_FILE")
    log_account_snapshot = getattr(args, "log_account_snapshot", None)
    if log_account_snapshot is None:
        flag = get_env_optional_bool("QMT_SERVER_LOG_ACCOUNT")
        log_account_snapshot = bool(flag) if flag is not None else False
    access_log_enabled = getattr(args, "access_log", None)
    if access_log_enabled is None:
        flag = get_env_optional_bool("QMT_SERVER_ACCESS_LOG")
        access_log_enabled = True if flag is None else bool(flag)
    order_risk_enabled = get_env_bool("QMT_SERVER_ORDER_RISK_ENABLED", False)
    idempotency_max_entries = get_env_int("QMT_SERVER_IDEMPOTENCY_MAX_ENTRIES", 50000)
    if idempotency_max_entries <= 0:
        raise ValueError("QMT_SERVER_IDEMPOTENCY_MAX_ENTRIES 必须为正整数")
    accounts_map = _parse_accounts(
        getattr(args, "accounts", None) or get_env("QMT_SERVER_ACCOUNTS")
    )
    default_account_id = get_env("QMT_ACCOUNT_ID")
    default_account_type = get_env("QMT_ACCOUNT_TYPE", "stock")
    default_data_path = get_env("QMT_DATA_PATH")
    if default_account_id and "default" not in accounts_map:
        accounts_map["default"] = AccountConfig(
            key="default",
            account_id=default_account_id,
            account_type=default_account_type or "stock",
            data_path=default_data_path,
            session_id=get_env_int("QMT_SESSION_ID", 0) or None,
            auto_subscribe=get_env_optional_bool("QMT_AUTO_SUBSCRIBE"),
        )
    # Propagate default data path/session_id to entries lacking them
    for cfg in accounts_map.values():
        if not cfg.data_path:
            cfg.data_path = default_data_path
        if cfg.session_id is None:
            sess = get_env("QMT_SESSION_ID")
            if sess:
                try:
                    cfg.session_id = int(sess)
                except ValueError:
                    cfg.session_id = None
        if cfg.auto_subscribe is None:
            cfg.auto_subscribe = get_env_optional_bool("QMT_AUTO_SUBSCRIBE")

    _parse_account_brokers(
        getattr(args, "account_brokers", None) or get_env("QMT_SERVER_ACCOUNT_BROKERS"),
        accounts_map,
    )

    sub_accounts = _parse_sub_accounts(
        getattr(args, "sub_accounts", None) or get_env("QMT_SERVER_SUB_ACCOUNTS")
    )

    server_config = ServerConfig(
        server_type=server_type,
        listen=listen,
        port=port,
        token=raw_token,
        enable_data=True if enable_data is None else enable_data,
        enable_broker=True if enable_broker is None else enable_broker,
        tls=tls,
        allowlist=allowlist,
        max_connections=max_connections,
        heartbeat_enabled=getattr(args, "heartbeat_enabled", True),
        max_subscriptions=max_subscriptions,
        allow_full_market=allow_full_market,
        accounts=list(accounts_map.values()),
        sub_accounts=sub_accounts,
        generated_token=generated_token,
        log_file=log_file,
        log_account_snapshot=bool(log_account_snapshot),
        access_log_enabled=bool(access_log_enabled),
        order_risk_enabled=bool(order_risk_enabled),
        idempotency_max_entries=idempotency_max_entries,
    )
    return server_config
