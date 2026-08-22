from __future__ import annotations

import ipaddress
import secrets
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from bullet_trade.utils.env_loader import (
    get_env,
    get_env_bool,
    get_env_float,
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
    key: str
    account_id: str
    account_type: str = "stock"
    data_path: Optional[str] = None
    session_id: Optional[int] = None
    auto_subscribe: Optional[bool] = None


@dataclass
class SubAccountConfig:
    sub_account_id: str
    account_key: str
    order_limit: Optional[float] = None


@dataclass
class ServerConfig:
    """保存通用远程服务及显式华鑫 Trader/XMD 模块配置。"""

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
    idempotency_ttl_seconds: int = 300
    idempotency_journal_path: Optional[str] = None
    idempotency_journal_max_entries: int = 100000
    huaxin_order_identity_journal_path: Optional[str] = None
    huaxin_xmd_backend: Optional[str] = None
    huaxin_xmd_python: Optional[str] = None
    huaxin_xmd_sdk_dir: Optional[str] = None
    huaxin_xmd_front: Optional[str] = None
    huaxin_xmd_max_age_seconds: float = 5.0
    huaxin_xmd_connect_timeout: float = 15.0
    huaxin_xmd_command_timeout: float = 5.0
    huaxin_xmd_snapshot_timeout: float = 5.0


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
    items = []
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


def _is_loopback_listener(value: str) -> bool:
    """判断监听地址是否仅为本机回环。

    Args:
        value: CLI 或环境变量提供的监听地址。

    Returns:
        bool: localhost、IPv4/IPv6 loopback 时为 True。
    """

    text = str(value or "").strip().lower()
    if text == "localhost":
        return True
    try:
        return bool(ipaddress.ip_address(text).is_loopback)
    except ValueError:
        return False


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
        raise ValueError("Huaxin server allowlist 含非法 IP/CIDR: " + ", ".join(invalid))
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
    huaxin_server = str(server_type or "").strip().lower() in {"huaxin", "huaxin-tora"}
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
        strict=huaxin_server and not _is_loopback_listener(listen),
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
    idempotency_ttl_seconds = get_env_int("QMT_SERVER_IDEMPOTENCY_TTL_SECONDS", 300)
    idempotency_journal_path = get_env("QMT_SERVER_IDEMPOTENCY_JOURNAL_PATH")
    idempotency_journal_max_entries = get_env_int(
        "QMT_SERVER_IDEMPOTENCY_JOURNAL_MAX_ENTRIES", 100000
    )
    huaxin_order_identity_journal_path = (
        get_env("HUAXIN_ORDER_IDENTITY_JOURNAL_PATH") if huaxin_server else None
    )
    huaxin_xmd_backend = get_env("HUAXIN_XMD_BACKEND") if huaxin_server else None
    huaxin_xmd_python = get_env("HUAXIN_XMD_PYTHON") if huaxin_server else None
    huaxin_xmd_sdk_dir = get_env("HUAXIN_XMD_SDK_DIR") if huaxin_server else None
    huaxin_xmd_front = get_env("HUAXIN_XMD_FRONT") if huaxin_server else None
    huaxin_xmd_max_age_seconds = (
        get_env_float("HUAXIN_XMD_MAX_AGE_SECONDS", 5.0) if huaxin_server else 5.0
    )
    huaxin_xmd_connect_timeout = (
        get_env_float("HUAXIN_XMD_CONNECT_TIMEOUT", 15.0) if huaxin_server else 15.0
    )
    huaxin_xmd_command_timeout = (
        get_env_float("HUAXIN_XMD_COMMAND_TIMEOUT", 5.0) if huaxin_server else 5.0
    )
    huaxin_xmd_snapshot_timeout = (
        get_env_float("HUAXIN_XMD_SNAPSHOT_TIMEOUT", 5.0) if huaxin_server else 5.0
    )

    accounts_map = _parse_accounts(
        getattr(args, "accounts", None) or get_env("QMT_SERVER_ACCOUNTS")
    )
    if huaxin_server:
        default_account_id = get_env("HUAXIN_ACCOUNT_ID") or get_env("QMT_ACCOUNT_ID")
        default_account_type = get_env("HUAXIN_ACCOUNT_TYPE", "stock")
        default_data_path = None
    else:
        default_account_id = get_env("QMT_ACCOUNT_ID")
        default_account_type = get_env("QMT_ACCOUNT_TYPE", "stock")
        default_data_path = get_env("QMT_DATA_PATH")
    if default_account_id and "default" not in accounts_map:
        accounts_map["default"] = AccountConfig(
            key="default",
            account_id=default_account_id,
            account_type=default_account_type or "stock",
            data_path=default_data_path,
            session_id=None if huaxin_server else get_env_int("QMT_SESSION_ID", 0) or None,
            auto_subscribe=(None if huaxin_server else get_env_optional_bool("QMT_AUTO_SUBSCRIBE")),
        )
    # Propagate default data path/session_id to entries lacking them
    for cfg in accounts_map.values():
        if huaxin_server:
            continue
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

    sub_accounts = _parse_sub_accounts(
        getattr(args, "sub_accounts", None) or get_env("QMT_SERVER_SUB_ACCOUNTS")
    )

    cfg = ServerConfig(
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
        idempotency_ttl_seconds=max(0, int(idempotency_ttl_seconds)),
        idempotency_journal_path=idempotency_journal_path,
        idempotency_journal_max_entries=max(1, int(idempotency_journal_max_entries)),
        huaxin_order_identity_journal_path=huaxin_order_identity_journal_path,
        huaxin_xmd_backend=huaxin_xmd_backend,
        huaxin_xmd_python=huaxin_xmd_python,
        huaxin_xmd_sdk_dir=huaxin_xmd_sdk_dir,
        huaxin_xmd_front=huaxin_xmd_front,
        huaxin_xmd_max_age_seconds=float(huaxin_xmd_max_age_seconds),
        huaxin_xmd_connect_timeout=float(huaxin_xmd_connect_timeout),
        huaxin_xmd_command_timeout=float(huaxin_xmd_command_timeout),
        huaxin_xmd_snapshot_timeout=float(huaxin_xmd_snapshot_timeout),
    )
    return cfg
