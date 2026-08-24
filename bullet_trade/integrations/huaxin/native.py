"""
作者: BruceLee
文件职责: 通过显式 ctypes 加载和调用 BulletTrade 自研华鑫 flat C ABI。
主要输入: 已通过内容指纹校验的 native bundle、队列容量和 drain 上限。
主要输出: bridge 版本、结构化 health、opaque runtime 生命周期和批量事件。
上游关系: doctor/CLI 或未来 Huaxin Broker/Realtime Feed 在 preflight 后显式调用。
下游关系: native_src 中的自研 fake/offline bridge；未来可替换为同 ABI 的真实 bridge。
关键环境或配置: import 本模块不执行 dlopen；只有 NativeBridge.load 才显式加载动态库。
"""

from __future__ import annotations

import ctypes
import math
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Type

from .errors import (
    HUAXIN_NATIVE_UNAVAILABLE,
    NATIVE_ABI_INCOMPATIBLE,
    NATIVE_CALL_FAILED,
    VENDOR_SCHEMA_INCOMPATIBLE,
    HuaxinAbiError,
    HuaxinNativeCallError,
    HuaxinNativeUnavailableError,
)

ABI_VERSION = 2
VENDOR_SCHEMA_ID = "bullet_trade.huaxin.offline_fake.v1"
FIELD_SET_VERSION = "1"
TRADER_VENDOR_SCHEMA_ID = "bullet_trade.huaxin.tora_trader.v1"
TRADER_FIELD_SET_VERSION = "tora-v4.1.8-node-transfer-v1"
MODE_OFFLINE_FAKE = "offline_fake"
MODE_TRADER = "trader"
VENDOR_SCHEMA_ID_CAPACITY = 64
FIELD_SET_VERSION_CAPACITY = 32
EVENT_PAYLOAD_CAPACITY = 192
REQUEST_PAYLOAD_CAPACITY = 192
OWNED_EVENT_PAYLOAD_CAPACITY = 1024
MAX_DRAIN_EVENTS = 4096
REQUEST_TYPE_PING = 1
REQUEST_QUERY_SECURITY = 100
REQUEST_QUERY_SHAREHOLDER_ACCOUNT = 101
REQUEST_QUERY_TRADING_ACCOUNT = 102
REQUEST_QUERY_POSITION = 103
REQUEST_QUERY_ORDER = 104
REQUEST_QUERY_TRADE = 105
REQUEST_QUERY_SYSTEM_NODE = 106
REQUEST_QUERY_FUND_TRANSFER_DETAIL = 107
REQUEST_QUERY_POSITION_TRANSFER_DETAIL = 108
REQUEST_PLACE_LIMIT = 120
REQUEST_CANCEL_ORDER = 121
REQUEST_PLACE_ORDER = 122
REQUEST_TRANSFER_FUND = 123
REQUEST_TRANSFER_POSITION = 124

EVENT_BRIDGE_CREATED = 1
EVENT_OFFLINE_READY = 2
EVENT_REQUEST_COMPLETED = 3
EVENT_STATE = 100
EVENT_ERROR = 101
EVENT_LOGIN = 102
EVENT_SECURITY = 110
EVENT_SHAREHOLDER_ACCOUNT = 111
EVENT_TRADING_ACCOUNT = 112
EVENT_POSITION = 113
EVENT_ORDER = 114
EVENT_TRADE = 115
EVENT_QUERY_END = 116
EVENT_SYSTEM_NODE = 117
EVENT_FUND_TRANSFER_DETAIL = 118
EVENT_POSITION_TRANSFER_DETAIL = 119
EVENT_ORDER_INSERT_RESPONSE = 120
EVENT_ORDER_ACTION_RESPONSE = 121
EVENT_FUND_TRANSFER_RESPONSE = 122
EVENT_POSITION_TRANSFER_RESPONSE = 123
EVENT_FUND_TRANSFER = 124
EVENT_POSITION_TRANSFER = 125

EVENT_NAMES = {
    EVENT_BRIDGE_CREATED: "bridge_created",
    EVENT_OFFLINE_READY: "offline_ready",
    EVENT_REQUEST_COMPLETED: "request_completed",
    EVENT_STATE: "state",
    EVENT_ERROR: "error",
    EVENT_LOGIN: "login",
    EVENT_SECURITY: "security",
    EVENT_SHAREHOLDER_ACCOUNT: "shareholder_account",
    EVENT_TRADING_ACCOUNT: "trading_account",
    EVENT_POSITION: "position",
    EVENT_ORDER: "order",
    EVENT_TRADE: "trade",
    EVENT_QUERY_END: "query_end",
    EVENT_SYSTEM_NODE: "system_node",
    EVENT_FUND_TRANSFER_DETAIL: "fund_transfer_detail",
    EVENT_POSITION_TRANSFER_DETAIL: "position_transfer_detail",
    EVENT_ORDER_INSERT_RESPONSE: "order_insert_response",
    EVENT_ORDER_ACTION_RESPONSE: "order_action_response",
    EVENT_FUND_TRANSFER_RESPONSE: "fund_transfer_response",
    EVENT_POSITION_TRANSFER_RESPONSE: "position_transfer_response",
    EVENT_FUND_TRANSFER: "fund_transfer",
    EVENT_POSITION_TRANSFER: "position_transfer",
}

FLOW_PATH_CAPACITY = 256
FRONT_CAPACITY = 256
LOGIN_ACCOUNT_CAPACITY = 32
DEPARTMENT_CAPACITY = 16
PASSWORD_CAPACITY = 64
USER_PRODUCT_INFO_CAPACITY = 10
INTERFACE_PRODUCT_INFO_CAPACITY = 32
TERMINAL_INFO_CAPACITY = 255
MAC_ADDRESS_CAPACITY = 20
INTERFACE_ADDRESS_CAPACITY = 128
EXCHANGE_CAPACITY = 8
INVESTOR_CAPACITY = 32
BUSINESS_UNIT_CAPACITY = 32
SHAREHOLDER_CAPACITY = 16
SECURITY_CAPACITY = 32
SECURITY_NAME_CAPACITY = 96
ORDER_LOCAL_ID_CAPACITY = 16
ORDER_SYS_ID_CAPACITY = 32
TRADE_ID_CAPACITY = 32
DATE_CAPACITY = 16
TIME_CAPACITY = 16
ERROR_MESSAGE_CAPACITY = 256
NODE_INFO_CAPACITY = 32

SESSION_FLAG_ENABLE_NODE_TRANSFER = 0x01

_LOGIN_ACCOUNT_TYPES = {
    "user_id": 0,
    "account_id": 1,
    "sha_stock": 2,
    "sz_stock": 3,
    "sh_b_stock": 4,
    "sz_b_stock": 5,
    "three_new_board_a": 6,
    "three_new_board_b": 7,
    "hk_stock": 8,
    "unified_user_id": 9,
    "bj_stock": 10,
}
_TRADE_COMM_MODES = {"tcp": 0, "tcp_direct": 3}
_TOPIC_MODES = {"restart": 0, "resume": 1, "quick": 2}
_ORDER_PRICE_TYPES = {
    "limit": 1,
    "home_best": 2,
    "opponent_best": 3,
    "five_level": 4,
    "any_price": 5,
}
_TIME_CONDITIONS = {"gfd": 1, "ioc": 2}
_VOLUME_CONDITIONS = {"any": 1, "all": 2}
_TRANSFER_DIRECTIONS = {"node_move_in": 1, "node_move_out": 2}
_TRANSFER_POSITION_TYPES = {
    "all": 1,
    "history": 2,
    "today_buy_sell": 3,
    "today_purchase_redeem": 4,
    "today_split_merge": 5,
}
_TRANSFER_STATUSES = {
    0: "unknown",
    1: "handling",
    2: "success",
    3: "failed",
    4: "repeal_handling",
    5: "repeal_success",
    6: "repeal_failed",
    7: "external_accepted",
    8: "sent_to_engine",
}

_SSE_ORDER_COMBINATIONS = {
    ("limit", "gfd", "any"),
    ("home_best", "gfd", "any"),
    ("opponent_best", "gfd", "any"),
    ("five_level", "ioc", "any"),
    ("five_level", "gfd", "any"),
}
_SZSE_ORDER_COMBINATIONS = {
    ("limit", "gfd", "any"),
    ("home_best", "gfd", "any"),
    ("opponent_best", "gfd", "any"),
    ("five_level", "ioc", "any"),
    ("any_price", "ioc", "any"),
    ("any_price", "ioc", "all"),
}

NATIVE_RESULT_ABI_INCOMPATIBLE = -2
NATIVE_RESULT_STRUCT_SIZE_INCOMPATIBLE = -3
NATIVE_RESULT_SCHEMA_INCOMPATIBLE = -6
NATIVE_RESULT_BUFFER_OWNERSHIP_ERROR = -7


class _SchemaIdentity(ctypes.Structure):
    """映射固定长度、显式字节数的 C ABI schema 身份。

    该结构只嵌入其他 POD，不独立传给 native；关键状态是不依赖 NUL 结尾的
    vendor schema ID 和 field-set version。
    """

    _fields_ = [
        ("vendor_schema_id_size", ctypes.c_uint32),
        ("field_set_version_size", ctypes.c_uint32),
        ("vendor_schema_id", ctypes.c_uint8 * VENDOR_SCHEMA_ID_CAPACITY),
        ("field_set_version", ctypes.c_uint8 * FIELD_SET_VERSION_CAPACITY),
    ]


class _CreateOptions(ctypes.Structure):
    """映射 caller-owned 的 C ABI v2 runtime 创建参数结构。

    由 ``NativeBridge.create`` 在栈式 ctypes 内存中构造；native 只在调用期间读取，
    关键状态包含精确 ABI/结构大小、容量和 schema 身份。
    """

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("queue_capacity", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
        ("schema", _SchemaIdentity),
    ]


class _Health(ctypes.Structure):
    """映射 caller-owned 的 C ABI v2 离线 health 输出结构。

    Python 先初始化 ABI/大小/schema，native 再填充水位；关键状态不包含任何账号、
    SDK 路径或厂商对象。
    """

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("state", ctypes.c_int32),
        ("queue_capacity", ctypes.c_uint32),
        ("queue_size", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
        ("dropped_events", ctypes.c_uint64),
        ("schema", _SchemaIdentity),
    ]


class _SessionConfig(ctypes.Structure):
    """映射 caller-owned Trader 会话配置，所有文本使用显式长度。"""

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("encrypt", ctypes.c_uint8),
        ("enable_trading", ctypes.c_uint8),
        ("enable_cancel", ctypes.c_uint8),
        ("reserved_flags", ctypes.c_uint8),
        ("login_account_type", ctypes.c_int32),
        ("trade_comm_mode", ctypes.c_int32),
        ("private_topic", ctypes.c_int32),
        ("public_topic", ctypes.c_int32),
        ("flow_path_size", ctypes.c_uint32),
        ("flow_path", ctypes.c_uint8 * FLOW_PATH_CAPACITY),
        ("trade_front_size", ctypes.c_uint32),
        ("trade_front", ctypes.c_uint8 * FRONT_CAPACITY),
        ("login_account_size", ctypes.c_uint32),
        ("login_account", ctypes.c_uint8 * LOGIN_ACCOUNT_CAPACITY),
        ("department_id_size", ctypes.c_uint32),
        ("department_id", ctypes.c_uint8 * DEPARTMENT_CAPACITY),
        ("password_size", ctypes.c_uint32),
        ("password", ctypes.c_uint8 * PASSWORD_CAPACITY),
        ("dynamic_password_size", ctypes.c_uint32),
        ("dynamic_password", ctypes.c_uint8 * PASSWORD_CAPACITY),
        ("user_product_info_size", ctypes.c_uint32),
        ("user_product_info", ctypes.c_uint8 * USER_PRODUCT_INFO_CAPACITY),
        ("interface_product_info_size", ctypes.c_uint32),
        ("interface_product_info", ctypes.c_uint8 * INTERFACE_PRODUCT_INFO_CAPACITY),
        ("terminal_info_size", ctypes.c_uint32),
        ("terminal_info", ctypes.c_uint8 * TERMINAL_INFO_CAPACITY),
        ("mac_address_size", ctypes.c_uint32),
        ("mac_address", ctypes.c_uint8 * MAC_ADDRESS_CAPACITY),
        ("interface_address_size", ctypes.c_uint32),
        ("interface_address", ctypes.c_uint8 * INTERFACE_ADDRESS_CAPACITY),
        ("schema", _SchemaIdentity),
    ]


class _TraderHealth(ctypes.Structure):
    """映射不改变旧 health 布局的 Trader readiness 输出结构。"""

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("state", ctypes.c_int32),
        ("queue_capacity", ctypes.c_uint32),
        ("queue_size", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
        ("dropped_events", ctypes.c_uint64),
        ("transport_connected", ctypes.c_uint8),
        ("logged_in", ctypes.c_uint8),
        ("ready_for_queries", ctypes.c_uint8),
        ("ready_for_new_orders", ctypes.c_uint8),
        ("ready_for_cancel", ctypes.c_uint8),
        ("reserved_flags", ctypes.c_uint8 * 3),
        ("session_epoch", ctypes.c_uint64),
        ("last_error_id", ctypes.c_int32),
        ("reserved_tail", ctypes.c_uint32),
        ("schema", _SchemaIdentity),
    ]


class _Request(ctypes.Structure):
    """映射 caller-owned 的只读 fake POD 请求结构。

    由 ``NativeRuntime.submit_request`` 构造并仅在同步 C 调用期间借给 bridge；关键状态
    包含稳定 request ID、请求类型、schema 以及不依赖文本终止符的 bytes payload。
    """

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("request_type", ctypes.c_uint32),
        ("payload_size", ctypes.c_uint32),
        ("request_id", ctypes.c_uint64),
        ("schema", _SchemaIdentity),
        ("payload", ctypes.c_uint8 * REQUEST_PAYLOAD_CAPACITY),
    ]


class _Event(ctypes.Structure):
    """映射 bridge-owned batch 内的无指针 C ABI v2 事件 POD。

    由 native 连续缓冲区持有直至显式 free；关键状态包含 request/sequence/int64 时间、
    schema 身份和显式长度 payload，Python 必须先复制再释放 batch。
    """

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("event_type", ctypes.c_uint32),
        ("payload_size", ctypes.c_uint32),
        ("sequence", ctypes.c_uint64),
        ("received_ns", ctypes.c_int64),
        ("request_id", ctypes.c_uint64),
        ("schema", _SchemaIdentity),
        ("payload", ctypes.c_uint8 * EVENT_PAYLOAD_CAPACITY),
    ]


class _EventBatch(ctypes.Structure):
    """映射需显式释放的 bridge-owned 批量事件描述符。

    Python 负责描述符内存，bridge 负责 ``events`` 指向的连续数组；关键状态包含数量、
    stride、schema 和不可复制的 ownership token，生命周期以 free 函数闭环。
    """

    _fields_ = [
        ("abi_version", ctypes.c_uint32),
        ("struct_size", ctypes.c_uint32),
        ("event_count", ctypes.c_uint32),
        ("event_stride", ctypes.c_uint32),
        ("schema", _SchemaIdentity),
        ("events", ctypes.POINTER(_Event)),
        ("ownership_token", ctypes.c_uint64),
    ]


class _QueryRequest(ctypes.Structure):
    """映射证券查询的可选交易所和证券过滤条件。"""

    _fields_ = [
        ("exchange_size", ctypes.c_uint32),
        ("security_size", ctypes.c_uint32),
        ("exchange", ctypes.c_uint8 * EXCHANGE_CAPACITY),
        ("security", ctypes.c_uint8 * SECURITY_CAPACITY),
    ]


class _LimitOrderRequest(ctypes.Structure):
    """映射固定限价/GFD/AV 现货委托请求。"""

    _fields_ = [
        ("exchange_size", ctypes.c_uint32),
        ("investor_id_size", ctypes.c_uint32),
        ("business_unit_id_size", ctypes.c_uint32),
        ("shareholder_id_size", ctypes.c_uint32),
        ("security_size", ctypes.c_uint32),
        ("direction", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 3),
        ("limit_price", ctypes.c_double),
        ("amount", ctypes.c_uint32),
        ("order_ref", ctypes.c_int32),
        ("exchange", ctypes.c_uint8 * EXCHANGE_CAPACITY),
        ("investor_id", ctypes.c_uint8 * INVESTOR_CAPACITY),
        ("business_unit_id", ctypes.c_uint8 * BUSINESS_UNIT_CAPACITY),
        ("shareholder_id", ctypes.c_uint8 * SHAREHOLDER_CAPACITY),
        ("security", ctypes.c_uint8 * SECURITY_CAPACITY),
    ]


class _OrderRequest(ctypes.Structure):
    """映射使用 BulletTrade 稳定枚举的沪深现货委托请求。"""

    _fields_ = [
        ("exchange_size", ctypes.c_uint32),
        ("investor_id_size", ctypes.c_uint32),
        ("business_unit_id_size", ctypes.c_uint32),
        ("shareholder_id_size", ctypes.c_uint32),
        ("security_size", ctypes.c_uint32),
        ("direction", ctypes.c_uint8),
        ("order_price_type", ctypes.c_uint8),
        ("time_condition", ctypes.c_uint8),
        ("volume_condition", ctypes.c_uint8),
        ("limit_price", ctypes.c_double),
        ("amount", ctypes.c_uint32),
        ("order_ref", ctypes.c_int32),
        ("exchange", ctypes.c_uint8 * EXCHANGE_CAPACITY),
        ("investor_id", ctypes.c_uint8 * INVESTOR_CAPACITY),
        ("business_unit_id", ctypes.c_uint8 * BUSINESS_UNIT_CAPACITY),
        ("shareholder_id", ctypes.c_uint8 * SHAREHOLDER_CAPACITY),
        ("security", ctypes.c_uint8 * SECURITY_CAPACITY),
    ]


class _CancelOrderRequest(ctypes.Structure):
    """映射 OrderSysID 或完整会话三元组的明确身份撤单。"""

    _fields_ = [
        ("exchange_size", ctypes.c_uint32),
        ("order_sys_id_size", ctypes.c_uint32),
        ("front_id", ctypes.c_int32),
        ("session_id", ctypes.c_int32),
        ("order_ref", ctypes.c_int32),
        ("exchange", ctypes.c_uint8 * EXCHANGE_CAPACITY),
        ("order_sys_id", ctypes.c_uint8 * ORDER_SYS_ID_CAPACITY),
    ]


class _SystemNodeQueryRequest(ctypes.Structure):
    """映射按节点号过滤的系统节点查询；零表示查询全部。"""

    _fields_ = [("node_id", ctypes.c_int32)]


class _FundTransferDetailQueryRequest(ctypes.Structure):
    """映射资金划拨流水查询过滤条件。"""

    _fields_ = [
        ("department_id_size", ctypes.c_uint32),
        ("account_id_size", ctypes.c_uint32),
        ("investor_id_size", ctypes.c_uint32),
        ("currency", ctypes.c_uint8),
        ("transfer_direction", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 2),
        ("department_id", ctypes.c_uint8 * DEPARTMENT_CAPACITY),
        ("account_id", ctypes.c_uint8 * LOGIN_ACCOUNT_CAPACITY),
        ("investor_id", ctypes.c_uint8 * INVESTOR_CAPACITY),
    ]


class _PositionTransferDetailQueryRequest(ctypes.Structure):
    """映射证券划拨流水查询过滤条件。"""

    _fields_ = [
        ("exchange_size", ctypes.c_uint32),
        ("investor_id_size", ctypes.c_uint32),
        ("business_unit_id_size", ctypes.c_uint32),
        ("shareholder_id_size", ctypes.c_uint32),
        ("security_size", ctypes.c_uint32),
        ("transfer_direction", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 3),
        ("exchange", ctypes.c_uint8 * EXCHANGE_CAPACITY),
        ("investor_id", ctypes.c_uint8 * INVESTOR_CAPACITY),
        ("business_unit_id", ctypes.c_uint8 * BUSINESS_UNIT_CAPACITY),
        ("shareholder_id", ctypes.c_uint8 * SHAREHOLDER_CAPACITY),
        ("security", ctypes.c_uint8 * SECURITY_CAPACITY),
    ]


class _TransferFundRequest(ctypes.Structure):
    """映射默认关闭的跨节点资金划拨写请求。"""

    _fields_ = [
        ("department_id_size", ctypes.c_uint32),
        ("account_id_size", ctypes.c_uint32),
        ("apply_serial", ctypes.c_int32),
        ("external_node_id", ctypes.c_int32),
        ("currency", ctypes.c_uint8),
        ("transfer_direction", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 6),
        ("amount", ctypes.c_double),
        ("department_id", ctypes.c_uint8 * DEPARTMENT_CAPACITY),
        ("account_id", ctypes.c_uint8 * LOGIN_ACCOUNT_CAPACITY),
    ]


class _TransferPositionRequest(ctypes.Structure):
    """映射使用同一持仓行完整身份的跨节点证券划拨请求。"""

    _fields_ = [
        ("exchange_size", ctypes.c_uint32),
        ("investor_id_size", ctypes.c_uint32),
        ("business_unit_id_size", ctypes.c_uint32),
        ("shareholder_id_size", ctypes.c_uint32),
        ("security_size", ctypes.c_uint32),
        ("apply_serial", ctypes.c_int32),
        ("volume", ctypes.c_int32),
        ("market_id", ctypes.c_int32),
        ("external_node_id", ctypes.c_int32),
        ("transfer_direction", ctypes.c_uint8),
        ("transfer_position_type", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 2),
        ("exchange", ctypes.c_uint8 * EXCHANGE_CAPACITY),
        ("investor_id", ctypes.c_uint8 * INVESTOR_CAPACITY),
        ("business_unit_id", ctypes.c_uint8 * BUSINESS_UNIT_CAPACITY),
        ("shareholder_id", ctypes.c_uint8 * SHAREHOLDER_CAPACITY),
        ("security", ctypes.c_uint8 * SECURITY_CAPACITY),
    ]


class _OwnedEvent(ctypes.Structure):
    """映射真实 Trader bridge-owned 大 payload 事件。"""

    pass


class _OwnedEventBatch(ctypes.Structure):
    """映射真实 Trader bridge-owned 大事件批次。"""

    pass


_OwnedEvent._fields_ = [
    ("abi_version", ctypes.c_uint32),
    ("struct_size", ctypes.c_uint32),
    ("event_type", ctypes.c_uint32),
    ("payload_size", ctypes.c_uint32),
    ("sequence", ctypes.c_uint64),
    ("received_ns", ctypes.c_int64),
    ("request_id", ctypes.c_uint64),
    ("schema", _SchemaIdentity),
    ("payload", ctypes.c_uint8 * OWNED_EVENT_PAYLOAD_CAPACITY),
]
_OwnedEventBatch._fields_ = [
    ("abi_version", ctypes.c_uint32),
    ("struct_size", ctypes.c_uint32),
    ("event_count", ctypes.c_uint32),
    ("event_stride", ctypes.c_uint32),
    ("schema", _SchemaIdentity),
    ("events", ctypes.POINTER(_OwnedEvent)),
    ("ownership_token", ctypes.c_uint64),
]


class _StateEvent(ctypes.Structure):
    """映射 Trader 状态与 readiness 变化。"""

    _fields_ = [
        ("state", ctypes.c_int32),
        ("reason", ctypes.c_int32),
        ("transport_connected", ctypes.c_uint8),
        ("logged_in", ctypes.c_uint8),
        ("ready_for_queries", ctypes.c_uint8),
        ("ready_for_new_orders", ctypes.c_uint8),
        ("ready_for_cancel", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 3),
        ("session_epoch", ctypes.c_uint64),
    ]


class _ErrorEvent(ctypes.Structure):
    """映射脱离厂商指针的错误事件。"""

    _fields_ = [
        ("error_id", ctypes.c_int32),
        ("vendor_request_id", ctypes.c_int32),
        ("message_size", ctypes.c_uint32),
        ("message", ctypes.c_uint8 * ERROR_MESSAGE_CAPACITY),
    ]


class _LoginEvent(ctypes.Structure):
    """映射登录成功后的会话身份。"""

    _fields_ = [
        ("front_id", ctypes.c_int32),
        ("session_id", ctypes.c_int32),
        ("max_order_ref", ctypes.c_int32),
        ("trading_day_size", ctypes.c_uint32),
        ("login_time_size", ctypes.c_uint32),
        ("trading_day", ctypes.c_uint8 * DATE_CAPACITY),
        ("login_time", ctypes.c_uint8 * TIME_CAPACITY),
    ]


class _SecurityEvent(ctypes.Structure):
    """映射证券基础信息查询记录。"""

    _fields_ = [
        ("exchange_size", ctypes.c_uint32),
        ("security_size", ctypes.c_uint32),
        ("security_name_size", ctypes.c_uint32),
        ("short_name_size", ctypes.c_uint32),
        ("exchange", ctypes.c_uint8 * EXCHANGE_CAPACITY),
        ("security", ctypes.c_uint8 * SECURITY_CAPACITY),
        ("security_name", ctypes.c_uint8 * SECURITY_NAME_CAPACITY),
        ("short_name", ctypes.c_uint8 * SECURITY_NAME_CAPACITY),
        ("market_id", ctypes.c_int32),
        ("security_type", ctypes.c_int32),
        ("order_unit", ctypes.c_int32),
        ("limit_buy_unit", ctypes.c_int32),
        ("limit_sell_unit", ctypes.c_int32),
        ("min_limit_buy", ctypes.c_int32),
        ("max_limit_buy", ctypes.c_int32),
        ("min_limit_sell", ctypes.c_int32),
        ("max_limit_sell", ctypes.c_int32),
        ("market_buy_unit", ctypes.c_int32),
        ("market_sell_unit", ctypes.c_int32),
        ("min_market_buy", ctypes.c_int32),
        ("max_market_buy", ctypes.c_int32),
        ("min_market_sell", ctypes.c_int32),
        ("max_market_sell", ctypes.c_int32),
        ("volume_multiple", ctypes.c_int32),
        ("has_price_limit", ctypes.c_uint8),
        ("day_trading", ctypes.c_uint8),
        ("reserved_flags", ctypes.c_uint8 * 2),
        ("security_status", ctypes.c_int64),
        ("price_tick", ctypes.c_double),
        ("pre_close_price", ctypes.c_double),
        ("upper_limit_price", ctypes.c_double),
        ("lower_limit_price", ctypes.c_double),
    ]


class _ShareholderEvent(ctypes.Structure):
    """映射股东账户查询记录。"""

    _fields_ = [
        ("investor_id_size", ctypes.c_uint32),
        ("exchange_size", ctypes.c_uint32),
        ("shareholder_id_size", ctypes.c_uint32),
        ("investor_id", ctypes.c_uint8 * INVESTOR_CAPACITY),
        ("exchange", ctypes.c_uint8 * EXCHANGE_CAPACITY),
        ("shareholder_id", ctypes.c_uint8 * SHAREHOLDER_CAPACITY),
        ("market_id", ctypes.c_int32),
        ("shareholder_id_type", ctypes.c_int32),
        ("main_flag", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 3),
    ]


class _AccountEvent(ctypes.Structure):
    """映射资金账户查询记录。"""

    _fields_ = [
        ("department_id_size", ctypes.c_uint32),
        ("account_id_size", ctypes.c_uint32),
        ("department_id", ctypes.c_uint8 * DEPARTMENT_CAPACITY),
        ("account_id", ctypes.c_uint8 * LOGIN_ACCOUNT_CAPACITY),
        ("currency", ctypes.c_int32),
        ("reserved", ctypes.c_int32),
        ("available_cash", ctypes.c_double),
        ("transferable_cash", ctypes.c_double),
        ("frozen_cash", ctypes.c_double),
    ]


class _PositionEvent(ctypes.Structure):
    """映射持仓查询的权威余额、可用量、冻结量和未成交在途字段。"""

    _fields_ = [
        ("exchange_size", ctypes.c_uint32),
        ("investor_id_size", ctypes.c_uint32),
        ("shareholder_id_size", ctypes.c_uint32),
        ("security_size", ctypes.c_uint32),
        ("trading_day_size", ctypes.c_uint32),
        ("exchange", ctypes.c_uint8 * EXCHANGE_CAPACITY),
        ("investor_id", ctypes.c_uint8 * INVESTOR_CAPACITY),
        ("shareholder_id", ctypes.c_uint8 * SHAREHOLDER_CAPACITY),
        ("security", ctypes.c_uint8 * SECURITY_CAPACITY),
        ("trading_day", ctypes.c_uint8 * DATE_CAPACITY),
        ("current_position", ctypes.c_int32),
        ("available_position", ctypes.c_int32),
        ("history_position", ctypes.c_int32),
        ("history_frozen", ctypes.c_int32),
        ("today_bs", ctypes.c_int32),
        ("today_bs_frozen", ctypes.c_int32),
        ("today_pr", ctypes.c_int32),
        ("today_pr_frozen", ctypes.c_int32),
        ("total_cost", ctypes.c_double),
        ("today_sm", ctypes.c_int32),
        ("today_sm_frozen", ctypes.c_int32),
        ("pre_position", ctypes.c_int32),
        ("pre_frozen", ctypes.c_int32),
        ("repay_untrade_volume", ctypes.c_int32),
        ("repay_transfer_untrade_volume", ctypes.c_int32),
        ("collateral_buy_untrade_volume", ctypes.c_int32),
        ("credit_buy_untrade_volume", ctypes.c_int32),
        ("credit_sell_untrade_volume", ctypes.c_int32),
        ("history_position_price", ctypes.c_double),
        ("open_position_cost", ctypes.c_double),
        ("collateral_buy_untrade_amount", ctypes.c_double),
        ("credit_buy_untrade_amount", ctypes.c_double),
        ("credit_sell_untrade_amount", ctypes.c_double),
        ("business_unit_id_size", ctypes.c_uint32),
        ("market_id", ctypes.c_int32),
        ("business_unit_id", ctypes.c_uint8 * BUSINESS_UNIT_CAPACITY),
    ]


class _SystemNodeEvent(ctypes.Structure):
    """映射柜台系统节点目录记录。"""

    _fields_ = [
        ("node_id", ctypes.c_int32),
        ("node_info_size", ctypes.c_uint32),
        ("current", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 3),
        ("node_info", ctypes.c_uint8 * NODE_INFO_CAPACITY),
    ]


class _TransferResponseEvent(ctypes.Structure):
    """映射仅表示请求接受或拒绝的划拨响应。"""

    _fields_ = [
        ("error_id", ctypes.c_int32),
        ("apply_serial", ctypes.c_int32),
        ("message_size", ctypes.c_uint32),
        ("message", ctypes.c_uint8 * ERROR_MESSAGE_CAPACITY),
    ]


class _FundTransferEvent(ctypes.Structure):
    """映射资金划拨最终回报或权威流水记录。"""

    _fields_ = [
        ("department_id_size", ctypes.c_uint32),
        ("account_id_size", ctypes.c_uint32),
        ("investor_id_size", ctypes.c_uint32),
        ("business_unit_id_size", ctypes.c_uint32),
        ("operate_date_size", ctypes.c_uint32),
        ("operate_time_size", ctypes.c_uint32),
        ("status_message_size", ctypes.c_uint32),
        ("fund_serial", ctypes.c_int32),
        ("apply_serial", ctypes.c_int32),
        ("front_id", ctypes.c_int32),
        ("session_id", ctypes.c_int32),
        ("external_node_id", ctypes.c_int32),
        ("currency", ctypes.c_uint8),
        ("transfer_direction", ctypes.c_uint8),
        ("transfer_status", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8),
        ("amount", ctypes.c_double),
        ("department_id", ctypes.c_uint8 * DEPARTMENT_CAPACITY),
        ("account_id", ctypes.c_uint8 * LOGIN_ACCOUNT_CAPACITY),
        ("investor_id", ctypes.c_uint8 * INVESTOR_CAPACITY),
        ("business_unit_id", ctypes.c_uint8 * BUSINESS_UNIT_CAPACITY),
        ("operate_date", ctypes.c_uint8 * DATE_CAPACITY),
        ("operate_time", ctypes.c_uint8 * TIME_CAPACITY),
        ("status_message", ctypes.c_uint8 * ERROR_MESSAGE_CAPACITY),
    ]


class _PositionTransferEvent(ctypes.Structure):
    """映射证券划拨最终回报或权威流水记录。"""

    _fields_ = [
        ("exchange_size", ctypes.c_uint32),
        ("investor_id_size", ctypes.c_uint32),
        ("business_unit_id_size", ctypes.c_uint32),
        ("shareholder_id_size", ctypes.c_uint32),
        ("security_size", ctypes.c_uint32),
        ("trading_day_size", ctypes.c_uint32),
        ("operate_date_size", ctypes.c_uint32),
        ("operate_time_size", ctypes.c_uint32),
        ("status_message_size", ctypes.c_uint32),
        ("position_serial", ctypes.c_int32),
        ("apply_serial", ctypes.c_int32),
        ("front_id", ctypes.c_int32),
        ("session_id", ctypes.c_int32),
        ("market_id", ctypes.c_int32),
        ("external_node_id", ctypes.c_int32),
        ("history_volume", ctypes.c_int32),
        ("today_bs_volume", ctypes.c_int32),
        ("today_pr_volume", ctypes.c_int32),
        ("today_sm_volume", ctypes.c_int32),
        ("transfer_direction", ctypes.c_uint8),
        ("transfer_position_type", ctypes.c_uint8),
        ("transfer_status", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8),
        ("exchange", ctypes.c_uint8 * EXCHANGE_CAPACITY),
        ("investor_id", ctypes.c_uint8 * INVESTOR_CAPACITY),
        ("business_unit_id", ctypes.c_uint8 * BUSINESS_UNIT_CAPACITY),
        ("shareholder_id", ctypes.c_uint8 * SHAREHOLDER_CAPACITY),
        ("security", ctypes.c_uint8 * SECURITY_CAPACITY),
        ("trading_day", ctypes.c_uint8 * DATE_CAPACITY),
        ("operate_date", ctypes.c_uint8 * DATE_CAPACITY),
        ("operate_time", ctypes.c_uint8 * TIME_CAPACITY),
        ("status_message", ctypes.c_uint8 * ERROR_MESSAGE_CAPACITY),
    ]


class _OrderEvent(ctypes.Structure):
    """映射委托查询和私有流回报记录。"""

    _fields_ = [
        ("exchange_size", ctypes.c_uint32),
        ("investor_id_size", ctypes.c_uint32),
        ("shareholder_id_size", ctypes.c_uint32),
        ("security_size", ctypes.c_uint32),
        ("order_local_id_size", ctypes.c_uint32),
        ("order_sys_id_size", ctypes.c_uint32),
        ("trading_day_size", ctypes.c_uint32),
        ("insert_time_size", ctypes.c_uint32),
        ("status_message_size", ctypes.c_uint32),
        ("exchange", ctypes.c_uint8 * EXCHANGE_CAPACITY),
        ("investor_id", ctypes.c_uint8 * INVESTOR_CAPACITY),
        ("shareholder_id", ctypes.c_uint8 * SHAREHOLDER_CAPACITY),
        ("security", ctypes.c_uint8 * SECURITY_CAPACITY),
        ("direction", ctypes.c_uint8),
        ("order_price_type", ctypes.c_uint8),
        ("time_condition", ctypes.c_uint8),
        ("volume_condition", ctypes.c_uint8),
        ("order_status", ctypes.c_uint8),
        ("submit_status", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 2),
        ("limit_price", ctypes.c_double),
        ("amount", ctypes.c_int32),
        ("filled", ctypes.c_int32),
        ("canceled", ctypes.c_int32),
        ("front_id", ctypes.c_int32),
        ("session_id", ctypes.c_int32),
        ("order_ref", ctypes.c_int32),
        ("order_local_id", ctypes.c_uint8 * ORDER_LOCAL_ID_CAPACITY),
        ("order_sys_id", ctypes.c_uint8 * ORDER_SYS_ID_CAPACITY),
        ("trading_day", ctypes.c_uint8 * DATE_CAPACITY),
        ("insert_time", ctypes.c_uint8 * TIME_CAPACITY),
        ("status_message", ctypes.c_uint8 * ERROR_MESSAGE_CAPACITY),
    ]


class _TradeEvent(ctypes.Structure):
    """映射成交查询和私有流回报记录。"""

    _fields_ = [
        ("exchange_size", ctypes.c_uint32),
        ("investor_id_size", ctypes.c_uint32),
        ("shareholder_id_size", ctypes.c_uint32),
        ("security_size", ctypes.c_uint32),
        ("trade_id_size", ctypes.c_uint32),
        ("order_sys_id_size", ctypes.c_uint32),
        ("order_local_id_size", ctypes.c_uint32),
        ("trade_date_size", ctypes.c_uint32),
        ("trade_time_size", ctypes.c_uint32),
        ("trading_day_size", ctypes.c_uint32),
        ("exchange", ctypes.c_uint8 * EXCHANGE_CAPACITY),
        ("investor_id", ctypes.c_uint8 * INVESTOR_CAPACITY),
        ("shareholder_id", ctypes.c_uint8 * SHAREHOLDER_CAPACITY),
        ("security", ctypes.c_uint8 * SECURITY_CAPACITY),
        ("direction", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 3),
        ("trade_id", ctypes.c_uint8 * TRADE_ID_CAPACITY),
        ("order_sys_id", ctypes.c_uint8 * ORDER_SYS_ID_CAPACITY),
        ("order_local_id", ctypes.c_uint8 * ORDER_LOCAL_ID_CAPACITY),
        ("order_ref", ctypes.c_int32),
        ("price", ctypes.c_double),
        ("amount", ctypes.c_int32),
        ("trade_date", ctypes.c_uint8 * DATE_CAPACITY),
        ("trade_time", ctypes.c_uint8 * TIME_CAPACITY),
        ("trading_day", ctypes.c_uint8 * DATE_CAPACITY),
    ]


class _QueryEndEvent(ctypes.Structure):
    """映射每个查询唯一、明确的完成边界。"""

    _fields_ = [
        ("request_type", ctypes.c_uint32),
        ("error_id", ctypes.c_int32),
        ("record_count", ctypes.c_uint32),
        ("message_size", ctypes.c_uint32),
        ("message", ctypes.c_uint8 * ERROR_MESSAGE_CAPACITY),
    ]


class _OrderResponseEvent(ctypes.Structure):
    """映射报单或撤单同步响应/错误回报。"""

    _fields_ = [
        ("error_id", ctypes.c_int32),
        ("order_ref", ctypes.c_int32),
        ("order_sys_id_size", ctypes.c_uint32),
        ("message_size", ctypes.c_uint32),
        ("order_sys_id", ctypes.c_uint8 * ORDER_SYS_ID_CAPACITY),
        ("message", ctypes.c_uint8 * ERROR_MESSAGE_CAPACITY),
    ]


@dataclass(frozen=True)
class NativeHealth:
    """表示 fake/offline 或真实 Trader runtime 的不可变 health 快照。

    由 ``NativeRuntime.health`` 从严格校验后的 C POD 创建；关键状态包含有界队列水位、
    丢弃计数和显式 vendor schema/field-set，不持有 native 内存。
    """

    state: int
    queue_capacity: int
    queue_size: int
    dropped_events: int
    vendor_schema_id: str
    field_set_version: str
    transport_connected: bool = False
    logged_in: bool = False
    ready_for_queries: bool = False
    ready_for_new_orders: bool = False
    ready_for_cancel: bool = False
    session_epoch: int = 0
    last_error_id: int = 0


@dataclass(frozen=True)
class NativeEvent:
    """表示从 native 有界队列 drain 出的 Python 自有事件副本。

    由 runtime 在释放 bridge-owned batch 前逐字段复制；关键状态保留 uint64 request/sequence、
    int64 接收时间、schema 身份及原始 bytes payload。
    """

    event_type: int
    sequence: int
    received_ns: int
    request_id: int
    vendor_schema_id: str
    field_set_version: str
    payload: bytes
    data: Mapping[str, object] = field(default_factory=dict)

    @property
    def event_name(self) -> str:
        """返回稳定事件名，未知整数使用 ``unknown_<id>``。

        Returns:
            str: 供 adapter 匹配且不依赖厂商类型的事件名。
        """

        return EVENT_NAMES.get(self.event_type, f"unknown_{self.event_type}")


@dataclass(frozen=True)
class NativeSessionConfig:
    """表示启动真实 Trader 会话所需的显式配置。

    密码、动态口令、TerminalInfo 和 MacAddress 不参与 repr；调用方应从受控配置
    注入，不得写入 manifest、日志或普通协议事件。
    """

    flow_path: str
    trade_front: str
    login_account: str
    password: str = field(repr=False)
    terminal_info: str = field(repr=False)
    user_product_info: str
    mac_address: str = field(default="", repr=False)
    department_id: str = ""
    dynamic_password: str = field(default="", repr=False)
    interface_product_info: str = ""
    interface_address: str = ""
    login_account_type: str = "account_id"
    trade_comm_mode: str = "tcp"
    encrypt: bool = False
    private_topic: str = "resume"
    public_topic: Optional[str] = None
    enable_trading: bool = False
    enable_cancel: bool = False
    enable_node_transfer: bool = False

    @classmethod
    def from_mapping(
        cls: Type["NativeSessionConfig"], config: Mapping[str, Any]
    ) -> "NativeSessionConfig":
        """从华鑫 broker 配置映射构造会话配置。

        Args:
            config: 使用公开同名键的配置；``login_account`` 缺省回退 ``account_id``。

        Returns:
            NativeSessionConfig: 已完成必填键存在性检查的不可变配置。

        Raises:
            ValueError: flow/front/account/password/UserProductInfo/TerminalInfo
                任一缺失。
        """

        values = {
            "flow_path": config.get("flow_path"),
            "trade_front": config.get("trade_front"),
            "login_account": config.get("login_account") or config.get("account_id"),
            "password": config.get("password"),
            "user_product_info": config.get("user_product_info"),
            "terminal_info": config.get("terminal_info"),
        }
        missing = [
            name for name, value in values.items() if not isinstance(value, str) or not value
        ]
        if missing:
            raise ValueError(f"NativeSessionConfig 缺少必填字段: {', '.join(sorted(missing))}")
        return cls(
            flow_path=str(values["flow_path"]),
            trade_front=str(values["trade_front"]),
            login_account=str(values["login_account"]),
            password=str(values["password"]),
            user_product_info=str(values["user_product_info"]),
            terminal_info=str(values["terminal_info"]),
            mac_address=str(config.get("mac_address") or ""),
            department_id=str(config.get("department_id") or ""),
            dynamic_password=str(config.get("dynamic_password") or ""),
            interface_product_info=str(config.get("interface_product_info") or ""),
            interface_address=str(config.get("interface_address") or ""),
            login_account_type=str(config.get("login_account_type") or "account_id"),
            trade_comm_mode=str(config.get("trade_comm_mode") or "tcp"),
            encrypt=bool(config.get("encrypt", False)),
            private_topic=str(config.get("private_topic") or "resume"),
            public_topic=(
                str(config["public_topic"]) if config.get("public_topic") is not None else None
            ),
            enable_trading=bool(config.get("enable_trading", False)),
            enable_cancel=bool(config.get("enable_cancel", False)),
            enable_node_transfer=bool(config.get("enable_node_transfer", False)),
        )


@dataclass(frozen=True)
class NativeLimitOrderRequest:
    """表示固定限价、当日有效、任意数量成交的 A 股现货委托。"""

    exchange: str
    investor_id: str
    shareholder_id: str
    security: str
    direction: str
    limit_price: float
    amount: int
    order_ref: int
    business_unit_id: str = ""


@dataclass(frozen=True)
class NativeOrderRequest:
    """表示使用受控价格、时效和成交量条件的沪深 A 股现货委托。

    所有类型字段均使用 BulletTrade canonical 名称；调用方不能传入 TORA 原始字符。
    ``limit_price`` 对上交所市价单是强制保护限价，深交所市价单可按官方合同传 0，
    也可由上层安全策略传入正保护价。
    """

    exchange: str
    investor_id: str
    shareholder_id: str
    security: str
    direction: str
    order_price_type: str
    time_condition: str
    volume_condition: str
    limit_price: float
    amount: int
    order_ref: int
    business_unit_id: str = ""


@dataclass(frozen=True)
class NativeCancelOrderRequest:
    """表示使用 OrderSysID 或完整 FrontID/SessionID/OrderRef 的撤单身份。"""

    exchange: str
    order_sys_id: str = ""
    front_id: int = 0
    session_id: int = 0
    order_ref: int = 0


@dataclass(frozen=True)
class NativeFundTransferDetailQuery:
    """表示资金划拨流水的可选权威查询过滤条件。"""

    department_id: str = ""
    account_id: str = ""
    investor_id: str = ""
    currency: str = ""
    transfer_direction: str = ""


@dataclass(frozen=True)
class NativePositionTransferDetailQuery:
    """表示证券划拨流水的可选权威查询过滤条件。"""

    exchange: str = ""
    investor_id: str = ""
    business_unit_id: str = ""
    shareholder_id: str = ""
    security: str = ""
    transfer_direction: str = ""


@dataclass(frozen=True)
class NativeTransferFundRequest:
    """表示显式 ApplySerial 的跨节点资金划拨写请求。"""

    department_id: str
    account_id: str
    currency: str
    transfer_direction: str
    amount: float
    apply_serial: int
    external_node_id: int


@dataclass(frozen=True)
class NativeTransferPositionRequest:
    """表示使用同一持仓行完整身份的跨节点证券划拨写请求。"""

    exchange: str
    investor_id: str
    business_unit_id: str
    shareholder_id: str
    security: str
    market_id: int
    transfer_direction: str
    transfer_position_type: str
    volume: int
    apply_serial: int
    external_node_id: int


def _schema_identity(
    vendor_schema_id: str = VENDOR_SCHEMA_ID,
    field_set_version: str = FIELD_SET_VERSION,
) -> _SchemaIdentity:
    """构造当前 wrapper 明确要求的 schema 身份 POD。

    Args:
        无。

    Returns:
        _SchemaIdentity: 带显式长度且未依赖 NUL 结尾的 C ABI 字节结构。

    Side Effects:
        仅分配 Python 管理的 ctypes 内存，不调用 native。
    """

    vendor_schema = vendor_schema_id.encode("utf-8")
    field_set = field_set_version.encode("utf-8")
    if len(vendor_schema) > VENDOR_SCHEMA_ID_CAPACITY:
        raise RuntimeError("VENDOR_SCHEMA_ID 超过 C ABI 固定容量")
    if len(field_set) > FIELD_SET_VERSION_CAPACITY:
        raise RuntimeError("FIELD_SET_VERSION 超过 C ABI 固定容量")
    identity = _SchemaIdentity(
        vendor_schema_id_size=len(vendor_schema),
        field_set_version_size=len(field_set),
    )
    identity.vendor_schema_id[: len(vendor_schema)] = vendor_schema
    identity.field_set_version[: len(field_set)] = field_set
    return identity


def _decode_schema_identity(
    identity: _SchemaIdentity,
    operation: str,
    expected_vendor_schema_id: str = VENDOR_SCHEMA_ID,
    expected_field_set_version: str = FIELD_SET_VERSION,
) -> tuple:
    """严格解码 native 返回的 schema 身份并拒绝越界长度。

    Args:
        identity: 来自 health、event 或 batch 的嵌入 schema POD。
        operation: 当前检查阶段，用于脱敏诊断。

    Returns:
        tuple: ``(vendor_schema_id, field_set_version)`` 两个 UTF-8 文本。

    Raises:
        HuaxinAbiError: 长度越界、UTF-8 非法或值与 wrapper 合同不一致。

    Side Effects:
        无。
    """

    vendor_size = int(identity.vendor_schema_id_size)
    field_set_size = int(identity.field_set_version_size)
    if vendor_size > VENDOR_SCHEMA_ID_CAPACITY or field_set_size > FIELD_SET_VERSION_CAPACITY:
        raise HuaxinAbiError(
            VENDOR_SCHEMA_INCOMPATIBLE,
            "native schema 身份长度超过 C ABI 固定容量",
            {"operation": operation},
        )
    try:
        vendor_schema_id = bytes(identity.vendor_schema_id[:vendor_size]).decode(
            "utf-8", errors="strict"
        )
        field_set_version = bytes(identity.field_set_version[:field_set_size]).decode(
            "utf-8", errors="strict"
        )
    except UnicodeDecodeError as exc:
        raise HuaxinAbiError(
            VENDOR_SCHEMA_INCOMPATIBLE,
            "native schema 身份不是合法 UTF-8",
            {"operation": operation},
        ) from exc
    if (
        vendor_schema_id != expected_vendor_schema_id
        or field_set_version != expected_field_set_version
    ):
        raise HuaxinAbiError(
            VENDOR_SCHEMA_INCOMPATIBLE,
            "native vendor schema 或 field-set 与 wrapper 不一致",
            {
                "operation": operation,
                "expected_vendor_schema_id": expected_vendor_schema_id,
                "actual_vendor_schema_id": vendor_schema_id,
                "expected_field_set_version": expected_field_set_version,
                "actual_field_set_version": field_set_version,
            },
        )
    return vendor_schema_id, field_set_version


def _validate_native_struct(
    value: ctypes.Structure,
    structure_type: Type[ctypes.Structure],
    operation: str,
    expected_vendor_schema_id: str = VENDOR_SCHEMA_ID,
    expected_field_set_version: str = FIELD_SET_VERSION,
) -> None:
    """验证 native 返回 POD 的 ABI major、精确结构大小和 schema。

    Args:
        value: 已由 native 填充的 ctypes 结构。
        structure_type: 当前合同预期的 ctypes 结构类型。
        operation: 当前操作名称，用于脱敏错误详情。

    Returns:
        None。

    Raises:
        HuaxinAbiError: ABI、结构大小或 schema 任一不匹配。

    Side Effects:
        无。
    """

    actual_abi = int(getattr(value, "abi_version"))
    actual_size = int(getattr(value, "struct_size"))
    expected_size = ctypes.sizeof(structure_type)
    if actual_abi != ABI_VERSION or actual_size != expected_size:
        raise HuaxinAbiError(
            NATIVE_ABI_INCOMPATIBLE,
            "native 返回结构的 ABI 或大小不兼容",
            {
                "operation": operation,
                "expected_abi": ABI_VERSION,
                "actual_abi": actual_abi,
                "expected_size": expected_size,
                "actual_size": actual_size,
            },
        )
    _decode_schema_identity(
        getattr(value, "schema"),
        operation,
        expected_vendor_schema_id,
        expected_field_set_version,
    )


def _encode_text(value: str, capacity: int, field_name: str, required: bool = False) -> bytes:
    """把 Python 文本编码为不含 NUL 的定长 ABI bytes。

    Args:
        value: 待编码文本。
        capacity: C ABI 固定容量。
        field_name: 受控错误中的字段名。
        required: 是否拒绝空文本。

    Returns:
        bytes: UTF-8 编码结果。

    Raises:
        TypeError: value 不是字符串。
        ValueError: 文本为空、含 NUL 或超过固定容量。
    """

    if not isinstance(value, str):
        raise TypeError(f"{field_name} 必须为 str")
    encoded = value.encode("utf-8")
    if required and not encoded:
        raise ValueError(f"{field_name} 不能为空")
    if b"\0" in encoded:
        raise ValueError(f"{field_name} 不得包含 NUL")
    if len(encoded) > capacity:
        raise ValueError(f"{field_name} UTF-8 长度不能超过 {capacity} 字节")
    return encoded


def _assign_text(
    value: ctypes.Structure,
    field_name: str,
    text: str,
    capacity: int,
    required: bool = False,
) -> None:
    """把显式长度文本写入含 ``<name>_size`` 和 bytes 数组的 ctypes 结构。

    Args:
        value: 目标 ctypes 结构。
        field_name: 目标数组字段名。
        text: 待编码文本。
        capacity: 目标固定容量。
        required: 是否拒绝空文本。

    Returns:
        None。

    Side Effects:
        修改 caller-owned ctypes 结构，不调用 native。
    """

    encoded = _encode_text(text, capacity, field_name, required)
    setattr(value, f"{field_name}_size", len(encoded))
    getattr(value, field_name)[: len(encoded)] = encoded


def _session_config_to_raw(
    config: NativeSessionConfig,
    vendor_schema_id: str,
    field_set_version: str,
) -> _SessionConfig:
    """把公开会话配置转换为严格版本化的 C ABI POD。

    Args:
        config: 不可变公开配置。
        vendor_schema_id: 当前真实 bundle 声明的 schema。
        field_set_version: 当前真实 bundle 声明的字段集。

    Returns:
        _SessionConfig: 调用期间由 Python 持有的配置结构。

    Raises:
        ValueError: 枚举或文本字段不满足合同。
    """

    if config.login_account_type not in _LOGIN_ACCOUNT_TYPES:
        raise ValueError("login_account_type 不在允许枚举中")
    if config.trade_comm_mode not in _TRADE_COMM_MODES:
        raise ValueError("trade_comm_mode 仅允许 tcp 或 tcp_direct")
    if config.private_topic not in _TOPIC_MODES:
        raise ValueError("private_topic 仅允许 restart、resume 或 quick")
    if config.public_topic is not None and config.public_topic not in _TOPIC_MODES:
        raise ValueError("public_topic 仅允许 None、restart、resume 或 quick")
    raw = _SessionConfig(
        abi_version=ABI_VERSION,
        struct_size=ctypes.sizeof(_SessionConfig),
        encrypt=int(config.encrypt),
        enable_trading=int(config.enable_trading),
        enable_cancel=int(config.enable_cancel),
        reserved_flags=(SESSION_FLAG_ENABLE_NODE_TRANSFER if config.enable_node_transfer else 0),
        login_account_type=_LOGIN_ACCOUNT_TYPES[config.login_account_type],
        trade_comm_mode=_TRADE_COMM_MODES[config.trade_comm_mode],
        private_topic=_TOPIC_MODES[config.private_topic],
        public_topic=(-1 if config.public_topic is None else _TOPIC_MODES[config.public_topic]),
        schema=_schema_identity(vendor_schema_id, field_set_version),
    )
    _assign_text(raw, "flow_path", config.flow_path, FLOW_PATH_CAPACITY, True)
    _assign_text(raw, "trade_front", config.trade_front, FRONT_CAPACITY, True)
    _assign_text(raw, "login_account", config.login_account, LOGIN_ACCOUNT_CAPACITY, True)
    _assign_text(raw, "department_id", config.department_id, DEPARTMENT_CAPACITY)
    _assign_text(raw, "password", config.password, PASSWORD_CAPACITY, True)
    _assign_text(raw, "dynamic_password", config.dynamic_password, PASSWORD_CAPACITY)
    _assign_text(
        raw,
        "user_product_info",
        config.user_product_info,
        USER_PRODUCT_INFO_CAPACITY,
        True,
    )
    _assign_text(
        raw,
        "interface_product_info",
        config.interface_product_info,
        INTERFACE_PRODUCT_INFO_CAPACITY,
    )
    _assign_text(raw, "terminal_info", config.terminal_info, TERMINAL_INFO_CAPACITY, True)
    _assign_text(raw, "mac_address", config.mac_address, MAC_ADDRESS_CAPACITY, True)
    _assign_text(
        raw,
        "interface_address",
        config.interface_address,
        INTERFACE_ADDRESS_CAPACITY,
    )
    return raw


def _structure_bytes(value: ctypes.Structure) -> bytes:
    """复制一个无指针 ctypes POD 的完整二进制布局。

    Args:
        value: 待复制的 POD。

    Returns:
        bytes: 精确 ``ctypes.sizeof(value)`` 字节。
    """

    return ctypes.string_at(ctypes.byref(value), ctypes.sizeof(value))


def _clear_structure(value: ctypes.Structure) -> None:
    """覆盖并清空包含会话身份或凭据的 caller-owned POD。

    Args:
        value: 调用完成后不再使用的 ctypes 结构。

    Returns:
        None。

    Side Effects:
        原地把结构的全部字节覆盖为零；调用方不得继续读取原字段。
    """

    ctypes.memset(ctypes.byref(value), 0, ctypes.sizeof(value))


def _query_payload(exchange: str = "", security: str = "") -> bytes:
    """构造证券查询过滤 payload。

    Args:
        exchange: 可选 SSE/SZSE/BSE 等交易所文本。
        security: 可选证券代码。

    Returns:
        bytes: `_QueryRequest` 的完整二进制布局。
    """

    raw = _QueryRequest()
    _assign_text(raw, "exchange", exchange, EXCHANGE_CAPACITY)
    _assign_text(raw, "security", security, SECURITY_CAPACITY)
    return _structure_bytes(raw)


def _limit_order_payload(order: NativeLimitOrderRequest) -> bytes:
    """校验并构造限价委托 payload。

    Args:
        order: 公开限价委托请求。

    Returns:
        bytes: `_LimitOrderRequest` 的完整二进制布局。

    Raises:
        ValueError: 方向、价格、数量或订单引用不合法。
    """

    direction = order.direction.lower()
    if direction not in {"buy", "sell"}:
        raise ValueError("direction 仅允许 buy 或 sell")
    try:
        limit_price = float(order.limit_price)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("limit_price 必须为有限且大于 0 的数值") from None
    if not math.isfinite(limit_price) or limit_price <= 0.0:
        raise ValueError("limit_price 必须为有限且大于 0 的数值")
    if (
        isinstance(order.amount, bool)
        or not isinstance(order.amount, int)
        or order.amount < 1
        or order.amount > (1 << 31) - 1
    ):
        raise ValueError("amount 必须为 1..INT32_MAX 的非 bool 整数")
    if order.order_ref < 1 or order.order_ref > (1 << 31) - 1:
        raise ValueError("order_ref 必须为正 int32")
    raw = _LimitOrderRequest(
        direction=0 if direction == "buy" else 1,
        limit_price=limit_price,
        amount=order.amount,
        order_ref=order.order_ref,
    )
    _assign_text(raw, "exchange", order.exchange, EXCHANGE_CAPACITY, True)
    _assign_text(raw, "investor_id", order.investor_id, INVESTOR_CAPACITY, True)
    _assign_text(raw, "business_unit_id", order.business_unit_id, BUSINESS_UNIT_CAPACITY)
    _assign_text(raw, "shareholder_id", order.shareholder_id, SHAREHOLDER_CAPACITY, True)
    _assign_text(raw, "security", order.security, SECURITY_CAPACITY, True)
    return _structure_bytes(raw)


def _normalize_order_exchange(exchange: str) -> str:
    """把公开交易所别名归一为 native 写请求唯一名称。

    Args:
        exchange: SSE/SZSE/BSE 或兼容的 SH/SZ/XSHG/XSHE/BJ/XBEI/数字别名。

    Returns:
        str: SSE、SZSE 或 BSE。

    Raises:
        TypeError: exchange 不是字符串。
        ValueError: exchange 不在受支持的现货交易所别名中。
    """

    if not isinstance(exchange, str):
        raise TypeError("exchange 必须为 str")
    value = exchange.upper()
    if value in {"SSE", "SH", "XSHG", "1"}:
        return "SSE"
    if value in {"SZSE", "SZ", "XSHE", "2"}:
        return "SZSE"
    if value in {"BSE", "BJ", "XBEI", "4"}:
        return "BSE"
    raise ValueError("exchange 仅允许 SSE、SZSE、BSE 及其公开别名")


def _order_payload(order: NativeOrderRequest) -> bytes:
    """校验交易所矩阵并构造不含厂商原始枚举的通用委托 payload。

    Args:
        order: 使用 canonical 类型值的限价或市价委托请求。

    Returns:
        bytes: `_OrderRequest` 的完整二进制布局。

    Raises:
        TypeError: 订单类型不正确。
        ValueError: 枚举、交易所组合、保护价、数量或订单引用不合法。
    """

    if not isinstance(order, NativeOrderRequest):
        raise TypeError("order 必须为 NativeOrderRequest")
    if not isinstance(order.direction, str):
        raise ValueError("direction 必须使用 buy 或 sell 字符串")
    direction = order.direction.lower()
    if direction not in {"buy", "sell"}:
        raise ValueError("direction 仅允许 buy 或 sell")
    if not isinstance(order.order_price_type, str):
        raise ValueError("order_price_type 必须使用 canonical 字符串")
    if not isinstance(order.time_condition, str):
        raise ValueError("time_condition 必须使用 canonical 字符串")
    if not isinstance(order.volume_condition, str):
        raise ValueError("volume_condition 必须使用 canonical 字符串")
    order_price_type = order.order_price_type.lower()
    time_condition = order.time_condition.lower()
    volume_condition = order.volume_condition.lower()
    if order_price_type not in _ORDER_PRICE_TYPES:
        raise ValueError("order_price_type 不在受控枚举中")
    if time_condition not in _TIME_CONDITIONS:
        raise ValueError("time_condition 不在受控枚举中")
    if volume_condition not in _VOLUME_CONDITIONS:
        raise ValueError("volume_condition 不在受控枚举中")

    exchange = _normalize_order_exchange(order.exchange)
    combination = (order_price_type, time_condition, volume_condition)
    if exchange == "SSE":
        supported = combination in _SSE_ORDER_COMBINATIONS
    elif exchange == "SZSE":
        supported = combination in _SZSE_ORDER_COMBINATIONS
    else:
        supported = combination == ("limit", "gfd", "any")
    if not supported:
        raise ValueError("当前交易所不支持该 order_price_type/time_condition/volume_condition 组合")

    try:
        limit_price = float(order.limit_price)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("limit_price 必须为有限数值") from None
    if not math.isfinite(limit_price) or limit_price < 0.0:
        raise ValueError("limit_price 必须为有限且不小于 0 的数值")
    if (order_price_type == "limit" or exchange == "SSE") and limit_price <= 0.0:
        raise ValueError("限价单和上交所市价单的 limit_price 必须大于 0")
    if (
        isinstance(order.amount, bool)
        or not isinstance(order.amount, int)
        or order.amount < 1
        or order.amount > (1 << 31) - 1
    ):
        raise ValueError("amount 必须为 1..INT32_MAX 的非 bool 整数")
    if (
        isinstance(order.order_ref, bool)
        or not isinstance(order.order_ref, int)
        or order.order_ref < 1
        or order.order_ref > (1 << 31) - 1
    ):
        raise ValueError("order_ref 必须为正 int32")

    raw = _OrderRequest(
        direction=0 if direction == "buy" else 1,
        order_price_type=_ORDER_PRICE_TYPES[order_price_type],
        time_condition=_TIME_CONDITIONS[time_condition],
        volume_condition=_VOLUME_CONDITIONS[volume_condition],
        limit_price=limit_price,
        amount=order.amount,
        order_ref=order.order_ref,
    )
    _assign_text(raw, "exchange", exchange, EXCHANGE_CAPACITY, True)
    _assign_text(raw, "investor_id", order.investor_id, INVESTOR_CAPACITY, True)
    _assign_text(raw, "business_unit_id", order.business_unit_id, BUSINESS_UNIT_CAPACITY)
    _assign_text(raw, "shareholder_id", order.shareholder_id, SHAREHOLDER_CAPACITY, True)
    _assign_text(raw, "security", order.security, SECURITY_CAPACITY, True)
    return _structure_bytes(raw)


def _cancel_order_payload(cancel: NativeCancelOrderRequest) -> bytes:
    """校验并构造明确身份撤单 payload。

    Args:
        cancel: OrderSysID 或完整会话三元组撤单请求。

    Returns:
        bytes: `_CancelOrderRequest` 的完整二进制布局。

    Raises:
        ValueError: 身份为空或会话三元组只提供一部分。
    """

    identity_values = (cancel.front_id, cancel.session_id, cancel.order_ref)
    has_any_session_identity = any(value != 0 for value in identity_values)
    has_complete_session_identity = (
        cancel.front_id > 0 and cancel.session_id != 0 and cancel.order_ref > 0
    )
    if has_any_session_identity and not has_complete_session_identity:
        raise ValueError("FrontID/SessionID/OrderRef 必须同时完整提供")
    if not cancel.order_sys_id and not has_complete_session_identity:
        raise ValueError("撤单必须提供 OrderSysID 或完整会话三元组")
    for name, value in (("front_id", cancel.front_id), ("order_ref", cancel.order_ref)):
        if value < 0 or value > (1 << 31) - 1:
            raise ValueError(f"{name} 必须位于非负 int32 范围")
    if cancel.session_id < -(1 << 31) or cancel.session_id > (1 << 31) - 1:
        raise ValueError("session_id 必须位于有符号 int32 范围")
    raw = _CancelOrderRequest(
        front_id=cancel.front_id,
        session_id=cancel.session_id,
        order_ref=cancel.order_ref,
    )
    _assign_text(raw, "exchange", cancel.exchange, EXCHANGE_CAPACITY, True)
    _assign_text(raw, "order_sys_id", cancel.order_sys_id, ORDER_SYS_ID_CAPACITY)
    return _structure_bytes(raw)


def _transfer_direction_code(value: str, *, allow_empty: bool) -> int:
    """把公开节点划拨方向转换为稳定 C ABI 值。

    Args:
        value: ``node_move_in``、``node_move_out``，查询可传空。
        allow_empty: 是否允许空方向作为查询通配。

    Returns:
        int: 稳定 ABI 方向值。

    Raises:
        ValueError: 方向不在白名单中。
    """

    text = str(value or "").strip().lower()
    if allow_empty and not text:
        return 0
    if text not in _TRANSFER_DIRECTIONS:
        raise ValueError("transfer_direction 仅允许 node_move_in 或 node_move_out")
    return _TRANSFER_DIRECTIONS[text]


def _single_char_code(value: str, field_name: str, *, allow_empty: bool) -> int:
    """把单字节厂商字符字段转换为无符号整数。

    Args:
        value: 单个 ASCII 字符。
        field_name: 错误信息字段名。
        allow_empty: 是否允许空文本并返回零。

    Returns:
        int: 0..127 字节值。

    Raises:
        ValueError: 值不是允许的单字节 ASCII。
    """

    text = str(value or "")
    if allow_empty and not text:
        return 0
    encoded = text.encode("ascii", errors="strict")
    if len(encoded) != 1 or encoded[0] == 0:
        raise ValueError(f"{field_name} 必须为单个非 NUL ASCII 字符")
    return encoded[0]


def _system_node_query_payload(node_id: int = 0) -> bytes:
    """构造系统节点查询 payload。

    Args:
        node_id: 零查询全部，正数查询指定节点。

    Returns:
        bytes: 固定布局查询参数。
    """

    if isinstance(node_id, bool) or not isinstance(node_id, int) or node_id < 0:
        raise ValueError("node_id 必须为非负 int")
    return _structure_bytes(_SystemNodeQueryRequest(node_id=node_id))


def _fund_transfer_detail_query_payload(query: NativeFundTransferDetailQuery) -> bytes:
    """构造资金划拨流水查询 payload。

    Args:
        query: 可选账户和方向过滤。

    Returns:
        bytes: 固定布局查询参数。
    """

    if not isinstance(query, NativeFundTransferDetailQuery):
        raise TypeError("query 必须为 NativeFundTransferDetailQuery")
    raw = _FundTransferDetailQueryRequest(
        currency=_single_char_code(query.currency, "currency", allow_empty=True),
        transfer_direction=_transfer_direction_code(query.transfer_direction, allow_empty=True),
    )
    _assign_text(raw, "department_id", query.department_id, DEPARTMENT_CAPACITY)
    _assign_text(raw, "account_id", query.account_id, LOGIN_ACCOUNT_CAPACITY)
    _assign_text(raw, "investor_id", query.investor_id, INVESTOR_CAPACITY)
    return _structure_bytes(raw)


def _position_transfer_detail_query_payload(
    query: NativePositionTransferDetailQuery,
) -> bytes:
    """构造证券划拨流水查询 payload。

    Args:
        query: 可选证券身份和方向过滤。

    Returns:
        bytes: 固定布局查询参数。
    """

    if not isinstance(query, NativePositionTransferDetailQuery):
        raise TypeError("query 必须为 NativePositionTransferDetailQuery")
    raw = _PositionTransferDetailQueryRequest(
        transfer_direction=_transfer_direction_code(query.transfer_direction, allow_empty=True)
    )
    _assign_text(raw, "exchange", query.exchange, EXCHANGE_CAPACITY)
    _assign_text(raw, "investor_id", query.investor_id, INVESTOR_CAPACITY)
    _assign_text(raw, "business_unit_id", query.business_unit_id, BUSINESS_UNIT_CAPACITY)
    _assign_text(raw, "shareholder_id", query.shareholder_id, SHAREHOLDER_CAPACITY)
    _assign_text(raw, "security", query.security, SECURITY_CAPACITY)
    return _structure_bytes(raw)


def _transfer_fund_payload(transfer: NativeTransferFundRequest) -> bytes:
    """校验并构造跨节点资金划拨 payload。

    Args:
        transfer: 显式方向、金额、节点和 ApplySerial。

    Returns:
        bytes: 固定布局写请求。
    """

    if not isinstance(transfer, NativeTransferFundRequest):
        raise TypeError("transfer 必须为 NativeTransferFundRequest")
    try:
        amount = float(transfer.amount)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("amount 必须为有限正数") from None
    if not math.isfinite(amount) or amount <= 0.0:
        raise ValueError("amount 必须为有限正数")
    for field_name, value in (
        ("apply_serial", transfer.apply_serial),
        ("external_node_id", transfer.external_node_id),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or not (1 <= value < (1 << 31)):
            raise ValueError(f"{field_name} 必须为正 int32")
    raw = _TransferFundRequest(
        apply_serial=transfer.apply_serial,
        external_node_id=transfer.external_node_id,
        currency=_single_char_code(transfer.currency, "currency", allow_empty=False),
        transfer_direction=_transfer_direction_code(transfer.transfer_direction, allow_empty=False),
        amount=amount,
    )
    _assign_text(raw, "department_id", transfer.department_id, DEPARTMENT_CAPACITY, True)
    _assign_text(raw, "account_id", transfer.account_id, LOGIN_ACCOUNT_CAPACITY, True)
    return _structure_bytes(raw)


def _transfer_position_payload(transfer: NativeTransferPositionRequest) -> bytes:
    """校验并构造同一持仓身份的跨节点证券划拨 payload。

    Args:
        transfer: 持仓同行身份、数量、方向和 ApplySerial。

    Returns:
        bytes: 固定布局写请求。
    """

    if not isinstance(transfer, NativeTransferPositionRequest):
        raise TypeError("transfer 必须为 NativeTransferPositionRequest")
    for field_name, value in (
        ("apply_serial", transfer.apply_serial),
        ("external_node_id", transfer.external_node_id),
        ("volume", transfer.volume),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or not (1 <= value < (1 << 31)):
            raise ValueError(f"{field_name} 必须为正 int32")
    if (
        isinstance(transfer.market_id, bool)
        or not isinstance(transfer.market_id, int)
        or not (1 <= transfer.market_id <= 255)
    ):
        raise ValueError("market_id 必须为 1..255 的整数")
    position_type = str(transfer.transfer_position_type or "").strip().lower()
    if position_type not in _TRANSFER_POSITION_TYPES:
        raise ValueError("transfer_position_type 不在受控枚举中")
    raw = _TransferPositionRequest(
        apply_serial=transfer.apply_serial,
        volume=transfer.volume,
        market_id=transfer.market_id,
        external_node_id=transfer.external_node_id,
        transfer_direction=_transfer_direction_code(transfer.transfer_direction, allow_empty=False),
        transfer_position_type=_TRANSFER_POSITION_TYPES[position_type],
    )
    _assign_text(raw, "exchange", transfer.exchange, EXCHANGE_CAPACITY, True)
    _assign_text(raw, "investor_id", transfer.investor_id, INVESTOR_CAPACITY, True)
    _assign_text(raw, "business_unit_id", transfer.business_unit_id, BUSINESS_UNIT_CAPACITY)
    _assign_text(raw, "shareholder_id", transfer.shareholder_id, SHAREHOLDER_CAPACITY, True)
    _assign_text(raw, "security", transfer.security, SECURITY_CAPACITY, True)
    return _structure_bytes(raw)


def _decode_event_text(raw: Any, size: int, capacity: int, field_name: str) -> str:
    """按显式长度解码可能为 UTF-8 或 GB18030 的厂商文本。

    Args:
        raw: ctypes uint8 数组。
        size: native 声明的有效字节数。
        capacity: 当前字段固定容量。
        field_name: ABI 错误中的字段名。

    Returns:
        str: 解码后的文本。

    Raises:
        HuaxinAbiError: 长度越界或文本无法按允许编码解码。
    """

    if size < 0 or size > capacity:
        raise HuaxinAbiError(
            NATIVE_ABI_INCOMPATIBLE,
            "Trader 事件文本长度超过固定容量",
            {"field": field_name},
        )
    value = bytes(raw[:size])
    for encoding in ("utf-8", "gb18030"):
        try:
            return value.decode(encoding, errors="strict")
        except UnicodeDecodeError:
            continue
    raise HuaxinAbiError(
        NATIVE_ABI_INCOMPATIBLE,
        "Trader 事件文本编码无法识别",
        {"field": field_name},
    )


def _payload_as(payload: bytes, structure_type: Type[ctypes.Structure]) -> ctypes.Structure:
    """把事件 payload 严格解析为指定 ctypes POD。

    Args:
        payload: native 深拷贝出的 bytes。
        structure_type: 与 event_type 对应的固定结构。

    Returns:
        ctypes.Structure: Python 自有内存中的结构副本。

    Raises:
        HuaxinAbiError: payload 大小与合同不一致。
    """

    expected = ctypes.sizeof(structure_type)
    if len(payload) != expected:
        raise HuaxinAbiError(
            NATIVE_ABI_INCOMPATIBLE,
            "Trader 事件 payload 大小不兼容",
            {"expected_size": expected, "actual_size": len(payload)},
        )
    return structure_type.from_buffer_copy(payload)


def _char_value(value: int) -> str:
    """把厂商单字节枚举转为可序列化字符。

    Args:
        value: 0-255 整数。

    Returns:
        str: ASCII 字符；零值返回空字符串。
    """

    return "" if value == 0 else bytes((value,)).decode("latin-1")


def _direction_value(value: int) -> str:
    """把 bridge 归一化方向码转为 buy/sell 或原始字符。

    Args:
        value: bridge 方向整数。

    Returns:
        str: buy、sell 或未知原始字符。
    """

    if value == 0:
        return "buy"
    if value == 1:
        return "sell"
    return _char_value(value)


def _order_price_type_value(value: int) -> str:
    """把 bridge 稳定价格类型码还原为 canonical 名称。

    Args:
        value: bridge 稳定整数；未知值可能是保留的厂商原始字符。

    Returns:
        str: canonical 名称或未识别的原始单字节字符。
    """

    names = {code: name for name, code in _ORDER_PRICE_TYPES.items()}
    return names.get(value, _char_value(value))


def _time_condition_value(value: int) -> str:
    """把 bridge 稳定有效期条件码还原为 canonical 名称。

    Args:
        value: bridge 稳定整数；未知值可能是保留的厂商原始字符。

    Returns:
        str: gfd、ioc 或未识别的原始单字节字符。
    """

    names = {code: name for name, code in _TIME_CONDITIONS.items()}
    return names.get(value, _char_value(value))


def _volume_condition_value(value: int) -> str:
    """把 bridge 稳定成交量条件码还原为 canonical 名称。

    Args:
        value: bridge 稳定整数；未知值可能是保留的厂商原始字符。

    Returns:
        str: any、all 或未识别的原始单字节字符。
    """

    names = {code: name for name, code in _VOLUME_CONDITIONS.items()}
    return names.get(value, _char_value(value))


def _transfer_direction_name(value: int) -> str:
    """把稳定划拨方向整数转换为公开名称。

    Args:
        value: C ABI 稳定方向值。

    Returns:
        str: canonical 方向或 ``unknown``。
    """

    return {1: "node_move_in", 2: "node_move_out"}.get(value, "unknown")


def _transfer_position_type_name(value: int) -> str:
    """把稳定持仓划拨类型整数转换为公开名称。

    Args:
        value: C ABI 稳定持仓类型。

    Returns:
        str: canonical 持仓类型或 ``unknown``。
    """

    return {
        1: "all",
        2: "history",
        3: "today_buy_sell",
        4: "today_purchase_redeem",
        5: "today_split_merge",
    }.get(value, "unknown")


def _decode_event_data(event_type: int, payload: bytes) -> Mapping[str, object]:
    """把真实 Trader typed payload 解码为 adapter 可消费的结构化字典。

    Args:
        event_type: 稳定 flat ABI 事件整数。
        payload: 已复制到 Python 的 payload。

    Returns:
        Mapping[str, object]: 不含厂商指针或凭据的事件数据；未知事件返回空映射。
    """

    if event_type == EVENT_STATE:
        value = _payload_as(payload, _StateEvent)
        return {
            "state": int(value.state),
            "reason": int(value.reason),
            "transport_connected": bool(value.transport_connected),
            "logged_in": bool(value.logged_in),
            "ready_for_queries": bool(value.ready_for_queries),
            "ready_for_new_orders": bool(value.ready_for_new_orders),
            "ready_for_cancel": bool(value.ready_for_cancel),
            "session_epoch": int(value.session_epoch),
        }
    if event_type == EVENT_ERROR:
        value = _payload_as(payload, _ErrorEvent)
        return {
            "error_id": int(value.error_id),
            "vendor_request_id": int(value.vendor_request_id),
            "error_message": _decode_event_text(
                value.message,
                int(value.message_size),
                ERROR_MESSAGE_CAPACITY,
                "error_message",
            ),
        }
    if event_type == EVENT_LOGIN:
        value = _payload_as(payload, _LoginEvent)
        return {
            "front_id": int(value.front_id),
            "session_id": int(value.session_id),
            "max_order_ref": int(value.max_order_ref),
            "trading_day": _decode_event_text(
                value.trading_day, int(value.trading_day_size), DATE_CAPACITY, "trading_day"
            ),
            "login_time": _decode_event_text(
                value.login_time, int(value.login_time_size), TIME_CAPACITY, "login_time"
            ),
        }
    if event_type == EVENT_SECURITY:
        value = _payload_as(payload, _SecurityEvent)
        return {
            "exchange": _decode_event_text(
                value.exchange, int(value.exchange_size), EXCHANGE_CAPACITY, "exchange"
            ),
            "security": _decode_event_text(
                value.security, int(value.security_size), SECURITY_CAPACITY, "security"
            ),
            "security_name": _decode_event_text(
                value.security_name,
                int(value.security_name_size),
                SECURITY_NAME_CAPACITY,
                "security_name",
            ),
            "short_name": _decode_event_text(
                value.short_name,
                int(value.short_name_size),
                SECURITY_NAME_CAPACITY,
                "short_name",
            ),
            "market_id": int(value.market_id),
            "security_type": int(value.security_type),
            "order_unit": int(value.order_unit),
            "limit_buy_unit": int(value.limit_buy_unit),
            "limit_sell_unit": int(value.limit_sell_unit),
            "min_limit_buy": int(value.min_limit_buy),
            "max_limit_buy": int(value.max_limit_buy),
            "min_limit_sell": int(value.min_limit_sell),
            "max_limit_sell": int(value.max_limit_sell),
            "market_buy_unit": int(value.market_buy_unit),
            "market_sell_unit": int(value.market_sell_unit),
            "min_market_buy": int(value.min_market_buy),
            "max_market_buy": int(value.max_market_buy),
            "min_market_sell": int(value.min_market_sell),
            "max_market_sell": int(value.max_market_sell),
            "volume_multiple": int(value.volume_multiple),
            "has_price_limit": bool(value.has_price_limit),
            "day_trading": bool(value.day_trading),
            "security_status": int(value.security_status),
            "price_tick": float(value.price_tick),
            "pre_close_price": float(value.pre_close_price),
            "upper_limit_price": float(value.upper_limit_price),
            "lower_limit_price": float(value.lower_limit_price),
        }
    if event_type == EVENT_SHAREHOLDER_ACCOUNT:
        value = _payload_as(payload, _ShareholderEvent)
        return {
            "investor_id": _decode_event_text(
                value.investor_id,
                int(value.investor_id_size),
                INVESTOR_CAPACITY,
                "investor_id",
            ),
            "exchange": _decode_event_text(
                value.exchange, int(value.exchange_size), EXCHANGE_CAPACITY, "exchange"
            ),
            "shareholder_id": _decode_event_text(
                value.shareholder_id,
                int(value.shareholder_id_size),
                SHAREHOLDER_CAPACITY,
                "shareholder_id",
            ),
            "market_id": int(value.market_id),
            "shareholder_id_type": int(value.shareholder_id_type),
            "main_flag": bool(value.main_flag),
        }
    if event_type == EVENT_TRADING_ACCOUNT:
        value = _payload_as(payload, _AccountEvent)
        return {
            "department_id": _decode_event_text(
                value.department_id,
                int(value.department_id_size),
                DEPARTMENT_CAPACITY,
                "department_id",
            ),
            "account_id": _decode_event_text(
                value.account_id,
                int(value.account_id_size),
                LOGIN_ACCOUNT_CAPACITY,
                "account_id",
            ),
            "currency": _char_value(int(value.currency)),
            "available_cash": float(value.available_cash),
            "transferable_cash": float(value.transferable_cash),
            "frozen_cash": float(value.frozen_cash),
        }
    if event_type == EVENT_POSITION:
        value = _payload_as(payload, _PositionEvent)
        return {
            "exchange": _decode_event_text(
                value.exchange, int(value.exchange_size), EXCHANGE_CAPACITY, "exchange"
            ),
            "investor_id": _decode_event_text(
                value.investor_id,
                int(value.investor_id_size),
                INVESTOR_CAPACITY,
                "investor_id",
            ),
            "shareholder_id": _decode_event_text(
                value.shareholder_id,
                int(value.shareholder_id_size),
                SHAREHOLDER_CAPACITY,
                "shareholder_id",
            ),
            "security": _decode_event_text(
                value.security, int(value.security_size), SECURITY_CAPACITY, "security"
            ),
            "trading_day": _decode_event_text(
                value.trading_day, int(value.trading_day_size), DATE_CAPACITY, "trading_day"
            ),
            "current_position": int(value.current_position),
            "available_position": int(value.available_position),
            "history_position": int(value.history_position),
            "history_frozen": int(value.history_frozen),
            "today_bs": int(value.today_bs),
            "today_bs_frozen": int(value.today_bs_frozen),
            "today_pr": int(value.today_pr),
            "today_pr_frozen": int(value.today_pr_frozen),
            "total_cost": float(value.total_cost),
            "today_sm": int(value.today_sm),
            "today_sm_frozen": int(value.today_sm_frozen),
            "pre_position": int(value.pre_position),
            "pre_frozen": int(value.pre_frozen),
            "repay_untrade_volume": int(value.repay_untrade_volume),
            "repay_transfer_untrade_volume": int(value.repay_transfer_untrade_volume),
            "collateral_buy_untrade_volume": int(value.collateral_buy_untrade_volume),
            "credit_buy_untrade_volume": int(value.credit_buy_untrade_volume),
            "credit_sell_untrade_volume": int(value.credit_sell_untrade_volume),
            "history_position_price": float(value.history_position_price),
            "open_position_cost": float(value.open_position_cost),
            "collateral_buy_untrade_amount": float(value.collateral_buy_untrade_amount),
            "credit_buy_untrade_amount": float(value.credit_buy_untrade_amount),
            "credit_sell_untrade_amount": float(value.credit_sell_untrade_amount),
            "business_unit_id": _decode_event_text(
                value.business_unit_id,
                int(value.business_unit_id_size),
                BUSINESS_UNIT_CAPACITY,
                "business_unit_id",
            ),
            "market_id": int(value.market_id),
        }
    if event_type == EVENT_SYSTEM_NODE:
        value = _payload_as(payload, _SystemNodeEvent)
        return {
            "node_id": int(value.node_id),
            "node_info": _decode_event_text(
                value.node_info,
                int(value.node_info_size),
                NODE_INFO_CAPACITY,
                "node_info",
            ),
            "current": bool(value.current),
        }
    if event_type in {EVENT_FUND_TRANSFER_DETAIL, EVENT_FUND_TRANSFER}:
        value = _payload_as(payload, _FundTransferEvent)
        return {
            "department_id": _decode_event_text(
                value.department_id,
                int(value.department_id_size),
                DEPARTMENT_CAPACITY,
                "department_id",
            ),
            "account_id": _decode_event_text(
                value.account_id,
                int(value.account_id_size),
                LOGIN_ACCOUNT_CAPACITY,
                "account_id",
            ),
            "investor_id": _decode_event_text(
                value.investor_id,
                int(value.investor_id_size),
                INVESTOR_CAPACITY,
                "investor_id",
            ),
            "business_unit_id": _decode_event_text(
                value.business_unit_id,
                int(value.business_unit_id_size),
                BUSINESS_UNIT_CAPACITY,
                "business_unit_id",
            ),
            "operate_date": _decode_event_text(
                value.operate_date,
                int(value.operate_date_size),
                DATE_CAPACITY,
                "operate_date",
            ),
            "operate_time": _decode_event_text(
                value.operate_time,
                int(value.operate_time_size),
                TIME_CAPACITY,
                "operate_time",
            ),
            "status_message": _decode_event_text(
                value.status_message,
                int(value.status_message_size),
                ERROR_MESSAGE_CAPACITY,
                "status_message",
            ),
            "fund_serial": int(value.fund_serial),
            "apply_serial": int(value.apply_serial),
            "front_id": int(value.front_id),
            "session_id": int(value.session_id),
            "external_node_id": int(value.external_node_id),
            "currency": _char_value(int(value.currency)),
            "transfer_direction": _transfer_direction_name(int(value.transfer_direction)),
            "transfer_status": _TRANSFER_STATUSES.get(int(value.transfer_status), "unknown"),
            "amount": float(value.amount),
        }
    if event_type in {EVENT_POSITION_TRANSFER_DETAIL, EVENT_POSITION_TRANSFER}:
        value = _payload_as(payload, _PositionTransferEvent)
        return {
            "exchange": _decode_event_text(
                value.exchange, int(value.exchange_size), EXCHANGE_CAPACITY, "exchange"
            ),
            "investor_id": _decode_event_text(
                value.investor_id,
                int(value.investor_id_size),
                INVESTOR_CAPACITY,
                "investor_id",
            ),
            "business_unit_id": _decode_event_text(
                value.business_unit_id,
                int(value.business_unit_id_size),
                BUSINESS_UNIT_CAPACITY,
                "business_unit_id",
            ),
            "shareholder_id": _decode_event_text(
                value.shareholder_id,
                int(value.shareholder_id_size),
                SHAREHOLDER_CAPACITY,
                "shareholder_id",
            ),
            "security": _decode_event_text(
                value.security, int(value.security_size), SECURITY_CAPACITY, "security"
            ),
            "trading_day": _decode_event_text(
                value.trading_day,
                int(value.trading_day_size),
                DATE_CAPACITY,
                "trading_day",
            ),
            "operate_date": _decode_event_text(
                value.operate_date,
                int(value.operate_date_size),
                DATE_CAPACITY,
                "operate_date",
            ),
            "operate_time": _decode_event_text(
                value.operate_time,
                int(value.operate_time_size),
                TIME_CAPACITY,
                "operate_time",
            ),
            "status_message": _decode_event_text(
                value.status_message,
                int(value.status_message_size),
                ERROR_MESSAGE_CAPACITY,
                "status_message",
            ),
            "position_serial": int(value.position_serial),
            "apply_serial": int(value.apply_serial),
            "front_id": int(value.front_id),
            "session_id": int(value.session_id),
            "market_id": int(value.market_id),
            "external_node_id": int(value.external_node_id),
            "history_volume": int(value.history_volume),
            "today_bs_volume": int(value.today_bs_volume),
            "today_pr_volume": int(value.today_pr_volume),
            "today_sm_volume": int(value.today_sm_volume),
            "transfer_direction": _transfer_direction_name(int(value.transfer_direction)),
            "transfer_position_type": _transfer_position_type_name(
                int(value.transfer_position_type)
            ),
            "transfer_status": _TRANSFER_STATUSES.get(int(value.transfer_status), "unknown"),
        }
    if event_type == EVENT_ORDER:
        value = _payload_as(payload, _OrderEvent)
        return {
            "exchange": _decode_event_text(
                value.exchange, int(value.exchange_size), EXCHANGE_CAPACITY, "exchange"
            ),
            "investor_id": _decode_event_text(
                value.investor_id,
                int(value.investor_id_size),
                INVESTOR_CAPACITY,
                "investor_id",
            ),
            "shareholder_id": _decode_event_text(
                value.shareholder_id,
                int(value.shareholder_id_size),
                SHAREHOLDER_CAPACITY,
                "shareholder_id",
            ),
            "security": _decode_event_text(
                value.security, int(value.security_size), SECURITY_CAPACITY, "security"
            ),
            "direction": _direction_value(int(value.direction)),
            "order_price_type": _order_price_type_value(int(value.order_price_type)),
            "time_condition": _time_condition_value(int(value.time_condition)),
            "volume_condition": _volume_condition_value(int(value.volume_condition)),
            "limit_price": float(value.limit_price),
            "amount": int(value.amount),
            "filled": int(value.filled),
            "canceled": int(value.canceled),
            "front_id": int(value.front_id),
            "session_id": int(value.session_id),
            "order_ref": int(value.order_ref),
            "order_local_id": _decode_event_text(
                value.order_local_id,
                int(value.order_local_id_size),
                ORDER_LOCAL_ID_CAPACITY,
                "order_local_id",
            ),
            "order_sys_id": _decode_event_text(
                value.order_sys_id,
                int(value.order_sys_id_size),
                ORDER_SYS_ID_CAPACITY,
                "order_sys_id",
            ),
            "order_status": _char_value(int(value.order_status)),
            "submit_status": _char_value(int(value.submit_status)),
            "trading_day": _decode_event_text(
                value.trading_day, int(value.trading_day_size), DATE_CAPACITY, "trading_day"
            ),
            "insert_time": _decode_event_text(
                value.insert_time, int(value.insert_time_size), TIME_CAPACITY, "insert_time"
            ),
            "status_msg": _decode_event_text(
                value.status_message,
                int(value.status_message_size),
                ERROR_MESSAGE_CAPACITY,
                "status_message",
            ),
        }
    if event_type == EVENT_TRADE:
        value = _payload_as(payload, _TradeEvent)
        return {
            "exchange": _decode_event_text(
                value.exchange, int(value.exchange_size), EXCHANGE_CAPACITY, "exchange"
            ),
            "investor_id": _decode_event_text(
                value.investor_id,
                int(value.investor_id_size),
                INVESTOR_CAPACITY,
                "investor_id",
            ),
            "shareholder_id": _decode_event_text(
                value.shareholder_id,
                int(value.shareholder_id_size),
                SHAREHOLDER_CAPACITY,
                "shareholder_id",
            ),
            "security": _decode_event_text(
                value.security, int(value.security_size), SECURITY_CAPACITY, "security"
            ),
            "direction": _direction_value(int(value.direction)),
            "trade_id": _decode_event_text(
                value.trade_id, int(value.trade_id_size), TRADE_ID_CAPACITY, "trade_id"
            ),
            "order_sys_id": _decode_event_text(
                value.order_sys_id,
                int(value.order_sys_id_size),
                ORDER_SYS_ID_CAPACITY,
                "order_sys_id",
            ),
            "order_local_id": _decode_event_text(
                value.order_local_id,
                int(value.order_local_id_size),
                ORDER_LOCAL_ID_CAPACITY,
                "order_local_id",
            ),
            "order_ref": int(value.order_ref),
            "price": float(value.price),
            "amount": int(value.amount),
            "trade_date": _decode_event_text(
                value.trade_date, int(value.trade_date_size), DATE_CAPACITY, "trade_date"
            ),
            "trade_time": _decode_event_text(
                value.trade_time, int(value.trade_time_size), TIME_CAPACITY, "trade_time"
            ),
            "trading_day": _decode_event_text(
                value.trading_day, int(value.trading_day_size), DATE_CAPACITY, "trading_day"
            ),
        }
    if event_type == EVENT_QUERY_END:
        value = _payload_as(payload, _QueryEndEvent)
        return {
            "request_type": int(value.request_type),
            "error_id": int(value.error_id),
            "record_count": int(value.record_count),
            "error_message": _decode_event_text(
                value.message,
                int(value.message_size),
                ERROR_MESSAGE_CAPACITY,
                "error_message",
            ),
        }
    if event_type in {EVENT_ORDER_INSERT_RESPONSE, EVENT_ORDER_ACTION_RESPONSE}:
        value = _payload_as(payload, _OrderResponseEvent)
        return {
            "error_id": int(value.error_id),
            "order_ref": int(value.order_ref),
            "order_sys_id": _decode_event_text(
                value.order_sys_id,
                int(value.order_sys_id_size),
                ORDER_SYS_ID_CAPACITY,
                "order_sys_id",
            ),
            "error_message": _decode_event_text(
                value.message,
                int(value.message_size),
                ERROR_MESSAGE_CAPACITY,
                "error_message",
            ),
        }
    if event_type in {EVENT_FUND_TRANSFER_RESPONSE, EVENT_POSITION_TRANSFER_RESPONSE}:
        value = _payload_as(payload, _TransferResponseEvent)
        return {
            "error_id": int(value.error_id),
            "apply_serial": int(value.apply_serial),
            "error_message": _decode_event_text(
                value.message,
                int(value.message_size),
                ERROR_MESSAGE_CAPACITY,
                "error_message",
            ),
        }
    return {}


def _configure_signatures(library: ctypes.CDLL) -> None:
    """
    为显式加载的自研动态库设置 ctypes 参数和返回类型。

    参数:
        library: 已由调用方显式 dlopen 的 ctypes 动态库对象。
    返回:
        无返回值。
    副作用:
        修改 ctypes 函数对象的 argtypes/restype，避免隐式整数截断。
    异常:
        AttributeError: 动态库缺少必需 C ABI 符号时抛出。
    """

    library.bt_huaxin_abi_version.argtypes = []
    library.bt_huaxin_abi_version.restype = ctypes.c_uint32
    library.bt_huaxin_bridge_version.argtypes = []
    library.bt_huaxin_bridge_version.restype = ctypes.c_char_p
    library.bt_huaxin_vendor_schema_id.argtypes = []
    library.bt_huaxin_vendor_schema_id.restype = ctypes.c_char_p
    library.bt_huaxin_field_set_version.argtypes = []
    library.bt_huaxin_field_set_version.restype = ctypes.c_char_p
    library.bt_huaxin_error_message.argtypes = [ctypes.c_int32]
    library.bt_huaxin_error_message.restype = ctypes.c_char_p
    library.bt_huaxin_create.argtypes = [
        ctypes.POINTER(_CreateOptions),
        ctypes.POINTER(ctypes.c_void_p),
    ]
    library.bt_huaxin_create.restype = ctypes.c_int32
    library.bt_huaxin_destroy.argtypes = [ctypes.c_void_p]
    library.bt_huaxin_destroy.restype = ctypes.c_int32
    library.bt_huaxin_get_health.argtypes = [ctypes.c_void_p, ctypes.POINTER(_Health)]
    library.bt_huaxin_get_health.restype = ctypes.c_int32
    library.bt_huaxin_start_session.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_SessionConfig),
    ]
    library.bt_huaxin_start_session.restype = ctypes.c_int32
    library.bt_huaxin_stop_session.argtypes = [ctypes.c_void_p]
    library.bt_huaxin_stop_session.restype = ctypes.c_int32
    library.bt_huaxin_get_trader_health.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_TraderHealth),
    ]
    library.bt_huaxin_get_trader_health.restype = ctypes.c_int32
    library.bt_huaxin_submit_request.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_Request),
    ]
    library.bt_huaxin_submit_request.restype = ctypes.c_int32
    library.bt_huaxin_drain_event_batch.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(_EventBatch),
    ]
    library.bt_huaxin_drain_event_batch.restype = ctypes.c_int32
    library.bt_huaxin_free_event_batch.argtypes = [ctypes.POINTER(_EventBatch)]
    library.bt_huaxin_free_event_batch.restype = ctypes.c_int32
    library.bt_huaxin_drain_owned_event_batch.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(_OwnedEventBatch),
    ]
    library.bt_huaxin_drain_owned_event_batch.restype = ctypes.c_int32
    library.bt_huaxin_free_owned_event_batch.argtypes = [ctypes.POINTER(_OwnedEventBatch)]
    library.bt_huaxin_free_owned_event_batch.restype = ctypes.c_int32


class NativeBridge:
    """代表已通过 bundle 与 ABI v2 身份校验的显式 C bridge。

    核心协作对象是 ``verify_bundle``、ctypes 动态库和 ``NativeRuntime``；关键状态包含
    制品路径、不可变 manifest 视图及配置好签名的库对象，构造本身不创建 runtime。
    """

    def __init__(
        self,
        library_path: Path,
        library: ctypes.CDLL,
        manifest: Dict[str, Any],
    ) -> None:
        """
        保存已加载动态库和已验证 manifest。

        参数:
            library_path: bundle 内自研动态库的绝对路径。
            library: 已配置函数签名的 ctypes.CDLL 对象。
            manifest: 已通过指纹和 artifact hash 校验的 manifest。
        返回:
            无返回值；初始化 bridge 对象。
        """

        self.library_path = library_path
        self._library = library
        self.manifest = dict(manifest)
        bridge_manifest = manifest.get("bridge", {})
        self.mode = str(manifest.get("mode", MODE_OFFLINE_FAKE))
        self.expected_vendor_schema_id = str(
            bridge_manifest.get("vendor_schema_id", VENDOR_SCHEMA_ID)
        )
        self.expected_field_set_version = str(
            bridge_manifest.get("field_set_version", FIELD_SET_VERSION)
        )

    @classmethod
    def load(cls: Type["NativeBridge"], bundle_path: Path) -> "NativeBridge":
        """
        校验 bundle 后显式加载自研 native bridge。

        参数:
            bundle_path: 含 manifest.json 和自研动态库的 bundle 目录。
        返回:
            可创建 opaque runtime 的 NativeBridge。
        副作用:
            校验成功后调用 ctypes.CDLL；真实 bundle 会由系统装载器解析其已校验的
            ``$ORIGIN/vendor`` 依赖，但不会创建 TORA runtime 或连接柜台。
        异常:
            HuaxinBundleError: manifest、指纹或 artifact 校验失败。
            HuaxinNativeUnavailableError: 动态库无法由当前平台加载。
            HuaxinAbiError: 动态库 ABI major 与 wrapper 不一致。
        """

        from .build import _runtime_vendor_status, verify_bundle

        manifest, artifact_path = verify_bundle(bundle_path)
        if manifest.get("mode") == MODE_TRADER:
            runtime_ready, _runtime_path, runtime_status = _runtime_vendor_status(
                bundle_path, manifest
            )
            if not runtime_ready:
                raise HuaxinNativeUnavailableError(
                    HUAXIN_NATIVE_UNAVAILABLE,
                    "真实华鑫 bundle 的外部 Trader 运行时库未通过校验",
                    {"runtime_status": runtime_status},
                )
        try:
            library = ctypes.CDLL(str(artifact_path))
            _configure_signatures(library)
        except (OSError, AttributeError) as exc:
            raise HuaxinNativeUnavailableError(
                HUAXIN_NATIVE_UNAVAILABLE,
                "自研华鑫 bridge 无法在当前进程显式加载",
                {"error_type": type(exc).__name__},
            ) from exc

        actual_abi = int(library.bt_huaxin_abi_version())
        if actual_abi != ABI_VERSION:
            raise HuaxinAbiError(
                NATIVE_ABI_INCOMPATIBLE,
                "Python wrapper 与 native bridge ABI 不一致",
                {"expected": ABI_VERSION, "actual": actual_abi},
            )
        actual_vendor_schema = cls._decode_static_text(
            library.bt_huaxin_vendor_schema_id(), "vendor_schema_id"
        )
        actual_field_set = cls._decode_static_text(
            library.bt_huaxin_field_set_version(), "field_set_version"
        )
        bridge_manifest = manifest.get("bridge")
        expected_vendor_schema = (
            str(bridge_manifest.get("vendor_schema_id"))
            if isinstance(bridge_manifest, dict)
            else VENDOR_SCHEMA_ID
        )
        expected_field_set = (
            str(bridge_manifest.get("field_set_version"))
            if isinstance(bridge_manifest, dict)
            else FIELD_SET_VERSION
        )
        if actual_vendor_schema != expected_vendor_schema or actual_field_set != expected_field_set:
            raise HuaxinAbiError(
                VENDOR_SCHEMA_INCOMPATIBLE,
                "Python wrapper 与 native bridge schema 身份不一致",
                {
                    "expected_vendor_schema_id": expected_vendor_schema,
                    "actual_vendor_schema_id": actual_vendor_schema,
                    "expected_field_set_version": expected_field_set,
                    "actual_field_set_version": actual_field_set,
                },
            )
        return cls(artifact_path, library, manifest)

    @staticmethod
    def _decode_static_text(raw: Optional[bytes], field_name: str) -> str:
        """解码由 bridge 静态存储持有的非空 UTF-8 文本。

        Args:
            raw: ``c_char_p`` 返回的 bytes 或空指针对应的 None。
            field_name: 当前字段名，用于受控诊断。

        Returns:
            str: 解码后的文本。

        Raises:
            HuaxinAbiError: 指针为空或 bytes 不是合法 UTF-8。

        Side Effects:
            无；静态文本归 bridge 所有，Python 不释放。
        """

        if raw is None:
            raise HuaxinAbiError(
                NATIVE_ABI_INCOMPATIBLE,
                "native bridge 缺少必需静态身份文本",
                {"field": field_name},
            )
        try:
            return raw.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise HuaxinAbiError(
                NATIVE_ABI_INCOMPATIBLE,
                "native bridge 静态身份文本不是合法 UTF-8",
                {"field": field_name},
            ) from exc

    def abi_version(self) -> int:
        """
        读取已加载 bridge 的 ABI major。

        参数:
            无。
        返回:
            native bridge 报告的 ABI 整数。
        """

        return int(self._library.bt_huaxin_abi_version())

    def bridge_version(self) -> str:
        """
        读取自研 bridge 的非敏感版本标识。

        参数:
            无。
        返回:
            UTF-8 版本字符串；空指针时返回空字符串。
        """

        raw = self._library.bt_huaxin_bridge_version()
        return self._decode_static_text(raw, "bridge_version")

    def vendor_schema_id(self) -> str:
        """读取 bridge 明确声明的 vendor schema ID。

        Args:
            无。

        Returns:
            str: 与所有 v2 POD 共同使用的 schema ID。

        Side Effects:
            无；返回值来自 bridge 静态存储的 Python 副本。
        """

        return self._decode_static_text(
            self._library.bt_huaxin_vendor_schema_id(), "vendor_schema_id"
        )

    def field_set_version(self) -> str:
        """读取 bridge 明确声明的 field-set version。

        Args:
            无。

        Returns:
            str: 与所有 v2 POD 共同使用的字段集版本。

        Side Effects:
            无；返回值来自 bridge 静态存储的 Python 副本。
        """

        return self._decode_static_text(
            self._library.bt_huaxin_field_set_version(), "field_set_version"
        )

    def create(self, queue_capacity: int = 64) -> "NativeRuntime":
        """
        创建一个只含 fake/offline 有界队列的 opaque runtime。

        参数:
            queue_capacity: native 队列最大事件数，必须位于 2 到 1,000,000。
        返回:
            需要显式 close 或使用上下文管理器的 NativeRuntime。
        副作用:
            在当前进程 native 堆上分配一个 handle；不创建线程或网络连接。
        异常:
            ValueError: 队列容量越界。
            HuaxinNativeCallError: C ABI 创建失败。
        """

        if queue_capacity < 2 or queue_capacity > 1_000_000:
            raise ValueError("queue_capacity 必须位于 2 到 1,000,000")
        options = _CreateOptions(
            abi_version=ABI_VERSION,
            struct_size=ctypes.sizeof(_CreateOptions),
            queue_capacity=queue_capacity,
            reserved=0,
            schema=_schema_identity(
                self.expected_vendor_schema_id,
                self.expected_field_set_version,
            ),
        )
        handle = ctypes.c_void_p()
        result = int(self._library.bt_huaxin_create(ctypes.byref(options), ctypes.byref(handle)))
        self._raise_for_result(result, "create")
        if not handle.value:
            raise HuaxinNativeCallError(
                NATIVE_CALL_FAILED,
                "native create 返回成功但未提供 handle",
                {"operation": "create"},
            )
        return NativeRuntime(self, handle)

    def _raise_for_result(self, result: int, operation: str) -> None:
        """
        将 C ABI 非零返回码转换为稳定 Python 异常。

        参数:
            result: C ABI 返回的整数错误码。
            operation: 当前操作名称，仅用于脱敏诊断。
        返回:
            返回码为零时无返回值。
        异常:
            HuaxinAbiError: native 报告 ABI/struct size 不兼容。
            HuaxinNativeCallError: 其他受控 native 错误。
        """

        if result == 0:
            return
        raw_message = self._library.bt_huaxin_error_message(result)
        native_message = raw_message.decode("utf-8", errors="replace") if raw_message else "unknown"
        details = {
            "operation": operation,
            "native_code": result,
            "native_message": native_message,
        }
        if result in (
            NATIVE_RESULT_ABI_INCOMPATIBLE,
            NATIVE_RESULT_STRUCT_SIZE_INCOMPATIBLE,
        ):
            raise HuaxinAbiError(
                NATIVE_ABI_INCOMPATIBLE,
                "native C ABI 版本或结构大小不兼容",
                details,
            )
        if result == NATIVE_RESULT_SCHEMA_INCOMPATIBLE:
            raise HuaxinAbiError(
                VENDOR_SCHEMA_INCOMPATIBLE,
                "native vendor schema 或 field-set 不兼容",
                details,
            )
        raise HuaxinNativeCallError(
            NATIVE_CALL_FAILED,
            "native C ABI 调用失败",
            details,
        )


class NativeRuntime:
    """管理一个 opaque native handle，并提供同步 health 与有界批量 drain。"""

    def __init__(self, bridge: NativeBridge, handle: ctypes.c_void_p) -> None:
        """
        保存 bridge、opaque handle 和 Python 侧生命周期锁。

        参数:
            bridge: 创建当前 handle 的已加载 bridge。
            handle: C ABI 返回的非空 opaque 指针。
        返回:
            无返回值；初始化 runtime 对象。
        """

        self._bridge = bridge
        self._handle: Optional[ctypes.c_void_p] = handle
        self._lock = threading.RLock()

    def _require_handle(self) -> ctypes.c_void_p:
        """
        返回仍有效的 opaque handle。

        参数:
            无。
        返回:
            尚未关闭的 ctypes.c_void_p。
        异常:
            RuntimeError: runtime 已经关闭。
        """

        if self._handle is None or not self._handle.value:
            raise RuntimeError("NativeRuntime 已关闭")
        return self._handle

    def health(self) -> NativeHealth:
        """
        读取 runtime 的离线状态和有界队列水位。

        参数:
            无。
        返回:
            不含路径、账号或网络信息的 NativeHealth 快照。
        副作用:
            获取短时 Python 生命周期锁并调用同步 C ABI。
        """

        with self._lock:
            handle = self._require_handle()
            schema = _schema_identity(
                self._bridge.expected_vendor_schema_id,
                self._bridge.expected_field_set_version,
            )
            if self._bridge.mode == MODE_TRADER:
                trader_raw = _TraderHealth(
                    abi_version=ABI_VERSION,
                    struct_size=ctypes.sizeof(_TraderHealth),
                    schema=schema,
                )
                result = int(
                    self._bridge._library.bt_huaxin_get_trader_health(
                        handle, ctypes.byref(trader_raw)
                    )
                )
                self._bridge._raise_for_result(result, "trader_health")
                _validate_native_struct(
                    trader_raw,
                    _TraderHealth,
                    "trader_health",
                    self._bridge.expected_vendor_schema_id,
                    self._bridge.expected_field_set_version,
                )
                vendor_schema_id, field_set_version = _decode_schema_identity(
                    trader_raw.schema,
                    "trader_health",
                    self._bridge.expected_vendor_schema_id,
                    self._bridge.expected_field_set_version,
                )
                return NativeHealth(
                    state=int(trader_raw.state),
                    queue_capacity=int(trader_raw.queue_capacity),
                    queue_size=int(trader_raw.queue_size),
                    dropped_events=int(trader_raw.dropped_events),
                    vendor_schema_id=vendor_schema_id,
                    field_set_version=field_set_version,
                    transport_connected=bool(trader_raw.transport_connected),
                    logged_in=bool(trader_raw.logged_in),
                    ready_for_queries=bool(trader_raw.ready_for_queries),
                    ready_for_new_orders=bool(trader_raw.ready_for_new_orders),
                    ready_for_cancel=bool(trader_raw.ready_for_cancel),
                    session_epoch=int(trader_raw.session_epoch),
                    last_error_id=int(trader_raw.last_error_id),
                )
            raw = _Health(
                abi_version=ABI_VERSION,
                struct_size=ctypes.sizeof(_Health),
                state=0,
                queue_capacity=0,
                queue_size=0,
                reserved=0,
                dropped_events=0,
                schema=schema,
            )
            result = int(self._bridge._library.bt_huaxin_get_health(handle, ctypes.byref(raw)))
            self._bridge._raise_for_result(result, "health")
            _validate_native_struct(
                raw,
                _Health,
                "health",
                self._bridge.expected_vendor_schema_id,
                self._bridge.expected_field_set_version,
            )
            vendor_schema_id, field_set_version = _decode_schema_identity(
                raw.schema,
                "health",
                self._bridge.expected_vendor_schema_id,
                self._bridge.expected_field_set_version,
            )
            return NativeHealth(
                state=int(raw.state),
                queue_capacity=int(raw.queue_capacity),
                queue_size=int(raw.queue_size),
                dropped_events=int(raw.dropped_events),
                vendor_schema_id=vendor_schema_id,
                field_set_version=field_set_version,
            )

    def submit_request(
        self,
        request_id: int,
        payload: bytes = b"",
        request_type: int = REQUEST_TYPE_PING,
    ) -> None:
        """同步提交一个版本化 fake POD 请求。

        Args:
            request_id: 非零 uint64 请求标识，由调用方生成并用于关联回执。
            payload: 最多 192 字节的原始二进制负载，可包含 NUL。
            request_type: 稳定请求类型；当前离线合同只支持 ping。

        Returns:
            None。

        Raises:
            TypeError: payload 不是 bytes。
            ValueError: request_id 或 payload 越出固定合同范围。
            HuaxinNativeCallError: native 拒绝请求或队列已满。

        Side Effects:
            获取生命周期锁并让 native fake runtime 入队一个请求完成事件；不联网、不交易。
        """

        self._submit_payload(request_id, payload, request_type, "submit_request")

    def _submit_payload(
        self,
        request_id: int,
        payload: bytes,
        request_type: int,
        operation: str,
    ) -> None:
        """构造通用请求头并同步调用 native dispatcher。

        Args:
            request_id: 调用方稳定请求标识。
            payload: 与 request_type 对应的 typed bytes。
            request_type: flat ABI 请求整数。
            operation: 受控异常中的操作名。

        Returns:
            None。

        Raises:
            TypeError: payload 不是 bytes。
            ValueError: 标识或 payload 超出当前模式合同。
            HuaxinNativeCallError: native 或厂商同步拒绝请求。
        """

        if not isinstance(payload, bytes):
            raise TypeError("payload 必须为 bytes")
        max_request_id = (1 << 31) - 1 if self._bridge.mode == MODE_TRADER else (1 << 64) - 1
        if request_id < 1 or request_id > max_request_id:
            raise ValueError(f"request_id 必须位于 1 到 {max_request_id}")
        if len(payload) > REQUEST_PAYLOAD_CAPACITY:
            raise ValueError(f"payload 不能超过 {REQUEST_PAYLOAD_CAPACITY} 字节")
        raw = _Request(
            abi_version=ABI_VERSION,
            struct_size=ctypes.sizeof(_Request),
            request_type=request_type,
            payload_size=len(payload),
            request_id=request_id,
            schema=_schema_identity(
                self._bridge.expected_vendor_schema_id,
                self._bridge.expected_field_set_version,
            ),
        )
        raw.payload[: len(payload)] = payload
        with self._lock:
            handle = self._require_handle()
            result = int(self._bridge._library.bt_huaxin_submit_request(handle, ctypes.byref(raw)))
            if result != 0:
                print(
                    f"[DEBUG NATIVE] bt_huaxin_submit_request operation={operation} result={result} request_type={raw.request_type} payload_len={len(payload)}"
                )
            self._bridge._raise_for_result(result, operation)

    def start_session(self, config: NativeSessionConfig) -> None:
        """启动真实 TORA Trader 生命周期、连接和自动登录。

        Args:
            config: 明确包含 flow/front/身份和独立写门禁的会话配置。

        Returns:
            None；登录结果通过 health 与 owned events 异步观察。

        Raises:
            TypeError: config 类型不正确。
            HuaxinNativeCallError: fake 模式或 native 同步拒绝启动。

        Side Effects:
            真实模式创建 TORA API 线程并连接配置的交易前置。
        """

        if not isinstance(config, NativeSessionConfig):
            raise TypeError("config 必须为 NativeSessionConfig")
        raw = _session_config_to_raw(
            config,
            self._bridge.expected_vendor_schema_id,
            self._bridge.expected_field_set_version,
        )
        try:
            with self._lock:
                handle = self._require_handle()
                result = int(
                    self._bridge._library.bt_huaxin_start_session(handle, ctypes.byref(raw))
                )
                self._bridge._raise_for_result(result, "start_session")
        finally:
            _clear_structure(raw)

    def stop_session(self) -> None:
        """幂等停止 Trader 会话但保留 runtime 供最后 drain/health。

        Returns:
            None。

        Side Effects:
            真实模式注销 SPI 并调用厂商 Release；fake 模式为空操作。
        """

        with self._lock:
            handle = self._require_handle()
            result = int(self._bridge._library.bt_huaxin_stop_session(handle))
            self._bridge._raise_for_result(result, "stop_session")

    def query_security(self, request_id: int, exchange: str = "", security: str = "") -> None:
        """提交证券基础信息查询。

        Args:
            request_id: 正 int32 请求标识。
            exchange: 可选交易所过滤。
            security: 可选证券代码过滤。

        Returns:
            None；记录与 query_end 通过 drain 返回。
        """

        self._submit_payload(
            request_id,
            _query_payload(exchange, security),
            REQUEST_QUERY_SECURITY,
            "query_security",
        )

    def query_shareholder_accounts(self, request_id: int) -> None:
        """提交股东账户查询并通过 query_end 标记完成。

        Args:
            request_id: 正 int32 请求标识。

        Returns:
            None。
        """

        self._submit_payload(
            request_id, b"", REQUEST_QUERY_SHAREHOLDER_ACCOUNT, "query_shareholder_accounts"
        )

    def query_trading_accounts(self, request_id: int) -> None:
        """提交资金账户查询。

        Args:
            request_id: 正 int32 请求标识。

        Returns:
            None。
        """

        self._submit_payload(
            request_id, b"", REQUEST_QUERY_TRADING_ACCOUNT, "query_trading_accounts"
        )

    def query_positions(self, request_id: int) -> None:
        """提交持仓查询。

        Args:
            request_id: 正 int32 请求标识。

        Returns:
            None。
        """

        self._submit_payload(request_id, b"", REQUEST_QUERY_POSITION, "query_positions")

    def query_orders(self, request_id: int) -> None:
        """提交当日委托查询。

        Args:
            request_id: 正 int32 请求标识。

        Returns:
            None。
        """

        self._submit_payload(request_id, b"", REQUEST_QUERY_ORDER, "query_orders")

    def query_trades(self, request_id: int) -> None:
        """提交当日成交查询。

        Args:
            request_id: 正 int32 请求标识。

        Returns:
            None。
        """

        self._submit_payload(request_id, b"", REQUEST_QUERY_TRADE, "query_trades")

    def query_system_nodes(self, request_id: int, node_id: int = 0) -> None:
        """提交柜台系统节点目录查询。

        Args:
            request_id: 正 int32 请求标识。
            node_id: 零查询全部，正数查询指定节点。

        Returns:
            None；记录与 query_end 通过 drain 返回。
        """

        self._submit_payload(
            request_id,
            _system_node_query_payload(node_id),
            REQUEST_QUERY_SYSTEM_NODE,
            "query_system_nodes",
        )

    def query_fund_transfer_details(
        self,
        request_id: int,
        query: Optional[NativeFundTransferDetailQuery] = None,
    ) -> None:
        """提交资金划拨流水查询。

        Args:
            request_id: 正 int32 请求标识。
            query: 可选过滤条件；缺省查询当前账户可见流水。

        Returns:
            None；记录与 query_end 通过 drain 返回。
        """

        self._submit_payload(
            request_id,
            _fund_transfer_detail_query_payload(query or NativeFundTransferDetailQuery()),
            REQUEST_QUERY_FUND_TRANSFER_DETAIL,
            "query_fund_transfer_details",
        )

    def query_position_transfer_details(
        self,
        request_id: int,
        query: Optional[NativePositionTransferDetailQuery] = None,
    ) -> None:
        """提交证券划拨流水查询。

        Args:
            request_id: 正 int32 请求标识。
            query: 可选过滤条件；缺省查询当前账户可见流水。

        Returns:
            None；记录与 query_end 通过 drain 返回。
        """

        self._submit_payload(
            request_id,
            _position_transfer_detail_query_payload(query or NativePositionTransferDetailQuery()),
            REQUEST_QUERY_POSITION_TRANSFER_DETAIL,
            "query_position_transfer_details",
        )

    def place_limit(self, request_id: int, order: NativeLimitOrderRequest) -> None:
        """提交受 native 交易门禁保护的限价委托。

        Args:
            request_id: 正 int32 请求标识。
            order: 限价/GFD/AV 委托身份和价格数量。

        Returns:
            None；响应和最终状态通过 owned events 返回。
        """

        if not isinstance(order, NativeLimitOrderRequest):
            raise TypeError("order 必须为 NativeLimitOrderRequest")
        self._submit_payload(
            request_id,
            _limit_order_payload(order),
            REQUEST_PLACE_LIMIT,
            "place_limit",
        )

    def place_order(self, request_id: int, order: NativeOrderRequest) -> None:
        """提交经 Python/native 双层交易所矩阵门禁的现货委托。

        Args:
            request_id: 正 int32 请求标识。
            order: 使用 canonical 三元组和显式保护价的委托请求。

        Returns:
            None；响应和最终状态通过 owned events 返回。

        Raises:
            TypeError: order 不是 NativeOrderRequest。
            ValueError: 类型、交易所组合、价格或数量不满足安全合同。
        """

        if not isinstance(order, NativeOrderRequest):
            raise TypeError("order 必须为 NativeOrderRequest")
        self._submit_payload(
            request_id,
            _order_payload(order),
            REQUEST_PLACE_ORDER,
            "place_order",
        )

    def cancel_order(self, request_id: int, cancel: NativeCancelOrderRequest) -> None:
        """提交受独立撤单门禁保护的明确身份撤单。

        Args:
            request_id: 正 int32 请求标识，同时用作 OrderActionRef。
            cancel: OrderSysID 或完整会话三元组身份。

        Returns:
            None；响应和最终状态通过 owned events 返回。
        """

        if not isinstance(cancel, NativeCancelOrderRequest):
            raise TypeError("cancel 必须为 NativeCancelOrderRequest")
        self._submit_payload(
            request_id,
            _cancel_order_payload(cancel),
            REQUEST_CANCEL_ORDER,
            "cancel_order",
        )

    def transfer_fund(self, request_id: int, transfer: NativeTransferFundRequest) -> None:
        """提交默认关闭且不自动重试的跨节点资金划拨。

        Args:
            request_id: 正 int32 请求标识。
            transfer: 已持久化 ApplySerial 的资金动作。

        Returns:
            None；接受响应和最终回报通过 owned events 返回。
        """

        self._submit_payload(
            request_id,
            _transfer_fund_payload(transfer),
            REQUEST_TRANSFER_FUND,
            "transfer_fund",
        )

    def transfer_position(
        self,
        request_id: int,
        transfer: NativeTransferPositionRequest,
    ) -> None:
        """提交默认关闭且不自动重试的跨节点证券划拨。

        Args:
            request_id: 正 int32 请求标识。
            transfer: 已持久化 ApplySerial 和同行身份的证券动作。

        Returns:
            None；接受响应和最终回报通过 owned events 返回。
        """

        self._submit_payload(
            request_id,
            _transfer_position_payload(transfer),
            REQUEST_TRANSFER_POSITION,
            "transfer_position",
        )

    def drain(self, max_events: int) -> List[NativeEvent]:
        """
        从 native 队列最多复制指定数量的事件。

        参数:
            max_events: 本次最多返回的事件数，必须位于 1 到 4096。
        返回:
            Python 自有的 NativeEvent 列表，长度不超过 max_events。
        副作用:
            从 native 有界队列移除已复制事件。
        异常:
            ValueError: max_events 越界。
            HuaxinNativeCallError: native drain 失败。
        """

        if max_events < 1 or max_events > MAX_DRAIN_EVENTS:
            raise ValueError(f"max_events 必须位于 1 到 {MAX_DRAIN_EVENTS}")
        if self._bridge.mode == MODE_TRADER:
            return self._drain_owned(max_events)
        with self._lock:
            handle = self._require_handle()
            schema = _schema_identity(
                self._bridge.expected_vendor_schema_id,
                self._bridge.expected_field_set_version,
            )
            batch = _EventBatch(
                abi_version=ABI_VERSION,
                struct_size=ctypes.sizeof(_EventBatch),
                event_count=0,
                event_stride=0,
                schema=schema,
                events=None,
                ownership_token=0,
            )
            try:
                result = int(
                    self._bridge._library.bt_huaxin_drain_event_batch(
                        handle,
                        ctypes.c_uint32(max_events),
                        ctypes.byref(batch),
                    )
                )
                self._bridge._raise_for_result(result, "drain_event_batch")
                _validate_native_struct(
                    batch,
                    _EventBatch,
                    "drain_event_batch",
                    self._bridge.expected_vendor_schema_id,
                    self._bridge.expected_field_set_version,
                )
                count = int(batch.event_count)
                if count > max_events:
                    raise HuaxinAbiError(
                        NATIVE_ABI_INCOMPATIBLE,
                        "native batch 数量超过调用方上限",
                        {"operation": "drain_event_batch"},
                    )
                event_address = ctypes.cast(batch.events, ctypes.c_void_p).value
                ownership_token = int(batch.ownership_token)
                if count == 0:
                    if event_address or ownership_token:
                        raise HuaxinAbiError(
                            NATIVE_ABI_INCOMPATIBLE,
                            "native 空 batch 携带了所有权指针",
                            {"operation": "drain_event_batch"},
                        )
                    return []
                if (
                    int(batch.event_stride) != ctypes.sizeof(_Event)
                    or not event_address
                    or not ownership_token
                ):
                    raise HuaxinAbiError(
                        NATIVE_ABI_INCOMPATIBLE,
                        "native batch stride 或所有权描述符不兼容",
                        {"operation": "drain_event_batch"},
                    )

                events: List[NativeEvent] = []
                for index in range(count):
                    raw = batch.events[index]
                    _validate_native_struct(
                        raw,
                        _Event,
                        "drain_event",
                        self._bridge.expected_vendor_schema_id,
                        self._bridge.expected_field_set_version,
                    )
                    payload_size = int(raw.payload_size)
                    if payload_size > EVENT_PAYLOAD_CAPACITY:
                        raise HuaxinAbiError(
                            NATIVE_ABI_INCOMPATIBLE,
                            "native event payload 长度超过固定容量",
                            {"operation": "drain_event"},
                        )
                    vendor_schema_id, field_set_version = _decode_schema_identity(
                        raw.schema,
                        "drain_event",
                        self._bridge.expected_vendor_schema_id,
                        self._bridge.expected_field_set_version,
                    )
                    events.append(
                        NativeEvent(
                            event_type=int(raw.event_type),
                            sequence=int(raw.sequence),
                            received_ns=int(raw.received_ns),
                            request_id=int(raw.request_id),
                            vendor_schema_id=vendor_schema_id,
                            field_set_version=field_set_version,
                            payload=bytes(raw.payload[:payload_size]),
                        )
                    )
                return events
            finally:
                cleanup_batch = _EventBatch(
                    abi_version=ABI_VERSION,
                    struct_size=ctypes.sizeof(_EventBatch),
                    event_count=int(batch.event_count),
                    event_stride=int(batch.event_stride),
                    schema=schema,
                    events=batch.events,
                    ownership_token=int(batch.ownership_token),
                )
                free_result = int(
                    self._bridge._library.bt_huaxin_free_event_batch(ctypes.byref(cleanup_batch))
                )
                if free_result != 0:
                    self._bridge._raise_for_result(free_result, "free_event_batch")

    def _drain_owned(self, max_events: int) -> List[NativeEvent]:
        """复制并释放真实 Trader 的 bridge-owned 大事件批次。

        Args:
            max_events: 本次最多复制的事件数，已由公开 drain 校验。

        Returns:
            List[NativeEvent]: payload 和结构化 data 均由 Python 自有的事件列表。

        Raises:
            HuaxinAbiError: batch、event 或 typed payload 不兼容。
            HuaxinNativeCallError: native drain/free 返回非零错误。

        Side Effects:
            从 native 有界队列移除事件，并在 finally 中恰好释放一次 batch。
        """

        with self._lock:
            handle = self._require_handle()
            schema = _schema_identity(
                self._bridge.expected_vendor_schema_id,
                self._bridge.expected_field_set_version,
            )
            batch = _OwnedEventBatch(
                abi_version=ABI_VERSION,
                struct_size=ctypes.sizeof(_OwnedEventBatch),
                event_count=0,
                event_stride=0,
                schema=schema,
                events=None,
                ownership_token=0,
            )
            try:
                result = int(
                    self._bridge._library.bt_huaxin_drain_owned_event_batch(
                        handle,
                        ctypes.c_uint32(max_events),
                        ctypes.byref(batch),
                    )
                )
                self._bridge._raise_for_result(result, "drain_owned_event_batch")
                _validate_native_struct(
                    batch,
                    _OwnedEventBatch,
                    "drain_owned_event_batch",
                    self._bridge.expected_vendor_schema_id,
                    self._bridge.expected_field_set_version,
                )
                count = int(batch.event_count)
                if count > max_events:
                    raise HuaxinAbiError(
                        NATIVE_ABI_INCOMPATIBLE,
                        "native owned batch 数量超过调用方上限",
                        {"operation": "drain_owned_event_batch"},
                    )
                event_address = ctypes.cast(batch.events, ctypes.c_void_p).value
                ownership_token = int(batch.ownership_token)
                if count == 0:
                    if event_address or ownership_token:
                        raise HuaxinAbiError(
                            NATIVE_ABI_INCOMPATIBLE,
                            "native owned 空 batch 携带了所有权指针",
                            {"operation": "drain_owned_event_batch"},
                        )
                    return []
                if (
                    int(batch.event_stride) != ctypes.sizeof(_OwnedEvent)
                    or not event_address
                    or not ownership_token
                ):
                    raise HuaxinAbiError(
                        NATIVE_ABI_INCOMPATIBLE,
                        "native owned batch stride 或所有权描述符不兼容",
                        {"operation": "drain_owned_event_batch"},
                    )
                events: List[NativeEvent] = []
                for index in range(count):
                    raw = batch.events[index]
                    _validate_native_struct(
                        raw,
                        _OwnedEvent,
                        "drain_owned_event",
                        self._bridge.expected_vendor_schema_id,
                        self._bridge.expected_field_set_version,
                    )
                    payload_size = int(raw.payload_size)
                    if payload_size > OWNED_EVENT_PAYLOAD_CAPACITY:
                        raise HuaxinAbiError(
                            NATIVE_ABI_INCOMPATIBLE,
                            "native owned event payload 长度超过固定容量",
                            {"operation": "drain_owned_event"},
                        )
                    vendor_schema_id, field_set_version = _decode_schema_identity(
                        raw.schema,
                        "drain_owned_event",
                        self._bridge.expected_vendor_schema_id,
                        self._bridge.expected_field_set_version,
                    )
                    payload = bytes(raw.payload[:payload_size])
                    events.append(
                        NativeEvent(
                            event_type=int(raw.event_type),
                            sequence=int(raw.sequence),
                            received_ns=int(raw.received_ns),
                            request_id=int(raw.request_id),
                            vendor_schema_id=vendor_schema_id,
                            field_set_version=field_set_version,
                            payload=payload,
                            data=_decode_event_data(int(raw.event_type), payload),
                        )
                    )
                return events
            finally:
                cleanup_batch = _OwnedEventBatch(
                    abi_version=ABI_VERSION,
                    struct_size=ctypes.sizeof(_OwnedEventBatch),
                    event_count=int(batch.event_count),
                    event_stride=int(batch.event_stride),
                    schema=schema,
                    events=batch.events,
                    ownership_token=int(batch.ownership_token),
                )
                free_result = int(
                    self._bridge._library.bt_huaxin_free_owned_event_batch(
                        ctypes.byref(cleanup_batch)
                    )
                )
                if free_result != 0:
                    self._bridge._raise_for_result(free_result, "free_owned_event_batch")

    def close(self) -> None:
        """
        幂等销毁 opaque native handle。

        参数:
            无。
        返回:
            无返回值。
        副作用:
            释放 native 堆对象，并使后续 health/drain 受控失败。
        """

        with self._lock:
            if self._handle is None:
                return
            handle = self._handle
            self._handle = None
            result = int(self._bridge._library.bt_huaxin_destroy(handle))
            self._bridge._raise_for_result(result, "destroy")

    def __enter__(self) -> "NativeRuntime":
        """
        进入 runtime 上下文。

        参数:
            无。
        返回:
            当前 NativeRuntime。
        """

        self._require_handle()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[Any],
    ) -> None:
        """
        离开上下文并确保 native handle 被释放。

        参数:
            exc_type: 上下文内异常类型或 None。
            exc_value: 上下文内异常实例或 None。
            traceback: 上下文内异常 traceback 或 None。
        返回:
            无返回值，不吞掉上下文内异常。
        副作用:
            调用 close 释放 native handle。
        """

        self.close()
