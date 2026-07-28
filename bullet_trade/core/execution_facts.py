"""
BulletTrade 回测机器事实 V1 的规范化、流式写入和原子发布基础层。

作者: BruceLee
文件职责: 定义 execution-facts/v1 envelope、稳定身份、严格校验、NDJSON staging 与 manifest。
主要输入: 回测权威事件、固定运行 UUID、精确数值、aware 业务时间和受控发布元数据。
主要输出: 不可覆盖的 facts.ndjson、manifest.json、complete guard 及 consumer 复验摘要。
上下游关系: 上游是未来回测引擎事件 sink，下游是只认成功 manifest 的机器事实 consumer。
关键约定: 本模块只使用标准库和包版本元数据，不访问业务数据库、网络或券商生产写路径；
trusted root 及父目录必须由调用者独占，同一 run 由上层串行发布，成功目录随后保持不可变。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
import weakref
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, localcontext
from enum import Enum
from pathlib import Path
from typing import Any, DefaultDict, Dict, List, Mapping, NoReturn, Optional, Set, Tuple, Union
from uuid import UUID, uuid5

from .models import OrderStatus, OrderStyle
from .price_basis import EffectivePriceBasis

SCHEMA_VERSION = "execution-facts/v1"
FACTS_PARTIAL_FILENAME = "facts.ndjson.partial"
FACTS_FILENAME = "facts.ndjson"
MANIFEST_PARTIAL_FILENAME = "manifest.json.partial"
MANIFEST_FILENAME = "manifest.json"
PUBLISH_INCOMPLETE_FILENAME = ".publish.incomplete"
PUBLISH_COMPLETE_FILENAME = ".publish.complete"
PUBLISH_PROTOCOL_VERSION = "execution-facts-publish/v1"

_SOURCE_EVENT_NAMESPACE = UUID("f84ace76-98dc-5d82-b208-c7842fd7fc9d")
_SHANGHAI_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")
_CANONICAL_SECURITY_PATTERN = re.compile(
    r"^[A-Z0-9]+(?:-[A-Z0-9]+)*\.(?:XSHG|XSHE|BSE|XSGE|CCFX|XDCE|XZCE|XINE)$"
)
_SECURITY_INPUT_PATTERN = re.compile(r"^([A-Z0-9]+(?:-[A-Z0-9]+)*)\.([A-Z0-9]+)$")
_SECURITY_SUFFIX_ALIASES = {
    "SH": "XSHG",
    "XSHG": "XSHG",
    "SZ": "XSHE",
    "XSHE": "XSHE",
    "BJ": "BSE",
    "BSE": "BSE",
    "SHFE": "XSGE",
    "XSHF": "XSGE",
    "XSGE": "XSGE",
    "CFFEX": "CCFX",
    "CCFX": "CCFX",
    "DCE": "XDCE",
    "XDCE": "XDCE",
    "CZCE": "XZCE",
    "XZCE": "XZCE",
    "INE": "XINE",
    "XINE": "XINE",
}
_CANONICAL_UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_DECIMAL_PATTERN = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_SAFE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_.:@-]{1,256}$")
_CONTRACT_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9_.:@-]+/[A-Za-z0-9_.:@-]+$")
_CONTROL_CHARACTER_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_SECRET_PATTERNS = (
    re.compile(r"(?i)\b(?:BT_)?(?:SECRET|CREDENTIAL)_CANARY_[A-Za-z0-9_.:/+-]+"),
    re.compile(
        r"""(?i)(?<![A-Za-z0-9_])["']?Authorization["']?\s*[:=]\s*"""
        r"""(?:"Bearer\s+[^"\r\n]*"|'Bearer\s+[^'\r\n]*'|"""
        r"""Bearer\s+(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\s,;}\]]+))"""
    ),
    re.compile(
        r"""(?i)(?<![A-Za-z0-9_])["']?"""
        r"(?:BT_TOKEN|JQDATA_PASSWORD|JQDATA_TOKEN|MYSQL_PASSWORD|REDIS_PASSWORD)"
        r"""["']?"""
        r"""\s*[:=]\s*(?:'[^'\r\n]*'|"[^"\r\n]*"|[^\s,;]+)"""
    ),
    re.compile(
        r"""(?i)(?<![A-Za-z0-9_])["']?"""
        r"(?:secret|password|passwd|api[_-]?key|authorization|cookie|dsn|token)"
        r"""["']?\s*[:=]\s*(?:'[^'\r\n]*'|"[^"\r\n]*"|[^\s,;}\]]+)"""
    ),
    re.compile(r"(?i)\b(?:mysql|redis|rediss)(?:\+[a-z0-9_]+)?://[^\s/:@]*:[^\s/@]+@[^\s,;]+"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b"),
)
MAX_DECIMAL_DIGITS = 38
MAX_DECIMAL_SCALE = 18
MAX_DECIMAL_INTEGER_DIGITS = 38
MAX_DECIMAL_INPUT_CHARS = 256
MAX_DECIMAL_EXPONENT_ABS = 38
MAX_DECIMAL_CANONICAL_CHARS = 58
MAX_FACT_LINE_BYTES = 64 * 1024
MAX_FACTS_FILE_BYTES = 2 * 1024 * 1024 * 1024
MAX_MANIFEST_FILE_BYTES = 1024 * 1024
MAX_WRITER_BUFFER_BYTES = 8 * 1024 * 1024
V1_ORDER_STATUS_VALUES = {
    "new",
    "open",
    "filling",
    "partly_canceled",
    "canceling",
    "filled",
    "canceled",
    "rejected",
    "held",
}
V1_ORDER_TYPE_VALUES = {"market", "limit"}
_EVENT_ID_BYTES = 16
_EVENT_ID_MEMORY_LIMIT = 8192
_EVENT_ID_BUCKET_BUFFER_BYTES = 4096
_EVENT_ID_BUCKET_READ_RECORDS = 4096


class EventType(str, Enum):
    """execution-facts/v1 固定事件类型。

    枚举用于 writer 和 validator 的 fail-closed 判断；新增必填事件语义需要升级 schema。
    """

    ORDER_INTENT = "order_intent"
    ORDER_EVENT = "order_event"
    FILL = "fill"
    FEE = "fee"
    RESERVATION = "reservation"
    CASH_LEDGER = "cash_ledger"
    ACCOUNT_DAILY = "account_daily"
    POSITION_DAILY = "position_daily"
    DAILY_PERFORMANCE = "daily_performance"
    RUN_METRIC = "run_metric"
    RECONCILE_EVENT = "reconcile_event"


class FeeType(str, Enum):
    """execution-facts/v1 固定费用分类。

    无法细分但确实发生的费用必须使用 ``other`` 并在 payload 中保留脱敏来源。
    """

    COMMISSION = "commission"
    STAMP_TAX = "stamp_tax"
    TRANSFER_FEE = "transfer_fee"
    ORDER_FLOW = "order_flow"
    CANCEL_FLOW = "cancel_flow"
    SLIPPAGE = "slippage"
    OTHER = "other"


class ExecutionFactsError(RuntimeError):
    """机器事实基础异常。

    所有 writer、校验、完整性和安全异常均继承此类，便于上层统一失败关闭。
    """


class ExecutionFactsValidationError(ExecutionFactsError, ValueError):
    """输入或 schema 不符合 execution-facts/v1 合同。"""


class ExecutionFactsIntegrityError(ExecutionFactsError):
    """已写文件的 sequence、身份、行数、字节数或 SHA 完整性失败。"""


class ExecutionFactsConflictError(ExecutionFactsError, FileExistsError):
    """目标目录已有 partial、final 或 manifest，writer 拒绝覆盖。"""


class ExecutionFactsAlreadyPublishedError(ExecutionFactsConflictError):
    """同一运行已有完整且可验证的发布，writer 以只读冲突返回。"""


class ExecutionFactsSecurityError(ExecutionFactsError, PermissionError):
    """固定 capability 绑定前或读取期间的路径、类型、secret 安全边界失败。

    该错误不表示 writer 持有 pathname 生命周期锁；同权限写者在线性化点之后修改由部署方
    独占 trusted root、串行 run 和成功目录不可变的前提约束，后续 consumer 重新打开时复验。
    """


@dataclass(frozen=True)
class FactsFileSummary:
    """一次 NDJSON 流式校验得到的不可变摘要。

    Attributes:
        record_count: 校验通过的事实行数。
        byte_size: 文件原始字节数。
        sha256: 文件原始字节的十六进制 SHA-256。
        first_sequence: 首条事实序号；空文件时为 None。
        last_sequence: 末条事实序号；空文件时为 None。
        event_counts: 各事件类型的行数。
    """

    record_count: int
    byte_size: int
    sha256: str
    first_sequence: Optional[int]
    last_sequence: Optional[int]
    event_counts: Dict[str, int]


@dataclass(frozen=True)
class QualityReport:
    """内部业务校验器生成的不可变发布质量报告。

    调用方不能传入 ``status``；只有完整 validator 无发现地返回实例时，报告才具备
    ``PASSED`` 语义。facts SHA、事件计数和业务日范围共同防止旧报告被另一份 staging 复用。

    Attributes:
        run_id: 报告绑定的 canonical 运行 UUID。
        facts_sha256: 报告绑定的 facts 原始 SHA-256。
        event_counts: 按固定事件类型排序的计数二元组。
        business_date_start: 首个被验证业务日。
        business_date_end: 最后一个被验证业务日。
        account_day_count: 通过资产恒等式的账户日数量。
        reconciliation_count: 通过的 reconcile_event 数量。
    """

    run_id: str
    facts_sha256: str
    event_counts: Tuple[Tuple[str, int], ...]
    business_date_start: str
    business_date_end: str
    account_day_count: int
    reconciliation_count: int

    @property
    def status(self) -> str:
        """返回内部 validator 独占的成功状态。

        Returns:
            str: 固定 ``PASSED``；失败校验不会构造 QualityReport。
        """

        return "PASSED"

    def as_manifest_dict(self) -> Dict[str, Any]:
        """生成可审计的 manifest quality object。

        Returns:
            Dict[str, Any]: 包含 SHA 绑定、业务日和 legacy 对账证据的固定结构。
        """

        return {
            "status": self.status,
            "checks_version": "execution-facts-quality/v1",
            "event_counts": dict(self.event_counts),
            "legacy_reconciliation": {
                "status": "PASSED",
                "version": "execution-facts-reconcile/v1",
            },
            "audit": {
                "facts_sha256": self.facts_sha256,
                "business_date_start": self.business_date_start,
                "business_date_end": self.business_date_end,
                "account_day_count": self.account_day_count,
                "reconciliation_count": self.reconciliation_count,
            },
        }


_FIELD_KINDS: Dict[str, str] = {
    "account_id": "identifier",
    "action": "reservation_action",
    "actual": "decimal",
    "adjustments": "decimal",
    "after_status": "order_status",
    "amount": "decimal",
    "available_cash": "decimal",
    "available_quantity": "decimal",
    "average_cost": "decimal",
    "balance": "decimal",
    "before_status": "order_status",
    "benchmark_return": "decimal",
    "cash": "decimal",
    "category": "ledger_category",
    "cumulative_return": "decimal",
    "daily_return": "decimal",
    "difference": "decimal",
    "drawdown": "decimal",
    "entry_id": "identifier",
    "expected": "decimal",
    "excess_return": "decimal",
    "fee_type": "fee_type",
    "fill_id": "identifier",
    "filled_quantity": "decimal",
    "gross_return": "decimal",
    "locked_cash": "decimal",
    "market_value": "decimal",
    "metric_name": "identifier",
    "metric_value": "decimal",
    "note": "text",
    "order_id": "identifier",
    "net_asset_value": "decimal",
    "net_return": "decimal",
    "order_type": "order_type",
    "positions_value": "decimal",
    "price": "decimal",
    "quantity": "decimal",
    "rate": "decimal",
    "reason": "text",
    "remaining_quantity": "decimal",
    "requested_price": "decimal",
    "requested_quantity": "decimal",
    "reservation_id": "identifier",
    "resource_type": "resource_type",
    "security": "security",
    "side": "side",
    "source": "text",
    "status": "reconcile_status",
    "strategy_return": "decimal",
    "total_value": "decimal",
    "unit": "identifier",
    "value": "decimal",
}

_PAYLOAD_RULES: Dict[EventType, Tuple[Set[str], Set[str]]] = {
    EventType.ORDER_INTENT: (
        {"order_id", "security", "side", "requested_quantity", "order_type"},
        {"requested_price", "account_id", "note"},
    ),
    EventType.ORDER_EVENT: (
        {
            "order_id",
            "before_status",
            "after_status",
            "requested_quantity",
            "filled_quantity",
            "remaining_quantity",
        },
        {"security", "account_id", "reason"},
    ),
    EventType.FILL: (
        {"order_id", "fill_id", "security", "side", "quantity", "price", "amount"},
        {"account_id"},
    ),
    EventType.FEE: (
        {"order_id", "fee_type", "amount", "source"},
        {"fill_id", "security", "rate", "reason"},
    ),
    EventType.RESERVATION: (
        {"reservation_id", "order_id", "resource_type", "action", "value", "unit"},
        {"account_id", "security", "reason"},
    ),
    EventType.CASH_LEDGER: (
        {"entry_id", "account_id", "category", "amount", "balance", "source"},
        {"order_id", "fill_id", "fee_type", "security", "reason"},
    ),
    EventType.ACCOUNT_DAILY: (
        {
            "account_id",
            "cash",
            "available_cash",
            "locked_cash",
            "positions_value",
            "total_value",
        },
        {"adjustments"},
    ),
    EventType.POSITION_DAILY: (
        {
            "account_id",
            "security",
            "quantity",
            "available_quantity",
            "average_cost",
            "price",
            "market_value",
        },
        set(),
    ),
    EventType.DAILY_PERFORMANCE: (
        {
            "account_id",
            "total_value",
            "net_asset_value",
            "daily_return",
            "cumulative_return",
            "strategy_return",
            "drawdown",
        },
        {"benchmark_return", "excess_return", "gross_return", "net_return"},
    ),
    EventType.RUN_METRIC: (
        {"metric_name", "metric_value"},
        {"unit", "source"},
    ),
    EventType.RECONCILE_EVENT: (
        {"metric_name", "status", "expected", "actual", "difference"},
        {"reason", "source"},
    ),
}

_ENUM_VALUES: Dict[str, Set[str]] = {
    "fee_type": {item.value for item in FeeType},
    "ledger_category": {
        "principal",
        "fee",
        "corporate_action",
        "adjustment",
        "deposit",
        "withdrawal",
    },
    "order_status": V1_ORDER_STATUS_VALUES,
    "order_type": V1_ORDER_TYPE_VALUES,
    "reconcile_status": {"PASSED", "FAILED"},
    "reservation_action": {"created", "consumed", "released", "adjusted"},
    "resource_type": {"cash", "position"},
    "side": {"BUY", "SELL"},
}


def _field_json_schema(kind: str) -> Dict[str, Any]:
    """根据内部字段类型生成 JSON Schema 片段。

    Args:
        kind: ``_FIELD_KINDS`` 中的字段类型。

    Returns:
        Dict[str, Any]: 不允许模糊数值或未知枚举的 schema 片段。

    Raises:
        ExecutionFactsValidationError: 字段类型未注册。
    """

    if kind == "decimal":
        return {
            "type": "string",
            "pattern": _DECIMAL_PATTERN.pattern,
            "maxLength": MAX_DECIMAL_CANONICAL_CHARS,
        }
    if kind == "security":
        return {"type": "string", "pattern": _CANONICAL_SECURITY_PATTERN.pattern}
    if kind == "identifier":
        return {"type": "string", "pattern": _SAFE_IDENTIFIER_PATTERN.pattern}
    if kind == "text":
        return {"type": "string", "maxLength": 4096}
    if kind in _ENUM_VALUES:
        return {"type": "string", "enum": sorted(_ENUM_VALUES[kind])}
    raise ExecutionFactsValidationError("未注册的 payload 字段类型: {0}".format(kind))


def _payload_json_schema(event_type: EventType) -> Dict[str, Any]:
    """生成指定事件的白名单 payload JSON Schema。

    Args:
        event_type: V1 固定事件类型。

    Returns:
        Dict[str, Any]: 带 required 和 additionalProperties=false 的 schema。
    """

    required, optional = _PAYLOAD_RULES[event_type]
    fields = sorted(required | optional)
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": sorted(required),
        "properties": {name: _field_json_schema(_FIELD_KINDS[name]) for name in fields},
    }
    if event_type is EventType.ORDER_EVENT:
        schema["allOf"] = [
            {
                "if": {
                    "properties": {
                        "after_status": {
                            "enum": [
                                OrderStatus.canceled.value,
                                OrderStatus.partly_canceled.value,
                                OrderStatus.rejected.value,
                            ]
                        }
                    },
                    "required": ["after_status"],
                },
                "then": {
                    "required": ["reason"],
                    "properties": {"reason": {"type": "string", "pattern": r".*\S.*"}},
                },
            }
        ]
    return schema


def _build_execution_facts_json_schema() -> Dict[str, Any]:
    """构造冻结的 execution-facts/v1 envelope JSON Schema。

    Returns:
        Dict[str, Any]: 可序列化、可发布且不依赖第三方校验库的 schema 字典。
    """

    required = [
        "schema_version",
        "run_id",
        "source_event_id",
        "sequence",
        "event_type",
        "occurred_at",
        "trade_date",
        "payload",
    ]
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://bullettrade.cn/schemas/execution-facts-v1.schema.json",
        "title": "BulletTrade execution facts V1",
        "type": "object",
        "additionalProperties": False,
        "required": required,
        "properties": {
            "schema_version": {"const": SCHEMA_VERSION},
            "run_id": {"type": "string", "format": "uuid"},
            "source_event_id": {"type": "string", "format": "uuid"},
            "sequence": {"type": "integer", "minimum": 1},
            "event_type": {"type": "string", "enum": [item.value for item in EventType]},
            "occurred_at": {"type": "string", "format": "date-time"},
            "trade_date": {"type": "string", "format": "date"},
            "payload": {"type": "object"},
        },
        "oneOf": [
            {
                "properties": {
                    "event_type": {"const": event_type.value},
                    "payload": _payload_json_schema(event_type),
                }
            }
            for event_type in EventType
        ],
    }


EXECUTION_FACTS_V1_JSON_SCHEMA = _build_execution_facts_json_schema()

EXECUTION_FACTS_V1_COMPATIBILITY = {
    "unknown_major": "reject",
    "unknown_event_type": "reject",
    "unknown_payload_field": "reject",
    "authority": "complete_guard_manifest_and_facts",
    "legacy_complete_contract": False,
}


def _coerce_event_type(value: Any) -> EventType:
    """把调用方事件类型转换为固定枚举。

    Args:
        value: EventType 或其字符串值。

    Returns:
        EventType: 合同内事件类型。

    Raises:
        ExecutionFactsValidationError: 未知事件类型。
    """

    try:
        return value if isinstance(value, EventType) else EventType(value)
    except (TypeError, ValueError) as exc:
        raise ExecutionFactsValidationError("未知 execution-facts/v1 事件类型") from exc


def validate_run_id(value: Any) -> str:
    """严格校验 canonical 小写带连字符 UUID 运行身份。

    Args:
        value: 外部或内部生成的 run ID。

    Returns:
        str: 与输入相同的 canonical UUID 文本。

    Raises:
        ExecutionFactsValidationError: 类型、长度、字符或 UUID 格式不合法。
    """

    if not isinstance(value, str) or not _CANONICAL_UUID_PATTERN.fullmatch(value):
        raise ExecutionFactsValidationError("run_id 必须是 canonical 小写带连字符 UUID")
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise ExecutionFactsValidationError("run_id 不是有效 UUID") from exc
    if str(parsed) != value:
        raise ExecutionFactsValidationError("run_id 必须使用 canonical UUID 文本")
    return value


def decimal_to_text(value: Union[Decimal, int, str]) -> str:
    """把精确数值转成无指数、无多余零的 Decimal 文本。

    Args:
        value: Decimal、整数或可精确解析的十进制字符串。

    Returns:
        str: execution-facts/v1 规范 Decimal 文本。

    Raises:
        ExecutionFactsValidationError: bool、float、空值、NaN、Infinity 或非法文本。
    """

    if (
        isinstance(value, bool)
        or isinstance(value, float)
        or not isinstance(value, (Decimal, int, str))
    ):
        raise ExecutionFactsValidationError("权威数值只接受 Decimal、整数或十进制字符串")
    if isinstance(value, str):
        if not value.strip():
            raise ExecutionFactsValidationError("Decimal 文本不能为空")
        if len(value) > MAX_DECIMAL_INPUT_CHARS:
            raise ExecutionFactsValidationError("Decimal 输入文本超过冻结长度预算")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ExecutionFactsValidationError("无法解析精确 Decimal 数值") from exc
    if not number.is_finite():
        raise ExecutionFactsValidationError("Decimal 数值禁止 NaN 或 Infinity")
    sign, original_digits, exponent = number.as_tuple()
    if not isinstance(exponent, int) or abs(exponent) > MAX_DECIMAL_EXPONENT_ABS:
        raise ExecutionFactsValidationError("Decimal 指数超过 V1 上限")
    if not any(original_digits):
        return "0"
    digits = list(original_digits)
    while len(digits) > 1 and digits[-1] == 0:
        digits.pop()
        exponent += 1
    significant_digits = len(digits)
    integer_digits = max(1, significant_digits + exponent)
    scale = max(0, -exponent)
    if significant_digits > MAX_DECIMAL_DIGITS:
        raise ExecutionFactsValidationError("Decimal 有效位数超过 V1 上限")
    if integer_digits > MAX_DECIMAL_INTEGER_DIGITS:
        raise ExecutionFactsValidationError("Decimal 整数位数超过 V1 上限")
    if scale > MAX_DECIMAL_SCALE:
        raise ExecutionFactsValidationError("Decimal 小数位数超过 V1 上限")
    digit_text = "".join(str(digit) for digit in digits)
    if exponent >= 0:
        text = digit_text + ("0" * exponent)
    else:
        point = len(digit_text) + exponent
        if point > 0:
            text = digit_text[:point] + "." + digit_text[point:]
        else:
            text = "0." + ("0" * (-point)) + digit_text
    if sign:
        text = "-" + text
    if not _DECIMAL_PATTERN.fullmatch(text):
        raise ExecutionFactsValidationError("Decimal 规范化结果不符合 V1 合同: {0!r}".format(text))
    return text


def normalize_utc_datetime(value: datetime) -> Tuple[str, str]:
    """把 aware 时间转换为 UTC 文本并派生 Asia/Shanghai 业务日。

    Args:
        value: 带有效 ``utcoffset`` 的 datetime。

    Returns:
        Tuple[str, str]: ``(UTC ISO-8601, trade_date)``。

    Raises:
        ExecutionFactsValidationError: 输入不是 datetime 或缺少有效时区。
    """

    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ExecutionFactsValidationError("occurred_at 必须是 aware datetime")
    utc_value = value.astimezone(timezone.utc)
    occurred_at = utc_value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    trade_date = value.astimezone(_SHANGHAI_TIMEZONE).date().isoformat()
    return occurred_at, trade_date


def validate_canonical_security_code(value: str) -> str:
    """把证券代码别名收敛为唯一 BulletTrade canonical 后缀格式。

    Args:
        value: 待验证证券或合约代码。

    Returns:
        str: 唯一 canonical 证券或合约代码。

    Raises:
        ExecutionFactsValidationError: 缺失后缀、大小写、期权语法或市场后缀不受支持。
    """

    if not isinstance(value, str):
        raise ExecutionFactsValidationError("证券代码必须是文本")
    matched = _SECURITY_INPUT_PATTERN.fullmatch(value)
    if matched is None:
        raise ExecutionFactsValidationError("证券代码必须使用大写代码和受支持市场后缀")
    symbol, suffix = matched.groups()
    canonical_suffix = _SECURITY_SUFFIX_ALIASES.get(suffix)
    if canonical_suffix is None:
        raise ExecutionFactsValidationError("证券代码市场后缀不受支持")
    canonical = "{0}.{1}".format(symbol, canonical_suffix)
    if not _CANONICAL_SECURITY_PATTERN.fullmatch(canonical):
        raise ExecutionFactsValidationError("证券代码无法规范化为 V1 canonical 身份")
    return canonical


def build_source_event_id(
    run_id: str,
    event_type: Union[EventType, str],
    authority_id: str,
    state_version: Union[int, str],
    sequence: int,
) -> str:
    """用运行、事件、权威对象、状态版本和序号生成稳定 UUIDv5。

    Args:
        run_id: canonical 运行 UUID。
        event_type: 固定 V1 事件类型。
        authority_id: 引擎权威对象身份，仅参与哈希，不原样持久化。
        state_version: 权威对象状态版本。
        sequence: 当前运行内严格递增序号。

    Returns:
        str: canonical 小写 UUIDv5 source event ID。

    Raises:
        ExecutionFactsValidationError: 任一身份输入不合法。
    """

    normalized_run_id = validate_run_id(run_id)
    normalized_event_type = _coerce_event_type(event_type)
    if not isinstance(authority_id, str) or not authority_id or len(authority_id) > 1024:
        raise ExecutionFactsValidationError("authority_id 必须是 1 到 1024 字符的非空文本")
    if _CONTROL_CHARACTER_PATTERN.search(authority_id):
        raise ExecutionFactsValidationError("authority_id 禁止控制字符")
    if isinstance(state_version, bool) or not isinstance(state_version, (int, str)):
        raise ExecutionFactsValidationError("state_version 必须是整数或文本")
    if isinstance(state_version, str) and (not state_version or len(state_version) > 256):
        raise ExecutionFactsValidationError("state_version 文本必须是 1 到 256 字符")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        raise ExecutionFactsValidationError("sequence 必须是从 1 开始的整数")
    name = "\x1f".join(
        (
            normalized_run_id,
            normalized_event_type.value,
            authority_id,
            str(state_version),
            str(sequence),
        )
    )
    return str(uuid5(_SOURCE_EVENT_NAMESPACE, name))


def _contains_secret(value: str) -> bool:
    """判断文本是否命中受控 canary 或常见秘密赋值模式。

    Args:
        value: 待扫描文本。

    Returns:
        bool: 命中时为 True。
    """

    return any(pattern.search(value) for pattern in _SECRET_PATTERNS)


def redact_sensitive_text(value: Any) -> str:
    """统一脱敏普通文本或异常 ``repr`` 中的秘密。

    Args:
        value: 文本、异常或可安全转成 ``repr`` 的诊断对象。

    Returns:
        str: Authorization、环境变量、DSN、API key 与 canary 均已替换的文本。

    Side Effects:
        无；不会记录或输出原始值。
    """

    text = value if isinstance(value, str) else repr(value)
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub("[REDACTED]", text)
    return text


def _redact_text(value: str, field_name: str) -> str:
    """校验普通诊断文本并脱敏受控秘密模式。

    Args:
        value: payload 中允许持久化的文本。
        field_name: 用于错误定位的字段名。

    Returns:
        str: 最多 4096 字符且秘密片段已替换的文本。

    Raises:
        ExecutionFactsValidationError: 类型、长度或控制字符不合法。
    """

    if not isinstance(value, str):
        raise ExecutionFactsValidationError("{0} 必须是文本".format(field_name))
    if len(value) > 4096 or _CONTROL_CHARACTER_PATTERN.search(value):
        raise ExecutionFactsValidationError("{0} 超长或包含控制字符".format(field_name))
    return redact_sensitive_text(value)


def _validate_identifier(value: Any, field_name: str) -> str:
    """验证可持久化的稳定业务标识。

    Args:
        value: 待校验标识。
        field_name: 用于错误定位的字段名。

    Returns:
        str: 不含路径、控制字符或秘密模式的标识。

    Raises:
        ExecutionFactsValidationError: 标识不符合白名单。
        ExecutionFactsSecurityError: 标识命中秘密模式。
    """

    if not isinstance(value, str) or not _SAFE_IDENTIFIER_PATTERN.fullmatch(value):
        raise ExecutionFactsValidationError("{0} 不是安全业务标识".format(field_name))
    if _contains_secret(value):
        raise ExecutionFactsSecurityError("{0} 疑似包含秘密，不允许作为事实身份".format(field_name))
    return value


def _validate_contract_version(value: Any, field_name: str) -> str:
    """验证 ``family/vN`` 形式的计算或检查合同版本。

    Args:
        value: 待验证版本文本。
        field_name: 用于错误定位的字段名。

    Returns:
        str: 不含路径穿越、控制字符或秘密模式的合同版本。

    Raises:
        ExecutionFactsValidationError: 文本不符合单层合同版本格式。
        ExecutionFactsSecurityError: 文本命中秘密模式。
    """

    if not isinstance(value, str) or not _CONTRACT_VERSION_PATTERN.fullmatch(value):
        raise ExecutionFactsValidationError("{0} 必须是 family/version 形式".format(field_name))
    if ".." in value or value.startswith("/") or value.endswith("/") or _contains_secret(value):
        raise ExecutionFactsSecurityError("{0} 包含不安全路径或秘密模式".format(field_name))
    return value


def _normalize_payload_value(field_name: str, value: Any) -> Any:
    """按冻结字段类型规范化一个 payload 值。

    Args:
        field_name: V1 白名单字段名。
        value: 调用方原始值。

    Returns:
        Any: JSON 可序列化的规范值。

    Raises:
        ExecutionFactsValidationError: 字段未注册或值不符合类型合同。
    """

    kind = _FIELD_KINDS.get(field_name)
    if kind is None:
        raise ExecutionFactsValidationError("未知 payload 字段: {0}".format(field_name))
    if kind == "decimal":
        return decimal_to_text(value)
    if kind == "security":
        return validate_canonical_security_code(value)
    if kind == "identifier":
        return _validate_identifier(value, field_name)
    if kind == "text":
        return _redact_text(value, field_name)
    if kind in _ENUM_VALUES:
        if kind == "order_status" and isinstance(value, OrderStatus):
            value = value.value
        if kind == "order_type" and isinstance(value, OrderStyle):
            value = value.value
        if not isinstance(value, str) or value not in _ENUM_VALUES[kind]:
            raise ExecutionFactsValidationError("{0} 不在固定枚举中".format(field_name))
        return value
    raise ExecutionFactsValidationError("无法处理 payload 字段类型: {0}".format(kind))


def _normalize_payload(event_type: EventType, payload: Mapping[str, Any]) -> Dict[str, Any]:
    """按事件白名单规范化 payload，并拒绝未知或缺失字段。

    Args:
        event_type: 当前 V1 事件类型。
        payload: 引擎权威事件字段。

    Returns:
        Dict[str, Any]: 可 canonical JSON 序列化的 payload。

    Raises:
        ExecutionFactsValidationError: payload 不是 mapping、键非文本、缺字段或含未知字段。
    """

    if not isinstance(payload, Mapping):
        raise ExecutionFactsValidationError("payload 必须是 mapping")
    if any(not isinstance(name, str) for name in payload):
        raise ExecutionFactsValidationError("payload 字段名必须是文本")
    required, optional = _PAYLOAD_RULES[event_type]
    present = set(payload)
    missing = sorted(required - present)
    unknown = sorted(present - required - optional)
    if missing:
        raise ExecutionFactsValidationError(
            "{0} 缺少 payload 字段: {1}".format(event_type.value, missing)
        )
    if unknown:
        raise ExecutionFactsValidationError("{0} 含未知 payload 字段".format(event_type.value))
    normalized = {name: _normalize_payload_value(name, payload[name]) for name in sorted(present)}
    if event_type is EventType.ORDER_EVENT:
        after_status = normalized["after_status"]
        if (
            after_status
            in {
                OrderStatus.canceled.value,
                OrderStatus.rejected.value,
                OrderStatus.partly_canceled.value,
            }
            and not str(normalized.get("reason", "")).strip()
        ):
            raise ExecutionFactsValidationError("撤单、拒单或部分撤单事件必须记录 reason")
    return normalized


def _canonical_json_bytes(value: Mapping[str, Any], *, trailing_newline: bool) -> bytes:
    """把 mapping 编码为稳定 UTF-8 JSON 字节。

    Args:
        value: 已通过类型和秘密校验的 mapping。
        trailing_newline: 是否为 NDJSON 或文本制品添加换行。

    Returns:
        bytes: sort_keys、紧凑分隔符和 UTF-8 编码的字节。
    """

    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if trailing_newline:
        text += "\n"
    return text.encode("utf-8")


def _reject_duplicate_json_pairs(pairs: Any) -> Dict[str, Any]:
    """把 JSON object pairs 转为字典并拒绝重复键。

    Args:
        pairs: ``json.loads`` 传入的有序 ``(key, value)`` 对。

    Returns:
        Dict[str, Any]: 不含重复字段的 JSON object。

    Raises:
        ExecutionFactsIntegrityError: 同一 object 出现重复字段。
    """

    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ExecutionFactsIntegrityError("JSON object 含重复字段")
        result[key] = value
    return result


def _loads_json_no_duplicates(raw: bytes, artifact_name: str) -> Any:
    """解析 UTF-8 JSON 并拒绝任意层级重复键。

    Args:
        raw: 有界读取的 JSON 原始字节。
        artifact_name: 用于脱敏错误定位的制品名称。

    Returns:
        Any: 解析后的 JSON 值。

    Raises:
        ExecutionFactsIntegrityError: UTF-8、JSON 或重复键损坏。
    """

    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ExecutionFactsIntegrityError("JSON 制品损坏: {0}".format(artifact_name)) from exc


def _scan_artifact_for_secrets(
    path: Path,
    *,
    maximum_bytes: int,
    maximum_line_bytes: int,
    directory_descriptor: Optional[int] = None,
    filename: Optional[str] = None,
) -> None:
    """以有界流式读取扫描持久制品中的秘密模式。

    Args:
        path: 待扫描的 facts、manifest、partial 或 guard 普通文件。
        maximum_bytes: 当前制品合同允许的总字节数。
        maximum_line_bytes: 当前制品单行允许的最大字节数。
        directory_descriptor: 可选固定目录句柄，提供时不使用 path 寻址。
        filename: 与 directory_descriptor 配套的固定 basename。

    Returns:
        None: 文件大小、UTF-8 和 secret canary 均通过时无返回值。

    Raises:
        ExecutionFactsIntegrityError: 文件超限或 UTF-8 损坏。
        ExecutionFactsSecurityError: 文件类型不安全或仍含秘密。
    """

    if directory_descriptor is not None:
        if filename is None:
            raise ExecutionFactsSecurityError("dir_fd secret 扫描缺少 basename")
        _ensure_regular_file_at(directory_descriptor, filename)
        metadata = os.stat(filename, dir_fd=directory_descriptor, follow_symlinks=False)
        descriptor = os.open(
            filename,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory_descriptor,
        )
    else:
        _ensure_regular_file(path)
        metadata = path.stat()
        descriptor = os.open(
            str(path),
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    if metadata.st_size > maximum_bytes:
        os.close(descriptor)
        raise ExecutionFactsIntegrityError("机器事实制品超过冻结字节预算")
    observed_bytes = 0
    with os.fdopen(descriptor, "rb") as stream:
        while True:
            raw_line = stream.readline(maximum_line_bytes + 1)
            if not raw_line:
                break
            observed_bytes += len(raw_line)
            if observed_bytes > maximum_bytes or len(raw_line) > maximum_line_bytes:
                raise ExecutionFactsIntegrityError("机器事实制品超过冻结读取预算")
            try:
                text = raw_line.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ExecutionFactsIntegrityError("机器事实制品不是有效 UTF-8") from exc
            if _contains_secret(text):
                raise ExecutionFactsSecurityError("机器事实制品 secret 扫描失败")


def _validate_utc_text(value: Any, field_name: str) -> datetime:
    """验证 manifest 或 fact 中的 UTC ``Z`` 时间文本。

    Args:
        value: 待解析值。
        field_name: 用于错误定位的字段名。

    Returns:
        datetime: timezone.utc 下的 aware datetime。

    Raises:
        ExecutionFactsValidationError: 文本格式、时区或 canonical 形式不合法。
    """

    if not isinstance(value, str) or not value.endswith("Z"):
        raise ExecutionFactsValidationError("{0} 必须是 aware UTC Z 时间".format(field_name))
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ExecutionFactsValidationError("{0} 不是有效 UTC 时间".format(field_name)) from exc
    normalized, _ = normalize_utc_datetime(parsed)
    if normalized != value:
        raise ExecutionFactsValidationError("{0} 不是 V1 canonical UTC 时间".format(field_name))
    return parsed


class _ExactEventIdTracker:
    """以固定内存阈值和临时磁盘分桶完成 UUID 精确查重。

    小文件直接使用受限集合；超过阈值后把 UUID 的 16 原始字节按首字节写入私有临时目录。
    单桶仍超阈值时按后续字节递归分桶，因此任意规模和任意分布都不会退回线性内存集合。
    """

    def __init__(self, memory_limit: Optional[int] = None) -> None:
        """创建尚未接收事件 ID 的精确 tracker。

        Args:
            memory_limit: 单个内存集合允许的最大 UUID 数；None 使用模块固定门槛。

        Raises:
            ExecutionFactsValidationError: 门槛不是正整数。
        """

        effective_limit = _EVENT_ID_MEMORY_LIMIT if memory_limit is None else memory_limit
        if (
            not isinstance(effective_limit, int)
            or isinstance(effective_limit, bool)
            or effective_limit < 1
        ):
            raise ExecutionFactsValidationError("事件 ID 内存查重门槛必须是正整数")
        self.memory_limit = effective_limit
        self._memory_ids: Set[bytes] = set()
        self._temporary_directory: Optional[Any] = None
        self._root_directory: Optional[Path] = None
        self._root_buffers: Dict[int, bytearray] = {}
        self._finished = False

    @staticmethod
    def _bucket_path(directory: Path, bucket_index: int) -> Path:
        """生成私有临时目录内的固定桶文件名。

        Args:
            directory: 当前递归层目录。
            bucket_index: UUID 当前字节的 0 到 255 值。

        Returns:
            Path: ``00.ids`` 到 ``ff.ids`` 之一。
        """

        return directory / "{0:02x}.ids".format(bucket_index)

    @staticmethod
    def _flush_one_bucket(path: Path, buffer: bytearray) -> None:
        """把一个固定大小桶缓冲追加到私有临时文件。

        Args:
            path: 当前桶文件。
            buffer: 由完整 16 字节 UUID 记录组成的缓冲。

        Returns:
            None: 缓冲为空或写入成功后无返回值。

        Raises:
            ExecutionFactsIntegrityError: 缓冲字节未按 UUID 边界对齐。
            OSError: 临时磁盘写入失败。

        Side Effects:
            以权限不超过 0600 追加桶文件并清空缓冲。
        """

        if not buffer:
            return
        if len(buffer) % _EVENT_ID_BYTES:
            raise ExecutionFactsIntegrityError("事件 ID 桶缓冲未按 UUID 边界对齐")
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(str(path), flags, 0o600)
        try:
            _write_all(descriptor, bytes(buffer))
        finally:
            os.close(descriptor)
        buffer.clear()

    @classmethod
    def _append_bucket_record(
        cls,
        directory: Path,
        depth: int,
        record: bytes,
        buffers: Dict[int, bytearray],
    ) -> None:
        """按 UUID 指定字节把一条记录加入固定总量的桶缓冲。

        Args:
            directory: 当前分桶目录。
            depth: 用作桶索引的 UUID 字节位置。
            record: 恰好 16 字节的 UUID。
            buffers: 当前递归层最多 256 个小缓冲。

        Returns:
            None: 记录进入缓冲或已被追加到桶文件。

        Raises:
            ExecutionFactsIntegrityError: UUID 长度或 depth 不合法。
            OSError: 桶缓冲达到门槛后的临时磁盘写入失败。
        """

        if len(record) != _EVENT_ID_BYTES or not 0 <= depth < _EVENT_ID_BYTES:
            raise ExecutionFactsIntegrityError("事件 ID 分桶记录或深度不合法")
        bucket_index = record[depth]
        buffer = buffers.setdefault(bucket_index, bytearray())
        buffer.extend(record)
        if len(buffer) >= _EVENT_ID_BUCKET_BUFFER_BYTES:
            cls._flush_one_bucket(cls._bucket_path(directory, bucket_index), buffer)

    @classmethod
    def _flush_bucket_buffers(cls, directory: Path, buffers: Dict[int, bytearray]) -> None:
        """按桶编号顺序写完当前递归层的全部缓冲。

        Args:
            directory: 当前分桶目录。
            buffers: 由桶编号映射到固定大小 bytearray 的字典。

        Returns:
            None: 全部非空缓冲写入后无返回值。

        Raises:
            OSError: 任一桶临时文件追加失败。

        Side Effects:
            写入桶文件并清空 buffers。
        """

        for bucket_index in sorted(buffers):
            cls._flush_one_bucket(cls._bucket_path(directory, bucket_index), buffers[bucket_index])
        buffers.clear()

    def _start_disk_spill(self) -> None:
        """创建私有临时目录并把受限内存集合迁入首层桶。

        Returns:
            None: 全部既有 UUID 已进入固定桶缓冲。

        Raises:
            OSError: 临时目录或桶文件创建失败。

        Side Effects:
            在系统临时目录创建权限隔离的短生命周期查重制品。
        """

        self._temporary_directory = tempfile.TemporaryDirectory(prefix="bullet-trade-event-id-")
        self._root_directory = Path(self._temporary_directory.name) / "root"
        self._root_directory.mkdir(mode=0o700)
        for record in self._memory_ids:
            self._append_bucket_record(self._root_directory, 0, record, self._root_buffers)
        self._memory_ids.clear()

    def add(self, source_event_id: str) -> None:
        """登记一个 UUIDv5，并在任意阶段精确拒绝重复值。

        Args:
            source_event_id: 已通过 envelope 格式校验的 canonical UUID 文本。

        Returns:
            None: 事件 ID 唯一时进入内存集合或磁盘桶。

        Raises:
            ExecutionFactsIntegrityError: tracker 已完成或事件 ID 重复。
            OSError: 临时分桶写入失败。
        """

        if self._finished:
            raise ExecutionFactsIntegrityError("事件 ID tracker 已完成，禁止继续登记")
        record = UUID(source_event_id).bytes
        if self._root_directory is None:
            if record in self._memory_ids:
                raise ExecutionFactsIntegrityError("source_event_id 在同一 facts 文件中重复")
            if len(self._memory_ids) < self.memory_limit:
                self._memory_ids.add(record)
                return
            self._start_disk_spill()
        if self._root_directory is None:
            raise ExecutionFactsIntegrityError("事件 ID 临时分桶目录未创建")
        self._append_bucket_record(self._root_directory, 0, record, self._root_buffers)

    def _validate_small_bucket(self, path: Path, record_count: int) -> None:
        """在固定门槛内读取单桶并用精确集合检查重复。

        Args:
            path: 已按 UUID 前缀分桶的临时文件。
            record_count: 从文件长度计算的 UUID 数。

        Returns:
            None: 所有记录长度正确且唯一时无返回值。

        Raises:
            ExecutionFactsIntegrityError: 文件短读或发现重复 UUID。
        """

        content = path.read_bytes()
        if len(content) != record_count * _EVENT_ID_BYTES:
            raise ExecutionFactsIntegrityError("事件 ID 桶文件发生短读")
        records = {
            content[offset : offset + _EVENT_ID_BYTES]
            for offset in range(0, len(content), _EVENT_ID_BYTES)
        }
        if len(records) != record_count:
            raise ExecutionFactsIntegrityError("source_event_id 在同一 facts 文件中重复")

    def _partition_large_bucket(self, path: Path, depth: int) -> Path:
        """把超出内存门槛的桶按下一个 UUID 字节精确再分桶。

        Args:
            path: 当前超大桶文件。
            depth: 下一个用于分桶的 UUID 字节位置。

        Returns:
            Path: 包含下一层桶文件的私有子目录。

        Raises:
            ExecutionFactsIntegrityError: 读取块未按 16 字节对齐。
            OSError: 临时目录、读取或分桶写入失败。

        Side Effects:
            创建下一层目录，写入子桶并删除已完全迁移的父桶。
        """

        child_directory = path.parent / (path.stem + "-depth-{0}".format(depth))
        child_directory.mkdir(mode=0o700)
        buffers: Dict[int, bytearray] = {}
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(_EVENT_ID_BYTES * _EVENT_ID_BUCKET_READ_RECORDS)
                if not chunk:
                    break
                if len(chunk) % _EVENT_ID_BYTES:
                    raise ExecutionFactsIntegrityError("事件 ID 桶读取块未按 UUID 边界对齐")
                for offset in range(0, len(chunk), _EVENT_ID_BYTES):
                    self._append_bucket_record(
                        child_directory,
                        depth,
                        chunk[offset : offset + _EVENT_ID_BYTES],
                        buffers,
                    )
        self._flush_bucket_buffers(child_directory, buffers)
        path.unlink()
        return child_directory

    def _validate_bucket(self, path: Path, next_depth: int) -> None:
        """精确验证一个桶，必要时递归分桶以保持固定内存上限。

        Args:
            path: 当前桶文件。
            next_depth: 超大桶下一次使用的 UUID 字节位置。

        Returns:
            None: 当前桶及全部子桶均没有重复时无返回值。

        Raises:
            ExecutionFactsIntegrityError: 文件损坏、UUID 全字节相同或发现重复。
            OSError: 临时磁盘操作失败。
        """

        byte_size = path.stat().st_size
        if byte_size % _EVENT_ID_BYTES:
            raise ExecutionFactsIntegrityError("事件 ID 桶文件长度不合法")
        record_count = byte_size // _EVENT_ID_BYTES
        if record_count <= self.memory_limit:
            self._validate_small_bucket(path, record_count)
            return
        if next_depth >= _EVENT_ID_BYTES:
            raise ExecutionFactsIntegrityError("source_event_id 在同一 facts 文件中重复")
        child_directory = self._partition_large_bucket(path, next_depth)
        try:
            for child_path in sorted(child_directory.glob("*.ids")):
                self._validate_bucket(child_path, next_depth + 1)
        finally:
            for child_path in child_directory.glob("*.ids"):
                child_path.unlink()
            child_directory.rmdir()

    def finish(self) -> None:
        """完成磁盘缓冲写入并递归验证所有桶的精确唯一性。

        Returns:
            None: 重复调用或全部事件 ID 唯一时无返回值。

        Raises:
            ExecutionFactsIntegrityError: 任一桶发现重复或损坏记录。
            OSError: 临时磁盘写入、读取或清理失败。
        """

        if self._finished:
            return
        if self._root_directory is not None:
            self._flush_bucket_buffers(self._root_directory, self._root_buffers)
            for path in sorted(self._root_directory.glob("*.ids")):
                self._validate_bucket(path, 1)
        self._finished = True

    def close(self) -> None:
        """清理短生命周期分桶目录和剩余内存身份。

        Returns:
            None: 无论是否发生过 spill，清理后无返回值。

        Side Effects:
            删除系统临时目录中的全部精确查重桶文件。
        """

        self._memory_ids.clear()
        self._root_buffers.clear()
        if self._temporary_directory is not None:
            self._temporary_directory.cleanup()
            self._temporary_directory = None
            self._root_directory = None

    def __enter__(self) -> "_ExactEventIdTracker":
        """返回 tracker 供流式 validator 的 with 语句使用。

        Returns:
            _ExactEventIdTracker: 当前实例。
        """

        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """正常退出时完成查重，任何退出路径都清理临时桶。

        Args:
            exc_type: 可选异常类型。
            exc_value: 可选异常值。
            traceback: 可选异常堆栈。

        Returns:
            None: 不吞掉原异常或 finish 产生的完整性异常。

        Side Effects:
            正常路径调用 finish，随后清理全部临时查重制品。
        """

        del exc_value, traceback
        try:
            if exc_type is None:
                self.finish()
        finally:
            self.close()


def _validate_envelope(
    value: Any,
    expected_sequence: int,
    expected_run_id: Optional[str],
    seen_event_ids: _ExactEventIdTracker,
) -> Tuple[str, str]:
    """校验一条已反序列化事实 envelope。

    Args:
        value: json.loads 得到的对象。
        expected_sequence: 本行必须匹配的严格序号。
        expected_run_id: 可选的固定运行 UUID。
        seen_event_ids: 当前文件的有界内存、磁盘分桶精确查重 tracker。

    Returns:
        Tuple[str, str]: ``(run_id, event_type)``。

    Raises:
        ExecutionFactsValidationError: schema、字段、时间或 payload 不合法。
        ExecutionFactsIntegrityError: sequence 或 source event ID 重复。
    """

    if not isinstance(value, dict):
        raise ExecutionFactsValidationError("事实 envelope 必须是 JSON object")
    required = set(EXECUTION_FACTS_V1_JSON_SCHEMA["required"])
    present = set(value)
    if present != required:
        raise ExecutionFactsValidationError(
            "事实 envelope 字段必须精确匹配 V1，缺失={0} 未知={1}".format(
                sorted(required - present), sorted(present - required)
            )
        )
    if value["schema_version"] != SCHEMA_VERSION:
        raise ExecutionFactsValidationError("未知 execution facts schema")
    run_id = validate_run_id(value["run_id"])
    if expected_run_id is not None and run_id != expected_run_id:
        raise ExecutionFactsIntegrityError("事实 run_id 与期望运行不一致")
    sequence = value["sequence"]
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence != expected_sequence:
        raise ExecutionFactsIntegrityError("事实 sequence 不连续")
    source_event_id = value["source_event_id"]
    if not isinstance(source_event_id, str) or not _CANONICAL_UUID_PATTERN.fullmatch(
        source_event_id
    ):
        raise ExecutionFactsValidationError("source_event_id 必须是 canonical UUIDv5")
    parsed_event_id = UUID(source_event_id)
    if parsed_event_id.version != 5 or str(parsed_event_id) != source_event_id:
        raise ExecutionFactsValidationError("source_event_id 必须是 canonical UUIDv5")
    occurred_at = _validate_utc_text(value["occurred_at"], "occurred_at")
    trade_date = value["trade_date"]
    try:
        parsed_trade_date = date.fromisoformat(trade_date)
    except (TypeError, ValueError) as exc:
        raise ExecutionFactsValidationError("trade_date 必须是 ISO 日期") from exc
    expected_trade_date = occurred_at.astimezone(_SHANGHAI_TIMEZONE).date()
    if parsed_trade_date != expected_trade_date:
        raise ExecutionFactsValidationError("trade_date 必须按 Asia/Shanghai 从 occurred_at 派生")
    event_type = _coerce_event_type(value["event_type"])
    normalized_payload = _normalize_payload(event_type, value["payload"])
    if normalized_payload != value["payload"]:
        raise ExecutionFactsValidationError("payload 不是 execution-facts/v1 canonical 表示")
    seen_event_ids.add(source_event_id)
    return run_id, event_type.value


def _ensure_regular_file(path: Path) -> None:
    """通过 lstat 确认目标存在、非 symlink 且为普通文件。

    Args:
        path: 待读取或发布的文件路径。

    Returns:
        None: 校验通过时无返回值。

    Raises:
        ExecutionFactsSecurityError: 路径是 symlink 或非普通文件。
        FileNotFoundError: 文件不存在。
    """

    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ExecutionFactsSecurityError("机器事实路径必须是非 symlink 普通文件: {0}".format(path))


def _assert_no_symlink_ancestors(path: Path) -> None:
    """按请求的词法路径逐组件拒绝已存在 symlink 祖先。

    Args:
        path: 尚未 resolve 的目录或文件路径。

    Returns:
        None: 当前已存在的每一层均不是 symlink 时无返回值。

    Raises:
        ExecutionFactsSecurityError: 任一现存组件是 symlink。
    """

    absolute_path = path.absolute()
    current = Path(absolute_path.anchor)
    for part in absolute_path.parts[1:]:
        current = current / part
        if not current.exists() and not current.is_symlink():
            continue
        metadata = current.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise ExecutionFactsSecurityError("机器事实路径的任一祖先均不得是 symlink: {0}".format(current))


def _ensure_safe_output_directory(path: Path) -> None:
    """创建或验证 writer 独享输出目录。

    Args:
        path: 固定事实文件所在目录。

    Returns:
        None: 目录存在且不是 symlink 时无返回值。

    Raises:
        ExecutionFactsSecurityError: 目标是 symlink 或不是目录。

    Side Effects:
        目录不存在时创建父目录和目标目录。
    """

    _assert_no_symlink_ancestors(path)
    if path.exists() or path.is_symlink():
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise ExecutionFactsSecurityError("机器事实输出必须是非 symlink 目录: {0}".format(path))
        return
    path.mkdir(parents=True, mode=0o700)
    _assert_no_symlink_ancestors(path)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ExecutionFactsSecurityError("机器事实输出目录创建后类型异常: {0}".format(path))


def _open_directory_chain_no_symlinks(path: Path) -> int:
    """从文件系统根开始逐组件打开并固定非 symlink 目录句柄。

    Args:
        path: 必须已存在的绝对或可绝对化目录路径。

    Returns:
        int: 调用方负责关闭的最终目录 descriptor。

    Raises:
        ExecutionFactsSecurityError: 平台缺少 dir_fd、组件为链接或不是目录。
        OSError: 权限或底层文件系统打开失败。
    """

    if (
        os.name != "posix"
        or not getattr(os, "O_DIRECTORY", 0)
        or not getattr(os, "O_NOFOLLOW", 0)
        or os.open not in os.supports_dir_fd
    ):
        raise ExecutionFactsSecurityError("当前平台缺少可信目录 openat 能力")
    absolute = path.absolute()
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptor = os.open(absolute.anchor, flags)
    try:
        for part in absolute.parts[1:]:
            next_descriptor = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
    except BaseException:
        os.close(descriptor)
        raise
    metadata = os.fstat(descriptor)
    if not stat.S_ISDIR(metadata.st_mode):
        os.close(descriptor)
        raise ExecutionFactsSecurityError("可信目录 descriptor 不是目录")
    return descriptor


def _require_secure_directory_platform() -> None:
    """在创建 partial 前验证 dir_fd、nofollow 与目录持久化能力。

    Returns:
        None: 当前 POSIX runtime 暴露全部必要原语时无返回值。

    Raises:
        ExecutionFactsSecurityError: Windows 或缺少任一 required 原语的平台失败关闭。
    """

    required_dir_fd_functions = (os.open, os.mkdir, os.stat, os.unlink, os.link)
    if (
        os.name != "posix"
        or not getattr(os, "O_DIRECTORY", 0)
        or not getattr(os, "O_NOFOLLOW", 0)
        or any(function not in os.supports_dir_fd for function in required_dir_fd_functions)
    ):
        raise ExecutionFactsSecurityError("当前平台缺少 required dir_fd、nofollow 或目录 fsync 证明")


def _exists_at(directory_descriptor: int, filename: str) -> bool:
    """在固定目录 descriptor 下判断 basename 是否存在且不跟随链接。

    Args:
        directory_descriptor: 固定输出目录句柄。
        filename: 固定 basename。

    Returns:
        bool: 任意类型目录项存在时为 True。
    """

    try:
        os.stat(filename, dir_fd=directory_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True


def _ensure_regular_file_at(directory_descriptor: int, filename: str) -> None:
    """确认固定目录下 basename 是非 symlink 普通文件。

    Args:
        directory_descriptor: 固定输出目录句柄。
        filename: 固定 basename。

    Returns:
        None: 类型安全时无返回值。

    Raises:
        ExecutionFactsSecurityError: 目标是链接或非普通文件。
        FileNotFoundError: 目标不存在。
    """

    metadata = os.stat(filename, dir_fd=directory_descriptor, follow_symlinks=False)
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ExecutionFactsSecurityError("dir_fd 下机器事实必须是普通文件")


def _open_exclusive_at(directory_descriptor: int, filename: str) -> int:
    """只在固定输出目录 descriptor 下排他创建 basename。

    Args:
        directory_descriptor: 固定输出目录句柄。
        filename: 固定 staging basename。

    Returns:
        int: 调用方负责关闭的文件 descriptor。

    Raises:
        ExecutionFactsConflictError: 同名目录项已存在。
        OSError: 文件系统不支持所需原语。
    """

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        return os.open(filename, flags, 0o600, dir_fd=directory_descriptor)
    except FileExistsError as exc:
        raise ExecutionFactsConflictError("机器事实 staging 已存在，拒绝覆盖") from exc


def _fsync_directory_descriptor(directory_descriptor: int) -> None:
    """直接持久化已固定的输出目录 descriptor。

    Args:
        directory_descriptor: 固定目录句柄。

    Returns:
        None: fsync 成功后无返回值。

    Raises:
        ExecutionFactsSecurityError: 非 POSIX 平台不具备证明能力。
        OSError: 文件系统拒绝目录 fsync。
    """

    if os.name != "posix":
        raise ExecutionFactsSecurityError("当前平台无法证明目录 fsync")
    os.fsync(directory_descriptor)


def _write_all(file_descriptor: int, data: bytes) -> None:
    """循环写完全部字节，正确处理短写入。

    Args:
        file_descriptor: 已打开的只写文件描述符。
        data: 待持久化字节。

    Returns:
        None: 全部字节写入后无返回值。

    Raises:
        OSError: 磁盘满、句柄关闭或系统写入失败。

    Side Effects:
        向 staging 文件追加字节。
    """

    view = memoryview(data)
    offset = 0
    while offset < len(view):
        written = os.write(file_descriptor, view[offset:])
        if written <= 0:
            raise OSError("机器事实写入未取得进展")
        offset += written


def _fsync_directory(directory: Path) -> None:
    """在支持目录 fsync 的系统上持久化目录元数据。

    Args:
        directory: staging 与 final 共用的父目录。

    Returns:
        None: 不支持目录句柄的平台安全跳过。

    Raises:
        OSError: 支持目录 fsync 的平台持久化失败。
    """

    if os.name != "posix":
        raise ExecutionFactsSecurityError("当前平台无法用标准库证明目录 fsync，原子发布失败关闭")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(str(directory), flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_exclusive_at(
    directory_descriptor: int,
    source_filename: str,
    target_filename: str,
) -> None:
    """在同一固定目录 descriptor 下排他发布 staging basename。

    Args:
        directory_descriptor: 构造期固定并持续持有的输出目录句柄。
        source_filename: 已 fsync 且验证通过的 staging basename。
        target_filename: 不允许覆盖的 final basename。

    Returns:
        None: final durable 且 staging 已删除后无返回值。

    Raises:
        ExecutionFactsConflictError: final 已存在。
        ExecutionFactsSecurityError: staging 类型不安全。
        OSError: hardlink、目录 fsync、删除或回滚失败。
    """

    _ensure_regular_file_at(directory_descriptor, source_filename)
    if _exists_at(directory_descriptor, target_filename):
        raise ExecutionFactsConflictError("机器事实 final 已存在，拒绝覆盖")
    try:
        os.link(
            source_filename,
            target_filename,
            src_dir_fd=directory_descriptor,
            dst_dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except FileExistsError as exc:
        raise ExecutionFactsConflictError("机器事实 final 并发出现，拒绝覆盖") from exc
    try:
        _fsync_directory_descriptor(directory_descriptor)
        os.unlink(source_filename, dir_fd=directory_descriptor)
        _fsync_directory_descriptor(directory_descriptor)
    except BaseException:
        try:
            if not _exists_at(directory_descriptor, source_filename) and _exists_at(
                directory_descriptor, target_filename
            ):
                os.link(
                    target_filename,
                    source_filename,
                    src_dir_fd=directory_descriptor,
                    dst_dir_fd=directory_descriptor,
                    follow_symlinks=False,
                )
            if _exists_at(directory_descriptor, target_filename):
                os.unlink(target_filename, dir_fd=directory_descriptor)
            _fsync_directory_descriptor(directory_descriptor)
        except OSError:
            pass
        raise


def _build_publish_marker(manifest: Mapping[str, Any], encoded_manifest: bytes) -> Dict[str, Any]:
    """构造与 canonical manifest 字节绑定的发布状态标志。

    Args:
        manifest: 已通过 V1 校验的 manifest。
        encoded_manifest: writer 即将发布的 canonical manifest 原始字节。

    Returns:
        Dict[str, Any]: 固定协议、运行身份、路径和 SHA-256 标志内容。

    Raises:
        ExecutionFactsValidationError: manifest 缺少合法 run_id。

    Side Effects:
        无；只计算内存中的 SHA-256。
    """

    raw_run_id = manifest.get("run_id")
    if not isinstance(raw_run_id, str):
        raise ExecutionFactsValidationError("发布标志要求 manifest.run_id 为文本")
    run_id = validate_run_id(raw_run_id)
    return {
        "protocol_version": PUBLISH_PROTOCOL_VERSION,
        "run_id": run_id,
        "manifest_path": MANIFEST_FILENAME,
        "manifest_sha256": hashlib.sha256(encoded_manifest).hexdigest(),
    }


def _write_durable_publish_guard_at(
    directory_descriptor: int,
    filename: str,
    marker: Mapping[str, Any],
) -> None:
    """在固定目录 descriptor 下创建并持久化 incomplete guard。

    Args:
        directory_descriptor: 固定输出目录句柄。
        filename: incomplete guard basename。
        marker: 与 manifest 原始字节 SHA 绑定的 marker。

    Returns:
        None: guard 文件和目录均 durable 后无返回值。

    Raises:
        ExecutionFactsConflictError: guard 已存在。
        OSError: 写入或 fsync 失败。
    """

    descriptor = _open_exclusive_at(directory_descriptor, filename)
    try:
        _write_all(descriptor, _canonical_json_bytes(marker, trailing_newline=True))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory_descriptor(directory_descriptor)


def _commit_publish_guard_at(
    directory_descriptor: int,
    incomplete_filename: str,
    complete_filename: str,
) -> None:
    """在固定目录 descriptor 下完成发布最后可见状态切换。

    Args:
        directory_descriptor: 固定输出目录句柄。
        incomplete_filename: durable incomplete guard basename。
        complete_filename: 唯一成功 guard basename。

    Returns:
        None: complete durable 且 incomplete 已删除后立即返回。

    Raises:
        ExecutionFactsConflictError: complete 已存在。
        ExecutionFactsSecurityError: incomplete 类型不安全。
        OSError: hardlink、目录 fsync 或删除失败。

    Side Effects:
        incomplete 删除成功后不再调用任何可能抛错的系统操作。
    """

    _ensure_regular_file_at(directory_descriptor, incomplete_filename)
    if _exists_at(directory_descriptor, complete_filename):
        raise ExecutionFactsConflictError("机器事实 complete guard 已存在，拒绝覆盖")
    try:
        os.link(
            incomplete_filename,
            complete_filename,
            src_dir_fd=directory_descriptor,
            dst_dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except FileExistsError as exc:
        raise ExecutionFactsConflictError("机器事实 complete guard 并发出现，拒绝覆盖") from exc
    _fsync_directory_descriptor(directory_descriptor)
    try:
        os.unlink(incomplete_filename, dir_fd=directory_descriptor)
    except BaseException:
        if not _exists_at(directory_descriptor, incomplete_filename):
            return
        raise


def _validate_publish_marker(
    marker: Dict[str, Any],
    manifest: Mapping[str, Any],
    manifest_sha256: str,
) -> None:
    """验证 complete guard 与实际 canonical manifest 精确绑定。

    Args:
        marker: 从 ``.publish.complete`` 读取的 JSON object。
        manifest: 已通过 V1 字段与 facts 摘要校验的 manifest。
        manifest_sha256: 从磁盘 manifest 原始字节计算的 SHA-256。

    Returns:
        None: 协议、运行身份、路径和 SHA 全部一致时无返回值。

    Raises:
        ExecutionFactsValidationError: marker 字段或协议格式非法。
        ExecutionFactsIntegrityError: marker 与 manifest 身份或摘要不一致。
    """

    required = {"protocol_version", "run_id", "manifest_path", "manifest_sha256"}
    if set(marker) != required:
        raise ExecutionFactsValidationError("complete guard 字段必须精确匹配发布协议")
    if marker["protocol_version"] != PUBLISH_PROTOCOL_VERSION:
        raise ExecutionFactsValidationError("complete guard 发布协议版本不受支持")
    marker_run_id = validate_run_id(marker["run_id"])
    if marker_run_id != manifest["run_id"]:
        raise ExecutionFactsIntegrityError("complete guard run_id 与 manifest 不一致")
    if marker["manifest_path"] != MANIFEST_FILENAME:
        raise ExecutionFactsValidationError("complete guard manifest_path 不合法")
    marker_sha256 = marker["manifest_sha256"]
    if not isinstance(marker_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", marker_sha256):
        raise ExecutionFactsValidationError("complete guard manifest_sha256 格式不合法")
    if not isinstance(manifest_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", manifest_sha256):
        raise ExecutionFactsValidationError("manifest 原始 SHA-256 格式不合法")
    if marker_sha256 != manifest_sha256:
        raise ExecutionFactsIntegrityError("complete guard manifest SHA-256 不一致")


def _read_json_regular_file_with_sha(path: Path) -> Tuple[Dict[str, Any], str]:
    """在不跟随 symlink 的前提下读取 JSON object 及原始字节 SHA。

    Args:
        path: manifest 或其他 JSON 制品。

    Returns:
        Tuple[Dict[str, Any], str]: 解析后的 object 与磁盘原始字节 SHA-256。

    Raises:
        ExecutionFactsSecurityError: 文件类型不安全。
        ExecutionFactsIntegrityError: UTF-8 或 JSON 损坏、顶层非 object。
    """

    _ensure_regular_file(path)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(str(path), flags)
    digest = hashlib.sha256()
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ExecutionFactsSecurityError("JSON 制品不是普通文件: {0}".format(path))
        if metadata.st_size > MAX_MANIFEST_FILE_BYTES:
            raise ExecutionFactsIntegrityError("JSON 制品超过 manifest 冻结字节预算")
        chunks = []
        total_bytes = 0
        while True:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            total_bytes += len(chunk)
            if total_bytes > MAX_MANIFEST_FILE_BYTES:
                raise ExecutionFactsIntegrityError("JSON 制品超过 manifest 冻结字节预算")
            chunks.append(chunk)
            digest.update(chunk)
    finally:
        os.close(descriptor)
    value = _loads_json_no_duplicates(b"".join(chunks), path.name)
    if not isinstance(value, dict):
        raise ExecutionFactsIntegrityError("JSON 制品顶层必须是 object: {0}".format(path))
    return value, digest.hexdigest()


def _read_json_regular_file(path: Path) -> Dict[str, Any]:
    """读取安全 JSON object，并忽略同时计算的原始文件 SHA。

    Args:
        path: manifest、guard 或其他 JSON 制品。

    Returns:
        Dict[str, Any]: 解析后的 JSON object。

    Raises:
        ExecutionFactsSecurityError: 文件类型不安全。
        ExecutionFactsIntegrityError: UTF-8、JSON 或顶层类型损坏。
    """

    value, _ = _read_json_regular_file_with_sha(path)
    return value


def _read_json_regular_file_with_sha_at(
    directory_descriptor: int, filename: str
) -> Tuple[Dict[str, Any], str]:
    """从固定目录 descriptor 有界读取 JSON object 和原始 SHA。

    Args:
        directory_descriptor: 已固定的输出目录句柄。
        filename: manifest 或 guard 固定 basename。

    Returns:
        Tuple[Dict[str, Any], str]: 解析 object 与磁盘原始 SHA-256。

    Raises:
        ExecutionFactsSecurityError: 目录项不是普通文件。
        ExecutionFactsIntegrityError: 文件超限、损坏、重复键或顶层非 object。
    """

    _ensure_regular_file_at(directory_descriptor, filename)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(filename, flags, dir_fd=directory_descriptor)
    digest = hashlib.sha256()
    chunks: List[bytes] = []
    total_bytes = 0
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ExecutionFactsSecurityError("dir_fd JSON 制品不是普通文件")
        if metadata.st_size > MAX_MANIFEST_FILE_BYTES:
            raise ExecutionFactsIntegrityError("JSON 制品超过 manifest 冻结字节预算")
        while True:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            total_bytes += len(chunk)
            if total_bytes > MAX_MANIFEST_FILE_BYTES:
                raise ExecutionFactsIntegrityError("JSON 制品超过 manifest 冻结字节预算")
            chunks.append(chunk)
            digest.update(chunk)
    finally:
        os.close(descriptor)
    value = _loads_json_no_duplicates(b"".join(chunks), filename)
    if not isinstance(value, dict):
        raise ExecutionFactsIntegrityError("JSON 制品顶层必须是 object")
    return value, digest.hexdigest()


def _read_json_regular_file_at(directory_descriptor: int, filename: str) -> Dict[str, Any]:
    """从固定目录 descriptor 读取 JSON object 并忽略 SHA。

    Args:
        directory_descriptor: 已固定输出目录句柄。
        filename: JSON basename。

    Returns:
        Dict[str, Any]: 已拒绝重复键的 JSON object。
    """

    value, _ = _read_json_regular_file_with_sha_at(directory_descriptor, filename)
    return value


def validate_facts_file(
    path: Union[str, Path],
    *,
    expected_run_id: Optional[str] = None,
    expected_record_count: Optional[int] = None,
    expected_byte_size: Optional[int] = None,
    expected_sha256: Optional[str] = None,
    _directory_descriptor: Optional[int] = None,
    _filename: Optional[str] = None,
) -> FactsFileSummary:
    """流式复验 facts 或 partial 的 schema、顺序、身份和文件摘要。

    Args:
        path: 待验证 NDJSON 文件。
        expected_run_id: 可选固定运行 UUID。
        expected_record_count: 可选期望行数。
        expected_byte_size: 可选期望原始字节数。
        expected_sha256: 可选期望 SHA-256。
        _directory_descriptor: 内部固定目录句柄；consumer/writer 防 TOCTOU 使用。
        _filename: 与内部目录句柄配套的固定 basename。

    Returns:
        FactsFileSummary: 只保留计数和摘要的有界内存结果。

    Raises:
        ExecutionFactsValidationError: 事实 schema 或字段值非法。
        ExecutionFactsIntegrityError: NDJSON 损坏、序号/身份/摘要不一致。
        ExecutionFactsSecurityError: 路径是 symlink 或非普通文件。
    """

    facts_path = Path(path)
    if expected_run_id is not None:
        expected_run_id = validate_run_id(expected_run_id)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    if _directory_descriptor is not None:
        if _filename is None:
            raise ExecutionFactsSecurityError("dir_fd facts 校验缺少 basename")
        _ensure_regular_file_at(_directory_descriptor, _filename)
        descriptor = os.open(_filename, flags, dir_fd=_directory_descriptor)
    else:
        _ensure_regular_file(facts_path)
        descriptor = os.open(str(facts_path), flags)
    digest = hashlib.sha256()
    record_count = 0
    byte_size = 0
    first_sequence: Optional[int] = None
    last_sequence: Optional[int] = None
    event_counts: Counter[str] = Counter()
    observed_run_id: Optional[str] = None
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ExecutionFactsSecurityError("facts 必须是普通文件: {0}".format(facts_path))
        if metadata.st_size > MAX_FACTS_FILE_BYTES:
            raise ExecutionFactsIntegrityError("facts 超过冻结总字节预算")
        with _ExactEventIdTracker() as seen_event_ids:
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                line_number = 0
                while True:
                    raw_line = stream.readline(MAX_FACT_LINE_BYTES + 1)
                    if not raw_line:
                        break
                    line_number += 1
                    if len(raw_line) > MAX_FACT_LINE_BYTES:
                        raise ExecutionFactsIntegrityError(
                            "NDJSON 第 {0} 行超过冻结单行预算".format(line_number)
                        )
                    if not raw_line.endswith(b"\n"):
                        raise ExecutionFactsIntegrityError(
                            "NDJSON 第 {0} 行缺少 canonical 换行".format(line_number)
                        )
                    byte_size += len(raw_line)
                    if byte_size > MAX_FACTS_FILE_BYTES:
                        raise ExecutionFactsIntegrityError("facts 超过冻结总字节预算")
                    digest.update(raw_line)
                    if not raw_line.strip():
                        raise ExecutionFactsIntegrityError("NDJSON 第 {0} 行为空".format(line_number))
                    value = _loads_json_no_duplicates(
                        raw_line, "NDJSON 第 {0} 行".format(line_number)
                    )
                    expected_sequence = record_count + 1
                    run_id, event_type = _validate_envelope(
                        value,
                        expected_sequence,
                        expected_run_id or observed_run_id,
                        seen_event_ids,
                    )
                    if observed_run_id is None:
                        observed_run_id = run_id
                    record_count += 1
                    if first_sequence is None:
                        first_sequence = expected_sequence
                    last_sequence = expected_sequence
                    event_counts[event_type] += 1
    finally:
        os.close(descriptor)
    actual_sha256 = digest.hexdigest()
    if expected_record_count is not None and record_count != expected_record_count:
        raise ExecutionFactsIntegrityError(
            "facts 行数不符，期望 {0}，实际 {1}".format(expected_record_count, record_count)
        )
    if expected_byte_size is not None and byte_size != expected_byte_size:
        raise ExecutionFactsIntegrityError(
            "facts 字节数不符，期望 {0}，实际 {1}".format(expected_byte_size, byte_size)
        )
    if expected_sha256 is not None and actual_sha256 != expected_sha256:
        raise ExecutionFactsIntegrityError("facts SHA-256 不符")
    return FactsFileSummary(
        record_count=record_count,
        byte_size=byte_size,
        sha256=actual_sha256,
        first_sequence=first_sequence,
        last_sequence=last_sequence,
        event_counts={event_type.value: event_counts[event_type.value] for event_type in EventType},
    )


def _quality_decimal(payload: Mapping[str, Any], field_name: str) -> Decimal:
    """读取已经通过 schema 校验的 canonical Decimal payload。

    Args:
        payload: 当前事实 payload。
        field_name: 已知 Decimal 字段名。

    Returns:
        Decimal: 与持久文本精确相等的数值。
    """

    return Decimal(payload[field_name])


def _raise_quality_failure(message: str) -> NoReturn:
    """以统一、无 payload 回显的形式阻断发布质量。

    Args:
        message: 不含外部原始值的固定中文原因。

    Raises:
        ExecutionFactsIntegrityError: 每次调用均抛出质量失败。
    """

    raise ExecutionFactsIntegrityError("execution facts 质量失败: {0}".format(message))


def validate_execution_facts_quality(
    path: Union[str, Path],
    *,
    summary: Optional[FactsFileSummary] = None,
    expected_run_id: Optional[str] = None,
    _directory_descriptor: Optional[int] = None,
    _filename: Optional[str] = None,
) -> QualityReport:
    """在固定高精度 Decimal context 中执行完整业务质量校验。

    Args:
        path: 已 durable 的 facts partial 或 final 普通文件。
        summary: 可选 schema/sequence/SHA 摘要。
        expected_run_id: 可选固定 canonical run ID。
        _directory_descriptor: 内部固定目录句柄。
        _filename: 与内部目录句柄配套的固定 basename。

    Returns:
        QualityReport: 只有所有业务恒等式通过时才返回的不可变报告。

    Raises:
        ExecutionFactsError: schema、完整性、质量、安全或文件能力失败。
    """

    with localcontext() as decimal_context:
        decimal_context.prec = MAX_DECIMAL_DIGITS * 4
        return _validate_execution_facts_quality_exact(
            path,
            summary=summary,
            expected_run_id=expected_run_id,
            _directory_descriptor=_directory_descriptor,
            _filename=_filename,
        )


def _validate_execution_facts_quality_exact(
    path: Union[str, Path],
    *,
    summary: Optional[FactsFileSummary] = None,
    expected_run_id: Optional[str] = None,
    _directory_descriptor: Optional[int] = None,
    _filename: Optional[str] = None,
) -> QualityReport:
    """流式校验每日快照、订单链、费用现金与资产恒等式。

    Args:
        path: 已 durable 的 facts partial 或 final 普通文件。
        summary: 可选的 schema/sequence/SHA 摘要，避免重复计算该层结果。
        expected_run_id: 可选固定 canonical run ID。
        _directory_descriptor: 内部固定目录句柄；writer/consumer 防 TOCTOU 使用。
        _filename: 与内部目录句柄配套的固定 basename。

    Returns:
        QualityReport: 仅在全部业务检查通过时构造的不可变 PASSED 报告。

    Raises:
        ExecutionFactsIntegrityError: 零事实、缺日、关系缺失、恒等式或 reconcile 失败。
        ExecutionFactsValidationError: 事实 schema 或运行身份非法。
        ExecutionFactsSecurityError: 制品类型或 secret 扫描失败。
    """

    facts_path = Path(path)
    checked_summary = summary or validate_facts_file(
        facts_path,
        expected_run_id=expected_run_id,
        _directory_descriptor=_directory_descriptor,
        _filename=_filename,
    )
    if checked_summary.record_count == 0:
        _raise_quality_failure("零事实运行不得发布 complete")
    if expected_run_id is not None:
        expected_run_id = validate_run_id(expected_run_id)
    _scan_artifact_for_secrets(
        facts_path,
        maximum_bytes=MAX_FACTS_FILE_BYTES,
        maximum_line_bytes=MAX_FACT_LINE_BYTES,
        directory_descriptor=_directory_descriptor,
        filename=_filename,
    )

    business_dates: Set[str] = set()
    account_daily: Dict[Tuple[str, str], Dict[str, Decimal]] = {}
    daily_performance: Dict[Tuple[str, str], Dict[str, Decimal]] = {}
    position_totals: DefaultDict[Tuple[str, str], Decimal] = defaultdict(Decimal)
    position_keys: Set[Tuple[str, str, str]] = set()
    account_references: Set[Tuple[str, str]] = set()
    orders: Dict[str, Dict[str, Any]] = {}
    order_events: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    reservations: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    fills: Dict[str, Dict[str, Any]] = {}
    fills_by_order: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    fees: List[Dict[str, Any]] = []
    principal_ledger: DefaultDict[str, Decimal] = defaultdict(Decimal)
    principal_ledger_orders: DefaultDict[str, Set[str]] = defaultdict(set)
    fee_ledger: DefaultDict[Tuple[str, str, str], Decimal] = defaultdict(Decimal)
    last_cash_balance: Dict[Tuple[str, str], Decimal] = {}
    cash_entry_ids: Set[str] = set()
    reconciliation_count = 0
    observed_run_id: Optional[str] = None
    observed_records = 0
    observed_bytes = 0
    observed_digest = hashlib.sha256()

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    if _directory_descriptor is not None:
        if _filename is None:
            raise ExecutionFactsSecurityError("dir_fd quality 校验缺少 basename")
        _ensure_regular_file_at(_directory_descriptor, _filename)
        quality_descriptor = os.open(_filename, flags, dir_fd=_directory_descriptor)
    else:
        _ensure_regular_file(facts_path)
        quality_descriptor = os.open(str(facts_path), flags)
    with os.fdopen(quality_descriptor, "rb") as stream:
        while True:
            raw_line = stream.readline(MAX_FACT_LINE_BYTES + 1)
            if not raw_line:
                break
            observed_records += 1
            observed_bytes += len(raw_line)
            observed_digest.update(raw_line)
            if len(raw_line) > MAX_FACT_LINE_BYTES:
                raise ExecutionFactsIntegrityError("质量校验发现超长 NDJSON 行")
            value = _loads_json_no_duplicates(raw_line, "quality facts")
            if not isinstance(value, dict):
                raise ExecutionFactsIntegrityError("质量校验要求事实 envelope 为 object")
            run_id = validate_run_id(value.get("run_id"))
            if observed_run_id is None:
                observed_run_id = run_id
            if run_id != observed_run_id or (
                expected_run_id is not None and run_id != expected_run_id
            ):
                _raise_quality_failure("运行身份在事实流中漂移")
            trade_date = value.get("trade_date")
            if not isinstance(trade_date, str):
                _raise_quality_failure("事实缺少业务日")
            business_dates.add(trade_date)
            event_type = _coerce_event_type(value.get("event_type"))
            payload = value.get("payload")
            if not isinstance(payload, dict):
                _raise_quality_failure("payload 不是 object")

            account_id = payload.get("account_id")
            if isinstance(account_id, str):
                account_references.add((trade_date, account_id))

            if event_type is EventType.ACCOUNT_DAILY:
                key = (trade_date, payload["account_id"])
                if key in account_daily:
                    _raise_quality_failure("同一账户业务日出现重复 account_daily")
                cash = _quality_decimal(payload, "cash")
                available_cash = _quality_decimal(payload, "available_cash")
                locked_cash = _quality_decimal(payload, "locked_cash")
                positions_value = _quality_decimal(payload, "positions_value")
                total_value = _quality_decimal(payload, "total_value")
                adjustments = Decimal(payload.get("adjustments", "0"))
                if min(cash, available_cash, locked_cash, positions_value, total_value) < 0:
                    _raise_quality_failure("账户日快照包含负资产")
                if available_cash + locked_cash != cash:
                    _raise_quality_failure("available_cash、locked_cash 与 cash 不平")
                if cash + positions_value + adjustments != total_value:
                    _raise_quality_failure("现金、持仓、调整与总资产不平")
                account_daily[key] = {
                    "cash": cash,
                    "positions_value": positions_value,
                    "total_value": total_value,
                }
            elif event_type is EventType.POSITION_DAILY:
                key = (trade_date, payload["account_id"])
                position_key = (trade_date, payload["account_id"], payload["security"])
                if position_key in position_keys:
                    _raise_quality_failure("同一账户业务日标的出现多个 canonical 身份")
                position_keys.add(position_key)
                quantity = _quality_decimal(payload, "quantity")
                available_quantity = _quality_decimal(payload, "available_quantity")
                price = _quality_decimal(payload, "price")
                market_value = _quality_decimal(payload, "market_value")
                if quantity < 0 or available_quantity < 0 or available_quantity > quantity:
                    _raise_quality_failure("持仓数量或可用数量不合法")
                if price < 0 or market_value != quantity * price:
                    _raise_quality_failure("持仓市值与数量价格不平")
                position_totals[key] += market_value
            elif event_type is EventType.DAILY_PERFORMANCE:
                key = (trade_date, payload["account_id"])
                if key in daily_performance:
                    _raise_quality_failure("同一账户业务日出现重复 daily_performance")
                daily_performance[key] = {
                    "total_value": _quality_decimal(payload, "total_value"),
                    "net_asset_value": _quality_decimal(payload, "net_asset_value"),
                }
            elif event_type is EventType.ORDER_INTENT:
                order_id = payload["order_id"]
                if order_id in orders:
                    _raise_quality_failure("order_intent 身份重复")
                if _quality_decimal(payload, "requested_quantity") <= 0:
                    _raise_quality_failure("订单请求量必须为正")
                orders[order_id] = dict(payload)
            elif event_type is EventType.ORDER_EVENT:
                requested = _quality_decimal(payload, "requested_quantity")
                filled = _quality_decimal(payload, "filled_quantity")
                remaining = _quality_decimal(payload, "remaining_quantity")
                if requested <= 0 or filled < 0 or remaining < 0 or filled + remaining != requested:
                    _raise_quality_failure("订单状态数量恒等式失败")
                events = order_events[payload["order_id"]]
                if events and events[-1]["after_status"] != payload["before_status"]:
                    _raise_quality_failure("订单状态迁移不连续")
                events.append(dict(payload))
            elif event_type is EventType.RESERVATION:
                if _quality_decimal(payload, "value") < 0:
                    _raise_quality_failure("reservation value 不得为负")
                reservations[payload["order_id"]].append(dict(payload))
            elif event_type is EventType.FILL:
                fill_id = payload["fill_id"]
                if fill_id in fills:
                    _raise_quality_failure("fill_id 重复")
                quantity = _quality_decimal(payload, "quantity")
                price = _quality_decimal(payload, "price")
                amount = _quality_decimal(payload, "amount")
                if quantity <= 0 or price < 0 or amount != quantity * price:
                    _raise_quality_failure("成交数量、价格和金额不平")
                fill = dict(payload)
                fills[fill_id] = fill
                fills_by_order[payload["order_id"]].append(fill)
            elif event_type is EventType.FEE:
                if _quality_decimal(payload, "amount") < 0:
                    _raise_quality_failure("费用金额不得为负")
                fees.append(dict(payload))
            elif event_type is EventType.CASH_LEDGER:
                entry_id = payload["entry_id"]
                if entry_id in cash_entry_ids:
                    _raise_quality_failure("cash ledger entry_id 重复")
                cash_entry_ids.add(entry_id)
                amount = _quality_decimal(payload, "amount")
                balance = _quality_decimal(payload, "balance")
                last_cash_balance[(trade_date, payload["account_id"])] = balance
                if payload["category"] == "principal":
                    fill_id = payload.get("fill_id")
                    order_id = payload.get("order_id")
                    if not isinstance(fill_id, str) or not isinstance(order_id, str):
                        _raise_quality_failure("成交本金 cash ledger 必须关联 order_id 与 fill_id")
                    principal_ledger[fill_id] += amount
                    principal_ledger_orders[fill_id].add(order_id)
                elif payload["category"] == "fee":
                    order_id = payload.get("order_id")
                    fee_type = payload.get("fee_type")
                    if not isinstance(order_id, str) or not isinstance(fee_type, str):
                        _raise_quality_failure("费用 cash ledger 必须关联 order_id 与 fee_type")
                    fee_ledger[(order_id, str(payload.get("fill_id", "")), fee_type)] += amount
            elif event_type is EventType.RECONCILE_EVENT:
                if payload["status"] != "PASSED":
                    _raise_quality_failure("reconcile_event 明确失败")
                expected = _quality_decimal(payload, "expected")
                actual = _quality_decimal(payload, "actual")
                difference = _quality_decimal(payload, "difference")
                if expected != actual or difference != 0 or difference != actual - expected:
                    _raise_quality_failure("reconcile_event 数值不平")
                reconciliation_count += 1

    if observed_records != checked_summary.record_count:
        _raise_quality_failure("质量校验行数与 facts 摘要不一致")
    if (
        observed_bytes != checked_summary.byte_size
        or observed_digest.hexdigest() != checked_summary.sha256
    ):
        _raise_quality_failure("质量校验期间 facts 字节或 SHA 发生变化")
    if observed_run_id is None:
        _raise_quality_failure("零事实运行不得发布 complete")
    if not business_dates:
        _raise_quality_failure("运行缺少业务日")
    if reconciliation_count == 0:
        _raise_quality_failure("legacy reconciliation 为 NOT_RUN")
    if not account_daily or not daily_performance:
        _raise_quality_failure("缺少 account_daily 或 daily_performance")
    if set(account_daily) != set(daily_performance):
        _raise_quality_failure("每日账户与绩效账户范围不一致")
    if {trade_date for trade_date, _ in account_daily} != business_dates:
        _raise_quality_failure("至少一个业务日缺少账户快照")
    for key, account in account_daily.items():
        if position_totals[key] != account["positions_value"]:
            _raise_quality_failure("position_daily 合计与账户持仓市值不平")
        performance = daily_performance[key]
        if performance["total_value"] != account["total_value"]:
            _raise_quality_failure("daily_performance 与账户总资产不平")
        if performance["net_asset_value"] < 0:
            _raise_quality_failure("net_asset_value 不得为负")
        last_balance = last_cash_balance.get(key)
        if last_balance is not None and last_balance != account["cash"]:
            _raise_quality_failure("现金流水末值与账户现金不平")
    if not account_references.issubset(set(account_daily)):
        _raise_quality_failure("事实引用了缺少日终快照的账户")

    for order_id, order in orders.items():
        events = order_events.get(order_id, [])
        if not events:
            _raise_quality_failure("订单缺少 order_event")
        last_event = events[-1]
        terminal_status = last_event["after_status"]
        if terminal_status not in {
            OrderStatus.filled.value,
            OrderStatus.canceled.value,
            OrderStatus.rejected.value,
            OrderStatus.partly_canceled.value,
        }:
            _raise_quality_failure("订单未进入可收口终态")
        terminal_states = {
            OrderStatus.filled.value,
            OrderStatus.canceled.value,
            OrderStatus.rejected.value,
            OrderStatus.partly_canceled.value,
        }
        for event_index, event in enumerate(events):
            if event_index < len(events) - 1 and event["after_status"] in terminal_states:
                _raise_quality_failure("订单在终态之后仍发生状态迁移")
            if event_index:
                previous = events[event_index - 1]
                if Decimal(event["filled_quantity"]) < Decimal(previous["filled_quantity"]):
                    _raise_quality_failure("订单累计已成量发生回退")
                if Decimal(event["remaining_quantity"]) > Decimal(previous["remaining_quantity"]):
                    _raise_quality_failure("订单剩余量发生反向增加")
            if event.get("security") is not None and event["security"] != order["security"]:
                _raise_quality_failure("订单状态事件证券身份不一致")
            if (
                event.get("account_id") is not None
                and order.get("account_id") is not None
                and event["account_id"] != order["account_id"]
            ):
                _raise_quality_failure("订单状态事件账户身份不一致")
        requested = Decimal(order["requested_quantity"])
        if Decimal(last_event["requested_quantity"]) != requested:
            _raise_quality_failure("订单意图与状态请求量不一致")
        order_fills = fills_by_order.get(order_id, [])
        fill_quantity = sum((Decimal(fill["quantity"]) for fill in order_fills), Decimal(0))
        if fill_quantity != Decimal(last_event["filled_quantity"]):
            _raise_quality_failure("订单最终已成量与 fill 合计不一致")
        if terminal_status == OrderStatus.filled.value and fill_quantity != requested:
            _raise_quality_failure("filled 订单未完整成交")
        if (
            terminal_status in {OrderStatus.canceled.value, OrderStatus.rejected.value}
            and fill_quantity
        ):
            _raise_quality_failure("无成交终态订单含 fill")
        if terminal_status == OrderStatus.partly_canceled.value and not (
            0 < fill_quantity < requested
        ):
            _raise_quality_failure("partly_canceled 订单成交量不合法")
        actions = {item["action"] for item in reservations.get(order_id, [])}
        for reservation in reservations.get(order_id, []):
            if (
                reservation.get("security") is not None
                and reservation["security"] != order["security"]
            ):
                _raise_quality_failure("reservation 证券身份与订单不一致")
            if (
                reservation.get("account_id") is not None
                and order.get("account_id") is not None
                and reservation["account_id"] != order["account_id"]
            ):
                _raise_quality_failure("reservation 账户身份与订单不一致")
        if terminal_status == OrderStatus.filled.value and "consumed" not in actions:
            _raise_quality_failure("filled 订单缺少 reservation consumed")
        if (
            terminal_status
            in {
                OrderStatus.canceled.value,
                OrderStatus.rejected.value,
                OrderStatus.partly_canceled.value,
            }
            and "released" not in actions
        ):
            _raise_quality_failure("撤拒订单缺少 reservation released")

    unknown_order_references = (set(order_events) | set(reservations) | set(fills_by_order)) - set(
        orders
    )
    if unknown_order_references:
        _raise_quality_failure("状态、reservation 或 fill 引用了未知订单")
    fee_keys: DefaultDict[Tuple[str, str, str], Decimal] = defaultdict(Decimal)
    fill_fee_ids: Set[str] = set()
    for fee in fees:
        order_id = fee["order_id"]
        if order_id not in orders:
            _raise_quality_failure("费用引用未知订单")
        if fee.get("security") is not None and fee["security"] != orders[order_id]["security"]:
            _raise_quality_failure("费用证券身份与订单不一致")
        fill_id = str(fee.get("fill_id", ""))
        if fill_id:
            fee_fill = fills.get(fill_id)
            if fee_fill is None or fee_fill["order_id"] != order_id:
                _raise_quality_failure("费用引用未知或异单 fill")
            fill_fee_ids.add(fill_id)
        fee_keys[(order_id, fill_id, fee["fee_type"])] += Decimal(fee["amount"])
    for fill_id, fill in fills.items():
        fill_order = orders.get(fill["order_id"])
        if fill_order is None:
            _raise_quality_failure("fill 引用未知订单")
        if fill["security"] != fill_order["security"] or fill["side"] != fill_order["side"]:
            _raise_quality_failure("fill 与订单证券或方向不一致")
        if (
            fill.get("account_id") is not None
            and fill_order.get("account_id") is not None
            and fill["account_id"] != fill_order["account_id"]
        ):
            _raise_quality_failure("fill 与订单账户身份不一致")
        expected_principal = Decimal(fill["amount"])
        if fill["side"] == "BUY":
            expected_principal = -expected_principal
        if principal_ledger[fill_id] != expected_principal:
            _raise_quality_failure("fill 与本金 cash ledger 不平")
        if principal_ledger_orders[fill_id] != {fill["order_id"]}:
            _raise_quality_failure("fill 与本金 cash ledger 订单关系不一致")
        if fill_id not in fill_fee_ids:
            _raise_quality_failure("fill 缺少显式费用事实")
    if set(principal_ledger) - set(fills):
        _raise_quality_failure("本金 cash ledger 引用了未知 fill")
    if set(fee_ledger) != set(fee_keys):
        _raise_quality_failure("费用事实与费用 cash ledger 关系不完整")
    for fee_key, fee_amount in fee_keys.items():
        if fee_ledger[fee_key] != -fee_amount:
            _raise_quality_failure("费用事实与现金扣减金额不平")

    return QualityReport(
        run_id=observed_run_id,
        facts_sha256=checked_summary.sha256,
        event_counts=tuple(
            (event_type.value, checked_summary.event_counts[event_type.value])
            for event_type in EventType
        ),
        business_date_start=min(business_dates),
        business_date_end=max(business_dates),
        account_day_count=len(account_daily),
        reconciliation_count=reconciliation_count,
    )


def _normalize_price_basis(value: Mapping[str, Any]) -> Dict[str, Any]:
    """验证并序列化 writer 接收的多日运行级价格策略。

    Args:
        value: 可由每个事实 trade_date 唯一推导参考日的 8 字段 mapping。

    Returns:
        Dict[str, Any]: 与 EffectivePriceBasis 不变量一致的 canonical policy。

    Raises:
        ExecutionFactsValidationError: 字段、复权策略、参考日或业务日范围非法。
        ExecutionFactsSecurityError: provider 疑似包含秘密。
    """

    if not isinstance(value, Mapping):
        raise ExecutionFactsValidationError("price_basis 必须是 mapping")
    required = {
        "use_real_price",
        "fq",
        "provider",
        "business_timezone",
        "reference_policy",
        "configured_ref_date",
        "business_date_start",
        "business_date_end",
    }
    if set(value) != required:
        raise ExecutionFactsValidationError("price_basis 字段必须精确匹配 V1")
    use_real_price = value["use_real_price"]
    if not isinstance(use_real_price, bool):
        raise ExecutionFactsValidationError("price_basis.use_real_price 必须是 bool")
    fq = value["fq"]
    if fq not in {"none", "pre"}:
        raise ExecutionFactsValidationError("price_basis.fq 必须是 none/pre")
    provider = _validate_identifier(value["provider"], "price_basis.provider")
    if value["business_timezone"] != "Asia/Shanghai":
        raise ExecutionFactsValidationError("price_basis.business_timezone 必须是 Asia/Shanghai")
    reference_policy = value["reference_policy"]
    if reference_policy not in {
        "not_applicable",
        "current_trade_date",
        "min_configured_and_current_trade_date",
    }:
        raise ExecutionFactsValidationError("price_basis.reference_policy 不受支持")
    configured_reference = value["configured_ref_date"]
    if configured_reference is not None:
        try:
            configured_reference = date.fromisoformat(str(configured_reference)).isoformat()
        except (TypeError, ValueError) as exc:
            raise ExecutionFactsValidationError("configured_ref_date 必须是 ISO 日期或 None") from exc
    business_dates: List[str] = []
    for field_name in ("business_date_start", "business_date_end"):
        try:
            business_dates.append(date.fromisoformat(str(value[field_name])).isoformat())
        except (TypeError, ValueError) as exc:
            raise ExecutionFactsValidationError(
                "price_basis.{0} 必须是 ISO 日期".format(field_name)
            ) from exc
    business_date_start, business_date_end = business_dates
    if business_date_start > business_date_end:
        raise ExecutionFactsValidationError("price_basis 业务日范围起止颠倒")
    if reference_policy == "not_applicable":
        if use_real_price or fq != "none" or configured_reference is not None:
            raise ExecutionFactsValidationError("not_applicable 必须匹配未复权 EffectivePriceBasis")
    elif reference_policy == "current_trade_date":
        if not use_real_price or fq != "pre" or configured_reference is not None:
            raise ExecutionFactsValidationError("current_trade_date 必须匹配动态前复权口径")
    else:
        if not use_real_price or fq != "pre" or configured_reference is None:
            raise ExecutionFactsValidationError("min_configured_and_current_trade_date 必须携带显式参考日")
    return {
        "use_real_price": use_real_price,
        "fq": fq,
        "provider": provider,
        "business_timezone": "Asia/Shanghai",
        "reference_policy": reference_policy,
        "configured_ref_date": configured_reference,
        "business_date_start": business_date_start,
        "business_date_end": business_date_end,
    }


def derive_pre_factor_ref_date(price_basis: Mapping[str, Any], trade_date: str) -> Optional[str]:
    """按运行级策略为单条事实唯一推导有效前复权参考日。

    Args:
        price_basis: 已规范或待规范的 8 字段运行级策略。
        trade_date: 当前事实的 Asia/Shanghai ISO 业务日。

    Returns:
        Optional[str]: 未复权为 None；前复权为不晚于业务日的 ISO 日期。

    Raises:
        ExecutionFactsValidationError: 策略、业务日或范围不合法。
    """

    policy = _normalize_price_basis(price_basis)
    return _derive_pre_factor_ref_date_from_normalized(policy, trade_date)


def _derive_pre_factor_ref_date_from_normalized(
    policy: Mapping[str, Any], trade_date: str
) -> Optional[str]:
    """从已规范的运行级策略低开销推导单日参考日。

    Args:
        policy: ``_normalize_price_basis`` 已返回的固定 8 字段结构。
        trade_date: 当前事实的 ISO 业务日。

    Returns:
        Optional[str]: 当前业务日唯一对应的有效参考日。

    Raises:
        ExecutionFactsValidationError: 业务日非法或超出策略范围。
    """

    try:
        current_date = date.fromisoformat(trade_date)
    except (TypeError, ValueError) as exc:
        raise ExecutionFactsValidationError("事实 trade_date 必须是 ISO 日期") from exc
    if not (
        date.fromisoformat(policy["business_date_start"])
        <= current_date
        <= date.fromisoformat(policy["business_date_end"])
    ):
        raise ExecutionFactsValidationError("事实 trade_date 超出价格策略业务日范围")
    reference_policy = policy["reference_policy"]
    if reference_policy == "not_applicable":
        return None
    if reference_policy == "current_trade_date":
        return current_date.isoformat()
    configured = date.fromisoformat(policy["configured_ref_date"])
    return min(configured, current_date).isoformat()


def _validate_manifest(value: Dict[str, Any], expected_run_id: Optional[str]) -> None:
    """验证发布 manifest 的固定字段、类型和成功质量语义。

    Args:
        value: 已解析 manifest object。
        expected_run_id: 可选运行 UUID。

    Returns:
        None: 全部字段通过时无返回值。

    Raises:
        ExecutionFactsValidationError: schema、字段或时间不符合 V1。
        ExecutionFactsIntegrityError: run ID、facts 摘要或质量状态异常。
    """

    required = {
        "schema_version",
        "producer",
        "run_id",
        "facts",
        "started_at",
        "finished_at",
        "calculation_version",
        "price_basis",
        "quality",
    }
    if set(value) != required:
        raise ExecutionFactsValidationError("manifest 字段必须精确匹配 execution-facts/v1")
    if value["schema_version"] != SCHEMA_VERSION:
        raise ExecutionFactsValidationError("未知 manifest schema")
    run_id = validate_run_id(value["run_id"])
    if expected_run_id is not None and run_id != validate_run_id(expected_run_id):
        raise ExecutionFactsIntegrityError("manifest run_id 与期望运行不一致")
    producer = value["producer"]
    if not isinstance(producer, dict) or set(producer) != {"name", "version"}:
        raise ExecutionFactsValidationError("manifest producer 字段不合法")
    if producer["name"] != "bullet-trade":
        raise ExecutionFactsValidationError("manifest producer.name 必须是 bullet-trade")
    _validate_identifier(producer["version"], "producer.version")
    _validate_contract_version(value["calculation_version"], "calculation_version")
    started = _validate_utc_text(value["started_at"], "started_at")
    finished = _validate_utc_text(value["finished_at"], "finished_at")
    if finished < started:
        raise ExecutionFactsValidationError("finished_at 不得早于 started_at")
    price_basis = value["price_basis"]
    if not isinstance(price_basis, dict):
        raise ExecutionFactsValidationError("manifest price_basis 必须是 object")
    reconstructed_price_basis = _normalize_price_basis(
        {
            "use_real_price": price_basis.get("use_real_price"),
            "fq": price_basis.get("fq"),
            "provider": price_basis.get("provider"),
            "business_timezone": price_basis.get("business_timezone"),
            "reference_policy": price_basis.get("reference_policy"),
            "configured_ref_date": price_basis.get("configured_ref_date"),
            "business_date_start": price_basis.get("business_date_start"),
            "business_date_end": price_basis.get("business_date_end"),
        }
    )
    if reconstructed_price_basis != price_basis:
        raise ExecutionFactsValidationError("manifest price_basis 不是 canonical V1 表示")
    facts = value["facts"]
    facts_required = {
        "path",
        "record_count",
        "byte_size",
        "sha256",
        "first_sequence",
        "last_sequence",
    }
    if not isinstance(facts, dict) or set(facts) != facts_required:
        raise ExecutionFactsValidationError("manifest facts 摘要字段不合法")
    if facts["path"] != FACTS_FILENAME:
        raise ExecutionFactsValidationError("manifest facts.path 必须是固定文件名")
    for name in ("record_count", "byte_size"):
        if not isinstance(facts[name], int) or isinstance(facts[name], bool) or facts[name] < 0:
            raise ExecutionFactsValidationError("manifest facts.{0} 必须是非负整数".format(name))
    if not isinstance(facts["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", facts["sha256"]):
        raise ExecutionFactsValidationError("manifest facts.sha256 格式不合法")
    expected_first = 1 if facts["record_count"] else None
    expected_last = facts["record_count"] if facts["record_count"] else None
    if facts["first_sequence"] != expected_first or facts["last_sequence"] != expected_last:
        raise ExecutionFactsIntegrityError("manifest facts sequence 范围与行数不一致")
    quality = value["quality"]
    quality_required = {
        "status",
        "checks_version",
        "event_counts",
        "legacy_reconciliation",
        "audit",
    }
    if not isinstance(quality, dict) or set(quality) != quality_required:
        raise ExecutionFactsValidationError("manifest quality 字段不合法")
    if quality["status"] != "PASSED":
        raise ExecutionFactsIntegrityError("只有 PASSED quality manifest 可被消费")
    _validate_contract_version(quality["checks_version"], "quality.checks_version")
    event_counts = quality["event_counts"]
    if not isinstance(event_counts, dict):
        raise ExecutionFactsValidationError("quality.event_counts 必须是 object")
    if set(event_counts) != {event_type.value for event_type in EventType}:
        raise ExecutionFactsValidationError("quality.event_counts 必须显式包含全部 V1 事件类型")
    for event_name, count in event_counts.items():
        _coerce_event_type(event_name)
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ExecutionFactsValidationError("quality.event_counts 必须是非负整数")
    reconciliation = quality["legacy_reconciliation"]
    if not isinstance(reconciliation, dict) or set(reconciliation) != {"status", "version"}:
        raise ExecutionFactsValidationError("legacy_reconciliation 字段不合法")
    if reconciliation["status"] != "PASSED":
        raise ExecutionFactsIntegrityError("legacy reconciliation 未通过时不得发布成功 manifest")
    _validate_contract_version(reconciliation["version"], "legacy_reconciliation.version")
    audit = quality["audit"]
    audit_required = {
        "facts_sha256",
        "business_date_start",
        "business_date_end",
        "account_day_count",
        "reconciliation_count",
    }
    if not isinstance(audit, dict) or set(audit) != audit_required:
        raise ExecutionFactsValidationError("quality.audit 字段不合法")
    if audit["facts_sha256"] != facts["sha256"]:
        raise ExecutionFactsIntegrityError("quality report 未绑定当前 facts SHA")
    if (
        audit["business_date_start"] != price_basis["business_date_start"]
        or audit["business_date_end"] != price_basis["business_date_end"]
    ):
        raise ExecutionFactsIntegrityError("quality report 与价格策略业务日范围不一致")
    for field_name in ("account_day_count", "reconciliation_count"):
        if (
            not isinstance(audit[field_name], int)
            or isinstance(audit[field_name], bool)
            or audit[field_name] < 1
        ):
            raise ExecutionFactsValidationError("quality.audit 计数必须是正整数")


def _validate_published_artifacts(
    directory: Path,
    *,
    expected_run_id: Optional[str] = None,
    manifest: Optional[Dict[str, Any]] = None,
    directory_descriptor: Optional[int] = None,
) -> Dict[str, Any]:
    """复验 manifest 与 facts 内容，不判断外层发布 guard。

    Args:
        directory: 已确认安全的 facts 与 manifest 所在目录。
        expected_run_id: 可选期望运行 UUID。
        manifest: 可选已从同一目录安全读取的 manifest，供 complete guard 绑定校验复用。
        directory_descriptor: 可选固定目录句柄，提供时所有 leaf 读取均使用 dir_fd。

    Returns:
        Dict[str, Any]: 内容和摘要通过校验的 manifest。

    Raises:
        ExecutionFactsValidationError: schema 或字段不合法。
        ExecutionFactsIntegrityError: facts 摘要、事件计数或身份不一致。
        ExecutionFactsSecurityError: facts 或 manifest 文件类型不安全。
    """

    if manifest is None:
        manifest_value = (
            _read_json_regular_file_at(directory_descriptor, MANIFEST_FILENAME)
            if directory_descriptor is not None
            else _read_json_regular_file(directory / MANIFEST_FILENAME)
        )
    else:
        manifest_value = manifest
    _validate_manifest(manifest_value, expected_run_id)
    facts_meta = manifest_value["facts"]
    summary = validate_facts_file(
        directory / FACTS_FILENAME,
        expected_run_id=manifest_value["run_id"],
        expected_record_count=facts_meta["record_count"],
        expected_byte_size=facts_meta["byte_size"],
        expected_sha256=facts_meta["sha256"],
        _directory_descriptor=directory_descriptor,
        _filename=FACTS_FILENAME if directory_descriptor is not None else None,
    )
    if summary.first_sequence != facts_meta["first_sequence"]:
        raise ExecutionFactsIntegrityError("manifest first_sequence 与 facts 不一致")
    if summary.last_sequence != facts_meta["last_sequence"]:
        raise ExecutionFactsIntegrityError("manifest last_sequence 与 facts 不一致")
    if summary.event_counts != manifest_value["quality"]["event_counts"]:
        raise ExecutionFactsIntegrityError("manifest event_counts 与 facts 不一致")
    quality_report = validate_execution_facts_quality(
        directory / FACTS_FILENAME,
        summary=summary,
        expected_run_id=manifest_value["run_id"],
        _directory_descriptor=directory_descriptor,
        _filename=FACTS_FILENAME if directory_descriptor is not None else None,
    )
    if quality_report.as_manifest_dict() != manifest_value["quality"]:
        raise ExecutionFactsIntegrityError("manifest quality 与内部复验报告不一致")
    return manifest_value


def _reject_incomplete_publication(directory: Path) -> None:
    """拒绝 incomplete guard 或任一 staging partial 尚存的发布。

    Args:
        directory: 已确认安全的发布目录。

    Returns:
        None: 没有未完成标志或 partial 时无返回值。

    Raises:
        ExecutionFactsSecurityError: 未完成制品是 symlink 或非普通文件。
        ExecutionFactsIntegrityError: 发现未完成标志或 partial。
    """

    paths = (
        directory / PUBLISH_INCOMPLETE_FILENAME,
        directory / FACTS_PARTIAL_FILENAME,
        directory / MANIFEST_PARTIAL_FILENAME,
    )
    for path in paths:
        if path.is_symlink():
            raise ExecutionFactsSecurityError("发布未完成制品不得是 symlink: {0}".format(path))
        if path.exists():
            _ensure_regular_file(path)
            raise ExecutionFactsIntegrityError("发布未完成，consumer 拒绝读取: {0}".format(path.name))


def _reject_incomplete_publication_at(directory_descriptor: int) -> None:
    """在固定目录 descriptor 下拒绝 incomplete 与任一 partial。

    Args:
        directory_descriptor: consumer 已固定的发布目录句柄。

    Returns:
        None: 三个未完成 basename 均不存在时无返回值。

    Raises:
        ExecutionFactsSecurityError: 未完成目录项类型不安全。
        ExecutionFactsIntegrityError: 任一未完成目录项存在。
    """

    for filename in (
        PUBLISH_INCOMPLETE_FILENAME,
        FACTS_PARTIAL_FILENAME,
        MANIFEST_PARTIAL_FILENAME,
    ):
        if _exists_at(directory_descriptor, filename):
            _ensure_regular_file_at(directory_descriptor, filename)
            raise ExecutionFactsIntegrityError("发布未完成，consumer 拒绝读取")


def validate_published_execution_facts(
    output_dir: Union[str, Path], *, expected_run_id: Optional[str] = None
) -> Dict[str, Any]:
    """在一个固定目录 capability 上复验 complete、manifest 与 facts。

    Args:
        output_dir: 固定 facts、manifest 与发布 guard 所在目录。
        expected_run_id: 可选期望运行 UUID。

    Returns:
        Dict[str, Any]: 同一固定 directory descriptor 在线性化校验点的一致 manifest 快照。

    Raises:
        ExecutionFactsValidationError: schema、字段或 complete guard 不合法。
        ExecutionFactsIntegrityError: 发布未完成、制品摘要或身份不一致。
        ExecutionFactsSecurityError: 目录或文件类型不安全。

    Notes:
        consumer 防止读取期间的 symlink 重定向、目录混包与可信根外读取；调用方必须保证发布
        目录由可信主体独占并在成功发布后不可变。本函数不承诺同权限恶意写者在线性化点之后
        不重命名公开 pathname 或修改 leaf，后续重新打开仍会按 guard 和 SHA 失败关闭。
    """

    directory = Path(output_dir)
    if directory.is_symlink() or not directory.is_dir():
        raise ExecutionFactsSecurityError("发布目录必须是非 symlink 目录")
    _assert_no_symlink_ancestors(directory)
    requested_metadata = directory.lstat()
    try:
        directory_descriptor = _open_directory_chain_no_symlinks(directory)
    except OSError as exc:
        raise ExecutionFactsSecurityError("发布目录无法绑定可信 dir_fd") from exc
    try:
        descriptor_metadata = os.fstat(directory_descriptor)
        expected_identity = (requested_metadata.st_dev, requested_metadata.st_ino)
        if (descriptor_metadata.st_dev, descriptor_metadata.st_ino) != expected_identity:
            raise ExecutionFactsSecurityError("发布目录在 consumer 绑定期间被替换")
        _reject_incomplete_publication_at(directory_descriptor)
        if not _exists_at(directory_descriptor, PUBLISH_COMPLETE_FILENAME):
            raise ExecutionFactsIntegrityError("发布未完成：缺少 complete guard")
        _ensure_regular_file_at(directory_descriptor, PUBLISH_COMPLETE_FILENAME)
        marker = _read_json_regular_file_at(directory_descriptor, PUBLISH_COMPLETE_FILENAME)
        _scan_artifact_for_secrets(
            directory / PUBLISH_COMPLETE_FILENAME,
            maximum_bytes=MAX_MANIFEST_FILE_BYTES,
            maximum_line_bytes=MAX_MANIFEST_FILE_BYTES,
            directory_descriptor=directory_descriptor,
            filename=PUBLISH_COMPLETE_FILENAME,
        )
        _scan_artifact_for_secrets(
            directory / MANIFEST_FILENAME,
            maximum_bytes=MAX_MANIFEST_FILE_BYTES,
            maximum_line_bytes=MAX_MANIFEST_FILE_BYTES,
            directory_descriptor=directory_descriptor,
            filename=MANIFEST_FILENAME,
        )
        _scan_artifact_for_secrets(
            directory / FACTS_FILENAME,
            maximum_bytes=MAX_FACTS_FILE_BYTES,
            maximum_line_bytes=MAX_FACT_LINE_BYTES,
            directory_descriptor=directory_descriptor,
            filename=FACTS_FILENAME,
        )
        manifest_value, manifest_sha256 = _read_json_regular_file_with_sha_at(
            directory_descriptor, MANIFEST_FILENAME
        )
        manifest = _validate_published_artifacts(
            directory,
            expected_run_id=expected_run_id,
            manifest=manifest_value,
            directory_descriptor=directory_descriptor,
        )
        _validate_publish_marker(marker, manifest, manifest_sha256)
        _reject_incomplete_publication_at(directory_descriptor)
        if directory.is_symlink() or not directory.is_dir():
            raise ExecutionFactsSecurityError("发布目录在 consumer 读取期间被替换")
        final_metadata = directory.lstat()
        if (final_metadata.st_dev, final_metadata.st_ino) != expected_identity:
            raise ExecutionFactsSecurityError("发布目录身份在 consumer 读取期间变化")
        return manifest
    finally:
        os.close(directory_descriptor)


class ExecutionFactsWriter:
    """单运行、有界缓冲、append-only 的 execution-facts/v1 writer。

    writer 在构造时排他创建 partial；append 只做规范化与流式持久化，finalize 重读校验后
    先发布 facts 与 manifest，复验后最后切换 complete guard。实例不参与交易决策，也不提供
    覆盖能力；incomplete、partial 或缺少 complete 的目录永远不可消费。

    writer 的安全能力是构造时固定的目录 descriptor，而不是可变 pathname。调用方必须独占
    trusted root 及父目录、串行化同一 run，并把成功包视为不可变；同权限恶意进程在线性化点
    之后重命名 pathname 或改写 regular leaf 不属于 V1 威胁模型。
    """

    def __init__(
        self,
        *,
        output_dir: Union[str, Path],
        run_id: str,
        producer_version: str,
        calculation_version: str,
        price_basis: Mapping[str, Any],
        trusted_output_root: Union[str, Path],
        buffer_size_bytes: int = 64 * 1024,
        started_at: Optional[datetime] = None,
    ) -> None:
        """创建运行独享 partial writer。

        Args:
            output_dir: 固定机器事实目录，不拼接 run ID。
            run_id: canonical 小写 UUID。
            producer_version: 当前 BulletTrade 版本。
            calculation_version: 引擎记账/计算合同版本。
            price_basis: 只描述价格口径的固定字段 mapping。
            buffer_size_bytes: 内存缓冲上限，最少 1 字节。
            started_at: 可注入的 aware UTC 起始时间；默认当前 UTC。
            trusted_output_root: 调用方显式提供且独占管理的既有可信数据根；output_dir 必须位于
                其内且全链无 symlink，同一 run 由上层串行发布。

        Raises:
            ExecutionFactsValidationError: 身份、元数据或缓冲参数非法。
            ExecutionFactsConflictError: 目录已有任何事实制品。
            ExecutionFactsAlreadyPublishedError: 目录已有完整发布。
            ExecutionFactsIntegrityError: 既有发布已损坏。
            ExecutionFactsSecurityError: 路径或元数据触发安全边界。

        Side Effects:
            创建输出目录和权限不超过 0600 的 ``facts.ndjson.partial``。
        """

        self.run_id = validate_run_id(run_id)
        self.producer_version = _validate_identifier(producer_version, "producer_version")
        self.calculation_version = _validate_contract_version(
            calculation_version, "calculation_version"
        )
        self.price_basis = _normalize_price_basis(price_basis)
        if (
            not isinstance(buffer_size_bytes, int)
            or isinstance(buffer_size_bytes, bool)
            or buffer_size_bytes < 1
            or buffer_size_bytes > MAX_WRITER_BUFFER_BYTES
        ):
            raise ExecutionFactsValidationError("buffer_size_bytes 必须在冻结正整数预算内")
        self.buffer_size_bytes = buffer_size_bytes
        started_value = started_at or datetime.now(timezone.utc)
        self.started_at, _ = normalize_utc_datetime(started_value)
        _require_secure_directory_platform()
        requested_output = Path(output_dir)
        requested_root = Path(trusted_output_root)
        if ".." in requested_output.parts or ".." in requested_root.parts:
            raise ExecutionFactsSecurityError("机器事实路径禁止父目录穿越片段")
        _assert_no_symlink_ancestors(requested_root)
        _assert_no_symlink_ancestors(requested_output)
        if requested_root.is_symlink() or not requested_root.is_dir():
            raise ExecutionFactsSecurityError("可信数据根必须是预先存在的非 symlink 目录")
        self.trusted_output_root = requested_root.resolve(strict=True)
        self.output_dir = requested_output.resolve(strict=False)
        try:
            self.output_dir.relative_to(self.trusted_output_root)
        except ValueError as exc:
            raise ExecutionFactsSecurityError("机器事实输出目录逃逸可信数据根") from exc
        _ensure_safe_output_directory(self.output_dir)
        output_metadata = self.output_dir.lstat()
        self._output_directory_identity = (output_metadata.st_dev, output_metadata.st_ino)
        self.partial_path = self.output_dir / FACTS_PARTIAL_FILENAME
        self.facts_path = self.output_dir / FACTS_FILENAME
        self.manifest_partial_path = self.output_dir / MANIFEST_PARTIAL_FILENAME
        self.manifest_path = self.output_dir / MANIFEST_FILENAME
        self.publish_incomplete_path = self.output_dir / PUBLISH_INCOMPLETE_FILENAME
        self.publish_complete_path = self.output_dir / PUBLISH_COMPLETE_FILENAME
        self._directory_descriptor = _open_directory_chain_no_symlinks(self.output_dir)
        self._directory_finalizer = weakref.finalize(
            self,
            os.close,
            self._directory_descriptor,
        )
        self._guard_existing_artifacts()
        self._descriptor: Optional[int] = None
        try:
            self._descriptor = _open_exclusive_at(
                self._directory_descriptor,
                FACTS_PARTIAL_FILENAME,
            )
            self._assert_output_directory_identity()
        except BaseException:
            if self._descriptor is not None:
                os.close(self._descriptor)
                self._descriptor = None
            try:
                if _exists_at(self._directory_descriptor, FACTS_PARTIAL_FILENAME):
                    os.unlink(FACTS_PARTIAL_FILENAME, dir_fd=self._directory_descriptor)
                    _fsync_directory_descriptor(self._directory_descriptor)
            except OSError:
                pass
            raise
        self._buffer = bytearray()
        self._digest = hashlib.sha256()
        self._record_count = 0
        self._byte_size = 0
        self._event_counts: Counter[str] = Counter()
        self._fact_business_dates: Set[str] = set()
        self._observed_price_basis_dates: Set[str] = set()
        self._price_basis_failed = False
        self._closed = False
        self._failed = False

    @property
    def buffered_bytes(self) -> int:
        """返回当前内存缓冲字节数。

        Returns:
            int: 始终不超过 ``buffer_size_bytes`` 的字节数。
        """

        return len(self._buffer)

    def _guard_existing_artifacts(self) -> None:
        """拒绝覆盖已有 partial、facts、manifest 或发布 guard。

        Returns:
            None: 目录为空时无返回值。

        Raises:
            ExecutionFactsAlreadyPublishedError: 现有发布完整且 run ID 相同。
            ExecutionFactsConflictError: 现有发布属于其他运行或存在孤儿制品。
            ExecutionFactsIntegrityError: manifest 存在但事实已损坏。
        """

        if _exists_at(self._directory_descriptor, MANIFEST_FILENAME):
            manifest = validate_published_execution_facts(self.output_dir)
            if manifest["run_id"] == self.run_id:
                raise ExecutionFactsAlreadyPublishedError("同一 run_id 已发布，writer 不覆盖既有相同 SHA 制品")
            raise ExecutionFactsConflictError("输出目录已属于其他 run_id")
        for filename in (
            FACTS_PARTIAL_FILENAME,
            FACTS_FILENAME,
            MANIFEST_PARTIAL_FILENAME,
            PUBLISH_INCOMPLETE_FILENAME,
            PUBLISH_COMPLETE_FILENAME,
        ):
            if _exists_at(self._directory_descriptor, filename):
                raise ExecutionFactsConflictError("输出目录已有孤儿制品，拒绝覆盖")

    def _assert_output_directory_identity(self) -> None:
        """确认输出目录和祖先仍是构造时绑定的真实目录。

        Returns:
            None: 路径存在、全链无 symlink 且设备/inode 身份未变化时无返回值。

        Raises:
            ExecutionFactsSecurityError: 目录被替换、重命名、软链接接管或类型变化。
        """

        if self.output_dir.is_symlink() or not self.output_dir.is_dir():
            raise ExecutionFactsSecurityError("机器事实输出目录在运行中被替换")
        _ensure_safe_output_directory(self.output_dir)
        metadata = self.output_dir.lstat()
        if (metadata.st_dev, metadata.st_ino) != self._output_directory_identity:
            raise ExecutionFactsSecurityError("机器事实输出目录身份在运行中发生变化")
        descriptor_metadata = os.fstat(self._directory_descriptor)
        if (
            descriptor_metadata.st_dev,
            descriptor_metadata.st_ino,
        ) != self._output_directory_identity:
            raise ExecutionFactsSecurityError("机器事实固定目录 descriptor 身份异常")

    def _ensure_writable(self) -> int:
        """确认 writer 仍可追加，并返回文件描述符。

        Returns:
            int: 当前打开的 partial 文件描述符。

        Raises:
            ExecutionFactsError: writer 已关闭或此前写入失败。
        """

        if self._closed or self._descriptor is None:
            raise ExecutionFactsError("execution facts writer 已关闭")
        if self._failed:
            raise ExecutionFactsError("execution facts writer 已失败，禁止继续写入")
        self._assert_output_directory_identity()
        return self._descriptor

    def _flush_buffer(self) -> None:
        """把当前内存缓冲完整追加到 partial。

        Returns:
            None: 缓冲为空或写入成功后无返回值。

        Raises:
            OSError: 磁盘或文件句柄写入失败，并把 writer 标记为失败。

        Side Effects:
            写入 partial 并清空内存缓冲。
        """

        descriptor = self._ensure_writable()
        if not self._buffer:
            return
        try:
            _write_all(descriptor, bytes(self._buffer))
        except OSError:
            self._failed = True
            raise
        self._buffer.clear()

    def append(
        self,
        event_type: Union[EventType, str],
        *,
        authority_id: str,
        state_version: Union[int, str],
        occurred_at: datetime,
        payload: Mapping[str, Any],
        effective_price_basis: Optional[EffectivePriceBasis] = None,
    ) -> Dict[str, Any]:
        """规范化并追加一条权威事件，不允许调用方控制 sequence 或事件 UUID。

        Args:
            event_type: 固定 V1 事件类型。
            authority_id: 引擎权威对象身份，仅参与 UUIDv5。
            state_version: 当前权威状态版本。
            occurred_at: aware 业务发生时刻。
            payload: 对应事件的白名单字段。
            effective_price_basis: 可选的同业务日不可变价格口径证明。

        Returns:
            Dict[str, Any]: 已写入或已进入有界缓冲的 canonical envelope。

        Raises:
            ExecutionFactsValidationError: 事件、时间、数值、代码或字段不合法。
            OSError: 自动 flush 时磁盘写入失败。

        Side Effects:
            把 canonical NDJSON 行放入有界缓冲，必要时流式写入 partial。
        """

        self._ensure_writable()
        normalized_event_type = _coerce_event_type(event_type)
        occurred_text, trade_date = normalize_utc_datetime(occurred_at)
        _derive_pre_factor_ref_date_from_normalized(self.price_basis, trade_date)
        if effective_price_basis is not None:
            self.observe_effective_price_basis(
                effective_price_basis,
                expected_trade_date=trade_date,
            )
        normalized_payload = _normalize_payload(normalized_event_type, payload)
        sequence = self._record_count + 1
        source_event_id = build_source_event_id(
            self.run_id,
            normalized_event_type,
            authority_id,
            state_version,
            sequence,
        )
        envelope: Dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "source_event_id": source_event_id,
            "sequence": sequence,
            "event_type": normalized_event_type.value,
            "occurred_at": occurred_text,
            "trade_date": trade_date,
            "payload": normalized_payload,
        }
        encoded = _canonical_json_bytes(envelope, trailing_newline=True)
        if len(encoded) > MAX_FACT_LINE_BYTES:
            raise ExecutionFactsValidationError("事实行超过 execution-facts/v1 冻结单行预算")
        if self._byte_size + len(encoded) > MAX_FACTS_FILE_BYTES:
            raise ExecutionFactsValidationError("facts 超过 execution-facts/v1 冻结总字节预算")
        if len(encoded) > self.buffer_size_bytes:
            self._flush_buffer()
            try:
                _write_all(self._ensure_writable(), encoded)
            except OSError:
                self._failed = True
                raise
        else:
            if len(self._buffer) + len(encoded) > self.buffer_size_bytes:
                self._flush_buffer()
            self._buffer.extend(encoded)
        self._digest.update(encoded)
        self._record_count += 1
        self._byte_size += len(encoded)
        self._event_counts[normalized_event_type.value] += 1
        self._fact_business_dates.add(trade_date)
        return envelope

    def observe_effective_price_basis(
        self,
        basis: EffectivePriceBasis,
        *,
        expected_trade_date: Optional[str] = None,
    ) -> None:
        """校验一个业务日实例与运行级价格策略完全一致。

        Args:
            basis: current data、保护价与撮合共同使用的不可变实例。
            expected_trade_date: 从当前事实派生的可选业务日；防止跨日错绑。

        Returns:
            None: provider、fq、参考日与 policy 可唯一解释时记录该业务日。

        Raises:
            ExecutionFactsValidationError: 输入不是 EffectivePriceBasis 或业务日错绑。
            ExecutionFactsIntegrityError: provider、复权开关或有效参考日发生漂移。
        """

        self._ensure_writable()
        if not isinstance(basis, EffectivePriceBasis):
            raise ExecutionFactsValidationError("价格口径证明必须是 EffectivePriceBasis")
        trade_date = basis.business_time.date().isoformat()
        if expected_trade_date is not None and trade_date != expected_trade_date:
            self._price_basis_failed = True
            raise ExecutionFactsIntegrityError("价格口径业务日与事实 trade_date 不一致")
        try:
            expected_reference = _derive_pre_factor_ref_date_from_normalized(
                self.price_basis, trade_date
            )
        except ExecutionFactsValidationError:
            self._price_basis_failed = True
            raise
        actual_reference = (
            basis.pre_factor_ref_date.isoformat() if basis.pre_factor_ref_date is not None else None
        )
        if (
            basis.use_real_price != self.price_basis["use_real_price"]
            or basis.fq != self.price_basis["fq"]
            or basis.provider != self.price_basis["provider"]
            or actual_reference != expected_reference
        ):
            self._price_basis_failed = True
            raise ExecutionFactsIntegrityError("运行中 provider 或 EffectivePriceBasis 发生漂移")
        self._observed_price_basis_dates.add(trade_date)

    def flush(self, *, durable: bool = False) -> None:
        """把内存缓冲写入 partial，并可选择 fsync 到持久介质。

        Args:
            durable: True 时对 partial 文件描述符执行 fsync。

        Returns:
            None: 写入与可选持久化成功后无返回值。

        Raises:
            OSError: 写入或 fsync 失败，并把 writer 标记为失败。

        Side Effects:
            清空缓冲；durable 模式同步文件内容与元数据。
        """

        self._flush_buffer()
        if durable:
            try:
                os.fsync(self._ensure_writable())
            except OSError:
                self._failed = True
                raise

    def _close_descriptor(self) -> None:
        """关闭 partial 文件描述符且只执行一次。

        Returns:
            None: 已关闭时也无返回值。

        Side Effects:
            释放操作系统文件描述符。
        """

        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None
        self._closed = True

    def finalize(
        self,
        *,
        finished_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """校验 partial，排他发布 facts/manifest，最后切换 complete guard。

        Args:
            finished_at: 可注入的 aware 结束时刻；默认当前 UTC。

        Returns:
            Dict[str, Any]: 已在构造时固定目录 inode 上通过复验并完成 guard 切换的 manifest。

        Raises:
            ExecutionFactsValidationError: 质量或时间元数据不合法。
            ExecutionFactsIntegrityError: partial 或最终制品摘要不一致。
            ExecutionFactsConflictError: final 或 manifest 已存在。
            OSError: flush、fsync、链接、删除或目录持久化失败。

        Side Effects:
            成功时发布不可覆盖 facts、manifest 与 complete guard；失败时保留 incomplete 或
            partial，使任何残留 manifest 都无法通过 consumer validator。complete 成为唯一
            成功 guard 是终态线性化点，之后不再执行可能抛错的 pathname 检查。

        Notes:
            成功仅承诺提交到构造期固定的目录 capability；部署方必须独占 trusted root 和父
            目录、串行化同一 run，并在发布后保持 leaf 不可变。V1 不承诺同权限恶意写者在
            线性化点之后不重命名公开 pathname 或修改 regular leaf。
        """

        finished_value = finished_at or datetime.now(timezone.utc)
        finished_text, _ = normalize_utc_datetime(finished_value)
        if _validate_utc_text(finished_text, "finished_at") < _validate_utc_text(
            self.started_at, "started_at"
        ):
            raise ExecutionFactsValidationError("finished_at 不得早于 started_at")
        self.flush(durable=True)
        self._close_descriptor()
        self._assert_output_directory_identity()
        summary = validate_facts_file(
            self.partial_path,
            expected_run_id=self.run_id,
            expected_record_count=self._record_count,
            expected_byte_size=self._byte_size,
            expected_sha256=self._digest.hexdigest(),
            _directory_descriptor=self._directory_descriptor,
            _filename=FACTS_PARTIAL_FILENAME,
        )
        quality_report = validate_execution_facts_quality(
            self.partial_path,
            summary=summary,
            expected_run_id=self.run_id,
            _directory_descriptor=self._directory_descriptor,
            _filename=FACTS_PARTIAL_FILENAME,
        )
        if self._price_basis_failed:
            raise ExecutionFactsIntegrityError("运行中价格口径证明曾发生漂移")
        if self._observed_price_basis_dates != self._fact_business_dates:
            raise ExecutionFactsIntegrityError("至少一个事实业务日缺少 EffectivePriceBasis 证明")
        if (
            quality_report.business_date_start != self.price_basis["business_date_start"]
            or quality_report.business_date_end != self.price_basis["business_date_end"]
        ):
            raise ExecutionFactsIntegrityError("事实业务日范围与运行级价格策略不一致")
        _scan_artifact_for_secrets(
            self.partial_path,
            maximum_bytes=MAX_FACTS_FILE_BYTES,
            maximum_line_bytes=MAX_FACT_LINE_BYTES,
            directory_descriptor=self._directory_descriptor,
            filename=FACTS_PARTIAL_FILENAME,
        )
        self._assert_output_directory_identity()
        _publish_exclusive_at(
            self._directory_descriptor,
            FACTS_PARTIAL_FILENAME,
            FACTS_FILENAME,
        )
        manifest: Dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "producer": {"name": "bullet-trade", "version": self.producer_version},
            "run_id": self.run_id,
            "facts": {
                "path": FACTS_FILENAME,
                "record_count": summary.record_count,
                "byte_size": summary.byte_size,
                "sha256": summary.sha256,
                "first_sequence": summary.first_sequence,
                "last_sequence": summary.last_sequence,
            },
            "started_at": self.started_at,
            "finished_at": finished_text,
            "calculation_version": self.calculation_version,
            "price_basis": self.price_basis,
            "quality": quality_report.as_manifest_dict(),
        }
        _validate_manifest(manifest, self.run_id)
        encoded_manifest = _canonical_json_bytes(manifest, trailing_newline=True)
        if len(encoded_manifest) > MAX_MANIFEST_FILE_BYTES:
            raise ExecutionFactsValidationError("manifest 超过 execution-facts/v1 冻结字节预算")
        self._assert_output_directory_identity()
        descriptor = _open_exclusive_at(
            self._directory_descriptor,
            MANIFEST_PARTIAL_FILENAME,
        )
        try:
            _write_all(descriptor, encoded_manifest)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _scan_artifact_for_secrets(
            self.manifest_partial_path,
            maximum_bytes=MAX_MANIFEST_FILE_BYTES,
            maximum_line_bytes=MAX_MANIFEST_FILE_BYTES,
            directory_descriptor=self._directory_descriptor,
            filename=MANIFEST_PARTIAL_FILENAME,
        )
        publish_marker = _build_publish_marker(manifest, encoded_manifest)
        self._assert_output_directory_identity()
        _write_durable_publish_guard_at(
            self._directory_descriptor,
            PUBLISH_INCOMPLETE_FILENAME,
            publish_marker,
        )
        self._assert_output_directory_identity()
        _publish_exclusive_at(
            self._directory_descriptor,
            MANIFEST_PARTIAL_FILENAME,
            MANIFEST_FILENAME,
        )
        try:
            self._assert_output_directory_identity()
            published = _validate_published_artifacts(
                self.output_dir,
                expected_run_id=self.run_id,
                directory_descriptor=self._directory_descriptor,
            )
            if published != manifest:
                raise ExecutionFactsIntegrityError("发布 manifest 复读结果不一致")
        except BaseException:
            try:
                os.unlink(MANIFEST_FILENAME, dir_fd=self._directory_descriptor)
                _fsync_directory_descriptor(self._directory_descriptor)
            except OSError:
                pass
            raise
        self._assert_output_directory_identity()
        _commit_publish_guard_at(
            self._directory_descriptor,
            PUBLISH_INCOMPLETE_FILENAME,
            PUBLISH_COMPLETE_FILENAME,
        )
        return manifest

    def abort(self) -> None:
        """停止 writer 并尽力保留已接收事实的 partial 诊断。

        Returns:
            None: 重复调用也无返回值。

        Side Effects:
            尽力写出当前缓冲并关闭描述符；绝不发布 facts 或 manifest。
        """

        if self._closed:
            return
        if not self._failed:
            try:
                self._flush_buffer()
            except OSError:
                pass
        self._close_descriptor()

    def __enter__(self) -> "ExecutionFactsWriter":
        """返回当前 writer 供 with 语句使用。

        Returns:
            ExecutionFactsWriter: 当前实例。
        """

        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """离开 with 时仅 abort，不隐式宣称运行成功。

        Args:
            exc_type: 可选异常类型。
            exc_value: 可选异常值。
            traceback: 可选异常堆栈。

        Returns:
            None: 异常继续向上传播。

        Side Effects:
            调用 ``abort``，保留 partial 且不发布 manifest。
        """

        del exc_type, exc_value, traceback
        self.abort()


__all__ = [
    "EXECUTION_FACTS_V1_COMPATIBILITY",
    "EXECUTION_FACTS_V1_JSON_SCHEMA",
    "FACTS_FILENAME",
    "FACTS_PARTIAL_FILENAME",
    "MANIFEST_FILENAME",
    "MANIFEST_PARTIAL_FILENAME",
    "MAX_DECIMAL_CANONICAL_CHARS",
    "MAX_DECIMAL_DIGITS",
    "MAX_DECIMAL_EXPONENT_ABS",
    "MAX_DECIMAL_INPUT_CHARS",
    "MAX_DECIMAL_INTEGER_DIGITS",
    "MAX_DECIMAL_SCALE",
    "MAX_FACT_LINE_BYTES",
    "MAX_FACTS_FILE_BYTES",
    "MAX_MANIFEST_FILE_BYTES",
    "MAX_WRITER_BUFFER_BYTES",
    "PUBLISH_COMPLETE_FILENAME",
    "PUBLISH_INCOMPLETE_FILENAME",
    "PUBLISH_PROTOCOL_VERSION",
    "SCHEMA_VERSION",
    "EventType",
    "ExecutionFactsAlreadyPublishedError",
    "ExecutionFactsConflictError",
    "ExecutionFactsError",
    "ExecutionFactsIntegrityError",
    "ExecutionFactsSecurityError",
    "ExecutionFactsValidationError",
    "ExecutionFactsWriter",
    "FactsFileSummary",
    "FeeType",
    "QualityReport",
    "V1_ORDER_STATUS_VALUES",
    "V1_ORDER_TYPE_VALUES",
    "build_source_event_id",
    "decimal_to_text",
    "derive_pre_factor_ref_date",
    "normalize_utc_datetime",
    "redact_sensitive_text",
    "validate_canonical_security_code",
    "validate_execution_facts_quality",
    "validate_facts_file",
    "validate_published_execution_facts",
    "validate_run_id",
]
