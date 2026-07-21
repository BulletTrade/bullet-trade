"""
BulletTrade 证券身份 V1 的权威解析、序列化与供应商边界适配。

作者: BruceLee
文件职责: 定义结构化 SecurityId、稳定错误码、canonical 长后缀和 JQ/QMT/THS 适配器。
主要输入: 带明确来源格式的证券代码，或同花顺纯代码与可信交易所元数据。
主要输出: security-id/v1 canonical 身份、供应商边界代码、稳定幂等身份和语义哈希。
上下游关系: 上游是数据源、券商和历史兼容入站边界，下游是领域比较、信号、账本与订单幂等。
关键约定: 领域 canonical 固定使用长后缀；无后缀不得猜交易所；THS 股票模拟边界不支持期货。
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Union, cast

SECURITY_ID_SCHEME_VERSION = "security-id/v1"

_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+(?:-[A-Z0-9]+)*$")
_SUFFIXED_CODE_PATTERN = re.compile(
    r"^(?P<symbol>[A-Z0-9]+(?:-[A-Z0-9]+)*)\.(?P<suffix>[A-Z0-9]+)$"
)


class SecurityExchange(str, Enum):
    """security-id/v1 支持的 canonical 交易所集合。"""

    XSHG = "XSHG"
    XSHE = "XSHE"
    BSE = "BSE"
    XSGE = "XSGE"
    CCFX = "CCFX"
    XDCE = "XDCE"
    XZCE = "XZCE"
    XINE = "XINE"


class SecurityIdErrorCode(str, Enum):
    """跨仓 conformance 使用的稳定错误分类。"""

    INVALID_TYPE = "invalid_type"
    EMPTY = "empty"
    INVALID_FORMAT = "invalid_format"
    INVALID_SYMBOL = "invalid_symbol"
    MISSING_EXCHANGE = "missing_exchange"
    UNKNOWN_EXCHANGE = "unknown_exchange"
    EXCHANGE_NOT_ALLOWED = "exchange_not_allowed"
    EXCHANGE_CONFLICT = "exchange_conflict"
    INVALID_SOURCE = "invalid_source"
    INVALID_TARGET = "invalid_target"
    INVALID_SERIALIZATION = "invalid_serialization"
    ADAPTER_UNSUPPORTED = "adapter_unsupported"


class SecurityCodeSource(str, Enum):
    """证券代码入站来源格式。"""

    CANONICAL = "canonical"
    JQ = "jq"
    QMT = "qmt"
    THS = "ths"
    LEGACY_ALIAS = "legacy_alias"


class SecurityCodeTarget(str, Enum):
    """证券代码出站目标格式。"""

    CANONICAL = "canonical"
    JQ = "jq"
    QMT = "qmt"
    THS = "ths"


class SecurityIdValidationError(ValueError):
    """security-id/v1 输入或边界格式校验失败。"""

    def __init__(self, code: SecurityIdErrorCode, message: str) -> None:
        """保存稳定错误码和面向运维的诊断文本。

        Args:
            code: 跨仓一致的错误分类。
            message: 便于定位输入边界的诊断文本。

        Returns:
            None: 初始化异常对象，无业务返回值。
        """

        self.code = code
        super().__init__(message)


_CANONICAL_SUFFIX_TO_EXCHANGE = {item.value: item for item in SecurityExchange}
_JQ_SUFFIX_TO_EXCHANGE = dict(_CANONICAL_SUFFIX_TO_EXCHANGE)
_QMT_SUFFIX_TO_EXCHANGE = {
    "SH": SecurityExchange.XSHG,
    "SZ": SecurityExchange.XSHE,
    "BJ": SecurityExchange.BSE,
    "SHFE": SecurityExchange.XSGE,
    "CFFEX": SecurityExchange.CCFX,
    "DCE": SecurityExchange.XDCE,
    "CZCE": SecurityExchange.XZCE,
    "INE": SecurityExchange.XINE,
}
_LEGACY_SUFFIX_TO_EXCHANGE = {
    **_CANONICAL_SUFFIX_TO_EXCHANGE,
    **_QMT_SUFFIX_TO_EXCHANGE,
    "XSHF": SecurityExchange.XSGE,
}
_QMT_EXCHANGE_TO_SUFFIX = {
    SecurityExchange.XSHG: "SH",
    SecurityExchange.XSHE: "SZ",
    SecurityExchange.BSE: "BJ",
    SecurityExchange.XSGE: "SHFE",
    SecurityExchange.CCFX: "CFFEX",
    SecurityExchange.XDCE: "DCE",
    SecurityExchange.XZCE: "CZCE",
    SecurityExchange.XINE: "INE",
}
_THS_SUPPORTED_EXCHANGES = frozenset(
    {SecurityExchange.XSHG, SecurityExchange.XSHE, SecurityExchange.BSE}
)


def _normalize_required_text(value: Any, field_name: str) -> str:
    """验证必填文本并只做大小写规范化。

    Args:
        value: 待验证输入。
        field_name: 诊断文本中的字段名称。

    Returns:
        str: 大写后的非空文本。

    Raises:
        SecurityIdValidationError: 输入不是文本、为空或含首尾空白。
    """

    if not isinstance(value, str):
        raise SecurityIdValidationError(
            SecurityIdErrorCode.INVALID_TYPE,
            "{0} 必须是文本".format(field_name),
        )
    if not value:
        raise SecurityIdValidationError(
            SecurityIdErrorCode.EMPTY,
            "{0} 不能为空".format(field_name),
        )
    if value != value.strip():
        raise SecurityIdValidationError(
            SecurityIdErrorCode.INVALID_FORMAT,
            "{0} 不得含首尾空白".format(field_name),
        )
    return value.upper()


def _normalize_symbol(value: Any) -> str:
    """把证券主体规范为 V1 大写 symbol。

    Args:
        value: 不含市场后缀的证券主体。

    Returns:
        str: 通过 V1 正则校验的大写 symbol。

    Raises:
        SecurityIdValidationError: symbol 为空、类型错误或字符不合法。
    """

    symbol = _normalize_required_text(value, "symbol")
    if not _SYMBOL_PATTERN.fullmatch(symbol):
        raise SecurityIdValidationError(
            SecurityIdErrorCode.INVALID_SYMBOL,
            "symbol 仅允许字母、数字和分段连字符",
        )
    return symbol


def _normalize_canonical_exchange(value: Any) -> SecurityExchange:
    """验证领域层交易所必须是 canonical 长后缀。

    Args:
        value: SecurityExchange 或 canonical 交易所文本。

    Returns:
        SecurityExchange: 对应的 canonical 枚举。

    Raises:
        SecurityIdValidationError: 交易所类型、后缀或格式不符合 V1。
    """

    if isinstance(value, SecurityExchange):
        return value
    suffix = _normalize_required_text(value, "exchange")
    exchange = _CANONICAL_SUFFIX_TO_EXCHANGE.get(suffix)
    if exchange is not None:
        return exchange
    if suffix in _LEGACY_SUFFIX_TO_EXCHANGE:
        raise SecurityIdValidationError(
            SecurityIdErrorCode.EXCHANGE_NOT_ALLOWED,
            "领域 exchange 必须使用 canonical 长后缀",
        )
    raise SecurityIdValidationError(
        SecurityIdErrorCode.UNKNOWN_EXCHANGE,
        "未知交易所后缀: {0}".format(suffix),
    )


def _parse_suffixed_code(
    value: Any,
    suffixes: Mapping[str, SecurityExchange],
    source_name: str,
) -> "SecurityId":
    """按指定入站边界解析带后缀代码。

    Args:
        value: 待解析的供应商或 canonical 代码。
        suffixes: 当前边界允许的后缀到 canonical 交易所映射。
        source_name: 诊断文本中的来源名称。

    Returns:
        SecurityId: 已规范化的领域身份。

    Raises:
        SecurityIdValidationError: 缺少交易所、格式错误或后缀不属于该边界。
    """

    normalized = _normalize_required_text(value, "security")
    if "." not in normalized:
        raise SecurityIdValidationError(
            SecurityIdErrorCode.MISSING_EXCHANGE,
            "{0} 代码缺少交易所后缀".format(source_name),
        )
    matched = _SUFFIXED_CODE_PATTERN.fullmatch(normalized)
    if matched is None:
        raise SecurityIdValidationError(
            SecurityIdErrorCode.INVALID_FORMAT,
            "{0} 代码格式不符合 security-id/v1".format(source_name),
        )
    symbol = matched.group("symbol")
    suffix = matched.group("suffix")
    exchange = suffixes.get(suffix)
    if exchange is None:
        if suffix in _LEGACY_SUFFIX_TO_EXCHANGE:
            raise SecurityIdValidationError(
                SecurityIdErrorCode.EXCHANGE_NOT_ALLOWED,
                "{0} 边界不允许后缀 {1}".format(source_name, suffix),
            )
        raise SecurityIdValidationError(
            SecurityIdErrorCode.UNKNOWN_EXCHANGE,
            "未知交易所后缀: {0}".format(suffix),
        )
    return SecurityId(symbol=symbol, exchange=exchange)


def _normalize_exchange_metadata(value: Any) -> SecurityExchange:
    """把可信的外部 exchange 元数据规范为 canonical 交易所。

    Args:
        value: canonical 或已登记供应商交易所后缀。

    Returns:
        SecurityExchange: canonical 交易所枚举。

    Raises:
        SecurityIdValidationError: 元数据缺失或交易所未知。
    """

    if value is None or value == "":
        raise SecurityIdValidationError(
            SecurityIdErrorCode.MISSING_EXCHANGE,
            "纯证券代码必须提供可信 exchange 元数据",
        )
    if isinstance(value, SecurityExchange):
        return value
    suffix = _normalize_required_text(value, "exchange")
    exchange = _LEGACY_SUFFIX_TO_EXCHANGE.get(suffix)
    if exchange is None:
        raise SecurityIdValidationError(
            SecurityIdErrorCode.UNKNOWN_EXCHANGE,
            "未知 exchange 元数据: {0}".format(suffix),
        )
    return exchange


def _coerce_source(value: Union[SecurityCodeSource, str]) -> SecurityCodeSource:
    """把调用方来源参数转换为固定枚举。

    Args:
        value: 来源枚举或其文本值。

    Returns:
        SecurityCodeSource: 标准来源枚举。

    Raises:
        SecurityIdValidationError: 来源未登记或不是文本。
    """

    if isinstance(value, SecurityCodeSource):
        return value
    if not isinstance(value, str):
        raise SecurityIdValidationError(
            SecurityIdErrorCode.INVALID_SOURCE,
            "security code source 必须是已登记文本",
        )
    try:
        return SecurityCodeSource(value.strip().lower())
    except ValueError as exc:
        raise SecurityIdValidationError(
            SecurityIdErrorCode.INVALID_SOURCE,
            "未登记 security code source: {0}".format(value),
        ) from exc


def _coerce_target(value: Union[SecurityCodeTarget, str]) -> SecurityCodeTarget:
    """把调用方目标参数转换为固定枚举。

    Args:
        value: 目标枚举或其文本值。

    Returns:
        SecurityCodeTarget: 标准目标枚举。

    Raises:
        SecurityIdValidationError: 目标未登记或不是文本。
    """

    if isinstance(value, SecurityCodeTarget):
        return value
    if not isinstance(value, str):
        raise SecurityIdValidationError(
            SecurityIdErrorCode.INVALID_TARGET,
            "security code target 必须是已登记文本",
        )
    try:
        return SecurityCodeTarget(value.strip().lower())
    except ValueError as exc:
        raise SecurityIdValidationError(
            SecurityIdErrorCode.INVALID_TARGET,
            "未登记 security code target: {0}".format(value),
        ) from exc


@dataclass(frozen=True, order=True)
class SecurityId:
    """领域层唯一证券身份，由大写 symbol 与 canonical exchange 组成。"""

    symbol: str
    exchange: SecurityExchange

    def __post_init__(self) -> None:
        """在直接构造时执行与解析器一致的严格校验。

        Args:
            self: dataclass 当前实例。

        Returns:
            None: 仅规范化冻结字段，无业务返回值。
        """

        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(self, "exchange", _normalize_canonical_exchange(self.exchange))

    @classmethod
    def parse_canonical(cls, value: Any) -> "SecurityId":
        """解析领域 canonical 长后缀文本。

        Args:
            value: 例如 ``510180.XSHG`` 的 canonical 文本。

        Returns:
            SecurityId: 结构化证券身份。

        Raises:
            SecurityIdValidationError: 输入缺少后缀或使用供应商短后缀。
        """

        return _parse_suffixed_code(value, _CANONICAL_SUFFIX_TO_EXCHANGE, "canonical")

    @classmethod
    def parse_legacy_alias(cls, value: Any) -> "SecurityId":
        """在受控兼容读边界解析已登记的历史别名。

        Args:
            value: 带 canonical 或已登记供应商后缀的历史代码。

        Returns:
            SecurityId: 收敛后的领域身份。

        Raises:
            SecurityIdValidationError: 输入无后缀、格式错误或交易所未知。
        """

        return _parse_suffixed_code(value, _LEGACY_SUFFIX_TO_EXCHANGE, "legacy_alias")

    @classmethod
    def from_versioned_dict(cls, payload: Mapping[str, Any]) -> "SecurityId":
        """从 security-id/v1 结构化序列化恢复身份并校验冗余 canonical。

        Args:
            payload: 含 scheme_version、symbol、exchange 和 canonical 的映射。

        Returns:
            SecurityId: 校验通过的证券身份。

        Raises:
            SecurityIdValidationError: schema、字段集合或冗余 canonical 不一致。
        """

        if not isinstance(payload, Mapping):
            raise SecurityIdValidationError(
                SecurityIdErrorCode.INVALID_SERIALIZATION,
                "SecurityId 序列化必须是映射",
            )
        expected_keys = {"scheme_version", "symbol", "exchange", "canonical"}
        if set(payload.keys()) != expected_keys:
            raise SecurityIdValidationError(
                SecurityIdErrorCode.INVALID_SERIALIZATION,
                "SecurityId 序列化字段集合不符合 security-id/v1",
            )
        if payload.get("scheme_version") != SECURITY_ID_SCHEME_VERSION:
            raise SecurityIdValidationError(
                SecurityIdErrorCode.INVALID_SERIALIZATION,
                "SecurityId scheme_version 不受支持",
            )
        raw_symbol = payload.get("symbol")
        raw_exchange = payload.get("exchange")
        if not isinstance(raw_symbol, str) or not isinstance(raw_exchange, str):
            raise SecurityIdValidationError(
                SecurityIdErrorCode.INVALID_SERIALIZATION,
                "SecurityId symbol/exchange 必须是文本",
            )
        identity = cls(
            symbol=cast(str, raw_symbol),
            exchange=cast(SecurityExchange, raw_exchange),
        )
        if raw_symbol != identity.symbol or raw_exchange != identity.exchange.value:
            raise SecurityIdValidationError(
                SecurityIdErrorCode.INVALID_SERIALIZATION,
                "SecurityId symbol/exchange 必须已经是 canonical 大写文本",
            )
        if payload.get("canonical") != identity.canonical:
            raise SecurityIdValidationError(
                SecurityIdErrorCode.EXCHANGE_CONFLICT,
                "SecurityId canonical 与 symbol/exchange 不一致",
            )
        return identity

    @property
    def canonical(self) -> str:
        """返回领域层唯一长后缀文本。

        Returns:
            str: ``symbol.exchange`` canonical 文本。
        """

        return "{0}.{1}".format(self.symbol, self.exchange.value)

    @property
    def idempotency_identity(self) -> str:
        """返回包含 scheme 版本的稳定幂等身份片段。

        Returns:
            str: 可安全参与订单、缓存和信号幂等键的身份文本。
        """

        return "{0}|{1}".format(SECURITY_ID_SCHEME_VERSION, self.canonical)

    @property
    def semantic_signature(self) -> str:
        """返回结构化身份序列化的稳定 SHA-256 语义哈希。

        Returns:
            str: 64 位小写十六进制哈希；该值用于一致性校验，不是认证签名。
        """

        encoded = json.dumps(
            self.to_versioned_dict(),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def to_versioned_dict(self) -> Dict[str, str]:
        """生成可跨仓传递的 security-id/v1 结构化序列化。

        Returns:
            Dict[str, str]: 固定字段集合和稳定文本值。
        """

        return {
            "scheme_version": SECURITY_ID_SCHEME_VERSION,
            "symbol": self.symbol,
            "exchange": self.exchange.value,
            "canonical": self.canonical,
        }

    def __str__(self) -> str:
        """使用 canonical 文本表示证券身份。

        Returns:
            str: 与 ``canonical`` 属性相同的稳定文本。
        """

        return self.canonical


class JoinQuantSecurityIdAdapter:
    """聚宽长后缀与 SecurityId 之间的无状态边界适配器。"""

    @staticmethod
    def parse(value: Any) -> SecurityId:
        """把聚宽格式代码解析为领域身份。

        Args:
            value: 聚宽长后缀证券或期货代码。

        Returns:
            SecurityId: canonical 领域身份。

        Raises:
            SecurityIdValidationError: 输入不是已登记聚宽格式。
        """

        return _parse_suffixed_code(value, _JQ_SUFFIX_TO_EXCHANGE, "jq")

    @staticmethod
    def format(security_id: SecurityId) -> str:
        """把领域身份格式化为聚宽长后缀代码。

        Args:
            security_id: 已校验的领域身份。

        Returns:
            str: 聚宽边界代码。

        Raises:
            SecurityIdValidationError: 参数不是 SecurityId。
        """

        _require_security_id(security_id)
        return security_id.canonical


class QmtSecurityIdAdapter:
    """QMT 短后缀与 SecurityId 之间的无状态边界适配器。"""

    @staticmethod
    def parse(value: Any) -> SecurityId:
        """把 QMT 原生后缀代码解析为领域身份。

        Args:
            value: QMT 股票、ETF、北交所或期货代码。

        Returns:
            SecurityId: canonical 领域身份。

        Raises:
            SecurityIdValidationError: 输入不是已登记 QMT 格式。
        """

        return _parse_suffixed_code(value, _QMT_SUFFIX_TO_EXCHANGE, "qmt")

    @staticmethod
    def format(security_id: SecurityId) -> str:
        """把领域身份格式化为 QMT 原生后缀代码。

        Args:
            security_id: 已校验的领域身份。

        Returns:
            str: QMT 边界代码。

        Raises:
            SecurityIdValidationError: 参数不是 SecurityId 或交易所无映射。
        """

        _require_security_id(security_id)
        suffix = _QMT_EXCHANGE_TO_SUFFIX.get(security_id.exchange)
        if suffix is None:
            raise SecurityIdValidationError(
                SecurityIdErrorCode.ADAPTER_UNSUPPORTED,
                "QMT adapter 不支持交易所 {0}".format(security_id.exchange.value),
            )
        return "{0}.{1}".format(security_id.symbol, suffix)


class ThsSecurityIdAdapter:
    """同花顺股票模拟纯代码与 SecurityId 之间的严格边界适配器。"""

    @staticmethod
    def parse(value: Any, exchange: Any) -> SecurityId:
        """用纯代码和可信交易所元数据解析同花顺证券身份。

        Args:
            value: 同花顺返回的不带后缀证券代码。
            exchange: 来自账户或行情元数据的可信交易所。

        Returns:
            SecurityId: canonical 领域身份。

        Raises:
            SecurityIdValidationError: 缺少交易所、代码含后缀或市场不受支持。
        """

        symbol = _normalize_symbol(value)
        normalized_exchange = _normalize_exchange_metadata(exchange)
        if normalized_exchange not in _THS_SUPPORTED_EXCHANGES:
            raise SecurityIdValidationError(
                SecurityIdErrorCode.ADAPTER_UNSUPPORTED,
                "THS 股票模拟 adapter 不支持交易所 {0}".format(normalized_exchange.value),
            )
        return SecurityId(symbol=symbol, exchange=normalized_exchange)

    @staticmethod
    def format(security_id: SecurityId) -> str:
        """把领域身份格式化为同花顺股票模拟纯代码。

        Args:
            security_id: 已校验的领域身份。

        Returns:
            str: 不含后缀的证券代码。

        Raises:
            SecurityIdValidationError: 参数错误或当前交易所不是股票模拟市场。
        """

        _require_security_id(security_id)
        if security_id.exchange not in _THS_SUPPORTED_EXCHANGES:
            raise SecurityIdValidationError(
                SecurityIdErrorCode.ADAPTER_UNSUPPORTED,
                "THS 股票模拟 adapter 不支持交易所 {0}".format(security_id.exchange.value),
            )
        return security_id.symbol


def _require_security_id(value: Any) -> SecurityId:
    """确保出站边界只接收结构化领域身份。

    Args:
        value: 待格式化对象。

    Returns:
        SecurityId: 原对象，便于调用方继续使用。

    Raises:
        SecurityIdValidationError: 参数不是 SecurityId。
    """

    if not isinstance(value, SecurityId):
        raise SecurityIdValidationError(
            SecurityIdErrorCode.INVALID_TYPE,
            "供应商出站 adapter 只接受 SecurityId",
        )
    return value


def parse_security_id(
    value: Any,
    source: Union[SecurityCodeSource, str],
    exchange: Optional[Any] = None,
) -> SecurityId:
    """按显式来源选择严格入站解析器。

    Args:
        value: 待解析证券代码。
        source: canonical、jq、qmt、ths 或受控 legacy_alias。
        exchange: 仅 THS 纯代码允许使用的可信交易所元数据。

    Returns:
        SecurityId: 唯一 canonical 领域身份。

    Raises:
        SecurityIdValidationError: 来源、代码或 exchange 使用不符合契约。
    """

    normalized_source = _coerce_source(source)
    if normalized_source == SecurityCodeSource.THS:
        return ThsSecurityIdAdapter.parse(value, exchange)
    if exchange is not None:
        raise SecurityIdValidationError(
            SecurityIdErrorCode.EXCHANGE_CONFLICT,
            "仅 THS 纯代码边界允许额外 exchange 元数据",
        )
    if normalized_source == SecurityCodeSource.CANONICAL:
        return SecurityId.parse_canonical(value)
    if normalized_source == SecurityCodeSource.JQ:
        return JoinQuantSecurityIdAdapter.parse(value)
    if normalized_source == SecurityCodeSource.QMT:
        return QmtSecurityIdAdapter.parse(value)
    return SecurityId.parse_legacy_alias(value)


def format_security_id(
    security_id: SecurityId,
    target: Union[SecurityCodeTarget, str],
) -> str:
    """按显式目标格式化已校验领域身份。

    Args:
        security_id: 结构化 SecurityId。
        target: canonical、jq、qmt 或 ths。

    Returns:
        str: 目标边界的证券代码。

    Raises:
        SecurityIdValidationError: 目标未知、参数错误或 adapter 不支持市场。
    """

    normalized_target = _coerce_target(target)
    _require_security_id(security_id)
    if normalized_target == SecurityCodeTarget.CANONICAL:
        return security_id.canonical
    if normalized_target == SecurityCodeTarget.JQ:
        return JoinQuantSecurityIdAdapter.format(security_id)
    if normalized_target == SecurityCodeTarget.QMT:
        return QmtSecurityIdAdapter.format(security_id)
    return ThsSecurityIdAdapter.format(security_id)


__all__ = [
    "SECURITY_ID_SCHEME_VERSION",
    "SecurityCodeSource",
    "SecurityCodeTarget",
    "SecurityExchange",
    "SecurityId",
    "SecurityIdErrorCode",
    "SecurityIdValidationError",
    "JoinQuantSecurityIdAdapter",
    "QmtSecurityIdAdapter",
    "ThsSecurityIdAdapter",
    "format_security_id",
    "parse_security_id",
]
