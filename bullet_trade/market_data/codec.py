"""
作者: BruceLee

文件职责: 为版本化类型化市场事件提供确定、无损且 fail-closed 的 JSON wire codec。
主要输入: MarketEvent 及其具名子类，或来自可信传输边界的 schema-v1 wire mapping/JSON。
主要输出: 不依赖厂商 SDK 的 JSON-safe 字典、UTF-8 JSON 和恢复后的具体事件模型。
上游关系: 由 realtime feed、未来 Huaxin native wrapper 和远程 market-data 协议调用。
下游关系: 供客户端、EventBus、录制回放和合同测试保留证券身份、时间与通道序列。
关键配置约定: 仅接受显式支持的 schema；未知对象不字符串化；大整数与 bytes 使用带标签编码。
"""

from __future__ import annotations

import base64
import json
import math
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Type, Union

from .models import (
    CompatibilityTickEvent,
    ConnectionStateEvent,
    ConsolidatedTickEvent,
    DepthSnapshotEvent,
    IopvEvent,
    MarketEvent,
    MarketEventRoute,
    MarketStatusEvent,
    OrderDetailEvent,
    QuoteSnapshotEvent,
    SecurityStatusEvent,
    SequenceGapEvent,
    SourceSequence,
    TransactionEvent,
)


class MarketEventCodecError(ValueError):
    """表示市场事件 wire 编解码因结构、类型或一致性错误而受控失败。"""


class UnsupportedMarketEventSchemaError(MarketEventCodecError):
    """表示对端市场事件 schema 不在当前显式支持列表中。"""


_WIRE_TYPE_KEY = "__bullet_trade_wire_type__"
_SAFE_JSON_INTEGER = (1 << 53) - 1
_SUPPORTED_SCHEMAS = frozenset({"1"})

_EVENT_CLASSES: Mapping[str, Type[MarketEvent]] = {
    event_class.__name__: event_class
    for event_class in (
        MarketEvent,
        CompatibilityTickEvent,
        QuoteSnapshotEvent,
        DepthSnapshotEvent,
        TransactionEvent,
        OrderDetailEvent,
        ConsolidatedTickEvent,
        IopvEvent,
        SecurityStatusEvent,
        MarketStatusEvent,
        SequenceGapEvent,
        ConnectionStateEvent,
    )
}

_ENVELOPE_FIELDS = frozenset(
    {
        "event_class",
        "schema_version",
        "field_set_version",
        "field_profile",
        "provider",
        "capability_key",
        "route_rule",
        "route",
        "event_type",
        "level",
        "security",
        "raw_security",
        "raw_market_code",
        "exchange",
        "asset_type",
        "trading_day",
        "trading_day_source",
        "exchange_time",
        "gateway_received_at",
        "client_received_at",
        "stream_id",
        "channel_id",
        "session_epoch",
        "source_sequence",
        "raw_type",
        "payload",
        "provider_extension",
        "raw_profile",
        "field_presence",
        "completeness",
        "missing_fields",
    }
)

_ROUTE_FIELDS = frozenset(
    {
        "provider",
        "capability_key",
        "rule_id",
        "semantic_class",
        "manifest_version",
        "provider_version",
        "build_id",
        "location",
        "reason",
    }
)

_SEQUENCE_FIELDS = frozenset({"schema_version", "ordering_scope", "values"})


def _require_exact_fields(payload: Mapping[str, Any], expected: Iterable[str], label: str) -> None:
    """
    校验一个 wire object 的字段集合与当前 schema 完全一致。

    Args:
        payload: 待检查的 wire mapping。
        expected: 当前 schema 允许且要求的完整字段集合。
        label: 错误信息中的对象名称。

    Returns:
        None: 字段集合一致时返回。

    Raises:
        MarketEventCodecError: 存在缺失或未知字段时抛出，避免静默忽略新 schema。
    """
    expected_set = set(expected)
    actual_set = set(payload)
    missing = sorted(expected_set.difference(actual_set))
    unknown = sorted(actual_set.difference(expected_set))
    if missing or unknown:
        raise MarketEventCodecError(f"{label} 字段不符合当前 schema: missing={missing}, unknown={unknown}")


def _encode_datetime(value: Optional[datetime]) -> Optional[Mapping[str, Any]]:
    """
    将可选 datetime 编码为保留 offset、微秒和 fold 的结构。

    Args:
        value: 待编码的 datetime 或 None。

    Returns:
        Optional[Mapping[str, Any]]: JSON-safe 时间结构或 None。
    """
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise MarketEventCodecError(f"预期 datetime，实际为 {type(value).__name__}")
    return {"iso": value.isoformat(), "fold": int(value.fold)}


def _decode_datetime(value: Any, label: str) -> Optional[datetime]:
    """
    从严格结构恢复可选 datetime。

    Args:
        value: wire 中的时间结构或 None。
        label: 错误信息使用的字段名。

    Returns:
        Optional[datetime]: 恢复后的 datetime，保留原 UTC offset、微秒和 fold。

    Raises:
        MarketEventCodecError: 结构、ISO 文本或 fold 非法时抛出。
    """
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise MarketEventCodecError(f"{label} 必须是时间结构或 null")
    _require_exact_fields(value, ("iso", "fold"), label)
    iso_value = value["iso"]
    fold = value["fold"]
    if not isinstance(iso_value, str) or not isinstance(fold, int) or isinstance(fold, bool):
        raise MarketEventCodecError(f"{label} 的 iso/fold 类型非法")
    if fold not in (0, 1):
        raise MarketEventCodecError(f"{label}.fold 必须为 0 或 1")
    try:
        return datetime.fromisoformat(iso_value).replace(fold=fold)
    except ValueError as exc:
        raise MarketEventCodecError(f"{label} 不是合法 ISO8601 datetime") from exc


def _encode_date(value: Optional[date]) -> Optional[str]:
    """
    将可选交易日编码为 ISO8601 日期。

    Args:
        value: 待编码的 date 或 None；datetime 不允许冒充交易日。

    Returns:
        Optional[str]: ISO 日期或 None。

    Raises:
        MarketEventCodecError: value 不是纯 date 时抛出。
    """
    if value is None:
        return None
    if not isinstance(value, date) or isinstance(value, datetime):
        raise MarketEventCodecError(f"预期 date，实际为 {type(value).__name__}")
    return value.isoformat()


def _decode_date(value: Any, label: str) -> Optional[date]:
    """
    从 ISO8601 文本恢复可选交易日。

    Args:
        value: wire 中的日期字符串或 None。
        label: 错误信息使用的字段名。

    Returns:
        Optional[date]: 恢复后的 date。

    Raises:
        MarketEventCodecError: 类型或日期文本非法时抛出。
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise MarketEventCodecError(f"{label} 必须是 ISO 日期字符串或 null")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise MarketEventCodecError(f"{label} 不是合法 ISO8601 date") from exc


def _encode_value(value: Any) -> Any:
    """
    将事件三层载荷递归编码为无歧义 JSON-safe 值。

    Args:
        value: canonical/vendor/raw/sequence 中的 Python 值。

    Returns:
        Any: 仅由 JSON 基础类型组成的值；映射和特殊标量使用私有类型标签。

    Raises:
        MarketEventCodecError: 映射键非字符串或类型不在白名单时抛出。

    Notes:
        超出 JavaScript 安全整数范围的整数编码为十进制文本，避免 int64 经 JSON/JS
        路径转成 float；bytes 使用 base64，未知对象绝不调用 str() 猜测语义。
    """
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        if -_SAFE_JSON_INTEGER <= value <= _SAFE_JSON_INTEGER:
            return value
        return {_WIRE_TYPE_KEY: "integer", "value": str(value)}
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        if math.isnan(value):
            label = "nan"
        elif value > 0:
            label = "positive_infinity"
        else:
            label = "negative_infinity"
        return {_WIRE_TYPE_KEY: "float", "value": label}
    if isinstance(value, Decimal):
        return {_WIRE_TYPE_KEY: "decimal", "value": str(value)}
    if isinstance(value, bytes):
        encoded = base64.b64encode(value).decode("ascii")
        return {_WIRE_TYPE_KEY: "bytes", "encoding": "base64", "value": encoded}
    if isinstance(value, datetime):
        return {
            _WIRE_TYPE_KEY: "datetime",
            "iso": value.isoformat(),
            "fold": int(value.fold),
        }
    if isinstance(value, date):
        return {_WIRE_TYPE_KEY: "date", "iso": value.isoformat()}
    if isinstance(value, Mapping):
        items: List[List[Any]] = []
        for key in sorted(value):
            if not isinstance(key, str):
                raise MarketEventCodecError("动态 mapping 的键必须是字符串")
            items.append([key, _encode_value(value[key])])
        return {_WIRE_TYPE_KEY: "mapping", "items": items}
    if isinstance(value, tuple):
        return {_WIRE_TYPE_KEY: "tuple", "items": [_encode_value(item) for item in value]}
    if isinstance(value, list):
        return {_WIRE_TYPE_KEY: "list", "items": [_encode_value(item) for item in value]}
    raise MarketEventCodecError(f"不支持的市场事件 wire 值类型: {type(value).__name__}")


def _decode_tagged_mapping(value: Mapping[str, Any]) -> Any:
    """
    解码一个带 BulletTrade 私有类型标签的动态值。

    Args:
        value: 包含 ``__bullet_trade_wire_type__`` 的 mapping。

    Returns:
        Any: 恢复后的 Python 标量、容器、日期时间或 bytes。

    Raises:
        MarketEventCodecError: 标签未知、字段集合或编码内容非法时抛出。
    """
    tag = value.get(_WIRE_TYPE_KEY)
    if not isinstance(tag, str):
        raise MarketEventCodecError("动态 mapping 缺少合法 wire type 标签")
    if tag == "integer":
        _require_exact_fields(value, (_WIRE_TYPE_KEY, "value"), "integer")
        raw = value["value"]
        if not isinstance(raw, str):
            raise MarketEventCodecError("integer.value 必须是十进制字符串")
        try:
            return int(raw, 10)
        except ValueError as exc:
            raise MarketEventCodecError("integer.value 不是合法十进制整数") from exc
    if tag == "float":
        _require_exact_fields(value, (_WIRE_TYPE_KEY, "value"), "float")
        labels = {
            "nan": float("nan"),
            "positive_infinity": float("inf"),
            "negative_infinity": float("-inf"),
        }
        raw = value["value"]
        if raw not in labels:
            raise MarketEventCodecError("float.value 包含未知特殊值")
        return labels[raw]
    if tag == "decimal":
        _require_exact_fields(value, (_WIRE_TYPE_KEY, "value"), "decimal")
        raw = value["value"]
        if not isinstance(raw, str):
            raise MarketEventCodecError("decimal.value 必须是字符串")
        try:
            return Decimal(raw)
        except InvalidOperation as exc:
            raise MarketEventCodecError("decimal.value 不是合法 Decimal") from exc
    if tag == "bytes":
        _require_exact_fields(value, (_WIRE_TYPE_KEY, "encoding", "value"), "bytes")
        if value["encoding"] != "base64" or not isinstance(value["value"], str):
            raise MarketEventCodecError("bytes 必须使用 base64 字符串编码")
        try:
            return base64.b64decode(value["value"].encode("ascii"), validate=True)
        except (UnicodeEncodeError, ValueError) as exc:
            raise MarketEventCodecError("bytes.value 不是合法 base64") from exc
    if tag == "datetime":
        _require_exact_fields(value, (_WIRE_TYPE_KEY, "iso", "fold"), "datetime")
        return _decode_datetime({"iso": value["iso"], "fold": value["fold"]}, "datetime")
    if tag == "date":
        _require_exact_fields(value, (_WIRE_TYPE_KEY, "iso"), "date")
        return _decode_date(value["iso"], "date")
    if tag in {"tuple", "list"}:
        _require_exact_fields(value, (_WIRE_TYPE_KEY, "items"), tag)
        items = value["items"]
        if not isinstance(items, list):
            raise MarketEventCodecError(f"{tag}.items 必须是数组")
        decoded = [_decode_value(item) for item in items]
        return tuple(decoded) if tag == "tuple" else decoded
    if tag == "mapping":
        _require_exact_fields(value, (_WIRE_TYPE_KEY, "items"), "mapping")
        items = value["items"]
        if not isinstance(items, list):
            raise MarketEventCodecError("mapping.items 必须是数组")
        decoded_mapping: Dict[str, Any] = {}
        for pair in items:
            if not isinstance(pair, list) or len(pair) != 2 or not isinstance(pair[0], str):
                raise MarketEventCodecError("mapping.items 每项必须是 [string, value]")
            key = pair[0]
            if key in decoded_mapping:
                raise MarketEventCodecError(f"mapping.items 出现重复键: {key}")
            decoded_mapping[key] = _decode_value(pair[1])
        return decoded_mapping
    raise MarketEventCodecError(f"未知动态 wire type: {tag}")


def _decode_value(value: Any) -> Any:
    """
    递归恢复 codec 白名单中的动态值。

    Args:
        value: JSON 解码后的基础值或带标签 mapping。

    Returns:
        Any: 恢复后的 Python 值。

    Raises:
        MarketEventCodecError: 遇到未标记对象、未标记数组或其他非法类型时抛出。
    """
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise MarketEventCodecError("非有限浮点必须使用 float wire type 标签")
        return value
    if isinstance(value, int):
        if not -_SAFE_JSON_INTEGER <= value <= _SAFE_JSON_INTEGER:
            raise MarketEventCodecError("超出 JSON 安全范围的整数必须使用 integer wire type 标签")
        return value
    if isinstance(value, Mapping):
        return _decode_tagged_mapping(value)
    if isinstance(value, list):
        raise MarketEventCodecError("动态数组必须携带 tuple/list wire type 标签")
    raise MarketEventCodecError(f"不支持的动态 wire 值类型: {type(value).__name__}")


def _route_to_wire(route: MarketEventRoute) -> Mapping[str, Any]:
    """
    将事件路由证明转换为固定字段 wire mapping。

    Args:
        route: 已由模型校验的 MarketEventRoute。

    Returns:
        Mapping[str, Any]: JSON-safe 且字段完整的 route mapping。
    """
    return {
        "provider": route.provider,
        "capability_key": route.capability_key,
        "rule_id": route.rule_id,
        "semantic_class": route.semantic_class,
        "manifest_version": route.manifest_version,
        "provider_version": route.provider_version,
        "build_id": route.build_id,
        "location": route.location,
        "reason": route.reason,
    }


def _route_from_wire(value: Any) -> MarketEventRoute:
    """
    从固定字段 wire mapping 恢复事件路由证明。

    Args:
        value: wire 中的 route object。

    Returns:
        MarketEventRoute: 校验后的不可变 route。

    Raises:
        MarketEventCodecError: 结构或字段值非法时抛出。
    """
    if not isinstance(value, Mapping):
        raise MarketEventCodecError("route 必须是 object")
    _require_exact_fields(value, _ROUTE_FIELDS, "route")
    try:
        return MarketEventRoute(**dict(value))
    except (TypeError, ValueError) as exc:
        raise MarketEventCodecError(f"route 内容非法: {exc}") from exc


def _sequence_to_wire(sequence: SourceSequence) -> Mapping[str, Any]:
    """
    将通道限定的原始序列转换为固定字段 wire mapping。

    Args:
        sequence: 已冻结的 SourceSequence。

    Returns:
        Mapping[str, Any]: 包含 schema、ordering scope 和无损 values 的 mapping。
    """
    return {
        "schema_version": sequence.schema_version,
        "ordering_scope": sequence.ordering_scope,
        "values": _encode_value(sequence.components),
    }


def _sequence_from_wire(value: Any) -> SourceSequence:
    """
    从 wire mapping 恢复通道限定的原始序列。

    Args:
        value: wire 中的 source_sequence object。

    Returns:
        SourceSequence: 保留 int64/bytes 等原始值的不可变序列。

    Raises:
        MarketEventCodecError: 结构、版本或 values 非法时抛出。
    """
    if not isinstance(value, Mapping):
        raise MarketEventCodecError("source_sequence 必须是 object")
    _require_exact_fields(value, _SEQUENCE_FIELDS, "source_sequence")
    schema_version = value["schema_version"]
    ordering_scope = value["ordering_scope"]
    if not isinstance(schema_version, str) or not isinstance(ordering_scope, str):
        raise MarketEventCodecError("source_sequence schema/scope 必须是字符串")
    decoded_values = _decode_value(value["values"])
    if not isinstance(decoded_values, Mapping):
        raise MarketEventCodecError("source_sequence.values 必须解码为 mapping")
    try:
        return SourceSequence(
            components=decoded_values,
            ordering_scope=ordering_scope,
            schema_version=schema_version,
        )
    except ValueError as exc:
        raise MarketEventCodecError(f"source_sequence 内容非法: {exc}") from exc


def market_event_to_wire(event: MarketEvent) -> Dict[str, Any]:
    """
    将类型化市场事件编码为 schema-v1 JSON-safe 字典。

    Args:
        event: MarketEvent 或已注册的具名事件子类。

    Returns:
        Dict[str, Any]: 字段固定、可由标准 json 模块编码的独立字典。

    Raises:
        UnsupportedMarketEventSchemaError: 事件 schema 当前不受支持时抛出。
        MarketEventCodecError: 事件类未注册或任一动态值不可无损传输时抛出。
    """
    if not isinstance(event, MarketEvent):
        raise MarketEventCodecError("market_event_to_wire 只接受 MarketEvent")
    if event.schema_version not in _SUPPORTED_SCHEMAS:
        raise UnsupportedMarketEventSchemaError(f"不支持市场事件 schema_version={event.schema_version!r}")
    event_class = type(event).__name__
    if event_class not in _EVENT_CLASSES or _EVENT_CLASSES[event_class] is not type(event):
        raise MarketEventCodecError(f"未注册市场事件类: {event_class}")
    route = event.route
    if route is None:  # pragma: no cover - MarketEvent.__post_init__ 已保证
        raise MarketEventCodecError("事件缺少 route provenance")
    sequence = event.source_sequence
    if not isinstance(sequence, SourceSequence):  # pragma: no cover - 模型已归一化
        raise MarketEventCodecError("事件 source_sequence 未归一化")
    wire = {
        "event_class": event_class,
        "schema_version": event.schema_version,
        "field_set_version": event.field_set_version,
        "field_profile": event.field_profile.value,
        "provider": event.provider,
        "capability_key": event.capability_key,
        "route_rule": event.route_rule,
        "route": _route_to_wire(route),
        "event_type": event.event_type.value,
        "level": event.level.value,
        "security": event.security,
        "raw_security": event.raw_security,
        "raw_market_code": event.raw_market_code,
        "exchange": event.exchange,
        "asset_type": event.asset_type,
        "trading_day": _encode_date(event.trading_day),
        "trading_day_source": event.trading_day_source,
        "exchange_time": _encode_datetime(event.exchange_time),
        "gateway_received_at": _encode_datetime(event.gateway_received_at),
        "client_received_at": _encode_datetime(event.client_received_at),
        "stream_id": event.stream_id,
        "channel_id": event.channel_id,
        "session_epoch": event.session_epoch,
        "source_sequence": _sequence_to_wire(sequence),
        "raw_type": event.raw_type,
        "payload": _encode_value(event.payload),
        "provider_extension": _encode_value(event.provider_extension),
        "raw_profile": _encode_value(event.raw_profile),
        "field_presence": list(event.field_presence),
        "completeness": event.completeness,
        "missing_fields": list(event.missing_fields),
    }
    _require_exact_fields(wire, _ENVELOPE_FIELDS, "market event")
    return wire


def _require_optional_string(value: Any, label: str) -> Optional[str]:
    """
    校验 wire 中的可选字符串而不进行隐式类型转换。

    Args:
        value: 待检查值。
        label: 错误信息使用的字段名。

    Returns:
        Optional[str]: 原字符串或 None。

    Raises:
        MarketEventCodecError: 值不是字符串或 None 时抛出。
    """
    if value is None or isinstance(value, str):
        return value
    raise MarketEventCodecError(f"{label} 必须是字符串或 null")


def _require_string_list(value: Any, label: str) -> Tuple[str, ...]:
    """
    校验 wire 中的字符串数组。

    Args:
        value: 待检查值。
        label: 错误信息使用的字段名。

    Returns:
        Tuple[str, ...]: 保持 wire 顺序的字符串元组。

    Raises:
        MarketEventCodecError: 值不是纯字符串数组时抛出。
    """
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise MarketEventCodecError(f"{label} 必须是字符串数组")
    return tuple(value)


def market_event_from_wire(wire: Mapping[str, Any]) -> MarketEvent:
    """
    从 schema-v1 wire mapping 严格恢复具体市场事件类。

    Args:
        wire: 来自 JSON 或进程内传输的市场事件 object。

    Returns:
        MarketEvent: 与 ``event_class/event_type`` 一致的具体不可变事件实例。

    Raises:
        UnsupportedMarketEventSchemaError: schema 未显式支持时抛出。
        MarketEventCodecError: 缺字段、多字段、类型错误或模型约束不一致时抛出。
    """
    if not isinstance(wire, Mapping):
        raise MarketEventCodecError("市场事件 wire 必须是 object")
    schema_version = wire.get("schema_version")
    if not isinstance(schema_version, str) or schema_version not in _SUPPORTED_SCHEMAS:
        raise UnsupportedMarketEventSchemaError(f"不支持市场事件 schema_version={schema_version!r}")
    _require_exact_fields(wire, _ENVELOPE_FIELDS, "market event")
    event_class_name = wire["event_class"]
    if not isinstance(event_class_name, str):
        raise MarketEventCodecError("event_class 必须是字符串")
    event_class = _EVENT_CLASSES.get(event_class_name)
    if event_class is None:
        raise MarketEventCodecError(f"未知市场事件类: {event_class_name}")
    for required_string in (
        "field_set_version",
        "field_profile",
        "provider",
        "capability_key",
        "event_type",
        "level",
        "exchange",
        "session_epoch",
    ):
        if not isinstance(wire[required_string], str):
            raise MarketEventCodecError(f"{required_string} 必须是字符串")
    completeness = wire["completeness"]
    if not isinstance(completeness, bool):
        raise MarketEventCodecError("completeness 必须是 bool")
    payload = _decode_value(wire["payload"])
    provider_extension = _decode_value(wire["provider_extension"])
    raw_profile = _decode_value(wire["raw_profile"])
    if not all(isinstance(item, Mapping) for item in (payload, provider_extension, raw_profile)):
        raise MarketEventCodecError("payload/provider_extension/raw_profile 必须解码为 mapping")
    try:
        return event_class(
            provider=wire["provider"],
            capability_key=wire["capability_key"],
            event_type=wire["event_type"],
            level=wire["level"],
            exchange=wire["exchange"],
            session_epoch=wire["session_epoch"],
            payload=payload,
            security=_require_optional_string(wire["security"], "security"),
            raw_security_code=_require_optional_string(wire["raw_security"], "raw_security"),
            raw_market_code=_require_optional_string(wire["raw_market_code"], "raw_market_code"),
            asset_type=_require_optional_string(wire["asset_type"], "asset_type"),
            schema_version=schema_version,
            field_set_version=wire["field_set_version"],
            field_profile=wire["field_profile"],
            route_rule=_require_optional_string(wire["route_rule"], "route_rule"),
            route=_route_from_wire(wire["route"]),
            trading_day=_decode_date(wire["trading_day"], "trading_day"),
            trading_day_source=_require_optional_string(
                wire["trading_day_source"], "trading_day_source"
            ),
            exchange_time=_decode_datetime(wire["exchange_time"], "exchange_time"),
            gateway_received_at=_decode_datetime(
                wire["gateway_received_at"], "gateway_received_at"
            ),
            client_received_at=_decode_datetime(wire["client_received_at"], "client_received_at"),
            stream_id=_require_optional_string(wire["stream_id"], "stream_id"),
            channel_id=_require_optional_string(wire["channel_id"], "channel_id"),
            source_sequence=_sequence_from_wire(wire["source_sequence"]),
            raw_type=_require_optional_string(wire["raw_type"], "raw_type"),
            provider_extension=provider_extension,
            raw_profile=raw_profile,
            field_presence=_require_string_list(wire["field_presence"], "field_presence"),
            completeness=completeness,
            missing_fields=_require_string_list(wire["missing_fields"], "missing_fields"),
        )
    except MarketEventCodecError:
        raise
    except (TypeError, ValueError) as exc:
        raise MarketEventCodecError(f"市场事件模型校验失败: {exc}") from exc


def dumps_market_event(event: MarketEvent) -> str:
    """
    将市场事件编码为确定性 UTF-8 JSON 文本。

    Args:
        event: 待编码的类型化市场事件。

    Returns:
        str: 按 key 排序、不输出 NaN 常量的紧凑 JSON 文本。

    Raises:
        MarketEventCodecError: 事件不可按当前 schema 无损编码时抛出。
    """
    try:
        return json.dumps(
            market_event_to_wire(event),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        if isinstance(exc, MarketEventCodecError):
            raise
        raise MarketEventCodecError(f"市场事件 JSON 编码失败: {exc}") from exc


def _reject_duplicate_pairs(pairs: Sequence[Tuple[str, Any]]) -> Dict[str, Any]:
    """
    构造 JSON object 并拒绝重复键。

    Args:
        pairs: json decoder 按输入顺序提供的键值对。

    Returns:
        Dict[str, Any]: 无重复键的普通字典。

    Raises:
        MarketEventCodecError: 同一 object 出现重复键时抛出。
    """
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise MarketEventCodecError(f"JSON object 出现重复键: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    """
    拒绝 JSON 标准之外的 NaN/Infinity 常量。

    Args:
        value: json decoder 识别到的非标准常量文本。

    Returns:
        None: 本函数始终抛出异常，不产生返回值。

    Raises:
        MarketEventCodecError: 每次调用均抛出，特殊浮点必须使用 codec 标签。
    """
    raise MarketEventCodecError(f"JSON 不允许未标记常量: {value}")


def loads_market_event(payload: Union[str, bytes, bytearray]) -> MarketEvent:
    """
    从 UTF-8 JSON 文本严格恢复市场事件。

    Args:
        payload: str、bytes 或 bytearray 形式的完整单事件 JSON。

    Returns:
        MarketEvent: schema 和具体事件类均已校验的事件。

    Raises:
        MarketEventCodecError: UTF-8、JSON、重复键或事件结构非法时抛出。
        UnsupportedMarketEventSchemaError: JSON 携带未知 schema 时抛出。
    """
    if isinstance(payload, (bytes, bytearray)):
        try:
            text = bytes(payload).decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise MarketEventCodecError("市场事件 JSON 不是合法 UTF-8") from exc
    elif isinstance(payload, str):
        text = payload
    else:
        raise MarketEventCodecError("loads_market_event 只接受 str/bytes/bytearray")
    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_json_constant,
        )
    except MarketEventCodecError:
        raise
    except json.JSONDecodeError as exc:
        raise MarketEventCodecError(f"市场事件 JSON 解析失败: {exc.msg}") from exc
    if not isinstance(decoded, Mapping):
        raise MarketEventCodecError("市场事件 JSON 顶层必须是 object")
    return market_event_from_wire(decoded)


__all__ = [
    "MarketEventCodecError",
    "UnsupportedMarketEventSchemaError",
    "dumps_market_event",
    "loads_market_event",
    "market_event_from_wire",
    "market_event_to_wire",
]
