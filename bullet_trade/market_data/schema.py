"""
作者: BruceLee

文件职责: 定义实时行情从 owned vendor record 到 canonical payload，再到兼容投影的单向分层 schema。
主要输入: 已深复制的厂商字段观测、版本化映射规则、字段 profile 与兼容投影规则。
主要输出: 不可变的 canonical/vendor/raw 投影、字段存在性、完整性、缺失原因与显式损失清单。
上游关系: 由 native bridge wrapper、脱敏回放或其他实时行情 adapter 构造 owned record。
下游关系: 供 MarketEvent、远程协议、兼容 tick/current-data 和字段覆盖测试消费。
关键配置约定: 只自动执行同 major 的显式前向迁移；schema、manifest、struct size 或迁移链不匹配时 fail closed。
"""

from __future__ import annotations

import math
import re
from dataclasses import InitVar, dataclass, field, replace
from enum import Enum
from types import MappingProxyType
from typing import Any, Callable, Dict, List, Mapping, MutableMapping, Optional, Set, Tuple, Union

from .models import FieldProfile

__all__ = [
    "CompatibilityProjection",
    "CompatibilityProjectionSchema",
    "FieldLayer",
    "FieldMissingReason",
    "MarketDataProjectionSchema",
    "MarketDataSchemaProjector",
    "MarketSchemaError",
    "MissingField",
    "OwnedVendorRecord",
    "SchemaCompatibilityError",
    "SchemaFieldRule",
    "SchemaMigrationError",
    "SchemaMigrationRegistry",
    "SchemaProjection",
    "SchemaProjectionError",
    "SourceLayoutIdentity",
    "VendorFieldObservation",
    "VendorRecordValidationError",
    "VendorSourceType",
]


_VERSION_PATTERN = re.compile(r"^(0|[1-9][0-9]*)(?:\.(0|[1-9][0-9]*))?(?:\.(0|[1-9][0-9]*))?$")
_SIGNED_INT64_MIN = -(2**63)
_SIGNED_INT64_MAX = 2**63 - 1
_UNSIGNED_INT64_MAX = 2**64 - 1
_SCHEMA_PROJECTION_FACTORY_TOKEN = object()


class MarketSchemaError(ValueError):
    """表示行情分层 schema 的可诊断受控错误。"""


class VendorRecordValidationError(MarketSchemaError):
    """表示 owned vendor record 未满足所有权、类型或缺失语义合同。"""


class SchemaCompatibilityError(MarketSchemaError):
    """表示厂商 schema、manifest、struct size 或版本无法安全解释。"""


class SchemaMigrationError(MarketSchemaError):
    """表示记录版本缺少显式前向迁移或迁移结果违约。"""


class SchemaProjectionError(MarketSchemaError):
    """表示字段映射或兼容投影无法按声明完成。"""


class FieldLayer(str, Enum):
    """表示厂商字段的 canonical、vendor 或 raw 单一归属层。"""

    CANONICAL = "canonical"
    VENDOR = "vendor"
    RAW = "raw"


class FieldMissingReason(str, Enum):
    """表示已声明但没有规范值的稳定缺失原因。"""

    SOURCE_ABSENT = "source_absent"
    NOT_APPLICABLE = "not_applicable"
    NOT_APPLICABLE_EXCHANGE = "not_applicable_exchange"
    DECODE_ERROR = "decode_error"
    INVALID_SENTINEL = "invalid_sentinel"
    MALFORMED_SOURCE = "malformed_source"
    REDACTED = "redacted"


class VendorSourceType(str, Enum):
    """定义 owned vendor record 允许且具备明确 Python 值语义的源类型。"""

    BOOL = "bool"
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    UINT8 = "uint8"
    UINT16 = "uint16"
    UINT32 = "uint32"
    UINT64 = "uint64"
    FLOAT = "float"
    DOUBLE = "double"
    STRING = "string"
    BYTES = "bytes"
    ENUM = "enum"
    ARRAY = "array"


def _normalize_nonempty(value: str, label: str) -> str:
    """
    去除字符串首尾空白并拒绝空值。

    Args:
        value: 待规范化的字符串。
        label: 错误信息中使用的字段名。

    Returns:
        str: 去除首尾空白的非空字符串。

    Raises:
        MarketSchemaError: 输入不是字符串或规范化后为空时抛出。
    """
    if not isinstance(value, str):
        raise MarketSchemaError(f"{label} 必须是字符串")
    normalized = value.strip()
    if not normalized:
        raise MarketSchemaError(f"{label} 不能为空")
    return normalized


def _normalize_optional(value: Optional[str], label: str) -> Optional[str]:
    """
    规范化可选字符串，同时拒绝空白伪值。

    Args:
        value: 待规范化的可选字符串。
        label: 错误信息中使用的字段名。

    Returns:
        Optional[str]: ``None`` 或规范化后的非空字符串。

    Raises:
        MarketSchemaError: 非 ``None`` 输入规范化后为空时抛出。
    """
    if value is None:
        return None
    return _normalize_nonempty(value, label)


def _parse_version(value: str, label: str) -> Tuple[int, int, int]:
    """
    将 ``1``、``1.2`` 或 ``1.2.3`` 解析为可比较的三元组。

    Args:
        value: 待解析的版本字符串。
        label: 错误信息中使用的字段名。

    Returns:
        Tuple[int, int, int]: major/minor/patch 三元组。

    Raises:
        MarketSchemaError: 版本不是字符串或格式不合法时抛出。
    """
    if not isinstance(value, str):
        raise MarketSchemaError(f"{label}: schema version 必须是字符串")
    normalized = value.strip()
    matched = _VERSION_PATTERN.fullmatch(normalized)
    if matched is None:
        raise MarketSchemaError(f"{label}: 非法 schema version {value!r}")
    parts = tuple(int(part) if part is not None else 0 for part in matched.groups())
    return parts[0], parts[1], parts[2]


def _format_version(value: Tuple[int, int, int]) -> str:
    """
    将版本三元组格式化为稳定的三段字符串。

    Args:
        value: major/minor/patch 三元组。

    Returns:
        str: ``major.minor.patch`` 形式的版本。
    """
    return ".".join(str(part) for part in value)


def _normalize_version(value: str, label: str) -> str:
    """
    将版本转换为稳定的三段字符串。

    Args:
        value: 待规范化的版本。
        label: 错误信息中使用的字段名。

    Returns:
        str: 三段版本字符串。

    Raises:
        MarketSchemaError: 版本无法解析时抛出。
    """
    try:
        return _format_version(_parse_version(value, label))
    except MarketSchemaError as exc:
        raise MarketSchemaError(f"{label}: {exc}") from exc


def _validate_migration_path(
    source_version: str,
    target_version: str,
    migration_path: Tuple[str, ...],
) -> Tuple[str, ...]:
    """
    校验迁移路径从源版本连续、同 major 且严格前向到达目标版本。

    Args:
        source_version: 投影前 owned record 的 schema 版本。
        target_version: 投影目标 schema 版本。
        migration_path: ``source->target`` 形式的显式迁移边序列。

    Returns:
        Tuple[str, ...]: 规范化后的迁移路径。

    Raises:
        SchemaProjectionError: 路径格式、连续性、major 或目标不自洽时抛出。
    """
    try:
        source = _normalize_version(source_version, "projection source schema version")
        target = _normalize_version(target_version, "projection target schema version")
    except MarketSchemaError as exc:
        raise SchemaProjectionError(str(exc)) from exc
    edges = tuple(migration_path)
    if not edges:
        if source != target:
            raise SchemaProjectionError("source schema version 不同但 migration_path 为空")
        return ()
    current = source
    normalized_edges: List[str] = []
    for raw_edge in edges:
        if not isinstance(raw_edge, str) or raw_edge.count("->") != 1:
            raise SchemaProjectionError("migration_path 必须使用 source->target 字符串")
        raw_start, raw_end = raw_edge.split("->", 1)
        try:
            start = _normalize_version(raw_start, "migration source version")
            end = _normalize_version(raw_end, "migration target version")
            start_parts = _parse_version(start, "migration source version")
            end_parts = _parse_version(end, "migration target version")
        except MarketSchemaError as exc:
            raise SchemaProjectionError(str(exc)) from exc
        if start != current:
            raise SchemaProjectionError("migration_path 不连续或起点不匹配")
        if start_parts[0] != end_parts[0] or end_parts <= start_parts:
            raise SchemaProjectionError("migration_path 必须同 major 且严格前向")
        normalized_edges.append(f"{start}->{end}")
        current = end
    if current != target:
        raise SchemaProjectionError("migration_path 未到达目标 schema version")
    return tuple(normalized_edges)


def _freeze_value(value: Any) -> Any:
    """
    深复制并冻结 schema 边界允许的 JSON-like 值。

    Args:
        value: 厂商字段、canonical 转换值或投影容器。

    Returns:
        Any: mapping 转为只读 mapping，list/tuple 转为 tuple，bytearray
        转为 bytes 后的独立值。

    Raises:
        VendorRecordValidationError: 键类型、浮点值或对象类型不能无损传输时抛出。
    """
    if value is None or isinstance(value, (bool, int, str, bytes)):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise VendorRecordValidationError("schema 值不允许 NaN 或 Infinity")
        return value
    if isinstance(value, Mapping):
        frozen: Dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise VendorRecordValidationError("schema mapping 键必须是非空字符串")
            frozen[key] = _freeze_value(item)
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    raise VendorRecordValidationError(f"schema 不支持值类型: {type(value).__name__}")


def _normalize_path(value: str, label: str) -> str:
    """
    校验并规范化点分层字段路径。

    Args:
        value: 如 ``depth.bid_prices`` 的字段路径。
        label: 错误信息中使用的字段名。

    Returns:
        str: 每个分段均非空的点分层路径。

    Raises:
        MarketSchemaError: 路径为空或包含空分段时抛出。
    """
    normalized = _normalize_nonempty(value, label)
    parts = tuple(part.strip() for part in normalized.split("."))
    if any(not part for part in parts):
        raise MarketSchemaError(f"{label} 包含空路径分段")
    return ".".join(parts)


def _paths_conflict(first: str, second: str) -> bool:
    """
    判断两个点分层路径是否重复或存在父子冲突。

    Args:
        first: 第一个已规范化路径。
        second: 第二个已规范化路径。

    Returns:
        bool: 路径无法同时安全写入同一 mapping 时返回 true。
    """
    return first == second or first.startswith(second + ".") or second.startswith(first + ".")


def _assign_path(target: MutableMapping[str, Any], path: str, value: Any) -> None:
    """
    向已通过冲突校验的可变 mapping 写入点分层值。

    Args:
        target: 待写入的可变 mapping。
        path: 已规范化的目标路径。
        value: 待写入的已拥有值。

    Returns:
        None: 写入成功后正常返回。

    Side Effects:
        修改 ``target`` 及其新建子 mapping。
    """
    current = target
    parts = path.split(".")
    for part in parts[:-1]:
        child = current.get(part)
        if child is None:
            child = {}
            current[part] = child
        if not isinstance(child, MutableMapping):
            raise SchemaProjectionError(f"投影路径与已有值冲突: {path}")
        current = child
    current[parts[-1]] = value


def _read_path(source: Mapping[str, Any], path: str) -> Tuple[bool, Any]:
    """
    按点分层路径读取 mapping，并区分缺键与值为 ``None``。

    Args:
        source: 待读取的 mapping。
        path: 规范化字段路径。

    Returns:
        Tuple[bool, Any]: 第一项表示路径是否存在，第二项是对应值。
    """
    current: Any = source
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return False, None
        current = current[part]
    return True, current


def _leaf_paths(source: Mapping[str, Any], prefix: str = "") -> Tuple[str, ...]:
    """
    稳定枚举 nested mapping 的所有叶子路径。

    Args:
        source: 待枚举的 nested mapping。
        prefix: 递归使用的已有路径前缀。

    Returns:
        Tuple[str, ...]: 按字典序排列的叶子路径。
    """
    return tuple(_leaf_values(source, prefix))


def _leaf_values(source: Mapping[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    枚举 nested mapping 的叶子路径和值，并将非根空 mapping 视为叶子。

    Args:
        source: 待枚举的 nested mapping。
        prefix: 递归使用的已有路径前缀。

    Returns:
        Dict[str, Any]: 路径到叶子值的稳定 mapping。

    Raises:
        SchemaProjectionError: mapping 键非法或两种结构形成同一歧义路径时抛出。
    """
    if not source:
        return {prefix: source} if prefix else {}
    items: List[Tuple[str, Any]] = []
    for key, value in source.items():
        if not isinstance(key, str) or not key:
            raise SchemaProjectionError("projection mapping 键必须是非空字符串")
        items.append((key, value))
    values: Dict[str, Any] = {}
    for key, value in sorted(items, key=lambda item: item[0]):
        path = f"{prefix}.{key}" if prefix else key
        nested = _leaf_values(value, path) if isinstance(value, Mapping) else {path: value}
        overlap = set(values).intersection(nested)
        if overlap:
            raise SchemaProjectionError(f"projection mapping 路径有歧义: {sorted(overlap)}")
        values.update(nested)
    return values


def _projection_leaf_values(
    payload: Mapping[str, Any],
    provider_extension: Mapping[str, Any],
    raw_profile: Mapping[str, Any],
) -> Dict[str, Any]:
    """
    收集三层投影中必须逐叶提供 presence 或 missing reason 的值。

    Args:
        payload: canonical payload。
        provider_extension: vendor 扩展 payload。
        raw_profile: 包含 record 元数据及 values/enums/bytes 的 raw payload。

    Returns:
        Dict[str, Any]: 带层前缀的证据路径和值；raw record 元数据不计入字段证据。

    Raises:
        SchemaProjectionError: 三层路径冲突或 raw profile 结构不完整时抛出。
    """
    values = _leaf_values(payload, "payload") if payload else {}
    provider_values = (
        _leaf_values(provider_extension, "provider_extension") if provider_extension else {}
    )
    overlap = set(values).intersection(provider_values)
    if overlap:  # pragma: no cover - 固定顶层前缀使其仅作防御
        raise SchemaProjectionError(f"projection layer 路径冲突: {sorted(overlap)}")
    values.update(provider_values)
    expected_sections = {"record", "values", "enums", "bytes"}
    for namespace, raw_namespace in raw_profile.items():
        if not isinstance(namespace, str) or not namespace:
            raise SchemaProjectionError("raw profile namespace 必须是非空字符串")
        if not isinstance(raw_namespace, Mapping):
            raise SchemaProjectionError("raw profile namespace 必须映射到 mapping")
        if set(raw_namespace) != expected_sections:
            raise SchemaProjectionError("raw profile 必须精确包含 record/values/enums/bytes")
        if not isinstance(raw_namespace["record"], Mapping):
            raise SchemaProjectionError("raw profile record 必须是 mapping")
        for section in ("values", "enums", "bytes"):
            section_values = raw_namespace[section]
            if not isinstance(section_values, Mapping):
                raise SchemaProjectionError(f"raw profile {section} 必须是 mapping")
            if not section_values:
                continue
            nested = _leaf_values(section_values, f"raw_profile.{namespace}.{section}")
            overlap = set(values).intersection(nested)
            if overlap:
                raise SchemaProjectionError(f"projection layer 路径冲突: {sorted(overlap)}")
            values.update(nested)
    return values


def _normalize_projection_paths(values: Tuple[str, ...], label: str) -> Tuple[str, ...]:
    """
    规范化投影证据路径，并拒绝非字符串和重复声明。

    Args:
        values: 待校验的路径序列。
        label: 错误信息中使用的字段名。

    Returns:
        Tuple[str, ...]: 排序后的唯一规范路径。

    Raises:
        SchemaProjectionError: 路径类型、格式或唯一性不满足合同时抛出。
    """
    normalized: List[str] = []
    for value in values:
        if not isinstance(value, str):
            raise SchemaProjectionError(f"{label} 只允许字符串路径")
        try:
            path = _normalize_path(value, label)
        except MarketSchemaError as exc:
            raise SchemaProjectionError(str(exc)) from exc
        if path in normalized:
            raise SchemaProjectionError(f"{label} 不允许重复路径: {path}")
        normalized.append(path)
    return tuple(sorted(normalized))


def _validate_projection_evidence(
    leaf_values: Mapping[str, Any],
    field_presence: Tuple[str, ...],
    missing_fields: Mapping[str, "MissingField"],
    *,
    validate_layers: bool,
) -> None:
    """
    校验每个字段叶子与 presence/missing 证据一一对应。

    Args:
        leaf_values: 投影中的路径到实际叶子值 mapping。
        field_presence: 声明非空存在的路径。
        missing_fields: 声明为 null 及其原因的路径。
        validate_layers: 是否按三层路径前缀校验 MissingField.layer。

    Returns:
        None: 所有叶子值与证据精确对应时正常返回。

    Raises:
        SchemaProjectionError: 路径缺证据、多证据、值语义或层归属不一致时抛出。
    """
    present_paths = set(field_presence)
    missing_paths = set(missing_fields)
    overlap = present_paths.intersection(missing_paths)
    if overlap:
        raise SchemaProjectionError(f"字段不能同时 present 与 missing: {sorted(overlap)}")
    evidence_paths = present_paths.union(missing_paths)
    leaf_paths = set(leaf_values)
    if evidence_paths != leaf_paths:
        missing_evidence = leaf_paths.difference(evidence_paths)
        unknown_evidence = evidence_paths.difference(leaf_paths)
        raise SchemaProjectionError(
            "字段叶子与 presence/missing 证据不一致: "
            f"missing={sorted(missing_evidence)}, unknown={sorted(unknown_evidence)}"
        )
    for path in field_presence:
        if leaf_values[path] is None:
            raise SchemaProjectionError(f"present 字段必须为非 None: {path}")
    for path, missing in missing_fields.items():
        if leaf_values[path] is not None:
            raise SchemaProjectionError(f"missing 字段必须为 None: {path}")
        if not validate_layers:
            continue
        if path.startswith("payload."):
            expected_layer = FieldLayer.CANONICAL
        elif path.startswith("provider_extension."):
            expected_layer = FieldLayer.VENDOR
        elif path.startswith("raw_profile."):
            expected_layer = FieldLayer.RAW
        else:  # pragma: no cover - 精确叶子集合已限制固定顶层前缀
            raise SchemaProjectionError(f"未知 projection layer 路径: {path}")
        if missing.layer is not expected_layer:
            raise SchemaProjectionError(f"missing field layer 与路径不一致: {path}")


@dataclass(frozen=True)
class SourceLayoutIdentity:
    """绑定可被同一字段 schema 安全解释的一套厂商来源与内存布局证据。"""

    provider: str
    module: str
    sdk_version: str
    source_callback: str
    source_struct: str
    source_struct_size: int
    manifest_hash: str

    def __post_init__(self) -> None:
        """
        规范化来源身份，并拒绝缺失证据或非法 struct size。

        Args:
            本方法只使用 dataclass 字段。

        Returns:
            None: 来源与布局证据校验通过后正常返回。

        Raises:
            MarketSchemaError: 任一身份字段为空或 struct size 非正整数时抛出。
        """
        normalized = {
            "provider": _normalize_nonempty(self.provider, "source provider"),
            "module": _normalize_nonempty(self.module, "source module"),
            "sdk_version": _normalize_nonempty(self.sdk_version, "source sdk_version"),
            "source_callback": _normalize_nonempty(self.source_callback, "source callback"),
            "source_struct": _normalize_nonempty(self.source_struct, "source struct"),
            "manifest_hash": _normalize_nonempty(self.manifest_hash, "manifest hash"),
        }
        if (
            isinstance(self.source_struct_size, bool)
            or not isinstance(self.source_struct_size, int)
            or self.source_struct_size <= 0
        ):
            raise MarketSchemaError("source struct size 必须是正整数")
        for name, value in normalized.items():
            object.__setattr__(self, name, value)


def _validate_raw_record_identity(
    raw_profile: Mapping[str, Any],
    *,
    source_layout: SourceLayoutIdentity,
    vendor_schema_id: str,
    record_schema_id: str,
    record_schema_version: str,
    mapping_version: str,
    redacted_fields: Tuple[str, ...],
) -> None:
    """
    校验 raw record 只含安全来源元数据且与投影外层身份一致。

    Args:
        raw_profile: canonical_with_raw profile 的厂商 namespace mapping。
        source_layout: 投影顶层完整来源与内存布局身份。
        vendor_schema_id: 投影外层厂商 schema ID。
        record_schema_id: 投影外层 owned record schema ID。
        record_schema_version: 投影外层 owned record schema 版本。
        mapping_version: 投影使用的字段映射版本。
        redacted_fields: 投影声明的排序脱敏字段。

    Returns:
        None: 每个 raw record 均满足固定键、类型与身份一致性后返回。

    Raises:
        SchemaProjectionError: record 含未知元数据、类型非法或身份不一致时抛出。
    """
    expected_keys = {
        "provider",
        "module",
        "sdk_version",
        "vendor_schema_id",
        "record_schema_id",
        "record_schema_version",
        "source_callback",
        "source_struct",
        "source_struct_size",
        "manifest_hash",
        "mapping_version",
        "redacted_fields",
    }
    string_fields = expected_keys.difference({"source_struct_size", "redacted_fields"})
    for raw_namespace in raw_profile.values():
        if not isinstance(raw_namespace, Mapping):  # pragma: no cover - 结构 helper 已检查
            raise SchemaProjectionError("raw profile namespace 必须映射到 mapping")
        record = raw_namespace.get("record")
        if not isinstance(record, Mapping):  # pragma: no cover - 结构 helper 已检查
            raise SchemaProjectionError("raw profile record 必须是 mapping")
        if set(record) != expected_keys:
            raise SchemaProjectionError("raw profile record 含未知或缺失元数据键")
        for name in string_fields:
            value = record[name]
            if not isinstance(value, str) or value != value.strip() or not value:
                raise SchemaProjectionError(f"raw profile record {name} 必须是非空字符串")
        struct_size = record["source_struct_size"]
        if isinstance(struct_size, bool) or not isinstance(struct_size, int) or struct_size <= 0:
            raise SchemaProjectionError("raw profile record source_struct_size 必须是正整数")
        redacted_fields = record["redacted_fields"]
        if not isinstance(redacted_fields, tuple) or any(
            not isinstance(name, str) or not name.strip() for name in redacted_fields
        ):
            raise SchemaProjectionError("raw profile record redacted_fields 必须是字符串 tuple")
        if tuple(sorted(set(redacted_fields))) != redacted_fields:
            raise SchemaProjectionError("raw profile record redacted_fields 必须排序且唯一")
        if record["provider"] != source_layout.provider:
            raise SchemaProjectionError("raw profile record provider 与投影不一致")
        if record["module"] != source_layout.module:
            raise SchemaProjectionError("raw profile record module 与投影不一致")
        if record["sdk_version"] != source_layout.sdk_version:
            raise SchemaProjectionError("raw profile record sdk_version 与投影不一致")
        if record["vendor_schema_id"] != vendor_schema_id:
            raise SchemaProjectionError("raw profile record vendor_schema_id 与投影不一致")
        if record["record_schema_id"] != record_schema_id:
            raise SchemaProjectionError("raw profile record schema_id 与投影不一致")
        try:
            raw_version = _normalize_version(
                record["record_schema_version"], "raw record_schema_version"
            )
        except MarketSchemaError as exc:
            raise SchemaProjectionError(str(exc)) from exc
        if raw_version != record_schema_version:
            raise SchemaProjectionError("raw profile record schema version 与投影不一致")
        if record["source_callback"] != source_layout.source_callback:
            raise SchemaProjectionError("raw profile record source_callback 与投影不一致")
        if record["source_struct"] != source_layout.source_struct:
            raise SchemaProjectionError("raw profile record source_struct 与投影不一致")
        if record["source_struct_size"] != source_layout.source_struct_size:
            raise SchemaProjectionError("raw profile record source_struct_size 与投影不一致")
        if record["manifest_hash"] != source_layout.manifest_hash:
            raise SchemaProjectionError("raw profile record manifest_hash 与投影不一致")
        if record["mapping_version"] != mapping_version:
            raise SchemaProjectionError("raw profile record mapping_version 与投影不一致")
        if record["redacted_fields"] != redacted_fields:
            raise SchemaProjectionError("raw profile record redacted_fields 与投影不一致")


@dataclass(frozen=True)
class VendorFieldObservation:
    """表示 owned record 中一个厂商字段的值、存在性与原始证据。"""

    source_type: str
    value: Any = None
    present: bool = True
    missing_reason: Optional[FieldMissingReason] = None
    raw_enum: Optional[Union[str, int, bytes]] = None
    raw_bytes: Optional[bytes] = None

    def __post_init__(self) -> None:
        """
        校验缺失语义、int64 边界与原始 enum/bytes，并深复制值。

        Args:
            本方法只使用 dataclass 字段。

        Returns:
            None: 校验和冻结完成后正常返回。

        Raises:
            VendorRecordValidationError: 存在性、类型或数值边界不自洽时抛出。
        """
        try:
            source_type = VendorSourceType(
                _normalize_nonempty(self.source_type, "source_type").lower()
            ).value
        except MarketSchemaError as exc:
            raise VendorRecordValidationError(str(exc)) from exc
        except ValueError as exc:
            raise VendorRecordValidationError("未知 source_type") from exc
        if not isinstance(self.present, bool):
            raise VendorRecordValidationError("present 必须是 bool")
        try:
            missing_reason = (
                None if self.missing_reason is None else FieldMissingReason(self.missing_reason)
            )
        except ValueError as exc:
            raise VendorRecordValidationError("未知字段缺失原因") from exc
        if self.present:
            if missing_reason is not None:
                raise VendorRecordValidationError("present 字段不能同时声明 missing_reason")
            if self.value is None:
                raise VendorRecordValidationError("present 字段必须携带明确值")
        else:
            if missing_reason is None:
                raise VendorRecordValidationError("missing 字段必须携带 missing_reason")
            if self.value is not None:
                raise VendorRecordValidationError("missing 字段的规范值必须为 None")

        value = _freeze_value(self.value)
        raw_enum = self.raw_enum
        if isinstance(raw_enum, bytearray):
            raw_enum = bytes(raw_enum)
        if raw_enum is not None and (
            isinstance(raw_enum, bool) or not isinstance(raw_enum, (str, int, bytes))
        ):
            raise VendorRecordValidationError("raw_enum 只允许 str/int/bytes")
        raw_bytes = self.raw_bytes
        if isinstance(raw_bytes, bytearray):
            raw_bytes = bytes(raw_bytes)
        if raw_bytes is not None and not isinstance(raw_bytes, bytes):
            raise VendorRecordValidationError("raw_bytes 必须是 bytes")
        if missing_reason is FieldMissingReason.REDACTED and (
            raw_enum is not None or raw_bytes is not None
        ):
            raise VendorRecordValidationError("REDACTED 字段不得携带 raw enum/bytes")

        signed_widths = {"int8": 8, "int16": 16, "int32": 32, "int64": 64}
        unsigned_widths = {"uint8": 8, "uint16": 16, "uint32": 32, "uint64": 64}
        if source_type in signed_widths and self.present:
            bits = signed_widths[source_type]
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not -(2 ** (bits - 1)) <= value <= 2 ** (bits - 1) - 1
            ):
                raise VendorRecordValidationError(f"{source_type} 字段必须精确位于有符号 {bits} 位范围")
        elif source_type in unsigned_widths and self.present:
            bits = unsigned_widths[source_type]
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 0 <= value <= 2**bits - 1
            ):
                raise VendorRecordValidationError(f"{source_type} 字段必须精确位于无符号 {bits} 位范围")
        elif source_type == "bool" and self.present and not isinstance(value, bool):
            raise VendorRecordValidationError("bool 字段必须使用 bool")
        elif source_type in {"float", "double"} and self.present and not isinstance(value, float):
            raise VendorRecordValidationError(f"{source_type} 字段必须使用有限 float")
        elif source_type == "string" and self.present and not isinstance(value, str):
            raise VendorRecordValidationError("string 字段必须使用 str")
        elif source_type == "bytes" and self.present and not isinstance(value, bytes):
            raise VendorRecordValidationError("bytes 字段必须使用 bytes/bytearray")
        elif (
            source_type == "enum"
            and self.present
            and (isinstance(value, bool) or not isinstance(value, (str, int, bytes)))
        ):
            raise VendorRecordValidationError("enum 字段只允许 str/int/bytes 且不接受 bool")
        elif source_type == "array" and self.present and not isinstance(value, tuple):
            raise VendorRecordValidationError("array 字段必须使用 list/tuple")

        if source_type == "enum" and self.present and raw_enum is None:
            raw_enum = value
        if source_type == "bytes" and self.present and raw_bytes is None:
            raw_bytes = value
        object.__setattr__(self, "source_type", source_type)
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "missing_reason", missing_reason)
        object.__setattr__(self, "raw_enum", raw_enum)
        object.__setattr__(self, "raw_bytes", raw_bytes)


@dataclass(frozen=True)
class OwnedVendorRecord:
    """保存与厂商指针和 struct 生命周期脱离的版本化字段记录。"""

    provider: str
    module: str
    sdk_version: str
    vendor_schema_id: str
    record_schema_id: str
    record_schema_version: str
    source_callback: str
    source_struct: str
    source_struct_size: int
    manifest_hash: str
    mapping_version: str
    fields: Mapping[str, VendorFieldObservation]
    field_fidelity: bool = True
    redacted_fields: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """
        规范化记录身份，复制字段 mapping 并校验结构尺寸。

        Args:
            本方法只使用 dataclass 字段。

        Returns:
            None: 记录完成拥有权隔离后正常返回。

        Raises:
            VendorRecordValidationError: 身份、尺寸、字段类型或脱敏声明非法时抛出。
        """
        try:
            normalized = {
                "provider": _normalize_nonempty(self.provider, "provider"),
                "module": _normalize_nonempty(self.module, "module"),
                "sdk_version": _normalize_nonempty(self.sdk_version, "sdk_version"),
                "vendor_schema_id": _normalize_nonempty(self.vendor_schema_id, "vendor_schema_id"),
                "record_schema_id": _normalize_nonempty(self.record_schema_id, "record_schema_id"),
                "record_schema_version": _normalize_version(
                    self.record_schema_version, "record_schema_version"
                ),
                "source_callback": _normalize_nonempty(self.source_callback, "source_callback"),
                "source_struct": _normalize_nonempty(self.source_struct, "source_struct"),
                "manifest_hash": _normalize_nonempty(self.manifest_hash, "manifest_hash"),
                "mapping_version": _normalize_nonempty(self.mapping_version, "mapping_version"),
            }
        except MarketSchemaError as exc:
            raise VendorRecordValidationError(str(exc)) from exc
        if (
            isinstance(self.source_struct_size, bool)
            or not isinstance(self.source_struct_size, int)
            or self.source_struct_size <= 0
        ):
            raise VendorRecordValidationError("source_struct_size 必须是正整数")
        if not isinstance(self.field_fidelity, bool):
            raise VendorRecordValidationError("field_fidelity 必须是 bool")
        copied_fields: Dict[str, VendorFieldObservation] = {}
        for name, observation in self.fields.items():
            normalized_name = _normalize_nonempty(name, "vendor field name")
            if normalized_name in copied_fields:
                raise VendorRecordValidationError(f"重复 vendor field: {normalized_name}")
            if not isinstance(observation, VendorFieldObservation):
                raise VendorRecordValidationError(
                    f"vendor field {normalized_name} 必须是 VendorFieldObservation"
                )
            copied_fields[normalized_name] = observation
        redacted_fields = tuple(
            sorted({_normalize_nonempty(name, "redacted field") for name in self.redacted_fields})
        )
        for redacted_name in redacted_fields:
            redacted_observation = copied_fields.get(redacted_name)
            if redacted_observation is not None and (
                redacted_observation.present
                or redacted_observation.missing_reason is not FieldMissingReason.REDACTED
                or redacted_observation.raw_enum is not None
                or redacted_observation.raw_bytes is not None
            ):
                raise VendorRecordValidationError(f"脱敏字段 {redacted_name} 不得携带规范值或 raw 证据")
        for name, value in normalized.items():
            object.__setattr__(self, name, value)
        object.__setattr__(self, "fields", MappingProxyType(copied_fields))
        object.__setattr__(self, "redacted_fields", redacted_fields)

    def evolve(
        self,
        *,
        record_schema_version: str,
        fields: Mapping[str, VendorFieldObservation],
        mapping_version: Optional[str] = None,
    ) -> "OwnedVendorRecord":
        """
        为显式 schema 迁移生成保持源身份的新 owned record。

        Args:
            record_schema_version: 迁移后的记录版本。
            fields: 迁移后的完整字段观测 mapping。
            mapping_version: 可选的新映射版本，默认保持原值。

        Returns:
            OwnedVendorRecord: 与原记录分离且重新校验的新版本记录。

        Side Effects:
            无；原记录不会被修改。
        """
        return replace(
            self,
            record_schema_version=record_schema_version,
            fields=fields,
            mapping_version=self.mapping_version if mapping_version is None else mapping_version,
        )

    @property
    def source_layout_identity(self) -> SourceLayoutIdentity:
        """
        返回投影 schema 必须整组精确验收的来源与布局身份。

        Returns:
            SourceLayoutIdentity: provider/module/SDK/callback/struct/size/manifest 组合。
        """
        return SourceLayoutIdentity(
            provider=self.provider,
            module=self.module,
            sdk_version=self.sdk_version,
            source_callback=self.source_callback,
            source_struct=self.source_struct,
            source_struct_size=self.source_struct_size,
            manifest_hash=self.manifest_hash,
        )

    @property
    def immutable_source_identity(self) -> Tuple[Any, ...]:
        """
        返回迁移过程不得更改的厂商源身份。

        Returns:
            Tuple[Any, ...]: provider/module/SDK/schema/callback/struct/manifest 组合。
        """
        return (
            self.provider,
            self.module,
            self.sdk_version,
            self.vendor_schema_id,
            self.record_schema_id,
            self.source_callback,
            self.source_struct,
            self.source_struct_size,
            self.manifest_hash,
        )


FieldConverter = Callable[[Any], Any]
VendorRecordMigration = Callable[[OwnedVendorRecord], OwnedVendorRecord]


@dataclass(frozen=True)
class SchemaFieldRule:
    """将一个已分类厂商字段单向映射到 canonical、vendor 或 raw 层。"""

    source_field: str
    target_field: str
    layer: FieldLayer
    expected_source_type: Optional[str] = None
    enum_mapping: Mapping[Union[str, int, bytes], str] = field(default_factory=dict)
    unknown_enum_value: str = "unknown"
    converter: Optional[FieldConverter] = None
    conversion_id: Optional[str] = None

    def __post_init__(self) -> None:
        """
        校验路径、层级、枚举表与可审计转换标识。

        Args:
            本方法只使用 dataclass 字段。

        Returns:
            None: 规则冻结完成后正常返回。

        Raises:
            MarketSchemaError: 字段、枚举表或 converter/conversion_id 不自洽时抛出。
        """
        source_field = _normalize_nonempty(self.source_field, "source_field")
        target_field = _normalize_path(self.target_field, "target_field")
        try:
            layer = FieldLayer(self.layer)
        except ValueError as exc:
            raise MarketSchemaError("未知 field layer") from exc
        expected_source_type = _normalize_optional(
            self.expected_source_type, "expected_source_type"
        )
        if expected_source_type is not None:
            try:
                expected_source_type = VendorSourceType(expected_source_type.lower()).value
            except ValueError as exc:
                raise MarketSchemaError("未知 expected_source_type") from exc
        conversion_id = _normalize_optional(self.conversion_id, "conversion_id")
        if (self.converter is None) != (conversion_id is None):
            raise MarketSchemaError("converter 与 conversion_id 必须同时提供")
        if self.converter is not None and not callable(self.converter):
            raise MarketSchemaError("converter 必须可调用")
        enum_mapping: Dict[Union[str, int, bytes], str] = {}
        for raw_value, canonical_value in self.enum_mapping.items():
            if isinstance(raw_value, bool) or not isinstance(raw_value, (str, int, bytes)):
                raise MarketSchemaError("enum_mapping 原值只允许 str/int/bytes")
            enum_mapping[raw_value] = _normalize_nonempty(
                canonical_value, "enum_mapping canonical value"
            )
        unknown_enum_value = _normalize_nonempty(self.unknown_enum_value, "unknown_enum_value")
        object.__setattr__(self, "source_field", source_field)
        object.__setattr__(self, "target_field", target_field)
        object.__setattr__(self, "layer", layer)
        object.__setattr__(self, "expected_source_type", expected_source_type)
        object.__setattr__(self, "enum_mapping", MappingProxyType(enum_mapping))
        object.__setattr__(self, "unknown_enum_value", unknown_enum_value)
        object.__setattr__(self, "conversion_id", conversion_id)


@dataclass(frozen=True)
class MarketDataProjectionSchema:
    """声明一个 owned record 版本到三层 payload 的可验证映射合同。"""

    schema_id: str
    schema_version: str
    vendor_schema_id: str
    field_set_version: str
    mapping_version: str
    vendor_namespace: str
    source_layouts: Tuple[SourceLayoutIdentity, ...]
    field_rules: Tuple[SchemaFieldRule, ...]
    redacted_fields: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """
        冻结 schema 并拒绝未分类来源、重复映射和父子路径冲突。

        Args:
            本方法只使用 dataclass 字段。

        Returns:
            None: schema 校验与冻结完成后正常返回。

        Raises:
            MarketSchemaError: 版本、结构、manifest 或字段分类合同不完整时抛出。
        """
        schema_id = _normalize_nonempty(self.schema_id, "schema_id")
        schema_version = _normalize_version(self.schema_version, "schema_version")
        vendor_schema_id = _normalize_nonempty(self.vendor_schema_id, "vendor_schema_id")
        field_set_version = _normalize_nonempty(self.field_set_version, "field_set_version")
        mapping_version = _normalize_nonempty(self.mapping_version, "mapping_version")
        vendor_namespace = _normalize_nonempty(self.vendor_namespace, "vendor_namespace")
        source_layouts = tuple(self.source_layouts)
        if not source_layouts or not all(
            isinstance(layout, SourceLayoutIdentity) for layout in source_layouts
        ):
            raise MarketSchemaError("source_layouts 必须是非空 SourceLayoutIdentity 序列")
        if len(set(source_layouts)) != len(source_layouts):
            raise MarketSchemaError("source_layouts 不允许重复身份")
        rules = tuple(self.field_rules)
        if not rules or not all(isinstance(rule, SchemaFieldRule) for rule in rules):
            raise MarketSchemaError("field_rules 必须是非空 SchemaFieldRule 序列")
        source_fields: Set[str] = set()
        paths_by_layer: Dict[FieldLayer, List[str]] = {layer: [] for layer in FieldLayer}
        for rule in rules:
            if rule.source_field in source_fields:
                raise MarketSchemaError(f"厂商字段重复分类: {rule.source_field}")
            for existing_path in paths_by_layer[rule.layer]:
                if _paths_conflict(existing_path, rule.target_field):
                    raise MarketSchemaError(
                        f"{rule.layer.value} 目标路径冲突: " f"{existing_path} / {rule.target_field}"
                    )
            source_fields.add(rule.source_field)
            paths_by_layer[rule.layer].append(rule.target_field)
        redacted_fields = tuple(
            sorted({_normalize_nonempty(item, "redacted field") for item in self.redacted_fields})
        )
        overlap = source_fields.intersection(redacted_fields)
        if overlap:
            raise MarketSchemaError(f"字段不能同时映射与脱敏: {sorted(overlap)}")
        object.__setattr__(self, "schema_id", schema_id)
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "vendor_schema_id", vendor_schema_id)
        object.__setattr__(self, "field_set_version", field_set_version)
        object.__setattr__(self, "mapping_version", mapping_version)
        object.__setattr__(self, "vendor_namespace", vendor_namespace)
        object.__setattr__(self, "source_layouts", source_layouts)
        object.__setattr__(self, "field_rules", rules)
        object.__setattr__(self, "redacted_fields", redacted_fields)


@dataclass(frozen=True)
class MissingField:
    """记录投影字段为 null 的稳定原因和来源分类。"""

    source_field: str
    layer: FieldLayer
    reason: FieldMissingReason

    def __post_init__(self) -> None:
        """
        规范化缺失字段元数据。

        Args:
            本方法只使用 dataclass 字段。

        Returns:
            None: 校验通过后正常返回。

        Raises:
            MarketSchemaError: 字段名、层或原因非法时抛出。
        """
        source_field = _normalize_nonempty(self.source_field, "missing source_field")
        try:
            layer = FieldLayer(self.layer)
            reason = FieldMissingReason(self.reason)
        except ValueError as exc:
            raise MarketSchemaError("未知 missing field layer/reason") from exc
        object.__setattr__(self, "source_field", source_field)
        object.__setattr__(self, "layer", layer)
        object.__setattr__(self, "reason", reason)


@dataclass(frozen=True)
class SchemaProjection:
    """
    保存按 field profile 生成的不可变 canonical/vendor/raw 三层投影。

    本类型是 projector 的只读输出和结构一致性容器，不是对同进程恶意 Python
    代码的认证令牌。跨进程或外部输入必须从受信 owned record/schema 重新投影，
    不得把调用方自行构造的对象当作来源证明；传输应使用 MarketEvent codec。
    """

    schema_id: str
    schema_version: str
    field_set_version: str
    field_profile: FieldProfile
    provider: str
    vendor_schema_id: str
    source_layout: SourceLayoutIdentity
    mapping_version: str
    source_record_schema_version: str
    redacted_fields: Tuple[str, ...]
    field_sources: Mapping[str, str]
    payload: Mapping[str, Any]
    provider_extension: Mapping[str, Any]
    raw_profile: Mapping[str, Any]
    field_presence: Tuple[str, ...]
    completeness: bool
    missing_fields: Mapping[str, MissingField]
    migration_path: Tuple[str, ...] = ()
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        """
        冻结投影容器并校验 presence/completeness/missing 互斥关系。

        Args:
            本方法只使用 dataclass 字段。

        Returns:
            None: 投影自洽且冻结后正常返回。

        Raises:
            SchemaProjectionError: 字段集合或完整性声明自相矛盾时抛出。
        """
        if _factory_token is not _SCHEMA_PROJECTION_FACTORY_TOKEN:
            raise SchemaProjectionError("SchemaProjection 必须由 MarketDataSchemaProjector 创建")
        schema_id = _normalize_nonempty(self.schema_id, "projection schema_id")
        schema_version = _normalize_version(self.schema_version, "projection schema_version")
        field_set_version = _normalize_nonempty(
            self.field_set_version, "projection field_set_version"
        )
        provider = _normalize_nonempty(self.provider, "projection provider")
        vendor_schema_id = _normalize_nonempty(self.vendor_schema_id, "projection vendor_schema_id")
        if not isinstance(self.source_layout, SourceLayoutIdentity):
            raise SchemaProjectionError("projection source_layout 必须是 SourceLayoutIdentity")
        if provider != self.source_layout.provider:
            raise SchemaProjectionError("projection provider 与 source_layout 不一致")
        mapping_version = _normalize_nonempty(self.mapping_version, "projection mapping_version")
        source_record_schema_version = _normalize_version(
            self.source_record_schema_version,
            "projection source_record_schema_version",
        )
        redacted_fields = tuple(
            sorted(
                {
                    _normalize_nonempty(name, "projection redacted field")
                    for name in self.redacted_fields
                }
            )
        )
        try:
            field_profile = FieldProfile(self.field_profile)
        except (TypeError, ValueError) as exc:
            raise SchemaProjectionError("未知 field profile") from exc
        if not isinstance(self.completeness, bool):
            raise SchemaProjectionError("projection completeness 必须是 bool")
        for label, container in (
            ("payload", self.payload),
            ("provider_extension", self.provider_extension),
            ("raw_profile", self.raw_profile),
        ):
            if not isinstance(container, Mapping):
                raise SchemaProjectionError(f"{label} 必须是 mapping")
        if field_profile is FieldProfile.CANONICAL:
            if self.provider_extension or self.raw_profile:
                raise SchemaProjectionError("canonical profile 不得携带 vendor/raw layer")
        elif field_profile is FieldProfile.CANONICAL_WITH_VENDOR:
            if not self.provider_extension or self.raw_profile:
                raise SchemaProjectionError("canonical_with_vendor 必须携带 vendor 且不得携带 raw layer")
        elif not self.provider_extension or not self.raw_profile:
            raise SchemaProjectionError("canonical_with_raw 必须同时携带 vendor 与 raw layer")
        if field_profile is FieldProfile.CANONICAL_WITH_RAW and set(self.provider_extension) != set(
            self.raw_profile
        ):
            raise SchemaProjectionError("vendor/raw namespace 必须一致")
        field_presence = _normalize_projection_paths(
            self.field_presence, "projection field_presence"
        )
        if not isinstance(self.missing_fields, Mapping):
            raise SchemaProjectionError("missing_fields 必须是 mapping")
        missing_fields: Dict[str, MissingField] = {}
        for raw_path, missing in self.missing_fields.items():
            if not isinstance(raw_path, str):
                raise SchemaProjectionError("missing_fields 键必须是字符串路径")
            path = _normalize_path(raw_path, "projection missing path")
            if path in missing_fields:
                raise SchemaProjectionError(f"missing_fields 不允许重复路径: {path}")
            if not isinstance(missing, MissingField):
                raise SchemaProjectionError("missing_fields 必须映射到 MissingField")
            missing_fields[path] = missing
        if not isinstance(self.field_sources, Mapping):
            raise SchemaProjectionError("field_sources 必须是 mapping")
        field_sources: Dict[str, str] = {}
        for raw_path, raw_source_field in self.field_sources.items():
            if not isinstance(raw_path, str):
                raise SchemaProjectionError("field_sources 键必须是字符串路径")
            path = _normalize_path(raw_path, "field_sources path")
            if path in field_sources:
                raise SchemaProjectionError(f"field_sources 不允许重复路径: {path}")
            field_sources[path] = _normalize_nonempty(
                raw_source_field, "field_sources source field"
            )
        leaf_values = _projection_leaf_values(
            self.payload, self.provider_extension, self.raw_profile
        )
        unknown_source_paths = set(field_sources).difference(leaf_values)
        if unknown_source_paths:
            raise SchemaProjectionError(f"field_sources 引用不存在投影叶子: {sorted(unknown_source_paths)}")
        for path, missing in missing_fields.items():
            expected_source = field_sources.get(path)
            if expected_source is None:
                raise SchemaProjectionError(f"missing 字段缺少 field_sources 证据: {path}")
            if missing.source_field != expected_source:
                raise SchemaProjectionError(f"missing source_field 与投影规则不一致: {path}")
        if self.raw_profile:
            _validate_raw_record_identity(
                self.raw_profile,
                source_layout=self.source_layout,
                vendor_schema_id=vendor_schema_id,
                record_schema_id=schema_id,
                record_schema_version=schema_version,
                mapping_version=mapping_version,
                redacted_fields=redacted_fields,
            )
        _validate_projection_evidence(
            leaf_values, field_presence, missing_fields, validate_layers=True
        )
        if self.completeness != (not missing_fields):
            raise SchemaProjectionError("completeness 必须与 missing_fields 是否为空一致")
        migration_path = _validate_migration_path(
            source_record_schema_version,
            schema_version,
            tuple(self.migration_path),
        )
        object.__setattr__(self, "schema_id", schema_id)
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "field_set_version", field_set_version)
        object.__setattr__(self, "field_profile", field_profile)
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "vendor_schema_id", vendor_schema_id)
        object.__setattr__(self, "mapping_version", mapping_version)
        object.__setattr__(self, "source_record_schema_version", source_record_schema_version)
        object.__setattr__(self, "redacted_fields", redacted_fields)
        object.__setattr__(self, "field_sources", MappingProxyType(field_sources))
        object.__setattr__(self, "payload", _freeze_value(self.payload))
        object.__setattr__(self, "provider_extension", _freeze_value(self.provider_extension))
        object.__setattr__(self, "raw_profile", _freeze_value(self.raw_profile))
        object.__setattr__(self, "field_presence", field_presence)
        object.__setattr__(self, "missing_fields", MappingProxyType(missing_fields))
        object.__setattr__(self, "migration_path", migration_path)

    @property
    def missing_reasons(self) -> Mapping[str, str]:
        """
        返回便于协议和健康状态消费的路径到原因字符串映射。

        Returns:
            Mapping[str, str]: 不可变的缺失原因 mapping。
        """
        return MappingProxyType(
            {path: missing.reason.value for path, missing in self.missing_fields.items()}
        )


@dataclass(frozen=True)
class CompatibilityProjectionSchema:
    """声明只从 canonical payload 生成的版本化、显式有损兼容视图。"""

    schema_id: str
    schema_version: str
    source_schema_id: str
    source_schema_major: int
    field_mappings: Mapping[str, str]
    allowed_omissions: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """
        校验兼容字段路径、源 major 和必须明示的裁剪清单。

        Args:
            本方法只使用 dataclass 字段。

        Returns:
            None: 兼容 schema 校验与冻结完成后正常返回。

        Raises:
            MarketSchemaError: major、字段路径或投影目标冲突时抛出。
        """
        schema_id = _normalize_nonempty(self.schema_id, "compat schema_id")
        schema_version = _normalize_version(self.schema_version, "compat schema_version")
        source_schema_id = _normalize_nonempty(self.source_schema_id, "compat source_schema_id")
        if (
            isinstance(self.source_schema_major, bool)
            or not isinstance(self.source_schema_major, int)
            or self.source_schema_major < 0
        ):
            raise MarketSchemaError("source_schema_major 必须是非负整数")
        field_mappings: Dict[str, str] = {}
        target_paths: List[str] = []
        for target_field, source_field in self.field_mappings.items():
            target = _normalize_path(target_field, "compat target field")
            source = _normalize_path(source_field, "compat source field")
            for existing in target_paths:
                if _paths_conflict(existing, target):
                    raise MarketSchemaError(f"compatibility 目标路径冲突: {existing} / {target}")
            target_paths.append(target)
            field_mappings[target] = source
        if not field_mappings:
            raise MarketSchemaError("compat field_mappings 不能为空")
        allowed_omissions = tuple(
            sorted(
                {
                    _normalize_path(path, "compat allowed omission")
                    for path in self.allowed_omissions
                }
            )
        )
        object.__setattr__(self, "schema_id", schema_id)
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "source_schema_id", source_schema_id)
        object.__setattr__(self, "field_mappings", MappingProxyType(field_mappings))
        object.__setattr__(self, "allowed_omissions", allowed_omissions)


@dataclass(frozen=True)
class CompatibilityProjection:
    """保存只从 canonical 生成的不可变兼容 payload 及显式损失清单。"""

    schema_id: str
    schema_version: str
    source_schema_id: str
    source_schema_version: str
    payload: Mapping[str, Any]
    field_presence: Tuple[str, ...]
    completeness: bool
    missing_fields: Mapping[str, MissingField]
    omitted_canonical_fields: Tuple[str, ...]

    def __post_init__(self) -> None:
        """
        冻结兼容 payload 并校验 completeness 与 missing 一致。

        Args:
            本方法只使用 dataclass 字段。

        Returns:
            None: 兼容投影自洽后正常返回。

        Raises:
            SchemaProjectionError: presence/missing/completeness 相互矛盾时抛出。
        """
        schema_id = _normalize_nonempty(self.schema_id, "compat projection schema_id")
        schema_version = _normalize_version(self.schema_version, "compat projection schema_version")
        source_schema_id = _normalize_nonempty(
            self.source_schema_id, "compat projection source_schema_id"
        )
        source_schema_version = _normalize_version(
            self.source_schema_version, "compat projection source_schema_version"
        )
        if not isinstance(self.completeness, bool):
            raise SchemaProjectionError("compat completeness 必须是 bool")
        if not isinstance(self.payload, Mapping):
            raise SchemaProjectionError("compat payload 必须是 mapping")
        field_presence = _normalize_projection_paths(self.field_presence, "compat field_presence")
        if not isinstance(self.missing_fields, Mapping):
            raise SchemaProjectionError("compat missing_fields 必须是 mapping")
        missing_fields: Dict[str, MissingField] = {}
        for raw_path, missing in self.missing_fields.items():
            if not isinstance(raw_path, str):
                raise SchemaProjectionError("compat missing_fields 键必须是字符串路径")
            path = _normalize_path(raw_path, "compat missing path")
            if path in missing_fields:
                raise SchemaProjectionError(f"compat missing_fields 不允许重复路径: {path}")
            if not isinstance(missing, MissingField):
                raise SchemaProjectionError("compat missing_fields 必须映射到 MissingField")
            missing_fields[path] = missing
        _validate_projection_evidence(
            _leaf_values(self.payload),
            field_presence,
            missing_fields,
            validate_layers=False,
        )
        if self.completeness != (not missing_fields):
            raise SchemaProjectionError("compat completeness 与 missing_fields 不一致")
        omitted_canonical_fields = _normalize_projection_paths(
            self.omitted_canonical_fields, "omitted_canonical_fields"
        )
        object.__setattr__(self, "schema_id", schema_id)
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "source_schema_id", source_schema_id)
        object.__setattr__(self, "source_schema_version", source_schema_version)
        object.__setattr__(self, "payload", _freeze_value(self.payload))
        object.__setattr__(self, "field_presence", field_presence)
        object.__setattr__(self, "missing_fields", MappingProxyType(missing_fields))
        object.__setattr__(self, "omitted_canonical_fields", omitted_canonical_fields)


class SchemaMigrationRegistry:
    """管理同 schema major 内显式、唯一的前向 owned-record 迁移链。"""

    def __init__(self) -> None:
        """
        创建空迁移注册表。

        Returns:
            None: 新建实例初始为无迁移状态。
        """
        self._migrations: Dict[Tuple[str, str], Tuple[str, VendorRecordMigration]] = {}

    def register(
        self,
        schema_id: str,
        source_version: str,
        target_version: str,
        migration: VendorRecordMigration,
    ) -> None:
        """
        注册一条同 major 的显式前向迁移。

        Args:
            schema_id: 迁移归属的 owned record schema ID。
            source_version: 迁移起始版本。
            target_version: 迁移结束版本。
            migration: 输入旧记录并返回新记录的纯转换函数。

        Returns:
            None: 注册成功后正常返回。

        Side Effects:
            向当前 registry 增加一条迁移边。

        Raises:
            SchemaMigrationError: 迁移非前向、跨 major、重复或不可调用时抛出。
        """
        normalized_schema = _normalize_nonempty(schema_id, "migration schema_id")
        try:
            source = _parse_version(source_version, "source_version")
            target = _parse_version(target_version, "target_version")
        except MarketSchemaError as exc:
            raise SchemaMigrationError(str(exc)) from exc
        if source[0] != target[0]:
            raise SchemaMigrationError("不允许自动跨 major schema 迁移")
        if target <= source:
            raise SchemaMigrationError("schema 迁移必须严格前向")
        if not callable(migration):
            raise SchemaMigrationError("migration 必须可调用")
        source_text = _format_version(source)
        target_text = _format_version(target)
        key = (normalized_schema, source_text)
        if key in self._migrations:
            raise SchemaMigrationError(f"重复 schema 迁移起点: {key}")
        self._migrations[key] = (target_text, migration)

    def migrate(
        self, record: OwnedVendorRecord, target_schema_id: str, target_version: str
    ) -> Tuple[OwnedVendorRecord, Tuple[str, ...]]:
        """
        沿唯一显式链将 owned record 迁移到目标版本。

        Args:
            record: 需要迁移的不可变 owned record。
            target_schema_id: 目标 schema ID。
            target_version: 目标 schema 版本。

        Returns:
            Tuple[OwnedVendorRecord, Tuple[str, ...]]: 迁移后记录与路径证据。

        Raises:
            SchemaMigrationError: schema ID/major 不匹配、降级、链缺失或迁移函数违约时抛出。
        """
        schema_id = _normalize_nonempty(target_schema_id, "target_schema_id")
        if record.record_schema_id != schema_id:
            raise SchemaMigrationError(
                f"record schema_id={record.record_schema_id} 不匹配目标 {schema_id}"
            )
        try:
            current = _parse_version(record.record_schema_version, "record_schema_version")
            target = _parse_version(target_version, "target_version")
        except MarketSchemaError as exc:
            raise SchemaMigrationError(str(exc)) from exc
        if current[0] != target[0]:
            raise SchemaMigrationError("不允许自动跨 major schema 迁移")
        if current > target:
            raise SchemaMigrationError("schema 不允许自动降级")
        migrated = record
        migration_path: List[str] = []
        visited: Set[str] = set()
        while current < target:
            current_text = _format_version(current)
            if current_text in visited:
                raise SchemaMigrationError("schema 迁移链存在循环")
            visited.add(current_text)
            edge = self._migrations.get((schema_id, current_text))
            if edge is None:
                raise SchemaMigrationError(
                    f"缺少 schema 迁移: {schema_id} {current_text} -> " f"{_format_version(target)}"
                )
            edge_target_text, migration = edge
            edge_target = _parse_version(edge_target_text, "registered target_version")
            if edge_target > target:
                raise SchemaMigrationError(
                    f"schema 迁移边 {current_text}->{edge_target_text} 超过目标 "
                    f"{_format_version(target)}"
                )
            source_identity = migrated.immutable_source_identity
            try:
                candidate = migration(migrated)
            except Exception as exc:
                raise SchemaMigrationError(
                    f"schema 迁移执行失败: {current_text}->{edge_target_text}"
                ) from exc
            if not isinstance(candidate, OwnedVendorRecord):
                raise SchemaMigrationError("migration 必须返回 OwnedVendorRecord")
            if candidate.immutable_source_identity != source_identity:
                raise SchemaMigrationError("migration 不得修改厂商源身份")
            if candidate.record_schema_version != edge_target_text:
                raise SchemaMigrationError("migration 返回的 record_schema_version 与注册目标不一致")
            migrated = candidate
            migration_path.append(f"{current_text}->{edge_target_text}")
            current = edge_target
        return migrated, tuple(migration_path)


class MarketDataSchemaProjector:
    """执行 owned record 验证、版本迁移、三层映射和单向兼容投影。"""

    def __init__(
        self,
        schema: MarketDataProjectionSchema,
        migrations: Optional[SchemaMigrationRegistry] = None,
    ) -> None:
        """
        创建绑定一个固定 schema 的投影器。

        Args:
            schema: 目标三层映射 schema。
            migrations: 可选的同 major 显式迁移注册表。

        Returns:
            None: 投影器初始化后正常返回。

        Raises:
            MarketSchemaError: schema 或 migrations 类型非法时抛出。
        """
        if not isinstance(schema, MarketDataProjectionSchema):
            raise MarketSchemaError("schema 必须是 MarketDataProjectionSchema")
        if migrations is not None and not isinstance(migrations, SchemaMigrationRegistry):
            raise MarketSchemaError("migrations 必须是 SchemaMigrationRegistry")
        self._schema = schema
        self._migrations = migrations

    def _prepare_record(
        self, record: OwnedVendorRecord
    ) -> Tuple[OwnedVendorRecord, Tuple[str, ...]]:
        """
        将记录迁移到目标版本并校验 schema/manifest/struct 一致性。

        Args:
            record: 待映射的 owned vendor record。

        Returns:
            Tuple[OwnedVendorRecord, Tuple[str, ...]]: 已校验记录与迁移路径。

        Raises:
            SchemaCompatibilityError: 记录不可信或与目标 schema 不匹配时抛出。
            SchemaMigrationError: 版本不同且无法显式迁移时抛出。
        """
        if not isinstance(record, OwnedVendorRecord):
            raise SchemaCompatibilityError("record 必须是 OwnedVendorRecord")
        if not record.field_fidelity:
            raise SchemaCompatibilityError("owned record field_fidelity=false")
        prepared = record
        migration_path: Tuple[str, ...] = ()
        if record.record_schema_version != self._schema.schema_version:
            if self._migrations is None:
                raise SchemaMigrationError(
                    f"未配置从 {record.record_schema_version} 到 " f"{self._schema.schema_version} 的迁移链"
                )
            prepared, migration_path = self._migrations.migrate(
                record, self._schema.schema_id, self._schema.schema_version
            )
        if not prepared.field_fidelity:  # 迁移不得将可信记录降为不可信后继续
            raise SchemaCompatibilityError("migrated record field_fidelity=false")
        if prepared.record_schema_id != self._schema.schema_id:
            raise SchemaCompatibilityError("owned record schema_id 不匹配")
        if prepared.vendor_schema_id != self._schema.vendor_schema_id:
            raise SchemaCompatibilityError("vendor_schema_id 不匹配")
        if prepared.mapping_version != self._schema.mapping_version:
            raise SchemaCompatibilityError("mapping_version 不匹配")
        if prepared.source_layout_identity not in self._schema.source_layouts:
            raise SchemaCompatibilityError("source layout identity 不匹配")
        mapped_fields = {rule.source_field for rule in self._schema.field_rules}
        allowed_fields = mapped_fields.union(self._schema.redacted_fields)
        unknown_fields = set(prepared.fields).difference(allowed_fields)
        if unknown_fields:
            raise SchemaCompatibilityError(f"存在未分类厂商字段: {sorted(unknown_fields)}")
        if set(prepared.redacted_fields) != set(self._schema.redacted_fields):
            raise SchemaCompatibilityError("owned record 与 schema 的 redacted_fields 不一致")
        missing_declarations = mapped_fields.difference(prepared.fields)
        if missing_declarations:
            raise SchemaCompatibilityError(f"owned record 未声明字段存在性: {sorted(missing_declarations)}")
        for redacted_name in self._schema.redacted_fields:
            observation = prepared.fields.get(redacted_name)
            if observation is not None and (
                observation.present
                or observation.missing_reason is not FieldMissingReason.REDACTED
                or observation.raw_enum is not None
                or observation.raw_bytes is not None
            ):
                raise SchemaCompatibilityError(f"脱敏字段 {redacted_name} 不得映射")
        return prepared, migration_path

    @staticmethod
    def _project_observation(rule: SchemaFieldRule, observation: VendorFieldObservation) -> Any:
        """
        对一个 present 观测执行枚举规范化与显式值转换。

        Args:
            rule: 当前字段映射规则。
            observation: 当前 owned 字段观测。

        Returns:
            Any: 已冻结的目标层值。

        Raises:
            SchemaProjectionError: 源类型、enum key 或 converter 结果不合同时抛出。
        """
        value = observation.value
        if rule.enum_mapping:
            raw_key = observation.raw_enum if observation.raw_enum is not None else value
            try:
                value = rule.enum_mapping.get(raw_key, rule.unknown_enum_value)
            except TypeError as exc:
                raise SchemaProjectionError(f"字段 {rule.source_field} enum 原值不可用作映射键") from exc
        if rule.converter is not None:
            try:
                value = rule.converter(value)
            except Exception as exc:
                raise SchemaProjectionError(
                    f"字段 {rule.source_field} 转换 {rule.conversion_id} 失败"
                ) from exc
        if value is None:
            raise SchemaProjectionError(f"present 字段 {rule.source_field} 不得投影为 None")
        try:
            return _freeze_value(value)
        except VendorRecordValidationError as exc:
            raise SchemaProjectionError(f"字段 {rule.source_field} 转换结果不可无损传输") from exc

    def project(
        self,
        record: OwnedVendorRecord,
        field_profile: FieldProfile = FieldProfile.CANONICAL,
    ) -> SchemaProjection:
        """
        将 owned vendor record 单向投影为指定 canonical/vendor/raw profile。

        Args:
            record: 已脱离厂商指针生命周期的 owned record。
            field_profile: canonical、canonical_with_vendor 或 canonical_with_raw。

        Returns:
            SchemaProjection: 携带 presence/completeness/missing reason 的不可变投影。

        Raises:
            SchemaCompatibilityError: source schema/manifest/struct 不可安全解释时抛出。
            SchemaMigrationError: 版本迁移链不完整时抛出。
            SchemaProjectionError: 字段类型或转换失败时抛出。
        """
        try:
            profile = FieldProfile(field_profile)
        except ValueError as exc:
            raise SchemaProjectionError("未知 field profile") from exc
        prepared, migration_path = self._prepare_record(record)
        include_vendor = profile in {
            FieldProfile.CANONICAL_WITH_VENDOR,
            FieldProfile.CANONICAL_WITH_RAW,
        }
        include_raw = profile is FieldProfile.CANONICAL_WITH_RAW
        canonical_payload: Dict[str, Any] = {}
        vendor_payload: Dict[str, Any] = {}
        raw_values: Dict[str, Any] = {}
        raw_enums: Dict[str, Any] = {}
        raw_bytes: Dict[str, Any] = {}
        missing_fields: Dict[str, MissingField] = {}
        field_sources: Dict[str, str] = {}

        for rule in self._schema.field_rules:
            observation = prepared.fields[rule.source_field]
            if rule.expected_source_type is not None and (
                observation.source_type != rule.expected_source_type
            ):
                raise SchemaProjectionError(
                    f"字段 {rule.source_field} source_type={observation.source_type} "
                    f"不匹配 {rule.expected_source_type}"
                )
            if rule.layer is FieldLayer.CANONICAL:
                target_container = canonical_payload
                field_path = f"payload.{rule.target_field}"
                included = True
            elif rule.layer is FieldLayer.VENDOR:
                target_container = vendor_payload
                field_path = (
                    f"provider_extension.{self._schema.vendor_namespace}.{rule.target_field}"
                )
                included = include_vendor
            else:
                target_container = raw_values
                field_path = (
                    f"raw_profile.{self._schema.vendor_namespace}.values.{rule.target_field}"
                )
                included = include_raw
            if not included:
                continue
            field_sources[field_path] = rule.source_field
            if observation.present:
                value = self._project_observation(rule, observation)
                _assign_path(target_container, rule.target_field, value)
            else:
                _assign_path(target_container, rule.target_field, None)
                if observation.missing_reason is None:  # pragma: no cover - model already guards
                    raise SchemaProjectionError(f"字段 {rule.source_field} 缺少 missing_reason")
                missing_fields[field_path] = MissingField(
                    source_field=rule.source_field,
                    layer=rule.layer,
                    reason=observation.missing_reason,
                )

        if include_raw:
            for field_name in sorted(prepared.fields):
                if field_name in prepared.redacted_fields:
                    continue
                observation = prepared.fields[field_name]
                if observation.raw_enum is not None:
                    raw_enums[field_name] = observation.raw_enum
                if observation.raw_bytes is not None:
                    raw_bytes[field_name] = observation.raw_bytes

        provider_extension: Mapping[str, Any]
        if include_vendor:
            provider_extension = {self._schema.vendor_namespace: vendor_payload}
        else:
            provider_extension = {}
        raw_profile: Mapping[str, Any]
        if include_raw:
            raw_profile = {
                self._schema.vendor_namespace: {
                    "record": {
                        "provider": prepared.provider,
                        "module": prepared.module,
                        "sdk_version": prepared.sdk_version,
                        "vendor_schema_id": prepared.vendor_schema_id,
                        "record_schema_id": prepared.record_schema_id,
                        "record_schema_version": prepared.record_schema_version,
                        "source_callback": prepared.source_callback,
                        "source_struct": prepared.source_struct,
                        "source_struct_size": prepared.source_struct_size,
                        "manifest_hash": prepared.manifest_hash,
                        "mapping_version": prepared.mapping_version,
                        "redacted_fields": prepared.redacted_fields,
                    },
                    "values": raw_values,
                    "enums": raw_enums,
                    "bytes": raw_bytes,
                }
            }
        else:
            raw_profile = {}
        leaf_values = _projection_leaf_values(canonical_payload, provider_extension, raw_profile)
        field_presence = tuple(path for path, value in leaf_values.items() if value is not None)
        return SchemaProjection(
            schema_id=self._schema.schema_id,
            schema_version=self._schema.schema_version,
            field_set_version=self._schema.field_set_version,
            field_profile=profile,
            provider=prepared.provider,
            vendor_schema_id=prepared.vendor_schema_id,
            source_layout=prepared.source_layout_identity,
            mapping_version=prepared.mapping_version,
            source_record_schema_version=record.record_schema_version,
            redacted_fields=prepared.redacted_fields,
            field_sources=field_sources,
            payload=canonical_payload,
            provider_extension=provider_extension,
            raw_profile=raw_profile,
            field_presence=field_presence,
            completeness=not missing_fields,
            missing_fields=missing_fields,
            migration_path=migration_path,
            _factory_token=_SCHEMA_PROJECTION_FACTORY_TOKEN,
        )

    def project_compatibility(
        self,
        projection: SchemaProjection,
        compatibility_schema: CompatibilityProjectionSchema,
    ) -> CompatibilityProjection:
        """
        只从 canonical payload 生成显式有损兼容视图。

        Args:
            projection: 三层投影结果；方法只读取其 ``payload`` 与 canonical
                presence/missing 元数据。
            compatibility_schema: 兼容字段和允许裁剪清单。

        Returns:
            CompatibilityProjection: 不引用 vendor/raw/owned record 的不可变兼容 payload。

        Raises:
            SchemaCompatibilityError: 源 schema ID/major 不匹配时抛出。
            SchemaProjectionError: 兼容映射引用未声明字段或出现未列出损失时抛出。
        """
        if not isinstance(projection, SchemaProjection):
            raise SchemaProjectionError("projection 必须是 SchemaProjection")
        if not isinstance(compatibility_schema, CompatibilityProjectionSchema):
            raise SchemaProjectionError("compatibility_schema 必须是 CompatibilityProjectionSchema")
        if projection.schema_id != compatibility_schema.source_schema_id:
            raise SchemaCompatibilityError("compatibility source_schema_id 不匹配")
        source_version = _parse_version(projection.schema_version, "projection schema_version")
        if source_version[0] != compatibility_schema.source_schema_major:
            raise SchemaCompatibilityError("compatibility source schema major 不匹配")

        canonical_leaf_paths = set(_leaf_paths(projection.payload))
        mapped_source_paths = set(compatibility_schema.field_mappings.values())
        omitted_fields = canonical_leaf_paths.difference(mapped_source_paths)
        undeclared_omissions = omitted_fields.difference(compatibility_schema.allowed_omissions)
        if undeclared_omissions:
            raise SchemaProjectionError(f"兼容投影存在未声明裁剪: {sorted(undeclared_omissions)}")
        payload: Dict[str, Any] = {}
        missing_fields: Dict[str, MissingField] = {}
        for target_field, source_field in compatibility_schema.field_mappings.items():
            if source_field not in canonical_leaf_paths:
                raise SchemaProjectionError(f"兼容映射必须引用 canonical 叶子字段: {source_field}")
            exists, value = _read_path(projection.payload, source_field)
            if not exists:
                raise SchemaProjectionError(f"兼容映射引用不存在 canonical 字段: {source_field}")
            _assign_path(payload, target_field, value)
            canonical_path = f"payload.{source_field}"
            if value is not None:
                continue
            missing = projection.missing_fields.get(canonical_path)
            if missing is None:
                raise SchemaProjectionError(
                    f"canonical 字段无 presence 也无 missing reason: {source_field}"
                )
            missing_fields[target_field] = missing
        compatibility_leaf_values = _leaf_values(payload)
        field_presence = tuple(
            path for path, value in compatibility_leaf_values.items() if value is not None
        )
        return CompatibilityProjection(
            schema_id=compatibility_schema.schema_id,
            schema_version=compatibility_schema.schema_version,
            source_schema_id=projection.schema_id,
            source_schema_version=projection.schema_version,
            payload=payload,
            field_presence=field_presence,
            completeness=not missing_fields,
            missing_fields=missing_fields,
            omitted_canonical_fields=tuple(sorted(omitted_fields)),
        )
