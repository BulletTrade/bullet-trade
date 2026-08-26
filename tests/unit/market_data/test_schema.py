"""
作者: BruceLee

文件职责: 验证 owned vendor record、canonical/vendor/raw profile 与有损兼容投影的纯离线合同。
主要输入: 合成厂商字段、未知枚举、无法解码 bytes、int64 边界与显式版本迁移。
主要输出: 三层字段值、presence/completeness/missing reason、损失清单和 fail-closed 异常断言。
上游关系: 覆盖 bullet_trade.market_data.schema 的公共类与投影方法。
下游关系: 为未来 Huaxin field manifest、native wrapper、远程协议和兼容 tick 接入提供回归门禁。
关键配置约定: 全部 fixture 均为脱敏合成数据，不联网、不加载 SDK、不连接服务器且不执行任何交易。
"""

from dataclasses import replace
from typing import Any, Dict, Mapping, Optional

import pytest

import bullet_trade.market_data as market_data
import bullet_trade.market_data.schema as schema_module
from bullet_trade.market_data.codec import dumps_market_event, loads_market_event
from bullet_trade.market_data.models import (
    FieldProfile,
    MarketDataLevel,
    MarketEvent,
    MarketEventType,
)
from bullet_trade.market_data.schema import (
    CompatibilityProjectionSchema,
    FieldLayer,
    FieldMissingReason,
    MarketDataProjectionSchema,
    MarketDataSchemaProjector,
    MarketSchemaError,
    MissingField,
    OwnedVendorRecord,
    SchemaCompatibilityError,
    SchemaFieldRule,
    SchemaMigrationError,
    SchemaMigrationRegistry,
    SchemaProjectionError,
    SourceLayoutIdentity,
    VendorFieldObservation,
    VendorRecordValidationError,
)

pytestmark = pytest.mark.unit


def test_schema_contract_is_exported_from_market_data_package() -> None:
    """
    验证三层 schema 的公共合同可从稳定 package root 导入。

    Returns:
        None: 关键模型、投影器与错误类型均被 package ``__all__`` 暴露后返回。
    """
    expected = {
        "CompatibilityProjection",
        "CompatibilityProjectionSchema",
        "MarketDataProjectionSchema",
        "MarketDataSchemaProjector",
        "OwnedVendorRecord",
        "SchemaProjection",
        "VendorFieldObservation",
        "VendorRecordValidationError",
        "VendorSourceType",
    }
    assert expected.issubset(set(market_data.__all__))
    for name in expected:
        assert getattr(market_data, name) is not None


def _base_fields() -> Dict[str, VendorFieldObservation]:
    """
    构造同时覆盖 canonical、vendor、raw、真实零值与缺失语义的字段。

    Returns:
        Dict[str, VendorFieldObservation]: 合成 owned record 字段观测。
    """
    return {
        "LastPrice": VendorFieldObservation(source_type="double", value=10.25),
        "TotalVolume": VendorFieldObservation(source_type="int64", value=9223372036854775807),
        "TickType": VendorFieldObservation(source_type="enum", value=b"Z"),
        "BidQueue": VendorFieldObservation(source_type="array", value=[1200, 800, 500]),
        "SecurityName": VendorFieldObservation(
            source_type="bytes",
            present=False,
            missing_reason=FieldMissingReason.DECODE_ERROR,
            raw_bytes=b"\xff\xfe",
        ),
        "Info1": VendorFieldObservation(source_type="int32", value=7),
        "ZeroValue": VendorFieldObservation(source_type="int64", value=0),
        "UpperLimitPrice": VendorFieldObservation(
            source_type="double",
            present=False,
            missing_reason=FieldMissingReason.NOT_APPLICABLE_EXCHANGE,
        ),
        "Secret": VendorFieldObservation(
            source_type="bytes",
            present=False,
            missing_reason=FieldMissingReason.REDACTED,
        ),
    }


def _base_record(
    *,
    fields: Optional[Mapping[str, VendorFieldObservation]] = None,
    record_schema_version: str = "1.0",
    mapping_version: str = "map-v1",
) -> OwnedVendorRecord:
    """
    构造固定来源身份的合成 owned vendor record。

    Args:
        fields: 可选替换字段 mapping，默认使用完整基线。
        record_schema_version: owned record schema 版本。
        mapping_version: 字段映射版本。

    Returns:
        OwnedVendorRecord: 与厂商指针生命周期脱离的测试记录。
    """
    return OwnedVendorRecord(
        provider="synthetic-vendor",
        module="l2",
        sdk_version="fixture-1",
        vendor_schema_id="vendor.l2.fixture.v1",
        record_schema_id="market.fixture",
        record_schema_version=record_schema_version,
        source_callback="OnSyntheticDepth",
        source_struct="SyntheticDepth",
        source_struct_size=512,
        manifest_hash="fixture-manifest-sha256",
        mapping_version=mapping_version,
        fields=_base_fields() if fields is None else fields,
        redacted_fields=("Secret",),
    )


def _attempt_mapping_write(mapping: Mapping[str, Any], key: str, value: Any) -> None:
    """
    尝试修改声明为只读的 mapping，仅用于断言运行时不可变性。

    Args:
        mapping: 待尝试修改的只读 mapping。
        key: 待写入的键。
        value: 待写入的值。

    Returns:
        None: 若 mapping 错误允许写入则返回；正常合同下应抛 TypeError。

    Raises:
        TypeError: 当 mapping 按合同不可变时抛出。

    Side Effects:
        只在被测 mapping 错误可变时才可能写入测试值。
    """
    mapping[key] = value  # type: ignore[index]


def _base_schema(
    *,
    schema_version: str = "1.0",
    mapping_version: str = "map-v1",
    field_rules: Any = None,
) -> MarketDataProjectionSchema:
    """
    构造将基线字段 100% 分类为 canonical/vendor/raw/redacted 的 schema。

    Args:
        schema_version: 目标记录 schema 版本。
        mapping_version: 目标映射版本。
        field_rules: 可选替换规则序列。

    Returns:
        MarketDataProjectionSchema: 不依赖任何 SDK 的映射 schema。
    """
    rules = (
        SchemaFieldRule(
            "LastPrice", "last_price", FieldLayer.CANONICAL, expected_source_type="double"
        ),
        SchemaFieldRule(
            "TotalVolume", "volume", FieldLayer.CANONICAL, expected_source_type="int64"
        ),
        SchemaFieldRule(
            "TickType",
            "tick_type",
            FieldLayer.CANONICAL,
            expected_source_type="enum",
            enum_mapping={b"A": "add", b"D": "delete", b"T": "trade"},
        ),
        SchemaFieldRule(
            "BidQueue", "depth.bid_queue", FieldLayer.VENDOR, expected_source_type="array"
        ),
        SchemaFieldRule(
            "SecurityName",
            "security_name",
            FieldLayer.CANONICAL,
            expected_source_type="bytes",
        ),
        SchemaFieldRule("Info1", "Info1", FieldLayer.RAW, expected_source_type="int32"),
        SchemaFieldRule(
            "ZeroValue", "zero_value", FieldLayer.CANONICAL, expected_source_type="int64"
        ),
        SchemaFieldRule(
            "UpperLimitPrice",
            "high_limit",
            FieldLayer.CANONICAL,
            expected_source_type="double",
        ),
    )
    return MarketDataProjectionSchema(
        schema_id="market.fixture",
        schema_version=schema_version,
        vendor_schema_id="vendor.l2.fixture.v1",
        field_set_version="fixture-field-set-v1",
        mapping_version=mapping_version,
        vendor_namespace="synthetic_vendor",
        source_layouts=(
            SourceLayoutIdentity(
                provider="synthetic-vendor",
                module="l2",
                sdk_version="fixture-1",
                source_callback="OnSyntheticDepth",
                source_struct="SyntheticDepth",
                source_struct_size=512,
                manifest_hash="fixture-manifest-sha256",
            ),
        ),
        field_rules=rules if field_rules is None else tuple(field_rules),
        redacted_fields=("Secret",),
    )


def test_owned_record_deep_copies_source_and_preserves_exact_int64() -> None:
    """
    验证 owned record 与外部可变容器脱离，且大整数不经浮点路径。

    Returns:
        None: 拥有权、不可变性和 int64 精度断言通过后返回。

    Side Effects:
        仅修改本测试持有的原始 list/bytearray，不修改产品状态。
    """
    queue = [1200, 800]
    raw_name = bytearray(b"\xff\xfe")
    fields = _base_fields()
    fields["BidQueue"] = VendorFieldObservation(source_type="array", value=queue)
    fields["SecurityName"] = VendorFieldObservation(
        source_type="bytes",
        present=False,
        missing_reason=FieldMissingReason.DECODE_ERROR,
        raw_bytes=raw_name,
    )
    record = _base_record(fields=fields)

    queue.append(1)
    raw_name[0] = 0

    assert record.fields["BidQueue"].value == (1200, 800)
    assert record.fields["SecurityName"].raw_bytes == b"\xff\xfe"
    assert record.fields["TotalVolume"].value == 9223372036854775807
    with pytest.raises(TypeError):
        record.fields["BidQueue"].value[0] = 0
    with pytest.raises(TypeError):
        _attempt_mapping_write(
            record.fields, "new", VendorFieldObservation(source_type="int32", value=1)
        )


def test_three_field_profiles_preserve_presence_missing_enum_bytes_and_zero() -> None:
    """
    验证三种 profile 逐层增加信息，且真实 0 与缺失 null 不混淆。

    Returns:
        None: canonical/vendor/raw 载荷和完整性断言通过后返回。
    """
    projector = MarketDataSchemaProjector(_base_schema())
    record = _base_record()

    canonical = projector.project(record, FieldProfile.CANONICAL)
    with_vendor = projector.project(record, FieldProfile.CANONICAL_WITH_VENDOR)
    with_raw = projector.project(record, FieldProfile.CANONICAL_WITH_RAW)

    assert canonical.payload["last_price"] == 10.25
    assert canonical.payload["volume"] == 9223372036854775807
    assert canonical.payload["tick_type"] == "unknown"
    assert canonical.payload["zero_value"] == 0
    assert canonical.payload["security_name"] is None
    assert canonical.payload["high_limit"] is None
    assert canonical.provider_extension == {}
    assert canonical.raw_profile == {}
    assert canonical.field_presence == (
        "payload.last_price",
        "payload.tick_type",
        "payload.volume",
        "payload.zero_value",
    )
    assert canonical.missing_reasons == {
        "payload.security_name": "decode_error",
        "payload.high_limit": "not_applicable_exchange",
    }
    assert canonical.completeness is False

    assert with_vendor.provider_extension["synthetic_vendor"]["depth"]["bid_queue"] == (
        1200,
        800,
        500,
    )
    assert with_vendor.raw_profile == {}
    assert "provider_extension.synthetic_vendor.depth.bid_queue" in with_vendor.field_presence

    raw = with_raw.raw_profile["synthetic_vendor"]
    assert with_raw.provider_extension == with_vendor.provider_extension
    assert raw["values"]["Info1"] == 7
    assert raw["enums"]["TickType"] == b"Z"
    assert raw["bytes"]["SecurityName"] == b"\xff\xfe"
    assert raw["record"]["provider"] == "synthetic-vendor"
    assert raw["record"]["source_struct_size"] == 512
    assert raw["record"]["redacted_fields"] == ("Secret",)
    assert "Secret" not in raw["values"]

    with pytest.raises(TypeError):
        _attempt_mapping_write(canonical.payload, "last_price", 11.0)
    with pytest.raises(TypeError):
        _attempt_mapping_write(raw["enums"], "TickType", b"A")


def test_vendor_and_raw_layers_preserve_their_own_missing_reasons() -> None:
    """
    验证 vendor/raw profile 中的 null 各自携带分层路径和稳定缺失原因。

    Returns:
        None: vendor 与 raw 字段均不被静默丢弃且 completeness=false 后返回。
    """
    fields = _base_fields()
    fields["BidQueue"] = VendorFieldObservation(
        source_type="array",
        present=False,
        missing_reason=FieldMissingReason.NOT_APPLICABLE,
    )
    fields["Info1"] = VendorFieldObservation(
        source_type="int32",
        present=False,
        missing_reason=FieldMissingReason.SOURCE_ABSENT,
    )

    projection = MarketDataSchemaProjector(_base_schema()).project(
        _base_record(fields=fields), FieldProfile.CANONICAL_WITH_RAW
    )

    assert projection.provider_extension["synthetic_vendor"]["depth"]["bid_queue"] is None
    assert projection.raw_profile["synthetic_vendor"]["values"]["Info1"] is None
    assert (
        projection.missing_reasons["provider_extension.synthetic_vendor.depth.bid_queue"]
        == "not_applicable"
    )
    assert (
        projection.missing_reasons["raw_profile.synthetic_vendor.values.Info1"] == "source_absent"
    )
    assert projection.completeness is False


def test_compatibility_projection_is_canonical_only_lossy_and_immutable() -> None:
    """
    验证兼容投影只读 canonical，显式列出裁剪并不能反向污染源投影。

    Returns:
        None: 兼容 payload、missing reason、loss list 与不可变性断言通过后返回。
    """
    projector = MarketDataSchemaProjector(_base_schema())
    typed = projector.project(_base_record(), FieldProfile.CANONICAL_WITH_RAW)
    compatibility_schema = CompatibilityProjectionSchema(
        schema_id="legacy.tick",
        schema_version="1",
        source_schema_id="market.fixture",
        source_schema_major=1,
        field_mappings={
            "last_price": "last_price",
            "volume": "volume",
            "name": "security_name",
        },
        allowed_omissions=("tick_type", "zero_value", "high_limit"),
    )

    compatibility = projector.project_compatibility(typed, compatibility_schema)

    assert compatibility.payload == {
        "last_price": 10.25,
        "volume": 9223372036854775807,
        "name": None,
    }
    assert compatibility.field_presence == ("last_price", "volume")
    assert compatibility.missing_fields["name"].reason is FieldMissingReason.DECODE_ERROR
    assert compatibility.omitted_canonical_fields == (
        "high_limit",
        "tick_type",
        "zero_value",
    )
    assert "depth" not in compatibility.payload
    assert "synthetic_vendor" not in compatibility.payload
    assert typed.raw_profile["synthetic_vendor"]["enums"]["TickType"] == b"Z"
    with pytest.raises(TypeError):
        _attempt_mapping_write(compatibility.payload, "last_price", 12.0)
    assert typed.payload["last_price"] == 10.25


def test_compatibility_projection_rejects_undeclared_loss_and_major_mismatch() -> None:
    """
    验证未列入损失清单的 canonical 字段和不兼容 major 均 fail closed。

    Returns:
        None: 两类不安全投影均被受控拒绝后返回。
    """
    projector = MarketDataSchemaProjector(_base_schema())
    typed = projector.project(_base_record())
    undeclared_loss = CompatibilityProjectionSchema(
        schema_id="legacy.tick",
        schema_version="1",
        source_schema_id="market.fixture",
        source_schema_major=1,
        field_mappings={"last_price": "last_price"},
        allowed_omissions=("volume",),
    )
    with pytest.raises(SchemaProjectionError, match="未声明裁剪"):
        projector.project_compatibility(typed, undeclared_loss)

    wrong_major = CompatibilityProjectionSchema(
        schema_id="legacy.tick",
        schema_version="1",
        source_schema_id="market.fixture",
        source_schema_major=2,
        field_mappings={"last_price": "last_price"},
        allowed_omissions=(
            "volume",
            "tick_type",
            "security_name",
            "zero_value",
            "high_limit",
        ),
    )
    with pytest.raises(SchemaCompatibilityError, match="major"):
        projector.project_compatibility(typed, wrong_major)


def test_projection_dataclass_rejects_profile_and_field_evidence_forgery() -> None:
    """
    验证 public dataclass 不能通过 replace 伪造层级、来源或迁移证据。

    Returns:
        None: 所有绕过 projector 的 public dataclass 重建均被拒绝后返回。
    """
    canonical = MarketDataSchemaProjector(_base_schema()).project(_base_record())

    with pytest.raises(SchemaProjectionError, match="必须由 MarketDataSchemaProjector 创建"):
        replace(canonical, provider_extension={"forged": {"value": 1}})
    with pytest.raises(SchemaProjectionError, match="必须由 MarketDataSchemaProjector 创建"):
        replace(canonical, field_presence=canonical.field_presence + ("payload.ghost",))
    with pytest.raises(SchemaProjectionError, match="必须由 MarketDataSchemaProjector 创建"):
        replace(canonical, migration_path=("0.0.0->9.9.9",))

    with_raw = MarketDataSchemaProjector(_base_schema()).project(
        _base_record(), FieldProfile.CANONICAL_WITH_RAW
    )
    raw_namespace = dict(with_raw.raw_profile["synthetic_vendor"])
    raw_record = dict(raw_namespace["record"])
    raw_record["pointer"] = "0x1234"
    raw_namespace["record"] = raw_record
    with pytest.raises(SchemaProjectionError, match="必须由 MarketDataSchemaProjector 创建"):
        replace(with_raw, raw_profile={"synthetic_vendor": raw_namespace})

    raw_record.pop("pointer")
    raw_record["provider"] = "forged-provider"
    with pytest.raises(SchemaProjectionError, match="必须由 MarketDataSchemaProjector 创建"):
        replace(with_raw, raw_profile={"synthetic_vendor": raw_namespace})

    forged_missing = dict(canonical.missing_fields)
    forged_missing["payload.security_name"] = replace(
        forged_missing["payload.security_name"], source_field="ForgedField"
    )
    with pytest.raises(SchemaProjectionError, match="必须由 MarketDataSchemaProjector 创建"):
        replace(canonical, missing_fields=forged_missing)


def test_projection_private_factory_token_cannot_bypass_provenance_validation() -> None:
    """
    验证即使越过非公共构造门禁，raw、missing 与 migration 证据仍需逐项自洽。

    Returns:
        None: 三类协调不足的证据伪造均被结构校验拒绝后返回。
    """
    projector = MarketDataSchemaProjector(_base_schema())
    with_raw = projector.project(_base_record(), FieldProfile.CANONICAL_WITH_RAW)
    token = schema_module._SCHEMA_PROJECTION_FACTORY_TOKEN

    raw_namespace = dict(with_raw.raw_profile["synthetic_vendor"])
    raw_record = dict(raw_namespace["record"])
    raw_record["source_callback"] = "ForgedCallback"
    raw_namespace["record"] = raw_record
    with pytest.raises(SchemaProjectionError, match="source_callback 与投影不一致"):
        replace(
            with_raw,
            raw_profile={"synthetic_vendor": raw_namespace},
            _factory_token=token,
        )

    canonical = projector.project(_base_record())
    forged_missing = dict(canonical.missing_fields)
    forged_missing["payload.security_name"] = replace(
        forged_missing["payload.security_name"], source_field="ForgedSecret"
    )
    with pytest.raises(SchemaProjectionError, match="source_field 与投影规则不一致"):
        replace(canonical, missing_fields=forged_missing, _factory_token=token)

    with pytest.raises(SchemaProjectionError, match="migration source version"):
        replace(
            canonical,
            migration_path=("untrusted->fabricated",),
            _factory_token=token,
        )


def test_compatibility_projection_dataclass_rejects_forged_field_evidence() -> None:
    """
    验证兼容投影的 public dataclass 也要求每个叶子和值证据精确对应。

    Returns:
        None: 伪造 presence 和把非 null 值声明 missing 均被拒绝后返回。
    """
    projector = MarketDataSchemaProjector(_base_schema())
    typed = projector.project(_base_record())
    schema = CompatibilityProjectionSchema(
        schema_id="legacy.tick",
        schema_version="1",
        source_schema_id="market.fixture",
        source_schema_major=1,
        field_mappings={"last_price": "last_price", "name": "security_name"},
        allowed_omissions=("volume", "tick_type", "zero_value", "high_limit"),
    )
    compatibility = projector.project_compatibility(typed, schema)

    with pytest.raises(SchemaProjectionError, match="证据不一致"):
        replace(
            compatibility,
            field_presence=compatibility.field_presence + ("ghost",),
        )
    with pytest.raises(SchemaProjectionError, match="missing 字段必须为 None"):
        replace(
            compatibility,
            field_presence=(),
            missing_fields={
                "last_price": MissingField(
                    source_field="LastPrice",
                    layer=FieldLayer.CANONICAL,
                    reason=FieldMissingReason.SOURCE_ABSENT,
                ),
                **compatibility.missing_fields,
            },
        )


def test_projection_preserves_int64_enum_and_bytes_through_market_event_codec() -> None:
    """
    验证三层投影进入现有 MarketEvent codec 后 int64 和 raw bytes 仍精确。

    Returns:
        None: 大整数使用带标签 wire 类型且 enum/bytes 逐值一致后返回。
    """
    projector = MarketDataSchemaProjector(_base_schema())
    projection = projector.project(_base_record(), FieldProfile.CANONICAL_WITH_RAW)
    event = MarketEvent(
        provider=projection.provider,
        capability_key="realtime.snapshot.l2",
        event_type=MarketEventType.SNAPSHOT_L2,
        level=MarketDataLevel.L2,
        exchange="XSHG",
        session_epoch="fixture-epoch",
        payload=projection.payload,
        schema_version="1",
        field_set_version=projection.field_set_version,
        field_profile=projection.field_profile,
        provider_extension=projection.provider_extension,
        raw_profile=projection.raw_profile,
        field_presence=projection.field_presence,
        completeness=projection.completeness,
        missing_fields=tuple(projection.missing_fields),
    )

    encoded = dumps_market_event(event)
    restored = loads_market_event(encoded)

    assert '"integer"' in encoded
    assert '"bytes"' in encoded
    assert restored.payload["volume"] == 9223372036854775807
    assert restored.raw_profile["synthetic_vendor"]["enums"]["TickType"] == b"Z"
    assert restored.raw_profile["synthetic_vendor"]["bytes"]["SecurityName"] == b"\xff\xfe"


def test_vendor_field_validation_distinguishes_zero_missing_and_int64_bounds() -> None:
    """
    验证真实零值可用，缺失必须给原因，并且 int64/uint64 拒绝浮点或越界。

    Returns:
        None: 合法边界和所有非法输入断言通过后返回。
    """
    zero = VendorFieldObservation(source_type="int64", value=0)
    missing = VendorFieldObservation(
        source_type="int64",
        present=False,
        missing_reason=FieldMissingReason.SOURCE_ABSENT,
    )
    unsigned_max = VendorFieldObservation(source_type="uint64", value=18446744073709551615)

    assert zero.present is True and zero.value == 0
    assert missing.present is False and missing.value is None
    assert unsigned_max.value == 18446744073709551615
    with pytest.raises(VendorRecordValidationError, match="missing_reason"):
        VendorFieldObservation(source_type="int64", present=False)
    with pytest.raises(VendorRecordValidationError, match="missing_reason"):
        VendorFieldObservation(
            source_type="int64",
            value=1,
            missing_reason=FieldMissingReason.SOURCE_ABSENT,
        )
    with pytest.raises(VendorRecordValidationError, match="int64"):
        VendorFieldObservation(source_type="int64", value=float(2**53 + 1))
    with pytest.raises(VendorRecordValidationError, match="int64"):
        VendorFieldObservation(source_type="int64", value=2**63)
    with pytest.raises(VendorRecordValidationError, match="uint64"):
        VendorFieldObservation(source_type="uint64", value=2**64)


@pytest.mark.parametrize(
    "source_type,value,match",
    (
        ("bool", 1, "bool"),
        ("bool", "false", "bool"),
        ("float", True, "float"),
        ("float", "1.2", "float"),
        ("double", True, "double"),
        ("double", b"1.2", "double"),
        ("int32", 2**31, "int32"),
        ("uint32", -1, "uint32"),
        ("enum", True, "enum"),
        ("array", "not-an-array", "array"),
    ),
)
def test_vendor_field_source_types_reject_cross_type_values(
    source_type: str, value: Any, match: str
) -> None:
    """
    验证已知 source_type 不接受 Python 隐式真值或字符串数值转换。

    Args:
        source_type: 应被严格校验的厂商源类型。
        value: 与该源类型不匹配的合成值。
        match: 期望错误信息包含的类型名。

    Returns:
        None: 所有跨类型输入均在 owned record 入口被拒绝后返回。
    """
    with pytest.raises(VendorRecordValidationError, match=match):
        VendorFieldObservation(source_type=source_type, value=value)


def test_vendor_source_type_contract_rejects_unclassified_custom_names() -> None:
    """
    验证 observation 与 schema rule 共享同一封闭 source type 词表。

    Returns:
        None: 未定义的 float64 名称不能绕过已知数值类型语义后返回。
    """
    with pytest.raises(VendorRecordValidationError, match="未知 source_type"):
        VendorFieldObservation(source_type="float64", value=1)
    with pytest.raises(MarketSchemaError, match="未知 expected_source_type"):
        SchemaFieldRule(
            "LastPrice",
            "last_price",
            FieldLayer.CANONICAL,
            expected_source_type="float64",
        )


@pytest.mark.parametrize("raw_field", ("raw_enum", "raw_bytes"))
def test_redacted_field_rejects_all_raw_evidence(raw_field: str) -> None:
    """
    验证 REDACTED 字段不能把枚举或原 bytes 带入 raw profile。

    Args:
        raw_field: 本次伪造的 raw evidence 字段名。

    Returns:
        None: 脱敏字段在构造 observation 时即 fail closed 后返回。
    """
    evidence: Dict[str, Any] = {raw_field: b"synthetic-secret"}
    with pytest.raises(VendorRecordValidationError, match="REDACTED"):
        VendorFieldObservation(
            source_type="bytes",
            present=False,
            missing_reason=FieldMissingReason.REDACTED,
            **evidence,
        )


def test_schema_migration_cannot_restore_a_redacted_field() -> None:
    """
    验证显式版本迁移也不能把已脱敏字段恢复成 canonical/raw 值。

    Returns:
        None: 迁移构造携带原值的 redacted observation 时受控失败后返回。
    """
    schema = replace(_base_schema(), schema_version="1.1")
    registry = SchemaMigrationRegistry()

    def restore_secret(record: OwnedVendorRecord) -> OwnedVendorRecord:
        """
        构造故意恢复脱敏字段的非法迁移。

        Args:
            record: 迁移前的 1.0 owned record。

        Returns:
            OwnedVendorRecord: 合同要求下永远无法成功返回。
        """
        fields = dict(record.fields)
        fields["Secret"] = VendorFieldObservation(source_type="bytes", value=b"secret")
        return record.evolve(record_schema_version="1.1", fields=fields)

    registry.register("market.fixture", "1.0", "1.1", restore_secret)
    with pytest.raises(SchemaMigrationError, match="迁移执行失败"):
        MarketDataSchemaProjector(schema, registry).project(_base_record())


def test_schema_identity_fields_require_real_strings() -> None:
    """
    验证来源、版本和 manifest 身份不接受 None、整数或任意对象字符串化。

    Returns:
        None: 非字符串身份在模型边界被稳定拒绝后返回。
    """
    with pytest.raises(VendorRecordValidationError, match="必须是字符串"):
        replace(_base_record(), provider=123)
    with pytest.raises(MarketSchemaError, match="必须是字符串"):
        replace(_base_schema().source_layouts[0], manifest_hash=None)
    with pytest.raises(VendorRecordValidationError, match="必须是字符串"):
        replace(_base_record(), record_schema_version=1)
    with pytest.raises(VendorRecordValidationError, match="必须是字符串"):
        VendorFieldObservation(source_type=123, value=1)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "record_update,match",
    (
        ({"vendor_schema_id": "vendor.unknown.v2"}, "vendor_schema_id"),
        ({"provider": "wrong-provider"}, "source layout identity"),
        ({"module": "wrong-module"}, "source layout identity"),
        ({"sdk_version": "wrong-sdk"}, "source layout identity"),
        ({"source_callback": "OnWrongCallback"}, "source layout identity"),
        ({"source_struct_size": 513}, "source layout identity"),
        ({"manifest_hash": "changed-manifest"}, "source layout identity"),
        ({"mapping_version": "unknown-map"}, "mapping_version"),
        ({"field_fidelity": False}, "field_fidelity"),
        ({"redacted_fields": ()}, "redacted_fields"),
    ),
)
def test_source_schema_manifest_struct_and_fidelity_mismatch_fail_closed(
    record_update: Mapping[str, Any], match: str
) -> None:
    """
    验证来源布局、厂商 schema、mapping 和 fidelity 任一不符即拒绝。

    Args:
        record_update: 应使记录不兼容的 dataclass 字段修改。
        match: 期望受控错误包含的关键字。

    Returns:
        None: 投影在产生 payload 前被拒绝后返回。
    """
    projector = MarketDataSchemaProjector(_base_schema())
    record = replace(_base_record(), **record_update)

    with pytest.raises(SchemaCompatibilityError, match=match):
        projector.project(record)


def test_source_layout_identity_rejects_cross_product_of_two_valid_layouts() -> None:
    """
    验证 callback 与另一合法 struct/manifest 拼接后不能通过独立集合验收。

    Returns:
        None: 两套合法布局的混合身份在产生 payload 前被精确拒绝后返回。
    """
    layout_a = _base_schema().source_layouts[0]
    layout_b = SourceLayoutIdentity(
        provider="synthetic-vendor",
        module="trade",
        sdk_version="fixture-2",
        source_callback="OnSyntheticTrade",
        source_struct="SyntheticTrade",
        source_struct_size=128,
        manifest_hash="fixture-trade-manifest-sha256",
    )
    schema = replace(_base_schema(), source_layouts=(layout_a, layout_b))
    mixed_record = replace(
        _base_record(),
        source_struct=layout_b.source_struct,
        source_struct_size=layout_b.source_struct_size,
        manifest_hash=layout_b.manifest_hash,
    )

    with pytest.raises(SchemaCompatibilityError, match="source layout identity"):
        MarketDataSchemaProjector(schema).project(mixed_record)


def test_unclassified_or_undeclared_vendor_fields_fail_closed() -> None:
    """
    验证 owned record 额外字段与 schema 规则引用的未声明字段都不会被静默丢弃。

    Returns:
        None: 两种字段覆盖缺口均被拒绝后返回。
    """
    projector = MarketDataSchemaProjector(_base_schema())
    extra_fields = _base_fields()
    extra_fields["FutureField"] = VendorFieldObservation(source_type="int32", value=1)
    with pytest.raises(SchemaCompatibilityError, match="未分类"):
        projector.project(_base_record(fields=extra_fields))

    missing_fields = _base_fields()
    del missing_fields["Info1"]
    with pytest.raises(SchemaCompatibilityError, match="未声明字段存在性"):
        projector.project(_base_record(fields=missing_fields))


def _legacy_volume_record(version: str = "1.0") -> OwnedVendorRecord:
    """
    构造仅含旧字段名的最小迁移测试记录。

    Args:
        version: owned record 版本。

    Returns:
        OwnedVendorRecord: 字段名为 ``OldVolume`` 的记录。
    """
    return OwnedVendorRecord(
        provider="synthetic-vendor",
        module="l2",
        sdk_version="fixture-1",
        vendor_schema_id="vendor.l2.fixture.v1",
        record_schema_id="market.migration.fixture",
        record_schema_version=version,
        source_callback="OnSyntheticVolume",
        source_struct="SyntheticVolume",
        source_struct_size=64,
        manifest_hash="migration-manifest",
        mapping_version="map-v1",
        fields={
            "OldVolume": VendorFieldObservation(source_type="int64", value=9223372036854775807)
        },
    )


def _migration_schema() -> MarketDataProjectionSchema:
    """
    构造要求 1.1 记录和新字段名的迁移目标 schema。

    Returns:
        MarketDataProjectionSchema: 只映射 ``Volume64`` 的目标 schema。
    """
    return MarketDataProjectionSchema(
        schema_id="market.migration.fixture",
        schema_version="1.1",
        vendor_schema_id="vendor.l2.fixture.v1",
        field_set_version="migration-field-set-v2",
        mapping_version="map-v2",
        vendor_namespace="synthetic_vendor",
        source_layouts=(
            SourceLayoutIdentity(
                provider="synthetic-vendor",
                module="l2",
                sdk_version="fixture-1",
                source_callback="OnSyntheticVolume",
                source_struct="SyntheticVolume",
                source_struct_size=64,
                manifest_hash="migration-manifest",
            ),
        ),
        field_rules=(
            SchemaFieldRule(
                "Volume64", "volume", FieldLayer.CANONICAL, expected_source_type="int64"
            ),
        ),
    )


def _migrate_volume_v1_to_v1_1(record: OwnedVendorRecord) -> OwnedVendorRecord:
    """
    将旧 ``OldVolume`` 字段显式重命名为 1.1 的 ``Volume64``。

    Args:
        record: 1.0 的 owned record。

    Returns:
        OwnedVendorRecord: 保持厂商源身份的 1.1 新记录。
    """
    fields = dict(record.fields)
    fields["Volume64"] = fields.pop("OldVolume")
    return record.evolve(
        record_schema_version="1.1",
        fields=fields,
        mapping_version="map-v2",
    )


def test_explicit_same_major_migration_preserves_identity_and_int64() -> None:
    """
    验证同 major 显式迁移可用，路径可追溯且 int64 不丢精度。

    Returns:
        None: 迁移后值、版本、路径和原记录不变断言通过后返回。
    """
    record = _legacy_volume_record()
    registry = SchemaMigrationRegistry()
    registry.register("market.migration.fixture", "1.0", "1.1", _migrate_volume_v1_to_v1_1)
    projector = MarketDataSchemaProjector(_migration_schema(), registry)

    projection = projector.project(record)

    assert projection.payload["volume"] == 9223372036854775807
    assert projection.schema_version == "1.1.0"
    assert projection.migration_path == ("1.0.0->1.1.0",)
    assert "OldVolume" in record.fields
    assert "Volume64" not in record.fields


def test_missing_cross_major_downgrade_and_wrong_migration_fail_closed() -> None:
    """
    验证缺迁移、跨 major、自动降级与修改源身份的迁移全部被拒绝。

    Returns:
        None: 所有不安全版本路径均 fail closed 后返回。
    """
    schema = _migration_schema()
    with pytest.raises(SchemaMigrationError, match="未配置"):
        MarketDataSchemaProjector(schema).project(_legacy_volume_record())

    registry = SchemaMigrationRegistry()
    with pytest.raises(SchemaMigrationError, match="跨 major"):
        registry.register("market.migration.fixture", "1.0", "2.0", _migrate_volume_v1_to_v1_1)

    newer = _legacy_volume_record(version="1.2")
    newer = newer.evolve(record_schema_version="1.2", fields=newer.fields, mapping_version="map-v2")
    with pytest.raises(SchemaMigrationError, match="降级"):
        MarketDataSchemaProjector(schema, registry).project(newer)

    def change_provider(record: OwnedVendorRecord) -> OwnedVendorRecord:
        """
        构造一个故意违反源身份不变合同的迁移。

        Args:
            record: 输入 owned record。

        Returns:
            OwnedVendorRecord: 错误篡改 provider 的记录。
        """
        migrated = _migrate_volume_v1_to_v1_1(record)
        return replace(migrated, provider="tampered-provider")

    bad_registry = SchemaMigrationRegistry()
    bad_registry.register("market.migration.fixture", "1.0", "1.1", change_provider)
    with pytest.raises(SchemaMigrationError, match="不得修改"):
        MarketDataSchemaProjector(schema, bad_registry).project(_legacy_volume_record())


def test_schema_rules_reject_duplicate_classification_path_conflicts_and_unpaired_converter() -> (
    None
):
    """
    验证字段重复分类、父子目标冲突与无审计 ID 的 converter 不可进入 schema。

    Returns:
        None: 三类 schema 定义错误均在构造期被拒绝后返回。
    """

    def identity(value: Any) -> Any:
        """
        原样返回字段值，仅用于测试 converter 审计标识校验。

        Args:
            value: 任意 schema 值。

        Returns:
            Any: 未修改的输入值。
        """
        return value

    with pytest.raises(MarketSchemaError, match="同时提供"):
        SchemaFieldRule("LastPrice", "last_price", FieldLayer.CANONICAL, converter=identity)

    duplicate_source = (
        SchemaFieldRule("LastPrice", "last_price", FieldLayer.CANONICAL),
        SchemaFieldRule("LastPrice", "price", FieldLayer.VENDOR),
    )
    with pytest.raises(MarketSchemaError, match="重复分类"):
        _base_schema(field_rules=duplicate_source)

    conflicting_paths = (
        SchemaFieldRule("LastPrice", "depth", FieldLayer.CANONICAL),
        SchemaFieldRule("TotalVolume", "depth.volume", FieldLayer.CANONICAL),
    )
    with pytest.raises(MarketSchemaError, match="路径冲突"):
        _base_schema(field_rules=conflicting_paths)


def test_projection_rejects_source_type_mismatch_and_converter_failure() -> None:
    """
    验证字段类型不符与显式 converter 异常都不会产生部分 payload。

    Returns:
        None: 两种字段级错误均被 SchemaProjectionError 包装后返回。
    """
    wrong_type_fields = _base_fields()
    wrong_type_fields["TotalVolume"] = VendorFieldObservation(source_type="double", value=100.0)
    with pytest.raises(SchemaProjectionError, match="source_type"):
        MarketDataSchemaProjector(_base_schema()).project(_base_record(fields=wrong_type_fields))

    def broken_converter(value: Any) -> Any:
        """
        始终抛错，用于验证转换异常边界。

        Args:
            value: 未使用的原始字段值。

        Returns:
            Any: 永不返回。

        Raises:
            RuntimeError: 每次调用均主动抛出。
        """
        raise RuntimeError(f"cannot convert {value!r}")

    broken_rules = list(_base_schema().field_rules)
    broken_rules[0] = SchemaFieldRule(
        "LastPrice",
        "last_price",
        FieldLayer.CANONICAL,
        expected_source_type="double",
        converter=broken_converter,
        conversion_id="fixture-broken-v1",
    )
    with pytest.raises(SchemaProjectionError, match="fixture-broken-v1"):
        MarketDataSchemaProjector(_base_schema(field_rules=broken_rules)).project(_base_record())
