"""
作者: BruceLee

文件职责: 验证版本化市场事件模型和 JSON wire codec 的离线无损及 fail-closed 合同。
主要输入: 合成 L1/L2/控制事件、int64 序列、bytes、日期时间和损坏 wire payload。
主要输出: 具体事件类 round-trip、来源字段逐值一致与确定性受控错误断言。
上游关系: 覆盖 bullet_trade.market_data.models 和 codec 的公开接口。
下游关系: 为未来 native bridge、远程 market stream、录制回放和 EventBus 接入提供门禁。
关键配置约定: 全部数据脱敏；不联网、不加载厂商 SDK、不执行任何交易动作。
"""

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Type

import pytest

from bullet_trade.market_data import (
    CompatibilityTickEvent,
    ConnectionStateEvent,
    ConsolidatedTickEvent,
    DepthSnapshotEvent,
    IopvEvent,
    MarketDataLevel,
    MarketEvent,
    MarketEventCodecError,
    MarketEventRoute,
    MarketEventType,
    MarketStatusEvent,
    OrderDetailEvent,
    QuoteSnapshotEvent,
    SecurityStatusEvent,
    SequenceGapEvent,
    SourceSequence,
    TransactionEvent,
    UnsupportedMarketEventSchemaError,
    dumps_market_event,
    loads_market_event,
    market_event_from_wire,
    market_event_to_wire,
)

pytestmark = pytest.mark.unit

_EVENT_CAPABILITIES = {
    MarketEventType.TICK_COMPAT: "realtime.stream.tick_compat",
    MarketEventType.SNAPSHOT_L1: "realtime.snapshot.l1",
    MarketEventType.SNAPSHOT_L2: "realtime.snapshot.l2",
    MarketEventType.TRANSACTION: "realtime.stream.transaction",
    MarketEventType.ORDER_DETAIL: "realtime.stream.order_detail",
    MarketEventType.CONSOLIDATED_TICK: "realtime.stream.consolidated_tick",
    MarketEventType.IOPV: "realtime.stream.iopv",
    MarketEventType.SECURITY_STATUS: "realtime.stream.security_status",
    MarketEventType.MARKET_STATUS: "realtime.stream.market_status",
    MarketEventType.STREAM_GAP: "realtime.stream.transaction",
    MarketEventType.STREAM_STATUS: "realtime.stream.transaction",
}


def _event_kwargs(
    event_type: MarketEventType,
    level: MarketDataLevel,
    *,
    requires_security: bool,
    requires_sequence: bool,
) -> Dict[str, Any]:
    """
    构造具名事件模型的最小合法公共参数。

    Args:
        event_type: 目标事件枚举。
        level: 目标行情层级。
        requires_security: 是否补充标准/原始证券代码。
        requires_sequence: 是否补充 stream/channel 和原始序列。

    Returns:
        Dict[str, Any]: 可直接展开给 MarketEvent 子类的参数字典。
    """
    kwargs: Dict[str, Any] = {
        "provider": "mock",
        "capability_key": _EVENT_CAPABILITIES[event_type],
        "event_type": event_type,
        "level": level,
        "exchange": "XSHG",
        "session_epoch": "epoch-1",
        "payload": {"event": event_type.value},
        "gateway_received_at": datetime(2026, 8, 13, 9, 30, 0, 123456),
    }
    if requires_security:
        kwargs.update(
            security="600000.XSHG",
            raw_security_code="600000",
            raw_market_code="SH",
        )
    if requires_sequence:
        kwargs.update(
            stream_id="stream-1",
            channel_id="channel-1",
            source_sequence=SourceSequence(components={"MainSeq": 1, "SubSeq": 2}),
        )
    return kwargs


@pytest.mark.parametrize(
    "event_class,event_type,level,requires_security,requires_sequence",
    (
        (
            CompatibilityTickEvent,
            MarketEventType.TICK_COMPAT,
            MarketDataLevel.TICK_COMPAT,
            True,
            False,
        ),
        (QuoteSnapshotEvent, MarketEventType.SNAPSHOT_L1, MarketDataLevel.L1, True, False),
        (DepthSnapshotEvent, MarketEventType.SNAPSHOT_L2, MarketDataLevel.L2, True, False),
        (TransactionEvent, MarketEventType.TRANSACTION, MarketDataLevel.L2, True, True),
        (OrderDetailEvent, MarketEventType.ORDER_DETAIL, MarketDataLevel.L2, True, True),
        (
            ConsolidatedTickEvent,
            MarketEventType.CONSOLIDATED_TICK,
            MarketDataLevel.L2,
            True,
            True,
        ),
        (IopvEvent, MarketEventType.IOPV, MarketDataLevel.L1, True, False),
        (SecurityStatusEvent, MarketEventType.SECURITY_STATUS, MarketDataLevel.L1, True, False),
        (MarketStatusEvent, MarketEventType.MARKET_STATUS, MarketDataLevel.L1, False, False),
        (SequenceGapEvent, MarketEventType.STREAM_GAP, MarketDataLevel.L2, False, True),
        (
            ConnectionStateEvent,
            MarketEventType.STREAM_STATUS,
            MarketDataLevel.L2,
            False,
            False,
        ),
    ),
)
def test_all_named_event_models_round_trip_to_their_concrete_class(
    event_class: Type[MarketEvent],
    event_type: MarketEventType,
    level: MarketDataLevel,
    requires_security: bool,
    requires_sequence: bool,
) -> None:
    """
    验证全部快照、逐笔、状态和控制事件都按具体类型恢复。

    Args:
        event_class: 当前具名事件类。
        event_type: 与事件类绑定的枚举。
        level: 当前事件合法的行情层级。
        requires_security: 当前模型是否要求证券身份。
        requires_sequence: 当前模型是否要求通道序列。

    Returns:
        None: 断言成功后正常返回。
    """
    event = event_class(
        **_event_kwargs(
            event_type,
            level,
            requires_security=requires_security,
            requires_sequence=requires_sequence,
        )
    )

    restored = loads_market_event(dumps_market_event(event))

    assert type(restored) is event_class
    assert restored == event


def _full_transaction_event() -> TransactionEvent:
    """
    构造覆盖来源、时间、序列和三层字段的完整逐笔事件。

    Returns:
        TransactionEvent: 不依赖 SDK 的完整合成事件。
    """
    china_tz = timezone(timedelta(hours=8), name="CST")
    gateway_time = datetime(2026, 8, 13, 9, 30, 0, 123456, tzinfo=china_tz)
    return TransactionEvent(
        provider="huaxin-mock",
        capability_key="realtime.stream.transaction",
        event_type=MarketEventType.TRANSACTION,
        level=MarketDataLevel.L2,
        exchange="XSHE",
        session_epoch="l2-session-7",
        payload={
            "last_price": 10.125,
            "volume": 9223372036854775807,
            "depth": ({"price": 10.12, "volume": 200},),
        },
        security="000001.XSHE",
        raw_security_code="000001",
        raw_market_code="2",
        asset_type="stock",
        field_set_version="huaxin-l2-v1",
        field_profile="canonical_with_raw",
        route_rule="route-realtime-transaction",
        route=MarketEventRoute(
            provider="huaxin-mock",
            capability_key="realtime.stream.transaction",
            rule_id="route-realtime-transaction",
            semantic_class="realtime_transaction",
            manifest_version="manifest-v3",
            provider_version="4.0.8-test",
            build_id="build-test-1",
            location="remote",
            reason="primary_supported_ready",
        ),
        trading_day=date(2026, 8, 13),
        trading_day_source="authenticated_session",
        exchange_time=gateway_time - timedelta(microseconds=50),
        gateway_received_at=gateway_time,
        client_received_at=gateway_time + timedelta(milliseconds=2),
        stream_id="l2-transaction-xshe",
        channel_id="channel-3",
        source_sequence=SourceSequence(
            components={
                "MainSeq": 9223372036854775807,
                "SubSeq": 18446744073709551615,
                "RawMarker": b"\xff\x00",
            }
        ),
        raw_type="Transaction",
        provider_extension={
            "huaxin_tora": {
                "schema_version": "1",
                "ExecType": "F",
                "BidApplSeqNum": 9223372036854775807,
            }
        },
        raw_profile={"UnknownEnum": b"Z", "SecurityName": b"\xff\xfe"},
        field_presence=("last_price", "volume", "ExecType"),
        completeness=False,
        missing_fields=("security_name",),
    )


def test_full_event_wire_round_trip_preserves_identity_time_int64_and_bytes() -> None:
    """
    验证标准/原始代码、时间、来源、int64、bytes 和三层载荷逐值一致。

    Returns:
        None: 全部逐值和不可变性断言成功后返回。
    """
    event = _full_transaction_event()

    encoded = dumps_market_event(event)
    wire = json.loads(encoded)
    restored = loads_market_event(encoded.encode("utf-8"))

    assert '"integer"' in encoded
    assert '"bytes"' in encoded
    assert wire["gateway_received_at"]["iso"].endswith("+08:00")
    assert restored == event
    assert restored.security == "000001.XSHE"
    assert restored.raw_security_code == "000001"
    assert restored.raw_security == "000001"
    assert restored.raw_market_code == "2"
    assert wire["raw_security"] == "000001"
    assert "raw_security_code" not in wire
    assert restored.source_sequence["MainSeq"] == 9223372036854775807
    assert restored.source_sequence["SubSeq"] == 18446744073709551615
    assert restored.source_sequence["RawMarker"] == b"\xff\x00"
    assert restored.raw_profile["SecurityName"] == b"\xff\xfe"
    assert restored.gateway_received_at == event.gateway_received_at
    assert restored.client_received_at != restored.gateway_received_at

    with pytest.raises(TypeError):
        restored.payload["depth"][0]["price"] = 11.0


def test_unknown_schema_fields_classes_and_mismatched_models_fail_closed() -> None:
    """
    验证未知版本、额外字段、未知类及 class/type 不一致均不会被静默解析。

    Returns:
        None: 所有损坏 wire 均触发预期受控异常后返回。
    """
    wire = market_event_to_wire(_full_transaction_event())

    unknown_schema = dict(wire)
    unknown_schema["schema_version"] = "2"
    with pytest.raises(UnsupportedMarketEventSchemaError):
        market_event_from_wire(unknown_schema)

    unknown_field = dict(wire)
    unknown_field["future_field"] = "must-bump-schema"
    with pytest.raises(MarketEventCodecError, match="unknown"):
        market_event_from_wire(unknown_field)

    unknown_class = dict(wire)
    unknown_class["event_class"] = "FutureTransactionEvent"
    with pytest.raises(MarketEventCodecError, match="未知市场事件类"):
        market_event_from_wire(unknown_class)

    mismatched_class = dict(wire)
    mismatched_class["event_class"] = "QuoteSnapshotEvent"
    with pytest.raises(MarketEventCodecError, match="必须使用 event_type"):
        market_event_from_wire(mismatched_class)


def test_codec_rejects_unsupported_python_values_and_duplicate_json_keys() -> None:
    """
    验证 codec 不会字符串化未知对象，JSON parser 也拒绝重复键覆盖。

    Returns:
        None: 未知对象和重复键均被拒绝后返回。
    """
    event = MarketEvent(
        provider="mock",
        capability_key="realtime.snapshot.l1",
        event_type=MarketEventType.SNAPSHOT_L1,
        level=MarketDataLevel.L1,
        exchange="XSHG",
        session_epoch="epoch-1",
        payload={"unsupported": object()},
    )
    with pytest.raises(MarketEventCodecError, match="不支持"):
        market_event_to_wire(event)

    with pytest.raises(MarketEventCodecError, match="重复键"):
        loads_market_event('{"schema_version":"1","schema_version":"1"}')


def test_sequence_scope_rejects_unproven_global_ordering() -> None:
    """
    验证原始序列只能声明局部通道/会话作用域，不能虚构全局总序。

    Returns:
        None: global ordering 声明被模型拒绝后返回。
    """
    with pytest.raises(ValueError, match="全局顺序"):
        SourceSequence(components={"Sequence": 1}, ordering_scope="global")


def test_named_transaction_requires_security_and_channel_sequence() -> None:
    """
    验证逐笔模型缺少标准身份或局部序列证据时同步拒绝。

    Returns:
        None: 两类缺失证据均触发 ValueError 后返回。
    """
    kwargs = _event_kwargs(
        MarketEventType.TRANSACTION,
        MarketDataLevel.L2,
        requires_security=False,
        requires_sequence=False,
    )
    with pytest.raises(ValueError, match="security"):
        TransactionEvent(**kwargs)

    kwargs.update(security="600000.XSHG", raw_security_code="600000")
    with pytest.raises(ValueError, match="stream_id"):
        TransactionEvent(**kwargs)


def test_gateway_time_is_not_overwritten_by_client_arrival_time() -> None:
    """
    验证 gateway 首次接收时间与更晚的客户端接收时间独立保存。

    Returns:
        None: 两个时间点 round-trip 后仍彼此独立即返回。
    """
    event = _full_transaction_event()
    wire = market_event_to_wire(event)
    restored = market_event_from_wire(wire)

    assert restored.gateway_received_at == event.gateway_received_at
    assert restored.client_received_at == event.client_received_at
    assert restored.client_received_at > restored.gateway_received_at


def test_named_event_capability_and_direct_model_types_fail_closed() -> None:
    """
    验证具名业务事件绑定精确 capability，直接构造也执行 bool/date/time 强校验。

    Returns:
        None: 所有 capability 和直接字段类型错误均被模型同步拒绝后返回。
    """
    valid = _event_kwargs(
        MarketEventType.SNAPSHOT_L1,
        MarketDataLevel.L1,
        requires_security=True,
        requires_sequence=False,
    )
    wrong_capability = dict(valid)
    wrong_capability["capability_key"] = "history.bars"
    with pytest.raises(ValueError, match="capability_key"):
        QuoteSnapshotEvent(**wrong_capability)

    missing_gateway_time = dict(valid)
    missing_gateway_time["gateway_received_at"] = None
    with pytest.raises(ValueError, match="gateway_received_at"):
        QuoteSnapshotEvent(**missing_gateway_time)

    invalid_completeness = dict(valid)
    invalid_completeness["completeness"] = 1
    with pytest.raises(ValueError, match="completeness"):
        QuoteSnapshotEvent(**invalid_completeness)

    invalid_trading_day = dict(valid)
    invalid_trading_day["trading_day"] = datetime(2026, 8, 13, 0, 0)
    invalid_trading_day["trading_day_source"] = "session"
    with pytest.raises(ValueError, match="trading_day"):
        QuoteSnapshotEvent(**invalid_trading_day)

    invalid_gateway_type = dict(valid)
    invalid_gateway_type["gateway_received_at"] = "2026-08-13T09:30:00"
    with pytest.raises(ValueError, match="gateway_received_at"):
        QuoteSnapshotEvent(**invalid_gateway_type)


def test_decoder_rejects_unmarked_integer_outside_json_safe_range() -> None:
    """
    验证恶意或旧对端不能用普通 JSON number 绕过大整数标签合同。

    Returns:
        None: source sequence 中未标记的大整数被受控拒绝后返回。

    Side Effects:
        仅修改本测试持有的独立 wire 字典，不修改原 MarketEvent。
    """
    wire = market_event_to_wire(_full_transaction_event())
    sequence_values = wire["source_sequence"]["values"]
    for pair in sequence_values["items"]:
        if pair[0] == "MainSeq":
            pair[1] = 9223372036854775807
            break
    else:  # pragma: no cover - fixture 合同变化时给出明确失败
        pytest.fail("测试 wire 缺少 MainSeq")

    with pytest.raises(MarketEventCodecError, match="安全范围"):
        market_event_from_wire(wire)


def test_decoder_rejects_unmarked_nonfinite_float() -> None:
    """
    验证进程内 mapping 不能用 raw NaN 绕过特殊浮点 wire type 标签。

    Returns:
        None: 未标记 NaN 被受控拒绝后返回。

    Side Effects:
        仅修改本测试持有的独立 wire 字典，不修改原 MarketEvent。
    """
    wire = market_event_to_wire(_full_transaction_event())
    payload = wire["payload"]
    payload["items"].append(["invalid_float", float("nan")])

    with pytest.raises(MarketEventCodecError, match="非有限浮点"):
        market_event_from_wire(wire)
