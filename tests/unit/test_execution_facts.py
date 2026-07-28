"""
BulletTrade 回测机器事实 writer 的单元与故障边界测试。

作者: BruceLee
文件职责: 冻结 execution-facts/v1 envelope、事件语义、流式写入和原子发布合同。
主要输入: 临时运行目录、确定性事件、故障注入和受控 secret canary。
主要输出: 对 facts、manifest、失败关闭、幂等身份和安全边界的断言。
上下游关系: 上游是 OpenSpec 机器事实合同，下游是未来回测引擎接线和 Quant consumer。
关键约定: 测试不连接数据库、Web、QMT 或网络，不以 legacy CSV 冒充完整机器事实。
"""

import hashlib
import json
import shutil
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest

import bullet_trade.core.execution_facts as facts_module
from bullet_trade.core.analysis import export_trades
from bullet_trade.core.execution_facts import (
    EXECUTION_FACTS_V1_COMPATIBILITY,
    EXECUTION_FACTS_V1_JSON_SCHEMA,
    SCHEMA_VERSION,
    EventType,
    ExecutionFactsAlreadyPublishedError,
    ExecutionFactsConflictError,
    ExecutionFactsError,
    ExecutionFactsIntegrityError,
    ExecutionFactsSecurityError,
    ExecutionFactsValidationError,
    ExecutionFactsWriter,
    FeeType,
    build_source_event_id,
    decimal_to_text,
    validate_facts_file,
    validate_published_execution_facts,
)
from bullet_trade.core.price_basis import EffectivePriceBasis

pytestmark = pytest.mark.unit

RUN_ID = "12345678-1234-5678-9234-567812345678"
OCCURRED_AT = datetime(2026, 7, 14, 18, 30, 1, 123456, tzinfo=timezone.utc)
PRICE_BASIS = {
    "use_real_price": False,
    "fq": "none",
    "provider": "unit-provider",
    "business_timezone": "Asia/Shanghai",
    "reference_policy": "not_applicable",
    "configured_ref_date": None,
    "business_date_start": "2026-07-15",
    "business_date_end": "2026-07-15",
}


def _writer(tmp_path: Path, name: str = "execution-facts", **kwargs: Any) -> ExecutionFactsWriter:
    """创建使用固定元数据的测试 writer。

    Args:
        tmp_path: pytest 提供的临时目录。
        name: 当前 writer 的子目录名称。
        **kwargs: 覆盖 writer 构造参数的值。

    Returns:
        ExecutionFactsWriter: 尚未 finalize 的 writer。
    """

    options: Dict[str, Any] = {
        "output_dir": tmp_path / name,
        "run_id": RUN_ID,
        "producer_version": "0.9.2",
        "calculation_version": "engine-ledger/v1",
        "price_basis": PRICE_BASIS,
        "trusted_output_root": tmp_path,
        "started_at": OCCURRED_AT,
        "buffer_size_bytes": 128,
    }
    options.update(kwargs)
    return ExecutionFactsWriter(**options)


def _order_payload(note: str = "unit") -> Dict[str, Any]:
    """构造合法订单意图 payload。

    Args:
        note: 订单备注，用于普通与脱敏测试。

    Returns:
        Dict[str, Any]: 符合 V1 白名单的订单意图字段。
    """

    return {
        "order_id": "order-1",
        "security": "511880.XSHG",
        "side": "BUY",
        "requested_quantity": Decimal("100"),
        "order_type": "market",
        "requested_price": Decimal("100.0710"),
        "note": note,
    }


def _append_order(writer: ExecutionFactsWriter, note: str = "unit") -> Dict[str, Any]:
    """向 writer 追加一条合法订单事实。

    Args:
        writer: 接收事实的 writer。
        note: 订单备注。

    Returns:
        Dict[str, Any]: 已规范化的事实 envelope。
    """

    writer.observe_effective_price_basis(
        EffectivePriceBasis.create(
            use_real_price=False,
            provider=writer.price_basis["provider"],
            business_time=OCCURRED_AT,
        )
    )
    return writer.append(
        EventType.ORDER_INTENT,
        authority_id="order-1",
        state_version=1,
        occurred_at=OCCURRED_AT,
        payload=_order_payload(note=note),
    )


def _complete_order_for_publish(writer: ExecutionFactsWriter) -> None:
    """把测试订单收口为拒单，并补齐每日账户、绩效和对账事实。

    Args:
        writer: 已由 ``_append_order`` 写入 order_intent 的 writer。

    Returns:
        None: required 发布所需的剩余事实已追加。
    """

    writer.append(
        EventType.ORDER_EVENT,
        authority_id="order-1",
        state_version=2,
        occurred_at=OCCURRED_AT,
        payload={
            "order_id": "order-1",
            "before_status": "open",
            "after_status": "rejected",
            "requested_quantity": Decimal("100"),
            "filled_quantity": Decimal("0"),
            "remaining_quantity": Decimal("100"),
            "reason": "unit-rejection",
        },
    )
    writer.append(
        EventType.RESERVATION,
        authority_id="reservation-1",
        state_version=1,
        occurred_at=OCCURRED_AT,
        payload={
            "reservation_id": "reservation-1",
            "order_id": "order-1",
            "resource_type": "cash",
            "action": "released",
            "value": Decimal("0"),
            "unit": "CNY",
            "reason": "unit-rejection",
        },
    )
    writer.append(
        EventType.ACCOUNT_DAILY,
        authority_id="account-1",
        state_version=1,
        occurred_at=OCCURRED_AT,
        payload={
            "account_id": "account-1",
            "cash": Decimal("100000"),
            "available_cash": Decimal("100000"),
            "locked_cash": Decimal("0"),
            "positions_value": Decimal("0"),
            "total_value": Decimal("100000"),
        },
    )
    writer.append(
        EventType.DAILY_PERFORMANCE,
        authority_id="performance-1",
        state_version=1,
        occurred_at=OCCURRED_AT,
        payload={
            "account_id": "account-1",
            "total_value": Decimal("100000"),
            "net_asset_value": Decimal("1"),
            "daily_return": Decimal("0"),
            "cumulative_return": Decimal("0"),
            "strategy_return": Decimal("0"),
            "drawdown": Decimal("0"),
        },
    )
    writer.append(
        EventType.RECONCILE_EVENT,
        authority_id="reconcile-1",
        state_version=1,
        occurred_at=OCCURRED_AT,
        payload={
            "metric_name": "total-assets",
            "status": "PASSED",
            "expected": Decimal("100000"),
            "actual": Decimal("100000"),
            "difference": Decimal("0"),
        },
    )


def _raw_fact(
    *,
    sequence: int = 1,
    source_event_id: str = "805dbc67-8089-50ce-a875-f342406370c1",
    schema_version: str = SCHEMA_VERSION,
) -> Dict[str, Any]:
    """构造用于破坏性校验测试的原始事实。

    Args:
        sequence: 事实序号。
        source_event_id: 稳定事件 UUID。
        schema_version: 待验证的 schema 版本。

    Returns:
        Dict[str, Any]: 可直接序列化为 NDJSON 的 envelope。
    """

    return {
        "schema_version": schema_version,
        "run_id": RUN_ID,
        "source_event_id": source_event_id,
        "sequence": sequence,
        "event_type": "order_intent",
        "occurred_at": "2026-07-14T18:30:01.123456Z",
        "trade_date": "2026-07-15",
        "payload": {
            "order_id": "order-1",
            "security": "511880.XSHG",
            "side": "BUY",
            "requested_quantity": "100",
            "order_type": "market",
        },
    }


def _write_ndjson(path: Path, *records: Dict[str, Any]) -> bytes:
    """把原始事实写成 canonical NDJSON。

    Args:
        path: 目标文件。
        *records: 要按顺序写入的事实。

    Returns:
        bytes: 实际写入的字节内容。

    Side Effects:
        创建或覆盖测试临时文件。
    """

    content = b"".join(
        (
            json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        for record in records
    )
    path.write_bytes(content)
    return content


def test_schema_and_compatibility_contract_is_frozen() -> None:
    """验证 V1 schema、事件枚举、费用枚举和兼容规则已显式冻结。"""

    assert SCHEMA_VERSION == "execution-facts/v1"
    assert EXECUTION_FACTS_V1_JSON_SCHEMA["$id"].endswith("execution-facts-v1.schema.json")
    assert EXECUTION_FACTS_V1_JSON_SCHEMA["additionalProperties"] is False
    assert EXECUTION_FACTS_V1_JSON_SCHEMA["required"] == [
        "schema_version",
        "run_id",
        "source_event_id",
        "sequence",
        "event_type",
        "occurred_at",
        "trade_date",
        "payload",
    ]
    assert set(EXECUTION_FACTS_V1_JSON_SCHEMA["properties"]["event_type"]["enum"]) == {
        item.value for item in EventType
    }
    assert {item.value for item in FeeType} == {
        "commission",
        "stamp_tax",
        "transfer_fee",
        "order_flow",
        "cancel_flow",
        "slippage",
        "other",
    }
    assert EXECUTION_FACTS_V1_COMPATIBILITY == {
        "unknown_major": "reject",
        "unknown_event_type": "reject",
        "unknown_payload_field": "reject",
        "authority": "complete_guard_manifest_and_facts",
        "legacy_complete_contract": False,
    }


def test_decimal_text_is_exact_and_canonical() -> None:
    """验证 Decimal 和整数被规范化，float、NaN 与 Infinity 被拒绝。"""

    assert decimal_to_text(Decimal("100.0710")) == "100.071"
    assert decimal_to_text(Decimal("-0.000")) == "0"
    assert decimal_to_text(100) == "100"
    assert decimal_to_text("1E+3") == "1000"
    for value in (1.1, Decimal("NaN"), Decimal("Infinity"), "-Infinity", True):
        with pytest.raises(ExecutionFactsValidationError):
            decimal_to_text(value)
    for value in ("", "not-decimal"):
        with pytest.raises(ExecutionFactsValidationError):
            decimal_to_text(value)


def test_identity_payload_and_time_helper_rejection_matrix() -> None:
    """覆盖身份、payload、元数据和 UTC 规范化的拒绝分支。"""

    with pytest.raises(ExecutionFactsValidationError, match="字段类型"):
        facts_module._field_json_schema("unknown")
    invalid_source_inputs = [
        {"authority_id": "", "state_version": 1, "sequence": 1},
        {"authority_id": "bad\x00id", "state_version": 1, "sequence": 1},
        {"authority_id": "order", "state_version": True, "sequence": 1},
        {"authority_id": "order", "state_version": "", "sequence": 1},
        {"authority_id": "order", "state_version": 1, "sequence": 0},
    ]
    for values in invalid_source_inputs:
        with pytest.raises(ExecutionFactsValidationError):
            build_source_event_id(RUN_ID, EventType.ORDER_INTENT, **values)

    with pytest.raises(ExecutionFactsValidationError, match="文本"):
        facts_module._redact_text(1, "note")
    with pytest.raises(ExecutionFactsValidationError, match="超长"):
        facts_module._redact_text("x" * 4097, "note")
    with pytest.raises(ExecutionFactsValidationError, match="业务标识"):
        facts_module._validate_identifier("bad/path", "order_id")
    with pytest.raises(ExecutionFactsSecurityError, match="秘密"):
        facts_module._validate_identifier("SECRET_CANARY_identity", "order_id")
    with pytest.raises(ExecutionFactsValidationError, match="family/version"):
        facts_module._validate_contract_version("version-only", "version")
    with pytest.raises(ExecutionFactsSecurityError, match="不安全"):
        facts_module._validate_contract_version("family/..", "version")
    with pytest.raises(ExecutionFactsValidationError, match="mapping"):
        facts_module._normalize_payload(EventType.ORDER_INTENT, [])
    with pytest.raises(ExecutionFactsValidationError, match="字段名"):
        facts_module._normalize_payload(EventType.ORDER_INTENT, {1: "bad"})
    with pytest.raises(ExecutionFactsValidationError, match="缺少"):
        facts_module._normalize_payload(EventType.ORDER_INTENT, {})
    with pytest.raises(ExecutionFactsValidationError, match="UTC Z"):
        facts_module._validate_utc_text("2026-07-15T00:00:00", "time")
    with pytest.raises(ExecutionFactsValidationError, match="有效 UTC"):
        facts_module._validate_utc_text("invalidZ", "time")
    with pytest.raises(ExecutionFactsValidationError, match="canonical UTC"):
        facts_module._validate_utc_text("2026-07-15T00:00:00Z", "time")


def test_envelope_has_stable_id_strict_sequence_and_shanghai_trade_date(tmp_path: Path) -> None:
    """验证 envelope 身份、序号、UTC 和跨零点业务日期。"""

    writer = _writer(tmp_path, name="first")
    first = _append_order(writer)
    second = writer.append(
        EventType.RUN_METRIC,
        authority_id="metric-total-return",
        state_version=1,
        occurred_at=OCCURRED_AT + timedelta(seconds=1),
        payload={"metric_name": "total_return", "metric_value": Decimal("0.001")},
    )
    writer.abort()

    same = _writer(tmp_path, name="second")
    repeated = _append_order(same)
    same.abort()

    assert first["sequence"] == 1
    assert second["sequence"] == 2
    assert first["source_event_id"] == repeated["source_event_id"]
    assert first["source_event_id"] == build_source_event_id(
        RUN_ID, EventType.ORDER_INTENT, "order-1", 1, 1
    )
    assert first["occurred_at"] == "2026-07-14T18:30:01.123456Z"
    assert first["trade_date"] == "2026-07-15"
    assert first["payload"]["requested_price"] == "100.071"


@pytest.mark.parametrize(
    ("event_type", "payload"),
    [
        (EventType.ORDER_INTENT, _order_payload()),
        (
            EventType.ORDER_EVENT,
            {
                "order_id": "order-1",
                "before_status": "open",
                "after_status": "filled",
                "requested_quantity": Decimal("100"),
                "filled_quantity": Decimal("100"),
                "remaining_quantity": Decimal("0"),
            },
        ),
        (
            EventType.FILL,
            {
                "order_id": "order-1",
                "fill_id": "fill-1",
                "security": "511880.XSHG",
                "side": "BUY",
                "quantity": Decimal("100"),
                "price": Decimal("100.071"),
                "amount": Decimal("10007.1"),
            },
        ),
        (
            EventType.FEE,
            {
                "order_id": "order-1",
                "fee_type": "commission",
                "amount": Decimal("0"),
                "source": "cost-model",
            },
        ),
        (
            EventType.RESERVATION,
            {
                "reservation_id": "reservation-1",
                "order_id": "order-1",
                "resource_type": "cash",
                "action": "created",
                "value": Decimal("10007.1"),
                "unit": "CNY",
            },
        ),
        (
            EventType.CASH_LEDGER,
            {
                "entry_id": "cash-1",
                "account_id": "account-1",
                "category": "principal",
                "amount": Decimal("-10007.1"),
                "balance": Decimal("89992.9"),
                "source": "fill",
            },
        ),
        (
            EventType.ACCOUNT_DAILY,
            {
                "account_id": "account-1",
                "cash": Decimal("89992.9"),
                "available_cash": Decimal("89992.9"),
                "locked_cash": Decimal("0"),
                "positions_value": Decimal("10011.9"),
                "total_value": Decimal("100004.8"),
            },
        ),
        (
            EventType.POSITION_DAILY,
            {
                "account_id": "account-1",
                "security": "511880.XSHG",
                "quantity": Decimal("100"),
                "available_quantity": Decimal("100"),
                "average_cost": Decimal("100.071"),
                "price": Decimal("100.119"),
                "market_value": Decimal("10011.9"),
            },
        ),
        (
            EventType.DAILY_PERFORMANCE,
            {
                "account_id": "account-1",
                "total_value": Decimal("100004.8"),
                "net_asset_value": Decimal("1.000048"),
                "daily_return": Decimal("0.000048"),
                "cumulative_return": Decimal("0.000048"),
                "strategy_return": Decimal("0.000048"),
                "drawdown": Decimal("0"),
            },
        ),
        (
            EventType.RUN_METRIC,
            {"metric_name": "total-return", "metric_value": Decimal("0.000048")},
        ),
        (
            EventType.RECONCILE_EVENT,
            {
                "metric_name": "total-assets",
                "status": "PASSED",
                "expected": Decimal("100004.8"),
                "actual": Decimal("100004.8"),
                "difference": Decimal("0"),
            },
        ),
    ],
)
def test_every_v1_event_type_accepts_its_frozen_payload(
    tmp_path: Path, event_type: EventType, payload: Dict[str, Any]
) -> None:
    """验证所有 V1 事件类型均有可执行的严格白名单 payload。

    Args:
        tmp_path: pytest 临时目录。
        event_type: 当前参数化事件类型。
        payload: 对应事件的最小合法 payload。
    """

    writer = _writer(tmp_path, name=event_type.value)
    envelope = writer.append(
        event_type,
        authority_id="authority-1",
        state_version=1,
        occurred_at=OCCURRED_AT,
        payload=payload,
    )
    writer.abort()
    assert envelope["event_type"] == event_type.value
    assert envelope["sequence"] == 1


def test_append_is_bounded_and_finalize_publishes_verified_manifest(tmp_path: Path) -> None:
    """验证有界缓冲、facts 校验和 manifest 原子发布。"""

    writer = _writer(tmp_path, buffer_size_bytes=64)
    _append_order(writer)
    _complete_order_for_publish(writer)
    assert writer.buffered_bytes <= 64
    manifest = writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=5))

    output_dir = tmp_path / "execution-facts"
    facts_path = output_dir / "facts.ndjson"
    manifest_path = output_dir / "manifest.json"
    assert facts_path.is_file()
    assert manifest_path.is_file()
    assert facts_path.stat().st_mode & 0o777 <= 0o600
    assert manifest_path.stat().st_mode & 0o777 <= 0o600
    assert not (output_dir / "facts.ndjson.partial").exists()
    assert not (output_dir / "manifest.json.partial").exists()
    assert not (output_dir / ".publish.incomplete").exists()
    assert (output_dir / ".publish.complete").is_file()
    assert (output_dir / ".publish.complete").stat().st_mode & 0o777 <= 0o600
    assert manifest["schema_version"] == SCHEMA_VERSION
    assert manifest["producer"] == {"name": "bullet-trade", "version": "0.9.2"}
    assert manifest["facts"]["record_count"] == 6
    assert manifest["facts"]["first_sequence"] == 1
    assert manifest["facts"]["last_sequence"] == 6
    assert manifest["facts"]["byte_size"] == facts_path.stat().st_size
    assert manifest["facts"]["sha256"] == hashlib.sha256(facts_path.read_bytes()).hexdigest()
    assert manifest["quality"]["status"] == "PASSED"
    assert manifest["quality"]["event_counts"]["order_intent"] == 1
    assert manifest["quality"]["event_counts"]["fill"] == 0
    assert set(manifest["quality"]["event_counts"]) == {item.value for item in EventType}
    assert validate_published_execution_facts(output_dir, expected_run_id=RUN_ID) == manifest


def test_manifest_records_price_basis_and_quality_metadata(tmp_path: Path) -> None:
    """验证 manifest 记录计算版本、价格口径和 legacy 对账状态。"""

    writer = _writer(tmp_path)
    _append_order(writer)
    _complete_order_for_publish(writer)
    manifest = writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=2))

    assert manifest["calculation_version"] == "engine-ledger/v1"
    assert manifest["price_basis"] == PRICE_BASIS
    assert manifest["quality"]["checks_version"] == "execution-facts-quality/v1"
    assert manifest["quality"]["legacy_reconciliation"] == {
        "status": "PASSED",
        "version": "execution-facts-reconcile/v1",
    }


@pytest.mark.parametrize(
    "run_id",
    [
        "../escape",
        "12345678/1234/5678/9234/567812345678",
        "12345678-1234-5678-9234-567812345678-extra",
        "12345678123456789234567812345678",
        "not-a-uuid",
        "",
    ],
)
def test_invalid_run_id_is_rejected_before_output_creation(tmp_path: Path, run_id: str) -> None:
    """验证非法运行身份不会创建目录或文件。

    Args:
        tmp_path: pytest 临时目录。
        run_id: 待拒绝的非法运行身份。
    """

    output_dir = tmp_path / "execution-facts"
    with pytest.raises(ExecutionFactsValidationError):
        _writer(tmp_path, run_id=run_id)
    assert not output_dir.exists()


def test_unknown_event_fee_and_payload_field_are_rejected(tmp_path: Path) -> None:
    """验证未知事件、费用分类和 payload 字段 fail closed。"""

    writer = _writer(tmp_path)
    with pytest.raises(ExecutionFactsValidationError):
        writer.append(
            "future_event",
            authority_id="future-1",
            state_version=1,
            occurred_at=OCCURRED_AT,
            payload={},
        )
    with pytest.raises(ExecutionFactsValidationError):
        writer.append(
            EventType.FEE,
            authority_id="fee-1",
            state_version=1,
            occurred_at=OCCURRED_AT,
            payload={
                "order_id": "order-1",
                "fee_type": "unknown_fee",
                "amount": Decimal("1"),
                "source": "unit",
            },
        )
    payload = _order_payload()
    payload["environment"] = "SHOULD_NOT_PERSIST"
    with pytest.raises(ExecutionFactsValidationError):
        writer.append(
            EventType.ORDER_INTENT,
            authority_id="order-2",
            state_version=1,
            occurred_at=OCCURRED_AT,
            payload=payload,
        )
    writer.abort()


def test_naive_time_and_noncanonical_security_are_rejected(tmp_path: Path) -> None:
    """验证无时区时间和非 canonical 证券代码被拒绝。"""

    writer = _writer(tmp_path)
    with pytest.raises(ExecutionFactsValidationError):
        writer.append(
            EventType.ORDER_INTENT,
            authority_id="order-naive",
            state_version=1,
            occurred_at=datetime(2026, 7, 15, 9, 30),
            payload=_order_payload(),
        )
    for security in ("511880", "511880.SSE", "511880.xshg", "../511880.XSHG"):
        payload = _order_payload()
        payload["security"] = security
        with pytest.raises(ExecutionFactsValidationError):
            writer.append(
                EventType.ORDER_INTENT,
                authority_id="order-code",
                state_version=1,
                occurred_at=OCCURRED_AT,
                payload=payload,
            )
    writer.abort()


def test_duplicate_sequence_id_gap_and_unknown_schema_are_rejected(tmp_path: Path) -> None:
    """验证读取校验拒绝重复 ID、重复/缺口 sequence 与未知 schema。"""

    path = tmp_path / "facts.ndjson.partial"
    first = _raw_fact(sequence=1)
    duplicate_id = _raw_fact(sequence=2, source_event_id=first["source_event_id"])
    _write_ndjson(path, first, duplicate_id)
    with pytest.raises(ExecutionFactsIntegrityError, match="source_event_id"):
        validate_facts_file(path, expected_run_id=RUN_ID)

    _write_ndjson(
        path, first, _raw_fact(sequence=1, source_event_id="01fdba82-7413-5dc7-96ee-c9c57ad1f834")
    )
    with pytest.raises(ExecutionFactsIntegrityError, match="sequence"):
        validate_facts_file(path, expected_run_id=RUN_ID)

    _write_ndjson(
        path, first, _raw_fact(sequence=3, source_event_id="01fdba82-7413-5dc7-96ee-c9c57ad1f834")
    )
    with pytest.raises(ExecutionFactsIntegrityError, match="sequence"):
        validate_facts_file(path, expected_run_id=RUN_ID)

    _write_ndjson(path, _raw_fact(schema_version="execution-facts/v2"))
    with pytest.raises(ExecutionFactsValidationError, match="schema"):
        validate_facts_file(path, expected_run_id=RUN_ID)


def test_disk_bucket_tracker_detects_non_adjacent_duplicate_exactly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """强制 spill 后仍精确发现跨桶缓冲的非相邻重复 UUID。

    Args:
        tmp_path: pytest 临时目录。
        monkeypatch: 用于把 spill 门槛缩小到一条记录的测试工具。
    """

    monkeypatch.setattr(facts_module, "_EVENT_ID_MEMORY_LIMIT", 1)
    path = tmp_path / "facts.ndjson.partial"
    first = _raw_fact(sequence=1)
    second = _raw_fact(
        sequence=2,
        source_event_id="01fdba82-7413-5dc7-96ee-c9c57ad1f834",
    )
    duplicate = _raw_fact(sequence=3, source_event_id=first["source_event_id"])
    _write_ndjson(path, first, second, duplicate)

    with pytest.raises(ExecutionFactsIntegrityError, match="source_event_id"):
        validate_facts_file(path, expected_run_id=RUN_ID)


def test_envelope_corruption_matrix_is_rejected(tmp_path: Path) -> None:
    """验证 envelope 顶层、身份、时间、业务日与 canonical payload 损坏均被拒绝。"""

    path = tmp_path / "facts.ndjson.partial"
    cases = []
    cases.append([])
    extra = _raw_fact()
    extra["unknown"] = "value"
    cases.append(extra)
    bad_event_id = _raw_fact(source_event_id="not-a-uuid")
    cases.append(bad_event_id)
    v4_event_id = _raw_fact(source_event_id="12345678-1234-4678-9234-567812345678")
    cases.append(v4_event_id)
    bad_time = _raw_fact()
    bad_time["occurred_at"] = "invalidZ"
    cases.append(bad_time)
    bad_date = _raw_fact()
    bad_date["trade_date"] = "invalid"
    cases.append(bad_date)
    wrong_date = _raw_fact()
    wrong_date["trade_date"] = "2026-07-14"
    cases.append(wrong_date)
    noncanonical_decimal = _raw_fact()
    noncanonical_decimal["payload"]["requested_quantity"] = "100.0"
    cases.append(noncanonical_decimal)
    for index, value in enumerate(cases):
        path.write_text(json.dumps(value) + "\n", encoding="utf-8")
        with pytest.raises((ExecutionFactsValidationError, ExecutionFactsIntegrityError)):
            validate_facts_file(path, expected_run_id=RUN_ID)
        assert index >= 0

    _write_ndjson(path, _raw_fact())
    with pytest.raises(ExecutionFactsIntegrityError, match="run_id"):
        validate_facts_file(
            path,
            expected_run_id="87654321-4321-5678-9234-567812345678",
        )


def test_corrupt_partial_line_count_size_and_sha_are_rejected(tmp_path: Path) -> None:
    """验证损坏 NDJSON 以及行数、字节数、SHA 不符均 fail closed。"""

    path = tmp_path / "facts.ndjson.partial"
    content = _write_ndjson(path, _raw_fact())
    summary = validate_facts_file(
        path,
        expected_run_id=RUN_ID,
        expected_record_count=1,
        expected_byte_size=len(content),
        expected_sha256=hashlib.sha256(content).hexdigest(),
    )
    assert summary.record_count == 1

    with pytest.raises(ExecutionFactsIntegrityError, match="行数"):
        validate_facts_file(path, expected_record_count=2)
    with pytest.raises(ExecutionFactsIntegrityError, match="字节"):
        validate_facts_file(path, expected_byte_size=len(content) + 1)
    with pytest.raises(ExecutionFactsIntegrityError, match="SHA"):
        validate_facts_file(path, expected_sha256="0" * 64)

    path.write_bytes(content + b"not-json\n")
    with pytest.raises(ExecutionFactsIntegrityError, match="NDJSON"):
        validate_facts_file(path, expected_run_id=RUN_ID)


def test_finalize_detects_partial_tampering_and_keeps_manifest_unpublished(tmp_path: Path) -> None:
    """验证 finalize 重读 partial，可发现写入后的篡改。"""

    writer = _writer(tmp_path)
    _append_order(writer)
    _complete_order_for_publish(writer)
    writer.flush(durable=True)
    partial_path = tmp_path / "execution-facts" / "facts.ndjson.partial"
    partial_path.write_bytes(partial_path.read_bytes() + b"corrupt\n")

    with pytest.raises(ExecutionFactsIntegrityError):
        writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=1))
    assert partial_path.exists()
    assert not (partial_path.parent / "manifest.json").exists()


def test_quality_failure_and_post_publish_recheck_failure_leave_no_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证质量失败和发布后复验失败均撤销成功 manifest。

    Args:
        tmp_path: pytest 临时目录。
        monkeypatch: pytest 故障注入工具。
    """

    quality_writer = _writer(tmp_path, name="quality")
    _append_order(quality_writer)
    with pytest.raises(ExecutionFactsIntegrityError, match="质量失败"):
        quality_writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=1))
    assert not (tmp_path / "quality" / "manifest.json").exists()

    time_writer = _writer(tmp_path, name="invalid-finished-time")
    _append_order(time_writer)
    with pytest.raises(ExecutionFactsValidationError, match="不得早于"):
        time_writer.finalize(finished_at=OCCURRED_AT - timedelta(seconds=1))
    time_writer.abort()

    recheck_writer = _writer(tmp_path, name="recheck")
    _append_order(recheck_writer)
    _complete_order_for_publish(recheck_writer)

    def fail_recheck(
        output_dir: Any,
        *,
        expected_run_id: Any = None,
        directory_descriptor: Any = None,
    ) -> Dict[str, Any]:
        """模拟 manifest 原子可见后的复验失败。

        Args:
            output_dir: 已发布目录。
            expected_run_id: 期望运行身份。

        Returns:
            Dict[str, Any]: 此故障函数不会返回。

        Raises:
            ExecutionFactsIntegrityError: 始终模拟复验失败。
        """

        del output_dir, expected_run_id, directory_descriptor
        raise ExecutionFactsIntegrityError("post publish recheck failed")

    monkeypatch.setattr(facts_module, "_validate_published_artifacts", fail_recheck)
    with pytest.raises(ExecutionFactsIntegrityError, match="post publish"):
        recheck_writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=1))
    assert (tmp_path / "recheck" / "facts.ndjson").exists()
    assert not (tmp_path / "recheck" / "manifest.json").exists()


def test_manifest_validation_rejects_schema_identity_summary_and_quality_tampering(
    tmp_path: Path,
) -> None:
    """验证 manifest 各关键维度被篡改后 consumer 均 fail closed。

    Args:
        tmp_path: pytest 临时目录。
    """

    writer = _writer(tmp_path, name="base")
    _append_order(writer)
    _complete_order_for_publish(writer)
    writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=1))
    base_dir = tmp_path / "base"
    base_manifest = json.loads((base_dir / "manifest.json").read_text(encoding="utf-8"))

    mutations = [
        lambda value: value.update({"schema_version": "execution-facts/v2"}),
        lambda value: value.update({"unknown": True}),
        lambda value: value["producer"].update({"name": "other"}),
        lambda value: value["producer"].update({"version": "bad/path"}),
        lambda value: value.update({"calculation_version": "invalid"}),
        lambda value: value.update({"finished_at": "2026-07-14T18:00:00.000000Z"}),
        lambda value: value["price_basis"].update({"fq": "future"}),
        lambda value: value["facts"].update({"path": "other.ndjson"}),
        lambda value: value["facts"].update({"record_count": -1}),
        lambda value: value["facts"].update({"sha256": "bad"}),
        lambda value: value["facts"].update({"last_sequence": 2}),
        lambda value: value["quality"].update({"status": "FAILED"}),
        lambda value: value["quality"].update({"checks_version": "invalid"}),
        lambda value: value["quality"].update({"event_counts": {"future": 1}}),
        lambda value: value["quality"].update({"event_counts": {"order_intent": -1}}),
        lambda value: value["quality"]["legacy_reconciliation"].update({"status": "FAILED"}),
    ]
    for index, mutate in enumerate(mutations):
        case_dir = tmp_path / "manifest-cases" / str(index)
        shutil.copytree(base_dir, case_dir)
        manifest = json.loads(json.dumps(base_manifest))
        mutate(manifest)
        (case_dir / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False), encoding="utf-8"
        )
        with pytest.raises(ExecutionFactsError):
            validate_published_execution_facts(case_dir, expected_run_id=RUN_ID)


def test_complete_guard_is_mandatory_and_rejects_incomplete_or_partial_artifacts(
    tmp_path: Path,
) -> None:
    """consumer 必须要求绑定 manifest SHA 的 complete，并拒绝任何未完成制品。

    Args:
        tmp_path: pytest 临时输出目录。

    Side Effects:
        复制一份合法发布并分别删除、篡改或添加发布状态文件。
    """

    writer = _writer(tmp_path, name="guard-base")
    _append_order(writer)
    _complete_order_for_publish(writer)
    writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=1))
    base_dir = tmp_path / "guard-base"

    missing_complete = tmp_path / "guard-missing-complete"
    shutil.copytree(base_dir, missing_complete)
    (missing_complete / ".publish.complete").unlink()
    with pytest.raises(ExecutionFactsIntegrityError, match="缺少 complete"):
        validate_published_execution_facts(missing_complete, expected_run_id=RUN_ID)

    incomplete = tmp_path / "guard-incomplete"
    shutil.copytree(base_dir, incomplete)
    shutil.copyfile(incomplete / ".publish.complete", incomplete / ".publish.incomplete")
    with pytest.raises(ExecutionFactsIntegrityError, match="发布未完成"):
        validate_published_execution_facts(incomplete, expected_run_id=RUN_ID)

    facts_partial = tmp_path / "guard-facts-partial"
    shutil.copytree(base_dir, facts_partial)
    shutil.copyfile(facts_partial / "facts.ndjson", facts_partial / "facts.ndjson.partial")
    with pytest.raises(ExecutionFactsIntegrityError, match="发布未完成"):
        validate_published_execution_facts(facts_partial, expected_run_id=RUN_ID)

    manifest_partial = tmp_path / "guard-manifest-partial"
    shutil.copytree(base_dir, manifest_partial)
    shutil.copyfile(
        manifest_partial / "manifest.json",
        manifest_partial / "manifest.json.partial",
    )
    with pytest.raises(ExecutionFactsIntegrityError, match="发布未完成"):
        validate_published_execution_facts(manifest_partial, expected_run_id=RUN_ID)

    bad_sha = tmp_path / "guard-bad-sha"
    shutil.copytree(base_dir, bad_sha)
    marker_path = bad_sha / ".publish.complete"
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    marker["manifest_sha256"] = "0" * 64
    marker_path.write_text(json.dumps(marker, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(ExecutionFactsIntegrityError, match="SHA-256"):
        validate_published_execution_facts(bad_sha, expected_run_id=RUN_ID)

    reformatted_manifest = tmp_path / "guard-reformatted-manifest"
    shutil.copytree(base_dir, reformatted_manifest)
    reformatted_path = reformatted_manifest / "manifest.json"
    manifest_value = json.loads(reformatted_path.read_text(encoding="utf-8"))
    reformatted_path.write_text(
        json.dumps(manifest_value, ensure_ascii=False, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ExecutionFactsIntegrityError, match="SHA-256"):
        validate_published_execution_facts(reformatted_manifest, expected_run_id=RUN_ID)


def test_complete_guard_state_switch_never_returns_failure_with_consumable_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """complete durable 失败保留 incomplete；删除已完成后抛错则按成功收口。

    Args:
        tmp_path: pytest 临时输出目录。
        monkeypatch: pytest 故障注入工具。

    Side Effects:
        分别注入 complete 目录 fsync、删除前失败和删除后异常三个状态切换边界。
    """

    fsync_writer = _writer(tmp_path, name="complete-fsync-failure")
    _append_order(fsync_writer)
    _complete_order_for_publish(fsync_writer)
    original_directory_fsync = facts_module._fsync_directory_descriptor

    def fail_complete_directory_fsync(directory_descriptor: int) -> None:
        """complete 与 incomplete 同时可见时模拟目录 fsync 失败。

        Args:
            directory_descriptor: 当前固定发布目录句柄。

        Raises:
            OSError: 命中 complete guard durable 阶段时固定抛出。
        """

        root = tmp_path / "complete-fsync-failure"
        if (root / ".publish.complete").exists() and (root / ".publish.incomplete").exists():
            raise OSError("complete guard directory fsync failed")
        original_directory_fsync(directory_descriptor)

    monkeypatch.setattr(
        facts_module,
        "_fsync_directory_descriptor",
        fail_complete_directory_fsync,
    )
    with pytest.raises(OSError, match="complete guard directory fsync"):
        fsync_writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=1))
    fsync_dir = tmp_path / "complete-fsync-failure"
    assert (fsync_dir / ".publish.complete").is_file()
    assert (fsync_dir / ".publish.incomplete").is_file()
    with pytest.raises(ExecutionFactsIntegrityError, match="发布未完成"):
        validate_published_execution_facts(fsync_dir, expected_run_id=RUN_ID)
    monkeypatch.undo()

    unlink_writer = _writer(tmp_path, name="complete-unlink-failure")
    _append_order(unlink_writer)
    _complete_order_for_publish(unlink_writer)
    original_unlink = facts_module.os.unlink

    def fail_before_incomplete_unlink(path: Any, *args: Any, **kwargs: Any) -> None:
        """在 incomplete 删除发生前固定失败。

        Args:
            path: 待删除路径。
            *args: Path.unlink 的位置参数。
            **kwargs: Path.unlink 的关键字参数。

        Raises:
            OSError: 目标为 incomplete guard 时固定抛出。
        """

        if Path(path).name == ".publish.incomplete":
            raise OSError("incomplete guard unlink failed before removal")
        original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(facts_module.os, "unlink", fail_before_incomplete_unlink)
    with pytest.raises(OSError, match="before removal"):
        unlink_writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=1))
    unlink_dir = tmp_path / "complete-unlink-failure"
    assert (unlink_dir / ".publish.complete").is_file()
    assert (unlink_dir / ".publish.incomplete").is_file()
    with pytest.raises(ExecutionFactsIntegrityError, match="发布未完成"):
        validate_published_execution_facts(unlink_dir, expected_run_id=RUN_ID)
    monkeypatch.undo()

    committed_writer = _writer(tmp_path, name="complete-unlink-committed")
    _append_order(committed_writer)
    _complete_order_for_publish(committed_writer)

    def remove_incomplete_then_raise(path: Any, *args: Any, **kwargs: Any) -> None:
        """模拟 unlink 已删除 incomplete 但系统调用随后报告异常。

        Args:
            path: 待删除路径。
            *args: Path.unlink 的位置参数。
            **kwargs: Path.unlink 的关键字参数。

        Raises:
            OSError: incomplete 已实际删除后固定抛出。
        """

        original_unlink(path, *args, **kwargs)
        if Path(path).name == ".publish.incomplete":
            raise OSError("incomplete unlink reported after removal")

    monkeypatch.setattr(facts_module.os, "unlink", remove_incomplete_then_raise)
    manifest = committed_writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=1))
    committed_dir = tmp_path / "complete-unlink-committed"
    assert not (committed_dir / ".publish.incomplete").exists()
    assert validate_published_execution_facts(committed_dir, expected_run_id=RUN_ID) == manifest


def test_writer_constructor_state_and_context_manager_guards(tmp_path: Path) -> None:
    """验证 writer 构造元数据、关闭状态与上下文退出的防御分支。"""

    with pytest.raises(ExecutionFactsValidationError, match="buffer"):
        _writer(tmp_path, name="bad-buffer", buffer_size_bytes=0)
    bad_basis = dict(PRICE_BASIS)
    bad_basis["fq"] = "future"
    with pytest.raises(ExecutionFactsValidationError, match="fq"):
        _writer(tmp_path, name="bad-basis", price_basis=bad_basis)
    reversed_range = dict(PRICE_BASIS)
    reversed_range["business_date_start"] = "2026-07-16"
    with pytest.raises(ExecutionFactsValidationError, match="起止颠倒"):
        _writer(tmp_path, name="reversed-range", price_basis=reversed_range)
    with pytest.raises(ExecutionFactsSecurityError):
        _writer(tmp_path, name="secret-provider", producer_version="SECRET_CANARY_version")
    with pytest.raises(ExecutionFactsSecurityError, match="发布目录"):
        validate_published_execution_facts(tmp_path / "missing")

    writer = _writer(tmp_path, name="closed")
    writer.abort()
    writer.abort()
    with pytest.raises(ExecutionFactsError, match="关闭"):
        _append_order(writer)

    with _writer(tmp_path, name="context") as context_writer:
        _append_order(context_writer)
    assert (tmp_path / "context" / "facts.ndjson.partial").exists()
    assert not (tmp_path / "context" / "manifest.json").exists()


def test_existing_same_and_different_sha_are_non_destructive(tmp_path: Path) -> None:
    """验证相同发布只读返回冲突，篡改发布返回完整性失败且均不覆盖。"""

    writer = _writer(tmp_path)
    _append_order(writer)
    _complete_order_for_publish(writer)
    writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=1))
    output_dir = tmp_path / "execution-facts"
    facts_path = output_dir / "facts.ndjson"
    manifest_path = output_dir / "manifest.json"
    complete_path = output_dir / ".publish.complete"
    original_facts = facts_path.read_bytes()
    original_manifest = manifest_path.read_bytes()
    original_complete = complete_path.read_bytes()

    with pytest.raises(ExecutionFactsAlreadyPublishedError):
        _writer(tmp_path)
    assert facts_path.read_bytes() == original_facts
    assert manifest_path.read_bytes() == original_manifest
    assert complete_path.read_bytes() == original_complete

    with pytest.raises(ExecutionFactsConflictError, match="其他 run_id"):
        _writer(tmp_path, run_id="87654321-4321-5678-9234-567812345678")
    assert facts_path.read_bytes() == original_facts
    assert manifest_path.read_bytes() == original_manifest
    assert complete_path.read_bytes() == original_complete

    facts_path.write_bytes(original_facts + b"\n")
    tampered = facts_path.read_bytes()
    with pytest.raises(ExecutionFactsIntegrityError):
        _writer(tmp_path)
    assert facts_path.read_bytes() == tampered
    assert manifest_path.read_bytes() == original_manifest
    assert complete_path.read_bytes() == original_complete


def test_orphan_final_and_partial_files_block_new_writer(tmp_path: Path) -> None:
    """验证无 manifest 的既有 final 或损坏 partial 不会被覆盖。"""

    output_dir = tmp_path / "execution-facts"
    output_dir.mkdir()
    (output_dir / "facts.ndjson").write_text("orphan", encoding="utf-8")
    with pytest.raises(ExecutionFactsConflictError):
        _writer(tmp_path)

    (output_dir / "facts.ndjson").unlink()
    (output_dir / "facts.ndjson.partial").write_text("broken", encoding="utf-8")
    with pytest.raises(ExecutionFactsConflictError):
        _writer(tmp_path)


def test_disk_fsync_and_atomic_publish_failures_leave_no_success_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证磁盘写、fsync 和排他发布失败都不会生成成功 manifest。

    Args:
        tmp_path: pytest 临时目录。
        monkeypatch: pytest 故障注入工具。
    """

    flush_writer = _writer(
        tmp_path,
        name="flush-disk",
        buffer_size_bytes=4096,
    )
    _append_order(flush_writer)

    def partially_write_then_fill_disk(file_descriptor: int, data: bytes) -> None:
        """写入半行后模拟磁盘满，形成可识别的损坏 partial。

        Args:
            file_descriptor: 当前 partial 文件描述符。
            data: writer 准备 flush 的完整缓冲。

        Raises:
            OSError: 半行落盘后始终模拟磁盘满。
        """

        facts_module.os.write(file_descriptor, data[: len(data) // 2])
        raise OSError("disk full during flush")

    monkeypatch.setattr(facts_module, "_write_all", partially_write_then_fill_disk)
    with pytest.raises(OSError, match="disk full during flush"):
        flush_writer.flush(durable=True)
    flush_partial = tmp_path / "flush-disk" / "facts.ndjson.partial"
    assert flush_partial.is_file()
    assert flush_partial.stat().st_size > 0
    assert not (tmp_path / "flush-disk" / "facts.ndjson").exists()
    assert not (tmp_path / "flush-disk" / "manifest.json").exists()
    monkeypatch.undo()
    with pytest.raises(ExecutionFactsIntegrityError, match="NDJSON"):
        validate_facts_file(flush_partial, expected_run_id=RUN_ID)
    flush_writer.abort()

    disk_writer = _writer(tmp_path, name="disk")
    _append_order(disk_writer)
    _complete_order_for_publish(disk_writer)
    monkeypatch.setattr(
        facts_module,
        "_write_all",
        lambda file_descriptor, data: (_ for _ in ()).throw(OSError("disk full")),
    )
    with pytest.raises(OSError, match="disk full"):
        disk_writer.finalize()
    disk_directory = tmp_path / "disk"
    assert (disk_directory / "facts.ndjson").is_file()
    assert (disk_directory / "manifest.json.partial").is_file()
    assert not (disk_directory / "manifest.json").exists()
    monkeypatch.undo()
    assert (
        validate_facts_file(disk_directory / "facts.ndjson", expected_run_id=RUN_ID).record_count
        == 6
    )

    fsync_writer = _writer(tmp_path, name="fsync")
    _append_order(fsync_writer)
    _complete_order_for_publish(fsync_writer)
    monkeypatch.setattr(
        facts_module.os,
        "fsync",
        lambda file_descriptor: (_ for _ in ()).throw(OSError("fsync failed")),
    )
    with pytest.raises(OSError, match="fsync failed"):
        fsync_writer.finalize()
    fsync_partial = tmp_path / "fsync" / "facts.ndjson.partial"
    assert fsync_partial.is_file()
    assert not (tmp_path / "fsync" / "manifest.json").exists()
    monkeypatch.undo()
    assert validate_facts_file(fsync_partial, expected_run_id=RUN_ID).record_count == 6
    fsync_writer.abort()

    publish_writer = _writer(tmp_path, name="publish")
    _append_order(publish_writer)
    _complete_order_for_publish(publish_writer)
    original_link = facts_module.os.link

    def fail_facts_publish(source: Any, target: Any, **options: Any) -> None:
        """模拟同目录排他发布失败。

        Args:
            source: 原 partial 路径。
            target: 目标 final 路径。
            **options: ``os.link`` 的平台兼容选项。

        Raises:
            OSError: 始终模拟 rename/link 失败。
        """

        del source, target, options
        raise OSError("atomic rename failed")

    monkeypatch.setattr(facts_module.os, "link", fail_facts_publish)
    with pytest.raises(OSError, match="atomic rename failed"):
        publish_writer.finalize()
    assert (tmp_path / "publish" / "facts.ndjson.partial").exists()
    assert not (tmp_path / "publish" / "facts.ndjson").exists()
    assert not (tmp_path / "publish" / "manifest.json").exists()
    monkeypatch.setattr(facts_module.os, "link", original_link)
    assert (
        validate_facts_file(
            tmp_path / "publish" / "facts.ndjson.partial",
            expected_run_id=RUN_ID,
        ).record_count
        == 6
    )

    directory_writer = _writer(tmp_path, name="directory-fsync")
    _append_order(directory_writer)
    _complete_order_for_publish(directory_writer)
    original_directory_fsync = facts_module._fsync_directory_descriptor

    def fail_manifest_directory_fsync(directory_descriptor: int) -> None:
        """模拟 manifest 已链接后第二次目录 fsync 失败。

        Args:
            directory_descriptor: 待持久化的固定事实目录句柄。

        Raises:
            OSError: 第四次调用对应 manifest 删除 partial 后的目录持久化。
        """

        directory = tmp_path / "directory-fsync"
        if (directory / "manifest.json").exists() and not (
            directory / "manifest.json.partial"
        ).exists():
            raise OSError("manifest directory fsync failed")
        original_directory_fsync(directory_descriptor)

    monkeypatch.setattr(
        facts_module,
        "_fsync_directory_descriptor",
        fail_manifest_directory_fsync,
    )
    with pytest.raises(OSError, match="manifest directory"):
        directory_writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=1))
    directory = tmp_path / "directory-fsync"
    assert (directory / "facts.ndjson").exists()
    assert (directory / "manifest.json.partial").exists()
    assert not (directory / "manifest.json").exists()
    assert validate_facts_file(directory / "facts.ndjson", expected_run_id=RUN_ID).record_count == 6
    assert (
        json.loads((directory / "manifest.json.partial").read_text(encoding="utf-8"))["run_id"]
        == RUN_ID
    )


def test_manifest_directory_fsync_and_rollback_link_double_failure_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """manifest 目录持久化与回滚 hardlink 同时失败时仍不可被 consumer 接受。

    Args:
        tmp_path: pytest 临时输出目录。
        monkeypatch: pytest 故障注入工具。

    Side Effects:
        只在临时目录注入 manifest 发布双重故障，并验证后续 writer 不覆盖现场。
    """

    writer = _writer(tmp_path, name="manifest-double-failure")
    _append_order(writer)
    _complete_order_for_publish(writer)
    output_dir = tmp_path / "manifest-double-failure"
    original_directory_fsync = facts_module._fsync_directory_descriptor
    original_link = facts_module.os.link
    rollback_link_failures = {"count": 0}

    def fail_after_manifest_staging_guard_is_removed(directory_descriptor: int) -> None:
        """在 manifest final 已可见且 partial 已删除后模拟目录 fsync 失败。

        Args:
            directory_descriptor: 当前机器事实固定输出目录句柄。

        Raises:
            OSError: 命中 manifest 无 partial 的危险窗口时固定抛出。
        """

        manifest_path = output_dir / "manifest.json"
        partial_path = output_dir / "manifest.json.partial"
        if manifest_path.exists() and not partial_path.exists():
            raise OSError("manifest directory fsync failed after staging removal")
        original_directory_fsync(directory_descriptor)

    def fail_manifest_staging_restore_link(
        source: Any,
        target: Any,
        **options: Any,
    ) -> None:
        """只让 manifest final 恢复为 partial 的回滚 hardlink 失败。

        Args:
            source: hardlink 源路径。
            target: hardlink 目标路径。
            **options: os.link 的跨平台兼容参数。

        Raises:
            OSError: 命中 manifest 回滚 hardlink 时固定抛出。
        """

        if Path(source).name == "manifest.json" and Path(target).name == "manifest.json.partial":
            rollback_link_failures["count"] += 1
            raise OSError("manifest rollback hardlink failed")
        original_link(source, target, **options)

    monkeypatch.setattr(
        facts_module,
        "_fsync_directory_descriptor",
        fail_after_manifest_staging_guard_is_removed,
    )
    monkeypatch.setattr(facts_module.os, "link", fail_manifest_staging_restore_link)
    with pytest.raises(OSError, match="manifest directory fsync"):
        writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=1))
    assert rollback_link_failures["count"] == 1
    assert (output_dir / "manifest.json").is_file()

    with pytest.raises(ExecutionFactsError, match="发布未完成"):
        validate_published_execution_facts(output_dir, expected_run_id=RUN_ID)
    assert (output_dir / ".publish.incomplete").is_file()
    assert not (output_dir / ".publish.complete").exists()

    facts_before_retry = (output_dir / "facts.ndjson").read_bytes()
    manifest_before_retry = (output_dir / "manifest.json").read_bytes()
    marker_before_retry = (output_dir / ".publish.incomplete").read_bytes()
    monkeypatch.undo()
    with pytest.raises(ExecutionFactsError):
        _writer(tmp_path, name="manifest-double-failure")
    assert (output_dir / "facts.ndjson").read_bytes() == facts_before_retry
    assert (output_dir / "manifest.json").read_bytes() == manifest_before_retry
    assert (output_dir / ".publish.incomplete").read_bytes() == marker_before_retry


def test_symlink_paths_are_rejected(tmp_path: Path) -> None:
    """验证输出目录与待校验文件的 symlink 均被拒绝。"""

    actual = tmp_path / "actual"
    actual.mkdir()
    linked = tmp_path / "execution-facts"
    linked.symlink_to(actual, target_is_directory=True)
    with pytest.raises(ExecutionFactsSecurityError):
        _writer(tmp_path)

    target = tmp_path / "target.ndjson"
    _write_ndjson(target, _raw_fact())
    link = tmp_path / "linked.ndjson"
    link.symlink_to(target)
    with pytest.raises(ExecutionFactsSecurityError):
        validate_facts_file(link, expected_run_id=RUN_ID)


def test_secret_canary_is_redacted_from_all_artifacts(tmp_path: Path) -> None:
    """验证备注中的 secret canary 只以脱敏形式持久化。"""

    canary = "BT_SECRET_CANARY_never_persist_this_value"
    writer = _writer(tmp_path)
    _append_order(writer, note=f"diagnostic {canary}")
    _complete_order_for_publish(writer)
    writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=1))

    output_dir = tmp_path / "execution-facts"
    artifact = (output_dir / "facts.ndjson").read_text(encoding="utf-8") + (
        output_dir / "manifest.json"
    ).read_text(encoding="utf-8")
    assert canary not in artifact
    assert "[REDACTED]" in artifact


def test_boundary_has_no_quant_sqlalchemy_web_qmt_or_secret_dependencies() -> None:
    """验证 writer 基础层只依赖标准库和 BulletTrade 版本元数据。"""

    source = Path(facts_module.__file__).read_text(encoding="utf-8").lower()
    forbidden = (
        "bullet_quant",
        "sqlalchemy",
        "fastapi",
        "django",
        "flask",
        "xtquant",
        "qmt_broker",
        "password=",
        "token=",
    )
    assert all(term not in source for term in forbidden)


def test_legacy_trade_csv_cannot_represent_complete_machine_contract(tmp_path: Path) -> None:
    """固定 legacy trades.csv 丢失生命周期、reservation、滑点与稳定身份的合同缺口。"""

    output = tmp_path / "trades.csv"
    export_trades(
        {
            "trades": [
                {
                    "time": OCCURRED_AT,
                    "security": "511880.XSHG",
                    "amount": 100,
                    "price": 100.071,
                    "commission": 0,
                    "tax": 0,
                    "direction": "买入",
                }
            ]
        },
        str(output),
    )
    columns = set(pd.read_csv(output, encoding="utf-8-sig").columns)
    machine_only = {
        "source_event_id",
        "sequence",
        "order_id",
        "before_status",
        "after_status",
        "reservation_id",
        "reservation_action",
        "slippage",
        "transfer_fee",
        "order_flow",
        "cancel_flow",
    }
    assert columns.isdisjoint(machine_only)
    assert {"手续费", "印花税", "总费用"}.issubset(columns)
