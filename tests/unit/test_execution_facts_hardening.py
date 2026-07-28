"""
BulletTrade 原生 execution-facts/v1 发布阻断项的聚焦红绿测试。

作者: BruceLee
文件职责: 固定 QualityReport、封闭 payload、价格策略、脱敏、输入预算和路径安全合同。
主要输入: 临时事实目录、确定性订单链、每日快照、价格口径和故障注入。
主要输出: 对 complete 发布门、canonical 身份、金额关系和跨平台失败关闭的断言。
上下游关系: 上游是更新后的 OpenSpec，下游是未来回测引擎 sink 与 Quant 原生 consumer。
关键约定: 不连接数据库、Web、QMT 或网络；所有成功发布均来自真实内部 validator；
可信根和父目录由单一可信 writer 独占，同权限写者在线性化点后的修改按 capability 合同处理。
"""

import json
import shutil
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

import bullet_trade.core.execution_facts as facts_module
from bullet_trade.core.execution_facts import (
    EXECUTION_FACTS_V1_COMPATIBILITY,
    EXECUTION_FACTS_V1_JSON_SCHEMA,
    MAX_FACT_LINE_BYTES,
    MAX_MANIFEST_FILE_BYTES,
    MAX_WRITER_BUFFER_BYTES,
    EventType,
    ExecutionFactsIntegrityError,
    ExecutionFactsSecurityError,
    ExecutionFactsValidationError,
    ExecutionFactsWriter,
    decimal_to_text,
    derive_pre_factor_ref_date,
    redact_sensitive_text,
    validate_canonical_security_code,
    validate_execution_facts_quality,
    validate_facts_file,
    validate_published_execution_facts,
)
from bullet_trade.core.models import OrderStatus, OrderStyle
from bullet_trade.core.price_basis import EffectivePriceBasis

pytestmark = pytest.mark.unit

RUN_ID = "12345678-1234-5678-9234-567812345678"
DAY_ONE = datetime(2026, 7, 15, 2, 0, tzinfo=timezone.utc)
DAY_TWO = DAY_ONE + timedelta(days=1)


def _policy(
    *,
    use_real_price: bool = False,
    reference_policy: str = "not_applicable",
    configured_ref_date: Optional[str] = None,
    start: str = "2026-07-15",
    end: str = "2026-07-15",
    provider: str = "unit-provider",
) -> Dict[str, Any]:
    """构造固定 8 字段运行级价格策略。

    Args:
        use_real_price: 是否启用动态前复权。
        reference_policy: 三种冻结参考日策略之一。
        configured_ref_date: 显式参考日。
        start: 首业务日。
        end: 末业务日。
        provider: 脱敏 provider 身份。

    Returns:
        Dict[str, Any]: writer 可接收的价格策略。
    """

    return {
        "use_real_price": use_real_price,
        "fq": "pre" if use_real_price else "none",
        "provider": provider,
        "business_timezone": "Asia/Shanghai",
        "reference_policy": reference_policy,
        "configured_ref_date": configured_ref_date,
        "business_date_start": start,
        "business_date_end": end,
    }


def _writer(
    root: Path,
    *,
    name: str = "execution-facts",
    price_basis: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> ExecutionFactsWriter:
    """创建绑定临时可信根的 writer。

    Args:
        root: pytest 临时根目录。
        name: 输出子目录。
        price_basis: 可选运行级价格策略。
        **kwargs: 覆盖构造参数。

    Returns:
        ExecutionFactsWriter: 尚未写事实的实例。
    """

    options: Dict[str, Any] = {
        "output_dir": root / name,
        "trusted_output_root": root,
        "run_id": RUN_ID,
        "producer_version": "0.9.2",
        "calculation_version": "engine-ledger/v1",
        "price_basis": price_basis or _policy(),
        "started_at": DAY_ONE,
        "buffer_size_bytes": 256,
    }
    options.update(kwargs)
    return ExecutionFactsWriter(**options)


def _basis(
    moment: datetime, *, use_real_price: bool = False, provider: str = "unit-provider"
) -> EffectivePriceBasis:
    """创建与测试策略同日的不可变价格口径。

    Args:
        moment: 当前业务日内的 aware 时间。
        use_real_price: 是否动态前复权。
        provider: provider 身份。

    Returns:
        EffectivePriceBasis: 已规范且不读取未来的价格实例。
    """

    return EffectivePriceBasis.create(
        use_real_price=use_real_price,
        provider=provider,
        business_time=moment,
        pre_factor_ref_date="2099-01-01" if use_real_price else None,
    )


def _append_empty_day(
    writer: ExecutionFactsWriter,
    moment: datetime,
    *,
    cash: Decimal = Decimal("100000"),
    available_cash: Optional[Decimal] = None,
    locked_cash: Decimal = Decimal("0"),
    positions_value: Decimal = Decimal("0"),
    total_value: Optional[Decimal] = None,
    include_account: bool = True,
    include_performance: bool = True,
    reconcile_status: str = "PASSED",
    use_real_price: bool = False,
) -> None:
    """追加一个无交易但可审计的账户日。

    Args:
        writer: 当前机器事实 writer。
        moment: 当日事实时间。
        cash: 日末现金。
        available_cash: 可用现金；默认扣除锁定现金。
        locked_cash: 锁定现金。
        positions_value: 持仓市值。
        total_value: 总资产；默认现金加持仓。
        include_account: 是否写 account_daily。
        include_performance: 是否写 daily_performance。
        reconcile_status: 对账状态。
        use_real_price: 当日 EffectivePriceBasis 开关。

    Returns:
        None: 事实已追加。
    """

    effective_available = cash - locked_cash if available_cash is None else available_cash
    effective_total = cash + positions_value if total_value is None else total_value
    writer.observe_effective_price_basis(
        _basis(moment, use_real_price=use_real_price, provider=writer.price_basis["provider"])
    )
    if include_account:
        writer.append(
            EventType.ACCOUNT_DAILY,
            authority_id="account-1-{0}".format(moment.date()),
            state_version=1,
            occurred_at=moment,
            payload={
                "account_id": "account-1",
                "cash": cash,
                "available_cash": effective_available,
                "locked_cash": locked_cash,
                "positions_value": positions_value,
                "total_value": effective_total,
            },
        )
    if include_performance:
        writer.append(
            EventType.DAILY_PERFORMANCE,
            authority_id="performance-1-{0}".format(moment.date()),
            state_version=1,
            occurred_at=moment,
            payload={
                "account_id": "account-1",
                "total_value": effective_total,
                "net_asset_value": Decimal("1"),
                "daily_return": Decimal("0"),
                "cumulative_return": Decimal("0"),
                "strategy_return": Decimal("0"),
                "drawdown": Decimal("0"),
            },
        )
    writer.append(
        EventType.RECONCILE_EVENT,
        authority_id="reconcile-1-{0}".format(moment.date()),
        state_version=1,
        occurred_at=moment,
        payload={
            "metric_name": "total-assets",
            "status": reconcile_status,
            "expected": effective_total,
            "actual": effective_total,
            "difference": Decimal("0"),
        },
    )


def _append_filled_order_day(writer: ExecutionFactsWriter) -> None:
    """追加订单、成交、费用、现金和日终快照完整链。

    Args:
        writer: 当前机器事实 writer。

    Returns:
        None: 完整链已追加。
    """

    writer.observe_effective_price_basis(_basis(DAY_ONE))
    writer.append(
        EventType.ORDER_INTENT,
        authority_id="order-1",
        state_version=1,
        occurred_at=DAY_ONE,
        payload={
            "order_id": "order-1",
            "account_id": "account-1",
            "security": "000001.SH",
            "side": "BUY",
            "requested_quantity": Decimal("100"),
            "order_type": OrderStyle.market,
        },
    )
    writer.append(
        EventType.ORDER_EVENT,
        authority_id="order-1",
        state_version=2,
        occurred_at=DAY_ONE,
        payload={
            "order_id": "order-1",
            "account_id": "account-1",
            "before_status": OrderStatus.open,
            "after_status": OrderStatus.filled,
            "requested_quantity": Decimal("100"),
            "filled_quantity": Decimal("100"),
            "remaining_quantity": Decimal("0"),
        },
    )
    writer.append(
        EventType.RESERVATION,
        authority_id="reservation-1",
        state_version=1,
        occurred_at=DAY_ONE,
        payload={
            "reservation_id": "reservation-1",
            "order_id": "order-1",
            "account_id": "account-1",
            "resource_type": "cash",
            "action": "consumed",
            "value": Decimal("1001"),
            "unit": "CNY",
        },
    )
    writer.append(
        EventType.FILL,
        authority_id="fill-1",
        state_version=1,
        occurred_at=DAY_ONE,
        payload={
            "order_id": "order-1",
            "fill_id": "fill-1",
            "account_id": "account-1",
            "security": "000001.XSHG",
            "side": "BUY",
            "quantity": Decimal("100"),
            "price": Decimal("10"),
            "amount": Decimal("1000"),
        },
    )
    writer.append(
        EventType.FEE,
        authority_id="fee-1",
        state_version=1,
        occurred_at=DAY_ONE,
        payload={
            "order_id": "order-1",
            "fill_id": "fill-1",
            "fee_type": "commission",
            "amount": Decimal("1"),
            "source": "cost-model",
        },
    )
    writer.append(
        EventType.CASH_LEDGER,
        authority_id="cash-principal",
        state_version=1,
        occurred_at=DAY_ONE,
        payload={
            "entry_id": "cash-principal",
            "account_id": "account-1",
            "category": "principal",
            "amount": Decimal("-1000"),
            "balance": Decimal("9000"),
            "source": "fill",
            "order_id": "order-1",
            "fill_id": "fill-1",
        },
    )
    writer.append(
        EventType.CASH_LEDGER,
        authority_id="cash-fee",
        state_version=1,
        occurred_at=DAY_ONE,
        payload={
            "entry_id": "cash-fee",
            "account_id": "account-1",
            "category": "fee",
            "amount": Decimal("-1"),
            "balance": Decimal("8999"),
            "source": "fee",
            "order_id": "order-1",
            "fill_id": "fill-1",
            "fee_type": "commission",
        },
    )
    writer.append(
        EventType.POSITION_DAILY,
        authority_id="position-1",
        state_version=1,
        occurred_at=DAY_ONE,
        payload={
            "account_id": "account-1",
            "security": "000001.XSHG",
            "quantity": Decimal("100"),
            "available_quantity": Decimal("100"),
            "average_cost": Decimal("10.01"),
            "price": Decimal("10"),
            "market_value": Decimal("1000"),
        },
    )
    _append_empty_day(
        writer,
        DAY_ONE,
        cash=Decimal("8999"),
        positions_value=Decimal("1000"),
        total_value=Decimal("9999"),
    )


def test_quality_report_exclusively_controls_complete(tmp_path: Path) -> None:
    """验证 complete 只由内部质量报告控制，调用方不能伪造 PASSED。

    Args:
        tmp_path: pytest 提供的临时可信根目录。

    Returns:
        None: 零事实、伪造状态和缺每日绩效三条路径均被阻断。
    """

    zero = _writer(tmp_path, name="zero")
    with pytest.raises(ExecutionFactsIntegrityError, match="零事实"):
        zero.finalize(finished_at=DAY_ONE + timedelta(seconds=1))
    assert not (tmp_path / "zero" / ".publish.complete").exists()

    forged = _writer(tmp_path, name="forged")
    with pytest.raises(TypeError):
        forged.finalize(quality_status="PASSED")  # type: ignore[call-arg]
    forged.abort()

    missing = _writer(tmp_path, name="missing")
    _append_empty_day(missing, DAY_ONE, include_performance=False)
    with pytest.raises(ExecutionFactsIntegrityError, match="daily_performance"):
        missing.finalize(finished_at=DAY_ONE + timedelta(seconds=1))
    assert not (tmp_path / "missing" / ".publish.complete").exists()


def test_daily_equations_and_failed_reconcile_block_complete(tmp_path: Path) -> None:
    """验证现金、资产恒等式和 reconcile FAILED 任一错误均失败关闭。

    Args:
        tmp_path: pytest 提供的临时可信根目录。

    Returns:
        None: 三类业务质量错误均未产生 complete guard。
    """

    cash = _writer(tmp_path, name="cash")
    _append_empty_day(cash, DAY_ONE, available_cash=Decimal("99999"))
    with pytest.raises(ExecutionFactsIntegrityError, match="available_cash"):
        cash.finalize(finished_at=DAY_ONE + timedelta(seconds=1))

    assets = _writer(tmp_path, name="assets")
    _append_empty_day(assets, DAY_ONE, total_value=Decimal("999999"))
    with pytest.raises(ExecutionFactsIntegrityError, match="总资产"):
        assets.finalize(finished_at=DAY_ONE + timedelta(seconds=1))

    reconcile = _writer(tmp_path, name="reconcile")
    _append_empty_day(reconcile, DAY_ONE, reconcile_status="FAILED")
    with pytest.raises(ExecutionFactsIntegrityError, match="reconcile_event"):
        reconcile.finalize(finished_at=DAY_ONE + timedelta(seconds=1))


def test_complete_order_fee_cash_and_position_chain_publishes(tmp_path: Path) -> None:
    """验证完整买单链通过内部质量报告并可被 consumer 重新复验。

    Args:
        tmp_path: pytest 提供的临时可信根目录。

    Returns:
        None: 发布 manifest、审计字段和 consumer 复验均一致。
    """

    writer = _writer(tmp_path)
    _append_filled_order_day(writer)
    manifest = writer.finalize(finished_at=DAY_ONE + timedelta(seconds=1))

    assert manifest["quality"]["status"] == "PASSED"
    assert manifest["quality"]["legacy_reconciliation"]["status"] == "PASSED"
    assert manifest["quality"]["audit"]["account_day_count"] == 1
    assert validate_published_execution_facts(tmp_path / "execution-facts") == manifest


@pytest.mark.parametrize("missing_kind", ["fee", "principal", "fee_ledger"])
def test_incomplete_order_fill_fee_cash_relationship_is_rejected(
    tmp_path: Path, missing_kind: str
) -> None:
    """删除成交费用或现金关系时发布必须失败。

    Args:
        tmp_path: pytest 临时根目录。
        missing_kind: 要从 canonical facts 中删除的关系种类。

    Returns:
        None: 被删关系均由质量校验识别并阻断。
    """

    writer = _writer(tmp_path, name=missing_kind)
    _append_filled_order_day(writer)
    writer.flush(durable=True)
    writer._close_descriptor()
    partial = tmp_path / missing_kind / "facts.ndjson.partial"
    rows = [json.loads(line) for line in partial.read_text(encoding="utf-8").splitlines()]
    if missing_kind == "fee":
        rows = [row for row in rows if row["event_type"] != "fee"]
    elif missing_kind == "principal":
        rows = [
            row
            for row in rows
            if not (
                row["event_type"] == "cash_ledger" and row["payload"]["category"] == "principal"
            )
        ]
    else:
        rows = [
            row
            for row in rows
            if not (row["event_type"] == "cash_ledger" and row["payload"]["category"] == "fee")
        ]
    for sequence, row in enumerate(rows, start=1):
        row["sequence"] = sequence
    partial.write_text(
        "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )
    summary = validate_facts_file(partial, expected_run_id=RUN_ID)
    with pytest.raises(ExecutionFactsIntegrityError):
        validate_execution_facts_quality(partial, summary=summary, expected_run_id=RUN_ID)


def test_payload_whitelist_canonical_enums_and_reason_are_closed(tmp_path: Path) -> None:
    """验证未知字段、非 canonical 枚举和空终态 reason 均被拒绝。

    Args:
        tmp_path: pytest 提供的临时可信根目录。

    Returns:
        None: 运行时校验与导出 JSON Schema 均保持封闭合同。
    """

    assert EXECUTION_FACTS_V1_COMPATIBILITY["unknown_payload_field"] == "reject"
    assert facts_module.V1_ORDER_STATUS_VALUES == {item.value for item in OrderStatus}
    assert facts_module.V1_ORDER_TYPE_VALUES == {item.value for item in OrderStyle}
    writer = _writer(tmp_path)
    base = {
        "order_id": "order-1",
        "security": "000001.XSHG",
        "side": "BUY",
        "requested_quantity": Decimal("100"),
        "order_type": "market",
    }
    for bad_type in ("MARKET", "submitted", 23):
        payload = dict(base, order_type=bad_type)
        with pytest.raises(ExecutionFactsValidationError):
            writer.append(
                EventType.ORDER_INTENT,
                authority_id="bad-type",
                state_version=1,
                occurred_at=DAY_ONE,
                payload=payload,
            )
    with pytest.raises(ExecutionFactsValidationError, match="未知"):
        writer.append(
            EventType.ORDER_INTENT,
            authority_id="unknown-field",
            state_version=1,
            occurred_at=DAY_ONE,
            payload=dict(base, future_field="no"),
        )
    for reason in (None, "", "   "):
        payload = {
            "order_id": "order-1",
            "before_status": "open",
            "after_status": "rejected",
            "requested_quantity": Decimal("100"),
            "filled_quantity": Decimal("0"),
            "remaining_quantity": Decimal("100"),
        }
        if reason is not None:
            payload["reason"] = reason
        with pytest.raises(ExecutionFactsValidationError):
            writer.append(
                EventType.ORDER_EVENT,
                authority_id="bad-reason",
                state_version=1,
                occurred_at=DAY_ONE,
                payload=payload,
            )
    writer.abort()
    order_event_schema = next(
        item
        for item in EXECUTION_FACTS_V1_JSON_SCHEMA["oneOf"]
        if item["properties"]["event_type"]["const"] == "order_event"
    )["properties"]["payload"]
    assert order_event_schema["additionalProperties"] is False
    assert order_event_schema["allOf"][0]["then"]["required"] == ["reason"]


def test_security_aliases_collapse_to_one_identity_and_options_are_supported() -> None:
    """验证市场别名收敛为 JQ canonical，期权连字符不分裂身份。

    Returns:
        None: 冻结别名映射唯一，非冻结或畸形代码均被拒绝。
    """

    groups = {
        "000001.XSHG": ("000001.SH", "000001.XSHG"),
        "000001.XSHE": ("000001.SZ", "000001.XSHE"),
        "430001.BSE": ("430001.BJ", "430001.BSE"),
        "RB2410.XSGE": ("RB2410.SHFE", "RB2410.XSHF", "RB2410.XSGE"),
        "IO2407-C-3500.CCFX": ("IO2407-C-3500.CFFEX", "IO2407-C-3500.CCFX"),
        "M2409-C-2500.XDCE": ("M2409-C-2500.DCE", "M2409-C-2500.XDCE"),
        "SR409C6000.XZCE": ("SR409C6000.CZCE", "SR409C6000.XZCE"),
        "SC2408.XINE": ("SC2408.INE", "SC2408.XINE"),
    }
    for expected, aliases in groups.items():
        assert {validate_canonical_security_code(alias) for alias in aliases} == {expected}
    for invalid in ("000001", "000001.SSE", "-M2409.XDCE", "M2409-.XDCE", "A..XDCE"):
        with pytest.raises(ExecutionFactsValidationError):
            validate_canonical_security_code(invalid)


def test_multiday_price_policy_is_derived_and_provider_drift_fails(tmp_path: Path) -> None:
    """验证多日动态前复权逐日推导且 provider 漂移失败关闭。

    Args:
        tmp_path: pytest 提供的临时可信根目录。

    Returns:
        None: 三种策略与逐日证明一致，漂移运行不能发布。
    """

    current_policy = _policy(
        use_real_price=True,
        reference_policy="current_trade_date",
        start="2026-07-15",
        end="2026-07-16",
    )
    assert derive_pre_factor_ref_date(current_policy, "2026-07-15") == "2026-07-15"
    assert derive_pre_factor_ref_date(current_policy, "2026-07-16") == "2026-07-16"
    current = _writer(tmp_path, name="current", price_basis=current_policy)
    _append_empty_day(current, DAY_ONE, use_real_price=True)
    _append_empty_day(current, DAY_TWO, use_real_price=True)
    manifest = current.finalize(finished_at=DAY_TWO + timedelta(seconds=1))
    assert manifest["price_basis"] == current_policy

    configured_policy = _policy(
        use_real_price=True,
        reference_policy="min_configured_and_current_trade_date",
        configured_ref_date="2099-01-01",
        start="2026-07-15",
        end="2026-07-16",
    )
    assert derive_pre_factor_ref_date(configured_policy, "2026-07-15") == "2026-07-15"
    assert derive_pre_factor_ref_date(configured_policy, "2026-07-16") == "2026-07-16"
    configured = _writer(tmp_path, name="configured", price_basis=configured_policy)
    _append_empty_day(configured, DAY_ONE, use_real_price=True)
    _append_empty_day(configured, DAY_TWO, use_real_price=True)
    configured_manifest = configured.finalize(finished_at=DAY_TWO + timedelta(seconds=1))
    assert configured_manifest["price_basis"] == configured_policy

    no_action_policy = _policy(start="2026-07-15", end="2026-07-16")
    no_action = _writer(tmp_path, name="no-action", price_basis=no_action_policy)
    _append_empty_day(no_action, DAY_ONE)
    _append_empty_day(no_action, DAY_TWO)
    no_action_manifest = no_action.finalize(finished_at=DAY_TWO + timedelta(seconds=1))
    assert no_action_manifest["price_basis"]["reference_policy"] == "not_applicable"

    drift = _writer(tmp_path, name="drift")
    with pytest.raises(ExecutionFactsIntegrityError, match="漂移"):
        drift.observe_effective_price_basis(_basis(DAY_ONE, provider="other-provider"))
    _append_empty_day(drift, DAY_ONE)
    with pytest.raises(ExecutionFactsIntegrityError, match="曾发生漂移"):
        drift.finalize(finished_at=DAY_ONE + timedelta(seconds=1))


def test_secret_redactor_and_artifact_scan_cover_frozen_patterns(tmp_path: Path) -> None:
    """验证 Bearer、环境变量、DSN、API key 和异常文本统一脱敏。

    Args:
        tmp_path: pytest 提供的临时制品目录。

    Returns:
        None: 内存错误与跨边界长行扫描均未泄漏冻结 secret 模式。
    """

    secrets = (
        "Authorization: Bearer abc.def-123",
        'Authorization: Bearer "quoted-secret-123"',
        '{"Authorization":"Bearer json-header-secret-123"}',
        '{"token":"json-secret-123"}',
        "{'password': 'repr-secret-123'}",
        "BT_TOKEN='abc def suffix'",
        "JQDATA_PASSWORD=jq-secret",
        "mysql://user:pass@host/db",
        "redis://:pass@host/0",
        "rediss://:pass@host/0",
        "sk-abcdefgh",
    )
    for secret in secrets:
        redacted = redact_sensitive_text(ValueError(secret))
        assert secret not in redacted
        assert "secret-123" not in redacted
        assert "[REDACTED]" in redacted
    with pytest.raises(ExecutionFactsValidationError) as event_error:
        facts_module._coerce_event_type("Authorization: Bearer leaked-token")
    assert "leaked-token" not in str(event_error.value)

    artifact = tmp_path / "artifact.json"
    artifact.write_text(
        "x" * (64 * 1024 - len("mysql://user:") - 1)
        + " "
        + "mysql://user:"
        + "p" * 3000
        + "@host/db\n",
        encoding="utf-8",
    )
    with pytest.raises(ExecutionFactsSecurityError):
        facts_module._scan_artifact_for_secrets(
            artifact,
            maximum_bytes=MAX_MANIFEST_FILE_BYTES,
            maximum_line_bytes=MAX_MANIFEST_FILE_BYTES,
        )
    for index, persisted_secret in enumerate(
        (
            '{"token":"json-secret-123"}\n',
            "{'password': 'repr-secret-123'}\n",
            'Authorization: Bearer "quoted-secret-123"\n',
        )
    ):
        persisted = tmp_path / "persisted-secret-{0}.json".format(index)
        persisted.write_text(persisted_secret, encoding="utf-8")
        with pytest.raises(ExecutionFactsSecurityError, match="secret"):
            facts_module._scan_artifact_for_secrets(
                persisted,
                maximum_bytes=MAX_MANIFEST_FILE_BYTES,
                maximum_line_bytes=MAX_MANIFEST_FILE_BYTES,
            )


def test_decimal_and_json_budgets_fail_before_unbounded_expansion(tmp_path: Path) -> None:
    """验证 Decimal、JSON、单行和 writer 缓冲预算在扩张前失败关闭。

    Args:
        tmp_path: pytest 提供的临时可信根目录。

    Returns:
        None: 精确上限可通过，指数、精度、深度和容量越界均被拒绝。
    """

    exact = Decimal("12345678901234567890123456789012345678")
    original_precision = getcontext().prec
    try:
        for precision in (6, 28, 50):
            getcontext().prec = precision
            text = decimal_to_text(exact)
            assert Decimal(text) == exact
    finally:
        getcontext().prec = original_precision
    for value in (Decimal("1e1000000"), Decimal("0e1000000"), Decimal("1e-1000000")):
        with pytest.raises(ExecutionFactsValidationError):
            decimal_to_text(value)
    for value in (
        Decimal("123456789012345678901234567890123456789"),
        Decimal("0.0000000000000000001"),
    ):
        with pytest.raises(ExecutionFactsValidationError):
            decimal_to_text(value)
    decimal_schema = facts_module._field_json_schema("decimal")
    assert decimal_schema["maxLength"] == facts_module.MAX_DECIMAL_CANONICAL_CHARS
    with pytest.raises(ExecutionFactsIntegrityError):
        facts_module._loads_json_no_duplicates(b'{"a":1,"a":2}', "duplicate")
    with pytest.raises(ExecutionFactsIntegrityError):
        facts_module._loads_json_no_duplicates(b"[" * 2000 + b"0" + b"]" * 2000, "deep")

    long_line = tmp_path / "long.ndjson"
    long_line.write_bytes(b"x" * (MAX_FACT_LINE_BYTES + 1))
    with pytest.raises(ExecutionFactsIntegrityError, match="单行"):
        validate_facts_file(long_line)
    with pytest.raises(ExecutionFactsValidationError, match="buffer"):
        _writer(tmp_path, name="large-buffer", buffer_size_bytes=MAX_WRITER_BUFFER_BYTES + 1)

    low_precision = _writer(tmp_path, name="low-precision-quality")
    exact_cash = Decimal("12345678901234567890123456789012345678")
    original_precision = getcontext().prec
    try:
        getcontext().prec = 6
        _append_empty_day(
            low_precision,
            DAY_ONE,
            cash=exact_cash,
            available_cash=exact_cash,
            total_value=exact_cash,
        )
        manifest = low_precision.finalize(finished_at=DAY_ONE + timedelta(seconds=1))
    finally:
        getcontext().prec = original_precision
    assert manifest["quality"]["status"] == "PASSED"


def test_trusted_root_traversal_static_and_runtime_symlinks_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证可信根逃逸、祖先链接和运行中目录替换不能写入外部路径。

    Args:
        tmp_path: pytest 提供的临时可信根和攻击目录。
        monkeypatch: 在首次 openat 边界注入目录替换竞态。

    Returns:
        None: 静态与动态攻击均失败，可信根外保持零写入。
    """

    trusted = tmp_path / "trusted"
    trusted.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    with pytest.raises(ExecutionFactsSecurityError, match="穿越"):
        ExecutionFactsWriter(
            output_dir=trusted / "child" / ".." / ".." / "outside" / "facts",
            trusted_output_root=trusted,
            run_id=RUN_ID,
            producer_version="0.9.2",
            calculation_version="engine-ledger/v1",
            price_basis=_policy(),
            started_at=DAY_ONE,
        )

    linked_parent = trusted / "linked"
    inside = trusted / "inside"
    inside.mkdir()
    linked_parent.symlink_to(inside, target_is_directory=True)
    with pytest.raises(ExecutionFactsSecurityError):
        ExecutionFactsWriter(
            output_dir=linked_parent / "facts",
            trusted_output_root=trusted,
            run_id=RUN_ID,
            producer_version="0.9.2",
            calculation_version="engine-ledger/v1",
            price_basis=_policy(),
            started_at=DAY_ONE,
        )

    writer = _writer(trusted, name="runtime")
    original = trusted / "runtime-original"
    (trusted / "runtime").rename(original)
    (trusted / "runtime").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ExecutionFactsSecurityError, match="替换"):
        writer.append(
            EventType.RUN_METRIC,
            authority_id="metric",
            state_version=1,
            occurred_at=DAY_ONE,
            payload={"metric_name": "metric", "metric_value": Decimal("1")},
        )
    assert not (outside / "facts.ndjson.partial").exists()

    race_root = tmp_path / "race-root"
    race_root.mkdir()
    race_outside = tmp_path / "race-outside"
    race_outside.mkdir()
    original_open_at = facts_module._open_exclusive_at

    def swap_before_first_open(directory_descriptor: int, filename: str) -> int:
        """在首次 partial open 前替换请求路径，验证 dir_fd 不会跟随。

        Args:
            directory_descriptor: writer 已固定的原输出目录句柄。
            filename: 即将创建的 staging basename。

        Returns:
            int: 原目录内创建的文件 descriptor。

        Side Effects:
            把请求目录改名并用指向可信根外的 symlink 接管原路径。
        """

        requested = race_root / "execution-facts"
        requested.rename(race_root / "execution-facts-original")
        requested.symlink_to(race_outside, target_is_directory=True)
        return original_open_at(directory_descriptor, filename)

    monkeypatch.setattr(facts_module, "_open_exclusive_at", swap_before_first_open)
    with pytest.raises(ExecutionFactsSecurityError, match="替换"):
        _writer(race_root)
    assert not (race_outside / "facts.ndjson.partial").exists()
    assert not (race_root / "execution-facts-original" / "facts.ndjson.partial").exists()


def test_non_posix_directory_fsync_and_nas_hardlink_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """无目录 fsync 或排他 hardlink 能力时保留 partial 且无 complete。

    Args:
        tmp_path: pytest 临时根目录。
        monkeypatch: 平台与文件系统故障注入工具。

    Returns:
        None: 不支持的平台在写前失败，NAS hardlink 失败不生成 complete。
    """

    unsupported_output = tmp_path / "unsupported-platform"
    monkeypatch.setattr(facts_module.os, "name", "nt")
    with pytest.raises(ExecutionFactsSecurityError, match="fsync"):
        facts_module._fsync_directory(tmp_path)
    with pytest.raises(ExecutionFactsSecurityError, match="required dir_fd"):
        _writer(tmp_path, name="unsupported-platform")
    assert not unsupported_output.exists()
    monkeypatch.undo()

    writer = _writer(tmp_path, name="nas")
    _append_empty_day(writer, DAY_ONE)
    monkeypatch.setattr(
        facts_module.os,
        "link",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("hardlink unsupported")),
    )
    with pytest.raises(OSError, match="hardlink unsupported"):
        writer.finalize(finished_at=DAY_ONE + timedelta(seconds=1))
    assert not (tmp_path / "nas" / ".publish.complete").exists()


def test_writer_and_consumer_keep_one_directory_identity_during_races(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证线性化点之前切换目录或新增 incomplete 均失败关闭。

    Args:
        tmp_path: pytest 临时根目录。
        monkeypatch: 在指定系统调用边界注入目录竞态。

    Returns:
        None: 三种竞态都未产生可消费的错误目录。
    """

    writer = _writer(tmp_path, name="writer-race")
    _append_empty_day(writer, DAY_ONE)
    writer_directory = tmp_path / "writer-race"
    writer_original = tmp_path / "writer-race-original"
    writer_outside = tmp_path / "writer-race-outside"
    writer_outside.mkdir()
    original_publish_at = facts_module._publish_exclusive_at

    def swap_during_facts_publish(
        directory_descriptor: int,
        source_filename: str,
        target_filename: str,
    ) -> None:
        """在 facts hardlink 前切换请求路径，但仍调用固定 dir_fd 发布。

        Args:
            directory_descriptor: writer 固定的原目录句柄。
            source_filename: facts partial basename。
            target_filename: facts final basename。

        Returns:
            None: 原实现完成原目录内发布。

        Side Effects:
            首次 facts 发布时把请求路径替换成指向可信根外的 symlink。
        """

        if source_filename == "facts.ndjson.partial":
            writer_directory.rename(writer_original)
            writer_directory.symlink_to(writer_outside, target_is_directory=True)
        original_publish_at(directory_descriptor, source_filename, target_filename)

    monkeypatch.setattr(facts_module, "_publish_exclusive_at", swap_during_facts_publish)
    with pytest.raises(ExecutionFactsSecurityError, match="替换"):
        writer.finalize(finished_at=DAY_ONE + timedelta(seconds=1))
    assert (writer_original / "facts.ndjson").is_file()
    assert not (writer_original / ".publish.complete").exists()
    assert not (writer_outside / "facts.ndjson").exists()
    monkeypatch.undo()

    incomplete_writer = _writer(tmp_path, name="consumer-incomplete")
    _append_empty_day(incomplete_writer, DAY_ONE)
    incomplete_writer.finalize(finished_at=DAY_ONE + timedelta(seconds=1))
    original_read_at = facts_module._read_json_regular_file_with_sha_at
    injected = {"done": False}

    def inject_incomplete_during_read(
        directory_descriptor: int,
        filename: str,
    ) -> Any:
        """在 consumer 已完成首次 guard 检查后新增 incomplete。

        Args:
            directory_descriptor: consumer 固定的发布目录句柄。
            filename: 当前读取的 JSON basename。

        Returns:
            Any: 原 JSON reader 的 object 与 SHA 结果。

        Side Effects:
            首次读取 manifest 时创建 regular incomplete guard。
        """

        result = original_read_at(directory_descriptor, filename)
        if filename == "manifest.json" and not injected["done"]:
            injected["done"] = True
            descriptor = facts_module.os.open(
                ".publish.incomplete",
                facts_module.os.O_WRONLY | facts_module.os.O_CREAT | facts_module.os.O_EXCL,
                0o600,
                dir_fd=directory_descriptor,
            )
            try:
                facts_module.os.write(descriptor, b"{}\n")
            finally:
                facts_module.os.close(descriptor)
        return result

    monkeypatch.setattr(
        facts_module,
        "_read_json_regular_file_with_sha_at",
        inject_incomplete_during_read,
    )
    with pytest.raises(ExecutionFactsIntegrityError, match="发布未完成"):
        validate_published_execution_facts(tmp_path / "consumer-incomplete")
    monkeypatch.undo()

    switch_writer = _writer(tmp_path, name="consumer-switch")
    _append_empty_day(switch_writer, DAY_ONE)
    switch_writer.finalize(finished_at=DAY_ONE + timedelta(seconds=1))
    switch_directory = tmp_path / "consumer-switch"
    replacement = tmp_path / "consumer-switch-replacement"
    moved_original = tmp_path / "consumer-switch-original"
    shutil.copytree(switch_directory, replacement)
    switched = {"done": False}

    def switch_directory_during_read(
        directory_descriptor: int,
        filename: str,
    ) -> Any:
        """在 manifest 读取时用字节相同的另一目录替换请求路径。

        Args:
            directory_descriptor: consumer 固定的原目录句柄。
            filename: 当前 JSON basename。

        Returns:
            Any: 始终从原固定句柄读取的 object 与 SHA。

        Side Effects:
            首次 manifest 读取前完成 A/B 目录切换。
        """

        if filename == "manifest.json" and not switched["done"]:
            switched["done"] = True
            switch_directory.rename(moved_original)
            replacement.rename(switch_directory)
        return original_read_at(directory_descriptor, filename)

    monkeypatch.setattr(
        facts_module,
        "_read_json_regular_file_with_sha_at",
        switch_directory_during_read,
    )
    with pytest.raises(ExecutionFactsSecurityError, match="身份"):
        validate_published_execution_facts(switch_directory)


def test_terminal_capability_contract_does_not_claim_permanent_path_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证 writer/consumer 返回的是固定目录 capability 的线性化快照。

    Args:
        tmp_path: pytest 提供的可信根、迁移目录和未受信路径。
        monkeypatch: 在线性化点之后模拟合同明确排除的同权限命名空间修改。

    Returns:
        None: 固定 inode 上的包保持完整且根外零写入，后续 consumer 会识别新状态。

    Notes:
        该测试不是授权共享 trusted root；部署仍必须由单 writer 独占目录并串行发布。它只把
        portable POSIX 无法冻结 pathname 的边界写成可执行合同，防止以后在 complete 后增加
        可抛检查并制造“已经可消费但 finalize 返回失败”的矛盾。
    """

    output_directory = tmp_path / "writer-linearized"
    moved_directory = tmp_path / "writer-linearized-original"
    replacement_directory = tmp_path / "writer-linearized-replacement"
    replacement_directory.mkdir()
    writer = _writer(tmp_path, name=output_directory.name)
    _append_empty_day(writer, DAY_ONE)
    original_commit = facts_module._commit_publish_guard_at

    def move_public_path_after_complete(
        directory_descriptor: int,
        incomplete_filename: str,
        complete_filename: str,
    ) -> None:
        """在 complete 终态切换后替换公开 pathname。

        Args:
            directory_descriptor: writer 构造期固定的原目录 capability。
            incomplete_filename: incomplete guard 的冻结 basename。
            complete_filename: complete guard 的冻结 basename。

        Returns:
            None: 原目录完成提交后，公开 pathname 被同权限测试进程替换。

        Side Effects:
            把已提交目录改名，并在原位置创建指向空替代目录的 symlink。
        """

        original_commit(directory_descriptor, incomplete_filename, complete_filename)
        output_directory.rename(moved_directory)
        output_directory.symlink_to(replacement_directory, target_is_directory=True)

    monkeypatch.setattr(
        facts_module,
        "_commit_publish_guard_at",
        move_public_path_after_complete,
    )
    manifest = writer.finalize(finished_at=DAY_ONE + timedelta(seconds=1))
    assert validate_published_execution_facts(moved_directory) == manifest
    assert list(replacement_directory.iterdir()) == []
    with pytest.raises(ExecutionFactsSecurityError):
        validate_published_execution_facts(output_directory)
    monkeypatch.undo()

    consumer_directory = tmp_path / "consumer-linearized"
    consumer_writer = _writer(tmp_path, name=consumer_directory.name)
    _append_empty_day(consumer_writer, DAY_ONE)
    consumer_manifest = consumer_writer.finalize(finished_at=DAY_ONE + timedelta(seconds=1))
    original_reject = facts_module._reject_incomplete_publication_at
    reject_calls = {"count": 0}

    def insert_incomplete_after_final_check(directory_descriptor: int) -> None:
        """在 consumer 最后一次未完成检查的线性化点之后插入 guard。

        Args:
            directory_descriptor: consumer 固定的发布目录 capability。

        Returns:
            None: 第二次检查完成后，测试进程新增 incomplete guard。

        Side Effects:
            以同一目录 descriptor 排他创建新的 incomplete 文件。
        """

        original_reject(directory_descriptor)
        reject_calls["count"] += 1
        if reject_calls["count"] != 2:
            return
        descriptor = facts_module.os.open(
            ".publish.incomplete",
            facts_module.os.O_WRONLY | facts_module.os.O_CREAT | facts_module.os.O_EXCL,
            0o600,
            dir_fd=directory_descriptor,
        )
        facts_module.os.close(descriptor)

    monkeypatch.setattr(
        facts_module,
        "_reject_incomplete_publication_at",
        insert_incomplete_after_final_check,
    )
    assert validate_published_execution_facts(consumer_directory) == consumer_manifest
    monkeypatch.undo()
    with pytest.raises(ExecutionFactsIntegrityError, match="发布未完成"):
        validate_published_execution_facts(consumer_directory)
