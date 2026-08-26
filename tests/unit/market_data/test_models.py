"""
作者: BruceLee

文件职责: 验证订阅 selector、稳定指纹、逐项回执和 typed 事件不可变合同。
主要输入: 不同顺序的 MarketSubscriptionSpec、合成逐项状态和 MarketEvent。
主要输出: 指纹、整体状态、明细属性与校验错误断言。
上游关系: 覆盖 bullet_trade.market_data.models 的公共接口。
下游关系: 为远程协议和实时 Feed 序列化提供稳定模型回归门禁。
关键配置约定: 测试数据全部脱敏且不依赖时间、网络或厂商字段。
"""

import pytest

from bullet_trade.market_data import (
    MarketDataLevel,
    MarketEvent,
    MarketEventType,
    MarketSubscriptionReceipt,
    MarketSubscriptionSpec,
    SubscriptionItemResult,
    SubscriptionItemState,
    SubscriptionSelector,
    SubscriptionState,
)

pytestmark = pytest.mark.unit


def _symbol_spec(request_id: str = "request-1") -> MarketSubscriptionSpec:
    """
    构造一个双标的 L2 测试订阅。

    Args:
        request_id: 只用于请求关联、不参与语义指纹的 ID。

    Returns:
        MarketSubscriptionSpec: 规范化 symbols selector。
    """
    return MarketSubscriptionSpec(
        request_id=request_id,
        selector=SubscriptionSelector.SYMBOLS,
        symbols=("600000.XSHG", "000001.XSHE"),
        level=MarketDataLevel.L2,
        event_types=(MarketEventType.TRANSACTION, MarketEventType.SNAPSHOT_L2),
        asset_types=("stock",),
        require_continuity=True,
    )


def test_subscription_fingerprint_is_semantic_and_stable() -> None:
    """验证输入顺序和 request_id 不改变同一订阅语义指纹。"""
    first = _symbol_spec("request-a")
    second = MarketSubscriptionSpec(
        request_id="request-b",
        selector="symbols",
        symbols=("000001.XSHE", "600000.XSHG", "000001.XSHE"),
        level="l2",
        event_types=("snapshot_l2", "transaction"),
        asset_types=("stock",),
        require_continuity=True,
    )

    assert first.fingerprint == second.fingerprint
    assert first.symbols == ("000001.XSHE", "600000.XSHG")
    assert second.event_types == (
        MarketEventType.SNAPSHOT_L2,
        MarketEventType.TRANSACTION,
    )


def test_selector_and_event_wildcard_validation_fail_closed() -> None:
    """验证 selector 作用域互斥且 '*' 不能与显式事件混用。"""
    with pytest.raises(ValueError, match="selector=symbols"):
        MarketSubscriptionSpec(
            request_id="bad-scope",
            selector="symbols",
            level="l1",
            event_types=("snapshot_l1",),
        )
    with pytest.raises(ValueError, match="必须单独使用"):
        MarketSubscriptionSpec(
            request_id="bad-events",
            selector="all",
            level="l2",
            event_types=("*", "transaction"),
        )


def test_receipt_exposes_partial_details_and_event_is_immutable() -> None:
    """验证回执逐项 confirmed/rejected 明细及 typed payload 只读保护。"""
    spec = _symbol_spec()
    items = (
        SubscriptionItemResult(
            selector=spec.selector,
            scope="000001.XSHE",
            level=spec.level,
            event_type=MarketEventType.SNAPSHOT_L2,
            state=SubscriptionItemState.CONFIRMED,
        ),
        SubscriptionItemResult(
            selector=spec.selector,
            scope="000001.XSHE",
            level=spec.level,
            event_type=MarketEventType.TRANSACTION,
            state=SubscriptionItemState.REJECTED,
            code="CONTINUITY_UNAVAILABLE",
        ),
    )
    receipt = MarketSubscriptionReceipt.from_items(
        subscription_id="subscription-1",
        spec=spec,
        session_epoch="epoch-1",
        items=items,
        actual_event_types=(MarketEventType.SNAPSHOT_L2, MarketEventType.TRANSACTION),
        effective_symbols=("000001.XSHE",),
    )

    assert receipt.state is SubscriptionState.PARTIAL
    assert len(receipt.requested) == 2
    assert len(receipt.sent) == 1
    assert len(receipt.confirmed) == 1
    assert receipt.rejected[0].code == "CONTINUITY_UNAVAILABLE"

    event = MarketEvent(
        provider="mock",
        capability_key="realtime.snapshot.l2",
        event_type="snapshot_l2",
        level="l2",
        exchange="XSHE",
        session_epoch="epoch-1",
        security="000001.XSHE",
        payload={"last_price": 10.5},
    )
    with pytest.raises(TypeError):
        event.payload["last_price"] = 11.0
