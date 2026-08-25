"""验证华鑫节点资产摘要纯函数合同。"""

from copy import deepcopy

import pytest

from bullet_trade.integrations.huaxin_node_assets import (
    HUAXIN_NODE16_READY_SCHEMA,
    HuaxinNodeAssetDigestError,
    build_huaxin_node_asset_snapshot_digest,
    build_huaxin_positions_snapshot_id,
)


def _snapshot() -> dict:
    """构造不含生产身份的完整 node16 资产快照。

    Returns:
        dict: 包含现金和两条 ETF 持仓的测试快照。
    """

    return {
        "trading_day": "20260826",
        "captured_at": "2026-08-26T08:30:00+08:00",
        "node_id": 16,
        "node": {"node_id": 16, "provenance": "configured_session"},
        "account": {
            "department_id": "D",
            "account_id": "A",
            "currency": "CNY",
            "available_cash": "1000.00",
            "transferable_cash": 1000,
            "frozen_cash": 0,
        },
        "positions": [
            {
                "exchange": "SSE",
                "security": "511880.SH",
                "current_position": 10000,
                "available_position": 10000,
                "history_position": 10000,
                "onroad_position": 0,
                "investor_id": "I1",
                "business_unit_id": "B1",
                "shareholder_id": "S1",
                "market_id": 1,
            },
            {
                "exchange": "SZSE",
                "security": "159001.SZ",
                "current_position": 20,
                "available_position": 20,
                "history_position": 20,
                "investor_id": "I2",
                "business_unit_id": "B2",
                "shareholder_id": "S2",
                "market_id": 2,
            },
        ],
    }


def test_digest_matches_frozen_writer_contract() -> None:
    """验证摘要与完整华鑫 writer 的冻结合同完全一致。

    Returns:
        None: 断言节点摘要、持仓摘要和 READY schema 固定值。
    """

    snapshot = _snapshot()

    assert HUAXIN_NODE16_READY_SCHEMA == "huaxin-node16-ready/v1"
    assert (
        build_huaxin_positions_snapshot_id(snapshot["trading_day"], snapshot["positions"])
        == "c636c401d51c4d2f2bac8d836d2805a75079ff9ea10473e798dc18b390b6f35c"
    )
    assert build_huaxin_node_asset_snapshot_digest(snapshot) == (
        "b904b80a61f252ee92d6d04e54b63853a184f49d375c624b21402f82457b7b6c"
    )


def test_digest_ignores_capture_time_and_row_order_but_detects_asset_drift() -> None:
    """验证采样时间和行序无关，现金或持仓漂移一定改变摘要。

    Returns:
        None: 断言稳定性和业务漂移敏感性。
    """

    first = _snapshot()
    reordered = deepcopy(first)
    reordered["captured_at"] = "2026-08-26T08:31:00+08:00"
    reordered["positions"] = list(reversed(reordered["positions"]))
    stable = build_huaxin_node_asset_snapshot_digest(first)

    assert build_huaxin_node_asset_snapshot_digest(reordered) == stable
    cash_changed = deepcopy(first)
    cash_changed["account"]["available_cash"] = "999.99"
    assert build_huaxin_node_asset_snapshot_digest(cash_changed) != stable
    position_changed = deepcopy(first)
    position_changed["positions"][0]["current_position"] = 9999
    assert build_huaxin_node_asset_snapshot_digest(position_changed) != stable


@pytest.mark.parametrize(
    ("field", "value", "error_code"),
    [
        ("trading_day", "2026-08-26", "node_asset_trading_day_invalid"),
        ("node", {}, "node_asset_node_mismatch"),
        ("positions", None, "node_asset_positions_invalid"),
    ],
)
def test_invalid_complete_snapshot_fails_closed(field: str, value: object, error_code: str) -> None:
    """验证日期、节点证明和完整持仓缺失时失败关闭。

    Args:
        field: 要破坏的快照字段。
        value: 非法字段值。
        error_code: 预期稳定错误码。

    Returns:
        None: 断言抛出确定性摘要错误。
    """

    snapshot = _snapshot()
    snapshot[field] = value

    with pytest.raises(HuaxinNodeAssetDigestError, match=error_code):
        build_huaxin_node_asset_snapshot_digest(snapshot)
