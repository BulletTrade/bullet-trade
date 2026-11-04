"""作者：BruceLee。

职责：验证QMT严格只读事实的实际SDK调用、真实空集合和失败隔离。
输入：独立SDK夹具；输出：四类读取成功或明确异常、零交易调用及旧接口回归断言。
上下游：QmtBroker.get_account_facts；不加载真实xtquant或连接券商。
约定：None、SDK异常、缺字段不能变为空仓；旧宽松接口保持既有行为。
"""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from bullet_trade.broker.qmt import QmtBroker


def broker_fixture():
    """无输入，返回已连接标记的本项Broker和SDK；只读方法真实执行，写方法直接失败。"""
    broker = QmtBroker("001234")
    broker._connected = True
    broker._xt_account = object()
    sdk = SimpleNamespace(
        query_stock_asset=Mock(return_value=SimpleNamespace(cash=97000, total_asset=100000)),
        query_stock_positions=Mock(return_value=[]),
        query_stock_orders=Mock(return_value=[]),
        query_stock_trades=Mock(return_value=[]),
        order_stock=Mock(side_effect=AssertionError("禁止交易")),
        cancel_order_stock=Mock(side_effect=AssertionError("禁止撤单")),
    )
    broker._xt_trader = sdk
    return broker, sdk


def test_real_empty_lists_are_success_and_never_submit():
    """无输入返回；SDK成功给出空数组时确认空仓/无成交，不执行任何交易写入。"""
    broker, sdk = broker_fixture()
    result = broker.get_account_facts()
    assert result["status"] == "complete" and result["schema_version"] == 1
    assert result["positions"] == result["orders"] == result["executions"] == []
    assert result["assets"]["available_cash"] == 97000
    for method in (
        sdk.query_stock_asset,
        sdk.query_stock_positions,
        sdk.query_stock_orders,
        sdk.query_stock_trades,
    ):
        assert method.call_count == 1
    sdk.order_stock.assert_not_called()
    sdk.cancel_order_stock.assert_not_called()


@pytest.mark.parametrize(
    "method",
    ["query_stock_asset", "query_stock_positions", "query_stock_orders", "query_stock_trades"],
)
@pytest.mark.parametrize("failure", ["none", "error"])
def test_none_and_exception_are_not_empty_lists(method, failure):
    """输入失败接口和类型，无返回；任一原始查询失败拒绝整包且不泄漏SDK异常。"""
    broker, sdk = broker_fixture()
    if failure == "none":
        getattr(sdk, method).return_value = None
    else:
        getattr(sdk, method).side_effect = RuntimeError("secret-in-sdk-error")
    with pytest.raises(ValueError) as error:
        broker.get_account_facts()
    assert "secret-in-sdk-error" not in str(error.value)
    sdk.order_stock.assert_not_called()


def test_missing_optional_fields_stay_unknown_and_legacy_reads_stay_compatible():
    """无输入返回；严格字段不借零兜底，旧接口仍保留原行为，不被新接口改写。"""
    broker, sdk = broker_fixture()
    sdk.query_stock_positions.return_value = [SimpleNamespace(stock_code="510300.SH", volume=100)]
    result = broker.get_account_facts()
    assert result["positions"][0]["closeable_amount"] is None
    assert result["positions"][0]["avg_cost"] is None
    assert result["positions"][0]["current_price"] is None
    sdk.query_stock_positions.return_value = None
    assert broker.get_positions() == []
    sdk.query_stock_orders.side_effect = RuntimeError("fixture")
    assert broker.get_orders() == []


def test_missing_capability_or_malformed_row_is_not_silently_dropped():
    """无输入返回；缺接口、坏行和缺资产字段明确失败，不通过过滤后空列表伪装成功。"""
    broker, sdk = broker_fixture()
    del sdk.query_stock_trades
    with pytest.raises((ValueError, NotImplementedError)):
        broker.get_account_facts()
    broker, sdk = broker_fixture()
    sdk.query_stock_orders.return_value = [{"order_id": "missing-security"}]
    with pytest.raises(ValueError):
        broker.get_account_facts()
    sdk.query_stock_orders.return_value = []
    sdk.query_stock_asset.return_value = SimpleNamespace(cash=97000)
    with pytest.raises(ValueError):
        broker.get_account_facts()
