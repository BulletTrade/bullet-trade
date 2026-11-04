"""作者：BruceLee。

职责：验收严格券商事实的空结果、失败及旧适配器默认行为。
输入：无网络合同夹具；输出：明确失败不补零的断言。
上下游：broker.facts与BrokerBase；不加载厂商SDK或真实券商。
约定：只证明传输合同，不替代各适配器SDK成功读取验收。
"""

import pytest

from bullet_trade.broker.base import BrokerBase
from bullet_trade.broker.facts import account_facts_from_reads, validate_account_facts


def test_real_empty_lists_and_explicit_zero_assets_are_valid():
    """无输入返回；明确零资产和真实空明细可通过，不要求非空仓位才认定查询成功。"""
    value = account_facts_from_reads({"total_value": 0, "available_cash": 0}, [], [], [])
    assert validate_account_facts(value) is value


@pytest.mark.parametrize("field", ["assets", "positions", "orders", "executions"])
def test_failed_reads_cannot_become_empty_facts(field):
    """输入失败字段，无返回；任一读取None都拒绝，不转换为空集合。"""
    value = account_facts_from_reads({"total_value": 100, "available_cash": 100}, [], [], [])
    value[field] = None
    with pytest.raises(ValueError):
        validate_account_facts(value)


def test_missing_version_or_asset_and_old_adapter_are_explicitly_unsupported():
    """无输入返回；非法合同不猜默认值，旧适配器未实现新接口时明确失败且不调用旧查询。"""
    with pytest.raises(ValueError):
        validate_account_facts({})
    with pytest.raises(ValueError):
        account_facts_from_reads({}, [], [], [])
    with pytest.raises(NotImplementedError):
        BrokerBase.get_account_facts(object())


@pytest.mark.parametrize("value", [True, "NaN", "Infinity", "-Infinity", "bad"])
@pytest.mark.parametrize("field", ["total_value", "available_cash", "cash", "frozen_cash"])
def test_invalid_asset_numbers_are_not_accepted(field, value):
    """输入资产字段与非法数值，无返回；拒绝布尔、非有限值和坏文本，不把它们补零。"""
    assets = {"total_value": 0, "available_cash": 0, field: value}
    with pytest.raises(ValueError, match="数值无效"):
        account_facts_from_reads(assets, [], [], [])
