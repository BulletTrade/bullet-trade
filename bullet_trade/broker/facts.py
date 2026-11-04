"""作者：BruceLee。

职责：定义严格只读券商事实的传输合同，阻止查询失败被空列表或默认资产替代。
输入：适配器明确完成的资产、持仓、委托与成交读取；输出：版本化完整结果或明确异常。
上下游：Broker及Remote Server复用，工作台校验后建立镜像；不包含下单、撤单或结算。
约定：结构验证不替代SDK成功证据；适配器必须先确认四类原始读取成功，才能构造结果。
"""

from decimal import Decimal, InvalidOperation

__all__ = ["account_facts_from_reads", "validate_account_facts"]


def validate_account_facts(value):
    """输入严格事实响应，返回原值；缺失、None或不支持版本均抛ValueError，不进行补值。"""
    keys = {"schema_version", "status", "assets", "positions", "orders", "executions"}
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError("严格券商事实响应不完整")
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["status"] != "complete"
    ):
        raise ValueError("严格券商事实版本或状态不受支持")
    assets = value["assets"]
    if not isinstance(assets, dict) or any(
        assets.get(key) is None for key in ("total_value", "available_cash")
    ):
        raise ValueError("券商资产尚未成功读取")
    for key in ("total_value", "available_cash", "cash", "frozen_cash"):
        if assets.get(key) is None:
            continue
        try:
            if isinstance(assets[key], bool) or not Decimal(str(assets[key])).is_finite():
                raise ValueError()
        except (InvalidOperation, ValueError):
            raise ValueError("券商资产数值无效") from None
    for key in ("positions", "orders", "executions"):
        if not isinstance(value[key], list) or any(not isinstance(row, dict) for row in value[key]):
            raise ValueError("券商明细尚未成功读取")
    return value


def account_facts_from_reads(assets, positions, orders, executions):
    """输入已成功的四类读取，返回完整合同；真实空列表有效，None绝不能代表空仓或无成交。"""
    return validate_account_facts(
        {
            "schema_version": 1,
            "status": "complete",
            "assets": assets,
            "positions": positions,
            "orders": orders,
            "executions": executions,
        }
    )
