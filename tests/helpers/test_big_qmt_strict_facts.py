"""作者：BruceLee。

职责：验收独立GBK helper的严格读取，不启动HTTP或执行任何真实委托。
输入：实际helper模块、get_trade_detail_data夹具；输出：真实空结果、失败及字段保留断言。
上下游：/facts、BigQmtBrokerAdapter；旧/account等接口仍由原回归覆盖。
约定：默认和终端范围都成功才能去重并集，None和SDK异常不得补成空数组。
"""

from types import SimpleNamespace

import pytest

from test_big_qmt_gateway_strategy_sample import _load_helper


def install_reader(module, monkeypatch, values):
    """输入helper、补丁器及各类原始数据，返回调用记录；不安装写入函数。"""
    calls = []

    def reader(account, kind, detail, source=None):
        """输入实际SDK查询参数，返回本项原始结果或抛出指定异常。"""
        assert account == "001234"
        calls.append((detail, source))
        value = values[detail]
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(module, "get_trade_detail_data", reader, raising=False)
    return calls


def values():
    """无输入，返回独立零持仓原始查询；资产来自实际SDK字段而不是配置回显。"""
    return {
        "account": [SimpleNamespace(m_strAccountID="001234", m_dAvailable=0, m_dBalance=0)],
        "position": [],
        "order": [],
        "deal": [],
    }


def test_empty_lists_are_success_and_both_sources_are_read(monkeypatch):
    """输入补丁器，无返回；真实空仓和零资产可通过，全部查询范围都有实际成功结果。"""
    module = _load_helper()
    calls = install_reader(module, monkeypatch, values())
    result = module._query_strict_facts("001234", "STOCK")
    assert result["status"] == "complete" and result["assets"]["total_value"] == 0
    assert result["positions"] == result["orders"] == result["executions"] == []
    assert calls == [
        ("account", None),
        ("position", None),
        ("order", "qmt"),
        ("order", None),
        ("deal", "qmt"),
        ("deal", None),
    ]
    assert module._route_to_action("/facts", {}) == "facts"
    assert "facts" in module.ACCOUNT_QUERY_ACTIONS


@pytest.mark.parametrize("kind", ["account", "position", "order", "deal"])
@pytest.mark.parametrize("failure", [None, RuntimeError("sdk-private-error")])
def test_failed_sdk_result_is_not_empty(kind, failure, monkeypatch):
    """输入失败类型，无返回；任一查询None/抛错都失败，且不把SDK敏感异常向上传播。"""
    module = _load_helper()
    data = values()
    data[kind] = failure
    install_reader(module, monkeypatch, data)
    with pytest.raises(ValueError) as error:
        module._query_strict_facts("001234", "STOCK")
    assert "sdk-private-error" not in str(error.value)


def test_missing_fields_are_unknown_not_default_zero_and_bad_row_fails(monkeypatch):
    """输入补丁器，无返回；无可卖/成本/费用时保留None，坏证券行不可静默过滤。"""
    module = _load_helper()
    data = values()
    data["position"] = [
        SimpleNamespace(m_strInstrumentID="510300", m_strExchangeID="SH", m_nVolume=100)
    ]
    data["order"] = [
        SimpleNamespace(
            m_strOrderSysID="O1",
            m_strInstrumentID="510300",
            m_strExchangeID="SH",
            m_nVolumeTotalOriginal=100,
            m_nOrderStatus=50,
        )
    ]
    data["deal"] = [
        SimpleNamespace(
            m_strTradeID="T1",
            m_strInstrumentID="510300",
            m_strExchangeID="SH",
            m_nVolume=100,
            m_dTradePrice=3,
        )
    ]
    install_reader(module, monkeypatch, data)
    result = module._query_strict_facts("001234", "STOCK")
    assert len(result["orders"]) == len(result["executions"]) == 1
    assert result["positions"][0]["closeable_amount"] is None
    assert result["positions"][0]["avg_cost"] is None
    assert result["orders"][0]["filled"] is None
    assert result["executions"][0]["commission"] is None
    data["order"] = [SimpleNamespace(m_strOrderSysID="bad")]
    with pytest.raises(ValueError):
        module._query_strict_facts("001234", "STOCK")
