# -*- coding: utf-8 -*-
"""
作者: BruceLee
日期: 2026-09-22
文件说明:
    验证远端版本化证券规则在核心数据 API 中的优先级、刷新和失败保留语义。

主要输入:
    provider 返回的普通证券元数据或 dtype/value 封装元数据。
主要输出:
    pytest 断言，确保版本化规则短缓存且旧版行为保持兼容。
上下游关系:
    上游是远端 data.security_info；下游是分类、T+、费用和滑点规则消费者。
关键约定:
    仅带 rule_version 的当前元数据按 60 秒刷新，历史查询和旧元数据保持原缓存行为。
"""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Union

import pytest

from bullet_trade.data import api as data_api


class RuleMetadataProvider:
    """按队列返回证券元数据，用于观察缓存和失败行为。"""

    def __init__(self, responses: List[Union[Dict[str, Any], Exception]]) -> None:
        """保存响应队列并初始化调用次数。

        Args:
            responses: 每次调用返回的字典或需要抛出的异常。

        Returns:
            None: 仅初始化测试对象。
        """
        self.responses = list(responses)
        self.calls = 0

    def auth(self, *args: Any, **kwargs: Any) -> bool:
        """模拟认证成功。

        Args:
            *args: 未使用的位置参数。
            **kwargs: 未使用的关键字参数。

        Returns:
            bool: 始终返回 True。
        """
        return True

    def get_security_info(self, security: str, date: Any = None) -> Dict[str, Any]:
        """返回下一项测试元数据或抛出指定异常。

        Args:
            security: 查询证券代码。
            date: 可选历史日期。

        Returns:
            Dict[str, Any]: 当前队列项中的证券元数据。

        Raises:
            Exception: 队列项是异常时原样抛出。
        """
        del security, date
        index = min(self.calls, len(self.responses) - 1)
        response = self.responses[index]
        self.calls += 1
        if isinstance(response, Exception):
            raise response
        return response


@pytest.fixture(autouse=True)
def _isolated_security_rule_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """隔离证券规则、provider 与两类缓存全局状态。

    Args:
        monkeypatch: pytest 全局状态替换工具。

    Returns:
        None: fixture 负责测试前后的自动恢复。
    """
    monkeypatch.setattr(data_api, "_security_info_cache", {})
    monkeypatch.setattr(data_api, "_security_info_cache_refreshed_at", {})
    monkeypatch.setattr(data_api, "_security_overrides", {})
    monkeypatch.setattr(data_api, "_security_overrides_loaded", True)
    monkeypatch.setattr(data_api, "_current_context", None)
    monkeypatch.setattr(data_api, "_auth_attempted", True)


def _set_conflicting_json_rule() -> None:
    """设置会与远端规则冲突的代码级 JSON 等价规则。

    Returns:
        None: 通过公开覆盖入口修改进程内规则并清空缓存。
    """
    data_api.set_security_overrides(
        {
            "by_category": {
                "stock": {"tplus": 1, "slippage": 0.00246, "tick_decimals": 2},
                "fund": {"tplus": 0, "slippage": 0.003, "tick_decimals": 3},
            },
            "by_code": {
                "511880.XSHG": {
                    "name": "本地名称",
                    "category": "fund",
                    "tplus": 0,
                    "cash_tool": False,
                    "fee_policy": "normal",
                    "slippage": 0.003,
                }
            },
        }
    )


def test_versioned_wrapped_metadata_wins_over_json_rule(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证 dtype/value 中的版本化显式规则优先于本地 JSON 冲突值。

    Args:
        monkeypatch: pytest 全局状态替换工具。

    Returns:
        None: 规则优先级断言通过后结束。
    """
    _set_conflicting_json_rule()
    provider = RuleMetadataProvider(
        [
            {
                "dtype": "dict",
                "value": {
                    "type": "etf",
                    "category": "money_market_fund",
                    "tplus": 1,
                    "cash_tool": True,
                    "fee_policy": "zero_system_fee",
                    "slippage": 0.0,
                    "rule_version": "a" * 64,
                },
            }
        ]
    )
    monkeypatch.setattr(data_api, "_provider", provider)

    info = data_api.get_security_info("511880.XSHG")

    assert info["category"] == "money_market_fund"
    assert info["tplus"] == 1
    assert info["cash_tool"] is True
    assert info["fee_policy"] == "zero_system_fee"
    assert info["slippage"] == 0.0
    assert info["name"] == "本地名称"


def test_versioned_current_metadata_refreshes_after_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证当前版本化规则在 60 秒内复用，过期后从同一 provider 刷新。

    Args:
        monkeypatch: pytest 全局状态替换工具。

    Returns:
        None: 调用次数与刷新结果断言通过后结束。
    """
    clock = [100.0]
    provider = RuleMetadataProvider(
        [
            {"category": "fund", "tplus": 1, "rule_version": "a" * 64},
            {"category": "fund", "tplus": 0, "rule_version": "b" * 64},
        ]
    )
    monkeypatch.setattr(data_api.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(data_api, "_provider", provider)
    monkeypatch.setattr(
        data_api,
        "_current_context",
        SimpleNamespace(current_dt=datetime(2026, 9, 22, 10, 0, 0)),
    )

    first = data_api.get_security_info("510610.XSHG")
    clock[0] = 159.9
    within_ttl = data_api.get_security_info("510610.XSHG")
    clock[0] = 160.0
    refreshed = data_api.get_security_info("510610.XSHG")

    assert first["tplus"] == 1
    assert within_ttl["rule_version"] == "a" * 64
    assert refreshed["tplus"] == 0
    assert refreshed["rule_version"] == "b" * 64
    assert provider.calls == 2


def test_versioned_refresh_failure_keeps_last_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证刷新失败保留最近成功规则，不回退本地 JSON。

    Args:
        monkeypatch: pytest 全局状态替换工具。

    Returns:
        None: 最近成功版本和调用次数断言通过后结束。
    """
    _set_conflicting_json_rule()
    clock = [10.0]
    provider = RuleMetadataProvider(
        [
            {
                "category": "money_market_fund",
                "cash_tool": True,
                "rule_version": "a" * 64,
            },
            RuntimeError("远端暂不可用"),
        ]
    )
    monkeypatch.setattr(data_api.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(data_api, "_provider", provider)

    first = data_api.get_security_info("511880.XSHG")
    clock[0] = 70.0
    retained = data_api.get_security_info("511880.XSHG")

    assert retained is first
    assert retained["category"] == "money_market_fund"
    assert retained["cash_tool"] is True
    assert retained["rule_version"] == "a" * 64
    assert provider.calls == 2


def test_versioned_historical_metadata_keeps_permanent_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证显式历史日期查询保持原有永久缓存行为。

    Args:
        monkeypatch: pytest 全局状态替换工具。

    Returns:
        None: 历史缓存未刷新断言通过后结束。
    """
    clock = [1.0]
    provider = RuleMetadataProvider(
        [
            {"category": "fund", "tplus": 1, "rule_version": "a" * 64},
            {"category": "fund", "tplus": 0, "rule_version": "b" * 64},
        ]
    )
    monkeypatch.setattr(data_api.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(data_api, "_provider", provider)

    first = data_api.get_security_info("510610.XSHG", date=date(2026, 9, 1))
    clock[0] = 1000.0
    second = data_api.get_security_info("510610.XSHG", date=date(2026, 9, 1))

    assert second is first
    assert second["tplus"] == 1
    assert provider.calls == 1


def test_legacy_metadata_keeps_json_priority_and_permanent_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证无 rule_version 的旧元数据继续由 JSON 覆盖且不按 TTL 刷新。

    Args:
        monkeypatch: pytest 全局状态替换工具。

    Returns:
        None: 旧行为兼容断言通过后结束。
    """
    _set_conflicting_json_rule()
    clock = [1.0]
    provider = RuleMetadataProvider(
        [
            {"category": "stock", "tplus": 1},
            {"category": "money_market_fund", "tplus": 1},
        ]
    )
    monkeypatch.setattr(data_api.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(data_api, "_provider", provider)

    first = data_api.get_security_info("511880.XSHG")
    clock[0] = 1000.0
    second = data_api.get_security_info("511880.XSHG")

    assert first["category"] == "fund"
    assert first["tplus"] == 0
    assert second is first
    assert provider.calls == 1


@pytest.mark.parametrize(
    "resetter", [data_api.reset_security_overrides, lambda: data_api.set_security_overrides({})]
)
def test_override_resetters_clear_versioned_refresh_cache(resetter: Any) -> None:
    """验证重置和设置覆盖规则时同步清除版本化刷新时间。

    Args:
        resetter: 要调用的覆盖规则公开入口。

    Returns:
        None: 两类缓存均被清空后结束。
    """
    data_api._security_info_cache[("511880.XSHG", None)] = data_api.SecurityInfo(
        "511880.XSHG", {"rule_version": "a" * 64}
    )
    data_api._security_info_cache_refreshed_at[("511880.XSHG", None)] = 1.0

    resetter()

    assert data_api._security_info_cache == {}
    assert data_api._security_info_cache_refreshed_at == {}
