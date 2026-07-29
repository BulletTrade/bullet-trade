# -*- coding: utf-8 -*-

"""
作者: BruceLee
日期: 2026-07-29
文件说明:
    校验 Gateway V2 复用的场内基金代码级 T+ 规则。

主要输入:
    bullet_trade/config/security_overrides.json。
主要输出:
    pytest 断言，确保 19 个目标品种为 T+0，510610 为 T+1。
上下游关系:
    上游是 bullet-trade 证券配置；下游由 AIStocks Gateway V2 账本读取代码级规则。
关键约定:
    规则键统一使用聚宽长后缀，QMT 短后缀由 V2 代码归一化层兼容。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pytest

from bullet_trade.data import api as data_api


T0_SECURITY_CODES = (
    "513180.XSHG",
    "513330.XSHG",
    "513050.XSHG",
    "159920.XSHE",
    "510900.XSHG",
    "513100.XSHG",
    "513500.XSHG",
    "513000.XSHG",
    "513030.XSHG",
    "518880.XSHG",
    "159934.XSHE",
    "159985.XSHE",
    "162411.XSHE",
    "511010.XSHG",
    "511520.XSHG",
    "511380.XSHG",
    "511360.XSHG",
    "511990.XSHG",
    "511880.XSHG",
)


def _load_code_overrides() -> Dict[str, Dict[str, Any]]:
    """读取代码级证券规则。

    Returns:
        Dict[str, Dict[str, Any]]: 按聚宽证券代码索引的配置字典。
    """

    config_path = (
        Path(__file__).resolve().parents[2] / "bullet_trade" / "config" / "security_overrides.json"
    )
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return {
        str(security): dict(rule)
        for security, rule in (payload.get("by_code") or {}).items()
        if isinstance(rule, dict)
    }


@pytest.mark.parametrize("security", T0_SECURITY_CODES)
def test_target_security_is_explicitly_tplus_zero(security: str) -> None:
    """验证每个目标品种都有显式 T+0 代码规则。

    Args:
        security: 聚宽长后缀证券代码。

    Returns:
        None: 配置断言通过后结束。
    """

    rule = _load_code_overrides()[security]

    assert rule["tplus"] == 0


def test_510610_is_explicitly_tplus_one() -> None:
    """验证境内能源股票 ETF 510610 不会被误列为 T+0。

    Returns:
        None: T+1 配置断言通过后结束。
    """

    rule = _load_code_overrides()["510610.XSHG"]

    assert rule["tplus"] == 1
    assert "白银" not in str(rule.get("name") or "")


@pytest.mark.parametrize(
    ("security", "expected_tplus"),
    (("513180.SH", 0), ("513180.XSHG", 0), ("510610.SH", 1), ("510610.XSHG", 1)),
)
def test_data_api_applies_code_rule_to_jq_and_qmt_suffixes(
    security: str,
    expected_tplus: int,
) -> None:
    """验证 bullet-trade 元数据合并也兼容 QMT 与聚宽后缀。

    Args:
        security: QMT 或聚宽格式证券代码。
        expected_tplus: 代码级规则期望值。

    Returns:
        None: 元数据合并结果通过后结束。
    """

    data_api.reset_security_overrides()
    info = data_api._merge_overrides(security, {"type": "etf"})

    assert info["tplus"] == expected_tplus
