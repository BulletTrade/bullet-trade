"""Beta 数据源工厂注册测试。"""

from __future__ import annotations

import pytest

import bullet_trade.data.api as data_api
from bullet_trade.data.providers.easy_tdx import EasyTdxProvider
from bullet_trade.data.providers.rqdata import RQDataProvider


@pytest.mark.unit
def test_provider_name_aliases_are_normalized() -> None:
    """新 provider 别名应归一到稳定缓存键。"""
    assert data_api._normalize_provider_name("rqdatac") == "rqdata"
    assert data_api._normalize_provider_name("ricequant") == "rqdata"
    assert data_api._normalize_provider_name("tdx") == "easy_tdx"
    assert data_api._normalize_provider_name("easy-tdx") == "easy_tdx"


@pytest.mark.unit
def test_create_rqdata_provider_without_importing_sdk() -> None:
    """创建 RQDataProvider 不应立刻导入或认证真实 rqdatac。"""
    provider = data_api._create_provider("rqdata", overrides={"rqdatac": object()})

    assert isinstance(provider, RQDataProvider)
    assert provider.name == "rqdata"


@pytest.mark.unit
def test_create_easy_tdx_provider_without_connecting() -> None:
    """创建 EasyTdxProvider 不应立刻连接通达信服务器。"""
    provider = data_api._create_provider("tdx", overrides={"use_stub": True})

    assert isinstance(provider, EasyTdxProvider)
    assert provider.name == "easy_tdx"
    assert provider._connected is False
