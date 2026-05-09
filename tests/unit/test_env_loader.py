"""
测试环境变量加载器

单元测试：测试环境变量加载和获取功能
"""

import pytest
from bullet_trade.utils.env_loader import (
    get_env,
    get_env_bool,
    get_env_int,
    get_data_provider_config,
    get_broker_config,
    get_system_config,
    get_risk_control_config,
)


@pytest.mark.unit
class TestEnvLoader:
    """环境变量加载器测试"""

    def test_get_env(self, monkeypatch):
        """测试获取环境变量"""
        monkeypatch.setenv("TEST_KEY", "test_value")
        assert get_env("TEST_KEY") == "test_value"
        assert get_env("NON_EXISTENT") is None
        assert get_env("NON_EXISTENT", "default") == "default"

    def test_get_env_bool(self, monkeypatch):
        """测试获取布尔类型环境变量"""
        monkeypatch.setenv("TEST_BOOL_TRUE", "true")
        monkeypatch.setenv("TEST_BOOL_FALSE", "false")
        monkeypatch.setenv("TEST_BOOL_1", "1")
        monkeypatch.setenv("TEST_BOOL_0", "0")

        assert get_env_bool("TEST_BOOL_TRUE") is True
        assert get_env_bool("TEST_BOOL_FALSE") is False
        assert get_env_bool("TEST_BOOL_1") is True
        assert get_env_bool("TEST_BOOL_0") is False
        assert get_env_bool("NON_EXISTENT") is False
        assert get_env_bool("NON_EXISTENT", True) is True

    def test_get_env_int(self, monkeypatch):
        """测试获取整数类型环境变量"""
        monkeypatch.setenv("TEST_INT", "123")
        monkeypatch.setenv("TEST_INT_INVALID", "not_a_number")

        assert get_env_int("TEST_INT") == 123
        assert get_env_int("TEST_INT_INVALID") == 0
        assert get_env_int("NON_EXISTENT") == 0
        assert get_env_int("NON_EXISTENT", 42) == 42

    def test_get_data_provider_config(self, monkeypatch):
        """测试获取数据提供者配置"""
        monkeypatch.setenv("DEFAULT_DATA_PROVIDER", "jqdata")
        monkeypatch.setenv("JQDATA_USERNAME", "test_user")
        monkeypatch.setenv("JQDATA_PASSWORD", "test_pwd")

        config = get_data_provider_config()

        assert config["default"] == "jqdata"
        assert config["jqdata"]["username"] == "test_user"
        assert config["jqdata"]["password"] == "test_pwd"

    def test_get_broker_config(self, monkeypatch):
        """测试获取券商配置"""
        monkeypatch.setenv("DEFAULT_BROKER", "qmt")
        monkeypatch.setenv("QMT_ACCOUNT_ID", "12345")

        config = get_broker_config()

        assert config["default"] == "qmt"
        assert config["qmt"]["account_id"] == "12345"

    def test_get_system_config(self, monkeypatch):
        """测试获取系统配置"""
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        config = get_system_config()

        assert config["log_level"] == "DEBUG"
        assert config["log_file_level"] == "DEBUG"

    def test_get_risk_control_config_min_buy_order_value(self, monkeypatch):
        """测试读取最小买入订单金额配置"""
        monkeypatch.setenv("MIN_BUY_ORDER_VALUE", "1000.5")

        config = get_risk_control_config()

        assert config["min_buy_order_value"] == 1000.5
