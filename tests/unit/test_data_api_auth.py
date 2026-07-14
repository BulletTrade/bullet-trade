import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest

import bullet_trade.data.api as data_api
from bullet_trade.data.providers.base import DataProvider


class DummyProvider(DataProvider):
    def __init__(self):
        self.auth_calls = 0

    def auth(self, user=None, pwd=None, host=None, port=None):
        self.auth_calls += 1

    def get_price(self, *args, **kwargs):
        return pd.DataFrame()

    def get_trade_days(self, *args, **kwargs):
        return []

    def get_all_securities(self, *args, **kwargs):
        return pd.DataFrame()

    def get_index_stocks(self, *args, **kwargs):
        return []

    def get_split_dividend(self, *args, **kwargs):
        return []


class SlowAuthProvider(DummyProvider):
    """通过短暂阻塞扩大并发认证竞态窗口的测试 provider。"""

    name = "dummy"

    def __init__(self):
        super().__init__()
        self._calls_lock = threading.Lock()

    def auth(self, user=None, pwd=None, host=None, port=None):
        with self._calls_lock:
            self.auth_calls += 1
        time.sleep(0.05)


@pytest.mark.unit
def test_get_data_provider_triggers_auth_once(monkeypatch):
    dummy = DummyProvider()
    monkeypatch.setattr(data_api, "_provider", dummy, raising=False)
    monkeypatch.setattr(data_api, "_auth_attempted", False, raising=False)

    returned = data_api.get_data_provider()
    assert returned is dummy
    assert dummy.auth_calls == 1

    # Subsequent调用不会重复认证
    data_api.get_data_provider()
    assert dummy.auth_calls == 1


@pytest.mark.unit
def test_default_provider_is_created_lazily_and_reused(monkeypatch):
    """验证默认 provider 首次显式获取时才创建，并且只认证一次。

    Args:
        monkeypatch: pytest 提供的隔离补丁工具。
    """
    dummy = DummyProvider()
    create_calls = []

    def fake_create(provider_name=None, overrides=None):
        """记录 provider 创建调用并返回测试实例。

        Args:
            provider_name: 工厂收到的规范化 provider 名称。
            overrides: 工厂收到的覆盖配置。

        Returns:
            DummyProvider: 共享的测试 provider。
        """
        create_calls.append((provider_name, overrides))
        return dummy

    monkeypatch.setattr(data_api, "_provider", None)
    monkeypatch.setattr(data_api, "_provider_cache", {})
    monkeypatch.setattr(data_api, "_provider_auth_attempted", {})
    monkeypatch.setattr(data_api, "_auth_attempted", False)
    monkeypatch.setattr(data_api, "_create_provider", fake_create)
    monkeypatch.setattr(data_api, "get_data_provider_config", lambda: {"default": "dummy"})

    assert data_api._provider is None
    first = data_api.get_data_provider()
    second = data_api.get_data_provider()

    assert first is dummy
    assert second is dummy
    assert create_calls == [("dummy", None)]
    assert dummy.auth_calls == 1


@pytest.mark.unit
@pytest.mark.parametrize("requested_provider", [None, "dummy"])
def test_concurrent_first_access_authenticates_once(monkeypatch, requested_provider):
    """验证默认与具名 provider 的并发首次访问都只认证一次。"""
    dummy = SlowAuthProvider()
    create_calls = []
    start_barrier = threading.Barrier(2)

    def fake_create(provider_name=None, overrides=None):
        create_calls.append((provider_name, overrides))
        return dummy

    def get_provider():
        start_barrier.wait(timeout=2)
        return data_api.get_data_provider(requested_provider)

    monkeypatch.setattr(data_api, "_provider", None)
    monkeypatch.setattr(data_api, "_provider_cache", {})
    monkeypatch.setattr(data_api, "_provider_auth_attempted", {})
    monkeypatch.setattr(data_api, "_auth_attempted", False)
    monkeypatch.setattr(data_api, "_pending_default_provider_name", None)
    monkeypatch.setattr(data_api, "_create_provider", fake_create)
    monkeypatch.setattr(data_api, "get_data_provider_config", lambda: {"default": "dummy"})

    with ThreadPoolExecutor(max_workers=2) as executor:
        providers = list(executor.map(lambda _: get_provider(), range(2)))

    assert providers == [dummy, dummy]
    assert create_calls == [("dummy", None)]
    assert dummy.auth_calls == 1


@pytest.mark.unit
def test_reload_data_provider_from_env_defers_recreation(monkeypatch):
    """验证刷新环境配置只失效旧实例，直到真实取数才按指定名称重建。"""
    old_provider = DummyProvider()
    new_provider = DummyProvider()
    create_calls = []

    def fake_create(provider_name=None, overrides=None):
        create_calls.append((provider_name, overrides))
        return new_provider

    monkeypatch.setattr(data_api, "_provider", old_provider)
    monkeypatch.setattr(data_api, "_provider_cache", {"base": old_provider})
    monkeypatch.setattr(data_api, "_provider_auth_attempted", {"base": True})
    monkeypatch.setattr(data_api, "_auth_attempted", True)
    monkeypatch.setattr(data_api, "_pending_default_provider_name", None)
    monkeypatch.setattr(data_api, "_create_provider", fake_create)

    data_api.reload_data_provider_from_env("qmt-remote")

    assert create_calls == []
    assert data_api._provider is None
    assert data_api._provider_cache == {}
    assert data_api._provider_auth_attempted == {}
    assert data_api._pending_default_provider_name == "remote_qmt"

    assert data_api.get_data_provider() is new_provider
    assert create_calls == [("remote_qmt", None)]
    assert new_provider.auth_calls == 1
    assert data_api._pending_default_provider_name is None


@pytest.mark.unit
def test_remote_qmt_normal_chain_starts_once_on_first_real_use(monkeypatch):
    """验证远程 QMT 仍在首次真实数据调用时启动，并保持请求链可用。

    Args:
        monkeypatch: pytest 提供的隔离补丁工具。
    """
    import bullet_trade.data.providers.remote_qmt as remote_qmt

    class FakeRemoteQmtConnection:
        """记录远程 QMT 生命周期的无网络测试连接。"""

        instances = []

        def __init__(self, host, port, token, **kwargs):
            """保存连接配置但不访问网络。

            Args:
                host: 远程服务地址。
                port: 远程服务端口。
                token: 鉴权 token。
                **kwargs: TLS 等可选连接配置。
            """
            self.host = host
            self.port = port
            self.token = token
            self.kwargs = kwargs
            self.start_calls = 0
            self.requests = []
            type(self).instances.append(self)

        def add_event_listener(self, event, handler):
            """接受事件监听注册。

            Args:
                event: 事件名称。
                handler: 事件回调。
            """
            self.event = event
            self.handler = handler

        def start(self):
            """记录一次连接启动调用。"""
            self.start_calls += 1

        def request(self, action, payload):
            """记录 RPC 请求并返回交易日测试数据。

            Args:
                action: RPC action 名称。
                payload: RPC 请求参数。

            Returns:
                dict: 与远程协议兼容的测试响应。
            """
            self.requests.append((action, payload))
            return {"values": ["2026-07-14"]}

    monkeypatch.setattr(remote_qmt, "RemoteQmtConnection", FakeRemoteQmtConnection)
    monkeypatch.setenv("DEFAULT_DATA_PROVIDER", "qmt-remote")
    monkeypatch.setenv("QMT_SERVER_HOST", "127.0.0.1")
    monkeypatch.setenv("QMT_SERVER_PORT", "58620")
    monkeypatch.setenv("QMT_SERVER_TOKEN", "normal-chain-test")
    monkeypatch.setattr(data_api, "_provider", None)
    monkeypatch.setattr(data_api, "_provider_cache", {})
    monkeypatch.setattr(data_api, "_provider_auth_attempted", {})
    monkeypatch.setattr(data_api, "_auth_attempted", False)

    assert FakeRemoteQmtConnection.instances == []
    trade_days = data_api.get_trade_days(count=1)
    default_provider = data_api.get_data_provider()
    named_provider = data_api.get_data_provider("remote_qmt")

    assert [day.strftime("%Y-%m-%d") for day in trade_days] == ["2026-07-14"]
    assert default_provider is named_provider
    assert len(FakeRemoteQmtConnection.instances) == 1
    connection = FakeRemoteQmtConnection.instances[0]
    assert connection.start_calls == 1
    assert connection.requests == [("data.trade_days", {"start": None, "end": None, "count": 1})]
