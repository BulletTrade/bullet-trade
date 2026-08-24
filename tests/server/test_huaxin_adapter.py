"""验证 Huaxin server adapter 的 Trader/XMD 分离、安全门禁和透传合同。"""

import asyncio
import sys
import textwrap
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from bullet_trade.data.providers.huaxin import HuaxinDataProvider
from bullet_trade.integrations.huaxin.asset_consolidation import (
    HUAXIN_ASSET_CONSOLIDATION_ORDER_BLOCKED,
    HuaxinAssetConsolidationConfig,
)
from bullet_trade.integrations.huaxin.errors import (
    HuaxinNativeUnavailableError,
    HuaxinTradingDisabledError,
)
from bullet_trade.integrations.huaxin.xmd_backend import (
    DEFAULT_MAX_AGE_SECONDS,
    HUAXIN_XMD_SOURCE,
    Python37XmdBackend,
    XmdBackendError,
    _normalise_tick,
)

_TEST_XMD_FRONT = "tcp://127.0.0.1:9402"
from bullet_trade.server import config as server_config_module
from bullet_trade.server.adapters.base import AccountRouter, AdapterBundle
from bullet_trade.server.adapters.huaxin import (
    HuaxinBrokerAdapter,
    HuaxinDataAdapter,
    _load_huaxin_broker_config,
    _validate_huaxin_server_config,
    build_huaxin_bundle,
)
from bullet_trade.server.app import (
    ServerApplication,
    _build_write_fingerprint,
    _enforce_cancel_result_semantics,
)
from bullet_trade.server.config import (
    AccountConfig,
    ServerConfig,
    TLSConfig,
    _parse_allowlist,
    build_server_config,
)


def _server_config(**overrides):
    """构造单账户、broker-only 测试服务配置。

    Args:
        **overrides: ServerConfig 字段覆盖。

    Returns:
        ServerConfig: 测试配置。
    """

    values = {
        "server_type": "huaxin",
        "listen": "127.0.0.1",
        "enable_data": False,
        "enable_broker": True,
        "accounts": [AccountConfig(key="default", account_id="acct")],
    }
    values.update(overrides)
    return ServerConfig(**values)


async def _wait_for_condition(predicate, timeout=1.0):
    """等待异步 watchdog 使给定条件成立。

    Args:
        predicate: 无参数布尔函数。
        timeout: 最长等待秒数。

    Returns:
        None。

    Raises:
        AssertionError: 超时仍未满足条件时抛出。
    """

    loop = asyncio.get_running_loop()
    deadline = loop.time() + float(timeout)
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("等待 watchdog 状态变化超时")


def test_non_loopback_requires_tls_fixed_token_and_allowlist() -> None:
    """验证非回环监听缺任一网络安全条件都会 fail closed。"""

    with pytest.raises(ValueError, match="TLS"):
        _validate_huaxin_server_config(_server_config(listen="0.0.0.0"), {})

    with pytest.raises(ValueError, match="固定 token"):
        _validate_huaxin_server_config(
            _server_config(
                listen="0.0.0.0",
                tls=TLSConfig(True, "cert.pem", "key.pem"),
                generated_token=True,
                allowlist=["10.0.0.1"],
            ),
            {},
        )


def test_allowlist_rejects_invalid_entries_and_preserves_ipv6_host_prefix() -> None:
    """验证非法白名单 fail closed，IPv6 单地址严格规范为 /128。"""

    assert _parse_allowlist("10.0.0.1,2001:db8::1", strict=True) == [
        "10.0.0.1/32",
        "2001:db8::1/128",
    ]
    with pytest.raises(ValueError, match="非法"):
        _parse_allowlist("not-an-ip", strict=True)

    config = _server_config(allowlist=["2001:db8::1"])
    app = ServerApplication(
        config, AccountRouter(config.accounts), AdapterBundle(None, None, False)
    )
    assert app._is_ip_allowed("2001:db8::1") is True
    assert app._is_ip_allowed("2001:db8::2") is False
    assert app._is_ip_allowed(None) is False

    invalid = _server_config(allowlist=["not-an-ip"])
    with pytest.raises(ValueError, match="非法"):
        ServerApplication(
            invalid, AccountRouter(invalid.accounts), AdapterBundle(None, None, False)
        )

    with pytest.raises(ValueError, match="allowlist"):
        _validate_huaxin_server_config(
            _server_config(
                listen="0.0.0.0",
                tls=TLSConfig(True, "cert.pem", "key.pem"),
                token="fixed",
                generated_token=False,
            ),
            {},
        )


def test_huaxin_bundle_requires_key_but_not_sqlite_journal(
    monkeypatch,
) -> None:
    """验证华鑫写保留幂等键要求，但不会创建或要求 SQLite。

    Args:
        monkeypatch: pytest monkeypatch fixture。

    Returns:
        None。
    """

    login_metadata = {
        "mac_address": "00-11-22-33-44-55",
        "user_product_info": "BT",
    }
    monkeypatch.setattr(
        "bullet_trade.server.adapters.huaxin._load_huaxin_broker_config",
        lambda: dict(login_metadata),
    )
    config = _server_config()
    router = AccountRouter(config.accounts)
    bundle = build_huaxin_bundle(config, router)

    assert bundle.data_adapter is None
    assert isinstance(bundle.broker_adapter, HuaxinBrokerAdapter)
    assert bundle.broker_writes_require_persistent_idempotency is False
    assert bundle.broker_writes_require_idempotency_key is True

    _validate_huaxin_server_config(
        config,
        {**login_metadata, "enable_trading": True},
    )
    health = ServerApplication(config, router, bundle)._health_snapshot()["value"]
    assert health["idempotency_journal"]["required"] is False
    assert health["idempotency_journal"]["ready"] is False
    assert health["idempotency_journal"]["state"] == "ready"
    assert health["huaxin"]["actions"]["broker.place_order"]["status"] == "unavailable"


def test_huaxin_server_uses_huaxin_account_environment(monkeypatch) -> None:
    """验证 server 路由账户不再错误依赖 QMT_ACCOUNT_ID。"""

    values = {
        "HUAXIN_ACCOUNT_ID": "huaxin-account",
        "HUAXIN_ACCOUNT_TYPE": "stock",
    }
    monkeypatch.setattr(
        server_config_module,
        "get_env",
        lambda name, default=None: values.get(name, default),
    )
    monkeypatch.setattr(server_config_module, "get_env_int", lambda name, default=0: default)
    monkeypatch.setattr(server_config_module, "get_env_bool", lambda name, default=False: default)
    monkeypatch.setattr(server_config_module, "get_env_float", lambda name, default=0.0: default)
    monkeypatch.setattr(server_config_module, "get_env_optional_bool", lambda name: None)
    args = SimpleNamespace(
        server_type="huaxin",
        listen="127.0.0.1",
        token="fixed-token",
        enable_data=False,
        enable_broker=True,
        tls_cert=None,
        tls_key=None,
    )

    config = build_server_config(args)

    assert len(config.accounts) == 1
    assert config.accounts[0].account_id == "huaxin-account"
    assert config.accounts[0].data_path is None
    assert not hasattr(config, "huaxin_order_identity_journal_path")


def test_huaxin_xmd_config_accepts_any_explicit_tcp_front() -> None:
    """验证启用 data 时要求显式 sidecar、路径和通用 TCP 前置。"""

    missing = _server_config(enable_data=True, enable_broker=False, accounts=[])
    with pytest.raises(ValueError, match="HUAXIN_XMD_BACKEND"):
        _validate_huaxin_server_config(missing, {})

    wrong_front = _server_config(
        enable_data=True,
        enable_broker=False,
        accounts=[],
        huaxin_xmd_backend="python37_sidecar",
        huaxin_xmd_python="/private/python",
        huaxin_xmd_sdk_dir="/private/sdk",
        huaxin_xmd_front="http://127.0.0.1:7780",
    )
    with pytest.raises(ValueError, match="tcp://host:port"):
        _validate_huaxin_server_config(wrong_front, {})

    for front in (_TEST_XMD_FRONT, "tcp://127.0.0.2:9502"):
        valid = _server_config(
            enable_data=True,
            enable_broker=False,
            accounts=[],
            huaxin_xmd_backend="python37_sidecar",
            huaxin_xmd_python="/private/python",
            huaxin_xmd_sdk_dir="/private/sdk",
            huaxin_xmd_front=front,
        )
        _validate_huaxin_server_config(valid, {})

    too_relaxed = _server_config(
        enable_data=True,
        enable_broker=False,
        accounts=[],
        huaxin_xmd_backend="python37_sidecar",
        huaxin_xmd_python="/private/python",
        huaxin_xmd_sdk_dir="/private/sdk",
        huaxin_xmd_front=_TEST_XMD_FRONT,
        huaxin_xmd_max_age_seconds=30.1,
    )
    with pytest.raises(ValueError, match="不能超过"):
        _validate_huaxin_server_config(too_relaxed, {})


def test_huaxin_server_config_reads_namespaced_xmd_environment(monkeypatch) -> None:
    """验证 Huaxin server 配置只从显式 HUAXIN_XMD_* 字段装配行情模块。"""

    values = {
        "HUAXIN_XMD_BACKEND": "python37_sidecar",
        "HUAXIN_XMD_PYTHON": "/private/huaxin37/bin/python",
        "HUAXIN_XMD_SDK_DIR": "/private/xmd-sdk",
        "HUAXIN_XMD_FRONT": _TEST_XMD_FRONT,
    }
    monkeypatch.setattr(
        server_config_module,
        "get_env",
        lambda name, default=None: values.get(name, default),
    )
    monkeypatch.setattr(server_config_module, "get_env_int", lambda name, default=0: default)
    monkeypatch.setattr(
        server_config_module,
        "get_env_bool",
        lambda name, default=False: name == "HUAXIN_XMD_SIMULATION_REPLAY" or default,
    )
    monkeypatch.setattr(server_config_module, "get_env_float", lambda name, default=0.0: default)
    monkeypatch.setattr(server_config_module, "get_env_optional_bool", lambda name: None)
    args = SimpleNamespace(
        server_type="huaxin",
        listen="127.0.0.1",
        token="fixed-token",
        enable_data=True,
        enable_broker=False,
        tls_cert=None,
        tls_key=None,
    )

    config = build_server_config(args)

    assert config.huaxin_xmd_backend == "python37_sidecar"
    assert config.huaxin_xmd_python == "/private/huaxin37/bin/python"
    assert config.huaxin_xmd_sdk_dir == "/private/xmd-sdk"
    assert config.huaxin_xmd_front == _TEST_XMD_FRONT
    assert config.huaxin_xmd_max_age_seconds == DEFAULT_MAX_AGE_SECONDS
    assert config.huaxin_xmd_simulation_replay is True
    assert config.accounts == []


def test_huaxin_provider_uses_same_thirty_second_default(monkeypatch) -> None:
    """验证直接 provider 未配置时与 Server/backend 同为 30 秒。"""

    monkeypatch.delenv("HUAXIN_XMD_MAX_AGE_SECONDS", raising=False)

    provider = HuaxinDataProvider(
        {
            "huaxin_xmd_python": "/private/python",
            "huaxin_xmd_sdk_dir": "/private/sdk",
            "huaxin_xmd_front": _TEST_XMD_FRONT,
        }
    )

    assert provider._max_age_seconds == DEFAULT_MAX_AGE_SECONDS


def test_huaxin_provider_reads_explicit_simulation_replay_flag() -> None:
    """验证直接 provider 只在显式配置时启用仿真回放时钟。"""

    provider = HuaxinDataProvider({"huaxin_xmd_simulation_replay": "true"})

    assert provider._simulation_replay is True


def test_huaxin_broker_config_maps_required_mac_address(monkeypatch) -> None:
    """验证 server 将独立生产 MacAddress 映射到 native 会话配置。"""

    monkeypatch.setattr(
        "bullet_trade.server.adapters.huaxin.get_broker_config",
        lambda: {"huaxin": {}},
    )
    monkeypatch.setattr(
        "bullet_trade.server.adapters.huaxin.get_env",
        lambda name, default=None: {
            "HUAXIN_MAC_ADDRESS": "00-11-22-33-44-55",
            "HUAXIN_USER_PRODUCT_INFO": "BT",
        }.get(name, default),
    )

    config = _load_huaxin_broker_config()

    assert config["mac_address"] == "00-11-22-33-44-55"
    assert config["user_product_info"] == "BT"
    assert "order_identity_journal_path" not in config


@pytest.mark.parametrize(
    ("broker_config", "message"),
    [
        ({"user_product_info": "BT"}, "HUAXIN_MAC_ADDRESS"),
        ({"mac_address": "00-11-22-33-44-55"}, "HUAXIN_USER_PRODUCT_INFO"),
        (
            {"mac_address": "A" * 21, "user_product_info": "BT"},
            "HUAXIN_MAC_ADDRESS.*20",
        ),
        (
            {
                "mac_address": "00-11-22-33-44-55",
                "user_product_info": "BulletTrade",
            },
            "HUAXIN_USER_PRODUCT_INFO.*10",
        ),
    ],
)
def test_huaxin_server_rejects_missing_or_oversized_login_metadata(broker_config, message) -> None:
    """验证 server 创建 adapter 前拒绝缺失或越界的登录终端字段。

    Args:
        broker_config: 待验证的 Huaxin broker 配置。
        message: 预期错误文本正则。

    Returns:
        None。
    """

    with pytest.raises(ValueError, match=message):
        _validate_huaxin_server_config(_server_config(), broker_config)


def test_cancel_fingerprint_includes_exact_tora_identity() -> None:
    """验证同一撤单键不能在不同 OrderSysID 间被静默复用。"""

    base = {
        "order_id": "stable-local",
        "idempotency_key": "cancel-1",
        "provider_extension": {"huaxin_tora": {"exchange": "SSE", "order_sys_id": "SYS1"}},
    }
    changed = {
        **base,
        "provider_extension": {"huaxin_tora": {"exchange": "SSE", "order_sys_id": "SYS2"}},
    }

    assert _build_write_fingerprint("broker.cancel_order", base) != _build_write_fingerprint(
        "broker.cancel_order", changed
    )


def test_cancel_action_rejection_is_not_mislabeled_as_terminal_order() -> None:
    """验证撤单响应拒绝不会被误写成原订单已进入拒绝终态。"""

    result = _enforce_cancel_result_semantics(
        {"order_id": "SYS1", "idempotency_key": "cancel-1"},
        {
            "order_id": "SYS1",
            "value": False,
            "status": "rejected",
            "cancel_outcome": "rejected",
        },
    )

    assert result["value"] is False
    assert result["cancel_outcome"] == "rejected"


class _FakeXmdBackend:
    """提供可注入 HuaxinDataAdapter 的纯内存新鲜 XMD backend。"""

    def __init__(self, **kwargs):
        """保存构造参数并建立一条默认新鲜快照。

        Args:
            **kwargs: HuaxinDataAdapter 传入的显式 XMD 配置。

        Returns:
            None。
        """

        self.config = dict(kwargs)
        self.started = False
        self.stopped = False
        self.subscriptions = set()
        now = datetime.now(timezone(timedelta(hours=8))).isoformat()
        self.snapshot = {
            "security": "511880.XSHG",
            "sid": "511880.XSHG",
            "source": HUAXIN_XMD_SOURCE,
            "provider": HUAXIN_XMD_SOURCE,
            "source_time": now,
            "received_time": now,
            "age_seconds": 0.0,
            "last_price": 100.705,
            "price": 100.705,
            "bid_price1": 100.704,
            "ask_price1": 100.705,
            "bid_volume1": 100,
            "ask_volume1": 200,
        }

    def start(self):
        """标记 fake backend 已启动。

        Returns:
            None。
        """

        self.started = True

    def stop(self):
        """标记 fake backend 已停止。

        Returns:
            None。
        """

        self.stopped = True
        self.started = False

    def subscribe(self, security):
        """记录订阅并返回成功确认。

        Args:
            security: 标准证券代码。

        Returns:
            dict: 订阅成功响应。
        """

        self.subscriptions.add(security)
        return {"type": "response", "op": "subscribe", "ok": True, "active": True}

    def unsubscribe(self, security):
        """删除订阅并返回成功确认。

        Args:
            security: 标准证券代码。

        Returns:
            dict: 退订成功响应。
        """

        self.subscriptions.discard(security)
        return {"type": "response", "op": "unsubscribe", "ok": True, "active": False}

    def get_latest(self, security, wait_timeout=0.0):
        """返回当前内存快照。

        Args:
            security: 标准证券代码。
            wait_timeout: 测试中忽略的等待秒数。

        Returns:
            dict: 快照副本。
        """

        del wait_timeout
        assert security in self.subscriptions
        return dict(self.snapshot)

    def health(self):
        """返回 fake XMD 就绪状态。

        Returns:
            dict: 脱敏 health。
        """

        return {
            "ready": self.started,
            "state": "ready" if self.started else "unavailable",
            "source": HUAXIN_XMD_SOURCE,
            "subscriptions": len(self.subscriptions),
        }


def _xmd_server_config(**overrides):
    """构造启用 Huaxin XMD、禁用 Trader 的测试配置。

    Args:
        **overrides: ServerConfig 字段覆盖。

    Returns:
        ServerConfig: data-only 华鑫配置。
    """

    values = {
        "server_type": "huaxin",
        "listen": "127.0.0.1",
        "enable_data": True,
        "enable_broker": False,
        "accounts": [],
        "huaxin_xmd_backend": "python37_sidecar",
        "huaxin_xmd_python": "/private/huaxin37/bin/python",
        "huaxin_xmd_sdk_dir": "/private/xmd-sdk",
        "huaxin_xmd_front": _TEST_XMD_FRONT,
        "huaxin_xmd_max_age_seconds": DEFAULT_MAX_AGE_SECONDS,
        "huaxin_xmd_snapshot_timeout": 1.0,
    }
    values.update(overrides)
    return ServerConfig(**values)


@pytest.mark.asyncio
async def test_huaxin_data_adapter_returns_only_fresh_xmd_snapshot() -> None:
    """验证 snapshot/live_current/current_tick 共享同一华鑫新鲜来源合同。"""

    adapter = HuaxinDataAdapter(
        _xmd_server_config(),
        backend_factory=_FakeXmdBackend,
    )
    await adapter.start()

    snapshot = await adapter.get_snapshot({"security": "511880.XSHG"})
    live_current = await adapter.get_live_current({"security": "511880.XSHG"})
    current_tick = await adapter.get_current_tick("511880.XSHG")

    assert snapshot["source"] == HUAXIN_XMD_SOURCE
    assert snapshot["bid_price1"] == 100.704
    assert live_current["ask_price1"] == 100.705
    assert current_tick["security"] == "511880.XSHG"
    assert adapter.backend_status()["actions"]["data.history"]["status"] == "unsupported"
    assert not hasattr(adapter, "get_history")
    await adapter.stop()
    assert adapter._backend.stopped is True


@pytest.mark.asyncio
async def test_huaxin_data_adapter_rejects_wrong_source_and_stale_cache() -> None:
    """验证错误来源和过期缓存都不会进入 server 实时数据接口。"""

    adapter = HuaxinDataAdapter(
        _xmd_server_config(huaxin_xmd_max_age_seconds=1.0),
        backend_factory=_FakeXmdBackend,
    )
    await adapter.start()
    adapter._backend.snapshot["source"] = "miniqmt"
    with pytest.raises(XmdBackendError, match="不是华鑫 XMD"):
        await adapter.get_snapshot({"security": "511880.XSHG"})

    stale = datetime.now(timezone(timedelta(hours=8))) - timedelta(seconds=5)
    adapter._backend.snapshot.update(
        {
            "source": HUAXIN_XMD_SOURCE,
            "source_time": stale.isoformat(),
            "received_time": stale.isoformat(),
        }
    )
    with pytest.raises(XmdBackendError, match="超过允许时效"):
        await adapter.get_live_current({"security": "511880.XSHG"})
    await adapter.stop()


@pytest.mark.asyncio
async def test_xmd_adapter_keeps_server_alive_when_initial_front_is_closed() -> None:
    """验证初次 XMD 登录超时不会阻止 Server adapter 常驻并在前置恢复后 ready。"""

    created = []

    class _FirstUnavailableBackend(_FakeXmdBackend):
        """仅第一套 backend 模拟前置关闭的替身。"""

        def __init__(self, *args, fail_start=False, **kwargs):
            """保存本实例是否模拟首次启动失败。

            Args:
                *args: Fake backend 位置参数。
                fail_start: 是否抛出登录就绪超时。
                **kwargs: Fake backend 关键字参数。

            Returns:
                None。
            """

            super().__init__(*args, **kwargs)
            self.fail_start = bool(fail_start)

        def start(self):
            """第一套返回可恢复超时，后续实例正常启动。

            Returns:
                None。

            Raises:
                XmdBackendError: fail_start=True 时模拟前置未开放。
            """

            if self.fail_start:
                raise XmdBackendError("sidecar_ready_timeout", "仿真前置暂未开放")
            super().start()

    def _factory(**kwargs):
        """首个 backend 注入可恢复失败，重建实例恢复正常。

        Args:
            **kwargs: adapter 传入的 backend 配置。

        Returns:
            _FirstUnavailableBackend: 新 backend。
        """

        backend = _FirstUnavailableBackend(fail_start=not created, **kwargs)
        created.append(backend)
        return backend

    adapter = HuaxinDataAdapter(_xmd_server_config(), backend_factory=_factory)
    adapter._watchdog_interval_seconds = 0.01

    await adapter.start()
    assert adapter._state == "degraded"
    await _wait_for_condition(lambda: len(created) == 2 and adapter._state == "ready")

    assert adapter.backend_status()["ready"] is True
    assert adapter.backend_status()["reconnect_count"] == 1
    await adapter.stop()


@pytest.mark.asyncio
async def test_xmd_adapter_rebuilds_after_ready_disconnect_and_restores_subscription() -> None:
    """验证 XMD ready 后 sidecar 失活时快速失败，并重建订阅与新鲜快照。"""

    reconnect_gate = threading.Event()
    created = []

    class _BlockingReconnectBackend(_FakeXmdBackend):
        """让第二套 backend 在 start 中等待测试释放的替身。"""

        def __init__(self, *args, block_start=False, **kwargs):
            """保存是否阻塞本次启动。

            Args:
                *args: Fake backend 位置参数。
                block_start: 是否等待 reconnect_gate。
                **kwargs: Fake backend 关键字参数。

            Returns:
                None。
            """

            super().__init__(*args, **kwargs)
            self.block_start = bool(block_start)

        def start(self):
            """第二次启动等待主测试检查快速失败语义。

            Returns:
                None。
            """

            if self.block_start:
                reconnect_gate.wait(1.0)
            super().start()

    def _factory(**kwargs):
        """依次创建首个和阻塞中的重连 backend。

        Args:
            **kwargs: adapter 传入的 backend 配置。

        Returns:
            _BlockingReconnectBackend: 新 backend。
        """

        backend = _BlockingReconnectBackend(block_start=bool(created), **kwargs)
        created.append(backend)
        return backend

    adapter = HuaxinDataAdapter(_xmd_server_config(), backend_factory=_factory)
    adapter._watchdog_interval_seconds = 0.01
    await adapter.start()
    await adapter.get_snapshot({"security": "511880.XSHG"})
    assert "511880.XSHG" in adapter._subscriptions

    created[0].started = False
    await _wait_for_condition(lambda: adapter._state == "reconnecting")
    with pytest.raises(XmdBackendError, match="正在恢复"):
        await adapter.get_snapshot({"security": "511880.XSHG"})

    reconnect_gate.set()
    await _wait_for_condition(lambda: len(created) == 2 and adapter._state == "ready")

    assert created[0].stopped is True
    assert created[1].subscriptions == {"511880.XSHG"}
    assert adapter.backend_status()["reconnect_count"] == 1
    restored = await adapter.get_snapshot({"security": "511880.XSHG"})
    assert restored["source"] == HUAXIN_XMD_SOURCE
    await adapter.stop()
    assert adapter._state == "stopped"
    assert adapter._watchdog_task is None


def test_xmd_tick_normalization_preserves_exchange_time_and_order_book() -> None:
    """验证 sidecar tick 归一后保留北京时间、盘口和唯一华鑫来源。"""

    now_dt = datetime(2026, 8, 17, 14, 33, 51, 779000, tzinfo=timezone(timedelta(hours=8)))
    payload = {
        "type": "tick",
        "security": "511880",
        "exchange": "SSE",
        "TradingDay": "20260817",
        "UpdateTime": "14:33:51",
        "Millisec": 779,
        "Last": 100.705,
        "Bid1": 100.704,
        "Ask1": 100.705,
        "BidVolume1": 100,
        "AskVolume1": 200,
        "UpperLimit": 110.0,
        "LowerLimit": 90.0,
        "Volume": 300,
        "Turnover": 400.5,
        "receive_ns": int(now_dt.timestamp() * 1_000_000_000),
    }

    snapshot = _normalise_tick(payload, now=now_dt.timestamp())

    assert snapshot["security"] == "511880.XSHG"
    assert snapshot["source"] == HUAXIN_XMD_SOURCE
    assert snapshot["source_time"].endswith("+08:00")
    assert snapshot["time_basis"] == "exchange_time"
    assert snapshot["bid_price1"] == 100.704
    assert snapshot["ask_price1"] == 100.705
    assert snapshot["age_seconds"] == pytest.approx(0.0)


def test_xmd_tick_simulation_replay_uses_receive_time_for_artificial_trading_day() -> None:
    """验证 7×24 仿真人工交易日仅在显式开关下改用接收时间。"""

    now_dt = datetime(2026, 8, 22, 14, 3, 0, tzinfo=timezone(timedelta(hours=8)))
    payload = {
        "type": "tick",
        "security": "511880",
        "exchange": "SSE",
        "TradingDay": "20450424",
        "UpdateTime": "09:53:00",
        "Millisec": 0,
        "Last": 100.705,
        "Bid1": 100.704,
        "Ask1": 100.705,
        "BidVolume1": 100,
        "AskVolume1": 200,
        "UpperLimit": 110.0,
        "LowerLimit": 90.0,
        "Volume": 300,
        "Turnover": 400.5,
        "receive_ns": int(now_dt.timestamp() * 1_000_000_000),
    }

    with pytest.raises(XmdBackendError, match="明显晚于本机时间"):
        _normalise_tick(payload, now=now_dt.timestamp())

    snapshot = _normalise_tick(
        payload,
        now=now_dt.timestamp(),
        simulation_replay=True,
    )

    assert snapshot["trading_day"] == "20450424"
    assert snapshot["update_time"] == "09:53:00"
    assert snapshot["time_basis"] == "receive_time_simulation_replay"
    assert snapshot["source_time"] == snapshot["received_time"]
    assert snapshot["age_seconds"] == pytest.approx(0.0)


def test_python37_xmd_backend_consumes_frozen_jsonl_protocol(tmp_path) -> None:
    """验证 parent backend 以固定 argv 消费 ready/response/tick/stop 协议。

    Args:
        tmp_path: pytest 提供的隔离临时目录。

    Returns:
        None。
    """

    sdk_dir = tmp_path / "sdk"
    sdk_dir.mkdir()
    sidecar = tmp_path / "fake_xmd_sidecar.py"
    sidecar.write_text(
        textwrap.dedent(
            """
            import datetime
            import json
            import sys
            import time

            if sys.argv[-2:] != ["--front", "tcp://127.0.0.1:9402"]:
                raise SystemExit(3)

            def emit(value):
                print(json.dumps(value, separators=(",", ":")), flush=True)

            emit({
                "type": "ready",
                "api_version": "fake-1",
                "running": True,
                "connected": True,
                "logged_in": True,
                "released": False,
                "queue_capacity": 10,
                "queue_size": 0,
                "dropped_events": 0,
            })
            for line in sys.stdin:
                command = json.loads(line)
                response = {
                    "type": "response",
                    "request_id": command["request_id"],
                    "op": command["op"],
                    "ok": True,
                }
                if command["op"] == "subscribe":
                    response.update({
                        "security": command["security"],
                        "exchange": command["exchange"],
                        "active": True,
                    })
                    emit(response)
                    zone = datetime.timezone(datetime.timedelta(hours=8))
                    now = datetime.datetime.now(zone)
                    emit({
                        "type": "tick",
                        "security": command["security"],
                        "exchange": command["exchange"],
                        "TradingDay": now.strftime("%Y%m%d"),
                        "UpdateTime": now.strftime("%H:%M:%S"),
                        "Millisec": now.microsecond // 1000,
                        "Last": 100.705,
                        "Bid1": 100.704,
                        "Ask1": 100.705,
                        "BidVolume1": 100,
                        "AskVolume1": 200,
                        "UpperLimit": 110.0,
                        "LowerLimit": 90.0,
                        "Volume": 300,
                        "Turnover": 400.5,
                        "receive_ns": time.time_ns(),
                    })
                elif command["op"] == "unsubscribe":
                    response.update({
                        "security": command["security"],
                        "exchange": command["exchange"],
                        "active": False,
                    })
                    emit(response)
                else:
                    emit(response)
                if command["op"] == "stop":
                    break
            """
        ),
        encoding="utf-8",
    )
    backend = Python37XmdBackend(
        python_path=sys.executable,
        sdk_dir=str(sdk_dir),
        front=_TEST_XMD_FRONT,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS,
        connect_timeout=2.0,
        command_timeout=2.0,
        sidecar_path=str(sidecar),
    )

    backend.start()
    receipt = backend.subscribe("511880.XSHG")
    snapshot = backend.get_latest("511880.XSHG", wait_timeout=2.0)

    assert receipt["active"] is True
    assert snapshot["source"] == HUAXIN_XMD_SOURCE
    assert snapshot["last_price"] == 100.705
    assert backend.health()["ready"] is True
    backend.stop()
    assert backend.health()["process_alive"] is False


def test_huaxin_health_keeps_trader_and_xmd_readiness_separate() -> None:
    """验证组合 health 不以 XMD 就绪替代 Trader readiness，反之亦然。"""

    merged = ServerApplication._merge_huaxin_backend_statuses(
        [
            {
                "backend_type": "huaxin",
                "component": "trader",
                "ready": False,
                "state": "degraded",
                "reason": "baseline_query_pending",
                "actions": {"broker.account": {"status": "degraded"}},
            },
            {
                "backend_type": "huaxin",
                "component": "xmd_l1",
                "ready": True,
                "state": "ready",
                "source": HUAXIN_XMD_SOURCE,
                "actions": {"data.live_current": {"status": "ready"}},
            },
        ]
    )

    assert merged["ready"] is False
    assert merged["state"] == "degraded"
    assert merged["modules"]["trader"]["ready"] is False
    assert merged["modules"]["xmd_l1"]["ready"] is True
    assert merged["actions"]["data.live_current"]["status"] == "ready"


@pytest.mark.asyncio
async def test_huaxin_write_path_requires_explicit_price_without_data_fallback() -> None:
    """验证华鑫下单缺价格时在 server 入口拒绝，且不会触发快照或历史补价。"""

    class StrictData:
        """记录所有潜在补价调用的严格实时 adapter 替身。"""

        requires_explicit_execution_price = True

        def __init__(self):
            """初始化调用计数。

            Returns:
                None。
            """

            self.snapshot_calls = 0
            self.history_calls = 0

        async def get_snapshot(self, payload):
            """记录快照调用并返回伪行情。

            Args:
                payload: 测试请求。

            Returns:
                dict: 伪行情。
            """

            del payload
            self.snapshot_calls += 1
            return {"last_price": 100.0}

        async def get_history(self, payload):
            """记录历史调用并返回伪分钟线。

            Args:
                payload: 测试请求。

            Returns:
                dict: 伪历史记录。
            """

            del payload
            self.history_calls += 1
            return {"records": [[100.0]]}

    data = StrictData()
    config = _xmd_server_config()
    app = ServerApplication(
        config,
        AccountRouter(config.accounts),
        AdapterBundle(data, None, False),
    )

    with pytest.raises(ValueError, match="HUAXIN_EXECUTION_PRICE_REQUIRED"):
        await app._maybe_fill_price(
            {
                "security": "511880.XSHG",
                "amount": 100,
                "side": "SELL",
                "style": {"type": "limit"},
            }
        )
    assert data.snapshot_calls == 0
    assert data.history_calls == 0

    await app._maybe_fill_price(
        {
            "security": "511880.XSHG",
            "amount": 100,
            "side": "SELL",
            "style": {"type": "limit", "price": 100.7},
        }
    )
    assert data.snapshot_calls == 0
    assert data.history_calls == 0


@pytest.mark.asyncio
async def test_huaxin_write_path_rejects_missing_or_wrong_source_xmd_snapshot() -> None:
    """验证华鑫权威实时行情缺失或来源错误时不会继续下单前检查。

    Returns:
        None: 两种错误均在 broker 调用前抛出稳定拒绝。
    """

    class BrokenAuthoritativeData:
        """模拟无法返回华鑫 XMD 快照的权威实时 adapter。"""

        authoritative_realtime_only = True

        async def get_live_current(self, payload):
            """模拟实时查询失败。

            Args:
                payload: 当前证券请求。

            Raises:
                RuntimeError: 固定模拟 XMD 不可用。
            """

            del payload
            raise RuntimeError("xmd unavailable")

    config = _xmd_server_config()
    missing_app = ServerApplication(
        config,
        AccountRouter(config.accounts),
        AdapterBundle(BrokenAuthoritativeData(), None, False),
    )
    with pytest.raises(ValueError, match="HUAXIN_XMD_SNAPSHOT_REQUIRED"):
        await missing_app._maybe_reject_when_paused(
            {"security": "511880.XSHG", "style": {"type": "limit", "price": 100.7}}
        )

    class WrongSourceData:
        """模拟错误来源但字段形状正常的实时 adapter。"""

        authoritative_realtime_only = True

        async def get_live_current(self, payload):
            """返回标记为 MiniQMT 的伪实时快照。

            Args:
                payload: 当前证券请求。

            Returns:
                dict: 故意使用错误 source 的快照。
            """

            del payload
            return {"source": "windows_miniqmt_xtdata", "last_price": 100.7}

    wrong_source_app = ServerApplication(
        config,
        AccountRouter(config.accounts),
        AdapterBundle(WrongSourceData(), None, False),
    )
    with pytest.raises(ValueError, match="HUAXIN_XMD_SOURCE_INVALID"):
        await wrong_source_app._maybe_reject_when_paused(
            {"security": "511880.XSHG", "style": {"type": "limit", "price": 100.7}}
        )


@pytest.mark.asyncio
async def test_server_starts_and_stops_data_adapter_independently() -> None:
    """验证 ServerApplication 会独立管理 data adapter 生命周期。"""

    class LifecycleData:
        """记录 server 生命周期调用的 data adapter 替身。"""

        def __init__(self):
            """初始化生命周期标记。

            Returns:
                None。
            """

            self.started = False
            self.stopped = False

        async def start(self):
            """记录 start 调用。

            Returns:
                None。
            """

            self.started = True

        async def stop(self):
            """记录 stop 调用。

            Returns:
                None。
            """

            self.stopped = True

        async def get_current_tick(self, security):
            """提供 tick manager 所需的空快照合同。

            Args:
                security: 标准证券代码。

            Returns:
                dict: 空快照。
            """

            del security
            return {}

    data = LifecycleData()
    config = _xmd_server_config()
    app = ServerApplication(
        config,
        AccountRouter(config.accounts),
        AdapterBundle(data, None, False),
    )

    await app._start_components()
    assert data.started is True
    await app.shutdown()
    assert data.stopped is True


class _FakeBroker:
    """记录 adapter 查询、限价和完整撤单载荷的 broker 替身。"""

    def __init__(self, account_id, account_type, config):
        """保存构造字段并初始化调用记录。

        Args:
            account_id: 账户标识。
            account_type: 账户类型。
            config: 华鑫配置。

        Returns:
            None。
        """

        self.account_id = account_id
        self.account_type = account_type
        self.config = config
        self.place_payload = None
        self.place_call_count = 0
        self.cancel_payload = None
        self.connected = False
        self.baseline_queries = set()

    def connect(self):
        """标记已连接。

        Returns:
            bool: True。
        """

        self.connected = True
        self.baseline_queries.update({"account", "positions", "orders", "trades"})
        return True

    def disconnect(self):
        """标记已断开。

        Returns:
            bool: True。
        """

        self.connected = False
        return True

    def get_account_info(self):
        """返回测试资金。

        Returns:
            dict: 测试资金。
        """

        self.baseline_queries.add("account")
        return {"available_cash": 1000}

    def get_positions(self):
        """返回测试持仓。

        Returns:
            list: 测试持仓。
        """

        self.baseline_queries.add("positions")
        return [{"security": "511880.XSHG", "amount": 100}]

    def get_orders(self, **kwargs):
        """返回测试委托并忽略过滤。

        Args:
            **kwargs: 公共过滤参数。

        Returns:
            list: 测试委托。
        """

        self.baseline_queries.add("orders")
        return [{"order_id": kwargs.get("order_id") or "O1", "status": "open"}]

    def get_trades(self, **kwargs):
        """返回测试成交。

        Args:
            **kwargs: 公共过滤参数。

        Returns:
            list: 测试成交。
        """

        self.baseline_queries.add("trades")
        return [{"trade_id": "T1", "order_id": kwargs.get("order_id") or "O1"}]

    def get_trading_day(self):
        """返回固定的柜台权威交易日。

        Returns:
            str: ``20260824``。
        """

        return "20260824"

    def get_security_master(self, security):
        """返回固定的证券主数据。

        Args:
            security: 标准证券代码。

        Returns:
            dict: 含证券代码、跳价和涨跌停价的测试主数据。
        """

        return {
            "security": security,
            "price_tick": 0.001,
            "upper_limit_price": 101.0,
            "lower_limit_price": 99.0,
        }

    def submit_order(self, direction, security, amount, price, **kwargs):
        """记录 adapter 透传的限价或显式市价字段。

        Args:
            direction: 买卖方向。
            security: 标准代码。
            amount: 数量。
            price: 限价或保护价。
            **kwargs: extra、超时等扩展。

        Returns:
            dict: submit_unknown 测试响应。
        """

        self.place_payload = (direction, security, amount, price, kwargs)
        self.place_call_count += 1
        return {"order_id": "O1", "status": "submit_unknown"}

    def submit_cancel_order(self, order_id, payload, **kwargs):
        """记录完整撤单载荷。

        Args:
            order_id: 订单号。
            payload: 完整请求。
            **kwargs: 超时扩展。

        Returns:
            dict: submit_unknown 测试响应。
        """

        self.cancel_payload = (order_id, dict(payload), kwargs)
        return {"order_id": order_id, "status": "submit_unknown"}

    def health_snapshot(self):
        """返回全 readiness 测试 health。

        Returns:
            dict: readiness 字段。
        """

        return {
            "ready_for_queries": self.connected,
            "ready_for_new_orders": self.connected,
            "ready_for_cancel": self.connected,
            "trading_enabled": True,
            "cancel_order_enabled": True,
            "order_ref_allocator_ready": True,
            "order_identity_ready": True,
            "security_order_constraints_ready": True,
            "baseline_query_ready": self.baseline_queries
            == {
                "account",
                "positions",
                "orders",
                "trades",
            },
        }


class _GateCoordinator:
    """提供 Adapter 生命周期与双门禁测试的内存归集协调器。"""

    def __init__(self, config, source_snapshot_provider=None):
        """保存配置并默认阻断新下单。

        Args:
            config: 归集配置。
            source_snapshot_provider: 测试中未使用的源快照注入。

        Returns:
            None。
        """

        del source_snapshot_provider
        self.config = config
        self.poll_seconds = 0.01
        self.drive_count = 0
        self.allowed = False

    def order_allowed(self):
        """返回当前下单门禁。

        Returns:
            bool: allowed 测试状态。
        """

        return self.allowed

    def drive_once(self, broker):
        """记录后台协调器已经独立执行。

        Args:
            broker: Adapter 默认 Broker。

        Returns:
            dict: 脱敏归集健康摘要。
        """

        del broker
        self.drive_count += 1
        return self.health_snapshot()

    def record_runtime_error(self, exc):
        """记录测试后台异常类型。

        Args:
            exc: 后台异常。

        Returns:
            None。
        """

        self.error_type = type(exc).__name__

    def health_snapshot(self):
        """返回不含私密身份的归集状态。

        Returns:
            dict: observing 健康摘要。
        """

        return {
            "enabled": True,
            "mode": self.config.mode,
            "state": "observing",
            "reason": "stable_samples_pending",
            "trading_day": "20260825",
            "action_count": 0,
            "action_states": {},
            "updated_at": None,
        }

    def blocked_order_result(self):
        """返回稳定的新下单阻断结果。

        Returns:
            dict: rejected 响应。
        """

        return {
            "value": False,
            "status": "rejected",
            "submission_state": "rejected",
            "reason": HUAXIN_ASSET_CONSOLIDATION_ORDER_BLOCKED,
        }


@pytest.mark.asyncio
async def test_adapter_delegates_queries_and_preserves_cancel_payload() -> None:
    """验证 adapter 对查询、限价/显式市价和撤单幂等载荷的精确透传。"""

    config = _server_config()
    router = AccountRouter(config.accounts)
    adapter = HuaxinBrokerAdapter(
        config,
        router,
        broker_config={"enable_trading": True, "enable_cancel": True},
        broker_factory=_FakeBroker,
    )
    await adapter.start()
    ctx = router.get("default")

    assert adapter.backend_status()["state"] == "ready"
    assert adapter.backend_status()["actions"]["broker.place_order"]["status"] == "ready"
    assert (await adapter.get_account_info(ctx))["value"]["available_cash"] == 1000
    assert (await adapter.get_positions(ctx))[0]["amount"] == 100
    assert (await adapter.list_orders(ctx))[0]["order_id"] == "O1"
    assert (await adapter.list_trades(ctx))[0]["trade_id"] == "T1"
    assert await adapter.get_trading_day() == "20260824"
    assert (await adapter.get_security_info("511880.XSHG"))["price_tick"] == 0.001
    place = await adapter.place_order(
        ctx,
        {
            "security": "511880.XSHG",
            "side": "BUY",
            "amount": 100,
            "style": {"type": "limit", "price": 100.2},
            "idempotency_key": "place-1",
        },
    )
    assert place["order_id"] == "O1"
    broker = adapter._brokers["default"]
    assert "order_identity_journal_path" not in broker.config
    assert broker.place_payload[4]["market"] is False
    assert broker.place_payload[4]["extra"]["idempotency_key"] == "place-1"

    market_place = await adapter.place_order(
        ctx,
        {
            "security": "511880.XSHG",
            "side": "SELL",
            "amount": 100,
            "style": {
                "type": "market",
                "market_type": "opponent_best",
                "protect_price": 99.9,
            },
            "idempotency_key": "place-market-1",
        },
    )
    assert market_place["order_id"] == "O1"
    assert broker.place_payload[:4] == ("sell", "511880.XSHG", 100, 99.9)
    assert broker.place_payload[4]["market"] is True
    assert broker.place_payload[4]["extra"]["market_type"] == "opponent_best"

    cancel_payload = {
        "order_id": "SYS1",
        "idempotency_key": "cancel-1",
        "provider_extension": {"huaxin_tora": {"exchange": "SSE", "order_sys_id": "SYS1"}},
    }
    await adapter.cancel_order_request(ctx, cancel_payload)
    assert broker.cancel_payload[1] == cancel_payload
    assert adapter.backend_status()["actions"]["broker.place_order"]["status"] == "ready"
    await adapter.stop()


@pytest.mark.asyncio
async def test_asset_consolidation_runs_in_background_and_blocks_only_new_orders(tmp_path) -> None:
    """验证归集后台任务不阻塞启动，查询和精确撤单保留而新下单被拒绝。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    config = _server_config()
    router = AccountRouter(config.accounts)
    consolidation_config = HuaxinAssetConsolidationConfig.from_mapping(
        {
            "mode": "full",
            "source_mode": "external_snapshot",
            "source_snapshot_path": tmp_path / "source.json",
            "state_path": tmp_path / "state.json",
            "source_node_id": 22,
            "target_node_id": 11,
            "source_role": "source-query",
            "target_role": "target-writer",
            "source_host": "source-host",
            "target_host": "target-host",
        }
    )
    adapter = HuaxinBrokerAdapter(
        config,
        router,
        broker_config={"enable_trading": True, "enable_cancel": True},
        broker_factory=_FakeBroker,
        consolidation_config=consolidation_config,
        consolidation_factory=_GateCoordinator,
    )

    await adapter.start()
    context = router.get("default")
    account = await adapter.get_account_info(context)
    blocked = await adapter.place_order(
        context,
        {
            "security": "511880.XSHG",
            "side": "BUY",
            "amount": 100,
            "style": {"type": "limit", "price": 100.2},
        },
    )
    cancel = await adapter.cancel_order_request(context, {"order_id": "SYS1"})
    health = adapter.backend_status()

    assert account["value"]["available_cash"] == 1000
    assert blocked["reason"] == HUAXIN_ASSET_CONSOLIDATION_ORDER_BLOCKED
    assert cancel["order_id"] == "SYS1"
    assert adapter._brokers["default"].place_call_count == 0
    assert adapter._brokers["default"].cancel_payload is not None
    assert health["ready"] is True
    assert health["actions"]["broker.place_order"]["status"] == "unavailable"
    assert health["actions"]["broker.cancel_order"]["status"] == "ready"
    assert health["asset_consolidation"]["state"] == "observing"
    assert adapter._consolidation_task is not None
    await adapter.stop()
    assert adapter._consolidation_task is None


@pytest.mark.asyncio
async def test_asset_consolidation_second_gate_prevents_executor_race() -> None:
    """验证快速门禁后状态关闭时，executor 内二次门禁仍阻止 native 下单。"""

    class _RaceGate(_GateCoordinator):
        """第一次允许、第二次拒绝以模拟入队后的归集状态竞争。"""

        def __init__(self):
            """初始化门禁调用计数。

            Returns:
                None。
            """

            self.calls = 0
            self.config = type("Config", (), {"mode": "full"})()

        def order_allowed(self):
            """仅第一次快速检查返回允许。

            Returns:
                bool: 首次 True，后续 False。
            """

            self.calls += 1
            return self.calls == 1

    config = _server_config()
    router = AccountRouter(config.accounts)
    adapter = HuaxinBrokerAdapter(
        config,
        router,
        broker_config={"enable_trading": True},
        broker_factory=_FakeBroker,
        consolidation_config=HuaxinAssetConsolidationConfig(),
    )
    await adapter.start()
    adapter._asset_consolidation = _RaceGate()

    result = await adapter.place_order(
        router.get("default"),
        {
            "security": "511880.XSHG",
            "side": "BUY",
            "amount": 100,
            "style": {"type": "limit", "price": 100.2},
        },
    )

    assert result["reason"] == HUAXIN_ASSET_CONSOLIDATION_ORDER_BLOCKED
    assert adapter._brokers["default"].place_call_count == 0
    await adapter.stop()


@pytest.mark.asyncio
async def test_asset_consolidation_off_creates_no_coordinator_task_or_state_file(tmp_path) -> None:
    """验证显式 off 不创建协调器任务，也不读写任何归集状态文件。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        None。
    """

    config = _server_config()
    router = AccountRouter(config.accounts)
    adapter = HuaxinBrokerAdapter(
        config,
        router,
        broker_config={"enable_trading": True},
        broker_factory=_FakeBroker,
        consolidation_config=HuaxinAssetConsolidationConfig(),
    )

    await adapter.start()

    assert adapter._asset_consolidation is None
    assert adapter._consolidation_task is None
    assert adapter.backend_status()["asset_consolidation"]["state"] == "off"
    assert list(tmp_path.iterdir()) == []
    await adapter.stop()


@pytest.mark.asyncio
async def test_huaxin_data_calendar_and_security_info_delegate_to_trader() -> None:
    """验证统一数据协议把交易日与证券信息委托给同一 Trader 会话。"""

    config = _xmd_server_config(
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="acct")],
    )
    router = AccountRouter(config.accounts)
    broker_adapter = HuaxinBrokerAdapter(
        config,
        router,
        broker_config={"enable_trading": True, "enable_cancel": True},
        broker_factory=_FakeBroker,
    )
    data_adapter = HuaxinDataAdapter(
        config,
        backend_factory=_FakeXmdBackend,
        broker_adapter=broker_adapter,
    )
    await broker_adapter.start()
    await data_adapter.start()

    trade_days = await data_adapter.get_trade_days(
        {"start_date": "2026-08-24", "end_date": "2026-08-24"}
    )
    outside = await data_adapter.get_trade_days(
        {"start_date": "2026-08-22", "end_date": "2026-08-22"}
    )
    security_info = await data_adapter.get_security_info({"security": "511880.XSHG"})

    assert trade_days == {"dtype": "list", "values": ["2026-08-24"]}
    assert outside == {"dtype": "list", "values": []}
    assert security_info["value"]["security"] == "511880.XSHG"
    assert security_info["value"]["price_tick"] == 0.001
    assert data_adapter.backend_status()["actions"]["data.trade_days"]["status"] == "ready"
    await data_adapter.stop()
    await broker_adapter.stop()


@pytest.mark.asyncio
async def test_huaxin_server_memory_idempotency_requires_key_without_sqlite() -> None:
    """验证华鑫写只使用进程内幂等，未知态同键不会再次调用 native adapter。"""

    config = _server_config()
    router = AccountRouter(config.accounts)
    adapter = HuaxinBrokerAdapter(
        config,
        router,
        broker_config={"enable_trading": True, "enable_cancel": True},
        broker_factory=_FakeBroker,
    )
    await adapter.start()
    app = ServerApplication(
        config,
        router,
        AdapterBundle(
            data_adapter=None,
            broker_adapter=adapter,
            broker_writes_require_persistent_idempotency=False,
            broker_writes_require_idempotency_key=True,
        ),
    )
    session = SimpleNamespace(account_key="default", sub_account_id=None)
    payload = {
        "security": "511880.XSHG",
        "side": "BUY",
        "amount": 100,
        "style": {"type": "limit", "price": 100.2},
        "idempotency_key": "memory-only-unknown",
    }

    first = await app._dispatch_broker(session, "place_order", dict(payload))
    repeated = await app._dispatch_broker(session, "place_order", dict(payload))

    assert first["status"] == repeated["status"] == "submit_unknown"
    assert adapter._brokers["default"].place_call_count == 1
    assert app._idempotency_journal is None
    missing_key = dict(payload)
    missing_key.pop("idempotency_key")
    with pytest.raises(ValueError, match="idempotency_key"):
        await app._dispatch_broker(session, "place_order", missing_key)
    assert adapter._brokers["default"].place_call_count == 1
    await adapter.stop()


@pytest.mark.asyncio
async def test_broker_adapter_rebuilds_after_ready_disconnect_without_replaying_writes() -> None:
    """验证 Trader ready 后断线会快速失败并在同一 executor 重建，不重放写请求。"""

    reconnect_gate = threading.Event()
    created = []

    class _BlockingReconnectBroker(_FakeBroker):
        """让第二套 Broker 在 connect 中等待测试释放的替身。"""

        def __init__(self, *args, block_connect=False, **kwargs):
            """保存是否阻塞本次连接。

            Args:
                *args: FakeBroker 位置参数。
                block_connect: 是否等待 reconnect_gate。
                **kwargs: FakeBroker 关键字参数。

            Returns:
                None。
            """

            super().__init__(*args, **kwargs)
            self.block_connect = bool(block_connect)

        def connect(self):
            """第二次连接等待主测试确认重连期间已快速失败。

            Returns:
                bool: 最终复用父类成功结果。
            """

            if self.block_connect:
                reconnect_gate.wait(1.0)
            return super().connect()

    def _factory(**kwargs):
        """依次创建首个和阻塞中的重连 Broker。

        Args:
            **kwargs: adapter 传入的 Broker 构造参数。

        Returns:
            _BlockingReconnectBroker: 新 Broker。
        """

        broker = _BlockingReconnectBroker(block_connect=bool(created), **kwargs)
        created.append(broker)
        return broker

    config = _server_config()
    router = AccountRouter(config.accounts)
    adapter = HuaxinBrokerAdapter(
        config,
        router,
        broker_config={"enable_trading": True, "enable_cancel": True},
        broker_factory=_factory,
    )
    adapter._watchdog_interval_seconds = 0.01
    await adapter.start()
    assert adapter.backend_status()["state"] == "ready"

    created[0].connected = False
    await _wait_for_condition(lambda: adapter._state == "reconnecting")
    with pytest.raises(HuaxinNativeUnavailableError, match="正在恢复"):
        await adapter.get_account_info(router.get("default"))

    reconnect_gate.set()
    await _wait_for_condition(lambda: len(created) == 2 and adapter._state == "ready")

    assert created[0].connected is False
    assert created[1].connected is True
    assert created[0].place_payload is None and created[1].place_payload is None
    assert created[0].cancel_payload is None and created[1].cancel_payload is None
    assert adapter.backend_status()["reconnect_count"] == 1
    await adapter.stop()
    assert adapter._state == "stopped"
    assert adapter._watchdog_task is None


@pytest.mark.asyncio
async def test_adapter_rejects_market_without_explicit_type_before_broker_call() -> None:
    """验证远程市价缺少 market_type 时不会调用 HuaxinBroker。"""

    config = _server_config()
    router = AccountRouter(config.accounts)
    adapter = HuaxinBrokerAdapter(
        config,
        router,
        broker_config={"enable_trading": True},
        broker_factory=_FakeBroker,
    )
    await adapter.start()

    with pytest.raises(HuaxinTradingDisabledError, match="market_type"):
        await adapter.place_order(
            router.get("default"),
            {
                "security": "511880.XSHG",
                "side": "SELL",
                "amount": 100,
                "style": {"type": "market", "protect_price": 99.9},
                "idempotency_key": "missing-market-type",
            },
        )

    assert adapter._brokers["default"].place_payload is None
    await adapter.stop()


def test_market_type_participates_in_server_idempotency_fingerprint() -> None:
    """验证同一幂等键的本方最优与对手方最优具有不同写入指纹。"""

    base = {
        "account_key": "default",
        "security": "511880.XSHG",
        "side": "SELL",
        "amount": 100,
        "idempotency_key": "same-key",
        "style": {"type": "market", "protect_price": 99.9},
    }
    home_best = dict(base)
    home_best["style"] = dict(base["style"], market_type="home_best")
    opponent_best = dict(base)
    opponent_best["style"] = dict(base["style"], market_type="opponent_best")

    assert _build_write_fingerprint("broker.place_order", home_best) != _build_write_fingerprint(
        "broker.place_order", opponent_best
    )
