"""验证 Huaxin server adapter 的 Trader/XMD 分离、安全门禁和透传合同。"""

import sys
import textwrap
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from bullet_trade.integrations.huaxin.errors import HuaxinTradingDisabledError
from bullet_trade.integrations.huaxin.xmd_backend import (
    HUAXIN_DG14_L1_TCP_FRONT,
    HUAXIN_XMD_SOURCE,
    Python37XmdBackend,
    XmdBackendError,
    _normalise_tick,
)
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


def test_huaxin_bundle_is_broker_only_and_missing_journal_keeps_readonly_start(
    monkeypatch,
) -> None:
    """验证缺写 journal 时仍可构造只读服务，但写 action 明确 unavailable。

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
    assert bundle.broker_writes_require_persistent_idempotency is True

    _validate_huaxin_server_config(
        config,
        {**login_metadata, "enable_trading": True},
    )
    health = ServerApplication(config, router, bundle)._health_snapshot()["value"]
    assert health["idempotency_journal"]["state"] == "unavailable"
    assert health["huaxin"]["actions"]["broker.place_order"]["status"] == "unavailable"


def test_huaxin_server_uses_huaxin_account_environment(monkeypatch) -> None:
    """验证 server 路由账户不再错误依赖 QMT_ACCOUNT_ID。"""

    values = {
        "HUAXIN_ACCOUNT_ID": "huaxin-account",
        "HUAXIN_ACCOUNT_TYPE": "stock",
        "HUAXIN_ORDER_IDENTITY_JOURNAL_PATH": "/private/huaxin-orders.sqlite3",
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
    assert config.huaxin_order_identity_journal_path == "/private/huaxin-orders.sqlite3"


def test_huaxin_xmd_config_is_explicit_and_rejects_wrong_front() -> None:
    """验证启用 data 时必须显式选择 sidecar、路径和当前东莞 14 前置。"""

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
        huaxin_xmd_front="tcp://127.0.0.1:7780",
    )
    with pytest.raises(ValueError, match="东莞 14"):
        _validate_huaxin_server_config(wrong_front, {})

    valid = _server_config(
        enable_data=True,
        enable_broker=False,
        accounts=[],
        huaxin_xmd_backend="python37_sidecar",
        huaxin_xmd_python="/private/python",
        huaxin_xmd_sdk_dir="/private/sdk",
        huaxin_xmd_front=HUAXIN_DG14_L1_TCP_FRONT,
    )
    _validate_huaxin_server_config(valid, {})


def test_huaxin_server_config_reads_namespaced_xmd_environment(monkeypatch) -> None:
    """验证 Huaxin server 配置只从显式 HUAXIN_XMD_* 字段装配行情模块。"""

    values = {
        "HUAXIN_XMD_BACKEND": "python37_sidecar",
        "HUAXIN_XMD_PYTHON": "/private/huaxin37/bin/python",
        "HUAXIN_XMD_SDK_DIR": "/private/xmd-sdk",
        "HUAXIN_XMD_FRONT": HUAXIN_DG14_L1_TCP_FRONT,
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
        enable_data=True,
        enable_broker=False,
        tls_cert=None,
        tls_key=None,
    )

    config = build_server_config(args)

    assert config.huaxin_xmd_backend == "python37_sidecar"
    assert config.huaxin_xmd_python == "/private/huaxin37/bin/python"
    assert config.huaxin_xmd_sdk_dir == "/private/xmd-sdk"
    assert config.huaxin_xmd_front == HUAXIN_DG14_L1_TCP_FRONT
    assert config.accounts == []


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
            "HUAXIN_ORDER_IDENTITY_JOURNAL_PATH": "/private/huaxin-orders.sqlite3",
        }.get(name, default),
    )

    config = _load_huaxin_broker_config()

    assert config["mac_address"] == "00-11-22-33-44-55"
    assert config["user_product_info"] == "BT"
    assert config["order_identity_journal_path"] == "/private/huaxin-orders.sqlite3"


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
        "huaxin_xmd_front": HUAXIN_DG14_L1_TCP_FRONT,
        "huaxin_xmd_max_age_seconds": 5.0,
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
    assert snapshot["bid_price1"] == 100.704
    assert snapshot["ask_price1"] == 100.705
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
        front=HUAXIN_DG14_L1_TCP_FRONT,
        max_age_seconds=5.0,
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
        self.cancel_payload = None
        self.connected = False
        self.baseline_queries = set()

    def connect(self):
        """标记已连接。

        Returns:
            bool: True。
        """

        self.connected = True
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
            "ready_for_queries": True,
            "ready_for_new_orders": True,
            "ready_for_cancel": True,
            "trading_enabled": True,
            "cancel_order_enabled": True,
            "order_ref_allocator_ready": True,
            "order_identity_journal_ready": True,
            "security_order_constraints_ready": True,
            "baseline_query_ready": self.baseline_queries
            == {
                "account",
                "positions",
                "orders",
                "trades",
            },
        }


@pytest.mark.asyncio
async def test_adapter_delegates_queries_and_preserves_cancel_payload() -> None:
    """验证 adapter 对查询、限价/显式市价和撤单幂等载荷的精确透传。"""

    config = _server_config(
        idempotency_journal_path="/tmp/test-journal.sqlite",
        huaxin_order_identity_journal_path="/private/huaxin-orders.sqlite3",
    )
    router = AccountRouter(config.accounts)
    adapter = HuaxinBrokerAdapter(
        config,
        router,
        broker_config={"enable_trading": True, "enable_cancel": True},
        broker_factory=_FakeBroker,
    )
    await adapter.start()
    ctx = router.get("default")

    assert adapter.backend_status()["state"] == "degraded"
    assert adapter.backend_status()["actions"]["broker.place_order"]["status"] == "unavailable"
    assert (await adapter.get_account_info(ctx))["value"]["available_cash"] == 1000
    assert (await adapter.get_positions(ctx))[0]["amount"] == 100
    assert (await adapter.list_orders(ctx))[0]["order_id"] == "O1"
    assert (await adapter.list_trades(ctx))[0]["trade_id"] == "T1"
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
    assert broker.config["order_identity_journal_path"] == "/private/huaxin-orders.sqlite3"
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
async def test_adapter_rejects_market_without_explicit_type_before_broker_call() -> None:
    """验证远程市价缺少 market_type 时不会调用 HuaxinBroker。"""

    config = _server_config(idempotency_journal_path="/tmp/test-journal.sqlite")
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
