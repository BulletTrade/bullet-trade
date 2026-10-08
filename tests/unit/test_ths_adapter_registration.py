"""THS 注册阶段不得把缺失的 GUI 事实报告为可用交易能力。"""

import asyncio
from pathlib import Path
import subprocess
import sys

import pytest

from bullet_trade.server import cli
from bullet_trade.server.adapters import get_adapter
from bullet_trade.server.adapters.base import AccountRouter
from bullet_trade.server.adapters.ths import (
    ThsBrokerAdapter,
    ThsNotReadyError,
    ThsUnsupportedError,
    declare_ths_capabilities,
)
from bullet_trade.server.config import AccountConfig, ServerConfig


def test_ths_registered_requires_explicit_loopback_config(monkeypatch) -> None:
    builder = cli._get_server_adapter("ths")
    assert get_adapter("ths") is builder
    router = AccountRouter([AccountConfig(key="sim", account_id="masked")])

    with pytest.raises(ThsUnsupportedError, match="market data unsupported"):
        builder(ServerConfig(server_type="ths"), router)

    for key in ("THS_SERVICE_URL", "THS_SERVICE_TOKEN", "THS_SERVICE_ACCOUNT"):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(ThsNotReadyError, match="THS_SERVICE_URL"):
        builder(ServerConfig(server_type="ths", enable_data=False), router)

    monkeypatch.setenv("THS_SERVICE_URL", "http://127.0.0.1:8781")
    monkeypatch.setenv("THS_SERVICE_TOKEN", "test-only-token-012345678901")
    monkeypatch.setenv("THS_SERVICE_ACCOUNT", "masked")
    bundle = builder(ServerConfig(server_type="ths", enable_data=False), router)
    assert isinstance(bundle.broker_adapter, ThsBrokerAdapter)
    assert bundle.data_adapter is None
    monkeypatch.setenv("THS_SERVICE_ACCOUNT", "other")
    with pytest.raises(ThsNotReadyError, match="identity mismatch"):
        builder(ServerConfig(server_type="ths", enable_data=False), router)

    bundle = builder(
        ServerConfig(server_type="ths", enable_broker=False, enable_data=False), router
    )
    assert bundle.broker_adapter is None
    assert bundle.data_adapter is None

    with pytest.raises(ThsUnsupportedError, match="market data unsupported"):
        builder(ServerConfig(server_type="ths", enable_broker=False), router)


def test_default_server_adapters_import_without_ths_or_zoneinfo() -> None:
    script = """
import sys
import bullet_trade

# The package's data imports may load zoneinfo through pandas on Python 3.9.
# Isolate the ServerCLI import after that common package initialization.
for name in tuple(sys.modules):
    if name == 'zoneinfo' or name.startswith('zoneinfo.'):
        del sys.modules[name]

class ForbidThsImports:
    def find_spec(self, fullname, path=None, target=None):
        if (fullname == 'zoneinfo' or fullname.startswith('zoneinfo.')
                or fullname == 'bullet_trade.server.adapters.ths'
                or fullname.startswith('bullet_trade.integrations.ths')):
            raise AssertionError('unexpected import: ' + fullname)

guard = ForbidThsImports()
sys.meta_path.insert(0, guard)
from bullet_trade.server import cli
for name in ('qmt', 'big_qmt', 'huaxin'):
    assert callable(cli._get_server_adapter(name)), name
assert 'bullet_trade.server.adapters.ths' not in sys.modules
assert 'zoneinfo' not in sys.modules
actual_version = cli.sys.version_info
cli.sys.version_info = (3, 8, 20)
try:
    try:
        cli._get_server_adapter('ths')
    except RuntimeError as exc:
        assert 'Python 3.9 or newer' in str(exc)
    else:
        raise AssertionError('THS must reject Python 3.8')
finally:
    cli.sys.version_info = actual_version
assert 'bullet_trade.server.adapters.ths' not in sys.modules
sys.meta_path.remove(guard)
builder = cli._get_server_adapter(' THS ')
assert callable(builder)
assert 'bullet_trade.server.adapters.ths' in sys.modules
assert cli.get_adapter('ths') is builder
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_ths_selection_rejects_python_38_before_import(monkeypatch) -> None:
    monkeypatch.setattr(cli.sys, "version_info", (3, 8, 20))
    with pytest.raises(RuntimeError, match="THS server adapter requires Python 3.9 or newer"):
        cli._get_server_adapter(" THS ")


def test_ths_capabilities_separate_environment_version_and_actions() -> None:
    simulation = declare_ths_capabilities(
        environment="simulation", client_version="unverified-version"
    )
    brokerage = declare_ths_capabilities(environment="brokerage")

    assert simulation["client_version"] == "unverified-version"
    assert simulation["environment"] == "simulation"
    assert brokerage["environment"] == "brokerage"
    for capabilities in (simulation, brokerage):
        assert capabilities["profile_verified"] is False
        assert set(capabilities["queries"].values()) == {"requires_current_snapshot"}
        assert capabilities["orders"]["limit_buy"] == "requires_durable_service_and_gui_gate"
        assert capabilities["orders"]["cancel_by_original_order_id"] == "requires_durable_service_and_gui_gate"
        assert capabilities["orders"]["market_order"] == "unsupported"


def test_ths_adapter_fails_closed_without_business_transport() -> None:
    router = AccountRouter([AccountConfig(key="sim", account_id="masked")])
    account = router.get("sim")
    adapter = ThsBrokerAdapter(router, transport=object())

    calls = (
        adapter.get_account_info(account),
        adapter.get_positions(account),
        adapter.list_orders(account),
        adapter.list_trades(account),
        adapter.get_order_status(account, "original-id"),
        adapter.place_order(account, {"order_type": "limit"}),
        adapter.cancel_order_request(account, {"order_id": "original-id"}),
    )
    for call in calls:
        with pytest.raises(ThsNotReadyError, match="unavailable"):
            asyncio.run(call)
    with pytest.raises(ThsUnsupportedError, match="idempotency_key"):
        asyncio.run(adapter.cancel_order(account, "original-id"))
