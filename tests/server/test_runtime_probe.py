from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path

import pytest

from bullet_trade.server.adapters.base import AccountRouter
from bullet_trade.server.adapters.stub import build_stub_bundle
from bullet_trade.server.app import ServerApplication
from bullet_trade.server.config import AccountConfig, ServerConfig
from bullet_trade.server.runtime_probe import ProbeConfig, RemoteRuntimeProbe


def _ensure_current_event_loop() -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def _run_loop(loop: asyncio.AbstractEventLoop, app: ServerApplication) -> None:
    asyncio.set_event_loop(loop)
    loop.create_task(app.start())
    loop.run_forever()


@pytest.fixture
def seeded_stub_server():
    port = 59341
    config = ServerConfig(
        server_type="stub",
        listen="127.0.0.1",
        port=port,
        token="stub-token",
        enable_data=True,
        enable_broker=True,
        accounts=[AccountConfig(key="default", account_id="demo")],
    )
    _ensure_current_event_loop()
    router = AccountRouter(config.accounts)
    bundle = build_stub_bundle(config, router)
    data_adapter = bundle.data_adapter
    assert data_adapter is not None
    data_adapter._ticks.update(  # type: ignore[attr-defined]
        {
            "000001.XSHE": {
                "sid": "000001.XSHE",
                "symbol": "000001.XSHE",
                "last_price": 12.34,
                "dt": "2026-04-01 09:30:01",
                "time": "2026-04-01 09:30:01",
                "volume": 1000,
                "provider": "stub",
            },
            "159915.XSHE": {
                "sid": "159915.XSHE",
                "symbol": "159915.XSHE",
                "last_price": 3.21,
                "dt": "2026-04-01 09:30:02",
                "time": "2026-04-01 09:30:02",
                "volume": 2000,
                "provider": "stub",
            },
            "518880.XSHG": {
                "sid": "518880.XSHG",
                "symbol": "518880.XSHG",
                "last_price": 10.01,
                "dt": "2026-04-01 09:30:03",
                "time": "2026-04-01 09:30:03",
                "volume": 3000,
                "provider": "stub",
            },
        }
    )
    app = ServerApplication(config, router, bundle)
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=_run_loop, args=(loop, app), daemon=True)
    thread.start()
    asyncio.run_coroutine_threadsafe(app.wait_started(), loop).result(timeout=5)
    try:
        broker_adapter = bundle.broker_adapter
        assert broker_adapter is not None
        account = router.get("default")
        state = broker_adapter._account_state_for(account)  # type: ignore[attr-defined]
        state["available_cash"] = 812345.67
        state["transferable_cash"] = 812345.67
        positions = broker_adapter._positions_for(account)  # type: ignore[attr-defined]
        positions["159915.XSHE"] = {
            "security": "159915.XSHE",
            "name": "创业板ETF易方达",
            "amount": 61400,
            "available_amount": 61300,
            "closeable_amount": 61300,
            "can_use_volume": 61300,
            "avg_cost": 3.245035342019544,
            "last_price": 3.215,
            "current_price": 3.215,
        }
        positions["518880.XSHG"] = {
            "security": "518880.XSHG",
            "name": "黄金ETF华安",
            "amount": 2500,
            "available_amount": 2500,
            "closeable_amount": 2500,
            "can_use_volume": 2500,
            "avg_cost": 9.08264,
            "last_price": 9.924,
            "current_price": 9.924,
        }
        broker_adapter._orders_for(account).append(  # type: ignore[attr-defined]
            {
                "order_id": "seed-order-1",
                "security": "159915.XSHE",
                "amount": 100,
                "filled": 0,
                "traded_volume": 0,
                "status": "open",
                "side": "BUY",
                "style_type": "limit",
                "style": "limit",
                "order_price": 3.214,
                "price": 0.0,
                "order_type": 23,
                "is_buy": True,
                "commission_fee": 0.0,
                "commission": 0.0,
                "tax": 0.0,
                "deal_balance": 0.0,
                "frozen_cash": 321.4,
                "frozen_amount": 0,
                "order_remark": "bullet-trade",
                "strategy_name": "bullet-trade",
                "status_msg": "",
                "price_type": 50,
                "order_time": 1775011904,
                "order_sysid": "71059",
                "raw_status": 50,
            }
        )
        broker_adapter._trades_for(account).append(  # type: ignore[attr-defined]
            {
                "trade_id": "seed-trade-1",
                "order_id": "seed-order-2",
                "security": "518880.XSHG",
                "amount": 100,
                "price": 9.923,
                "traded_price": 9.923,
                "deal_balance": 992.3,
                "time": 1775011906,
                "commission": 0.0,
                "commission_fee": 0.0,
                "tax": 0.0,
            }
        )
        broker_adapter._recalculate_account_totals(account)  # type: ignore[attr-defined]
        yield config
    finally:
        asyncio.run_coroutine_threadsafe(app.shutdown(), loop).result(timeout=5)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)


@pytest.mark.integration
def test_runtime_probe_inspect_only_with_stub(seeded_stub_server, tmp_path: Path):
    output_dir = tmp_path / "probe-inspect"
    probe = RemoteRuntimeProbe(
        ProbeConfig(
            host=seeded_stub_server.listen,
            port=seeded_stub_server.port,
            token=seeded_stub_server.token,
            output_dir=output_dir,
            inspect_symbol="000001.XSHE",
            limit_symbol="159915.XSHE",
            market_symbol="518880.XSHG",
            tick_timeout_sec=3.0,
        )
    )

    report = probe.run(trade_smoke=False)

    assert report["overall_status"] == "ok"
    step_map = {step["name"]: step for step in report["steps"]}
    assert step_map["admin.health"]["status"] == "ok"
    assert step_map["data.snapshot"]["status"] == "ok"
    assert step_map["data.subscribe.tick"]["status"] == "ok"
    assert step_map["provider.get_price.minute"]["status"] == "ok"
    assert step_map["broker.account"]["status"] == "ok"
    assert step_map["broker.orders"]["status"] == "ok"
    assert step_map["broker.trades"]["status"] == "ok"
    assert "last_price" in report["observed_contracts"]["snapshot_keys"]
    assert "symbol" in report["observed_contracts"]["tick_event_keys"]
    assert report["observed_contracts"]["minute_history_columns"] == ["close", "high", "low", "money", "open", "volume"]
    assert report["observed_contracts"]["daily_history_columns"] == ["close", "high", "low", "money", "open", "volume"]
    assert "available_cash" in report["observed_contracts"]["account_keys"]
    assert "positions" in report["observed_contracts"]["account_keys"]
    assert "raw_status" in report["observed_contracts"]["order_keys"]
    assert "price_type" in report["observed_contracts"]["order_keys"]
    assert "strategy_name" in report["observed_contracts"]["order_keys"]
    assert "trade_id" in report["observed_contracts"]["trade_keys"]
    assert "price" in report["observed_contracts"]["trade_keys"]
    assert (output_dir / "probe_report.json").exists()
    assert (output_dir / "probe_report.md").exists()


@pytest.mark.integration
def test_runtime_probe_trade_smoke_with_stub(seeded_stub_server, tmp_path: Path):
    output_dir = tmp_path / "probe-trade"
    probe = RemoteRuntimeProbe(
        ProbeConfig(
            host=seeded_stub_server.listen,
            port=seeded_stub_server.port,
            token=seeded_stub_server.token,
            output_dir=output_dir,
            inspect_symbol="000001.XSHE",
            limit_symbol="159915.XSHE",
            market_symbol="518880.XSHG",
            order_amount=100,
            tick_timeout_sec=3.0,
        )
    )

    report = probe.run(trade_smoke=True)

    assert report["overall_status"] == "ok"
    step_map = {step["name"]: step for step in report["steps"]}
    assert step_map["broker.limit_buy_cancel"]["status"] == "ok"
    assert step_map["broker.market_buy_cleanup"]["status"] == "ok"
    assert "order_id" in report["observed_contracts"]["order_keys"]
    limit_payload = json.loads((output_dir / "raw" / "15_broker_limit_buy_cancel.json").read_text(encoding="utf-8"))
    market_payload = json.loads((output_dir / "raw" / "16_broker_market_buy_cleanup.json").read_text(encoding="utf-8"))
    assert "cancel_response" in limit_payload
    assert limit_payload["cancel_response"]["timed_out"] is False
    assert limit_payload["live_snapshot"]["high_limit"] > limit_payload["live_snapshot"]["last_price"]
    assert limit_payload["live_snapshot"]["low_limit"] < limit_payload["live_snapshot"]["last_price"]
    assert "requested_protect_price" in market_payload
    assert market_payload["live_snapshot"]["high_limit"] > market_payload["live_snapshot"]["last_price"]
    assert market_payload["live_snapshot"]["low_limit"] < market_payload["live_snapshot"]["last_price"]
