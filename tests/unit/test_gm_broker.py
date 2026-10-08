"""GM 的持久化幂等、未知提交、账户隔离和成交身份合同。"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from bullet_trade.integrations.gm.broker import GmBroker


def make(tmp_path, client=None):
    if client is None:
        client = Mock()

        def query(method, **kwargs):
            if method == "status":
                return {"state": 3, "error_code": 0}
            if method == "cash":
                return dict(
                    account_id="sim",
                    nav=1000000,
                    available=1000000,
                    balance=1000000,
                    market_value=0,
                    order_frozen=0,
                )
            if method == "positions":
                return []
            if method == "quote":
                return [
                    dict(
                        symbol="SHSE.511880",
                        price=100,
                        created_at=datetime.now(timezone.utc).isoformat(),
                    )
                ]
            if method == "place":
                return [
                    dict(
                        account_id="sim",
                        cl_ord_id="native1",
                        symbol=kwargs["symbol"],
                        volume=kwargs["volume"],
                    )
                ]
            if method in ("orders", "trades"):
                return []
            raise AssertionError(method)

        client.query.side_effect = query
    b = GmBroker(
        "sim",
        config=dict(
            token="secret",
            strategy_id="strategy",
            journal_path=str(tmp_path / "orders.sqlite"),
            enable_trading=True,
        ),
        client=client,
    )
    b.connect()
    return b


def buy(b, key="test", **kwargs):
    return asyncio.run(
        b.buy("511880.XSHG", 100, kwargs.get("price", 100), extra={"idempotency_key": key})
    )


def test_repeated_request_exactly_once_and_survives_restart(tmp_path):
    b = make(tmp_path)
    first = buy(b)
    assert buy(b) == first
    assert sum(c.args[0] == "place" for c in b.client.query.call_args_list) == 1
    b.disconnect()
    b = make(tmp_path)
    assert buy(b) == first
    assert not any(c.args[0] == "place" for c in b.client.query.call_args_list)
    b.disconnect()


def test_conflicting_idempotency_payload_fails(tmp_path):
    b = make(tmp_path)
    buy(b)
    with pytest.raises(ValueError, match="幂等"):
        buy(b, price=100.001)
    b.disconnect()


def test_unknown_submit_returns_stable_pending_and_blocks_new_write(tmp_path):
    b = make(tmp_path)
    original = b.client.query.side_effect

    def query(method, **kwargs):
        if method == "place":
            raise TimeoutError()
        return original(method, **kwargs)

    b.client.query.side_effect = query
    oid = buy(b)
    assert buy(b) == oid
    assert b.get_orders()[0]["submission_state"] == "submit_unknown"
    assert b.get_orders()[0]["status"] == "new"
    with pytest.raises(RuntimeError, match="未决"):
        buy(b, key="new")
    b.disconnect()
    b = make(tmp_path)
    with pytest.raises(RuntimeError, match="未决"):
        buy(b, key="new")
    b.disconnect()


@pytest.mark.parametrize("price", [0, -1, float("nan"), float("inf"), 100.0001, 102])
def test_invalid_or_outlier_price_never_calls_place(tmp_path, price):
    b = make(tmp_path)
    with pytest.raises(ValueError):
        buy(b, price=price)
    assert not any(c.args[0] == "place" for c in b.client.query.call_args_list)
    b.disconnect()


def test_account_journal_cannot_be_reused_for_another_account(tmp_path):
    b = make(tmp_path)
    b.disconnect()
    b.account_id = "another"
    with pytest.raises(ValueError, match="另一账户"):
        b.connect()


def test_single_journal_writer(tmp_path):
    b = make(tmp_path)
    with pytest.raises(TimeoutError):
        make(tmp_path)
    b.disconnect()


def test_sell_requires_actual_sellable_position(tmp_path):
    b = make(tmp_path)
    with pytest.raises(ValueError, match="可卖"):
        asyncio.run(b.sell("511880.XSHG", 100, 100))
    b.disconnect()


def test_stale_quote_blocks_write(tmp_path):
    b = make(tmp_path)
    original = b.client.query.side_effect
    b.client.query.side_effect = lambda method, **kw: (
        [dict(symbol="SHSE.511880", price=100, created_at="2020-01-01T00:00:00+08:00")]
        if method == "quote"
        else original(method, **kw)
    )
    with pytest.raises(RuntimeError, match="过期"):
        buy(b)
    b.disconnect()


def recorded(tmp_path):
    import json
    from pathlib import Path

    evidence = json.loads(
        (Path(__file__).parents[1] / "fixtures/gm_trading_20261008.json").read_text()
    )
    b = make(tmp_path)
    for side in ("buy", "sell"):
        b._db.execute(
            "INSERT INTO submissions VALUES (?,?,?,?)", (side, "{}", side + "-native", "known")
        )
    b._db.commit()
    original = b.client.query.side_effect
    b.client.query.side_effect = lambda method, **kw: (
        evidence[method] if method in ("orders", "trades") else original(method, **kw)
    )
    return b, evidence


def test_synthetic_sdk_roundtrip_fee_and_cash_replay(tmp_path):
    b, facts = recorded(tmp_path)
    trades = b.get_trades()
    assert [t["commission"] for t in trades] == [5, 5]
    assert all(t["commission_source"] == "final_order_filled_commission" for t in trades)
    assert all(t["raw"]["commission"] == 0 for t in trades)
    assert b.get_trades() == trades
    expected = facts["before_cash"] - trades[0]["gross_amount"] + trades[1]["gross_amount"] - 10
    assert facts["after_cash"] == expected
    b.disconnect()


def test_duplicate_execution_does_not_duplicate_fee(tmp_path):
    b, facts = recorded(tmp_path)
    facts["trades"].append(dict(facts["trades"][0]))
    assert len(b.get_trades()) == 2
    assert sum(t["commission"] for t in b.get_trades()) == 10
    b.disconnect()


def test_conflicting_execution_identity_fails(tmp_path):
    b, facts = recorded(tmp_path)
    duplicate = dict(facts["trades"][0], volume=200)
    facts["trades"].append(duplicate)
    with pytest.raises(RuntimeError, match="不同记录"):
        b.get_trades()
    b.disconnect()


def test_partial_fee_is_not_guessed(tmp_path):
    b, facts = recorded(tmp_path)
    facts["orders"][0]["status"] = 2
    with pytest.raises(RuntimeError, match="手续费尚未最终确认"):
        b.get_trades()
    b.disconnect()


def test_incomplete_execution_snapshot_is_not_final(tmp_path):
    b, facts = recorded(tmp_path)
    facts["trades"][0]["volume"] = 50
    with pytest.raises(RuntimeError, match="尚未对齐"):
        b.get_trades()
    b.disconnect()


def test_missing_execution_id_fails(tmp_path):
    b, facts = recorded(tmp_path)
    facts["trades"][0]["exec_id"] = ""
    with pytest.raises(RuntimeError, match="稳定身份"):
        b.get_trades()
    b.disconnect()


def test_foreign_account_order_rejected(tmp_path):
    b, facts = recorded(tmp_path)
    facts["orders"][0]["account_id"] = "foreign"
    with pytest.raises(RuntimeError, match="账户不匹配"):
        b.get_orders()
    b.disconnect()


@pytest.mark.parametrize(
    "native,status",
    [
        (1, "open"),
        (2, "filling"),
        (3, "filled"),
        (5, "partly_canceled"),
        (6, "canceling"),
        (8, "rejected"),
        (9, "held"),
        (99, "held"),
    ],
)
def test_native_order_states(tmp_path, native, status):
    b, facts = recorded(tmp_path)
    facts["orders"][0]["status"] = native
    assert b.get_orders()[0]["status"] == status
    b.disconnect()


def test_cancel_ack_without_final_state_is_not_success(tmp_path):
    b, facts = recorded(tmp_path)
    facts["orders"][0]["status"] = 1
    original = b.client.query.side_effect
    b.client.query.side_effect = lambda method, **kw: (
        True if method == "cancel" else original(method, **kw)
    )
    assert asyncio.run(b.cancel_order(b._local_id("buy"))) is False
    assert b.get_orders()[0]["status"] == "open"
    b.disconnect()


def test_readonly_rejects_writes(tmp_path):
    b = make(tmp_path)
    b.config["enable_trading"] = False
    with pytest.raises(RuntimeError, match="未启用"):
        buy(b)
    b.disconnect()


def test_live_engine_consumes_recorded_orders_and_trades_without_duplicates(tmp_path):
    from bullet_trade.core.live_engine import LiveEngine
    from bullet_trade.core.models import OrderStatus

    b, _ = recorded(tmp_path)
    strategy = tmp_path / "strategy.py"
    strategy.write_text("def initialize(context):\n    pass\n")
    engine = LiveEngine(
        strategy_file=strategy,
        broker_factory=lambda: b,
        live_config={"runtime_dir": str(tmp_path / "runtime")},
    )
    engine._ensure_broker_created()
    orders = engine.get_orders(from_broker=True, strict=True)
    assert len(orders) == 2
    assert all(o.status == OrderStatus.filled and o.filled == 100 for o in orders.values())
    trades = engine.get_trades(strict=True)
    assert len(trades) == 2 and sum(t.commission for t in trades.values()) == 10
    assert len(engine.get_trades(strict=True)) == 2
    b.disconnect()
