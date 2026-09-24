"""Fee lookup extensions must preserve strict live settings snapshots."""

from types import SimpleNamespace

import pytest

from bullet_trade.core.live_engine import LiveEngine
from bullet_trade.core.settings import OrderCost, get_settings, reset_settings, set_order_cost


@pytest.fixture
def snapshot_engine():
    engine = LiveEngine.__new__(LiveEngine)
    engine.config = SimpleNamespace(checkpoint_persistence_enabled=True)
    return engine


def test_old_default_snapshot_restores_without_new_backtest_options(snapshot_engine):
    old_snapshot = snapshot_engine._collect_settings_snapshot()
    old_snapshot["options"].pop("equity_cash_budget", None)
    old_snapshot["options"].pop("equity_cash_decimals", None)

    reset_settings()
    snapshot_engine._apply_settings_snapshot(old_snapshot)

    assert snapshot_engine._collect_settings_snapshot() == old_snapshot
    assert "equity_cash_budget" not in get_settings().options
    assert "equity_cash_decimals" not in get_settings().options


@pytest.mark.parametrize("alias", ["mmf", "money"])
@pytest.mark.parametrize("reference", [None, "511990.XSHG"])
def test_fee_alias_snapshot_preserves_original_storage_key(snapshot_engine, alias, reference):
    cost = OrderCost(open_commission=0.001, close_commission=0.002)
    set_order_cost(cost, type=alias, ref=reference)
    section = "order_cost_overrides" if reference else "order_cost"
    key = f"{alias}_{reference}" if reference else alias
    snapshot = snapshot_engine._collect_settings_snapshot()
    assert key in snapshot[section]

    reset_settings()
    snapshot_engine._apply_settings_snapshot(snapshot)

    restored = snapshot_engine._collect_settings_snapshot()
    assert restored == snapshot
    assert restored[section][key]["open_commission"] == 0.001
