from bullet_trade.core.live_engine import LiveEngine
from bullet_trade.core.models import Order


def test_live_engine_build_order_remark_uses_strategy_name(tmp_path):
    strategy_file = tmp_path / "demo_strategy.py"
    strategy_file.write_text("def initialize(context):\n    pass\n")

    engine = LiveEngine(
        strategy_file=str(strategy_file),
        live_config={"strategy_name": "Alpha-1"},
    )
    order = Order(order_id="oid-123", security="000001.XSHE", amount=100)

    remark = engine._prepare_order_metadata(order)
    assert remark is not None
    assert remark.startswith("bt:alpha-1:")
    assert len(remark) <= 24
    assert order.extra.get("order_remark") == remark
    assert order.extra.get("strategy_name") == "Alpha-1"


class _BrokerWithExtra:
    async def buy(self, security, amount, price=None, wait_timeout=None, remark=None, *, market=False, extra=None):
        return "oid-extra"


class _BrokerWithoutExtra:
    async def buy(self, security, amount, price=None, wait_timeout=None, remark=None, *, market=False):
        return "oid-plain"


def test_live_engine_detects_broker_extra_support(tmp_path):
    strategy_file = tmp_path / "demo_strategy.py"
    strategy_file.write_text("def initialize(context):\n    pass\n")

    engine = LiveEngine(
        strategy_file=str(strategy_file),
        live_config={"strategy_name": "Alpha-1"},
    )

    assert engine._broker_method_accepts_extra(_BrokerWithExtra().buy) is True
    assert engine._broker_method_accepts_extra(_BrokerWithoutExtra().buy) is False


def test_live_engine_copies_order_extra_payload(tmp_path):
    strategy_file = tmp_path / "demo_strategy.py"
    strategy_file.write_text("def initialize(context):\n    pass\n")

    engine = LiveEngine(
        strategy_file=str(strategy_file),
        live_config={"strategy_name": "Alpha-1"},
    )
    order = Order(
        order_id="oid-123",
        security="000001.XSHE",
        amount=100,
        extra={"signal_batch_id": 113, "execution_batch_id": 151},
    )

    payload = engine._order_extra_payload(order)

    assert payload == {"signal_batch_id": 113, "execution_batch_id": 151}
    payload["signal_batch_id"] = 999
    assert order.extra["signal_batch_id"] == 113
