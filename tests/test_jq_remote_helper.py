import pytest

from helpers import bullet_trade_jq_remote_helper as helper
from tests.test_remote_server import stub_server  # 复用 stub server fixture


class _RecordingClient:
    def __init__(self):
        self.requests = []
        self.positions = [{"security": "000001.XSHE", "amount": 300, "closeable_amount": 300}]

    def request(self, action, payload):
        self.requests.append((action, dict(payload)))
        if action == "broker.positions":
            return self.positions
        if action == "broker.order_status":
            return {"order_id": payload.get("order_id"), "status": "filled"}
        return {"order_id": "oid-1", "status": "open", "amount": payload.get("amount"), "price": 10.0}


class _FakeDataClient:
    def get_last_price(self, security):
        assert security == "000001.XSHE"
        return 10.0


def test_helper_restores_legacy_stringified_tuple_columns():
    payload = {
        "dtype": "dataframe",
        "columns": [
            "('600635.XSHG', 'time')",
            "('600635.XSHG', 'open')",
            "('000001.XSHG', 'open')",
        ],
        "records": [[1777392000000.0, 5.4, 4061.822]],
    }

    df = helper._df_from_payload(payload)

    assert isinstance(df.columns, helper.pd.MultiIndex)
    assert df.columns.names == ["field", "code"]
    assert list(df.columns) == [
        ("time", "600635.XSHG"),
        ("open", "600635.XSHG"),
        ("open", "000001.XSHG"),
    ]
    assert df["open"]["600635.XSHG"].iloc[0] == 5.4


def test_helper_order_sends_zero_wait_timeout():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    oid = broker.order("000001.XSHE", 100, 10.0, "BUY", 0)

    assert oid == "oid-1"
    action, payload = client.requests[0]
    assert action == "broker.place_order"
    assert payload["wait_timeout"] == 0
    assert payload["style"] == {"type": "limit", "price": 10.0}
    assert payload["idempotency_key"].startswith("bt-helper-")


def test_helper_order_without_price_sends_market_sell_payload():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    broker.order("000001.XSHE", -100, price=None, wait_timeout=0)

    action, payload = client.requests[0]
    assert action == "broker.place_order"
    assert payload["side"] == "SELL"
    assert payload["amount"] == 100
    assert payload["market"] is True
    assert payload["style"] == {"type": "market"}


def test_helper_order_target_value_accepts_value_alias_for_market_sell():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")
    broker.bind_data_client(_FakeDataClient())

    broker.order_target_value("000001.XSHE", value=0, wait_timeout=5)

    action, payload = next(item for item in client.requests if item[0] == "broker.place_order")
    assert action == "broker.place_order"
    assert payload["side"] == "SELL"
    assert payload["amount"] == 300
    assert payload["wait_timeout"] == 5
    assert payload["market"] is True
    assert payload["style"] == {"type": "market"}


def test_helper_order_value_without_price_sends_market_sell_payload():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")
    broker.bind_data_client(_FakeDataClient())

    broker.order_value("000001.XSHE", -1000, wait_timeout=0)

    action, payload = client.requests[-1]
    assert action == "broker.place_order"
    assert payload["side"] == "SELL"
    assert payload["amount"] == 100
    assert payload["market"] is True
    assert payload["style"] == {"type": "market"}


def test_helper_order_target_without_price_sends_market_delta_payloads():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    broker.order_target("000001.XSHE", 500, wait_timeout=0)
    broker.order_target("000001.XSHE", 100, wait_timeout=0)

    place_orders = [payload for action, payload in client.requests if action == "broker.place_order"]
    assert place_orders[0]["side"] == "BUY"
    assert place_orders[0]["amount"] == 200
    assert place_orders[0]["style"] == {"type": "market"}
    assert place_orders[1]["side"] == "SELL"
    assert place_orders[1]["amount"] == 200
    assert place_orders[1]["style"] == {"type": "market"}


def test_helper_order_target_value_without_price_sends_market_buy_payload():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")
    broker.bind_data_client(_FakeDataClient())

    broker.order_target_value("000001.XSHE", target_value=5000, wait_timeout=0)

    action, payload = client.requests[-1]
    assert action == "broker.place_order"
    assert payload["side"] == "BUY"
    assert payload["amount"] == 200
    assert payload["market"] is True
    assert payload["style"] == {"type": "market"}


def test_helper_order_sends_optional_order_payload_fields():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")

    broker.order(
        "000001.XSHE",
        100,
        price=10.0,
        wait_timeout=0,
        market=True,
        order_remark="bt:test:abcd1234",
        idempotency_key="idem-1",
    )

    _, payload = client.requests[0]
    assert payload["style"] == {"type": "market", "protect_price": 10.0}
    assert payload["market"] is True
    assert payload["order_remark"] == "bt:test:abcd1234"
    assert payload["idempotency_key"] == "idem-1"


def test_helper_order_value_without_price_uses_bound_data_client():
    client = _RecordingClient()
    broker = helper.RemoteBrokerClient(client, account_key="default")
    broker.bind_data_client(_FakeDataClient())

    broker.order_value("000001.XSHE", 1000, wait_timeout=0, idempotency_key="idem-value")

    _, payload = client.requests[0]
    assert payload["amount"] == 100
    assert payload["idempotency_key"] == "idem-value"


def test_helper_e2e_with_stub(stub_server):
    helper.configure(
        host=stub_server.listen,
        port=stub_server.port,
        token=stub_server.token,
        account_key=stub_server.accounts[0].key if stub_server.accounts else "default",
    )
    assert helper.get_broker_client()._data_client is helper.get_data_client()
    # 账户与持仓
    account = helper.get_account()
    assert account.available_cash == 1_000_000
    positions = helper.get_positions()
    assert positions == []

    # 下单（限价 / 自动补价市价）
    oid = helper.order("000001.XSHE", 100, price=10.0, wait_timeout=0)
    assert oid
    status = helper.get_order_status(oid)
    assert status.get("order_id") == oid

    open_orders = helper.get_open_orders()
    assert oid in open_orders
    orders = helper.get_orders(order_id=oid)
    assert oid in orders
    trades = helper.get_trades(order_id=oid)
    assert isinstance(trades, dict)

    cancel_resp = helper.cancel_order(oid)
    assert cancel_resp.get("value") is True or cancel_resp.get("success", True) is True
