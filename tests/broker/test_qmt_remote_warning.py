import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from bullet_trade.broker.qmt_remote import RemoteQmtBroker


class _FakeConn:
    def __init__(self):
        self.requests = []

    def start(self):
        pass

    def close(self):
        pass

    def request(self, action, payload, timeout=30.0):
        self.requests.append((action, payload))
        return {"order_id": "oid-1", "warning": "000001.XSHE 停牌，拒绝远程委托"}


def test_remote_warning_prints_and_captures(capsys, monkeypatch):
    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc")
    broker._connection = _FakeConn()  # type: ignore
    broker.connect()
    broker._place_order_sync("BUY", "000001.XSHE", 100, None, None)
    out = capsys.readouterr().out
    assert "停牌" in out
    assert broker._last_warning and "停牌" in broker._last_warning


def test_remote_qmt_broker_market_order_without_price_does_not_prefill_protect_price(monkeypatch):
    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc")
    fake_conn = _FakeConn()
    broker._connection = fake_conn  # type: ignore
    broker.connect()

    broker._place_order_sync("SELL", "000001.XSHE", 100, None, None)

    action, payload = fake_conn.requests[0]
    assert action == "broker.place_order"
    assert payload["side"] == "SELL"
    assert payload["market"] is True
    assert payload["style"] == {"type": "market"}


def test_remote_qmt_broker_market_order_with_price_keeps_explicit_protect_price(monkeypatch):
    monkeypatch.setenv("QMT_SERVER_TOKEN", "dummy-token")
    broker = RemoteQmtBroker(account_id="acc")
    fake_conn = _FakeConn()
    broker._connection = fake_conn  # type: ignore
    broker.connect()

    broker._place_order_sync("BUY", "000001.XSHE", 100, 10.5, None, market=True)

    action, payload = fake_conn.requests[0]
    assert action == "broker.place_order"
    assert payload["side"] == "BUY"
    assert payload["market"] is True
    assert payload["style"] == {"type": "market", "protect_price": 10.5}
