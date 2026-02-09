import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import helpers.bullet_trade_jq_remote_helper as helper  # type: ignore


class _FakeClient:
    def __init__(self):
        self.calls = 0

    def request(self, action, payload):
        self.calls += 1
        return {"order_id": "oid-1", "warning": "000001.XSHE 停牌，拒绝远程委托"}


def test_warning_print(monkeypatch, capsys):
    fake = _FakeClient()
    broker = helper.RemoteBrokerClient(fake)
    broker._data_client = None
    broker._client = fake
    broker._place_order("000001.XSHE", 100, None, "BUY", wait_timeout=0)
    captured = capsys.readouterr()
    assert "停牌" in captured.err


def test_get_orders_passes_from_broker_flag():
    payloads = []

    class _FakeClient:
        def request(self, action, payload):
            payloads.append((action, payload))
            return []

    broker = helper.RemoteBrokerClient(_FakeClient())
    orders = broker.get_orders(from_broker=True)
    assert orders == {}
    assert payloads
    action, payload = payloads[0]
    assert action == "broker.orders"
    assert payload.get("from_broker") is True


def test_protocol_version_warning(monkeypatch, capsys):
    class _DummySocket:
        def settimeout(self, timeout):
            return None

        def close(self):
            return None

    def _fake_create_connection(address, timeout=10):
        return _DummySocket()

    def _fake_send(self, sock, message):
        return None

    def _fake_urandom(size):
        return b"\x00\x00\x00\x01"

    monkeypatch.setattr(helper.socket, "create_connection", _fake_create_connection)
    monkeypatch.setattr(helper._ShortLivedClient, "_send", _fake_send)
    monkeypatch.setattr(helper.os, "urandom", _fake_urandom)
    monkeypatch.setattr(helper, "_DEBUG", False, raising=False)

    client = helper._ShortLivedClient("127.0.0.1", 58620, "token", retries=0)
    payload = {"foo": "bar"}
    req_id = str(id(payload) ^ 1)

    responses = [
        {"type": "handshake_ack", "protocol": helper.HELPER_PROTOCOL_VERSION + 1},
        {"type": "response", "id": req_id, "payload": {"ok": True}},
    ]

    def _fake_recv(self, sock):
        return responses.pop(0)

    monkeypatch.setattr(helper._ShortLivedClient, "_recv", _fake_recv)

    result = client.request("data.snapshot", payload)
    captured = capsys.readouterr()
    assert "协议版本" in captured.err
    assert result == {"ok": True}
