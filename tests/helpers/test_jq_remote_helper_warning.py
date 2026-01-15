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
