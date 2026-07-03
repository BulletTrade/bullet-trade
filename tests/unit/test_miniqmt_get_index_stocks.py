from datetime import datetime

from bullet_trade.data.providers import miniqmt
from bullet_trade.data.providers.miniqmt import MiniQMTProvider


class DummyXt:
    def __init__(self) -> None:
        self.download_calls = 0
        self.last_symbol = None

    def download_index_weight(self) -> None:
        self.download_calls += 1

    def get_index_weight(self, symbol: str):
        self.last_symbol = symbol
        return {"000001.SZ": 0.1, "600000.SH": 0.2}


def test_miniqmt_get_index_stocks_uses_index_weight(monkeypatch) -> None:
    dummy = DummyXt()
    provider = MiniQMTProvider({"cache_dir": None, "auto_download": True})
    monkeypatch.setattr(provider, "_ensure_xtdata", lambda: dummy)
    warnings = []

    def _capture_warning(msg, *args, **kwargs):
        try:
            formatted = msg % args if args else msg
        except Exception:
            formatted = str(msg)
        warnings.append(formatted)

    monkeypatch.setattr(miniqmt.logger, "warning", _capture_warning)

    result = provider.get_index_stocks("399101.XSHE", date=datetime(2025, 1, 2))

    assert dummy.download_calls == 1
    assert dummy.last_symbol == "399101.SZ"
    assert result == ["000001.XSHE", "600000.XSHG"]
    assert any("get_index_stocks 不支持历史日期" in message for message in warnings)


def test_miniqmt_get_index_stocks_can_opt_out_index_weight_download(monkeypatch) -> None:
    dummy = DummyXt()
    provider = MiniQMTProvider(
        {"cache_dir": None, "auto_download": True, "index_weight_auto_download": False}
    )
    monkeypatch.setattr(provider, "_ensure_xtdata", lambda: dummy)

    result = provider.get_index_stocks("399101.XSHE")

    assert dummy.download_calls == 0
    assert dummy.last_symbol == "399101.SZ"
    assert result == ["000001.XSHE", "600000.XSHG"]
