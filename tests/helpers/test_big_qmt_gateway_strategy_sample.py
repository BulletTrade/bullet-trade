import importlib.util
from pathlib import Path
from types import SimpleNamespace

from tornado.httputil import HTTPHeaders


TEST_ACCOUNT_ID = "test_account_id"


def _load_helper():
    path = Path(__file__).resolve().parents[2] / "helpers" / "big_qmt_gateway_strategy_sample.py"
    spec = importlib.util.spec_from_file_location("bt_big_qmt_gateway_strategy_sample_for_test", str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeValues:
    def __init__(self, rows):
        self._rows = rows

    def tolist(self):
        return self._rows


class _FakeIndex:
    name = None

    def tolist(self):
        return ["20260701", "20260702"]


class _FakeStimeIndex:
    name = "stime"

    def tolist(self):
        return ["20260701093000"]


class _FakeFrame:
    columns = ["open", "close"]
    index = _FakeIndex()
    values = _FakeValues([[1.0, 2.0]])


class _FakeFrameWithStime:
    columns = ["open", "close"]
    index = _FakeStimeIndex()
    values = _FakeValues([[1.0, 2.0]])

    def __getitem__(self, columns):
        indexes = [self.columns.index(column) for column in columns]
        rows = [[row[index] for index in indexes] for row in self.values.tolist()]
        return _FakeFrameSelected(columns, rows, index=self.index)


class _FakeFrameSelected:
    def __init__(self, columns, rows, index=None):
        self.columns = list(columns)
        self.index = index or _FakeIndex()
        self.values = _FakeValues(rows)


class _FakeContext:
    def __init__(self):
        self.full_tick_codes = None
        self.history_call = None
        self.trade_days_call = None
        self.security_info_call = None
        self.sector_calls = []

    def get_full_tick(self, stock_code=None):
        self.full_tick_codes = list(stock_code or [])
        return {"000001.SZ": {"last_price": 12.3}}

    def get_market_data_ex(self, fields, stock_code, **kwargs):
        self.history_call = (fields, stock_code, kwargs)
        return {stock_code[0]: _FakeFrame()}

    def get_trading_dates(self, security, start, end, count, period):
        self.trade_days_call = (security, start, end, count, period)
        return ["20260701"]

    def get_instrument_detail(self, security, is_complete=True):
        self.security_info_call = (security, is_complete)
        return {
            "InstrumentName": "Ping An Bank",
            "InstrumentID": security.split(".", 1)[0],
            "ProductType": "stock",
            "OpenDate": 19910403,
            "ExpireDate": 99999999,
        }

    def get_stock_list_in_sector(self, sector):
        self.sector_calls.append(sector)
        return ["000001.XSHE", "000002.XSHE"]

    def get_divid_factors(self, security):
        return {
            "20190101": [0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.9],
            "20200101": [0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0],
        }


class _FakeContextWithIndexWeight(_FakeContext):
    def __init__(self):
        super().__init__()
        self.index_weight_call = None

    def get_index_weight(self, index_symbol):
        self.index_weight_call = index_symbol
        return {"600000.SH": 0.4, "000001.SZ": 0.6}


class _FakeContextTradeDaysFallback(_FakeContext):
    def get_trading_dates(self, security, start, end, count, period):
        self.trade_days_call = (security, start, end, count, period)
        return []


class _FakeContextHistoryWithStime(_FakeContext):
    def get_market_data_ex(self, fields, stock_code, **kwargs):
        self.history_call = (fields, stock_code, kwargs)
        return {stock_code[0]: _FakeFrameWithStime()}


class _FakeAccount:
    m_dAvailable = 123.0
    m_dBalance = 456.0
    m_dInstrumentValue = 78.0


class _FakeOrder:
    def __init__(self, order_id, code="000001", exchange="SZ", volume=100, traded=0, remark="fake"):
        self.m_strOrderSysID = order_id
        self.m_strInstrumentID = code
        self.m_strExchangeID = exchange
        self.m_nOrderStatus = 50
        self.m_nVolumeTotal = volume - traded
        self.m_nVolumeTotalOriginal = volume
        self.m_nVolumeTraded = traded
        self.m_dTradedPrice = 0.0
        self.m_dLimitPrice = 10.0
        self.m_strRemark = remark


class _FakeTrade:
    def __init__(self, trade_id, order_id, code="000001", exchange="SZ", volume=100, remark=""):
        self.m_strTradeID = trade_id
        self.m_strOrderSysID = order_id
        self.m_strInstrumentID = code
        self.m_strExchangeID = exchange
        self.m_nVolume = volume
        self.m_dTradePrice = 10.1
        self.m_dPrice = 10.2
        self.m_strRemark = remark


def test_big_qmt_helper_keeps_current_tick_as_snapshot_capability():
    helper = _load_helper()
    context = _FakeContext()

    response = helper._dispatch_qmt_action(
        context,
        "current_tick",
        {"security": "000001.XSHE", "request_id": "r-current"},
    )

    assert response["ok"] is True
    assert response["value"]["ticks"]["000001.XSHE"]["last_price"] == 12.3
    assert response["value"]["qmt_codes"] == ["000001.SZ"]
    assert context.full_tick_codes == ["000001.SZ"]


def test_big_qmt_gateway_handler_reads_request_headers():
    helper = _load_helper()
    handler = object.__new__(helper._GatewayHandler)
    handler.request = SimpleNamespace(headers=HTTPHeaders({"X-BulletTrade-Request-Id": "r-headers"}))

    assert handler._request_id() == "r-headers"


def test_runtime_health_reports_gateway_build_id():
    helper = _load_helper()
    runtime = helper._GatewayRuntime()

    health = runtime.health()

    assert health["gateway_build_id"] == helper.GATEWAY_BUILD_ID


def test_runtime_reports_context_missing_for_current_tick_without_context():
    helper = _load_helper()
    runtime = helper._GatewayRuntime()
    runtime.direct_dispatch = True

    response = runtime.submit("current_tick", {"security": "000001.XSHE", "request_id": "r-no-context"})

    assert response["ok"] is False
    assert response["code"] == "QMT_CONTEXT_NOT_READY"
    assert runtime.request_queue.qsize() == 0


def test_runtime_dispatches_account_actions_without_context(monkeypatch):
    helper = _load_helper()
    monkeypatch.setattr(helper, "ACCOUNT_ID", "change_me_account_id")
    runtime = helper._GatewayRuntime()
    runtime.direct_dispatch = True

    monkeypatch.setattr(
        helper,
        "get_trade_detail_data",
        lambda account_id, account_type, detail_type, *args: [_FakeAccount()] if detail_type == "account" else [],
        raising=False,
    )

    response = runtime.submit("account", {"request_id": "r-account"})

    assert response["ok"] is False
    assert response["code"] == "ACCOUNT_NOT_CONFIGURED"
    assert runtime.request_queue.qsize() == 0

    response = runtime.submit("account", {"request_id": "r-account-2", "account_id": "demo"})

    assert response["ok"] is True
    assert response["value"]["account_id"] == "demo"
    assert response["value"]["available_cash"] == 123.0
    assert response["value"]["total_value"] == 456.0


def test_runtime_bypasses_queue_for_account_actions_even_with_context(monkeypatch):
    helper = _load_helper()
    runtime = helper._GatewayRuntime()
    runtime.direct_dispatch = False
    runtime.context_info = _FakeContext()

    monkeypatch.setattr(
        helper,
        "get_trade_detail_data",
        lambda account_id, account_type, detail_type, *args: [_FakeAccount()] if detail_type == "account" else [],
        raising=False,
    )

    response = runtime.submit("account", {"request_id": "r-account-direct", "account_id": "demo"})

    assert response["ok"] is True
    assert response["value"]["account_id"] == "demo"
    assert runtime.request_queue.qsize() == 0


def test_runtime_does_not_queue_when_context_missing_even_in_queue_mode():
    helper = _load_helper()
    runtime = helper._GatewayRuntime()
    runtime.direct_dispatch = False

    response = runtime.submit("history", {"security": "000001.XSHE", "request_id": "r-history"})

    assert response["ok"] is False
    assert response["code"] == "QMT_CONTEXT_NOT_READY"
    assert runtime.request_queue.qsize() == 0


def test_big_qmt_helper_dispatches_non_tick_data_apis(monkeypatch):
    helper = _load_helper()
    context = _FakeContext()
    download_calls = []
    monkeypatch.setattr(
        helper,
        "download_history_data",
        lambda *args, **kwargs: download_calls.append((args, kwargs)),
        raising=False,
    )

    history = helper._dispatch_qmt_action(
        context,
        "history",
        {"security": "000001.XSHE", "fields": "open,close", "frequency": "daily"},
    )
    assert history["ok"] is True
    assert history["value"]["dtype"] == "dataframe"
    assert history["value"]["records"] == [[1.0, 2.0]]
    assert context.history_call[0] == ["open", "close"]
    assert context.history_call[1] == ["000001.SZ"]
    assert context.history_call[2]["period"] == "1d"
    assert download_calls[0][0] == ("000001.SZ", "1d", "", "")

    trade_days = helper._dispatch_qmt_action(context, "trade_days", {"security": "000001.XSHE", "count": 1})
    assert trade_days["ok"] is True
    assert trade_days["value"]["values"] == ["20260701"]
    assert context.trade_days_call[0] == "000001.SZ"

    security_info = helper._dispatch_qmt_action(context, "security_info", {"security": "000001.XSHE"})
    assert security_info["ok"] is True
    assert security_info["value"]["display_name"] == "Ping An Bank"
    assert security_info["value"]["qmt_security"] == "000001.SZ"
    assert security_info["value"]["qmt_code"] == "000001.SZ"
    assert security_info["value"]["code"] == "000001.XSHE"
    assert security_info["value"]["start_date"] == "1991-04-03T00:00:00"
    assert security_info["value"]["end_date"] == "2200-01-01T00:00:00"
    assert context.security_info_call[0] == "000001.SZ"

    cache = helper._dispatch_qmt_action(
        context,
        "ensure_cache",
        {"security": "000001.XSHE", "frequency": "minute", "start": "20260701", "end": "20260701"},
    )
    assert cache["ok"] is True
    assert download_calls[1][0] == ("000001.SZ", "1m", "20260701", "20260701")

    all_securities = helper._dispatch_qmt_action(context, "all_securities", {"types": ["stock"]})
    assert all_securities["ok"] is True
    assert all_securities["value"]["columns"] == [
        "display_name",
        "name",
        "start_date",
        "end_date",
        "type",
        "qmt_code",
    ]
    assert all_securities["value"]["records"][0][0] == "000001.XSHE"
    assert all_securities["value"]["records"][0][1] == "Ping An Bank"
    assert all_securities["value"]["records"][0][3] == "1991-04-03T00:00:00"
    assert all_securities["value"]["records"][0][4] is None
    assert all_securities["value"]["records"][0][5] == "stock"

    etf_securities = helper._dispatch_qmt_action(context, "all_securities", {"types": ["etf"]})
    assert etf_securities["ok"] is True
    assert context.sector_calls[-1] == "\u6caa\u6df1ETF"
    assert etf_securities["value"]["records"][0][5] == "etf"

    index_stocks = helper._dispatch_qmt_action(context, "index_stocks", {"index_symbol": "000300.XSHG"})
    assert index_stocks["ok"] is True
    assert index_stocks["value"]["stocks"] == ["000001.XSHE", "000002.XSHE"]
    assert index_stocks["value"]["source"] == "sector_fallback"

    split_dividend = helper._dispatch_qmt_action(
        context,
        "split_dividend",
        {"security": "000001.XSHE", "start": "20200101", "end": "20201231"},
    )
    assert split_dividend["ok"] is True
    assert split_dividend["value"]["events"] == [
        {
            "security": "000001.XSHE",
            "date": "2020-01-01",
            "security_type": "stock",
            "scale_factor": 1.0,
            "bonus_pre_tax": 1.0,
            "per_base": 10,
        }
    ]


def test_big_qmt_helper_history_drops_unrequested_stime_column(monkeypatch):
    helper = _load_helper()
    context = _FakeContextHistoryWithStime()
    monkeypatch.setattr(helper, "download_history_data", lambda *args, **kwargs: None, raising=False)

    history = helper._dispatch_qmt_action(
        context,
        "history",
        {"security": "000001.XSHE", "fields": ["open", "close"], "count": 1},
    )

    assert history["ok"] is True
    assert history["value"]["columns"] == ["open", "close"]
    assert history["value"]["records"] == [[1.0, 2.0]]


def test_big_qmt_helper_history_uses_miniqmt_ratio_dividend_types(monkeypatch):
    helper = _load_helper()
    context = _FakeContext()
    monkeypatch.setattr(helper, "download_history_data", lambda *args, **kwargs: None, raising=False)

    helper._dispatch_qmt_action(
        context,
        "history",
        {"security": "000001.XSHE", "fields": ["open"], "fq": "pre", "count": 1},
    )
    assert context.history_call[2]["dividend_type"] == "front_ratio"

    helper._dispatch_qmt_action(
        context,
        "history",
        {"security": "000001.XSHE", "fields": ["open"], "fq": "post", "count": 1},
    )
    assert context.history_call[2]["dividend_type"] == "back_ratio"


def test_big_qmt_helper_history_returns_error_when_auto_ensure_fails():
    helper = _load_helper()
    context = _FakeContext()

    history = helper._dispatch_qmt_action(
        context,
        "history",
        {"security": "000001.XSHE", "fields": ["open"], "count": 1},
    )

    assert history["ok"] is False
    assert history["code"] == "QMT_API_NOT_READY"
    assert context.history_call is None


def test_big_qmt_helper_history_allows_explicit_auto_download_false():
    helper = _load_helper()
    context = _FakeContext()

    history = helper._dispatch_qmt_action(
        context,
        "history",
        {"security": "000001.XSHE", "fields": ["open"], "count": 1, "auto_download": False},
    )

    assert history["ok"] is True
    assert context.history_call[1] == ["000001.SZ"]


def test_big_qmt_helper_index_stocks_prefers_index_weight(monkeypatch):
    helper = _load_helper()
    context = _FakeContextWithIndexWeight()
    download_calls = []
    monkeypatch.setattr(helper, "download_index_weight", lambda *args, **kwargs: download_calls.append(args), raising=False)

    response = helper._dispatch_qmt_action(context, "index_stocks", {"index_symbol": "000300.XSHG"})

    assert response["ok"] is True
    assert response["value"]["stocks"] == ["600000.XSHG", "000001.XSHE"]
    assert response["value"]["values"] == ["600000.XSHG", "000001.XSHE"]
    assert response["value"]["source"] == "get_index_weight"
    assert context.index_weight_call == "000300.SH"
    assert context.sector_calls == []
    assert download_calls == [()]


def test_big_qmt_helper_history_rounds_adjusted_prices_like_miniqmt():
    helper = _load_helper()

    stock = helper._normalize_history_payload(
        {
            "dtype": "dataframe",
            "columns": ["open", "high", "low", "close", "volume"],
            "records": [[10.51463626, 10.53400023, 10.4178164, 10.42749839, 747632.0]],
        },
        "000001.XSHE",
    )
    etf = helper._normalize_history_payload(
        {
            "dtype": "dataframe",
            "columns": ["open", "high", "low", "close", "volume"],
            "records": [[6.15851537, 6.20554635, 6.15215984, 6.19156417, 9597201.0]],
        },
        "510300.XSHG",
    )

    assert stock["records"] == [[10.51, 10.53, 10.42, 10.43, 747632.0]]
    assert etf["records"] == [[6.159, 6.206, 6.152, 6.192, 9597201.0]]


def test_big_qmt_helper_minute_history_uses_miniqmt_volume_unit():
    helper = _load_helper()

    minute = helper._normalize_history_payload(
        {
            "dtype": "dataframe",
            "columns": ["open", "close", "volume"],
            "records": [[10.29, 10.29, 7724.0]],
        },
        "000001.XSHE",
        "1m",
    )

    assert minute["records"] == [[10.29, 10.29, 772400.0]]


def test_big_qmt_helper_trade_days_falls_back_to_daily_history_index():
    helper = _load_helper()
    context = _FakeContextTradeDaysFallback()

    response = helper._dispatch_qmt_action(
        context,
        "trade_days",
        {"security": "000001.XSHE", "start": "20260701", "end": "20260702", "count": 2},
    )

    assert response["ok"] is True
    assert response["value"]["values"] == ["20260701", "20260702"]


def test_big_qmt_helper_trade_days_normalizes_dash_dates_for_qmt():
    helper = _load_helper()
    context = _FakeContext()

    response = helper._dispatch_qmt_action(
        context,
        "trade_days",
        {"security": "000001.XSHE", "start": "2026-07-03", "end": "2026-07-03", "count": 1},
    )

    assert response["ok"] is True
    assert context.trade_days_call == ("000001.SZ", "20260703", "20260703", 1, "1d")


def test_big_qmt_helper_trade_days_fallback_uses_normalized_dates():
    helper = _load_helper()
    context = _FakeContextTradeDaysFallback()

    response = helper._dispatch_qmt_action(
        context,
        "trade_days",
        {"security": "000001.XSHE", "start": "2026-07-01", "end": "2026-07-02", "count": 2},
    )

    assert response["ok"] is True
    assert context.history_call[2]["start_time"] == "20260701"
    assert context.history_call[2]["end_time"] == "20260702"


def test_big_qmt_helper_trade_days_uses_default_security_when_missing_symbol():
    helper = _load_helper()
    context = _FakeContext()

    response = helper._dispatch_qmt_action(
        context,
        "trade_days",
        {"start": "20260701", "end": "20260702", "count": 2},
    )

    assert response["ok"] is True
    assert context.trade_days_call[0] == "000001.SZ"


def test_big_qmt_helper_merges_order_and_trade_sources(monkeypatch):
    helper = _load_helper()
    qmt_order = _FakeOrder("order-qmt")
    manual_order = _FakeOrder("order-manual", code="600000", exchange="SH")
    qmt_trade = _FakeTrade("trade-qmt", "order-qmt")
    manual_trade = _FakeTrade("trade-manual", "order-manual", code="600000", exchange="SH")

    def fake_getter(account_id, account_type, detail_type, *args):
        source = args[0] if args else None
        if detail_type == "order" and source == "qmt":
            return [qmt_order]
        if detail_type == "order" and source is None:
            return [qmt_order, manual_order]
        if detail_type == "deal" and source == "qmt":
            return [qmt_trade]
        if detail_type == "trade" and source is None:
            return [manual_trade]
        return []

    monkeypatch.setattr(helper, "get_trade_detail_data", fake_getter, raising=False)

    orders = helper._query_orders("demo", "stock")
    trades = helper._query_trades("demo", "stock")

    assert [item["order_id"] for item in orders] == ["order-qmt", "order-manual"]
    assert [item["trade_id"] for item in trades] == ["trade-qmt", "trade-manual"]
    assert orders[1]["security"] == "600000.XSHG"


def test_big_qmt_helper_debug_scans_trade_detail_combinations(monkeypatch):
    helper = _load_helper()

    def fake_getter(account_id, account_type, detail_type, *args):
        source = args[0] if args else None
        if detail_type == "order" and source == "qmt":
            return [_FakeOrder("order-qmt")]
        if detail_type == "order" and source is None:
            return [_FakeOrder("order-default")]
        if detail_type == "deal":
            return [_FakeTrade("trade", "order-qmt")]
        return []

    monkeypatch.setattr(helper, "get_trade_detail_data", fake_getter, raising=False)

    response = helper._dispatch_qmt_action(
        None,
        "debug_trade_detail",
        {
            "account_id": "demo",
            "detail_types": "order,deal",
            "sources": "qmt,default",
            "limit": 1,
            "request_id": "r-debug",
        },
    )

    assert response["ok"] is True
    results = response["value"]["results"]
    by_key = {(item["detail_type"], item["source"]): item for item in results}
    assert by_key[("order", "qmt")]["row_count"] == 1
    assert by_key[("order", "default")]["samples"][0]["m_strOrderSysID"] == "order-default"
    assert by_key[("deal", "qmt")]["row_count"] == 1


def test_big_qmt_helper_accepts_article_order_payload(monkeypatch):
    helper = _load_helper()
    calls = []

    def fake_passorder(*args):
        calls.append(args)
        return "order-ref-1"

    monkeypatch.setattr(helper, "passorder", fake_passorder, raising=False)

    response = helper._dispatch_qmt_action(
        _FakeContext(),
        "place_order",
        {
            "stock": "000001.SZ",
            "volume": 100,
            "price": 10.0,
            "prType": 5,
            "side": "BUY",
            "account_id": TEST_ACCOUNT_ID,
            "request_id": "r-order",
        },
    )

    assert response["ok"] is True
    assert response["value"]["order_ref"] == "order-ref-1"
    assert response["value"]["passorder_return_type"] == "str"
    assert response["value"]["security"] == "000001.XSHE"
    assert response["value"]["qmt_security"] == "000001.SZ"
    assert calls[0][0] == 23
    assert calls[0][1] == 1101
    assert calls[0][2] == TEST_ACCOUNT_ID
    assert calls[0][3] == "000001.SZ"
    assert calls[0][4] == 5
    assert calls[0][5] == 10.0
    assert calls[0][6] == 100
    assert calls[0][7] == "qmt"
    assert calls[0][8] == helper.PASSORDER_QUICK_TRADE
    assert str(calls[0][9]).startswith("BT")
    assert isinstance(calls[0][10], _FakeContext)
    assert response["value"]["passorder_signature"] == "official_user_order_id"


def test_big_qmt_helper_marks_zero_passorder_return_submit_unknown(monkeypatch, tmp_path):
    helper = _load_helper()

    monkeypatch.setattr(helper, "LOG_FILE", str(tmp_path / "gateway.log"))
    helper.ORDER_TAG_STORE_LOADED = False
    helper.ORDER_TAGS_BY_ID = {}
    helper.PENDING_ORDER_TAGS = []
    monkeypatch.setattr(helper, "passorder", lambda *args: 0, raising=False)

    response = helper._dispatch_qmt_action(
        _FakeContext(),
        "place_order",
        {
            "stock": "000001.SZ",
            "volume": 100,
            "price": 10.0,
            "prType": 11,
            "side": "BUY",
            "account_id": TEST_ACCOUNT_ID,
            "sub_account_id": "sub-a",
            "order_remark": "bt:zero",
            "qmt_user_order_id": "BTZERO",
            "request_id": "r-order-zero",
        },
    )

    assert response["ok"] is True
    assert response["value"]["status"] == "submit_unknown"
    assert response["value"]["submit_unknown"] is True
    assert response["value"]["order_id"] == ""
    assert response["value"]["passorder_return"] == 0
    assert response["value"]["quick_trade"] == helper.PASSORDER_QUICK_TRADE
    assert response["value"]["order_tag_recorded"] is True
    assert helper.PENDING_ORDER_TAGS[-1]["order_remark"] == "sub:sub-a|bt:zero"
    assert helper.PENDING_ORDER_TAGS[-1]["qmt_user_order_id"] == "BTZERO"


def test_big_qmt_helper_allows_request_quick_trade_override(monkeypatch):
    helper = _load_helper()
    calls = []

    def fake_passorder(*args):
        calls.append(args)
        return "order-ref-quick"

    monkeypatch.setattr(helper, "passorder", fake_passorder, raising=False)

    response = helper._dispatch_qmt_action(
        _FakeContext(),
        "place_order",
        {
            "stock": "000001.SZ",
            "volume": 100,
            "price": 10.0,
            "prType": 11,
            "side": "BUY",
            "account_id": TEST_ACCOUNT_ID,
            "quick_trade": 0,
            "request_id": "r-order-quick",
        },
    )

    assert response["ok"] is True
    assert response["value"]["quick_trade"] == 0
    assert calls[0][8] == 0


def test_big_qmt_helper_encodes_virtual_account_and_sends_qmt_user_order_id(monkeypatch, tmp_path):
    helper = _load_helper()
    calls = []

    def fake_passorder(*args):
        calls.append(args)
        return "order-ref-sub"

    monkeypatch.setattr(helper, "LOG_FILE", str(tmp_path / "gateway.log"))
    helper.ORDER_TAG_STORE_LOADED = False
    helper.ORDER_TAGS_BY_ID = {}
    helper.PENDING_ORDER_TAGS = []
    monkeypatch.setattr(helper, "passorder", fake_passorder, raising=False)

    response = helper._dispatch_qmt_action(
        _FakeContext(),
        "place_order",
        {
            "security": "000001.XSHE",
            "amount": 100,
            "price": 10.0,
            "side": "BUY",
            "account_id": TEST_ACCOUNT_ID,
            "sub_account_id": "sub-a",
            "order_remark": "bt:alpha:abcd1234",
            "qmt_user_order_id": "QMT-USER-1",
            "request_id": "r-order-sub",
        },
    )

    assert response["ok"] is True
    assert response["value"]["order_remark"] == "sub:sub-a|bt:alpha:abcd1234"
    assert response["value"]["qmt_user_order_id"] == "QMT-USER-1"
    assert response["value"]["passorder_signature"] == "official_user_order_id"
    assert response["value"]["sub_account_id"] == "sub-a"
    assert response["value"]["order_tag_recorded"] is True
    assert len(calls[0]) == 11
    assert calls[0][7] == "qmt"
    assert calls[0][9] == "QMT-USER-1"


def test_big_qmt_helper_falls_back_to_article_passorder_signature_for_remark(monkeypatch, tmp_path):
    helper = _load_helper()
    calls = []

    def fake_passorder(*args):
        calls.append(args)
        if len(args) == 11:
            raise TypeError("old passorder signature")
        return "order-ref-old"

    monkeypatch.setattr(helper, "LOG_FILE", str(tmp_path / "gateway.log"))
    helper.ORDER_TAG_STORE_LOADED = False
    helper.ORDER_TAGS_BY_ID = {}
    helper.PENDING_ORDER_TAGS = []
    monkeypatch.setattr(helper, "PASSORDER_USE_REMARK_SIGNATURE", True)
    monkeypatch.setattr(helper, "passorder", fake_passorder, raising=False)

    response = helper._dispatch_qmt_action(
        _FakeContext(),
        "place_order",
        {
            "security": "000001.XSHE",
            "amount": 100,
            "price": 10.0,
            "side": "BUY",
            "account_id": TEST_ACCOUNT_ID,
            "sub_account_id": "sub-a",
            "order_remark": "bt:alpha:abcd1234",
            "request_id": "r-order-old",
        },
    )

    assert response["ok"] is True
    assert [len(item) for item in calls] == [11, 10]
    assert calls[-1][7] == "qmt"
    assert response["value"]["passorder_signature"] == "article"
    assert response["value"]["passorder_fallback_error"] == "old passorder signature"


def test_big_qmt_helper_filters_orders_and_trades_by_virtual_account(monkeypatch, tmp_path):
    helper = _load_helper()
    order_a = _FakeOrder("order-a", remark="sub:sub-a|bt:a")
    order_b = _FakeOrder("order-b", code="600000", exchange="SH", remark="sub:sub-b|bt:b")
    trade_a = _FakeTrade("trade-a", "order-a", remark="")
    trade_b = _FakeTrade("trade-b", "order-b", code="600000", exchange="SH", remark="")

    def fake_getter(account_id, account_type, detail_type, *args):
        source = args[0] if args else None
        if source == "qmt":
            return []
        if detail_type == "order":
            return [order_a, order_b]
        if detail_type == "deal":
            return [trade_a, trade_b]
        return []

    monkeypatch.setattr(helper, "get_trade_detail_data", fake_getter, raising=False)

    orders = helper._dispatch_qmt_action(
        None,
        "orders",
        {"account_id": "demo", "sub_account_id": "sub-a", "request_id": "r-orders-sub"},
    )
    trades = helper._dispatch_qmt_action(
        None,
        "trades",
        {"account_id": "demo", "sub_account_id": "sub-a", "request_id": "r-trades-sub"},
    )

    assert [item["order_id"] for item in orders["value"]["orders"]] == ["order-a"]
    assert orders["value"]["orders"][0]["sub_account_id"] == "sub-a"
    assert [item["trade_id"] for item in trades["value"]["trades"]] == ["trade-a"]
    assert trades["value"]["trades"][0]["order_remark"] == "sub:sub-a|bt:a"
    assert trades["value"]["trades"][0]["sub_account_id"] == "sub-a"


def test_big_qmt_helper_matches_pending_virtual_order_tag_when_qmt_remark_is_short_id(monkeypatch, tmp_path):
    helper = _load_helper()
    order = _FakeOrder("order-new", volume=100, traded=0, remark="BTSHORTID")
    order.m_dLimitPrice = 1.0
    trade = _FakeTrade("trade-new", "order-new", remark="BTSHORTID")

    monkeypatch.setattr(helper, "LOG_FILE", str(tmp_path / "gateway.log"))
    helper.ORDER_TAG_STORE_LOADED = False
    helper.ORDER_TAGS_BY_ID = {}
    helper.PENDING_ORDER_TAGS = []
    helper._remember_order_tag(
        None,
        {
            "security": "000001.XSHE",
            "qmt_security": "000001.SZ",
            "side": "BUY",
            "amount": 100,
            "price": 1.0,
            "qmt_user_order_id": "BTSHORTID",
            "order_remark": "sub:sub-a|bt:a",
            "remark": "sub:sub-a|bt:a",
            "strategy_name": "alpha",
            "sub_account_id": "sub-a",
            "virtual_account_id": "sub-a",
        },
    )

    def fake_getter(account_id, account_type, detail_type, *args):
        source = args[0] if args else None
        if source == "qmt":
            return []
        if detail_type == "order":
            return [order]
        if detail_type == "deal":
            return [trade]
        return []

    monkeypatch.setattr(helper, "get_trade_detail_data", fake_getter, raising=False)

    orders = helper._dispatch_qmt_action(
        None,
        "orders",
        {"account_id": "demo", "sub_account_id": "sub-a", "request_id": "r-orders-pending"},
    )
    trades = helper._dispatch_qmt_action(
        None,
        "trades",
        {"account_id": "demo", "sub_account_id": "sub-a", "request_id": "r-trades-pending"},
    )

    assert [item["order_id"] for item in orders["value"]["orders"]] == ["order-new"]
    assert orders["value"]["orders"][0]["order_remark"] == "sub:sub-a|bt:a"
    assert orders["value"]["orders"][0]["qmt_user_order_id"] == "BTSHORTID"
    assert orders["value"]["orders"][0]["sub_account_id"] == "sub-a"
    assert [item["trade_id"] for item in trades["value"]["trades"]] == ["trade-new"]
    assert trades["value"]["trades"][0]["order_remark"] == "sub:sub-a|bt:a"
    assert trades["value"]["trades"][0]["qmt_user_order_id"] == "BTSHORTID"
    assert trades["value"]["trades"][0]["sub_account_id"] == "sub-a"


def test_big_qmt_helper_uses_deal_price_fallback():
    helper = _load_helper()
    trade = _FakeTrade("trade-price", "order-price")
    trade.m_dTradePrice = 0.0
    trade.m_dPrice = 8.75

    result = helper._trade_to_dict(trade)

    assert result["price"] == 8.75


def test_big_qmt_helper_date_filter_accepts_epoch_milliseconds():
    helper = _load_helper()

    assert helper._date_digits("673113600000") == "19910502"
    assert helper._date_in_range("673113600000", "19900101", "19991231") is True
    assert helper._date_in_range("673113600000", "20100101", "20260701") is False
    assert helper._date_in_range("20260701000000", "20260101", "20260701") is True


def test_big_qmt_helper_split_dividend_filters_epoch_millisecond_keys(monkeypatch):
    helper = _load_helper()

    class _Context:
        def get_divid_factors(self, qmt_security):
            assert qmt_security == "000001.SZ"
            return {
                "673113600000": [1, 2, 3],
                "1278000000000": [4, 5, 6],
            }

    result = helper._dispatch_qmt_action(
        _Context(),
        "split_dividend",
        {
            "security": "000001.XSHE",
            "start": "20100101",
            "end": "20260701",
            "request_id": "r-dividend-filter",
        },
    )

    assert result["ok"] is True
    assert [item["date"] for item in result["value"]["events"]] == [helper._date_iso("1278000000000")]
    assert result["value"]["events"][0]["bonus_pre_tax"] == 40.0
    assert result["value"]["events"][0]["per_base"] == 10
