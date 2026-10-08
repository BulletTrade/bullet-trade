import json
import sys
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from bullet_trade.integrations.ths.actor_service import CollectedSnapshot
from bullet_trade.integrations.ths.windows.driver import (
    DriverBlocked, NativeBackend, ObservedQuery, Profile, WindowsDriver,
    _safe_captcha_diagnostics, funds_data, make_driver,
)
from bullet_trade.integrations.ths.windows.native_actions import (
    NativeActionBlocked, NativeActions, parse_order_confirmation,
    parse_order_receipt,
)
from bullet_trade.integrations.ths.request_store import Request


class VerifiedFake:
    client_lock_factory = nullcontext

    def __init__(self, account):
        self.account = account

    def query(self, kind, should_yield):
        return CollectedSnapshot(self.account, kind, {}, datetime.now(timezone.utc), True)

    def query_observed(self, kind, should_yield):
        return ObservedQuery(self.account, kind, [], datetime.now(timezone.utc),
                             False, ("whole_page_coverage_unverified",))


def test_captcha_diagnostics_keep_only_safe_fixed_scalars():
    secret = "captcha-or-account-must-not-persist"
    raw = {"edit_readback_length": 0, "edit_readback_shape": "empty",
           "edit_diagnostic": {"same_edit_hwnd": True,
                               "native_wm_gettext_observed": True,
                               "native_wm_gettext_length": 0,
                               "captcha": secret, "window_text": secret}}
    focus = {"identity_ok": True, "initial_length": 0, "attached": True,
             "attach_error": 0, "sent_events": 8, "foreground_requested": False,
             "focus_query_error": 0, "focus_matches_edit": True,
             "sendinput_error": 0, "focus_matches_after": True,
             "detached": True, "detach_error": 0, "text_length_error": 1460,
             "captcha": secret, "exception": secret, "edit_hwnd": 12345}
    safe = _safe_captcha_diagnostics(raw, focus)
    assert safe == {
        "edit_readback_length": 0, "edit_readback_shape": "empty",
        "edit_diagnostic": {"same_edit_hwnd": True,
                            "native_wm_gettext_observed": True,
                            "native_wm_gettext_length": 0},
        "focus_input_diagnostic": {
            "identity_ok": True, "initial_length": 0, "attached": True,
            "attach_error": 0, "sent_events": 8, "foreground_requested": False,
            "focus_query_error": 0, "focus_matches_edit": True,
            "sendinput_error": 0, "focus_matches_after": True,
            "detached": True, "detach_error": 0, "text_length_error": 1460}}
    assert secret not in json.dumps(safe, ensure_ascii=False)


def test_captcha_diagnostics_reject_untrusted_shapes_and_values():
    secret = "sensitive freeform diagnostic"
    safe = _safe_captcha_diagnostics(
        {"edit_readback_length": True, "edit_readback_shape": secret,
         "edit_diagnostic": {"status": secret,
                             "native_wm_gettext_length": -1,
                             "same_edit_hwnd": secret}},
        {"identity_ok": secret, "sent_events": 99,
         "focus_query_error": secret, "attach_error": -1})
    assert safe == {}
    assert _safe_captcha_diagnostics(
        {"edit_diagnostic": {"status": "unavailable"}}, None) == {
            "edit_diagnostic": {"status": "unavailable"}}


def profile_file(tmp_path):
    template_dir = tmp_path / "private-templates"
    template_dir.mkdir()
    path = tmp_path / "private-profile.json"
    path.write_text(json.dumps({
        "account": "sim-account", "expected_card": "1234567890",
        "account_mask": "模拟炒股-TEST**01",
        "ocr_template_dir": str(template_dir),
        "client_exe": "C:/ExampleTradingClient/xiadan.exe",
    }), encoding="utf-8")
    return path


def test_private_profile_and_injected_backend(tmp_path, monkeypatch):
    path = profile_file(tmp_path)
    monkeypatch.setenv("THS_GUI_PROFILE", str(path))
    driver = make_driver(account="sim-account", state_dir=tmp_path,
                         backend=VerifiedFake("sim-account"))
    assert isinstance(driver, WindowsDriver)
    assert driver.profile.expected_card == "1234567890"
    assert driver.ready is False
    driver.backend.ready = True
    assert driver.ready is True
    assert driver.query("account", lambda: False).complete is True
    observed = driver.query_observed("positions", lambda: False)
    assert observed.complete is False
    assert observed.missing == ("whole_page_coverage_unverified",)
    with pytest.raises(DriverBlocked, match="profile_account_mismatch"):
        make_driver(account="other", state_dir=tmp_path,
                    backend=VerifiedFake("other"))
    loaded = Profile.load("sim-account", path)
    assert loaded.ocr_template_dir.is_dir()
    assert loaded.client_exe == "C:/ExampleTradingClient/xiadan.exe"
    raw = json.loads(path.read_text(encoding="utf-8"))
    del raw["client_exe"]
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(DriverBlocked, match="profile_fields_unverified"):
        Profile.load("sim-account", path)


def test_native_adapter_requires_explicit_account_and_executable():
    from bullet_trade.integrations.ths.windows.native.readonly_sampler import (
        SamplingError, WindowsAdapter,
    )
    with pytest.raises(SamplingError, match="client_exe_required"):
        WindowsAdapter(account="模拟炒股-TEST**01")
    with pytest.raises(SamplingError, match="account_mask_required"):
        WindowsAdapter(account="", client_exe="C:/ExampleTradingClient/xiadan.exe")


def test_funds_controls_validate_amounts_and_total_assets():
    def pair(label_id, label, value_id, value, y):
        return [
            {"control_id": label_id, "class_name": "Static", "rectangle": [0, y, 100, y+20],
             "text": label, "visible": True, "main_hwnd": 77},
            {"control_id": value_id, "class_name": "Static", "rectangle": [105, y, 180, y+20],
             "text": value, "visible": True, "main_hwnd": 77},
        ]
    records = (pair(2388, "资金余额", 1012, "100.00", 0)
               + pair(1686, "冻结金额", 1013, "20.00", 30)
               + pair(2005, "可用金额", 1016, "80.00", 60)
               + pair(1688, "总 资 产", 1015, "150.00", 90))
    assert funds_data(records, 77) == {
        "cash_balance": "100.00", "available_cash": "80.00",
        "frozen_cash": "20.00", "total_value": "150.00"}
    changed = [dict(row) for row in records]
    changed[-1]["text"] = "90.00"
    with pytest.raises(DriverBlocked, match="funds_relation_unverified"):
        funds_data(changed, 77)


def test_private_ncc_templates_require_all_ten_digits(tmp_path):
    from PIL import Image
    from bullet_trade.integrations.ths.windows.native.template_ncc import NccTemplateEngine

    engine = NccTemplateEngine(tmp_path)
    assert engine.warmup() is False
    for digit in range(10):
        image = Image.new("L", (20, 20), 255)
        image.putpixel((digit, 1), 0)
        image.save(tmp_path / (str(digit) + "-sample.png"))
    assert engine.warmup() is True
    assert set(engine._template_labels.tolist()) == set(range(10))


def _request(kind, params):
    return Request("request-1", "sim-account",
                   datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat(),
                   "key", kind, params, {"virtual_account_id": "v1"},
                   "preparing", time.time() + 60, None, None, time.time(), time.time())


class FakeNativeHost:
    def __init__(self, *, rows=(), available="1000", complete=True):
        self.profile = SimpleNamespace(account="sim-account", account_mask="模拟炒股-TEST**01")
        self.adapter = SimpleNamespace(locate=lambda: (100, 200),
                                       app=SimpleNamespace(process=300))
        self.rows = rows
        self.available = available
        self.complete = complete
        self.identities = 0
        self.store = SimpleNamespace(get_by_contract=lambda *args: None)

    def _identity(self):
        self.identities += 1
        return "1234567890"

    def _account_query(self):
        return SimpleNamespace(data={"available_cash": self.available})

    def query_observed(self, kind, should_yield):
        return ObservedQuery("sim-account", kind, None,
                             datetime.now(timezone.utc), self.complete,
                             () if self.complete else ("whole_page_coverage_unverified",), self.rows)


def test_native_preflight_uses_only_operation_needed_observation():
    buy = _request("limit_buy", {"security": "518880.XSHG", "price": "8.500", "quantity": 100})
    host = FakeNativeHost(available="850")
    proof = NativeActions(host).preflight(buy)
    assert proof.allowed and proof.queries_complete and host.identities >= 2
    denied_actions = NativeActions(FakeNativeHost(available="849.99"))
    denied = denied_actions.preflight(buy)
    assert denied.allowed is False and denied.queries_complete is False
    assert denied_actions.abort_prepared(buy) is None

    sell = _request("limit_sell", buy.params)
    rows = ({"证券代码": "600000", "可用余额": "50"},
            {"证券代码": "518880", "可用余额": "100"})
    assert NativeActions(FakeNativeHost(rows=rows)).preflight(sell).allowed
    assert NativeActions(FakeNativeHost(rows=rows + (rows[1],))).preflight(sell).allowed is False
    assert NativeActions(FakeNativeHost(rows=rows, complete=False)).preflight(sell).allowed is False


def test_confirmation_mismatch_and_receipt_without_contract_stay_unknown():
    detail = "资金帐号：模拟炒股-TEST**01\n证券代码：518880\n买入价格：8.500\n买入数量：100\n您是否确定以上买入委托？"
    records = [
        {"id": 1365, "class": "Static", "text": "委托确认", "visible": True},
        {"id": 1040, "class": "Static", "text": detail, "visible": True},
        {"id": 6, "class": "Button", "text": "是(&Y)", "visible": True,
         "enabled": True, "hwnd": 10},
    ]
    assert parse_order_confirmation(records, account_mask="模拟炒股-TEST**01", side="buy",
                                    code="518880", price=Decimal("8.500"), quantity=100) == 10
    with pytest.raises(NativeActionBlocked, match="confirmation_price_mismatch"):
        parse_order_confirmation(records, account_mask="模拟炒股-TEST**01", side="buy",
                                 code="518880", price=Decimal("8.501"), quantity=100)
    assert parse_order_receipt([{"class": "Static", "text": "委托已提交",
                                 "visible": True}]).status == "unknown"


def test_cancel_preflight_refuses_multirow_even_if_target_matches():
    request = _request("cancel", {"broker_contract_no": "000123"})
    rows = ({"合同编号": "000123"}, {"合同编号": "000124"})
    host = FakeNativeHost(rows=rows)
    original = SimpleNamespace(account=request.account, trade_day=request.trade_day,
        state="accepted", broker_contract_no="000123", kind="limit_buy",
        params={"security": "518880.XSHG", "quantity": 100, "price": "8.500"},
        origin={"virtual_account_id": "v1"})
    host.store.get_by_contract = lambda *args: original
    assert NativeActions(host).preflight(request).allowed is False

    one = FakeNativeHost(rows=({"合同编号": "000123", "证券代码": "518880",
                                "操作": "买入", "备注": "未成交",
                                "委托数量": "100", "成交数量": "0",
                                "交易市场": "上海Ａ股", "委托价格": "8.500"},))
    one.store.get_by_contract = lambda *args: original
    actions = NativeActions(one)
    proof = actions.preflight(request)
    assert proof.allowed is True  # One complete candidate; no checkbox assumption.


def test_pre_submit_prepare_failure_yields_local_abort_proof(monkeypatch):
    request = _request("limit_buy", {"security": "518880.XSHG", "price": "8.500",
                                     "quantity": 100})
    actions = NativeActions(FakeNativeHost(available="1000"))
    assert actions.preflight(request).allowed
    monkeypatch.setattr(actions, "_prepare_order",
                        lambda _: (_ for _ in ()).throw(NativeActionBlocked("form_unverified")))
    monkeypatch.setattr(actions, "_dialogs", lambda main, pid: [])
    assert actions.prepare(request) is None
    proof = actions.validate_readback(request)
    assert proof.allowed is False and proof.input_matches is False
    assert actions.abort_prepared(request) is None


def test_reused_dialog_handle_and_private_evidence(tmp_path, monkeypatch):
    host = FakeNativeHost()
    host.state_dir = tmp_path
    actions = NativeActions(host)
    old = (17, [{"class": "Static", "text": "委托确认", "visible": True}])
    new = (17, [{"class": "Static", "text": "委托已提交 合同编号：000123",
                 "visible": True}])
    monkeypatch.setattr(actions, "_dialogs", lambda main, pid: [new])
    assert actions._wait_one_dialog(100, 300, 0.1, previous=old) == new
    request = _request("limit_buy", {"security": "518880.XSHG", "price": "8.500",
                                     "quantity": 100})
    digest = actions._save_evidence(request, "receipt", new)
    evidence = tmp_path / "driver-evidence" / (digest + ".json")
    assert evidence.is_file()
    assert "委托已提交" in evidence.read_text(encoding="utf-8")


def test_main_identity_can_be_verified_on_information_page_without_grid():
    from bullet_trade.integrations.ths.windows.native.readonly_sampler import (
        SamplingError, WindowsAdapter,
    )
    native = WindowsAdapter.__new__(WindowsAdapter)
    native.account = 'configured-account'
    window = SimpleNamespace(handle=77, is_visible=lambda: True,
        child_window=lambda **kwargs: SimpleNamespace(wrapper_object=lambda:
            SimpleNamespace(selected_text=lambda: native.account)),
        descendants=lambda: [])
    native._windows = lambda: [window]
    native.gui = SimpleNamespace(IsWindow=lambda hwnd: True,
        GetWindowText=lambda hwnd: '网上股票交易系统5.0')
    native.app = SimpleNamespace(window=lambda **kwargs: window)
    assert native.locate_main() == 77
    with pytest.raises(SamplingError, match='visible_query_grid_not_unique'):
        native.locate()


def test_order_correlation_requires_explicit_exclusive_account_profile(tmp_path):
    path=profile_file(tmp_path)
    assert Profile.load('sim-account',path).exclusive_order_source is False
    raw=json.loads(path.read_text(encoding='utf-8'))
    raw['exclusive_order_source']=True
    path.write_text(json.dumps(raw),encoding='utf-8')
    assert Profile.load('sim-account',path).exclusive_order_source is True
    raw['exclusive_order_source']='true'
    path.write_text(json.dumps(raw),encoding='utf-8')
    with pytest.raises(DriverBlocked,match='profile_exclusive_source_unverified'):
        Profile.load('sim-account',path)
    actions=NativeActions(FakeNativeHost())
    with pytest.raises(NativeActionBlocked,match='order_result_unknown'):
        actions._correlate_confirmed_order(_request('limit_buy',
            {'security':'518880.XSHG','quantity':100,'price':'8.500'}),
            time.time(),'confirmation',time.time(),'confirmation-click-timing')


def _accepted_order(contract="ORDER-A"):
    return Request("request-" + contract, "sim-account", "2025-01-06",
                   "key-" + contract, "limit_buy",
                   {"security": "600001.XSHG", "quantity": 10, "price": "10.000"},
                   {"subaccount_key": "parent:sim-account"}, "accepted",
                   time.time() + 60, contract, "receipt-test", time.time(), time.time())


def _native_trade(contract="ORDER-A", fill="FILL-A", **overrides):
    row = {"成交时间": "09:35:02", "证券代码": "600001", "证券名称": "合成证券",
           "操作": "买入", "成交数量": "4", "成交均价": "9.990",
           "成交金额": "39.960", "合同编号": contract, "成交编号": fill,
           "委托时间": "09:35:00"}
    row.update(overrides)
    return row


def test_native_trade_normalization_prefers_explicit_day_and_market(monkeypatch):
    backend = NativeBackend.__new__(NativeBackend)
    backend.profile = SimpleNamespace(account="sim-account")
    backend.store = SimpleNamespace(get_by_contract=lambda *_: (_ for _ in ()).throw(
        AssertionError("owned lookup should not run")))
    row = _native_trade(**{"成交日期": "20250106", "交易市场": "上海"})
    result = backend._normalize_observed_rows("trades", [row])
    assert result[0]["time"] == "2025-01-06T09:35:02+08:00"
    assert "day_source" not in result[0]


def test_native_trade_normalization_requires_all_rows_owned(monkeypatch):
    import bullet_trade.integrations.ths.windows.driver as driver_module

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2025, 1, 6, 10, 0, tzinfo=tz)

    monkeypatch.setattr(driver_module, "datetime", FixedDateTime)
    backend = NativeBackend.__new__(NativeBackend)
    backend.profile = SimpleNamespace(account="sim-account")
    calls = []
    def lookup(account, day, contract):
        calls.append((account, day, contract))
        return _accepted_order() if contract == "ORDER-A" else None
    backend.store = SimpleNamespace(get_by_contract=lookup)
    result = backend._normalize_observed_rows("trades", [_native_trade()])
    assert result[0]["trade_day"] == "2025-01-06"
    assert result[0]["day_source"] == "durable_accepted_request"
    assert result[0]["request_id"] == "request-ORDER-A"
    assert calls == [("sim-account", "2025-01-06", "ORDER-A")]
    with pytest.raises(ValueError):
        backend._normalize_observed_rows("trades", [
            _native_trade(), _native_trade("UNKNOWN", "FILL-B")])
    with pytest.raises(ValueError):
        backend._normalize_observed_rows("trades", [
            _native_trade(**{"成交日期": "2025-01-07"})])


def test_identity_waits_for_stable_expected_card_and_rejects_other_card(monkeypatch):
    import bullet_trade.integrations.ths.windows.driver as driver_module

    cards = ["1234567890", None, "1234567890", "1234567890", "1234567890"]
    current = {"card": None, "page": "holdings"}
    def enumerate_children(_main, callback, _arg):
        current["card"] = cards.pop(0) if cards else "1234567890"
        callback(1, None)
        if current["card"] is not None:
            callback(2, None)
    fake_win32gui = SimpleNamespace(
        EnumChildWindows=enumerate_children,
        IsWindowVisible=lambda _hwnd: True,
        GetClassName=lambda _hwnd: "Static",
        GetDlgCtrlID=lambda hwnd: 1 if hwnd == 1 else 1383,
        GetWindowText=lambda hwnd: "资金卡号" if hwnd == 1 else current["card"],
    )
    monkeypatch.setitem(sys.modules, "win32gui", fake_win32gui)

    class Tree:
        def get_item(self, path):
            destination = "information" if path == ["修改客户信息"] else "holdings"
            return SimpleNamespace(select=lambda: current.update(page=destination))

    window = SimpleNamespace(child_window=lambda **_kwargs: SimpleNamespace(
        wrapper_object=lambda: SimpleNamespace(
            selected_text=lambda: "模拟炒股-TEST**01")))
    events = []
    adapter = SimpleNamespace(
        locate_main=lambda: 77,
        dialog_fingerprint=lambda _main: events.append("existing_guard"),
        app=SimpleNamespace(process=3, window=lambda **_kwargs: window),
        _visible_query_tree=lambda _main: Tree(),
        locate=lambda: (77, 88),
        verify_page=lambda page: page == current["page"] or (_ for _ in ()).throw(
            AssertionError("wrong page")),
    )
    backend = NativeBackend.__new__(NativeBackend)
    backend.profile = SimpleNamespace(account_mask="模拟炒股-TEST**01",
                                      expected_card="1234567890")
    backend.adapter = adapter
    backend._clear_copy_captcha = lambda: events.append("captcha_cleanup")
    backend.connection_notice = SimpleNamespace(
        close_once=lambda main, pid: events.append("notice_gate") or False)
    assert backend._identity() == "1234567890"
    assert events[:3] == ["captcha_cleanup", "notice_gate", "existing_guard"]
    assert current["page"] == "holdings"
    cards[:] = ["9999999999"]
    with pytest.raises(DriverBlocked, match="funds_card_mismatch"):
        backend._identity()
    assert current["page"] == "holdings"
