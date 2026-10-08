"""Synthetic, offline checks of the single-row native cancellation boundary."""

from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import sys

import pytest

from bullet_trade.integrations.ths.request_store import Request
from bullet_trade.integrations.ths.windows.native.grid_scroll_evidence import GridScroll
from bullet_trade.integrations.ths.windows.native.query_context import QueryContext
from bullet_trade.integrations.ths.windows.native_actions import NativeActions, NativeActionBlocked


DAY = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
TERMS = {"security": "518880.XSHG", "quantity": 100, "price": "8.500"}
ROW = {"合同编号": "000123", "证券代码": "518880", "交易市场": "上海Ａ股",
       "操作": "买入", "备注": "未成交", "委托数量": "100", "成交数量": "0",
       "委托价格": "8.500", "委托时间": "09:35:00"}


def request(kind="cancel", params=None):
    return Request("request-1", "sim-account", DAY, "key", kind,
                   params or {"broker_contract_no": "000123"},
                   {"virtual_account_id": "v1"}, "preparing", 9999999999.0,
                   None, None, 0.0, 0.0)


class Window:
    def __init__(self, host, role):
        self.host, self.role = host, role
        self.handle = 1099 if role == "cancel_button" else 20 if role == "grid" else 60

    def wrapper_object(self): return self
    def child_window(self, **kwargs): return Window(self.host, "cancel_button")
    def is_visible(self): return True
    def is_enabled(self):
        if self.role != "cancel_button":
            return True
        return (self.host.cancel_button_enabled_after_selection
                and any(role == "grid" for role, _ in self.host.clicks))
    def window_text(self): return "撤单(Del)"
    def click_input(self, **kwargs):
        self.host.clicks.append((self.role, kwargs))
        if self.role == "cancel_button":
            self.host.dialog_open = True
        elif self.role == "yes":
            self.host.dialog_open = False


class Host:
    def __init__(self, rows=(ROW,), *, complete=True, metadata=None):
        self.profile = SimpleNamespace(account="sim-account",
                                       account_mask="模拟炒股-TEST**01")
        self.clicks = []
        self.dialog_open = False
        self.cancel_button_enabled_after_selection = True
        self.location = (10, 20)
        self.adapter = SimpleNamespace(
            locate=lambda: self.location,
            app=SimpleNamespace(process=7, window=lambda handle: Window(
                self, "grid" if handle == 20 else "yes" if handle == 60 else "main")))
        self.original = SimpleNamespace(
            account="sim-account", trade_day=DAY, state="accepted",
            broker_contract_no="000123", kind="limit_buy", params=dict(TERMS),
            origin={"virtual_account_id": "v1"})
        self.store = SimpleNamespace(get_by_contract=lambda *args: self.original)
        self.rows = tuple(rows)
        self.complete = complete
        self.metadata = {} if metadata is None else metadata
        self.terminal = {**ROW, "备注": "全部撤单", "撤消数量": "100"}
        self.orders_complete = True

    def _identity(self): return "synthetic-card"

    def query_observed(self, kind, should_yield):
        if kind == "cancelable":
            return SimpleNamespace(complete=self.complete, raw_rows=self.rows,
                                   metadata=self.metadata)
        if kind == "orders":
            return SimpleNamespace(complete=self.orders_complete,
                                   raw_rows=(self.terminal,), metadata={})
        if kind == "positions":
            return SimpleNamespace(complete=self.complete, raw_rows=self.rows,
                                   metadata={})
        raise AssertionError(kind)


@pytest.fixture
def context(monkeypatch):
    from bullet_trade.integrations.ths.windows.native import query_context
    value = QueryContext(True, True, True, GridScroll(0, 1, 2, 0))
    monkeypatch.setattr(query_context, "inspect_query_context",
                        lambda *args: value)
    return value


def actions(host, monkeypatch):
    class CheckedPointer:
        def button(self, *args, **kwargs):
            assert kwargs["current"]() is True

        def grid(self, *args, **kwargs):
            assert kwargs["coords"] == (7, 45)
            assert kwargs["current"]() is True

    result = NativeActions(host, pointer_gate=CheckedPointer())
    monkeypatch.setattr(result, "_dialogs", lambda main, pid: [])
    return result


def test_single_complete_original_prepares_without_click(context, monkeypatch):
    host = Host()
    native = actions(host, monkeypatch)
    req = request()
    assert native.preflight(req).allowed is True
    native.prepare(req)
    assert host.clicks == []
    assert native.validate_readback(req).allowed is True
    assert host.clicks == []


def test_incomplete_or_multirow_is_local_denial(context, monkeypatch):
    for host in (Host(complete=False), Host(rows=(ROW, {**ROW, "合同编号": "000124"}))):
        native = actions(host, monkeypatch)
        assert native.preflight(request()).allowed is False
        assert host.clicks == []


def test_sell_requires_complete_positions_observation(monkeypatch):
    row = {"证券代码": "518880", "可用余额": "100"}
    req = request("limit_sell", dict(TERMS))
    assert actions(Host(rows=(row,), complete=False), monkeypatch).preflight(req).allowed is False
    assert actions(Host(rows=(row,), complete=True), monkeypatch).preflight(req).allowed is True


@pytest.mark.parametrize("changed", [
    {"交易市场": "深圳Ａ股"}, {"证券代码": "000001"}, {"操作": "卖出"},
    {"委托数量": "101"}, {"委托价格": "8.501"},
    {"委托日期": "2025-01-03"},
])
def test_original_terms_or_date_mismatch_is_denied(context, monkeypatch, changed):
    host = Host(rows=({**ROW, **changed},))
    native = actions(host, monkeypatch)
    assert native.preflight(request()).allowed is False
    assert host.clicks == []


def test_context_must_prove_single_unfiltered_ready_row(monkeypatch, context):
    from bullet_trade.integrations.ths.windows.native import query_context
    host = Host()
    native = actions(host, monkeypatch)
    req = request()
    assert native.preflight(req).allowed
    monkeypatch.setattr(query_context, "inspect_query_context",
                        lambda *args: QueryContext(True, False, True,
                                                   GridScroll(0, 1, 2, 0)))
    native.prepare(req)
    assert native.validate_readback(req).allowed is False
    assert host.clicks == []


def test_only_submit_selects_row_and_requires_complete_terminal_order(context, monkeypatch):
    host = Host()
    native = actions(host, monkeypatch)
    req = request()
    assert native.preflight(req).allowed
    native.prepare(req)
    assert native.validate_readback(req).allowed
    assert host.clicks == []
    dialog = (80, [{"class": "Static", "text": "撤单确认"},
                   {"class": "Button", "id": 6, "text": "是(&Y)",
                    "enabled": True, "visible": True, "hwnd": 60}])
    monkeypatch.setattr(native, "_dialogs", lambda main, pid: [dialog]
                        if host.dialog_open else [])
    monkeypatch.setattr(native, "_wait_one_dialog", lambda *args: dialog)
    monkeypatch.setitem(sys.modules, "win32gui",
                        SimpleNamespace(GetParent=lambda hwnd: 80))
    result = native.submit(req)
    assert result.broker_contract_no == "000123"
    assert [role for role, _ in host.clicks] == ["grid", "cancel_button", "yes"]
    assert host.clicks[0][1] == {"coords": (7, 45)}


def test_terminal_query_must_be_complete_even_after_confirmation(context, monkeypatch):
    host = Host()
    host.orders_complete = False
    native = actions(host, monkeypatch)
    req = request()
    assert native.preflight(req).allowed
    native.prepare(req)
    dialog = (80, [{"class": "Static", "text": "撤单确认"},
                   {"class": "Button", "id": 6, "text": "是(&Y)",
                    "enabled": True, "visible": True, "hwnd": 60}])
    monkeypatch.setattr(native, "_dialogs", lambda main, pid: [dialog]
                        if host.dialog_open else [])
    monkeypatch.setattr(native, "_wait_one_dialog", lambda *args: dialog)
    monkeypatch.setitem(sys.modules, "win32gui",
                        SimpleNamespace(GetParent=lambda hwnd: 80))
    with pytest.raises(NativeActionBlocked, match="cancel_result_unknown"):
        native.submit(req)


def test_disabled_after_selection_has_distinct_code_and_no_cancel_click(context, monkeypatch):
    host = Host()
    host.cancel_button_enabled_after_selection = False
    native = actions(host, monkeypatch)
    req = request()
    assert native.preflight(req).allowed
    native.prepare(req)
    with pytest.raises(NativeActionBlocked, match="cancel_button_after_selection_unverified"):
        native.submit(req)
    assert [role for role, _ in host.clicks] == ["grid"]


def test_submit_refuses_changed_single_row_before_any_cancel_click(context, monkeypatch):
    host = Host()
    native = actions(host, monkeypatch)
    req = request()
    assert native.preflight(req).allowed
    native.prepare(req)
    host.rows = ({**ROW, "成交数量": "10", "备注": "部成"},)
    with pytest.raises(NativeActionBlocked, match="cancel_row_changed"):
        native.submit(req)
    assert host.clicks == []


def test_terminal_terms_mismatch_cannot_be_accepted(context, monkeypatch):
    host = Host()
    host.terminal = {**host.terminal, "委托价格": "8.501"}
    native = actions(host, monkeypatch)
    req = request()
    assert native.preflight(req).allowed
    native.prepare(req)
    dialog = (80, [{"class": "Static", "text": "撤单确认"},
                   {"class": "Button", "id": 6, "text": "是(&Y)",
                    "enabled": True, "visible": True, "hwnd": 60}])
    monkeypatch.setattr(native, "_dialogs", lambda main, pid: [dialog]
                        if host.dialog_open else [])
    monkeypatch.setattr(native, "_wait_one_dialog", lambda *args: dialog)
    monkeypatch.setitem(sys.modules, "win32gui",
                        SimpleNamespace(GetParent=lambda hwnd: 80))
    with pytest.raises(NativeActionBlocked, match="cancel_row_terms_mismatch"):
        native.submit(req)


def test_cancel_expiring_during_final_query_has_no_click(context, monkeypatch):
    from bullet_trade.integrations.ths.windows import native_actions
    host = Host()
    native = actions(host, monkeypatch)
    req = request()
    assert native.preflight(req).allowed
    native.prepare(req)
    assert native.validate_readback(req).allowed
    original_query = host.query_observed
    def expiring_query(*args):
        value = original_query(*args)
        monkeypatch.setattr(native_actions.time, 'time', lambda: req.expires_at)
        return value
    host.query_observed = expiring_query
    with pytest.raises(NativeActionBlocked, match='request_expired_before_click'):
        native.submit(req)
    assert host.clicks == []


def test_cancel_crossing_day_before_confirmation_does_not_click_yes(context, monkeypatch):
    from datetime import timedelta
    from bullet_trade.integrations.ths.windows import native_actions
    host = Host()
    native = actions(host, monkeypatch)
    req = request()
    assert native.preflight(req).allowed
    native.prepare(req)
    dialog = (80, [{'class': 'Static', 'text': '撤单确认'},
                   {'class': 'Button', 'id': 6, 'text': '是(&Y)',
                    'enabled': True, 'visible': True, 'hwnd': 60}])
    monkeypatch.setattr(native, '_dialogs', lambda main, pid: [dialog]
                        if host.dialog_open else [])
    class NextDay(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(tz) + timedelta(days=1)
    def waiting(*args):
        monkeypatch.setattr(native_actions, 'datetime', NextDay)
        return dialog
    monkeypatch.setattr(native, '_wait_one_dialog', waiting)
    monkeypatch.setitem(sys.modules, 'win32gui', SimpleNamespace(GetParent=lambda hwnd: 80))
    with pytest.raises(NativeActionBlocked, match='trade_day_changed_before_click'):
        native.submit(req)
    assert [role for role, _ in host.clicks] == ['grid', 'cancel_button']
