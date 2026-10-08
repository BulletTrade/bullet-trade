"""Offline regression for the bounded native buy/sell preparation path."""

from __future__ import annotations

import sys
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from bullet_trade.integrations.ths.request_store import Request
from bullet_trade.integrations.ths.windows.native.order_form_controls import (
    Control, FormObservation, FormUnverified,
)
from bullet_trade.integrations.ths.windows.native.order_focus_input import (
    OrderFocusError, write_order_edit_with_focus,
)
from bullet_trade.integrations.ths.windows.native_actions import (
    NativeActionBlocked, NativeActions,
)


MASK = "模拟炒股-TEST**01"
HANDLES = {1032: 132, 1033: 133, 1034: 134}


def _request(kind: str) -> Request:
    now = datetime.now(ZoneInfo("Asia/Shanghai")).timestamp()
    return Request(
        "request-test", "test-account", datetime.fromtimestamp(
            now, ZoneInfo("Asia/Shanghai")).date().isoformat(),
        "key-test", kind,
        {"security": "600001.XSHG", "price": "10.500", "quantity": 100},
        {"subaccount_key": "parent:test-account"}, "preparing", now + 60,
        None, None, now, now,
    )


def _form(side: str) -> FormObservation:
    controls = tuple(Control(cid, "Edit", hwnd, 120, True, True,
                             (200, 10 + index * 30, 280, 30 + index * 30))
                     for index, (cid, hwnd) in enumerate(HANDLES.items()))
    return FormObservation(
        100, "网上股票交易系统5.0", 120, 200, MASK,
        "买入[F1]" if side == "buy" else "卖出[F2]",
        True, True, True, True, controls,
    )


def _harness(monkeypatch, side: str, *, change_after_code: bool = False,
             modal_after_code: bool = False, absent_reads: int = 0,
             form_failure: str | None = None, tree_extra_class: str | None = None):
    """Replace native transports; no real window or keyboard operation occurs."""
    import bullet_trade.integrations.ths.windows.native_actions as actions_module
    import bullet_trade.integrations.ths.windows.native.native_form_observer as observer_module
    import bullet_trade.integrations.ths.windows.native.native_order_form_backend as form_module
    import bullet_trade.integrations.ths.windows.native.order_focus_input as input_module

    state = SimpleNamespace(selected=None, changed=False, modal=False,
                            values={132: "", 133: "", 134: ""}, writes=[],
                            click_count=0, hidden_seen=0, observe_calls=0,
                            absent_reads=absent_reads, main_changed=False)
    model = _form(side)
    changed_model = replace(model, form_hwnd=121)

    class Tree:
        def get_item(self, path):
            return SimpleNamespace(select=lambda: setattr(state, "selected", path[0]),
                                   is_selected=lambda: state.selected == path[0])

    class Window:
        def child_window(self, **kwargs):
            assert kwargs == {"control_id": 2322, "class_name": "ComboBox"}
            return SimpleNamespace(wrapper_object=lambda: SimpleNamespace(
                selected_text=lambda: MASK))

        def wrapper_object(self):
            return SimpleNamespace(click_input=lambda: setattr(
                state, "click_count", state.click_count + 1))

    adapter = SimpleNamespace(
        locate=lambda: (100, 150),
        locate_main=lambda: 101 if state.main_changed else 100,
        app=SimpleNamespace(
            process=200, window=lambda **_kwargs: Window()),
        _visible_query_tree=lambda _main: Tree(),
    )
    host = SimpleNamespace(adapter=adapter, profile=SimpleNamespace(account_mask=MASK))
    actions = NativeActions(host)

    class FakeObserver:
        def __init__(self, **kwargs):
            assert kwargs == {"adapter": adapter, "main_hwnd": 100,
                              "client_pid": 200, "account_mask": MASK,
                              "diagnostics": kwargs["diagnostics"]}
            self.diagnostics = kwargs["diagnostics"]

        def observe(self):
            state.observe_calls += 1
            if state.absent_reads > 0 or form_failure is not None:
                state.absent_reads -= 1
                counts = {cid: 0 for cid in (1032, 1033, 1034, 3001, 1400, 1399, 1006)}
                if form_failure == "duplicate":
                    counts[1032] = 2
                if form_failure == "mixed":
                    counts[1032] = 1
                if form_failure == "identity_after_absent":
                    state.main_changed = True
                counts.update({2322: 1, 129: 2 if tree_extra_class else 1})
                candidates = {cid: [] for cid in counts}
                candidates[2322] = [{"class": "ComboBox"}]
                candidates[129] = ([{"class": tree_extra_class}]
                                   if tree_extra_class else []) + [
                                       {"class": "SysTreeView32"}]
                if form_failure == "duplicate":
                    candidates[1032] = [{"class": "Edit"}, {"class": "Edit"}]
                if form_failure == "mixed":
                    candidates[1032] = [{"class": "Edit"}]
                self.diagnostics.update(stage="controls", reason="control_not_unique",
                                        matches={cid: {"count": count,
                                                       "candidates": candidates[cid]}
                                                 for cid, count in counts.items()})
                raise FormUnverified("control_not_unique")
            return changed_model if state.changed else model

    class FakeApi:
        def send_message_timeout(self, *args):
            raise AssertionError("reader should use the injected edit transport")

    def read_line(_send, hwnd):
        return state.values[hwnd]

    def write(hwnd, main, form, pid, value, *, is_current, diagnostic):
        assert (main, form, pid) == (100, 120, 200)
        assert diagnostic == {}
        assert is_current() is True
        state.writes.append((hwnd, value))
        state.values[hwnd] = value
        if hwnd == 132:
            state.changed = change_after_code
            state.modal = modal_after_code

    def enum_windows(callback, arg):
        # A completed hidden window remains in the process. It must never
        # become a visible modal or block an otherwise valid form.
        state.hidden_seen += 1
        callback(900, arg)
        if state.modal:
            callback(901, arg)

    fake_gui = SimpleNamespace(
        GetWindowRect=lambda _main: (0, 0, 800, 600),
        EnumWindows=enum_windows,
        IsWindowVisible=lambda hwnd: hwnd == 901,
        GetClassName=lambda hwnd: "#32770" if hwnd == 901 else "Other",
        GetWindow=lambda hwnd, _flag: 100 if hwnd == 901 else 0,
        GetAncestor=lambda hwnd, _flag: 100 if hwnd == 901 else 0,
        EnumChildWindows=lambda _hwnd, _callback, _arg: None,
    )
    fake_process = SimpleNamespace(GetWindowThreadProcessId=lambda _hwnd: (1, 200))
    monkeypatch.setitem(sys.modules, "win32api", SimpleNamespace(SetCursorPos=lambda _pos: None))
    monkeypatch.setitem(sys.modules, "win32gui", fake_gui)
    monkeypatch.setitem(sys.modules, "win32process", fake_process)
    monkeypatch.setattr(observer_module, "NativeFormObserver", FakeObserver)
    monkeypatch.setattr(form_module, "Win32FormApi", FakeApi)
    monkeypatch.setattr(form_module, "_read_single_edit_line", read_line)
    monkeypatch.setattr(input_module, "write_order_edit_with_focus", write)
    monkeypatch.setattr(actions_module.time, "sleep", lambda _seconds: None)
    return actions, state


@pytest.mark.parametrize("kind,side,tree", [
    ("limit_buy", "buy", "买入[F1]"),
    ("limit_sell", "sell", "卖出[F2]"),
])
def test_hidden_old_popup_does_not_block_complete_prepare_and_readback(
        monkeypatch, kind, side, tree):
    actions, state = _harness(monkeypatch, side)
    request = _request(kind)
    actions._prepare_order(request)
    actions._validate_order(request)
    assert state.selected == tree
    assert state.hidden_seen > 0
    assert state.writes == [(132, "600001"), (133, "10.500"), (134, "100")]
    assert tuple(state.values[HANDLES[cid]] for cid in (1032, 1033, 1034)) == (
        "600001", "10.500", "100")
    assert state.click_count == 0


@pytest.mark.parametrize("failure", ["form_changed", "owned_modal"])
def test_prepare_stops_after_code_when_form_or_modal_changes(monkeypatch, failure):
    actions, state = _harness(
        monkeypatch, "buy", change_after_code=failure == "form_changed",
        modal_after_code=failure == "owned_modal")
    with pytest.raises(NativeActionBlocked, match="order_code_popup_unverified"):
        actions._prepare_order(_request("limit_buy"))
    assert state.writes == [(132, "600001")]
    assert state.values[133] == state.values[134] == ""
    assert state.click_count == 0


def test_focus_input_sends_no_next_key_after_business_context_changes():
    class FakeInputBackend:
        def __init__(self):
            self.sent = []

        def window_info(self, hwnd):
            base = {"pid": 200, "thread_id": 10, "visible": True,
                    "enabled": True, "parent": 0, "class_name": "Window",
                    "control_id": 0}
            if hwnd == 132:
                base.update(parent=120, class_name="Edit", control_id=1032)
            return base

        def is_child(self, main, form): return (main, form) == (100, 120)
        def current_thread_id(self): return 11
        def attach(self, caller, target, enabled): return True, 0
        def foreground(self, hwnd): assert_hwnd(hwnd, 100)
        def focus(self, hwnd): assert_hwnd(hwnd, 132)
        def gui_focus(self, thread): return 132, 0
        def foreground_hwnd(self): return 100
        def keyboard_modifiers_clear(self): return True
        def select_all(self, hwnd): return True, 0
        def send_key(self, vk):
            self.sent.append(vk)
            return 2, 0
        def pause(self): pass

    def assert_hwnd(actual, expected):
        assert actual == expected

    backend = FakeInputBackend()
    with pytest.raises(OrderFocusError, match="business_identity_unverified"):
        write_order_edit_with_focus(
            132, 100, 120, 200, "600001",
            is_current=lambda: not backend.sent, diagnostic={}, backend=backend)
    assert backend.sent == [8]  # Backspace only; no first code digit.


def test_late_complete_form_waits_then_prepares_without_early_write(monkeypatch):
    actions, state = _harness(monkeypatch, "sell", absent_reads=2)
    actions._prepare_order(_request("limit_sell"))
    assert state.observe_calls >= 3
    assert state.writes == [(132, "600001"), (133, "10.500"), (134, "100")]
    assert state.click_count == 0


@pytest.mark.parametrize("failure,reason,expected_calls", [
    ("duplicate", "control_not_unique", 1),
    ("mixed", "control_not_unique", 1),
    ("identity_after_absent", "order_form_context_unverified", 1),
])
def test_ambiguous_or_wrong_identity_stops_before_field_write(
        monkeypatch, failure, reason, expected_calls):
    actions, state = _harness(monkeypatch, "sell", form_failure=failure)
    with pytest.raises((FormUnverified, NativeActionBlocked), match=reason):
        actions._prepare_order(_request("limit_sell"))
    assert state.observe_calls == expected_calls
    assert state.writes == [] and state.click_count == 0
    assert actions._form_wait_counts[1032] == (2 if failure == "duplicate"
                                               else 1 if failure == "mixed" else 0)


def test_all_absent_times_out_at_five_seconds_without_write(monkeypatch):
    import bullet_trade.integrations.ths.windows.native_actions as actions_module
    actions, state = _harness(monkeypatch, "sell", absent_reads=1000)
    clock = [0.0]
    monkeypatch.setattr(actions_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(actions_module.time, "sleep",
                        lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    with pytest.raises(FormUnverified, match="control_not_unique"):
        actions._prepare_order(_request("limit_sell"))
    assert 5.0 <= clock[0] < 7.0
    assert state.writes == [] and state.click_count == 0
    assert all(actions._form_wait_counts[cid] == 0 for cid in
               (1032, 1033, 1034, 3001, 1400, 1399, 1006))


def test_v57_raw_tree_count_two_with_one_real_tree_still_waits(monkeypatch):
    # v57 read-only evidence had ID 129: Afx:00A90000:0 + SysTreeView32.
    actions, state = _harness(monkeypatch, "sell", absent_reads=2,
                              tree_extra_class="Afx:00A90000:0")
    actions._prepare_order(_request("limit_sell"))
    assert state.observe_calls >= 3
    assert state.writes == [(132, "600001"), (133, "10.500"), (134, "100")]
    assert actions._form_wait_counts[129] == 2


def test_two_real_tree_controls_do_not_enter_wait(monkeypatch):
    actions, state = _harness(monkeypatch, "sell", form_failure="all_absent",
                              tree_extra_class="SysTreeView32")
    with pytest.raises(FormUnverified, match="control_not_unique"):
        actions._prepare_order(_request("limit_sell"))
    assert state.observe_calls == 1
    assert state.writes == []
