"""Synthetic UI shapes only; no client access or account identifiers."""

from copy import deepcopy
import ctypes

import pytest

from bullet_trade.integrations.ths.windows.native.connection_notice import (
    ConnectionNoticeError, ConnectionNoticeGate, Win32ConnectionNoticeProvider,
)


BODY = "[主站]数据发送错误 auth plugin ServiceId[0x00030000]"


def notice_shape():
    controls = (
        (11, "Static", 1004, BODY, True),
        (12, "Button", 2, "确定", True),
        (13, "Static", 2399, "报告问题", True),
        (14, "Static", 1365, None, True),
        (15, "Button", 1009, "", True),
        (16, "AfxWnd140s", 50, "", True),
        (17, "AfxWnd140s", 51, "", True),
    )
    return {"hwnd": 10, "pid": 3, "class_name": "#32770", "owner": 1,
            "root_owner": 1, "parent": 1, "visible": True, "enabled": True,
            "children": tuple({"hwnd": hwnd, "pid": 3, "parent": 10,
                               "class_name": cls, "control_id": cid, "text": value,
                               "visible": True, "enabled": enabled,
                               "style": 11 if cid == 1009 else 0}
                              for hwnd, cls, cid, value, enabled in controls)}


class FakeProvider:
    def __init__(self):
        self.shape = notice_shape()
        self.dialogs = (10,)
        self.events = []
        self.reads = 0
        self.ready = True
        self.closed_state = False
        self.close_on_click = True
        self.change_on_read = None
        self.click_error = None

    def owned_dialogs(self, main):
        self.events.append("inventory")
        return self.dialogs

    def describe(self, main, pid, dialog):
        self.events.append("describe")
        self.reads += 1
        value = deepcopy(self.shape)
        if self.change_on_read == self.reads:
            value["children"][0]["text"] = "different notification"
        return value

    def input_ready(self):
        self.events.append("input_ready")
        return self.ready

    def main_current(self, main, pid):
        self.events.append("main_current")
        return main == 1 and pid == 3

    def prepare_input(self, dialog, button):
        self.events.append("prepare_input")
        return True

    def target_current(self, dialog, button):
        self.events.append("target_current")
        return dialog == 10 and button == 12

    def click_input(self, button):
        self.events.append(("click", button))
        if self.click_error:
            raise self.click_error
        self.closed_state = self.close_on_click

    def closed(self, dialog):
        self.events.append("closed")
        return self.closed_state


def test_exact_notice_clicks_only_confirm_after_all_checks():
    provider = FakeProvider()
    gate = ConnectionNoticeGate(provider)
    assert gate.close_once(1, 3) is True
    assert provider.events.count("describe") == 3
    assert provider.events.index("target_current") < provider.events.index(("click", 12))
    assert [event for event in provider.events if isinstance(event, tuple)] == [("click", 12)]
    assert gate.last_click_phase == "dialog_closed"


@pytest.mark.parametrize("change", [
    lambda row: row["children"][0].update(text="other body"),
    lambda row: row.update(owner=2),
    lambda row: row.update(pid=4),
    lambda row: row.update(class_name="Edit"),
    lambda row: row["children"][0].update(parent=2),
    lambda row: row.update(children=row["children"] + ({
        "hwnd": 18, "pid": 3, "parent": 10, "class_name": "Button",
        "control_id": 9, "text": "", "visible": True, "enabled": True},)),
    lambda row: row["children"][4].update(text="unexpected close label"),
    lambda row: row["children"][4].update(style=0),
])
def test_other_notices_or_controls_never_click(change):
    provider = FakeProvider()
    change(provider.shape)
    with pytest.raises(ConnectionNoticeError):
        ConnectionNoticeGate(provider).close_once(1, 3)
    assert not any(isinstance(item, tuple) for item in provider.events)


def test_changed_body_and_unavailable_input_stop_before_click():
    provider = FakeProvider()
    provider.change_on_read = 2
    with pytest.raises(ConnectionNoticeError, match="connection_notice_changed"):
        ConnectionNoticeGate(provider).close_once(1, 3)
    assert not any(isinstance(item, tuple) for item in provider.events)

    provider = FakeProvider()
    provider.ready = False
    with pytest.raises(ConnectionNoticeError, match="connection_notice_input_unavailable"):
        ConnectionNoticeGate(provider).close_once(1, 3)
    assert "prepare_input" not in provider.events


def test_click_failure_or_unclosed_dialog_is_not_retried():
    now = [0.0]
    def sleep(seconds):
        now[0] += seconds

    provider = FakeProvider()
    provider.click_error = TimeoutError("synthetic click timeout")
    gate = ConnectionNoticeGate(provider, clock=lambda: now[0], sleep=sleep)
    with pytest.raises(ConnectionNoticeError, match="connection_notice_click_unverified"):
        gate.close_once(1, 3)
    assert gate.last_click_phase == "call_started"
    with pytest.raises(ConnectionNoticeError, match="connection_notice_retry_blocked"):
        gate.close_once(1, 3)
    assert provider.events.count(("click", 12)) == 1

    provider = FakeProvider()
    provider.close_on_click = False
    gate = ConnectionNoticeGate(provider, clock=lambda: now[0], sleep=sleep)
    with pytest.raises(ConnectionNoticeError, match="connection_notice_close_unverified"):
        gate.close_once(1, 3)
    assert gate.last_click_phase == "call_returned"
    assert provider.events.count(("click", 12)) == 1
    provider.shape["hwnd"] = 20
    provider.dialogs = (20,)
    provider.shape["children"] = tuple({**child, "parent": 20}
                                       for child in provider.shape["children"])
    with pytest.raises(ConnectionNoticeError, match="connection_notice_retry_blocked"):
        gate.close_once(1, 3)
    assert provider.events.count(("click", 12)) == 1


def test_native_inventory_does_not_treat_main_as_its_own_notice():
    native = Win32ConnectionNoticeProvider.__new__(Win32ConnectionNoticeProvider)
    class Gui:
        def IsWindow(self, hwnd): return True
        def IsWindowVisible(self, hwnd): return True
        def GetWindow(self, hwnd, flag): return 1 if hwnd == 10 else 0
        def GetAncestor(self, hwnd, flag): return 1
        def EnumWindows(self, callback, arg):
            for hwnd in (1, 10):
                callback(hwnd, arg)
    native.gui = Gui()
    native.con = type("Constants", (), {"GW_OWNER": 4, "GA_ROOTOWNER": 3})()
    assert native.owned_dialogs(1) == (10,)


def test_native_body_uses_one_bounded_wm_gettext_call():
    native = Win32ConnectionNoticeProvider.__new__(Win32ConnectionNoticeProvider)
    native.ctypes = ctypes
    calls = []
    class User:
        def SendMessageTimeoutW(self, hwnd, message, size, buffer, flags, timeout, copied):
            calls.append((message, timeout))
            raw = ctypes.create_unicode_buffer(BODY)
            ctypes.memmove(buffer, raw, ctypes.sizeof(raw))
            ctypes.cast(copied, ctypes.POINTER(ctypes.c_size_t)).contents.value = len(BODY)
            return 1
    native.user = User()
    assert native._text(11, timeout_ms=2000) == BODY
    assert calls == [(0x000D, 2000)]
