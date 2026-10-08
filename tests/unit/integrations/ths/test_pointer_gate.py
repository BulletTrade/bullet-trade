"""Offline pointer observations; no desktop or broker calls."""

from copy import deepcopy
from datetime import datetime
import sys
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from bullet_trade.integrations.ths.windows.native.pointer_gate import (
    PointerGate, PointerGateError,
)
from bullet_trade.integrations.ths.windows.native_actions import (
    NativeActionBlocked, NativeActions,
)


class Provider:
    def __init__(self, *, modal=False):
        self.foreground_hwnd = 80 if modal else 10
        self.hit_hwnd = 60 if modal else 42
        self.reads = 0
        self.change_on_second_read = False
        self.client_calls = 0
        self.client_shift_on_second = False
        self.windows = {
            10: {"pid": 7, "parent": 0, "owner": 0, "root_owner": 10,
                 "class_name": "Main", "control_id": 0, "text": "client",
                 "visible": True, "enabled": True, "rect": (0, 0, 500, 400)},
            12: {"pid": 7, "parent": 10, "owner": 0, "root_owner": 10,
                 "class_name": "Form", "control_id": 0, "text": "",
                 "visible": True, "enabled": True, "rect": (0, 0, 400, 300)},
            42: {"pid": 7, "parent": 12, "owner": 0, "root_owner": 10,
                 "class_name": "Button", "control_id": 1006, "text": "卖出",
                 "visible": True, "enabled": True, "rect": (100, 100, 160, 130)},
            20: {"pid": 7, "parent": 10, "owner": 0, "root_owner": 10,
                 "class_name": "CVirtualGridCtrl", "control_id": 1047, "text": "",
                 "visible": True, "enabled": True, "rect": (50, 70, 300, 250)},
            80: {"pid": 7, "parent": 0, "owner": 10, "root_owner": 10,
                 "class_name": "#32770", "control_id": 0, "text": "",
                 "visible": True, "enabled": True, "rect": (180, 100, 350, 220)},
            60: {"pid": 7, "parent": 80, "owner": 0, "root_owner": 10,
                 "class_name": "Button", "control_id": 6, "text": "是(&Y)",
                 "visible": True, "enabled": True, "rect": (220, 170, 280, 200)},
        }

    def window(self, hwnd):
        self.reads += 1
        value = deepcopy(self.windows[hwnd])
        if self.change_on_second_read and hwnd == 42 and self.reads > 2:
            value["parent"] = 99
        return value

    def is_child(self, parent, child):
        return (parent, child) in {(10, 42), (10, 20), (80, 60)}

    def foreground(self):
        return self.foreground_hwnd

    def hit(self, point):
        self.last_point = point
        return self.hit_hwnd

    def client_to_screen(self, hwnd, coords):
        self.client_calls += 1
        left, top, _, _ = self.windows[hwnd]["rect"]
        drift = 1 if self.client_shift_on_second and self.client_calls > 1 else 0
        return (left + 4 + coords[0] + drift, top + 4 + coords[1])


def test_main_button_and_modal_button_need_exact_foreground_and_hit():
    provider = Provider()
    gate = PointerGate(provider)
    gate.button(10, 7, 42, parent=12, control_id=1006, caption="卖出",
                current=lambda: True)
    assert provider.last_point == (134, 119)
    provider.foreground_hwnd = 99
    with pytest.raises(PointerGateError, match="pointer_focus_or_hit_changed"):
        gate.button(10, 7, 42, parent=12, control_id=1006, caption="卖出",
                    current=lambda: True)

    provider = Provider(modal=True)
    provider.windows[10]["enabled"] = False  # Owner is disabled by a modal dialog.
    gate = PointerGate(provider)
    gate.button(10, 7, 60, parent=80, modal=80, control_id=6,
                caption="是(&Y)", current=lambda: True)
    provider.hit_hwnd = 10
    with pytest.raises(PointerGateError, match="pointer_focus_or_hit_changed"):
        gate.button(10, 7, 60, parent=80, modal=80, control_id=6,
                    caption="是(&Y)", current=lambda: True)


def test_grid_coordinate_and_target_identity_are_checked():
    provider = Provider()
    provider.hit_hwnd = 20
    gate = PointerGate(provider)
    gate.grid(10, 7, 20, coords=(7, 45), current=lambda: True)
    assert provider.last_point == (61, 119)
    provider.windows[20]["pid"] = 8
    with pytest.raises(PointerGateError, match="pointer_target_unverified"):
        gate.grid(10, 7, 20, coords=(7, 45), current=lambda: True)
    provider.windows[20]["pid"] = 7
    provider.windows[20]["class_name"] = "Button"
    with pytest.raises(PointerGateError, match="pointer_grid_point_unverified"):
        gate.grid(10, 7, 20, coords=(7, 45), current=lambda: True)


def test_main_disabled_without_modal_cannot_click():
    provider = Provider()
    provider.windows[10]["enabled"] = False
    with pytest.raises(PointerGateError, match="pointer_target_unverified"):
        PointerGate(provider).button(10, 7, 42, parent=12, control_id=1006,
                                     caption="卖出", current=lambda: True)


def test_client_origin_shift_before_click_is_rejected():
    provider = Provider()
    provider.client_shift_on_second = True
    with pytest.raises(PointerGateError, match="pointer_changed_before_click"):
        PointerGate(provider).button(10, 7, 42, parent=12, control_id=1006,
                                     caption="卖出", current=lambda: True)


def test_submit_pointer_failure_keeps_unknown_and_sends_no_click():
    provider = Provider()
    provider.foreground_hwnd = 99
    clicks = []
    host = SimpleNamespace(adapter=SimpleNamespace(app=SimpleNamespace(
        window=lambda **kwargs: SimpleNamespace(wrapper_object=lambda:
            SimpleNamespace(click_input=lambda: clicks.append(kwargs["handle"]))))))
    native = NativeActions(host, pointer_gate=PointerGate(provider))
    native._prepared = {
        "main": 10, "pid": 7, "current": lambda: True, "side": "sell",
        "model": SimpleNamespace(controls=(SimpleNamespace(
            hwnd=42, parent_hwnd=12, control_id=1006,
            class_name="Button", caption="卖出"),))}
    native._dialogs = lambda *args: []
    request = SimpleNamespace(state="submit_unknown", expires_at=9999999999,
                              trade_day=datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat())
    with pytest.raises(NativeActionBlocked, match="pointer_focus_or_hit_changed"):
        native._submit_order(request)
    assert clicks == []
    assert request.state == "submit_unknown"


def test_business_or_control_change_stops_before_any_click():
    provider = Provider()
    gate = PointerGate(provider)
    checks = [0]
    def changing_business():
        checks[0] += 1
        return checks[0] == 1
    with pytest.raises(PointerGateError, match="pointer_changed_before_click"):
        gate.button(10, 7, 42, parent=12, control_id=1006, caption="卖出",
                    current=changing_business)
    provider.change_on_second_read = True
    provider.reads = 0
    with pytest.raises(PointerGateError, match="pointer_changed_before_click"):
        gate.button(10, 7, 42, parent=12, control_id=1006, caption="卖出",
                    current=lambda: True)


@pytest.mark.parametrize("change", ["foreground", "hit", "parent", "pid", "business"])
def test_native_action_receipt_never_clicks_when_real_gate_rejects(monkeypatch, change):
    provider = Provider(modal=True)
    provider.windows[10]["enabled"] = False
    provider.windows[60]["text"] = "确定"
    if change == "foreground":
        provider.foreground_hwnd = 10
    elif change == "hit":
        provider.hit_hwnd = 42
    elif change == "parent":
        provider.windows[60]["parent"] = 10
    elif change == "pid":
        provider.windows[60]["pid"] = 8
    clicks = []
    button = SimpleNamespace(wrapper_object=lambda: SimpleNamespace(
        click_input=lambda: clicks.append(60)))
    host = SimpleNamespace(adapter=SimpleNamespace(app=SimpleNamespace(
        window=lambda **kwargs: button)))
    native = NativeActions(host, pointer_gate=PointerGate(provider))
    dialog = (80, [{"hwnd": 60, "id": 6, "class": "Button", "text": "确定",
                   "visible": True, "enabled": True}])
    native._dialogs = lambda *args: [] if change == "business" else [dialog]
    monkeypatch.setitem(sys.modules, "win32gui", SimpleNamespace(
        GetParent=lambda hwnd: 80))
    with pytest.raises(NativeActionBlocked, match="pointer_"):
        native._close_known_receipt(10, 7, dialog)
    assert clicks == []
