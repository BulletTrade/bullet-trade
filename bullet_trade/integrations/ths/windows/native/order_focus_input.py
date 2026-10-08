"""Explicit bounded keyboard input for one verified THS order-form Edit.

No Enter, Tab, button action, account selection or implicit WM_SETTEXT fallback.
The caller separately performs full three-field readback and owns GUI locks.
"""
from __future__ import annotations

import re
import time
from typing import Callable, MutableMapping

from .captcha_focus_input import WindowsInputBackend


_EDIT_IDS = frozenset((1032, 1033, 1034))
_EM_SETSEL = 0x00B1
_VK_BACK = 0x08
_VK_OEM_PERIOD = 0xBE


class OrderFocusError(RuntimeError):
    """Fixed reason code; never includes the intended order value."""


class WindowsOrderInputBackend(WindowsInputBackend):
    """Lazy Windows transport. Tests inject a backend and never invoke Win32."""

    def __init__(self) -> None:
        super().__init__()
        self.user.GetForegroundWindow.restype = self.wintypes.HWND
        self.user.GetAsyncKeyState.argtypes = (self.ctypes.c_int,)
        self.user.GetAsyncKeyState.restype = self.ctypes.c_short

    def is_child(self, main_hwnd: int, form_hwnd: int) -> bool:
        return bool(self.gui.IsChild(main_hwnd, form_hwnd))

    def foreground_hwnd(self) -> int:
        return int(self.user.GetForegroundWindow() or 0)

    def keyboard_modifiers_clear(self) -> bool:
        return all(not (int(self.user.GetAsyncKeyState(vk)) & 0x8000)
                   for vk in (16, 17, 18, 91, 92))  # Shift/Ctrl/Alt/Win keys

    def select_all(self, edit_hwnd: int) -> tuple[bool, int]:
        result = self.ctypes.c_size_t()
        self.ctypes.set_last_error(0)
        # EM_SETSEL's result can be zero on success. Check transport status.
        sent = self.user.SendMessageTimeoutW(
            edit_hwnd, _EM_SETSEL, 0, -1, 0x0001 | 0x0002, 250,
            self.ctypes.byref(result))
        return bool(sent), self.ctypes.get_last_error()

    def send_key(self, vk: int) -> tuple[int, int]:
        inputs = (self.Input * 2)()
        inputs[0].kind = inputs[1].kind = 1  # INPUT_KEYBOARD
        inputs[0].value.key.vk = inputs[1].value.key.vk = vk
        inputs[1].value.key.flags = 2  # KEYEVENTF_KEYUP
        self.ctypes.set_last_error(0)
        sent = int(self.user.SendInput(2, inputs, self.ctypes.sizeof(self.Input)))
        return sent, self.ctypes.get_last_error()

    def release_key(self, vk: int) -> tuple[int, int]:
        keyup = (self.Input * 1)()
        keyup[0].kind = 1
        keyup[0].value.key.vk = vk
        keyup[0].value.key.flags = 2  # KEYEVENTF_KEYUP
        self.ctypes.set_last_error(0)
        sent = int(self.user.SendInput(1, keyup, self.ctypes.sizeof(self.Input)))
        return sent, self.ctypes.get_last_error()

    def pause(self) -> None:
        time.sleep(0.03)


def write_order_edit_with_focus(
    edit_hwnd: int, main_hwnd: int, form_hwnd: int, pid: int, value: str,
    *, is_current: Callable[[], bool], diagnostic: MutableMapping[str, object],
    backend=None, trace: Callable[[str, int | None], None] | None = None,
) -> None:
    """Clear and type one Edit; delivery alone never proves three-field readback."""
    if (not isinstance(value, str) or len(value) > 64
            or (value and re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value) is None)):
        raise ValueError("order_value_invalid")
    if (any(type(item) is not int or item <= 0
            for item in (edit_hwnd, main_hwnd, form_hwnd, pid))
            or len({edit_hwnd, main_hwnd, form_hwnd}) != 3):
        raise ValueError("window_identity_invalid")
    if not callable(is_current) or not isinstance(diagnostic, MutableMapping):
        raise ValueError("input_contract_invalid")
    if trace is not None and not callable(trace):
        raise ValueError("trace_contract_invalid")
    if backend is None:
        backend = WindowsOrderInputBackend()
    expected_id: int | None = None

    def emit(stage: str, key_index: int | None = None) -> None:
        if trace is None:
            return
        try:
            trace(stage, key_index)
        except Exception:
            raise OrderFocusError("trace_unavailable") from None

    def check_context() -> int:
        nonlocal expected_id
        emit("context.before")
        try:
            current = is_current()
        except Exception:
            raise OrderFocusError("business_identity_unverified") from None
        if current is not True:
            raise OrderFocusError("business_identity_unverified")
        try:
            main = backend.window_info(main_hwnd)
            form = backend.window_info(form_hwnd)
            edit = backend.window_info(edit_hwnd)
            valid = (type(edit["control_id"]) is int and edit["control_id"] in _EDIT_IDS
                     and (expected_id is None or edit["control_id"] == expected_id)
                     and edit["class_name"] == "Edit" and edit["parent"] == form_hwnd
                     and main["pid"] == form["pid"] == edit["pid"] == pid
                     and main["visible"] is True and main["enabled"] is True
                     and form["visible"] is True and form["enabled"] is True
                     and edit["visible"] is True and edit["enabled"] is True
                     and backend.is_child(main_hwnd, form_hwnd) is True
                     and type(edit["thread_id"]) is int and edit["thread_id"] > 0
                     and edit["thread_id"] == form["thread_id"] == main["thread_id"])
        except Exception:
            raise OrderFocusError("order_edit_context_unavailable") from None
        diagnostic["identity_ok"] = bool(valid)
        if not valid:
            raise OrderFocusError("order_edit_context_unverified")
        if expected_id is None:
            expected_id = edit["control_id"]
        emit("context.after")
        return edit["thread_id"]

    target = check_context()
    caller = backend.current_thread_id()
    emit("attach.before")
    attached, attach_error = backend.attach(caller, target, True)
    diagnostic.update(attached=attached, attach_error=attach_error, sent_events=0)
    if attached is not True:
        raise OrderFocusError("input_attach_failed")
    try:
        emit("attach.after")
        emit("focus.before")
        backend.foreground(main_hwnd)
        backend.focus(edit_hwnd)
        emit("focus.after")
    finally:
        # No observation, message or keyboard event is sent while queues are attached.
        detach_trace_failed = False
        try:
            emit("detach.before")
        except OrderFocusError:
            detach_trace_failed = True
        detached, detach_error = backend.attach(caller, target, False)
        diagnostic.update(detached=detached, detach_error=detach_error)
        if detached is not True:
            raise OrderFocusError("input_detach_failed")
        if detach_trace_failed:
            raise OrderFocusError("trace_unavailable")
    emit("detach.after")

    def check_focus() -> None:
        emit("focus_check.before")
        if check_context() != target:
            raise OrderFocusError("order_edit_thread_changed")
        focused, error = backend.gui_focus(target)
        diagnostic["focus_query_error"] = error
        if focused != edit_hwnd or error != 0:
            raise OrderFocusError("focus_not_order_edit")
        if backend.foreground_hwnd() != main_hwnd:
            raise OrderFocusError("main_not_foreground")
        if backend.keyboard_modifiers_clear() is not True:
            raise OrderFocusError("keyboard_modifiers_active")
        emit("focus_check.after")

    def send(vk: int, index: int) -> None:
        check_focus()
        emit("send.before", index)
        sent, error = backend.send_key(vk)
        diagnostic["sent_events"] += sent
        diagnostic["sendinput_error"] = error
        if sent != 2:
            if sent == 1:
                released, release_error = backend.release_key(vk)
                diagnostic.update(key_released=released == 1,
                                  release_error=release_error)
                if released != 1:
                    raise OrderFocusError("key_release_unverified")
            raise OrderFocusError("sendinput_incomplete")
        emit("send.after", index)
        check_focus()
        backend.pause()

    check_focus()
    emit("selection.before")
    selected, select_error = backend.select_all(edit_hwnd)
    diagnostic["select_error"] = select_error
    if selected is not True:
        raise OrderFocusError("edit_selection_unverified")
    emit("selection.after")
    check_focus()
    send(_VK_BACK, 0)
    for index, char in enumerate(value, 1):
        send(_VK_OEM_PERIOD if char == "." else ord(char), index)
    check_focus()
    diagnostic["focus_matches_after"] = True
