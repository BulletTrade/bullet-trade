"""Bounded keyboard input for a verified four-digit THS captcha Edit control.

The caller owns challenge identification and readback. This module never logs
the candidate or treats keyboard delivery as challenge acceptance.
"""

from __future__ import annotations

import re
import time
from typing import Callable, MutableMapping, Optional


class EditLengthQueryError(RuntimeError):
    """A bounded WM_GETTEXTLENGTH request did not return a trustworthy length."""

    def __init__(self, win_error: int) -> None:
        super().__init__("edit_length_query_timeout" if win_error in (0, 1460)
                         else "edit_length_query_failed")
        self.win_error = win_error


class WindowsInputBackend:
    """Lazy native implementation; tests inject a backend without Windows APIs."""

    def __init__(self) -> None:
        import ctypes
        from ctypes import wintypes
        import win32gui
        import win32process

        self.ctypes = ctypes
        self.wintypes = wintypes
        self.gui = win32gui
        self.process = win32process
        self.user = ctypes.WinDLL("user32", use_last_error=True)
        self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        self.kernel.GetCurrentThreadId.restype = wintypes.DWORD
        self.user.AttachThreadInput.argtypes = (wintypes.DWORD, wintypes.DWORD, wintypes.BOOL)
        self.user.AttachThreadInput.restype = wintypes.BOOL
        self.user.SetForegroundWindow.argtypes = (wintypes.HWND,)
        self.user.SetForegroundWindow.restype = wintypes.BOOL
        self.user.SetFocus.argtypes = (wintypes.HWND,)
        self.user.SetFocus.restype = wintypes.HWND
        self.user.SendMessageTimeoutW.argtypes = (
            wintypes.HWND, wintypes.UINT, ctypes.c_size_t, ctypes.c_ssize_t,
            wintypes.UINT, wintypes.UINT, ctypes.POINTER(ctypes.c_size_t))
        self.user.SendMessageTimeoutW.restype = ctypes.c_ssize_t
        self.user.SendMessageTimeoutA.argtypes = self.user.SendMessageTimeoutW.argtypes
        self.user.SendMessageTimeoutA.restype = ctypes.c_ssize_t
        self.user.IsWindowUnicode.argtypes = (wintypes.HWND,)
        self.user.IsWindowUnicode.restype = wintypes.BOOL

        class GuiInfo(ctypes.Structure):
            _fields_ = [("size", wintypes.DWORD), ("flags", wintypes.DWORD),
                        ("active", wintypes.HWND), ("focus", wintypes.HWND),
                        ("capture", wintypes.HWND), ("menu", wintypes.HWND),
                        ("move", wintypes.HWND), ("caret", wintypes.HWND),
                        ("rect", wintypes.RECT)]

        class Mouse(ctypes.Structure):
            _fields_ = [("dx", wintypes.LONG), ("dy", wintypes.LONG),
                        ("data", wintypes.DWORD), ("flags", wintypes.DWORD),
                        ("time", wintypes.DWORD), ("extra", ctypes.c_size_t)]

        class Key(ctypes.Structure):
            _fields_ = [("vk", wintypes.WORD), ("scan", wintypes.WORD),
                        ("flags", wintypes.DWORD), ("time", wintypes.DWORD),
                        ("extra", ctypes.c_size_t)]

        class Value(ctypes.Union):
            _fields_ = [("mouse", Mouse), ("key", Key)]

        class Input(ctypes.Structure):
            _fields_ = [("kind", wintypes.DWORD), ("value", Value)]

        self.GuiInfo = GuiInfo
        self.Input = Input
        self.user.GetGUIThreadInfo.argtypes = (wintypes.DWORD, ctypes.POINTER(GuiInfo))
        self.user.GetGUIThreadInfo.restype = wintypes.BOOL
        self.user.SendInput.argtypes = (wintypes.UINT, ctypes.POINTER(Input), ctypes.c_int)
        self.user.SendInput.restype = wintypes.UINT
        self.user.GetForegroundWindow.restype = wintypes.HWND
        self.user.GetAsyncKeyState.argtypes = (ctypes.c_int,)
        self.user.GetAsyncKeyState.restype = ctypes.c_short

    def window_info(self, hwnd: int) -> dict:
        thread_id, pid = self.process.GetWindowThreadProcessId(hwnd)
        return {"class_name": self.gui.GetClassName(hwnd),
                "control_id": self.gui.GetDlgCtrlID(hwnd),
                "parent": self.gui.GetParent(hwnd), "pid": pid,
                "thread_id": thread_id,
                "visible": bool(self.gui.IsWindowVisible(hwnd)),
                "enabled": bool(self.gui.IsWindowEnabled(hwnd))}

    def text_length(self, hwnd: int) -> int:
        result = self.ctypes.c_size_t()
        self.ctypes.set_last_error(0)
        sent = self.user.SendMessageTimeoutW(
            hwnd, 0x000E, 0, 0, 0x0002, 500, self.ctypes.byref(result))
        if not sent:
            raise EditLengthQueryError(self.ctypes.get_last_error())
        return int(result.value)

    def read_prefix(self, hwnd: int, timeout_ms: int) -> str:
        """Bounded EM_GETLINE row 0; only its reported 0..4 chars are decoded."""
        ctypes = self.ctypes
        wide = bool(self.user.IsWindowUnicode(hwnd))
        unit = 2 if wide else 1
        buffer = ctypes.create_string_buffer(10 * unit)
        ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ushort))[0] = 4
        result = ctypes.c_size_t()
        sender = (self.user.SendMessageTimeoutW if wide
                  else self.user.SendMessageTimeoutA)
        ctypes.set_last_error(0)
        sent = bool(sender(hwnd, 0x00C4, 0, ctypes.addressof(buffer),
                           0x0002, timeout_ms, ctypes.byref(result)))
        if not sent:
            raise RuntimeError("captcha_prefix_transport_failed")
        count = int(result.value)
        if not 0 <= count <= 4:
            raise RuntimeError("captcha_prefix_count_invalid")
        try:
            return ctypes.string_at(ctypes.addressof(buffer), count * unit).decode(
                "utf-16-le" if wide else "ascii")
        except UnicodeError as exc:
            raise RuntimeError("captcha_prefix_decode_failed") from exc

    def current_thread_id(self) -> int:
        return int(self.kernel.GetCurrentThreadId())

    def attach(self, caller: int, target: int, enabled: bool) -> tuple[bool, int]:
        self.ctypes.set_last_error(0)
        ok = bool(self.user.AttachThreadInput(caller, target, enabled))
        return ok, self.ctypes.get_last_error()

    def foreground(self, hwnd: int) -> bool:
        return bool(self.user.SetForegroundWindow(hwnd))

    def focus(self, hwnd: int) -> None:
        self.user.SetFocus(hwnd)

    def gui_focus(self, thread_id: int) -> tuple[Optional[int], int]:
        info = self.GuiInfo()
        info.size = self.ctypes.sizeof(info)
        self.ctypes.set_last_error(0)
        if not self.user.GetGUIThreadInfo(thread_id, self.ctypes.byref(info)):
            return None, self.ctypes.get_last_error()
        return int(info.focus or 0), 0

    def foreground_hwnd(self) -> int:
        return int(self.user.GetForegroundWindow() or 0)

    def keyboard_modifiers_clear(self) -> bool:
        return all(not (int(self.user.GetAsyncKeyState(vk)) & 0x8000)
                   for vk in (16, 17, 18, 91, 92))

    def send_digit(self, digit: str) -> tuple[int, int]:
        keydown = (self.Input * 1)()
        keyup = (self.Input * 1)()
        keydown[0].kind = keyup[0].kind = 1  # INPUT_KEYBOARD
        keydown[0].value.key.vk = keyup[0].value.key.vk = ord(digit)
        keyup[0].value.key.flags = 2  # KEYEVENTF_KEYUP
        down_sent = up_sent = 0
        down_error = up_error = 0
        hold_failed = False
        try:
            try:
                self.ctypes.set_last_error(0)
                down_sent = int(self.user.SendInput(
                    1, keydown, self.ctypes.sizeof(self.Input)))
                down_error = self.ctypes.get_last_error()
                if down_sent == 1:
                    time.sleep(0.05)
            except Exception:
                hold_failed = True
        finally:
            # Always release the exact digit, including a partial SendInput or
            # an interrupted hold. Never retry the keydown or clear modifiers.
            try:
                self.ctypes.set_last_error(0)
                up_sent = int(self.user.SendInput(
                    1, keyup, self.ctypes.sizeof(self.Input)))
                up_error = self.ctypes.get_last_error()
            except Exception:
                up_sent = 0
        if up_sent != 1 or up_error:
            raise RuntimeError("captcha_key_release_failed")
        if down_sent != 1 or down_error or hold_failed:
            raise RuntimeError("captcha_key_down_or_hold_failed")
        return 2, 0

    def release_digit(self, digit: str) -> tuple[int, int]:
        keyup = (self.Input * 1)()
        keyup[0].kind = 1
        keyup[0].value.key.vk = ord(digit)
        keyup[0].value.key.flags = 2
        self.ctypes.set_last_error(0)
        sent = int(self.user.SendInput(1, keyup, self.ctypes.sizeof(self.Input)))
        return sent, self.ctypes.get_last_error()

    def pause(self) -> None:
        time.sleep(0.15)


def write_captcha_with_focus(
    edit_hwnd: int,
    dialog_hwnd: int,
    client_pid: int,
    value: str,
    *,
    is_current: Callable[[int], bool],
    diagnostic: MutableMapping[str, object],
    backend=None,
) -> None:
    """Send four digits only while control identity and GUI focus stay stable."""
    if not isinstance(value, str) or re.fullmatch(r"[0-9]{4}", value) is None:
        raise ValueError("invalid_candidate")
    if any(not isinstance(handle, int) or isinstance(handle, bool) or handle <= 0
           for handle in (edit_hwnd, dialog_hwnd, client_pid)):
        raise ValueError("invalid_window_identity")
    if backend is None:
        backend = WindowsInputBackend()

    def check_identity() -> int:
        if not is_current(dialog_hwnd):
            raise RuntimeError("dialog_changed")
        dialog = backend.window_info(dialog_hwnd)
        edit = backend.window_info(edit_hwnd)
        valid = (edit["class_name"].lower() == "edit"
                 and edit["control_id"] == 2404
                 and edit["parent"] == dialog_hwnd
                 and edit["pid"] == dialog["pid"] == client_pid
                 and edit["visible"] and edit["enabled"]
                 and dialog["visible"] and dialog["enabled"]
                 and edit["thread_id"] == dialog["thread_id"]
                 and isinstance(edit["thread_id"], int)
                 and edit["thread_id"] > 0)
        diagnostic["identity_ok"] = bool(valid)
        if not valid:
            raise RuntimeError("edit_identity_invalid")
        return edit["thread_id"]

    target = check_identity()
    try:
        initial_length = backend.text_length(edit_hwnd)
    except EditLengthQueryError as exc:
        diagnostic["text_length_error"] = exc.win_error
        raise
    diagnostic["initial_length"] = initial_length
    if initial_length != 0:
        raise RuntimeError("edit_not_empty")

    caller = backend.current_thread_id()
    attached, attach_error = backend.attach(caller, target, True)
    diagnostic.update(attached=attached, attach_error=attach_error, sent_events=0)
    if not attached:
        raise RuntimeError("input_attach_failed")
    try:
        diagnostic["foreground_requested"] = backend.foreground(dialog_hwnd)
        backend.focus(edit_hwnd)
    finally:
        # Attached queues are used only to request foreground and focus.
        # No identity observation, window message or key is sent until detach.
        detached, detach_error = backend.attach(caller, target, False)
        diagnostic.update(detached=detached, detach_error=detach_error)
        if not detached:
            raise RuntimeError("input_detach_failed")

    def check_focus() -> None:
        if check_identity() != target:
            raise RuntimeError("edit_thread_changed")
        focused_hwnd, error = backend.gui_focus(target)
        diagnostic["focus_query_error"] = error
        diagnostic["focus_matches_edit"] = focused_hwnd == edit_hwnd and error == 0
        if focused_hwnd != edit_hwnd or error != 0:
            raise RuntimeError("focus_not_edit")
        foreground = backend.foreground_hwnd() == dialog_hwnd
        diagnostic["foreground_matches_dialog"] = foreground
        if not foreground:
            raise RuntimeError("dialog_not_foreground")
        modifiers_clear = backend.keyboard_modifiers_clear() is True
        diagnostic["modifiers_clear"] = modifiers_clear
        if not modifiers_clear:
            raise RuntimeError("keyboard_modifiers_active")

    check_focus()
    for index, digit in enumerate(value, 1):
        check_focus()
        sent, error = backend.send_digit(digit)
        diagnostic["sent_events"] += sent
        diagnostic["sendinput_error"] = error
        if sent != 2:
            if sent == 1:
                released, release_error = backend.release_digit(digit)
                diagnostic.update(key_released=released == 1,
                                  release_error=release_error)
                if released != 1:
                    raise RuntimeError("key_release_unverified")
            raise RuntimeError("sendinput_incomplete")
        check_focus()
        # The client may process SendInput after it returns. Give each digit
        # one bounded chance to appear; never send the same digit again.
        deadline = time.monotonic() + 0.5
        time.sleep(0.2)
        expected = value[:index]
        while True:
            check_focus()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("captcha_prefix_missing")
            observed = backend.read_prefix(edit_hwnd, max(1, int(remaining * 1000)))
            if observed == expected:
                break
            if not expected.startswith(observed):
                raise RuntimeError("captcha_prefix_mismatch")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("captcha_prefix_missing")
            time.sleep(min(0.05, remaining))
        backend.pause()
    check_focus()
    diagnostic["focus_matches_after"] = True
