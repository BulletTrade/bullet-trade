"""Close one exactly identified THS connection notice before normal GUI checks.

This is a UI cleanup, never evidence of a healthy connection or broker write.
All errors are fixed codes; no window text is included in diagnostics.
"""

from __future__ import annotations

import time


_BODY = "[主站]数据发送错误 auth plugin ServiceId[0x00030000]"
_COOLDOWN_SECONDS = 10.0
_CLOSE_TIMEOUT_SECONDS = 2.0


class ConnectionNoticeError(RuntimeError):
    """Fixed diagnostic code without notice text or account information."""


def _verified_shape(observation: dict, main: int, pid: int, dialog: int) -> int:
    if (observation.get("hwnd") != dialog or observation.get("pid") != pid
            or observation.get("class_name") != "#32770"
            or observation.get("owner") != main
            or observation.get("root_owner") != main
            or observation.get("parent") not in (0, main)
            or observation.get("visible") is not True
            or observation.get("enabled") is not True):
        raise ConnectionNoticeError("connection_notice_dialog_unverified")
    children = observation.get("children")
    if not isinstance(children, tuple) or len(children) != 7:
        raise ConnectionNoticeError("connection_notice_controls_unverified")
    seen = set()
    controls = {}
    afx = []
    for child in children:
        if (not isinstance(child, dict) or type(child.get("hwnd")) is not int
                or child["hwnd"] <= 0 or child["hwnd"] in seen
                or child.get("pid") != pid or child.get("parent") != dialog
                or child.get("visible") is not True):
            raise ConnectionNoticeError("connection_notice_controls_unverified")
        seen.add(child["hwnd"])
        key = (child.get("class_name"), child.get("control_id"))
        if child.get("class_name") == "AfxWnd140s":
            if child.get("text") != "":
                raise ConnectionNoticeError("connection_notice_controls_unverified")
            afx.append(child)
        elif key in {("Static", 1004), ("Static", 2399), ("Static", 1365),
                     ("Button", 2), ("Button", 1009)} and key not in controls:
            controls[key] = child
        else:
            raise ConnectionNoticeError("connection_notice_controls_unverified")
    if (len(afx) != 2 or len(controls) != 5
            or controls[("Static", 1004)].get("text") != _BODY
            or controls[("Static", 2399)].get("text") != "报告问题"
            or controls[("Button", 2)].get("text") != "确定"
            or controls[("Button", 1009)].get("text") != ""
            or controls[("Button", 1009)].get("style", 0) & 0xF != 11
            or controls[("Button", 2)].get("enabled") is not True):
        raise ConnectionNoticeError("connection_notice_content_unverified")
    # Static 1365 is the owner-drawn title: existence is checked, text is not.
    return controls[("Button", 2)]["hwnd"]


class ConnectionNoticeGate:
    """One physical click per dialog HWND, with a cooldown across new HWNDs."""

    def __init__(self, provider, *, clock=time.monotonic, sleep=time.sleep):
        self.provider = provider
        self.clock = clock
        self.sleep = sleep
        self.attempted = set()
        self.cooldown_until = 0.0
        self.last_click_phase = None  # Invocation is not proof of delivered input.

    def close_once(self, main: int, pid: int) -> bool:
        dialogs = self.provider.owned_dialogs(main)
        if not dialogs:
            return False
        if len(dialogs) != 1:
            raise ConnectionNoticeError("connection_notice_not_unique")
        dialog = dialogs[0]
        original = self.provider.describe(main, pid, dialog)
        button = _verified_shape(original, main, pid, dialog)
        identity = (main, pid, dialog)
        if identity in self.attempted or self.clock() < self.cooldown_until:
            raise ConnectionNoticeError("connection_notice_retry_blocked")
        if (not self.provider.input_ready()
                or not self.provider.main_current(main, pid)):
            raise ConnectionNoticeError("connection_notice_input_unavailable")
        if (self.provider.owned_dialogs(main) != (dialog,)
                or self.provider.describe(main, pid, dialog) != original):
            raise ConnectionNoticeError("connection_notice_changed")
        if not self.provider.prepare_input(dialog, button):
            raise ConnectionNoticeError("connection_notice_target_unverified")
        if (self.provider.owned_dialogs(main) != (dialog,)
                or not self.provider.main_current(main, pid)
                or self.provider.describe(main, pid, dialog) != original
                or not self.provider.target_current(dialog, button)):
            raise ConnectionNoticeError("connection_notice_changed")
        self.attempted.add(identity)
        self.cooldown_until = self.clock() + _COOLDOWN_SECONDS
        self.last_click_phase = "call_started"
        try:
            self.provider.click_input(button)
        except Exception:
            raise ConnectionNoticeError("connection_notice_click_unverified") from None
        self.last_click_phase = "call_returned"
        deadline = self.clock() + _CLOSE_TIMEOUT_SECONDS
        while self.clock() < deadline:
            if self.provider.closed(dialog):
                self.last_click_phase = "dialog_closed"
                return True
            self.sleep(min(0.1, max(0.0, deadline - self.clock())))
        raise ConnectionNoticeError("connection_notice_close_unverified")


class Win32ConnectionNoticeProvider:
    """Bounded same-process Win32 observations; no input except verified click."""

    def __init__(self, adapter):
        import ctypes
        from ctypes import wintypes
        import win32con
        import win32gui
        import win32process

        self.adapter = adapter
        self.ctypes = ctypes
        self.gui = win32gui
        self.process = win32process
        self.con = win32con
        self.user = ctypes.WinDLL("user32", use_last_error=True)
        self.user.SendMessageTimeoutW.argtypes = (
            wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM,
            wintypes.UINT, wintypes.UINT, ctypes.POINTER(ctypes.c_size_t))
        self.user.SendMessageTimeoutW.restype = ctypes.c_size_t
        self.user.GetForegroundWindow.restype = wintypes.HWND
        self.user.SetForegroundWindow.argtypes = (wintypes.HWND,)
        self.user.SetForegroundWindow.restype = wintypes.BOOL
        self.user.WindowFromPoint.argtypes = (wintypes.POINT,)
        self.user.WindowFromPoint.restype = wintypes.HWND
        self.POINT = wintypes.POINT

    def owned_dialogs(self, main: int) -> tuple[int, ...]:
        found = []
        def collect(hwnd, _):
            if (hwnd != main and self.gui.IsWindow(hwnd) and self.gui.IsWindowVisible(hwnd)
                    and (self.gui.GetWindow(hwnd, self.con.GW_OWNER) == main
                         or self.gui.GetAncestor(hwnd, self.con.GA_ROOTOWNER) == main)):
                found.append(int(hwnd))
        self.gui.EnumWindows(collect, None)
        return tuple(found)

    def _text(self, hwnd: int, *, timeout_ms: int = 500) -> str:
        # Never ask an unknown process to handle a window message. The caller
        # has already checked every child's PID, class, ID and parent.
        buffer = self.ctypes.create_unicode_buffer(256)
        copied = self.ctypes.c_size_t()
        if not self.user.SendMessageTimeoutW(
                hwnd, 0x000D, len(buffer),
                self.ctypes.cast(buffer, self.ctypes.c_void_p).value,
                0x0002, timeout_ms, self.ctypes.byref(copied)):
            raise ConnectionNoticeError("connection_notice_text_unverified")
        if copied.value >= len(buffer) - 1 or len(buffer.value) != copied.value:
            raise ConnectionNoticeError("connection_notice_text_unverified")
        return buffer.value

    def describe(self, main: int, pid: int, dialog: int) -> dict:
        gui = self.gui
        if not gui.IsWindow(dialog):
            raise ConnectionNoticeError("connection_notice_dialog_unverified")
        _, observed_pid = self.process.GetWindowThreadProcessId(dialog)
        owner = int(gui.GetWindow(dialog, self.con.GW_OWNER) or 0)
        root_owner = int(gui.GetAncestor(dialog, self.con.GA_ROOTOWNER) or 0)
        parent = int(gui.GetParent(dialog) or 0)
        if (observed_pid != pid or gui.GetClassName(dialog) != "#32770"
                or owner != main or root_owner != main or parent not in (0, main)
                or not gui.IsWindowVisible(dialog) or not gui.IsWindowEnabled(dialog)):
            raise ConnectionNoticeError("connection_notice_dialog_unverified")
        handles = []
        gui.EnumChildWindows(dialog, lambda hwnd, _: handles.append(int(hwnd)), None)
        if len(handles) != 7:
            raise ConnectionNoticeError("connection_notice_controls_unverified")
        children = []
        for hwnd in handles:
            _, child_pid = self.process.GetWindowThreadProcessId(hwnd)
            class_name = gui.GetClassName(hwnd)
            control_id = int(gui.GetDlgCtrlID(hwnd))
            child_parent = int(gui.GetParent(hwnd) or 0)
            if (not gui.IsWindow(hwnd) or child_pid != pid or child_parent != dialog
                    or not gui.IsWindowVisible(hwnd)):
                raise ConnectionNoticeError("connection_notice_controls_unverified")
            if ((class_name, control_id) not in {
                    ("Static", 1004), ("Static", 2399), ("Static", 1365),
                    ("Button", 2), ("Button", 1009)}
                    and class_name != "AfxWnd140s"):
                raise ConnectionNoticeError("connection_notice_controls_unverified")
            if (class_name, control_id) == ("Static", 1365):
                text = None
            elif class_name == "AfxWnd140s" or (class_name, control_id) == ("Button", 1009):
                text = gui.GetWindowText(hwnd)
            else:
                text = self._text(hwnd, timeout_ms=2000 if
                                  (class_name, control_id) == ("Static", 1004)
                                  else 500)
            children.append({"hwnd": hwnd, "pid": child_pid,
                             "parent": child_parent, "class_name": class_name,
                             "control_id": control_id, "text": text,
                             "style": int(gui.GetWindowLong(hwnd, self.con.GWL_STYLE)),
                             "visible": True, "enabled": bool(gui.IsWindowEnabled(hwnd))})
        return {"hwnd": dialog, "pid": observed_pid, "class_name": "#32770",
                "owner": owner, "root_owner": root_owner, "parent": parent,
                "visible": True, "enabled": True, "children": tuple(children)}

    def input_ready(self) -> bool:
        return self.adapter._cursor_input_ready()

    def main_current(self, main: int, pid: int) -> bool:
        return self.adapter.app.process == pid and self.adapter.locate_main() == main

    def _hit_target(self, dialog: int, button: int) -> bool:
        if (not self.gui.IsWindow(dialog) or not self.gui.IsWindow(button)
                or self.gui.GetParent(button) != dialog):
            return False
        left, top, right, bottom = self.gui.GetWindowRect(button)
        dl, dt, dr, db = self.gui.GetWindowRect(dialog)
        if (left >= right or top >= bottom or not
                (dl <= left < right <= dr and dt <= top < bottom <= db)):
            return False
        point = self.POINT((left + right) // 2, (top + bottom) // 2)
        return int(self.user.WindowFromPoint(point) or 0) == button

    def prepare_input(self, dialog: int, button: int) -> bool:
        self.user.SetForegroundWindow(dialog)
        return self.target_current(dialog, button)

    def target_current(self, dialog: int, button: int) -> bool:
        return (int(self.user.GetForegroundWindow() or 0) == dialog
                and self._hit_target(dialog, button))

    def click_input(self, button: int) -> None:
        self.adapter.app.window(handle=button).wrapper_object().click_input()

    def closed(self, dialog: int) -> bool:
        return not self.gui.IsWindow(dialog)
