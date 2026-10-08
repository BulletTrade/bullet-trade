"""Explicit UI-only profile for one observed code-completion popup.

Diagnostic opt-in only. Filtering one known HWND cannot prove account identity,
selected security, order input correctness, or permission to submit.
"""
from __future__ import annotations

from dataclasses import dataclass

from .order_form_controls import FormObservation


_POPUP_CLASS = "Afx:00A90000:843:00000000:00000005:00000000"
_POPUP_STYLE = 0x44000000
_POPUP_EXSTYLE = 0x88
_SCROLL_ID = 487297329
_WS_VISIBLE = 0x10000000


class PopupProfileError(RuntimeError):
    """Fixed code only; no account, window text, or field contents."""


class Win32PopupApi:
    """Lazy native read surface; no input or window mutation methods."""

    def __init__(self):
        import ctypes
        from ctypes import wintypes
        import win32gui
        import win32process
        from .order_focus_input import WindowsOrderInputBackend

        self.gui, self.process = win32gui, win32process
        self.input = WindowsOrderInputBackend()
        self.user = self.input.user
        self.user.GetDesktopWindow.restype = wintypes.HWND
        self.user.GetAncestor.argtypes = (wintypes.HWND, wintypes.UINT)
        self.user.GetAncestor.restype = wintypes.HWND
        self.user.GetWindow.argtypes = (wintypes.HWND, wintypes.UINT)
        self.user.GetWindow.restype = wintypes.HWND
        self.user.GetForegroundWindow.restype = wintypes.HWND
        get_long = getattr(self.user, "GetWindowLongPtrW", None) or self.user.GetWindowLongW
        get_long.argtypes = (wintypes.HWND, ctypes.c_int)
        get_long.restype = ctypes.c_ssize_t
        self.get_long = get_long

    def is_window(self, hwnd): return bool(self.gui.IsWindow(hwnd))
    def visible(self, hwnd): return bool(self.gui.IsWindowVisible(hwnd))
    def class_name(self, hwnd): return self.gui.GetClassName(hwnd)
    def control_id(self, hwnd): return int(self.gui.GetDlgCtrlID(hwnd))
    def parent(self, hwnd): return int(self.gui.GetParent(hwnd) or 0)
    def owner(self, hwnd): return int(self.user.GetWindow(hwnd, 4) or 0)
    def ancestor(self, hwnd, flag): return int(self.user.GetAncestor(hwnd, flag) or 0)
    def desktop(self): return int(self.user.GetDesktopWindow() or 0)
    def rect(self, hwnd): return tuple(self.gui.GetWindowRect(hwnd))
    def style(self, hwnd): return int(self.get_long(hwnd, -16))
    def exstyle(self, hwnd): return int(self.get_long(hwnd, -20))
    def children(self, hwnd):
        found = []
        self.gui.EnumChildWindows(hwnd, lambda child, _: (found.append(int(child)) or True), None)
        return tuple(found)

    def pid_thread(self, hwnd):
        thread, pid = self.process.GetWindowThreadProcessId(hwnd)
        return int(pid), int(thread)

    def foreground_hwnd(self): return int(self.user.GetForegroundWindow() or 0)
    def gui_focus(self, thread): return self.input.gui_focus(thread)


@dataclass(frozen=True)
class _Shape:
    popup_class: str
    popup_pid_thread: tuple[int, int]
    popup_parent: int
    popup_owner: int
    popup_ancestors: tuple[int, int, int]
    popup_style_without_visibility: int
    popup_exstyle: int
    popup_rect: tuple[int, int, int, int]
    child_hwnd: int
    child_class: str
    child_id: int
    child_pid_thread: tuple[int, int]
    child_parent: int
    child_owner: int
    child_ancestors: tuple[int, int, int]
    child_style_without_visibility: int
    child_exstyle: int
    child_rect: tuple[int, int, int, int]


class NativeCodePopupProfile:
    """Freezes exactly one hidden popup and its sole scrollbar child."""

    def __init__(self, *, binding: FormObservation, popup_hwnd: int,
                 child_hwnd: int, native_api=None,
                 expected_child_id: int = _SCROLL_ID):
        if (not isinstance(binding, FormObservation)
                or type(popup_hwnd) is not int or popup_hwnd <= 0
                or type(child_hwnd) is not int or child_hwnd <= 0
                or popup_hwnd == child_hwnd
                or type(expected_child_id) is not int or expected_child_id < 0):
            raise PopupProfileError("popup_binding_unverified")
        code = [c for c in binding.controls if c.control_id == 1032]
        if len(code) != 1 or code[0].class_name != "Edit":
            raise PopupProfileError("code_edit_unverified")
        self.binding, self.popup_hwnd, self.child_hwnd = binding, popup_hwnd, child_hwnd
        self.expected_child_id = expected_child_id
        self.code = code[0]
        self.api = native_api if native_api is not None else Win32PopupApi()
        first = self._shape(require_hidden=True)
        second = self._shape(require_hidden=True)
        if first != second:
            raise PopupProfileError("popup_capture_unstable")
        self._frozen = second

    def _shape(self, *, require_hidden: bool = False) -> _Shape:
        try:
            a, b, code = self.api, self.binding, self.code
            p, c = self.popup_hwnd, self.child_hwnd
            if (not a.is_window(b.main_hwnd) or not a.is_window(code.hwnd)
                    or not a.is_window(p) or not a.is_window(c)):
                raise PopupProfileError("popup_missing")
            main_identity = a.pid_thread(b.main_hwnd)
            code_identity = a.pid_thread(code.hwnd)
            popup_identity = a.pid_thread(p)
            child_identity = a.pid_thread(c)
            if (main_identity != code_identity or main_identity != popup_identity
                    or main_identity != child_identity
                    or main_identity[0] != b.client_pid or main_identity[1] <= 0
                    or a.class_name(code.hwnd) != "Edit" or a.control_id(code.hwnd) != 1032
                    or a.parent(code.hwnd) != b.form_hwnd or a.rect(code.hwnd) != code.rect
                    or a.class_name(p) != _POPUP_CLASS
                    or a.parent(p) != a.desktop() or a.owner(p) != 0
                    or (a.style(p) & ~_WS_VISIBLE) != _POPUP_STYLE
                    or a.exstyle(p) != _POPUP_EXSTYLE
                    or a.children(p) != (c,)
                    or a.class_name(c) != "ScrollBar"
                    or a.control_id(c) != self.expected_child_id
                    or a.parent(c) != p):
                raise PopupProfileError("popup_structure_changed")
            popup_rect, child_rect = a.rect(p), a.rect(c)
            if (popup_rect[0] != code.rect[0] or popup_rect[1] != code.rect[3] + 1
                    or not (popup_rect[0] < popup_rect[2]
                            and popup_rect[1] < popup_rect[3]
                            and popup_rect[0] <= child_rect[0] < child_rect[2] <= popup_rect[2]
                            and popup_rect[1] <= child_rect[1] < child_rect[3] <= popup_rect[3])):
                raise PopupProfileError("popup_geometry_changed")
            if require_hidden and (a.visible(p) or a.style(p) & _WS_VISIBLE):
                raise PopupProfileError("popup_not_initially_hidden")
            ancestor_p = tuple(a.ancestor(p, flag) for flag in (1, 2, 3))
            ancestor_c = tuple(a.ancestor(c, flag) for flag in (1, 2, 3))
            if any(handle <= 0 for handle in (*ancestor_p, *ancestor_c)):
                raise PopupProfileError("popup_ancestry_unverified")
            return _Shape(_POPUP_CLASS, popup_identity, a.parent(p), a.owner(p),
                          ancestor_p, a.style(p) & ~_WS_VISIBLE, a.exstyle(p), popup_rect,
                          c, a.class_name(c), a.control_id(c), child_identity,
                          a.parent(c), a.owner(c), ancestor_c,
                          a.style(c) & ~_WS_VISIBLE, a.exstyle(c), child_rect)
        except PopupProfileError:
            raise
        except Exception:
            raise PopupProfileError("popup_metadata_unavailable") from None

    def qualifies(self) -> bool:
        """Recheck metadata; visible popup additionally requires code Edit focus."""
        try:
            first = self._shape()
            second = self._shape()
            if first != second or second != self._frozen:
                return False
            if self.api.visible(self.popup_hwnd):
                focused, error = self.api.gui_focus(second.popup_pid_thread[1])
                return (self.api.foreground_hwnd() == self.binding.main_hwnd
                        and error == 0 and focused == self.code.hwnd)
            return True
        except Exception:
            return False


class PopupFilteringObserverApi:
    """Delegates ObserverApi; filters only this exact, freshly qualified HWND."""

    def __init__(self, observer_api, profile: NativeCodePopupProfile):
        if not isinstance(profile, NativeCodePopupProfile):
            raise PopupProfileError("profile_unverified")
        self.observer_api, self.profile = observer_api, profile

    def __getattr__(self, name):
        return getattr(self.observer_api, name)

    def top_windows(self):
        windows = tuple(self.observer_api.top_windows())
        popup = self.profile.popup_hwnd
        try:
            exists = self.profile.api.is_window(popup)
        except Exception:
            raise PopupProfileError("popup_metadata_unavailable") from None
        if not exists:
            raise PopupProfileError("popup_missing")
        if windows.count(popup) > 1:
            raise PopupProfileError("popup_top_not_unique")
        if not self.profile.qualifies():
            raise PopupProfileError("popup_metadata_unverified")
        return tuple(hwnd for hwnd in windows if hwnd != popup)


class MultiPopupFilteringObserverApi:
    """Filter at most two independently captured, exact popup HWNDs."""

    def __init__(self, observer_api, profiles: tuple[NativeCodePopupProfile, ...]):
        if (not isinstance(profiles, tuple) or not 1 <= len(profiles) <= 2
                or any(not isinstance(profile, NativeCodePopupProfile)
                       for profile in profiles)):
            raise PopupProfileError("profiles_unverified")
        first = profiles[0]
        if (len({profile.popup_hwnd for profile in profiles}) != len(profiles)
                or any((profile.binding.main_hwnd, profile.binding.client_pid,
                        profile.code.hwnd) !=
                       (first.binding.main_hwnd, first.binding.client_pid,
                        first.code.hwnd) for profile in profiles)):
            raise PopupProfileError("profiles_binding_mismatch")
        self.observer_api, self.profiles = observer_api, profiles

    def __getattr__(self, name):
        return getattr(self.observer_api, name)

    def top_windows(self):
        windows = tuple(self.observer_api.top_windows())
        visible = 0
        known = set()
        for profile in self.profiles:
            popup = profile.popup_hwnd
            if windows.count(popup) > 1:
                raise PopupProfileError("popup_top_not_unique")
            try:
                exists = profile.api.is_window(popup)
            except Exception:
                raise PopupProfileError("popup_metadata_unavailable") from None
            if not exists:
                raise PopupProfileError("popup_missing")
            if not profile.qualifies():
                raise PopupProfileError("popup_metadata_unverified")
            try:
                visible += profile.api.visible(popup) is True
            except Exception:
                raise PopupProfileError("popup_metadata_unavailable") from None
            known.add(popup)
        if visible > 1:
            raise PopupProfileError("multiple_popups_visible")
        return tuple(hwnd for hwnd in windows if hwnd not in known)
