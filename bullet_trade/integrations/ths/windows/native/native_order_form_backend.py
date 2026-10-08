"""Bounded native access to three verified order-form Edit controls only.

The caller supplies a fresh form observer and an independent business-identity
predicate. This module never selects an account, clicks a button, or submits.
"""

from __future__ import annotations

import ctypes
from typing import Callable

from .order_form_controls import FormObservation, FormUnverified


EM_GETLINECOUNT = 0x00BA
EM_LINELENGTH = 0x00C1
EM_GETLINE = 0x00C4
WM_SETTEXT = 0x000C
ES_MULTILINE = 0x0004
ES_PASSWORD = 0x0020
_EDIT_IDS = frozenset((1032, 1033, 1034))
_MAX_CHARS = 64


def _read_single_edit_line(send, hwnd: int) -> str:
    """Transport only; caller must check Edit and business context separately."""
    if send(hwnd, EM_GETLINECOUNT, 0) != 1:
        raise FormUnverified('edit_line_count_unverified')
    length = send(hwnd, EM_LINELENGTH, 0)
    if not 0 <= length <= _MAX_CHARS:
        raise FormUnverified('edit_length_unverified')
    buffer = ctypes.create_unicode_buffer(_MAX_CHARS + 1)
    ctypes.c_ushort.from_buffer(buffer).value = _MAX_CHARS
    count = send(hwnd, EM_GETLINE, 0, buffer)
    if count != length or not 0 <= count <= _MAX_CHARS:
        raise FormUnverified('edit_length_changed')
    value = ''.join(buffer[:count])
    if '\r' in value or '\n' in value or '\x00' in value:
        raise FormUnverified('edit_line_unverified')
    return value


class Win32FormApi:
    """Thin lazy-loaded Win32 transport; no window discovery or GUI action."""

    def __init__(self):
        from ctypes import wintypes

        self._ctypes = ctypes
        self._wintypes = wintypes
        user32 = ctypes.WinDLL('user32', use_last_error=True)
        self._user32 = user32
        user32.IsWindow.argtypes = (wintypes.HWND,)
        user32.IsWindow.restype = wintypes.BOOL
        user32.IsWindowVisible.argtypes = (wintypes.HWND,)
        user32.IsWindowVisible.restype = wintypes.BOOL
        user32.IsWindowEnabled.argtypes = (wintypes.HWND,)
        user32.IsWindowEnabled.restype = wintypes.BOOL
        user32.GetParent.argtypes = (wintypes.HWND,)
        user32.GetParent.restype = wintypes.HWND
        user32.IsChild.argtypes = (wintypes.HWND, wintypes.HWND)
        user32.IsChild.restype = wintypes.BOOL
        user32.GetDlgCtrlID.argtypes = (wintypes.HWND,)
        user32.GetDlgCtrlID.restype = ctypes.c_int
        user32.GetWindowThreadProcessId.argtypes = (wintypes.HWND, ctypes.POINTER(wintypes.DWORD))
        user32.GetWindowThreadProcessId.restype = wintypes.DWORD
        user32.GetClassNameW.argtypes = (wintypes.HWND, wintypes.LPWSTR, ctypes.c_int)
        user32.GetClassNameW.restype = ctypes.c_int
        get_long = getattr(user32, 'GetWindowLongPtrW', None)
        if get_long is None:
            get_long = user32.GetWindowLongW
        get_long.argtypes = (wintypes.HWND, ctypes.c_int)
        get_long.restype = ctypes.c_ssize_t
        self._get_long = get_long
        user32.SendMessageTimeoutW.argtypes = (
            wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM,
            wintypes.UINT, wintypes.UINT, ctypes.POINTER(ctypes.c_size_t))
        user32.SendMessageTimeoutW.restype = wintypes.LPARAM

    def is_window(self, hwnd: int) -> bool:
        return bool(self._user32.IsWindow(hwnd))

    def is_visible(self, hwnd: int) -> bool:
        return bool(self._user32.IsWindowVisible(hwnd))

    def is_enabled(self, hwnd: int) -> bool:
        return bool(self._user32.IsWindowEnabled(hwnd))

    def parent(self, hwnd: int) -> int:
        return int(self._user32.GetParent(hwnd) or 0)

    def is_child(self, parent_hwnd: int, hwnd: int) -> bool:
        return bool(self._user32.IsChild(parent_hwnd, hwnd))

    def control_id(self, hwnd: int) -> int:
        return int(self._user32.GetDlgCtrlID(hwnd))

    def pid(self, hwnd: int) -> int:
        value = self._wintypes.DWORD()
        if not self._user32.GetWindowThreadProcessId(hwnd, ctypes.byref(value)):
            raise FormUnverified('native_pid_unverified')
        return int(value.value)

    def class_name(self, hwnd: int) -> str:
        buffer = ctypes.create_unicode_buffer(64)
        if not self._user32.GetClassNameW(hwnd, buffer, len(buffer)):
            raise FormUnverified('native_class_unverified')
        return buffer.value

    def style(self, hwnd: int) -> int:
        ctypes.set_last_error(0)
        value = self._get_long(hwnd, -16)  # GWL_STYLE
        if value == 0 and ctypes.get_last_error():
            raise FormUnverified('native_style_unverified')
        return int(value)

    def send_message_timeout(self, hwnd: int, message: int, wparam: int,
                             buffer: ctypes.Array | None, timeout_ms: int) -> int:
        result = ctypes.c_size_t()
        lparam = ctypes.addressof(buffer) if buffer is not None else 0
        # SMTO_BLOCK | SMTO_ABORTIFHUNG: no indefinite cross-process call.
        ok = self._user32.SendMessageTimeoutW(
            hwnd, message, wparam, lparam, 0x0001 | 0x0002,
            timeout_ms, ctypes.byref(result))
        if not ok:
            raise FormUnverified('native_message_unavailable')
        return int(result.value)


class NativeOrderFormBackend:
    def __init__(self, *, observe: Callable[[], FormObservation],
                 current_business_identity: Callable[[], bool],
                 binding: FormObservation, api=None, timeout_ms: int = 250,
                 write_action: Callable[[int, str], None] | None = None):
        if type(timeout_ms) is not int or not 1 <= timeout_ms <= 2000:
            raise ValueError('invalid_native_timeout')
        if write_action is not None and not callable(write_action):
            raise ValueError('invalid_write_action')
        self._api = api if api is not None else Win32FormApi()
        self._observe = observe
        self._business_identity = current_business_identity
        self._binding = self._context(binding)
        self._timeout_ms = timeout_ms
        # Explicit profile choice only. A failed WM_SETTEXT never falls back.
        self._write_action = write_action

    @staticmethod
    def _context(observation: FormObservation) -> tuple:
        if (not isinstance(observation, FormObservation)
                or type(observation.main_hwnd) is not int or observation.main_hwnd <= 0
                or type(observation.form_hwnd) is not int or observation.form_hwnd <= 0
                or observation.form_hwnd == observation.main_hwnd
                or observation.main_title != '网上股票交易系统5.0'
                or type(observation.client_pid) is not int or observation.client_pid <= 0
                or not isinstance(observation.account_mask, str) or not observation.account_mask
                or observation.selected_tree not in ('买入[F1]', '卖出[F2]')
                or observation.main_visible is not True or observation.main_enabled is not True
                or observation.form_visible is not True or observation.form_enabled is not True):
            raise FormUnverified('form_binding_unverified')
        edits = []
        for control_id in sorted(_EDIT_IDS):
            found = [control for control in observation.controls
                     if control.control_id == control_id]
            if len(found) != 1:
                raise FormUnverified('form_binding_unverified')
            control = found[0]
            if (control.class_name != 'Edit' or type(control.hwnd) is not int
                    or control.hwnd <= 0 or control.parent_hwnd != observation.form_hwnd
                    or control.visible is not True or control.enabled is not True):
                raise FormUnverified('form_binding_unverified')
            edits.append((control_id, control.hwnd))
        if len({hwnd for _, hwnd in edits}) != 3:
            raise FormUnverified('form_binding_unverified')
        return (observation.main_hwnd, observation.main_title,
                observation.form_hwnd, observation.client_pid,
                observation.account_mask, observation.selected_tree, tuple(edits))

    def observe(self) -> FormObservation:
        try:
            identity_current = self._business_identity()
            observation = self._observe()
        except Exception:
            raise FormUnverified('form_observation_unavailable') from None
        if identity_current is not True:
            raise FormUnverified('business_identity_unverified')
        if self._context(observation) != self._binding:
            raise FormUnverified('form_binding_changed')
        return observation

    def _edit(self, hwnd: int) -> None:
        observation = self.observe()
        expected = dict(self._binding[-1])
        if type(hwnd) is not int or hwnd not in expected.values():
            raise FormUnverified('edit_not_bound')
        control_id = next(key for key, value in expected.items() if value == hwnd)
        try:
            api = self._api
            valid = (api.is_window(observation.main_hwnd)
                     and api.is_window(observation.form_hwnd)
                     and api.is_window(hwnd)
                     and api.is_visible(observation.main_hwnd)
                     and api.is_enabled(observation.main_hwnd)
                     and api.is_visible(observation.form_hwnd)
                     and api.is_enabled(observation.form_hwnd)
                     and api.pid(observation.main_hwnd) == observation.client_pid
                     and api.pid(observation.form_hwnd) == observation.client_pid
                     and api.pid(hwnd) == observation.client_pid
                     and api.is_child(observation.main_hwnd, observation.form_hwnd)
                     and api.parent(hwnd) == observation.form_hwnd
                     and api.class_name(hwnd) == 'Edit'
                     and api.control_id(hwnd) == control_id
                     and api.is_visible(hwnd) and api.is_enabled(hwnd)
                     and not api.style(hwnd) & (ES_PASSWORD | ES_MULTILINE))
        except Exception:
            raise FormUnverified('edit_context_unavailable') from None
        if not valid:
            raise FormUnverified('edit_context_unverified')

    def _message(self, hwnd: int, message: int, wparam: int,
                 buffer: ctypes.Array | None = None) -> int:
        try:
            return self._api.send_message_timeout(
                hwnd, message, wparam, buffer, self._timeout_ms)
        except Exception:
            raise FormUnverified('native_message_unavailable') from None

    def get_line(self, edit_hwnd: int) -> str:
        self._edit(edit_hwnd)
        value = _read_single_edit_line(self._message, edit_hwnd)
        self._edit(edit_hwnd)
        return value

    def set_edit(self, edit_hwnd: int, value: str) -> None:
        if (not isinstance(value, str) or len(value) > _MAX_CHARS
                or any(char in value for char in ('\r', '\n', '\x00'))):
            raise FormUnverified('edit_value_unverified')
        self._edit(edit_hwnd)
        if self._write_action is not None:
            try:
                if self._write_action(edit_hwnd, value) is not None:
                    raise FormUnverified('edit_write_action_unverified')
            except Exception:
                raise FormUnverified('edit_write_action_unverified') from None
            self._edit(edit_hwnd)
            if self.get_line(edit_hwnd) != value:
                raise FormUnverified('edit_write_readback_unverified')
            return
        buffer = ctypes.create_unicode_buffer(value)
        if self._message(edit_hwnd, WM_SETTEXT, 0, buffer) != 1:
            raise FormUnverified('edit_write_unverified')
        self._edit(edit_hwnd)
