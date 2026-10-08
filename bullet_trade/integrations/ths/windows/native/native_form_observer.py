"""Read-only, fail-closed observation of the two THS order forms.

The account ComboBox mask is a window binding, never a business identity.
The caller must still supply an independent business-identity predicate.
"""

from __future__ import annotations

import ctypes
from ctypes import wintypes

from .native_order_form_backend import ES_MULTILINE, ES_PASSWORD
from .order_form_controls import Control, FormObservation, FormUnverified, _fingerprint


MAIN_TITLE = '网上股票交易系统5.0'
_FORM_CLASSES = {1032: 'Edit', 1033: 'Edit', 1034: 'Edit',
                 3001: 'Static', 1400: 'Static', 1399: 'Static',
                 1006: 'Button'}
_SIDE_PATHS = {'买入[F1]': ['买入[F1]'], '卖出[F2]': ['卖出[F2]']}


class Win32ObserverApi:
    """Small native enumeration surface; text is requested only by the observer."""

    def __init__(self):
        user32 = ctypes.WinDLL('user32', use_last_error=True)
        self._user32 = user32
        self._enum_proc = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
        user32.EnumWindows.argtypes = (self._enum_proc, wintypes.LPARAM)
        user32.EnumWindows.restype = wintypes.BOOL
        user32.EnumChildWindows.argtypes = (wintypes.HWND, self._enum_proc, wintypes.LPARAM)
        user32.EnumChildWindows.restype = wintypes.BOOL
        user32.IsWindow.argtypes = (wintypes.HWND,)
        user32.IsWindow.restype = wintypes.BOOL
        user32.IsWindowVisible.argtypes = (wintypes.HWND,)
        user32.IsWindowVisible.restype = wintypes.BOOL
        user32.IsWindowEnabled.argtypes = (wintypes.HWND,)
        user32.IsWindowEnabled.restype = wintypes.BOOL
        user32.GetWindowThreadProcessId.argtypes = (wintypes.HWND, ctypes.POINTER(wintypes.DWORD))
        user32.GetWindowThreadProcessId.restype = wintypes.DWORD
        user32.GetParent.argtypes = (wintypes.HWND,)
        user32.GetParent.restype = wintypes.HWND
        user32.IsChild.argtypes = (wintypes.HWND, wintypes.HWND)
        user32.IsChild.restype = wintypes.BOOL
        user32.GetDlgCtrlID.argtypes = (wintypes.HWND,)
        user32.GetDlgCtrlID.restype = ctypes.c_int
        user32.GetClassNameW.argtypes = (wintypes.HWND, wintypes.LPWSTR, ctypes.c_int)
        user32.GetClassNameW.restype = ctypes.c_int
        user32.GetWindowTextLengthW.argtypes = (wintypes.HWND,)
        user32.GetWindowTextLengthW.restype = ctypes.c_int
        user32.GetWindowTextW.argtypes = (wintypes.HWND, wintypes.LPWSTR, ctypes.c_int)
        user32.GetWindowTextW.restype = ctypes.c_int
        user32.GetWindowRect.argtypes = (wintypes.HWND, ctypes.POINTER(wintypes.RECT))
        user32.GetWindowRect.restype = wintypes.BOOL
        get_long = getattr(user32, 'GetWindowLongPtrW', None) or user32.GetWindowLongW
        get_long.argtypes = (wintypes.HWND, ctypes.c_int)
        get_long.restype = ctypes.c_ssize_t
        self._get_long = get_long

    def _enumerate(self, function, *args) -> tuple[int, ...]:
        handles = []
        callback = self._enum_proc(lambda hwnd, _: (handles.append(int(hwnd)) or True))
        if not function(*args, callback, 0):
            raise FormUnverified('native_enumeration_unavailable')
        return tuple(handles)

    def top_windows(self) -> tuple[int, ...]:
        return self._enumerate(self._user32.EnumWindows)

    def child_windows(self, hwnd: int) -> tuple[int, ...]:
        return self._enumerate(self._user32.EnumChildWindows, hwnd)

    def is_window(self, hwnd: int) -> bool: return bool(self._user32.IsWindow(hwnd))
    def is_visible(self, hwnd: int) -> bool: return bool(self._user32.IsWindowVisible(hwnd))
    def is_enabled(self, hwnd: int) -> bool: return bool(self._user32.IsWindowEnabled(hwnd))
    def parent(self, hwnd: int) -> int: return int(self._user32.GetParent(hwnd) or 0)
    def is_child(self, main: int, hwnd: int) -> bool: return bool(self._user32.IsChild(main, hwnd))
    def control_id(self, hwnd: int) -> int: return int(self._user32.GetDlgCtrlID(hwnd))

    def pid(self, hwnd: int) -> int:
        value = wintypes.DWORD()
        if not self._user32.GetWindowThreadProcessId(hwnd, ctypes.byref(value)):
            raise FormUnverified('native_pid_unavailable')
        return int(value.value)

    def class_name(self, hwnd: int) -> str:
        value = ctypes.create_unicode_buffer(64)
        if not self._user32.GetClassNameW(hwnd, value, len(value)):
            raise FormUnverified('native_class_unavailable')
        return value.value

    def text(self, hwnd: int) -> str:
        length = self._user32.GetWindowTextLengthW(hwnd)
        if not 0 <= length <= 128:
            raise FormUnverified('native_caption_unavailable')
        value = ctypes.create_unicode_buffer(length + 1)
        if self._user32.GetWindowTextW(hwnd, value, len(value)) != length:
            raise FormUnverified('native_caption_unavailable')
        return value.value

    def rect(self, hwnd: int) -> tuple[int, int, int, int]:
        value = wintypes.RECT()
        if not self._user32.GetWindowRect(hwnd, ctypes.byref(value)):
            raise FormUnverified('native_rect_unavailable')
        return value.left, value.top, value.right, value.bottom

    def style(self, hwnd: int) -> int:
        ctypes.set_last_error(0)
        value = self._get_long(hwnd, -16)
        if value == 0 and ctypes.get_last_error():
            raise FormUnverified('native_style_unavailable')
        return int(value)


class NativeFormObserver:
    def __init__(self, *, adapter, main_hwnd: int, client_pid: int,
                 account_mask: str, api=None, diagnostics: dict | None = None):
        if (type(main_hwnd) is not int or main_hwnd <= 0
                or type(client_pid) is not int or client_pid <= 0
                or not isinstance(account_mask, str) or not account_mask):
            raise ValueError('invalid_observer_binding')
        self._adapter = adapter
        self._main_hwnd = main_hwnd
        self._client_pid = client_pid
        self._account_mask = account_mask
        self._api = api if api is not None else Win32ObserverApi()
        if diagnostics is not None and not isinstance(diagnostics, dict):
            raise TypeError('invalid_observer_diagnostics')
        self._diagnostics = diagnostics

    def _record_matches(self, reason: str, matched: dict[int, list[int]]) -> None:
        """Only target ID, HWND, class and parent; no control or account text."""
        if self._diagnostics is None:
            return
        api = self._api
        summary = {}
        for control_id, handles in matched.items():
            candidates = []
            for hwnd in handles:
                try:
                    cls, parent = api.class_name(hwnd), api.parent(hwnd)
                except Exception:
                    cls, parent = None, None
                candidates.append({'hwnd': hwnd, 'class': cls, 'parent': parent})
            summary[control_id] = {'count': len(handles), 'candidates': candidates}
        self._diagnostics.update(stage='controls', reason=reason, matches=summary)

    def _snapshot(self) -> tuple[FormObservation, tuple]:
        api, main = self._api, self._main_hwnd
        mains = [hwnd for hwnd in api.top_windows()
                 if api.is_window(hwnd) and api.pid(hwnd) == self._client_pid
                 and api.text(hwnd) == MAIN_TITLE]
        if (mains != [main] or not api.is_visible(main) or not api.is_enabled(main)
                or api.parent(main) != 0):
            raise FormUnverified('main_window_unverified')
        children = api.child_windows(main)
        if len(children) != len(set(children)):
            raise FormUnverified('control_not_unique')
        raw_matches: dict[int, list[int]] = {key: [] for key in (*_FORM_CLASSES, 2322, 129)}
        for hwnd in children:
            if api.is_window(hwnd) and api.is_visible(hwnd):
                control_id = api.control_id(hwnd)
                if control_id in raw_matches:
                    raw_matches[control_id].append(hwnd)
        matched: dict[int, list[int]] = {}
        for control_id, handles in raw_matches.items():
            expected = ('ComboBox' if control_id == 2322 else
                        'SysTreeView32' if control_id == 129 else _FORM_CLASSES[control_id])
            exact = [hwnd for hwnd in handles if api.class_name(hwnd) == expected]
            if len(exact) != 1:
                self._record_matches('control_not_unique', raw_matches)
                raise FormUnverified('control_not_unique')
            matched[control_id] = exact
            hwnd = exact[0]
            if (api.pid(hwnd) != self._client_pid
                    or not api.is_child(main, hwnd) or not api.is_enabled(hwnd)):
                self._record_matches('control_context_unverified', raw_matches)
                raise FormUnverified('control_context_unverified')
        combo, tree_hwnd = matched[2322][0], matched[129][0]
        # No other ComboBox or Static text is requested.
        mask = self._adapter.app.window(handle=combo).wrapper_object().selected_text()
        if mask != self._account_mask:
            raise FormUnverified('account_mask_changed')
        tree = self._adapter._visible_query_tree(main)
        if getattr(tree, 'handle', None) != tree_hwnd:
            raise FormUnverified('tree_context_unverified')
        selected = [name for name, path in _SIDE_PATHS.items()
                    if tree.get_item(path).is_selected() is True]
        if len(selected) != 1:
            raise FormUnverified('selected_tree_unverified')
        controls = []
        parents = set()
        for control_id, expected in _FORM_CLASSES.items():
            hwnd = matched[control_id][0]
            parent = api.parent(hwnd)
            parents.add(parent)
            if parent <= 0 or not api.is_child(main, parent):
                raise FormUnverified('form_context_unverified')
            if expected == 'Edit' and api.style(hwnd) & (ES_PASSWORD | ES_MULTILINE):
                raise FormUnverified('edit_style_unverified')
            controls.append(Control(control_id, expected, hwnd, parent, True, True,
                                    api.rect(hwnd),
                                    api.text(hwnd) if expected in ('Static', 'Button') else ''))
        if len(parents) != 1:
            raise FormUnverified('form_group_unverified')
        form = parents.pop()
        if (form == main or not api.is_window(form) or api.pid(form) != self._client_pid
                or not api.is_visible(form) or not api.is_enabled(form)):
            raise FormUnverified('form_context_unverified')
        observation = FormObservation(main, MAIN_TITLE, form, self._client_pid,
                                      mask, selected[0], True, True, True, True,
                                      tuple(controls))
        _fingerprint(observation, 'buy' if selected[0] == '买入[F1]' else 'sell')
        return observation, (tuple(mains), combo, tree_hwnd, tuple(controls))

    def observe(self) -> FormObservation:
        if self._diagnostics is not None:
            self._diagnostics.clear()
        try:
            first, before = self._snapshot()
            second, after = self._snapshot()
        except FormUnverified:
            if self._diagnostics is not None and 'stage' not in self._diagnostics:
                self._diagnostics.update(stage='snapshot', reason='form_observation_unverified')
            raise
        except Exception:
            if self._diagnostics is not None and 'stage' not in self._diagnostics:
                self._diagnostics.update(stage='snapshot', reason='form_observation_unavailable')
            raise FormUnverified('form_observation_unavailable') from None
        if first != second or before != after:
            if self._diagnostics is not None:
                self._diagnostics.update(stage='recheck', reason='form_observation_changed')
            raise FormUnverified('form_observation_changed')
        return second
