"""Bounded native UI binding guard for an already verified THS order form.

This is not business identity, complete query evidence, or a GateProof. Tree
caret only freezes the caller-verified caret; it does not prove unique selection.
"""
from __future__ import annotations

import ctypes

from .native_form_observer import Win32ObserverApi
from .native_order_form_backend import ES_MULTILINE, ES_PASSWORD, Win32FormApi
from .order_form_controls import FormObservation, _fingerprint


CB_GETCURSEL = 0x0147
TVM_GETNEXTITEM = 0x110A
TVGN_CARET = 9
_DEFAULT_TIMEOUT_MS = 250
_CONTROLS = {1032: "Edit", 1033: "Edit", 1034: "Edit",
             3001: "Static", 1400: "Static", 1399: "Static", 1006: "Button"}
_TARGETS = {**_CONTROLS, 2322: "ComboBox", 129: "SysTreeView32"}


class UiBindingUnverified(RuntimeError):
    """Fixed reason code, never a caption or account value."""


def _native_owner(hwnd: int) -> int:
    user32 = ctypes.WinDLL("user32", use_last_error=True)
    user32.GetWindow.argtypes = (ctypes.c_void_p, ctypes.c_uint)
    user32.GetWindow.restype = ctypes.c_void_p
    return int(user32.GetWindow(hwnd, 4) or 0)  # GW_OWNER


class NativeOrderUiGuard:
    def __init__(self, *, binding: FormObservation, tree_hwnd: int,
                 combo_hwnd: int, observer_api=None, form_api=None, owner_of=None,
                 timeout_ms: int = _DEFAULT_TIMEOUT_MS):
        if type(timeout_ms) is not int or not 1 <= timeout_ms <= 2000:
            raise ValueError("invalid_native_timeout")
        if not isinstance(binding, FormObservation):
            raise UiBindingUnverified("binding_unverified")
        side = {"买入[F1]": "buy", "卖出[F2]": "sell"}.get(binding.selected_tree)
        try:
            if side is None:
                raise ValueError
            _fingerprint(binding, side)
        except Exception:
            raise UiBindingUnverified("binding_unverified") from None
        if (type(tree_hwnd) is not int or tree_hwnd <= 0
                or type(combo_hwnd) is not int or combo_hwnd <= 0
                or tree_hwnd == combo_hwnd):
            raise UiBindingUnverified("binding_unverified")
        self.binding = binding
        self.tree_hwnd, self.combo_hwnd = tree_hwnd, combo_hwnd
        self.api = observer_api if observer_api is not None else Win32ObserverApi()
        self.form_api = form_api if form_api is not None else Win32FormApi()
        self.owner_of = owner_of if owner_of is not None else _native_owner
        self.timeout_ms = timeout_ms
        self._top: frozenset[int] | None = None
        self._combo_index: int | None = None
        self._tree_caret: int | None = None
        self.last_error: str | None = None
        self._top = self._structure()

    def _structure(self) -> frozenset[int]:
        try:
            api, binding = self.api, self.binding
            main, form, pid = binding.main_hwnd, binding.form_hwnd, binding.client_pid
            if (not api.is_window(main) or api.parent(main) != 0
                    or api.text(main) != binding.main_title or api.pid(main) != pid
                    or not api.is_visible(main) or not api.is_enabled(main)
                    or not api.is_window(form) or not api.is_child(main, form)
                    or api.pid(form) != pid or not api.is_visible(form)
                    or not api.is_enabled(form)):
                raise UiBindingUnverified("main_form_unverified")
            top = frozenset(hwnd for hwnd in api.top_windows()
                             if api.is_window(hwnd) and api.is_visible(hwnd)
                             and api.pid(hwnd) == pid)
            if main not in top:
                raise UiBindingUnverified("top_level_changed")
            for hwnd in api.top_windows():
                if (api.is_window(hwnd) and api.is_visible(hwnd)
                        and api.class_name(hwnd) == "#32770"
                        and self.owner_of(hwnd) == main):
                    raise UiBindingUnverified("owned_dialog_unverified")
            if self._top is not None and top != self._top:
                raise UiBindingUnverified("top_level_changed")
            children = api.child_windows(main)
            expected = {control.control_id: control for control in binding.controls}
            for control_id, cls in _TARGETS.items():
                candidates = [hwnd for hwnd in children if api.is_window(hwnd)
                              and api.is_visible(hwnd) and api.control_id(hwnd) == control_id
                              and api.class_name(hwnd) == cls]
                if len(candidates) != 1:
                    raise UiBindingUnverified("control_not_unique")
                hwnd = candidates[0]
                bound = (self.tree_hwnd if control_id == 129 else
                         self.combo_hwnd if control_id == 2322 else expected[control_id].hwnd)
                if (hwnd != bound or api.pid(hwnd) != pid or not api.is_enabled(hwnd)
                        or not api.is_child(main, hwnd)):
                    raise UiBindingUnverified("control_binding_changed")
                if control_id in _CONTROLS:
                    control = expected[control_id]
                    if (api.parent(hwnd) != form or api.rect(hwnd) != control.rect
                            or (cls in ("Static", "Button")
                                and api.text(hwnd) != control.caption)
                            or (cls == "Edit" and api.style(hwnd) &
                                (ES_PASSWORD | ES_MULTILINE))):
                        raise UiBindingUnverified("control_binding_changed")
            return top
        except UiBindingUnverified:
            raise
        except Exception:
            raise UiBindingUnverified("native_observation_unavailable") from None

    def _selection(self) -> tuple[int, int]:
        try:
            index = self.form_api.send_message_timeout(
                self.combo_hwnd, CB_GETCURSEL, 0, None, self.timeout_ms)
            caret = self.form_api.send_message_timeout(
                self.tree_hwnd, TVM_GETNEXTITEM, TVGN_CARET, None, self.timeout_ms)
        except Exception:
            raise UiBindingUnverified("selection_message_unavailable") from None
        # CB_ERR (-1) is returned as unsigned by Win32FormApi's c_size_t.
        if (type(index) is not int or not 0 <= index <= 65535
                or type(caret) is not int or caret <= 0):
            raise UiBindingUnverified("selection_unverified")
        return index, caret

    def capture(self) -> None:
        """Call only after independent observation certified the selected tree."""
        if self._combo_index is not None or self._tree_caret is not None:
            raise UiBindingUnverified("capture_repeated")
        self._structure()
        first = self._selection()
        second = self._selection()
        self._structure()
        if first != second:
            raise UiBindingUnverified("selection_unstable")
        self._combo_index, self._tree_caret = second

    def is_current(self) -> bool:
        """Fail closed on UI drift. Caller must separately check business identity."""
        try:
            if self._combo_index is None or self._tree_caret is None:
                raise UiBindingUnverified("capture_required")
            self._structure()
            first = self._selection()
            second = self._selection()
            self._structure()
            if first != second or second != (self._combo_index, self._tree_caret):
                raise UiBindingUnverified("selection_changed")
            self.last_error = None
            return True
        except UiBindingUnverified as exc:
            self.last_error = str(exc)
            return False
