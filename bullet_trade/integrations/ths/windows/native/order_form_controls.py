"""Guarded THS order-form input. This module has no order-submit operation.

The caller holds the GUI actor lock and supplies fresh window observations plus
an independent, current business-identity check for every action.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import re
from typing import Callable, Protocol


class FormUnverified(RuntimeError):
    """Fixed error codes only; never includes account or field contents."""


@dataclass(frozen=True)
class Control:
    control_id: int
    class_name: str
    hwnd: int
    parent_hwnd: int
    visible: bool
    enabled: bool
    rect: tuple[int, int, int, int]
    caption: str = ""


@dataclass(frozen=True)
class FormObservation:
    main_hwnd: int
    main_title: str
    form_hwnd: int
    client_pid: int
    account_mask: str
    selected_tree: str
    main_visible: bool
    main_enabled: bool
    form_visible: bool
    form_enabled: bool
    controls: tuple[Control, ...]


@dataclass(frozen=True)
class OrderPlan:
    side: str  # buy or sell
    code: str
    price: Decimal
    quantity: int
    tick: Decimal  # Supplied by caller from verified security rules.


class FormBackend(Protocol):
    def observe(self) -> FormObservation: ...
    def get_line(self, edit_hwnd: int) -> str: ...
    def set_edit(self, edit_hwnd: int, value: str) -> None: ...


_FIELDS = ((1032, 3001), (1033, 1400), (1034, 1399))
_SIDE = {"buy": ("买入[F1]", "买入", "买入价格", "买入数量"),
         "sell": ("卖出[F2]", "卖出", "卖出价格", "卖出数量")}


def _positive_decimal(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
        raise FormUnverified("invalid_decimal")
    return value


def _valid_original(values: tuple[str, str, str]) -> bool:
    code, price, quantity = values
    if not all(isinstance(value, str) for value in values):
        return False
    if code and re.fullmatch(r"[0-9]{6}", code) is None:
        return False
    if price:
        if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", price) is None:
            return False
        try:
            if Decimal(price) <= 0:
                return False
        except InvalidOperation:
            return False
    return not quantity or (re.fullmatch(r"[0-9]+", quantity) is not None and int(quantity) > 0)


def _control(observation: FormObservation, control_id: int, cls: str) -> Control:
    found = [control for control in observation.controls if control.control_id == control_id]
    if len(found) != 1:
        raise FormUnverified("control_not_unique")
    control = found[0]
    if (control.class_name != cls or type(control.hwnd) is not int or control.hwnd <= 0
            or control.parent_hwnd != observation.form_hwnd
            or control.visible is not True or control.enabled is not True):
        raise FormUnverified("control_context_unverified")
    rect = control.rect
    if (not isinstance(rect, tuple) or len(rect) != 4
            or any(type(value) is not int for value in rect)
            or rect[0] >= rect[2] or rect[1] >= rect[3]):
        raise FormUnverified("control_geometry_unverified")
    return control


def _fingerprint(observation: FormObservation, side: str) -> tuple:
    if side not in _SIDE:
        raise FormUnverified("side_unverified")
    if (type(observation.main_hwnd) is not int or observation.main_hwnd <= 0
            or observation.main_title != "网上股票交易系统5.0"
            or type(observation.form_hwnd) is not int or observation.form_hwnd <= 0
            or type(observation.client_pid) is not int or observation.client_pid <= 0
            or observation.main_visible is not True or observation.main_enabled is not True
            or observation.form_visible is not True or observation.form_enabled is not True
            or not isinstance(observation.account_mask, str) or not observation.account_mask):
        raise FormUnverified("window_unverified")
    tree, button_caption, price_label, quantity_label = _SIDE[side]
    if observation.selected_tree != tree:
        raise FormUnverified("selected_tree_unverified")
    controls = []
    for (edit_id, label_id), caption in zip(_FIELDS, ("证券代码", price_label, quantity_label)):
        edit = _control(observation, edit_id, "Edit")
        label = _control(observation, label_id, "Static")
        if label.caption != caption:
            raise FormUnverified("label_unverified")
        lx1, ly1, lx2, ly2 = label.rect
        ex1, ey1, ex2, ey2 = edit.rect
        overlap = min(ly2, ey2) - max(ly1, ey1)
        if not (0 <= ex1 - lx2 <= 100 and overlap > 0
                and overlap * 2 >= min(ly2 - ly1, ey2 - ey1)):
            raise FormUnverified("label_geometry_unverified")
        controls.extend((edit, label))
    button = _control(observation, 1006, "Button")
    if button.caption != button_caption:
        raise FormUnverified("button_unverified")
    controls.append(button)
    if len({control.hwnd for control in controls}) != len(controls):
        raise FormUnverified("control_not_unique")
    return (observation.main_hwnd, observation.main_title,
            observation.form_hwnd, observation.client_pid,
            observation.account_mask, observation.selected_tree, tuple(controls))


class OrderFormControls:
    def __init__(self, backend: FormBackend, *, main_hwnd: int, client_pid: int,
                 account_mask: str, current_business_identity: Callable[[], bool]):
        self._backend = backend
        self._main_hwnd = main_hwnd
        self._client_pid = client_pid
        self._account_mask = account_mask
        self._business_identity = current_business_identity
        self._side: str | None = None
        self._binding: tuple | None = None
        self._original: tuple[str, str, str] | None = None

    def _current(self, side: str) -> tuple:
        if self._business_identity() is not True:
            raise FormUnverified("business_identity_unverified")
        observation = self._backend.observe()
        binding = _fingerprint(observation, side)
        if (observation.main_hwnd != self._main_hwnd
                or observation.client_pid != self._client_pid
                or observation.account_mask != self._account_mask):
            raise FormUnverified("window_identity_changed")
        if self._binding is not None and binding != self._binding:
            raise FormUnverified("form_binding_changed")
        return binding

    def capture(self, side: str) -> tuple[str, str, str]:
        """Capture only the three verified order Edit values; no password reads."""
        self._binding = None
        self._side = None
        self._original = None
        binding = self._current(side)
        self._binding, self._side = binding, side
        try:
            values = tuple(self._backend.get_line(self._current(side)[-1][index * 2].hwnd)
                           for index in range(3))
        except Exception:
            self._binding, self._side = None, None
            raise
        if len(values) != 3 or not _valid_original(values):
            self._binding, self._side = None, None
            raise FormUnverified("original_fields_unverified")
        self._original = values
        return values

    def _write_checked(self, index: int, value: str) -> None:
        assert self._side is not None and self._binding is not None
        binding = self._current(self._side)
        edit = binding[-1][index * 2]
        self._backend.set_edit(edit.hwnd, value)
        self._current(self._side)
        if self._backend.get_line(edit.hwnd) != value:
            raise FormUnverified("field_readback_mismatch")

    def _verify_all(self, expected: tuple[str, str, str]) -> None:
        """Catch later edits that changed an earlier field through form linkage."""
        assert self._side is not None
        observed = tuple(self._backend.get_line(self._current(self._side)[-1][index * 2].hwnd)
                         for index in range(3))
        self._current(self._side)
        if observed != expected:
            raise FormUnverified("form_readback_mismatch")

    def fill_without_submit(self, plan: OrderPlan) -> tuple[str, str, str]:
        if self._binding is None or self._side is None or self._original is None:
            raise FormUnverified("capture_required")
        if not isinstance(plan, OrderPlan) or plan.side != self._side:
            raise FormUnverified("side_unverified")
        if not isinstance(plan.code, str) or re.fullmatch(r"[0-9]{6}", plan.code) is None:
            raise FormUnverified("code_invalid")
        price, tick = _positive_decimal(plan.price), _positive_decimal(plan.tick)
        if price % tick != 0:
            raise FormUnverified("price_tick_mismatch")
        if type(plan.quantity) is not int or plan.quantity <= 0:
            raise FormUnverified("quantity_invalid")
        values = (plan.code, format(price, "f"), str(plan.quantity))
        for index, value in enumerate(values):
            self._write_checked(index, value)
        self._verify_all(values)
        return values

    def safe_restore(self) -> None:
        """Restore captured fields only while the same identity and controls remain."""
        if self._side is None or self._binding is None or self._original is None:
            raise FormUnverified("capture_required")
        if not _valid_original(self._original):
            raise FormUnverified("original_fields_unverified")
        self._current(self._side)
        for index, value in enumerate(self._original):
            self._write_checked(index, value)
        self._verify_all(self._original)
