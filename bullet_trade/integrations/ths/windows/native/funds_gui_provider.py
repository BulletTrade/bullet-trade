"""Read-only funds-page sampling for one simulated xiadan main window.

Window controls, refresh evidence and business identity are separate inputs.
No default total-assets mapping exists: the observed 2026-09-28 inventory only
verified cash balance, frozen cash and available cash.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Mapping, Sequence

from .account_binding_contract import BindingProof
from .funds_control_reader import AMOUNT
from .funds_query_contract import FundsQueryObservation


MAIN_TITLE = "网上股票交易系统5.0"
FUNDS_PAGE = "资金股票"


class FundsSamplingUnverified(RuntimeError):
    """A fixed reason code, without account numbers or control text."""


@dataclass(frozen=True)
class RefreshEvidence:
    main_hwnd: int
    page: str
    generation: str


@dataclass(frozen=True)
class IdentityEvidence:
    business_account: str | None
    kind: str
    source: str
    login_state: str
    main_hwnd: int
    refresh_id: str
    binding_proof: BindingProof | None = None
    binding_verifier: Callable[[BindingProof], None] | None = None


@dataclass(frozen=True)
class TotalAssetsControlSpec:
    """IDs must be supplied only after the label/value pair is verified."""

    label_id: int
    value_id: int
    label_text: str = "总资产"


class NativeFundsControls:
    """Win32 backend; only enumerates and reads existing controls."""

    def __init__(self, total_assets_spec: TotalAssetsControlSpec | None = None):
        self._app = None
        self._read_ids = {2388, 1012, 1686, 1013, 2005, 1016}
        if total_assets_spec is not None:
            self._read_ids.update((total_assets_spec.label_id, total_assets_spec.value_id))

    def main_handles(self, adapter) -> tuple[int, ...]:
        import win32gui
        import win32process

        process = adapter.app.process
        found = []

        def visit(hwnd, _):
            if win32gui.IsWindowVisible(hwnd) and win32gui.GetWindowText(hwnd) == MAIN_TITLE:
                _, pid = win32process.GetWindowThreadProcessId(hwnd)
                if pid == process:
                    found.append(hwnd)

        win32gui.EnumWindows(visit, None)
        return tuple(found)

    def connect(self, hwnd: int):
        from pywinauto import Application
        import win32process

        self._app = Application(backend="win32").connect(handle=hwnd)
        _, pid = win32process.GetWindowThreadProcessId(hwnd)
        if self._app.process != pid:
            raise FundsSamplingUnverified("main_window_process_changed")
        return self._app.window(handle=hwnd)

    def selected_page(self, main) -> str:
        import win32gui

        handles = []
        def visit(hwnd, _):
            if (win32gui.GetDlgCtrlID(hwnd) == 129
                    and win32gui.GetClassName(hwnd) == "SysTreeView32"
                    and win32gui.IsWindowVisible(hwnd)):
                handles.append(hwnd)
        win32gui.EnumChildWindows(main.handle, visit, None)
        if len(handles) != 1:
            return "unknown"
        tree = self._app.window(handle=handles[0]).wrapper_object()
        selected = []

        def walk(nodes):
            for node in nodes:
                if node.is_selected():
                    selected.append(node.text())
                walk(node.children())

        walk(tree.roots())
        return selected[0] if len(selected) == 1 else "unknown"

    def controls(self, hwnd: int) -> list[dict[str, object]]:
        import win32gui

        records = []

        def visit(child, _):
            if not win32gui.IsWindow(child) or not win32gui.IsWindowVisible(child):
                return
            control_id = win32gui.GetDlgCtrlID(child)
            class_name = win32gui.GetClassName(child)
            # Read only the explicitly mapped funds Static pairs. In particular,
            # never fetch text from an Edit, account ComboBox or unrelated label.
            if class_name != "Static" or control_id not in self._read_ids:
                return
            records.append({
                "control_id": control_id,
                "class_name": class_name,
                "rectangle": list(win32gui.GetWindowRect(child)),
                "text": win32gui.GetWindowText(child).strip(),
                "visible": bool(win32gui.IsWindowVisible(child)),
                "main_hwnd": hwnd,
            })

        win32gui.EnumChildWindows(hwnd, visit, None)
        return records


def _total_assets(records: Sequence[Mapping[str, object]], hwnd: int,
                  spec: TotalAssetsControlSpec | None) -> str | None:
    if (spec is None or spec.label_id == spec.value_id
            or spec.label_text not in {"总资产", "总 资 产"}):
        return None
    matched = {spec.label_id: [], spec.value_id: []}
    for record in records:
        control_id = record.get("control_id")
        if control_id in matched:
            matched[control_id].append(record)
    if any(len(items) != 1 for items in matched.values()):
        return None
    label, value = matched[spec.label_id][0], matched[spec.value_id][0]
    if any(row.get("main_hwnd") != hwnd or row.get("class_name") != "Static"
           or row.get("visible") is not True for row in (label, value)):
        return None
    if label.get("text") != spec.label_text:
        return None
    a, b = label.get("rectangle"), value.get("rectangle")
    if (not isinstance(a, (tuple, list)) or not isinstance(b, (tuple, list))
            or len(a) != 4 or len(b) != 4
            or any(type(n) is not int for n in (*a, *b))):
        return None
    lx1, ly1, lx2, ly2 = a
    vx1, vy1, vx2, vy2 = b
    overlap = min(ly2, vy2) - max(ly1, vy1)
    if (lx1 >= lx2 or ly1 >= ly2 or vx1 >= vx2 or vy1 >= vy2
            or not 0 <= vx1 - lx2 <= 12 or overlap <= 0
            or overlap * 5 < min(ly2 - ly1, vy2 - vy1) * 4):
        return None
    raw = value.get("text")
    if not isinstance(raw, str) or AMOUNT.fullmatch(raw.strip()) is None:
        return None
    try:
        amount = Decimal(raw.strip().replace(",", ""))
    except InvalidOperation:
        return None
    return raw.strip() if amount.is_finite() and amount >= 0 else None


class FundsGuiProvider:
    """Produce one observation; the contract decides if it is publishable.

    ``refresh_provider`` must independently prove the current page generation.
    ``identity_provider`` must use an authenticated account record or supply a
    current independently verified binding proof and a trusted verifier that
    calls AccountBindingContract.assert_current on fresh GUI card observations.
    A masked label is insufficient. The verifier must not self-assert from
    the same unconfirmed screen; a protected record and user confirmation are
    external prerequisites.
    The caller holds the client lock around this call and publication decision.
    """

    def __init__(self, *, refresh_provider: Callable, identity_provider: Callable,
                 controls=None, total_assets_spec: TotalAssetsControlSpec | None = None,
                 prepare_sample: Callable | None = None,
                 now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        self.controls = controls if controls is not None else NativeFundsControls(total_assets_spec)
        self.refresh_provider = refresh_provider
        self.identity_provider = identity_provider
        self.total_assets_spec = total_assets_spec
        self.prepare_sample = prepare_sample
        self.now = now

    def __call__(self, adapter) -> FundsQueryObservation:
        try:
            handles = self.controls.main_handles(adapter)
            if len(handles) != 1 or type(handles[0]) is not int or handles[0] <= 0:
                raise FundsSamplingUnverified("main_window_not_unique")
            hwnd = handles[0]
            main = self.controls.connect(hwnd)  # Native backend uses Application.connect(handle=...).
            if self.controls.selected_page(main) != FUNDS_PAGE:
                raise FundsSamplingUnverified("funds_page_unverified")
            # A page switch and stable values did not refresh balance 1012 in
            # the observed client. Perform any explicit refresh once, before
            # acquiring the independent completion/identity evidence. Merely
            # sending F5 must never manufacture that evidence.
            if self.prepare_sample is not None:
                if self.prepare_sample(adapter, main, hwnd) is not True:
                    raise FundsSamplingUnverified("funds_prepare_unverified")
                if (self.controls.main_handles(adapter) != (hwnd,)
                        or self.controls.selected_page(main) != FUNDS_PAGE):
                    raise FundsSamplingUnverified("funds_prepare_context_changed")
            before = self.refresh_provider(main, hwnd)
            if (not isinstance(before, RefreshEvidence) or before.main_hwnd != hwnd
                    or before.page != FUNDS_PAGE or not isinstance(before.generation, str)
                    or not before.generation):
                raise FundsSamplingUnverified("refresh_unverified")
            identity = self.identity_provider(main, hwnd, before.generation)
            records = self.controls.controls(hwnd)
            total = _total_assets(records, hwnd, self.total_assets_spec)
            after = self.refresh_provider(main, hwnd)
            identity_after = self.identity_provider(main, hwnd, before.generation)
            if (after != before or tuple(self.controls.main_handles(adapter)) != (hwnd,)
                    or self.controls.selected_page(main) != FUNDS_PAGE):
                raise FundsSamplingUnverified("funds_sample_drifted")
            if not isinstance(identity, IdentityEvidence):
                raise FundsSamplingUnverified("identity_unverified")
            if (identity.main_hwnd != hwnd or identity.refresh_id != before.generation
                    or identity_after != identity):
                raise FundsSamplingUnverified("identity_sample_drifted")
            if (identity.source == "independent_verified_binding"
                    and (type(identity.binding_proof) is not BindingProof
                         or identity_after.binding_proof is not identity.binding_proof)):
                raise FundsSamplingUnverified("identity_sample_drifted")
            collected_at = self.now()
            if (not isinstance(collected_at, datetime) or collected_at.tzinfo is None
                    or collected_at.utcoffset() is None):
                raise FundsSamplingUnverified("sample_time_unverified")
            return FundsQueryObservation(
                records=records, main_hwnd=hwnd, page=FUNDS_PAGE,
                login_state=identity.login_state, collected_at=collected_at,
                business_account=identity.business_account,
                identity_kind=identity.kind, identity_source=identity.source,
                refresh_id=before.generation, controls_refresh_id=before.generation,
                total_value=total, total_value_refresh_id=(before.generation if total else None),
                total_value_source=("current_funds_page_control" if total else "unverified"),
                total_value_field_verified=total is not None,
                binding_proof=identity.binding_proof,
                binding_verifier=identity.binding_verifier,
            )
        except FundsSamplingUnverified:
            raise
        except Exception as exc:
            raise FundsSamplingUnverified("funds_control_sampling_failed") from exc
