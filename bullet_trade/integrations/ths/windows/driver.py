"""Guarded Windows GUI driver for an explicitly configured THS client."""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

from ..actor_service import CollectedSnapshot
from ..normalization import broker_data
from ..request_store import Request
from ..runtime import BrokerAcceptance, BrokerRejection, GateProof
from ..scheduler import YIELD


class DriverBlocked(RuntimeError):
    """Fixed diagnostic code; never includes account data or OCR content."""


def _safe_captcha_diagnostics(raw: dict, focus: object) -> dict:
    """Retain only bounded shape, count, and Win32 result scalars."""
    result = {}
    length = raw.get("edit_readback_length")
    if type(length) is int and 0 <= length <= 4096:
        result["edit_readback_length"] = length
    shape = raw.get("edit_readback_shape")
    if type(shape) is str and shape in {"empty", "four_digits", "other"}:
        result["edit_readback_shape"] = shape

    edit = raw.get("edit_diagnostic")
    if isinstance(edit, dict):
        safe_edit = {}
        for key in ("same_edit_hwnd", "native_wm_gettext_observed"):
            if type(edit.get(key)) is bool:
                safe_edit[key] = edit[key]
        count = edit.get("native_wm_gettext_length")
        if type(count) is int and 0 <= count <= 4096:
            safe_edit["native_wm_gettext_length"] = count
        if edit.get("status") == "unavailable":
            safe_edit["status"] = "unavailable"
        if safe_edit:
            result["edit_diagnostic"] = safe_edit

    if isinstance(focus, dict):
        safe_focus = {}
        for key in ("identity_ok", "attached", "foreground_requested",
                    "focus_matches_edit", "focus_matches_after", "detached",
                    "foreground_matches_dialog", "modifiers_clear",
                    "key_released"):
            if type(focus.get(key)) is bool:
                safe_focus[key] = focus[key]
        for key in ("initial_length", "sent_events"):
            value = focus.get(key)
            if type(value) is int and 0 <= value <= (8 if key == "sent_events" else 4096):
                safe_focus[key] = value
        for key in ("attach_error", "focus_query_error", "sendinput_error",
                    "detach_error", "text_length_error", "release_error"):
            value = focus.get(key)
            if type(value) is int and 0 <= value <= 0xFFFFFFFF:
                safe_focus[key] = value
        if safe_focus:
            result["focus_input_diagnostic"] = safe_focus
    return result


@dataclass(frozen=True)
class Profile:
    account: str
    expected_card: str
    account_mask: str
    ocr_template_dir: Path
    client_exe: str
    exclusive_order_source: bool = False

    @classmethod
    def load(cls, account: str, path: str | Path) -> "Profile":
        profile_path = Path(path)
        if not profile_path.is_file():
            raise DriverBlocked("profile_missing")
        try:
            raw = json.loads(profile_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError) as exc:
            raise DriverBlocked("profile_unreadable") from exc
        required = {
            "account", "expected_card", "account_mask", "ocr_template_dir",
            "client_exe"
        }
        if (not isinstance(raw, dict) or not required <= set(raw)
                or set(raw) - required - {"exclusive_order_source"}):
            raise DriverBlocked("profile_fields_unverified")
        exclusive = raw.get("exclusive_order_source", False)
        if type(exclusive) is not bool:
            raise DriverBlocked("profile_exclusive_source_unverified")
        if raw["account"] != account or not isinstance(account, str) or not account:
            raise DriverBlocked("profile_account_mismatch")
        card, mask, directory = (raw[k] for k in
                                 ("expected_card", "account_mask", "ocr_template_dir"))
        client_exe = raw["client_exe"]
        if (not isinstance(card, str) or re.fullmatch(r"[0-9]{8,32}", card) is None
                or not isinstance(mask, str) or not mask.startswith("模拟炒股")
                or mask != mask.strip()
                or not isinstance(directory, str)):
            raise DriverBlocked("profile_identity_unverified")
        template_dir = Path(directory)
        if not template_dir.is_absolute():
            template_dir = profile_path.parent / template_dir
        if not template_dir.is_dir():
            raise DriverBlocked("ocr_templates_missing")
        import ntpath
        if (not isinstance(client_exe, str) or not ntpath.isabs(client_exe)
                or ntpath.basename(client_exe).casefold() != "xiadan.exe"):
            raise DriverBlocked("client_exe_unverified")
        return cls(account, card, mask, template_dir, client_exe, exclusive)


@dataclass(frozen=True)
class ObservedQuery:
    account: str
    kind: str
    data: object
    observed_at: datetime
    complete: bool
    missing: tuple[str, ...]
    raw_rows: tuple[dict, ...] = ()
    metadata: dict = field(default_factory=dict)


class VerifiedBackend(Protocol):
    client_lock_factory: object

    def query(self, kind: str, should_yield) -> CollectedSnapshot | object: ...
    def preflight(self, request: Request) -> GateProof: ...
    def prepare(self, request: Request) -> None: ...
    def validate_readback(self, request: Request) -> GateProof: ...
    def submit(self, request: Request) -> BrokerAcceptance | BrokerRejection: ...
    def abort_prepared(self, request: Request) -> None: ...


class NativeBackend:
    """Native identity, funds and restricted simulation order actions.

    Tables are publishable only after independently checking the query page,
    filters, clipboard owner and unchanged native row range.
    """

    def __init__(self, profile: Profile, state_dir: Path):
        if os.name != "nt":
            raise DriverBlocked("windows_session_required")
        from .native.readonly_sampler import session_client_lock
        from .native.readonly_solver import SolverWindowsAdapter
        from ..request_store import RequestStore
        self.client_lock_factory = session_client_lock
        self.profile = profile
        self.state_dir = state_dir
        self.adapter = SolverWindowsAdapter(account=profile.account_mask,
                                            client_exe=profile.client_exe)
        self.store = RequestStore(state_dir / "requests.sqlite3")
        from .native.connection_notice import (ConnectionNoticeGate,
                                               Win32ConnectionNoticeProvider)
        self.connection_notice = ConnectionNoticeGate(
            Win32ConnectionNoticeProvider(self.adapter))
        from .native_actions import NativeActions
        self.actions = NativeActions(self)
        self.ready = True  # Restricted simulation actions; every call rechecks gates.

    def _clear_copy_captcha(self) -> bool:
        """Cancel only the detector-qualified copy prompt, never a broker dialog."""
        adapter = self.adapter
        adapter.locate_main()  # Recheck the configured account and process first.
        handle = adapter.captcha_dialog()
        if handle is None:
            return False
        if adapter.cancel_captcha(handle) is not True:
            raise DriverBlocked("copy_cleanup_unverified")
        time.sleep(3)
        if adapter.captcha_dialog() is not None:
            raise DriverBlocked("late_copy_dialog_unverified")
        adapter.dialog_fingerprint(adapter.locate_main())
        return True

    def _identity(self) -> str:
        """Read the separate funds-card control twice, restoring holdings."""
        import win32gui
        adapter = self.adapter
        main = adapter.locate_main()
        self._clear_copy_captcha()
        notice = getattr(self, "connection_notice", None)
        if notice is not None:
            from .native.connection_notice import ConnectionNoticeError
            try:
                if notice.close_once(main, adapter.app.process):
                    if adapter.locate_main() != main:
                        raise DriverBlocked("connection_notice_main_changed")
            except ConnectionNoticeError as exc:
                raise DriverBlocked(str(exc)) from None
            except DriverBlocked:
                raise
            except Exception:
                raise DriverBlocked("connection_notice_unverified") from None
        adapter.dialog_fingerprint(main)
        selected = adapter.app.window(handle=main).child_window(
            control_id=2322, class_name="ComboBox").wrapper_object().selected_text()
        if selected != self.profile.account_mask:
            raise DriverBlocked("account_mask_mismatch")
        tree = adapter._visible_query_tree(main)
        # This information page was qualified with native tree selection;
        # mouse hit-testing differs for scrolled nodes on a disconnected desktop.
        tree.get_item(["修改客户信息"]).select()

        def read_card():
            records = []
            def collect(hwnd, _):
                if win32gui.IsWindowVisible(hwnd) and win32gui.GetClassName(hwnd) == "Static":
                    records.append((win32gui.GetDlgCtrlID(hwnd), win32gui.GetWindowText(hwnd).strip()))
            win32gui.EnumChildWindows(main, collect, None)
            if not any(text.rstrip("：:") == "资金卡号" for _, text in records):
                return None
            cards = [text for cid, text in records
                     if cid == 1383 and re.fullmatch(r"[0-9]{8,32}", text)]
            return cards[0] if len(cards) == 1 else None

        try:
            deadline = time.monotonic() + 6
            first_seen_at = None
            saw_expected = False
            while time.monotonic() < deadline:
                card = read_card()
                if card is not None and card != self.profile.expected_card:
                    raise DriverBlocked("funds_card_mismatch")
                if card is None:
                    first_seen_at = None
                elif first_seen_at is None:
                    saw_expected = True
                    first_seen_at = time.monotonic()
                elif time.monotonic() - first_seen_at >= 0.3:
                    return card
                time.sleep(min(0.2, max(0, deadline - time.monotonic())))
            raise DriverBlocked("funds_card_unstable" if saw_expected
                                else "funds_card_not_visible")
        finally:
            tree.get_item(["查询[F4]", "资金股票"]).select()
            deadline = time.monotonic() + 6
            while True:
                try:
                    observed_main, _ = adapter.locate()
                    adapter.verify_page("holdings")
                    if observed_main != main:
                        raise DriverBlocked("funds_main_window_changed")
                    break
                except Exception:
                    if time.monotonic() >= deadline:
                        raise DriverBlocked("holdings_restore_unverified") from None
                    time.sleep(0.2)

    def query(self, kind, should_yield):
        if kind not in {"account", "positions", "orders", "trades", "cancelable"}:
            raise DriverBlocked("query_kind_unsupported")
        if should_yield():
            return YIELD
        if kind == "account":
            return self._account_query()
        observed = self.query_observed(kind, should_yield)
        if observed is YIELD:
            return YIELD
        if not observed.complete:
            raise DriverBlocked("table_evidence_unverified:" + ",".join(observed.missing))
        broker_data(kind, observed.data)
        return CollectedSnapshot(observed.account, kind, observed.data,
                                 observed.observed_at, True, observed.metadata)

    def query_observed(self, kind, should_yield) -> ObservedQuery | object:
        """Collect rows and the independent evidence from the same attempt."""
        from .native.ocr_candidate import FourDigitCandidateOCR
        from .native.readonly_solver import solve_with_retries
        from .native.readonly_sampler import read_stable_clipboard
        from .native.table_snapshot import parse_table
        from .native.template_ncc import NccTemplateEngine
        from .native.query_context import inspect_query_context
        from .native.table_coverage import assess_table_coverage
        table = {"positions": "holdings", "orders": "orders", "trades": "trades",
                 "cancelable": "cancelable"}.get(kind)
        if table is None:
            raise DriverBlocked("diagnostic_kind_unsupported")
        if should_yield():
            return YIELD
        self._identity()
        engine = NccTemplateEngine(self.profile.ocr_template_dir)
        if not engine.warmup():
            raise DriverBlocked("ocr_templates_unverified")
        self.adapter.select_page(table)
        main, grid = self.adapter.locate()
        window = self.adapter.app.window(handle=main)
        window.set_focus()
        window.type_keys("{F5}", set_foreground=False)
        time.sleep(1)
        before = inspect_query_context(self.adapter, table, main, grid)
        # A stable native page after explicit refresh is required. This is
        # observation freshness; it does not assert a server-side timestamp.
        for _ in range(2):
            time.sleep(0.3)
            next_frame = inspect_query_context(self.adapter, table, main, grid)
            if next_frame != before:
                raise DriverBlocked("query_context_unstable")
        if not (before.page_verified and before.filter_verified
                and before.loading_finished and before.scroll is not None):
            raise DriverBlocked("query_context_unverified")
        if should_yield():
            return YIELD  # No copy modal is outstanding at this safe boundary.
        raw = solve_with_retries(self.adapter, FourDigitCandidateOCR(engine), table,
                                 self.state_dir / "observed" / table,
                                 max_attempts=1, total_timeout=42, attempt_timeout=35,
                                 quiet_seconds=0.5)
        self._last_query_diagnostic = {"kind": kind, **{
            key: raw.get(key) for key in (
                "status", "reason", "error", "cleanup_error", "dialog_closed",
                "copy_sent", "attempt_count", "retry_stop_reason", "phase_seconds")},
            **_safe_captcha_diagnostics(
                raw, getattr(self.adapter, "_focus_input_diagnostic", None)
                if raw.get("reason") == "captcha_edit_mismatch" else None)}
        diagnostic_path = self.state_dir / "observed" / table / "last-result.json"
        diagnostic_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = diagnostic_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self._last_query_diagnostic, ensure_ascii=True),
                             encoding="utf-8")
        os.replace(temporary, diagnostic_path)
        residual_copy = self._clear_copy_captcha()
        if (raw.get("status") not in {"client_accepted", "new_table_no_captcha"}
                or raw.get("cleanup_error") or residual_copy):
            raise DriverBlocked("query_copy_unverified")
        sequence = raw.get("clipboard_sequence_after")
        if type(sequence) is not int:
            raise DriverBlocked("clipboard_sequence_unverified")
        content = read_stable_clipboard(self.adapter, sequence)
        if content is None:
            raise DriverBlocked("clipboard_content_unverified")
        if raw.get("query_page_identity") != "header_and_tree":
            raise DriverBlocked("query_page_identity_unverified")
        # Do not reselect a page: the context must remain the one just copied.
        after = inspect_query_context(self.adapter, table, main, grid)
        account_ok = self.adapter.locate_main() == main
        clipboard_owned = self.adapter.clipboard_from_client(main)
        try:
            parsed = parse_table(table, content,
                                 sequence_before=raw["clipboard_sequence_before"],
                                 sequence_after=sequence)
        except (KeyError, ValueError, TypeError) as exc:
            raise DriverBlocked("observed_rows_unverified") from exc
        coverage = assess_table_coverage(
            rows_count=len(parsed.rows), scroll_before=before.scroll,
            scroll_after=after.scroll, account=account_ok,
            page=before.page_verified and after.page_verified,
            filter=before.filter_verified and after.filter_verified,
            explicit_refresh=True,
            loading_finished=before.loading_finished and after.loading_finished,
            clipboard_owned=clipboard_owned)
        missing = list(coverage.missing)
        try:
            # A client can retain the previous trading day's rows over a
            # holiday. Never turn an HH:MM:SS value into today's timestamp.
            data = self._normalize_observed_rows(kind, list(parsed.rows))
            if kind in {"orders", "cancelable"}:
                from ..owned_order_scope import bind_owned_order_days
                data = bind_owned_order_days(
                    data, account=self.profile.account,
                    trade_day=datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat(),
                    lookup=self.store.get_by_contract)
            broker_data(kind, data)
        except (ValueError, TypeError):
            data = None
            missing.append("normalization_unverified")
        if self.adapter.clipboard_sequence() != sequence:
            raise DriverBlocked("clipboard_changed_after_copy")
        metadata = {"source": "simulation_gui_clipboard",
                    "scope": "client_current_query_page",
                    "refresh": "explicit_f5_stable_context",
                    "schema": "bullettrade_broker_v1",
                    "row_count": len(parsed.rows),
                    "coverage": "native_range_and_unfiltered_page"}
        # Date scope is optional; absent dates remain unknown rather than
        # being inferred from the local clock or a page caption.
        field_name = "成交日期" if kind == "trades" else "委托日期"
        dates = {row.get(field_name) for row in parsed.rows}
        if len(dates) == 1:
            explicit = next(iter(dates))
            if isinstance(explicit, str):
                try:
                    day = (datetime.strptime(explicit, "%Y%m%d").date()
                           if re.fullmatch(r"\d{8}", explicit)
                           else datetime.strptime(explicit, "%Y-%m-%d").date())
                    metadata["trade_day"] = day.isoformat()
                except ValueError:
                    pass
        return ObservedQuery(self.profile.account, kind, data,
                             datetime.now(timezone.utc), not missing,
                             tuple(missing), tuple(dict(row) for row in parsed.rows), metadata)

    def _normalize_observed_rows(self, kind: str, rows: list[dict]) -> list[dict]:
        """Keep explicit broker dates; attribute undated fills only to owned orders."""
        from ..normalization import normalize_rows

        if kind != "trades":
            return normalize_rows(kind, rows, None)
        try:
            return normalize_rows("trades", rows, None)
        except ValueError:
            from ..owned_trade_scope import normalize_owned_trades

            # The current local day is only a lookup scope. The helper assigns
            # a date solely from each exact durable accepted originating order.
            lookup_day = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
            return normalize_owned_trades(
                rows, account=self.profile.account, trade_day=lookup_day,
                lookup=self.store.get_by_contract)

    def _account_query(self) -> CollectedSnapshot:
        from .native.funds_gui_provider import (NativeFundsControls,
                                                TotalAssetsControlSpec)
        self._identity()
        adapter = self.adapter
        adapter.select_page("holdings")
        main_hwnd, _ = adapter.locate()
        adapter.verify_page("holdings")
        main = adapter.app.window(handle=main_hwnd)
        # This is the observed explicit funds refresh action. Capture two
        # stable cash controls after it; market-valued assets may update.
        # Neither sample comes from a clipboard.
        main.set_focus()
        main.type_keys("{F5}", set_foreground=False)
        time.sleep(1)
        spec = TotalAssetsControlSpec(1688, 1015, "总 资 产")
        controls = NativeFundsControls(spec)
        if controls.main_handles(adapter) != (main_hwnd,):
            raise DriverBlocked("funds_main_window_changed")
        first = controls.controls(main_hwnd)
        time.sleep(0.3)
        second = controls.controls(main_hwnd)
        if adapter.locate()[0] != main_hwnd:
            raise DriverBlocked("funds_controls_unstable")
        data = stable_funds_data(first, second, main_hwnd)
        broker_data("account", data)
        self._identity()
        return CollectedSnapshot(self.profile.account, "account", data,
                                 datetime.now(timezone.utc), True,
                                 {"source": "simulation_gui_funds_controls",
                                  "refresh": "explicit_f5_stable_controls",
                                  "total_value_consistency": "latest_validated_market_valuation",
                                  "schema": "bullettrade_broker_v1"})

    def preflight(self, request):
        return self.actions.preflight(request)

    def prepare(self, request):
        return self.actions.prepare(request)

    def validate_readback(self, request):
        return self.actions.validate_readback(request)

    def submit(self, request):
        return self.actions.submit(request)

    def abort_prepared(self, request):
        return self.actions.abort_prepared(request)


class WindowsDriver:
    """Scope-check a verified backend before the actor can use it."""

    def __init__(self, profile: Profile, state_dir: str | Path,
                 *, backend: VerifiedBackend | None = None):
        self.profile = profile
        self.account = profile.account
        self.state_dir = Path(state_dir)
        self.backend = backend if backend is not None else NativeBackend(profile, self.state_dir)
        if not callable(getattr(self.backend, "client_lock_factory", None)):
            raise DriverBlocked("client_lock_unavailable")
        self.client_lock_factory = self.backend.client_lock_factory

    @property
    def ready(self) -> bool:
        """Pure status read; never probes the desktop or claims runtime load."""
        return getattr(self.backend, "ready", False) is True

    def _request(self, request: Request) -> None:
        if not isinstance(request, Request) or request.account != self.account:
            raise DriverBlocked("request_scope_mismatch")

    def query(self, kind, should_yield):
        result = self.backend.query(kind, should_yield)
        if result is YIELD:
            return result
        if (not isinstance(result, CollectedSnapshot) or result.account != self.account
                or result.kind != kind or result.complete is not True):
            raise DriverBlocked("query_evidence_unverified")
        return result

    def query_observed(self, kind, should_yield):
        diagnostic = getattr(self.backend, "query_observed", None)
        if not callable(diagnostic):
            raise DriverBlocked("observed_query_unavailable")
        result = diagnostic(kind, should_yield)
        if result is YIELD:
            return result
        if (not isinstance(result, ObservedQuery) or result.account != self.account
                or result.kind != kind or type(result.complete) is not bool
                or result.complete != (not result.missing)
                or (result.complete and result.data is None)):
            raise DriverBlocked("observed_query_scope_unverified")
        return result

    def preflight(self, request):
        self._request(request)
        return self.backend.preflight(request)

    def prepare(self, request):
        self._request(request)
        return self.backend.prepare(request)

    def validate_readback(self, request):
        self._request(request)
        return self.backend.validate_readback(request)

    def submit(self, request):
        self._request(request)
        result = self.backend.submit(request)
        if not isinstance(result, (BrokerAcceptance, BrokerRejection)):
            raise DriverBlocked("broker_result_unverified")
        return result

    def abort_prepared(self, request):
        self._request(request)
        return self.backend.abort_prepared(request)


class ComposedBackend:
    """Connect the independently verified query and mature limit/cancel gates.

    The callers must supply concrete proof, form, exact selection and submit
    providers. In particular, a copied grid without a page coverage proof
    cannot become a cancel snapshot. The service drivers check persisted
    request identity and the submit_unknown marker before their action hook.
    """

    def __init__(self, *, query_backend, limit_driver, cancel_driver,
                 client_lock_factory):
        from .native.service_limit_driver import ServiceLimitDriver
        from .native.service_cancel_driver import ServiceCancelDriver
        from .native.service_trading_driver import ServiceTradingDriver
        if (not callable(getattr(query_backend, "query", None))
                or not callable(client_lock_factory)
                or not isinstance(limit_driver, ServiceLimitDriver)
                or not isinstance(cancel_driver, ServiceCancelDriver)):
            raise DriverBlocked("verified_components_required")
        self.trading = ServiceTradingDriver(limit=limit_driver,
                                            cancel=cancel_driver)
        self.query_backend = query_backend
        self.client_lock_factory = client_lock_factory
        self.ready = True

    def query(self, kind, should_yield):
        return self.query_backend.query(kind, should_yield)

    def preflight(self, request):
        return self.trading.preflight(request)

    def prepare(self, request):
        return self.trading.prepare(request)

    def validate_readback(self, request):
        return self.trading.validate_readback(request)

    def submit(self, request):
        return self.trading.submit(request)

    def abort_prepared(self, request):
        return self.trading.abort_prepared(request)


def make_driver(*, account: str, state_dir: str | Path,
                backend: VerifiedBackend | None = None) -> WindowsDriver:
    path = os.environ.get("THS_GUI_PROFILE")
    if not path:
        raise DriverBlocked("profile_environment_missing")
    profile = Profile.load(account, path)
    return WindowsDriver(profile, state_dir, backend=backend)


def funds_data(records: list[dict], main_hwnd: int) -> dict[str, str]:
    """Validate the observed funds controls and total-assets label/value pair."""
    from .native.funds_control_reader import read_funds_controls
    from .native.funds_gui_provider import TotalAssetsControlSpec, _total_assets
    total = _total_assets(records, main_hwnd,
                          TotalAssetsControlSpec(1688, 1015, "总 资 产"))
    if total is None:
        raise DriverBlocked("total_assets_control_unverified")
    try:
        reading = read_funds_controls(records, main_hwnd=main_hwnd,
                                      page="资金股票", login_state="logged_in",
                                      account_verified=True, sample_current=True)
    except ValueError as exc:
        raise DriverBlocked("funds_controls_unverified") from exc
    if not reading.complete or Decimal(total.replace(",", "")) < Decimal(reading.balance):
        raise DriverBlocked("funds_relation_unverified")
    return {"cash_balance": reading.balance, "available_cash": reading.available,
            "frozen_cash": reading.frozen,
            "total_value": str(Decimal(total.replace(",", "")))}


def stable_funds_data(first: list[dict], second: list[dict],
                      main_hwnd: int) -> dict[str, str]:
    """Require stable cash and control identity while allowing market valuation.

    Both frames independently validate labels, geometry and balance relations.
    Only the total-assets value text may change between frames; its control
    identity and geometry must remain unchanged. Return the latest valuation.
    """
    funds_data(first, main_hwnd)
    data = funds_data(second, main_hwnd)

    def signature(records):
        return sorted(
            ({**record, "text": None} if record["control_id"] == 1015
             else dict(record) for record in records),
            key=lambda record: record["control_id"],
        )

    if signature(first) != signature(second):
        raise DriverBlocked("funds_controls_unstable")
    return data
