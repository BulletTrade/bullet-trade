"""Restricted simulation GUI actions, bound to one profile and actor session.

`submit` begins at the first possible broker button. GuiRuntime has already
committed submit_unknown. Any ambiguous dialog or missing original contract
raises, leaving that marker unresolved for operator review.
"""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from ..runtime import BrokerAcceptance, BrokerRejection, GateProof
from .native.pointer_gate import PointerGate, PointerGateError


class NativeActionBlocked(RuntimeError):
    """Fixed diagnostic code without account, order, or dialog contents."""


_LOCAL_DENIALS = frozenset({
    "buy_funds_insufficient", "target_row_not_unique",
    "target_quantity_unverified", "target_quantity_insufficient",
    "sell_observation_invalid", "cancel_observation_incomplete",
    "cancel_date_mismatch",
    "cancel_multirow_unsupported", "cancel_contract_mismatch",
    "cancel_row_terms_mismatch", "cancel_row_quantity_unverified",
    "cancel_row_quantity_mismatch",
})


@dataclass(frozen=True)
class DialogResult:
    status: str
    contract_no: str | None = None
    reason: str | None = None


def _plain(value: str) -> str:
    return html.unescape(re.sub(r"<[^>]*>", "", value)).replace("\r\n", "\n")


def parse_order_confirmation(records, *, account_mask, side, code, price, quantity):
    """Return the one enabled Yes HWND only for an exact order confirmation."""
    if (side not in {"buy", "sell"} or re.fullmatch(r"[0-9]{6}", code or "") is None
            or not isinstance(price, Decimal) or not price.is_finite()
            or price <= 0 or type(quantity) is not int or quantity <= 0):
        raise NativeActionBlocked("confirmation_expected_order_invalid")
    visible = [r for r in records if r.get("visible") is True]
    title = [r for r in visible if r.get("id") == 1365 and r.get("class") == "Static"]
    detail = [r for r in visible if r.get("id") == 1040 and r.get("class") == "Static"]
    if (len(title) != 1 or len(detail) != 1
            or _plain(title[0].get("text", "")).strip() != "委托确认"):
        raise NativeActionBlocked("confirmation_shape_unverified")
    body = _plain(detail[0].get("text", ""))
    verb = "买入" if side == "buy" else "卖出"
    other = "卖出" if side == "buy" else "买入"
    labels = {"资金帐号", "证券代码", verb + "价格", verb + "数量"}
    if re.search(other + r"(?:价格|数量|委托)", body):
        raise NativeActionBlocked("confirmation_side_mismatch")
    if any(len(re.findall(re.escape(label) + r"\s*[：:]", body)) != 1
           for label in labels):
        raise NativeActionBlocked("confirmation_duplicate_or_missing_field")
    values = {}
    for line in body.splitlines():
        match = re.fullmatch(r"\s*([^：:]+)[：:]\s*(.*?)\s*", line)
        if match and match[1].strip() in labels:
            key = match[1].strip()
            if key in values:
                raise NativeActionBlocked("confirmation_duplicate_field")
            values[key] = match[2]
    if set(values) != labels:
        raise NativeActionBlocked("confirmation_fields_missing")
    if (values["资金帐号"] != account_mask
            or re.fullmatch(re.escape(code) + r"(?:\([^()\n]+\))?", values["证券代码"]) is None
            or values[verb + "数量"] != str(quantity)
            or "您是否确定以上" + verb + "委托？" not in body):
        raise NativeActionBlocked("confirmation_fields_mismatch")
    try:
        actual_price = Decimal(values[verb + "价格"])
    except InvalidOperation as exc:
        raise NativeActionBlocked("confirmation_price_mismatch") from exc
    if not actual_price.is_finite() or actual_price != price:
        raise NativeActionBlocked("confirmation_price_mismatch")
    buttons = [r for r in visible if r.get("id") == 6 and r.get("class") == "Button"]
    if (sum(r.get("id") == 6 for r in visible) != 1
            or len(buttons) != 1 or buttons[0].get("enabled") is not True
            or buttons[0].get("text") not in {"是(&Y)", "是(Y)", "是"}
            or type(buttons[0].get("hwnd")) is not int):
        raise NativeActionBlocked("confirmation_button_unverified")
    return buttons[0]["hwnd"]


def parse_order_receipt(records) -> DialogResult:
    """Only a literal broker contract in an explicit success is acceptance."""
    texts = [_plain(r["text"]) for r in records
             if r.get("visible") is True and r.get("class") == "Static"
             and isinstance(r.get("text"), str)]
    body = "\n".join(texts)
    numbers = re.findall(r"(?:合同编号|委托编号)\s*[：:]\s*([0-9A-Za-z]+)", body)
    reasons = {code for phrase, code in (
        ("不在交易时间", "outside_trading_hours"),
        ("非交易时间", "outside_trading_hours"),
        ("休市", "market_closed"), ("未登录", "not_logged_in"),
        ("资金不足", "insufficient_funds"), ("余额不足", "insufficient_funds"),
        ("可用不足", "insufficient_balance"),
        ("可用股份不足", "insufficient_holdings"),
        ("持仓不足", "insufficient_holdings")) if phrase in body}
    success = "委托已提交" in body or "委托成功" in body
    if success and not reasons and len(numbers) == 1:
        return DialogResult("submitted", contract_no=numbers[0])
    if reasons and not success and not numbers and len(reasons) == 1:
        return DialogResult("rejected", reason=next(iter(reasons)))
    return DialogResult("unknown")


def _digest(*parts) -> str:
    return hashlib.sha256("\0".join(map(str, parts)).encode("utf-8")).hexdigest()


def _target_row(rows: tuple[dict, ...], code: str, field: str, quantity: int):
    matches = [row for row in rows if row.get("证券代码") == code]
    if len(matches) != 1:
        raise NativeActionBlocked("target_row_not_unique")
    try:
        available = int(matches[0][field])
    except (KeyError, ValueError, TypeError) as exc:
        raise NativeActionBlocked("target_quantity_unverified") from exc
    if available < quantity:
        raise NativeActionBlocked("target_quantity_insufficient")
    return matches[0]


class NativeActions:
    """One request at a time; the actor owns locks and durable state."""

    def __init__(self, host, *, pointer_gate=None):
        self.host = host
        self._request_id = None
        self._session_id = None
        self._prepared = None
        self._preflight = None
        self._original_order = None
        self._prepare_failed = False
        self._order_baseline = None
        self.last_prepare_diagnostic = None
        self._form_wait_counts = None
        self._pointer_gate = pointer_gate

    def _pointer(self):
        if self._pointer_gate is None:
            self._pointer_gate = PointerGate()
        return self._pointer_gate

    def _guard_button(self, main, pid, button, *, parent=None, modal=False,
                      control_id, caption, current):
        try:
            self._pointer().button(main, pid, button, parent=parent, modal=modal,
                                   control_id=control_id, caption=caption,
                                   current=current)
        except PointerGateError as exc:
            raise NativeActionBlocked(str(exc)) from None
        except Exception:
            raise NativeActionBlocked("pointer_guard_unavailable") from None

    def _guard_grid(self, main, pid, grid, *, coords, current):
        try:
            self._pointer().grid(main, pid, grid, coords=coords, current=current)
        except PointerGateError as exc:
            raise NativeActionBlocked(str(exc)) from None
        except Exception:
            raise NativeActionBlocked("pointer_guard_unavailable") from None

    def _scope(self, request):
        if request.account != self.host.profile.account:
            raise NativeActionBlocked("request_account_mismatch")
        if request.kind not in {"limit_buy", "limit_sell", "cancel"}:
            raise NativeActionBlocked("request_kind_unsupported")
        if not self.host.profile.account_mask.startswith("模拟炒股"):
            raise NativeActionBlocked("simulation_profile_required")
        if request.trade_day != datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat():
            raise NativeActionBlocked("trade_day_unverified")
        if request.kind in {"limit_buy", "limit_sell"}:
            if (re.fullmatch(r"[0-9]{6}\.(?:XSHG|XSHE)",
                             request.params.get("security", "")) is None
                    or type(request.params.get("quantity")) is not int
                    or request.params["quantity"] <= 0):
                raise NativeActionBlocked("order_terms_unverified")
            try:
                price = Decimal(request.params["price"])
            except (KeyError, InvalidOperation, TypeError) as exc:
                raise NativeActionBlocked("order_price_unverified") from exc
            if not price.is_finite() or price <= 0:
                raise NativeActionBlocked("order_price_unverified")
        else:
            contract = request.params.get("broker_contract_no")
            original = self.host.store.get_by_contract(request.account,
                                                       request.trade_day, contract)
            if (original is None or getattr(original, "broker_contract_no", None) != contract
                    or getattr(original, "account", None) != request.account
                    or getattr(original, "trade_day", None) != request.trade_day
                    or getattr(original, "state", None) != "accepted"
                    or getattr(original, "kind", None) not in {"limit_buy", "limit_sell"}
                    or not isinstance(getattr(original, "origin", None), dict)
                    or any(original.origin.get(key) != request.origin.get(key)
                           for key in ("virtual_account_id", "subaccount_key"))):
                raise NativeActionBlocked("cancel_original_owner_unverified")
            self._original_order = original
        self.host._identity()
        main, _ = self.host.adapter.locate()
        return main, self.host.adapter.app.process

    @staticmethod
    def _write_deadline(request):
        """Recheck immediately before every possible broker write click."""
        if time.time() >= request.expires_at:
            raise NativeActionBlocked("request_expired_before_click")
        if request.trade_day != datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat():
            raise NativeActionBlocked("trade_day_changed_before_click")

    def _proof(self, request, phase, *, allowed, input_matches=False):
        main, pid = self.host.adapter.locate()[0], self.host.adapter.app.process
        session = f"{pid}:{main}"
        if self._session_id is not None and session != self._session_id:
            raise NativeActionBlocked("gui_session_changed")
        evidence_ref = "native:" + (phase + ":" if not allowed else "") + _digest(
            request.request_id, phase, session)
        return GateProof(request.request_id, request.account, request.trade_day,
                         session, evidence_ref,
                         time.monotonic(), allowed, True, allowed, allowed,
                         allowed, input_matches)

    def preflight(self, request):
        if self._request_id is not None:
            raise NativeActionBlocked("request_already_prepared")
        main, pid = self._scope(request)
        session = f"{pid}:{main}"
        self._session_id = session
        try:
            if request.kind == "limit_buy":
                funds = self.host._account_query().data
                required = Decimal(request.params["price"]) * request.params["quantity"]
                if Decimal(funds["available_cash"]) < required:
                    raise NativeActionBlocked("buy_funds_insufficient")
            elif request.kind == "limit_sell":
                observed = self.host.query_observed("positions", lambda: False)
                if observed.complete is not True:
                    raise NativeActionBlocked("sell_observation_invalid")
                _target_row(observed.raw_rows, request.params["security"][:6],
                            "可用余额", request.params["quantity"])
            else:
                self._query_single_cancel_row(request)
            self.host._identity()
        except NativeActionBlocked as exc:
            if str(exc) in _LOCAL_DENIALS:
                # Identity/session was already checked by _scope. No submit or
                # preparation occurred; allow GuiRuntime to abort locally.
                self._request_id = request.request_id
                self._preflight = False
                return self._proof(request, "preflight_denied:" + str(exc),
                                   allowed=False)
            self._session_id = None
            self._original_order = None
            raise
        except Exception:
            self._session_id = None
            self._original_order = None
            raise
        self._request_id = request.request_id
        self._preflight = True
        return self._proof(request, "preflight", allowed=True)

    def prepare(self, request):
        if request.request_id != self._request_id or self._preflight is not True:
            raise NativeActionBlocked("preflight_required")
        self._form_wait_counts = None
        try:
            if request.kind == "cancel":
                self._prepare_cancel(request)
            else:
                if getattr(self.host.profile, "exclusive_order_source", False) is True:
                    baseline = self.host.query_observed("orders", lambda: False)
                    if baseline.complete is not True:
                        raise NativeActionBlocked("order_baseline_incomplete")
                    self._order_baseline = tuple(dict(row) for row in baseline.raw_rows)
                self._prepare_order(request)
        except Exception as exc:
            # No submit button is touched in prepare. If there is no modal
            # dialog, return a failed readback proof so GuiRuntime invokes
            # abort_prepared and records local_aborted after safe cleanup.
            self.last_prepare_diagnostic = {"type": type(exc).__name__}
            if (isinstance(exc, NativeActionBlocked)
                    or type(exc).__name__ in {"DriverBlocked", "PopupProfileError", "FormUnverified"}):
                self.last_prepare_diagnostic["code"] = str(exc)
            diagnostic = getattr(self.host, "_last_query_diagnostic", None)
            if isinstance(diagnostic, dict):
                self.last_prepare_diagnostic["query"] = diagnostic
            if isinstance(self._form_wait_counts, dict):
                self.last_prepare_diagnostic["form_control_counts"] = self._form_wait_counts
            if hasattr(self.host, "state_dir"):
                self._save_evidence(request, "prepare_failure", self.last_prepare_diagnostic)
            main, _ = self.host.adapter.locate()
            if self._dialogs(main, self.host.adapter.app.process):
                raise
            self._prepare_failed = True

    def validate_readback(self, request):
        if request.request_id == self._request_id and self._prepare_failed:
            return self._proof(request, "prepare_failed", allowed=False)
        if request.request_id != self._request_id or self._prepared is None:
            raise NativeActionBlocked("preparation_required")
        if request.kind == "cancel":
            self._validate_cancel(request)
        else:
            self._validate_order(request)
        return self._proof(request, "readback", allowed=True, input_matches=True)

    def submit(self, request):
        if request.request_id != self._request_id or self._prepared is None:
            raise NativeActionBlocked("readback_required")
        self.validate_readback(request)
        try:
            return (self._submit_cancel(request) if request.kind == "cancel"
                    else self._submit_order(request))
        finally:
            self._request_id = self._session_id = self._prepared = self._preflight = self._original_order = None
            self._prepare_failed = False
            self._order_baseline = None

    def abort_prepared(self, request):
        if request.request_id != self._request_id:
            raise NativeActionBlocked("request_binding_changed")
        if self._prepared is not None and request.kind in {"limit_buy", "limit_sell"}:
            # Only clear the three verified fields, before any submit action.
            self._clear_order_fields()
        if self._prepared is not None and request.kind == "cancel":
            main = self._prepared["main"]
            if self._dialogs(main, self._prepared["pid"]):
                raise NativeActionBlocked("cancel_cleanup_dialog_unverified")
            self.host.adapter.select_page("holdings")
            self.host.adapter.verify_page("holdings")
        self._request_id = self._session_id = self._prepared = self._preflight = self._original_order = None
        self._prepare_failed = False
        self._order_baseline = None

    def _query_single_cancel_row(self, request):
        observed = self.host.query_observed("cancelable", lambda: False)
        if observed.complete is not True:
            raise NativeActionBlocked("cancel_observation_incomplete")
        rows = observed.raw_rows
        if len(rows) != 1:
            raise NativeActionBlocked("cancel_multirow_unsupported")
        row = rows[0]
        if row.get("合同编号") != request.params["broker_contract_no"]:
            raise NativeActionBlocked("cancel_contract_mismatch")
        self._check_cancel_date(row, observed.metadata.get("trade_day"))
        self._check_cancel_row(row)
        return row

    def _check_cancel_date(self, row, stated_day):
        from ..normalization import _timestamp
        if stated_day is not None and stated_day != self._original_order.trade_day:
            raise NativeActionBlocked("cancel_date_mismatch")
        try:
            if "委托时间" in row:
                timestamp = _timestamp(row, "委托时间", None)
                row_day = timestamp[:10] if timestamp is not None else None
            elif "委托日期" in row:
                raw_day = row["委托日期"]
                row_day = (datetime.strptime(raw_day, "%Y%m%d").date().isoformat()
                           if re.fullmatch(r"[0-9]{8}", raw_day) else
                           datetime.strptime(raw_day, "%Y-%m-%d").date().isoformat())
            else:
                row_day = None
        except (ValueError, TypeError) as exc:
            raise NativeActionBlocked("cancel_date_mismatch") from exc
        if row_day is not None and row_day != self._original_order.trade_day:
            raise NativeActionBlocked("cancel_date_mismatch")

    def _check_cancel_row(self, row, *, terminal=False):
        from ..normalization import _security
        original = self._original_order
        if original is None:
            raise NativeActionBlocked("cancel_original_owner_unverified")
        expected_side = "买入" if original.kind == "limit_buy" else "卖出"
        try:
            security = _security(row)
            price = Decimal(row["委托价格"])
            expected_price = Decimal(original.params["price"])
        except (KeyError, ValueError, InvalidOperation, TypeError) as exc:
            raise NativeActionBlocked("cancel_row_terms_mismatch") from exc
        if (security != original.params["security"]
                or row.get("操作") != expected_side
                or not price.is_finite() or price <= 0 or price != expected_price
                or (not terminal and row.get("备注") not in {"未成交", "已报", "部成"})):
            raise NativeActionBlocked("cancel_row_terms_mismatch")
        try:
            quantity = int(row["委托数量"])
            filled = int(row["成交数量"])
        except (KeyError, ValueError, TypeError) as exc:
            raise NativeActionBlocked("cancel_row_quantity_unverified") from exc
        if (quantity != original.params["quantity"] or filled < 0
                or (not terminal and filled >= quantity)):
            raise NativeActionBlocked("cancel_row_quantity_mismatch")

    def _dialogs(self, main, pid):
        import win32gui
        import win32process
        found = []
        def visit(hwnd, _):
            if (win32gui.IsWindowVisible(hwnd) and win32gui.GetClassName(hwnd) == "#32770"
                    and win32process.GetWindowThreadProcessId(hwnd)[1] == pid
                    and main in (win32gui.GetWindow(hwnd, 4), win32gui.GetAncestor(hwnd, 3))):
                records = []
                def child(control, __):
                    cls = win32gui.GetClassName(control)
                    if win32gui.IsWindowVisible(control) and cls in {"Static", "Button"}:
                        records.append({"hwnd": control,
                                        "id": win32gui.GetDlgCtrlID(control),
                                        "class": cls, "text": win32gui.GetWindowText(control),
                                        "visible": True,
                                        "enabled": bool(win32gui.IsWindowEnabled(control))})
                win32gui.EnumChildWindows(hwnd, child, None)
                found.append((hwnd, records))
        win32gui.EnumWindows(visit, None)
        return found

    def _save_evidence(self, request, phase, content) -> str:
        """Persist raw local evidence before deciding a broker result."""
        payload = {"request_id": request.request_id, "phase": phase,
                   "observed_at": time.time(), "content": content}
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":")).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        directory = self.host.state_dir / "driver-evidence"
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        path = directory / (digest + ".json")
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        if os.name != "nt":
            parent_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
        return digest

    def _close_known_receipt(self, main, pid, dialog):
        hwnd, records = dialog
        buttons = [r for r in records if r.get("class") == "Button"
                   and r.get("visible") is True and r.get("enabled") is True
                   and r.get("text") in {"确定", "确认", "OK"}]
        if len(buttons) != 1 or len([r for r in records if r.get("class") == "Button"
                                     and r.get("visible") is True and r.get("enabled") is True]) != 1:
            raise NativeActionBlocked("receipt_close_button_unverified")
        import win32gui
        if win32gui.GetParent(buttons[0]["hwnd"]) != hwnd:
            raise NativeActionBlocked("receipt_close_owner_unverified")
        close_button = self.host.adapter.app.window(
            handle=buttons[0]["hwnd"]).wrapper_object()
        self._guard_button(main, pid, buttons[0]["hwnd"], parent=hwnd,
                           modal=hwnd, control_id=buttons[0]["id"],
                           caption=buttons[0]["text"],
                           current=lambda: self._dialogs(main, pid) == [dialog])
        close_button.click_input()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if not self._dialogs(main, pid):
                return
            time.sleep(0.1)
        raise NativeActionBlocked("receipt_close_unverified")

    def _await_order_form(self, adapter, main: int, pid: int, side: str):
        """Read-only bounded wait for a selected form to become complete."""
        if side not in {"buy", "sell"}:
            raise ValueError("order_side_invalid")
        import win32process
        from .native.native_form_observer import NativeFormObserver
        from .native.order_form_controls import FormUnverified
        form_diagnostic = {}
        observer = NativeFormObserver(adapter=adapter, main_hwnd=main,
                                      client_pid=pid,
                                      account_mask=self.host.profile.account_mask,
                                      diagnostics=form_diagnostic)
        self._form_wait_counts = None
        expected_tree = "买入[F1]" if side == "buy" else "卖出[F2]"
        ready_deadline = time.monotonic() + 5
        required_ids = (1032, 1033, 1034, 3001, 1400, 1399, 1006)
        while True:
            # Read-only binding checks; host._identity would select holdings.
            if (adapter.locate_main() != main or adapter.app.process != pid
                    or win32process.GetWindowThreadProcessId(main)[1] != pid
                    or self._dialogs(main, pid)):
                raise NativeActionBlocked("order_form_context_unverified")
            selected_mask = adapter.app.window(handle=main).child_window(
                control_id=2322, class_name="ComboBox"
            ).wrapper_object().selected_text()
            selected_tree = adapter._visible_query_tree(main).get_item(
                [expected_tree]).is_selected()
            if (selected_mask != self.host.profile.account_mask
                    or selected_tree is not True or self._dialogs(main, pid)):
                raise NativeActionBlocked("order_form_context_unverified")
            try:
                model = observer.observe()  # Its two full frames remain mandatory.
                break
            except FormUnverified as exc:
                matches = form_diagnostic.get("matches")
                counts = ({cid: item.get("count") for cid, item in matches.items()}
                          if isinstance(matches, dict) else {})
                self._form_wait_counts = {cid: count for cid, count in counts.items()
                                          if type(cid) is int and type(count) is int}
                def one_expected_class(cid, expected_class):
                    item = matches.get(cid, {})
                    candidates = item.get("candidates")
                    return (isinstance(candidates, list)
                            and type(counts.get(cid)) is int
                            and len(candidates) == counts[cid]
                            and sum(type(row) is dict
                                    and row.get("class") == expected_class
                                    for row in candidates) == 1)
                all_absent = (str(exc) == "control_not_unique"
                              and form_diagnostic.get("reason") == "control_not_unique"
                              and set(counts) == set(required_ids) | {2322, 129}
                              and all(type(counts[cid]) is int and counts[cid] == 0
                                      for cid in required_ids)
                              and one_expected_class(2322, "ComboBox")
                              and one_expected_class(129, "SysTreeView32"))
                remaining = ready_deadline - time.monotonic()
                if not all_absent or remaining <= 0:
                    raise
                time.sleep(min(0.1, remaining))
        return observer, model

    def _prepare_order(self, request):
        import win32api
        import win32gui
        from .native.native_order_form_backend import Win32FormApi, _read_single_edit_line
        from .native.order_focus_input import write_order_edit_with_focus
        adapter = self.host.adapter
        main, _ = adapter.locate()
        pid = adapter.app.process
        side = "buy" if request.kind == "limit_buy" else "sell"
        tree = adapter._visible_query_tree(main)
        tree.get_item(["买入[F1]" if side == "buy" else "卖出[F2]"]).select()
        time.sleep(0.5)
        left, top, _, _ = win32gui.GetWindowRect(main)
        win32api.SetCursorPos((left + 200, top + 100))
        time.sleep(0.3)
        observer, model = self._await_order_form(adapter, main, pid, side)
        # Unrelated hidden completion windows persist in this process. They are
        # not part of the visible form binding. Input independently checks the
        # exact Edit focus and main foreground before and after every key.
        expected_tree = "买入[F1]" if side == "buy" else "卖出[F2]"
        if model.selected_tree != expected_tree or self._dialogs(main, pid):
            raise NativeActionBlocked("order_form_context_unverified")
        fields = {c.control_id: c.hwnd for c in model.controls if c.class_name == "Edit"}
        if set(fields) != {1032, 1033, 1034}:
            raise NativeActionBlocked("order_fields_unverified")
        api = Win32FormApi()
        send = lambda h, msg, wp, buffer=None: api.send_message_timeout(h, msg, wp, buffer, 2000)
        def current():
            try:
                return (observer.observe() == model
                        and adapter.app.window(handle=main).child_window(
                            control_id=2322, class_name="ComboBox"
                        ).wrapper_object().selected_text() == self.host.profile.account_mask
                        and not self._dialogs(main, pid))
            except Exception:
                return False
        def read():
            if not current():
                raise NativeActionBlocked("order_context_changed")
            return tuple(_read_single_edit_line(send, fields[cid])
                         for cid in (1032, 1033, 1034))
        original = read()
        if (re.fullmatch(r"[0-9]{0,6}", original[0]) is None
                or re.fullmatch(r"[0-9]*(?:\.[0-9]+)?", original[1]) is None
                or re.fullmatch(r"[0-9]*", original[2]) is None):
            raise NativeActionBlocked("order_original_fields_unverified")
        values = (request.params["security"][:6], request.params["price"],
                  str(request.params["quantity"]))
        def write(cid, value):
            write_order_edit_with_focus(fields[cid], main, model.form_hwnd, pid,
                                        value, is_current=current, diagnostic={})
        self._prepared = {"main": main, "pid": pid, "observer": observer,
                          "model": model, "fields": fields, "send": send,
                          "original": original, "values": values,
                          "side": side, "read": read, "write": write,
                          "current": current}
        # Clear price/quantity before code; a code change may trigger a popup
        # or overwrite later fields. Every write has its own context check.
        for cid in (1033, 1034):
            if original[(1032, 1033, 1034).index(cid)]:
                write(cid, "")
        if original[0] != values[0]:
            write(1032, values[0])
        time.sleep(1)
        # If the client changed form shape after code entry, stop before submit.
        if not current():
            raise NativeActionBlocked("order_code_popup_unverified")
        write(1033, values[1])
        write(1034, values[2])
        self._validate_order(request)

    def _validate_order(self, request):
        prepared = self._prepared
        if not prepared or not prepared["current"]():
            raise NativeActionBlocked("order_context_changed")
        values = prepared["read"]()
        expected = prepared["values"]
        try:
            price_equal = Decimal(values[1]) == Decimal(expected[1])
        except InvalidOperation:
            price_equal = False
        if values[0] != expected[0] or not price_equal or values[2] != expected[2]:
            raise NativeActionBlocked("independent_order_readback_mismatch")

    def _clear_order_fields(self):
        prepared = self._prepared
        if prepared is None or not prepared["current"]():
            raise NativeActionBlocked("order_cleanup_context_unverified")
        for cid, value in zip((1032, 1033, 1034), prepared["original"]):
            prepared["write"](cid, value)
        if prepared["read"]() != prepared["original"]:
            raise NativeActionBlocked("order_cleanup_readback_mismatch")

    def _submit_order(self, request):
        prepared = self._prepared
        adapter = self.host.adapter
        main, pid = prepared["main"], prepared["pid"]
        if not prepared["current"]() or self._dialogs(main, pid):
            raise NativeActionBlocked("order_submit_context_unverified")
        buttons = [c for c in prepared["model"].controls
                   if c.control_id == 1006 and c.class_name == "Button"]
        if len(buttons) != 1 or buttons[0].caption != ("买入" if prepared["side"] == "buy" else "卖出"):
            raise NativeActionBlocked("order_submit_button_unverified")
        # First possibly submitting action: runtime has committed unknown.
        self._write_deadline(request)
        submit_button = adapter.app.window(handle=buttons[0].hwnd).wrapper_object()
        self._guard_button(
            main, pid, buttons[0].hwnd, parent=buttons[0].parent_hwnd,
            control_id=1006, caption=buttons[0].caption,
            current=lambda: prepared["current"]() and not self._dialogs(main, pid))
        self._write_deadline(request)
        first_click_before_at = time.time()
        first_click_returned = False
        try:
            submit_button.click_input()
            first_click_returned = True
        finally:
            first_click_after_at = time.time()
            self._save_evidence(request, "first_submit_click_timing", {
                "before_at": first_click_before_at,
                "after_at": first_click_after_at,
                "click_returned": first_click_returned})
        dialog = self._wait_one_dialog(main, pid, 25)
        self._save_evidence(request, "first_order_dialog", dialog)
        receipt = parse_order_receipt(dialog[1])
        if receipt.status == "unknown":
            yes = parse_order_confirmation(dialog[1],
                                           account_mask=self.host.profile.account_mask,
                                           side=prepared["side"], code=prepared["values"][0],
                                           price=Decimal(prepared["values"][1]),
                                           quantity=int(prepared["values"][2]))
            import win32gui
            if win32gui.GetParent(yes) != dialog[0]:
                raise NativeActionBlocked("confirmation_owner_unverified")
            confirmation_ref = self._save_evidence(request, "exact_order_confirmation", dialog)
            self._write_deadline(request)
            yes_record = next(r for r in dialog[1] if r.get("hwnd") == yes)
            yes_button = adapter.app.window(handle=yes).wrapper_object()
            self._guard_button(
                main, pid, yes, parent=dialog[0], modal=dialog[0],
                control_id=6, caption=yes_record["text"],
                current=lambda: self._dialogs(main, pid) == [dialog])
            self._write_deadline(request)
            confirmed_at = time.time()  # Immediately before the financial click.
            confirmation_click_returned = False
            try:
                yes_button.click_input()
                confirmation_click_returned = True
            finally:
                confirmation_click_after_at = time.time()
                confirmation_click_timing_ref = self._save_evidence(
                    request, "confirmation_click_timing", {
                        "before_at": confirmed_at,
                        "after_at": confirmation_click_after_at,
                        "click_returned": confirmation_click_returned,
                        "confirmation_ref": confirmation_ref})
            try:
                dialog = self._wait_one_dialog(main, pid, 15, previous=dialog)
            except NativeActionBlocked as exc:
                if str(exc) != "broker_dialog_timeout_unknown" or self._dialogs(main, pid):
                    raise
                return self._correlate_confirmed_order(
                    request, confirmed_at, confirmation_ref,
                    confirmation_click_after_at, confirmation_click_timing_ref)
            self._save_evidence(request, "post_confirmation_dialog", dialog)
            receipt = parse_order_receipt(dialog[1])
        if receipt.status == "submitted" and receipt.contract_no:
            digest = self._save_evidence(request, "accepted_receipt", dialog)
            self._close_known_receipt(main, pid, dialog)
            return BrokerAcceptance(receipt.contract_no,
                                    "native-receipt:" + digest)
        if receipt.status == "rejected" and receipt.reason:
            digest = self._save_evidence(request, "rejected_receipt", dialog)
            self._close_known_receipt(main, pid, dialog)
            return BrokerRejection("native-rejection:" + digest)
        raise NativeActionBlocked("order_result_unknown")

    def _correlate_confirmed_order(self, request, submitted_at, confirmation_ref,
                                   confirmation_click_after_at, confirmation_click_timing_ref):
        """One exact confirmed action, one new contract, two full GUI tables.

        The configured account must be exclusively controlled by this actor.
        No receipt lookup ever repeats the submit or confirmation click.
        """
        from .native.order_correlation import correlate_new_order, OrderCorrelationError
        if (getattr(self.host.profile, "exclusive_order_source", False) is not True
                or self._order_baseline is None or not confirmation_ref):
            raise NativeActionBlocked("order_result_unknown")
        observed = self.host.query_observed("orders", lambda: False)
        evidence = {
            "before": self._order_baseline, "after": observed.raw_rows,
            "before_complete": True, "after_complete": observed.complete,
            "confirmation_ref": confirmation_ref,
            "confirmation_click_timing_ref": confirmation_click_timing_ref,
            "submitted_at": submitted_at,
            "confirmation_click_after_at": confirmation_click_after_at,
            "observed_at": observed.observed_at.isoformat(),
            "observed_at_epoch": observed.observed_at.timestamp()}
        try:
            contract = correlate_new_order(
                request, self._order_baseline, observed.raw_rows,
                before_complete=True, after_complete=observed.complete,
                confirmed_terms=True, submitted_at=submitted_at,
                observed_at=observed.observed_at.timestamp())
        except OrderCorrelationError as exc:
            evidence["failure_reason"] = str(exc)
            self._save_evidence(request, "confirmed_order_delta_unresolved", evidence)
            raise NativeActionBlocked("order_result_unknown:" + str(exc)) from exc
        evidence["contract_no"] = contract
        digest = self._save_evidence(request, "confirmed_order_delta", evidence)
        return BrokerAcceptance(contract, "native-order-diff:" + digest)

    def _wait_one_dialog(self, main, pid, seconds, *, previous=None):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            dialogs = self._dialogs(main, pid)
            if (len(dialogs) == 1
                    and (previous is None or (dialogs[0] != previous
                         and parse_order_receipt(dialogs[0][1]).status != "unknown"))):
                return dialogs[0]
            if len(dialogs) > 1:
                raise NativeActionBlocked("multiple_broker_dialogs")
            time.sleep(0.25)
        raise NativeActionBlocked("broker_dialog_timeout_unknown")

    def _prepare_cancel(self, request):
        adapter = self.host.adapter
        row = self._query_single_cancel_row(request)
        main, grid = adapter.locate()
        self._cancel_context(main, grid, adapter.app.process)
        self._prepared = {"main": main, "pid": adapter.app.process,
                          "contract": request.params["broker_contract_no"],
                          "row": dict(row), "grid": grid}

    def _cancel_context(self, main, grid, pid):
        from .native.query_context import inspect_query_context
        adapter = self.host.adapter
        if (adapter.app.process != pid or adapter.locate() != (main, grid)
                or self._dialogs(main, pid)):
            raise NativeActionBlocked("cancel_page_changed")
        context = inspect_query_context(adapter, "cancelable", main, grid)
        scroll = context.scroll
        if (context.page_verified is not True or context.filter_verified is not True
                or context.loading_finished is not True or scroll is None
                or scroll.minimum != 0 or scroll.maximum - scroll.minimum != 1
                or scroll.position != 0 or scroll.page <= 0
                or adapter.locate() != (main, grid) or self._dialogs(main, pid)):
            raise NativeActionBlocked("cancel_context_unverified")
        return context

    def _validate_cancel(self, request):
        prepared = self._prepared
        adapter = self.host.adapter
        if not prepared or prepared["contract"] != request.params["broker_contract_no"]:
            raise NativeActionBlocked("cancel_binding_changed")
        main, grid = adapter.locate()
        if main != prepared["main"] or grid != prepared["grid"]:
            raise NativeActionBlocked("cancel_page_changed")
        self._cancel_context(main, grid, prepared["pid"])

    def _submit_cancel(self, request):
        prepared = self._prepared
        adapter = self.host.adapter
        main, pid = prepared["main"], prepared["pid"]
        self._validate_cancel(request)
        fresh_row = self._query_single_cancel_row(request)
        if dict(fresh_row) != prepared["row"]:
            raise NativeActionBlocked("cancel_row_changed")
        self._cancel_context(main, prepared["grid"], pid)
        button = adapter.app.window(handle=main).child_window(
            control_id=1099, class_name="Button").wrapper_object()
        if not button.is_visible() or button.window_text() != "撤单(Del)":
            raise NativeActionBlocked("cancel_button_unverified")
        # The unique fresh full-table row is the sole possible target. This
        # client profile uses its first-row selector before the cancel button.
        # Both actions are after GuiRuntime's durable submit_unknown marker.
        self._write_deadline(request)
        grid_wrapper = adapter.app.window(handle=prepared["grid"]).wrapper_object()
        self._guard_grid(main, pid, prepared["grid"], coords=(7, 45),
                         current=lambda: bool(self._cancel_context(
                             main, prepared["grid"], pid)))
        self._write_deadline(request)
        grid_wrapper.click_input(coords=(7, 45))
        self._cancel_context(main, prepared["grid"], pid)
        # This client enables cancel only after the sole row is selected.
        # Reacquire the same button and use a distinct post-selection code:
        # this failure can no longer establish that no selection click occurred.
        selected_button = adapter.app.window(handle=main).child_window(
            control_id=1099, class_name="Button").wrapper_object()
        if (selected_button.handle != button.handle or not selected_button.is_visible()
                or not selected_button.is_enabled()
                or selected_button.window_text() != "撤单(Del)"):
            raise NativeActionBlocked("cancel_button_after_selection_unverified")
        self._write_deadline(request)
        self._guard_button(
            main, pid, selected_button.handle, control_id=1099,
            caption="撤单(Del)",
            current=lambda: bool(self._cancel_context(
                main, prepared["grid"], pid))
            and adapter.app.window(handle=main).child_window(
                control_id=1099, class_name="Button").wrapper_object().handle
            == selected_button.handle
            and selected_button.is_visible() and selected_button.is_enabled()
            and selected_button.window_text() == "撤单(Del)")
        self._write_deadline(request)
        selected_button.click_input()
        dialog = self._wait_one_dialog(main, pid, 20)
        plain = [_plain(r.get("text", "")).strip() for r in dialog[1]
                 if r.get("class") == "Static"]
        yes = [r for r in dialog[1] if r.get("class") == "Button"
               and r.get("id") == 6 and r.get("text") == "是(&Y)"
               and r.get("enabled") is True and r.get("visible") is True]
        if ("撤单确认" not in plain or len(yes) != 1
                or len([r for r in dialog[1] if r.get("class") == "Button"
                        and r.get("id") == 6 and r.get("enabled") is True]) != 1
                or self._dialogs(main, pid) != [dialog]):
            raise NativeActionBlocked("cancel_confirmation_unverified")
        import win32gui
        if win32gui.GetParent(yes[0]["hwnd"]) != dialog[0]:
            raise NativeActionBlocked("cancel_confirmation_owner_unverified")
        self._write_deadline(request)
        yes_button = adapter.app.window(handle=yes[0]["hwnd"]).wrapper_object()
        self._guard_button(
            main, pid, yes[0]["hwnd"], parent=dialog[0], modal=dialog[0],
            control_id=6, caption="是(&Y)",
            current=lambda: self._dialogs(main, pid) == [dialog])
        self._write_deadline(request)
        yes_button.click_input()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if dialog[0] not in [h for h, _ in self._dialogs(main, pid)]:
                break
            time.sleep(0.25)
        else:
            raise NativeActionBlocked("cancel_result_unknown")
        if self._dialogs(main, pid):
            raise NativeActionBlocked("cancel_result_unknown")
        # No acceptance from dialog disappearance. Require the exact original
        # contract in a new observed terminal order row.
        observed = self.host.query_observed("orders", lambda: False)
        if observed.complete is not True:
            raise NativeActionBlocked("cancel_result_unknown")
        matches = [row for row in observed.raw_rows
                   if row.get("合同编号") == prepared["contract"]]
        if (len(matches) != 1 or matches[0].get("备注") not in
                {"已撤", "已撤单", "全部撤单"}):
            raise NativeActionBlocked("cancel_result_unknown")
        self._check_cancel_date(matches[0], observed.metadata.get("trade_day"))
        self._check_cancel_row(matches[0], terminal=True)
        try:
            filled = int(matches[0]["成交数量"])
            canceled = int(matches[0]["撤消数量"])
        except (KeyError, ValueError, TypeError) as exc:
            raise NativeActionBlocked("cancel_terminal_quantity_unverified") from exc
        if (filled < 0 or canceled <= 0
                or filled + canceled != self._original_order.params["quantity"]):
            raise NativeActionBlocked("cancel_terminal_quantity_mismatch")
        return BrokerAcceptance(prepared["contract"],
                                "native-cancel:" + _digest(request.request_id,
                                                           prepared["contract"],
                                                           matches[0].get("备注")))
