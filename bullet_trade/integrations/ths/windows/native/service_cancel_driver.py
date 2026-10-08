"""Offline exact-cancel GuiDriver for one simulation account.

All observations, GUI selection and possible submit actions are injected. This
module has no broker button implementation and creates no persistence layer.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
import math
import time
from typing import Callable, Protocol
from zoneinfo import ZoneInfo

from bullet_trade.integrations.ths.request_store import Request, RequestStore
from bullet_trade.integrations.ths.runtime import BrokerAcceptance, BrokerRejection, GateProof
from .cancel_target_gate import (CancelableOrder, CancelableOrdersSnapshot,
                                ExactCancelPermit, permit_exact_cancel)


class CancelDriverUnverified(RuntimeError):
    """Fixed diagnostic codes, without account or contract content."""


@dataclass(frozen=True)
class CancelSnapshotEvidence:
    snapshot: CancelableOrdersSnapshot
    request_id: str
    session_id: str
    sampled_at: float
    evidence_ref: str


class ExactSelectionBackend(Protocol):
    def capture(self, permit: ExactCancelPermit) -> None: ...
    def select_without_submit(self, permit: ExactCancelPermit) -> None: ...
    def read_selected_exact(self) -> CancelableOrder: ...
    def safe_restore(self) -> None: ...


def _fingerprint(request: Request) -> tuple:
    if not isinstance(request, Request):
        raise CancelDriverUnverified("request_unverified")
    try:
        if (request.kind != "cancel" or not isinstance(request.params, dict)
                or set(request.params) != {"broker_contract_no"}
                or not isinstance(request.origin, dict)):
            raise ValueError
        contract = request.params["broker_contract_no"]
        if (not isinstance(contract, str) or not contract or contract != contract.strip()
                or any(not isinstance(k, str) or not isinstance(v, str)
                       for k, v in request.origin.items())):
            raise ValueError
        return (request.request_id, request.account, request.trade_day,
                request.idempotency_key, request.kind, contract,
                tuple(sorted(request.origin.items())), request.expires_at)
    except (AttributeError, KeyError, TypeError, ValueError):
        raise CancelDriverUnverified("request_unverified") from None


class ServiceCancelDriver:
    """Restricted cancel implementation; Runtime owns the only submit marker."""

    def __init__(self, *, store: RequestStore, account: str,
                 proof_provider: Callable[[Request, str], GateProof],
                 snapshot_provider: Callable[[Request, str], CancelSnapshotEvidence],
                 selection: ExactSelectionBackend,
                 submit_action: Callable[[Request, ExactCancelPermit], BrokerAcceptance | BrokerRejection] | None = None,
                 monotonic=time.monotonic, wall_time=time.time):
        if (not isinstance(store, RequestStore) or not isinstance(account, str) or not account
                or not callable(proof_provider) or not callable(snapshot_provider)
                or any(not callable(getattr(selection, name, None)) for name in
                       ("capture", "select_without_submit", "read_selected_exact", "safe_restore"))
                or (submit_action is not None and not callable(submit_action))):
            raise CancelDriverUnverified("driver_configuration_unverified")
        self.store, self.account = store, account
        self.proof_provider, self.snapshot_provider = proof_provider, snapshot_provider
        self.selection, self.submit_action = selection, submit_action
        self.monotonic, self.wall_time = monotonic, wall_time
        self._binding: tuple | None = None
        self._owner: tuple | None = None
        self._session: str | None = None
        self._captured = False
        self._prepared = False
        self._validated = False
        self._last_cleared_binding: tuple | None = None

    def _clear(self) -> None:
        if self._binding is not None:
            self._last_cleared_binding = self._binding
        self._binding = self._owner = self._session = None
        self._captured = self._prepared = self._validated = False

    def _restore_failure(self, code: str) -> None:
        try:
            if self._captured:
                self.selection.safe_restore()
        except Exception:
            raise CancelDriverUnverified("safe_restore_unverified") from None
        self._clear()
        raise CancelDriverUnverified(code) from None

    def _owner_for(self, request: Request) -> tuple:
        fingerprint = _fingerprint(request)
        if request.account != self.account:
            raise CancelDriverUnverified("request_scope_unverified")
        if request.trade_day != datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat():
            raise CancelDriverUnverified("trade_day_unverified")
        original = self.store.get_by_contract(request.account, request.trade_day, fingerprint[5])
        if (original is None or original.state != "accepted"
                or original.kind not in ("limit_buy", "limit_sell")
                or original.broker_contract_no != fingerprint[5]
                or not isinstance(original.origin, dict)
                or any(original.origin.get(k) != request.origin.get(k)
                       for k in ("virtual_account_id", "subaccount_key"))
                or not isinstance(original.params, dict)
                or set(original.params) != {"security", "price", "quantity"}):
            raise CancelDriverUnverified("original_order_unverified")
        security, quantity = original.params["security"], original.params["quantity"]
        if (not isinstance(security, str) or not security
                or type(quantity) is not int or quantity <= 0):
            raise CancelDriverUnverified("original_order_unverified")
        return (original.request_id, original.broker_contract_no, security,
                "买入" if original.kind == "limit_buy" else "卖出", quantity,
                tuple(sorted(original.origin.items())))

    def _same(self, request: Request) -> tuple:
        if self._binding is None or _fingerprint(request) != self._binding:
            raise CancelDriverUnverified("request_binding_unverified")
        if self._owner_for(request) != self._owner:
            raise CancelDriverUnverified("original_order_changed")
        return self._owner

    def _proof(self, request: Request, phase: str, *, require_input: bool) -> GateProof:
        before = self.monotonic()
        proof = self.proof_provider(request, phase)
        after = self.monotonic()
        if (not isinstance(proof, GateProof)
                or proof.request_id != request.request_id or proof.account != request.account
                or proof.trade_day != request.trade_day or not proof.session_id
                or not proof.evidence_ref or (self._session is not None and proof.session_id != self._session)
                or type(proof.sampled_at) not in (int, float)
                or not before <= proof.sampled_at <= after
                or proof.allowed is not True or proof.account_verified is not True
                or proof.queries_complete is not True or proof.capacity_ready is not True
                or proof.context_valid is not True
                or (require_input and proof.input_matches is not True)):
            raise CancelDriverUnverified("gate_proof_unverified")
        return proof

    def _permit(self, request: Request, phase: str, owner: tuple,
                session_id: str) -> tuple[ExactCancelPermit, CancelableOrder]:
        before = self.monotonic()
        evidence = self.snapshot_provider(request, phase)
        after = self.monotonic()
        if (not isinstance(evidence, CancelSnapshotEvidence)
                or evidence.request_id != request.request_id
                or evidence.session_id != session_id or not evidence.evidence_ref
                or type(evidence.sampled_at) not in (int, float)
                or not math.isfinite(evidence.sampled_at)
                or not before <= evidence.sampled_at <= after):
            raise CancelDriverUnverified("snapshot_evidence_unverified")
        snapshot = evidence.snapshot
        try:
            permit = permit_exact_cancel(
                account=request.account, trade_day=request.trade_day,
                contract_no=owner[1], security=owner[2], side=owner[3],
                order_quantity=owner[4], snapshot=snapshot)
            row = next(row for row in snapshot.rows if row.contract_no == owner[1])
        except Exception:
            raise CancelDriverUnverified("cancel_target_unverified") from None
        return permit, row

    def _selected(self, expected: CancelableOrder) -> None:
        observed = self.selection.read_selected_exact()
        if not isinstance(observed, CancelableOrder) or observed != expected:
            raise CancelDriverUnverified("selected_target_unverified")

    def preflight(self, request: Request) -> GateProof:
        if self._binding is not None:
            raise CancelDriverUnverified("prepared_request_pending")
        binding = _fingerprint(request)
        owner = self._owner_for(request)
        proof = self._proof(request, "preflight", require_input=False)
        self._permit(request, "preflight", owner, proof.session_id)
        persisted = self.store.get(request.request_id)
        if (persisted is None or persisted.state != "preparing"
                or _fingerprint(persisted) != binding or _fingerprint(request) != binding
                or self._owner_for(request) != owner):
            raise CancelDriverUnverified("request_binding_unverified")
        self._binding, self._owner, self._session = binding, owner, proof.session_id
        return proof

    def prepare(self, request: Request) -> None:
        try:
            owner = self._same(request)
            if self._captured or self._prepared:
                raise CancelDriverUnverified("prepare_repeated")
            self._proof(request, "prepare", require_input=False)
            self._same(request)
            permit, _ = self._permit(request, "prepare", owner, self._session)
            self._same(request)
            self.selection.capture(permit)
            self._captured = True
            self.selection.select_without_submit(permit)
            self._same(request)
            self._prepared = True
        except Exception:
            self._restore_failure("prepare_unverified")

    def validate_readback(self, request: Request) -> GateProof:
        try:
            owner = self._same(request)
            if not self._prepared or self._validated:
                raise CancelDriverUnverified("prepare_required")
            proof = self._proof(request, "readback", require_input=True)
            self._same(request)
            _, row = self._permit(request, "readback", owner, self._session)
            self._same(request)
            self._selected(row)
            self._same(request)
            self._validated = True
            return proof
        except Exception:
            self._restore_failure("readback_unverified")

    def submit(self, request: Request) -> BrokerAcceptance | BrokerRejection:
        try:
            owner = self._same(request)
            if not self._prepared or not self._validated or self.submit_action is None:
                raise CancelDriverUnverified("submit_unavailable")
            persisted = self.store.get(request.request_id)
            if (persisted is None or persisted.state != "submit_unknown"
                    or _fingerprint(persisted) != self._binding):
                raise CancelDriverUnverified("submission_marker_unverified")
            self._proof(request, "submit", require_input=True)
            self._same(request)
            _, row = self._permit(request, "submit", owner, self._session)
            self._same(request)
            self._selected(row)
            self._same(request)
            self._proof(request, "submit_final", require_input=True)
            self._same(request)
            final_permit, final_row = self._permit(request, "submit_final", owner, self._session)
            self._same(request)
            self._selected(final_row)
            self._same(request)
            self._proof(request, "submit_commit", require_input=True)
            self._same(request)
            persisted = self.store.get(request.request_id)
            if (persisted is None or persisted.state != "submit_unknown"
                    or _fingerprint(persisted) != self._binding):
                raise CancelDriverUnverified("submission_marker_unverified")
            if self.wall_time() >= request.expires_at:
                raise CancelDriverUnverified("request_expired_before_action")
            self._same(request)
            if _fingerprint(persisted) != self._binding:
                raise CancelDriverUnverified("submission_marker_unverified")
            action_request = replace(persisted, params=dict(persisted.params),
                                     origin=dict(persisted.origin))
        except Exception:
            self._restore_failure("submit_gate_unverified")
        try:
            result = self.submit_action(action_request, final_permit)
        finally:
            self._clear()
        if isinstance(result, BrokerAcceptance):
            if (result.broker_contract_no != owner[1]
                    or not isinstance(result.evidence_ref, str) or not result.evidence_ref):
                raise CancelDriverUnverified("broker_result_unverified")
        elif isinstance(result, BrokerRejection):
            if not isinstance(result.evidence_ref, str) or not result.evidence_ref:
                raise CancelDriverUnverified("broker_result_unverified")
        else:
            raise CancelDriverUnverified("broker_result_unverified")
        return result

    def abort_prepared(self, request: Request) -> None:
        binding = _fingerprint(request)
        if self._binding is None:
            if binding == self._last_cleared_binding:
                return
            raise CancelDriverUnverified("request_binding_unverified")
        if binding != self._binding or request.account != self.account:
            raise CancelDriverUnverified("request_binding_unverified")
        if not self._captured:
            self._clear()
            return
        try:
            self.selection.safe_restore()
        except Exception:
            raise CancelDriverUnverified("safe_restore_unverified") from None
        self._clear()
