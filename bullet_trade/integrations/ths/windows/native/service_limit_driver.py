"""Offline wiring for one restricted simulation limit-order GUI actor.

The caller owns the actor lock, fresh gate evidence, independent field reader,
and any possible submit action. This module never clicks a broker control.
"""
from __future__ import annotations

from datetime import datetime
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
import re
import time
from types import MappingProxyType
from typing import Callable, Mapping
from zoneinfo import ZoneInfo

from bullet_trade.integrations.ths.request_store import Request, RequestStore
from bullet_trade.integrations.ths.runtime import (
    BrokerAcceptance, BrokerRejection, GateProof,
)
from .order_form_controls import OrderFormControls, OrderPlan


class LimitDriverUnverified(RuntimeError):
    """Fixed diagnostic codes; never expose an account or form value."""


@dataclass(frozen=True)
class SecurityRule:
    tick: Decimal
    lot_size: int


def _fingerprint(request: Request) -> tuple:
    if not isinstance(request, Request):
        raise LimitDriverUnverified("request_unverified")
    try:
        params = request.params
        if not isinstance(params, dict) or set(params) != {"security", "price", "quantity"}:
            raise ValueError
        origin = request.origin
        if not isinstance(origin, dict) or any(not isinstance(k, str) or not isinstance(v, str)
                                               for k, v in origin.items()):
            raise ValueError
        return (request.request_id, request.account, request.trade_day,
                request.idempotency_key, request.kind, tuple(sorted(origin.items())),
                params["security"], params["price"], params["quantity"],
                request.expires_at)
    except (AttributeError, TypeError, ValueError, KeyError):
        raise LimitDriverUnverified("request_unverified") from None


class ServiceLimitDriver:
    """GuiDriver's limit-order methods plus delegated read-only query()."""

    def __init__(self, *, query_bridge, form: OrderFormControls, store: RequestStore,
                 account: str, security_rules: Mapping[str, SecurityRule],
                 proof_provider: Callable[[Request, str], GateProof],
                 independent_readback: Callable[[Request, OrderPlan], tuple[str, str, str]],
                 submit_action: Callable[[Request, OrderPlan], BrokerAcceptance | BrokerRejection] | None = None,
                 monotonic=time.monotonic, wall_time=time.time):
        if (not isinstance(account, str) or not account
                or getattr(query_bridge, "account", None) != account
                or not callable(getattr(query_bridge, "query", None))
                or not isinstance(form, OrderFormControls)
                or not isinstance(store, RequestStore)
                or not callable(proof_provider) or not callable(independent_readback)):
            raise LimitDriverUnverified("driver_configuration_unverified")
        if submit_action is not None and not callable(submit_action):
            raise LimitDriverUnverified("submit_action_unverified")
        if not isinstance(security_rules, Mapping) or not security_rules:
            raise LimitDriverUnverified("security_rules_unverified")
        rules = dict(security_rules)
        for code, rule in rules.items():
            if (not isinstance(code, str)
                    or re.fullmatch(r"[0-9]{6}\.(?:XSHG|XSHE)", code) is None
                    or not isinstance(rule, SecurityRule)
                    or not isinstance(rule.tick, Decimal) or not rule.tick.is_finite()
                    or rule.tick <= 0 or type(rule.lot_size) is not int or rule.lot_size <= 0):
                raise LimitDriverUnverified("security_rules_unverified")
        self.query_bridge = query_bridge
        self.form = form
        self.store = store
        self.account = account
        self.security_rules = MappingProxyType(rules)
        self.proof_provider = proof_provider
        self.independent_readback = independent_readback
        self.submit_action = submit_action
        self.monotonic = monotonic
        self.wall_time = wall_time
        self._binding: tuple | None = None
        self._plan: OrderPlan | None = None
        self._session: str | None = None
        self._captured = False
        self._prepared = False
        self._validated = False
        self._last_cleared_binding: tuple | None = None

    def query(self, kind: str, should_yield):
        return self.query_bridge.query(kind, should_yield)

    def _plan_for(self, request: Request) -> OrderPlan:
        fingerprint = _fingerprint(request)
        if request.account != self.account or request.kind not in {"limit_buy", "limit_sell"}:
            raise LimitDriverUnverified("request_scope_unverified")
        if request.trade_day != datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat():
            raise LimitDriverUnverified("trade_day_unverified")
        code, raw_price, quantity = fingerprint[6:9]
        if code not in self.security_rules:
            raise LimitDriverUnverified("security_not_allowed")
        if not isinstance(raw_price, str) or re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", raw_price) is None:
            raise LimitDriverUnverified("price_unverified")
        try:
            price = Decimal(raw_price)
        except InvalidOperation:
            raise LimitDriverUnverified("price_unverified") from None
        rule = self.security_rules[code]
        tick = rule.tick
        try:
            valid_price = price.is_finite() and price > 0 and price % tick == 0
        except (InvalidOperation, ArithmeticError):
            valid_price = False
        if not valid_price:
            raise LimitDriverUnverified("price_tick_unverified")
        if type(quantity) is not int or quantity <= 0 or quantity % rule.lot_size != 0:
            raise LimitDriverUnverified("quantity_unverified")
        return OrderPlan("buy" if request.kind == "limit_buy" else "sell",
                         code[:6], price, quantity, tick)

    def _same(self, request: Request) -> OrderPlan:
        if self._binding is None or _fingerprint(request) != self._binding:
            raise LimitDriverUnverified("request_binding_unverified")
        plan = self._plan_for(request)
        if plan != self._plan:
            raise LimitDriverUnverified("request_binding_unverified")
        return plan

    def _proof(self, request: Request, phase: str, *, require_input: bool) -> GateProof:
        sampled_after = self.monotonic()
        proof = self.proof_provider(request, phase)
        sampled_before = self.monotonic()
        if not isinstance(proof, GateProof):
            raise LimitDriverUnverified("gate_proof_unverified")
        if (proof.request_id != request.request_id or proof.account != request.account
                or proof.trade_day != request.trade_day or not proof.session_id
                or not proof.evidence_ref
                or (self._session is not None and proof.session_id != self._session)
                or type(proof.sampled_at) not in (int, float)
                or not sampled_after <= proof.sampled_at <= sampled_before
                or proof.allowed is not True or proof.account_verified is not True
                or proof.queries_complete is not True or proof.capacity_ready is not True
                or proof.context_valid is not True
                or (require_input and proof.input_matches is not True)):
            raise LimitDriverUnverified("gate_proof_unverified")
        return proof

    def _readback(self, request: Request, plan: OrderPlan) -> None:
        expected = (plan.code, format(plan.price, "f"), str(plan.quantity))
        observed = self.independent_readback(request, plan)
        if not isinstance(observed, tuple) or len(observed) != 3 or observed != expected:
            raise LimitDriverUnverified("independent_readback_unverified")

    def _restore_after_failure(self, code: str) -> None:
        try:
            if self._captured:
                self.form.safe_restore()
        except Exception:
            raise LimitDriverUnverified("safe_restore_unverified") from None
        self._clear_binding()
        raise LimitDriverUnverified(code) from None

    def _clear_binding(self) -> None:
        if self._binding is not None:
            self._last_cleared_binding = self._binding
        self._binding = self._plan = self._session = None
        self._captured = self._prepared = self._validated = False

    def abort_prepared(self, request: Request) -> None:
        """Caller-owned pre-submit abort, for example Runtime expiry after readback."""
        binding = _fingerprint(request)
        if self._binding is None:
            if binding == self._last_cleared_binding:
                return
            raise LimitDriverUnverified("request_binding_unverified")
        if binding != self._binding or request.account != self.account:
            raise LimitDriverUnverified("request_binding_unverified")
        if not self._captured:
            self._clear_binding()
            return
        try:
            self.form.safe_restore()
        except Exception:
            raise LimitDriverUnverified("safe_restore_unverified") from None
        self._clear_binding()

    def preflight(self, request: Request) -> GateProof:
        if self._binding is not None:
            raise LimitDriverUnverified("prepared_request_pending")
        plan = self._plan_for(request)
        proof = self._proof(request, "preflight", require_input=False)
        persisted = self.store.get(request.request_id)
        if (persisted is None or persisted.state != "preparing"
                or _fingerprint(request) != _fingerprint(persisted)):
            raise LimitDriverUnverified("request_binding_unverified")
        self._binding = _fingerprint(request)
        self._plan = plan
        self._session = proof.session_id
        return proof

    def prepare(self, request: Request) -> None:
        try:
            plan = self._same(request)
            if self._captured or self._prepared:
                raise LimitDriverUnverified("prepare_repeated")
            self.form.capture(plan.side)
            self._captured = True
            self.form.fill_without_submit(plan)
            self._same(request)
            self._prepared = True
        except Exception:
            self._restore_after_failure("prepare_unverified")

    def validate_readback(self, request: Request) -> GateProof:
        try:
            plan = self._same(request)
            if not self._prepared or self._validated:
                raise LimitDriverUnverified("prepare_required")
            proof = self._proof(request, "readback", require_input=True)
            self._same(request)
            self._readback(request, plan)
            self._same(request)
            self._validated = True
            return proof
        except Exception:
            self._restore_after_failure("readback_unverified")

    def submit(self, request: Request) -> BrokerAcceptance | BrokerRejection:
        try:
            plan = self._same(request)
            if not self._prepared or not self._validated:
                raise LimitDriverUnverified("readback_required")
            if self.submit_action is None:
                raise LimitDriverUnverified("submit_action_unavailable")
            persisted = self.store.get(request.request_id)
            if (persisted is None or persisted.state != "submit_unknown"
                    or _fingerprint(persisted) != self._binding):
                raise LimitDriverUnverified("submission_marker_unverified")
            self._proof(request, "submit", require_input=True)
            self._same(request)
            self._readback(request, plan)
            self._same(request)
            self._proof(request, "submit_final", require_input=True)
            self._same(request)
            persisted = self.store.get(request.request_id)
            if (persisted is None or persisted.state != "submit_unknown"
                    or _fingerprint(persisted) != self._binding):
                raise LimitDriverUnverified("submission_marker_unverified")
            self._same(request)
            if self.wall_time() >= request.expires_at:
                raise LimitDriverUnverified("request_expired_before_action")
            self._same(request)
            if _fingerprint(persisted) != self._binding:
                raise LimitDriverUnverified("submission_marker_unverified")
            action_request = replace(persisted, params=dict(persisted.params),
                                     origin=dict(persisted.origin))
        except Exception:
            self._restore_after_failure("submit_gate_unverified")
        # GuiRuntime has already durably marked submit_unknown at this boundary.
        try:
            result = self.submit_action(action_request, plan)
        finally:
            self._binding = self._plan = self._session = None
            self._captured = self._prepared = self._validated = False
        if isinstance(result, BrokerAcceptance):
            if (not isinstance(result.broker_contract_no, str) or not result.broker_contract_no
                    or result.broker_contract_no == request.request_id
                    or not isinstance(result.evidence_ref, str) or not result.evidence_ref):
                raise LimitDriverUnverified("broker_result_unverified")
        elif isinstance(result, BrokerRejection):
            if not isinstance(result.evidence_ref, str) or not result.evidence_ref:
                raise LimitDriverUnverified("broker_result_unverified")
        else:
            raise LimitDriverUnverified("broker_result_unverified")
        return result

    def cancel(self, request: Request):
        raise LimitDriverUnverified("cancel_unsupported")
