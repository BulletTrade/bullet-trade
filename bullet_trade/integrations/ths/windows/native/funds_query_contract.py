"""Offline contract for one current funds-page observation.

The caller supplies observations, never a cached balance or an inferred asset
value. This module does not sample the GUI or establish account identity.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import re
from typing import Callable, Mapping, Sequence

from .account_binding_contract import BindingProof
from .funds_control_reader import (AMOUNT, FundsControlUnverified,
                                  read_funds_controls)


@dataclass(frozen=True)
class FundsQueryObservation:
    records: Sequence[Mapping[str, object]]
    main_hwnd: int
    page: str
    login_state: str
    collected_at: datetime
    business_account: str | None
    identity_kind: str
    identity_source: str
    refresh_id: str
    controls_refresh_id: str
    total_value: str | None
    total_value_refresh_id: str | None
    total_value_source: str
    total_value_field_verified: bool
    zero_balance_confirmed: bool = False
    binding_proof: BindingProof | None = None
    binding_verifier: Callable[[BindingProof], None] | None = None


@dataclass(frozen=True)
class FundsQueryDiagnostic:
    """Local partial reading; never returned as a standard snapshot."""

    balance: str | None
    available: str | None
    frozen: str | None
    total_value: str | None
    missing: tuple[str, ...]


def _binding_matches(proof: object, account: str, hwnd: int, refresh_id: str) -> bool:
    """Check a supplied proof's shape and sample scope, not its issuance.

    The caller must first use AccountBindingContract.assert_current against the
    protected record and current GUI card observation; this function cannot
    establish that independent fact from a proof-shaped value alone.
    """
    if type(proof) is not BindingProof:
        return False
    context = proof.context
    return (proof.service_account_id == account
            and isinstance(proof.profile_id, str) and bool(proof.profile_id)
            and isinstance(proof.card_digest, str)
            and re.fullmatch(r"[0-9a-f]{64}", proof.card_digest) is not None
            and isinstance(proof.proof_token, str)
            and re.fullmatch(r"[0-9a-f]{32}", proof.proof_token) is not None
            and isinstance(context, tuple) and len(context) == 5
            and isinstance(context[0], str) and bool(context[0])
            and type(context[1]) is int and context[1] > 0
            and type(context[2]) is int and context[2] > 0
            and type(context[3]) is int and context[3] == hwnd
            and context[4] == refresh_id)


def _binding_current(observation: FundsQueryObservation, account: str) -> bool:
    proof = observation.binding_proof
    if not _binding_matches(proof, account, observation.main_hwnd,
                            observation.refresh_id):
        return False
    verifier = observation.binding_verifier
    if not callable(verifier):
        return False
    try:
        # The caller supplies a trusted verifier that invokes
        # AccountBindingContract.assert_current with a fresh GUI observation.
        verifier(proof)
    except Exception:
        return False
    return True


def assess_funds_observation(observation: FundsQueryObservation, account: str):
    """Return (publishable account data or None, local diagnostic).

    The producer must independently prove the business account, one refresh
    generation for controls and total assets, and the total-assets field.
    A masked UI label or a reused cache generation does not meet this contract.
    """
    if not isinstance(observation, FundsQueryObservation):
        return None, FundsQueryDiagnostic(None, None, None, None,
                                          ("funds_observation_unavailable",))
    account_ok = (isinstance(account, str) and bool(account)
                  and observation.business_account == account)
    binding_ok = (observation.identity_source == "independent_verified_binding"
                  and account_ok and _binding_current(observation, account))
    identity_ok = (observation.login_state == "logged_in"
                   and observation.identity_kind == "stable_business_account"
                   and account_ok
                   and (observation.identity_source == "authenticated_account_record"
                        or (observation.identity_source == "independent_verified_binding"
                            and binding_ok)))
    refresh_ok = (isinstance(observation.refresh_id, str)
                  and bool(observation.refresh_id)
                  and observation.controls_refresh_id == observation.refresh_id
                  and observation.total_value_refresh_id == observation.refresh_id
                  and isinstance(observation.collected_at, datetime)
                  and observation.collected_at.tzinfo is not None
                  and observation.collected_at.utcoffset() is not None)
    total_ok = (observation.total_value_field_verified is True
                and observation.total_value_source == "current_funds_page_control"
                and isinstance(observation.total_value, str)
                and AMOUNT.fullmatch(observation.total_value.strip()) is not None)
    total = None
    if total_ok:
        try:
            value = Decimal(observation.total_value.strip().replace(",", ""))
            if value.is_finite() and value >= 0:
                total = str(value)
        except InvalidOperation:
            pass
    missing = []
    if not identity_ok:
        missing.append("stable_business_account_unverified")
    if not refresh_ok:
        missing.append("same_refresh_unverified")
    if total is None:
        missing.append("total_assets_unverified")
    try:
        reading = read_funds_controls(
            observation.records, main_hwnd=observation.main_hwnd,
            page=observation.page, login_state=observation.login_state,
            account_verified=identity_ok, sample_current=refresh_ok,
            zero_balance_confirmed=observation.zero_balance_confirmed)
    except FundsControlUnverified as exc:
        missing.append(str(exc))
        return None, FundsQueryDiagnostic(None, None, None, total, tuple(missing))
    if not reading.complete:
        missing.append(reading.reason)
    if total is not None and Decimal(total) < Decimal(reading.balance):
        missing.append("total_assets_below_cash_balance")
    diagnostic = FundsQueryDiagnostic(reading.balance, reading.available,
                                      reading.frozen, total, tuple(missing))
    if missing:
        return None, diagnostic
    return {"cash_balance": reading.balance, "available_cash": reading.available,
            "frozen_cash": reading.frozen, "total_value": total}, diagnostic
