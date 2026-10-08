"""Single GUI actor boundary, independent of the configured client driver.

Driver.prepare MUST contain no action that might submit. Driver.submit starts
with the first possibly submitting GUI action. The runtime commits
submit_unknown before calling it. A timeout after this point stays unknown.
"""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from pathlib import Path
import time
from typing import Callable, ContextManager, Iterator, Optional, Protocol, Union

from filelock import FileLock, Timeout

from .request_store import Request, RequestError, RequestStore, UnresolvedSubmission


class ActorBusy(RuntimeError):
    pass


class DriverUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class GateProof:
    """Driver-attested fresh checks for this exact request and GUI session.

    The driver must verify account/session identity and all observations
    required by THIS operation: funds for a buy, an exact fresh sellable
    position for a sell, an exact eligible original contract for a cancel,
    plus exact input readback. ``queries_complete`` refers to this operation's
    declared observation set; it never certifies a global positions/orders
    snapshot. Partial public queries must remain explicitly incomplete.
    """

    request_id: str
    account: str
    trade_day: str
    session_id: str
    evidence_ref: str
    sampled_at: float  # time.monotonic(), captured during this actor operation
    allowed: bool
    account_verified: bool
    queries_complete: bool
    capacity_ready: bool
    context_valid: bool
    input_matches: bool


@dataclass(frozen=True)
class BrokerAcceptance:
    broker_contract_no: str
    evidence_ref: str


@dataclass(frozen=True)
class BrokerRejection:
    evidence_ref: str


class GuiDriver(Protocol):
    def preflight(self, request: Request) -> GateProof:
        """Fresh read-only pre-submit checks; no GUI submit action."""

    def prepare(self, request: Request) -> None:
        """Fill controls only; must not cause submission or confirmation."""

    def validate_readback(self, request: Request) -> GateProof:
        """Independently read exact GUI input and session after preparation."""

    def submit(self, request: Request) -> Union[BrokerAcceptance, BrokerRejection]:
        """First possible submit action onward; return explicit broker evidence."""


def _check_proof(proof: GateProof, request: Request, session_id: Optional[str] = None,
                 *, require_input: bool = False, started_at: float) -> bool:
    if not isinstance(proof, GateProof):
        raise RequestError("driver gate proof required")
    if (proof.request_id != request.request_id or proof.account != request.account
            or proof.trade_day != request.trade_day or not proof.session_id
            or not proof.evidence_ref or (session_id is not None and proof.session_id != session_id)
            or isinstance(proof.sampled_at, bool)
            or not isinstance(proof.sampled_at, (int, float))
            or not started_at <= proof.sampled_at <= time.monotonic()
            or time.monotonic() - proof.sampled_at > 60):
        raise RequestError("driver gate proof does not match request/session")
    fields = (proof.allowed, proof.account_verified, proof.queries_complete,
              proof.capacity_ready, proof.context_valid)
    return all(value is True for value in fields) and (
        not require_input or proof.input_matches is True)


class GuiRuntime:
    def __init__(self, store: RequestStore, driver: Optional[GuiDriver],
                 lock_path: str | Path, *,
                 client_lock_factory: Optional[Callable[[], ContextManager[None]]] = None):
        self.store = store
        self.driver = driver
        self.lock = FileLock(str(lock_path))
        if client_lock_factory is not None and not callable(client_lock_factory):
            raise TypeError("client_lock_factory must be callable")
        self.client_lock_factory = nullcontext if client_lock_factory is None else client_lock_factory

    @contextmanager
    def actor(self) -> Iterator[None]:
        """Use both locks for read refresh and write; join a shared client lock when supplied."""
        try:
            # Match the existing session-client-then-file lock order. Neither
            # query nor submit may start before both contexts are entered.
            with self.client_lock_factory():
                with self.lock.acquire(timeout=0):
                    yield
        except Timeout as exc:
            raise ActorBusy("another GUI actor owns the session") from exc

    def _abort_prepared(self, request: Request) -> None:
        """Optional synchronous cleanup, still under both actor locks.

        A cleanup failure keeps preparing/submit_unknown unresolved. This hook
        is never called after the first possible submit action has started.
        """
        cleanup = getattr(self.driver, "abort_prepared", None)
        if cleanup is None:
            return
        try:
            if not callable(cleanup) or cleanup(request) is not None:
                raise RequestError("driver preparation cleanup failed")
        except Exception:
            raise RequestError("driver preparation cleanup failed") from None

    def run_once(self) -> Optional[Request]:
        if self.driver is None:
            raise DriverUnavailable("verified THS GUI driver is not configured")
        with self.actor():
            if self.store.has_unresolved():
                raise UnresolvedSubmission("preparing/submit_unknown requires explicit reconciliation")
            request = self.store.next_queued()
            if request is None:
                return None
            request = self.store.mark_preparing(request.request_id)
            if request.state == "expired":
                return request

            # A crash in preparing is intentionally never resumed. A proven
            # pre-submit stop is local, not a broker rejection.
            started_at = time.monotonic()
            proof = self.driver.preflight(request)
            if not _check_proof(proof, request, started_at=started_at):
                self._abort_prepared(request)
                return self.store.mark_local_aborted(request.request_id, proof.evidence_ref)
            self.driver.prepare(request)
            readback = self.driver.validate_readback(request)
            if not _check_proof(readback, request, proof.session_id,
                                require_input=True, started_at=started_at):
                self._abort_prepared(request)
                return self.store.mark_local_aborted(request.request_id, readback.evidence_ref)
            if time.time() >= request.expires_at:
                self._abort_prepared(request)
                return self.store.mark_local_aborted(request.request_id,
                                                     "local:expired_before_submit")

            # FULL-synchronous commit is complete before first possible submit.
            self.store.mark_submit_unknown(request.request_id)
            if time.time() >= request.expires_at:
                # The marker is durable; do not undo it or replay this request.
                self._abort_prepared(request)
                return self.store.get(request.request_id)
            result = self.driver.submit(request)
            if isinstance(result, BrokerAcceptance):
                return self.store.mark_accepted(request.request_id,
                                                result.broker_contract_no,
                                                result.evidence_ref)
            if isinstance(result, BrokerRejection):
                return self.store.mark_rejected(request.request_id, result.evidence_ref)
            raise RequestError("driver returned no explicit broker result; request remains unknown")
