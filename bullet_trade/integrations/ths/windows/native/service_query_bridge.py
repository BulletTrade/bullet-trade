"""Read-only simulation query bridge for ActorService.

The current clipboard parser cannot certify a whole page. A caller must supply
independent, same-request page/account/filter/pagination/loading evidence before
this bridge can return a publishable CollectedSnapshot.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
from pathlib import Path
from types import MappingProxyType
import time
from zoneinfo import ZoneInfo

from bullet_trade.integrations.ths.actor_service import CollectedSnapshot
from bullet_trade.integrations.ths.scheduler import YIELD
from bullet_trade.integrations.ths.normalization import normalize_rows, broker_data
from .funds_query_contract import assess_funds_observation
from .query_evidence_gate import QueryProof, assess_query
from .readonly_sampler import read_stable_clipboard, session_client_lock
from .readonly_solver import SolverWindowsAdapter, solve_with_retries
from .table_snapshot import TableSnapshot, parse_table


KIND_TO_TABLE = {"positions": "holdings", "orders": "orders",
                 "trades": "trades", "cancelable": "cancelable"}


class QueryUnverified(RuntimeError):
    """Private diagnostic is retained; exception text contains no account/rows/OTP."""

    def __init__(self, reason: str, raw_result: dict | None = None):
        super().__init__(reason)
        self.reason = reason
        self.raw_result = raw_result


def _data(kind: str, snapshot: TableSnapshot, account: str, collected_at: datetime,
          verified_security_map=None):
    day = collected_at.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
    data = normalize_rows(kind, list(snapshot.rows), day,
                          verified_security_map=verified_security_map)
    broker_data(kind, data)  # Validate standard fields, retain exact decimals in storage.
    return data


class ServiceQueryBridge:
    """Only the query side of a GUI driver; write methods remain elsewhere."""

    def __init__(self, *, candidate_provider, account: str, output_dir: Path,
                 adapter=None, client_exe=None, proof_provider=None, funds_provider=None,
                 verified_security_map=None,
                 solver=solve_with_retries,
                 monotonic=time.monotonic, sleep=time.sleep):
        normalize_rows("trades", [], "2026-01-01",
                       verified_security_map=verified_security_map)
        self.verified_security_map = (None if verified_security_map is None
                                      else MappingProxyType(dict(verified_security_map)))
        if adapter is None:
            adapter = SolverWindowsAdapter(account=account, client_exe=client_exe)
        if not isinstance(account, str) or not account or account != getattr(adapter, "account", None):
            raise ValueError("simulation_account_mismatch")
        self.adapter = adapter
        self.candidate_provider = candidate_provider
        self.account = account
        self.output_dir = Path(output_dir)
        self.proof_provider = proof_provider
        self.funds_provider = funds_provider
        self.last_funds_diagnostic = None
        self.solver = solver
        self.monotonic = monotonic
        self.sleep = sleep

    def _verify_table_page_identity(self, kind: str, raw: dict) -> None:
        if kind not in {"orders", "cancelable"}:
            return
        try:
            checker = getattr(self.adapter, "verify_table_page_identity", None)
            if not callable(checker) or checker(kind, raw["main_hwnd"], raw["grid_hwnd"]) is not True:
                raise QueryUnverified("query_page_controls_unverified", raw)
        except QueryUnverified:
            raise
        except Exception:
            raise QueryUnverified("query_page_controls_unverified", raw) from None

    def query(self, kind: str, should_yield):
        if kind == "account":
            return self._query_account(should_yield)
        if kind not in KIND_TO_TABLE:
            raise QueryUnverified("unsupported_query_kind")
        if should_yield():
            return YIELD  # no GUI call has started
        table_kind = KIND_TO_TABLE[kind]
        raw = None
        with session_client_lock():
            self.adapter.select_page(table_kind)
            # The solver owns captcha identity, bounded retry and cleanup.
            raw = self.solver(self.adapter, self.candidate_provider, table_kind,
                              self.output_dir / table_kind)
            if raw.get("status") != "client_accepted" or raw.get("client_accepted") is not True \
                    or raw.get("dialog_closed") is not True or raw.get("cleanup_error"):
                raise QueryUnverified("solver_did_not_accept_query", raw)
            try:
                collected_at = datetime.fromisoformat(raw["finished_at"].replace("Z", "+00:00"))
                if collected_at.tzinfo is None:
                    raise QueryUnverified("solver_collection_time_unverified", raw)
                # A successful solver result is still not a whole-page proof.
                seq = raw["clipboard_sequence_after"]
                before = raw["clipboard_sequence_submit_pre"]
                if not isinstance(seq, int) or not isinstance(before, int) or seq <= before:
                    raise QueryUnverified("fresh_clipboard_unverified", raw)
                content = read_stable_clipboard(self.adapter, seq)
                if content is None or hashlib.sha256(content.encode("utf-8")).hexdigest() != raw.get("clipboard_sha256"):
                    raise QueryUnverified("solver_clipboard_changed", raw)
                if not self.adapter.clipboard_from_client(raw["main_hwnd"]):
                    raise QueryUnverified("clipboard_attribution_unverified", raw)
                parsed = parse_table(table_kind, content, sequence_before=before,
                                     sequence_after=seq)
                if parsed.columns != tuple(raw.get("table_columns", ())) or len(parsed.rows) != raw.get("table_row_count"):
                    raise QueryUnverified("solver_table_changed", raw)
                # A selected tree node can precede the central grid switch.
                # Orders and cancelable may share identical headers, so neither
                # the tree nor copied headers alone prove which page was copied.
                if raw.get("query_page_identity") != "header_and_tree":
                    raise QueryUnverified("query_page_identity_unverified", raw)
                self.adapter.verify_page(table_kind)
                if self.adapter.locate() != (raw["main_hwnd"], raw["grid_hwnd"]):
                    raise QueryUnverified("query_identity_changed", raw)
                self._verify_table_page_identity(table_kind, raw)
                deadline = self.monotonic() + 3.0
                while True:
                    if self.adapter.captcha_dialog() is not None:
                        raise QueryUnverified("late_captcha_dialog", raw)
                    self._verify_table_page_identity(table_kind, raw)
                    remaining = deadline - self.monotonic()
                    if remaining <= 0:
                        break
                    self.sleep(min(0.1, remaining))
                if self.adapter.clipboard_sequence() != seq:
                    raise QueryUnverified("clipboard_changed_after_settle", raw)
                self.adapter.dialog_fingerprint(raw["main_hwnd"])
                self.adapter.verify_page(table_kind)
                if self.adapter.locate() != (raw["main_hwnd"], raw["grid_hwnd"]):
                    raise QueryUnverified("query_identity_changed", raw)
                self._verify_table_page_identity(table_kind, raw)
                if self.proof_provider is None:
                    raise QueryUnverified("whole_page_evidence_unavailable", raw)
                proof, certified = self.proof_provider(table_kind, self.adapter, parsed, raw)
                self._verify_table_page_identity(table_kind, raw)
                if not isinstance(proof, QueryProof) or not isinstance(certified, TableSnapshot) \
                        or proof.requested_account != self.account \
                        or proof.requested_kind != table_kind \
                        or (certified.kind, certified.columns, certified.rows,
                            certified.clipboard_sequence_before, certified.clipboard_sequence_after) != \
                           (parsed.kind, parsed.columns, parsed.rows,
                            parsed.clipboard_sequence_before, parsed.clipboard_sequence_after):
                    raise QueryUnverified("evidence_not_same_query", raw)
                evidence = assess_query(proof, certified, captcha_acceptance_observed=True,
                                        clipboard_from_client=True, clipboard_stable=True)
                if not evidence.query_complete:
                    raise QueryUnverified("whole_page_evidence_unverified", raw)
                data = _data(kind, certified, self.account, collected_at,
                             self.verified_security_map)
                self._verify_table_page_identity(table_kind, raw)
                return CollectedSnapshot(self.account, kind, data, collected_at, True,
                                         {"source": "simulation_gui_copy",
                                          "schema": "bullettrade_broker_v1",
                                          "clipboard_sha256": raw["clipboard_sha256"]})
            except QueryUnverified:
                raise
            except Exception as exc:
                raise QueryUnverified("query_postcheck_failed", raw) from exc

    def _query_account(self, should_yield):
        self.last_funds_diagnostic = None
        if should_yield():
            return YIELD
        if self.funds_provider is None:
            raise QueryUnverified("funds_provider_unavailable")
        with session_client_lock():
            try:
                # Provider owns fresh read-only sampling. The same client lock
                # covers observation and publication decision.
                observation = self.funds_provider(self.adapter)
                data, diagnostic = assess_funds_observation(observation, self.account)
                self.last_funds_diagnostic = diagnostic
                if data is None:
                    raise QueryUnverified("funds_evidence_unverified")
                broker_data("account", data)
                return CollectedSnapshot(
                    self.account, "account", data, observation.collected_at, True,
                    {"source": "simulation_gui_funds_controls",
                     "schema": "bullettrade_broker_v1"})
            except QueryUnverified:
                raise
            except Exception as exc:
                raise QueryUnverified("funds_query_failed") from exc
