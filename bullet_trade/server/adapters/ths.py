"""THS broker adapter backed by the durable local service request protocol."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
import os
import time
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from ...integrations.ths.client import (
    RequestAmbiguous, RequestNotFound, ServiceUnavailable, SubmissionUncertain,
    ThsServiceClient,
)
from ...integrations.ths.normalization import broker_data
from . import register_adapter
from .base import AccountContext, AccountRouter, AdapterBundle
from ..config import ServerConfig


class ThsNotReadyError(RuntimeError):
    """The service cannot supply a qualified broker fact."""


class ThsUnsupportedError(RuntimeError):
    """The THS profile does not support the requested action."""


class ThsRequestPending(ThsNotReadyError):
    """A durable request exists, but no usable broker contract is known yet."""

    def __init__(self, state: str, *, request_id: Optional[str],
                 idempotency_key: str,
                 source_idempotency_key: Optional[str] = None,
                 trade_day: Optional[str] = None) -> None:
        self.state = state
        self.request_id = request_id
        self.idempotency_key = idempotency_key
        self.source_idempotency_key = source_idempotency_key
        self.trade_day = trade_day
        super().__init__(f"THS request {state}: request_id={request_id or 'unknown'} "
                         f"idempotency_key={source_idempotency_key or idempotency_key} "
                         f"durable_key={idempotency_key} trade_day={trade_day or 'unknown'}")


def declare_ths_capabilities(
    *, environment: str = "unknown", client_version: Optional[str] = None
) -> Dict:
    """Static profile, not a claim that a GUI session or trade gate is ready."""
    if environment not in {"unknown", "simulation", "brokerage"}:
        raise ValueError("THS environment 必须是 unknown、simulation 或 brokerage")
    return {
        "server_type": "ths", "environment": environment,
        "client_version": client_version, "profile_verified": False,
        "account_identity": "unverified", "interactive_session": "unverified",
        "queries": {name: "requires_current_snapshot" for name in
                    ("account", "positions", "orders", "trades", "order_status")},
        "orders": {"limit_buy": "requires_durable_service_and_gui_gate",
                   "limit_sell": "requires_durable_service_and_gui_gate",
                   "cancel_by_original_order_id": "requires_durable_service_and_gui_gate",
                   "market_order": "unsupported", "conditional_order": "unsupported"},
        "reason": "runtime_service_and_gui_facts_required",
    }


class ThsBrokerAdapter:
    """One parent account per loopback service; no GUI or automatic retry here."""

    supports_durable_resolution = True

    def __init__(self, account_router: AccountRouter, *,
                 transport: Optional[ThsServiceClient] = None,
                 wait_seconds: float = 2.0, request_ttl_seconds: float = 120.0) -> None:
        if not 0 <= wait_seconds <= 10 or not 0 < request_ttl_seconds <= 300:
            raise ValueError("THS wait/TTL out of bounds")
        self.account_router = account_router
        self._transport = transport
        self._wait_seconds = wait_seconds
        self._request_ttl_seconds = request_ttl_seconds

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def _client(self, account: AccountContext) -> ThsServiceClient:
        if (not isinstance(self._transport, ThsServiceClient)
                or self._transport.account != account.config.account_id):
            raise ThsNotReadyError("THS query unavailable: account-bound transport missing")
        return self._transport

    async def get_account_info(self, account: AccountContext) -> Dict:
        return {"dtype": "dict", "value": await self._read(account, "account")}

    async def get_positions(self, account: AccountContext) -> List[Dict]:
        return await self._read(account, "positions")

    async def list_orders(self, account: AccountContext,
                          filters: Optional[Dict] = None) -> List[Dict]:
        if self._has_query_filters(filters):
            raise ThsUnsupportedError("THS order filters unsupported")
        return await self._read(account, "orders")

    async def list_trades(self, account: AccountContext,
                          filters: Optional[Dict] = None) -> List[Dict]:
        if self._has_query_filters(filters):
            raise ThsUnsupportedError("THS trade filters unsupported")
        return await self._read(account, "trades")

    @staticmethod
    def _has_query_filters(payload: Optional[Dict]) -> bool:
        if not payload:
            return False
        if not isinstance(payload, dict):
            return True
        routing = {"account_key", "sub_account_id", "filters"}
        return (bool(set(payload) - routing)
                or ("filters" in payload and payload["filters"] not in (None, {})))

    async def _read(self, account: AccountContext, kind: str):
        client = self._client(account)
        try:
            return broker_data(kind, await asyncio.to_thread(client.current_data, kind))
        except (ServiceUnavailable, ValueError, TypeError) as exc:
            raise ThsNotReadyError(
                "THS query unavailable: snapshot is not current and complete"
            ) from exc

    async def get_order_status(self, account: AccountContext, order_id: str) -> Dict:
        if not isinstance(order_id, str) or not order_id or order_id != order_id.strip():
            raise ThsNotReadyError("THS order status unavailable: original contract required")
        orders = await self._read(account, "orders")
        matches = [row for row in orders if row.get("order_id") == order_id]
        if len(matches) != 1:
            raise ThsNotReadyError("THS order status unavailable: original contract not unique")
        return matches[0]

    def _origin_and_key(self, account: AccountContext, payload: Dict) -> tuple[dict, str]:
        key = payload.get("idempotency_key")
        if not isinstance(key, str) or not key or key != key.strip():
            raise ValueError("THS idempotency_key required")
        sub = payload.get("sub_account_id")
        if sub:
            if not isinstance(sub, str) or sub != sub.strip():
                raise ValueError("THS sub_account_id invalid")
            origin = {"virtual_account_id": sub}
            source = "sub:" + sub
        else:
            source = "parent:" + account.config.key
            origin = {"subaccount_key": source}
        for field in ("strategy_id", "client_order_id"):
            value = payload.get(field)
            if value is not None:
                if not isinstance(value, str) or not value or value != value.strip():
                    raise ValueError(f"THS {field} invalid")
                origin[field] = value
        # Fixed-length digest avoids delimiter collisions between source and caller key.
        scope = json.dumps([account.config.account_id, account.config.key,
                            sub or ""], ensure_ascii=False, separators=(",", ":"))
        prefix = hashlib.sha256(scope.encode("utf-8")).hexdigest()[:24]
        key_digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return origin, "ths:" + prefix + ":" + key_digest

    async def _submit(self, account: AccountContext, payload: Dict,
                      kind: str, params: dict) -> Dict:
        client = self._client(account)
        origin, key = self._origin_and_key(account, payload)
        today = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
        explicit_day = payload.get("trade_day")
        day = explicit_day or today
        body = {"account": client.account, "trade_day": day,
                "idempotency_key": key, "kind": kind, "params": params,
                "origin": origin, "expires_at": time.time() + self._request_ttl_seconds}
        try:
            if payload.get("request_id"):
                result = await asyncio.to_thread(client.request_status, payload["request_id"])
            else:
                result = await asyncio.to_thread(
                    client.request_status_by_key, explicit_day, key)
        except RequestNotFound:
            if payload.get("request_id"):
                raise ThsNotReadyError("THS explicit request_id not found; no new request created")
            if day != today:
                raise ThsNotReadyError("THS historical request not found; no new request created")
            try:
                result = await asyncio.to_thread(client.record_request, body)
            except SubmissionUncertain as exc:
                # A lost POST receipt can only be recovered with a read. Never POST again.
                try:
                    result = await asyncio.to_thread(client.request_status_by_key, day, key)
                except ServiceUnavailable:
                    raise ThsRequestPending("receipt_unknown", request_id=None,
                                            idempotency_key=key,
                                            source_idempotency_key=payload["idempotency_key"],
                                            trade_day=day) from exc
        except RequestAmbiguous as exc:
            raise ThsNotReadyError("THS request key ambiguous across trading days") from exc
        except SubmissionUncertain as exc:
            raise ThsRequestPending("status_unknown", request_id=None,
                                    idempotency_key=key,
                                    source_idempotency_key=payload["idempotency_key"],
                                    trade_day=day) from exc
        except ServiceUnavailable as exc:
            raise ThsNotReadyError("THS request service unavailable") from exc
        if (result.get("kind") != kind or result.get("params") != params
                or result.get("origin") != origin
                or result.get("idempotency_key") != key
                or result.get("account") != client.account
                or (explicit_day and result.get("trade_day") != explicit_day)):
            raise ValueError("THS idempotency_key reused with different request")
        request_id = result.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            raise ThsRequestPending("invalid_receipt", request_id=None,
                                    idempotency_key=key,
                                    source_idempotency_key=payload["idempotency_key"],
                                    trade_day=day)
        deadline = time.monotonic() + self._wait_seconds
        while result.get("state") in {"queued", "preparing"} and time.monotonic() < deadline:
            await asyncio.sleep(min(0.1, max(0, deadline - time.monotonic())))
            try:
                result = await asyncio.to_thread(client.request_status, request_id)
            except ServiceUnavailable as exc:
                raise ThsRequestPending("status_unknown", request_id=request_id,
                                        idempotency_key=key,
                                        source_idempotency_key=payload["idempotency_key"],
                                        trade_day=result.get("trade_day")) from exc
        state = result.get("state")
        contract = result.get("broker_contract_no")
        result_day = result.get("trade_day")
        if state == "accepted" and isinstance(contract, str) and contract:
            if kind == "cancel":
                outcome, row = await self._cancel_terminal(account, contract, result_day)
                if outcome == "cancelled":
                    return {"order_id": contract, "request_id": request_id,
                            "trade_day": result_day,
                            "status": row["status"], "submission_state": row["status"],
                            "value": True, "last_snapshot": row,
                            "idempotency_key": payload["idempotency_key"]}
                if outcome == "rejected":
                    return {"order_id": contract, "request_id": request_id,
                            "trade_day": result_day, "status": "rejected",
                            "submission_state": "rejected", "value": False,
                            "cancel_outcome": "rejected", "last_snapshot": row,
                            "idempotency_key": payload["idempotency_key"]}
                raise ThsRequestPending("cancel_received", request_id=request_id,
                                        idempotency_key=key,
                                        source_idempotency_key=payload["idempotency_key"],
                                        trade_day=result_day)
            return {"order_id": contract, "broker_contract_no": contract,
                    "request_id": request_id, "trade_day": result_day,
                    "status": "accepted",
                    "submission_state": "accepted", "idempotency_key": payload["idempotency_key"]}
        if state in {"rejected", "local_aborted", "expired"}:
            raise ThsNotReadyError(f"THS request {state}: request_id={request_id}")
        raise ThsRequestPending(str(state or "status_unknown"), request_id=request_id,
                                idempotency_key=key,
                                source_idempotency_key=payload["idempotency_key"],
                                trade_day=result_day)

    async def _cancel_terminal(self, account: AccountContext, contract: str,
                               trade_day: str) -> tuple[str, Optional[Dict]]:
        try:
            snapshot = await asyncio.to_thread(self._client(account).qualified_snapshot, "orders")
            metadata = snapshot.get("metadata") or {}
            if not isinstance(metadata, dict) or not isinstance(trade_day, str):
                return "unknown", None
            orders = broker_data("orders", snapshot["data"])
        except (ServiceUnavailable, ValueError, TypeError):
            return "unknown", None
        matches = [row for row in orders if row.get("order_id") == contract]
        if len(matches) != 1:
            return "unknown", None
        row = matches[0]
        metadata_day = metadata.get("trade_day")
        if (metadata_day is not None and metadata_day != trade_day
                or row.get("trade_day") not in (None, trade_day)):
            return "unknown", None
        from ...integrations.ths.owned_order_scope import _explicit_order_day
        try:
            explicit_day = _explicit_order_day(row)
        except ValueError:
            return "unknown", None
        if explicit_day is not None and explicit_day != trade_day:
            return "unknown", None
        scope_present = "trade_day_source" in row or "request_id" in row
        row_scoped = (row.get("trade_day") == trade_day
                      and row.get("trade_day_source") == "durable_accepted_request"
                      and isinstance(row.get("request_id"), str)
                      and bool(row["request_id"]))
        if (scope_present and not row_scoped) or not (metadata_day == trade_day or row_scoped):
            return "unknown", None
        status = str(row.get("status") or "").lower()
        if status in {
            "canceled", "cancelled", "partly_canceled", "partly_cancelled"
        }:
            return "cancelled", row
        if status in {"filled", "rejected", "failed", "error"}:
            return "rejected", row
        return "unknown", row

    @staticmethod
    def _order_request(payload: Dict) -> tuple[str, dict]:
        style = payload.get("style") or {"type": "limit"}
        if (payload.get("market") is True or payload.get("market_type")
                or not isinstance(style, dict)
                or str(style.get("type") or "limit").lower() != "limit"
                or style.get("market_type")):
            raise ThsUnsupportedError("THS market/conditional order unsupported")
        security = payload.get("security")
        side = payload.get("side")
        if not isinstance(security, str) or not security or security != security.strip():
            raise ValueError("THS security required")
        if not isinstance(side, str) or side.upper() not in {"BUY", "SELL"}:
            raise ValueError("THS BUY/SELL side required")
        amount = payload.get("amount", payload.get("volume"))
        if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
            raise ValueError("THS positive integer amount required")
        price = style.get("price", payload.get("price"))
        if price is None or price == "":
            raise ThsUnsupportedError("THS explicit limit price required")
        try:
            decimal_price = Decimal(str(price))
        except InvalidOperation as exc:
            raise ValueError("THS invalid limit price") from exc
        if not decimal_price.is_finite() or decimal_price <= 0:
            raise ValueError("THS positive finite limit price required")
        # Decimal normalization makes 10, 10.0 and 10.00 the same durable body.
        return "limit_" + side.lower(), {
            "security": security, "quantity": amount,
            "price": format(decimal_price.normalize(), "f"),
        }

    async def place_order(self, account: AccountContext, payload: Dict) -> Dict:
        self._client(account)
        kind, params = self._order_request(payload)
        return await self._submit(account, payload, kind, params)

    async def cancel_order(self, account: AccountContext, order_id: str) -> Dict:
        raise ThsUnsupportedError("THS cancellation requires an idempotency_key")

    async def cancel_order_request(self, account: AccountContext, payload: Dict) -> Dict:
        self._client(account)
        order_id = payload.get("order_id")
        if not isinstance(order_id, str) or not order_id or order_id != order_id.strip():
            raise ValueError("THS original broker contract required")
        return await self._submit(account, payload, "cancel",
                                  {"broker_contract_no": order_id})

    async def resolve_submission(self, account: AccountContext, payload: Dict) -> Dict:
        """Read durable request status only; never enqueue while resolving."""
        client = self._client(account)
        action = payload.get("write_action") or payload.get("action")
        original = payload.get("request_payload")
        key = payload.get("idempotency_key")
        if (action not in {"broker.place_order", "broker.cancel_order"}
                or not isinstance(original, dict) or not original
                or not isinstance(key, str) or not key or key != key.strip()
                or original.get("idempotency_key") not in (None, key)):
            raise ValueError("THS original action, payload and idempotency_key required")
        routed_sub = payload.get("sub_account_id")
        if routed_sub and original.get("sub_account_id") not in (None, routed_sub):
            raise ValueError("THS original sub-account identity mismatch")
        if routed_sub and "sub_account_id" not in original:
            original = {**original, "sub_account_id": routed_sub}
        if action == "broker.place_order":
            kind, params = self._order_request(original)
        else:
            contract = original.get("order_id")
            if not isinstance(contract, str) or not contract or contract != contract.strip():
                raise ValueError("THS original broker contract required")
            kind, params = "cancel", {"broker_contract_no": contract}
        original = {**original, "idempotency_key": key}
        origin, durable_key = self._origin_and_key(account, original)
        day = payload.get("trade_day") or original.get("trade_day")
        try:
            request_id = payload.get("request_id")
            if request_id:
                result = await asyncio.to_thread(client.request_status, request_id)
            else:
                result = await asyncio.to_thread(client.request_status_by_key, day, durable_key)
        except RequestNotFound:
            return self._resolution(action, key, "submit_unknown", None, None,
                                    reason="durable_request_not_found", trade_day=day)
        except RequestAmbiguous:
            return self._resolution(action, key, "reconciling", None, None,
                                    reason="request_key_ambiguous_across_days", trade_day=day)
        except ServiceUnavailable:
            return self._resolution(action, key, "reconciling", None, None,
                                    reason="durable_request_query_unavailable", trade_day=day)
        request_id = result.get("request_id")
        if (result.get("kind") != kind or result.get("params") != params
                or result.get("origin") != origin
                or result.get("idempotency_key") != durable_key
                or result.get("account") != client.account
                or (day is not None and result.get("trade_day") != day)
                or not isinstance(request_id, str) or not request_id):
            raise ValueError("THS durable request identity mismatch")
        state = result.get("state")
        contract = result.get("broker_contract_no")
        day = result.get("trade_day")
        if state == "accepted" and isinstance(contract, str) and contract:
            if kind == "cancel":
                if contract != params["broker_contract_no"]:
                    raise ValueError("THS cancel contract identity mismatch")
                outcome, row = await self._cancel_terminal(account, contract, day)
                if outcome == "unknown":
                    return self._resolution(action, key, "reconciling", request_id,
                                            contract, reason="cancel_not_confirmed",
                                            trade_day=day)
                if outcome == "rejected":
                    return self._resolution(action, key, "rejected", request_id,
                                            contract, reason="target_already_terminal",
                                            trade_day=day,
                                            resolved_result={"order_id": contract,
                                                             "status": row["status"],
                                                             "value": False,
                                                             "cancel_outcome": "rejected",
                                                             "last_snapshot": row})
                return self._resolution(action, key, "accepted", request_id,
                                        contract, trade_day=day, resolved_result={
                                            "order_id": contract, "status": row["status"],
                                            "value": True, "last_snapshot": row})
            return self._resolution(action, key, "accepted", request_id, contract,
                                    trade_day=day,
                                    resolved_result={
                                        "order_id": contract, "status": "accepted",
                                        "security": params["security"],
                                        "side": "BUY" if kind == "limit_buy" else "SELL",
                                        "amount": params["quantity"],
                                        "price": params["price"]})
        if state in {"rejected", "local_aborted", "expired"}:
            return self._resolution(action, key, "rejected", request_id, None,
                                    reason=str(state), trade_day=day)
        return self._resolution(action, key, "reconciling", request_id, None,
                                reason=str(state or "status_unknown"), trade_day=day)

    @staticmethod
    def _resolution(action: str, key: str, state: str,
                    request_id: Optional[str], contract: Optional[str],
                    *, reason: Optional[str] = None,
                    trade_day: Optional[str] = None,
                    resolved_result: Optional[Dict] = None) -> Dict:
        response = {"write_action": action, "idempotency_key": key,
                    "status": state, "submission_state": state,
                    "request_id": request_id,
                    "trade_day": trade_day,
                    "evidence": {"source": "ths_request_store"}}
        if contract is not None:
            response["order_id"] = contract
        if reason is not None:
            response["reason"] = reason
        if resolved_result is not None:
            response["resolved_result"] = resolved_result
        return response


def build_ths_bundle(config: ServerConfig, router: AccountRouter) -> AdapterBundle:
    if config.enable_data:
        raise ThsUnsupportedError("THS market data unsupported")
    if not config.enable_broker:
        return AdapterBundle(data_adapter=None, broker_adapter=None)
    url = os.environ.get("THS_SERVICE_URL", "")
    token = os.environ.get("THS_SERVICE_TOKEN", "")
    account_id = os.environ.get("THS_SERVICE_ACCOUNT", "")
    if not url or not token or not account_id:
        raise ThsNotReadyError("THS broker unavailable: THS_SERVICE_URL/TOKEN/ACCOUNT required")
    if any(ctx.config.account_id != account_id for ctx in router.list_accounts()):
        raise ThsNotReadyError("THS broker unavailable: parent account identity mismatch")
    try:
        client = ThsServiceClient(url, token, account=account_id)
    except ValueError as exc:
        raise ThsNotReadyError("THS broker unavailable: invalid loopback service config") from exc
    try:
        wait_seconds = float(os.environ.get("THS_WAIT_SECONDS", "2"))
        ttl_seconds = float(os.environ.get("THS_REQUEST_TTL_SECONDS", "120"))
        adapter = ThsBrokerAdapter(router, transport=client,
                                   wait_seconds=wait_seconds,
                                   request_ttl_seconds=ttl_seconds)
    except ValueError as exc:
        raise ThsNotReadyError("THS broker unavailable: invalid wait/TTL config") from exc
    return AdapterBundle(data_adapter=None, broker_adapter=adapter)


register_adapter("ths", build_ths_bundle)

__all__ = ["ThsBrokerAdapter", "ThsNotReadyError", "ThsRequestPending",
           "ThsUnsupportedError", "build_ths_bundle", "declare_ths_capabilities"]
