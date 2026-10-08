from datetime import date
import sqlite3
import time

import pytest

from bullet_trade.integrations.ths.request_store import (
    RequestConflict, RequestError, RequestStore, StoreBusy, UnresolvedSubmission,
)


_DEFAULT_ORIGIN = object()


def add(store, key="k", *, expires_at=None, kind="limit_buy", params=None,
        origin=_DEFAULT_ORIGIN):
    if origin is _DEFAULT_ORIGIN:
        origin = {"virtual_account_id": "v1"}
    return store.enqueue("sim-account", date.today().isoformat(), key, kind,
                         params or {"security": "600000", "quantity": 100, "price": "10.00"},
                         expires_at or time.time() + 60, origin=origin)


def test_reopen_idempotency_conflict_and_broker_identity(tmp_path):
    path = tmp_path / "requests.sqlite3"
    first_store = RequestStore(path)
    first = add(first_store)
    second_store = RequestStore(path)
    waiting = add(second_store, "waiting")
    assert add(second_store, expires_at=first.expires_at).request_id == first.request_id
    with pytest.raises(RequestConflict):
        add(second_store, params={"security": "600000", "quantity": 100, "price": "10.01"},
            expires_at=first.expires_at)
    preparing = second_store.mark_preparing(first.request_id)
    assert preparing.state == "preparing"
    assert RequestStore(path).has_unresolved()
    with pytest.raises(UnresolvedSubmission):
        add(second_store, "other")
    second_store.mark_submit_unknown(first.request_id)
    with pytest.raises(UnresolvedSubmission):
        RequestStore(path).mark_preparing(waiting.request_id)
    accepted = second_store.mark_accepted(first.request_id, "001234", "broker-order:001234")
    assert accepted.request_id != accepted.broker_contract_no
    assert accepted.broker_contract_no == "001234"
    assert second_store.get_by_contract(first.account, first.trade_day, "001234") == accepted
    assert second_store.get_by_contract(first.account, first.trade_day, first.request_id) is None
    assert RequestStore(path).get(first.request_id).state == "accepted"
    assert not RequestStore(path).has_unresolved()


def test_unknown_blocks_new_writes_and_does_not_requeue(tmp_path):
    path = tmp_path / "requests.sqlite3"
    store = RequestStore(path)
    first = add(store)
    store.mark_preparing(first.request_id)
    store.mark_submit_unknown(first.request_id)
    restarted = RequestStore(path)
    assert restarted.next_queued() is None
    with pytest.raises(UnresolvedSubmission):
        add(restarted, "new")
    assert restarted.get(first.request_id).state == "submit_unknown"


def test_expired_unknown_retains_key_and_blocks_gui_retry(tmp_path):
    path = tmp_path / "requests.sqlite3"
    store = RequestStore(path)
    first = add(store, expires_at=time.time() + 0.05)
    store.mark_preparing(first.request_id)
    store.mark_submit_unknown(first.request_id)
    restarted = RequestStore(path)
    assert restarted.next_queued(now=first.expires_at + 1) is None
    assert restarted.get(first.request_id).state == "submit_unknown"
    assert add(restarted, expires_at=first.expires_at).request_id == first.request_id
    with pytest.raises(UnresolvedSubmission):
        add(restarted, "new")
    with pytest.raises(RequestConflict):
        restarted.mark_preparing(first.request_id)


def test_local_stop_and_broker_rejection_have_distinct_transitions(tmp_path):
    store = RequestStore(tmp_path / "requests.sqlite3")
    before_submit = add(store, "before-submit")
    store.mark_preparing(before_submit.request_id)
    with pytest.raises(RequestConflict):
        store.mark_rejected(before_submit.request_id, "not-a-broker-result")
    stopped = store.mark_local_aborted(before_submit.request_id,
                                       "local:expired_before_submit")
    assert stopped.state == "local_aborted"
    assert stopped.evidence_ref == "local:expired_before_submit"
    assert [e.state for e in store.events(before_submit.request_id)] == [
        "queued", "preparing", "local_aborted"]
    assert not store.has_unresolved()
    assert add(store, "before-submit", expires_at=before_submit.expires_at) == stopped
    with pytest.raises(RequestConflict):
        store.mark_submit_unknown(before_submit.request_id)

    submitted = add(store, "submitted")
    store.mark_preparing(submitted.request_id)
    store.mark_submit_unknown(submitted.request_id)
    with pytest.raises(RequestConflict):
        store.mark_local_aborted(submitted.request_id, "local:late-expiry")
    rejected = store.mark_rejected(submitted.request_id, "broker:rejected")
    assert rejected.state == "rejected"
    assert rejected.evidence_ref == "broker:rejected"
    assert [e.state for e in store.events(submitted.request_id)] == [
        "queued", "preparing", "submit_unknown", "rejected"]


def test_expiry_capacity_and_cancel_exact_original_number(tmp_path):
    store = RequestStore(tmp_path / "requests.sqlite3", max_pending=1)
    expired = add(store, expires_at=time.time() - 1)
    assert expired.state == "expired"
    assert store.next_queued() is None
    assert store.get(expired.request_id).state == "expired"
    owner = add(store, "owner", origin={"virtual_account_id": "v1"})
    store.mark_preparing(owner.request_id)
    store.mark_submit_unknown(owner.request_id)
    store.mark_accepted(owner.request_id, "000123", "broker:000123")
    queued = add(store, "cancel", kind="cancel", params={"broker_contract_no": "000123"},
                 origin={"virtual_account_id": "v1"})
    assert queued.params["broker_contract_no"] == "000123"
    with pytest.raises(StoreBusy):
        add(store, "over-capacity")
    with pytest.raises(RequestError):
        add(store, "bad-cancel", kind="cancel", params={"broker_contract_no": " 000123"})
    with pytest.raises(RequestError):
        add(store, "float", params={"security": "600000", "quantity": 100, "price": 10.0})
    with pytest.raises(RequestConflict):
        add(store, "other-owner", kind="cancel", params={"broker_contract_no": "000123"},
            origin={"virtual_account_id": "virtual-b"})
    with pytest.raises(RequestConflict):
        add(store, "manual", kind="cancel", params={"broker_contract_no": "991122"},
            origin={"virtual_account_id": "v1"})


def test_expired_queued_request_releases_capacity_without_actor_step(tmp_path):
    path = tmp_path / "requests.sqlite3"
    store = RequestStore(path, max_pending=1)
    expired = add(store, "expired", expires_at=time.time() - 1)

    restarted = RequestStore(path, max_pending=1)
    fresh = add(restarted, "fresh")
    assert fresh.state == "queued"
    assert restarted.get(expired.request_id).state == "expired"
    assert restarted.next_queued().request_id == fresh.request_id


def test_origin_digest_and_cancel_receipt_can_reference_target(tmp_path):
    store = RequestStore(tmp_path / "requests.sqlite3")
    owner = add(store, "order", origin={"virtual_account_id": "v1", "strategy_id": "s1"})
    with pytest.raises(RequestConflict):
        add(store, "order", expires_at=owner.expires_at,
            origin={"virtual_account_id": "virtual-b", "strategy_id": "s1"})
    store.mark_preparing(owner.request_id)
    store.mark_submit_unknown(owner.request_id)
    store.mark_accepted(owner.request_id, "000123", "broker:000123")
    cancel = add(store, "cancel", kind="cancel", params={"broker_contract_no": "000123"},
                 origin={"virtual_account_id": "v1", "client_order_id": "cancel-1"})
    store.mark_preparing(cancel.request_id)
    store.mark_submit_unknown(cancel.request_id)
    assert store.mark_accepted(cancel.request_id, "000123", "cancel-receipt:1").state == "accepted"
    assert store.get_by_contract(owner.account, owner.trade_day, "000123").request_id == owner.request_id
    other = add(store, "other")
    store.mark_preparing(other.request_id)
    store.mark_submit_unknown(other.request_id)
    with pytest.raises(RequestConflict):
        store.mark_accepted(other.request_id, "000123", "different-order")
    assert store.get(other.request_id).state == "submit_unknown"
    assert [e.state for e in store.events(other.request_id)] == [
        "queued", "preparing", "submit_unknown"]


def test_cancel_unknown_survives_restart_without_losing_order_owner(tmp_path):
    path = tmp_path / "requests.sqlite3"
    store = RequestStore(path)
    order = add(store, "order")
    store.mark_preparing(order.request_id)
    store.mark_submit_unknown(order.request_id)
    store.mark_accepted(order.request_id, "000123", "broker:000123")
    cancel = add(store, "cancel", kind="cancel",
                 params={"broker_contract_no": "000123"})
    store.mark_preparing(cancel.request_id)
    store.mark_submit_unknown(cancel.request_id)

    restarted = RequestStore(path)
    assert restarted.has_unresolved()
    assert restarted.next_queued() is None
    assert restarted.get(cancel.request_id).params == {"broker_contract_no": "000123"}
    assert restarted.get_by_contract(order.account, order.trade_day, "000123").request_id == order.request_id
    assert restarted.mark_accepted(cancel.request_id, "000123", "cancel-receipt:1").state == "accepted"
    assert not restarted.has_unresolved()


def test_missing_virtual_owner_rejected_for_order_and_cancel(tmp_path):
    store = RequestStore(tmp_path / "requests.sqlite3")
    for kind, params in (
        ("limit_buy", {"security": "600000", "quantity": 100, "price": "10.00"}),
        ("limit_sell", {"security": "600000", "quantity": 100, "price": "10.00"}),
        ("cancel", {"broker_contract_no": "000123"}),
    ):
        for origin in (None, {}, {"strategy_id": "s1"}):
            with pytest.raises(RequestError, match="virtual_account_id or subaccount_key"):
                add(store, "%s-%s" % (kind, origin), kind=kind,
                    params=params, origin=origin)
    assert store.next_queued() is None


def test_subaccount_key_alone_is_valid_owner(tmp_path):
    store = RequestStore(tmp_path / "requests.sqlite3")
    request = add(store, "sub:one", origin={"subaccount_key": "sub-1"})
    assert request.origin == {"subaccount_key": "sub-1"}


def test_events_survive_restart_and_paginate_without_duplicate_idempotent_events(tmp_path):
    path = tmp_path / "requests.sqlite3"
    store = RequestStore(path)
    request = add(store, origin={"subaccount_key": "parent:sim-account"})
    assert add(store, expires_at=request.expires_at,
               origin={"subaccount_key": "parent:sim-account"}) == request
    store.mark_preparing(request.request_id)
    store.mark_submit_unknown(request.request_id)
    store.mark_accepted(request.request_id, "001234", "broker:001234")

    restarted = RequestStore(path)
    first_page = restarted.events(request.request_id, limit=2)
    second_page = restarted.events(request.request_id, after_id=first_page[-1].event_id, limit=2)
    assert [e.state for e in first_page + second_page] == [
        "queued", "preparing", "submit_unknown", "accepted"]
    assert [e.event_type for e in first_page + second_page] == [
        "queued", "preparing", "submit_unknown", "accepted"]
    assert second_page[-1].broker_contract_no == "001234"
    assert second_page[-1].evidence_ref == "broker:001234"
    assert restarted.events(request.request_id, after_id=second_page[-1].event_id) == []
    assert add(restarted, expires_at=request.expires_at,
               origin={"subaccount_key": "parent:sim-account"}) == restarted.get(request.request_id)
    assert len(restarted.events(request.request_id)) == 4
    with pytest.raises(RequestConflict):
        restarted.mark_accepted(request.request_id, "001234", "broker:001234")
    assert len(restarted.events(request.request_id)) == 4


def test_expiration_events_cover_entry_reclaim_and_preparing_boundary(tmp_path, monkeypatch):
    path = tmp_path / "requests.sqlite3"
    store = RequestStore(path, max_pending=1)
    old = add(store, "old", expires_at=time.time() - 1)
    assert [e.state for e in store.events(old.request_id)] == ["expired"]
    waiting = add(store, "waiting", expires_at=time.time() + 5)
    direct_expired = add(store, "direct-expired", expires_at=time.time() - 1)
    assert direct_expired.state == "expired"
    assert [e.state for e in store.events(direct_expired.request_id)] == ["expired"]
    with pytest.raises(StoreBusy):
        add(store, "full")
    assert store.next_queued(now=waiting.expires_at + 1) is None
    assert [e.state for e in store.events(waiting.request_id)] == ["queued", "expired"]

    boundary = add(store, "boundary", expires_at=time.time() + 5)
    with monkeypatch.context() as patch:
        patch.setattr("bullet_trade.integrations.ths.request_store.time.time",
                      lambda: boundary.expires_at + 1)
        assert store.mark_preparing(boundary.request_id).state == "expired"
    assert [e.state for e in store.events(boundary.request_id)] == ["queued", "expired"]


def test_migration_records_only_current_state_snapshot_once(tmp_path):
    path = tmp_path / "requests.sqlite3"
    store = RequestStore(path)
    request = add(store)
    store.mark_preparing(request.request_id)
    store.mark_submit_unknown(request.request_id)
    # Emulate a database created by the previous implementation.
    with sqlite3.connect(path) as db:
        db.execute("DROP TABLE ths_request_events")
    migrated = RequestStore(path)
    events = migrated.events(request.request_id)
    assert [(e.event_type, e.state) for e in events] == [
        ("migration_current_state", "submit_unknown")]
    assert RequestStore(path).events(request.request_id) == events
    migrated.mark_rejected(request.request_id, "broker:rejected")
    assert [(e.event_type, e.state) for e in migrated.events(request.request_id)] == [
        ("migration_current_state", "submit_unknown"), ("rejected", "rejected")]
