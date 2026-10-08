import hashlib
import json
import time
from datetime import date

import pytest
from filelock import FileLock

from bullet_trade.integrations.ths.recovery import RecoveryError, main, recover
from bullet_trade.integrations.ths.request_store import RequestStore


def setup_request(tmp_path, *, kind="limit_buy", state="submit_unknown"):
    store = RequestStore(tmp_path / "requests.sqlite3")
    params = ({"broker_contract_no": "000123"} if kind == "cancel" else
              {"security": "600000", "quantity": 100, "price": "10.00"})
    if kind == "cancel":
        order = store.enqueue("account", date.today().isoformat(), "owner", "limit_buy",
                              {"security": "600000", "quantity": 100, "price": "10.00"},
                              time.time() + 60, origin={"virtual_account_id": "v1"})
        store.mark_preparing(order.request_id)
        store.mark_submit_unknown(order.request_id)
        store.mark_accepted(order.request_id, "000123", "broker:owner")
    request = store.enqueue("account", date.today().isoformat(), "key-" + kind, kind,
                            params, time.time() + 60,
                            origin={"virtual_account_id": "v1"})
    store.mark_preparing(request.request_id)
    if state == "submit_unknown":
        store.mark_submit_unknown(request.request_id)
    return store, store.get(request.request_id)


def manifest_for(tmp_path, request, resolution, *, source="broker_receipt"):
    evidence = tmp_path / "receipt.bin"
    evidence.write_bytes(b"operator inspected exact broker evidence")
    payload = {key: getattr(request, key) for key in
               ("request_id", "account", "trade_day", "idempotency_key",
                "kind", "params", "origin")}
    payload.update(expected_state=request.state, resolution=resolution,
                   evidence_source=source, evidence_path=str(evidence),
                   evidence_sha256=hashlib.sha256(evidence.read_bytes()).hexdigest(),
                   broker_contract_no=("000123" if resolution == "accepted" else None))
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path, payload, evidence


def test_accepted_recovery_is_reviewed_idempotent_and_audited(tmp_path):
    store, request = setup_request(tmp_path)
    path, _, _ = manifest_for(tmp_path, request, "accepted")
    with pytest.raises(RecoveryError, match="operator_review_required"):
        recover(tmp_path, path)
    assert store.get(request.request_id).state == "submit_unknown"
    first = recover(tmp_path, path, operator_reviewed=True)
    assert first.status == "applied"
    assert first.audit_path.is_file()
    assert store.get(request.request_id).broker_contract_no == "000123"
    events = store.events(request.request_id)
    second = recover(tmp_path, path, operator_reviewed=True)
    assert second.status == "already_applied"
    assert store.events(request.request_id) == events
    assert main(["--state-dir", str(tmp_path), "--evidence-file", str(path),
                 "--operator-reviewed"]) == 0


@pytest.mark.parametrize("resolution,state", [("rejected", "submit_unknown"),
                                           ("local_aborted", "preparing")])
def test_only_allowed_reviewed_terminal_transitions(tmp_path, resolution, state):
    store, request = setup_request(tmp_path, state=state)
    path, _, _ = manifest_for(tmp_path, request, resolution,
                              source="operator_reviewed_receipt")
    result = recover(tmp_path, path, operator_reviewed=True)
    assert result.state == resolution
    assert store.get(request.request_id).evidence_ref.startswith("recovery:")


def test_mismatch_or_changed_evidence_leaves_unresolved_request(tmp_path):
    store, request = setup_request(tmp_path)
    path, payload, evidence = manifest_for(tmp_path, request, "accepted")
    payload["origin"] = {"virtual_account_id": "different"}
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RecoveryError, match="request_identity_mismatch"):
        recover(tmp_path, path, operator_reviewed=True)
    payload["origin"] = request.origin
    payload["params"]["quantity"] = 100.0
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RecoveryError, match="request_identity_mismatch"):
        recover(tmp_path, path, operator_reviewed=True)
    payload["params"] = request.params
    path.write_text(json.dumps(payload), encoding="utf-8")
    evidence.write_bytes(b"changed evidence")
    with pytest.raises(RecoveryError, match="evidence_digest_mismatch"):
        recover(tmp_path, path, operator_reviewed=True)
    assert store.get(request.request_id).state == "submit_unknown"
    assert not (tmp_path / "recovery-audit" / (request.request_id + ".json")).exists()


def test_cancel_must_use_exact_original_contract(tmp_path):
    store, request = setup_request(tmp_path, kind="cancel")
    path, payload, _ = manifest_for(tmp_path, request, "accepted")
    payload["broker_contract_no"] = "123"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RecoveryError, match="cancel_contract_mismatch"):
        recover(tmp_path, path, operator_reviewed=True)
    assert store.get(request.request_id).state == "submit_unknown"


def test_actor_lock_must_be_free(tmp_path):
    store, request = setup_request(tmp_path)
    path, _, _ = manifest_for(tmp_path, request, "rejected")
    with FileLock(str(tmp_path / "actor-owner.lock")).acquire(timeout=0):
        with pytest.raises(RecoveryError, match="actor_or_gui_lock_busy"):
            recover(tmp_path, path, operator_reviewed=True)
    with FileLock(str(tmp_path / "gui-actor.lock")).acquire(timeout=0):
        with pytest.raises(RecoveryError, match="actor_or_gui_lock_busy"):
            recover(tmp_path, path, operator_reviewed=True)
    assert store.get(request.request_id).state == "submit_unknown"
