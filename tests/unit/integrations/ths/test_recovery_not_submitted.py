"""Synthetic operator review of a cancel failure before its first click."""

from __future__ import annotations

import ast
import hashlib
import json
import time
from datetime import date

import pytest

from bullet_trade.integrations.ths.recovery import RecoveryError, recover
from bullet_trade.integrations.ths.request_store import RequestConflict, RequestStore


IDENTITY = ("request_id", "account", "trade_day", "idempotency_key",
            "kind", "params", "origin")


def _digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def _unknown_request(tmp_path, *, kind="cancel"):
    store = RequestStore(tmp_path / "requests.sqlite3")
    day = date.today().isoformat()
    if kind == "cancel":
        owner = store.enqueue("test-account", day, "owner", "limit_buy",
                              {"security": "600001.XSHG", "quantity": 10,
                               "price": "10.000"}, time.time() + 60,
                              origin={"subaccount_key": "parent:test-account"})
        store.mark_preparing(owner.request_id)
        store.mark_submit_unknown(owner.request_id)
        store.mark_accepted(owner.request_id, "ORDER-A", "broker:owner")
        params = {"broker_contract_no": "ORDER-A"}
    else:
        params = {"security": "600001.XSHG", "quantity": 10, "price": "10.000"}
    request = store.enqueue("test-account", day, "key-test", kind, params,
                            time.time() + 60,
                            origin={"subaccount_key": "parent:test-account"})
    store.mark_preparing(request.request_id)
    store.mark_submit_unknown(request.request_id)
    return store, store.get(request.request_id)


def _source_text(*, click_before=False):
    before = "        button.click_input()\n" if click_before else ""
    return ("class NativeActions:\n"
            "    def _submit_cancel(self, request):\n"
            + before +
            "        if not button.is_visible():\n"
            "            raise NativeActionBlocked('cancel_button_unverified')\n"
            "        button.click_input()\n")


def _case(tmp_path, request, *, click_before=False):
    source_dir = tmp_path / "executed-source"
    source_dir.mkdir()
    source_path = source_dir / "native_actions.py"
    source_path.write_text(_source_text(click_before=click_before), encoding="utf-8")
    source_hash = _digest(source_path)
    source_ast = ast.parse(source_path.read_text(encoding="utf-8"))
    raise_line = next(node.lineno for node in ast.walk(source_ast)
                      if isinstance(node, ast.Raise))
    exception = {"request_id": request.request_id, "type": "NativeActionBlocked",
                 "code": "cancel_button_unverified",
                 "site": {"function": "_submit_cancel", "line": raise_line},
                 "source_sha256": source_hash}
    log_path = tmp_path / "exception-log.json"
    _write_json(log_path, {"exception": exception})
    evidence = {key: getattr(request, key) for key in IDENTITY}
    evidence.update(
        execution_source={"path": str(source_path), "sha256": source_hash},
        exception_log={"path": str(log_path), "sha256": _digest(log_path)},
        exception=exception, failure_boundary="before_cancel_selection",
        reviewed_no_cancel_selection_submit_confirmation=True)
    evidence_path = tmp_path / "pre-click-evidence.json"
    _write_json(evidence_path, evidence)
    manifest = {key: getattr(request, key) for key in IDENTITY}
    manifest.update(expected_state="submit_unknown", resolution="not_submitted",
                    evidence_source="operator_reviewed_pre_click_failure",
                    evidence_path=str(evidence_path), evidence_sha256=_digest(evidence_path),
                    broker_contract_no=None)
    manifest_path = tmp_path / "manifest.json"
    _write_json(manifest_path, manifest)
    return manifest_path, manifest, evidence_path, evidence, source_path, log_path


def _refresh_evidence(manifest_path, manifest, evidence_path, evidence):
    _write_json(evidence_path, evidence)
    manifest["evidence_sha256"] = _digest(evidence_path)
    _write_json(manifest_path, manifest)


def test_pre_click_cancel_resolution_is_audited_before_cas_and_idempotent(
        tmp_path, monkeypatch):
    store, request = _unknown_request(tmp_path)
    path, manifest, *_ = _case(tmp_path, request)
    original = RequestStore.mark_cancel_not_submitted
    observed = []

    def audited_transition(self, request_id, evidence_ref):
        audit = tmp_path / "recovery-audit" / (request_id + ".json")
        assert audit.is_file()
        assert json.loads(audit.read_text(encoding="utf-8"))["manifest_sha256"] == _digest(path)
        observed.append(True)
        return original(self, request_id, evidence_ref)

    monkeypatch.setattr(RequestStore, "mark_cancel_not_submitted", audited_transition)
    with pytest.raises(RecoveryError, match="operator_review_required"):
        recover(tmp_path, path)
    assert store.get(request.request_id).state == "submit_unknown"
    first = recover(tmp_path, path, operator_reviewed=True)
    assert first.state == "local_aborted" and first.status == "applied"
    assert observed == [True]
    events = store.events(request.request_id)
    assert events[-1].event_type == "operator_reviewed_not_submitted"
    assert events[-1].state == "local_aborted"
    second = recover(tmp_path, path, operator_reviewed=True)
    assert second.status == "already_applied"
    assert store.events(request.request_id) == events
    same_key = store.enqueue(request.account, request.trade_day, request.idempotency_key,
                             request.kind, request.params, request.expires_at,
                             origin=request.origin)
    assert same_key.request_id == request.request_id
    assert store.events(request.request_id) == events


def test_missing_bound_log_or_source_digest_leaves_unknown(tmp_path):
    store, request = _unknown_request(tmp_path)
    path, manifest, evidence_path, evidence, source_path, log_path = _case(tmp_path, request)
    log_path.unlink()
    with pytest.raises(RecoveryError, match="bound_file_unavailable"):
        recover(tmp_path, path, operator_reviewed=True)
    assert store.get(request.request_id).state == "submit_unknown"
    assert not (tmp_path / "recovery-audit" / (request.request_id + ".json")).exists()
    _write_json(log_path, {"exception": evidence["exception"]})
    source_path.write_text(source_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(RecoveryError, match="bound_file_digest_mismatch"):
        recover(tmp_path, path, operator_reviewed=True)


def test_missing_execution_source_leaves_unknown(tmp_path):
    store, request = _unknown_request(tmp_path)
    path, _, _, _, source_path, _ = _case(tmp_path, request)
    source_path.unlink()
    with pytest.raises(RecoveryError, match="bound_file_unavailable"):
        recover(tmp_path, path, operator_reviewed=True)
    assert store.get(request.request_id).state == "submit_unknown"


@pytest.mark.parametrize("change,code", [
    ("wrong_request", "pre_click_exception_mismatch"),
    ("wrong_exception_code", "pre_click_exception_mismatch"),
    ("wrong_exception_source", "pre_click_exception_mismatch"),
    ("wrong_site", "execution_site_unverified"),
    ("review_false", "failure_boundary_unverified"),
    ("log_conflict", "exception_log_mismatch"),
])
def test_conflicting_exception_or_review_cannot_resolve_unknown(tmp_path, change, code):
    store, request = _unknown_request(tmp_path)
    path, manifest, evidence_path, evidence, _, log_path = _case(tmp_path, request)
    if change == "wrong_request":
        evidence["exception"]["request_id"] = "another-request"
    elif change == "wrong_exception_code":
        evidence["exception"]["code"] = "cancel_result_unknown"
    elif change == "wrong_exception_source":
        evidence["exception"]["source_sha256"] = "0" * 64
    elif change == "wrong_site":
        evidence["exception"]["site"]["function"] = "_submit_order"
    elif change == "review_false":
        evidence["reviewed_no_cancel_selection_submit_confirmation"] = False
    else:
        _write_json(log_path, {"exception": {**evidence["exception"], "code": "other"}})
        evidence["exception_log"]["sha256"] = _digest(log_path)
    if change not in {"review_false", "log_conflict"}:
        _write_json(log_path, {"exception": evidence["exception"]})
        evidence["exception_log"]["sha256"] = _digest(log_path)
    _refresh_evidence(path, manifest, evidence_path, evidence)
    with pytest.raises(RecoveryError, match=code):
        recover(tmp_path, path, operator_reviewed=True)
    assert store.get(request.request_id).state == "submit_unknown"


def test_click_before_reported_raise_cannot_prove_not_submitted(tmp_path):
    store, request = _unknown_request(tmp_path)
    path, _, *_ = _case(tmp_path, request, click_before=True)
    with pytest.raises(RecoveryError, match="failure_boundary_unverified"):
        recover(tmp_path, path, operator_reviewed=True)
    assert store.get(request.request_id).state == "submit_unknown"


def test_buy_or_wrong_resolution_and_ordinary_abort_stay_blocked(tmp_path):
    store, request = _unknown_request(tmp_path, kind="limit_buy")
    path, manifest, *_ = _case(tmp_path, request)
    with pytest.raises(RecoveryError, match="pre_click_resolution_unverified"):
        recover(tmp_path, path, operator_reviewed=True)
    with pytest.raises(RequestConflict):
        store.mark_cancel_not_submitted(request.request_id, "review:invalid")
    with pytest.raises(RequestConflict):
        store.mark_local_aborted(request.request_id, "runtime:invalid")
    manifest["resolution"] = "rejected"
    _write_json(path, manifest)
    with pytest.raises(RecoveryError, match="pre_click_resolution_unverified"):
        recover(tmp_path, path, operator_reviewed=True)
    assert store.get(request.request_id).state == "submit_unknown"
