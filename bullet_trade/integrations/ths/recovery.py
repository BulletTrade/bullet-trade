"""Local operator-reviewed repair of one unresolved THS request.

Stop the actor first. Review a real receipt or other broker evidence, create a
private JSON manifest, then run ``python -m bullet_trade.integrations.ths.recovery
--state-dir DIR --evidence-file MANIFEST.json --operator-reviewed``. This tool
never opens the GUI, resubmits, or infers a contract from matching trade fields.

The separate ``not_submitted`` cancel resolution requires review of the .py
source copy actually executed for the failed request and its bound exception
log. The current checkout's source hash alone does not prove that version ran.
The reviewed assertion covers absence of cancel selection, submit and confirm.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from filelock import FileLock, Timeout

from .request_store import Request, RequestError, RequestStore, StoreBusy


class RecoveryError(ValueError):
    """Fixed diagnostic codes only; no confidential evidence in messages."""


@dataclass(frozen=True)
class RecoveryResult:
    request_id: str
    state: str
    status: str  # applied or already_applied
    audit_path: Path


_IDENTITY = ("request_id", "account", "trade_day", "idempotency_key",
             "kind", "params", "origin")
_FIELDS = set(_IDENTITY) | {"expected_state", "resolution", "evidence_source",
                            "evidence_path", "evidence_sha256", "broker_contract_no"}
_TRANSITIONS = {("submit_unknown", "accepted"),
                ("submit_unknown", "rejected"),
                ("preparing", "local_aborted"),
                ("submit_unknown", "not_submitted")}

_PRE_CLICK_SOURCE = "operator_reviewed_pre_click_failure"


def _read_manifest(path: Path) -> tuple[dict, str]:
    if not path.is_file() or path.is_symlink():
        raise RecoveryError("manifest_file_unavailable")
    try:
        content = path.read_bytes()
        manifest = json.loads(content.decode("utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise RecoveryError("manifest_unreadable") from exc
    if not isinstance(manifest, dict) or set(manifest) != _FIELDS:
        raise RecoveryError("manifest_fields_unverified")
    return manifest, hashlib.sha256(content).hexdigest()


def _check_evidence(manifest: dict, manifest_path: Path) -> None:
    if (not isinstance(manifest["evidence_source"], str)
            or manifest["evidence_source"] not in {
                "operator_reviewed_receipt", "broker_receipt", _PRE_CLICK_SOURCE}):
        raise RecoveryError("evidence_source_unverified")
    digest = manifest["evidence_sha256"]
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise RecoveryError("evidence_digest_unverified")
    raw_path = manifest["evidence_path"]
    if (not isinstance(raw_path, str) or not raw_path
            or raw_path.startswith("\\\\") or raw_path.startswith("//")):
        raise RecoveryError("evidence_path_unverified")
    path = Path(raw_path)
    if not path.is_absolute():
        path = manifest_path.parent / path
    if not path.is_file() or path.is_symlink() or path.resolve() == manifest_path.resolve():
        raise RecoveryError("evidence_file_unavailable")
    try:
        observed = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise RecoveryError("evidence_file_unreadable") from exc
    if observed != digest:
        raise RecoveryError("evidence_digest_mismatch")


def _read_bound_file(spec: object, base: Path, *, suffix: str | None = None) -> tuple[Path, bytes]:
    if (not isinstance(spec, dict) or set(spec) != {"path", "sha256"}
            or not isinstance(spec["path"], str) or not spec["path"]
            or spec["path"].startswith(("//", "\\\\"))
            or not isinstance(spec["sha256"], str)
            or re.fullmatch(r"[0-9a-f]{64}", spec["sha256"]) is None):
        raise RecoveryError("bound_file_unverified")
    path = Path(spec["path"])
    if not path.is_absolute():
        path = base / path
    if path.is_symlink() or not path.is_file() or (suffix and path.suffix != suffix):
        raise RecoveryError("bound_file_unavailable")
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise RecoveryError("bound_file_unreadable") from exc
    if hashlib.sha256(content).hexdigest() != spec["sha256"]:
        raise RecoveryError("bound_file_digest_mismatch")
    return path, content


def _check_pre_click_source(source: bytes, site: object) -> None:
    """Conservatively locate the sole failure raise before any click_input."""
    try:
        tree = ast.parse(source.decode("utf-8"))
    except (SyntaxError, UnicodeError) as exc:
        raise RecoveryError("execution_source_unparseable") from exc
    methods = [method for cls in tree.body if isinstance(cls, ast.ClassDef)
               and cls.name == "NativeActions" for method in cls.body
               if isinstance(method, ast.FunctionDef) and method.name == "_submit_cancel"]
    if len(methods) != 1:
        raise RecoveryError("execution_site_unverified")
    method = methods[0]
    def is_target_raise(node):
        value = node.exc if isinstance(node, ast.Raise) else None
        return (isinstance(value, ast.Call)
                and isinstance(value.func, ast.Name)
                and value.func.id == "NativeActionBlocked"
                and len(value.args) == 1
                and isinstance(value.args[0], ast.Constant)
                and value.args[0].value == "cancel_button_unverified")
    raises = [node for node in ast.walk(tree) if is_target_raise(node)]
    if (len(raises) != 1 or raises[0] not in ast.walk(method)
            or site != {"function": "_submit_cancel", "line": raises[0].lineno}):
        raise RecoveryError("execution_site_unverified")
    parents = {child: parent for parent in ast.walk(method)
               for child in ast.iter_child_nodes(parent)}
    ancestor = parents.get(raises[0])
    while ancestor is not method:
        if (ancestor is None or isinstance(ancestor, (ast.FunctionDef,
                                                       ast.AsyncFunctionDef, ast.Lambda))):
            raise RecoveryError("execution_site_unverified")
        ancestor = parents.get(ancestor)
    clicks = [node for node in ast.walk(method) if isinstance(node, ast.Call)
              and isinstance(node.func, ast.Attribute)
              and node.func.attr == "click_input"]
    if not clicks or min(node.lineno for node in clicks) <= raises[0].lineno:
        raise RecoveryError("failure_boundary_unverified")


def _check_not_submitted_evidence(manifest: dict, manifest_path: Path,
                                  request: Request) -> None:
    """Evidence JSON binds the exact request, executed .py and exception log.

    The log must be JSON with an ``exception`` object equal to the evidence's
    exception object. The operator asserts that no cancel selection, submit,
    or confirmation happened; AST establishes the reported raise's position.
    """
    evidence_path, raw = _read_bound_file(
        {"path": manifest["evidence_path"], "sha256": manifest["evidence_sha256"]},
        manifest_path.parent)
    if evidence_path.resolve() == manifest_path.resolve():
        raise RecoveryError("evidence_file_unavailable")
    try:
        evidence = json.loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise RecoveryError("pre_click_evidence_unreadable") from exc
    required = set(_IDENTITY) | {
        "execution_source", "exception_log", "exception", "failure_boundary",
        "reviewed_no_cancel_selection_submit_confirmation"}
    if not isinstance(evidence, dict) or set(evidence) != required:
        raise RecoveryError("pre_click_evidence_fields_unverified")
    if any(json.dumps(evidence[field], sort_keys=True, ensure_ascii=False) !=
           json.dumps(getattr(request, field), sort_keys=True, ensure_ascii=False)
           for field in _IDENTITY):
        raise RecoveryError("pre_click_request_mismatch")
    if (evidence["failure_boundary"] != "before_cancel_selection"
            or evidence["reviewed_no_cancel_selection_submit_confirmation"] is not True):
        raise RecoveryError("failure_boundary_unverified")
    source_path, source = _read_bound_file(evidence["execution_source"],
                                           evidence_path.parent, suffix=".py")
    if source_path.name != "native_actions.py":
        raise RecoveryError("execution_source_unverified")
    log_path, log_raw = _read_bound_file(evidence["exception_log"], evidence_path.parent)
    if len({source_path.resolve(), log_path.resolve(), evidence_path.resolve(),
            manifest_path.resolve()}) != 4:
        raise RecoveryError("bound_files_not_distinct")
    exception = evidence["exception"]
    if (not isinstance(exception, dict)
            or set(exception) != {"request_id", "type", "code", "site", "source_sha256"}
            or exception["request_id"] != request.request_id
            or exception["type"] != "NativeActionBlocked"
            or exception["code"] != "cancel_button_unverified"
            or exception["source_sha256"] != evidence["execution_source"]["sha256"]):
        raise RecoveryError("pre_click_exception_mismatch")
    try:
        log = json.loads(log_raw.decode("utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise RecoveryError("exception_log_unreadable") from exc
    if not isinstance(log, dict) or log.get("exception") != exception:
        raise RecoveryError("exception_log_mismatch")
    _check_pre_click_source(source, exception["site"])


def _check_request(manifest: dict, request: Request) -> tuple[str, str]:
    if any(json.dumps(manifest[field], sort_keys=True, ensure_ascii=False) !=
           json.dumps(getattr(request, field), sort_keys=True, ensure_ascii=False)
           for field in _IDENTITY):
        raise RecoveryError("request_identity_mismatch")
    expected, resolution = manifest["expected_state"], manifest["resolution"]
    if (not isinstance(expected, str) or not isinstance(resolution, str)
            or (expected, resolution) not in _TRANSITIONS):
        raise RecoveryError("transition_not_allowed")
    if resolution == "not_submitted":
        if request.kind != "cancel" or manifest["evidence_source"] != _PRE_CLICK_SOURCE:
            raise RecoveryError("pre_click_resolution_unverified")
    elif manifest["evidence_source"] == _PRE_CLICK_SOURCE:
        raise RecoveryError("pre_click_resolution_unverified")
    contract = manifest["broker_contract_no"]
    if resolution == "accepted":
        if (not isinstance(contract, str) or not contract or contract != contract.strip()
                or contract == request.request_id):
            raise RecoveryError("broker_contract_unverified")
        if request.kind == "cancel" and contract != request.params["broker_contract_no"]:
            raise RecoveryError("cancel_contract_mismatch")
    elif contract is not None:
        raise RecoveryError("unexpected_broker_contract")
    return expected, resolution


def _save_audit(path: Path, payload: dict, digest: str) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.exists():
        try:
            old = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise RecoveryError("audit_unreadable") from exc
        if any(old.get(field) != payload[field] for field in payload if field != "reviewed_at"):
            raise RecoveryError("audit_conflict")
        return
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":")).encode("utf-8")
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise RecoveryError("audit_conflict") from None
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        if os.name != "nt":
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except BaseException:
        # Retain a partially written intent for manual inspection; never
        # overwrite it on retry or apply a state transition after write failure.
        raise


def recover(state_dir: str | Path, evidence_file: str | Path, *,
            operator_reviewed: bool = False) -> RecoveryResult:
    """Apply one narrowly authorized state repair while both actor locks are held."""
    if operator_reviewed is not True:
        raise RecoveryError("operator_review_required")
    directory, manifest_path = Path(state_dir), Path(evidence_file)
    if not (directory / "requests.sqlite3").is_file():
        raise RecoveryError("request_database_missing")
    manifest, manifest_digest = _read_manifest(manifest_path)
    try:
        request_id = str(uuid.UUID(manifest["request_id"]))
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise RecoveryError("request_id_unverified") from exc
    if request_id != manifest["request_id"]:
        raise RecoveryError("request_id_unverified")
    _check_evidence(manifest, manifest_path)
    owner_lock = FileLock(str(directory / "actor-owner.lock"))
    gui_lock = FileLock(str(directory / "gui-actor.lock"))
    try:
        with owner_lock.acquire(timeout=0):
            with gui_lock.acquire(timeout=0):
                if _read_manifest(manifest_path)[1] != manifest_digest:
                    raise RecoveryError("manifest_changed_during_review")
                store = RequestStore(directory / "requests.sqlite3")
                request = store.get(request_id)
                if request is None:
                    raise RecoveryError("request_missing")
                expected, resolution = _check_request(manifest, request)
                if resolution == "not_submitted":
                    _check_not_submitted_evidence(manifest, manifest_path, request)
                evidence_ref = ("recovery:not_submitted:" if resolution == "not_submitted"
                                else "recovery:") + manifest["evidence_sha256"]
                terminal_state = "local_aborted" if resolution == "not_submitted" else resolution
                audit_path = directory / "recovery-audit" / (request_id + ".json")
                audit = {"request_id": request_id, "manifest_sha256": manifest_digest,
                         "reviewed_manifest": manifest,
                         "evidence_sha256": manifest["evidence_sha256"],
                         "evidence_source": manifest["evidence_source"],
                         "expected_state": expected, "resolution": resolution,
                         "reviewed_at": time.time()}
                if request.state == terminal_state:
                    if not audit_path.is_file() or request.evidence_ref != evidence_ref:
                        raise RecoveryError("completed_state_without_matching_audit")
                    if resolution == "accepted" and request.broker_contract_no != manifest["broker_contract_no"]:
                        raise RecoveryError("completed_contract_mismatch")
                    _save_audit(audit_path, audit, manifest_digest)
                    return RecoveryResult(request_id, terminal_state, "already_applied", audit_path)
                if request.state != expected:
                    raise RecoveryError("expected_state_mismatch")
                _save_audit(audit_path, audit, manifest_digest)
                if resolution == "accepted":
                    changed = store.mark_accepted(request_id, manifest["broker_contract_no"], evidence_ref)
                elif resolution == "rejected":
                    changed = store.mark_rejected(request_id, evidence_ref)
                elif resolution == "not_submitted":
                    changed = store.mark_cancel_not_submitted(request_id, evidence_ref)
                else:
                    changed = store.mark_local_aborted(request_id, evidence_ref)
                return RecoveryResult(request_id, changed.state, "applied", audit_path)
    except Timeout as exc:
        raise RecoveryError("actor_or_gui_lock_busy") from exc
    except sqlite3.Error as exc:
        raise RecoveryError("request_database_unavailable") from exc
    except (RequestError, StoreBusy) as exc:
        raise RecoveryError("request_transition_failed") from exc


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--evidence-file", required=True,
                        help="private JSON manifest naming a separate hashed local evidence file")
    parser.add_argument("--operator-reviewed", action="store_true",
                        help="affirm operator review of the bound request and evidence")
    args = parser.parse_args(argv)
    try:
        result = recover(args.state_dir, args.evidence_file,
                         operator_reviewed=args.operator_reviewed)
    except RecoveryError as exc:
        print("recovery_failed:" + str(exc), file=sys.stderr)
        return 2
    print("recovery_" + result.status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
