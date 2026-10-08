"""Cooperative actor loop. Run separately from HTTP, on the interactive desktop.

An integration driver supplies query() as well as GuiDriver's write methods.
Query must either return a full fresh snapshot, or YIELD after verified dialog
cleanup. Raising/timeout never publishes an empty snapshot. Supported client
capabilities depend on the configured driver and its current evidence.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
import time
import math
import os
import threading

from .runtime import GuiRuntime
from .scheduler import GuiTaskScheduler, YIELD


@dataclass(frozen=True)
class CollectedSnapshot:
    account: str
    kind: str
    data: object
    collected_at: datetime
    complete: bool
    metadata: dict = field(default_factory=dict)


class ActorService:
    def __init__(self, requests, snapshots, driver, *, account, lock_path,
                 interval=20, kinds=("orders", "trades", "account", "positions", "cancelable"),
                 client_lock_factory=None, write_enabled=False,
                 snapshot_max_age_seconds=120):
        if write_enabled is not True and write_enabled is not False:
            raise ValueError("write_enabled must be boolean")
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("interval must be positive and finite")
        if not kinds or len(set(kinds)) != len(kinds):
            raise ValueError("nonempty unique query kinds required")
        if not math.isfinite(snapshot_max_age_seconds) or snapshot_max_age_seconds < 0:
            raise ValueError("snapshot_max_age_seconds must be finite and nonnegative")
        self.requests = requests
        self.snapshots = snapshots
        self.driver = driver
        self.account = account
        self.interval = interval
        self.kinds = kinds
        self.snapshot_max_age_seconds = snapshot_max_age_seconds
        self.write_enabled = write_enabled
        self.runtime = GuiRuntime(requests, driver, lock_path,
                                  client_lock_factory=client_lock_factory)
        self.scheduler = GuiTaskScheduler()
        self._operation_started_at = None
        self._last_error = None
        self._last_error_at = None

    def _pressure(self):
        # Unknown submissions prevent new writes; allow read-only reconciliation.
        return (self.write_enabled and not self.requests.has_unresolved()
                and self.requests.next_queued() is not None)

    def _refresh(self, kind):
        def execute(context):
            if self._pressure():
                return YIELD
            try:
                with self.runtime.actor():
                    result = self.driver.query(kind, self._pressure)
                    if result is YIELD:
                        # The driver contract requires verified cleanup BEFORE yielding.
                        return YIELD
                    if (not isinstance(result, CollectedSnapshot)
                            or result.account != self.account or result.kind != kind
                            or result.complete is not True):
                        raise ValueError("query_identity_or_completeness_unverified")
                    self.snapshots.publish_success(self.account, kind, result.data,
                        result.collected_at, complete=True, metadata=result.metadata)
            except Exception as exc:
                self.snapshots.record_error(self.account, kind, type(exc).__name__)
                raise
        return execute

    def step(self):
        if self.driver is None:
            raise RuntimeError("qualified GUI driver required")
        if self._pressure():
            result = self.runtime.run_once()
            # Schedule post-operation reconciliation without issuing a GUI call here.
            for kind in ("orders", "trades"):
                if kind in self.kinds:
                    self.scheduler.submit(self._refresh(kind), priority=5,
                                          key="refresh:" + kind)
            return result
        for kind in self.kinds:
            self.scheduler.submit_refresh(kind, self._refresh(kind), interval=self.interval)
        return self.scheduler.run_next()

    def _publish_health(self, *, stopped=False):
        # The heartbeat never calls the driver: GUI ownership stays on run().
        started = self._operation_started_at
        overlong = started is not None and time.time() - started > 120
        ready = (not stopped and not overlong
                 and getattr(self.driver, "ready", False) is True)
        unresolved = self.requests.has_unresolved()
        # Each kind owns its current error. A later success for another kind
        # must not erase it; last_error below remains the historical loop error.
        current_query_errors = {}
        for kind in self.kinds:
            snapshot = self.snapshots.read(
                self.account, kind, max_age_seconds=self.snapshot_max_age_seconds)
            if snapshot["status"] != "complete" or snapshot["stale"]:
                current_query_errors[kind] = (
                    snapshot["last_error"] or
                    ("stale" if snapshot["stale"] and snapshot["status"] == "complete"
                     else snapshot["status"])
                )
        queries_ready = not current_query_errors
        self.snapshots.publish_success(self.account, "actor_health", {
            "last_error": self._last_error, "last_error_at": self._last_error_at,
            "current_query_errors": current_query_errors,
            "required_snapshot_kinds": list(self.kinds),
            "queries_ready": queries_ready,
            "driver_ready": ready, "writes_enabled": self.write_enabled,
            "writes_stopped": stopped or not self.write_enabled or unresolved or overlong,
            "trading_ready": ready and self.write_enabled and not unresolved and queries_ready,
            "busy": started is not None, "operation_started_at": started,
            "process_id": os.getpid(), "pending_refreshes": self.scheduler.pending_count,
        }, datetime.now(timezone.utc), complete=True)

    def run(self, stop):
        """Keep health available while a slow GUI call runs; never replace that actor."""
        heartbeat_stop = threading.Event()

        def heartbeat():
            while not heartbeat_stop.is_set():
                try:
                    self._publish_health()
                except Exception:
                    # A failed heartbeat becomes stale. Do not claim healthy
                    # by changing collected_at without a successful commit.
                    pass
                heartbeat_stop.wait(2)

        thread = threading.Thread(target=heartbeat, name="ths-health", daemon=True)
        thread.start()
        try:
            while not stop.is_set():
                self._operation_started_at = time.time()
                try:
                    self.step()
                    error = None
                except Exception as exc:
                    error = type(exc).__name__
                    self._last_error = error
                    self._last_error_at = datetime.now(timezone.utc).isoformat()
                finally:
                    self._operation_started_at = None
                stop.wait(0.1 if error is None else 1)
        finally:
            heartbeat_stop.set()
            thread.join(timeout=3)
            self._publish_health(stopped=True)
