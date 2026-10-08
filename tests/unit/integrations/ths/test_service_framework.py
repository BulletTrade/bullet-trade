"""Bounded local service checks with a synthetic driver; no THS GUI is used."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import datetime, timezone
from http.client import HTTPConnection
import json
import importlib
import math
import os
from pathlib import Path
import threading
import time

import pytest

from bullet_trade.integrations.ths.actor_service import ActorService, CollectedSnapshot
from bullet_trade.integrations.ths.http_service import LocalServer, ServiceApplication
from bullet_trade.integrations.ths.request_store import RequestStore
from bullet_trade.integrations.ths.runtime import BrokerAcceptance, GateProof
from bullet_trade.integrations.ths.scheduler import YIELD
from bullet_trade.integrations.ths.snapshot_store import SnapshotStore
from bullet_trade.server.adapters.base import AccountRouter, AdapterBundle
from bullet_trade.server.adapters.big_qmt import BigQmtBrokerAdapter
from bullet_trade.server.app import IdempotencyConflictError, ServerApplication
from bullet_trade.server.config import AccountConfig, ServerConfig


ACCOUNT = "paper"
TOKEN = "synthetic-test-token-0123456789012345"


def _get(server, path):
    connection = HTTPConnection(*server.server_address, timeout=3)
    started = time.perf_counter()
    try:
        connection.request("GET", path, headers={"Authorization": "Bearer " + TOKEN})
        response = connection.getresponse()
        body = json.loads(response.read())
        return response.status, body, (time.perf_counter() - started) * 1000
    finally:
        connection.close()


def _percentile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def _wait_until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.025)
    return predicate()


class SlowSyntheticDriver:
    ready = True
    client_lock_factory = staticmethod(nullcontext)

    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = 0
        self.lock = threading.Lock()

    def query(self, kind, should_yield):
        with self.lock:
            self.calls += 1
        self.entered.set()
        if not self.release.wait(15):
            raise TimeoutError("synthetic query held too long")
        return CollectedSnapshot(ACCOUNT, kind, [{"quantity": 2}],
                                 datetime.now(timezone.utc), True)


def test_loopback_load_during_slow_query_and_independent_health(tmp_path):
    app = ServiceApplication(tmp_path, account=ACCOUNT, allow_requests=True)
    app.snapshot_store.publish_success(ACCOUNT, "positions", [{"quantity": 1}],
                                       datetime.now(timezone.utc), complete=True)
    driver = SlowSyntheticDriver()
    actor = ActorService(RequestStore(app.requests), SnapshotStore(app.snapshots), driver,
                         account=ACCOUNT, lock_path=tmp_path / "actor.lock",
                         kinds=("positions",), interval=20, write_enabled=True,
                         client_lock_factory=driver.client_lock_factory)
    stop = threading.Event()
    actor_thread = threading.Thread(target=actor.run, args=(stop,), daemon=True)
    with LocalServer(("127.0.0.1", 0), app, TOKEN) as server:
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        actor_thread.start()
        try:
            assert driver.entered.wait(5)
            assert _wait_until(lambda: _get(server, "/health")[1]["trading_ready"])
            first_version = _get(server, "/health")[1]["actor_health"]["version"]
            assert _wait_until(
                lambda: _get(server, "/health")[1]["actor_health"]["version"] > first_version,
                timeout=4,
            ), "heartbeat stopped during a slow GUI query"

            def read_many(_):
                samples = []
                for _ in range(64):
                    status, body, latency = _get(server, "/snapshots/positions")
                    assert status == 200 and body["data"] == [{"quantity": 1}]
                    samples.append(latency)
                return samples

            with ThreadPoolExecutor(max_workers=8) as pool:
                batches = list(pool.map(read_many, range(8)))
            latencies = [value for batch in batches for value in batch]
            assert len(latencies) == 512
            assert driver.calls == 1, "API reads caused additional GUI queries"
            assert max(latencies) < 3000, "loopback GET exceeded its bounded timeout"
            report = {
                "scope": "offline synthetic driver and local loopback only",
                "requests": len(latencies), "reader_threads": 8,
                "gui_query_calls_while_blocked": driver.calls,
                "latency_ms": {"p50": round(_percentile(latencies, 0.50), 3),
                               "p95": round(_percentile(latencies, 0.95), 3),
                               "max": round(max(latencies), 3)},
                "health_advanced_during_query": True,
            }
            report_path = os.environ.get("THS_FRAMEWORK_REPORT")
            if report_path:
                Path(report_path).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        finally:
            driver.release.set()
            stop.set()
            actor_thread.join(timeout=5)
            server.shutdown()
            server_thread.join(timeout=5)
        assert not actor_thread.is_alive()
        # The HTTP listener is already stopped; inspect the same persisted state.
        stopped = app.dispatch("GET", "/health")[1]
        assert stopped["trading_ready"] is False
        assert stopped["actor_health"]["data"]["writes_stopped"] is True


class YieldingSyntheticDriver:
    ready = True
    client_lock_factory = staticmethod(nullcontext)

    def __init__(self):
        self.entered = threading.Event()
        self.continue_query = threading.Event()
        self.queries = 0
        self.submissions = 0

    def query(self, kind, should_yield):
        self.queries += 1
        if self.queries == 1:
            self.entered.set()
            assert self.continue_query.wait(5)
            if should_yield():
                return YIELD
        return CollectedSnapshot(ACCOUNT, kind, [], datetime.now(timezone.utc), True)

    @staticmethod
    def _proof(request):
        return GateProof(request.request_id, request.account, request.trade_day,
                         "synthetic-session", "synthetic:proof", time.monotonic(),
                         True, True, True, True, True, True)

    def preflight(self, request):
        return self._proof(request)

    def prepare(self, request):
        return None

    def validate_readback(self, request):
        return self._proof(request)

    def submit(self, request):
        self.submissions += 1
        return BrokerAcceptance("SYNTHETIC-1", "synthetic:accepted")


def test_cooperative_yield_prioritizes_write_and_refresh_key_is_bounded(tmp_path):
    requests = RequestStore(tmp_path / "requests.sqlite3")
    snapshots = SnapshotStore(tmp_path / "snapshots.sqlite3")
    driver = YieldingSyntheticDriver()
    actor = ActorService(requests, snapshots, driver, account=ACCOUNT,
                         lock_path=tmp_path / "actor.lock", kinds=("positions",),
                         interval=20, write_enabled=True,
                         client_lock_factory=driver.client_lock_factory)
    result = []
    worker = threading.Thread(target=lambda: result.append(actor.step()))
    worker.start()
    try:
        assert driver.entered.wait(5)
        for _ in range(100):
            assert actor.scheduler.submit_refresh(
                "positions", actor._refresh("positions"), interval=20) is None
        assert actor.scheduler.pending_count == 0  # the sole refresh is active
        request = requests.enqueue(
            ACCOUNT, "2026-10-01", "synthetic:one", "limit_buy",
            {"security": "518880.XSHG", "quantity": 100, "price": "8.500"},
            time.time() + 60, origin={"virtual_account_id": "va"})
    finally:
        driver.continue_query.set()
        worker.join(timeout=5)
    assert not worker.is_alive() and result[0].status == "yielded"
    assert actor.scheduler.pending_count == 1
    actor.step()
    assert requests.get(request.request_id).state == "accepted"
    assert driver.submissions == 1 and driver.queries == 1
    actor.step()
    assert driver.queries == 2 and actor.scheduler.pending_count == 0


def _server_app(broker):
    config = ServerConfig(server_type="stub", listen="127.0.0.1", port=0,
                          accounts=[AccountConfig(key="default", account_id="demo")])
    router = AccountRouter(config.accounts)
    return ServerApplication(config, router, AdapterBundle(None, broker)), router.get("default")


def _resolution_payload(amount=100):
    original = {"security": "518880.XSHG", "side": "BUY", "amount": amount,
                "style": {"type": "limit", "price": 8.5}, "idempotency_key": "one"}
    return {"write_action": "broker.place_order", "idempotency_key": "one",
            "request_payload": original}


def test_server_opt_in_resolution_and_fingerprint_guard():
    class DurableBroker:
        supports_durable_resolution = True

        def __init__(self):
            self.calls = 0

        async def resolve_submission(self, ctx, payload):
            self.calls += 1
            return {"write_action": "broker.place_order", "idempotency_key": "one",
                    "submission_state": "accepted", "status": "accepted",
                    "evidence": {"source": "synthetic durable store"}}

        async def list_orders(self, ctx, filters=None):
            raise AssertionError("durable answer should precede order fallback")

    broker = DurableBroker()
    app, ctx = _server_app(broker)

    async def exercise():
        original = _resolution_payload()
        await app._claim_idempotent_write("default", None, "broker.place_order",
                                          original["request_payload"])
        result = await app._resolve_submission("default", None, ctx, original)
        assert result["submission_state"] == "accepted"
        assert broker.calls == 1
        with pytest.raises(IdempotencyConflictError):
            await app._resolve_submission("default", None, ctx, _resolution_payload(200))
        assert broker.calls == 1, "fingerprint conflict reached durable resolver"

    asyncio.run(exercise())


@pytest.mark.parametrize("failure", ["none", "exception"])
def test_opt_in_resolver_failure_never_uses_legacy_order_fallback(failure):
    class DurableProbe:
        supports_durable_resolution = True

        def __init__(self):
            self.resolver_calls = 0
            self.order_calls = 0

        async def resolve_submission(self, ctx, payload):
            self.resolver_calls += 1
            if failure == "exception":
                raise TimeoutError("synthetic resolver unavailable")
            return None

        async def list_orders(self, ctx, filters=None):
            self.order_calls += 1
            return [{"order_id": "OLD-CONTRACT", "idempotency_key": "one"}]

    broker = DurableProbe()
    app, ctx = _server_app(broker)
    result = asyncio.run(app._resolve_submission("default", None, ctx,
                                                 _resolution_payload()))
    assert result["status"] == "submit_unknown"
    assert broker.resolver_calls == 1 and broker.order_calls == 0


def test_big_qmt_without_opt_in_keeps_order_evidence_path():
    class LegacyQmtProbe(BigQmtBrokerAdapter):
        def __init__(self):
            self.resolver_calls = 0
            self.order_calls = 0

        async def resolve_submission(self, ctx, payload):
            self.resolver_calls += 1
            raise AssertionError("legacy QMT must not call THS durable resolver")

        async def list_orders(self, ctx, filters=None):
            self.order_calls += 1
            return []

    broker = LegacyQmtProbe()
    assert getattr(broker, "supports_durable_resolution", False) is False
    app, ctx = _server_app(broker)
    result = asyncio.run(app._resolve_submission("default", None, ctx,
                                                  _resolution_payload()))
    assert result["status"] == "submit_unknown"
    assert broker.resolver_calls == 0 and broker.order_calls == 1


def test_cli_enable_trading_is_explicit_for_api_and_actor(tmp_path, monkeypatch):
    cli = importlib.import_module("bullet_trade.integrations.ths.__main__")
    actor_calls = []
    api_calls = []
    monkeypatch.setattr(cli, "run_actor", lambda *args, **kwargs: actor_calls.append(kwargs))

    class FakeServer:
        def __init__(self, address, application, token):
            api_calls.append((address, application, token))

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def serve_forever(self, **kwargs):
            return None

    monkeypatch.setattr(cli, "LocalServer", FakeServer)
    monkeypatch.setenv("THS_SERVICE_TOKEN", TOKEN)
    common = ["--state-dir", str(tmp_path), "--account", ACCOUNT]
    cli.main(common + ["--actor", "--driver-factory", "synthetic:make"])
    assert actor_calls[-1]["enable_trading"] is False
    cli.main(common + ["--actor", "--driver-factory", "synthetic:make",
                       "--enable-trading"])
    assert actor_calls[-1]["enable_trading"] is True
    cli.main(common)
    assert api_calls[-1][1].allow_requests is False
    cli.main(common + ["--enable-trading"])
    assert api_calls[-1][1].allow_requests is True
