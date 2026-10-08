"""Run the loopback API or a separately owned interactive-desktop GUI actor."""
import argparse
import importlib
import os
from pathlib import Path
import signal
import threading
import math

from filelock import FileLock, Timeout

from .actor_service import ActorService
from .http_service import LocalServer, ServiceApplication
from .request_store import RequestStore
from .snapshot_store import SnapshotStore


def _load_driver(factory_ref, *, account, state_dir):
    module_name, separator, factory_name = factory_ref.partition(":")
    if not separator or not module_name or not factory_name or ":" in factory_name:
        raise ValueError("driver factory must be module:function")
    factory = getattr(importlib.import_module(module_name), factory_name)
    if not callable(factory):
        raise TypeError("driver factory must be callable")
    driver = factory(account=account, state_dir=state_dir)
    if not callable(getattr(driver, "query", None)):
        raise TypeError("driver.query is required")
    if not callable(getattr(driver, "client_lock_factory", None)):
        raise TypeError("driver.client_lock_factory is required")
    return driver


def run_actor(state_dir, account, driver_factory_ref, *, interval=20, stop=None,
              enable_trading=False, snapshot_max_age_seconds=120):
    """Run one actor in the caller's interactive process (read-only by default).

    A driver factory is an explicit trust decision; this entry point does not
    qualify its account, GUI session, snapshots, or trading profile.
    """
    if not math.isfinite(interval) or interval <= 0:
        raise ValueError("interval must be positive and finite")
    if (not math.isfinite(snapshot_max_age_seconds)
            or snapshot_max_age_seconds < 0):
        raise ValueError("snapshot_max_age_seconds must be finite and nonnegative")
    directory = Path(state_dir)
    directory.mkdir(parents=True, exist_ok=True)
    owner = FileLock(str(directory / "actor-owner.lock"))
    try:
        with owner.acquire(timeout=0):
            driver = _load_driver(driver_factory_ref, account=account, state_dir=directory)
            try:
                actor = ActorService(
                    RequestStore(directory / "requests.sqlite3"),
                    SnapshotStore(directory / "snapshots.sqlite3"), driver,
                    account=account, lock_path=directory / "gui-actor.lock",
                    interval=interval, client_lock_factory=driver.client_lock_factory,
                    write_enabled=enable_trading,
                    snapshot_max_age_seconds=snapshot_max_age_seconds,
                )
                own_stop = stop is None
                if own_stop:
                    stop = threading.Event()
                    previous = {}
                    for signum in (signal.SIGINT, signal.SIGTERM):
                        previous[signum] = signal.getsignal(signum)
                        signal.signal(signum, lambda *_: stop.set())
                try:
                    actor.run(stop)
                finally:
                    if own_stop:
                        for signum, handler in previous.items():
                            signal.signal(signum, handler)
            finally:
                close = getattr(driver, "close", None)
                if callable(close):
                    close()
    except Timeout as exc:
        raise RuntimeError("actor_already_running") from exc


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--actor", action="store_true", help="run only the GUI actor")
    parser.add_argument("--driver-factory", help="trusted local module:function for actor")
    parser.add_argument("--interval", type=float, default=20)
    parser.add_argument("--port", type=int, default=18765)
    parser.add_argument("--max-age", type=float, default=60)
    parser.add_argument("--snapshot-max-age", type=float, default=120,
                        help="actor snapshot freshness limit in seconds (default: 120)")
    parser.add_argument("--enable-trading", action="store_true",
                        help="enable durable request intake or actor execution; both processes need this flag")
    args = parser.parse_args(argv)
    if args.actor:
        if not args.driver_factory:
            parser.error("--actor requires --driver-factory")
        run_actor(args.state_dir, args.account, args.driver_factory,
                  interval=args.interval, enable_trading=args.enable_trading,
                  snapshot_max_age_seconds=args.snapshot_max_age)
        return
    if args.driver_factory:
        parser.error("--driver-factory requires --actor")
    token = os.environ.get("THS_SERVICE_TOKEN", "")
    application = ServiceApplication(args.state_dir, account=args.account,
                                     max_age_seconds=args.max_age,
                                     allow_requests=args.enable_trading)
    with LocalServer(("127.0.0.1", args.port), application, token) as server:
        try:
            server.serve_forever(poll_interval=0.25)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
