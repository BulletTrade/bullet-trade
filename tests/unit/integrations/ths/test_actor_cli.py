"""Offline actor process smoke test with an explicitly supplied synthetic factory."""

import os
from datetime import date
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

import pytest

from bullet_trade.integrations.ths import __main__ as ths_cli
from bullet_trade.integrations.ths.snapshot_store import SnapshotStore
from bullet_trade.integrations.ths.request_store import RequestStore


@pytest.mark.parametrize("option, expected", [([], 120), (["--snapshot-max-age", "180"], 180)])
def test_cli_passes_actor_snapshot_age(monkeypatch, option, expected):
    received = []
    monkeypatch.setattr(ths_cli, "run_actor", lambda *args, **kwargs: received.append(kwargs))
    ths_cli.main(["--actor", "--state-dir", "synthetic-state", "--account", "paper",
                  "--driver-factory", "synthetic:make_driver", *option])
    assert received[0]["snapshot_max_age_seconds"] == expected


@pytest.mark.parametrize("age", [120, 180])
def test_run_actor_passes_snapshot_age_to_service(monkeypatch, tmp_path, age):
    received = []
    class Driver:
        client_lock_factory = staticmethod(lambda: None)

    class FakeActor:
        def __init__(self, *args, **kwargs):
            received.append(kwargs["snapshot_max_age_seconds"])

        def run(self, stop):
            pass

    monkeypatch.setattr(ths_cli, "_load_driver", lambda *args, **kwargs: Driver())
    monkeypatch.setattr(ths_cli, "ActorService", FakeActor)
    kwargs = {} if age == 120 else {"snapshot_max_age_seconds": age}
    ths_cli.run_actor(tmp_path / "state", "paper", "synthetic:make_driver",
                      stop=threading.Event(), **kwargs)
    assert received == [age]


@pytest.mark.parametrize("age", [float("nan"), float("inf"), -1])
def test_run_actor_rejects_bad_snapshot_age_before_driver(monkeypatch, tmp_path, age):
    def forbidden_driver(*args, **kwargs):
        raise AssertionError("driver must not be initialized")

    monkeypatch.setattr(ths_cli, "_load_driver", forbidden_driver)
    state_dir = tmp_path / "state"
    with pytest.raises(ValueError, match="snapshot_max_age_seconds"):
        ths_cli.run_actor(state_dir, "paper", "synthetic:make_driver",
                          snapshot_max_age_seconds=age)
    assert not state_dir.exists()


def test_actor_cli_owns_state_and_stops_cleanly(tmp_path):
    factory_dir = tmp_path / "factory"
    factory_dir.mkdir()
    (factory_dir / "synthetic_ths_driver.py").write_text(
        "from contextlib import nullcontext\n"
        "from datetime import datetime, timezone\n"
        "from pathlib import Path\n"
        "from bullet_trade.integrations.ths.actor_service import CollectedSnapshot\n"
        "class Driver:\n"
        "    client_lock_factory = staticmethod(nullcontext)\n"
        "    def __init__(self, account, state_dir):\n"
        "        self.account, self.state_dir = account, Path(state_dir)\n"
        "    def query(self, kind, should_yield):\n"
        "        data = {'available_cash': '1', 'total_value': '1'} if kind == 'account' else []\n"
        "        return CollectedSnapshot(self.account, kind, data, datetime.now(timezone.utc), True)\n"
        "    def close(self):\n"
        "        (self.state_dir / 'closed').write_text('yes')\n"
        "def make_driver(*, account, state_dir):\n"
        "    marker = Path(state_dir) / 'constructed'\n"
        "    marker.write_text(marker.read_text() + 'x' if marker.exists() else 'x')\n"
        "    return Driver(account, state_dir)\n",
        encoding="utf-8",
    )
    state_dir = tmp_path / "state"
    requests = RequestStore(state_dir / "requests.sqlite3")
    queued = requests.enqueue(
        "paper", date.today().isoformat(), "virtual-a:queued", "limit_buy",
        {"security": "600000.XSHG", "quantity": 100, "price": "10.00"},
        time.time() + 60, origin={"virtual_account_id": "virtual-a"},
    )
    repo = Path(__file__).parents[4]
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join((str(factory_dir), str(repo), env.get("PYTHONPATH", "")))
    command = [sys.executable, "-m", "bullet_trade.integrations.ths", "--actor",
               "--state-dir", str(state_dir), "--account", "paper",
               "--driver-factory", "synthetic_ths_driver:make_driver"]
    process = subprocess.Popen(command, cwd=repo, env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 8
        row = None
        while time.monotonic() < deadline:
            if process.poll() is not None:
                break
            if (state_dir / "snapshots.sqlite3").exists():
                try:
                    row = SnapshotStore(state_dir / "snapshots.sqlite3").read("paper", "positions")
                    if row["version"] > 0:
                        break
                except Exception:
                    pass
            time.sleep(0.05)
        assert process.poll() is None
        assert row is not None and row["version"] > 0
        assert row["data"] == []
        assert RequestStore(state_dir / "requests.sqlite3").get(queued.request_id).state == "queued"
        assert (state_dir / "constructed").read_text() == "x"

        duplicate = subprocess.run(command, cwd=repo, env=env, text=True,
                                   capture_output=True, timeout=5)
        assert duplicate.returncode != 0
        assert "actor_already_running" in duplicate.stderr
        assert (state_dir / "constructed").read_text() == "x"
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
        _, stderr = process.communicate(timeout=5)
    assert process.returncode == 0, stderr
    assert (state_dir / "closed").read_text() == "yes"
    assert RequestStore(state_dir / "requests.sqlite3").get(queued.request_id).state == "queued"
