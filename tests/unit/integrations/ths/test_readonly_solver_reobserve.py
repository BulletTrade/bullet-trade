"""Bounded, read-only recovery of a briefly hidden original query window."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from bullet_trade.integrations.ths.windows.native.readonly_sampler import SamplingError
from bullet_trade.integrations.ths.windows.native import readonly_solver


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        assert 0 < seconds <= 0.1
        self.now += seconds


class Adapter:
    def __init__(self, *, mode="recover"):
        self.mode = mode
        self.copied = 0
        self.detected = 0
        self.located = 0
        self.page_checks = 0
        self.post_confirm_quiet_seconds = 0
        self.confirmed = 0
        self.edit = ""
        self.post_calls = 0
        self.hidden_polls = 0
        self.owner_calls = 0
        self.visible = True
        self.window_exists = True
        self.pid = 500
        self._main_handle = 100
        self.app = SimpleNamespace(process=500)
        self.gui = SimpleNamespace(
            IsWindow=lambda hwnd: hwnd == 100 and self.window_exists,
            IsWindowVisible=lambda hwnd: hwnd == 100 and self.visible,
            GetWindowText=lambda hwnd: "网上股票交易系统5.0")
        self.process = SimpleNamespace(
            GetWindowThreadProcessId=lambda hwnd: (0, self.pid))
        if mode.startswith("native_"):
            self.post_confirm_quiet_seconds = 3.5 if mode == "native_repeat" else 0.2

    def locate(self, *, known_dialog_handles=frozenset()):
        assert known_dialog_handles in (frozenset(), frozenset({77}))
        self.located += 1
        if self.mode.startswith("native_") and self.confirmed:
            if self.mode in {"native_account_changed", "native_instant_account_changed"}:
                raise SamplingError("simulation_account_unverified")
            if self.mode in {"native_auxiliary", "native_instant_auxiliary"}:
                self.visible = True
            elif self.mode == "native_different_window":
                self.visible = True
                return 101, 200
            elif not self.visible:
                self.hidden_polls += 1
                if self.hidden_polls % 2:
                    raise SamplingError("simulation_window_missing")
                self.visible = True
        if self.copied:
            if self.mode == "always_hidden":
                raise SamplingError("simulation_window_missing")
            if self.mode == "account_changed":
                raise SamplingError("simulation_account_unverified")
            if self.mode == "different_window":
                return 101, 200
            if (self.located == 2 and self.mode not in
                    {"identified_hidden", "submitted_hidden"}
                    and not self.mode.startswith("native_")):
                raise SamplingError("simulation_window_missing")
        return 100, 200

    def verify_page(self, kind):
        assert kind == "holdings"
        self.page_checks += 1

    def captcha_dialog(self):
        if not self.copied:
            return None
        self.detected += 1
        if self.mode == "other_error":
            raise SamplingError("captcha_owner_mismatch")
        if self.mode == "none_then_hidden":
            if self.detected == 2:
                raise SamplingError("main_window_not_bound")
            return None
        if self.mode == "repeat_hidden":
            if self.detected % 2 == 0:
                raise SamplingError("main_window_not_bound")
            return None
        if self.mode == "identified_hidden":
            if self.detected == 2:
                raise SamplingError("main_window_not_bound")
            return 77
        if self.mode == "submitted_hidden":
            if self.confirmed:
                if self.detected >= 5:
                    raise SamplingError("main_window_not_bound")
                return None
            return 77
        if self.mode.startswith("native_"):
            if self.confirmed:
                self.post_calls += 1
                if (self.post_calls == 1 or
                        self.mode == "native_repeat" and self.post_calls % 2):
                    self.visible = self.mode.startswith("native_instant")
                    if self.mode == "native_destroyed":
                        self.window_exists = False
                    if self.mode == "native_pid_changed":
                        self.pid = 501
                    raise SamplingError("main_window_not_bound")
                return 78 if self.mode == "native_replacement" else None
            return 77
        if self.detected == 1:
            raise SamplingError("main_window_not_bound")
        return None

    def dialog_fingerprint(self, main):
        assert main == 100
        return frozenset()

    def clipboard_sequence(self):
        if self.mode in {"none_then_hidden", "repeat_hidden"}:
            return 2 if self.mode == "none_then_hidden" and self.detected >= 3 else 1
        if self.mode in {"identified_hidden", "submitted_hidden"}:
            return 1
        if self.mode.startswith("native_"):
            if self.confirmed and self.mode in {
                    "native_recover", "native_recover_unowned", "native_instant"}:
                return 2
            if self.confirmed and self.mode == "native_recover_pending":
                return 2 if self.post_calls >= 3 else 1
            return 1
        return 2 if self.copied else 1

    def copy_once(self, grid):
        assert grid == 200
        self.copied += 1

    def clipboard_from_client(self, main):
        self.owner_calls += 1
        if self.mode == "native_recover_pending" and self.owner_calls == 1:
            return False
        if self.mode == "native_recover_unowned":
            return False
        return main == 100

    def capture_image(self, hwnd, path):
        assert hwnd == 77
        path.write_bytes(b"one-captcha")

    def read_captcha_edit(self, hwnd):
        assert hwnd == 77
        return self.edit

    def write_captcha_edit(self, hwnd, value):
        assert hwnd == 77
        self.edit = value

    def confirm_captcha(self, hwnd):
        assert hwnd == 77
        self.confirmed += 1

    def post_confirm_dialog_fingerprint(self, main, captcha):
        assert main == 100 and captcha in (None, 77)
        return (frozenset({(999, 0, 0, "unknown")})
                if self.mode in {"native_auxiliary", "native_instant_auxiliary"}
                else frozenset())


def _run(adapter, clock, tmp_path, monkeypatch):
    monkeypatch.setattr(
        readonly_solver, "read_stable_clipboard",
        lambda *args, **kwargs: (
            "证券代码\t股票余额\t可用余额\t冻结数量\n"
            "600001\t100\t100\t0\n"))
    return readonly_solver._solve_once_locked(
        adapter, SimpleNamespace(recognize=lambda _: "1234"), "holdings", tmp_path,
        timeout=5, poll_interval=0.1, settle=0,
        monotonic=clock.monotonic, sleep=clock.sleep,
        process_lock_held=True)


def test_briefly_hidden_original_window_reobserved_without_second_copy(tmp_path, monkeypatch):
    adapter, clock = Adapter(), Clock()
    result = _run(adapter, clock, tmp_path, monkeypatch)
    assert result["status"] == "new_table_no_captcha"
    assert "main_reobserved" in result["phase_seconds"]
    assert adapter.page_checks >= 2
    assert adapter.copied == 1
    assert adapter.detected == 2
    assert clock.now <= 1.0


def test_first_poll_none_then_hidden_main_reobserved_once(tmp_path, monkeypatch):
    adapter, clock = Adapter(mode="none_then_hidden"), Clock()
    result = _run(adapter, clock, tmp_path, monkeypatch)
    assert result["status"] == "new_table_no_captcha"
    assert "main_reobserved" in result["phase_seconds"]
    assert adapter.detected == 3
    assert adapter.copied == 1


def test_all_prechallenge_polls_share_one_absolute_deadline(tmp_path, monkeypatch):
    adapter, clock = Adapter(mode="repeat_hidden"), Clock()
    observed_deadlines = []
    original = readonly_solver._initial_copy_dialog_observation

    def checked(*args, **kwargs):
        observed_deadlines.append(kwargs["deadline"])
        return original(*args, **kwargs)

    monkeypatch.setattr(readonly_solver, "_initial_copy_dialog_observation", checked)
    result = _run(adapter, clock, tmp_path, monkeypatch)
    assert result["reason"] == "main_window_not_bound"
    assert len(observed_deadlines) > 1
    assert set(observed_deadlines) == {3.0}
    assert clock.now <= 3.0 + 1e-9
    assert adapter.copied == 1


@pytest.mark.parametrize("mode,expected_confirmed", [
    ("identified_hidden", 0), ("submitted_hidden", 1),
])
def test_hidden_after_identification_or_submission_stops_without_reobserve(
        mode, expected_confirmed, tmp_path, monkeypatch):
    adapter, clock = Adapter(mode=mode), Clock()
    helper_calls = []
    original = readonly_solver._initial_copy_dialog_observation

    def checked(*args, **kwargs):
        helper_calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(readonly_solver, "_initial_copy_dialog_observation", checked)
    result = _run(adapter, clock, tmp_path, monkeypatch)
    assert result["reason"] == "main_window_not_bound"
    assert len(helper_calls) == 1
    assert adapter.confirmed == expected_confirmed
    assert adapter.copied == 1


def test_native_hidden_after_confirm_reobserves_and_keeps_one_submission(
        tmp_path, monkeypatch):
    monkeypatch.setattr(readonly_solver, "SolverWindowsAdapter", Adapter)
    adapter, clock = Adapter(mode="native_recover"), Clock()
    result = _run(adapter, clock, tmp_path, monkeypatch)
    assert result["status"] == "client_accepted"
    assert "post_confirm_main_reobserved" in result["phase_seconds"]
    assert result["post_confirm_reobserved_sequence"] == 2
    assert result["post_confirm_reobserved_clipboard_from_client"] is True
    assert adapter.copied == adapter.confirmed == 1


def test_native_reappears_before_hidden_state_check_and_reverifies(tmp_path, monkeypatch):
    monkeypatch.setattr(readonly_solver, "SolverWindowsAdapter", Adapter)
    adapter, clock = Adapter(mode="native_instant"), Clock()
    result = _run(adapter, clock, tmp_path, monkeypatch)
    assert result["status"] == "client_accepted"
    assert "post_confirm_main_reobserved" in result["phase_seconds"]
    assert adapter.page_checks >= 2
    assert adapter.copied == adapter.confirmed == 1


@pytest.mark.parametrize("mode,reason", [
    ("native_instant_account_changed", "simulation_account_unverified"),
    ("native_instant_auxiliary", "post_submit_dialog_changed"),
])
def test_native_instant_reappearance_still_rejects_drift(
        mode, reason, tmp_path, monkeypatch):
    monkeypatch.setattr(readonly_solver, "SolverWindowsAdapter", Adapter)
    adapter, clock = Adapter(mode=mode), Clock()
    result = _run(adapter, clock, tmp_path, monkeypatch)
    assert result["reason"] == reason
    assert adapter.copied == adapter.confirmed == 1


def test_native_recovery_waits_for_fresh_clipboard_attribution(tmp_path, monkeypatch):
    monkeypatch.setattr(readonly_solver, "SolverWindowsAdapter", Adapter)
    adapter, clock = Adapter(mode="native_recover_pending"), Clock()
    result = _run(adapter, clock, tmp_path, monkeypatch)
    assert result["status"] == "client_accepted"
    assert result["post_confirm_reobserved_sequence"] == 1
    assert result["post_confirm_reobserved_clipboard_from_client"] is False
    assert adapter.owner_calls >= 2
    assert adapter.copied == adapter.confirmed == 1


def test_native_fresh_table_with_wrong_clipboard_owner_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(readonly_solver, "SolverWindowsAdapter", Adapter)
    adapter, clock = Adapter(mode="native_recover_unowned"), Clock()
    result = _run(adapter, clock, tmp_path, monkeypatch)
    assert result["status"] != "client_accepted"
    assert result["post_confirm_reobserved_clipboard_from_client"] is False
    assert result["clipboard_sequence_after"] == 2
    assert adapter.owner_calls >= 2
    assert adapter.copied == adapter.confirmed == 1


def test_native_repeated_hidden_uses_fixed_confirm_deadline(tmp_path, monkeypatch):
    monkeypatch.setattr(readonly_solver, "SolverWindowsAdapter", Adapter)
    adapter, clock = Adapter(mode="native_repeat"), Clock()
    deadlines = []
    original = readonly_solver._post_confirm_dialog_observation

    def checked(*args, **kwargs):
        deadlines.append(kwargs["deadline"])
        return original(*args, **kwargs)

    monkeypatch.setattr(readonly_solver, "_post_confirm_dialog_observation", checked)
    result = _run(adapter, clock, tmp_path, monkeypatch)
    assert result["reason"] == "main_window_not_bound"
    assert len(deadlines) > 1 and set(deadlines) == {3.0}
    assert clock.now <= 3.0 + 1e-9
    assert adapter.copied == adapter.confirmed == 1


@pytest.mark.parametrize("mode,reason", [
    ("native_destroyed", "query_identity_changed"),
    ("native_pid_changed", "query_identity_changed"),
    ("native_account_changed", "simulation_account_unverified"),
    ("native_different_window", "query_identity_changed"),
    ("native_auxiliary", "post_submit_dialog_changed"),
    ("native_replacement", "captcha_identity_changed"),
])
def test_native_post_confirm_drift_or_replacement_stops(
        mode, reason, tmp_path, monkeypatch):
    monkeypatch.setattr(readonly_solver, "SolverWindowsAdapter", Adapter)
    adapter, clock = Adapter(mode=mode), Clock()
    result = _run(adapter, clock, tmp_path, monkeypatch)
    assert result["status"] == "error"
    assert result["reason"] == reason
    assert adapter.copied == adapter.confirmed == 1


@pytest.mark.parametrize("mode,reason", [
    ("different_window", "query_identity_changed"),
    ("account_changed", "simulation_account_unverified"),
    ("other_error", "captcha_owner_mismatch"),
    ("always_hidden", "main_window_not_bound"),
])
def test_changed_identity_other_error_or_deadline_never_retries_copy(
        mode, reason, tmp_path, monkeypatch):
    adapter, clock = Adapter(mode=mode), Clock()
    result = _run(adapter, clock, tmp_path, monkeypatch)
    assert result["status"] == "error"
    assert result["reason"] == reason
    assert adapter.copied == 1
    assert "main_reobserved" not in result["phase_seconds"]
    if mode == "other_error":
        assert adapter.located == 1
    if mode == "always_hidden":
        assert clock.now <= 1.0


def test_reobservation_obeys_remaining_original_attempt_deadline(tmp_path, monkeypatch):
    adapter, clock = Adapter(mode="always_hidden"), Clock()
    monkeypatch.setattr(readonly_solver, "read_stable_clipboard",
                        lambda *args, **kwargs: pytest.fail("clipboard read after timeout"))
    result = readonly_solver._solve_once_locked(
        adapter, SimpleNamespace(), "holdings", tmp_path,
        timeout=0.25, poll_interval=0.1, settle=0,
        monotonic=clock.monotonic, sleep=clock.sleep,
        process_lock_held=True)
    assert result["reason"] == "main_window_not_bound"
    assert clock.now <= 0.25
    assert adapter.copied == 1
