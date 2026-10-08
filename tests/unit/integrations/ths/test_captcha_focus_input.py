"""No Win32 calls: verify the captcha keyboard boundary and failure stops."""

import ctypes
from types import SimpleNamespace
import pytest
import bullet_trade.integrations.ths.windows.native.captcha_focus_input as focus_module
from bullet_trade.integrations.ths.windows.driver import _safe_captcha_diagnostics

from bullet_trade.integrations.ths.windows.native.captcha_focus_input import (
    WindowsInputBackend, write_captcha_with_focus,
)


class Backend:
    def __init__(self):
        self.events = []
        self.attached = False
        self.detach_ok = True
        self.focus_hwnd = 101
        self.foreground_window = 202
        self.modifiers_clear = True
        self.dialog_current = True
        self.sent = 0
        self.typed = ""

    def observe(self, name):
        assert not self.attached, f"{name} while attached"
        self.events.append(name)

    def window_info(self, hwnd):
        self.observe("window_info")
        return {"class_name": "Edit" if hwnd == 101 else "#32770",
                "control_id": 2404 if hwnd == 101 else 0,
                "parent": 202 if hwnd == 101 else 0,
                "pid": 303, "thread_id": 404,
                "visible": True, "enabled": True}

    def text_length(self, _hwnd):
        self.observe("text_length")
        return 0

    def current_thread_id(self):
        self.observe("current_thread_id")
        return 505

    def attach(self, _caller, _target, enabled):
        self.events.append("attach" if enabled else "detach")
        if enabled:
            self.attached = True
            return True, 0
        self.attached = not self.detach_ok
        return self.detach_ok, 0 if self.detach_ok else 5

    def foreground(self, _hwnd):
        self.events.append("request_foreground")
        return True

    def focus(self, _hwnd):
        self.events.append("request_focus")

    def gui_focus(self, _target):
        self.observe("gui_focus")
        return self.focus_hwnd, 0

    def foreground_hwnd(self):
        self.observe("foreground_hwnd")
        return self.foreground_window

    def keyboard_modifiers_clear(self):
        self.observe("modifiers")
        return self.modifiers_clear

    def send_digit(self, _digit):
        self.observe("send_digit")
        self.sent += 1
        self.typed += _digit
        return 2, 0

    def read_prefix(self, _hwnd, timeout_ms):
        self.observe("read_prefix")
        assert 1 <= timeout_ms <= 500
        return self.typed

    def release_digit(self, _digit):
        self.observe("release_digit")
        return 1, 0

    def pause(self):
        self.observe("pause")


def run(backend):
    def current(hwnd):
        backend.observe("is_current")
        return hwnd == 202 and backend.dialog_current
    diagnostic = {}
    write_captcha_with_focus(101, 202, 303, "1234",
                             is_current=current, diagnostic=diagnostic,
                             backend=backend)
    return diagnostic


@pytest.fixture(autouse=True)
def no_real_wait(monkeypatch):
    monkeypatch.setattr(focus_module.time, "sleep", lambda _: None)


def test_attach_only_requests_foreground_and_focus_then_detaches_before_observation():
    backend = Backend()
    diagnostic = run(backend)
    start = backend.events.index("attach")
    end = backend.events.index("detach")
    assert backend.events[start + 1:end] == ["request_foreground", "request_focus"]
    assert backend.events.index("send_digit") > end
    assert backend.sent == 4 and diagnostic["sent_events"] == 8
    assert diagnostic["detached"] is True
    assert diagnostic["foreground_matches_dialog"] is True
    assert diagnostic["modifiers_clear"] is True


@pytest.mark.parametrize("field,value,reason", [
    ("detach_ok", False, "input_detach_failed"),
    ("focus_hwnd", 999, "focus_not_edit"),
    ("foreground_window", 999, "dialog_not_foreground"),
    ("modifiers_clear", False, "keyboard_modifiers_active"),
    ("dialog_current", False, "dialog_changed"),
])
def test_invalid_post_detach_state_never_sends(field, value, reason):
    backend = Backend()
    setattr(backend, field, value)
    with pytest.raises(RuntimeError, match=reason):
        run(backend)
    assert backend.sent == 0


def test_identity_change_between_digits_stops_next_key():
    backend = Backend()
    original = backend.send_digit

    def change_after_first(digit):
        result = original(digit)
        backend.dialog_current = False
        return result

    backend.send_digit = change_after_first
    with pytest.raises(RuntimeError, match="dialog_changed"):
        run(backend)
    assert backend.sent == 1


def test_modifier_change_between_digits_stops_next_key():
    backend = Backend()
    original = backend.send_digit

    def change_after_first(digit):
        result = original(digit)
        backend.modifiers_clear = False
        return result

    backend.send_digit = change_after_first
    with pytest.raises(RuntimeError, match="keyboard_modifiers_active"):
        run(backend)
    assert backend.sent == 1


def test_partial_send_releases_its_key_without_submitting_more_digits():
    backend = Backend()

    def partial(_digit):
        backend.observe("send_digit")
        backend.sent += 1
        return 1, 5

    backend.send_digit = partial
    with pytest.raises(RuntimeError, match="sendinput_incomplete"):
        run(backend)
    assert backend.sent == 1
    assert backend.events.count("release_digit") == 1


def test_new_focus_diagnostics_keep_only_bounded_scalars():
    safe = _safe_captcha_diagnostics({}, {
        "foreground_matches_dialog": False, "modifiers_clear": True,
        "key_released": True, "release_error": 5,
        "captcha": "1234", "dialog_text": "sensitive"})
    assert safe == {"focus_input_diagnostic": {
        "foreground_matches_dialog": False, "modifiers_clear": True,
        "key_released": True, "release_error": 5}}


class InputRecord:
    def __init__(self):
        self.kind = 0
        self.value = SimpleNamespace(key=SimpleNamespace(vk=0, flags=0))


class InputFactory:
    def __mul__(self, count):
        return lambda: [InputRecord() for _ in range(count)]


def native_fake(results):
    events = []
    answers = iter(results)

    def send(count, keys, _size):
        assert count == 1
        events.append((keys[0].value.key.vk, keys[0].value.key.flags))
        result = next(answers)
        if isinstance(result, Exception):
            raise result
        return result

    backend = SimpleNamespace(
        Input=InputFactory(),
        ctypes=SimpleNamespace(set_last_error=lambda _: None,
                               get_last_error=lambda: 0,
                               sizeof=lambda _: 8),
        user=SimpleNamespace(SendInput=send))
    return backend, events


def test_native_digit_has_one_keydown_50ms_hold_and_final_keyup(monkeypatch):
    backend, events = native_fake((1, 1))
    monkeypatch.setattr(focus_module.time, "sleep",
                        lambda seconds: events.append(("hold", seconds)))
    assert WindowsInputBackend.send_digit(backend, "7") == (2, 0)
    assert events == [(ord("7"), 0), ("hold", 0.05), (ord("7"), 2)]


@pytest.mark.parametrize("results,reason", [
    ((0, 1), "captcha_key_down_or_hold_failed"),
    ((1, 0), "captcha_key_release_failed"),
    ((OSError("transport"), 1), "captcha_key_down_or_hold_failed"),
])
def test_native_digit_failure_still_attempts_final_keyup(monkeypatch, results, reason):
    backend, events = native_fake(results)
    monkeypatch.setattr(focus_module.time, "sleep", lambda _: None)
    with pytest.raises(RuntimeError, match=reason):
        WindowsInputBackend.send_digit(backend, "1")
    assert events[-1] == (ord("1"), 2)
    assert events.count((ord("1"), 0)) == 1


def test_native_digit_interrupted_hold_still_releases(monkeypatch):
    backend, events = native_fake((1, 1))

    def interrupted(_seconds):
        raise RuntimeError("interrupted")

    monkeypatch.setattr(focus_module.time, "sleep", interrupted)
    with pytest.raises(RuntimeError, match="captcha_key_down_or_hold_failed"):
        WindowsInputBackend.send_digit(backend, "2")
    assert events == [(ord("2"), 0), (ord("2"), 2)]


def test_each_digit_has_exact_verified_prefix_before_next_send():
    backend = Backend()
    run(backend)
    events = backend.events
    assert events.count("send_digit") == 4
    for left, right in zip([index for index, event in enumerate(events)
                            if event == "send_digit"],
                           [index for index, event in enumerate(events)
                            if event == "read_prefix"]):
        assert left < right


def test_wrong_prefix_stops_without_resending_digit():
    backend = Backend()
    backend.read_prefix = lambda _hwnd, _timeout: "9"
    with pytest.raises(RuntimeError, match="captcha_prefix_mismatch"):
        run(backend)
    assert backend.sent == 1


def test_missing_prefix_stops_without_resending_digit():
    backend = Backend()
    backend.read_prefix = lambda _hwnd, _timeout: ""
    with pytest.raises(RuntimeError, match="captcha_prefix_missing"):
        run(backend)
    assert backend.sent == 1


@pytest.mark.parametrize("wide", [True, False])
def test_native_prefix_reader_selects_a_w_and_caps_at_four(wide, monkeypatch):
    monkeypatch.setattr(ctypes, "set_last_error", lambda _value: None, raising=False)
    called = []

    def sender(name):
        def send(hwnd, message, line, pointer, flags, timeout, output):
            called.append(name)
            assert (hwnd, message, line, flags) == (101, 0x00C4, 0, 2)
            assert 1 <= timeout <= 500
            assert ctypes.c_ushort.from_address(pointer).value == 4
            payload = "1234".encode("utf-16-le" if wide else "ascii")
            ctypes.memmove(pointer, payload, len(payload))
            output._obj.value = 4
            return 1
        return send

    backend = SimpleNamespace(
        ctypes=ctypes,
        user=SimpleNamespace(IsWindowUnicode=lambda _hwnd: wide,
                             SendMessageTimeoutA=sender("A"),
                             SendMessageTimeoutW=sender("W")))
    assert WindowsInputBackend.read_prefix(backend, 101, 500) == "1234"
    assert called == ["W" if wide else "A"]


def test_native_prefix_reader_rejects_count_above_four(monkeypatch):
    monkeypatch.setattr(ctypes, "set_last_error", lambda _value: None, raising=False)

    def send(_hwnd, _message, _line, _pointer, _flags, _timeout, output):
        output._obj.value = 5
        return 1

    backend = SimpleNamespace(
        ctypes=ctypes,
        user=SimpleNamespace(IsWindowUnicode=lambda _hwnd: False,
                             SendMessageTimeoutA=send,
                             SendMessageTimeoutW=send))
    with pytest.raises(RuntimeError, match="captcha_prefix_count_invalid"):
        WindowsInputBackend.read_prefix(backend, 101, 500)
