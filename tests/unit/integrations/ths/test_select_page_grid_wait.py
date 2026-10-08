"""A disappearing query grid may be brief; identity and ambiguity still stop."""

from types import SimpleNamespace

import pytest

from bullet_trade.integrations.ths.windows.native import readonly_solver
from bullet_trade.integrations.ths.windows.native.readonly_sampler import SamplingError


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        assert 0 < seconds <= 0.1
        self.now += seconds


def adapter_for_counts(counts, *, identity_change=False, account_change=False,
                       unknown_dialog=False):
    adapter = readonly_solver.SolverWindowsAdapter.__new__(
        readonly_solver.SolverWindowsAdapter)
    state = SimpleNamespace(selected=False, polls=0, locations=0,
                            dialog_checks=0, verified=0)
    grid_counts = iter(counts)
    last_count = counts[-1]

    def descendants():
        state.polls += 1
        count = next(grid_counts, last_count)
        return [SimpleNamespace(handle=200 + n, control_id=lambda: 1047,
                                class_name=lambda: 'CVirtualGridCtrl',
                                is_visible=lambda: True)
                for n in range(count)]

    main = SimpleNamespace(handle=100, descendants=descendants)
    adapter.app = SimpleNamespace(window=lambda **kwargs: main)

    def locate_main(*, known_dialog_handles=frozenset()):
        state.locations += 1
        if state.selected and account_change:
            raise SamplingError('simulation_account_unverified')
        adapter._main_handle = 101 if state.selected and identity_change else 100
        return adapter._main_handle

    def dialog_fingerprint(_main):
        state.dialog_checks += 1
        if state.selected and unknown_dialog:
            raise SamplingError('unrecognized_auxiliary_dialog')
        return frozenset()

    adapter.locate_main = locate_main
    adapter.dialog_fingerprint = dialog_fingerprint
    adapter._cursor_input_ready = lambda: True
    adapter._visible_query_tree = lambda _main: SimpleNamespace(
        get_item=lambda _path: SimpleNamespace(select=lambda: setattr(state, 'selected', True)))
    adapter.verify_page = lambda _kind: setattr(state, 'verified', state.verified + 1)
    return adapter, state


def test_select_page_waits_for_zero_grid_then_verifies_page(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(readonly_solver, 'time', SimpleNamespace(
        monotonic=clock.monotonic, sleep=clock.sleep))
    adapter, state = adapter_for_counts([1, 0, 0, 1])

    adapter.select_page('cancelable')

    assert state.polls == 5  # select_page performs a final location check.
    assert state.verified == 1
    assert adapter._last_query_grid_count == 1
    assert clock.now == pytest.approx(0.2)


def test_select_page_stops_after_three_seconds_of_zero_grid(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(readonly_solver, 'time', SimpleNamespace(
        monotonic=clock.monotonic, sleep=clock.sleep))
    adapter, state = adapter_for_counts([1, 0])

    with pytest.raises(SamplingError, match='select_after_tree:visible_query_grid_not_unique'):
        adapter.select_page('cancelable')

    assert clock.now == pytest.approx(3.0)
    assert state.verified == 0
    assert adapter._last_query_grid_count == 0


def test_select_page_rejects_double_grid_without_wait(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(readonly_solver, 'time', SimpleNamespace(
        monotonic=clock.monotonic, sleep=clock.sleep))
    adapter, state = adapter_for_counts([1, 2])

    with pytest.raises(SamplingError, match='select_after_tree:visible_query_grid_not_unique'):
        adapter.select_page('cancelable')

    assert clock.now == 0
    assert state.verified == 0
    assert adapter._last_query_grid_count == 2


@pytest.mark.parametrize('change,reason', [
    ('identity_change', 'query_identity_changed'),
    ('account_change', 'simulation_account_unverified'),
    ('unknown_dialog', 'unrecognized_auxiliary_dialog'),
])
def test_select_page_stops_on_identity_or_dialog_change(monkeypatch, change, reason):
    clock = Clock()
    monkeypatch.setattr(readonly_solver, 'time', SimpleNamespace(
        monotonic=clock.monotonic, sleep=clock.sleep))
    adapter, state = adapter_for_counts([1, 0], **{change: True})

    with pytest.raises(SamplingError, match='select_after_tree:' + reason):
        adapter.select_page('cancelable')

    assert clock.now == 0
    assert state.verified == 0
