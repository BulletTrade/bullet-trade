import pytest

from bullet_trade.integrations.ths.scheduler import GuiTaskScheduler, YIELD


def test_priority_fifo_and_expiry():
    now = [100.0]
    scheduler = GuiTaskScheduler(clock=lambda: now[0])
    seen = []
    scheduler.submit(lambda ctx: seen.append("low"), priority=10)
    scheduler.submit(lambda ctx: seen.append("first"), priority=0)
    scheduler.submit(lambda ctx: seen.append("second"), priority=0)
    scheduler.submit(lambda ctx: seen.append("expired"), priority=-1, expires_at=101)
    now[0] = 102
    assert [scheduler.run_next().status for _ in range(3)] == ["done"] * 3
    assert seen == ["first", "second", "low"]
    assert scheduler.run_next().status == "empty"


def test_refresh_coalescing_interval_and_safe_point_yield():
    now = [0.0]
    scheduler = GuiTaskScheduler(clock=lambda: now[0])
    seen = []
    def refresh(ctx):
        seen.append("refresh")
        if ctx.should_yield():
            return YIELD
        return "ok"
    assert scheduler.submit_refresh("A:positions", refresh) is not None
    assert scheduler.submit_refresh("A:positions", refresh) is None
    scheduler.submit(lambda ctx: seen.append("request"), priority=0)
    assert scheduler.run_next().status == "done"
    assert scheduler.run_next().value == "ok"
    assert seen == ["request", "refresh"]
    assert scheduler.submit_refresh("A:positions", refresh) is None
    now[0] = 20
    assert scheduler.submit_refresh("A:positions", refresh) is not None
    assert scheduler.run_next().status == "done"


def test_cooperative_yield_requeues_without_interruption():
    scheduler = GuiTaskScheduler(clock=lambda: 0)
    seen = []
    def low(ctx):
        seen.append("low-safe-point")
        if len(seen) == 1:
            scheduler.submit(lambda inner: seen.append("high"), priority=0)
        return YIELD if ctx.should_yield() else "finished"
    scheduler.submit(low, priority=10)
    assert scheduler.run_next().status == "yielded"
    assert scheduler.run_next().status == "done"
    assert scheduler.run_next().value == "finished"
    assert seen == ["low-safe-point", "high", "low-safe-point"]


def test_running_key_and_expired_queue_do_not_accumulate():
    now = [0.0]
    scheduler = GuiTaskScheduler(clock=lambda: now[0])
    def callback(ctx):
        assert scheduler.submit(lambda inner: None, key="one") is None
    scheduler.submit(callback, key="one")
    scheduler.run_next()
    assert scheduler.submit(lambda ctx: None, key="one") is not None
    scheduler.submit(lambda ctx: None, priority=0, expires_at=1, key="two")
    now[0] = 2
    assert scheduler.run_next().status == "done"
    assert scheduler.run_next().status == "empty"
    assert scheduler.submit(lambda ctx: None, key="two") is not None


def test_bounded_queue_and_invalid_times():
    scheduler = GuiTaskScheduler(clock=lambda: 1, max_pending=1)
    assert scheduler.submit(lambda ctx: None) is not None
    assert scheduler.pending_count == 1
    with pytest.raises(OverflowError):
        scheduler.submit(lambda ctx: None)
    with pytest.raises(ValueError):
        scheduler.submit(lambda ctx: None, expires_at=float("nan"))
    with pytest.raises(ValueError):
        scheduler.submit_refresh("orders", lambda ctx: None, interval=float("inf"))
    scheduler.run_next()
    assert scheduler.pending_count == 0
