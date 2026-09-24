from datetime import datetime
import logging

import pytest


from bullet_trade.core.globals import g, log, reset_globals


@pytest.fixture(autouse=True)
def _reset_globals():
    previous_time = log.strategy_time
    reset_globals()
    log.set_strategy_time(None)
    yield
    reset_globals()
    log.set_strategy_time(previous_time)


def test_log_format_backtest(caplog):
    g.live_trade = False
    log.set_strategy_time(datetime(2025, 1, 2, 9, 0))
    caplog.set_level("INFO", logger="jq_strategy")
    log.info("测试消息")
    record = caplog.records[-1]
    assert "[2025-01-02 09:00:00]" in record.message
    assert "策略时间:" not in record.message
    formatted = log._formatter.format(record)
    assert formatted.startswith("[INFO] [2025-01-02 09:00:00]")


def test_log_format_live_delay(caplog):
    g.live_trade = True
    log.set_strategy_time(datetime(2025, 1, 2, 9, 0))
    caplog.set_level("INFO", logger="jq_strategy")
    log.info("测试消息")
    record = caplog.records[-1]
    assert "[策略:2025-01-02 09:00:00]" in record.message
    assert "delay=" in record.message


@pytest.mark.parametrize("method", ["debug", "info", "warn", "warning", "error", "critical"])
@pytest.mark.parametrize("message,args,expected", [
    ("value=%s count=%d", ("x", 2), "value=x count=2"),
    ("value=%(value)s", ({"value": "x"},), "value=x"),
    ("value={} count={}", ("x", 2), "value=x count=2"),
    ("value={:.2f}", (1.256,), "value=1.26"),
    ("value", ("x", 2), "value x 2"),
    ("value", ({"a": 2},), "value {'a': 2}"),
    ("broken {", (2,), "broken { 2"),
    ("broken %d", ("x",), "broken %d x"),
    ("literal {} %s", (), "literal {} %s"),
])
def test_log_argument_styles(method, message, args, expected, caplog, capsys):
    caplog.set_level(logging.DEBUG, logger="jq_strategy")
    getattr(log, method)(message, *args, extra={"audit_id": "test"})
    record = caplog.records[-1]
    assert record.getMessage() == expected
    assert record.audit_id == "test"
    assert "Logging error" not in capsys.readouterr().err


def test_disabled_log_does_not_format_arguments(caplog):
    class Unformattable:
        def __str__(self):
            raise AssertionError("disabled log formatted an argument")

    caplog.set_level(logging.INFO, logger="jq_strategy")
    log.debug("value={}", Unformattable())


def test_brace_log_keeps_exception_and_strategy_time(caplog):
    caplog.set_level(logging.ERROR, logger="jq_strategy")
    log.set_strategy_time(datetime(2025, 1, 2, 9, 0))
    try:
        raise ValueError("example")
    except ValueError:
        log.error("failure {}", "x", exc_info=True)
    record = caplog.records[-1]
    assert "[2025-01-02 09:00:00] failure x" == record.getMessage()
    assert record.exc_info[0] is ValueError
