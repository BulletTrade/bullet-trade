from datetime import datetime

import pytest


from bullet_trade.core.globals import g, log, reset_globals


@pytest.fixture(autouse=True)
def _reset_globals():
    reset_globals()
    yield
    reset_globals()


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
