import logging
from datetime import datetime, timedelta

from bullet_trade.core.globals import log, g


def test_log_format_backtest(caplog):
    caplog.set_level(logging.INFO)
    g.live_trade = False
    log.set_strategy_time(datetime(2025, 1, 1, 9, 40, 0))
    log.info("hello")
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "[2025-01-01 09:40:00]" in text
    assert "策略时间:" not in text
    assert "delay=" not in text


def test_log_format_live(caplog):
    caplog.set_level(logging.INFO)
    g.live_trade = True
    # 策略时间比当前早 3 秒
    log.set_strategy_time(datetime.now() - timedelta(seconds=3))
    log.info("hello")
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "策略:" in text and "当前:" in text and "delay=" in text
