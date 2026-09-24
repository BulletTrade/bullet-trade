import pytest


from bullet_trade.core import pricing
from bullet_trade.core.orders import MarketOrderStyle


def _assert_close(actual: float, expected: float, tol: float = 1e-6):
    assert abs(actual - expected) <= tol, f"{actual} != {expected}"


def test_min_price_step_etf():
    _assert_close(pricing.get_min_price_step("510050.XSHG", 3.0), 0.001)


def test_min_price_step_a_share_brackets():
    _assert_close(pricing.get_min_price_step("600000.XSHG", 12.0), 0.01)
    _assert_close(pricing.get_min_price_step("600000.XSHG", 0.8), 0.001)


def test_min_price_step_unknown_for_futures():
    """期货档位由合约规格表按合约解析，pricing 不猜档位。"""
    for security in ("T2109.CCFX", "IF2407.CCFX", "RB2201.XSGE", "AU1606.XSGE"):
        assert pricing.get_min_price_step(security, 100.0) == 0.0


def test_clamp_price_to_trade_bounds_keeps_fine_futures_grid():
    """国债期货 0.005 档位上的成交价不能被重新压到 0.01 网格。"""
    price = pricing.clamp_price_to_trade_bounds("T2006.CCFX", 101.085, 101.035, None, None, True)
    _assert_close(price, 101.085)


def test_price_bounds_mainboard():
    buy_upper, sell_lower = pricing.compute_price_bounds("600000.XSHG", 10.0, 0.01)
    _assert_close(buy_upper, 10.2)
    _assert_close(sell_lower, 9.8)


def test_price_bounds_beijing():
    buy_upper, sell_lower = pricing.compute_price_bounds("430047.BJ", 10.0, 0.01)
    _assert_close(buy_upper, max(10 * 1.05, 10 + 0.1))
    _assert_close(sell_lower, min(10 * 0.95, 10 - 0.1))


def test_compute_market_protect_price_defaults():
    price = pricing.compute_market_protect_price("600000.XSHG", 10.0, 11.0, 9.0, 0.015, True)
    _assert_close(price, 10.15)
    sell_price = pricing.compute_market_protect_price("600000.XSHG", 10.0, 11.0, 9.0, -0.015, False)
    _assert_close(sell_price, 9.85)


def test_compute_market_protect_price_clamped_by_cage():
    # 将保护价拉高至超出笼子，结果需要裁剪到 10.2
    price = pricing.compute_market_protect_price("600000.XSHG", 10.0, 10.4, 9.2, 0.5, True)
    _assert_close(price, 10.2)


def test_clamp_price_to_trade_bounds_uses_limit_and_price_cage():
    buy_price = pricing.clamp_price_to_trade_bounds(
        "600000.XSHG",
        10.5,
        10.0,
        10.15,
        9.0,
        True,
    )
    _assert_close(buy_price, 10.15)

    sell_price = pricing.clamp_price_to_trade_bounds(
        "600000.XSHG",
        9.5,
        10.0,
        11.0,
        9.85,
        False,
    )
    _assert_close(sell_price, 9.85)


def test_resolve_market_percent_priority():
    cfg_buy = 0.015
    cfg_sell = -0.015
    style = MarketOrderStyle(buy_price_percent=0.02, sell_price_percent=-0.02)
    _assert_close(pricing.resolve_market_percent(style, True, cfg_buy, cfg_sell), 0.02)
    _assert_close(pricing.resolve_market_percent(style, False, cfg_buy, cfg_sell), -0.02)
    # 无策略覆盖时应回落到配置
    default_style = MarketOrderStyle()
    _assert_close(pricing.resolve_market_percent(default_style, True, cfg_buy, cfg_sell), cfg_buy)
    _assert_close(pricing.resolve_market_percent(default_style, False, cfg_buy, cfg_sell), cfg_sell)
