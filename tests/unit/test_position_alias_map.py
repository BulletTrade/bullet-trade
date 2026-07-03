import pytest

from bullet_trade.core.models import Portfolio, Position, SecurityPositionMap, SubPortfolio


def test_backtest_portfolio_positions_support_qmt_and_jq_suffix_aliases():
    portfolio = Portfolio(available_cash=1000.0, total_value=1000.0)
    position = Position(
        security="511880.SH",
        total_amount=900,
        closeable_amount=900,
        avg_cost=100.6,
        price=100.6,
        value=90540.0,
    )

    portfolio.positions["511880.SH"] = position

    assert "511880.SH" in portfolio.positions
    assert "511880.XSHG" in portfolio.positions
    assert portfolio.positions.get("511880.XSHG") is position
    assert portfolio.positions["511880.XSHG"] is position

    portfolio.update_value()

    assert list(portfolio.positions.keys()) == ["511880.SH"]
    assert list(portfolio.positions.values()) == [position]
    assert portfolio.positions_value == pytest.approx(90540.0)
    assert portfolio.total_value == pytest.approx(91540.0)


def test_position_alias_map_updates_existing_alias_without_duplicate_value():
    positions = SecurityPositionMap()
    original = Position(security="000001.SZ", total_amount=100, value=1000.0)
    replacement = Position(security="000001.XSHE", total_amount=200, value=2000.0)

    positions["000001.SZ"] = original
    positions["000001.XSHE"] = replacement

    assert len(positions) == 1
    assert list(positions.keys()) == ["000001.SZ"]
    assert positions["000001.SZ"] is replacement
    assert positions["000001.XSHE"] is replacement
    assert positions.pop("000001.XSHE") is replacement
    assert positions == {}


def test_subportfolio_positions_support_suffix_aliases():
    subportfolio = SubPortfolio()
    position = Position(security="159915.SZ", total_amount=1000, value=3231.0)

    subportfolio.positions["159915.SZ"] = position

    assert subportfolio.positions["159915.XSHE"] is position
    assert subportfolio.positions.get("159915.XSHE") is position
    assert subportfolio.positions.pop("159915.XSHE") is position
    assert subportfolio.positions.get("159915.SZ") is None
