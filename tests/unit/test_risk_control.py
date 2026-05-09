"""
风控模块单元测试
"""

import pytest
from datetime import date
from bullet_trade.core.risk_control import RiskController, RiskStats


class TestRiskStats:
    """测试 RiskStats 数据类"""

    def test_reset(self):
        """测试重置功能"""
        stats = RiskStats()
        stats.daily_trades = 10
        stats.daily_trade_value = 100000
        stats.daily_buy_value = 60000
        stats.daily_sell_value = 40000
        stats.daily_cancels = 3
        stats.rejected_cancels = 1
        stats.cancel_by_order = {"o1": 1}

        stats.reset()

        assert stats.daily_trades == 0
        assert stats.daily_trade_value == 0.0
        assert stats.daily_buy_value == 0.0
        assert stats.daily_sell_value == 0.0
        assert stats.daily_cancels == 0
        assert stats.current_date == date.today()
        assert stats.rejected_cancels == 0
        assert stats.cancel_by_order == {}


class TestRiskController:
    """测试 RiskController 风控管理器"""

    @pytest.fixture
    def risk_config(self):
        """测试用风控配置"""
        return {
            "max_order_value": 100000,
            "max_daily_trade_value": 500000,
            "max_daily_trades": 100,
            "max_daily_cancels": 20,
            "min_cancel_interval_seconds": 1.0,
            "max_cancel_per_order": 2,
            "min_buy_order_value": 0.0,
            "max_stock_count": 20,
            "max_position_ratio": 20.0,
            "stop_loss_ratio": 5.0,
        }

    @pytest.fixture
    def risk_controller(self, risk_config):
        """创建风控管理器实例"""
        return RiskController(config=risk_config)

    def test_initialization(self, risk_controller):
        """测试初始化"""
        assert risk_controller.config is not None
        assert risk_controller.stats is not None
        assert risk_controller.stats.current_date == date.today()

    def test_check_order_success(self, risk_controller):
        """测试订单检查通过"""
        result = risk_controller.check_order(order_value=50000, current_positions_count=10)
        assert result is True

    def test_check_order_exceeds_max_order_value(self, risk_controller):
        """测试单笔订单金额超限"""
        with pytest.raises(ValueError, match="单笔订单金额超限"):
            risk_controller.check_order(
                order_value=150000,  # 超过 100000
                current_positions_count=10,
            )
        assert risk_controller.stats.rejected_orders == 1

    def test_check_order_min_buy_value_disabled_by_default(self, risk_controller):
        """测试默认不限制最小买入金额"""
        result = risk_controller.check_order(
            order_value=100,
            current_positions_count=10,
            action="buy",
        )
        assert result is True

    def test_check_order_rejects_buy_below_min_value(self, risk_config):
        """测试买入订单金额低于最小值时拒绝"""
        risk_config["min_buy_order_value"] = 1000.0
        risk = RiskController(config=risk_config)

        with pytest.raises(ValueError, match="买入订单金额低于最小值"):
            risk.check_order(
                order_value=999.99,
                current_positions_count=10,
                action="buy",
            )
        assert risk.stats.rejected_orders == 1

    def test_check_order_allows_sell_below_min_buy_value(self, risk_config):
        """测试最小买入金额不影响卖出订单"""
        risk_config["min_buy_order_value"] = 1000.0
        risk = RiskController(config=risk_config)

        result = risk.check_order(
            order_value=100,
            current_positions_count=10,
            action="sell",
        )

        assert result is True

    def test_check_order_exceeds_daily_trades(self, risk_controller):
        """测试当日交易次数超限"""
        # 模拟已交易 100 次
        risk_controller.stats.daily_trades = 100

        with pytest.raises(ValueError, match="当日交易次数超限"):
            risk_controller.check_order(order_value=50000, current_positions_count=10)
        assert risk_controller.stats.rejected_orders == 1

    def test_check_order_exceeds_daily_trade_value(self, risk_controller):
        """测试当日交易金额超限"""
        # 模拟已交易 450000
        risk_controller.stats.daily_trade_value = 450000

        with pytest.raises(ValueError, match="当日交易金额超限"):
            risk_controller.check_order(
                order_value=100000, current_positions_count=10  # 总计 550000，超过 500000
            )
        assert risk_controller.stats.rejected_orders == 1

    def test_check_order_exceeds_max_positions(self, risk_controller):
        """测试持仓数量超限"""
        with pytest.raises(ValueError, match="持仓数量超限"):
            risk_controller.check_order(
                order_value=50000, current_positions_count=20, action="buy"  # 已达上限
            )
        assert risk_controller.stats.rejected_orders == 1

    def test_check_order_exceeds_position_ratio(self, risk_controller):
        """测试单只股票仓位超限"""
        with pytest.raises(ValueError, match="单只股票仓位超限"):
            risk_controller.check_order(
                order_value=80000,  # 占比 25%，超过 20%
                current_positions_count=10,
                total_value=320000,  # 80000/320000 = 25%
                action="buy",
            )
        assert risk_controller.stats.rejected_orders == 1

    def test_record_trade_buy(self, risk_controller):
        """测试记录买入交易"""
        risk_controller.record_trade(order_value=50000, action="buy")

        assert risk_controller.stats.daily_trades == 1
        assert risk_controller.stats.daily_trade_value == 50000
        assert risk_controller.stats.daily_buy_value == 50000
        assert risk_controller.stats.daily_sell_value == 0

    def test_record_trade_sell(self, risk_controller):
        """测试记录卖出交易"""
        risk_controller.record_trade(order_value=30000, action="sell")

        assert risk_controller.stats.daily_trades == 1
        assert risk_controller.stats.daily_trade_value == 30000
        assert risk_controller.stats.daily_buy_value == 0
        assert risk_controller.stats.daily_sell_value == 30000

    def test_record_multiple_trades(self, risk_controller):
        """测试记录多笔交易"""
        risk_controller.record_trade(order_value=50000, action="buy")
        risk_controller.record_trade(order_value=30000, action="sell")
        risk_controller.record_trade(order_value=20000, action="buy")

        assert risk_controller.stats.daily_trades == 3
        assert risk_controller.stats.daily_trade_value == 100000
        assert risk_controller.stats.daily_buy_value == 70000
        assert risk_controller.stats.daily_sell_value == 30000

    def test_check_cancel_success(self, risk_controller):
        assert risk_controller.check_cancel("oid-1") is True

    def test_check_cancel_exceeds_daily_limit(self, risk_controller):
        risk_controller.stats.daily_cancels = 20
        with pytest.raises(ValueError, match="当日撤单次数超限"):
            risk_controller.check_cancel("oid-1")
        assert risk_controller.stats.rejected_cancels == 1

    def test_check_cancel_too_frequent(self, risk_controller, monkeypatch):
        from datetime import datetime

        risk_controller.stats.last_cancel_at = datetime.now()
        with pytest.raises(ValueError, match="撤单过于频繁"):
            risk_controller.check_cancel("oid-1")
        assert risk_controller.stats.rejected_cancels == 1

    def test_check_cancel_exceeds_per_order_limit(self, risk_controller):
        risk_controller.stats.cancel_by_order["oid-1"] = 2
        with pytest.raises(ValueError, match="单笔订单撤单次数超限"):
            risk_controller.check_cancel("oid-1")
        assert risk_controller.stats.rejected_cancels == 1

    def test_record_cancel(self, risk_controller):
        risk_controller.record_cancel("oid-1")
        assert risk_controller.stats.daily_cancels == 1
        assert risk_controller.stats.cancel_by_order["oid-1"] == 1
        assert risk_controller.stats.last_cancel_at is not None

    def test_check_stop_loss_triggered(self, risk_controller):
        """测试触发止损"""
        result = risk_controller.check_stop_loss(current_price=9.0, cost_price=10.0)  # 亏损 10%
        assert result is True

    def test_check_stop_loss_not_triggered(self, risk_controller):
        """测试未触发止损"""
        result = risk_controller.check_stop_loss(current_price=9.8, cost_price=10.0)  # 亏损 2%
        assert result is False

    def test_check_stop_loss_profit(self, risk_controller):
        """测试盈利状态不触发止损"""
        result = risk_controller.check_stop_loss(current_price=11.0, cost_price=10.0)  # 盈利 10%
        assert result is False

    def test_reset_daily_counter(self, risk_controller):
        """测试重置每日计数器"""
        risk_controller.stats.daily_trades = 10
        risk_controller.stats.daily_trade_value = 100000

        risk_controller.reset_daily_counter()

        assert risk_controller.stats.daily_trades == 0
        assert risk_controller.stats.daily_trade_value == 0
        assert risk_controller.stats.current_date == date.today()

    def test_get_status(self, risk_controller):
        """测试获取状态"""
        risk_controller.stats.daily_trades = 5
        risk_controller.stats.daily_trade_value = 50000
        risk_controller.stats.daily_buy_value = 30000
        risk_controller.stats.daily_sell_value = 20000
        risk_controller.stats.rejected_orders = 2

        status = risk_controller.get_status()

        assert "日期" in status
        assert "当日交易次数" in status
        assert "5/100" in status["当日交易次数"]
        assert status["剩余交易次数"] == 95
        assert status["被拒绝订单数"] == 2
        assert status["当日撤单次数"] == "0/20"

    def test_get_status_summary(self, risk_controller):
        """测试获取状态摘要"""
        risk_controller.stats.daily_trades = 5
        risk_controller.stats.daily_trade_value = 50000

        summary = risk_controller.get_status_summary()

        assert "风控状态" in summary
        assert "5/100" in summary

    def test_is_trade_allowed_true(self, risk_controller):
        """测试允许交易"""
        result = risk_controller.is_trade_allowed(order_value=50000)
        assert result is True

    def test_is_trade_allowed_false_exceeds_order_value(self, risk_controller):
        """测试不允许交易（订单金额超限）"""
        result = risk_controller.is_trade_allowed(order_value=150000)
        assert result is False

    def test_is_trade_allowed_respects_min_buy_value(self, risk_config):
        """测试快速检查会识别最小买入金额"""
        risk_config["min_buy_order_value"] = 1000.0
        risk = RiskController(config=risk_config)

        assert risk.is_trade_allowed(order_value=999.99, action="buy") is False
        assert risk.is_trade_allowed(order_value=999.99, action="sell") is True

    def test_is_trade_allowed_false_exceeds_daily_trades(self, risk_controller):
        """测试不允许交易（交易次数超限）"""
        risk_controller.stats.daily_trades = 100
        result = risk_controller.is_trade_allowed(order_value=50000)
        assert result is False

    def test_is_trade_allowed_false_exceeds_daily_value(self, risk_controller):
        """测试不允许交易（交易金额超限）"""
        risk_controller.stats.daily_trade_value = 450000
        result = risk_controller.is_trade_allowed(order_value=100000)
        assert result is False

    def test_get_max_order_value_allowed(self, risk_controller):
        """测试获取最大允许订单金额"""
        # 初始状态：min(100000, 500000) = 100000
        max_value = risk_controller.get_max_order_value_allowed()
        assert max_value == 100000

        # 已交易 450000：min(100000, 50000) = 50000
        risk_controller.stats.daily_trade_value = 450000
        max_value = risk_controller.get_max_order_value_allowed()
        assert max_value == 50000

        # 已交易 500000：min(100000, 0) = 0
        risk_controller.stats.daily_trade_value = 500000
        max_value = risk_controller.get_max_order_value_allowed()
        assert max_value == 0

    def test_complete_workflow(self, risk_controller):
        """测试完整工作流"""
        # 1. 检查订单
        assert risk_controller.check_order(order_value=50000, current_positions_count=5)

        # 2. 记录交易
        risk_controller.record_trade(order_value=50000, action="buy")

        # 3. 验证统计
        assert risk_controller.stats.daily_trades == 1
        assert risk_controller.stats.daily_trade_value == 50000

        # 4. 再次交易
        assert risk_controller.check_order(order_value=30000, current_positions_count=6)
        risk_controller.record_trade(order_value=30000, action="buy")

        # 5. 验证累计统计
        assert risk_controller.stats.daily_trades == 2
        assert risk_controller.stats.daily_trade_value == 80000

        # 6. 获取状态
        status = risk_controller.get_status()
        assert status["剩余交易次数"] == 98
        assert status["剩余交易金额"] == 420000


class TestGlobalRiskController:
    """测试全局风控管理器"""

    def test_get_global_risk_controller(self):
        """测试获取全局风控管理器"""
        from bullet_trade.core.risk_control import (
            get_global_risk_controller,
            reset_global_risk_controller,
        )

        # 重置
        reset_global_risk_controller()

        # 获取实例
        risk1 = get_global_risk_controller()
        risk2 = get_global_risk_controller()

        # 应该是同一个实例
        assert risk1 is risk2

        # 记录交易
        risk1.record_trade(order_value=50000)

        # 在另一个引用中也能看到
        assert risk2.stats.daily_trades == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
