"""
风控管理模块

提供订单风控检查、交易次数限制等功能
"""

from typing import Optional, Dict, Any
from datetime import date, datetime
from dataclasses import dataclass, field
import logging

from bullet_trade.utils.env_loader import get_risk_control_config


logger = logging.getLogger(__name__)


@dataclass
class RiskStats:
    """风控统计信息"""

    current_date: Optional[date] = None
    daily_trades: int = 0  # 当日交易次数
    daily_trade_value: float = 0.0  # 当日交易金额（元）
    daily_buy_value: float = 0.0  # 当日买入金额
    daily_sell_value: float = 0.0  # 当日卖出金额
    daily_cancels: int = 0  # 当日撤单次数
    rejected_orders: int = 0  # 被拒绝的订单数
    rejected_cancels: int = 0  # 被拒绝的撤单数
    last_cancel_at: Optional[datetime] = None
    cancel_by_order: Dict[str, int] = field(default_factory=dict)

    def reset(self):
        """重置统计信息"""
        self.current_date = date.today()
        self.daily_trades = 0
        self.daily_trade_value = 0.0
        self.daily_buy_value = 0.0
        self.daily_sell_value = 0.0
        self.daily_cancels = 0
        self.rejected_orders = 0
        self.rejected_cancels = 0
        self.last_cancel_at = None
        self.cancel_by_order = {}


class RiskController:
    """
    风控管理器

    负责：
    - 单笔订单金额检查
    - 最小买入订单金额检查
    - 每日交易次数限制
    - 每日交易金额限制
    - 持仓数量控制
    - 单只股票仓位控制

    示例：
        >>> risk = RiskController()
        >>> risk.check_order(
        ...     order_value=50000,
        ...     current_positions_count=10,
        ...     total_value=1000000
        ... )
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化风控管理器

        Args:
            config: 风控配置字典，如果为 None 则从环境变量加载
        """
        self.config = config or get_risk_control_config()
        self.config["min_buy_order_value"] = self._normalize_min_buy_order_value(
            self.config.get("min_buy_order_value", 0.0)
        )
        self.stats = RiskStats()
        self.stats.reset()

        logger.info("=" * 60)
        logger.info("风控管理器已初始化")
        logger.info(f"  单笔订单最大金额: ¥{self.config['max_order_value']:,}")
        logger.info(f"  最小买入订单金额: ¥{self.config['min_buy_order_value']:,.2f}")
        logger.info(f"  单日最大交易金额: ¥{self.config['max_daily_trade_value']:,}")
        logger.info(f"  单日最大交易次数: {self.config['max_daily_trades']}")
        logger.info(f"  单日最大撤单次数: {self.config['max_daily_cancels']}")
        logger.info(f"  撤单最小时间间隔: {self.config['min_cancel_interval_seconds']}s")
        logger.info(f"  单笔订单最大撤单次数: {self.config['max_cancel_per_order']}")
        logger.info(f"  最大持仓股票数: {self.config['max_stock_count']}")
        logger.info(f"  单只股票最大仓位: {self.config['max_position_ratio']}%")
        logger.info(f"  止损比例: {self.config['stop_loss_ratio']}%")
        logger.info("=" * 60)

    def check_order(
        self,
        order_value: float,
        current_positions_count: int,
        security: Optional[str] = None,
        total_value: Optional[float] = None,
        action: str = "buy",
    ) -> bool:
        """
        检查订单是否符合风控规则

        Args:
            order_value: 订单金额（元）
            current_positions_count: 当前持仓数量
            security: 证券代码（可选）
            total_value: 账户总资产（可选，用于检查单只股票仓位）
            action: 操作类型 'buy' 或 'sell'

        Returns:
            bool: 检查通过返回 True

        Raises:
            ValueError: 当订单不符合风控规则时抛出
        """
        # 检查是否需要重置每日计数器
        self._check_and_reset_daily()

        action = str(action or "buy").lower()
        min_buy_order_value = float(self.config.get("min_buy_order_value") or 0.0)

        # 1. 检查买入订单最小金额；默认 0 表示不限制，卖出不受影响
        if action == "buy" and min_buy_order_value > 0 and order_value < min_buy_order_value:
            self.stats.rejected_orders += 1
            raise ValueError(
                f"❌ 买入订单金额低于最小值: "
                f"¥{order_value:,.2f} < ¥{min_buy_order_value:,.2f}"
            )

        # 2. 检查单笔订单金额
        if order_value > self.config["max_order_value"]:
            self.stats.rejected_orders += 1
            raise ValueError(
                f"❌ 单笔订单金额超限: "
                f"¥{order_value:,.2f} > ¥{self.config['max_order_value']:,}"
            )

        # 3. 检查当日交易次数
        if self.stats.daily_trades >= self.config["max_daily_trades"]:
            self.stats.rejected_orders += 1
            raise ValueError(
                f"❌ 当日交易次数超限: "
                f"{self.stats.daily_trades} >= {self.config['max_daily_trades']}"
            )

        # 4. 检查当日交易金额
        total_trade_value = self.stats.daily_trade_value + order_value
        if total_trade_value > self.config["max_daily_trade_value"]:
            self.stats.rejected_orders += 1
            raise ValueError(
                f"❌ 当日交易金额超限: "
                f"¥{total_trade_value:,.2f} > ¥{self.config['max_daily_trade_value']:,}"
            )

        # 5. 检查持仓数量（仅买入时检查）
        if action == "buy" and current_positions_count >= self.config["max_stock_count"]:
            self.stats.rejected_orders += 1
            raise ValueError(
                f"❌ 持仓数量超限: "
                f"{current_positions_count} >= {self.config['max_stock_count']}"
            )

        # 6. 检查单只股票仓位（如果提供了总资产）
        if action == "buy" and total_value is not None and total_value > 0:
            position_ratio = (order_value / total_value) * 100
            if position_ratio > self.config["max_position_ratio"]:
                self.stats.rejected_orders += 1
                raise ValueError(
                    f"❌ 单只股票仓位超限: "
                    f"{position_ratio:.2f}% > {self.config['max_position_ratio']}%"
                )

        logger.debug(
            f"✅ 风控检查通过: {security or '未知证券'}, "
            f"金额: ¥{order_value:,.2f}, 操作: {action}"
        )
        return True

    def check_cancel(self, order_id: Optional[str] = None) -> bool:
        """
        检查撤单是否符合风控规则。

        Args:
            order_id: 订单 ID，可选。提供后会检查单笔订单的撤单次数。

        Returns:
            bool: 检查通过返回 True

        Raises:
            ValueError: 当撤单不符合风控规则时抛出
        """
        self._check_and_reset_daily()

        if self.stats.daily_cancels >= self.config["max_daily_cancels"]:
            self.stats.rejected_cancels += 1
            raise ValueError(
                f"❌ 当日撤单次数超限: "
                f"{self.stats.daily_cancels} >= {self.config['max_daily_cancels']}"
            )

        interval_limit = float(self.config.get("min_cancel_interval_seconds") or 0.0)
        if interval_limit > 0 and self.stats.last_cancel_at is not None:
            elapsed = (datetime.now() - self.stats.last_cancel_at).total_seconds()
            if elapsed < interval_limit:
                self.stats.rejected_cancels += 1
                raise ValueError(f"❌ 撤单过于频繁: {elapsed:.2f}s < {interval_limit:.2f}s")

        order_id = str(order_id or "").strip()
        if order_id:
            current = int(self.stats.cancel_by_order.get(order_id, 0))
            max_per_order = int(self.config.get("max_cancel_per_order") or 0)
            if max_per_order > 0 and current >= max_per_order:
                self.stats.rejected_cancels += 1
                raise ValueError(
                    f"❌ 单笔订单撤单次数超限: "
                    f"order_id={order_id}, {current} >= {max_per_order}"
                )

        logger.debug(f"✅ 撤单风控检查通过: order_id={order_id or '未知'}")
        return True

    def record_trade(self, order_value: float, action: str = "buy"):
        """
        记录交易

        Args:
            order_value: 订单金额
            action: 操作类型 'buy' 或 'sell'
        """
        self._check_and_reset_daily()

        self.stats.daily_trades += 1
        self.stats.daily_trade_value += order_value

        if action == "buy":
            self.stats.daily_buy_value += order_value
        else:
            self.stats.daily_sell_value += order_value

        logger.info(
            f"📝 已记录交易: 当日第 {self.stats.daily_trades} 笔, "
            f"金额: ¥{order_value:,.2f}, "
            f"累计: ¥{self.stats.daily_trade_value:,.2f}"
        )

    def record_cancel(self, order_id: Optional[str] = None):
        """
        记录撤单。
        """
        self._check_and_reset_daily()
        self.stats.daily_cancels += 1
        self.stats.last_cancel_at = datetime.now()
        order_id = str(order_id or "").strip()
        if order_id:
            self.stats.cancel_by_order[order_id] = (
                int(self.stats.cancel_by_order.get(order_id, 0)) + 1
            )
        logger.info(
            f"📝 已记录撤单: 当日第 {self.stats.daily_cancels} 次, "
            f"order_id={order_id or '-'}"
        )

    def check_stop_loss(self, current_price: float, cost_price: float) -> bool:
        """
        检查是否触发止损

        Args:
            current_price: 当前价格
            cost_price: 成本价

        Returns:
            bool: 如果触发止损返回 True
        """
        if cost_price <= 0:
            return False

        loss_ratio = ((current_price - cost_price) / cost_price) * 100

        if loss_ratio <= -self.config["stop_loss_ratio"]:
            logger.warning(
                f"⚠️ 触发止损: 当前亏损 {abs(loss_ratio):.2f}% >= "
                f"止损线 {self.config['stop_loss_ratio']}%"
            )
            return True

        return False

    def reset_daily_counter(self):
        """手动重置每日计数器"""
        old_date = self.stats.current_date
        self.stats.reset()
        logger.info(f"🔄 已重置每日计数器: {old_date} -> {self.stats.current_date}")

    def _check_and_reset_daily(self):
        """检查日期，如果是新的一天则重置计数器"""
        today = date.today()
        if self.stats.current_date != today:
            logger.info(f"📅 检测到新交易日: {today}")
            self.reset_daily_counter()

    @staticmethod
    def _normalize_min_buy_order_value(value: Any) -> float:
        """
        归一化最小买入金额配置。

        Args:
            value: 配置值，允许数字或字符串。

        Returns:
            float: 大于等于 0 的金额；无效值按 0 处理。
        """
        try:
            parsed = float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, parsed)

    def get_status(self) -> Dict[str, Any]:
        """
        获取当前风控状态

        Returns:
            dict: 包含当前风控状态的字典
        """
        max_trades = self.config["max_daily_trades"]
        max_value = self.config["max_daily_trade_value"]

        return {
            "日期": str(self.stats.current_date),
            "当日交易次数": f"{self.stats.daily_trades}/{max_trades}",
            "当日交易金额": f"¥{self.stats.daily_trade_value:,.2f}/¥{max_value:,}",
            "剩余交易次数": max_trades - self.stats.daily_trades,
            "剩余交易金额": max_value - self.stats.daily_trade_value,
            "最小买入金额": f"¥{self.config.get('min_buy_order_value', 0.0):,.2f}",
            "当日撤单次数": f"{self.stats.daily_cancels}/{self.config['max_daily_cancels']}",
            "剩余撤单次数": self.config["max_daily_cancels"] - self.stats.daily_cancels,
            "当日买入金额": f"¥{self.stats.daily_buy_value:,.2f}",
            "当日卖出金额": f"¥{self.stats.daily_sell_value:,.2f}",
            "被拒绝订单数": self.stats.rejected_orders,
            "被拒绝撤单数": self.stats.rejected_cancels,
        }

    def get_status_summary(self) -> str:
        """
        获取风控状态摘要（格式化字符串）

        Returns:
            str: 格式化的状态摘要
        """
        status = self.get_status()
        return (
            f"风控状态 [{status['日期']}]: "
            f"交易次数 {status['当日交易次数']}, "
            f"交易金额 {status['当日交易金额']}"
        )

    def print_status(self):
        """打印风控状态"""
        status = self.get_status()
        print("\n" + "=" * 60)
        print("📊 风控状态")
        print("=" * 60)
        for key, value in status.items():
            print(f"  {key}: {value}")
        print("=" * 60 + "\n")

    def is_trade_allowed(self, order_value: float, action: str = "buy") -> bool:
        """
        快速检查是否允许交易（不抛出异常）

        Args:
            order_value: 订单金额
            action: 操作类型 'buy' 或 'sell'；最小买入金额仅对 buy 生效

        Returns:
            bool: 允许交易返回 True，否则返回 False
        """
        self._check_and_reset_daily()

        action = str(action or "buy").lower()
        min_buy_order_value = float(self.config.get("min_buy_order_value") or 0.0)
        if action == "buy" and min_buy_order_value > 0 and order_value < min_buy_order_value:
            return False

        # 检查交易次数
        if self.stats.daily_trades >= self.config["max_daily_trades"]:
            return False

        # 检查订单金额
        if order_value > self.config["max_order_value"]:
            return False

        # 检查当日交易金额
        if self.stats.daily_trade_value + order_value > self.config["max_daily_trade_value"]:
            return False

        return True

    def get_max_order_value_allowed(self) -> float:
        """
        获取当前允许的最大订单金额

        Returns:
            float: 最大允许订单金额
        """
        self._check_and_reset_daily()

        # 基于配置的最大金额
        max_by_config = self.config["max_order_value"]

        # 基于当日剩余额度
        max_by_daily = self.config["max_daily_trade_value"] - self.stats.daily_trade_value

        # 返回较小值
        return max(0, min(max_by_config, max_by_daily))


# 全局风控管理器实例（可选）
_global_risk_controller: Optional[RiskController] = None


def get_global_risk_controller() -> RiskController:
    """
    获取全局风控管理器实例（单例模式）

    Returns:
        RiskController: 全局风控管理器
    """
    global _global_risk_controller
    if _global_risk_controller is None:
        _global_risk_controller = RiskController()
    return _global_risk_controller


def reset_global_risk_controller():
    """重置全局风控管理器"""
    global _global_risk_controller
    _global_risk_controller = None
    logger.info("🔄 全局风控管理器已重置")
