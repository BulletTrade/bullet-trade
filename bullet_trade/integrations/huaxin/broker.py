"""
作者: BruceLee

文件职责: 提供第一阶段 HuaxinBroker 的配置门禁和启动前 fail-closed 骨架。
主要输入: 私有环境配置中的账户占位、native bundle 路径及交易/撤单开关。
主要输出: 初始化前 native readiness 错误和不可绕过的本地写操作拒绝。
上游关系: 券商注册表与 LiveEngine 只在用户显式选择 ``huaxin`` 时构造本类。
下游关系: 离线 doctor；未来真实 Trader C ABI wrapper 将替换受控未实现分支。
关键配置: 当前 offline fake 永远不是真实 native ready；任何查询、交易和撤单均不可达。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from ...broker.base import BrokerBase
from .build import DoctorReport, doctor
from .errors import (
    HUAXIN_BACKEND_NOT_IMPLEMENTED,
    HUAXIN_CANCEL_DISABLED,
    HUAXIN_NATIVE_UNAVAILABLE,
    HUAXIN_TRADING_DISABLED,
    HuaxinNativeUnavailableError,
    HuaxinTradingDisabledError,
)


class HuaxinBroker(BrokerBase):
    """实现华鑫券商公共合同的第一阶段安全骨架。

    本类仅负责配置与 readiness 边界，不连接 TORA Trader。核心状态包括最近一次
    doctor 报告和两个默认关闭的写开关；后续真实实现必须保持这些门禁位于写调用之前。
    """

    def __init__(
        self,
        account_id: str,
        account_type: str = "stock",
        *,
        config: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """创建尚未连接且默认禁止写入的华鑫券商。

        Args:
            account_id: 私有配置中的账户标识；当前阶段不会发送或记录它。
            account_type: 账户类型，当前只保留公共 Broker 合同字段。
            config: 华鑫私有配置映射，可能包含 bundle 路径和布尔门禁。

        Returns:
            None。

        Side Effects:
            只复制配置，不读取 bundle、不 dlopen、不联网、不连接柜台。
        """

        super().__init__(account_id=account_id, account_type=account_type)
        self._config = dict(config or {})
        self._enable_trading = bool(self._config.get("enable_trading", False))
        self._enable_cancel = bool(self._config.get("enable_cancel", False))
        self._doctor_report: Optional[DoctorReport] = None

    @property
    def doctor_report(self) -> Optional[DoctorReport]:
        """返回最近一次启动前诊断快照。

        Returns:
            尚未执行时为 None，否则为不可变 ``DoctorReport``。
        """

        return self._doctor_report

    def preflight(self) -> None:
        """在策略 initialize 前验证真实华鑫 native readiness。

        Returns:
            None；只有未来真实 bundle 报告 ``native_ready=true`` 才可能返回。

        Raises:
            HuaxinNativeUnavailableError: bundle 缺失、仅为 offline fake 或真实 SDK 未就绪。

        Side Effects:
            只执行离线 doctor，不编译、不 dlopen、不联网、不连接柜台。
        """

        raw_bundle = self._config.get("bundle_path")
        bundle_path = Path(str(raw_bundle)).expanduser() if raw_bundle else None
        report = doctor(bundle_path=bundle_path, load=False)
        self._doctor_report = report
        if not report.native_ready:
            raise HuaxinNativeUnavailableError(
                HUAXIN_NATIVE_UNAVAILABLE,
                "华鑫真实 native/SDK 尚未就绪，策略初始化已被拒绝",
                {
                    "reason_code": report.reason_code,
                    "offline_bridge_ready": report.offline_bridge_ready,
                },
            )

    def connect(self) -> bool:
        """拒绝当前第一阶段尚未实现的真实 Trader 连接。

        Returns:
            当前阶段不会正常返回。

        Raises:
            HuaxinNativeUnavailableError: native 未就绪或 Trader 后端尚未实现。

        Side Effects:
            不连接网络、不创建 TORA API，也不改变 connected 状态。
        """

        self.preflight()
        self._raise_backend_not_implemented("connect")
        return False

    def disconnect(self) -> bool:
        """以幂等方式清理当前无连接状态。

        Returns:
            始终为 True。

        Side Effects:
            把本地 connected 标记设为 False；当前阶段没有 native 资源可释放。
        """

        self._connected = False
        return True

    def get_account_info(self) -> Dict[str, Any]:
        """拒绝尚未接通的真实账户查询。

        Returns:
            当前阶段不会正常返回。

        Raises:
            HuaxinNativeUnavailableError: Trader 查询后端尚未实现。
        """

        self._raise_backend_not_implemented("get_account_info")
        return {}

    def get_positions(self) -> List[Dict[str, Any]]:
        """拒绝尚未接通的真实持仓查询。

        Returns:
            当前阶段不会正常返回。

        Raises:
            HuaxinNativeUnavailableError: Trader 查询后端尚未实现。
        """

        self._raise_backend_not_implemented("get_positions")
        return []

    async def buy(
        self,
        security: str,
        amount: int,
        price: Optional[float] = None,
        wait_timeout: Optional[float] = None,
        remark: Optional[str] = None,
        *,
        market: bool = False,
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """在最靠近公共入口的位置拒绝买入请求。

        Args:
            security: 标准证券代码。
            amount: 委托数量。
            price: 限价或市价保护价。
            wait_timeout: 等待回报的超时秒数。
            remark: 订单备注。
            market: 是否为市价意图。
            extra: 可选审计扩展。

        Returns:
            当前阶段不会正常返回。

        Raises:
            HuaxinTradingDisabledError: 默认交易开关关闭。
            HuaxinNativeUnavailableError: 即使开关误开，真实写后端仍未实现。
        """

        del security, amount, price, wait_timeout, remark, market, extra
        self._require_trading_enabled()
        self._raise_backend_not_implemented("buy")
        return ""

    async def sell(
        self,
        security: str,
        amount: int,
        price: Optional[float] = None,
        wait_timeout: Optional[float] = None,
        remark: Optional[str] = None,
        *,
        market: bool = False,
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """在最靠近公共入口的位置拒绝卖出请求。

        Args:
            security: 标准证券代码。
            amount: 委托数量。
            price: 限价或市价保护价。
            wait_timeout: 等待回报的超时秒数。
            remark: 订单备注。
            market: 是否为市价意图。
            extra: 可选审计扩展。

        Returns:
            当前阶段不会正常返回。

        Raises:
            HuaxinTradingDisabledError: 默认交易开关关闭。
            HuaxinNativeUnavailableError: 即使开关误开，真实写后端仍未实现。
        """

        del security, amount, price, wait_timeout, remark, market, extra
        self._require_trading_enabled()
        self._raise_backend_not_implemented("sell")
        return ""

    async def cancel_order(self, order_id: str) -> bool:
        """在最靠近公共入口的位置拒绝撤单请求。

        Args:
            order_id: BulletTrade 稳定本地订单标识。

        Returns:
            当前阶段不会正常返回。

        Raises:
            HuaxinTradingDisabledError: 默认撤单开关关闭。
            HuaxinNativeUnavailableError: 即使开关误开，真实撤单后端仍未实现。
        """

        del order_id
        if not self._enable_cancel:
            raise HuaxinTradingDisabledError(
                HUAXIN_CANCEL_DISABLED,
                "华鑫撤单开关默认关闭",
                {"required_flag": "HUAXIN_ENABLE_CANCEL"},
            )
        self._raise_backend_not_implemented("cancel_order")
        return False

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """拒绝尚未接通的真实订单查询。

        Args:
            order_id: BulletTrade 稳定本地订单标识。

        Returns:
            当前阶段不会正常返回。

        Raises:
            HuaxinNativeUnavailableError: Trader 查询后端尚未实现。
        """

        del order_id
        self._raise_backend_not_implemented("get_order_status")
        return {}

    def _require_trading_enabled(self) -> None:
        """检查默认关闭的交易硬门禁。

        Returns:
            交易开关已开时返回 None。

        Raises:
            HuaxinTradingDisabledError: 交易开关关闭时抛出。
        """

        if not self._enable_trading:
            raise HuaxinTradingDisabledError(
                HUAXIN_TRADING_DISABLED,
                "华鑫交易开关默认关闭",
                {"required_flag": "HUAXIN_ENABLE_TRADING"},
            )

    @staticmethod
    def _raise_backend_not_implemented(operation: str) -> None:
        """将当前未接线操作转换为稳定 native unavailable 错误。

        Args:
            operation: 不含业务参数的公共操作名称。

        Returns:
            本函数不会正常返回。

        Raises:
            HuaxinNativeUnavailableError: 每次调用都抛出。
        """

        raise HuaxinNativeUnavailableError(
            HUAXIN_BACKEND_NOT_IMPLEMENTED,
            "华鑫真实 Trader bridge 尚未实现",
            {"operation": operation},
        )


__all__ = ["HuaxinBroker"]
