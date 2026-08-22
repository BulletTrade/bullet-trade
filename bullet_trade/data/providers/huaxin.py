"""
作者: BruceLee
文件职责: 华鑫 XMD L1 实时行情数据提供者，直接对接华鑫 XMD 行情前置。
主要输入: 标的代码（如 511880.XSHG）与环境变量中的 XMD 配置。
主要输出: 实时盘口快照字典与 SecurityUnitData 实例。
上下游关系: 供 get_current_data() / get_current_tick() / LiveEngine 直接调用。
关键环境或配置约定: 依赖 HUAXIN_XMD_PYTHON, HUAXIN_XMD_SDK_DIR, HUAXIN_XMD_FRONT。
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set

from ...core.models import SecurityUnitData
from ...integrations.huaxin.xmd_backend import Python37XmdBackend, XmdBackend
from .base import DataProvider

log = logging.getLogger(__name__)


class HuaxinDataProvider(DataProvider):
    """华鑫 XMD L1 实时行情数据提供者。

    在 Python 3.11 环境下直接拉起并管理 Python 3.7 XMD 行情 Sidecar，
    向策略提供极速原生 L1 行情切片，无需依赖独立的 QMT Server。
    """

    name = "huaxin"

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        """初始化华鑫数据源。

        参数:
            config: 可选配置字典，缺省时自动从环境变量读取。
        """
        super().__init__()
        self._cfg = dict(config or {})
        self._python_path = str(
            self._cfg.get("huaxin_xmd_python")
            or self._cfg.get("xmd_python")
            or os.getenv("HUAXIN_XMD_PYTHON")
            or "/usr/bin/python3.7"
        )
        self._sdk_dir = str(
            self._cfg.get("huaxin_xmd_sdk_dir")
            or self._cfg.get("xmd_sdk_dir")
            or os.getenv("HUAXIN_XMD_SDK_DIR")
            or ""
        )
        self._front = str(
            self._cfg.get("huaxin_xmd_front")
            or self._cfg.get("xmd_front")
            or os.getenv("HUAXIN_XMD_FRONT")
            or ""
        )
        self._max_age_seconds = float(
            self._cfg.get("huaxin_xmd_max_age_seconds")
            or os.getenv("HUAXIN_XMD_MAX_AGE_SECONDS")
            or 30.0
        )
        self._connect_timeout = float(
            self._cfg.get("huaxin_xmd_connect_timeout")
            or os.getenv("HUAXIN_XMD_CONNECT_TIMEOUT")
            or 15.0
        )
        self._command_timeout = float(
            self._cfg.get("huaxin_xmd_command_timeout")
            or os.getenv("HUAXIN_XMD_COMMAND_TIMEOUT")
            or 5.0
        )
        self._snapshot_timeout = float(
            self._cfg.get("huaxin_xmd_snapshot_timeout")
            or os.getenv("HUAXIN_XMD_SNAPSHOT_TIMEOUT")
            or 2.0
        )
        self._backend: Optional[XmdBackend] = None
        self._authenticated = False
        self._subscribed: Set[str] = set()

    def auth(
        self,
        user: str = "",
        pwd: str = "",
        host: str = "",
        port: Optional[int] = None,
    ) -> bool:
        """启动并鉴权 XMD 行情 backend。

        参数:
            user: 兼容参数，不使用。
            pwd: 兼容参数，不使用。
            host: 可覆盖 XMD 前置地址。
            port: 兼容参数。

        返回值:
            bool: 成功启动并连接 XMD 时返回 True。
        """
        if self._authenticated and self._backend is not None:
            return True

        front = host or self._front
        if not front:
            front = os.getenv("HUAXIN_XMD_FRONT", "")
        if not self._sdk_dir:
            self._sdk_dir = os.getenv("HUAXIN_XMD_SDK_DIR", "")

        backend = Python37XmdBackend(
            python_path=self._python_path,
            sdk_dir=self._sdk_dir,
            front=front,
            max_age_seconds=self._max_age_seconds,
            connect_timeout=self._connect_timeout,
            command_timeout=self._command_timeout,
        )
        try:
            backend.start()
            self._backend = backend
            self._authenticated = True
            log.info("✅ 华鑫 XMD L1 行情数据源启动成功: front=%s", front)
            return True
        except Exception as exc:
            log.error("❌ 华鑫 XMD L1 行情数据源启动失败: %s", exc)
            self._backend = None
            self._authenticated = False
            return False

    def is_authenticated(self) -> bool:
        """返回当前数据提供者是否已成功认证/启动。"""
        return self._authenticated and self._backend is not None

    def subscribe(self, security: str) -> bool:
        """订阅目标证券行情。

        参数:
            security: 标准代码（如 511880.XSHG）。

        返回值:
            bool: 订阅是否成功。
        """
        if not self._authenticated:
            self.auth()
        if self._backend is None:
            return False
        if security in self._subscribed:
            return True
        try:
            res = self._backend.subscribe(security)
            if res.get("ok"):
                self._subscribed.add(security)
                return True
        except Exception as exc:
            log.warning("华鑫 XMD 订阅失败 %s: %s", security, exc)
        return False

    def get_current_tick(self, security: str) -> Optional[Dict[str, Any]]:
        """获取目标标的的最新 L1 tick 快照。

        参数:
            security: 标准代码（如 511880.XSHG）。

        返回值:
            Optional[Dict[str, Any]]: 包含 last_price, high_limit, low_limit 等的快照字典。
        """
        if not self._authenticated:
            self.auth()
        if self._backend is None:
            return None

        if security not in self._subscribed:
            self.subscribe(security)

        try:
            latest = self._backend.get_latest(security, wait_timeout=self._snapshot_timeout)
            if not latest or latest.get("type") == "error":
                return None
            res = {
                "security": security,
                "last_price": float(latest.get("last_price") or latest.get("price") or 0.0),
                "high_limit": float(latest.get("high_limit") or 0.0),
                "low_limit": float(latest.get("low_limit") or 0.0),
                "source_time": latest.get("source_time"),
                "paused": bool(latest.get("paused", False)),
                "bid_price1": latest.get("bid_price1"),
                "ask_price1": latest.get("ask_price1"),
                "bid_volume1": latest.get("bid_volume1"),
                "ask_volume1": latest.get("ask_volume1"),
                "name": str(latest.get("name") or latest.get("security_name") or ""),
                "display_name": str(latest.get("display_name") or latest.get("short_name") or ""),
                "price_tick": float(latest.get("price_tick") or 0.01),
                "day_trading": bool(latest.get("day_trading", False)),
            }
            # 尝试从活动的 HuaxinBroker 静态主数据中补充中文名与跳价
            try:
                from bullet_trade.broker.registry import get_broker_instance
                broker = get_broker_instance("huaxin") if callable(get_broker_instance) else None
                if broker and hasattr(broker, "get_security_master"):
                    master = broker.get_security_master(security)
                    if master:
                        if not res["name"]:
                            res["name"] = str(master.get("security_name") or "")
                        if not res["display_name"]:
                            res["display_name"] = str(master.get("short_name") or master.get("security_name") or "")
                        if "price_tick" in master:
                            res["price_tick"] = float(master["price_tick"])
                        if "day_trading" in master:
                            res["day_trading"] = bool(master["day_trading"])
            except Exception:
                pass
            return res
        except Exception as exc:
            log.debug("获取华鑫实时行情快照失败 %s: %s", security, exc)
            return None

    def get_live_current(self, security: str) -> Optional[Dict[str, Any]]:
        """获取实盘实时行情，对齐 get_current_tick。"""
        return self.get_current_tick(security)

    def get_snapshot(self, security: str) -> Optional[SecurityUnitData]:
        """获取并构造 SecurityUnitData 实例。

        参数:
            security: 标准代码。

        返回值:
            Optional[SecurityUnitData]: 填充好的 SecurityUnitData 实例。
        """
        tick = self.get_current_tick(security)
        if not tick:
            return None
        last_price = float(tick.get("last_price") or tick.get("price") or 0.0)
        high_limit = float(tick.get("high_limit") or 0.0)
        low_limit = float(tick.get("low_limit") or 0.0)
        paused = bool(tick.get("paused", False))
        is_st = bool(tick.get("is_st", False))
        source_time = tick.get("source_time")
        if isinstance(source_time, str):
            try:
                source_time = datetime.fromisoformat(source_time)
            except Exception:
                source_time = None

        return SecurityUnitData(
            security=security,
            last_price=last_price,
            high_limit=high_limit,
            low_limit=low_limit,
            paused=paused,
            is_st=is_st,
            source_time=source_time,
            source="huaxin_xmd_l1",
            bid_price1=tick.get("bid_price1"),
            ask_price1=tick.get("ask_price1"),
            bid_volume1=tick.get("bid_volume1"),
            ask_volume1=tick.get("ask_volume1"),
            name=str(tick.get("name") or ""),
            display_name=str(tick.get("display_name") or ""),
            price_tick=float(tick.get("price_tick") or 0.01),
            day_trading=bool(tick.get("day_trading", False)),
        )

    def get_price(
        self,
        security: Any,
        start_date: Optional[Any] = None,
        end_date: Optional[Any] = None,
        frequency: str = "daily",
        fields: Optional[List[str]] = None,
        skip_paused: bool = False,
        fq: str = "pre",
        count: Optional[int] = None,
        panel: bool = True,
        fill_paused: bool = True,
        pre_factor_ref_date: Optional[Any] = None,
        prefer_engine: bool = False,
        force_no_engine: bool = False,
    ) -> Any:
        """华鑫 XMD 为只读实时切片源，历史 K 线返回空 DataFrame。"""
        import pandas as pd

        return pd.DataFrame()

    def get_trade_days(
        self,
        start_date: Optional[Any] = None,
        end_date: Optional[Any] = None,
        count: Optional[int] = None,
    ) -> List[Any]:
        """获取华鑫柜台权威交易日历。

        优先从华鑫柜台连接获取当日权威 TradingDay。
        """
        import pandas as pd

        cur_trading_day = None
        try:
            from bullet_trade.broker.registry import get_broker_instance
            broker = get_broker_instance("huaxin") if callable(get_broker_instance) else None
            if broker and hasattr(broker, "get_trading_day"):
                t_str = broker.get_trading_day()
                if t_str and len(t_str) == 8:
                    cur_trading_day = datetime.strptime(t_str, "%Y%m%d").date()
        except Exception:
            pass

        if cur_trading_day is None:
            # 尝试从系统当前工作日（排除周末）构建
            now = datetime.now()
            if now.weekday() < 5:
                cur_trading_day = now.date()

        if cur_trading_day is None:
            return []

        # 根据 start_date, end_date, count 进行过滤返回
        if end_date is not None:
            end_dt = pd.to_datetime(end_date).date()
            if start_date is not None:
                start_dt = pd.to_datetime(start_date).date()
                if start_dt <= cur_trading_day <= end_dt:
                    return [cur_trading_day]
                return []
            if cur_trading_day <= end_dt:
                return [cur_trading_day]
            return []

        return [cur_trading_day]

    def get_all_securities(self, types: Any = "stock", date: Optional[Any] = None) -> Any:
        """标的元数据：返回空 DataFrame。"""
        import pandas as pd

        return pd.DataFrame()

    def get_index_stocks(self, index_symbol: str, date: Optional[Any] = None) -> List[str]:
        """指数成分股：返回空列表。"""
        return []

    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[Any] = None,
        end_date: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        """分红送配：返回空列表。"""
        return []
