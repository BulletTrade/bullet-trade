from abc import ABC, abstractmethod
from typing import Union, List, Optional, Dict, Any
from datetime import date as Date
from datetime import datetime
import pandas as pd


class DataProvider(ABC):
    """
    抽象数据提供者接口。
    不同数据源（jqdatasdk、tushare、miniqmt等）实现该接口，
    以便在框架内可插拔切换数据来源。
    """
    name: str = "base"
    # 是否要求实时行情必须由 provider 提供（用于 live 模式防止回落到历史数据）
    requires_live_data: bool = False
    # 未复权是 DataProvider 基础合同；动态前复权必须由具体 provider 显式声明。
    supports_unadjusted_price_basis: bool = True
    supports_dynamic_pre_price_basis: bool = False

    def supports_effective_price_basis(
        self,
        *,
        fq: str,
        pre_factor_ref_date: Optional[Union[str, datetime, Date]] = None,
    ) -> bool:
        """证明当前 provider 是否支持指定有效价格口径。

        Args:
            fq: 规范化复权口径，目前支持 ``none`` 与 ``pre``。
            pre_factor_ref_date: 动态前复权参考日；``pre`` 口径必须提供。

        Returns:
            bool: provider 明确支持并承诺失败关闭时返回 True，否则返回 False。

        Side Effects:
            无；该能力检查不得认证、联网或切换数据源。
        """

        normalized = str(fq or "none").strip().lower()
        if normalized == "none":
            return bool(self.supports_unadjusted_price_basis)
        if normalized == "pre" and pre_factor_ref_date is not None:
            return bool(self.supports_dynamic_pre_price_basis)
        return False

    def auth(self, user: Optional[str] = None, pwd: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None) -> None:
        """执行数据源认证或初始化。可选。默认读取环境变量，也可传入账号参数。"""
        pass

    @abstractmethod
    def get_price(
        self,
        security: Union[str, List[str]],
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        frequency: str = 'daily',
        fields: Optional[List[str]] = None,
        skip_paused: bool = False,
        fq: str = 'pre',
        count: Optional[int] = None,
        panel: bool = True,
        fill_paused: bool = True,
        pre_factor_ref_date: Optional[Union[str, datetime]] = None,
        prefer_engine: bool = False,
        force_no_engine: bool = False,
    ) -> pd.DataFrame:
        pass


    @abstractmethod
    def get_trade_days(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        count: Optional[int] = None
    ) -> List[datetime]:
        pass

    @abstractmethod
    def get_all_securities(
        self,
        types: Union[str, List[str]] = 'stock',
        date: Optional[Union[str, datetime]] = None
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_index_stocks(
        self,
        index_symbol: str,
        date: Optional[Union[str, datetime]] = None
    ) -> List[str]:
        pass

    @abstractmethod
    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None
    ) -> List[Dict[str, Any]]:
        pass

    def __getattr__(self, name: str):
        """
        当 provider 缺少某方法时，允许回退到同一 provider 的 SDK/客户端。
        """
        try:
            resolver = object.__getattribute__(self, "_sdk_fallback")
        except AttributeError:
            resolver = None
        if callable(resolver):
            return resolver(name)
        raise AttributeError(name)

    def get_security_info(
        self,
        security: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        """
        返回指定标的的元信息，例如类型或子类型。
        默认返回空字典，可由具体数据提供者覆盖。
        """
        _ = date
        return {}

    def get_bars(
        self,
        security: Union[str, List[str]],
        count: int,
        unit: str = '1d',
        fields: Optional[List[str]] = None,
        include_now: bool = False,
        end_dt: Optional[Union[str, datetime]] = None,
        fq_ref_date: Union[int, datetime] = 1,
        df: bool = False,
    ) -> Any:
        """
        返回单标的或多标的的 K 线数据（可选实现）。
        默认抛出异常，具体数据源按需覆盖。
        """
        raise NotImplementedError("当前数据源未实现 get_bars")

    def get_ticks(
        self,
        security: str,
        end_dt: Union[str, datetime],
        start_dt: Optional[Union[str, datetime]] = None,
        count: Optional[int] = None,
        fields: Optional[List[str]] = None,
        skip: bool = False,
        df: bool = False,
    ) -> Any:
        """
        返回 tick 数据（可选实现）。
        默认抛出异常，具体数据源按需覆盖。
        """
        raise NotImplementedError("当前数据源未实现 get_ticks")

    def get_current_tick(
        self,
        security: str,
        dt: Optional[Union[str, datetime]] = None,
        df: bool = False,
    ) -> Optional[Any]:
        """
        返回指定时刻的最新 tick（可选实现）。
        默认抛出异常，具体数据源按需覆盖。
        """
        raise NotImplementedError("当前数据源未实现 get_current_tick")

    def get_extras(
        self,
        info: str,
        security_list: List[str],
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        df: bool = True,
        count: Optional[int] = None,
    ) -> Any:
        """
        返回扩展数据（可选实现）。
        默认抛出异常，具体数据源按需覆盖。
        """
        raise NotImplementedError("当前数据源未实现 get_extras")

    def get_fundamentals(
        self,
        query_object: Any,
        date: Optional[Union[str, datetime]] = None,
        statDate: Optional[str] = None,
    ) -> Any:
        """
        返回财务数据（可选实现）。
        默认抛出异常，具体数据源按需覆盖。
        """
        raise NotImplementedError("当前数据源未实现 get_fundamentals")

    def get_fundamentals_continuously(
        self,
        query_object: Any,
        end_date: Optional[Union[str, datetime]] = None,
        count: int = 1,
        panel: bool = True,
    ) -> Any:
        """
        返回连续财务数据（可选实现）。
        默认抛出异常，具体数据源按需覆盖。
        """
        raise NotImplementedError("当前数据源未实现 get_fundamentals_continuously")

    def get_index_weights(self, index_id: str, date: Optional[Union[str, datetime]] = None) -> Any:
        """返回指数权重（可选实现）。"""
        raise NotImplementedError("当前数据源未实现 get_index_weights")

    def get_industry_stocks(self, industry_code: str, date: Optional[Union[str, datetime]] = None) -> List[str]:
        """返回行业成分股（可选实现）。"""
        raise NotImplementedError("当前数据源未实现 get_industry_stocks")

    def get_industry(self, security: Union[str, List[str]], date: Optional[Union[str, datetime]] = None) -> Any:
        """返回标的行业信息（可选实现）。"""
        raise NotImplementedError("当前数据源未实现 get_industry")

    def get_concept_stocks(self, concept_code: str, date: Optional[Union[str, datetime]] = None) -> List[str]:
        """返回概念成分股（可选实现）。"""
        raise NotImplementedError("当前数据源未实现 get_concept_stocks")

    def get_concept(self, security: Union[str, List[str]], date: Optional[Union[str, datetime]] = None) -> Any:
        """返回标的概念信息（可选实现）。"""
        raise NotImplementedError("当前数据源未实现 get_concept")

    def get_fund_info(self, security: str, date: Optional[Union[str, datetime]] = None) -> Any:
        """返回基金信息（可选实现）。"""
        raise NotImplementedError("当前数据源未实现 get_fund_info")

    def get_margincash_stocks(self, date: Optional[Union[str, datetime]] = None) -> Any:
        """返回融资标的列表（可选实现）。"""
        raise NotImplementedError("当前数据源未实现 get_margincash_stocks")

    def get_marginsec_stocks(self, date: Optional[Union[str, datetime]] = None) -> Any:
        """返回融券标的列表（可选实现）。"""
        raise NotImplementedError("当前数据源未实现 get_marginsec_stocks")

    def get_dominant_future(self, underlying_symbol: str, date: Optional[Union[str, datetime]] = None) -> Any:
        """返回主力期货合约（可选实现）。"""
        raise NotImplementedError("当前数据源未实现 get_dominant_future")

    def get_future_contracts(self, underlying_symbol: str, date: Optional[Union[str, datetime]] = None) -> Any:
        """返回期货合约列表（可选实现）。"""
        raise NotImplementedError("当前数据源未实现 get_future_contracts")

    def get_billboard_list(
        self,
        stock_list: Optional[List[str]] = None,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        count: Optional[int] = None,
    ) -> Any:
        """返回龙虎榜数据（可选实现）。"""
        raise NotImplementedError("当前数据源未实现 get_billboard_list")

    def get_locked_shares(
        self,
        stock_list: List[str],
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        forward_count: Optional[int] = None,
    ) -> Any:
        """返回限售股数据（可选实现）。"""
        raise NotImplementedError("当前数据源未实现 get_locked_shares")

    def get_trade_day(self, security: Union[str, List[str]], query_dt: Union[str, datetime]) -> Any:
        """返回指定时间的交易日（可选实现）。"""
        raise NotImplementedError("当前数据源未实现 get_trade_day")

    def subscribe_ticks(self, symbols: List[str]) -> None:
        """
        订阅指定标的 tick（可选实现）。默认不操作。
        """
        return None

    def subscribe_markets(self, markets: List[str]) -> None:
        """
        订阅市场级 tick（可选实现，如 ['SH','SZ']）。默认不操作。
        """
        return None

    def unsubscribe_ticks(self, symbols: Optional[List[str]] = None) -> None:
        """
        取消 tick 订阅（可选实现）。symbols 为 None 表示全部取消。默认不操作。
        """
        return None

    def unsubscribe_markets(self, markets: Optional[List[str]] = None) -> None:
        """
        取消市场级 tick 订阅（可选实现）。默认不操作。
        """
        return None
