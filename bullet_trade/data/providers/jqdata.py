from typing import Union, List, Optional, Dict, Any, Set, Callable, Tuple
from datetime import datetime, date as Date
import logging
import os
import json
import pandas as pd
import jqdatasdk as jq
from jqdatasdk import finance, query

from .base import DataProvider
from ..cache import CacheManager

# 动态补丁：修复 jqdatasdk 的 get_price_engine 忽略 pre_factor_ref_date 的问题
@jq.utils.assert_auth
def _patched_get_price_engine(security, start_date=None, end_date=None,
                              frequency='daily', fields=None, skip_paused=False,
                              fq='pre', count=None, pre_factor_ref_date=None, panel=True):
    security = jq.utils.convert_security(security)
    start_date = jq.utils.to_date_str(start_date)
    end_date = jq.utils.to_date_str(end_date)
    pre_factor_ref_date = jq.utils.to_date_str(pre_factor_ref_date)
    return jq.client.JQDataClient.instance().get_price_engine(**locals())

# 应用补丁到 jqdatasdk
jq.get_price_engine = _patched_get_price_engine

logger = logging.getLogger(__name__)


class _FinanceColumnStub:
    def __init__(self, name: str) -> None:
        self.name = name

    def __eq__(self, _other: object) -> "_FinanceColumnStub":
        return self

    __ge__ = __le__ = __lt__ = __gt__ = __eq__

    def __repr__(self) -> str:
        return f"<FinanceColumnStub {self.name}>"


class _FinanceTableStub:
    def __getattr__(self, name: str) -> str:
        return _FinanceColumnStub(name)


_FINANCE_TABLE_STUB = _FinanceTableStub()


class JQDataProvider(DataProvider):
    name: str = "jqdatasdk"
    _DEFAULT_PRICE_FIELDS: List[str] = ['open', 'close', 'high', 'low', 'volume', 'money']
    _PRICE_SCALE_FIELDS: Set[str] = {
        'open', 'close', 'high', 'low', 'avg', 'price', 'high_limit', 'low_limit', 'pre_close'
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config = config or {}
        cache_dir = self.config.get('cache_dir')
        use_env_cache = 'cache_dir' not in self.config
        self._cache = CacheManager(
            provider_name=self.name,
            cache_dir=cache_dir,
            fallback_to_env=use_env_cache,
        )
        self._security_info_cache: Dict[str, Dict[str, Any]] = {}
        self._fund_membership_cache: Dict[str, Set[str]] = {}
        self._price_engine_supported: Optional[bool] = None
        self._security_overrides_loaded = False
        self._security_overrides: Dict[str, Any] = {}

    @staticmethod
    def _sanitize_env_value(value: str) -> str:
        return value.split('#', 1)[0].strip()

    @staticmethod
    def _parse_port(port: Optional[int], env_value: str) -> Optional[int]:
        if port is not None:
            return port
        cleaned = JQDataProvider._sanitize_env_value(env_value)
        if not cleaned:
            return None
        try:
            return int(cleaned)
        except ValueError:
            logger.warning("Invalid JQDATA_PORT value '%s'; ignoring custom port.", env_value)
            return None

    @staticmethod
    def _parse_host(host: Optional[str], env_value: str) -> str:
        if host:
            return host
        return JQDataProvider._sanitize_env_value(env_value)

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            val = float(value)
            if pd.isna(val):
                return None
            return val
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_ratio(row: Dict[str, Any], ratio_keys: List[str], number_key: str) -> float:
        for key in ratio_keys:
            ratio_val = JQDataProvider._safe_float(row.get(key))
            if ratio_val is not None:
                return ratio_val

        number_val = JQDataProvider._safe_float(row.get(number_key))
        if number_val is None or number_val == 0.0:
            return 0.0

        base_candidates = [
            "distributed_share_base_implement",
            "distributed_share_base_board",
            "distributed_share_base_shareholders",
            "total_capital_before_transfer",
            "float_capital_before_transfer",
        ]
        for base_key in base_candidates:
            base_val = JQDataProvider._safe_float(row.get(base_key))
            if base_val and base_val > 0:
                return (number_val / base_val) * 10.0

        if number_val < 100.0:
            return number_val

        logger.warning(
            "无法推断送股/转增比例，疑似聚宽字段含总股本。字段=%s, 值=%s",
            number_key,
            number_val,
        )
        return 0.0

    def auth(self, user: Optional[str] = None, pwd: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None) -> None:
        cfg_server = self.config.get('server') or os.getenv('JQDATA_SERVER', '')
        jq_server = self._parse_host(host, cfg_server)
        cfg_port = self.config.get('port')
        jq_port_env = str(cfg_port) if cfg_port is not None else os.getenv('JQDATA_PORT', '')
        jq_port = self._parse_port(port, jq_port_env)
        jq_user = (
            user
            or self.config.get('username')
            or os.getenv('JQDATA_USERNAME')
            or os.getenv('JQDATA_USER', '')
        )
        jq_pwd = (
            pwd
            or self.config.get('password')
            or os.getenv('JQDATA_PASSWORD')
            or os.getenv('JQDATA_PWD', '')
        )
        # 允许空host/port走默认
        if jq_user:
            try:
                if jq_server and jq_port:
                    jq.auth(jq_user, jq_pwd, host=jq_server, port=jq_port)
                else:
                    jq.auth(jq_user, jq_pwd)
            except Exception as e:
                # 忽略认证失败，由调用方处理
                logger.error("Failed to authenticate with JQData: %s", e)
                raise e

    def get_price(self, security: Union[str, List[str]], start_date: Optional[Union[str, datetime]] = None,
                  end_date: Optional[Union[str, datetime]] = None, frequency: str = 'daily',
                  fields: Optional[List[str]] = None, skip_paused: bool = False, fq: str = 'pre',
                  count: Optional[int] = None, panel: bool = True, fill_paused: bool = True,
                  pre_factor_ref_date: Optional[Union[str, datetime]] = None, prefer_engine: bool = False,
                  force_no_engine: bool = False) -> pd.DataFrame:
        kwargs = {
            'security': security,
            'start_date': start_date,
            'end_date': end_date,
            'frequency': frequency,
            'fields': fields,
            'skip_paused': skip_paused,
            'fq': fq,
            'count': count,
            'panel': panel,
            'fill_paused': fill_paused,
            'pre_factor_ref_date': pre_factor_ref_date,
            'prefer_engine': prefer_engine,
            'force_no_engine': force_no_engine,
        }

        def _fetch_price_engine(kw: Dict[str, Any]) -> pd.DataFrame:
            return jq.get_price_engine(
                security=kw['security'],
                start_date=kw.get('start_date'),
                end_date=kw.get('end_date'),
                frequency=kw.get('frequency', 'daily'),
                fields=kw.get('fields'),
                skip_paused=kw.get('skip_paused', False),
                fq=kw.get('fq', 'pre'),
                count=kw.get('count'),
                pre_factor_ref_date=kw.get('pre_factor_ref_date'),
                panel=kw.get('panel', True),
            )

        def _fetch_price(kw: Dict[str, Any]) -> pd.DataFrame:
            fq_val = kw.get('fq', 'pre')
            # Normalize 'none' string to None for jqdatasdk to return unadjusted prices
            if isinstance(fq_val, str) and fq_val.lower() == 'none':
                fq_param = None
            else:
                fq_param = fq_val
            return jq.get_price(
                security=kw['security'],
                start_date=kw.get('start_date'),
                end_date=kw.get('end_date'),
                frequency=kw.get('frequency', 'daily'),
                fields=kw.get('fields'),
                skip_paused=kw.get('skip_paused', False),
                fq=fq_param,
                count=kw.get('count'),
                panel=kw.get('panel', True),
                fill_paused=kw.get('fill_paused', True),
            )

        force_no_engine = bool(force_no_engine)
        # If a pre_factor_ref_date is explicitly provided for forward-adjusted data,
        # route to get_price_engine so the parameter is honored by jqdatasdk.
        should_try_engine = (
            (prefer_engine or pre_factor_ref_date is not None)
            and fq == 'pre'
            and not force_no_engine
        )

        if force_no_engine and fq == 'pre' and pre_factor_ref_date is not None:
            result = self._manual_prefactor_fallback(
                kwargs,
                _fetch_price,
                fields,
                pre_factor_ref_date,
                fq,
            )
            return self._round_price_result(result, security) if fq == 'pre' else result

        if should_try_engine and self._price_engine_supported is not False:
            try:
                result = self._cache.cached_call('get_price_engine', kwargs, _fetch_price_engine, result_type='df')
                if self._price_engine_supported is None:
                    self._price_engine_supported = True
                return self._round_price_result(result, security) if fq == 'pre' else result
            except Exception as exc:
                if self._is_engine_missing_error(exc):
                    if self._price_engine_supported is not False:
                        logger.debug("检测到 get_price_engine 不可用，使用复权因子回退: %s", exc)
                    self._price_engine_supported = False
                    result = self._manual_prefactor_fallback(
                        kwargs,
                        _fetch_price,
                        fields,
                        pre_factor_ref_date,
                        fq,
                    )
                    return self._round_price_result(result, security) if fq == 'pre' else result
                raise

        if should_try_engine and self._price_engine_supported is False:
            result = self._manual_prefactor_fallback(
                kwargs,
                _fetch_price,
                fields,
                pre_factor_ref_date,
                fq,
            )
            return self._round_price_result(result, security) if fq == 'pre' else result

        result = self._cache.cached_call('get_price', kwargs, _fetch_price, result_type='df')
        return self._round_price_result(result, security) if fq == 'pre' else result

    def get_security_info(
        self,
        security: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        if not security:
            return {}
        cache_key = (security, date) if date is not None else (security, None)
        cached = self._security_info_cache.get(cache_key)
        if cached is not None:
            return cached

        result: Dict[str, Any] = {}
        try:
            if date is not None:
                sec_info = jq.get_security_info(security, date=date)
            else:
                sec_info = jq.get_security_info(security)
        except Exception as exc:
            logger.debug("Failed to fetch security info for %s: %s", security, exc)
            sec_info = None

        if sec_info is not None:
            result_type = getattr(sec_info, 'type', None)
            subtype = getattr(sec_info, 'subtype', None)
            display_name = getattr(sec_info, 'display_name', None)
            name = getattr(sec_info, 'name', None)
            start_date = getattr(sec_info, 'start_date', None)
            end_date = getattr(sec_info, 'end_date', None)
            parent = getattr(sec_info, 'parent', None)
            price_decimals = getattr(sec_info, 'price_decimals', None)
            tick_size = None
            if hasattr(sec_info, "get_tick_size"):
                try:
                    tick_size = sec_info.get_tick_size()
                except Exception:
                    tick_size = None
            if result_type:
                result['type'] = result_type
            if subtype:
                result['subtype'] = subtype
            if display_name:
                result['display_name'] = display_name
            if name:
                result['name'] = name
            if start_date:
                result['start_date'] = start_date
            if end_date:
                result['end_date'] = end_date
            if parent:
                result['parent'] = parent
            if price_decimals is not None:
                result['price_decimals'] = price_decimals
            if tick_size is not None:
                result['tick_size'] = tick_size

        result['code'] = security

        if result.get('type') == 'fund' and not result.get('subtype'):
            inferred = self._infer_fund_subtype(security)
            if inferred:
                result['subtype'] = inferred

        self._security_info_cache[cache_key] = result
        return result

    def _infer_fund_subtype(self, security: str) -> Optional[str]:
        for alias, canonical in (
            ('money_market_fund', 'money_market_fund'),
            ('mmf', 'money_market_fund'),
        ):
            members = self._get_fund_members(alias)
            if security in members:
                return canonical
        return None

    def _get_fund_members(self, subtype: str) -> Set[str]:
        cached = self._fund_membership_cache.get(subtype)
        if cached is not None:
            return cached
        members: Set[str] = set()
        try:
            df = jq.get_all_securities([subtype])
        except Exception as exc:
            logger.debug("Failed to load fund list for subtype %s: %s", subtype, exc)
        else:
            members = set(df.index) if df is not None else set()
        self._fund_membership_cache[subtype] = members
        return members


    def get_trade_days(self, start_date: Optional[Union[str, datetime]] = None,
                        end_date: Optional[Union[str, datetime]] = None,
                        count: Optional[int] = None) -> List[datetime]:
        if start_date is None and end_date is None and count is None:
            end_date = Date.today()
        kwargs = {
            'start_date': start_date,
            'end_date': end_date,
            'count': count,
        }

        def _fetch(kw: Dict[str, Any]) -> List[datetime]:
            return jq.get_trade_days(start_date=kw.get('start_date'), end_date=kw.get('end_date'), count=kw.get('count'))

        return self._cache.cached_call('get_trade_days', kwargs, _fetch, result_type='list_date')

    def get_all_securities(self, types: Union[str, List[str]] = 'stock',
                           date: Optional[Union[str, datetime]] = None) -> pd.DataFrame:
        kwargs = {
            'types': types,
            'date': date,
        }

        def _fetch(kw: Dict[str, Any]) -> pd.DataFrame:
            return jq.get_all_securities(types=kw.get('types', 'stock'), date=kw.get('date'))

        return self._cache.cached_call('get_all_securities', kwargs, _fetch, result_type='df')

    def get_index_stocks(self, index_symbol: str, date: Optional[Union[str, datetime]] = None) -> List[str]:
        kwargs = {
            'index_symbol': index_symbol,
            'date': date,
        }

        def _fetch(kw: Dict[str, Any]) -> List[str]:
            return jq.get_index_stocks(kw.get('index_symbol'), date=kw.get('date'))

        return self._cache.cached_call('get_index_stocks', kwargs, _fetch, result_type='list_str')

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
        return jq.get_bars(
            security,
            count=count,
            unit=unit,
            fields=fields,
            include_now=include_now,
            end_dt=end_dt,
            fq_ref_date=fq_ref_date,
            df=df,
        )

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
        return jq.get_ticks(
            security,
            start_dt=start_dt,
            end_dt=end_dt,
            count=count,
            fields=fields,
            skip=skip,
            df=df,
        )

    def get_current_tick(
        self,
        security: str,
        dt: Optional[Union[str, datetime]] = None,
        df: bool = False,
    ) -> Any:
        tick = jq.get_current_tick(security)
        if not df:
            return tick
        if tick is None:
            return pd.DataFrame()
        if isinstance(tick, pd.DataFrame):
            return tick
        if isinstance(tick, dict):
            return pd.DataFrame([tick])
        try:
            return pd.DataFrame([vars(tick)])
        except Exception:
            return pd.DataFrame([{'value': tick}])

    def get_extras(
        self,
        info: str,
        security_list: List[str],
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        df: bool = True,
        count: Optional[int] = None,
    ) -> Any:
        return jq.get_extras(
            info,
            security_list,
            start_date=start_date,
            end_date=end_date,
            df=df,
            count=count,
        )

    def get_fundamentals(
        self,
        query_object: Any,
        date: Optional[Union[str, datetime]] = None,
        statDate: Optional[str] = None,
    ) -> Any:
        return jq.get_fundamentals(query_object, date=date, statDate=statDate)

    def get_fundamentals_continuously(
        self,
        query_object: Any,
        end_date: Optional[Union[str, datetime]] = None,
        count: int = 1,
        panel: bool = True,
    ) -> Any:
        return jq.get_fundamentals_continuously(query_object, end_date=end_date, count=count, panel=panel)

    def get_index_weights(self, index_id: str, date: Optional[Union[str, datetime]] = None) -> Any:
        return jq.get_index_weights(index_id, date=date)

    def get_industry_stocks(self, industry_code: str, date: Optional[Union[str, datetime]] = None) -> List[str]:
        return jq.get_industry_stocks(industry_code, date=date)

    def get_industry(self, security: Union[str, List[str]], date: Optional[Union[str, datetime]] = None) -> Any:
        security = jq.utils.convert_security(security)
        date_str = jq.utils.to_date_str(date) if date is not None else None
        try:
            return jq.client.JQDataClient.instance().get_industry(security=security, date=date_str)
        except TypeError:
            return jq.get_industry(security, date=date)

    def get_concept_stocks(self, concept_code: str, date: Optional[Union[str, datetime]] = None) -> List[str]:
        return jq.get_concept_stocks(concept_code, date=date)

    def get_concept(self, security: Union[str, List[str]], date: Optional[Union[str, datetime]] = None) -> Any:
        return jq.get_concept(security, date=date)

    def get_fund_info(self, security: str, date: Optional[Union[str, datetime]] = None) -> Any:
        return jq.get_fund_info(security, date=date)

    def get_margincash_stocks(self, date: Optional[Union[str, datetime]] = None) -> Any:
        return jq.get_margincash_stocks(date)

    def get_marginsec_stocks(self, date: Optional[Union[str, datetime]] = None) -> Any:
        return jq.get_marginsec_stocks(date)

    def get_dominant_future(self, underlying_symbol: str, date: Optional[Union[str, datetime]] = None) -> Any:
        query_date = date or datetime.now()
        return jq.get_dominant_future(underlying_symbol, date=query_date)

    def get_future_contracts(self, underlying_symbol: str, date: Optional[Union[str, datetime]] = None) -> Any:
        query_date = date or datetime.now()
        return jq.get_future_contracts(underlying_symbol, date=query_date)

    def get_billboard_list(
        self,
        stock_list: Optional[List[str]] = None,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        count: Optional[int] = None,
    ) -> Any:
        if start_date is None and count is None and end_date is not None:
            start_date = end_date
        start_str = jq.utils.to_date_str(start_date) if start_date is not None else None
        end_str = jq.utils.to_date_str(end_date) if end_date is not None else None
        return jq.get_billboard_list(stock_list=stock_list, start_date=start_str, end_date=end_str, count=count)

    def get_locked_shares(
        self,
        stock_list: List[str],
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        forward_count: Optional[int] = None,
    ) -> Any:
        start_str = jq.utils.to_date_str(start_date) if start_date is not None else None
        end_str = jq.utils.to_date_str(end_date) if end_date is not None else None
        return jq.get_locked_shares(
            stock_list, start_date=start_str, end_date=end_str, forward_count=forward_count
        )

    def get_trade_day(self, security: Union[str, List[str]], query_dt: Union[str, datetime]) -> Any:
        if hasattr(jq, 'get_trade_day'):
            return jq.get_trade_day(security, query_dt)
        try:
            trade_days = jq.get_trade_days(end_date=query_dt, count=1)
        except Exception:
            trade_days = []
        if trade_days is None or len(trade_days) == 0:
            last_day = None
        else:
            last_value = trade_days[-1]
            try:
                last_day = pd.to_datetime(last_value).date()
            except Exception:
                last_day = last_value

        if isinstance(security, (list, tuple, set)):
            securities = list(security)
        else:
            securities = [security]
        return {str(sec): last_day for sec in securities}

    # ------------------------ Live 快照 ------------------------
    def get_live_current(self, security: str) -> Dict[str, Any]:
        """
        返回实盘当前快照（最小字段）基于 jqdatasdk：
        - last_price: 当前价（get_current_tick 或 get_price 最近值）
        - high_limit/low_limit: 当日涨跌停价（若可获取）
        - paused: 默认 False（无停牌实时接口时退化）
        若无法获取，返回空字典。
        """
        try:
            today = datetime.now().date()
            last_price = None
            high_limit = 0.0
            low_limit = 0.0
            paused = False

            # 1) 优先使用 get_current_tick（若可用）
            try:
                if hasattr(jq, 'get_current_tick'):
                    tick = jq.get_current_tick(security)
                    if tick is not None:
                        last_price = tick.get('last_price') if isinstance(tick, dict) else getattr(tick, 'last_price', None)
            except Exception:
                pass

            # 2) 获取当日涨跌停（多路径）：
            #    a) get_price(近一分钟) 自带 high_limit/low_limit 字段
            #    b) get_security_info 尝试读取
            #    c) 计算推断：昨收 * (1±limit_ratio)，limit_ratio 通过 is_st 判定 0.05/0.1
            min_df = None
            try:
                min_df = jq.get_price(
                    security,
                    count=1,
                    end_date=datetime.now(),
                    frequency='minute',
                    fields=['close', 'high_limit', 'low_limit']
                )
            except Exception:
                min_df = None
            if min_df is not None and not min_df.empty:
                row = min_df.iloc[-1]
                if last_price is None:
                    last_price = float(row.get('close') or 0.0)
                if 'high_limit' in row and pd.notna(row.get('high_limit')):
                    high_limit = float(row.get('high_limit') or 0.0)
                if 'low_limit' in row and pd.notna(row.get('low_limit')):
                    low_limit = float(row.get('low_limit') or 0.0)

            # b) get_security_info（若支持）
            try:
                info = jq.get_security_info(security)
                if info is not None:
                    if not high_limit:
                        high_limit = float(getattr(info, 'high_limit', 0.0) or 0.0)
                    if not low_limit:
                        low_limit = float(getattr(info, 'low_limit', 0.0) or 0.0)
            except Exception:
                pass

            # c) 推断涨跌停：若仍为 0 则根据昨收与 ST 推断
            if (not high_limit) or (not low_limit):
                try:
                    # 昨收
                    daily = jq.get_price(security, count=2, end_date=today, frequency='daily', fields=['close'])
                    pre_close = None
                    if daily is not None and not daily.empty:
                        last_idx = daily.index[-1]
                        if hasattr(last_idx, "date"):
                            last_date = last_idx.date()
                        else:
                            last_date = Date.fromisoformat(str(last_idx))
                        if last_date == today and len(daily) >= 2:
                            pre_close = float(daily['close'].iloc[-2])
                        else:
                            pre_close = float(daily['close'].iloc[-1])
                    # ST 判定
                    is_st_val = False
                    try:
                        if hasattr(jq, 'get_extras'):
                            st_df = jq.get_extras('is_st', [security], start_date=today, end_date=today)
                            if st_df is not None and not st_df.empty:
                                is_st_val = bool(st_df.iloc[-1].values[0])
                    except Exception:
                        pass
                    if pre_close is not None and pre_close > 0:
                        ratio = 0.05 if is_st_val else 0.10
                        if not high_limit:
                            high_limit = round(pre_close * (1 + ratio), 4)
                        if not low_limit:
                            low_limit = round(pre_close * (1 - ratio), 4)
                except Exception:
                    pass

            # 3) 停牌判定：尝试使用 get_extras('is_paused') 或 'paused'
            try:
                if hasattr(jq, 'get_extras'):
                    paused_df = None
                    try:
                        paused_df = jq.get_extras('is_paused', [security], start_date=today, end_date=today)
                    except Exception:
                        paused_df = None
                    if paused_df is None or paused_df.empty:
                        try:
                            paused_df = jq.get_extras('paused', [security], start_date=today, end_date=today)
                        except Exception:
                            paused_df = None
                    if paused_df is not None and not paused_df.empty:
                        paused = bool(paused_df.iloc[-1].values[0])
            except Exception:
                pass

            if last_price is None:
                return {}
            return {
                'last_price': float(last_price),
                'high_limit': float(high_limit or 0.0),
                'low_limit': float(low_limit or 0.0),
                'paused': bool(paused),
            }
        except Exception:
            return {}

    # ------------------------ 分红/拆分 ------------------------
    @staticmethod
    def _to_date(d: Optional[Union[str, datetime, Date]]) -> Optional[Date]:
        if d is None:
            return None
        if isinstance(d, Date) and not isinstance(d, datetime):
            return d
        try:
            return pd.to_datetime(d).date()
        except Exception:
            return None

    def _infer_security_type(self, security: str, ref_date: Optional[Date]) -> str:
        try:
            for t in ['stock', 'etf', 'lof', 'fund', 'fja', 'fjb']:
                df = self.get_all_securities(types=t, date=ref_date)
                if not df.empty and security in df.index:
                    return t
        except Exception:
            pass
        return 'stock'

    def get_split_dividend(self, security: str,
                           start_date: Optional[Union[str, datetime, Date]] = None,
                           end_date: Optional[Union[str, datetime, Date]] = None) -> List[Dict[str, Any]]:
        kwargs = {
            'security': security,
            'start_date': start_date,
            'end_date': end_date,
        }

        def _fetch(kw: Dict[str, Any]) -> List[Dict[str, Any]]:
            security_i = kw['security']
            sd = self._to_date(kw.get('start_date'))
            ed = self._to_date(kw.get('end_date'))
            if sd is None or ed is None:
                # Provider层要求明确日期
                return []
            sec_type = self._infer_security_type(security_i, ed)
            code_num = security_i.split('.')[0]
            events: List[Dict[str, Any]] = []
            try:
                if sec_type in ('fja', 'fjb'):
                    try:
                        table = finance.FUND_MF_DAILY_PROFIT
                    except Exception:
                        table = _FINANCE_TABLE_STUB
                    q = query(table).filter(
                        table.code == code_num,
                        table.day >= sd,
                        table.day <= ed
                    )
                    df = finance.run_query(q)
                    for _, row in df.iterrows():
                        daily_profit = float(row.get('daily_profit', 0.0) or 0.0)
                        events.append({
                            'security': security_i,
                            'date': row['day'],
                            'security_type': sec_type,
                            'scale_factor': 1.0,
                            'bonus_pre_tax': daily_profit / 10000.0,
                            'per_base': 1,
                        })
                elif sec_type in ('fund', 'etf', 'lof'):
                    try:
                        table = finance.FUND_DIVIDEND
                    except Exception:
                        table = _FINANCE_TABLE_STUB
                    q = query(table).filter(
                        table.code == code_num,
                        table.ex_date >= sd,
                        table.ex_date <= ed
                    )
                    df = finance.run_query(q)
                    for _, row in df.iterrows():
                        proportion = float(row.get('proportion', 0.0) or 0.0)
                        split_ratio = row.get('split_ratio', None)
                        try:
                            scale_factor = float(split_ratio) if split_ratio is not None else 1.0
                        except Exception:
                            scale_factor = 1.0
                        events.append({
                            'security': security_i,
                            'date': row.get('ex_date') or row.get('record_date'),
                            'security_type': sec_type,
                            'scale_factor': scale_factor,
                            # 聚宽事件口径：基金/ETF/LOF的 proportion 视为“每份派息”，按每1份为基数计算
                            'bonus_pre_tax': proportion,
                            'per_base': 1,
                        })
                else:
                    try:
                        table = finance.STK_XR_XD
                    except Exception:
                        table = _FINANCE_TABLE_STUB
                    q = query(table).filter(
                        table.code == security_i,
                        table.a_xr_date >= sd,
                        table.a_xr_date <= ed
                    )
                    df = finance.run_query(q)
                    for _, row in df.iterrows():
                        bonus_rmb = float(row.get('bonus_ratio_rmb', 0.0) or 0.0)
                        stock_paid = self._extract_ratio(
                            row,
                            ['dividend_ratio', 'stock_dividend_ratio'],
                            'dividend_number',
                        )
                        into_shares = self._extract_ratio(
                            row,
                            ['transfer_ratio', 'stock_transfer_ratio'],
                            'transfer_number',
                        )
                        per_base = 10
                        try:
                            scale_factor = 1.0 + (stock_paid + into_shares) / per_base
                        except Exception:
                            scale_factor = 1.0
                        events.append({
                            'security': security_i,
                            'date': row.get('a_xr_date') or row.get('a_bonus_date'),
                            'security_type': 'stock',
                            'scale_factor': scale_factor,
                            'bonus_pre_tax': bonus_rmb,
                            'per_base': per_base,
                        })
            except Exception:
                pass
            return events

        return self._cache.cached_call('get_split_dividend', kwargs, _fetch, result_type='list_dict')

    def _manual_prefactor_fallback(
        self,
        kwargs: Dict[str, Any],
        fetch_fn: Callable[[Dict[str, Any]], pd.DataFrame],
        requested_fields: Optional[Union[List[str], str]],
        pre_factor_ref_date: Optional[Union[str, datetime]],
        fq: str,
    ) -> pd.DataFrame:
        fallback_kwargs = dict(kwargs)
        fallback_kwargs['prefer_engine'] = False
        fallback_kwargs['fq'] = 'pre'
        fields_with_factor, added_factor = self._prepare_fields_with_factor(requested_fields)
        fallback_kwargs['fields'] = fields_with_factor
        result = self._cache.cached_call('get_price', fallback_kwargs, fetch_fn, result_type='df')
        result = self._deflate_prefactor_result(result)
        factor_ref_map = self._fetch_factor_ref_map(kwargs.get("security"), pre_factor_ref_date)
        return self._apply_prefactor_adjustment(
            result,
            fq=fq,
            drop_factor=added_factor,
            factor_ref_map=factor_ref_map,
        )

    def _prepare_fields_with_factor(
        self,
        fields: Optional[Union[List[str], str]]
    ) -> Tuple[Optional[List[str]], bool]:
        if fields is None:
            enriched = list(self._DEFAULT_PRICE_FIELDS)
            enriched.append('factor')
            return enriched, True
        if isinstance(fields, str):
            normalized = [fields]
        elif isinstance(fields, tuple):
            normalized = list(fields)
        else:
            normalized = list(fields)
        if 'factor' in normalized:
            return normalized, False
        normalized.append('factor')
        return normalized, True

    def _apply_prefactor_adjustment(
        self,
        data: Any,
        fq: str,
        drop_factor: bool,
        factor_ref_map: Optional[Dict[str, float]] = None,
    ) -> Any:
        if data is None:
            return data
        if fq != 'pre':
            return self._drop_factor_from_result(data) if drop_factor else data
        try:
            working = data.copy()
        except Exception:
            working = data
        adjusted = self._adjust_dataframe_result(working, factor_ref_map)
        if drop_factor:
            adjusted = self._drop_factor_from_result(adjusted)
        return adjusted

    def _deflate_prefactor_result(self, data: Any) -> Any:
        if data is None or not isinstance(data, pd.DataFrame) or data.empty:
            return data
        result_df = data.copy()
        cols = result_df.columns
        if isinstance(cols, pd.MultiIndex):
            top_levels = list(cols.get_level_values(0))
            if 'factor' not in top_levels:
                return result_df
            try:
                factor_block = result_df.xs('factor', axis=1, level=0)
            except Exception:
                return result_df
            numeric_block = factor_block.apply(pd.to_numeric, errors='coerce')
            numeric_block.replace(0.0, float('nan'), inplace=True)
            ratio_df = 1.0 / numeric_block
            ratio_df.replace([float('inf'), float('-inf')], float('nan'), inplace=True)
            ratio_df = ratio_df.ffill().bfill()
            ratio_df.fillna(1.0, inplace=True)
            for field in self._PRICE_SCALE_FIELDS:
                if field in top_levels:
                    try:
                        value_block = result_df.xs(field, axis=1, level=0)
                    except Exception:
                        continue
                    scaled = value_block.multiply(ratio_df, fill_value=0.0)
                    for code in scaled.columns:
                        result_df[(field, code)] = scaled[code]
            return result_df

        base_cols = list(result_df.columns)
        if 'factor' not in base_cols:
            return result_df
        factor_series = pd.to_numeric(result_df['factor'], errors='coerce')
        factor_series.replace(0.0, float('nan'), inplace=True)
        ratio = 1.0 / factor_series
        ratio.replace([float('inf'), float('-inf')], float('nan'), inplace=True)
        ratio = ratio.ffill().bfill()
        ratio.fillna(1.0, inplace=True)
        for field in self._PRICE_SCALE_FIELDS:
            if field in result_df.columns:
                result_df[field] = result_df[field].multiply(ratio, fill_value=0.0)
        return result_df

    def _adjust_dataframe_result(self, data: Any, factor_ref_map: Optional[Dict[str, float]] = None) -> Any:
        if isinstance(data, pd.DataFrame):
            return self._adjust_dataframe(data, factor_ref_map)
        if hasattr(data, 'to_frame'):
            try:
                df = data.to_frame()
            except Exception:
                return data
            return self._adjust_dataframe(df, factor_ref_map)
        return data

    def _adjust_dataframe(self, df: pd.DataFrame, factor_ref_map: Optional[Dict[str, float]] = None) -> pd.DataFrame:
        if df.empty:
            return df
        result_df = df.copy()
        cols = result_df.columns
        if isinstance(cols, pd.MultiIndex):
            top_levels = list(cols.get_level_values(0))
            if 'factor' not in top_levels:
                return result_df
            try:
                factor_block = result_df.xs('factor', axis=1, level=0)
            except Exception:
                return result_df
            ratio_df = self._compute_ratio_frame(factor_block, factor_ref_map)
            if ratio_df is None:
                return result_df
            for field in self._PRICE_SCALE_FIELDS:
                if field in top_levels:
                    try:
                        value_block = result_df.xs(field, axis=1, level=0)
                    except Exception:
                        continue
                    scaled = value_block.multiply(ratio_df, fill_value=0.0)
                    for code in scaled.columns:
                        result_df[(field, code)] = scaled[code]
            return result_df

        base_cols = list(result_df.columns)
        has_factor = 'factor' in base_cols
        if has_factor and 'code' in base_cols and 'time' in base_cols:
            return self._adjust_long_dataframe(result_df, factor_ref_map)
        if has_factor:
            factor_ref = None
            if factor_ref_map:
                factor_ref = next(iter(factor_ref_map.values()))
            ratio_series = self._compute_ratio_series(result_df['factor'], factor_ref)
            for field in self._PRICE_SCALE_FIELDS:
                if field in result_df.columns:
                    result_df[field] = result_df[field].multiply(ratio_series, fill_value=0.0)
            return result_df
        return result_df

    def _adjust_long_dataframe(self, df: pd.DataFrame, factor_ref_map: Optional[Dict[str, float]] = None) -> pd.DataFrame:
        working = df.copy()
        if 'time' not in working.columns or 'code' not in working.columns or 'factor' not in working.columns:
            return working
        if factor_ref_map:
            for code, factor_ref in factor_ref_map.items():
                mask = working['code'] == code
                if not mask.any():
                    continue
                ratio = self._compute_ratio_series(working.loc[mask, 'factor'], factor_ref)
                for field in self._PRICE_SCALE_FIELDS:
                    if field in working.columns:
                        working.loc[mask, field] = working.loc[mask, field] * ratio
        else:
            ratio = self._compute_ratio_series(working['factor'], None)
            for field in self._PRICE_SCALE_FIELDS:
                if field in working.columns:
                    working[field] = working[field] * ratio
        return working

    def _compute_ratio_frame(
        self,
        factor_df: pd.DataFrame,
        factor_ref_map: Optional[Dict[str, float]] = None,
    ) -> Optional[pd.DataFrame]:
        if factor_df is None or factor_df.empty:
            return None
        ratio_columns: Dict[str, pd.Series] = {}
        for col in factor_df.columns:
            factor_ref = None
            if factor_ref_map is not None:
                factor_ref = factor_ref_map.get(str(col))
            ratio_columns[col] = self._compute_ratio_series(factor_df[col], factor_ref)
        ratio_df = pd.DataFrame(ratio_columns)
        ratio_df = ratio_df.reindex(factor_df.index)
        ratio_df.replace([float('inf'), float('-inf')], float('nan'), inplace=True)
        ratio_df = ratio_df.ffill().bfill()
        ratio_df.fillna(1.0, inplace=True)
        return ratio_df

    def _compute_ratio_series(self, series: pd.Series, factor_ref: Optional[float]) -> pd.Series:
        if series is None or series.empty:
            return pd.Series([], index=series.index if isinstance(series, pd.Series) else None, dtype=float)
        denom = pd.to_numeric(series, errors='coerce')
        denom.replace(0.0, float('nan'), inplace=True)
        if factor_ref in (None, 0.0):
            factor_ref = 1.0
        ratio = denom / factor_ref
        ratio.replace([float('inf'), float('-inf')], float('nan'), inplace=True)
        ratio = ratio.ffill().bfill()
        ratio.fillna(1.0, inplace=True)
        return ratio

    def _drop_factor_from_result(self, data: Any) -> Any:
        if isinstance(data, pd.DataFrame):
            if isinstance(data.columns, pd.MultiIndex):
                if 'factor' in data.columns.get_level_values(0):
                    return data.drop(columns='factor', level=0)
                return data
            if 'factor' in data.columns:
                return data.drop(columns=['factor'])
            return data
        return data

    def _fetch_factor_ref_map(
        self,
        security: Union[str, List[str], None],
        ref_date: Optional[Union[str, datetime]],
    ) -> Dict[str, float]:
        if not security or ref_date is None:
            return {}
        try:
            ref_day = pd.to_datetime(ref_date).date()
        except Exception:
            return {}
        securities = [security] if isinstance(security, str) else list(security)
        if not securities:
            return {}
        kwargs = {
            "security": securities,
            "end_date": ref_day,
            "frequency": "daily",
            "fields": ["factor"],
            "count": 1,
            "panel": False,
            "fq": "pre",
        }

        def _fetch(kw: Dict[str, Any]) -> pd.DataFrame:
            return jq.get_price(
                security=kw["security"],
                end_date=kw["end_date"],
                frequency=kw["frequency"],
                fields=kw["fields"],
                count=kw["count"],
                panel=kw["panel"],
                fq=kw["fq"],
            )

        df = self._cache.cached_call("get_price_factor_ref", kwargs, _fetch, result_type="df")
        return self._extract_factor_ref_map(df, securities)

    def _extract_factor_ref_map(
        self,
        df: Any,
        securities: List[str],
    ) -> Dict[str, float]:
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return {}
        ref_map: Dict[str, float] = {}
        if "code" in df.columns and "factor" in df.columns:
            for code, sub in df.groupby("code"):
                if sub.empty:
                    continue
                val = sub.iloc[-1]["factor"]
                try:
                    ref_map[str(code)] = float(val)
                except Exception:
                    continue
            return ref_map
        if isinstance(df.columns, pd.MultiIndex):
            if "factor" in df.columns.get_level_values(0):
                try:
                    block = df.xs("factor", axis=1, level=0)
                except Exception:
                    block = None
                if block is not None and not block.empty:
                    for code in block.columns:
                        try:
                            ref_map[str(code)] = float(block.iloc[-1][code])
                        except Exception:
                            continue
                    return ref_map
        if "factor" in df.columns:
            code = securities[0] if securities else ""
            try:
                ref_map[str(code)] = float(df.iloc[-1]["factor"])
            except Exception:
                pass
        return ref_map

    def _resolve_price_decimals(self, security: str) -> int:
        info = None
        try:
            info = self.get_security_info(security)
        except Exception:
            info = None
        if isinstance(info, dict):
            override_decimals = self._resolve_override_decimals(security, info)
            if override_decimals is not None:
                return override_decimals
            tick_size = info.get("tick_size")
            tick_decimals = self._decimals_from_tick_size(tick_size)
            if tick_decimals is not None:
                return tick_decimals
            for key in ("price_decimals", "tick_decimals"):
                val = info.get(key)
                if isinstance(val, (int, float)) and val >= 0:
                    return int(val)
        return 2

    @staticmethod
    def _decimals_from_tick_size(tick_size: Any) -> Optional[int]:
        try:
            tick = float(tick_size)
        except Exception:
            return None
        if tick <= 0:
            return None
        text = f"{tick:.10f}".rstrip("0").rstrip(".")
        if "." not in text:
            return 0
        return len(text.split(".", 1)[1])

    def _load_security_overrides(self) -> None:
        if self._security_overrides_loaded:
            return
        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "config",
            "security_overrides.json",
        )
        data: Dict[str, Any] = {}
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                    if isinstance(payload, dict):
                        data = payload
        except Exception as exc:
            logger.debug("读取security_overrides失败: %s", exc)
        self._security_overrides = data
        self._security_overrides_loaded = True

    @staticmethod
    def _candidate_security_keys(security: str) -> List[str]:
        if not security or "." not in security:
            return [security]
        code, suffix = security.split(".", 1)
        suffix = suffix.upper()
        if suffix in ("XSHG", "SH"):
            return [security, f"{code}.XSHG", f"{code}.SH"]
        if suffix in ("XSHE", "SZ"):
            return [security, f"{code}.XSHE", f"{code}.SZ"]
        if suffix in ("BJ", "BSE"):
            return [security, f"{code}.BJ", f"{code}.BSE"]
        return [security]

    def _resolve_override_decimals(self, security: str, info: Dict[str, Any]) -> Optional[int]:
        self._load_security_overrides()
        overrides = self._security_overrides or {}
        by_category = overrides.get("by_category") or {}
        by_prefix = overrides.get("by_prefix") or {}
        by_code = overrides.get("by_code") or {}

        category = None
        tick_decimals = None
        if isinstance(by_code, dict):
            for key in self._candidate_security_keys(security):
                entry = by_code.get(key)
                if isinstance(entry, dict):
                    if entry.get("tick_decimals") is not None:
                        tick_decimals = entry.get("tick_decimals")
                    if entry.get("category"):
                        category = entry.get("category")
                    break

        if category is None and isinstance(by_prefix, dict):
            code = security.split(".", 1)[0]
            for prefix, cat in by_prefix.items():
                if code.startswith(str(prefix)):
                    category = cat
                    break

        if category is None:
            subtype = str(info.get("subtype") or "").lower()
            primary = str(info.get("type") or "").lower()
            if subtype in ("mmf", "money_market_fund"):
                category = "money_market_fund"
            elif primary in ("fund", "etf"):
                category = "fund"
            else:
                category = "stock"

        if tick_decimals is None and isinstance(by_category, dict) and category:
            entry = by_category.get(category)
            if isinstance(entry, dict) and entry.get("tick_decimals") is not None:
                tick_decimals = entry.get("tick_decimals")

        try:
            if tick_decimals is not None:
                return int(tick_decimals)
        except Exception:
            return None
        return None

    def _round_price_result(self, data: Any, security: Union[str, List[str]]) -> Any:
        if not isinstance(data, pd.DataFrame):
            return data
        if data.empty:
            return data
        securities = [security] if isinstance(security, str) else list(security)
        if not securities:
            return data
        decimals_map = {str(code): self._resolve_price_decimals(str(code)) for code in securities}
        price_fields = {str(f) for f in self._PRICE_SCALE_FIELDS}
        result_df = data.copy()
        cols = result_df.columns
        if isinstance(cols, pd.MultiIndex):
            for field, code in cols:
                if str(field) in price_fields:
                    dec = decimals_map.get(str(code), 2)
                    try:
                        result_df[(field, code)] = result_df[(field, code)].round(dec)
                    except Exception:
                        continue
            return result_df
        if "code" in result_df.columns:
            for code, dec in decimals_map.items():
                mask = result_df["code"] == code
                if not mask.any():
                    continue
                for field in price_fields:
                    if field in result_df.columns:
                        result_df.loc[mask, field] = result_df.loc[mask, field].round(dec)
            return result_df
        dec = decimals_map.get(str(securities[0]), 2)
        for field in price_fields:
            if field in result_df.columns:
                result_df[field] = result_df[field].round(dec)
        return result_df

    @staticmethod
    def _is_engine_missing_error(exc: Exception) -> bool:
        message = str(exc)
        return 'get_price_engine' in message
