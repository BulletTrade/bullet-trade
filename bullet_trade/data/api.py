"""
数据API包装

包装多数据源的函数，避免未来函数，确保回测准确性
"""

from typing import Union, List, Optional, Dict, Any, Callable, Tuple
import importlib
from datetime import datetime, timedelta, date as Date, time as Time
import pandas as pd
import re
import os
import json


from ..core.models import SecurityUnitData
from ..core.globals import log
from ..core.exceptions import FutureDataError, UserError
from ..core.settings import get_settings
from ..utils.env_loader import get_data_provider_config


# 全局上下文，用于获取当前回测时间
_current_context = None

# 引入可插拔数据提供者，并设置默认Provider
from .providers.base import DataProvider

# 记录是否已在实盘强制关缓存并提醒过
_cache_forced_off_warned = False

# 按名称缓存 provider 实例与认证状态，避免重复初始化/认证
_provider_cache: Dict[str, DataProvider] = {}
_provider_auth_attempted: Dict[str, bool] = {}


def _normalize_provider_name(name: Optional[str]) -> str:
    """
    规范化 provider 名称（用于缓存键与回退映射）。
    """
    if not name:
        return ""
    lowered = name.lower()
    if lowered in ("jqdata", "jqdatasdk"):
        return "jqdata"
    if lowered in ("qmt", "miniqmt"):
        return "miniqmt"
    if lowered in ("qmt-remote", "remote-qmt", "remote_qmt"):
        return "remote_qmt"
    return lowered


def _create_provider(provider_name: Optional[str] = None, overrides: Optional[Dict[str, Any]] = None) -> DataProvider:
    """
    根据名称创建数据提供者实例，支持读取环境配置并按需覆盖参数。
    """
    config = get_data_provider_config()
    target = _normalize_provider_name(provider_name or config.get('default') or 'jqdata')
    overrides = overrides or {}

    if target == 'jqdata':
        from .providers.jqdata import JQDataProvider
        provider_cfg = dict(config.get('jqdata', {}) or {})
        provider_cfg.update(overrides)
        return JQDataProvider(provider_cfg)
    if target in ('tushare',):
        from .providers.tushare import TushareProvider
        provider_cfg = dict(config.get('tushare', {}) or {})
        provider_cfg.update(overrides)
        return TushareProvider(provider_cfg)
    if target in ('qmt', 'miniqmt'):
        from .providers.miniqmt import MiniQMTProvider
        provider_cfg = dict(config.get('qmt', {}) or {})
        provider_cfg.update(overrides)
        return MiniQMTProvider(provider_cfg)
    if target in ('qmt-remote', 'remote-qmt', 'remote_qmt'):
        from .providers.remote_qmt import RemoteQmtProvider
        provider_cfg = dict(config.get('remote_qmt', {}) or {})
        provider_cfg.update(overrides)
        return RemoteQmtProvider(provider_cfg)

    raise ValueError(f"未知的数据提供者: {provider_name}")


    """兼容聚宽风格的证券信息对象，既支持属性访问也保留字典语义。"""

    __slots__ = ("code",)

    def __init__(self, code: str, data: Optional[Dict[str, Any]] = None):
        super().__init__()
        object.__setattr__(self, "code", code)
        if data:
            for key, value in data.items():
                if value is not None:
                    super().__setitem__(key, value)

        # 为常用字段设置默认值，避免属性访问时抛异常
        for key in ("display_name", "name", "start_date", "end_date", "type", "subtype", "parent"):
            self.setdefault(key, None)

    def __getattr__(self, item: str) -> Any:
        # 未提供的字段返回 None，贴近聚宽 SDK 的容错行为
        return self.get(item, None)

    def __setattr__(self, key: str, value: Any) -> None:
        if key == "code":
            object.__setattr__(self, key, value)
        else:
            self[key] = value

    def __delattr__(self, item: str) -> None:
        if item == "code":
            raise AttributeError("code 字段不可删除")
        try:
            del self[item]
        except KeyError as exc:
            raise AttributeError(item) from exc

    def to_dict(self) -> Dict[str, Any]:
        return dict(self)

def _ensure_auth():
    """确保数据提供者已认证"""
    global _auth_attempted
    if not _auth_attempted:
        try:
            _provider.auth()
            _auth_attempted = True
            _provider_auth_attempted[_normalize_provider_name(getattr(_provider, "name", None))] = True
        except Exception as e:
            # 认证失败，但不设置 _auth_attempted = True
            # 这样下次调用时还会重试
            print(f"数据源认证失败: {e}")
            # 不设置 _auth_attempted，允许重试


def _ensure_auth_for(provider: DataProvider, provider_name: str, *, raise_on_fail: bool = False) -> None:
    """
    确保指定 provider 已认证，按名称跟踪认证状态。
    """
    name = _normalize_provider_name(provider_name or getattr(provider, "name", ""))
    attempted = _provider_auth_attempted.get(name, False)
    if attempted:
        return
    try:
        provider.auth()
        _provider_auth_attempted[name] = True
        if provider is _provider:
            # 同步全局默认数据源的认证标记
            globals()["_auth_attempted"] = True
    except Exception as exc:
        message = f"{name} 数据源认证失败: {exc}"
        if raise_on_fail:
            raise RuntimeError(message) from exc
        print(message)


def _sdk_fallback_targets(provider_name: str, provider: DataProvider, method_name: str) -> Callable[..., Any]:
    """
    返回一个可调用对象，用于在 provider 缺少某方法时回退到对应 SDK/客户端。
    不存在可用回退时抛出 AttributeError。
    """
    normalized = _normalize_provider_name(provider_name or getattr(provider, "name", ""))
    attempts: List[str] = []
    errors: List[str] = []

    def _lazy_import(module_path: str) -> Optional[Any]:
        try:
            module = importlib.import_module(module_path)
            attempts.append(module_path)
            return module
        except Exception as exc:
            errors.append(f"{module_path} 导入失败: {exc}")
            return None

    def _get_from(obj: Any, label: str) -> Optional[Callable[..., Any]]:
        if obj is None:
            return None
        attempts.append(label)
        return getattr(obj, method_name, None)

    if normalized == "jqdata":
        mod = _lazy_import("jqdatasdk")
        if mod:
            target = getattr(mod, method_name, None)
            if target:
                return target
    elif normalized == "tushare":
        client = None
        ensure_client = getattr(provider, "_ensure_client", None)
        if callable(ensure_client):
            try:
                client = ensure_client()
            except Exception as exc:
                errors.append(f"tushare.pro_api() 初始化失败: {exc}")
        candidate = _get_from(client, "tushare.pro_api()")
        if candidate:
            return candidate
        mod = _lazy_import("tushare")
        if mod:
            target = getattr(mod, method_name, None)
            if target:
                return target
    elif normalized in ("miniqmt",):
        mod = _lazy_import("xtquant.xtdata")
        if mod:
            target = getattr(mod, method_name, None)
            if target:
                return target
    elif normalized == "remote_qmt":
        raise AttributeError(f"{normalized} 未实现 {method_name}，且无可用的 SDK 回退路径")

    detail = "; ".join(attempts + errors) if (attempts or errors) else "无可用回退"
    raise AttributeError(f"{normalized} 未实现 {method_name}，已尝试回退到同名 provider 的 SDK/客户端: {detail}")


def _bind_sdk_fallback(provider: DataProvider, provider_name: str) -> None:
    """
    为 provider 绑定 SDK 回退解析器（仅同一 provider，不跨数据源）。
    """
    if getattr(provider, "_sdk_fallback", None):
        return

    def _resolver(method_name: str) -> Callable[..., Any]:
        return _sdk_fallback_targets(provider_name, provider, method_name)

    setattr(provider, "_sdk_fallback", _resolver)


_provider: DataProvider = _create_provider()
_bind_sdk_fallback(_provider, _normalize_provider_name(getattr(_provider, "name", None)))
_provider_cache[_normalize_provider_name(getattr(_provider, "name", None))] = _provider
_auth_attempted = False
_security_info_cache: Dict[Any, "SecurityInfo"] = {}
_security_overrides_loaded = False
_security_overrides: Dict[str, Any] = {}


class SecurityInfo(dict):
    """兼容聚宽风格的证券信息对象，既支持属性访问也保留字典语义。"""

    __slots__ = ("code",)

    def __init__(self, code: str, data: Optional[Dict[str, Any]] = None):
        super().__init__()
        object.__setattr__(self, "code", code)
        if data:
            for key, value in data.items():
                if value is not None:
                    super().__setitem__(key, value)

        # 为常用字段设置默认值，避免属性访问时抛异常
        for key in ("display_name", "name", "start_date", "end_date", "type", "subtype", "parent"):
            self.setdefault(key, None)

    def __getattr__(self, item: str) -> Any:
        # 未提供的字段返回 None，贴近聚宽 SDK 的容错行为
        return self.get(item, None)

    def __setattr__(self, key: str, value: Any) -> None:
        if key == "code":
            object.__setattr__(self, key, value)
        else:
            self[key] = value

    def __delattr__(self, item: str) -> None:
        if item == "code":
            raise AttributeError("code 字段不可删除")
        try:
            del self[item]
        except KeyError as exc:
            raise AttributeError(item) from exc

    def to_dict(self) -> Dict[str, Any]:
        return dict(self)

def set_data_provider(provider: Union[DataProvider, str], **provider_kwargs) -> None:
    """
    设置当前数据提供者。
    支持直接传入 DataProvider 实例，或传入 provider 名称（如 'jqdata'、'tushare'、'miniqmt'）。
    """
    global _provider, _auth_attempted, _security_info_cache, _cache_forced_off_warned
    if isinstance(provider, DataProvider):
        _provider = provider
    else:
        _provider = _create_provider(provider_name=provider, overrides=provider_kwargs)

    normalized = _normalize_provider_name(getattr(_provider, "name", None))
    _bind_sdk_fallback(_provider, normalized)
    _provider_cache[normalized] = _provider
    _provider_auth_attempted[normalized] = False
    _auth_attempted = False
    _security_info_cache = {}
    _cache_forced_off_warned = False
    try:
        _provider.auth()
        _auth_attempted = True
    except Exception:
        _auth_attempted = True
        pass


def reload_data_provider_from_env(provider_name: Optional[str] = None) -> None:
    """
    根据最新环境变量刷新数据提供者实例。
    """
    global _provider, _auth_attempted, _security_info_cache, _cache_forced_off_warned
    _provider = _create_provider(provider_name=provider_name, overrides=None)
    normalized = _normalize_provider_name(getattr(_provider, "name", None))
    _bind_sdk_fallback(_provider, normalized)
    _provider_cache[normalized] = _provider
    _provider_auth_attempted[normalized] = False
    _auth_attempted = False
    _security_info_cache = {}
    _cache_forced_off_warned = False

def get_data_provider(provider_name: Optional[str] = None) -> DataProvider:
    """
    获取指定数据提供者实例（若未认证则触发一次认证）。
    - 不传 provider_name：返回全局默认数据源。
    - 传入 provider_name：按名称缓存并返回对应实例，不修改全局默认。
    """
    if provider_name is None:
        _maybe_disable_cache_for_live()
        _ensure_auth()
        _bind_sdk_fallback(_provider, _normalize_provider_name(getattr(_provider, "name", None)))
        return _provider

    normalized = _normalize_provider_name(provider_name)
    provider = _provider_cache.get(normalized)
    if provider is None:
        provider = _create_provider(provider_name=normalized, overrides=None)
        _provider_cache[normalized] = provider
        _provider_auth_attempted[normalized] = False

    _bind_sdk_fallback(provider, normalized)
    _ensure_auth_for(provider, normalized, raise_on_fail=True)
    return provider


def set_current_context(context):
    """设置当前回测上下文"""
    global _current_context
    _current_context = context


def _is_live_mode() -> bool:
    try:
        return bool(_current_context and getattr(_current_context, "run_params", {}).get("is_live"))
    except Exception:
        return False


def _maybe_disable_cache_for_live() -> None:
    """
    实盘模式下强制关闭数据提供者的磁盘缓存，避免延迟/IO风险。
    """
    global _cache_forced_off_warned
    if not _is_live_mode():
        return
    cache_obj = getattr(_provider, "_cache", None)
    if cache_obj is None or not getattr(cache_obj, "enabled", False):
        return
    cache_dir = getattr(cache_obj, "cache_dir", "") or "未配置"
    cache_obj.enabled = False
    if not _cache_forced_off_warned:
        log.warning("实盘模式已强制关闭数据源缓存，原缓存目录: %s", cache_dir)
        _cache_forced_off_warned = True


def _config_base_dir() -> str:
    # bullet_trade/data -> base at bullet_trade
    return os.path.dirname(os.path.dirname(__file__))


def _security_overrides_path() -> str:
    return os.path.join(_config_base_dir(), 'config', 'security_overrides.json')


def _load_security_overrides_if_needed() -> None:
    global _security_overrides_loaded, _security_overrides
    if _security_overrides_loaded:
        return
    path = _security_overrides_path()
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    _security_overrides = data
    except Exception as exc:
        log.debug(f"读取security_overrides失败: {exc}")
        _security_overrides = {}
    finally:
        _security_overrides_loaded = True


def set_security_overrides(overrides: Dict[str, Any]) -> None:
    """以编程方式设置/覆盖标的元数据（category/tplus/slippage）。"""
    global _security_overrides_loaded, _security_overrides
    _security_overrides = overrides or {}
    _security_overrides_loaded = True
    _security_info_cache.clear()


def reset_security_overrides() -> None:
    """清除覆盖并恢复到默认配置文件内容。"""
    global _security_overrides_loaded, _security_overrides
    _security_overrides = {}
    _security_overrides_loaded = False
    _security_info_cache.clear()


def _merge_overrides(security: str, base_info: Dict[str, Any]) -> Dict[str, Any]:
    """将配置覆盖项合并到基础元信息中。支持分类默认与按代码覆盖。"""
    _load_security_overrides_if_needed()
    if not _security_overrides:
        return base_info

    by_code = _security_overrides.get('by_code') or {}
    by_category = _security_overrides.get('by_category') or {}
    by_prefix = _security_overrides.get('by_prefix') or {}

    out = dict(base_info)

    # 推断分类
    category = out.get('category')
    subtype = str(out.get('subtype') or '').lower()
    primary = str(out.get('type') or '').lower()
    if not category:
        if subtype in ('mmf', 'money_market_fund'):
            category = 'money_market_fund'
        elif primary in ('fund', 'etf'):
            # jqdatasdk 返回 type='etf'，统一归类为 fund
            category = 'fund'
        elif primary == 'stock':
            category = 'stock'
        else:
            category = 'stock'
        out['category'] = category

    # 前缀归类（仅在未显式设置 category 或仍为 stock 的情况下尝试）
    try:
        if isinstance(by_prefix, dict):
            code = security.split('.', 1)[0]
            for prefix, cat in by_prefix.items():
                if code.startswith(prefix):
                    # 仅在未显式分类或默认为stock时采用前缀分类
                    if out.get('category') in (None, '', 'stock'):
                        out['category'] = cat
                    break
    except Exception:
        pass

    # 分类默认
    category = out.get('category')
    cat_defaults = by_category.get(category, {}) if isinstance(by_category, dict) else {}
    if isinstance(cat_defaults, dict):
        for k, v in cat_defaults.items():
            out.setdefault(k, v)

    # 代码覆盖
    code_over = {}
    if isinstance(by_code, dict):
        for key in _candidate_security_keys(security):
            if key in by_code:
                code_over = by_code.get(key, {}) or {}
                break
    if isinstance(code_over, dict):
        out.update({k: v for k, v in code_over.items() if v is not None})

    return out


def _candidate_security_keys(security: str) -> List[str]:
    """生成兼容后缀的候选键，用于规则匹配。"""
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


def _resolve_limit_rule(security: str, info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """解析涨跌停比例规则（优先级：by_code > by_prefix > by_category > default）。"""
    _load_security_overrides_if_needed()
    if not isinstance(_security_overrides, dict):
        return {}

    rules = _security_overrides.get("limit_rules") or {}
    if not isinstance(rules, dict):
        return {}

    result: Dict[str, Any] = {}
    default_rule = rules.get("default")
    if isinstance(default_rule, dict):
        result.update(default_rule)

    if info is None:
        info = get_security_info(security)
    category = str((info or {}).get("category") or "").lower()
    by_category = rules.get("by_category") or {}
    if isinstance(by_category, dict) and category:
        cat_rule = by_category.get(category)
        if isinstance(cat_rule, dict):
            result.update(cat_rule)

    by_prefix = rules.get("by_prefix") or {}
    if isinstance(by_prefix, dict):
        code = security.split(".", 1)[0]
        candidates = sorted(by_prefix.items(), key=lambda item: len(str(item[0])), reverse=True)
        for prefix, rule in candidates:
            if code.startswith(str(prefix)):
                if isinstance(rule, dict):
                    result.update(rule)
                break

    by_code = rules.get("by_code") or {}
    if isinstance(by_code, dict):
        for key in _candidate_security_keys(security):
            code_rule = by_code.get(key)
            if isinstance(code_rule, dict):
                result.update(code_rule)
                break

    return result


def _resolve_limit_ratio(security: str, info: Optional[Dict[str, Any]] = None) -> Optional[float]:
    rule = _resolve_limit_rule(security, info)
    ratio = rule.get("ratio")
    try:
        ratio_value = float(ratio)
    except Exception:
        return None
    if ratio_value <= 0:
        return None
    return ratio_value


def _extract_close_series(df: Any, security: str) -> Optional[pd.Series]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None

    if "time" in df.columns and "code" in df.columns and "close" in df.columns:
        sub = df[df["code"] == security]
        if sub.empty:
            sub = df
        series = sub.set_index("time")["close"]
        return series

    if isinstance(df.columns, pd.MultiIndex):
        if ("close", security) in df.columns:
            return df[("close", security)]
        try:
            block = df.xs("close", axis=1, level=0)
        except Exception:
            block = None
        if block is not None and not block.empty:
            if security in block.columns:
                return block[security]
            return block.iloc[:, 0]

    if "close" in df.columns:
        return df["close"]

    return None


def _resolve_fq_ref_date(current_dt: Union[datetime, Date, Any], use_real_price: bool) -> Optional[Date]:
    """按聚宽语义返回前复权参考日期。"""
    if use_real_price:
        try:
            if isinstance(current_dt, datetime):
                return current_dt.date()
            if isinstance(current_dt, Date):
                return current_dt
            return pd.to_datetime(current_dt).date()
        except Exception:
            return None
    raw = _get_setting('fq_ref_date')
    if raw is None:
        return Date.today()
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, Date):
        return raw
    try:
        return pd.to_datetime(raw).date()
    except Exception:
        return Date.today()


def _fetch_pre_close(
    security: str,
    current_dt: datetime,
    use_real_price: bool,
    force_no_engine: bool,
) -> Optional[float]:
    kwargs: Dict[str, Any] = {
        "security": security,
        "end_date": current_dt,
        "frequency": "daily",
        "fields": ["close"],
        "count": 2,
        "fq": "pre",
    }
    pre_ref = _resolve_fq_ref_date(current_dt, use_real_price)
    if pre_ref is not None:
        kwargs.update(
            prefer_engine=not force_no_engine,
            pre_factor_ref_date=pre_ref,
            force_no_engine=force_no_engine,
        )

    try:
        df = _provider.get_price(**kwargs)
    except Exception:
        return None

    series = _extract_close_series(df, security)
    if series is None or series.empty:
        return None

    try:
        series = series.dropna()
    except Exception:
        pass
    if series.empty:
        return None

    idx = series.index
    if not isinstance(idx, pd.DatetimeIndex):
        try:
            series.index = pd.to_datetime(idx)
        except Exception:
            pass

    try:
        last_ts = series.index[-1]
        last_date = last_ts.date() if hasattr(last_ts, "date") else None
    except Exception:
        last_date = None

    current_date = current_dt.date() if isinstance(current_dt, datetime) else None
    if last_date is not None and current_date is not None:
        if last_date == current_date and len(series) >= 2:
            value = series.iloc[-2]
        else:
            value = series.iloc[-1]
    else:
        value = series.iloc[-1]

    try:
        return float(value)
    except Exception:
        return None


def _apply_limit_fallback(
    security: str,
    current_dt: datetime,
    last_price: float,
    high_limit: float,
    low_limit: float,
    use_real_price: bool,
    force_no_engine: bool,
) -> Tuple[float, float]:
    if high_limit > 0 and low_limit > 0:
        return high_limit, low_limit

    ratio = _resolve_limit_ratio(security)
    if ratio is None:
        return high_limit, low_limit

    pre_close = _fetch_pre_close(security, current_dt, use_real_price, force_no_engine)
    base_price = pre_close if pre_close and pre_close > 0 else (last_price if last_price > 0 else None)
    if base_price is None:
        return high_limit, low_limit

    decimals = get_tick_decimals(security)
    if high_limit <= 0:
        high_limit = round(base_price * (1 + ratio), decimals)
    if low_limit <= 0:
        low_limit = round(base_price * (1 - ratio), decimals)

    return high_limit, low_limit


class BacktestCurrentData:
    """当前行情（回测/非 tick 路径）。延迟加载，按 (security, time) 缓存。"""

    def __init__(self, context):
        self._context = context
        self._cache: Dict[Any, SecurityUnitData] = {}

    def __getitem__(self, security: str) -> SecurityUnitData:
        current_dt = self._context.current_dt
        cache_key = (security, current_dt)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            def _decide_window(ts: Union[datetime, Date]) -> Dict[str, Any]:
                ct: Optional[Time] = None
                cd: Optional[Date] = None
                if isinstance(ts, datetime):
                    ct = ts.time()
                    cd = ts.date()
                elif isinstance(ts, Date):
                    cd = ts
                m_start = Time(9, 31)
                m_end = Time(15, 0)
                pre_open = Time(9, 25)
                use_min = bool(ct is not None and m_start <= ct < m_end)
                use_open = bool(ct is not None and pre_open <= ct < m_start)
                return {"use_minute": use_min, "use_open_window": use_open, "current_date": cd}

            win = _decide_window(current_dt)
            use_minute = win["use_minute"]
            use_open_price_window = win["use_open_window"]
            current_date = win["current_date"]

            use_real_price = _get_setting('use_real_price')
            force_no_engine = _get_setting('force_no_engine')
            fields = ['open', 'close', 'high_limit', 'low_limit', 'paused']

            def _build_fetch_kwargs(freq_text: str) -> Dict[str, Any]:
                kw = dict(
                    security=security,
                    end_date=current_dt,
                    frequency=freq_text,
                    fields=fields,
                    count=1,
                    fq='pre',
                )
                pre_ref = _resolve_fq_ref_date(current_date or current_dt, use_real_price)
                if pre_ref is not None:
                    kw.update(
                        prefer_engine=not force_no_engine,
                        pre_factor_ref_date=pre_ref,
                        force_no_engine=force_no_engine,
                    )
                return kw

            if use_minute:
                df = _provider.get_price(**_build_fetch_kwargs('minute'))
            else:
                df = _provider.get_price(**_build_fetch_kwargs('daily'))

            if not df.empty:
                if 'time' in df.columns and 'code' in df.columns:
                    row = df.iloc[-1]
                    close_price = float(row['close']) if pd.notna(row['close']) else 0.0
                    high_limit = float(row.get('high_limit', 0.0)) if pd.notna(row.get('high_limit', 0.0)) else 0.0
                    low_limit = float(row.get('low_limit', 0.0)) if pd.notna(row.get('low_limit', 0.0)) else 0.0
                    paused = bool(row.get('paused', False))
                else:
                    row = df.iloc[-1]
                    close_price = float(row['close']) if pd.notna(row['close']) else 0.0
                    if 'high_limit' in row:
                        high_limit = float(row.get('high_limit', 0.0)) if pd.notna(row.get('high_limit', 0.0)) else 0.0
                        low_limit = float(row.get('low_limit', 0.0)) if pd.notna(row.get('low_limit', 0.0)) else 0.0
                        paused = bool(row.get('paused', False))
                    else:
                        high_limit = 0.0
                        low_limit = 0.0
                        paused = False

                open_price = float(row['open']) if 'open' in row and pd.notna(row['open']) else None
                row_timestamp: Optional[datetime] = None
                if 'time' in row and pd.notna(row['time']):
                    try:
                        row_timestamp = pd.to_datetime(row['time'])
                    except Exception:
                        row_timestamp = None
                elif isinstance(df.index, pd.DatetimeIndex):
                    row_timestamp = df.index[-1].to_pydatetime()
                elif hasattr(df.index[-1], 'to_timestamp'):
                    row_timestamp = df.index[-1].to_timestamp()

                row_date = row_timestamp.date() if row_timestamp else None
                should_use_open = (
                    open_price is not None and use_open_price_window and current_date is not None and row_date == current_date
                )
                last_price = open_price if should_use_open else close_price

                high_limit, low_limit = _apply_limit_fallback(
                    security,
                    current_dt,
                    last_price,
                    high_limit,
                    low_limit,
                    use_real_price,
                    force_no_engine,
                )

                data = SecurityUnitData(
                    security=security,
                    last_price=last_price,
                    high_limit=high_limit,
                    low_limit=low_limit,
                    paused=paused,
                )
            else:
                data = SecurityUnitData(security=security, last_price=0.0)
                log.debug(f"{security}无数据")

        except Exception as e:
            log.debug(f"获取{security}数据失败: {e}")
            data = SecurityUnitData(security=security, last_price=0.0)

        self._cache[cache_key] = data
        return data

    def __contains__(self, security: str) -> bool:
        try:
            data = self.__getitem__(security)
            return data.last_price > 0
        except Exception:
            return False

    def keys(self):
        return self._cache.keys()

    def items(self):
        return self._cache.items()

    def values(self):
        return self._cache.values()


class LiveCurrentData:
    """当前行情（实盘/tick 优先）。若 tick 不可用则回退到 BacktestCurrentData 逻辑。"""

    def __init__(self, context):
        self._context = context
        self._cache: Dict[Any, SecurityUnitData] = {}
        self._fallback = BacktestCurrentData(context)

    @staticmethod
    def _to_qmt_code(code: str) -> str:
        if code.endswith('.XSHE'):
            return code.replace('.XSHE', '.SZ')
        if code.endswith('.XSHG'):
            return code.replace('.XSHG', '.SH')
        return code

    def __getitem__(self, security: str) -> SecurityUnitData:
        current_dt = self._context.current_dt
        cache_key = (security, current_dt)
        if cache_key in self._cache:
            return self._cache[cache_key]

        requires_live = bool(getattr(_provider, 'requires_live_data', False))
        snap = None
        try:
            get_fn = getattr(_provider, 'get_live_current', None)
            if callable(get_fn):
                snap = get_fn(security)
        except Exception:
            if requires_live:
                raise

        if isinstance(snap, dict) and snap.get('last_price') is not None:
            last_price = float(snap.get('last_price') or 0.0)
            high_limit = float(snap.get('high_limit') or 0.0)
            low_limit = float(snap.get('low_limit') or 0.0)
            paused = bool(snap.get('paused') or False)
            use_real_price = _get_setting('use_real_price')
            force_no_engine = _get_setting('force_no_engine')
            high_limit, low_limit = _apply_limit_fallback(
                security,
                current_dt,
                last_price,
                high_limit,
                low_limit,
                use_real_price,
                force_no_engine,
            )
            data = SecurityUnitData(
                security=security,
                last_price=last_price,
                high_limit=high_limit,
                low_limit=low_limit,
                paused=paused,
            )
        else:
            if requires_live:
                provider_name = getattr(_provider, 'name', 'unknown')
                raise RuntimeError(
                    f"数据源 {provider_name} 未返回 {security} 的实时行情，请检查行情订阅/连接"
                )
            data = self._fallback[security]

        self._cache[cache_key] = data
        return data

    def __contains__(self, security: str) -> bool:
        try:
            data = self.__getitem__(security)
            return data.last_price > 0
        except Exception:
            return False

    def keys(self):
        return self._cache.keys()

    def items(self):
        return self._cache.items()

    def values(self):
        return self._cache.values()


def _get_setting(key: str, default: Any = False) -> Any:
    """统一获取设置项"""
    return get_settings().options.get(key, default)


def _should_avoid_future() -> bool:
    return bool(_current_context and _get_setting('avoid_future_data'))


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value in (None, "", "NaT"):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, Date) and not isinstance(value, datetime):
        return datetime.combine(value, Time(0, 0))
    try:
        parsed = pd.to_datetime(value)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    if isinstance(parsed, pd.Timestamp):
        return parsed.to_pydatetime()
    return None


def _resolve_context_dt(value: Any, default_to_context: bool = True) -> Optional[datetime]:
    dt = _coerce_datetime(value)
    if dt is None and default_to_context and _current_context:
        return _current_context.current_dt
    return dt


def _resolve_context_date(value: Any, default_to_context: bool = True) -> Optional[Date]:
    if value is None and default_to_context and _current_context:
        return _current_context.current_dt.date()
    return _coerce_date(value) if value is not None else None


def _ensure_not_future_dt(value: Optional[datetime], label: str) -> Optional[datetime]:
    if not _should_avoid_future() or value is None:
        return value
    current_dt = _current_context.current_dt
    if value > current_dt:
        raise FutureDataError(
            f"avoid_future_data=True时，{label}({value})不能大于当前时间({current_dt})"
        )
    return value


def _ensure_not_future_date(value: Optional[Date], label: str) -> Optional[Date]:
    if not _should_avoid_future() or value is None:
        return value
    current_date = _current_context.current_dt.date()
    if value > current_date:
        raise FutureDataError(
            f"avoid_future_data=True时，{label}({value})不能大于当前日期({current_date})"
        )
    return value


_HISTORY_VIEW_UNSUPPORTED: Dict[str, set] = {
    "miniqmt": {"get_all_securities"},
    "qmt-remote": {"get_all_securities"},
}


def _ensure_history_view(api_name: str, date_value: Optional[Date]) -> None:
    if not _current_context or _is_live_mode():
        return
    if date_value is None:
        return
    provider_name = getattr(_provider, "name", "")
    unsupported = _HISTORY_VIEW_UNSUPPORTED.get(provider_name, set())
    if api_name in unsupported:
        raise UserError(f"{provider_name} 数据源不支持 {api_name} 的历史视角，回测中不可用")


def _raise_if_not_implemented(error: Exception) -> None:
    if isinstance(error, NotImplementedError):
        raise


def _query_mentions_valuation(query_object: Any) -> bool:
    """
    粗略判断查询是否涉及估值表，用于回测时间防护。
    """
    if query_object is None:
        return False
    try:
        text = str(query_object).lower()
    except Exception:
        return False
    return 'valuation' in text or 'stock_valuation' in text


def _is_permission_error(error: Exception) -> bool:
    message = str(error)
    if not message:
        return False
    lowered = message.lower()
    return 'method not allowed' in lowered or 'permission' in lowered or '权限' in message


def _raise_permission_error(error: Exception, api_name: str) -> None:
    if _is_permission_error(error):
        raise UserError(f"{api_name} 权限不足: {error}") from error


def _is_sql_unsupported(error: Exception) -> bool:
    message = str(error)
    lowered = message.lower()
    if 'sql' not in lowered and 'sql' not in message:
        return False
    if 'unexpected keyword argument' in lowered:
        return True
    if 'unsupported' in lowered or 'not allowed' in lowered:
        return True
    if '不接受' in message or '不支持' in message or '只支持' in message:
        return True
    return False

def _coerce_date(value: Any) -> Optional[Date]:
    if value in (None, "", "NaT"):
        return None
    if isinstance(value, Date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        parsed = pd.to_datetime(value)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.date()


def _normalize_security_info(security: str, raw_info: Any) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    if isinstance(raw_info, dict):
        normalized.update({k: v for k, v in raw_info.items() if v is not None})
    else:
        for attr in (
            'type',
            'subtype',
            'display_name',
            'name',
            'start_date',
            'end_date',
            'parent',
        ):
            if hasattr(raw_info, attr):
                value = getattr(raw_info, attr)
                if value is not None:
                    normalized[attr] = value

    normalized.setdefault('code', security)

    for field in ('start_date', 'end_date'):
        normalized[field] = _coerce_date(normalized.get(field))

    if normalized.get('end_date') is None:
        normalized['end_date'] = Date(2200, 1, 1)

    return normalized


def get_security_info(security: str, date: Optional[Union[str, datetime]] = None) -> SecurityInfo:
    """
    获取标的的基础信息（如类型、子类型），结果会缓存。
    若当前数据源不支持，返回空对象。
    """
    if not security:
        return SecurityInfo('', {})

    resolved_date = _resolve_context_date(date, default_to_context=True)
    resolved_date = _ensure_not_future_date(resolved_date, "get_security_info.date")
    cache_key = (security, resolved_date)

    cached = _security_info_cache.get(cache_key)
    if cached is not None:
        return cached

    _ensure_auth()

    info_fn = getattr(_provider, 'get_security_info', None)
    if not callable(info_fn):
        empty = SecurityInfo(security, {})
        _security_info_cache[cache_key] = empty
        return empty

    try:
        try:
            raw_info = info_fn(security, date=resolved_date)
        except TypeError:
            raw_info = info_fn(security)
    except Exception as exc:
        log.debug(f"获取{security}基本信息失败: {exc}")
        raw_info = {}

    normalized = _normalize_security_info(security, raw_info)
    # 应用配置覆盖（分类/tplus/slippage等）
    normalized = _merge_overrides(security, normalized)
    info_obj = SecurityInfo(security, normalized)
    _security_info_cache[cache_key] = info_obj
    return info_obj


def _coerce_price_result_to_dataframe(result: Any) -> pd.DataFrame:
    """将 provider.get_price 的返回结果统一为 DataFrame，容忍 Panel/长表/宽表/字典等形式。
    不负责最终的字段 MultiIndex 兼容变换，仅做基础整形。
    """
    # Panel-like
    if hasattr(result, 'to_frame'):
        try:
            return result.to_frame()
        except Exception as e:
            log.debug(f"to_frame 失败: {e}")
            try:
                return pd.DataFrame(result)
            except Exception:
                return pd.DataFrame()
    # DataFrame
    if isinstance(result, pd.DataFrame):
        # 长表 (time, code, value...)
        if 'time' in result.columns and 'code' in result.columns:
            try:
                value_columns = [c for c in result.columns if c not in ['time', 'code']]
                if len(value_columns) == 1:
                    pivot_df = result.pivot(index='time', columns='code', values=value_columns[0])
                    pivot_df.columns = pd.MultiIndex.from_product(
                        [value_columns, pivot_df.columns.tolist()], names=['field', 'code']
                    )
                    if not isinstance(pivot_df.index, pd.DatetimeIndex):
                        pivot_df.index = pd.to_datetime(pivot_df.index)
                    return pivot_df
                # 多字段长表
                parts = [result.pivot(index='time', columns='code', values=col) for col in value_columns]
                df_wide = pd.concat(parts, axis=1, keys=value_columns)
                df_wide.columns.names = ['field', 'code']
                if not isinstance(df_wide.index, pd.DatetimeIndex):
                    df_wide.index = pd.to_datetime(df_wide.index)
                return df_wide
            except Exception as e:
                log.debug(f"长表转换失败: {e}")
                return result
        # 宽表直接返回
        return result
    # 字典或其他
    try:
        df = pd.DataFrame(result)
        if 'time' in df.columns:
            try:
                df['time'] = pd.to_datetime(df['time'])
                df.set_index('time', inplace=True)
            except Exception:
                pass
        return df
    except Exception as e:
        log.debug(f"无法将结果转换为 DataFrame: {e}")
        return pd.DataFrame()


def get_price(
    security: Union[str, List[str]],
    start_date: Optional[Union[str, datetime]] = None,
    end_date: Optional[Union[str, datetime]] = None,
    frequency: str = 'daily',
    fields: Optional[List[str]] = None,
    skip_paused: bool = False,
    fq: str = 'pre',
    count: Optional[int] = None,
    panel: bool = True,
    fill_paused: bool = True
) -> pd.DataFrame:
    """
    获取历史数据（避免未来函数，支持真实价格）
    
    Args:
        security: 标的代码或代码列表
        start_date: 开始日期
        end_date: 结束日期（会自动限制在当前回测时间之前）
        frequency: 频率 ('daily', '1d', 'minute', '1m')
        fields: 字段列表 ['open', 'close', 'high', 'low', 'volume', 'money']
        skip_paused: 是否跳过停牌
        fq: 复权方式 ('pre'-前复权, 'post'-后复权, None-不复权)
        count: 获取数量
        panel: 是否返回panel格式
        fill_paused: 是否填充停牌数据
        
    Returns:
        DataFrame
        
    Raises:
        FutureDataError: 当 avoid_future_data=True 时访问未来数据
    """
    # 确保数据提供者已认证
    _ensure_auth()
    
    # 警告：panel=True 已废弃，与聚宽官方保持一致
    # 聚宽官方提示：不建议继续使用panel（panel将在pandas未来版本不再支持，将来升级pandas后，您的策略会失败）
    if panel and isinstance(security, (list, tuple, set)):
        import warnings
        warnings.warn(
            "不建议继续使用panel（panel将在pandas未来版本不再支持，将来升级pandas后，您的策略会失败），"
            "建议 get_price 传入 panel=False 参数",
            UserWarning
        )
    
    force_no_engine = _get_setting('force_no_engine')

    if not _current_context:
        # 没有回测上下文，直接调用原始API
        return _provider.get_price(
            security=security,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            fields=fields,
            skip_paused=skip_paused,
            fq=fq,
            count=count,
            panel=panel,
            fill_paused=fill_paused,
            force_no_engine=force_no_engine,
        )
    
    avoid_future = _get_setting('avoid_future_data')
    use_real_price = _get_setting('use_real_price')
    
    current_dt = _current_context.current_dt
    
    # 检查参数冲突：start_date 和 count 不能同时使用（与聚宽保持一致）
    if count is not None and start_date is not None:
        raise UserError("get_price 不能同时指定 start_date 和 count 两个参数")
    
    # 处理 end_date 的默认值（与聚宽保持一致）
    # 聚宽官方：如果没有提供 end_date，默认是 datetime.datetime(2015, 12, 31)
    # 但如果提供了 count，end_date 应该默认为 current_dt（从当前时间往前推）
    if end_date is None:
        if count is not None:
            # 使用 count 时，end_date 默认为当前时间（从当前时间往前推 count 条数据）
            end_date = current_dt
        else:
            # 没有 count 时，使用聚宽的默认值（过去的日期）
            end_date = datetime(2015, 12, 31)
    elif isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    elif isinstance(end_date, Date) and not isinstance(end_date, datetime):
        end_date = datetime.combine(end_date, Time(15, 0))
    
    # 标准化频率
    frequency_map = {'daily': '1d', 'minute': '1m'}
    freq = frequency_map.get(frequency, frequency)
    
    # 避免未来数据检查
    if avoid_future:
        if fields is None:
            fields = ['open', 'close', 'high', 'low', 'volume', 'money']
        
        # 检查 end_date 是否超过当前时间
        if 'm' in freq:
            # 分钟数据
            if end_date > current_dt:
                raise FutureDataError(
                    f"avoid_future_data=True时，get_price的end_date({end_date})"
                    f"不能大于当前时间({current_dt})"
                )
        elif 'd' in freq:
            # 日数据
            if end_date.date() > current_dt.date():
                raise FutureDataError(
                    f"avoid_future_data=True时，get_price的end_date({end_date.date()})"
                    f"不能大于当前日期({current_dt.date()})"
                )
            elif end_date.date() == current_dt.date():
                # 同一天，需要检查时间和字段
                _check_intraday_future_data(current_dt, fields, end_date)
    
    # 限制 end_date 不超过当前时间
    end_date = min(end_date, current_dt)
    
    # 真实价格模式：使用当前回测时间作为复权参考日期
    # 注意：当 panel=False 时跳过真实价格模式，因为 get_price_engine 不支持 panel 参数
    # 此时直接使用标准的 get_price，它正确支持 panel=False 返回长表格式
    if use_real_price and fq == 'pre' and panel:
        # 使用当前回测日期作为复权参考日期，以获得当时的真实价格
        
        # 确保 pre_factor_ref_date 是 datetime.date 类型
        if isinstance(current_dt, Date) and not isinstance(current_dt, datetime):
            pre_factor_ref_date = current_dt
        elif isinstance(current_dt, datetime):
            pre_factor_ref_date = current_dt.date()
        else:
            try:
                pre_factor_ref_date = pd.to_datetime(current_dt).date()
            except:
                log.warning(f"无法转换 current_dt 为 date 类型: {current_dt}, 使用今天日期")
                pre_factor_ref_date = Date.today()
        
        try:
            # 真实价格模式优先使用提供者内部引擎支持
            #log.debug(f"调用 provider.get_price(prefer_engine=True): security={security}, fields={fields}")
            result = _provider.get_price(
                security=security,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                fields=fields,
                skip_paused=skip_paused,
                fq=fq,
                count=count,
                panel=panel,
                fill_paused=fill_paused,
                prefer_engine=not force_no_engine,
                pre_factor_ref_date=pre_factor_ref_date,
                force_no_engine=force_no_engine,
            )
            #log.debug(f"provider.get_price 返回: {type(result)}, {result.shape if hasattr(result, 'shape') else 'No shape'}")
            # 如果 panel=False 且返回的是长表格式（包含 time 和 code 列），直接返回，不进行转换
            # 这样可以保持与聚宽官方一致的行为，支持 df.groupby('code') 等操作
            # 注意：必须在 _coerce_price_result_to_dataframe 之前检查，否则会被转换成宽表
            if not panel and isinstance(result, pd.DataFrame) and 'time' in result.columns and 'code' in result.columns:
                return result
            df = _coerce_price_result_to_dataframe(result)
            return _make_compatible_dataframe(df, fields)
            
        except Exception as e:
            _raise_if_not_implemented(e)
            log.warning(f"真实价格模式调用失败: {e}，回退到标准复权")
    
    # 标准模式或真实价格模式失败时的回退策略
    try:
        df = _provider.get_price(
            security=security,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            fields=fields,
            skip_paused=skip_paused,
            fq=fq,
            count=count,
            panel=panel,
            fill_paused=fill_paused,
            force_no_engine=force_no_engine,
        )
        
        # 调试日志：查看返回的数据格式
        log.debug(f"get_price 返回: panel={panel}, type={type(df)}, "
                  f"columns={list(df.columns) if isinstance(df, pd.DataFrame) else 'N/A'}, "
                  f"shape={df.shape if hasattr(df, 'shape') else 'N/A'}")
        
        # 如果 panel=False 且返回的是长表格式（包含 time 和 code 列），直接返回，不进行转换
        # 这样可以保持与聚宽官方一致的行为，支持 df.groupby('code') 等操作
        if not panel and isinstance(df, pd.DataFrame) and 'time' in df.columns and 'code' in df.columns:
            return df
        
        # 兼容性处理：让多证券情况下也能通过 df['close'] 访问
        return _make_compatible_dataframe(df, fields)
        
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取价格数据失败: {e}")
        return pd.DataFrame()



def _make_compatible_dataframe(df: pd.DataFrame, fields: Optional[List[str]]) -> pd.DataFrame:
    """
    让 DataFrame 兼容策略代码的访问方式
    
    目标：让多证券情况下也能通过 df['close'] 访问到数据
    
    Args:
        df: 原始 DataFrame
        fields: 请求的字段列表
        
    Returns:
        兼容的 DataFrame（列为 MultiIndex(field, code)），保证 df['close'] 返回二维矩阵
    """
    if df.empty:
        return df

    known_fields = {
        'open', 'close', 'high', 'low', 'volume', 'money',
        'avg', 'price', 'high_limit', 'low_limit', 'paused'
    }

    # 情况1：索引是 (code, time) 的 MultiIndex（panel=False 常见返回）
    if isinstance(df.index, pd.MultiIndex) and set(df.index.names or []) >= {'code', 'time'}:
        try:
            df_reset = df.reset_index()
            # 确定值字段列表
            if fields and len(fields) > 0:
                value_columns = [col for col in fields if col in df_reset.columns]
            else:
                value_columns = [c for c in df_reset.columns if c not in ['code', 'time']]
            if not value_columns:
                return df

            wide_list = []
            for col in value_columns:
                wide_col = df_reset.pivot(index='time', columns='code', values=col)
                wide_list.append(wide_col)

            if len(wide_list) == 1:
                wide = wide_list[0]
                wide.columns = pd.MultiIndex.from_product(
                    [value_columns, wide.columns.tolist()], names=['field', 'code']
                )
            else:
                wide = pd.concat(wide_list, axis=1, keys=value_columns)
                wide.columns.names = ['field', 'code']

            if not isinstance(wide.index, pd.DatetimeIndex):
                wide.index = pd.to_datetime(wide.index)
            return wide
        except Exception:
            return df

    # 情况2：列是 MultiIndex（但层级顺序可能是 (code, field)）
    if isinstance(df.columns, pd.MultiIndex):
        try:
            level0 = set(map(str, df.columns.get_level_values(0)))
            level1 = set(map(str, df.columns.get_level_values(1)))
            # 期望：field 在外层。如果 field 出现在第2层而非第1层，则交换层级。
            if (level1 & known_fields) and not (level0 & known_fields):
                df = df.swaplevel(0, 1, axis=1)
            df.columns.names = ['field', 'code']
            return df
        except Exception:
            return df

    # 情况3：普通列，但为多证券+单字段（列是证券代码集合）
    # 注意：需要排除列名是已知字段名的情况，避免把字段名误当成证券代码
    cols_set = set(map(str, df.columns))
    if fields and len(fields) == 1 and df.shape[1] > 1 and not (cols_set & known_fields):
        try:
            df.columns = pd.MultiIndex.from_product(
                [fields, list(map(str, df.columns))], names=['field', 'code']
            )
            return df
        except Exception:
            return df

    # 其他情况（单证券或已是期望结构）直接返回
    return df


def _check_intraday_future_data(current_dt: datetime, fields: List[str], end_date: datetime):
    """
    检查盘中是否访问未来数据
    
    Args:
        current_dt: 当前回测时间
        fields: 请求的字段
        end_date: 结束日期
        
    Raises:
        FutureDataError: 如果访问了未来数据
    """
    # 交易时间段
    market_open = Time(9, 30)
    market_close = Time(15, 0)
    
    current_time = current_dt.time()
    
    # 盘前（开盘前）
    if current_time < market_open:
        future_fields = set(fields) & {
            'open', 'close', 'high', 'low', 'volume', 'money', 'avg', 'price'
        }
        if future_fields:
            raise FutureDataError(
                f"avoid_future_data=True时，当天开盘前不能获取当日的{future_fields}字段数据，"
                f"current_dt={current_dt}, end_date={end_date}"
            )
    
    # 盘中（开盘后、收盘前）
    elif market_open <= current_time < market_close:
        future_fields = set(fields) & {
            'close', 'high', 'low', 'volume', 'money', 'avg', 'price'
        }
        if future_fields:
            raise FutureDataError(
                f"avoid_future_data=True时，盘中不能取当日的{future_fields}字段数据，"
                f"current_dt={current_dt}, end_date={end_date}"
            )
    
    # 盘后（收盘后）- 可以获取所有数据
    else:
        pass


def attribute_history(
    security: str,
    count: int,
    unit: str = '1d',
    fields: Optional[List[str]] = None,
    skip_paused: bool = False,
    df: bool = True,
    fq: str = 'pre'
) -> Union[pd.DataFrame, Dict]:
    """
    获取单个标的历史数据（避免未来函数，支持真实价格）。

    Args:
        security: 标的代码
        count: 获取数量
        unit: 时间单位 ('1d', '1m')
        fields: 字段列表
        skip_paused: 是否跳过停牌
        df: 是否返回DataFrame
        fq: 复权方式

    Returns:
        DataFrame或Dict
    """
    if not _current_context:
        end_date = datetime.now()
    else:
        end_date = _current_context.current_dt
        if 'm' in unit:
            end_date = end_date + timedelta(minutes=1)

    frequency = 'daily' if 'd' in unit else 'minute'

    try:
        return get_price(
            security=security,
            end_date=end_date,
            frequency=frequency,
            fields=fields,
            skip_paused=skip_paused,
            fq=fq,
            count=count
        )
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取历史数据失败: {e}")
        return pd.DataFrame() if df else {}


def history(
    count: int,
    unit: str = '1d',
    field: Union[str, List[str]] = 'avg',
    security_list: Optional[Union[str, List[str]]] = None,
    df: bool = True,
    skip_paused: bool = False,
    fq: str = 'pre',
) -> Any:
    """
    获取多个标的历史数据（聚宽风格）。
    """
    if security_list is None:
        universe = _get_setting('universe', None)
        if universe:
            security_list = list(universe)
        else:
            raise UserError("history 需要提供 security_list 或先设置 universe")

    fields = [field] if isinstance(field, str) else list(field)

    end_dt = _resolve_context_dt(None, default_to_context=True)
    if end_dt is None:
        end_dt = datetime.now()
    if 'm' in unit:
        end_dt = end_dt + timedelta(minutes=1)
    elif 'd' in unit:
        end_dt = end_dt - timedelta(days=1)

    frequency = 'daily' if 'd' in unit else 'minute'
    result = get_price(
        security=security_list,
        end_date=end_dt,
        frequency=frequency,
        fields=fields,
        skip_paused=skip_paused,
        fq=fq,
        count=count,
        panel=df,
    )
    if df or not isinstance(result, pd.DataFrame):
        return result
    return result.to_dict()


def get_bars(
    security: Union[str, List[str]],
    count: int,
    unit: str = '1d',
    fields: Union[List[str], tuple] = ('date', 'open', 'high', 'low', 'close'),
    include_now: bool = False,
    end_dt: Optional[Union[str, datetime]] = None,
    fq_ref_date: Union[int, datetime, Date, None] = 1,
    df: bool = False,
) -> Any:
    """
    获取 K 线数据（聚宽风格）。
    """
    _ensure_auth()
    resolved_end = _resolve_context_dt(end_dt, default_to_context=True)
    if resolved_end is None:
        resolved_end = datetime.now()
    resolved_end = _ensure_not_future_dt(resolved_end, "get_bars.end_dt")

    if fq_ref_date == 1:
        fq_ref_date = _current_context.current_dt.date() if _current_context else Date.today()
    elif isinstance(fq_ref_date, datetime):
        fq_ref_date = fq_ref_date.date()
    elif fq_ref_date is not None and not isinstance(fq_ref_date, Date):
        raise UserError("fq_ref_date 必须为 None 或 datetime.date 类型")

    try:
        return _provider.get_bars(
            security=security,
            count=count,
            unit=unit,
            fields=list(fields) if isinstance(fields, tuple) else fields,
            include_now=include_now,
            end_dt=resolved_end,
            fq_ref_date=fq_ref_date,
            df=df,
        )
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取 K 线数据失败: {e}")
        try:
            freq = 'daily' if 'd' in unit else 'minute'
            fallback_fields = None
            if fields is not None:
                fallback_fields = [f for f in fields if f != 'date']
            fallback = get_price(
                security=security,
                end_date=resolved_end,
                frequency=freq,
                fields=fallback_fields,
                count=count,
                panel=True,
            )
            if df or not isinstance(fallback, pd.DataFrame):
                return fallback
            return fallback.to_records(index=False)
        except Exception:
            return pd.DataFrame() if df else []


def get_ticks(
    security: str,
    end_dt: Optional[Union[str, datetime]] = None,
    start_dt: Optional[Union[str, datetime]] = None,
    count: Optional[int] = None,
    fields: Optional[List[str]] = None,
    skip: bool = True,
    df: bool = False,
) -> Any:
    """
    获取 tick 数据（聚宽风格）。
    """
    _ensure_auth()
    resolved_end = _resolve_context_dt(end_dt, default_to_context=True)
    if resolved_end is None:
        raise UserError("get_ticks 需要提供 end_dt 或在回测上下文中调用")
    resolved_end = _ensure_not_future_dt(resolved_end, "get_ticks.end_dt")
    resolved_start = _resolve_context_dt(start_dt, default_to_context=False)
    if resolved_start is None and count is None:
        count = 1

    try:
        return _provider.get_ticks(
            security=security,
            end_dt=resolved_end,
            start_dt=resolved_start,
            count=count,
            fields=fields,
            skip=skip,
            df=df,
        )
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取 tick 数据失败: {e}")
        return pd.DataFrame() if df else []


def get_current_tick(
    security: str,
    dt: Optional[Union[str, datetime]] = None,
    df: bool = False,
) -> Any:
    """
    获取最新 tick 快照（聚宽风格）。
    """
    _ensure_auth()
    target_dt = _resolve_context_dt(dt, default_to_context=True)
    if target_dt is None:
        target_dt = datetime.now()
    target_dt = _ensure_not_future_dt(target_dt, "get_current_tick.dt")

    use_price_proxy = bool(
        _current_context
        and not _is_live_mode()
        and getattr(_provider, 'name', '') == 'jqdatasdk'
    )

    if use_price_proxy:
        try:
            price_df = get_price(
                security=security,
                end_date=target_dt,
                frequency='minute',
                fields=['close'],
                count=1,
                panel=False,
            )
            close_value = None
            tick_time = target_dt
            series = None
            if isinstance(price_df, pd.DataFrame) and not price_df.empty:
                if isinstance(price_df.columns, pd.MultiIndex):
                    if ('close', security) in price_df.columns:
                        series = price_df[('close', security)]
                    elif (security, 'close') in price_df.columns:
                        series = price_df[(security, 'close')]
                    else:
                        for col in price_df.columns:
                            if isinstance(col, tuple) and 'close' in col:
                                series = price_df[col]
                                break
                elif 'close' in price_df.columns:
                    series = price_df['close']
            if series is not None and len(series) > 0:
                close_value = series.iloc[-1]
                if isinstance(series.index, pd.DatetimeIndex) and len(series.index) > 0:
                    tick_time = series.index[-1]

            if close_value is None or (isinstance(close_value, float) and pd.isna(close_value)):
                return pd.DataFrame() if df else None

            tick = {
                'security': security,
                'datetime': tick_time,
                'current': float(close_value),
                'last_price': float(close_value),
            }
            return pd.DataFrame([tick]) if df else tick
        except Exception as e:
            log.error(f"回测 get_current_tick 使用分钟线失败: {e}")
            return pd.DataFrame() if df else None

    try:
        result = _provider.get_current_tick(security, dt=target_dt, df=df)
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取 tick 快照失败: {e}")
        result = None

    if _is_live_mode() and getattr(_provider, 'requires_live_data', False) and not result:
        provider_name = getattr(_provider, 'name', 'unknown')
        raise RuntimeError(f"数据源 {provider_name} 未返回实时 tick，请检查行情订阅/连接")
    return result


def get_extras(
    info: str,
    security_list: List[str],
    start_date: Optional[Union[str, datetime]] = None,
    end_date: Optional[Union[str, datetime]] = None,
    df: bool = True,
    count: Optional[int] = None,
) -> Any:
    """
    获取扩展数据（聚宽风格）。
    """
    _ensure_auth()
    resolved_start = _resolve_context_date(start_date, default_to_context=False)
    resolved_end = _resolve_context_date(end_date, default_to_context=True)
    resolved_end = _ensure_not_future_date(resolved_end, "get_extras.end_date")

    if _should_avoid_future() and resolved_end and _current_context:
        current_date = _current_context.current_dt.date()
        if resolved_end == current_date and info != 'is_st':
            if _current_context.current_dt.time() < Time(15, 0):
                raise FutureDataError(
                    f"avoid_future_data=True时，回测中get_extras只能在收盘后才能取{info}数据，"
                    f"current_dt={_current_context.current_dt}"
                )

    if resolved_start is None and count is None:
        resolved_start = Date(2015, 1, 1)

    if isinstance(security_list, str):
        security_list = [security_list]
    try:
        return _provider.get_extras(
            info,
            security_list,
            start_date=resolved_start,
            end_date=resolved_end,
            df=df,
            count=count,
        )
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取扩展数据失败: {e}")
        return pd.DataFrame() if df else {}


def get_fundamentals(
    query_object: Any,
    date: Optional[Union[str, datetime]] = None,
    statDate: Optional[str] = None,
) -> Any:
    """
    获取财务数据（聚宽风格）。
    """
    _ensure_auth()
    if date is not None and statDate is not None:
        raise UserError("get_fundamentals 只允许传入 date 或 statDate 中的一个")

    if _should_avoid_future() and statDate is not None:
        raise FutureDataError("avoid_future_data=True时，get_fundamentals 不支持 statDate 参数")

    if date is None and statDate is None:
        if _current_context:
            resolved_date = _current_context.current_dt.date() - timedelta(days=1)
        else:
            resolved_date = Date.today() - timedelta(days=1)
    else:
        resolved_date = _resolve_context_date(date, default_to_context=False)

    if resolved_date is not None:
        yesterday = Date.today() - timedelta(days=1)
        if resolved_date > yesterday:
            resolved_date = yesterday

    resolved_date = _ensure_not_future_date(resolved_date, "get_fundamentals.date")
    if _should_avoid_future() and resolved_date and _current_context:
        if resolved_date == _current_context.current_dt.date() and _query_mentions_valuation(query_object):
            if _current_context.current_dt.time() < Time(15, 0):
                raise FutureDataError(
                    "avoid_future_data=True时，回测中get_fundamentals取估值表数据需在15:00之后"
                )
    try:
        return _provider.get_fundamentals(query_object, date=resolved_date, statDate=statDate)
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取财务数据失败: {e}")
        return pd.DataFrame()


def get_fundamentals_continuously(
    query_object: Any,
    end_date: Optional[Union[str, datetime]] = None,
    count: int = 1,
    panel: bool = True,
) -> Any:
    """
    获取连续财务数据（聚宽风格）。
    """
    _ensure_auth()
    if end_date is None:
        if _current_context:
            end_date = _current_context.current_dt.date() - timedelta(days=1)
        else:
            end_date = Date.today() - timedelta(days=1)
    resolved_end = _resolve_context_date(end_date, default_to_context=False)
    resolved_end = _ensure_not_future_date(resolved_end, "get_fundamentals_continuously.end_date")
    if _should_avoid_future() and resolved_end and _current_context:
        if resolved_end == _current_context.current_dt.date() and _query_mentions_valuation(query_object):
            if _current_context.current_dt.time() < Time(15, 0):
                raise FutureDataError(
                    "avoid_future_data=True时，回测中get_fundamentals_continuously取估值表数据需在15:00之后"
                )
    try:
        return _provider.get_fundamentals_continuously(
            query_object, end_date=resolved_end, count=count, panel=panel
        )
    except Exception as e:
        _raise_if_not_implemented(e)
        if _is_sql_unsupported(e):
            return _fallback_fundamentals_continuously(query_object, resolved_end, count, panel)
        _raise_permission_error(e, "get_fundamentals_continuously")
        log.error(f"获取连续财务数据失败: {e}")
        return pd.DataFrame()


def _fallback_fundamentals_continuously(
    query_object: Any,
    end_date: Optional[Date],
    count: int,
    panel: bool,
) -> pd.DataFrame:
    trade_days = get_trade_days(end_date=end_date, count=count)
    if not trade_days:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    for day in trade_days:
        day_date = pd.to_datetime(day).date()
        try:
            df = _provider.get_fundamentals(query_object, date=day_date, statDate=None)
        except Exception as exc:
            _raise_if_not_implemented(exc)
            _raise_permission_error(exc, "get_fundamentals")
            log.error(f"获取财务数据失败: {exc}")
            continue
        if df is None or getattr(df, 'empty', False):
            continue
        df = df.copy()
        if 'day' not in df.columns:
            df['day'] = day_date
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    merged = pd.concat(frames, ignore_index=True)
    if panel:
        return merged
    return merged


def get_trade_day(security: Union[str, List[str]], query_dt: Union[str, datetime]) -> Any:
    """
    获取指定时间的交易日（聚宽风格）。
    """
    _ensure_auth()
    resolved_dt = _resolve_context_dt(query_dt, default_to_context=False)
    resolved_dt = _ensure_not_future_dt(resolved_dt, "get_trade_day.query_dt")
    try:
        return _provider.get_trade_day(security, resolved_dt or query_dt)
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取交易日失败: {e}")
        return None


def get_index_weights(index_id: str, date: Optional[Union[str, datetime]] = None) -> Any:
    """
    获取指数权重（聚宽风格）。
    """
    _ensure_auth()
    resolved_date = _resolve_context_date(date, default_to_context=True)
    resolved_date = _ensure_not_future_date(resolved_date, "get_index_weights.date")
    _ensure_history_view("get_index_weights", resolved_date)
    try:
        return _provider.get_index_weights(index_id, date=resolved_date)
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取指数权重失败: {e}")
        return pd.DataFrame()


def get_industry_stocks(industry_code: str, date: Optional[Union[str, datetime]] = None) -> List[str]:
    """
    获取行业成分股（聚宽风格）。
    """
    _ensure_auth()
    resolved_date = _resolve_context_date(date, default_to_context=True)
    resolved_date = _ensure_not_future_date(resolved_date, "get_industry_stocks.date")
    _ensure_history_view("get_industry_stocks", resolved_date)
    try:
        return _provider.get_industry_stocks(industry_code, date=resolved_date)
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取行业成分股失败: {e}")
        return []


def get_industry(security: Union[str, List[str]], date: Optional[Union[str, datetime]] = None) -> Any:
    """
    获取标的行业信息（聚宽风格）。
    """
    _ensure_auth()
    resolved_date = _resolve_context_date(date, default_to_context=True)
    resolved_date = _ensure_not_future_date(resolved_date, "get_industry.date")
    _ensure_history_view("get_industry", resolved_date)
    try:
        return _provider.get_industry(security, date=resolved_date)
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取行业信息失败: {e}")
        return {}


def get_concept_stocks(concept_code: str, date: Optional[Union[str, datetime]] = None) -> List[str]:
    """
    获取概念成分股（聚宽风格）。
    """
    _ensure_auth()
    resolved_date = _resolve_context_date(date, default_to_context=True)
    resolved_date = _ensure_not_future_date(resolved_date, "get_concept_stocks.date")
    _ensure_history_view("get_concept_stocks", resolved_date)
    try:
        return _provider.get_concept_stocks(concept_code, date=resolved_date)
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取概念成分股失败: {e}")
        return []


def get_concept(security: Union[str, List[str]], date: Optional[Union[str, datetime]] = None) -> Any:
    """
    获取标的概念信息（聚宽风格）。
    """
    _ensure_auth()
    resolved_date = _resolve_context_date(date, default_to_context=True)
    resolved_date = _ensure_not_future_date(resolved_date, "get_concept.date")
    _ensure_history_view("get_concept", resolved_date)
    try:
        return _provider.get_concept(security, date=resolved_date)
    except Exception as e:
        _raise_if_not_implemented(e)
        _raise_permission_error(e, "get_concept")
        log.error(f"获取概念信息失败: {e}")
        return {}


def get_fund_info(security: str, date: Optional[Union[str, datetime]] = None) -> Any:
    """
    获取基金信息（聚宽风格）。
    """
    _ensure_auth()
    resolved_date = _resolve_context_date(date, default_to_context=True)
    resolved_date = _ensure_not_future_date(resolved_date, "get_fund_info.date")
    try:
        return _provider.get_fund_info(security, date=resolved_date)
    except Exception as e:
        _raise_if_not_implemented(e)
        _raise_permission_error(e, "get_fund_info")
        log.error(f"获取基金信息失败: {e}")
        return {}


def get_margincash_stocks(date: Optional[Union[str, datetime]] = None) -> Any:
    """
    获取融资标的列表（聚宽风格）。
    """
    _ensure_auth()
    resolved_date = _resolve_context_date(date, default_to_context=True)
    resolved_date = _ensure_not_future_date(resolved_date, "get_margincash_stocks.date")
    _ensure_history_view("get_margincash_stocks", resolved_date)
    try:
        return _provider.get_margincash_stocks(resolved_date)
    except Exception as e:
        _raise_if_not_implemented(e)
        _raise_permission_error(e, "get_margincash_stocks")
        log.error(f"获取融资标的失败: {e}")
        return []


def get_marginsec_stocks(date: Optional[Union[str, datetime]] = None) -> Any:
    """
    获取融券标的列表（聚宽风格）。
    """
    _ensure_auth()
    resolved_date = _resolve_context_date(date, default_to_context=True)
    resolved_date = _ensure_not_future_date(resolved_date, "get_marginsec_stocks.date")
    _ensure_history_view("get_marginsec_stocks", resolved_date)
    try:
        return _provider.get_marginsec_stocks(resolved_date)
    except Exception as e:
        _raise_if_not_implemented(e)
        _raise_permission_error(e, "get_marginsec_stocks")
        log.error(f"获取融券标的失败: {e}")
        return []


def get_dominant_future(underlying_symbol: str, date: Optional[Union[str, datetime]] = None) -> Any:
    """
    获取主力期货合约（聚宽风格）。
    """
    _ensure_auth()
    resolved_date = _resolve_context_date(date, default_to_context=True)
    resolved_date = _ensure_not_future_date(resolved_date, "get_dominant_future.date")
    try:
        return _provider.get_dominant_future(underlying_symbol, resolved_date)
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取主力合约失败: {e}")
        return None


def get_future_contracts(underlying_symbol: str, date: Optional[Union[str, datetime]] = None) -> Any:
    """
    获取期货合约列表（聚宽风格）。
    """
    _ensure_auth()
    resolved_date = _resolve_context_date(date, default_to_context=True)
    resolved_date = _ensure_not_future_date(resolved_date, "get_future_contracts.date")
    try:
        return _provider.get_future_contracts(underlying_symbol, resolved_date)
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取期货合约失败: {e}")
        return []


def get_billboard_list(
    stock_list: Optional[List[str]] = None,
    start_date: Optional[Union[str, datetime]] = None,
    end_date: Optional[Union[str, datetime]] = None,
    count: Optional[int] = None,
) -> Any:
    """
    获取龙虎榜数据（聚宽风格）。
    """
    _ensure_auth()
    if end_date is None and _current_context:
        end_date = _current_context.current_dt.date() - timedelta(days=1)
    resolved_start = _resolve_context_date(start_date, default_to_context=False)
    resolved_end = _resolve_context_date(end_date, default_to_context=True)
    resolved_end = _ensure_not_future_date(resolved_end, "get_billboard_list.end_date")
    if _should_avoid_future() and resolved_end and _current_context:
        if resolved_end == _current_context.current_dt.date() and _current_context.current_dt.time() < Time(15, 0):
            raise FutureDataError(
                "avoid_future_data=True时，回测中get_billboard_list只能在收盘后获取当日数据"
            )
    try:
        return _provider.get_billboard_list(
            stock_list=stock_list,
            start_date=resolved_start,
            end_date=resolved_end,
            count=count,
        )
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取龙虎榜数据失败: {e}")
        return pd.DataFrame()


def get_locked_shares(
    stock_list: List[str],
    start_date: Optional[Union[str, datetime]] = None,
    end_date: Optional[Union[str, datetime]] = None,
    forward_count: Optional[int] = None,
) -> Any:
    """
    获取限售股数据（聚宽风格）。
    """
    _ensure_auth()
    resolved_start = _resolve_context_date(start_date, default_to_context=False)
    resolved_end = _resolve_context_date(end_date, default_to_context=True)
    if resolved_start is None and resolved_end is not None:
        resolved_start = resolved_end
    resolved_end = _ensure_not_future_date(resolved_end, "get_locked_shares.end_date")
    try:
        return _provider.get_locked_shares(
            stock_list=stock_list,
            start_date=resolved_start,
            end_date=resolved_end,
            forward_count=forward_count,
        )
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取限售股数据失败: {e}")
        return pd.DataFrame()


def _should_use_live_current() -> bool:
    """判断是否使用 LiveCurrentData：只要是实盘则启用。"""
    try:
        from ..core.globals import g  # type: ignore
        return bool(getattr(g, 'live_trade', False))
    except Exception:
        return False


def get_current_data() -> Any:
    """
    获取当前行情数据容器（避免未来函数）
    
    返回一个CurrentData对象，支持延迟加载。
    当访问 current_data[security] 时才真正获取该标的的数据。
    
    Returns:
        CurrentData对象，支持字典式访问
        
    Examples:
        >>> current_data = get_current_data()
        >>> price = current_data['000001.XSHE'].last_price
        >>> if '000001.XSHE' in current_data:
        >>>     print(current_data['000001.XSHE'].paused)
    """
    if not _current_context:
        # 返回一个空的CurrentData对象
        class EmptyCurrentData:
            def __getitem__(self, key):
                return SecurityUnitData(security=key, last_price=0.0)
            def __contains__(self, key):
                return False
            def keys(self):
                return []
        return EmptyCurrentData()
    
    return LiveCurrentData(_current_context) if _should_use_live_current() else BacktestCurrentData(_current_context)


def get_trade_days(
    start_date: Optional[Union[str, datetime]] = None,
    end_date: Optional[Union[str, datetime]] = None,
    count: Optional[int] = None
) -> List[datetime]:
    """
    获取交易日列表（避免未来函数）
    
    Args:
        start_date: 开始日期
        end_date: 结束日期（会自动限制在当前回测时间之前）
        count: 获取数量
        
    Returns:
        交易日列表
    """
    if _current_context:
        max_dt = _current_context.current_dt
        resolved_end = _resolve_context_dt(end_date, default_to_context=True)
        if resolved_end is not None:
            if _should_avoid_future() and resolved_end > max_dt:
                raise FutureDataError(
                    f"avoid_future_data=True时，get_trade_days的end_date({resolved_end})不能大于当前时间({max_dt})"
                )
            if resolved_end > max_dt:
                resolved_end = max_dt
        end_date = resolved_end
    if start_date is None and count is None:
        if end_date is None:
            end_date = Date.today()
        count = 1

    try:
        trade_days = _provider.get_trade_days(
            start_date=start_date,
            end_date=end_date,
            count=count
        )
        return [pd.to_datetime(d) for d in trade_days]
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取交易日失败: {e}")
        return []


def get_all_securities(
    types: Union[str, List[str]] = 'stock',
    date: Optional[Union[str, datetime]] = None
) -> pd.DataFrame:
    """
    获取所有标的信息
    
    Args:
        types: 标的类型 ('stock', 'fund', 'index', 'futures', 'etf', 'lof', 'fja', 'fjb')
        date: 日期（会自动限制在当前回测时间之前）
        
    Returns:
        DataFrame
    """
    resolved_date = _resolve_context_date(date, default_to_context=True)
    resolved_date = _ensure_not_future_date(resolved_date, "get_all_securities.date")
    _ensure_history_view("get_all_securities", resolved_date)
    
    try:
        return _provider.get_all_securities(types=types, date=resolved_date)
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取标的信息失败: {e}")
        return pd.DataFrame()


def get_index_stocks(
    index_symbol: str,
    date: Optional[Union[str, datetime]] = None
) -> List[str]:
    """
    获取指数成分股
    
    Args:
        index_symbol: 指数代码
        date: 日期（会自动限制在当前回测时间之前）
        
    Returns:
        成分股代码列表
    """
    resolved_date = _resolve_context_date(date, default_to_context=True)
    resolved_date = _ensure_not_future_date(resolved_date, "get_index_stocks.date")
    _ensure_history_view("get_index_stocks", resolved_date)
    
    try:
        return _provider.get_index_stocks(index_symbol, date=resolved_date)
    except Exception as e:
        _raise_if_not_implemented(e)
        log.error(f"获取指数成分股失败: {e}")
        return []


def get_tick_decimals(security: str) -> int:
    """获取标的的价格精度（小数位数）。
    
    根据 security_overrides.json 的配置确定：
    - stock: 2位小数（0.01步长）
    - fund/money_market_fund: 3位小数（0.001步长）
    
    Args:
        security: 证券代码，如 '512100.XSHG'
    
    Returns:
        int: 价格精度，2 或 3
    """
    info = get_security_info(security)
    category = str(info.get('category') or '').lower()
    if category in ('fund', 'money_market_fund'):
        return 3
    return 2


__all__ = [
    'get_price', 'history', 'attribute_history', 'get_bars', 'get_ticks', 'get_current_tick',
    'get_current_data', 'get_trade_days', 'get_trade_day',
    'get_all_securities', 'get_security_info', 'get_fund_info',
    'get_index_stocks', 'get_index_weights',
    'get_industry_stocks', 'get_industry', 'get_concept_stocks', 'get_concept',
    'get_margincash_stocks', 'get_marginsec_stocks',
    'get_dominant_future', 'get_future_contracts',
    'get_billboard_list', 'get_locked_shares',
    'get_extras', 'get_fundamentals', 'get_fundamentals_continuously',
    'get_split_dividend',
    'set_current_context', 'set_data_provider', 'get_data_provider',
    'set_security_overrides', 'reset_security_overrides', 'get_tick_decimals',
]


# =========================
# 分红/拆分数据获取（统一结构）
# =========================
def _to_date(d: Optional[Union[str, datetime, Date]]) -> Optional[Date]:
    if d is None:
        return None
    if isinstance(d, Date) and not isinstance(d, datetime):
        return d
    try:
        return pd.to_datetime(d).date()
    except Exception:
        return None


def _infer_security_type(security: str, ref_date: Optional[Date]) -> str:
    """
    通过 get_all_securities 推断标的类型。
    返回值之一：'stock', 'etf', 'lof', 'fund', 'fja', 'fjb'
    未识别则默认 'stock'。
    """
    try:
        check_date = ref_date or (_current_context.current_dt.date() if _current_context else None)
        for t in ['stock', 'etf', 'lof', 'fund', 'fja', 'fjb']:
            df = _provider.get_all_securities(types=t, date=check_date)
            if not df.empty and security in df.index:
                return t
    except Exception:
        pass
    return 'stock'


def _parse_dividend_note(note: str) -> Dict[str, Any]:
    """
    解析股票分红文字说明，提取每10股的送股、转增、派现（税前）数值。
    返回字典：{'per_base': 10, 'stock_paid': float, 'into_shares': float, 'bonus_pre_tax': float}
    若出现“每股”则按每股计，per_base=1。
    该解析为启发式，尽量覆盖常见格式：
    - “每10股派X元(含税)” / “10派X元” / “派X元(每10股)”
    - “每10股送X股” / “10送X股”
    - “每10股转增X股” / “10转X股” / “转增X股(每10股)”
    """
    result = {'per_base': 10, 'stock_paid': 0.0, 'into_shares': 0.0, 'bonus_pre_tax': 0.0}
    if not note:
        return result
    s = note.replace('（', '(').replace('）', ')').replace('，', ',').replace('。', '.')
    s = re.sub(r'\s+', '', s)

    # 基数：每股 或 每10股
    if '每股' in s:
        result['per_base'] = 1
    elif '每10股' in s or re.search(r'(^|[^\d])10(送|转|派)', s):
        result['per_base'] = 10

    # 派现（税前）
    m = re.search(r'(每10股|10派|派)(?P<val>\d+(?:\.\d+)?)(元|现金)?', s)
    if not m and '每股' in s:
        m = re.search(r'每股派(?P<val>\d+(?:\.\d+)?)(元|现金)?', s)
        if m:
            # 每股派 -> 每10股派
            result['bonus_pre_tax'] = float(m.group('val')) * 10
    elif m:
        result['bonus_pre_tax'] = float(m.group('val'))

    # 送股
    m = re.search(r'(每10股|10送|送)(?P<val>\d+(?:\.\d+)?)(股)?', s)
    if not m and '每股' in s:
        m = re.search(r'每股送(?P<val>\d+(?:\.\d+)?)(股)?', s)
        if m:
            result['stock_paid'] = float(m.group('val')) * 10
    elif m:
        result['stock_paid'] = float(m.group('val'))

    # 转增
    m = re.search(r'(每10股|10转|转增|转)(?P<val>\d+(?:\.\d+)?)(股)?', s)
    if not m and '每股' in s:
        m = re.search(r'每股转增(?P<val>\d+(?:\.\d+)?)(股)?', s)
        if m:
            result['into_shares'] = float(m.group('val')) * 10
    elif m:
        result['into_shares'] = float(m.group('val'))

    return result


def get_split_dividend(
    security: str,
    start_date: Optional[Union[str, datetime, Date]] = None,
    end_date: Optional[Union[str, datetime, Date]] = None
) -> List[Dict[str, Any]]:
    """
    获取指定标的在区间内的分红/拆分事件（统一结构）。

    返回的每个事件为字典：
    - 'security': 代码
    - 'date': 日期（ex_date 或 day）
    - 'security_type': 'stock'/'etf'/'lof'/'fund'/'fja'/'fjb'
    - 'scale_factor': float，拆分/送转后持仓系数；无则为1.0
    - 'bonus_pre_tax': float，每10股（或每份）派现金额（税前）
    - 'per_base': int，基数（股票通常为10，货币基金为1）
    """
    # 统一日期
    sd = _to_date(start_date)
    ed = _to_date(end_date)
    if _current_context:
        cd = _current_context.current_dt.date()
        if ed is None or ed > cd:
            ed = cd
        if sd is None:
            sd = ed
    elif sd is None or ed is None:
        raise UserError('get_split_dividend 需要提供开始/结束日期，或在回测上下文中调用')

    # 直接通过当前数据提供者获取标准化事件
    try:
        return _provider.get_split_dividend(security, start_date=sd, end_date=ed)
    except Exception as e:
        _raise_if_not_implemented(e)
        log.debug(f"获取分红数据失败[{security}]: {e}")
        return []
