from __future__ import annotations

import os
from datetime import datetime, date as Date
from typing import Any, Dict, List, Optional, Union, Tuple

import pandas as pd
import logging

from .base import DataProvider
from ..cache import CacheManager

logger = logging.getLogger(__name__)


class MiniQMTProvider(DataProvider):
    """
    基于 xtquant.xtdata 的本地 QMT 数据提供者。
    依赖安装 miniQMT/xtquant 客户端，并能够访问本地行情数据目录。
    """

    name: str = "miniqmt"
    requires_live_data: bool = True
    _SUFFIX_TO_QMT: Dict[str, str] = {
        "SZ": "SZ",
        "XSHE": "SZ",
        "SH": "SH",
        "XSHG": "SH",
    }
    _QMT_TO_JQ: Dict[str, str] = {
        "SZ": "XSHE",
        "SH": "XSHG",
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config = config or {}
        self.data_dir = self.config.get("data_dir") or os.getenv("QMT_DATA_PATH")
        if self.data_dir:
            self.config["data_dir"] = self.data_dir
        cache_dir_set = "cache_dir" in self.config
        cache_dir = self.config.get("cache_dir")
        self.market = (self.config.get("market") or os.getenv("MINIQMT_MARKET") or "SH").upper()
        self.mode = (self.config.get("mode") or "backtest").lower()
        if self.mode not in {"backtest", "live"}:
            self.mode = "backtest"
        auto_download = self.config.get("auto_download")
        if auto_download is None:
            env_auto_str = os.getenv("MINIQMT_AUTO_DOWNLOAD")
            if env_auto_str is not None:
                auto_download = env_auto_str.lower() in ("1", "true", "yes", "on")
        if auto_download is None:
            auto_download = self.mode != "live"
        self.auto_download = bool(auto_download)
        self.config["auto_download"] = self.auto_download
        self._cache = CacheManager(
            provider_name=self.name,
            cache_dir=cache_dir,
            fallback_to_env=not cache_dir_set,
        )
        self._tick_callback = None

    # ------------------------ 工具函数 ------------------------
    @staticmethod
    def _ensure_xtdata():
        try:
            from xtquant import xtdata  # type: ignore

            return xtdata
        except ImportError as exc:  # pragma: no cover - 仅在缺少依赖时触发
            raise ImportError(
                "miniQMT 数据源依赖 xtquant，请安装官方 SDK（pip install xtquant）或 bullet-trade[qmt]"
            ) from exc

    @classmethod
    def _normalize_security_code(cls, security: str) -> str:
        if not security:
            return security
        sec = security.strip()
        if not sec:
            return sec
        if "." not in sec:
            return sec.upper()
        code, suffix = sec.split(".", 1)
        mapped = cls._SUFFIX_TO_QMT.get(suffix.upper(), suffix.upper())
        return f"{code.upper()}.{mapped}"

    @classmethod
    def _format_like_template(cls, normalized: str, template: str) -> str:
        if not normalized or "." not in normalized or not template or "." not in template:
            return normalized
        code = normalized.split(".", 1)[0]
        qmt_suffix = normalized.split(".", 1)[1].upper()
        tpl_suffix = template.strip().split(".", 1)[1].upper()
        mapped = cls._SUFFIX_TO_QMT.get(tpl_suffix, tpl_suffix)
        if mapped == qmt_suffix:
            return f"{code}.{template.split('.', 1)[1]}"
        return normalized

    @classmethod
    def _to_jq_code(cls, normalized: str) -> str:
        if not normalized or "." not in normalized:
            return normalized
        code, suffix = normalized.split(".", 1)
        jq_suffix = cls._QMT_TO_JQ.get(suffix.upper())
        if jq_suffix:
            return f"{code}.{jq_suffix}"
        return normalized

    @staticmethod
    def _to_date(value: Optional[Union[str, datetime, Date]]) -> Optional[Date]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, Date):
            return value
        try:
            return pd.to_datetime(value).date()
        except Exception:
            return None

    @staticmethod
    def _normalize_requested_security_type(value: Any) -> str:
        cleaned = str(value or "").strip().lower()
        if not cleaned:
            return "stock"
        if cleaned == "fund":
            return "fund"
        if cleaned in {"stock", "etf", "index"}:
            return cleaned
        return cleaned

    @classmethod
    def _resolve_sector_type(cls, requested_type: str) -> Optional[tuple[str, str]]:
        normalized = cls._normalize_requested_security_type(requested_type)
        if normalized == "fund":
            return "etf", "fund"
        if normalized in {"stock", "etf", "index"}:
            return normalized, normalized
        return None

    @staticmethod
    def _extract_instrument_type(raw_type: Any) -> Optional[str]:
        if isinstance(raw_type, str):
            cleaned = raw_type.strip().lower()
            return cleaned or None
        if isinstance(raw_type, dict):
            for key, enabled in raw_type.items():
                if enabled:
                    cleaned = str(key).strip().lower()
                    if cleaned:
                        return cleaned
            if len(raw_type) == 1:
                key = next(iter(raw_type))
                cleaned = str(key).strip().lower()
                return cleaned or None
        return None

    def _detect_instrument_type(self, xt: Any, security: str) -> Optional[str]:
        detector = getattr(xt, "get_instrument_type", None)
        if not callable(detector):
            return None
        try:
            return self._extract_instrument_type(detector(security))
        except Exception:
            return None

    def _format_time(self, value: Optional[Union[str, datetime, Date]], period: str) -> str:
        if value is None:
            return ""
        dt = value
        if isinstance(value, str):
            dt = pd.to_datetime(value)
        if isinstance(dt, Date) and not isinstance(dt, datetime):
            dt = datetime.combine(dt, datetime.min.time())
        normalized = self._normalize_period(period)
        fmt = "%Y%m%d" if normalized == "1d" else "%Y%m%d%H%M%S"
        return dt.strftime(fmt)

    def get_current_tick(
        self,
        security: str,
        dt: Optional[Union[str, datetime]] = None,
        df: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        返回简化 tick：last_price + 时间戳。优先调用 xtdata.get_last_quote，失败则回退到 1m K 线最新价。
        """
        _ = dt, df
        try:
            xtdata = self._ensure_xtdata()
            code = self._normalize_security_code(security)
            quote = xtdata.get_last_quote(code)  # type: ignore[attr-defined]
            if quote:
                if isinstance(quote, dict):
                    last = quote.get("lastPrice") or quote.get("last_price") or quote.get("price")
                    ts = quote.get("time") or quote.get("datetime")
                else:
                    last = getattr(quote, "lastPrice", None) or getattr(quote, "last_price", None) or getattr(quote, "price", None)
                    ts = getattr(quote, "time", None) or getattr(quote, "datetime", None)
                if ts is None:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if last is not None:
                    return {"sid": self._to_jq_code(code), "last_price": float(last), "dt": ts}
        except Exception:
            pass
        try:
            df = self.get_price(security, count=1, frequency="1m")
            if df is not None and not df.empty:
                last = df.iloc[-1]["close"]
                dt = df.index[-1] if df.index.name else df.iloc[-1].get("datetime", datetime.now())
                return {"sid": security, "last_price": float(last), "dt": str(dt)}
        except Exception:
            return None
        return None

    @staticmethod
    def _normalize_period(frequency: Optional[str]) -> str:
        """
        将框架内部的 frequency 文本统一映射为 xtquant 支持的 period 标记。
        默认回落到日线，避免传入非法字符串导致 xtquant 返回空数据。
        """
        if not frequency:
            return "1d"
        freq = str(frequency).strip().lower()
        alias = {
            "daily": "1d",
            "day": "1d",
            "1day": "1d",
            "d": "1d",
            "minute": "1m",
            "min": "1m",
            "1minute": "1m",
            "m": "1m",
        }
        normalized = alias.get(freq)
        if normalized:
            return normalized
        # 保留 xtquant 直接支持的 n?m/n?d 形式
        if freq.endswith(("m", "d")) and freq[:-1].isdigit():
            return freq
        return "1d"

    # ------------------------ 认证 ------------------------
    def auth(self, user: Optional[str] = None, pwd: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = None) -> None:
        _ = user, pwd, host, port
        xt = self._ensure_xtdata()
        if not self.data_dir:
            env_dir = os.getenv("QMT_DATA_PATH")
            if env_dir:
                self.data_dir = env_dir
                self.config["data_dir"] = env_dir

    # ------------------------ Tick 订阅 ------------------------
    def subscribe_ticks(self, symbols: List[str]) -> None:
        try:
            xtdata = self._ensure_xtdata()
            mapped = [self._normalize_security_code(s) for s in symbols]
            for code in mapped:
                xtdata.subscribe_quote(code, period="tick", callback=self._tick_callback)  # type: ignore[attr-defined]
            logger.info("MiniQMT 订阅 tick: %s", mapped)
        except Exception as exc:
            logger.error("MiniQMT 订阅 tick 失败: %s", exc)
            raise

    def subscribe_markets(self, markets: List[str]) -> None:
        try:
            xtdata = self._ensure_xtdata()
            # subscribe_whole_quote 不支持 period 参数
            xtdata.subscribe_whole_quote(list(markets), callback=self._tick_callback)  # type: ignore[attr-defined]
            logger.info("MiniQMT 订阅全市场 tick: %s", markets)
        except Exception as exc:
            logger.error("MiniQMT 订阅全市场 tick 失败: %s", exc)
            raise

    def unsubscribe_ticks(self, symbols: Optional[List[str]] = None) -> None:
        try:
            xtdata = self._ensure_xtdata()
            if symbols:
                mapped = [self._normalize_security_code(s) for s in symbols]
                if hasattr(xtdata, "unsubscribe_quote"):
                    xtdata.unsubscribe_quote(mapped)  # type: ignore[attr-defined]
            else:
                if hasattr(xtdata, "unsubscribe_all"):
                    xtdata.unsubscribe_all()  # type: ignore[attr-defined]
            logger.info("MiniQMT 退订 tick: %s", symbols if symbols else "ALL")
        except Exception:
            pass

    def unsubscribe_markets(self, markets: Optional[List[str]] = None) -> None:
        try:
            if not markets:
                return
            xtdata = self._ensure_xtdata()
            if hasattr(xtdata, "unsubscribe_whole_quote"):
                xtdata.unsubscribe_whole_quote(list(markets))  # type: ignore[attr-defined]
            logger.info("MiniQMT 退订市场 tick: %s", markets)
        except Exception:
            pass

    def set_tick_callback(self, callback) -> None:
        """
        设置 xtdata 的 tick 回调，调用时会在后续 subscribe 中透传。
        """
        self._tick_callback = callback

    # ------------------------ K 线数据 ------------------------
    def get_price(
        self,
        security: Union[str, List[str]],
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        frequency: str = "daily",
        fields: Optional[List[str]] = None,
        skip_paused: bool = False,
        fq: str = "pre",
        count: Optional[int] = None,
        panel: bool = True,
        fill_paused: bool = True,
        pre_factor_ref_date: Optional[Union[str, datetime]] = None,
        prefer_engine: bool = False,
        force_no_engine: bool = False,
    ) -> pd.DataFrame:
        securities = security if isinstance(security, (list, tuple)) else [security]
        frames: Dict[str, pd.DataFrame] = {}
        normalized_map: Dict[str, str] = {}
        unique_normalized: List[str] = []
        normalized_frequency = self._normalize_period(frequency)
        for sec in securities:
            normalized = self._normalize_security_code(sec)
            normalized_map[sec] = normalized
            if normalized not in unique_normalized:
                unique_normalized.append(normalized)

        cached_frames: Dict[str, pd.DataFrame] = {}
        for normalized in unique_normalized:
            kwargs = {
                "security": normalized,
                "start_date": start_date,
                "end_date": end_date,
                "frequency": normalized_frequency,
                "fields": fields,
                "skip_paused": skip_paused,
                "fq": fq,
                "count": count,
                "pre_factor_ref_date": pre_factor_ref_date,
            }

            def _fetch_single(kw: Dict[str, Any]) -> pd.DataFrame:
                return self._get_price_single(
                    kw["security"],
                    start_date=kw.get("start_date"),
                    end_date=kw.get("end_date"),
                    frequency=kw.get("frequency", "daily"),
                    fields=kw.get("fields"),
                    skip_paused=kw.get("skip_paused", False),
                    fq=kw.get("fq"),
                    count=kw.get("count"),
                    pre_factor_ref_date=kw.get("pre_factor_ref_date"),
                )

            cached_frames[normalized] = self._cache.cached_call("get_price", kwargs, _fetch_single, result_type="df")

        for sec in securities:
            normalized = normalized_map[sec]
            frames[sec] = cached_frames[normalized].copy()

        if len(frames) == 1:
            return next(iter(frames.values()))
        if panel:
            return pd.concat(frames, axis=1)

        rows = []
        for sec, df in frames.items():
            tmp = df.copy()
            tmp["code"] = sec
            rows.append(tmp)
        return pd.concat(rows, axis=0)

    def _get_price_single(
        self,
        security: str,
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
        frequency: str,
        fields: Optional[List[str]],
        skip_paused: bool,
        fq: Optional[str],
        count: Optional[int],
        pre_factor_ref_date: Optional[Union[str, datetime]],
    ) -> pd.DataFrame:
        xt = self._ensure_xtdata()
        security = self._normalize_security_code(security)
        period = self._normalize_period(frequency)
        start_str = self._format_time(start_date, period)
        end_str = self._format_time(end_date, period)
        if self.auto_download:
            xt.download_history_data(stock_code=security, period=period)

        raw_df = self._fetch_local_data(
            xt,
            security=security,
            period=period,
            start_time=start_str,
            end_time=end_str,
            count=count,
            dividend_type="none",
        )
        if raw_df.empty:
            return raw_df

        if fq == "pre":
            # Prefer xtquant built-in front-ratio (前复权) first, then anchor to ref date
            # This ensures parity with JQData when a pre_factor_ref_date is provided.
            adj_df = self._fetch_local_data(
                xt,
                security=security,
                period=period,
                start_time=start_str,
                end_time=end_str,
                count=count,
                dividend_type="front_ratio",
            )
            if adj_df.empty:
                # Fallback: event-based forward-adjust when local ratio data is unavailable
                adj_df = self._build_adjusted_from_events(security, raw_df, direction="pre")
            df = self._align_reference(raw_df, adj_df, pre_factor_ref_date)
            # Adjust numeric precision to match raw price precision (2 or 3+ decimals as needed)
            decimals = self._infer_price_decimals_from_raw(raw_df)
            df = self._round_price_columns(df, decimals)
        elif fq == "post":
            adj_df = self._fetch_local_data(
                xt,
                security=security,
                period=period,
                start_time=start_str,
                end_time=end_str,
                count=count,
                dividend_type="back_ratio",
            )
            if adj_df.empty:
                adj_df = self._build_adjusted_from_events(security, raw_df, direction="post")
            df = self._align_reference(raw_df, adj_df, pre_factor_ref_date, default_to_start=True)
            decimals = self._infer_price_decimals_from_raw(raw_df)
            df = self._round_price_columns(df, decimals)
        else:
            df = raw_df

        # 兼容 JQData 的 skip_paused=False 行为：填充停牌日数据
        # QMT 不返回停牌日的数据，但 JQData 会返回（volume=0, paused=1）
        # 当 skip_paused=False 且有 end_date 时，检查是否需要填充停牌日
        if not skip_paused and end_date and period == "1d" and not df.empty:
            df = self._fill_paused_days(df, start_date, end_date, xt)

        if skip_paused:
            df = df[df.get("volume", 0) > 0]

        if count:
            df = df.tail(count)

        if fields:
            missing = [f for f in fields if f not in df.columns]
            for f in missing:
                df[f] = 0.0
            df = df[fields]

        return df

    def _fill_paused_days(
        self,
        df: pd.DataFrame,
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
        xt,
    ) -> pd.DataFrame:
        """
        填充停牌日数据，使 QMT 行为与 JQData 的 skip_paused=False 一致。
        
        QMT 的 get_local_data 不返回停牌日的数据，但 JQData 会返回（volume=0, paused=1）。
        此方法检查日期范围内缺失的交易日，并用前一天的收盘价填充。
        """
        if df.empty:
            return df
        
        try:
            # 使用 df 的实际日期范围，而不是传入的 start_date/end_date
            # 因为传入的 end_date 可能被 _fetch_local_data 往后推了
            df_min_date = df.index.min()
            df_max_date = df.index.max()
            
            # 转换为日期对象
            if hasattr(df_min_date, 'date'):
                actual_start = df_min_date.date()
            elif hasattr(df_min_date, 'to_pydatetime'):
                actual_start = df_min_date.to_pydatetime().date()
            else:
                actual_start = df_min_date
                
            if hasattr(df_max_date, 'date'):
                actual_end = df_max_date.date()
            elif hasattr(df_max_date, 'to_pydatetime'):
                actual_end = df_max_date.to_pydatetime().date()
            else:
                actual_end = df_max_date
            
            # 如果有 end_date 参数，使用它作为实际结束日期（确保包含请求的 end_date）
            if end_date:
                try:
                    req_end = pd.to_datetime(end_date).date()
                    # 只在 req_end 在合理范围内时使用（不能是未来的日期）
                    from datetime import date as Date
                    today = Date.today()
                    if req_end <= today and req_end >= actual_start:
                        actual_end = max(actual_end, req_end)
                except Exception:
                    pass
            
            # 获取日期范围内的所有交易日
            start_str = self._format_time(actual_start, "1d")
            end_str = self._format_time(actual_end, "1d")
            
            logger.debug(f"QMT _fill_paused_days: 查询交易日范围 {start_str} - {end_str}")
            
            trade_days_ts = xt.get_trading_dates(
                self.market, 
                start_time=start_str, 
                end_time=end_str, 
                count=-1
            )
            if not trade_days_ts:
                return df
            
            # 转换为日期集合
            from datetime import datetime as dt
            trade_days = set()
            for ts in trade_days_ts:
                d = dt.fromtimestamp(ts / 1000.0).date()
                trade_days.add(d)
            
            # 获取 df 中已有的日期
            existing_dates = set()
            for idx in df.index:
                if hasattr(idx, 'date'):
                    existing_dates.add(idx.date())
                elif hasattr(idx, 'to_pydatetime'):
                    existing_dates.add(idx.to_pydatetime().date())
            
            # 找出缺失的交易日（停牌日）
            missing_dates = trade_days - existing_dates
            if not missing_dates:
                return df
            
            #logger.debug(f"QMT _fill_paused_days: 发现 {len(missing_dates)} 个停牌日需要填充: {sorted(missing_dates)}")
            
            # 为缺失的日期创建填充行
            fill_rows = []
            for missing_date in sorted(missing_dates):
                # 找到该日期之前最近的数据行作为参考
                ref_row = None
                missing_ts = pd.Timestamp(missing_date)
                earlier_rows = df[df.index < missing_ts]
                if not earlier_rows.empty:
                    ref_row = earlier_rows.iloc[-1]
                elif not df.empty:
                    # 如果没有更早的数据，用第一行作为参考
                    ref_row = df.iloc[0]
                
                if ref_row is not None:
                    # 创建停牌日数据行
                    fill_data = {}
                    close_price = ref_row.get('close', 0.0)
                    for col in df.columns:
                        if col in ('open', 'high', 'low', 'close'):
                            fill_data[col] = close_price
                        elif col == 'volume':
                            fill_data[col] = 0.0
                        elif col == 'money':
                            fill_data[col] = 0.0
                        elif col == 'paused':
                            fill_data[col] = 1.0  # 标记为停牌
                        else:
                            fill_data[col] = 0.0
                    
                    fill_rows.append((pd.Timestamp(missing_date).normalize(), fill_data))
            
            if fill_rows:
                # 添加 paused 列（如果不存在）
                if 'paused' not in df.columns:
                    df = df.copy()
                    df['paused'] = 0.0
                
                # 创建填充 DataFrame 并合并
                fill_df = pd.DataFrame(
                    [row[1] for row in fill_rows],
                    index=[row[0] for row in fill_rows]
                )
                df = pd.concat([df, fill_df]).sort_index()
                #logger.debug(f"QMT _fill_paused_days: 填充后 df.index={df.index.tolist()[-5:] if len(df) > 5 else df.index.tolist()}")
            
            return df
            
        except Exception as e:
            logger.debug(f"QMT _fill_paused_days: 填充失败 {e}")
            return df

    @staticmethod
    def _round_price_columns(df: pd.DataFrame, decimals: int) -> pd.DataFrame:
        if df.empty or decimals is None:
            return df
        cols = [c for c in ("open", "high", "low", "close") if c in df.columns]
        if not cols:
            return df
        out = df.copy()
        for c in cols:
            out[c] = out[c].astype(float).round(decimals)
        return out

    @staticmethod
    def _infer_price_decimals_from_raw(raw_df: pd.DataFrame) -> int:
        """Infer price precision from raw series by checking how closely values align
        to 2 or 3 decimals. Fall back to 2 if uncertain.
        """
        if raw_df is None or raw_df.empty:
            return 2
        cols = [c for c in ("open", "high", "low", "close") if c in raw_df.columns]
        if not cols:
            return 2
        sample = raw_df[cols].tail(min(len(raw_df), 40)).to_numpy().ravel()
        sample = [float(x) for x in sample if pd.notna(x)]
        if not sample:
            return 2
        def _score(dec):
            tol = 5e-5 if dec <= 2 else 5e-6
            ok = 0
            for v in sample:
                if abs(v - round(v, dec)) <= tol:
                    ok += 1
            return ok / len(sample)
        score2 = _score(2)
        score3 = _score(3)
        # Prefer smallest decimals that fits well; require high fit to choose lower precision
        if score2 >= 0.98:
            return 2
        if score3 >= 0.98 or score3 > score2:
            return 3
        # Fall back to 2
        return 2

    def _fetch_local_data(
        self,
        xt,
        security: str,
        period: str,
        start_time: str,
        end_time: str,
        count: Optional[int],
        dividend_type: str,
    ) -> pd.DataFrame:
        # 注意：QMT 的 get_local_data 在使用 count 参数时会跳过停牌日，
        # 导致与 JQData 行为不一致（JQData 会包含停牌日的数据）。
        # 
        # 解决方案：当同时指定 end_time 和 count 时，不传 count 给 QMT，
        # 而是先获取到 end_time 的完整数据，再用 tail(count) 截取。
        # 这样可以确保停牌日的数据也被包含在内。
        use_count_in_xt = count
        use_start_time = start_time
        
        use_end_time = end_time
        
        if end_time and count and not start_time:
            # 当没有 start_time 但有 end_time 和 count 时，
            # 需要构造一个合理的 start_time，否则 QMT 不知道从哪开始获取
            # 往前推 count + 30 天作为 start_time（多取一些以覆盖停牌等情况）
            # 同时将 end_time 往后推几天，因为 QMT 在 end_time 是停牌日时不返回该天数据
            try:
                end_dt = pd.to_datetime(end_time)
                buffer_days = max(count * 2, 30)  # 取 count*2 和 30 的较大值
                start_dt = end_dt - pd.Timedelta(days=buffer_days)
                use_start_time = start_dt.strftime("%Y%m%d")
                # 将 end_time 往后推 10 天，确保停牌日也能被包含（QMT 行为不一致，需要更大的缓冲）
                use_end_time = (end_dt + pd.Timedelta(days=10)).strftime("%Y%m%d")
                use_count_in_xt = -1  # 不让 QMT 处理 count
                logger.debug(f"QMT _fetch_local_data: 构造 start_time={use_start_time}, end_time={use_end_time}(原{end_time}), count={count}")
            except Exception:
                pass
        elif end_time and count:
            # 有 start_time 的情况，也不让 QMT 处理 count
            # 同样需要将 end_time 往后推
            try:
                end_dt = pd.to_datetime(end_time)
                use_end_time = (end_dt + pd.Timedelta(days=10)).strftime("%Y%m%d")
                use_count_in_xt = -1
            except Exception:
                pass
        
        logger.debug(
            f"QMT _fetch_local_data: 调用 xt.get_local_data(stock_list=[{security}], "
            f"count={use_count_in_xt or -1}, period={period}, "
            f"start_time={use_start_time}, end_time={end_time}, dividend_type={dividend_type})"
        )
        try:
            data = xt.get_local_data(
                stock_list=[security],
                count=use_count_in_xt or -1,
                period=period,
                start_time=use_start_time,
                end_time=end_time,
                dividend_type=dividend_type,
            )
        except Exception as e:
            logger.error(
                f"QMT _fetch_local_data: xt.get_local_data 调用失败: {type(e).__name__}: {e} "
                f"(security={security}, period={period}, start_time={use_start_time}, end_time={end_time})"
            )
            raise
        
        #logger.debug(f"QMT _fetch_local_data: xt.get_local_data 返回 data.keys()={list(data.keys()) if data else None}")
        
        df = data.get(security)
        if df is None or df.empty:
            logger.debug(f"QMT _fetch_local_data: 返回空 DataFrame (df={df})")
            return pd.DataFrame()
        
        df = df.copy()
        #logger.debug(f"QMT _fetch_local_data: df.columns={list(df.columns)}, df.shape={df.shape}")
        
        # xtquant 时间戳为毫秒级 UTC，需要转换到沪深时区（Asia/Shanghai）再处理
        if "time" not in df.columns:
            logger.error(
                f"QMT _fetch_local_data: 数据缺少 'time' 列！"
                f"现有列: {list(df.columns)}, security={security}, period={period}"
            )
            raise KeyError(f"数据缺少 'time' 列 (security={security}, period={period}, columns={list(df.columns)})")
        
        idx_utc = pd.to_datetime(df["time"].astype("int64"), unit="ms", utc=True)
        idx = pd.DatetimeIndex(idx_utc).tz_convert("Asia/Shanghai").tz_localize(None)
        # Normalize daily bars to date-only index to align with JQData
        if period == "1d":
            idx = idx.normalize()
        df.index = idx
        df.index.name = None
        df.rename(columns={"amount": "money"}, inplace=True)
        df["money"] = df.get("money", 0.0)
        
        # 在这里处理 count，确保停牌日数据也被包含
        if end_time and count and not df.empty:
            #logger.debug(f"QMT _fetch_local_data: 截取前 df.index={df.index.tolist()[-5:] if len(df) > 5 else df.index.tolist()}")
            # 先过滤掉超过原始 end_time 的数据（因为我们把 end_time 往后推了）
            try:
                end_dt = pd.to_datetime(end_time)
                if period == "1d":
                    # 日线数据，index 已经 normalize 为当天 00:00:00
                    # 需要包含 end_time 当天的数据，所以用 <= end_dt.normalize()
                    end_dt_normalized = end_dt.normalize()
                    df = df[df.index <= end_dt_normalized]
                else:
                    df = df[df.index <= end_dt]
                #logger.debug(f"QMT _fetch_local_data: 过滤后 df.index={df.index.tolist()[-5:] if len(df) > 5 else df.index.tolist()}")
            except Exception as e:
                logger.debug(f"QMT _fetch_local_data: 过滤失败 {e}")
            # 然后再 tail(count)
            df = df.tail(count)
            #logger.debug(f"QMT _fetch_local_data: 截取后 df.index={df.index.tolist()}")
        
        return df

    @classmethod
    def _standardize_event(cls, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        标准化分红/拆分事件，统一格式但保持原有 per_base 口径。
        - 股票：per_base=10，bonus_pre_tax 为每10股派息
        - 基金/ETF：per_base=1，bonus_pre_tax 为每1份派息
        """
        normalized = dict(event)
        normalized_security = normalized.get("security")
        normalized["security"] = cls._normalize_security_code(normalized_security or "")
        
        # 保持原有 per_base，不做归一化
        try:
            normalized["per_base"] = float(normalized.get("per_base") or 1.0) or 1.0
        except Exception:
            normalized["per_base"] = 1.0
        
        # bonus_pre_tax 保持原值，不除以 per_base
        try:
            normalized["bonus_pre_tax"] = float(normalized.get("bonus_pre_tax") or 0.0)
        except Exception:
            normalized["bonus_pre_tax"] = 0.0
        
        try:
            normalized["scale_factor"] = float(normalized.get("scale_factor", 1.0) or 1.0)
        except Exception:
            normalized["scale_factor"] = 1.0
        
        date_value = normalized.get("date")
        if date_value is not None:
            try:
                normalized["date"] = pd.to_datetime(date_value).date()
            except Exception:
                pass
        return normalized

    def _collect_dividend_events(
        self,
        security: str,
        raw_df: pd.DataFrame,
    ) -> List[Dict[str, Any]]:
        if raw_df.empty:
            return []
        start_dt = raw_df.index.min()
        end_dt = raw_df.index.max()
        start_date = None
        end_date = None
        if hasattr(start_dt, "to_pydatetime") and pd.notna(start_dt):
            start_date = start_dt.to_pydatetime().date()
        if hasattr(end_dt, "to_pydatetime") and pd.notna(end_dt):
            end_date = end_dt.to_pydatetime().date()
        events = self._get_xt_split_dividend(security, start_date=start_date, end_date=end_date)
        if events:
            return [self._standardize_event(event) for event in events]
        return []

    def _build_adjusted_from_events(
        self,
        security: str,
        raw_df: pd.DataFrame,
        direction: str,
    ) -> pd.DataFrame:
        if direction not in {"pre", "post"}:
            return pd.DataFrame()
        events = self._collect_dividend_events(security, raw_df)
        if not events:
            return pd.DataFrame()
        if direction == "post":
            # 缺少官方后复权数据时回退到未复权，避免误差放大
            return pd.DataFrame()
        price_cols = [col for col in ["open", "high", "low", "close"] if col in raw_df.columns]
        if not price_cols:
            return pd.DataFrame()
        adj_df = raw_df.copy()
        # Build multiplicative forward-adjust factors series
        factors = pd.Series(1.0, index=adj_df.index)
        sorted_events = sorted(
            (
                {
                    **event,
                    "date": pd.to_datetime(event.get("date"), errors="coerce"),
                }
                for event in events
            ),
            key=lambda item: item["date"] if item["date"] is not pd.NaT else pd.Timestamp.max,
        )
        for event in sorted_events:
            event_date = event.get("date")
            if event_date is pd.NaT or event_date is None:
                continue
            event_day = event_date.date()
            mask = adj_df.index.date < event_day
            if not mask.any():
                continue
            # scale factor: for share bonus/split, forward-adjust multiply earlier prices by 1/scale
            try:
                scale = float(event.get("scale_factor") or 1.0)
            except Exception:
                scale = 1.0
            scale_factor = 1.0 / scale if scale and scale > 0 else 1.0
            # cash dividend: forward-adjust multiply earlier prices by (preclose - cash_per_share)/preclose
            try:
                cash = float(event.get("bonus_pre_tax") or 0.0)
            except Exception:
                cash = 0.0
            try:
                per_base = float(event.get("per_base") or 10.0)
            except Exception:
                per_base = 10.0
            # 将 bonus_pre_tax 转换为每股分红
            cash_per_share = cash / per_base if per_base > 0 else 0.0
            # Determine preclose for the ex-date
            preclose = None
            if "preClose" in adj_df.columns and event_day in adj_df.index.date:
                preclose = float(adj_df.loc[adj_df.index.date == event_day, "preClose"].iloc[0])
            if preclose is None or preclose == 0.0:
                # fallback to previous day's close
                prev = adj_df.index[adj_df.index.date < event_day]
                if len(prev) > 0 and "close" in adj_df.columns:
                    preclose = float(adj_df.loc[prev.max(), "close"])
            cash_factor = 1.0
            if cash_per_share and preclose and preclose > 0:
                cash_factor = max((preclose - cash_per_share) / preclose, 0.0)
            total_factor = scale_factor * cash_factor
            if total_factor != 1.0:
                factors.loc[mask] = factors.loc[mask] * total_factor
        # Apply factors to OHLC
        for col in price_cols:
            adj_df[col] = adj_df[col].astype(float) * factors
        return adj_df

    def _align_reference(
        self,
        raw_df: pd.DataFrame,
        adj_df: pd.DataFrame,
        pre_factor_ref_date: Optional[Union[str, datetime]],
        default_to_start: bool = False,
    ) -> pd.DataFrame:
        if adj_df.empty:
            return raw_df
        if not pre_factor_ref_date:
            return adj_df
        try:
            ref_dt = pd.to_datetime(pre_factor_ref_date)
        except Exception:
            ref_dt = adj_df.index[0 if default_to_start else -1]

        reference_raw = None
        reference_adj = None
        if ref_dt in raw_df.index and ref_dt in adj_df.index:
            reference_raw = raw_df.loc[ref_dt, "close"]
            reference_adj = adj_df.loc[ref_dt, "close"]
        else:
            ref_date = ref_dt.date() if hasattr(ref_dt, "date") else None
            if ref_date is not None and hasattr(raw_df.index, "date"):
                raw_mask = raw_df.index.date == ref_date
                adj_mask = adj_df.index.date == ref_date
                if raw_mask.any() and adj_mask.any():
                    try:
                        reference_raw = raw_df.loc[raw_mask, "close"].iloc[-1]
                        reference_adj = adj_df.loc[adj_mask, "close"].iloc[-1]
                    except Exception:
                        reference_raw = None
                        reference_adj = None
        if reference_raw is None or reference_adj is None:
            return adj_df
        if reference_adj == 0:
            return adj_df
        scale = reference_raw / reference_adj
        for col in ["open", "high", "low", "close"]:
            if col in adj_df.columns:
                adj_df[col] = adj_df[col] * scale
        return adj_df

    # ------------------------ 交易日/基础信息 ------------------------
    def get_trade_days(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        count: Optional[int] = None,
    ) -> List[datetime]:
        kwargs = {"start_date": start_date, "end_date": end_date, "count": count}

        def _fetch(kw: Dict[str, Any]) -> List[datetime]:
            xt = self._ensure_xtdata()
            start_str = self._format_time(kw.get("start_date"), "1d")
            end_str = self._format_time(kw.get("end_date"), "1d")
            data = xt.get_trading_dates(self.market, start_time=start_str, end_time=end_str, count=kw.get("count", -1) or -1)
            # 注意：QMT 返回的时间戳是北京时间（本地时区），需要使用 fromtimestamp 而不是 pd.to_datetime
            # pd.to_datetime(unit='ms') 会当作 UTC 时间处理，导致日期偏移
            from datetime import datetime as dt
            return [dt.fromtimestamp(ts / 1000.0) for ts in data]

        return self._cache.cached_call("get_trade_days", kwargs, _fetch, result_type="list_date")

    def get_trade_day(self, security: Union[str, List[str]], query_dt: Union[str, datetime]) -> Any:
        try:
            trade_days = self.get_trade_days(end_date=query_dt, count=1)
        except Exception:
            trade_days = []
        if not trade_days:
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

    def get_all_securities(
        self,
        types: Union[str, List[str]] = "stock",
        date: Optional[Union[str, datetime]] = None,
    ) -> pd.DataFrame:
        if isinstance(types, str):
            types = [types]
        normalized_types = tuple(self._normalize_requested_security_type(item) for item in types)
        kwargs = {"types": normalized_types, "date": date}

        def _fetch(kw: Dict[str, Any]) -> Dict[str, Any]:
            xt = self._ensure_xtdata()
            sectors = {"stock": "沪深A股", "etf": "沪深ETF", "index": "沪深指数"}
            rows = []
            resolved_types = []
            for requested_type in kw["types"]:
                resolved = self._resolve_sector_type(requested_type)
                if resolved and resolved not in resolved_types:
                    resolved_types.append(resolved)
            for sector_type, result_type in resolved_types:
                sector = sectors.get(sector_type)
                if not sector:
                    continue
                codes = xt.get_stock_list_in_sector(sector)
                for code in codes:
                    info = xt.get_instrument_detail(code)
                    if not info or not isinstance(info, dict):
                        rows.append(
                            {
                                "ts_code": code,
                                "display_name": code,
                                "name": code,
                                "start_date": None,
                                "end_date": None,
                                "type": result_type,
                            }
                        )
                        continue
                    rows.append(
                        {
                            "ts_code": code,
                            "display_name": info.get("InstrumentName", code),
                            "name": info.get("InstrumentID", code),
                            "start_date": pd.to_datetime(info.get("OpenDate"), errors="coerce"),
                            "end_date": pd.to_datetime(info.get("ExpireDate"), errors="coerce"),
                            "type": result_type,
                        }
                    )
            if not rows:
                return {}
            df = pd.DataFrame(rows).drop_duplicates("ts_code").set_index("ts_code")
            return df.to_dict(orient="index")

        data = self._cache.cached_call("get_all_securities", kwargs, _fetch, result_type="list_dict")
        if not data:
            return pd.DataFrame(columns=["display_name", "name", "start_date", "end_date", "type"])
        df = pd.DataFrame.from_dict(data, orient="index")
        if not df.empty:
            df["qmt_code"] = df.index
            jq_codes = [self._to_jq_code(code) for code in df.index]
            df.index = jq_codes
        df["start_date"] = pd.to_datetime(df["start_date"])
        df["end_date"] = pd.to_datetime(df["end_date"])
        return df

    def get_index_stocks(self, index_symbol: str, date: Optional[Union[str, datetime]] = None) -> List[str]:
        if date is not None:
            logger.warning("MiniQMT 的 get_index_stocks 不支持历史日期，已忽略 date=%s，仅返回最新成分股", date)
        # QMT 只返回最新权重，date 不参与缓存键，避免历史日期导致永久缓存
        kwargs = {"index_symbol": index_symbol, "date": None}

        def _fetch(kw: Dict[str, Any]) -> List[str]:
            xt = self._ensure_xtdata()
            normalized_symbol = self._normalize_security_code(kw["index_symbol"])
            if self.auto_download and hasattr(xt, "download_index_weight"):
                try:
                    xt.download_index_weight()
                except Exception:
                    pass
            if hasattr(xt, "get_index_weight"):
                data = xt.get_index_weight(normalized_symbol)
                if data:
                    if isinstance(data, dict):
                        codes = list(data.keys())
                    elif isinstance(data, (list, tuple, set)):
                        codes = list(data)
                    elif hasattr(data, "keys"):
                        try:
                            codes = list(data.keys())
                        except Exception:
                            codes = []
                    elif hasattr(data, "index"):
                        try:
                            codes = list(data.index)
                        except Exception:
                            codes = []
                    else:
                        codes = []
                    return [self._to_jq_code(code) for code in codes if code]
            return []

        return self._cache.cached_call("get_index_stocks", kwargs, _fetch, result_type="list_str")

    def get_security_info(
        self,
        security: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        xt = self._ensure_xtdata()
        _ = date
        normalized = self._normalize_security_code(security)
        detected_type = self._detect_instrument_type(xt, normalized)
        try:
            info = xt.get_instrument_detail(normalized)
        except Exception:
            info = None
        jq_code = self._to_jq_code(normalized)
        if not info or not isinstance(info, dict):
            return {
                "display_name": jq_code,
                "name": jq_code,
                "start_date": None,
                "end_date": None,
                "type": detected_type or "stock",
                "subtype": None,
                "parent": None,
                "code": jq_code,
                "qmt_code": normalized,
            }
        start = self._to_date(info.get("OpenDate"))
        end = self._to_date(info.get("ExpireDate"))
        if end is None:
            end = Date(2200, 1, 1)
        return {
            "display_name": info.get("InstrumentName") or jq_code,
            "name": info.get("InstrumentID") or jq_code.split(".", 1)[0],
            "start_date": start,
            "end_date": end,
            "type": detected_type or "stock",
            "subtype": None,
            "parent": None,
            "code": jq_code,
            "qmt_code": normalized,
        }

    # ------------------------ Live 快照 ------------------------
    def get_live_current(self, security: str) -> Dict[str, Any]:
        """
        返回实盘当前快照（最小字段）：
        - last_price, high_limit, low_limit, paused
        若 xtdata 不可用或取值失败，返回空字典由上层回退处理。
        """
        xt = self._ensure_xtdata()
        code = self._normalize_security_code(security)
        try:
            tick_map = xt.get_full_tick([code])
            t = tick_map.get(code) if isinstance(tick_map, dict) else None
            if not t or t.get('lastPrice') is None:
                return {}
            last_price = float(t.get('lastPrice'))
            info = xt.get_instrument_detail(code)
            high_limit = float(info.get('UpStopPrice') or 0.0) if isinstance(info, dict) else 0.0
            low_limit = float(info.get('DownStopPrice') or 0.0) if isinstance(info, dict) else 0.0
            paused = False
            try:
                open_int = t.get('openInt') if isinstance(t, dict) else None
                if open_int is not None:
                    # 0, 10 - 默认为未知
                        # 1 - 停牌
                        # 11 - 开盘前S
                        # 12 - 集合竞价时段C
                        # 13 - 连续交易T
                        # 14 - 休市B
                        # 15 - 闭市E
                        # 16 - 波动性中断V
                        # 17 - 临时停牌P
                        # 18 - 收盘集合竞价U
                        # 19 - 盘中集合竞价M
                        # 20 - 暂停交易至闭市N
                        # 21 - 获取字段异常
                        # 22 - 盘后固定价格行情
                        # 23 - 盘后固定价格行情完毕

                        # 状态码	含义	是否真正"停牌"？
                        # 1	停牌	✅ 是
                        # 11	开盘前S	❌ 不是停牌，只是未开盘
                        # 12	集合竞价时段C	❌ 不是停牌，可以挂单
                        # 13	连续交易T	❌ 正常交易
                        # 14	休市B	❌ 午休，不是停牌
                        # 15	闭市E	❌ 已收盘，不是停牌
                        # 16	波动性中断V	⚠️ 临时中断
                        # 17	临时停牌P	✅ 是
                        # 18	收盘集合竞价U	❌ 可以交易
                        # 19	盘中集合竞价M	❌ 可以交易
                        # 20	暂停交易至闭市N	✅ 是
                        # 22	盘后固定价格行情	❌ 可以交易
                    #paused = (int(open_int) != 13)
                    # 更准确的写法应该是：
                    paused = int(open_int) in (1, 17, 20)  # 或加上 16
            except Exception:
                pass
            return {
                'last_price': last_price,
                'high_limit': high_limit,
                'low_limit': low_limit,
                'paused': paused,
            }
        except Exception:
            return {}

    def _get_xt_split_dividend(
        self,
        security: str,
        start_date: Optional[Union[str, datetime, Date]],
        end_date: Optional[Union[str, datetime, Date]],
    ) -> List[Dict[str, Any]]:
        xt = self._ensure_xtdata()
        start_str = self._format_time(start_date, "1d")
        end_str = self._format_time(end_date, "1d")
        try:
            df = xt.get_divid_factors(
                stock_code=security,
                start_time=start_str,
                end_time=end_str,
            )
        except TypeError:
            # 老接口签名不含 keyword
            df = xt.get_divid_factors(security, start_str or "", end_str or "")
        except Exception:
            return []

        if df is None or len(df) == 0:
            return []

        df = df.copy()
        if "time" in df.columns:
            event_dt = pd.to_datetime(df["time"].astype("int64"), unit="ms", utc=True)
        else:
            event_dt = pd.to_datetime(df.index, errors="coerce", utc=True)
        # 统一转到沪深时区再取日期，避免跨日偏差
        if isinstance(event_dt, pd.Series):
            # Series 需要通过 dt 访问器完成时区转换与去除
            localized_dt = event_dt.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
            df["event_date"] = localized_dt.dt.date
        else:
            # DatetimeIndex 直接转换后再构造成 Series，便于统一使用 dt 接口
            localized_index = event_dt.tz_convert("Asia/Shanghai").tz_localize(None)
            df["event_date"] = pd.Series(localized_index, index=df.index).dt.date
        df.dropna(subset=["event_date"], inplace=True)

        start_date_obj = self._to_date(start_date)
        end_date_obj = self._to_date(end_date)
        if start_date_obj:
            df = df[df["event_date"] >= start_date_obj]
        if end_date_obj:
            df = df[df["event_date"] <= end_date_obj]

        if df.empty:
            return []

        # 判断证券类型：基金/ETF代码通常以5开头（如511880），股票为6位数字
        code_only = security.split(".")[0] if "." in security else security
        is_fund = code_only.startswith("5") and len(code_only) == 6

        events: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            # xtquant 字段：interest(现金股利)、stockGift(送股)、stockBonus(转增)、allotNum(配股)
            # 注意：QMT 返回的 interest 是"每1股"派息，需要乘以10转换为"每10股"口径
            interest_raw = float(row.get("interest", 0.0) or 0.0)
            stock_gift_raw = float(row.get("stockGift", 0.0) or 0.0)
            stock_bonus_raw = float(row.get("stockBonus", 0.0) or 0.0)
            allot_num_raw = float(row.get("allotNum", 0.0) or 0.0)
            
            if is_fund:
                # 基金/ETF：per_base=1，直接使用原始值（每1份派息）
                per_base = 1
                bonus_pre_tax = interest_raw
                scale = 1.0 + stock_gift_raw + stock_bonus_raw + allot_num_raw
            else:
                # 股票：per_base=10，需要将 QMT 的"每1股"口径乘以10转换为"每10股"
                per_base = 10
                bonus_pre_tax = interest_raw * 10.0
                # 送股、转增、配股也是"每1股"，需要乘以10
                scale = 1.0 + (stock_gift_raw + stock_bonus_raw + allot_num_raw) * 10.0 / 10.0
            
            events.append(
                {
                    "security": security,
                    "date": row["event_date"],
                    "security_type": "fund" if is_fund else "stock",
                    "scale_factor": float(scale),
                    "bonus_pre_tax": float(bonus_pre_tax),
                    "per_base": per_base,
                }
            )
        return events

    # ------------------------ 分红 / 拆分 ------------------------
    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[Union[str, datetime, Date]] = None,
        end_date: Optional[Union[str, datetime, Date]] = None,
    ) -> List[Dict[str, Any]]:
        kwargs = {"security": security, "start_date": start_date, "end_date": end_date}

        def _fetch(kw: Dict[str, Any]) -> List[Dict[str, Any]]:
            template_security = kw["security"]
            normalized_security = self._normalize_security_code(template_security)
            events = self._get_xt_split_dividend(
                normalized_security,
                start_date=kw.get("start_date"),
                end_date=kw.get("end_date"),
            )
            standardized = [self._standardize_event(event) for event in events]
            if not standardized:
                return []
            adapted: List[Dict[str, Any]] = []
            for event in standardized:
                converted = dict(event)
                converted["security"] = self._format_like_template(
                    event["security"],
                    template_security,
                )
                adapted.append(converted)
            return adapted

        return self._cache.cached_call("get_split_dividend", kwargs, _fetch, result_type="list_dict")
