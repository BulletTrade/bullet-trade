from __future__ import annotations

import os
import re
from datetime import datetime, date as Date
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from .base import DataProvider


class RQDataProvider(DataProvider):
    """基于 rqdatac 的数据提供者，内部将 RiceQuant API 转换为聚宽风格。"""

    name: str = "rqdata"

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config = config or {}
        self._username = self.config.get("username") or os.getenv("RQDATA_USERNAME") or os.getenv("RQDATA_USER", "")
        self._password = self.config.get("password") or os.getenv("RQDATA_PASSWORD") or os.getenv("RQDATA_PWD", "")
        self._license = self.config.get("license") or os.getenv("RQDATA_LICENSE", "")
        self._rq = None
        self._sdk_fallback = None

    def __getattr__(self, name: str):
        if name == "_sdk_fallback":
            return None
        return DataProvider.__getattr__(self, name)

    # ---- 工具方法 ----

    @staticmethod
    def _ensure_rq_module():
        """确保 rqdatac 已安装，未安装时抛出 ImportError。"""
        try:
            import rqdatac as rq
            return rq
        except ImportError as exc:
            raise ImportError("未安装 rqdatac，请执行 `pip install rqdatac`") from exc

    def _ensure_client(self):
        """延迟初始化并返回已认证的 rqdatac 实例。"""
        if self._rq is None:
            self.auth()
        return self._rq

    def _format_date(self, value: Optional[Union[str, datetime, Date]]) -> Optional[str]:
        """将日期统一转为 YYYYMMDD 字符串，None 透传。"""
        if value is None:
            return None
        if isinstance(value, str):
            return value if len(value) == 8 and value.isdigit() else pd.to_datetime(value).strftime("%Y%m%d")
        if isinstance(value, (datetime, Date)):
            return value.strftime("%Y%m%d")
        return None

    def _normalize_frequency(self, frequency: str) -> str:
        """聚宽频率 → RQData 频率（daily→1d, minute→1m 等）。"""
        _MAP = {"daily": "1d", "1d": "1d", "d": "1d", "minute": "1m", "1m": "1m", "m1": "1m"}
        return _MAP.get(frequency.lower(), frequency.lower())

    @staticmethod
    def _translate_fields_to_rq(fields: Optional[List[str]]) -> Optional[List[str]]:
        """聚宽字段名 → RQData 字段名，额外字段(factor/avg/paused)不传给 RQData。"""
        if not fields:
            return None
        _MAP = {"money": "total_turnover", "high_limit": "limit_up", "low_limit": "limit_down", "pre_close": "prev_close"}
        _EXTRA = {"factor", "avg", "paused"}
        result = [_MAP.get(f, f) for f in fields if f not in _EXTRA]
        return result or None

    @staticmethod
    def _is_stock(sid) -> bool:
        """判断是否为A股股票（非ETF/基金/指数）。"""
        m = re.match(r"^(\d{6})\.", str(sid))
        return bool(m and m.group(1)[0] in ("0", "3", "6"))

    # ---- 认证 ----

    def auth(self, user=None, pwd=None, host=None, port=None) -> None:
        """认证 rqdatac（license 优先，否则用户名密码）。"""
        _ = host, port
        username = user or self._username
        password = pwd or self._password
        if self._license:
            username, password = "license", self._license
        elif not username:
            raise RuntimeError("RQData 账号未配置，请设置 RQDATA_USERNAME 或 RQDATA_LICENSE")
        rq = self._ensure_rq_module()
        rq.init(username, password)
        self._rq = rq

    # ---- K 线数据 ----

    _JQ_DEFAULT_FIELDS = ["open", "close", "high", "low", "volume", "money"]
    _EXTRA_FIELDS = {"factor", "avg", "paused"}

    def get_price(self, security, start_date=None, end_date=None, frequency="daily",
                  fields=None, skip_paused=False, fq="pre", count=None, panel=True,
                  fill_paused=True, pre_factor_ref_date=None, prefer_engine=False,
                  force_no_engine=False) -> pd.DataFrame:
        """获取 K 线数据，完全兼容聚宽 get_price 接口。"""
        kwargs = {
            "security": security, "start_date": start_date, "end_date": end_date,
            "frequency": frequency, "fields": fields, "skip_paused": skip_paused,
            "fq": fq, "count": count, "panel": panel, "fill_paused": fill_paused,
            "pre_factor_ref_date": pre_factor_ref_date, "prefer_engine": prefer_engine,
            "force_no_engine": force_no_engine,
        }

        def _fetch(kw: Dict[str, Any]) -> pd.DataFrame:
            security = kw["security"]
            is_single = isinstance(security, str)
            securities = [security] if is_single else list(security)
            jq_fields = list(kw["fields"]) if kw["fields"] else list(self._JQ_DEFAULT_FIELDS)
            start_date = kw.get("start_date")
            end_date = kw.get("end_date")
            count = kw.get("count")
            freq = kw["frequency"]
            fq_val = kw.get("fq", "pre")

            if count and start_date is not None:
                raise ValueError("get_price 不能同时指定 start_date 和 count 两个参数")

            rq_freq = self._normalize_frequency(freq)
            adjust = {"pre": "pre", "post": "post"}.get(fq_val, "none")

            # count + end_date 反推 start_date
            if end_date is not None and start_date is None and count is not None:
                if "m" in rq_freq:
                    trade_days_needed = count // 240
                elif "w" in rq_freq:
                    trade_days_needed = count * 5
                else:
                    trade_days_needed = count
                days_list = self.get_trade_days(end_date=end_date, count=trade_days_needed)
                start_date = days_list[0] if days_list else end_date

            rq_api_fields = self._translate_fields_to_rq(jq_fields)

            # 分钟数据没有 limit_up/limit_down，需从日频获取
            _is_minute = rq_freq.endswith("m")
            _stripped_limit = []
            _rq_fields = list(rq_api_fields) if rq_api_fields else []
            if _is_minute and _rq_fields:
                for _f in ("limit_up", "limit_down"):
                    if _f in _rq_fields:
                        _rq_fields.remove(_f)
                        _stripped_limit.append(_f)
            df = None
            if _is_minute:
                # 获取start_date和end_date的时分转字符串后拼接成HHMM:HHMM
                start_time = start_date.strftime("%H:%M")
                end_time = end_date.strftime("%H:%M")
                time_str = (start_time,end_time)
                df = self._ensure_client().get_price(
                    order_book_ids=securities, start_date=start_date, end_date=end_date,
                    frequency=rq_freq, fields=_rq_fields or None, adjust_type=adjust,
                    skip_suspended=kw.get("skip_paused", False), expect_df=True, market="cn",
                    time_slice=time_str
                )
            else:
                df = self._ensure_client().get_price(
                    order_book_ids=securities, start_date=start_date, end_date=end_date,
                    frequency=rq_freq, fields=_rq_fields or None, adjust_type=adjust,
                    skip_suspended=kw.get("skip_paused", False), expect_df=True, market="cn"
                )

            if df is None or df.empty:
                return self._empty_price_frame(is_single, kw.get("panel", True), jq_fields)

            self._postprocess_rename(df)

            if _stripped_limit:
                df = self._merge_daily_limits(df, _stripped_limit, securities, kw.get("skip_paused", False), is_single)

            extra_fields = [f for f in jq_fields if f in self._EXTRA_FIELDS]
            self._postprocess_extra_fields(df, extra_fields, securities, start_date, end_date, freq, kw.get("skip_paused", False), fq_val)
            self._postprocess_fill_paused(df, kw.get("fill_paused", True), kw.get("skip_paused", False))

            if count:
                df = df.groupby(level="order_book_id", sort=False).tail(count) if not is_single else df.tail(count)

            if not is_single:
                return self._reshape_multi(df, securities, jq_fields, kw.get("panel", True))
            return self._reshape_single(df, securities, jq_fields, kw.get("panel", True))

        return _fetch(kwargs)

    def _postprocess_rename(self, df: pd.DataFrame) -> None:
        """RQData 列名 → 聚宽风格列名。"""
        df.rename(columns={
            "total_turnover": "money", "limit_up": "high_limit",
            "limit_down": "low_limit", "prev_close": "pre_close",
        }, errors="ignore", inplace=True)

    def _merge_daily_limits(self, df, limit_fields, securities, skip_paused, is_single):
        """分钟数据没有涨跌停价，从日频数据获取后按日期合并。"""
        if isinstance(df.index, pd.MultiIndex):
            df.index = df.index.set_names(["code", "time"])
            time_vals = df.index.get_level_values("time")
        else:
            time_vals = df.index

        dates = pd.to_datetime(time_vals).normalize().unique()
        if len(dates) == 0:
            return df

        try:
            daily_df = self._ensure_client().get_price(
                order_book_ids=securities,
                start_date=dates[0].strftime("%Y-%m-%d"),
                end_date=dates[-1].strftime("%Y-%m-%d"),
                frequency="1d", fields=limit_fields, adjust_type="none",
                skip_suspended=skip_paused, expect_df=True, market="cn",
            )
        except ValueError:
            return df

        if daily_df is None or daily_df.empty:
            return df

        self._postprocess_rename(daily_df)

        if isinstance(df.index, pd.MultiIndex):
            df_flat = df.reset_index()
            df_flat["_date"] = pd.to_datetime(df_flat["time"]).dt.normalize()
            daily_flat = daily_df.reset_index().rename(columns={"order_book_id": "code"})
            daily_flat["date"] = pd.to_datetime(daily_flat["date"])
            merged = pd.merge(df_flat, daily_flat, left_on=["code", "_date"], right_on=["code", "date"], how="left")
            merged = merged.drop(columns=["_date", "date"])
            return merged.set_index(["code", "time"])
        else:
            df_flat = df.reset_index()
            idx_col = df_flat.columns[0]
            df_flat["_date"] = pd.to_datetime(df_flat[idx_col]).dt.normalize()
            daily_flat = daily_df.droplevel(0).reset_index()
            daily_flat["date"] = pd.to_datetime(daily_flat["date"])
            merged = pd.merge(df_flat, daily_flat, left_on="_date", right_on="date", how="left")
            merged = merged.drop(columns=["_date", "date"])
            merged = merged.set_index(idx_col)
            merged.index.name = None
            return merged

    def _postprocess_extra_fields(self, df, extra_fields, securities,
                                  start_date, end_date, frequency, skip_paused, fq):
        """计算衍生字段：factor(复权因子)、avg(均价)、paused(停牌)。"""
        if not extra_fields:
            return

        for f in extra_fields:
            if f == "factor":
                raw = self._ensure_client().get_ex_factor(
                    order_book_ids=securities, start_date=start_date, end_date=end_date, market="cn",
                )
                df = pd.merge(df, raw[["code", "time", "factor"]], on=["code", "time"], how="left")
            elif f == "avg":
                freq = self._normalize_frequency(frequency)
                adjust = {"pre": "pre", "post": "post"}.get(fq, "none")
                raw = self._ensure_client().get_price(
                    order_book_ids=securities, start_date=start_date, end_date=end_date,
                    frequency=freq, fields=["money", "volume"], adjust_type=adjust,
                    skip_suspended=skip_paused, expect_df=True, market="cn",
                )
                df["avg"] = raw["money"] / raw["volume"].replace(0.0, pd.NA) if not raw.empty else pd.NA
            elif f == "paused":
                stock_ids = [s for s in securities if self._is_stock(s)]
                if stock_ids:
                    try:
                        raw = self._ensure_client().is_suspended(
                            order_book_ids=stock_ids, start_date=start_date, end_date=end_date, market="cn",
                        )
                        raw = raw.stack().reset_index(name="is_suspended").reset_index()
                        raw = raw.rename(columns={"date": "time", "order_book_id": "code"})
                        had_multi = isinstance(df.index, pd.MultiIndex)
                        idx_names = list(df.index.names) if had_multi else None
                        df = df.reset_index()
                        df = pd.merge(df, raw, on=["time", "code"], how="left")
                        df["paused"] = df["is_suspended"].fillna(False).astype(bool)
                        df.drop(columns=["is_suspended"], inplace=True, errors="ignore")
                        if had_multi and idx_names:
                            df = df.set_index(idx_names)
                    except Exception:
                        df["paused"] = False
                else:
                    df["paused"] = False
            else:
                raise ValueError(f"Unknown field: {f}")

    def _postprocess_fill_paused(self, df, fill_paused, skip_paused):
        """fill_paused=False 时，停牌行(volume=0)填 NaN。"""
        if skip_paused or fill_paused or "volume" not in df.columns:
            return
        zero = df["volume"] == 0
        if zero.any():
            df.loc[zero, df.columns.difference(["volume"])] = pd.NA

    def _reshape_multi(self, df, securities, jq_fields, panel):
        """多证券 DataFrame 重置索引，返回含 code/time 列的平铺格式。"""
        df.index = df.index.set_names(["code", "time"])
        return df.reset_index()

    def _reshape_single(self, df, security, jq_fields, panel):
        """单证券 DataFrame，提取时间为索引。"""
        if isinstance(df.index, pd.MultiIndex):
            time_level = next((n for n in df.index.names if n in ("time", "date")), None)
            if time_level:
                df = df.set_index(df.index.get_level_values(time_level))
            else:
                df = df.reset_index()
                time_col = next((c for c in ("time", "date") if c in df.columns), None)
                if time_col:
                    df = df.set_index(time_col)
        else:
            time_col = next((c for c in ("time", "date") if c in df.columns), None)
            if time_col:
                df = df.set_index(time_col)
        df.index.name = None
        return df

    def _empty_price_frame(self, is_single, panel, fields):
        """构造结构正确但内容为空的 DataFrame。"""
        if is_single:
            return pd.DataFrame(columns=fields if panel else ["time", "code"] + fields)
        if panel:
            return pd.DataFrame(columns=pd.MultiIndex.from_tuples([], names=["field", "code"]))
        return pd.DataFrame(columns=["time", "code"] + fields)

    # ---- 交易日 / 基础信息 ----

    def get_trade_days(self, start_date=None, end_date=None, count=None) -> List[datetime]:
        """获取交易日列表（datetime 对象）。"""
        kwargs = {"start_date": start_date, "end_date": end_date, "count": count}

        def _fetch(kw: Dict[str, Any]) -> List[str]:
            rq = self._ensure_client()
            s_date, e_date, cnt = kw.get("start_date"), kw.get("end_date"), kw.get("count")
            if s_date is None and e_date is None:
                return []
            if s_date is None:
                s_date = e_date if cnt == 0 else rq.get_previous_trading_date(e_date, n=cnt, market="cn")
            elif e_date is None:
                e_date = s_date if cnt == 0 else rq.get_next_trading_date(s_date, n=cnt, market="cn")
            dates = rq.get_trading_dates(start_date=s_date, end_date=e_date, market="cn")
            date_strs = [d.strftime("%Y%m%d") for d in dates]
            if cnt and cnt != -1:
                date_strs = date_strs[-cnt:]
            return date_strs

        date_strs = _fetch(kwargs)
        return [pd.to_datetime(d).to_pydatetime() for d in date_strs]

    def get_all_securities(self, types="stock", date=None) -> pd.DataFrame:
        """获取指定类型的所有证券基础信息。"""
        if isinstance(types, str):
            types = [types]
        kwargs = {"types": tuple(sorted(types)), "date": date}

        def _fetch(kw: Dict[str, Any]) -> Dict[str, Any]:
            rq = self._ensure_client()
            rows = []
            for t in kw["types"]:
                rq_type = self._map_type_to_rq(t)
                if rq_type is None:
                    continue
                df = rq.all_instruments(type=rq_type, date=kw.get("date"), market="cn")
                if df is None or df.empty:
                    continue
                df = df.copy()
                df["type"] = t
                df["display_name"] = df.get("symbol", df.get("order_book_id", ""))
                df["name"] = df.get("symbol", "")
                df["start_date"] = pd.to_datetime(df.get("listed_date"), errors="coerce")
                df["end_date"] = pd.to_datetime(df.get("de_listed_date"), errors="coerce")
                df['industry_code'] = df.get("industry_code", "")# 国民经济行业分类代码
                df['industry_name'] = df.get("industry_name", "")# 国民经济行业分类名称
                # 合约状态,'Active' - 正常上市, 'Delisted' - 终止上市, 'TemporarySuspended' - 暂停上市
                df['status'] = df.get("status", "")
                # 如果type是etf,那么需要额外获取fund_type,不是ETF的话,这列为''
                if t == "etf":
                    df["fund_type"] = df.get("fund_type", "")
                else:
                    df["fund_type"] = ""
                cols = ["order_book_id", "display_name", "name", "start_date", "end_date", "type", "fund_type", "industry_code", "industry_name", "status"]
                rows.append(df[[c for c in cols if c in df.columns]])
            if not rows:
                return {}
            merged = pd.concat(rows, ignore_index=True).drop_duplicates("order_book_id")
            merged.set_index("order_book_id", inplace=True)
            return merged.to_dict(orient="index")

        data = _fetch(kwargs)
        if not data:
            return pd.DataFrame(columns=["display_name", "name", "start_date", "end_date", "type", "fund_type", "industry_code", "industry_name", "status"])
        df = pd.DataFrame.from_dict(data, orient="index")
        df["start_date"] = pd.to_datetime(df["start_date"])
        df["end_date"] = pd.to_datetime(df["end_date"])
        return df

    @staticmethod
    def _map_type_to_rq(t: str) -> Optional[str]:
        """聚宽证券类型 → RQData instrument type。"""
        return {"stock": "CS", "index": "INDX", "etf": "ETF", "lof": "LOF", "fund": "FUND"}.get(t)

    def get_index_stocks(self, index_symbol, date=None) -> List[str]:
        """获取指数成分股列表。"""
        kwargs = {"index_symbol": index_symbol, "date": date}

        def _fetch(kw: Dict[str, Any]) -> List[str]:
            rq = self._ensure_client()
            target_date = kw.get("date") or Date.today()
            result = rq.index_components(kw["index_symbol"], date=target_date, market="cn")
            if result is None:
                return []
            if isinstance(result, tuple):
                return list(result[0]) if result[0] else []
            return list(result)

        return _fetch(kwargs)

    def get_index_weights(self, index_id, date=None) -> Any:
        """获取指数成分股权重。"""
        kwargs = {"index_id": index_id, "date": date}

        def _fetch(kw: Dict[str, Any]) -> pd.DataFrame:
            rq = self._ensure_client()
            target_date = kw.get("date") or Date.today()
            weights = rq.index_weights(kw["index_id"], date=target_date)
            if weights is None or (hasattr(weights, "empty") and weights.empty):
                return pd.DataFrame(columns=["code", "weight", "date"])
            if isinstance(weights, pd.Series):
                df = weights.reset_index()
                df.columns = ["code", "weight"]
                df["date"] = pd.to_datetime(target_date)
                return df
            return pd.DataFrame(columns=["code", "weight", "date"])

        return _fetch(kwargs)

    def get_security_info(self, security, date=None) -> Dict[str, Any]:
        """获取单个证券的详细信息。"""

        def _normalize_date(value: Any) -> Optional[Date]:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            try:
                return pd.to_datetime(value).date()
            except Exception:
                return None

        def _inst_attr(inst, attr, default=None):
            return getattr(inst, attr, default) if hasattr(inst, attr) else inst.get(attr, default)

        try:
            rq = self._ensure_client()
            instruments = rq.instruments(security, market="cn")
            if instruments is None:
                return self._default_security_info(security)
            inst = instruments[0] if isinstance(instruments, list) and instruments else instruments
            if isinstance(instruments, list) and len(instruments) == 0:
                return self._default_security_info(security)

            return {
                "display_name": _inst_attr(inst, "symbol", security),
                "name": _inst_attr(inst, "symbol", security.split(".", 1)[0]),
                "start_date": _normalize_date(_inst_attr(inst, "listed_date")),
                "end_date": _normalize_date(_inst_attr(inst, "de_listed_date")) or Date(2200, 1, 1),
                "type": self._map_rq_type_to_jq(_inst_attr(inst, "type")),
                "subtype": None,
                "parent": None,
            }
        except Exception:
            return self._default_security_info(security)

    @staticmethod
    def _default_security_info(security: str) -> Dict[str, Any]:
        """RQData 查询失败时的默认证券信息。"""
        return {
            "display_name": security, "name": security.split(".", 1)[0],
            "start_date": None, "end_date": Date(2200, 1, 1),
            "type": "stock", "subtype": None, "parent": None,
        }

    @staticmethod
    def _map_rq_type_to_jq(rq_type: Optional[str]) -> str:
        """RQData instrument type → 聚宽证券类型。"""
        mapping = {
            "CS": "stock", "INDX": "index", "ETF": "etf", "LOF": "lof",
            "FUND": "fund", "Future": "futures", "Option": "options", "Convertible": "convertible",
        }
        return mapping.get(rq_type, "stock")

    def get_trade_day(self, security, query_dt) -> Any:
        """获取指定日期最近的交易日，返回 {证券代码: date}。"""
        try:
            rq = self._ensure_client()
            query_date = pd.to_datetime(query_dt).date()
            prev_date = rq.get_previous_trading_date(query_date, n=0, market="cn") or query_date
        except Exception:
            prev_date = pd.to_datetime(query_dt).date()
        securities = list(security) if isinstance(security, (list, tuple, set)) else [security]
        return {str(sec): prev_date for sec in securities}

    # ---- Live 快照 ----

    def get_live_current(self, security: str) -> Dict[str, Any]:
        """获取证券实时快照（最新价、涨跌停价）。"""
        try:
            rq = self._ensure_client()
            df = rq.get_price(order_book_ids=security, frequency="1m", adjust_type="none", expect_df=True, market="cn")
            if df is None or df.empty:
                return {}
            row = df.sort_index().iloc[-1]
            return {
                "last_price": float(row.get("close") or 0.0),
                "high_limit": float(row.get("limit_up") or 0.0) if "limit_up" in df.columns else 0.0,
                "low_limit": float(row.get("limit_down") or 0.0) if "limit_down" in df.columns else 0.0,
                "paused": False,
            }
        except Exception:
            return {}

    # ---- 分红 / 拆分 ----

    def get_split_dividend(self, security, start_date=None, end_date=None) -> List[Dict[str, Any]]:
        """获取分红送股和拆分事件，同日事件合并。"""
        kwargs = {
            "security": security,
            "start_date": self._format_date(start_date),
            "end_date": self._format_date(end_date),
        }

        def _fetch(kw: Dict[str, Any]) -> List[Dict[str, Any]]:
            def _parse_date(value: Optional[str]) -> Optional[Date]:
                if not value:
                    return None
                try:
                    return pd.to_datetime(value).date()
                except Exception:
                    return None

            def _safe_float(value: Any) -> Optional[float]:
                try:
                    if value is None or (isinstance(value, float) and pd.isna(value)):
                        return None
                    val = float(value)
                    return val if not pd.isna(val) else None
                except (TypeError, ValueError):
                    return None

            def _in_range(check: Optional[Date]) -> bool:
                return check is not None and (not start_dt or check >= start_dt) and (not end_dt or check <= end_dt)

            rq = self._ensure_client()
            sec = kw["security"]
            start_dt = _parse_date(kw.get("start_date"))
            end_dt = _parse_date(kw.get("end_date"))
            events: List[Dict[str, Any]] = []
            seen = set()

            def _make_event(**overrides):
                base = {"security": sec, "security_type": "stock", "scale_factor": 1.0, "bonus_pre_tax": 0.0, "per_base": 10}
                base.update(overrides)
                return base

            try:
                div_df = rq.get_dividend(sec, start_date=start_dt, end_date=end_dt, expect_df=True, market="cn")
                if div_df is not None and not div_df.empty:
                    for _, row in div_df.iterrows():
                        ex_date = _parse_date(row.get("ex_dividend_date"))
                        if not _in_range(ex_date):
                            continue
                        cash = _safe_float(row.get("dividend_cash_before_tax")) or 0.0
                        round_lot = _safe_float(row.get("round_lot")) or 10.0
                        sig = (ex_date, round(cash, 6), round(round_lot, 2))
                        if sig in seen:
                            continue
                        seen.add(sig)
                        events.append(_make_event(date=ex_date, bonus_pre_tax=cash, per_base=round_lot))
            except Exception:
                pass

            try:
                split_df = rq.get_split(sec, start_date=start_dt, end_date=end_dt, market="cn")
                if split_df is not None and not split_df.empty:
                    for _, row in split_df.iterrows():
                        ex_date = _parse_date(row.get("ex_dividend_date"))
                        if not _in_range(ex_date):
                            continue
                        split_from = _safe_float(row.get("split_coefficient_from")) or 10.0
                        split_to = _safe_float(row.get("split_coefficient_to")) or 10.0
                        scale = split_to / split_from if split_from > 0 else 1.0
                        sig = (ex_date, round(scale, 6), "split")
                        if sig in seen:
                            continue
                        seen.add(sig)
                        existing = next((ev for ev in events if ev.get("date") == ex_date), None)
                        if existing:
                            existing["scale_factor"] = scale
                        else:
                            events.append(_make_event(date=ex_date, scale_factor=scale))
            except Exception:
                pass

            return events

        return _fetch(kwargs)

    # ---- 扩展数据 ----

    def get_extras(self, info, security_list, start_date=None, end_date=None, df=True, count=None) -> Any:
        """获取扩展数据（is_st / 基金净值 / 期货数据），兼容聚宽 get_extras。"""
        if end_date is None:
            end_date = Date.today()
        if count is not None:
            if start_date is not None:
                raise ValueError("count 与 start_date 不可同时使用")
            if count <= 0:
                raise ValueError("count 必须大于 0")
            days = self.get_trade_days(end_date=end_date, count=count)
            if days:
                start_date = days[0]
            else:
                return pd.DataFrame(columns=security_list) if df else {sec: np.array([]) for sec in security_list}

        _DISPATCH = {
            "acc_net_value": lambda: self._get_extras_fund_nav(info, security_list, start_date, end_date, df),
            "unit_net_value": lambda: self._get_extras_fund_nav(info, security_list, start_date, end_date, df),
            "adj_net_value": lambda: self._get_extras_fund_nav(info, security_list, start_date, end_date, df),
            "futures_sett_price": lambda: self._get_extras_futures(info, security_list, start_date, end_date, df),
            "futures_positions": lambda: self._get_extras_futures(info, security_list, start_date, end_date, df),
            "is_st": lambda: self._get_extras_is_st(security_list, start_date, end_date, df),
        }
        handler = _DISPATCH.get(info)
        if handler is None:
            raise ValueError(f"不支持的 info 类型: {info}，可选: is_st, acc_net_value, unit_net_value, futures_sett_price, futures_positions, adj_net_value")
        return handler()

    def _extras_to_dict(self, pivoted, security_list):
        """将 pivot 后的 DataFrame 转为 {code: np.array} 字典。"""
        return {sec: (pivoted[sec].dropna().values if len(pivoted[sec].dropna()) > 0 else np.array([])) for sec in security_list}

    def _get_extras_fund_nav(self, info, security_list, start_date, end_date, df) -> Any:
        """获取基金净值（累计/单位/复权净值）。"""
        field_map = {"acc_net_value": "acc_net_value", "unit_net_value": "unit_net_value", "adj_net_value": "adjusted_net_value"}
        field = field_map[info]
        fund_codes = [sec.split(".")[0] if "." in sec else sec for sec in security_list]
        code_map = dict(zip(fund_codes, security_list))
        rq = self._ensure_client()

        def _fetch(_kw=None):
            try:
                result = rq.fund.get_nav(fund_codes, start_date=start_date, end_date=end_date, fields=[field], expect_df=df, market="cn")
            except Exception as e:
                raise Exception(f"fund.get_nav 失败: {e}")
            if result is None or result.empty:
                return None
            if isinstance(result.index, pd.MultiIndex):
                result = result.reset_index()
                if "order_book_id" in result.columns and "datetime" in result.columns:
                    pivoted = result.pivot(index="datetime", columns="order_book_id", values=field)
                else:
                    pivoted = result.set_index("datetime")[[field]]
                    pivoted.columns = [security_list[0]]
            else:
                pivoted = pd.DataFrame({security_list[0]: result[field]})
            pivoted.rename(columns=code_map, inplace=True)
            for sec in security_list:
                if sec not in pivoted.columns:
                    pivoted[sec] = np.nan
            return pivoted[security_list]

        cache_key = {"info": info, "security_list": sorted(security_list), "start_date": str(start_date) if start_date else None, "end_date": str(end_date) if end_date else None}
        pivoted = _fetch(cache_key)
        if pivoted is None:
            pivoted = pd.DataFrame(columns=security_list)
        return self._extras_to_dict(pivoted, security_list) if not df else pivoted

    def _get_extras_futures(self, info, security_list, start_date, end_date, df) -> Any:
        """获取期货扩展数据（结算价/持仓量）。"""
        field = "settlement" if info == "futures_sett_price" else "open_interest"
        rq = self._ensure_client()

        def _fetch(_kw=None):
            result = rq.get_price(
                order_book_ids=security_list, start_date=start_date, end_date=end_date,
                frequency="1d", fields=[field], adjust_type="none", expect_df=True, market="cn",
            )
            if result is None or result.empty:
                return None
            if isinstance(result.index, pd.MultiIndex):
                pivoted = result[field].unstack(level="order_book_id")
            elif len(security_list) == 1:
                pivoted = pd.DataFrame({security_list[0]: result[field]})
            else:
                pivoted = result
            for sec in security_list:
                if sec not in pivoted.columns:
                    pivoted[sec] = np.nan
            return pivoted[security_list]

        cache_key = {"info": info, "security_list": sorted(security_list), "start_date": str(start_date) if start_date else None, "end_date": str(end_date) if end_date else None}
        pivoted = _fetch(cache_key)
        if pivoted is None:
            pivoted = pd.DataFrame(columns=security_list)
        return self._extras_to_dict(pivoted, security_list) if not df else pivoted

    def _get_extras_is_st(self, security_list, start_date, end_date, df) -> Any:
        """获取股票 ST 标记数据。"""
        if not security_list:
            return pd.DataFrame(dtype=bool) if df else {}
        rq = self._ensure_client()

        def _fetch(_kw=None):
            trade_days = self.get_trade_days(start_date=start_date, end_date=end_date)
            if not trade_days:
                return None
            sorted_dates = sorted(set(td.date() if hasattr(td, "date") else td for td in trade_days))
            st_map: Dict[str, Dict[Date, bool]] = {sec: {} for sec in security_list}
            prev_all_instruments = None
            for d in sorted_dates:
                try:
                    all_inst = rq.all_instruments(type="CS", date=d, market="cn")
                except Exception:
                    all_inst = None
                source = all_inst if all_inst is not None and not all_inst.empty else prev_all_instruments
                if source is not None:
                    if all_inst is not None and not all_inst.empty:
                        prev_all_instruments = all_inst
                    for _, row in source.iterrows():
                        oid = row.get("order_book_id")
                        if oid in st_map:
                            st_map[oid][d] = row.get("special_type", "Normal") in ("ST", "StarST", "PT")

            rows = [{"datetime": pd.Timestamp(d), "security": sec, "is_st": st_map.get(sec, {}).get(d, False)}
                    for sec in security_list for d in sorted_dates]
            if not rows:
                return None
            result = pd.DataFrame(rows)
            pivoted = result.pivot(index="datetime", columns="security", values="is_st")
            pivoted = pivoted.reindex(columns=security_list, fill_value=False)
            return pivoted.reindex(index=pd.DatetimeIndex(sorted_dates), fill_value=False)

        cache_key = {"info": "is_st", "security_list": sorted(security_list), "start_date": str(start_date) if start_date else None, "end_date": str(end_date) if end_date else None}
        pivoted = _fetch(cache_key)
        if pivoted is None:
            pivoted = pd.DataFrame(columns=security_list, dtype=bool)
        if not df:
            return {sec: pivoted[sec].values if sec in pivoted.columns else np.array([]) for sec in security_list}
        return pivoted

    # ---- 复权因子 ----

    def get_ex_factor(self, security_list, start_date=None, end_date=None, market="cn") -> pd.DataFrame:
        """获取复权因子 DataFrame。"""
        securities = [security_list] if isinstance(security_list, str) else list(security_list)
        kwargs = {
            "security_list": sorted(securities),
            "start_date": self._format_date(start_date),
            "end_date": self._format_date(end_date),
            "market": market,
        }

        def _fetch(kw: Dict[str, Any]) -> pd.DataFrame:
            order_book_ids = kw["security_list"]
            start_date = kw.get("start_date")
            end_date = kw.get("end_date")
            rq = self._ensure_client()

            if end_date is None:
                end_date = rq.get_previous_trading_date(datetime.now())
            end_date = pd.Timestamp(end_date)

            df_factor = rq.get_ex_factor(order_book_ids=order_book_ids).reset_index()
            for col in ["ex_date", "announcement_date", "ex_end_date"]:
                df_factor[col] = pd.to_datetime(df_factor[col], errors="coerce")
            df_factor = df_factor.sort_values(["order_book_id", "ex_date"]).reset_index(drop=True)

            if start_date is None:
                start_date = df_factor["ex_date"].min()
            start_date = pd.Timestamp(start_date)

            df_factor["ex_end_date"] = df_factor.groupby("order_book_id")["ex_end_date"].transform(lambda x: x.fillna(end_date))

            filtered_factors = []
            for _, group in df_factor.groupby("order_book_id"):
                mask = (group["ex_end_date"] >= start_date) & (group["ex_date"] <= end_date)
                valid_group = group[mask].copy()
                if len(valid_group) == 0:
                    continue
                valid_group.loc[valid_group.index[0], "ex_date"] = start_date
                valid_group.loc[valid_group.index[-1], "ex_end_date"] = end_date
                filtered_factors.append(valid_group)

            df_filtered = pd.concat(filtered_factors, ignore_index=True)
            trading_dates = pd.to_datetime(rq.get_trading_dates(start_date, end_date)).values

            stock_ids = df_filtered["order_book_id"].unique()
            df_cartesian = pd.MultiIndex.from_product([stock_ids, trading_dates], names=["order_book_id", "time"]).to_frame(index=False)

            df_daily = pd.merge_asof(
                df_cartesian.sort_values("time"),
                df_filtered[["order_book_id", "ex_date", "ex_cum_factor", "ex_factor"]].sort_values("ex_date"),
                left_on="time", right_on="ex_date", by="order_book_id", direction="backward",
            )
            df_daily = df_daily.drop("ex_date", axis=1).sort_values(["order_book_id", "time"]).reset_index(drop=True)

            latest_cum = df_factor.groupby("order_book_id")["ex_cum_factor"].last().to_dict()
            df_daily["factor"] = df_daily["ex_cum_factor"] / df_daily["order_book_id"].map(latest_cum)
            df_daily.rename(columns={"order_book_id": "code"}, inplace=True)
            return df_daily

        raw = _fetch(kwargs)
        return raw if raw is not None else pd.DataFrame()
