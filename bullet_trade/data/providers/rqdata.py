"""
RQData 数据源适配器

文件职责：通过 rqdatac SDK 提供聚宽兼容的数据源接口。
主要输入：聚宽风格证券代码、日期范围、频率、字段、复权参数。
主要输出：聚宽兼容的 DataFrame、交易日列表、证券列表、指数成分和除权除息事件。
上下游关系：由 bullet_trade.data.api 的 provider 工厂按显式名称 rqdata 创建。
关键约定：rqdatac 延迟导入，缺依赖或缺账号只影响 rqdata provider，不影响默认数据源。
"""

from __future__ import annotations

import importlib
import math
import os
from datetime import date as Date
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import pandas as pd

from .base import DataProvider


class RQDataProvider(DataProvider):
    """米筐 RQData 数据源，负责把 rqdatac 返回值转换为 BulletTrade 兼容格式。"""

    name: str = "rqdata"
    _DEFAULT_PRICE_FIELDS: List[str] = ["open", "close", "high", "low", "volume", "money"]
    _FIELD_TO_RQ: Dict[str, str] = {
        "money": "total_turnover",
        "high_limit": "limit_up",
        "low_limit": "limit_down",
        "pre_close": "prev_close",
    }
    _FIELD_FROM_RQ: Dict[str, str] = {value: key for key, value in _FIELD_TO_RQ.items()}
    _EXTRA_FIELDS = {"factor", "paused", "avg"}
    _PRICE_FIELDS = {"open", "close", "high", "low", "pre_close", "high_limit", "low_limit"}

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        """初始化 provider 配置，不在构造阶段导入或认证 rqdatac。"""
        self.config = config or {}
        self._username = (
            self.config.get("username")
            or os.getenv("RQDATA_USERNAME")
            or os.getenv("RQDATA_USER")
            or ""
        )
        self._password = (
            self.config.get("password")
            or os.getenv("RQDATA_PASSWORD")
            or os.getenv("RQDATA_PWD")
            or ""
        )
        self._license = self.config.get("license") or os.getenv("RQDATA_LICENSE") or ""
        self._rq: Optional[Any] = self.config.get("rqdatac")
        self._authenticated = bool(self.config.get("authenticated", False))

    def _ensure_rqdatac(self) -> Any:
        """返回 rqdatac 模块；缺失时抛出带安装指引的 ImportError。"""
        if self._rq is not None:
            return self._rq
        try:
            self._rq = importlib.import_module("rqdatac")
        except ImportError as exc:  # pragma: no cover - 只在真实缺依赖时触发
            raise ImportError(
                "未安装 RQData SDK，请执行 `pip install bullet-trade[rqdata]` 或 `pip install rqdatac`"
            ) from exc
        return self._rq

    def auth(
        self,
        user: Optional[str] = None,
        pwd: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        """认证 RQData；优先使用 license，其次使用用户名和密码。"""
        _ = host, port
        if self._authenticated:
            return
        rq = self._ensure_rqdatac()
        license_text = self._sanitize_str(self._license)
        username = self._sanitize_str(user or self._username)
        password = self._sanitize_str(pwd or self._password)
        if license_text:
            init = getattr(rq, "init", None)
            if not callable(init):
                raise RuntimeError("rqdatac 未提供 init 方法，无法认证 RQData")
            init(license=license_text)
            self._authenticated = True
            return
        if username and password:
            init = getattr(rq, "init", None)
            if not callable(init):
                raise RuntimeError("rqdatac 未提供 init 方法，无法认证 RQData")
            init(username, password)
            self._authenticated = True
            return
        raise RuntimeError("RQData 账号未配置，请设置 RQDATA_LICENSE 或 RQDATA_USERNAME/RQDATA_PASSWORD")

    @staticmethod
    def _sanitize_str(value: Any) -> str:
        """清理字符串配置值，去掉空白和行内注释。"""
        if value is None:
            return ""
        return str(value).split("#", 1)[0].strip()

    @staticmethod
    def _as_list(security: Union[str, Sequence[str]]) -> List[str]:
        """把单个证券或证券序列统一为字符串列表。"""
        if isinstance(security, str):
            return [security]
        if security is None:
            return []
        return [str(item) for item in security]

    @staticmethod
    def _infer_jq_security_type(security: str) -> str:
        """按聚宽后缀和常见代码段推断证券类型。"""
        code = str(security)
        code_part = code.split(".", 1)[0]
        if (code.endswith(".XSHG") and code_part.startswith("000")) or (
            code.endswith(".XSHE") and code_part.startswith("399")
        ):
            return "index"
        if code_part.startswith(("15", "16", "18", "50", "51")):
            return "fund"
        return "stock"

    @staticmethod
    def _format_date(
        value: Optional[Union[str, datetime, Date, pd.Timestamp]]
    ) -> Optional[pd.Timestamp]:
        """把日期参数标准化为 pandas Timestamp，None 原样返回。"""
        if value is None:
            return None
        parsed = pd.to_datetime(value)
        if pd.isna(parsed):
            return None
        return pd.Timestamp(parsed)

    @classmethod
    def _normalize_frequency(cls, frequency: str) -> str:
        """把聚宽风格频率转换为 rqdatac 常用频率字符串。"""
        freq = str(frequency or "daily").lower().strip()
        aliases = {
            "daily": "1d",
            "day": "1d",
            "d": "1d",
            "1day": "1d",
            "minute": "1m",
            "m1": "1m",
            "1min": "1m",
        }
        return aliases.get(freq, freq)

    @staticmethod
    def _bars_per_day(frequency: str) -> int:
        """返回指定分钟频率对应的单日 bar 数，用于 count 窗口估算。"""
        freq = str(frequency or "").lower().strip()
        mapping = {"1m": 240, "5m": 48, "15m": 16, "30m": 8, "60m": 4}
        return mapping.get(freq, 1)

    @staticmethod
    def _is_intraday(frequency: str) -> bool:
        """判断频率是否属于分钟或小时级 K 线。"""
        freq = str(frequency or "").lower()
        return freq.endswith("m") or freq in {"minute", "1min", "m1"}

    @classmethod
    def _translate_fields_to_rq(
        cls, fields: Optional[List[str]], *, frequency: str = "1d"
    ) -> Optional[List[str]]:
        """把 BulletTrade 字段映射为 rqdatac 字段，并剔除需后处理的扩展字段。"""
        if not fields:
            return None
        result: List[str] = []
        intraday = cls._is_intraday(frequency)
        for field in fields:
            if field in cls._EXTRA_FIELDS:
                continue
            if intraday and field in {"high_limit", "low_limit"}:
                continue
            mapped = cls._FIELD_TO_RQ.get(field, field)
            if mapped not in result:
                result.append(mapped)
        return result or None

    @classmethod
    def _rename_rq_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        """把 rqdatac 字段名改回 BulletTrade/聚宽字段名。"""
        if df.empty:
            return df
        return df.rename(columns=cls._FIELD_FROM_RQ)

    @staticmethod
    def _adjust_type(fq: Optional[str]) -> str:
        """把聚宽复权参数转换为 rqdatac adjust_type。"""
        if fq is None:
            return "none"
        cleaned = str(fq).lower().strip()
        if cleaned in {"", "none", "false", "raw"}:
            return "none"
        if cleaned in {"pre", "qfq"}:
            return "pre"
        if cleaned in {"post", "hfq"}:
            return "post"
        return cleaned

    def _resolve_start_for_count(
        self,
        rq: Any,
        *,
        end_date: pd.Timestamp,
        count: int,
        frequency: str,
    ) -> pd.Timestamp:
        """根据 count 和 end_date 推导安全的 start_date，分钟线使用向上取整天数。"""
        if count <= 0:
            return end_date
        freq = self._normalize_frequency(frequency)
        if self._is_intraday(freq):
            bars = self._bars_per_day(freq)
            days = max(1, int(math.ceil(float(count) / float(max(bars, 1))))) + 1
        else:
            days = max(1, int(count))
        previous = getattr(rq, "get_previous_trading_date", None)
        if callable(previous):
            try:
                return pd.Timestamp(previous(end_date, n=days, market="cn"))
            except TypeError:
                return pd.Timestamp(previous(end_date, n=days))
            except Exception:
                pass
        return end_date - pd.Timedelta(days=max(days * 2, 1))

    def _call_rq_get_price(
        self,
        rq: Any,
        *,
        securities: List[str],
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        frequency: str,
        fields: Optional[List[str]],
        skip_paused: bool,
        fq: Optional[str],
    ) -> pd.DataFrame:
        """调用 rqdatac.get_price，并转换为基础 DataFrame。"""
        rq_fields = self._translate_fields_to_rq(fields, frequency=frequency)
        if not rq_fields:
            return pd.DataFrame()
        kwargs = {
            "order_book_ids": securities if len(securities) > 1 else securities[0],
            "start_date": start_date,
            "end_date": end_date,
            "frequency": frequency,
            "fields": rq_fields,
            "adjust_type": self._adjust_type(fq),
            "skip_suspended": bool(skip_paused),
            "expect_df": True,
            "market": "cn",
        }
        raw = rq.get_price(**kwargs)
        if raw is None:
            return pd.DataFrame()
        df = pd.DataFrame(raw).copy()
        if df.empty:
            return df
        return self._rename_rq_columns(self._normalize_price_index(df))

    @staticmethod
    def _normalize_price_index(df: pd.DataFrame) -> pd.DataFrame:
        """把 rqdatac 常见索引和列名统一为 MultiIndex(code,time)。"""
        result = df.copy()
        if isinstance(result.index, pd.MultiIndex):
            names = list(result.index.names)
            code_name = "order_book_id" if "order_book_id" in names else names[0]
            time_name = (
                "datetime" if "datetime" in names else ("date" if "date" in names else names[-1])
            )
            result = result.reset_index().rename(columns={code_name: "code", time_name: "time"})
        else:
            result = result.reset_index()
            if "order_book_id" in result.columns:
                result.rename(columns={"order_book_id": "code"}, inplace=True)
            if "datetime" in result.columns:
                result.rename(columns={"datetime": "time"}, inplace=True)
            elif "date" in result.columns:
                result.rename(columns={"date": "time"}, inplace=True)
            elif "index" in result.columns:
                result.rename(columns={"index": "time"}, inplace=True)
        if "code" not in result.columns:
            result["code"] = ""
        if "time" not in result.columns:
            result["time"] = pd.NaT
        result["code"] = result["code"].astype(str)
        result["time"] = pd.to_datetime(result["time"], errors="coerce")
        result = result.dropna(subset=["time"])
        result.set_index(["code", "time"], inplace=True)
        result.sort_index(inplace=True)
        return result

    def _date_index_for_factor(
        self,
        rq: Any,
        *,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        base_index: Optional[pd.MultiIndex],
    ) -> List[pd.Timestamp]:
        """为 factor/paused 后处理构造交易日索引。"""
        if base_index is not None and len(base_index) > 0:
            return sorted({pd.Timestamp(item[1]).normalize() for item in base_index})
        getter = getattr(rq, "get_trading_dates", None)
        if callable(getter):
            try:
                days = getter(start_date, end_date, market="cn")
            except TypeError:
                days = getter(start_date, end_date)
            return [pd.Timestamp(day).normalize() for day in days]
        return [pd.Timestamp(day).normalize() for day in pd.bdate_range(start_date, end_date)]

    def _fetch_factor_long(
        self,
        rq: Any,
        *,
        securities: List[str],
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        base_index: Optional[pd.MultiIndex],
    ) -> pd.DataFrame:
        """获取并展开 RQData 复权因子，返回 MultiIndex(code,time) 的 factor 表。"""
        dates = self._date_index_for_factor(
            rq, start_date=start_date, end_date=end_date, base_index=base_index
        )
        if not dates:
            return pd.DataFrame()
        factor_by_code: Dict[str, pd.Series] = {}
        getter = getattr(rq, "get_ex_factor", None)
        if callable(getter):
            try:
                raw = getter(
                    securities if len(securities) > 1 else securities[0],
                    start_date=None,
                    end_date=end_date,
                    market="cn",
                )
            except TypeError:
                raw = getter(
                    securities if len(securities) > 1 else securities[0],
                    start_date=None,
                    end_date=end_date,
                )
            events = self._normalize_factor_events(raw)
        else:
            events = pd.DataFrame()
        for code in securities:
            base = pd.Series(1.0, index=pd.DatetimeIndex(dates), dtype="float64")
            if not events.empty:
                subset = events[events["code"].astype(str) == str(code)].copy()
                if not subset.empty:
                    subset.sort_values("time", inplace=True)
                    for _, row in subset.iterrows():
                        factor = row.get("factor")
                        if pd.isna(factor):
                            continue
                        base.loc[base.index >= pd.Timestamp(row["time"]).normalize()] = float(
                            factor
                        )
            factor_by_code[code] = base.ffill().bfill().fillna(1.0)
        rows = []
        for code, series in factor_by_code.items():
            for ts, value in series.items():
                rows.append({"code": code, "time": ts, "factor": float(value)})
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).set_index(["code", "time"]).sort_index()

    @staticmethod
    def _normalize_factor_events(raw: Any) -> pd.DataFrame:
        """把 rqdatac.get_ex_factor 返回值整理为 code/time/factor 三列。"""
        if raw is None:
            return pd.DataFrame(columns=["code", "time", "factor"])
        df = pd.DataFrame(raw).copy()
        if df.empty:
            return pd.DataFrame(columns=["code", "time", "factor"])
        if "order_book_id" not in df.columns:
            if isinstance(df.index, pd.MultiIndex):
                df = df.reset_index()
            else:
                df = df.reset_index().rename(columns={"index": "order_book_id"})
        time_col = (
            "ex_date" if "ex_date" in df.columns else ("date" if "date" in df.columns else None)
        )
        if time_col is None:
            return pd.DataFrame(columns=["code", "time", "factor"])
        factor_col = (
            "ex_cum_factor"
            if "ex_cum_factor" in df.columns
            else ("factor" if "factor" in df.columns else "ex_factor")
        )
        df = df.rename(columns={"order_book_id": "code", time_col: "time", factor_col: "factor"})
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df["factor"] = pd.to_numeric(df["factor"], errors="coerce")
        df = df.dropna(subset=["time"])
        return df[["code", "time", "factor"]]

    def _fetch_paused_long(
        self,
        rq: Any,
        *,
        securities: List[str],
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        base_index: Optional[pd.MultiIndex],
    ) -> pd.DataFrame:
        """获取停牌状态并整理为 MultiIndex(code,time) 的 paused 表。"""
        dates = self._date_index_for_factor(
            rq, start_date=start_date, end_date=end_date, base_index=base_index
        )
        if not dates:
            return pd.DataFrame()
        rows = []
        getter = getattr(rq, "is_suspended", None)
        suspended = None
        if callable(getter):
            try:
                suspended = getter(
                    securities, start_date=start_date, end_date=end_date, market="cn"
                )
            except TypeError:
                suspended = getter(securities, start_date=start_date, end_date=end_date)
        for code in securities:
            for ts in dates:
                rows.append(
                    {
                        "code": code,
                        "time": pd.Timestamp(ts).normalize(),
                        "paused": self._lookup_paused(suspended, code, ts),
                    }
                )
        return pd.DataFrame(rows).set_index(["code", "time"]).sort_index()

    @staticmethod
    def _lookup_paused(raw: Any, code: str, ts: pd.Timestamp) -> bool:
        """从 rqdatac 停牌返回值中读取单个证券单日状态。"""
        if raw is None:
            return False
        try:
            df = pd.DataFrame(raw)
            day = pd.Timestamp(ts).normalize()
            if code in df.index and day in pd.to_datetime(df.columns):
                return bool(df.loc[code, df.columns[pd.to_datetime(df.columns).get_loc(day)]])
            if day in df.index and code in df.columns:
                return bool(df.loc[day, code])
        except Exception:
            return False
        return False

    def _merge_extra_fields(
        self,
        rq: Any,
        df: pd.DataFrame,
        *,
        requested_fields: List[str],
        securities: List[str],
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        frequency: str,
    ) -> pd.DataFrame:
        """按请求补充 factor、paused、avg 和分钟涨跌停字段。"""
        result = df.copy()
        base_index = result.index if isinstance(result.index, pd.MultiIndex) else None
        if result.empty:
            result = self._build_empty_base_index(
                rq,
                securities=securities,
                start_date=start_date,
                end_date=end_date,
            )
            base_index = result.index
        if "factor" in requested_fields:
            factor = self._fetch_factor_long(
                rq,
                securities=securities,
                start_date=start_date,
                end_date=end_date,
                base_index=base_index,
            )
            result = result.join(factor, how="left")
        if "paused" in requested_fields:
            paused = self._fetch_paused_long(
                rq,
                securities=securities,
                start_date=start_date,
                end_date=end_date,
                base_index=base_index,
            )
            result = result.join(paused, how="left")
            result["paused"] = result["paused"].fillna(False).astype(bool)
        if "avg" in requested_fields:
            if "avg" not in result.columns:
                money = pd.to_numeric(result.get("money", 0.0), errors="coerce")
                volume = pd.to_numeric(result.get("volume", 0.0), errors="coerce")
                result["avg"] = money.where(volume == 0, money / volume)
        if self._is_intraday(frequency) and {"high_limit", "low_limit"} & set(requested_fields):
            limit_df = self._fetch_daily_limits(
                rq,
                securities=securities,
                start_date=start_date,
                end_date=end_date,
            )
            result = self._merge_daily_limits(result, limit_df)
        return result

    def _build_empty_base_index(
        self,
        rq: Any,
        *,
        securities: List[str],
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
    ) -> pd.DataFrame:
        """在只请求扩展字段时构造 code/time 基础索引。"""
        dates = self._date_index_for_factor(
            rq, start_date=start_date, end_date=end_date, base_index=None
        )
        idx = pd.MultiIndex.from_product([securities, dates], names=["code", "time"])
        return pd.DataFrame(index=idx)

    def _fetch_daily_limits(
        self,
        rq: Any,
        *,
        securities: List[str],
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
    ) -> pd.DataFrame:
        """拉取日线涨跌停，用于分钟线返回值合并。"""
        try:
            return self._call_rq_get_price(
                rq,
                securities=securities,
                start_date=start_date.normalize(),
                end_date=end_date.normalize(),
                frequency="1d",
                fields=["high_limit", "low_limit"],
                skip_paused=False,
                fq="none",
            )
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def _merge_daily_limits(price_df: pd.DataFrame, limit_df: pd.DataFrame) -> pd.DataFrame:
        """把日线 high_limit/low_limit 按日期合并到分钟线 DataFrame。"""
        if price_df.empty or limit_df.empty:
            return price_df
        result = price_df.copy()
        working = result.reset_index()
        limits = limit_df.reset_index()
        working["_date"] = pd.to_datetime(working["time"]).dt.normalize()
        limits["_date"] = pd.to_datetime(limits["time"]).dt.normalize()
        merged = working.merge(
            limits[["code", "_date", "high_limit", "low_limit"]],
            on=["code", "_date"],
            how="left",
            suffixes=("", "_daily"),
        )
        for field in ("high_limit", "low_limit"):
            daily_col = f"{field}_daily"
            if daily_col in merged.columns:
                if field in merged.columns:
                    merged[field] = merged[field].where(merged[field].notna(), merged[daily_col])
                else:
                    merged[field] = merged[daily_col]
        drop_cols = [
            col for col in ("_date", "high_limit_daily", "low_limit_daily") if col in merged.columns
        ]
        merged.drop(columns=drop_cols, inplace=True)
        merged.set_index(["code", "time"], inplace=True)
        merged.sort_index(inplace=True)
        return merged

    def _apply_manual_pre_ref(
        self,
        rq: Any,
        df: pd.DataFrame,
        *,
        securities: List[str],
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        pre_factor_ref_date: Union[str, datetime, Date, pd.Timestamp],
    ) -> pd.DataFrame:
        """用 factor 把未复权价格锚定到指定前复权参考日。"""
        if df.empty:
            return df
        ref_ts = self._format_date(pre_factor_ref_date) or end_date
        factor = self._fetch_factor_long(
            rq,
            securities=securities,
            start_date=start_date,
            end_date=max(end_date, ref_ts),
            base_index=None,
        )
        if factor.empty:
            return df
        result = df.join(factor, how="left")
        result["factor"] = result.groupby(level=0)["factor"].ffill().bfill().fillna(1.0)
        for code in securities:
            try:
                series = factor.xs(code, level=0)["factor"]
                ref_values = series[series.index <= ref_ts]
                factor_ref = (
                    float(ref_values.iloc[-1]) if not ref_values.empty else float(series.iloc[-1])
                )
            except Exception:
                factor_ref = 1.0
            if not factor_ref:
                factor_ref = 1.0
            mask = result.index.get_level_values(0) == code
            ratio = pd.to_numeric(result.loc[mask, "factor"], errors="coerce") / factor_ref
            for field in self._PRICE_FIELDS:
                if field in result.columns:
                    result.loc[mask, field] = (
                        pd.to_numeric(result.loc[mask, field], errors="coerce") * ratio.values
                    )
        result.drop(columns=["factor"], inplace=True, errors="ignore")
        return result

    def _finalize_price_result(
        self,
        df: pd.DataFrame,
        *,
        requested_fields: List[str],
        securities: List[str],
        count: Optional[int],
        panel: bool,
    ) -> pd.DataFrame:
        """按字段、count 和 panel 参数组装最终 get_price 返回值。"""
        if df.empty:
            return pd.DataFrame()
        result = df.copy()
        result = result.sort_index()
        if count is not None:
            result = result.groupby(level=0, group_keys=False).tail(int(count))
        for field in requested_fields:
            if field not in result.columns:
                default = False if field == "paused" else 0.0
                result[field] = default
        result = result[requested_fields]
        if len(securities) == 1 and panel:
            single = result.xs(securities[0], level=0, drop_level=True)
            single.index = pd.to_datetime(single.index)
            return single
        if panel:
            parts = []
            for field in requested_fields:
                pivot = result[field].unstack(level=0)
                pivot = pivot.reindex(columns=securities)
                parts.append(pivot)
            wide = pd.concat(parts, axis=1, keys=requested_fields)
            wide.columns.names = ["field", "code"]
            wide.index = pd.to_datetime(wide.index)
            return wide
        long_df = result.reset_index()
        long_df.rename(columns={"level_0": "code", "level_1": "time"}, inplace=True)
        long_df["time"] = pd.to_datetime(long_df["time"], errors="coerce")
        long_df = long_df[["time", "code"] + requested_fields]
        return long_df.sort_values(["time", "code"]).reset_index(drop=True)

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
        """获取历史 K 线，并返回聚宽兼容 DataFrame。"""
        _ = fill_paused, prefer_engine, force_no_engine
        securities = self._as_list(security)
        if not securities:
            return pd.DataFrame()
        if count is not None and start_date is not None:
            raise ValueError("get_price 不能同时指定 start_date 和 count")
        rq = self._ensure_rqdatac()
        req_fields = list(fields or self._DEFAULT_PRICE_FIELDS)
        freq = self._normalize_frequency(frequency)
        end_ts = self._format_date(end_date) or pd.Timestamp.now()
        start_ts = self._format_date(start_date)
        if count is not None and start_ts is None:
            start_ts = self._resolve_start_for_count(
                rq, end_date=end_ts, count=int(count), frequency=freq
            )
        if start_ts is None:
            start_ts = end_ts
        fetch_fields = list(req_fields)
        if "avg" in fetch_fields:
            for base_field in ("money", "volume"):
                if base_field not in fetch_fields:
                    fetch_fields.append(base_field)
        manual_ref = fq == "pre" and pre_factor_ref_date is not None
        base_fq = "none" if manual_ref else fq
        base = self._call_rq_get_price(
            rq,
            securities=securities,
            start_date=start_ts,
            end_date=end_ts,
            frequency=freq,
            fields=fetch_fields,
            skip_paused=skip_paused,
            fq=base_fq,
        )
        if manual_ref:
            base = self._apply_manual_pre_ref(
                rq,
                base,
                securities=securities,
                start_date=start_ts,
                end_date=end_ts,
                pre_factor_ref_date=pre_factor_ref_date,
            )
        enriched = self._merge_extra_fields(
            rq,
            base,
            requested_fields=req_fields,
            securities=securities,
            start_date=start_ts,
            end_date=end_ts,
            frequency=freq,
        )
        return self._finalize_price_result(
            enriched,
            requested_fields=req_fields,
            securities=securities,
            count=count,
            panel=panel,
        )

    def get_trade_days(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        count: Optional[int] = None,
    ) -> List[datetime]:
        """获取交易日列表，按 count 做尾部裁剪。"""
        rq = self._ensure_rqdatac()
        end_ts = self._format_date(end_date) or pd.Timestamp.today()
        start_ts = self._format_date(start_date)
        if start_ts is None and count:
            start_ts = self._resolve_start_for_count(
                rq, end_date=end_ts, count=int(count), frequency="1d"
            )
        if start_ts is None:
            return []
        getter = getattr(rq, "get_trading_dates")
        try:
            raw_days = getter(start_ts, end_ts, market="cn")
        except TypeError:
            raw_days = getter(start_ts, end_ts)
        days = [pd.Timestamp(day).to_pydatetime() for day in raw_days]
        if count:
            days = days[-int(count) :]
        return days

    def get_all_securities(
        self,
        types: Union[str, List[str]] = "stock",
        date: Optional[Union[str, datetime]] = None,
    ) -> pd.DataFrame:
        """获取证券列表，支持股票、基金和指数的基础映射。"""
        rq = self._ensure_rqdatac()
        type_list = [types] if isinstance(types, str) else list(types or [])
        type_map = {"stock": "CS", "fund": "Fund", "etf": "Fund", "index": "INDX"}
        rows = []
        target_date = self._format_date(date)
        for item in type_list:
            rq_type = type_map.get(str(item).lower(), str(item))
            all_instruments = getattr(rq, "all_instruments", None)
            if not callable(all_instruments):
                continue
            try:
                df = all_instruments(type=rq_type, date=target_date, market="cn")
            except TypeError:
                df = all_instruments(type=rq_type, date=target_date)
            if df is None or pd.DataFrame(df).empty:
                continue
            part = pd.DataFrame(df).copy()
            code_col = "order_book_id" if "order_book_id" in part.columns else "symbol"
            name_col = "symbol" if "symbol" in part.columns else "abbrev_symbol"
            for _, row in part.iterrows():
                code = str(row.get(code_col, ""))
                if not code:
                    continue
                rows.append(
                    {
                        "code": code,
                        "display_name": row.get("special_type") or row.get(name_col) or code,
                        "name": row.get(name_col) or code,
                        "start_date": row.get("listed_date"),
                        "end_date": row.get("de_listed_date") or Date(2200, 1, 1),
                        "type": str(item).lower(),
                    }
                )
        if not rows:
            return pd.DataFrame(columns=["display_name", "name", "start_date", "end_date", "type"])
        result = pd.DataFrame(rows).drop_duplicates(subset=["code"]).set_index("code")
        for field in ("start_date", "end_date"):
            result[field] = pd.to_datetime(result[field], errors="coerce").dt.date
        return result

    def get_index_stocks(
        self,
        index_symbol: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> List[str]:
        """获取指数成分股；SDK 不支持时返回空列表。"""
        rq = self._ensure_rqdatac()
        target = self._format_date(date)
        getter = getattr(rq, "index_components", None) or getattr(rq, "index_weights", None)
        if not callable(getter):
            return []
        try:
            raw = getter(index_symbol, date=target, market="cn")
        except TypeError:
            raw = getter(index_symbol, date=target)
        if raw is None:
            return []
        if isinstance(raw, pd.Series):
            return [str(item) for item in raw.index.tolist()]
        df = pd.DataFrame(raw)
        if "order_book_id" in df.columns:
            return [str(item) for item in df["order_book_id"].tolist()]
        return [str(item) for item in list(raw)]

    def get_index_weights(
        self,
        index_id: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> Any:
        """获取指数权重，直接返回 rqdatac 结构化结果。"""
        rq = self._ensure_rqdatac()
        getter = getattr(rq, "index_weights", None)
        if not callable(getter):
            raise NotImplementedError("RQData SDK 未提供 index_weights")
        target = self._format_date(date)
        try:
            return getter(index_id, date=target, market="cn")
        except TypeError:
            return getter(index_id, date=target)

    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> List[Dict[str, Any]]:
        """获取分红拆股事件；SDK 不支持时返回空列表。"""
        rq = self._ensure_rqdatac()
        getter = getattr(rq, "get_dividend", None) or getattr(rq, "get_ex_factor", None)
        if not callable(getter):
            return []
        start_ts = self._format_date(start_date)
        end_ts = self._format_date(end_date) or pd.Timestamp.today()
        try:
            raw = getter(security, start_date=start_ts, end_date=end_ts, market="cn")
        except TypeError:
            raw = getter(security, start_date=start_ts, end_date=end_ts)
        df = pd.DataFrame(raw)
        if df.empty:
            return []
        if "order_book_id" not in df.columns:
            df = df.reset_index().rename(columns={"index": "order_book_id"})
        result = []
        for _, row in df.iterrows():
            event_date = row.get("ex_date") or row.get("date")
            if pd.isna(event_date):
                continue
            result.append(
                {
                    "security": security,
                    "date": pd.Timestamp(event_date).date(),
                    "bonus_pre_tax": float(row.get("cash_before_tax", 0.0) or 0.0),
                    "scale_factor": float(row.get("ex_factor", 1.0) or 1.0),
                    "per_base": 10.0,
                }
            )
        return result

    def get_security_info(
        self,
        security: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        """获取单个证券基础信息；缺少 catalog 时返回最小可用信息。"""
        securities = self.get_all_securities(types=["stock", "fund", "index"], date=date)
        if not securities.empty and security in securities.index:
            row = securities.loc[security].to_dict()
            row["code"] = security
            return row
        code = str(security)
        kind = self._infer_jq_security_type(code)
        return {"code": security, "display_name": security, "name": security, "type": kind}

    def get_live_current(self, security: str) -> Dict[str, Any]:
        """获取当前行情快照，供 LiveCurrentData 使用。"""
        try:
            minute = self.get_price(
                security,
                end_date=pd.Timestamp.now(),
                frequency="1m",
                fields=["close"],
                count=1,
                fq="none",
                panel=True,
            )
            if minute.empty:
                return {}
            daily = self.get_price(
                security,
                end_date=pd.Timestamp.now(),
                frequency="1d",
                fields=["high_limit", "low_limit", "paused"],
                count=1,
                fq="none",
                panel=True,
            )
            row = minute.iloc[-1]
            limit_row = daily.iloc[-1] if not daily.empty else {}
            return {
                "last_price": float(row.get("close", 0.0) or 0.0),
                "high_limit": float(limit_row.get("high_limit", 0.0) or 0.0),
                "low_limit": float(limit_row.get("low_limit", 0.0) or 0.0),
                "paused": bool(limit_row.get("paused", False)),
            }
        except Exception:
            return {}
