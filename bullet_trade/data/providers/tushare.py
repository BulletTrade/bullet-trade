from __future__ import annotations

import os
from datetime import datetime, date as Date
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from .base import DataProvider
from ..cache import CacheManager


class TushareProvider(DataProvider):
    """基于 tushare.pro 的数据提供者，字段与复权口径对齐兼容层约定。"""

    name: str = "tushare"
    _TS_SUFFIX_TO_JQ = {"SH": "XSHG", "SZ": "XSHE"}
    _JQ_SUFFIX_TO_TS = {"XSHG": "SH", "XSHE": "SZ"}

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config = config or {}
        self._token = self.config.get("token") or os.getenv("TUSHARE_TOKEN")
        self._tushare_custom_url = self.config.get("tushare_custom_url") or os.getenv("TUSHARE_CUSTOM_URL")
        cache_dir_set = "cache_dir" in self.config
        cache_dir = self.config.get("cache_dir")
        self._cache = CacheManager(
            provider_name=self.name,
            cache_dir=cache_dir,
            fallback_to_env=not cache_dir_set,
        )
        self._pro = None
        self._asset_type_cache: Dict[str, str] = {}

    # ------------------------ 公共工具 ------------------------
    @classmethod
    def _to_ts_code(cls, security: str) -> str:
        if not security or not isinstance(security, str) or "." not in security:
            return security
        code, suffix = security.split(".", 1)
        mapped = cls._JQ_SUFFIX_TO_TS.get(suffix.upper())
        if mapped:
            return f"{code}.{mapped}"
        return security

    @classmethod
    def _to_jq_code(cls, security: str) -> str:
        if not security or not isinstance(security, str) or "." not in security:
            return security
        code, suffix = security.split(".", 1)
        mapped = cls._TS_SUFFIX_TO_JQ.get(suffix.upper())
        if mapped:
            return f"{code}.{mapped}"
        return security

    @staticmethod
    def _ensure_ts_module():
        try:
            import tushare as ts  # type: ignore

            return ts
        except ImportError as exc:  # pragma: no cover - 仅在缺失依赖时触发
            raise ImportError(
                "未安装 tushare，请执行 `pip install bullet-trade[tushare]` 或 `pip install tushare`"
            ) from exc

    def _ensure_client(self):
        if self._pro is None:
            self.auth()
        return self._pro

    def _format_date(self, value: Optional[Union[str, datetime, Date]]) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            if len(value) == 8 and value.isdigit():
                return value
            return pd.to_datetime(value).strftime("%Y%m%d")
        if isinstance(value, datetime):
            return value.strftime("%Y%m%d")
        if isinstance(value, Date):
            return value.strftime("%Y%m%d")
        return None

    def _normalize_frequency(self, frequency: str) -> str:
        freq = frequency.lower()
        if freq in ("daily", "1d", "d"):
            return "D"
        if freq in ("minute", "1m", "m1"):
            return "1min"
        if freq.endswith("m"):
            return f"{freq}"
        return freq.upper()

    def _apply_fields(self, df: pd.DataFrame, fields: Optional[List[str]]) -> pd.DataFrame:
        if fields:
            missing = [f for f in fields if f not in df.columns]
            if missing:
                extra_cols = {f: 0.0 for f in missing}
                df = df.assign(**extra_cols)
            df = df[fields]
        return df

    @classmethod
    def _infer_asset_by_code(cls, security: str) -> Optional[str]:
        jq_code = cls._to_jq_code(security)
        if not jq_code or "." not in jq_code:
            return None

        code, suffix = jq_code.split(".", 1)
        suffix = suffix.upper()

        if suffix == "XSHG":
            if code.startswith("000"):
                return "I"
            if code.startswith("5"):
                return "FD"
            if code.startswith("6"):
                return "E"
        elif suffix == "XSHE":
            if code.startswith("399"):
                return "I"
            if code.startswith(("15", "16", "18")):
                return "FD"
            if code.startswith(("000", "001", "002", "003", "300", "301")):
                return "E"

        return None

    def _infer_asset_from_catalog(self, jq_code: str) -> Optional[str]:
        for types, asset in (
            (["index"], "I"),
            (["fund", "etf", "lof"], "FD"),
            (["stock"], "E"),
        ):
            try:
                df = self.get_all_securities(types=types)
            except Exception:
                continue
            if df is not None and not df.empty and jq_code in df.index:
                return asset
        return None

    def _infer_asset(self, security: str) -> str:
        jq_code = self._to_jq_code(security)
        cache_key = jq_code.upper() if isinstance(jq_code, str) else str(jq_code)
        cached = self._asset_type_cache.get(cache_key)
        if cached:
            return cached

        asset = self._infer_asset_by_code(jq_code)
        if asset is None:
            asset = self._infer_asset_from_catalog(jq_code)
        if asset is None:
            asset = "E"

        self._asset_type_cache[cache_key] = asset
        return asset

    # ------------------------ 认证 ------------------------
    def auth(
        self,
        user: Optional[str] = None,
        pwd: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        _ = port, pwd  # tushare 不使用这些字段
        token = user or self._token
        if not token:
            raise RuntimeError("Tushare token 未配置，请设置 TUSHARE_TOKEN 或在 auth 中手动传入")
        ts = self._ensure_ts_module()
        self._pro = ts.pro_api(token)
        self._token = token
        # 支持自定义 API URL
        tushare_custom_url = host or self._tushare_custom_url
        if tushare_custom_url:
            self._pro._DataApi__http_url = tushare_custom_url
            print("使用自定义的URL")
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

        for sec in securities:
            asset = self._infer_asset(sec)
            kwargs = {
                "security": sec,
                "start_date": start_date,
                "end_date": end_date,
                "frequency": frequency,
                "fields": fields,
                "skip_paused": skip_paused,
                "fq": fq,
                "count": count,
                "pre_factor_ref_date": pre_factor_ref_date,
                "asset": asset,
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
                    asset=kw.get("asset"),
                )

            frames[sec] = self._cache.cached_call("get_price", kwargs, _fetch_single, result_type="df")

        if len(frames) == 1:
            return next(iter(frames.values()))

        if panel:
            return pd.concat(frames, axis=1)

        long_rows = []
        for sec, df in frames.items():
            tmp = df.copy()
            tmp["code"] = sec
            long_rows.append(tmp)
        merged = pd.concat(long_rows, axis=0)
        return merged

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
        asset: Optional[str] = None,
    ) -> pd.DataFrame:
        start_str = self._format_date(start_date)
        end_str = self._format_date(end_date)
        freq = self._normalize_frequency(frequency)
        ts = self._ensure_ts_module()
        pro = self._ensure_client()
        asset = asset or self._infer_asset(security)
        ts_code = self._to_ts_code(security)

        df = ts.pro_bar(
            ts_code=ts_code,
            start_date=start_str,
            end_date=end_str,
            freq=freq,
            adj=None,
            asset=asset,
            api=pro,
        )
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.sort_values("trade_date")
        df.index = pd.to_datetime(df["trade_date"])
        df.rename(
            columns={
                "vol": "volume",
                "amount": "money",
                "high_limit": "high_limit",
                "low_limit": "low_limit",
            },
            inplace=True,
        )
        if "ts_code" in df.columns:
            df["ts_code"] = df["ts_code"].apply(self._to_jq_code)
        df["money"] = df.get("money", 0.0)
        df["volume"] = df.get("volume", 0.0)

        if skip_paused and "is_paused" in df.columns:
            df = df[df["is_paused"] == 0]

        if asset == "E" and fq in ("pre", "post"):
            df = self._apply_adjustment(
                security=security,
                df=df,
                fq=fq,
                pre_factor_ref_date=pre_factor_ref_date,
            )

        if count:
            df = df.tail(count)

        df = self._apply_fields(df, fields)
        return df

    def _apply_adjustment(
        self,
        security: str,
        df: pd.DataFrame,
        fq: str,
        pre_factor_ref_date: Optional[Union[str, datetime]],
    ) -> pd.DataFrame:
        start_dt = df.index.min()
        end_dt = df.index.max()
        factor_df = self._fetch_adj_factor(security, start_dt, end_dt)
        if factor_df.empty or "adj_factor" not in factor_df.columns:
            fallback = self._build_adjusted_from_events(
                security=security,
                raw_df=df,
                fq=fq,
                pre_factor_ref_date=pre_factor_ref_date,
            )
            return fallback if not fallback.empty else df

        factor_df.index = pd.to_datetime(factor_df["trade_date"]).dt.normalize()
        merged = df.join(factor_df["adj_factor"], how="left")
        merged["adj_factor"] = merged["adj_factor"].ffill().bfill()
        ref_date = pre_factor_ref_date
        if ref_date is None and fq == "pre":
            latest_trade_day = self._latest_trade_day()
            ref_date = latest_trade_day or Date.today()
        if ref_date is None:
            ref_date = end_dt if fq == "pre" else start_dt
        try:
            ref_dt = pd.to_datetime(ref_date).normalize()
        except Exception:
            ref_dt = pd.to_datetime(end_dt if fq == "pre" else start_dt).normalize()
        ref_factor = None
        if ref_dt is not None:
            if ref_dt in factor_df.index:
                ref_factor = factor_df.loc[ref_dt, "adj_factor"]
            else:
                extra_df = self._fetch_adj_factor(security, ref_dt, ref_dt)
                if not extra_df.empty and "adj_factor" in extra_df.columns:
                    ref_factor = extra_df["adj_factor"].iloc[-1]
        if ref_factor is None or (isinstance(ref_factor, float) and pd.isna(ref_factor)):
            ref_factor = merged["adj_factor"].iloc[-1] if fq == "pre" else merged["adj_factor"].iloc[0]
        if fq == "pre":
            ratio = merged["adj_factor"] / ref_factor
        else:
            ratio = ref_factor / merged["adj_factor"]

        for col in ["open", "high", "low", "close"]:
            if col in merged.columns:
                merged[col] = merged[col] * ratio

        merged.drop(columns=["adj_factor"], inplace=True, errors="ignore")
        return merged

    def _build_adjusted_from_events(
        self,
        security: str,
        raw_df: pd.DataFrame,
        fq: str,
        pre_factor_ref_date: Optional[Union[str, datetime]],
    ) -> pd.DataFrame:
        if fq not in ("pre", "post"):
            return pd.DataFrame()
        if raw_df.empty:
            return pd.DataFrame()
        if fq == "post":
            return pd.DataFrame()

        def _to_date(value: Optional[Union[str, datetime, Date]]) -> Optional[Date]:
            if value is None:
                return None
            try:
                return pd.to_datetime(value).date()
            except Exception:
                return None

        start_dt = raw_df.index.min()
        end_dt = raw_df.index.max()
        ref_date: Optional[Union[str, datetime, Date]] = None
        if fq == "pre":
            if pre_factor_ref_date is not None:
                ref_date = pre_factor_ref_date
            else:
                latest_trade_day = self._latest_trade_day()
                ref_date = latest_trade_day.date() if isinstance(latest_trade_day, datetime) else latest_trade_day
                if ref_date is None:
                    ref_date = Date.today()

        start_date = _to_date(start_dt)
        end_date = _to_date(ref_date if ref_date is not None else end_dt)
        if start_date and end_date and end_date < start_date:
            end_date = start_date

        events = self.get_split_dividend(
            security,
            start_date=start_date,
            end_date=end_date,
        )
        if not events:
            return pd.DataFrame()

        price_cols = [col for col in ["open", "high", "low", "close"] if col in raw_df.columns]
        if not price_cols:
            return pd.DataFrame()

        adj_df = raw_df.copy()
        factors = pd.Series(1.0, index=adj_df.index)
        # 基于分红/送转事件构建前复权因子
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
            try:
                scale = float(event.get("scale_factor") or 1.0)
            except Exception:
                scale = 1.0
            scale_factor = 1.0 / scale if scale and scale > 0 else 1.0
            try:
                cash = float(event.get("bonus_pre_tax") or 0.0)
            except Exception:
                cash = 0.0
            try:
                per_base = float(event.get("per_base") or 10.0)
            except Exception:
                per_base = 10.0
            cash_per_share = cash / per_base if per_base > 0 else 0.0

            preclose = None
            if "pre_close" in adj_df.columns and event_day in adj_df.index.date:
                preclose = float(adj_df.loc[adj_df.index.date == event_day, "pre_close"].iloc[0])
            elif "preClose" in adj_df.columns and event_day in adj_df.index.date:
                preclose = float(adj_df.loc[adj_df.index.date == event_day, "preClose"].iloc[0])
            if preclose is None or preclose == 0.0:
                prev = adj_df.index[adj_df.index.date < event_day]
                if len(prev) > 0 and "close" in adj_df.columns:
                    preclose = float(adj_df.loc[prev.max(), "close"])

            cash_factor = 1.0
            if cash_per_share and preclose and preclose > 0:
                cash_factor = max((preclose - cash_per_share) / preclose, 0.0)
            total_factor = scale_factor * cash_factor
            if total_factor != 1.0:
                factors.loc[mask] = factors.loc[mask] * total_factor

        for col in price_cols:
            adj_df[col] = adj_df[col].astype(float) * factors

        return self._align_reference(raw_df, adj_df, pre_factor_ref_date)

    @staticmethod
    def _align_reference(
        raw_df: pd.DataFrame,
        adj_df: pd.DataFrame,
        pre_factor_ref_date: Optional[Union[str, datetime]],
    ) -> pd.DataFrame:
        if adj_df.empty or not pre_factor_ref_date:
            return adj_df
        try:
            ref_dt = pd.to_datetime(pre_factor_ref_date)
        except Exception:
            return adj_df
        if ref_dt not in raw_df.index or ref_dt not in adj_df.index:
            return adj_df
        try:
            reference_raw = float(raw_df.loc[ref_dt, "close"])
            reference_adj = float(adj_df.loc[ref_dt, "close"])
        except Exception:
            return adj_df
        if reference_adj == 0.0:
            return adj_df
        scale = reference_raw / reference_adj
        for col in ["open", "high", "low", "close"]:
            if col in adj_df.columns:
                adj_df[col] = adj_df[col] * scale
        return adj_df

    def _latest_trade_day(self) -> Optional[datetime]:
        try:
            days = self.get_trade_days(end_date=Date.today(), count=1)
            if days:
                return pd.to_datetime(days[-1])
        except Exception:
            return None
        return None

    def _fetch_adj_factor(self, security: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
        kwargs = {
            "security": security,
            "start_date": start_dt.strftime("%Y%m%d"),
            "end_date": end_dt.strftime("%Y%m%d"),
        }

        def _fetch(kw: Dict[str, Any]) -> pd.DataFrame:
            pro = self._ensure_client()
            ts_code = self._to_ts_code(kw["security"])
            return pro.adj_factor(
                ts_code=ts_code,
                start_date=kw["start_date"],
                end_date=kw["end_date"],
            )

        return self._cache.cached_call("adj_factor", kwargs, _fetch, result_type="df")

    # ------------------------ 交易日/基础信息 ------------------------
    def get_trade_days(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        count: Optional[int] = None,
    ) -> List[datetime]:
        kwargs = {
            "start_date": start_date,
            "end_date": end_date,
            "count": count,
        }

        def _fetch(kw: Dict[str, Any]) -> List[str]:
            pro = self._ensure_client()
            df = pro.trade_cal(
                exchange="SSE",
                start_date=self._format_date(kw.get("start_date")),
                end_date=self._format_date(kw.get("end_date")),
                fields="cal_date,is_open",
            )
            open_days = df[df["is_open"] == 1]["cal_date"].sort_values().tolist()
            if kw.get("count") and kw["count"] != -1:
                open_days = open_days[-kw["count"] :]
            return open_days

        date_strs = self._cache.cached_call("get_trade_days", kwargs, _fetch, result_type="list_str")
        return [pd.to_datetime(d).to_pydatetime() for d in date_strs]

    def get_all_securities(
        self,
        types: Union[str, List[str]] = "stock",
        date: Optional[Union[str, datetime]] = None,
    ) -> pd.DataFrame:
        if isinstance(types, str):
            types = [types]

        kwargs = {"types": tuple(sorted(types)), "date": date}

        def _fetch(kw: Dict[str, Any]) -> Dict[str, Any]:
            pro = self._ensure_client()
            rows = []
            for t in kw["types"]:
                if t == "stock":
                    df = pro.stock_basic(
                        exchange="",
                        list_status="L",
                        fields="ts_code,name,list_date,delist_date",
                    )
                    df["type"] = "stock"
                elif t in ("fund", "etf", "lof"):
                    df = pro.fund_basic(
                        status="L",
                        market="E",
                        fields="ts_code,name,list_date,delist_date,found_date",
                    )
                    if t == "etf":
                        df = df[df["ts_code"].str.endswith(("SH", "SZ"))]
                    elif t == "lof":
                        df = df[df["ts_code"].str.contains("LOF")]
                    df["type"] = t
                elif t == "index":
                    df = pro.index_basic(market="SSE")
                    df = pd.concat([df, pro.index_basic(market="SZSE")])
                    df.rename(columns={"fullname": "name"}, inplace=True)
                    df["type"] = "index"
                else:
                    continue

                df["display_name"] = df["name"]
                if "list_date" in df.columns:
                    start_series = df["list_date"]
                elif "found_date" in df.columns:
                    start_series = df["found_date"]
                else:
                    start_series = pd.Series([None] * len(df))
                df["start_date"] = pd.to_datetime(start_series, errors="coerce")
                end_series = df["delist_date"] if "delist_date" in df.columns else pd.Series([None] * len(df))
                df["end_date"] = pd.to_datetime(end_series, errors="coerce")
                rows.append(df[["ts_code", "display_name", "name", "start_date", "end_date", "type"]])

            if not rows:
                return {}
            merged = pd.concat(rows, ignore_index=True).drop_duplicates("ts_code")
            if kw.get("date") is not None:
                try:
                    target_dt = pd.to_datetime(kw["date"])
                    start_dt = pd.to_datetime(merged["start_date"], errors="coerce").fillna(pd.Timestamp.min)
                    end_dt = pd.to_datetime(merged["end_date"], errors="coerce").fillna(pd.Timestamp.max)
                    merged = merged[(start_dt <= target_dt) & (end_dt >= target_dt)]
                except Exception:
                    pass
            merged.set_index("ts_code", inplace=True)
            merged.index = [self._to_jq_code(code) for code in merged.index]
            return merged.to_dict(orient="index")

        data = self._cache.cached_call("get_all_securities", kwargs, _fetch, result_type="list_dict")
        if not data:
            return pd.DataFrame(columns=["display_name", "name", "start_date", "end_date", "type"])
        df = pd.DataFrame.from_dict(data, orient="index")
        df["start_date"] = pd.to_datetime(df["start_date"])
        df["end_date"] = pd.to_datetime(df["end_date"])
        return df

    def get_index_stocks(self, index_symbol: str, date: Optional[Union[str, datetime]] = None) -> List[str]:
        kwargs = {"index_symbol": index_symbol, "date": date}

        def _fetch(kw: Dict[str, Any]) -> List[str]:
            pro = self._ensure_client()
            index_code = self._to_ts_code(kw["index_symbol"])
            target_date = self._format_date(kw.get("date")) or datetime.today().strftime("%Y%m%d")
            df = pro.index_weight(index_code=index_code, trade_date=target_date)
            if df is None or df.empty:
                return []
            return [self._to_jq_code(code) for code in df["con_code"].dropna().tolist()]

        return self._cache.cached_call("get_index_stocks", kwargs, _fetch, result_type="list_str")

    def get_index_weights(self, index_id: str, date: Optional[Union[str, datetime]] = None) -> Any:
        kwargs = {"index_id": index_id, "date": date}

        def _fetch(kw: Dict[str, Any]) -> pd.DataFrame:
            pro = self._ensure_client()
            index_code = self._to_ts_code(kw["index_id"])
            target_date = self._format_date(kw.get("date")) or datetime.today().strftime("%Y%m%d")
            df = pro.index_weight(index_code=index_code, trade_date=target_date)
            if df is None or df.empty:
                return pd.DataFrame(columns=["code", "weight", "date"])
            df = df.rename(columns={"con_code": "code", "trade_date": "date"})
            df["code"] = df["code"].apply(self._to_jq_code)
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            return df[["code", "weight", "date"]]

        return self._cache.cached_call("get_index_weights", kwargs, _fetch, result_type="df")

    def get_security_info(
        self,
        security: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        def _normalize_date(value: Any) -> Optional[Date]:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            try:
                return pd.to_datetime(value).date()
            except Exception:
                return None

        target = self._to_jq_code(security)
        for t in ("stock", "fund", "etf", "lof", "index"):
            df = self.get_all_securities(types=t, date=date)
            if df is None or df.empty or target not in df.index:
                continue
            row = df.loc[target]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            return {
                "display_name": row.get("display_name") or target,
                "name": row.get("name") or target.split(".", 1)[0],
                "start_date": _normalize_date(row.get("start_date")),
                "end_date": _normalize_date(row.get("end_date")) or Date(2200, 1, 1),
                "type": row.get("type") or "stock",
                "subtype": None,
                "parent": None,
            }
        return {
            "display_name": target,
            "name": target.split(".", 1)[0],
            "start_date": None,
            "end_date": Date(2200, 1, 1),
            "type": "stock",
            "subtype": None,
            "parent": None,
        }

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

    # ------------------------ Live 快照 ------------------------
    def get_live_current(self, security: str) -> Dict[str, Any]:
        """
        返回实盘当前快照（最小字段）基于 tushare：
        - last_price: 当前价（回退使用最近1分钟 close）
        - high_limit/low_limit: 当日涨跌停价（若可获取）
        - paused: 默认 False
        若不可用或失败，返回空字典。
        """
        try:
            ts = self._ensure_ts_module()
            pro = self._ensure_client()
            # 回退策略：使用 pro.bar/ts.pro_bar 获取最近一分钟数据
            ts_code = self._to_ts_code(security)
            asset = self._infer_asset(security)
            df = ts.pro_bar(ts_code=ts_code, freq='1min', asset=asset, api=pro)
            if df is None or df.empty:
                return {}
            df = df.sort_values('trade_time' if 'trade_time' in df.columns else 'trade_date')
            row = df.iloc[-1]
            last_price = float(row.get('close') or 0.0)
            high_limit = float(row.get('up_limit') or 0.0) if 'up_limit' in df.columns else 0.0
            low_limit = float(row.get('down_limit') or 0.0) if 'down_limit' in df.columns else 0.0
            return {
                'last_price': last_price,
                'high_limit': high_limit,
                'low_limit': low_limit,
                'paused': False,
            }
        except Exception:
            return {}

    # ------------------------ 分红 / 拆分 ------------------------
    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[Union[str, datetime, Date]] = None,
        end_date: Optional[Union[str, datetime, Date]] = None,
    ) -> List[Dict[str, Any]]:
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
                    if pd.isna(val):
                        return None
                    return val
                except (TypeError, ValueError):
                    return None

            pro = self._ensure_client()
            sec = kw["security"]
            ts_code = self._to_ts_code(sec)
            sec_jq = self._to_jq_code(sec)
            start_dt = _parse_date(kw.get("start_date"))
            end_dt = _parse_date(kw.get("end_date"))

            def _in_range(check: Optional[Date]) -> bool:
                if check is None:
                    return False
                if start_dt and check < start_dt:
                    return False
                if end_dt and check > end_dt:
                    return False
                return True

            # 判断证券类型：基金/ETF代码通常以5开头（如511880），股票为6位数字
            code_only = sec_jq.split(".")[0] if "." in sec_jq else sec_jq
            is_fund = code_only.startswith("5") and len(code_only) == 6

            events: List[Dict[str, Any]] = []

            if is_fund:
                # 基金分红：使用 fund_div 接口
                try:
                    seen_dividends = set()
                    df = pro.fund_div(ts_code=ts_code)
                    if df is not None and not df.empty:
                        for _, row in df.iterrows():
                            div_proc = str(row.get("div_proc") or "")
                            if div_proc and "实施" not in div_proc:
                                continue
                            ex_date = _parse_date(row.get("ex_date") or row.get("ann_date"))
                            if not _in_range(ex_date):
                                continue
                            # Tushare 基金分红字段：div_cash 为每份派息
                            cash = _safe_float(row.get("div_cash")) or 0.0
                            signature = (ex_date, round(cash, 6))
                            if signature in seen_dividends:
                                continue
                            seen_dividends.add(signature)
                            events.append(
                                {
                                    "security": sec_jq,
                                    "date": ex_date,
                                    "security_type": "fund",
                                    "scale_factor": 1.0,
                                    "bonus_pre_tax": cash,
                                    "per_base": 1,
                                }
                            )
                except Exception:
                    # 如果基金接口失败，尝试用股票接口
                    pass

            # 股票分红：使用 dividend 接口
            if not is_fund or not events:
                try:
                    df = pro.dividend(ts_code=ts_code)
                    if df is not None and not df.empty:
                        for _, row in df.iterrows():
                            div_proc = str(row.get("div_proc") or "")
                            if div_proc and "实施" not in div_proc:
                                continue
                            ex_date = _parse_date(row.get("ex_date"))
                            if not _in_range(ex_date):
                                continue
                            # Tushare 股票分红字段为每股口径，需转换为每10股
                            cash_pre = _safe_float(row.get("cash_div_tax"))
                            if cash_pre is None or cash_pre == 0.0:
                                cash_pre = _safe_float(row.get("cash_div")) or 0.0
                            stock_paid = _safe_float(row.get("stk_bo_rate")) or 0.0
                            transfer = _safe_float(row.get("stk_co_rate")) or 0.0
                            if stock_paid == 0.0 and transfer == 0.0:
                                stock_paid = _safe_float(row.get("stk_div")) or 0.0
                            per_base = 10
                            scale = 1.0 + stock_paid + transfer
                            events.append(
                                {
                                    "security": sec_jq,
                                    "date": ex_date,
                                    "security_type": "stock",
                                    "scale_factor": scale,
                                    "bonus_pre_tax": cash_pre * per_base,
                                    "per_base": per_base,
                                }
                            )
                except Exception:
                    pass

            return events

        return self._cache.cached_call("get_split_dividend", kwargs, _fetch, result_type="list_dict")
