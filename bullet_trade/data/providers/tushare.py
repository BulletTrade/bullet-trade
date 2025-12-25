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
    ) -> pd.DataFrame:
        securities = security if isinstance(security, (list, tuple)) else [security]
        frames: Dict[str, pd.DataFrame] = {}

        for sec in securities:
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
    ) -> pd.DataFrame:
        start_str = self._format_date(start_date)
        end_str = self._format_date(end_date)
        freq = self._normalize_frequency(frequency)
        ts = self._ensure_ts_module()
        pro = self._ensure_client()
        asset = "E"  # 默认股票
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

        if fq in ("pre", "post"):
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
            return df

        factor_df.index = pd.to_datetime(factor_df["trade_date"])
        merged = df.join(factor_df["adj_factor"], how="left")
        merged["adj_factor"] = merged["adj_factor"].ffill().bfill()
        ref_date = pre_factor_ref_date or end_dt if fq == "pre" else pre_factor_ref_date or start_dt
        try:
            ref_date = pd.to_datetime(ref_date)
        except Exception:
            ref_date = end_dt if fq == "pre" else start_dt

        ref_factor = merged.loc[ref_date, "adj_factor"] if ref_date in merged.index else merged["adj_factor"].iloc[-1]
        if fq == "pre":
            ratio = merged["adj_factor"] / ref_factor
        else:
            ratio = ref_factor / merged["adj_factor"]

        for col in ["open", "high", "low", "close"]:
            if col in merged.columns:
                merged[col] = merged[col] * ratio

        merged.drop(columns=["adj_factor"], inplace=True, errors="ignore")
        return merged

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
            open_days = df[df["is_open"] == 1]["cal_date"].tolist()
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
                df["start_date"] = pd.to_datetime(df.get("list_date") or df.get("found_date"), errors="coerce")
                df["end_date"] = pd.to_datetime(df.get("delist_date"), errors="coerce")
                rows.append(df[["ts_code", "display_name", "name", "start_date", "end_date", "type"]])

            if not rows:
                return {}
            merged = pd.concat(rows, ignore_index=True).drop_duplicates("ts_code")
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
            df = ts.pro_bar(ts_code=ts_code, freq='1min', api=pro)
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
