"""
easy_tdx 通达信在线数据源适配器

文件职责：通过 easy_tdx SDK 直连通达信在线行情服务器，提供聚宽兼容行情接口。
主要输入：聚宽或通达信风格证券代码、日期范围、频率、字段、复权参数。
主要输出：K 线 DataFrame、交易日、证券列表、实时快照和有限的除权除息事件。
上下游关系：由 bullet_trade.data.api 的 provider 工厂按 easy_tdx/tdx 名称创建。
关键约定：真实模式连接失败不得自动返回假行情；stub 仅在 use_stub=True 时用于测试或演示。
"""

from __future__ import annotations

import importlib
import math
import os
from datetime import date as Date
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd

from .base import DataProvider

_MARKET_SZ = 0
_MARKET_SH = 1
_MARKET_BJ = 2
_SUFFIX_TO_MARKET: Dict[str, int] = {
    "XSHE": _MARKET_SZ,
    "SZ": _MARKET_SZ,
    "XSHG": _MARKET_SH,
    "SH": _MARKET_SH,
    "XBJG": _MARKET_BJ,
    "BJ": _MARKET_BJ,
    "BSE": _MARKET_BJ,
}
_MARKET_TO_SUFFIX: Dict[int, str] = {
    _MARKET_SZ: "XSHE",
    _MARKET_SH: "XSHG",
    _MARKET_BJ: "XBJG",
}
_FREQUENCY_TO_PERIOD: Dict[str, int] = {
    "5m": 0,
    "15m": 1,
    "30m": 2,
    "60m": 3,
    "1h": 3,
    "daily": 4,
    "1d": 4,
    "d": 4,
    "weekly": 5,
    "1w": 5,
    "monthly": 6,
    "1M": 6,
    "1m": 7,
    "minute": 7,
    "m1": 7,
}
_ADJUST_MAP: Dict[Optional[str], int] = {
    None: 0,
    "": 0,
    "none": 0,
    "pre": 1,
    "qfq": 1,
    "post": 2,
    "hfq": 2,
}
_DEFAULT_PRICE_FIELDS = ["open", "close", "high", "low", "volume", "money"]
_PRICE_FIELDS = {"open", "close", "high", "low", "avg", "price", "high_limit", "low_limit"}
_STUB_NAMES: Dict[str, str] = {
    "000001.XSHE": "平安银行",
    "600519.XSHG": "贵州茅台",
    "601318.XSHG": "中国平安",
    "510050.XSHG": "上证50ETF",
    "000001.XSHG": "上证指数",
}


class EasyTdxProvider(DataProvider):
    """通达信在线行情数据源，负责把 easy_tdx 输出转换为聚宽兼容格式。"""

    name: str = "easy_tdx"
    requires_live_data: bool = False
    _MAX_DAILY_FETCH = 50000
    _MAX_INTRADAY_FETCH = 50000

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        """初始化 provider，不在构造阶段连接通达信服务器。"""
        self.config = config or {}
        self._host = self.config.get("host") or os.getenv("EASY_TDX_HOST")
        self._port = int(self.config.get("port") or os.getenv("EASY_TDX_PORT") or 7709)
        self._timeout = float(self.config.get("timeout") or os.getenv("EASY_TDX_TIMEOUT") or 10.0)
        self._use_stub = self._parse_bool(
            self.config.get("use_stub") or os.getenv("EASY_TDX_USE_STUB")
        )
        self._client: Optional[Any] = self.config.get("client")
        self._connected = bool(self._client is not None)
        self._easy_tdx: Optional[Any] = self.config.get("easy_tdx")
        self._mac_client_cls: Optional[Any] = self.config.get("mac_client_cls")
        self._tdx_client_cls: Optional[Any] = self.config.get("tdx_client_cls")

    @staticmethod
    def _parse_bool(value: Any) -> bool:
        """把环境变量或配置值解析为布尔值。"""
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _as_list(security: Union[str, Sequence[str]]) -> List[str]:
        """把单证券或证券序列统一为列表。"""
        if isinstance(security, str):
            return [security]
        if security is None:
            return []
        return [str(item) for item in security]

    @staticmethod
    def tdx_to_jq(market: int, code: str) -> str:
        """把通达信 market/code 转换为聚宽代码。"""
        suffix = _MARKET_TO_SUFFIX.get(int(market), "XSHE")
        return f"{str(code).zfill(6)}.{suffix}"

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
    def jq_to_tdx(security: str) -> Tuple[int, str]:
        """把聚宽或简写代码转换为通达信 market/code。"""
        sec = str(security).strip()
        if "." in sec:
            code, suffix = sec.rsplit(".", 1)
            return _SUFFIX_TO_MARKET.get(suffix.upper(), _MARKET_SZ), code
        code = sec
        if code.startswith(("6", "5", "9")):
            return _MARKET_SH, code
        if code.startswith(("8", "4")):
            return _MARKET_BJ, code
        return _MARKET_SZ, code

    @staticmethod
    def _normalize_frequency(frequency: str) -> str:
        """把聚宽频率标准化为内部字符串。"""
        freq = str(frequency or "daily").strip()
        lower = freq.lower()
        if lower in {"daily", "1d", "d", "day"}:
            return "daily"
        if lower in {"minute", "1m", "m1", "1min"}:
            return "1m"
        return freq

    @classmethod
    def _resolve_period(cls, frequency: str) -> int:
        """把标准化频率转换为 easy_tdx Period 整数。"""
        freq = str(frequency or "daily").strip()
        return _FREQUENCY_TO_PERIOD.get(freq, _FREQUENCY_TO_PERIOD.get(freq.lower(), 4))

    @staticmethod
    def _resolve_adjust(fq: Optional[str]) -> int:
        """把聚宽复权参数转换为 easy_tdx adjust 整数。"""
        if fq is None:
            return 0
        return _ADJUST_MAP.get(str(fq).lower().strip(), 0)

    @classmethod
    def _bars_per_day(cls, frequency: str) -> int:
        """返回分钟频率对应单日 bar 数。"""
        mapping = {"1m": 240, "5m": 48, "15m": 16, "30m": 8, "60m": 4}
        return mapping.get(str(frequency).lower(), 1)

    @staticmethod
    def _format_timestamp(value: Optional[Union[str, datetime, Date]]) -> Optional[pd.Timestamp]:
        """把日期参数转换为 Timestamp。"""
        if value is None:
            return None
        parsed = pd.to_datetime(value)
        if pd.isna(parsed):
            return None
        return pd.Timestamp(parsed)

    def _ensure_easy_tdx(self) -> Any:
        """延迟导入 easy_tdx 模块；缺失时抛出安装指引。"""
        if self._easy_tdx is not None:
            return self._easy_tdx
        try:
            self._easy_tdx = importlib.import_module("easy_tdx")
        except ImportError as exc:  # pragma: no cover - 只在真实缺依赖时触发
            raise ImportError(
                "未安装 easy-tdx，请在 Python 3.10+ 环境执行 `pip install bullet-trade[tdx]` 或 `pip install easy-tdx`"
            ) from exc
        return self._easy_tdx

    def _resolve_mac_client_cls(self) -> Any:
        """解析 MacClient 类，兼容测试注入和真实 SDK。"""
        if self._mac_client_cls is not None:
            return self._mac_client_cls
        module = self._ensure_easy_tdx()
        self._mac_client_cls = getattr(module, "MacClient")
        return self._mac_client_cls

    def _resolve_tdx_client_cls(self) -> Any:
        """解析 TdxClient 类，缺失时抛出 NotImplementedError。"""
        if self._tdx_client_cls is not None:
            return self._tdx_client_cls
        module = self._ensure_easy_tdx()
        cls = getattr(module, "TdxClient", None)
        if cls is None:
            raise NotImplementedError("easy_tdx 未提供 TdxClient，无法读取除权除息数据")
        self._tdx_client_cls = cls
        return cls

    @staticmethod
    def _ensure_config_dir() -> None:
        """确保 easy_tdx 默认配置目录存在，避免自动选主机保存配置失败。"""
        Path.home().joinpath(".easy_tdx").mkdir(parents=True, exist_ok=True)

    def auth(
        self,
        user: Optional[str] = None,
        pwd: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        """连接通达信行情服务器；真实模式失败时抛出异常，stub 模式跳过连接。"""
        _ = user, pwd
        if self._connected:
            return
        if self._use_stub:
            self._connected = True
            return
        target_host = host or self._host
        target_port = int(port or self._port)
        client_cls = self._resolve_mac_client_cls()
        try:
            self._ensure_config_dir()
            if target_host:
                self._client = client_cls(host=target_host, port=target_port, timeout=self._timeout)
            elif hasattr(client_cls, "from_best_host"):
                self._client = client_cls.from_best_host(port=target_port, timeout=self._timeout)
            else:
                self._client = client_cls(port=target_port, timeout=self._timeout)
            connect = getattr(self._client, "connect", None)
            if callable(connect):
                connect()
            self._connected = True
        except Exception as exc:
            self._client = None
            self._connected = False
            message = "EasyTdxProvider 连接通达信行情服务器失败；真实模式不会自动返回假行情，" "如仅做离线测试请显式传入 use_stub=True"
            raise RuntimeError(message) from exc

    def _ensure_client(self) -> Optional[Any]:
        """返回已连接客户端；stub 模式返回 None。"""
        if self._use_stub:
            self._connected = True
            return None
        if not self._connected:
            self.auth()
        return self._client

    def _resolve_fetch_count(
        self,
        *,
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
        frequency: str,
        count: Optional[int],
    ) -> int:
        """根据日期范围和 count 估算通达信请求条数。"""
        if count is not None:
            return max(int(count), 1)
        start_ts = self._format_timestamp(start_date)
        end_ts = self._format_timestamp(end_date) or pd.Timestamp.now()
        if start_ts is None:
            return 800 if frequency != "daily" else 3000
        fetch_end = max(end_ts, pd.Timestamp.now())
        days = max((fetch_end - start_ts).days + 1, 1)
        bars = self._bars_per_day(frequency)
        estimated = max(int(math.ceil(days * 0.75)) * bars + bars * 5, bars)
        cap = self._MAX_INTRADAY_FETCH if bars > 1 else self._MAX_DAILY_FETCH
        return min(max(estimated, 100 if bars == 1 else bars * 10), cap)

    def _fetch_single_kline(
        self,
        client: Any,
        security: str,
        *,
        period: int,
        fetch_count: int,
        adjust: int,
        fields: List[str],
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
        count: Optional[int],
    ) -> pd.DataFrame:
        """调用 easy_tdx 获取单证券 K 线并标准化。"""
        market, code = self.jq_to_tdx(security)
        fetcher = getattr(client, "get_stock_kline", None)
        if not callable(fetcher):
            raise RuntimeError("easy_tdx MacClient 未提供 get_stock_kline 方法")
        raw = fetcher(
            market=market,
            code=code,
            period=period,
            start=0,
            count=fetch_count,
            times=1,
            adjust=adjust,
        )
        df = self._normalize_kline_frame(raw, fields)
        df = self._filter_by_date(df, start_date=start_date, end_date=end_date)
        if count is not None and not df.empty:
            df = df.tail(int(count))
        return df

    def _fetch_daily_raw_for_factor(self, client: Any, security: str) -> pd.DataFrame:
        """读取未复权日线，用于按除权除息事件构造累计复权因子。"""
        market, code = self.jq_to_tdx(security)
        fetcher = getattr(client, "get_stock_kline", None)
        if not callable(fetcher):
            raise RuntimeError("easy_tdx MacClient 未提供 get_stock_kline 方法")
        raw = fetcher(
            market=market,
            code=code,
            period=self._resolve_period("daily"),
            start=0,
            count=self._MAX_DAILY_FETCH,
            times=1,
            adjust=0,
        )
        df = self._normalize_kline_frame(raw, ["close"])
        if df.empty:
            return df
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[df.index.notna()].sort_index()
        return df

    @staticmethod
    def _normalize_kline_frame(raw: Any, fields: List[str]) -> pd.DataFrame:
        """把 easy_tdx K 线返回值转换为 index=time 的聚宽字段表。"""
        df = pd.DataFrame(raw).copy()
        if df.empty:
            return pd.DataFrame()
        df.rename(columns={"vol": "volume", "amount": "money"}, inplace=True)
        time_col = None
        for candidate in ("datetime", "date", "time"):
            if candidate in df.columns:
                time_col = candidate
                break
        if time_col is None:
            return pd.DataFrame()
        df["time"] = pd.to_datetime(df[time_col], errors="coerce")
        df.dropna(subset=["time"], inplace=True)
        df.set_index("time", inplace=True)
        df.sort_index(inplace=True)
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
        if "money" in df.columns:
            df["money"] = pd.to_numeric(df["money"], errors="coerce").fillna(0.0)
        for field in _DEFAULT_PRICE_FIELDS:
            if field not in df.columns:
                df[field] = 0.0
        if "high_limit" in fields and "high_limit" not in df.columns:
            df["high_limit"] = 0.0
        if "low_limit" in fields and "low_limit" not in df.columns:
            df["low_limit"] = 0.0
        if "paused" in fields and "paused" not in df.columns:
            df["paused"] = False
        keep = [field for field in fields if field in df.columns]
        return df[keep]

    @staticmethod
    def _filter_by_date(
        df: pd.DataFrame,
        *,
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
    ) -> pd.DataFrame:
        """按 start_date/end_date 裁剪 K 线结果。"""
        if df.empty:
            return df
        result = df.copy()
        start_ts = EasyTdxProvider._format_timestamp(start_date)
        end_ts = EasyTdxProvider._format_timestamp(end_date)
        if start_ts is not None:
            result = result[result.index >= start_ts]
        if end_ts is not None:
            result = result[result.index <= end_ts]
        return result

    @staticmethod
    def _as_float(value: Any, default: float = 0.0) -> float:
        """把通达信事件字段转换为浮点数；空值或异常时返回默认值。"""
        try:
            if value is None or pd.isna(value):
                return default
            return float(value)
        except Exception:
            return default

    def _fetch_xdxr_events(self, security: str) -> pd.DataFrame:
        """读取通达信除权除息事件表，失败时返回空表。"""
        try:
            client_cls = self._resolve_tdx_client_cls()
        except Exception:
            return pd.DataFrame()
        market, code = self.jq_to_tdx(security)
        client = None
        try:
            if self._host:
                client = client_cls(host=self._host, port=self._port, timeout=self._timeout)
            else:
                client = client_cls(port=self._port, timeout=self._timeout)
            connect = getattr(client, "connect", None)
            if callable(connect):
                connect()
            records = client.get_xdxr_info(market, code)
        except Exception:
            return pd.DataFrame()
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
        df = pd.DataFrame(records).copy()
        if df.empty or "date" not in df.columns:
            return pd.DataFrame()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df.dropna(subset=["date"], inplace=True)
        if "category" in df.columns:
            df = df[df["category"] == 1]
        return df.sort_values("date").reset_index(drop=True)

    def _event_adjust_factor(self, event: pd.Series, daily_raw: pd.DataFrame) -> float:
        """按单条除权除息事件计算前复权乘数。"""
        event_day = pd.Timestamp(event["date"]).normalize()
        before = daily_raw[daily_raw.index < event_day]
        if before.empty or "close" not in before.columns:
            return 1.0
        preclose = self._as_float(before["close"].iloc[-1], 0.0)
        if preclose <= 0:
            return 1.0
        cash = self._as_float(event.get("fenhong"), 0.0)
        bonus_ratio = self._as_float(event.get("songzhuangu"), 0.0) / 10.0
        rights_ratio = self._as_float(event.get("peigu"), 0.0) / 10.0
        rights_price = self._as_float(event.get("peigujia"), 0.0)
        denominator = 1.0 + bonus_ratio + rights_ratio
        if denominator <= 0:
            return 1.0
        factor = (preclose - cash + rights_price * rights_ratio) / (preclose * denominator)
        return factor if factor > 0 else 1.0

    def _build_factor_for_index(
        self,
        client: Any,
        security: str,
        target_index: pd.DatetimeIndex,
        *,
        ref_date: Optional[Union[str, datetime, Date]] = None,
    ) -> pd.Series:
        """按 TDX 除权除息事件为目标时间索引构造累计前复权 factor。"""
        if target_index.empty:
            return pd.Series(dtype="float64")
        daily_raw = self._fetch_daily_raw_for_factor(client, security)
        if daily_raw.empty:
            return pd.Series(dtype="float64")
        events = self._fetch_xdxr_events(security)
        if events.empty:
            return pd.Series(1.0, index=target_index, dtype="float64")

        target_ts = pd.DatetimeIndex(pd.to_datetime(target_index, errors="coerce")).dropna()
        if target_ts.empty:
            return pd.Series(dtype="float64")
        horizon = target_ts.max().normalize()
        ref_ts = self._format_timestamp(ref_date)
        if ref_ts is not None:
            horizon = max(horizon, ref_ts.normalize())

        daily = daily_raw[daily_raw.index <= horizon].copy()
        if daily.empty:
            return pd.Series(dtype="float64")
        daily_days = pd.DatetimeIndex(daily.index.normalize()).drop_duplicates().sort_values()
        factor_by_day = pd.Series(1.0, index=daily_days, dtype="float64")
        for _, event in events.iterrows():
            event_day = pd.Timestamp(event["date"]).normalize()
            if event_day > horizon:
                continue
            event_factor = self._event_adjust_factor(event, daily_raw)
            if event_factor == 1.0:
                continue
            factor_by_day.loc[factor_by_day.index < event_day] *= event_factor

        target_days = pd.DatetimeIndex(pd.to_datetime(target_index, errors="coerce")).normalize()
        aligned = factor_by_day.reindex(target_days, method="ffill")
        aligned = aligned.bfill().fillna(1.0)
        return pd.Series(aligned.to_numpy(dtype="float64"), index=target_index, dtype="float64")

    @staticmethod
    def _factor_ref_value(
        factor: pd.Series,
        *,
        ref_date: Optional[Union[str, datetime, Date]],
    ) -> float:
        """从 factor 序列中提取动态前复权参考值。"""
        if factor.empty:
            return 1.0
        if ref_date is None:
            value = factor.iloc[-1]
            return float(value) if pd.notna(value) and value else 1.0
        ref_ts = EasyTdxProvider._format_timestamp(ref_date)
        if ref_ts is None:
            value = factor.iloc[-1]
            return float(value) if pd.notna(value) and value else 1.0
        sub = factor[factor.index <= ref_ts]
        value = sub.iloc[-1] if not sub.empty else factor.iloc[-1]
        return float(value) if pd.notna(value) and value else 1.0

    def _apply_constructed_pre_adjustment(
        self,
        frame: pd.DataFrame,
        *,
        factor: pd.Series,
        ref_date: Optional[Union[str, datetime, Date]],
    ) -> pd.DataFrame:
        """使用构造出的 factor 对未复权 K 线执行动态前复权。"""
        if frame.empty or factor.empty:
            return frame
        result = frame.copy()
        aligned = factor.reindex(result.index).ffill().bfill().fillna(1.0)
        factor_ref = self._factor_ref_value(aligned, ref_date=ref_date)
        ratio = aligned / factor_ref
        for field in _PRICE_FIELDS:
            if field in result.columns:
                result[field] = pd.to_numeric(result[field], errors="coerce") * ratio
        result["factor"] = aligned
        return result

    def _assemble_price_result(
        self,
        frames: Dict[str, pd.DataFrame],
        *,
        fields: List[str],
        securities: List[str],
        panel: bool,
    ) -> pd.DataFrame:
        """按聚宽兼容 shape 组装单证券、多证券、panel 和长表。"""
        if not frames:
            return pd.DataFrame()
        if len(securities) == 1 and panel:
            return frames.get(securities[0], pd.DataFrame())
        if panel:
            parts = []
            for field in fields:
                field_frames = []
                for security in securities:
                    df = frames.get(security)
                    if df is None or df.empty:
                        continue
                    series = (
                        df[field]
                        if field in df.columns
                        else pd.Series(index=df.index, dtype="float64")
                    )
                    field_frames.append(series.rename(security))
                if field_frames:
                    parts.append(pd.concat(field_frames, axis=1))
                else:
                    parts.append(pd.DataFrame(columns=securities))
            wide = pd.concat(parts, axis=1, keys=fields)
            wide.columns.names = ["field", "code"]
            wide.sort_index(inplace=True)
            return wide
        rows = []
        for security in securities:
            df = frames.get(security)
            if df is None or df.empty:
                continue
            part = df.copy().reset_index().rename(columns={"index": "time"})
            if "time" not in part.columns:
                part.rename(columns={part.columns[0]: "time"}, inplace=True)
            part["code"] = security
            for field in fields:
                if field not in part.columns:
                    part[field] = False if field == "paused" else 0.0
            rows.append(part[["time", "code"] + fields])
        if not rows:
            return pd.DataFrame()
        return (
            pd.concat(rows, ignore_index=True).sort_values(["time", "code"]).reset_index(drop=True)
        )

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
        """获取通达信 K 线；前复权优先使用除权除息事件构造动态 factor。"""
        _ = skip_paused, fill_paused, prefer_engine, force_no_engine
        securities = self._as_list(security)
        if not securities:
            return pd.DataFrame()
        requested_fields = list(fields or _DEFAULT_PRICE_FIELDS)
        needs_factor = "factor" in requested_fields
        frequency_norm = self._normalize_frequency(frequency)
        if self._use_stub:
            return self._stub_get_price(
                securities,
                fields=requested_fields,
                frequency=frequency_norm,
                start_date=start_date,
                end_date=end_date,
                count=count,
                panel=panel,
            )
        client = self._ensure_client()
        if client is None:
            return pd.DataFrame()
        fetch_count = self._resolve_fetch_count(
            start_date=start_date,
            end_date=end_date,
            frequency=frequency_norm,
            count=count,
        )
        period = self._resolve_period(frequency_norm)
        use_constructed_pre = str(fq or "").lower().strip() in {"pre", "qfq"} or needs_factor
        adjust = 0 if use_constructed_pre else self._resolve_adjust(fq)
        frames: Dict[str, pd.DataFrame] = {}
        for sec in securities:
            fetch_fields = list(requested_fields)
            if use_constructed_pre and all(field == "factor" for field in fetch_fields):
                fetch_fields = ["close"]
            frame = self._fetch_single_kline(
                client,
                sec,
                period=period,
                fetch_count=fetch_count,
                adjust=adjust,
                fields=fetch_fields,
                start_date=start_date,
                end_date=end_date,
                count=count,
            )
            if use_constructed_pre and not frame.empty:
                factor = self._build_factor_for_index(
                    client,
                    sec,
                    pd.DatetimeIndex(frame.index),
                    ref_date=pre_factor_ref_date,
                )
                if factor.empty and pre_factor_ref_date is not None:
                    raise NotImplementedError("easy_tdx online 模式无法构造复权因子，不能执行动态前复权")
                if not factor.empty:
                    frame = self._apply_constructed_pre_adjustment(
                        frame,
                        factor=factor,
                        ref_date=pre_factor_ref_date,
                    )
                elif str(fq or "").lower().strip() in {"pre", "qfq"}:
                    frame = self._fetch_single_kline(
                        client,
                        sec,
                        period=period,
                        fetch_count=fetch_count,
                        adjust=self._resolve_adjust(fq),
                        fields=fetch_fields,
                        start_date=start_date,
                        end_date=end_date,
                        count=count,
                    )
            for field in requested_fields:
                if field not in frame.columns:
                    frame[field] = 0.0
            frame = frame[[field for field in requested_fields if field in frame.columns]]
            if not frame.empty:
                frames[sec] = frame
        return self._assemble_price_result(
            frames,
            fields=requested_fields,
            securities=securities,
            panel=panel,
        )

    def get_bars(
        self,
        security: Union[str, List[str]],
        count: int,
        unit: str = "1d",
        fields: Optional[List[str]] = None,
        include_now: bool = False,
        end_dt: Optional[Union[str, datetime]] = None,
        fq_ref_date: Union[int, datetime] = 1,
        df: bool = False,
    ) -> Any:
        """用 get_price 提供聚宽 get_bars 兼容入口。"""
        _ = include_now, fq_ref_date
        request_fields = [field for field in (fields or _DEFAULT_PRICE_FIELDS) if field != "date"]
        frequency = "daily" if str(unit).lower() in {"1d", "d", "day", "daily"} else unit
        result = self.get_price(
            security=security,
            end_date=end_dt,
            frequency=frequency,
            fields=request_fields,
            count=count,
            fq="pre",
            panel=df,
        )
        if df or not isinstance(result, pd.DataFrame):
            return result
        return result.to_dict()

    def get_trade_days(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        count: Optional[int] = None,
    ) -> List[datetime]:
        """用上证指数日线日期近似通达信交易日历。"""
        if self._use_stub:
            days = self._stub_trade_days(start_date=start_date, end_date=end_date, count=count)
            return days
        client = self._ensure_client()
        if client is None:
            return []
        fetch_count = max(int(count or 3000), 1)
        raw = client.get_stock_kline(
            market=_MARKET_SH,
            code="999999",
            period=4,
            start=0,
            count=min(fetch_count, self._MAX_DAILY_FETCH),
            times=1,
            adjust=0,
        )
        df = self._normalize_kline_frame(raw, ["close"])
        df = self._filter_by_date(df, start_date=start_date, end_date=end_date)
        days = [pd.Timestamp(idx).to_pydatetime() for idx in df.index]
        if count:
            days = days[-int(count) :]
        return days

    def get_all_securities(
        self,
        types: Union[str, List[str]] = "stock",
        date: Optional[Union[str, datetime]] = None,
    ) -> pd.DataFrame:
        """获取证券列表；online 模式依赖 easy_tdx quotes-list 能力。"""
        _ = date
        if self._use_stub:
            return self._stub_all_securities(types)
        client = self._ensure_client()
        if client is None:
            return pd.DataFrame()
        getter = getattr(client, "get_stock_quotes_list", None)
        if not callable(getter):
            return pd.DataFrame()
        type_list = [types] if isinstance(types, str) else list(types or [])
        rows = []
        categories = self._resolve_categories(type_list)
        for category, label in categories:
            try:
                raw = getter(category, count=5000)
            except TypeError:
                raw = getter(category)
            except Exception:
                continue
            df = pd.DataFrame(raw)
            if df.empty:
                continue
            for _, row in df.iterrows():
                code = str(row.get("code", "")).strip()
                if not code:
                    continue
                market = int(row.get("market", _MARKET_SZ) or _MARKET_SZ)
                jq_code = self.tdx_to_jq(market, code)
                rows.append(
                    {
                        "code": jq_code,
                        "display_name": row.get("name") or jq_code,
                        "name": row.get("name") or code,
                        "start_date": Date(1990, 12, 19),
                        "end_date": Date(2200, 1, 1),
                        "type": label,
                    }
                )
        if not rows:
            return pd.DataFrame(columns=["display_name", "name", "start_date", "end_date", "type"])
        return pd.DataFrame(rows).drop_duplicates(subset=["code"]).set_index("code")

    def _resolve_categories(self, types: List[str]) -> List[Tuple[Any, str]]:
        """根据请求类型解析 easy_tdx 分类枚举或保守整数。"""
        try:
            category_module = importlib.import_module("easy_tdx.mac.enums")
            category = getattr(category_module, "Category")
            stock_cat = getattr(category, "A", 0)
            index_cat = getattr(category, "ZS", 1)
            fund_cat = getattr(category, "ETF", 2)
        except Exception:
            stock_cat, index_cat, fund_cat = 0, 1, 2
        requested = {str(item).lower() for item in types} or {"stock"}
        result = []
        if "all" in requested or "stock" in requested:
            result.append((stock_cat, "stock"))
        if "all" in requested or "index" in requested:
            result.append((index_cat, "index"))
        if "all" in requested or {"fund", "etf"} & requested:
            result.append((fund_cat, "fund"))
        return result

    def get_index_stocks(
        self,
        index_symbol: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> List[str]:
        """获取指数成分股；online 无法稳定取得时返回空列表。"""
        _ = date
        if self._use_stub:
            return ["000001.XSHE", "600519.XSHG"] if index_symbol else []
        client = self._ensure_client()
        if client is None:
            return []
        getter = getattr(client, "get_board_members", None)
        if not callable(getter):
            return []
        market, code = self.jq_to_tdx(index_symbol)
        board_code = f"{market}{code}"
        try:
            raw = getter(board_code)
        except Exception:
            return []
        df = pd.DataFrame(raw)
        if df.empty:
            return []
        result = []
        for _, row in df.iterrows():
            item_code = str(row.get("code", "")).strip()
            if item_code:
                result.append(self.tdx_to_jq(int(row.get("market", _MARKET_SZ)), item_code))
        return result

    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> List[Dict[str, Any]]:
        """读取通达信除权除息记录；不可用时返回空列表。"""
        if self._use_stub:
            return []
        try:
            client_cls = self._resolve_tdx_client_cls()
        except Exception:
            return []
        market, code = self.jq_to_tdx(security)
        try:
            client = client_cls(host=self._host, port=self._port, timeout=self._timeout)
            connect = getattr(client, "connect", None)
            if callable(connect):
                connect()
            records = client.get_xdxr_info(market, code)
        except Exception:
            return []
        finally:
            try:
                client.close()
            except Exception:
                pass
        start_ts = self._format_timestamp(start_date)
        end_ts = self._format_timestamp(end_date)
        records_df = pd.DataFrame(records).copy()
        if records_df.empty:
            return []
        if "category" in records_df.columns:
            records_df = records_df[records_df["category"] == 1]
        result = []
        for _, rec in records_df.iterrows():
            event_date = self._record_date(rec)
            if event_date is None:
                continue
            if start_ts is not None and pd.Timestamp(event_date) < start_ts.normalize():
                continue
            if end_ts is not None and pd.Timestamp(event_date) > end_ts.normalize():
                continue
            split = (
                self._as_float(rec.get("songzhuangu", 0.0), 0.0)
                + self._as_float(rec.get("peigu", 0.0), 0.0)
            ) / 10.0
            result.append(
                {
                    "security": security,
                    "date": event_date,
                    "scale_factor": 1.0 + split,
                    "bonus_pre_tax": self._as_float(rec.get("fenhong", 0.0), 0.0),
                    "per_base": 1.0,
                }
            )
        return result

    @staticmethod
    def _record_date(record: Any) -> Optional[Date]:
        """从 easy_tdx 除权记录对象中提取日期。"""
        if hasattr(record, "get"):
            raw_date = record.get("date")
            if raw_date is not None:
                try:
                    return pd.Timestamp(raw_date).date()
                except Exception:
                    return None
        try:
            return Date(int(record.year), int(record.month), int(record.day))
        except Exception:
            raw_date = getattr(record, "date", None)
            if raw_date is None:
                return None
            try:
                return pd.Timestamp(raw_date).date()
            except Exception:
                return None

    def get_security_info(
        self,
        security: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        """返回证券基础信息，online 模式尽量从 quote/info 中补名称。"""
        _ = date
        code = str(security)
        display_name = _STUB_NAMES.get(code, code)
        if not self._use_stub:
            try:
                client = self._ensure_client()
                info_getter = (
                    getattr(client, "get_symbol_info", None) if client is not None else None
                )
                if callable(info_getter):
                    market, raw_code = self.jq_to_tdx(code)
                    raw_info = info_getter(market, raw_code)
                    display_name = str(getattr(raw_info, "name", None) or display_name)
            except Exception:
                pass
        kind = self._infer_jq_security_type(code)
        return {
            "code": security,
            "display_name": display_name,
            "name": display_name,
            "start_date": Date(1990, 12, 19),
            "end_date": Date(2200, 1, 1),
            "type": kind,
        }

    def get_current_tick(
        self,
        security: str,
        dt: Optional[Union[str, datetime]] = None,
        df: bool = False,
    ) -> Optional[Any]:
        """获取实时快照；stub 模式返回显式测试数据。"""
        if self._use_stub:
            tick = self._stub_current_tick(security)
            return pd.DataFrame([tick]) if df else tick
        client = self._ensure_client()
        if client is None:
            return pd.DataFrame() if df else None
        getter = getattr(client, "get_stock_quotes", None)
        if not callable(getter):
            return pd.DataFrame() if df else None
        market, code = self.jq_to_tdx(security)
        try:
            quotes = getter([(market, code)])
        except Exception:
            return pd.DataFrame() if df else None
        quote_df = pd.DataFrame(quotes)
        if quote_df.empty:
            return pd.DataFrame() if df else None
        row = quote_df.iloc[0]
        limit_up = row.get("limit_up", row.get("buy_price_limit", 0.0))
        limit_down = row.get("limit_down", row.get("sell_price_limit", 0.0))
        tick = {
            "sid": security,
            "symbol": security,
            "last_price": float(
                row.get("price", row.get("last_price", row.get("close", 0.0))) or 0.0
            ),
            "open": float(row.get("open", 0.0) or 0.0),
            "high": float(row.get("high", 0.0) or 0.0),
            "low": float(row.get("low", 0.0) or 0.0),
            "volume": float(row.get("vol", row.get("volume", 0.0)) or 0.0) * 100.0,
            "amount": float(row.get("amount", 0.0) or 0.0),
            "pre_close": float(row.get("pre_close", 0.0) or 0.0),
            "limit_up": float(limit_up or 0.0),
            "limit_down": float(limit_down or 0.0),
            "trading_status": int(row.get("trading_status", 0) or 0),
            "dt": str(dt or datetime.now()),
            "provider": self.name,
        }
        return pd.DataFrame([tick]) if df else tick

    def get_live_current(self, security: str) -> Dict[str, Any]:
        """返回 LiveCurrentData 需要的实时行情字典。"""
        tick = self.get_current_tick(security)
        if not isinstance(tick, dict):
            return {}
        status = int(tick.get("trading_status", 0) or 0)
        return {
            "last_price": float(tick.get("last_price", 0.0) or 0.0),
            "high_limit": float(tick.get("limit_up", 0.0) or 0.0),
            "low_limit": float(tick.get("limit_down", 0.0) or 0.0),
            "paused": bool(status & 0x8020),
        }

    def _stub_get_price(
        self,
        securities: List[str],
        *,
        fields: List[str],
        frequency: str,
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
        count: Optional[int],
        panel: bool,
    ) -> pd.DataFrame:
        """生成显式 stub 模式行情，仅用于单元测试和演示。"""
        end_ts = self._format_timestamp(end_date) or pd.Timestamp("2026-04-01")
        if start_date is not None:
            start_ts = self._format_timestamp(start_date) or (end_ts - pd.Timedelta(days=10))
        elif count is not None:
            start_ts = end_ts - pd.Timedelta(days=max(int(count) * 2, 1))
        else:
            start_ts = end_ts - pd.Timedelta(days=10)
        dates = []
        if frequency == "1m":
            current = start_ts.normalize() + pd.Timedelta(hours=9, minutes=30)
            while current <= end_ts:
                if current.weekday() < 5:
                    dates.append(current)
                current += pd.Timedelta(minutes=1)
        else:
            for day in pd.bdate_range(start_ts.normalize(), end_ts.normalize()):
                dates.append(pd.Timestamp(day))
        if count:
            dates = dates[-int(count) :]
        frames = {}
        for idx, sec in enumerate(securities):
            base = 10.0 + idx * 3.0
            rows = []
            for offset, ts in enumerate(dates):
                close = base + offset * 0.01
                rows.append(
                    {
                        "time": ts,
                        "open": round(close - 0.02, 3),
                        "close": round(close, 3),
                        "high": round(close + 0.03, 3),
                        "low": round(close - 0.04, 3),
                        "volume": 100000.0 + offset * 100.0,
                        "money": (100000.0 + offset * 100.0) * close,
                        "high_limit": round(close * 1.1, 3),
                        "low_limit": round(close * 0.9, 3),
                        "paused": False,
                    }
                )
            frame = pd.DataFrame(rows).set_index("time") if rows else pd.DataFrame()
            for field in fields:
                if field not in frame.columns:
                    frame[field] = False if field == "paused" else 0.0
            frames[sec] = frame[fields] if not frame.empty else frame
        return self._assemble_price_result(
            frames, fields=fields, securities=securities, panel=panel
        )

    @staticmethod
    def _stub_trade_days(
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
        count: Optional[int],
    ) -> List[datetime]:
        """生成显式 stub 模式交易日。"""
        end_ts = EasyTdxProvider._format_timestamp(end_date) or pd.Timestamp("2026-04-01")
        start_ts = EasyTdxProvider._format_timestamp(start_date) or (end_ts - pd.Timedelta(days=30))
        days = [pd.Timestamp(day).to_pydatetime() for day in pd.bdate_range(start_ts, end_ts)]
        if count:
            days = days[-int(count) :]
        return days

    @staticmethod
    def _stub_all_securities(types: Union[str, List[str]]) -> pd.DataFrame:
        """生成显式 stub 模式证券列表。"""
        requested = {types} if isinstance(types, str) else set(types or [])
        rows = []
        for code, name in _STUB_NAMES.items():
            kind = EasyTdxProvider._infer_jq_security_type(code)
            if requested and "all" not in requested and kind not in requested:
                continue
            rows.append(
                {
                    "code": code,
                    "display_name": name,
                    "name": name,
                    "start_date": Date(1990, 12, 19),
                    "end_date": Date(2200, 1, 1),
                    "type": kind,
                }
            )
        return pd.DataFrame(rows).set_index("code") if rows else pd.DataFrame()

    @staticmethod
    def _stub_current_tick(security: str) -> Dict[str, Any]:
        """生成显式 stub 模式实时快照。"""
        return {
            "sid": security,
            "symbol": security,
            "last_price": 10.0,
            "open": 9.9,
            "high": 10.1,
            "low": 9.8,
            "volume": 100000.0,
            "amount": 1000000.0,
            "pre_close": 9.95,
            "limit_up": 10.95,
            "limit_down": 8.96,
            "trading_status": 0,
            "provider": "easy_tdx_stub",
        }
