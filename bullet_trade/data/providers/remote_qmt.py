from __future__ import annotations

import ast
import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Union

import pandas as pd

from .base import DataProvider
from ...remote import RemoteQmtConnection


_PRICE_FIELD_NAMES = {
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "money",
    "amount",
    "avg",
    "price",
    "highlimit",
    "lowlimit",
    "paused",
    "preclose",
    "pre_close",
    "suspendflag",
    "suspend_flag",
    "openinterest",
    "open_interest",
    "settlementprice",
    "settelementprice",
}


def _env(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(key, default)


def _dataframe_from_payload(payload: Dict[str, Any]) -> pd.DataFrame:
    if not payload or payload.get("dtype") != "dataframe":
        return pd.DataFrame()
    columns = payload.get("columns") or []
    column_tuples = payload.get("column_tuples") or None
    records = payload.get("records") or []
    if column_tuples:
        columns = _multiindex_from_payload_columns(column_tuples, payload.get("column_index_names"))
    else:
        columns = _parse_legacy_tuple_columns(columns)
    df = pd.DataFrame(records, columns=columns)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = _normalise_price_multiindex_columns(df.columns)
    return df


def _price_field_tokens(values) -> Set[str]:
    return {str(value).replace(" ", "").replace("_", "").lower() for value in values}


def _normalise_price_multiindex_columns(columns: pd.MultiIndex) -> pd.MultiIndex:
    if columns.nlevels != 2:
        return columns
    level0 = _price_field_tokens(columns.get_level_values(0))
    level1 = _price_field_tokens(columns.get_level_values(1))
    if (level1 & _PRICE_FIELD_NAMES) and not (level0 & _PRICE_FIELD_NAMES):
        columns = columns.swaplevel(0, 1)
        columns.names = ["field", "code"]
    elif (level0 & _PRICE_FIELD_NAMES) and not (level1 & _PRICE_FIELD_NAMES):
        columns.names = ["field", "code"]
    return columns


def _multiindex_from_payload_columns(column_tuples, names) -> pd.MultiIndex:
    tuples = [tuple(items) for items in column_tuples]
    index = pd.MultiIndex.from_tuples(tuples)
    if names and len(names) == index.nlevels:
        index.names = list(names)
    return index


def _parse_legacy_tuple_columns(columns):
    parsed = []
    for column in columns:
        if not isinstance(column, str) or not column.startswith("("):
            return columns
        try:
            value = ast.literal_eval(column)
        except Exception:
            return columns
        if not isinstance(value, tuple) or len(value) != 2:
            return columns
        parsed.append(value)
    return pd.MultiIndex.from_tuples(parsed) if parsed else columns


class RemoteQmtProvider(DataProvider):
    """
    通过 TCP 远程访问 bullet-trade server 的数据提供者。
    """

    name = "qmt-remote"
    requires_live_data = True

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config = config or {}
        host = self.config.get("host") or _env("QMT_SERVER_HOST", "127.0.0.1")
        port = int(self.config.get("port") or _env("QMT_SERVER_PORT", 58620))
        token = self.config.get("token") or _env("QMT_SERVER_TOKEN")
        if not token:
            raise RuntimeError("缺少 QMT_SERVER_TOKEN，用于鉴权远程 server")
        tls_cert = self.config.get("tls_cert") or _env("QMT_SERVER_TLS_CERT")
        tls_enabled = bool(tls_cert)
        self._connection = RemoteQmtConnection(host, port, token, tls_cert=tls_cert, tls_enabled=tls_enabled)
        self._connection.add_event_listener("tick", self._handle_tick_event)
        self._connection.start()
        self._subscription_key = "remote-provider"
        self._tick_callback: Optional[Callable[..., None]] = None
        self._tick_context: Optional[Any] = None

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

        def _is_minute_frequency(value: str) -> bool:
            freq = str(value or "").strip().lower()
            if "minute" in freq or "min" in freq:
                return True
            return freq.endswith("m") and freq[:-1].isdigit()

        def _str_format(date_obj):
            if date_obj and isinstance(date_obj, datetime):
                return date_obj.strftime("%Y-%m-%d %H:%M:%S" if _is_minute_frequency(frequency) else "%Y-%m-%d")
            return date_obj

        payload = {
            "security": security,
            "start": _str_format(start_date),
            "end": _str_format(end_date),
            "frequency": frequency,
            "fields": fields,
            "fq": fq,
            "count": count,
            "pre_factor_ref_date": _str_format(pre_factor_ref_date),
        }

        resp = self._connection.request("data.history", payload)
        return _dataframe_from_payload(resp)

    def get_trade_days(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        count: Optional[int] = None,
    ) -> List[pd.Timestamp]:
        payload = {"start": start_date, "end": end_date, "count": count}
        resp = self._connection.request("data.trade_days", payload)
        values = resp.get("value") or resp.get("values") or []
        return [pd.to_datetime(v) for v in values]

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

    def get_all_securities(self, types: Union[str, List[str]] = "stock", date: Optional[str] = None) -> pd.DataFrame:
        payload = {"types": types, "date": date}
        resp = self._connection.request("data.get_all_securities", payload)
        return _dataframe_from_payload(resp)

    def get_index_stocks(self, index_symbol: str, date: Optional[str] = None) -> List[str]:
        payload = {"index_symbol": index_symbol, "date": date}
        resp = self._connection.request("data.get_index_stocks", payload)
        return resp.get("values") or []

    def get_split_dividend(
        self,
        security: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        payload = {"security": security, "start": start_date, "end": end_date}
        resp = self._connection.request("data.get_split_dividend", payload)
        return resp.get("events") or []

    def get_security_info(
        self,
        security: str,
        date: Optional[Union[str, datetime]] = None,
    ) -> Dict[str, Any]:
        payload = {"security": security, "date": date}
        resp = self._connection.request("data.security_info", payload)
        if not isinstance(resp, dict):
            return {}
        value = resp.get("value")
        if isinstance(value, dict) and value:
            return value
        return {
            key: item
            for key, item in resp.items()
            if key not in {"dtype", "value"} and item is not None
        }

    def set_tick_callback(
        self,
        callback: Callable[..., None],
        context: Optional[Any] = None,
    ) -> None:
        """注册标准 provider tick 回调，并兼容旧版显式上下文。

        Args:
            callback: LiveEngine 使用 ``callback(tick)``；旧 Core API 可使用
                ``callback(context, tick)``。
            context: 旧版调用方绑定的上下文；省略时按标准单参数合同分发。

        Returns:
            None: 仅更新内存中的回调和可选上下文。

        Side Effects:
            后续远程 tick 事件会同步调用已注册回调。
        """

        self._tick_callback = callback
        self._tick_context = context

    def subscribe_ticks(self, symbols: List[str]) -> Dict:
        return self._connection.subscribe(self._subscription_key, symbols)

    def unsubscribe_ticks(self, symbols: Optional[List[str]] = None) -> Dict:
        return self._connection.unsubscribe(self._subscription_key, symbols)

    def get_current_tick(
        self,
        security: str,
        dt: Optional[Union[str, datetime]] = None,
        df: bool = False,
    ) -> Dict[str, Any]:
        _ = dt, df
        payload = {"security": security}
        resp = self._connection.request("data.snapshot", payload)
        return resp or {}

    def _handle_tick_event(self, payload: Dict[str, Any]) -> None:
        """规范化远程 tick 事件并按新旧回调合同安全分发。

        Args:
            payload: 远程连接推送的原始 tick 字典。

        Returns:
            None: 无回调、缺证券代码或回调异常时静默返回。

        Side Effects:
            有显式旧版上下文时调用 ``callback(context, tick)``，否则调用
            ``callback(tick)``。
        """

        callback = self._tick_callback
        if not callback:
            return
        symbol = payload.get("symbol") or payload.get("sid")
        if not symbol:
            return
        tick = {
            "sid": self._to_jq_code(symbol),
            "last_price": payload.get("last_price") or payload.get("lastPrice"),
            "dt": payload.get("dt") or payload.get("time"),
        }
        try:
            if self._tick_context is None:
                callback(tick)
            else:
                callback(self._tick_context, tick)
        except Exception:
            pass

    @staticmethod
    def _to_jq_code(symbol: str) -> str:
        if symbol.endswith(".SZ"):
            return symbol.replace(".SZ", ".XSHE")
        if symbol.endswith(".SH"):
            return symbol.replace(".SH", ".XSHG")
        return symbol
