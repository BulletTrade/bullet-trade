from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd

from .base import DataProvider
from ...remote import RemoteQmtConnection


def _env(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(key, default)


def _dataframe_from_payload(payload: Dict[str, Any]) -> pd.DataFrame:
    if not payload or payload.get("dtype") != "dataframe":
        return pd.DataFrame()
    columns = payload.get("columns") or []
    records = payload.get("records") or []
    df = pd.DataFrame(records, columns=columns)
    return df


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
        self._tick_callback: Optional[Callable[[Any, Dict[str, Any]], None]] = None
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

        def _str_format(date_obj):
            if date_obj and isinstance(date_obj, datetime):
                return date_obj.strftime('%Y-%m-%d %H:%M:%S' if 'minute' in frequency else '%Y-%m-%d')
            return date_obj

        payload = {
            "security": security if isinstance(security, str) else ",".join(security),
            "start": _str_format(start_date),
            "end": _str_format(end_date),
            "frequency": frequency,
            "fields": fields,
            "fq": fq,
            "count": count,
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
        return resp.get("value") or {}

    def set_tick_callback(self, callback: Callable[[Any, Dict[str, Any]], None], context: Any) -> None:
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
