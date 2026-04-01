from __future__ import annotations

import argparse
import asyncio
import json
import re
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from bullet_trade.broker.qmt_remote import RemoteQmtBroker
from bullet_trade.core import pricing
from bullet_trade.data.providers.remote_qmt import RemoteQmtProvider
from bullet_trade.remote import RemoteQmtConnection
from bullet_trade.utils.env_loader import get_broker_config, load_env


_DEFAULT_HISTORY_FIELDS = ["open", "close", "high", "low", "volume", "money"]
_SUMMARY_VALUE_MAXLEN = 200
_REQUIRED_FIELD_GROUPS = {
    "snapshot": [
        {"sid", "symbol"},
        {"last_price", "lastPrice", "price"},
        {"dt", "time", "datetime"},
    ],
    "tick_event": [
        {"symbol", "sid"},
        {"last_price", "lastPrice", "price"},
        {"dt", "time", "datetime"},
    ],
    "order": [
        {"order_id"},
        {"security"},
        {"status"},
        {"amount"},
        {"filled", "traded_volume"},
    ],
    "trade": [
        {"trade_id"},
        {"order_id"},
        {"security"},
        {"price", "traded_price", "trade_price"},
    ],
}


@dataclass
class ProbeConfig:
    host: str
    port: int
    token: str
    output_dir: Path
    account_key: Optional[str] = None
    sub_account_id: Optional[str] = None
    inspect_symbol: str = "000001.XSHE"
    limit_symbol: str = "159915.XSHE"
    market_symbol: str = "518880.XSHG"
    order_amount: int = 100
    tick_timeout_sec: float = 8.0


def default_output_dir() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    stamp = datetime.now().strftime("runtime_check_%Y%m%d_%H%M%S")
    return repo_root / "logs" / "runtime_checks" / "remote_qmt_probe" / stamp


def load_probe_config(
    *,
    env_file: Optional[str] = None,
    output_dir: Optional[str] = None,
    account_key: Optional[str] = None,
    sub_account_id: Optional[str] = None,
    inspect_symbol: str = "000001.XSHE",
    limit_symbol: str = "159915.XSHE",
    market_symbol: str = "518880.XSHG",
    order_amount: int = 100,
    tick_timeout_sec: float = 8.0,
) -> ProbeConfig:
    load_env(env_file, override=False)
    broker_cfg = get_broker_config().get("qmt-remote", {})
    host = broker_cfg.get("host")
    port = broker_cfg.get("port")
    token = broker_cfg.get("token")
    if not host or not port or not token:
        raise RuntimeError("缺少 QMT_SERVER_HOST/QMT_SERVER_PORT/QMT_SERVER_TOKEN，请检查 .env")
    resolved_output = Path(output_dir).expanduser() if output_dir else default_output_dir()
    return ProbeConfig(
        host=str(host),
        port=int(port),
        token=str(token),
        output_dir=resolved_output,
        account_key=account_key or broker_cfg.get("account_key"),
        sub_account_id=sub_account_id or broker_cfg.get("sub_account_id"),
        inspect_symbol=inspect_symbol,
        limit_symbol=limit_symbol,
        market_symbol=market_symbol,
        order_amount=max(1, int(order_amount)),
        tick_timeout_sec=max(1.0, float(tick_timeout_sec)),
    )


class RemoteRuntimeProbe:
    """远程 QMT 运行时探针。

    目标：
    1. 真实连接远程 server，记录当前协议/字段/数据形态。
    2. 输出可复用的 JSON/Markdown 报告，帮助补齐 stub 与测试。
    3. 在显式开启时，对限价/市价/撤单回路做最小 smoke。
    """

    def __init__(self, config: ProbeConfig) -> None:
        self.config = config
        self._conn: Optional[RemoteQmtConnection] = None
        self._provider: Optional[RemoteQmtProvider] = None
        self._broker: Optional[RemoteQmtBroker] = None
        self._step_seq = 0
        self._steps: List[Dict[str, Any]] = []
        self._observed: Dict[str, Any] = {
            "snapshot_keys": [],
            "tick_event_keys": [],
            "security_info_keys": [],
            "account_keys": [],
            "position_keys": [],
            "order_keys": [],
            "trade_keys": [],
            "minute_history_columns": [],
            "daily_history_columns": [],
            "provider_minute_columns": [],
            "provider_daily_columns": [],
        }

    def run(self, *, trade_smoke: bool = False) -> Dict[str, Any]:
        self._prepare_output_dirs()
        try:
            self._run_inspect_steps()
            if trade_smoke:
                self._run_trade_smoke_steps()
        finally:
            self.close()

        report = self._build_report(trade_smoke=trade_smoke)
        self._write_report(report)
        return report

    def close(self) -> None:
        if self._broker is not None:
            try:
                self._broker.disconnect()
            except Exception:
                pass
            self._broker = None
        if self._provider is not None:
            try:
                self._provider._connection.close()  # type: ignore[attr-defined]
            except Exception:
                pass
            self._provider = None
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _prepare_output_dirs(self) -> None:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        (self.config.output_dir / "raw").mkdir(parents=True, exist_ok=True)

    def _run_inspect_steps(self) -> None:
        health = self._run_step(
            name="admin.health",
            category="raw_rpc",
            request={"action": "admin.health", "payload": {}},
            fn=lambda: self.connection.request("admin.health", {}),
        )
        features = _extract_server_features(health)

        if "data" in features or not features:
            self._run_data_steps()
        else:
            self._record_skipped("data.*", "raw_rpc", "server 未启用 data feature")

        if "broker" in features or not features:
            self._run_broker_read_steps()
        else:
            self._record_skipped("broker.*", "broker", "server 未启用 broker feature")

    def _run_data_steps(self) -> None:
        inspect_symbol = self.config.inspect_symbol
        self._run_step(
            name="data.trade_days",
            category="raw_rpc",
            request={"action": "data.trade_days", "payload": {"count": 5}},
            fn=lambda: self.connection.request("data.trade_days", {"count": 5}),
        )
        security_info = self._run_step(
            name="data.security_info",
            category="raw_rpc",
            request={"action": "data.security_info", "payload": {"security": inspect_symbol}},
            fn=lambda: self.connection.request("data.security_info", {"security": inspect_symbol}),
        )
        self._observe_dict_keys("security_info_keys", security_info)

        snapshot = self._run_step(
            name="data.snapshot",
            category="raw_rpc",
            request={"action": "data.snapshot", "payload": {"security": inspect_symbol}},
            fn=lambda: self.connection.request("data.snapshot", {"security": inspect_symbol}),
        )
        self._observe_dict_keys("snapshot_keys", snapshot)

        minute_payload = {
            "security": inspect_symbol,
            "count": 5,
            "frequency": "1m",
            "fields": list(_DEFAULT_HISTORY_FIELDS),
            "fq": "none",
        }
        minute_history = self._run_step(
            name="data.history.minute",
            category="raw_rpc",
            request={"action": "data.history", "payload": minute_payload},
            fn=lambda: self.connection.request("data.history", minute_payload),
        )
        self._observe_dataframe_columns("minute_history_columns", minute_history)

        daily_payload = {
            "security": inspect_symbol,
            "count": 5,
            "frequency": "1d",
            "fields": list(_DEFAULT_HISTORY_FIELDS),
            "fq": "none",
        }
        daily_history = self._run_step(
            name="data.history.daily",
            category="raw_rpc",
            request={"action": "data.history", "payload": daily_payload},
            fn=lambda: self.connection.request("data.history", daily_payload),
        )
        self._observe_dataframe_columns("daily_history_columns", daily_history)

        tick_event = self._run_step(
            name="data.subscribe.tick",
            category="event",
            request={"action": "data.subscribe", "payload": {"securities": [inspect_symbol]}},
            fn=lambda: self._capture_tick_event(inspect_symbol),
        )
        self._observe_dict_keys("tick_event_keys", tick_event)

        provider_tick = self._run_step(
            name="provider.get_current_tick",
            category="provider",
            request={"security": inspect_symbol},
            fn=lambda: self.provider.get_current_tick(inspect_symbol),
        )
        if not self._observed["snapshot_keys"]:
            self._observe_dict_keys("snapshot_keys", provider_tick)

        provider_minute = self._run_step(
            name="provider.get_price.minute",
            category="provider",
            request={"security": inspect_symbol, "count": 5, "frequency": "1m"},
            fn=lambda: self.provider.get_price(
                inspect_symbol,
                count=5,
                frequency="1m",
                fields=list(_DEFAULT_HISTORY_FIELDS),
                fq="none",
            ),
        )
        self._observe_dataframe_columns("provider_minute_columns", provider_minute)

        provider_daily = self._run_step(
            name="provider.get_price.daily",
            category="provider",
            request={"security": inspect_symbol, "count": 5, "frequency": "daily"},
            fn=lambda: self.provider.get_price(
                inspect_symbol,
                count=5,
                frequency="daily",
                fields=list(_DEFAULT_HISTORY_FIELDS),
                fq="none",
            ),
        )
        self._observe_dataframe_columns("provider_daily_columns", provider_daily)

    def _run_broker_read_steps(self) -> None:
        account = self._run_step(
            name="broker.account",
            category="broker",
            request=self._base_broker_request(),
            fn=lambda: self.broker.get_account_info(),
        )
        self._observe_dict_keys("account_keys", account)

        positions = self._run_step(
            name="broker.positions",
            category="broker",
            request=self._base_broker_request(),
            fn=lambda: self.broker.get_positions(),
        )
        self._observe_list_item_keys("position_keys", positions)

        orders = self._run_step(
            name="broker.orders",
            category="broker",
            request=self._base_broker_request(),
            fn=lambda: self.broker.get_orders(),
        )
        self._observe_list_item_keys("order_keys", orders)

        trades = self._run_step(
            name="broker.trades",
            category="broker",
            request=self._base_broker_request(),
            fn=lambda: self.broker.get_trades(),
        )
        self._observe_list_item_keys("trade_keys", trades)

    def _run_trade_smoke_steps(self) -> None:
        limit_order_id = self._run_step(
            name="broker.limit_buy_cancel",
            category="trade_smoke",
            request={
                **self._base_broker_request(),
                "security": self.config.limit_symbol,
                "amount": self.config.order_amount,
                "style": "limit",
            },
            fn=self._run_limit_buy_cancel_smoke,
        )
        if isinstance(limit_order_id, dict):
            self._observe_dict_keys("order_keys", limit_order_id)

        market_result = self._run_step(
            name="broker.market_buy_cleanup",
            category="trade_smoke",
            request={
                **self._base_broker_request(),
                "security": self.config.market_symbol,
                "amount": self.config.order_amount,
                "style": "market",
            },
            fn=self._run_market_buy_cleanup_smoke,
        )
        if isinstance(market_result, dict):
            self._observe_dict_keys("order_keys", market_result.get("status") or {})

    def _run_limit_buy_cancel_smoke(self) -> Dict[str, Any]:
        symbol = self.config.limit_symbol
        live_snapshot = self._get_live_snapshot(symbol)
        base_price = (
            live_snapshot.get("last_price")
            or live_snapshot.get("lastPrice")
            or live_snapshot.get("price")
            or 0
        )
        if not base_price:
            raise RuntimeError(f"无法为 {symbol} 获取最新价，限价 smoke 无法继续")
        tick_size = pricing.get_min_price_step(symbol, float(base_price))
        low_limit = _maybe_float(live_snapshot.get("low_limit"))
        limit_price = max(float(base_price) - tick_size, tick_size)
        if low_limit and low_limit > 0:
            limit_price = max(limit_price, low_limit)
        limit_price = round(limit_price / tick_size) * tick_size
        order_id = asyncio.run(
            self.broker.buy(
                symbol,
                self.config.order_amount,
                price=limit_price,
                wait_timeout=0,
            )
        )
        status = asyncio.run(self.broker.get_order_status(order_id))
        order_row_before_cancel = self._fetch_order_row(order_id)
        cancel_response = self.connection.request(
            "broker.cancel_order",
            {**self._base_broker_request(), "order_id": order_id},
        )
        cancel_ok = _extract_cancel_ok(cancel_response)
        canceled = asyncio.run(self.broker.get_order_status(order_id))
        order_row_after_cancel = self._fetch_order_row(order_id)
        return {
            "order_id": order_id,
            "limit_price": limit_price,
            "tick_size": tick_size,
            "live_snapshot": live_snapshot,
            "status": status,
            "order_row_before_cancel": order_row_before_cancel,
            "cancel_ok": cancel_ok,
            "cancel_response": cancel_response,
            "canceled_status": canceled,
            "order_row_after_cancel": order_row_after_cancel,
        }

    def _run_market_buy_cleanup_smoke(self) -> Dict[str, Any]:
        symbol = self.config.market_symbol
        live_snapshot = self._get_live_snapshot(symbol)
        last_price = (
            live_snapshot.get("last_price")
            or live_snapshot.get("lastPrice")
            or live_snapshot.get("price")
            or 0
        )
        if not last_price:
            raise RuntimeError(f"无法为 {symbol} 获取最新价，市价 smoke 无法继续")
        requested_protect_price = pricing.compute_market_protect_price(
            symbol,
            float(last_price),
            _maybe_float(live_snapshot.get("high_limit")),
            _maybe_float(live_snapshot.get("low_limit")),
            0.015,
            True,
        )
        order_id = asyncio.run(
            self.broker.buy(
                symbol,
                self.config.order_amount,
                price=requested_protect_price,
                wait_timeout=0,
                market=True,
            )
        )
        status = asyncio.run(self.broker.get_order_status(order_id))
        order_row_before_cleanup = self._fetch_order_row(order_id)
        cleanup = {"attempted_cancel": False, "cancel_ok": None, "after_cancel_status": None}
        status_name = str(status.get("status") or "").lower()
        if status_name in {"", "new", "submitted", "open", "filling", "canceling"}:
            cleanup["attempted_cancel"] = True
            cancel_response = self.connection.request(
                "broker.cancel_order",
                {**self._base_broker_request(), "order_id": order_id},
            )
            cleanup["cancel_response"] = cancel_response
            cleanup["cancel_ok"] = _extract_cancel_ok(cancel_response)
            cleanup["after_cancel_status"] = asyncio.run(self.broker.get_order_status(order_id))
            cleanup["order_row_after_cancel"] = self._fetch_order_row(order_id)
        return {
            "order_id": order_id,
            "requested_protect_price": requested_protect_price,
            "live_snapshot": live_snapshot,
            "status": status,
            "order_row_before_cleanup": order_row_before_cleanup,
            **cleanup,
        }

    def _capture_tick_event(self, symbol: str) -> Dict[str, Any]:
        q: Queue = Queue()
        received = threading.Event()

        def _handler(payload: Dict[str, Any]) -> None:
            q.put(payload)
            received.set()

        self.connection.add_event_listener("tick", _handler)
        self.connection.subscribe("runtime-probe", [symbol])
        try:
            if not received.wait(timeout=self.config.tick_timeout_sec):
                raise TimeoutError(
                    f"订阅 {symbol} 后 {self.config.tick_timeout_sec:.1f}s 内未收到 tick 事件"
                )
            try:
                event = q.get_nowait()
            except Empty as exc:  # pragma: no cover - 极短竞态
                raise TimeoutError("tick 事件已触发但队列为空") from exc
            return event
        finally:
            try:
                self.connection.unsubscribe("runtime-probe", [symbol])
            except Exception:
                pass

    def _run_step(
        self,
        *,
        name: str,
        category: str,
        request: Optional[Dict[str, Any]],
        fn: Callable[[], Any],
    ) -> Any:
        self._step_seq += 1
        artifact_stem = f"{self._step_seq:02d}_{_slugify(name)}"
        started_at = datetime.now().isoformat(sep=" ", timespec="seconds")
        try:
            result = fn()
        except Exception as exc:
            self._steps.append(
                {
                    "name": name,
                    "category": category,
                    "status": "error",
                    "started_at": started_at,
                    "summary": str(exc),
                    "request": request,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            return None

        artifact_path = self._write_json_artifact(artifact_stem, result)
        self._steps.append(
            {
                "name": name,
                "category": category,
                "status": "ok",
                "started_at": started_at,
                "summary": _summarize_value(result),
                "request": request,
                "response_shape": _shape_of_value(result),
                "artifact": str(artifact_path),
            }
        )
        return result

    def _record_skipped(self, name: str, category: str, reason: str) -> None:
        self._steps.append(
            {
                "name": name,
                "category": category,
                "status": "skipped",
                "started_at": datetime.now().isoformat(sep=" ", timespec="seconds"),
                "summary": reason,
            }
        )

    def _write_json_artifact(self, stem: str, value: Any) -> Path:
        path = self.config.output_dir / "raw" / f"{stem}.json"
        path.write_text(
            json.dumps(_json_ready(value), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return path

    def _build_report(self, *, trade_smoke: bool) -> Dict[str, Any]:
        suggestions = self._build_suggestions()
        overall_status = "ok"
        if any(step.get("status") == "error" for step in self._steps):
            overall_status = "error"
        return {
            "generated_at": datetime.now().isoformat(sep=" ", timespec="seconds"),
            "overall_status": overall_status,
            "trade_smoke_enabled": trade_smoke,
            "server": {
                "host": self.config.host,
                "port": self.config.port,
                "account_key": self.config.account_key,
                "sub_account_id": self.config.sub_account_id,
                "token_masked": _mask_secret(self.config.token),
            },
            "symbols": {
                "inspect_symbol": self.config.inspect_symbol,
                "limit_symbol": self.config.limit_symbol,
                "market_symbol": self.config.market_symbol,
                "order_amount": self.config.order_amount,
            },
            "steps": self._steps,
            "observed_contracts": self._observed,
            "suggestions": suggestions,
        }

    def _write_report(self, report: Dict[str, Any]) -> None:
        json_path = self.config.output_dir / "probe_report.json"
        md_path = self.config.output_dir / "probe_report.md"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(_render_markdown_report(report), encoding="utf-8")

    def _build_suggestions(self) -> List[str]:
        suggestions: List[str] = []
        for contract_name, observed_key in (
            ("snapshot", "snapshot_keys"),
            ("tick_event", "tick_event_keys"),
            ("order", "order_keys"),
            ("trade", "trade_keys"),
        ):
            missing_groups = _missing_field_groups(
                self._observed.get(observed_key) or [],
                _REQUIRED_FIELD_GROUPS.get(contract_name) or [],
            )
            if missing_groups:
                pretty = " / ".join("{" + ",".join(sorted(group)) + "}" for group in missing_groups)
                suggestions.append(
                    f"{contract_name} 当前缺少关键字段组 {pretty}；"
                    f"若要让 stub 与真实接口更贴近，建议优先补齐这些字段或别名。"
                )
        minute_cols = set(self._observed.get("minute_history_columns") or [])
        if minute_cols and not {"close", "volume"}.issubset(minute_cols):
            suggestions.append(
                "分钟线返回中未稳定看到 close/volume；建议检查 server 的 data.history 字段透传。"
            )
        daily_cols = set(self._observed.get("daily_history_columns") or [])
        if daily_cols and "close" not in daily_cols:
            suggestions.append("日线返回中未看到 close；stub 和真实接口都应至少保留 close。")
        if not suggestions:
            suggestions.append("当前关键字段闭环基本齐全；下一步可以直接用 raw 响应回填 stub fixture。")
        return suggestions

    def _observe_dict_keys(self, bucket: str, value: Any) -> None:
        keys = _extract_dict_keys(value)
        if keys:
            self._observed[bucket] = sorted(set(self._observed.get(bucket) or []).union(keys))

    def _observe_list_item_keys(self, bucket: str, value: Any) -> None:
        keys = _extract_list_item_keys(value)
        if keys:
            self._observed[bucket] = sorted(set(self._observed.get(bucket) or []).union(keys))

    def _observe_dataframe_columns(self, bucket: str, value: Any) -> None:
        cols = _extract_dataframe_columns(value)
        if cols:
            self._observed[bucket] = sorted(set(self._observed.get(bucket) or []).union(cols))

    def _base_broker_request(self) -> Dict[str, Any]:
        return {
            "account_key": self.config.account_key,
            "sub_account_id": self.config.sub_account_id,
        }

    def _get_live_snapshot(self, symbol: str) -> Dict[str, Any]:
        try:
            result = self.connection.request("data.live_current", {"security": symbol})
            if isinstance(result, dict) and result:
                return result
        except Exception:
            pass
        try:
            result = self.connection.request("data.snapshot", {"security": symbol})
            if isinstance(result, dict):
                return result
        except Exception:
            pass
        return {}

    def _fetch_order_row(self, order_id: str) -> Dict[str, Any]:
        try:
            rows = self.broker.get_orders(order_id=order_id)
        except Exception:
            return {}
        if isinstance(rows, list) and rows:
            return rows[0]
        return {}

    @property
    def connection(self) -> RemoteQmtConnection:
        if self._conn is None:
            self._conn = RemoteQmtConnection(self.config.host, self.config.port, self.config.token)
            self._conn.start()
        return self._conn

    @property
    def provider(self) -> RemoteQmtProvider:
        if self._provider is None:
            self._provider = RemoteQmtProvider(
                {
                    "host": self.config.host,
                    "port": self.config.port,
                    "token": self.config.token,
                }
            )
        return self._provider

    @property
    def broker(self) -> RemoteQmtBroker:
        if self._broker is None:
            self._broker = RemoteQmtBroker(
                account_id=self.config.account_key or "remote",
                config={
                    "host": self.config.host,
                    "port": self.config.port,
                    "token": self.config.token,
                    "account_key": self.config.account_key,
                    "sub_account_id": self.config.sub_account_id,
                },
            )
            self._broker.connect()
        return self._broker


def _extract_server_features(value: Any) -> List[str]:
    if not isinstance(value, dict):
        return []
    inner = value.get("value") if isinstance(value.get("value"), dict) else value
    features = inner.get("features")
    if isinstance(features, list):
        return [str(item) for item in features]
    return []


def _mask_secret(value: Optional[str]) -> str:
    if not value:
        return ""
    if len(value) <= 6:
        return "*" * len(value)
    return f"{value[:3]}***{value[-3:]}"


def _json_ready(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    try:
        import pandas as pd  # type: ignore

        if isinstance(value, pd.DataFrame):
            return {
                "type": "dataframe",
                "shape": [int(value.shape[0]), int(value.shape[1])],
                "columns": [str(col) for col in value.columns],
                "records_preview": value.head(5).to_dict(orient="records"),
            }
    except Exception:
        pass
    return str(value)


def _shape_of_value(value: Any) -> Dict[str, Any]:
    if value is None:
        return {"type": "none"}
    if isinstance(value, dict):
        if value.get("dtype") == "dataframe":
            columns = list(value.get("columns") or [])
            records = list(value.get("records") or [])
            return {
                "type": "dataframe_payload",
                "columns": columns,
                "rows": len(records),
            }
        return {"type": "dict", "keys": sorted(value.keys())[:30]}
    if isinstance(value, list):
        sample_keys = sorted(value[0].keys())[:30] if value and isinstance(value[0], dict) else []
        return {"type": "list", "len": len(value), "sample_keys": sample_keys}
    try:
        import pandas as pd  # type: ignore

        if isinstance(value, pd.DataFrame):
            return {
                "type": "dataframe",
                "rows": int(value.shape[0]),
                "columns": [str(col) for col in value.columns],
            }
    except Exception:
        pass
    return {"type": type(value).__name__}


def _summarize_value(value: Any) -> str:
    shape = _shape_of_value(value)
    if shape["type"] in {"dataframe_payload", "dataframe"}:
        return f"{shape['type']} rows={shape.get('rows', 0)} cols={len(shape.get('columns', []))}"
    if shape["type"] == "dict":
        keys = ",".join(shape.get("keys") or [])
        return f"dict keys=[{keys}]"
    if shape["type"] == "list":
        return f"list len={shape.get('len', 0)} sample_keys={shape.get('sample_keys', [])}"
    text = str(value)
    return text if len(text) <= _SUMMARY_VALUE_MAXLEN else text[:_SUMMARY_VALUE_MAXLEN] + "..."


def _extract_dict_keys(value: Any) -> List[str]:
    if not isinstance(value, dict):
        return []
    if isinstance(value.get("value"), dict):
        return sorted({str(key) for key in value.get("value", {}).keys()} | {str(key) for key in value.keys() if key != "value"})
    return sorted(str(key) for key in value.keys())


def _extract_list_item_keys(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    keys = set()
    for item in value:
        if isinstance(item, dict):
            keys.update(str(key) for key in item.keys())
    return sorted(keys)


def _extract_dataframe_columns(value: Any) -> List[str]:
    if isinstance(value, dict) and value.get("dtype") == "dataframe":
        return [str(col) for col in value.get("columns") or []]
    try:
        import pandas as pd  # type: ignore

        if isinstance(value, pd.DataFrame):
            return [str(col) for col in value.columns]
    except Exception:
        pass
    return []


def _missing_field_groups(observed: Iterable[str], groups: Sequence[Iterable[str]]) -> List[set[str]]:
    observed_set = {str(item) for item in observed}
    missing: List[set[str]] = []
    for group in groups:
        group_set = {str(item) for item in group}
        if observed_set.isdisjoint(group_set):
            missing.append(group_set)
    return missing


def _maybe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_cancel_ok(value: Any) -> bool:
    if isinstance(value, dict):
        raw = value.get("value")
        if isinstance(raw, bool):
            return raw
        if raw is None:
            return bool(value.get("success", True))
        return bool(raw)
    return bool(value)


def _render_markdown_report(report: Dict[str, Any]) -> str:
    lines = [
        "# Remote QMT Runtime Probe",
        "",
        f"- 生成时间: {report.get('generated_at')}",
        f"- 总状态: {report.get('overall_status')}",
        f"- Host: {report.get('server', {}).get('host')}:{report.get('server', {}).get('port')}",
        f"- Account: {report.get('server', {}).get('account_key') or '-'}",
        f"- SubAccount: {report.get('server', {}).get('sub_account_id') or '-'}",
        f"- Trade Smoke: {report.get('trade_smoke_enabled')}",
        "",
        "## Steps",
        "",
    ]
    for step in report.get("steps") or []:
        lines.append(
            f"- [{step.get('status')}] {step.get('name')}: {step.get('summary')}"
        )
        if step.get("artifact"):
            lines.append(f"  artifact: {step.get('artifact')}")
    lines.extend(["", "## Observed Contracts", ""])
    observed = report.get("observed_contracts") or {}
    for key in sorted(observed.keys()):
        value = observed.get(key) or []
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Suggestions", ""])
    for item in report.get("suggestions") or []:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def _slugify(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower() or "step"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="探测远程 bullet-trade server 的实时/交易接口返回形态。")
    parser.add_argument("--env-file", default=None, help="可选 .env 路径，默认从当前目录向上查找")
    parser.add_argument("--output-dir", default=None, help="报告输出目录")
    parser.add_argument("--inspect-symbol", default="000001.XSHE", help="默认检查标的")
    parser.add_argument("--limit-symbol", default="159915.XSHE", help="限价 smoke 标的")
    parser.add_argument("--market-symbol", default="518880.XSHG", help="市价 smoke 标的")
    parser.add_argument("--order-amount", type=int, default=100, help="交易 smoke 数量")
    parser.add_argument("--tick-timeout-sec", type=float, default=8.0, help="tick 订阅等待秒数")
    parser.add_argument("--account-key", default=None, help="覆盖 .env 中的 QMT_SERVER_ACCOUNT_KEY")
    parser.add_argument("--sub-account-id", default=None, help="覆盖 .env 中的 QMT_SERVER_SUB_ACCOUNT")
    parser.add_argument(
        "--trade-smoke",
        action="store_true",
        help="显式开启限价/市价/撤单 smoke。默认仅做只读 inspect。",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    config = load_probe_config(
        env_file=args.env_file,
        output_dir=args.output_dir,
        account_key=args.account_key,
        sub_account_id=args.sub_account_id,
        inspect_symbol=args.inspect_symbol,
        limit_symbol=args.limit_symbol,
        market_symbol=args.market_symbol,
        order_amount=args.order_amount,
        tick_timeout_sec=args.tick_timeout_sec,
    )
    probe = RemoteRuntimeProbe(config)
    report = probe.run(trade_smoke=bool(args.trade_smoke))
    print(f"报告已输出到: {config.output_dir}")
    print(f"整体状态: {report.get('overall_status')}")
    return 0 if report.get("overall_status") == "ok" else 1


__all__ = [
    "ProbeConfig",
    "RemoteRuntimeProbe",
    "build_arg_parser",
    "default_output_dir",
    "load_probe_config",
    "main",
]
