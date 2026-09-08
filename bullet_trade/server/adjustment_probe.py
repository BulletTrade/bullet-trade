"""大 QMT 复权仿真取数工具；作者：BruceLee。

输入：明确的证券、历史窗口、周期，以及现有 helper 地址和本机环境文件。
输出：包含原始 HTTP 数据、查询参数和失败记录的 JSON 文件，不覆盖已有文件。
上下游：仅查询现有大 QMT helper，为离线复权核验提供证据，不接入 data.history 主路径。
约定：不调用交易、订阅、独立下载或启停接口；现有 history 接口可能自行补齐缓存。
认证只从环境文件/环境变量读取，不在报告中输出；不加载聚宽或 MiniQMT 数据源。
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence
from urllib.parse import urlsplit

__all__ = ["capture_adjustment_inputs", "summarize_capture", "main"]

MODES = ("none", "front", "front_ratio", "back", "back_ratio")
FREQUENCIES = ("1m", "5m", "15m", "30m", "60m", "1d")
FIELDS = ("open", "high", "low", "close", "volume", "money", "preClose")
_HEALTH_FIELDS = (
    "ready",
    "qmt_api_ready",
    "build",
    "version",
    "pid",
    "gateway_build_id",
    "dividend_event_schema",
)
_EVENT_FACT_FIELDS = {
    "cash_per_share",
    "gift",
    "transfer",
    "rights",
    "rights_price",
    "share_reform",
    "qmt_dr",
}
_ERROR_CODES = {
    "BAD_REQUEST",
    "QMT_API_NOT_READY",
    "HISTORY_FAILED",
    "SPLIT_DIVIDEND_FAILED",
    "SPLIT_DIVIDEND_INVALID_DATA",
    "ENSURE_CACHE_FAILED",
    "HTTP_401",
    "HTTP_403",
    "HTTP_404",
    "BIG_QMT_GATEWAY_ERROR",
    "BIG_QMT_GATEWAY_UNAVAILABLE",
    "BIG_QMT_GATEWAY_TIMEOUT",
}


def _error_summary(exc: Exception) -> Dict[str, str]:
    """脱敏异常信息；输入异常，返回类型及白名单错误码，不输出正文或服务端任意code。"""

    code = getattr(exc, "code", "")
    return {
        "type": type(exc).__name__,
        "code": code if isinstance(code, str) and code in _ERROR_CODES else "UNCLASSIFIED",
    }


def _json_text(value: Any) -> str:
    """生成严格JSON证据；输入报告，返回文本，将非有限浮点转成明确标签，不改输入。

    在打开输出文件前完成序列化，避免NaN/Inf导致半份文件。标签保留非法值位置供诊断。
    """

    def safe(item: Any) -> Any:
        """递归转换JSON值；输入任意子项，返回新值或原标量，无外部副作用。"""

        if isinstance(item, float) and not math.isfinite(item):
            return {"invalid_number": repr(item)}
        if isinstance(item, dict):
            return {key: safe(child) for key, child in item.items()}
        if isinstance(item, (tuple, list)):
            return [safe(child) for child in item]
        return item

    return json.dumps(safe(value), ensure_ascii=True, allow_nan=False, indent=2)


def _wire_times(value: Dict[str, Any]) -> Sequence[datetime]:
    """校验单证券wire行情的时间轴；输入响应，返回递增时间序列，缺失/重复/乱序抛错。"""

    columns = value.get("columns", [])
    if len(columns) != len(set(columns)):
        raise ValueError("行情字段重复")
    indexes = value.get("index_columns") or [
        name for name in ("time", "stime", "date", "datetime", "index") if name in columns
    ]
    if len(indexes) != 1 or indexes[0] not in columns:
        raise ValueError("缺少唯一行情时间列")
    position = columns.index(indexes[0])
    times = []
    for row in value.get("records", []):
        text = str(row[position])
        if re.fullmatch(r"\d{8}|\d{14}", text):
            stamp = datetime.strptime(text, "%Y%m%d" if len(text) == 8 else "%Y%m%d%H%M%S")
        elif re.fullmatch(r"\d{13}", text):
            stamp = datetime.fromtimestamp(int(text) / 1000, timezone.utc)
        else:
            stamp = datetime.fromisoformat(text)
        if stamp.tzinfo is not None:
            stamp = stamp.astimezone(timezone(timedelta(hours=8))).replace(tzinfo=None)
        times.append(stamp)
    if times != sorted(set(times)):
        raise ValueError("行情时间轴重复或乱序")
    return times


def _timestamp(value: str) -> datetime:
    """将 ISO 日期或时间解析为中国市场本地时间；返回 datetime，非法值抛 ValueError。

    输入必须不含时区，避免诊断窗口被隐式移动；本函数没有外部访问或状态修改。
    """

    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        raise ValueError("窗口必须使用不带时区的中国市场日期/时间")
    return parsed


def _validate_request(
    securities: Sequence[str],
    start: str,
    end: str,
    reference_date: str,
    frequencies: Sequence[str],
    modes: Sequence[str],
) -> None:
    """校验显式诊断范围；输入为证券/时间/周期/复权模式，无返回，非法参数抛 ValueError。

    仅允许固定行情查询维度，不访问网络，不替调用者扩展证券或时间范围。
    """

    if not securities or any(
        not re.fullmatch(r"\d{6}\.(SH|SZ|XSHG|XSHE)", symbol) for symbol in securities
    ):
        raise ValueError("必须指定沪深股票/ETF代码")
    if len(set(securities)) != len(securities):
        raise ValueError("证券不能重复")
    if _timestamp(start) > _timestamp(end):
        raise ValueError("start 不能晚于 end")
    ref = date.fromisoformat(reference_date)
    if ref < _timestamp(end).date():
        raise ValueError("此采集器仅支持参考日不早于历史窗口末日；反向锚定使用离线单测")
    if not frequencies or not set(frequencies).issubset(FREQUENCIES):
        raise ValueError("诊断周期必须为 1m/5m/15m/30m/60m/1d")
    if not modes or not set(modes).issubset(MODES):
        raise ValueError("诊断只允许 none/front/front_ratio/back/back_ratio")
    if len(set(frequencies)) != len(frequencies) or len(set(modes)) != len(modes):
        raise ValueError("周期及模式不能重复")


def capture_adjustment_inputs(
    request: Callable[[str, Optional[Dict[str, Any]]], Any],
    *,
    securities: Sequence[str],
    start: str,
    end: str,
    reference_date: str,
    frequencies: Sequence[str] = ("1d",),
    modes: Sequence[str] = MODES,
) -> Dict[str, Any]:
    """顺序采集仿真 helper 的行情与事件，返回原始证据字典。

    request 接受路径和载荷；None 载荷表示 GET，其余表示 POST。仅调用 health、
    history、split_dividend。start/end/ref 为显式窗口；额外日线从 start 前14天取至ref，
    仅为获取窗口首个事件的前收盘，不能证明全历史事件完整。事件查询读取截至ref的历史。
    失败记录异常类型及错误码，不保存异常正文以避免认证信息泄漏。无重试、不并发轰击QMT。
    """

    _validate_request(securities, start, end, reference_date, frequencies, modes)
    report: Dict[str, Any] = {
        "schema": "big-qmt-adjustment-capture/v1",
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "host": platform.node(),
        "securities": list(securities),
        "start": start,
        "end": end,
        "reference_date": reference_date,
        "cases": [],
        "errors": [],
        "notes": [
            "仅行情查询；现有history接口可能自行补缓存，无独立下载、订阅或交易",
            "原生复权未必锚定reference_date；该日期只限定依赖数据，不能视为原生已重锚",
            "事件接口成功或非空不能证明上市起完整；压缩事件不得当成完整事实",
            "采集成功不等于复权价格或信号验收通过",
        ],
    }
    health = request("/health", None)
    if not isinstance(health, dict):
        raise ValueError("helper health 格式无效")
    report["health_before"] = {key: health[key] for key in _HEALTH_FIELDS if key in health}
    if health.get("ready") is not True or health.get("qmt_api_ready") is False:
        raise ValueError("helper 尚未准备好，不继续请求行情")
    support_start = (_timestamp(start).date() - timedelta(days=14)).isoformat()

    def collect(kind: str, path: str, payload: Dict[str, Any]) -> None:
        """记录一次只读请求的载荷与响应；输入分类/路径/参数，无返回，错误存入报告。

        唯一外部副作用是调用注入的读取函数；失败不自动重试，也不打印异常正文。
        """

        case = {"kind": kind, "path": path, "request": payload}
        report["cases"].append(case)
        try:
            value = request(path, payload)
            if not isinstance(value, dict):
                raise ValueError("数据响应不是字典")
            case["response"] = value
            if kind == "events":
                events = value.get("events")
                if not isinstance(events, list):
                    raise ValueError("事件响应缺少events列表")
                case["event_fields_complete"] = bool(events) and all(
                    isinstance(event, dict) and _EVENT_FACT_FIELDS.issubset(event)
                    for event in events
                )
                case["history_completeness_verified"] = False
            else:
                columns = value.get("columns", [])
                records = value.get("records", [])
                required = {"open", "high", "low", "close", "volume", "money"}
                if not required.issubset(columns) or not records:
                    raise ValueError("行情为空或缺少OHLC/量额")
                for row in records:
                    if len(row) != len(columns):
                        raise ValueError("行情列和行长度不一致")
                    for field in required:
                        number = float(row[columns.index(field)])
                        if not math.isfinite(number):
                            raise ValueError("行情含非有限数据")
                        if (field in {"volume", "money"} and number < 0) or (
                            payload["fq"] == "none"
                            and field in {"open", "high", "low", "close"}
                            and number <= 0
                        ):
                            raise ValueError("行情含非法价格或量额")
                times = _wire_times(value)
                case["first_time"] = times[0].isoformat()
                case["last_time"] = times[-1].isoformat()
                case["expected_calendar_coverage_verified"] = False
        except Exception as exc:
            error = _error_summary(exc)
            case["error"] = error
            report["errors"].append({"kind": kind, "request": payload, **error})

    for security in securities:
        collect("events", "/data/split_dividend", {"security": security, "end": reference_date})
        collect(
            "support_daily",
            "/data/history",
            {
                "security": security,
                "start": support_start,
                "end": reference_date,
                "frequency": "1d",
                "fq": "none",
                "fields": list(FIELDS),
                "subscribe": False,
                "fill_data": False,
            },
        )
        for frequency in frequencies:
            for mode in modes:
                collect(
                    "history",
                    "/data/history",
                    {
                        "security": security,
                        "start": start,
                        "end": end,
                        "frequency": frequency,
                        "fq": mode,
                        "fields": list(FIELDS),
                        "subscribe": False,
                        "fill_data": False,
                    },
                )
    try:
        health_after = request("/health", None)
        if not isinstance(health_after, dict):
            raise ValueError("结束health格式无效")
        report["health_after"] = {
            key: health_after[key] for key in _HEALTH_FIELDS if key in health_after
        }
        if health_after.get("ready") is not True or health_after.get("qmt_api_ready") is False:
            raise ValueError("结束时helper未就绪")
    except Exception as exc:
        report["errors"].append({"kind": "health_after", **_error_summary(exc)})
    report["completed_at"] = datetime.now(timezone.utc).isoformat()
    report["capture_ok"] = not report["errors"]
    report["adjustment_accepted"] = False
    return report


def summarize_capture(report: Dict[str, Any]) -> Dict[str, Any]:
    """将原始报告缩成行数、字段和错误摘要；输入报告，返回新字典，不修改输入或访问网络。"""

    rows = []
    for case in report.get("cases", []):
        request = case["request"]
        response = case.get("response", {})
        records = response.get("records", [])
        events = response.get("events", [])
        rows.append(
            {
                "kind": case["kind"],
                "security": request["security"],
                "frequency": request.get("frequency"),
                "fq": request.get("fq"),
                "rows": len(records) if isinstance(records, list) else None,
                "columns": response.get("columns", []),
                "events": len(events) if isinstance(events, list) else None,
                "event_fields_complete": case.get("event_fields_complete"),
                "error": case.get("error"),
            }
        )
    return {
        "capture_ok": report["capture_ok"],
        "adjustment_accepted": False,
        "cases": rows,
        "errors": report["errors"],
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    """执行显式命令行采集并写新JSON文件；输入argv，返回退出码0或2。

    网络只访问现有helper，凭据仅从本机env读取；输出存在则拒绝覆盖。不创建服务或修改环境文件。
    """

    parser = argparse.ArgumentParser(description="大QMT复权只读取数，不执行策略或交易")
    parser.add_argument("--gateway-url", default="http://127.0.0.1:9000")
    parser.add_argument("--env-file")
    parser.add_argument("--security", action="append", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--reference-date", required=True)
    parser.add_argument("--frequency", action="append", choices=FREQUENCIES)
    parser.add_argument("--mode", action="append", choices=MODES)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("输出文件已存在，拒绝覆盖")
    if not args.output.parent.is_dir():
        parser.error("输出目录不存在，请指定已有诊断目录")
    url = urlsplit(args.gateway_url)
    if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password:
        parser.error("gateway-url必须为不含认证信息的HTTP(S)地址")
    from dotenv import dotenv_values

    from bullet_trade.server.adapters.big_qmt import BigQmtGatewayClient, BigQmtGatewayConfig
    from bullet_trade.utils.env_loader import get_env

    env = dotenv_values(args.env_file) if args.env_file else {}
    client = BigQmtGatewayClient(
        BigQmtGatewayConfig(
            base_url=args.gateway_url,
            password=env.get("BIG_QMT_GATEWAY_PASSWORD") or get_env("BIG_QMT_GATEWAY_PASSWORD"),
            secret=env.get("BIG_QMT_GATEWAY_SECRET") or get_env("BIG_QMT_GATEWAY_SECRET"),
            timeout_seconds=120.0,
        )
    )

    def request(path: str, payload: Optional[Dict[str, Any]]) -> Any:
        """调用三个允许的helper读取路径；输入路径/参数，返回JSON或抛异常，不接受交易路由。"""

        if path not in {"/health", "/data/history", "/data/split_dividend"}:
            raise ValueError("诊断拒绝非行情路径")
        return client.request_json(path, payload, "GET" if payload is None else "POST")

    try:
        report = capture_adjustment_inputs(
            request,
            securities=args.security,
            start=args.start,
            end=args.end,
            reference_date=args.reference_date,
            frequencies=args.frequency or ["1d"],
            modes=args.mode or MODES,
        )
    except Exception as exc:
        report = {
            "schema": "big-qmt-adjustment-capture/v1",
            "capture_ok": False,
            "adjustment_accepted": False,
            "cases": [],
            "errors": [{"kind": "capture_start", **_error_summary(exc)}],
        }
    rendered = _json_text(report)
    with args.output.open("x", encoding="utf-8") as stream:
        stream.write(rendered)
    print(json.dumps(summarize_capture(report), ensure_ascii=True, allow_nan=False))
    return 0 if report["capture_ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
