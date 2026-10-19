"""Bullet Data 本地只读标准 API 适配。

作者: BruceLee
职责: 将固定 manifest 的 canonical Parquet 转换为 BulletTrade/聚宽形状。
输入: 本地工作区、行情目录、数据版本及标准行情查询参数。
输出: 行情、交易日、证券信息或明确的覆盖异常；不发布数据、不执行交易。
上下游: 研究/回测显式装配本 provider，存储及文件摘要校验复用 bullet-data。
约定: 不导入外部 SDK、不联网；缺字段不补零，不将未知停牌状态当正常。
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from .base import DataProvider

__all__ = ["LocalBulletDataProvider"]

DEFAULT_FIELDS = ["open", "close", "high", "low", "volume", "money"]
PRICE_FIELDS = ["open", "close", "high", "low", "avg", "pre_close", "high_limit", "low_limit"]


def _timestamp(value: Any) -> pd.Timestamp:
    """规范日期到上海本地时间；输入日期，返回无时区时间，非法或空值抛 ValueError。"""
    parsed = pd.Timestamp(value)
    if pd.isna(parsed):
        raise ValueError("查询日期不能为空")
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert("Asia/Shanghai").tz_localize(None)
    return parsed


def _count(value: Optional[int]) -> Optional[int]:
    """校验条数；输入可选整数，返回原值，布尔、非整数或非正值抛 ValueError。"""
    if value is not None and (type(value) is not int or value <= 0):
        raise ValueError("count 必须为正整数")
    return value


class LocalBulletDataProvider(DataProvider):
    """绑定单一数据版本的只读适配器，无外部行情或凭据依赖。"""

    name = "bullet_data"
    strict_query_errors = True

    def __init__(self, config: Dict[str, Any]) -> None:
        """输入工作区/行情目录/版本配置；固定 manifest，缺数据或依赖时明确失败。"""
        from bullet_data.provider import LocalDataProvider

        if not isinstance(config, dict) or not config.get("workspace_root"):
            raise ValueError("本地数据需要指定工作目录")
        self.local = LocalDataProvider(
            config["workspace_root"],
            market_data_root=config.get("market_data_root"),
            manifest_id=config.get("manifest_id"),
        )
        self.manifest_id = self.local.resolved_manifest_id

    def _missing(self, object_name: str, code: str, start: str, end: str, reason: str) -> None:
        """输入对象/证券/区间/原因，抛结构化本地覆盖异常；不尝试其他数据源。"""
        from bullet_data.provider import CoverageGap, CoverageReport, LocalCoverageError

        raise LocalCoverageError(
            CoverageReport(
                False,
                self.manifest_id,
                "BULLET_DATA_LOCAL",
                (CoverageGap(object_name, code, start, end, reason),),
                (object_name,),
            )
        )

    def _calendar(self, start: Any = None, end: Any = None) -> pd.DataFrame:
        """读取上海日历并核验工作日覆盖；输入可选区间，返回排序副本或覆盖异常。"""
        frame = self.local.query(object_name="trade_calendar", filters={"market": "XSHG"})
        if frame.empty or not {"trade_date", "is_open"}.issubset(frame.columns):
            self._missing("trade_calendar", "*", "", "", "MISSING_TRADE_CALENDAR")
        frame = frame.sort_values("trade_date").copy()
        first = _timestamp(start) if start is not None else _timestamp(frame.trade_date.min())
        last = _timestamp(end) if end is not None else _timestamp(frame.trade_date.max())
        if first.normalize() > last.normalize():
            raise ValueError("开始日期不能晚于结束日期")
        expected = set(pd.bdate_range(first.normalize(), last.normalize()).strftime("%Y-%m-%d"))
        actual = set(frame.trade_date.astype(str))
        missing = sorted(expected - actual)
        if missing:
            self._missing("trade_calendar", "*", missing[0], missing[-1], "MISSING_TRADE_CALENDAR")
        flags = frame.is_open
        if flags.isna().any() or not flags.isin([True, False, 0, 1]).all():
            self._missing(
                "trade_calendar", "*", str(first.date()), str(last.date()), "UNKNOWN_OPEN_STATE"
            )
        values = pd.to_datetime(frame.trade_date)
        return frame[(values >= first.normalize()) & (values <= last.normalize())].copy()

    def get_trade_days(self, start_date=None, end_date=None, count=None) -> List[datetime]:
        """输入日期区间或条数，返回开市日；不足条数或日历缺口明确失败，无副作用。"""
        count = _count(count)
        if count is not None and start_date is not None:
            raise ValueError("不能同时指定 start_date 和 count")
        calendar = self._calendar(start_date, end_date)
        days = calendar.loc[calendar.is_open.astype(bool), "trade_date"].tolist()
        if count is not None:
            if len(days) < count:
                self._missing("trade_calendar", "*", "", str(end_date or ""), "INSUFFICIENT_COUNT")
            days = days[-count:]
        return [pd.Timestamp(item).to_pydatetime() for item in days]

    def _master(self, codes=None, date=None) -> pd.DataFrame:
        """输入可选证券与历史日期，返回主数据；不虚构上市日、名称或证券类型。"""
        frame = self.local.query(object_name="instrument_master", codes=codes)
        required = {"canonical_code", "display_name", "security_type"}
        if frame.empty or not required.issubset(frame.columns):
            self._missing(
                "instrument_master", ",".join(codes or ["*"]), "", "", "MISSING_INSTRUMENT_MASTER"
            )
        if date is not None:
            day = str(_timestamp(date).date())
            if "list_date" not in frame or frame.list_date.isna().any():
                self._missing("instrument_master", "*", day, day, "MISSING_LIST_DATE")
            frame = frame[frame.list_date.astype(str) <= day]
            if "delist_date" in frame:
                frame = frame[frame.delist_date.isna() | (frame.delist_date.astype(str) >= day)]
        return frame.rename(
            columns={"list_date": "start_date", "delist_date": "end_date", "security_type": "type"}
        ).copy()

    def get_all_securities(self, types="stock", date=None) -> pd.DataFrame:
        """输入证券类型和日期，返回以代码为索引的主数据；缺失必要历史字段抛异常。"""
        frame = self._master(date=date)
        wanted = [types] if isinstance(types, str) else list(types or [])
        if wanted:
            frame = frame[frame.type.isin(wanted)]
        return frame.set_index("canonical_code")

    def get_security_info(self, security: str, date=None) -> Dict[str, Any]:
        """输入证券和日期，返回元数据；证券不存在或历史不可见时抛覆盖异常。"""
        frame = self._master(codes=[security], date=date)
        if frame.empty:
            self._missing(
                "instrument_master",
                security,
                str(date or ""),
                str(date or ""),
                "SECURITY_NOT_VISIBLE",
            )
        output = frame.iloc[0].to_dict()
        output["code"] = output.pop("canonical_code")
        for column in ("start_date", "end_date"):
            if column in output:
                output[column] = (
                    None if pd.isna(output[column]) else _timestamp(output[column]).date()
                )
        return output

    def get_index_stocks(self, index_symbol: str, date=None) -> List[str]:
        """按生效区间查询指数成份；输入指数/日期，返回代码，缺对象不伪装空成份。"""
        if date is None:
            raise ValueError("本地指数成份查询需要指定日期")
        frame = self.local.query(object_name="index_members", filters={"index_code": index_symbol})
        if frame.empty or not {"canonical_code", "start_date", "end_date"}.issubset(frame):
            self._missing(
                "index_members", index_symbol, str(date), str(date), "MISSING_INDEX_MEMBERS"
            )
        day = str(_timestamp(date).date())
        selected = frame[
            (frame.start_date.astype(str) <= day)
            & (frame.end_date.isna() | (frame.end_date.astype(str) >= day))
        ]
        return sorted(selected.canonical_code.unique().tolist())

    def get_split_dividend(
        self, security: str, start_date=None, end_date=None
    ) -> List[Dict[str, Any]]:
        """查询已发布公司行动；输入证券/日期区间，返回原始事实，不推测无事件证券。"""
        manifest = self.local._manifest()
        if not manifest.partitions_for("corporate_actions"):
            self._missing(
                "corporate_actions",
                security,
                str(start_date or ""),
                str(end_date or ""),
                "MISSING_CORPORATE_ACTIONS",
            )
        frame = self.local.query(
            object_name="corporate_actions", codes=[security], start=start_date, end=end_date
        )
        return frame.rename(columns={"action_date": "date"}).to_dict("records")

    def get_price(
        self,
        security: Union[str, List[str]],
        start_date=None,
        end_date=None,
        frequency="daily",
        fields=None,
        skip_paused=False,
        fq="pre",
        count=None,
        panel=True,
        fill_paused=True,
        pre_factor_ref_date=None,
        prefer_engine=False,
        force_no_engine=False,
    ) -> pd.DataFrame:
        """读取固定版日/分钟行情；输入标准参数，返回单表/长表/宽表，缺口不回退。

        count 不越过缺行，停牌仅按显式状态处理。当前前复权/原始价可用；
        后复权缺少统一原点合同，明确未实现而非返回伪后复权。无写入副作用。
        """
        from bullet_data.adjust import apply_adjustment, normalize_fq
        from bullet_data.coverage import expected_trade_minutes
        from bullet_data.provider import LocalCoverageError

        count = _count(count)
        if count is not None and start_date is not None:
            raise ValueError("不能同时指定 start_date 和 count")
        codes = [security] if isinstance(security, str) else list(dict.fromkeys(security))
        if not codes or any(not isinstance(item, str) or not item for item in codes):
            raise ValueError("请指定证券代码")
        if skip_paused and len(codes) != 1:
            raise ValueError("skip_paused=True 仅支持单证券")
        mode = normalize_fq(fq)
        if mode == "post":
            raise NotImplementedError("本地后复权尚未提供统一原点合同，请使用前复权或原始价")
        normalized = {"daily": "1d", "minute": "1m", "1d": "1d", "1m": "1m"}.get(frequency)
        if normalized is None:
            raise NotImplementedError(f"本地数据暂不支持频率 {frequency}")
        requested = list(fields) if fields is not None else list(DEFAULT_FIELDS)
        if not requested or any(not isinstance(field, str) for field in requested):
            raise ValueError("请指定有效行情字段")
        calendar = self._calendar(start_date, end_date)
        if end_date is None:
            end_date = str(calendar.trade_date.max())
        end = _timestamp(end_date)
        if normalized == "1m" and isinstance(end_date, str) and len(end_date) == 10:
            end = end.normalize() + pd.Timedelta(hours=23, minutes=59, seconds=59)
        start = (
            _timestamp(start_date)
            if start_date is not None
            else _timestamp(calendar.trade_date.min())
        )
        open_days = calendar.loc[calendar.is_open.astype(bool), "trade_date"].tolist()
        if normalized == "1d":
            slots = open_days
            object_name, time_column = "price_bars_1d", "trade_date"
        else:
            slots, _ = expected_trade_minutes(calendar, start_dt=str(start), end_dt=str(end))
            object_name, time_column = "price_bars_1m", "trade_time"
        frames = {}
        for code in codes:
            if not slots and count is None:
                frames[code] = pd.DataFrame(columns=requested, index=pd.DatetimeIndex([]))
                continue
            frame = self.local.query(
                object_name=object_name,
                codes=[code],
                start=str(start.date()) if normalized == "1d" else str(start),
                end=str(end.date()) if normalized == "1d" else str(end),
            )
            if time_column not in frame:
                self._missing(object_name, code, str(start), str(end), "MISSING_BARS")
            chosen = list(slots)
            if count is not None:
                chosen = []
                indexed = frame.set_index(time_column)
                for slot in reversed(slots):
                    if slot not in indexed.index:
                        self._missing(object_name, code, slot, slot, "MISSING_BAR")
                    row = indexed.loc[slot]
                    if skip_paused:
                        if (
                            "paused" not in row
                            or pd.isna(row["paused"])
                            or row["paused"] not in (0, 1, True, False)
                        ):
                            self._missing(object_name, code, slot, slot, "UNKNOWN_PAUSED_STATE")
                        if bool(row["paused"]):
                            continue
                    chosen.append(slot)
                    if len(chosen) == count:
                        break
                if len(chosen) < count:
                    self._missing(object_name, code, str(start), str(end), "INSUFFICIENT_COUNT")
                chosen.reverse()
            if chosen:
                report = self.local.preflight(
                    codes=[code],
                    start=chosen[0],
                    end=chosen[-1],
                    frequency=normalized,
                    require_adjustment=bool(mode or "factor" in requested),
                )
                if not report.complete:
                    raise LocalCoverageError(report)
            frame = frame[frame[time_column].isin(chosen)].copy()
            if skip_paused or not fill_paused or "paused" in requested:
                if (
                    "paused" not in frame
                    or frame.paused.isna().any()
                    or not frame.paused.isin([0, 1, True, False]).all()
                ):
                    self._missing(object_name, code, str(start), str(end), "UNKNOWN_PAUSED_STATE")
                if skip_paused:
                    frame = frame[~frame.paused.astype(bool)]
                elif not fill_paused:
                    frame.loc[
                        frame.paused.astype(bool),
                        [field for field in DEFAULT_FIELDS + ["avg"] if field in frame],
                    ] = float("nan")
            if mode or "factor" in requested:
                factors = self.local.query(
                    object_name="adjustment_factor", codes=[code], end=str(end.date())
                )
                if (
                    "factor" not in factors
                    or not factors.factor.map(
                        lambda value: pd.notna(value)
                        and math.isfinite(float(value))
                        and float(value) > 0
                    ).all()
                ):
                    self._missing(
                        "adjustment_factor", code, str(start), str(end), "INVALID_ADJUSTMENT_FACTOR"
                    )
                reference = pre_factor_ref_date if pre_factor_ref_date is not None else end.date()
                if _timestamp(reference).date() > end.date():
                    raise ValueError("复权参考日不能晚于查询结束日")
                if mode:
                    frame = apply_adjustment(
                        frame,
                        factors,
                        fq=mode,
                        pre_factor_ref_date=reference,
                        date_column=time_column,
                    )
                if "factor" in requested:
                    ref = factors[
                        factors.trade_date.astype(str) <= str(_timestamp(reference).date())
                    ]
                    if ref.empty:
                        self._missing(
                            "adjustment_factor",
                            code,
                            str(reference),
                            str(reference),
                            "MISSING_REFERENCE_FACTOR",
                        )
                    mapping = factors.set_index("trade_date").factor
                    frame["factor"] = frame[time_column].astype(str).str[:10].map(mapping) / float(
                        ref.iloc[-1].factor
                    )
            missing = set(requested) - set(frame.columns)
            if missing or (not frame.empty and frame[requested].isna().any().any() and fill_paused):
                self._missing(
                    object_name,
                    code,
                    str(start),
                    str(end),
                    "MISSING_FIELD:" + ",".join(sorted(missing)),
                )
            frame.index = pd.DatetimeIndex(pd.to_datetime(frame[time_column]), name=None)
            frames[code] = frame[requested].sort_index()
        if len(codes) == 1:
            result = frames[codes[0]]
        elif panel:
            result = pd.concat(
                [
                    pd.concat([frames[code][field].rename(code) for code in codes], axis=1)
                    for field in requested
                ],
                axis=1,
                keys=requested,
            )
            result.columns.names = ["field", "code"]
        else:
            result = (
                pd.concat(
                    [
                        frame.rename_axis("time").reset_index().assign(code=code)
                        for code, frame in frames.items()
                    ],
                    ignore_index=True,
                )[["time", "code"] + requested]
                .sort_values(["time", "code"])
                .reset_index(drop=True)
            )
        result.attrs["data_source"] = {"type": self.name, "manifest_id": self.manifest_id}
        return result
