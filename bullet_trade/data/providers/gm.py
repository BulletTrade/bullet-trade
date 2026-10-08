"""作者: BruceLee
职责: 将 GM 只读行情转换为 BulletTrade 数据接口，独立完成复权量价转换并记录 GM 字段来源。
输入: 标准证券代码/日期/周期/字段和 GM 配置；输出: DataFrame、交易日、元信息、分红事件。
上下游: data.api 惰性创建本 provider；data_client 隔离 SDK；聚宽仅用于独立的验收工具，不参与运行数据。
关键约束: 不访问账户/交易、不跨数据源兜底，缺少停牌/事件事实时抛错，不伪造行情。
"""

from __future__ import annotations

from datetime import date as Date
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from ...integrations.gm.data_client import GmDataClient, GmDataError
from ...integrations.gm.validation import gm_symbol, jq_symbol, local_time
from .base import DataProvider

PRICE_FIELDS = ["open", "high", "low", "close"]
DEFAULT_FIELDS = PRICE_FIELDS + ["volume", "money"]
SUPPORTED_FIELDS = set(
    DEFAULT_FIELDS + ["factor", "paused", "pre_close", "high_limit", "low_limit", "avg"]
)


def _timestamp(value: Any = None, *, end: bool = False) -> pd.Timestamp:
    """按上海本地时间解析日期；仅日期的截止时间包含整日。"""
    ts = (
        pd.Timestamp.now(tz="Asia/Shanghai").tz_localize(None)
        if value is None
        else local_time(value)
    )
    if end and (
        isinstance(value, Date)
        and not isinstance(value, datetime)
        or isinstance(value, str)
        and len(value) == 10
    ):
        ts += pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    return ts


def _period(value: str) -> tuple[str, int]:
    """只接受日线与常用分钟周期，避免把不支持周期静默降级。"""
    if value in {"daily", "day", "1d"}:
        return "1d", 0
    if value in {"minute", "1m"}:
        return "60s", 1
    if value in {"5m", "15m", "30m", "60m"}:
        return str(int(value[:-1]) * 60) + "s", int(value[:-1])
    raise NotImplementedError("GM 支持日线及 1/5/15/30/60 分钟线")


class GmDataProvider(DataProvider):
    """使用当前终端 SDK，SDK 只在显式数据请求的子进程中加载。"""

    name = "gm"
    requires_live_data = True

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config = dict(config or {})
        self._client = self.config.get("client") or GmDataClient(self.config)
        self._metadata: Dict[str, Dict[str, Any]] = {}
        # 拒绝先前误加的双来源配置，避免旧配置悄悄变更数据口径。
        if self.config.get("alignment_mode", "native") != "native" or any(
            self.config.get(k) is not None
            for k in ("reference_client", "rpc_env_file", "rpc_host", "rpc_port", "rpc_authkey")
        ):
            raise ValueError("掘金数据源必须独立使用 GM；聚宽仅可用于外部测试对比")
        self._factor_cache: Dict[tuple, float] = {}

    def auth(self, user=None, pwd=None, host=None, port=None) -> None:
        """检查数据客户端配置，不启动策略或交易连接。"""
        self._client.auth()

    def _query(self, method: str, **kwargs: Any) -> Any:
        return self._client.query(method, **kwargs)

    def _info(self, security: str) -> Dict[str, Any]:
        symbol = gm_symbol(security)
        if symbol not in self._metadata:
            rows = self._query("get_instrumentinfos", symbols=symbol, df=False)
            matches = [x for x in rows if x.get("symbol") == symbol]
            if len(matches) != 1:
                raise GmDataError("GM 未返回唯一证券元信息")
            row = dict(matches[0])
            group = {1: 1010, 2: 1020, 3: 1060}.get(int(row.get("sec_type", 0)))
            if group is None:
                raise NotImplementedError("GM 当前适配仅覆盖 A 股、场内基金和指数")
            classified = self._query("get_symbol_infos", sec_type1=group, symbols=symbol, df=False)
            if len(classified) != 1 or classified[0].get("symbol") != symbol:
                raise GmDataError("GM 未返回唯一证券分类")
            row.update(classified[0])
            self._metadata[symbol] = row
        return self._metadata[symbol]

    @staticmethod
    def _decimals(info: Dict[str, Any]) -> int:
        tick = Decimal(str(info.get("price_tick", 0)))
        if not tick.is_finite() or tick <= 0:
            raise GmDataError("GM 证券报价精度缺失")
        return max(0, -int(tick.normalize().as_tuple().exponent))

    @staticmethod
    def _frame(rows: Any, symbol: str, daily: bool) -> pd.DataFrame:
        columns = DEFAULT_FIELDS
        if not rows:
            return pd.DataFrame(columns=columns, index=pd.DatetimeIndex([]), dtype=float)
        if any(row.get("symbol") != symbol for row in rows):
            raise GmDataError("GM 行情证券与请求不一致")
        frame = pd.DataFrame(rows).rename(columns={"amount": "money"})
        try:
            frame.index = pd.DatetimeIndex([local_time(t) for t in frame.pop("eob")])
            if daily:
                frame.index = frame.index.normalize()
            frame = frame[columns].astype(float)
        except (KeyError, ValueError, TypeError):
            raise GmDataError("GM 行情字段或时间格式错误") from None
        if frame.index.has_duplicates or not frame.index.is_monotonic_increasing:
            raise GmDataError("GM 行情时间重复或乱序")
        if not np.isfinite(frame.to_numpy()).all() or (frame < 0).any().any():
            raise GmDataError("GM 行情含无效数值")
        return frame

    def _history(
        self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, period: str
    ) -> pd.DataFrame:
        # 小时间片的最坏行数远低于 SDK 的 33000 上限，避免静默截断。
        chunk = pd.Timedelta(days=3650 if period == "1d" else 30)
        pieces = []
        cursor = start
        while cursor <= end:
            stop = min(end, cursor + chunk - pd.Timedelta(microseconds=1))
            rows = self._query(
                "history",
                symbol=symbol,
                frequency=period,
                start_time=str(cursor if period == "1d" else cursor - pd.Timedelta(seconds=1)),
                end_time=str(stop),
                fields="symbol,eob,open,high,low,close,volume,amount",
                adjust=0,
                df=False,
            )
            if len(rows) >= 33000:
                raise GmDataError("GM 历史行情可能被截断")
            pieces.append(self._frame(rows, symbol, period == "1d"))
            cursor = stop + pd.Timedelta(microseconds=1)
        result = pd.concat(pieces) if pieces else self._frame([], symbol, period == "1d")
        if result.index.has_duplicates:
            raise GmDataError("GM 分段历史行情出现重复时间")
        return result.loc[
            (result.index >= start.normalize() if period == "1d" else result.index >= start)
            & (result.index <= end)
        ]

    def _previous(self, symbol: str, end: pd.Timestamp) -> pd.DataFrame:
        rows = self._query(
            "history_n",
            symbol=symbol,
            frequency="1d",
            count=1,
            end_time=str(end),
            fields="symbol,eob,open,high,low,close,volume,amount",
            adjust=0,
            df=False,
        )
        frame = self._frame(rows, symbol, True)
        if not frame.empty and frame.index[-1] > end:
            raise GmDataError("GM 前收盘查询返回未来行情")
        return frame

    def _daily_status(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        rows = self._query(
            "get_history_instruments",
            symbols=symbol,
            start_date=start.date().isoformat(),
            end_date=end.date().isoformat(),
            fields="symbol,trade_date,is_suspended,pre_close,upper_limit,lower_limit,adj_factor",
            df=False,
        )
        if not rows:
            return pd.DataFrame(index=pd.DatetimeIndex([]))
        if any(x.get("symbol") != symbol for x in rows):
            raise GmDataError("GM 历史状态证券不一致")
        frame = pd.DataFrame(rows)
        frame.index = pd.DatetimeIndex([local_time(t).normalize() for t in frame.pop("trade_date")])
        if frame.index.has_duplicates or not frame.index.is_monotonic_increasing:
            raise GmDataError("GM 历史状态重复或乱序")
        return frame

    def _fill_daily(
        self, frame: pd.DataFrame, symbol: str, start: pd.Timestamp, end: pd.Timestamp, fill: bool
    ) -> pd.DataFrame:
        info = self._info(jq_symbol(symbol))
        start = max(start.normalize(), local_time(info["listed_date"]).normalize())
        last = local_time(info["delisted_date"]).normalize()
        end = min(end, last - pd.Timedelta(microseconds=1))
        days = pd.DatetimeIndex(self.get_trade_days(start, end))
        missing = days.difference(frame.index)
        if not len(missing):
            frame["paused"] = 0.0
            return frame
        status = self._daily_status(symbol, start, end)
        if any(d not in status.index or status.loc[d].get("is_suspended") != 1 for d in missing):
            raise GmDataError("GM 缺少行情但未确认全天停牌，拒绝补行")
        frame = frame.reindex(days)
        frame["paused"] = frame.index.isin(missing).astype(float)
        if fill:
            prev = self._previous(symbol, start - pd.Timedelta(microseconds=1))
            close = float(prev.close.iloc[-1]) if not prev.empty else np.nan
            for day in days:
                if day not in missing:
                    close = float(frame.loc[day, "close"])
                else:
                    if not np.isfinite(close):
                        raise GmDataError("GM 停牌补行缺少此前真实收盘价")
                    frame.loc[day, PRICE_FIELDS] = close
                    frame.loc[day, ["volume", "money"]] = 0
        else:
            frame.loc[missing, :] = np.nan
        return frame

    def _minute_grid(self, start, end):
        """中国股票分钟日历，仅包含两段连续竞价，按请求区间取标签。"""
        days = self.get_trade_days(start.normalize(), end.normalize())
        values = []
        for day in days:
            date = pd.Timestamp(day)
            for first, last in [("09:31", "11:30"), ("13:01", "15:00")]:
                values.extend(
                    pd.date_range(
                        str(date.date()) + " " + first, str(date.date()) + " " + last, freq="min"
                    )
                )
        index = pd.DatetimeIndex(values)
        return index[(index >= start) & (index <= end)]

    def _complete_zero_volume_day(self, symbol, day, asof):
        """只对已结束历史日用 GM 日/分钟量额守恒证明缺口零成交；不推测盘中缺行。"""
        if day.date() >= _timestamp().date() or asof < day + pd.Timedelta(hours=15):
            raise GmDataError("GM 当日分钟缺行不能用未来全天数据补齐")
        end = day + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        daily = self._history(symbol, day, end, "1d")
        minutes = self._history(symbol, day, end, "60s")
        if len(daily) != 1 or minutes.empty:
            raise GmDataError("GM 分钟缺行且缺少完整日线/分钟证据")
        # 股数必须完全守恒；金额仅允许浮点累加误差，不能掩盖真实漏成交。
        if minutes.volume.sum() != daily.volume.iloc[0] or not np.isclose(
            minutes.money.sum(), daily.money.iloc[0], rtol=0, atol=1e-6
        ):
            raise GmDataError("GM 分钟与日线量额不守恒，拒绝补齐缺失分钟")
        grid = self._minute_grid(day, end)
        if not minutes.index.isin(grid).all():
            raise GmDataError("GM 分钟时间不属于交易日历")
        previous = self._previous(symbol, day - pd.Timedelta(microseconds=1))
        values = minutes.reindex(grid)
        close = previous.close.iloc[-1] if not previous.empty else np.nan
        for stamp in grid:
            if stamp in minutes.index:
                close = values.loc[stamp, "close"]
            else:
                if not np.isfinite(close):
                    raise GmDataError("GM 零成交分钟缺少此前真实收盘价")
                values.loc[stamp, PRICE_FIELDS] = close
                values.loc[stamp, ["volume", "money"]] = 0.0
        return values

    def _fill_minutes(self, frame, symbol, start, end, fill, skip=False):
        """整日停牌按状态补齐；盘中缺口必须先由本源完整历史日量额守恒证明。"""
        info = self._info(jq_symbol(symbol))
        start = max(start, local_time(info["listed_date"]))
        end = min(end, local_time(info["delisted_date"]) - pd.Timedelta(microseconds=1))
        index = self._minute_grid(start, end)
        missing = index.difference(frame.index)
        if not len(missing):
            frame["paused"] = 0.0
            return frame
        status = self._daily_status(symbol, start, end)
        if any(d.normalize() not in status.index for d in missing):
            raise GmDataError("GM 分钟行情缺失且未确认全天停牌状态")
        full_pause = pd.DatetimeIndex(
            [t for t in missing if status.loc[t.normalize()].get("is_suspended") == 1]
        )
        partial = missing.difference(full_pause)
        frame = frame.reindex(index)
        for day in partial.normalize().unique():
            complete = self._complete_zero_volume_day(symbol, day, end)
            stamps = partial[partial.normalize() == day]
            frame.loc[stamps, DEFAULT_FIELDS] = complete.loc[stamps, DEFAULT_FIELDS]
        frame["paused"] = frame.index.isin(full_pause).astype(float)
        if len(full_pause) and fill:
            previous = self._previous(symbol, start.normalize() - pd.Timedelta(microseconds=1))
            close = previous.close.iloc[-1] if not previous.empty else np.nan
            for stamp in index:
                if stamp not in full_pause:
                    close = frame.loc[stamp, "close"]
                else:
                    if not np.isfinite(close):
                        raise GmDataError("GM 分钟停牌补行缺少此前真实收盘")
                    frame.loc[stamp, PRICE_FIELDS] = close
                    frame.loc[stamp, ["volume", "money"]] = 0.0
        elif not fill:
            frame.loc[full_pause, :] = np.nan
        return frame.loc[~frame.index.isin(full_pause)] if skip else frame

    def _factor(self, security, frame, mode, anchor):
        """仅从 GM 历史证券状态取累计因子；前复权除以固定参考日因子。"""
        if not mode:
            return pd.Series(1.0, index=frame.index)
        # 官方指数分类没有股票现金分红/送转，价格不复权，也不要求不存在的 adj_factor。
        if int(self._info(security).get("sec_type1", 0)) == 1060:
            return pd.Series(1.0, index=frame.index)
        status = self._daily_status(gm_symbol(security), frame.index[0], frame.index[-1])
        if "adj_factor" not in status or any(
            t.normalize() not in status.index for t in frame.index
        ):
            raise GmDataError("GM 复权因子覆盖不完整")
        factors = pd.Series(
            [status.loc[t.normalize(), "adj_factor"] for t in frame.index],
            index=frame.index,
            dtype=float,
        )
        if mode == "pre":
            key = (security, anchor)
            # 当天因子可能盘中更新；只缓存固定历史参考日。
            historical = anchor < _timestamp().date()
            if not historical or key not in self._factor_cache:
                days = self.get_trade_days(end_date=anchor, count=1)
                if not days:
                    raise GmDataError("GM 固定基准日交易日缺失")
                if pd.Timestamp(days[-1]).date() > anchor:
                    raise GmDataError("GM 基准因子返回未来日期")
                ref = self._daily_status(
                    gm_symbol(security), pd.Timestamp(days[-1]), pd.Timestamp(days[-1])
                )
                if ref.empty or "adj_factor" not in ref:
                    raise GmDataError("GM 固定基准日因子缺失")
                base = float(ref.adj_factor.iloc[-1])
                if not np.isfinite(base) or base <= 0:
                    raise GmDataError("固定基准日复权因子无效")
                if historical:
                    self._factor_cache[key] = base
            else:
                base = self._factor_cache[key]
            factors = factors / base
        if not np.isfinite(factors.dropna().to_numpy()).all() or (factors.dropna() <= 0).any():
            raise GmDataError("复权因子无效")
        if factors.loc[frame.close.notna()].isna().any():
            raise GmDataError("有效行情缺少复权因子")
        return factors

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
    ):
        """获取原始 bar 后统一复权；停牌仅在已确认状态下补行，默认不跨源取数。"""
        wanted = list(DEFAULT_FIELDS if fields is None else fields)
        if set(wanted) - SUPPORTED_FIELDS:
            raise NotImplementedError("GM 不支持请求中的行情字段")
        if fq not in {None, "none", "pre", "post"}:
            raise ValueError("fq 必须为 None/none/pre/post")
        if count is not None and (
            isinstance(count, bool) or not isinstance(count, int) or count <= 0
        ):
            raise ValueError("count 必须是正整数")
        if start_date is not None and count is not None:
            raise ValueError("start_date 和 count 不能同时指定")
        if isinstance(security, list):
            pieces = {
                s: self.get_price(
                    s,
                    start_date,
                    end_date,
                    frequency,
                    wanted,
                    skip_paused,
                    fq,
                    count,
                    True,
                    fill_paused,
                    pre_factor_ref_date,
                )
                for s in security
            }
            if panel:
                return (
                    pd.concat(pieces, axis=1).swaplevel(0, 1, axis=1) if pieces else pd.DataFrame()
                )
            return (
                pd.concat(
                    [f.assign(code=s).rename_axis("time").reset_index() for s, f in pieces.items()],
                    ignore_index=True,
                )
                if pieces
                else pd.DataFrame(columns=["time", "code"] + wanted)
            )
        symbol = gm_symbol(security)
        period, minutes = _period(frequency)
        period = "60s" if minutes else period
        end = _timestamp(end_date, end=True)
        if count is not None:
            if minutes and not skip_paused:
                base_count = count * minutes
                days = self.get_trade_days(end_date=end, count=(base_count + 239) // 240 + 1)
                grid = (
                    self._minute_grid(pd.Timestamp(days[0]), end) if days else pd.DatetimeIndex([])
                )
                start = (
                    grid[-base_count] if len(grid) >= base_count else grid[0] if len(grid) else end
                )
                frame = self._history(symbol, start, end, period)
            elif minutes == 0 and not skip_paused:
                dates = self.get_trade_days(end_date=end, count=count)
                start = pd.Timestamp(dates[0]) if dates else end.normalize()
                frame = self._history(symbol, start, end, period)
            else:
                fetch_count = count * max(minutes, 1)
                if fetch_count > 33000:
                    raise ValueError("GM 单次 count 对应的基础分钟条数不能超过 33000")
                rows = self._query(
                    "history_n",
                    symbol=symbol,
                    frequency=period,
                    count=fetch_count,
                    end_time=str(end),
                    fields="symbol,eob,open,high,low,close,volume,amount",
                    adjust=0,
                    df=False,
                )
                frame = self._frame(rows, symbol, minutes == 0)
                if not frame.empty and frame.index[-1] > end:
                    raise GmDataError("GM count 查询返回未来行情")
                start = frame.index[0] if not frame.empty else end.normalize()
        else:
            if start_date is None:
                raise ValueError("GM get_price 需要 start_date 或 count")
            start = _timestamp(start_date)
            if start > end:
                raise ValueError("开始时间不能晚于结束时间")
            frame = self._history(symbol, start, end, period)
        if minutes == 0 and not skip_paused:
            frame = self._fill_daily(frame, symbol, start, end, fill_paused)
        elif minutes:
            frame = self._fill_minutes(
                frame, symbol, start, end, fill_paused or skip_paused, skip_paused
            )
        else:
            frame["paused"] = 0.0
        if frame.empty:
            return pd.DataFrame(columns=wanted, index=pd.DatetimeIndex([]))
        info = self._info(security)
        decimals = self._decimals(info)
        frame.loc[:, PRICE_FIELDS] = frame[PRICE_FIELDS].round(decimals)
        if minutes:
            # 聚宽分钟金额使用整数元；上海 GM 源自身已丢失的小数无法反推。
            frame["money"] = np.trunc(frame.money)
        mode = None if fq == "none" else fq
        anchor = _timestamp(pre_factor_ref_date).date()
        factors = self._factor(security, frame, mode, anchor)
        if mode:
            frame.loc[:, PRICE_FIELDS] = np.round(
                frame[PRICE_FIELDS].to_numpy() * factors.to_numpy()[:, None], decimals
            )
            frame["volume"] = np.round(frame.volume / factors)
        frame["factor"] = factors
        if {"pre_close", "high_limit", "low_limit"} & set(wanted):
            status = self._daily_status(symbol, start, end)
            for dest, source in [
                ("pre_close", "pre_close"),
                ("high_limit", "upper_limit"),
                ("low_limit", "lower_limit"),
            ]:
                if dest not in wanted:
                    continue
                if source not in status or any(
                    t.normalize() not in status.index for t in frame.index
                ):
                    raise GmDataError("GM 历史状态字段覆盖不完整")
                values = np.array(
                    [status.loc[t.normalize(), source] for t in frame.index], dtype=float
                )
                if not np.isfinite(values).all():
                    raise GmDataError("GM 历史状态含无效价格")
                frame[dest] = np.round(values * frame.factor.to_numpy(), decimals)
            if minutes and "pre_close" in wanted:
                previous = self._query(
                    "history_n",
                    symbol=symbol,
                    frequency="60s",
                    count=1,
                    end_time=str(frame.index[0] - pd.Timedelta(seconds=1)),
                    fields="symbol,eob,open,high,low,close,volume,amount",
                    adjust=0,
                    df=False,
                )
                before = self._frame(previous, symbol, False)
                values = frame.close.shift()
                if not before.empty and before.index[-1].date() == frame.index[0].date():
                    values.iloc[0] = round(
                        float(before.close.iloc[-1]) * frame.factor.iloc[0], decimals
                    )
                else:
                    values.iloc[0] = frame.pre_close.iloc[0]
                # 跨日首分钟使用当日除权参考价，避免沿用昨天未经除权的价格。
                for pos in range(1, len(frame)):
                    if frame.index[pos].date() != frame.index[pos - 1].date():
                        values.iloc[pos] = frame.pre_close.iloc[pos]
                frame["pre_close"] = values
        if minutes > 1:
            frame = self._aggregate_minutes(frame, minutes)
        if "avg" in wanted:
            frame["avg"] = (
                (frame.money / frame.volume.replace(0, np.nan)).fillna(frame.close).round(decimals)
            )
        result = frame[wanted].tail(count) if count else frame[wanted]
        result.attrs["data_source"] = "gm"
        result.attrs["alignment_mode"] = "native"
        result.attrs["field_sources"] = {f: "gm" for f in wanted}
        result.attrs["price_factor_source"] = "gm" if mode else "none"
        return result

    @staticmethod
    def _aggregate_minutes(frame, group):
        """按聚宽连续 1m 行分组，保留末尾不足一组；不使用固定时钟窗口的原生 GM Xm。"""
        rows, times = [], []
        for start in range(0, len(frame), group):
            chunk = frame.iloc[start : start + group]
            values = {field: chunk[field].iloc[-1] for field in chunk.columns}
            values.update(
                open=chunk.open.iloc[0],
                high=chunk.high.max(skipna=False),
                low=chunk.low.min(skipna=False),
                volume=chunk.volume.sum(skipna=False),
                money=chunk.money.sum(skipna=False),
            )
            if "pre_close" in chunk:
                values["pre_close"] = chunk.pre_close.iloc[0]
            values["paused"] = chunk.paused.max()
            rows.append(values)
            times.append(chunk.index[-1])
        return pd.DataFrame(rows, index=pd.DatetimeIndex(times), columns=frame.columns)

    def get_trade_days(self, start_date=None, end_date=None, count=None):
        """返回交易日历，count 向前扩展直到数量足够或达到历史边界。"""
        if count is not None and (
            start_date is not None
            or isinstance(count, bool)
            or not isinstance(count, int)
            or count <= 0
        ):
            raise ValueError("交易日 count 为正整数且不能和 start_date 同时指定")
        end = _timestamp(end_date).normalize()
        first = (
            _timestamp(start_date).normalize()
            if start_date is not None
            else pd.Timestamp("1990-01-01")
        )
        if first > end:
            raise ValueError("交易日开始时间晚于结束时间")
        if count:
            first = max(first, end - pd.Timedelta(days=count * 2 + 30))
        while True:
            rows = self._query(
                "get_trading_dates",
                exchange="SHSE",
                start_date=first.date().isoformat(),
                end_date=end.date().isoformat(),
            )
            days = [local_time(x).normalize().to_pydatetime() for x in rows]
            if (
                len(set(days)) != len(days)
                or days != sorted(days)
                or any(not first <= pd.Timestamp(x) <= end for x in days)
            ):
                raise GmDataError("GM 交易日历重复、乱序或越界")
            if not count or len(days) >= count or first <= pd.Timestamp("1990-01-01"):
                return days[-count:] if count else days
            first = max(pd.Timestamp("1990-01-01"), first - (end - first) - pd.Timedelta(days=30))

    def get_security_info(self, security, date=None):
        """提供证券元信息；基金品种与货币 ETF 分类来自官方细类/板块。"""
        row = self._info(security)
        day = _timestamp(date).date()
        listed, delisted = (
            local_time(row["listed_date"]).date(),
            local_time(row["delisted_date"]).date(),
        )
        if not listed <= day < delisted:
            raise ValueError("证券在查询日期未上市或已退市")
        kind = self._kind(row)
        if kind is None:
            raise NotImplementedError("GM 当前适配仅覆盖 A 股、场内基金和指数")
        return dict(
            display_name=row["sec_name"],
            name=row.get("sec_abbr", ""),
            start_date=listed,
            end_date=delisted,
            type=kind,
            price_tick=row["price_tick"],
        )

    @staticmethod
    def _kind(row):
        if row.get("board") == 10200105:
            return "mmf"
        return {101001: "stock", 102001: "etf", 102002: "lof", 102005: "fund", 106001: "index"}.get(
            row.get("sec_type2")
        )

    def get_all_securities(self, types="stock", date=None):
        """查询含退市标的的静态全集，并按上市/退市日过滤；名称为源端当前名称。"""
        kinds = [types] if isinstance(types, str) else list(types)
        mapping = {
            "stock": 1010,
            "fund": 1020,
            "etf": 1020,
            "lof": 1020,
            "mmf": 1020,
            "index": 1060,
        }
        if set(kinds) - set(mapping):
            raise NotImplementedError("GM 不支持请求的证券类型")
        rows = []
        for group in sorted({mapping[k] for k in kinds}):
            rows.extend(
                self._query("get_symbol_infos", sec_type1=group, exchanges="SHSE,SZSE", df=False)
            )
        day = _timestamp(date).date()
        result = {}
        for row in rows:
            code = jq_symbol(row["symbol"])
            row = dict(row, sec_type={1010: 1, 1020: 2, 1060: 3}[row["sec_type1"]])
            self._metadata[gm_symbol(code)] = row
            listed, delisted = (
                local_time(row["listed_date"]).date(),
                local_time(row["delisted_date"]).date(),
            )
            kind = self._kind(row)
            if listed <= day < delisted and (
                kind in kinds or "fund" in kinds and row["sec_type1"] == 1020
            ):
                result[code] = dict(
                    display_name=row["sec_name"],
                    name=row.get("sec_abbr", ""),
                    start_date=listed,
                    end_date=delisted,
                    type=kind,
                )
        return pd.DataFrame.from_dict(
            result,
            orient="index",
            columns=["display_name", "name", "start_date", "end_date", "type"],
        )

    def get_index_stocks(self, index_symbol, date=None):
        """只读取指定日期的历史成份；不得以最新成份代替历史查询。"""
        days = self.get_trade_days(end_date=date, count=1)
        if not days:
            return []
        rows = self._query(
            "stk_get_index_constituents",
            index=gm_symbol(index_symbol),
            trade_date=days[0].date().isoformat(),
        )
        if not rows:
            raise GmDataError("GM 未返回指定交易日的指数成份快照")
        symbols = [jq_symbol(r["symbol"]) for r in rows]
        if len(set(symbols)) != len(symbols):
            raise GmDataError("GM 指数成份重复")
        return sorted(symbols)

    def get_split_dividend(self, security, start_date=None, end_date=None):
        """将 GM 每股权益转换为框架股票每十股/基金每份事件，不依赖付费基金接口。"""
        if start_date is None or end_date is None:
            raise ValueError("GM 分红查询需要明确开始/结束日期")
        first, last = _timestamp(start_date).date(), _timestamp(end_date).date()
        if first > last:
            raise ValueError("分红开始日期晚于结束日期")
        symbol = gm_symbol(security)
        info = self._info(security)
        base = 10 if info["sec_type"] == 1 else 1
        rows = self._query(
            "get_dividend",
            symbol=symbol,
            start_date=first.isoformat(),
            end_date=last.isoformat(),
            df=False,
        )
        events = []
        for r in rows:
            if r.get("symbol") != symbol:
                raise GmDataError("GM 分红证券与请求不一致")
            if r["allotment_ratio"]:
                raise NotImplementedError("标准分红接口不能表达配股认购款；请使用复权行情接口")
            day = local_time(r["created_at"]).date()
            if first <= day <= last:
                events.append(
                    dict(
                        security=security,
                        date=day,
                        security_type="stock" if base == 10 else "fund",
                        scale_factor=1 + r["share_div_ratio"] + r["share_trans_ratio"],
                        bonus_pre_tax=r["cash_div"] * base,
                        per_base=base,
                    )
                )
        return sorted(events, key=lambda e: e["date"])

    @staticmethod
    def _tick_frame(rows, symbol):
        """映射原始 tick 与五档盘口；时间保留上海时区，并检查证券和排序。"""
        fields = ["time", "current", "volume", "money"] + [
            f"{s}{n}_{v}" for s in ("a", "b") for n in range(1, 6) for v in ("p", "v")
        ]
        values = []
        times = []
        for row in rows:
            if row.get("symbol") != symbol:
                raise GmDataError("GM tick 证券与请求不一致")
            stamp = local_time(row["created_at"])
            times.append(stamp)
            value = dict(
                time=float(stamp.strftime("%Y%m%d%H%M%S")),
                current=row["price"],
                volume=row["cum_volume"],
                money=row["cum_amount"],
            )
            for i, quote in enumerate(row.get("quotes", [])[:5], 1):
                for side, source in [("a", "ask"), ("b", "bid")]:
                    for dest, name in [("p", "p"), ("v", "v")]:
                        value[f"{side}{i}_{dest}"] = quote[f"{source}_{name}"]
            values.append(value)
        frame = pd.DataFrame(values, columns=fields)
        frame.index = pd.DatetimeIndex(times)
        if frame.index.has_duplicates or not frame.index.is_monotonic_increasing:
            raise GmDataError("GM tick 时间重复或乱序")
        if (
            not frame.empty
            and not np.isfinite(frame[["current", "volume", "money"]].to_numpy(dtype=float)).all()
        ):
            raise GmDataError("GM tick 含无效量价")
        return frame

    def get_ticks(
        self, security, end_dt, start_dt=None, count=None, fields=None, skip=False, df=False
    ):
        """过滤连续量价未变快照；先取过滤上下文，再截 count，避免把尾部重复快照当新成交。"""
        if count is not None and (
            isinstance(count, bool) or not isinstance(count, int) or count <= 0 or count > 33000
        ):
            raise ValueError("tick count 必须为 1 至 33000 的整数")
        symbol = gm_symbol(security)
        end = _timestamp(end_dt, end=True)
        start = _timestamp(start_dt) if start_dt is not None else None
        if start is not None and start > end:
            raise ValueError("tick 开始时间晚于结束时间")
        if start is None:
            wanted = count or 1
            fetched = min(33000, max(128, wanted + 1)) if skip else wanted
            while True:
                rows = self._query(
                    "history_n",
                    symbol=symbol,
                    frequency="tick",
                    count=fetched,
                    end_time=str(end),
                    df=False,
                )
                frame = self._tick_frame(rows, symbol)
                if not frame.empty and frame.index[-1] > end:
                    raise GmDataError("GM tick 返回未来数据")
                if skip and not frame.empty:
                    changed = (
                        frame[["current", "volume", "money"]]
                        .ne(frame[["current", "volume", "money"]].shift())
                        .any(axis=1)
                    )
                    # 满窗口的第一行缺少前序上下文，不能假定为新的成交。
                    if len(rows) == fetched:
                        changed.iloc[0] = False
                    frame = frame.loc[changed]
                if len(frame) >= wanted or len(rows) < fetched:
                    frame = frame.tail(wanted)
                    break
                if fetched == 33000:
                    raise GmDataError("GM tick 过滤上下文超过 SDK 上限")
                fetched = min(33000, fetched * 2)
        else:
            pieces = []
            for day in pd.date_range(start.normalize(), end.normalize()):
                # 从开盘前开始，跨查询起点仍有过滤上下文；不跨日比较累计成交量。
                stop = min(end, day + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1))
                rows = self._query(
                    "history",
                    symbol=symbol,
                    frequency="tick",
                    start_time=str(day),
                    end_time=str(stop),
                    df=False,
                )
                if len(rows) >= 33000:
                    raise GmDataError("GM tick 日内数据可能被截断")
                piece = self._tick_frame(rows, symbol)
                if skip and not piece.empty:
                    piece = piece.loc[
                        piece[["current", "volume", "money"]]
                        .ne(piece[["current", "volume", "money"]].shift())
                        .any(axis=1)
                    ]
                pieces.append(piece)
            frame = pd.concat(pieces) if pieces else self._tick_frame([], symbol)
            frame = frame.loc[(frame.index >= start) & (frame.index <= end)]
            if count:
                frame = frame.tail(count)
        wanted_fields = list(frame.columns) if fields is None else list(fields)
        if set(wanted_fields) - set(frame.columns):
            raise NotImplementedError("GM 不支持请求中的 tick 字段")
        if any(frame[f].isna().any() for f in wanted_fields) and not frame.empty:
            raise GmDataError("GM tick 缺少请求的盘口字段")
        frame = frame[wanted_fields].reset_index(drop=True)
        return frame if df else frame.to_records(index=False)

    def get_current_tick(self, security, dt=None, df=False):
        """历史时刻严格走历史 tick；当前行情只读取显式证券。"""
        if dt is not None:
            frame = self.get_ticks(
                security,
                dt,
                count=1,
                fields=["time", "current", "volume", "money"],
                skip=False,
                df=True,
            )
            return frame if df else (None if frame.empty else frame.iloc[0].to_dict())
        symbol = gm_symbol(security)
        rows = self._query(
            "current", symbols=symbol, fields="symbol,created_at,price,cum_volume,cum_amount,quotes"
        )
        if len(rows) != 1:
            raise GmDataError("GM 未返回唯一最新 tick")
        frame = self._tick_frame(rows, symbol).reset_index(drop=True)
        return frame if df else frame.iloc[0].dropna().to_dict()

    def get_live_current(self, security):
        """实时快照需满足时间新鲜度，不能用陈旧报价作为可成交行情。"""
        tick = self.get_current_tick(security)
        time = pd.to_datetime(str(int(tick["time"])), format="%Y%m%d%H%M%S")
        age = (_timestamp() - time).total_seconds()
        if age < -1 or age > float(self.config.get("max_live_age_seconds", 5)):
            raise GmDataError("GM 最新行情已过期或时间在未来")
        day = time.date()
        status = self._daily_status(gm_symbol(security), pd.Timestamp(day), pd.Timestamp(day))
        if status.empty or time.normalize() not in status.index:
            raise GmDataError("GM 当日证券状态缺失")
        row = status.loc[time.normalize()]
        return dict(
            last_price=tick["current"],
            paused=bool(row["is_suspended"]),
            high_limit=row["upper_limit"],
            low_limit=row["lower_limit"],
            day_open=None,
            name=self._info(security)["sec_name"],
            last_time=time.to_pydatetime(),
        )

    def get_bars(
        self,
        security,
        count,
        unit="1d",
        fields=None,
        include_now=False,
        end_dt=None,
        fq_ref_date=1,
        df=False,
    ):
        """获取已完成 bar，排除未完成日/分钟；复权基准遵从显式 fq_ref_date。"""
        end = _timestamp(end_dt, end=True)
        _, minutes = _period(unit)
        if not include_now:
            if minutes:
                end = end.floor(str(minutes) + "min")
            elif end.time() < datetime.strptime("15:00", "%H:%M").time():
                end = end.normalize() - pd.Timedelta(microseconds=1)
        fq = None if fq_ref_date is None else "pre"
        ref = _timestamp().date() if fq_ref_date == 1 else fq_ref_date
        wanted = list(fields or ["date", "open", "high", "low", "close"])

        def single(code):
            frame = self.get_price(
                code,
                end_date=end,
                frequency=unit,
                count=count,
                fields=[f for f in wanted if f != "date"],
                skip_paused=True,
                fq=fq,
                pre_factor_ref_date=ref,
            )
            if "date" in wanted:
                frame.insert(0, "date", frame.index)
            frame = frame[wanted]
            return frame if df else frame.to_records(index=False)

        return {s: single(s) for s in security} if isinstance(security, list) else single(security)

    def subscribe_ticks(self, symbols):
        """隔离查询客户端没有推送通道，不能把订阅请求静默视为成功。"""
        raise NotImplementedError("GM 数据源暂不支持 tick 推送订阅")

    def subscribe_markets(self, markets):
        """隔离查询客户端不支持市场级订阅。"""
        raise NotImplementedError("GM 数据源暂不支持市场推送订阅")

    def unsubscribe_ticks(self, symbols=None):
        """当前不存在可取消的 tick 订阅能力。"""
        raise NotImplementedError("GM 数据源暂不支持 tick 推送订阅")

    def unsubscribe_markets(self, markets=None):
        """当前不存在可取消的市场订阅能力。"""
        raise NotImplementedError("GM 数据源暂不支持市场推送订阅")
