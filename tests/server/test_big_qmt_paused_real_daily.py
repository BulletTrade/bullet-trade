"""真实ETF停牌日线的两日周期复权回归。

作者：BruceLee
职责：验证2015年折算前两个停牌日的完整两组OHLC量额及NaN传播。
输入：纳管QMT四日全字段、真实交易日历、既有证券资料与完整事件；不使用聚宽或dr。
输出：none/pre/post与填充/不填充六组合的逐格断言及实际前收盘日期验证。
上下游：真实BigQmtDataAdapter和纯计算，通过四接口限域内存Gateway取数，不启动服务。
环境：pytest/pandas/numpy；仅读取本地JSON，无网络、行情下载、交易或文件写入。
"""

from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from decimal import Decimal, ROUND_HALF_UP, localcontext
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from bullet_trade.data.providers.remote_qmt import _dataframe_from_payload
from bullet_trade.server.adapters import big_qmt as adapter_module
from bullet_trade.server.adapters.big_qmt import BigQmtDataAdapter, BigQmtGatewayConfig
from bullet_trade.server.adapters.qmt import dataframe_to_payload

pytestmark = pytest.mark.unit
_FIXTURES = Path(__file__).parents[1] / "fixtures"
_SECURITY = "510500.XSHG"
_FIELDS = ["open", "high", "low", "close", "volume", "money"]


class _PausedDailyGateway:
    """重放唯一真实四日窗口；持有原始副本、真实事件及调用记录，不提供其他能力。"""

    def __init__(self):
        """读取纳管事实；无输入或返回，仅初始化内存状态，不补造行情、停牌或事件。"""
        source = json.loads(
            (_FIXTURES / "big_qmt_paused_daily_201504_20260908.json").read_text("utf-8")
        )
        saved = json.loads((_FIXTURES / "big_qmt_history_facts_20260908.json").read_text("utf-8"))
        self.facts = next(case for case in saved["cases"] if case["security"] == _SECURITY)
        raw = source["response"]
        self.raw = pd.DataFrame(raw["records"], columns=raw["columns"]).set_index("stime")
        self.raw.index = pd.to_datetime(self.raw.index, format="%Y%m%d")
        self.days = pd.to_datetime(source["calendar"], format="%Y%m%d")
        self.config = BigQmtGatewayConfig()
        self.calls = []

    async def post(self, path, payload=None, *, timeout_seconds=None):
        """回应四个只读接口；输入路径、载荷及超时，返回原事实wire，超范围请求断言失败。"""
        assert path in {
            "/data/security_info",
            "/data/trade_days",
            "/data/history",
            "/data/split_dividend",
        }
        assert payload["security"] == _SECURITY
        self.calls.append((path, deepcopy(payload)))
        if path == "/data/security_info":
            return deepcopy(self.facts["info"])
        lower = pd.Timestamp(payload["start"]) if payload.get("start") is not None else None
        upper = pd.Timestamp(payload["end"])
        if path == "/data/trade_days":
            days = self.days[self.days <= upper]
            if lower is not None:
                days = days[days >= lower]
            if payload.get("count", -1) > 0:
                days = days[-payload["count"] :]
            return {"dtype": "list", "values": days.strftime("%Y%m%d").tolist()}
        if path == "/data/split_dividend":
            result = deepcopy(self.facts["events"])
            result["events"] = [
                event for event in result["events"] if lower <= pd.Timestamp(event["date"]) <= upper
            ]
            return result
        assert lower is not None and self.days[0] <= lower <= upper <= self.days[-1]
        assert payload["frequency"] == "1d" and payload["fq"] == "none"
        assert payload["subscribe"] is False
        frame = self.raw.loc[lower:upper]
        if payload["fill_data"]:
            assert set(payload["fields"]).issubset({"close", "preClose", "suspendFlag"})
        else:
            # 已知fill_data=False缺两停牌行，只按源flag筛选，不从零量或填充价推断。
            frame = frame.loc[frame["suspendFlag"] == 0]
        frame = frame.loc[:, payload["fields"]].copy()
        frame.index = frame.index.strftime("%Y%m%d")
        return dataframe_to_payload(frame.rename_axis("stime"))


def _expected_two_days(gateway, fq, fill_paused):
    """独立手算完整两组；输入真实事实、模式和填充选择，返回预期表，不调用被测纯函数。"""
    traded = gateway.raw.loc[gateway.raw["suspendFlag"] == 0, _FIELDS].astype(float).copy()
    assert traded.index.tolist() == pd.to_datetime(["2015-04-10", "2015-04-15"]).tolist()
    event = next(
        event for event in gateway.facts["events"]["events"] if event["date"] == "2015-04-15"
    )
    assert [event[k] for k in ("cash_per_share", "transfer", "rights", "rights_price")] == [0] * 4
    assert event["gift"] == -0.719675 and event["share_reform"] is False
    with localcontext() as context:
        context.prec = 40
        close = Decimal(str(traded.iloc[0]["close"]))
        assert close == Decimal("2.243")
        reference = (close / (Decimal(1) + Decimal(str(event["gift"])))).quantize(
            Decimal("0.001"), rounding=ROUND_HALF_UP
        )
        assert reference == Decimal("8.001")
        forward = reference / close
        factors = {
            "none": [1.0, 1.0],
            "pre": [float(forward), 1.0],
            "post": [1.0, float(Decimal(1) / forward)],
        }[fq]
    traded.loc[:, _FIELDS[:4]] = np.round(
        traded[_FIELDS[:4]].to_numpy() * np.array(factors)[:, None], 3
    )
    traded["volume"] = np.round(traded["volume"].to_numpy() * 100 / factors)
    before, after = traded.iloc[0], traded.iloc[1]
    if fill_paused:
        rows = [
            [before.open, before.high, before.low, before.close, before.volume, before.money],
            [
                before.close,
                max(before.close, after.high),
                min(before.close, after.low),
                after.close,
                after.volume,
                after.money,
            ],
        ]
    else:
        # 固定两日组：首组尾NaN不影响已有高低；次组首NaN传播高低，量额均传播NaN。
        rows = [
            [before.open, before.high, before.low, np.nan, np.nan, np.nan],
            [np.nan, np.nan, np.nan, after.close, np.nan, np.nan],
        ]
    return pd.DataFrame(
        rows, columns=_FIELDS, index=pd.DatetimeIndex(["2015-04-13", "2015-04-15"], name="time")
    )


@pytest.mark.parametrize("fq", ["none", "pre", "post"])
@pytest.mark.parametrize("fill_paused", [True, False])
def test_real_paused_daily_two_day_matrix(fq, fill_paused, monkeypatch):
    """核验真实六组合的两行及C日期；输入模式/填充与补丁器，无返回，仅截获内存纯计算参数。"""
    gateway = _PausedDailyGateway()
    original = gateway.raw.copy(deep=True)
    captured = []
    real_adjust = adapter_module.adjust_bars

    def capture_adjust(raw, events, **kwargs):
        """记录真实计算输入；输入原价、事件及参数，返回原函数结果，不替换算法或修改数据。"""
        captured.append((deepcopy(events), dict(kwargs)))
        return real_adjust(raw, events, **kwargs)

    monkeypatch.setattr(adapter_module, "adjust_bars", capture_adjust)
    wire = asyncio.run(
        BigQmtDataAdapter(gateway).get_history(
            {
                "security": _SECURITY,
                "frequency": "2d",
                "start": "2015-04-10",
                "end": "2015-04-15",
                "fields": _FIELDS,
                "fq": fq,
                "fill_paused": fill_paused,
                "skip_paused": False,
                "pre_factor_ref_date": "2015-04-15" if fq == "pre" else None,
            }
        )
    )
    actual = _dataframe_from_payload(wire)
    expected = _expected_two_days(gateway, fq, fill_paused)
    assert actual.shape == (2, 6)
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    pd.testing.assert_frame_equal(gateway.raw, original)
    assert len(captured) == 1
    events, parameters = captured[0]
    if fq == "none":
        assert events == []
    else:
        assert len(events) == 1
        assert events[0]["previous_close_date"] == "2015-04-10"
        assert events[0]["previous_close"] == 2.243
    if fq == "post":
        assert str(parameters["post_origin_date"]) == "2013-03-15"
