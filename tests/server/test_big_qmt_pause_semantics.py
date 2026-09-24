"""大 QMT 盘中与整日停牌的市场样本回归。

作者：BruceLee
职责：重放 WHZ 原价，经公开 Provider/真实 Adapter 验证停牌语义与时间边界。
输入：2026-09-24 冻结的 513100/000929 行情及聚宽对照；输出：逐格断言。
上下游：RemoteQmtProvider -> BigQmtDataAdapter -> 内存行情网关，无网络和交易。
环境约定：本地 pytest/pandas；交易日历限定于样本覆盖，元数据仅用于格式转换。
"""

import json
from copy import deepcopy
from pathlib import Path

import pandas as pd
import pytest
from test_big_qmt_history_standardization import _client
from tests.price_pause_contract import CASES, assert_pause_contract

from bullet_trade.data import api as data_api
from bullet_trade.remote.connection import RemoteServerError
from bullet_trade.server.adapters import big_qmt as module
from bullet_trade.server.adapters.qmt import dataframe_to_payload

pytestmark = pytest.mark.unit
FIELDS = ["open", "high", "low", "close", "volume", "money", "paused"]
FIXTURE = Path(__file__).parents[1] / "fixtures" / "big_qmt_pause_20260924.json"


class FrozenGateway:
    """只读冻结市场网关；维护行情和调用记录，仅提供本测试所需的白名单接口。"""

    def __init__(self):
        """读取本机冻结样本；无输入，返回None，不访问网络或账户。"""
        self.config = module.BigQmtGatewayConfig()
        self.evidence = json.loads(FIXTURE.read_text())
        self.frames = {
            (
                row["request"]["security"],
                row["request"]["frequency"],
                row["request"]["fill_data"],
            ): module._history_frame(row["response"])
            for row in self.evidence["qmt"]
        }
        self.calls = []

    async def post(self, path, payload=None, *, timeout_seconds=None):
        """按请求裁剪冻结事实；输入路径和载荷，返回新wire，越界动作直接断言失败。"""
        self.calls.append((path, deepcopy(payload)))
        security = payload["security"].replace(".XSHG", ".SH").replace(".XSHE", ".SZ")
        assert security in {"513100.SH", "000929.SZ"}
        if path == "/data/security_info":
            return dict(
                start_date="1990-01-01",
                type="etf" if security.startswith("513") else "stock",
                PriceTick=0.001 if security.startswith("513") else 0.01,
            )
        if path == "/data/trade_days":
            days = self.frames[(security, "1d", True)].index
            if payload.get("start"):
                days = days[days >= pd.Timestamp(payload["start"]).normalize()]
            days = days[days <= pd.Timestamp(payload["end"])]
            if payload.get("count", -1) > 0:
                days = days[-payload["count"] :]
            return dict(dtype="list", values=days.strftime("%Y-%m-%d").tolist())
        assert path == "/data/history", f"冻结测试不允许 {path}"
        assert payload["fq"] == "none" and payload["subscribe"] is False
        freq = payload["frequency"]
        frame = self.frames[(security, freq, payload["fill_data"])]
        lower, upper = pd.Timestamp(payload["start"]), pd.Timestamp(payload["end"])
        frame = frame.loc[(frame.index >= lower) & (frame.index <= upper), payload["fields"]].copy()
        frame.index = frame.index.strftime("%Y%m%d" if freq == "1d" else "%Y%m%d%H%M%S")
        frame.index.name = "stime"
        return dataframe_to_payload(frame)


@pytest.fixture(autouse=True)
def fixed_clock(monkeypatch):
    """固定样本后的时钟；输入pytest夹具，返回None，仅替换本进程时钟。"""

    def now():
        """无输入，返回样本完成后的时间，无副作用。"""
        return pd.Timestamp("2026-09-24 18:00:00")

    monkeypatch.setattr(module, "_history_now", now)


def request(gateway, security="513100.XSHG", **kwargs):
    """输入冻结网关、证券和参数，返回真实Provider结果，不修改原始样本。"""
    provider, _ = _client(gateway)
    options = dict(
        start_date="2026-09-24 09:30",
        end_date="2026-09-24 15:00",
        frequency="1m",
        fields=FIELDS,
        fq=None,
        skip_paused=False,
        fill_paused=True,
    )
    options.update(kwargs)
    return provider.get_price(security, **options)


@pytest.mark.parametrize("skip", [False, True])
@pytest.mark.parametrize("fill", [False, True])
@pytest.mark.parametrize("fq", [None, "pre"])
def test_partial_pause_keeps_240_rows_and_real_bars(skip, fill, fq):
    """核对盘中暂停参数矩阵；输入标志与复权模式，无返回，原始成交行必须逐格保留。"""
    gateway = FrozenGateway()
    result = request(
        gateway, skip_paused=skip, fill_paused=fill, fq=fq, pre_factor_ref_date="2026-09-24"
    )
    assert len(result) == 240
    assert (result["paused"] == 0).all()
    assert (result.iloc[:60][FIELDS[:4]] == 2.308).all().all()
    assert (result.iloc[:60][["volume", "money"]] == 0).all().all()
    raw = gateway.frames[("513100.SH", "1m", False)].loc["2026-09-24", FIELDS[:6]]
    pd.testing.assert_frame_equal(
        result.loc[raw.index, FIELDS[:6]], raw, check_names=False, check_freq=False
    )


@pytest.mark.parametrize("skip", [False, True])
@pytest.mark.parametrize("count", [5, 60, 200, 240, 241])
def test_count_includes_partial_pause_minutes(skip, count):
    """核对count和skip组合；输入数量与标志，无返回，盘中暂停不得使count提前跨天。"""
    gateway = FrozenGateway()
    full = request(gateway, start_date="2026-09-23 09:30", skip_paused=skip)
    result = request(gateway, start_date=None, count=count, skip_paused=skip)
    pd.testing.assert_frame_equal(result, full.tail(count))


@pytest.mark.parametrize("skip", [False, True])
@pytest.mark.parametrize("fill", [False, True])
@pytest.mark.parametrize("frequency", ["daily", "1m"])
def test_full_day_pause_matches_rpc_and_mirror(skip, fill, frequency):
    """重放真实整日停牌；输入参数矩阵，无返回，停牌全部字段逐格对齐两聚宽源。"""
    gateway = FrozenGateway()
    start, end = (
        ("2026-06-12", "2026-06-16")
        if frequency == "daily"
        else ("2026-06-15 09:30", "2026-06-15 15:00")
    )
    result = request(
        gateway,
        security="000929.XSHE",
        start_date=start,
        end_date=end,
        frequency=frequency,
        skip_paused=skip,
        fill_paused=fill,
    )
    for source in ["mirror", "rpc"]:
        wire = gateway.evidence["jq_full_day"][f"{source}_{frequency}_s{int(skip)}_f{int(fill)}"][
            "frame"
        ]
        expected = pd.DataFrame(
            wire["data"], columns=wire["columns"], index=pd.to_datetime(wire["index"]), dtype=float
        )
        pd.testing.assert_index_equal(result.index, expected.index, check_names=False)
        if frequency == "daily":
            # 成交日源端量额有精度差异，此处比较所验证的停牌日全部字段。
            result_day = result.loc[result.index == pd.Timestamp("2026-06-15")]
            expected = expected.loc[expected.index == pd.Timestamp("2026-06-15")]
        else:
            result_day = result
        pd.testing.assert_frame_equal(
            result_day,
            expected,
            check_names=False,
            check_freq=False,
            check_dtype=not result_day.empty,
        )


@pytest.mark.parametrize("skip", [False, True])
@pytest.mark.parametrize("fill", [False, True])
def test_before_resume_does_not_read_future_price(skip, fill):
    """验证历史截断不穿越；输入标志，无返回，窗口无可见成交价时明确失败且不抓未来分钟。"""
    gateway = FrozenGateway()
    with pytest.raises(RemoteServerError, match="截止时间内可见成交价格"):
        request(gateway, end_date="2026-09-24 10:00", skip_paused=skip, fill_paused=fill)
    for path, payload in gateway.calls:
        if path == "/data/history" and payload["frequency"] == "1m":
            assert pd.Timestamp(payload["end"]) <= pd.Timestamp("2026-09-24 10:00:59")


@pytest.mark.parametrize(
    "fault", ["flag", "missing", "volume", "changed_real", "daily", "missing_real"]
)
def test_ambiguous_or_contradictory_facts_fail(fault):
    """注入异常事实；输入故障类别，无返回，缺数据与矛盾数据不能变成合成行情。"""
    gateway = FrozenGateway()
    key = ("513100.SH", "1m", True)
    frame = gateway.frames[key]
    stamp = pd.Timestamp("2026-09-24 09:45")
    if fault == "flag":
        frame.loc[stamp, "suspendFlag"] = 0
    elif fault == "missing":
        gateway.frames[key] = frame.drop(stamp)
    elif fault == "volume":
        frame.loc[stamp, "volume"] = 100
    elif fault == "changed_real":
        frame.loc[pd.Timestamp("2026-09-24 11:00"), "close"] += 0.001
    elif fault == "daily":
        gateway.frames[("513100.SH", "1d", True)].loc["2026-09-24", "suspendFlag"] = 1
    else:
        gateway.frames[key] = frame.drop(pd.Timestamp("2026-09-24 11:00"))
    with pytest.raises(RemoteServerError, match="停牌事实|成交量额|事实矛盾|已有成交分钟"):
        request(gateway)


def test_middle_gap_uses_prior_close_not_later_open():
    """注入有明确事实的盘中缺口；无输入返回，补价使用此前收盘且保留零量额。"""
    gateway = FrozenGateway()
    stamp = pd.Timestamp("2026-09-24 11:00")
    raw_key, fill_key = ("513100.SH", "1m", False), ("513100.SH", "1m", True)
    prior = gateway.frames[raw_key].loc[stamp - pd.Timedelta(minutes=1), "close"]
    gateway.frames[raw_key] = gateway.frames[raw_key].drop(stamp)
    gateway.frames[fill_key].loc[stamp, ["suspendFlag", "volume", "money"]] = [1, 0, 0]
    result = request(gateway)
    assert (result.loc[stamp, FIELDS[:4]] == prior).all()
    assert (result.loc[stamp, FIELDS[4:]] == 0).all()


@pytest.mark.parametrize(
    "security,frequency,start,end,rows",
    [
        ("513100.XSHG", "1m", "2026-09-24 09:30", "2026-09-24 15:00", 240),
        ("000929.XSHE", "daily", "2026-06-12", "2026-06-16", 3),
    ],
)
def test_public_api_defaults_keep_pause_semantics(
    monkeypatch, security, frequency, start, end, rows
):
    """通过公开get_price检查默认停牌参数；输入样本与夹具，无返回，不依赖真实认证。"""
    provider, calls = _client(FrozenGateway())

    def get_provider():
        """无输入，返回冻结Provider，不进行认证或网络连接。"""
        return provider

    monkeypatch.setattr(data_api, "_ensure_auth", get_provider)
    monkeypatch.setattr(data_api, "_get_default_provider", get_provider)
    result = data_api.get_price(
        security, start_date=start, end_date=end, frequency=frequency, fields=FIELDS, fq=None
    )
    assert len(result) == rows
    assert calls[0]["skip_paused"] is False
    assert calls[0]["fill_paused"] is True
    if frequency == "daily":
        assert result.loc["2026-06-15", "close"] == 9.48
        assert result.loc["2026-06-15", "paused"] == 1
    else:
        assert (result["paused"] == 0).all()


@pytest.mark.parametrize("frequency,count", [("5m", 48), ("60m", 4)])
def test_partial_pause_aggregation_preserves_volume(frequency, count):
    """验证暂停分钟参与固定周期合成；输入周期和数量，无返回，量额守恒且开盘价一致。"""
    gateway = FrozenGateway()
    result = request(gateway, frequency=frequency, fields=FIELDS[:6])
    assert len(result) == count
    assert result.iloc[0]["open"] == 2.308
    raw = gateway.frames[("513100.SH", "1m", False)].loc["2026-09-24"]
    assert result["volume"].sum() == raw["volume"].sum()
    assert result["money"].sum() == raw["money"].sum()


@pytest.mark.parametrize("case", CASES, ids=[case["id"] for case in CASES])
def test_frozen_big_qmt_uses_shared_provider_contract(case):
    """输入跨源固定用例，无返回，WHZ样本重放必须通过与其他provider相同的断言。"""
    provider, _ = _client(FrozenGateway())
    assert_pause_contract(provider.get_price(**case["request"]), case)
