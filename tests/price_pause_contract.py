"""跨数据源的固定停牌契约样例。

作者：BruceLee
职责：保存明确证券、历史窗口、参数及聚宽预期，供任意 get_price 数据源复用。
输入：调用方返回的 DataFrame；输出：时间轴、字段、停牌值的严格断言。
上下游：离线市场重放或显式联网 provider 测试 -> 本模块；不访问网络或账户。
环境约定：pytest/pandas/numpy；日期永久固定，不使用今天，不从被测数据推导预期。
"""

from copy import deepcopy

import numpy as np
import pandas as pd

FIELDS = ["open", "high", "low", "close", "volume", "money", "paused"]

# 2026-09-24 实测：000929 的 RPC 与镜像八组完全一致；513100 当天镜像未覆盖，
# 盘中暂停基准来自 RPC，不能把过期镜像自动填充的 240 根当作有效基准。
SCENARIOS = (
    {
        "id": "stock_full_pause_daily",
        "security_type": "深市 A 股股票，兰州黄河，撤销退市风险警示时整日停牌",
        "source": "JoinQuant RPC + JQData 镜像，2026-09-24 冻结",
        "announcement": "https://static.cninfo.com.cn/finalpage/2026-06-13/1225369698.PDF",
        "request": {
            "security": "000929.XSHE",
            "start_date": "2026-06-12",
            "end_date": "2026-06-16",
            "frequency": "daily",
        },
        "days": ["2026-06-12", "2026-06-15", "2026-06-16"],
        "pause_start": "2026-06-15",
        "pause_end": "2026-06-15",
        "pause_price": 9.48,
        "full_day": True,
    },
    {
        "id": "stock_full_pause_minute",
        "security_type": "深市 A 股股票，兰州黄河，整日停牌",
        "source": "JoinQuant RPC + JQData 镜像，2026-09-24 冻结",
        "request": {
            "security": "000929.XSHE",
            "start_date": "2026-06-15 09:30:00",
            "end_date": "2026-06-15 15:00:00",
            "frequency": "1m",
        },
        "days": ["2026-06-15"],
        "pause_start": "2026-06-15 09:31:00",
        "pause_end": "2026-06-15 15:00:00",
        "pause_price": 9.48,
        "full_day": True,
    },
    {
        "id": "qdii_etf_partial_pause_minute",
        "security_type": "沪市 QDII ETF，国泰纳斯达克 100，开盘暂停至 10:30",
        "source": "JoinQuant RPC，2026-09-24 冻结；同期镜像尚未覆盖，不作此日基准",
        "request": {
            "security": "513100.XSHG",
            "start_date": "2026-09-24 09:30:00",
            "end_date": "2026-09-24 15:00:00",
            "frequency": "1m",
        },
        "days": ["2026-09-24"],
        "pause_start": "2026-09-24 09:31:00",
        "pause_end": "2026-09-24 10:30:00",
        "pause_price": 2.308,
        "full_day": False,
    },
)


def pause_cases():
    """无输入，返回固定三场景及参数矩阵；独立复制参数，不修改基准或读取当前日期。"""
    cases = []
    for scenario in SCENARIOS:
        for flags in [None, (False, False), (False, True), (True, False), (True, True)]:
            case = deepcopy(scenario)
            case["request"].update(fields=FIELDS.copy(), fq=None, panel=False)
            if flags is None:
                case["id"] += "_defaults"
            else:
                skip, fill = flags
                case["request"].update(skip_paused=skip, fill_paused=fill)
                case["id"] += f"_skip{int(skip)}_fill{int(fill)}"
            cases.append(case)
            if not case["full_day"] and flags is not None:
                counted = deepcopy(case)
                counted["id"] += "_count200"
                counted["request"].pop("start_date")
                counted["request"]["count"] = 200
                cases.append(counted)
    return cases


CASES = pause_cases()


def assert_pause_contract(actual, case):
    """输入任意源的行情和固定用例，严格核对停牌契约；无返回，不修补或排序实际结果。

    断言全索引、字段、整日/盘中 paused、暂停段量价及空值、正常行有限非负。
    正常成交分钟的跨源 OHLC/量额逐格一致性不在本断言范围，不能据此宣称完全等价。
    不支持或覆盖不足必须由调用方报告失败，不能自动跳过、容忍全空或更换日期。
    """
    req = case["request"]
    skip, fill = req.get("skip_paused", False), req.get("fill_paused", True)
    if req["frequency"] == "daily":
        expected = pd.DatetimeIndex(case["days"])
    else:
        day = case["days"][0]
        expected = pd.date_range(day + " 09:31", day + " 11:30", freq="min").append(
            pd.date_range(day + " 13:01", day + " 15:00", freq="min")
        )
    lower, upper = pd.Timestamp(case["pause_start"]), pd.Timestamp(case["pause_end"])
    if skip and case["full_day"]:
        expected = expected[(expected < lower) | (expected > upper)]
    if req.get("count"):
        expected = expected[-req["count"] :]
    assert isinstance(actual, pd.DataFrame), case["id"]
    assert actual.columns.tolist() == FIELDS, case["id"]
    if expected.empty:
        # 聚宽空表可能携带普通空 Index；没有时间标签时不约束索引存储类型。
        assert actual.empty, case["id"]
        return
    pd.testing.assert_index_equal(actual.index, expected, check_names=False, obj=case["id"])
    paused = actual.loc[(actual.index >= lower) & (actual.index <= upper)]
    normal = actual.loc[(actual.index < lower) | (actual.index > upper)]
    if case["full_day"] and not fill and not skip:
        assert paused.isna().all().all(), case["id"]
    else:
        np.testing.assert_allclose(
            paused[FIELDS[:4]].to_numpy(dtype=float),
            case["pause_price"],
            rtol=0,
            atol=1e-12,
            err_msg=case["id"],
        )
        assert (paused[["volume", "money"]] == 0).all().all(), case["id"]
        assert (paused["paused"] == int(case["full_day"])).all(), case["id"]
    assert np.isfinite(normal.to_numpy(dtype=float)).all(), case["id"]
    assert (normal.to_numpy(dtype=float) >= 0).all(), case["id"]
    assert (normal["paused"] == 0).all(), case["id"]
