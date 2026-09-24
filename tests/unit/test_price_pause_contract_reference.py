"""固定停牌契约的聚宽基准回归。

作者：BruceLee
职责：确保同一断言适用于冻结的 RPC/镜像基准，且能识别错误返回。
输入：真实市场 JSON 与固定参数；输出：pytest 断言；上游为脱敏快照，下游为共享契约。
环境：本机 pytest/pandas，无认证、网络、交易或基准自动重写。
"""

import json
from pathlib import Path

import pandas as pd
import pytest
from tests.price_pause_contract import CASES, FIELDS, assert_pause_contract

pytestmark = pytest.mark.unit
FIXTURES = Path(__file__).parents[1] / "fixtures"


def reference_frame(case):
    """输入固定用例，返回对应 RPC 原始结果表；无外部副作用，参数不匹配直接断言失败。"""
    rows = json.loads((FIXTURES / "price_pause_jq_reference_20260924.json").read_text())["cases"]
    row = next(row for row in rows if row["case_id"] == case["id"])
    assert row["request"] == case["request"]
    wire = row["frame"]
    return pd.DataFrame(wire["data"], columns=wire["columns"], index=pd.to_datetime(wire["index"]))


@pytest.mark.parametrize("case", CASES, ids=[case["id"] for case in CASES])
def test_frozen_rpc_matches_fixed_contract(case):
    """输入固定用例，无返回，冻结 RPC 数据必须满足共享预期，不能只校验本地 QMT。"""
    assert_pause_contract(reference_frame(case), case)


@pytest.mark.parametrize(
    "case", [case for case in CASES if case["full_day"]], ids=lambda case: case["id"]
)
def test_frozen_mirror_matches_fixed_contract(case):
    """输入整日停牌用例，无返回，同一契约也必须匹配独立镜像实测。"""
    req = case["request"]
    skip, fill = int(req.get("skip_paused", False)), int(req.get("fill_paused", True))
    key = f"mirror_{req['frequency']}_s{skip}_f{fill}"
    wire = json.loads((FIXTURES / "big_qmt_pause_20260924.json").read_text())["jq_full_day"][key][
        "frame"
    ]
    frame = pd.DataFrame(wire["data"], columns=wire["columns"], index=pd.to_datetime(wire["index"]))
    assert_pause_contract(frame, case)


@pytest.mark.parametrize(
    "fault",
    ["missing_row", "wrong_paused", "wrong_price", "nonzero_volume", "empty", "wrong_field"],
)
def test_shared_contract_rejects_incompatible_results(fault):
    """输入常见兼容错误，无返回，确认契约不会因空表或字段外形正确而误报通过。"""
    case = next(case for case in CASES if case["id"] == "qdii_etf_partial_pause_minute_defaults")
    frame = reference_frame(case)
    first = frame.index[0]
    if fault == "missing_row":
        frame = frame.iloc[1:]
    elif fault == "wrong_paused":
        frame.loc[first, "paused"] = 1
    elif fault == "wrong_price":
        frame.loc[first, "close"] = 2.334
    elif fault == "nonzero_volume":
        frame.loc[first, "volume"] = 100
    elif fault == "empty":
        frame = pd.DataFrame(columns=FIELDS, index=pd.DatetimeIndex([]))
    else:
        frame = frame.rename(columns={"money": "amount"})
    with pytest.raises(AssertionError):
        assert_pause_contract(frame, case)
