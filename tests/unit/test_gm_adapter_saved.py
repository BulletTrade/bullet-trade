"""重放独立 GM 数据；聚宽只用于断言已观察的差异，不向 Provider 提供数据。"""

import json
from pathlib import Path

import pandas as pd
import pytest

from bullet_trade.data.api import _create_provider
from bullet_trade.integrations.gm.validation import BAR_FIELDS, compare_bars

DATA = json.loads((Path(__file__).parents[1] / "fixtures/gm_independent_20261004.json").read_text())


class CapturedSdk:
    def auth(self):
        pass

    def query(self, method, **kwargs):
        key = json.dumps([method, kwargs], sort_keys=True, default=str)
        if key in DATA["sdk"]:
            return DATA["sdk"][key]
        # 新版逐分钟日历检查允许取已采集完整日历的子区间，不编造交易日。
        if method == "get_trading_dates":
            for saved, days in DATA["sdk"].items():
                name, query = json.loads(saved)
                if (
                    name == method
                    and query["exchange"] == kwargs["exchange"]
                    and query["start_date"] <= kwargs["start_date"]
                    and query["end_date"] >= kwargs["end_date"]
                ):
                    return [d for d in days if kwargs["start_date"] <= d <= kwargs["end_date"]]
        raise AssertionError("GM 重放缺少请求的实测记录")


@pytest.mark.parametrize("case", DATA["cases"], ids=lambda c: c["name"])
def test_actual_gm_payloads_replay_without_benchmark_input(case):
    provider = _create_provider("gm", dict(client=CapturedSdk()))
    actual = provider.get_price(case["security"], fields=list(BAR_FIELDS), **case["kwargs"])
    value = case["observed_gm"]
    expected = pd.DataFrame(
        value["data"], columns=value["columns"], index=pd.to_datetime(value["index"])
    )
    pd.testing.assert_frame_equal(expected, actual, check_names=False, check_freq=False)
    value = case["benchmark_jq"]
    benchmark = pd.DataFrame(
        value["data"], columns=value["columns"], index=pd.to_datetime(value["index"])
    )
    result = compare_bars(benchmark, actual, 0.01)
    # 确认比较器仍报告已采集差异，不能靠外部输入或放宽阈值把失败改成通过。
    assert result["ok"] is case["benchmark_ok"], result
    assert actual.attrs["data_source"] == "gm"
    assert set(actual.attrs["field_sources"].values()) == {"gm"}
