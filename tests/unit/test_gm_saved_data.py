"""重放实测数据；已有 vendor 差异必须保持可见，避免比较器退化后假通过。"""

import json
from pathlib import Path

import pandas as pd
import pytest

from bullet_trade.integrations.gm.validation import compare_bars, gm_bars, legacy_dividends

DATA = json.loads((Path(__file__).parents[1] / "fixtures/gm_rpc_parity_20261003.json").read_text())


@pytest.mark.parametrize("name", list(DATA["cases"]))
def test_saved_rpc_and_gm_rows_preserve_acceptance_findings(name):
    sample = DATA["cases"][name]
    reference = pd.DataFrame(sample["reference"]).set_index("time")
    observed = gm_bars(sample["gm_rows"], sample["security"])
    result = compare_bars(reference, observed, sample["tick"])
    assert result["ok"] is sample["expected_ok"]
    if name == "stock_cash_july_pre":
        assert all(result["fields"][f]["ok"] for f in ["open", "high", "low", "close", "money"])
        assert not result["fields"]["volume"]["ok"]
    elif name == "minute_etf_raw":
        assert result["fields"]["money"]["max_abs_diff"] == 1.0
        assert all(result["fields"][f]["ok"] for f in ["open", "high", "low", "close", "volume"])
    elif name == "paused_fill":
        assert len(result["missing"]) == 5


def test_live_gm_dividend_units_remain_separate_from_rpc_acceptance():
    # 这些为 GM 实测；RPC Finance 被线程问题阻塞，不能标成已通过对齐。
    stock = legacy_dividends(DATA["dividend_units"]["stock_dividend"], "601318.XSHG")
    assert [x["cash_per_share"] for x in stock] == [1.5, 0.93]
    transfer = legacy_dividends(DATA["dividend_units"]["transfer_dividend"], "300750.XSHE")
    assert transfer[0]["date"] == "2023-04-26"
    assert transfer[0]["scale_factor"] == pytest.approx(1.8)
    assert transfer[0]["cash_per_share"] == 2.52
    fund = legacy_dividends(DATA["dividend_units"]["fund_dividend_legacy"], "511880.XSHG")
    assert fund[0]["cash_per_share"] == 1.5521
