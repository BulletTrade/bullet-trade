"""正式 GM Provider 验收报告复核；读取已明确采集的数据，不自动再次联网。"""

import json
from pathlib import Path

import pytest

from bullet_trade.integrations.gm.acceptance import PRACTICAL_RULES

pytestmark = [pytest.mark.e2e, pytest.mark.requires_network]

WINDOWS = [
    "recent_etf",
    "recent_bank",
    "stock_cash_july",
    "stock_cash_october",
    "stock_transfer",
    "fund_year_end",
    "etf_distribution",
    "index",
    "minute_etf",
    "minute_bank",
]
EXPECTED = [name + "_" + str(mode) for name in WINDOWS for mode in [None, "pre", "post"]] + [
    "paused_fill",
    "paused_skip",
    "holiday_empty",
    "count_raw",
    "exact_start_1m",
    "exact_start_5m",
    "minute_halt",
    "tick_skip_False",
    "tick_skip_True",
    "fields_None",
    "fields_pre",
    "fields_post",
    "minute_fields",
    "multi_symbol",
    "type_601318.XSHG",
    "type_000001.XSHE",
    "type_510300.XSHG",
    "type_511880.XSHG",
    "calendar",
    "index_constituents",
    "bars_include_False",
    "bars_include_True",
]


@pytest.fixture(scope="module")
def report(request):
    path = request.config.getoption("--gm-adapter-report")
    if not path:
        pytest.fail("需要显式 --gm-adapter-report，不能把未采集算通过")
    result = json.loads(Path(path).read_text())
    assert result["baseline"] == "JoinQuant RPC"
    assert result["alignment_mode"] == "native"
    assert result["runtime_data_sources"] == ["gm"]
    assert result["benchmark_role"] == "comparison_only_not_provider_input"
    assert result["provider_source_sha256"] and result["created_at"]
    assert result["total"] == len(EXPECTED) == 52
    assert len(result["cases"]) == 52 and {x["id"] for x in result["cases"]} == set(EXPECTED)
    assert (
        "GmDataProvider" in result["runtime_path"]
        and "production data_worker" in result["runtime_path"]
    )
    assert "Finance dividend RPC blocked" in result["not_covered"]
    profile = request.config.getoption("--gm-acceptance-profile")
    if profile == "practical":
        assert (
            result.get("acceptance_policy") == "gm_practical_v1"
        ), "实用验收需要 review_gm_acceptance.py 生成的报告；原始报告请指定 strict"
        assert result["policy_rules"] == PRACTICAL_RULES
    return {x["id"]: x for x in result["cases"]}, profile


@pytest.mark.parametrize("case_id", EXPECTED)
def test_registered_gm_provider_matches_selected_rpc_contract(report, case_id):
    cases, profile = report
    field = "practical_ok" if profile == "practical" else "strict_ok"
    assert cases[case_id].get(field, cases[case_id]["ok"]), json.dumps(
        cases[case_id], ensure_ascii=False
    )
