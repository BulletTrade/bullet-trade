"""验收报告生成器必须有来源事实，不能仅翻转通过标志或从价格拟合倍数。"""

import hashlib
import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).parents[2]
spec = importlib.util.spec_from_file_location("gm_review", ROOT / "scripts/review_gm_acceptance.py")
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)


@pytest.fixture
def captured(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    index = pd.to_datetime(["2024-07-25", "2024-07-26"])
    reference = pd.DataFrame(
        dict(close=[4.0, 4.4], volume=[1000.0, 909.0], money=[4000.0, 4000.0]), index=index
    )
    observed = reference.copy()
    observed.close *= 2
    observed.volume /= 2
    observed.to_csv(source / "recent_etf_post_adapter.csv")
    reference.to_csv(source / "recent_etf_post_jq.csv")
    report = dict(
        total=1,
        alignment_mode="native",
        runtime_data_sources=["gm"],
        provider_source_sha256=hashlib.sha256(
            (ROOT / "bullet_trade/data/providers/gm.py").read_bytes()
        ).hexdigest(),
        cases=[
            dict(
                id="recent_etf_post",
                ok=False,
                comparison={"ok": False},
                field_sources={"close": "gm"},
            )
        ],
    )
    (source / "report.json").write_text(json.dumps(report))
    sdk = {
        json.dumps(["get_instrumentinfos", {}]): [
            dict(symbol="SHSE.510300", price_tick=0.001, sec_type=2)
        ],
        json.dumps(["get_history_instruments", {}]): [
            dict(symbol="SHSE.510300", trade_date="2024-07-25", adj_factor=2.0)
        ],
    }
    (source / "sdk_reads.json").write_text(json.dumps(sdk))
    factors = tmp_path / "benchmark.json"
    factors.write_text(json.dumps({"510300.XSHG": [dict(date="2024-07-25", factor=1.0)]}))
    return source, factors


def test_common_basis_requires_each_sources_own_factor_and_preserves_original_report(captured):
    source, factors = captured
    before = (source / "report.json").read_bytes()
    result = review.review_report(source, factors)
    assert result["passed"] == 1 and result["strict_passed"] == 0
    assert result["cases"][0]["common_basis"]["gm_factor"] == 2
    assert result["cases"][0]["common_basis"]["benchmark_factor"] == 1
    assert (source / "report.json").read_bytes() == before


@pytest.mark.parametrize(
    "kind", ["missing_gm_factor", "missing_benchmark_factor", "invalid_factor", "missing_csv"]
)
def test_price_ratio_cannot_substitute_missing_basis_evidence(captured, kind):
    source, factors = captured
    if kind == "missing_gm_factor":
        value = json.loads((source / "sdk_reads.json").read_text())
        value.pop(json.dumps(["get_history_instruments", {}]))
        (source / "sdk_reads.json").write_text(json.dumps(value))
    elif kind == "missing_benchmark_factor":
        factors.write_text("{}")
    elif kind == "invalid_factor":
        factors.write_text(json.dumps({"510300.XSHG": [dict(date="2024-07-25", factor=0)]}))
    elif kind == "missing_csv":
        (source / "recent_etf_post_adapter.csv").unlink()
    assert not review.review_report(source, factors)["ok"]


@pytest.mark.parametrize(
    "kind", ["mixed_source", "stale_provider", "duplicated_case", "external_field"]
)
def test_untrustworthy_report_is_not_accepted(captured, kind):
    source, factors = captured
    value = json.loads((source / "report.json").read_text())
    if kind == "mixed_source":
        value["runtime_data_sources"] = ["gm", "joinquant"]
    elif kind == "stale_provider":
        value["provider_source_sha256"] = "wrong"
    elif kind == "duplicated_case":
        value["cases"] *= 2
        value["total"] = 2
    elif kind == "external_field":
        value["cases"][0]["field_sources"]["close"] = "joinquant"
    (source / "report.json").write_text(json.dumps(value))
    with pytest.raises(ValueError):
        review.review_report(source, factors)


def test_non_numeric_contract_failure_is_not_waived(captured):
    source, factors = captured
    value = json.loads((source / "report.json").read_text())
    value["cases"] = [dict(id="calendar", ok=False, error_type="TimeoutError")]
    (source / "report.json").write_text(json.dumps(value))
    result = review.review_report(source, factors)
    assert result["passed"] == 0 and not result["ok"]
