"""
报告生成模块测试
"""

import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from bullet_trade.core import analysis as analysis_module
from bullet_trade.core.analysis import generate_html_report, generate_report, load_results_from_directory
from bullet_trade.reporting import ReportGenerationError, generate_cli_report

pytestmark = pytest.mark.unit


def _prepare_results_dir(tmp_path: Path) -> Path:
    target_dir = tmp_path / "results"
    target_dir.mkdir(parents=True)

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "total_value": [100000.0, 101000.0, 102500.0],
            "cash": [100000.0, 50000.0, 40000.0],
            "positions_value": [0.0, 51000.0, 62500.0],
            "returns": [0.0, 1000.0, 2500.0],
            "returns_pct": [0.0, 1.0, 2.5],
            "daily_returns": [0.0, 0.01, 0.0148514851],
            "benchmark_price": [4000.0, 4040.0, 4080.0],
            "benchmark_value": [100000.0, 101000.0, 102000.0],
            "benchmark_returns_pct": [0.0, 1.0, 2.0],
            "excess_returns_pct": [0.0, 0.0, 0.5],
        }
    ).set_index("date")
    df.to_csv(target_dir / "daily_records.csv", encoding="utf-8-sig")

    metrics_payload = {
        "generated_at": "2024-01-01T00:00:00Z",
        "meta": {
            "start_date": "2024-01-02",
            "end_date": "2024-01-04",
            "run_started_at": "2024-01-05 09:30:00",
            "run_finished_at": "2024-01-05 09:30:03",
            "runtime_seconds": 3.21,
            "benchmark": "000300.XSHG",
            "initial_total_value": 100000.0,
        },
        "metrics": {
            "策略收益": 2.50,
            "策略年化收益": 10.11,
            "基准收益": 2.00,
            "累计超额收益": 0.50,
            "最大回撤": -0.80,
            "胜率": 55.0,
            "盈亏比": 1.23,
            "交易天数": 3,
        },
    }
    (target_dir / "metrics.json").write_text(
        json.dumps(metrics_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return target_dir


def test_generate_cli_report_html(tmp_path):
    results_dir = _prepare_results_dir(tmp_path)
    report_path = generate_cli_report(input_dir=str(results_dir), fmt="html")
    assert report_path.exists()
    content = report_path.read_text(encoding="utf-8")
    assert content.count("data:image/png;base64") >= 4
    assert "<table" in content
    assert "Benchmark" in content
    assert "000300.XSHG" in content
    assert "累计超额收益" in content
    assert "回测启动时间" in content


def test_generate_cli_report_pdf(tmp_path):
    results_dir = _prepare_results_dir(tmp_path)
    report_path = generate_cli_report(input_dir=str(results_dir), fmt="pdf")
    assert report_path.exists()
    assert report_path.read_bytes().startswith(b"%PDF")


def test_generate_cli_report_with_metric_filter(tmp_path):
    results_dir = _prepare_results_dir(tmp_path)
    custom_path = results_dir / "custom.html"
    report_path = generate_cli_report(
        input_dir=str(results_dir),
        output_path=str(custom_path),
        fmt="html",
        metrics_keys=["胜率", "策略收益"],
    )
    html = report_path.read_text(encoding="utf-8")
    assert html.index("胜率") < html.index("策略收益")


def test_generate_cli_report_missing_metrics(tmp_path):
    source_dir = _prepare_results_dir(tmp_path / "source")
    target_dir = tmp_path / "results"
    target_dir.mkdir()
    shutil.copy(source_dir / "daily_records.csv", target_dir / "daily_records.csv")
    with pytest.raises(ReportGenerationError):
        generate_cli_report(input_dir=str(target_dir), fmt="html")


def test_generate_report_exports_metrics_json(tmp_path, monkeypatch):
    actual_sample_dir = _prepare_results_dir(tmp_path)
    monkeypatch.setattr(
        "bullet_trade.data.api.get_all_securities",
        lambda types=None: pd.DataFrame(),
        raising=False,
    )
    results = load_results_from_directory(str(actual_sample_dir))
    output_dir = tmp_path / "output"
    generate_report(
        results=results,
        output_dir=str(output_dir),
        gen_images=False,
        gen_csv=False,
        gen_html=False,
    )
    metrics_path = output_dir / "metrics.json"
    assert metrics_path.exists()
    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert "metrics" in payload
    assert payload["meta"]["benchmark"] == "000300.XSHG"
    assert payload["meta"]["run_started_at"] == "2024-01-05 09:30:00"


def test_load_results_from_directory_merges_metrics_meta(tmp_path):
    results_dir = _prepare_results_dir(tmp_path)
    results = load_results_from_directory(str(results_dir))
    assert results["meta"]["benchmark"] == "000300.XSHG"
    assert results["meta"]["run_started_at"] == "2024-01-05 09:30:00"


def test_generate_html_report_includes_benchmark_and_run_context(tmp_path):
    results_dir = _prepare_results_dir(tmp_path)
    html = generate_html_report(results_dir=str(results_dir))
    assert "Benchmark: 000300.XSHG" in html
    assert "回测启动: 2024-01-05 09:30:00" in html
    assert "BulletTrade 回测" in html
    assert "https://bullettrade.cn/favicon.svg" in html
    assert '"autorange":"reversed"' in html
    assert "return raw.toFixed(1) + '%'" in html
    assert "回撤 / 超额收益 (%)" not in html
    assert '"yaxis3"' not in html
    assert '"text":"回撤 (%)"' in html
    assert '"text":"资产 / 超额资产 (元)"' in html
    assert '超额资产 (元)' in html


def test_analysis_resample_aliases_work_across_supported_pandas_versions():
    series = pd.Series(
        [0.01, 0.02, -0.01],
        index=pd.to_datetime(["2023-12-29", "2024-01-03", "2024-02-01"]),
    )

    annual = series.resample(analysis_module._YEAR_END_FREQ).sum()
    monthly = series.resample(analysis_module._MONTH_END_FREQ).sum()

    assert annual.index.year.tolist() == [2023, 2024]
    assert annual.round(4).tolist() == [0.01, 0.01]
    assert monthly.index.year.tolist() == [2023, 2024, 2024]
    assert monthly.index.month.tolist() == [12, 1, 2]
    assert monthly.round(4).tolist() == [0.01, 0.02, -0.01]
