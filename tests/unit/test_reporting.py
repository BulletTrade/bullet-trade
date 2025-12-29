"""
报告生成模块测试
"""

import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from bullet_trade.core.analysis import generate_report, load_results_from_directory
from bullet_trade.reporting import ReportGenerationError, generate_cli_report

pytestmark = pytest.mark.unit


def _prepare_results_dir(tmp_path: Path) -> Path:
    project_root = Path(__file__).resolve().parents[3]
    sample_dir = project_root / "backtest_results"
    target_dir = tmp_path / "results"
    target_dir.mkdir()
    # 查找子目录中的 daily_records.csv 文件
    sample_file = None
    for subdir in sample_dir.iterdir():
        if subdir.is_dir():
            candidate = subdir / "daily_records.csv"
            if candidate.exists():
                sample_file = candidate
                break
    if sample_file is None:
        raise FileNotFoundError(f"未找到测试数据文件: {sample_dir}/*/daily_records.csv")
    shutil.copy(sample_file, target_dir / "daily_records.csv")
    # metrics.json 提供最小指标集
    metrics_payload = {
        "generated_at": "2024-01-01T00:00:00Z",
        "meta": {"start_date": "2023-01-01", "end_date": "2023-12-31"},
        "metrics": {
            "策略收益": 12.34,
            "策略年化收益": 10.11,
            "最大回撤": -5.67,
            "胜率": 55.0,
            "盈亏比": 1.23,
            "交易天数": 250,
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
    assert "data:image/png;base64" in content
    assert "<table" in content


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
    project_root = Path(__file__).resolve().parents[3]
    sample_dir = project_root / "backtest_results"
    target_dir = tmp_path / "results"
    target_dir.mkdir()
    # 查找子目录中的 daily_records.csv 文件
    sample_file = None
    for subdir in sample_dir.iterdir():
        if subdir.is_dir():
            candidate = subdir / "daily_records.csv"
            if candidate.exists():
                sample_file = candidate
                break
    if sample_file is None:
        raise FileNotFoundError(f"未找到测试数据文件: {sample_dir}/*/daily_records.csv")
    shutil.copy(sample_file, target_dir / "daily_records.csv")
    with pytest.raises(ReportGenerationError):
        generate_cli_report(input_dir=str(target_dir), fmt="html")


def test_generate_report_exports_metrics_json(tmp_path, monkeypatch):
    project_root = Path(__file__).resolve().parents[3]
    sample_dir = project_root / "backtest_results"
    # 查找子目录中的测试数据
    actual_sample_dir = None
    for subdir in sample_dir.iterdir():
        if subdir.is_dir() and (subdir / "daily_records.csv").exists():
            actual_sample_dir = subdir
            break
    if actual_sample_dir is None:
        raise FileNotFoundError(f"未找到测试数据目录: {sample_dir}/*/daily_records.csv")
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
