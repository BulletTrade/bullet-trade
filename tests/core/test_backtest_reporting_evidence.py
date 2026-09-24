"""验证图表与可复算成交证据采用同一资金口径。"""

import matplotlib.pyplot as plt
import pandas as pd
import plotly.io as pio
import pytest

from bullet_trade.core import analysis
from bullet_trade.core.equity_statistics import annotate_equity_trade_pnls
from bullet_trade.reporting import _build_chart_images, _build_drawdown_figure


def results_with_first_day_loss():
    values = [90.0, 105.0, 94.5]
    frame = pd.DataFrame(
        {
            "total_value": values,
            "cash": values,
            "positions_value": [0.0] * 3,
            "daily_returns": [-.1, 105 / 90 - 1, -.1],
        },
        index=pd.date_range("2024-01-02", periods=3),
    )
    return {
        "daily_records": frame, "trades": [], "events": [],
        "summary": {"初始资金": "100.00"},
        "meta": {"initial_total_value": 100.0},
    }


def test_static_chart_includes_initial_equity_in_returns_and_drawdown(monkeypatch):
    results = results_with_first_day_loss()
    captured = []
    monkeypatch.setattr(analysis, "_ensure_plot_fonts", lambda: None)
    with monkeypatch.context() as patch:
        patch.setattr(plt, "close", lambda figure: captured.append(figure))
        analysis.plot_results(results)
    figure = captured[-1]
    try:
        assert figure.axes[1].lines[0].get_ydata() == pytest.approx([-10, 5, -5.5])
        drawdown_axis = next(axis for axis in figure.axes if axis.get_ylabel() == "回撤 (%)")
        assert drawdown_axis.lines[0].get_ydata() == pytest.approx([-10, 0, -10])
        assert any("初始资金" in text.get_text() for text in drawdown_axis.texts)
        assert min(drawdown_axis.lines[0].get_ydata()) == analysis.calculate_metrics(results)["最大回撤"]
    finally:
        plt.close(figure)


def test_interactive_chart_uses_initial_equity_and_labels_initial_peak(monkeypatch, tmp_path):
    results = results_with_first_day_loss()
    figures = []

    def capture(figure, **kwargs):
        figures.append(figure)
        return "<div>chart</div>"

    monkeypatch.setattr(pio, "to_html", capture)
    analysis.generate_html_report(results, output_file=str(tmp_path / "report.html"))
    total = next(figure for figure in figures if figure.layout.title.text == "总资产与回撤")
    traces = {trace.name: trace for trace in total.data}
    assert list(traces["初始资金"].y) == [100, 100, 100]
    assert list(traces["回撤 (%)"].y) == pytest.approx([-10, 0, -10])
    assert any("初始资金" in item.text for item in total.layout.annotations)


def test_cli_drawdown_receives_initial_equity_and_keeps_legacy_fallback(monkeypatch):
    results = results_with_first_day_loss()
    frame = results["daily_records"]
    captured = []

    def capture(figure):
        captured.append(figure)
        return "data:image/png;base64,example"

    monkeypatch.setattr("bullet_trade.reporting._figure_to_data_url", capture)
    _build_chart_images(frame, results["meta"])
    try:
        drawdown = next(figure for figure in captured if figure.axes[0].get_title() == "最大回撤曲线")
        assert drawdown.axes[0].lines[0].get_ydata() == pytest.approx([-10, 0, -10])
    finally:
        for figure in captured:
            plt.close(figure)
    legacy = _build_drawdown_figure(frame)
    try:
        assert legacy.axes[0].lines[0].get_ydata() == pytest.approx([0, 0, -10])
    finally:
        plt.close(legacy)


def test_export_keeps_display_columns_and_lossless_fee_columns(tmp_path):
    records = [
        dict(security="000001.XSHE", time="2024-01-02", amount=100, price=10,
             commission=.12345, tax=.01005),
        dict(security="000001.XSHE", time="2024-01-03", amount=-100, price=11,
             commission=.23456, tax=.02006),
    ]
    annotate_equity_trade_pnls(records, [])
    path = tmp_path / "trades.csv"
    analysis.export_trades({"trades": records}, str(path))
    exported = pd.read_csv(path)
    assert exported["手续费"].tolist() == [.12, .23]
    assert exported["印花税"].tolist() == [.01, .02]
    assert exported["commission"].tolist() == pytest.approx([.12345, .23456])
    assert exported["tax"].tolist() == pytest.approx([.01005, .02006])
    assert exported["cost"].tolist() == pytest.approx([.1335, .25462])
    sale = exported.iloc[1]
    assert sale["realized_pnl_net"] == pytest.approx(
        sale["realized_pnl_gross"] - sale["allocated_entry_fees"] - sale["commission"] - sale["tax"]
    )
    reconstructed = exported.to_dict("records")
    assert analysis._get_trade_attr(reconstructed[0], "commission") == pytest.approx(.12345)
    second_path = tmp_path / "trades_again.csv"
    analysis.export_trades({"trades": reconstructed}, str(second_path))
    pd.testing.assert_frame_equal(exported, pd.read_csv(second_path))
