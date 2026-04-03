"""
回测报告生成模块

提供从回测结果目录构建标准化 HTML/PDF 报告的功能，并支持自定义模板与指标筛选。
"""

from __future__ import annotations

import base64
import io
import json
from dataclasses import dataclass
from datetime import datetime
from importlib import resources
from pathlib import Path
from string import Template
from typing import Any, Dict, Iterable, List, Optional, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

from bullet_trade.core import analysis as core_analysis
from bullet_trade.utils.font_config import setup_chinese_fonts

DEFAULT_TEMPLATE_NAME = "default.html"
DEFAULT_METRICS_ORDER: Sequence[str] = (
    "策略收益",
    "策略年化收益",
    "基准收益",
    "累计超额收益",
    "最大回撤",
    "最大回撤区间",
    "最大回撤持续天数",
    "夏普比率",
    "索提诺比率",
    "Calmar比率",
    "胜率",
    "盈亏比",
    "交易天数",
)


class ReportGenerationError(Exception):
    """报告生成失败时抛出的异常。"""


@dataclass
class ReportContext:
    title: str
    generated_at: str
    summary_rows: List[Dict[str, str]]
    metric_rows: List[Dict[str, str]]
    equity_image: Optional[str]
    excess_image: Optional[str]
    drawdown_image: Optional[str]
    monthly_heatmap_image: Optional[str]


def generate_cli_report(
    *,
    input_dir: str,
    output_path: Optional[str] = None,
    fmt: str = "html",
    template_path: Optional[str] = None,
    metrics_keys: Optional[Iterable[str]] = None,
    title: Optional[str] = None,
) -> Path:
    """
    从回测结果目录生成报告。

    Args:
        input_dir: 回测结果目录
        output_path: 报告输出文件路径，默认位于 input_dir 下
        fmt: 报告格式，支持 html 或 pdf
        template_path: HTML 模板路径，使用 string.Template 占位符
        metrics_keys: 需要展示的指标名称列表（保留顺序）
        title: 报告标题，默认使用目录名称

    Returns:
        生成的报告文件路径
    """

    fmt = fmt.lower()
    if fmt not in {"html", "pdf"}:
        raise ReportGenerationError(f"不支持的报告格式: {fmt}")

    # 确保字体配置已加载，避免图表/PDF 出现中文乱码
    try:
        setup_chinese_fonts()
    except Exception:
        # 字体配置异常不阻断报告生成
        pass

    results_path = Path(input_dir).expanduser().resolve()
    if not results_path.exists():
        raise ReportGenerationError(f"回测目录不存在: {results_path}")

    metrics_path = results_path / "metrics.json"
    if not metrics_path.exists():
        raise ReportGenerationError(
            f"未找到 {metrics_path.name}，请确认回测已完成并生成指标文件及图表。"
        )

    metrics_payload = _load_metrics_payload(metrics_path)
    metrics = metrics_payload["metrics"]
    meta = metrics_payload.get("meta", {})

    results = core_analysis.load_results_from_directory(str(results_path))

    context = _build_context(
        results_dir=results_path,
        results=results,
        metrics=metrics,
        metrics_keys=metrics_keys,
        generated_at=metrics_payload.get("generated_at"),
        title=title,
        meta=meta,
    )

    output_path_obj = _derive_output_path(results_path, output_path, fmt)

    if fmt == "html":
        template_text = _load_template_text(template_path)
        html = _render_html_report(context, template_text)
        output_path_obj.write_text(html, encoding="utf-8")
    else:
        _render_pdf_report(context, output_path_obj)

    return output_path_obj


def _load_metrics_payload(metrics_path: Path) -> Dict[str, Any]:
    with metrics_path.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if "metrics" not in data or not isinstance(data["metrics"], dict):
        raise ReportGenerationError(f"{metrics_path} 格式不正确，缺少 metrics 字段。")
    return data


def _derive_output_path(results_dir: Path, output_path: Optional[str], fmt: str) -> Path:
    if output_path:
        path = Path(output_path).expanduser()
        if path.is_dir():
            return path / f"report.{fmt}"
        return path.resolve()
    return results_dir / f"report.{fmt}"


def _build_context(
    *,
    results_dir: Path,
    results: Dict[str, Any],
    metrics: Dict[str, Any],
    metrics_keys: Optional[Iterable[str]],
    generated_at: Optional[str],
    title: Optional[str],
    meta: Optional[Dict[str, Any]],
) -> ReportContext:
    df: pd.DataFrame = results["daily_records"]
    if df.empty:
        raise ReportGenerationError("daily_records.csv 为空，无法生成报告。")

    merged_meta = _merge_meta(results.get("meta") or {}, meta or {})
    summary_rows = _build_summary_rows(df, merged_meta, metrics)
    metric_rows = _build_metric_rows(metrics, metrics_keys)
    charts = _build_chart_images(df, merged_meta)

    context_title = title or merged_meta.get("title")
    if not context_title:
        context_title = results_dir.name

    generated_at_value = generated_at or datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    return ReportContext(
        title=context_title,
        generated_at=generated_at_value,
        summary_rows=summary_rows,
        metric_rows=metric_rows,
        equity_image=charts.get("equity"),
        excess_image=charts.get("excess"),
        drawdown_image=charts.get("drawdown"),
        monthly_heatmap_image=charts.get("monthly_heatmap"),
    )


def _merge_meta(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base or {})
    for key, value in (overlay or {}).items():
        if key not in merged or merged.get(key) in (None, "", {}, []):
            merged[key] = value
    return merged


def _build_summary_rows(
    df: pd.DataFrame,
    meta: Dict[str, Any],
    metrics: Dict[str, Any],
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    start_date = meta.get("start_date")
    end_date = meta.get("end_date")
    if not start_date and not df.empty:
        start_date = df.index.min().strftime("%Y-%m-%d")
    if not end_date and not df.empty:
        end_date = df.index.max().strftime("%Y-%m-%d")

    initial_value = float(df["total_value"].iloc[0]) if not df.empty else None
    final_value = float(df["total_value"].iloc[-1]) if not df.empty else None
    trading_days = len(df)
    runtime_seconds = meta.get("runtime_seconds")

    rows.append({"label": "回测开始日期", "value": start_date or "-"})
    rows.append({"label": "回测结束日期", "value": end_date or "-"})
    rows.append({"label": "回测启动时间", "value": str(meta.get("run_started_at") or "-")})
    rows.append({"label": "回测结束时间", "value": str(meta.get("run_finished_at") or "-")})
    rows.append(
        {
            "label": "总耗时",
            "value": f"{float(runtime_seconds):.2f}s"
            if isinstance(runtime_seconds, (int, float))
            else "-",
        }
    )
    rows.append({"label": "Benchmark", "value": str(meta.get("benchmark") or "-")})
    rows.append({"label": "交易天数", "value": str(trading_days)})
    rows.append({"label": "初始资金", "value": _format_currency(initial_value)})
    rows.append({"label": "期末资产", "value": _format_currency(final_value)})
    if "基准收益" in metrics:
        rows.append({"label": "基准收益", "value": _format_metric_value("基准收益", metrics.get("基准收益"))})
    if "累计超额收益" in metrics:
        rows.append(
            {
                "label": "累计超额收益",
                "value": _format_metric_value("累计超额收益", metrics.get("累计超额收益")),
            }
        )

    return rows


def _build_metric_rows(
    metrics: Dict[str, Any], metrics_keys: Optional[Iterable[str]]
) -> List[Dict[str, str]]:
    if metrics_keys:
        order = [key.strip() for key in metrics_keys if key.strip()]
    else:
        order = [key for key in DEFAULT_METRICS_ORDER if key in metrics]
        # append rest deterministically
        rest = [key for key in metrics.keys() if key not in order]
        order.extend(rest)

    rows: List[Dict[str, str]] = []
    for key in order:
        if key not in metrics:
            continue
        rows.append({"label": key, "value": _format_metric_value(key, metrics[key])})
    return rows


def _build_chart_images(df: pd.DataFrame, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Optional[str]]:
    charts: Dict[str, Optional[str]] = {
        "equity": None,
        "excess": None,
        "drawdown": None,
        "monthly_heatmap": None,
    }
    if df.empty:
        return charts

    charts["equity"] = _figure_to_data_url(_build_equity_figure(df, meta or {}))
    charts["excess"] = _figure_to_data_url(_build_excess_figure(df, meta or {}))
    charts["drawdown"] = _figure_to_data_url(_build_drawdown_figure(df))
    charts["monthly_heatmap"] = _figure_to_data_url(_build_monthly_heatmap_figure(df))
    return charts


def _build_equity_figure(df: pd.DataFrame, meta: Dict[str, Any]):
    fig, ax = plt.subplots(figsize=(10, 4))
    base = float(df["total_value"].iloc[0])
    ax.plot(df.index, df["total_value"], color="#1f77b4", linewidth=2)
    benchmark_ctx = core_analysis._compute_benchmark_context(
        df,
        base_value=meta.get("initial_total_value"),
    )
    benchmark_value = benchmark_ctx.get("benchmark_value")
    benchmark_code = meta.get("benchmark") or "benchmark"
    if benchmark_value is not None:
        ax.plot(
            df.index,
            benchmark_value,
            color="#ff7f0e",
            linewidth=2,
            linestyle=":",
            label=f"Benchmark({benchmark_code})",
        )
    ax.set_title("账户净值曲线", fontsize=12)
    ax.set_ylabel("总资产 (元)")
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis="x", rotation=30)
    ax.axhline(y=base, linestyle="--", color="#ff7f0e", alpha=0.5, label="初始资金")
    ax.legend(loc="best")
    fig.tight_layout()
    return fig


def _build_excess_figure(df: pd.DataFrame, meta: Dict[str, Any]):
    fig, ax = plt.subplots(figsize=(10, 4))
    benchmark_ctx = core_analysis._compute_benchmark_context(
        df,
        base_value=meta.get("initial_total_value"),
    )
    excess_returns = benchmark_ctx.get("excess_returns_pct")
    if excess_returns is None:
        ax.text(0.5, 0.5, "无 benchmark 数据", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        return fig
    ax.plot(df.index, excess_returns, color="#9467bd", linewidth=2)
    ax.axhline(y=0.0, linestyle="--", color="#666", alpha=0.6)
    ax.set_title("累计超额收益曲线", fontsize=12)
    ax.set_ylabel("超额收益 (%)")
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    return fig


def _build_drawdown_figure(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(10, 4))
    cummax = df["total_value"].cummax()
    drawdown = (df["total_value"] - cummax) / cummax * 100
    ax.fill_between(df.index, drawdown, 0, color="#d62728", alpha=0.3)
    ax.plot(df.index, drawdown, color="#d62728", linewidth=2)
    ax.set_title("最大回撤曲线", fontsize=12)
    ax.set_ylabel("回撤 (%)")
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    return fig


def _build_monthly_heatmap_figure(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(10, 4))
    daily_returns = df["daily_returns"].dropna()
    if daily_returns.empty:
        ax.text(0.5, 0.5, "无每日收益数据", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        return fig

    monthly = (daily_returns + 1).groupby([daily_returns.index.year, daily_returns.index.month]).prod() - 1
    heatmap = monthly.unstack(level=1)
    months = list(range(1, 13))
    heatmap = heatmap.reindex(columns=months)
    heatmap.index.name = "年份"
    heatmap.columns = [f"{m}月" for m in months]

    data = heatmap.fillna(0).values * 100
    vmax = np.max(np.abs(data)) if np.any(data) else 1.0
    if vmax == 0 or not np.isfinite(vmax):
        vmax = 1.0

    im = ax.imshow(data, aspect="auto", cmap="RdYlGn_r", vmin=-vmax, vmax=vmax)
    ax.set_title("月度收益热力图 (%)", fontsize=12)
    ax.set_xlabel("月份")
    ax.set_ylabel("年份")
    ax.set_xticks(np.arange(len(months)))
    ax.set_xticklabels(heatmap.columns, rotation=45, ha="right")
    ax.set_yticks(np.arange(len(heatmap.index)))
    ax.set_yticklabels(heatmap.index.astype(int))

    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            val = data[i, j]
            label = f"{val:.1f}"
            text_color = "black" if abs(val) < vmax * 0.6 else "white"
            ax.text(j, i, label, ha="center", va="center", color=text_color, fontsize=7)

    fig.colorbar(im, ax=ax, shrink=0.8, label="收益 (%)")
    fig.tight_layout()
    return fig


def _figure_to_data_url(fig: plt.Figure) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", bbox_inches="tight")
    plt.close(fig)
    buffer.seek(0)
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def _render_html_report(context: ReportContext, template_text: str) -> str:
    summary_html = _rows_to_table(context.summary_rows)
    metrics_html = _rows_to_table(context.metric_rows)

    template = Template(template_text)
    return template.safe_substitute(
        title=context.title,
        generated_at=context.generated_at,
        summary_table=summary_html,
        metrics_table=metrics_html,
        equity_image=_image_html(context.equity_image, "收益曲线"),
        excess_image=_image_html(context.excess_image, "累计超额收益曲线"),
        drawdown_image=_image_html(context.drawdown_image, "回撤曲线"),
        heatmap_image=_image_html(context.monthly_heatmap_image, "月度收益热力图"),
    )


def _render_pdf_report(context: ReportContext, output_path: Path) -> None:
    with PdfPages(output_path) as pdf:
        fig = plt.figure(figsize=(11.69, 8.27))  # A4 landscape
        fig.patch.set_facecolor("white")
        fig.suptitle(context.title, fontsize=16, fontweight="bold")

        text = [
            f"生成时间: {context.generated_at}",
            "",
            "【策略概览】",
        ]
        for row in context.summary_rows:
            text.append(f"- {row['label']}: {row['value']}")
        text.append("")
        text.append("【核心指标】")
        for row in context.metric_rows:
            text.append(f"- {row['label']}: {row['value']}")

        fig.text(0.02, 0.95, "\n".join(text), fontsize=10, va="top")
        pdf.savefig(fig)
        plt.close(fig)

        for image_data, title in [
            (context.equity_image, "账户净值曲线"),
            (context.excess_image, "累计超额收益曲线"),
            (context.drawdown_image, "最大回撤曲线"),
            (context.monthly_heatmap_image, "月度收益热力图"),
        ]:
            fig = plt.figure(figsize=(11.69, 8.27))
            if image_data:
                ax = fig.add_subplot(111)
                ax.set_title(title, fontsize=14)
                ax.axis("off")
                img = _data_url_to_array(image_data)
                ax.imshow(img)
            else:
                fig.text(0.5, 0.5, "暂无数据", ha="center", va="center")
            pdf.savefig(fig)
            plt.close(fig)


def _rows_to_table(rows: Sequence[Dict[str, str]]) -> str:
    if not rows:
        return "<p>暂无数据</p>"
    html_rows = []
    for row in rows:
        html_rows.append(
            f"<tr><th>{row['label']}</th><td>{row['value']}</td></tr>"
        )
    return "<table class='bt-report-table'>" + "".join(html_rows) + "</table>"


def _image_html(data_url: Optional[str], alt: str) -> str:
    if not data_url:
        return f"<p class='bt-report-placeholder'>{alt}暂无数据</p>"
    return f"<img src='{data_url}' alt='{alt}' loading='lazy' />"


def _data_url_to_array(data_url: str) -> np.ndarray:
    if not data_url.startswith("data:image/png;base64,"):
        raise ReportGenerationError("无法解析图像数据。")
    raw = base64.b64decode(data_url.split(",", 1)[1])
    buffer = io.BytesIO(raw)
    image = plt.imread(buffer, format="png")
    buffer.close()
    return image


def _format_currency(value: Optional[float]) -> str:
    if value is None or not np.isfinite(value):
        return "-"
    return f"{value:,.2f}"


def _format_metric_value(key: str, value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, (int, float)):
        if any(token in key for token in ("收益", "波动", "回撤", "率")) and "比率" not in key:
            return f"{value:.2f}%"
        if "比率" in key or "比" in key:
            return f"{value:.2f}"
        return f"{value:.0f}"
    return str(value)


def _load_template_text(template_path: Optional[str]) -> str:
    if template_path:
        path = Path(template_path).expanduser()
        if not path.exists():
            raise ReportGenerationError(f"模板文件不存在: {path}")
        return path.read_text(encoding="utf-8")
    try:
        return resources.read_text(
            "bullet_trade.reporting.templates", DEFAULT_TEMPLATE_NAME, encoding="utf-8"
        )
    except FileNotFoundError as exc:
        raise ReportGenerationError(f"内置模板缺失: {exc}") from exc


__all__ = ["generate_cli_report", "ReportGenerationError"]
