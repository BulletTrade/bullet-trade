"""
风险分析和可视化

提供回测结果的分析和可视化功能
"""

from typing import Dict, Any, Optional, Sequence, List
import json
from datetime import datetime
from functools import lru_cache
import warnings

try:
    import pandas as pd
    import numpy as np
except ImportError:  # pragma: no cover
    pd = None  # type: ignore
    np = None  # type: ignore

try:
    import matplotlib.pyplot as plt
    from matplotlib.pyplot import MultipleLocator
except ImportError:  # pragma: no cover
    plt = None  # type: ignore

    class MultipleLocator:  # type: ignore
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError("matplotlib 未安装，无法使用分析可视化功能")

# 忽略警告
warnings.filterwarnings('ignore')

_fonts_ready = False

_TRADE_KEY_ALIASES: Dict[str, Sequence[str]] = {
    'time': ('time', '时间', 'datetime', '交易时间'),
    'security': ('security', '标的', 'symbol', 'code'),
    'amount': ('amount', '数量', 'trade_amount', '成交量'),
    'price': ('price', '价格', 'trade_price'),
    'commission': ('commission', '手续费'),
    'tax': ('tax', '印花税'),
    'direction': ('direction', '方向'),
    'turnover': ('turnover', '金额'),
    'cost': ('cost', '总费用'),
}


@lru_cache(maxsize=None)
def _resolve_supported_offset_alias(*candidates: str) -> str:
    if pd is None:
        return candidates[0]
    for alias in candidates:
        try:
            pd.date_range("2000-01-01", periods=1, freq=alias)
            return alias
        except Exception:
            continue
    return candidates[0]


_YEAR_END_FREQ = _resolve_supported_offset_alias("YE", "Y")
_MONTH_END_FREQ = _resolve_supported_offset_alias("ME", "M")


def _merge_meta_dict(base: Optional[Dict[str, Any]], overlay: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base or {})
    for key, value in (overlay or {}).items():
        if key not in merged or merged.get(key) in (None, "", {}, []):
            merged[key] = value
    return merged


def _last_valid_number(series: Optional[pd.Series]) -> Optional[float]:
    if series is None:
        return None
    try:
        clean = pd.to_numeric(series, errors="coerce").dropna()
    except Exception:
        return None
    if clean.empty:
        return None
    return float(clean.iloc[-1])


def _compute_benchmark_context(
    df: pd.DataFrame,
    *,
    base_value: Optional[float] = None,
) -> Dict[str, Any]:
    empty = {
        "benchmark_value": None,
        "benchmark_returns_pct": None,
        "excess_returns_pct": None,
        "benchmark_total_returns_pct": None,
        "benchmark_annual_returns_pct": None,
        "excess_total_returns_pct": None,
    }
    if df is None or df.empty:
        return empty

    def _numeric_series(column: str) -> Optional[pd.Series]:
        if column not in df.columns:
            return None
        series = pd.to_numeric(df[column], errors="coerce")
        if series.dropna().empty:
            return None
        return series

    base = base_value
    if base is None or base <= 0:
        try:
            base = float(pd.to_numeric(df["total_value"], errors="coerce").dropna().iloc[0])
        except Exception:
            base = None

    strategy_returns_pct = _numeric_series("returns_pct")
    if strategy_returns_pct is None and base and base > 0 and "total_value" in df.columns:
        strategy_total_value = pd.to_numeric(df["total_value"], errors="coerce")
        strategy_returns_pct = (strategy_total_value / float(base) - 1.0) * 100.0

    benchmark_returns_pct = _numeric_series("benchmark_returns_pct")
    benchmark_value = _numeric_series("benchmark_value")
    if benchmark_returns_pct is None and benchmark_value is not None and base and base > 0:
        benchmark_returns_pct = (benchmark_value / float(base) - 1.0) * 100.0
    elif benchmark_value is None and benchmark_returns_pct is not None and base and base > 0:
        benchmark_value = float(base) * (1.0 + benchmark_returns_pct / 100.0)

    excess_returns_pct = _numeric_series("excess_returns_pct")
    if excess_returns_pct is None and strategy_returns_pct is not None and benchmark_returns_pct is not None:
        excess_returns_pct = strategy_returns_pct - benchmark_returns_pct

    benchmark_total_returns_pct = _last_valid_number(benchmark_returns_pct)
    excess_total_returns_pct = _last_valid_number(excess_returns_pct)

    benchmark_annual_returns_pct: Optional[float] = None
    if benchmark_value is not None:
        clean = pd.to_numeric(benchmark_value, errors="coerce").dropna()
        trading_days = len(clean)
        years = trading_days / 250.0
        if trading_days > 0 and years > 0 and clean.iloc[0] > 0:
            benchmark_annual_returns_pct = (
                pow(float(clean.iloc[-1]) / float(clean.iloc[0]), 1 / years) - 1
            ) * 100.0

    return {
        "benchmark_value": benchmark_value,
        "benchmark_returns_pct": benchmark_returns_pct,
        "excess_returns_pct": excess_returns_pct,
        "benchmark_total_returns_pct": benchmark_total_returns_pct,
        "benchmark_annual_returns_pct": benchmark_annual_returns_pct,
        "excess_total_returns_pct": excess_total_returns_pct,
    }


def _get_trade_attr(trade: Any, key: str, default: Any = None) -> Any:
    """兼容对象与字典格式的交易记录字段获取。"""
    if isinstance(trade, dict):
        for alias in _TRADE_KEY_ALIASES.get(key, (key,)):
            if alias in trade:
                return trade[alias]
        return default
    return getattr(trade, key, default)


def _ensure_plot_fonts():
    """按需配置中文字体，避免在未生成图片时初始化。"""
    global _fonts_ready
    if _fonts_ready:
        return
    try:
        from ..utils.font_config import setup_chinese_fonts
        setup_chinese_fonts()
    except Exception as exc:
        print(f"中文字体配置失败，继续使用默认字体: {exc}")
    finally:
        _fonts_ready = True


def plot_results(results: Dict[str, Any], save_path: str = None, show_plots: bool = False):
    """
    绘制回测结果图表
    
    Args:
        results: 回测结果字典
        save_path: 保存路径，如果提供则保存图片
        show_plots: 是否在生成后展示图像
    """
    _ensure_plot_fonts()
    df = results['daily_records']
    
    # 创建图表
    fig, axes = plt.subplots(4, 1, figsize=(28, 24), constrained_layout=True)
    fig.suptitle('策略回测结果', fontsize=18, fontweight='bold', y=0.995)
    
    # 1. 资产曲线
    ax1 = axes[0]
    ax1.plot(df.index, df['total_value'], label='策略净值', linewidth=2, color='#1f77b4')
    ax1.axhline(y=float(str(results['summary']['初始资金']).replace(',', '')), 
                color='red', linestyle='--', alpha=0.5, label='初始资金')
    # 1.1 回撤（右轴）合并到净值图
    ax1b = ax1.twinx()
    cummax = df['total_value'].expanding().max()
    drawdown = (df['total_value'] - cummax) / cummax * 100
    ax1b.fill_between(df.index, drawdown, 0, alpha=0.3, color='#d62728', label='回撤 (%)')
    ax1b.plot(df.index, drawdown, linewidth=2, color='darkred')
    ax1.set_ylabel('账户总值 (元)', fontsize=12)
    ax1b.set_ylabel('回撤 (%)', fontsize=12)
    dd_min = float(drawdown.min()) if len(drawdown) > 0 else -1.0
    ax1b.set_ylim(min(dd_min * 1.05, -1.0), 0)
    ax1b.tick_params(axis='y', colors='darkred')
    ax1.set_title('账户净值与回撤', fontsize=14)
    # 合并图例
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax1b.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc='best')
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # 标注Top5最大回撤区间（峰到谷）
    intervals = []
    peak_idx = 0
    peak_val = float(df['total_value'].iloc[0])
    min_dd = 0.0
    min_idx = 0
    for i, v in enumerate(df['total_value'].values):
        v = float(v)
        if v >= peak_val - 1e-9:
            if min_dd < 0:
                intervals.append((peak_idx, min_idx, min_dd))
            peak_val = v
            peak_idx = i
            min_dd = 0.0
            min_idx = i
        else:
            dd_i = (v - peak_val) / peak_val
            if dd_i < min_dd:
                min_dd = dd_i
                min_idx = i
    if min_dd < 0:
        intervals.append((peak_idx, min_idx, min_dd))
    intervals.sort(key=lambda x: x[2])
    intervals = intervals[:5]
    for (p_idx, t_idx, ddv) in intervals:
        start = df.index[p_idx]
        end = df.index[t_idx]
        # 在净值曲线与回撤曲线上高亮区间
        ax1.axvspan(start, end, color='#d62728', alpha=0.3)
        ax1b.axvspan(start, end, color='#d62728', alpha=0.3)
        dd_val_pct = ddv * 100.0
        txt = f"{abs(dd_val_pct):.2f}%\n{start.date()} → {end.date()}"
        y = float(drawdown.iloc[t_idx])
        ax1b.annotate(txt, xy=(end, y), xytext=(end, y + 5), textcoords='data',
                      arrowprops=dict(arrowstyle='->', color='black', alpha=0.6),
                      fontsize=9, ha='left', va='bottom')
    
    # 2. 收益率曲线
    ax2 = axes[1]
    cumulative_returns = (df['total_value'] / df['total_value'].iloc[0] - 1) * 100
    ax2.plot(df.index, cumulative_returns, label='累计收益率', linewidth=2, color='#d62728')
    ax2.axhline(y=0, color='red', linestyle='--', alpha=0.5)
    ax2.set_ylabel('收益率 (%)', fontsize=12)
    ax2.set_title('累计收益率曲线', fontsize=14)
    ax2.legend(loc='best')
    ax2.grid(True, alpha=0.3)
    ax2.tick_params(axis='x', rotation=45)
    
    # 3. 仓位比例（合并到总图）
    ax3 = axes[2]
    position_ratio = df['positions_value'] / df['total_value'] * 100
    ax3.plot(df.index, position_ratio, label='仓位比例', linewidth=2, color='#1f77b4')
    ax3.set_ylabel('仓位比例 (%)', fontsize=12)
    ax3.set_xlabel('日期', fontsize=12)
    ax3.set_title('仓位比例', fontsize=14)
    ax3.legend(loc='best')
    ax3.grid(True, alpha=0.3)
    ax3.tick_params(axis='x', rotation=45)
    ax3.set_ylim(0, 100)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"图表已保存至: {save_path}")

    if show_plots:
        plt.show()
    else:
        plt.close(fig)


def plot_positions(results: Dict[str, Any], save_path: str = None, show_plots: bool = False):
    """
    绘制持仓分布
    
    Args:
        results: 回测结果字典
        save_path: 保存路径
        show_plots: 是否在生成后展示图像
    """
    _ensure_plot_fonts()
    df = results['daily_records']
    
    fig, axes = plt.subplots(2, 1, figsize=(14, 8))
    
    # 1. 现金和持仓市值
    ax1 = axes[0]
    ax1.plot(df.index, df['cash'], label='现金', linewidth=2)
    ax1.plot(df.index, df['positions_value'], label='持仓市值', linewidth=2)
    ax1.set_ylabel('金额 (元)', fontsize=12)
    ax1.set_title('现金与持仓市值变化', fontsize=14)
    ax1.legend(loc='best')
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # 2. 仓位比例
    ax2 = axes[1]
    position_ratio = df['positions_value'] / df['total_value'] * 100
    ax2.plot(df.index, position_ratio, label='仓位比例', linewidth=2, color='orange')
    ax2.set_ylabel('仓位比例 (%)', fontsize=12)
    ax2.set_xlabel('日期', fontsize=12)
    ax2.set_title('仓位比例变化', fontsize=14)
    ax2.legend(loc='best')
    ax2.grid(True, alpha=0.3)
    ax2.tick_params(axis='x', rotation=45)
    ax2.set_ylim(0, 100)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"图表已保存至: {save_path}")

    if show_plots:
        plt.show()
    else:
        plt.close(fig)


def _compute_trade_win_stats(trades: List[Dict[str, Any]]) -> Dict[str, float]:
    """按成交（卖出）口径统计交易胜率与次数。"""
    # 统一访问器
    def _ga(obj, key, default=None):
        try:
            return getattr(obj, key)
        except Exception:
            return obj.get(key, default) if isinstance(obj, dict) else default

    def _normalize_trade_time(value):
        """将各种时间格式统一为可比较的字符串，避免排序报错。"""
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, datetime):
            return value.isoformat(sep=' ')
        try:
            ts = pd.to_datetime(value, errors='coerce')
        except Exception:
            return str(value)
        if pd.isna(ts):
            return str(value)
        return ts.isoformat()

    # 排序确保时间顺序
    enumerated_trades = list(enumerate(trades or []))

    def _sort_key(item):
        idx, trade = item
        normalized = _normalize_trade_time(_ga(trade, 'time'))
        return (normalized is None, normalized or '', idx)

    enumerated_trades.sort(key=_sort_key)
    sorted_trades = [trade for _, trade in enumerated_trades]
    # 按标的维护仓位均价
    state: Dict[str, Dict[str, float]] = {}
    def st(code: str):
        if code not in state:
            state[code] = {'qty': 0.0, 'avg': 0.0}
        return state[code]

    win = 0
    loss = 0
    for t in sorted_trades:
        code = _ga(t, 'security')
        if not code:
            continue
        s = st(code)
        amt = float(_ga(t, 'amount', 0) or 0)
        price = float(_ga(t, 'price', 0) or 0)
        commission = float(_ga(t, 'commission', 0) or 0)
        tax = float(_ga(t, 'tax', 0) or 0)
        if amt > 0:
            total = s['avg'] * s['qty'] + price * amt
            s['qty'] = s['qty'] + amt
            s['avg'] = (total / s['qty']) if s['qty'] > 0 else 0.0
        elif amt < 0:
            sell_qty = abs(amt)
            pnl = (price - s['avg']) * sell_qty - commission - tax
            if pnl > 1e-12:
                win += 1
            elif pnl < -1e-12:
                loss += 1
            s['qty'] = max(0.0, s['qty'] - sell_qty)

    total = win + loss
    win_rate = (win / total * 100.0) if total > 0 else 0.0
    return {
        '交易胜率': win_rate,
        '交易盈利次数': win,
        '交易亏损次数': loss,
    }


def _compute_trade_profit_loss_ratio(trades: List[Dict[str, Any]]) -> float:
    """
    计算基于交易的盈亏比（聚宽公式：总盈利额 / 总亏损额）。
    
    按卖出成交计算每笔交易的盈亏，汇总后计算比率。
    """
    # 统一访问器
    def _ga(obj, key, default=None):
        try:
            return getattr(obj, key)
        except Exception:
            return obj.get(key, default) if isinstance(obj, dict) else default

    def _normalize_trade_time(value):
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, datetime):
            return value.isoformat(sep=' ')
        try:
            ts = pd.to_datetime(value, errors='coerce')
        except Exception:
            return str(value)
        if pd.isna(ts):
            return str(value)
        return ts.isoformat()

    # 排序确保时间顺序
    enumerated_trades = list(enumerate(trades or []))

    def _sort_key(item):
        idx, trade = item
        normalized = _normalize_trade_time(_ga(trade, 'time'))
        return (normalized is None, normalized or '', idx)

    enumerated_trades.sort(key=_sort_key)
    sorted_trades = [trade for _, trade in enumerated_trades]
    
    # 按标的维护仓位均价
    state: Dict[str, Dict[str, float]] = {}
    def st(code: str):
        if code not in state:
            state[code] = {'qty': 0.0, 'avg': 0.0}
        return state[code]

    total_profit = 0.0  # 总盈利额
    total_loss = 0.0    # 总亏损额（绝对值）
    
    for t in sorted_trades:
        code = _ga(t, 'security')
        if not code:
            continue
        s = st(code)
        amt = float(_ga(t, 'amount', 0) or 0)
        price = float(_ga(t, 'price', 0) or 0)
        commission = float(_ga(t, 'commission', 0) or 0)
        tax = float(_ga(t, 'tax', 0) or 0)
        
        if amt > 0:
            # 买入：更新加权平均成本
            total_cost = s['avg'] * s['qty'] + price * amt
            s['qty'] = s['qty'] + amt
            s['avg'] = (total_cost / s['qty']) if s['qty'] > 0 else 0.0
        elif amt < 0:
            # 卖出：计算盈亏
            sell_qty = abs(amt)
            # 盈亏 = (卖出价 - 成本价) * 数量 - 手续费 - 印花税
            pnl = (price - s['avg']) * sell_qty - commission - tax
            if pnl > 0:
                total_profit += pnl
            else:
                total_loss += abs(pnl)
            s['qty'] = max(0.0, s['qty'] - sell_qty)
    
    # 盈亏比 = 总盈利额 / 总亏损额
    if total_loss > 1e-12:
        return total_profit / total_loss
    elif total_profit > 1e-12:
        return float('inf')  # 只盈利无亏损
    else:
        return 0.0


def calculate_metrics(results: Dict[str, Any]) -> Dict[str, float]:
    """
    计算详细的风险指标（与聚宽兼容）
    
    公式参考聚宽官方文档：
    - 无风险利率：默认 4%（与聚宽一致）
    - 夏普比率 = (年化收益 - 无风险利率) / 年化波动率
    - 索提诺比率 = (年化收益 - 无风险利率) / 下行波动率
    - 下行波动率：使用聚宽动态目标公式
    - 盈亏比 = 总盈利额 / 总亏损额（基于交易）
    
    Args:
        results: 回测结果字典
        
    Returns:
        指标字典（数值已四舍五入：百分比2位，比率4位）
    """
    # 无风险利率（与聚宽一致，默认 4%）
    RISK_FREE_RATE = 0.04
    
    df = results['daily_records']
    
    # 检查是否有数据
    if df.empty or len(df) == 0:
        # 返回空数据的默认指标
        return {
            '策略收益': 0.0,
            '策略年化收益': 0.0,
            '基准收益': 0.0,
            '基准年化收益': 0.0,
            '累计超额收益': 0.0,
            'Alpha': 0.0,
            'Beta': 0.0,
            '夏普比率': 0.0,
            '索提诺比率': 0.0,
            '信息比率': 0.0,
            '最大回撤': 0.0,
            '最大回撤区间': '无',
            '波动率': 0.0,
            '日胜率': 0.0,
            '交易胜率': 0.0,
            '盈亏比': 0.0,
            '交易天数': 0,
            '交易次数': 0,
        }
    
    # 基本统计（与引擎摘要口径统一：以初始总资产为基准）
    base = None
    try:
        base = float(results.get('meta', {}).get('initial_total_value'))
    except Exception:
        base = None
    if not base or base <= 0:
        base = float(df['total_value'].iloc[0])

    benchmark_ctx = _compute_benchmark_context(df, base_value=base)
    
    # 策略收益（百分比）
    total_returns = (df['total_value'].iloc[-1] / base - 1) * 100
    trading_days = len(df)
    years = trading_days / 250.0
    
    # 年化收益（聚宽公式：((1+P)^(250/n) - 1) * 100%）
    if years > 0:
        annual_returns = (pow(df['total_value'].iloc[-1] / base, 1/years) - 1) * 100
    else:
        annual_returns = 0.0
    
    # 日收益率序列（小数形式）
    daily_returns = df['daily_returns'].dropna()
    n = len(daily_returns)
    
    # 策略波动率（聚宽公式：sqrt(250/(n-1) * Σ(rp - rp_avg)^2)）
    # pandas std() 默认 ddof=1，等价于聚宽公式
    if n > 1:
        volatility = daily_returns.std() * np.sqrt(250) * 100  # 转为百分比
    else:
        volatility = 0.0
    
    # 最大回撤（聚宽公式：Max((Px - Py) / Px)，y > x）
    cummax = df['total_value'].expanding().max()
    drawdown = (df['total_value'] - cummax) / cummax * 100  # 百分比
    max_drawdown = float(drawdown.min())
    
    # 计算最大回撤区间（峰到谷）
    max_dd_interval = '未知'
    try:
        if len(drawdown) > 0 and not pd.isna(max_drawdown):
            dd_trough = drawdown.idxmin()
            # 峰值取回撤谷值之前（含）区间的最高净值时间
            peak_series = df['total_value'].loc[:dd_trough]
            if len(peak_series) > 0:
                dd_peak = peak_series.idxmax()
                # 格式化日期字符串
                peak_date = pd.to_datetime(dd_peak).date()
                trough_date = pd.to_datetime(dd_trough).date()
                max_dd_interval = f"{peak_date} 至 {trough_date}"
    except Exception:
        max_dd_interval = '计算失败'
    
    # 最大回撤持续时间
    dd_duration = 0
    current_dd = 0
    for dd in drawdown:
        if dd < 0:
            current_dd += 1
            dd_duration = max(dd_duration, current_dd)
        else:
            current_dd = 0
    
    # ========== 夏普比率（聚宽公式）==========
    # Sharpe = (Rp - Rf) / σp
    # Rp = 策略年化收益率（小数），Rf = 无风险利率（0.04），σp = 年化波动率（小数）
    if volatility > 0:
        sharpe_ratio = (annual_returns / 100.0 - RISK_FREE_RATE) / (volatility / 100.0)
    else:
        sharpe_ratio = 0.0
    
    # ========== 下行波动率（聚宽公式）==========
    # σpd = sqrt(250/n * Σ(rp - rpi_avg)^2 * f(t))
    # 其中 rpi_avg = 截至第i日的平均收益率，f(t) = 1 if rp < rpi_avg else 0
    if n > 0:
        downside_sq_sum = 0.0
        cumsum = 0.0
        for i, rp in enumerate(daily_returns.values):
            cumsum += rp
            rpi_avg = cumsum / (i + 1)  # 截至第i日的平均收益率
            if rp < rpi_avg:
                downside_sq_sum += (rp - rpi_avg) ** 2
        downside_volatility = np.sqrt(250.0 / n * downside_sq_sum) * 100  # 转为百分比
    else:
        downside_volatility = 0.0
    
    # ========== 索提诺比率（聚宽公式）==========
    # Sortino = (Rp - Rf) / σpd
    if downside_volatility > 0:
        sortino_ratio = (annual_returns / 100.0 - RISK_FREE_RATE) / (downside_volatility / 100.0)
    else:
        sortino_ratio = 0.0
    
    # 日胜率（基于 daily_returns > 0）
    winning_days = int((daily_returns > 0).sum())
    losing_days = int((daily_returns < 0).sum())
    win_rate_daily = winning_days / n * 100 if n > 0 else 0.0
    
    # ========== 盈亏比（聚宽公式：总盈利额 / 总亏损额）==========
    # 基于交易记录计算，而非日收益
    trades = results.get('trades', [])
    total_profit = 0.0
    total_loss = 0.0
    for t in trades:
        # 兼容对象与字典格式
        if isinstance(t, dict):
            amt = float(t.get('amount') or t.get('数量') or 0)
            price = float(t.get('price') or t.get('价格') or 0)
            commission = float(t.get('commission') or t.get('手续费') or 0)
            tax = float(t.get('tax') or t.get('印花税') or 0)
        else:
            amt = float(getattr(t, 'amount', 0) or 0)
            price = float(getattr(t, 'price', 0) or 0)
            commission = float(getattr(t, 'commission', 0) or 0)
            tax = float(getattr(t, 'tax', 0) or 0)
        
        if amt < 0:  # 卖出交易
            # 简化计算：卖出金额 - 费用（实际盈亏需要成本，这里用交易金额近似）
            trade_value = abs(amt) * price - commission - tax
            # 注：精确盈亏需要成本价，这里用 _compute_trade_win_stats 的逻辑
    
    # 使用 _compute_trade_win_stats 计算精确的盈亏比
    trade_stats = _compute_trade_win_stats(trades)
    
    # 计算基于交易的盈亏比（总盈利额 / 总亏损额）
    profit_loss_ratio = _compute_trade_profit_loss_ratio(trades)
    
    # Calmar比率 = 年化收益 / |最大回撤|
    if max_drawdown < 0:
        calmar_ratio = annual_returns / abs(max_drawdown)
    else:
        calmar_ratio = 0.0

    # ========== 四舍五入：百分比保留2位，比率保留4位 ==========
    metrics = {
        '策略收益': round(total_returns, 2),
        '策略年化收益': round(annual_returns, 2),
        '基准收益': round(float(benchmark_ctx.get('benchmark_total_returns_pct') or 0.0), 2),
        '基准年化收益': round(float(benchmark_ctx.get('benchmark_annual_returns_pct') or 0.0), 2),
        '累计超额收益': round(float(benchmark_ctx.get('excess_total_returns_pct') or 0.0), 2),
        '策略波动率': round(volatility, 2),
        '最大回撤': round(max_drawdown, 2),
        '最大回撤区间': max_dd_interval,
        '最大回撤持续天数': dd_duration,
        '夏普比率': round(sharpe_ratio, 4),
        '索提诺比率': round(sortino_ratio, 4),
        'Calmar比率': round(calmar_ratio, 4),
        '下行波动率': round(downside_volatility, 2),
        '日胜率': round(win_rate_daily, 2),
        '日盈利天数': winning_days,
        '日亏损天数': losing_days,
        '交易胜率': round(float(trade_stats['交易胜率']), 2),
        '交易盈利次数': int(trade_stats['交易盈利次数']),
        '交易亏损次数': int(trade_stats['交易亏损次数']),
        '盈亏比': round(profit_loss_ratio, 4),
        '交易天数': trading_days,
    }
    # 兼容旧口径：保留"胜率"字段
    metrics['胜率'] = round(float(trade_stats['交易胜率']), 2)
    # 添加收益回撤比（等同Calmar比率，方便排序）
    metrics['收益回撤比'] = round(calmar_ratio, 4)
    
    return metrics


def print_metrics(metrics: Dict[str, float]):
    """
    打印风险指标
    
    Args:
        metrics: 指标字典
    """
    print("\n" + "=" * 70)
    print("详细风险指标")
    print("=" * 70)
    
    for key, value in metrics.items():
        if '收益' in key or '波动' in key or '回撤' in key or '率' in key:
            if isinstance(value, (int, float)):
                print(f"{key:20s}: {value:>12.2f}%")
            else:
                print(f"{key:20s}: {value:>12}")
        elif '比率' in key or '比' in key:
            if isinstance(value, (int, float)):
                print(f"{key:20s}: {value:>12.2f}")
            else:
                print(f"{key:20s}: {value:>12}")
        else:
            if isinstance(value, (int, float)):
                print(f"{key:20s}: {value:>12.0f}")
            else:
                print(f"{key:20s}: {value:>12}")
    
    print("=" * 70)


def export_metrics(metrics: Dict[str, float], file_path: str):
    """
    导出风险指标到 CSV
    
    Args:
        metrics: 指标字典
        file_path: 保存文件路径
    """
    rows = []
    for key, value in metrics.items():
        # 与打印格式保持一致的格式化值
        if ('收益' in key or '波动' in key or '回撤' in key or '率' in key) and ('比率' not in key):
            formatted = f"{value:.2f}%" if isinstance(value, (int, float)) else str(value)
        elif ('比率' in key or '比' in key):
            formatted = f"{value:.2f}" if isinstance(value, (int, float)) else str(value)
        else:
            if isinstance(value, float):
                formatted = f"{value:.0f}"
            elif isinstance(value, int):
                formatted = str(value)
            else:
                formatted = str(value)
        rows.append({'指标': key, '值': value, '格式化值': formatted})
    dfm = pd.DataFrame(rows)
    dfm.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"风险指标已导出至: {file_path}")


def export_metrics_json(metrics: Dict[str, Any], file_path: str, meta: Optional[Dict[str, Any]] = None):
    """
    导出风险指标到 JSON，供 CLI 报告生成使用。

    Args:
        metrics: 指标字典
        file_path: 保存文件路径
        meta: 可选的元信息（如开始/结束日期）
    """
    payload = {
        "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "metrics": {},
    }
    if meta:
        payload["meta"] = meta

    for key, value in metrics.items():
        if isinstance(value, (np.generic,)):
            value = value.item()
        elif isinstance(value, (pd.Timestamp, pd.Timedelta)):
            value = value.isoformat()
        payload["metrics"][key] = value

    with open(file_path, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    print(f"风险指标JSON已导出至: {file_path}")


def export_trades(results: Dict[str, Any], file_path: str):
    """
    导出交易记录
    
    Args:
        results: 回测结果字典
        file_path: 保存文件路径
    """
    trades = results['trades']
    if not trades:
        print("无交易记录")
        return
    
    # 转换为DataFrame
    trade_records = []
    for trade in trades:
        amount_val = _get_trade_attr(trade, 'amount', 0) or 0
        price_val = _get_trade_attr(trade, 'price', 0) or 0
        turnover_val = _get_trade_attr(trade, 'turnover')
        if turnover_val is None and isinstance(trade, dict):
            turnover_val = trade.get('金额')
        if turnover_val is None:
            turnover_val = price_val * abs(amount_val)
        commission_val = _get_trade_attr(trade, 'commission', 0) or 0
        tax_val = _get_trade_attr(trade, 'tax', 0) or 0
        cost_val = _get_trade_attr(trade, 'cost')
        if cost_val is None:
            cost_val = commission_val + tax_val
        # 金额字段规范到“分”，四舍五入
        def _fen(x):
            from decimal import Decimal, ROUND_HALF_UP
            return float(Decimal(str(x)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        commission_val = _fen(commission_val)
        tax_val = _fen(tax_val)
        cost_val = _fen(cost_val)
        direction_val = _get_trade_attr(trade, 'direction')
        if direction_val is None:
            if amount_val > 0:
                direction_val = '买入'
            elif amount_val < 0:
                direction_val = '卖出'
            else:
                direction_val = '持仓'
        trade_records.append({
            '时间': _get_trade_attr(trade, 'time'),
            '标的': _get_trade_attr(trade, 'security'),
            '数量': amount_val,
            '价格': price_val,
            '金额': turnover_val,
            '手续费': commission_val,
            '印花税': tax_val,
            '总费用': cost_val,
            '方向': direction_val,
        })
    
    df = pd.DataFrame(trade_records)
    df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"交易记录已导出至: {file_path}")

# 新增：交互式HTML报告（Plotly）
def load_results_from_directory(results_dir: str) -> Dict[str, Any]:
    """从指定目录读取CSV并重建用于报告的 results 字典。
    必需文件：daily_records.csv；可选：trades.csv、dividend_split_events.csv、daily_positions.csv。
    当缺少 daily_returns 时自动根据 total_value 计算。
    """
    import os
    import pandas as pd

    # 读取每日记录
    dr_path = os.path.join(results_dir, 'daily_records.csv')
    if not os.path.exists(dr_path):
        raise FileNotFoundError(f"未找到每日记录: {dr_path}")
    # 尝试两种读取方式以兼容不同导出
    try:
        df = pd.read_csv(dr_path, index_col=0, parse_dates=[0], encoding='utf-8-sig')
    except Exception:
        df = pd.read_csv(dr_path, encoding='utf-8-sig')
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        else:
            # 回退策略：将首列视为日期索引
            df.index = pd.to_datetime(df.iloc[:, 0])
            df.drop(df.columns[0], axis=1, inplace=True)
    if 'total_value' not in df.columns:
        raise ValueError('daily_records.csv 缺少 total_value 列，无法生成报告')
    if 'daily_returns' not in df.columns:
        df['daily_returns'] = df['total_value'].pct_change()
    if not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index)
        except Exception as e:
            raise ValueError(f'无法解析日期索引: {e}')

    results: Dict[str, Any] = {'daily_records': df}

    # 读取交易记录（可选）
    trades_path = os.path.join(results_dir, 'trades.csv')
    if os.path.exists(trades_path):
        try:
            tdf = pd.read_csv(trades_path, encoding='utf-8-sig')
            results['trades'] = tdf.to_dict(orient='records')
        except Exception:
            results['trades'] = []
    else:
        results['trades'] = []

    # 读取分红/拆分事件（可选）
    events_path = os.path.join(results_dir, 'dividend_split_events.csv')
    if os.path.exists(events_path):
        try:
            evdf = pd.read_csv(events_path, encoding='utf-8-sig')
            results['events'] = evdf.to_dict(orient='records')
        except Exception:
            results['events'] = []
    else:
        results['events'] = []

    # 新增：读取每日持仓快照（可选）
    daily_pos_path = os.path.join(results_dir, 'daily_positions.csv')
    if os.path.exists(daily_pos_path):
        try:
            dp = pd.read_csv(daily_pos_path, encoding='utf-8-sig')
            # 规范日期列为datetime，保留原始列
            if 'date' in dp.columns:
                try:
                    dp['date'] = pd.to_datetime(dp['date'])
                except Exception:
                    pass
            results['daily_positions'] = dp
        except Exception:
            results['daily_positions'] = None
    else:
        results['daily_positions'] = None

    # 构造元信息（尽量从数据推断）
    base_name = os.path.basename(os.path.normpath(results_dir))
    start_date = df.index.min().strftime('%Y-%m-%d') if len(df) > 0 else ''
    end_date = df.index.max().strftime('%Y-%m-%d') if len(df) > 0 else ''
    results['meta'] = {
        'strategy_file': base_name,
        'start_date': start_date,
        'end_date': end_date,
        'algorithm_id': None,
        'extras': {},
        'runtime_seconds': None,
    }

    metrics_path = os.path.join(results_dir, 'metrics.json')
    if os.path.exists(metrics_path):
        try:
            with open(metrics_path, 'r', encoding='utf-8') as fp:
                payload = json.load(fp)
            if isinstance(payload, dict):
                results['meta'] = _merge_meta_dict(results['meta'], payload.get('meta', {}))
        except Exception:
            pass

    return results

def generate_report(
    results: Dict[str, Any] = None,
    output_dir: Optional[str] = None,
    gen_images: bool = False,
    gen_csv: bool = True,
    gen_html: bool = True,
    results_dir: Optional[str] = None,
    show_plots: bool = False,
):
    """
    生成完整的回测报告（图片/CSV/HTML 一键）
    
    支持两种调用：
    - 传入内存 results（原行为不变）
    - 仅指定 results_dir（从目录CSV重建）并默认输出至该目录
    
    Args:
        results: 回测结果字典（可选）
        output_dir: 输出目录（默认使用 results_dir）
        gen_images: 是否生成PNG图片（默认False，可按需开启）
        gen_csv: 是否生成CSV文件（默认True）
        gen_html: 是否生成交互式HTML报告（默认True）
        results_dir: 回测结果目录（可选）
        show_plots: 是否在生成报告时展示图像
    """
    import os
    os.makedirs(output_dir or results_dir or '.', exist_ok=True)

    # 兼容目录结构
    if results is None:
        if results_dir is None:
            raise ValueError('必须提供 results 或 results_dir')
        results = load_results_from_directory(results_dir)
        output_dir = output_dir or results_dir
    else:
        # 如提供目录，补全缺失字段并默认输出至该目录
        if results_dir is not None:
            try:
                loaded = load_results_from_directory(results_dir)
                if results.get('daily_records') is None:
                    results['daily_records'] = loaded.get('daily_records')
                if not results.get('trades'):
                    results['trades'] = loaded.get('trades', [])
                if not results.get('events'):
                    results['events'] = loaded.get('events', [])
                if results.get('daily_positions') is None:
                    results['daily_positions'] = loaded.get('daily_positions', None)
                results['meta'] = _merge_meta_dict(results.get('meta', {}), loaded.get('meta', {}))
            except Exception:
                pass
            output_dir = output_dir or results_dir
        else:
            output_dir = output_dir or '.'

    # 计算指标
    metrics = calculate_metrics(results)
    results['metrics'] = metrics
    
    # 打印指标到控制台
    print_metrics(metrics)
    
    # 风险指标CSV（受开关控制）
    if gen_csv:
        export_metrics(metrics, os.path.join(output_dir, 'risk_metrics.csv'))
    try:
        export_metrics_json(metrics, os.path.join(output_dir, 'metrics.json'), meta=results.get('meta'))
    except Exception as exc:
        print(f"Warning: 指标JSON导出失败: {exc}")
    
    # 图片（受开关控制）
    if gen_images:
        _ensure_plot_fonts()
        plot_results(
            results,
            save_path=os.path.join(output_dir, 'backtest_results.png'),
            show_plots=show_plots,
        )
        plot_positions(
            results,
            save_path=os.path.join(output_dir, 'positions.png'),
            show_plots=show_plots,
        )
    
    # 交易记录CSV（受开关控制）
    if gen_csv:
        export_trades(results, os.path.join(output_dir, 'trades.csv'))
    
    # 分红/拆分事件CSV（受开关控制）
    events = results.get('events', [])
    if gen_csv and events:
        ev_df = pd.DataFrame(events)
        cols = ['event_type','strategy_time','code','event_date','per_base','bonus_pre_tax','net_bonus','tax_rate_percent','cash_in','scale_factor','old_amount','new_amount','cash_before','cash_after','positions_value_before','positions_value_after','total_value_before','total_value_after']
        # 仅在包含这些列时按顺序导出
        ev_df = ev_df[[c for c in cols if c in ev_df.columns]]
        ev_df.to_csv(os.path.join(output_dir, 'dividend_split_events.csv'), index=False, encoding='utf-8-sig')
        print(f"分红/拆分事件已导出至: {os.path.join(output_dir, 'dividend_split_events.csv')}")
    elif not events:
        print("无分红/拆分事件")
    
    # 每日数据CSV（受开关控制）
    df = results['daily_records']
    if gen_csv:
        df.to_csv(os.path.join(output_dir, 'daily_records.csv'), encoding='utf-8-sig')
        print(f"每日数据已导出至: {os.path.join(output_dir, 'daily_records.csv')}")
    
    # 每日持仓快照CSV（受开关控制）
    daily_pos = results.get('daily_positions')
    if gen_csv and daily_pos is not None and not getattr(daily_pos, 'empty', False):
        daily_pos = daily_pos.copy()
        # 规范日期列，计算市值与浮动盈亏
        try:
            daily_pos['date'] = pd.to_datetime(daily_pos['date'])
        except Exception:
            pass
        # 市值（若无则用 price*amount）
        if 'value' not in daily_pos.columns:
            daily_pos['value'] = daily_pos.get('price', 0.0).astype(float) * daily_pos.get('amount', 0).astype(float)
        # 浮动盈亏与盈亏率（以 avg_cost 为基准）
        if 'avg_cost' in daily_pos.columns:
            daily_pos['floating_pnl'] = (daily_pos.get('price', 0.0).astype(float) - daily_pos['avg_cost'].astype(float)) * daily_pos.get('amount', 0).astype(float)
            daily_pos['floating_pnl_pct'] = np.where(daily_pos['avg_cost'].astype(float) > 0,
                                                     (daily_pos.get('price', 0.0).astype(float) / daily_pos['avg_cost'].astype(float) - 1) * 100,
                                                     np.nan)
        else:
            daily_pos['floating_pnl'] = np.nan
            daily_pos['floating_pnl_pct'] = np.nan
        # 合并每日现金/持仓/总资产
        dr = results.get('daily_records')
        if dr is not None and len(dr) > 0:
            idx_dates = pd.to_datetime(dr.index).date
            cash_map = {d: float(c) for d, c in zip(idx_dates, dr['cash'])} if 'cash' in dr.columns else {}
            pos_map = {d: float(v) for d, v in zip(idx_dates, dr['positions_value'])} if 'positions_value' in dr.columns else {}
            total_map = {d: float(v) for d, v in zip(idx_dates, dr['total_value'])} if 'total_value' in dr.columns else {}
            daily_pos['date_key'] = daily_pos['date'].dt.date if 'date' in daily_pos.columns else pd.NaT
            daily_pos['cash'] = daily_pos['date_key'].map(cash_map).fillna(0.0)
            daily_pos['positions_value'] = daily_pos['date_key'].map(pos_map).fillna(np.nan)
            daily_pos['total_value'] = daily_pos['date_key'].map(total_map).fillna(np.nan)
            # 每日汇总：浮动盈亏与总市值（含现金）
            group_sum = daily_pos.groupby('date_key')['value'].sum().rename('daily_positions_value')
            group_pnl = daily_pos.groupby('date_key')['floating_pnl'].sum().rename('daily_floating_pnl')
            daily_pos = daily_pos.merge(group_sum, left_on='date_key', right_index=True, how='left')
            daily_pos = daily_pos.merge(group_pnl, left_on='date_key', right_index=True, how='left')
            daily_pos['daily_total_market_value'] = daily_pos['daily_positions_value'].fillna(0.0) + daily_pos['cash'].fillna(0.0)
        # 导出并确保列顺序
        cols = ['date','code','amount','closeable_amount','avg_cost','acc_avg_cost','price','value','floating_pnl','floating_pnl_pct','cash','positions_value','total_value','daily_positions_value','daily_total_market_value','daily_floating_pnl']
        daily_pos = daily_pos[[c for c in cols if c in daily_pos.columns]]
        daily_pos.to_csv(os.path.join(output_dir, 'daily_positions.csv'), index=False, encoding='utf-8-sig')
        print(f"每日持仓快照已导出至: {os.path.join(output_dir, 'daily_positions.csv')}")
    elif daily_pos is None or getattr(daily_pos, 'empty', True):
        print("无每日持仓快照或为空")

    # 数据分析：年收益、月收益热力图、开仓次数、分标的盈亏（受开关控制）
    export_annual_returns(
        results,
        img_path=os.path.join(output_dir, 'annual_returns.png') if gen_images else None,
        csv_path=os.path.join(output_dir, 'annual_returns.csv') if gen_csv else None,
    )
    export_monthly_returns_heatmap(
        results,
        img_path=os.path.join(output_dir, 'monthly_returns_heatmap.png') if gen_images else None,
        csv_path=os.path.join(output_dir, 'monthly_returns.csv') if gen_csv else None,
    )
    export_open_counts(
        results,
        img_path=os.path.join(output_dir, 'open_counts.png') if gen_images else None,
        csv_path=os.path.join(output_dir, 'open_counts.csv') if gen_csv else None,
    )
    export_instrument_pnl(
        results,
        img_path=os.path.join(output_dir, 'instrument_pnl.png') if gen_images else None,
        csv_path=os.path.join(output_dir, 'instrument_pnl.csv') if gen_csv else None,
    )

    # 日收益日历图PNG输出（pyecharts）
    if gen_images:
        try:
            export_daily_return_calendar_png(
                results,
                img_path=os.path.join(output_dir, 'calendar.png'),
                color_method="log",
                color_k=0.2,
                show_visualmap=False,
                height_px=110,
            )
        except Exception as e:
            print(f"日历图PNG导出失败: {e}")
    
    # 交互式HTML报告（受开关控制，默认True）
    if gen_html:
        df = results.get('daily_records')
        if df is not None and len(df) > 0:
            generate_html_report(results, output_file=os.path.join(output_dir, 'report.html'))
        else:
            print("⚠️  无交易日数据，跳过HTML报告生成")
    
    print(f"\n回测报告已生成至: {output_dir}")


def export_annual_returns(results: Dict[str, Any], img_path: str = None, csv_path: str = None):
    """导出年收益图和/或CSV（根据传入路径决定输出内容）"""
    if img_path:
        _ensure_plot_fonts()
    df = results['daily_records']
    dr = df['daily_returns'].dropna()
    if dr.empty:
        print("无法计算年收益：无每日收益数据")
        return
    annual = (dr + 1).groupby(dr.index.year).apply(lambda x: x.prod() - 1) * 100
    annual_df = annual.rename('年收益(%)').to_frame()
    annual_df.index.name = '年份'
    if csv_path:
        annual_df.to_csv(csv_path, encoding='utf-8-sig')
        print(f"年收益CSV已导出至: {csv_path}")
    if img_path:
        # 绘制柱状图
        plt.figure(figsize=(12, 4))
        years = annual_df.index.astype(int)
        values = annual_df['年收益(%)'].values
        colors = ['#d62728' if v >= 0 else '#2ca02c' for v in values]
        plt.bar(years, values, color=colors)
        plt.title('年收益', fontsize=14)
        plt.xlabel('年份')
        plt.ylabel('收益 (%)')
        for x, v in zip(years, values):
            plt.text(x, v, f"{v:.1f}%", ha='center', va='bottom' if v>=0 else 'top', fontsize=9)
        plt.tight_layout()
        plt.savefig(img_path, dpi=300, bbox_inches='tight')
        print(f"年收益图已保存至: {img_path}")
        plt.close()
    if csv_path:
        annual_df.to_csv(csv_path, encoding='utf-8-sig')
        print(f"年收益CSV已导出至: {csv_path}")
    if img_path:
        # 绘制柱状图
        plt.figure(figsize=(12, 4))
        years = annual_df.index.astype(int)
        values = annual_df['年收益(%)'].values
        colors = ['#d62728' if v >= 0 else '#2ca02c' for v in values]
        plt.bar(years, values, color=colors)
        plt.title('年收益', fontsize=14)
        plt.xlabel('年份')
        plt.ylabel('收益 (%)')
        for x, v in zip(years, values):
            plt.text(x, v, f"{v:.1f}%", ha='center', va='bottom' if v>=0 else 'top', fontsize=9)
        plt.tight_layout()
        plt.savefig(img_path, dpi=300, bbox_inches='tight')
        print(f"年收益图已保存至: {img_path}")
        plt.close()


def export_monthly_returns_heatmap(results: Dict[str, Any], img_path: str = None, csv_path: str = None):
    """导出月收益热力图和/或CSV（根据传入路径决定输出内容）"""
def export_monthly_returns_heatmap(results: Dict[str, Any], img_path: str = None, csv_path: str = None):
    """导出月收益热力图和/或CSV（根据传入路径决定输出内容）"""
    if img_path:
        _ensure_plot_fonts()
    df = results['daily_records']
    dr = df['daily_returns'].dropna()
    if dr.empty:
        print("无法计算月收益：无每日收益数据")
        return
    monthly = (dr + 1).groupby([dr.index.year, dr.index.month]).apply(lambda x: x.prod() - 1) * 100
    heat_df = monthly.unstack(level=1)
    # 确保列为1-12
    months = list(range(1, 13))
    heat_df = heat_df.reindex(columns=months)
    heat_df.index.name = '年份'
    heat_df.columns = [f'{m}月' for m in months]
    if csv_path:
        heat_df.to_csv(csv_path, encoding='utf-8-sig')
        print(f"月收益CSV已导出至: {csv_path}")
    if img_path:
        # 绘制热力图（使用matplotlib）
        plt.figure(figsize=(14, 6))
        data = heat_df.values.astype(float)
        # 配置对称色轴，正绿负红
        if np.isnan(data).all():
            vlim = 1.0
        else:
            vlim = max(abs(np.nanmin(data)), abs(np.nanmax(data)))
            if not np.isfinite(vlim) or vlim == 0:
                vlim = 1.0
        im = plt.imshow(data, aspect='auto', cmap='RdYlGn_r', vmin=-vlim, vmax=vlim)
        plt.title('月收益（热力图）', fontsize=14)
        plt.xlabel('月份')
        plt.ylabel('年份')
        plt.xticks(ticks=np.arange(len(months)), labels=[f'{m}月' for m in months])
        plt.yticks(ticks=np.arange(len(heat_df.index)), labels=heat_df.index.astype(int))
        cbar = plt.colorbar(im)
        cbar.set_label('收益 (%)')
        # 单元格标注
        for i in range(data.shape[0]):
            for j in range(data.shape[1]):
                val = data[i, j]
                if np.isnan(val):
                    continue
                text_color = 'black' if abs(val) < vlim * 0.6 else 'white'
                plt.text(j, i, f"{val:.1f}", ha='center', va='center', color=text_color, fontsize=8)
        plt.tight_layout()
        plt.savefig(img_path, dpi=300, bbox_inches='tight')
        print(f"月收益热力图已保存至: {img_path}")
        plt.close()
    if csv_path:
        heat_df.to_csv(csv_path, encoding='utf-8-sig')
        print(f"月收益CSV已导出至: {csv_path}")
    if img_path:
        # 绘制热力图（使用matplotlib）
        plt.figure(figsize=(14, 6))
        data = heat_df.values.astype(float)
        # 配置对称色轴，正绿负红
        if np.isnan(data).all():
            vlim = 1.0
        else:
            vlim = max(abs(np.nanmin(data)), abs(np.nanmax(data)))
            if not np.isfinite(vlim) or vlim == 0:
                vlim = 1.0
        im = plt.imshow(data, aspect='auto', cmap='RdYlGn_r', vmin=-vlim, vmax=vlim)
        plt.title('月收益（热力图）', fontsize=14)
        plt.xlabel('月份')
        plt.ylabel('年份')
        plt.xticks(ticks=np.arange(len(months)), labels=[f'{m}月' for m in months])
        plt.yticks(ticks=np.arange(len(heat_df.index)), labels=heat_df.index.astype(int))
        cbar = plt.colorbar(im)
        cbar.set_label('收益 (%)')
        # 单元格标注
        for i in range(data.shape[0]):
            for j in range(data.shape[1]):
                val = data[i, j]
                if np.isnan(val):
                    continue
                text_color = 'black' if abs(val) < vlim * 0.6 else 'white'
                plt.text(j, i, f"{val:.1f}", ha='center', va='center', color=text_color, fontsize=8)
        plt.tight_layout()
        plt.savefig(img_path, dpi=300, bbox_inches='tight')
        print(f"月收益热力图已保存至: {img_path}")
        plt.close()


def export_open_counts(results: Dict[str, Any], img_path: str = None, csv_path: str = None, top_n: int = 30):
    """导出开仓标的次数柱状图和/或CSV（统计买入次数）"""
def export_open_counts(results: Dict[str, Any], img_path: str = None, csv_path: str = None, top_n: int = 30):
    """导出开仓标的次数柱状图和/或CSV（统计买入次数）"""
    if img_path:
        _ensure_plot_fonts()
    trades = results.get('trades', [])
    if not trades:
        print("无法统计开仓次数：无交易记录")
        return
    # 构建DataFrame
    rows = []
    for t in trades:
        rows.append(
            {
                'time': _get_trade_attr(t, 'time'),
                'security': _get_trade_attr(t, 'security'),
                'amount': _get_trade_attr(t, 'amount', 0),
            }
        )
    df = pd.DataFrame(rows)
    df = df.sort_values('time')
    open_df = df[df['amount'] > 0]
    if open_df.empty:
        print("无法统计开仓次数：无买入记录")
        return
    counts = open_df.groupby('security').size().sort_values(ascending=False)
    counts_df = counts.rename('开仓次数').to_frame()
    counts_df.index.name = '标的'
    # 获取中文名称映射
    name_map = {}
    try:
        from bullet_trade.data.api import get_all_securities
        for t in ['stock', 'etf', 'lof', 'fund', 'fja', 'fjb', 'index']:
            try:
                sec_df = get_all_securities(types=t)
                if not sec_df.empty:
                    for code in set(counts_df.index) & set(sec_df.index):
                        nm = sec_df.loc[code].get('display_name') if 'display_name' in sec_df.columns else None
                        if not nm:
                            nm = sec_df.loc[code].get('name') if 'name' in sec_df.columns else ''
                        if nm:
                            name_map[code] = str(nm)
            except Exception:
                pass
    except Exception:
        pass
    counts_df['名称'] = counts_df.index.map(lambda c: name_map.get(c, ''))
    if csv_path:
        counts_df.to_csv(csv_path, encoding='utf-8-sig')
        print(f"开仓次数CSV已导出至: {csv_path}")
    if img_path:
        # 绘制柱状图（Top N）
        plot_df = counts_df.head(top_n)
        plt.figure(figsize=(14, 6))
        plt.bar(range(len(plot_df)), plot_df['开仓次数'], color='#1f77b4')
        labels = [f"{str(c).split('.')[0]}\n{name_map.get(c, '')}" for c in plot_df.index]
        plt.title(f'开仓标的次数（Top {len(plot_df)}）', fontsize=14)
        plt.xlabel('标的')
        plt.ylabel('次数')
        plt.xticks(range(len(plot_df)), labels, rotation=45, ha='right')
        for i, v in enumerate(plot_df['开仓次数'].values):
            plt.text(i, v, f"{int(v)}", ha='center', va='bottom', fontsize=8)
        plt.tight_layout()
        plt.savefig(img_path, dpi=300, bbox_inches='tight')
        print(f"开仓次数柱状图已保存至: {img_path}")
        plt.close()
    if csv_path:
        counts_df.to_csv(csv_path, encoding='utf-8-sig')
        print(f"开仓次数CSV已导出至: {csv_path}")
    if img_path:
        # 绘制柱状图（Top N）
        plot_df = counts_df.head(top_n)
        plt.figure(figsize=(14, 6))
        plt.bar(range(len(plot_df)), plot_df['开仓次数'], color='#1f77b4')
        labels = [f"{str(c).split('.')[0]}\n{name_map.get(c, '')}" for c in plot_df.index]
        plt.title(f'开仓标的次数（Top {len(plot_df)}）', fontsize=14)
        plt.xlabel('标的')
        plt.ylabel('次数')
        plt.xticks(range(len(plot_df)), labels, rotation=45, ha='right')
        for i, v in enumerate(plot_df['开仓次数'].values):
            plt.text(i, v, f"{int(v)}", ha='center', va='bottom', fontsize=8)
        plt.tight_layout()
        plt.savefig(img_path, dpi=300, bbox_inches='tight')
        print(f"开仓次数柱状图已保存至: {img_path}")
        plt.close()


def export_instrument_pnl(results: Dict[str, Any], img_path: str = None, csv_path: str = None, top_n: int = 30):
    """导出分标的盈亏汇总柱状图和/或CSV（按交易+分红，考虑拆分）"""
def export_instrument_pnl(results: Dict[str, Any], img_path: str = None, csv_path: str = None, top_n: int = 30):
    """导出分标的盈亏汇总柱状图和/或CSV（按交易+分红，考虑拆分）"""
    if img_path:
        _ensure_plot_fonts()
    trades = results.get('trades', [])
    events = results.get('events', [])
    if not trades and not events:
        print("无法统计盈亏：无交易与事件数据")
        return
    # 合并时间轴（交易与事件）
    timeline = []
    for t in trades:
        timeline.append(
            {
                'ts': _get_trade_attr(t, 'time'),
                'type': 'trade',
                'security': _get_trade_attr(t, 'security'),
                'amount': _get_trade_attr(t, 'amount', 0) or 0,
                'price': _get_trade_attr(t, 'price', 0) or 0,
                'commission': _get_trade_attr(t, 'commission', 0) or 0,
                'tax': _get_trade_attr(t, 'tax', 0) or 0,
            }
        )
    for e in events:
        ts = e.get('strategy_time') or e.get('event_date')
        timeline.append({'ts': ts, 'type': 'event', **e})
    # 排序
    timeline = [x for x in timeline if x['ts'] is not None]
    timeline.sort(key=lambda x: x['ts'])
    
    # 按标的维护持仓与成本
    state = {}  # code -> {qty, avg_cost, realized}
    def get_state(code):
        if code not in state:
            state[code] = {'qty': 0, 'avg_cost': 0.0, 'realized': 0.0}
        return state[code]
    
    for item in timeline:
        if item['type'] == 'trade':
            code = item['security']
            s = get_state(code)
            amt = item['amount']
            price = item['price']
            fee = (item.get('commission', 0.0) or 0.0) + (item.get('tax', 0.0) or 0.0)
            if amt > 0:
                # 买入：更新加权平均成本
                total_cost_value = s['avg_cost'] * s['qty'] + price * amt
                new_qty = s['qty'] + amt
                s['avg_cost'] = (total_cost_value / new_qty) if new_qty > 0 else 0.0
                s['qty'] = new_qty
                # 手续费计入支出
                s['realized'] -= fee
            else:
                sell_qty = abs(amt)
                # 卖出：实现盈亏 = (卖价-均价)*数量 - 费用
                s['realized'] += (price - s['avg_cost']) * sell_qty
                s['realized'] -= fee
                s['qty'] = s['qty'] - sell_qty
                if s['qty'] < 0:
                    # 理论上不会发生，防御性处理
                    s['qty'] = 0
        else:  # event
            et = item.get('event_type')
            code = item.get('code')
            if not code:
                continue
            s = get_state(code)
            if et == '拆分/送转':
                scale = float(item.get('scale_factor') or 1.0)
                if abs(scale - 1.0) > 1e-9:
                    s['qty'] = int(round(s['qty'] * scale))
                    if s['avg_cost'] > 0: s['avg_cost'] = s['avg_cost'] / scale
            elif et == '现金分红':
                cash_in = float(item.get('cash_in') or 0.0)
                s['realized'] += cash_in
            # 其他事件不影响已实现盈亏
    
    # 汇总结果
    rows = []
    for code, s in state.items():
        rows.append({'标的': code, '盈亏(元)': s['realized']})
    pnl_df = pd.DataFrame(rows)
    if pnl_df.empty:
        print("无法统计盈亏：结果为空")
        return
    pnl_df = pnl_df.sort_values('盈亏(元)', ascending=False)
    # 获取中文名称映射并加入CSV
    name_map = {}
    try:
        from bullet_trade.data.api import get_all_securities
        for t in ['stock', 'etf', 'lof', 'fund', 'fja', 'fjb', 'index']:
            try:
                sec_df = get_all_securities(types=t)
                if not sec_df.empty:
                    for code in set(pnl_df['标的'].values) & set(sec_df.index):
                        nm = sec_df.loc[code].get('display_name') if 'display_name' in sec_df.columns else None
                        if not nm:
                            nm = sec_df.loc[code].get('name') if 'name' in sec_df.columns else ''
                        if nm:
                            name_map[code] = str(nm)
            except Exception:
                pass
    except Exception:
        pass
    pnl_df['名称'] = pnl_df['标的'].map(lambda c: name_map.get(c, ''))
    if csv_path:
        pnl_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"分标的盈亏CSV已导出至: {csv_path}")
    if img_path:
        # 绘制柱状图（Top N）
        plot_df = pnl_df.head(top_n)
        colors = ['#d62728' if v >= 0 else '#2ca02c' for v in plot_df['盈亏(元)'].values]
        plt.figure(figsize=(14, 6))
        plt.bar(range(len(plot_df)), plot_df['盈亏(元)'], color=colors)
        labels = [f"{str(c).split('.')[0]}\n{name_map.get(c, '')}" for c in plot_df['标的'].values]
        plt.title(f'分标的盈亏（Top {len(plot_df)}）', fontsize=14)
        plt.xlabel('标的')
        plt.ylabel('盈亏 (元)')
        plt.xticks(range(len(plot_df)), labels, rotation=45, ha='right')
        for i, v in enumerate(plot_df['盈亏(元)'].values):
            plt.text(i, v, f"{v:.0f}", ha='center', va='bottom' if v>=0 else 'top', fontsize=8)
        plt.tight_layout()
        plt.savefig(img_path, dpi=300, bbox_inches='tight')
        print(f"分标的盈亏柱状图已保存至: {img_path}")
        plt.close()
    if csv_path:
        pnl_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"分标的盈亏CSV已导出至: {csv_path}")
    if img_path:
        # 绘制柱状图（Top N）
        plot_df = pnl_df.head(top_n)
        colors = ['#d62728' if v >= 0 else '#2ca02c' for v in plot_df['盈亏(元)'].values]
        plt.figure(figsize=(14, 6))
        plt.bar(range(len(plot_df)), plot_df['盈亏(元)'], color=colors)
        labels = [f"{str(c).split('.')[0]}\n{name_map.get(c, '')}" for c in plot_df['标的'].values]
        plt.title(f'分标的盈亏（Top {len(plot_df)}）', fontsize=14)
        plt.xlabel('标的')
        plt.ylabel('盈亏 (元)')
        plt.xticks(range(len(plot_df)), labels, rotation=45, ha='right')
        for i, v in enumerate(plot_df['盈亏(元)'].values):
            plt.text(i, v, f"{v:.0f}", ha='center', va='bottom' if v>=0 else 'top', fontsize=8)
        plt.tight_layout()
        plt.savefig(img_path, dpi=300, bbox_inches='tight')
        print(f"分标的盈亏柱状图已保存至: {img_path}")
        plt.close()


def generate_html_report(results: Dict[str, Any] = None, output_file: Optional[str] = None, results_dir: Optional[str] = None):
    """生成交互式HTML报告。
    兼容两种最小化调用方式：
    1) 传入内存 results（原行为不变）
    2) 仅指定 results_dir，从目录CSV重建并生成（output_file 默认写入该目录）
    """
    import plotly.graph_objs as go
    import plotly.io as pio
    import plotly.subplots as sp
    import os

    # 兼容目录模式
    if results is None:
        if results_dir is None:
            raise ValueError('必须提供 results 或 results_dir')
        results = load_results_from_directory(results_dir)
        if output_file is None:
            output_file = os.path.join(results_dir, 'report.html')
    else:
        # 如提供目录，补全缺失字段
        if results_dir is not None:
            try:
                loaded = load_results_from_directory(results_dir)
                if results.get('daily_records') is None:
                    results['daily_records'] = loaded.get('daily_records')
                if not results.get('trades'):
                    results['trades'] = loaded.get('trades', [])
                if not results.get('events'):
                    results['events'] = loaded.get('events', [])
                if results.get('daily_positions') is None:
                    results['daily_positions'] = loaded.get('daily_positions', None)
                results['meta'] = _merge_meta_dict(results.get('meta', {}), loaded.get('meta', {}))
            except Exception:
                pass
        if output_file is None and results_dir is not None:
            output_file = os.path.join(results_dir, 'report.html')

    df = results.get('daily_records')
    if df is None or len(df) == 0:
        raise ValueError('缺少 daily_records 或为空，无法生成HTML报告')

    trades = results.get('trades', [])
    events = results.get('events', [])
    metrics = calculate_metrics(results)
    meta = results.get('meta', {})
    base_total_value = None
    try:
        base_total_value = float(meta.get('initial_total_value'))
    except Exception:
        base_total_value = None
    benchmark_ctx = _compute_benchmark_context(df, base_value=base_total_value)

    # 页面标题与运行耗时
    title = meta.get('strategy_file', '策略回测报告')
    date_range = f"{meta.get('start_date', '')} 至 {meta.get('end_date', '')}"
    runtime_seconds = meta.get('runtime_seconds', None)
    runtime_text = f"总用时: {runtime_seconds:.2f}s" if isinstance(runtime_seconds, (int, float)) else "总用时: 未记录"
    run_started_at = meta.get('run_started_at') or '未记录'
    run_finished_at = meta.get('run_finished_at') or '未记录'
    benchmark_code = meta.get('benchmark') or '-'
    
    # 样式（增加网格化风险指标布局与头部元信息 & 表格美化）
    style = """
    <style>
    body { font-family: "PingFang SC", "Noto Sans SC", "Microsoft YaHei", sans-serif; margin: 20px; color: #111827; }
    .header { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; }
    .brand { display: flex; align-items: center; gap: 14px; }
    .brand-logo { width: 40px; height: 40px; flex: 0 0 40px; }
    .eyebrow { font-size: 13px; font-weight: 700; color: #2563eb; letter-spacing: 0.08em; text-transform: uppercase; }
    .title { font-size: 24px; font-weight: bold; }
    .subtitle { color: #555; margin-top: 4px; }
    .meta { color: #333; font-size: 13px; }
    .section { margin-top: 24px; }
    .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; align-items: stretch; }
    .metric-item { border: 1px solid #eee; border-radius: 6px; padding: 8px 10px; background: #fafafa; }
    .metric-label { font-size: 12px; color: #666; }
    .metric-value { font-size: 16px; font-weight: 600; color: #111; margin-top: 4px; }

    /* 表格美化 */
    details { margin: 10px 0; }
    details summary { cursor: pointer; font-weight: 600; padding: 6px 8px; background: #f7f7f9; border: 1px solid #eee; border-radius: 6px; }
    details[open] summary { border-bottom-left-radius: 0; border-bottom-right-radius: 0; background: #f0f0f5; }
    .table-wrap { position: relative; }
    table.table { border-collapse: collapse; width: 100%; font-size: 13px; }
    table.table th, table.table td { border: 1px solid #e5e5e5; padding: 6px 8px; }
    table.table thead th { position: sticky; top: 0; background: #f5f6fa; z-index: 2; box-shadow: inset 0 -1px 0 #e5e5e5; }
    table.table-striped tbody tr:nth-child(odd) td { background-color: #fbfbfd; }
    table.table-colstriped tbody td:nth-child(odd) { background-color: rgba(0,0,0,0.02); }
    table.table tbody tr:hover td { background-color: #f2f7ff; }
    table.table caption { caption-side: top; text-align: left; color: #666; padding: 4px 0; }
    </style>
    """
    # 新增：每日持仓分组视图样式
    style += """
    <style>
    .tag.tag-cash { display:inline-block; background:#c9f7d8; color:#0b7d45; padding:2px 6px; border-radius:4px; font-weight:600; }
    .num { text-align:right; }
    .pos { color:#d62728; font-weight:600; }
    .neg { color:#2ca02c; font-weight:600; }
    .daily-footer { text-align:right; margin-top:6px; font-size:14px; }
    </style>
    """
    
    # 顶部头部
    meta_lines = [f"Benchmark: {benchmark_code}", f"回测启动: {run_started_at}", f"回测结束: {run_finished_at}", runtime_text]
    header_html = f"""
    <div class=\"header\">
      <div class=\"brand\">
        <img class=\"brand-logo\" src=\"https://bullettrade.cn/favicon.svg\" alt=\"BulletTrade\" />
        <div>
        <div class=\"eyebrow\">BulletTrade 回测</div>
        <div class=\"title\">{title}</div>
        <div class=\"subtitle\">{date_range}</div>
        </div>
      </div>
      <div class=\"meta\">{'<br>'.join(meta_lines)}</div>
    </div>
    """
    
    # 图1：总资产曲线 + 回撤（右轴）并标注Top5最大回撤区间
    fig_total = sp.make_subplots(specs=[[{"secondary_y": True}]])
    fig_total.add_trace(
        go.Scatter(
            x=df.index,
            y=df['total_value'],
            mode='lines',
            name='总资产',
            line=dict(color='#1f77b4', width=2)
        ),
        secondary_y=False
    )
    benchmark_value = benchmark_ctx.get('benchmark_value')
    if benchmark_value is not None:
        fig_total.add_trace(
            go.Scatter(
                x=df.index,
                y=benchmark_value,
                mode='lines',
                name=f"Benchmark({benchmark_code})",
                line=dict(color='#ff7f0e', width=2, dash='dot')
            ),
            secondary_y=False
        )
        excess_asset_value = pd.to_numeric(df['total_value'], errors='coerce') - pd.to_numeric(
            benchmark_value,
            errors='coerce',
        )
        fig_total.add_trace(
            go.Scatter(
                x=df.index,
                y=excess_asset_value,
                mode='lines',
                name='超额资产 (元)',
                line=dict(color='#9467bd', width=2, dash='dot')
            ),
            secondary_y=False
        )
    # 初始资金（用首日总资产作为近似）
    if len(df) > 0:
        init_cash = float(df['total_value'].iloc[0])
        fig_total.add_trace(
            go.Scatter(
                x=df.index,
                y=[init_cash] * len(df),
                mode='lines',
                name='初始资金',
                line=dict(color='red', dash='dash'),
                showlegend=True
            ),
            secondary_y=False
        )
    # 回撤（右轴）
    cummax = df['total_value'].expanding().max()
    drawdown = (df['total_value'] - cummax) / cummax * 100
    dd_min = float(drawdown.min()) if len(drawdown) > 0 else -1.0
    fig_total.add_trace(
        go.Scatter(
            x=df.index,
            y=drawdown,
            mode='lines',
            name='回撤 (%)',
            line=dict(color='darkred', width=2),
            fill='tozeroy'
        ),
        secondary_y=True
    )
    fig_total.update_layout(title='总资产与回撤', xaxis_title='日期')
    fig_total.update_yaxes(title_text='资产 / 超额资产 (元)', secondary_y=False)
    fig_total.update_yaxes(
        title_text='回撤 (%)',
        secondary_y=True,
        range=[min(dd_min * 1.05, -1.0), 0],
        tickfont=dict(color='darkred'),
        title_font=dict(color='darkred'),
    )
    
    # 标注Top5最大回撤区间（峰到谷）
    intervals = []
    peak_idx = 0
    peak_val = float(df['total_value'].iloc[0]) if len(df) > 0 else 0.0
    min_dd = 0.0
    min_idx = 0
    for i, v in enumerate(df['total_value'].values):
        v = float(v)
        if v >= peak_val - 1e-9:
            if min_dd < 0:
                intervals.append((peak_idx, min_idx, min_dd))
            peak_val = v
            peak_idx = i
            min_dd = 0.0
            min_idx = i
        else:
            dd_i = (v - peak_val) / peak_val
            if dd_i < min_dd:
                min_dd = dd_i
                min_idx = i
    if min_dd < 0:
        intervals.append((peak_idx, min_idx, min_dd))
    intervals.sort(key=lambda x: x[2])
    intervals = intervals[:5]
    for (p_idx, t_idx, ddv) in intervals:
        start = df.index[p_idx]
        end = df.index[t_idx]
        # 阴影高亮区间
        fig_total.add_vrect(x0=start, x1=end, fillcolor='rgba(214,39,40,0.3)', line_width=0, layer='below')
        # 在右轴（回撤轴）标注百分比与区间日期
        dd_val_pct = ddv * 100.0
        y = float(drawdown.iloc[t_idx]) if len(drawdown) > t_idx else 0
        fig_total.add_annotation(
            x=end,
            y=y,
            yref='y2',
            text=f"{abs(dd_val_pct):.2f}%<br>{start.date()} → {end.date()}",
            showarrow=True,
            arrowhead=2,
            ax=0,
            ay=-40,
            font=dict(size=10)
        )
    
    # 图2：年收益柱状图，添加数值标注
    annual_returns = df['daily_returns'].resample(_YEAR_END_FREQ).sum() * 100
    annual_years = [d.year for d in annual_returns.index]
    annual_values = annual_returns.values
    fig_annual = go.Figure()
    annual_colors = ['#d62728' if float(v) >= 0 else '#2ca02c' for v in annual_values]
    fig_annual.add_trace(go.Bar(
        x=annual_years,
        y=annual_values,
        marker=dict(color=annual_colors),
        text=[f"{v:.1f}%" for v in annual_values],
        textposition='outside',
        name='年收益'
    ))
    fig_annual.update_layout(title='年收益', xaxis_title='年份', yaxis_title='收益(%)', uniformtext_minsize=8, uniformtext_mode='hide')
    
    # 图3：月度热力图，添加每格标注（正红负绿）
    monthly_returns = df['daily_returns'].resample(_MONTH_END_FREQ).sum() * 100
    heatmap_data = pd.DataFrame({ 'month': monthly_returns.index.month, 'year': monthly_returns.index.year, 'value': monthly_returns.values })
    pivot = heatmap_data.pivot(index='year', columns='month', values='value').fillna(0)
    monthly_colorscale = [[0.0, '#2ca02c'], [0.5, '#ffffbf'], [1.0, '#d62728']]
    fig_monthly = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=list(pivot.columns),
        y=list(pivot.index),
        colorscale=monthly_colorscale,
        zmid=0,
        colorbar=dict(title='收益(%)')
    ))
    fig_monthly.update_layout(title='月度收益热力图', xaxis_title='月份', yaxis_title='年份')
    fig_monthly.update_yaxes(autorange='reversed')
    # 添加每格文字标注
    for yi, yv in enumerate(pivot.index):
        for xi, xv in enumerate(pivot.columns):
            val = pivot.iloc[yi, xi]
            fig_monthly.add_annotation(x=xv, y=yv, text=f"{val:.1f}%", showarrow=False, font=dict(size=10, color='black'))
    
    # 图4：开仓次数柱状图，添加数值标注
    if len(trades) > 0:
        trade_df = pd.DataFrame(trades)
        # 兼容不同来源的字段：优先使用 'date'，否则从 'time' 或中文 '时间' 派生
        if 'date' in trade_df.columns:
            trade_df['date'] = pd.to_datetime(trade_df['date']).dt.date
        elif 'time' in trade_df.columns:
            trade_df['date'] = pd.to_datetime(trade_df['time']).dt.date
        elif '时间' in trade_df.columns:
            trade_df['date'] = pd.to_datetime(trade_df['时间']).dt.date
        else:
            trade_df['date'] = pd.NaT
        open_counts = trade_df.dropna(subset=['date']).groupby('date').size().reset_index(name='count')
        fig_open = go.Figure()
        fig_open.add_trace(go.Bar(x=open_counts['date'], y=open_counts['count'], text=[f"{int(v)}" for v in open_counts['count']], textposition='outside', name='开仓次数'))
        fig_open.update_layout(title='开仓次数', xaxis_title='日期', yaxis_title='次数', uniformtext_minsize=8, uniformtext_mode='hide')
    else:
        fig_open = go.Figure()
        fig_open.update_layout(title='开仓次数 (无交易)')
    
    # 图5：分标盈亏柱状图（兼容无pnl字段），融合分红与拆分
    def _compute_instrument_pnl(trades_list, events_list):
        state = {}
        def get_state(code):
            if code not in state:
                state[code] = {'qty': 0, 'avg_cost': 0.0, 'realized': 0.0}
            return state[code]
        timeline = []
        for t in trades_list:
            try:
                ts = getattr(t, 'time', None) if not isinstance(t, dict) else t.get('time') or t.get('时间')
                security = getattr(t, 'security', None) if not isinstance(t, dict) else t.get('security') or t.get('标的') or t.get('code')
                amount = getattr(t, 'amount', 0) if not isinstance(t, dict) else t.get('amount') or t.get('数量') or 0
                price = getattr(t, 'price', 0.0) if not isinstance(t, dict) else t.get('price') or t.get('价格') or 0.0
                commission = getattr(t, 'commission', 0.0) if not isinstance(t, dict) else t.get('commission') or t.get('手续费') or 0.0
                tax = getattr(t, 'tax', 0.0) if not isinstance(t, dict) else t.get('tax') or t.get('印花税') or 0.0
                if ts and security:
                    timeline.append({'ts': ts, 'type': 'trade', 'security': security, 'amount': amount, 'price': price, 'commission': commission, 'tax': tax})
            except Exception:
                continue
        for e in events_list:
            ts = e.get('strategy_time') or e.get('event_date')
            if ts:
                timeline.append({'ts': ts, 'type': 'event', **e})
        timeline.sort(key=lambda x: x['ts'])
        for item in timeline:
            if item['type'] == 'trade':
                code = item['security']
                s = get_state(code)
                amt = int(item.get('amount') or 0)
                price = float(item.get('price') or 0.0)
                fee = float(item.get('commission') or 0.0) + float(item.get('tax') or 0.0)
                if amt > 0:
                    total_cost_value = s['avg_cost'] * s['qty'] + price * amt
                    new_qty = s['qty'] + amt
                    s['avg_cost'] = (total_cost_value / new_qty) if new_qty > 0 else 0.0
                    s['qty'] = new_qty
                    s['realized'] -= fee
                else:
                    sell_qty = abs(amt)
                    s['realized'] += (price - s['avg_cost']) * sell_qty
                    s['realized'] -= fee
                    s['qty'] = max(0, s['qty'] - sell_qty)
            else:
                et = item.get('event_type')
                code = item.get('code')
                if not code:
                    continue
                s = get_state(code)
                if et == '拆分/送转':
                    scale = float(item.get('scale_factor') or 1.0)
                    if abs(scale - 1.0) > 1e-9:
                        s['qty'] = int(round(s['qty'] * scale))
                        s['avg_cost'] = (s['avg_cost'] / scale) if s['avg_cost'] > 0 else 0.0
                elif et == '现金分红':
                    cash_in = float(item.get('cash_in') or 0.0)
                    s['realized'] += cash_in
        rows = [{'code': code, 'pnl': s['realized']} for code, s in state.items()]
        return pd.DataFrame(rows)
    try:
        pnl_df = _compute_instrument_pnl(trades, events)
        if pnl_df is not None and not pnl_df.empty:
            pnl_df = pnl_df.sort_values('pnl', ascending=False)

            # 构造名称映射：优先 instrument_pnl.csv -> daily_positions -> events/trades
            name_map = {}
            try:
                if results_dir:
                    ip_csv = os.path.join(results_dir, 'instrument_pnl.csv')
                    if os.path.isfile(ip_csv):
                        ip_df = pd.read_csv(ip_csv, encoding='utf-8')
                        code_col = '标的' if '标的' in ip_df.columns else ('code' if 'code' in ip_df.columns else None)
                        name_col = '名称' if '名称' in ip_df.columns else ('name' if 'name' in ip_df.columns else None)
                        if code_col and name_col:
                            name_map.update({str(c): str(n) for c, n in zip(ip_df[code_col], ip_df[name_col])})
            except Exception:
                pass
            try:
                dp = results.get('daily_positions')
                if dp is not None and not dp.empty:
                    code_col = 'security' if 'security' in dp.columns else ('标的' if '标的' in dp.columns else ('code' if 'code' in dp.columns else None))
                    name_col = 'name' if 'name' in dp.columns else ('名称' if '名称' in dp.columns else None)
                    if code_col and name_col:
                        for c, n in zip(dp[code_col], dp[name_col]):
                            if str(c) not in name_map and pd.notna(n):
                                name_map[str(c)] = str(n)
            except Exception:
                pass
            try:
                # 来自事件与交易的兜底
                for e in events:
                    c = e.get('code')
                    n = e.get('name') or e.get('名称')
                    if c and n and str(c) not in name_map:
                        name_map[str(c)] = str(n)
                if len(trades) > 0:
                    tdf = pd.DataFrame(trades)
                    t_code_col = 'security' if 'security' in tdf.columns else ('标的' if '标的' in tdf.columns else ('code' if 'code' in tdf.columns else None))
                    t_name_col = 'name' if 'name' in tdf.columns else ('名称' if '名称' in tdf.columns else None)
                    if t_code_col and t_name_col:
                        last_names = tdf.dropna(subset=[t_name_col]).groupby(t_code_col)[t_name_col].last()
                        for c, n in last_names.items():
                            if str(c) not in name_map:
                                name_map[str(c)] = str(n)
            except Exception:
                pass

            # X 轴标签与颜色：代码+名称，正红负绿
            x_labels = [f"{str(c).split('.')[0]}\n{name_map.get(str(c), '')}" for c in pnl_df['code']]
            bar_colors = ['#d62728' if float(v) >= 0 else '#2ca02c' for v in pnl_df['pnl']]

            fig_pnl = go.Figure()
            fig_pnl.add_trace(go.Bar(x=x_labels, y=pnl_df['pnl'], marker=dict(color=bar_colors), text=[f"{v:.2f}" for v in pnl_df['pnl']], textposition='outside', name='分标盈亏'))
            fig_pnl.update_layout(title='分标盈亏', xaxis_title='标的(代码/名称)', yaxis_title='盈亏', uniformtext_minsize=8, uniformtext_mode='hide')
        else:
            fig_pnl = go.Figure()
            fig_pnl.update_layout(title='分标盈亏 (无交易或无事件)')
    except Exception:
        fig_pnl = go.Figure()
        fig_pnl.update_layout(title='分标盈亏 (数据缺失)')
    
    # 风险指标网格（紧凑布局）
    metrics_items = list(metrics.items())
    # 格式化部分指标（带百分号）
    percent_keys = {'策略收益','策略年化收益','基准收益','基准年化收益','累计超额收益','最大回撤','日胜率','交易胜率','胜率'}
    metrics_html_items = []
    for k, v in metrics_items:
        val = v
        if k in percent_keys and isinstance(v, (int, float)):
            val = f"{v:.2f}%"
        elif isinstance(v, float):
            val = f"{v:.4f}"
        metrics_html_items.append(f"<div class='metric-item'><div class='metric-label'>{k}</div><div class='metric-value'>{val}</div></div>")
    metrics_grid_html = f"<div class='metrics-grid'>{''.join(metrics_html_items)}</div>"
    
    # 导出各图为HTML片段
    total_html = pio.to_html(fig_total, include_plotlyjs='cdn', full_html=False)
    annual_html = pio.to_html(fig_annual, include_plotlyjs=False, full_html=False)
    monthly_html = pio.to_html(fig_monthly, include_plotlyjs=False, full_html=False)
    open_html = pio.to_html(fig_open, include_plotlyjs=False, full_html=False)
    pnl_html = pio.to_html(fig_pnl, include_plotlyjs=False, full_html=False)
    
    # 日收益日历图（pyecharts）
    try:
        calendar_section_html = build_calendar_section_html_from_df(
            df,
            title_prefix=os.path.basename(title),
            color_method="log",
            color_k=0.2,
            show_visualmap=False,
            height_px=110
        )
    except Exception:
        calendar_section_html = ""
    
    # CSV数据表（可折叠）
    def df_html(df_in: pd.DataFrame) -> str:
        try:
            html_table = df_in.to_html(index=False, border=0, classes='table table-striped table-colstriped', escape=True)
        except Exception:
            try:
                html_table = pd.DataFrame(df_in).to_html(index=False, border=0, classes='table table-striped table-colstriped', escape=True)
            except Exception:
                html_table = "<p>无法渲染表格</p>"
        return f"<div class='table-wrap' style='overflow:auto; max-height:480px; border:1px solid #eee; padding:0;'>{html_table}</div>"

    html_parts = []
    html_parts.append("<section class='section'>")
    html_parts.append("<h2>CSV数据表</h2>")

    # trades
    try:
        if isinstance(trades, list) and len(trades) > 0:
            def _tr_to_dict(t):
                if isinstance(t, dict):
                    return t
                return {
                    'time': getattr(t, 'time', None),
                    'security': getattr(t, 'security', None),
                    'amount': getattr(t, 'amount', None),
                    'price': getattr(t, 'price', None),
                    'commission': getattr(t, 'commission', None),
                    'tax': getattr(t, 'tax', None),
                }
            trades_rows = [_tr_to_dict(t) for t in trades]
            trades_df = pd.DataFrame(trades_rows)
        else:
            trades_df = pd.DataFrame()
    except Exception:
        trades_df = pd.DataFrame()
    if not trades_df.empty:
        html_parts.append("<details><summary>交易记录 trades.csv</summary>" + df_html(trades_df) + "</details>")
    else:
        html_parts.append("<details><summary>交易记录 trades.csv</summary><p>无交易记录</p></details>")

    # daily_records
    try:
        dr_df = df.copy()
        dr_df_reset = dr_df.reset_index()
        if dr_df_reset.columns[0] not in ['date','日期']:
            dr_df_reset.rename(columns={dr_df_reset.columns[0]: 'date'}, inplace=True)
        html_parts.append("<details><summary>每日数据 daily_records.csv</summary>" + df_html(dr_df_reset) + "</details>")
    except Exception:
        pass

    # annual_returns
    try:
        annual_df = df['daily_returns'].resample(_YEAR_END_FREQ).sum().to_frame('annual_returns')
        annual_df = annual_df.reset_index()
        annual_df.rename(columns={annual_df.columns[0]: 'year'}, inplace=True)
        annual_df['annual_returns'] = annual_df['annual_returns'] * 100.0
        html_parts.append("<details><summary>年收益 annual_returns.csv</summary>" + df_html(annual_df) + "</details>")
    except Exception:
        pass

    # monthly_returns
    try:
        heat_df = pivot.copy()
        heat_df = heat_df.reset_index()
        heat_df.rename(columns={heat_df.columns[0]: 'year'}, inplace=True)
        html_parts.append("<details><summary>月收益 monthly_returns.csv</summary>" + df_html(heat_df) + "</details>")
    except Exception:
        pass

    # open_counts（按标的统计开仓次数）
    try:
        if len(trades) > 0:
            tdf = pd.DataFrame(trades)
            sec_col = 'security' if 'security' in tdf.columns else ('标的' if '标的' in tdf.columns else ('code' if 'code' in tdf.columns else None))
            amt_col = 'amount' if 'amount' in tdf.columns else ('数量' if '数量' in tdf.columns else None)
            if sec_col and amt_col:
                oc_df = tdf[tdf[amt_col].astype(float) > 0].groupby(sec_col).size().sort_values(ascending=False).rename('开仓次数').to_frame()
                oc_df = oc_df.reset_index().rename(columns={sec_col: '标的'})
                html_parts.append("<details><summary>开仓次数 open_counts.csv</summary>" + df_html(oc_df) + "</details>")
    except Exception:
        pass

    # instrument_pnl
    try:
        if 'pnl_df' in locals() and pnl_df is not None and not pnl_df.empty:
            pnl_df_show = pnl_df.copy()
            if 'name_map' in locals() and isinstance(name_map, dict):
                pnl_df_show['name'] = pnl_df_show['code'].map(lambda c: name_map.get(str(c), ''))
            html_parts.append("<details><summary>分标的盈亏 instrument_pnl.csv</summary>" + df_html(pnl_df_show) + "</details>")
    except Exception:
        pass

    # events
    try:
        events_df = pd.DataFrame(events)
        if not events_df.empty:
            html_parts.append("<details><summary>分红/拆分事件 dividend_split_events.csv</summary>" + df_html(events_df) + "</details>")
    except Exception:
        pass

    # daily_positions
    try:
        daily_pos = results.get('daily_positions')
        if daily_pos is not None and not getattr(daily_pos, 'empty', True):
            html_parts.append("<details><summary>每日持仓快照 daily_positions.csv</summary>" + df_html(daily_pos) + "</details>")
    except Exception:
        pass

    html_parts.append("</section>")
    csv_tables_html = "".join(html_parts)

    # 新增：每日持仓分组视图（按日聚合展示）
    daily_group_html = ""
    trade_group_html = ""
    try:
        dp = results.get('daily_positions')
        if dp is not None and not getattr(dp, 'empty', True):
            dp = dp.copy()
            if 'date' in dp.columns:
                try:
                    dp['date'] = pd.to_datetime(dp['date'])
                except Exception:
                    pass
                dp['date_key'] = dp['date'].dt.date
            else:
                dp['date_key'] = pd.NaT
            if 'value' not in dp.columns:
                dp['value'] = dp.get('price', 0.0).astype(float) * dp.get('amount', 0).astype(float)
            if 'floating_pnl' not in dp.columns:
                if 'avg_cost' in dp.columns:
                    dp['floating_pnl'] = (dp.get('price', 0.0).astype(float) - dp['avg_cost'].astype(float)) * dp.get('amount', 0).astype(float)
                else:
                    dp['floating_pnl'] = 0.0
            idx_dates = pd.to_datetime(df.index).date
            cash_map = {d: float(c) for d, c in zip(idx_dates, df['cash'])} if 'cash' in df.columns else {}
            sections = []
            for dkey, g in dp.groupby('date_key'):
                 cash_val = cash_map.get(dkey, 0.0)
                 mv_sum = float(g['value'].sum()) if 'value' in g.columns else 0.0
                 pnl_sum = float(g['floating_pnl'].sum()) if 'floating_pnl' in g.columns else 0.0
                 rows = []
                 rows.append(f"<tr><td><span class='tag tag-cash'>Cash</span></td><td></td><td></td><td class='num'>{cash_val:,.2f}</td><td class='num'>0.00</td></tr>")
                 for _, r in g.iterrows():
                     name = r.get('name') or r.get('名称') or ''
                     code = r.get('code') or r.get('security') or ''
                     label = f"{name}({str(code)})" if name else str(code)
                     amount = int(float(r.get('amount') or 0))
                     price_val = float(r.get('price') or 0.0)
                     mv = float(r.get('value') or 0.0)
                     pnl = float(r.get('floating_pnl') or 0.0)
                     pnl_cls = 'neg' if pnl < 0 else 'pos'
                     # 价格统一使用3位小数精度（ETF为0.001步长）
                     rows.append(f"<tr><td>{label}</td><td class='num'>{amount}</td><td class='num'>{price_val:.3f}</td><td class='num'>{mv:,.2f}</td><td class='num {pnl_cls}'>{pnl:,.2f}</td></tr>")
                 total_mv = cash_val + mv_sum
                 pnl_cls_sum = 'neg' if pnl_sum < 0 else 'pos'
                 table = "<table class='table'><thead><tr><th>标的</th><th>数量</th><th>价格</th><th>市值</th><th>盈亏</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
                 footer = f"<div class='daily-footer'>总共: <b>{total_mv:,.2f}</b> <span class='pnl {pnl_cls_sum}'>{pnl_sum:,.2f}</span></div>"
                 sections.append(f"<details><summary>{dkey}</summary>{table}{footer}</details>")

            # 交易记录分组视图（与每日持仓分组视图相似）
            trade_sections = []
            try:
                 if isinstance(trades, list) and len(trades) > 0:
                     def _tr_to_dict(t):
                         if isinstance(t, dict):
                             return t
                         return {
                             'time': getattr(t, 'time', None),
                             'security': getattr(t, 'security', None),
                             'amount': getattr(t, 'amount', None),
                             'price': getattr(t, 'price', None),
                             'commission': getattr(t, 'commission', None),
                             'tax': getattr(t, 'tax', None),
                             'name': getattr(t, 'name', None),
                         }
                     t_rows = [_tr_to_dict(t) for t in trades]
                     tdf = pd.DataFrame(t_rows)
                     # 日期键
                     if 'date' in tdf.columns:
                         tdf['date_key'] = pd.to_datetime(tdf['date']).dt.date
                     elif 'time' in tdf.columns:
                         tdf['date_key'] = pd.to_datetime(tdf['time']).dt.date
                     elif '时间' in tdf.columns:
                         tdf['date_key'] = pd.to_datetime(tdf['时间']).dt.date
                     else:
                         tdf['date_key'] = pd.NaT
                     # 规范字段
                     def _get_val(r, klist, default=None):
                         for k in klist:
                             if k in r and pd.notna(r[k]):
                                 return r[k]
                         return default
                     for dkey, g in tdf.groupby('date_key'):
                         rows_t = []
                         total_amount_value = 0.0
                         total_fee = 0.0
                         for _, r in g.iterrows():
                             name = _get_val(r, ['name','名称'], '')
                             code = _get_val(r, ['security','标的','code'], '')
                             label = f"{name}({str(code)})" if name else str(code)
                             amt = float(_get_val(r, ['amount','数量'], 0) or 0)
                             price = float(_get_val(r, ['price','价格'], 0.0) or 0.0)
                             commission = float(_get_val(r, ['commission','手续费'], 0.0) or 0.0)
                             tax = float(_get_val(r, ['tax','印花税'], 0.0) or 0.0)
                             value = price * abs(amt)
                             fee = commission + tax
                             direction = '买入' if amt > 0 else '卖出'
                             rows_t.append(f"<tr><td>{label}</td><td class='num'>{int(abs(amt))}</td><td class='num'>{price:.3f}</td><td class='num'>{value:,.2f}</td><td class='num'>{fee:,.2f}</td><td>{direction}</td></tr>")
                             total_amount_value += value
                             total_fee += fee
                         table_t = "<table class='table'><thead><tr><th>标的</th><th>数量</th><th>价格</th><th>成交额</th><th>费用</th><th>方向</th></tr></thead><tbody>" + "".join(rows_t) + "</tbody></table>"
                         footer_t = f"<div class='daily-footer'>总成交额: <b>{total_amount_value:,.2f}</b> | 总费用: <b>{total_fee:,.2f}</b></div>"
                         trade_sections.append(f"<details><summary>{dkey}</summary>{table_t}{footer_t}</details>")
            except Exception:
                pass

            if sections:
                 inner = "".join(sections)
                 daily_group_html = "<section class='section'><h2>每日持仓分组视图</h2><details><summary>每日持仓分组视图 daily_positions_grouped</summary>" + inner + "</details></section>"

                 # 独立构建交易分组视图为同级 section
                 if trade_sections:
                     trade_group_html = "<section class='section'><h2>交易记录分组视图</h2><details><summary>交易记录分组视图 daily_trades_grouped</summary>" + "".join(trade_sections) + "</details></section>"
    except Exception:
        daily_group_html = ""

    # 组装完整 HTML：添加 head、charset 与标题
    head_html = f"""<!DOCTYPE html>
<html lang=\"zh-CN\">
<head>
<meta charset=\"utf-8\">
<meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\">
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
<title>BulletTrade 回测 - {title}</title>
{style}
</head>
<body>
"""
    body_html_parts = [header_html, metrics_grid_html, total_html, annual_html, monthly_html, calendar_section_html, open_html, pnl_html, daily_group_html, trade_group_html, csv_tables_html, "</body></html>"]
    html = "\n".join([head_html] + body_html_parts)

    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html)
    return html


def export_annual_returns(results: Dict[str, Any], img_path: str = None, csv_path: str = None):
    """导出年收益图和/或CSV（根据传入路径决定输出内容）"""
    df = results['daily_records']
    dr = df['daily_returns'].dropna()
    if dr.empty:
        print("无法计算年收益：无每日收益数据")
        return
    annual = (dr + 1).groupby(dr.index.year).apply(lambda x: x.prod() - 1) * 100
    annual_df = annual.rename('年收益(%)').to_frame()
    annual_df.index.name = '年份'
    if csv_path:
        annual_df.to_csv(csv_path, encoding='utf-8-sig')
        print(f"年收益CSV已导出至: {csv_path}")
    if img_path:
        # 绘制柱状图
        plt.figure(figsize=(12, 4))
        years = annual_df.index.astype(int)
        values = annual_df['年收益(%)'].values
        colors = ['#d62728' if v >= 0 else '#2ca02c' for v in values]
        plt.bar(years, values, color=colors)
        plt.title('年收益', fontsize=14)
        plt.xlabel('年份')
        plt.ylabel('收益 (%)')
        for x, v in zip(years, values):
            plt.text(x, v, f"{v:.1f}%", ha='center', va='bottom' if v>=0 else 'top', fontsize=9)
        plt.tight_layout()
        plt.savefig(img_path, dpi=300, bbox_inches='tight')
        print(f"年收益图已保存至: {img_path}")
        plt.close()


def export_monthly_returns_heatmap(results: Dict[str, Any], img_path: str = None, csv_path: str = None):
    """导出月收益热力图和/或CSV（根据传入路径决定输出内容）"""
    df = results['daily_records']
    dr = df['daily_returns'].dropna()
    if dr.empty:
        print("无法计算月收益：无每日收益数据")
        return
    monthly = (dr + 1).groupby([dr.index.year, dr.index.month]).apply(lambda x: x.prod() - 1) * 100
    heat_df = monthly.unstack(level=1)
    # 确保列为1-12
    months = list(range(1, 13))
    heat_df = heat_df.reindex(columns=months)
    heat_df.index.name = '年份'
    heat_df.columns = [f'{m}月' for m in months]
    if csv_path:
        heat_df.to_csv(csv_path, encoding='utf-8-sig')
        print(f"月收益CSV已导出至: {csv_path}")
    if img_path:
        # 绘制热力图（使用matplotlib）
        plt.figure(figsize=(14, 6))
        data = heat_df.values.astype(float)
        # 配置对称色轴，正绿负红
        if np.isnan(data).all():
            vlim = 1.0
        else:
            vlim = max(abs(np.nanmin(data)), abs(np.nanmax(data)))
            if not np.isfinite(vlim) or vlim == 0:
                vlim = 1.0
        im = plt.imshow(data, aspect='auto', cmap='RdYlGn_r', vmin=-vlim, vmax=vlim)
        plt.title('月收益（热力图）', fontsize=14)
        plt.xlabel('月份')
        plt.ylabel('年份')
        plt.xticks(ticks=np.arange(len(months)), labels=[f'{m}月' for m in months])
        plt.yticks(ticks=np.arange(len(heat_df.index)), labels=heat_df.index.astype(int))
        cbar = plt.colorbar(im)
        cbar.set_label('收益 (%)')
        # 单元格标注
        for i in range(data.shape[0]):
            for j in range(data.shape[1]):
                val = data[i, j]
                if np.isnan(val):
                    continue
                text_color = 'black' if abs(val) < vlim * 0.6 else 'white'
                plt.text(j, i, f"{val:.1f}", ha='center', va='center', color=text_color, fontsize=8)
        plt.tight_layout()
        plt.savefig(img_path, dpi=300, bbox_inches='tight')
        print(f"月收益热力图已保存至: {img_path}")
        plt.close()


def export_open_counts(results: Dict[str, Any], img_path: str = None, csv_path: str = None, top_n: int = 30):
    """导出开仓标的次数柱状图和/或CSV（统计买入次数）"""
    trades = results.get('trades', [])
    if not trades:
        print("无法统计开仓次数：无交易记录")
        return
    # 构建DataFrame
    rows = []
    for t in trades:
        rows.append(
            {
                'time': _get_trade_attr(t, 'time'),
                'security': _get_trade_attr(t, 'security'),
                'amount': _get_trade_attr(t, 'amount', 0),
            }
        )
    df = pd.DataFrame(rows)
    df = df.sort_values('time')
    open_df = df[df['amount'] > 0]
    if open_df.empty:
        print("无法统计开仓次数：无买入记录")
        return
    counts = open_df.groupby('security').size().sort_values(ascending=False)
    counts_df = counts.rename('开仓次数').to_frame()
    counts_df.index.name = '标的'
    # 获取中文名称映射
    name_map = {}
    try:
        from bullet_trade.data.api import get_all_securities
        for t in ['stock', 'etf', 'lof', 'fund', 'fja', 'fjb', 'index']:
            try:
                sec_df = get_all_securities(types=t)
                if not sec_df.empty:
                    for code in set(counts_df.index) & set(sec_df.index):
                        nm = sec_df.loc[code].get('display_name') if 'display_name' in sec_df.columns else None
                        if not nm:
                            nm = sec_df.loc[code].get('name') if 'name' in sec_df.columns else ''
                        if nm:
                            name_map[code] = str(nm)
            except Exception:
                pass
    except Exception:
        pass
    counts_df['名称'] = counts_df.index.map(lambda c: name_map.get(c, ''))
    if csv_path:
        counts_df.to_csv(csv_path, encoding='utf-8-sig')
        print(f"开仓次数CSV已导出至: {csv_path}")
    if img_path:
        # 绘制柱状图（Top N）
        plot_df = counts_df.head(top_n)
        plt.figure(figsize=(14, 6))
        plt.bar(range(len(plot_df)), plot_df['开仓次数'], color='#1f77b4')
        labels = [f"{str(c).split('.')[0]}\n{name_map.get(c, '')}" for c in plot_df.index]
        plt.title(f'开仓标的次数（Top {len(plot_df)}）', fontsize=14)
        plt.xlabel('标的')
        plt.ylabel('次数')
        plt.xticks(range(len(plot_df)), labels, rotation=45, ha='right')
        for i, v in enumerate(plot_df['开仓次数'].values):
            plt.text(i, v, f"{int(v)}", ha='center', va='bottom', fontsize=8)
        plt.tight_layout()
        plt.savefig(img_path, dpi=300, bbox_inches='tight')
        print(f"开仓次数柱状图已保存至: {img_path}")
        plt.close()


def export_instrument_pnl(results: Dict[str, Any], img_path: str = None, csv_path: str = None, top_n: int = 30):
    """导出分标的盈亏汇总柱状图和/或CSV（按交易+分红，考虑拆分）"""
    trades = results.get('trades', [])
    events = results.get('events', [])
    if not trades and not events:
        print("无法统计盈亏：无交易与事件数据")
        return
    # 合并时间轴（交易与事件）
    timeline = []
    for t in trades:
        timeline.append(
            {
                'ts': _get_trade_attr(t, 'time'),
                'type': 'trade',
                'security': _get_trade_attr(t, 'security'),
                'amount': _get_trade_attr(t, 'amount', 0) or 0,
                'price': _get_trade_attr(t, 'price', 0) or 0,
                'commission': _get_trade_attr(t, 'commission', 0) or 0,
                'tax': _get_trade_attr(t, 'tax', 0) or 0,
            }
        )
    for e in events:
        ts = e.get('strategy_time') or e.get('event_date')
        timeline.append({'ts': ts, 'type': 'event', **e})
    # 排序
    timeline = [x for x in timeline if x['ts'] is not None]
    timeline.sort(key=lambda x: x['ts'])
    
    # 按标的维护持仓与成本
    state = {}  # code -> {qty, avg_cost, realized}
    def get_state(code):
        if code not in state:
            state[code] = {'qty': 0, 'avg_cost': 0.0, 'realized': 0.0}
        return state[code]
    
    for item in timeline:
        if item['type'] == 'trade':
            code = item['security']
            s = get_state(code)
            amt = item['amount']
            price = item['price']
            fee = (item.get('commission', 0.0) or 0.0) + (item.get('tax', 0.0) or 0.0)
            if amt > 0:
                # 买入：更新加权平均成本
                total_cost_value = s['avg_cost'] * s['qty'] + price * amt
                new_qty = s['qty'] + amt
                s['avg_cost'] = (total_cost_value / new_qty) if new_qty > 0 else 0.0
                s['qty'] = new_qty
                # 手续费计入支出
                s['realized'] -= fee
            else:
                sell_qty = abs(amt)
                # 卖出：实现盈亏 = (卖价-均价)*数量 - 费用
                s['realized'] += (price - s['avg_cost']) * sell_qty
                s['realized'] -= fee
                s['qty'] = s['qty'] - sell_qty
                if s['qty'] < 0:
                    # 理论上不会发生，防御性处理
                    s['qty'] = 0
        else:  # event
            et = item.get('event_type')
            code = item.get('code')
            if not code:
                continue
            s = get_state(code)
            if et == '拆分/送转':
                scale = float(item.get('scale_factor') or 1.0)
                if abs(scale - 1.0) > 1e-9:
                    s['qty'] = int(round(s['qty'] * scale))
                    if s['avg_cost'] > 0: s['avg_cost'] = s['avg_cost'] / scale
            elif et == '现金分红':
                cash_in = float(item.get('cash_in') or 0.0)
                s['realized'] += cash_in
            # 其他事件不影响已实现盈亏
    
    # 汇总结果
    rows = []
    for code, s in state.items():
        rows.append({'标的': code, '盈亏(元)': s['realized']})
    pnl_df = pd.DataFrame(rows)
    if pnl_df.empty:
        print("无法统计盈亏：结果为空")
        return
    pnl_df = pnl_df.sort_values('盈亏(元)', ascending=False)
    # 获取中文名称映射并加入CSV
    name_map = {}
    try:
        from bullet_trade.data.api import get_all_securities
        for t in ['stock', 'etf', 'lof', 'fund', 'fja', 'fjb', 'index']:
            try:
                sec_df = get_all_securities(types=t)
                if not sec_df.empty:
                    for code in set(pnl_df['标的'].values) & set(sec_df.index):
                        nm = sec_df.loc[code].get('display_name') if 'display_name' in sec_df.columns else None
                        if not nm:
                            nm = sec_df.loc[code].get('name') if 'name' in sec_df.columns else ''
                        if nm:
                            name_map[code] = str(nm)
            except Exception:
                pass
    except Exception:
        pass
    pnl_df['名称'] = pnl_df['标的'].map(lambda c: name_map.get(c, ''))
    if csv_path:
        pnl_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"分标的盈亏CSV已导出至: {csv_path}")
    if img_path:
        # 绘制柱状图（Top N）
        plot_df = pnl_df.head(top_n)
        colors = ['#d62728' if v >= 0 else '#2ca02c' for v in plot_df['盈亏(元)'].values]
        plt.figure(figsize=(14, 6))
        plt.bar(range(len(plot_df)), plot_df['盈亏(元)'], color=colors)
        labels = [f"{str(c).split('.')[0]}\n{name_map.get(c, '')}" for c in plot_df['标的'].values]
        plt.title(f'分标的盈亏（Top {len(plot_df)}）', fontsize=14)
        plt.xlabel('标的')
        plt.ylabel('盈亏 (元)')
        plt.xticks(range(len(plot_df)), labels, rotation=45, ha='right')
        for i, v in enumerate(plot_df['盈亏(元)'].values):
            plt.text(i, v, f"{v:.0f}", ha='center', va='bottom' if v>=0 else 'top', fontsize=8)
        plt.tight_layout()
        plt.savefig(img_path, dpi=300, bbox_inches='tight')
        print(f"分标的盈亏柱状图已保存至: {img_path}")
        plt.close()


def rebuild_report_from_directory(
    results_dir: str,
    gen_images: bool = False,
    gen_csv: bool = True,
    gen_html: bool = True,
    show_plots: bool = False,
):
    results = load_results_from_directory(results_dir)
    return generate_report(
        results=results,
        output_dir=results_dir,
        gen_images=gen_images,
        gen_csv=gen_csv,
        gen_html=gen_html,
        show_plots=show_plots,
    )


# —— 推导“日收益率”（decimal），优先用 total_value 的日变化；否则用 daily_returns
def _infer_daily_return_for_calendar_df(df: pd.DataFrame) -> pd.Series:
    df = df.sort_index().copy()
    idx = pd.DatetimeIndex(df.index)
    if 'total_value' in df.columns:
        s = pd.to_numeric(df['total_value'], errors='coerce')
        r_d = s.pct_change()
        # 若首日为空且有 daily_returns，则用首日 daily_returns 近似
        if 'daily_returns' in df.columns and pd.isna(r_d.iloc[0]):
            unit = 1.0 + pd.to_numeric(df['daily_returns'], errors='coerce').fillna(0.0)
            r_d.iloc[0] = unit.iloc[0] - 1.0
    elif 'daily_returns' in df.columns:
        unit = 1.0 + pd.to_numeric(df['daily_returns'], errors='coerce').fillna(0.0)
        r_d = unit.pct_change()
        r_d.iloc[0] = unit.iloc[0] - 1.0
    else:
        r_d = pd.Series(0.0, index=idx)
    r_d = r_d.fillna(0.0)
    r_d.index = idx
    return r_d

# —— 带符号的颜色变换：method ∈ {"log","asinh","linear"}；k 为“百分点”尺度
def _color_transform_for_calendar(v_pct: pd.Series, method: str = "log", k: float = 0.5) -> pd.Series:
    v = v_pct.astype(float)
    s = max(float(k), 1e-9)
    if method == "log":
        t = np.sign(v) * np.log1p(np.abs(v) / s)
    elif method == "asinh":
        t = np.arcsinh(v / s)
    else:  # "linear"
        t = v
    return pd.Series(t, index=v_pct.index)

# —— 生成日收益日历的 Page（pyecharts）
def build_daily_return_calendar_page_from_df(
    df: pd.DataFrame,
    title_prefix: str = None,
    color_method: str = "log",
    color_k: float = 0.5,
    show_visualmap: bool = False,
    height_px: int = 160,
    height_ratio: float = 0.13,
    inner_top_px: int = 2,
    inner_bottom_px: int = 2,
    hatch_holiday: bool = True,
    holiday_label: str = "休市",
    holiday_color: str = "#FFFFFF",
    hatch_rotation: float = 0.785398,
    hatch_alpha: float = 0.1,
):
    try:
        from pyecharts.charts import Calendar, Page
        from pyecharts import options as opts
        from pyecharts.commons.utils import JsCode
    except Exception as e:
        raise RuntimeError(f"pyecharts 未安装或导入失败：{e}")

    r_d = _infer_daily_return_for_calendar_df(df)
    r_pct = (r_d * 100.0).round(3)
    c_val = _color_transform_for_calendar(r_pct, method=color_method, k=color_k)

    years = sorted(pd.DatetimeIndex(r_pct.index).year.unique().tolist())
    vmax_t = float(np.nanmax(np.abs(c_val.values))) if len(c_val) else 0.0
    if not np.isfinite(vmax_t) or vmax_t <= 0:
        vmax_t = 1.0

    page = Page(layout=Page.SimplePageLayout)

    chart_ids = []

    for y in years:
        data_year = [{
            "name": f"{d.strftime('%Y-%m-%d')}：{float(r_pct.loc[d]):.2f}%",
            "value": [d.strftime("%Y-%m-%d"), float(c_val.loc[d]), float(r_pct.loc[d])]
        } for d in r_pct.index if d.year == y]

        if hatch_holiday:
            all_days = pd.date_range(f"{y}-01-01", f"{y}-12-31", freq="D")
            trade_days = set(d.normalize() for d in r_pct.index if d.year == y)
            holidays = [d for d in all_days if d not in trade_days]

            decal_obj = {
                "symbol": "rect",
                "dashArrayX": [1, 0],
                "dashArrayY": [4, 4],
                "rotation": float(hatch_rotation),
                "symbolSize": 0.8,
                "color": f"rgba(0,0,0,{hatch_alpha})",
            }
            for d in holidays:
                ds = d.strftime("%Y-%m-%d")
                data_year.append({
                    "name": f"{ds}：{holiday_label}",
                    "value": [ds, 0.0, None],
                    "itemStyle": {
                        "color": holiday_color,
                        "decal": decal_obj
                    }
                })

        cal = (
            Calendar(init_opts=opts.InitOpts(width="100%", height=f"{height_px}px"))
            .add(
                series_name=str(y),
                yaxis_data=data_year,
                label_opts=opts.LabelOpts(
                    is_show=True,
                    position="inside",
                    font_size=7,
                    color="#333333",
                    margin=0,
                    formatter=JsCode(
                        """
                        function(params) {
                          var raw = params.value && params.value.length > 2 ? params.value[2] : null;
                          if (raw === null || raw === undefined || isNaN(raw)) {
                            return '';
                          }
                          return raw.toFixed(1) + '%';
                        }
                        """
                    ),
                ),
                calendar_opts=opts.CalendarOpts(
                    range_=str(y),
                    pos_top=inner_top_px,
                    pos_bottom=inner_bottom_px,
                    splitline_opts=opts.SplitLineOpts(is_show=True),
                    daylabel_opts=opts.CalendarDayLabelOpts(name_map="cn", first_day=1),
                    monthlabel_opts=opts.CalendarMonthLabelOpts(name_map="cn"),
                ),
            )
            .set_global_opts(
                legend_opts=opts.LegendOpts(is_show=False),
                toolbox_opts=opts.ToolboxOpts(is_show=False),
                visualmap_opts=opts.VisualMapOpts(
                    min_=-vmax_t, max_=vmax_t,
                    range_color=["#00FF00", "#FFFFFF", "#FF0000"],
                    is_show=bool(show_visualmap),
                    orient="horizontal",
                    pos_left="center",
                    pos_top="2%"
                ),
                tooltip_opts=opts.TooltipOpts(
                    formatter=JsCode(
                        """
                        function(params) {
                          var raw = params.value && params.value.length > 2 ? params.value[2] : null;
                          if (raw === null || raw === undefined || isNaN(raw)) {
                            return params.name;
                          }
                          return params.value[0] + '：' + raw.toFixed(2) + '%';
                        }
                        """
                    )
                ),
            )
        )
        page.add(cal)
        chart_ids.append(cal.chart_id)

    # 动态宽高比：按容器宽度设高 = 宽度 * height_ratio
    try:
        import json
        if height_ratio is not None:
            script = (
                "(function(){\n"
                "  var ratio = " + str(height_ratio) + ";\n"
                "  var ids = " + json.dumps(chart_ids) + ";\n"
                "  function adjust(){\n"
                "    ids.forEach(function(id){\n"
                "      var el = document.getElementById(id);\n"
                "      if(!el) return;\n"
                "      var w = el.clientWidth || (el.parentElement ? el.parentElement.clientWidth : 0) || document.documentElement.clientWidth;\n"
                "      var h = Math.max(100, Math.round(w * ratio));\n"
                "      el.style.width = '100%';\n"
                "      el.style.height = h + 'px';\n"
                "      var inst = echarts.getInstanceByDom(el);\n"
                "      if(inst){ inst.resize(); }\n"
                "    });\n"
                "  }\n"
                "  if (document.readyState === 'complete') { adjust(); } else { window.addEventListener('load', adjust); }\n"
                "  window.addEventListener('resize', adjust);\n"
                "})();"
            )
            page.add_js_funcs(script)
    except Exception:
        pass

    return page

# —— 将日历图嵌入到 HTML section
def build_calendar_section_html_from_df(
    df: pd.DataFrame,
    title_prefix: str = None,
    color_method: str = "log",
    color_k: float = 0.5,
    show_visualmap: bool = False,
    height_px: int = 160,
    height_ratio: float = 0.13,
    inner_top_px: int = 2,
    inner_bottom_px: int = 2,
    hatch_holiday: bool = True,
    holiday_label: str = "休市",
    holiday_color: str = "#FFFFFF",
    hatch_rotation: float = 0.785398,
    hatch_alpha: float = 0.1,
) -> str:
    page = build_daily_return_calendar_page_from_df(
        df,
        title_prefix=title_prefix,
        color_method=color_method,
        color_k=color_k,
        show_visualmap=show_visualmap,
        height_px=height_px,
        height_ratio=height_ratio,
        inner_top_px=inner_top_px,
        inner_bottom_px=inner_bottom_px,
        hatch_holiday=hatch_holiday,
        holiday_label=holiday_label,
        holiday_color=holiday_color,
        hatch_rotation=hatch_rotation,
        hatch_alpha=hatch_alpha,
    )
    # 内嵌 HTML 片段
    embed_html = page.render_embed()
    title_text = (title_prefix + " ") if title_prefix else ""
    return f"<section class='section'><h2>{title_text}日收益日历</h2>" + embed_html + "</section>"

# —— 输出日历图为 PNG（需要 snapshot 依赖）
def export_daily_return_calendar_png(
    results: Dict[str, Any],
    img_path: str,
    title_prefix: str = None,
    color_method: str = "log",
    color_k: float = 0.5,
    show_visualmap: bool = False,
    height_px: int = 160,
    height_ratio: float = 0.13,
    inner_top_px: int = 2,
    inner_bottom_px: int = 2,
    hatch_holiday: bool = True,
    holiday_label: str = "休市",
    holiday_color: str = "#FFFFFF",
    hatch_rotation: float = 0.785398,
    hatch_alpha: float = 0.1,
) -> None:
    import os
    df = results.get('daily_records')
    if df is None or getattr(df, 'empty', False):
        print("无法导出日历图：daily_records 为空")
        return
    page = build_daily_return_calendar_page_from_df(
        df,
        title_prefix=title_prefix,
        color_method=color_method,
        color_k=color_k,
        show_visualmap=show_visualmap,
        height_px=height_px,
        height_ratio=height_ratio,
        inner_top_px=inner_top_px,
        inner_bottom_px=inner_bottom_px,
        hatch_holiday=hatch_holiday,
        holiday_label=holiday_label,
        holiday_color=holiday_color,
        hatch_rotation=hatch_rotation,
        hatch_alpha=hatch_alpha,
    )
    # 选择 snapshot 引擎
    try:
        from pyecharts.render import make_snapshot
        try:
            from snapshot_selenium import snapshot as _snapshot
            # Ensure chromedriver is available on PATH for Selenium
            try:
                import chromedriver_autoinstaller, os
                _cd_path = chromedriver_autoinstaller.install()
                if _cd_path:
                    os.environ['PATH'] = os.path.dirname(_cd_path) + os.pathsep + os.environ.get('PATH', '')
            except Exception:
                pass
        except Exception:
            try:
                from snapshot_phantomjs import snapshot as _snapshot
            except Exception:
                _snapshot = None
        if _snapshot is None:
            raise RuntimeError("未找到 snapshot_selenium 或 snapshot_phantomjs，请安装以导出PNG")
        # 先渲染到临时HTML，再截图成PNG
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix='.html', delete=False)
        tmp_path = tmp.name
        tmp.close()
        page.render(tmp_path)
        # 使用 Selenium 全页截图，保证多年份完整输出
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            # 尝试自动安装并配置 chromedriver
            try:
                import chromedriver_autoinstaller
                chromedriver_autoinstaller.install()
            except Exception:
                pass
            options = Options()
            options.add_argument('--headless=new')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            driver = webdriver.Chrome(options=options)
            driver.get('file://' + tmp_path)
            import time
            time.sleep(1.0)
            # 触发一次 resize，确保比例脚本生效
            driver.execute_script('window.dispatchEvent(new Event("resize"));')
            import time, base64
            time.sleep(0.6)
            # 计算整页尺寸并截图（使用 DevTools 全页截图）
            total_height = driver.execute_script('return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);')
            total_width = driver.execute_script('return Math.max(document.body.scrollWidth, document.documentElement.scrollWidth);')
            # 避免过窄截图并设置最小高度
            if not isinstance(total_width, int) or total_width < 1280:
                total_width = 1280
            if not isinstance(total_height, int) or total_height < 600:
                total_height = 600
            # 依据年份数量估算需要的总高度，防止被截断
            try:
                years_count = len(pd.DatetimeIndex(df.index).year.unique())
            except Exception:
                years_count = 1
            expected_total_height = int(max(total_height, total_width * float(height_ratio or 0.13) * max(years_count,1) + (inner_top_px + inner_bottom_px) * max(years_count,1) + 80))
            driver.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
                "width": int(total_width),
                "height": int(expected_total_height),
                "deviceScaleFactor": 1,
                "mobile": False
            })
            # 同步设置窗口大小，确保截图区域足够高
            try:
                driver.set_window_size(int(total_width), int(expected_total_height))
            except Exception:
                pass
            png = driver.execute_cdp_cmd("Page.captureScreenshot", {
                "format": "png",
                "captureBeyondViewport": True,
                "fromSurface": True,
                "clip": {
                    "x": 0,
                    "y": 0,
                    "width": int(total_width),
                    "height": int(expected_total_height),
                    "scale": 1
                }
            })
            with open(img_path, "wb") as f:
                f.write(base64.b64decode(png.get("data", "")))
            driver.quit()
        except Exception:
            # 回退到 pyecharts 官方 snapshot 实现
            make_snapshot(_snapshot, tmp_path, img_path)
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        print(f"日历图PNG已保存至: {img_path}")
    except Exception as e:
        # 写出可嵌入的HTML作为兜底
        out_dir = os.path.dirname(os.path.abspath(img_path))
        html_fallback = os.path.join(out_dir, 'calendar.html')
        page.render(html_fallback)
        print(f"PNG导出失败({e})，已生成HTML: {html_fallback}。请安装 snapshot_selenium 或 snapshot_phantomjs 以导出PNG。")


__all__ = [
    'plot_results', 'plot_positions', 'calculate_metrics',
    'print_metrics', 'export_trades', 'generate_report',
    'generate_html_report', 'load_results_from_directory', 'rebuild_report_from_directory',
    'export_metrics_json',
    'export_annual_returns', 'export_monthly_returns_heatmap', 'export_open_counts', 'export_instrument_pnl',
    'export_daily_return_calendar_png', 'build_calendar_section_html_from_df'
]
