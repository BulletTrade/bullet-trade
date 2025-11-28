"""
参数并行优化器

提供在多CPU上并行运行参数组合的回测，并将每个组合的风险指标导出到CSV。
该过程不生成报告文件（不调用 generate_report），仅返回与保存CSV。
"""
from typing import Dict, Any, List, Optional, Iterable, Tuple
import itertools
import os
import signal
import sys
import time
import traceback
import json
import multiprocessing as mp
import pandas as pd
import numpy as np


def _worker_init():
    """
    子进程初始化函数：
    1. 让子进程忽略 SIGINT 信号，由主进程统一处理中断
    2. 禁用文件日志，避免多进程同时写入同一个日志文件导致冲突
    """
    # Windows 上 signal.SIGINT 可能不存在或行为不同，需要容错处理
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except (AttributeError, ValueError, OSError):
        pass
    
    # 禁用子进程的文件日志，避免多进程同时写入/轮转同一个日志文件
    # 这是多进程优化时的常见问题，尤其在 Windows 上会导致 PermissionError
    try:
        from .globals import log
        if log._file_handler:
            try:
                log.logger.removeHandler(log._file_handler)
            except Exception:
                pass
            try:
                log._file_handler.close()
            except Exception:
                pass
            log._file_handler = None
    except Exception:
        pass

# 进度条（可选）
try:
    from tqdm import tqdm
except Exception:
    tqdm = None

from .engine import create_backtest
from .analysis import calculate_metrics


def _expand_param_grid(param_grid: Dict[str, Iterable]) -> List[Dict[str, Any]]:
    """将参数网格展开为所有组合列表。"""
    if not param_grid:
        return [{}]
    keys = list(param_grid.keys())
    values = [list(v) for v in param_grid.values()]
    combos = []
    for vs in itertools.product(*values):
        combos.append({k: v for k, v in zip(keys, vs)})
    return combos


def _worker_task(task: Dict[str, Any]) -> Dict[str, Any]:
    """子进程任务：运行一次回测并计算指标。"""
    strategy_file = task['strategy_file']
    start_date = task['start_date']
    end_date = task['end_date']
    frequency = task.get('frequency', 'day')
    initial_cash = float(task.get('initial_cash', 1000000))
    benchmark = task.get('benchmark')
    initial_positions = task.get('initial_positions')
    extras_base = task.get('extras_base') or {}
    combo_params = task.get('combo_params') or {}
    algorithm_id = task.get('algorithm_id')

    # 组合extras：基础extras + 组合参数
    extras = dict(extras_base)
    extras.update(combo_params)

    # 静默策略输出（可选）
    try:
        if task.get('quiet'):
            from .globals import log
            level = str(task.get('quiet_level') or 'error')
            # 同时设置策略与系统日志级别，尽量减少输出
            log.set_level('strategy', level)
            log.set_level('system', level)
    except Exception:
        pass

    start_ts = time.time()
    row: Dict[str, Any] = {}
    try:
        # 运行回测（不写任何日志文件）
        results = create_backtest(
            strategy_file=strategy_file,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            initial_cash=initial_cash,
            benchmark=benchmark,
            log_file=None,
            extras=extras,
            initial_positions=initial_positions,
            algorithm_id=algorithm_id,
        )
        # 计算风险指标
        metrics = calculate_metrics(results)

        # 行数据：参数与指标合并
        row.update(combo_params)
        row.update(metrics)
        # 额外衍生指标：收益回撤比（等同Calmar比率）
        try:
            calmar = float(metrics.get('Calmar比率', 0.0))
        except Exception:
            # 兜底：自行计算
            df = results['daily_records']
            total = float(df['total_value'].iloc[-1]) / float(df['total_value'].iloc[0])
            trading_days = len(df)
            years = trading_days / 250.0 if trading_days else 0.0
            annual = (pow(total, 1/years) - 1) * 100 if years > 0 else 0.0
            cummax = df['total_value'].expanding().max()
            drawdown = (df['total_value'] - cummax) / cummax * 100
            mdd = float(drawdown.min()) if len(drawdown) else -1.0
            calmar = annual / abs(mdd) if mdd < 0 else 0.0
        row['收益回撤比'] = calmar

        # 基本元信息
        meta = results.get('meta', {})
        row['算法ID'] = meta.get('algorithm_id') or algorithm_id or ''
        row['策略文件'] = meta.get('strategy_file') or strategy_file
        row['起始日期'] = meta.get('start_date') or start_date
        row['结束日期'] = meta.get('end_date') or end_date
        row['频率'] = frequency
        row['初始资金'] = float(results['daily_records']['total_value'].iloc[0]) if len(results['daily_records']) else initial_cash

        # 运行耗时
        row['耗时秒'] = round(time.time() - start_ts, 3)
        row['错误'] = ''
    except Exception as e:
        # 错误行：标记错误并写入组合参数
        row.update(combo_params)
        row['错误'] = f"{type(e).__name__}: {e}"
        row['耗时秒'] = round(time.time() - start_ts, 3)
        row['算法ID'] = algorithm_id or ''
        row['策略文件'] = strategy_file
        row['起始日期'] = start_date
        row['结束日期'] = end_date
        row['频率'] = frequency
        row['初始资金'] = initial_cash
    return row


def run_param_grid(
    strategy_file: str,
    start_date: str,
    end_date: str,
    frequency: str = 'day',
    initial_cash: float = 1000000,
    initial_positions: Optional[List[Dict[str, Any]]] = None,
    extras_base: Optional[Dict[str, Any]] = None,
    param_grid: Optional[Dict[str, Iterable]] = None,
    processes: Optional[int] = None,
    benchmark: Optional[str] = None,
    algorithm_id: Optional[str] = None,
    output_csv: Optional[str] = None,
    sort_by: str = '收益回撤比',
    top_n: int = 10,
    quiet: bool = False,
    quiet_level: str = 'error',
    show_progress: bool = True,
    progress_desc: Optional[str] = None,
) -> pd.DataFrame:
    """
    并行运行参数组合回测并返回/保存指标CSV。

    Args:
        strategy_file: 策略文件路径
        start_date: 回测开始日期 'YYYY-MM-DD'
        end_date: 回测结束日期 'YYYY-MM-DD'
        frequency: 回测频率 ('day' 或 'minute')
        initial_cash: 初始资金（现金）
        initial_positions: 初始持仓列表
        extras_base: 基础extras（常量参数）
        param_grid: 参数网格（字典，值为可迭代），如 {'mt_days': [22, 23, 24], 'ma_days': range(33,55)}
        processes: 并行进程数，默认CPU核心数
        benchmark: 基准标的（可选）
        algorithm_id: 算法ID（可选）
        output_csv: 保存CSV路径（可选，不传则不保存）
        sort_by: 排序指标，默认按 '收益回撤比'（Calmar比率）降序
        top_n: 控制台输出的Top建议数量（默认10）

    Returns:
        包含参数与风险指标的DataFrame（已按 sort_by 排序）
    """
    combos = _expand_param_grid(param_grid or {})
    if processes is None or processes <= 0:
        try:
            processes = os.cpu_count() or 1
        except Exception:
            processes = 1
    
    # 多进程模式下打印警告：子进程文件日志已禁用
    if processes > 1:
        # 使用 ANSI 颜色代码：黄色警告
        YELLOW = "\033[33m"
        RESET = "\033[0m"
        print(f"{YELLOW}⚠️  多进程优化模式：子进程文件日志已禁用，避免日志文件冲突{RESET}")
        print(f"{YELLOW}   如需调试单个参数组合，请使用单次回测命令{RESET}")
        print()

    tasks = []
    for combo in combos:
        tasks.append({
            'strategy_file': strategy_file,
            'start_date': start_date,
            'end_date': end_date,
            'frequency': frequency,
            'initial_cash': initial_cash,
            'benchmark': benchmark,
            'initial_positions': initial_positions,
            'extras_base': extras_base or {},
            'combo_params': combo,
            'algorithm_id': algorithm_id,
            'quiet': quiet,
            'quiet_level': quiet_level,
        })

    rows: List[Dict[str, Any]] = []
    interrupted = False
    
    if processes == 1:
        # 单进程模式：直接处理中断
        iterator = map(_worker_task, tasks)
        try:
            if show_progress and tqdm:
                for r in tqdm(iterator, total=len(tasks), desc=progress_desc or '参数优化', unit='组'):
                    rows.append(r)
            else:
                for r in iterator:
                    rows.append(r)
        except KeyboardInterrupt:
            interrupted = True
            print("\n\n⚠️ 用户中断优化，已完成 {}/{} 组合".format(len(rows), len(tasks)))
    else:
        # 多进程模式：需要手动管理 Pool 以正确处理中断
        # 注意：Windows 上 imap_unordered 迭代器阻塞时无法响应 Ctrl+C
        # 因此使用 apply_async + 带超时的 get() 轮询，让主进程能定期检查中断信号
        pool = None
        try:
            # 使用 initializer 让子进程忽略 SIGINT，由主进程统一处理
            pool = mp.Pool(processes=processes, initializer=_worker_init)
            
            # 使用 apply_async 提交所有任务，返回 AsyncResult 列表
            async_results = [pool.apply_async(_worker_task, (task,)) for task in tasks]
            
            # 关闭 pool，不再接受新任务（但已提交的任务继续执行）
            pool.close()
            
            # 带超时轮询等待结果，这样主进程能响应 Ctrl+C
            total = len(async_results)
            if show_progress and tqdm:
                pbar = tqdm(total=total, desc=progress_desc or '参数优化', unit='组')
                for ar in async_results:
                    # 使用带超时的 get()，每 0.5 秒检查一次，让主进程能响应中断
                    while True:
                        try:
                            r = ar.get(timeout=0.5)
                            break
                        except mp.TimeoutError:
                            continue  # 超时但任务未完成，继续等待
                    rows.append(r)
                    pbar.update(1)
                pbar.close()
            elif show_progress and not tqdm:
                done = 0
                last_emit = time.time()
                emit_interval = 1.0
                for ar in async_results:
                    while True:
                        try:
                            r = ar.get(timeout=0.5)
                            break
                        except mp.TimeoutError:
                            continue
                    rows.append(r)
                    done += 1
                    now = time.time()
                    if now - last_emit >= emit_interval or done == total:
                        pct = (done / total) * 100
                        print(f"进度: {done}/{total} ({pct:.1f}%)")
                        last_emit = now
            else:
                for ar in async_results:
                    while True:
                        try:
                            r = ar.get(timeout=0.5)
                            break
                        except mp.TimeoutError:
                            continue
                    rows.append(r)
        except KeyboardInterrupt:
            interrupted = True
            print("\n\n⚠️ 用户中断优化，正在停止子进程...")
            if pool:
                pool.terminate()  # 强制终止所有子进程
            print("已完成 {}/{} 组合".format(len(rows), len(tasks)))
        finally:
            if pool:
                pool.join()  # 等待所有子进程结束

    df = pd.DataFrame(rows)
    # 排序（降序）
    if sort_by in df.columns:
        df = df.sort_values(by=sort_by, ascending=False)
    elif 'Calmar比率' in df.columns:
        df = df.sort_values(by='Calmar比率', ascending=False)

    # 保存CSV（如需）
    if output_csv:
        try:
            os.makedirs(os.path.dirname(output_csv) or '.', exist_ok=True)
            df.to_csv(output_csv, index=False, encoding='utf-8-sig')
            print(f"参数网格风险指标已保存：{output_csv}")
        except Exception as e:
            print(f"保存CSV失败：{e}")

    # 控制台输出Top建议
    try:
        print("\n参数优化Top建议（按收益回撤比降序）：")
        cols = [c for c in ['收益回撤比', 'Calmar比率', '策略年化收益', '最大回撤', '胜率', '夏普比率'] if c in df.columns]
        preview_cols = list(df.columns)
        # 让参数列先展示
        param_cols = list((param_grid or {}).keys())
        ordered_cols = param_cols + [c for c in preview_cols if c not in param_cols]
        df_preview = df[ordered_cols]
        print(df_preview.head(top_n).to_string(index=False))
    except Exception:
        pass

    return df


def generate_param_search_report(
    results: Optional[pd.DataFrame] = None,
    output_file: Optional[str] = None,
    results_csv: Optional[str] = None,
    top_n: int = 20,
    sort_by: str = '收益回撤比',
    param_columns: Optional[List[str]] = None,
    title: Optional[str] = None,
    digits: int = 2,
) -> str:
    """
    生成参数优化结果的交互式HTML报告（Plotly）。

    使用方式：
    - 传入优化结果DataFrame（或CSV路径）与输出文件路径，即可生成报告。

    Args:
        results: 参数优化结果DataFrame（可选，与 results_csv 二选一）
        output_file: 输出HTML文件路径（可选，不传则与CSV同目录写入默认文件名）
        results_csv: 参数优化CSV路径（可选，与 results 二选一）
        top_n: Top结果表展示条数（默认20）
        sort_by: 排序指标（默认 '收益回撤比'）
        param_columns: 参数列（可选，不传则自动推断）
        title: 报告标题（可选）

    Returns:
        生成的HTML文件路径
    """
    # 延迟导入，避免非报告路径下的plotly硬依赖
    try:
        import plotly.graph_objs as go
        import plotly.io as pio
        import plotly.subplots as sp
    except Exception as e:
        raise RuntimeError(f"需要安装 plotly 才能生成报告：{e}")

    if results is None and not results_csv:
        raise ValueError("必须传入 results 或 results_csv 其中之一。")

    if results is not None:
        df = results.copy()
    else:
        df = pd.read_csv(results_csv, encoding='utf-8-sig')

    # 过滤错误行
    if '错误' in df.columns:
        df = df[(df['错误'].isna()) | (df['错误'] == '')]

    # 数值列转为数值（容错百分号等）
    def _num(col: str) -> Optional[str]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            return col
        return None

    def _fmt_val(v: Any) -> str:
        if pd.isna(v):
            return ''
        try:
            if isinstance(v, (int, np.integer)):
                return f"{int(v)}"
            if isinstance(v, (float, np.floating)):
                return f"{float(v):.{digits}f}"
        except Exception:
            pass
        return str(v)

    # 主评分列（优先使用sort_by/收益回撤比/Calmar比率）
    score_col = None
    for c in [sort_by, '收益回撤比', 'Calmar比率']:
        if c in df.columns:
            score_col = c
            _num(c)
            break

    # 年化和回撤列推断
    annual_candidates = ['年化收益率%', '策略年化收益', '年化收益']
    mdd_candidates = ['最大回撤%', '最大回撤']
    annual_col = next((c for c in annual_candidates if c in df.columns), None)
    mdd_col = next((c for c in mdd_candidates if c in df.columns), None)
    if annual_col:
        _num(annual_col)
    if mdd_col:
        _num(mdd_col)

    # 若无评分列，尝试用 年化/最大回撤 估算
    if not score_col and annual_col and mdd_col:
        score_col = '收益回撤比(估算)'
        df[score_col] = (df[annual_col] / df[mdd_col].abs()).replace([np.inf, -np.inf], np.nan)

    if not score_col:
        # 兜底：选择第一列的数值列作为排序（极端场景）
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        score_col = num_cols[0] if num_cols else df.columns[0]

    # 推断参数列（排除明显的指标与元信息）
    if not param_columns:
        meta_cols = {'算法ID', '策略文件', '起始日期', '结束日期', '频率', '初始资金', '耗时秒', '错误'}
        metric_keywords = ['收益', '回撤', '夏普', '胜率', '波动', 'Calmar', '卡玛']
        param_columns = [
            c for c in df.columns
            if c not in meta_cols and not any(k in c for k in metric_keywords)
        ]
    param_columns = list(param_columns or [])

    # 排序与Top
    df_sorted = df.sort_values(by=score_col, ascending=False)
    df_top = df_sorted.head(top_n)

    # 图1：收益回撤比分布
    fig_hist = go.Figure()
    fig_hist.add_trace(go.Histogram(x=df_sorted[score_col], nbinsx=40, marker_color='#3b82f6'))
    fig_hist.update_layout(title=f"{title or '参数优化'} — {score_col}分布", xaxis_title=score_col, yaxis_title='数量')
    fig_hist.update_traces(hovertemplate=f"{score_col}=%{{x:.{digits}f}}<br>数量=%{{y}}<extra></extra>")
    fig_hist.update_xaxes(tickformat=f".{digits}f")

    # 可选：索提诺比率分布
    sortino_candidates = ['索提诺比率', 'Sortino比率', 'Sortino Ratio', 'Sortino']
    sortino_col = next((c for c in sortino_candidates if c in df.columns), None)
    if sortino_col:
        _num(sortino_col)
        fig_sortino = go.Figure()
        fig_sortino.add_trace(go.Histogram(x=df_sorted[sortino_col], nbinsx=40, marker_color='#8b5cf6'))
        fig_sortino.update_layout(title=f"{title or '参数优化'} — {sortino_col}分布", xaxis_title=sortino_col, yaxis_title='数量')
        fig_sortino.update_traces(hovertemplate=f"{sortino_col}=%{{x:.{digits}f}}<br>数量=%{{y}}<extra></extra>")
        fig_sortino.update_xaxes(tickformat=f".{digits}f")

    # 图2：年化 vs 最大回撤散点（按评分着色）
    if annual_col and mdd_col:
        fig_scatter = go.Figure()
        fig_scatter.add_trace(go.Scatter(
            x=df_sorted[annual_col],
            y=df_sorted[mdd_col].abs(),
            mode='markers',
            marker=dict(
                size=8,
                color=df_sorted[score_col],
                colorscale='Viridis',
                showscale=True,
                colorbar=dict(title=score_col)
            ),
            text=[" | ".join([f"{c}={str(df_sorted.iloc[i][c])}" for c in param_columns[:6]]) for i in range(len(df_sorted))],
            hovertemplate=f"{annual_col}=%{{x:.{digits}f}}<br>{mdd_col}=%{{y:.{digits}f}}<br>{score_col}=%{{marker.color:.{digits}f}}<br>%{{text}}<extra></extra>",
        ))
        fig_scatter.update_layout(title=f"{title or '参数优化'} — 年化 vs 最大回撤")
        fig_scatter.update_xaxes(tickformat=f".{digits}f")
        fig_scatter.update_yaxes(tickformat=f".{digits}f")
    else:
        fig_scatter = None

    # 图3：Top-N参数与指标表
    table_cols = param_columns[:6] + [c for c in [score_col, annual_col, mdd_col, '夏普比率'] if c and c in df_top.columns]
    fig_table = go.Figure(data=[go.Table(
        header=dict(values=table_cols, fill_color='#f3f4f6', font=dict(color='#111827'), align='center'),
        cells=dict(values=[[ _fmt_val(v) for v in df_top[c].tolist() ] for c in table_cols], fill_color='#ffffff', font=dict(color='#111827'), align='center')
    )])
    fig_table.update_layout(title=f"Top-{top_n} 组合（按 {score_col} 排序）")

    # 图4：参数边际效应（最多展示4个参数）
    show_params = param_columns[:4]
    if show_params:
        rows = len(show_params)
        fig_param = sp.make_subplots(rows=rows, cols=1, subplot_titles=[f"{p} → {score_col}均值" for p in show_params])
        for i, p in enumerate(show_params, start=1):
            g = df.groupby(p)[score_col].mean().sort_values(ascending=False)
            fig_param.add_trace(go.Bar(x=g.index.astype(str), y=g.values, marker_color='#10b981', hovertemplate=f"{p}=%{{x}}<br>{score_col}=%{{y:.{digits}f}}<extra></extra>"), row=i, col=1)
            fig_param.update_xaxes(title_text=p, row=i, col=1)
            fig_param.update_yaxes(title_text=f"{score_col}均值", tickformat=f".{digits}f", row=i, col=1)
        fig_param.update_layout(title=f"参数边际效应（Top {len(show_params)}）")
    else:
        fig_param = None

    # 组装HTML
    parts = []
    head = f"<h1 style='font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, Helvetica, Arial; margin:8px 0'>{title or '参数优化分析报告'}</h1>"
    parts.append(head)
    parts.append(pio.to_html(fig_hist, include_plotlyjs='cdn', full_html=False))
    if 'sortino_col' in locals() and sortino_col:
        parts.append(pio.to_html(fig_sortino, include_plotlyjs=False, full_html=False))
    if fig_scatter:
        parts.append(pio.to_html(fig_scatter, include_plotlyjs=False, full_html=False))
    parts.append(pio.to_html(fig_table, include_plotlyjs=False, full_html=False))
    if fig_param:
        parts.append(pio.to_html(fig_param, include_plotlyjs=False, full_html=False))

    html = "\n".join(parts)

    if not output_file:
        base_dir = os.path.dirname(results_csv) if results_csv else '.'
        output_file = os.path.join(base_dir or '.', 'param_search_report.html')

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)

    return output_file


__all__ = ['run_param_grid', 'generate_param_search_report']