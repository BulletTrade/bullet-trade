"""
参数优化命令处理

运行参数优化
"""

import json
from pathlib import Path
from typing import Any, Dict, List


# Python 表达式求值时允许使用的安全内置函数
_SAFE_BUILTINS = {
    'range': range,
    'list': list,
    'int': int,
    'float': float,
    'round': round,
    'abs': abs,
    'min': min,
    'max': max,
    'sum': sum,
    'len': len,
    'sorted': sorted,
    'reversed': reversed,
    'enumerate': enumerate,
    'zip': zip,
    'map': map,
    'filter': filter,
    # 数学函数（如果需要）
    '__builtins__': {},
}


def _expand_py_expression(value: Any) -> List[Any]:
    """
    展开 Python 表达式参数值。
    
    如果 value 是以 "py:" 开头的字符串，则将其后的内容作为 Python 表达式求值。
    表达式结果必须是可迭代对象，会被转换为列表。
    
    Args:
        value: 参数值，可以是：
            - 普通列表：直接返回
            - "py:range(1,10)" 形式的字符串：执行表达式并返回结果列表
            
    Returns:
        展开后的参数值列表
        
    Examples:
        >>> _expand_py_expression([1, 2, 3])
        [1, 2, 3]
        >>> _expand_py_expression("py:range(1, 5)")
        [1, 2, 3, 4]
        >>> _expand_py_expression("py:[x/10 for x in range(10, 20, 2)]")
        [1.0, 1.2, 1.4, 1.6, 1.8]
    """
    if isinstance(value, str) and value.startswith('py:'):
        expr = value[3:].strip()
        try:
            # 使用受限的内置函数执行表达式
            result = eval(expr, {"__builtins__": _SAFE_BUILTINS}, {})
            # 确保结果是列表
            return list(result)
        except Exception as e:
            raise ValueError(f"Python 表达式求值失败: '{expr}' - {e}")
    return value


def _process_param_grid(param_grid: Dict[str, Any]) -> Dict[str, List[Any]]:
    """
    处理参数网格，展开所有 Python 表达式。
    
    Args:
        param_grid: 原始参数网格配置
        
    Returns:
        处理后的参数网格，所有 "py:" 表达式已展开为列表
    """
    processed = {}
    for key, value in param_grid.items():
        processed[key] = _expand_py_expression(value)
    return processed


def run_optimize(args):
    """
    运行参数优化
    
    Args:
        args: 命令行参数
        
    Returns:
        退出码
    """
    print("=" * 60)
    print("BulletTrade - 参数优化")
    print("=" * 60)
    print()
    
    # 验证文件
    strategy_file = Path(args.strategy_file)
    params_file = Path(args.params)
    
    if not strategy_file.exists():
        print(f"❌ 策略文件不存在: {strategy_file}")
        return 1
    
    if not params_file.exists():
        print(f"❌ 参数文件不存在: {params_file}")
        return 1
    
    # 读取参数配置
    try:
        with open(params_file, 'r', encoding='utf-8') as f:
            param_config = json.load(f)
    except Exception as e:
        print(f"❌ 读取参数文件失败: {e}")
        return 1
    
    # 提取参数网格
    param_grid_raw = param_config.get('param_grid', {})
    if not param_grid_raw:
        print("❌ 参数文件中未找到 'param_grid' 配置")
        return 1
    
    # 处理 Python 表达式（支持 "py:range(1,10)" 等语法）
    try:
        param_grid = _process_param_grid(param_grid_raw)
    except ValueError as e:
        print(f"❌ 参数表达式错误: {e}")
        return 1
    
    # 计算总组合数
    total_combos = 1
    for values in param_grid.values():
        total_combos *= len(values)
    
    print(f"策略文件: {strategy_file}")
    print(f"参数文件: {params_file}")
    print(f"回测区间: {args.start} 至 {args.end}")
    print(f"并行进程: {args.processes or '自动'}")
    print(f"\n参数网格（共 {total_combos} 种组合）:")
    for key, values in param_grid.items():
        # 如果原始值是表达式，显示原始表达式和展开后的值
        raw_value = param_grid_raw.get(key)
        if isinstance(raw_value, str) and raw_value.startswith('py:'):
            # 展开后的值太长则截断显示
            if len(values) > 10:
                display_values = f"{values[:5]} ... {values[-2:]} (共{len(values)}个)"
            else:
                display_values = values
            print(f"  {key}: {raw_value}")
            print(f"       → {display_values}")
        else:
            print(f"  {key}: {values}")
    print()
    
    try:
        # 导入优化器
        from bullet_trade.core.optimizer import run_param_grid
        
        # 运行优化
        print("开始参数优化...")
        results_df = run_param_grid(
            strategy_file=str(strategy_file),
            start_date=args.start,
            end_date=args.end,
            param_grid=param_grid,
            processes=args.processes,
            output_csv=args.output
        )
        
        # 显示最优参数
        print("\n" + "=" * 60)
        if len(results_df) > 0:
            print("优化完成！")
            print("=" * 60)
            print(f"\n结果已保存至: {args.output}")
            print(f"\n前5名参数组合:")
            print(results_df.head().to_string())
        else:
            print("优化未完成（无结果）")
        print("=" * 60)
        
        return 0
    
    except KeyboardInterrupt:
        # 用户按 Ctrl+C 中断
        print("\n" + "=" * 60)
        print("优化已中断")
        print("=" * 60)
        return 130  # 标准的 SIGINT 退出码
        
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
        print("\n提示: 请确保已正确安装 BulletTrade")
        return 1
        
    except Exception as e:
        print(f"❌ 优化失败: {e}")
        import traceback
        traceback.print_exc()
        return 1

