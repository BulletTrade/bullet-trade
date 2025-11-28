"""
参数优化命令测试

测试 Python 表达式参数语法等功能
"""

import pytest

from bullet_trade.cli.optimize import (
    _expand_py_expression,
    _process_param_grid,
)


class TestExpandPyExpression:
    """测试 _expand_py_expression 函数"""

    def test_plain_list_unchanged(self):
        """普通列表应直接返回"""
        result = _expand_py_expression([1, 2, 3])
        assert result == [1, 2, 3]

    def test_plain_string_unchanged(self):
        """普通字符串（不以 py: 开头）应直接返回"""
        result = _expand_py_expression("some_value")
        assert result == "some_value"

    def test_range_expression(self):
        """测试 range() 表达式"""
        result = _expand_py_expression("py:range(1, 5)")
        assert result == [1, 2, 3, 4]

    def test_range_with_step(self):
        """测试带步长的 range() 表达式"""
        result = _expand_py_expression("py:range(10, 35, 5)")
        assert result == [10, 15, 20, 25, 30]

    def test_list_comprehension(self):
        """测试列表推导式"""
        result = _expand_py_expression("py:[x*2 for x in range(1, 5)]")
        assert result == [2, 4, 6, 8]

    def test_float_division(self):
        """测试浮点数除法"""
        result = _expand_py_expression("py:[x/10 for x in range(1, 5)]")
        assert result == [0.1, 0.2, 0.3, 0.4]

    def test_round_function(self):
        """测试 round() 函数"""
        result = _expand_py_expression("py:[round(x*0.01, 2) for x in range(1, 4)]")
        assert result == [0.01, 0.02, 0.03]

    def test_list_wrapper(self):
        """测试 list() 包装"""
        result = _expand_py_expression("py:list(range(3))")
        assert result == [0, 1, 2]

    def test_with_whitespace(self):
        """测试带空格的表达式"""
        result = _expand_py_expression("py:  range(1, 4)  ")
        assert result == [1, 2, 3]

    def test_abs_function(self):
        """测试 abs() 函数"""
        result = _expand_py_expression("py:[abs(x) for x in [-1, -2, 3]]")
        assert result == [1, 2, 3]

    def test_min_max_function(self):
        """测试 min/max 函数"""
        result = _expand_py_expression("py:[min(x, 5) for x in range(1, 10, 2)]")
        assert result == [1, 3, 5, 5, 5]

    def test_sorted_function(self):
        """测试 sorted() 函数"""
        result = _expand_py_expression("py:sorted([3, 1, 2])")
        assert result == [1, 2, 3]

    def test_invalid_expression_raises(self):
        """无效表达式应抛出 ValueError"""
        with pytest.raises(ValueError, match="Python 表达式求值失败"):
            _expand_py_expression("py:invalid_func()")

    def test_syntax_error_raises(self):
        """语法错误应抛出 ValueError"""
        with pytest.raises(ValueError, match="Python 表达式求值失败"):
            _expand_py_expression("py:[x for x in")

    def test_dangerous_builtin_blocked(self):
        """危险的内置函数应被阻止"""
        # __import__ 被阻止
        with pytest.raises(ValueError):
            _expand_py_expression("py:__import__('os')")

    def test_open_blocked(self):
        """open() 函数应被阻止"""
        with pytest.raises(ValueError):
            _expand_py_expression("py:list(open('test.txt'))")

    def test_eval_blocked(self):
        """eval() 函数应被阻止"""
        with pytest.raises(ValueError):
            _expand_py_expression("py:eval('1+1')")


class TestProcessParamGrid:
    """测试 _process_param_grid 函数"""

    def test_plain_values_unchanged(self):
        """普通列表值应保持不变"""
        param_grid = {
            "param1": [1, 2, 3],
            "param2": [0.1, 0.2],
        }
        result = _process_param_grid(param_grid)
        assert result == param_grid

    def test_mixed_plain_and_expression(self):
        """混合普通值和表达式"""
        param_grid = {
            "param1": [1, 2, 3],
            "param2": "py:range(10, 15)",
        }
        result = _process_param_grid(param_grid)
        assert result["param1"] == [1, 2, 3]
        assert result["param2"] == [10, 11, 12, 13, 14]

    def test_all_expressions(self):
        """全部使用表达式"""
        param_grid = {
            "rank_days": "py:range(10, 35, 5)",
            "threshold": "py:[x/100 for x in range(1, 4)]",
        }
        result = _process_param_grid(param_grid)
        assert result["rank_days"] == [10, 15, 20, 25, 30]
        assert result["threshold"] == [0.01, 0.02, 0.03]

    def test_empty_grid(self):
        """空参数网格"""
        result = _process_param_grid({})
        assert result == {}

    def test_invalid_expression_raises(self):
        """无效表达式应抛出 ValueError"""
        param_grid = {
            "param1": "py:invalid()",
        }
        with pytest.raises(ValueError):
            _process_param_grid(param_grid)


class TestPyExpressionEdgeCases:
    """测试边界情况"""

    def test_empty_range(self):
        """空 range"""
        result = _expand_py_expression("py:range(5, 5)")
        assert result == []

    def test_negative_range(self):
        """负数 range"""
        result = _expand_py_expression("py:range(-5, 0)")
        assert result == [-5, -4, -3, -2, -1]

    def test_float_list_comprehension(self):
        """浮点数列表推导式"""
        result = _expand_py_expression("py:[x/10.0 for x in range(1, 4)]")
        assert result == [0.1, 0.2, 0.3]

    def test_nested_comprehension(self):
        """嵌套函数调用"""
        result = _expand_py_expression("py:sorted([3, 1, 2], reverse=True)")
        assert result == [3, 2, 1]

    def test_filter_usage(self):
        """测试 filter 函数"""
        result = _expand_py_expression("py:list(filter(lambda x: x > 2, [1, 2, 3, 4]))")
        assert result == [3, 4]

    def test_map_usage(self):
        """测试 map 函数"""
        result = _expand_py_expression("py:list(map(lambda x: x*2, [1, 2, 3]))")
        assert result == [2, 4, 6]

    def test_zip_usage(self):
        """测试 zip 函数"""
        result = _expand_py_expression("py:list(zip([1, 2], [3, 4]))")
        assert result == [(1, 3), (2, 4)]

    def test_sum_in_comprehension(self):
        """测试 sum 在推导式中"""
        # 虽然不常用，但应该能工作
        result = _expand_py_expression("py:[sum(range(x)) for x in range(1, 5)]")
        assert result == [0, 1, 3, 6]  # sum(range(1))=0, sum(range(2))=1, sum(range(3))=3, sum(range(4))=6


class TestRealWorldExamples:
    """真实场景测试"""

    def test_momentum_strategy_params(self):
        """动量策略参数"""
        param_grid = {
            "rank_days": "py:range(10, 35, 5)",
            "buy_signal_days": "py:range(3, 8)",
            "stop_signal_days": "py:range(2, 6)",
        }
        result = _process_param_grid(param_grid)
        
        assert result["rank_days"] == [10, 15, 20, 25, 30]
        assert result["buy_signal_days"] == [3, 4, 5, 6, 7]
        assert result["stop_signal_days"] == [2, 3, 4, 5]
        
        # 验证组合数
        total = len(result["rank_days"]) * len(result["buy_signal_days"]) * len(result["stop_signal_days"])
        assert total == 5 * 5 * 4  # 100 种组合

    def test_threshold_params(self):
        """阈值参数（需要小数）"""
        param_grid = {
            "buy_threshold": "py:[round(x*0.01, 2) for x in range(1, 6)]",
            "sell_threshold": "py:[round(x*0.01, 2) for x in range(1, 6)]",
        }
        result = _process_param_grid(param_grid)
        
        assert result["buy_threshold"] == [0.01, 0.02, 0.03, 0.04, 0.05]
        assert result["sell_threshold"] == [0.01, 0.02, 0.03, 0.04, 0.05]

    def test_percentage_params(self):
        """百分比参数"""
        param_grid = {
            "stop_loss_pct": "py:[x/100 for x in range(5, 15, 2)]",
        }
        result = _process_param_grid(param_grid)
        
        expected = [0.05, 0.07, 0.09, 0.11, 0.13]
        assert result["stop_loss_pct"] == expected

