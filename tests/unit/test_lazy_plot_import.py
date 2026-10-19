"""作者：BruceLee。

职责：核验数据接口导入不提前初始化绘图，显式绘图仍保留原接口和Agg后端。
输入：独立解释器、空环境文件及冷缓存；输出：模块加载、实际PNG与兼容断言。
上下游：公共bullet_trade入口、Provider及原分析模块；不调用SDK、网络或交易。
约定：每项只清理本项临时目录/子进程，不以暖缓存或放宽研究时限掩盖字体扫描。
"""

import os
import subprocess
import sys
from pathlib import Path


def isolated(code, directory):
    """输入验证代码和自有目录，返回完成的子进程；隔离env/cwd，失败提供无秘密测试诊断。"""
    directory.mkdir()
    env_file = directory / ".env"
    env_file.touch(mode=0o600)
    environment = {
        key: value
        for key, value in os.environ.items()
        if key in {"PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "LANG", "LC_ALL"}
    }
    environment.update(
        HOME=str(directory),
        USERPROFILE=str(directory),
        BT_ENV_FILE=str(env_file),
        MPLCONFIGDIR=str(directory / "matplotlib"),
        PYTHONPATH=str(Path(__file__).resolve().parents[2]),
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=directory,
        env=environment,
        capture_output=True,
        text=True,
        timeout=40,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def test_data_import_never_attempts_plotting_or_font_scan(tmp_path):
    """输入自有目录，无返回；用导入拦截器证明零绘图尝试，不依赖缓存命中或仅测运行耗时。"""
    result = isolated(
        '''
import importlib.abc
import sys
attempts = []
class BlockPlot(importlib.abc.MetaPathFinder):
    """记录不应出现的绘图库导入；不代替数据Provider。"""
    def find_spec(self, fullname, path=None, target=None):
        """输入模块名/路径/目标，返回空或拒绝；只阻止绘图相关副作用。"""
        if fullname in {'matplotlib.pyplot', 'matplotlib.font_manager', 'bullet_trade.core.analysis'}:
            attempts.append(fullname)
            raise ImportError('data-only import must not initialize plotting')
        return None
sys.meta_path.insert(0, BlockPlot())
import bullet_trade
from bullet_trade.data.providers.base import DataProvider
assert attempts == [], attempts
assert 'matplotlib.pyplot' not in sys.modules
assert 'matplotlib.font_manager' not in sys.modules
assert 'bullet_trade.core.analysis' not in sys.modules
assert 'plt' in dir(bullet_trade)
assert callable(bullet_trade.get_price)
try:
    bullet_trade.not_an_export
except AttributeError:
    pass
else:
    raise AssertionError('unknown attribute should fail')
print('data-only import passed')
''',
        tmp_path / "data",
    )
    assert result.stdout.strip() == "data-only import passed"
    assert not list((tmp_path / "data" / "matplotlib").glob("fontlist-*.json"))


def test_explicit_pyplot_and_analysis_keep_public_identity_and_real_png(tmp_path):
    """输入自有目录，无返回；实际显式加载/绘图验证原导出、缓存对象和非交互后端。"""
    result = isolated(
        """
import sys
from pathlib import Path
import bullet_trade
assert 'matplotlib.pyplot' not in sys.modules
from bullet_trade import plt
import matplotlib.pyplot as actual
assert plt is actual and bullet_trade.plt is actual
assert actual.get_backend().lower() == 'agg'
from bullet_trade import plot_results, calculate_metrics
from bullet_trade.core import analysis
assert plot_results is analysis.plot_results
assert calculate_metrics is analysis.calculate_metrics
figure, axis = plt.subplots()
axis.plot([1, 2, 3], [2, 4, 3])
figure.savefig('figure.png')
plt.close(figure)
assert Path('figure.png').read_bytes().startswith(b'\\x89PNG\\r\\n\\x1a\\n')
print('real plotting passed')
""",
        tmp_path / "plot",
    )
    assert result.stdout.strip() == "real plotting passed"


def test_optional_plot_import_failure_is_warned_and_cached_as_none(tmp_path):
    """输入自有目录，无返回；模拟绘图失败只在显式请求时警告，仍保持旧None兼容，不影响数据。"""
    result = isolated(
        '''
import warnings
import bullet_trade
original = bullet_trade.import_module
def failing(name, *args):
    """输入模块及参数，返回原导入或测试异常；只注入绘图库失败。"""
    if name == 'matplotlib.pyplot':
        raise RuntimeError('fixture-render-error')
    return original(name, *args)
bullet_trade.import_module = failing
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always')
    assert bullet_trade.plt is None
    assert bullet_trade.plt is None
    assert len(caught) == 1
    assert 'fixture-render-error' in str(caught[0].message)
assert callable(bullet_trade.get_price)
print('optional plotting passed')
''',
        tmp_path / "failure",
    )
    assert result.stdout.strip() == "optional plotting passed"
