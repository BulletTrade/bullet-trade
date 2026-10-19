"""作者：BruceLee。

职责：为纯实时仿真执行侧保存和加载原子 JSON 检查点，不承担账户预约或资金结算。
输入：调用方独占的本地文件路径、SimulationMatcher 及恢复时的原账户/政策和时钟。
输出：已保存检查点或恢复后的同一模拟账户撮合器；错误保留现有文件并显式抛出。
上下游：未来持续账户执行适配调用；只依赖开源撮合合同和 filelock，不导入私有 ORM。
约定：调用方必须在确认操作前保存，并独立完成账本幂等结算；本模块不保证跨库事务。
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from filelock import FileLock

from .simulation_matcher import SimulationMatcher, SimulationPolicy, _digest

__all__ = ["SimulationCheckpointFile"]


def _unique_keys(pairs: list) -> Dict[str, Any]:
    """输入 JSON 键值对，返回映射；重复字段拒绝，防止两种解析结果解释同一检查点。"""
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("仿真检查点包含重复字段")
        result[key] = value
    return result


def _no_clock() -> datetime:
    """无输入，始终报错；文件校验只重建旧日志，不应读取新运行时钟。"""
    raise RuntimeError("检查点校验不能推进运行时钟")


class SimulationCheckpointFile:
    """单文件执行侧检查点；短文件锁串行提交，同目录替换避免读取半份 JSON。

    文件不是账本，不含密码、现金或持仓。不同账户/环境/政策或分叉日志不能覆盖原文件；
    调用方自行提供独立账户目录，不以账户字符串拼接路径。
    """

    def __init__(self, path: Path) -> None:
        """输入已有目录中的目标文件，无返回；仅保存路径，不创建目录、不读写运行状态。"""
        self.path = Path(path).absolute()
        self._lock_path = self.path.with_name(self.path.name + ".lock")

    def _check_path(self) -> None:
        """无输入返回；拒绝符号链接或非普通文件，错误不覆盖其他目录/账户的数据。"""
        if not self.path.parent.is_dir():
            raise ValueError("仿真检查点目录不存在")
        for item in (self.path, self._lock_path, *self.path.parents):
            if item.is_symlink():
                raise ValueError("仿真检查点路径不能是符号链接")
        if self.path.exists() and not self.path.is_file():
            raise ValueError("仿真检查点必须是普通文件")

    def _read(self) -> Optional[Dict[str, Any]]:
        """无输入，返回完整检查点或缺文件时 None；损坏不当作空账户，调用方已持文件锁。"""
        try:
            with self.path.open("r", encoding="utf-8") as source:
                document = json.load(source, object_pairs_hook=_unique_keys)
        except FileNotFoundError:
            return None
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("仿真检查点损坏，不能按空账户恢复") from exc
        if not isinstance(document, dict) or set(document) != {"payload", "sha256"}:
            raise ValueError("仿真检查点结构无效")
        if _digest(document["payload"]) != document["sha256"]:
            raise ValueError("仿真检查点校验失败")
        return document

    def load(
        self,
        *,
        account_id: str,
        policy: SimulationPolicy,
        clock: Callable[[], datetime],
        is_trading_time: Callable[[str, datetime], bool],
    ) -> Optional[SimulationMatcher]:
        """输入原账户和政策及新时钟/时段依赖，返回恢复实例或缺文件 None；不发订单/成交。

        校验失败抛错且保留文件，调用方不得自动删文件、建空账户或重发未决订单。
        """
        self._check_path()
        with FileLock(str(self._lock_path)):
            self._check_path()
            document = self._read()
            if document is None:
                return None
            return SimulationMatcher.restore(
                document,
                account_id=account_id,
                policy=policy,
                clock=clock,
                is_trading_time=is_trading_time,
            )

    def save(self, matcher: SimulationMatcher) -> None:
        """输入当前撮合器，无返回；校验前缀后原子保存，不允许旧副本或分叉覆盖更新状态。

        替换前失败保留上一检查点；替换后目录 fsync 失败属于提交结果未知，调用方须加载
        原路径核对，不得重发。返回成功只证明执行侧文件保存，不证明共同账本已结算。
        """
        document = matcher.checkpoint()
        self._check_path()
        with FileLock(str(self._lock_path)):
            self._check_path()
            previous = self._read()
            if previous is not None:
                recovered = SimulationMatcher.restore(
                    previous,
                    account_id=matcher.account_id,
                    policy=matcher.policy,
                    clock=_no_clock,
                    is_trading_time=lambda security, at: False,
                )
                old, new = recovered.checkpoint()["payload"], document["payload"]
                count = len(old["commands"])
                if new["commands"][:count] != old["commands"]:
                    raise ValueError("旧状态或分叉日志不能覆盖仿真检查点")
                if old["last_time"] is not None and (
                    new["last_time"] is None
                    or datetime.fromisoformat(new["last_time"])
                    < datetime.fromisoformat(old["last_time"])
                ):
                    raise ValueError("旧时间水位不能覆盖仿真检查点")
                if count == len(new["commands"]) and old["facts_digest"] != new["facts_digest"]:
                    raise ValueError("相同日志不能覆盖不同执行事实")
            temporary = None
            try:
                descriptor, filename = tempfile.mkstemp(
                    prefix=".simulation-", suffix=".tmp", dir=self.path.parent
                )
                temporary = Path(filename)
                with os.fdopen(descriptor, "w", encoding="utf-8") as target:
                    json.dump(document, target, ensure_ascii=False, sort_keys=True)
                    target.flush()
                    os.fsync(target.fileno())
                os.replace(temporary, self.path)
                if os.name != "nt":
                    directory = os.open(self.path.parent, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
