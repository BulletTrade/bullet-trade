"""作者: BruceLee
文件职责: 验证 Live 运行态正常持久化及所有关键失败路径均失败关闭。
主要输入: 临时运行目录、损坏状态文件和受控文件系统异常。
主要输出: ``g.pkl``/``live_state.json`` 恢复断言及健康锁存异常断言。
上下游关系: 覆盖 ``bullet_trade.core.live_runtime``，保护 LiveEngine 重启恢复语义。
关键环境或配置约定: 测试只使用 pytest 临时目录，不访问网络或真实账户。
"""

from __future__ import annotations

import json
import os
import pickle
import time
from datetime import datetime

import pytest

from bullet_trade.core import live_runtime
from bullet_trade.core.globals import g
from bullet_trade.core.live_runtime import (
    LiveRuntimePersistenceError,
    assert_live_runtime_healthy,
    init_live_runtime,
    load_scheduler_cursor,
    persist_scheduler_cursor,
    save_g,
    start_g_autosave,
    stop_g_autosave,
)


@pytest.fixture(autouse=True)
def _clean_live_runtime() -> None:
    """在每个用例前后停止自动保存并清空策略全局变量。

    Args:
        无。

    Returns:
        None。pytest yield 前后执行隔离清理。
    """

    stop_g_autosave()
    g.clear()
    yield
    stop_g_autosave()
    g.clear()


def test_g_persist_cycle(tmp_path) -> None:
    """验证正常 ``g.pkl`` 可原子保存并在重新初始化后恢复。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过断言表达成功条件。
    """

    runtime = tmp_path / "rt"
    init_live_runtime(str(runtime))
    g.foo = 123
    save_g()

    g.foo = 0
    init_live_runtime(str(runtime))
    assert g.foo == 123
    assert_live_runtime_healthy()


def test_autosave_thread(tmp_path) -> None:
    """验证后台线程能按期保存 ``g.pkl``。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过文件存在和健康断言表达成功条件。
    """

    runtime = tmp_path / "rt2"
    init_live_runtime(str(runtime))
    g.bar = 456
    start_g_autosave(interval_sec=1)
    time.sleep(1.2)
    stop_g_autosave()

    path = os.path.join(str(runtime), "g.pkl")
    assert os.path.exists(path)
    assert_live_runtime_healthy()


def test_corrupt_g_file_blocks_restore(tmp_path) -> None:
    """验证损坏的已有 ``g.pkl`` 不会静默回退为空状态。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过异常字段断言表达成功条件。
    """

    runtime = tmp_path / "corrupt-g"
    runtime.mkdir()
    state_path = runtime / "g.pkl"
    original_bytes = b"not-a-pickle"
    state_path.write_bytes(original_bytes)

    with pytest.raises(LiveRuntimePersistenceError) as caught:
        init_live_runtime(str(runtime))

    assert caught.value.operation == "load_g"
    assert caught.value.path.endswith("g.pkl")
    with pytest.raises(LiveRuntimePersistenceError, match="health_check_after_load_g"):
        save_g()
    assert state_path.read_bytes() == original_bytes


def test_non_mapping_g_file_blocks_restore(tmp_path) -> None:
    """验证可反序列化但顶层不是映射的状态同样阻断启动。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过异常原因类型断言表达成功条件。
    """

    runtime = tmp_path / "invalid-g"
    runtime.mkdir()
    with (runtime / "g.pkl").open("wb") as state_file:
        pickle.dump(["unexpected"], state_file)

    with pytest.raises(LiveRuntimePersistenceError) as caught:
        init_live_runtime(str(runtime))

    assert caught.value.operation == "load_g"
    assert isinstance(caught.value.cause, TypeError)


def test_corrupt_live_state_blocks_cursor_restore(tmp_path) -> None:
    """验证损坏的 ``live_state.json`` 不会被当作无调度历史。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过异常操作名称断言表达成功条件。
    """

    runtime = tmp_path / "corrupt-live-state"
    runtime.mkdir()
    (runtime / "live_state.json").write_text("{broken", encoding="utf-8")
    with pytest.raises(LiveRuntimePersistenceError) as caught:
        init_live_runtime(str(runtime))

    assert caught.value.operation == "load_live_state"
    assert (runtime / "live_state.json").read_text(encoding="utf-8") == "{broken"


def test_invalid_scheduler_cursor_blocks_restore(tmp_path) -> None:
    """验证非法调度游标不会静默返回 None 并造成重复执行窗口。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过异常操作名称断言表达成功条件。
    """

    runtime = tmp_path / "invalid-cursor"
    runtime.mkdir()
    (runtime / "live_state.json").write_text(
        '{"scheduler": {"last_cursor": "not-a-time"}}',
        encoding="utf-8",
    )
    with pytest.raises(LiveRuntimePersistenceError) as caught:
        init_live_runtime(str(runtime))

    assert caught.value.operation == "load_live_state"


def test_live_state_write_failure_is_visible(tmp_path, monkeypatch) -> None:
    """验证调度游标原子替换失败会直接抛错并锁存健康故障。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。通过直接异常和健康异常断言表达成功条件。
    """

    runtime = tmp_path / "state-write-failure"
    init_live_runtime(str(runtime))

    def _raise_permission_error(source: str, target: str) -> None:
        """模拟文件系统拒绝原子替换。

        Args:
            source: 临时文件路径。
            target: 最终文件路径。

        Returns:
            None。本辅助函数始终抛出 PermissionError。

        Raises:
            PermissionError: 每次调用均抛出。
        """

        raise PermissionError(f"blocked replace: {source} -> {target}")

    monkeypatch.setattr(live_runtime.os, "replace", _raise_permission_error)

    with pytest.raises(LiveRuntimePersistenceError) as caught:
        persist_scheduler_cursor(datetime(2026, 8, 25, 14, 50))

    assert caught.value.operation == "write_live_state"
    with pytest.raises(LiveRuntimePersistenceError, match="health_check_after_write_live_state"):
        assert_live_runtime_healthy()


def test_g_save_failure_is_visible_to_health_check(tmp_path, monkeypatch) -> None:
    """验证 ``g.pkl`` 保存失败既直接抛错又能被主循环健康检查发现。

    Args:
        tmp_path: pytest 提供的临时目录。
        monkeypatch: pytest 提供的受控替换工具。

    Returns:
        None。通过异常链和健康锁存断言表达成功条件。
    """

    runtime = tmp_path / "g-write-failure"
    init_live_runtime(str(runtime))
    g.target_positions = {"518880.XSHG": 1.0}

    def _raise_permission_error(source: str, target: str) -> None:
        """模拟 ``g.pkl`` 原子替换被拒绝。

        Args:
            source: 临时文件路径。
            target: 最终文件路径。

        Returns:
            None。本辅助函数始终抛出 PermissionError。

        Raises:
            PermissionError: 每次调用均抛出。
        """

        raise PermissionError(f"blocked replace: {source} -> {target}")

    monkeypatch.setattr(live_runtime.os, "replace", _raise_permission_error)

    with pytest.raises(LiveRuntimePersistenceError) as caught:
        save_g()

    assert caught.value.operation == "save_g"
    with pytest.raises(LiveRuntimePersistenceError, match="health_check_after_save_g"):
        assert_live_runtime_healthy()


def test_scheduler_cursor_round_trip(tmp_path) -> None:
    """验证合法调度游标仍保持原有往返恢复语义。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过时间等值断言表达成功条件。
    """

    runtime = tmp_path / "cursor-round-trip"
    init_live_runtime(str(runtime))
    expected = datetime(2026, 8, 25, 14, 50)
    persist_scheduler_cursor(expected)

    init_live_runtime(str(runtime))
    assert load_scheduler_cursor() == expected


@pytest.mark.parametrize(
    "payload",
    [
        {"scheduler": []},
        {"subscriptions": {"symbols": "159915.XSHE", "markets": []}},
        {"strategy": {"version": 1, "settings": [], "tasks": []}},
        {"strategy": {"version": 1, "settings": {}, "tasks": {}}},
        {
            "strategy": {
                "version": 1,
                "settings": {},
                "tasks": [{"module": 1, "func": "publish", "schedule_type": "daily"}],
            }
        },
        {
            "strategy": {
                "version": 1,
                "settings": {},
                "tasks": [
                    {
                        "module": "strategy",
                        "func": "publish",
                        "schedule_type": "weekly",
                        "time": "10:40",
                        "reference_security": 159915,
                    }
                ],
            }
        },
        {
            "strategy": {
                "version": 1,
                "settings": {},
                "tasks": [
                    {
                        "module": "strategy",
                        "func": "publish",
                        "schedule_type": "monthly",
                        "time": "14:50",
                        "force": "yes",
                    }
                ],
            }
        },
    ],
)
def test_nested_live_state_is_validated_before_g_assignment(tmp_path, payload) -> None:
    """验证嵌套状态损坏会在替换内存 ``g`` 之前阻断初始化。

    Args:
        tmp_path: pytest 提供的临时目录。
        payload: 参数化注入的非法嵌套状态。

    Returns:
        None。通过异常、内存哨兵和原始字节断言表达成功条件。
    """

    runtime = tmp_path / "nested-invalid"
    runtime.mkdir()
    with (runtime / "g.pkl").open("wb") as state_file:
        pickle.dump({"candidate": "must-not-apply"}, state_file)
    raw_state = json.dumps(payload, ensure_ascii=False)
    (runtime / "live_state.json").write_text(raw_state, encoding="utf-8")
    g._data = {"sentinel": "preserved"}

    with pytest.raises(LiveRuntimePersistenceError) as caught:
        init_live_runtime(str(runtime))

    assert caught.value.operation == "load_live_state"
    assert g._data == {"sentinel": "preserved"}
    assert (runtime / "live_state.json").read_text(encoding="utf-8") == raw_state


@pytest.mark.parametrize("filename", ["g.pkl.tmp", "live_state.json.tmp"])
def test_orphan_atomic_tmp_blocks_initialization(tmp_path, filename: str) -> None:
    """验证遗留原子写临时文件不会被覆盖或当作首次运行忽略。

    Args:
        tmp_path: pytest 提供的临时目录。
        filename: 参数化注入的临时文件名。

    Returns:
        None。通过异常和临时文件字节不变断言表达成功条件。
    """

    runtime = tmp_path / "orphan-tmp"
    runtime.mkdir()
    tmp_file = runtime / filename
    original = b"forensic-partial-write"
    tmp_file.write_bytes(original)

    with pytest.raises(LiveRuntimePersistenceError):
        init_live_runtime(str(runtime))

    assert tmp_file.read_bytes() == original


def test_first_run_resets_g_after_all_preflight_checks_pass(tmp_path) -> None:
    """验证切换到空运行目录时不会继承同进程旧策略的 ``g``。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过全局状态为空断言表达成功条件。
    """

    g._data = {"old_runtime": True}
    init_live_runtime(str(tmp_path / "new-runtime"))

    assert g._data == {}


def test_stop_autosave_keeps_reference_when_worker_is_still_alive(tmp_path) -> None:
    """验证停止超时不会遗忘仍可能写盘的后台线程。

    Args:
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。通过异常和线程引用保持断言表达成功条件。
    """

    init_live_runtime(str(tmp_path / "slow-worker"))

    class _NeverStopsThread:
        """模拟收到停止信号后仍未退出的自动保存线程。"""

        def is_alive(self) -> bool:
            """报告线程仍存活。

            Args:
                无。

            Returns:
                bool: 固定返回 True。
            """

            return True

        def join(self, timeout: float) -> None:
            """模拟等待超时但线程没有退出。

            Args:
                timeout: 调用方允许等待的秒数。

            Returns:
                None。仅消费参数，不改变存活状态。
            """

            _ = timeout

    fake_thread = _NeverStopsThread()
    old_thread = live_runtime._autosave_thread
    old_event = live_runtime._autosave_stop_event
    live_runtime._autosave_thread = fake_thread  # type: ignore[assignment]
    live_runtime._autosave_stop_event = live_runtime.threading.Event()
    try:
        with pytest.raises(LiveRuntimePersistenceError) as caught:
            stop_g_autosave(join_timeout=0)
        assert caught.value.operation == "stop_g_autosave"
        assert live_runtime._autosave_thread is fake_thread
    finally:
        live_runtime._autosave_thread = old_thread
        live_runtime._autosave_stop_event = old_event


def test_atexit_wrapper_never_leaks_callback_exception(monkeypatch, tmp_path) -> None:
    """验证解释器退出包装器不会产生 ``Exception ignored`` 回调异常。

    Args:
        monkeypatch: pytest 提供的受控替换工具。
        tmp_path: pytest 提供的临时目录。

    Returns:
        None。函数正常返回即证明异常没有逃逸。
    """

    init_live_runtime(str(tmp_path / "atexit"))

    def _raise_save_error() -> None:
        """模拟退出阶段保存失败。

        Args:
            无。

        Returns:
            None。本辅助函数始终抛出异常。

        Raises:
            RuntimeError: 每次调用均抛出。
        """

        raise RuntimeError("shutdown storage unavailable")

    monkeypatch.setattr(live_runtime, "save_g", _raise_save_error)
    live_runtime._atexit_save_g()
