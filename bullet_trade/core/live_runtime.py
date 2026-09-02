"""作者: BruceLee
文件职责: 严格恢复和持久化 Live 策略全局状态、调度游标及订阅元数据。
主要输入: 策略独占运行目录、全局对象 ``g``、调度游标和订阅集合。
主要输出: 原子写入 ``g.pkl`` 与 ``live_state.json``，并向 LiveEngine 暴露健康锁存。
上下游关系: 上游为 LiveEngine 和策略调度器；下游为重启恢复、语义健康与事故审计。
关键环境或配置约定: 已有状态损坏或任一持久化失败均失败关闭，不回退为空状态。
"""

from __future__ import annotations

import atexit
import copy
import json
import os
import pickle
import threading
from datetime import datetime
from typing import Any, Dict, NoReturn, Optional, Sequence, Set, Tuple

from .globals import g, log

_runtime_dir: Optional[str] = None
_autosave_thread: Optional[threading.Thread] = None
_autosave_stop_event: Optional[threading.Event] = None
_state_cache: Optional[Dict[str, Any]] = None
_state_lock = threading.Lock()
_g_lock = threading.Lock()
_failure_lock = threading.Lock()
_autosave_lifecycle_lock = threading.Lock()
_restored_from_disk = False
_last_persistence_failure: Optional["LiveRuntimePersistenceError"] = None
_atexit_registered = False


class LiveRuntimePersistenceError(RuntimeError):
    """表示 Live 运行态无法可靠读取或持久化。

    核心协作对象：LiveEngine 主循环和本模块的运行态读写函数。
    关键状态：保存失败操作、目标路径和原始异常，供日志与外部健康检查定位。
    """

    def __init__(self, operation: str, path: str, cause: BaseException) -> None:
        """构造可诊断的运行态持久化异常。

        Args:
            operation: 失败的读写操作名称。
            path: 发生故障的运行态文件路径。
            cause: 不含业务载荷的原始异常对象。

        Returns:
            None。异常实例保存诊断字段并生成安全消息。
        """

        self.operation = str(operation)
        self.path = str(path)
        self.cause = cause
        super().__init__(
            f"Live 运行态持久化失败: operation={self.operation} "
            f"path={self.path} error={type(cause).__name__}: {cause}"
        )


def _remember_and_raise_failure(
    operation: str,
    path: str,
    cause: BaseException,
) -> NoReturn:
    """记录首个持久化故障、输出异常链并立即抛出。

    Args:
        operation: 失败操作名称。
        path: 失败文件路径。
        cause: 原始异常。

    Returns:
        NoReturn: 本函数不会正常返回。

    Raises:
        LiveRuntimePersistenceError: 每次调用均抛出，且首个错误会进入健康锁存。
    """

    failure = LiveRuntimePersistenceError(operation, path, cause)
    global _last_persistence_failure
    with _failure_lock:
        if _last_persistence_failure is None:
            _last_persistence_failure = failure
    log.error(
        str(failure),
        exc_info=(type(cause), cause, cause.__traceback__),
    )
    raise failure from cause


def _reset_persistence_failure() -> None:
    """在新的运行态初始化前清空旧进程内故障锁存。

    Args:
        无。

    Returns:
        None。仅重置内存引用，不修改任何磁盘文件。
    """

    global _last_persistence_failure
    with _failure_lock:
        _last_persistence_failure = None


def assert_live_runtime_healthy() -> None:
    """确认后台自动保存没有发生未处理的持久化故障。

    Args:
        无。

    Returns:
        None。健康时直接返回。

    Raises:
        LiveRuntimePersistenceError: 后台或前序读写曾失败时抛出，阻断主循环。
    """

    with _failure_lock:
        failure = _last_persistence_failure
    if failure is not None:
        raise LiveRuntimePersistenceError(
            f"health_check_after_{failure.operation}",
            failure.path,
            failure,
        ) from failure


def _g_path() -> str:
    """返回当前运行目录中的策略全局状态路径。

    Args:
        无。

    Returns:
        str: ``g.pkl`` 的绝对路径。
    """

    assert _runtime_dir is not None
    return os.path.join(_runtime_dir, "g.pkl")


def _state_path() -> str:
    """返回当前运行目录中的扩展状态路径。

    Args:
        无。

    Returns:
        str: ``live_state.json`` 的绝对路径。
    """

    assert _runtime_dir is not None
    return os.path.join(_runtime_dir, "live_state.json")


def _validate_string_list(value: Any, field_name: str) -> None:
    """校验订阅字段必须是纯字符串列表。

    Args:
        value: 待校验的 JSON 值。
        field_name: 用于异常消息的字段名称。

    Returns:
        None。值合法时直接返回。

    Raises:
        TypeError: 值不是列表或列表包含非字符串元素时抛出。
    """

    if not isinstance(value, list):
        raise TypeError(f"{field_name} 必须是字符串列表")
    if any(not isinstance(item, str) for item in value):
        raise TypeError(f"{field_name} 只能包含字符串")


def _validate_strategy_metadata(metadata: Any) -> None:
    """校验策略元数据的已知嵌套容器与关键字段类型。

    Args:
        metadata: ``live_state.json.strategy`` 的候选值。

    Returns:
        None。缺省值或结构合法时直接返回。

    Raises:
        TypeError: 任一已知容器或关键字段类型不合法时抛出。
        ValueError: 日期字段无法解析时抛出。
    """

    if metadata is None:
        return
    if not isinstance(metadata, dict):
        raise TypeError("strategy 必须是 JSON object")
    if "version" in metadata and not isinstance(metadata["version"], int):
        raise TypeError("strategy.version 必须是整数")
    strategy_hash = metadata.get("strategy_hash")
    if strategy_hash is not None and not isinstance(strategy_hash, str):
        raise TypeError("strategy.strategy_hash 必须是字符串或 null")
    for field_name in ("settings",):
        value = metadata.get(field_name)
        if value is not None and not isinstance(value, dict):
            raise TypeError(f"strategy.{field_name} 必须是 JSON object")
    settings = metadata.get("settings") or {}
    for field_name in (
        "options",
        "order_cost",
        "order_cost_overrides",
        "slippage",
        "slippage_map",
    ):
        value = settings.get(field_name)
        if value is not None and not isinstance(value, dict):
            raise TypeError(f"strategy.settings.{field_name} 必须是 JSON object")
    benchmark = settings.get("benchmark")
    if benchmark is not None and not isinstance(benchmark, str):
        raise TypeError("strategy.settings.benchmark 必须是字符串或 null")
    for field_name in ("order_cost", "order_cost_overrides", "slippage_map"):
        values = settings.get(field_name) or {}
        for key, value in values.items():
            if not isinstance(key, str) or not isinstance(value, dict):
                raise TypeError(f"strategy.settings.{field_name} 必须是字符串到 JSON object 的映射")
    tasks = metadata.get("tasks")
    if tasks is not None:
        if not isinstance(tasks, list):
            raise TypeError("strategy.tasks 必须是 JSON array")
        for index, task in enumerate(tasks):
            if not isinstance(task, dict):
                raise TypeError(f"strategy.tasks[{index}] 必须是 JSON object")
            for field_name in ("module", "func", "schedule_type", "time"):
                value = task.get(field_name)
                if value is not None and not isinstance(value, str):
                    raise TypeError(f"strategy.tasks[{index}].{field_name} 必须是字符串或 null")
            reference_security = task.get("reference_security")
            if reference_security is not None and not isinstance(reference_security, str):
                raise TypeError(
                    f"strategy.tasks[{index}].reference_security 必须是字符串或 null"
                )
            if metadata.get("version") == 1:
                for field_name in ("module", "func", "schedule_type", "time"):
                    if not task.get(field_name):
                        raise TypeError(f"strategy.tasks[{index}].{field_name} 是 v1 必填字符串")
            for field_name in ("weekday", "monthday"):
                value = task.get(field_name)
                if value is not None and not isinstance(value, int):
                    raise TypeError(f"strategy.tasks[{index}].{field_name} 必须是整数或 null")
            enabled = task.get("enabled")
            if enabled is not None and not isinstance(enabled, bool):
                raise TypeError(f"strategy.tasks[{index}].enabled 必须是布尔值")
            force = task.get("force")
            if force is not None and not isinstance(force, bool):
                raise TypeError(f"strategy.tasks[{index}].force 必须是布尔值")
    start_date = metadata.get("strategy_start_date")
    if start_date is not None:
        if not isinstance(start_date, str):
            raise TypeError("strategy.strategy_start_date 必须是 ISO 日期字符串")
        datetime.fromisoformat(start_date)


def _validate_live_state(state: Any) -> Dict[str, Any]:
    """完整校验扩展运行态的已知嵌套结构。

    Args:
        state: 从 JSON 读取或即将写入的完整状态。

    Returns:
        Dict[str, Any]: 通过校验的原映射。

    Raises:
        TypeError: 顶层或已知嵌套字段类型不合法时抛出。
        ValueError: 调度游标或日期字段无法解析时抛出。
    """

    if not isinstance(state, dict):
        raise TypeError("live_state.json 顶层必须是 JSON object")
    scheduler = state.get("scheduler")
    if scheduler is not None:
        if not isinstance(scheduler, dict):
            raise TypeError("scheduler 必须是 JSON object")
        cursor = scheduler.get("last_cursor")
        if cursor is not None:
            if not isinstance(cursor, str):
                raise TypeError("scheduler.last_cursor 必须是 ISO 时间字符串")
            datetime.fromisoformat(cursor)
    subscriptions = state.get("subscriptions")
    if subscriptions is not None:
        if not isinstance(subscriptions, dict):
            raise TypeError("subscriptions 必须是 JSON object")
        for field_name in ("symbols", "markets"):
            if field_name in subscriptions:
                _validate_string_list(
                    subscriptions[field_name],
                    f"subscriptions.{field_name}",
                )
    _validate_strategy_metadata(state.get("strategy"))
    return state


def _load_candidate_state(path: str) -> Dict[str, Any]:
    """从候选路径读取并校验完整扩展运行态。

    Args:
        path: 候选 ``live_state.json`` 路径。

    Returns:
        Dict[str, Any]: 文件不存在时为空映射，否则为已验证状态。

    Raises:
        OSError: 文件读取失败时抛出。
        ValueError: JSON 或嵌套语义不合法时抛出。
        TypeError: 嵌套字段类型不合法时抛出。
    """

    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as state_file:
        loaded = json.load(state_file)
    return _validate_live_state(loaded)


def _load_candidate_g(path: str) -> Optional[Dict[str, Any]]:
    """从候选路径读取并校验策略全局状态。

    Args:
        path: 候选 ``g.pkl`` 路径。

    Returns:
        Optional[Dict[str, Any]]: 文件不存在时为 None，否则为已验证映射。

    Raises:
        OSError: 文件读取失败时抛出。
        pickle.UnpicklingError: pickle 损坏时抛出。
        TypeError: 顶层不是字典时抛出。
    """

    if not os.path.exists(path):
        return None
    with open(path, "rb") as state_file:
        loaded = pickle.load(state_file)
    if not isinstance(loaded, dict):
        raise TypeError("g.pkl 顶层必须是 dict")
    return loaded


def _validate_orphan_tmp_boundary(path: str, *, committed_exists: bool) -> None:
    """校验遗留临时文件是否有可用的已提交最终状态。

    Args:
        path: 最终状态文件路径；函数检查同名 ``.tmp``。
        committed_exists: 最终状态文件是否在本次读取前存在并已通过校验。

    Returns:
        None。临时文件不存在或最终状态有效时直接返回。

    Raises:
        FileExistsError: 仅有遗留临时文件而没有有效最终状态时抛出。

    Side Effects:
        最终状态有效时只记录告警，不读取、改名或删除临时文件。
    """

    tmp_path = path + ".tmp"
    if not os.path.exists(tmp_path):
        return
    if not committed_exists:
        raise FileExistsError(f"检测到未决原子写临时文件且最终状态不存在: {tmp_path}")
    log.warning(
        "检测到遗留原子写临时文件，已按通过校验的最终状态继续启动；"
        "临时文件将在下次正常保存时覆盖: %s",
        tmp_path,
    )


def _load_state() -> Dict[str, Any]:
    """严格加载并缓存扩展运行态。

    Args:
        无。

    Returns:
        Dict[str, Any]: 已验证为映射的状态；文件不存在时返回空映射。

    Raises:
        LiveRuntimePersistenceError: 文件存在但无法读取、解析或不是映射时抛出。
    """

    global _state_cache
    if _runtime_dir is None:
        return {}
    assert_live_runtime_healthy()
    with _state_lock:
        if _state_cache is not None:
            return copy.deepcopy(_state_cache)
        path = _state_path()
        try:
            committed_exists = os.path.exists(path)
            _state_cache = _load_candidate_state(path)
            _validate_orphan_tmp_boundary(
                path,
                committed_exists=committed_exists,
            )
        except Exception as exc:
            _remember_and_raise_failure("load_live_state", path, exc)
        return copy.deepcopy(_state_cache)


def _write_state(state: Dict[str, Any]) -> None:
    """原子写入扩展运行态并更新内存缓存。

    Args:
        state: 要持久化的完整扩展状态映射。

    Returns:
        None。写入成功后更新缓存。

    Raises:
        LiveRuntimePersistenceError: 创建、序列化或替换状态文件失败时抛出。
    """

    if _runtime_dir is None:
        return
    assert_live_runtime_healthy()
    with _state_lock:
        state_path = _state_path()
        tmp = state_path + ".tmp"
        try:
            _validate_live_state(state)
            payload = json.dumps(
                state,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ).encode("utf-8")
            os.makedirs(os.path.dirname(tmp), exist_ok=True)
            with open(tmp, "wb") as state_file:
                state_file.write(payload)
                state_file.flush()
                os.fsync(state_file.fileno())
            os.replace(tmp, state_path)
            global _state_cache
            _state_cache = copy.deepcopy(state)
        except Exception as exc:
            _remember_and_raise_failure("write_live_state", state_path, exc)


def init_live_runtime(runtime_dir: str) -> None:
    """初始化 Live 运行态并严格恢复已有 ``g.pkl``。

    Args:
        runtime_dir: 独占的策略运行目录。

    Returns:
        None。文件不存在时建立首次运行上下文，存在时恢复同一映射状态。

    Raises:
        LiveRuntimePersistenceError: 目录或已有 ``g.pkl`` 无法读取时抛出。
    """

    global _runtime_dir, _restored_from_disk, _state_cache
    candidate_dir = os.path.abspath(os.path.expanduser(runtime_dir))
    _reset_persistence_failure()

    with _autosave_lifecycle_lock:
        if _autosave_thread is not None and _autosave_thread.is_alive():
            cause = RuntimeError("自动保存线程仍在运行，拒绝切换运行目录")
            _remember_and_raise_failure("init_with_live_autosave", candidate_dir, cause)

    g_path = os.path.join(candidate_dir, "g.pkl")
    state_path = os.path.join(candidate_dir, "live_state.json")
    try:
        if os.path.exists(candidate_dir) and not os.path.isdir(candidate_dir):
            raise NotADirectoryError(candidate_dir)
        committed_g_exists = os.path.exists(g_path)
        candidate_g = _load_candidate_g(g_path)
        _validate_orphan_tmp_boundary(
            g_path,
            committed_exists=committed_g_exists,
        )
    except Exception as exc:
        _remember_and_raise_failure("load_g", g_path, exc)

    try:
        committed_state_exists = os.path.exists(state_path)
        candidate_state = _load_candidate_state(state_path)
        _validate_orphan_tmp_boundary(
            state_path,
            committed_exists=committed_state_exists,
        )
    except Exception as exc:
        _remember_and_raise_failure("load_live_state", state_path, exc)

    try:
        os.makedirs(candidate_dir, exist_ok=True)
    except Exception as exc:
        _remember_and_raise_failure("init_runtime_dir", candidate_dir, exc)

    _runtime_dir = candidate_dir
    _state_cache = copy.deepcopy(candidate_state)
    _restored_from_disk = candidate_g is not None
    g._data = candidate_g if candidate_g is not None else {}  # type: ignore[attr-defined]


def save_g() -> None:
    """原子保存策略全局状态到 ``RUNTIME_DIR/g.pkl``。

    Args:
        无。

    Returns:
        None。尚未初始化运行目录时不执行写入。

    Raises:
        LiveRuntimePersistenceError: 序列化、写入或原子替换失败时抛出。
    """

    if _runtime_dir is None:
        return
    assert_live_runtime_healthy()
    state_path = _g_path()
    with _g_lock:
        try:
            tmp = state_path + ".tmp"
            payload = pickle.dumps(
                getattr(g, "_data", {}),
                protocol=pickle.HIGHEST_PROTOCOL,
            )
            with open(tmp, "wb") as state_file:
                state_file.write(payload)
                state_file.flush()
                os.fsync(state_file.fileno())
            os.replace(tmp, state_path)
        except Exception as exc:
            _remember_and_raise_failure("save_g", state_path, exc)


def _autosave_worker(interval_sec: int, stop_event: threading.Event) -> None:
    """定期保存 ``g``，首次失败后停止并等待主循环接管故障。

    Args:
        interval_sec: 两次保存之间的秒数，最小为 1 秒。
        stop_event: 当前工作线程独占的停止事件，避免新旧线程相互误停。

    Returns:
        None。收到停止事件或发生持久化故障时结束线程。
    """

    while not stop_event.is_set():
        if stop_event.wait(max(1, interval_sec)):
            return
        try:
            save_g()
        except LiveRuntimePersistenceError:
            return


def _atexit_save_g() -> None:
    """在解释器退出时执行不向 atexit 泄漏异常的最后保存。

    Args:
        无。

    Returns:
        None。失败已由 ``save_g`` 锁存和记录，本包装器只避免 Python 输出
        ``Exception ignored in atexit callback``。
    """

    if _runtime_dir is None or not os.path.isdir(_runtime_dir):
        return
    try:
        save_g()
    except Exception:
        return


def start_g_autosave(interval_sec: int = 60) -> None:
    """启动后台线程周期性保存 ``g``。

    Args:
        interval_sec: 自动保存间隔秒数，最小按 1 秒处理。

    Returns:
        None。已有存活线程时保持原线程，不重复启动。
    """

    global _autosave_thread, _autosave_stop_event, _atexit_registered
    assert_live_runtime_healthy()
    with _autosave_lifecycle_lock:
        if _autosave_thread and _autosave_thread.is_alive():
            return
        _autosave_thread = None
        stop_event = threading.Event()
        thread = threading.Thread(
            target=_autosave_worker,
            args=(interval_sec, stop_event),
            daemon=True,
            name="bullet-trade-g-autosave",
        )
        _autosave_stop_event = stop_event
        _autosave_thread = thread
        try:
            thread.start()
        except Exception as exc:
            _autosave_thread = None
            _autosave_stop_event = None
            path = _g_path() if _runtime_dir is not None else "g.pkl"
            _remember_and_raise_failure("start_g_autosave", path, exc)
        if not _atexit_registered:
            atexit.register(_atexit_save_g)
            _atexit_registered = True
    log.warning(
        "已启用兼容模式 g 自动保存；策略并发修改嵌套对象时不提供事务快照，"
        "要求严格分钟 checkpoint 的正式 runner 必须关闭自动保存"
    )


def stop_g_autosave(join_timeout: float = 5.0) -> None:
    """停止自动保存线程并等待其退出。

    Args:
        join_timeout: 等待线程退出的最长秒数；超时会锁存故障并保留线程引用。

    Returns:
        None。停止动作不额外覆盖磁盘状态。
    """

    global _autosave_thread, _autosave_stop_event
    with _autosave_lifecycle_lock:
        thread = _autosave_thread
        stop_event = _autosave_stop_event
        if thread is None:
            return
        if stop_event is not None:
            stop_event.set()
    if thread.is_alive():
        thread.join(timeout=max(0.0, float(join_timeout)))
    if thread.is_alive():
        path = _g_path() if _runtime_dir is not None else "g.pkl"
        cause = TimeoutError("自动保存线程未在等待时间内退出")
        _remember_and_raise_failure("stop_g_autosave", path, cause)
    with _autosave_lifecycle_lock:
        if _autosave_thread is thread:
            _autosave_thread = None
            _autosave_stop_event = None


def load_scheduler_cursor() -> Optional[datetime]:
    """读取最近一次调度游标用于重启恢复。

    Args:
        无。

    Returns:
        Optional[datetime]: 没有游标时返回 None，否则返回解析后的时间。

    Raises:
        LiveRuntimePersistenceError: 已有游标不是合法 ISO 时间时抛出。
    """

    state = _load_state()
    cursor = (state.get("scheduler") or {}).get("last_cursor")
    if not cursor:
        return None
    try:
        return datetime.fromisoformat(cursor)
    except Exception as exc:
        _remember_and_raise_failure("parse_scheduler_cursor", _state_path(), exc)


def persist_scheduler_cursor(dt: datetime) -> None:
    """保存最近一次调度游标用于重启恢复。

    Args:
        dt: 已完成处理的调度分钟。

    Returns:
        None。成功后原子更新 ``live_state.json``。
    """

    state = _load_state()
    scheduler = dict(state.get("scheduler") or {})
    scheduler["last_cursor"] = dt.isoformat()
    state["scheduler"] = scheduler
    _write_state(state)


def load_subscription_state() -> Tuple[Set[str], Set[str]]:
    """返回上次记录的 tick 订阅。

    Args:
        无。

    Returns:
        Tuple[Set[str], Set[str]]: 已保存的证券代码集合和市场集合。
    """

    state = _load_state()
    record = state.get("subscriptions") or {}
    symbols = set(record.get("symbols") or [])
    markets = set(record.get("markets") or [])
    return symbols, markets


def persist_subscription_state(symbols: Sequence[str], markets: Sequence[str]) -> None:
    """保存 tick 订阅状态供重启恢复。

    Args:
        symbols: 当前订阅的证券代码序列。
        markets: 当前订阅的市场序列。

    Returns:
        None。成功后原子更新 ``live_state.json``。
    """

    state = _load_state()
    state["subscriptions"] = {
        "symbols": sorted({str(symbol) for symbol in symbols}),
        "markets": sorted({str(market) for market in markets}),
    }
    _write_state(state)


def runtime_restored() -> bool:
    """报告本次初始化是否从已有 ``g.pkl`` 成功恢复。

    Args:
        无。

    Returns:
        bool: 仅在已有映射状态完整恢复时为 True。
    """

    return _restored_from_disk


def load_strategy_metadata() -> Dict[str, Any]:
    """读取当前策略元数据快照。

    Args:
        无。

    Returns:
        Dict[str, Any]: 元数据映射的独立副本。
    """

    state = _load_state()
    return dict(state.get("strategy") or {})


def persist_strategy_metadata(metadata: Dict[str, Any]) -> None:
    """持久化当前策略元数据快照。

    Args:
        metadata: 策略哈希、配置、任务和能力声明等元数据。

    Returns:
        None。成功后原子更新 ``live_state.json``。
    """

    state = _load_state()
    state["strategy"] = metadata
    _write_state(state)
