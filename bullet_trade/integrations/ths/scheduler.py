"""单 GUI owner 的内存任务队列；任务仅在协作安全点让出。"""

from __future__ import annotations

import heapq
import math
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


YIELD = object()


@dataclass(frozen=True)
class TaskRunResult:
    status: str
    task_id: Optional[int] = None
    value: Any = None


@dataclass
class TaskContext:
    scheduler: "GuiTaskScheduler"
    priority: int

    def should_yield(self) -> bool:
        """GUI 操作完成后的安全点查询；调用方返回 YIELD 才让出。"""
        return self.scheduler.has_higher_priority(self.priority)


@dataclass(order=True)
class _Task:
    priority: int
    sequence: int
    task_id: int = field(compare=False)
    callback: Callable[[TaskContext], Any] = field(compare=False)
    expires_at: Optional[float] = field(compare=False)
    key: Optional[str] = field(compare=False)


class GuiTaskScheduler:
    """优先级数值越小越先执行；同优先级按入队顺序执行。"""

    def __init__(self, *, clock: Callable[[], float] = time.monotonic,
                 max_pending: int = 128) -> None:
        if not isinstance(max_pending, int) or max_pending < 1:
            raise ValueError("max_pending must be positive")
        self._clock = clock
        self.max_pending = max_pending
        self._lock = threading.Lock()
        self._runner_lock = threading.Lock()
        self._heap: List[_Task] = []
        self._keys: Dict[str, int] = {}
        self._refresh_times: Dict[str, float] = {}
        self._sequence = 0
        self._active = 0

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._heap)

    @staticmethod
    def _valid_deadline(expires_at: Optional[float]) -> None:
        if expires_at is not None and not math.isfinite(expires_at):
            raise ValueError("expires_at must be finite")

    def _prune_expired_locked(self, now: float) -> None:
        retained = []
        for task in self._heap:
            if task.expires_at is not None and task.expires_at <= now:
                if task.key is not None:
                    self._keys.pop(task.key, None)
            else:
                retained.append(task)
        if len(retained) != len(self._heap):
            self._heap = retained
            heapq.heapify(self._heap)

    def submit(
        self,
        callback: Callable[[TaskContext], Any],
        *,
        priority: int = 10,
        expires_at: Optional[float] = None,
        key: Optional[str] = None,
    ) -> Optional[int]:
        """同 key 的待执行或运行中任务只保留一份。时间是 monotonic 秒。"""
        if not callable(callback):
            raise TypeError("callback must be callable")
        self._valid_deadline(expires_at)
        now = self._clock()
        if expires_at is not None and expires_at <= now:
            return None
        with self._lock:
            self._prune_expired_locked(now)
            if key is not None and key in self._keys:
                return None
            if len(self._heap) + self._active >= self.max_pending:
                raise OverflowError("GUI task queue is full")
            self._sequence += 1
            task = _Task(priority, self._sequence, self._sequence, callback, expires_at, key)
            heapq.heappush(self._heap, task)
            if key is not None:
                self._keys[key] = task.task_id
            return task.task_id

    def submit_refresh(
        self,
        kind: str,
        callback: Callable[[TaskContext], Any],
        *,
        priority: int = 10,
        interval: float = 20.0,
        expires_at: Optional[float] = None,
    ) -> Optional[int]:
        """周期刷新不积压；20 秒是默认目标间隔，并非完成时限。"""
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("interval must be positive")
        self._valid_deadline(expires_at)
        if not kind:
            raise ValueError("kind is required")
        key = "refresh:" + kind
        now = self._clock()
        with self._lock:
            self._prune_expired_locked(now)
            if key in self._keys or now - self._refresh_times.get(key, float("-inf")) < interval:
                return None
            if expires_at is not None and expires_at <= now:
                return None
            if not callable(callback):
                raise TypeError("callback must be callable")
            if len(self._heap) + self._active >= self.max_pending:
                raise OverflowError("GUI task queue is full")
            self._sequence += 1
            task = _Task(priority, self._sequence, self._sequence, callback, expires_at, key)
            heapq.heappush(self._heap, task)
            self._keys[key] = task.task_id
            self._refresh_times[key] = now
            return task.task_id

    def has_higher_priority(self, priority: int) -> bool:
        now = self._clock()
        with self._lock:
            return any(task.priority < priority and
                       (task.expires_at is None or task.expires_at > now)
                       for task in self._heap)

    def run_next(self) -> TaskRunResult:
        """调用线程承担 GUI owner；并发 run_next 被串行化，无后台线程。"""
        with self._runner_lock:
            expired = False
            with self._lock:
                while self._heap:
                    task = heapq.heappop(self._heap)
                    if task.expires_at is None or task.expires_at > self._clock():
                        break
                    expired = True
                    if task.key is not None:
                        self._keys.pop(task.key, None)
                else:
                    return TaskRunResult("expired" if expired else "empty")
                self._active = 1
            try:
                value = task.callback(TaskContext(self, task.priority))
                if value is YIELD:
                    with self._lock:
                        if task.expires_at is not None and task.expires_at <= self._clock():
                            return TaskRunResult("expired", task.task_id)
                        self._sequence += 1
                        task.sequence = self._sequence
                        heapq.heappush(self._heap, task)
                    return TaskRunResult("yielded", task.task_id)
                return TaskRunResult("done", task.task_id, value)
            finally:
                with self._lock:
                    self._active = 0
                    if task.key is not None:
                        if not any(item.task_id == task.task_id for item in self._heap):
                            self._keys.pop(task.key, None)
