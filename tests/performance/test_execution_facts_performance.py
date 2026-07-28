"""
execution-facts/v1 writer 与 validator 的确定性性能发布门。

作者: BruceLee
文件职责: 实测 10 万常规 lane 与显式 100 万发布 lane 的吞吐、内存、RSS 和完整性。
主要输入: 固定 run ID、固定业务时间、按序生成的 run_metric 事实和临时输出目录。
主要输出: append/finalize/validate 指标 JSON，以及行数、SHA、唯一性和有界内存断言。
上下游关系: 上游是机器事实 writer， 下游是跨平台发布证据与 Bullet Quant consumer。
关键约定: 100 万 lane 仅在环境变量显式开启时运行；测试不连接网络、业务数据库或券商。
"""

import gc
import hashlib
import json
import os
import sys
import threading
import time
import tracemalloc
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import pytest

from bullet_trade.core.execution_facts import (
    EventType,
    ExecutionFactsWriter,
    validate_published_execution_facts,
)
from bullet_trade.core.price_basis import EffectivePriceBasis

try:
    import psutil
except ImportError:  # pragma: no cover - 最小 wheel 环境使用标准库 RSS 回退
    psutil = None

pytestmark = pytest.mark.slow

RUN_ID = "12345678-1234-5678-9234-567812345678"
OCCURRED_AT = datetime(2026, 7, 15, tzinfo=timezone.utc)
REGULAR_EVENT_COUNT = 100_000
RELEASE_EVENT_COUNT = 1_000_000
EXPECTED_REGULAR_SHA256 = "c810d1597f282500748dd0baf34f552f0568b0fddbca9eafc468b763ac226ca9"
EXPECTED_RELEASE_SHA256 = "9d11840e6aefa56767a4b3a2295e31bcc0473b75bbc13610afa5f6fa9cce7daa"
WRITER_TRACEMALLOC_LIMIT_BYTES = 8 * 1024 * 1024
VALIDATOR_TRACEMALLOC_LIMIT_BYTES = 32 * 1024 * 1024
PHASE_RSS_DELTA_LIMIT_BYTES = 128 * 1024 * 1024
MINIMUM_EVENTS_PER_SECOND = 1_000


def _current_rss_bytes() -> Tuple[int, str]:
    """读取当前进程 RSS，优先使用 psutil，缺失时使用标准库峰值。

    Returns:
        Tuple[int, str]: ``(RSS 字节数, 数据来源)``；不可用时字节数为 0。
    """

    if psutil is not None:
        return psutil.Process(os.getpid()).memory_info().rss, "psutil"
    try:
        import resource
    except ImportError:  # pragma: no cover - Windows 最小环境且未安装 psutil
        return 0, "unavailable"
    maximum = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    multiplier = 1 if sys.platform == "darwin" else 1024
    return int(maximum * multiplier), "resource-maximum"


class _RssSampler:
    """以固定短周期采样当前进程 RSS 的测试辅助器。"""

    def __init__(self) -> None:
        """创建尚未启动的采样器。

        Side Effects:
            初始化停止事件，但不创建活动线程。
        """

        current, source = _current_rss_bytes()
        self.peak_bytes = current
        self.source = source
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _sample(self) -> None:
        """循环采样 RSS，直到退出上下文时收到停止信号。

        Returns:
            None: 停止后退出。

        Side Effects:
            更新 ``peak_bytes``。
        """

        while not self._stop_event.wait(0.005):
            current, _ = _current_rss_bytes()
            self.peak_bytes = max(self.peak_bytes, current)

    def __enter__(self) -> "_RssSampler":
        """启动后台 RSS 采样线程。

        Returns:
            _RssSampler: 当前采样器。

        Side Effects:
            启动一个 daemon 线程。
        """

        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """停止采样并合并退出时 RSS。

        Args:
            exc_type: 可选异常类型。
            exc_value: 可选异常值。
            traceback: 可选异常堆栈。

        Returns:
            None: 不吞掉被测阶段异常。
        """

        del exc_type, exc_value, traceback
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()
        current, _ = _current_rss_bytes()
        self.peak_bytes = max(self.peak_bytes, current)


def _measure_phase(event_count: int, operation: Callable[[], Any]) -> Tuple[Any, Dict[str, Any]]:
    """测量一个同步阶段的耗时、吞吐、tracemalloc 与 RSS。

    Args:
        event_count: 用于换算吞吐的事实条数。
        operation: 不接收参数的 append、finalize 或 validate 操作。

    Returns:
        Tuple[Any, Dict[str, Any]]: 操作返回值和可 JSON 序列化的阶段指标。

    Side Effects:
        触发垃圾回收、tracemalloc 和短生命周期 RSS 采样线程。
    """

    gc.collect()
    rss_baseline, rss_source = _current_rss_bytes()
    tracemalloc.start()
    tracemalloc.reset_peak()
    started = time.perf_counter()
    try:
        with _RssSampler() as sampler:
            result = operation()
        elapsed = time.perf_counter() - started
        current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return result, {
        "seconds": elapsed,
        "events_per_second": event_count / elapsed,
        "rss_source": rss_source,
        "rss_baseline_bytes": rss_baseline,
        "rss_peak_bytes": sampler.peak_bytes,
        "rss_delta_bytes": max(0, sampler.peak_bytes - rss_baseline),
        "tracemalloc_current_bytes": current_bytes,
        "tracemalloc_peak_bytes": peak_bytes,
    }


def _append_deterministic_events(writer: ExecutionFactsWriter, event_count: int) -> None:
    """不保留 append 返回值地生成固定 run_metric 事实序列。

    Args:
        writer: 当前运行的机器事实 writer。
        event_count: 从 0 开始顺序生成的事件条数。

    Returns:
        None: 全部事件进入 writer 后无返回值。

    Side Effects:
        向 writer 的有界缓冲和 partial 追加事实。
    """

    if event_count < 3:
        raise AssertionError("性能 lane 至少需要三个每日质量事实")
    writer.observe_effective_price_basis(
        EffectivePriceBasis.create(
            use_real_price=False,
            provider="benchmark",
            business_time=OCCURRED_AT,
        )
    )
    writer.append(
        EventType.ACCOUNT_DAILY,
        authority_id="account-1",
        state_version=1,
        occurred_at=OCCURRED_AT,
        payload={
            "account_id": "account-1",
            "cash": Decimal("100000"),
            "available_cash": Decimal("100000"),
            "locked_cash": Decimal("0"),
            "positions_value": Decimal("0"),
            "total_value": Decimal("100000"),
        },
    )
    writer.append(
        EventType.DAILY_PERFORMANCE,
        authority_id="performance-1",
        state_version=1,
        occurred_at=OCCURRED_AT,
        payload={
            "account_id": "account-1",
            "total_value": Decimal("100000"),
            "net_asset_value": Decimal("1"),
            "daily_return": Decimal("0"),
            "cumulative_return": Decimal("0"),
            "strategy_return": Decimal("0"),
            "drawdown": Decimal("0"),
        },
    )
    writer.append(
        EventType.RECONCILE_EVENT,
        authority_id="reconcile-1",
        state_version=1,
        occurred_at=OCCURRED_AT,
        payload={
            "metric_name": "total-assets",
            "status": "PASSED",
            "expected": Decimal("100000"),
            "actual": Decimal("100000"),
            "difference": Decimal("0"),
        },
    )
    for index in range(event_count - 3):
        writer.append(
            EventType.RUN_METRIC,
            authority_id="metric-{0}".format(index),
            state_version=1,
            occurred_at=OCCURRED_AT,
            payload={
                "metric_name": "metric-{0}".format(index),
                "metric_value": Decimal(index),
            },
        )


def _sha256_file(path: Path) -> str:
    """以固定块大小计算大 facts 文件 SHA，避免测试自身线性占用内存。

    Args:
        path: 待计算摘要的普通文件。

    Returns:
        str: 小写十六进制 SHA-256。
    """

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _run_benchmark(event_count: int, root: Path) -> Dict[str, Any]:
    """执行 append→finalize→重新 validate 的完整性能链。

    Args:
        event_count: 本 lane 的确定性事实条数。
        root: pytest 临时目录。

    Returns:
        Dict[str, Any]: 文件摘要和三个阶段的实测指标。

    Side Effects:
        在临时目录生成完整机器事实、manifest 和指标 JSON。
    """

    output_dir = root / "execution-facts"
    writer = ExecutionFactsWriter(
        output_dir=output_dir,
        trusted_output_root=root,
        run_id=RUN_ID,
        producer_version="0.9.2",
        calculation_version="engine-ledger/v1",
        price_basis={
            "use_real_price": False,
            "fq": "none",
            "provider": "benchmark",
            "business_timezone": "Asia/Shanghai",
            "reference_policy": "not_applicable",
            "configured_ref_date": None,
            "business_date_start": "2026-07-15",
            "business_date_end": "2026-07-15",
        },
        buffer_size_bytes=64 * 1024,
        started_at=OCCURRED_AT,
    )
    _, append_metrics = _measure_phase(
        event_count,
        lambda: _append_deterministic_events(writer, event_count),
    )
    manifest, finalize_metrics = _measure_phase(
        event_count,
        lambda: writer.finalize(finished_at=OCCURRED_AT + timedelta(seconds=1)),
    )
    checked, validate_metrics = _measure_phase(
        event_count,
        lambda: validate_published_execution_facts(output_dir, expected_run_id=RUN_ID),
    )
    facts_path = output_dir / "facts.ndjson"
    actual_sha256 = _sha256_file(facts_path)
    report = {
        "implementation": "bounded-exact-disk-buckets",
        "event_count": event_count,
        "facts_bytes": facts_path.stat().st_size,
        "facts_sha256": actual_sha256,
        "manifest_record_count": manifest["facts"]["record_count"],
        "validated_record_count": checked["facts"]["record_count"],
        "append": append_metrics,
        "finalize": finalize_metrics,
        "validate": validate_metrics,
    }
    report_path = root / "execution-facts-performance-{0}.json".format(event_count)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return report


def _assert_hard_gates(report: Dict[str, Any], event_count: int) -> None:
    """执行与机器性能波动解耦的完整性和内存硬门。

    Args:
        report: ``_run_benchmark`` 返回的实测报告。
        event_count: 当前 lane 期望事实条数。

    Returns:
        None: 全部门通过时无返回值。

    Raises:
        AssertionError: 发生丢失、重复、SHA 不一致、线性内存或极端吞吐退化。
    """

    assert report["manifest_record_count"] == event_count
    assert report["validated_record_count"] == event_count
    assert len(report["facts_sha256"]) == 64
    assert report["append"]["tracemalloc_peak_bytes"] <= WRITER_TRACEMALLOC_LIMIT_BYTES
    assert report["finalize"]["tracemalloc_peak_bytes"] <= VALIDATOR_TRACEMALLOC_LIMIT_BYTES
    assert report["validate"]["tracemalloc_peak_bytes"] <= VALIDATOR_TRACEMALLOC_LIMIT_BYTES
    for phase_name in ("append", "finalize", "validate"):
        phase = report[phase_name]
        assert phase["seconds"] > 0
        assert phase["events_per_second"] >= MINIMUM_EVENTS_PER_SECOND
        if phase["rss_source"] == "psutil":
            assert phase["rss_delta_bytes"] <= PHASE_RSS_DELTA_LIMIT_BYTES


def test_execution_facts_regular_100k_performance_lane(tmp_path: Path) -> None:
    """运行 10 万确定性事实常规性能门并冻结事实 SHA。

    Args:
        tmp_path: pytest 临时输出目录。
    """

    report = _run_benchmark(REGULAR_EVENT_COUNT, tmp_path)
    _assert_hard_gates(report, REGULAR_EVENT_COUNT)
    assert report["facts_sha256"] == EXPECTED_REGULAR_SHA256


@pytest.mark.skipif(
    os.getenv("BULLET_TRADE_RUN_MILLION_FACTS") != "1",
    reason="设置 BULLET_TRADE_RUN_MILLION_FACTS=1 才运行 100 万发布 lane",
)
def test_execution_facts_release_1m_performance_lane(tmp_path: Path) -> None:
    """显式运行 100 万事实发布门，验证无丢失重复及有界内存。

    Args:
        tmp_path: pytest 临时输出目录。
    """

    report = _run_benchmark(RELEASE_EVENT_COUNT, tmp_path)
    _assert_hard_gates(report, RELEASE_EVENT_COUNT)
    assert report["facts_sha256"] == EXPECTED_RELEASE_SHA256
