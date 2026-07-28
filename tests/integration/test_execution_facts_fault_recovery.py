"""
execution-facts/v1 在真实子进程中断后的 partial 恢复边界测试。

作者: BruceLee
文件职责: 用操作系统终止子进程，验证 durable partial 可诊断且绝不出现成功 manifest。
主要输入: 固定 100 条 run_metric 事实、pytest 临时目录和独立 Python 子进程。
主要输出: 对进程终止码、partial 行数/SHA、final 与 manifest 缺失的断言。
上下游关系: 上游是 ExecutionFactsWriter.flush，下游是故障诊断和重新运行流程。
关键约定: 测试不模拟成功发布，不连接网络或外部服务，子进程只写 pytest 临时目录。
"""

import hashlib
import subprocess
import sys
import time
from pathlib import Path

import pytest

from bullet_trade.core.execution_facts import validate_facts_file

pytestmark = pytest.mark.integration

RUN_ID = "12345678-1234-5678-9234-567812345678"
EVENT_COUNT = 100

_CHILD_SCRIPT = r"""
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from bullet_trade.core.execution_facts import EventType, ExecutionFactsWriter

output_dir = Path(sys.argv[1])
ready_path = Path(sys.argv[2])
occurred_at = datetime(2026, 7, 15, tzinfo=timezone.utc)
writer = ExecutionFactsWriter(
    output_dir=output_dir,
    trusted_output_root=output_dir.parent,
    run_id="12345678-1234-5678-9234-567812345678",
    producer_version="0.9.2",
    calculation_version="engine-ledger/v1",
    price_basis={
        "use_real_price": False,
        "fq": "none",
        "provider": "fault-probe",
        "business_timezone": "Asia/Shanghai",
        "reference_policy": "not_applicable",
        "configured_ref_date": None,
        "business_date_start": "2026-07-15",
        "business_date_end": "2026-07-15",
    },
    buffer_size_bytes=256,
    started_at=occurred_at,
)
for index in range(100):
    writer.append(
        EventType.RUN_METRIC,
        authority_id="metric-{0}".format(index),
        state_version=1,
        occurred_at=occurred_at,
        payload={
            "metric_name": "metric-{0}".format(index),
            "metric_value": Decimal(index),
        },
    )
writer.flush(durable=True)
ready_path.write_text("ready\n", encoding="utf-8")
time.sleep(60)
"""


def test_killed_process_preserves_only_durable_partial(tmp_path: Path) -> None:
    """终止已 fsync 子进程并复验 partial，无 final 或成功 manifest。

    Args:
        tmp_path: pytest 提供的临时输出目录。

    Side Effects:
        启动并终止一个只写临时目录的 Python 子进程。
    """

    output_dir = tmp_path / "execution-facts"
    ready_path = tmp_path / "ready"
    project_root = Path(__file__).resolve().parents[2]
    process = subprocess.Popen(
        [sys.executable, "-c", _CHILD_SCRIPT, str(output_dir), str(ready_path)],
        cwd=str(project_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 15
        while not ready_path.exists():
            if process.poll() is not None:
                stdout, stderr = process.communicate(timeout=5)
                pytest.fail(
                    "子进程在 durable partial 就绪前退出: stdout={0!r}, stderr={1!r}".format(stdout, stderr)
                )
            if time.monotonic() >= deadline:
                pytest.fail("等待子进程 durable partial 超时")
            time.sleep(0.01)
        process.kill()
        process.communicate(timeout=10)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=10)

    partial_path = output_dir / "facts.ndjson.partial"
    assert process.returncode != 0
    assert partial_path.is_file()
    assert not (output_dir / "facts.ndjson").exists()
    assert not (output_dir / "manifest.json.partial").exists()
    assert not (output_dir / "manifest.json").exists()

    summary = validate_facts_file(partial_path, expected_run_id=RUN_ID)
    assert summary.record_count == EVENT_COUNT
    assert summary.first_sequence == 1
    assert summary.last_sequence == EVENT_COUNT
    assert summary.sha256 == hashlib.sha256(partial_path.read_bytes()).hexdigest()
