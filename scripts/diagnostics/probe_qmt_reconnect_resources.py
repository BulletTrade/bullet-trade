"""作者: BruceLee。
职责: 在空白隔离目录验证真实 Windows SDK 连续连接失败时的资源稳定性。
输入: --data-dir 空目录、--attempts 次数；输出: JSON 资源采样和非零失败退出码。
上游: 发布验收；下游: QmtBroker 与 XtQuantTrader；不连接真实账户或调用订单接口。
约定: 仅 Windows，目录不得包含已有文件；进程退出后由调用方删除测试目录。
"""

import argparse
import ctypes
import ctypes.wintypes as wintypes
import json
import os
from pathlib import Path

from bullet_trade.broker.qmt import QmtBroker


def resource_sample(data_dir):
    """输入隔离目录，返回本进程句柄和队列文件统计；只读 Windows 计数器。"""
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    kernel.GetProcessHandleCount.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    handles = wintypes.DWORD()
    if not kernel.GetProcessHandleCount(kernel.GetCurrentProcess(), ctypes.byref(handles)):
        raise ctypes.WinError(ctypes.get_last_error())
    queues = list(data_dir.glob("down_queue_*"))
    return {
        "handles": handles.value,
        "queue_count": len(queues),
        "queue_bytes": sum(p.stat().st_size for p in queues),
    }


def main():
    """读取参数验证真实 SDK 失败重试；返回零或抛异常，写入独占测试目录但不交易。"""
    parser = argparse.ArgumentParser(description="隔离验证 QMT 失败重连资源")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--attempts", type=int, default=10)
    args = parser.parse_args()
    if os.name != "nt":
        raise RuntimeError("此诊断只允许在 Windows 执行")
    path = Path(args.data_dir).resolve()
    if path.exists() and any(path.iterdir()):
        raise RuntimeError("测试目录必须为空，拒绝访问现有 QMT 数据")
    path.mkdir(parents=True, exist_ok=True)
    if args.attempts < 2:
        raise ValueError("至少重复两次")
    broker = QmtBroker(
        account_id="BT_ISOLATED_RESOURCE_PROBE", data_path=str(path), auto_subscribe=False
    )
    samples = []
    trader = None
    try:
        for attempt in range(args.attempts):
            try:
                broker.connect()
            except RuntimeError as exc:
                if "connect() 失败" not in str(exc):
                    raise
            else:
                raise RuntimeError("隔离目录意外连接成功，立即停止测试")
            if trader is None:
                trader = broker._xt_trader
            if trader is None or trader is not broker._xt_trader:
                raise AssertionError("失败重试创建了新 SDK 实例")
            sample = {"attempt": attempt + 1, **resource_sample(path)}
            samples.append(sample)
            print(json.dumps(sample), flush=True)
        if samples[-1]["queue_count"] != samples[0]["queue_count"]:
            raise AssertionError("重复失败新增了 SDK 队列文件")
        if samples[-1]["handles"] > samples[0]["handles"] + 16:
            raise AssertionError("重复失败导致句柄明显增长")
        print(
            json.dumps(
                {
                    "ok": True,
                    "attempts": args.attempts,
                    "first": samples[0],
                    "last": samples[-1],
                    "order_calls": 0,
                }
            ),
            flush=True,
        )
    finally:
        broker.disconnect()


if __name__ == "__main__":
    main()
