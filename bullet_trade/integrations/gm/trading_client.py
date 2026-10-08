"""GM 交易 worker 生命周期和串行 IPC；不重试写请求。"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import threading
import uuid


class GmTradingError(RuntimeError):
    """脱敏交易会话错误。"""


class GmTradingClient:
    def __init__(self, config):
        self.config = dict(config)
        self.process = None
        self._lock = threading.RLock()
        self._responses = queue.Queue()
        self.timeout = float(config.get("timeout", 20))
        if not 0 < self.timeout <= 120:
            raise ValueError("GM timeout 必须在 (0, 120] 内")

    def start(self):
        with self._lock:
            if self.process is not None:
                raise GmTradingError("会话已经启动")
            env = dict(os.environ, PYTHONIOENCODING="utf-8")
            self.process = subprocess.Popen(
                [
                    self.config.get("python_executable") or sys.executable,
                    "-u",
                    "-m",
                    "bullet_trade.integrations.gm.trading_worker",
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                encoding="utf8",
                env=env,
            )
            proc = self.process
            responses = queue.Queue()
            self._responses = responses

            def reader():
                for line in proc.stdout:
                    if line.startswith("BT_GM_TRADE="):
                        try:
                            responses.put(json.loads(line.split("=", 1)[1]))
                        except ValueError:
                            responses.put(None)
                responses.put(None)

            threading.Thread(target=reader, daemon=True).start()
            try:
                self.process.stdin.write(json.dumps(self.config) + "\n")
                self.process.stdin.flush()
                self._receive("ready")
            except Exception:
                self.close()
                raise

    def _receive(self, request_id):
        try:
            value = self._responses.get(timeout=self.timeout)
        except queue.Empty:
            raise GmTradingError("GM SDK 响应超时，写请求不得自动重试") from None
        if not value or value.get("id") != request_id or not value.get("ok"):
            raise GmTradingError("GM SDK 会话或请求失败")
        return value.get("data")

    def query(self, method, **kwargs):
        with self._lock:
            if self.process is None or self.process.poll() is not None:
                raise GmTradingError("GM SDK worker 未运行")
            request_id = uuid.uuid4().hex
            try:
                self.process.stdin.write(
                    json.dumps(dict(id=request_id, method=method, **kwargs)) + "\n"
                )
                self.process.stdin.flush()
                return self._receive(request_id)
            except GmTradingError:
                self.close()
                raise
            except (OSError, ValueError):
                self.close()
                raise GmTradingError("GM SDK 通道中断") from None

    def close(self):
        with self._lock:
            proc = self.process
            if proc is None:
                return
            try:
                if proc.poll() is None:
                    proc.stdin.close()
                    proc.wait(timeout=3)
            except (OSError, subprocess.TimeoutExpired):
                proc.kill()
                proc.wait(timeout=3)
            finally:
                if proc.stdout:
                    proc.stdout.close()
                self.process = None
