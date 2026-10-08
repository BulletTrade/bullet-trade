"""同花顺模拟账号的有界只读复制采样；不含交易入口。

Windows 依赖在 WindowsAdapter 实例化时才导入，纯状态逻辑可离线测试。
一次调用只发一次复制命令；超时以后出现的弹窗无法由本次调用保证清理。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import ntpath
import os
from contextlib import contextmanager
from pathlib import Path
import threading
import time
from typing import Protocol

from .table_snapshot import SCHEMAS, SnapshotError, parse_table
from .native_captcha_detector import DetectionError, detect_copy_captcha

MAX_SAMPLES = 3
_PROCESS_LOCK = threading.Lock()
_MUTEX_NAME = r'Local\BulletTrade_THS_Xiadan_Simulation_Readonly_Copy'


class SamplingError(RuntimeError):
    pass


def _mutex_wait_state(result: int) -> tuple[bool, bool]:
    """WAIT_ABANDONED 也转移所有权；须释放句柄并拒绝复用未知 GUI 状态。"""
    return result in (0, 0x80), result == 0x80


@contextmanager
def session_client_lock():
    """同一 Windows 会话中所有本客户端只读复制探针共用的互斥锁。"""
    if os.name != 'nt':
        yield
        return
    import ctypes
    from ctypes import wintypes
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateMutexW.argtypes = (wintypes.LPVOID, wintypes.BOOL, wintypes.LPCWSTR)
    kernel.CreateMutexW.restype = wintypes.HANDLE
    kernel.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.ReleaseMutex.argtypes = (wintypes.HANDLE,)
    kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
    handle = kernel.CreateMutexW(None, False, _MUTEX_NAME)
    if not handle:
        raise SamplingError('session_lock_unavailable')
    acquired = False
    try:
        acquired, abandoned = _mutex_wait_state(kernel.WaitForSingleObject(handle, 0))
        if abandoned:
            raise SamplingError('session_lock_abandoned')
        if not acquired:
            raise SamplingError('session_client_busy')
        yield
    finally:
        if acquired:
            kernel.ReleaseMutex(handle)
        kernel.CloseHandle(handle)


def classify_clipboard(kind: str, before: int, after: int, content: str | None) -> str:
    """纯逻辑层：序列前进且表头/行可解析才算新文本，不证明全量性。"""
    if after <= before:
        return 'unchanged'
    if content is None:
        return 'unknown'
    try:
        parse_table(kind, content, sequence_before=before, sequence_after=after)
    except SnapshotError:
        return 'unknown'
    return 'table'


def read_stable_clipboard(adapter: Adapter, expected_sequence: int, *,
                          attempts: int = 3, retry_interval: float = 0.02,
                          sleep=time.sleep, monotonic=time.monotonic,
                          busy_timeout: float = 1.0,
                          deadline: float | None = None) -> str | None:
    """Only OpenClipboard access-denied gets a longer bounded wait.

    Every observation must retain the original expected sequence. Other read
    failures keep the previous three-attempt policy; this never resends copy.
    """
    if (attempts < 1 or retry_interval < 0 or not math.isfinite(retry_interval)
            or not math.isfinite(busy_timeout) or busy_timeout < 0
            or (deadline is not None and not math.isfinite(deadline))):
        raise ValueError('invalid_clipboard_retry')
    busy_deadline = monotonic() + busy_timeout
    if deadline is not None:
        busy_deadline = min(busy_deadline, deadline)
    ordinary_failures = 0
    seen_busy = False
    while True:
        if adapter.clipboard_sequence() != expected_sequence:
            raise SamplingError('clipboard_sequence_changed_during_read')
        if seen_busy and monotonic() >= busy_deadline:
            raise SamplingError('clipboard_read_unavailable')
        if deadline is not None and monotonic() >= deadline:
            raise SamplingError('clipboard_read_unavailable')
        try:
            content = adapter.clipboard_text()
        except Exception as exc:
            if adapter.clipboard_sequence() != expected_sequence:
                raise SamplingError('clipboard_sequence_changed_during_read') from exc
            code = getattr(exc, 'winerror', None)
            function = getattr(exc, 'funcname', None)
            if code is None and getattr(exc, 'args', None):
                code = exc.args[0]
            if function is None and len(getattr(exc, 'args', ())) > 1:
                function = exc.args[1]
            if code == 5 and function == 'OpenClipboard':
                seen_busy = True
                remaining = busy_deadline - monotonic()
                if remaining <= 0:
                    raise SamplingError('clipboard_read_unavailable') from exc
                sleep(min(0.05, remaining))
                continue
            ordinary_failures += 1
            if ordinary_failures >= attempts:
                raise SamplingError('clipboard_read_unavailable') from exc
            pause = retry_interval
            if deadline is not None:
                pause = min(pause, max(0.0, deadline - monotonic()))
            sleep(pause)
            continue
        if adapter.clipboard_sequence() != expected_sequence:
            raise SamplingError('clipboard_sequence_changed_during_read')
        return content


class Adapter(Protocol):
    def locate(self) -> tuple[int, int]: ...
    def verify_page(self, kind: str) -> None: ...
    def clipboard_sequence(self) -> int: ...
    def clipboard_text(self) -> str | None: ...
    def captcha_dialog(self) -> int | None: ...
    def copy_once(self, grid_hwnd: int) -> None: ...
    def capture_image(self, dialog_hwnd: int, path: Path) -> None: ...
    def cancel_captcha(self, dialog_hwnd: int) -> bool: ...


def _utc_time() -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def _safe_error(exc: BaseException) -> str:
    # 不把窗口文本、剪贴板、账号或 traceback 写进公开结果。
    return type(exc).__name__


def sample_once(adapter: Adapter, kind: str, output_dir: Path, *, timeout: float = 8.0,
                poll_interval: float = 0.1, settle: float = 1.0,
                monotonic=time.monotonic, sleep=time.sleep) -> dict:
    """单次状态机。识别验证码后才允许截图/取消，清理仅作用于该句柄。"""
    with session_client_lock():
        return _sample_once_locked(adapter, kind, output_dir, timeout=timeout,
                                   poll_interval=poll_interval, settle=settle,
                                   monotonic=monotonic, sleep=sleep)


def _sample_once_locked(adapter: Adapter, kind: str, output_dir: Path, *, timeout: float,
                        poll_interval: float, settle: float, monotonic, sleep) -> dict:
    if timeout <= 0 or poll_interval <= 0 or settle < 0:
        raise ValueError('invalid_timing')
    if kind not in SCHEMAS:
        raise ValueError('unknown_table_kind')
    if not _PROCESS_LOCK.acquire(blocking=False):
        raise SamplingError('sampler_reentrant')
    result = {'started_at': _utc_time(), 'kind': kind, 'status': 'error',
              'copy_sent': False, 'clipboard_sequence_before': None,
              'clipboard_sequence_after': None, 'sha256': None,
              'dialog_closed': None}
    identified: int | None = None
    try:
        main_hwnd, grid_hwnd = adapter.locate()
        result.update(main_hwnd=main_hwnd, grid_hwnd=grid_hwnd)
        adapter.verify_page(kind)
        if adapter.captcha_dialog() is not None:
            raise SamplingError('preexisting_captcha')
        before = adapter.clipboard_sequence()
        result['clipboard_sequence_before'] = before
        adapter.copy_once(grid_hwnd)
        result['copy_sent'] = True
        start = monotonic()
        deadline = start + timeout
        first_table_at: float | None = None
        observed = 'unchanged'
        while True:
            # 弹窗优先：同一轮同时看到新表格与弹窗时先完成本次弹窗清理。
            identified = adapter.captcha_dialog()
            if identified is not None:
                result['dialog_hwnd'] = identified
                output_dir.mkdir(parents=True, exist_ok=True)
                image = output_dir / ('captcha-' + time.strftime('%Y%m%dT%H%M%S', time.gmtime())
                                      + '-' + str(time.time_ns()) + '.png')
                adapter.capture_image(identified, image)
                result.update(status='captured', image=str(image),
                              image_sha256=hashlib.sha256(image.read_bytes()).hexdigest(),
                              image_size=image.stat().st_size)
                result['sha256'] = result['image_sha256']
                dialog_to_cancel = identified
                identified = None  # 取消调用若已发送点击但抛错，不重复点击。
                result['dialog_closed'] = adapter.cancel_captcha(dialog_to_cancel)
                if not result['dialog_closed']:
                    result['status'] = 'cleanup_failed'
                break
            after = adapter.clipboard_sequence()
            result['clipboard_sequence_after'] = after
            if after > before:
                content = read_stable_clipboard(adapter, after, sleep=sleep)
                if content is not None:
                    result['clipboard_sha256'] = hashlib.sha256(content.encode('utf-8')).hexdigest()
                    result['sha256'] = result['clipboard_sha256']
                current = classify_clipboard(kind, before, after, content)
                if current == 'table':
                    observed = 'table'
                    if first_table_at is None:
                        first_table_at = monotonic()
                    # 摘要取自复制文本，不保存可能含持仓/委托的原文。
                else:
                    observed = 'unknown'
                    first_table_at = None
            now = monotonic()
            if first_table_at is not None and now - first_table_at >= settle:
                result['status'] = 'new_table'
                break
            if now >= deadline:
                result['status'] = {'unchanged': 'timeout', 'unknown': 'unknown_clipboard',
                                    'table': 'new_table'}[observed]
                break
            sleep(min(poll_interval, deadline - now))
        result['late_dialog_possible'] = result['status'] != 'captured'
    except Exception as exc:
        result['status'] = 'error'
        result['error'] = _safe_error(exc)
        if isinstance(exc, SamplingError):
            result['reason'] = str(exc)
        # 不清理已存在或身份不明弹窗。只对本次循环中确认的句柄尝试清理。
        if identified is not None:
            try:
                if adapter.captcha_dialog() == identified:
                    result['dialog_closed'] = adapter.cancel_captcha(identified)
            except Exception as cleanup_exc:
                result['cleanup_error'] = _safe_error(cleanup_exc)
        result['late_dialog_possible'] = bool(result['copy_sent'])
    finally:
        try:
            if result['copy_sent']:
                result['clipboard_sequence_after'] = adapter.clipboard_sequence()
        except Exception:
            pass
        result['finished_at'] = _utc_time()
        _PROCESS_LOCK.release()
    return result


def sample_batch(adapter: Adapter, kind: str, output_dir: Path, *, count: int = 1,
                 timeout: float = 8.0) -> list[dict]:
    """硬上限只允许 1..3 次；独占锁避免同目录跨进程并发采样。"""
    if not 1 <= count <= MAX_SAMPLES:
        raise ValueError('sample_count_out_of_bounds')
    with session_client_lock():
        return _sample_batch_locked(adapter, kind, output_dir, count=count,
                                    timeout=timeout)


def _sample_batch_locked(adapter: Adapter, kind: str, output_dir: Path, *, count: int,
                         timeout: float) -> list[dict]:
    output_dir.mkdir(parents=True, exist_ok=True)
    lock_path = output_dir / '.readonly_sampler.lock'
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise SamplingError('sampler_already_running_or_stale_lock') from exc
    results = []
    try:
        with os.fdopen(fd, 'w', encoding='ascii') as lock_file:
            lock_file.write(str(os.getpid()))
        for _ in range(count):
            result = sample_once(adapter, kind, output_dir, timeout=timeout)
            results.append(result)
            if result['status'] != 'captured' and result['status'] != 'new_table':
                break
        return results
    finally:
        lock_path.unlink(missing_ok=True)


class WindowsAdapter:
    """Bind the explicitly configured client executable and account mask."""
    def __init__(self, *, account: str, client_exe: str | None = None):
        if not isinstance(account, str) or not account:
            raise SamplingError('account_mask_required')
        if (not isinstance(client_exe, str) or not client_exe
                or not ntpath.isabs(client_exe)
                or ntpath.basename(client_exe).casefold() != 'xiadan.exe'):
            raise SamplingError('client_exe_required')
        from pywinauto import Application
        import psutil
        import win32con
        import win32gui
        import win32process
        import win32clipboard
        import ctypes
        # Bind the exact configured process and its observed main HWND.
        expected_exe = ntpath.normcase(ntpath.normpath(client_exe))
        clients = [process for process in psutil.process_iter(['pid', 'name', 'exe'])
                   if (process.info.get('name') or '').casefold() == 'xiadan.exe'
                   and ntpath.normcase(ntpath.normpath(process.info.get('exe') or ''))
                   == expected_exe]
        if len(clients) != 1:
            raise SamplingError('simulation_client_not_unique')
        mains = []
        def collect_main(hwnd, _):
            if (win32gui.IsWindowVisible(hwnd)
                    and win32gui.GetWindowText(hwnd) == '网上股票交易系统5.0'):
                _, pid = win32process.GetWindowThreadProcessId(hwnd)
                if pid == clients[0].pid:
                    mains.append(hwnd)
        win32gui.EnumWindows(collect_main, None)
        if len(mains) != 1:
            raise SamplingError('simulation_main_not_unique')
        self.app = Application(backend='win32').connect(handle=mains[0])
        if self.app.process != clients[0].pid:
            raise SamplingError('simulation_client_identity_changed')
        self.account = account
        self.con = win32con
        self.gui = win32gui
        self.process = win32process
        self.clipboard = win32clipboard
        self.user32 = ctypes.windll.user32

    def _visible_query_tree(self, main_hwnd: int):
        """The client has both a visible and a hidden ID-129 TreeView."""
        handles = []
        def collect(hwnd, _):
            if (self.gui.GetDlgCtrlID(hwnd) == 129
                    and self.gui.GetClassName(hwnd) == 'SysTreeView32'
                    and self.gui.IsWindowVisible(hwnd)):
                handles.append(hwnd)
        self.gui.EnumChildWindows(main_hwnd, collect, None)
        if len(handles) != 1:
            raise SamplingError('visible_query_tree_not_unique')
        return self.app.window(handle=handles[0]).wrapper_object()

    def _windows(self):
        """从原始 HWND 枚举，避免 pywinauto.windows() 整批包装失效句柄。"""
        handles = []
        def collect(hwnd, _):
            if not self.gui.IsWindow(hwnd) or not self.gui.IsWindowVisible(hwnd):
                return
            _, pid = self.process.GetWindowThreadProcessId(hwnd)
            if pid == self.app.process:
                handles.append(hwnd)
        self.gui.EnumWindows(collect, None)
        windows = []
        for hwnd in handles:
            if not self.gui.IsWindow(hwnd):
                continue
            try:
                if (self.gui.GetWindowText(hwnd) != '网上股票交易系统5.0'
                        and self.gui.GetClassName(hwnd) != '#32770'):
                    continue
                windows.append(self.app.window(handle=hwnd).wrapper_object())
            except Exception as exc:
                if not self.gui.IsWindow(hwnd):
                    continue
                raise SamplingError('window_wrapper_failed') from exc
        return windows

    def locate_main(self, *, known_dialog_handles: frozenset[int] = frozenset()) -> int:
        """Locate the bound account even on an information page with no grid."""
        mains = []
        other_dialog = False
        account_unverified = False
        for window in self._windows():
            try:
                if not self.gui.IsWindow(window.handle):
                    continue
                if not window.is_visible():
                    continue
                if self.gui.GetWindowText(window.handle) != '网上股票交易系统5.0':
                    if window.handle not in known_dialog_handles:
                        other_dialog = True
                    continue
            except Exception as exc:
                if not self.gui.IsWindow(window.handle):
                    continue
                raise SamplingError('main_window_detection_failed') from exc
            try:
                combo = self.app.window(handle=window.handle).child_window(
                    control_id=2322, class_name='ComboBox').wrapper_object()
                if combo.selected_text() == self.account:
                    mains.append(window)
                else:
                    account_unverified = True
            except Exception:
                if self.gui.IsWindow(window.handle):
                    account_unverified = True
        if len(mains) > 1:
            raise SamplingError('simulation_window_not_unique')
        if account_unverified:
            raise SamplingError('simulation_account_unverified')
        if not mains:
            raise SamplingError('simulation_window_state_unverified' if other_dialog
                                else 'simulation_window_missing')
        self._main_handle = mains[0].handle
        return mains[0].handle

    def locate(self, *, known_dialog_handles: frozenset[int] = frozenset()) -> tuple[int, int]:
        self._last_query_grid_count = None
        main = self.app.window(handle=self.locate_main(known_dialog_handles=known_dialog_handles))
        grids = [g for g in main.descendants() if g.control_id() == 1047
                 and g.class_name() == 'CVirtualGridCtrl' and g.is_visible()]
        # Only the count is retained; no account, row or window text escapes.
        self._last_query_grid_count = len(grids)
        if len(grids) != 1:
            raise SamplingError('visible_query_grid_not_unique')
        self._main_handle = main.handle
        return main.handle, grids[0].handle

    def verify_page(self, kind: str) -> None:
        # 2026-09-28 只读现场探针确认查询树 ID 129 的唯一选中项为“资金股票”。
        # 其余页面尚无相同口径的现场证据，保持拒绝。
        if kind != 'holdings':
            raise SamplingError(f'{kind}_page_identity_unverified')
        main_hwnd = getattr(self, '_main_handle', None)
        if main_hwnd is None:
            raise SamplingError('main_window_not_bound')
        try:
            tree = self._visible_query_tree(main_hwnd)
            selected = []
            def walk(nodes, depth=0):
                if depth > 5 or len(selected) > 1:
                    return
                for node in nodes:
                    if node.is_selected():
                        selected.append(node.text())
                    walk(node.children(), depth + 1)
            walk(tree.roots())
            if selected != ['资金股票']:
                raise SamplingError('holdings_page_identity_mismatch')
        except SamplingError:
            raise
        except Exception as exc:
            raise SamplingError('holdings_page_identity_unverified') from exc

    def clipboard_sequence(self) -> int:
        return int(self.user32.GetClipboardSequenceNumber())

    def clipboard_text(self) -> str | None:
        self.clipboard.OpenClipboard()
        try:
            if not self.clipboard.IsClipboardFormatAvailable(self.con.CF_UNICODETEXT):
                return None
            return self.clipboard.GetClipboardData(self.con.CF_UNICODETEXT)
        finally:
            self.clipboard.CloseClipboard()

    def clipboard_from_client(self, main_hwnd: int) -> bool:
        import ctypes
        from ctypes import wintypes
        self.user32.GetClipboardOwner.restype = wintypes.HWND
        owner = self.user32.GetClipboardOwner()
        if not owner or main_hwnd != getattr(self, '_main_handle', None):
            return False
        try:
            _, owner_pid = self.process.GetWindowThreadProcessId(owner)
            _, main_pid = self.process.GetWindowThreadProcessId(main_hwnd)
            root = self.gui.GetAncestor(owner, self.con.GA_ROOT)
            owner_class = self.gui.GetClassName(owner)
            # 现场复制后的 owner 是同一 xiadan PID 的独立隐藏窗口
            # CLIPBRDWNDCLASS（2026-09-28 只读探针），不是主窗的子窗口。
            return owner_pid == main_pid and (
                owner == main_hwnd or root == main_hwnd or owner_class == 'CLIPBRDWNDCLASS')
        except Exception:
            return False

    def captcha_dialog(self) -> int | None:
        main_hwnd = getattr(self, '_main_handle', None)
        if main_hwnd is None:
            raise SamplingError('main_window_not_bound')
        try:
            return detect_copy_captcha(
                self.gui, self.process, self.con, main_hwnd=main_hwnd,
                client_pid=self.app.process,
            )
        except DetectionError as exc:
            raise SamplingError(str(exc)) from exc
        except Exception as exc:
            raise SamplingError('captcha_detection_failed') from exc

    def _confirmed_dialog(self, handle: int):
        if self.captcha_dialog() != handle:
            raise SamplingError('captcha_identity_changed')
        return self.app.window(handle=handle)

    def copy_once(self, grid_hwnd: int) -> None:
        self.gui.PostMessage(grid_hwnd, self.con.WM_COMMAND, 0xE122, 0)

    def capture_image(self, dialog_hwnd: int, path: Path) -> None:
        image = self._confirmed_dialog(dialog_hwnd).child_window(control_id=2405).wrapper_object()
        image.capture_as_image().save(path)

    def cancel_captcha(self, dialog_hwnd: int) -> bool:
        dialog = self._confirmed_dialog(dialog_hwnd)
        button = dialog.child_window(control_id=2, class_name='Button').wrapper_object()
        button.click_input()
        for _ in range(20):
            if not dialog.exists() or not dialog.is_visible():
                return True
            time.sleep(0.1)
        # click_input may be ignored on a disconnected desktop. IDCANCEL is
        # restricted to the same copy-CAPTCHA HWND after another identity check.
        if self.captcha_dialog() != dialog_hwnd:
            return False
        import ctypes
        from ctypes import wintypes
        user32 = ctypes.WinDLL('user32', use_last_error=True)
        user32.PostMessageW.argtypes = (wintypes.HWND, wintypes.UINT,
                                       wintypes.WPARAM, wintypes.LPARAM)
        user32.PostMessageW.restype = wintypes.BOOL
        if not user32.PostMessageW(dialog_hwnd, self.con.WM_COMMAND, 2, 0):
            return False
        for _ in range(20):
            if not self.gui.IsWindow(dialog_hwnd) or not self.gui.IsWindowVisible(dialog_hwnd):
                return True
            time.sleep(0.1)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=('holdings', 'orders', 'trades', 'cancelable'))
    parser.add_argument('output_dir', type=Path)
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--timeout', type=float, default=8.0)
    parser.add_argument('--account-mask', required=True)
    parser.add_argument('--client-exe', required=True)
    args = parser.parse_args()
    results = sample_batch(WindowsAdapter(account=args.account_mask,
                                          client_exe=args.client_exe), args.kind, args.output_dir,
                           count=args.count, timeout=args.timeout)
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0 if all(item['status'] in {'captured', 'new_table'} for item in results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
