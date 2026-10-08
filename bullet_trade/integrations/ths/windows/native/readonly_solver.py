"""模拟账户只读复制验证码受理探针；没有交易、撤单或登录入口。

``client_accepted`` 只表示本次候选使复制查询完成，不是人工标注真值。
调用结束后的迟到弹窗必须由上层另行检查。
"""
from __future__ import annotations

import hashlib
import math
import os
from pathlib import Path
import re
import time
import traceback
from typing import Protocol

from .readonly_sampler import (SamplingError, WindowsAdapter, _PROCESS_LOCK,
                              _safe_error, _utc_time, read_stable_clipboard,
                              session_client_lock)
from .captcha_roi import check_roi_png
from .table_snapshot import SCHEMAS, SnapshotError, matching_page_kinds, parse_table
from .grid_scroll_evidence import scroll_corroboration


class CandidateProvider(Protocol):
    def recognize(self, image: bytes) -> str: ...


class SolverAdapter(Protocol):
    def locate(self, *, known_dialog_handles: frozenset[int] = frozenset()) -> tuple[int, int]: ...
    def dialog_fingerprint(self, main_hwnd: int) -> frozenset[tuple[int, int, int, str]]: ...
    def verify_page(self, kind: str) -> None: ...
    def clipboard_sequence(self) -> int: ...
    def clipboard_text(self) -> str | None: ...
    def clipboard_from_client(self, main_hwnd: int) -> bool: ...
    def captcha_dialog(self) -> int | None: ...
    def copy_once(self, grid_hwnd: int) -> None: ...
    def capture_image(self, dialog_hwnd: int, path: Path) -> None: ...
    def read_captcha_edit(self, dialog_hwnd: int) -> str: ...
    def write_captcha_edit(self, dialog_hwnd: int, value: str) -> None: ...
    def confirm_captcha(self, dialog_hwnd: int) -> None: ...
    def cancel_captcha(self, dialog_hwnd: int) -> bool: ...


class SolverWindowsAdapter(WindowsAdapter):
    """仅扩展采样器已经确认身份的复制验证码弹窗控件。"""

    PAGE_PATHS = {
        'holdings': ['查询[F4]', '资金股票'],
        'orders': ['查询[F4]', '当日委托'],
        'trades': ['查询[F4]', '当日成交'],
        'cancelable': ['撤单[F3]'],
    }
    # Client-specific rendering quiet period. Tests and other adapters without
    # this profile setting retain zero wait; an instance may override with 0.
    post_confirm_quiet_seconds = 2.0

    def grid_scroll_state(self, main_hwnd: int, grid_hwnd: int):
        from .grid_scroll_evidence import read_grid_scroll
        return read_grid_scroll(grid_hwnd, main_hwnd)

    def verify_table_page_identity(self, kind: str, main_hwnd: int, grid_hwnd: int) -> bool:
        """Disambiguate shared headers using freshly observed page controls."""
        from .query_page_controls import verify_native_query_page
        return verify_native_query_page(self, kind, main_hwnd, grid_hwnd)

    def _cursor_input_ready(self) -> bool:
        """Probe the actual input capability before pywinauto uses mouse focus.

        Setting the cursor to its current point has no intended UI effect;
        failure stops navigation before a blocking or late GUI action.
        """
        if os.name != 'nt':
            return True  # Only the Windows adapter executes live GUI actions.
        try:
            import ctypes
            from ctypes import wintypes
            user32 = ctypes.WinDLL('user32', use_last_error=True)
            user32.GetCursorPos.argtypes = (ctypes.POINTER(wintypes.POINT),)
            user32.GetCursorPos.restype = wintypes.BOOL
            user32.SetCursorPos.argtypes = (ctypes.c_int, ctypes.c_int)
            user32.SetCursorPos.restype = wintypes.BOOL
            point = wintypes.POINT()
            return (bool(user32.GetCursorPos(ctypes.byref(point)))
                    and bool(user32.SetCursorPos(point.x, point.y)))
        except Exception:
            return False

    def dialog_fingerprint(self, main_hwnd: int) -> frozenset[tuple[int, int, int, str]]:
        """记录无主辅助窗；主窗拥有的未知对话框一律停下。"""
        return self._dialog_fingerprint(main_hwnd)

    def post_confirm_dialog_fingerprint(
            self, main_hwnd: int, captcha_hwnd: int | None
    ) -> frozenset[tuple[int, int, int, str]]:
        """确认后仅略过仍由原身份检查确认的本次验证码弹窗。"""
        return self._dialog_fingerprint(main_hwnd, allowed_captcha_hwnd=captcha_hwnd)

    def _dialog_fingerprint(
            self, main_hwnd: int, *, allowed_captcha_hwnd: int | None = None
    ) -> frozenset[tuple[int, int, int, str]]:
        dialogs = set()
        known_secondary_seen = 0
        for window in self._windows():
            try:
                if window.handle == main_hwnd:
                    continue
                if self.gui.IsWindow(window.handle) and window.is_visible() \
                        and window.class_name() == '#32770':
                    owner = self.gui.GetWindow(window.handle, self.con.GW_OWNER)
                    root_owner = self.gui.GetAncestor(window.handle, self.con.GA_ROOTOWNER)
                    if main_hwnd in (owner, root_owner):
                        if (window.handle == allowed_captcha_hwnd
                                and self.captcha_dialog() == allowed_captcha_hwnd):
                            continue
                        raise SamplingError('unexpected_main_dialog')
                    title = self.gui.GetWindowText(window.handle)
                    children = [child for child in window.descendants() if child.is_visible()]
                    # 现场只读普查确认 xiadan 同进程还会显示一个独立的
                    # “专业版下单”顶层窗口。它不是复制验证码，也不属于主窗。
                    # 只在无 owner 且业务窗口身份稳定时将其纳入基线指纹；
                    # 本页复制命令发送至已核验网格 HWND，不依赖前台焦点。
                    # 任何窗口集合变化仍由调用方停下。
                    known_secondary = (title == '专业版下单' and owner == 0
                                       and root_owner == window.handle)
                    if known_secondary:
                        known_secondary_seen += 1
                        if known_secondary_seen > 1:
                            raise SamplingError('secondary_window_not_unique')
                    if (any(child.class_name() in {'Button', 'Edit'} for child in children)
                            or any(marker in text for text in [title] +
                                   [child.window_text() for child in children]
                                   for marker in ('验证码', '登录', '密码'))) and not known_secondary:
                        # Diagnostics are best effort; an incomplete wrapper
                        # must not replace the original unknown-dialog stop.
                        try:
                            self._last_auxiliary_diagnostic = {
                                'hwnd': window.handle, 'owner': owner, 'root_owner': root_owner,
                                'title_is_known_secondary': title == '专业版下单',
                                'title_length': len(title),
                                'visible_child_shapes': [
                                    {'class': child.class_name(), 'id': child.control_id(),
                                     'has_sensitive_marker': any(marker in child.window_text()
                                         for marker in ('验证码', '登录', '密码'))}
                                    for child in children],
                            }
                        except Exception:
                            self._last_auxiliary_diagnostic = {
                                'hwnd': window.handle, 'owner': owner,
                                'root_owner': root_owner, 'diagnostic_unavailable': True,
                            }
                        raise SamplingError('unrecognized_auxiliary_dialog')
                    dialogs.add((window.handle, owner, root_owner, title))
            except SamplingError:
                raise
            except Exception as exc:
                raise SamplingError('dialog_inventory_unverified') from exc
        return frozenset(dialogs)

    def select_page(self, kind: str) -> None:
        """仅选中已知查询/撤单页；委托页先设为“全部”。"""
        if kind not in self.PAGE_PATHS:
            raise SamplingError('query_page_unknown')
        try:
            main_hwnd, _ = self.locate()  # account and unique current grid
        except SamplingError as exc:
            raise SamplingError('select_initial:' + str(exc)) from exc
        baseline_dialogs = self.dialog_fingerprint(main_hwnd)
        known_dialog_handles = frozenset(item[0] for item in baseline_dialogs)
        if not self._cursor_input_ready():
            raise SamplingError('input_desktop_unavailable')
        main = self.app.window(handle=main_hwnd)
        tree = self._visible_query_tree(main_hwnd)
        tree.get_item(self.PAGE_PATHS[kind]).select()
        observation_deadline = time.monotonic() + 3.0
        def verify_location() -> None:
            while True:
                observed_dialogs = self.dialog_fingerprint(main_hwnd)
                if observed_dialogs != baseline_dialogs:
                    raise SamplingError('query_dialog_changed')
                try:
                    located = self.locate(known_dialog_handles=known_dialog_handles)
                except SamplingError as exc:
                    if (str(exc) != 'visible_query_grid_not_unique'
                            or getattr(self, '_last_query_grid_count', None) != 0):
                        raise
                    if getattr(self, '_main_handle', None) != main_hwnd:
                        raise SamplingError('query_identity_changed') from exc
                    remaining = observation_deadline - time.monotonic()
                    if remaining <= 0:
                        raise
                    time.sleep(min(0.1, remaining))
                    continue
                if located[0] != main_hwnd:
                    raise SamplingError('query_identity_changed')
                return
        try:
            verify_location()
        except SamplingError as exc:
            raise SamplingError('select_after_tree:' + str(exc)) from exc
        if kind == 'orders':
            def visible_filters():
                return [w for w in main.descendants() if w.control_id() == 2410
                        and w.class_name() == 'ComboBox' and w.is_visible()]
            # 树的选中状态可先于中央内容变化；只等待可观察的筛选控件，
            # 不用固定时长冒充页面就绪。其它页的内容身份由复制表头再核验。
            filter_deadline = time.monotonic() + 6.0
            filters = visible_filters()
            while not filters:
                remaining = filter_deadline - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(0.1, remaining))
                verify_location()
                filters = visible_filters()
            if len(filters) != 1 or '全部' not in filters[0].item_texts():
                raise SamplingError('orders_all_filter_unavailable')
            if not self._cursor_input_ready():
                raise SamplingError('input_desktop_unavailable')
            filters[0].select('全部')
        verify_location()
        self.verify_page(kind)

    def verify_page(self, kind: str) -> None:
        if kind not in self.PAGE_PATHS:
            raise SamplingError('query_page_unknown')
        main_hwnd = getattr(self, '_main_handle', None)
        if main_hwnd is None:
            raise SamplingError('main_window_not_bound')
        try:
            main = self.app.window(handle=main_hwnd)
            tree = self._visible_query_tree(main_hwnd)
            if not tree.get_item(self.PAGE_PATHS[kind]).is_selected():
                raise SamplingError('query_page_identity_mismatch')
            if kind == 'orders':
                filters = [w for w in main.descendants() if w.control_id() == 2410
                           and w.class_name() == 'ComboBox' and w.is_visible()]
                if len(filters) != 1 or filters[0].selected_text() != '全部':
                    raise SamplingError('orders_filter_not_all')
        except SamplingError:
            raise
        except Exception as exc:
            raise SamplingError('query_page_identity_unverified') from exc

    def capture_image(self, dialog_hwnd: int, path: Path) -> None:
        super().capture_image(dialog_hwnd, path)
        quality = check_roi_png(path.read_bytes())
        if not quality.ok:
            raise SamplingError('captcha_roi_' + quality.reason)

    def _edit(self, dialog_hwnd: int):
        return self._confirmed_dialog(dialog_hwnd).child_window(
            control_id=2404, class_name='Edit').wrapper_object()

    def read_captcha_edit(self, dialog_hwnd: int) -> str:
        # GetWindowText cannot retrieve another process's Edit text. Read the
        # Edit line itself, as in the frozen version that passed on this client.
        return self._edit(dialog_hwnd).get_line(0)

    def edit_diagnostic(self, dialog_hwnd: int) -> dict:
        """Only lengths/identity are exported; never the CAPTCHA contents."""
        import ctypes
        from ctypes import wintypes
        edit = self._edit(dialog_hwnd)
        hwnd = edit.handle
        user32 = ctypes.WinDLL('user32', use_last_error=True)
        user32.SendMessageTimeoutW.argtypes = (
            wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM,
            wintypes.UINT, wintypes.UINT, ctypes.POINTER(ctypes.c_size_t))
        user32.SendMessageTimeoutW.restype = wintypes.LPARAM
        buffer = ctypes.create_unicode_buffer(32)
        output = ctypes.c_size_t()
        sent = bool(user32.SendMessageTimeoutW(
            hwnd, 0x000D, len(buffer), ctypes.cast(buffer, ctypes.c_void_p).value,
            0x0002, 500, ctypes.byref(output)))
        return {'same_edit_hwnd': hwnd == getattr(self, '_last_edit_hwnd', None),
                'native_wm_gettext_observed': sent,
                'native_wm_gettext_length': len(buffer.value) if sent else None}

    def write_captcha_edit(self, dialog_hwnd: int, value: str) -> None:
        from .captcha_focus_input import write_captcha_with_focus
        edit = self._edit(dialog_hwnd)
        self._last_edit_hwnd = edit.handle
        self._focus_input_diagnostic = {}
        # Direct Edit messages are rejected by this client. Prove the real
        # input-queue focus before each key; the solver still checks readback.
        write_captcha_with_focus(
            edit.handle, dialog_hwnd, self.app.process, value,
            is_current=lambda hwnd: self.captcha_dialog() == hwnd,
            diagnostic=self._focus_input_diagnostic)

    def confirm_captcha(self, dialog_hwnd: int) -> None:
        dialog = self._confirmed_dialog(dialog_hwnd)
        button = dialog.child_window(control_id=1, class_name='Button').wrapper_object()
        if not button.is_visible() or button.window_text() not in {'确定', '确认', 'OK'}:
            raise SamplingError('captcha_confirm_not_identified')
        button.click_input()


def solve_once(adapter: SolverAdapter, candidate_provider: CandidateProvider, kind: str,
               output_dir: Path, *, timeout: float = 20.0, poll_interval: float = 0.1,
               settle: float = 1.0, monotonic=time.monotonic, sleep=time.sleep,
               cohort=None) -> dict:
    """一次复制和至多一次验证码提交；所有成功路径都要通过新表格解析。"""
    with session_client_lock():
        return _solve_once_locked(adapter, candidate_provider, kind, output_dir,
                                  timeout=timeout, poll_interval=poll_interval,
                                  settle=settle, monotonic=monotonic, sleep=sleep,
                                  cohort=cohort)


def solve_with_retries(adapter: SolverAdapter, candidate_provider: CandidateProvider,
                       kind: str, output_dir: Path, *, max_attempts: int = 2,
                       total_timeout: float = 60.0, attempt_timeout: float = 20.0,
                       quiet_seconds: float = 3.0, poll_interval: float = 0.1,
                       settle: float = 1.0, monotonic=time.monotonic,
                       sleep=time.sleep) -> dict:
    """只读复制挑战的有限刷新；只重试已清理的明确拒绝/无效候选。

    每轮重新复制以获取新挑战，不复用旧截图。未知客户端状态绝不重放。
    返回的 attempts 是私有证据，可能包含四位提交值及本地图像路径。
    """
    if type(max_attempts) is not int or not 1 <= max_attempts <= 3:
        raise ValueError('max_attempts_out_of_bounds')
    if total_timeout <= 0 or attempt_timeout <= 0 or quiet_seconds <= 0:
        raise ValueError('invalid_retry_timing')
    deadline = monotonic() + total_timeout
    attempts = []
    stop_reason = None
    retry_error = None
    with session_client_lock():
        if not _PROCESS_LOCK.acquire(blocking=False):
            raise SamplingError('sampler_reentrant')
        try:
            for index in range(max_attempts):
                remaining = deadline - monotonic()
                if remaining <= 0:
                    stop_reason = 'deadline'
                    break
                result = _solve_once_locked(
                    adapter, candidate_provider, kind,
                    output_dir / f'attempt-{index + 1:02d}',
                    timeout=min(attempt_timeout, remaining),
                    poll_interval=poll_interval, settle=settle,
                    monotonic=monotonic, sleep=sleep, process_lock_held=True)
                attempts.append(result)
                if result['status'] not in {'rejected', 'invalid_candidate'}:
                    break
                if result.get('dialog_closed') is not True or result.get('cleanup_error'):
                    stop_reason = 'dialog_cleanup_failed'
                    break
                if index + 1 == max_attempts:
                    stop_reason = 'attempt_limit'
                    break
                remaining = deadline - monotonic()
                if remaining < quiet_seconds:
                    stop_reason = 'deadline'
                    break
                # 复制命令可能导致迟到弹窗。静默观察期间保持两层客户端锁。
                try:
                    if adapter.captcha_dialog() is not None:
                        stop_reason = 'late_dialog'
                        break
                    sleep(quiet_seconds)
                    if adapter.captcha_dialog() is not None:
                        stop_reason = 'late_dialog'
                        break
                except Exception as exc:
                    stop_reason = 'retry_state_unverified'
                    retry_error = _safe_error(exc)
                    break
            if not attempts:
                return {'status': 'retry_deadline_exhausted', 'attempt_count': 0,
                        'attempts': [], 'client_accepted': None}
            final = dict(attempts[-1])
            final['attempt_count'] = len(attempts)
            final['attempts'] = attempts
            if stop_reason is not None:
                final['retry_stop_reason'] = stop_reason
            if retry_error is not None:
                final['retry_error'] = retry_error
            if stop_reason == 'dialog_cleanup_failed':
                final['status'] = 'cleanup_pending'
            elif final['status'] in {'rejected', 'invalid_candidate'} and final.get('dialog_closed') is True:
                if stop_reason == 'attempt_limit':
                    final['status'] = 'retry_exhausted'
                elif stop_reason == 'deadline':
                    final['status'] = 'retry_deadline_exhausted'
                elif stop_reason == 'late_dialog':
                    final['status'] = 'cleanup_pending'
                elif stop_reason == 'retry_state_unverified':
                    final['status'] = 'error'
                    final['reason'] = stop_reason
            return final
        finally:
            _PROCESS_LOCK.release()


def _initial_copy_dialog_observation(adapter: SolverAdapter, kind: str,
                                     main_hwnd: int, grid_hwnd: int,
                                     known_dialog_handles: frozenset[int], *,
                                     deadline: float, poll_interval: float,
                                     monotonic, sleep, phase) -> int | None:
    """Reobserve only a briefly hidden original main after the first copy.

    This path sends no copy, CAPTCHA, window activation or display command.
    All non-transient identity and dialog errors retain their normal stop.
    """
    try:
        return adapter.captcha_dialog()
    except SamplingError as first_error:
        if str(first_error) != 'main_window_not_bound':
            raise
        original_error = first_error
    reobserve_deadline = min(deadline, monotonic() + 1.0)
    while True:
        remaining = reobserve_deadline - monotonic()
        if remaining <= 0:
            raise original_error
        sleep(min(poll_interval, 0.1, remaining))
        if monotonic() >= reobserve_deadline:
            raise original_error
        try:
            located = adapter.locate(known_dialog_handles=known_dialog_handles)
        except SamplingError as exc:
            if str(exc) == 'simulation_window_missing':
                continue
            raise
        if located != (main_hwnd, grid_hwnd):
            raise SamplingError('query_identity_changed')
        adapter.verify_page(kind)
        try:
            detected = adapter.captcha_dialog()
        except SamplingError as exc:
            if str(exc) == 'main_window_not_bound':
                continue
            raise
        phase('main_reobserved')
        return detected


def _post_confirm_dialog_observation(adapter: SolverAdapter, kind: str,
                                     main_hwnd: int, grid_hwnd: int,
                                     known_dialog_handles: frozenset[int],
                                     identified: int, baseline_dialogs, *,
                                     deadline: float, poll_interval: float,
                                     monotonic, sleep, phase) -> tuple[int | None, bool]:
    """Only a bound native main briefly hidden after confirmed CAPTCHA may reappear."""
    try:
        return adapter.captcha_dialog(), False
    except SamplingError as original_error:
        if str(original_error) != 'main_window_not_bound':
            raise
        hidden_error = original_error

    def original_main_is_visible():
        if not isinstance(adapter, SolverWindowsAdapter):
            raise hidden_error
        try:
            gui = adapter.gui
            if (getattr(adapter, '_main_handle', None) != main_hwnd
                    or not gui.IsWindow(main_hwnd)
                    or gui.GetWindowText(main_hwnd) != '网上股票交易系统5.0'
                    or adapter.process.GetWindowThreadProcessId(main_hwnd)[1]
                    != adapter.app.process):
                raise SamplingError('query_identity_changed')
            return bool(gui.IsWindowVisible(main_hwnd))
        except SamplingError:
            raise
        except Exception as exc:
            raise SamplingError('simulation_window_state_unverified') from exc

    immediately_visible = original_main_is_visible()
    reobserve_deadline = min(deadline, monotonic() + 1.0)
    while True:
        remaining = reobserve_deadline - monotonic()
        if remaining <= 0:
            raise hidden_error
        if not immediately_visible:
            sleep(min(poll_interval, 0.1, remaining))
        immediately_visible = False
        if monotonic() >= reobserve_deadline:
            raise hidden_error
        try:
            located = adapter.locate(
                known_dialog_handles=known_dialog_handles | frozenset({identified}))
        except SamplingError as exc:
            if str(exc) == 'simulation_window_missing':
                original_main_is_visible()
                continue
            raise
        if located != (main_hwnd, grid_hwnd):
            raise SamplingError('query_identity_changed')
        adapter.verify_page(kind)
        try:
            detected = adapter.captcha_dialog()
        except SamplingError as exc:
            if str(exc) == 'main_window_not_bound':
                original_main_is_visible()
                continue
            raise
        if detected not in (None, identified):
            raise SamplingError('captcha_identity_changed')
        if adapter.post_confirm_dialog_fingerprint(main_hwnd, detected) != baseline_dialogs:
            raise SamplingError('post_submit_dialog_changed')
        phase('post_confirm_main_reobserved')
        return detected, True


def _solve_once_locked(adapter: SolverAdapter, candidate_provider: CandidateProvider,
                       kind: str, output_dir: Path, *, timeout: float,
                       poll_interval: float, settle: float, monotonic, sleep,
                       cohort=None, process_lock_held: bool = False) -> dict:
    if kind not in SCHEMAS:
        raise ValueError('unknown_table_kind')
    if timeout <= 0 or poll_interval <= 0 or settle < 0:
        raise ValueError('invalid_timing')
    post_confirm_quiet = getattr(adapter, 'post_confirm_quiet_seconds', 0.0)
    if (isinstance(post_confirm_quiet, bool)
            or not isinstance(post_confirm_quiet, (int, float))
            or not math.isfinite(post_confirm_quiet) or post_confirm_quiet < 0):
        raise ValueError('invalid_post_confirm_quiet_seconds')
    if not process_lock_held and not _PROCESS_LOCK.acquire(blocking=False):
        raise SamplingError('sampler_reentrant')
    result = {'started_at': _utc_time(), 'kind': kind, 'status': 'error',
              'copy_sent': False, 'submitted': None, 'client_accepted': None,
              'query_page_identity': 'unknown', 'query_complete': None,
              'clipboard_sequence_before': None, 'clipboard_sequence_after': None,
              'clipboard_sequence_submit_pre': None, 'dialog_closed': None,
              'late_dialog_possible': False, 'phase_seconds': {}}
    started = monotonic()
    def phase(name: str) -> None:
        result['phase_seconds'][name] = round(monotonic() - started, 3)
    identified: int | None = None
    submitted = False
    try:
        main_hwnd, grid_hwnd = adapter.locate()
        phase('window_located')
        result.update(main_hwnd=main_hwnd, grid_hwnd=grid_hwnd)
        adapter.verify_page(kind)
        phase('page_verified')
        scroll_reader = getattr(adapter, 'grid_scroll_state', None)
        scroll_before = scroll_reader(main_hwnd, grid_hwnd) if scroll_reader else None
        result['grid_scroll_before'] = (vars(scroll_before) if scroll_before else None)
        if adapter.captcha_dialog() is not None:
            raise SamplingError('preexisting_captcha')
        baseline_dialogs = adapter.dialog_fingerprint(main_hwnd)
        known_dialog_handles = frozenset(item[0] for item in baseline_dialogs)
        before = adapter.clipboard_sequence()
        result['clipboard_sequence_before'] = before
        adapter.copy_once(grid_hwnd)
        phase('copy_sent')
        result['copy_sent'] = True
        deadline = monotonic() + timeout
        pre_challenge_reobserve_deadline = min(deadline, monotonic() + 3.0)
        post_confirm_reobserve_deadline: float | None = None
        first_table_at: float | None = None

        def observe_after_confirm():
            nonlocal first_table_at
            if (post_confirm_reobserve_deadline is None
                    or monotonic() >= post_confirm_reobserve_deadline):
                return adapter.captcha_dialog()
            observed, reobserved = _post_confirm_dialog_observation(
                adapter, kind, main_hwnd, grid_hwnd, known_dialog_handles,
                identified, baseline_dialogs,
                deadline=post_confirm_reobserve_deadline,
                poll_interval=poll_interval, monotonic=monotonic,
                sleep=sleep, phase=phase)
            if reobserved:
                first_table_at = None
                result['post_confirm_reobserved_sequence'] = adapter.clipboard_sequence()
                result['post_confirm_reobserved_clipboard_from_client'] = (
                    adapter.clipboard_from_client(main_hwnd))
            return observed

        while True:
            if (identified is None and not submitted
                    and monotonic() < pre_challenge_reobserve_deadline):
                current = _initial_copy_dialog_observation(
                    adapter, kind, main_hwnd, grid_hwnd, known_dialog_handles,
                    deadline=pre_challenge_reobserve_deadline,
                    poll_interval=poll_interval,
                    monotonic=monotonic, sleep=sleep, phase=phase)
            elif submitted:
                current = observe_after_confirm()
            else:
                current = adapter.captcha_dialog()
            if identified is None and current is not None:
                identified = current
                phase('captcha_detected')
                result['dialog_hwnd'] = current
                if adapter.locate() != (main_hwnd, grid_hwnd):
                    raise SamplingError('query_identity_changed')
                output_dir.mkdir(parents=True, exist_ok=True)
                image = output_dir / ('captcha-' + time.strftime('%Y%m%dT%H%M%S', time.gmtime())
                                      + '-' + str(time.time_ns()) + '.png')
                adapter.capture_image(current, image)
                phase('captcha_captured')
                image_bytes = image.read_bytes()
                result.update(image=str(image), image_sha256=hashlib.sha256(image_bytes).hexdigest(),
                              image_size=len(image_bytes))
                if cohort is not None:
                    cohort.begin(image_bytes)  # durable sample claim precedes OCR
                try:
                    candidate = candidate_provider.recognize(image_bytes)
                except Exception as exc:
                    if cohort is not None:
                        cohort.candidate(type(candidate_provider).__name__, None, error=exc)
                    raise
                if cohort is not None:
                    cohort.candidate(type(candidate_provider).__name__, candidate)
                phase('ocr_candidate_ready')
                if not isinstance(candidate, str) or re.fullmatch(r'[0-9]{4}', candidate) is None:
                    result['status'] = 'invalid_candidate'
                    break
                if monotonic() >= deadline:
                    result['status'] = 'timeout'
                    break
                if adapter.captcha_dialog() != current:
                    raise SamplingError('captcha_identity_changed')
                if adapter.read_captcha_edit(current) != '':
                    raise SamplingError('captcha_edit_not_empty')
                adapter.write_captcha_edit(current, candidate)
                phase('captcha_written')
                if adapter.captcha_dialog() != current:
                    raise SamplingError('captcha_identity_changed')
                readback = adapter.read_captcha_edit(current)
                if readback != candidate:
                    result['edit_readback_length'] = len(readback)
                    result['edit_readback_shape'] = (
                        'empty' if not readback else
                        'four_digits' if re.fullmatch(r'[0-9]{4}', readback) else
                        'other')
                    diagnostic = getattr(adapter, 'edit_diagnostic', None)
                    if diagnostic is not None:
                        try:
                            result['edit_diagnostic'] = diagnostic(current)
                        except Exception:
                            result['edit_diagnostic'] = {'status': 'unavailable'}
                    raise SamplingError('captcha_edit_mismatch')
                if adapter.locate() != (main_hwnd, grid_hwnd):
                    raise SamplingError('query_identity_changed')
                before_submit_image = output_dir / ('before-submit-' + str(time.time_ns()) + '.png')
                try:
                    adapter.capture_image(current, before_submit_image)
                    if hashlib.sha256(before_submit_image.read_bytes()).hexdigest() != result['image_sha256']:
                        raise SamplingError('captcha_image_changed')
                finally:
                    before_submit_image.unlink(missing_ok=True)
                if monotonic() >= deadline:
                    result['status'] = 'timeout'
                    break
                submit_pre = adapter.clipboard_sequence()
                result['clipboard_sequence_submit_pre'] = submit_pre
                if cohort is not None:
                    cohort.pre_submit(candidate, clipboard_sequence=submit_pre,
                                      roi_sha256=result['image_sha256'])
                adapter.confirm_captcha(current)
                phase('captcha_confirmed')
                submitted = True
                post_confirm_reobserve_deadline = min(deadline, monotonic() + 3.0)
                result['submitted'] = candidate
                result['status'] = 'submitted_pending'
                if post_confirm_quiet:
                    quiet_started = monotonic()
                    quiet_until = quiet_started + post_confirm_quiet
                    result['post_confirm_quiet_requested_seconds'] = post_confirm_quiet
                    result['post_confirm_quiet_wait_seconds'] = 0.0
                    phase('post_confirm_quiet_started')
                    if quiet_until >= deadline:
                        result['status'] = 'timeout'
                        break
                    while True:
                        now = monotonic()
                        result['post_confirm_quiet_wait_seconds'] = round(
                            now - quiet_started, 3)
                        if now >= deadline:
                            result['status'] = 'timeout'
                            break
                        observed = observe_after_confirm()
                        if observed not in (None, identified):
                            raise SamplingError('captcha_identity_changed')
                        quiet_inventory = getattr(adapter, 'post_confirm_dialog_fingerprint', None)
                        dialogs = (quiet_inventory(main_hwnd, observed)
                                   if quiet_inventory is not None else
                                   adapter.dialog_fingerprint(main_hwnd) if observed is None else
                                   baseline_dialogs)
                        if dialogs != baseline_dialogs:
                            raise SamplingError('post_submit_dialog_changed')
                        if now >= quiet_until:
                            phase('post_confirm_quiet_completed')
                            break
                        sleep(min(poll_interval, quiet_until - now, deadline - now))
                    if result['status'] == 'timeout':
                        break
                current = observe_after_confirm()
                if current not in (None, identified):
                    raise SamplingError('captcha_identity_changed')
            elif identified is not None and current not in (None, identified):
                raise SamplingError('captcha_identity_changed')

            after = adapter.clipboard_sequence()
            result['clipboard_sequence_after'] = after
            table_ok = False
            required_before = (result['clipboard_sequence_submit_pre'] if submitted
                               else before)
            if after > required_before:
                if monotonic() >= deadline:
                    result['status'] = 'timeout'
                    break
                try:
                    content = read_stable_clipboard(
                        adapter, after, sleep=sleep, monotonic=monotonic,
                        deadline=deadline)
                    if content is not None and not adapter.clipboard_from_client(main_hwnd):
                        result['status'] = 'attribution_unverified'
                    elif content is not None:
                        result['clipboard_sha256'] = hashlib.sha256(content.encode('utf-8')).hexdigest()
                        snapshot = parse_table(kind, content, sequence_before=required_before,
                                               sequence_after=after)
                        result['table_columns'] = snapshot.columns
                        result['table_row_count'] = len(snapshot.rows)
                        result['query_page_identity'] = (
                            'header_unique_pending_tree'
                            if matching_page_kinds(snapshot.columns) == {kind}
                            else 'unknown')
                        table_ok = True
                        phase('fresh_table_parsed')
                except SamplingError as exc:
                    if str(exc) == 'clipboard_sequence_changed_during_read':
                        result['status'] = 'attribution_unverified'
                    else:
                        raise
                except (SnapshotError, OSError):
                    pass
            if identified is not None and current == identified:
                # 编辑框重置只用于识别拒绝；不把 OCR 值当真值。
                if submitted:
                    try:
                        edit_value = adapter.read_captcha_edit(identified)
                    except SamplingError as exc:
                        if str(exc) != 'captcha_identity_changed':
                            raise
                        # 弹窗可在两次控件读取之间自然关闭；只有确实无弹窗时
                        # 才继续凭新表格判定，出现另一弹窗仍交由异常路径停下。
                        if adapter.captcha_dialog() is not None:
                            raise
                        current = None
                    else:
                        if edit_value != result['submitted']:
                            result['status'] = 'rejected'
                            break
            if identified is not None and current == identified:
                first_table_at = None
            elif table_ok:
                if monotonic() >= deadline:
                    result['status'] = 'timeout'
                    break
                if submitted and current is None:
                    if adapter.dialog_fingerprint(main_hwnd) != baseline_dialogs:
                        raise SamplingError('post_submit_dialog_changed')
                try:
                    located = adapter.locate(known_dialog_handles=known_dialog_handles)
                except SamplingError as exc:
                    if (str(exc) != 'simulation_window_missing' or not submitted
                            or current is not None or monotonic() >= deadline):
                        raise
                    # 只对纯缺窗作一次短暂重看；不重发复制或验证码提交。
                    sleep(min(poll_interval, 0.1, deadline - monotonic()))
                    if monotonic() >= deadline:
                        raise SamplingError('post_submit_window_observation_deadline') from exc
                    if adapter.captcha_dialog() is not None:
                        raise SamplingError('captcha_identity_changed')
                    if adapter.dialog_fingerprint(main_hwnd) != baseline_dialogs:
                        raise SamplingError('post_submit_dialog_changed')
                    located = adapter.locate(known_dialog_handles=known_dialog_handles)
                    phase('main_window_reobserved')
                if located != (main_hwnd, grid_hwnd):
                    raise SamplingError('query_identity_changed')
                adapter.verify_page(kind)
                if result['query_page_identity'] == 'header_unique_pending_tree':
                    result['query_page_identity'] = 'header_and_tree'
                elif matching_page_kinds(tuple(result['table_columns'])) != {kind}:
                    # A shared header plus the selected tree alone can still
                    # describe the previous central panel. Require fresh native
                    # controls and the unique grid in the bound main window,
                    # on every acceptance check. Loading is a separate proof.
                    page_checker = getattr(adapter, 'verify_table_page_identity', None)
                    controls_verified = (callable(page_checker)
                                         and page_checker(kind, main_hwnd, grid_hwnd) is True)
                    result['query_page_control_identity_verified'] = controls_verified
                    result['query_page_identity'] = ('header_and_tree'
                                                     if controls_verified else 'unknown')
                if first_table_at is None:
                    first_table_at = monotonic()
                if monotonic() - first_table_at >= settle:
                    scroll_after = scroll_reader(main_hwnd, grid_hwnd) if scroll_reader else None
                    result['grid_scroll_after'] = (vars(scroll_after) if scroll_after else None)
                    result['grid_scroll_corroboration'] = scroll_corroboration(
                        scroll_before, scroll_after, result.get('table_row_count'))
                    result['status'] = 'client_accepted' if submitted else 'new_table_no_captcha'
                    result['client_accepted'] = True if submitted else None
                    result['dialog_closed'] = True if submitted else None
                    phase('client_accepted')
                    break
            else:
                first_table_at = None

            if result['status'] == 'attribution_unverified':
                break

            now = monotonic()
            if now >= deadline:
                result['status'] = ('attribution_unverified'
                                    if submitted and after > before and
                                    after <= result['clipboard_sequence_submit_pre']
                                    else 'timeout')
                break
            sleep(min(poll_interval, deadline - now))
    except Exception as exc:
        result['status'] = 'error'
        result['error'] = _safe_error(exc)
        result['error_sites'] = [f'{Path(frame.filename).name}:{frame.name}:{frame.lineno}'
                                 for frame in traceback.extract_tb(exc.__traceback__)[-5:]]
        if isinstance(exc, SamplingError):
            result['reason'] = str(exc)
    finally:
        # 只取消本次发现且此刻仍为同一身份的复制验证码弹窗。
        if identified is not None and result['status'] != 'client_accepted':
            try:
                if adapter.captcha_dialog() == identified:
                    result['dialog_closed'] = adapter.cancel_captcha(identified)
            except Exception as exc:
                result['cleanup_error'] = _safe_error(exc)
        try:
            if result['copy_sent']:
                result['clipboard_sequence_after'] = adapter.clipboard_sequence()
        except Exception:
            pass
        result['late_dialog_possible'] = bool(result['copy_sent'])
        result['finished_at'] = _utc_time()
        if cohort is not None:
            try:
                cohort.finish(result)
            except Exception as exc:
                result['status'] = 'error'
                result['reason'] = 'cohort_finish_failed'
                result['error'] = _safe_error(exc)
        if not process_lock_held:
            _PROCESS_LOCK.release()
    return result
