"""Native HWND-only recognition of the known Xiadan copy-CAPTCHA dialog.

No child wrappers are created and Edit text is never requested. A caller must
still verify the returned HWND afresh before any capture or cancel action.
"""
from __future__ import annotations


class DetectionError(RuntimeError):
    pass


def detect_copy_captcha(gui, process, con, *, main_hwnd: int, client_pid: int) -> int | None:
    """Return the sole visible copy dialog in the fixed client process."""
    if not gui.IsWindow(main_hwnd) or not gui.IsWindowVisible(main_hwnd):
        raise DetectionError('main_window_not_bound')
    _, main_pid = process.GetWindowThreadProcessId(main_hwnd)
    if main_pid != client_pid:
        raise DetectionError('simulation_client_identity_changed')

    dialogs = []

    def collect_dialog(hwnd, _):
        if not gui.IsWindow(hwnd):
            return
        try:
            if not gui.IsWindowVisible(hwnd) or gui.GetClassName(hwnd) != '#32770':
                return
            _, pid = process.GetWindowThreadProcessId(hwnd)
            if pid != client_pid:
                return
            statics = []
            controls = {2405: [], 2404: [], 2: []}

            def collect_child(child, __):
                if not gui.IsWindow(child):
                    return
                try:
                    if not gui.IsWindowVisible(child):
                        return
                    _, child_pid = process.GetWindowThreadProcessId(child)
                    if child_pid != client_pid:
                        raise DetectionError('captcha_child_pid_mismatch')
                    cls = gui.GetClassName(child)
                    control_id = gui.GetDlgCtrlID(child)
                    # GetWindowText is deliberately limited to Static labels
                    # and the Cancel button title. It must never read Edit.
                    if cls == 'Static':
                        statics.append(gui.GetWindowText(child))
                    if control_id in controls:
                        title = gui.GetWindowText(child) if cls == 'Button' and control_id == 2 else None
                        controls[control_id].append((cls, title))
                except DetectionError:
                    raise
                except Exception:
                    if not gui.IsWindow(child) or not gui.IsWindow(hwnd):
                        return
                    raise

            gui.EnumChildWindows(hwnd, collect_child, None)
            if not gui.IsWindow(hwnd) or not gui.IsWindowVisible(hwnd):
                return
            copy_labels = [text for text in statics
                           if text.startswith('检测到您正在拷贝数据')]
            captcha_labels = [text for text in statics if '验证码' in text]
            looks_like_captcha = bool(captcha_labels or controls[2405] or controls[2404]
                                      or any('拷贝数据' in text or '验证' in text
                                             for text in statics))
            if not looks_like_captcha:
                return
            owner = gui.GetWindow(hwnd, con.GW_OWNER)
            root_owner = gui.GetAncestor(hwnd, con.GA_ROOTOWNER)
            if owner != main_hwnd or root_owner != main_hwnd:
                raise DetectionError('captcha_owner_mismatch')
            if len(copy_labels) != 1 or len(captcha_labels) != 1:
                raise DetectionError('copy_captcha_marker_unverified')
            if (controls[2405] != [('Static', None)]
                    or controls[2404] != [('Edit', None)]
                    or len(controls[2]) != 1
                    or controls[2][0][0] != 'Button'
                    or controls[2][0][1] not in {'取消', 'Cancel'}):
                raise DetectionError('captcha_shape_unverified')
            dialogs.append(hwnd)
        except DetectionError:
            raise
        except Exception:
            if not gui.IsWindow(hwnd):
                return
            raise DetectionError('captcha_detection_failed') from None

    try:
        gui.EnumWindows(collect_dialog, None)
    except DetectionError:
        raise
    except Exception as exc:
        raise DetectionError('captcha_detection_failed') from exc
    if len(dialogs) > 1:
        raise DetectionError('captcha_dialog_not_unique')
    return dialogs[0] if dialogs else None
