"""Read-only foreground and hit-test guard for native broker mouse input.

The caller retains the business binding and performs the click separately.
No focus changes, cursor movement, or retry are performed here.
"""

from __future__ import annotations


class PointerGateError(RuntimeError):
    """Fixed code; never includes window or order text."""


class Win32PointerProvider:
    def __init__(self):
        import win32con
        import win32gui
        import win32process

        self.con = win32con
        self.gui = win32gui
        self.process = win32process

    def window(self, hwnd):
        gui = self.gui
        if not gui.IsWindow(hwnd):
            raise PointerGateError("pointer_window_unavailable")
        _, pid = self.process.GetWindowThreadProcessId(hwnd)
        return {"pid": pid, "parent": int(gui.GetParent(hwnd) or 0),
                "owner": int(gui.GetWindow(hwnd, self.con.GW_OWNER) or 0),
                "root_owner": int(gui.GetAncestor(hwnd, self.con.GA_ROOTOWNER) or 0),
                "class_name": gui.GetClassName(hwnd),
                "control_id": int(gui.GetDlgCtrlID(hwnd)),
                "text": gui.GetWindowText(hwnd),
                "visible": bool(gui.IsWindowVisible(hwnd)),
                "enabled": bool(gui.IsWindowEnabled(hwnd)),
                "rect": tuple(gui.GetWindowRect(hwnd))}

    def is_child(self, parent, child):
        return bool(self.gui.IsChild(parent, child))

    def foreground(self):
        return int(self.gui.GetForegroundWindow() or 0)

    def hit(self, point):
        return int(self.gui.WindowFromPoint(point) or 0)

    def client_to_screen(self, hwnd, coords):
        return tuple(self.gui.ClientToScreen(hwnd, coords))


class PointerGate:
    def __init__(self, provider=None):
        self.provider = provider if provider is not None else Win32PointerProvider()

    @staticmethod
    def _valid_rect(rect):
        return (type(rect) is tuple and len(rect) == 4
                and all(type(item) is int for item in rect)
                and rect[0] < rect[2] and rect[1] < rect[3])

    def _check(self, *, main, pid, target, parent, modal, control_id,
               caption, coords, current):
        source = self.provider
        if (any(type(value) is not int or value <= 0 for value in (main, pid, target))
                or (parent is not None and (type(parent) is not int or parent <= 0))
                or not callable(current)
                or (modal is not False and (type(modal) is not int or modal <= 0))):
            raise PointerGateError("pointer_contract_unverified")
        try:
            if current() is not True:
                raise PointerGateError("pointer_business_context_changed")
            main_info = source.window(main)
            target_info = source.window(target)
            modal_info = source.window(modal) if type(modal) is int else None
            if (main_info["pid"] != pid or main_info["visible"] is not True
                    or main_info["parent"] != 0
                    or (modal_info is None and main_info["enabled"] is not True)
                    or target_info["pid"] != pid or target_info["visible"] is not True
                    or target_info["enabled"] is not True
                    or type(target_info["parent"]) is not int
                    or target_info["parent"] <= 0
                    or not self._valid_rect(target_info["rect"])
                    or (parent is not None and target_info["parent"] != parent)
                    or source.is_child(main, target) is not True and modal_info is None):
                raise PointerGateError("pointer_target_unverified")
            if modal_info is not None:
                if (modal_info["pid"] != pid or modal_info["class_name"] != "#32770"
                        or modal_info["visible"] is not True
                        or modal_info["enabled"] is not True
                        or modal_info["parent"] not in (0, main)
                        or modal_info["owner"] != main
                        or modal_info["root_owner"] != main
                        or target_info["parent"] != modal
                        or source.is_child(modal, target) is not True):
                    raise PointerGateError("pointer_modal_unverified")
                expected_foreground = modal
            else:
                expected_foreground = main
            if control_id is not None:
                if (target_info["class_name"] != "Button"
                        or target_info["control_id"] != control_id
                        or target_info["text"] != caption):
                    raise PointerGateError("pointer_button_unverified")
                l, t, r, b = target_info["rect"]
                coords = ((r - l) // 2, (b - t) // 2)
            else:
                if (type(coords) is not tuple or len(coords) != 2
                        or any(type(value) is not int or value < 0 for value in coords)
                        or target_info["class_name"] != "CVirtualGridCtrl"
                        or target_info["control_id"] != 1047):
                    raise PointerGateError("pointer_grid_point_unverified")
                l, t, r, b = target_info["rect"]
            point = source.client_to_screen(target, coords)
            if (type(point) is not tuple or len(point) != 2
                    or any(type(value) is not int for value in point)):
                raise PointerGateError("pointer_point_unverified")
            if not (l <= point[0] < r and t <= point[1] < b):
                raise PointerGateError("pointer_point_outside_target")
            if (source.foreground() != expected_foreground
                    or source.hit(point) != target):
                raise PointerGateError("pointer_focus_or_hit_changed")
            # Last observation before caller invokes click_input. A subsequent
            # asynchronous focus change remains an unknown outcome, never a retry.
            if (current() is not True or source.window(main) != main_info
                    or source.window(target) != target_info
                    or (modal_info is not None and source.window(modal) != modal_info)
                    or source.client_to_screen(target, coords) != point
                    or source.foreground() != expected_foreground
                    or source.hit(point) != target):
                raise PointerGateError("pointer_changed_before_click")
        except PointerGateError:
            raise
        except Exception:
            raise PointerGateError("pointer_observation_unavailable") from None

    def button(self, main, pid, button, *, parent=None, modal=False,
               control_id, caption, current):
        self._check(main=main, pid=pid, target=button, parent=parent,
                    modal=modal, control_id=control_id, caption=caption,
                    coords=None, current=current)

    def grid(self, main, pid, grid, *, coords, current):
        self._check(main=main, pid=pid, target=grid, parent=None,
                    modal=False, control_id=None, caption=None,
                    coords=coords, current=current)
