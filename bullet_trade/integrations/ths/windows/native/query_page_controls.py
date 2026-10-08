"""Read-only page-shape evidence for THS orders and cancelable queries.

This proves neither business account identity nor table freshness, pagination,
row completeness, or cancel eligibility. The caller first verifies the tree.
"""

from __future__ import annotations

from dataclasses import dataclass


_MAIN_TITLE = '网上股票交易系统5.0'
_TREES = {'orders': '当日委托', 'cancelable': '撤单[F3]'}
_ORDERS = {2410: ('ComboBox', None), 2449: ('Button', '过滤'),
           3501: ('Button', '汇总')}
_CANCEL = {1099: ('Button', '撤单(Del)'), 30001: ('Button', '全撤(Z /)'),
           30002: ('Button', '撤买(X)'), 30003: ('Button', '撤卖(C)')}
_INTERESTING_IDS = frozenset((1047, *_ORDERS, *_CANCEL))
_BUTTON_IDS = frozenset((*_CANCEL, 2449, 3501))


@dataclass(frozen=True)
class PageControl:
    hwnd: int
    control_id: int
    class_name: str
    parent_hwnd: int
    pid: int
    visible: bool
    caption: str | None = None
    selected_text: str | None = None


@dataclass(frozen=True)
class QueryPageObservation:
    main_hwnd: int
    main_title: str
    client_pid: int
    selected_tree: str
    controls: tuple[PageControl, ...]
    # Native reader verifies these HWNDs with IsChild(main, panel).
    descendant_panels: frozenset[int]
    grid_headers: tuple[str, ...] = ()  # Informational; never used as identity.
    row_count: int | None = None


def verify_query_page(observation: QueryPageObservation, kind: str,
                      grid_hwnd: int) -> bool:
    """Fail closed on any missing, duplicate, mixed, or detached page shape."""
    return _page_shape_failure(observation, kind, grid_hwnd) is None


def _page_shape_failure(observation: QueryPageObservation, kind: str,
                        grid_hwnd: int) -> str | None:
    if (type(kind) is not str or kind not in _TREES
            or not isinstance(observation, QueryPageObservation)):
        return 'invalid_input'
    if (type(observation.main_hwnd) is not int or observation.main_hwnd <= 0
            or observation.main_title != _MAIN_TITLE
            or type(observation.client_pid) is not int or observation.client_pid <= 0
            or observation.selected_tree != _TREES[kind]):
        return 'main_or_tree_identity'
    if (not isinstance(observation.controls, tuple)
            or not isinstance(observation.descendant_panels, frozenset)
            or type(grid_hwnd) is not int or grid_hwnd <= 0):
        return 'observation_structure'
    if any(not isinstance(control, PageControl) for control in observation.controls):
        return 'control_structure'
    visible = [control for control in observation.controls if control.visible is True]
    relevant = [control for control in visible if control.control_id in _INTERESTING_IDS]
    if any(type(c.hwnd) is not int or c.hwnd <= 0 or c.pid != observation.client_pid
           or type(c.parent_hwnd) is not int or c.parent_hwnd <= 0
           or c.parent_hwnd == observation.main_hwnd
           or c.parent_hwnd not in observation.descendant_panels
           for c in relevant):
        return 'relevant_control_context'
    # ID 1047 is also used by a visible Afx page widget. Only this exact
    # class is a grid; still require a unique visible grid and matching HWND.
    grids = [control for control in relevant if control.control_id == 1047
             and control.class_name == 'CVirtualGridCtrl']
    if len(grids) != 1 or grids[0].hwnd != grid_hwnd:
        return 'grid_shape'
    required = _ORDERS if kind == 'orders' else _CANCEL
    forbidden = _CANCEL if kind == 'orders' else _ORDERS
    if any(control.control_id in forbidden for control in relevant):
        return 'mixed_page_controls'
    group = []
    for control_id, (class_name, caption) in required.items():
        found = [control for control in relevant if control.control_id == control_id]
        if len(found) != 1:
            return 'required_control_not_unique'
        control = found[0]
        if control.class_name != class_name:
            return 'required_control_class'
        if caption is not None and control.caption != caption:
            return 'required_button_caption'
        if control_id == 2410 and control.selected_text != '全部':
            return 'orders_filter_not_all'
        group.append(control)
    if len({control.parent_hwnd for control in group}) != 1:
        return 'group_parent_mismatch'
    if len({control.hwnd for control in [grids[0], *group]}) != len(group) + 1:
        return 'required_hwnd_not_unique'
    return None


def verify_native_query_page(adapter, kind: str, main_hwnd: int,
                             grid_hwnd: int, diagnostics: dict | None = None) -> bool:
    """Read only the whitelisted IDs. Caller first runs adapter.verify_page.

    ``selected_tree`` is the caller-verified tree for ``kind``. This reader
    independently checks the native page shape; it does not reselect a tree.
    """
    if diagnostics is not None:
        diagnostics.clear()
    def failed(stage: str, reason: str) -> bool:
        if diagnostics is not None:
            diagnostics.update(stage=stage, reason=reason)
        return False

    if (type(kind) is not str or kind not in _TREES
            or type(main_hwnd) is not int or main_hwnd <= 0):
        return failed('input', 'invalid_input')
    stage = 'adapter_binding'
    try:
        gui, process, app = adapter.gui, adapter.process, adapter.app
        stage = 'main_identity'
        if (not gui.IsWindow(main_hwnd) or not gui.IsWindowVisible(main_hwnd)
                or gui.GetWindowText(main_hwnd) != _MAIN_TITLE
                or process.GetWindowThreadProcessId(main_hwnd)[1] != app.process):
            return failed(stage, 'main_identity')
        stage = 'enumerate_children'
        handles = []
        gui.EnumChildWindows(main_hwnd, lambda hwnd, _: handles.append(hwnd), None)
        controls = []
        panels = set()
        for hwnd in handles:
            stage = 'child_identity'
            if not gui.IsWindow(hwnd):
                return failed(stage, 'child_disappeared')
            control_id = gui.GetDlgCtrlID(hwnd)
            if control_id not in _INTERESTING_IDS:
                continue
            if not gui.IsWindowVisible(hwnd):
                continue
            stage = 'parent_identity'
            parent = gui.GetParent(hwnd)
            if (not parent or not gui.IsWindow(parent)
                    or not gui.IsChild(main_hwnd, parent)
                    or process.GetWindowThreadProcessId(parent)[1] != app.process):
                return failed(stage, 'parent_identity')
            panels.add(parent)
            stage = 'button_caption'
            caption = gui.GetWindowText(hwnd) if control_id in _BUTTON_IDS else None
            stage = 'orders_filter'
            selected = (app.window(handle=hwnd).wrapper_object().selected_text()
                        if control_id == 2410 else None)
            stage = 'control_metadata'
            controls.append(PageControl(
                hwnd=hwnd, control_id=control_id,
                class_name=gui.GetClassName(hwnd), parent_hwnd=parent,
                pid=process.GetWindowThreadProcessId(hwnd)[1], visible=True,
                caption=caption, selected_text=selected))
        stage = 'pure_shape'
        observation = QueryPageObservation(
            main_hwnd=main_hwnd, main_title=gui.GetWindowText(main_hwnd),
            client_pid=app.process, selected_tree=_TREES[kind],
            controls=tuple(controls), descendant_panels=frozenset(panels))
        reason = _page_shape_failure(observation, kind, grid_hwnd)
        if reason is not None:
            return failed(stage, reason)
        if diagnostics is not None:
            diagnostics.update(stage='complete', reason='verified')
        return True
    except Exception as exc:
        if diagnostics is not None:
            diagnostics.update(stage=stage, reason='native_exception',
                               exception_type=type(exc).__name__)
        return False
