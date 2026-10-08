"""Read-only query-page context observed around one table collection attempt.

This is a local observation, not a complete-table or broker-fact certificate.
The caller must separately bind account identity, refresh, clipboard ownership,
and row coverage. Windows dependencies are loaded only for the native provider.
"""

from __future__ import annotations

from dataclasses import dataclass

from .grid_scroll_evidence import GridScroll


_KINDS = frozenset({"holdings", "trades", "orders", "cancelable"})
_FILTER_IDS = frozenset({1337, 1001, 3348, 2410})
_LOADING_TEXT = ("正在查询", "正在加载", "请稍候")


@dataclass(frozen=True)
class QueryContext:
    page_verified: bool
    filter_verified: bool
    loading_finished: bool
    scroll: GridScroll | None


class NativeQueryProvider:
    """Bounded native reads; no navigation, click, or field mutation."""

    def __init__(self, adapter):
        from .native_form_observer import Win32ObserverApi
        from .native_order_form_backend import Win32FormApi
        self._adapter = adapter
        self._observer = Win32ObserverApi()
        self._form = Win32FormApi()

    def __getattr__(self, name):
        return getattr(self._observer, name)

    def selected_text(self, hwnd: int) -> str:
        return self._adapter.app.window(handle=hwnd).wrapper_object().selected_text()

    def edit_line(self, hwnd: int) -> str:
        from .native_order_form_backend import _read_single_edit_line
        return _read_single_edit_line(
            lambda h, msg, wp, buf=None: self._form.send_message_timeout(
                h, msg, wp, buf, 2000), hwnd)

    def scroll(self, main_hwnd: int, grid_hwnd: int) -> GridScroll | None:
        from .grid_scroll_evidence import read_grid_scroll
        return read_grid_scroll(grid_hwnd, main_hwnd)


def _bound(adapter, provider, main: int, grid: int) -> bool:
    """Recheck the same native main/grid identity before and after all reads."""
    if adapter.locate() != (main, grid):
        return False
    pid = adapter.app.process
    return bool(
        type(pid) is int and pid > 0
        and provider.is_window(main) and provider.is_window(grid)
        and provider.is_visible(main) and provider.is_visible(grid)
        and provider.is_enabled(main) and provider.is_enabled(grid)
        and provider.pid(main) == pid and provider.pid(grid) == pid
        and provider.parent(main) == 0 and provider.is_child(main, grid)
        and provider.class_name(grid) == "CVirtualGridCtrl"
        and provider.control_id(grid) == 1047
    )


def _filters_empty(provider, main: int, pid: int, kind: str,
                   children: tuple[int, ...]) -> bool:
    visible = [hwnd for hwnd in children
               if provider.is_window(hwnd) and provider.is_visible(hwnd)]
    relevant = {key: [] for key in _FILTER_IDS}
    for hwnd in visible:
        control_id = provider.control_id(hwnd)
        if control_id in relevant:
            relevant[control_id].append(hwnd)
    if any(len(relevant[key]) > 1 for key in (1337, 3348, 2410)):
        return False

    combos = relevant[1337]
    if combos:
        combo = combos[0]
        if not _control(provider, main, pid, combo, "ComboBox"):
            return False
    else:
        combo = None

    query_edits = []
    for hwnd in relevant[1001]:
        # The qualified client also reuses ID 1001 for a visible custom tab.
        # It is not an Edit and cannot be interpreted as a query filter.
        if provider.class_name(hwnd) == "CCustomTabCtrl":
            if not _control(provider, main, pid, hwnd, "CCustomTabCtrl"):
                return False
            continue
        if provider.class_name(hwnd) != "Edit":
            return False
        parent = provider.parent(hwnd)
        # Account ComboBox 2322 may also own an Edit 1001. Never read it.
        if (parent and provider.is_window(parent)
                and provider.control_id(parent) == 2322
                and provider.class_name(parent) == "ComboBox"):
            continue
        if combo is None or parent != combo:
            return False
        query_edits.append(hwnd)
    if combo is not None:
        if len(query_edits) != 1 or not _control(provider, main, pid,
                                                query_edits[0], "Edit"):
            return False
        if provider.edit_line(query_edits[0]) != "":
            return False

    codes = relevant[3348]
    if codes:
        if kind != "cancelable" or not _control(provider, main, pid,
                                                 codes[0], "Edit"):
            return False
        if provider.edit_line(codes[0]) != "":
            return False

    groups = relevant[2410]
    if groups:
        if not _control(provider, main, pid, groups[0], "ComboBox"):
            return False
        if provider.selected_text(groups[0]) != "全部":
            return False
    return True


def _control(provider, main: int, pid: int, hwnd: int, expected_class: str) -> bool:
    return bool(provider.is_enabled(hwnd) and provider.pid(hwnd) == pid
                and provider.is_child(main, hwnd)
                and provider.class_name(hwnd) == expected_class)


def _loading_finished(provider, main: int, pid: int,
                      children: tuple[int, ...]) -> bool:
    for hwnd in children:
        if not provider.is_window(hwnd) or not provider.is_visible(hwnd):
            continue
        if provider.pid(hwnd) != pid or not provider.is_child(main, hwnd):
            return False
        class_name = provider.class_name(hwnd)
        if class_name.casefold() in {"progressbar", "msctls_progress32"}:
            return False
        if class_name == "Static" and any(
                marker in provider.text(hwnd) for marker in _LOADING_TEXT):
            return False
    return True


def inspect_query_context(adapter, kind: str, main_hwnd: int, grid_hwnd: int,
                          *, provider=None) -> QueryContext:
    """Inspect one stable frame; failure and unknown evidence fail closed.

    ``provider`` is injectable for offline tests. The native implementation
    only reads controls and uses a two-second bounded Edit message transport.
    """
    failed = QueryContext(False, False, False, None)
    if (type(kind) is not str or kind not in _KINDS
            or type(main_hwnd) is not int or main_hwnd <= 0
            or type(grid_hwnd) is not int or grid_hwnd <= 0):
        return failed
    try:
        source = provider if provider is not None else NativeQueryProvider(adapter)
        if not _bound(adapter, source, main_hwnd, grid_hwnd):
            return failed
        adapter.verify_page(kind)
        if (kind in {"orders", "cancelable"}
                and adapter.verify_table_page_identity(kind, main_hwnd, grid_hwnd)
                is not True):
            return failed
        dialogs = adapter.dialog_fingerprint(main_hwnd)
        children = tuple(source.child_windows(main_hwnd))
        if len(children) != len(set(children)):
            return failed
        filters = _filters_empty(source, main_hwnd, adapter.app.process, kind,
                                 children)
        loading = _loading_finished(source, main_hwnd, adapter.app.process,
                                    children)
        scroll = source.scroll(main_hwnd, grid_hwnd)
        if not isinstance(scroll, GridScroll):
            scroll = None
        if (not _bound(adapter, source, main_hwnd, grid_hwnd)
                or adapter.dialog_fingerprint(main_hwnd) != dialogs):
            return failed
        adapter.verify_page(kind)
        if (kind in {"orders", "cancelable"}
                and adapter.verify_table_page_identity(kind, main_hwnd, grid_hwnd)
                is not True):
            return failed
        return QueryContext(True, filters, loading, scroll)
    except Exception:
        # Do not leak control text, account text, or platform exception detail.
        return failed
