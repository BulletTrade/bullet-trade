"""Read-only grid scroll observations; never a stand-alone completeness proof."""
from __future__ import annotations

import ctypes
from ctypes import wintypes
from dataclasses import dataclass


@dataclass(frozen=True)
class GridScroll:
    minimum: int
    maximum: int
    page: int
    position: int


def read_grid_scroll(grid_hwnd: int, main_hwnd: int) -> GridScroll | None:
    """Inspect the unique caller-supplied grid HWND without moving the UI."""
    if not isinstance(grid_hwnd, int) or grid_hwnd <= 0:
        return None
    if not isinstance(main_hwnd, int) or main_hwnd <= 0:
        return None
    import win32gui
    if (not win32gui.IsWindow(grid_hwnd)
            or not win32gui.IsWindowVisible(grid_hwnd)
            or win32gui.GetClassName(grid_hwnd) != 'CVirtualGridCtrl'
            or win32gui.GetDlgCtrlID(grid_hwnd) != 1047
            or not win32gui.IsChild(main_hwnd, grid_hwnd)):
        return None

    class SCROLLINFO(ctypes.Structure):
        _fields_ = [('cbSize', wintypes.UINT), ('fMask', wintypes.UINT),
                    ('nMin', ctypes.c_int), ('nMax', ctypes.c_int),
                    ('nPage', wintypes.UINT), ('nPos', ctypes.c_int),
                    ('nTrackPos', ctypes.c_int)]

    info = SCROLLINFO()
    info.cbSize = ctypes.sizeof(info)
    info.fMask = 0x17  # SIF_ALL; reads only, does not scroll.
    user32 = ctypes.WinDLL('user32', use_last_error=True)
    user32.GetScrollInfo.argtypes = (wintypes.HWND, ctypes.c_int,
                                    ctypes.POINTER(SCROLLINFO))
    user32.GetScrollInfo.restype = wintypes.BOOL
    if not user32.GetScrollInfo(grid_hwnd, 1, ctypes.byref(info)):
        return None
    return GridScroll(info.nMin, info.nMax, info.nPage, info.nPos)


def scroll_corroboration(before: GridScroll | None, after: GridScroll | None,
                         parsed_rows: int | None) -> str:
    """A matching range is a diagnostic, never `query_complete`."""
    if before is None or after is None or parsed_rows is None:
        return 'unknown'
    if before != after:
        return 'changed_during_copy'
    if before.minimum < 0 or before.maximum < before.minimum or before.page < 0:
        return 'invalid_range'
    return ('range_matches_rows' if before.maximum - before.minimum == parsed_rows
            else 'range_differs_from_rows')
