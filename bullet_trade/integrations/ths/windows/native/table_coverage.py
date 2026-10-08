"""Combine independent observations before claiming a copied table is complete.

The scroll range corroborates the row count; it is never sufficient by itself.
Callers must supply every identity, freshness, and clipboard observation from
the same collection attempt. This module performs no GUI or clipboard I/O.
"""

from __future__ import annotations

from dataclasses import dataclass

from .grid_scroll_evidence import GridScroll


@dataclass(frozen=True)
class TableCoverage:
    complete: bool
    missing: tuple[str, ...]


def assess_table_coverage(
    *,
    rows_count: int,
    scroll_before: GridScroll | None,
    scroll_after: GridScroll | None,
    account: bool | None,
    page: bool | None,
    filter: bool | None,
    explicit_refresh: bool | None,
    loading_finished: bool | None,
    clipboard_owned: bool | None,
) -> TableCoverage:
    """Require an unchanged, plausible range and six explicit true proofs.

    ``page`` is the verified query-page identity, while ``scroll_before.page``
    is the number of visible grid rows. A scrollable table may still be complete
    when a verified clipboard copy contains rows beyond the viewport.
    """
    missing: list[str] = []
    for name, value in (
        ("account", account),
        ("page", page),
        ("filter", filter),
        ("explicit_refresh", explicit_refresh),
        ("loading_finished", loading_finished),
        ("clipboard_owned", clipboard_owned),
    ):
        if value is not True:
            missing.append(name)

    if type(rows_count) is not int or rows_count < 0:
        missing.append("rows_count_invalid")

    if not isinstance(scroll_before, GridScroll):
        missing.append("scroll_before_missing")
    if not isinstance(scroll_after, GridScroll):
        missing.append("scroll_after_missing")
    if isinstance(scroll_before, GridScroll) and isinstance(scroll_after, GridScroll):
        if scroll_before != scroll_after:
            missing.append("scroll_changed")
        scroll = scroll_before
        values = (scroll.minimum, scroll.maximum, scroll.page, scroll.position)
        if any(type(value) is not int for value in values):
            missing.append("scroll_invalid")
        elif type(rows_count) is int and rows_count >= 0:
            if rows_count == 0:
                if values != (0, 0, 0, 0):
                    missing.append("empty_range_invalid")
            else:
                if scroll.minimum != 0 or scroll.maximum < scroll.minimum:
                    missing.append("scroll_range_invalid")
                elif scroll.maximum - scroll.minimum != rows_count:
                    missing.append("scroll_row_count_mismatch")
                if (scroll.page <= 0 or scroll.position < 0
                        or scroll.position > max(0, scroll.maximum - scroll.page + 1)):
                    missing.append("scroll_viewport_invalid")

    return TableCoverage(complete=not missing, missing=tuple(missing))
