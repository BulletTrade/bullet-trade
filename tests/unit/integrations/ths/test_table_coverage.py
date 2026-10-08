from bullet_trade.integrations.ths.windows.native.grid_scroll_evidence import GridScroll
from bullet_trade.integrations.ths.windows.native.table_coverage import assess_table_coverage


def assess(rows_count, scroll_before, scroll_after=None, **proofs):
    evidence = dict(account=True, page=True, filter=True, explicit_refresh=True,
                    loading_finished=True, clipboard_owned=True)
    evidence.update(proofs)
    return assess_table_coverage(
        rows_count=rows_count, scroll_before=scroll_before,
        scroll_after=scroll_before if scroll_after is None else scroll_after,
        **evidence,
    )


def test_full_clipboard_can_exceed_visible_page():
    result = assess(12, GridScroll(0, 12, 4, 3))
    assert result.complete is True
    assert result.missing == ()


def test_short_table_can_fit_with_page_larger_than_row_count():
    assert assess(2, GridScroll(0, 2, 5, 0)).complete is True


def test_empty_table_requires_exact_zero_range():
    assert assess(0, GridScroll(0, 0, 0, 0)).complete is True
    for scroll in (GridScroll(0, 0, 1, 0), GridScroll(0, 1, 0, 0),
                   GridScroll(0, 0, 0, 1)):
        result = assess(0, scroll)
        assert result.complete is False
        assert "empty_range_invalid" in result.missing


def test_changed_range_and_row_count_mismatch_are_rejected():
    changed = assess(3, GridScroll(0, 3, 2, 0), GridScroll(0, 4, 2, 0))
    assert changed.complete is False
    assert "scroll_changed" in changed.missing
    mismatch = assess(3, GridScroll(0, 4, 2, 0))
    assert mismatch.complete is False
    assert "scroll_row_count_mismatch" in mismatch.missing


def test_unknown_or_false_proofs_are_not_inferred_from_range():
    for proof in ("account", "page", "filter", "explicit_refresh",
                  "loading_finished", "clipboard_owned"):
        for value in (False, None, 1):
            result = assess(3, GridScroll(0, 3, 2, 0), **{proof: value})
            assert result.complete is False
            assert proof in result.missing


def test_invalid_rows_and_viewport_are_rejected():
    assert "rows_count_invalid" in assess(True, GridScroll(0, 1, 1, 0)).missing
    assert "rows_count_invalid" in assess(-1, GridScroll(0, 1, 1, 0)).missing
    assert "scroll_viewport_invalid" in assess(3, GridScroll(0, 3, 2, 3)).missing
    assert "scroll_viewport_invalid" in assess(3, GridScroll(0, 3, 0, 0)).missing
    assert "scroll_range_invalid" in assess(3, GridScroll(1, 4, 2, 0)).missing
