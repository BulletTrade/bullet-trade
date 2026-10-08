from dataclasses import dataclass
from types import SimpleNamespace

from bullet_trade.integrations.ths.windows.native.grid_scroll_evidence import GridScroll
from bullet_trade.integrations.ths.windows.native.query_context import inspect_query_context


@dataclass
class Control:
    parent: int
    control_id: int
    class_name: str
    text: str = ""
    visible: bool = True
    enabled: bool = True
    selected: str = ""


class Frame:
    def __init__(self):
        self.controls = {
            10: Control(0, 0, "Main"),
            20: Control(10, 1047, "CVirtualGridCtrl"),
            30: Control(10, 1337, "ComboBox"),
            31: Control(30, 1001, "Edit"),
            40: Control(10, 3348, "Edit"),
            50: Control(10, 2410, "ComboBox", selected="全部"),
            60: Control(10, 2322, "ComboBox"),
            61: Control(60, 1001, "Edit", text="ACCOUNT_FIELD_NOT_READ"),
        }
        self.read_edits = []
        self.scroll_value = GridScroll(0, 3, 2, 0)

    def is_window(self, hwnd): return hwnd in self.controls
    def is_visible(self, hwnd): return self.controls[hwnd].visible
    def is_enabled(self, hwnd): return self.controls[hwnd].enabled
    def pid(self, hwnd): return 7
    def parent(self, hwnd): return self.controls[hwnd].parent
    def is_child(self, parent, hwnd):
        while hwnd in self.controls and self.controls[hwnd].parent:
            hwnd = self.controls[hwnd].parent
            if hwnd == parent:
                return True
        return False
    def class_name(self, hwnd): return self.controls[hwnd].class_name
    def control_id(self, hwnd): return self.controls[hwnd].control_id
    def text(self, hwnd): return self.controls[hwnd].text
    def child_windows(self, main): return tuple(h for h in self.controls if h != main)
    def selected_text(self, hwnd): return self.controls[hwnd].selected
    def edit_line(self, hwnd):
        self.read_edits.append(hwnd)
        return self.controls[hwnd].text
    def scroll(self, main, grid): return self.scroll_value


class Adapter:
    def __init__(self):
        self.app = SimpleNamespace(process=7)
        self.location = (10, 20)
        self.page_calls = []
        self.table_calls = []
        self.dialogs = frozenset()

    def locate(self): return self.location
    def verify_page(self, kind): self.page_calls.append(kind)
    def verify_table_page_identity(self, kind, main, grid):
        self.table_calls.append((kind, main, grid))
        return True
    def dialog_fingerprint(self, main): return self.dialogs


def inspect(kind="cancelable", frame=None, adapter=None):
    return inspect_query_context(adapter or Adapter(), kind, 10, 20,
                                 provider=frame or Frame())


def test_query_and_cancel_filters_are_empty_without_reading_account_edit():
    frame = Frame()
    adapter = Adapter()
    context = inspect(frame=frame, adapter=adapter)
    assert (context.page_verified, context.filter_verified,
            context.loading_finished, context.scroll) == (
                True, True, True, GridScroll(0, 3, 2, 0))
    assert frame.read_edits == [31, 40]
    assert adapter.table_calls == [("cancelable", 10, 20)] * 2


def test_fixed_query_layout_without_optional_filters():
    frame = Frame()
    for hwnd in (30, 31, 40, 50):
        del frame.controls[hwnd]
    adapter = Adapter()
    result = inspect("holdings", frame, adapter)
    assert result.filter_verified is True
    assert result.page_verified is True
    assert frame.read_edits == []
    assert adapter.table_calls == []


def test_nonempty_or_ambiguous_filters_fail_closed():
    for hwnd, value in ((31, "SYNTHETIC"), (40, "123456"),
                        (50, "买入")):
        frame = Frame()
        if hwnd == 50:
            frame.controls[hwnd].selected = value
        else:
            frame.controls[hwnd].text = value
        assert inspect(frame=frame).filter_verified is False
    frame = Frame()
    frame.controls[32] = Control(30, 1001, "Edit")
    assert inspect(frame=frame).filter_verified is False
    frame = Frame()
    frame.controls[32] = Control(10, 1337, "ComboBox")
    assert inspect(frame=frame).filter_verified is False
    frame = Frame()
    frame.controls[32] = Control(10, 1001, "Edit")
    assert inspect(frame=frame).filter_verified is False


def test_loading_and_disabled_grid_are_rejected():
    frame = Frame()
    frame.controls[70] = Control(10, 70, "Static", "正在加载数据")
    assert inspect(frame=frame).loading_finished is False
    frame = Frame()
    frame.controls[70] = Control(10, 70, "msctls_progress32")
    assert inspect(frame=frame).loading_finished is False
    frame = Frame()
    frame.controls[20].enabled = False
    assert inspect(frame=frame).page_verified is False


def test_identity_and_modal_changes_fail_closed():
    frame = Frame()
    adapter = Adapter()
    adapter.location = (10, 99)
    assert inspect(frame=frame, adapter=adapter).page_verified is False

    class ModalAdapter(Adapter):
        def dialog_fingerprint(self, main):
            raise RuntimeError("synthetic_modal")

    assert inspect(frame=Frame(), adapter=ModalAdapter()).page_verified is False

    class MovingAdapter(Adapter):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def locate(self):
            self.calls += 1
            return (10, 20) if self.calls == 1 else (10, 99)

    assert inspect(frame=Frame(), adapter=MovingAdapter()).page_verified is False


def test_orders_require_additional_page_identity():
    class FailedTableAdapter(Adapter):
        def verify_table_page_identity(self, kind, main, grid):
            return False

    assert inspect("orders", Frame(), FailedTableAdapter()).page_verified is False


def test_known_id_collision_is_a_tab_not_a_filter_edit():
    frame = Frame()
    frame.controls[80] = Control(10, 1001, "CCustomTabCtrl")
    assert inspect(frame=frame).filter_verified is True
    assert frame.read_edits == [31, 40]
    frame.controls[80].class_name = "UnknownWidget"
    assert inspect(frame=frame).filter_verified is False
