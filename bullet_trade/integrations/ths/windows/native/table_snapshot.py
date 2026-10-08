"""同花顺剪贴板表格的离线完整性检查；不访问客户端。"""
from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from typing import Mapping

SCHEMAS: Mapping[str, tuple[str, ...]] = {
    'holdings': ('证券代码', '股票余额', '可用余额', '冻结数量'),
    'orders': ('委托时间', '证券代码', '操作', '委托数量', '成交数量', '撤消数量', '合同编号'),
    'trades': ('成交时间', '证券代码', '操作', '成交数量', '成交金额', '合同编号', '成交编号'),
    'cancelable': ('证券代码', '操作', '委托数量', '成交数量', '合同编号'),
}


def matching_page_kinds(columns: tuple[str, ...]) -> frozenset[str]:
    """返回仅凭复制表头可能对应的页面；同表头不能证明页面身份。"""
    names = set(columns)
    return frozenset(kind for kind, required in SCHEMAS.items()
                     if set(required) <= names)

class SnapshotError(ValueError):
    """拒绝把无效或缺失的复制结果当作完整表格。"""

    completeness = 'incomplete'

@dataclass(frozen=True)
class TableSnapshot:
    kind: str
    clipboard_sequence_before: int
    clipboard_sequence_after: int
    columns: tuple[str, ...]
    rows: tuple[dict[str, str], ...]
    completeness: str = 'unknown'
    completeness_reason: str = '剪贴板文本无法证明筛选状态、分页状态及是否覆盖全量记录'
    page_identity: str = 'unknown'


def _empty_placeholder(cells: list[str]) -> bool:
    """真实客户端可能复制出仅含数值零的空占位行。"""
    return all(not cell or re.fullmatch(r'0+(?:\.0+)?', cell) for cell in cells)


def parse_table(kind: str, text: str, *, sequence_before: int, sequence_after: int) -> TableSnapshot:
    """只接受本次新复制、预期表头及可判定的表格。空文本不是空表。"""
    if kind not in SCHEMAS:
        raise SnapshotError(f'未知表格类型: {kind}')
    if sequence_after <= sequence_before or sequence_before < 0:
        raise SnapshotError('剪贴板没有新的复制序列')
    if not text or '\t' not in text:
        raise SnapshotError('剪贴板无有效表格')
    try:
        raw_rows = list(csv.reader(io.StringIO(text), delimiter='\t', strict=True))
    except csv.Error as exc:
        raise SnapshotError('剪贴板表格格式错误') from exc
    if not raw_rows:
        raise SnapshotError('表头缺失')
    header = [x.strip() for x in raw_rows[0]]
    if header and header[-1] == '':
        header.pop()  # 同花顺每行可能带末尾制表符
    if (not header or any(not name or '\n' in name or '\r' in name or '\t' in name for name in header)
        or len(header) != len(set(header)) or kind not in matching_page_kinds(tuple(header))):
        raise SnapshotError(f'{kind} 表头不匹配或重复')
    code_index = header.index('证券代码')
    order_index = header.index('合同编号') if '合同编号' in header else None
    trade_index = header.index('成交编号') if '成交编号' in header else None
    parsed: list[dict[str, str]] = []
    placeholder_lines: list[int] = []
    for line, values in enumerate(raw_rows[1:], start=2):
        if values and values[-1] == '' and len(values) == len(header) + 1:
            values = values[:-1]
        if len(values) != len(header):
            raise SnapshotError(f'第{line}行列数不符')
        cells = [x.strip() for x in values]
        if any('\n' in cell or '\r' in cell or '\t' in cell for cell in cells):
            raise SnapshotError(f'第{line}行包含嵌入式换行或制表符')
        code = cells[code_index]
        if not code:
            # 仅跳过已观察到的空/零占位行；分组、汇总或有值但无代码的行不可丢弃。
            if order_index is not None and cells[order_index]:
                raise SnapshotError(f'第{line}行合同号存在但证券代码缺失')
            if trade_index is not None and cells[trade_index]:
                raise SnapshotError(f'第{line}行成交号存在但证券代码缺失')
            if not _empty_placeholder(cells):
                raise SnapshotError(f'第{line}行无证券代码且不是空占位行')
            placeholder_lines.append(line)
            continue
        if not re.fullmatch(r'\d{6}', code):
            raise SnapshotError(f'第{line}行证券代码非法')
        row = dict(zip(header, cells))
        if any(not row[column] for column in SCHEMAS[kind]):
            raise SnapshotError(f'第{line}行必需列值缺失')
        parsed.append(row)
    reason = '剪贴板文本无法证明筛选状态、分页状态及是否覆盖全量记录'
    if kind in {'orders', 'cancelable'}:
        reason += '；委托与可撤历史样本表头相同，剪贴板无法确认页面身份'
    if placeholder_lines:
        reason += f'；跳过{len(placeholder_lines)}行空/零占位行，须由页面证据确认其身份'
    return TableSnapshot(kind, sequence_before, sequence_after, tuple(header), tuple(parsed),
                         completeness_reason=reason)
