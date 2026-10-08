"""只读查询的离线证据门；不会访问客户端或推断空表的业务含义。"""
from __future__ import annotations

from dataclasses import dataclass

from .table_snapshot import SCHEMAS, TableSnapshot


@dataclass(frozen=True)
class QueryProof:
    """由调用方分别核验的页面证据；表头不能代替这些观察。"""

    requested_kind: str
    requested_account: str
    observed_page_kind: str = 'unknown'
    observed_account: str = 'unknown'
    page_observed: bool = False
    account_observed: bool = False
    filter_verified: bool = False
    pagination_verified: bool = False
    loading_complete: bool = False
    same_request: bool = False


@dataclass(frozen=True)
class QueryEvidence:
    captcha_acceptance_observed: bool
    fresh_table_parsed: bool
    table_completeness: str
    query_complete: bool
    reconciliation_status: str
    reasons: tuple[str, ...]


def assess_query(
    proof: QueryProof,
    snapshot: TableSnapshot | None,
    *,
    captcha_acceptance_observed: bool = False,
    clipboard_from_client: bool = False,
    clipboard_stable: bool = False,
    orders_snapshot: TableSnapshot | None = None,
    trades_snapshot: TableSnapshot | None = None,
) -> QueryEvidence:
    """合并已取得的证据；未知值保持未知，不把空行数当作无订单。"""
    if proof.requested_kind not in SCHEMAS:
        raise ValueError('unknown_query_kind')
    fresh = bool(
        snapshot is not None
        and snapshot.kind == proof.requested_kind
        and snapshot.clipboard_sequence_before >= 0
        and snapshot.clipboard_sequence_after > snapshot.clipboard_sequence_before
        and set(SCHEMAS[proof.requested_kind]) <= set(snapshot.columns)
        and clipboard_from_client is True
        and clipboard_stable is True
    )
    identity = bool(
        proof.page_observed is True
        and proof.observed_page_kind == proof.requested_kind
        and proof.account_observed is True
        and bool(proof.requested_account)
        and proof.requested_account == proof.observed_account
    )
    context = bool(
        identity and proof.filter_verified is True and proof.pagination_verified is True
        and proof.loading_complete is True and proof.same_request is True
    )
    # parse_table 当前始终返回 unknown；必须有独立核验后的快照才可进入 complete。
    complete = bool(
        fresh and context and snapshot is not None
        and snapshot.completeness == 'complete'
        and snapshot.page_identity == proof.requested_kind
    )
    inconsistent = bool(
        orders_snapshot is not None and trades_snapshot is not None
        and orders_snapshot.kind == 'orders' and trades_snapshot.kind == 'trades'
        and not orders_snapshot.rows and bool(trades_snapshot.rows)
    )
    reasons: list[str] = []
    if captcha_acceptance_observed is not True:
        reasons.append('captcha_acceptance_unverified')
    if not fresh:
        reasons.append('fresh_table_unverified')
    if not identity:
        reasons.append('page_or_account_unverified')
    if not context:
        reasons.append('filter_pagination_loading_or_request_unverified')
    if snapshot is None or snapshot.completeness != 'complete' or snapshot.page_identity != proof.requested_kind:
        reasons.append('snapshot_completeness_or_page_unknown')
    if inconsistent:
        reasons.append('trades_exist_but_orders_empty')
    return QueryEvidence(
        captcha_acceptance_observed=captcha_acceptance_observed is True,
        fresh_table_parsed=fresh,
        table_completeness='inconsistent' if inconsistent else ('complete' if complete else 'unknown'),
        query_complete=bool(captcha_acceptance_observed is True and complete and not inconsistent),
        reconciliation_status='pending_reconcile' if inconsistent else 'unknown',
        reasons=tuple(reasons),
    )
