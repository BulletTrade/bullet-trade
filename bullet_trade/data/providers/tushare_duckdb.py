"""
tushare_duckdb — tushare 模块的「本地优先」封装。

用法
----
    import tushare_duckdb as ts

    ts.set_token("你的token")            # 同 tushare 用法
    df = ts.daily(ts_code="000001.SZ",
                  start_date="20260701", end_date="20260731")
    df2 = ts.fund_daily(ts_code="159865.SZ", ...)
    df3 = ts.adj_factor(ts_code="000001.SZ", ...)
    df4 = ts.fund_adj(ts_code="159865.SZ", ...)
    df5 = ts.pro_bar(ts_code="000001.SZ", asset="E", ...)

设计目标
--------
1. 以下方法被重写，规则一致：先查本地 DuckDB → 有数据直接返回 → 否则回退父逻辑：
   - `daily`      → 本地 `stk_daily`
   - `fund_daily` → 本地 `etf_daily`
   - `adj_factor` → 本地 `stk_adj_factor`
   - `fund_adj`   → 本地 `fund_adj_factor`
2. `pro_bar` 被重写：当 `adj=None、freq='D'、asset∈{FD,E}` 且无均线等短路条件
   满足时，走本地 daily / fund_daily；否则回退父函数 `ts.pro_bar(...)`。
3. 其它所有方法 / 属性通过模块 `__getattr__` 委托给 `tushare` 模块，
   行为与直接 `import tushare` 完全一致。

注意
----
- tushare 模块本身没有模块级 `daily` / `fund_daily` / `adj_factor` / `fund_adj`，
  父逻辑即 `ts.pro_api().<方法>(...)`；`pro_bar` 是 tushare 模块级函数，回退时
  传 `api=ts.pro_api()` 复用父逻辑。
- 本地表 `trade_date` 为 DATE 类型，返回时转回 tushare 的 `YYYYMMDD` 字符串，
  保证与远程结果一致；`fund_daily` 的列序中 pre_close 在 open 之前，与 daily 不同。
- 默认库路径 `./duckdb/tushare.duckdb`，可用 `set_db_path()` 覆盖。
"""

import os

import duckdb
import pandas as pd

import tushare as _ts


# ---------------------------------------------------------------------- #
# 模块配置
# ---------------------------------------------------------------------- #
_STK_DAILY_TABLE = "stk_daily"
_ETF_DAILY_TABLE = "etf_daily"
_STK_ADJ_FACTOR_TABLE = "stk_adj_factor"
_FUND_ADJ_FACTOR_TABLE = "fund_adj_factor"
_DEFAULT_DB_PATH = "./duckdb/tushare.duckdb"

#: 本地 stk_daily 的标准列（顺序与 tushare daily 默认返回一致）
_DAILY_COLUMNS = [
    "ts_code", "trade_date", "open", "high", "low", "close",
    "pre_close", "change", "pct_chg", "vol", "amount",
]

#: 本地 etf_daily 的标准列（顺序与 tushare fund_daily 默认返回一致，
#: 注意 fund_daily 的 pre_close 在 open 之前，与 daily 不同）
_FUND_DAILY_COLUMNS = [
    "ts_code", "trade_date", "pre_close", "open", "high", "low",
    "close", "change", "pct_chg", "vol", "amount",
]

#: 复权因子表的标准列（stk_adj_factor / fund_adj_factor 一致，
#: 顺序与 tushare adj_factor / fund_adj 默认返回一致）
_ADJ_FACTOR_COLUMNS = [
    "ts_code", "trade_date", "adj_factor",
]

_db_path = _DEFAULT_DB_PATH


def set_db_path(path):
    """设置本地 DuckDB 文件路径（默认 ./duckdb/tushare.duckdb）。"""
    global _db_path
    _db_path = path


# ---------------------------------------------------------------------- #
# 重写 daily / fund_daily / adj_factor / fund_adj
# ---------------------------------------------------------------------- #
def daily(fields='', **kwargs):
    """
    重写 tushare daily：

    1. 先用调用参数查询本地 `stk_daily` 表；
    2. 有数据 → 返回本地结果（列格式与远程一致）；
    3. 无数据（或本地库/表不可用）→ 回退 tushare 父逻辑 pro.daily。
    """
    return _local_common("daily", _STK_DAILY_TABLE, _DAILY_COLUMNS, fields, kwargs)


def fund_daily(fields='', **kwargs):
    """
    重写 tushare fund_daily：查本地 `etf_daily`，未命中回退 pro.fund_daily。
    """
    return _local_common(
        "fund_daily", _ETF_DAILY_TABLE, _FUND_DAILY_COLUMNS, fields, kwargs
    )


def adj_factor(fields='', **kwargs):
    """
    重写 tushare adj_factor：查本地 `stk_adj_factor`，未命中回退 pro.adj_factor。
    """
    return _local_common(
        "adj_factor", _STK_ADJ_FACTOR_TABLE, _ADJ_FACTOR_COLUMNS, fields, kwargs
    )


def fund_adj(fields='', **kwargs):
    """
    重写 tushare fund_adj：查本地 `fund_adj_factor`，未命中回退 pro.fund_adj。
    """
    return _local_common(
        "fund_adj", _FUND_ADJ_FACTOR_TABLE, _ADJ_FACTOR_COLUMNS, fields, kwargs
    )


def _local_common(api_name, table, columns, fields, kwargs):
    """
    daily / fund_daily / adj_factor / fund_adj 的公共重写逻辑：
    先查本地表，命中返回；未命中回退 tushare 父逻辑。
    """
    df = _query_local(table, columns, fields, kwargs)
    if df is not None and not df.empty:
        return df

    # 回退父逻辑：tushare 的 pro_api().{api_name}(...)
    return getattr(_ts.pro_api(), api_name)(fields=fields, **kwargs)


# ---------------------------------------------------------------------- #
# 重写 pro_bar
# ---------------------------------------------------------------------- #
def pro_bar(ts_code='', start_date='', end_date='', freq='D', asset='E',
            exchange='', adj=None, ma=[], factors=None, adjfactor=False,
            offset=None, limit=None, fields='', contract_type='', retry_count=3,
            api=None):
    """
    重写 tushare pro_bar：

    短路条件（全部满足时优先走本地）：
        adj=None、freq='D'、asset∈{FD,E}、无均线（ma 为 None 或空列表）、
        adjfactor∈{None,False}，且 fields/factors/offset/limit 均为默认值。
        （ma=[] 等价于 ma=None —— tushare 的默认无均线值）
    - asset='FD' → 调 fund_daily（查本地 etf_daily）
    - asset='E'  → 调 daily（查本地 stk_daily）
    命中非空 → 直接返回本地结果；
    否则 → 回退父函数 ts.pro_bar(..., api=api or ts.pro_api())。
    """
    # 归一化 freq / asset（与 tushare pro_bar 内部逻辑一致）
    freq_s = freq.strip()
    freq_norm = (
        freq_s.lower()
        if len(freq_s) >= 3
        else (freq_s.upper() if asset.strip().upper() != 'C' else freq_s.lower())
    )
    asset_norm = asset.strip().upper()

    no_ma = ma is None or (isinstance(ma, (list, tuple)) and len(ma) == 0)

    can_shortcut = (
        adj is None
        and freq_norm == 'D'
        and asset_norm in ('FD', 'E')
        and no_ma
        and adjfactor in (None, False)
        and (fields in ('', None))
        and (factors is None or len(factors) == 0)
        and offset is None
        and limit is None
    )
    if can_shortcut:
        kwargs = dict(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if asset_norm == 'FD':
            data = fund_daily(**kwargs)
        if asset_norm == 'E':  # 'E'
            data = daily(**kwargs)
        if data is not None and not data.empty:
            return data

    # 回退父函数 ts.pro_bar（api 未传入时用 ts.pro_api()）
    return _ts.pro_bar(
        ts_code=ts_code, api=api or _ts.pro_api(), start_date=start_date, end_date=end_date,
        freq=freq, asset=asset, exchange=exchange, adj=adj, ma=ma,
        factors=factors, adjfactor=adjfactor, offset=offset, limit=limit,
        fields=fields, contract_type=contract_type, retry_count=retry_count,
    )


# ---------------------------------------------------------------------- #
# 本地查询实现
# ---------------------------------------------------------------------- #
def _query_local(table, columns, fields='', kwargs=None):
    """构造 WHERE 条件查询本地表（stk_daily / etf_daily / 复权因子表）。"""
    kwargs = kwargs or {}
    ts_code = kwargs.get("ts_code")
    trade_date = kwargs.get("trade_date")
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")

    # 没有任何筛选条件：不查本地（避免全表扫描），直接回退远程
    if not any([ts_code, trade_date, start_date, end_date]):
        return None

    # 库文件不存在：回退远程
    if not os.path.exists(_db_path):
        return None

    conds, params = [], []
    if ts_code:
        conds.append("ts_code = ?")
        params.append(str(ts_code))
    if trade_date:
        conds.append("trade_date = ?")
        params.append(_to_date(trade_date))
    if start_date:
        conds.append("trade_date >= ?")
        params.append(_to_date(start_date))
    if end_date:
        conds.append("trade_date <= ?")
        params.append(_to_date(end_date))

    select_cols = _resolve_select_columns(fields, columns)
    sql = (
        f"SELECT {', '.join(select_cols)} "
        f"FROM {table} "
        f"WHERE {' AND '.join(conds)}"
    )
    try:
        con = duckdb.connect(_db_path, read_only=True)
        try:
            df = con.execute(sql, params).fetchdf()
        finally:
            con.close()
    except Exception:
        # 表不存在 / 查询异常：回退远程
        return None

    # trade_date 从 DATE 转回 tushare 的 YYYYMMDD 字符串
    if len(df) and "trade_date" in df.columns:
        df["trade_date"] = df["trade_date"].dt.strftime("%Y%m%d")
    return df


# ---------------------------------------------------------------------- #
# 工具函数
# ---------------------------------------------------------------------- #
def _to_date(s):
    """把 tushare 的 YYYYMMDD 字符串转成 DuckDB DATE 字面量格式。"""
    if isinstance(s, str):
        s = s.strip()
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:]}"
        return s
    return s


def _resolve_select_columns(fields, columns):
    """解析 fields 参数为合法的本地列列表（过滤不存在的列）。"""
    if fields:
        cols = [c.strip() for c in str(fields).split(",") if c.strip()]
        cols = [c for c in cols if c in columns]
        if cols:
            return cols
    return columns


# ---------------------------------------------------------------------- #
# 模块级继承：其它一切委托给 tushare 模块（PEP 562）
# ---------------------------------------------------------------------- #
def __getattr__(name):
    """模块级 __getattr__：本模块未定义的属性/方法全部委托给 tushare。"""
    return getattr(_ts, name)


def __dir__():
    """dir() 同时展示本模块与 tushare 模块的符号。"""
    return sorted(set(globals().keys()) | set(dir(_ts)))


if __name__ == "__main__":
    # 1) daily 本地命中（stk_daily 覆盖至今）
    df1 = daily(ts_code="000001.SZ", start_date="20260720", end_date="20260731")
    print("daily 本地命中 df1 shape:", df1.shape, "cols:", list(df1.columns))
    print(df1.head())

    # 2) fund_daily 本地命中（etf_daily 已同步）
    df2 = fund_daily(ts_code="159865.SZ", start_date="20260720", end_date="20260731")
    print("fund_daily 本地命中 df2 shape:", df2.shape, "cols:", list(df2.columns))
    print(df2.head())

    # 3) adj_factor 本地命中（本地覆盖区间 2020-01-02 ~ 2022-02-07）
    df3 = adj_factor(ts_code="000001.SZ", start_date="20220101", end_date="20220207")
    print("adj_factor 本地命中 df3 shape:", df3.shape, "cols:", list(df3.columns))
    print(df3.head())

    # 4) fund_adj 本地为空 → 回退远程（真实 token）
    df4 = fund_adj(ts_code="159865.SZ", start_date="20220101", end_date="20220207")
    print("fund_adj 回退 df4 shape:", df4.shape, "cols:", list(df4.columns))
    print(df4.head())

    # 5) pro_bar 短路：asset='E' 走本地 daily
    df5 = pro_bar(ts_code="000001.SZ", asset="E",
                  start_date="20260720", end_date="20260731")
    print("pro_bar 短路 df5 shape:", df5.shape, "cols:", list(df5.columns))
    print(df5.head())

    # 委托验证：set_token 等由 tushare 模块提供
    print("set_token 委托自 tushare:", getattr(_ts, "set_token"))
