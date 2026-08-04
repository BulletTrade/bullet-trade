-- ============================================================================
-- tushare.initdb.sql
-- 数据库结构初始化脚本（仅结构，不含数据）
--
-- 来源数据库 : ./duckdb/tushare.duckdb
-- 导出日期   : 2026-08-03
-- 包含对象   : 9 张表（CREATE TABLE）、主键、NOT NULL、二级索引
--
-- 用法：
--   python -c "import duckdb; duckdb.connect('./duckdb/tushare.duckdb').execute(open('tushare.initdb.sql').read())"
--   或 duckdb ./duckdb/tushare.duckdb < tushare.initdb.sql
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. 股票基本信息 (stk_info)
--    主键: ts_code
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stk_info (
    ts_code        VARCHAR NOT NULL,
    symbol         VARCHAR,
    name           VARCHAR,
    area           VARCHAR,
    industry       VARCHAR,
    cnspell        VARCHAR,
    market         VARCHAR,
    list_date      VARCHAR,
    act_name       VARCHAR,
    act_ent_type   VARCHAR,
    PRIMARY KEY (ts_code)
);

CREATE INDEX IF NOT EXISTS idx_stk_info_list_date ON stk_info (list_date);
CREATE INDEX IF NOT EXISTS idx_stk_info_industry  ON stk_info (industry);

-- ---------------------------------------------------------------------------
-- 2. A股日线行情 (stk_daily)
--    主键: ts_code + trade_date
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stk_daily (
    ts_code      VARCHAR   NOT NULL,
    trade_date   DATE      NOT NULL,
    open         DECIMAL(18,4),
    high         DECIMAL(18,4),
    low          DECIMAL(18,4),
    close        DECIMAL(18,4),
    pre_close    DECIMAL(18,4),
    change       DECIMAL(18,4),
    pct_chg      DECIMAL(18,4),
    vol          DECIMAL(18,4),
    amount       DECIMAL(18,4),
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_stk_daily_trade_date ON stk_daily (trade_date);

-- ---------------------------------------------------------------------------
-- 3. 股票复权因子 (stk_adj_factor)
--    主键: ts_code + trade_date
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stk_adj_factor (
    ts_code      VARCHAR   NOT NULL,
    trade_date   DATE      NOT NULL,
    adj_factor   DECIMAL(18,4),
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_stk_adj_factor_trade_date ON stk_adj_factor (trade_date);

-- ---------------------------------------------------------------------------
-- 4. ETF基础信息 (etf_basic_info)
--    主键: ts_code
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etf_basic_info (
    ts_code        VARCHAR NOT NULL,
    csname         VARCHAR,
    extname        VARCHAR,
    cname          VARCHAR,
    index_code     VARCHAR,
    index_name     VARCHAR,
    setup_date     VARCHAR,
    list_date      VARCHAR,
    list_status    VARCHAR,
    exchange       VARCHAR,
    mgr_name       VARCHAR,
    custod_name    VARCHAR,
    mgt_fee        DOUBLE,
    etf_type       VARCHAR,
    PRIMARY KEY (ts_code)
);

-- ---------------------------------------------------------------------------
-- 5. ETF日线行情 (etf_daily)
--    主键: ts_code + trade_date
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etf_daily (
    ts_code      VARCHAR   NOT NULL,
    trade_date   DATE      NOT NULL,
    pre_close    DECIMAL(18,4),
    open         DECIMAL(18,4),
    high         DECIMAL(18,4),
    low          DECIMAL(18,4),
    close        DECIMAL(18,4),
    change       DECIMAL(18,4),
    pct_chg      DECIMAL(18,4),
    vol          DECIMAL(18,4),
    amount       DECIMAL(18,4),
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_etf_daily_trade_date ON etf_daily (trade_date);

-- ---------------------------------------------------------------------------
-- 6. 基金复权因子 (fund_adj_factor)
--    主键: ts_code + trade_date
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fund_adj_factor (
    ts_code      VARCHAR   NOT NULL,
    trade_date   DATE      NOT NULL,
    adj_factor   DECIMAL(18,4),
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_fund_adj_factor_trade_date ON fund_adj_factor (trade_date);


-- ---------------------------------------------------------------------------
-- 7. 同步状态记录 (table_sync_state)
--    注: 源库中无主键约束
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS table_sync_state (
    source_table    VARCHAR,
    dimension_type  VARCHAR,
    dimension_value VARCHAR,
    is_sync         INTEGER,
    error_message   VARCHAR,
    updated_at      TIMESTAMP
);
