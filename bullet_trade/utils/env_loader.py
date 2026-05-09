"""
环境变量加载器

使用 python-dotenv 加载 .env 文件中的配置
"""

import os
from pathlib import Path
from typing import Any, Optional


def load_env(env_file: Optional[str] = None, verbose: bool = False, override: bool = False) -> None:
    """
    加载环境变量

    Args:
        env_file: .env 文件路径，默认为项目根目录的 .env
        verbose: 是否打印加载信息
    """
    try:
        from dotenv import load_dotenv

        if env_file is None:
            # 允许通过环境变量显式指定 env 文件
            for key in ("BT_ENV_FILE", "BULLET_TRADE_ENV_FILE", "ENV_FILE"):
                candidate = os.environ.get(key)
                if candidate:
                    env_file = candidate
                    break
            # 查找 .env 文件（从当前目录向上查找）
            current_dir = Path.cwd()
            while current_dir != current_dir.parent:
                env_path = current_dir / ".env"
                if env_path.exists():
                    env_file = str(env_path)
                    break
                current_dir = current_dir.parent

        if env_file and Path(env_file).exists():
            load_dotenv(env_file, override=override)
            if verbose:
                print(f"✓ 已加载环境变量: {env_file}")
        elif verbose:
            print("⚠ 未找到 .env 文件，将使用系统环境变量")

    except ImportError:
        if verbose:
            print("⚠ 未安装 python-dotenv，跳过 .env 文件加载")


def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    获取环境变量

    Args:
        key: 环境变量名
        default: 默认值

    Returns:
        环境变量值
    """
    return os.environ.get(key, default)


def parse_bool(value: Any, default: bool = False) -> bool:
    """
    将字符串、数字或布尔值解析为布尔类型。
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    return bool(value)


def get_env_bool(key: str, default: bool = False) -> bool:
    """
    获取布尔类型环境变量

    Args:
        key: 环境变量名
        default: 默认值

    Returns:
        布尔值
    """
    value = get_env(key)
    return parse_bool(value, default=default)


def get_env_optional_bool(key: str) -> Optional[bool]:
    """
    获取布尔类型环境变量（可为空）

    Args:
        key: 环境变量名

    Returns:
        布尔值或 None（未设置）
    """
    value = get_env(key)
    if value is None:
        return None
    return parse_bool(value)


def get_env_int(key: str, default: int = 0) -> int:
    """
    获取整数类型环境变量

    Args:
        key: 环境变量名
        default: 默认值

    Returns:
        整数值
    """
    value = get_env(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def get_env_float(key: str, default: float = 0.0) -> float:
    """
    获取浮点数类型环境变量

    Args:
        key: 环境变量名
        default: 默认值

    Returns:
        浮点数值
    """
    value = get_env(key)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def get_data_provider_config() -> dict:
    """
    获取数据提供者配置

    Returns:
        配置字典
    """
    base_cache_dir = get_env("DATA_CACHE_DIR")

    def cache_dir_for(name: str) -> Optional[str]:
        if not base_cache_dir:
            return None
        return str(Path(base_cache_dir) / name)

    return {
        "default": get_env("DEFAULT_DATA_PROVIDER", "jqdata"),
        "jqdata": {
            "username": get_env("JQDATA_USERNAME"),
            "password": get_env("JQDATA_PASSWORD"),
            "server": get_env("JQDATA_SERVER"),
            "port": get_env_int("JQDATA_PORT", 0),
            "cache_dir": cache_dir_for("jqdata"),
        },
        "tushare": {
            "token": get_env("TUSHARE_TOKEN"),
            "cache_dir": cache_dir_for("tushare"),
            "tushare_custom_url": get_env("TUSHARE_CUSTOM_URL"),
        },
        "qmt": {
            "data_dir": get_env("QMT_DATA_PATH"),
            "auto_download": get_env_optional_bool("MINIQMT_AUTO_DOWNLOAD"),
            "market": get_env("MINIQMT_MARKET"),
            "cache_dir": cache_dir_for("miniqmt"),
            "tushare_token": get_env("TUSHARE_TOKEN"),
            "tushare_custom_url": get_env("TUSHARE_CUSTOM_URL"),
        },
        "remote_qmt": {
            "host": get_env("QMT_SERVER_HOST"),
            "port": get_env_int("QMT_SERVER_PORT", 58620),
            "token": get_env("QMT_SERVER_TOKEN"),
            "tls_cert": get_env("QMT_SERVER_TLS_CERT"),
        },
    }


def get_broker_config() -> dict:
    """
    获取券商配置

    Returns:
        配置字典
    """
    return {
        "default": get_env("DEFAULT_BROKER", "simulator"),
        "qmt": {
            "account_id": get_env("QMT_ACCOUNT_ID"),
            "account_type": get_env("QMT_ACCOUNT_TYPE", "stock"),
            "data_path": get_env("QMT_DATA_PATH"),
            "session_id": get_env("QMT_SESSION_ID"),
            "auto_subscribe": get_env_optional_bool("QMT_AUTO_SUBSCRIBE"),
        },
        "simulator": {
            "initial_cash": get_env_int("SIMULATOR_INITIAL_CASH", 1000000),
        },
        "qmt-remote": {
            "host": get_env("QMT_SERVER_HOST", "127.0.0.1"),
            "port": get_env_int("QMT_SERVER_PORT", 58620),
            "token": get_env("QMT_SERVER_TOKEN"),
            "tls_cert": get_env("QMT_SERVER_TLS_CERT"),
            "account_key": get_env("QMT_SERVER_ACCOUNT_KEY"),
            "sub_account_id": get_env("QMT_SERVER_SUB_ACCOUNT"),
        },
    }


def get_system_config() -> dict:
    """
    获取系统配置

    Returns:
        配置字典，包含：
        - log_level: 控制台日志级别（默认 INFO）
        - log_file_level: 文件日志级别（默认跟随 log_level）
    """
    log_level = get_env("LOG_LEVEL", "INFO")
    # LOG_FILE_LEVEL 未设置时，跟随 LOG_LEVEL
    log_file_level = get_env("LOG_FILE_LEVEL") or log_level
    return {
        "log_dir": get_env("LOG_DIR", "./logs"),
        "log_level": log_level,
        "log_file_level": log_file_level,
    }


def get_risk_control_config() -> dict:
    """
    获取风控配置

    Returns:
        风控配置字典，包含以下字段：
        - max_order_value: 单笔订单最大金额（元）
        - max_daily_trade_value: 单日最大交易金额（元）
        - max_daily_trades: 单日最大交易次数
        - max_daily_cancels: 单日最大撤单次数
        - min_cancel_interval_seconds: 撤单最小时间间隔
        - max_cancel_per_order: 单笔订单最大撤单尝试次数
        - min_buy_order_value: 最小买入订单金额（元），0 表示不限制
        - max_stock_count: 最大持仓股票数
        - max_position_ratio: 单只股票最大仓位（%）
        - stop_loss_ratio: 止损比例（%）
    """
    return {
        "max_order_value": get_env_int("MAX_ORDER_VALUE", 100000),
        "max_daily_trade_value": get_env_int("MAX_DAILY_TRADE_VALUE", 500000),
        "max_daily_trades": get_env_int("MAX_DAILY_TRADES", 100),
        "max_daily_cancels": get_env_int("MAX_DAILY_CANCELS", 100),
        "min_cancel_interval_seconds": get_env_float("MIN_CANCEL_INTERVAL_SECONDS", 0.0),
        "max_cancel_per_order": get_env_int("MAX_CANCEL_PER_ORDER", 3),
        "min_buy_order_value": get_env_float("MIN_BUY_ORDER_VALUE", 0.0),
        "max_stock_count": get_env_int("MAX_STOCK_COUNT", 20),
        "max_position_ratio": get_env_float("MAX_POSITION_RATIO", 20.0),
        "stop_loss_ratio": get_env_float("STOP_LOSS_RATIO", 5.0),
    }


def get_live_trade_config() -> dict:
    """
    获取实盘交易相关配置。

    返回：
    - order_max_volume: 单笔委托最大数量（默认 1_000_000 股）

    - trade_max_wait_time: 同步下单最大等待秒数（默认 16，0 表示异步）

    - event_time_out: 策略事件超时秒数（默认 60）

    - strategy_name: 策略名称（用于实盘订单备注标识，可选）

    - scheduler_market_periods: 交易时段覆写字符串（如 '09:30-11:30,13:00-15:00'）

    - account_sync_interval / enabled: 账户同步间隔秒及开关（默认 60 / True）

    - order_sync_interval / enabled: 订单轮询间隔秒及开关（默认 10 / True）

    - g_autosave_interval / enabled: g 自动保存间隔及开关（默认 60 / True）

    - tick_subscription_limit: Tick 订阅标的上限（默认 100）

    - tick_sync_interval / enabled: Tick 轮询间隔及开关（默认 2 / True）

    - risk_check_interval / enabled: 风控后台任务（默认 300 / False）

    - broker_heartbeat_interval: 券商心跳检测间隔（默认 30）

    - runtime_dir: 运行时持久化目录（默认 './runtime'）

    """

    return {
        "order_max_volume": get_env_int("ORDER_MAX_VOLUME", 1_000_000),
        "trade_max_wait_time": get_env_int("TRADE_MAX_WAIT_TIME", 16),
        "event_time_out": get_env_int("EVENT_TIME_OUT", 60),
        "strategy_name": get_env("STRATEGY_NAME"),
        "scheduler_market_periods": get_env("SCHEDULER_MARKET_PERIODS"),
        "account_sync_interval": get_env_int("ACCOUNT_SYNC_INTERVAL", 60),
        "account_sync_enabled": get_env_bool("ACCOUNT_SYNC_ENABLED", True),
        "order_sync_interval": get_env_int("ORDER_SYNC_INTERVAL", 10),
        "order_sync_enabled": get_env_bool("ORDER_SYNC_ENABLED", True),
        "g_autosave_interval": get_env_int("G_AUTOSAVE_INTERVAL", 60),
        "g_autosave_enabled": get_env_bool("G_AUTOSAVE_ENABLED", True),
        "tick_subscription_limit": get_env_int("TICK_SUBSCRIPTION_LIMIT", 100),
        "tick_sync_interval": get_env_int("TICK_SYNC_INTERVAL", 2),
        "tick_sync_enabled": get_env_bool("TICK_SYNC_ENABLED", True),
        "risk_check_interval": get_env_int("RISK_CHECK_INTERVAL", 300),
        "risk_check_enabled": get_env_bool("RISK_CHECK_ENABLED", False),
        "calendar_skip_weekend": get_env_bool("CALENDAR_SKIP_WEEKEND", True),
        "calendar_retry_minutes": get_env_int("CALENDAR_RETRY_MINUTES", 1),
        "broker_heartbeat_interval": get_env_int("BROKER_HEARTBEAT_INTERVAL", 30),
        "runtime_dir": get_env("RUNTIME_DIR", "./runtime"),
        "portfolio_refresh_throttle_ms": get_env_int("PORTFOLIO_REFRESH_THROTTLE_MS", 200),
        # 市价保护价默认 ±1.5%（可被 .env 覆盖）
        "market_buy_price_percent": get_env_float("MARKET_BUY_PRICE_PERCENT", 0.015),
        "market_sell_price_percent": get_env_float("MARKET_SELL_PRICE_PERCENT", -0.015),
    }


# 自动加载环境变量
if __name__ != "__main__":
    load_env()
