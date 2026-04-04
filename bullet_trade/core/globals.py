"""
全局对象

提供全局变量 g 和日志系统 log
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from typing import Any, Optional
from datetime import datetime


class GlobalVariables:
    """
    全局变量容器 g
    
    用于存储用户的各类可被 pickle.dumps 函数序列化的全局数据
    """
    
    def __init__(self):
        self._data = {}
    
    def __setattr__(self, name: str, value: Any):
        if name == '_data':
            super().__setattr__(name, value)
        else:
            self._data[name] = value
    
    def __getattr__(self, name: str):
        if name == '_data':
            return super().__getattribute__(name)
        return self._data.get(name)
    
    def __delattr__(self, name: str):
        if name in self._data:
            del self._data[name]
    
    def __contains__(self, name: str):
        return name in self._data
    
    def __repr__(self):
        return f"GlobalVariables({self._data})"
    
    def clear(self):
        """清空所有全局变量"""
        self._data = {}


STRATEGY_LABEL = "策略"
CURRENT_LABEL = "当前"
STRATEGY_TIME_LABEL = "策略时间"


class _ColorFormatter(logging.Formatter):
    """
    为控制台输出增加简单的 ANSI 颜色（文件日志不加色）。
    """

    COLORS = {
        logging.DEBUG: "\033[90m",  # 灰
        logging.INFO: "",
        logging.WARNING: "\033[33m",  # 黄/橙
        logging.ERROR: "\033[31m",  # 红
        logging.CRITICAL: "\033[31m",
    }
    RESET = "\033[0m"

    def __init__(
        self,
        fmt: str,
        datefmt: Optional[str],
        color_enabled: bool,
        compact_fmt: Optional[str] = None,
    ) -> None:
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.full_fmt = fmt
        self.compact_fmt = compact_fmt or fmt
        self.color_enabled = color_enabled

    def format(self, record: logging.LogRecord) -> str:
        original_fmt = self._style._fmt
        try:
            if getattr(record, "hide_wall_time", False):
                self._style._fmt = self.compact_fmt
            else:
                self._style._fmt = self.full_fmt
            msg = super().format(record)
        finally:
            self._style._fmt = original_fmt
        if not self.color_enabled:
            return msg
        prefix = self.COLORS.get(record.levelno, "")
        if not prefix:
            return msg
        return f"{prefix}{msg}{self.RESET}"


class Logger:
    """
    日志系统
    
    提供不同级别的日志输出功能
    """
    
    def __init__(self):
        self.logger = logging.getLogger('jq_strategy')
        self.logger.setLevel(logging.INFO)
        self.strategy_time = None  # 策略时间（回测时间）
        self._file_handler: Optional[RotatingFileHandler] = None
        # 统一格式
        self._formatter = _ColorFormatter(
            '%(asctime)s [%(levelname)s] %(message)s',
            '%Y-%m-%d %H:%M:%S',
            False,
            compact_fmt='[%(levelname)s] %(message)s',
        )
        self._color_enabled = self._detect_color_support()

        if not self.logger.handlers:
            # 从环境变量读取日志级别配置
            try:
                from bullet_trade.utils.env_loader import get_system_config  # type: ignore
                sys_cfg = get_system_config() or {}
            except Exception:
                sys_cfg = {}
            
            # 控制台日志级别（LOG_LEVEL）
            console_level_name = str(sys_cfg.get('log_level', 'INFO')).upper()
            console_level = getattr(logging, console_level_name, logging.INFO)
            
            # 文件日志级别（LOG_FILE_LEVEL，未设置则跟随 LOG_LEVEL）
            file_level_name = str(sys_cfg.get('log_file_level', console_level_name)).upper()
            file_level = getattr(logging, file_level_name, console_level)
            
            # Logger 主体级别取两者中更低的（以便两个 handler 都能收到消息）
            self.logger.setLevel(min(console_level, file_level))
            
            # 控制台 handler
            sh = logging.StreamHandler()
            sh.setLevel(console_level)
            sh.setFormatter(
                _ColorFormatter(
                    '%(asctime)s [%(levelname)s] %(message)s',
                    '%Y-%m-%d %H:%M:%S',
                    self._color_enabled,
                    compact_fmt='[%(levelname)s] %(message)s',
                )
            )
            self.logger.addHandler(sh)

            # 文件 handler（按 .env 配置）
            try:
                log_dir = sys_cfg.get('log_dir') or './logs'
                os.makedirs(log_dir, exist_ok=True)
                fh = RotatingFileHandler(os.path.join(log_dir, 'app.log'), maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
                fh.setLevel(file_level)
                fh.setFormatter(self._formatter)
                self.logger.addHandler(fh)
                self._file_handler = fh
            except Exception:
                # 文件日志失败不阻断
                pass
        
        self._levels = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
            'system': logging.CRITICAL,
        }
        
        # 分模块日志级别（聚宽兼容）
        self._module_levels = {
            'system': logging.INFO,  # 系统日志级别
            'strategy': logging.INFO,  # 策略日志级别（保持INFO，不受system影响）
        }

        # 让 bullet_trade.* 标准 logger 复用同一套 handler，方便 CLI 场景统一屏幕/文件输出
        self._sync_standard_logger()

    def configure_file_logging(
        self, *, log_dir: Optional[str] = None, file_path: Optional[str] = None, level_name: Optional[str] = None
    ) -> None:
        """
        运行时更新文件日志路径或级别，供 CLI 覆写等场景使用。
        """
        if log_dir is None and file_path is None and level_name is None:
            return

        target_level = self.logger.level
        if level_name:
            candidate = getattr(logging, str(level_name).upper(), None)
            if isinstance(candidate, int):
                target_level = candidate
        self.logger.setLevel(target_level)
        for handler in self.logger.handlers:
            handler.setLevel(target_level)

        if not log_dir and not file_path:
            return

        try:
            if file_path:
                target_path = os.path.abspath(os.path.expanduser(file_path))
                directory = os.path.dirname(target_path) or "."
            else:
                directory = os.path.abspath(os.path.expanduser(log_dir or "."))
                target_path = os.path.join(directory, "app.log")
            os.makedirs(directory, exist_ok=True)
            fh = RotatingFileHandler(
                target_path,
                maxBytes=5 * 1024 * 1024,
                backupCount=3,
                encoding='utf-8',
            )
            fh.setLevel(target_level)
            fh.setFormatter(self._formatter)
            if self._file_handler:
                try:
                    self.logger.removeHandler(self._file_handler)
                except Exception:
                    pass
                try:
                    self._file_handler.close()
                except Exception:
                    pass
            self.logger.addHandler(fh)
            self._file_handler = fh
            self._sync_standard_logger()
        except Exception:
            pass

    def _detect_color_support(self) -> bool:
        if os.getenv("LOG_FORCE_COLOR"):
            return True
        if os.getenv("NO_COLOR"):
            return False
        try:
            if sys.platform.startswith("win"):
                # 新版 Windows 终端默认支持 ANSI，失败则回退为 False
                return sys.stdout.isatty()
            return sys.stdout.isatty()
        except Exception:
            return False

    def _sync_standard_logger(self) -> None:
        """将 bullet_trade.* 标准 logger 的 handler/级别与全局 logger 保持一致，便于 CLI 后台/文件统一输出"""
        try:
            std_logger = logging.getLogger("bullet_trade")
            std_logger.handlers.clear()
            for handler in self.logger.handlers:
                std_logger.addHandler(handler)
            std_logger.setLevel(self.logger.level)
            std_logger.propagate = False
        except Exception:
            pass

    def reload_from_env(self) -> None:
        """
        根据环境变量刷新日志配置。
        """
        try:
            from bullet_trade.utils.env_loader import get_system_config  # type: ignore
            sys_cfg = get_system_config() or {}
        except Exception:
            return

        console_level_name = str(sys_cfg.get('log_level', 'INFO')).upper()
        console_level = getattr(logging, console_level_name, logging.INFO)
        file_level_name = str(sys_cfg.get('log_file_level', console_level_name)).upper()
        file_level = getattr(logging, file_level_name, console_level)

        self.logger.setLevel(min(console_level, file_level))
        for handler in self.logger.handlers:
            if self._file_handler and handler is self._file_handler:
                handler.setLevel(file_level)
            else:
                handler.setLevel(console_level)

        log_dir = sys_cfg.get('log_dir') or './logs'
        if log_dir:
            try:
                target_dir = os.path.abspath(os.path.expanduser(log_dir))
                target_path = os.path.join(target_dir, 'app.log')
                current_dir = ''
                if self._file_handler:
                    current_dir = os.path.dirname(getattr(self._file_handler, 'baseFilename', '') or '')
                if not self._file_handler or os.path.abspath(current_dir) != target_dir:
                    os.makedirs(target_dir, exist_ok=True)
                    fh = RotatingFileHandler(
                        target_path,
                        maxBytes=5 * 1024 * 1024,
                        backupCount=3,
                        encoding='utf-8',
                    )
                    fh.setLevel(file_level)
                    fh.setFormatter(self._formatter)
                    if self._file_handler:
                        try:
                            self.logger.removeHandler(self._file_handler)
                        except Exception:
                            pass
                        try:
                            self._file_handler.close()
                        except Exception:
                            pass
                    self.logger.addHandler(fh)
                    self._file_handler = fh
            except Exception:
                pass

        self._sync_standard_logger()
    
    def set_strategy_time(self, dt):
        """设置策略时间（回测时间）"""
        self.strategy_time = dt
    
    def _format_message(self, msg: str) -> str:
        """格式化消息，添加策略时间"""
        try:
            if not self.strategy_time:
                return msg
            now = datetime.now()
            from .globals import g as _g  # type: ignore
            is_live = bool(getattr(_g, 'live_trade', False))
            strategy_str = self.strategy_time.strftime("%Y-%m-%d %H:%M:%S")
            if is_live:
                delay = (now - self.strategy_time).total_seconds()
                current_str = now.strftime("%Y-%m-%d %H:%M:%S")
                return (
                    f"[{STRATEGY_LABEL}:{strategy_str}] "
                    f"[{CURRENT_LABEL}:{current_str}] "
                    f"[delay={delay:+.3f}s] {msg}"
                )
            return f"[{strategy_str}] {msg}"
        except Exception:
            return msg

    def _with_log_extra(self, kwargs: dict) -> dict:
        extra = kwargs.get("extra")
        if extra is None:
            extra = {}
        else:
            extra = dict(extra)
        try:
            from .globals import g as _g  # type: ignore
            if self.strategy_time and not bool(getattr(_g, 'live_trade', False)):
                extra.setdefault("hide_wall_time", True)
        except Exception:
            pass
        kwargs["extra"] = extra
        return kwargs


    def debug(self, msg: str, *args, **kwargs):
        """输出DEBUG级别日志"""
        kwargs = self._with_log_extra(kwargs)
        self.logger.debug(self._format_message(msg), *args, **kwargs)
    
    def info(self, msg: str, *args, **kwargs):
        """输出INFO级别日志"""
        kwargs = self._with_log_extra(kwargs)
        self.logger.info(self._format_message(msg), *args, **kwargs)
    
    def warn(self, msg: str, *args, **kwargs):
        """输出WARNING级别日志"""
        kwargs = self._with_log_extra(kwargs)
        self.logger.warning(self._format_message(msg), *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs):
        """输出WARNING级别日志（别名）"""
        kwargs = self._with_log_extra(kwargs)
        self.logger.warning(self._format_message(msg), *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs):
        """输出ERROR级别日志"""
        kwargs = self._with_log_extra(kwargs)
        self.logger.error(self._format_message(msg), *args, **kwargs)
    
    def critical(self, msg: str, *args, **kwargs):
        """输出CRITICAL级别日志"""
        kwargs = self._with_log_extra(kwargs)
        self.logger.critical(self._format_message(msg), *args, **kwargs)
    
    def set_level(self, module: str, level: str):
        """
        设置日志级别（聚宽兼容）
        
        Args:
            module: 模块名称
                - 'system': 系统日志（回测引擎产生的日志）
                - 'strategy': 策略日志（用户策略中的log.info()等）
            level: 日志级别（'debug', 'info', 'warning', 'error'）
            
        Note:
            设置 'system' 为 'error' 只会影响系统日志，不会影响策略日志
            这样用户策略中的 log.info() 仍然可以正常输出
        """
        if level in self._levels:
            # 保存模块级别设置
            if module in self._module_levels:
                self._module_levels[module] = self._levels[level]
            
            # 注意：在聚宽中，set_level('system', 'error') 
            # 只影响系统日志，不影响策略日志
            # 我们的实现：策略日志（用户调用log.info）始终可以输出
            # 只有系统日志（回测引擎内部）受module='system'影响
            
            # 如果设置的是strategy模块，才改变logger级别
            if module == 'strategy':
                self.logger.setLevel(self._levels[level])
                for handler in self.logger.handlers:
                    handler.setLevel(self._levels[level])
                self._sync_standard_logger()


# 创建全局单例
g = GlobalVariables()
log = Logger()


def reset_globals():
    """重置全局变量（用于回测开始时）"""
    g.clear()


__all__ = ['g', 'log', 'reset_globals']
