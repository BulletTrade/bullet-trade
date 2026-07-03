from __future__ import annotations

import asyncio
import signal
from typing import Optional

from bullet_trade.core.globals import log
from bullet_trade.utils.env_loader import load_env

from .adapters import get_adapter  # type: ignore
from .adapters import big_qmt  # noqa: F401
from .adapters import qmt  # noqa: F401
from .adapters.base import AccountRouter
from .app import ServerApplication
from .config import build_server_config


def _install_signal_handlers(app: ServerApplication) -> None:
    loop = asyncio.get_running_loop()

    def _handle(sig):
        log.info(f"收到信号 {sig.name}，准备退出 qmt server")
        asyncio.create_task(app.shutdown())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle, sig)
        except NotImplementedError:
            pass


async def _async_main(args) -> int:
    # 如果显式提供 env 文件，则覆盖加载一次（优先于默认 .env）
    try:
        env_file = getattr(args, "env_file", None)
        if env_file:
            load_env(env_file=env_file, verbose=False, override=True)
    except Exception:
        pass
    config = build_server_config(args)
    if config.log_file:
        try:
            log.configure_file_logging(file_path=config.log_file)
        except Exception:
            log.warning("无法将日志写入 %s，继续使用默认输出", config.log_file)
    router = AccountRouter(config.accounts)
    builder = get_adapter(config.server_type)
    bundle = builder(config, router)
    app = ServerApplication(config=config, router=router, adapters=bundle)
    _install_signal_handlers(app)
    if config.generated_token:
        log.warning("未配置 QMT_SERVER_TOKEN，已临时生成随机 token: %s", config.token)
    await app.start()
    return 0


def run_server_command(args) -> int:
    """
    CLI entry for `bullet-trade server ...`.
    """
    try:
        return asyncio.run(_async_main(args))
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        log.error(f"启动 qmt server 失败: {exc}")
        return 1
