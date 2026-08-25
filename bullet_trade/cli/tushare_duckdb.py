from datetime import datetime

import os
import subprocess
import sys
import threading
from pathlib import Path


def _run_capture(cmd, log_file=None):
    """以子进程方式运行命令，实时转发输出并截获所有行。

    子进程的 stdout/stderr 会逐行实时转发到父进程的 stdout/stderr，
    同时把读取到的每一行（去尾换行）收集起来返回，避免管道写满导致死锁。

    传入 ``log_file`` 时，无论子进程成功还是失败，截获的行都会追加写入该日志
    文件（带时间戳、命令与退出码头），方便事后排查。

    :param cmd: 命令参数列表
    :param log_file: 可选日志文件路径
    :return: 截获的输出行列表（stdout 与 stderr 混合，按读取顺序）
    :raises subprocess.CalledProcessError: 子进程非零退出时抛出
    """
    lines = []

    def _forward(stream, target):
        for line in iter(stream.readline, ""):
            lines.append(line.rstrip("\n"))
            target.write(line)
            target.flush()

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    stderr_thread = threading.Thread(target=_forward, args=(proc.stderr, sys.stderr))
    stderr_thread.start()
    try:
        _forward(proc.stdout, sys.stdout)
    finally:
        stderr_thread.join()
    proc.wait()

    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with log_file.open("a", encoding="utf-8") as f:
            f.write(
                f"\n===== {datetime.now():%Y-%m-%d %H:%M:%S} "
                f"{Path(cmd[1]).name} rc={proc.returncode} =====\n"
            )
            for line in lines:
                f.write(line + "\n")

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)
    return lines


def _load_tushare_env():
    """确保 .env 已加载，使 tushare_tasks_file / tushare_duckdb_path / TUSHARE_TOKEN 可用。

    优先加载包内 .env（不依赖当前工作目录），否则退回默认向上搜索。
    """
    from bullet_trade.utils.env_loader import load_env

    pkg_env = Path(__file__).resolve().parent.parent / ".env"
    if pkg_env.exists():
        load_env(str(pkg_env))
    else:
        load_env()


def run_persistence(args):
    """
    持久化数据到 DuckDB 数据库。

    命令行参数优先，缺省时回退到 .env 配置（tushare_tasks_file /
    tushare_duckdb_path / TUSHARE_TOKEN）。
    """
    from bullet_trade.cli.tushare_duckdb import persist_tushare_to_duckdb

    _load_tushare_env()
    tasks_file = args.tushare_tasks_file or os.environ.get("tushare_tasks_file")
    duckdb_path = args.tushare_duckdb_path or os.environ.get("tushare_duckdb_path")
    token = args.token or os.environ.get("TUSHARE_TOKEN")
    if not tasks_file:
        print("❌ 缺少 --tasks-file，且 .env 未配置 tushare_tasks_file")
        return 1
    if not duckdb_path:
        print("❌ 缺少 --duckdb-path，且 .env 未配置 tushare_duckdb_path")
        return 1

    db_path = duckdb_path
    try:
        persist_tushare_to_duckdb(
            db_path=db_path,
            tushare_token=token,
            tasks_file=tasks_file,
            duckdb_path=duckdb_path,
        )
        print(f"✓ 数据已持久化到 DuckDB 数据库: {db_path}")
        return 0
    except Exception as exc:  # pragma: no cover - 防御性兜底
        print(f"❌ 数据持久化失败: {exc}")
        import traceback

        traceback.print_exc()
        return 1


def persist_tushare_to_duckdb(db_path, tushare_token, tasks_file, duckdb_path):
    """
    将 Tushare 数据持久化到 DuckDB 数据库。

    通过子进程调用 helpers/tushare_persistence/sync_table.py 完成批量同步。

    :param db_path: DuckDB 数据库文件路径
    :param tushare_token: Tushare Token
    :param tasks_file: 同步任务文件路径（JSON，批量模式）
    :param duckdb_path: 传给 sync_table.py 的 --duckdb-path 参数
    """
    from bullet_trade.core.api import get_data_provider
    if tushare_token:
        os.environ.setdefault("TUSHARE_TOKEN", tushare_token)

    # 以子进程方式调用 sync_table.py，实时转发滚动输出、截获并落盘日志
    script = Path(__file__).resolve().parents[2] / "helpers" / "tushare_persistence" / "sync_table.py"
    cmd = [
        sys.executable,
        str(script),
        "--tasks-file", tasks_file,
        "--duckdb-path", duckdb_path,
    ]
    log_dir = Path(os.environ.get("LOG_DIR", "logs")).expanduser()
    log_file = log_dir / "tushare_sync.log"
    return _run_capture(cmd, log_file=log_file)