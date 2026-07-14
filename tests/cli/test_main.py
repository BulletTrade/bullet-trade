import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from bullet_trade.cli.main import apply_cli_overrides
from bullet_trade.core.globals import Logger


def test_main_env_file_triggers_refresh(monkeypatch):
    import bullet_trade.cli.main as main_mod
    import bullet_trade.utils.env_loader as env_loader

    called = {"load": None, "override": None, "load_calls": 0, "refresh": 0}

    def fake_load_env(env_file=None, override=False):
        called["load"] = env_file
        called["override"] = override
        called["load_calls"] += 1

    def fake_refresh():
        called["refresh"] += 1

    monkeypatch.setattr(env_loader, "load_env", fake_load_env)
    monkeypatch.setattr(main_mod, "_refresh_env_dependents", fake_refresh)
    monkeypatch.setattr(sys, "argv", ["bullet-trade", "--env-file", "custom.env"])

    exit_code = main_mod.main()

    assert exit_code == 0
    assert called["load"] == "custom.env"
    assert called["override"] is True
    assert called["load_calls"] == 1
    assert called["refresh"] == 1


def test_main_env_file_without_command_does_not_create_provider(monkeypatch):
    """验证显式 env 文件仅打印帮助时刷新配置但不初始化数据连接。"""
    import bullet_trade.cli.main as main_mod
    import bullet_trade.core.globals as globals_mod
    import bullet_trade.data.api as data_api
    import bullet_trade.utils.env_loader as env_loader

    create_calls = []
    old_provider = object()

    def fake_create(provider_name=None, overrides=None):
        create_calls.append((provider_name, overrides))
        return object()

    monkeypatch.setattr(env_loader, "load_env", lambda env_file=None, override=False: None)
    monkeypatch.setattr(globals_mod.log, "reload_from_env", lambda: None)
    monkeypatch.setattr(data_api, "_provider", old_provider)
    monkeypatch.setattr(data_api, "_provider_cache", {"base": old_provider})
    monkeypatch.setattr(data_api, "_provider_auth_attempted", {"base": True})
    monkeypatch.setattr(data_api, "_auth_attempted", True)
    monkeypatch.setattr(data_api, "_create_provider", fake_create)
    monkeypatch.setattr(data_api, "_pending_default_provider_name", None)
    monkeypatch.setattr(data_api, "_security_info_cache", {"cached": object()})
    monkeypatch.setattr(data_api, "_cache_forced_off_warned", True)
    monkeypatch.setattr(sys, "argv", ["bullet-trade", "--env-file", "custom.env"])

    exit_code = main_mod.main()

    assert exit_code == 0
    assert create_calls == []
    assert data_api._provider is None
    assert data_api._provider_cache == {}


def test_main_without_env_file_skips_refresh(monkeypatch):
    import bullet_trade.cli.main as main_mod
    import bullet_trade.utils.env_loader as env_loader

    called = {"load_calls": 0, "refresh": 0}

    def fake_load_env(env_file=None, override=False):
        called["load_calls"] += 1

    def fake_refresh():
        called["refresh"] += 1

    monkeypatch.setattr(env_loader, "load_env", fake_load_env)
    monkeypatch.setattr(main_mod, "_refresh_env_dependents", fake_refresh)
    monkeypatch.setattr(sys, "argv", ["bullet-trade"])

    exit_code = main_mod.main()

    assert exit_code == 0
    assert called["load_calls"] == 0
    assert called["refresh"] == 0


def test_apply_cli_overrides_updates_log_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("LOG_DIR", raising=False)
    bootstrap_dir = tmp_path / "bootstrap"
    monkeypatch.setenv("LOG_DIR", str(bootstrap_dir))
    logger = Logger()

    log_dir = tmp_path / "cli_logs"
    args = SimpleNamespace(log_dir=str(log_dir), runtime_dir=None)

    overrides = apply_cli_overrides(args, logger=logger)

    assert overrides == {}
    assert os.environ["LOG_DIR"] == str(log_dir.resolve())
    handler = logger._file_handler
    assert handler is not None
    assert Path(handler.baseFilename).parent == log_dir.resolve()


def test_apply_cli_overrides_sets_runtime_override(tmp_path, monkeypatch):
    monkeypatch.delenv("RUNTIME_DIR", raising=False)
    runtime_dir = tmp_path / "runtime"
    args = SimpleNamespace(runtime_dir=str(runtime_dir), log_dir=None)

    overrides = apply_cli_overrides(args)

    expected = str(runtime_dir.resolve())
    assert overrides["runtime_dir"] == expected
    assert os.environ["RUNTIME_DIR"] == expected


@pytest.mark.parametrize(
    ("argument", "expected_returncode", "expected_text"),
    [
        ("--help", 0, "usage: bullet-trade"),
        ("--version", 0, "bullet-trade "),
        ("--vision", 2, "--vision"),
    ],
)
def test_cli_metadata_does_not_connect_remote_qmt_from_cwd_env(
    tmp_path, argument, expected_returncode, expected_text
):
    """验证 CLI 元命令会读取 cwd 配置，但不会在参数解析前连接远程 QMT。

    Args:
        tmp_path: pytest 提供的临时工作目录。
        argument: 本次子进程使用的 CLI 参数。
        expected_returncode: argparse 对该参数的预期退出码。
        expected_text: 标准输出或错误输出中应出现的文本。

    Side Effects:
        在临时目录创建 `.env`、`sitecustomize.py` 和探针标记文件，并启动一个全新 Python
        子进程。探针会在子进程尝试 `asyncio.open_connection` 时立即终止该子进程，避免访问真实网络。
    """
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "DEFAULT_DATA_PROVIDER=qmt-remote",
                "QMT_SERVER_HOST=127.0.0.1",
                "QMT_SERVER_PORT=1",
                "QMT_SERVER_TOKEN=cli-bootstrap-test",
                "BT_CLI_TEST_ENV_LOADED=from-cwd-dotenv",
            ]
        ),
        encoding="utf-8",
    )

    ready_marker = tmp_path / "sitecustomize-ready"
    connect_marker = tmp_path / "qmt-connect-attempted"
    (tmp_path / "sitecustomize.py").write_text(
        "\n".join(
            [
                "import asyncio",
                "import os",
                "from pathlib import Path",
                "Path(os.environ['BT_CLI_READY_MARKER']).write_text('ready', encoding='utf-8')",
                "async def _forbidden_open_connection(*args, **kwargs):",
                "    Path(os.environ['BT_CLI_CONNECT_MARKER']).write_text('attempted', encoding='utf-8')",
                "    os._exit(91)",
                "asyncio.open_connection = _forbidden_open_connection",
            ]
        ),
        encoding="utf-8",
    )

    child_env = os.environ.copy()
    for key in (
        "DEFAULT_DATA_PROVIDER",
        "QMT_SERVER_HOST",
        "QMT_SERVER_PORT",
        "QMT_SERVER_TOKEN",
        "QMT_SERVER_TLS_CERT",
        "BT_ENV_FILE",
        "BULLET_TRADE_ENV_FILE",
        "ENV_FILE",
        "BT_CLI_TEST_ENV_LOADED",
    ):
        child_env.pop(key, None)
    repo_root = Path(__file__).resolve().parents[2]
    python_path = [str(tmp_path), str(repo_root)]
    if child_env.get("PYTHONPATH"):
        python_path.append(child_env["PYTHONPATH"])
    child_env["PYTHONPATH"] = os.pathsep.join(python_path)
    child_env["BT_CLI_READY_MARKER"] = str(ready_marker)
    child_env["BT_CLI_CONNECT_MARKER"] = str(connect_marker)

    runner = "\n".join(
        [
            "import os",
            "from bullet_trade.cli.main import main",
            "if os.environ.get('DEFAULT_DATA_PROVIDER') != 'qmt-remote':",
            "    raise SystemExit(92)",
            "if os.environ.get('BT_CLI_TEST_ENV_LOADED') != 'from-cwd-dotenv':",
            "    raise SystemExit(93)",
            "raise SystemExit(main())",
        ]
    )
    result = subprocess.run(
        [sys.executable, "-c", runner, argument],
        cwd=tmp_path,
        env=child_env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    combined_output = result.stdout + result.stderr
    assert ready_marker.exists(), "sitecustomize 探针未加载，测试无法证明没有网络连接"
    assert not connect_marker.exists(), combined_output
    assert result.returncode == expected_returncode, combined_output
    assert expected_text in combined_output
