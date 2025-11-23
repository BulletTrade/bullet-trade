import argparse
import json
import os
import socket
import subprocess
import sys
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from bullet_trade.utils.env_loader import load_env


DEFAULT_CONFIG_DIR = ".bullet-trade"
DEFAULT_SETTINGS_FILE = "setting.json"
DEFAULT_JUPYTER_CONFIG_DIR = "jupyter-config"


@dataclass
class LabSettings:
    host: str
    port: int
    root_dir: Path
    env_path: Path
    open_browser: bool
    no_password: bool
    no_cert: bool
    token: Optional[str] = None
    allow_origin: Optional[str] = None


def _home_dir() -> Path:
    override = os.environ.get("BULLET_TRADE_HOME")
    if override:
        return Path(override).expanduser()
    return Path.home()


def _default_settings(base_home: Optional[Path] = None) -> LabSettings:
    base = base_home or _home_dir()
    root_dir = base / "bullet-trade"
    env_path = root_dir / ".env"
    return LabSettings(
        host="127.0.0.1",
        port=8088,
        root_dir=root_dir,
        env_path=env_path,
        open_browser=True,
        no_password=True,
        no_cert=True,
        token=None,
        allow_origin=None,
    )


def _settings_path(base_home: Optional[Path] = None) -> Path:
    base = base_home or _home_dir()
    return base / DEFAULT_CONFIG_DIR / DEFAULT_SETTINGS_FILE


def _config_dir(base_home: Optional[Path] = None) -> Path:
    base = base_home or _home_dir()
    return base / DEFAULT_CONFIG_DIR / DEFAULT_JUPYTER_CONFIG_DIR


def load_or_init_settings(base_home: Optional[Path] = None) -> Tuple[LabSettings, bool, Path]:
    settings_path = _settings_path(base_home)
    default_settings = _default_settings(base_home)
    first_run = False
    if settings_path.exists():
        try:
            data = json.loads(settings_path.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        settings = LabSettings(
            host=data.get("host", default_settings.host),
            port=int(data.get("port", default_settings.port)),
            root_dir=Path(data.get("root_dir", default_settings.root_dir)).expanduser(),
            env_path=Path(data.get("env_path", default_settings.env_path)).expanduser(),
            open_browser=bool(data.get("open_browser", default_settings.open_browser)),
            no_password=bool(data.get("no_password", default_settings.no_password)),
            no_cert=bool(data.get("no_cert", default_settings.no_cert)),
            token=data.get("token", default_settings.token),
            allow_origin=data.get("allow_origin", default_settings.allow_origin),
        )
    else:
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        settings = default_settings
        first_run = True
        _write_settings(settings_path, settings)
    return settings, first_run, settings_path


def _write_settings(settings_path: Path, settings: LabSettings) -> None:
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    settings_path.write_text(
        json.dumps(
            {
                "host": settings.host,
                "port": settings.port,
                "root_dir": str(settings.root_dir.expanduser()),
                "env_path": str(settings.env_path.expanduser()),
                "open_browser": settings.open_browser,
                "no_password": settings.no_password,
                "no_cert": settings.no_cert,
                "token": settings.token,
                "allow_origin": settings.allow_origin,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _get_template_dir() -> Optional[Path]:
    try:
        traversable = resources.files("bullet_trade.notebook")
        with resources.as_file(traversable) as path:
            return Path(path)
    except Exception:
        return None


def copy_notebooks_if_needed(root_dir: Path, first_run: bool) -> Dict[str, int]:
    result = {"copied": 0, "skipped": 0}
    template_dir = _get_template_dir()
    if not first_run or template_dir is None or not template_dir.exists():
        return result

    for item in template_dir.rglob("*"):
        if item.is_dir():
            continue
        relative = item.relative_to(template_dir)
        target = root_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            result["skipped"] += 1
            continue
        target.write_bytes(item.read_bytes())
        result["copied"] += 1
    return result


def ensure_env_file(env_path: Path) -> bool:
    env_path = env_path.expanduser()
    if env_path.exists():
        return False
    env_path.parent.mkdir(parents=True, exist_ok=True)
    env_path.write_text(
        "# BulletTrade JupyterLab 默认 .env\n"
        "# 在此配置数据源/券商等参数，如 DEFAULT_DATA_PROVIDER、DEFAULT_BROKER 等。\n"
        "# 数据源类型 (jqdata, tushare, qmt)\n"
        "DEFAULT_DATA_PROVIDER=qmt\n"
        "# 券商类型 (qmt, simulator)\n"
        "DEFAULT_BROKER=qmt\n"
        "# QMT 券商配置\n"
        "QMT_ACCOUNT_ID=your_account_id\n"
        "QMT_ACCOUNT_TYPE=stock\n"
        "# QMT 数据目录\n"
        "QMT_DATA_PATH=C:\\国金QMT交易端模拟\\userdata_mini\n"
        "# QMT 会话 ID\n"
        "QMT_SESSION_ID=0\n"
        ,
        encoding="utf-8",
    )
    return True


def ensure_snippets(first_run: bool, home_dir: Optional[Path] = None) -> Optional[Path]:
    if not first_run:
        return None
    base = home_dir or _home_dir()
    target = (
        base
        / ".jupyter"
        / "lab"
        / "user-settings"
        / "@jupyterlab"
        / "snippets-extension"
        / "snippets.json"
    )
    snippets = [
        {
            "category": "BulletTrade 回测示例",
            "name": "最小回测",
            "code": [
                "from jqdata import *",
                "",
                "def initialize(context):",
                "    set_benchmark('000300.XSHG')",
                "    run_daily(handle_bar)",
                "",
                "def handle_bar(context):",
                "    order('000001.XSHE', 100)",
            ],
        },
        {
            "category": "BulletTrade 实盘示例",
            "name": "QMT 下单",
            "code": [
                "from bullet_trade.compat.api import *",
                "",
                "def handle_data(context, data):",
                "    order_target_percent('600000.XSHG', 0.1)",
            ],
        },
    ]
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            try:
                current = json.loads(target.read_text(encoding="utf-8")).get("snippets", [])
            except Exception:
                current = []
        else:
            current = []
        names = {(item.get("category"), item.get("name")) for item in current}
        for snippet in snippets:
            if (snippet["category"], snippet["name"]) not in names:
                current.append(snippet)
        target.write_text(json.dumps({"snippets": current}, ensure_ascii=False, indent=2), encoding="utf-8")
        return target
    except Exception:
        return None


def _is_loopback(host: str) -> bool:
    return host in ("127.0.0.1", "localhost")


def _check_port_available(host: str, port: int) -> Tuple[bool, Optional[str]]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        result = sock.connect_ex((host, port))
        if result == 0:
            return False, f"端口 {host}:{port} 已被占用"
    return True, None


def diagnose(host: str, port: int, root_dir: Path) -> int:
    ok = True
    try:
        import jupyterlab  # noqa: F401
        print("✓ jupyterlab 已安装")
    except Exception:
        ok = False
        print("✗ 未检测到 jupyterlab，请执行: pip install bullet-trade")
    available, reason = _check_port_available(host, port)
    if available:
        print(f"✓ 端口可用: {host}:{port}")
    else:
        ok = False
        print(f"✗ 端口占用: {reason}")
    if root_dir.exists() and os.access(root_dir, os.W_OK):
        print(f"✓ Notebook 根目录可写: {root_dir}")
    else:
        ok = False
        print(f"✗ Notebook 根目录不可写或不存在: {root_dir}")
    return 0 if ok else 1


def build_jupyter_command(
    host: str,
    port: int,
    root_dir: Path,
    open_browser: bool,
    token: Optional[str],
    no_token: bool,
    password: Optional[str],
    certfile: Optional[str],
    keyfile: Optional[str],
    allow_origin: Optional[str],
) -> List[str]:
    args = [
        sys.executable,
        "-m",
        "jupyterlab",
        f"--ServerApp.ip={host}",
        f"--ServerApp.port={port}",
        f"--ServerApp.root_dir={root_dir}",
        f"--ServerApp.open_browser={'True' if open_browser else 'False'}",
    ]
    if no_token:
        args.append("--ServerApp.token=")
    elif token:
        args.append(f"--ServerApp.token={token}")
    if password is not None:
        args.append(f"--ServerApp.password={password}")
    if certfile:
        args.append(f"--ServerApp.certfile={certfile}")
    if keyfile:
        args.append(f"--ServerApp.keyfile={keyfile}")
    if allow_origin:
        args.append(f"--ServerApp.allow_origin={allow_origin}")
    return args


def run_lab(args: argparse.Namespace) -> int:
    settings, first_run, settings_path = load_or_init_settings()
    config_dir = _config_dir()

    host = args.ip or settings.host
    port = args.port or settings.port
    root_dir = Path(args.notebook_dir or settings.root_dir).expanduser()
    env_path = Path(args.env_file or settings.env_path or (root_dir / ".env")).expanduser()
    open_browser = settings.open_browser
    if getattr(args, "no_browser", False):
        open_browser = False
    if getattr(args, "browser", False):
        open_browser = True

    token = args.token or settings.token
    no_token = getattr(args, "no_token", False)
    password = args.password
    certfile = args.certfile
    keyfile = args.keyfile
    allow_origin = args.allow_origin or settings.allow_origin
    no_password = settings.no_password if password is None else False
    no_cert = settings.no_cert if not certfile else False

    # 安全检查
    if no_token and (password is None or password == ""):
        print("✗ 已关闭 token，但未设置密码，出于安全考虑拒绝启动。")
        return 1
    if not _is_loopback(host) and no_password and no_cert:
        print("✗ 检测到监听非 127.0.0.1 且未启用密码/证书，为避免裸露风险已拒绝启动。")
        print("  请在 ~/.bullet-trade/setting.json 启用密码/证书或改为 127.0.0.1。")
        return 1

    root_dir.mkdir(parents=True, exist_ok=True)
    env_created = ensure_env_file(env_path)
    copies = copy_notebooks_if_needed(root_dir, first_run)
    snippet_path = ensure_snippets(first_run)

    # persist settings if defaults differ
    if first_run or settings.root_dir != root_dir or settings.env_path != env_path:
        settings.root_dir = root_dir
        settings.env_path = env_path
        settings.no_password = no_password
        settings.no_cert = no_cert
        _write_settings(settings_path, settings)

    if getattr(args, "diagnose", False):
        return diagnose(host, port, root_dir)

    # load env from root only
    load_env(env_file=str(env_path), verbose=True, override=True)

    print(f"设置文件: {settings_path}")
    print(f"Notebook 根目录: {root_dir}")
    print(f".env 文件: {env_path}")
    if env_created:
        print("✓ 已创建默认 .env，可按需填入数据源/券商参数。")
    if first_run:
        print("✓ 首次运行已初始化设置文件与目录。")
    if copies.get("copied"):
        print(f"✓ 已复制示例文件 {copies['copied']} 个（跳过 {copies['skipped']} 个同名文件）。")
    elif copies.get("skipped"):
        print(f"提示：示例文件已存在，跳过 {copies['skipped']} 个。")
    if snippet_path:
        print(f"✓ 已写入代码片段设置，可在 Command Palette 搜索 BulletTrade，路径：{snippet_path}")

    available, reason = _check_port_available(host, port)
    if not available:
        print(f"✗ 端口占用: {reason}")
        return 1

    try:
        import jupyterlab  # noqa: F401
    except Exception:
        print("✗ 未检测到 jupyterlab，请执行: pip install bullet-trade")
        return 1

    if not os.access(root_dir, os.W_OK):
        print(f"✗ Notebook 根目录不可写: {root_dir}")
        return 1

    # 采用隔离的 Jupyter 配置目录，避免继承用户旧密码/设置
    config_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.setdefault("JUPYTER_CONFIG_DIR", str(config_dir))
    print(f"Jupyter 配置目录: {env['JUPYTER_CONFIG_DIR']}")

    cmd = build_jupyter_command(
        host=host,
        port=port,
        root_dir=root_dir,
        open_browser=open_browser,
        token=token,
        no_token=no_token,
        password=password,
        certfile=certfile,
        keyfile=keyfile,
        allow_origin=allow_origin,
    )
    print(f"即将启动 JupyterLab: {' '.join(cmd)}")
    print("提示：如需修改默认行为，请编辑 ~/.bullet-trade/setting.json。")
    result = subprocess.run(cmd, cwd=str(root_dir), env=env)
    return result.returncode
