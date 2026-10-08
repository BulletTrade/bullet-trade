"""GM 数据运行链独立性：外部基准不进入 Provider，不加载聚宽 RPC 或 SDK。"""

import ast
import builtins
from pathlib import Path

import pytest

from bullet_trade.data.api import _create_provider
from bullet_trade.data.providers import gm
from bullet_trade.utils.env_loader import get_data_provider_config
from tests.unit.test_gm_data_provider import factored


@pytest.mark.parametrize(
    "configuration",
    [
        {"alignment_mode": "joinquant_rpc"},
        {"reference_client": object()},
        {"rpc_env_file": "/not/an/input"},
        {"rpc_host": "not-an-input"},
        {"rpc_authkey": "not-an-input"},
    ],
)
def test_mixed_source_configuration_is_rejected(configuration):
    with pytest.raises(ValueError, match="独立使用 GM"):
        _create_provider("gm", configuration)


def test_complete_price_read_needs_no_joinquant_configuration_or_import(monkeypatch):
    for key in ["JQ_PROXY_HOST", "JQ_PROXY_PORT", "JQ_PROXY_AUTHKEY", "GM_JQ_RPC_ENV_FILE"]:
        monkeypatch.delenv(key, raising=False)
    original_import = builtins.__import__

    def import_without_reference(name, *args, **kwargs):
        if any(value in name for value in ("jqdatasdk", "rpc_proxy", "gm.reference")):
            pytest.fail("GM 运行链加载了外部数据源")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_reference)
    provider = _create_provider("gm", {"client": factored()._client})
    result = provider.get_price(
        "601318.XSHG",
        "2024-07-25",
        "2024-07-25",
        skip_paused=True,
        pre_factor_ref_date="2024-07-30",
        fields=["close", "volume", "money", "factor"],
    )
    assert result.iloc[0].tolist() == [5.0, 200.0, 1000.0, 0.5]
    assert set(result.attrs["field_sources"].values()) == {"gm"}


def test_reference_environment_is_not_loaded_into_gm_config(monkeypatch):
    monkeypatch.setenv("GM_JQ_RPC_ENV_FILE", "/not/an/input")
    monkeypatch.setenv("JQ_PROXY_HOST", "not-an-input")
    config = get_data_provider_config()["gm"]
    assert not any("rpc" in k or "reference" in k for k in config)


def test_production_gm_package_contains_no_reference_runtime_imports():
    paths = [Path(gm.__file__)]
    package = paths[0].parents[2] / "integrations/gm"
    assert not (package / "reference.py").exists()
    assert not (package / "reference_worker.py").exists()
    paths += [package / "data_client.py", package / "data_worker.py"]
    for path in paths:
        tree = ast.parse(path.read_text())
        imports = [
            alias.name if isinstance(node, ast.Import) else node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        ]
        assert not any("reference" in name or "jqdatasdk" in name for name in imports)
