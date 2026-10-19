"""作者：BruceLee。

职责：验证服务端按账户声明券商配置与原CLI，不依赖客户端身份值。
输入：隔离环境变量/Namespace、父账户映射；输出：明确声明/格式及旧默认行为断言。
上下游：原build_server_config、AccountConfig和CLI；不启动SDK、网络或交易。
约定：别名不是资金账号，券商声明不是账户查询；新项不参与订单启停。
"""

from types import SimpleNamespace

import pytest

from bullet_trade.server.config import AccountConfig, build_server_config
from bullet_trade.cli.main import create_parser


@pytest.fixture
def server_environment(monkeypatch):
    """输入隔离环境补丁，返回本项明确值；构造配置不读取用户QMT账号或真实令牌。"""
    values = {"QMT_SERVER_ACCOUNTS": "main=001234:stock,other=009876:stock", "QMT_SERVER_TOKEN": "fixture-token"}
    monkeypatch.setattr("bullet_trade.server.config.get_env", lambda key, default=None: values.get(key, default))
    return values


@pytest.mark.unit
def test_account_declaration_is_optional_and_preserves_leading_zero():
    """无输入返回；原位置参数不变，券商明确规范化但不改资金账号或产生查询证明。"""
    assert AccountConfig("main", "001234").broker_id is None
    declared = AccountConfig("main", "001234", broker_id=" GuoJin ")
    assert declared.broker_id == "guojin" and declared.account_id == "001234"
    for invalid in ("", "broker name", "x/y", 123, True):
        with pytest.raises(ValueError):
            AccountConfig("main", "001234", broker_id=invalid)


@pytest.mark.unit
def test_environment_maps_each_parent_and_cli_has_precedence(server_environment):
    """输入环境金样，无返回；多账户不相互继承券商，CLI显式映射优先，旧账户参数不改。"""
    server_environment["QMT_SERVER_ACCOUNT_BROKERS"] = "main=GuoJin,other=huatai"
    config = build_server_config(SimpleNamespace())
    assert {row.key: row.broker_id for row in config.accounts} == {"main": "guojin", "other": "huatai"}
    overridden = build_server_config(SimpleNamespace(account_brokers="main=other_broker"))
    assert {row.key: row.broker_id for row in overridden.accounts} == {"main": "other_broker", "other": None}
    server_environment.pop("QMT_SERVER_ACCOUNT_BROKERS")
    assert all(row.broker_id is None for row in build_server_config(SimpleNamespace()).accounts)


@pytest.mark.unit
@pytest.mark.parametrize("raw", ["unknown=guojin", "main=guojin,main=huatai", "main", "main=", "main=wrong value", "main=../broker"])
def test_unknown_duplicate_or_invalid_declaration_is_not_silently_accepted(server_environment, raw):
    """输入错误声明，无返回；不忽略未知别名或暗取最后一项，也不补默认券商。"""
    with pytest.raises(ValueError):
        build_server_config(SimpleNamespace(account_brokers=raw))


@pytest.mark.unit
def test_default_account_from_existing_env_can_be_explicitly_declared(server_environment):
    """输入单账户旧环境，无返回；只用原default别名绑定声明，不需要改旧accounts格式。"""
    server_environment.pop("QMT_SERVER_ACCOUNTS")
    server_environment["QMT_ACCOUNT_ID"] = "001234"
    server_environment["QMT_SERVER_ACCOUNT_BROKERS"] = "default=guojin"
    account = build_server_config(SimpleNamespace()).accounts[0]
    assert account.key == "default" and account.account_id == "001234" and account.broker_id == "guojin"


@pytest.mark.unit
def test_original_cli_parses_declarations_and_does_not_add_a_trading_gate(server_environment):
    """输入原CLI夹具，无返回；声明走同配置工厂，未增加订单开关或强制字段。"""
    args = create_parser().parse_args(["server", "--account-brokers", "main=guojin,other=huatai"])
    config = build_server_config(args)
    assert config.accounts[0].broker_id == "guojin" and config.accounts[1].broker_id == "huatai"
    assert build_server_config(create_parser().parse_args(["server"])).accounts[0].broker_id is None
