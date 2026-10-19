"""作者：BruceLee。

职责：验收仿真执行检查点的原单恢复、幂等、故障与身份隔离，不代替账户结算验收。
输入：固定时钟、虚构政策/行情、临时 JSON 文件和 I/O 故障注入。
输出：恢复前后逐笔事实、状态、日志和文件不变断言；不联网、不构造真实券商。
上下游：SimulationMatcher 和 SimulationCheckpointFile；政策数值仅用于本地金样。
"""

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from bullet_trade.broker.simulation_checkpoint import SimulationCheckpointFile
from bullet_trade.broker.simulation_matcher import (
    SimulationMatcher,
    SimulationPolicy,
    SimulationQuote,
    _digest,
)


class Clock:
    """可显式推进的金样时钟；不使用机器时间作为市场时间。"""

    def __init__(self):
        """无输入返回；初始化一个带时区的虚构交易时刻。"""
        self.at = datetime(2026, 10, 9, 9, 30, tzinfo=timezone(timedelta(hours=8)))

    def __call__(self):
        """无输入，返回当前夹具时间，不自动推进。"""
        return self.at


def engine(clock):
    """输入时钟，返回模拟账户执行器；显式政策不用于正式默认配置。"""
    return SimulationMatcher(
        account_id="fixture-account",
        policy=SimulationPolicy("fixture-v1", timedelta(seconds=10), 300),
        clock=clock,
        is_trading_time=lambda security, at: True,
    )


def submit(matcher, clock, order_id="o1", quantity=500):
    """输入执行器、时钟与身份/量，返回受理事实；此处不伪造资金预约或账户结算。"""
    return matcher.submit(
        order_id=order_id,
        security="510300.XSHG",
        side="buy",
        quantity=quantity,
        protection_price=Decimal("4.000"),
        order_type="limit",
        expires_at=clock.at + timedelta(minutes=1),
        quantity_step=100,
        price_tick=Decimal("0.001"),
        rule_version="fixture-rule",
        fee_version="fixture-fee",
    )


def quote(clock, quote_id="q1", **overrides):
    """输入时钟、报价身份和覆盖字段，返回带来源的模型行情；容量不是真实盘口深度。"""
    fields = dict(
        quote_id=quote_id,
        security="510300.XSHG",
        price=Decimal("3.990"),
        event_at=clock.at,
        received_at=clock.at,
        quantity=500,
        source="fixture-only",
        kind="snapshot",
        data_version="fixture-data",
        paused=False,
    )
    fields.update(overrides)
    return SimulationQuote(**fields)


def restore(checkpoint, matcher, fixture_clock, **overrides):
    """输入快照、原执行器和时钟，返回恢复实例；可覆盖依赖验证拒绝路径。"""
    inputs = dict(
        account_id=matcher.account_id,
        policy=matcher.policy,
        clock=fixture_clock,
        is_trading_time=lambda security, at: True,
    )
    inputs.update(overrides)
    return SimulationMatcher.restore(checkpoint, **inputs)


def forbidden(*args):
    """输入任意参数立即报错；用于证明恢复不调用行情/新日历/运行时钟依赖。"""
    raise AssertionError("恢复不应调用外部依赖")


def rehash(document):
    """输入测试修改后的快照，无返回；重算外层校验以验收内部合同而非仅 hash。"""
    document["sha256"] = _digest(document["payload"])


@pytest.mark.unit
class TestSimulationCheckpoint:
    """覆盖 JSON 原单恢复与文件故障；不宣称已接通策略或跨账本事务。"""

    def test_candidate_guard_preserves_capacity_and_replays_original_decisions(self):
        """无输入返回；拒绝首单不耗事件容量，日志保存布尔判断，恢复不调用新账本或费用。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock, "o1", 300)
        submit(matcher, clock, "o2", 300)
        observed = []

        def guard(order, quantity, price):
            """输入候选，返回仅第二单可成交；记录候选，不改变账户或撮合器。"""
            observed.append((order.order_id, quantity, price))
            return order.order_id == "o2"

        event = quote(clock)
        assert [item.order_id for item in matcher.process_quote(event, can_fill=guard)] == ["o2"]
        assert [item[0] for item in observed] == ["o1", "o2"]
        checkpoint = matcher.checkpoint()
        assert checkpoint["payload"]["commands"][-1]["arguments"]["fill_checks"] == {
            "o1": False,
            "o2": True,
        }
        recovered = restore(checkpoint, matcher, clock, is_trading_time=forbidden)
        assert recovered.checkpoint() == checkpoint
        assert recovered.candidate_checks(event.quote_id) == {"o1": False, "o2": True}
        recovered.candidate_checks(event.quote_id)["o1"] = True
        assert recovered.candidate_checks(event.quote_id)["o1"] is False
        assert recovered.candidate_checks("absent") is None
        assert recovered.process_quote(event, can_fill=forbidden) == ()
        assert observed == [("o1", 300, Decimal("3.990")), ("o2", 300, Decimal("3.990"))]
        for bad in (
            None,
            {"o1": False},
            {"o1": False, "o2": 1},
            {"o1": False, "o2": True, "stranger": True},
        ):
            damaged = matcher.checkpoint()
            damaged["payload"]["commands"][-1]["arguments"]["fill_checks"] = bad
            rehash(damaged)
            with pytest.raises(ValueError):
                restore(damaged, matcher, clock)

    def test_candidate_guard_error_does_not_publish_quote_or_partial_facts(self):
        """无输入返回；回调异常/非布尔拒绝整次候选，事实及日志不留下半成交，随后原事件可正常处理。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock, "o1", 300)
        submit(matcher, clock, "o2", 300)
        before = matcher.checkpoint()
        event = quote(clock, quantity=600)
        with pytest.raises(AssertionError):
            matcher.process_quote(event, can_fill=forbidden)
        assert matcher.checkpoint() == before
        with pytest.raises(ValueError):
            matcher.process_quote(event, can_fill=lambda *_: 1)
        assert matcher.checkpoint() == before
        with pytest.raises(ValueError):
            matcher.process_quote(event, can_fill=True)
        assert matcher.checkpoint() == before
        assert [item.order_id for item in matcher.process_quote(event)] == ["o1"]
        assert "fill_checks" not in matcher.checkpoint()["payload"]["commands"][-1]["arguments"]
        assert matcher.candidate_checks(event.quote_id) is None

    @pytest.mark.parametrize(
        "state", ["empty", "accepted", "partial", "filled", "cancelled", "expired"]
    )
    def test_roundtrip_preserves_all_states_and_fact_ids(self, state):
        """输入目标状态，无返回；序列化恢复后保留所有身份、时区、Decimal 与事件序号。"""
        clock = Clock()
        matcher = engine(clock)
        if state != "empty":
            submit(matcher, clock)
        if state in {"partial", "filled", "cancelled", "expired"}:
            matcher.process_quote(quote(clock))
        if state == "filled":
            matcher.process_quote(quote(clock, "q2"))
        elif state == "cancelled":
            matcher.cancel("o1")
        elif state == "expired":
            clock.at += timedelta(minutes=1)
            matcher.advance()
        checkpoint = json.loads(json.dumps(matcher.checkpoint()))
        recovered = restore(checkpoint, matcher, clock, clock=forbidden, is_trading_time=forbidden)
        assert recovered.checkpoint() == checkpoint
        assert recovered.orders() == matcher.orders()
        assert recovered.executions() == matcher.executions()
        assert recovered.events() == matcher.events()

    def test_resume_keeps_duplicate_quote_and_original_order_idempotent(self):
        """无输入返回；恢复不会补发旧成交，旧报价不再撮合，新报价只完成原单余量。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        original = matcher.process_quote(quote(clock))
        recovered = restore(matcher.checkpoint(), matcher, clock)
        assert recovered.process_quote(quote(clock)) == ()
        assert submit(recovered, clock).filled_quantity == 300
        remaining = recovered.process_quote(quote(clock, "q2"))
        assert remaining[0].quantity == 200
        assert recovered.executions()[0].execution_id == original[0].execution_id
        assert len(recovered.executions()) == 2
        assert not recovered.cancel("o1")

    def test_recovery_does_not_expire_or_fetch_new_data_until_explicit_advance(self):
        """无输入返回；重启晚于有效期仍先恢复原事实，显式推进才生成一次到期事实。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        checkpoint = matcher.checkpoint()
        clock.at += timedelta(days=1)
        recovered = restore(checkpoint, matcher, clock)
        assert recovered.orders()[0].status == "accepted"
        assert recovered.checkpoint() == checkpoint
        assert len(recovered.advance()) == 1
        assert recovered.orders()[0].status == "expired"
        assert recovered.advance() == ()
        again = restore(recovered.checkpoint(), matcher, clock)
        assert again.events() == recovered.events()

    @pytest.mark.parametrize("condition", ["closed", "paused", "stale", "zero", "out_of_order"])
    def test_ignored_market_events_are_restored_without_new_calendar(self, condition):
        """输入未成交行情条件，无返回；保留原规则判断和去重水位，新日历不能改写旧成交。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        if condition == "closed":
            matcher._is_trading_time = lambda security, at: False
        values = {
            "paused": dict(paused=True),
            "stale": dict(event_at=clock.at - timedelta(seconds=20)),
            "zero": dict(quantity=0),
            "out_of_order": dict(event_at=clock.at - timedelta(seconds=1)),
        }.get(condition, {})
        if condition == "out_of_order":
            matcher.process_quote(quote(clock, "newer", price=Decimal("4.100")))
        assert matcher.process_quote(quote(clock, **values)) == ()
        recovered = restore(matcher.checkpoint(), matcher, clock, is_trading_time=forbidden)
        assert recovered.checkpoint() == matcher.checkpoint()
        assert recovered.executions() == ()

    @pytest.mark.parametrize("change", ["account", "version", "age", "capacity"])
    def test_wrong_identity_or_policy_is_rejected(self, change):
        """输入账户/政策差异，无返回；恢复不能换账户或悄悄使用新政策，旧实例不变。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        before = matcher.checkpoint()
        overrides = {
            "account": dict(account_id="other-account"),
            "version": dict(policy=replace(matcher.policy, version="other-v1")),
            "age": dict(policy=replace(matcher.policy, max_quote_age=timedelta(seconds=11))),
            "capacity": dict(policy=replace(matcher.policy, max_fill_quantity=400)),
        }[change]
        with pytest.raises(ValueError):
            restore(before, matcher, clock, **overrides)
        assert matcher.checkpoint() == before

    @pytest.mark.parametrize(
        "change",
        [
            "checksum",
            "environment",
            "schema",
            "facts",
            "command",
            "quantity",
            "time",
            "missing_time",
        ],
    )
    def test_corruption_does_not_become_an_empty_account(self, change):
        """输入损坏字段，无返回；即使重算外层 hash，内部身份、经济事实和时间水位仍须一致。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        matcher.process_quote(quote(clock))
        broken = matcher.checkpoint()
        payload = broken["payload"]
        if change == "checksum":
            broken["sha256"] = "0" * 64
        elif change == "environment":
            payload["environment"] = "live"
        elif change == "schema":
            payload["schema"] = True
        elif change == "facts":
            payload["facts_digest"] = "0" * 64
        elif change == "command":
            payload["commands"][0]["operation"] = "execute_python"
        elif change == "quantity":
            payload["commands"][0]["arguments"]["quantity"] = 600
        elif change == "time":
            payload["last_time"] = (clock.at - timedelta(seconds=1)).isoformat()
        elif change == "missing_time":
            payload["last_time"] = None
        if change != "checksum":
            rehash(broken)
        with pytest.raises(ValueError):
            restore(broken, matcher, clock)
        assert matcher.orders()[0].filled_quantity == 300

    def test_snapshot_is_independent_and_preserves_rejected_operation_clock(self):
        """无输入返回；外部修改快照不改内部，重复/失败命令推进的时间水位在恢复后仍受约束。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        checkpoint = matcher.checkpoint()
        checkpoint["payload"]["commands"].clear()
        assert len(matcher.checkpoint()["payload"]["commands"]) == 1
        clock.at += timedelta(seconds=1)
        with pytest.raises(ValueError):
            submit(matcher, clock, quantity=600)
        recovered = restore(matcher.checkpoint(), matcher, clock)
        clock.at -= timedelta(seconds=1)
        with pytest.raises(ValueError, match="逆行"):
            recovered.advance()

    def test_calendar_failure_does_not_commit_partial_quote_or_expiry(self):
        """无输入返回；外部时段判断报错不记录半个报价/到期，修复后原报价可完整处理一次。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        original = matcher.checkpoint()
        matcher._is_trading_time = forbidden
        with pytest.raises(AssertionError):
            matcher.process_quote(quote(clock))
        assert matcher.checkpoint() == original
        matcher._is_trading_time = lambda security, at: True
        assert len(matcher.process_quote(quote(clock))) == 1
        assert restore(matcher.checkpoint(), matcher, clock).executions() == matcher.executions()

    def test_file_roundtrip_and_missing_file(self, tmp_path):
        """输入临时目录，无返回；缺文件与损坏区分，保存和重载后继续原单而非另起账本。"""
        clock = Clock()
        matcher = engine(clock)
        store = SimulationCheckpointFile(tmp_path / "state.json")
        arguments = dict(
            account_id=matcher.account_id,
            policy=matcher.policy,
            clock=clock,
            is_trading_time=forbidden,
        )
        assert store.load(**arguments) is None
        submit(matcher, clock)
        matcher.process_quote(quote(clock))
        store.save(matcher)
        recovered = store.load(**arguments)
        assert recovered.executions() == matcher.executions()
        recovered._is_trading_time = lambda security, at: True
        recovered.process_quote(quote(clock, "q2"))
        store.save(recovered)
        assert store.load(**arguments).orders()[0].status == "filled"
        assert not list(tmp_path.glob(".simulation-*.tmp"))

    def test_stale_or_forked_writer_cannot_overwrite_checkpoint(self, tmp_path):
        """输入临时目录，无返回；较旧副本和同账户分叉日志都被拒绝，已有文件保持不变。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        old = restore(matcher.checkpoint(), matcher, clock)
        store = SimulationCheckpointFile(tmp_path / "state.json")
        matcher.process_quote(quote(clock))
        store.save(matcher)
        before = store.path.read_bytes()
        with pytest.raises(ValueError, match="旧状态"):
            store.save(old)
        old.cancel("o1")
        with pytest.raises(ValueError, match="分叉"):
            store.save(old)
        assert store.path.read_bytes() == before

    def test_atomic_replace_failure_keeps_previous_checkpoint(self, tmp_path, monkeypatch):
        """输入临时目录和故障注入，无返回；替换前失败不留下半文件或覆盖上一已确认状态。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        store = SimulationCheckpointFile(tmp_path / "state.json")
        store.save(matcher)
        before = store.path.read_bytes()
        matcher.process_quote(quote(clock))

        def fail_replace(*args):
            """输入替换参数立即抛 I/O 错误，模拟磁盘提交前失败，不改变旧文件。"""
            raise OSError("fixture replace failure")

        monkeypatch.setattr("bullet_trade.broker.simulation_checkpoint.os.replace", fail_replace)
        with pytest.raises(OSError, match="replace failure"):
            store.save(matcher)
        assert store.path.read_bytes() == before
        assert not list(tmp_path.glob(".simulation-*.tmp"))

    @pytest.mark.parametrize(
        "content", ["", "{", '{"payload": {}, "payload": {}, "sha256": "x"}', "[]"]
    )
    def test_damaged_file_is_not_overwritten_or_treated_as_missing(self, tmp_path, content):
        """输入损坏 JSON，无返回；读写都显式失败并原样保留文件，不默认为首次启动。"""
        clock = Clock()
        matcher = engine(clock)
        store = SimulationCheckpointFile(tmp_path / "state.json")
        store.path.write_text(content)
        with pytest.raises(ValueError):
            store.load(
                account_id=matcher.account_id,
                policy=matcher.policy,
                clock=clock,
                is_trading_time=forbidden,
            )
        with pytest.raises(ValueError):
            store.save(matcher)
        assert store.path.read_text() == content

    @pytest.mark.parametrize("target", ["file", "lock", "parent"])
    def test_symlink_paths_do_not_write_other_state(self, tmp_path, target):
        """输入符号链接位置，无返回；链接到其他文件或目录时拒绝且原内容不变。"""
        clock = Clock()
        matcher = engine(clock)
        real = tmp_path / "real"
        real.mkdir()
        other = real / "other.json"
        other.write_text("private-fixture")
        path = tmp_path / "state.json"
        if target == "file":
            path.symlink_to(other)
        elif target == "lock":
            path.with_name("state.json.lock").symlink_to(other)
        else:
            link = tmp_path / "linked"
            link.symlink_to(real, target_is_directory=True)
            path = link / "state.json"
        with pytest.raises(ValueError, match="符号链接"):
            SimulationCheckpointFile(path).save(matcher)
        assert other.read_text() == "private-fixture"

    @pytest.mark.parametrize("location", ["missing_parent", "directory"])
    def test_invalid_storage_location_is_rejected(self, tmp_path, location):
        """输入无效存储位置，无返回；不自动创建运行目录或把目录覆盖成文件。"""
        path = tmp_path / "missing" / "state.json" if location == "missing_parent" else tmp_path
        with pytest.raises(ValueError):
            SimulationCheckpointFile(path).save(engine(Clock()))

    @pytest.mark.parametrize(
        "change",
        [
            "document",
            "commands",
            "command_shape",
            "quote_shape",
            "cancel_shape",
            "decimal",
            "trading",
            "naive_time",
        ],
    )
    def test_invalid_recovery_structure_is_rejected(self, change):
        """输入格式/类型损坏，无返回；恢复失败保持原事实，不执行未知类型或非法价格。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        matcher.process_quote(quote(clock))
        matcher.cancel("o1")
        broken = matcher.checkpoint()
        payload = broken["payload"]
        if change == "document":
            broken["extra"] = 1
        elif change == "commands":
            payload["commands"] = "not-a-list"
        elif change == "command_shape":
            payload["commands"][0]["extra"] = 1
        elif change == "quote_shape":
            payload["commands"][1]["arguments"]["extra"] = 1
        elif change == "cancel_shape":
            payload["commands"][2]["arguments"]["extra"] = 1
        elif change == "decimal":
            payload["commands"][0]["arguments"]["protection_price"] = "invalid"
        elif change == "trading":
            payload["commands"][1]["arguments"]["trading"] = None
        else:
            payload["commands"][0]["at"] = clock.at.replace(tzinfo=None).isoformat()
        rehash(broken)
        with pytest.raises(ValueError):
            restore(broken, matcher, clock)
        assert len(matcher.executions()) == 1

    def test_invalid_calendar_type_does_not_commit_quote(self):
        """无输入返回；未知时段值不能当作可交易，失败后报价和事件仍未写入。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        matcher._is_trading_time = lambda security, at: "yes"
        before = matcher.checkpoint()
        with pytest.raises(ValueError, match="布尔"):
            matcher.process_quote(quote(clock))
        assert matcher.checkpoint() == before

    def test_file_checksum_failure_and_wrong_account_keep_original(self, tmp_path):
        """输入临时目录，无返回；文件校验错误或他账户保存均失败，不覆盖原文件。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        store = SimulationCheckpointFile(tmp_path / "state.json")
        store.save(matcher)
        before = store.path.read_bytes()
        other = SimulationMatcher(
            account_id="other",
            policy=matcher.policy,
            clock=clock,
            is_trading_time=lambda security, at: True,
        )
        with pytest.raises(ValueError, match="账户"):
            store.save(other)
        assert store.path.read_bytes() == before
        broken = json.loads(before)
        broken["sha256"] = "0" * 64
        store.path.write_text(json.dumps(broken))
        corrupt = store.path.read_bytes()
        with pytest.raises(ValueError, match="校验"):
            store.save(matcher)
        assert store.path.read_bytes() == corrupt

    def test_directory_fsync_failure_is_unknown_not_a_false_rollback(self, tmp_path, monkeypatch):
        """输入提交后故障，无返回；抛错但重新加载可核对已落文件，不声称回退或自动重发。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        store = SimulationCheckpointFile(tmp_path / "state.json")
        store.save(matcher)
        matcher.process_quote(quote(clock))
        calls = []
        import os

        original = os.fsync

        def fail_directory(descriptor):
            """输入文件描述符，第二次 fsync 模拟目录持久化失败；第一次仍真实同步文件。"""
            calls.append(descriptor)
            if len(calls) == 2:
                raise OSError("fixture directory fsync failure")
            return original(descriptor)

        monkeypatch.setattr("bullet_trade.broker.simulation_checkpoint.os.fsync", fail_directory)
        with pytest.raises(OSError, match="directory fsync"):
            store.save(matcher)
        recovered = store.load(
            account_id=matcher.account_id,
            policy=matcher.policy,
            clock=clock,
            is_trading_time=forbidden,
        )
        assert recovered.executions() == matcher.executions()
        assert recovered.process_quote(quote(clock)) == ()

    def test_concurrent_same_snapshot_saves_remain_identical(self, tmp_path):
        """输入临时目录，无返回；同一快照并发保存串行落盘，不改变事实或产生半个文件。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        matcher.process_quote(quote(clock))
        store = SimulationCheckpointFile(tmp_path / "state.json")
        with ThreadPoolExecutor(max_workers=6) as pool:
            assert list(pool.map(store.save, [matcher] * 12)) == [None] * 12
        recovered = store.load(
            account_id=matcher.account_id,
            policy=matcher.policy,
            clock=clock,
            is_trading_time=forbidden,
        )
        assert recovered.checkpoint() == matcher.checkpoint()

    def test_concurrent_forked_writers_have_only_one_winner(self, tmp_path):
        """输入临时目录，无返回；分叉副本不能互相覆盖，最终事实只来自一条完整日志。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        first, second = [restore(matcher.checkpoint(), matcher, clock) for _ in range(2)]
        first.process_quote(quote(clock, "branch-a", quantity=100))
        second.process_quote(quote(clock, "branch-b", quantity=200))
        store = SimulationCheckpointFile(tmp_path / "state.json")
        store.save(matcher)

        def save_branch(branch):
            """输入日志分支，返回是否成功；预期冲突显式报告，不重试覆盖另一分支。"""
            try:
                store.save(branch)
                return True
            except ValueError:
                return False

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(save_branch, [first, second]))
        assert sorted(results) == [False, True]
        recovered = store.load(
            account_id=matcher.account_id,
            policy=matcher.policy,
            clock=clock,
            is_trading_time=forbidden,
        )
        assert recovered.checkpoint() in [first.checkpoint(), second.checkpoint()]
        assert len(recovered.executions()) == 1

    def test_file_fsync_failure_keeps_previous_state(self, tmp_path, monkeypatch):
        """输入落盘前故障，无返回；临时文件 fsync 失败保留上版并清理未提交文件。"""
        clock = Clock()
        matcher = engine(clock)
        submit(matcher, clock)
        store = SimulationCheckpointFile(tmp_path / "state.json")
        store.save(matcher)
        before = store.path.read_bytes()
        matcher.process_quote(quote(clock))

        def fail_fsync(descriptor):
            """输入文件描述符，抛模拟磁盘同步错误；不执行替换。"""
            raise OSError("fixture file fsync failure")

        monkeypatch.setattr("bullet_trade.broker.simulation_checkpoint.os.fsync", fail_fsync)
        with pytest.raises(OSError, match="file fsync"):
            store.save(matcher)
        assert store.path.read_bytes() == before
        assert not list(tmp_path.glob(".simulation-*.tmp"))
