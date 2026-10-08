from datetime import datetime,timezone
from types import SimpleNamespace
import pytest
from bullet_trade.integrations.ths.scheduler import YIELD
from bullet_trade.integrations.ths.windows.driver import NativeBackend,ObservedQuery,DriverBlocked

def backend(value):
    obj=NativeBackend.__new__(NativeBackend)
    obj.query_observed=lambda kind,pressure:value
    return obj

def test_formal_query_publishes_only_qualified_complete_table():
    observed=ObservedQuery('paper','orders',[],datetime.now(timezone.utc),True,(),
                           metadata={'schema':'bullettrade_broker_v1'})
    result=backend(observed).query('orders',lambda:False)
    assert result.complete and result.data==[]
    assert result.metadata==observed.metadata

def test_incomplete_and_normalization_failure_do_not_become_empty_snapshot():
    observed=ObservedQuery('paper','positions',None,datetime.now(timezone.utc),False,
                           ('normalization_unverified',))
    with pytest.raises(DriverBlocked,match='normalization_unverified'):
        backend(observed).query('positions',lambda:False)
    malformed=ObservedQuery('paper','positions',[{}],datetime.now(timezone.utc),True,())
    with pytest.raises(ValueError,match='required_broker_fields_missing'):
        backend(malformed).query('positions',lambda:False)

def test_pressure_yields_at_formal_collection_boundary():
    assert backend(YIELD).query('orders',lambda:False) is YIELD
    assert backend(None).query('orders',lambda:True) is YIELD


def test_copy_cleanup_cancels_only_detector_qualified_prompt(monkeypatch):
    from bullet_trade.integrations.ths.windows import driver
    monkeypatch.setattr(driver.time,'sleep',lambda _:None)
    obj=NativeBackend.__new__(NativeBackend)
    calls=[];pending=[71,None]
    obj.adapter=SimpleNamespace(locate_main=lambda:10,
        captcha_dialog=lambda:pending.pop(0),
        cancel_captcha=lambda h:calls.append(h) or True,
        dialog_fingerprint=lambda main:frozenset())
    assert obj._clear_copy_captcha() is True
    assert calls==[71]
    obj.adapter.captcha_dialog=lambda:None
    assert obj._clear_copy_captcha() is False
    assert calls==[71]


def test_unknown_dialog_and_failed_cleanup_are_not_hidden(monkeypatch):
    from bullet_trade.integrations.ths.windows import driver
    monkeypatch.setattr(driver.time,'sleep',lambda _:None)
    obj=NativeBackend.__new__(NativeBackend)
    obj.adapter=SimpleNamespace(locate_main=lambda:10,
        captcha_dialog=lambda:71,cancel_captcha=lambda _:False)
    with pytest.raises(DriverBlocked,match='copy_cleanup_unverified'):
        obj._clear_copy_captcha()
    def unknown():raise DriverBlocked('unknown_dialog')
    obj.adapter.captcha_dialog=unknown
    with pytest.raises(DriverBlocked,match='unknown_dialog'):
        obj._clear_copy_captcha()
