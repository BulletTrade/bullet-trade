"""Synthetic checks of the no-success-dialog entry point; no Windows I/O."""
import json
from datetime import datetime,timezone
from types import SimpleNamespace
import sys,time
import pytest
from zoneinfo import ZoneInfo
from bullet_trade.integrations.ths.request_store import Request
from bullet_trade.integrations.ths.windows.driver import ObservedQuery
from bullet_trade.integrations.ths.windows.native_actions import NativeActions,NativeActionBlocked


def scenario(tmp_path,monkeypatch,*,exclusive=True,new_count=1,residual=False,
             error='broker_dialog_timeout_unknown',confirm_click_raises=False,
             observed_delay_seconds=0):
    now=time.time();day=datetime.fromtimestamp(now,ZoneInfo('Asia/Shanghai')).date().isoformat()
    request=Request('synthetic-request','paper',day,'once','limit_buy',
        {'security':'518880.XSHG','price':'8.500','quantity':100},{},'submit_unknown',
        now+120,None,None,now,now)
    clicks=[];queries=[];closed=[False];dialog_open=[False]
    details='资金帐号：模拟炒股-TEST**01\n证券代码：518880\n买入价格：8.500\n买入数量：100\n您是否确定以上买入委托？'
    dialog=(80,[{'id':1365,'class':'Static','text':'委托确认','visible':True},
        {'id':1040,'class':'Static','text':details,'visible':True},
        {'id':6,'class':'Button','text':'是(&Y)','hwnd':60,'visible':True,'enabled':True}])
    class Widget:
        def __init__(self,handle):self.handle=handle
        def wrapper_object(self):return self
        def click_input(self):
            clicks.append(self.handle)
            if self.handle==42:dialog_open[0]=True
            if self.handle==60:
                closed[0]=True
                dialog_open[0]=False
                if confirm_click_raises:raise OSError('confirmation click transport failed')
    host=SimpleNamespace(profile=SimpleNamespace(account='paper',account_mask='模拟炒股-TEST**01',exclusive_order_source=exclusive),state_dir=tmp_path)
    host.adapter=SimpleNamespace(app=SimpleNamespace(window=lambda handle:Widget(handle)))
    def query(kind,pressure):
        queries.append(kind)
        row={'合同编号':'000123','证券代码':'518880','交易市场':'上海Ａ股','操作':'买入',
             '委托价格':'8.500','委托数量':'100','成交数量':'0','撤消数量':'0',
             '备注':'未成交','委托时间':datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%H:%M:%S')}
        rows=tuple({**row,'合同编号':'00012'+str(i+3)} for i in range(new_count))
        return ObservedQuery('paper',kind,[],
                             datetime.fromtimestamp(time.time()+observed_delay_seconds,
                                                    timezone.utc),True,(),rows)
    host.query_observed=query
    class CheckedPointer:
        def button(self,*args,**kwargs):
            assert kwargs['current']() is True
    native=NativeActions(host,pointer_gate=CheckedPointer())
    native._order_baseline=()
    native._prepared={'main':10,'pid':7,'current':lambda:True,'side':'buy',
        'values':('518880','8.500','100'),
        'model':SimpleNamespace(controls=(SimpleNamespace(control_id=1006,class_name='Button',caption='买入',hwnd=42,parent_hwnd=12),))}
    monkeypatch.setattr(native,'_dialogs',lambda *args:[dialog]
                        if dialog_open[0] or residual and closed[0] else [])
    def wait(*args,previous=None):
        if previous is None:return dialog
        raise NativeActionBlocked(error)
    monkeypatch.setattr(native,'_wait_one_dialog',wait)
    monkeypatch.setitem(sys.modules,'win32gui',SimpleNamespace(GetParent=lambda _:80))
    return native,request,clicks,queries


def evidence_by_phase(tmp_path, phase):
    records = [json.loads(path.read_text(encoding='utf-8'))
               for path in (tmp_path/'driver-evidence').glob('*.json')]
    matched = [item['content'] for item in records if item['phase']==phase]
    assert len(matched)==1
    return matched[0]


def test_no_success_dialog_uses_one_confirmed_delta_without_resubmission(tmp_path,monkeypatch):
    native,request,clicks,queries=scenario(tmp_path,monkeypatch)
    result=native._submit_order(request)
    assert result.broker_contract_no=='000123'
    assert result.evidence_ref.startswith('native-order-diff:')
    assert clicks==[42,60] and queries==['orders']
    assert list((tmp_path/'driver-evidence').glob('*.json'))
    first=evidence_by_phase(tmp_path,'first_submit_click_timing')
    confirm=evidence_by_phase(tmp_path,'confirmation_click_timing')
    delta=evidence_by_phase(tmp_path,'confirmed_order_delta')
    assert first['before_at']<=first['after_at']<=confirm['before_at']<=confirm['after_at']
    assert first['click_returned'] is confirm['click_returned'] is True
    assert delta['submitted_at']==confirm['before_at']
    assert delta['confirmation_click_after_at']==confirm['after_at']
    assert delta['confirmation_click_timing_ref'] and delta['confirmation_ref']
    assert delta['before']==[] and len(delta['after'])==1
    assert delta['before_complete'] is delta['after_complete'] is True


@pytest.mark.parametrize('options',[{'exclusive':False},{'residual':True},{'error':'multiple_broker_dialogs'}])
def test_gate_or_modal_failure_does_not_lookup_or_repeat_order(tmp_path,monkeypatch,options):
    native,request,clicks,queries=scenario(tmp_path,monkeypatch,**options)
    with pytest.raises(NativeActionBlocked):native._submit_order(request)
    assert queries==[] and clicks==[42,60]


def test_ambiguous_delta_stays_unknown_with_no_extra_clicks(tmp_path,monkeypatch):
    native,request,clicks,queries=scenario(tmp_path,monkeypatch,new_count=2)
    with pytest.raises(NativeActionBlocked,match='new_contract_not_unique'):
        native._submit_order(request)
    assert clicks==[42,60] and queries==['orders']
    delta=evidence_by_phase(tmp_path,'confirmed_order_delta_unresolved')
    assert delta['failure_reason']=='new_contract_not_unique'
    assert delta['before']==[] and len(delta['after'])==2
    assert delta['confirmation_ref'] and delta['confirmation_click_timing_ref']
    assert isinstance(delta['submitted_at'],float)
    assert isinstance(delta['observed_at'],str)


def test_confirmation_click_exception_remains_unknown_without_replay(tmp_path,monkeypatch):
    native,request,clicks,queries=scenario(tmp_path,monkeypatch,confirm_click_raises=True)
    with pytest.raises(OSError,match='confirmation click transport failed'):
        native._submit_order(request)
    assert clicks==[42,60] and queries==[]
    timing=evidence_by_phase(tmp_path,'confirmation_click_timing')
    assert timing['click_returned'] is False
    assert timing['before_at']<=timing['after_at']


def test_time_window_is_still_60_seconds_and_failure_evidence_survives(tmp_path,monkeypatch):
    native,request,clicks,queries=scenario(
        tmp_path,monkeypatch,observed_delay_seconds=61)
    with pytest.raises(NativeActionBlocked,match='time_window_unverified'):
        native._submit_order(request)
    assert clicks==[42,60] and queries==['orders']
    delta=evidence_by_phase(tmp_path,'confirmed_order_delta_unresolved')
    assert delta['failure_reason']=='time_window_unverified'
    assert delta['before']==[] and len(delta['after'])==1
    assert delta['observed_at_epoch']-delta['submitted_at']>60


def test_evidence_failure_after_submit_has_no_replay(tmp_path,monkeypatch):
    native,request,clicks,queries=scenario(tmp_path,monkeypatch)
    save=native._save_evidence
    def failing(request,phase,content):
        if phase=='confirmed_order_delta':raise OSError('disk failure')
        return save(request,phase,content)
    monkeypatch.setattr(native,'_save_evidence',failing)
    with pytest.raises(OSError,match='disk failure'):native._submit_order(request)
    assert clicks==[42,60] and queries==['orders']
