"""Route one GuiRuntime actor to limit or exact-cancel drivers.

This facade never creates locks, workers, a second journal, or submit actions.
GuiRuntime owns serialization and the durable unknown marker.
"""
from bullet_trade.integrations.ths.request_store import Request
from .service_limit_driver import ServiceLimitDriver
from .service_cancel_driver import ServiceCancelDriver


class ServiceTradingDriver:
    def __init__(self, *, limit: ServiceLimitDriver, cancel: ServiceCancelDriver):
        if (not isinstance(limit, ServiceLimitDriver)
                or not isinstance(cancel, ServiceCancelDriver)
                or limit.account != cancel.account or limit.store is not cancel.store):
            raise ValueError('trading_driver_configuration_unverified')
        self.limit, self.cancel = limit, cancel
        self.store, self.account = limit.store, limit.account

    def _driver(self, request):
        if not isinstance(request, Request) or request.account != self.account:
            raise ValueError('request_scope_unverified')
        if request.kind in ('limit_buy', 'limit_sell'):
            return self.limit
        if request.kind == 'cancel':
            return self.cancel
        raise ValueError('request_kind_unsupported')

    def query(self, kind, should_yield):
        return self.limit.query(kind, should_yield)

    def preflight(self, request):
        return self._driver(request).preflight(request)

    def prepare(self, request):
        return self._driver(request).prepare(request)

    def validate_readback(self, request):
        return self._driver(request).validate_readback(request)

    def submit(self, request):
        return self._driver(request).submit(request)

    def abort_prepared(self, request):
        return self._driver(request).abort_prepared(request)
