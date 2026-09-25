"""Shared fixtures for the BACnet proxy tests. Nothing here touches the network."""
import asyncio
import json

from unittest import mock
from uuid import uuid4

import pytest

from bacpypes3.apdu import AbortPDU, ErrorPDU, RejectPDU

from protocol_proxy.protocol.bacnet import bacnet_proxy as proxy_module


def error_pdu(error_class='device', error_code='unknown-object') -> ErrorPDU:
    """bacpypes3 builds these from the wire; construct one the way a device's Error reply would look."""
    pdu = ErrorPDU()
    pdu.errorClass, pdu.errorCode = error_class, error_code
    return pdu


def abort_pdu(reason=4) -> AbortPDU:
    return AbortPDU(reason=reason)


def reject_pdu(reason=1) -> RejectPDU:
    return RejectPDU(reason=reason)


class Raise:
    """Queue this in FakeBACnet to have the call raise instead of return."""
    def __init__(self, exc):
        self.exc = exc


class FakeBACnet:
    """Stands in for protocol_proxy.protocol.bacnet.bacnet.BACnet: records calls and returns queued replies."""

    def __init__(self, *args, **kwargs):
        self.init_args, self.init_kwargs = args, kwargs
        self.calls: list[tuple[str, tuple, dict]] = []
        self.replies: dict[str, list] = {}
        self.app = object()

    def queue(self, method, *replies):
        self.replies.setdefault(method, []).extend(replies)

    def _record(self, name, args, kwargs):
        self.calls.append((name, args, kwargs))
        queued = self.replies.get(name)
        reply = queued.pop(0) if queued else None
        if isinstance(reply, Raise):
            raise reply.exc
        return reply

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)

        async def method(*args, **kwargs):
            return self._record(name, args, kwargs)
        return method

    def load_cached_devices(self, network=None):          # the one synchronous method the endpoints use
        return self._record('load_cached_devices', (network,), {})

    def called(self, name):
        return [(a, k) for n, a, k in self.calls if n == name]


@pytest.fixture(autouse=True)
def event_loop_for_bacpypes():
    """bacpypes3 objects look up the current event loop when constructed; asyncio.run() leaves none behind."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    if not loop.is_closed():
        loop.run_until_complete(asyncio.sleep(0))     # let bacpypes3 objects finish their scheduled _post_init
        loop.close()
    asyncio.set_event_loop(None)


@pytest.fixture
def fake_bacnet_class(monkeypatch):
    created = []

    def factory(*args, **kwargs):
        instance = FakeBACnet(*args, **kwargs)
        created.append(instance)
        return instance
    monkeypatch.setattr(proxy_module, 'BACnet', factory)
    return created


def make_proxy(**kwargs):
    """A real BACnetProxy on its real IPC base class. Call inside a running event loop."""
    return proxy_module.BACnetProxy('10.0.0.1/24', proxy_id=uuid4(), token=uuid4(), manager_address='127.0.0.1',
                                    manager_port=1, manager_id=uuid4(), manager_token=uuid4(),
                                    registration_retry_delay=0, registration_timeout=0.05, **kwargs)


async def call(proxy, name, headers=None, **message):
    """Invoke an endpoint as the IPC layer would after authenticating the caller, decoding the JSON reply."""
    endpoint = proxy.callbacks[name].method
    inner = getattr(endpoint, '__wrapped__', endpoint)
    reply = await inner(proxy, headers, json.dumps(message).encode('utf8'))
    return None if reply is None else json.loads(reply)


def run(coro):
    """asyncio.run, cancelling any tasks the test left behind (COV and time-sync periodics) before the loop closes."""
    async def wrapper():
        try:
            return await coro
        finally:
            pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
    return asyncio.run(wrapper())
