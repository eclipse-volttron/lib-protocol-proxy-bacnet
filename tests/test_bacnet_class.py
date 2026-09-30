"""The proxy's BACnet wrapper around the bacpypes3 application, exercised against a fake application."""
import asyncio
import logging

from datetime import datetime
from unittest import mock

import pytest

from bacpypes3.apdu import ErrorRejectAbortNack, TimeSynchronizationRequest
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import Null, ObjectIdentifier

from protocol_proxy.protocol.bacnet import bacnet as bacnet_module

from tests.conftest import error_pdu


@pytest.fixture
def bacnet():
    with mock.patch.object(bacnet_module.Application, 'from_object_list', return_value=mock.Mock()) as factory:
        instance = bacnet_module.BACnet('10.0.0.1/24', bacnet_port=3, apdu_timeout=1.0, apdu_retries=1)
    instance.factory_call = factory.call_args
    return instance


def run(coro):
    return asyncio.run(coro)


@pytest.mark.parametrize('configured, udp', [(0, 47808), (3, 47811), (47809, 47809), (1024, 1024)])
def test_udp_port_accepts_offsets_and_absolute_ports(configured, udp):
    assert bacnet_module.BACnet.udp_port(configured) == udp


def test_udp_port_rejects_negative():
    with pytest.raises(ValueError):
        bacnet_module.BACnet.udp_port(-1)


def test_local_objects_reflect_port_and_apdu_settings(bacnet):
    device_object, network_port = bacnet.factory_call.args[0][:2]
    assert network_port.bacnetIPUDPPort == 47811
    assert device_object.apduTimeout == 1000 and device_object.numberOfApduRetries == 1


def test_read_property_converts_arguments_and_unwraps_errors(bacnet):
    bacnet.app.read_property = mock.AsyncMock(return_value=68.25)
    assert run(bacnet.read_property('10.0.0.5', 'analog-input,1', 'present-value', '2')) == 68.25
    address, oid, prop, index = bacnet.app.read_property.call_args.args
    assert address == Address('10.0.0.5') and oid == ObjectIdentifier('analog-input,1')
    assert prop == 'present-value' and index == 2
    bacnet.app.read_property = mock.AsyncMock(side_effect=error_pdu('device', 'unknown-object'))
    result = run(bacnet.read_property('10.0.0.5', 'analog-input,1', 'present-value'))
    assert isinstance(result, ErrorRejectAbortNack)        # returned, not raised, so the endpoint can serialize it


def test_write_property_uses_null_for_none_and_returns_errors(bacnet):
    bacnet.app.write_property = mock.AsyncMock(return_value=None)
    assert run(bacnet.write_property('10.0.0.5', 'analog-value,1', 'present-value', None, '8')) is None
    address, oid, prop, value, index, priority = bacnet.app.write_property.call_args.args
    assert isinstance(value, Null) and priority == 8 and index is None
    run(bacnet.write_property('10.0.0.5', 'analog-value,1', 'present-value', 65.0, 16, '3'))
    assert bacnet.app.write_property.call_args.args[3] == 65.0 and bacnet.app.write_property.call_args.args[4] == 3
    bacnet.app.write_property = mock.AsyncMock(side_effect=error_pdu('property', 'write-access-denied'))
    assert isinstance(run(bacnet.write_property('10.0.0.5', 'analog-value,1', 'present-value', 1, 8)), ErrorRejectAbortNack)


def test_time_synchronization_sends_request_and_logs_errors(bacnet, caplog):
    bacnet.app.request = mock.AsyncMock(return_value=None)
    when = datetime(2026, 9, 24, 12, 30, 15)
    run(bacnet.time_synchronization('10.0.0.5', when))
    request = bacnet.app.request.call_args.args[0]
    assert isinstance(request, TimeSynchronizationRequest) and request.pduDestination == Address('10.0.0.5')
    assert tuple(request.time.date)[:3] == (2026 - 1900, 9, 24) and tuple(request.time.time)[:2] == (12, 30)
    bacnet.app.request = mock.AsyncMock(return_value=error_pdu('services', 'service-request-denied'))
    with caplog.at_level(logging.WARNING):
        run(bacnet.time_synchronization('10.0.0.5', when))
    assert 'Error calling Time Synchronization' in caplog.text


def test_who_is_shapes_results_and_swallows_failures(bacnet):
    i_am = mock.Mock(pduSource=Address('10.0.0.5'), iAmDeviceIdentifier=ObjectIdentifier('device,1001'), maxAPDULengthAccepted=1024,
                     segmentationSupported='segmented-both', vendorID=999)
    bacnet.app.who_is = mock.AsyncMock(return_value=[i_am])
    found = run(bacnet.who_is(0, 4194303, '10.0.0.255'))
    assert found == [{'pduSource': '10.0.0.5', 'deviceIdentifier': ['device', 1001], 'maxAPDULengthAccepted': 1024,
                      'segmentationSupported': 'segmented-both', 'vendorID': 999}]
    low, high, dest = bacnet.app.who_is.call_args.args
    assert (low, high, dest) == (0, 4194303, Address('10.0.0.255'))
    bacnet.app.who_is = mock.AsyncMock(side_effect=asyncio.TimeoutError())
    assert run(bacnet.who_is(1, 1, '10.0.0.5')) == []
    bacnet.app.who_is = mock.AsyncMock(side_effect=RuntimeError('boom'))
    assert run(bacnet.who_is(1, 1, '10.0.0.5')) == []


def test_batch_read_collects_callback_results_and_survives_exceptions(bacnet):
    async def fake_run(self, app, callback):
        callback('t', 68.25)
        callback('f', RuntimeError('no response'))
    with mock.patch.object(bacnet_module.BatchRead, 'run', fake_run):
        result = run(bacnet.batch_read('10.0.0.5', {'t': {'object_id': 'analog-input,1', 'property': 'present-value', 'array_index': None},
                                                    'f': {'object_id': 'binary-value,1', 'property': 'present-value', 'array_index': 0}}))
    assert result['t'] == 68.25 and isinstance(result['f'], RuntimeError)
    with mock.patch.object(bacnet_module.BatchRead, 'run', mock.AsyncMock(side_effect=RuntimeError('boom'))):
        assert run(bacnet.batch_read('10.0.0.5', {})) == {}
