"""BACnetProxy IPC endpoints, driven through the real IPC base class with the BACnet wrapper replaced by a fake."""
import asyncio
import json

from types import SimpleNamespace
from unittest import mock

import pytest

from bacpypes3.errors import ExecutionError

from protocol_proxy.protocol.bacnet.bacnet_proxy import BACnetProxy

from tests.conftest import call, error_pdu, make_proxy, run


def test_constructor_passes_settings_to_bacnet_and_registers_endpoints(fake_bacnet_class):
    async def main():
        proxy = make_proxy(bacnet_port=3, apdu_timeout=2.0, apdu_retries=1)
        [bacnet] = fake_bacnet_class
        assert bacnet.init_args[:2] == ('10.0.0.1/24', 3)
        assert bacnet.init_kwargs['apdu_timeout'] == 2.0 and bacnet.init_kwargs['apdu_retries'] == 1
        expected = {'BATCH_READ', 'SETUP_COV', 'CANCEL_COV', 'QUERY_DEVICE', 'READ_PROPERTY', 'TIME_SYNCHRONIZATION',
                    'SETUP_TIME_SYNCHRONIZATION', 'WRITE_PROPERTY', 'READ_DEVICE_ALL', 'WHO_IS', 'SCAN_SUBNET',
                    'READ_OBJECT_LIST_NAMES', 'READ_OBJECT_LIST', 'CLEAR_CACHE', 'GET_CACHE_STATS'}
        assert expected <= set(proxy.callbacks)
        assert proxy.callbacks['BATCH_READ'].timeout == proxy.batch_read_timeout == 30.0     # 4 s cycle -> floor
        assert proxy.callbacks['READ_PROPERTY'].timeout == proxy.request_timeout
    run(main())


def test_read_write_batch_and_query_endpoints_pass_fields_through(fake_bacnet_class):
    async def main():
        proxy = make_proxy()
        [bacnet] = fake_bacnet_class
        bacnet.queue('read_property', 68.25)
        reply = await call(proxy, 'READ_PROPERTY', device_address='10.0.0.5', object_identifier='analog-input,1',
                           property_identifier='present-value')
        assert reply == {'result': 68.25, 'error': {}}
        assert bacnet.called('read_property') == [(('10.0.0.5', 'analog-input,1', 'present-value', None), {})]

        bacnet.queue('write_property', None)
        reply = await call(proxy, 'WRITE_PROPERTY', device_address='10.0.0.5', object_identifier='analog-value,1',
                           property_identifier='present-value', value=65.0, priority=8, property_array_index=2)
        assert reply == {'result': None, 'error': {}}
        assert bacnet.called('write_property') == [(('10.0.0.5', 'analog-value,1', 'present-value', 65.0, 8, 2), {})]

        # A device error *returned* (not raised) by the wrapper is serialized into the error half.
        bacnet.queue('write_property', error_pdu('property', 'write-access-denied'))
        reply = await call(proxy, 'WRITE_PROPERTY', device_address='10.0.0.5', object_identifier='analog-value,1',
                           property_identifier='present-value', value=65.0, priority=8)
        assert reply['error']['error'] == 'ErrorPDU' and 'write-access-denied' in reply['error']['error_code']

        bacnet.queue('batch_read', {'p0': 1.0, 'p1': ExecutionError('device', 'unknown-object')})
        specs = {'p0': {'object_id': 'analog-input,1', 'property': 'present-value', 'array_index': None},
                 'p1': {'object_id': 'analog-input,2', 'property': 'present-value', 'array_index': None}}
        reply = await call(proxy, 'BATCH_READ', device_address='10.0.0.5', read_specifications=specs)
        assert reply['result'] == {'p0': 1.0} and reply['error']['p1']['error'] == 'ExecutionError'
        assert bacnet.called('batch_read') == [(('10.0.0.5', specs), {})]

        bacnet.queue('query_device', 'device,1001')
        reply = await call(proxy, 'QUERY_DEVICE', address='10.0.0.5')
        assert reply['result'] == 'device,1001' and bacnet.called('query_device') == [(('10.0.0.5', 'object-identifier'), {})]
    run(main())


def test_who_is_defaults_and_overrides(fake_bacnet_class):
    async def main():
        proxy = make_proxy()
        [bacnet] = fake_bacnet_class
        bacnet.queue('who_is', [], [{'deviceIdentifier': ['device', 7]}])
        assert (await call(proxy, 'WHO_IS'))['result'] == []
        assert bacnet.called('who_is')[0] == ((0, 4194303, '255.255.255.255:47808'), {})
        reply = await call(proxy, 'WHO_IS', device_instance_low=7, device_instance_high=7, dest='10.0.0.5')
        assert reply['result'] == [{'deviceIdentifier': ['device', 7]}]
        assert bacnet.called('who_is')[1] == ((7, 7, '10.0.0.5'), {})
    run(main())


def test_time_synchronization_endpoint(fake_bacnet_class):
    async def main():
        proxy = make_proxy()
        [bacnet] = fake_bacnet_class
        reply = await call(proxy, 'TIME_SYNCHRONIZATION', device_address='10.0.0.5', date_time='2026-09-24T12:30:15')
        assert reply == {'result': None, 'error': {}}
        (address, when), _ = bacnet.called('time_synchronization')[0]
        assert address == '10.0.0.5' and when.hour == 12 and when.second == 15
        reply = await call(proxy, 'TIME_SYNCHRONIZATION', device_address='10.0.0.5', date_time='not a date')
        assert reply['error']['error'] == 'ValueError'
    run(main())


def test_setup_time_synchronization_periodic_runs_and_cancels(fake_bacnet_class):
    async def main():
        proxy = make_proxy()
        [bacnet] = fake_bacnet_class
        await call(proxy, 'SETUP_TIME_SYNCHRONIZATION', device_address='10.0.0.5', interval='0.01', time_zone='UTC')
        assert '10.0.0.5' in proxy._time_sync_periodics
        await asyncio.sleep(0.05)
        assert len(bacnet.called('time_synchronization')) >= 2
        (_, kwargs) = bacnet.called('time_synchronization')[0]
        assert kwargs['device_address'] == '10.0.0.5' and kwargs['date_time'].tzinfo is not None
        await call(proxy, 'SETUP_TIME_SYNCHRONIZATION', device_address='10.0.0.5')      # no interval: cancel
        await asyncio.sleep(0)
        assert '10.0.0.5' not in proxy._time_sync_periodics
        await call(proxy, 'SETUP_TIME_SYNCHRONIZATION', device_address='10.0.0.5')      # cancelling again is harmless
    run(main())


def test_cov_setup_starts_subscription_and_forwards_values(fake_bacnet_class):
    async def main():
        proxy = make_proxy()
        [bacnet] = fake_bacnet_class
        peer = object()
        headers = SimpleNamespace(sender_id='driver-1')
        proxy.peers['driver-1'] = peer
        proxy.send = mock.AsyncMock()
        sub = dict(subscription_key='campus/b/dev/Temp', device_address='10.0.0.5',
                   monitored_object_identifier='analogInput:1', property_identifier='present-value',
                   issue_confirmed_notifications=True, lifetime=180.0)
        await call(proxy, 'SETUP_COV', headers=headers, **sub)
        await asyncio.sleep(0)
        assert 'campus/b/dev/Temp' in proxy._subscribed_cov
        [(args, kwargs)] = bacnet.called('change_of_value')
        assert kwargs['device_address'] == '10.0.0.5' and kwargs['object_identifier'] == 'analogInput:1'
        assert kwargs['lifetime'] == 180.0 and kwargs['confirmed'] is True and kwargs['stop_event'] is not None

        # Re-subscribing with identical parameters does not start another task.
        await call(proxy, 'SETUP_COV', headers=headers, **sub)
        await asyncio.sleep(0)
        assert len(bacnet.called('change_of_value')) == 1

        # Changed parameters stop the old subscription and start a new one.
        old_event = proxy._subscribed_cov['campus/b/dev/Temp'].stop_event
        await call(proxy, 'SETUP_COV', headers=headers, **{**sub, 'lifetime': 60.0})
        await asyncio.sleep(0)
        assert old_event.is_set() and len(bacnet.called('change_of_value')) == 2

        # Values from the device are pushed to the subscribing peer as RECEIVE_COV.
        await kwargs['cov_callback'](72.5)
        (sent_peer, message), _ = proxy.send.call_args
        assert sent_peer is peer and message.method_name == 'RECEIVE_COV'
        assert json.loads(message.payload) == {'result': {'campus/b/dev/Temp': 72.5}, 'error': {}}

        # Subscriptions have independent stop events: cancelling one leaves the other running.
        await call(proxy, 'SETUP_COV', headers=headers, **{**sub, 'subscription_key': 'campus/b/dev/Other',
                                                          'monitored_object_identifier': 'analogInput:2'})
        await asyncio.sleep(0)
        await call(proxy, 'CANCEL_COV', subscription_key='campus/b/dev/Temp')
        assert 'campus/b/dev/Temp' not in proxy._subscribed_cov
        assert not proxy._subscribed_cov['campus/b/dev/Other'].stop_event.is_set()
    run(main())


def test_read_device_all_success_and_error_paths(fake_bacnet_class):
    async def main():
        proxy = make_proxy()
        [bacnet] = fake_bacnet_class
        bacnet.queue('read_device_all', {'analog-input,1': {'object-name': 'Temp'}})
        reply = await call(proxy, 'READ_DEVICE_ALL', device_address='10.0.0.5', device_object_identifier='device,7')
        assert reply['result'] == {'analog-input,1': {'object-name': 'Temp'}}
        reply = await call(proxy, 'READ_DEVICE_ALL', device_address='10.0.0.5')      # missing field -> KeyError
        assert 'device_object_identifier' in reply['error'] and 'traceback' in reply
    run(main())


def test_scan_subnet_argument_conversion_and_errors(fake_bacnet_class):
    async def main():
        proxy = make_proxy()
        [bacnet] = fake_bacnet_class
        bacnet.queue('scan_subnet', {'devices': []})
        reply = await call(proxy, 'SCAN_SUBNET', network='10.0.0.0/24', whois_timeout='1.5', port='47809', low_id='1',
                           high_id='10', enable_brute_force=False, semaphore_limit='5', max_duration='30')
        assert reply == {'devices': []}
        [(args, kwargs)] = bacnet.called('scan_subnet')
        assert args == ('10.0.0.0/24',)
        assert kwargs == {'whois_timeout': 1.5, 'port': 47809, 'low_id': 1, 'high_id': 10, 'enable_brute_force': False,
                          'semaphore_limit': 5, 'max_duration': 30.0}
        reply = await call(proxy, 'SCAN_SUBNET')                                     # missing network
        assert 'error' in reply
    run(main())


def test_object_list_names_endpoint_converts_units_and_reports_errors(fake_bacnet_class):
    async def main():
        proxy = make_proxy()
        [bacnet] = fake_bacnet_class
        bacnet.queue('read_object_list_names_paginated',
                     {'status': 'success', 'results': {'analog-input,1': {'object-name': 'Temp', 'units': 64},
                                                       'binary-value,1': 'odd'}, 'page': 1},
                     {'status': 'error', 'error': 'device unreachable'})
        reply = await call(proxy, 'READ_OBJECT_LIST_NAMES', device_address='10.0.0.5', device_object_identifier='device,7',
                           page=1, page_size=50)
        assert reply['status'] == 'success'
        assert reply['results']['analog-input,1']['units'] in ('degreesFahrenheit', 'degrees-fahrenheit')
        assert reply['results']['binary-value,1'] == 'odd'
        [(args, kwargs)] = bacnet.called('read_object_list_names_paginated')
        assert args[:4] == ('10.0.0.5', 'device,7', 1, 50) and args[4] is False
        reply = await call(proxy, 'READ_OBJECT_LIST_NAMES', device_address='10.0.0.5', device_object_identifier='device,7')
        assert reply == {'status': 'error', 'error': 'device unreachable'}
        bacnet.app = None
        reply = await call(proxy, 'READ_OBJECT_LIST_NAMES', device_address='10.0.0.5', device_object_identifier='device,7')
        assert reply['status'] == 'error' and 'not available' in reply['error']
    run(main())


def test_cache_endpoints(fake_bacnet_class):
    async def main():
        proxy = make_proxy()
        [bacnet] = fake_bacnet_class
        proxy._object_list_cache.update({'10.0.0.5:device,7': (['a', 'b'], 0), '10.0.0.6:device,8': (['c'], 0)})
        stats = await call(proxy, 'GET_CACHE_STATS')
        assert stats['total_entries'] == 2
        reply = await call(proxy, 'CLEAR_CACHE', device_address='10.0.0.5')
        assert reply['status'] == 'success' and set(proxy._object_list_cache) == {'10.0.0.6:device,8'}
        reply = await call(proxy, 'CLEAR_CACHE')
        assert proxy._object_list_cache == {}
        bacnet.queue('load_cached_devices', [{'address': '10.0.0.5'}])
        assert await call(proxy, 'GET_CACHED_DEVICES', network='10.0.0.0/24') == [{'address': '10.0.0.5'}]
        assert bacnet.called('load_cached_devices') == [(('10.0.0.0/24',), {})]
    run(main())


def test_unique_remote_id_is_local_interface_and_port():
    assert BACnetProxy.get_unique_remote_id(('10.0.0.1/24', 3, 'extra')) == ('10.0.0.1/24', 3)
