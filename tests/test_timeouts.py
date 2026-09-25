"""Launch options and IPC limits derived from the APDU settings. Runs without a network."""
from argparse import ArgumentParser
from unittest import mock

import pytest

from protocol_proxy.protocol.bacnet import bacnet as bacnet_module
from protocol_proxy.protocol.bacnet.bacnet_proxy import BACnetProxy, launch_bacnet


def test_launch_options_include_apdu_settings():
    parser, _ = launch_bacnet(ArgumentParser())
    opts = vars(parser.parse_args(['--local-interface', '10.0.0.1/24']))
    assert (opts['apdu_timeout'], opts['apdu_retries'], opts['batch_read_timeout']) == (3.0, 3, None)
    opts = vars(parser.parse_args(['--local-interface', '10.0.0.1/24', '--apdu-timeout', '1.5', '--apdu-retries', '1',
                                   '--batch-read-timeout', '45']))
    assert (opts['apdu_timeout'], opts['apdu_retries'], opts['batch_read_timeout']) == (1.5, 1, 45.0)


def test_apdu_settings_reach_the_local_device_object():
    with mock.patch.object(bacnet_module.Application, 'from_object_list') as from_object_list:
        bacnet_module.BACnet('10.0.0.1/24', apdu_timeout=1.5, apdu_retries=1)
    device_object = from_object_list.call_args.args[0][0]
    assert device_object.apduTimeout == 1500 and device_object.numberOfApduRetries == 1
    with mock.patch.object(bacnet_module.Application, 'from_object_list') as from_object_list:
        bacnet_module.BACnet('10.0.0.1/24')
    device_object = from_object_list.call_args.args[0][0]
    assert device_object.apduTimeout == 3000 and device_object.numberOfApduRetries == 3


@pytest.mark.parametrize('apdu_timeout, retries, explicit, expected', [
    (3.0, 3, None, (30.0, 60.0)),        # 12 s cycle: request 2 cycles -> floor 30; batch 5 cycles = 60
    (1.0, 0, None, (30.0, 30.0)),        # tiny cycle: both floored at 30
    (10.0, 3, None, (80.0, 200.0)),      # 40 s cycle
    (3.0, 3, 45.0, (30.0, 45.0)),        # explicit batch limit honoured
    (3.0, 3, 5.0, (30.0, 30.0)),         # but never below the floor
])
def test_callback_timeouts(apdu_timeout, retries, explicit, expected):
    assert BACnetProxy.callback_timeouts(apdu_timeout, retries, explicit) == expected
