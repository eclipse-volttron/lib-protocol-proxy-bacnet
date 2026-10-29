"""The server role: a ServedBACnetDevice on the loopback interface, exercised by a second bacpypes3 application."""
import asyncio
import socket
from types import SimpleNamespace
from uuid import uuid4

import pytest

from bacpypes3.app import Application
from bacpypes3.local.device import DeviceObject
from bacpypes3.local.networkport import NetworkPortObject
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import Null, ObjectIdentifier

from protocol_proxy.protocol.bacnet.server import ServedBACnetDevice, ServedPoint
from tests.conftest import call, make_proxy, run

POINTS = [
    {'topic': 's/Temp', 'object_type': 'analogInput', 'instance': 1, 'units': 'degreesFahrenheit', 'initial_value': 68.25, 'cov_increment': 0.1},
    {'topic': 's/Setpoint', 'object_type': 'analogValue', 'instance': 1, 'units': 'degreesFahrenheit', 'initial_value': 72.5},
    {'topic': 's/Fan', 'object_type': 'binaryValue', 'instance': 1, 'initial_value': False},
    {'topic': 's/Mode', 'object_type': 'multiStateValue', 'instance': 1, 'number_of_states': 3, 'initial_value': 2},
    {'topic': 's/Locked', 'object_type': 'analogValue', 'instance': 2, 'remote_writable': False, 'initial_value': 1.0},
    {'topic': 's/Status', 'object_type': 'binaryInput', 'instance': 1},
]


def free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class Pushes:
    def __init__(self):
        self.values = []

    async def __call__(self, values):
        self.values.append(values)

    def latest(self, topic):
        for values in reversed(self.values):
            if topic in values:
                return values[topic]
        return None


async def wait_for(predicate, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


async def serve(points=POINTS):
    pushes = Pushes()
    served = ServedBACnetDevice('127.0.0.1/32', free_udp_port(), device_id=4242, object_name='served', push=pushes)
    served.configure_points(points)
    await served.start()
    served.pushes = pushes
    return served


async def peer_app():
    port = free_udp_port()
    device = DeviceObject(objectIdentifier=('device', 4243), objectName='peer')
    network_port = NetworkPortObject(f'127.0.0.1/32:{port}', objectIdentifier=('network-port', 1), objectName='np',
                                     networkNumber=0, networkNumberQuality='configured')
    app = Application.from_object_list([device, network_port])
    await asyncio.sleep(0.05)
    return app


def test_served_point_rules():
    p = ServedPoint.from_dict({'topic': 't', 'object_type': 'analogValue', 'instance': 3})
    assert p.remote_writable and p.identifier == ('analog-value', 3) and p.kind == 'analog'
    assert ServedPoint.from_dict({'topic': 't', 'object_type': 'analogInput', 'instance': 3}).remote_writable is False
    assert ServedPoint.from_dict({'topic': 't', 'object_type': 'binaryInput', 'instance': 3, 'remote_writable': True}).remote_writable
    b = ServedPoint.from_dict({'topic': 't', 'object_type': 'binaryValue', 'instance': 1})
    assert b.to_bacnet('on') == 'active' and b.to_bacnet(0) == 'inactive' and b.to_platform('active') is True
    m = ServedPoint.from_dict({'topic': 't', 'object_type': 'multiStateValue', 'instance': 1, 'number_of_states': 3})
    assert m.to_bacnet('2') == 2
    with pytest.raises(ValueError):
        m.to_bacnet(4)
    with pytest.raises(ValueError, match='cannot be served'):
        ServedPoint.from_dict({'topic': 't', 'object_type': 'calendar', 'instance': 1})
    with pytest.raises(ValueError, match='only present-value'):
        ServedPoint.from_dict({'topic': 't', 'object_type': 'analogValue', 'instance': 1, 'property': 'units'})
    s = ServedBACnetDevice('127.0.0.1/32', 0, device_id=1)
    with pytest.raises(ValueError, match='both name'):
        s.configure_points([{'topic': 'a', 'object_type': 'analogValue', 'instance': 1}, {'topic': 'b', 'object_type': 'analogValue', 'instance': 1}])


def test_starting_values_reads_platform_writes_and_seed():
    async def main():
        served = await serve()
        try:
            results, errors = await served.read(['s/Temp', 's/Setpoint', 's/Fan', 's/Mode', 's/Locked', 's/Status', 's/zz'])
            assert errors == {'s/zz': 'unregistered topic'}
            assert results == {'s/Temp': 68.25, 's/Setpoint': 72.5, 's/Fan': False, 's/Mode': 2, 's/Locked': 1.0, 's/Status': False}
            results, errors = await served.write({'s/Temp': 70.5, 's/Fan': 'on', 's/Mode': 3, 's/Status': True, 's/Setpoint': 'warm', 's/zz': 1})
            assert set(results) == {'s/Temp', 's/Fan', 's/Mode', 's/Status'} and 'bad value' in errors['s/Setpoint']
            results, _ = await served.read(['s/Temp', 's/Fan', 's/Mode', 's/Status'])
            assert results == {'s/Temp': 70.5, 's/Fan': True, 's/Mode': 3, 's/Status': True}
            assert served.pushes.values == []                                   # platform writes are not pushed back
            fresh = ServedBACnetDevice('127.0.0.1/32', free_udp_port(), device_id=4244, object_name='fresh')
            fresh.configure_points(POINTS[:2] + [{'topic': 's/x', 'object_type': 'analogValue', 'instance': 9}])
            await fresh.start({'s/x': 3.5, 's/Temp': 1.0})                        # seeds beat starting values
            try:
                assert fresh.seeded == 2 and (await fresh.read(['s/x', 's/Temp', 's/Setpoint']))[0] == {'s/x': 3.5, 's/Temp': 1.0, 's/Setpoint': 72.5}
                await fresh.write({'s/x': 4.0})
                await fresh.start({'s/x': 9.0})                                     # held values win over seeds
                assert (await fresh.read(['s/x']))[0] == {'s/x': 4.0} and fresh.seeded == 0
                assert await fresh.push_all() == {'s/x': 4.0, 's/Temp': 1.0, 's/Setpoint': 72.5}
            finally:
                await fresh.close()
        finally:
            await served.close()
    run(main())


def test_peer_reads_writes_and_subscribes():
    async def main():
        served = await serve()
        peer = await peer_app()
        addr = Address(f'127.0.0.1:{served.udp_port(served.port)}')
        try:
            assert float(await peer.read_property(addr, ObjectIdentifier('analog-input,1'), 'present-value')) == 68.25
            assert str(await peer.read_property(addr, ObjectIdentifier('analog-value,1'), 'object-name')) == 'Setpoint'
            await peer.write_property(addr, ObjectIdentifier('analog-value,1'), 'present-value', 65.0, None, 8)
            await peer.write_property(addr, ObjectIdentifier('binary-value,1'), 'present-value', 'active', None, 8)
            await peer.write_property(addr, ObjectIdentifier('multi-state-value,1'), 'present-value', 3, None, 8)
            assert await wait_for(lambda: served.pushes.latest('s/Mode') == 3)
            assert served.pushes.latest('s/Setpoint') == 65.0 and served.pushes.latest('s/Fan') is True
            results, _ = await served.read(['s/Setpoint', 's/Fan', 's/Mode'])
            assert results == {'s/Setpoint': 65.0, 's/Fan': True, 's/Mode': 3}
            # Relinquishing the command returns the platform's value (the relinquish default).
            await peer.write_property(addr, ObjectIdentifier('analog-value,1'), 'present-value', Null(()), None, 8)
            assert await wait_for(lambda: served.pushes.latest('s/Setpoint') == 72.5)
            # Points that are not remote-writable refuse the write; inputs are such by default.
            for obj in ('analog-value,2', 'analog-input,1', 'binary-input,1'):
                with pytest.raises(BaseException) as excinfo:          # bacpypes3's Error is not an Exception subclass
                    await peer.write_property(addr, ObjectIdentifier(obj), 'present-value', 5.0 if obj.startswith('analog') else 'active', None, 8)
                assert 'write-access-denied' in str(excinfo.value)
            results, _ = await served.read(['s/Locked', 's/Temp', 's/Status'])
            assert results == {'s/Locked': 1.0, 's/Temp': 68.25, 's/Status': False}
            # A change-of-value subscription from the peer sees the platform's writes.
            got = []
            async with peer.change_of_value(addr, ObjectIdentifier('analog-input,1'), None, True, 60) as scm:
                await served.write({'s/Temp': 69.0})                              # the initial notification comes first
                while ('present-value', 69.0) not in got and len(got) < 8:
                    pid, value = await asyncio.wait_for(scm.get_value(), 5)
                    got.append((str(pid), float(value) if str(pid) == 'present-value' else value))
            assert ('present-value', 69.0) in got
        finally:
            peer.close()
            await served.close()
    run(main())


def test_proxy_serves_and_closes_a_device(fake_bacnet_class):
    async def main():
        proxy = make_proxy()
        pushes = []

        async def fake_push(served, values):
            pushes.append((served.remote_id, values))
        proxy._push_served = fake_push
        remote = uuid4()
        headers = SimpleNamespace(remote_id=remote)
        port = free_udp_port()
        reply = await call(proxy, 'REGISTER_REMOTE', headers, role='server', local_interface='127.0.0.1/32', port=port, device_id=4242,
                           object_name='served', points=POINTS, values={'s/Status': True, 's/Temp': 1.0})
        assert reply['error'] == {}, reply
        key = f'127.0.0.1/32:{port}:4242'
        assert reply['result'] == {'server': key, 'points': len(POINTS), 'seeded': 2, 'port': port}
        served = proxy.servers[key]
        assert proxy.remotes[remote] is served and served.app is not None
        await asyncio.sleep(0.05)
        assert pushes and pushes[0][0] == remote and pushes[0][1]['s/Status'] is True and pushes[0][1]['s/Temp'] == 1.0   # seeds win
        reply = await call(proxy, 'READ_POINTS', headers, topics=['s/Temp', 's/zz'])
        assert reply == {'result': {'s/Temp': 1.0}, 'error': {'s/zz': 'unregistered topic'}}
        reply = await call(proxy, 'WRITE_POINTS', headers, values={'s/Temp': 70.0})
        assert reply['result']['s/Temp'] == {'status': 'stored', 'value': 70.0}
        reply = await call(proxy, 'REGISTER_REMOTE', headers, role='server', local_interface='127.0.0.1/32', port=port, device_id=4242,
                           points=POINTS[:2])
        assert reply['error'] == {} and proxy.servers[key] is served and set(served.points) == {'s/Temp', 's/Setpoint'}
        assert (await call(proxy, 'READ_POINTS', headers, topics=['s/Temp']))['result'] == {'s/Temp': 70.0}   # kept
        reply = await call(proxy, 'REGISTER_REMOTE', headers, role='server', local_interface='127.0.0.1/32', device_id=1)
        assert 'needs port' in reply['error']['device']
        reply = await call(proxy, 'REGISTER_REMOTE', headers, role='client', local_interface='127.0.0.1/32', port=1, device_id=1)
        assert 'served devices only' in reply['error']['device']
        reply = await call(proxy, 'READ_POINTS', SimpleNamespace(remote_id=uuid4()), topics=['s/Temp'])
        assert 'No served device' in reply['error']['device']
        reply = await call(proxy, 'CLOSE_REMOTE', headers)
        assert reply['result'] == {'closed': True} and proxy.servers == {} and proxy.remotes == {}
    run(main())
