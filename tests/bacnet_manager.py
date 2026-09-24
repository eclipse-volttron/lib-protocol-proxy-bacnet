"""Manual smoke script: launches a BACnet proxy through the gevent manager and queries a device.
Not collected by pytest; run directly with a reachable BACnet device address."""
import json
import logging
import sys

from bacpypes3.primitivedata import ObjectIdentifier
from gevent import joinall, sleep, spawn
from gevent.event import AsyncResult

from protocol_proxy.ipc import ProtocolProxyMessage
from protocol_proxy.manager.gevent import GeventProtocolProxyManager

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class BACnetManager:
    def __init__(self, local_interface: str, device_address: str, bacnet_port: int = 0):
        self.ppm: GeventProtocolProxyManager = GeventProtocolProxyManager.get_manager('bacnet')
        self.local_interface = local_interface
        self.device_address = device_address
        self.bacnet_port = bacnet_port

    def run(self):
        self.ppm.start()
        self.ppm.get_proxy((self.local_interface, self.bacnet_port),
                           local_interface=self.local_interface, bacnet_port=self.bacnet_port)
        joinall([spawn(self.main_loop), spawn(self.ppm.select_loop)])

    def main_loop(self):
        while not self.ppm._stop:
            sleep(10)
            _log.debug('BACMan: IN MAIN LOOP')
            proxy_id = self.ppm.get_proxy_id((self.local_interface, self.bacnet_port))
            result = self.ppm.send(self.ppm.peers[proxy_id],
                                   ProtocolProxyMessage(
                                       method_name='QUERY_DEVICE',
                                       payload=json.dumps({'address': self.device_address}).encode('utf8'),
                                       response_expected=True
                                   ))
            if isinstance(result, AsyncResult):
                result = json.loads(result.get().decode('utf8'))
                device_id = ObjectIdentifier(tuple(result['result']))
                _log.debug(f'BACMan: The remote device has ID: {device_id}\n')

                result = self.ppm.send(self.ppm.peers[proxy_id],
                                       ProtocolProxyMessage(
                                           method_name='READ_PROPERTY',
                                           payload=json.dumps({
                                               'device_address': self.device_address,
                                               'object_identifier': str(device_id),
                                               'property_identifier': 'object-list'
                                           }).encode('utf8'),
                                           response_expected=True
                                       ))
                object_list = json.loads(result.get().decode('utf8'))['result']
                _log.debug(f'The object list has {len(object_list)} objects.\n\n')


if __name__ == '__main__':
    if len(sys.argv) < 3:
        sys.exit(f'Usage: {sys.argv[0]} <local_interface> <device_address> [bacnet_port]')
    BACnetManager(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 0).run()
