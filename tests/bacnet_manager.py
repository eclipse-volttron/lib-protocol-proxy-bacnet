import json
import logging

from bacpypes3.primitivedata import ObjectIdentifier, ObjectType
from gevent import joinall, sleep, spawn
from gevent.event import AsyncResult

from protocol_proxy.ipc import ProtocolProxyMessage
from protocol_proxy.manager import ProtocolProxyManager

from protocol_proxy.protocol.bacnet_proxy import BACnetProxy

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class BACnetManager:
    def __init__(self, local_device_address):
        self.ppm = ProtocolProxyManager.get_manager(BACnetProxy)
        self.local_device_address = local_device_address

    def run(self):
        self.ppm.start()
        self.ppm.get_proxy((self.local_device_address, 0), local_device_address=self.local_device_address)
        joinall([spawn(self.main_loop), spawn(self.ppm.select_loop)])

    def main_loop(self):
        while not self.ppm._stop:
           sleep(10)
           _log.debug(f'BACMan: IN MAIN LOOP')
           proxy_id = self.ppm.get_proxy_id((self.local_device_address, 0))
           result = self.ppm.send(self.ppm.peers[proxy_id],
                         ProtocolProxyMessage(
                             method_name='QUERY_DEVICE',
                             payload=json.dumps({'address': '130.20.24.157'
                                                }).encode('utf8'),
                            response_expected=True
                         ))
           if isinstance(result, AsyncResult):
               result = json.loads(result.get().decode('utf8'))
               device_id = ObjectIdentifier(tuple(result))

               _log.debug(f'BACMan: The remote device has ID: {device_id}\n')

               result = self.ppm.send(self.ppm.peers[proxy_id],
                                      ProtocolProxyMessage(
                                          method_name='READ_PROPERTY',
                                          payload=json.dumps({
                                              'device_address': '130.20.24.157',
                                              'object_identifier': str(device_id),
                                              'property_identifier': 'object-list'

                                          }).encode('utf8'),
                                          response_expected=True
                                      )
                                    )
               _log.debug('The object list has this many objects: ')
               _log.debug(len([ObjectIdentifier(tuple(r)) for r in json.loads(result.get().decode('utf8'))]))
               _log.debug('\n\n')
