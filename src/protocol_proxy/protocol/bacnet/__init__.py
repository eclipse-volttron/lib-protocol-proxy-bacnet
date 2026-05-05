import logging

from argparse import ArgumentParser
from typing import Callable

from .bacnet_proxy import BACnetProxy

PROXY_CLASS = BACnetProxy

_log = logging.getLogger(__name__)

async def run_proxy(local_interface, **kwargs):
    _log.info(f'Launching BACnet Proxy at interface {local_interface} using parameters: {kwargs}.')
    bp = BACnetProxy(local_interface, **kwargs)
    await bp.start()

def launch_bacnet(parser: ArgumentParser) -> tuple[ArgumentParser, Callable]:
    parser.add_argument('--local-interface', type=str, required=True,
                        help='Address on the local machine of this BACnet Proxy.')
    parser.add_argument('--bacnet-port', type=int, default=0,
                        help='The BACnet port as an offset from 47808.')
    parser.add_argument('--vendor-id', type=int, default=999,
                        help='The BACnet vendor ID to use for the local device of this BACnet Proxy.')
    parser.add_argument('--object-name', type=str, default='VOLTTRON BACnet Proxy',
                        help='The name of the local device for this BACnet Proxy.')
    return parser, run_proxy
