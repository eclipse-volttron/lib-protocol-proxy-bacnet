from argparse import ArgumentParser
from typing import Callable

from .bacnet_proxy import BACnetProxy

PROXY_CLASS = BACnetProxy


async def run_proxy(local_device_address, **kwargs):
    bp = BACnetProxy(local_device_address, **kwargs)
    await bp.start()

def launch_bacnet(parser: ArgumentParser) -> tuple[ArgumentParser, Callable]:
    parser.add_argument('--local-device-address', type=str, required=True,
                        help='Address on the local machine of this BACnet Proxy.')
    parser.add_argument('--bacnet-network', type=int, default=0,
                        help='The BACnet port as an offset from 47808.')
    parser.add_argument('--vendor-id', type=int, default=999,
                        help='The BACnet vendor ID to use for the local device of this BACnet Proxy.')
    parser.add_argument('--object-name', type=str, default='VOLTTRON BACnet Proxy',
                        help='The name of the local device for this BACnet Proxy.')
    return parser, run_proxy
