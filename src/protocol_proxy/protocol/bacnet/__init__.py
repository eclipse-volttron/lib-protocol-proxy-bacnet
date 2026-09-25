"""BACnet plugin for protocol_proxy.

Proxies are launched with ``python -m protocol_proxy.proxy <module>:<class>`` (see protocol_proxy.proxy.launch), so
this package may import its proxy class directly.
"""
import logging

from .bacnet import BACnet
from .bacnet_proxy import BACnetProxy, launch_bacnet, run_proxy

__all__ = ['BACnet', 'BACnetProxy', 'PROXY_CLASS', 'launch_bacnet', 'run_bacnet_device', 'run_proxy']

PROXY_CLASS = BACnetProxy

_log = logging.getLogger(__name__)


async def run_bacnet_device(local_interface, **kwargs):
    _log.info(f'Launching BACnet Device at interface {local_interface} using parameters: {kwargs}.')
    return BACnet(local_interface, **kwargs)
