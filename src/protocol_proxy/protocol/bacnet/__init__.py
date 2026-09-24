"""BACnet plugin for protocol_proxy.

The proxy class is imported lazily so that running ``python -m protocol_proxy.protocol.bacnet.bacnet_proxy``
executes that module exactly once. When it is the running ``__main__`` module, attribute lookups resolve to it,
so ``protocol_proxy.protocol.bacnet.BACnetProxy`` is the very class the running proxy was built from (which
proxy plugins rely on for their ``isinstance`` checks).
"""
import logging
import sys

from typing import TYPE_CHECKING

from .bacnet import BACnet

if TYPE_CHECKING:
    from .bacnet_proxy import BACnetProxy, launch_bacnet, run_proxy

__all__ = ['BACnet', 'BACnetProxy', 'PROXY_CLASS', 'launch_bacnet', 'run_bacnet_device', 'run_proxy']

_log = logging.getLogger(__name__)

_PROXY_MODULE_NAME = f'{__name__}.bacnet_proxy'
_LAZY_ATTRIBUTES = {'BACnetProxy': 'BACnetProxy', 'PROXY_CLASS': 'BACnetProxy',
                    'launch_bacnet': 'launch_bacnet', 'run_proxy': 'run_proxy'}


def _proxy_module():
    """Return the bacnet_proxy module, reusing ``__main__`` when that is the module being run with ``python -m``."""
    main = sys.modules.get('__main__')
    if getattr(getattr(main, '__spec__', None), 'name', None) == _PROXY_MODULE_NAME:
        return main
    from . import bacnet_proxy
    return bacnet_proxy


def __getattr__(name: str):
    if name in _LAZY_ATTRIBUTES:
        return getattr(_proxy_module(), _LAZY_ATTRIBUTES[name])
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')


async def run_bacnet_device(local_interface, **kwargs):
    _log.info(f'Launching BACnet Device at interface {local_interface} using parameters: {kwargs}.')
    return BACnet(local_interface, **kwargs)
