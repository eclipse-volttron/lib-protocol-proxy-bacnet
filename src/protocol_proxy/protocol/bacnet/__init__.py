# TODO: Removing this will avoid import warning, but is it better to launch here instead of bacnet_proxy.main()?
from .bacnet_proxy import BACnetProxy

PROXY_CLASS = BACnetProxy
