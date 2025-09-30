import asyncio
import json
import logging
import sys
import traceback

from dataclasses import dataclass
from datetime import datetime
from functools import partial

from protocol_proxy.ipc import callback, ProtocolProxyMessage
from protocol_proxy.proxy import launch
from protocol_proxy.proxy.asyncio import AsyncioProtocolProxy

from .bacnet import BACnet
from .bacnet_utils import make_jsonable
from .cache_handler import (_clear_cache_for_device, _get_cache_stats)
from .json import serialize

_log = logging.getLogger(__name__)


@dataclass
class COVSubscription:
    address: str
    object_identifier: str
    confirmed: bool | None
    lifetime: bool | None
    stop_event: asyncio.Event = asyncio.Event()

    def is_unchanged(self, address: str, object_identifier: str, confirmed: bool | None, lifetime: bool | None) -> bool:
        return (address == self.address and object_identifier == self.object_identifier
                and confirmed == confirmed and lifetime == lifetime)

class BACnetProxy(AsyncioProtocolProxy):
    def __init__(self, local_device_address, bacnet_network=0, vendor_id=999, object_name='VOLTTRON BACnet Proxy',
                 **kwargs):
        _log.debug('IN BACNETPROXY __init__')
        super(BACnetProxy, self).__init__(**kwargs)
        self.bacnet = BACnet(local_device_address, bacnet_network, vendor_id, object_name, **kwargs)
        self.loop = asyncio.get_event_loop()
        
        # Cache for object-list to avoid re-reading on every page request
        # Format: {device_key: (object_list, timestamp)}
        self._object_list_cache = {}
        self._cache_timeout = 300
        self._subscribed_cov: dict[str, COVSubscription] = {}

        self.register_callback(self.batch_read_endpoint, 'BATCH_READ', provides_response=True)
        #self.register_callback(self.confirmed_private_transfer_endpoint, 'CONFIRMED_PRIVATE_TRANSFER', provides_response=True)
        self.register_callback(self.cov_setup_endpoint, 'SETUP_COV', provides_response=False)
        self.register_callback(self.cov_cancel_endpoint, 'CANCEL_COV', provides_response=False)
        self.register_callback(self.query_device_endpoint, 'QUERY_DEVICE', provides_response=True)
        self.register_callback(self.read_property_endpoint, 'READ_PROPERTY', provides_response=True)
        #self.register_callback(self.read_property_multiple_endpoint, 'READ_PROPERTY_MULTIPLE', provides_response=True)
        self.register_callback(self.time_synchronization_endpoint, 'TIME_SYNCHRONIZATION', provides_response=True)
        self.register_callback(self.write_property_endpoint, 'WRITE_PROPERTY', provides_response=True)
        self.register_callback(self.read_device_all_endpoint, 'READ_DEVICE_ALL', provides_response=True)
        self.register_callback(self.who_is_endpoint, 'WHO_IS', provides_response=True)
        self.register_callback(self.scan_subnet_endpoint, 'SCAN_SUBNET', provides_response=True, timeout=300)
        self.register_callback(self.read_object_list_names_endpoint, 'READ_OBJECT_LIST_NAMES', provides_response=True, timeout=300)
        self.register_callback(self.read_object_list_names_endpoint, 'READ_OBJECT_LIST', provides_response=True, timeout=300)
        self.register_callback(self.clear_cache_endpoint, 'CLEAR_CACHE', provides_response=True)
        self.register_callback(self.get_cache_stats_endpoint, 'GET_CACHE_STATS', provides_response=True)

    @callback
    async def batch_read_endpoint(self, _, raw_message: bytes):
        """Endpoint to gracefully handle read multiple with fallback to single reads."""
        message = json.loads(raw_message.decode('utf8'))
        address = message['device_address']
        read_specifications = message['read_specifications']
        result = await self.bacnet.batch_read(address, read_specifications)
        return serialize(result)

    # @callback
    # async def confirmed_private_transfer_endpoint(self, _, raw_message: bytes):
    #     """Endpoint for confirmed private transfer."""
    #     message = json.loads(raw_message.decode('utf8'))
    #     address = Address(message['address'])
    #     vendor_id = message['vendor_id']
    #     service_number = message['service_number']
    #     # TODO: from_json appears not to exist.
    #     #  Need a means to deserialize parameters for TagList. Commenting endpoint until fixed.
    #     service_parameters = TagList.from_json(message.get('service_parameters', []))
    #     result = await self.bacnet.confirmed_private_transfer(address, vendor_id, service_number, service_parameters)
    #     return serialize(result)

    async def cov_callback_function(self, peer, key, value):
        message = ProtocolProxyMessage(
            method_name='RECEIVE_COV',
            payload=serialize({key: value})
            )
        _log.debug(f'@@@@@@ SENDING COV CALLBACK: {message}')
        await self.send(peer, message)

    @callback
    async def cov_setup_endpoint(self, headers, raw_message: bytes):
        """Endpoint for starting or modifying change of value subscription."""
        peer = self.peers.get(headers.sender_id)
        message = json.loads(raw_message.decode('utf8'))
        key = message['subscription_key']
        address = message['device_address']
        object_identifier = message['monitored_object_identifier']
        property_identifier = message['property_identifier']
        confirmed = message['issue_confirmed_notifications']
        lifetime = message['lifetime']
        if existing := self._subscribed_cov.get(key):
            if existing.is_unchanged(address, object_identifier, confirmed, lifetime):
                return
            else:
                existing.stop_event.set()
                self._subscribed_cov.pop(key)
        # TODO: What happens if remote has existing subscription that was lost on this end?
        self._subscribed_cov[key] = COVSubscription(address, object_identifier, confirmed, lifetime)
        cov_callback = partial(self.cov_callback_function, peer, key)
        self.loop.create_task(self.bacnet.change_of_value(device_address=address, object_identifier=object_identifier,
                                          process_identifier=None, confirmed=confirmed, lifetime=lifetime,
                                          stop_event=self._subscribed_cov[key].stop_event, cov_callback=cov_callback,
                                                          property_identifier=property_identifier
                                                          ))

    @callback
    async def cov_cancel_endpoint(self, _, raw_message: bytes):
        message = json.loads(raw_message.decode('utf8'))
        key = message['subscription_key']
        if existing := self._subscribed_cov.pop(key):
            existing.stop_event.set()

    @callback
    async def query_device_endpoint(self, _, raw_message: bytes):
        """Endpoint for querying a device."""
        message = json.loads(raw_message.decode('utf8'))
        address = message['address']
        property_name = message.get('property_name', 'object-identifier')
        result = await self.bacnet.query_device(address, property_name)
        return serialize(result)

    @callback
    async def read_property_endpoint(self, _, raw_message: bytes):
        """Endpoint for reading a property from a BACnet device."""
        message = json.loads(raw_message.decode('utf8'))
        address = message['device_address']
        object_identifier = message['object_identifier']
        property_identifier = message['property_identifier']
        property_array_index = message.get('property_array_index', None)
        result = await self.bacnet.read_property(address, object_identifier, property_identifier, property_array_index)
        return serialize(result)

    # @callback # TODO: Underlying service is not fully implemented. Commenting endpoint until resolved.
    # async def read_property_multiple_endpoint(self, _, raw_message: bytes):
    #     """Endpoint for reading multiple properties from a BACnet device."""
    #     message = json.loads(raw_message.decode('utf8'))
    #     address = message['device_address']
    #     read_specifications = message['read_specifications']
    #     result = await self.bacnet.read_property_multiple(address, read_specifications)
    #     return serialize(result)

    @callback
    async def time_synchronization_endpoint(self, _, raw_message: bytes):
        """Endpoint for setting time on a BACnet device."""
        message = json.loads(raw_message.decode('utf8'))
        address = message['address']
        if date_time_string := message.get('date_time'):
            try:
                date_time = datetime.fromisoformat(date_time_string)
            except ValueError as e:
                return serialize(e)
        result = await self.bacnet.time_synchronization(address, date_time)
        return serialize(result)

    @callback
    async def write_property_endpoint(self, _, raw_message: bytes):
        """Endpoint for writing a property to a BACnet device."""
        message = json.loads(raw_message.decode('utf8'))
        address = message['device_address']
        object_identifier = message['object_identifier']
        property_identifier = message['property_identifier']
        value = message['value']
        priority = message['priority']
        property_array_index = message.get('property_array_index', None)
        result = await self.bacnet.write_property(address, object_identifier, property_identifier, value, priority,
                                            property_array_index)
        return serialize(result)

    # @callback  # TODO: Underlying service is not fully implemented. Commenting endpoint until resolved.
    # async def write_property_multiple_endpoint(self, _, raw_message: bytes):
    #     """Endpoint for writing multiple properties to a BACnet device."""
    #     message = json.loads(raw_message.decode('utf8'))
    #     address = message['device_address']
    #     write_specifications = message['write_specifications']
    #     result = await self.bacnet.write_property_multiple(address, write_specifications)
    #     return serialize(result)

    @callback
    async def read_device_all_endpoint(self, _, raw_message: bytes):
        """Endpoint for reading all properties from a BACnet device."""
        try:
            message = json.loads(raw_message.decode('utf8'))
            device_address = message['device_address']
            device_object_identifier = message['device_object_identifier']
            result = await self.bacnet.read_device_all(device_address, device_object_identifier)
            # if not result:  # TODO: Is there really a good reason to fill in a value here? This isn't really an error.
            #     return json.dumps({"error": "No data returned from read_device_all"}).encode('utf8')
            return serialize(result)
        except Exception as e:
            # TODO: Should we handle this with serialize, instead?
            tb = traceback.format_exc()
            return json.dumps({"error": str(e), "traceback": tb}).encode('utf8')

    @callback
    async def who_is_endpoint(self, _, raw_message: bytes):
        """Endpoint for WHO-IS discovery."""
        message = json.loads(raw_message.decode('utf8'))
        device_instance_low = message.get('device_instance_low', 0)
        device_instance_high = message.get('device_instance_high', 4194303)
        dest = message.get('dest', '255.255.255.255:47808')
        apdu_timeout = message.get('apdu_timeout', None)  # Keep for backward compatibility but don't use  # TODO: Why!?
        result = await self.bacnet.who_is(device_instance_low, device_instance_high, dest)
        return serialize(result)

    @callback
    async def scan_subnet_endpoint(self, _, raw_message: bytes):
        """Endpoint for subnet scanning.
           Input JSON (all optional except network):
             {
               "network": "192.168.1.0/24",
               "whois_timeout": 2.0,
               "port": 47808,
               "low_id": 0,
               "high_id": 4194303,
               "enable_brute_force": true,
               "semaphore_limit": 20,
               "max_duration": 280.0
             }
           Note: Removed force_fresh_scan - scan is always real but cache-informed.
        """
        try:
            message = json.loads(raw_message.decode('utf8'))
            network_str = message['network']
            whois_timeout = float(message.get('whois_timeout', 2.0))
            port = int(message.get('port', 47808))
            low_id = int(message.get('low_id', 0))
            high_id = int(message.get('high_id', 4194303))
            enable_brute_force = bool(message.get('enable_brute_force', True))
            semaphore_limit = int(message.get('semaphore_limit', 20))
            max_duration = float(message.get('max_duration', 280.0))

            # Removed force_fresh_scan parameter
            result = await self.bacnet.scan_subnet(network_str,
                                                   whois_timeout=whois_timeout,
                                                   port=port,
                                                   low_id=low_id,
                                                   high_id=high_id,
                                                   enable_brute_force=enable_brute_force,
                                                   semaphore_limit=semaphore_limit,
                                                   max_duration=max_duration)
            return json.dumps(result).encode('utf8')
        except Exception as e:
            _log.error(f"scan_subnet_endpoint error: {e}")
            return json.dumps({"error": str(e)}).encode('utf8')

    @callback
    async def read_object_list_names_endpoint(self, _, raw_message: bytes):
        """Endpoint for reading object-list and object-names from a BACnet device with pagination and caching."""
        try:
            message = json.loads(raw_message.decode('utf8'))
            device_address = message['device_address']
            device_object_identifier = message['device_object_identifier']
            page = message.get('page', 1)
            page_size = message.get('page_size', 100)
            force_fresh_read = message.get('force_fresh_read', False)

            logging.getLogger(__name__).info(
                f"read_object_list_names_endpoint called for device {device_address}, page {page}, page_size {page_size}, force_fresh_read={force_fresh_read}"
            )

            # Check if the BACnet application is still connected
            if not hasattr(self.bacnet, 'app') or self.bacnet.app is None:
                return json.dumps({
                    "status": "error",
                    "error": "BACnet application not available"
                }).encode('utf8')

            result = await self.bacnet.read_object_list_names_paginated(
                device_address, device_object_identifier, page, page_size, force_fresh_read,
                self._object_list_cache, self._cache_timeout)

            logging.getLogger(__name__).info(
                f"read_object_list_names_paginated returned response with status: {result.get('status')}"
            )

            # Check for error in the result
            if result.get('status') == 'error':
                logging.getLogger(__name__).error(
                    f"Error in read_object_list_names_paginated: {result['error']}")
                return json.dumps(result).encode('utf8')

            # Make the results jsonable using the unified method
            if 'results' in result:
                jsonable_results = {}
                for obj_id, properties in result['results'].items():
                    if isinstance(properties, dict):
                        processed_properties = {}
                        for prop_name, prop_value in properties.items():
                            # Set context for units conversion
                            if prop_name == 'units' and isinstance(prop_value, int):
                                # Handle units specifically - try to convert to EngineeringUnits
                                try:
                                    from bacpypes3.basetypes import EngineeringUnits
                                    engineering_unit = EngineeringUnits(prop_value)
                                    unit_str = str(engineering_unit)
                                    if unit_str.startswith(
                                            'EngineeringUnits(') and unit_str.endswith(')'):
                                        processed_properties[prop_name] = unit_str[
                                            17:-1]    # Remove "EngineeringUnits(" and ")"
                                    else:
                                        processed_properties[prop_name] = unit_str
                                    logging.getLogger(__name__).debug(
                                        f"Converted units {prop_value} to {processed_properties[prop_name]} for {obj_id}"
                                    )
                                except (ImportError, ValueError, TypeError) as e:
                                    logging.getLogger(__name__).warning(
                                        f"Failed to convert units {prop_value} for {obj_id}: {e}")
                                    processed_properties[prop_name] = make_jsonable(prop_value)
                            else:
                                # Use unified make_jsonable for all other properties
                                processed_properties[prop_name] = make_jsonable(prop_value)
                        jsonable_results[str(obj_id)] = processed_properties
                    else:
                        jsonable_results[str(obj_id)] = make_jsonable(properties)
                result['results'] = jsonable_results

            return json.dumps(result).encode('utf8')
        except Exception as e:
            tb = traceback.format_exc()
            logging.getLogger(__name__).error(
                f"Exception in read_object_list_names_endpoint: {e}\n{tb}")
            return json.dumps({"status": "error", "error": str(e), "traceback": tb}).encode('utf8')

    @callback
    async def clear_cache_endpoint(self, _, raw_message: bytes):
        """Endpoint for clearing cache."""
        message = json.loads(raw_message.decode('utf8'))
        device_address = message.get('device_address', None)
        device_object_identifier = message.get('device_object_identifier', None)

        if device_address:
            _clear_cache_for_device(self._object_list_cache, device_address, device_object_identifier)
            result = {"status": "success", "message": f"Cache cleared for device {device_address}"}
        else:
            self._object_list_cache.clear()
            result = {"status": "success", "message": "All cache cleared"}

        return json.dumps(result).encode('utf8')

    @callback
    async def get_cache_stats_endpoint(self, _, raw_message: bytes):
        """Endpoint for getting cache statistics."""
        result = _get_cache_stats(self._object_list_cache, self._cache_timeout)
        return json.dumps(result).encode('utf8')

    #TODO Fix
    @callback
    async def get_cached_devices_endpoint(self, _, raw_message: bytes):
        """New endpoint specifically for retrieving cached devices without scanning."""
        try:
            message = json.loads(raw_message.decode('utf8'))
            network_str = message.get('network', None)    # Optional network filter

            cached_devices = self.bacnet.load_cached_devices(network_str)
            return json.dumps(cached_devices).encode('utf8')
        except Exception as e:
            _log.error(f"get_cached_devices_endpoint error: {e}")
            return json.dumps({"error": str(e)}).encode('utf8')

    @classmethod
    def get_unique_remote_id(cls, unique_remote_id: tuple) -> tuple:
        """Get a unique identifier for the proxy server
         given a unique_remote_id and protocol-specific set of parameters."""
        return unique_remote_id[0:2]  # TODO: How can we know what the first two params really are?
                                      #  (Ideally they are address and port.)
                                      #  Consider named tuple?

if __name__ == '__main__':
    from . import launch_bacnet
    sys.exit(launch(launch_bacnet))
