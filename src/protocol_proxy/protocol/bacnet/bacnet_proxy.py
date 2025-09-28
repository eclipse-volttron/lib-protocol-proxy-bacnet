import asyncio
import ipaddress
import json
import logging
import sys
import time
import traceback
import csv

from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Type

from bacpypes3.basetypes import PropertyReference
from bacpypes3.lib.batchread import BatchRead, DeviceAddressObjectPropertyReference
from bacpypes3.pdu import Address
from bacpypes3.apdu import ErrorRejectAbortNack
from bacpypes3.primitivedata import ObjectIdentifier, TagList

from protocol_proxy.ipc import callback, ProtocolProxyMessage
from protocol_proxy.proxy import launch
from protocol_proxy.proxy.asyncio import AsyncioProtocolProxy

from .bacnet import BACnet
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
        self.register_callback(self.confirmed_private_transfer_endpoint, 'CONFIRMED_PRIVATE_TRANSFER', provides_response=True)
        self.register_callback(self.cov_setup_endpoint, 'SETUP_COV', provides_response=False)
        self.register_callback(self.cov_cancel_endpoint, 'CANCEL_COV', provides_response=False)
        self.register_callback(self.query_device_endpoint, 'QUERY_DEVICE', provides_response=True)
        self.register_callback(self.read_property_endpoint, 'READ_PROPERTY', provides_response=True)
        self.register_callback(self.read_property_multiple_endpoint, 'READ_PROPERTY_MULTIPLE', provides_response=True)
        self.register_callback(self.time_synchronization_endpoint, 'TIME_SYNCHRONIZATION', provides_response=True)
        self.register_callback(self.write_property_endpoint, 'WRITE_PROPERTY', provides_response=True)
        self.register_callback(self.read_device_all_endpoint, 'READ_DEVICE_ALL', provides_response=True)
        self.register_callback(self.who_is_endpoint, 'WHO_IS', provides_response=True)
        self.register_callback(self.scan_subnet_endpoint, 'SCAN_SUBNET', provides_response=True, timeout=300)
        self.register_callback(self.read_object_list_names_endpoint, 'READ_OBJECT_LIST_NAMES', provides_response=True, timeout=300)
        self.register_callback(self.read_object_list_names_endpoint, 'READ_OBJECT_LIST', provides_response=True, timeout=300)
        self.register_callback(self.clear_cache_endpoint, 'CLEAR_CACHE', provides_response=True)
        self.register_callback(self.get_cache_stats_endpoint, 'GET_CACHE_STATS', provides_response=True)

        
    def save_discovered_device(self, device_info: dict, network_str: str, scan_method: str = "unknown"):
        """Save a discovered device to CSV cache."""
        try:
            device_instance = device_info.get('deviceIdentifier', [None, None])[1]
            device_address = device_info.get('pduSource', '').split(':')[0]
            
            if not device_instance or not device_address:
                return
            
            # Set up cache file path (create directory if needed)
            from pathlib import Path
            cache_dir = Path.home() / '.bacnet_scan_tool'
            cache_dir.mkdir(exist_ok=True)
            device_cache_file = cache_dir / 'discovered_devices.csv'
                
            # Create CSV if it doesn't exist
            if not device_cache_file.exists():
                with open(device_cache_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['device_instance', 'device_address', 'vendor_id', 'first_discovered', 'last_seen', 'scan_count', 'networks_found_on'])
            
            # Read existing data
            existing = {}
            with open(device_cache_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing[(row['device_instance'], row['device_address'])] = row
            
            # Update or create record
            key = (str(device_instance), device_address)
            now = datetime.now().isoformat()
            
            if key in existing:
                # Update existing
                existing[key]['last_seen'] = now
                existing[key]['scan_count'] = str(int(existing[key]['scan_count']) + 1)
                networks = existing[key]['networks_found_on'].split(';') if existing[key]['networks_found_on'] else []
                if network_str not in networks:
                    networks.append(network_str)
                existing[key]['networks_found_on'] = ';'.join(networks)
            else:
                # New device
                existing[key] = {
                    'device_instance': str(device_instance),
                    'device_address': device_address,
                    'vendor_id': str(device_info.get('vendorID', '')),
                    'first_discovered': now,
                    'last_seen': now,
                    'scan_count': '1',
                    'networks_found_on': network_str
                }
            
            # Write back CSV
            with open(device_cache_file, 'w', newline='') as f:
                fieldnames = ['device_instance', 'device_address', 'vendor_id', 'first_discovered', 'last_seen', 'scan_count', 'networks_found_on']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(existing.values())
            
            # Also save JSON copy
            device_json_file = cache_dir / 'discovered_devices.json'
            with open(device_json_file, 'w') as f:
                json.dump(list(existing.values()), f, indent=2)
                
            _log.info(f"Saved device {device_instance} at {device_address} to {device_cache_file} and JSON")
            
        except Exception as e:
            _log.error(f"Error saving device: {e}")

    def load_cached_devices(self, network_str: str = None) -> list:
        """Load cached devices from CSV file.
        
        Args:
            network_str: Optional network filter (e.g., "192.168.1.0/24")
                        If provided, only returns devices found on that network
                        
        Returns:
            list: Devices in same format as scan_subnet returns
        """
        try:
            from pathlib import Path
            cache_dir = Path.home() / '.bacnet_scan_tool'
            device_cache_file = cache_dir / 'discovered_devices.csv'
            
            if not device_cache_file.exists():
                _log.debug("No device cache file found")
                return []
            
            devices = []
            with open(device_cache_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Filter by network if specified
                    if network_str:
                        networks = row.get('networks_found_on', '').split(';') if row.get('networks_found_on') else []
                        if network_str not in networks:
                            continuenetworks = row.get('networks_found_on', '').split(';') if row.get('networks_found_on') else []
                        if network_str not in networks:
                            continue
                    
                    # Convert back to scan_subnet format
                    device = {
                        'pduSource': f"{row['device_address']}:47808",
                        'deviceIdentifier': ['device', int(row['device_instance'])],
                        'vendorID': int(row['vendor_id']) if row['vendor_id'] and row['vendor_id'].isdigit() else None,
                        'vendorName': '',  # Could be looked up from vendor_id
                        'maxAPDULengthAccepted': None,  # Not stored in cache
                        'segmentationSupported': None,  # Not stored in cache
                        '_cached': True,  # Mark as from cache
                        '_cache_info': {
                            'first_discovered': row['first_discovered'],
                            'last_seen': row['last_seen'],
                            'scan_count': int(row['scan_count']) if row['scan_count'].isdigit() else 0,
                            'networks': row.get('networks_found_on', '').split(';') if row.get('networks_found_on') else []
                        }
                    }
                    devices.append(device)
            
            _log.info(f"Loaded {len(devices)} cached devices" + (f" for network {network_str}" if network_str else ""))
            return devices
            
        except Exception as e:
            _log.error(f"Error loading cached devices: {e}")
            return []

    @callback
    async def batch_read_endpoint(self, _, raw_message: bytes):
        """Endpoint to gracefully handle read multiple with fallback to single reads."""
        message = json.loads(raw_message.decode('utf8'))
        address = message['device_address']
        read_specifications = message['read_specifications']
        result = await self.bacnet.batch_read(address, read_specifications)
        return serialize(result)

    @callback
    async def confirmed_private_transfer_endpoint(self, _, raw_message: bytes):
        """Endpoint for confirmed private transfer."""
        message = json.loads(raw_message.decode('utf8'))
        address = Address(message['address'])
        vendor_id = message['vendor_id']
        service_number = message['service_number']
        # TODO: from_json appears to be an AI hallucination.
        #  Need a means to deserialize parameters for TagList or need to not expose this endpoint.
        service_parameters = TagList.from_json(message.get('service_parameters', []))
        result = await self.bacnet.confirmed_private_transfer(address, vendor_id, service_number, service_parameters)
        return serialize(result)

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

    @callback
    async def read_property_multiple_endpoint(self, _, raw_message: bytes):
        """Endpoint for reading multiple properties from a BACnet device."""
        message = json.loads(raw_message.decode('utf8'))
        address = message['device_address']
        read_specifications = message['read_specifications']
        result = await self.bacnet.read_property_multiple(address, read_specifications)
        return serialize(result)

    @callback
    async def time_synchronization_endpoint(self, _, raw_message: bytes):
        """Endpoint for setting time on a BACnet device."""
        message = json.loads(raw_message.decode('utf8'))
        address = message['address']
        date_time = datetime.fromisoformat(message['date_time']) if hasattr(message, 'date_time') else None
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

    @callback
    async def write_property_multiple_endpoint(self, _, raw_message: bytes):
        """Endpoint for writing multiple properties to a BACnet device."""
        message = json.loads(raw_message.decode('utf8'))
        address = message['device_address']
        write_specifications = message['write_specifications']
        result = await self.bacnet.write_property_multiple(address, write_specifications)
        return serialize(result)

    @callback
    async def read_device_all_endpoint(self, _, raw_message: bytes):
        """Endpoint for reading all properties from a BACnet device."""
        try:
            message = json.loads(raw_message.decode('utf8'))
            device_address = message['device_address']
            device_object_identifier = message['device_object_identifier']
            result = await self.read_device_all(device_address, device_object_identifier)
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
        result = await self.who_is(device_instance_low, device_instance_high, dest)
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
               "max_duration": 280.0,
               "force_fresh_scan": false
             }
           Backward compatible: old payload with only network still works.
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
            force_fresh_scan = bool(message.get('force_fresh_scan', False))
            result = await self.scan_subnet(
                network_str,
                whois_timeout=whois_timeout,
                port=port,
                low_id=low_id,
                high_id=high_id,
                enable_brute_force=enable_brute_force,
                semaphore_limit=semaphore_limit,
                max_duration=max_duration,
                force_fresh_scan=force_fresh_scan
            )
            return serialize(result)
        except Exception as e:
            # TODO: Should we handle this with serialize, instead?
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
            force_fresh_read = message.get('force_fresh_read', False)  # New parameter
            
            logging.getLogger(__name__).info(f"read_object_list_names_endpoint called for device {device_address}, page {page}, page_size {page_size}, force_fresh_read={force_fresh_read}")
            
            # Check if the BACnet application is still connected
            if not hasattr(self.bacnet, 'app') or self.bacnet.app is None:
                # TODO: Should we handle this with serialize, instead?
                return json.dumps({"status": "error", "error": "BACnet application not available"}).encode('utf8')
            
            result = await self.read_object_list_names_paginated(device_address, device_object_identifier, page, page_size, force_fresh_read)
            
            logging.getLogger(__name__).info(f"read_object_list_names_paginated returned response with status: {result.get('status')}")
            
            # Check for error in the result
            if result.get('status') == 'error':
                # TODO: Should we handle this with serialize, instead?
                logging.getLogger(__name__).error(f"Error in read_object_list_names_paginated: {result['error']}")
                return json.dumps(result).encode('utf8')

            # TODO: Why is this turning all ints into engineering units?
            def make_jsonable(val):
                if isinstance(val, (str, int, float, bool)):
                    # Special handling for integer units - convert to EngineeringUnits name
                    if isinstance(val, int):
                        # Check if this looks like a BACnet EngineeringUnits value
                        try:
                            from bacpypes3.basetypes import EngineeringUnits
                            # Try to convert the integer to an EngineeringUnits enum
                            engineering_unit = EngineeringUnits(val)
                            unit_str = str(engineering_unit)
                            # Handle BACnet EngineeringUnits string format
                            if unit_str.startswith('EngineeringUnits(') and unit_str.endswith(')'):
                                return unit_str[17:-1]  # Remove "EngineeringUnits(" and ")"
                            else:
                                return unit_str
                        except (ImportError, ValueError, TypeError):
                            # If conversion fails, return the original value
                            pass
                    return val
                if val is None:
                    return None
                if isinstance(val, (list, tuple, set)):
                    return [make_jsonable(v) for v in val]
                if isinstance(val, dict):
                    return {str(k): make_jsonable(v) for k, v in val.items()}
                if isinstance(val, (bytes, bytearray)):
                    return val.hex()
                # Handle BACnet EngineeringUnits and other enum-like objects
                if hasattr(val, '__class__') and 'EngineeringUnits' in str(val.__class__):
                    # This is a BACnet EngineeringUnits object
                    unit_str = str(val)
                    if unit_str.startswith('EngineeringUnits(') and unit_str.endswith(')'):
                        return unit_str[17:-1]  # Remove "EngineeringUnits(" and ")"
                    else:
                        return unit_str
                if hasattr(val, 'name') and hasattr(val, 'value'):
                    # This is likely a standard Python enum
                    return str(val.name)
                if hasattr(val, 'name') and not hasattr(val, 'value'):
                    # Handle other enum-like objects that only have name
                    return str(val.name)
                if hasattr(val, '__str__'):
                    val_str = str(val)
                    # Skip conversion to FORCED if it looks like an error object
                    if 'ErrorType' in val_str or 'Error' in type(val).__name__:
                        return None
                    # Check if it's a BACnet EngineeringUnits string representation
                    if 'EngineeringUnits:' in val_str or 'EngineeringUnits(' in val_str:
                        # Extract the unit name from various formats
                        import re
                        match = re.search(r'EngineeringUnits(?:\(|:)\s*([^>)]+)', val_str)
                        if match:
                            return match.group(1).strip()
                    return val_str
                return str(val)
            
            # Make the results jsonable
            if 'results' in result:
                jsonable_results = {}
                for obj_id, properties in result['results'].items():
                    if isinstance(properties, dict):
                        # Special handling for units property
                        processed_properties = {}
                        for prop_name, prop_value in properties.items():
                            if prop_name == 'units' and isinstance(prop_value, int):
                                # Convert numeric units to EngineeringUnits name
                                try:
                                    from bacpypes3.basetypes import EngineeringUnits
                                    engineering_unit = EngineeringUnits(prop_value)
                                    # Get the string representation and extract the unit name
                                    unit_str = str(engineering_unit)
                                    # BACnet EngineeringUnits string format is like "EngineeringUnits(amperes)"
                                    # or just the name directly, so we need to handle both cases
                                    if unit_str.startswith('EngineeringUnits(') and unit_str.endswith(')'):
                                        unit_name = unit_str[17:-1]  # Remove "EngineeringUnits(" and ")"
                                    else:
                                        unit_name = unit_str
                                    processed_properties[prop_name] = unit_name
                                    logging.getLogger(__name__).debug(f"Converted units {prop_value} to {unit_name} for {obj_id}")
                                except (ImportError, ValueError, TypeError) as e:
                                    logging.getLogger(__name__).warning(f"Failed to convert units {prop_value} for {obj_id}: {e}")
                                    processed_properties[prop_name] = make_jsonable(prop_value)
                            else:
                                processed_properties[prop_name] = make_jsonable(prop_value)
                        jsonable_results[str(obj_id)] = processed_properties
                    else:
                        jsonable_results[str(obj_id)] = make_jsonable(properties)
                result['results'] = jsonable_results
            
            return json.dumps(result).encode('utf8')
        except Exception as e:
            tb = traceback.format_exc()
            logging.getLogger(__name__).error(f"Exception in read_object_list_names_endpoint: {e}\n{tb}")
            return json.dumps({"status": "error", "error": str(e), "traceback": tb}).encode('utf8')

    @callback
    async def clear_cache_endpoint(self, _, raw_message: bytes):
        """Endpoint for clearing cache."""
        message = json.loads(raw_message.decode('utf8'))
        device_address = message.get('device_address', None)
        device_object_identifier = message.get('device_object_identifier', None)
        
        if device_address:
            self._clear_cache_for_device(device_address, device_object_identifier)
            result = {"status": "success", "message": f"Cache cleared for device {device_address}"}
        else:
            self._object_list_cache.clear()
            result = {"status": "success", "message": "All cache cleared"}
        # TODO: These results do not seem to follow quite the same format as others.
        return json.dumps(result).encode('utf8')

    @callback
    async def get_cache_stats_endpoint(self, _, raw_message: bytes):
        """Endpoint for getting cache statistics."""
        result = self._get_cache_stats()
        return serialize(result)

    @classmethod
    def get_unique_remote_id(cls, unique_remote_id: tuple) -> tuple:
        """Get a unique identifier for the proxy server
         given a unique_remote_id and protocol-specific set of parameters."""
        return unique_remote_id[0:2]  # TODO: How can we know what the first two params really are?
                                      #  (Ideally they are address and port.)
                                      #  Consider named tuple?

    async def scan_subnet(
        self, 
        network_str: str, 
        whois_timeout: float = 3.0,
        port: int = 47808,
        low_id: int = 0,
        high_id: int = 4194303,
        enable_brute_force: bool = True,
        semaphore_limit: int = 20,
        max_duration: float = 280.0,
        force_fresh_scan: bool = False
    ) -> list:
        """
        Hybrid subnet scan:
          1. Check cache first (unless force_fresh_scan=True)
          2. Directed broadcast Who-Is (subnet broadcast)
          3. Limited broadcast Who-Is (255.255.255.255)
          4. Brute force unicast sweep (only if no devices found from broadcasts)

        Parameters (all have defaults to preserve old behavior):
          network_str        CIDR (e.g. 192.168.1.0/24)
          whois_timeout      Seconds to wait per broadcast (wait_for timeout)
          port               UDP BACnet port to target (default 47808)
          low_id / high_id   Device instance range filter (default 0-4194303)
          enable_brute_force Whether to fall back to per-host unicast if broadcasts find nothing
          semaphore_limit    Concurrency for brute force sweep (default 20)
          max_duration       Safety cap (seconds) for brute force phase (default 280)
          force_fresh_scan   If True, skip cache and force fresh network scan (default False)
        Returns: list[device_dict]
        """

        start_time = time.time()

        # Handle cache logic - check cache unless force_fresh_scan is True
        if not force_fresh_scan:
            cached_devices = self.load_cached_devices(network_str)
            if cached_devices:
                _log.info(f"[scan_subnet] Returning {len(cached_devices)} cached devices for {network_str}")
                return cached_devices

        try:
            net = ipaddress.IPv4Network(network_str, strict=False)
        except ValueError as e:
            _log.error(f"[scan_subnet] Invalid network string '{network_str}': {e}")
            return []

        discovered: list[dict] = []
        seen_keys: set = set()

        def add_devices(devs: list[dict] | None):
            if not devs:
                return
            for d in devs:
                key = (d.get("pduSource"), tuple(d.get("deviceIdentifier", [])))
                if key not in seen_keys:
                    discovered.append(d)
                    seen_keys.add(key)

        # 1. Directed broadcast
        directed_broadcast = f"{net.broadcast_address}:{port}"
        _log.debug(f"[scan_subnet] Directed broadcast Who-Is -> {directed_broadcast}")
        try:
            resp = await asyncio.wait_for(self.who_is(low_id, high_id, directed_broadcast), timeout=whois_timeout)
            add_devices(resp)
        except asyncio.TimeoutError:
            _log.debug("[scan_subnet] Directed broadcast timeout (continuing)")
        except Exception as e:
            _log.debug(f"[scan_subnet] Directed broadcast error: {e}")

        # 2. Limited broadcast if none found yet
        if not discovered:
            limited_broadcast = f"255.255.255.255:{port}"
            _log.debug(f"[scan_subnet] Limited broadcast Who-Is -> {limited_broadcast}")
            try:
                resp = await asyncio.wait_for(self.who_is(low_id, high_id, limited_broadcast), timeout=whois_timeout)
                add_devices(resp)
            except asyncio.TimeoutError:
                _log.debug("[scan_subnet] Limited broadcast timeout (continuing)")
            except Exception as e:
                _log.debug(f"[scan_subnet] Limited broadcast error: {e}")

        # 3. Brute force unicast sweep (only if still nothing)
        if not discovered and enable_brute_force:
            host_count = sum(1 for _ in net.hosts())
            if host_count > 1024:
                _log.warning(f"[scan_subnet] Large network sweep: {host_count} hosts")

            _log.debug(f"[scan_subnet] Starting unicast sweep over {network_str} port={port} concurrency={semaphore_limit}")
            sem = asyncio.Semaphore(semaphore_limit)

            async def probe(ip_obj):
                async with sem:
                    if time.time() - start_time > max_duration:
                        return
                    dest = f"{ip_obj}:{port}"
                    try:
                        resp = await self.who_is(low_id, high_id, dest)
                        if resp:
                            _log.debug(f"[scan_subnet] Unicast hit {dest} -> {len(resp)} device(s)")
                            add_devices(resp)
                    except Exception:
                        pass  # per-host errors are non-fatal

            tasks = []
            for host_ip in net.hosts():
                if time.time() - start_time > max_duration:
                    _log.warning("[scan_subnet] Time limit reached; stopping sweep early")
                    break
                tasks.append(asyncio.create_task(probe(host_ip)))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        for device in discovered:
            self.save_discovered_device(device, network_str, "scan")

        elapsed = time.time() - start_time
        _log.info(f"[scan_subnet] Complete devices_found={len(discovered)} elapsed={elapsed:.2f}s")
        return discovered

    async def read_device_all(self, device_address: str, device_object_identifier: str) -> dict:
        properties = [
            "object-identifier",
            "object-name",
            "object-type",
            "system-status",
            "vendor-name",
            "vendor-identifier",
            "model-name",
            "firmware-revision",
            "application-software-version",
            "location",
            "description",
            "protocol-version",
            "protocol-revision",
            "protocol-services-supported",
            "protocol-object-types-supported",
            "object-list",
            "structured-object-list",
            "max-apdu-length-accepted",
            "segmentation-supported",
            "max-segments-accepted",
            "vt-classes-supported",
            "active-vt-sessions",
            "local-time",
            "local-date",
            "utc-offset",
            "daylight-savings-status",
            "apdu-segment-timeout",
            "apdu-timeout",
            "number-of-apdu-retries",
            "time-synchronization-recipients",
            "max-master",
            "max-info-frames",
            "device-address-binding",
            "database-revision",
            "configuration-files",
            "last-restore-time",
            "backup-failure-timeout",
            "backup-preparation-time",
            "restore-preparation-time",
            "restore-completion-time",
            "backup-and-restore-state",
            "active-cov-subscriptions",
            "last-restart-reason",
            "time-of-device-restart",
            "restart-notification-recipients",
            "utc-time-synchronization-recipients",
            "time-synchronization-interval",
            "align-intervals",
            "interval-offset",
            "serial-number",
            "property-list",
            "status-flags",
            "event-state",
            "reliability",
            "event-detection-enable",
            "notification-class",
            "event-enable",
            "acked-transitions",
            "notify-type",
            "event-time-stamps",
            "event-message-texts",
            "event-message-texts-config",
            "reliability-evaluation-inhibit",
            "active-cov-multiple-subscriptions",
            "audit-notification-recipient",
            "audit-level",
            "auditable-operations",
            "device-uuid",
            "tags",
            "profile-location",
            "deployed-profile-location",
            "profile-name",
        ]
        device_obj = ObjectIdentifier(device_object_identifier)
        daopr_list = [
            DeviceAddressObjectPropertyReference(
                key=prop,
                device_address=device_address,
                object_identifier=device_obj,
                property_reference=PropertyReference(prop)
            ) for prop in properties
        ]
        results = {}
        def callback(key, value):
            logging.getLogger(__name__).debug(f"BatchRead callback: key={key}, value={value}")
            results[key] = value
        batch = BatchRead(daopr_list)
        try:
            await asyncio.wait_for(batch.run(self.bacnet.app, callback=callback), timeout=30)
        except asyncio.TimeoutError:
            logging.getLogger(__name__).error("BatchRead timed out after 30 seconds!")
            results['error'] = 'Timeout waiting for BACnet device response.'
        except Exception as e:
            logging.getLogger(__name__).exception(f"Exception in BatchRead: {e}")
            results['error'] = str(e)
        return results

    async def _get_cached_object_list(self, device_address: str, device_object_identifier: str):
        """
        Get object-list from cache if available and not expired, otherwise read from device.
        Returns the object-list or None if there was an error.
        """
        cache_key = f"{device_address}:{device_object_identifier}"
        current_time = time.time()
        
        # Check cache first
        if cache_key in self._object_list_cache:
            object_list, timestamp = self._object_list_cache[cache_key]
            if current_time - timestamp < self._cache_timeout:
                _log.debug(f"Using cached object-list for {cache_key}")
                return object_list
        
        # Cache miss or expired - read from device
        try:
            _log.debug(f"Reading object-list from device {device_address}")
            object_list = await self.bacnet.read_property(device_address, device_object_identifier, "object-list")
            if object_list:
                self._object_list_cache[cache_key] = (object_list, current_time)
                _log.debug(f"Cached object-list for {cache_key} with {len(object_list)} objects")
                return object_list
        except Exception as e:
            _log.error(f"Error reading object-list from {device_address}: {e}")
            return None

    def _clear_cache_for_device(self, device_address: str, device_object_identifier: str = None):
        """Clear cache for a specific device or all devices."""
        if device_object_identifier:
            cache_key = f"{device_address}:{device_object_identifier}"
            if cache_key in self._object_list_cache:
                del self._object_list_cache[cache_key]
                _log.debug(f"Cleared cache for {cache_key}")
        else:
            # Clear all entries for this device address
            keys_to_remove = [key for key in self._object_list_cache.keys() if key.startswith(f"{device_address}:")]
            for key in keys_to_remove:
                del self._object_list_cache[key]
            _log.debug(f"Cleared cache for all objects at {device_address}")

    def _get_cache_stats(self):
        """Get cache statistics for debugging."""
        current_time = time.time()
        stats = {
            "total_entries": len(self._object_list_cache),
            "entries": []
        }
        
        for cache_key, (object_list, timestamp) in self._object_list_cache.items():
            age = current_time - timestamp
            stats["entries"].append({
                "device": cache_key,
                "object_count": len(object_list) if object_list else 0,
                "age_seconds": age,
                "expired": age > self._cache_timeout
            })
        
        return stats

    def save_object_properties(self, device_address: str, device_object_identifier: str, object_id: str, properties: dict):
        """Save object properties to CSV cache."""
        try:
            from pathlib import Path
            cache_dir = Path.home() / '.bacnet_scan_tool'
            cache_dir.mkdir(exist_ok=True)
            object_cache_file = cache_dir / 'object_properties.csv'
            
            # Create CSV if it doesn't exist
            if not object_cache_file.exists():
                with open(object_cache_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'device_address', 'device_object_identifier', 'object_id', 
                        'object_name', 'units', 'present_value', 'object_type',
                        'first_discovered', 'last_updated', 'read_count'
                    ])
            
            # Read existing data
            existing = {}
            with open(object_cache_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = (row['device_address'], row['device_object_identifier'], row['object_id'])
                    existing[key] = row
            
            # Helper function to safely convert property values
            def safe_property_value(value):
                if value is None:
                    return ""
                # Check if it's an ErrorType object
                if hasattr(value, '__class__') and 'ErrorType' in str(value.__class__):
                    return ""  # Skip error values
                return str(value)
            
            # Update or create record
            key = (device_address, device_object_identifier, object_id)
            now = datetime.now().isoformat()
            
            if key in existing:
                # Update existing
                existing[key]['last_updated'] = now
                existing[key]['read_count'] = str(int(existing[key]['read_count']) + 1)
                # Update properties if they exist and aren't errors
                if 'object-name' in properties:
                    existing[key]['object_name'] = safe_property_value(properties['object-name'])
                if 'units' in properties:
                    existing[key]['units'] = safe_property_value(properties['units'])
                if 'present-value' in properties:
                    existing[key]['present_value'] = safe_property_value(properties['present-value'])
            else:
                # New object
                existing[key] = {
                    'device_address': device_address,
                    'device_object_identifier': device_object_identifier,
                    'object_id': object_id,
                    'object_name': safe_property_value(properties.get('object-name', '')),
                    'units': safe_property_value(properties.get('units', '')),
                    'present_value': safe_property_value(properties.get('present-value', '')),
                    'object_type': str(properties.get('object-type', '')),
                    'first_discovered': now,
                    'last_updated': now,
                    'read_count': '1'
                }
            
            # Write back CSV
            with open(object_cache_file, 'w', newline='') as f:
                fieldnames = [
                    'device_address', 'device_object_identifier', 'object_id', 
                    'object_name', 'units', 'present_value', 'object_type',
                    'first_discovered', 'last_updated', 'read_count'
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(existing.values())
            
            # Also save JSON copy
            object_json_file = cache_dir / 'object_properties.json'
            with open(object_json_file, 'w') as f:
                json.dump(list(existing.values()), f, indent=2)
            
            _log.debug(f"Saved object {object_id} properties for device {device_address} to CSV and JSON")
            
        except Exception as e:
            _log.error(f"Error saving object properties: {e}")

    def load_cached_object_properties(self, device_address: str, device_object_identifier: str, page: int = 1, page_size: int = 100) -> dict:
        """Load cached object properties from CSV file with pagination."""
        try:
            from pathlib import Path
            cache_dir = Path.home() / '.bacnet_scan_tool'
            object_cache_file = cache_dir / 'object_properties.csv'
            
            if not object_cache_file.exists():
                _log.debug("No object properties cache file found")
                return None
            
            # Read all objects for this device
            device_objects = []
            with open(object_cache_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if (row['device_address'] == device_address and 
                        row['device_object_identifier'] == device_object_identifier):
                        device_objects.append(row)
            
            if not device_objects:
                _log.debug(f"No cached objects found for device {device_address}")
                return None
            
            # Calculate pagination based on cached objects
            total_cached_objects = len(device_objects)
            total_cached_pages = (total_cached_objects + page_size - 1) // page_size
            
            # Check if the requested page exists in our cache
            if page > total_cached_pages:
                _log.debug(f"Requested page {page} is beyond cached data (cached pages: {total_cached_pages}), falling back to fresh read")
                return None  # Fall back to fresh read
            
            # We have this page in cache, return it
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            page_objects = device_objects[start_idx:end_idx]
            
            # Convert to expected format
            results = {}
            for obj in page_objects:
                obj_id = obj['object_id']
                results[obj_id] = {
                    'object-name': obj['object_name'] if obj['object_name'] else None,
                    'units': obj['units'] if obj['units'] else None,
                    'present-value': obj['present_value'] if obj['present_value'] else None,
                    '_cached': True,
                    '_cache_info': {
                        'first_discovered': obj['first_discovered'],
                        'last_updated': obj['last_updated'],
                        'read_count': int(obj['read_count']) if obj['read_count'].isdigit() else 0
                    }
                }
            
            _log.info(f"Loaded {len(results)} cached objects for device {device_address}, page {page}")
            
            return {
                "status": "done",
                "results": results,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_items": total_cached_objects,
                    "total_pages": total_cached_pages,
                    "has_next": page < total_cached_pages,
                    "has_previous": page > 1
                },
                "_from_cache": True
            }
            
        except Exception as e:
            _log.error(f"Error loading cached object properties: {e}")
            return None

    async def read_object_list_names_paginated(self, device_address: str, device_object_identifier: str, page: int = 1, page_size: int = 100, force_fresh_read: bool = False) -> dict:
        """
        Reads object properties with CSV caching support.
        
        Parameters:
            force_fresh_read: If True, skip cache and read fresh from device
        """
        _log.info(f"Starting read_object_list_names_paginated for device {device_address}, page {page}, page_size {page_size}, force_fresh_read={force_fresh_read}")
        
        # Validate pagination parameters
        if page < 1:
            return {"status": "error", "error": "Page number must be >= 1"}
        if page_size < 1 or page_size > 1000:
            return {"status": "error", "error": "Page size must be between 1 and 1000"}
        
        # Check cache first (unless force_fresh_read is True)
        if not force_fresh_read:
            cached_result = self.load_cached_object_properties(device_address, device_object_identifier, page, page_size)
            if cached_result:
                _log.info(f"Returning cached object properties for device {device_address}, page {page}")
                return cached_result
        
        # Cache miss or forced fresh read - get from device
        _log.info(f"Cache miss or fresh read forced - reading from device {device_address}")
        
        # Step 1: Get object-list from cache or read from device
        object_list = await self._get_cached_object_list(device_address, device_object_identifier)
        
        if object_list is None:
            return {"status": "error", "error": "Failed to read object-list from device"}
        
        total_objects = len(object_list)
        total_pages = (total_objects + page_size - 1) // page_size
        
        _log.info(f"Object-list has {total_objects} objects, {total_pages} total pages")
        
        # Calculate pagination bounds
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        # Get the page of objects
        page_objects = object_list[start_idx:end_idx]
        
        if not page_objects:
            return {"status": "error", "error": "No objects found for this page"}
        
        # Step 2: Prepare batch read for object-name, units, and present-value of each object
        daopr_list = []
        
        for objid in page_objects:
            # Always read object-name for all objects
            daopr_list.append(DeviceAddressObjectPropertyReference(
                key=f"{objid}:object-name",
                device_address=device_address,
                object_identifier=objid,
                property_reference=PropertyReference("object-name")
            ))
            
            # Only read units and present-value for objects that typically have them
            obj_type = str(objid).split(',')[0] if ',' in str(objid) else str(objid)
            
            # Skip units and present-value for object types that don't typically have them
            skip_types = ['device', 'program', 'schedule', 'trend-log', 'file', 'group', 'notification-class']
            
            if obj_type not in skip_types:
                # Read units (if applicable)
                daopr_list.append(DeviceAddressObjectPropertyReference(
                    key=f"{objid}:units",
                    device_address=device_address,
                    object_identifier=objid,
                    property_reference=PropertyReference("units")
                ))
                # Read present-value (if applicable)  
                daopr_list.append(DeviceAddressObjectPropertyReference(
                    key=f"{objid}:present-value",
                    device_address=device_address,
                    object_identifier=objid,
                    property_reference=PropertyReference("present-value")
                ))
        
        _log.info(f"Prepared batch read for {len(daopr_list)} objects on page {page}")
        
        # Step 3: Execute batch read
        raw_results = {}
        def callback(key, value):
            logging.getLogger(__name__).debug(f"BatchRead callback: key={key}, value={value}")
            raw_results[key] = value
        
        batch = BatchRead(daopr_list)
        try:
            await asyncio.wait_for(batch.run(self.bacnet.app, callback=callback), timeout=90)
            logging.getLogger(__name__).info(f"BatchRead completed successfully with {len(raw_results)} raw results for page {page}")
            
            # Check if we have any results after the batch read
            if not raw_results:
                logging.getLogger(__name__).warning("BatchRead completed but no results were received")
                return {"status": "error", "error": "No results received from BACnet device"}
            
            # Process raw results to organize by object identifier
            results = {}
            objects_to_cache = {}
            
            for key, value in raw_results.items():
                if ':' in key:
                    obj_id, property_name = key.rsplit(':', 1)
                    if obj_id not in results:
                        results[obj_id] = {}
                    if obj_id not in objects_to_cache:
                        objects_to_cache[obj_id] = {}
                    
                    results[obj_id][property_name] = value
                    objects_to_cache[obj_id][property_name] = value
                else:
                    # Fallback for any keys without property suffix
                    results[key] = value
            
            # Save to cache
            for obj_id, properties in objects_to_cache.items():
                self.save_object_properties(device_address, device_object_identifier, obj_id, properties)
            
        except asyncio.TimeoutError:
            logging.getLogger(__name__).error(f"BatchRead timed out after 90 seconds for page {page}!")
            return {"status": "error", "error": "Timeout waiting for BACnet device response after 90 seconds"}
        except Exception as e:
            logging.getLogger(__name__).exception(f"Exception in BatchRead for page {page}: {e}")
            return {"status": "error", "error": f"BatchRead failed: {str(e)}"}
        
        # Return paginated response
        return {
            "status": "done",
            "results": results,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_items": total_objects,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            },
            "_from_cache": False
        }

    async def who_is(self, device_instance_low: int, device_instance_high: int, dest: str):
        destination_addr = dest if isinstance(dest, Address) else Address(dest)
        _log.debug(f"Sending Who-Is to {destination_addr} (low_id: {device_instance_low}, high_id: {device_instance_high})")
        
        app_instance = None
        try:
            app_instance = self.bacnet.app
            # Perform WHO-IS discovery (note: bacpypes3 who_is doesn't accept apdu_timeout parameter)
            i_am_responses = await app_instance.who_is(device_instance_low, device_instance_high, destination_addr)
            _log.debug(f"Received {len(i_am_responses)} I-Am response(s) from {destination_addr}")
            
            devices_found = []
            if i_am_responses:
                for i_am_pdu in i_am_responses:
                    device_info = {
                        "pduSource": str(i_am_pdu.pduSource),
                        "deviceIdentifier": i_am_pdu.iAmDeviceIdentifier,
                        "maxAPDULengthAccepted": i_am_pdu.maxAPDULengthAccepted,
                        "segmentationSupported": str(i_am_pdu.segmentationSupported),
                        "vendorID": i_am_pdu.vendorID,
                    }
                    devices_found.append(device_info)
            return devices_found
        except asyncio.TimeoutError:
            _log.warning(f"Who-Is timeout for {destination_addr}")
            return []
        except ErrorRejectAbortNack as e_bac:
            _log.warning(f"BACnet error during Who-Is: {e_bac}")
            return []
        except Exception as e_gen:
            _log.error(f"General error during Who-Is: {e_gen}")
            return []


async def run_proxy(local_device_address, **kwargs):
    bp = BACnetProxy(local_device_address, **kwargs)
    await bp.start()


def launch_bacnet(parser: ArgumentParser) -> tuple[ArgumentParser, Type[AsyncioProtocolProxy]]:
    parser.add_argument('--local-device-address', type=str, required=True,
                        help='Address on the local machine of this BACnet Proxy.')
    parser.add_argument('--bacnet-network', type=int, default=0,
                        help='The BACnet port as an offset from 47808.')
    parser.add_argument('--vendor-id', type=int, default=999,
                        help='The BACnet vendor ID to use for the local device of this BACnet Proxy.')
    parser.add_argument('--object-name', type=str, default='VOLTTRON BACnet Proxy',
                        help='The name of the local device for this BACnet Proxy.')
    return parser, run_proxy


if __name__ == '__main__':
    sys.exit(launch(launch_bacnet))
