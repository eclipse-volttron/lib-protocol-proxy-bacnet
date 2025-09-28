import asyncio
import logging

from collections.abc import Callable
from datetime import datetime
from typing import Any

from bacpypes3.app import Application, ExecutionError
from bacpypes3.basetypes import DateTime
from bacpypes3.constructeddata import AnyAtomic
from bacpypes3.lib.batchread import BatchRead, DeviceAddressObjectPropertyReference
from bacpypes3.pdu import Address
from bacpypes3.apdu import (ConfirmedPrivateTransferACK, ConfirmedPrivateTransferError, ConfirmedPrivateTransferRequest,
                            ErrorRejectAbortNack, TimeSynchronizationRequest)
from bacpypes3.primitivedata import Date, Null, ObjectIdentifier, ObjectType, TagList, Time, PropertyIdentifier
from bacpypes3.vendor import get_vendor_info

_log = logging.getLogger(__name__)


class BACnet:
    def __init__(self, local_device_address, bacnet_network=0, vendor_id=999, object_name='VOLTTRON BACnet Proxy',
                 device_info_cache=None, router_info_cache=None, ase_id=None, **_):
        _log.debug('WELCOME BAC')
        vendor_info = get_vendor_info(vendor_id)
        device_object_class = vendor_info.get_object_class(ObjectType.device)
        device_object = device_object_class(objectIdentifier=('device', vendor_id), objectName=object_name)
        network_port_object_class = vendor_info.get_object_class(ObjectType.networkPort)
        network_port_object = network_port_object_class(local_device_address,
                                                        objectIdentifier=("network-port", bacnet_network),
                                                        objectName="NetworkPort-1", networkNumber=bacnet_network,
                                                        networkNumberQuality="configured")
        self.app = Application.from_object_list(
            [device_object, network_port_object],
            device_info_cache=device_info_cache,  # TODO: If these should be passed in, add to args & launch.
            router_info_cache=router_info_cache,
            aseID=ase_id
        )
        _log.debug(f'WE HAVE AN APP: {self.app.device_info_cache}')

    async def query_device(self, address: str, property_name: str = 'object-identifier'):
        """Returns properties about the device at the given address.
            If a different property name is not given, this will be the object-id.
            This function allows unicast discovery.
            This can get everything from device if it is using read_property_multiple and ALL
        """
        _log.debug('IN QUERY DEVICE METHOD')
        return await self.read_property(device_address=address, object_identifier='device:4194303',
                                        property_identifier=property_name)

    async def batch_read(self, device_address: str, read_specifications: dict[str, dict]):
        daopr_list = [
            DeviceAddressObjectPropertyReference(
                key=key,
                device_address=device_address,
                object_identifier=spec['object_id'],
                property_reference=(spec['property'], spec['array_index'])
                    if spec['array_index'] is not None else spec['property']
            ) for key, spec in read_specifications.items()
        ]
        results = {}
        batch = BatchRead(daopr_list)
        # run until the batch is done
        await batch.run(self.app, lambda k, v: results.update({k: v}))
        return results

    async def change_of_value(self, device_address: str, object_identifier: str, process_identifier: int | None,
                              confirmed: bool | None, lifetime: bool | None,
                              stop_event: asyncio.Event, cov_callback: Callable, property_identifier: str = None):
        try:
            property_identifier = PropertyIdentifier(property_identifier)
            async with self.app.change_of_value(
                    Address(device_address), ObjectIdentifier(object_identifier),
                    process_identifier, confirmed, int(lifetime)  # TODO: The context manager should be refreshing automatically. Is it?
            ) as scm:
                while not stop_event.is_set():
                    received_property_identifier, property_value = await scm.get_value()
                    if not property_identifier or property_identifier == received_property_identifier:
                        await cov_callback(property_value)
                # Cancel subscription on stop_event.
                scm.issue_confirmed_notifications = None
                scm.lifetime = None
                scm.refresh_subscription()
        except ErrorRejectAbortNack as e:
            # TODO: Should we try again? If so, how often and for how long?
            # TODO: At the point this is not handled here, the caller should be notified the COV subscription is failed.
            _log.warning(f"COV subscription to {object_identifier} on {device_address} failed: {e}")
        except ExecutionError as e:
            # Catch other potential bacpypes3 errors
            _log.warning(f"BACpypes3 execution error: {e}")
        except asyncio.CancelledError:
            # Handle cleanup if the coroutine is cancelled
            _log.info(f"COV subscription to {object_identifier} on {device_address} was cancelled.")
        except Exception as e:
            _log.warning(f"Exception handling COV: {e}")

    async def read_property(self, device_address: str, object_identifier: str, property_identifier: str,
                   property_array_index: int | None = None):
        try:
            _log.debug(f"BACnet.read_property called with device_address={device_address}, object_identifier={object_identifier}, property_identifier={property_identifier}, property_array_index={property_array_index}")
            response = await self.app.read_property(
                Address(device_address),
                ObjectIdentifier(object_identifier),
                property_identifier,
                int(property_array_index) if property_array_index is not None else None
            )
            _log.debug(f"BACnet.read_property response: {response}")
        except ErrorRejectAbortNack as err:
            _log.debug(f'Error reading property {err}')
            response = err
        if isinstance(response, AnyAtomic):
            response = response.get_value()
        _log.debug(f"BACnet.read_property final response: {response}")
        return response

    async def read_property_multiple(self, device_address: str, read_specifications: dict):
        try:  # TODO: Do we need to fall back to read_property in loop? How to detect that? Should it be in driver instead?
            _log.debug(f'Reading one or more properties at {device_address}: {read_specifications}')
            # spec_list = []
            # for (object_id, property_id, property_array_index) in read_specifications.values():
            #     spec_list.extend([
            #         ObjectIdentifier(object_id),
            #         property_id])
            #     if property_array_index is not None:
            #         spec_list.append(int(property_array_index))
            response = await self.app.read_property_multiple(
                Address(device_address),
                ['analogInput, 3000741',  # TODO: This is hard coded for testing. Make this a parsed input.
                ['presentValue']]
            )
            _log.debug(f'Response is: {response}')
        except ErrorRejectAbortNack as err:  # TODO: This does not seem to be catching abortPDU errors.
            _log.debug(f'Error reading property {err}')
            response = err
        if isinstance(response, AnyAtomic):  # TODO: The response probably needs to be parsed. See example code.
            response = response.get_value()
            # _log.debug(f'Response from read_property_multiple: {response}')
        return response

    async def write_property(self, device_address: str, object_identifier: str, property_identifier: str, value: Any,
                    priority: int, property_array_index: int | None = None):
        value = Null(()) if value is None else value
        # TODO: Is additional casting required?
        try:
            return await self.app.write_property(
                Address(device_address),
                ObjectIdentifier(object_identifier),
                property_identifier,
                value,
                int(property_array_index) if property_array_index is not None else None,
                int(priority)
            )
        except ErrorRejectAbortNack as e:
            print(str(e))

    async def write_property_multiple(self, device_address: str, write_specifications: list):
        # TODO Implement write_property_multiple.
        return []

    async def time_synchronization(self, device_address: str, date_time: datetime = None):
        date_time = date_time if date_time else datetime.now()
        address = Address(device_address)
        time_synchronization_request = TimeSynchronizationRequest(
            destination=Address(address),
            time=DateTime(date=Date(date_time.date()), time=Time(date_time.time()))
        )
        response = await self.app.request(time_synchronization_request)
        if isinstance(response, ErrorRejectAbortNack):
            _log.warning(f'Error calling Time Synchronization Service: {response}')


    async def confirmed_private_transfer(self, address: Address, vendor_id: int, service_number: int,
                                         service_parameters: TagList = None) -> Any:
        # TODO: Probably need one or more try blocks.
        # TODO: service_parameters probably needs to already be formatted, but how?
        cpt_request = ConfirmedPrivateTransferRequest(destination=address,
                                                      vendorID=vendor_id,
                                                      serviceNumber=service_number)
        if service_parameters:
            cpt_request.serviceParameters = service_parameters
        response = await self.app.request(cpt_request)
        if isinstance(response, ConfirmedPrivateTransferError):
            _log.warning(f'Error calling Confirmed Private Transfer Service: {response}')
            return None
        elif isinstance(response, ConfirmedPrivateTransferACK):
            return response
        else:
            _log.warning(f'Some other Error: {response}')  # TODO: Improve error handling.
            return None
