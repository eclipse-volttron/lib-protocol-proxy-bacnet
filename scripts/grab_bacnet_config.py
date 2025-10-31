#!/usr/bin/env python3
# -*- coding: utf-8 -*- {{{
# ===----------------------------------------------------------------------===
#
#                 Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2023 Battelle Memorial Institute
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# ===----------------------------------------------------------------------===
# }}}

"""
BACnet Device Configuration Scraper using BACpypes3

Simple utility to scrape device registers and write them to a configuration file.
This is a modernized version of the legacy grab_bacnet_config.py that used BACpypes v2,
now updated to use BACpypes3 with asyncio.

Usage:
    grab_bacnet_config.py DEVICE_ID [--address TARGET] [--registry-out-file FILE] [--driver-out-file FILE]
    
    With BACpypes.ini in current directory (backward compatible):
    - Uses local device config from INI file
    - --address is optional target device address
    
    Without BACpypes.ini:
    - Use --local for your local IP with subnet (e.g., 192.168.1.173/24)

Examples:
    # WITH BACpypes.ini (like old script):
    python3 grab_bacnet_config.py 12345 --registry-out-file device.csv --driver-out-file config.json
    
    # Target specific device address
    python3 grab_bacnet_config.py 12345 --address 192.168.1.248 --registry-out-file device.csv
    
    # WITHOUT BACpypes.ini:
    python3 grab_bacnet_config.py 12345 --local 192.168.1.173/24 --registry-out-file device.csv
"""

import sys
import asyncio
import argparse
import traceback
import json
from csv import DictWriter
from os.path import basename
from pathlib import Path
from typing import Optional, Any, Dict, List
from configparser import ConfigParser

from bacpypes3.debugging import ModuleLogger, bacpypes_debugging
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier, Enumerated, Unsigned, Boolean, Integer, Real, Double
from bacpypes3.constructeddata import Array, ArrayOf
from bacpypes3.basetypes import PropertyIdentifier, EngineeringUnits
from bacpypes3.app import Application
from bacpypes3.apdu import ErrorRejectAbortNack

# some debugging
_debug = 0
_log = ModuleLogger(globals())


async def get_iam(app: Application, device_id: int, target_address: Optional[str] = None) -> Optional[Any]:
    """
    Send WHO-IS for a specific device and wait for I-AM response
    
    Args:
        app: BACpypes3 Application instance
        device_id: Device instance ID to find
        target_address: Optional target address, None for broadcast
        
    Returns:
        I-AM response or None if not found
    """
    if _debug:
        _log.debug(f"get_iam device_id={device_id} target_address={target_address}")
    
    try:
        destination = Address(target_address) if target_address else None
        
        # Send WHO-IS for specific device
        i_ams = await app.who_is(device_id, device_id, destination)
        
        if i_ams:
            return i_ams[0]  # Return first response
        else:
            _log.error(f"No I-AM response received for device {device_id}")
            return None
            
    except Exception as e:
        _log.error(f"Error getting I-AM: {e}")
        return None


async def read_prop(
    app: Application,
    address: Address,
    obj_type: str,
    obj_inst: int,
    prop_id: str,
    index: Optional[int] = None
) -> Optional[Any]:
    """
    Read a property from a BACnet object
    
    Args:
        app: BACpypes3 Application instance
        address: Device address
        obj_type: Object type (e.g., 'analogInput')
        obj_inst: Object instance number
        prop_id: Property identifier (e.g., 'presentValue')
        index: Optional array index
        
    Returns:
        Property value or None on error
    """
    try:
        value = await app.read_property(
            address,
            ObjectIdentifier(f"{obj_type},{obj_inst}"),
            prop_id,
            index
        )
        return value
    except ErrorRejectAbortNack as e:
        if _debug:
            _log.debug(f"Error reading {obj_type}:{obj_inst} {prop_id}: {e}")
        raise TypeError(f"Error reading property: {e}")
    except Exception as e:
        if _debug:
            _log.debug(f"Exception reading {obj_type}:{obj_inst} {prop_id}: {e}")
        raise


async def process_object(
    app: Application,
    address: Address,
    obj_type: str,
    index: int,
    max_range_report: float,
    config_writer: DictWriter
) -> None:
    """
    Process a single BACnet object and write to CSV
    
    Args:
        app: BACpypes3 Application instance
        address: Device address
        obj_type: Object type string
        index: Object instance number
        max_range_report: Maximum range for reporting
        config_writer: CSV writer for output
    """
    if _debug:
        _log.debug(f'obj_type = {obj_type}')
        _log.debug(f'bacnet_index = {index}')

    writable = 'FALSE'
    
    # Check if object type has presentValue
    # For BACpypes3, we'll try to read it and skip if it fails
    try:
        present_value = await read_prop(app, address, obj_type, index, "presentValue")
    except (TypeError, Exception):
        if _debug:
            _log.debug('This object type has no presentValue or error reading it. Skipping.')
        return
    
    # Get object name
    object_name = "NO NAME! PLEASE NAME THIS."
    try:
        object_name = await read_prop(app, address, obj_type, index, "objectName")
        if _debug:
            _log.debug(f'object name = {object_name}')
    except (TypeError, Exception):
        if _debug:
            _log.debug(traceback.format_exc())
    
    # Get description
    object_notes = ''
    try:
        object_notes = await read_prop(app, address, obj_type, index, "description")
    except (TypeError, Exception):
        if _debug:
            _log.debug(traceback.format_exc())
    
    object_units = 'UNKNOWN'
    object_units_details = ''
    
    # Determine units based on object type
    if obj_type.startswith('binary'):
        object_units = 'Boolean'
        
    elif obj_type.startswith('multiState'):
        object_units = 'State'
        try:
            state_count = await read_prop(app, address, obj_type, index, "numberOfStates")
            object_units_details = f'State count: {state_count}'
        except (TypeError, Exception):
            if _debug:
                _log.debug(traceback.format_exc())
        
        try:
            state_list = await read_prop(app, address, obj_type, index, "stateText")
            if state_list and len(state_list) > 1:
                enum_strings = [str(name) for name in state_list[1:]]
                object_notes = ', '.join(f'{x}={y}' for x, y in enumerate(enum_strings, start=1))
        except (TypeError, Exception):
            pass
        
        if obj_type != 'multiStateInput':
            try:
                default_value = await read_prop(app, address, obj_type, index, "relinquishDefault")
                object_units_details += f' (default {default_value})'
                object_units_details = object_units_details.strip()
            except (TypeError, Exception):
                pass
                
    elif obj_type.startswith('analog') or obj_type in ('largeAnalogValue', 'integerValue', 'positiveIntegerValue'):
        # Try to get units
        try:
            units = await read_prop(app, address, obj_type, index, "units")
            # BACpypes3 returns EngineeringUnits enum
            if isinstance(units, EngineeringUnits):
                # Get the enum name (e.g., 'degreesCelsius')
                object_units = units.attr if hasattr(units, 'attr') else str(units)
            else:
                object_units = str(units)
        except (TypeError, Exception):
            object_units = 'UNKNOWN UNITS'
        
        # Get resolution for inputs
        if not object_notes and not obj_type.endswith('Value'):
            try:
                res_value = await read_prop(app, address, obj_type, index, "resolution")
                object_notes = f'Resolution: {res_value:.6g}'
            except (TypeError, Exception):
                pass
        
        # Get min/max values
        if obj_type not in ('largeAnalogValue', 'integerValue', 'positiveIntegerValue'):
            try:
                min_value = await read_prop(app, address, obj_type, index, "minPresValue")
                max_value = await read_prop(app, address, obj_type, index, "maxPresValue")
                
                has_min = (min_value is not None) and (min_value > -max_range_report)
                has_max = (max_value is not None) and (max_value < max_range_report)
                
                if has_min and has_max:
                    object_units_details = f'{min_value:.2f} to {max_value:.2f}'
                elif has_min:
                    object_units_details = f'Min: {min_value:.2f}'
                elif has_max:
                    object_units_details = f'Max: {max_value:.2f}'
                else:
                    object_units_details = 'No limits.'
            except (TypeError, Exception):
                if _debug:
                    _log.debug(traceback.format_exc())
        
        # Get default value for outputs
        if obj_type != 'analogInput':
            try:
                default_value = await read_prop(app, address, obj_type, index, "relinquishDefault")
                object_units_details += f' (default {default_value})'
                object_units_details = object_units_details.strip()
            except (TypeError, Exception):
                pass
    
    elif obj_type == 'loop':
        object_units = 'Loop'
    else:
        # Try to get units anyway
        try:
            units = await read_prop(app, address, obj_type, index, "units")
            # BACpypes3 returns EngineeringUnits enum
            if isinstance(units, EngineeringUnits):
                # Get the enum name (e.g., 'degreesCelsius')
                object_units = units.attr if hasattr(units, 'attr') else str(units)
            else:
                object_units = str(units)
        except (TypeError, Exception):
            object_units = 'UNKNOWN UNITS'
    
    if _debug:
        _log.debug(f'  object units = {object_units}')
        _log.debug(f'  object units details = {object_units_details}')
        _log.debug(f'  object notes = {object_notes}')
    
    results = {
        'Reference Point Name': object_name,
        'Volttron Point Name': object_name,
        'Units': object_units,
        'Unit Details': object_units_details,
        'BACnet Object Type': obj_type,
        'Property': 'presentValue',
        'Writable': writable,
        'Index': index,
        'Notes': object_notes
    }
    
    config_writer.writerow(results)


async def main() -> None:
    """Main entry point"""
    app = None
    
    try:
        # Parse arguments
        parser = SimpleArgumentParser(description=__doc__)
        
        parser.add_argument(
            "device_id",
            type=int,
            help="Device ID of the target device"
        )
        
        parser.add_argument(
            "--ini",
            type=str,
            help="INI configuration file (like BACpypes.ini from old script)"
        )
        
        parser.add_argument(
            "--local",
            type=str,
            help="Local device address with subnet (e.g., 192.168.1.173/24)"
        )
        
        parser.add_argument(
            "--registry-out-file",
            type=argparse.FileType('w'),
            help="Output registry to CSV file",
            default=sys.stdout
        )
        
        parser.add_argument(
            "--driver-out-file",
            type=argparse.FileType('w'),
            help="Output driver configuration to JSON file",
            default=sys.stdout
        )
        
        parser.add_argument(
            "--max-range-report",
            type=float,
            help='Affects how very large numbers are reported in the "Unit Details" column',
            default=1.0e+20
        )
        
        args = parser.parse_args()
        
        if _debug:
            _log.debug("initialization")
            _log.debug(f"    - args: {args}")
        
        # Auto-detect BACpypes.ini (like bacnet_scan.py)
        ini_file = None
        if args.ini:
            ini_file = args.ini
        elif Path("BACpypes.ini").exists():
            ini_file = "BACpypes.ini"
            if _debug:
                _log.debug("Auto-detected BACpypes.ini in current directory")
        
        # Load INI file if found
        ini_config = None
        if ini_file:
            try:
                config = ConfigParser()
                config.read(ini_file)
                
                class INIConfig:
                    pass
                
                ini_config = INIConfig()
                
                if config.has_section('BACpypes'):
                    if config.has_option('BACpypes', 'objectName'):
                        ini_config.objectname = config.get('BACpypes', 'objectName')
                        if not args.name:
                            args.name = ini_config.objectname
                    
                    if config.has_option('BACpypes', 'address'):
                        ini_config.address = config.get('BACpypes', 'address')
                    
                    if config.has_option('BACpypes', 'objectIdentifier'):
                        ini_config.objectidentifier = config.get('BACpypes', 'objectIdentifier')
                        if not args.instance:
                            args.instance = int(ini_config.objectidentifier)
                    
                    if config.has_option('BACpypes', 'vendorIdentifier'):
                        ini_config.vendoridentifier = config.get('BACpypes', 'vendorIdentifier')
                        if not args.vendoridentifier:
                            args.vendoridentifier = int(ini_config.vendoridentifier)
                
                if _debug:
                    _log.debug(f"Loaded INI file: {ini_file}")
            
            except Exception as e:
                _log.error(f"Error loading INI file: {e}")
                sys.exit(1)
        
        # Handle local/target address logic (like bacnet_scan.py)
        target_address = None
        local_address = None
        
        if args.local:
            local_address = args.local
            target_address = args.address if args.address else None
        elif ini_config and hasattr(ini_config, 'address'):
            local_address = ini_config.address
            target_address = args.address if args.address else None
            if _debug:
                _log.debug(f"Using local address from INI: {local_address}")
        else:
            local_address = args.address
            target_address = None
        
        # Check for subnet mask
        if local_address and '/' not in local_address:
            print("\n⚠️  WARNING: Local address is missing subnet mask!")
            print(f"   Current address: {local_address}")
            print(f"   You probably meant: {local_address}/24")
            print("   BACpypes3 requires a subnet mask (e.g., /24) for proper operation.")
            print("   Attempting to continue, but may fail...\n")
        
        args.address = local_address
        
        # Build the application
        app = Application.from_args(args)
        if _debug:
            _log.debug(f"app: {app}")
        
        print(f"Connecting to device {args.device_id}...")
        print(f"Local address: {args.address}")
        if target_address:
            print(f"Target address: {target_address}")
        
        # Get I-AM from target device
        i_am = await get_iam(app, args.device_id, target_address)
        if not i_am:
            print(f"ERROR: Could not find device {args.device_id}")
            sys.exit(1)
        
        device_address = i_am.pduSource
        device_type, device_instance = i_am.iAmDeviceIdentifier
        
        if _debug:
            _log.debug(f'pduSource = {device_address}')
            _log.debug(f'iAmDeviceIdentifier = {i_am.iAmDeviceIdentifier}')
            _log.debug(f'maxAPDULengthAccepted = {i_am.maxAPDULengthAccepted}')
            _log.debug(f'segmentationSupported = {i_am.segmentationSupported}')
            _log.debug(f'vendorID = {i_am.vendorID}')
        
        print(f"Found device at {device_address}")
        
        # Write driver config JSON
        config_file_name = basename(args.registry_out_file.name)
        driver_config = {
            "driver_config": {
                "device_address": str(device_address),
                "device_id": device_instance
            },
            "driver_type": "bacnet",
            "registry_config": f"config://registry_configs/{config_file_name}"
        }
        
        json.dump(driver_config, args.driver_out_file, indent=4)
        
        # Get device name and description
        try:
            device_name = await read_prop(app, device_address, "device", device_instance, "objectName")
            print(f"Device name: {device_name}")
            if _debug:
                _log.debug(f'device_name = {device_name}')
        except (TypeError, Exception):
            if _debug:
                _log.debug('device missing objectName')
        
        try:
            device_description = await read_prop(app, device_address, "device", device_instance, "description")
            print(f"Device description: {device_description}")
            if _debug:
                _log.debug(f'description = {device_description}')
        except (TypeError, Exception):
            if _debug:
                _log.debug('device missing description')
        
        # Setup CSV writer
        config_writer = DictWriter(
            args.registry_out_file,
            (
                'Reference Point Name',
                'Volttron Point Name',
                'Units',
                'Unit Details',
                'BACnet Object Type',
                'Property',
                'Writable',
                'Index',
                'Write Priority',
                'Notes'
            )
        )
        config_writer.writeheader()
        
        # Get object list
        print("Reading object list...")
        try:
            object_count = await read_prop(app, device_address, "device", device_instance, "objectList", index=0)
            list_property = "objectList"
        except (TypeError, Exception):
            try:
                object_count = await read_prop(app, device_address, "device", device_instance, "structuredObjectList", index=0)
                list_property = "structuredObjectList"
            except (TypeError, Exception):
                print("ERROR: Could not read object list from device")
                sys.exit(1)
        
        if _debug:
            _log.debug(f'objectCount = {object_count}')
        
        print(f"Found {object_count} objects. Scraping configuration...")
        
        # Process each object
        for object_index in range(1, object_count + 1):
            if _debug:
                _log.debug(f'object_device_index = {object_index}')
            
            try:
                bac_object = await read_prop(app, device_address, "device", device_instance, list_property, index=object_index)
                
                # Extract object type and instance
                if isinstance(bac_object, ObjectIdentifier):
                    obj_type_str = str(bac_object[0])
                    obj_inst = bac_object[1]
                elif isinstance(bac_object, tuple) and len(bac_object) == 2:
                    obj_type_str = str(bac_object[0])
                    obj_inst = bac_object[1]
                else:
                    if _debug:
                        _log.debug(f"Unexpected object format: {bac_object}")
                    continue
                
                # Progress indicator
                if object_index % 10 == 0:
                    print(f"  Processing object {object_index}/{object_count}...")
                
                await process_object(app, device_address, obj_type_str, obj_inst, args.max_range_report, config_writer)
                
            except Exception as e:
                _log.debug(f"Unexpected error processing object {object_index}: {e}")
                if _debug:
                    _log.debug(traceback.format_exc())
        
        print(f"\nConfiguration scraping complete!")
        if args.registry_out_file != sys.stdout:
            print(f"Registry file: {args.registry_out_file.name}")
        if args.driver_out_file != sys.stdout:
            print(f"Driver config: {args.driver_out_file.name}")
    
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user.")
        if _debug:
            _log.debug("keyboard interrupt")
    except Exception as e:
        _log.exception(f"an error has occurred: {e}")
        sys.exit(1)
    finally:
        if _debug:
            _log.debug("finally")
        if app:
            app.close()


if __name__ == "__main__":
    asyncio.run(main())