#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BACnet Device Configuration Scraper using BACpypes3

Utility to scrape device registers and write them to a configuration file.
Supports both single device scraping and batch processing via CSV.

Usage:
    # Single Device
    bacnet-grab-config DEVICE_ID [--address TARGET] [--registry-out-file FILE] [--driver-out-file FILE]
    
    # Batch Mode
    bacnet-grab-config --batch-csv FILE [--out-directory DIR]

    Common Options:
    --ini FILE          : Use specific INI file
    --local ADDR        : Local IP address (required if no INI)
    --max-range-report  : Threshold for large number reporting

Examples:
    # Single device
    bacnet-grab-config 12345 --local 192.168.1.173/24 --registry-out-file device.csv
    
    # Batch mode
    bacnet-grab-config --batch-csv devices.csv --local 192.168.1.173/24 --out-directory configs/
"""

import sys
import asyncio
import argparse
import traceback
import json
import csv
import os
import errno
from csv import DictWriter
from os.path import basename, join
from pathlib import Path
from typing import Optional, Any, Dict, List, Union
from configparser import ConfigParser

from bacpypes3.debugging import ModuleLogger
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier
from bacpypes3.basetypes import EngineeringUnits
from bacpypes3.app import Application
from bacpypes3.apdu import ErrorRejectAbortNack

# some debugging
_debug = 0
_log = ModuleLogger(globals())


async def get_iam(app: Application, device_id: int, target_address: Optional[str] = None) -> Optional[Any]:
    """Send WHO-IS for a specific device and wait for I-AM response"""
    if _debug:
        _log.debug(f"get_iam device_id={device_id} target_address={target_address}")
    
    try:
        destination = Address(target_address) if target_address else None
        i_ams = await app.who_is(device_id, device_id, destination)
        if i_ams:
            return i_ams[0]
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
    """Read a property from a BACnet object"""
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
    """Process a single BACnet object and write to CSV"""
    if _debug:
        _log.debug(f'obj_type = {obj_type}')
        _log.debug(f'bacnet_index = {index}')

    writable = 'FALSE'
    
    try:
        present_value = await read_prop(app, address, obj_type, index, "presentValue")
    except (TypeError, Exception):
        if _debug:
            _log.debug('This object type has no presentValue or error reading it. Skipping.')
        return
    
    object_name = "NO NAME! PLEASE NAME THIS."
    try:
        object_name = await read_prop(app, address, obj_type, index, "objectName")
    except (TypeError, Exception):
        pass
    
    object_notes = ''
    try:
        object_notes = await read_prop(app, address, obj_type, index, "description")
    except (TypeError, Exception):
        pass
    
    object_units = 'UNKNOWN'
    object_units_details = ''
    
    if obj_type.startswith('binary'):
        object_units = 'Boolean'
        
    elif obj_type.startswith('multiState'):
        object_units = 'State'
        try:
            state_count = await read_prop(app, address, obj_type, index, "numberOfStates")
            object_units_details = f'State count: {state_count}'
        except (TypeError, Exception):
            pass
        
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
        try:
            units = await read_prop(app, address, obj_type, index, "units")
            if isinstance(units, EngineeringUnits):
                object_units = units.attr if hasattr(units, 'attr') else str(units)
            else:
                object_units = str(units)
        except (TypeError, Exception):
            object_units = 'UNKNOWN UNITS'
        
        if not object_notes and not obj_type.endswith('Value'):
            try:
                res_value = await read_prop(app, address, obj_type, index, "resolution")
                object_notes = f'Resolution: {res_value:.6g}'
            except (TypeError, Exception):
                pass
        
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
                pass
        
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
        try:
            units = await read_prop(app, address, obj_type, index, "units")
            if isinstance(units, EngineeringUnits):
                object_units = units.attr if hasattr(units, 'attr') else str(units)
            else:
                object_units = str(units)
        except (TypeError, Exception):
            object_units = 'UNKNOWN UNITS'
    
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


async def scrape_device(
    app: Application,
    device_id: int,
    target_address: Optional[str],
    registry_file_path: str,
    driver_file_path: str,
    max_range_report: float
) -> bool:
    """Scrape a single device and write configs"""
    print(f"Connecting to device {device_id}...")
    if target_address:
        print(f"Target address: {target_address}")
    
    try:
        # Get I-AM from target device
        i_am = await get_iam(app, device_id, target_address)
        if not i_am:
            print(f"ERROR: Could not find device {device_id}")
            return False
        
        device_address = i_am.pduSource
        device_type, device_instance = i_am.iAmDeviceIdentifier
        
        print(f"Found device at {device_address}")
        
        # Write driver config JSON
        config_file_name = basename(registry_file_path)
        driver_config = {
            "driver_config": {
                "device_address": str(device_address),
                "device_id": device_instance
            },
            "driver_type": "bacnet",
            "registry_config": f"config://registry_configs/{config_file_name}"
        }
        
        with open(driver_file_path, 'w') as f:
            json.dump(driver_config, f, indent=4)
        
        # Get device info
        try:
            device_name = await read_prop(app, device_address, "device", device_instance, "objectName")
            print(f"Device name: {device_name}")
        except (TypeError, Exception):
            pass
        
        # Setup CSV writer
        with open(registry_file_path, 'w', newline='') as f:
            config_writer = DictWriter(
                f,
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
                    return False
            
            print(f"Found {object_count} objects. Scraping configuration...")
            
            # Process each object
            for object_index in range(1, object_count + 1):
                try:
                    bac_object = await read_prop(app, device_address, "device", device_instance, list_property, index=object_index)
                    
                    if isinstance(bac_object, ObjectIdentifier):
                        obj_type_str = str(bac_object[0])
                        obj_inst = bac_object[1]
                    elif isinstance(bac_object, tuple) and len(bac_object) == 2:
                        obj_type_str = str(bac_object[0])
                        obj_inst = bac_object[1]
                    else:
                        continue
                    
                    if object_index % 10 == 0:
                        print(f"  Processing object {object_index}/{object_count}...")
                    
                    await process_object(app, device_address, obj_type_str, obj_inst, max_range_report, config_writer)
                    
                except Exception as e:
                    _log.debug(f"Unexpected error processing object {object_index}: {e}")
        
        print(f"Configuration scraping complete for device {device_id}!")
        return True

    except Exception as e:
        print(f"Error scraping device {device_id}: {e}")
        if _debug:
            traceback.print_exc()
        return False


def makedirs(path):
    """Create directory path, ignoring if it already exists"""
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


async def async_main() -> None:
    """Main entry point"""
    # Set up custom exception handler for "no broadcast" errors in background tasks
    loop = asyncio.get_running_loop()
    original_handler = loop.get_exception_handler()
    
    def handle_exception(loop, context):
        exception = context.get("exception")
        if exception and isinstance(exception, RuntimeError) and "no broadcast" in str(exception):
            print("\nError: Broadcast not supported on this network interface.")
            print("Please ensure you have specified a local address with a subnet mask (e.g., --local 192.168.1.5/24)")
            # Don't call default handler to avoid traceback
        else:
            if original_handler:
                original_handler(loop, context)
            else:
                loop.default_exception_handler(context)
    
    loop.set_exception_handler(handle_exception)

    app = None
    
    try:
        parser = SimpleArgumentParser(description=__doc__)
        
        # Make device_id optional because of batch mode
        parser.add_argument(
            "device_id",
            type=int,
            nargs='?',
            help="Device ID of the target device (required unless --batch-csv is used)"
        )
        
        parser.add_argument(
            "--batch-csv",
            type=str,
            help="Input CSV file for batch processing (columns: address, device_id)"
        )
        
        parser.add_argument(
            "--out-directory",
            type=str,
            default=".",
            help="Output directory for batch processing (default: current directory)"
        )
        
        parser.add_argument(
            "--ini",
            type=str,
            help="INI configuration file"
        )
        
        parser.add_argument(
            "--local",
            type=str,
            help="Local device address with subnet (e.g., 192.168.1.173/24)"
        )
        
        parser.add_argument(
            "--registry-out-file",
            type=str,
            help="Output registry to CSV file (single device mode)",
        )
        
        parser.add_argument(
            "--driver-out-file",
            type=str,
            help="Output driver configuration to JSON file (single device mode)",
        )
        
        parser.add_argument(
            "--max-range-report",
            type=float,
            help='Affects how very large numbers are reported',
            default=1.0e+20
        )
        
        args = parser.parse_args()
        
        # Validate arguments
        if not args.device_id and not args.batch_csv:
            parser.error("Either DEVICE_ID or --batch-csv must be specified")
        
        # Auto-detect BACpypes.ini
        ini_file = None
        if args.ini:
            ini_file = args.ini
        elif Path("BACpypes.ini").exists():
            ini_file = "BACpypes.ini"
        
        # Load INI file
        ini_config = None
        if ini_file:
            try:
                config = ConfigParser()
                config.read(ini_file)
                class INIConfig: pass
                ini_config = INIConfig()
                if config.has_section('BACpypes'):
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
            except Exception as e:
                print(f"Error loading INI file: {e}")
                sys.exit(1)
        
        # Determine local and target addresses
        # Logic matches bacnet_scan.py for backward compatibility
        
        raw_address_arg = args.address # The value passed via --address
        local_address = None
        target_address = None
        
        if args.local:
            # Explicit --local takes precedence for local interface
            local_address = args.local
            # If --address was also provided, it is the target
            target_address = raw_address_arg
        elif ini_config and hasattr(ini_config, 'address'):
            # INI file provides local address
            local_address = ini_config.address
            # --address argument becomes target (like old script)
            target_address = raw_address_arg
        else:
            # No INI and no --local
            # --address is the local interface (required for BACpypes3)
            local_address = raw_address_arg
            target_address = None # Broadcast or specific device ID discovery
        
        # Check for subnet mask in local address
        if local_address and '/' not in local_address:
            print(f"WARNING: Local address '{local_address}' missing subnet mask (e.g. /24).")

        # Set the address for the Application
        args.address = local_address
        
        # Build application
        app = Application.from_args(args)
        
        if args.batch_csv:
            # Batch Mode
            print(f"Starting batch processing from {args.batch_csv}")
            
            devices_dir = join(args.out_directory, "devices")
            registers_dir = join(args.out_directory, "registry_configs")
            makedirs(devices_dir)
            makedirs(registers_dir)
            
            with open(args.batch_csv, 'r') as f:
                reader = csv.DictReader(f)
                if 'device_id' not in reader.fieldnames:
                    print("ERROR: CSV must have 'device_id' column")
                    sys.exit(1)
                
                devices = list(reader)
                print(f"Found {len(devices)} devices")
                
                success_count = 0
                for i, device in enumerate(devices, 1):
                    device_id = int(device['device_id'])
                    target_addr = device.get('address')
                    
                    print(f"\n[{i}/{len(devices)}] Processing Device {device_id}...")
                    
                    reg_file = join(registers_dir, f"{device_id}.csv")
                    drv_file = join(devices_dir, str(device_id))
                    
                    if await scrape_device(app, device_id, target_addr, reg_file, drv_file, args.max_range_report):
                        success_count += 1
                
                print(f"\nBatch processing complete. Success: {success_count}/{len(devices)}")
                
        else:
            # Single Device Mode
            reg_file = args.registry_out_file if args.registry_out_file else "registry.csv"
            drv_file = args.driver_out_file if args.driver_out_file else "driver.json"
            
            await scrape_device(
                app, 
                args.device_id, 
                target_address, 
                reg_file, 
                drv_file, 
                args.max_range_report
            )

    except KeyboardInterrupt:
        print("\nInterrupted.")
    except Exception as e:
        _log.exception(f"Error: {e}")
        sys.exit(1)
    finally:
        if app:
            app.close()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()

if __name__ == "__main__":
    asyncio.run(main())
