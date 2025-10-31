#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BACnet Network Scanner using BACpypes3

This script performs WHO-IS discovery to find BACnet devices on the network.
It's a modernized version of the legacy bacnet_scan.py that used BACpypes v2,
now updated to use BACpypes3 with asyncio.

Usage:
    bacnet_scan.py [--address ADDR] [--range LOW HIGH] [--timeout SECONDS] [--csv-out FILE]
    
    Backward compatible with old BACpypes v2 script:
    - Automatically detects BACpypes.ini in current directory (just like old script)
    - Uses objectName, objectIdentifier, vendorIdentifier, address from INI
    - With INI: --address means TARGET device (exactly like old script)
    - Without INI: --address means YOUR local IP (required for BACpypes3)
    
    Advanced options:
    - --ini FILE : Use a different INI file
    - --local ADDR : Override local address from INI

Examples:
    # WITH BACpypes.ini in current directory (EXACTLY like old script!):
    
    # Broadcast scan
    python3 bacnet_scan.py --timeout 5
    
    # Unicast to specific target
    python3 bacnet_scan.py --address 192.168.1.248 --timeout 5
    
    # With device instance range filter
    python3 bacnet_scan.py --range 0 1000
    
    # Export results to CSV
    python3 bacnet_scan.py --csv-out devices.csv --timeout 10
    
    
    # WITHOUT BACpypes.ini (must provide local address):
    
    # Broadcast scan
    python3 bacnet_scan.py --address 192.168.1.173/24 --timeout 5
    
    # Unicast to specific target
    python3 bacnet_scan.py --local 192.168.1.173/24 --address 192.168.1.248
"""

import sys
import asyncio
import csv
from typing import Optional, List
from pathlib import Path

from configparser import ConfigParser

from bacpypes3.debugging import ModuleLogger
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier
from bacpypes3.apdu import IAmRequest
from bacpypes3.app import Application

# some debugging
_debug = 0
_log = ModuleLogger(globals())


class DeviceInfo:
    """Container for discovered device information"""
    
    def __init__(self, i_am: IAmRequest):
        self.address = str(i_am.pduSource)
        self.device_identifier = i_am.iAmDeviceIdentifier
        
        # Extract device instance from identifier
        if isinstance(self.device_identifier, ObjectIdentifier):
            self.device_instance = self.device_identifier[1]
        elif isinstance(self.device_identifier, (list, tuple)) and len(self.device_identifier) == 2:
            self.device_instance = self.device_identifier[1]
        else:
            self.device_instance = None
            
        self.max_apdu_length = i_am.maxAPDULengthAccepted
        self.segmentation_supported = str(i_am.segmentationSupported)
        self.vendor_id = i_am.vendorID
    
    def print_info(self) -> None:
        """Print device information to stdout - matches old script format"""
        sys.stdout.write('\n')
        sys.stdout.write('Device Address        = ' + self.address + '\n')
        sys.stdout.write('Device Id             = ' + str(self.device_instance) + '\n')
        sys.stdout.write('maxAPDULengthAccepted = ' + str(self.max_apdu_length) + '\n')
        sys.stdout.write('segmentationSupported = ' + self.segmentation_supported + '\n')
        sys.stdout.write('vendorID              = ' + str(self.vendor_id) + '\n')
        sys.stdout.flush()
    
    def to_dict(self) -> dict:
        """Convert to dictionary for CSV export"""
        return {
            'address': self.address,
            'device_id': self.device_instance,
            'max_apdu_length': self.max_apdu_length,
            'segmentation_supported': self.segmentation_supported,
            'vendor_id': self.vendor_id
        }


async def scan_network(
    app: Application,
    address: Optional[str] = None,
    low_limit: Optional[int] = None,
    high_limit: Optional[int] = None,
    timeout: float = 5.0
) -> List[DeviceInfo]:
    """
    Perform WHO-IS discovery on the network
    
    Args:
        app: BACpypes3 Application instance
        address: Target specific address, or None for broadcast (uses app.who_is default)
        low_limit: Lower device instance limit (inclusive)
        high_limit: Upper device instance limit (inclusive)
        timeout: Time to wait for responses in seconds
        
    Returns:
        List of DeviceInfo objects for discovered devices
    """
    if _debug:
        _log.debug(f"scan_network address={address} low={low_limit} high={high_limit} timeout={timeout}")
    
    # Set defaults if not provided
    if low_limit is None:
        low_limit = 0
    if high_limit is None:
        high_limit = 4194303  # Maximum device instance
    
    # Perform WHO-IS - if address is None, app.who_is() will use broadcast
    try:
        if _debug:
            _log.debug(f"Sending WHO-IS (low={low_limit}, high={high_limit})")
        
        # Call app.who_is like discover-devices.py does
        # If address is specified, pass it; otherwise let it default to broadcast
        if address:
            destination = Address(address)
            i_am_responses = await app.who_is(low_limit, high_limit, destination)
        else:
            i_am_responses = await app.who_is(low_limit, high_limit)
        
        if _debug:
            _log.debug(f"Received {len(i_am_responses)} I-Am responses")
        
        # Convert to DeviceInfo objects
        devices = [DeviceInfo(i_am) for i_am in i_am_responses]
        
        return devices
        
    except Exception as e:
        _log.error(f"Error during scan: {e}")
        return []


def write_csv(devices: List[DeviceInfo], filename: str) -> None:
    """
    Write discovered devices to CSV file
    
    Args:
        devices: List of DeviceInfo objects
        filename: Output CSV filename
    """
    if not devices:
        _log.warning("No devices to write to CSV")
        return
    
    fieldnames = ['address', 'device_id', 'max_apdu_length', 'segmentation_supported', 'vendor_id']
    
    try:
        with open(filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for device in devices:
                writer.writerow(device.to_dict())
        
        print(f"\nResults written to {filename}")
        
    except Exception as e:
        _log.error(f"Error writing CSV file: {e}")


async def main() -> None:
    """Main entry point"""
    app = None
    try:
        # Parse arguments - use SimpleArgumentParser which supports INI files
        parser = SimpleArgumentParser(description=__doc__)
        
        # Add INI file support (like old script)
        parser.add_argument(
            "--ini",
            type=str,
            help="INI configuration file (like BACpypes.ini from old script)",
        )
        
        # Add --local for explicitly setting local device address
        parser.add_argument(
            "--local",
            type=str,
            help="Local device address with subnet (e.g., 192.168.1.173/24). "
                 "If not provided along with --address, will use --address as local and broadcast."
        )
        
        parser.add_argument(
            "--range",
            type=int,
            nargs=2,
            metavar=('LOW', 'HIGH'),
            help="Lower and upper limit on device instance ID in results"
        )
        
        parser.add_argument(
            "--timeout",
            type=float,
            metavar='SECONDS',
            default=5.0,
            help="Time, in seconds, to wait for responses (default: 5.0)"
        )
        
        parser.add_argument(
            "--csv-out",
            type=str,
            dest="csv_out",
            help="Write results to the specified CSV file"
        )
        
        args = parser.parse_args()
        
        # Auto-detect BACpypes.ini in current directory (like old script)
        ini_file = None
        if args.ini:
            ini_file = args.ini
        elif Path("BACpypes.ini").exists():
            ini_file = "BACpypes.ini"
            if _debug:
                _log.debug("Auto-detected BACpypes.ini in current directory")
        
        # Load INI file if found (simple like old script)
        ini_config = None
        if ini_file:
            try:
                config = ConfigParser()
                config.read(ini_file)
                
                # Create a simple object to hold INI values (like args.ini in old script)
                class INIConfig:
                    pass
                
                ini_config = INIConfig()
                
                # Read values from [BACpypes] section
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
        
        # Handle argument logic to match old script behavior:
        # Old script: --address was the target device (local came from INI)
        # New script: --address (from SimpleArgumentParser) should be local device
        # 
        # Priority order for local address:
        # 1. --local argument (explicit)
        # 2. INI file (if provided)
        # 3. --address argument (default)
        
        target_address = None
        local_address = None
        
        if args.local:
            # Explicit --local takes precedence
            local_address = args.local
            target_address = args.address if args.address else None
        elif ini_config and hasattr(ini_config, 'address'):
            # INI file provides local address, --address becomes target (like old script!)
            local_address = ini_config.address
            target_address = args.address if args.address else None
            if _debug:
                _log.debug(f"Using local address from INI: {local_address}")
        else:
            # No INI or --local, so --address is local (required for BACpypes3)
            local_address = args.address
            target_address = None
        
        # Override args.address to be the local address for Application.from_args()
        args.address = local_address
        
        if _debug:
            _log.debug("args: %r", args)
        
        # Build the application
        app = Application.from_args(args)
        if _debug:
            _log.debug("app: %r", app)
        
        # Extract range arguments
        low_limit = args.range[0] if args.range else None
        high_limit = args.range[1] if args.range else None
        
        # Perform the scan
        print(f"Scanning for BACnet devices (timeout: {args.timeout}s)...")
        print(f"Local address: {args.address}")
        if target_address:
            print(f"Target address: {target_address}")
        else:
            print("Target: Broadcast")
        
        if args.range:
            print(f"Device instance range: {low_limit} to {high_limit}")
        
        devices = await scan_network(
            app=app,
            address=target_address,
            low_limit=low_limit,
            high_limit=high_limit,
            timeout=args.timeout
        )
        
        # Display results
        if devices:
            print(f"\nFound {len(devices)} device(s):")
            for device in devices:
                device.print_info()
            
            # Write CSV if requested
            if args.csv_out:
                write_csv(devices, args.csv_out)
        else:
            print("\nNo devices found.")
        
    except KeyboardInterrupt:
        print("\n\nScan interrupted by user.")
        if _debug:
            _log.debug("keyboard interrupt")
    except Exception as e:
        _log.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if app:
            app.close()


if __name__ == "__main__":
    asyncio.run(main())