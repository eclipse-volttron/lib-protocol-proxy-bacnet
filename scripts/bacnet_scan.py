#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BACnet Network Scanner using Protocol Proxy

This script uses the existing BACnet protocol proxy infrastructure to perform
WHO-IS discovery. It leverages the proven broadcast workaround that's built
into the proxy.

Usage:
    bacnet_scan_proxy.py [--address TARGET] [options]

Examples:
    # Scan entire network (auto-detect local IP)
    python3 bacnet_scan_proxy.py --timeout 5
    
    # Scan with specific local IP
    python3 bacnet_scan_proxy.py --local 192.168.1.173/24 --timeout 5
    
    # Unicast to specific device
    python3 bacnet_scan_proxy.py --address 192.168.1.248 --timeout 5
    
    # With device instance range filter
    python3 bacnet_scan_proxy.py --range 0 1000
    
    # Export results to CSV
    python3 bacnet_scan_proxy.py --csv-out devices.csv
"""

import sys
import asyncio
import csv
import json
import argparse
import socket
from pathlib import Path
from typing import List, Dict, Any, Optional

# Import the proxy's BACnet class directly
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from protocol_proxy.protocol.bacnet.bacnet import BACnet


class DeviceInfo:
    """Container for discovered device information"""
    
    def __init__(self, device_dict: dict):
        self.address = device_dict.get('pduSource', '').split(':')[0]
        device_id = device_dict.get('deviceIdentifier', [None, None])
        self.device_instance = device_id[1] if len(device_id) > 1 else None
        self.max_apdu_length = device_dict.get('maxAPDULengthAccepted')
        self.segmentation_supported = device_dict.get('segmentationSupported', '')
        self.vendor_id = device_dict.get('vendorID')
    
    def print_info(self) -> None:
        """Print device information to stdout"""
        print()
        print(f"Device Address        = {self.address}")
        print(f"Device Id             = {self.device_instance}")
        print(f"maxAPDULengthAccepted = {self.max_apdu_length}")
        print(f"segmentationSupported = {self.segmentation_supported}")
        print(f"vendorID              = {self.vendor_id}")
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


def write_csv(devices: List[DeviceInfo], filename: str) -> None:
    """Write discovered devices to CSV file"""
    if not devices:
        print("No devices to write to CSV")
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
        print(f"Error writing CSV file: {e}", file=sys.stderr)


def auto_detect_local_ip() -> str:
    """
    Auto-detect the local IP address that would be used to reach the internet.
    Returns IP with /24 subnet by default.
    """
    try:
        # Create a socket to determine which interface would be used
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            # Connect to an external IP (doesn't actually send packets)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            return f"{local_ip}/24"
    except Exception as e:
        raise RuntimeError(f"Could not auto-detect local IP address: {e}")


async def scan_network(
    local_address: Optional[str],
    target_address: Optional[str] = None,
    low_limit: int = 0,
    high_limit: int = 4194303,
    timeout: float = 5.0,
    enable_brute_force: bool = False
) -> List[DeviceInfo]:
    """
    Perform WHO-IS discovery using the BACnet proxy infrastructure
    
    Args:
        local_address: Local device address (e.g., "192.168.1.173/24"), None for auto-detect
        target_address: Target specific device address (None for broadcast)
        low_limit: Lower device instance limit
        high_limit: Upper device instance limit
        timeout: Time to wait for responses
        enable_brute_force: Enable unicast sweep fallback
        
    Returns:
        List of DeviceInfo objects for discovered devices
    """
    # Auto-detect local address if not provided
    if not local_address:
        print("Auto-detecting local IP address...")
        local_address = auto_detect_local_ip()
        print(f"Detected: {local_address}")
    
    # Initialize BACnet instance (like the proxy does)
    print(f"Initializing BACnet proxy at {local_address}...")
    bacnet = BACnet(
        local_device_address=local_address,
        bacnet_network=0,
        vendor_id=999,
        object_name='BACnet Scanner'
    )
    
    try:
        if target_address:
            # Unicast discovery to specific device
            print(f"Scanning target device: {target_address}")
            devices_found = await bacnet.who_is(low_limit, high_limit, target_address)
            
        else:
            # Broadcast discovery - use scan_subnet which has the workaround built-in
            print(f"Scanning network (broadcast + unicast sweep if enabled)...")
            
            # Extract network from local_address
            import ipaddress
            if '/' in local_address:
                network = ipaddress.IPv4Network(local_address, strict=False)
                network_str = str(network)
            else:
                # Default to /24 if no subnet specified
                network_str = f"{local_address}/24"
            
            print(f"Network: {network_str}")
            print(f"Device range: {low_limit} to {high_limit}")
            print(f"Timeout: {timeout}s")
            print(f"Brute force: {'enabled' if enable_brute_force else 'disabled'}")
            
            # Use scan_subnet which includes broadcast workaround
            devices_found = await bacnet.scan_subnet(
                network_str=network_str,
                whois_timeout=timeout,
                port=47808,
                low_id=low_limit,
                high_id=high_limit,
                enable_brute_force=enable_brute_force,
                semaphore_limit=20,
                max_duration=280.0
            )
        
        # Convert to DeviceInfo objects
        devices = [DeviceInfo(d) for d in devices_found]
        return devices
        
    finally:
        # Cleanup: close the BACnet application
        if hasattr(bacnet, 'app') and bacnet.app:
            bacnet.app.close()
            print("BACnet application closed")


async def main() -> None:
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--address",
        type=str,
        help="Target specific device address for WHO-IS request (unicast)"
    )
    
    parser.add_argument(
        "--local",
        type=str,
        help="Local device address with subnet (e.g., 192.168.1.173/24). "
             "If not provided, will auto-detect."
    )
    
    parser.add_argument(
        "--range",
        type=int,
        nargs=2,
        metavar=('LOW', 'HIGH'),
        help="Device instance range (default: 0 4194303)"
    )
    
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Time to wait for responses in seconds (default: 5.0)"
    )
    
    parser.add_argument(
        "--csv-out",
        type=str,
        help="Write results to CSV file"
    )
    
    parser.add_argument(
        "--enable-brute-force",
        action="store_true",
        help="Enable unicast sweep fallback if broadcast finds nothing"
    )
    
    args = parser.parse_args()
    
    # Extract range arguments
    low_limit = args.range[0] if args.range else 0
    high_limit = args.range[1] if args.range else 4194303
    
    try:
        # Perform the scan
        print(f"BACnet Network Scanner (using protocol proxy)")
        print("=" * 60)
        
        devices = await scan_network(
            local_address=args.local,
            target_address=args.address,
            low_limit=low_limit,
            high_limit=high_limit,
            timeout=args.timeout,
            enable_brute_force=args.enable_brute_force
        )
        
        # Display results
        print("\n" + "=" * 60)
        if devices:
            print(f"Found {len(devices)} device(s):")
            for device in devices:
                device.print_info()
            
            # Write CSV if requested
            if args.csv_out:
                write_csv(devices, args.csv_out)
        else:
            print("No devices found.")
        
    except KeyboardInterrupt:
        print("\n\nScan interrupted by user.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
