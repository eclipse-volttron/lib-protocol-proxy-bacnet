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
Batch BACnet Device Configuration Scraper using BACpypes3

Utility to scrape configuration from multiple BACnet devices using a CSV input file.
This is a modernized version of the legacy grab_multiple_configs.py that used BACpypes v2,
now updated to work with the BACpypes3 version of grab_bacnet_config.py.

Usage:
    grab_multiple_configs.py CSV_FILE [--out-directory DIR] [--ini INI_FILE] [--local LOCAL_IP]

Input CSV format:
    address,device_id
    192.168.1.248,3056211
    192.168.1.249,3056212
    192.168.1.250,3056213

Examples:
    # WITH BACpypes.ini (backward compatible):
    python3 grab_multiple_configs.py devices.csv --out-directory configs/
    
    # WITHOUT BACpypes.ini:
    python3 grab_multiple_configs.py devices.csv --local 192.168.1.173/24 --out-directory configs/
    
    # Use custom INI file:
    python3 grab_multiple_configs.py devices.csv --ini custom.ini --out-directory configs/

Output structure:
    configs/
        devices/
            3056211     (JSON driver config)
            3056212
            3056213
        registry_configs/
            3056211.csv (Registry CSV)
            3056212.csv
            3056213.csv
"""

import argparse
import csv
import os
import errno
import subprocess
import sys
from os.path import dirname, join, abspath


def makedirs(path):
    """Create directory path, ignoring if it already exists"""
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def main():
    arg_parser = argparse.ArgumentParser(description=__doc__, 
                                        formatter_class=argparse.RawDescriptionHelpFormatter)
    
    arg_parser.add_argument(
        "csv_file",
        type=argparse.FileType('r'),
        help="Input CSV file with device list (columns: address, device_id)"
    )
    
    arg_parser.add_argument(
        "--out-directory",
        help="Output directory for configs (default: current directory)",
        default="."
    )
    
    arg_parser.add_argument(
        "--ini",
        help="BACpypes.ini config file to use (auto-detects if not specified)"
    )
    
    arg_parser.add_argument(
        "--local",
        help="Local device address with subnet (e.g., 192.168.1.173/24). Required if no INI file."
    )
    
    args = arg_parser.parse_args()

    # Get the path to grab_bacnet_config.py (should be in same directory)
    program_name = "grab_bacnet_config.py"
    program_path = join(dirname(abspath(__file__)), program_name)
    
    # Check if grab_bacnet_config.py exists
    if not os.path.exists(program_path):
        print(f"ERROR: Could not find {program_name} at {program_path}")
        print(f"Make sure {program_name} is in the same directory as this script.")
        sys.exit(1)

    # Create output directories
    devices_dir = join(args.out_directory, "devices")
    registers_dir = join(args.out_directory, "registry_configs")

    makedirs(devices_dir)
    makedirs(registers_dir)
    
    print(f"Output directories:")
    print(f"  Devices: {devices_dir}")
    print(f"  Registries: {registers_dir}")
    print()

    # Read device list from CSV
    device_list = csv.DictReader(args.csv_file)
    
    # Validate CSV has required columns
    fieldnames = device_list.fieldnames
    if not fieldnames or 'device_id' not in fieldnames:
        print("ERROR: CSV file must have 'device_id' column")
        print(f"Found columns: {fieldnames}")
        sys.exit(1)
    
    devices = list(device_list)
    if not devices:
        print("ERROR: No devices found in CSV file")
        sys.exit(1)
    
    print(f"Found {len(devices)} device(s) to process")
    print()

    # Process each device
    success_count = 0
    error_count = 0
    
    for idx, device in enumerate(devices, 1):
        device_id = device["device_id"]
        address = device.get("address", "")
        
        print(f"[{idx}/{len(devices)}] Processing device {device_id}...")
        
        # Build command arguments
        prog_args = ["python3", program_path]
        prog_args.append(device_id)
        
        # Add address if provided in CSV
        if address:
            prog_args.append("--address")
            prog_args.append(address)
        
        # Add local IP if specified
        if args.local:
            prog_args.append("--local")
            prog_args.append(args.local)
        
        # Add registry output file
        prog_args.append("--registry-out-file")
        prog_args.append(join(registers_dir, f"{device_id}.csv"))
        
        # Add driver config output file
        prog_args.append("--driver-out-file")
        prog_args.append(join(devices_dir, device_id))
        
        # Add INI file if specified
        if args.ini is not None:
            prog_args.append("--ini")
            prog_args.append(args.ini)
        
        print(f"  Command: {' '.join(prog_args)}")
        
        # Execute grab_bacnet_config.py
        try:
            result = subprocess.call(prog_args)
            if result == 0:
                print(f"  ✓ Success")
                success_count += 1
            else:
                print(f"  ✗ Failed (exit code {result})")
                error_count += 1
        except Exception as e:
            print(f"  ✗ Error: {e}")
            error_count += 1
        
        print()
    
    # Print summary
    print("=" * 60)
    print("Summary:")
    print(f"  Total devices: {len(devices)}")
    print(f"  Successful:    {success_count}")
    print(f"  Failed:        {error_count}")
    print()
    
    if success_count > 0:
        print(f"Output files written to: {args.out_directory}")
        print(f"  Device configs:   {devices_dir}/")
        print(f"  Registry configs: {registers_dir}/")
    
    # Exit with error code if any devices failed
    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
