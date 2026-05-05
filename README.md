# Protocol Proxy BACnet Library
![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)
![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)
[![Passing?](https://github.com/eclipse-volttron/lib-protocol-proxy-bacnet/actions/workflows/run-tests.yml/badge.svg)](https://github.com/eclipse-volttron/lib-protocol-proxy-bacnet/actions/workflows/run-tests.yml)
[![pypi version](https://img.shields.io/pypi/v/protocol-proxy-bacnet.svg)](https://pypi.org/project/protocol-proxy-bacnet/)

This library provides support for communication and management of BACnet devices to a [Protocol Proxy](https://github.com/eclipse-volttron/lib-protocol-proxy) Manager.
Communication with a BACnet device on a network happens via a virtual BACnet device.

## Automatically installed dependencies
- python = ">=3.10,<4.0"
- protocol-proxy = ">=2.0.0rc0"
- bacpypes3 = ">=0.0.102"


[//]: # (# Documentation)

[//]: # (More detailed documentation can be found on [ReadTheDocs]&#40;https://eclipse-volttron.readthedocs.io/en/latest/external-docs/lib-protocol-proxy-bacnet/index.html. The RST source)

[//]: # (of the documentation for this component is located in the "docs" directory of this repository.)

# Installation
This library, along with its dependencies, can be installed using pip:

```shell
pip install protocol-proxy-bacnet
```

Note that this is rarely necessary as this library will typically be used as a dependency of an application acting as a
Protocol Proxy Manager, and will be installed as a dependency of that application.

# Development
This library is maintained by the VOLTTRON Development Team.

Please see the following [guidelines](https://github.com/eclipse-volttron/volttron-core/blob/develop/CONTRIBUTING.md)
for contributing to this and/or other VOLTTRON repositories.

[//]: # (Please see the following helpful guide about [using the Protocol Proxy]&#40;https://github.com/eclipse-volttron/lib-protocol-proxy/blob/develop/developing_with_protocol_proxy.md&#41;)

[//]: # (in your VOLTTRON agent or other applications.)

# Disclaimer Notice

This material was prepared as an account of work sponsored by an agency of the
United States Government.  Neither the United States Government nor the United
States Department of Energy, nor Battelle, nor any of their employees, nor any
jurisdiction or organization that has cooperated in the development of these
materials, makes any warranty, express or implied, or assumes any legal
liability or responsibility for the accuracy, completeness, or usefulness or any
information, apparatus, product, software, or process disclosed, or represents
that its use would not infringe privately owned rights.

Reference herein to any specific commercial product, process, or service by
trade name, trademark, manufacturer, or otherwise does not necessarily
constitute or imply its endorsement, recommendation, or favoring by the United
States Government or any agency thereof, or Battelle Memorial Institute. The
views and opinions of authors expressed herein do not necessarily state or
reflect those of the United States Government or any agency thereof.
