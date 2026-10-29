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
pip install lib-protocol-proxy-bacnet
```

# Development
This library is maintained by the VOLTTRON Development Team.

Please see the following [guidelines](https://github.com/eclipse-volttron/volttron-core/blob/develop/CONTRIBUTING.md)
for contributing to this and/or other VOLTTRON repositories.

[//]: # (Please see the following helpful guide about [using the Protocol Proxy]&#40;https://github.com/eclipse-volttron/lib-protocol-proxy/blob/develop/developing_with_protocol_proxy.md&#41;)

[//]: # (in your VOLTTRON agent or other applications.)

## Served devices (role server)

Besides reaching devices for its callers, the proxy serves BACnet/IP devices. `REGISTER_REMOTE` with `role: server`,
`local_interface`, `port` (an offset from 47808 or an absolute port, distinct from the proxy's own), `device_id`,
`object_name` and `points` creates a bacpypes3 application of its own for the served device: one local object per point
(analog, binary or multi-state input, output or value; `object_type`, `instance`, `units`, `number_of_states`,
`cov_increment`), each answering ReadProperty, ReadPropertyMultiple, WhoIs and SubscribeCOV. `remote_writable` says
whether peers may write a point's present value (outputs and values by default, inputs never); such objects are
commandable, so a peer's priority array applies and relinquishing returns the platform's value, which is the relinquish
default. Writes to other points are refused with write-access-denied.

`WRITE_POINTS` (`values`) sets present values from the platform, `READ_POINTS` (`topics`) reports them, and a peer's
accepted writes are pushed as `RECEIVE_COV` (the same message as change-of-value pushes) tagged with the caller's remote
id. Registration `values` seed points nothing has written, ahead of `initial_value`; re-registration keeps the values
of points that stay; `CLOSE_REMOTE` stops the served device. Replies to these messages carry plain platform values
(`{"result": ..., "error": ...}`).

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
