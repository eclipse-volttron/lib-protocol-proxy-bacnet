"""One BACnet/IP device served by the proxy (the ``server`` role).

A served device is its own bacpypes3 ``Application``: a DeviceObject with the configured instance number, a
NetworkPortObject on ``local_interface:port`` (a port of its own, distinct from the proxy's client application), and one
local object per registered point (analog, binary or multi-state input, output or value). bacpypes3 answers ReadProperty,
ReadPropertyMultiple, WhoIs and SubscribeCOV from those objects, so peers poll or subscribe to the served device like any
other.

Platform writes (``WRITE_POINTS``) set the objects' present values (and, for commandable objects, the relinquish
default). A peer's WriteProperty on a point that is ``remote_writable`` is accepted (commandable objects keep their
priority array), and the resulting present value is pushed to the caller; writes to other points are refused with
``writeAccessDenied``. Only ``present-value`` is served as a point value; other properties come from the registry row
(``units``, ``object_name``) or are bacpypes3's defaults.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from bacpypes3.app import Application
from bacpypes3.basetypes import BinaryPV
from bacpypes3.errors import PropertyError
from bacpypes3.local.analog import AnalogInputObject, AnalogOutputObject, AnalogValueObject
from bacpypes3.local.binary import BinaryInputObject, BinaryOutputObject, BinaryValueObject
from bacpypes3.local.cmd import Commandable
from bacpypes3.local.device import DeviceObject
from bacpypes3.local.multistate import MultiStateInputObject, MultiStateOutputObject, MultiStateValueObject
from bacpypes3.local.networkport import NetworkPortObject
from bacpypes3.primitivedata import Null

from .bacnet_utils import make_jsonable

_log = logging.getLogger(__name__)

PushCallback = Callable[[dict[str, Any]], Awaitable[None]]
OBJECT_CLASSES = {
    'analogInput': AnalogInputObject, 'analogOutput': AnalogOutputObject, 'analogValue': AnalogValueObject,
    'binaryInput': BinaryInputObject, 'binaryOutput': BinaryOutputObject, 'binaryValue': BinaryValueObject,
    'multiStateInput': MultiStateInputObject, 'multiStateOutput': MultiStateOutputObject, 'multiStateValue': MultiStateValueObject,
}
#: Object types a peer may command unless the row says otherwise; inputs are read-only by BACnet convention.
COMMANDABLE_BY_DEFAULT = frozenset({'analogOutput', 'analogValue', 'binaryOutput', 'binaryValue', 'multiStateOutput',
                                    'multiStateValue'})
KIND = {'analog': float, 'binary': bool, 'multiState': int}
BACNET_STANDARD_PORT = 47808


def _kind(object_type: str) -> str:
    for kind in KIND:
        if object_type.startswith(kind):
            return kind
    raise ValueError(f'unsupported BACnet object type {object_type!r}')


def _dashed(name: str) -> str:
    return ''.join(f'-{c.lower()}' if c.isupper() else c for c in name)


@dataclass(frozen=True)
class ServedPoint:
    topic: str
    object_type: str
    instance: int
    remote_writable: bool
    units: str | None = None
    object_name: str | None = None
    initial_value: Any = None
    number_of_states: int = 2
    cov_increment: float | None = None

    @classmethod
    def from_dict(cls, row: dict) -> 'ServedPoint':
        object_type = str(row['object_type'])
        if object_type not in OBJECT_CLASSES:
            raise ValueError(f"{row.get('topic')}: object type {object_type!r} cannot be served "
                             f"(one of {sorted(OBJECT_CLASSES)})")
        prop = str(row.get('property') or 'present-value').replace('_', '-').lower()
        if prop not in ('present-value', 'presentvalue'):
            raise ValueError(f"{row.get('topic')}: only present-value is served, not {prop!r}")
        remote_writable = row.get('remote_writable')
        if remote_writable in (None, ''):
            remote_writable = object_type in COMMANDABLE_BY_DEFAULT
        return cls(topic=str(row['topic']), object_type=object_type, instance=int(row['instance']),
                   remote_writable=bool(remote_writable), units=row.get('units') or None, object_name=row.get('object_name') or None,
                   initial_value=row.get('initial_value'), number_of_states=int(row.get('number_of_states') or 2),
                   cov_increment=None if row.get('cov_increment') in (None, '') else float(row['cov_increment']))

    @property
    def kind(self) -> str:
        return _kind(self.object_type)

    @property
    def identifier(self) -> tuple[str, int]:
        return _dashed(self.object_type), self.instance

    def to_bacnet(self, value: Any):
        """A platform value as the object's present-value type."""
        if self.kind == 'binary':
            if isinstance(value, str):
                text = value.strip().lower()
                if text in ('active', 'true', 't', 'on', 'yes', '1'):
                    return 'active'
                if text in ('inactive', 'false', 'f', 'off', 'no', '0', ''):
                    return 'inactive'
                raise ValueError(f'{value!r} is not a binary value')
            return 'active' if value else 'inactive'
        if self.kind == 'multiState':
            state = int(value)
            if not 1 <= state <= self.number_of_states:
                raise ValueError(f'{value!r} is not one of {self.number_of_states} states')
            return state
        if isinstance(value, bool):
            raise TypeError('a boolean is not an analog value')
        return float(value)

    def to_platform(self, value: Any):
        """An object's present value as the platform type (bool, int or float)."""
        if self.kind == 'binary':
            return (value == BinaryPV.active) if isinstance(value, BinaryPV) else str(value).lower() == 'active'
        if self.kind == 'multiState':
            return int(value)
        return float(value)


class _ServedMixin:
    """Refuses peer writes to points that are not remote-writable and marks accepted ones for the push."""
    _served: 'ServedBACnetDevice | None' = None
    _served_point: ServedPoint | None = None

    async def write_property(self, attr, value, index=None, priority=None):
        attr_name = self._property_identifier_class(attr).attr if isinstance(attr, int) else attr
        if attr_name == 'presentValue' and self._served is not None and self._served.serving_peer:
            if not self._served_point.remote_writable:
                raise PropertyError(errorCode='writeAccessDenied')
            self._served.peer_write_pending.add(self._served_point.topic)
        return await super().write_property(attr, value, index, priority)


def _served_class(base, commandable: bool):
    bases = (_ServedMixin, Commandable, base) if commandable else (_ServedMixin, base)
    return type(f'Served{base.__name__}', bases, {})


class ServedBACnetDevice:
    """A bacpypes3 application on ``local_interface:port`` as device ``device_id``, serving the registered points."""

    def __init__(self, local_interface: str, port: int, *, device_id: int, object_name: str = 'VOLTTRON served device',
                 vendor_id: int = 999, push: PushCallback | None = None, remote_id=None):
        self.local_interface, self.port = local_interface, int(port)
        self.device_id, self.object_name, self.vendor_id = int(device_id), object_name, int(vendor_id)
        self.push, self.remote_id = push, remote_id
        self.points: dict[str, ServedPoint] = {}
        self.objects: dict[str, Any] = {}
        self.values: dict[str, Any] = {}
        self.app: Application | None = None
        self.serving_peer = True
        self.peer_write_pending: set[str] = set()
        self.seeded = 0

    @classmethod
    def udp_port(cls, port: int) -> int:
        return port if port >= 1024 else BACNET_STANDARD_PORT + port

    # ---- configuration ------------------------------------------------------------------------------------------
    def configure_points(self, rows: list[dict]) -> int:
        points = [ServedPoint.from_dict(row) for row in rows]
        seen: dict[tuple[str, int], str] = {}
        for point in points:
            if point.identifier in seen:
                raise ValueError(f'{point.topic} and {seen[point.identifier]} both name {point.object_type} {point.instance}')
            seen[point.identifier] = point.topic
        self.points = {p.topic: p for p in points}
        return len(self.points)

    def describe(self) -> dict:
        return {'role': 'server', 'local_interface': self.local_interface, 'port': self.udp_port(self.port), 'device_id': self.device_id,
                'object_name': self.object_name, 'points': len(self.points),
                'objects': {t: list(p.identifier) for t, p in self.points.items()}, 'listening': self.app is not None}

    # ---- lifecycle ----------------------------------------------------------------------------------------------
    async def start(self, values: dict[str, Any] | None = None):
        """(Re)build the application from the point table.

        A point's value comes from, in order: what this process already holds (a platform restart), the caller's
        current value in ``values`` (the platform's memory, after a proxy restart), the row's ``initial_value``. The
        number of points seeded from ``values`` is left in ``seeded``.
        """
        values = values or {}
        kept = {topic: value for topic, value in self.values.items() if topic in self.points}
        if self.app is not None:
            self.app.close()
            self.app = None
        device = DeviceObject(objectIdentifier=('device', self.device_id), objectName=self.object_name,
                              vendorIdentifier=self.vendor_id)
        network_port = NetworkPortObject(f'{self.local_interface}:{self.udp_port(self.port)}',
                                         objectIdentifier=('network-port', 1), objectName='NetworkPort-1', networkNumber=0,
                                         networkNumberQuality='configured', bacnetIPUDPPort=self.udp_port(self.port))
        objects = {}
        for topic, point in self.points.items():
            klass = _served_class(OBJECT_CLASSES[point.object_type], point.remote_writable)
            kwargs: dict[str, Any] = {'objectIdentifier': point.identifier, 'objectName': point.object_name or topic.rsplit('/', 1)[-1]}
            if point.kind == 'multiState':
                kwargs['numberOfStates'] = point.number_of_states
                kwargs['presentValue'] = 1
            elif point.kind == 'binary':
                kwargs['presentValue'] = 'inactive'
            else:
                kwargs['presentValue'] = 0.0
                if point.units:
                    kwargs['units'] = point.units
                if point.cov_increment is not None:
                    kwargs['covIncrement'] = point.cov_increment
            obj = klass(**kwargs)
            obj._served, obj._served_point = self, point
            obj._property_monitors['presentValue'].append(lambda old, new, topic=topic: self._changed(topic, new))
            objects[topic] = obj
        self.app = Application.from_object_list([device, network_port, *objects.values()])
        self.objects = objects
        await asyncio.sleep(0)                             # let bacpypes3 finish the objects' post-init
        self.values = {}
        self.seeded = 0
        for topic, point in self.points.items():
            if topic in kept:
                value, source = kept[topic], 'held'
            elif values.get(topic) is not None:
                value, source = values[topic], 'seed'
            else:
                value, source = point.initial_value, 'starting'
            if value in (None, ''):
                continue
            try:
                self._store(point, value)
                self.seeded += source == 'seed'
            except (ValueError, TypeError) as e:
                _log.warning(f'{self.local_interface}:{self.port}: {source} value {value!r} for {topic} ignored: {e}')
        _log.info(f'Serving BACnet device {self.device_id} on {self.local_interface}:{self.udp_port(self.port)} with '
                  f'{len(self.points)} objects.')

    async def close(self):
        if self.app is not None:
            self.app.close()
            self.app = None

    # ---- values -------------------------------------------------------------------------------------------------
    def _store(self, point: ServedPoint, value: Any):
        obj = self.objects[point.topic]
        bacnet_value = point.to_bacnet(value)
        self.serving_peer = False
        try:
            if isinstance(obj, Commandable):
                obj.relinquishDefault = bacnet_value
            obj.presentValue = bacnet_value
        finally:
            self.serving_peer = True
        self.values[point.topic] = point.to_platform(obj.presentValue)

    def _current(self, point: ServedPoint):
        return point.to_platform(self.objects[point.topic].presentValue)

    def _changed(self, topic: str, new_value):
        """A property monitor: a peer's accepted write changed the present value; push it."""
        if topic not in self.peer_write_pending:
            return
        self.peer_write_pending.discard(topic)
        point = self.points[topic]
        value = point.to_platform(new_value)
        self.values[topic] = value
        if self.push is not None:
            asyncio.get_running_loop().create_task(self._push_safely({topic: value}))

    async def _push_safely(self, values: dict[str, Any]):
        try:
            await self.push(values)
        except Exception as e:    # pragma: no cover
            _log.warning(f'{self.local_interface}:{self.port}: unable to push a peer write: {e}')

    async def read(self, topics: list[str] | None, **_) -> tuple[dict, dict]:
        topics = list(topics) if topics is not None else list(self.points)
        results, errors = {}, {}
        for topic in topics:
            point = self.points.get(topic)
            if point is None:
                errors[topic] = 'unregistered topic'
            elif self.app is None:
                errors[topic] = 'not serving'
            else:
                results[topic] = self._current(point)
        return results, errors

    async def write(self, values: dict[str, Any], **_) -> tuple[dict, dict]:
        results, errors = {}, {}
        for topic, value in values.items():
            point = self.points.get(topic)
            if point is None:
                errors[topic] = 'unregistered topic'
                continue
            if self.app is None:
                errors[topic] = 'not serving'
                continue
            try:
                self._store(point, value)
            except (ValueError, TypeError) as e:
                errors[topic] = f'bad value {value!r}: {e}'
                continue
            results[topic] = {'status': 'stored', 'value': self.values[topic]}
        return results, errors

    async def push_all(self) -> dict[str, Any]:
        values = {topic: self._current(self.points[topic]) for topic in self.values if topic in self.points} if self.app else {}
        if values and self.push is not None:
            await self._push_safely(values)
        return values


def jsonable(value):
    return make_jsonable(value)
