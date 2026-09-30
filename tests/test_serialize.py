"""The proxy's reply serializer: BACnet values become JSON, BACnet errors become the 'error' half of the reply."""
import json

import pytest

from bacpypes3.basetypes import BinaryPV, EngineeringUnits
from bacpypes3.errors import ExecutionError
from bacpypes3.primitivedata import CharacterString, Enumerated, ObjectIdentifier, Real, Unsigned

from protocol_proxy.protocol.bacnet.json import serialize

from tests.conftest import abort_pdu, error_pdu, reject_pdu


def roundtrip(value):
    return json.loads(serialize(value))


@pytest.mark.parametrize('value, expected', [
    (72.5, 72.5), (0, 0), (True, True), (None, None), ('x', 'x'),
    (Real(1.5), 1.5), (Unsigned(7), 7), (CharacterString('name'), 'name'),
    (BinaryPV('active'), 1), (BinaryPV('inactive'), 0),
    (b'\x01\x02', '0102'),
    ([1, Real(2.0)], [1, 2.0]),
], ids=lambda v: type(v).__name__)     # bacpypes3 CharacterString subclasses str; default ids would call .encode()
def test_values(value, expected):
    assert roundtrip(value) == {'result': expected, 'error': {}}


def test_enumerations_and_identifiers_serialize_by_name_or_string():
    assert roundtrip(EngineeringUnits('degreesFahrenheit'))['result'] in ('degreesFahrenheit', 'degrees-fahrenheit', 64)
    out = roundtrip(ObjectIdentifier('analog-input,1'))['result']
    assert 'analog-input' in str(out) and '1' in str(out)


def test_dict_splits_values_from_errors():
    out = roundtrip({'p0': Real(1.0), 'p1': ExecutionError('device', 'unknown-object'), 'p2': 3})
    assert out['result'] == {'p0': 1.0, 'p2': 3}
    assert out['error']['p1']['error'] == 'ExecutionError' and 'unknown-object' in out['error']['p1']['details']


def test_pdu_errors_are_classified():
    abort = roundtrip(abort_pdu(4))['error']
    assert abort['error'] == 'AbortPDU' and 'reason' in abort
    reject = roundtrip(reject_pdu(1))['error']
    assert reject['error'] == 'RejectPDU'
    error = roundtrip(error_pdu('device', 'unknown-object'))['error']
    assert error['error'] == 'ErrorPDU' and 'unknown-object' in error['error_code']


def test_error_type_reports_class_and_code():
    from bacpypes3.basetypes import ErrorType
    out = roundtrip({'p': ErrorType(errorClass='object', errorCode='unknown-object')})['error']['p']
    assert out['error'] == 'ErrorType' and 'unknown-object' in out['error_code'] and 'unknown-object' in out['details']


def test_list_of_mixed_values_and_errors():
    out = roundtrip([Real(1.0), ExecutionError('property', 'unknown-property')])
    assert out['result'][0] == 1.0 and out['result'][1]['error'] == 'ExecutionError'


def test_unknown_objects_fall_back_to_str():
    class Odd:
        def __str__(self):
            return 'odd-thing'
    assert roundtrip({'k': Odd()}) == {'result': {'k': 'odd-thing'}, 'error': {}}
