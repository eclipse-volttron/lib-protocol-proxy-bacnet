"""make_jsonable and _handle_bacnet_response, used by the scan-tool endpoints."""
import ipaddress

from bacpypes3.basetypes import EngineeringUnits
from bacpypes3.errors import ExecutionError
from bacpypes3.primitivedata import ObjectIdentifier, Real

from protocol_proxy.protocol.bacnet.bacnet_utils import _handle_bacnet_response, make_jsonable

from tests.conftest import abort_pdu, error_pdu, reject_pdu


def test_primitives_and_containers():
    assert make_jsonable(1) == 1 and make_jsonable('a') == 'a' and make_jsonable(None) is None
    assert make_jsonable([1, (2, 3), {4}]) == [1, [2, 3], [4]]
    assert make_jsonable({1: {'a': b'\x0f'}}) == {'1': {'a': '0f'}}
    assert make_jsonable(ipaddress.ip_address('10.0.0.1')) == '10.0.0.1'


def test_bacnet_types():
    # Enumerated values are int subclasses, so they pass the primitive check unchanged and JSON-encode as ints;
    # the object-list endpoint converts unit codes to names itself.
    assert make_jsonable(EngineeringUnits('degreesFahrenheit')) == 64
    assert make_jsonable(Real(1.5)) in (1.5, '1.5')
    oid = make_jsonable(ObjectIdentifier('analog-input,1'))
    assert 'analog-input' in str(oid)


def test_errors_become_none():
    assert make_jsonable(ExecutionError('device', 'unknown-object')) is None


def test_handle_bacnet_response_classifies_pdus():
    assert _handle_bacnet_response(abort_pdu(4))['error'] == 'AbortPDU'
    assert _handle_bacnet_response(reject_pdu(1))['error'] == 'RejectPDU'
    err = _handle_bacnet_response(error_pdu('device', 'unknown-object'))
    assert err['error'] == 'ErrorPDU' and 'unknown-object' in err['error_code']
    assert _handle_bacnet_response({'plain': 1}) == {'plain': 1}
