import ipaddress
import json
import logging
import re

from enum import Enum

from bacpypes3.apdu import ErrorRejectAbortNack, AbortPDU, ErrorPDU, RejectPDU
from bacpypes3.basetypes import EngineeringUnits
from bacpypes3.constructeddata import Sequence
from bacpypes3.primitivedata import Atomic
from bacpypes3.json.util import atomic_encode, sequence_to_json

_log = logging.getLogger(__name__)

# TODO: Check how bacpypes3.json.sequence_to_json might be used in this.
# TODO: When can we handle an error, rather than just serializing it?
# TODO: The attributes are not entirely consistent between these. Fix this.
def _serialize(val):
    """Helper method to handle BACnet responses and convert errors to JSON-serializable format."""
    ret_val, err_val = {}, {}
    if isinstance(val, AbortPDU):
        err_val = {
            "error": "AbortPDU",
            "reason": str(val.apduAbortRejectReason) if hasattr(val,
                                                                'apduAbortRejectReason') else "Unknown abort reason",
            "details": str(val)
        }
    elif isinstance(val, ErrorPDU):
        err_val = {
            "error": "ErrorPDU",
            "error_class": str(val.errorClass) if hasattr(val, 'errorClass') else "Unknown",
            "error_code": str(val.errorCode) if hasattr(val, 'errorCode') else "Unknown",
            "details": str(val)
        }
    elif isinstance(val, RejectPDU):
        err_val = {
            "error": "RejectPDU",
            "reason": str(val.apduAbortRejectReason) if hasattr(val,
                                                                'apduAbortRejectReason') else "Unknown reject reason",
            "details": str(val)
        }
    elif isinstance(val, ErrorRejectAbortNack):
        err_val = {
            "error": "ErrorRejectAbortNack",
            "details": str(val)
        }
    elif hasattr(val, '__class__') and 'Error' in val.__class__.__name__:
        err_val = {
            "error": val.__class__.__name__,
            "details": str(val)
        }
    elif isinstance(val, (list, tuple, set)):
        ret_val = []
        for v in val:
            s, e = _serialize(v)
            ret_val.append(e if e else s)
    elif isinstance(val, (bytes, bytearray)):
        ret_val = val.hex()
    elif isinstance(val, dict):
        for k, v in val.items():
            r, e = _serialize(v)
            if e:
                err_val[k] = e
            else:
                ret_val[k] = r
    elif hasattr(val, 'as_tuple'):
        ret_val = str(val)
    elif isinstance(val, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        ret_val = str(val)
    # Handle BACPypes Atomic and Sequence types:
    elif isinstance(val, Atomic):
        ret_val = atomic_encode(val)
    elif isinstance(val, Sequence):
        ret_val = sequence_to_json(val)
    elif isinstance(val, Enum):
        ret_val = str(val.name)
    elif hasattr(val, 'name'):  # Handle other enum-like objects that only have name
        ret_val = str(val.name)
    elif isinstance(val, (str, int, float, bool, type(None))):
        ret_val = val
    else:
        _log.debug(f"Received unknown type: {type(val)}, forcing to str")
        ret_val = str(val)
    return ret_val, err_val

def serialize(val):
    ret_val, err_val = {}, {}
    try:
        ret_val, err_val = _serialize(val)
    except Exception as e:
        _log.exception(f"When exception occurred, ret_val had been: {ret_val}")
        err_val = {
            "error": "SerializationError",
            "details": str(e),
            "raw_type": str(type(val)),
            "raw_str": str(val)
        }
    ret_val = {'result': ret_val, 'error': err_val}
    return json.dumps(ret_val).encode('utf8')
