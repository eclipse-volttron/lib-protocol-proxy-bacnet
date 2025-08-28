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
        _log.debug("Received AbortPDU")
        err_val = {
            "error": "AbortPDU",
            "reason": str(val.apduAbortRejectReason) if hasattr(val,
                                                                'apduAbortRejectReason') else "Unknown abort reason",
            "details": str(val)
        }
    elif isinstance(val, ErrorPDU):
        _log.debug("Received ErrorPDU")
        err_val = {
            "error": "ErrorPDU",
            "error_class": str(val.errorClass) if hasattr(val, 'errorClass') else "Unknown",
            "error_code": str(val.errorCode) if hasattr(val, 'errorCode') else "Unknown",
            "details": str(val)
        }
    elif isinstance(val, RejectPDU):
        _log.debug("Received RejectPDU")
        err_val = {
            "error": "RejectPDU",
            "reason": str(val.apduAbortRejectReason) if hasattr(val,
                                                                'apduAbortRejectReason') else "Unknown reject reason",
            "details": str(val)
        }
    elif isinstance(val, ErrorRejectAbortNack):
        _log.debug("Received ErrorRejectAbortNack")
        err_val = {
            "error": "ErrorRejectAbortNack",
            "details": str(val)
        }
    elif hasattr(val, '__class__') and 'Error' in val.__class__.__name__:
        _log.debug("Received Class with Error in the name.")
        err_val = {
            "error": val.__class__.__name__,
            "details": str(val)
        }
    elif isinstance(val, (list, tuple, set)):
        _log.debug("Received list, tuple, or set")
        ret_val = []
        for v in val:
            s, e = _serialize(v)
            _log.debug(f's is: {s}. It is of type: {type(s)}')
            ret_val.append(e if e else s)
#        ret_val = [_serialize(v) for v in val]
        _log.debug(f"Unpacked to: {ret_val}")
    elif isinstance(val, (bytes, bytearray)):
        _log.debug("Received bytes")
        ret_val = val.hex()
    elif isinstance(val, dict):
    # elif hasattr(val, '__dict__') and not isinstance(val, type):  # TODO: Why not isinstance(val, dict)?
        _log.debug("Received dict object")
        for k, v in val.items():
            r, e = _serialize(v)
            if e:
                err_val[k] = e
            else:
                ret_val[k] = r
    elif hasattr(val, 'as_tuple'):
        _log.debug("Received something with as_tuple")
        ret_val = str(val)
    elif isinstance(val, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        _log.debug("Received ipaddress")
        ret_val = str(val)
    # Handle BACPypes Atomic and Sequence types:
    elif isinstance(val, Atomic):
        _log.debug("Received BACPypes Atomic Value")
        ret_val = atomic_encode(val)
    elif isinstance(val, Sequence):
        _log.debug("Received BACPypes Sequence Value")
        ret_val = sequence_to_json(val)
    # TODO: Why would the isinstance not work?
    # elif hasattr(val, '__class__') and 'EngineeringUnits' in str(val.__class__): # BACnet EngineeringUnits object
    #     _log.debug("Received EngineeringUnits")
    #     unit = str(val)
    #     # Remove "EngineeringUnits(" and ")"
    #     ret_val = unit[17:-1] if unit.startswith('EngineeringUnits(') and unit.endswith(')') else unit
    elif isinstance(val, Enum):
        _log.debug("Received Enum")
        ret_val = str(val.name)
    elif hasattr(val, 'name'):  # Handle other enum-like objects that only have name
        _log.debug("Received object with name attribute")
        ret_val = str(val.name)
    # TODO: If it has an __str__ method, just calling str will work. We can catch that at the end. Int has an __str__ method, so...
    # elif hasattr(val, '__str__'):
    #     _log.debug("Received object with __str__ method")
    #     val_str = str(val)
    # TODO: Why would we be receiving string versions of any of these objects? Is this something we expect?
    #     if 'ErrorType' in val_str or 'Error' in type(val).__name__:
    #         ret_val = {
    #             "error": val.__class__.__name__,
    #             "details": str(val)
    #         }
    #     # Check if it's a BACnet EngineeringUnits string representation
    #     if 'EngineeringUnits:' in val_str or 'EngineeringUnits(' in val_str:
    #         # Extract the unit name from various formats
    #         _log.debug("Received string version of EngineeringUnits")
    #         match = re.search(r'EngineeringUnits(?:\(|:)\s*([^>)]+)', val_str)
    #         if match:
    #             return match.group(1).strip()
    #     ret_val = val_str
    elif isinstance(val, (str, int, float, bool, type(None))):
        _log.debug("Received scalar")
        ret_val = val
    else:
        _log.debug("Received unknown type, forcing to str")
        # TODO: Replace this forced string conversion with proper BACnet object serialization
        ret_val = str(val)
    _log.debug(f'Returning ret_val of: {ret_val} with type: {type(ret_val)}')
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

# TODO: Not used. When should we attempt this?  Certainly not for all ints.
def get_engineering_unit(val):
    if isinstance(val, int):
        try:
            # Try to convert the integer to an EngineeringUnits enum
            engineering_unit = EngineeringUnits(val)
            unit_str = str(engineering_unit)
            # Handle BACnet EngineeringUnits string format # TODO: Why is this not just using .name?
            if unit_str.startswith('EngineeringUnits(') and unit_str.endswith(')'):
                return unit_str[17:-1]  # Remove "EngineeringUnits(" and ")"
            else:
                return unit_str
        except (ImportError, ValueError, TypeError):
            # If conversion fails, return the original value
            pass
        return str(val)
